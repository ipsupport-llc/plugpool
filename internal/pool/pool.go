// Package pool is one node group: machines that are powered on and off
// through their plugs. State is derived from the plug relays and the Node
// objects on every Refresh; only deletions in flight live in memory.
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/plug"
)

// Cluster is what the pool needs from Kubernetes.
type Cluster interface {
	// Node reports whether the Node object exists and is Ready.
	Node(ctx context.Context, name string) (exists, ready bool, err error)
	// DeleteNode deletes the Node object; an absent node is not an error.
	DeleteNode(ctx context.Context, name string) error
	// StartPowerOff starts the shutdown of the node's machine; idempotent.
	StartPowerOff(ctx context.Context, name string) error
}

// Clock is time, injectable for tests.
type Clock interface {
	Now() time.Time
	// Sleep waits for d or until ctx is done (then returns ctx.Err()).
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns the current time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep waits for d or until ctx is done.
func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// State of a pool node, as cluster-autoscaler sees its instance.
type State int

const (
	// Off: plug off, no Node object.
	Off State = iota
	// Creating: plug on, the machine has not registered yet.
	Creating
	// Running: the Node object exists.
	Running
	// Deleting: the scale-down sequence is running.
	Deleting
)

func (s State) String() string {
	return [...]string{"Off", "Creating", "Running", "Deleting"}[s]
}

// Instance is a pool node that is not Off.
type Instance struct {
	ID    string
	State State
}

// Pool is one node group.
type Pool struct {
	group    config.NodeGroup
	powerOff config.PowerOff
	prefix   string
	plugs    map[string]plug.Switch
	cluster  Cluster
	clock    Clock

	mu        sync.Mutex
	last      map[string]State
	poweredOn map[string]time.Time // when IncreaseSize switched a plug on
	deleting  map[string]bool
	wg        sync.WaitGroup
	stop      context.Context
	cancel    context.CancelFunc
}

// New returns the pool of group g. plugs has one switch per node name.
func New(g config.NodeGroup, p config.PowerOff, prefix string, plugs map[string]plug.Switch, c Cluster, clk Clock) *Pool {
	stop, cancel := context.WithCancel(context.Background())
	return &Pool{
		group: g, powerOff: p, prefix: prefix, plugs: plugs, cluster: c, clock: clk,
		last: map[string]State{}, poweredOn: map[string]time.Time{}, deleting: map[string]bool{},
		stop: stop, cancel: cancel,
	}
}

// Group is the pool's configuration.
func (p *Pool) Group() config.NodeGroup { return p.group }

// Refresh re-reads every node's Node object and, for nodes without one, its
// plug. A read error keeps the node's last state. It fails only when no plug
// it had to read could be read.
func (p *Pool) Refresh(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var plugReads, plugErrs int
	var lastErr error
	for _, n := range p.group.Nodes {
		if p.deleting[n.Name] {
			p.last[n.Name] = Deleting
			continue
		}
		exists, _, err := p.cluster.Node(ctx, n.Name)
		if err != nil {
			slog.Warn("node read failed, keeping last state", "node", n.Name, "err", err)
			continue
		}
		if exists {
			p.last[n.Name] = Running
			continue
		}
		plugReads++
		on, err := p.plugs[n.Name].State(ctx)
		if err != nil {
			plugErrs++
			lastErr = err
			slog.Warn("plug read failed, keeping last state", "node", n.Name, "err", err)
			continue
		}
		if on {
			p.last[n.Name] = Creating
		} else {
			p.last[n.Name] = Off
			delete(p.poweredOn, n.Name)
		}
	}
	if plugReads > 0 && plugErrs == plugReads {
		return fmt.Errorf("pool %s: no plug readable: %w", p.group.ID, lastErr)
	}
	return nil
}

// TargetSize counts the nodes that are Creating or Running.
func (p *Pool) TargetSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.targetSize()
}

func (p *Pool) targetSize() int {
	n := 0
	for _, s := range p.last {
		if s == Creating || s == Running {
			n++
		}
	}
	return n
}

// Instances lists every node that is not Off, in config order.
func (p *Pool) Instances() []Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Instance
	for _, n := range p.group.Nodes {
		if s := p.last[n.Name]; s != Off {
			out = append(out, Instance{ID: p.prefix + n.Name, State: s})
		}
	}
	return out
}

// IncreaseSize switches on the plugs of the first delta Off nodes.
func (p *Pool) IncreaseSize(ctx context.Context, delta int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if delta <= 0 {
		return fmt.Errorf("pool %s: delta %d must be positive", p.group.ID, delta)
	}
	if size := p.targetSize() + delta; size > p.group.MaxSize {
		return fmt.Errorf("pool %s: size %d would exceed maxSize %d", p.group.ID, size, p.group.MaxSize)
	}
	var off []string
	for _, n := range p.group.Nodes {
		if p.last[n.Name] == Off {
			off = append(off, n.Name)
		}
	}
	if len(off) < delta {
		return fmt.Errorf("pool %s: %d nodes off, %d requested", p.group.ID, len(off), delta)
	}
	for _, name := range off[:delta] {
		if err := p.plugs[name].Set(ctx, true); err != nil {
			return fmt.Errorf("pool %s: power on %s: %w", p.group.ID, name, err)
		}
		p.last[name] = Creating
		p.poweredOn[name] = p.clock.Now()
		slog.Info("plug switched on", "pool", p.group.ID, "node", name, "reason", "scale-up")
	}
	return nil
}

// DecreaseTargetSize switches off -delta Creating nodes, the most recently
// powered on first. delta is negative, as cluster-autoscaler passes it.
func (p *Pool) DecreaseTargetSize(ctx context.Context, delta int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if delta >= 0 {
		return fmt.Errorf("pool %s: delta %d must be negative", p.group.ID, delta)
	}
	var creating []string
	for _, n := range p.group.Nodes {
		if p.last[n.Name] == Creating {
			creating = append(creating, n.Name)
		}
	}
	if len(creating) < -delta {
		return fmt.Errorf("pool %s: only %d nodes not yet registered, asked to drop %d", p.group.ID, len(creating), -delta)
	}
	slices.SortStableFunc(creating, func(a, b string) int { return p.poweredOn[b].Compare(p.poweredOn[a]) })
	var errs []error
	for _, name := range creating[:-delta] {
		if err := p.plugs[name].Set(ctx, false); err != nil {
			errs = append(errs, fmt.Errorf("power off %s: %w", name, err))
			continue
		}
		p.last[name] = Off
		delete(p.poweredOn, name)
		slog.Info("plug switched off", "pool", p.group.ID, "node", name, "reason", "target size decreased")
	}
	return errors.Join(errs...)
}

// NodeFor returns the pool node a Kubernetes node is: by providerID, else by name.
func (p *Pool) NodeFor(providerID, name string) (string, bool) {
	for _, n := range p.group.Nodes {
		if providerID != "" && providerID == p.prefix+n.Name {
			return n.Name, true
		}
	}
	for _, n := range p.group.Nodes {
		if name != "" && name == n.Name {
			return n.Name, true
		}
	}
	return "", false
}
