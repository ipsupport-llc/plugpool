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

// NodeInfo is what the pool reads from a Node object.
type NodeInfo struct {
	Exists bool
	Ready  bool
	// ToBeDeleted: cluster-autoscaler's ToBeDeletedByClusterAutoscaler taint
	// is on the node, i.e. it has drained it for removal.
	ToBeDeleted bool
	ProviderID  string
}

// Cluster is what the pool needs from Kubernetes.
type Cluster interface {
	// Node reads the Node object; Exists is false when it is absent.
	Node(ctx context.Context, name string) (NodeInfo, error)
	// DeleteNode deletes the Node object; an absent node is not an error.
	DeleteNode(ctx context.Context, name string) error
	// StartPowerOff starts the shutdown of the node's machine; idempotent.
	StartPowerOff(ctx context.Context, name string) error
	// RemovePowerOff deletes the node's poweroff pods.
	RemovePowerOff(ctx context.Context, name string) error
}

// defaultPlugTimeout bounds one plug request made while serving an RPC;
// cluster-autoscaler's externalgrpc deadline is a few seconds.
const defaultPlugTimeout = 3 * time.Second

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
	// Unknown: no Node object and the plug has never been read.
	Unknown
)

func (s State) String() string {
	return [...]string{"Off", "Creating", "Running", "Deleting", "Unknown"}[s]
}

// Instance is a pool node that is not Off or Unknown.
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
	// plugTimeout bounds each plug request made while serving an RPC.
	plugTimeout time.Duration

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
	pl := &Pool{
		group: g, powerOff: p, prefix: prefix, plugs: plugs, cluster: c, clock: clk,
		plugTimeout: defaultPlugTimeout,
		last:        map[string]State{}, poweredOn: map[string]time.Time{}, deleting: map[string]bool{},
		stop: stop, cancel: cancel,
	}
	for _, n := range g.Nodes {
		pl.last[n.Name] = Unknown
	}
	return pl
}

// Group is the pool's configuration.
func (p *Pool) Group() config.NodeGroup { return p.group }

// Refresh re-reads every node's Node object and, for nodes without one, its
// plug. The reads run in parallel and outside the lock, each plug read
// bounded by plugTimeout. A read error keeps the node's last state and is
// only logged: a failing Refresh would stop cluster-autoscaler entirely, so
// one unreachable plug must not fail it.
func (p *Pool) Refresh(ctx context.Context) error {
	type result struct {
		name    string
		node    NodeInfo
		nodeErr error
		on      bool
		plugErr error
		read    bool // plug was read
	}
	results := make([]result, len(p.group.Nodes))
	var wg sync.WaitGroup
	for i, n := range p.group.Nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := result{name: n.Name}
			r.node, r.nodeErr = p.cluster.Node(ctx, n.Name)
			if r.nodeErr == nil && !r.node.Exists {
				pctx, cancel := context.WithTimeout(ctx, p.plugTimeout)
				r.on, r.plugErr = p.plugs[n.Name].State(pctx)
				cancel()
				r.read = true
			}
			results[i] = r
		}()
	}
	wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range results {
		switch {
		case p.deleting[r.name]:
			p.last[r.name] = Deleting
		case r.nodeErr != nil:
			slog.Warn("node read failed, keeping last state", "node", r.name, "err", r.nodeErr)
		case r.node.Exists:
			if r.node.ProviderID != "" && r.node.ProviderID != p.prefix+r.name {
				slog.Error("node providerID does not match providerIDPrefix + name; cluster-autoscaler will not match it to its instance",
					"node", r.name, "providerID", r.node.ProviderID, "expected", p.prefix+r.name)
			}
			p.last[r.name] = Running
		case r.plugErr != nil:
			slog.Warn("plug read failed, keeping last state", "node", r.name, "state", p.last[r.name], "err", r.plugErr)
		case r.on:
			p.last[r.name] = Creating
		default:
			p.last[r.name] = Off
			delete(p.poweredOn, r.name)
		}
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

// Instances lists every node that is not Off or Unknown, in config order.
func (p *Pool) Instances() []Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Instance
	for _, n := range p.group.Nodes {
		if s := p.last[n.Name]; s != Off && s != Unknown {
			out = append(out, Instance{ID: p.prefix + n.Name, State: s})
		}
	}
	return out
}

// IncreaseSize switches on the plugs of delta Off nodes, in config order.
// A plug that fails is skipped for the next Off node; nodes whose plug was
// never read (Unknown) are not candidates.
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
		return fmt.Errorf("pool %s: %d nodes known to be off, %d requested", p.group.ID, len(off), delta)
	}
	done := 0
	var errs []error
	for _, name := range off {
		if done == delta {
			break
		}
		pctx, cancel := context.WithTimeout(ctx, p.plugTimeout)
		err := p.plugs[name].Set(pctx, true)
		cancel()
		if err != nil {
			slog.Warn("power on failed, trying the next node", "pool", p.group.ID, "node", name, "err", err)
			errs = append(errs, fmt.Errorf("power on %s: %w", name, err))
			continue
		}
		p.last[name] = Creating
		p.poweredOn[name] = p.clock.Now()
		done++
		slog.Info("plug switched on", "pool", p.group.ID, "node", name, "reason", "scale-up")
	}
	if done < delta {
		return fmt.Errorf("pool %s: powered on %d of %d: %w", p.group.ID, done, delta, errors.Join(errs...))
	}
	return nil
}

// DecreaseTargetSize switches off -delta Creating nodes, the most recently
// powered on first. delta is negative, as cluster-autoscaler passes it. Each
// node's Node object is read again first: a machine that registered since
// the last Refresh keeps its power.
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
	want, done := -delta, 0
	var errs []error
	for _, name := range creating {
		if done == want {
			break
		}
		info, err := p.cluster.Node(ctx, name)
		if err != nil {
			errs = append(errs, fmt.Errorf("read node %s: %w", name, err))
			continue
		}
		if info.Exists {
			p.last[name] = Running
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, p.plugTimeout)
		err = p.plugs[name].Set(pctx, false)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("power off %s: %w", name, err))
			continue
		}
		p.last[name] = Off
		delete(p.poweredOn, name)
		done++
		slog.Info("plug switched off", "pool", p.group.ID, "node", name, "reason", "target size decreased")
	}
	if done < want {
		return fmt.Errorf("pool %s: dropped %d of %d unregistered nodes (the others registered or failed): %v", p.group.ID, done, want, errors.Join(errs...))
	}
	return nil
}

// NodeFor returns the pool node a Kubernetes node is: by providerID, or by
// name when the node has no providerID. A node whose providerID has another
// form is not a pool node, even with a matching name.
func (p *Pool) NodeFor(providerID, name string) (string, bool) {
	for _, n := range p.group.Nodes {
		if providerID != "" && providerID == p.prefix+n.Name {
			return n.Name, true
		}
		if providerID == "" && name != "" && name == n.Name {
			return n.Name, true
		}
	}
	return "", false
}
