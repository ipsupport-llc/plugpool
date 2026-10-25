package pool

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/plug"
)

// log records every side effect in order, across plugs and the cluster.
type log struct {
	mu    sync.Mutex
	calls []string
}

func (l *log) add(s string) { l.mu.Lock(); l.calls = append(l.calls, s); l.mu.Unlock() }
func (l *log) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

type fakePlug struct {
	name    string
	log     *log
	mu      sync.Mutex
	on      bool
	readErr error
	setErrs int // fail this many Set calls first
}

func (p *fakePlug) State(context.Context) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.on, p.readErr
}

func (p *fakePlug) Set(_ context.Context, on bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.setErrs > 0 {
		p.setErrs--
		return errors.New("plug unreachable")
	}
	p.on = on
	if on {
		p.log.add("plug on " + p.name)
	} else {
		p.log.add("plug off " + p.name)
	}
	return nil
}

type fakeNode struct{ ready bool }

type fakeCluster struct {
	log   *log
	mu    sync.Mutex
	nodes map[string]*fakeNode
	// powerOffMakesNotReady: StartPowerOff turns the node NotReady.
	powerOffMakesNotReady bool
	// gate, when set, blocks StartPowerOff until closed.
	gate chan struct{}
}

func (c *fakeCluster) Node(_ context.Context, name string) (bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[name]
	if !ok {
		return false, false, nil
	}
	return true, n.ready, nil
}

func (c *fakeCluster) DeleteNode(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, name)
	c.log.add("delete node " + name)
	return nil
}

func (c *fakeCluster) StartPowerOff(_ context.Context, name string) error {
	if c.gate != nil {
		<-c.gate
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log.add("poweroff " + name)
	if n, ok := c.nodes[name]; ok && c.powerOffMakesNotReady {
		n.ready = false
	}
	return nil
}

// fakeClock: Sleep returns at once and advances Now.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return ctx.Err()
}

type rig struct {
	pool    *Pool
	plugs   map[string]*fakePlug
	cluster *fakeCluster
	log     *log
	clock   *fakeClock
}

// newRig builds a pool of the named nodes, maxSize = len(names).
func newRig(names ...string) *rig {
	l := &log{}
	r := &rig{plugs: map[string]*fakePlug{}, cluster: &fakeCluster{log: l, nodes: map[string]*fakeNode{}, powerOffMakesNotReady: true}, log: l, clock: &fakeClock{now: time.Unix(1_800_000_000, 0)}}
	g := config.NodeGroup{ID: "pool", MinSize: 0, MaxSize: len(names), Template: config.Template{CPU: "4", Memory: "8Gi", Pods: "110"}}
	switches := map[string]plug.Switch{}
	for _, n := range names {
		g.Nodes = append(g.Nodes, config.Node{Name: n, Plug: config.Plug{URL: "http://plug-" + n + ".example", Switch: "Relay"}})
		p := &fakePlug{name: n, log: l}
		r.plugs[n] = p
		switches[n] = p
	}
	po := config.PowerOff{Namespace: "plugpool", Image: "busybox"}
	po.NotReadyTimeout.Duration = 5 * time.Minute
	po.Grace.Duration = 2 * time.Minute
	r.pool = New(g, po, "k3s://", switches, r.cluster, r.clock)
	return r
}
