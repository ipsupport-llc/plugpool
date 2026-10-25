package pool

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	pollInterval    = 5 * time.Second
	plugRetryPeriod = 10 * time.Second
)

// DeleteNodes starts the scale-down of each named node and returns once
// every one is marked Deleting; the sequence runs in the background (see
// scaleDown). A node already being deleted is skipped. An unknown name fails
// the whole call before anything is touched.
func (p *Pool) DeleteNodes(_ context.Context, names []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	known := map[string]bool{}
	for _, n := range p.group.Nodes {
		known[n.Name] = true
	}
	for _, name := range names {
		if !known[name] {
			return fmt.Errorf("pool %s: node %s is not in the pool", p.group.ID, name)
		}
	}
	for _, name := range names {
		if p.deleting[name] {
			continue
		}
		p.deleting[name] = true
		p.last[name] = Deleting
		p.wg.Add(1)
		go p.scaleDown(name)
	}
	return nil
}

// Wait blocks until every scale-down sequence has ended.
func (p *Pool) Wait() { p.wg.Wait() }

// Stop aborts running scale-down sequences (they stop at their next wait)
// and waits for them.
func (p *Pool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// scaleDown powers a node off: shutdown through the poweroff pod, NotReady
// (or notReadyTimeout), grace, plug off, Node deleted. A node without a
// Node object (it never registered) goes straight to plug off.
func (p *Pool) scaleDown(name string) {
	defer p.wg.Done()
	ctx := p.stop
	log := slog.With("pool", p.group.ID, "node", name)

	exists, ready, err := p.cluster.Node(ctx, name)
	if err != nil {
		log.Warn("node read failed, assuming it is up", "err", err)
		exists, ready = true, true
	}
	if exists {
		if ready {
			if err := p.cluster.StartPowerOff(ctx, name); err != nil {
				log.Warn("poweroff pod failed, cutting power after the timeout", "err", err)
			}
		}
		if !p.waitNotReady(ctx, name) && ctx.Err() == nil {
			log.Warn("node still Ready after the timeout, cutting power", "timeout", p.powerOff.NotReadyTimeout.Duration)
		}
		if p.clock.Sleep(ctx, p.powerOff.Grace.Duration) != nil {
			return
		}
	}
	for {
		err := p.plugs[name].Set(ctx, false)
		if err == nil {
			log.Info("plug switched off", "reason", "scale-down")
			break
		}
		log.Warn("plug off failed, retrying", "err", err)
		if p.clock.Sleep(ctx, plugRetryPeriod) != nil {
			return
		}
	}
	if exists {
		if err := p.cluster.DeleteNode(ctx, name); err != nil {
			log.Warn("node delete failed; cluster-autoscaler will see it unregistered", "err", err)
		} else {
			log.Info("node deleted", "reason", "scale-down")
		}
	}
	p.mu.Lock()
	delete(p.deleting, name)
	delete(p.poweredOn, name)
	p.last[name] = Off
	p.mu.Unlock()
}

// waitNotReady polls until the node is NotReady or gone, up to
// notReadyTimeout. It reports whether that happened.
func (p *Pool) waitNotReady(ctx context.Context, name string) bool {
	deadline := p.clock.Now().Add(p.powerOff.NotReadyTimeout.Duration)
	for {
		exists, ready, err := p.cluster.Node(ctx, name)
		if err == nil && (!exists || !ready) {
			return true
		}
		if !p.clock.Now().Before(deadline) {
			return false
		}
		if p.clock.Sleep(ctx, pollInterval) != nil {
			return false
		}
	}
}
