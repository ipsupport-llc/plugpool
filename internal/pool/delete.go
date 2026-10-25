package pool

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	pollInterval      = 5 * time.Second
	plugRetryPeriod   = 10 * time.Second
	nodeDeleteRetries = 3
)

// DeleteNodes starts the scale-down of each named node and returns once
// every one is marked Deleting; the sequence runs in the background (see
// scaleDown). A node already being deleted is skipped. An unknown name fails
// the whole call before anything is touched.
func (p *Pool) DeleteNodes(ctx context.Context, names []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// cluster-autoscaler gave up on this call (deadline) and may already
	// have untainted the nodes: start nothing.
	if err := ctx.Err(); err != nil {
		return err
	}
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
		p.setState(name, Deleting)
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
// (or notReadyTimeout), grace, plug off, poweroff pod removed, Node deleted.
// A node without a Node object (it never registered) goes straight to plug
// off. A registered node that cluster-autoscaler has not tainted for
// deletion is never touched: the sequence aborts (checked before the
// shutdown and before every plug-off attempt), so a stray or late
// DeleteNodes cannot cut a node that runs workloads, nor a machine that
// registered meanwhile.
func (p *Pool) scaleDown(name string) {
	defer p.wg.Done()
	ctx := p.stop
	log := slog.With("pool", p.group.ID, "node", name)

	info, ok := p.checkNode(ctx, log, name, "before shutdown")
	if !ok {
		return
	}
	// registered: the node existed when the sequence started, so a
	// poweroff pod may exist and must be cleaned up even if the Node
	// disappears on the way.
	registered := info.Exists
	if registered {
		if info.Ready {
			if err := p.cluster.StartPowerOff(ctx, name); err != nil {
				log.Warn("poweroff pod failed, cutting power after the timeout", "err", err)
			}
		}
		if !p.waitNotReady(ctx, name) && ctx.Err() == nil {
			log.Warn("node still Ready after the timeout", "timeout", p.powerOff.NotReadyTimeout.Duration)
		}
		if p.clock.Sleep(ctx, p.powerOff.Grace.Duration) != nil {
			return
		}
	}
	for {
		if _, ok = p.checkNode(ctx, log, name, "before plug off"); !ok {
			return
		}
		pctx, cancel := context.WithTimeout(ctx, p.plugTimeout)
		err := p.plugs[name].Set(pctx, false)
		cancel()
		if err == nil {
			log.Info("plug switched off", "reason", "scale-down")
			break
		}
		log.Warn("plug off failed, retrying", "err", err)
		if p.clock.Sleep(ctx, plugRetryPeriod) != nil {
			return
		}
	}
	state := Off
	if registered {
		// A poweroff pod left bound to the node would shut the machine down
		// again on its next boot: until it is gone, keep the Node and the
		// state Running, so the machine is not picked for a scale-up.
		if err := p.retry(ctx, func() error { return p.cluster.RemovePowerOff(ctx, name) }); err != nil {
			log.Warn("poweroff pod not removed; keeping the Node", "err", err)
			state = Running
		} else if err := p.retry(ctx, func() error { return p.cluster.DeleteNode(ctx, name) }); err != nil {
			log.Warn("node delete failed; the Node stays registered", "err", err)
			state = Running
		} else {
			log.Info("node deleted", "reason", "scale-down")
		}
	}
	p.finish(name, state)
}

// checkNode reads the node and decides whether the sequence may go on:
// not when the node cannot be read, and not when it is Ready without
// cluster-autoscaler's deletion taint. On abort the node leaves Deleting.
func (p *Pool) checkNode(ctx context.Context, log *slog.Logger, name, when string) (NodeInfo, bool) {
	info, err := p.cluster.Node(ctx, name)
	if err != nil {
		log.Warn("node read failed, aborting the scale-down", "when", when, "err", err)
		p.finish(name, Running)
		return info, false
	}
	if info.Exists && !info.ToBeDeleted {
		log.Warn("node is registered and not marked for deletion by cluster-autoscaler, aborting the scale-down", "when", when)
		p.finish(name, Running)
		return info, false
	}
	return info, true
}

// retry runs f up to nodeDeleteRetries times, plugRetryPeriod apart.
func (p *Pool) retry(ctx context.Context, f func() error) error {
	var err error
	for i := range nodeDeleteRetries {
		if err = f(); err == nil {
			return nil
		}
		if i == nodeDeleteRetries-1 || p.clock.Sleep(ctx, plugRetryPeriod) != nil {
			break
		}
	}
	return err
}

// finish ends the node's sequence with the given state.
func (p *Pool) finish(name string, s State) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.deleting, name)
	if s == Off {
		delete(p.poweredOn, name)
	}
	p.setState(name, s)
}

// waitNotReady polls until the node is NotReady or gone, up to
// notReadyTimeout. It reports whether that happened.
func (p *Pool) waitNotReady(ctx context.Context, name string) bool {
	deadline := p.clock.Now().Add(p.powerOff.NotReadyTimeout.Duration)
	for {
		info, err := p.cluster.Node(ctx, name)
		if err == nil && (!info.Exists || !info.Ready) {
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
