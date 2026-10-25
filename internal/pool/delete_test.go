package pool

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// running registers the node; tainted = cluster-autoscaler is deleting it.
func running(r *rig, name string, ready bool) {
	r.plugs[name].on = true
	r.cluster.nodes[name] = &fakeNode{ready: ready, tainted: true}
}

func TestDeleteHappyPath(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	start := r.clock.Now()
	if err := r.pool.DeleteNodes(ctx, []string{"w1"}); err != nil {
		t.Fatal(err)
	}
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"poweroff w1", "plug off w1", "remove poweroff w1", "delete node w1"}) {
		t.Fatalf("calls = %v", got)
	}
	if waited := r.clock.Now().Sub(start); waited < 2*time.Minute {
		t.Fatalf("plug went off %v after NotReady, want the 2m grace", waited)
	}
	if len(r.pool.Instances()) != 0 {
		t.Fatalf("instances after delete = %v", r.pool.Instances())
	}
}

func TestDeleteDeadNode(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", false)
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"plug off w1", "remove poweroff w1", "delete node w1"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestDeleteNeverNotReady(t *testing.T) {
	r := newRig("w1")
	r.cluster.powerOffMakesNotReady = false
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	start := r.clock.Now()
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"poweroff w1", "plug off w1", "remove poweroff w1", "delete node w1"}) {
		t.Fatalf("calls = %v", got)
	}
	if waited := r.clock.Now().Sub(start); waited < 7*time.Minute {
		t.Fatalf("plug went off after %v, want notReadyTimeout 5m + grace 2m", waited)
	}
}

func TestDeleteUnregistered(t *testing.T) {
	r := newRig("w1")
	r.plugs["w1"].on = true
	_ = r.pool.Refresh(ctx) // Creating
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"plug off w1"}) {
		t.Fatalf("calls = %v", got)
	}
	if len(r.pool.Instances()) != 0 {
		t.Fatalf("instances = %v", r.pool.Instances())
	}
}

func TestDeleteTwiceIsNoop(t *testing.T) {
	r := newRig("w1")
	r.cluster.gate = make(chan struct{})
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	if err := r.pool.DeleteNodes(ctx, []string{"w1"}); err != nil {
		t.Fatal(err)
	}
	close(r.cluster.gate)
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"poweroff w1", "plug off w1", "remove poweroff w1", "delete node w1"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestDeleteUnknownNode(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	if err := r.pool.DeleteNodes(ctx, []string{"w1", "stranger"}); err == nil {
		t.Fatal("unknown node: no error")
	}
	r.pool.Wait()
	if got := r.log.all(); len(got) != 0 {
		t.Fatalf("calls = %v", got)
	}
	if r.pool.TargetSize() != 1 {
		t.Fatal("w1 must stay Running")
	}
}

func TestPlugOffRetries(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", true)
	r.plugs["w1"].setErrs = 2
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"poweroff w1", "plug off w1", "remove poweroff w1", "delete node w1"}) {
		t.Fatalf("calls = %v", got)
	}
}

func TestDeletingExcludedFromTargetSize(t *testing.T) {
	r := newRig("w1", "w2")
	r.cluster.gate = make(chan struct{})
	running(r, "w1", true)
	running(r, "w2", true)
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	_ = r.pool.Refresh(ctx)
	if r.pool.TargetSize() != 1 {
		t.Fatalf("TargetSize = %d, want 1", r.pool.TargetSize())
	}
	if got := states(r)["k3s://w1"]; got != Deleting {
		t.Fatalf("w1 = %v, want Deleting", got)
	}
	close(r.cluster.gate)
	r.pool.Wait()
}

func TestDeleteReadyNodeWithoutTaintAborts(t *testing.T) {
	r := newRig("w1")
	r.plugs["w1"].on = true
	r.cluster.nodes["w1"] = &fakeNode{ready: true} // not being deleted by CA
	_ = r.pool.Refresh(ctx)
	if err := r.pool.DeleteNodes(ctx, []string{"w1"}); err != nil {
		t.Fatal(err)
	}
	r.pool.Wait()
	if got := r.log.all(); len(got) != 0 {
		t.Fatalf("a Ready node without the deletion taint was touched: %v", got)
	}
	_ = r.pool.Refresh(ctx)
	if got := states(r)["k3s://w1"]; got != Running {
		t.Fatalf("w1 = %v, want Running", got)
	}
}

func TestDeleteAbortsBeforePlugOffWhenUntainted(t *testing.T) {
	r := newRig("w1")
	r.cluster.powerOffMakesNotReady = false
	r.cluster.powerOffUntaints = true
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := r.log.all(); !slices.Equal(got, []string{"poweroff w1"}) {
		t.Fatalf("calls = %v, want no plug off for a Ready untainted node", got)
	}
}

func TestDeleteNodesCancelledContext(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", true)
	_ = r.pool.Refresh(ctx)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := r.pool.DeleteNodes(cctx, []string{"w1"}); err == nil {
		t.Fatal("cancelled RPC context: no error")
	}
	r.pool.Wait()
	if got := r.log.all(); len(got) != 0 {
		t.Fatalf("calls = %v", got)
	}
}

func TestDeleteNodeFailureKeepsRunning(t *testing.T) {
	r := newRig("w1")
	running(r, "w1", false)
	r.cluster.deleteErr = errors.New("forbidden")
	_ = r.pool.Refresh(ctx)
	_ = r.pool.DeleteNodes(ctx, []string{"w1"})
	r.pool.Wait()
	if got := states(r)["k3s://w1"]; got != Running {
		t.Fatalf("w1 = %v after a failed Node delete, want Running", got)
	}
}
