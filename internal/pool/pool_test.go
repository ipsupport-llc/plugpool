package pool

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

var ctx = context.Background()

func states(r *rig) map[string]State {
	m := map[string]State{}
	for _, i := range r.pool.Instances() {
		m[i.ID] = i.State
	}
	return m
}

func TestRefreshStates(t *testing.T) {
	r := newRig("w1", "w2", "w3", "w4")
	r.plugs["w1"].on, r.cluster.nodes["w1"] = true, &fakeNode{ready: true}
	r.cluster.nodes["w2"] = &fakeNode{ready: true} // plug reads off
	r.plugs["w3"].on = true
	if err := r.pool.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]State{"k3s://w1": Running, "k3s://w2": Running, "k3s://w3": Creating}
	if got := states(r); !mapsEqual(got, want) {
		t.Fatalf("instances = %v, want %v", got, want)
	}
	if r.pool.TargetSize() != 3 {
		t.Fatalf("TargetSize = %d", r.pool.TargetSize())
	}
}

func TestPlugErrorKeepsLastState(t *testing.T) {
	r := newRig("w1", "w2")
	r.plugs["w1"].on = true
	r.plugs["w2"].on = true
	_ = r.pool.Refresh(ctx) // both Creating
	r.plugs["w1"].readErr = errors.New("timeout")
	_ = r.pool.Refresh(ctx)
	if got := states(r)["k3s://w1"]; got != Creating {
		t.Fatalf("w1 after a plug read error = %v, want Creating", got)
	}

	fresh := newRig("w1", "w2")
	fresh.plugs["w1"].readErr = errors.New("timeout")
	_ = fresh.pool.Refresh(ctx)
	if _, listed := states(fresh)["k3s://w1"]; listed {
		t.Fatal("a never-read plug must count as Off")
	}
}

func TestRefreshNeverFailsOnPlugs(t *testing.T) {
	r := newRig("w1", "w2")
	r.plugs["w1"].readErr = errors.New("timeout")
	r.plugs["w2"].readErr = errors.New("timeout")
	if err := r.pool.Refresh(ctx); err != nil {
		t.Fatalf("plug errors must not fail Refresh (it halts cluster-autoscaler): %v", err)
	}
}

func TestRefreshHungPlugDoesNotBlockOthers(t *testing.T) {
	r := newRig("w1", "w2")
	r.plugs["w1"].hang = true
	r.plugs["w2"].on = true
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := r.pool.Refresh(cctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("Refresh took %v with one hung plug", time.Since(start))
	}
	if got := states(r)["k3s://w2"]; got != Creating {
		t.Fatalf("w2 = %v, want Creating", got)
	}
}

func TestRunningKeptOnPlugError(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	_ = r.pool.Refresh(ctx)
	delete(r.cluster.nodes, "w1")
	r.plugs["w1"].readErr = errors.New("timeout")
	_ = r.pool.Refresh(ctx)
	if got := states(r)["k3s://w1"]; got != Running {
		t.Fatalf("w1 = %v, want Running kept", got)
	}
}

func TestIncreaseSkipsUnknownAndFailingPlugs(t *testing.T) {
	r := newRig("w1", "w2", "w3")
	r.plugs["w1"].readErr = errors.New("unreachable") // never read: unknown
	r.plugs["w2"].setErrs = 1                         // readable, Set fails
	_ = r.pool.Refresh(ctx)
	if err := r.pool.IncreaseSize(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if got := r.log.all(); !slices.Equal(got, []string{"plug on w3"}) {
		t.Fatalf("calls = %v, want w3 switched on", got)
	}
}

func TestDecreaseSkipsJustRegistered(t *testing.T) {
	r := newRig("w1")
	_ = r.pool.Refresh(ctx)
	_ = r.pool.IncreaseSize(ctx, 1)
	r.cluster.nodes["w1"] = &fakeNode{ready: true} // registered since the last Refresh
	if err := r.pool.DecreaseTargetSize(ctx, -1); err == nil {
		t.Fatal("no error when the only Creating node has registered")
	}
	if got := r.log.all(); !slices.Equal(got, []string{"plug on w1"}) {
		t.Fatalf("calls = %v, a just-registered machine lost power", got)
	}
}

func TestNodeWinsOverPlugOff(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: false}
	_ = r.pool.Refresh(ctx)
	if got := states(r)["k3s://w1"]; got != Running {
		t.Fatalf("registered node with plug off = %v, want Running", got)
	}
}

func TestIncreaseSize(t *testing.T) {
	r := newRig("w1", "w2")
	_ = r.pool.Refresh(ctx)
	if err := r.pool.IncreaseSize(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if got := r.log.all(); !slices.Equal(got, []string{"plug on w1"}) {
		t.Fatalf("calls = %v", got)
	}
	if r.pool.TargetSize() != 1 {
		t.Fatalf("TargetSize = %d", r.pool.TargetSize())
	}
	for _, delta := range []int{2, 0, -1} {
		if err := r.pool.IncreaseSize(ctx, delta); err == nil {
			t.Fatalf("IncreaseSize(%d) past maxSize or invalid: no error", delta)
		}
	}
	if got := r.log.all(); len(got) != 1 {
		t.Fatalf("a refused IncreaseSize switched plugs: %v", got)
	}
}

func TestDecreaseTargetSizeOnlyCreating(t *testing.T) {
	r := newRig("w1", "w2")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.plugs["w1"].on = true
	_ = r.pool.Refresh(ctx)
	_ = r.pool.IncreaseSize(ctx, 1) // w2 Creating
	if err := r.pool.DecreaseTargetSize(ctx, -2); err == nil {
		t.Fatal("shrinking past the Creating nodes: no error")
	}
	if err := r.pool.DecreaseTargetSize(ctx, 1); err == nil {
		t.Fatal("positive delta: no error")
	}
	if err := r.pool.DecreaseTargetSize(ctx, -1); err != nil {
		t.Fatal(err)
	}
	if got := r.log.all(); !slices.Equal(got, []string{"plug on w2", "plug off w2"}) {
		t.Fatalf("calls = %v", got)
	}
	if r.pool.TargetSize() != 1 {
		t.Fatalf("TargetSize = %d", r.pool.TargetSize())
	}
}

func TestNodeFor(t *testing.T) {
	r := newRig("w1")
	if n, ok := r.pool.NodeFor("k3s://w1", ""); !ok || n != "w1" {
		t.Fatalf("by providerID = %q %v", n, ok)
	}
	if n, ok := r.pool.NodeFor("", "w1"); !ok || n != "w1" {
		t.Fatalf("by name = %q %v", n, ok)
	}
	if _, ok := r.pool.NodeFor("k3s://other", "other"); ok {
		t.Fatal("foreign node matched")
	}
	if _, ok := r.pool.NodeFor("rke2://w1", "w1"); ok {
		t.Fatal("a name match with a different providerID must not count")
	}
}

func mapsEqual(a, b map[string]State) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func TestRefreshDoesNotOverwriteConcurrentChange(t *testing.T) {
	r := newRig("w1", "w2")
	_ = r.pool.Refresh(ctx) // both Off
	w1read := make(chan struct{})
	r.plugs["w1"].onState = func() { close(w1read) }
	r.plugs["w2"].onState = func() {
		<-w1read                                            // w1 already observed Off
		if err := r.pool.IncreaseSize(ctx, 1); err != nil { // switches w1 on
			t.Error(err)
		}
	}
	_ = r.pool.Refresh(ctx)
	if got := states(r)["k3s://w1"]; got != Creating {
		t.Fatalf("w1 = %v after a concurrent scale-up, want Creating", got)
	}
}
