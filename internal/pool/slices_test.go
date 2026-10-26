package pool

import (
	"context"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// gpuSlice is what a DRA driver publishes for a node with n GPUs.
func gpuSlice(node string, n int) resourceapi.ResourceSlice {
	s := resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: node + "-gpu.example.com-abcde", UID: types.UID("uid-" + node), ResourceVersion: "7"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   "gpu.example.com",
			NodeName: &node,
			Pool:     resourceapi.ResourcePool{Name: node, Generation: 3, ResourceSliceCount: 1},
		},
	}
	for i := range n {
		s.Spec.Devices = append(s.Spec.Devices, resourceapi.Device{
			Name:     "gpu-" + string(rune('0'+i)),
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{"vram": {Value: resource.MustParse("6Gi")}},
		})
	}
	return s
}

type memStore struct {
	saved map[string][]resourceapi.ResourceSlice
	saves int
}

func (m *memStore) Load(context.Context) (map[string][]resourceapi.ResourceSlice, error) {
	return m.saved, nil
}
func (m *memStore) Save(_ context.Context, s map[string][]resourceapi.ResourceSlice) error {
	m.saved, m.saves = s, m.saves+1
	return nil
}

func TestTemplateSlicesFromSnapshot(t *testing.T) {
	r := newRig("w1", "w2")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{gpuSlice("w1", 2)}
	_ = r.pool.Refresh(ctx)

	got := r.pool.TemplateSlices()
	if len(got) != 1 || len(got[0].Spec.Devices) != 2 {
		t.Fatalf("template slices = %+v", got)
	}
	s := got[0]
	if *s.Spec.NodeName != "template-pool" || s.Spec.Pool.Name != "template-pool" || s.Name != "template-pool-0" {
		t.Fatalf("not renamed for the template: name %q node %q pool %q", s.Name, *s.Spec.NodeName, s.Spec.Pool.Name)
	}
	if s.UID != "" || s.ResourceVersion != "" {
		t.Fatalf("server metadata kept: %+v", s.ObjectMeta)
	}
	if s.Spec.Driver != "gpu.example.com" || s.Spec.Pool.ResourceSliceCount != 1 {
		t.Fatalf("spec changed: %+v", s.Spec)
	}
}

func TestTemplateSlicesSurviveNodeOff(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{gpuSlice("w1", 2)}
	_ = r.pool.Refresh(ctx)
	delete(r.cluster.nodes, "w1")
	delete(r.cluster.slices, "w1")
	_ = r.pool.Refresh(ctx)
	if got := r.pool.TemplateSlices(); len(got) != 1 {
		t.Fatalf("snapshot lost when the node went away: %+v", got)
	}
}

func TestTemplateSlicesNotReplacedByEmpty(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{gpuSlice("w1", 2)}
	_ = r.pool.Refresh(ctx)
	r.cluster.slices["w1"] = nil // driver not up yet after a boot
	_ = r.pool.Refresh(ctx)
	if got := r.pool.TemplateSlices(); len(got) != 1 {
		t.Fatalf("an empty read replaced the snapshot: %+v", got)
	}
}

func TestTemplateSlicesNoneWhenNeverSeen(t *testing.T) {
	r := newRig("w1")
	_ = r.pool.Refresh(ctx)
	if got := r.pool.TemplateSlices(); len(got) != 0 {
		t.Fatalf("template slices without any snapshot: %+v", got)
	}
}

func TestTemplateSlicesUseFirstNodeWithSnapshot(t *testing.T) {
	r := newRig("w1", "w2")
	r.cluster.nodes["w2"] = &fakeNode{ready: true}
	r.cluster.slices["w2"] = []resourceapi.ResourceSlice{gpuSlice("w2", 1)}
	_ = r.pool.Refresh(ctx)
	if got := r.pool.TemplateSlices(); len(got) != 1 || len(got[0].Spec.Devices) != 1 {
		t.Fatalf("template = %+v, want w2's snapshot", got)
	}
}

func TestSnapshotsPersisted(t *testing.T) {
	r := newRig("w1")
	st := &memStore{}
	r.pool.UseSliceStore(st)
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{gpuSlice("w1", 2)}
	_ = r.pool.Refresh(ctx)
	_ = r.pool.Refresh(ctx) // unchanged: no second save
	if st.saves != 1 || len(st.saved["w1"]) != 1 {
		t.Fatalf("saves = %d, saved = %+v", st.saves, st.saved)
	}

	// A new pool (plugpool restarted, every node off) starts from the store.
	fresh := newRig("w1")
	if err := fresh.pool.LoadSlices(ctx, st); err != nil {
		t.Fatal(err)
	}
	if got := fresh.pool.TemplateSlices(); len(got) != 1 {
		t.Fatalf("snapshot not loaded from the store: %+v", got)
	}
}

func poolSlice(node, pool string, gen, count int64, devices ...string) resourceapi.ResourceSlice {
	s := gpuSlice(node, 0)
	s.Name = node + "-" + pool + "-" + string(rune('0'+gen))
	s.Spec.Pool = resourceapi.ResourcePool{Name: pool, Generation: gen, ResourceSliceCount: count}
	for _, d := range devices {
		s.Spec.Devices = append(s.Spec.Devices, resourceapi.Device{Name: d})
	}
	return s
}

func TestTemplateKeepsDistinctPoolsOfADriver(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{poolSlice("w1", "a", 1, 1, "x"), poolSlice("w1", "b", 1, 1, "y")}
	_ = r.pool.Refresh(ctx)
	got := r.pool.TemplateSlices()
	if len(got) != 2 || got[0].Spec.Pool.Name == got[1].Spec.Pool.Name {
		t.Fatalf("two pools of one driver must stay two pools: %+v", got)
	}
	for _, s := range got {
		if s.Spec.Pool.ResourceSliceCount != 1 {
			t.Fatalf("pool %s ResourceSliceCount = %d, want 1", s.Spec.Pool.Name, s.Spec.Pool.ResourceSliceCount)
		}
	}
}

func TestIncompletePublicationDoesNotReplaceSnapshot(t *testing.T) {
	r := newRig("w1")
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	full := []resourceapi.ResourceSlice{poolSlice("w1", "w1", 1, 2, "a"), poolSlice("w1", "w1", 1, 2, "b")}
	r.cluster.slices["w1"] = full
	_ = r.pool.Refresh(ctx)
	// The driver restarts and has published only one of two slices so far.
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{poolSlice("w1", "w1", 2, 2, "a")}
	_ = r.pool.Refresh(ctx)
	if got := r.pool.TemplateSlices(); len(got) != 2 {
		t.Fatalf("an incomplete publication replaced the snapshot: %d slices", len(got))
	}
}

type failingStore struct {
	memStore
	fail int
}

func (f *failingStore) Save(ctx context.Context, s map[string][]resourceapi.ResourceSlice) error {
	if f.fail > 0 {
		f.fail--
		return context.DeadlineExceeded
	}
	return f.memStore.Save(ctx, s)
}

func TestFailedSaveIsRetried(t *testing.T) {
	r := newRig("w1")
	st := &failingStore{fail: 1}
	r.pool.UseSliceStore(st)
	r.cluster.nodes["w1"] = &fakeNode{ready: true}
	r.cluster.slices["w1"] = []resourceapi.ResourceSlice{gpuSlice("w1", 2)}
	_ = r.pool.Refresh(ctx) // save fails
	_ = r.pool.Refresh(ctx) // unchanged snapshot: must retry
	if st.saves != 1 || len(st.saved["w1"]) != 1 {
		t.Fatalf("snapshot not saved after a failed save: saves=%d", st.saves)
	}
}
