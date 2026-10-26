package kube

import (
	"strings"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func slice(name, node string) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourceapi.ResourceSliceSpec{
			Driver: "gpu.example.com", NodeName: &node,
			Pool:    resourceapi.ResourcePool{Name: node, ResourceSliceCount: 1},
			Devices: []resourceapi.Device{{Name: "gpu-0"}},
		},
	}
}

func TestResourceSlicesOfNode(t *testing.T) {
	c, cs := newCluster()
	for _, s := range []*resourceapi.ResourceSlice{slice("a", "w1"), slice("b", "w2"), slice("c", "w1")} {
		if _, err := cs.ResourceV1().ResourceSlices().Create(ctx, s, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.ResourceSlices(ctx, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || *got[0].Spec.NodeName != "w1" || *got[1].Spec.NodeName != "w1" {
		t.Fatalf("slices of w1 = %+v", got)
	}
}

func TestSliceStoreRoundTrip(t *testing.T) {
	c, _ := newCluster()
	st := c.SliceStore()
	empty, err := st.Load(ctx)
	if err != nil || len(empty) != 0 {
		t.Fatalf("Load without a ConfigMap = %v, %v", empty, err)
	}
	want := map[string][]resourceapi.ResourceSlice{"w1": {*slice("a", "w1")}}
	if err := st.Save(ctx, want); err != nil { // creates
		t.Fatal(err)
	}
	want["w2"] = []resourceapi.ResourceSlice{*slice("b", "w2")}
	if err := st.Save(ctx, want); err != nil { // updates
		t.Fatal(err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["w2"][0].Name != "b" || *got["w1"][0].Spec.NodeName != "w1" {
		t.Fatalf("round trip = %+v", got)
	}
}

// Each node group saves its own nodes' snapshots; one group's save must not
// drop another's.
func TestSliceStoreSaveKeepsOtherNodes(t *testing.T) {
	c, _ := newCluster()
	st := c.SliceStore()
	if err := st.Save(ctx, map[string][]resourceapi.ResourceSlice{"w1": {*slice("a", "w1")}}); err != nil {
		t.Fatal(err)
	}
	if err := st.Save(ctx, map[string][]resourceapi.ResourceSlice{"w2": {*slice("b", "w2")}}); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("after saves by two groups: %d nodes, want 2", len(got))
	}
}

func TestSliceStoreSkipsCorruptKey(t *testing.T) {
	c, cs := newCluster()
	if err := c.SliceStore().Save(ctx, map[string][]resourceapi.ResourceSlice{"w1": {*slice("a", "w1")}}); err != nil {
		t.Fatal(err)
	}
	cm, _ := cs.CoreV1().ConfigMaps("plugpool").Get(ctx, "plugpool-template-slices", metav1.GetOptions{})
	cm.Data["w2"] = "{not json"
	if _, err := cs.CoreV1().ConfigMaps("plugpool").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := c.SliceStore().Load(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("Load with one corrupt key = %v, %v; want w1 only", got, err)
	}
}

func TestSliceStoreKeepsOnlyNameAndSpec(t *testing.T) {
	c, cs := newCluster()
	s := slice("a", "w1")
	s.UID, s.ResourceVersion = "u1", "9"
	s.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubelet"}}
	if err := c.SliceStore().Save(ctx, map[string][]resourceapi.ResourceSlice{"w1": {*s}}); err != nil {
		t.Fatal(err)
	}
	cm, _ := cs.CoreV1().ConfigMaps("plugpool").Get(ctx, "plugpool-template-slices", metav1.GetOptions{})
	for _, bad := range []string{"u1", "kubelet", "resourceVersion"} {
		if strings.Contains(cm.Data["w1"], bad) {
			t.Fatalf("stored metadata %q: %s", bad, cm.Data["w1"])
		}
	}
}
