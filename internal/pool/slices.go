package pool

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sort"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SliceStore persists the per-node ResourceSlice snapshots, so a restarted
// plugpool still knows the devices of machines that are off.
type SliceStore interface {
	Load(ctx context.Context) (map[string][]resourceapi.ResourceSlice, error)
	Save(ctx context.Context, snapshots map[string][]resourceapi.ResourceSlice) error
}

// UseSliceStore makes Refresh save changed snapshots to s.
func (p *Pool) UseSliceStore(s SliceStore) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = s
}

// LoadSlices restores the snapshots of this pool's nodes from s and uses s
// from then on.
func (p *Pool) LoadSlices(ctx context.Context, s SliceStore) error {
	saved, err := s.Load(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = s
	for _, n := range p.group.Nodes {
		if sl := saved[n.Name]; len(sl) > 0 {
			p.snapshots[n.Name] = sl
		}
	}
	return nil
}

// TemplateSlices returns the DRA ResourceSlices a new machine of the group
// would publish: the snapshot of the first node (in config order) that has
// one, renamed to the template node and pool. Empty when no node of the
// group has published any. cluster-autoscaler assumes the machines of a group
// are alike; Refresh warns when the snapshots differ.
func (p *Pool) TemplateSlices() []resourceapi.ResourceSlice {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, n := range p.group.Nodes {
		snap := p.snapshots[n.Name]
		if len(snap) == 0 {
			continue
		}
		name := "template-" + p.group.ID
		out := make([]resourceapi.ResourceSlice, 0, len(snap))
		for i, s := range snap {
			c := s.DeepCopy()
			c.ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", name, i)}
			node := name
			c.Spec.NodeName = &node
			c.Spec.Pool.Name = name
			out = append(out, *c)
		}
		return out
	}
	return nil
}

// updateSnapshot records a node's slices (p.mu held). It reports whether the
// snapshot changed. An empty read never replaces a snapshot: the DRA driver
// may not be up yet after a boot.
func (p *Pool) updateSnapshot(node string, slices []resourceapi.ResourceSlice) bool {
	if len(slices) == 0 {
		return false
	}
	sort.Slice(slices, func(i, j int) bool { return slices[i].Name < slices[j].Name })
	old := p.snapshots[node]
	if len(old) == len(slices) {
		same := true
		for i := range old {
			if !reflect.DeepEqual(old[i].Spec, slices[i].Spec) {
				same = false
				break
			}
		}
		if same {
			return false
		}
	}
	p.snapshots[node] = slices
	return true
}

// warnIfUnalike logs when the group's snapshots offer different devices
// (p.mu held).
func (p *Pool) warnIfUnalike() {
	counts := map[string]string{}
	for node, snap := range p.snapshots {
		c := map[string]int{}
		for _, s := range snap {
			c[s.Spec.Driver] += len(s.Spec.Devices)
		}
		counts[node] = fmt.Sprint(c)
	}
	seen := map[string]bool{}
	for _, v := range counts {
		seen[v] = true
	}
	if len(seen) > 1 {
		slog.Warn("machines of the group publish different DRA devices; cluster-autoscaler assumes they are alike", "pool", p.group.ID, "devices", counts)
	}
}

func copySnapshots(m map[string][]resourceapi.ResourceSlice) map[string][]resourceapi.ResourceSlice {
	out := make(map[string][]resourceapi.ResourceSlice, len(m))
	for k, v := range m {
		out[k] = append([]resourceapi.ResourceSlice(nil), v...)
	}
	return out
}
