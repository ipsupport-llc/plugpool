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
		// Each original (driver, pool) keeps its own pool: the first pool of
		// a driver is named after the template node, further ones get a suffix.
		type poolKey struct{ driver, pool string }
		names := map[poolKey]string{}
		perDriver := map[string]int{}
		counts := map[poolKey]int64{}
		for _, s := range snap {
			k := poolKey{s.Spec.Driver, s.Spec.Pool.Name}
			counts[k]++
			if _, ok := names[k]; !ok {
				if n := perDriver[k.driver]; n == 0 {
					names[k] = name
				} else {
					names[k] = fmt.Sprintf("%s-%d", name, n)
				}
				perDriver[k.driver]++
			}
		}
		out := make([]resourceapi.ResourceSlice, 0, len(snap))
		for i, s := range snap {
			k := poolKey{s.Spec.Driver, s.Spec.Pool.Name}
			c := s.DeepCopy()
			c.ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", name, i)}
			node := name
			c.Spec.NodeName = &node
			c.Spec.Pool.Name = names[k]
			c.Spec.Pool.ResourceSliceCount = counts[k]
			out = append(out, *c)
		}
		return out
	}
	return nil
}

// updateSnapshot records a node's slices (p.mu held). It reports whether the
// snapshot changed. Only a complete publication replaces a snapshot: an empty
// read (the DRA driver not up yet after a boot) or a pool still being
// published (fewer slices than ResourceSliceCount at its newest generation)
// keeps the previous one.
func (p *Pool) updateSnapshot(node string, slices []resourceapi.ResourceSlice) bool {
	slices, complete := completePools(slices)
	if len(slices) == 0 || !complete {
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

// completePools keeps each (driver, pool)'s slices of its newest generation
// and reports whether every pool has all of its ResourceSliceCount slices.
func completePools(slices []resourceapi.ResourceSlice) ([]resourceapi.ResourceSlice, bool) {
	type poolKey struct{ driver, pool string }
	newest := map[poolKey]int64{}
	for _, s := range slices {
		k := poolKey{s.Spec.Driver, s.Spec.Pool.Name}
		if g, ok := newest[k]; !ok || s.Spec.Pool.Generation > g {
			newest[k] = s.Spec.Pool.Generation
		}
	}
	var out []resourceapi.ResourceSlice
	have := map[poolKey]int64{}
	want := map[poolKey]int64{}
	for _, s := range slices {
		k := poolKey{s.Spec.Driver, s.Spec.Pool.Name}
		if s.Spec.Pool.Generation != newest[k] {
			continue
		}
		out = append(out, *s.DeepCopy())
		have[k]++
		want[k] = s.Spec.Pool.ResourceSliceCount
	}
	for k, n := range want {
		if have[k] != n {
			return out, false
		}
	}
	return out, true
}

func copySnapshots(m map[string][]resourceapi.ResourceSlice) map[string][]resourceapi.ResourceSlice {
	out := make(map[string][]resourceapi.ResourceSlice, len(m))
	for k, v := range m {
		out[k] = append([]resourceapi.ResourceSlice(nil), v...)
	}
	return out
}
