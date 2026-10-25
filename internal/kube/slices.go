package kube

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/ipsupport-llc/plugpool/internal/pool"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sliceConfigMap holds the per-node ResourceSlice snapshots, one key per node.
const sliceConfigMap = "plugpool-template-slices"

// ResourceSlices returns the DRA ResourceSlices published for the node.
func (c *Cluster) ResourceSlices(ctx context.Context, node string) ([]resourceapi.ResourceSlice, error) {
	l, err := c.cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + node})
	if err != nil {
		return nil, err
	}
	// The field selector is served by the API server; filter again so a
	// client that ignores it (or an all-nodes slice) never leaks in.
	var out []resourceapi.ResourceSlice
	for _, s := range l.Items {
		if s.Spec.NodeName != nil && *s.Spec.NodeName == node {
			out = append(out, s)
		}
	}
	return out, nil
}

// SliceStore keeps the snapshots in ConfigMap plugpool-template-slices in the
// poweroff namespace (one JSON list per node; a save replaces only the nodes
// it is given).
func (c *Cluster) SliceStore() pool.SliceStore { return &sliceStore{c: c} }

type sliceStore struct{ c *Cluster }

func (s *sliceStore) Load(ctx context.Context) (map[string][]resourceapi.ResourceSlice, error) {
	cm, err := s.c.cs.CoreV1().ConfigMaps(s.c.powerOff.Namespace).Get(ctx, sliceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return map[string][]resourceapi.ResourceSlice{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string][]resourceapi.ResourceSlice{}
	for node, v := range cm.Data {
		var sl []resourceapi.ResourceSlice
		if err := json.Unmarshal([]byte(v), &sl); err != nil {
			slog.Warn("skipping unreadable resource slice snapshot", "configmap", sliceConfigMap, "node", node, "err", err)
			continue
		}
		out[node] = sl
	}
	return out, nil
}

func (s *sliceStore) Save(ctx context.Context, snapshots map[string][]resourceapi.ResourceSlice) error {
	data := map[string]string{}
	for node, sl := range snapshots {
		// Name and spec only: server metadata is noise and costs space.
		bare := make([]resourceapi.ResourceSlice, len(sl))
		for i, s := range sl {
			bare[i] = resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: s.Name}, Spec: s.Spec}
		}
		b, err := json.Marshal(bare)
		if err != nil {
			return err
		}
		data[node] = string(b)
	}
	cms := s.c.cs.CoreV1().ConfigMaps(s.c.powerOff.Namespace)
	cm, err := cms.Get(ctx, sliceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = cms.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: sliceConfigMap, Labels: map[string]string{"app.kubernetes.io/name": "plugpool"}},
			Data:       data,
		}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	// Only the given nodes' keys: several node groups share the ConfigMap.
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	for node, v := range data {
		cm.Data[node] = v
	}
	_, err = cms.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}
