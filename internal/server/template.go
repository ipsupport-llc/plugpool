package server

import (
	"maps"

	"github.com/ipsupport-llc/plugpool/internal/config"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// templateNode is a just-started node of group g, for scale-up simulation.
// The quantities were validated when the config was loaded.
func templateNode(g config.NodeGroup) *corev1.Node {
	labels := map[string]string{
		"kubernetes.io/os":   "linux",
		"kubernetes.io/arch": "amd64",
	}
	maps.Copy(labels, g.Template.Labels)
	res := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(g.Template.CPU),
		corev1.ResourceMemory: resource.MustParse(g.Template.Memory),
		corev1.ResourcePods:   resource.MustParse(g.Template.Pods),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "template-" + g.ID, Labels: labels},
		Spec:       corev1.NodeSpec{Taints: g.Template.Taints},
		Status: corev1.NodeStatus{
			Capacity:    res,
			Allocatable: res.DeepCopy(),
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}
