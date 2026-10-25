package kube

import (
	"context"
	"slices"
	"testing"

	"github.com/ipsupport-llc/plugpool/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var ctx = context.Background()

func node(name string, ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}},
	}
}

func newCluster(objs ...*corev1.Node) (*Cluster, *fake.Clientset) {
	cs := fake.NewClientset()
	for _, o := range objs {
		_, _ = cs.CoreV1().Nodes().Create(ctx, o, metav1.CreateOptions{})
	}
	return New(cs, config.PowerOff{Namespace: "plugpool", Image: "busybox:1.37"}), cs
}

func TestNodeReadyAbsent(t *testing.T) {
	c, _ := newCluster(node("up", corev1.ConditionTrue), node("down", corev1.ConditionUnknown))
	for _, tc := range []struct {
		name          string
		exists, ready bool
	}{{"up", true, true}, {"down", true, false}, {"gone", false, false}} {
		exists, ready, err := c.Node(ctx, tc.name)
		if err != nil || exists != tc.exists || ready != tc.ready {
			t.Errorf("Node(%s) = %v %v %v, want %v %v", tc.name, exists, ready, err, tc.exists, tc.ready)
		}
	}
}

func TestDeleteNode(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionFalse))
	if err := c.DeleteNode(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().Nodes().Get(ctx, "w1", metav1.GetOptions{}); err == nil {
		t.Fatal("node still there")
	}
	if err := c.DeleteNode(ctx, "w1"); err != nil {
		t.Fatalf("deleting an absent node: %v", err)
	}
}

func TestStartPowerOffPodSpec(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue))
	if err := c.StartPowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	pod, err := cs.CoreV1().Pods("plugpool").Get(ctx, "plugpool-poweroff-w1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s := pod.Spec
	if s.NodeName != "w1" || !s.HostPID || s.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("nodeName %q hostPID %v restart %q", s.NodeName, s.HostPID, s.RestartPolicy)
	}
	if len(s.Tolerations) != 1 || s.Tolerations[0].Operator != corev1.TolerationOpExists || s.Tolerations[0].Key != "" {
		t.Fatalf("tolerations = %v", s.Tolerations)
	}
	ctr := s.Containers[0]
	if ctr.SecurityContext == nil || ctr.SecurityContext.Privileged == nil || !*ctr.SecurityContext.Privileged {
		t.Fatal("container not privileged")
	}
	want := []string{"nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "systemctl", "poweroff"}
	if !slices.Equal(ctr.Command, want) || ctr.Image != "busybox:1.37" {
		t.Fatalf("command %v image %q", ctr.Command, ctr.Image)
	}
	if s.PriorityClassName != "system-node-critical" || pod.Labels["app.kubernetes.io/name"] != "plugpool-poweroff" {
		t.Fatalf("priority %q labels %v", s.PriorityClassName, pod.Labels)
	}
}

func TestStartPowerOffIdempotent(t *testing.T) {
	c, _ := newCluster(node("w1", corev1.ConditionTrue))
	for range 2 {
		if err := c.StartPowerOff(ctx, "w1"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStartPowerOffReplacesFinishedPod(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue))
	old := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "plugpool-poweroff-w1", Namespace: "plugpool", UID: "old"},
		Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	if _, err := cs.CoreV1().Pods("plugpool").Create(ctx, old, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.StartPowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	pod, err := cs.CoreV1().Pods("plugpool").Get(ctx, "plugpool-poweroff-w1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pod.UID == "old" || pod.Spec.NodeName != "w1" {
		t.Fatal("a finished poweroff pod from an earlier cycle was not replaced")
	}
}
