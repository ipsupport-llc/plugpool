package kube

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/pool"
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

func TestNodeInfo(t *testing.T) {
	up := node("up", corev1.ConditionTrue)
	up.Spec.ProviderID = "k3s://up"
	up.Spec.Taints = []corev1.Taint{{Key: "ToBeDeletedByClusterAutoscaler", Effect: corev1.TaintEffectNoSchedule}}
	c, _ := newCluster(up, node("down", corev1.ConditionUnknown))
	for _, tc := range []struct {
		name string
		want pool.NodeInfo
	}{
		{"up", pool.NodeInfo{Exists: true, Ready: true, ToBeDeleted: true, ProviderID: "k3s://up"}},
		{"down", pool.NodeInfo{Exists: true}},
		{"gone", pool.NodeInfo{}},
	} {
		got, err := c.Node(ctx, tc.name)
		if err != nil || got != tc.want {
			t.Errorf("Node(%s) = %+v %v, want %+v", tc.name, got, err, tc.want)
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

func powerOffPods(t *testing.T, cs *fake.Clientset, node string) []corev1.Pod {
	t.Helper()
	l, err := cs.CoreV1().Pods("plugpool").List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=plugpool-poweroff"})
	if err != nil {
		t.Fatal(err)
	}
	var out []corev1.Pod
	for _, p := range l.Items {
		if p.Spec.NodeName == node {
			out = append(out, p)
		}
	}
	return out
}

func TestStartPowerOffPodSpec(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue))
	if err := c.StartPowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	pods := powerOffPods(t, cs, "w1")
	if len(pods) != 1 {
		t.Fatalf("%d poweroff pods", len(pods))
	}
	pod := pods[0]
	s := pod.Spec
	if !strings.HasPrefix(pod.Name, "plugpool-poweroff-w1-") || !s.HostPID || s.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("name %q hostPID %v restart %q", pod.Name, s.HostPID, s.RestartPolicy)
	}
	if len(s.Tolerations) != 1 || s.Tolerations[0].Operator != corev1.TolerationOpExists || s.Tolerations[0].Key != "" {
		t.Fatalf("tolerations = %v", s.Tolerations)
	}
	if s.ActiveDeadlineSeconds == nil || *s.ActiveDeadlineSeconds <= 0 || s.AutomountServiceAccountToken == nil || *s.AutomountServiceAccountToken {
		t.Fatalf("activeDeadlineSeconds %v automountServiceAccountToken %v", s.ActiveDeadlineSeconds, s.AutomountServiceAccountToken)
	}
	ctr := s.Containers[0]
	if ctr.SecurityContext == nil || ctr.SecurityContext.Privileged == nil || !*ctr.SecurityContext.Privileged {
		t.Fatal("container not privileged")
	}
	want := []string{"nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "systemctl", "poweroff"}
	if !slices.Equal(ctr.Command, want) || ctr.Image != "busybox:1.37" {
		t.Fatalf("command %v image %q", ctr.Command, ctr.Image)
	}
	if s.PriorityClassName != "system-node-critical" {
		t.Fatalf("priority %q", s.PriorityClassName)
	}
}

func TestStartPowerOffIdempotentWhileActive(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue))
	for range 2 {
		if err := c.StartPowerOff(ctx, "w1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(powerOffPods(t, cs, "w1")); n != 1 {
		t.Fatalf("%d poweroff pods, want 1", n)
	}
}

func TestStartPowerOffIgnoresFinishedPod(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue))
	if err := c.StartPowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	old := powerOffPods(t, cs, "w1")[0]
	old.Status.Phase = corev1.PodSucceeded
	if _, err := cs.CoreV1().Pods("plugpool").UpdateStatus(ctx, &old, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Unix(1_900_000_000, 0) }
	if err := c.StartPowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	active := 0
	for _, p := range powerOffPods(t, cs, "w1") {
		if p.Status.Phase != corev1.PodSucceeded {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("%d new poweroff pods next to a finished one, want 1", active)
	}
}

func TestRemovePowerOff(t *testing.T) {
	c, cs := newCluster(node("w1", corev1.ConditionTrue), node("w2", corev1.ConditionTrue))
	_ = c.StartPowerOff(ctx, "w1")
	_ = c.StartPowerOff(ctx, "w2")
	if err := c.RemovePowerOff(ctx, "w1"); err != nil {
		t.Fatal(err)
	}
	if len(powerOffPods(t, cs, "w1")) != 0 || len(powerOffPods(t, cs, "w2")) != 1 {
		t.Fatal("RemovePowerOff must delete exactly the node's poweroff pods")
	}
}
