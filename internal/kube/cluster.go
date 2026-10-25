// Package kube is plugpool's Kubernetes side: Node objects and the one-shot
// pod that shuts a node's machine down.
package kube

import (
	"context"
	"fmt"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/pool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	powerOffLabel = "app.kubernetes.io/name"
	powerOffApp   = "plugpool-poweroff"
	// deletionTaint is what cluster-autoscaler puts on a node it removes.
	deletionTaint = "ToBeDeletedByClusterAutoscaler"
)

// Cluster implements pool.Cluster with client-go.
type Cluster struct {
	cs       kubernetes.Interface
	powerOff config.PowerOff
	now      func() time.Time
}

// New returns a Cluster using cs; poweroff pods follow p.
func New(cs kubernetes.Interface, p config.PowerOff) *Cluster {
	return &Cluster{cs: cs, powerOff: p, now: time.Now}
}

// Node reads the Node: Ready condition, cluster-autoscaler's deletion taint
// and spec.providerID.
func (c *Cluster) Node(ctx context.Context, name string) (pool.NodeInfo, error) {
	n, err := c.cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return pool.NodeInfo{}, nil
	}
	if err != nil {
		return pool.NodeInfo{}, err
	}
	info := pool.NodeInfo{Exists: true, ProviderID: n.Spec.ProviderID}
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			info.Ready = cond.Status == corev1.ConditionTrue
		}
	}
	for _, taint := range n.Spec.Taints {
		if taint.Key == deletionTaint {
			info.ToBeDeleted = true
		}
	}
	return info, nil
}

// DeleteNode deletes the Node object; an absent node is fine.
func (c *Cluster) DeleteNode(ctx context.Context, name string) error {
	err := c.cs.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// powerOffPods lists the node's poweroff pods.
func (c *Cluster) powerOffPods(ctx context.Context, node string) ([]corev1.Pod, error) {
	l, err := c.cs.CoreV1().Pods(c.powerOff.Namespace).List(ctx, metav1.ListOptions{LabelSelector: powerOffLabel + "=" + powerOffApp})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range l.Items {
		if p.Spec.NodeName == node {
			out = append(out, p)
		}
	}
	return out, nil
}

// StartPowerOff creates a poweroff pod on the node: privileged, in the host
// PID namespace, `systemctl poweroff` through nsenter (the pattern kured uses
// for reboots). It is bound to the node directly, so the scheduler and the
// cluster-autoscaler deletion taint do not apply. An active (not finished,
// not terminating) poweroff pod on the node means it is already started.
// Each cycle's pod has its own name, so a finished pod from an earlier
// cycle never stands in for a new one.
func (c *Cluster) StartPowerOff(ctx context.Context, node string) error {
	pods, err := c.powerOffPods(ctx, node)
	if err != nil {
		return err
	}
	for _, p := range pods {
		finished := p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
		if !finished && p.DeletionTimestamp == nil {
			return nil
		}
	}
	privileged, automount := true, false
	// The pod is pointless once plugpool cuts the power; at least 5 minutes
	// so a slow shutdown is not killed.
	deadline := max(int64((c.powerOff.NotReadyTimeout.Duration + c.powerOff.Grace.Duration).Seconds()), 300)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   fmt.Sprintf("plugpool-poweroff-%s-%x", node, c.now().Unix()),
			Labels: map[string]string{powerOffLabel: powerOffApp},
		},
		Spec: corev1.PodSpec{
			NodeName:                     node,
			HostPID:                      true,
			RestartPolicy:                corev1.RestartPolicyNever,
			PriorityClassName:            "system-node-critical",
			ActiveDeadlineSeconds:        &deadline,
			AutomountServiceAccountToken: &automount,
			Tolerations:                  []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:            "poweroff",
				Image:           c.powerOff.Image,
				Command:         []string{"nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "systemctl", "poweroff"},
				SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
			}},
		},
	}
	_, err = c.cs.CoreV1().Pods(c.powerOff.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

// RemovePowerOff deletes the node's poweroff pods at once (the machine is
// already off), so none is left bound to the node when it boots again.
func (c *Cluster) RemovePowerOff(ctx context.Context, node string) error {
	pods, err := c.powerOffPods(ctx, node)
	if err != nil {
		return err
	}
	zero := int64(0)
	for _, p := range pods {
		err := c.cs.CoreV1().Pods(c.powerOff.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
