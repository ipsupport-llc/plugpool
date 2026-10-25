// Package kube is plugpool's Kubernetes side: Node objects and the one-shot
// pod that shuts a node's machine down.
package kube

import (
	"context"
	"fmt"

	"github.com/ipsupport-llc/plugpool/internal/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Cluster implements pool.Cluster with client-go.
type Cluster struct {
	cs       kubernetes.Interface
	powerOff config.PowerOff
}

// New returns a Cluster using cs; poweroff pods follow p.
func New(cs kubernetes.Interface, p config.PowerOff) *Cluster {
	return &Cluster{cs: cs, powerOff: p}
}

// Node reports whether the Node exists and its Ready condition is True.
func (c *Cluster) Node(ctx context.Context, name string) (exists, ready bool, err error) {
	n, err := c.cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	for _, cond := range n.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return true, cond.Status == corev1.ConditionTrue, nil
		}
	}
	return true, false, nil
}

// DeleteNode deletes the Node object; an absent node is fine.
func (c *Cluster) DeleteNode(ctx context.Context, name string) error {
	err := c.cs.CoreV1().Nodes().Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// StartPowerOff creates the poweroff pod on the node: privileged, in the host
// PID namespace, `systemctl poweroff` through nsenter (the pattern kured uses
// for reboots). It is bound to the node directly, so the scheduler and the
// cluster-autoscaler deletion taint do not apply. A Pending or Running pod of
// that name means it is already started; a finished one, left from an earlier
// cycle, is replaced.
func (c *Cluster) StartPowerOff(ctx context.Context, node string) error {
	pods := c.cs.CoreV1().Pods(c.powerOff.Namespace)
	name := "plugpool-poweroff-" + node
	old, err := pods.Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil && (old.Status.Phase == corev1.PodSucceeded || old.Status.Phase == corev1.PodFailed):
		if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete finished poweroff pod %s: %w", name, err)
		}
	case err == nil:
		return nil
	case !apierrors.IsNotFound(err):
		return err
	}
	privileged := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"app.kubernetes.io/name": "plugpool-poweroff"},
		},
		Spec: corev1.PodSpec{
			NodeName:          node,
			HostPID:           true,
			RestartPolicy:     corev1.RestartPolicyNever,
			PriorityClassName: "system-node-critical",
			Tolerations:       []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{{
				Name:            "poweroff",
				Image:           c.powerOff.Image,
				Command:         []string{"nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "systemctl", "poweroff"},
				SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
			}},
		},
	}
	_, err = pods.Create(ctx, pod, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}
