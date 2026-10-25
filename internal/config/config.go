// Package config holds plugpool's configuration: the node groups, the
// machines in them and the smart plug that powers each machine.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// Config is the whole configuration file.
type Config struct {
	// Listen is the gRPC address cluster-autoscaler connects to.
	Listen string `json:"listen"`
	// ProviderIDPrefix + node name is a node's spec.providerID, which is
	// also its instance ID towards cluster-autoscaler.
	ProviderIDPrefix string      `json:"providerIDPrefix"`
	PowerOff         PowerOff    `json:"powerOff"`
	NodeGroups       []NodeGroup `json:"nodeGroups"`
}

// PowerOff configures how a node is shut down before its plug is switched off.
type PowerOff struct {
	// Namespace the one-shot poweroff pods run in (must admit privileged pods).
	Namespace string `json:"namespace"`
	// Image of the poweroff pod; it must provide nsenter.
	Image string `json:"image"`
	// NotReadyTimeout bounds the wait for the node to go NotReady.
	NotReadyTimeout metav1.Duration `json:"notReadyTimeout"`
	// Grace is the wait after NotReady before the plug is switched off.
	Grace metav1.Duration `json:"grace"`
}

// NodeGroup is one cluster-autoscaler node group: machines of one kind.
type NodeGroup struct {
	ID       string   `json:"id"`
	MinSize  int      `json:"minSize"`
	MaxSize  int      `json:"maxSize"`
	Template Template `json:"template"`
	Nodes    []Node   `json:"nodes"`
}

// Template describes a node of the group for scale-up from zero.
type Template struct {
	CPU    string            `json:"cpu"`
	Memory string            `json:"memory"`
	Pods   string            `json:"pods"`
	Labels map[string]string `json:"labels"`
	Taints []corev1.Taint    `json:"taints"`
}

// Node is one machine: its Kubernetes node name and its plug.
type Node struct {
	Name string `json:"name"`
	Plug Plug   `json:"plug"`
}

// Plug is an ESPHome smart plug: its web server URL and the switch entity
// that drives the relay.
type Plug struct {
	URL    string `json:"url"`
	Switch string `json:"switch"`
}

// Load reads the file at path, applies defaults and validates it.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.UnmarshalStrict(b, c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.defaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) defaults() {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8086"
	}
	if c.ProviderIDPrefix == "" {
		c.ProviderIDPrefix = "k3s://"
	}
	if c.PowerOff.NotReadyTimeout.Duration == 0 {
		c.PowerOff.NotReadyTimeout.Duration = 5 * time.Minute
	}
	if c.PowerOff.Grace.Duration == 0 {
		c.PowerOff.Grace.Duration = 2 * time.Minute
	}
	for i := range c.NodeGroups {
		if c.NodeGroups[i].Template.Pods == "" {
			c.NodeGroups[i].Template.Pods = "110"
		}
	}
}

// Validate reports every problem in the configuration.
func (c *Config) Validate() error {
	var errs []error
	if c.PowerOff.Namespace == "" {
		errs = append(errs, errors.New("powerOff.namespace is required"))
	}
	if c.PowerOff.Image == "" {
		errs = append(errs, errors.New("powerOff.image is required"))
	}
	if len(c.NodeGroups) == 0 {
		errs = append(errs, errors.New("nodeGroups: at least one group is required"))
	}
	groups := map[string]bool{}
	nodes := map[string]bool{}
	plugs := map[string]string{} // normalized plug URL + switch => node
	for _, g := range c.NodeGroups {
		p := fmt.Sprintf("nodeGroups[%s]", g.ID)
		if g.ID == "" {
			errs = append(errs, errors.New("nodeGroups: id is required"))
		}
		if groups[g.ID] {
			errs = append(errs, fmt.Errorf("%s: duplicate id", p))
		}
		groups[g.ID] = true
		if g.MinSize < 0 || g.MinSize > g.MaxSize {
			errs = append(errs, fmt.Errorf("%s: minSize %d must be between 0 and maxSize %d", p, g.MinSize, g.MaxSize))
		}
		if g.MaxSize > len(g.Nodes) {
			errs = append(errs, fmt.Errorf("%s: maxSize %d exceeds the %d nodes", p, g.MaxSize, len(g.Nodes)))
		}
		for _, q := range []struct{ name, v string }{{"cpu", g.Template.CPU}, {"memory", g.Template.Memory}, {"pods", g.Template.Pods}} {
			if _, err := resource.ParseQuantity(q.v); err != nil {
				errs = append(errs, fmt.Errorf("%s: template.%s %q is not a quantity", p, q.name, q.v))
			}
		}
		for _, n := range g.Nodes {
			if n.Name == "" {
				errs = append(errs, fmt.Errorf("%s: node name is required", p))
			}
			if nodes[n.Name] {
				errs = append(errs, fmt.Errorf("%s: node %s is listed twice", p, n.Name))
			}
			nodes[n.Name] = true
			u, err := url.Parse(n.Plug.URL)
			if n.Plug.URL == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				errs = append(errs, fmt.Errorf("%s: node %s: plug.url %q must be an http(s) URL", p, n.Name, n.Plug.URL))
			}
			if n.Plug.Switch == "" {
				errs = append(errs, fmt.Errorf("%s: node %s: plug.switch is required", p, n.Name))
			}
			// One plug powers one machine: switching it off for one node
			// must never cut another.
			if err == nil {
				port := u.Port()
				if port == "" {
					port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
				}
				key := strings.ToLower(u.Scheme+"://"+u.Hostname()) + ":" + port + strings.TrimSuffix(u.Path, "/") + "#" + n.Plug.Switch
				if other, dup := plugs[key]; dup {
					errs = append(errs, fmt.Errorf("%s: nodes %s and %s use the same plug", p, other, n.Name))
				}
				plugs[key] = n.Name
			}
		}
	}
	return errors.Join(errs...)
}
