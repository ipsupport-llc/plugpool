package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `
powerOff:
  namespace: plugpool
  image: busybox:1.37
nodeGroups:
  - id: pool
    minSize: 0
    maxSize: 2
    template: { cpu: "24", memory: 64Gi }
    nodes:
      - { name: worker-1, plug: { url: "http://plug-worker-1.example", switch: Relay } }
      - { name: worker-2, plug: { url: "http://plug-worker-2.example", switch: Relay } }
`

func write(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(write(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:8086" {
		t.Errorf("Listen = %q", c.Listen)
	}
	if c.ProviderIDPrefix != "k3s://" {
		t.Errorf("ProviderIDPrefix = %q", c.ProviderIDPrefix)
	}
	if c.PowerOff.NotReadyTimeout.Duration != 5*time.Minute || c.PowerOff.Grace.Duration != 2*time.Minute {
		t.Errorf("timeouts = %v / %v", c.PowerOff.NotReadyTimeout.Duration, c.PowerOff.Grace.Duration)
	}
	if c.NodeGroups[0].Template.Pods != "110" {
		t.Errorf("Pods = %q", c.NodeGroups[0].Template.Pods)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name, edit, field string
	}{
		{"no groups", "nodeGroups: []", "nodeGroups"},
		{"duplicate group", "dupgroup", "id"},
		{"duplicate node", "dupnode", "worker-1"},
		{"max above nodes", "maxSize: 3", "maxSize"},
		{"min above max", "minSize: 3", "minSize"},
		{"negative min", "minSize: -1", "minSize"},
		{"no plug url", `url: ""`, "url"},
		{"no switch", `switch: ""`, "switch"},
		{"bad cpu", `cpu: "lots"`, "cpu"},
		{"bad memory", "memory: big", "memory"},
		{"non http url", `url: "ftp://plug-worker-1.example"`, "url"},
		{"no namespace", "namespace: \"\"", "namespace"},
		{"no image", "image: \"\"", "image"},
		{"shared plug", "sharedplug", "same plug"},
		{"shared plug default port", "sharedport", "same plug"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mutate(minimal, tc.edit)
			_, err := Load(write(t, s))
			if err == nil {
				t.Fatalf("no error for %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("error %q does not name %q", err, tc.field)
			}
		})
	}
}

// mutate applies one edit to the minimal config: a "key: value" line
// replaces the first line with that key; the two dup* edits duplicate a
// group or a node.
func mutate(s, edit string) string {
	switch edit {
	case "dupgroup":
		i := strings.Index(s, "  - id: pool")
		return s + s[i:]
	case "dupnode":
		return strings.Replace(s, "worker-2, plug", "worker-1, plug", 1)
	case "sharedport":
		return strings.Replace(s, "http://plug-worker-2.example", "http://plug-worker-1.example:80", 1)
	case "sharedplug":
		return strings.Replace(s, "http://plug-worker-2.example", "http://PLUG-worker-1.example/", 1)
	case "nodeGroups: []":
		return s[:strings.Index(s, "nodeGroups:")] + edit + "\n"
	}
	key := strings.SplitN(edit, ":", 2)[0]
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		j := strings.Index(l, key+":")
		if j < 0 {
			continue
		}
		// replace "key: <value>" up to the next ',' or '}' or end of line
		rest := l[j:]
		end := len(rest)
		if k := strings.IndexAny(rest, ",}"); k >= 0 {
			end = k
		}
		lines[i] = l[:j] + edit + rest[end:]
		return strings.Join(lines, "\n")
	}
	panic("key not found: " + key)
}
