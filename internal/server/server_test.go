package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/plug"
	"github.com/ipsupport-llc/plugpool/internal/pool"
	"github.com/ipsupport-llc/plugpool/internal/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var ctx = context.Background()

type sw struct {
	mu sync.Mutex
	on bool
}

func (s *sw) State(context.Context) (bool, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.on, nil }
func (s *sw) Set(_ context.Context, on bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.on = on
	return nil
}

type cluster struct {
	mu    sync.Mutex
	nodes map[string]bool // name -> ready
}

func (c *cluster) Node(_ context.Context, n string) (bool, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.nodes[n]
	return ok, r, nil
}
func (c *cluster) DeleteNode(_ context.Context, n string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, n)
	return nil
}
func (c *cluster) StartPowerOff(_ context.Context, n string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[n] = false
	return nil
}

type instantClock struct{}

func (instantClock) Now() time.Time                                { return time.Unix(0, 0) }
func (instantClock) Sleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

type env struct {
	client protos.CloudProviderClient
	pool   *pool.Pool
	plugs  map[string]*sw
	nodes  *cluster
}

func setup(t *testing.T) *env {
	t.Helper()
	g := config.NodeGroup{
		ID: "pool", MinSize: 0, MaxSize: 2,
		Template: config.Template{CPU: "24", Memory: "64Gi", Pods: "110", Labels: map[string]string{"example.com/pool": "pool"}},
		Nodes: []config.Node{
			{Name: "w1", Plug: config.Plug{URL: "http://plug-w1.example", Switch: "Relay"}},
			{Name: "w2", Plug: config.Plug{URL: "http://plug-w2.example", Switch: "Relay"}},
		},
	}
	e := &env{plugs: map[string]*sw{"w1": {}, "w2": {}}, nodes: &cluster{nodes: map[string]bool{}}}
	switches := map[string]plug.Switch{"w1": e.plugs["w1"], "w2": e.plugs["w2"]}
	e.pool = pool.New(g, config.PowerOff{Namespace: "plugpool", Image: "busybox"}, "k3s://", switches, e.nodes, instantClock{})

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	protos.RegisterCloudProviderServer(s, New(map[string]*pool.Pool{"pool": e.pool}))
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	e.client = protos.NewCloudProviderClient(conn)
	return e
}

func TestNodeGroups(t *testing.T) {
	e := setup(t)
	r, err := e.client.NodeGroups(ctx, &protos.NodeGroupsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.NodeGroups) != 1 || r.NodeGroups[0].Id != "pool" || r.NodeGroups[0].MinSize != 0 || r.NodeGroups[0].MaxSize != 2 {
		t.Fatalf("NodeGroups = %v", r.NodeGroups)
	}
}

func TestNodeGroupForNode(t *testing.T) {
	e := setup(t)
	r, err := e.client.NodeGroupForNode(ctx, &protos.NodeGroupForNodeRequest{Node: &protos.ExternalGrpcNode{ProviderID: "k3s://w1", Name: "w1"}})
	if err != nil || r.NodeGroup.GetId() != "pool" {
		t.Fatalf("own node: %v %v", r, err)
	}
	r, err = e.client.NodeGroupForNode(ctx, &protos.NodeGroupForNodeRequest{Node: &protos.ExternalGrpcNode{ProviderID: "k3s://cp-1", Name: "cp-1"}})
	if err != nil || r.NodeGroup.GetId() != "" {
		t.Fatalf("foreign node must map to an empty id: %v %v", r, err)
	}
}

func TestIncreaseSizeAndNodes(t *testing.T) {
	e := setup(t)
	e.nodes.nodes["w2"] = true
	if _, err := e.client.Refresh(ctx, &protos.RefreshRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.NodeGroupIncreaseSize(ctx, &protos.NodeGroupIncreaseSizeRequest{Id: "pool", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	if !e.plugs["w1"].on {
		t.Fatal("w1 plug not switched on")
	}
	r, err := e.client.NodeGroupNodes(ctx, &protos.NodeGroupNodesRequest{Id: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]protos.InstanceStatus_InstanceState{}
	for _, i := range r.Instances {
		got[i.Id] = i.Status.InstanceState
	}
	if got["k3s://w1"] != protos.InstanceStatus_instanceCreating || got["k3s://w2"] != protos.InstanceStatus_instanceRunning {
		t.Fatalf("instances = %v", got)
	}
	ts, err := e.client.NodeGroupTargetSize(ctx, &protos.NodeGroupTargetSizeRequest{Id: "pool"})
	if err != nil || ts.TargetSize != 2 {
		t.Fatalf("TargetSize = %v %v", ts, err)
	}
}

func TestDeleteNodesMapsProviderIDs(t *testing.T) {
	e := setup(t)
	e.plugs["w1"].on, e.nodes.nodes["w1"] = true, true
	_, _ = e.client.Refresh(ctx, &protos.RefreshRequest{})
	_, err := e.client.NodeGroupDeleteNodes(ctx, &protos.NodeGroupDeleteNodesRequest{Id: "pool", Nodes: []*protos.ExternalGrpcNode{{ProviderID: "k3s://w1"}}})
	if err != nil {
		t.Fatal(err)
	}
	e.pool.Wait()
	if e.plugs["w1"].on {
		t.Fatal("w1 plug still on")
	}
	if _, err := e.client.NodeGroupDeleteNodes(ctx, &protos.NodeGroupDeleteNodesRequest{Id: "pool", Nodes: []*protos.ExternalGrpcNode{{ProviderID: "k3s://cp-1", Name: "cp-1"}}}); err == nil {
		t.Fatal("deleting a node outside the group: no error")
	}
}

func TestDecreaseTargetSize(t *testing.T) {
	e := setup(t)
	_, _ = e.client.Refresh(ctx, &protos.RefreshRequest{})
	_, _ = e.client.NodeGroupIncreaseSize(ctx, &protos.NodeGroupIncreaseSizeRequest{Id: "pool", Delta: 1})
	if _, err := e.client.NodeGroupDecreaseTargetSize(ctx, &protos.NodeGroupDecreaseTargetSizeRequest{Id: "pool", Delta: -1}); err != nil {
		t.Fatal(err)
	}
	if e.plugs["w1"].on {
		t.Fatal("w1 plug still on")
	}
}

func TestUnknownGroupNotFound(t *testing.T) {
	e := setup(t)
	_, err := e.client.NodeGroupTargetSize(ctx, &protos.NodeGroupTargetSizeRequest{Id: "nope"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
}

func TestTemplateNode(t *testing.T) {
	e := setup(t)
	r, err := e.client.NodeGroupTemplateNodeInfo(ctx, &protos.NodeGroupTemplateNodeInfoRequest{Id: "pool"})
	if err != nil {
		t.Fatal(err)
	}
	n := &corev1.Node{}
	if err := n.Unmarshal(r.NodeBytes); err != nil {
		t.Fatal(err)
	}
	if !n.Status.Allocatable.Cpu().Equal(resource.MustParse("24")) || !n.Status.Allocatable.Memory().Equal(resource.MustParse("64Gi")) || !n.Status.Capacity.Pods().Equal(resource.MustParse("110")) {
		t.Fatalf("allocatable = %v capacity = %v", n.Status.Allocatable, n.Status.Capacity)
	}
	if n.Labels["example.com/pool"] != "pool" || n.Labels["kubernetes.io/os"] != "linux" || n.Labels["kubernetes.io/arch"] != "amd64" {
		t.Fatalf("labels = %v", n.Labels)
	}
	ready := false
	for _, c := range n.Status.Conditions {
		ready = ready || (c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue)
	}
	if !ready || n.Name != "template-pool" {
		t.Fatalf("name %q conditions %v", n.Name, n.Status.Conditions)
	}
}

func TestOptionalRPCs(t *testing.T) {
	e := setup(t)
	if _, err := e.client.PricingNodePrice(ctx, &protos.PricingNodePriceRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("PricingNodePrice err = %v", err)
	}
	if _, err := e.client.NodeGroupGetOptions(ctx, &protos.NodeGroupAutoscalingOptionsRequest{Id: "pool"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("NodeGroupGetOptions err = %v", err)
	}
	if r, err := e.client.GPULabel(ctx, &protos.GPULabelRequest{}); err != nil || r.Label != "" {
		t.Fatalf("GPULabel = %v %v", r, err)
	}
	if r, err := e.client.GetAvailableGPUTypes(ctx, &protos.GetAvailableGPUTypesRequest{}); err != nil || len(r.GpuTypes) != 0 {
		t.Fatalf("GetAvailableGPUTypes = %v %v", r, err)
	}
}
