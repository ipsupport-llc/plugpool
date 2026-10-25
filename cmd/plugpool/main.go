// Command plugpool is a cluster-autoscaler externalgrpc cloud provider that
// powers the machines of a node pool on and off through ESPHome smart plugs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipsupport-llc/plugpool/internal/config"
	"github.com/ipsupport-llc/plugpool/internal/kube"
	"github.com/ipsupport-llc/plugpool/internal/plug"
	"github.com/ipsupport-llc/plugpool/internal/pool"
	"github.com/ipsupport-llc/plugpool/internal/protos"
	"github.com/ipsupport-llc/plugpool/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "", "path to the configuration file (required)")
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig path; empty = in-cluster")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if *cfgPath == "" {
		slog.Error("--config is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, *cfgPath, *kubeconfig); err != nil {
		slog.Error("plugpool failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfgPath, kubeconfig string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	rc, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("kubernetes config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	cluster := kube.New(cs, cfg.PowerOff)
	user, pass := os.Getenv("PLUGPOOL_PLUG_USERNAME"), os.Getenv("PLUGPOOL_PLUG_PASSWORD")
	httpc := &http.Client{Timeout: 10 * time.Second}

	pools := map[string]*pool.Pool{}
	for _, g := range cfg.NodeGroups {
		switches := map[string]plug.Switch{}
		for _, n := range g.Nodes {
			switches[n.Name] = plug.NewESPHome(n.Plug.URL, n.Plug.Switch, user, pass, httpc)
		}
		p := pool.New(g, cfg.PowerOff, cfg.ProviderIDPrefix, switches, cluster, pool.RealClock{})
		// DRA devices of machines seen before, for templates while they are off.
		if err := p.LoadSlices(ctx, cluster.SliceStore()); err != nil {
			slog.Warn("loading resource slice snapshots failed", "group", g.ID, "err", err)
			p.UseSliceStore(cluster.SliceStore())
		}
		if err := p.Refresh(ctx); err != nil {
			slog.Warn("initial refresh failed", "group", g.ID, "err", err)
		}
		pools[g.ID] = p
	}

	lis, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	protos.RegisterCloudProviderServer(s, server.New(pools))
	healthpb.RegisterHealthServer(s, health.NewServer())
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(lis) }()
	slog.Info("plugpool serving", "version", version, "listen", cfg.Listen, "groups", len(pools))

	select {
	case err = <-errc:
	case <-ctx.Done():
		slog.Info("shutting down")
		s.GracefulStop()
	}
	for _, p := range pools {
		p.Stop()
	}
	return err
}
