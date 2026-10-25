# plugpool

A [cluster-autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
cloud provider for bare-metal machines on smart plugs. It treats a group of
machines like a cloud node pool with a minimum of zero: when pods are pending
that would fit, it switches a machine's plug on and the machine boots and joins
the cluster; when cluster-autoscaler finds a machine unneeded, it shuts the
machine down and switches the plug off.

plugpool speaks cluster-autoscaler's
[externalgrpc](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler/cloudprovider/externalgrpc)
protocol and drives [ESPHome](https://esphome.io) plugs through their web
server REST API. It runs as a sidecar in the cluster-autoscaler pod, so the
two talk over localhost and nothing is exposed on the network.

## Requirements

- Each machine boots by itself when power returns (a BIOS/firmware
  "power on after AC loss" setting) and joins the cluster at boot.
- Each machine is on its own ESPHome plug with the `web_server` component
  enabled; the plug's relay switch is an entity of the web server.
- Node `spec.providerID` is `<prefix><node name>` (k3s sets `k3s://<name>`;
  set `providerIDPrefix` for other distributions).
- The pod runs on a node that can reach the plugs and is never scaled down.

## How a machine is powered

- **Scale-up:** the plug of the first machine that is off goes on (a plug
  that fails is skipped for the next machine). The machine is "creating"
  until its Node registers. If it does not register within
  cluster-autoscaler's `--max-node-provision-time`, cluster-autoscaler
  lowers the target size again (or deletes the instance), and plugpool
  switches that plug off — after checking that the machine has not
  registered in the meantime.
- **Scale-down:** after cluster-autoscaler has drained the node, plugpool
  - starts a one-shot privileged pod on the node that runs
    `systemctl poweroff` in the host namespaces;
  - waits until the node is NotReady (at most `powerOff.notReadyTimeout`);
  - waits `powerOff.grace` more, because some machines hang at the very
    end of an ACPI power-off after everything is synced;
  - switches the plug off and deletes the Node object.

  A node that is already dead goes through the same steps without the
  poweroff pod.

Safety rules:

- plugpool only switches the plugs in its configuration.
- A plug goes off only for a node cluster-autoscaler asked to delete or to
  drop from the target size. A registered, Ready node without
  cluster-autoscaler's `ToBeDeletedByClusterAutoscaler` taint is never shut
  down or switched off, whatever the request: this is checked before the
  shutdown and again before the plug goes off.
- A node matches its instance by `spec.providerID`; by name only when the
  node has no providerID. A providerID of another form is logged as a
  configuration error.

State comes from the plugs and the Node objects on every refresh, so a
restart of plugpool loses nothing except a scale-down in progress. Plugs are
read in parallel with a short timeout; one that cannot be read keeps its
last known state and never fails the refresh (a failed refresh stops
cluster-autoscaler entirely). A plug never read yet is not used for
scale-up.

## Configuration

```yaml
listen: 127.0.0.1:8086         # gRPC address for cluster-autoscaler
providerIDPrefix: "k3s://"     # node spec.providerID = prefix + name
powerOff:
  namespace: plugpool          # where poweroff pods run (privileged)
  image: busybox:1.37.0        # must provide nsenter
  notReadyTimeout: 5m          # default
  grace: 2m                    # default
nodeGroups:
  - id: workers
    minSize: 0
    maxSize: 2                 # at most the number of nodes listed
    template:                  # a node of this group, for scale-up from zero
      cpu: "24"
      memory: 64Gi
      pods: "110"              # default
      labels: { example.com/pool: workers }
      taints: []
    nodes:
      - name: worker-1
        plug: { url: "http://plug-worker-1.example", switch: "Relay" }
```

Plug credentials come from the environment:
`PLUGPOOL_PLUG_USERNAME` and `PLUGPOOL_PLUG_PASSWORD`.

The template node is what cluster-autoscaler assumes a new machine offers;
give the group's pool label in `template.labels` and select it in the
workloads that should scale the pool.

## Deploy

[`deploy/examples`](deploy/examples) has a complete example: namespace,
RBAC (cluster-autoscaler's rules plus Node delete and the poweroff pods),
configuration and the Deployment with both containers. Set a `nodeSelector`
that keeps the pod off the machines it powers. Raise the externalgrpc
`grpc_timeout` (default 5s) as in the example.

## Limits

- No GPU or DRA devices in the template: a pending pod that needs a device
  does not trigger a scale-up.
- One replica. A scale-down in progress when the pod stops is not resumed;
  cluster-autoscaler then sees an unready node and deletes it again later.

## Development

```
make test     # go vet + go test -race
make lint     # golangci-lint
make proto    # regenerate internal/protos (generated code is committed)
make image
```

## License

Apache-2.0. `internal/protos/externalgrpc.proto` is from
[kubernetes/autoscaler](https://github.com/kubernetes/autoscaler)
(cluster-autoscaler-1.36.1), Copyright The Kubernetes Authors.
