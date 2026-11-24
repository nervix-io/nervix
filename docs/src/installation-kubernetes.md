# Kubernetes Operator

The [Nervix Kubernetes Operator](https://github.com/nervix-io/nervix-k8s-operator) watches
namespaced `NervixCluster` resources across a Kubernetes cluster. For each resource, it creates the
services, StatefulSet, one persistent volume claim per Nervix node, and the certificates its nodes
authenticate each other with. It also coordinates one-time initialization of the `default` user and
of the cluster parameters a new cluster requests before scaling the StatefulSet to the requested
replica count.

## Prerequisites

Before installing the operator, ensure that:

- `kubectl` and Helm 3 can reach the Kubernetes cluster.
- The cluster has a default `StorageClass` that can provision persistent volumes.
- Your Kubernetes identity can create custom resource definitions and cluster-wide RBAC resources.
- Cluster nodes can pull images from `ghcr.io`.

## Install The Operator With Helm

The Helm chart is currently distributed in the operator repository rather than as a versioned chart
release. Clone the repository and install its bundled chart:

```bash
git clone --depth 1 https://github.com/nervix-io/nervix-k8s-operator.git

helm upgrade --install nervix-k8s-operator \
  ./nervix-k8s-operator/charts/nervix-k8s-operator \
  --namespace nervix-system \
  --create-namespace
```

Wait for the operator to become available:

```bash
kubectl --namespace nervix-system rollout status \
  deployment/nervix-k8s-operator
```

The chart installs the `NervixCluster` custom resource definition and deploys one operator that can
manage Nervix clusters in any namespace.

## Create The Initial User Secret

Create a namespace and a Secret containing the initial password for the `default` user. Use a
password file that contains the exact password without a trailing newline, and do not commit that
file:

```bash
kubectl create namespace nervix

kubectl --namespace nervix create secret generic nervix-initial-password \
  --from-file=password=/secure/path/to/password
```

The operator exposes this credential only to the first bootstrap pod. After authentication proves
that Nervix committed the user, the operator commits the requested
[cluster parameters](#cluster-parameters), restarts that pod without the credential and then scales
the cluster to the requested replica count.

## Create A Nervix Cluster

Save the following resource as `nervix-cluster.yaml`:

```yaml
apiVersion: nervix.io/v1alpha1
kind: NervixCluster
metadata:
  name: nervix
  namespace: nervix
spec:
  image: ghcr.io/nervix-io/nervix:debian-latest
  replicas: 3
  clusterId: nervix-kube
  initialClusterParameters:
    replicaCount: 2
  initialDefaultUserPasswordSecretRef:
    name: nervix-initial-password
    key: password
  storage: 20Gi
  resources:
    cpuRequest: 250m
    memoryRequest: 512Mi
    memoryLimit: 3Gi
```

The example spells out the operator's defaults for each node's volume and container resources.
Each node checks its volume and its memory before it starts, and refuses to start when they cannot
hold its configuration; [Node Sizing](#node-sizing) explains what a node derives from these values
and what the defaults hold.

Apply it and watch the cluster become ready:

```bash
kubectl apply --filename nervix-cluster.yaml
kubectl --namespace nervix get nervixcluster nervix --watch
```

The `READY` count should eventually match `REPLICAS`. Inspect the objects owned by the operator with:

```bash
kubectl --namespace nervix get pods,services,persistentvolumeclaims
```

One operator can manage multiple Nervix clusters. Create each cluster as a separate
`NervixCluster`, usually in a separate namespace or with a distinct name in the same namespace.

## Node Sizing

The operator gives each node's container the CPU and memory requests `spec.resources.cpuRequest`
and `spec.resources.memoryRequest`, `250m` and `512Mi` by default, and the memory limit
`spec.resources.memoryLimit`, `3Gi` by default. It sets no CPU limit. A node sizes itself from the
limits of its container, as [Sizing A Node](sizing.md#the-two-inputs) describes:

- Its memory limit is `memoryLimit`. Every memory budget, its client credit and its
  memory-pressure watermarks derive from it; the memory request, which Kubernetes schedules the
  pod by, does not change them.
- Its CPU count is every CPU the pod may run on, normally all of the Kubernetes node's CPUs, because
  a request sets no CPU quota. A CPU limit, from a LimitRange for example, becomes the node's CPU
  count, rounded down to whole CPUs and at least one.

The operator's startup command passes only each node's identity, addresses, interconnect TLS files,
database path and bootstrap options, plus the log filter `spec.logFilter` gives as `RUST_LOG`, and
`NervixCluster` has no field for any node setting. Every node therefore runs with the defaults its
settings declare and the capacity it detects.
`DESCRIBE CLUSTER` and `SHOW NODES` report each node's committed CPU count and memory.

With the default 3 GiB limit, a node derives 1,435 MiB of execution budgets, 192 MiB of client
credit and 64 MiB of database caches, 1,691 MiB that its startup check requires the limit to hold,
and pauses its ingestors at 2,688 MiB of allocated memory. These are ceilings rather than
allocations; leave room beside them for graph state, engine allocations and stacks. A memory limit
that cannot hold a node's budgets refuses that pod's start.

The volume, `spec.storage`, must be at least as large as each resource quota, 4 GiB for a resource
archive and 16 GiB for its extracted files by default, and the node's restore-staging quota follows
the volume. The default 20 GiB holds both, and a volume below a quota refuses the pod's start.
[Capacity Planning](capacity-planning.md#what-a-node-needs-to-start) gives the rules.

## Cluster Parameters

Cluster parameters, such as the replica count, are policy the whole cluster shares. The operator
passes no cluster parameter option to any node. Restarting a pod, recreating it on its persistent
volume, rolling the StatefulSet and scaling it therefore change no parameter. The shipped
three-node example requests two replicas, so each piece of runtime state is replicated to both
other nodes. Parameters left out of the request keep the server's defaults.

To start a new cluster with other values, request them in `spec.initialClusterParameters`:

```yaml
spec:
  initialDefaultUserPasswordSecretRef:
    name: nervix-initial-password
    key: password
  initialClusterParameters:
    replicaCount: 2
```

`replicaCount` is `REPLICA_COUNT`: how many nodes besides its owner hold a replica of each piece of
runtime state. The operator commits the request while it initializes the cluster, after the
bootstrap pod accepts the initial password and before the operator removes it. Running the bootstrap
pod's own `nervix-cli`, it reads the committed values with `DESCRIBE CLUSTER` and commits every
requested value that differs with one `ALTER CLUSTER SET`. It decides the outcome from the committed
values it reads back, so an initialization that resumes after the bootstrap pod or the operator
restarted commits each value once. The cluster stays initializing until the request is committed,
and reports any refusal. The operator's ClusterRole lets it exec into pods for this.

`status.initialClusterParameters` records the values the operator committed. The
`InitialClusterParametersCommitted` condition reports on the request: `Pending`, or `Uncommitted`
with the refusal, while the cluster initializes; `Committed` once the request is committed;
`ChangedAfterInitialization` when the request changes afterwards; and `InitializationNotRequested`
for a cluster without `initialDefaultUserPasswordSecretRef`, which the operator does not initialize.
The operator never changes an initialized cluster's parameters. Change them with `ALTER CLUSTER SET`,
which `DESCRIBE CLUSTER` then reports with each node's running values.
[Cluster Parameters](sizing.md#cluster-parameters) lists every parameter with its default and
bounds.

## Interconnect Certificates

Nodes authenticate all node-to-node traffic with mutual TLS, as
[Peer Identity And Authentication](interconnect.md#peer-identity-and-authentication) describes. The
operator generates a certificate authority for each `NervixCluster` and keeps it in the
`<name>-interconnect-ca` Secret, which only the operator reads. It issues every node a certificate
signed by that authority into the `<name>-interconnect-tls` Secret, which the pods mount, and issues
a node's certificate again 30 days before it expires. Nodes reload changed certificates without a
restart.

Deleting the `<name>-interconnect-ca` Secret makes the operator generate a new authority and issue
every node a new certificate. Until every node has reloaded its credentials, nodes that trust
different authorities refuse each other's connections.

## Enable Local Access

For development clusters that need direct access from outside Kubernetes, add this to the
`NervixCluster` spec:

```yaml
  localAccess:
    enabled: true
```

Apply the updated resource:

```bash
kubectl apply --filename nervix-cluster.yaml
```

The default NodePorts are `31390` for the gRPC entry point, `31420` for the web console, and `31090`
for health and metrics. The operator also creates per-node NodePorts so clients redirected to the
leader can reach it.

Connect the CLI to the gRPC entry point:

```bash
nervix-cli --server http://<node-address>:31390
```

The [web console](client-tools-web-console.md) is available at
`http://<node-address>:31420/console/`. Omit `localAccess` for an in-cluster-only deployment.

## Shutdown And Grace Periods

Deleting a pod, draining a Kubernetes node, or rolling the StatefulSet sends `SIGTERM`, which starts
graceful shutdown: the node saves what it holds, moves its units to the other nodes within its
shutdown timeout, and tears down. The operator starts every node with `--shutdown-timeout 600s` and
sets `terminationGracePeriodSeconds: 720`, the shutdown timeout and two more minutes for the save
and the teardown, so the kubelet's `SIGKILL` does not cut a normal shutdown short. A node whose
units did not all move within the shutdown timeout logs a cluster disruption and exits with status
`1`; a node whose save never ends is killed when the grace period ends. If you change the shutdown
timeout, change the grace period with it.

Note that `/readyz` reports whether the node has finished initializing and currently knows a Raft
leader; it does not turn negative when a node begins terminating. A stopping node is therefore not
removed from Service endpoints by readiness alone. It stops accepting new client connections itself
in the first shutdown phase, and then takes part in nothing but saving and moving its own work.

A terminating node saves what it holds, then moves its units to a live peer when one is
available; when none is, its units stay saved on its volume and its restarted pod restores them.
Rolling one pod at a time keeps a peer available to receive that work, so ownership moves instead
of waiting for the pod to come back. See [Shutdown And Recovery](shutdown.md) for the phases, what
the save keeps and cuts, and what each ending preserves.

## Rolling Updates

A change to `spec.image` or `spec.resources` changes the pods' template, and Kubernetes rolls the
StatefulSet: it restarts one pod at a time, from the highest ordinal, and waits for each restarted
pod to be ready before the next. A pod is ready when its `/readyz` answers 200, once its node has
installed the cluster's committed parameters and knows a leader, so the roll moves on only after
the restarted node has rejoined. [Rolling Changes](sizing.md#rolling-changes) gives the guarantees,
and [Reading DESCRIBE CLUSTER During A Roll](sizing.md#reading-describe-cluster-during-a-roll) shows
how to follow each step.

- **Cluster parameters.** The pods carry no option that seeds a parameter, so a roll changes no
  parameter, and every restarted node installs the committed record before it becomes ready. Run
  `ALTER CLUSTER SET` at any time, during a roll included.
- **Node capabilities.** A new `memoryLimit` gives each restarted node a new memory claim and new
  budgets, while the pods not yet restarted keep theirs; nodes run correctly with different claims,
  and `DESCRIBE CLUSTER` shows each claim committed for its node's new incarnation. Before lowering
  `memoryLimit`, check that the smaller limit still holds a node's budgets: a pod that cannot hold
  them refuses to start, and the roll waits on it.
- **Prerequisites.** Every pod keeps the default Raft heartbeat, gossip interval and clock-skew
  tolerance, the cluster identity `spec.clusterId` gives, or the resource's name without it, and the
  operator's certificate authority, so a roll moves no prerequisite that a node setting gives. The
  node-to-node wire contract, the consensus floors and the durability promise come from the build.
  When a new image differs from the running one in any of them, the first restarted pod is refused
  as it joins its peers, which still run the old image, and exits with status `1`. Kubernetes
  restarts it without it becoming ready, so the roll stops there while the other pods keep running.
  Revert `spec.image`, then delete that pod so the StatefulSet recreates it from the reverted
  template, as Kubernetes describes for a
  [forced rollback](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#forced-rollback).

## Production Considerations

Both the chart and the example currently default to rolling `latest` image tags. Pin reviewed
operator and Nervix image tags or digests before using this setup in production. Also choose
storage, resource requests, NodePort exposure, and Secret management appropriate for the cluster.

## Deployment Qualification

Nervix's Docker Build workflow deploys every publishable Debian server revision through the shipped
operator on native amd64 and arm64 minikube runners. Both checks must pass before the workflow can
update `debian-latest`. They run for image-building pull requests from this repository and manual
builds; a pull request marked `no-docker` builds no image and runs no deployment check.

Each check resolves the server image built by that run and the published operator image to registry
digests, records the chart and example's `main` revision, and changes only the example's server
image. It waits for user initialization, all three pods running that exact image, and a leader with
three Raft voters reported by `SHOW CLUSTER STATUS` through the bootstrap NodePort. The CLI comes
from the tested server image. Through `DESCRIBE CLUSTER`, the check requires the example's replica
count of 2 to be committed and running at its committed revision on all three nodes. It then
deletes the bootstrap pod, waits for a new pod identity on the same persistent volume claim and
for all three pods to be ready on the tested image, and verifies the three-voter membership and
the same replica policy revision again. A missing request, unapplied value or changed revision
fails the check.

Image identities, Kubernetes observations, policy descriptions before and after the restart,
the recorded revision and pod and volume claim identities, command output and current and
previous pod logs are retained as workflow artifacts. The check deletes its own minikube profile
after success, failure or cancellation.

This qualifies the default deployment's startup command, interconnect certificates, memory limits
and initialization sequence, and retention of the committed replica policy across the bootstrap
pod's restart. Minikube's hostpath provisioner reports the host filesystem's capacity;
it does not enforce the example's `20Gi` claim as a filesystem size. A passing deployment therefore
does not qualify volume-size enforcement, another storage provisioner, or production availability.
The [startup preflight](node-settings.md#the-startup-preflight) retains its separate volume tests.

Changes to the server's command line, startup settings or initialization contract must keep the
operator's generated startup command, chart and example current in the same delivery. The server
PR's deployment check is required evidence for that work. The operator's own workflow also checks
changes to its repository: it interrupts the example's initialization by stopping the operator and
recreating the bootstrap pod, then restarts and recreates pods on their volumes, rolls and scales
the StatefulSet, and changes the parameter request. After each step every pod must start and the
cluster must keep its committed parameters. It does not replace the server-image checks.
