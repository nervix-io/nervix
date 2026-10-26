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
  initialDefaultUserPasswordSecretRef:
    name: nervix-initial-password
    key: password
  storage: 20Gi
  resources:
    memoryLimit: 3Gi
```

Each node checks its volume and its memory before it starts, and refuses to start when they cannot
hold its configuration: its volume must be at least as large as each resource quota, 16 GiB for
extracted resources by default, and its memory limit must hold its execution budgets, 2.66 GiB. The
example spells out the operator's defaults, a 20 GiB volume and a 3 GiB memory limit, which meet
both checks. With a smaller volume or memory limit, every pod exits with the refusal.
[Capacity Planning](capacity-planning.md#what-a-node-needs-to-start) gives the rules.

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

## Cluster Parameters

Cluster parameters, such as the replica count, are policy the whole cluster shares. The operator
passes no cluster parameter option to any node. Restarting a pod, recreating it on its persistent
volume, rolling the StatefulSet and scaling it therefore change no parameter, and a new cluster
starts with the server's defaults.

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
restarted commits each value once. The cluster stays initializing until the request is committed. A
server without `ALTER CLUSTER SET` refuses the statement, and the refusal is reported. The operator's
ClusterRole lets it exec into pods for this.

`status.initialClusterParameters` records the values the operator committed. The
`InitialClusterParametersCommitted` condition reports on the request: `Pending`, or `Uncommitted`
with the refusal, while the cluster initializes; `Committed` once the request is committed;
`ChangedAfterInitialization` when the request changes afterwards; and `InitializationNotRequested`
for a cluster without `initialDefaultUserPasswordSecretRef`, which the operator does not initialize.
The operator never changes an initialized cluster's parameters. Change them with `ALTER CLUSTER SET`,
which `DESCRIBE CLUSTER` then reports with each node's running values.

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
graceful shutdown. A Nervix node ends its own shutdown within its shutdown timeout, 50 seconds by
default, which covers the 30-second default drain timeout and the services that stop after it. The
operator sets `terminationGracePeriodSeconds: 60` so the kubelet waits longer than that, and a node
that cannot finish exits on its own rather than being killed partway through its drain. If you raise
`NERVIX_SHUTDOWN_TIMEOUT`, raise the grace period with it.

Note that `/readyz` reports whether the node currently knows a Raft leader; it does not turn
negative when a node begins terminating. A draining node is therefore not removed from Service
endpoints by readiness alone. It stops accepting new client connections itself in the first
shutdown phase, and it keeps serving already-admitted work until its drain finishes.

A terminating node moves its scheduled work to a live peer when one is available, and otherwise
completes everything it has admitted in place. Rolling one pod at a time keeps a peer available to
receive that work, so ownership moves instead of waiting for the pod to come back. See
[Shutdown And Recovery](shutdown.md) for the phases, the drain guarantees, and what each ending
preserves.

## Production Considerations

Both the chart and the example currently default to rolling `latest` image tags. Pin reviewed
operator and Nervix image tags or digests before using this setup in production. Also choose
storage, resource requests, NodePort exposure, and Secret management appropriate for the cluster.

## Deployment Qualification

Nervix's Docker Build workflow deploys every publishable Debian server revision through the shipped
operator on native amd64 and arm64 minikube runners. Both checks must pass before the workflow can
update `debian-latest`. They run for image-building pull requests from this repository, manual builds
and nightly builds; a pull request marked `no-docker` builds no image and runs no deployment check.

Each check resolves the server image built by that run and the published operator image to registry
digests, records the chart and example's `main` revision, and changes only the example's server
image. It waits for user initialization, all three pods running that exact image, and a leader with
three Raft voters reported by `SHOW CLUSTER STATUS` through the bootstrap NodePort. The CLI comes
from the tested server image. Image identities, Kubernetes observations, command output and current
and previous pod logs are retained as workflow artifacts, and the check deletes its own minikube
profile after success, failure or cancellation.

This qualifies the default deployment's startup command, interconnect certificates, memory limits
and initialization sequence. Minikube's hostpath provisioner reports the host filesystem's capacity;
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
