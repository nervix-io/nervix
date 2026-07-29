# Kubernetes Operator

The [Nervix Kubernetes Operator](https://github.com/nervix-io/nervix-k8s-operator) watches
namespaced `NervixCluster` resources across a Kubernetes cluster. For each resource, it creates the
services, StatefulSet, and one persistent volume claim per Nervix node. It also coordinates
one-time initialization of the `default` user before scaling the StatefulSet to the requested
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
that Nervix committed the user, the operator restarts that pod without the credential and then
scales the cluster to the requested replica count.

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
  storage: 5Gi
```

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

The web console is available at `http://<node-address>:31420/console/`. Omit `localAccess` for an
in-cluster-only deployment.

## Production Considerations

Both the chart and the example currently default to rolling `latest` image tags. Pin reviewed
operator and Nervix image tags or digests before using this setup in production. Also choose
storage, resource requests, NodePort exposure, and Secret management appropriate for the cluster.
