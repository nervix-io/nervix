# Kubernetes dependency stack

This directory installs the local dependency stack with upstream Kubernetes
packaging instead of mirroring `docker-compose.yml` as raw Deployments.

Run:

```sh
just kube-deps
```

The target installs:

- cert-manager and a `nervix-internal-ca` `ClusterIssuer`
- Strimzi and a three-node Kafka cluster
- RabbitMQ Cluster Operator and a three-node RabbitMQ cluster
- Apache Pulsar Helm chart in distributed mode
- Bitnami Redis Helm chart in replication mode
- EMQX Helm chart with three nodes
- NATS Helm chart with three clustered servers
- kube-prometheus-stack with only Prometheus enabled
- local Helm charts for ElasticMQ and the Nervix mock server

Each dependency instance lives in its own namespace:

- Kafka: `nervix-kafka`
- RabbitMQ: `nervix-rabbitmq`
- Redis: `nervix-redis`
- EMQX: `nervix-emqx`
- NATS: `nervix-nats`
- Prometheus: `nervix-prometheus`
- Pulsar: `nervix-pulsar`
- ElasticMQ: `nervix-elasticmq`
- Mock server: `nervix-mock-server`

Operators that are normally cluster infrastructure are installed into their own
namespaces.

Run:

```sh
just kube-app
```

to install a three-node Nervix StatefulSet into the `nervix` namespace on top of
the dependency stack. Each pod has its own PVC and advertises its stable
StatefulSet FQDN through the `nervix-headless` service for internal gossip,
cluster API, and interconnect traffic. For local client redirects, each pod
advertises a host-reachable gRPC NodePort address. The server image used by the
manifest must include hostname/FQDN advertise-address support.

For local host access from minikube, the manifest creates a bootstrap NodePort
service:

- `nervix-local`: gRPC `31390`, observability `31090`

Each pod advertises a host-reachable gRPC address through the cluster status and
not-leader response. Those advertised addresses are backed by per-pod NodePort
services:

- `nervix-local-node-1`: gRPC `31391`, observability `31091`
- `nervix-local-node-2`: gRPC `31392`, observability `31092`
- `nervix-local-node-3`: gRPC `31393`, observability `31093`

Run the local CLI against the bootstrap endpoint with:

```sh
just kube-cli-command "SHOW CLUSTER STATUS;"
```
