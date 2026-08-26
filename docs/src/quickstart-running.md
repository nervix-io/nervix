# Running Nervix

This page starts everything the tutorial needs on your local machine: one Nervix node, a Kafka
broker, and a Redis server.

## Start The Server

A Nervix node is a single `nervix-server` process. Start a fresh single-node cluster:

```bash
nervix-server \
  --node-id node-1 \
  --cluster-api-listen-addr 127.0.0.1:47393 \
  --cluster-api-advertise-addr 127.0.0.1:47393 \
  --allow-bootstrap \
  --init-default-user-password nervix
```

- `--node-id` names this node inside the cluster.
- `--cluster-api-listen-addr` and `--cluster-api-advertise-addr` configure the internal cluster
  API; a single local node points both at the same local port.
- `--allow-bootstrap` lets this node form a brand-new cluster instead of joining an existing one.
- `--init-default-user-password` seeds the `default` user's password on the very first startup.
  Once the user exists, remove the flag from normal startup.

Every flag has a `NERVIX_*` environment variable twin, such as `NERVIX_NODE_ID`. Control-plane
state is persisted under `./.nervix-db` by default; use `--db-path` to relocate it.

The server exposes:

- the session gRPC API on `127.0.0.1:47391` (`--addr`), used by `nervix-cli` and the
  [Rust client library](client-library.md)
- the [web console](client-tools-web-console.md) at `http://127.0.0.1:47420/console/`, which
  combines a command line with a live graph view
- health and metrics endpoints on port `9090`: `/livez`, `/readyz`, and `/metrics`
  ([Metrics And Observability](metrics-and-observability.md#observability-server))

## Connect A Client

[`nervix-cli`](client-tools-cli.md) is the interactive NSPL client. With no arguments it connects
to `http://127.0.0.1:47391` as user `default` and prompts for the password:

```bash
nervix-cli
```

```text
nervix[default]> SHOW CLUSTER STATUS;
```

Statements end with `;`, and the prompt shows the active domain. The same statements can be typed
into the web console's command line, or executed one-shot with
`nervix-cli --command "<statements>"`. There is no file-execution mode: to run a saved script,
paste it into the client or pass it through `--command "$(cat pipeline.nspl)"`. To format a saved
script, use [`nervix-nspl-format`](client-tools-nspl-format.md).

## Start Kafka And Redis

The tutorial reads from Kafka and publishes to Redis. Any reachable Kafka broker and Redis server
work; for a throwaway local setup:

```bash
docker run -d --name broker -p 9092:9092 apache/kafka:latest
docker run -d --name redis -p 6379:6379 redis:7
```

If you work from a repository clone, `just deps` starts a complete local dependency stack instead
([Developing Nervix](developing-nervix.md#start-local-broker-dependencies)).

Nervix never creates topics, queues, or any other external entities as a side effect of running a
graph. Missing entities surface as initialization or publish errors, so the tutorial provisions
its Kafka topic explicitly before ingesting from it.

Continue to [Your First Pipeline: Kafka To Redis](./quickstart-first-pipeline.md).
