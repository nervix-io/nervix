# Running Nervix

## Prerequisites

Install:

- Rust via `rustup`
- `just`
- `zellij`

## Start The Server

The server crate and executable are both named `nervix-server`. The `just server` recipe runs that
dedicated server binary.

```bash
NERVIX_INIT_DEFAULT_USER_PASSWORD='nervix' just server
```

Fresh clusters require an initial password for the `default` user. Set
`NERVIX_INIT_DEFAULT_USER_PASSWORD` or pass `--init-default-user-password <password>` on the first
startup. The leader stores the `default` user's Argon2 password hash in the strongly consistent
control plane only when that user does not already exist. After the default user has been created,
remove the environment variable or flag from normal startup. Later users can be created through NSPL:

```nspl
CREATE USER my_username WITH PASSWORD 'my_secure_password';
```

Clustered startup example:

```bash
just server -- \
  --addr 127.0.0.1:47391 \
  --http-listen-addr 0.0.0.0:8080 \
  --https-listen-addr 0.0.0.0:8443 \
  --grpc-advertise-addr 10.0.0.10:47391 \
  --cluster-listen-addr 0.0.0.0:47392 \
  --cluster-advertise-addr 10.0.0.10:47392 \
  --cluster-api-mode http \
  --cluster-api-listen-addr 0.0.0.0:47393 \
  --cluster-api-advertise-addr 10.0.0.10:47393 \
  --interconnect-mode http \
  --interconnect-listen-addr 0.0.0.0:47394 \
  --interconnect-advertise-addr 10.0.0.10:47394 \
  --cluster-bootstrap-host 10.0.0.11:47392
```

Nervix uses separate listener addresses for plain and TLS server-side traffic:

- `--http-listen-addr` for HTTP and WS
- `--https-listen-addr` for HTTPS and WSS

Internal node-to-node traffic is configured separately:

- `--cluster-api-mode http|https`
- `--cluster-api-listen-addr` and `--cluster-api-advertise-addr` for plain HTTP cluster API
- `--cluster-api-https-listen-addr` and `--cluster-api-https-advertise-addr` for HTTPS cluster API
- `--interconnect-mode http|https`
- `--interconnect-listen-addr` and `--interconnect-advertise-addr` for plain interconnect
- `--interconnect-https-listen-addr` and `--interconnect-https-advertise-addr` for TLS interconnect

Mode selection uses:

- `http` for plain, unencrypted transport
- `https` for TLS transport

OpenTelemetry trace export is optional and uses the existing `tracing` instrumentation:

- `--otel-enabled` or `NERVIX_OTEL_ENABLED=true` enables OTLP trace export
- `--otel-otlp-endpoint` or `NERVIX_OTEL_OTLP_ENDPOINT` sets the OTLP gRPC collector endpoint, defaulting to `http://127.0.0.1:4317`
- `--otel-service-name` or `NERVIX_OTEL_SERVICE_NAME` sets the OpenTelemetry service name, defaulting to `nervix`
- `--otel-trace-sample-ratio` or `NERVIX_OTEL_TRACE_SAMPLE_RATIO` sets parent-based trace sampling from `0.0` through `1.0`

The `just deps` stack includes Quickwit and Jaeger for local trace storage and viewing. Quickwit receives OTLP traces on host port `4317`, and the Jaeger dashboard at `http://127.0.0.1:16686` is configured to query Quickwit as its trace backend.

The observability listener exposes health and graph metrics:

- `/livez` reports process liveness
- `/readyz` reports readiness once a leader is known
- `/metrics` reports raw Prometheus text metrics for graph nodes and relays

`DESCRIBE` commands include the same graph-node and relay metric labels, plus local derived values such as counter rates and histogram percentiles. See [Metrics And Observability](metrics-and-observability.md) for the metric families, labels, and rate semantics.

## Local Multi-Node Setup

```bash
just cluster-dashboard
```

The zellij dashboard seeds the `default` user once with the local password `nervix` and exports the
same password for the interactive client pane. Override it by setting `NERVIX_PASSWORD` or
`NERVIX_INIT_DEFAULT_USER_PASSWORD` before running the dashboard. If the local dashboard state was
already initialized with a different password, run `just reset-local-dashboard-state` before starting
fresh.

## Start The Interactive Client

The separate interactive client crate and executable are both named `nervix-cli`.

```bash
just client
```

The client connects as `default` unless `--username` or `NERVIX_USERNAME` is set. Pass
`--password`, set `NERVIX_PASSWORD`, or let the client prompt interactively.

Direct subscription example:

```bash
just client subscribe notifications
```

## Start Local Broker Dependencies

```bash
just deps
```

The local dependency stack includes broker and service containers used by the documented examples, including Kafka, Pulsar, RabbitMQ, Redis, MQTT, ClickHouse, Postgres, MySQL, MongoDB, RustFS, Prometheus, Quickwit, and Jaeger.

RustFS provides the local Rust-written S3-compatible target for Iceberg emitters:

- S3 endpoint: `http://127.0.0.1:9900`
- console: `http://127.0.0.1:9901`
- access key: `rustfsadmin`
- secret key: `rustfsadmin`
- bucket: `nervix-iceberg`

The compose stack also starts `fake-gcs` for GCS API emulation and `azurite` for Azure Blob API emulation:

- GCS endpoint: `http://127.0.0.1:4443`
- GCS bucket: `nervix-iceberg`
- Azure Blob endpoint: `http://127.0.0.1:10000/devstoreaccount1`
- Azure Blob container: `nervix-iceberg`
- Azure Blob development account: `devstoreaccount1`

The current Iceberg OpenDAL adapter honors custom GCS service endpoints, so `fake-gcs` can be used for local GCS tests. Azure Blob support is exposed through the adapter's ADLS/Blob URL forms (`wasb://` and `wasbs://`); the pinned adapter derives its endpoint from the storage URL and does not yet honor Azurite's path-style local endpoint, so Azurite is available in compose for blob-client work but Iceberg Azure local integration needs an adapter patch or upstream endpoint support.

Iceberg emitters stage local batch files under `/tmp` by default before committing them to blob storage. Use `--temp-dir` or `NERVIX_TEMP_DIR` to place runtime temporary files elsewhere.

Kinesis is not part of `just deps` today. For local Kinesis-compatible testing, run a separate LocalStack container and point a `CLIENT ... TYPE KINESIS` at its `endpoint`.

Minimal LocalStack example:

```yaml
services:
  localstack:
    image: localstack/localstack
    ports:
      - "4566:4566"
    environment:
      SERVICES: kinesis
      AWS_DEFAULT_REGION: us-east-1
```

## Prometheus Local Check

```bash
curl --get 'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode 'query=label_replace(vector(42.5), "source", "local", "", "")'
```

## OpenTelemetry Local Check

Start Nervix with `--otel-enabled` and keep the default OTLP endpoint when `just deps` is running. Open `http://127.0.0.1:16686`, select the `nervix` service, and search for traces. Quickwit is also available at `http://127.0.0.1:7280`.

## Validation And Tests

For repository-wide validation:

```bash
just validate
```

For test runs:

```bash
just test
```
