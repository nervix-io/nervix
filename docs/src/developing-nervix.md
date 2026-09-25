# Developing Nervix

This section is for working on Nervix itself. Everything here assumes a clone of the Nervix
repository and uses its `just` recipes; none of it is required to use Nervix.

## Clone The Repository

```bash
git clone https://github.com/nervix-io/nervix
cd nervix
```

## Prerequisites

Nervix is developed on Linux x86_64, Linux aarch64, and macOS arm64. The `just` recipes fetch an
ONNX Runtime build for the host and stop on any other.

Install:

- Rust via `rustup`
- `just`
- `zellij`

## Start The Server

The server crate and executable are both named `nervix-server`. The `just server` recipe creates a
development CA and a `default`/`node-1` identity, then runs that dedicated server binary.

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
  --cluster-id production \
  --node-id node-1 \
  --interconnect-listen-addr 0.0.0.0:47395 \
  --interconnect-advertise-addr node-1.internal.example:47395 \
  --interconnect-tls-ca /etc/nervix/interconnect/ca.pem \
  --interconnect-tls-cert /etc/nervix/interconnect/node-1.pem \
  --interconnect-tls-key /etc/nervix/interconnect/node-1-key.pem \
  --cluster-bootstrap-host node-2.internal.example:47395
```

Nervix uses separate listener addresses for plain and TLS server-side traffic:

- `--http-listen-addr` for HTTP and WS
- `--https-listen-addr` for HTTPS and WSS

All internal node-to-node traffic uses one authenticated HTTP/2 listener. The CA, certificate, and
private-key options are mandatory. Each node certificate must support both TLS server and client
authentication, contain the advertised DNS name or IP address, and contain exactly one identity URI
of the form `nervix://cluster/<cluster-id>/node/<node-id>`. Peers with a different cluster identity
or a certificate identity that disagrees with their protocol identity are rejected. Gossip, Raft,
resource transfer, and relay traffic use independent connection pools on this listener; bounded
rkyv is the internal control encoding, while relay batches remain Arrow IPC.
The server monitors all three interconnect PEM files. It reloads a changed bundle only after two
consecutive reads agree and the complete replacement passes certificate, identity, and lifetime
validation; an incomplete or invalid update leaves the active bundle in service and is retried.

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

The separate interactive client crate and executable are both named `nervix-cli`. Its options and
interactive behaviour are documented in [Command Line Client](client-tools-cli.md).

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

The local dependency stack includes broker and service containers used by the documented examples,
including Kafka, Pulsar, RabbitMQ, Redis, MQTT, ClickHouse, Postgres, MySQL, MongoDB, RustFS,
Prometheus, Quickwit, Jaeger, and a Sentry-compatible Bugsink service.

The local Sentry-compatible service is available at `http://127.0.0.1:18090`. Sign in with
`admin@example.org` / `admin`, create a project, and copy its DSN into a `TYPE SENTRY` client used
by a Sentry emitter. These credentials and the Compose `SECRET_KEY` are development-only.

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

## Prometheus Local Check

```bash
curl --get 'http://127.0.0.1:9090/api/v1/query' \
  --data-urlencode 'query=label_replace(vector(42.5), "source", "local", "", "")'
```

## OpenTelemetry Local Check

Start Nervix with `--otel-enabled` and keep the default OTLP endpoint when `just deps` is running. Open `http://127.0.0.1:16686`, select the `nervix` service, and search for traces. Quickwit is also available at `http://127.0.0.1:7280`.

## Connector Crates

[Connector Crates And The Connector Contract](./connector-contract.md) is the architecture
reference for ownership, source and sink lifecycle, and the cross-layer checklist for adding an
integration. The notes here identify the repository locations and validation commands.

The shared contract is in `crates/connector`, and each external integration is in
`crates/connectors/<name>`. The server's `src/runtime/ingestors` and emitter modules compose the
typed plans with those crates. Keep driver dependencies in their integration crates; the server
test harness may name a driver in `[dev-dependencies]` to provision or inspect an external system.
The node's own endpoint source lives in `src/runtime/ingestors/endpoint.rs` because it has no
external driver. Follow the architecture chapter's [integration checklist](./connector-contract.md#adding-an-integration)
when extending this layout.

## Validation And Tests

For repository-wide validation:

```bash
just validate
```

`just validate-nspl-docs` scans `docs/src` and parses every exact `nspl` code fence directly with
the parser crate. Use `nspl,ignore` only for loose grammar synopses and statement fragments that
are intentionally not complete NSPL scripts; the fence remains identified as NSPL in the rendered
documentation.

For test runs:

```bash
just test
```

Connector crates have a focused recipe. `just test-connectors` runs the unit tests of the
`nervix-connector` contract crate and of every `nervix-connector-*` integration crate. Arguments are
passed to the test binaries, so a test name filter narrows the run:

```bash
just test-connectors <filter>
```

### Deterministic concurrency checks

Run the in-process scheduling checks for the execution, interconnect, and server crates with:

```bash
just test-shuttle
```

Each check runs in its own process under bounded Shuttle schedules, then runs through the
uncontrolled-nondeterminism detector. A substring selects a focused check or protocol family:

```bash
just test-shuttle force_flush
```

On failure, the runner writes a schedule below
`target/shuttle-failures/<package>/<fully-qualified-test-name>/`. Pass the resulting schedule file
to the replay recipe; its parent directories identify the exact package and check:

```bash
just test-shuttle-replay target/shuttle-failures/<package>/<fully-qualified-test-name>/<schedule-file>
```

Keep the schedule with the failure report while fixing the owning protocol, then run the focused
check and the full suite. To verify schedule persistence and replay without changing a protocol,
set `SHUTTLE_FORCE_FAILURE=1` for a focused run, which deliberately fails after its invariant has
completed, and replay the written schedule with the same variable set. Remove the variable for
normal verification. Use `SHUTTLE_REPORT_STEPS=1` to inspect the highest explored step count when
setting a check's iteration and step budgets. [Data-Plane Concurrency](./data-plane-concurrency.md)
defines what these checks model, their limits, and the invariant held by each protocol.

### The scenario suite's execution budget

The Cucumber suite bounds its own run. A step, a teardown diagnostic or a node stop that never
returns ends the whole run at the budget rather than leaving the process alive until CI cancels the
job, which kills it mid-scenario with no record of what each scenario was doing. When the budget
expires the suite prints every scenario still active with its attempt, its phase, how long it has
been in that phase and the cluster nodes it holds, asks every live node to stop, waits one bounded
cleanup window for them, and exits with status `124`. A passing run still exits `0` and a failing
one still panics, so a wedged suite is told apart from a failing one by the exit status alone.

The default budget leaves the workflow job time for the work that precedes the suite and for the
artifact upload that follows a timeout. Give a run a budget of its own with `--suite-budget` or the
`NERVIX_TEST_SUITE_BUDGET` environment variable, which is how the timeout path is exercised without
waiting out the suite's own budget:

```bash
just test-scenarios --input tests/features/cluster/rejoin.feature --suite-budget 5s
```

The focused regressions that hold the harness's startup, status, teardown and watchdog budgets run
in seconds:

```bash
just test-harness-liveness
```

[Integration Test Lifecycle](./integration-test-lifecycle.md) defines every harness deadline, the
phases a scenario reports, and how each failure reaches CI output.

## Building The Documentation

`just book <version>` renders this book, and `just book-pdf <version>` additionally produces
`nervix.pdf` through pandoc and XeLaTeX.

Both recipes require the exact mdBook release pinned by `MDBOOK_VERSION` in
`scripts/build_book.py`, and they fail with that version in the message when a different one is
installed. CI resolves the same constant, so a local build always matches what is published. The
pin is exact because the rendered theme targets mdBook's internal element IDs and because the PDF
relies on mdBook rewriting print-page links into in-document anchors — on an older release every
cross-chapter link in the PDF would point back at the website instead.

Install the pinned release with:

```bash
cargo install mdbook --version <MDBOOK_VERSION> --locked
```

Building the PDF also needs `pandoc` and a XeLaTeX installation on `PATH`.
