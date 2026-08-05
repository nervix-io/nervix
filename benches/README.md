# End-to-end benchmark framework

The benchmark harness runs the same declared streaming workload against Nervix or a competitive
implementation. Each run gets fresh Testcontainers dependencies, fresh Kafka topics, a unique
consumer group, a high-rate idempotent producer, and a bounded wait for stable input/output count
parity. Nothing depends on the repository's long-lived Docker Compose stack.

List the available workloads and implementations:

```bash
just benchmark list
```

The first workload is `kafka-filter-map`: decode JSON from Kafka, retain records for which
`contains(value, "x")` is true, uppercase `value`, and publish JSON to another topic in the same
Kafka broker. It has `nervix` and `vector` implementations.

## Running implementations

Build the current checkout and run local Nervix:

```bash
just benchmark-nervix-local
```

Run a tagged Nervix image. The harness loads the domain and graph directly through the public
client-core API:

```bash
just benchmark-nervix-image nervix:benchmark
```

Run Vector 0.57.0 from its pinned official Debian image:

```bash
just benchmark run kafka-filter-map --implementation vector
```

Build the local Nervix binaries once and run every workload implementation in catalog order:

```bash
just benchmark-all-local
```

For pull requests carrying the exact `benchmark` label, the Docker workflow first completes its
normal amd64 Nervix runtime image. A separate downstream job then pulls that exact tag from GHCR and
runs the benchmark catalog against it. The downstream job builds only the benchmark harness; both
server startup and control-plane configuration target the completed image, with no CLI subprocess.
Adding the label starts a Docker build and benchmark run; subsequent commits rerun the complete
catalog. A least-privilege reporter job downloads the resulting artifact and updates one stable PR
comment with the comparison table; failed reruns replace stale results with the failure state.
Fork PRs remain excluded because their untrusted workflow tokens cannot push the image that this job
consumes.

The underlying image-only entry point is available for reproducing that CI path:

```bash
just benchmark-ci nervix:runtime target/benchmarks
```

The generic `benchmark` recipe only builds the harness. This keeps competitive runs from compiling
the Nervix server unnecessarily.

Override common load fields or workload parameters on any run:

```bash
just benchmark-nervix-local kafka-filter-map \
  --partitions 16 \
  --parameter emitter_flush_each=20s \
  --parameter emitter_max_batch_size=1GiB
```

`duration = "auto"` selects at least 30 seconds and twelve cycles of the slowest parameter whose
name ends in `_flush_each`. The example therefore runs for 240 seconds. `--duration-seconds N`
overrides that policy for reproduction or smoke testing.

Nervix consumes the flush values directly as NSPL durations and binary sizes. The runner derives
Vector's `batch.timeout_secs` and `batch.max_bytes` from those same settings. Vector measures its
native maximum before serialization, while Nervix limits an Arrow batch, so reports retain the
native values and should not imply byte-for-byte equivalence.

## Adding a workload

Create one directory under `benches/benchmarks/<slug>/` containing `benchmark.toml` and one Upon
template per implementation. The manifest owns the common load shape and tuning parameters:

```toml
name = "example"
description = "What crosses the measured path"
dependencies = ["kafka"]

[load]
duration = "auto"
partitions = 16
value_bytes = 128
max_backlog_messages = 4194304
wait_timeout_seconds = 120

[parameters]
emitter_flush_each = "10ms"
emitter_max_batch_size = "8MiB"

[implementations.nervix]
kind = "nervix"
template = "nervix.nspl"

[implementations.competitor]
kind = "container"
image = "vendor/product:explicit-tag"
template = "product.yaml"
config_path = "/etc/product/config.yaml"
command = ["--config", "/etc/product/config.yaml"]
readiness_port = 8686
readiness_path = "/health"
```

Templates receive `kafka_bootstrap_servers`, `input_topic`, `output_topic`, `consumer_group`, the
integer `lanes` list resolved from the run's partition count, the manifest's `parameters`, and a
`dependencies` map containing every started endpoint by its Cucumber key. Container
implementations join Kafka's run-scoped Docker network; a local Nervix process receives Kafka's
random host port instead.

The current typed dependency and load contract is Kafka-to-Kafka with one expected output per
input. The shared test-environment crate retains the other Cucumber dependency starters, but a
workload using one of them, filter selectivity, or different output cardinality needs a
corresponding typed benchmark contract before it is exposed in a manifest.

## Results

Every run writes to:

```text
target/benchmarks/<workload>/<implementation>/<run-id>/
```

Artifacts include the resolved parameters, rendered configuration, subject log, image identity for
container runs, load-driver log, and the count/rate report. `run-all` also writes
`benchmark-comparison.md` from the exact run directories produced by that invocation; it never
selects unrelated runs by timestamp. A run passes only after the output topic equals the
broker-acknowledged input count and remains stable for a confirmation interval.

This is a single-host end-to-end benchmark. Its rate includes Kafka, the load driver, the selected
product, and the output drain. Compare products only with identical workload inputs, and do not use
a run whose `peak_backlog_messages` equals the configured cap as a maximum-throughput result.
