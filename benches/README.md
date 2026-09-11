# End-to-end benchmark framework

The benchmark harness runs the same declared streaming workload against Nervix or a competitive
implementation. Each run gets fresh Testcontainers dependencies, fresh Kafka topics, a unique
consumer group, a high-rate idempotent producer, a timed steady-state warm-up, and a bounded wait
for the stable output the workload's declared load shape expects. Nothing depends on the
repository's long-lived Docker Compose stack.

List the available workloads and implementations:

```bash
just benchmark list
```

`kafka-filter-map` decodes JSON from Kafka, retains records for which `contains(value, "x")` is
true, uppercases `value`, and publishes JSON to another topic in the same Kafka broker. Every input
message produces one output message.

`kafka-dedup-window` is the stateful-processor workload: decode JSON from Kafka, retain the three
quarters of records whose value carries the retain marker, drop the duplicate of every key,
aggregate the survivors into tumbling windows, and publish one JSON summary per closed window. Its
measured path is ingestor → deduplicator → window processor → emitter.

Both workloads have `nervix` and `vector` implementations.

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
comment with every workload and implementation. The runner attempts the rest of the catalog after
an individual implementation fails, retains successful measurements, and lists failed entries in
the same PR report while keeping CI failed. Fork PRs remain excluded because their untrusted
workflow tokens cannot push the image that this job consumes.

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
  --warmup-seconds 10 \
  --parameter emitter_flush_each=20s \
  --parameter emitter_max_batch_size=1GiB
```

`duration = "auto"` selects at least 30 seconds and twelve cycles of the slowest parameter whose
name ends in `_flush_each`. The example therefore runs for 240 seconds. `--duration-seconds N`
overrides that policy for reproduction or smoke testing.

Nervix consumes the flush values directly as NSPL durations and binary sizes. The runner derives
Vector's `batch.timeout_secs`, `batch.max_bytes`, and `end_every_period_ms` from those same
settings. Vector measures its native maximum before serialization, while Nervix limits an Arrow
batch, so reports retain the native values and should not imply byte-for-byte equivalence.

`kafka-filter-map` also exposes `ingestor_mode`, the ingestor's whole `MODE` clause, so the same
graph can be measured under acknowledgement instead of the `NO_ACK PARALLEL` default. The clause
contains spaces, so quote it twice when overriding it through a `just` recipe — the outer quotes
are consumed by the shell and the inner ones survive into the recipe:

```bash
just benchmark-ab main 5 kafka-filter-map \
  --parameter "'ingestor_mode=ACK PARALLEL MAX 1024 BATCH TIMEOUT 10ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s'"
```

`kafka-dedup-window` matches its two implementations on the same drop rate and the same aggregate,
not on identical internals. Vector's `dedupe` evicts by cache size where Nervix expires by
`MAX TIME`, and Vector's `reduce` closes on a period where a Nervix window closes on whichever of
its message and duration bounds is met first; the Nervix window also retains the records it
buffered, which Vector's running sum does not. Sizing the Vector cache above the live keyspace and
matching the period to `window_max_delay` makes both graphs produce the same record total, which is
what parity checks.

## Load shapes

A workload declares in `[load.shape]` what the driver generates and what the measured path owes it
in return. The driver produces indivisible *cycles* of input messages, each cycle written to one
Kafka partition, and every shape states how many output records one complete cycle must yield.
Parity is exact against that contract, so a graph that drops records has to state its drop rate
rather than assume one output per input.

`uniform-passthrough` sends identical payloads and expects one output message per input message.
Its output record count is Kafka's high watermark, so the driver never has to read a payload back.

`keyed-windowed` sends cycles of `keys_per_cycle` distinct keys, each produced `copies_per_key`
times as one pass over the key list per copy. The first `retained_keys` keys of a cycle carry the
retain marker `x` at the head of their padded value; the rest are all padding. Every payload is the
same width, so `wire_bytes_per_message` stays a single number. A cycle therefore produces
`keys_per_cycle × copies_per_key` messages, of which `retained_keys × copies_per_key` pass the
filter and exactly `retained_keys` survive deduplication. Because the filter reads only the key's
own value, that count holds whether the node filter runs before or after deduplication.

Window output cardinality is not a function of the input count, so `keyed-windowed` parity is the
sum of the `count_field` the summaries carry rather than a count of output messages. The driver
consumes the output topic from its beginning on a run-scoped assignment and accumulates that sum,
which also drives the live backlog signal, the warm-up handshake, and the drain wait.

Duplicates of one key are `keys_per_cycle` messages apart on one partition. That is far enough to
exercise a live keyspace and close enough that the deduplicator's `MAX TIME` can never expire a key
between its copies and re-emit one. Raising `dedup_max_time` widens the retained keyspace and the
memory it costs without changing the expected output.

Warm-up sends at least one complete cycle per partition and continues under the same backlog bound
for `load.warmup_seconds`. It then waits for the exact records those cycles owe and requires both
the output message count and output record count to remain unchanged for the parity confirmation
interval before it establishes the measured-phase baseline. The catalog uses ten seconds so Kafka,
network, allocator, processor, and output paths reach steady state before measurement begins.

The window that is still filling when generation stops closes on its duration bound, so the
reported `drain_seconds` for a windowed workload includes up to one `window_max_delay` of waiting
that is not backlog.

## Making `MAX BATCH SIZE` bind

A byte cap only clamps a batch; it never grows one. Batch size is arrival rate × flush interval, so
the cap fires only when that product exceeds it. `kafka-dedup-window` carries the binding caps on
its two high-volume routes — the ingestor route at the full input rate and the deduplicator route
at three eighths of it — with `FLUSH EACH 50ms MAX BATCH SIZE 64KiB`. The emitter cannot bind,
because it sees one summary per closed window.

Confirm it from the subject's `messages_per_batch` histogram, scraped from the observability port
during a run. At the manifest defaults the size boundary decides both routes; raising only the caps
hands the decision back to the 50 ms interval and the batches grow:

| Route | `64KiB` caps | `--parameter ingestor_max_batch_size=8MiB --parameter dedup_max_batch_size=8MiB` |
|:--|--:|--:|
| ingestor → deduplicator | 786 rows | 1,741 rows |
| deduplicator → window | 576 rows | 699 rows |

## Comparing two local builds (A/B)

Performance claims are established locally on identical hardware. CI benchmark comments compare
Nervix and Vector within one run on whatever worker the job landed on, so they are a smoke signal
only, never a perf claim. Before merging a performance change, A/B it on an otherwise idle
machine:

```bash
just benchmark-ab main       # 3 interleaved runs per arm of kafka-filter-map
just benchmark-ab HEAD~1 5   # 5 runs per arm against the previous commit
```

`benchmark-ab <baseline-ref> [runs] [benchmark]` builds `nervix-server` twice — once from a clean
temporary worktree at `<baseline-ref>` and once from the current working tree — and caches the
binaries under `target/ab/`:

- `target/ab/<commit>/nervix-server` — the baseline binary, keyed by the resolved commit hash so a
  moving ref such as `main` re-keys correctly and a re-run skips the whole baseline build;
- `target/ab/candidate/nervix-server` — a snapshot of the current tree's binary, so a concurrent
  `cargo build` cannot swap it mid-comparison;
- `target/ab/build/` and `target/ab/worktree/` — the baseline build's dedicated target directory
  and temporary worktree. The worktree runs its own `just build-web-console`, so the baseline ref
  must already contain that recipe.

The harness `run-ab` subcommand then alternates the arms (baseline, candidate, baseline, …) so
slow machine drift cancels out instead of accumulating in one arm; the candidate always runs
second within a pair, which the per-run table keeps visible. Extra arguments are forwarded to
`run-ab`, so `--duration-seconds`, `--partitions`, and `--parameter` overrides work here too. With
the default 30-second auto duration, a 3+3 comparison takes several minutes after the builds.

Each run keeps the standard artifact layout under `target/benchmarks/ab/<arm>/`, and the summary —
per-arm mean/min/max of `end_to_end_messages_per_second`, the candidate-vs-baseline mean delta,
and a per-run table — is printed and written to `target/benchmarks/ab/ab-comparison.md`. A run
whose `peak_backlog_messages` reaches the configured cap is flagged; treat such rates as
bounded-pressure results, not maximum throughput.

Both arms are configured and measured by the current tree's harness and load driver, and
`run.toml`'s `git_revision` records that harness provenance for both arms; arm identity lives in
the summary labels and binary paths. Do not run concurrent builds or a second `benchmark-ab` from
the same checkout while a comparison is in flight.

## Adding a workload

Create one directory under `benches/benchmarks/<slug>/` containing `benchmark.toml` and one Upon
template per implementation. The manifest owns the common load shape and tuning parameters:

```toml
name = "example"
description = "What crosses the measured path"
dependencies = ["kafka"]

[load]
duration = "auto"
warmup_seconds = 10
partitions = 16
value_bytes = 128
max_backlog_messages = 4194304
wait_timeout_seconds = 120

[load.shape]
kind = "keyed-windowed"
keys_per_cycle = 1024
retained_keys = 768
copies_per_key = 2
count_field = "record_count"

[parameters]
emitter_flush_each = "10ms"
emitter_max_batch_size = "8MiB"

[implementations.nervix]
kind = "nervix"
template = "nervix.nspl.upon"

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

The typed dependency contract is Kafka-to-Kafka. The shared test-environment crate retains the
other Cucumber dependency starters, but a workload using one of them needs a corresponding typed
benchmark dependency before it is exposed in a manifest, and a workload whose output cardinality
neither declared shape describes needs a new `[load.shape]` variant with its own exact parity
arithmetic.

## Results

Every run writes to:

```text
target/benchmarks/<workload>/<implementation>/<run-id>/
```

Artifacts include the resolved parameters, rendered configuration, subject log, image identity for
container runs, load-driver log, and the count/rate report. `output-diagnostics.json` retains final
per-partition counts and, for windowed output, the last 32 summaries with their Kafka partition and
offset. `container-diagnostics/` retains inspect output, a final resource snapshot, and logs for
Kafka and container subjects. A Nervix run attempts the end-of-run `/metrics` scrape even after a
load or parity failure, writing the raw response to `nervix-metrics.prom` and, when valid, its
benchmark projection to `nervix-metrics.toml`. The comparison reports `messages_per_batch` p50,
p90, and p99 bucket upper bounds for every runtime target, the exact mean from `messages_total /
batches_total`, and `relay_buffer_len` p50, p90, and p99 bucket upper bounds.

`run-all` writes `benchmark-comparison.md` from the exact run directories produced by that
invocation; it never selects unrelated runs by timestamp. It attempts every declared workload and
implementation even when an earlier entry fails, and records every execution in a status table
before the completed measurements and failures. A run passes only after the output records the
workload's shape expects have arrived and remained stable for a confirmation interval and, for
Nervix, the metrics scrape has been parsed successfully.

This is a single-host end-to-end benchmark. Its rate includes Kafka, the load driver, the selected
product, and the output drain. Compare products only with identical workload inputs, and do not use
a run whose `peak_backlog_messages` equals the configured cap as a maximum-throughput result.
