# Kafka end-to-end benchmark

This benchmark builds and starts a real release `nervix-server`, provisions two fresh Kafka topics,
and loads parallel unbranched ingestor-to-emitter lanes. Every lane has one consumer in the same
consumer group, its own relay and emitter, and publishes to the same output topic. Kafka assigns
the equally sized partition set across those consumers; the driver sends round-robin across the
partitions and warms every partition before measurement.

The graph uses `MODE NO_ACK PARALLEL` so a long route flush interval does not serialize Kafka
consumption behind the preceding attached sink ACK. This makes it a throughput benchmark rather
than an ACK/redelivery-semantics benchmark. The producer still waits for Kafka acknowledgements,
and every run finishes only after the output topic has the exact same message count and that count
remains stable through a short confirmation interval.

Run it from the repository root:

```bash
just bench-kafka-e2e
```

The optional positional recipe arguments are duration in seconds (or `auto`), partition/lane count,
JSON value bytes, maximum accepted-input-minus-observed-output backlog, ingestor flush interval,
ingestor logical maximum batch size, emitter flush interval, and emitter logical maximum batch
size. Both pairs default to `10ms / 8MiB`, matching the original shared setting. Keep the ingestor
pair fixed while sweeping the emitter pair to isolate sink batching behavior:

```bash
just bench-kafka-e2e 30 16 128 4194304 10ms 8MiB 100ms 64MiB
```

The default `auto` duration is the greater of 30 seconds and twelve cycles of the slower ingestor or
emitter flush interval. This keeps short-cadence runs quick while giving long-cadence configurations
repeated steady-state cycles. For example, an emitter-only `20s / 1GiB` measurement runs for 240
seconds without manually calculating the duration:

```bash
just bench-kafka-e2e auto 16 128 1048576 10ms 8MiB 20s 1GiB
```

Pass an integer duration to override the policy when reproducing a run exactly.

The backlog bound must also cover records accumulated between large timed flushes. A run that
repeatedly reaches `peak_backlog_messages == configured_max_backlog_messages` is producer-throttled
by that safety bound and should be repeated with a larger value when measuring maximum throughput.

The runner requires Linux, Docker Compose, and the normal Rust/web-console build toolchain. It
starts only the `kafka` Compose service, not the complete dependency stack. If Kafka was already
running, it is left running; otherwise it is stopped after the run. Run-specific topics are
deleted after a successful benchmark and retained after a failure for diagnosis. Set
`NERVIX_BENCH_KEEP_TOPICS=1` to retain them after success.

Each run writes a report and its evidence under `target/benchmarks/kafka-e2e/<run-id>/`:

- stable broker-acknowledged input and Kafka output count parity;
- producer and end-to-end message and payload rates;
- configured and observed peak backlog, backlog at producer completion, and post-producer drain
  time;
- Nervix ingestor/emitter message, batch, and Arrow-byte counter deltas, asserted against the Kafka
  counts;
- independent ingestor and emitter flush intervals and logical batch-size limits;
- server CPU, peak RSS, and ending jemalloc gauges across load, drain, and parity confirmation;
- the Git state, whether compilation was skipped, and hashes of all three executables;
- raw Prometheus snapshots, `DESCRIBE` output, rendered NSPL, and process logs.

This is a single-host, end-to-end benchmark. Its throughput includes the local Kafka broker and
load driver and should not be presented as an isolated Nervix microbenchmark. Compare runs only on
equivalent hardware and with the same payload, lane, delivery-mode, backlog, and flush settings.
Count parity does not inspect record identities or payload equality; this benchmark is intended to
measure and reconcile the number of records at the two Kafka boundaries.
