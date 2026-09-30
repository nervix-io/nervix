# Retained task handle measurements

## Reproduce

Measured on 2026-09-30 UTC on `gleb-ryzer`, an Intel Core i9-14900HX with 32 logical CPUs,
x86-64 Linux, rustc 1.98.0 and LLVM 22.1.8. The base revision is
`b3ef3bdc0b450d5f52dcf2bb957509799a825723`; the candidate is the change containing this report.
The repository's configured kache wrapper builds the release benchmark with native CPU tuning.
Other worktrees were compiling on the same host, so the timings describe these warmed owner
operations and do not establish an end-to-end throughput improvement.

```bash
just bench-task-handles
just coverage-native-extras bench-smoke
```

`target/task-handles.json` holds 100 raw samples of 1,000 operations for each of the 16 paths,
after 100 warmup operations per path. Timing uses the primitive monotonic instant; allocation
bytes use jemalloc's cumulative thread counter before and after each sample. Samples and result
serialization allocate outside the measured interval. The table divides each sample by 1,000,
reports the median and nearest-rank p95, and reports the median allocated bytes per operation.

| Production owner operation | Median ns/op | p95 ns/op | Allocated bytes/op |
| --- | ---: | ---: | ---: |
| `ClientOutcome` | 18.32 | 22.00 | 0 |
| `Confirmation` | 25.84 | 30.78 | 0 |
| `ErrorRoutePublication` | 17.05 | 18.57 | 0 |
| `Freeze` | 23.06 | 25.03 | 0 |
| `GeneratorAck` | 430.98 | 467.03 | 576 |
| `HealthyStatus` | 17.94 | 18.45 | 0 |
| `IdleForceFlush` | 10.68 | 11.96 | 0 |
| `IngestAck` | 458.96 | 516.98 | 584 |
| `IngestClock` | 195.66 | 213.08 | 0 |
| `KafkaGeneration` | 14.93 | 15.03 | 0 |
| `MetricsDirty` | 13.09 | 13.54 | 0 |
| `ProcessorClock` | 118.37 | 135.86 | 0 |
| `QuiescedPayload` | 16.89 | 21.52 | 0 |
| `ReadyPoolBorrow` | 2.59 | 2.96 | 0 |
| `SubscriptionDrop` | 8.12 | 8.87 | 0 |
| `WasmBoundary` | 38.92 | 47.17 | 0 |

All 100 samples of every path apart from the two acknowledgement roots allocate zero bytes.
An acknowledgement root still allocates its in-memory attempt/completion state; retaining its
tracker removes registry discovery and does not remove that state.

## Scope and synchronization evidence

The fixture executes the production status clear, ready pool-borrow wrapper, confirmation guard,
ingest and generator acknowledgement roots, placement dirty mark, entity freeze observation,
ingestion clock read, Kafka generation read, processor clock snapshot, WASM assignment fence,
idle force-flush check, client outcome child, quiesce child, subscription drop child and prepared
error-route publication read. Handle resolution and owner registration occur before measurement.

The pool fixture supplies an immediately ready connection future and excludes driver/network
costs. Clock fixtures use an unpaced installed domain. The WASM fixture checks local-storage
assignment and excludes guest execution and durable replica waits. The error-route fixture loads
the prepared publication and excludes expression execution and relay delivery. The public
scenarios tagged `@retained_task_handles` cover the associated delivery, retry, pool contention,
clock restart, branch isolation, gate, handoff, checkpoint and force-flush behavior.

The force-flush regression counts actual coordinator acquisitions: repeated idle and already
claimed polls require zero. The same test failed against the initial implementation with 100
acquisitions for 100 idle polls. Pool regressions exercise pending, completion, cancellation and
replacement lifetimes. Status and freeze sequences run the registered Bolero properties, while
production-owner Shuttle checks explore status observations, freeze registration/release and
force-flush generations. Their ordinary hot paths access the retained publications, counters and
metric children recorded in the concurrent map inventory.
