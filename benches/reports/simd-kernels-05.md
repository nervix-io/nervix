# SIMD kernels 05: typed window admission

## Reproduce

The baseline is `d2d8d4722455d4bca9dcac43bded3612a3c9997b` (`origin/main` when the
same-host A/B started); the candidate is the revision containing this report. The candidate's
`Cargo.lock` blob is `6ecc62ecde1d8e8707f9dbac321e484af67ba790`. Measurements were made on
2026-09-29 UTC on an Intel Core i9-14900HX with 32 logical CPUs, x86-64 Linux, rustc 1.99.0,
LLVM 22.1.8, Arrow 58.4.0, and Criterion 0.5.1. Release builds use the repository's configured
kache wrapper and `target-cpu=native`. Other worktrees were building on this host, so small
timing differences are diagnostic rather than a stable speedup claim.

```bash
just bench-window-admission window_admission_per_run --sample-size 10 --warm-up-time 0.5 --measurement-time 1
just benchmark-ab origin/main 3 kafka-dedup-window --partitions 1 --duration-seconds 30 \
  --parameter sketch_enabled=true --parameter sketch_precision=10
```

The Criterion fixture folds one 4,096-row typed run with nullable i64 sums, packed boolean
counts, centered f64 moments, and fixed-range histogram indexes. It measures these kernel calls
as one group, not a complete window processor. The Kafka workload uses one partition and 128-byte
values, with 768 keys and two copies per cycle; 576 keys survive filtering and deduplication.
It closes windows after 500 messages or one second and enables the precision-10 HLL sketch.
The harness interleaves three baseline and three candidate runs, checking the exact summed
record count in output. Its throughput includes Kafka, the load driver, and the rest of Nervix.

## Generated instructions

Inspection of the optimized Criterion binary's AVX-512 `sum_i64` specialization with `nm -C` and
`objdump -d` found four `vpaddq` vector additions and two `vpsraq` arithmetic vector shifts in
the lane fold. The high and low 32-bit halves are reduced to i128 after vector accumulation.
The instruction evidence establishes vector arithmetic on this host for the exact i64 kernel;
the run also contains scalar validity selection and irregular histogram scatter. It does not
establish an AArch64 instruction shape.

## Criterion observation

The 4,096-row run's median was **30.696 µs** (Criterion interval 28.058–33.268 µs), or
**133.44 million rows/s** (interval 123.12–145.98 million). One of ten samples was a high
outlier. `/usr/bin/time -v` measured 11.75 s user CPU, 0.89 s system CPU, and 225,088 KiB peak
RSS for the whole `just` invocation; those process totals include compilation and benchmark
analysis and are not per-row costs. This fixture combines four kernels and does not isolate their
individual throughput.

## Same-host Kafka A/B

| Paired round | Baseline messages/s | Candidate messages/s | Candidate difference |
|:--|--:|--:|--:|
| 1 | 102,863 | 101,767 | −1.07% |
| 2 | 103,574 | 102,738 | −0.81% |
| 3 | 102,947 | 104,745 | +1.75% |
| Mean | 103,128 | 103,083 | −0.04% |

All six runs passed the aggregate-count output parity check. Every run reached a peak backlog
of 130,560 messages against the 131,072-message cap, so these are bounded-pressure rates rather
than measured maximum throughput. The last paired round reversed the direction of the first two;
the mean difference is too small to support either a speedup or a regression claim under this
host's concurrent build activity. The harness comparison and per-run reports are retained under
`target/benchmarks/ab/` locally.

## Semantic and coverage checks

The Cucumber outline “Typed run admission preserves exact aggregates and centered variance
across branches” passed on one and three nodes. It verifies nulls, one non-finite refusal,
exact SUM/COUNT/COUNT_IF/MIN/MAX, AVG/VAR, and a histogram percentile over interleaved branches.
The complete window statistics feature passed 26 scenarios, including sliding retraction and
snapshot restore. The window sketch feature passed five scenarios, including owner promotion and
branch eviction. The 23 kernel unit tests compare every SIMD level available on this host with
exact scalar results and cover a finite mean whose ordinary sum overflows. The 1,478 server unit
tests cover type pairs and sliced validity offsets. Final LCOV reports from `just coverage-lib`
for the kernel and server packages cover **1,182 of 1,261 changed executable Rust lines (93.7%)**.

```bash
just coverage-lib target/window-kernel-final.lcov --package nervix-simd-kernels
ORT_DYLIB_PATH="$(bash scripts/download_onnxruntime.sh --print-path)" \
  just coverage-lib target/window-server-final.lcov --package nervix-server --features testing
```
