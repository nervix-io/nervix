# VM functions 18: Arrow workload measurements

## Reproduce

Baseline `b02ef87b60a54a2278e8bab8d742b94761769d1b`; candidate is this change. The
`Cargo.lock` blob is `61d5f820a54403ea652efdd19709ed5ca53fc754`. Measurements were made on
2026-09-25 on a 32-logical-CPU Intel Core i9-14900HX, x86-64 Linux, with rustc 1.98.0,
LLVM 22.1.8, Arrow 58.4.0, Criterion 0.5.1, Aho-Corasick 1.1.5, and simd-json 0.17.3.
Other worktrees were building on this host during part of the run, so short timings are
diagnostic, not a statistically stable throughput claim. `perf` hardware counters were unavailable
(`perf_event_paranoid=4`). The workload shapes and commands are described in
[`benches/README.md`](../README.md#expression-vm-function-workbench).

```bash
just bench-vm vm_workload_shape --sample-size 10 --warm-up-time 0.5 --measurement-time 1
just bench-vm i64_arithmetic --sample-size 10 --warm-up-time 0.5 --measurement-time 1
just bench-vm json_extraction --sample-size 10 --warm-up-time 0.5 --measurement-time 1
just bench-vm conditional_arm --sample-size 10 --warm-up-time 0.5 --measurement-time 1
just bench-vm-alloc vm_workload_shape --test
just benchmark-ab b02ef87b 3 hot-path-processor --partitions 1 --duration-seconds 10 \
  --max-backlog-messages 2097152 \
  --parameter "'transform_expression=CASE WHEN contains_any(input.value, vec(input.value)) THEN upper(input.value) ELSE input.value END'"
just benchmark-nervix-local kafka-dedup-window --partitions 1 --duration-seconds 10 \
  --parameter sketch_enabled=true --parameter sketch_precision=10
```

Each VM fixture compiles a complete expression plan and executes Arrow batches through the
existing VM harness. The shape probe uses a 1,024-row arithmetic batch, a 17-row offset slice of
the same logical data, ragged list batches, 32- or 1,024-byte ASCII/UTF-8 text with zero or 50%
nulls, and `repeat` counts of 1, 8, and 64. The existing harness retains the 64–65,536-row
sweep, integer/float failure density, selection, calendar, network, JSON, and constant/dynamic
string search groups. The Kafka workload produces 128-byte values and exact one-for-one output
parity. The sketch workload generates distinct string keys in full windows of 100 or 500.

## VM observations

Criterion medians below are time per batch. Division by row count gives the corresponding
throughput. These are candidate shape observations, not baseline comparisons.

| 1,024-row fixture unless noted | Median | Approximate rows/s |
|:--|--:|--:|
| Arithmetic, 1 row | 3.44 µs | 0.29 million |
| Arithmetic, 8 rows | 3.38 µs | 2.37 million |
| Arithmetic, 1,025 rows, blocking-pool path | 25.17 µs | 40.7 million |
| Arithmetic, contiguous / sliced | 10.52 / 10.39 µs | 97.3 / 98.5 million |
| Ragged list, contiguous / sliced | 384.74 / 402.82 µs | 2.66 / 2.54 million |
| Text search, 32-byte ASCII / 1,024-byte UTF-8 | 117.76 / 143.61 µs | 8.70 / 7.13 million |
| Repeat expansion, 1 / 8 / 64 copies | 19.59 / 25.76 / 40.18 µs | 52.3 / 39.8 / 25.5 million |
| Integer arithmetic, no / sparse / dense failures | 8.09 / 15.48 / 51.07 µs | 126.6 / 66.2 / 20.1 million |
| JSON, four fields from one / four columns | 548.58 / 1,861.2 µs | 1.87 / 0.55 million |
| Conditional regex arm, none / 1% / 50% selected | 2.45 / 5.25 / 30.05 µs | 417 / 195 / 34.1 million |

The 1,025-row result includes the executor transition at 1,024 rows; compare it with a matched
1,024-row fixture to estimate that hop rather than treating a change in row count as pure
offload cost. The large difference in the ragged list case is the existing algorithm's work, not
evidence of a vectorized kernel. The JSON comparison demonstrates the existing one-parse-per-column
sharing. Dense row errors are materially slower than the clean integer path.

The optional allocator probe reports requested allocation and reallocation bytes for one warmed
execution, including any blocking worker, and the physical memory of output columns. On this
host: contiguous arithmetic had 71 allocation calls / 102,920 requested bytes; sliced arithmetic
had 71 / 102,976; ragged list had 8,192 / 731,792; dynamic search of 1,024 non-null rows had
1,109 / 253,798; `repeat` with 64 copies had 1,045 / 725,594. This is process-wide allocation
evidence, not an exclusive per-function allocation profile. The output-byte metric counts
projected columns too. `/usr/bin/time -v` on one dynamic-search Criterion filter reported 16.50 s
user CPU, 0.83 s system CPU, and 224 MiB peak RSS over 13.21 s elapsed; it includes compilation,
warmup, analysis, and heavy concurrent builder activity. It must not be divided into a per-row VM
CPU cost.

## Serialized same-host A/B

The dynamic `contains_any` cache now checks bounded borrowed Arrow pattern strings directly
against owned cache keys. It creates owned strings and a matcher only on a miss, preserving
null/error handling, the 128-pattern and 64 KiB limits, and the 64-set per-batch cache cap. A
baseline Criterion run of the
1-pattern, 32-byte case had a 126.91 µs median. A later candidate run varied widely and its
171.25 µs median overlapped intense concurrent compiler work; it cannot establish a VM speedup or
regression. The 64-pattern case was 901.55 µs before and 917.46 µs after, also inconclusive.

The serialized end-to-end A/B ran three interleaved rounds per arm with the same expression and
exact output parity. Baseline end-to-end rates were 102,583–109,062 msg/s (mean 106,119); candidate
rates were 102,717–110,237 (mean 107,162, +1.0%). **All six runs filled the 2,097,152-message
backlog cap**, so these are bounded-pressure rates and do not establish maximum throughput. Raw
Nervix delivery histograms for the relay-to-junction target put p99 in the 10–50 ms bucket in
each run; that is a bucket bound, not a precise end-to-end latency percentile. The candidate did
not show a repeatable regression in this workload, but the noisy microbenchmarks prevent a
definitive CPU speedup claim.

## Sketch accuracy and execution classification

The optional Kafka window sketch preserves exact `record_count` as the parity field and records
the estimate separately. From the last 31 full windows in each local run: at precision 10, 500
distinct keys gave mean estimate 498.48, mean absolute relative error 1.74%, range 478–517; at
precision 10, 100 keys gave 100.42, 1.90%, range 95–104; at precision 16, 500 keys gave 500.10,
0.26%, range 497–502. Partial windows were excluded. The precision-10 run yielded 93,557 msg/s,
the precision-16 run 97,741 msg/s, and a no-sketch control 98,211 msg/s. These are single
end-to-end runs with Kafka and unequal system pressure, not a sketch throughput ranking. The
estimates are approximate; the contract and theoretical error bounds are in the public processor
documentation.

Numeric Arrow buffer loops may be auto-vectorized by LLVM. JSON uses `simd-json`'s dispatched
parser; string matching uses Aho-Corasick over Arrow column values and can fall back to scalar
work. The cache change makes no SIMD claim. No generated-instruction or AArch64 claim is made
without target-specific inspection. Exact types, null propagation, per-row errors, sensitivity,
and caller-supplied domain time remain governed by the semantic suites.

## Semantic checks

`just test-vm` passed 551 unit tests, including borrowed cache-key equality and dynamic list
reuse. `just test-scenarios --input tests/features/runtime/string_search.feature --concurrency 1`
passed both one- and three-node scenarios with exact dynamic-search outputs. The benchmark
repository workload tests, Criterion test mode, `just validate`, and `just ratchet` passed. The
VM source patch reached 93.9% line coverage on instrumented changed lines with
`just coverage-lib target/vm-patch.lcov --package nervix-vm`.
