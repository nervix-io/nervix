# SIMD kernels 02: columnar JSON emission measurements

## Reproduce

Baseline `835b5f7c62e7618741ee5aec7150ba6dec347b0b`; candidate is this change. The
candidate `Cargo.lock` blob is `a04beb718bdf5e62797c3c33aee7ebc7fe6abe75`. Measurements
were made on 2026-09-28 UTC on a 32-logical-CPU Intel Core i9-14900HX, x86-64 Linux, with rustc
1.98.0, LLVM 22.1.8, Arrow 58.4.0, and Criterion 0.5.1. Other worktrees were building and
testing on the same host, so small timing differences require caution.

```bash
just bench-json-encode --sample-size 20 --warm-up-time 1 --measurement-time 3
just benchmark-ab 835b5f7c62e7618741ee5aec7150ba6dec347b0b 3 kafka-filter-map --partitions 1
```

The A/B run uses one partition on both arms. The preceding SIMD kernels 01 measurement found
that the default 16-partition setup could exceed the baseline's consensus storage admission
budget before measurement began. Both arms retain the same JSON workload and exact output parity
check.

## Encode path and generated instructions

The primitives kernel caches a `fearless_simd` level and dispatches a 64-byte-block classifier
for quotes, backslashes, and bytes below `0x20`. The Arrow string offsets turn those masks into
one escape bit per string. A compiled schemaful JSON codec owns preescaped keys; each batch owns
typed column readers and string masks. Clean strings copy directly, marked strings enter JSON
escaping, numbers write through itoa or ryu, and datetime and base64 output go to the caller's
writer without an extra string scan. The same writer builds ClickHouse `JSONEachRow` lines from
mapped columns. ClickHouse keeps its existing widened `F32` number rendering.

Inspection of the optimized Criterion binary with `nm -C` and `objdump -d -C` found the
classifier's AVX-512 variant using `vpcmpeqb`, `vpcmpltub`, and `kmovq`, and its AVX2 variant
using `vpcmpeqb` and `vpmovmskb`. This is generated-instruction evidence for the x86-64 build
on this host. Unit tests compare every supported host level and the forced scalar fallback with
a byte-by-byte reference on random and boundary inputs. No AArch64 instruction claim is made.

## Criterion observations

The 1,024-row fixture has an integer column, a clean UTF-8 column, and a string column with one
quoted/control row in four. The columnar measurement includes classification once per batch.
The direct serde reference writes the same fields from a Rust struct; it is cheaper than the
former server row view and is not a baseline for the product change.

| Encoder | Median per 1,024 rows | Time interval | Throughput interval |
|:--|--:|--:|--:|
| Columnar Arrow writer | 77.788 µs | 75.968–79.968 µs | 12.805–13.479 million rows/s |
| Direct serde struct | 74.034 µs | 73.084–75.160 µs | 13.624–14.011 million rows/s |

The intervals overlap narrowly and the direct serde reference is slightly faster at their
medians on this small shape. This microbenchmark alone establishes no server speedup.

## Same-host Kafka A/B observations

The benchmark harness interleaved three baseline and three candidate runs on one host, using a
30-second measurement and 10-second warm-up for each run. It checked exact output parity. The
baseline rates were 106,782, 106,678, and 98,560 messages/s; candidate rates were 107,148,
107,116, and 109,970 messages/s. Mean end-to-end rates were 104,007 and 108,078 messages/s,
respectively (+3.9% for the candidate). The baseline range was 98,560–106,782 messages/s and
the candidate range was 107,116–109,970 messages/s.

All six runs reached the 4,194,304-message backlog cap. These are bounded-pressure rates, not
maximum sustainable throughput. The third baseline run was also slower than its first two on
this shared host. The measured mean difference is therefore useful as a same-host observation,
but does not isolate the JSON encoder's contribution or establish a general speedup. The full
comparison and run artifacts are under `target/benchmarks/ab/` after reproduction.

## Semantic checks

The writer's unit tests compare exact serde JSON bytes for clean and escaped UTF-8, quoted keys,
all integer widths, finite and nonfinite floats, optional and nested nulls, fixed and variable
lists, datetimes, and base64 bytes across chunk boundaries. ClickHouse's mapped-column test
checks its current `F32` bytes. The existing bounded-codec tests sweep payload limits across
quotes, controls, Unicode, and length boundaries and preserve exact byte measurement.

The Kafka emission feature passed 13 scenarios and 148 steps, including the new one- and
three-node outline named “Kafka JSON emission preserves exact bytes for escaped and clean
columns.” It checks exact broker bytes for clean and escaped strings, sensitive field exclusion,
an omitted optional null, datetime, and bytes. The LLVM coverage run passed 1,314 server tests,
six column writer tests, six ClickHouse tests, and three kernel tests. Its changed Rust lines
covered 688 of 707 executable lines (97.3%). `just validate` and `just ratchet` passed.
