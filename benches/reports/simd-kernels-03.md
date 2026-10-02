# SIMD kernels 03: per-batch delivery latency measurements

## Reproduce

The Criterion baseline is `2444db5aba98e14527b9ba481b4f267f54ee4756`, the commit this change was
written on; the end-to-end baseline is `54d99a63f1e3d8b9f9ceacc54e33490bd2a6a486`, the `main` commit
it was merged onto. The candidate is this change. Measurements were made on 2026-09-28 UTC on a 24-logical-CPU AMD Ryzen AI 9 HX 370 (Zen 5, AVX-512F/DQ/VL/BW),
x86-64 Linux, with rustc 1.99.0, LLVM 22.1.8, Arrow 58.4.0, hdrhistogram 7.6.0, prometheus 0.14.0,
and Criterion 0.5.1. Local builds on this host use `target-cpu=native`. Other worktrees were
building and testing on the same host throughout (load average 19–30), so the timings below carry
wide intervals and are compared only within one round.

```bash
just bench-relay-interaction delivery_observation --sample-size 30 --warm-up-time 2 --measurement-time 5
just build-simd-kernels-x86-64-v3
just benchmark-ab 54d99a63 3 hot-path-processor --partitions 1
```

The Criterion group records one batch's delivery into a node input whose branch-local series sit
beside its global ones, exactly as a processor input records it. The baseline was measured in a
worktree of the baseline commit carrying the same benchmark file and batch fixture, with a driver
that performs the former processor-input sequence: `delivery_observation`, `observe_batch`, and one
`observe_delivery_latency` call per row. The two drivers differ only in that recording call. The
arms ran interleaved: baseline, candidate, baseline, candidate, then a third round of both.

## Observation path

A relay batch carries its rows' ingestion watermarks in two Arrow buffers of Unix-nanosecond
timestamps, the values a `TimestampNanosecondArray` holds. A node input hands the high-watermark
buffer and its acceptance instant to the metrics handle. One kernel pass computes the latest
watermark and, for every row at or before the instant, the elapsed nanoseconds rounded to the
nearest millisecond (halves up) and clamped at 30 seconds, then counts each row into the HDR
bucket its units' leading zeros select. Each latency series then merges those buckets into its four
rolling windows under one lock with one wall-clock reading. The Prometheus child receives the batch
through a local histogram and one flush, because the client accepts one sample per call.

## Generated instructions

`objdump -d -C` of the optimized Criterion binary (native build) shows the AVX-512 variant of the
fold as its own target-feature function, running the arithmetic on eight `zmm` lanes: `vpsubq` for
the elapsed time, `vpcmpnltq` for the at-or-before mask, `vpminuq` for the clamp, `vcvtuqq2pd`,
`vdivpd`, `vrndscalepd` (floor), and `vcvttpd2uqq` for the rounded units, and `vpmaxsq` for the
latest watermark. `vpextrq` appears where each valid lane is counted into its bucket, which is the
scalar scatter the design keeps.

`just build-simd-kernels-x86-64-v3` builds the kernel crate for the Docker image's x86-64-v3
payload. There the AVX2 variant is inlined into its caller and runs on four `ymm` lanes: `vpsubq`,
`vpcmpgtq` with `vblendvpd` for the mask, clamp, and maximum, the exact `u64`-to-`f64` sequence
(`vpblendd`, `vpsrlq`, `vpor`, `vsubpd`, `vaddpd`), `vdivpd` and `vroundpd` for the rounded
quotient, `vpsrlvq`/`vpsllvq` back to integers, and `vmovmskpd` for the lane mask. The scalar
`vdivsd`/`vroundsd` in the same function belong to the forced fallback level. This is
generated-instruction evidence for x86-64 on this toolchain; no AArch64 instruction claim is made.
Unit tests compare every level the host supports and the forced fallback with an integer reference
on random and boundary inputs.

## Criterion observations

Median time per batch, with Criterion's interval, for each round. Rounds 1 and 2 measured the
candidate before its watermark columns became plain Arrow value buffers and before `main` was
merged; round 3 measured the final code on a less loaded host (load average 9–14).

| Case | Baseline round 1 | Candidate round 1 | Baseline round 2 | Candidate round 2 |
|:--|--:|--:|--:|--:|
| 1 row | 1.344 µs (1.286–1.429) | 2.672 µs (2.376–3.108) | 1.568 µs (1.400–1.741) | 1.269 µs (1.234–1.301) |
| 64 rows, 1 ms apart | 25.89 µs (24.35–26.98) | 10.56 µs (9.70–11.38) | 21.71 µs (20.72–23.00) | 5.36 µs (5.20–5.60) |
| 1,024 rows, 1 ms apart | 356.7 µs (349.8–364.6) | 58.4 µs (55.4–61.2) | 337.3 µs (323.1–354.4) | 43.8 µs (40.2–46.4) |
| 1,024 rows, one watermark | 468.6 µs (446.9–503.2) | 36.5 µs (32.6–40.4) | 288.8 µs (285.0–293.1) | 10.4 µs (10.1–10.6) |

| Case | Baseline round 3 | Candidate round 3 |
|:--|--:|--:|
| 1 row | 1.312 µs (1.231–1.399) | 1.070 µs (1.051–1.090) |
| 64 rows, 1 ms apart | 20.96 µs (19.91–22.18) | 4.131 µs (4.086–4.199) |
| 1,024 rows, 1 ms apart | 322.9 µs (305.2–339.9) | 30.66 µs (29.84–31.62) |
| 1,024 rows, one watermark | 415.5 µs (377.6–462.8) | 7.896 µs (7.728–8.071) |

The former path cost roughly 300 ns per row whatever the watermarks were: every row took the
series lock twice (global and branch), read the wall clock twice per lock, converted its domain
timestamp through chrono, and updated the Prometheus child's atomics. Within each round the
per-batch path is 2.5–5× faster at 64 rows, 6–10.5× faster at 1,024 rows spread 1 ms apart, and
13–53× faster at 1,024 rows sharing one watermark. Its cost now depends on how many distinct
latency buckets a batch fills: rows of one ingest group share one watermark, and the 1,024-row
case with one watermark is dominated by the per-row Prometheus local fold, while 1,024 rows spread
1 ms apart fill about 512 buckets that each window merges. A single row costs about the same as
before; the first candidate round's single-row interval is an outlier of the shared host.

## Same-host end-to-end observations

The A/B harness interleaved three baseline and three candidate runs of `hot-path-processor` on one
host, each with a 10-second warm-up and a 30-second measurement at one partition, and checked exact
output parity. Baseline rates were 110,260, 109,215, and 110,745 messages/s (mean 110,073);
candidate rates were 109,468, 108,125, and 110,184 messages/s (mean 109,259, −0.7%). Every run
reached the 131,072-message backlog cap, so these are bounded-pressure rates rather than maximum
throughput, and the difference lies inside the run-to-run spread. Recording delivery was not the
limit of this workload at its 10 ms flush cadence, and this run establishes no end-to-end speedup.

The retained `/metrics` scrapes show both arms exporting one delivery-latency sample per received
message at the junction and the emitter (4.70 and 4.69 million in the first run of each arm), and
every one of the six runs places both targets' p99 in the 10–50 ms bucket.

## Semantic checks

The Cucumber outline "Node inputs record each batch's delivery latency from its rows' ingestion
watermarks" (one and three nodes) passed on the baseline before any product change and passes
unchanged on the candidate. It pins exact `DESCRIBE` percentiles and domain rates at two junctions,
a reingestor, and an emitter, Prometheus bucket counts, negative latencies that are not recorded, a
second batch older than the watermark every input has seen, and two tenants' interleaved rows.
Kernel unit tests compare every supported level and the forced fallback with an integer reference,
check the bucket layout against HdrHistogram for every unit of seven ranges at zero to three
significant figures, and pin rounding at the half-millisecond boundary. A metrics unit test shows
that a batch folded by the kernel fills exactly the HDR counts recording each latency one at a time
fills, across 0–31 s and beyond the clamp. After merging `main`, the server's 1,376 unit tests and
the outline passed again, and the server unit tests covered 615 of the 628 changed server lines
(97.9%); the remaining lines are call sites the Cucumber scenarios exercise.
