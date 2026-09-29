# SIMD kernels 04: columnar ingest admission

## Reproduce

The baseline was `658822567be472fc1c40ff2ce9f458e21c4613a6` (`origin/main` after the
branch update); the candidate was this working tree. The A/B run was made on 2026-09-29 UTC on
an Intel Core i9-14900HX with 32 logical CPUs and AVX2, x86-64 Linux, using rustc 1.98.0 and
the repository's configured `target-cpu=native` build. Both server binaries were built in release
mode with the configured kache compiler wrapper. The same benchmark harness interleaved three
baseline and three candidate runs on this host.

```bash
just benchmark-ab origin/main 3 hot-path-ingest
```

The workload forwarded concurrent Kafka inputs to one Kafka output with 16 partitions, 128-byte
values (164 bytes on wire), a 10-second warm-up, a 30-second generation interval, and a 131,072
message backlog cap. Both arms used `FLUSH EACH 10ms` for the ingestor and emitter. Each run
validated the exact number, IDs, and values of output records.

## End-to-end observations

| Round | Baseline messages/s | Candidate messages/s | Paired difference |
|:--|--:|--:|--:|
| 1 | 542,775 | 495,003 | −8.8% |
| 2 | 502,735 | 503,119 | +0.1% |
| 3 | 348,305 | 506,777 | +45.5% |
| Mean | 464,605 | 501,633 | +8.0% |

Every run reached the backlog cap and achieved exact output parity. The third baseline run was
much slower than its preceding runs while candidate runs stayed between 495,003 and 506,777
messages/s. This spread makes the +8.0% mean unsuitable as a speedup claim. These are
bounded-pressure end-to-end rates that include Kafka and the load driver, not the maximum rate
of the admission kernel. The workload uses an unpaced domain, so it measures the ingest-group
metadata and selection changes around the new path rather than paced-window SIMD arithmetic.
The individual load reports and harness comparison are under `target/benchmarks/ab/` locally.

## Semantic and coverage checks

The outline “One decoded batch admits each timestamp against the reached clock window” passed
on one and three nodes. It sends one decoded batch across the first retained center, an interior
period boundary and gap, and the last reached center, and observes accepted rows and rejected
rows on separate subscriptions. The complete paced-domain feature passed all 16 scenarios after
updating from `main`. Unit tests compared every SIMD level available on this host, including the
scalar fallback, against exact scalar admission across skew, period, and signed timestamp
boundaries. Combined unit and outline LCOV data covered 372 of 378 changed executable Rust lines
(98.4%) before the branch update, which did not change those source lines.
