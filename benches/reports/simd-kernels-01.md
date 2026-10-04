# SIMD kernels 01: live JSON ingest measurements

## Reproduce

Baseline `db3247192e728808588b3bcbbf84a987e7360077`; candidate is this change. The
candidate `Cargo.lock` blob is `81a5b59c9f9745ceb4af981ec6563a0e1ddd414f`. Measurements were made on
2026-09-26 on a 32-logical-CPU Intel Core i9-14900HX, x86-64 Linux, with rustc 1.99.0,
LLVM 22.1.8, Arrow 58.4.0, and simd-json 0.17.3. Other worktrees were compiling and testing on
the host during the measurements, so the observed spread limits the precision of the comparison.

```bash
just benchmark-ab db3247192e728808588b3bcbbf84a987e7360077 3 kafka-filter-map \
  --partitions 1
just benchmark-ab db3247192e728808588b3bcbbf84a987e7360077 3 hot-path-ingest \
  --partitions 1
```

The first `kafka-filter-map` attempt used the workload's default 16 partitions. Baseline setup
stopped before measurement because applying its setup transaction exceeded the consensus storage
admission budget. The one-partition runs above use the workload unchanged apart from its partition
count and compare both binaries under the same load.

## Direct decode path

Each open ingest group now owns mutable payload scratch and reusable `simd_json::Buffers`. The
host copies a connector's borrowed payload into that scratch, parses it once as borrowed simd-json
values, and appends those values directly to the group's typed Arrow builders. Codec compilation
pre-resolves schema keys with `KnownKey`; conversion uses the builder's already-resolved Arrow
data types, parses each datetime once, and streams base64 chunks directly into the binary builder.

The schemaful JSON path no longer constructs a serde JSON tree or an intermediate row map. The
serde JSON `preserve_order` feature remains enabled only for JAQ's public object-member-order
contract. The dispatched parser supplied by simd-json is the SIMD component measured here; this
change makes no additional generated-instruction claim for scalar conversion into Arrow builders.

## Serialized same-host A/B

Both workloads ran three interleaved rounds per arm. Every run preserved exact output parity.

| Workload | Baseline range | Baseline mean | Candidate range | Candidate mean | Mean delta |
|:--|--:|--:|--:|--:|--:|
| `kafka-filter-map` | 108,740–109,327 msg/s | 109,060 msg/s | 106,520–112,856 msg/s | 109,112 msg/s | +0.0% |
| `hot-path-ingest` | 109,151–111,668 msg/s | 110,047 msg/s | 104,321–112,333 msg/s | 109,125 msg/s | −0.8% |

All six `kafka-filter-map` runs reached its 4,194,304-message backlog cap. All six
`hot-path-ingest` runs reached its 131,072-message cap. The rates therefore describe behavior
under the configured bounded pressure and do not establish maximum throughput. The candidate
shows no repeatable material regression in these runs: the small mean changes are within the
observed run-to-run spread and the two workloads move in opposite directions.

## Semantic checks

The schemaful JSON unit suite covers every scalar and nested sequence type, exact integer shape
and range checks, optional nulls, invalid UTF-8 and escapes, decoder recovery, retained scratch and
parser-buffer reuse, and multi-chunk direct base64 decoding. The public HTTP ingestion scenario
runs on one and three nodes and verifies every JSON wire scalar, nested sequences, a null optional
field, strict-field rejection, malformed JSON reporting, and subscription delivery.

The final feature coverage run passed 1,245 server unit tests and all 23 codec-ingestion scenarios
(203 steps), including the one-node and three-node examples above. The LCOV intersection with the
patch covers 553 of 590 executable changed Rust lines (93.7%). `just validate`, `just ratchet`, and
`just fmt-check` pass on the completed candidate.
