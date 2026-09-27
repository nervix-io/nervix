# Client Wire 16 performance evidence

This report measures the accepted FlatBuffers Row protocol. It retains the task 01 Protobuf
measurement as a historical reference and records the current wire's costs without treating the
exploratory C++ synthetic result as a Nervix speedup. Raw reports are in
[`evidence/client-wire-16`](evidence/client-wire-16/). The performance Cucumber scenarios are
`Capture native gRPC and WebSocket costs with paused subscriber control traffic` and
`The same command is timed over <mode> gRPC`.
Raw JSON and Prometheus files are stored as deterministic `.gz` streams to keep the review diff
small; `gzip -dc tests/evidence/client-wire-16/baseline-final.json.gz` reads one without changing
its content.

## Reproduction and comparability

From the repository root, with the repository's configured `kache` wrapper:

```console
just client-wire-baseline 100 5 1024 target/client-wire16-current
just client-wire-baseline 100 5 1024 target/client-wire16-repeat
just client-wire-cost target/client-wire16-components-before.json
just client-wire-cost target/client-wire16-components-after.json
just client-wire-tls-cost target/client-wire16-tls
just client-wire-binding-cost target/client-wire16-binding-ffi.json
just client-wire-binding-host-cost target/client-wire16-binding-python
```

The first four baseline operations use task 01's exact semantic workload, bounds and recipe:
`DESCRIBE DOMAIN` on one gRPC session; 100 strict JSON HTTP inputs with 1,024-byte strings and
alternating `acme`/`beta` concrete branches, read as typed Row subscription frames; 100 live graph
snapshots on the authenticated console WebSocket; and five one-file, 1,024-byte resource uploads.
The release server and debug measurement harness profiles are unchanged. The report records every
latency and byte count, phase process counters, jemalloc and RSS observations, queue occupancy,
hardware, limits and raw Prometheus scrape. An added native client split runs only after those four
operations. Paused and slowly drained subscribers add 160 records each and time independent control
commands during publication. One-node plaintext/TLS probes use an in-process debug server and are
**relative transport measurements**, not direct additions to the release-server baseline.

The historical Protobuf summary comes from
[`client-wire-acceptance-ledger.md`](client-wire-acceptance-ledger.md#recorded-protobuf-run),
revision `2aa7ee71a2416ef4ef723d3a4355ec66f540fe2f`. Its raw artifact is unavailable in this
checkout, so historical p95 and throughput cannot be reconstructed. Changes made between that
revision and this task, and concurrent compilation on the shared host, also affect a cross-revision
latency comparison. The three current runs bound observed variation; they do not establish a
causal FlatBuffers speedup.

## End-to-end workload

All latency values below are microseconds. Current columns give run A / run B. Throughput is
completed sequential operations per elapsed second, including the debug client and server. The
historical column is task 01 p50 / p99; task 01 did not summarize p95. Runs A and B precede the
presentation edit; the final run follows it and adds measurements only after these four phases.

| Operation | Protobuf p50 / p99 | Current p50 | Current p95 | Current p99 | Current throughput / s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native command | 782 / 4,355 | 4,379 / 4,283 | 4,967 / 4,879 | 5,251 / 5,075 | 224 / 227 |
| Typed HTTP to Row subscription | 29,103 / 59,967 | 27,007 / 25,999 | 30,223 / 29,679 | 32,959 / 33,087 | 35.9 / 37.7 |
| Live graph snapshot | 119 / 2,387 | 242 / 276 | 396 / 454 | 757 / 889 | 3,617 / 2,757 |
| Resource upload | 41,471 / 47,263 | 18,991 / 20,447 | 21,807 / 23,327 | 21,807 / 23,327 | 50.3 / 49.4 |

The final run measured command 2,663/2,773/3,199 µs, typed subscription
20,847/24,895/33,471 µs, graph 149/241/420 µs, and upload 14,943/20,111/20,111 µs
(p50/p95/p99). Sequential throughput was 368, 46.1, 4,380 and 60.9 operations/s respectively.
The release-server native client split measured preparation at 1,893/3,121/3,379 µs and the
prepared request/reply at 832/2,199/2,461 µs. These are separate series with fresh execution
identities; their quantiles should not be added as if sample order were correlated. Host contention
also changed between runs, so the lower final end-to-end times are not attributed solely to the
presentation edit.

| Median request / response bytes | Protobuf | FlatBuffers A / B | Explanation |
| --- | ---: | ---: | --- |
| Native command | 93 / 1,508 | 176 / 1,640 | Current typed request and outcome carry explicit identities and fields. |
| Typed subscription | 1,067 / 1,142 | 1,067 / 1,320 | The HTTP JSON request is identical; the response is one typed Row frame. |
| Graph snapshot | 37 / 3,647 | 96 / 3,877 | Graph content remains live JSON inside the control snapshot. |
| Resource upload | 2,663 / 73 | 2,801 / 176 | The archive payload is identical; its framing and typed acknowledgement grew. |

For the historical four phases, server CPU was 34/7, 32/8 and 26/7 user/system ticks in A, B and
final at 100 ticks/s, compared with 36/18 in task 01. The debug client used 293/26 and 287/26
ticks in A and B. Server jemalloc resident after these phases was 58.1, 63.1 and 59.7 MB,
bracketing the historical 61.0 MB. Process peak RSS at that point was 142.3, 104.7 and 141.1 MB
versus the historical 94.1 MB. The large
variation in initial RSS across the current runs (133.0 versus 95.5 MB before measurement) makes
RSS unsuitable for claiming a protocol memory delta on this host. All raw reports retain the
before/after counters.

Control commands under 160-record paused and slow subscriber workloads completed in both runs.
Paused p50/p95/p99 was 5,295/9,879/10,151 µs in A and 4,827/5,487/7,167 µs in B. Slow p50/p95/p99
was 4,991/6,363/7,131 µs in A and 5,611/9,095/9,775 µs in B. The slow reader observed Row and
overflow events; a short drain timeout is recorded as a normal observation, since an overflow can
represent a whole burst. Queue samples and full event counts are in the raw reports. These values
are *control-command* latency; cycle throughput also includes eight HTTP publications per command.
The final run measured paused control at 3,645/3,931/4,037 µs and slow control at
3,743/4,115/4,139 µs (p50/p95/p99).

## Row stages and retained buffers

The component workload uses the server's `SubscriptionRowEncoder` over typed Arrow columns, with
the host's 256-row frame cap. It includes 100 alternating one-row branch frames, 1,024 narrow rows,
100 wide rows with strings/binary/nulls/redaction, 4,096 rows of 1 KiB detail, 256 rows of 16 KiB
detail that hit the byte limit, and every fourth row selected from 1,024 source rows. Each stage has
100 raw nanosecond and allocated-byte samples plus p50/p95/p99 in the JSON reports. This is a
single-thread component measurement, separate from network, admission and scheduling.

| Case | Rows / frames / wire bytes | Arrow to wire p50 | Verify p50 | Borrowed full scan p50 | Own all cells p50 | JSON text p50 after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Task 01 alternating | 100 / 100 / 131,192 | 112 µs | 52 µs | 7 µs | 15 µs | 100 µs |
| Narrow batch | 1,024 / 4 / 135,896 | 415 µs | 250 µs | 47 µs | 91 µs | 428 µs |
| Wide nullable/redacted | 100 / 1 / 124,576 | 271 µs | 132 µs | 28 µs | 50 µs | 187 µs |
| Large batch | 4,096 / 16 / 4,672,376 | 2,441 µs | 1,263 µs | 393 µs | 873 µs | 3,609 µs |
| Byte-limited batch | 256 / 2 / 4,224,416 | 332 µs | 134 µs | 25 µs | 160 µs | 1,578 µs |
| Selected quarter | 256 / 1 / 33,968 | 100 µs | 58 µs | 25 µs | 20 µs | 87 µs |

The task 01 shape constructs about 814,000 rows/s at the component p50 (100 / 123 µs before the
presentation edit; 100 / 112 µs after). That throughput is only for Arrow-to-wire construction,
not end-to-end subscription admission. On 1,024 narrow rows, borrowed selective access reads one
cell per frame in about 0.2 µs and allocates no bytes; full scan reads all 3,072 cells in 47 µs and
also allocates none. Owning materialization allocates 172 KB; JSON presentation allocates 724 KB
after the edit. The wide case's presentation allocation fell from 961 KB to 730 KB, and the narrow
case from 1,148 KB to 724 KB, because escaped field names and their order are now computed once per
batch. The single-row alternating case changed from 1,020 KB to 1,026 KB of allocated bytes: it
cannot amortize per-batch names. Timing also improved across unrelated stages between the two
builds on this busy host, so the allocation reductions, not the whole time delta, are the reliable
evidence for that edit.

Frame construction writes each Arrow column value directly into FlatBuffers tables. There is no
record-shaped JSON object, map of fields, or intermediate row payload in the delivery path;
`ArrowRowBatch` validates columns once per batch and writes selected cells through `CellWriter`.
The current copies and ownership changes are:

| Boundary | What happens | Evidence / cost |
| --- | --- | --- |
| Arrow to builder | Variable strings and binary are copied into the FlatBuffers builder; fixed scalars become tables. Cell and row offset vectors are temporary. | 100 alternating rows: 131 KB wire, 437 KB cumulative allocation; 123 µs p50 before display tuning. |
| Builder to queued frame | `collapse()` gives the builder's vector to `Bytes`, sliced at its live head. The queue and borrowed views share the backing buffer. | 4.67 MB of large-batch wire held 8.38 MB of live allocations; live allocated bytes returned to baseline on release. |
| gRPC sender | Tonic's encoder copies frame bytes into its output buffer. | 2–3 µs for 100 small frames / ~77 µs for 4.67 MB in the first run. |
| gRPC receiver | A small decoded frame is detached into owned `Bytes` below 64 KiB; a large frame shares the receive buffer. | 100 small frames: 3.3 µs and 154 KB allocation; one 125 KB wide frame: no detach allocation. |
| Verification and views | FlatBuffers verifies structure and UTF-8, then views borrow from the verified frame. | No allocation in verification or borrowed scans; no per-row object copy. |
| WebSocket | Binary messages require an owned `Vec<u8>`; converting a shared Row frame copies. | 3.8 µs / 154 KB for the small frames, ~79 µs / 4.72 MB for the large batch in the first run. |
| Host binding | The C ABI exposes a borrowed frame or copies selected columns to caller buffers. A retained event is an `Arc` reference. | See FFI and host raw reports; no mandatory Arrow dependency or embedded Row JSON. |

The large frame's jemalloc resident pages remained cached after release, while live allocated bytes
returned to baseline. This is allocator reuse, not a held Row frame. The byte-limited case held
4.28 MB for 4.22 MB wire, showing the 256-row cap and frame-byte limit prevent one unbounded
buffer. Trimming every builder buffer would add a whole-frame copy and would work against the
small-frame latency; no buffer-lifetime or batch-limit change is justified by these runs.

## Transport, bindings and budgets

The one-node plaintext/TLS raw probes keep the same `DESCRIBE DOMAIN` command and client. They
separately time connect, `Client::execute`, command preparation and execution of a freshly prepared
command. The debug in-process server means these figures are a relative transport comparison only.

| Mode | Connect | Command p50 / p95 / p99 | Prepare p50 | Prepared execution p50 |
| --- | ---: | ---: | ---: | ---: |
| HTTP | 2.11 ms | 4.924 / 6.231 / 6.729 ms | 2.758 ms | 3.772 ms |
| HTTPS | 4.33 ms | 6.144 / 8.136 / 9.481 ms | 3.132 ms | 4.641 ms |

The C ABI probe uses one verified 100-row, 46,600-byte frame in a debug unit-test process. Raw
borrowed frame access is 60 ns p50; `Arc` retain/release is 80 ns; copying 100 cell states, fixed
`I64` cells and variable string cells into caller buffers costs 64, 68 and 102 µs p50. The live
CPython probes passed the complete one- and three-node conformance scenarios. Both profiled a
one-row, 720-byte frame; p50 ranges across the two runs were 3.1–3.6 µs for a borrowed
`memoryview`, 3.3–3.7 µs for an owned `bytes` copy, 0.78–0.87 µs for a ctypes retain/release,
2.9–3.3 µs for a fixed column and 5.0–5.7 µs for a variable column. Releasing 256 retained host
references and collecting GC took 0.9–1.8 ms p50 over 20 samples. These host figures include
Python call overhead and are not extrapolated to 100-row batches.

The debug client spent about 0.43 s of CPU on 100 commands in runs A and B, nearly their full
0.44 s wall time. Its command-classification parser and preparation are a substantial part of the
cross-revision command difference: in the final release-server run, preparation was 1.89 ms p50
and the prepared request/reply was 0.83 ms p50. The historical raw client CPU and parser
samples are unavailable; the command difference cannot be assigned wholly to the wire format.

The budgets below are qualification thresholds derived from task 01's actual p99 and byte sizes,
with room for the cross-revision debug harness and shared-host variance. They are regression
alarms, not throughput guarantees. All three current runs pass them.

| Operation | p99 budget | Response-byte budget | Derivation |
| --- | ---: | ---: | --- |
| Command | 6.5 ms | 1,900 B | 1.5× historical p99; 1.25× historical response. |
| Typed Row subscription | 90 ms | 1,500 B | 1.5× historical p99; 1.31× historical response. |
| Graph snapshot | 3.6 ms | 4,600 B | 1.5× historical p99; 1.26× historical response. |
| Upload | 75 ms | 200 B | 1.59× historical p99; small typed ACK has an absolute byte cap. |

For the historical four-operation sequence, also flag release-server CPU above 54 user or 27
system ticks (1.5× task 01) and jemalloc resident above 92 MB (1.5× task 01). Track peak RSS and
paused/slow control latency as diagnostic series until a comparable historical load exists for
those additions. No new public wire form or serialization fallback was introduced.

## Validation and changed-line coverage

| Check | Result |
| --- | --- |
| `just test-package-lib nervix-client-wire` | 117 unit tests passed, including multirow branch, sort, escape, null and redaction display. |
| `just test-client-wire-bench-fixture` | 2 typed Arrow fixture tests passed; task 01 alternating input allocated 1,707 times, requested 435,776 bytes and emitted 131,192 wire bytes. |
| `just client-wire-baseline` | Three 100/5/1,024 release-server runs passed; final run includes command preparation and paused/slow control. |
| `just client-wire-tls-cost` | 2 HTTP/HTTPS Cucumber scenarios passed. |
| `just client-wire-binding-cost` and `just client-wire-binding-host-cost` | FFI probe passed; Python one- and three-node Cucumber conformance passed, 2 scenarios and 14 steps. |
| `just test-scenarios --tags @client_wire15 --concurrency 1 --retry 0` | 3 scenarios and 71 steps passed after the presentation change. |
| `just validate` and `just ratchet` | Passed; architecture debt did not increase. |

Six `just coverage-lib`, `just coverage-scenarios` and `just coverage-client-wire-cost` LCOV
reports together covered **833/847 executable changed Rust lines (98.35%)**. The union includes the
standalone benchmark, server fixture, FFI test helper, display path and both opt-in Cucumber
helpers. Comments and declarations without an LCOV execution counter are excluded. Python host
code was checked by the live conformance scenarios, not Rust LCOV. The raw result archives above
are data, not source coverage targets.
