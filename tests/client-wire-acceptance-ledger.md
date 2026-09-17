# Client wire failure and protobuf baseline ledger

This is the executable acceptance ledger for [Client Wire
01](https://app.clickup.com/t/86bc1ahkw). The source audit began at
`2aa7ee71a2416ef4ef723d3a4355ec66f540fe2f` on 15 September 2026. It records the failures that
later Client Wire tasks own, the deterministic seams that reproduce them, and the end-to-end
protobuf baseline that task 16 will compare with FlatBuffers.

The public scenarios use native gRPC sessions and NSPL commands. The console probe and graph
baseline use the console WebSocket session. Process recovery starts the actual `nervix-server`
binary, sends `SIGKILL`, and reopens the same database and ports. Focused unit probes are used only
where a public client cannot observe the dispatcher's waiter identity or the server's internal
subscription-interest balance.

Expected failures carry `@client_wire_expected_failure` or Rust's `#[ignore]`, name their owning
delivery task, and are excluded from the ordinary suite. A later task removes the quarantine only
after the owning behavior passes. Task 15 composes the final public qualification; this ledger does
not make that claim.

## Findings and ownership

| Finding | Audited failure | Owning delivery task | Executable regression |
| --- | --- | --- | --- |
| CW-F01 | A pending append that another session omitted before committing can be reported as successful from aggregate `Committed` state. | [03](https://app.clickup.com/t/86bc1ahmh) | `A command missing from a committed transaction cannot report aggregate success` (`@client_wire_missing_command`, one and three nodes) |
| CW-F02 | Replaying an admitted transaction append can preflight and append it a second time because the request has no durable position identity. | [03](https://app.clickup.com/t/86bc1ahmh) | `Replaying an admitted append does not preflight or append it again` (`@client_wire_duplicate_append`, one and three nodes) |
| CW-F03 | Retrying `BEGIN` after its response is lost creates a second transaction instead of recovering the original outcome. | [03](https://app.clickup.com/t/86bc1ahmh) | `Replaying a BEGIN whose response was lost returns the original transaction` (`@client_wire_lost_begin`, one and three nodes) |
| CW-F04 | The global transaction reconciler awaits one applying commit inline, preventing an unrelated orphan from reaching expiry and blocking cleanup/recovery behind it. | [06](https://app.clickup.com/t/86bc1ahnh) | `A stalled commit cannot block expiry of another orphaned transaction` (`@client_wire_stalled_commit`) |
| CW-F05 | `RELOCATE` plans from schedule S1, rereads S2 as its expected compare-and-swap value, and can publish the candidate derived from S1 over S2. | [05](https://app.clickup.com/t/86bc1ahn6) | `A relocation planned before a schedule revision cannot overwrite that revision` (`@client_wire_stale_relocation`) |
| CW-F06 | Native client replies consume a FIFO waiter. Concurrent registration and send order can differ, so one response can complete another request. | [11](https://app.clickup.com/t/86bc1ahw3) | `nervix_client_core::tests::response_reordering_cannot_take_another_requests_waiter` |
| CW-F07 | The native session response reader awaits bounded event queues inline. An unread subscription/server event can prevent an unrelated command reply from being routed. | [11](https://app.clickup.com/t/86bc1ahw3), with server lifecycle separation in [10](https://app.clickup.com/t/86bapbpt3) | `nervix_client_core::tests::saturated_event_consumer_cannot_block_a_command_reply` |
| CW-F08 | Every same-relay subscription increments interest, but deletion unregisters only when the final local subscription disappears, leaving the count positive. | [10](https://app.clickup.com/t/86bapbpt3) | `application::subscription::tests::deleting_two_same_relay_subscriptions_clears_interest` |
| CW-F09 | The native client does not own acknowledged desired subscriptions as generation-fenced state, so reconnect/leader movement loses the subscription. | [12](https://app.clickup.com/t/86bc1ahwj) | `A reconnected native client restores acknowledged subscriptions` (`@client_wire_subscription_restore`, three nodes and concrete `acme`/`beta` branches) |
| CW-F10 | The console dispatcher lets an unsolicited domain-list response take the first pending user request. | [13](https://app.clickup.com/t/86bc1ahym) | `tests::untracked_domain_push_cannot_discard_a_pending_websocket_request` in the `nervix-web-console` binary |

The schema and verified codec belong to [02](https://app.clickup.com/t/86bc1ahm4); persisted domain
mutation ownership to [04](https://app.clickup.com/t/86bc1ahmy); retained request-history bounds to
[07](https://app.clickup.com/t/86bc1ahuj); typed Arrow-to-Row encoding to
[08](https://app.clickup.com/t/86bc1ahvj); transport cutover to
[09](https://app.clickup.com/t/86bc1ahv3); cross-language qualification to
[14](https://app.clickup.com/t/86bc1ahyr); integrated qualification to
[15](https://app.clickup.com/t/86bc1ahyw); comparative measurement and tuning to
[16](https://app.clickup.com/t/86bc1ahz3); and public architecture/client documentation to
[17](https://app.clickup.com/t/86bc1ahz4).

## Deterministic fault controls

| Boundary | Control and observable barrier | Evidence |
| --- | --- | --- |
| Before admission | Arm a node's command admission pause before proposal, wait until the request reaches it, then transfer leadership, commit from another session, cancel, or release. | CW-F01 and existing leader-loss scheduling coverage |
| After durable admission | Arm the persistent-command pause after its applying record commits but before effects begin. The passing control drops the public stream and retries with the same reference. | `A command lost after durable admission is recovered by its request identity` (`@client_wire_durable_admission`) |
| After execution, before response | Arm response delivery after a `CommandResult` exists, wait for the barrier, drop the gRPC stream, then release. The same seam is above both public session transports. | CW-F02 and CW-F03 |
| Transaction progress | Pause a named node/domain commit after an exact number of applied statements. | CW-F04 |
| Planned schedule publication | Pause `RELOCATE` after it produces its target schedule and before it reads the publication base. | CW-F05 |
| Leader movement | Select the old and new node explicitly, request transfer, and wait until the destination reports itself leader. | CW-F09 and existing admission tests |
| Full process crash/restart | Signal the actual child PID, assert termination by `SIGKILL`, restart the same executable with the same database, TLS identity, credentials and ports, then query through gRPC. | `A SIGKILL restart preserves a command admitted before the crash` in `client_wire_process_restart.feature` |
| Full cluster restart | Stop and rebuild every node against its existing durable state. | Existing `Persisted rules are reapplied after a full cluster restart` matrix in `persistence.feature`, covering one node, three nodes, and a replicated three-node graph |
| Undrained consumers | Fill the bounded event sender and hold its receiver while routing a command result under a short monotonic deadline. | CW-F07 |
| Subscription create/delete/rebuild | Build two independently owned same-relay tasks, remove both in order, and inspect the owning interest index. The existing `rebuilt_relay_drops_subscription_with_clear_session_error` control rebuilds the relay under a live task. | CW-F08; CW-F09 adds public reconnect/restoration |

Scenario teardown releases every armed command pause. Fault controls are compiled only with the
`testing` feature and do not create a product protocol or compatibility surface.

## Public coverage matrix

| Surface | Topology and workload |
| --- | --- |
| Transaction identity and false success | Native gRPC, one and three nodes; fixed execution references; separate owner/finisher sessions where relevant |
| Relocation publication | Native gRPC, three nodes; two replicas; a Kafka domain-owned partition schedule changes from `0` to `0,1` between plan and publication |
| Subscription restoration | Native Rust client over gRPC, three nodes; leader transfer; interleaved `acme` and `beta` concrete branch records |
| Console dispatch | Console WebSocket protobuf dispatcher, direct desired-behavior probe |
| Crash/restart | One real `nervix-server` process; `SIGKILL`; same durable store and listening addresses |
| Baseline | One release `nervix-server` process; native gRPC command/subscription/upload traffic, HTTP record admission, and console WebSocket graph snapshots |

The public expected-failure scenarios live in
`tests/features/runtime/client_wire_failures.feature`. The real-process control lives in
`tests/features/runtime/client_wire_process_restart.feature`. Explicit tag selection is required
to run a quarantined regression, for example:

```console
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_missing_command --concurrency 1
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_duplicate_append --concurrency 1
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_lost_begin --concurrency 1
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_stalled_commit --concurrency 1
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_stale_relocation --concurrency 1
just test-scenarios --input tests/features/runtime/client_wire_failures.feature --tags @client_wire_subscription_restore --concurrency 1
```

The ignored dispatch/accounting probes run independently:

```console
cargo test -p nervix-client-core --lib tests::response_reordering_cannot_take_another_requests_waiter -- --ignored --exact
cargo test -p nervix-client-core --lib tests::saturated_event_consumer_cannot_block_a_command_reply -- --ignored --exact
cargo test -p nervix-web-console --bin nervix-web-console tests::untracked_domain_push_cannot_discard_a_pending_websocket_request -- --ignored --exact
cargo test --features testing --lib application::subscription::tests::deleting_two_same_relay_subscriptions_clears_interest -- --ignored --exact
```

## Protobuf performance baseline

`just client-wire-baseline` builds the web console and a normal release server binary, launches
that binary from the debug-only scenario harness, configures a fixed two-branch graph, warms the
command path, and writes raw evidence beneath `target/client-wire-baseline/`:

```console
just client-wire-baseline
just client-wire-baseline 100 5 1024 target/client-wire-baseline
```

The positional settings are command/subscription/snapshot samples, upload samples, payload bytes,
and output directory. Their defaults are `100`, `5`, `1024`, and
`target/client-wire-baseline`. The runner rejects zero samples, more than 10,000 samples, and a
payload above 16 MiB.

The fixed workload contains:

* `DESCRIBE DOMAIN` over one native gRPC bidirectional session;
* strict typed JSON HTTP input and native gRPC subscription delivery, alternating concrete branch
  keys `acme` and `beta` and validating the current `key=<json> payload=<json>` construction;
* `SetActiveDomain` graph snapshots over an authenticated console WebSocket;
* deterministic one-file resource archives over the native gRPC client-streaming upload;
* one MiB ingestor quiesce capacity and the current client/server/WebSocket queue capacities.

`client-wire-protobuf-baseline.json` records every raw observation plus p50/p90/p99/min/max/total
latency, exact protobuf or public request and response bytes, client and server user/system CPU
ticks, per-workload process counter snapshots, client jemalloc and server-exported jemalloc phase
snapshots, `/proc` resident and peak resident bytes, per-workload relay queue occupancy series,
workload parameters, queue limits, git revision and worktree state, release/debug profile, complete
`rustc -Vv`, kernel, architecture, CPU model/count, physical memory, and clock-tick rate. The raw
final Prometheus scrape is retained as `metrics.prom` beside it. Raw protobuf encoding and
WebSocket access exist only in this benchmark harness.

Task 16 must compare identical semantic work and bounds with this artifact. A debug smoke run is a
harness check, not a performance result.

### Recorded protobuf run

The default workload completed on 16 September 2026 UTC at source revision
`2aa7ee71a2416ef4ef723d3a4355ec66f540fe2f`. The server used the release profile; the measurement
harness used the debug profile because the repository deliberately prohibits combining the
`testing` feature with a release-like profile. Future comparisons must use the same recipe so the
client-side profile remains constant.

| Operation | Samples | Latency p50 / p90 / p99 / max (µs) | Request p50 (bytes) | Response p50 (bytes) |
| --- | ---: | ---: | ---: | ---: |
| Native command | 100 | 782 / 1,541 / 4,355 / 12,559 | 93 | 1,508 |
| Typed JSON subscription delivery | 100 | 29,103 / 38,687 / 59,967 / 69,951 | 1,067 | 1,142 |
| Console graph snapshot | 100 | 119 / 309 / 2,387 / 260,479 | 37 | 3,647 |
| Resource upload | 5 | 41,471 / 47,263 / 47,263 / 47,263 | 2,663 | 73 |

The release server consumed 36 user and 18 system CPU ticks at 100 ticks/second. Its observed peak
RSS was 94,089,216 bytes, and its final jemalloc resident measurement was 60,960,768 bytes. The
final relay occupancy histogram contains 200 observations with a summed queue length of 100; every
observation was at most one item. The JSON report retains the complete raw samples, phase counters,
histogram series, environment, and configured limits rather than only these summaries.

## Task 01 validation record

Recorded on 15 September 2026. Expected-red means the regression reached its named assertion and
failed for the audited behavior; it does not mean the harness crashed or timed out before the
barrier.

| Probe | Result |
| --- | --- |
| Cucumber test target with `testing` enabled | Pass |
| Durable-admission loss/retry control | Pass; retry recovered the admitted command by execution reference |
| Missing-command false-success, one/three nodes | Expected red; the absent command reported success after another session committed |
| Duplicate admitted append replay, one/three nodes | Expected red; replay reached append preflight and rejected the reused reference |
| Lost `BEGIN` response replay, one/three nodes | Expected red; retry created a different transaction ID |
| Stalled commit versus orphan expiry | Expected red; the unrelated transaction remained `OPEN` beyond expiry |
| Stale relocation publication after schedule revision | Expected red; stale relocation succeeded and overwrote the newer schedule |
| Native subscription restoration after leader transfer and two branches | Expected red; the `beta` delivery timed out after reconnecting away from the old leader |
| Native response waiter miscorrelation | Expected red; the command result was consumed by the domain-list waiter |
| Native saturated-event command routing | Expected red; the command result stalled behind the full event queue |
| Server subscription-interest cleanup | Expected red; deleting the final subscription left relay interest behind |
| Console unsolicited-response dispatch | Expected red; the domain response discarded the pending attach request |
| Real-process `SIGKILL` restart | Pass |
| Debug baseline smoke (`2` operation samples, `1` upload, `128` payload bytes) | Pass; JSON and raw metrics artifacts written |
| Release-server protobuf baseline (default workload) | Pass; JSON report and raw Prometheus scrape written |

No complete Cucumber suite or Client Wire 15 qualification is claimed by this task.
