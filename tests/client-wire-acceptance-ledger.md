# Client wire acceptance map

This map records current session guarantees, their owners and the deterministic controls that
exercise them. Historical run results and audit findings are retained on [Client Wire 01](https://app.clickup.com/t/86bc1ahkw).
Integrated recovery evidence is mapped in [Client Wire qualification](client-wire-15-qualification.md).
Comparative measurements and raw reports belong to [Client Wire 16](https://app.clickup.com/t/86bc1ahz3).

## Contract ownership

| Contract | Owning delivery | Executable regression |
| --- | --- | --- |
| A command reports only its own committed outcome | [03](https://app.clickup.com/t/86bc1ahmh) | `A command missing from a committed transaction cannot report aggregate success` (`@client_wire_missing_command`, one and three nodes) |
| An admitted append has one durable position | [03](https://app.clickup.com/t/86bc1ahmh) | `Replaying an admitted append does not preflight or append it again` (`@client_wire_duplicate_append`, one and three nodes) |
| A retried BEGIN recovers its original transaction | [03](https://app.clickup.com/t/86bc1ahmh) | `Replaying a BEGIN whose response was lost returns the original transaction` (`@client_wire_lost_begin`, one and three nodes) |
| A stalled commit leaves unrelated domains and expiry runnable | [06](https://app.clickup.com/t/86bc1ahnh) | `A stalled commit cannot block expiry, another domain, or tombstone cleanup` (`@client_wire_stalled_commit`) |
| A relocation is fenced to the schedule it planned from | [05](https://app.clickup.com/t/86bc1ahn6) | `A relocation planned before a schedule revision cannot overwrite that revision` (`@client_wire_stale_relocation`) |
| Replies correlate with the request identity they name | [11](https://app.clickup.com/t/86bc1ahw3); resolved by request-identity correlation in [09](https://app.clickup.com/t/86bc1ahv3) | `nervix_client_core::tests::response_reordering_cannot_take_another_requests_waiter` |
| Undrained events cannot block command replies | [11](https://app.clickup.com/t/86bc1ahw3), with server lifecycle separation in [10](https://app.clickup.com/t/86bapbpt3) | `nervix_client_core::tests::saturated_event_consumer_cannot_block_a_command_reply` |
| Every subscription releases its exact interest lease | [10](https://app.clickup.com/t/86bapbpt3) | `application::subscription::tests::deleting_two_same_relay_subscriptions_withdraws_the_interest_exactly`; `A node's interest in a relay follows every subscription of every session exactly` in `session_subscription_lifecycle.feature`, one and three nodes |
| Acknowledged subscriptions restore across reconnect | [12](https://app.clickup.com/t/86bc1ahwj) | `A reconnected native client restores acknowledged subscriptions` (`@client_wire_subscription_restore`, three nodes and concrete `acme`/`beta` branches) |
| Unsolicited console events cannot consume a request waiter | [13](https://app.clickup.com/t/86bc1ahym); resolved by request-identity correlation in [09](https://app.clickup.com/t/86bc1ahv3) | `tests::untracked_domain_push_cannot_discard_a_pending_websocket_request` in the `nervix-web-console` binary |
| A committed batch keeps its one outcome across leader movement | [Return a committed transaction batch's recorded outcome](https://app.clickup.com/t/86bckvkt6) | `A committed transaction batch keeps its one outcome when leadership moves while each attempt records it` (`@client_wire_transaction_batch_finalization_recovery`, three nodes); `transaction::tests::a_finished_transaction_answers_each_accepted_append_with_its_recorded_admission` in `nervix-consensus` |

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
| Before admission | Arm a node's command admission pause before proposal, wait until the request reaches it, then transfer leadership, commit from another session, cancel, or release. | a command reports only its own committed outcome and existing leader-loss scheduling coverage |
| After durable admission | Arm the persistent-command pause after its applying record commits but before effects begin. The passing control drops the public stream and retries with the same reference. | `A command lost after durable admission is recovered by its request identity` (`@client_wire_durable_admission`) |
| After execution, before response | Arm response delivery after a `CommandResult` exists, wait for the barrier, drop the gRPC stream, then release. The same seam is above both public session transports. | an admitted append has one durable position and a retried begin recovers its original transaction |
| After effects, before outcome recording | Arm a node's finalization pause, which holds the next persistent command whose effects returned a terminal result before the node proposes that outcome. It holds a request's own attempt and a leader's resumption of an applying execution alike, so a leadership transfer while it is held leaves the execution applying with its effects in place. | a committed batch keeps its one outcome across leader movement |
| Transaction progress | Pause a named node/domain commit after an exact number of applied statements. | a stalled commit leaves unrelated domains and expiry runnable |
| Planned schedule publication | Pause `RELOCATE` after it produces its target schedule and before it reads the publication base. | a relocation is fenced to the schedule it planned from |
| Leader movement | Select the old and new node explicitly, request transfer, and wait until the destination reports itself leader. | acknowledged subscriptions restore across reconnect and existing admission tests |
| Full process crash/restart | Signal the actual child PID, assert termination by `SIGKILL`, restart the same executable with the same database, TLS identity, credentials and ports, then query through gRPC. | `A SIGKILL restart preserves a command admitted before the crash` in `client_wire_process_restart.feature` |
| Full cluster restart | Stop and rebuild every node against its existing durable state. | Existing `Persisted rules are reapplied after a full cluster restart` matrix in `persistence.feature`, covering one node, three nodes, and a replicated three-node graph |
| Undrained consumers | Fill the bounded event sender and hold its receiver while routing a command result under a short monotonic deadline. | undrained events cannot block command replies |
| Subscription create/delete/rebuild | Announce two same-relay subscriptions, delete both in order, and read the node's lease count and `nervix_session_subscriptions`. `relay_rebuilds_end_subscribers_only_when_the_relay_rows_change` rebuilds a relay under a live subscriber with the same, a sensitive, and no definition. | Exact interest release and acknowledged subscription restoration |

Scenario teardown releases every armed command pause. Fault controls are compiled only with the
`testing` feature and do not create a product protocol or compatibility surface.

## Public coverage matrix

| Surface | Topology and workload |
| --- | --- |
| Transaction identity and false success | Native gRPC, one and three nodes; fixed execution references; separate owner/finisher sessions where relevant |
| Relocation publication | Native gRPC, three nodes; two replicas; a Kafka domain-owned partition schedule changes from `0` to `0,1` between plan and publication |
| Subscription restoration | Native Rust client over gRPC, three nodes; leader transfer; interleaved `acme` and `beta` concrete branch records |
| Console dispatch | Console WebSocket dispatcher, direct desired-behavior probe |
| Crash/restart | One real `nervix-server` process; `SIGKILL`; same durable store and listening addresses |


## Performance baseline

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
* strict typed JSON HTTP input and native gRPC Row subscription delivery, alternating concrete
  branch keys `acme` and `beta` and validating each row's `key=<json> payload=<json>` display text;
* domain-selection graph snapshots over an authenticated console WebSocket;
* deterministic one-file resource archives over the native gRPC client-streaming upload;
* one MiB ingestor quiesce capacity and the current client/server/WebSocket queue capacities.

`client-wire-baseline.json` records every raw observation plus p50/p90/p99/min/max/total
latency, the exact frame or public request and response bytes, client and server user/system CPU
ticks, per-workload process counter snapshots, client jemalloc and server-exported jemalloc phase
snapshots, `/proc` resident and peak resident bytes, per-workload relay queue occupancy series,
workload parameters, queue limits, git revision and worktree state, release/debug profile, complete
`rustc -Vv`, kernel, architecture, CPU model/count, physical memory, and clock-tick rate. The raw
final Prometheus scrape is retained as `metrics.prom` beside it. Raw frame and WebSocket access
exist only in this benchmark harness.
