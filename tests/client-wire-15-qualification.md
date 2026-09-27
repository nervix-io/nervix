# Client Wire 15 integrated qualification

This is the validation record for [Client Wire 15](https://app.clickup.com/t/86bc1ahyw),
against the FlatBuffers transport and durable lifecycle on task branch
`CU-86bc1ahyw_Client-Wire-15-Qualify-failover-full-restart-subscriptions-and-inspection-together_Gleb-Pomykalov`.
The branch was updated from `origin/main` at `37376b0d8125bfbd648d0f9bbe7967f7dd5360c5`
before implementation. The PR records the resulting commit and CI revision.

The integrated scenarios use native gRPC and the WebSocket console session. Browser cases use
Playwright. The new full-restart case starts three actual `nervix-server` processes with independent
databases and one cluster identity, sends `SIGKILL` to every process, and restarts all three from
those same databases and ports. In-process cluster scenarios provide deterministic fault barriers
for admission, applying, delivery, ownership, and resource installation.

## Acceptance evidence

| Contract | Executable evidence |
| --- | --- |
| Follower, leader loss, exact command outcomes, and cancellation | `client_wire_failures.feature`: lost durable admission, missing committed command, exact `BEGIN` and append retries, batch retry across leader change and restart, bounded execution history; `session_protocol.feature`: cancellation before and after admission, typed gRPC/WebSocket failures, empty discovery, and reconnect. |
| Transaction identity, takeover, expiry, and inspection | `nspl_transactions.feature`: 68 cases of exact transaction identity, competing sessions, queue and commit outcomes, replay, expiry, and typed inspection; `transaction_quiesce_qualification.feature`: a report over 2 MiB with 128 operations, leader transfer, commit, and exact report comparison after restart. |
| Domain-owner and plan-base fencing | `client_wire_failures.feature`: stalled commit does not block another domain or expiry, and a relocation planned before a schedule revision cannot overwrite the newer revision. The `@shutdown_qualification` relocation cases cover prepared ownership handoff, destination restart, cancellation, and leader reconciliation. `cordon_node.feature` checks scheduling eligibility. |
| Shutdown eligibility | The terminating-node selection in `graceful_shutdown.feature` checks that placement excludes a terminating incarnation, admitted work remains eligible, and restart restores schedulability. |
| All-voter crash and recovery | `Every killed process recovers retained commands and expires an overdue open transaction`: all three children receive `SIGKILL`; the cluster remains down past its 10-second transaction idle limit; all persisted voters restart, converge, and report `EXPIRED`; an uncommitted schema remains absent; retrying the pre-crash command identity returns its retained success without a duplicate schema effect. |
| Durable traffic and snapshot chain | `process_crash_recovery.feature` checks SIGKILL against interleaved branch traffic. `raft_replication.feature` checks pipelined order, log compaction, interrupted snapshot installation, and bounded durable catch-up under concurrent writes. The catch-up sampler permits a reservation equal to the configured Commands capacity, as the budget itself does. |
| Subscriptions, branches, and inspection under one leader loss | `Subscription restoration and typed transaction inspection survive the same leader loss`: an acknowledged Row subscription receives `acme`, the leader changes and the old leader stops, the same subscription restores and receives `beta`, and a typed transaction report remains correct before and after the change and after commit. `session_subscription_lifecycle.feature` checks relay rebuild/removal, START/STOP, interest balance, and undrained consumers. The client-core unit suite checks early events, repeated restoration, late generations, failed deletion, and saturation. |
| Real browser session and inspection | `connection_status.feature` covers follower entry, reconnect, outage, authentication failure, and process termination. `transaction_inspector.feature` checks typed and retained reports, preview bases, applying prefixes, and reconnection. The `@client_wire13` REPL cases cover command responsiveness, failed subscription creation, restored tabs, busy tabs, and cancellation after interruption. |
| Upload completion across uncertainty | `An uncertain upload completes once across installation and leader change`: a held install leaves the response uncertain, leadership transfers, and the same upload identity returns exactly version 1, installed and describable on all nodes. |

## Local runs

All commands run from the repository root with the task patch applied. The focused tag selects the
three added or extended scenarios. Counts are Cucumber's reported counts, including outline cases.

| Command | Result |
| --- | --- |
| `just test-scenarios --tags @client_wire15 --concurrency 1 --retry 0` | Pass: 3 scenarios, 71 steps. |
| `just test-scenarios --input tests/features/runtime/client_wire_failures.feature --concurrency 1` | Pass: 27 scenarios, 371 steps. |
| `just test-scenarios --input tests/features/runtime/nspl_transactions.feature --concurrency 2 --retry 0` | Pass: 68 scenarios, 870 steps. |
| `just test-scenarios --input tests/features/runtime/session_protocol.feature --concurrency 1 --retry 0` | Pass: 14 scenarios, 177 steps. |
| `just test-scenarios --input tests/features/runtime/session_subscription_lifecycle.feature --concurrency 1 --retry 0` | Pass: 10 scenarios, 160 steps. |
| `just test-scenarios --input tests/features/runtime/transaction_quiesce_qualification.feature --concurrency 1 --retry 0` | Pass: 1 scenario, 34 steps. |
| `just test-scenarios --input tests/features/web-console/connection_status.feature --concurrency 1 --retry 0` | Pass: 9 scenarios, 56 steps. |
| `just test-scenarios --input tests/features/web-console/transaction_inspector.feature --concurrency 1 --retry 0` | Pass: 12 scenarios, 340 steps. |
| `just test-scenarios --input tests/features/web-console/nspl_repl.feature --tags @client_wire13 --concurrency 1 --retry 0` | Pass: 8 scenarios, 117 steps. |
| `just test-scenarios --input tests/features/tools/cli_session.feature --concurrency 1 --retry 0` | Pass: 10 scenarios, 57 steps, including 270 sequential subscription rows. |
| `just test-client-conformance '@client_conformance_toolchain' --concurrency 1 --retry 0` | Pass: 19 scenarios, 118 steps across the C ABI, C, C++, Python, Java, Ruby, Go, Node.js, and Bun probes. |
| `just test-scenarios --input tests/features/cluster/resource_describe.feature --tags @resource_upload_idempotency --concurrency 1 --retry 0` | Pass: 3 scenarios, 51 steps. |
| `just test-scenarios --input tests/features/runtime/client_wire_process_restart.feature --concurrency 1 --retry 0` | Pass: 3 real-process scenarios, 29 steps. |
| `just test-scenarios --input tests/features/runtime/client_pool_bounds.feature --concurrency 1 --retry 0` | Pass: 3 scenarios, 41 steps, including one- and three-node persisted client pool bounds. |
| `just test-scenarios --input tests/features/cluster/subscription_interest.feature --concurrency 1 --retry 0` | Pass: 1 scenario, 18 steps. |
| `just test-scenarios --input tests/features/cluster/cluster_scheduling.feature --name Followers --concurrency 1 --retry 0` | Pass: 1 scenario, 4 steps. |
| `just test-scenarios --input tests/features/cluster/cluster_scheduling.feature --name Leadership --concurrency 1 --retry 0` | Pass: 3 scenarios, 42 steps. |
| `just test-scenarios --input tests/features/cluster/cluster_scheduling.feature --name Attached --concurrency 1 --retry 0` | Pass: 1 scenario, 13 steps; the attached ACK waits across a faulted Kafka emitter. |
| `just test-scenarios --input tests/features/cluster/leader_failover.feature --name Cluster --concurrency 1 --retry 0` | Pass: 1 scenario, 10 steps; the cluster elects a successor after direct leader death. |
| `just test-scenarios --input tests/features/cluster/process_crash_recovery.feature --concurrency 1 --retry 0` | Pass: 2 scenarios, 57 steps; SIGKILL under interleaved branch traffic preserves the durable checkpoint boundary. |
| `just test-scenarios --input tests/features/cluster/raft_replication.feature --concurrency 1 --retry 0` | Three scenarios and 37 steps passed. The fourth reached exactly its 24 MiB Commands capacity and exposed a too-strict test assertion. |
| `just test-scenarios --input tests/features/cluster/raft_replication.feature --name Durable --concurrency 1 --retry 0` | Pass after changing the sampler assertion to `peak <= capacity`: 1 scenario, 8 steps. No class reservation exceeded capacity. |
| `just test-scenarios --input tests/features/runtime/kafka_ingestion.feature --name detached --concurrency 1 --retry 0` | Pass: 6 scenarios, 57 steps; detached emitter and deduplicator branch failures do not hold the upstream Kafka ACK. |
| `just test-scenarios --input tests/features/cluster/graceful_shutdown.feature --name terminating --concurrency 1 --retry 0` | Pass: 3 scenarios, 55 steps. |
| `just test-scenarios --input tests/features/cluster/cordon_node.feature --concurrency 1 --retry 0` | Pass: 2 scenarios, 11 steps. |
| `just test-scenarios --input tests/features/cluster/resource_describe.feature --tags @resource_streaming --concurrency 1 --retry 0` | Pass: 1 scenario, 10 steps; large replication leaves control commands responsive. |
| `just test-scenarios --input tests/features/cluster/relocate.feature --tags @shutdown_qualification --concurrency 1 --retry 0` | Pass: 4 scenarios, 73 steps. |
| `just test-package-lib nervix-client-core` | Pass: 90 tests. |
| `just validate` | Pass. |
| `just coverage-scenarios target/client-wire15-final.lcov --tags @client_wire15 --concurrency 1 --retry 0` | Pass: 3 scenarios, 71 steps. LCOV against added or modified Rust lines in this patch: 313/346, 90.5%. |

The real-process test uses an intentional 11-second physical downtime to make the 10-second
transaction deadline overdue while every process is stopped. Other waits are bounded on observable
membership, leader, transaction, and subscription state. These checks qualify the current patch;
they do not measure performance or establish compatibility with previously stored formats.
