# Client Wire integrated qualification map

This map relates the session and durable lifecycle contracts to their executable evidence.
Historical run results and audit findings are retained on [Client Wire 15](https://app.clickup.com/t/86bc1ahyw).

The integrated scenarios use native gRPC and the WebSocket console session. Browser cases use
Playwright. The full-restart case starts three actual `nervix-server` processes with independent
databases and one cluster identity, sends `SIGKILL` to every process, and restarts all three from
those same databases and ports. In-process cluster scenarios provide deterministic fault barriers
for admission, applying, delivery, ownership, and resource installation.

## Acceptance evidence

| Contract | Executable evidence |
| --- | --- |
| Follower, leader loss, exact command outcomes, and cancellation | `client_wire_failures.feature`: lost durable admission, missing committed command, exact `BEGIN` and append retries, batch retry across leader change and restart, bounded execution history; `session_protocol.feature`: cancellation before and after admission, typed gRPC/WebSocket failures, empty discovery, and reconnect. |
| Transaction identity, takeover, expiry, and inspection | `plexus_transactions.feature`: exact transaction identity, competing sessions, queue and commit outcomes, replay, expiry, and typed inspection; `transaction_quiesce_qualification.feature`: a report over 2 MiB with 128 operations, leader transfer, commit, and exact report comparison after restart. |
| Domain-owner and plan-base fencing | `client_wire_failures.feature`: stalled commit does not block another domain or expiry, and a relocation planned before a schedule revision cannot overwrite the newer revision. The `@shutdown_qualification` relocation cases cover prepared ownership handoff, destination restart, cancellation, and leader reconciliation. `cordon_node.feature` checks scheduling eligibility. |
| Shutdown eligibility | The terminating-node selection in `graceful_shutdown.feature` checks that placement excludes a terminating incarnation, admitted work remains eligible, and restart restores schedulability. |
| All-voter crash and recovery | `Every killed process recovers retained commands and expires an overdue open transaction`: all three children receive `SIGKILL`; the cluster remains down past its 10-second transaction idle limit; all persisted voters restart, converge, and report `EXPIRED`; an uncommitted schema remains absent; retrying the pre-crash command identity returns its retained success without a duplicate schema effect. |
| Durable traffic and snapshot chain | `process_crash_recovery.feature` checks SIGKILL against interleaved branch traffic. `raft_replication.feature` checks pipelined order, log compaction, interrupted snapshot installation, and bounded durable catch-up under concurrent writes. The catch-up sampler permits a reservation equal to the configured Commands capacity, as the budget itself does. |
| Subscriptions, branches, and inspection under one leader loss | `Subscription restoration and typed transaction inspection survive the same leader loss`: an acknowledged Row subscription receives `acme`, the leader changes and the old leader stops, the same subscription restores and receives `beta`, and a typed transaction report remains correct before and after the change and after commit. `session_subscription_lifecycle.feature` checks relay rebuild/removal, START/STOP, interest balance, and undrained consumers. The client-core unit suite checks early events, repeated restoration, late generations, failed deletion, and saturation. |
| Real browser session and inspection | `connection_status.feature` covers follower entry, reconnect, outage, authentication failure, and process termination. `transaction_inspector.feature` checks typed and retained reports, preview bases, applying prefixes, and reconnection. The `@client_wire13` REPL cases cover command responsiveness, failed subscription creation, restored tabs, busy tabs, and cancellation after interruption. |
| Upload completion across uncertainty | `An uncertain upload completes once across installation and leader change`: a held install leaves the response uncertain, leadership transfers, and the same upload identity returns exactly version 1, installed and describable on all nodes. |
