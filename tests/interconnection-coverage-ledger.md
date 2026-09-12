# Interconnection coverage and qualification ledger

This ledger is the acceptance record for
[Interconnect 11](https://app.clickup.com/t/86bbw87b7) under the
[bounded HTTP/2 interconnection epic](https://app.clickup.com/t/86bapbmj6). It lists every audit
finding the proposal covers, the step that owns it, and the scenario or test that proves it on the
current source. A finding is closed here only when its owning step is complete and the evidence
named beside it passes through the repository's standard commands.

Run the named Cucumber evidence with `just test-scenarios --input <feature>`, and the whole suite
with `just test`.

## Transport shape

Nervix nodes speak one protocol to each other: authenticated HTTP/2 over mutual TLS, in five
directed pools separated by traffic class. There is no second transport, no protocol negotiation,
and no mixed-protocol deployment. Both sides of a connection present the same wire contract
fingerprint during the connection hello, and a peer that presents a different one is refused rather
than downgraded. Replacing the fingerprint replaces the contract outright: a cluster is upgraded by
a coordinated stop and start, with operator-provisioned credentials and explicit recreation of
affected node-local state.

Nothing in the node holds a global queue that every relay batch or every control request passes
through. Incoming relay payloads are handed to a lane that preserves arrival order inside one
authenticated logical channel and runs different channels concurrently; typed requests are
dispatched by the transport under its own bounded per-subquota admission. Management traffic —
health, cancellation, terminal outcomes — keeps reserved stream and memory capacity that saturated
relay or bulk work cannot consume.

## Finding coverage

| Audit finding or required invariant | Owning steps | Evidence |
| --- | --- | --- |
| Partial-frame cancellation and restart | 1, 2, 4 | `cluster/interconnect_lifetime.feature`: *Peer churn and silent handshakes leave the node responsive* |
| Global relay FIFO and cross-domain/branch blocking | 4, 5 | `runtime/interconnect_admission.feature`: *A waiting relay admission leaves another domain runnable*, *A waiting relay admission leaves another branch runnable* |
| Control handler blocks ACKs, gate release, or payload routing | 3, 4, 5 | `runtime/interconnect_admission.feature`: *Evicting a branch cancels its waiting remote admission*; `cluster/interconnect_health.feature`: *A silent peer does not delay peer health or control-plane work* |
| Sequential fanout and repeated encoding/copies | 3, 5 | `cluster/bounded_execution.feature`: *Occupied bulk execution leaves management work responsive* |
| Full-archive synchronous serving and memory amplification | 3, 6 | `cluster/resource_describe.feature`: *Large resource replication preserves control responsiveness*; `cluster/interconnect_observability.feature`: *A bulk transfer reports its progress without spending management capacity* |
| Sequential resource reconciliation and ambiguous readiness timeout | 6 | `cluster/resource_describe.feature`: *Readiness deadline returns the published version while a replica is pending*, *Upload retry reports one published version* |
| Async-worker stalls in Arrow, CBOR, rkyv, filesystem and snapshots | 3, 4, 6, 7, 8 | `cluster/bounded_execution.feature`; `cluster/interconnect_observability.feature`: *Occupied bulk execution leaves the reserved management budget intact* |
| Unbounded payload queues and item-only transport budgets | 3, 4, 5 | `cluster/interconnect_observability.feature`: *Interconnection series are exposed with bounded dimensions* — `nervix_execution_memory_capacity_bytes` and `nervix_execution_memory_reserved_bytes` per class |
| Queued admission timeout before progress starts | 5 | `runtime/interconnect_admission.feature`: *A waiting relay admission leaves another domain runnable* |
| Queue/backpressure blocks ping, writes, or shutdown | 2, 4, 10 | `cluster/interconnect_lifetime.feature`; `cluster/interconnect_health.feature` |
| Runtime snapshots exceed frame cap; sender/receiver limit mismatch | 3, 4, 7 | `runtime/materialized_stream.feature`: *Concurrent branch updates recover from one consistent columnar snapshot* |
| Untrusted counts/depth/decoded sizes exceed allocation limits | 3, 4 | `nervix-execution` limit validation tests; `nervix-interconnect` wire decode tests |
| Full snapshot buffers, ignored cancellation, partial-install risk | 7, 9 | `cluster/raft_replication.feature`: *A lagging follower recovers after bounded log compaction* |
| Materialized snapshot row reconstruction and inconsistent scans | 7 | `runtime/materialized_stream.feature`: *Concurrent branch updates recover from one consistent columnar snapshot* |
| Whole-state consensus serialization and lock-held persistence | 3, 8 | `cluster/consensus_storage.feature`: *Committed administrative changes recover atomically after storage failure* |
| Unpipelined Raft, disabled automatic snapshots, full-log scans | 8, 9 | `cluster/raft_replication.feature`: *Pipelined replication preserves committed order*, *A lagging follower recovers after bounded log compaction* |
| Durability before Raft persistence acknowledgement | 8, 9 | `cluster/consensus_storage.feature` |
| Unbounded health checks and reconciliation coupling | 10 | `cluster/interconnect_health.feature`: *A silent peer does not delay peer health or control-plane work* |
| Outbound-only connection cap, churn, unbounded handshake and task lifetime | 2, 4 | `cluster/interconnect_lifetime.feature`: *Peer churn and silent handshakes leave the node responsive* |
| Weak cluster API TLS and static introduction/key trust | 4 | `cluster/internal_tls.feature`: *Interconnect peers connect with certificate identities*, *Invalid interconnect peer credentials are rejected*, *Interconnect certificate authority rotates without restarting the cluster* |
| Intentional session/branch/Raft ordering and exact schema preservation | 3, 5, 6, 8, 9 | `runtime/interconnect_admission.feature` (interleaved branches); `cluster/raft_replication.feature` |
| In-memory-only relay attempts, ACKs, and suspended work | 3, 5, 7 | `runtime/interconnect_admission.feature`: *Evicting a branch cancels its waiting remote admission* |

## Observability

Every node exposes its own interconnection on `/metrics`. The series below are read from the state
that owns them each time the endpoint is scraped, so a level cannot disagree with the pools it
describes, and they are aggregated only by dimensions whose value sets are fixed at compile time:
traffic class, connection direction, reserved operation subquota, failure reason, and outcome. No
series carries a branch key, a peer identity, a domain, or an operation identifier, and no series
carries a payload value.

| Concern the proposal names | Series |
| --- | --- |
| Active connections and streams | `nervix_interconnect_connections`, `nervix_interconnect_streams` |
| Pending operations | `nervix_interconnect_pending_operations`, `nervix_interconnect_relay_channels`, `nervix_interconnect_relay_attempts`, `nervix_interconnect_relay_grants` |
| Queued and reserved bytes | `nervix_execution_memory_capacity_bytes`, `nervix_execution_memory_reserved_bytes` |
| Serialization queue and work duration | `nervix_execution_job_queue_seconds_total`, `nervix_execution_job_work_seconds_total`, with `nervix_execution_jobs_running` and `nervix_execution_jobs_pending` |
| Task poll delay | `nervix_node_scheduler_delay_seconds_total`, `nervix_node_scheduler_delay_peak_seconds`, `nervix_node_scheduler_samples_total` |
| Admission latency | `nervix_interconnect_relay_admission_wait_seconds_total` over `nervix_interconnect_relay_admissions_total` |
| ACK age | `nervix_interconnect_unresolved_outcome_age_seconds` |
| Health RTT | `nervix_interconnect_request_seconds_total{operation="liveness"}` over `nervix_interconnect_requests_total{operation="liveness"}` |
| Reconnect and reset reasons | `nervix_interconnect_connections_established_total`, `nervix_interconnect_connection_failures_total`, `nervix_interconnect_stream_resets_total` |
| Bulk progress | `nervix_interconnect_bulk_bytes_total` |
| Snapshot pins | `nervix_consensus_snapshot_pinned_generations`, `nervix_consensus_snapshot_pinned_readers`, `nervix_consensus_snapshot_unreferenced_generations` |
| Log retention | `nervix_consensus_log_last_index`, `nervix_consensus_log_snapshot_index`, `nervix_consensus_log_purged_index`, `nervix_consensus_log_retained_bytes` |
| Quota failures | `nervix_interconnect_quota_failures_total`, `nervix_execution_memory_rejections_total`, `nervix_execution_job_rejections_total` |

Evidence: `cluster/interconnect_observability.feature`. Its bounded-dimension step reads the whole
exposition and fails on any interconnection sample carrying a label outside that closed set;
*Cross-node relay delivery advances transport and admission series* proves the transport and
admission series move under real cross-node traffic; and *A bulk transfer reports its progress
without spending management capacity* proves a 48 MiB archive replication is visible as it runs
while the management pool keeps its connections, refuses no liveness request, and records no
capacity failure.

## User-visible resource status

`DESCRIBE RESOURCE <name> VERSION <n>` distinguishes the three states the proposal requires:
publication (`created_at`, `created_by_node`, and the assigned version), replication progress and
failure (a per-node `state=` and `error=` line, with `source=` and `verified_at=`), and cluster
readiness (`cluster_ready`). A readiness wait that expires returns the published version rather
than an upload failure.

Evidence: `cluster/resource_describe.feature`: *Uploaded resource is describable after replication*,
*Readiness deadline returns the published version while a replica is pending*, *Uploaded resources
converge after a node rejoins the cluster*.

## Open qualification

Two acceptance items in Step 11 are not closed by this ledger. They are recorded here rather than
omitted.

**Shaped-link performance qualification.** The proposal's comparative measurement — a fixed
three-node environment with shaped links, bulk offered load capped at 70% of link capacity, control
p99 no worse than the greater of 100 ms or twice the idle baseline, no false management
disconnects, bounded configured allocations, and progress on every eligible unsaturated relay
channel — needs a three-node benchmark environment with link shaping. The repository's benchmark
harness drives a single node through Kafka and has no shaping, so those numbers cannot be produced
from it yet.

Two of the four criteria are already proven here without timing, by deterministic barriers rather
than by measurement. *No false management disconnects*: during a 48 MiB bulk replication, node-1
keeps its management connections, refuses no liveness request, and records no management capacity
failure. *Bounded configured allocations*: each class reports its ceiling and what it holds, and
the reserved management budget stays at its configured capacity while every bulk worker is
occupied. Control responsiveness under bulk load is bounded rather than measured: *Occupied bulk
execution leaves management work responsive*, *Occupied bulk execution leaves the reserved
management budget intact*, *Large resource replication preserves control responsiveness*, and *A
silent peer does not delay peer health or control-plane work* each complete control work inside an
explicit bound while the competing class is saturated.

What the shaped environment adds is the distribution rather than the bound: a control p99 against
a measured idle baseline, per-relay-channel progress under a sustained offered load, and the
transport throughput comparison at identical record, admission and durability settings. The series
above are what such a run would read.

**Domain-clock evidence.** [Domain clocks 12](https://app.clickup.com/t/86bbwct04) owns committed
clock installation on join and restart, one fenced authority across owner and leader changes,
coalesced progress that cannot regress logical time, and correct logical execution under bulk
saturation. Its results belong in this ledger once that task completes.
