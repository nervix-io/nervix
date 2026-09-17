# Contentionless data-plane qualification ledger

This ledger is the acceptance record for
[Hot Path 17](https://app.clickup.com/t/86bc0jw07) under the
[contentionless data-plane epic](https://app.clickup.com/t/86bc0ju8a). It qualifies the integrated
data plane after Hot Path 01 through 16 and names the remaining synchronization sites together with
the contract that bounds each one.

The comparison revision is `144eac310533261a2e656f88c6dacc83c05be8d3`, the parent of Hot Path
01. Every A/B pair ran on the same host in one interleaved invocation while holding the shared
`/tmp/nervix-benchmark-ab.lock` lock. Cucumber retries were disabled.

## Lock-path audit

`just ratchet` reports 172 syntactic lock acquisitions and `DashMap::entry` calls across all files
classified as data-plane code. That deliberately broad count includes lifecycle work, cold
registration, test-only inspection, local collections whose `entry` method is not a lock, file I/O
whose `read` and `write` methods are not locks, and the explicit ordering fences below. A manual
trace of the owner batch, processor batch, ingest record, inbound frame, and acknowledgement paths
found zero acquisitions outside the epic's permitted fences.

| Path | Remaining synchronization | Bound and reason |
| --- | --- | --- |
| Relay owner batch | `RelayDispatchGate` engagement state in `runtime/relay_channel.rs` | The open fast path is two sequentially consistent atomic operations and never locks. The state mutex is reached only while a model change or ownership operation has explicitly closed the gate, and is bounded by the finite set of active engagements for that relay. |
| Remote owner delivery | `RelayOutboundSlot::gate` and its sequence mutex in `runtime/relay_boundary.rs` and `runtime/remote_dispatch.rs` | One slot is scoped to one destination, relay, payload role, and concrete branch. The async gate permits one encode/send at a time to preserve FIFO; the sequence mutex is entered only while that gate is held, protects three scalar fields, and is never held across an await. Slot lookup is a shared `get` on every established channel and `entry` only on the first racing installation. |
| Local relay fan-out | Atomics, `ArcSwap`, one lock-free queue per consumer, and `Notify` | Publishers do not serialize on a shared lock. Capacity bounds each consumer queue, and notification takes Tokio's internal waiter lock only while an atomic count proves that a publisher is waiting. |
| Processor batch | The `Mutex<BranchRuntime>` in `runtime/branch_runtime.rs` | One mutable runtime belongs to one concrete branch. Its dispatch lane admits one batch for that branch and queues later work, so the mutex expresses required branch-local processor ordering and is never reached through a map guard. Different branches execute independently. Expiry and eviction acquire it only after removing that branch from the instance registry. |
| Ingest record | The retained-payload mutex in `runtime/ingestor_quiesce.rs` | Normal intake reads an immutable `ArcSwap` publication and dispatches without a lock. The mutex is reached only after an explicit quiesce decision selects buffering; retained bytes are bounded by the route's declared `MAX SIZE`, and replay first checks an atomic buffered-record count. Kafka offset and endpoint route `entry` sites in the ratchet output are task-local `HashMap` setup or aggregation, not shared locks. |
| Inbound relay frame | Per-attempt admission state and the attempt/channel/admission maps in `interconnect/connection/relay.rs` | The mutex protects one attempt's small state transition among reserved, body-received, admitted, rejected, and cancelled, and no guard crosses an await. Map `entry` calls create or transition live protocol objects rather than read established state. There is at most one unadmitted batch per logical channel; typed admission quotas, relay stream quotas, the 32 MiB body limit, and the five-minute admission deadline bound the complete unresolved set. |
| Acknowledgement | The terminal sender `Option` in `runtime_ack.rs` | Per-share resolution and ownership-handoff accounting are atomic. Exactly one terminal transition takes the sender mutex, removes one `oneshot::Sender`, and releases the guard before sending. Interconnect terminal ACK retirement uses shared lookup before removal and is bounded by the unresolved admission set. |
| Metrics on these paths | A mutex inside each already-resolved histogram series in `metrics.rs` | Counter recording is atomic. A histogram handle is resolved when its task or branch is created, so recording takes no registry or map guard, constructs no key, performs one bounded histogram update, and holds no guard across an await. Snapshot and scrape readers run outside data-plane ownership. |

The audit found one remaining steady-state `entry()` in
`RelayBoundaryServices::outbound_slot`: every remote destination batch entered the shard even after
the slot existed. The qualification change now performs the borrowed shared lookup first and uses
`entry` only for a cold or racing installation, matching the existing ingress-slot contract.

The complete ratchet site listing and command output are attached to the task as
`data-plane-lock-sites.log` and `ratchet.log`.

## A/B benchmark matrix

The four workloads use Kafka partitions as concurrent publishers. Each cell ran three interleaved
pairs for ten measured seconds after a three-second warm-up. The two noisy cells were then rerun as
five clean interleaved pairs. Every run passed exact input/output cardinality.

| Workload | 1 publisher | 4 publishers | 16 publishers |
| --- | ---: | ---: | ---: |
| Ingest | +13.3% | +25.6% | +28.8% |
| Relay fan-out | +18.9% | +4.8% | +29.9% |
| Remote delivery | +2.3% | +3.9% | +8.8% |
| Processor | +15.8% | +20.6% | +25.4% |

All arms reached the configured 131,072-message backlog ceiling. These are therefore comparable
bounded-pressure rates, not claims about uncapped maximum throughput. The first remote-delivery/four
pair set contained a 7,056 msg/s candidate outlier and reported -18.3%; the clean five-pair rerun
measured 13,079 msg/s at baseline and 13,583 msg/s for the candidate, or +3.9%. The first
processor/sixteen set contained a candidate run coincident with Kafka reporting `Coordinator load
in progress` and reported -1.1%; the clean five-pair rerun measured 288,822 msg/s at baseline and
362,201 msg/s for the candidate, or +25.4%.

One attempted remote-delivery rerun reached setup while a two-voter Raft node reported
`last_log_index=6` and `last_applied=5`, then lost leadership on the first proposal. The benchmark
harness now waits for the exact voter count and equality of those indices before its first write.
The complete clean rerun passed after that readiness correction. Raw comparisons and run manifests
are attached to the task in `benchmark-summary.md` and `benchmark-evidence.tar.gz`.

## Three-node behavior

The public selection covers the contracts that can expose a hidden shared lock or stale published
handle:

| Area | Evidence |
| --- | --- |
| Relay | `runtime/relay_fanout.feature`: losing a subscriber leaves its other consumer flowing, including the three-node example |
| Ownership handoff | `cluster/drain_node.feature`: the gated owner handoff and replica promotion paths |
| Relocation | `cluster/relocate.feature`: materialized-state transfer and branch-local processor resumption on a three-node cluster |
| Quiesce | `runtime/entity_pause_alter.feature`: exact entity scope preserves the disjoint graph and resumes shared paths, including the three-node example |
| Replication | `runtime/deduplicator_replication.feature`: branch-local suppression survives a three-node restart with zero and one replicas |
| Metrics | `runtime/relay_metrics.feature`: relay traffic and buffer series retain their exact labels and values on the three-node examples |

The final command results are recorded in the validation section after the full suite completes.

## Architecture-source audit

No domain-clock mechanism changed in this task. `docs/src/domain-clock.md` still describes the
committed mapping and lifecycle generation, authority revision and incarnation fence, immutable
installed capability, replaceable one-report progress delivery, one execution snapshot per admitted
unit, logical versus physical deadline ownership, and restart reconstruction from committed state.

The remote slot correction does not change the wire protocol or transport lifecycle. During the
full-suite qualification, a three-node relocation exposed the interval in which a materialized
dependency reader can observe a committed destination before its prepared state activates, or the
previous destination just after it leaves the assignment. The read now treats rejected, unavailable,
and not-ready snapshot descriptions in that interval as ordinary absence and lets the declared
dependency policy decide whether to wait, skip, or use a default. `docs/src/interconnect.md` now
records that behavior. It also remains authoritative for immutable handler and live-target
publication, allocation-free and write-lock-free established stream leasing, per-channel FIFO with
one unadmitted batch, delivery identities and watermarks, bounded grant/admission/ACK state,
reserved management capacity, physical deadlines, and in-memory-only relay and ACK state.

The same qualification found a subscription-visibility race during prepared schedule activation.
The visibility handshake now includes every membership-live node, including a future owner that is
temporarily application-unavailable while it activates. This prevents the new owner from publishing
before it has observed existing subscriber interest.

## Flake isolation

The first full-suite run passed only after three retries of `Correlator branch expiration drops
pending correlation state`. The scenario observed one relay owner's inventory disappear without
first proving that the scheduled correlator owner had accepted the left record. Under three-node
delivery, the correlator could therefore create its branch after the scenario had already observed
zero instances. The scenario now records the scheduled owner, waits for its branch-instance gauge
to reach one, and then waits for it to return to zero before sending the right record. Three
consecutive retry-zero runs passed both cluster sizes: six scenarios and 108 steps.

One `HTTP polling follows paced domain cadence over multiple periods` step also retried once in the
first full-suite run. Three consecutive retry-zero isolation runs passed both cluster sizes at the
original 850 ms bound: six scenarios and 48 steps. The final full suite passed that bound without a
retry, so the strict cadence contract was retained.

## Validation record

| Command | Result |
| --- | --- |
| `just ratchet` | Pass after merging the latest `origin/main`: all counts at or below baseline; `result_string_errors=88`, `bare_error_signatures=812`, `data_plane_cluster_awaits=0`, `data_plane_lock_acquisitions=172`, and `write_once_rwlock_fields=1`. The benchmark framework's typed render errors lowered the debt counts and `debt-baseline.json` was updated. |
| `just test-benchmark-framework` | Pass: 30 tests. |
| Focused one-node/three-node Cucumber selection | Pass with retries disabled: relay fan-out 2 scenarios/24 steps; planned replica handoff 1/14; relocation 11/164; quiesce scope 2/26; deduplicator replication 3/69; relay metrics 7/108. |
| Correlator expiration isolation | Pass in three consecutive retry-zero runs: 6 scenarios/108 steps. |
| Paced HTTP cadence isolation | Pass in three consecutive retry-zero runs: 6 scenarios/48 steps. |
| `just test` | Pass after merging the latest `origin/main`: all workspace targets; Cucumber 173 features, 1,567 scenarios, and 16,617 steps with zero scenario retries. |
