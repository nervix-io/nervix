# Consensus Storage And Replication

Nervix uses Raft to replicate the control-plane state described in the
[Control Plane](./control-plane.md). Each node keeps its own durable log, applied state, votes,
membership, and snapshots. This chapter defines when those records are durable, how a leader paces
replication, how storage and memory stay bounded during catch-up, and what recovery may observe.

Consensus traffic uses the authenticated pools and admission classes described in
[Cluster Interconnect](./interconnect.md). Consensus storage is node-local: a quorum makes a command
committed, while every node independently persists and applies the committed log.

## Durable Write Path

Every consensus mutation uses a full synchronization barrier that asks the filesystem to persist
both data and filesystem metadata. The device and filesystem must honor that request for the
host-power-loss guarantees in this chapter to hold.

The path from a proposal to its reply has four distinct boundaries:

```text
client        leader log             follower logs          leader state machine
  |               |                       |                         |
  |-- propose --->|                       |                         |
  |               |-- SyncAll append ---->|-- SyncAll append       |
  |               |<----- quorum has the committed entry ----------|
  |               |------------------------------------------------>|
  |               |                      SyncAll applied range       |
  |<------------------------- applied response ---------------------|
```

| Boundary | What it proves |
| --- | --- |
| Appended | This node synchronized the complete log batch. It does not by itself mean that a quorum committed the entry. |
| Committed | A Raft quorum durably appended the entry. A node that has not applied it can replay it from the log or receive it again from the leader. |
| Applied | This node atomically synchronized the semantic changes, final applied position, membership, transaction progress, and revision for the applied range. |
| Client acknowledgement | The leader durably applied the command and produced its semantic response. Nothing acknowledged at this boundary is lost by process or host failure on storage that honors synchronization. |

### Appended Batches

Entries already ready for storage are encoded into the fewest reservation-bounded atomic writes.
One write performs one synchronization. A larger submission is split only when the next complete
entry would exceed the write's admitted memory; an entry is never split between writes.

The append call returns to Raft after the storage job has been admitted to the single ordered
consensus worker. Its I/O-completion signal is delivered later, after the write and synchronization
finish. Raft can keep its core responsive while storage works, but it cannot count the append as
durable before that signal. A vote, truncation, purge, or later append queued behind it observes the
same worker order.

A storage failure marks that node's consensus store failed. Later writes return
`consensus storage stopped after a failed write; restart the node to recover`. The error can arrive
after the device completed the write, so a failed or disconnected request is an uncertain outcome,
not proof that the command was absent. Recovery reads the durable state to decide.

### Applied Ranges And Client Replies

Committed entries are applied in order. Entries already available together share an atomic write
until adding the next entry's changes would exceed the reservation. That write contains the final
semantic records and metadata for the whole range. After synchronization succeeds, the node
publishes the resulting coherent revision and then answers every entry in the range.

A crash after a log write and before application does not lose an acknowledged command. If the
entry was committed, recovery streams it from the log and applies it. If it was not committed, a
later leader may discard it. In either case, the client had not yet received an applied response.
A crash or lost connection after durable application but before delivery of the response can leave
the client uncertain even though the command took effect; persistent administrative requests use
their stable execution reference to join or retrieve that result.

### Coordinated WASM Reset Publications

A coordinated WASM state reset stores its command execution reference, exact branch scope, phase,
and advanced guest-state generation in the domain schedule. The first applied schedule phase,
`Publishing`, is the irreversible authority boundary: after that applied response, restart and
leader failover continue the new generation even when its fresh initial checkpoint is not usable
yet. Repeating the same request rejoins that publication rather than appending another generation.

The initial guest checkpoint is runtime state and follows its separate local and replica
synchronization path. Live replicas install the published generation before the owner starts its
initial checkpoint. Reset preparation never publishes the temporary absence of a selected branch
through the branch-lifecycle checkpoint, and reinserting the branch advances that checkpoint's
revision before the new guest checkpoint is offered. Once the guest checkpoint is durable on the
owner and every assigned replica, a second consensus mutation changes the same reset to `Ready`.
Success is returned only after every live node applies that phase while the selected relay scope
remains fenced. A follower or offline replica that skips the intermediate runtime activation still
catches up through the ordered schedule records and cannot accept a checkpoint from the replaced
generation. Thus a pre-publication failure leaves the old schedule authoritative, while a
post-publication failure is recovered as committed but not yet usable rather than rolled back or
acknowledged early.

## Replication Pacing

The leader keeps one ordered append stream to each follower. That stream uses the replication pool;
heartbeats and leadership probes use a separate management connection so a slow or flow-controlled
log transfer cannot consume the path that maintains the leader lease.

One replication batch targets 1 MiB and stops at 1,024 entries. Commands remain whole, so the first
command may take a batch past the target; the encoded replication operation is bounded at 2 MiB.
Submissions and answers stay in log order. A conflict, higher vote, partial acceptance, stream
failure, or liveness failure ends that stream generation after its first result. Raft resumes from
the last progress the follower confirmed.

The leader permits at most 16 unacknowledged batches for one follower. It also waits whenever its
unacknowledged bytes plus the next 1 MiB target would exceed 16 MiB. Because one complete command
may cross the target, measured outstanding bytes may exceed that target budget by the excess of one
encoded batch; the 2 MiB operation limit remains the hard bound on that batch. The node's Commands
memory budget separately bounds the aggregate encoded and decoded copies across all peers.

### Progress-Based Liveness

Opening an append stream has a five-second deadline. Once open, the stream has no age limit. With a
batch outstanding, it is considered stalled only after five continuous seconds in which both of
these are true:

- no answer arrived;
- the follower accepted none of the leader's bytes.

An answer or transport flow-control progress restarts the idle window. A stream with no outstanding
batch can remain idle indefinitely. The time required for a follower to synchronize one batch is
therefore not treated as a failure while the transfer is still moving.

Heartbeats keep the shorter deadline derived from the configured heartbeat interval, capped at five
seconds. With the default 250 ms heartbeat, the current soft deadline is 187.5 ms. Replication
answers also prove leader contact, so sustained writes suppress redundant heartbeats for an
automatically derived window. The window is no longer than the heartbeat interval or half the gap
between that interval and the minimum election timeout. This keeps the heartbeat interval plus the
suppression window below the minimum election timeout.

The independent paths matter when a follower has slow storage: its append answer may take much
longer than a heartbeat deadline, while management traffic still prevents an election. Changing the
heartbeat interval does not change the append stream's five-second progress bound.

### Follower Memory And Backpressure

A follower takes a Commands-budget reservation before it decodes an append batch. It retains that
reservation until Raft has durably appended the batch and produced its answer. The default resident
window is four decoded batches per live stream. Reaching the window stops frame reads before another
batch is decoded, closes the HTTP/2 flow-control window, and makes the leader wait.

The default 24 MiB Commands budget is validated to hold the complete resident window beside one
maximum-size 2 MiB batch being encoded. Other command and replication work shares that aggregate
budget, so concurrent or superseded streams cannot grow memory without admission. Ending a stream
releases its queued batches and reservations; batches from a generation the leader abandoned do not
remain uncharged in the Raft core.

## Follower Catch-Up And Learner Promotion

A follower whose required log suffix is still retained receives bounded append batches over its
stream. If the leader has already purged part of that suffix, the follower receives the current
snapshot and then the remaining log. Client proposals and management heartbeats continue while the
catch-up runs.

A discovered node joins as a learner. Each membership reconciliation gives a blocking learner
catch-up attempt ten seconds. Promotion to voter is proposed only after that attempt reports the
learner caught up; the membership change has its own ten-second bound. A timeout leaves the node a
learner, records `raft membership reconciliation failed`, and the one-second reconciliation loop
tries again while ordinary replication continues.

The consensus event stream and `info` log record `raft add learner`, `raft wait for learner`,
`raft promote voters`, and `raft membership updated` transitions. `SHOW CLUSTER STATUS` reports the
local `raft.last_log_index`, `raft.last_applied`, and each member's learner or voter role.

## Bounded Log Reading

Committed-range application, startup replay, and leader-bounded replication streams read the log in
successive admitted chunks. A chunk targets 1 MiB or 1,024 entries, whichever is reached first, and
one oversized first entry travels alone. The reader releases the ordered storage worker between
chunks and retains the chunk's memory charge until its entries have been handed to the consumer.

There is no range-size failure at 2 MiB or at any other aggregate backlog size. A committed range
and a startup backlog may span any number of chunks. The state-machine worker applies the entries
already read before requesting another chunk, so a large backlog does not have to exist as one
allocation. A replication read also revalidates the leader's durable vote at every chunk boundary;
it stops rather than reading through a truncation performed under another leader.

## Log Retention And Admission

`retained_bytes` is the sum of the encoded values for the log entries the node currently holds. It
includes entries still in Fjall's memtable and excludes purged entries, tombstones, stale segment
space, snapshots, and other keyspaces. Append adds encoded entry bytes; purge and truncation subtract
the removed entries in the same ordered storage operation. Opening the database rebuilds the counter
by scanning the current log, so a process failure between a durable mutation and its in-memory
counter update does not skew the recovered value.

A node starts one snapshot after either of these thresholds is crossed:

- 10,000 committed entries since the completed snapshot;
- 64 MiB of appended entry bytes since the completed snapshot.

Only a completed snapshot permits covered log entries to be purged. By default the node retains the
newest 1,000 covered entries or 64 MiB of covered suffix, whichever bound is reached first. Entries
newer than the snapshot remain regardless of those settings. A failed or unfinished build therefore
does not remove the log needed to recover.

When `retained_bytes` is already above the 1 GiB admission cap, a new control-plane mutation waits
up to 30 seconds for snapshotting and purge to reclaim space. If it remains above the cap, the write
fails with:

```text
the retained raft log holds <retained> bytes against a <cap>-byte cap and did not reclaim within 30s
```

The check occurs before admitting the next mutation, so the mutation that crosses the cap may
complete. Reads, health checks, replication, snapshotting, and recovery remain available while new
mutations wait.

## Snapshot Lifecycle

### Sealing And Publication

A snapshot generation is a sequence of sections containing keyed state-machine records plus a
manifest that names the generation, applied log position, membership, section count, and total
bytes. The builder opens one consistent database view and seals it one section at a time. The
default section limit is 8 MiB. Each section acquires the Bulk budget for one consensus-storage
turn, is encoded and synchronized, and releases that reservation before the next section starts.
Concurrent state-machine writes can proceed between sections without changing the pinned view.

The manifest is synchronized only after every section is durable. Publishing that one record makes
the generation active atomically with its applied position and membership. A build interrupted
before manifest publication leaves the previous generation active.

### Generations And Pins

The newest published generation is always retained. An outgoing transfer pins the generation it is
reading. Multiple readers can share one pin, and at most one older generation may remain pinned
beside the active generation. If publication would leave a second older generation pinned, the
oldest transfer is made obsolete; its next read fails and Raft restarts it from the newest snapshot.

A superseded generation with no reader becomes unreferenced. Its sections are removed as part of a
later durable consensus batch. Startup discovers sections that no published manifest names and
marks those generations for the same deletion path. This keeps deletion ordered and durable without
treating an unfinished build or abandoned transfer as active state.

### Transfer And Installation

One complete snapshot transfer has a 30-second deadline. The sender reads one section at a time and
sends it in 64 KiB chunks through the Bulk pool. The receiver holds at most one section in memory,
validates its declared size and offsets, and synchronizes each completed section into a newly claimed
generation. A restarted transfer abandons the earlier staged generation.

After every declared section arrives, Raft revalidates the vote and whether the snapshot still
applies. Installation then synchronizes the new manifest together with an installation marker. That
write is the recovery boundary: before it, the old snapshot and state machine remain authoritative;
after it, startup must finish installing the marked generation.

The installer clears the prior state-machine records, writes each staged section in order, and
finally removes the marker while publishing the new generation. Every step is idempotent. A process
that stops after the marker was published redoes the complete replacement on its next start,
regardless of how many clear or section writes had finished. It exposes no recovered state until
the replacement and current-shape validation succeed.

## Storage Layout And Compatibility

| Path | Contents and ownership |
| --- | --- |
| `<db-path>` | Registry records and runtime-state checkpoints in the node database. |
| `<db-path>/consensus` | The dedicated consensus database and journal: log entries, votes and log metadata, applied state-machine records, snapshot manifests, installation markers, and snapshot sections. |

The databases have independent journals, memtables, rotation, and synchronization. A consensus
`SyncAll` neither flushes runtime journal bytes nor waits behind a runtime journal write. Both use
the bounded storage executor, but consensus has its own single ordered worker.

This layout has one current shape. A node database containing the earlier shared `raft_*` keyspaces
fails startup with `consensus storage shares the node database; recreate the node's stored state for
the dedicated consensus database layout`. A consensus database containing an unknown keyspace, a
malformed current archive, or incomplete current state fails with `invalid consensus record
storage; recreate the node's stored state`. Nervix does not migrate, reinterpret, or default those
records; recreate the node's stored state and let it rejoin from the cluster.

## Shutdown And Forced Endings

Consensus remains available throughout stop admission and graph drain. During terminal teardown,
the node stops Raft, which ends its append stream generations, then submits an idle barrier to the
ordered storage worker. Reaching that barrier proves every earlier submitted durable write returned
and released the consensus store before its database handle is dropped. The interconnect is stopped
after consensus, so peers first observe the append protocol ending and later the transport closing.

A repeated signal, an expired shutdown deadline, or `SIGKILL` skips that barrier. Recovery then sees
the last complete atomic storage boundaries: an append batch is present or absent; an applied range
contains its records and final metadata together or not at all; and a marked snapshot installation
is redone in full. See [Shutdown And Recovery](./shutdown.md#terminal-teardown) for the exact
SIGTERM, SIGKILL, stream-teardown, and restart behavior.

## Observability

| Signal | What catch-up or retention looks like |
| --- | --- |
| `SHOW CLUSTER STATUS` | On each node, `raft.last_log_index` advances as entries arrive and `raft.last_applied` follows durable application. Membership lines show learner or voter state. |
| `nervix_consensus_log_last_index` | The highest entry held locally. Compare nodes to see a log follower converge. |
| `nervix_consensus_log_snapshot_index` and `nervix_consensus_log_purged_index` | The snapshot coverage and removed prefix. A follower behind the purged index must use a snapshot. |
| `nervix_consensus_log_retained_bytes` | The exact encoded bytes in current log entries and the value used by admission. |
| `nervix_execution_memory_reserved_bytes{class="commands"}` | Includes decoded follower batches held until durable answers and replication payload admission. It stays bounded by the Commands capacity. |
| `nervix_interconnect_pending_operations{operation="append"}` | Live append-stream operations on the exporting node. |
| `nervix_interconnect_stream_resets_total{class="replication"}` | Abnormal replication stream endings, separated by deadline, capacity, peer, shutdown, or malformed reason. |
| `nervix_interconnect_bulk_bytes_total{class="bulk"}` | Bytes moving during snapshot catch-up. |
| `nervix_consensus_snapshot_pinned_generations`, `nervix_consensus_snapshot_pinned_readers`, `nervix_consensus_snapshot_unreferenced_generations` | Generations retained for outgoing transfers and generations awaiting durable deletion. |

Normal membership changes appear at `info`. Append generation endings and invalid or foreign batches
appear at `debug`; a stalled generation reports that the stream neither answered nor accepted bytes
for five seconds. Snapshot requests caused by the byte threshold appear at `info`, and byte-retention
purge requests appear at `debug`.

## Tuning

The server exposes heartbeat, election, snapshot, and retention policy as command-line arguments and
matching environment variables:

| Argument | Environment | Default | Effect |
| --- | --- | --- | --- |
| `--raft-heartbeat-interval` | `NERVIX_RAFT_HEARTBEAT_INTERVAL` | `250ms` | Period for leader liveness probes and basis for their soft deadline. It does not set the append-stream idle bound. |
| `--raft-election-timeout-min` | `NERVIX_RAFT_ELECTION_TIMEOUT_MIN` | `1500ms` | Lower end of the randomized follower election timeout. Must exceed the heartbeat interval plus the derived suppression window. |
| `--raft-election-timeout-max` | `NERVIX_RAFT_ELECTION_TIMEOUT_MAX` | `3000ms` | Exclusive upper end of the randomized election timeout. Must exceed the minimum. |
| `--raft-snapshot-entry-threshold` | `NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD` | `10000` | Applied entries since the last completed snapshot that request the next build. |
| `--raft-snapshot-byte-threshold` | `NERVIX_RAFT_SNAPSHOT_BYTE_THRESHOLD` | `64MiB` | Appended entry bytes since the last completed snapshot that request the next build. |
| `--raft-covered-log-entries-retained` | `NERVIX_RAFT_COVERED_LOG_ENTRIES_RETAINED` | `1000` | Newest snapshot-covered entries retained for log catch-up. |
| `--raft-covered-log-bytes-retained` | `NERVIX_RAFT_COVERED_LOG_BYTES_RETAINED` | `64MiB` | Newest snapshot-covered encoded bytes retained; the entry or byte bound reached first decides. |
| `--raft-retained-log-cap` | `NERVIX_RAFT_RETAINED_LOG_CAP` | `1GiB` | Current-log byte level above which later mutations wait up to 30 seconds for reclamation. |

Replication batch and memory limits are fixed by the current server execution policy; the server
does not expose flags for them:

| Limit | Current value |
| --- | --- |
| Replication batch target | 1 MiB or 1,024 entries; whole commands are never split |
| Encoded replication operation | 2 MiB |
| Leader window per follower | 16 batches and a 16 MiB target byte budget |
| Follower decoded resident window | 4 batches per live append stream |
| Commands memory budget | 24 MiB per node |
| Append stream setup / progress idle bound | 5 seconds / 5 seconds |
| Snapshot section / transport chunk | 8 MiB / 64 KiB |
| Complete snapshot transfer | 30 seconds |

Use the timing settings as one policy. Lowering the election minimum without lowering the heartbeat
interval can make the configuration invalid at startup. Raising the heartbeat interval does not
allow slower append storage because append liveness is progress-based and fixed at five seconds.

Snapshot and covered-log settings trade storage and catch-up method. Smaller thresholds build and
transfer snapshots more often; smaller covered suffixes make a lagging follower switch to a snapshot
sooner. Larger thresholds and suffixes retain more log and require a retained-log cap large enough
for the working set. The cap should leave room for entries newer than the latest completed snapshot,
because those entries cannot be purged regardless of the covered-log settings.
