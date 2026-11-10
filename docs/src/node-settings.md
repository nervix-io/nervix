# Node Settings

Every node setting has one class, a typed owner, one default, a derivation when applicable,
and a validation boundary. Values that depend on an external system belong to that connector's
declaration in Plexus, the Nervix graph definition language.

The vocabulary provides `SettingDefinition`, three sealed classes and `Setting<D>`.
The source ratchet counts numeric policy outside that boundary. Every option `nervix-server`
accepts is either a declared setting in its owner's group or part of the node's identity,
addresses and paths, which are not settings; [The Server's Settings](#the-servers-settings) lists
them. Runtime constants that are not options keep their current wiring while their area owners
adopt these declarations, and this chapter states the contract that adoption must satisfy. The
class API alone publishes no record, commits no parameter, detects no capacity and changes no
running service. A node publishes, carries and verifies its prerequisites as
[Prerequisites At Join](#prerequisites-at-join) describes, and its
[startup preflight](#the-startup-preflight) detects its memory and measures its volumes. Cluster
parameters have one committed Raft record, seeded at the first bootstrap and changed through
`ALTER CLUSTER SET`. Every admitted node announces its resolved capability record through Raft.

Runtime admission and failed committed-runtime installation share the existing half-second retry
cadence owned by `RuntimeAdmission`. This remains an internal policy constant pending settings
adoption; installation does not introduce another numeric policy or an operator option. Committed
schedule and domain updates can wake an installation retry sooner, and success ends that cadence.

Canonical Kafka start admission derives its deadline from the interconnect checkpoint-transfer
contract. Selecting and durably adopting a start cut waits for that transfer, so it shares the
existing transfer budget rather than defining another numeric policy or operator option.

Changes to the server's command line, startup settings and initialization contract keep the shipped
Kubernetes operator's generated startup command, chart and example current in the same delivery.
The server-image [deployment checks](installation-kubernetes.md#deployment-qualification) qualify
both native architectures before publication; the operator's own repository checks alone cannot
detect server changes.

## Choose The Class

Apply one test: does the value establish how a node reaches the cluster, what the cluster
accepts or promises, or what this node can carry?

| Class | Meaning | Authority and carrier | Verification | Change procedure |
| --- | --- | --- | --- | --- |
| Prerequisite | What a node must bring to reach the live cluster | Local provisioning or detection; gossip record and join handshake | Compare at join; refuse the incompatible node | Provision identity and trust; roll timing changes within supported relationship bounds |
| Cluster parameter | What the cluster accepts or promises | One committed Raft record; local flags seed first bootstrap only | Validate the transition at commit and Models against committed values | One administrative Plexus statement; apply live and rebuild construction-time owners in place where needed |
| Node capability | What one node can carry | Detected or overridden locally; announced through Raft as that node's capability record | Validate the local claim and its relationships; peers honour advertised bounds | Restart with new overrides or detected capacity; peers and placement adopt the committed record |

Two values that give the cluster two answers about acceptance or guarantees identify a cluster
parameter. A Model-validated cap is never an independent node flag. Capabilities change local
admission, placement or the work a peer can send; they cannot relax cluster guarantees. A node
without a committed capability record receives no placements. If an advertised bound changes,
a peer can refuse an operation with a typed answer so the sender slices it again while retaining
the row acknowledgement accounting.

## Committed Node Capabilities

`--cpu-count` (`NERVIX_CPU_COUNT`) overrides the process's cgroup-aware available parallelism.
`--memory-limit` (`NERVIX_MEMORY_LIMIT`) overrides the existing cgroup/physical-memory detector.
Both overrides must be positive. A failed host query remains `unknown`; memory preflight and the
committed claim use the same resolved measurement. The startup log includes these resolved values
and the receive bounds. Worker pools, budgets and watermarks retain their own setting owners.

After membership admission, each node submits one incarnation-bound `NodeCapabilityRecord` to the
current leader. Raft commits it under the node name. Repeating an identical announcement is a
no-op; a newer incarnation replaces the record, and an earlier or differently configured identical
incarnation is refused. Departure retains the last claim; `DROP NODE` removes it and fences that
incarnation. A restarted node waits for its own current claim before activating stored assignments.
Capability records are absent from the join handshake and gossip.

The receiver's local capacity options are:

| Option | Environment variable | Default |
| --- | --- | --- |
| `--interconnect-relay-batch-bytes` | `NERVIX_INTERCONNECT_RELAY_BATCH_BYTES` | Resolved executor encoded relay limit |
| `--interconnect-relay-decoded-bytes` | `NERVIX_INTERCONNECT_RELAY_DECODED_BYTES` | Resolved executor decoded relay limit |
| `--interconnect-bulk-chunk-bytes` | `NERVIX_INTERCONNECT_BULK_CHUNK_BYTES` | Resolved executor bulk chunk limit |
| `--interconnect-bulk-receive-window` | `NERVIX_INTERCONNECT_BULK_RECEIVE_WINDOW` | 1 MiB per bulk stream |

All are positive. The first three must fit the supplied executor's corresponding operation limit.
The bulk window must fit HTTP/2's connection limit for every stream and have its dedicated memory
budget behind it before listening. These options govern inter-node delivery independently of
public client admission. Change a claim by restarting the node with new overrides.

`DESCRIBE CLUSTER` shows each live incarnation's committed CPU count, memory bytes, four receive
bounds and its engine/connector host capabilities, pending claims, and retained departed records. Its `cluster-effective receive bounds` are
the componentwise minimum of matching live committed claims. Individual sends use their receiver's
claim, so one smaller peer does not shrink traffic between larger peers. The interconnect chapter
owns refusal, re-slicing, bootstrap transfer and flow-control behavior; the placement chapter owns
the capacity score and its precedence.

`adopted peer receive bounds` lists the claims this answering node's outbound pools currently use.
The independent capability controller refreshes them on the existing half-second runtime retry
cadence, including when another domain's leader reconciliation is busy. This separates committed
capacity from its local adoption without a second configuration authority.

## Transport, Resolver And Database Capacities

These are Nervix node capabilities. Provision them as server options, or with the matching
`NERVIX_` environment variable formed by uppercasing the option and replacing hyphens with
underscores. Restart a node to change its claim; `DESCRIBE CLUSTER` reports the committed claim
of each live incarnation, including its interconnect, DNS, database and consensus values.
The startup log records the same resolved values. They change local capacity and pacing,
without adding a prerequisite or changing graph acceptance.

| Server option | Default | Meaning |
| --- | --- | --- |
| `--interconnect-max-peers` | 64 | Other node identities this transport can retain |
| `--interconnect-concurrent-handshakes` | 32 | Concurrent transport handshakes |
| `--interconnect-incoming-queue-capacity` | 1,024 | General request and relay admission capacity |
| `--interconnect-request-quota` | Derived | Independent ordinary and append reserves: two requests per declared peer, bounded by the incoming queue |
| `--interconnect-stream-receive-window` | 64 KiB | Advertised HTTP/2 receive credit per non-bulk stream |
| `--interconnect-connection-receive-window` | 256 KiB | Advertised HTTP/2 receive credit per non-bulk connection |
| `--interconnect-connection-setup-timeout` | 5s | One DNS, TCP, TLS and binding budget |
| `--interconnect-request-timeout` | 10s | Transport request budget and the base of typed RPC deadlines |
| `--interconnect-progress-timeout` | 5s | Time without transfer progress |
| `--interconnect-reconnect-backoff` | 200ms | First reconnect delay |
| `--interconnect-max-reconnect-backoff` | 5s | Reconnect ceiling, at least the first delay |
| `--interconnect-reconnect-jitter-permille` | 0 | Downward jitter in thousandths, from 0 through 1,000 |
| `--dns-cache-entries` | 4,096 | Cached name/type answers |
| `--dns-positive-max-ttl` | 1h | Positive DNS TTL ceiling |
| `--dns-negative-max-ttl` | 30s | Negative DNS TTL ceiling |
| `--dns-hook-lookup-budget` | 30s | Lookup budget for client libraries without an operation deadline |
| `--dns-address-family-policy` | `dual-stack` | DNS queries: `dual-stack`, `ipv4` or `ipv6` |
| `--dns-trust-negative-answers` | `true` | Trust a name server's negative response without asking the next server |
| `--database-cache-bytes` | 32 MiB | Node database cache |
| `--database-journal-bytes` | 512 MiB | Node database journal capacity, at least 64 MiB |
| `--database-workers` | Derived | Available CPU count capped at four; one when unknown |
| `--raft-database-cache-bytes` | 32 MiB | Dedicated consensus database cache |
| `--raft-database-journal-bytes` | 512 MiB | Consensus database journal capacity, at least 64 MiB |
| `--raft-database-workers` | Derived | Same CPU derivation, independently overridable |

Counts, receive windows and deadlines must be positive and fit their engines. HTTP/2 windows
are at most 2³¹−1 bytes. The options are validated before opening stores or binding listeners.
Database cache capacities count alongside executor budgets in the startup memory check.

Connection capacity derives from both directions of every pool: twelve connections per declared
peer, 768 by default. Watermark capacity is eight times the incoming queue. Health-probe fan-out
and DNS lookup concurrency each equal `interconnect-max-peers`. Completion reserves are at least
one and otherwise one per sixteen peers, bounded by the incoming queue.

The default ordinary and append request reserves are now 128, replacing eight for the whole node.
An append stream retains its admission throughout its lifetime, so eight cannot serve nine
followers. Liveness admission increases from 32 to 64 with the default peer count. Each traffic
reserve remains independent; a full general queue cannot consume health or cancellation capacity.
An explicit lower request quota is reported as configured and refusal is a typed capacity outcome.

Typed RPCs derive their deadlines from the request budget: control one tenth, coordination one
fifth, setup one half, ordinary requests one, snapshot operations three, checkpoint transfers six,
and recovery twelve. A caller's remaining operation deadline still governs explicitly bounded
work. The application-health probe instead follows the running cluster liveness policy below.

Transport ping and application-health budgets follow the installed, Raft-committed
`NODE_UNAVAILABILITY_TIMEOUT`: ping interval one half, ping timeout one quarter and probe timeout
one tenth. At `4s`, `DESCRIBE CLUSTER` shows `ping-interval=2s ping-timeout=1s
probe-timeout=400ms`. Installing a revision updates established HTTP/2 connections as well as
future probes. A policy update adjusts an outstanding ping's deadline while retaining that ping.
The per-node line derives from its published running revision, so an unapplied commit is not
presented as installed.

Relay reconciliation retains channels for twice the host's total admission bound and sweeps at
one tenth of that retention: 600s and 60s under the current 300s host contract. These are
projections of the host's one policy input, without independent flags. An idle sender rotates its
channel at the host bound, before its receiver can forget the watermark. The runtime settings
owner retains authority over host admission and acknowledgement cadence.

### Consensus Capacities

These options have the same node-capability change procedure and environment-variable convention.

| Server option | Default |
| --- | --- |
| `--raft-append-batch-entries` | 256 |
| `--raft-outstanding-append-batches` | 16 |
| `--raft-outstanding-append-bytes` | 16 MiB |
| `--raft-append-idle-bound` | 5s |
| `--raft-snapshot-transfer-timeout` | 30s |
| `--raft-membership-mutation-timeout` | 10s |
| `--raft-retention-interval` | 5s |
| `--raft-retention-admission-timeout` | 30s |
| `--raft-purge-batch-entries` | 1 |
| `--raft-api-channel-capacity` | 65,536 |
| `--raft-notification-channel-capacity` | 65,536 |
| `--raft-state-machine-channel-capacity` | 1,024 |
| `--raft-api-batch-capacity` | 4,096 |
| `--raft-event-capacity` | 256 |
| `--raft-replication-backoff` | 200ms |
| `--raft-replication-backoff-maximum` | 1s |
| `--raft-membership-step-down-delay` | 150ms |

The append byte window must hold the executor's largest replication batch. A stream stalls only
when it has outstanding work and neither receives an answer nor sends accepted bytes throughout
the idle bound. Heartbeat response waits keep their lease budget; they do not borrow the data
stream's storage deadline.

The duration options in this table use positive whole milliseconds and reject fractions rather than
rounding the configured value in the engine.
The default replication retry series remains 200ms, 400ms, 800ms, then 1s at its ceiling.

Retention admission polls at the smaller of the retention interval divided by 100 and its
admission timeout divided by 600, with a one-millisecond engine floor: 50ms by default. Snapshot
replication lag is twice the snapshot entry threshold, saturating only at the integer ceiling
and remaining strictly greater than that threshold; the threshold cannot be `u64::MAX`.
Snapshot chunks follow the executor's bulk chunk bound. The replication chapter owns durability,
whole-transfer deadlines and the explicit maintained OpenRaft configuration.

## Permanent Cluster Identity

The first leader commits a permanent UUID and signing key for the cluster. `DESCRIBE CLUSTER;`
prints their public identity immediately after the leader line:

```text
leader: node-1
cluster uid: 72f3a93e-64f0-40a5-b95d-790c8a738fee
cluster key: <key thumbprint>
cluster created: 2026-11-08T12:00:00Z
```

The key value is its RFC 7638 SHA-256 thumbprint in canonical base64url. Every node reports the same
UUID, key and creation instant after it applies the record. Restart, leadership transfer and
membership changes preserve them. The private key is never included in this description.

`--cluster-id` remains the operator's display name and interconnect/gossip prerequisite. The
indented `cluster identity` prerequisite names that configured value; the unindented `cluster uid`
names the permanent generated identity. [Backup And Restore](backup-and-restore.md#what-a-restore-recreates)
explains why a fresh cluster restored from an archive keeps its own UUID and key.

## Cluster Licensing

A cluster uses the embedded free policy from its first bootstrap. Both current quotas,
`cluster.nodes` and `cluster.domains`, are unlimited. Node usage counts committed voters and
learners; domain usage counts committed domain records, including stopped domains. Returning
members and updates to an existing domain do not consume another slot.

```plexus
DESCRIBE LICENSE;
```

Every node answers this read-only command without a selected domain or a leader redirect.
It reports `mode: free`, `policy: free`, the public cluster identity, committed licensing time,
`processing: running`, and each quota's committed usage and limit. Followers describe their own
applied revision. The same observation accompanies the command as a typed state-inspection
envelope with a cluster-license entity, a single unit and shared Row sections; applications can
read it without parsing the display text.

The immutable published policy is the permission source for each admission. Domain planning
captures its quota constraint, and the committed creation checks that constraint against the
actual domain count. Membership admission checks before adding a learner or voter. A refusal
keeps its typed licensing cause through the owner's error boundary; it does not remove existing
domains or members. Free mode has no processing stop or next licensing deadline.

The free licensing record is installed atomically with the first committed cluster identity,
using that identity's creation timestamp. Replication, snapshots and restart preserve it. Its
time is a committed fact rather than a read of each answering node's clock. See
[Licensing Metrics](metrics-and-observability.md#licensing-metrics) for the matching observations.

## The Prerequisite List Is Closed

The prerequisites are cluster identity; the trust root; the node-to-node wire contract; the
command and management-event size floors needed to join consensus; the durability promise of the
Raft log; the Raft heartbeat, from which the election timeouts derive; the gossip interval, from
which failure detection derives; and the physical clock skew a node tolerates. One record holds
them all, and each has one source:

| Prerequisite | Source | Default | Two nodes run together when the values are |
| --- | --- | --- | --- |
| Cluster identity | `--cluster-id`, which the node's certificate must also name | `default` | equal |
| Trust root | Detected: a SHA-256 fingerprint over the SHA-256 digests of the trust anchors in `--interconnect-tls-ca`, sorted and without repeats, so the order of the file does not change it | none | equal |
| Wire contract | Fixed by the build: the fingerprint of the one node-to-node contract it speaks | none | equal |
| Largest command, largest management event | Detected: the executor's operation limits | 1 MiB, 64 KiB | equal |
| Durability promise | Fixed by the build: every Raft log append is synchronized, data and metadata, before the node acknowledges it, written `fsync every append` | none | equal |
| Raft heartbeat | `--raft-heartbeat-interval` | `250ms` | within a factor of two |
| Gossip interval | `--gossip-interval` | `500ms` | within a factor of two |
| Clock-skew tolerance | `--clock-skew-tolerance` | `500ms` | within a factor of two |

Two timings are within a factor of two when they are equal or the larger is below twice the
smaller: a node with a `250ms` heartbeat runs beside one with `400ms`, not beside one with
`500ms`. One rolling change may therefore move a timing by less than a factor of two, and a larger
move takes more than one roll. The factor is a fixed contract rather than a setting, because the
Raft timings rely on it. A node does not configure its election timeouts: a follower stands for
election after a randomized timeout between six and twelve of its own heartbeat intervals, 1.5 to 3
seconds by default. A compatible leader's heartbeat is shorter than twice the follower's, so the
slowest heartbeat a compatible leader sends stays within a third of the follower's minimum
election timeout. Gossip derives its failure detector the same way: it assumes ten gossip
intervals between a peer's heartbeats until it has observed some, and takes no gap longer than
twenty gossip intervals as a sample, 5 and 10 seconds by default.

The maximum Raft election timeout also bounds the leader lease and a heartbeat's response wait.
The interval still schedules heartbeats; an answer later than one interval but inside the lease
can renew it using that request's sending time. This adds no node setting or separate timing
prerequisite.

A proposed new prerequisite first has to establish why a committed cluster parameter applied by
rebuilding its owner cannot express it.

## The Startup Preflight

Before it binds a listener or opens a store, a node checks the environment it is about to run in.
It refuses to start at the first check that fails, exiting with status `1` and
`the startup preflight refused this node` above the cause:

1. It validates explicit execution and receive bounds before detection. The native process resolves
   CPU capacity once before constructing its runtime; in-process starts resolve it during preflight.
2. It resolves its memory limit: `--memory-limit` when given, otherwise the smallest memory limit
   of its control group and the groups above it, in the unified hierarchy or the version 1 memory
   hierarchy, or its physical memory when no group limit is smaller. It derives the execution
   settings, client byte credit, omitted receive limits and memory-pressure watermarks from these
   capacities. Every memory pool counts once, including independent active state reads and retained
   state-read cache. Their total plus producer and consumer node credit and the configured node and
   consensus database caches must fit known memory.
   A failed detector leaves capacity unknown and uses the declared capacity-independent defaults.
3. It creates its database directory and its temporary directory when they are absent, proves each
   writable with a probe file it removes again, and measures the total size of the volume each
   lives on.
4. It resolves its restore-staging quota against the total size of the volume that holds the
   database directory: `--restore-staging-max-bytes` when given, otherwise the smaller of 128 GiB
   and that size. It then requires the quota, `--resource-max-archive-bytes` and
   `--resource-max-extracted-bytes` each to fit that size. Each quota is compared with the size on
   its own, and nothing the volume already holds is subtracted.
5. It loads its name resolver, as [Name Resolution](name-resolution.md#resolver-lifetime)
   describes.

Each refusal names the setting or directory it judged, with the values involved:

| Check | Refusal |
| --- | --- |
| Memory budgets | `the execution memory budgets of 101.25MiB exceed node memory 32MiB` |
| Memory watermark | `memory-high-watermark (4GiB) exceeds memory-limit (3GiB)` |
| Directories | `the database directory <path> cannot be created`, `the temporary directory <path> is not writable` |
| Volumes | `the capacity of the volume holding the database directory <path> cannot be read` |
| Quotas | `restore-staging-max-bytes (128GiB) exceeds the 64GiB capacity of the volume holding the database directory <path>`, for a quota the operator gave, or `resource-max-extracted-bytes (16GiB) exceeds the 5GiB capacity of the volume holding the database directory <path>` |
| Resolver | `failed to load the name resolver configuration`, above the cause that resolver loading reports |

Only the restore-staging quota, a node capability, follows the volume. The resource quotas are
cluster parameters with fixed defaults, 4 GiB and 16 GiB. A node whose database volume is smaller
refuses to start. Provision a larger volume, seed smaller quotas on a new cluster, or change an
existing cluster's committed quotas through `ALTER CLUSTER SET` before joining it. A node that passes logs
`the startup preflight passed` at `info` with its memory limit, its memory budgets, the size of both
volumes and the restore-staging quota it resolved.
[Capacity Planning](capacity-planning.md#what-a-node-needs-to-start) sizes them.

Clock skew is the one environmental check that needs a peer, so a node makes it as it joins, as
[Prerequisites At Join](#prerequisites-at-join) describes. Environmental checks add no setting
class. [Name Resolution](name-resolution.md), [Domain Clock](domain-clock.md) and
[Consensus Storage And Replication](consensus-storage-and-replication.md) own their detailed
failure and timing contracts.

## Reachability Determines Layering

Nothing a node needs to reach the live cluster can depend on a value it learns only after
reaching it. Gossip establishes discovery from local identity, trust, bootstrap hosts and its
prerequisite cadence. The handshake verifies prerequisites. Consensus carries cluster
parameters and node capability records. The registry validates Models against committed
parameters; engines and the data plane receive resolved construction inputs or plans.

The join handshake carries prerequisites and needs no consensus read to establish consensus.
A returning node obtains construction-time cluster parameters from the live cluster's
published state and verifies them against the log as it catches up. Its stale local log is
not the initial policy authority. [Cluster Interconnect](interconnect.md) owns publication,
authentication, wire forms and discovery.

## Execution Capabilities

Execution options are node capabilities resolved at startup. Every flag has its `NERVIX_`
environment twin, formed by uppercasing its name and replacing hyphens with underscores.
Omitted values derive; an explicit value, including one equal to a usual default, stays explicit.

The five CPU classes keep independent admission. Control and credentials each default to one
worker. Data, extension and bulk divide the remaining CPU count as evenly as possible, in that
order, after honoring explicit requests. Each retains at least one place on small nodes, so the
CPU admission total is at most the larger of CPU count and five. These are admission capacities;
they do not promise an idle physical core. A stuck extension cannot take a data or control place.
Consensus storage defaults to one worker; filesystem and connector storage each default to two.

For each class, `--execution-<class>-workers` and `--execution-<class>-pending-jobs` override its
workers and queue. Class names are `control-cpu`, `credentials-cpu`, `data-cpu`, `extension-cpu`,
`bulk-cpu`, `consensus-storage`, `filesystem-storage`, and `connector-storage`. A queue defaults
to 64 pending jobs per worker, capped at 1,024 per class. Counts must be positive and representable
by the admission primitive; explicit CPU over-allocation is refused.

Each pool has `--execution-<pool>-memory-bytes`. With known memory, defaults are:

| Pool | Memory fraction | Minimum working set |
| --- | --- | --- |
| `management` | 1/128 | 64 KiB |
| `commands` | 1/32 | 24 MiB |
| `relay` | 1/16 | Enough for its two admitted operations |
| `state-read` | Resolved relay capacity | Enough for its two admitted operations |
| `state-read-cache` | Resolved relay capacity | Enough for its two admitted operations |
| `bulk` | 1/32 | 32 MiB |
| `bulk-window` | 1/64 | 16 MiB |
| `restore-metadata` | 1/8 | Positive |
| `credentials` | 1/256 | 19 MiB |

Derived pools are capped at the 32-bit byte-permit representation. Explicit values are never
silently reduced. With unknown memory, defaults are respectively 8, 24, 192, 192, 192, 32, 16,
2,048 and 19 MiB. Credentials keeps room for mounted NATS files; PBKDF2's operation charge stays
64 bytes and its creation policy remains the committed `PASSWORD_ITERATIONS` parameter.

`--execution-relay-encoded-bytes` and `--execution-relay-decoded-bytes` default to one sixth of the
relay pool, capped at 32 MiB each; `--execution-relay-scratch-bytes` defaults to one twelfth,
capped at 16 MiB. All are positive. Validation requires two complete operations to fit relay,
active read and cache pools independently. `--execution-resident-replication-batches` defaults
to at most four, retaining command-budget room for an answer. `--execution-bulk-chunk-bytes`
defaults to one 512th of bulk memory, capped at 64 KiB. Existing operation working-set checks
still apply. Peer receive limits derive from these final limits before transport construction.

`--client-producer-node-bytes` and `--client-consumer-node-bytes` each default to 1/32 of node
memory, holding at least the default 32 MiB session window, or 128 MiB with unknown memory.
They reserve node admission independently; committed per-session limits keep their own policy.
The full startup total counts both capacities as well as all nine executor pools. A 1 GiB node
uses 427 MiB of executor ceilings plus 64 MiB of node client credit, 491 MiB in total. These are
ceilings, not eagerly allocated buffers; graph state, engines and thread stacks need additional
headroom. Explicit totals that exceed known memory refuse startup.

The native process uses `--runtime-worker-threads` (default: resolved CPU count, or one),
`--runtime-max-blocking-threads` (at least all executor workers, retaining a default floor of 512
for registered external and standard I/O work), and `--runtime-thread-stack-size` (8 MiB).
Runtime workers cannot exceed the CPU claim. Blocking capacity must hold every executor worker;
the combined thread count and its stack-size product must be addressable. Stack size must be positive, addressable and
no larger than known memory. Keeping the stack and external-work allowance preserves existing
engine and driver requirements; executor CPU admission supplies the compute bound.

With known memory, omitted high and low watermarks default to 7/8 and 3/4 of memory. An explicit
pair keeps its exact values, including deliberately early pause thresholds. A pair must be supplied
together, low below high, and high must fit known memory. Watermarks govern observed process use;
they do not reserve another copy of the admission ceilings.

`DESCRIBE CLUSTER` appends the answering node's local workers, queues, memory capacities, client
credit and operation limits. Its per-node capability records continue to report committed CPU,
memory and peer receive bounds. Executor snapshots and observability metrics expose the actual
worker, queue and memory capacities; see [Metrics And Observability](metrics-and-observability.md#interconnection-metrics).

## Prerequisites At Join

A node publishes its record in its gossip state under the key `prerequisites`, and carries it in
the hello of every interconnect connection it dials. The node that accepts a connection judges the
hello's record against its own before it binds the connection, and answers with its own record or
with a refusal that names the first prerequisite they disagree on, in the order of the table above,
and both values. The dialing node judges an accepted answer's record the same way. Neither side
reads consensus, so the check holds for a node that has never reached the cluster. A certificate
that names another cluster is refused as a cluster-identity mismatch before any hello is read,
unless it answers at an endpoint recorded for a member it does not name, as described below.

A refusal affects only the node that is joining. A node that starts with peers to reach, a
bootstrap host or the other members its stored membership names, joins through the first
connection it dials that completes; when a connection it dials is refused before any has
completed, it stops instead. A node leaves that joining state once, and a connection another node
dials never moves it, so an incompatible newcomer never stops a node that has joined. A node that
bootstraps a cluster alone joins nothing.

An endpoint the stored membership recorded for a member can outlive the member: its address may
now serve a node of another cluster that trusts the same authority. A certificate there that names
another cluster and a node other than the recorded member is a stale route, not a refusal: the dial
fails as `TransportError::StaleRoute`, naming the endpoint and the member, refuses no one and leaves
the node joining, and the node joins through another recorded member or its bootstrap host. A
certificate that names the recorded member in another cluster is a cluster-identity refusal, and so
is a certificate of another cluster at a bootstrap host.

The member a joining node happens to reach first does not decide whether the cluster can run
beside it: timing compatibility is not transitive, so a node can run beside the member it joined
through and not beside another voter. The leader decides, on every membership reconciliation. It
judges the runs of the live voters that published a record it reads in the order the runs started:
a run is accepted when it can run beside every run accepted before it, and refused naming the first
of those, in name order, that it cannot. The run that started last is the one that changed,
so a member that returns with a record an older voter cannot run beside is refused, and the voters
that kept running are not. Refusing a voter's run changes no membership. The leader judges a live
node outside the membership against the accepted voters' records: it admits one that can run beside
every one of them, refuses one that cannot, naming the first such voter, and leaves one that has
published no record it can read outside the membership.

The leader publishes its verdicts in its gossip state under the key `admission_verdicts`, with the
Raft term it leads. A node that finds another node leading withdraws the verdicts it published, and
a reader that sees the verdicts of two leaders follows the newer term. A joining node reads the
verdict on its own run, its name and incarnation, and stops on a refusal, which names the voter and
the prerequisite with both values seen from the joining node. A member its stored membership names
returns without changing the voters or the quorum, so its join ends once the leader has accepted
its run and a peer's clock has answered within tolerance. Any other node's join ends once the
membership it has applied admits it, because from then on the cluster may count it among its
voters. Until then it also stops as soon as a live member of that membership publishes a record it
cannot run beside. A live node outside that membership, such as another joining node or one that
was itself refused and has not left gossip yet, never stops it.

Once its first connection completes, a joining node asks the peer it joined through for its
physical clock, then every other live peer it has completed a handshake with. It asks each peer
once, and asks a peer that did not answer again a gossip interval later. The peer read its clock
between the probe's departure and its answer's arrival, so one probe proves skew only when the
reading falls outside that window, and the skew it proves is the distance to the nearer end. A peer
whose clock reads before the Unix epoch answers so, and is behind by more than all the time since
it. A node whose proven skew to one of those peers exceeds its `--clock-skew-tolerance` stops, but
only while it is still joining: a skew that an answer proves once the membership has admitted a
newcomer is logged at `warn`, and the node stays.

A refused node logs `this node was refused at join and stops` at `error`, requests its own stop,
runs the ordinary shutdown phases, and exits with status `1`. Its report, beneath
`this node was refused at join`, names the peer, the prerequisite and both values, or the clock it
judged:

```text
node 'node-1' cannot run beside this node: raft-heartbeat-interval is 1s on this node and 250ms on the peer, not within a factor of 2
the physical clock of node 'node-1' is beyond this node's clock-skew-tolerance of 500ms: the peer is at least 1.2s ahead
```

The trust root follows the credentials a node holds. A credential reload that changes the trust
anchors republishes the record, and the node's later handshakes carry the new fingerprint. Two
nodes that hold different anchors cannot open a connection between them: during a rotation that
has reached some nodes and not others, the connections a reload replaces are refused until both
ends hold the same anchors. A node that has joined does not stop for it.

`DESCRIBE CLUSTER` lists every live node's published record and marks each timing that differs
from the leader's, and `nervix_cluster_timing_drift_nodes` counts the nodes whose timings differ.
Both sides of a refused connection log the refusal at `warn`. The leader logs a run it refuses once
while the refusal holds, at `warn` with `refused a node's run that cannot run beside a voter`, and
a node it leaves outside the membership at `debug` while that node has published no record the
leader can read. A returning member whose run the leader accepted logs
`the leader accepted this node's return beside every live voter` at `info`. An outbound pool
connection that ends in a refusal is counted in `nervix_interconnect_connection_failures_total`
with reason `prerequisite`. [Cluster Interconnect](interconnect.md#the-join-contract) owns the
handshake and its wire forms.

## Entity Label Limits

Domain Models share four committed metadata limits: `ENTITY_LABELS_MAX_COUNT` (64),
`ENTITY_LABELS_MAX_KEY_BYTES` (128 B), `ENTITY_LABELS_MAX_VALUE_BYTES` (1024 B), and
`ENTITY_LABELS_MAX_BYTES` (16384 B). Their declarations also supply the first-bootstrap options
`--entity-labels-max-count`, `--entity-labels-max-key-bytes`, `--entity-labels-max-value-bytes`, and
`--entity-labels-max-bytes`, with matching `NERVIX_ENTITY_LABELS_MAX_*` environment variables.
Each accepts a nonnegative `u64` value; the byte options accept the normal byte units.
`ALTER CLUSTER SET` validates each complete committed label map against the complete proposed
policy. A reduction that would invalidate a map has no effects. Existing outer command and
archive limits continue to bound allocation. See [Entity Labels](entity-labels.md).

## Typed Definitions And Defaults

An area group's fields use `Setting<D>`. Its `SettingDefinition` must choose exactly one sealed
`Class`: `Prerequisites`, `ClusterParameters` or `NodeCapabilities`. `Value` carries the semantic
type, `NAME` identifies the setting, `DERIVATION` describes a dependent default, and `Context`
supplies typed derivation and relationship inputs. The definition owns `default_value(context)`
and `validate(value, context)`.

`Setting::new` validates an explicit value. `Setting::resolve` chooses an explicit value or
the owner's default, then runs the same validation. Invalid defaults return the owner's report
too. Value access is shared or consumes the setting; replacing a value crosses validation again.
The compiling API example and paired `compile_fail` doctest prove a definition without its
class is incomplete. The class markers classify fields; they are not global configuration bags.

Write the default once in the definition. An edge's `default_value_t` obtains that value from
the definition. A machine-dependent or setting-dependent argument remains `Option` until its
owner resolves it with context. Absence means derivation; zero, empty strings and numeric
extrema never encode an omitted argument. `clap` stays at the edge, which converts arguments
once to validated settings before I/O. The vocabulary does not parse command-line arguments.

Dependent values are named methods of their owners, never flags. Validate relationships on
the operator's actual values, including budget totals and deadline ordering; retain assertions
on defaults too. Typed reports retain the definition owner's cause without flattening it.

## The Server's Settings

`nervix-server` reads its command line as one options group per area, each in its own file at the
server's command-line edge and printed under its own heading by `--help`. One group holds the
node's identity, listen and advertised addresses, TLS files, name resolution files and storage
paths. Those are what a node is provisioned with, not settings. Every other group is the
command-line form of one owner's class-typed group: each option is named by its definition's
`NAME`, has the environment variable `NERVIX_` followed by that name with underscores, and shows
the default its definition returns. Cluster-parameter help identifies it as a bootstrap default. Durations and byte quantities print in the edge's forms, such as `250ms`,
`15m` and `64MiB`, which read back to exactly the same value.

The session listener's `--grpc-tls-cert` and `--grpc-tls-key` paths belong to that provisioning
group, with `NERVIX_GRPC_TLS_CERT` and `NERVIX_GRPC_TLS_KEY` as their environment variables. They
have no default and are both required in HTTPS mode; HTTP mode refuses them. Resolution produces
an HTTP transport or an HTTPS transport carrying both paths. Material is validated and loaded
once before startup opens listeners; changing it requires a node restart. Console TLS paths have
the same startup lifetime. TLS algorithm policy remains separate from these node-specific files.

The edge converts each group once, and the application carries the result: a `NodeIdentity` and a
`NodeSettings` holding every group. The shutdown coordinator is derived from the shutdown timeout
the settings hold, so the two cannot disagree, and the runtime is built from its `RuntimeSettings`
and a named environment of the services it uses rather than from positional values. The
[nervix-server Reference](nervix-server-reference.md) is the help output of the release this book
describes, defaults included.

| Option | Class | Owner group |
| --- | --- | --- |
| `--raft-heartbeat-interval` | prerequisite | Raft timings |
| `--gossip-interval`, `--clock-skew-tolerance` | prerequisite | membership |
| `--raft-snapshot-entry-threshold`, `--raft-snapshot-byte-threshold`, `--raft-covered-log-entries-retained`, `--raft-covered-log-bytes-retained`, `--raft-retained-log-cap` | node capability | Raft retention |
| `--node-unavailability-timeout` | cluster parameter | membership |
| `--replica-count` | cluster parameter | scheduling |
| `--tls-cipher-suites`, `--tls-key-exchange-groups` | cluster parameter | outward TLS algorithm policy |
| `--password-iterations` | cluster parameter | password creation policy |
| `--state-snapshot-interval` | cluster parameter | runtime |
| `--domain-drain-timeout`, `--entity-gate-deadline` | cluster parameter | cluster policy |
| `--max-batch-messages`, `--client-producer-session-bytes`, `--client-consumer-session-bytes`, `--udf-code-bytes`, `--http-header-bytes`, `--http-header-fields`, `--client-batch-rows` | cluster parameter | model and session policy |
| `--restore-staging-max-bytes` | node capability | runtime |
| `--transaction-idle-timeout`, `--transaction-tombstone-retention`, `--transaction-max-statements`, `--transaction-max-source-bytes`, `--transaction-max-open` | cluster parameter | transaction limits |
| `--command-retry-validity`, `--command-execution-capacity` | cluster parameter | command execution |
| `--memory-high-watermark`, `--memory-low-watermark`, `--memory-pressure-check-interval`, `--memory-pressure-resume-jitter` | node capability | memory pressure |
| `--drain-timeout`, `--shutdown-timeout` | node capability | shutdown |
| `--resource-max-archive-bytes`, `--resource-max-extracted-bytes`, `--resource-max-file-count` | cluster parameter | resources |
| `--otel-trace-sample-ratio` | node capability | trace export |
| `--cpu-count`, `--memory-limit` | node capability | capacity |
| `--execution-*`, `--runtime-worker-threads`, `--runtime-max-blocking-threads`, `--runtime-thread-stack-size` | node capability | execution |
| `--client-producer-node-bytes`, `--client-consumer-node-bytes` | node capability | client admission |

Cluster-parameter options are optional seeds for the first bootstrap. An explicit command-line
or environment value, including a value equal to the default, is refused on every later start.
The help text names the definition's default without supplying an implicit seed option. With known memory, omitted memory watermarks derive as described below. With unknown memory and
no explicit pair, the node runs no memory-pressure supervisor. `--memory-limit` and
`--restore-staging-max-bytes` have no default value either: without them the node detects its
memory limit and derives its restore-staging quota from its database volume, as
[The Startup Preflight](#the-startup-preflight) describes.

### Engine And Connector Host Capabilities

These values are local `NodeCapabilities`. Changing them requires restarting that node; they do
not alter a graph's stored execution contract. Each flag has an environment twin formed as
`NERVIX_` plus its uppercase name with underscores.

| Server option | Default | Owner and bound |
| --- | --- | --- |
| `--vm-inline-row-limit` | 1024 rows | VM admission; zero sends every nonempty batch through the executor |
| `--vm-regex-search-cache-bytes` | 2 MiB | positive, addressable lazy-DFA cache per compiled regex |
| `--vm-dynamic-pattern-cache-capacity` | 64 patterns | positive, per call site and compiled program |
| `--vm-pattern-sets-per-batch` | 64 sets | positive, per dynamic `contains_any` call and batch |
| `--wasm-stack-bytes` | 512 KiB | positive, addressable stack per WASM invocation |
| `--wasm-memory-reservation-bytes` | 4 GiB on 64-bit hosts; 10 MiB on 32-bit hosts | virtual address reservation per guest memory; zero permits relocatable growth |
| `--connector-retry-multiplier` | 2 | positive multiplier applied between reconnect attempts |
| `--connector-retry-jitter-percent` | 0 | 0 through 100 percent added to the next multiplied delay, within the declared cap |
| `--kafka-domain-assignment-retry` | 100 ms | positive retry cadence while a domain-offset assignment is unavailable |
| `--kafka-partition-watch-interval` | 1 s | positive topology inspection cadence |
| `--syslog-intake-queue-capacity` | 64 frames | positive, within the native channel permit bound; stream intake backpressures when full |
| `--syslog-read-chunk-bytes` | 8 KiB | positive, addressable bytes per stream read, further limited by the frame bound |

The two Kafka durations accept at most `u32::MAX` whole seconds. VM programs carry the compiling
node's resolved settings into prepared regex calls and execution. Extension calls still use
Extension workers regardless of the inline threshold. Deterministic row-defect limits, including
compiled regex size and `contains_any` input limits, remain fixed across nodes.

Engine and connector host values are part of the incarnation-bound Raft capability claim and
appear in the resolved startup log and `DESCRIBE CLUSTER`. The current record uses one consensus
storage identity and wire fingerprint. A database in another stored shape fails to open and must
be recreated for this alpha build. Nodes speak the same current shape; their local capability
values may differ during a rolling restart.

WASM parallel compilation is explicitly disabled: independent module compilations already run
on bounded Bulk workers, so nested compilation fan-out cannot escape that admission boundary.
Stack and address reservation do not replace a processor's declared linear-memory or fuel limits.

The connector retry law preserves the initial delay and ceiling of `RETRY POLICY`; it changes
only later delays. Kafka, MQTT and the shared ingestor/emitter host receive the same law.
External broker timeouts, connection pools, keep-alive and request behavior remain declared client
`CONFIG` keys or entity clauses. Read the owning connector section in
[Ingestors](ingestors.md) or [Emitters](emitters.md) before choosing those values.

### Validation Before Any I/O

The edge validates every value, and every relationship between values, before the node opens a
store, binds a listener or starts a task:

- The Raft heartbeat interval is at least one millisecond, the resolution consensus runs timings
  at, and twelve of it, the longest election timeout, fit the milliseconds consensus counts it in.
- The gossip interval and the clock-skew tolerance are each at least one millisecond, and twenty
  gossip intervals fit a duration.
- A memory limit, when one is given, is above zero.
- The drain timeout does not exceed the shutdown timeout, which bounds every drain step. A drain
  timeout equal to it lets the drain use the whole deadline; the defaults leave time for terminal
  teardown after the drain.
- The memory watermarks are set together or not at all, the low one below the high one, and with
  them the check interval is above zero.
- The trace sample ratio lies between 0 and 1.

A value its option cannot read, such as malformed duration text or a sample ratio outside its
range, ends the process with status `2` as the command line reports it. A relationship that does
not hold, or a value outside its own bounds, ends the process with status `1` before it opens a
store, with a message that names the settings involved and their values, such as
`drain-timeout (60s) must not exceed shutdown-timeout (50s)` or
`raft-heartbeat-interval (500µs) must be at least 1ms, the resolution consensus runs timings at`.
The defaults satisfy every relationship, and the assertions that keep them doing so remain beside
them.

Rendered duration options preserve every nanosecond. Compact values use the largest exact unit,
such as `250ms` or `15m`; a large fractional value uses whole seconds and remaining nanoseconds,
such as `169112806400s 601538000ns`. These values read back unchanged when settings are reapplied
or another option is overridden.

## Rolling Changes

A cluster remains correct while two setting values coexist during a rolling restart in any
order. The change takes full effect after the last node adopts it; rollback follows the same
procedure in reverse. A prerequisite can reject a node outside its supported bounds: a timing
moves by less than a factor of two in one roll, and a restarted node whose timing is outside that
bound of a live peer's is refused as it joins, alone. Identity, trust, the wire contract, the
floors and the durability promise cannot be rolled, because two values of one never run together.
A capability is a local claim; a cluster parameter retains one committed authority throughout.

The alpha rule still requires one current data or message shape. Operational values coexist
within that shape. A roll introduces no earlier schema, dual decoder or migration path.

### Committed Parameter Changes

Cluster parameters have one durable authority. The first node that creates the cluster seeds its
parameter record from the bootstrap options. After that boundary, options for those parameters
are refused at startup, including on a returning bootstrap node. Change the record through
Plexus, the Nervix graph definition language:

```plexus
ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;
```

Duration assignments accept [multi-span duration literals](domains-and-time.md#duration-literals),
including the text canonical Plexus renders:

```plexus
ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s;
```

The statement runs outside domain transactions and needs no selected domain. Its assignments
form one transition: either every assignment is committed or none is. A statement assigns each
parameter at most once, so completion offers only the parameters it has not assigned yet, and a
parameter written a second time is refused at the name that repeats it, naming the parameter.
An invalid value, or a cap that an existing model exceeds, refuses the transition with the
parameter and owning model identified. Timings that drive repeated work or retained validity
must be greater than zero and no longer than `u32::MAX` whole seconds, with nanosecond precision
within that range. Counts used by a wire or native count fit its current representation. HTTP
application headers default to 128 distinct fields; `HTTP_HEADER_FIELDS` changes that count up to
16,384. This limit leaves 8,192 distinct-field slots in the native request map for client and
transport headers. A prerequisite cannot be assigned through this statement.

Validation and publication share a commit fence. A concurrent model change or parameter change
invalidates a captured transition before it applies, and validation runs again against the
current state. Model admission carries its captured parameter record through validation and
publication; application preflight and replicated apply compare its complete values and revision.
An identified transaction preview also includes that record in its planning basis. Any parameter
publication invalidates an earlier preview, including when its queued Models still fit the limits
or later assignments return the same values. A stale expected preview leaves the transaction open
with no effects until the caller inspects it again and decides whether to commit.
Restored model batches capture their Models, resource versions and parameter record together at
the model-planning step and carry that view through content preparation and publication. A valid
archive can still have its model step refused by a limit committed since archive preparation.
Known HTTP header names contribute their count and byte requirement even when their values depend
on input. Dynamic names may replace earlier values, so transition validation retains only guaranteed
byte contributions; request preparation checks each complete evaluated envelope against its captured
bounds.

HTTP polling headers use these same committed caps. Admission and parameter transitions count
every declared name and the complete literal or non-templated client-secret value. The header
declaration refuses repeated names ignoring case. A templated secret contributes its known name
at admission; the host captures the current caps when it composes the connector, which checks the
complete resolved values before opening a request. A raised cap governs new requests and a lower
cap cannot invalidate a polling request's known requirements. Syntax validation checks physical
representation bounds separately from committed policy, and secret values remain redacted.
Sizing keeps unresolved client or secret bindings available for the graph validator to resolve
when a transaction candidate becomes complete; it preserves their known name requirements.

WebSocket connection ingestors use the same committed caps for their literal upgrade headers.
The subprotocol offer counts as one field, with its canonical comma-joined tokens contributing
to the byte count. Model admission and parameter transitions both check these requirements;
raising the caps permits new declarations above the defaults. Syntax and plan conversion retain
the physical HTTP representation bounds independently of cluster policy.

Each live node installs the committed record before reporting that revision ready. Runtime timing
and resource-store owners publish a new immutable policy over the same storage. Snapshot tasks
observe interval changes at their existing bounded replication-poll boundary; a changed interval
starts a fresh period, while an unrelated parameter does not postpone a checkpoint. Client
attachments retain their granted byte and row limits until they end; new attachments and HTTP
publication preparation capture the current limits. The change does not discard payloads, acknowledgement
ownership, or stored state. A successful response means the live participants have installed the
change. An uncertain response retains its execution reference and resumes the same recorded
transition, even when a later command has changed another parameter.

`DESCRIBE CLUSTER` reports committed values and each node's running revision and values. A node
that has not applied the current record remains visibly behind. Sessions receive an informational
`ClusterParameters` notice after their node successfully installs a parameter revision. The installation supervisor
retries a failed graph installation; a retry of the same revision sends no additional parameter
notice. A joining or returning node obtains the
live cluster's published record before activating services that consume it and verifies that
record against consensus as it catches up.
Public session and observability listeners start before live-policy discovery. `SHOW CLUSTER
STATUS` and `DESCRIBE CLUSTER` remain available while an isolated returning node waits; status
reports `node.initialization=initializing` until initialization completes, then `ready`. Liveness
and metrics remain available, while `/readyz` returns 503 until initialization finishes and a
leader is known. Policy-dependent commands, session attachments, resource transfers and configured
HTTP intake refuse work until the live publication is known, the local committed record has caught
up, and initialization has registered its owners. No stale local record supplies startup policy. A durable command retains its execution reference and receives a nonterminal `OutcomeUnknown(NotYetAuthoritative)` during this wait, so reconnecting cannot turn an earlier committed result into a failure.
Discovery waits for a live publication or the first stop request, including a typed join refusal;
shutdown cancels the wait and retains that refusal through cleanup. During a whole-cluster restart,
peers start concurrently: each returning node needs another process's live publication before it
can finish startup.

Only the first bootstrapping node supplies parameter seed flags; another node given them
refuses startup. Changes validate the transition at commit: existing Models must fit a lowered
cap, and prerequisite relationships must still hold. Apply committed changes live. Rebuild an
owner that captures settings at construction over its existing storage. The current record changes
no OpenRaft construction setting: heartbeat and election timing remain prerequisites. Committed
and running revisions are published together, and delayed observations cannot move them backward.


Outward TLS algorithms are ordered-list cluster parameters. Both bootstrap defaults are `fips`:
`--tls-cipher-suites` (`NERVIX_TLS_CIPHER_SUITES`) and `--tls-key-exchange-groups`
(`NERVIX_TLS_KEY_EXCHANGE_GROUPS`) take comma-separated exact names or aliases. After bootstrap use
`ALTER CLUSTER SET TLS_CIPHER_SUITES = ('fips'), TLS_KEY_EXCHANGE_GROUPS = ('fips');`.
Aliases resolve in place and duplicate algorithms keep their first position. The complete resolved
pair validates at commit and is installed live for new handshakes; established connections keep
their negotiation. `DESCRIBE CLUSTER` and the settings log show resolved committed/running lists.
The [cryptography chapter](cryptography.md#outward-tls-policy) owns membership, exact spellings,
validation and the inventory of TLS stacks the policy governs. The interconnect's algorithms are a
[fixed contract](cryptography.md#interconnect-tls) that no setting reaches, because nothing a node
needs to reach the cluster depends on a cluster parameter. Protocol versions and signature schemes
have no additional cluster parameter.

## Fixed Values And The Source Ratchet

A wire format, RFC, persisted layout, ABI or invariant can fix a value. Its exact constant
declares the reason, gated by the analysis configuration:

```rust,ignore
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI stores its tag in four bytes"))]
const TAG_BYTES: usize = 4;
```

The compiler accepts this contract on a constant, including an associated constant, with the
`fixed` kind and one nonempty reason. Duplicate, malformed and misplaced contracts fail. The
source scanner validates the gate before excluding a constant, including inactive code. The
reason remains reviewed evidence; neither parser nor compiler proves its prose true.

The one-minute and fifteen-minute metric EMA windows, their decay conversion and the published
connector-interaction duration buckets are fixed observation contracts. Their declarations carry
the fixed reasons in the metric owners; they are not node settings. The current resolution is
documented in [Metrics And Observability](metrics-and-observability.md#connector-interactions).

`just ratchet --show hidden_tunables` lists the temporary count's sites. It counts numeric
`const` items of types `Duration`, `usize`, `u64`, `u32`, `u16`, `u8`, `i64`, `i32`, `f64`,
`NonZero*` and `ByteUnit`, plus numeric literal policy calls for duration construction, channel
and queue capacities, `with_capacity` and semaphore permits. A numeric constant counts once,
including its initializer. Class group defaults, definition defaults and fixed constants are
excluded. Tests, benchmarks, examples, tooling and the arbitrary, benchmark, model-harness and
test-environment crates are outside this count.

The baseline only decreases. The syntax count sees inactive source and authored macro
fragments; it does not resolve custom types, infer business meaning, expand external procedural
macros or discover implicit library defaults.
[Architecture Rules And The Debt Ratchet](architecture-enforcement.md#hidden-tunables) owns
its exact recognition and qualification limits. The settings compiler rule replaces this
detector and its numeric entry together at cutover.

## Names And Observability

Flags use `--<area>-<name>` and environment variables use `NERVIX_<AREA>_<NAME>`. Durations and
sizes use the existing human-readable forms. The [nervix-server Reference](nervix-server-reference.md)
is the help output, whose names and defaults come from the declarations.

Startup reports resolved settings with credentials and sensitive material redacted: the
`starting nervix server` line at `info` names the Raft heartbeat and the election timeouts derived
from it, the gossip interval, the clock-skew tolerance and the memory limit with where it came
from. Gossip exposes per-node prerequisites and the running committed revision. `DESCRIBE CLUSTER`
shows each live node's prerequisites and the timings that differ from the leader's, and
`nervix_cluster_timing_drift_nodes` counts those nodes. Committed parameters and capability
records, once a node announces them, are reported beside the running values they govern, with the
effective negotiated bounds. Refusals identify the setting and failed relationship without
disclosing secrets. Connector-specific values remain in their owning manual and
[Connector Contract](connector-contract.md).

## Verification Boundary

The executor's connector-storage capacity is `Setting<ConnectorStorageWorkers>` with class
`NodeCapabilities` inside `WorkerCounts`. Its one default is two workers. The semantic value is
nonzero, and both an explicit local claim and the default are checked against the primitive's
maximum permit representation before pool construction. Startup resolves it through the local
executor configuration, and the executor snapshot and worker metrics expose the running value.
The execution settings resolve it with the detected node capacities before construction.
Filesystem and consensus retain independent admission while a connector call holds its worker.

The interconnect's bulk receive window is `Setting<BulkReceiveWindow>` with class
`NodeCapabilities` inside `PeerReceiveSettings`, resolved against the supplied executor before
transport construction. Its one default is 1 MiB, the HTTP/2 window each
stream of a bulk connection grants its sender. `--interconnect-bulk-receive-window` overrides it.
The value must be
nonzero, and the connection window derived from it, one window for each of the four bulk streams,
must fit the 2^31-1 bytes HTTP/2 can advertise; the definition checks both. A transport refuses to
bind when the executor's `bulk_window` memory budget cannot back one window. The fixed constant
for the HTTP/2 maximum names RFC 9113.

A Kafka consumer's single control-operation turn is an ordering invariant rather than an
operator capacity. Its fixed constant states that a cancelled native mutation cannot be
overtaken by the next operation. The ingestor control-call budget is the external client's
`socket.timeout.ms`; [Connector Contract](connector-contract.md) owns that per-system value.

Sentry's 1,000,000-byte event limit is a fixed external ingestion contract. The connector
vocabulary declares its one constant; [Sentry's event contract](emitters.md#sentry) defines which bytes
count toward the limit.

`Setting<D>` copies or compares a resolved value when its semantic value supports those
operations, without imposing value traits on the definition marker. Generic debug formatting
identifies the declaration; the owning settings renderer controls value exposure and redaction.

The declaration API has value, default and relationship tests plus paired Rustdoc examples.
The source count has tracked and new-source gate fixtures, pinned exclusions, inactive and
macro syntax, compiler contract fixtures and the registered bounded `typed-hidden-tunables`
Bolero property. The declarations alone add no Cucumber scenario and change no lock or lifecycle
path. Public behavior introduced by an area owner requires its one-node and three-node scenarios
and the concurrency evidence its path needs.

The server's options are covered at their edge and at each owner. Unit tests render durations and
byte quantities, check that every settings option is named by its definition and read from its
environment twin, that help lists each area under its own heading, that an empty command line runs
every definition's default, and that each relationship refuses the node with its owner's typed
error. The registered `node-settings-command-line` Bolero property generates valid settings for
every group, scalar and pre-detection relationships held, and optional capability requests, and
requires them to read back unchanged without losing omission after rendering as
options, along with every duration and byte quantity. Public scenarios start a real
`nervix-server` process with a drain timeout above its shutdown timeout or a Raft heartbeat below
one millisecond and observe the refusal naming the settings, and restart a node of a one-node and a
three-node cluster with such a setting, through the harness step that names a setting for one
node, before restarting it with a valid one. Converting and validating the options takes no lock
and adds no lifecycle path, so the deadlock detector has nothing of it to observe.

The relay-capability scenarios preserve both concrete branches, exactly-once output and confirmed
acknowledgements across raised and lowered receive bounds. The oversized-row case observes both
branch refusals before submitting its fitting batch. A producer's first negative acknowledgement
does not establish that every branch buffer has flushed. Controlled examples stagger the branch
buffers' deadlines to exercise that distinction, while keeping the same subsequent delivery and
producer-completion assertions.

The prerequisite record, its compatibility rule, its gossip text and the order `DESCRIBE CLUSTER`
lists it in have unit tests beside the record, and the record's gossip text has a registered Bolero
property that requires it to read back unchanged. A second, `cluster-peer-clock-probes`, requires
the clock probe and every answer to it to read back unchanged through the archive. Public scenarios
on one-node and three-node clusters refuse a joining node whose cluster identity or trust root
differs, or whose Raft heartbeat is outside the bound, and require the refusal to name the
prerequisite and both values; refuse a node that joins through a member it can run beside but not
beside another voter, naming that voter; admit a node whose heartbeat differs within the bound and
show its drift through `DESCRIBE CLUSTER`; and refuse to start a node whose memory limit cannot hold
its budgets. A three-node scenario refuses a returning member that can run beside the voters it
recorded but not beside one admitted while it was away, naming that voter: admitting a voter while
a member is away needs a quorum without it, which fewer voters do not give. Unit tests cover the
leader's judgment of voter runs and candidates, a joining node's handling of its verdict, and the
verdicts' gossip text, whose registered `models-admission-verdicts` Bolero property requires it to
read back unchanged. Interconnect unit tests and the `stale recorded endpoint` case of the
[deterministic interconnect simulation](interconnect-simulation.md) cover the stale route.

Resource-volume checks use the chosen seed before the first commit, and the live publication for
a joining or returning node. Local default resource limits do not refuse a node before it can
learn that policy. The local preflight still measures and validates the restore-staging capability
before opening the database or binding listeners.

### Password Creation Policy

`ALTER CLUSTER SET PASSWORD_ITERATIONS = 210000;` changes the cost of newly created user
credentials. The setting defaults to 210,000 and accepts positive counts through 1,000,000.
`--password-iterations` (`NERVIX_PASSWORD_ITERATIONS`) seeds it only at first bootstrap.
The value is cluster policy, because every node must honor the same creation promise.
Existing users and admitted CREATE USER command executions retain their exact credential.
Verification uses a persisted common work count that never falls below any cost the cluster has
admitted, preserving equal work for known and unknown names across changes, restart and restore.
[Cryptography](cryptography.md#password-credentials) owns the credential and workload contract.

## State Inspection Capabilities

The runtime declares three local `NodeCapabilities` through `SettingDefinition`:

| Setting / CLI flag | Environment | Default | Validation |
| --- | --- | --- | --- |
| `inspection-max-rows` | `NERVIX_INSPECTION_MAX_ROWS` | 1024 | Nonzero, at most the 32-bit collection bound |
| `inspection-max-sections` | `NERVIX_INSPECTION_MAX_SECTIONS` | 128 | Nonzero, at most the 32-bit collection bound |
| `inspection-read-timeout` | `NERVIX_INSPECTION_READ_TIMEOUT` | 10s | Positive duration, at most an unsigned 32-bit seconds value |

Defaults and validation belong only to their definitions. CLI values resolve before runtime
construction and round-trip through the runtime argument projection. Each serving node enforces
its bounds; the coordinator also checks session collection and complete reply limits. The deadline
bounds the complete operator read and governs remote admission/setup as well as owner work.
The byte-stream request obtains its fallback read deadline through the same definition's
`default_value`; no independent transport timeout duplicates that policy.
These capabilities do not change materialized row delivery, replication or stored snapshot bounds.

The committed parameter carrier stores duration settings as exact `u32` seconds and `u32`
nanoseconds. Their declarations retain the positive bound of `u32::MAX` whole seconds, and their
public values, assignments and bootstrap seeds remain `Duration`. This lossless private layout
keeps the inline consensus record bounded as the policy grows; it adds no per-record allocation.
JSON retains the standard duration projection, and the current archived value validates the
nanosecond coordinate before constructing a setting.
