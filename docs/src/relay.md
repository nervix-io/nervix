# Streams And State

`RELAY` declares a named, schedulable runtime node between producers and consumers. Every relay
has one primary owner, and a relay whose branch declares several partitions has one owner for each
partition. Only an owner instantiates the relay buffer, concrete branch presence, relay metrics,
subscription fan-out, and optional materialized state, each for the keys it owns.

```plexus
CREATE IF NOT EXISTS RELAY notifications
  SCHEMA notification
  UNBRANCHED
  CAPACITY 1;
```

## Streams, Branch Keys, And Branches

External feeds commonly contain records for many tenants, users, devices, accounts, or other business groups. Nervix branching lets one declared graph process those groups independently.

Branching is defined by a schema name on a named branch:

- `CREATE BRANCH by_user SCHEMA user_branch TTL 5m` isolates each user
- `CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m` isolates each tenant
- `CREATE BRANCH by_tenant_user SCHEMA tenant_user_branch TTL 5m` isolates each tenant/user pair
- `PARTITIONS <n>` divides the branch's concrete keys into partitions that each have their own
  relay owners; see [Relay Partitions](#relay-partitions)
- `MAX INSTANCES <n> EVICT LRU` can cap active concrete branch instances for that branch

The web console's **Create** menu also has a branch form. Its schema picker searches internal
schemas in the captured domain, including schemas staged earlier in the attached transaction; the
form requires a TTL, starts its partition count at 1, and accepts the optional positive LRU
instance limit. Registry validation remains authoritative for whether the selected schema can
serve as a branch key. `SHOW CREATE BRANCH <name>` returns the stored branch's canonical
declaration.

The same menu has a relay form. It selects the relay's schema and branch from server-backed lists
of the captured domain, including models staged earlier in the attached transaction, and requires
an explicit **UNBRANCHED** or **BRANCHED BY** choice. Capacity starts from the default relay buffer,
and materialized state is none or `LAST BY TIMESTAMP`. `SHOW CREATE RELAY <name>` returns the
stored relay's canonical declaration, which always states its capacity. See
[Web Console](client-tools-web-console.md).

Relays select an explicit branch or declare unbranched execution:

```plexus
CREATE RELAY notifications SCHEMA notification BRANCHED BY by_tenant_user;
CREATE RELAY global_notifications SCHEMA notification UNBRANCHED;
```

## Relay Partitions

A branch declares how many partitions its concrete keys are divided into:

```plexus
CREATE BRANCH by_tenant SCHEMA tenant_branch TTL 5m PARTITIONS 4;
```

The count is a whole number from 1 through 65535. A declaration that writes no count has one
partition, and `SHOW CREATE BRANCH` omits `PARTITIONS 1`. `PARTITIONS` follows `TTL` and precedes
`MAX INSTANCES`.

A concrete key lands in partition `seed mod n`. Its seed is the first eight bytes, read
little-endian, of the key's fingerprint, the same fingerprint that names the branch in metrics and
diagnostics. The seed depends on the key's fields alone, never on the branch, relay or domain the key
appears in, so every node places a key in the same partition of every relay its branch branches, and
it stays there for as long as the count stays the same.

Every relay branched by a branch of several partitions is scheduled as one unit per partition,
partition `0` through `n - 1`. Each partition has its own owner, its own state replicas and its own
failover. The scheduler spreads a relay's partitions over the eligible nodes, least loaded first and
then round-robin, so four partitions on three nodes span at least two of them. Placement rules move a
partitioned relay as a whole only through `REQUIRE COLOCATION`, which keeps every partition with its
placement group; preferences and soft separation leave its partitions where the scheduler spread
them. See [Placement](placement.md).

Each partition owner holds:

- its own buffer of `CAPACITY` batches and its own dispatch gate
- the presence of the keys of its partition, their TTL and their `MAX INSTANCES ... EVICT LRU` limit
- the materialized rows of its keys, when the relay is materialized
- its own relay metrics, labelled with the partition

A producer resolves its key's partition once, when it binds a concrete branch, and sends that
branch's batches to the owner of that partition. An owner accepts a batch from another node only for
a partition it owns, and only when the key it decodes lands in that partition. Each partition owner
fans its batches out to every runtime consumer and every session subscription of the relay, so a
consumer and a subscription receive every partition. One key's records pass through one partition
owner, which keeps their order; records of different keys in different partitions are not ordered
with each other. Processors, generators and emitters stay single units that read every partition of
their input relays.

`SHOW CLUSTER STATUS` prints one schedule line per partition of a partitioned relay, naming the
partition after the relay, such as
`- domain=payments kind=relay name=tenant_events unit=partition:2 owner=node-3 replicas=-`.

An unbranched relay and a relay of a branch with one partition have one owner.

`ALTER BRANCH <branch> SET PARTITIONS <n>` changes the count:

```plexus
STOP;
ALTER BRANCH by_tenant SET PARTITIONS 2;
START;
```

The registry accepts it only while the branch's domain is stopped. On a running or paused domain it
is refused before anything is committed:

```text
branch 'by_tenant' changes its partition count only while its domain is stopped: STOP domain 'payments', run ALTER BRANCH, then START it
```

`START` rebuilds every relay of the branch with the new set of partitions. Materialized relay state
belongs to the domain start that began it and `START` clears it, so no row recorded under the
previous count is read under the new one.

## Altering Relays

`ALTER RELAY` accepts one or more comma-separated operations. Operations execute in written order,
so later operations may replace values set earlier in the same statement:

```plexus
ALTER RELAY notifications
  SET CAPACITY 8,
  SET SCHEMA notification_v2,
  SET BRANCHED BY by_tenant,
  SET MATERIALIZED STATE LAST BY TIMESTAMP;
```

The available operations are `SET CAPACITY`, `SET SCHEMA`, `SET BRANCHED BY`, `SET UNBRANCHED`,
`SET MATERIALIZED STATE LAST BY TIMESTAMP`, and `DROP MATERIALIZED STATE`. Dropping materialized
state when it is not configured is an error. The registry validates the complete resulting graph,
so schema or branching changes must be submitted in the same transaction as every dependent model
change needed to make the candidate graph valid.

Capacity changes are dynamically applied to a running domain without replacing the owner buffer
or its buffered batches. Schema and branching changes use domain pause because Arrow schemas and
branch identity are compiled into producers and consumers. Materialized-state add/drop uses entity
pause: Nervix gates and drains the relay, changes the relay's state-replica membership without
replacing its owner buffer, advances the state epoch observed by readers, and releases the gate.
Adding state therefore has no interval in which post-commit records can bypass materialization.
Dropping state purges the relay's in-memory and persisted materialized records before flow resumes.

An ingestor or reingestor uses `BRANCHED BY <branch>` to compute the branch key for each record. When records for a key arrive, Nervix uses a branch instance for that key. A branch instance is the runtime execution path for one concrete key.

Inside a branch, records retain their key while moving through the relay owner. Each concrete relay
branch has:

- the declared `RELAY` name it belongs to
- a branch identity
- a schema
- buffering behavior

Processing node state also belongs to the branch. That gives each group independent deduplicator
history, reorder buffers, window accumulators, and materialized entries. Relay buffering is owned
once per declared relay and may contain interleaved batches from several concrete branches.

Runtime branch rules:

- an `INGESTOR` starts a branch for one concrete branch key through `BRANCHED BY <branch>`
- normal downstream processors keep the same named branch and concrete branch key
- output routes and forwarders send records to downstream relay names inside the same branch
- stateful processors keep branch-local state for that group
- a `REINGESTOR` may consume across a branch boundary and start new downstream branches through `BRANCHED BY <branch>`
- an `EMITTER` consumes records across the whole input relay and terminates the branch at an external emitter

`branch` is a reserved namespace, so a relay cannot be named `branch`. Expressions inside a
concrete branch may read its immutable key with `branch.<key>`. Bare fields never resolve to branch
fields. Unbranched execution has no `branch.<key>` values, and successful emitter expressions do
not expose the branch scope.

## Internal Payload Model

After schema application, Nervix does not keep an internal per-message document format on relays. The runtime payload on a relay is an Apache Arrow record batch plus the schema, the row-aligned ACK state, and each row's low and high ingestion watermarks. The watermarks travel beside the batch in two Arrow buffers of Unix-nanosecond timestamps, which watermark-based logic and [delivery latency](metrics-and-observability.md#delivery-latency) read without converting each row.

Apache Arrow is used here for two practical reasons:

- fast vectorized processing over columnar data inside runtime nodes
- fast serialization and deserialization when batches move between nodes

Operationally that means:

- ingestors and reingestors batch decoded rows before writing into a relay
- deduplicators still apply row-level state semantics inside the node, while junctions stay Arrow-native and concatenate compatible branch-local batches before forwarding
- window processors keep branch-local online aggregate state and construct each route through
  aggregate expressions in ordered `SET` assignments
- batches remain branch-local until a `REINGESTOR` or `EMITTER` boundary changes the routing behavior
- a producer on a nonowner cluster node serializes a batch once and holds one fixed ingress slot
  until the owner admits it into the relay buffer
- the owner serializes an admitted batch at most once for its remote runtime consumers and once for
  remote session subscriptions, and shares each serialized body across the destination nodes; every
  local runtime consumer on a node shares that node's delivery, and a session subscription on a node
  that also hosts a runtime consumer piggybacks on it
- a delivery that reaches a node after its attached consumer moved away fails its attached
  acknowledgements, so the source redelivers the record along the owner's current routes; see
  [Consumers That Leave The Receiver](interconnect.md#consumers-that-leave-the-receiver)
- an owner batch that starts fan-out while a schedule swap fences its consumer routes fails its
  record acknowledgements and is retried by its source after the swap; fan-out already holding a
  dispatch permit finishes before those routes change
- an acknowledgement a receiver returns after the sending node restarted names that node's earlier
  run, so the restarted node rejects it and its own records stay pending until their own
  acknowledgements resolve; see
  [Acknowledgement Registrations](interconnect.md#acknowledgement-registrations)

Lookup and state-replication control paths are separate from this relay payload model. The Arrow batch path applies to relay movement inside the data plane.

Relay batches and their per-row ACK metadata are hot-path runtime data. They are not persisted or
replicated, and ACK guards, tokens, and maps are never part of runtime snapshots. Materialized
state, when enabled, is the only replicated part of the relay; it is not a second runtime node.

## Capacity

`CAPACITY <n>` controls the buffer on the relay owner: the single buffer of a relay with one
owner, and the buffer of each partition owner of a partitioned relay. It is one cluster-wide
backpressure boundary for each partition, not a per-producer, per-consumer, or per-branch capacity.
If downstream runtime consumers cannot drain a partition quickly enough, upstream dispatch to that
partition waits once its owner buffer and the fixed dispatch slots leading to it are occupied, while
the relay's other partitions keep flowing. A producer on another node also
waits while the owner has no relay memory left for the acknowledgement watches of the rows it
sends, until the owner has reported enough earlier rows, or for the grant of its next delivery, and
while its own node has no relay memory left to store the acknowledgements of the rows it sends; see
[Capacity Planning](./capacity-planning.md) and
[Held Deliveries](./interconnect.md#held-deliveries).

The capacity is a positive integer, through `18446744073709551615` on native 64-bit targets.
Creation and `ALTER RELAY ... SET CAPACITY` retain the exact accepted value in storage, replicated
commands, and `SHOW CREATE` after a node or full-cluster restart.

At most the following batches can be admitted or in dispatch for one relay partition, and for each
partition of a partitioned relay:

- `CAPACITY` batches in the owner buffer;
- one batch from each producer cluster node to the owner; and
- one batch from the owner to each remote consuming cluster node.

The owner has no additional inbound queue. A producer's slot is released only after the owner has
admitted that batch. A consumer-node slot is released only after that node has admitted its batch.
These fixed slots do not scale with the number of runtime consumers or subscriptions on a node.

The capacity can be changed after creation:

```plexus
ALTER RELAY notifications SET CAPACITY 5;
```

The updated capacity is persisted in the relay definition and applied in place to the active owner
buffer. Existing concrete branches, subscriptions, runtime consumers, and dispatch slots remain
attached.

A capacity change applies at once to admission and never discards buffered batches. Increasing
capacity admits waiting publishers immediately. When capacity is shrunk below the current buffered
depth, every buffered batch is kept and publishers observe relay backpressure until receivers drain
the buffer below the new capacity. The one-batch producer and consumer dispatch slots never resize.

Small capacities are useful in tests and tiny examples, but high-throughput
graphs should use capacities large enough to absorb several flush intervals of
batches. This is especially important for relays written by external ingestors
and relays read by reingestors, because a low buffer can multiply short waits
across every branch. If omitted, Nervix uses the default relay buffer.

## TTL

TTL is a branch contract, not a relay-local setting. `CREATE BRANCH` declares `TTL <duration>` after `SCHEMA <schema>`. `UNBRANCHED` branch roots do not declare TTL because there are no concrete branch instances to expire.

TTL controls:

- concrete relay-branch presence on the owner
- materialized-state cleanup when the relay is materialized
- downstream processor state cleanup for the same concrete branch

Expiration semantics:

- paced domains use domain logical time
- unpaced domains use wall clock time
- a branch's idle time is measured from the time at which its last input was accepted. A relay
  owner or processor that spends a long time waiting for input does not spend that wait out of the
  branch's TTL, so every branch receives its complete declared lifetime from its own last record
- idle time counts every reason a branch receives no input. When a stalled source or a stalled
  upstream delivery holds a branch's input back for longer than the TTL, every relay owner and
  processor that holds the branch releases it at its next scan, and a window processor drops the
  rows it retained for that branch
- every relay owner, processor, ingestor and reingestor using the branch applies that branch's TTL
  to its own branch-local runtime state. Releasing the branch ends only that entity's branch-local
  state and the relay delivery channels it owns or publishes through for the branch. Another relay
  or processor that holds a branch with the same key, under this branch or another branch of the
  same schema, keeps its branch and the deliveries in flight on it
- relay TTL and `MAX INSTANCES ... EVICT LRU` are enforced once by the relay owner across all
  producers in the cluster. A partitioned relay enforces them in each partition owner on the keys
  of that partition, so `MAX INSTANCES` caps each partition

TTL is the expiration boundary; releasing the expired branch is a separate maintenance scan that
runs on the physical clock. A branch is never released before its TTL has elapsed in the domain's
clock, and it is released at the first scan after that boundary. The scan interval is therefore
release latency added on top of the TTL, never early expiry, and accelerating `TIME RATE` shortens
the TTL in real time without changing the scan interval.

## Materialized State

Materialized relay state is enabled with:

```plexus
CREATE IF NOT EXISTS RELAY notifications
  SCHEMA notification
  UNBRANCHED
  WITH MATERIALIZED STATE LAST BY TIMESTAMP;
```

Current semantics:

- materialized state is keyed by the branch key
- a branch grouped by nothing has one root entry
- Nervix keeps the latest full record per branch group according to record metadata watermarks
- materialized state is persisted to Fjall
- persisted snapshots are replicated to scheduler-selected state replicas
- when a concrete branch-local relay expires, Nervix deletes the matching materialized entry and replicates that deletion

Because watermark and timestamp metadata travel alongside rows inside relay batches, batching does not change `LAST BY TIMESTAMP` semantics. Materialized state still compares records using the preserved runtime metadata for each row.

Operational notes:

- `STOP` preserves persisted materialized state
- `START` clears materialized state for the active domain before new execution proceeds
- after a crash, Nervix restores persisted materialized entries from Fjall
- a backup captures fresh materialized rows, exact schemas, typed branch keys and watermarks;
  `RESTORE ... RESUME` installs them on owners and replicas before generators start, retaining
  the archived domain start generation. A default stopped restore followed by `START` clears them.
  See [Backup And Restore](backup-and-restore.md#what-a-restore-recreates).
- Materialized branch activity times are not persisted. Restoring a materialized relay starts
  each restored branch's idle lifetime at that relay's activation time in the domain clock.
  Preserved row event watermarks still select `LAST BY TIMESTAMP`; they do not supply branch
  activity or an expiry deadline. The first maintenance scan therefore retains restored rows
  until their full TTL has elapsed from activation, even when their event timestamps are in the
  distant past or future.

Materialized state is also the readable snapshot surface for `GENERATOR` nodes. A generator declares exactly one materialized relay with `USING MATERIALIZED STATE <relay>` and reads it through `relay_state.<relay>.<field>`.

A partitioned materialized relay keeps each key's row with the owner of the key's partition and
replicates each partition to its own state replicas. A materialized dependency reads one key's row
from the owner of that key's partition, and a generator reads every partition's rows from their
owners.

`SHOW RELAY <relay> MATERIALIZED STATE` reports `kind: RELAY`, the relay owner, and its
scheduler-selected state replicas before the materialized entries or empty-state message. A
partitioned relay reports its partitions instead of one owner, as `DESCRIBE RELAY` does, and gathers
each partition's entries from its owner. Each
entry keeps its typed branch key through collection and renders it at this reporting boundary;
unbranched entries render as `key=(root)`. An entry's payload is written as JSON, which has no
number for a NaN or an infinity, so a float field holding one is written as the string `NaN`,
`Infinity` or `-Infinity`. A record holds one when a payload's number rounds past its `F32` field's
range, or when an Avro payload or a producer's Arrow batch carries one.

`DESCRIBE RELAY <relay>` reports the owner and state replicas immediately after `kind: RELAY`, then
the logical definition and owner buffer-utilization metrics. An ordinary relay reports
`replicas: -`. Traffic metrics remain on the producing or consuming runtime-node edge. A partitioned
relay reports its partition count and then one line per partition in place of the owner and replica
lines, and its metric lines come from every partition owner, each naming its partition:

```text
partitions: 4
partition 0: owner=node-2 replicas=-
partition 1: owner=node-1 replicas=-
partition 2: owner=node-3 replicas=-
partition 3: owner=node-2 replicas=-
```

`DESCRIBE RELAY <relay> WHERE (...)` is answered only by the current owner of the key's partition
and reports owner-authoritative concrete-branch existence and buffer metrics. A partitioned relay
also reports the key's partition and the node that owns it, as `partition: <p>` and `owner: <node>`
after the existence line. A branch exists from the moment
the owner accepts a batch for it until the owner evicts or expires it. Relay presence and metrics
are not replicated. A planned owner move drains admitted work before cutover; an owner failure loses
buffered batches, presence, and relay metrics. A new owner, whether on another node or after the
relay's execution is rebuilt on the same one, reports a branch only after it accepts a batch for
that branch itself. Materialized records survive when a current state replica can become owner.
Prometheus exports aggregate relay metrics without branch-key labels, with a `partition` label
that names a partitioned relay's partition and is `-` for every other series;
see [Metrics And Observability](metrics-and-observability.md).

## Other Replicated Runtime State

Materialized state is only one example of replicated runtime state. Others include:

- Kafka offsets for `OFFSET BY DOMAIN`
- deduplicator state
- graph metric summaries

Kafka partition assignment for `OFFSET BY DOMAIN` is not runtime-local replicated state. The leader observes Kafka partition topology, computes instance assignment, and commits that assignment into the Raft-backed domain schedule. That committed schedule is then persisted through the control-plane storage path and applied by runtime nodes.

This is still recovery-oriented state, not transactional exactly-once storage.
