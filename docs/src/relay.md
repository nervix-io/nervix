# Streams And State

`RELAY` declares a named, schedulable runtime node between producers and consumers. Every relay
has one primary owner. Only that owner instantiates the relay buffer, concrete branch presence,
relay metrics, subscription fan-out, and optional materialized state.

```nspl
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
- `MAX INSTANCES <n> EVICT LRU` can cap active concrete branch instances for that branch

Relays select an explicit branch or declare unbranched execution:

```nspl
CREATE RELAY notifications SCHEMA notification BRANCHED BY by_tenant_user;
CREATE RELAY global_notifications SCHEMA notification UNBRANCHED;
```

## Altering Relays

`ALTER RELAY` accepts one or more comma-separated operations. Operations execute in written order,
so later operations may replace values set earlier in the same statement:

```nspl
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
- an `EMITTER` consumes records across the whole input relay and terminates the branch at an external sink

`branch` is a reserved namespace, so a relay cannot be named `branch`. Expressions inside a
concrete branch may read its immutable key with `branch.<key>`. Bare fields never resolve to branch
fields. Unbranched execution has no `branch.<key>` values, and successful emitter expressions do
not expose the branch scope.

## Internal Payload Model

After schema application, Nervix does not keep an internal per-message document format on relays. The runtime payload on a relay is an Apache Arrow record batch plus the schema and per-row runtime metadata needed for ACKs and watermark-based logic.

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
- the owner serializes an admitted batch once for each remote consuming cluster node; every local
  runtime consumer on that node shares the delivery, and a session subscription on the same node
  piggybacks on it

Lookup and state-replication control paths are separate from this relay payload model. The Arrow batch path applies to relay movement inside the data plane.

Relay batches and their per-row ACK metadata are hot-path runtime data. They are not persisted or
replicated, and ACK guards, tokens, and maps are never part of runtime snapshots. Materialized
state, when enabled, is the only replicated part of the relay; it is not a second runtime node.

## Capacity

`CAPACITY <n>` controls the single buffer on the relay owner. It is one cluster-wide
backpressure boundary, not a per-producer, per-consumer, or per-branch capacity. If downstream
runtime consumers cannot drain the relay quickly enough, upstream dispatch waits once the owner
buffer and the fixed dispatch slots leading to it are occupied.

At most the following batches can be admitted or in dispatch for one relay:

- `CAPACITY` batches in the owner buffer;
- one batch from each producer cluster node to the owner; and
- one batch from the owner to each remote consuming cluster node.

The owner has no additional inbound queue. A producer's slot is released only after the owner has
admitted that batch. A consumer-node slot is released only after that node has admitted its batch.
These fixed slots do not scale with the number of runtime consumers or subscriptions on a node.

The capacity can be changed after creation:

```nspl
ALTER RELAY notifications SET CAPACITY 5;
```

The updated capacity is persisted in the relay definition and applied in place to the active owner
buffer. Existing concrete branches, subscriptions, runtime consumers, and dispatch slots remain
attached.

Increasing capacity is applied in place without reducing buffered data. When
capacity is shrunk below the current buffered depth, the active fan-out keeps its
existing physical buffer until receivers drain it far enough to apply the new
capacity without discarding in-memory batches. Publishers continue to observe relay backpressure
while the resize is pending. The one-batch producer and consumer dispatch slots never resize.

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
- every relay owner and processor using the branch applies that branch's TTL to its own
  branch-local runtime state
- relay TTL and `MAX INSTANCES ... EVICT LRU` are enforced once by the relay owner across all
  producers in the cluster

## Materialized State

Materialized relay state is enabled with:

```nspl
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
- per-group TTL metadata is not yet persisted, so crash recovery does not currently perform a startup sweep of stale materialized entries

Materialized state is also the readable snapshot surface for `GENERATOR` nodes. A generator declares exactly one materialized relay with `USING MATERIALIZED STATE <relay>` and reads it through `relay_state.<relay>.<field>`.

`SHOW RELAY <relay> MATERIALIZED STATE` reports `kind: RELAY`, the relay owner, and its
scheduler-selected state replicas before the materialized entries or empty-state message.

`DESCRIBE RELAY <relay>` reports the owner and state replicas immediately after `kind: RELAY`, then
the logical definition and owner buffer-utilization metrics. An ordinary relay reports
`replicas: -`. Traffic metrics remain on the producing or consuming runtime-node edge.

`DESCRIBE RELAY <relay> WHERE (...)` is answered only by the current relay owner and reports
owner-authoritative concrete-branch existence and buffer metrics. Relay presence and metrics are
not replicated. A planned owner move drains admitted work before cutover; an owner failure loses
buffered batches, presence, and relay metrics. Materialized records survive when a current state
replica can become owner. Prometheus exports aggregate relay metrics without branch-key labels;
see [Metrics And Observability](metrics-and-observability.md).

## Other Replicated Runtime State

Materialized state is only one example of replicated runtime state. Others include:

- Kafka offsets for `OFFSET BY DOMAIN`
- deduplicator state
- graph metric summaries

Kafka partition assignment for `OFFSET BY DOMAIN` is not runtime-local replicated state. The leader observes Kafka partition topology, computes instance assignment, and commits that assignment into the Raft-backed domain schedule. That committed schedule is then persisted through the control-plane storage path and applied by runtime nodes.

This is still recovery-oriented state, not transactional exactly-once storage.
