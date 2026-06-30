# Streams And State

`RELAY` declares a named connection between Nervix runtime nodes.

```nspl
CREATE [IF NOT EXISTS] RELAY notifications
  SCHEMA notification
  CAPACITY 1;
```

## Streams, Parameter Groups, And Branches

External feeds commonly contain records for many tenants, users, devices, accounts, or other business groups. Nervix parameter grouping lets one declared graph process those groups independently.

Parameterization is defined by a schema name, and concrete branch creation is defined by a named branch:

- `CREATE BRANCH by_user PARAMETERIZED BY user_branch VALUES { user_id = notifications.user_id } TTL 5m` isolates each user
- `CREATE BRANCH by_tenant PARAMETERIZED BY tenant_branch VALUES { tenant = notifications.tenant } TTL 5m` isolates each tenant
- `CREATE BRANCH by_tenant_user PARAMETERIZED BY tenant_user_branch VALUES { tenant = notifications.tenant, user_id = notifications.user_id } TTL 5m` isolates each tenant/user pair
- `MAX INSTANCES <n> EVICT LRU` can cap active concrete branch instances for that branch

Relays declare their branch-key shape without values:

```nspl
CREATE RELAY notifications SCHEMA notification PARAMETERIZED BY tenant_user_branch;
CREATE RELAY global_notifications SCHEMA notification UNBRANCHED;
```

An ingestor or reingestor uses `BRANCHED BY <branch>` to compute the parameter group for each record. When records for a group arrive, Nervix uses a branch instance for that group. A branch instance is the runtime execution path for one concrete group.

Inside a branch, records move through relay instances. Each relay instance has:

- the declared `RELAY` name it belongs to
- a branch identity
- a schema
- buffering behavior

Processing node state also belongs to the branch. That gives each group independent deduplicator history, reorder buffers, window accumulators, materialized entries, and branch-local relay buffers.

Runtime branch rules:

- an `INGESTOR` starts a branch for one concrete parameter group through `BRANCHED BY <branch>`
- normal downstream processors keep the same branch group
- output routes and forwarders send records to downstream relay names inside the same branch
- stateful processors keep branch-local state for that group
- a `REINGESTOR` may consume across a branch boundary and start new downstream branches through `BRANCHED BY <branch>`
- an `EMITTER` consumes records across the whole input relay and terminates the branch at an external sink

`branch` is a reserved namespace, so a relay cannot be named `branch`. Inside a parameterized branch, filter-map and parameter mapping expressions can read the current parameter group with `branch.<key>`. The key must exist in the branch schema, and unbranched execution has no `branch.<key>` fields.

## Internal Payload Model

After schema application, Nervix does not keep an internal per-message document format on relays. The runtime payload on a relay is an Apache Arrow record batch plus the schema and per-row runtime metadata needed for ACKs and watermark-based logic.

Apache Arrow is used here for two practical reasons:

- fast vectorized processing over columnar data inside runtime nodes
- fast serialization and deserialization when batches move between nodes

Operationally that means:

- ingestors and reingestors batch decoded rows before writing into a relay
- deduplicators still apply row-level state semantics inside the node, while junctions stay Arrow-native and concatenate compatible branch-local batches before forwarding
- window processors keep branch-local online aggregate state and emit Arrow batches containing only the fields declared by their `AGGREGATE` block
- batches remain branch-local until a `REINGESTOR` or `EMITTER` boundary changes the routing behavior
- inter-node relay transport serializes those batches with Arrow IPC, so the batch stays Arrow-native over the network too

Lookup and state-replication control paths are separate from this relay payload model. The Arrow batch path applies to relay movement inside the data plane.

Relay batches and their per-row ACK metadata are hot-path runtime data. They are not persisted as relay state, and ACK guards/tokens/maps are never part of runtime snapshots. Materialized relay state, when enabled, is a separate execution node state snapshot of selected record values.

## Capacity

`CAPACITY <n>` controls the relay buffer size for the relay runtime. For
parameterized relays, the capacity applies to each concrete branch-local relay
instance. It is an active backpressure boundary: if downstream runtime
consumers such as reingestors, emitters, or branch processors cannot drain a
relay quickly enough, upstream dispatch waits once the relay buffer is full.

The capacity can be changed after creation:

```nspl
ALTER RELAY notifications SET CAPACITY 5;
```

The updated capacity is persisted in the relay definition and applied to active
runtime fan-outs for the relay, including fan-outs used by existing concrete
branches of a parameterized relay. Existing subscriptions and runtime consumers
remain attached while the fan-out buffers are resized.

Increasing capacity is applied in place without reducing buffered data. When
capacity is shrunk below the current buffered depth, the active fan-out keeps its
existing physical buffer until receivers drain it far enough to apply the new
capacity without discarding in-memory batches. Publishers continue to observe
relay backpressure while the resize is pending.

Small capacities are useful in tests and tiny examples, but high-throughput
graphs should use capacities large enough to absorb several flush intervals of
batches. This is especially important for relays written by external ingestors
and relays read by reingestors, because a low buffer can multiply short waits
across every branch. If omitted, Nervix uses the default relay buffer.

## TTL

TTL is a branch contract, not a relay-local setting. `CREATE BRANCH` declares `TTL <duration>` after its `VALUES { ... }` map. `UNBRANCHED` branch roots do not declare TTL because there are no concrete parameter groups to expire.

TTL controls:

- concrete branch-local relay expiration in memory
- materialized-state cleanup when the relay is materialized
- downstream processor state cleanup for the same concrete branch

Expiration semantics:

- paced domains use domain logical time
- unpaced domains use wall clock time
- every relay and processor in the same branch tree uses the branch root's TTL and expires together

## Materialized State

Materialized relay state is enabled with:

```nspl
CREATE [IF NOT EXISTS] RELAY notifications
  SCHEMA notification
  WITH MATERIALIZED STATE LAST BY TIMESTAMP;
```

Current semantics:

- materialized state is keyed by the branch parameter group
- a branch grouped by nothing has one root entry
- Nervix keeps the latest full record per branch group according to record metadata watermarks
- materialized state is persisted to Fjall
- persisted snapshots are replicated to runtime followers
- when a concrete branch-local relay expires, Nervix deletes the matching materialized entry and replicates that deletion

Because watermark and timestamp metadata travel alongside rows inside relay batches, batching does not change `LAST BY TIMESTAMP` semantics. Materialized state still compares records using the preserved runtime metadata for each row.

Operational notes:

- `STOP` preserves persisted materialized state
- `START` clears materialized state for the active domain before new execution proceeds
- after a crash, Nervix restores persisted materialized entries from Fjall
- per-group TTL metadata is not yet persisted, so crash recovery does not currently perform a startup sweep of stale materialized entries

Materialized state is also the readable snapshot surface for `GENERATOR` nodes. A generator may reference `materialized_stream.field` values from any relay in the same domain that declares materialized state.

`SHOW RELAY <relay> MATERIALIZED STATE` includes the scheduled materializer owner and replicas before the materialized entries or empty-state message. This matches the placement visibility exposed by other state-holding runtime nodes.

`DESCRIBE RELAY <relay>` reports the logical relay definition, including schema, parameterization, capacity, materialized-state marker, and relay buffer-utilization metrics when available. Traffic metrics are reported on the producing or consuming runtime node edge instead of on the relay itself.

`DESCRIBE RELAY <relay> WHERE (...)` includes branch-local relay existence and buffer metrics for the matching concrete branch when metrics exist. These summaries are part of runtime state and are preserved through snapshot replication and node drain. Prometheus uses a separate live registry and exports aggregate relay metrics without branch labels; see [Metrics And Observability](metrics-and-observability.md).

## Other Replicated Runtime State

Materialized state is only one example of replicated runtime state. Others include:

- Kafka offsets for `OFFSET BY DOMAIN`
- deduplicator state
- graph metric summaries

Kafka partition assignment for `OFFSET BY DOMAIN` is not runtime-local replicated state. The leader observes Kafka partition topology, computes instance assignment, and commits that assignment into the Raft-backed domain schedule. That committed schedule is then persisted through the control-plane storage path and applied by runtime nodes.

This is still recovery-oriented state, not transactional exactly-once storage.
