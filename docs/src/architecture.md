# Architecture Overview

At a high level, Nervix has two major halves:

- a control plane that owns definitions, scheduling, lifecycle, and coordination
- a data plane that moves records through the runtime graph

The graph itself is built out of persisted NSPL models:

- `SCHEMA`
- `WIRE JSON SCHEMA`
- `WIRE CBOR SCHEMA`
- `WIRE AVRO SCHEMA`
- `CODEC`
- `RESOURCE`
- `RELAY`
- `CLIENT`
- `VHOST`
- `ENDPOINT`
- `INGESTOR`
- `JUNCTION`
- `DEDUPLICATOR`
- `REINGESTOR`
- `EMITTER`

Those models are stored in the registry and scheduled into a `DomainSchedule`. The schedule says
which nodes exist in a domain, which server is primary for each node, and which servers hold
replicas. A relay is one of those runtime nodes: it has one owner, and only materialized relay state
has scheduler-selected replicas.

This graph configuration is persisted with strong control-plane consistency. It is separate from runtime execution state and from the hot-path records moving through the graph.

Consensus access is restricted by operation. Observers can read locally applied state and watch
changes. Proposers can also attempt replicated mutations, administrators manage membership and
leadership transfers, and protocol receivers apply Raft messages independently of those capabilities.
Consumers receive only the capabilities their responsibilities require. Observation does not imply a
linearizable read, and proposal authority does not imply that the node is currently leader.

Raft still checks leadership when accepting a proposal. If leadership changes after a command is
admitted, the server preserves the leadership-loss result so the client can redirect to the new
leader or wait for an election. Long-running transaction commits validate leadership between steps;
their replicated progress survives leader loss so the new leader can continue execution. Raft
protocol listeners remain available on every live node through elections and membership changes.

The runtime then instantiates that schedule:

- ingestors attach to external systems or local endpoints
- relay owners buffer and route records, including fan-out to remote consumer nodes and sessions
- junctions, deduplicators, and reingestors transform or route records between relays
- emitters encode records and publish them externally

Clock ownership follows the same one-way conversion. NSPL parsing turns `PERIOD`, `SKEW`, start
timestamps, and rates into validated vocabulary values. The control plane commits one mapping and
fenced authority for a paced `START`. Each data-plane execution binds a capability for the exact
domain and generation and obtains one timestamp snapshot before calling an expression engine or a
WASM guest. Engines accept that timestamp as input and cannot read actual UTC. Logical deadlines
carry their domain and generation; operational deadlines are a separate process-monotonic type
whose construction is limited to timeout, retry, and external-I/O owners. Actual UTC enters the
data plane through one physical-time owner and is projected into logical time or used by an
explicit external observation contract.

The [Domain Clock](./domain-clock.md) chapter defines the mapping, lifecycle generation,
authority fence, progress delivery, local installation, execution snapshots, admission arithmetic,
and logical-deadline boundary in detail.

Runtime execution has its own persistence boundary. Selected execution-node state is persisted
through periodic snapshots and replication, but in-flight message batches and ACK state are
hot-path memory only. Relay buffers, concrete presence, fan-out, and metrics are owner-local and
unreplicated; optional materialized records use the relay's state replicas.

`RESOURCE` sits between the control plane and the runtime. The control plane versions and replicates it across the cluster, while runtime nodes use its unpacked local directory form when a model depends on concrete files.

One concrete example is `VHOST` TLS:

- the control plane tracks uploaded certificate bundles as resource versions
- a `VHOST` can bind one of those resources, optionally pinned to an explicit version
- the data plane serves HTTPS and WSS from a dedicated HTTPS listener using the local replicated resource files

All node-to-node traffic uses one mutually authenticated TLS 1.3 and HTTP/2 listener. Independent
management, command, replication, relay, and bulk pools isolate traffic, while typed operations,
bounded payloads, admission quotas, memory budgets, and deadlines keep one workload from exhausting
the node. Application health is observed separately from transport connectivity.

The [Cluster Interconnect](./interconnect.md) chapter defines peer identity, connection topology,
wire contracts, resource isolation, exchange forms, relay delivery and reconciliation, consensus
and bulk traffic, domain-clock progress, application health, lifecycle behavior, and observability.

The rest of this section splits control-plane semantics from data-plane semantics because that distinction is fundamental to how Nervix behaves.
