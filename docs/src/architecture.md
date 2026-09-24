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

Ingestors and emitters that reach an external system are connectors. The values that cross between
a connector and the runtime hosting it are defined once, in the connector contract: a client's
resolved configuration and the resource mounts it reads files from, the TLS, HTTP client, and
service-URL settings built from that configuration, physical deadlines and the actual-UTC read a
source stamps arrival with, and the transport headers and typed metadata a source message carries.
The runtime resolves resource mounts and projects metadata into its own columns; the contract
carries only the results.

On the sink side, the contract separates codec records from mapped Arrow rows. A record sink
receives one batch of encoded keys, payloads, headers, host positions, and, where the emitter
declares one, the ordering group the runtime evaluated for each record; a row sink receives a
mapped Arrow batch, its target columns, selected rows, and host-derived chunk ranges. Both return
per-record delivery or structured-rejection outcomes and at most one infrastructure failure. The
runtime retains batching, retry cadence, acknowledgement keepalive, stop deadlines, and fault
injection. Connectors reach transient status, events, staging storage, and general-error handling
only through an opaque host handle, so neither runtime types nor ACK maps cross the boundary.
Every record sink implements this contract in its own crate under `crates/connectors`: Kafka,
Pulsar, RabbitMQ, NATS, MQTT, Redis, ZeroMQ, Syslog, SQS, and Sentry. Each crate owns its driver
and the raw client configuration that driver reads, neither of which belongs to the server runtime.
The server's production manifest names the connector crates and contract, while each driver
dependency belongs to its connector crate. The server test harness may depend on those drivers
separately to provision and inspect external systems.

The source side has the same shape. A broker source implements the source contract in the crate
its sink already occupies: Kafka, Pulsar, RabbitMQ, NATS, MQTT, Redis Pub/Sub, ZeroMQ, and SQS,
beside the syslog listener and the WebSocket client source. A connector owns its transport: how it
opens, subscribes, reads the next batch, suspends and resumes, and what acknowledging or rejecting
a position means for its broker, together with its own header semantics. The runtime owns one
instance loop for every broker source: it parses the declared delivery mode into the acknowledgement
policy, opens each instance, decodes and dispatches what the connector reads, waits on the
acknowledgement roots it attached, acknowledges or rejects positions through the connector, and
paces retries, quiesce, readiness, and transient status. The HTTP and Prometheus sources are paced
instead: the runtime polls them on the domain cadence their `EVERY` declares.

The endpoint source is the one source that stays in the server. It has no driver, because the
node's own HTTP and HTTPS listener feeds it, but it implements the same source contract as a
request-scoped source: starting it binds the endpoint's routes to the runtime's request intake, and
closing it unbinds them. The runtime keeps request admission, the endpoint buffer, and rejection
with a `Retry-After` delay on the request path, where each request is admitted and dispatched as
its own ingest group; the endpoint's source loop only replays what the endpoint buffer retained once
a quiesce releases it.

Every ingestor starts on one path, whatever its source. The runtime compiles the ingestor's filter
and output routes and resolves its codec; the composition root, the one place in the server that
maps a source plan to the connector running it, resolves the client configuration and opens the
connector's instances; and only then does the runtime register the ingestor and start each
instance under the loop of its source family. An ingestor whose source cannot start therefore
leaves nothing running. Which quiesce modes a source honors, whether its messages carry readable
headers, and what acknowledgement its delivery mode declares all come from the source vocabulary;
no connector and no runtime table redeclares them.

A row sink writes values rather than encoded payloads, so the runtime evaluates its `VALUES`
mapping itself. The mapping compiles once when the emitter starts and runs once per batch,
producing one Arrow column per target column together with the rows that still have to be written;
a row whose expression failed is rejected with its structured message error and never reaches the
sink. Sensitivity is unchanged by that projection: a mapped value still requires explicit leakage to
leave the domain. Each row sink then encodes from those columns at its own boundary — OTLP protobuf
for OpenTelemetry in `crates/connectors/otel`, `JSONEachRow` lines for ClickHouse in
`crates/connectors/clickhouse`, bound parameters for Postgres and MySQL in
`crates/connectors/postgres` and `crates/connectors/mysql`, BSON documents for MongoDB in
`crates/connectors/mongodb`, and Arrow IPC staging files that one catalog commit turns into Parquet
data files for Iceberg in `crates/connectors/iceberg`. No mapped row is ever materialized as a
scalar between the two.

A sink that stages what it accepts declares that it retains acknowledgements, and the runtime then
hands it the acknowledgements of every row a write carries instead of resolving them as the write
returns. The sink reports the domain or physical deadline by which its staged work must be
published, and the runtime folds that deadline into the emitter's wake, asks for the commit once it
is reached, forces it for a drain, and retries a failed commit on the emitter's declared backoff
while keeping the retained acknowledgements alive. The sink's commit resolves those
acknowledgements and reports what it published, so nothing counts as sent before it is.

The pooled sinks keep the same split. A crate owns its driver's pool and the connection it hands
out, and the runtime owns the lease on the node's one instance of a named client, the wait a graph
node reports while it holds no connection, and the bounds the client declared.

The runtime names every sink crate in one place, its composition root. Each variant of an
emitter's sink plan maps to that crate's constructor, and the connector it opens is paired with the
input the runtime prepares for its contract: the emitter's codec for a record sink, the compiled
`VALUES` projection for a row sink. The emitter task holds that pairing as one boxed connector, so
its batching, retry, commit, and drain are written once for every sink, and no connector is ever
called once per row.

Clock ownership follows the same one-way conversion. NSPL parsing turns `PERIOD`, `SKEW`, start
timestamps, and rates into validated vocabulary values. The control plane commits one mapping and
fenced authority for a paced `START`. Each data-plane execution binds a capability for the exact
domain and generation and obtains one timestamp snapshot before calling an expression engine or a
WASM guest. Engines accept that timestamp as input and cannot read actual UTC. Logical deadlines
carry their domain and generation; operational deadlines are a separate process-monotonic type
whose construction is limited to timeout, retry, and external-I/O owners. Actual UTC enters the
data plane through one physical-time owner in the connector contract and is projected into logical
time or used by an explicit external observation contract.

The [Domain Clock](./domain-clock.md) chapter defines the mapping, lifecycle generation,
authority fence, progress delivery, local installation, execution snapshots, admission arithmetic,
and logical-deadline boundary in detail.

Runtime execution has its own persistence boundary. Selected execution-node state is persisted
through periodic snapshots and replication, but in-flight message batches and ACK state are
hot-path memory only. Relay buffers, concrete presence, fan-out, and metrics are owner-local and
unreplicated; optional materialized records use the relay's state replicas.

`RESOURCE` sits between the control plane and the runtime. The control plane numbers each uploaded
version and completes it only after every live node has verified and installed the same archive.
Every model that uses a resource pins one completed version, and its runtime consumers load exactly
that version from their node's local copy. An upload adds a version and moves nothing; only a model
mutation, such as `REBIND RESOURCE`, changes the version a model binds. A TLS `VHOST` is one such
binding: the HTTPS listener of every node presents the certificate of the version it pins.

The [Resource Versions And Bindings](./resource-versions.md) chapter defines catalog and store
ownership, the version lifecycle, what each binding loads and when, `LATEST` resolution, rebinding,
the `DYNAMIC` TLS refresh, and failure and recovery.

All node-to-node traffic uses one mutually authenticated TLS 1.3 and HTTP/2 listener. Independent
management, command, replication, relay, and bulk pools isolate traffic, while typed operations,
bounded payloads, admission quotas, memory budgets, and deadlines keep one workload from exhausting
the node. Application health is observed separately from transport connectivity.

The [Cluster Interconnect](./interconnect.md) chapter defines peer identity, connection topology,
wire contracts, resource isolation, exchange forms, relay delivery and reconciliation, consensus
and bulk traffic, domain-clock progress, application health, lifecycle behavior, and observability.

Stopping a node is its own ordered lifecycle. One owner advertises that the process incarnation is
terminating, closes public admission, keeps the services admitted work depends on alive while the
local graph drains, and only then tears down the node's tasks, connections, and storage. One
physical deadline bounds all of it, and a repeated signal or an expired deadline ends the process
the way a crash would.

The [Shutdown And Recovery](./shutdown.md) chapter defines stop requests, phases and outcomes, the
deadline and exit statuses, cordon versus terminating placement eligibility, intake stop, graph
drain and force flush, ownership handoff during shutdown, connector acknowledgement and commit
boundaries, terminal teardown, what survives each ending, and restart recovery.

The rest of this section splits control-plane semantics from data-plane semantics because that distinction is fundamental to how Nervix behaves.
