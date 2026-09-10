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

Runtime execution has its own persistence boundary. Selected execution-node state is persisted
through periodic snapshots and replication, but in-flight message batches and ACK state are
hot-path memory only. Relay buffers, concrete presence, fan-out, and metrics are owner-local and
unreplicated; optional materialized records use the relay's state replicas.

`RESOURCE` sits between the control plane and the runtime. The control plane versions and replicates it across the cluster, while runtime nodes use its unpacked local directory form when a model depends on concrete files.

One concrete example is `VHOST` TLS:

- the control plane tracks uploaded certificate bundles as resource versions
- a `VHOST` can bind one of those resources, optionally pinned to an explicit version
- the data plane serves HTTPS and WSS from a dedicated HTTPS listener using the local replicated resource files

All node-to-node traffic uses one authenticated HTTP/2 interconnect listener. TLS 1.3 is mandatory.
Every node certificate carries its cluster and node identity in a
`nervix://cluster/<cluster>/node/<node>` URI SAN and names its advertised DNS name or IP address in
an endpoint SAN. A connection is accepted only when its CA trust, cluster identity, node identity,
advertised endpoint, and HTTP/2 ALPN all agree.

Each peer has independent HTTP/2 pools for membership and management events, commands, Raft
replication, Arrow relay batches, and bulk transfers. Relay progress, cancellation, status, and
terminal admission acknowledgements use reserved management capacity. Every pool except
bulk is connected before a peer is reported ready, with capacity reserved in both directions. This
keeps gossip, heartbeats, elections, administrative operations, and the first remote batch and its
acknowledgement from waiting behind another traffic class. Gossip exchanges, Raft records,
resource chunks, and other non-Arrow messages use bounded, validated rkyv records. Relay payloads
remain Arrow IPC end to end. Resource archives and Raft snapshots cross the bulk pool as bounded
chunks rather than one whole in-memory wire message.

Connection setup, request progress, and whole-request deadlines are bounded. Failed pool slots
reconnect with exponential backoff. When membership removes a peer or changes its advertised
address, its prior slots are retired. Replacing credentials starts HTTP/2 graceful shutdown on old
inbound connections and creates new pools with the replacement certificate. Node shutdown stops
new admission, drains active streams for the configured interval, and then closes anything still
active; an incomplete TLS handshake cannot extend that bound.

Relay transfer has three observable boundaries. A successful relay-body response means the Arrow
bytes reached the receiving process. A terminal admission response means the concrete runtime
branch accepted the batch. Attached record acknowledgements report downstream processing
completion. Receiver reservations include capacity for their terminal admission response, and
progress responses may be coalesced while that terminal capacity remains reserved.

Each sender orders batches by authenticated peer, payload kind, domain, destination relay, and
concrete branch. Different channels run concurrently, while one channel preserves FIFO order and
has at most one batch waiting for runtime admission. A delivery identity combines both process
epochs with a channel incarnation and sequence. Reconnecting to the same receiver process queries
that identity before sending bytes again. A receiver process change makes an unresolved result
indeterminate and prevents automatic replay. A later source retry opens a fresh channel
incarnation, so it is explicitly a new attempt.

Cancellation and runtime admission share one atomic transition. Cancellation that wins before
admission permanently fences that delivery identity. Cancellation after admission reports the
known admitted outcome and does not reverse work already handed to the runtime. Branch and domain
waits therefore retain their own admitted memory while leaving unrelated channels and the reserved
management operations runnable. Evicting a branch cancels its unadmitted generation; if the branch
appears again, its delivery channel opens with a fresh incarnation and sequence.

The rest of this section splits control-plane semantics from data-plane semantics because that distinction is fundamental to how Nervix behaves.
