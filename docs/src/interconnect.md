# Cluster Interconnect

The cluster interconnect is the authenticated transport between Nervix nodes. It carries cluster
membership, consensus traffic, control-plane requests, domain-clock progress, relay batches,
replicated runtime state, resources, and snapshots. It is present on every live node and remains
available through leader changes, placement changes, elections, and recovery.

The interconnect owns peer authentication, transport connections, wire framing, traffic isolation,
bounded decoding, deadlines, and the delivery protocol for remote relay batches. The operation that
uses it still owns the meaning of a request, the state it changes, and any durable recovery policy.
In particular, a transport response is not automatically a statement that runtime work completed.
Relay delivery exposes separate receipt, admission, and downstream-completion boundaries so callers
can distinguish those outcomes.

## Listener And Peer Topology

Each node exposes one TCP listener for all node-to-node traffic. Every accepted connection uses
mutually authenticated TLS 1.3 and HTTP/2. The listener is independent of Raft leadership and graph
placement, so every live node can receive every class of interconnect operation. TLS provides
confidentiality and integrity as well as peer authentication.

Cluster discovery supplies the current node identity, incarnation, and advertised endpoint for each
peer. A configured bootstrap endpoint is the one exception: the initiating node knows the endpoint
before it knows the remote node identifier, then obtains and authenticates that identifier from the
peer certificate. Once discovered, a peer is addressed by its authenticated identity rather than by
an unverified endpoint claim.

Discovery also carries whether the advertised process incarnation has begun terminating. That state
belongs to the incarnation rather than the stable node identifier: it keeps the process available to
finish existing ownership handoffs and consensus work, but removes it from new placement
destinations. A restarted process has a new incarnation and does not inherit the advertisement.

Connections are directed. Both nodes in a pair build their own outbound connections because some
operations, including relay acknowledgements and cluster events, travel back over the receiver's
outbound management connection. A single connection never changes traffic class after it has been
bound.

The standard pool layout is:

| Pool | Primary traffic | Outbound connections per peer | Streams per connection | Readiness |
| --- | --- | ---: | ---: | --- |
| Management | Membership, health, consensus control, clock progress, relay progress and outcomes | 1 | 64 | Preconnected |
| Commands | Control-plane and runtime requests | 1 | 32 | Preconnected |
| Replication | Consensus log entries, runtime-state replication, ownership handoff | 1 | 8 | Preconnected |
| Relay | Arrow record batches | 2 | 64 each | Preconnected |
| Bulk | Resources, runtime snapshots, consensus snapshots | 1 | 4 | On demand |

A peer is transport-ready only after all five preconnected outbound connections are live. Bulk
traffic does not delay readiness; its connection is opened when needed. The two relay connections
allow unrelated relay channels to make progress independently while preserving ordering within each
logical channel.

The built-in topology limit is 64 peers. A node admits at most 768 incoming and outgoing
connections in total and performs at most 32 connection handshakes concurrently. Those limits cover
the six possible connections in each direction for every peer: five preconnected connections plus
the on-demand bulk connection.

## Peer Identity And Authentication

Every node certificate is signed by the configured cluster CA and supports both the TLS client and
server roles. It contains exactly one canonical identity URI:

```text
nervix://cluster/<cluster-id>/node/<node-id>
```

The certificate also contains a DNS or IP subject alternative name for the node's advertised
endpoint. Local credentials are rejected at startup unless their cluster identifier, node
identifier, endpoint name, validity period, and key all agree. A remote connection is accepted only
when all of the following are true:

- its certificate chains to the configured cluster CA
- its identity URI names the same cluster
- its node identifier is the expected peer identifier, once that identifier is known
- its endpoint subject alternative name matches the advertised endpoint being contacted
- its certificate is currently valid
- TLS negotiates the `h2` application protocol

The first HTTP/2 exchange binds the connection to its authenticated node, traffic class, advertised
endpoint, current process epoch, and wire-contract fingerprint. The receiver cross-checks those
claims against the TLS identity and the connection slot it is accepting. This prevents a valid peer
from relabeling a connection as a different node or traffic class.

Three identities serve different purposes:

- The certificate node identifier is the stable authenticated identity of a node.
- The discovery incarnation and endpoint generation identify the current cluster presence and
  advertised address of that node.
- The process epoch identifies one running interconnect process and fences in-memory delivery state
  across restarts.

Replacing an endpoint, restarting a process, and rotating a certificate therefore have distinct
meanings even when the stable node identifier does not change.

Coordination operations use a typed identity composed of the authenticated coordinator node, its
current process epoch, and a process-local sequence. The sequence begins independently in every
process; the node and process epoch make equal sequence values distinct across concurrent leaders
and restarts. For every coordination request, the receiver verifies the node and process epoch
against the bound connection before the application handler can observe the request. A process
therefore cannot issue or replay an identity that belongs to another node or to an earlier run of
the same node.

Entity-gate engagement binds that identity to one canonical domain, relay set, affected-entity set,
and purpose. A retry succeeds only for the same identity and complete scope; using the identity for
a different scope is rejected. Drain status and release carry the same identity, so a delayed
message from another coordinator or process incarnation cannot observe or remove the hold. Planned
ownership-handoff capture, preparation, confirmation, activation, and discard also carry this
identity. Prepared handoff state persists it in the sole current stored shape and checks it again
before activation or cleanup after a restart. The handoff's committed schedule-transition ID still
names the schedule change; it does not authorize coordination traffic.

The coordinator records every destination as an attempted participant before sending its prepare
request. A timeout, cancellation, or missing response after the destination persisted the request
therefore remains explicit cleanup work. Discard is exact and idempotent over the coordination
identity, transition ID, domain, and entity; a delayed discard for one operation cannot remove a
replacement prepared by another operation.

The current leader reconciles durable preparations after leadership or live-incarnation changes.
It first commits a consensus barrier and sends its log position with the reconciliation request on
the replication pool. Each participant applies through that position before consulting its local
committed schedule. An exact ownership transition in that schedule preserves its preparation even
when the original coordinator or destination process has gone. An uncommitted preparation remains
only while the original coordinator process and both bound participant incarnations are live and
the exact ownership-handoff gate is still held; every other preparation is discarded durably. A
second pass after the gate deadline reclaims work that was active during the first pass.

After a receiver admits a new gate engagement or release, a receiver-owned task finishes that state
transition even if the requesting connection disappears. Coordinator loss therefore cannot strand
an operation in a partially engaged state or cancel cleanup after the receiver accepted it.

Each receiver lease is a distinct in-memory instance even when an identical request is re-engaged
after release. Its deadline task may remove only that exact instance. An earlier deadline can
therefore neither release nor erase a replacement lease occupying the same logical operation key.

## Wire Contract And Payloads

All nodes in a running cluster use one current wire contract. A fixed fingerprint covers the set of
supported operations and their encoded shapes. A fingerprint mismatch rejects connection setup;
there is no version negotiation or alternate decoding path. A wire-contract change therefore
requires a coordinated cluster stop and start with all nodes on the same version.

Control records use bounded `rkyv` archives. The receiver validates an archive, including its shape
and nesting depth, before exposing it to an operation handler. Encoded and decoded memory is charged
to the traffic class before decoding begins. Unknown operations, a pool mismatch, malformed
archives, and values above the operation limit fail at the transport boundary.

Relay metadata uses the same validated control encoding, while relay bodies remain Arrow IPC from
the source relay to the destination runtime. Bulk operations transfer opaque byte chunks and let
the owning resource, snapshot, or state protocol interpret the stream. The primary payload limits
are:

| Payload | Maximum size |
| --- | ---: |
| Management event | 64 KiB |
| Command | 1 MiB |
| Replication batch | 2 MiB |
| Encoded relay batch | 32 MiB |
| Decoded relay batch | 32 MiB |
| Relay decode scratch space | 16 MiB |
| Bulk transfer chunk | 64 KiB |
| Snapshot section | 8 MiB |

HTTP/2 flow control adds another bound. Each stream begins with a 64 KiB receive window, each
connection begins with a 256 KiB receive window, and request headers are limited to 16 KiB. A
receiver releases more HTTP/2 credit only as it accepts and accounts for more data. Large streamed
objects can therefore cross the cluster without either endpoint holding the whole object in memory.

## Traffic And Resource Isolation

An operation's type fixes its pool, admission quota, response type, and deadline policy. Callers do
not select a less restricted pool at the call site. This preserves traffic isolation even when a
busy subsystem issues many requests.

The management connection reserves its 64 outbound stream slots as follows:

| Use | Reserved slots |
| --- | ---: |
| Shared management operations | 32 |
| Discovery | 4 |
| Application liveness | 8 |
| Progress reports and relay status | 8 |
| Runtime and relay admission | 4 |
| Relay cancellation | 4 |
| Relay terminal outcomes | 4 |

The replication pool reserves two of its eight slots for the active ordered consensus-append
stream and a replacement while the previous generation closes. The other six are shared
replication slots. The bulk pool reserves two slots for resources, one for snapshots, and one for
other bulk work. Commands and relay bodies use the shared capacity of their dedicated pools.

With the standard node capacity, typed requests also have independent node-wide admission quotas,
applied separately to incoming and outgoing work across all peers and connections:

| Request use | Standard admissions per direction |
| --- | ---: |
| Shared | 1,024 |
| Consensus append | 8 |
| Resource transfer | 8 |
| Snapshot transfer | 8 |
| Discovery | 8 |
| Application liveness | 32 |
| Domain-clock progress | 8 |
| Runtime and relay admission | 8 |
| Relay cancellation | 4 |
| Relay terminal outcomes | 4 |

These reservations mean that ordinary management traffic cannot consume the capacity required to
resolve already-started relay work or determine whether a peer is healthy.

Leasing a stream slot takes no exclusive lock and allocates nothing once a pool is established.
When an endpoint is registered for a peer, the transport builds the identity of every connection
slot that endpoint can hold, and each lease reads the registration, its slots, and their connections
through shared lookups. Only a slot that is not running yet, such as the first on-demand bulk
connection, takes the exclusive path that starts it. A replaced endpoint gets a new registration
with its own slot identities, so a lease can never select a connection that belongs to the endpoint
it replaced.

Memory and CPU execution are isolated by class:

| Class | Memory budget | CPU execution class |
| --- | ---: | --- |
| Management | 8 MiB | Control |
| Commands and replication | 24 MiB | Control |
| Relay | 192 MiB | Data |
| Bulk | 32 MiB | Bulk |

The total interconnect memory budget is 256 MiB. Reservations cannot borrow from another class. The
relay budget holds two worst-case operations, each consisting of a 32 MiB encoded body, a 32 MiB
decoded batch, and 16 MiB of decode scratch space. Startup validates the relationships among
operation limits and these budgets, including the paired work that must fit for independent
operations to keep making progress.

## Exchange Forms

The interconnect provides five exchange forms. Each keeps transport mechanics separate from the
operation's domain semantics.

1. **Bounded event.** The sender transmits one validated event and receives an empty success
   response after the receiver accepts that event. Cluster events and asynchronous acknowledgements
   use this form.
2. **Typed request and response.** The request type declares its operation name, response type,
   traffic class, admission subquota, deadline, and live-target requirement. Remote typed errors are
   kept distinct from transport failures. Application health and domain-clock progress use this form
   so their reserved quotas and typed outcomes apply independently of generic events.
3. **Streamed response.** The receiver declares the exact byte length, then sends a flow-controlled
   sequence of chunks. The reader rejects early end, extra bytes, and a stalled chunk. Dropping the
   reader cancels the stream and releases its reservations.
4. **Ordered duplex stream.** Each direction sends length-prefixed, validated frames in order and
   may close independently. Opening the stream is bounded by the operation's declared setup
   deadline. An idle established stream is valid; the owning protocol sets deadlines for answers it
   is awaiting. The initiator's sender reports when the peer's flow control last accepted its
   bytes, so that protocol can tell a slow answer from a peer that accepts nothing. Consensus append
   traffic uses this form.
5. **Relay delivery.** A management-plane grant reserves receiver capacity before an Arrow body is
   sent, followed by explicit runtime admission and optional downstream record acknowledgements.

Each handler registration publishes a complete replacement dispatch table, so an arriving operation
finds its handler without taking a lock and registrations that race each other all take effect.
Discovery likewise publishes the complete live-node set at once, and the live-target check a typed
request makes reads that set without a lock. Until discovery publishes its first set, every target
counts as live so that bootstrap discovery can reach its peers.

Connection setup has a five-second deadline covering TCP, TLS, HTTP/2, and connection binding. The
default bounded request deadline is ten seconds and includes time waiting for an admission or stream
slot. An operation can declare a tighter or longer semantic deadline; application health uses one
second, domain-clock progress uses two seconds, and resource and state stream openings use longer
operation deadlines. Once a streaming body is moving, five seconds without progress fails that
transfer. Queueing never creates an unbounded extension to a declared deadline.

## Remote Relay Delivery

The relay protocol connects the owner-local fan-out described in [Relay](./relay.md) to a concrete
runtime branch on another node. It preserves the Arrow batch as a columnar payload and carries only
bounded metadata beside it.

A logical outgoing channel is identified by the authenticated destination node, payload role,
domain, destination relay, and concrete branch. Different channels may run concurrently. Within one
channel, the sender preserves FIFO order and permits at most one batch to wait for runtime admission.
Each sender channel has a unique incarnation and a monotonically increasing sequence number.

For fan-out, the source encodes an Arrow batch once and shares those immutable encoded bytes across
destinations and transport retries. Delivery metadata, admission state, and attached record
acknowledgements remain specific to each destination channel.

A delivery proceeds as follows:

1. The sender leases a relay-body stream before requesting receiver capacity. This prevents a
   saturated body pool from holding a remote reservation that it cannot use.
2. The sender requests a grant over the reserved management admission quota. The request identifies
   the delivery process, channel, sequence, payload length, branch, relay, and acknowledgement shape.
3. The receiver verifies the sender process epoch and delivery order. Before issuing a grant, it
   reserves the encoded body, maximum decoded batch and scratch memory, an incoming queue item, and
   capacity for the eventual terminal admission outcome.
4. The grant remains valid for five seconds. The sender transmits the exact Arrow IPC body over the
   relay pool and names the grant and both process epochs.
5. The receiver reads exactly the granted length under the reservation, records body receipt, and
   places the work in its bounded application queue. The HTTP/2 body response confirms receipt by
   the receiving process only.
6. The application lane validates the exact schema, metadata, branch, and record-acknowledgement
   count, then resolves the configured concrete runtime branch. Work for one channel remains ordered,
   while other channels continue independently.
7. When the concrete runtime branch accepts the batch, the receiver sends a terminal admission
   outcome over the reserved management capacity. The sender can then release the channel for the
   next batch.
8. If the send includes record acknowledgements, later acknowledgement events report downstream
   processing completion. Detached sends and subscription fan-out end at admission.

These steps expose three intentionally different outcomes:

- **Body received** means the complete Arrow body reached the receiving process.
- **Admitted** means the intended concrete runtime branch accepted the batch.
- **Acknowledged** means the downstream work represented by an attached record acknowledgement
  completed.

While admission or an attached acknowledgement is unresolved, the receiver sends coalesced progress
events through the reserved management quota. The sender treats five seconds without progress as a
stalled exchange and bounds the total admission wait at five minutes. Progress keeps a live attempt
from being mistaken for a disconnected one; it does not change the delivery outcome.

### Ordering, Retry, And Reconciliation

A delivery identity combines the sender process epoch, receiver process epoch, channel incarnation,
and channel sequence. The receiver also records the expected content for that identity. Repeating
the same identity with different content is rejected.

After a connection loss, the sender first reconciles with the same receiver process. If the body was
already received or the batch was admitted, the receiver returns that known state and the sender
does not resend the body. Sequence watermarks reject reordering and duplicate enqueue. Inactive
sender channels rotate after five minutes. A receiver keeps a channel's watermark while a batch
granted on that channel is unresolved, and for at least ten minutes after the watermark was last
recorded or consulted by a grant, status, or cancellation request, so ordinary reconnects can still
reconcile. A consultation reads the watermark through a shared lookup and refreshes its retention
with one atomic maximum, so checking a delivery against its channel takes no exclusive lock on the
watermark.

Each attempt carries the channel and admission identities it was granted under. Once the receiver
has delivered an attempt's terminal outcome, it retires that same attempt: it advances the channel
watermark and releases the attempt, its channel occupancy, and its admission without rebuilding
either identity or looking the admission up again.

Relay attempts, grants, watermarks, admission state, and record-acknowledgement maps are in-memory
hot-path state. A receiver process-epoch change therefore makes an unresolved delivery
indeterminate. The transport does not replay it automatically against the new process. If the
source's policy calls for another attempt, that attempt opens a new channel incarnation and is a new
delivery identity.

### Cancellation And Branch Changes

Cancellation and runtime admission compete through one atomic transition. A cancellation that wins
before admission permanently fences that delivery identity, including when cancellation reaches the
receiver before the original grant request. If admission has already won, cancellation returns the
known admitted result and does not undo work handed to the runtime.

Removing or evicting a concrete branch cancels its unadmitted channel generation and releases its
reserved work. If the branch appears again, it uses a fresh channel incarnation and sequence. This
keeps a previous branch lifetime from being confused with the new runtime instance.

## Membership, Consensus, And Bulk Transfer

Cluster membership gossip uses management discovery capacity. It discovers topology and
incarnations but does not replace application health checks. Gossip payloads remain below the
management-event bound, so discovery cannot allocate an arbitrary wire message. A node that is
shutting down closes its gossip transport before it stops gossip, so an exchange still waiting on a
peer that stopped first ends at once instead of holding shutdown until its one-second deadline.

Terminal teardown closes the gossip exchange path before it asks the gossip loop to stop. The loop
reads its stop request only between rounds, and a round exchanges with each selected peer in turn
under a one-second request timeout. Closing the path first makes an exchange still waiting on a
peer that is itself stopping fail at once, instead of holding teardown for the rest of the round.

Consensus separates traffic according to the progress it protects:

- heartbeats, votes, leadership notifications, linearizable runtime-admission reads, and other
  small control exchanges use management capacity
- each leader-to-follower log uses one ordered duplex stream on the replication pool
- runtime-state replication and ownership handoff use the remaining replication capacity
- consensus snapshots use the snapshot reservation on the bulk pool

The ordered append stream can keep multiple batches in flight while preserving follower order. Its
consensus-level window bounds a follower to 16 outstanding batches and 16 MiB of unacknowledged log
data. Heartbeats and elections remain on management capacity, so a full append window does not block
leadership traffic.

The append stream opens under its five-second setup deadline. A follower answers a batch only after
appending it durably, so the leader does not time out individual answers. While a batch is
outstanding, it ends the stream only after five seconds in which no answer arrived and the
follower's flow control accepted none of the leader's bytes. That bound is independent of the
heartbeat interval, which still sets the deadline of each heartbeat.

A process restart does not admit ownership-sensitive runtime execution from its recovered local
state. The restarting node sends `raft_runtime_admission_read` to the leader through the reserved
management admission capacity. The leader performs a strict Raft `ReadIndex`, which confirms its
authority with a quorum and returns the inclusive committed log boundary for the read. A follower
then waits until its own state machine has applied that exact log ID before it reads the coherent
domain, clock-authority, and schedule state used to install runtime execution. The application
discards any runtime-state snapshot taken before this proof.

Admission retries on transport, leadership, and quorum failures without depending on a domain or
schedule notification. Consensus, discovery, administration, shutdown, the interconnect listener,
and configured application listeners continue running during the wait. Runtime routes remain
absent, so listening connectors cannot hand payloads to recovered graph execution. The proof is
process-start admission only; connectivity loss after admission does not revoke execution.

Resource archives and runtime-state snapshots use streamed bulk responses with an exact declared
length. Consensus snapshots use bounded begin, chunk, and finish operations on the same isolated
bulk class. Receivers stage and validate the owning artifact while releasing HTTP/2 credit chunk by
chunk. The whole transfer may exceed the 32 MiB bulk-memory budget because only bounded chunks and
the active decoded section are resident at once.

The [Control Plane](./control-plane.md) defines when replicated changes and resources become
authoritative. The [Data Plane](./data-plane.md) defines how a local execution consumes transferred
state. The interconnect supplies bounded delivery between those owners and does not reinterpret
their state.

## Domain Clock Progress

A paced domain's mapping, generation, and authority fence are committed control-plane state. The
interconnect carries only replaceable progress from that committed authority. It uses a typed
management request in the reserved progress subquota, rather than an unclassified cluster event.
The response confirms that the authenticated receiver evaluated the report against its current
fence; it does not establish or replace the receiver's committed clock mapping.

For each domain and ready remote node, the authority owns one delivery loop with at most one request
in flight and one latest pending report. A newer logical frontier replaces the pending report while
the loop waits for capacity or a response, so a fast clock or slow peer cannot create an unbounded
tick queue. The node-wide progress quota bounds attempts across every domain and peer. Liveness,
relay admission, cancellation, and terminal outcomes retain their independent capacity.

Only live peers that have installed the required runtime revision receive progress. A node that
joins or reconnects receives the newest retained frontier once it becomes ready. Each attempt has a
two-second physical deadline. A failed attempt waits 200 milliseconds, then retries the newest
frontier; authority shutdown cancels both an active request and its retry delay.

The wire report contains typed logical and UTC timestamps and no process-local monotonic instant.
The receiver authenticates the reporting node and applies the committed generation, revision,
authority, and fence checks before retaining the frontier. A stale, duplicate, reordered, or
superseded report cannot replace the mapping or move logical time backward. See
[Domains And Time](./domains-and-time.md) for clock semantics outside the transport boundary.

## Application Health And Availability

An established HTTP/2 connection and a successful transport `PING` show that bytes can move; they
do not show that the peer application can accept work. Nervix therefore probes application health
through a typed management request with reserved liveness capacity.

Each health round has at most one probe in flight for each peer and at most 32 probes across the
node. A probe has a one-second total deadline. Results are published as they complete, so a silent
peer occupies only its own concurrency slot. The next regular round begins roughly one second after
the previous round finishes.

Every result is bound to the exact certificate node identifier, discovery incarnation, endpoint
generation, advertised address, and observation time that were targeted. A healthy response must
return the same application identity. If discovery replaces the incarnation or endpoint while a
probe is running, its late result is ignored.

Health observations distinguish:

- **Healthy:** the current target returned the expected application identity within the deadline.
- **Failure:** the current target returned an error or did not answer within the deadline.
- **Capacity exhausted:** the probe could not obtain its reserved local capacity.
- **Unscheduled:** no probe was due for that target in the current round.

A missing, stale, capacity-exhausted, or unscheduled observation produces unknown availability. It
does not mark a peer unavailable and does not extend a previous run of failures. Only continuous,
fresh failures for the configured node-unavailability interval produce unavailable status; a healthy
observation resets that run. Scheduling and runtime availability use this application result, while
consensus membership continues to use the cluster topology established by gossip.

`SHOW CLUSTER STATUS` exposes the interconnect address, endpoint generation, observation age,
observation outcome, and derived availability. Its `connected` status means the latest application
probe is healthy, rather than merely that a transport pool exists.

## Connection And Credential Lifecycle

A preconnected pool slot that fails reconnects with exponential backoff beginning at 200
milliseconds and capped at five seconds. A peer removal, incarnation change, or advertised endpoint
change retires the old target and cancels work tied to its slots. New operations use only the new
target generation.

Interconnect certificate, key, and CA files are watched as one credential bundle. A candidate must
be complete, valid, and identical in two consecutive reads before it replaces the active bundle, so
a multi-file update cannot install a mixed generation. An invalid or partially written candidate
leaves the current credentials active while the watcher continues trying.

After a valid replacement, new outbound pools use the new credentials and existing inbound HTTP/2
connections begin graceful shutdown. Certificate expiration is also mapped to a process-monotonic
deadline when a connection is authenticated, so a connection cannot remain open beyond the validity
of either peer certificate even if the wall clock later moves.

A replacement publishes the new bundle before it advances the credential generation, and each
connection records the generation it authenticated under. An inbound connection compares that
record with the published generation, without a lock, whenever it accepts a stream and whenever a
replacement is announced, and begins graceful shutdown once the published generation has moved past
it. Because a connection reads the generation before the bundle, it can record a generation older
than its credentials, which only drains it early, but never a newer one that would let replaced
credentials outlive their replacement.

Application shutdown has three ordered phases. A server process issues the stop request that
starts them when it receives its first `SIGINT` or `SIGTERM`, and one shutdown deadline measured
from that request bounds all three. The stop request first marks the local process incarnation as
terminating and then closes admission on its public gRPC, connector, observability, and console
listeners, closing the client connections they had accepted. Its interconnect listener and
registered handlers remain available on that live node throughout the drain-support phase. They
continue carrying queued and active relay payloads, admission and record acknowledgements,
runtime-state replication and checkpoints, ownership-handoff coordination, domain-clock progress,
and the schedule revisions that activate committed ownership. Drain support first moves scheduled
work to a live replacement node when one exists and then completes the work the node already
admitted in place, so these consumers remain available until the node's own graphs are quiescent.

The relay payload lane retains its transport admission guard for every queued or active payload.
Sender-side runtime drain accounting retains the tracked root until admission and every requested
record acknowledgement resolve; this includes roots created for `NO_ACK` sources. Ownership
handoff does not complete until its replication acknowledgement and committed activation have been
observed. These owners keep admitted work visible while the supporting interconnect consumers are
still running.

Only after drain support completes or reports abandonment does terminal teardown call transport
shutdown. Transport shutdown rejects new interconnect admission, cancels pool and operation
waiters, and starts graceful HTTP/2 shutdown. Active transport work receives up to ten seconds to
drain; remaining connections and handlers are then closed. Connection setup and incomplete TLS
handshakes remain inside this bound. A repeated `SIGINT` or `SIGTERM`, or the shutdown deadline
passing, ends the process without running the rest of its shutdown, so its peers observe its
connections ending exactly as they do when the process crashes.

## Failure Ownership And Persistence

Transport failures identify setup, authentication, admission, encoding, decoding, flow-control,
timeout, remote-response, and target-departure failures separately. Typed remote errors remain
available to the operation owner, which decides whether a request is safe to retry. The interconnect
does not infer idempotency for arbitrary control-plane or runtime operations.

Connections, request state, relay grants, delivery reconciliation, progress trackers, and
acknowledgement maps are never persisted. Durable control-plane state remains in consensus, and
selected runtime state remains in its owning snapshot or replication mechanism. This boundary is
why process epochs are part of relay delivery identities and why an unresolved result across a
receiver restart is reported as indeterminate.

An ownership-handoff gate lease is also in-memory coordination state. Releasing or expiring that
lease removes the runtime fence but does not report a persisted preparation as cleaned up. Only an
exact durable discard, activation, or schedule-based reconciliation resolves that preparation.

## Observability

Each node exports interconnect measurements through its local metrics endpoint. The main groups
cover connection and stream occupancy, pending operations, setup failures and resets, quota
exhaustion, request latency, relay channels and grants, admission wait, unresolved delivery age, and
bulk-transfer bytes. Interconnect memory, worker queues, reactor delay, and consensus retention show
whether pressure originates in transport, execution, or the protocol using it.

Typed-request observations identify application health as operation `liveness` and replaceable
domain-clock delivery as operation `progress`, so their request counts, outcomes, latency, and quota
failures can be evaluated independently.

Metric labels are bounded dimensions such as traffic class, direction, operation, outcome, and
reason. They do not include peer, domain, relay, branch, delivery identity, or payload values.
Per-batch and payload-bearing logs use debug or trace levels and do not expose sensitive field
values. See [Metrics And Observability](./metrics-and-observability.md) for the metric and logging
contract.
