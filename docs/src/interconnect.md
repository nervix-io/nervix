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

## Simulation Boundary

The transport also runs, unchanged, inside a seeded Turmoil network simulation. That simulation is a
test harness outside product ownership, and
[Deterministic Interconnect Simulation](./interconnect-simulation.md) owns it: its build mode, the
fault model, supervision, replay and failure records, the scenario matrix, the commands and CI
budget, and the limits of what it establishes. The interconnect owns only the seams the simulation
plugs into. Each seam has one production behavior, which is what every node uses:

- **Sockets and DNS.** The TCP listener, outbound streams, and peer resolution use Tokio's
  operating-system APIs. The dedicated `turmoil` test build selects Turmoil's simulated TCP and DNS
  at that one boundary; TLS, HTTP/2, the envelope codec, and Arrow IPC above it are the same code in
  every build.
- **Certificate time.** Each credential bundle carries the one UTC clock its certificates are judged
  by, as described in [Peer Identity And Authentication](#peer-identity-and-authentication).
  Production bundles use the system clock.
- **Identity entropy.** The process epoch and relay grant identifiers are drawn from the transport's
  configured entropy, which in production is the operating system's secure random source.
- **Deadlines.** Every transport deadline is a Tokio instant, so it follows the clock of the runtime
  it runs on: connection setup, request and progress timeouts, reconnect backoff, relay grant
  lifetimes, and the drain deadline derived from certificate expiry.
- **CPU work.** Encoding and decoding run through `nervix-execution`. Its Turmoil build runs bounded
  CPU jobs as tasks on the simulated scheduler under the same admission, charge, and cancellation
  policy.

The transport's concurrent maps use per-process hash seeds. Where a walk over one of them causes
effects, the walk runs in the key's semantic order instead of map order: retiring removed or
departed peer targets, cancelling a node's or every connection slot, re-establishing preconnected
slots after a credential replacement, and sending relay progress reports. Walks that only count,
take a maximum, or remove entries independently of one another keep map order, because it cannot
change their result.

The Shuttle and Turmoil scheduler modes cannot be selected together; that build fails to compile
with a diagnostic naming both. The normal runtime dependency graph contains neither.

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

A peer advertises three endpoints independently: its interconnect endpoint as a host and port, and
its client and web-console endpoints as URLs. Discovery converges field by field, so each one is
either available, meaning the peer has published a value this node accepts, or unavailable. An
advertisement that has not arrived yet and one this node cannot read are the same unavailable state,
carry no reason, and are replaced by whatever a later round publishes. An unavailable endpoint is
never filled in from a default, another field, or an earlier value, so no consumer can rebuild an
address the peer has not advertised.

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

"Currently valid" is judged by the UTC clock of the local credential bundle. Production bundles use
the process wall clock. Rustls verification on both sides of the handshake and the transport's own
validity and expiry checks read that one clock, so they cannot disagree about a certificate.

The first HTTP/2 exchange binds the connection to its authenticated node, traffic class, advertised
endpoint, current process epoch, and wire-contract fingerprint. The receiver cross-checks those
claims against the TLS identity and the connection slot it is accepting. This prevents a valid peer
from relabeling a connection as a different node or traffic class.

Three identities serve different purposes:

- The certificate node identifier is the stable authenticated identity of a node.
- The discovery incarnation and endpoint generation identify the current cluster presence and
  advertised address of that node.
- The process epoch identifies one running interconnect process and fences in-memory delivery state
  across restarts. It is drawn from the transport's entropy when the transport binds, which in
  production is the operating system's secure random source.

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

A coordinated WASM guest-state reset uses the same authenticated coordination identity and adds its
typed reset scope to the entity-gate purpose. Engagement publishes a branch-selective relay fence:
one concrete fingerprint, the explicit unbranched instance, or all concrete branches. A retry must
match the complete domain, relay and entity set, purpose, and scope. It cannot widen a one-branch
hold or release another reset's hold.

An operator's NSPL reset enters through the ordinary typed session command protocol and ordered
transaction planner. The transaction step retains the command execution reference and exact scope;
the leader uses that reference when invoking this same coordinator request. A reconnect or a new
leader resumes the recorded step with the same identity. No separate public reset wire request or
JSON command path is introduced.

The management pool carries one typed coordinator request, which a node sends to the leader for a
reset that starts outside the ordered transaction path, such as a guest's request for a new lifetime
of its own branch. It carries the stable execution reference and exact reset target to the leader,
which runs the single control-plane operation. Its lower-level runtime requests have three actions.
`Prepare` reaches only the scheduled processor owner and creates fresh guest state while retaining
enough stopped branch state to abort before publication. `ActivateCommittedSchedule` reaches every
gate participant, applying the committed schedule locally to replicas before the owner writes and
replicates its initial checkpoint. This ordered activation does not join the ordinary cluster-wide
runtime-revision barrier: an owner whose initial checkpoint failed is the node that keeps that
barrier incomplete. The background runtime applicator still owns the node's prepared and ready
revision observations. `Abort` is valid only before publication and restores the retained branch
tasks. Responses carry the complete classified failure chain. A transport success proves only that
the receiver performed the requested action; the Raft-backed `Publishing` and `Ready` schedule
phases remain the durability and usability boundaries.

One more management request belongs to the same operation. A processor that declares
`ON REJECTED STATE RESET` raises a refused guest-state lifetime from the node that owns the branch,
and that node forwards it to the current leader as a typed recovery request naming the domain, the
processor, the refused branch or the explicit unbranched instance, the generation whose snapshot was
refused, and which of the two guest verdicts it gave. It carries no reset reference: the leader
derives one from that identity, so a request that is retried, forwarded again after a leadership
change, or raised by a new owner drives the very same coordinated reset instead of a second one. A
leader that finds the branch already past the reported generation answers that the lifetime is gone
rather than resetting the one that replaced it. The response carries the classified failure chain
and is not itself the durability boundary; the recovery's spent attempt and the reset's schedule
phases are.

The coordinator records every destination as an attempted participant before sending its prepare
request. A timeout, cancellation, or missing response after the destination persisted the request
therefore remains explicit cleanup work. A participant that refuses capture, preparation, forced
recovery preparation, confirmation, activation, or reconciliation answers with a rejection whose
text is its complete failure chain rather than only the outermost failure, so the coordinator
reports, for example, a destination WASM guest's classified restore failure with its stage, branch,
module, and saved state revision. Discard is exact and idempotent over the coordination
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
The subscription-interest visibility request includes the advertisement version, and its current
wire fingerprint fences that request shape during connection setup.

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

A connection stops granting stream leases the moment it begins to drain, whether it is retiring or
the transport is shutting down. A lease checks for the drain only after it holds its slot, so it
either returns that slot at once or was granted before the drain began and is one the drain waits
for. The drain therefore completes only after every leased slot has returned, and the connection
grants no further lease while it closes.

Memory and CPU execution are isolated by class:

| Class | Memory budget | CPU execution class |
| --- | ---: | --- |
| Management | 8 MiB | Control |
| Commands and replication | 24 MiB | Control |
| Relay | 192 MiB | Data |
| Bulk | 32 MiB | Bulk |

The total interconnect memory budget is 256 MiB. Reservations cannot borrow from another class. The
relay budget holds two worst-case operations, each consisting of a 32 MiB encoded body, a 32 MiB
decoded batch, and 16 MiB of decode scratch space. The commands and replication budget holds the
four decoded replication batches a follower keeps resident beside one batch being encoded, so a
follower whose receive window is full can still produce the answer that releases it. Startup
validates the relationships among operation limits and these budgets, including the paired work
that must fit for independent operations to keep making progress.

Stream slots are isolated per connection, and therefore per peer. Admission quotas, memory budgets,
and CPU wait queues are isolated per class and shared by every peer of the node. A slow or stalled
peer therefore holds at most the stream slots of its own connections. Once it holds the 32 shared
management streams of a connection, a further shared operation to that peer waits for a slot
until its own deadline, while liveness, cancellation, and discovery still use the reserved streams
of the same connection. Every connection to another peer keeps all of its slots. A stream whose
reader stops reading holds one HTTP/2 stream window of response bytes: flow control stops the
producer once that window is spent, and five seconds without progress resets the stream and returns
its admission and memory. A relay batch the receiving application holds without admitting keeps
the reservation of its exact body, the largest decoded batch, and decode scratch. Another channel
still receives grants and admission beside it, and cancelling the held batch returns the whole
reservation once the application drops the batch.

A CPU class admits each job separately: one of its workers runs, a bounded number wait, and the
next job is refused at once instead of joining an unbounded queue. A typed request runs several
jobs of its class in turn, encoding its payload and envelope and later decoding the response.
While a burst keeps the queue full, a request that was admitted for one job can therefore be
refused at its next one. A request refused at its first encode fails with `RequestError::Encode`,
and one refused at a later stage fails with `RequestError::Transport`. Every refused job holds no
memory charge, and every other class keeps admitting independently.

The [stalled-peer simulation](./interconnect-simulation.md#stalled-peer-isolation) checks these
bounds with exact values: a stalled peer and a healthy peer share one hub, and every observation of
the hub's pools, streams, admissions, worker queues, and memory budgets stays within its configured
bound until teardown releases them.

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

A typed request separates the operation's own outcome from the transport's. A handler that cannot
produce its value answers with one of four classes, and the asking node acts on the class rather
than on any text. **Rejected** means the answering node does not serve that subject at all, so a
caller working from a stale schedule resolves the owner again instead of retrying the same node.
**Unavailable** means the subject does not exist there. **Not ready** means it exists but cannot
answer yet, so the identical request can succeed later. **Failed** means the operation ran on the
answering node and lost.

Every class names the subject it refused: a domain, one entity of a domain, one kind of runtime
state held for an entity, or one subscriber's interest in a relay. The caller therefore reports what
failed without retaining the request it sent, and it never has to parse a message to decide what to
do. Only the failed class carries the answering node's own description, because a failure inside
another node's subsystem is opaque to the caller and that text exists for the operator reading it.
State snapshot exchange, dataflow node status, domain and entity drain status, entity gating, metric
description, relay, hash map and ingestor description, hash map queries, and subscription-interest
visibility all answer in these terms.

Each handler registration publishes a complete replacement dispatch table, so an arriving operation
finds its handler without taking a lock and registrations that race each other all take effect.
Registrations racing for one operation name within an exchange form publish it exactly once, and
every other one is rejected as already registered. Discovery likewise publishes the complete
live-node set at once, and the live-target check a typed request makes reads that set without a
lock. A request waiting for its target to leave registers for membership changes before it reads
that set, so a change published between the read and the wait still ends the wait. Until discovery
publishes its first set, every target counts as live so that bootstrap discovery can reach its
peers.

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

The [relay reconciliation and receiver-restart simulations](./interconnect-simulation.md#relay-reconciliation-and-cancellation)
check this boundary through the production authenticated connection. They lose the reply after an
Arrow batch reaches the receiver, reconnect to the same process or restart it, and require
reconciliation within this retention contract and an indeterminate result against a new process
epoch.

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
management-event bound, so discovery cannot allocate an arbitrary wire message. A node that cannot
take an exchange answers with a typed refusal rather than text: the message exceeds the gossip
bound, the sending node could not be registered as an outbound peer, or its gossip receiver has
shut down. A node that is shutting down closes its gossip transport before it stops gossip, so an
exchange still waiting on a peer that stopped first ends at once instead of holding shutdown until
its one-second deadline.

Admission to consensus membership requires an available interconnect endpoint. A discovered node
without one is not an admission candidate, so it is neither added as a learner nor promoted to
voter, and it becomes eligible on the round that publishes an endpoint this node accepts. The
client and web-console advertisements are independent of admission: a node joins, votes, and leads
with either of them unavailable. Membership follows a replaced endpoint by refreshing the learner
at its new address before promotion, and a node removed from membership stays out until it returns
with a newer incarnation.

A redirect to the leader names only the advertised endpoints discovery has established. A client
redirected during an election that has not yet observed the new leader's client endpoint receives
the leader identity without a redirect target rather than a guessed address, and retries until an
endpoint appears.

Terminal teardown closes the gossip exchange path before it asks the gossip loop to stop. The loop
reads its stop request only between rounds, and a round exchanges with each selected peer in turn
under a one-second request timeout. Closing the path first makes an exchange still waiting on a
peer that is itself stopping fail at once, instead of holding teardown for the rest of the round.

Session subscription interest also propagates through gossip. The key encoding is private to the
cluster layer: whenever the live-node state watcher changes, each node rebuilds an immutable index
from domain and relay to the interested node incarnations and advertisement versions and publishes
it through `ArcSwap`. A relay owner loads that snapshot and performs borrowed domain and relay
lookups, so per-batch remote fan-out neither formats a gossip key nor waits on the gossip mutex.
Subscription creation waits for the exact subscriber incarnation and at least the current interest
key's gossip version to appear in every live node's published index before it reports success.
The creating subscription holds its lease while capturing that version and waiting for visibility.
After withdrawal and reopening, an
advertisement from before the withdrawal cannot satisfy this handshake, even when the subscriber
node has not restarted. A withdrawal disappears from fan-out when the next gossip state snapshot is
published.

A node advertises interest in a relay exactly while at least one of its session subscriptions
holds a lease on it. Every subscription takes one lease before it attaches and releases it exactly
once, when it is withdrawn, abandoned before it was announced, or ended by its relay, so any
number of subscriptions from any number of sessions share one advertisement and the last release
withdraws it. Each write of the advertisement reads the lease count while it holds the gossip lock
that orders the writes, so the last write always matches the count: a release that finishes late
cannot withdraw the interest of a subscription that attached after it. The count per relay is
exported as `nervix_session_subscriptions`.

Consensus separates traffic according to the progress it protects:

- heartbeats, pre-votes, votes, leadership notifications, linearizable runtime-admission reads,
  and other small control exchanges use management capacity
- each leader-to-follower log uses one ordered duplex stream on the replication pool
- runtime-state replication and ownership handoff use the remaining replication capacity
- consensus snapshots use the snapshot reservation on the bulk pool

The ordered append stream can keep multiple batches in flight while preserving follower order. Its
consensus-level window bounds a follower to 16 outstanding batches and 16 MiB of unacknowledged log
data. Heartbeats and elections remain on management capacity, so a full append window does not block
leadership traffic.

Elections begin with a pre-vote exchange. A voter asks whether a quorum would accept its next term
before it persists that term or becomes a candidate. A restarted voter whose log is stale, or whose
discovery connections are not ready yet, therefore cannot advance the term and tear down the healthy
leader's replication streams. Pre-vote requests use their own typed management operation and apply
the same authenticated-origin check as vote requests.

A follower bounds the same stream from its own side. It keeps at most four decoded batches resident
at once, each charged to the commands and replication budget from the moment it is decoded until its
Raft core answers it, which happens only once the batch has been appended durably. A follower that
reaches the bound stops reading frames, so the leader's flow-control window closes and it stops
sending rather than growing the follower's memory. Decoded batches belonging to a stream the leader
has already torn down are released with that stream instead of staying queued behind it.

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
[Resource Versions And Bindings](./resource-versions.md#publication-and-transfer) defines when a
node fetches a resource archive and how it verifies and records the fetched version.

A runtime-state placement names exactly the state it addresses: the domain, entity, state kind, and
concrete branch; for every kind of state except branch-aggregated metrics and Kafka domain offsets,
the fingerprint of the schemas the state is laid out by; and for WASM processor guest state the
generation the committed schedule names for that branch. Branch-aggregated metrics and Kafka domain
offsets depend on no schema, so their placements carry no fingerprint and stay current across every
schema change of their entity. A checkpoint carries no identity of its own: a synchronization reply,
a handoff checkpoint, and a forced-recovery preparation each carry it beside the placement that
names it. A node answers a synchronization request, and acts on a checkpoint announcement or a
handoff checkpoint, only while the placement is current on that node, so an owner never serves, and
a replica never installs, state written under a replaced schema fingerprint or guest state of a
generation that has been replaced. A node that has not applied a schedule naming the entity has no
fingerprint to place its schema-bound state under, and refuses to place it rather than address the
state under an assumed identity. A replication acknowledgement counts only toward the placement it
names, so an acknowledgement for a replaced generation never satisfies the replica quorum of the
current one.
The schedule fingerprint an ownership handoff or forced recovery is bound to covers those schema
fingerprints and generations, so a preparation staged against an earlier schema or generation cannot
activate after a later one is committed.
Window state also binds to its current window model. A model replacement with unchanged schemas
therefore addresses a different checkpoint and cannot install rows accumulated under the preceding
window definition.

The same rule fences a coordinated reset. Once its `Publishing` schedule is committed, every
runtime-state request for the replaced WASM generation is stale even while the new initial
checkpoint is still being made durable. Replicas that were offline install the committed schedule
before accepting state, then synchronize only the new placement. A reset does not delete old bytes
through an unbounded cluster sweep; generation-addressed reads make them unreachable immediately,
and the existing bounded state-store retention removes them locally.

The leader forwards coordinated reset requests with a typed reason, so a guest request, operator
request, transaction effect, and rejected-snapshot recovery keep their provenance across nodes.
The existing remote describe exchange returns typed checkpoint facts from the execution owner:
generation, revisions, stage, and required and confirmed replica counts. The receiver combines
them with its scheduled binding, reset, and recovery facts, accepting only checkpoints of the
schedule's current generation. This read does not request synchronization, take ownership, or
change a checkpoint's completion state. A stored checkpoint whose previous replica boundary is
unknown is reported as such rather than treated as newly confirmed.

A replica acknowledges a branch-state checkpoint — WASM guest state, deduplicator and window state,
and the branch lifecycle that names the branches — only after it has written the checkpoint to its
own stable storage and synchronized it, never on receipt. A replica that already holds the announced
revision, or a newer one, synchronizes and acknowledges what it holds again, so an acknowledgement
lost in transit is replaced by the next announcement instead of stranding the owner. A node without
stable storage acknowledges nothing. The owner of a WASM processor branch releases the source
acknowledgements a guest checkpoint covers only once every replica the committed schedule assigns
has acknowledged that checkpoint's revision; a replica that is unreachable, lagging, or failing to
install stops those acknowledgements from being released rather than letting them through, and the
checkpoint fails after its ten-second deadline. The owner announces a WASM processor's new branch to
its replicas as soon as the branch appears. A replica that receives a checkpoint of a branch its
replicated branch lifecycle does not name yet first synchronizes the owner's branch lifecycle, and
refuses the checkpoint only when that lifecycle does not name the branch either, as for a branch the
owner has evicted. [WASM State And Recovery](./wasm-state.md#the-checkpoint) defines the checkpoint
these acknowledgements complete.
The owner publishes an empty final window checkpoint when it evicts a concrete window branch. A
replica that installs that revision replaces the evicted branch's rows and sketch panes with the
empty state. The branch lifecycle checkpoint records an incarnation for each concrete branch;
restoring a window checkpoint with a different incarnation starts an empty window. Reusing a branch
key after eviction therefore cannot attach a prior lifetime's retained rows, even when the earlier
checkpoint remains on a replica.
Window checkpoints carry a sealed container with separate bounded Arrow sections for retained
input and aggregate arguments, plus bounded typed sections for delayed histogram removals. Its
revision, row count, and branch incarnation are checked across the sections before restoration;
the ownership handoff and replica installation fences still govern whether the checkpoint can be
installed. A section that exceeds its bulk limit or disagrees with the container fails to open.
Sealing also refuses a container that cannot fit the available bulk memory reservation, instead
of waiting for a reservation larger than that budget.

Runtime-state synchronization replies and materialized-snapshot descriptions carry the shared
typed remote-operation failure envelope. Rejection, absence, temporary unreadiness, and execution
failure remain distinct across the node boundary, and the requester keeps that classification in
its local replication or snapshot-exchange error. Only an execution failure includes the serving
node's opaque diagnostic text. A materialized snapshot is streamed only after a successful typed
description identifies its exact length, digest, revision, fence, and branch generation; the
placement that the request names supplies its schema fingerprint.

A materialized dependency reader may observe the committed destination just before that node
activates its prepared state, or the previous destination just after it leaves the assignment. A
rejected, absent, or not-ready snapshot description in this handoff window means the dependency has
no available record for that read. The dependency policy then waits, skips, or supplies its declared
default; `REQUIRED WAIT` retains the batch and retries after routing, state, or bounded poll progress.
Execution failures and transport failures remain errors.

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

## HTTPS Listener Installation

Every node's HTTPS listener presents the TLS VHOST certificates of the runtime revision that node
applied, and it installs them before it reports that revision prepared. A command that creates,
changes, or drops a VHOST confirms the installation with the typed management request
`https_listener_installation`, which uses the reserved progress subquota and a two-second deadline.
The leader answers for itself in process and asks every other live process incarnation for its
latest installation at or after the command's runtime revision. The answer names the answering
incarnation and reports the installed revision, a failure with that node's own description, or that
the revision is still pending.

An answer from another incarnation, a transport failure, and a pending answer all leave that
incarnation pending, and it is asked again every 250 milliseconds until the command's completion
deadline. A failed installation ends the wait at once, so the command reports the failing node
without waiting for the deadline. The request reads installation state and changes nothing, so a
repeated or late request is harmless.
[The `DYNAMIC` TLS Refresh](./resource-versions.md#the-dynamic-tls-refresh) defines what every
listener presents and how a failed installation fails or rolls back the command.

## Application Health And Availability

An established HTTP/2 connection and a successful transport `PING` show that bytes can move; they
do not show that the peer application can accept work. Nervix therefore probes application health
through a typed management request with reserved liveness capacity.

A peer becomes a health target and an outbound target only while its interconnect endpoint is
available. One whose endpoint is unavailable is neither probed nor dialled, and its availability
stays unknown until discovery publishes an endpoint for it.

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

TLS loading identifies the CA certificate, node certificate, or node private key by kind and keeps
PEM parsing failures as typed categories. Error reports and watcher logs omit credential file paths
and malformed PEM input bytes.

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
waiters, and retires every pool connection the node opened: each stops leasing and closes once its
leased streams return. Connections that peers opened to the node close at once, together with the
handlers still serving their streams, so a peer's request the node has not answered fails instead
of completing. Whatever remains after ten seconds is then closed. Connection setup and incomplete
TLS handshakes remain inside this bound. A repeated `SIGINT` or `SIGTERM`, or the shutdown deadline
passing, ends the process without running the rest of its shutdown, so its peers observe its
connections ending exactly as they do when the process crashes.

See [Shutdown And Recovery](./shutdown.md) for the complete phase contract, the deadline and exit
statuses, and what each ending preserves.

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
domain-clock delivery and HTTPS listener installation probes as operation `progress`, so their
request counts, outcomes, latency, and quota failures can be evaluated independently.

Metric labels are bounded dimensions such as traffic class, direction, operation, outcome, and
reason. They do not include peer, domain, relay, branch, delivery identity, or payload values.
Per-batch and payload-bearing logs use debug or trace levels and do not expose sensitive field
values. See [Metrics And Observability](./metrics-and-observability.md) for the metric and logging
contract.
