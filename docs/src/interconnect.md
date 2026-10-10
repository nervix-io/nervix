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

For a transaction, a timed-out remote gate response may mean that engagement happened. The
control plane retains that uncertainty and any later recovery scope in the impact report;
[Transaction Quiescence And Impact Inspection](./transaction-quiescence.md) defines those
outcomes. This chapter owns the transport deadline and failure signal.

[Errors And Diagnostics](./errors-and-diagnostics.md) explains how local typed reports and remote
failure classes reach their callers and public edges. This chapter owns their wire representation
and transport failure semantics.

Clients never use this transport. [Client Session Protocol](./client-session-protocol.md) owns the
public client boundary: its listeners, authentication, FlatBuffers frames, request correlation,
command dispositions, and client recovery. The interconnect carries only node-to-node traffic,
including the subscription fan-out that feeds a client's Row frames.

Source-local compiler contracts distinguish peer/slot installation from recurring stream,
frame, admission and acknowledgement operations. Retained admission records and placement progress
name their bounded protocol key and transition bound. Transport selection reads immutable target
and connection publications. Delivery generations own remote ACK rows; authenticated peer owners
own relay protocol collections under short, peer-scoped transition guards.
A retained slot admits one worker through an atomic claim; established operations read that claim. The compiler's
contracts do not establish wire delivery or concurrency guarantees; those remain the protocols
and checks described here. [Data-Plane Concurrency](./data-plane-concurrency.md#source-contracts)
owns the compiler authoring contract.

## Simulation Boundary

The transport also runs, unchanged, inside a seeded Turmoil network simulation. That simulation is a
test harness outside product ownership, and
[Deterministic Interconnect Simulation](./interconnect-simulation.md) owns it: its build mode, the
fault model, supervision, replay and failure records, the scenario matrix, the commands and CI
budget, and the limits of what it establishes. The interconnect owns only the seams the simulation
plugs into. Each seam has one production behavior, which is what every node uses:

- **Sockets and name resolution.** The TCP listener and outbound streams are the sockets of the
  primitive boundary, `nervix_primitives::net`, which are Tokio's operating-system sockets, and
  peer names resolve through the node's own resolver, described in
  [Peer Name Resolution](#peer-name-resolution). In the dedicated `turmoil` test build the boundary
  selects Turmoil's simulated TCP, and the peer resolver answers from the simulated DNS table
  through the lookup the boundary offers in that build alone, never constructing the node
  resolver; TLS, HTTP/2, the envelope codec, and Arrow IPC above them are the same code in every
  build.
- **Certificate time.** Each credential bundle carries the one UTC clock its certificates are judged
  by, as described in [Peer Identity And Authentication](#peer-identity-and-authentication).
  Production bundles use the system clock.
- **Identity entropy.** The process epoch and relay grant identifiers are drawn from the transport's
  configured entropy, which in production is the operating system's secure random source.
- **Deadlines.** Every transport deadline is an instant of the boundary's clock,
  `nervix_primitives::time`, whose timers are Tokio's, so it follows the clock of the runtime it
  runs on: connection setup, request and progress timeouts, reconnect backoff, relay grant
  lifetimes, and the drain deadline derived from certificate expiry.
- **CPU work.** Encoding and decoding run through `nervix-execution`, which submits each admitted
  CPU job through the boundary's CPU-job mechanism. In the Turmoil build that mechanism runs the
  job as a task on the simulated scheduler, under the same admission, charge, and cancellation
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
peer. A bootstrap seed, configured or recovered, is the exception: the initiating node knows the
endpoint before it knows the remote node identifier, then obtains and authenticates that identifier
from the peer certificate. Once discovered, a peer is addressed by its authenticated identity
rather than by an unverified endpoint claim.

On restart, the node also uses the peer endpoints in its recovered Raft membership as gossip seeds,
excluding its own endpoint. These are contact hints, not current discovery advertisements: the
bootstrap exchange authenticates the answering node, and gossip then replaces the hint with that
node's current incarnation and advertised endpoint. This lets a former bootstrap node contact
survivors even when its deployment has no configured bootstrap host. A node with recovered peer
endpoints can also start when its configured bootstrap host does not resolve: it logs that lookup
failure and contacts the peers whose recovered endpoints do resolve.

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

The failure detector retains dead process identities separately from the live peer view. Explicit
Raft member removal uses the newest observed live or dead identity to fence the stopped process;
dead identities alone never make a peer eligible for membership admission. Scheduling can retain an
already established health target while gossip liveness lapses, as described below.

Connections are directed. Both nodes in a pair build their own outbound connections because some
operations, including relay acknowledgements and cluster events, travel back over the receiver's
outbound management connection. A single connection never changes traffic class after it has been
bound.

The standard pool layout is:

| Pool | Primary traffic | Outbound connections per peer | Streams per connection | Readiness |
| --- | --- | ---: | ---: | --- |
| Management | Membership, health, consensus control, clock progress, relay progress and outcomes | 1 | 64 | Preconnected |
| Commands | Control-plane and runtime requests | 1 | 32 | Preconnected |
| Replication | Consensus log entries, runtime-state descriptions and acknowledgements, ownership handoff control | 1 | 8 | Preconnected |
| Relay | Arrow record batches | 2 | 64 each | Preconnected |
| Bulk | Resources, runtime checkpoints and snapshots, consensus snapshots | 1 | 4 | On demand |

A peer is transport-ready only after all five preconnected outbound connections are live. Bulk
traffic does not delay readiness; its connection is opened when needed. The two relay connections
allow unrelated relay channels to make progress independently while preserving ordering within each
logical channel.

The built-in topology limit is 64 peers. A node admits at most 768 incoming and outgoing
connections in total and performs at most 32 connection handshakes concurrently. Those limits cover
the six possible connections in each direction for every peer: five preconnected connections plus
the on-demand bulk connection.

## Peer Name Resolution

A node resolves every host name the interconnect dials through the node's resolver.
[Name Resolution](./name-resolution.md) owns that resolver: its configuration options, the order in
which it answers literal addresses, hosts-file names and DNS names, its cache and TTL bounds, its
limit on concurrent lookups, its failures, and what it does not implement of the operating system's
name service. This section describes where and when the interconnect asks it, and what a failed
lookup means for a peer.

### Where The Interconnect Resolves

At startup a node resolves its own advertised interconnect endpoint, whose first address becomes
its gossip identity address, and its configured bootstrap endpoint, every address of which becomes a
gossip seed. Each lookup has the connection setup timeout, five seconds. Failure to resolve the
node's own endpoint fails startup with `failed to start cluster membership`. A configured bootstrap
endpoint must resolve when the node has no recovered Raft peer endpoints; otherwise its lookup
failure is logged at `warn` and skipped. An invalid configured bootstrap endpoint still fails
startup. The node also resolves the recovered Raft members' advertised endpoints in parallel. Their
successful answers become additional gossip seeds; an unavailable recovered endpoint is logged at
`warn` and skipped so it does not prevent the node from starting or using another reachable member.
These startup answers are not looked up again:
gossip keeps dialling the seed addresses they produced, and the gossip identity address stays the
one resolved at startup. Once gossip discovers a live peer, the interconnect replaces the recovery
hint with that peer's current advertised endpoint.

A discovered peer is registered at the interconnect endpoint it advertised, and every attempt to
open one of its pool connections resolves that endpoint again, inside the attempt's connection setup
deadline. The attempt dials the resolved addresses in order, giving each an equal share of the time
that remains, so an address that refuses or never answers leaves time for the next one. The
connection budget and ordered address-attempt policy are owned by `nervix-dns` and shared with
outbound connector transports; interconnect retains its socket and peer failure classification.
The first address that accepts carries the TLS handshake, HTTP/2 and the connection hello, inside
the same deadline; a failure there fails the attempt. The advertised host stays the TLS server
name, which the peer's certificate must name, and the authority of every request on the connection;
a literal IPv6 host is written in brackets there. A bootstrap exchange is the one exception: it
dials the exact seed address it was given, and the node it authenticates is dialled there until
discovery publishes the node's own endpoint.

Pool connections are keyed by the advertised endpoint rather than by an address, so a changed answer
or an expired TTL never retires an established connection. Only the next connection attempt, after
a connection ends, uses the new answer once the resolver's cached one has expired. A changed
advertised host or port is a different endpoint and retires the old one, as described in
[Connection And Credential Lifecycle](#connection-and-credential-lifecycle).

The connection setup deadline bounds each lookup, and the pool slot's reconnect backoff, described
in [Connection And Credential Lifecycle](#connection-and-credential-lifecycle), is the only retry
around it, so DNS retries never multiply the transport's own. A failed lookup is a connection setup failure: the slot retries with its
backoff, the attempt is counted with reason `resolution` in
`nervix_interconnect_connection_failures_total`, and the failure is logged at `debug` as
`interconnect pool connection failed` with the peer's node, endpoint, pool class and slot. Only
outbound pool connections are counted; the startup lookups and the bootstrap exchange are not. A
peer whose name does not resolve stays a health target, and its probes fail as they would for a peer
that cannot be reached.

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
  advertised address of that node. The incarnation also names the run of a node that registered a
  relay admission or record acknowledgement, as
  [Acknowledgement Registrations](#acknowledgement-registrations) describes.
- The process epoch identifies one running interconnect process and fences in-memory delivery state
  across restarts. It is drawn from the transport's entropy when the transport binds, which in
  production is the operating system's secure random source.

Replacing an endpoint, restarting a process, and rotating a certificate therefore have distinct
meanings even when the stable node identifier does not change.

Coordination operations use a typed identity composed of the authenticated coordinator node, its
current process epoch, and a process-local sequence. The sequence begins independently in every
process; the node and process epoch make equal sequence values distinct across concurrent leaders
and restarts. For every coordination request, the receiver verifies the node and process epoch
against the bound connection before the application handler can observe the request or open a
coordinated response stream. A process
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

A model alteration that includes an ingestor and shared downstream relay uses two entity-gate
operations. An intake-only scope, with no relay gates, first suspends the affected ingestors on
every node and drains their admitted work. The full subgraph operation then engages before that
intake operation is released. Each operation has its own authenticated coordination identity and
exact scope, and both use the same alteration deadline. Status, retries, receiver-owned release,
and lease expiry retain the normal identity checks throughout this overlap. The coordinator does
not change an existing operation's scope to advance the drain.

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

A stopping node moves its own scheduled work with one typed commands-pool request to the current
leader, `stopping_node_drain`. It names no node and carries one of two actions: drain, which cordons
the sender and moves its scheduled work through planned ownership handoffs, and release, which
clears the cordon that drain set. The leader acts for the node the authenticated connection belongs
to, so the certificate that admitted the connection is the whole authorization: a node can drain
and uncordon only itself, and no user credential takes part. The answer is completed or failed, each
with the leader's account of the action for the stopping node's log, or not the leader, from a node
that changed nothing and leaves the sender to ask the leader it observes next. A leader that loses
its leadership while it writes a release also answers not the leader, because clearing a cordon
again through the next leader is harmless. The leader runs the action in a service task of its own,
so a drain that has begun finishes, and releases what it holds, even when the sender's deadline
abandons the request first. For a drain that deadline is what remains of the sender's drain
timeout; for a release it is the bound
[Releasing The Drain Cordon](./shutdown.md#releasing-the-drain-cordon) states.
[Topology Cases](./shutdown.md#topology-cases) owns when a node sends it.

## Wire Contract And Payloads

All nodes in a running cluster use one current wire contract. A fixed fingerprint covers the set of
supported operations and their encoded shapes. A fingerprint mismatch rejects connection setup;
there is no version negotiation or alternate decoding path. A wire-contract change therefore
requires a coordinated cluster stop and start with all nodes on the same version.
The subscription-interest visibility request includes the advertisement version, and its current
wire fingerprint fences that request shape during connection setup.

The fingerprint also covers fixed-width 64-bit counts in Models, transaction commands and records,
and WASM inspection results. These fields use the vocabulary's `CountAsU64` adapter and checked
native decoding; an unrepresentable count fails decoding. Window processor state has current
runtime-state kind tag `8`, and its bulk snapshot codec validates the `NVXWIN64` frame signature
before decoding histogram delayed-removal bucket indices in the same count representation.

Control records use bounded `rkyv` archives. The receiver validates an archive, including its shape
and nesting depth, before exposing it to an operation handler. Encoded and decoded memory is charged
to the traffic class before decoding begins. Unknown operations, a pool mismatch, malformed
archives, and values above the operation limit fail at the transport boundary.

Validation checks an archive's shape. Semantic values such as typed names are checked when the
record is read back, so a record can be refused after part of it was read. The shared archive
reader owns each completed list or fixed-array element while reading the next, and releases the
initialized prefix and the list, boxed, or shared allocation if a later element is refused. A
relay grant request whose acknowledgement registrations include a registrar that is no node's name
is refused with everything already read freed. The operation handler receives no part of that
request.

Control-operation responses preserve a typed failure class and subject across the wire. A receiver
can distinguish a node that rejects ownership, an unavailable subject, a subject that is not ready,
and an operation that ran and failed without parsing display text. Only the final class carries an
operator-facing reason; callers decide retry and relocation from the class and subject.

Relay metadata uses the same validated control encoding, while relay bodies remain Arrow IPC from
the source relay to the destination runtime. Bulk operations transfer opaque byte chunks and let
the owning resource, snapshot, or state protocol interpret the stream.

Before the receiver gives a relay Arrow stream to Arrow's reader, the shared IPC framing owner
checks every message's continuation marker, metadata, declared body and column buffers against the
received bytes. A malformed stream reports `ArrowBodyError::Framing` before the reader can allocate
or slice from an unchecked length. A reader panic on malformed metadata that passes framing is
reported as `ArrowBodyError::Decode`.

The primary payload limits are:

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

A receiver also holds an Arrow body to its own framing before it decodes it. The body is one
canonical IPC stream: every message opens with the continuation marker, and the end-of-stream
marker ends the body. Every length the stream declares must lie within the bytes the body carries:
each message's metadata, its body, and each column buffer inside that body. A body that is framed
otherwise is refused as misframed, so a declared length never sizes an allocation the body does not
back, and a column buffer is never sliced outside its message. The same scan refuses, as
undecodable, a body whose schema declares a field type Nervix does not carry, a dictionary encoding
among them, or whose record batch is at odds with that schema. It opens every Arrow section of a
sealed snapshot, a checkpoint or a backup archive.

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

Leasing an established stream allocates nothing and acquires no shared discovery map. An immutable
persistent target table selects the peer's fixed pool arrays. Each slot retains its cancellation
lifetime, one atomic worker claim and an `ArcSwapOption` containing the current authenticated
connection. A lease retains that exact connection and takes its existing pool/subquota reservation.
Only a first use can win the worker claim, including on-demand bulk. The worker clears its connection
publication on loss and publishes a fresh connection after authenticated reconnect. The cold
connection registry remains available for registration, statistics and exact teardown; predecessor
teardown removes only its own allocation.

Endpoint or TLS replacement cancels the preceding slots before publishing new fixed arrays. Peer
retirement therefore cannot expose a replacement connection through a stale slot. A dial-address
update for an unchanged endpoint preserves slot identity and affects only the next connection
attempt. Such an update retains the fixed arrays' shared owner under its new target wrapper, so a
concurrent peer withdrawal still withdraws that exact pool lifetime. A recreated pool has a distinct
owner, even at the same endpoint, and survives a predecessor's withdrawal. The fixed array scan
retains round-robin selection and each class's existing capacities.

An operation that finds every stream of its class and subquota to a peer leased waits for one to be
released, within its request deadline. Each peer keeps one wakeup for each class and subquota, and
a released stream wakes a waiter of exactly its own class and subquota. A single wakeup shared by
every waiter could reach one that cannot use the released slot, while the waiter that can use it
slept on until its deadline. The waiter registers for that wakeup, and for any change of the peer's
connections, before it checks the pools a second time, so a release between its two checks still
wakes it. The wakeups belong to the peer rather than to one of its targets, so a waiter of a target
that was replaced meanwhile still hears a release of the replacement.

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
   reader cancels the stream and releases its reservations. The producer's opening callback and
   each chunk carry a local `StreamHandlerError` report; an admission, storage, or encoding cause
   stays beneath that handler context until the response reaches the wire boundary.
4. **Ordered duplex stream.** Each direction sends length-prefixed, validated frames in order and
   may close independently. Opening the stream is bounded by the operation's declared setup
   deadline. An idle established stream is valid; the owning protocol sets deadlines for answers it
   is awaiting. The initiator's sender reports when the peer's flow control last accepted its
   bytes, so that protocol can tell a slow answer from a peer that accepts nothing. Receiving is
   cancel-safe on both ends: a frame is decoded under the pool's memory and CPU admission after it
   leaves the stream, and a receive its caller abandons, because a timer or command it selects
   against won, leaves that decoding to the next receive, which finishes the frame before it reads
   another. No abandoned receive loses a frame or reorders one. Consensus append traffic,
   [client producer links](#client-producer-links) and
   [client consumer streams](#client-consumer-streams) use this form.
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
6. The application lane validates the body's Arrow IPC stream, which must hold a schema message
   of the field types Nervix carries, one uncompressed record batch that declares the field nodes
   and buffers that schema's fields take with every buffer inside its body, and the end-of-stream
   marker, and then the exact schema, metadata, branch, and record-acknowledgement count, then
   resolves the configured concrete runtime branch. Work for one channel remains ordered,
   while other channels continue independently. A payload that fails this validation never reaches
   the branch. The receiver keeps the failure as a report of `RuntimeError::DecodeRemoteRelay`
   with the typed `RemoteRelayDecodeError` beneath it, which names what the payload got wrong and
   keeps the decoder's own failure beneath that, and answers the payload's admission registration
   negatively with the whole chain rendered as the reason. A payload that carries no admission
   registration is refused the same way, with no registration to answer.
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
stalled exchange and bounds the total admission wait at five minutes. Once the delivery is admitted,
the sender waits for an attached record acknowledgement only while the receiver keeps reporting it,
and fails one the receiver reports nothing about for fifteen seconds, as
[Acknowledgement Registrations](#acknowledgement-registrations) describes. Progress keeps a live
attempt from being mistaken for a disconnected one; it does not change the delivery outcome.
For an attached acknowledgement, progress also carries a monotonic sequence and whether all of its
remaining handoff shares are parked, on `REQUIRED WAIT` or in a window that retains their rows. Each upstream node parks or reactivates its
own attached share in sequence order, so a domain drain excludes a parked chain across relay hops.
The eventual terminal acknowledgement still resolves every share; parking does not acknowledge the
source or persist an acknowledgement. Admission progress carries no parked state.

### Acknowledgement Registrations

The sender correlates the runtime admission of a delivery and every attached record acknowledgement
it waits on through a registration it places in the delivery. A registration names the waiting
entry by a number and names the run of the sending node that registered it: the node and the
discovery incarnation of that run. Every process numbers its registrations from one, so the number
alone repeats across restarts of a node, and the run is what keeps the registrations of different
runs apart.

The receiver returns every progress event and terminal outcome to the node the registration names,
carrying the whole registration back with it. The sending node resolves an entry only when the
registration names its current run. A receiver can still be resolving what an earlier run
registered after that run ended: a record acknowledgement whose downstream work completes after the
sending node restarted is the common case. Such an outcome is rejected and logged at `debug`. It
never resolves the admission or record acknowledgement the current run holds under the same number,
so a success reported for an earlier run can never acknowledge a record, and so commit it at its
source, before its own delivery completed. The earlier run's entries ended with its process, and
its sources redeliver the records they had not committed.

A receiver issues a grant only when the admission registration names the authenticated sending
node. Admission bookkeeping on both ends is keyed by the peer and the whole registration, so a
terminal outcome addressed to an earlier run neither completes nor retires the admission a later
run registered under the same number.

### Record Acknowledgements The Receiver Stops Reporting

A receiver reports every record acknowledgement it holds to the node that registered it. While the
downstream work the acknowledgement stands for continues, it sends a report 100 milliseconds after
its previous one was delivered or given up, and when the work completes it sends the terminal
outcome once. Each report and the outcome is one bounded event that the receiver retries for up to
five seconds and then gives up. Once the outcome is given up, or the receiver's run ends, nothing
reports that acknowledgement again.

The registering node therefore waits for an acknowledgement only while the receiver keeps reporting
it. From the moment the delivery that carries the acknowledgement is admitted, a sweep once a second
counts the passes in which the receiver reported nothing about it, and fails the acknowledgement
once fifteen seconds of such passes have gone by. The bound outlasts two consecutive reports that
each exhaust their five-second deadline, so a receiver that is still working on the record is not
mistaken for one that stopped. Before admission, the delivery guard resolves cancelled work and
the sweep bounds an abandoned registration at the five-minute total admission wait. The sweep counts its own passes rather
than elapsed time, so a registering node whose own execution stalled, such as a paused container,
does not fail acknowledgements whose reports it could not receive meanwhile.

A failed acknowledgement resolves negatively exactly once. The sweep and report transitions use
the exact delivery generation's guard, so a report that arrives first keeps the acknowledgement
pending, and a terminal outcome or the delivery's own failure that resolves it first leaves the
sweep nothing to fail. The source attempt fails with it and redelivers the record along the current
routes, so a sink that already completed the record can receive it again. A report or outcome that
arrives after its acknowledgement failed finds no entry and is rejected at `debug`.

Every node on a record's path applies the same bound. A relay owner that routed a record to the
node of an attached consumer reports the record alive to the source's node for as long as the
record's acknowledgement tree is unresolved. Without the bound, a terminal outcome lost between the
consumer's node and the relay owner would keep the source's acknowledgement alive, and its
`ACK TIMEOUT` from ever passing, for the rest of the relay owner's run. With it, the relay owner
fails the acknowledgement fifteen seconds after the consumer's node fell silent, stops reporting
the record, and the source's retry takes over. Each sweep that failed acknowledgements logs, at
`warn`, one line per receiver with the number it failed.

### Bounded Correlation And Peer Owners

The sender has 8,192 delivery positions and a separate 8,192 admission positions. One delivery
owns all its record rows, with independent outcomes; filling delivery capacity therefore leaves
room to register its runtime admission. The opaque acknowledgement number encodes a position,
generation and row. Resolution validates all three, together with the registrar's full discovery
identity. An exhausted generation is sealed permanently. A delayed report, terminal reply or
cleanup cannot change a replacement occupying the same position.
Unused positions are claimed on demand; only retired positions enter the bounded free queues.
Shutdown seals fresh claims before scanning positions that were ever claimed, so a concurrent
registrar either occupies a position shutdown visits or finds it closed.

Record storage and receiver ACK watches are charged to the relay memory budget. One task per
admitted batch multiplexes its row watches, with a fixed charge per row plus one task charge; a
wide frame therefore does not allocate one task per acknowledgement. Pending rows report progress
every 100 milliseconds, below the registrar's fifteen-second silence bound even when two reports
each exhaust their dispatch deadline. The same cadence keeps local emitter and message-error
acknowledgements alive while a connector request is pending, giving a one-second source
`ACK TIMEOUT` multiple chances to observe progress. Admission
refusal is typed and occurs before runtime admission. Cancellation before admission resolves every
held share negatively and returns its position. Completion of the last record returns the delivery
storage and its charge. Watcher memory remains charged through its dispatch attempts and ends on
completion, runtime shutdown, or the registrar run leaving or changing. The membership writer
publishes immutable process identities; a watcher gives initial discovery five seconds and stops
once a previously observed registrar is absent. Terminal delivery retains the five-second event
deadline described above; this does not promise suppression of replay duplicates after lost outcomes.

An authenticated connection retains its peer's protocol owner. That owner alone mutates ordinary
grant, attempt, channel, admission and outbound-correlation collections. Each synchronous guard
is scoped to that peer, is released before transport waits, and follows peer then record lock
order. Runtime admission and cancellation use one irreversible atomic verdict. Peer removal or
epoch replacement cancels unadmitted records and releases transport item and terminal permits,
including when runtime still borrows an admitted intake. Its admitted verdict remains valid.
Decoded metadata keeps its memory charge until its last borrower releases it.

With incoming queue capacity `Q`, each peer holds at most `Q` grants, attempts, active channels and
admission records, at most `2Q` channel watermarks, and at most `2Q` outbound attempts and admission
correlations. New unrelated channels are refused when retained watermarks occupy their budget;
they are never evicted early to admit a replay. Lost terminal replies remain reconcilable. A
sixty-second sweep reclaims protocol state after ten minutes without progress or reconciliation;
live admission reports renew retained records. Authentication before gossip membership permits
discovery, while relay operations wait for live membership. An owner still awaiting membership
ends after ten minutes and closes its bound intake connections. Routing publication has at most
the configured peer limit; normal frame and ACK operations use retained owners rather than a
node-wide shared-map guard.

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
reconcile. A consultation refreshes retention under that peer's protocol guard, together with its
sequence decision. Independent peers have independent guards.

The [relay reconciliation and receiver-restart simulations](./interconnect-simulation.md#relay-reconciliation-and-cancellation)
check this boundary through the production authenticated connection. They lose the reply after an
Arrow batch reaches the receiver, reconnect to the same process or restart it, and require
reconciliation within this retention contract and an indeterminate result against a new process
epoch.

Each attempt carries the channel and admission identities it was granted under. Once the receiver
has delivered an attempt's terminal outcome, it retires that same attempt: it advances the channel
watermark and releases the attempt, its channel occupancy, and its admission without rebuilding
either identity or looking the admission up again.

Relay attempts, grants, watermarks, admission state, and record-acknowledgement owners are in-memory
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

### Consumers That Leave The Receiver

The relay owner also fences the start of each buffered batch's fan-out against its local schedule
swap. It acquires a dispatch permit before reading the local and remote consumer sets, and keeps
that permit through fan-out, attachment of the consumer ACK shares, and resolution of the owner's
share. A swap closes the gate
and waits for existing permits before changing those sets. If the gate is already closed when a
buffered batch starts fan-out, the owner fails its record acknowledgements and the source retries
after the schedule changes. It cannot wait for the gate while holding that buffered batch: the
swap's drain counts the batch, so such a wait would prevent the swap from finishing. In particular,
an attached sibling on the old destination cannot acknowledge a batch while another attached
consumer moves to a new destination before the owner selected routes for that batch.

The receiver decides admission before it hands the batch to the runtime consumers of the relay, and
it hands the batch to the consumers that run on the node at that moment. An owner routes an attached
batch to a node because its schedule places an attached consumer of the relay there. That consumer
can leave the node after the owner routed the batch: a forced recovery moves every runtime node off
a node that application health or gossip judged unavailable, including one that is still running,
and the node stops the moved consumer when it applies the published schedule. The gap between
admission and dispatch can be long: the receiver sends the terminal admission outcome to the owner
before it dispatches the batch, and that send can take up to its five-second deadline while the
owner is unreachable.

A routed batch that carries record acknowledgements and finds no attached consumer of its relay on
the receiving node fails those acknowledgements. Its admission stands, so the transport does not
send it again, but the owner receives a failed record acknowledgement and fails its source attempt.
The source then redelivers the record along the owner's current routes. The receiver never reports
an attached record acknowledged when no attached consumer took the batch, so an attached record
cannot be committed at its source without a consumer having completed it. A batch without record
acknowledgements, such as a detached send, completes at admission as before.

## Membership, Consensus, And Bulk Transfer

Cluster membership gossip uses management discovery capacity. It discovers topology and
incarnations but does not replace application health checks. Gossip payloads remain below the
management-event bound, so discovery cannot allocate an arbitrary wire message. A node that cannot
take an exchange answers with a typed refusal rather than text: the message exceeds the gossip
bound, the sending node could not be registered as an outbound peer, or its gossip receiver has
shut down. Chitchat hands each outgoing datagram to a four-message queue for that destination. One
worker per destination drives its interconnect requests in order, under the one-second request
deadline; an unreachable peer therefore cannot hold the gossip loop while it receives from or sends
to healthy peers. A full destination queue drops its newest datagram, and the next gossip round
retries. Closing the transport cancels queued work and exchanges in flight.

Each node's Chitchat live set is its own failure-detector estimate. An isolated peer can remain
listed live until its missing heartbeats are observed, even after its links stop carrying requests;
application probes provide the separate signal that eventually makes its work eligible for failover.

Chitchat continues to select known dead peers for exchanges during its 24-hour dead-node retention
period. When application health has retired one of those peers from the outbound pool, a gossip
exchange reinstalls its known route before sending. The replacement connection still authenticates
the peer's node identity and advertised endpoint. This lets a node with no configured bootstrap
seed contact retained peers again after a partition heals; the returning exchange restores gossip
membership, after which application health and Raft reconciliation use the current advertisements.

Admission to consensus membership requires an available interconnect endpoint. A discovered node
without one is not an admission candidate, so it is neither added as a learner nor promoted to
voter, and it becomes eligible on the round that publishes an endpoint this node accepts. The
client and web-console advertisements are independent of admission: a node joins, votes, and leads
with either of them unavailable. Membership replaces the recorded address of an existing member
when its advertised endpoint changes. A returning voter remains a voter, while a learner still
waits to catch up before promotion. The recorded address is a startup contact hint; live Raft
traffic uses authenticated discovery by node identity. A node removed from membership stays out
until it returns with a newer incarnation.

A redirect to the leader names only the advertised endpoints discovery has established. A client
redirected during an election that has not yet observed the new leader's client endpoint receives
the leader identity without a redirect target rather than a guessed address, and retries until an
endpoint appears. [Leader Discovery, Redirect, And
Reconnect](./client-session-protocol.md#leader-discovery-redirect-and-reconnect) defines how
clients follow it.

Terminal teardown closes the gossip exchange path before it asks the gossip loop to stop. Closing
the path first cancels destination workers and their queued or in-flight requests, so a peer that is
itself stopping cannot hold teardown until an exchange deadline.

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
exported as `nervix_session_subscriptions`. [Row
Subscriptions](./client-session-protocol.md#row-subscriptions) defines what the subscriber's node
does with the batches this fan-out delivers.

Consensus separates traffic according to the progress it protects:

- heartbeats, pre-votes, votes, leadership notifications, linearizable runtime-admission reads,
  and other small control exchanges use management capacity
- each leader-to-follower log uses one ordered duplex stream on the replication pool
- runtime-state descriptions, catalog listings, acknowledgements and ownership handoff control use
  the remaining replication capacity; checkpoint bodies use the bulk pool's Snapshot subquota
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

Backup state capture uses typed drain, capture, inventory, and fetch operations. The leader reads
each node's admitted work through a management-class drain request and requests a separate
confirming force-flush round after all nodes appear quiet. The leader sends a management-class capture
request with its coordination identity and applied cut revision to each live node, then reads each
node's management-class inventory of staged sections. A capture request's deadline is the remaining
budget of the domain's cut rather than a fixed request deadline, because an owner stages its share of
the domain's state before it answers; a `WITHOUT PAUSE` capture takes the default cut budget. A receiver waits up to five seconds for its
state machine to apply that revision, installs its current runtime plan, and rechecks the sending
leader before it captures. A closed applied-state authority or a catch-up deadline refuses the
capture; a follower that is still applying the cut does not produce an archive from earlier state.
A captured section is fetched over a
snapshot-subquota bulk response: the inventory declares its path, length, content kind and digest,
and the leader stages and verifies the stream before adding it to the archive. The owner keeps a
staged section only for the coordinator process that requested it and releases expired stages.
The fetch stream authenticates that process identity before its handler can consume the stage; a
partitioned or cancelled fetch leaves any unconsumed stage available until expiry.
An owner streams each branch lifecycle and Kafka offset section from the cut's database snapshot
into its staged file under a `restore_metadata` conversion charge and a 64 KiB bulk buffer, so the
inventory declares a section's exact length and digest without the owner holding the section
whole. Materialized inventories distinguish archive descriptors, scalar identity groups and Arrow
column groups. Their bounded sections are staged from a fresh assignment-qualified capture and
fetched through the same authenticated stream; the capture never transfers an entire container
as one bulk response allocation. The leader verifies each length and digest before assembly.
An opening refused solely for request capacity is retried within one 30-second opening deadline;
other failures end the fetch. The captured stage remains unconsumed on an admission refusal.
Completed backup and materialized-state response streams release their Snapshot request and
connection-stream permits before local file verification or Arrow decoding.

The coordinator streams native lifecycle and Kafka metadata into quota-owned files before
installation. It retains the verified description's separate `restore_metadata` preparation
charge through conversion, while buffered file I/O reserves 2 MiB of bulk memory. Complete
encoded records are never copied into a local request or retained for remote transmission.
Native checkpoints and guest saves share the file source and bounded chunk path. See
[Backup And Restore](backup-and-restore.md#restoring) for preparation admission and its limits.

Restore state installation uses the snapshot bulk subquota after the stopped-domain schedule is
published. The leader admits a replicated installation authority carrying its identity and term,
the restore execution, mutation lease revision and installation generation. Every request carries
that authority. A receiver waits for its generation to apply and authenticates the sending leader.
A begin request declares placement, length and digest; chunks are ordered and at most 64 KiB;
finish verifies the staged file, then reads it directly inside a filesystem storage job that
reserves 2 MiB and stages bounded checkpoint chunks into an invisible installation namespace.
It retains the upload's disk-quota owner through that job. There is no full checkpoint buffer or nested
staged-reader reservation. Incomplete transfers expire under the node's staging quota.
Materialized restoration first converts archive-owned identities and exact-schema Arrow sections
to a staged native sealed file. That file uses these same begin/chunk/finish requests, preserving
its revision and branch generation and establishing its ownership fence in the new cluster.
For a stopped domain, owner capture selects materialized checkpoint readers from the same database
snapshot as the other captured state. Each reader retains its selected namespace and chunks until
bounded identity conversion and raw Arrow staging finish. Running and paused domains use fresh generation
capture under their assignment barriers. The capture inventory carries both through the existing
materialized section kinds and Snapshot stream contract. Stored capture never activates the domain.

Large materialized containers never use the encoded metadata install request.

After all checkpoints are staged, the leader sends each target node a publish request with the
complete checkpoint and byte counts. The receiver reserves a fixed 2 MiB, validates receipts,
headers and chunk digests one checkpoint at a time, synchronizes the generation data, and commits
and synchronizes one active-generation pointer. It then deletes obsolete keys in bounded batches.
The inventory size and total payload bytes do not determine the publication reservation.
An empty inventory clears unassigned nodes and implements configuration-only restoration. Local
and remote mutations revalidate the exact authority under the applied-state read guard, held
through the storage mutation and clearing of runtime handles. Publication of a new authority or
release of the replicated start gate requires the corresponding write guard, so a delayed
coordinator cannot mutate after its successor completes installation. The store also retains the
published authority and inventory to reject lower or competing generations. An exact retry must
carry the same counts and repeats durability and cleanup before acknowledging completion.
A failure after pointer commit can leave that complete generation selected with the start gate
closed; it cannot authorize START or clear handles before durable completion.
The domain's replicated start gate is released only after all nodes acknowledge publication.
`RESUME` activates its archived lifecycle in that same completion effect; delayed coordinators
are refused once the domain is running. The transfer authority remains independent of the
archived domain start generation and the relay checkpoint revision.
The seeded transport checks stream six 6 MiB sections, above the default 32 MiB bulk budget,
through capture fetch and ordered state installation after link repair. A second check restarts
the receiving process after the first 64 KiB install chunk: its replacement refuses an incomplete
finish and publishes only after a complete new 36 MiB transfer. The handlers retain counts and
digests; these checks qualify the authenticated transport, while the public restore scenarios
and storage checks qualify native containers, atomic publication and activation.
Capture fetch and state installation share the peer connection's one reserved snapshot stream
slot. The archive staging phase finishes and releases its fetch stream before installation starts;
an installer cannot retain a fetch stream while awaiting another snapshot request to that peer.
Staging and publication run on the admitted filesystem worker class. Authority is checked inside
the storage job after admission, so waiting for a worker cannot preserve an expired installation
right. Durable synchronization does not run on the async reactor.

A runtime-state placement names exactly the state it addresses: the domain, entity, state kind, and
concrete branch; for every kind of state except branch-aggregated metrics and Kafka domain offsets,
the fingerprint of the schemas the state is laid out by; and for WASM processor guest state the
generation the committed schedule names for that branch. Branch-aggregated metrics and Kafka domain
offsets depend on no schema, so their placements carry no fingerprint and stay current across every
schema change of their entity. A backup archive separately records the Kafka ingestor's schema
fingerprint and checks it against the restore target before installing offsets. A checkpoint carries
no identity of its own: a synchronization reply,
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
activate after a later one is committed. The control plane computes the exact fingerprint of the
committed schedule before runtime installation and carries it in the complete typed execution
revision used for activation and reconciliation; a node does not reconstruct it from a second
schedule view.
WASM handoff and forced-recovery preparation on a passive revision validate and retain the complete
checkpoint inventory without running guest callbacks or reading the stopped domain clock. The
guest validates its saved bytes when `START` installs the active revision, so stopped time is not
classified as missing checkpoint state.
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
checkpoint fails after its ninety-second deadline. The owner announces a WASM processor's new branch to
its replicas as soon as the branch appears. Every catch-up round of a replica synchronizes the
owner's branch lifecycle before it installs any branch checkpoint, and the replica refuses a
checkpoint of a branch that lifecycle does not name, as for a branch the owner has evicted.
[WASM State And Recovery](./wasm-state.md#the-checkpoint) defines the checkpoint these
acknowledgements complete.

A replica catches the branch-keyed entities it replicates up in rounds, one replica task for each
entity, through two replication-class request kinds and, when a checkpoint advanced, a bulk stream.
A state synchronization request names one placement and the revision the replica holds of it. The
owner answers through the actual state handle published when that placement was installed: with
the newer checkpoint's revision, length and BLAKE3 digest, or with nothing when it is current.
The owner reads storage only when it holds no live state for the placement. The replica opens a
Snapshot-subquota bulk stream for the described revision; the owner refuses a revision it no longer
holds. Chunks are at most the configured bulk chunk size (64 KiB by default). Each side admits the
whole captured or received allocation against `restore_metadata`, up to a 256 MiB checkpoint
transfer bound. The replica checks the stream's declared length, received length and digest before
the checkpoint can reach its assignment-fenced installation and stable-storage acknowledgement.
Debug events name the placement, revision and declared length when streaming and after verification,
without logging checkpoint bytes. A branch checkpoint listing request names the entity's branch
lifecycle placement and the cursor the replica's previous listing
returned. The owner answers with the next changes of its catalog of the entity's branch
checkpoints, in the order they happened and at most 256 of them: each branch whose checkpoint
changed, with the state that checkpoint belongs to and its newest revision, and each branch whose
state went away, followed by the cursor after them and whether more changes follow. The listing
restarts from the catalog's beginning when the replica has no cursor, when its cursor belongs to
another catalog, such as one a replaced or restarted owner kept, and when it is older than the
oldest removal the catalog kept. An owner that holds no branch state of the entity answers that it
holds none. Both requests are answered only while the answering node is assigned the placement. Admission
reads the retained entity assignment from the immutable routing publication; state and catalog
selection never reads the execution, identity or replicated-state registries. Route withdrawal
ends the exact state handle before replacement, while an already admitted request may finish on
the state it borrowed. A
round synchronizes the lifecycle, reads the catalog's changes, and requests only the checkpoints of
the branches that changed or were announced, so a round in which no branch changed sends two
requests however many branches the entity has.

Checkpoint descriptions and catalog listings each use their operation's five-second deadline,
including admission, connection capacity, the answer and decoding. A selected checkpoint's bulk
stream has a sixty-second progress deadline. The one-second replication poll
interval schedules the next idle round; it does not shorten an in-flight request's deadline. A
branch-aggregated replica uses the same checkpoint operation deadline. Polls and announcements retry
a failed exchange, while the owner's checkpoint completion deadline remains independent and may
fail if a replica cannot confirm in time. Replica request diagnostics retain the typed transport
cause beneath the target and placement context, so a timeout can be distinguished from capacity,
connection and framing failures without inspecting checkpoint bytes.

The owner of a placement offers its newest checkpoint to the replicas the committed schedule
assigns, and repeats the offer every 100 milliseconds to each replica that has not acknowledged
that revision, until every one of them has, the node stops being the placement's primary, the
placement's state goes away, or the node stops. A newer checkpoint taken meanwhile raises the
revision on offer instead of starting a second offer. Each announcer retains the installed route
and reads its primary and replica set from the same assignment slot used by checkpoint execution.
Removal, a replaced identity or loss of primary ownership ends its next step. Terminal runtime
teardown cancels a pending announcement dispatch or retry wait after domain drain, then joins the
announcers before withdrawing their routes. These messages are availability hints; the replica's
periodic synchronization supplies a hint cancelled during shutdown. The owner records
the highest revision each
replica acknowledged, so an acknowledgement delivered after a newer one never lowers it, and a
Kafka offset commit waiting for its replica quorum, like a WASM checkpoint waiting for its replicas,
completes on the acknowledgement that satisfies it rather than at its deadline. Only a node that
holds a placement's state announces it: an ownership handoff announces the final checkpoints it
captured from live state, and the placements it fills from its storage or with an empty checkpoint
reach replicas through their own synchronization. A replica acts on an announcement by waking the
task that keeps its copy of that placement current, which keeps the announcement until it next
waits when it is busy; an announcement of a placement the replica holds no state for wakes nothing.
The announcements of a branch-keyed entity's lifecycle and branch checkpoints wake the entity's one
replica task, which takes the newest announcement of each at the start of its next round and
fetches or acknowledges each announced revision then. The task also runs a round every replication
poll interval, so a checkpoint whose announcement was lost reaches the replica within one interval.

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

Materialized relay snapshots seal to quota-owned files one bounded identity/Arrow group at a
time. Descriptions retain the file's exact length, digest and revision; fetch responses open an
independent reader and release each bounded chunk before the next. The sealed cache retains no
whole-container byte allocation. Local periodic persistence writes the same container as 64 KiB
segments, synchronizes data before replacing the header, and retains the namespace selected
before executor admission. Neither path requires a reservation proportional to container length.
The synchronous ownership-handoff metadata boundary still admits its resident checkpoint entry
against the bulk budget; exceeding that admission is a typed checkpoint refusal.

Kafka offset replica catch-up first asks `describe_kafka_offsets` on the Commands pool whether
the owner's revision advanced. Its typed answer distinguishes an unchanged revision, a newer
revision, and the shared remote-operation refusal classes. An unchanged checkpoint is neither
encoded nor transferred. A newer checkpoint uses `sync_kafka_offsets` on Bulk with the Snapshot
subquota and a thirty-second progress deadline, independently of the replica polling cadence.
The owner retains the admitted offset topology and reads its conservative revision before the
offset slots, encodes the current native checkpoint directly into a quota-owned staging file, and
sends a forty-byte revision/digest header followed by chunks of at most 64 KiB. The declared
response length bounds the complete native payload. Encoding scratch and native conversion use
the separate `restore_metadata` admission; bounded file I/O and transport use Bulk. Neither the
replication message limit nor the bulk memory ceiling bounds the whole checkpoint's length.
The receiver stages bounded chunks, releases the response stream at EOF, verifies exact length
and digest, and decodes current archived entries directly into a new table under admitted CPU
work. Cancellation is checked between file blocks and native entries. Only complete conversion
reaches the replica's assignment-token installation barrier; a promoted or replaced assignment
rejects a delayed transfer. The replica acknowledges its installed revision afterward. Kafka
commit and reset waits use the native bulk operation's thirty-second budget. The owner
therefore accepts a replica that completes this checkpoint after a small Commands response budget;
an absent acknowledgement still ends the commit at the operation deadline. Transfer and admission
failures preserve their typed causes in the replica diagnostic and retry on a later round.
Every successful poll also repeats the replica's held revision when the checkpoint is unchanged,
so a lost acknowledgement does not strand the owner's quorum wait or require another transfer.
The replica checks its installation assignment before reporting that revision; promotion or
replacement fences a retained poll's acknowledgement as well as its delayed installation.
Debug checkpoint events identify encoding, receive and installation boundaries by placement and
revision, with declared byte length once known. Their timestamps distinguish encoding, transfer
and conversion delays without logging checkpoint payloads or partition offsets.

Ownership handoff capture returns the same checkpoint descriptions as a bounded control response.
The destination fetches each exact revision from the source over the Snapshot bulk subquota before
validating its state and publishing the prepared handoff. A failed, truncated, superseded or
digest-mismatched fetch fails preparation without installing a partial checkpoint. The source
serves a handoff fetch from the exact checkpoint capture persisted in its state store, or from
the retained committed WASM checkpoint. An ordinary replica fetch selects the published state
handle. A committed WASM save is already on stable storage before capture, even when a newer
published save is waiting for replicas. Capture
describes and releases each WASM branch save before reading the next branch, so its control
inventory retains descriptors rather than every guest save allocation. Other state kinds complete
their existing persistence steps before description. No checkpoint body enters a
replication-class message. Preparation retains its verified checkpoint set until activation, so
the destination's total held bytes still grow with the number and size of the entity's branches.

Runtime-state synchronization replies and materialized-snapshot descriptions carry the shared
typed remote-operation failure envelope. Rejection, absence, temporary unreadiness, and execution
failure remain distinct across the node boundary, and the requester keeps that classification in
its local replication or snapshot-exchange error. Only an execution failure includes the serving
node's opaque diagnostic text. A materialized snapshot is streamed only after a successful typed
description identifies its exact length, digest, revision, fence, and branch generation; the
placement that the request names supplies its schema fingerprint.

Domain routing retains each materialized relay's installed-state publication from the shared
state-replication routing owner. Branch reads and generator scans use that relay's immutable index,
while the selected route validates its retained assignment identity. Retirement ends the exact
state intake before removing its member; an ending predecessor cannot withdraw its replacement.
Materialized installation refuses a lower revision, fence or branch generation before changing
live rows. Transport decoding and the snapshot container remain owned by their existing engines;
this publication introduces no wire or stored-container variant.

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

## Client Producer Links

A client may open a producer for a [client ingestor](./ingestors.md#client-ingestors) through any
live node. When the serving node does not execute the ingestor, it forwards the producer to the node
that does over one `client_producer_link`: an ordered duplex stream on the relay pool, admitted
through the shared relay subquota, with a five-second setup deadline. A serving node keeps at most
one link to each owning node, opened by the first producer that needs it and shared by every
producer it forwards there, so forwarding holds one stream per peer however many producers use it.
Two opens racing for one owner start one link.

The opening frame names the serving node, and the owning node refuses a link whose named node is
not the peer it authenticated. Each forwarded producer then has a key the serving node assigns and
never reuses within its process, so a late frame about an ended producer can never reach a later
one. The serving node sends `Open` with the domain, ingestor, expected fields, credit, and the
largest batch one submission may carry, then the producer's `Submit` frames carrying the Arrow IPC
bytes of each batch and its `Clear` frames, and finally `Close` or `Detach`. The owning node answers
with `Opened` and the producer's description or `Refused` with its refusal, then the producer's
`Admitting`, `Outcome` and `Admission` frames, and finally `Ended` with a reason or `Closed`.
Frames of one producer keep their order in both directions, so its open precedes its batches and
its outcomes, admission changes, and end follow the answer to its open. A batch travels at most once
over the link, and the frames are validated and charged to the relay memory class like every other
relay-pool operation.

The owning node queues a forwarded batch like any other, but admits it only once the serving node
has cleared it. When the batch's turn in the ingestor's window comes, the owning node takes a slot
of the window for it and sends `Admitting` naming the batch. The serving node records that the
batch may now be admitted, answers `Clear`, and from then on counts the batch as possibly admitted;
only on that `Clear` does the owning node hand the batch to its admission worker. The serving node
clears only a batch it forwarded and holds no outcome for, and it answers in the order it was asked,
so clearances return in the order the owning node requested them. Admitting a forwarded batch
therefore costs one more round trip of the link. A batch awaiting its clearance holds its slot of
the window, so a serving node that stops answering holds at most the slots it was asked to clear,
until the silence limit ends its link.

The serving node waits at most 20 seconds for the owning node to answer a forwarded open, including
opening the link. Both ends send a heartbeat after two seconds without other frames and treat ten
seconds without hearing anything as a lost link; the owning node skips a heartbeat rather than queue
it behind 64 unsent answers. When a link ends for any reason, the serving node refuses the opens it
has not heard back about as `EndpointUnavailable` and ends every producer the link carried as
`OwnerLost`. Before that end, it answers each batch of the producer that it never cleared — whether
it sent the batch or the link ended before it could — as `NotAdmitted` with `ProducerEnded`: the
owning node admits nothing without a clearance, so none of those batches entered the graph. Only
the batches it cleared become `OutcomeUnknown` with cause `OwnerLost`. A crashed owning node closes
its connections and ends the link at once; one that stops answering without closing them ends it
when the silence limit passes. The owning node detaches the link's producers: their admitted
batches continue through the graph with nobody left to answer them, and the batches it queued or
was clearing are dropped unadmitted, releasing their slots of the window. A later producer opens a
new link.

Each end reserves the producer's granted bytes in its own 128 MiB producer budget: the serving node
for the batches its session holds, and the owning node again for the batches it retains for another
node. The link adds no reservation of its own beyond the transport's per-frame charge.
[Client Session Protocol](./client-session-protocol.md#producers) describes what the client sees.

## Client Consumer Streams

When a session opens a consumer of a `TO CLIENT` emitter executing on another node, the serving
node opens one `client_consumer_stream` duplex stream for that attachment on the relay pool and
shared subquota. The opening request names the authenticated serving node, domain, emitter, exact
schema fields and granted credit. The owner checks the peer identity, reserves the grant in its
own 128 MiB consumer budget and attaches to its local emitter endpoint before replying `Opened`
with the endpoint description or `Refused` with a typed cause. The serving node has already
reserved the same grant against its session and node budgets.

The owner sends each attempt's identity, fresh ACK reference, source relay, opaque branch
fingerprint, member count, execution snapshot and total byte count before its Arrow IPC bytes.
It splits those bytes into ordered chunks of at most 1 MiB, each charged by the relay pool.
The serving node checks the announced length against the grant and reassembles one delivery before
answering a client read. It sends `Settle` frames with a stream-local correlation key and receives
one typed result for each; an independent writer keeps settlement and heartbeat sends from
blocking response reads. Both ends send two-second heartbeats and end the attachment after ten
seconds of peer silence. A lost stream detaches the owner's consumer, revokes its attempts and
allows the retained batches to be reassigned. Output and ACK state stay volatile, so an owner
loss can require upstream replay. [Client Session
Protocol](./client-session-protocol.md#emitter-consumers) owns what clients observe.

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

A peer becomes a health target and an outbound target once discovery publishes its interconnect
endpoint. A target with no usable endpoint is neither probed nor dialled, and its availability stays
unknown until discovery publishes one. A previously established target remains eligible for probes
and outbound connections through a Chitchat liveness loss while application health still has an
observation of it from the node-unavailability interval, as described below. A different
incarnation or advertised endpoint must establish a new target.

Each health round has at most one probe in flight for each peer and at most 32 probes across the
node. A probe has a one-second total deadline. Results are published as they complete, so a silent
peer occupies only its own concurrency slot. The next regular round begins roughly one second after
the previous round finishes. Gossip updates remain pending while those bounded probes finish and
start the next round immediately afterward. They never cancel an in-flight round: repeated gossip
updates must not prevent an unreachable peer's deadline from becoming a failed observation.

Every result is bound to the exact certificate node identifier, discovery incarnation, endpoint
generation, advertised address, and observation time that were targeted. A healthy response must
return the same application identity. If discovery replaces the incarnation or endpoint while a
probe is running, its late result is ignored.

Health observations distinguish:

- **Healthy:** the current target returned the expected application identity within the deadline.
- **Failure:** the current target returned an error or did not answer within the deadline,
  including a target whose advertised host does not resolve.
- **Capacity exhausted:** the probe could not obtain its reserved local capacity.

A missing, stale, or capacity-exhausted observation produces unknown availability. It does not mark
a peer unavailable and does not extend a previous run of failures. Only continuous, fresh failures
for the configured node-unavailability interval produce unavailable status; a healthy observation
resets that run. Consensus membership continues to use the cluster topology established by gossip.

Scheduling and runtime availability retain a previously discovered incarnation that gossip stops
listing live only while its latest application observation is younger than the node-unavailability
interval and has not made it unavailable. A healthy, briefly failing, or capacity-refused
observation from that interval keeps the peer available. A peer with no completed observation in the
interval falls back to its gossip liveness, even though no failure was recorded. A stopped peer
therefore leaves scheduling and runtime availability no later than when gossip declares it dead and
its last observation has aged out, whether or not any probe to it completes.

During startup, Chitchat's dead set can also contain a voter whose first heartbeat arrived through
another peer's digest: the failure detector has insufficient heartbeat intervals to establish
liveness. Automatic scheduling uses a separate process-local live-observation history for its
first ten seconds, including observations made before acquiring leadership. A voter that has never
been observed live keeps scheduling in that bounded wait even when Chitchat lists it dead. Once all
current voters have been observed or the grace expires, ordinary effective availability governs
automatic failover. See [Whole-Cluster Restart Keeps Ownership](./shutdown.md#whole-cluster-restart-keeps-ownership).

Command completion reads the leader's effective availability view through the
`application_completion_peers` management progress request. The response names the leader's
incarnation, Raft term, and required process incarnations. A follower uses it only while its own
Raft leader and term still match, and includes its own incarnation in the barrier. Failure to reach
the leader leaves the command pending. This avoids conflicting completion sets when application
health is asymmetric: a connected follower may still probe an unreachable peer successfully after
the leader has retired that peer. Revision and HTTPS listener progress requests remain bound to the
reported incarnation.

`SHOW CLUSTER STATUS` exposes the interconnect address, endpoint generation, observation age,
observation outcome, and derived availability. Its `connected` status means the latest application
probe is healthy, rather than merely that a transport pool exists.

## Connection And Credential Lifecycle

A preconnected pool slot that fails reconnects with exponential backoff beginning at 200
milliseconds and capped at five seconds. A peer removal, incarnation change, or advertised endpoint
change retires the old target and cancels work tied to its slots. New operations use only the new
target generation. A new DNS answer for the same advertised endpoint is not a target change: it
leaves established connections in place and is used by the next connection attempt.

Each established HTTP/2 connection sends a protocol ping every 15 seconds and waits at most ten
seconds for its acknowledgement. The outbound and inbound ends both close a session that cannot
answer, independently of the operating system's TCP retransmission timeout. Closing the outbound
end starts its pool slot's bounded reconnect; closing the inbound end releases the per-peer class
slot so that reconnect can be accepted. Ordinary request deadlines still apply to individual
operations during the detection window.

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

A replacement publishes the new bundle and its generation as one value, under the generation after
the one it replaces, so two replacements that race still publish distinct generations. Each
connection records the generation of exactly the credentials it authenticated with. An inbound
connection compares that record with the published generation, without a lock, whenever it accepts
a stream and whenever a replacement is announced, and begins graceful shutdown once the published
generation has moved past it. The replacement publishes before it retires the outbound pool slots.
An outbound connection set up from the replaced credentials checks the published generation once it
is established: one that checks after the publication is refused, and one that checked before it is
held by a slot the retirement then ends, so replaced credentials never outlive their replacement.

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
shutdown. Transport shutdown rejects new interconnect admission, cancels pool and operation waiters,
and retires every pool connection the node opened: each stops leasing and closes once its leased
streams return. Connections that peers opened to the node close at once, together with the handlers
still serving their streams, so a peer's request the node has not answered fails instead of
completing. Whatever remains after ten seconds is then closed. Connection setup, including the name
resolution it performs, and incomplete TLS handshakes remain inside this bound. A repeated `SIGINT`
or `SIGTERM`, or the shutdown deadline passing, ends the process without running the rest of its
shutdown, so its peers observe its connections ending exactly as they do when the process crashes.

See [Shutdown And Recovery](./shutdown.md) for the complete phase contract, the deadline and exit
statuses, and what each ending preserves.

## Failure Ownership And Persistence

Transport failures identify name-resolution, setup, authentication, admission, encoding, decoding,
flow-control, timeout, remote-response, and target-departure failures separately. Typed remote
errors remain available to the operation owner, which decides whether a request is safe to retry.
The interconnect does not infer idempotency for arbitrary control-plane or runtime operations.
Local transport operations return `error_stack` reports. TLS and wire failures retain their typed
causes beneath `TransportError`; typed requests and stream readers add `RequestError` context while
retaining the transport report. Deadline, shutdown, quota, cancellation, relay-rejection, and
indeterminate-delivery decisions inspect the current typed context, not formatted report text.
Runtime dispatch, relay admission-response and remote acknowledgement logs render every context
of those local reports. State synchronization, checkpoint announcement and replica catch-up logs
also retain the underlying request, placement or storage cause when their owner adds context.
When remote ACK registration cannot reserve correlation memory, the affected record's negative
acknowledgement carries the registration report chain, including its admission cause.
An answering node sends its established remote failure class or stream rejection text over the
wire; a local report's cause chain is not serialized into an HTTP/2 response.

Connections, request state, relay grants, delivery reconciliation, progress trackers, and
acknowledgement maps are never persisted. Durable control-plane state remains in consensus, and
selected runtime state remains in its owning snapshot or replication mechanism. This boundary is
why process epochs are part of relay delivery identities, why an unresolved result across a
receiver restart is reported as indeterminate, and why an acknowledgement registration names the
run of the node that registered it.

An ownership-handoff gate lease is also in-memory coordination state. Releasing or expiring that
lease removes the runtime fence but does not report a persisted preparation as cleaned up. Only an
exact durable discard, activation, or schedule-based reconciliation resolves that preparation.

## Observability

Each node exports interconnect measurements through its local metrics endpoint. The main groups
cover connection and stream occupancy, pending operations, setup failures and resets, quota
exhaustion, request latency, relay channels and grants, admission wait, unresolved delivery age, and
bulk-transfer bytes. Interconnect memory, worker queues, reactor delay, and consensus retention show
whether pressure originates in transport, execution, or the protocol using it. A connection attempt
that could not resolve its peer's advertised host is counted with reason `resolution`, apart from
setup, handshake, capacity, and closed failures, so a DNS outage is visible as itself.

Typed-request observations identify application health as operation `liveness` and replaceable
domain-clock delivery and HTTPS listener installation probes as operation `progress`, so their
request counts, outcomes, latency, and quota failures can be evaluated independently. A client
producer link's opening, failure, silence, and the producers it ends or detaches are logged at
`debug` on both ends; the owning node's client-ingestor metrics count forwarded producers apart
from local ones.

Metric labels are bounded dimensions such as traffic class, direction, operation, outcome, and
reason. They do not include peer, domain, relay, branch, delivery identity, or payload values.
An owner-delivery admission failure logs its domain, relay, non-sensitive branch fingerprint and
target, together with the transport report's retained cancellation and rejection causes. The
undelivered batch and branch field values stay out of that diagnostic.
Per-batch and payload-bearing logs use debug or trace levels and do not expose sensitive field
values. See [Metrics And Observability](./metrics-and-observability.md) for the metric and logging
contract.
