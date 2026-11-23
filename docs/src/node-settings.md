# Node Settings

Every node setting has one class, a typed owner, one default, a derivation when applicable,
and a validation boundary. Values that depend on an external system belong to that connector's
declaration in Plexus, the Nervix graph definition language.

The vocabulary provides `SettingDefinition`, three sealed classes and `Setting<D>`.
The `nervix::hidden_tunable` compiler diagnostic refuses numeric policy outside that boundary.
Every option `nervix-server` accepts is either a declared setting in its owner's group or part of
what the node is provisioned with, its identity, addresses, paths and trace export, which are not
settings; [The Server's Settings](#the-servers-settings) lists them. A runtime constant that is not
an option is fixed by the contract it states, derived from typed input, or retained under a
reason-bearing expectation that names its repair task, and this chapter states the contract each
satisfies. The
class API alone publishes no record, commits no parameter, detects no capacity and changes no
running service. A node publishes, carries and verifies its prerequisites as
[Prerequisites At Join](#prerequisites-at-join) describes, and its
[startup preflight](#the-startup-preflight) detects its memory, measures its volumes and detects
its properties. Cluster parameters have one committed Raft record, seeded at the first bootstrap
and changed through `ALTER CLUSTER SET`. Every admitted node announces its
[node record](#the-node-record), which holds its resolved capability claim, through Raft.

Runtime admission and failed committed-runtime installation share the configured
`--control-plane-runtime-state-retry-interval`, 500ms by default. Committed schedule and domain
updates can wake an installation retry sooner, and success ends that cadence.

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

## Listener Bounds

Listener settings are local node capabilities. Startup logs the resolved values, and each node's
committed `DESCRIBE CLUSTER` capability line includes the complete `listeners` record. Changing a
flag requires restarting that node; it does not change Plexus or the cluster's graph definitions.
Each public listener enforces its own connection ceiling, independently of placement and leadership.

| Option | Default | Applies to |
| --- | --- | --- |
| `--listener-max-connections` | 1,024 | Each public TCP listener, including upgraded WebSockets and TLS handshakes |
| `--listener-header-bytes` | 16 KiB | HTTP/1 request head and chunked trailers |
| `--listener-max-headers` | 100, or fewer when `--listener-header-bytes` holds fewer header lines | Headers in one HTTP/1 request head or chunked trailer section |
| `--listener-buffer-bytes` | 408 KiB | HTTP/1 read buffer, and the largest body copy made on the connection; minimum 8 KiB |
| `--listener-header-timeout` | 30 s | Complete HTTP/1 request head |
| `--listener-tls-timeout` | 10 s | HTTPS, console TLS and gRPC TLS handshake |
| `--listener-idle-timeout` | 60 s | Accepted public I/O without read or write byte progress |
| `--listener-accept-retry-delay` | 100 ms | Public and interconnect accepts after a node-local failure |
| `--listener-http-keepalive` | true | HTTP/1 connection reuse |
| `--http-body-bytes` | 32 MiB | Complete HTTP endpoint payload, including chunked input |
| `--http-body-copy-bytes` | 64 KiB | Body bytes a data worker moves between cancellation checks |
| `--http-body-timeout` | 30 s | Complete HTTP input, including moving its body to a larger buffer |
| `--http-refusal-drain-timeout` | 5 s | Transport drain after a completed HTTP input refusal |
| `--websocket-message-bytes` | 64 MiB | Ingest endpoint WebSocket message |
| `--websocket-frame-bytes` | 16 MiB | Ingest endpoint WebSocket frame |
| `--websocket-write-buffer-bytes` | 128 KiB | Outgoing bytes an endpoint or console WebSocket collects before writing |
| `--grpc-header-bytes` | 16 KiB | HTTP/2 header list |
| `--grpc-concurrency` | 128 | Concurrent request handlers per gRPC connection |
| `--grpc-request-timeout` | 120 s | gRPC handler response deadline; an established streaming response keeps its lifetime |
| `--grpc-max-streams` | 256 | HTTP/2 streams per gRPC connection |
| `--grpc-stream-window-bytes` | 1 MiB | HTTP/2 receive window of one gRPC stream |
| `--grpc-connection-window-bytes` | 1 MiB | HTTP/2 receive window of one gRPC connection |
| `--grpc-send-buffer-bytes` | 400 KiB | Response bytes one gRPC stream queues before it waits |
| `--grpc-max-pending-reset-streams` | 20 | Client-reset gRPC streams that may wait unaccepted |
| `--grpc-max-error-reset-streams` | 1,024 | gRPC streams the listener resets for a client's protocol errors |
| `--grpc-keepalive-interval` | 30 s | HTTP/2 server ping interval |
| `--grpc-keepalive-timeout` | 20 s | HTTP/2 ping acknowledgement budget |
| `--listener-tcp-keepalive-time` | 60 s | Kernel TCP keepalive idle time |
| `--listener-tcp-keepalive-interval` | 10 s | Kernel keepalive probe interval |
| `--listener-tcp-keepalive-retries` | 3 | Kernel unanswered probe count |

Every option has the environment form `NERVIX_` followed by its uppercase name with underscores,
for example `NERVIX_LISTENER_MAX_CONNECTIONS` and `NERVIX_HTTP_BODY_BYTES`. Counts and byte limits
are positive and must fit their native protocol/admission representation. Deadlines are positive
and representable by the monotonic clock. Kernel keepalive durations are whole seconds from 1
through 32,767; the probe count is from 1 through 127, matching the native socket-option ranges.
The HTTP/1 header default preserves the transport's 16 KiB trailer ceiling. Request heads also
fit the configured read-buffer capacity.

The shared HTTP/1 parser refuses a request head with more than `--listener-max-headers` headers
with 431, including a WebSocket upgrade request; a chunked trailer section has the same count. The
parser reserves one slot per allowed header for every request it reads, so the count is at most
the header lines `--listener-header-bytes` holds at three bytes each, a one-byte name, its colon and
a line feed, the shortest line RFC 9112 allows. A larger value refuses startup. The default is 100,
or that number of lines when it is smaller: a request with more headers already exceeds the byte
bound, so the default refuses nothing the byte bound admits.

The gRPC listener sets every HTTP/2 limit itself rather than inheriting a library default. Its
receive windows bound what clients send ahead of the listener's reading: one stream at most
`--grpc-stream-window-bytes`, one connection across all of its streams at most
`--grpc-connection-window-bytes`. Both are at most 2,147,483,647 bytes, the largest window HTTP/2
can advertise, and the connection window is at least 65,535 bytes, the window RFC 9113 gives every
new connection, which a receiver can only raise. Clients of the gRPC listener therefore hold at
most `--listener-max-connections` connection windows of input on a node, 1 GiB at the defaults. Each
stream queues at most `--grpc-send-buffer-bytes` of responses before it waits for its client, at
most 4,294,967,295 bytes, so one connection queues at most `--grpc-max-streams` times that. These
bounds are declared rather than charged to the intake pool, and the startup memory check does not
add them. The windows never adapt to measured bandwidth, which would grow them past their declared
size. The listener accepts HTTP/2 frames of up to 16 KiB, the frame every HTTP/2 peer must accept;
the windows, not the frame size, bound what a connection buffers. HTTP/2's rapid-reset protection
ends a connection with `ENHANCE_YOUR_CALM` when its client resets more than
`--grpc-max-pending-reset-streams` streams the listener has not accepted yet, or makes the listener
reset more than `--grpc-max-error-reset-streams` streams for its protocol errors.

An endpoint or console WebSocket collects up to `--websocket-write-buffer-bytes` of outgoing frames
before writing them to its connection. A peer that stops reading holds at most that buffer, one
more message and its 14-byte frame header: an endpoint socket's messages are at most
`--websocket-message-bytes`, a console socket's one session frame. The socket refuses a message
beyond that, and startup refuses a write buffer whose capacity does not fit the address space.
Incoming frames are read in fixed 4 KiB chunks into storage that grows only to the frame being
assembled, which the frame and message limits cap.
The startup preflight requires the intake pool to hold two body limits plus the declared initial
read scratch and collection metadata. An explicit inconsistent pair refuses startup before stores
or listeners open.

Read and write byte progress both renew idle time. A WebSocket upgrade retains the accepted
socket's lease and deadline, so a silent peer waiting for its next frame eventually loses its
connection. The frame size setting caps each frame's payload. The message size setting caps the
assembled message across its frames. See the [endpoint manual](ingestors.md#websockets-endpoint)
for what a client observes on expiry or overflow.
Connection tasks are reaped while the listener runs; shutdown retains its
existing admission, grace and terminal-cut order. HTTP listeners keep one bounded refusal
slot alongside accepts, in addition to admitted connections. At capacity, cleartext HTTP returns 503 with reason
`listener_connection_capacity` through the same bounded HTTP parser. Receiving its request and
writing the refusal share one header-timeout budget; its completed transport uses the same bounded
refusal drain described below. While
that slot is occupied, further excess connections close without a protocol exchange, and newly
available admission capacity remains usable. A TLS or gRPC connection is closed before protocol
admission.

HTTP endpoint bodies use the independent `intake` executor memory class. It accounts the
transport read buffers a request holds conservatively, and its body buffer exactly, including both
allocations while a body moves to a larger buffer. Input contention refuses immediately with 503;
it never holds relay decode capacity while waiting for relay work.

A body that arrives in a single data frame keeps the shared byte buffer it was read into, with no
copy. From the second data frame on, the endpoint copies each frame into one body buffer as it
arrives and releases the frame at once, so the connection keeps reusing its read buffer instead of
holding one per read: the charge follows the body's size, however the network splits it. The body
buffer is sized to a declared `Content-Length`; without one it doubles as frames arrive, up to
`--http-body-bytes`. A copy of at most one read buffer runs on the connection itself. Moving a body
that already holds more runs through the data CPU class, with a cancellation check every
`--http-body-copy-bytes`, and its reservation follows the worker until it exits. The final body
keeps the charge for its own allocation until dispatch ends.

The endpoint's public input outcomes are:

| Status | Reason | Meaning |
| --- | --- | --- |
| 413 | `http_body_capacity` | The declared length or accumulated chunked payload exceeds the cap |
| 408 | `http_body_timeout` | The complete body did not arrive by its deadline |
| 431 | HTTP header capacity | Hyper refuses an oversized request head and closes the connection |
| 503 | `http_input_memory_capacity` | The intake pool cannot back retained input |
| 503 | `http_input_worker_capacity` | Moving a body larger than one read buffer cannot be admitted on a data worker, expires or finishes cancelled |
| 400 | `http_body_invalid` | Malformed or interrupted HTTP body framing, including oversized chunked trailers |

The HTTP input completion budget distinguishes phases: expiry while receiving bytes returns 408;
expiry while the server waits for or performs a body move on a data worker returns 503. The move
retains its reservation until the worker actually exits after cancellation.

Input refusals admit no payload to the graph. Hyper completes the response and closes its write
side before a bounded transport drain discards residual bytes, so an ongoing upload cannot reset
the connection before its client reads the refusal. The drain reuses the declared read-buffer
capacity, retains the connection lease, and ends at peer closure, shutdown or
`http-refusal-drain-timeout`. It does not parse HTTP or decode payloads; this follows the staged
transport closure in [RFC 9112 §9.6](https://www.rfc-editor.org/rfc/rfc9112.html#section-9.6).
An unfinished request
head is closed at its header deadline; it is distinct from the body's 408 response. Existing route,
quiesce and acknowledgement outcomes retain their own contracts.

Connection-local accept failures retry immediately. A node-local failure such as descriptor
exhaustion reports the failure and waits the declared accept retry delay before trying again.
Closing admission cancels that wait. The pause survives a listener future being temporarily dropped
while its owner reaps another completed connection.

## Committed Node Capabilities

`--cpu-count` (`NERVIX_CPU_COUNT`) overrides the process's cgroup-aware available parallelism.
`--memory-limit` (`NERVIX_MEMORY_LIMIT`) overrides the cgroup/physical-memory detector.
Both overrides must be positive. A failed host query remains `unknown`; memory preflight, the
derived execution settings and the committed claim use the same resolved measurement. The startup
log includes these resolved values and the receive bounds. Worker pools, budgets, client credit and
watermarks derive from them through their own setting owners, as
[Execution Capabilities](#execution-capabilities) states; [Sizing A Node](sizing.md) presents every
derivation for operators.

The claim also carries the total size of the volume that holds the node's database directory, as
the [startup preflight](#the-startup-preflight) measured it when the run started. It has no
override. The committed resource quotas must fit it on every admitted node, as
[Committed Parameter Changes](#committed-parameter-changes) describes. A volume that grows or
shrinks while the node runs counts from the node's next start, which measures it again.

After membership admission, each node submits one incarnation-bound capacity claim, a
`NodeCapabilityRecord`, to the current leader inside its [node record](#the-node-record). Raft
commits the record under the node name. Repeating an identical announcement is a no-op; a newer
incarnation replaces the record, and an earlier or differently configured identical incarnation is
refused. Departure retains the last record; `DROP NODE` removes it and fences that incarnation. A
restarted node waits for its own current record before activating stored assignments. Node
records are absent from the join handshake and gossip.

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
public client admission, with one link to it: a client producer or consumer attached through
another node crosses the interconnect, so the relay batch bound also caps the batches a producer
forwarded to this node is granted and the delivery chunks this node receives for a consumer it
serves ([Client Producer Links](interconnect.md#client-producer-links)). Change a claim by
restarting the node with new overrides.

`DESCRIBE CLUSTER` shows each live incarnation's committed CPU count, memory bytes, database volume
bytes, four receive bounds and its engine/connector host capabilities, pending claims, and retained
departed records. Its `cluster-effective receive bounds` are
the componentwise minimum of matching live committed claims. Individual sends use their receiver's
claim, so one smaller peer does not shrink traffic between larger peers. The interconnect chapter
owns refusal, re-slicing, bootstrap transfer and flow-control behavior; the placement chapter owns
the capacity score and its precedence.

`adopted peer receive bounds` lists the claims this answering node's outbound pools currently use.
The independent capability controller refreshes them on the configured runtime-state retry
cadence, 500ms by default and independent of Raft heartbeat, including when another domain's
leader reconciliation is busy. This separates committed
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
peer, 768 by default. Health-probe fan-out and DNS lookup concurrency each equal
`interconnect-max-peers`. Completion reserves are at least one and otherwise one per sixteen peers,
bounded by the incoming queue.

With the default 64 peers, the ordinary and append request reserves are 128 each and health-probe
fan-out is 64. An append stream retains its admission throughout its lifetime, so the derived
reserve holds an append stream and its replacement for every declared peer. Each traffic reserve is
independent; a full general queue cannot consume health or cancellation capacity. An explicit lower
request quota is reported as configured, and a refusal is a typed capacity outcome.

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
presented as installed. Each node probes every peer once per
`--control-plane-health-probe-interval` round and waits at most the probe timeout for each answer,
while an observation stays fresh for the timeout itself, so the timeout must exceed every admitted
node's interval plus the probe timeout; [Committed Parameter Changes](#committed-parameter-changes)
gives the refusals.

Relay reconciliation keeps each retained attempt and outbound correlation for twice the
`REMOTE_RELAY_TOTAL_TIMEOUT` its delivery captured, each channel watermark for twice the longest one
among the deliveries it recorded, and an owner still awaiting membership for twice the installed
committed value. Its sweep passes at one tenth of the shortest retention it must honour: 600s and
60s at the defaults. These are derivations of the cluster parameter, without flags of their own;
installing a committed revision moves the owner and sweep values, and a delivery keeps the
retention it captured. An idle sender rotates its channel at the smallest total admission budget
its deliveries on that incarnation captured, before its receiver can forget the watermark. Channel
watermarks and the retired-channel floors forgotten channels leave are charged to the relay memory
class while they are retained, never counted against a budget of their own.

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

## Control-Plane And Session Capabilities

Each incarnation's claim also records its resolved control-plane, session and client-link
capabilities. `DESCRIBE CLUSTER` reports those values for live and departed nodes. Reconcile
cadences are exported locally as `nervix_control_plane_cadence_seconds{operation="..."}`;
`membership` and `scheduling` identify their respective passes. A cadence changes when that node
restarts. It changes neither the cluster's accepted Models nor an existing operation's deadline.

| Capability | Default |
| --- | --- |
| Membership, scheduling, resource replication and application-health rounds | 1s |
| Transaction/archive sweep and leadership-sync retry | 250ms |
| Runtime-state admission, installation and capability-announcement retry | 500ms |
| Restore-staging sweep and interconnect TLS reload | 1s |
| Leader's Kafka partition watch | 1s |
| Inferencer model inspection deadline | 30s |
| Concurrent transaction recoveries / resource replications | 8 / 4 |
| Session frame / transfer / string bytes | 4 MiB / 64 MiB / 64 MiB |
| Session nesting / vector entries | 64 / 262,144 |
| Per-session outbound queue memory | 128 MiB |
| Session notice backlog / rows per subscription frame | 256 / 256 |
| Session leadership check / selected-domain snapshot | 250ms / 500ms |
| Authentication attempts per second after failure | 10 |
| Client-link heartbeat / producer open / consumer open | 2s / 20s / 20s |
| Producer answer / consumer answer / consumer writer queues | 64 / 64 / 64 |

The `--control-plane-*`, `--session-*`, `--client-link-*`,
`--authentication-attempts-per-second` and `--interconnect-tls-reload-interval` options expose
these capabilities; the generated server reference lists each option. The leader's partition
topology observer polls a domain's Kafka partition sources every
`--control-plane-kafka-partition-watch-interval` and its other sources every
`--partition-topology-poll-interval`. `--control-plane-model-inspection-timeout` bounds the
inspection of an inferencer's model while its binding is validated. Values and relationships
are validated before I/O. The application-health round must also fit the committed
`NODE_UNAVAILABILITY_TIMEOUT`, which a node checks against its bootstrap seeds or the live
committed policy before readiness, as [Committed Parameter Changes](#committed-parameter-changes)
describes. In particular, the session transfer holds a frame, its string bound fits
the transfer, and its queue memory holds at least one maximum-sized frame on each lane.
Each lane's depth is `queue_memory_bytes / (2 * frame_bytes)`, giving 16 frames by default.
The queue budget bounds queued frame bytes; transport buffers, executing requests, clock slots
and retained producer/consumer deliveries keep their own bounds. A backup download's queue is `max(queue_memory_bytes / (8 * frame_bytes), 1)`,
four frames by default. Its chunk size is one sixteenth of the session frame limit, 256 KiB
by default. Native clients retain their configured subscription record count, retained-frame
count and aggregate multiplier. The byte allowances use the current announced frame size plus
the in-memory event header, installed once with each checked exchange generation. The defaults
retain one such frame per subscription and four full subscriptions. A configured count that
cannot be represented with the contacted peer's frame size refuses that opening before queue
activation.

An authenticated session announces its checked limits to the client. Reconnects adopt the node
they reach, including after a capability roll. Producer and consumer links announce each
sender's heartbeat at open; a receiver tolerates five of its peer's intervals. This permits
different operational cadences within one cluster without a uniform-heartbeat assertion.
[Client Session Protocol](client-session-protocol.md#session-limits) and
[Cluster Interconnect](interconnect.md#client-producer-links) own the framing and failure rules.

Forced-recovery budgets, handoff reconciliation polling, entity-gate polling, domain-drain
polling and readiness propagation are committed cluster parameters. Their defaults are 5s,
250ms, 25ms, 50ms and 30s respectively. First-bootstrap flags seed them; later changes use
`ALTER CLUSTER SET`. New operations capture the current committed values at their owning
admission boundary. RPC deadlines derive from the transport request timeout, and the initial
membership, scheduling, health and resource pass delays derive from the gossip round. Derived
values have no independent flag.

Local transaction-application and gate release/engagement retries, clock-progress delivery,
backup retries and restore conversion admission have their own `--control-plane-*` capabilities.
Their defaults are 100ms for transaction application and for gate release and engagement, 200ms
for clock progress, 50ms for backups, and a 30s restore conversion wait retried every 50ms.
`--control-plane-runtime-state-retry-interval` keeps runtime admission, installation and node
capability publication retries at 500ms by default, independently of the Raft heartbeat. A node
may tune that retry without changing the cluster's consensus prerequisite.
Startup voter observation lasts twenty gossip rounds. Completion probes use one Raft heartbeat,
authority reassignment retries four fifths of a heartbeat, cordon release keeps four heartbeats,
and shutdown leader observation uses one tenth of a heartbeat. `--task-grace-period` controls
cooperative listener and background-task stop grace, two seconds by default and capped by the
remaining stop deadline. A drain may use the full configured timeout without cutting the
listener's normal grace to zero.

The complete parameter record stays allocation-free and `Copy`. Its validated parameter projection stores
whole seconds and subsecond nanoseconds in eight bytes; every permitted duration, through
`u32::MAX` seconds, fits.
The public duration values and their JSON seconds/nanoseconds encoding retain full precision.
This keeps the inline Raft command footprint bounded while carrying the complete policy.

The nine count parameters whose definitions already cap them within `u32::MAX` use four-byte
stored projections. Their public u64 values, defaults and admitted ranges remain unchanged;
full-width count and byte parameters retain eight bytes. Storage and JSON both validate through
the owning definition before publishing the complete policy.

### Trace Export

A node exports its own traces to an OpenTelemetry collector over OTLP gRPC once it is provisioned
with one. `--otel-otlp-endpoint` (`NERVIX_OTEL_OTLP_ENDPOINT`) names the collector, such as
`http://collector:4317`, and `--otel-service-name` (`NERVIX_OTEL_SERVICE_NAME`, `nervix` by
default) names the service its spans carry. Both are provisioning, like the node's addresses, not
settings. A node without an endpoint exports no traces. An endpoint that is not a URI, the empty
text included, refuses the node before it opens a store. The option outranks its environment twin.

How the node exports is a group of node capabilities in its operational claim, beside the
control-plane, session and client-link groups. `DESCRIBE CLUSTER` shows them on each node's
capability line, and changing one requires restarting that node.

| Option | Default | Meaning |
| --- | --- | --- |
| `--otel-trace-sample-ratio` | 1 | share of root traces the node exports, from 0 through 1; a span follows its parent's sampling decision |
| `--otel-trace-export-timeout` | 10s | limit on reaching the collector, the name lookup and TCP attempts together, and on each export request |
| `--otel-trace-export-interval` | 5s | longest a span waits in the queue before its batch exports; a full batch exports at once |
| `--otel-trace-queue-capacity` | 2048 spans | spans the node holds for export; a span that finds the queue full is dropped |
| `--otel-trace-export-batch-spans` | 512 spans, at most the queue | most spans one export request carries |

No `OTEL_*` environment variable applies to a node's trace export. The OpenTelemetry SDK reads
some of them while it builds its own defaults, and the node replaces each of those defaults with a
declared value. So no endpoint, header, compression, timeout, batch, sampler, span limit, resource
attribute or service name variable changes where the node sends its traces, what it sends or
whether it starts. The exporter sends no request metadata and compresses nothing. Spans keep the
OpenTelemetry specification's default limits: 128 attributes, events and links per span, and 128
attributes per event and per link. [Node Trace Export](name-resolution.md#node-trace-export)
describes how the exporter reaches its collector, and
[Metrics And Observability](metrics-and-observability.md#the-nodes-own-traces) what it exports.

## The Node Record

The capacity claim is one of three parts of the record Raft commits for every node run:

- **Capabilities**, the sizing settings above, say what the node can carry.
- **Properties** are facts the node detected about itself at startup: what the machine is. No
  option overrides them, and a value the node could not detect is recorded as undetected, with
  its reason.
- **Tags** say what the operator intends the node for. They come only from `--node-tags`.

One announcement commits all three under the claim's incarnation rules: an identical announcement
changes nothing, a newer incarnation replaces the whole record, a different record for an
incarnation that already has one is refused, departure keeps the last record, and `DROP NODE`
removes it. Raft records the revision, its log index, that committed each record. The records are
schedule planning inputs beside members, voters and cordons, so a changed record invalidates a
captured plan. A record travels in neither gossip nor the join handshake, and its change publishes
no notice.

### Detected Properties

| Property | What it is | Without a detected value |
| --- | --- | --- |
| `architecture` | The architecture the binary was built for, such as `x86_64` or `aarch64` | Always detected |
| `cpu.model` | The CPU model the operating system reports, read through the maintained `sysinfo` crate | `undetected` with the reason |
| `gpu.count` | The GPUs NVML reports. Nervix loads NVML dynamically from `libnvidia-ml.so.1`; no library or no device is `0` | `0`, with the reason when NVML did not answer |
| `gpu.models` | The model of each GPU, in device order | `none` |
| `gpu.memory` | The memory of the smallest GPU, in bytes | `none` |
| `gpu.driver` | The driver version NVML reports for the node's GPUs | `none` without a GPU; otherwise `undetected` with the reason |
| `onnx.version` | The version the ONNX Runtime library this process loaded reports | `undetected` with the reason |
| `onnx.providers` | The execution providers that library reports, in its order | `undetected` with the reason |
| `onnx.cuda` | Whether the CUDA execution provider registers in this process: its provider library and the CUDA runtime load | `false` with the reason |
| `cuda.available` | Exactly `gpu.count > 0 AND onnx.cuda` | Never undetected |

CUDA is two facts because a GPU in the chassis says nothing about whether this process's ONNX
Runtime can use it: the GPU and CPU images load different ONNX Runtime libraries. The CPU count and
the memory limit are the node's capabilities, which the record already holds; the properties do
not copy them.

The node detects its properties once, in the [startup preflight](#the-startup-preflight), after
every check that can refuse it, on its bulk workers: the CPU model, the GPUs and the ONNX Runtime
each in their own probe, side by side. `--node-detection-timeout` (`NERVIX_NODE_DETECTION_TIMEOUT`,
a node capability, `30s` by default) bounds how long the preflight waits for them. A probe that has
not answered by then is recorded as `undetected (detection did not finish within the node detection
timeout)`, and a probe that never returns keeps its bulk worker until the process exits. Detection
never refuses the start. The ONNX Runtime probe loads the library every inferencer of the process
loads, the one `ORT_DYLIB_PATH` names or otherwise `libonnxruntime.so` as the dynamic loader finds
it; a library that does not load leaves the `onnx` properties undetected with that reason. It then
uses the one ONNX Runtime environment every inferencer shares and makes one attempt to register the
CUDA execution provider in a session builder that it then drops; what the registration loaded stays
loaded, as it would for a session. `the startup preflight passed` logs the resolved properties at `info`.

### Tags

`--node-tags key=value[,key=value...]` (`NERVIX_NODE_TAGS`) gives a node its tags; the option is
repeatable and comma-separated. A key starts with a lower-case letter or a digit and holds only
lower-case letters, digits, `.`, `_` and `-`, up to 63 bytes. A value is 1 to 255 bytes of printable
text without a comma, a control character or a line separator; the first `=` of a tag ends its key,
so a value may hold another. A node carries at most 64 tags, each key once. These bounds are fixed:
they keep the record one Raft command, far below the 1 MiB largest-command floor every node accepts.
The edge validates the tags before any I/O, and a refusal ends the process with status `1`, naming
the tag and the rule it broke, such as `node tag key 'Pool' must start with a lower-case letter or a
digit and hold only lower-case letters, digits, '.', '_' and '-'`.

Tags describe the node alongside its identity, addresses and paths. A node changes its tags by
restarting with other values and announcing a new record. Tag changes follow the rolling-restart
procedure of a capability change.

Use [`SHOW NODES`](placement.md#selecting-nodes) to see the committed tag values and preview a
selector before using those values to choose nodes.

### In `DESCRIBE CLUSTER`

`DESCRIBE CLUSTER` prints each node's committed record after its prerequisites: the revision that
committed it and the incarnation it belongs to, or `record: not announced`, then one line per
property, the undetected ones with their reason, and one `key=value` line per tag in key order, or
`tags: none`. A departed node's retained record follows its committed capacity claim: the
capability line and its interconnect, membership, DNS, database and consensus lines.

```text
  record: revision 41 (incarnation 3)
  properties:
    architecture: x86_64
    cpu.model: AMD EPYC 7763 64-Core Processor
    gpu.count: 1
    gpu.models: NVIDIA L4
    gpu.memory: 24152899584
    gpu.driver: 550.54.15
    onnx.version: 1.30.0
    onnx.providers: CUDAExecutionProvider, CPUExecutionProvider
    onnx.cuda: true
    cuda.available: true
  tags:
    pool=gpu
    zone=eu
```

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

A cluster starts with the embedded free policy. Its current quotas, `cluster.nodes` and
`cluster.domains`, are unlimited, and its one capability, `licensing.dummy`, is disabled. Node usage
counts committed voters and learners; domain usage
counts committed domain records, including stopped domains. Returning members and updates to an
existing domain do not consume another slot. Free mode contacts no licensing service, including
after restart.

```plexus
ACTIVATE LICENSE PERSISTENT WITH KEY 'NVXK-ABCDEFG2-your-secret';
REFRESH LICENSE;
DESCRIBE LICENSE;
```

`ACTIVATE LICENSE` selects `PERSISTENT` or `EPHEMERAL`. It runs on the leader without a selected
domain and outside domain transactions. Nervix uses the credential for the creation request;
its durable execution retains the verified signed response and the public eight-character key
identifier. Neither the credential nor the command's source enters that effect or its semantic
request digest. If leadership moves or the leader cannot confirm its authority after the cluster
records the command, clients can recover under the same execution reference. Nervix resumes
applying the saved decision or returns the recorded outcome. If the service already processed
creation, Nervix adopts the matching activation through an authenticated refresh.
`REFRESH LICENSE` runs immediately on the leader, outside domain transactions, and completes when
the newer signed decision commits or the service or cluster returns a typed refusal.

```plexus
ROTATE LICENSE CLUSTER KEY;
RELEASE LICENSE;
```

`ROTATE LICENSE CLUSTER KEY` preserves the cluster UUID, activation, ephemeral expiry and every
signed deadline. In activated mode the pending key and exact signed request commit before sending;
the current key authorizes the request and the pending key proves possession. Only the verified
rotation completion installs both the next generation and its decision. A lost response replays
the retained request identity and bytes, including across leadership changes, and a definitive
service refusal abandons the pending key. Free and released clusters rotate locally without
service access or service-issued trust documents.

`RELEASE LICENSE` commits the free target policy before contacting the service. It returns after
that local commit; `DESCRIBE LICENSE` reports `mode: released`, `policy: releasing` and
`release: awaiting service confirmation` while the leader retries notification. Existing grants
retain their deadlines, reported as the typed `grants_until` timestamp. Confirmation does not
restart or extend them. Once confirmation arrives and no grant remains, the mode becomes free.
An expired ephemeral activation refuses release with `Expired`. Activation in released mode
first finishes any outstanding notification, then creates a new activation with a new lifetime.
Local rotations remain available during a service outage; the pending release keeps its original
signing capability until confirmation, even after several rotations and restart.

An administrator transfers an activation through the licensing service. The former cluster's
next refresh reports `transferred` and targets the free policy; a rebuilt cluster activates with
its own permanent identity and consumes the freed activation slot.

The service is the compiled deployment `https://licensing.nervix.io`. Plexus and node options
supply no alternative address or trust anchors. HTTPS uses the node resolver and outward TLS
policy, HTTP/1.1 or HTTP/2, no redirects and no environment proxy. Activation verifies the
service's signed documents against the production trust bundle the build embeds. The
[shared protocol](licensing-protocol.md#trust-and-provisioning) owns provisioning and verification.

The leader signs challenge-bound requests with the cluster key. Lost responses retry the same
request identity and bytes while its challenge remains valid; a fresh challenge retains that
identity. Losing leadership cancels the request. The following positive Duration parameters are
seeded at bootstrap and changed with `ALTER CLUSTER SET`:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `LICENSING_REQUEST_TIMEOUT` | `30s` | One request, including resolution, TLS and response body |
| `LICENSING_RETRY_BACKOFF_MAX` | `10m` | Exponential retry ceiling; full jitter starts at the request budget |
| `LICENSING_STREAM_IDLE_TIMEOUT` | `90s` | Licensing stream idle budget |

Each has its corresponding `--licensing-...` bootstrap flag and `NERVIX_LICENSING_...` environment
variable. Validated durations retain nanosecond precision across the complete declared range in
compact inline storage, preserving the Raft command footprint without additional allocations.

Only a committed, verified decision grants licensed usage. The coordinator verifies receipt,
each replica verifies apply, and recovery and snapshot installation rebuild verification from
original signed bytes and the independent activation binding. A semantically invalid document
loads as `degraded`, reports `decision_unverifiable`, grants the free policy and prompts refresh.
Structural record corruption refuses startup. Revoking a working key prevents new decisions;
a previously committed decision remains verified with its signing-key-revoked observation and
prompts an immediate refresh. Root-authenticated trust documents commit independently of an
unacceptable new decision. A supplied root succession authorizes later epochs; an epoch without
that chain is refused without replacing the committed decision.

Licensing time is committed and monotone. The leader proposes physical observations at deadline
crossings independently of its refresh attempt. The derived refresh state is `licensed fresh`,
`licensed due`, or `licensed overdue`; a bounded `valid_until` crossing becomes `lapsed freeze`.
An outage moves no signed deadline, and restart resets none. Freeze keeps existing work running
and refuses new domain and member usage. Existing domain lifecycle commands and backups remain
available. A later verified decision can clear the lapse. A newcomer refused during the freeze
stops, and the leader keeps refusing that run after the lapse clears, for as long as gossip still
shows it; the node joins under the cleared policy when it starts again.

The licensed leader holds one authenticated outbound event stream. Each connection obtains a
fresh challenge and single-use signed stream ticket; credentials stay out of the URL. The initial
`sync` compares notification, decision, conditions and trust revisions. A gap or a new `decision`
or `trust` hint schedules a coalesced refresh through the ordinary verification and Raft path;
duplicate and stale notification identifiers do not schedule another refresh. Hints grant no
permission. An in-flight refresh can satisfy a sync's reported revisions before another request
starts. A response the cluster refuses satisfies nothing: a sync that reports a newer decision
than the committed one still schedules a refresh, and the cluster commits the service's next
verified decision, never the refused copy.
Notification gaps and later changes still reconcile, and explicit refresh commands still run.
Reconnects retain `Last-Event-ID` within the current activation and leader tenure;
a new leader reconciles from its committed revisions. Leadership loss, shutdown or a changed
activation/key binding ends the exact stream before its replacement starts.

Stream setup shares the request timeout, resolver and TLS policy above. The idle timeout applies
to incoming bytes, including keep-alive comments. Parsing uses the maintained SSE parser with a
management-event-sized input envelope per parser lifetime; exhausting it renews the connection
with its cursor and reconciliation, so incomplete input cannot accumulate without a bound.
Reconnects use the declared exponential backoff with full jitter. Polling at `refresh_after` and
committed deadline crossings run independently of stream setup, reads and reconnect sleeps.
A blocked or unavailable stream therefore changes notification latency, never signed permission
or the polling schedule. Stream state and its cursor are process-local and never persisted.

An ephemeral activation ends at its `ephemeral_expires_at`, fixed when the service created it:
refreshes, restarts and leader changes keep it, and it never moves later. When committed licensing
time reaches it, the stop gate closes. The gate is derived from the committed activation binding and
licensing time, never stored as a flag, and does not depend on the decision's verification, so an
edited or unverifiable document cannot lift it. A persistent activation never closes it.

While the gate is closed:

- `DESCRIBE LICENSE` reports `policy: stopped` and
  `processing: stopped by licensing: ephemeral activation expired at <instant>`.
- Admission refuses all new usage with state `stopped`, `CREATE DOMAIN` and the domains a restore
  would create included. The cluster never falls back to the free policy.
- The leader stops every running domain through the ordinary graceful stop, which preserves its
  persisted runtime state: intake ends at once and the work in motion finishes. The licensing
  stop's request is forced a fixed 60 seconds after the expiry in committed licensing time
  (`LICENSING_STOP_ENFORCE_AFTER`, a fixed constant), whatever holds the domain then, and no
  `CANCEL` withdraws it; an operator's `STOP ENFORCE AFTER` that comes due earlier ends it earlier.
  `DESCRIBE DOMAIN` then reports `stopped by licensing: ephemeral activation expired at <instant>`
  with how the stop ended and what it cut until the domain's next `START`. An `ALTER` or a backup
  that holds a domain paused when the gate closes still completes, and its pause ends in the
  licensing stop's request instead of a resume. A domain another operation holds begins its
  licensing stop once that operation releases it, still forced at the same instant.
- Planning refuses `START`, a transactional `START` and `RESTORE ... RESUME` with
  `licensing stopped processing`. Consensus refuses every other change that would make a domain
  run: resuming a paused domain and writing a domain record that runs. A restore whose gate closed
  while it applied leaves its domains stopped with the licensing cause, and its report shows them
  stopped by licensing.
- Sessions, `DESCRIBE`, `SHOW`, `BACKUP`, `ALTER`, `UPLOAD RESOURCE`, `CREATE USER` and the
  licensing statements stay available.

Every node announces the closing to its sessions with an Error notice and the lifting with an Info
notice. Only a new activation lifts the gate: `ACTIVATE LICENSE` is accepted again once the
ephemeral activation expired, because the service released its capacity at that instant. Domains
stay stopped until an operator starts them.

Every node answers `DESCRIBE LICENSE` without selecting a domain or redirecting to the leader.
It reports the mode, policy, public identity, committed time, processing state, quotas and
capabilities. An activation adds its binding, public credential identifier, decision and conditions
revisions, verification, signed deadlines, license facts and refresh state.

A capability is a feature a license switches on. The typed `capabilities` section lists every
catalog capability with its `state`, `enabled` or `disabled`, and the text form prints one line
per capability, such as `capability licensing.dummy: enabled`. The section's `permitted` state and
its `permitted_until` deadline belong to a capability whose removal has a grace period; no current
capability has one. A capability the license's decision does not list is disabled, so a cluster
keeps working with a licensing service that does not know the capability yet. A capability the
cluster does not know is left out of the section and counts under `conditions not understood`. The
catalog's only capability, `licensing.dummy`, changes nothing in Nervix:
the free policy disables it, and a license whose plan enables it shows that a verified decision
reached the cluster. A follower marks the leader's
incarnation- and tenure-bound connectivity observation as reported. Connectivity distinguishes
reachable service, transport outage and non-retryable service refusal, retaining each outage or
refusal's start and typed cause. Connectivity grants nothing; a failed request renews no deadline.
A degraded inspection also renders its typed decision-verification rejection.
The command carries the same observation in a typed cluster-license state-inspection envelope
with one unit and shared Row sections, so applications need not parse display text.

A signed reduction below existing usage starts a committed grace period at receipt. The target
quota governs new usage immediately. During grace, a replacement may restore usage up to the
allowance held when the reduction arrived; it cannot expand that allowance. Refresh, restart and
leader transfer preserve the transition's identity, start and deadline. The next refresh reports
the committed transition, and a service-signed acknowledgement replaces its copy. Only that
acknowledgement or a signed administrator extension can move a deadline later. Before acknowledgement,
the signed offline deadline also bounds the temporary permission.
The typed `transitions` section exposes each identity, condition, original and target values,
usage at start, start time, signed deadline, effective deadline, acknowledgement and current quota
state. `issued_at` and `received_at` are independent typed decision timestamps; `transitions_active`
counts committed grants that remain active. A member replacement must have belonged to the
membership when its grant started. A domain restore can recover slots up to the same original
allowance. Creating another domain or introducing a new member follows the target quota.

An improvement applies immediately and ends grants it makes unnecessary. At a term-end tick or
release, Nervix targets the embedded free policy and assesses any reduction using the signed grace
in force. The current free policy has unlimited quotas, so returning to it does not create a quota
reduction. Expired ephemeral activations retain their firm stop and receive no grace. When a grant
ends with usage still above its target, inspection, metrics and a Warning notice report the excess;
licensing itself drops no domain, removes no member and tears down no existing work.

Admission evaluates a coherent committed observation. Domain planning captures its quota, and
committed creation checks current usage and lapse before adding a domain. Membership admission
checks before adding a learner or voter. A typed refusal never removes an existing domain or
member. [Licensing Metrics](metrics-and-observability.md#licensing-metrics) owns the matching series
and [notices](client-session-protocol.md#what-a-session-is-told) describe asynchronous warnings.

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

## Data-Plane Runtime Policy And Capacity

Nervix commits domain teardown, entity-gate and remote acknowledgement timing as cluster
parameters. Server options with the same hyphenated names seed the first bootstrap only; use
`ALTER CLUSTER SET` to change a running cluster. `DESCRIBE CLUSTER` distinguishes committed
policy from the policy each node has installed.

| Plexus parameter | Default | Meaning |
| --- | --- | --- |
| `DOMAIN_DRAIN_TIMEOUT` | 30s | Budget for draining a domain's admitted work |
| `ENTITY_GATE_DEADLINE` | 30s | Budget for a coordinated entity gate |
| `REMOTE_ACK_ALIVE_INTERVAL` | 100ms | Report cadence of a retained remote delivery and of held local work |
| `REMOTE_ACK_DISPATCH_TIMEOUT` | 5s | Inactivity budget of a remote admission attempt |
| `REMOTE_ACK_SILENCE_TIMEOUT` | 15s | Reporting silence allowed after admission |
| `REMOTE_ACK_SWEEP_INTERVAL` | 1s | Cadence for counting silence and admission sweeps |
| `REMOTE_RELAY_TOTAL_TIMEOUT` | 5m | Total remote admission budget |
| `REMOTE_RELAY_INSTANTIATION_WAIT` | 5s | Budget for routing to become available |
| `PAYLOAD_UNFOLD_LIMIT` | 65,536 | Maximum records one codec unfolds from one ingested payload |

Every duration is positive and no longer than `u32::MAX` seconds. Silence must be strictly greater
than twice the sum of dispatch timeout and alive interval. The sweep divides both silence and
total admission budgets exactly, including their nanoseconds. Dispatch and instantiation budgets
must each fit the total admission budget. Change related values in one statement; an invalid
combination publishes nothing. The unfold limit is a positive 32-bit record count, independent
of the maximum declared emitter batch size.

Both domain drain and gate budgets must independently fit every admitted node's local drain and
shutdown budgets. Parameter transitions capture those capacity claims in their commit fence;
a concurrently changed claim requires revalidation. A node joining or restarting with insufficient
local budgets refuses the selected committed policy before readiness. Departed membership retains
its claim until `DROP NODE`, so increasing lifecycle policy may require updating that node or
removing its membership first.

Each remote delivery captures the complete timing group once. Its receiver reports and its sender
sweeps with that group for the delivery's lifetime. Live changes govern new deliveries; they do
not shorten an earlier delivery's reporting window. A late sweep counts once and skips missed
ticks. Each retained relay attempt and correlation keeps twice the admission budget of its delivery,
and a channel watermark twice the longest budget among the deliveries it recorded; a sender rotates
its channel before that channel's watermark can expire.

Held local work reports the acknowledgements it holds alive at the same committed alive interval: a
delivery waiting for admission, a processor's evaluation, checkpoint or inference, an ingestor's
group, an emitter's buffered, staged or retried work and a message-error route's buffered errors.
Each keepalive reads the interval once its work waits and keeps it until that work ends, and an
emitter's or message-error route's recurring beat reads it again for each beat it arms after the
previous one fired, so a change reaches a running owner at its next keepalive or beat with no
rebuild.
[Data-Plane Concurrency](data-plane-concurrency.md) owns how each owner keeps the installed timing.

The following are node capabilities, resolved through `RuntimeSettings` and announced with the
node's immutable incarnation-bound claim. Restart to change them. Each has its `NERVIX_`
environment variable, formed by uppercasing the option and replacing hyphens with underscores.
`DESCRIBE CLUSTER` prints the resolved values.

| Server option | Default | Meaning |
| --- | --- | --- |
| `--runtime-delivery-capacity` | 8192 | Maximum concurrent remote deliveries in each correlation lane |
| `--runtime-ingest-group-max-rows` | 1024 | Decoded rows that close an ingest group |
| `--runtime-ingest-group-idle-flush` | 5ms | Idle deadline of a partial ingest group |
| `--runtime-unacknowledged-poll-batch` | 1 | Messages requested per unacknowledged broker poll |
| `--runtime-ingestor-error-retry` | 100ms | Delay after a transient ingestor error |
| `--runtime-reconnect-initial` | 250ms | Initial reconnect delay for host-owned retry |
| `--runtime-reconnect-max` | 30s | Maximum reconnect delay for host-owned retry |
| `--runtime-branch-expiration-scan` | 30s | Cadence of branch lifetime expiration scans |
| `--runtime-replication-poll` | 1s | Cadence of state replication polls |
| `--runtime-catch-up-concurrency` | 16 | Checkpoint fetches admitted in one catch-up round |
| `--runtime-catch-up-pages` | 64 | Catalog pages visited in one catch-up round |
| `--runtime-retained-removals` | 1024 | Catalog removals retained for incremental replication |
| `--runtime-snapshot-staging-bytes` | smaller of 128GiB and temporary volume capacity | Concurrent temporary snapshot staging quota |
| `--runtime-snapshot-bytes` | smaller of 64GiB, staging quota and temporary volume capacity | Largest snapshot staged by one transfer |
| `--runtime-snapshot-describe-timeout` | 30s | Deadline of describing a sealed state snapshot |
| `--runtime-snapshot-fetch-timeout` | 30s | Deadline of fetching a sealed state snapshot |
| `--runtime-wasm-checkpoint-timeout` | 90s | Deadline until every guest checkpoint replica confirms |
| `--runtime-event-capacity` | 256 | Runtime events retained for a lagging observer |
| `--runtime-wasm-recovery-capacity` | 32 | Guest-state recovery requests awaiting a decision |
| `--runtime-immediate-flush-minimum` | 100us | Minimum timer interval for immediate branch flushes |
| `--runtime-checkpoint-bytes` | 256MiB | Largest state checkpoint accepted by an in-memory transfer |
| `--runtime-wasm-epoch-tick-interval` | 1ms | WASM epoch preemption quantum |
| `--runtime-wasm-epoch-deadline-ticks` | 1 | WASM epoch ticks before an asynchronous yield |
| `--runtime-wasm-guest-buffer-bytes` | 64MiB | Maximum guest invocation buffer |
| `--runtime-wasm-optimize` | true | Enable WASM compilation optimisation |

Counts are positive and fit the unsigned 32-bit collection bound; byte capacities are positive
and fit signed 64-bit accounting. Reconnect initial delay cannot exceed its maximum, and a single
snapshot cannot exceed the concurrent staging quota. Snapshot quotas must fit the volume holding
the temporary directory, independently of the restore-staging quota on the database volume.
Group size controls decoded grouping; sequential broker intake still requests exactly one message.
A connector's declared intake capacity also applies when it is smaller than the configured group.
Kafka partition watching follows `--control-plane-kafka-partition-watch-interval`, among the
[control-plane capabilities](#control-plane-and-session-capabilities).

Materialized row-view metadata derives its ceiling from the resolved relay memory budget at both
capture and restore. Capture metadata retains its Bulk admission; reopened metadata retains its
State Read admission, preserving isolation between those owners. Provision both the metadata
reservation and its streaming working set in the relevant pools. Streaming snapshot and transfer
chunks use the executor's `bulk_chunk_bytes`.
Catalog pages derive their entry and variable-key byte allowance from the replication frame limit;
they preserve ordered cursor progress without materializing the entire catalog. Task teardown
receives one fifteenth of the domain drain budget, two seconds at the default. Internal drain,
freeze and replication rechecks derive from their owning deadline or replication cadence, with a
one-nanosecond floor. Every ONNX session, a branch's and a binding inspection's alike, uses one
intra-operation and one inter-operation thread because the executor already owns the parallel
admission of model runs, and ONNX Runtime's full graph optimization level, the library's own
default, declared so that an upgrade cannot change it. Whether a session keeps a memory arena and
memory patterns is not a node setting: each inferencer states it in Plexus with `WORKING MEMORY`
(see [Working memory](processors.md#working-memory)), and a binding inspection, which never runs
the model, keeps neither. State admission spins
only with parallel Data workers; its bounded spin is followed by the selected thread yield.

Framing, packed identity bits, sequential single-slot handoffs, accounting granules and persisted
segment sizes remain fixed contracts, with their owning code and format chapters declaring why.
A sleep-forever sentinel is not a maintenance cadence. The checkpoint transfer capacity remains
independent of the guest buffer because non-guest state uses the same transfer owner.

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
   capacities, and checks the listener body limits against the intake pool, the runtime threads
   against the CPU claim and the executor's workers, and the consensus append window against the
   largest replication batch. Every memory pool counts once, including independent active state
   reads and retained state-read cache. Their total plus producer and consumer node credit and the
   configured node and consensus database caches must fit known memory.
   A failed detector leaves capacity unknown and uses the declared capacity-independent defaults.
3. It creates its database directory and its temporary directory when they are absent, proves each
   writable with a probe file it removes again, and measures the total size of the volume each
   lives on.
4. It resolves its restore-staging quota against the total size of the volume that holds the
   database directory: `--restore-staging-max-bytes` when given, otherwise the smaller of 128 GiB
   and that size, and requires the quota to fit that size. The two snapshot staging quotas
   independently fit the measured temporary volume. Omitted snapshot staging is the smaller of
   128 GiB and that volume's capacity; an omitted per-transfer bound is the smaller of 64 GiB,
   the resolved staging quota and that capacity. Each quota is compared with the size on its own,
   and nothing the volume already holds is subtracted. Explicit values are refused when they do not
   fit, never reduced. The immutable node claim and runtime use these same resolved values.
5. It loads its name resolver, as [Name Resolution](name-resolution.md#resolver-lifetime)
   describes.
6. Once every check passed, it detects its [properties](#detected-properties), waiting at most its
   `--node-detection-timeout`. Detection never refuses the start.

Each refusal names the setting or directory it judged, with the values involved:

| Check | Refusal |
| --- | --- |
| Memory budgets | `the execution memory budgets of 229.25MiB exceed node memory 32MiB`, for the executor pools alone, or `memory-limit (500MiB) cannot hold the node's memory budgets of 507.16MiB`, for the complete total |
| Memory watermark | `memory-high-watermark (4GiB) exceeds memory-limit (3GiB)` |
| Directories | `the database directory <path> cannot be created`, `the temporary directory <path> is not writable` |
| Volumes | `the capacity of the volume holding the database directory <path> cannot be read` |
| Quotas | `restore-staging-max-bytes (128GiB) exceeds the 64GiB capacity of the volume holding the database directory <path>`, for a quota the operator gave, or `resource-max-extracted-bytes (16GiB) exceeds the 5GiB capacity of the volume holding the database directory <path>` |
| Resolver | `failed to load the name resolver configuration`, above the cause that resolver loading reports |

Only the restore-staging quota, a node capability, follows the volume. The resource quotas are
cluster parameters with fixed defaults, 4 GiB and 16 GiB, and the database volume must hold each of
them too. The node judges them once it knows the policy it runs under: the bootstrapping node its
seeds, after opening its consensus store and before its first commit, and a joining or returning
node the live cluster's published record, before it reports ready. A node whose database volume is
smaller refuses to start with the same refusal and exit status. Provision a larger volume, seed
smaller quotas on a new cluster, or change an existing cluster's committed quotas through
`ALTER CLUSTER SET` before joining it. The measured size of the database volume also enters the
node's committed claim, so `ALTER CLUSTER SET` cannot raise a quota above it while the node stays
admitted. Against the same policy, after the volume, the node also checks that its drain and
shutdown budgets hold the domain drain and gate budgets, and that `NODE_UNAVAILABILITY_TIMEOUT`
exceeds its `--control-plane-health-probe-interval` plus the probe timeout, a tenth of the timeout.
Either refusal stops the start with the same exit status. A node that passes the preflight logs
`the startup preflight passed` at `info` with its memory limit, its memory budgets, the size of both
volumes, the restore-staging quota it resolved and the properties it detected.
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
| `intake` | 1/16 | 128 MiB |
| `management` | 1/128 | 256 KiB, one consensus vote or metadata write |
| `commands` | 1/32 | 24 MiB |
| `relay` | 1/16 | Enough for its two admitted operations |
| `state-read` | Resolved relay capacity | Enough for its two admitted operations |
| `state-read-cache` | Resolved relay capacity | Enough for its two admitted operations |
| `bulk` | 1/32 | 32 MiB |
| `bulk-window` | 1/64 | 16 MiB |
| `restore-metadata` | 1/8 | Positive |
| `credentials` | 1/256 | 19 MiB |

Derived pools are capped at the 32-bit byte-permit representation. Explicit values are never
silently reduced. With unknown memory, defaults are respectively 128, 8, 24, 192, 192, 192, 32, 16,
2,048 and 19 MiB. Credentials keeps room for mounted NATS files; PBKDF2's operation charge stays
64 bytes and its creation policy remains the committed `PASSWORD_ITERATIONS` parameter.

The management pool must hold its largest operation, one consensus vote or metadata write. The
consensus store reserves four management events for each, 256 KiB with the fixed 64 KiB event (see
[Consensus Storage And Replication](consensus-storage-and-replication.md#storage-working-sets)).
A smaller explicit `--execution-management-memory-bytes` refuses startup before any store opens,
and the refusal names both values. A derived management pool never falls below this working set.

`--execution-relay-encoded-bytes` and `--execution-relay-decoded-bytes` default to one sixth of the
relay pool, capped at 32 MiB each; `--execution-relay-scratch-bytes` defaults to one twelfth,
capped at 16 MiB. All are positive. Validation requires two complete operations to fit relay,
active read and cache pools independently. `--execution-resident-replication-batches` defaults
to at most four, retaining command-budget room for an answer. `--execution-bulk-chunk-bytes`
defaults to one 512th of bulk memory, capped at 64 KiB. The resolved chunk sizes the staged
snapshot encoder's output buffer. Stored branch captures and native restore conversions include
that buffer in their bulk admission before encoding. Captured-section reads group sixteen
bulk quanta, capped at half the immutable bulk pool so a preceding transport buffer and the next
read can coexist; the default read remains 1 MiB. Existing operation working-set checks
still apply. Peer receive limits derive from these final limits before transport construction.

`--client-producer-node-bytes` and `--client-consumer-node-bytes` each default to 1/32 of node
memory, holding at least the default 32 MiB session window, or 128 MiB with unknown memory.
They reserve node admission independently; committed per-session limits keep their own policy.
The full startup total counts both capacities, all ten executor pools and both database caches.
A 1 GiB node uses 555 MiB of executor ceilings, 64 MiB of node client credit and 64 MiB of database
caches, 683 MiB in total. These are ceilings, not eagerly allocated buffers; graph state, engines
and thread stacks need additional headroom. Explicit totals that exceed known memory refuse
startup. [Sizing A Node](sizing.md#what-memory-sizes) shows how the totals scale with memory.

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
published no record it can read outside the membership. The leader keeps its verdict on a refused
run outside the membership, without judging that run again, until the run is no longer an admission
candidate: the run stops once it reads the verdict, yet the failure detector keeps it live in gossip
for several of its heartbeat intervals, about half a minute for a run that lived a second or two,
and a member admitted after its run stopped would never run. Once the voter it named stops, is
dropped or returns with a record the node could run beside, the leader therefore admits the node's
next run, not the refused one.

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
node's identity, its tags, listen and advertised addresses, the session transport mode
(`--grpc-mode`), TLS files, name resolution files, storage paths, the collector it exports its own
traces to, how it bootstraps or joins the cluster (`--allow-bootstrap`, `--cluster-bootstrap-host`)
and its initial administrator (`--default-user`, `--init-default-user-password`). Those are what a
node is provisioned with, not settings. A diagnostic build adds `--deadlock-evidence` and
`--deadlock-active-only`, with `NERVIX_DEADLOCK_EVIDENCE` and `NERVIX_DEADLOCK_ACTIVE_ONLY`, under
their own heading; they choose what the deadlock detector records and checks, and are not settings
either. Every other group is the command-line form of one owner's class-typed group: each option is
named by its definition's
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
| `--gossip-receive-queue-capacity`, `--gossip-tombstone-grace-period` | node capability | [membership](#membership-capabilities) |
| `--raft-snapshot-entry-threshold`, `--raft-snapshot-byte-threshold`, `--raft-covered-log-entries-retained`, `--raft-covered-log-bytes-retained`, `--raft-retained-log-cap` | node capability | Raft retention |
| `--raft-append-batch-entries` and the other [consensus capacities](#consensus-capacities) | node capability | consensus operations |
| `--database-cache-bytes`, `--database-journal-bytes`, `--database-workers`, `--raft-database-cache-bytes`, `--raft-database-journal-bytes`, `--raft-database-workers` | node capability | node and consensus databases |
| `--dns-cache-entries`, `--dns-positive-max-ttl`, `--dns-negative-max-ttl`, `--dns-hook-lookup-budget`, `--dns-address-family-policy`, `--dns-trust-negative-answers` | node capability | name resolution |
| `--interconnect-max-peers` and the other [transport capacities](#transport-resolver-and-database-capacities) | node capability | interconnect transport |
| `--interconnect-relay-batch-bytes`, `--interconnect-relay-decoded-bytes`, `--interconnect-bulk-chunk-bytes`, `--interconnect-bulk-receive-window` | node capability | peer receive bounds |
| `--listener-*`, `--http-body-bytes`, `--http-body-copy-bytes`, `--http-body-timeout`, `--http-refusal-drain-timeout`, `--websocket-message-bytes`, `--websocket-frame-bytes`, `--grpc-header-bytes`, `--grpc-concurrency`, `--grpc-request-timeout`, `--grpc-max-streams`, `--grpc-keepalive-interval`, `--grpc-keepalive-timeout` | node capability | [listener bounds](#listener-bounds) |
| `--vm-*`, `--wasm-stack-bytes`, `--wasm-memory-reservation-bytes`, `--udf-watchdog`, `--connector-retry-multiplier`, `--connector-retry-jitter-percent`, `--kafka-domain-assignment-retry`, `--partition-topology-poll-interval`, `--syslog-intake-queue-capacity`, `--syslog-read-chunk-bytes`, `--mqtt-read-ahead-headroom` | node capability | [engine and connector host](#engine-and-connector-host-capabilities) |
| `--inspection-max-rows`, `--inspection-max-sections`, `--inspection-read-timeout` | node capability | [state inspection](#state-inspection-capabilities) |
| `--entity-labels-max-count`, `--entity-labels-max-key-bytes`, `--entity-labels-max-value-bytes`, `--entity-labels-max-bytes` | cluster parameter | [entity label limits](#entity-label-limits) |
| `--node-unavailability-timeout` | cluster parameter | membership |
| `--replica-count` | cluster parameter | scheduling |
| `--tls-cipher-suites`, `--tls-key-exchange-groups` | cluster parameter | outward TLS algorithm policy |
| `--password-iterations` | cluster parameter | password creation policy |
| `--licensing-request-timeout`, `--licensing-retry-backoff-max`, `--licensing-stream-idle-timeout` | cluster parameter | licensing coordination |
| `--state-snapshot-interval` | cluster parameter | runtime |
| `--domain-drain-timeout`, `--entity-gate-deadline`, `--remote-ack-*`, `--remote-relay-*`, `--payload-unfold-limit` | cluster parameter | cluster policy |
| `--runtime-delivery-capacity` and the data-plane runtime options above | node capability | data-plane capacity |
| `--forced-recovery-budget`, `--ownership-handoff-poll-interval`, `--entity-gate-poll-interval`, `--domain-drain-poll-interval`, `--runtime-readiness-propagation-bound` | cluster parameter | coordination guarantees |
| `--max-batch-messages`, `--client-producer-session-bytes`, `--client-consumer-session-bytes`, `--udf-code-bytes`, `--udf-compile-budget`, `--http-header-bytes`, `--http-header-fields`, `--client-batch-rows` | cluster parameter | model and session policy |
| `--restore-staging-max-bytes` | node capability | runtime |
| `--transaction-idle-timeout`, `--transaction-tombstone-retention`, `--transaction-max-statements`, `--transaction-max-source-bytes`, `--transaction-max-open` | cluster parameter | transaction limits |
| `--command-retry-validity`, `--command-execution-capacity` | cluster parameter | command execution |
| `--memory-high-watermark`, `--memory-low-watermark`, `--memory-pressure-check-interval`, `--memory-pressure-resume-jitter` | node capability | memory pressure |
| `--drain-timeout`, `--shutdown-timeout`, `--task-grace-period` | node capability | shutdown |
| `--resource-max-archive-bytes`, `--resource-max-extracted-bytes`, `--resource-max-file-count` | cluster parameter | resources |
| `--otel-trace-sample-ratio`, `--otel-trace-export-timeout`, `--otel-trace-export-interval`, `--otel-trace-queue-capacity`, `--otel-trace-export-batch-spans` | node capability | trace export |
| `--cpu-count`, `--memory-limit`, `--node-detection-timeout` | node capability | capacity |
| `--execution-*`, `--runtime-worker-threads`, `--runtime-max-blocking-threads`, `--runtime-thread-stack-size` | node capability | execution |
| `--client-producer-node-bytes`, `--client-consumer-node-bytes` | node capability | client admission |
| `--control-plane-*` | node capability | local maintenance and recovery |
| `--session-*` | node capability | session verification, queues and observations |
| `--client-link-*` | node capability | producer and consumer forwarding |
| `--authentication-attempts-per-second`, `--interconnect-tls-reload-interval` | node capability | authentication pacing and local TLS material refresh |

Cluster-parameter options are optional seeds for the first bootstrap. An explicit command-line
or environment value, including a value equal to the default, refuses the start of every node that
is not bootstrapping a new cluster, later starts of the bootstrapping node included.
The help text names the definition's default without supplying an implicit seed option. With known
memory, omitted memory watermarks derive as [Execution Capabilities](#execution-capabilities)
describes. With unknown memory and no explicit pair, the node runs no memory-pressure supervisor.
`--memory-limit` has no default value either: without it the node detects its memory limit.
`--restore-staging-max-bytes` defaults to 128 GiB, and an omitted quota is held to the volume that
holds the node database while a quota the operator gives is never reduced, as
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
| `--udf-watchdog` | 5 s | positive; each call of a compiled Roto UDF that returns later fails its whole batch |
| `--connector-retry-multiplier` | 2 | positive multiplier applied between reconnect attempts |
| `--connector-retry-jitter-percent` | 0 | 0 through 100 percent added to the next multiplied delay, within the declared cap |
| `--kafka-domain-assignment-retry` | 100 ms | positive retry cadence while a domain-offset assignment is unavailable |
| `--partition-topology-poll-interval` | 1 s | positive Pulsar partition inspection and topology observer reconciliation cadence; Kafka inspection follows `--control-plane-kafka-partition-watch-interval` |
| `--syslog-intake-queue-capacity` | 64 frames | positive, within the native channel permit bound; stream intake backpressures when full |
| `--syslog-read-chunk-bytes` | 8 KiB | positive, addressable bytes per stream read, further limited by the frame bound |
| `--mqtt-read-ahead-headroom` | 64 publishes | positive, and MQTT's widest Receive Maximum, 65,535, plus it within the native channel permit bound; what every ACK-mode `CONNECT` advertises beyond the acknowledgement window, up to 65,535, the ACK-mode QoS 0 quota, whose overflow is dropped and counted, and the whole `NO ACK` read-ahead |

The Kafka durations and topology polling interval accept at most `u32::MAX` whole seconds. VM programs carry the compiling
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

`--udf-watchdog` is checked when a Roto UDF call returns: a call that took longer fails its whole
batch, and each node judges the calls it runs by its own value. It cannot interrupt native code
that never returns; [User-Defined Functions](udfs.md#nulls-errors-and-volatility) states what an
expired watchdog reports. How long compiling a UDF may take is the `UDF_COMPILE_BUDGET` cluster
parameter instead, because exceeding it refuses the statement that declares the function.

The connector retry law preserves the initial delay and ceiling of `RETRY POLICY`; it changes
only later delays. Kafka, MQTT and the shared ingestor/emitter host receive the same law.
External broker timeouts, connection pools, keep-alive and request behavior remain declared client
`CONFIG` keys or entity clauses. Read the owning connector section in
[Ingestors](ingestors.md) or [Emitters](emitters.md) before choosing those values.

The MQTT read-ahead headroom is the host's choice for every MQTT topic ingestor the node runs,
beside each statement's `ACK PARALLEL MAX`: a larger headroom keeps a connection reading longer
while the host holds a batch, and in an `ACK` mode lets more QoS 0 publishes wait instead of being
dropped, at the cost of memory for that many more admitted publishes per connection, each at most
the client's `max_packet_size`. The connector's declaration names the setting as the capacity of those
read-ahead buffers, so the [generated capability matrix](connector-capabilities.md) shows it.

### Membership Capabilities

The membership group holds two local `NodeCapabilities` beside its gossip interval and clock-skew
tolerance prerequisites. They add no prerequisite: nodes with different values run together, and a
change takes effect when that node restarts.

| Server option | Default | Owner and bound |
| --- | --- | --- |
| `--gossip-receive-queue-capacity` | 1,024 datagrams | positive, within the native channel permit bound; incoming gossip datagrams queued between the interconnect handler and Chitchat, each at most the 60 KiB gossip bound, so the queue retains up to its capacity times 60 KiB |
| `--gossip-tombstone-grace-period` | 60 s | positive, at most `u32::MAX` whole seconds, so Chitchat can add it to the instant a key was deleted; how long a deleted gossip key's tombstone stays before Chitchat collects it |

Each has the environment twin `NERVIX_` followed by its uppercase name with underscores. Both are
part of the incarnation-bound capability claim, appear in the startup preflight log, and
`DESCRIBE CLUSTER` prints them on the node's `membership:` line after its interconnect line, such as
`membership: gossip-receive-queue-capacity=1024 gossip-tombstone-grace-period=1m`.
[Cluster Interconnect](interconnect.md#membership-consensus-and-bulk-transfer) owns what the queue
and the grace govern.

The bus that carries each change of the live set from the membership task to the session notice
relay has no option of its own. Its one subscriber republishes each change as exactly one
`Membership` notice, so it never needs more room than the notice backlog: its capacity derives from
the resolved `--session-notice-capacity`, 256 by default, among the
[session capabilities](#control-plane-and-session-capabilities).

### Validation Before Any I/O

The edge validates every value, and every relationship between values, before the node opens a
store, binds a listener or starts a task:

- The Raft heartbeat interval is at least one millisecond, the resolution consensus runs timings
  at, and twelve of it, the longest election timeout, fit the milliseconds consensus counts it in.
- The gossip interval and the clock-skew tolerance are each at least one millisecond, and twenty
  gossip intervals fit a duration. The gossip receive queue fits the native channel permit bound
  and the gossip tombstone grace period is positive and at most `u32::MAX` whole seconds.
- The MQTT read-ahead headroom, beside MQTT's widest Receive Maximum, fits the native channel permit
  bound.
- A memory limit and a CPU count, when one is given, are above zero, and so is the node detection
  timeout.
- The node's tags follow the [tag rules](#tags): at most 64, each key once.
- The drain timeout does not exceed the shutdown timeout, which bounds every drain step. A drain
  timeout equal to it lets the drain use the whole deadline; the defaults leave time for terminal
  teardown after the drain.
- The memory watermarks are set together or not at all, the low one below the high one, and with
  them the check interval is above zero.
- The trace sample ratio lies between 0 and 1, and a trace export batch holds no more spans than
  the trace queue.
- A trace collector endpoint, when one is given, is a URI; the empty text is not.

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
[Sizing A Node](sizing.md#rolling-changes) gives the operator's procedure for each class and how
`DESCRIBE CLUSTER` reads while a roll is in progress.

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

`RESOURCE_MAX_ARCHIVE_BYTES` and `RESOURCE_MAX_EXTRACTED_BYTES` must each fit the database volume
that every admitted node's committed claim carries. A transition that raises either above one of
those volumes is refused before anything commits, naming the parameter, the node and both sizes:
`resource-max-extracted-bytes (8EiB) exceeds node 'node-1' database volume (1.82TiB)`. A departed
node's retained claim does not count. The check shares the commit fence with node announcements:
a node whose new claim carries a volume the committed quotas do not fit has its announcement
refused, and its startup preflight already refuses it, naming the volume's directory. Each claim
records the size its run measured at start, so grow a node's volume and restart that node before
raising a quota above its old size.

`NODE_UNAVAILABILITY_TIMEOUT` must exceed every admitted node's
`--control-plane-health-probe-interval` plus the probe timeout, a tenth of the timeout. A health
observation stays fresh for the timeout, and a node observes each peer again at most one interval
plus one probe timeout after its last observation of it. A shorter timeout leaves every
observation stale before the next one arrives: stale observations no longer keep a peer available
through a short gossip lapse, and a failing peer's run of failures restarts with each probe, so it
is never declared unavailable. A transition to such a timeout is refused before anything commits,
naming the node and the three durations: `node-unavailability-timeout (1s) must exceed node
'node-1' control-plane-health-probe-interval (1s) plus probe-timeout (100ms), a tenth of
node-unavailability-timeout`. A departed node's retained claim does not count. The check shares
the commit fence with node announcements: a node whose new claim carries an interval the committed
timeout does not fit has its announcement refused, and the node refuses its bootstrap seeds or the
live committed policy before readiness with the same refusal, naming itself. Raise the timeout
before lengthening a node's interval, and shorten the interval before lowering the timeout.

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

`UDF_COMPILE_BUDGET`, 10 seconds by default, bounds the wall-clock time of compiling one Roto UDF
and running its `test` blocks. It is a cluster parameter rather than a node capability because
exceeding it refuses `CREATE UDF`. Model admission compiles under the budget of the record it
captured for validation, and each node compiles a domain's UDFs under its installed record when it
activates or rebuilds the domain. The verdict stays the compiling node's own wall-clock
measurement, so it depends on that node's speed and load. A transition rechecks no committed UDF;
[User-Defined Functions](udfs.md#compile-budget) states what a lowered budget refuses later.

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
cap, the parameters' own relationships must still hold, and every admitted node's drain and
shutdown budgets must still hold the domain drain and entity-gate budgets. Apply committed changes
live. Rebuild an owner that captures settings at construction over its existing storage. The
current record changes no OpenRaft construction setting: heartbeat and election timing remain
prerequisites. Committed and running revisions are published together, and delayed observations
cannot move them backward.


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

The one-minute and fifteen-minute metric EMA windows, their decay conversion, the steps of the
percentile windows, the unit and precision of the HDR histograms that metric snapshots carry, the
published histogram buckets and the published connector-interaction duration buckets are fixed
observation contracts. Their declarations carry the fixed reasons in the metric owners; they are
not node settings. The current resolution is documented in
[Metrics And Observability](metrics-and-observability.md#connector-interaction-metrics).

The per-session bounds of the [client session protocol](client-session-protocol.md) (requests in
flight, producers, consumers and batch credit), the chunk bounds both peers of a stream check, the
process exit statuses and the forced-exit report bound of [Shutdown](shutdown.md#exit-status), and
the absence markers of packed atomic encodings are fixed the same way, each at its owner. A memory
charge that mirrors a buffer sized by `--execution-bulk-chunk-bytes` derives from the resolved bulk
chunk rather than repeating its default.

The shared session codec in `nervix-client-wire` fixes the WebSocket close codes RFC 6455
assigns, the frame header and the table and traversal budgets of rule F-3 of the
[Client Implementation Manual](client-implementation-manual.md#frames), the envelope bounds and
table depths the session schema's frame layouts fix, and the 64 KiB below which its gRPC decoder
copies a frame out of the read buffer (rule F-9). It takes each session limit's default and bounds
from that limit's own setting definition rather than restating them.

The `nervix::hidden_tunable` compiler diagnostic enforces this section. It refuses a numeric
constant of a governed type, such as `Duration`, an unsigned or signed integer, `f64`, `NonZero`
or `ByteUnit`, that is neither a setting definition's default, a class group's default nor a
fixed constant, and a numeric literal that chooses a duration, a capacity, a channel capacity or
a permit count anywhere else. Tests, benchmarks, examples, tooling and the arbitrary, benchmark,
model-harness and test-environment crates are outside it. A value kept outside a setting while its
owner designs the carrier carries a narrow, reason-bearing expectation on that one declaration or
operation, naming its repair task; the connector values that belong to a connector's Plexus
surface and the web console's timings are retained this way.
[Hidden Tunables](architecture-enforcement.md#hidden-tunables) owns what the rule recognizes, how
it resolves types and callees, and which compiled configurations it sees: like every compiler rule,
it judges only code a declared configuration compiles, and it does not discover implicit library
defaults.

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

The licensing stop's forced ending, `LICENSING_STOP_ENFORCE_AFTER`, is 60 seconds and a fixed
constant, never a setting, flag or clause. It is a licensing restriction rather than a tunable: a
temporary license is not meant to serve real traffic, and the cluster must be stopped one minute
after its expiry. No test build shortens it. See [Stopping A
Domain](./domains-and-time.md#stopping-a-domain).

Sentry's 1,000,000-byte event limit is a fixed external ingestion contract. The connector
vocabulary declares its one constant; [Sentry's event contract](emitters.md#sentry) defines which bytes
count toward the limit.

`Setting<D>` copies or compares a resolved value when its semantic value supports those
operations, without imposing value traits on the definition marker. Generic debug formatting
identifies the declaration; the owning settings renderer controls value exposure and redaction.

The declaration API has value, default and relationship tests plus paired Rustdoc examples.
The `nervix::hidden_tunable` rule has allowed, rejected and resolution-only compiler fixtures with
exact diagnostic spans, and fulfilled, unfulfilled, widened and invalid expectation checks.
The declarations alone add no Cucumber scenario and change no lock or lifecycle
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
