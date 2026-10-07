#  Sessions

Nervix supports session-local commands over its session protocol.

These commands are not persisted in the registry:

```nspl
CREATE SUBSCRIPTION acme_notifications TO notifications WHERE tenant = 'acme';
CREATE SUBSCRIPTION sampled_telemetry TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE input.tenant = 'acme';
DELETE SUBSCRIPTION acme_notifications;
DESCRIBE RELAY notifications WHERE (tenant = 'acme');
ATTACH DOMAIN CLOCK;
DETACH DOMAIN CLOCK;
```

Following a domain clock is not a subscription; see
[Domain Clock Attachment](#domain-clock-attachment). Publishing to a client ingestor uses
[producers](#producers), which are not statements either.

The [Client Ingestors And Emitters](./client-io-architecture.md) architecture chapter connects
these transient handles to their persisted graph endpoints, ACK boundaries and restoration.

This page describes sessions as a user of NSPL sees them. [Client Session
Protocol](./client-session-protocol.md) explains the protocol that carries them, and the [Client
Implementation Manual](./client-implementation-manual.md) states what a client implementation must
do.

Current session behavior:

- subscription creation validates the statement against the relay as the cluster schedule declares
  it, and attaches only while the serving node declares the relay with that same definition, whether
  or not it owns the relay and whether or not the domain is running
- subscription names are unique within one connected session and may refer to relays in different domains
- `DELETE SUBSCRIPTION` resolves only the session-local subscription name, independent of the currently active domain
- subscribing to a relay collects records from all active branch groups for that relay
- a branched subscription reports the concrete branch key of every batch of rows it delivers, and
  the text view repeats it on each row; sensitive branch-key fields are masked using the same rules
  as sensitive relay fields
- subscriptions are read-only views; only an optional `WHERE` predicate is supported, and a
  selected record is delivered without construction or transformation
- the predicate is an ordinary `BOOL` expression, including membership, range, and null-safe
  equality tests such as `input.status IN ('open', 'held')`; it selects a record only where it is
  true, so a null predicate, such as `IN` over a null field, does not select it
- bare fields, `message.<field>`, and `input.<field>` all read the subscribed relay record; the
  compiler rejects `output`, `branch`, and `relay_state` scopes when the subscription is created
- subscription syntax does not accept `INHERIT`, `SET`, `VALUES`, `INVOKE`, or other side effects
- a subscription with a `WHERE` predicate reads one snapshot of the domain's clock on the serving
  node for each batch it evaluates; every predicate expression and volatile UDF call for that batch
  sees the same instant, and a batch whose domain time cannot be read is skipped and reported
- optional `BATCH SAMPLE RATE <rate>` samples each selected row, after `WHERE` has been evaluated,
  with a pseudo-random draw; a rate of `1` delivers every selected row and `0` delivers none
- `BLOCKING` delivery waits for room in the session's subscription queue, which holds back the
  relay it reads, while `DROPPING` discards rows when that queue is full and reports how many it
  discarded before its next rows; the queue holds 16 frames shared by every subscription of the
  session, and a count still outstanding when the subscription ends is not reported
- subscription changes belong to the session rather than to a transaction, so `CREATE SUBSCRIPTION`
  and `DELETE SUBSCRIPTION` are refused while the session holds a transaction
- subscription events are delivered asynchronously to the connected client session
- the relay owner is the sole subscription fan-out source, so each admitted batch is delivered at
  most once to a subscription even when producers and consumers run on several cluster nodes
- when the subscription's cluster node also hosts a runtime consumer, subscription delivery
  piggybacks on the same owner-to-node Arrow IPC batch instead of adding another serialized copy
- runtime and server errors are also delivered asynchronously
- cluster membership updates are also delivered asynchronously

Sessions are runtime-facing protocol interactions, not part of the persisted namespace model.

A terminal reply may hold up to 64 MiB. A reply above the 4 MiB frame bound is carried as ordered
transfer parts that each fit one frame, then validated and reassembled before the client exposes
it. This is how a rendered transaction report larger than one
frame reaches the Rust client, CLI, and browser without truncation. A reply above the transfer bound
is rejected whole; no client receives a successful partial result.

## Subscription Lifecycle

A subscription is identified by its name together with the generation its session assigns when it
opens it. Every frame about a subscription carries both, and a name reused after deletion opens a
new generation, so a frame about an earlier generation is never taken for a later one. A
subscription belongs to the session that created it and moves through one lifecycle:

- **Creating.** The server validates the statement, makes the node's interest in the relay visible
  to every live node, and attaches to the relay. A refusal at any step leaves nothing behind: no
  interest, no attachment, and the name remains free. Reopening after the last subscription closed
  waits for the renewed interest to become visible; an advertisement from before that closure does
  not establish readiness.
- **Active.** The reply that opens the subscription carries the schema of its rows, and no row
  precedes it. When that reply cannot be delivered, because the request was cancelled, the session
  ended, or the reply did not fit the session limits, the subscription is abandoned before it
  delivers anything.
- **Ended by the server.** When its relay is redefined, so that a field's name, position, type,
  nullability, or sensitivity changes, the relay becomes branched or unbranched, or its branch or
  branch key schema changes, the subscription receives `SubscriptionEnded` with reason
  `RelayChanged`. When its relay or the relay's domain is removed, the reason is `RelayRemoved`.
  Either is the last frame about that generation, and no row of a redefined relay reaches a
  subscription announced under its earlier definition. Stopping and starting a domain, and
  rebuilds that keep the relay's definition, keep its subscriptions delivering. An ended
  subscription can still be deleted by name, and its name can be used again once its end has been
  sent; because replies travel ahead of queued subscription frames, that end can reach the client
  after the reply that opens the new generation.
- **Deleted.** `DELETE SUBSCRIPTION`, or the end of the session, stops the subscription at once
  even while its client reads nothing, and discards the frames it still has queued. Nothing about
  that generation follows the reply that deleted it.

Replies and session events travel ahead of the subscription rows a session has queued, so rows
waiting for a slow client never hold back a command reply or the reply to the unsubscribe that stops
them. Frames already handed to the transport stay in order: a reply follows the rows the transport
took before it. A node advertises
interest in a relay while at least one of its subscriptions holds it; see
[Cluster Interconnect](interconnect.md) and the `nervix_session_subscriptions` and
`nervix_session_subscription_dropped_rows_total` series in
[Metrics and Observability](metrics-and-observability.md).

Subscriptions are live views. They do not replay rows, keep durable offsets, or deliver exactly
once. A session is told of the rows it lost where the server knows of them: a `DROPPING`
subscription reports the rows it discarded, and rows a filter could not evaluate, rows of a batch
whose domain time could not be read, and every selected row of a batch the encoder could not fit
into a frame are reported as skipped. Rows in transit can also be lost without a report while
relay ownership moves between nodes or a node-to-node delivery fails. [Row
Subscriptions](./client-session-protocol.md#row-subscriptions) lists every gap and whether it is
reported.

Typed Row subscription frames carry a `BYTES` field as raw octets in a `BytesCell`; clients read
the value as borrowed bytes, including empty and non-UTF-8 sequences. JSON subscription views
render that field as padded standard base64 text. Sensitive byte fields are redacted in both views.

Persistent administrative commands carry a stable execution reference. The cluster binds that
reference to the authenticated owner, selected domain, and semantic command, continues admitted
work after the session disconnects, and retains one terminal result for the command retry
validity, 15 minutes by default. The reference is a UUIDv7 whose creation time bounds how long it
may be retried. The CLI, web console, and Rust client reuse the reference through redirects and
reconnects. Reuse for changed content fails. While a long command waits, server events,
subscription delivery, completion, and transaction inspection continue independently; the
session's later commands wait for it, because a session runs its commands in the order it received
them. See [Command Completion](command-completion.md) and [Exact
Recovery](./client-session-protocol.md#exact-recovery).

## Domain Clock Attachment

A session can follow the clock of a domain: its `START` generation and what the serving node has
installed for it. `ATTACH DOMAIN CLOCK;` starts following the clock of the active domain and
`DETACH DOMAIN CLOCK;` stops following it. Clients send them as an `AttachDomainClockRequest` and a
`DetachDomainClockRequest` naming the active domain, so each addresses the domain active when it
runs. Selecting another domain with `USE` does not change what the session follows, and a session
follows several domain clocks by attaching while each domain is active.

An attachment is keyed by its domain alone. It is not a subscription: it reads no relay, has no
name or generation of its own, and a session follows each domain clock at most once. The observed
clock carries the `START` generation and one state:

- **Stopped.** The generation exists but executes no domain work.
- **Uninstalled.** A paced generation is running, but the serving node has no assigned clock
  authority or mapping for it, so it cannot execute paced work.
- **Unpaced.** The domain reads actual UTC.
- **Paced.** The committed mapping the domain executes with: `PERIOD`, `SKEW`, the logical origin,
  the UTC anchor, and the time rate. Timestamps are signed Unix nanoseconds, period and skew are
  unsigned nanoseconds, and the rate is a double.

An attachment moves through one lifecycle:

- **Attached.** The reply carries the clock as the serving node has it installed, and no frame about
  the domain precedes it. A second attach is refused as already attached, and a domain the serving
  node does not have is refused as not found. A node that has not installed the cluster's committed
  domains since it started, as right after a restart, answers only once it has, so it never refuses
  a domain the cluster has as not found. That wait ends early only with the session. When the reply
  cannot be delivered, because the request was cancelled, the session ended, or the reply did not
  fit the session limits, the attachment is abandoned before it delivers anything. If the
  installation has not changed and the node already holds a tick of that generation, its newest
  tick is the first frame after the reply.
- **Following.** Each later change of the installed clock arrives as a `DomainClockObserved` frame
  carrying the new clock: `STOP` delivers stopped, a `START` delivers its generation and mapping,
  and a paced generation left without an assigned clock authority delivers uninstalled, then its
  mapping again once an authority is assigned. A frame carries the clock installed when it is sent.
  Changes made while an earlier frame waits for room on the session arrive together as the newest
  clock, and a change that leaves the clock as the client last received it sends nothing: moving the
  authority to another node and the pause that alters a running model are not observed. For a paced
  installation, each newer accepted tick arrives as a `DomainClockTicked` frame with its generation,
  one-based id, logical boundary, the authority's UTC observation, and the serving node's own
  logical reading taken when it built the frame. A tick of a new generation follows that
  generation's state frame. Ticks are replaceable on the control lane: each attached domain holds
  at most one queued tick, updated to the newest while the client is slow. Tick ids may skip after
  coalesced periods. An unpaced or uninstalled clock emits no ticks.
- **Detached.** `DETACH DOMAIN CLOCK` stops delivery before its reply, so nothing about the domain
  follows the reply. A detach without an attachment is refused as not attached.
- **Ended by the server.** When the domain no longer exists on the serving node, the session
  receives `DomainClockAttachmentEnded` with reason `DomainRemoved`, the last frame about that
  attachment. A request sent after that frame finds the attachment gone: an attach attaches again
  and a detach is refused as not attached.
- **Session end.** The attachment ends with the session, and nothing is sent about it. The Rust
  client, the CLI, and every host of the shared C binding attach every clock they followed again on
  their next session, which delivers the clock as it is then and the newest tick its serving node
  holds. That tick can repeat one the client already received, or precede it when another node
  serves the new session.

Both statements run in order with the session's commands and are refused, with the session-local
refusal, while the session holds a transaction. Neither is persisted or becomes transaction
content. See [Domains And Time](domains-and-time.md#following-a-domain-clock) for what a client
computes from a paced clock.

## Producers

A session can publish typed batches to a [client ingestor](ingestors.md#client-ingestors) through
producers it opens. A producer is not a statement: clients open, feed, and close it with the
`OpenIngestor`, `SubmitBatch`, and `CloseIngestor` requests, and the Rust client exposes it as
`Client::open_ingestor`; see [Client Library](client-library.md#producers).

- An open names its domain explicitly, so `USE` never retargets a producer, and it is answered on
  the session's ordered lane with the ingestor's input schema, its `START` generation, the endpoint
  contract and attachment it bound to, the ingestor's acknowledgement policy, the credit it was
  granted, and whether admission is open. A refused open leaves nothing attached.
- Producers belong to the session, not to a transaction: an open is refused while the session holds
  a transaction.
- A session holds at most 32 producers and 32 MiB of producer credit; the serving node holds at most
  128 MiB for every producer it serves or retains batches for.
- A submitted batch is handed to its producer without the session's receive loop waiting on it, so
  commands, subscriptions, domain clock frames, and every other request keep moving while batches
  await their graph outcome. Batches are not counted against the session's in-flight request limit;
  the producer's credit bounds them instead.
- Every batch receives exactly one terminal reply: not admitted, completed, processing failed, or of
  unknown outcome. A batch cannot be cancelled once sent; `CancelRequest` is refused for it, because
  withdrawing an admitted batch is not possible.
- A batch sent beyond the producer's credit is refused and ends the producer as a protocol
  violation. Its earlier batches still receive their outcomes.
- The session tells the client when a producer's admission is suspended or open again, and when the
  server ends a producer, which is the last frame about it. Closing a producer refuses its queued
  batches, waits for its admitted ones, and is answered once every batch has its outcome.
- When the session's node forwards a producer to the node that executes its ingestor and loses that
  node, whether it crashed or stopped answering, the producer ends as `owner lost`. Before the end,
  every batch that node could not have admitted is refused as `producer ended`, because the session's
  node clears each forwarded batch before it may be admitted; only the cleared batches are reported
  as of unknown outcome.
- When the session ends, its producers detach. Admitted batches continue through the graph with
  nobody left to answer them, so a client reports them as of unknown outcome. The Rust client keeps
  the producer handle and restores a fresh attachment if the domain generation and endpoint
  contract still match; it does not resend those uncertain batches.

## Emitter consumers

An application opens a consumer of a `TO CLIENT` emitter by naming its domain, emitter, and the
exact output fields, including order, optionality, and sensitivity. The open reserves room for
one maximum-sized Arrow delivery before reporting success. A session holds at most 32 consumers
and 32 MiB of consumer credit; the serving node reserves at most 128 MiB for this direction,
independently of its producer budget. Thus one session's producer and consumer ceilings total
64 MiB and a node's total 256 MiB. No consumer means retained output fills the bounded node
budget and backpressures the graph.

`OpenEmitter`, `ReadEmitterBatch`, `SettleEmitterBatch`, and `CloseEmitter` are session operations,
not relay subscriptions. A read is concurrent with the session's ordered control lane; the
receive loop can keep processing producer submissions, ACKs, commands, and clock observations
while that read waits. A returned batch is still unacknowledged. Its current reference must be
confirmed with `ACK`, retried, or rejected; close and session loss revoke unresolved attempts.
The server attachment belongs to its session exchange. The Rust client keeps the consumer handle
and restores a fresh attachment when the generation and endpoint contract still match. The first
read after a gap reports an interruption. Delivery references from the former attachment cannot
ACK through the new one. The server does not persist a consumer position. A session may attach
through a different node from the emitter; that node forwards output and settlement over the
authenticated interconnect. [Client Session Protocol](./client-session-protocol.md#emitter-consumers)
defines the frames, and [Emitters](./emitters.md#client-emitters) defines their graph ACK boundary.

## Suggestions

The session's `SuggestRequest` carries the full NSPL input, a UTF-8 byte cursor on a character
boundary, the selected domain, a page size from 1 through 100, and an optional continuation.
The server asks the composed client/server grammar for expectations at that cursor, then resolves
semantic references from one read of the selected domain's committed configuration with the
authenticated session's ordered queued transaction prefix applied. The prefix includes staged
model creation, alteration, renaming, and removal. Other sessions see committed configuration.
For `ALTER SCHEMA` and `ALTER WIRE ... SCHEMA`, field references are drawn from the named schema
after that prefix has been applied, so a staged field rename or removal changes the available
candidates immediately.
Inside a route expression, completion also offers ordinary VM builtins. For a junction,
`input.<field>` resolves through the input relay's declared schema, and a `SET` field resolves
through the output relay's declared schema. Qualified `udf::<name>` calls use the domain's UDF
models, including the session's queued changes. In an ingestor construction expression,
`read_header` and `read_headers` are offered only when the selected source supports header reads.
In an emitter `INVOKE` expression, `write_header` is offered only when the selected sink supports
header writes.

Each suggestion carries a display value, a kind, and a text edit with UTF-8 byte start and end
offsets and replacement text. Clients apply that edit to the original input; they do not infer the
range from the display value. A local path suggestion asks the native CLI to search its own
filesystem and carries the path fragment's source range. The grammar offers one wherever a
statement names a file or directory on the client's machine: the directory of `UPLOAD RESOURCE`,
the destination of `BACKUP`, and the archive `DESCRIBE BACKUP` or `RESTORE` reads. The server sorts
and deduplicates the candidate set before returning a bounded page. A continuation binds the input, cursor, domain,
configuration revision, and candidate set; if any of these changes, the server returns
`StaleContext` instead of silently paging through a different set.

`Ready` with no suggestions means there are no matches. `MissingContext` means a domain required
for a semantic reference is absent or unavailable. `StaleContext` means a transaction binding or
page basis no longer matches. `LookupFailed` means the semantic configuration read failed. These
statuses are part of the suggestion reply, so a client can distinguish them without interpreting
an empty candidate list.

Suggestions are read-only session requests. They can complete while a command is still pending;
they neither enter the command admission gate nor change the transaction queue.

## Backup Downloads

The session service's `DownloadBackup` method is a server-streaming gRPC call that sends the
archive of one completed backup. Its single request names the backup's execution reference. The
call authenticates from its metadata before it reads the request, like every session method, and a
call without valid credentials ends with `UNAUTHENTICATED`.

A node that retains the archive answers with a start frame carrying the archive's total size and
BLAKE3 digest, then the archive's bytes in chunks of at most 256 KiB, then a completion frame. It
reads the archive only as fast as the client takes frames, at most four frames ahead, so a slow
client holds back the reads instead of growing what the node holds for it. Every other answer is a
single frame:

| Answer | When |
| --- | --- |
| Refused `InvalidRequest` | The request does not decode. |
| Refused `Expired` | The retry validity of the execution reference has ended. |
| Refused `NotOwner` | Another user ran the backup. |
| Redirect to the leader | The node does not retain the archive and is not the leader. |
| Refused `NotRetained` | The leader does not retain the archive: a download already collected it, the node that assembled it restarted, or the reference names no backup. |
| Refused `ReadFailed` | The node could not read the archive it retains; the archive stays retained. |

The first download whose completion frame was queued collects the archive, and a later download of
it is refused. A download whose client goes away releases only its own hold, so the archive stays
retained for the client to download again. The call is not bounded by the request timeout; the
Rust client bounds the wait for each frame by it instead, and starts a failed download again from
the first byte. [Backup And Restore](backup-and-restore.md) describes backups and their archives.

The web console makes the same call over a WebSocket of its own on the console listener,
`/console/backups/download`, authenticated like the console session. The connection carries the one
request and the frames that answer it, one frame per binary message, and the node closes it
normally after the last frame. The console checks the archive against the backup's summary exactly
as the Rust client does before it hands the archive to the browser.

## Restore Streams

The session service's `RestoreBackup` method is a client-streaming gRPC call that carries one
restore and its archive, and a single reply answers it. The call authenticates from its metadata
before it reads a frame, like every session method, and a call without valid credentials ends with
`UNAUTHENTICATED`.

The first frame is the restore's start. It names the request identity of the reply, the restore's
execution reference, the `RESTORE` statement as canonical NSPL, and the archive's exact size and
BLAKE3 digest. Every later frame carries the next bytes of the archive, at most 256 KiB each. The
execution reference, the statement, and the archive's size and digest together are the restore's
request identity, so sending them again joins the same restore.

The reply is either the restore's outcome as a command outcome, or a typed refusal of the stream
itself, which leaves nothing changed:

| Answer | When |
| --- | --- |
| Refused `InvalidStream` | The first frame is not a start, a later frame is not a chunk, or a frame does not decode. |
| Refused `InvalidStatement` | The start does not name exactly one `RESTORE` statement. |
| Refused `SizeMismatch` | The chunks add up to more, or fewer, bytes than the start declares. |
| Refused `DigestMismatch` | The archive's bytes do not have the digest the start declares. |
| Refused `QuotaExceeded` | The archive is larger than the leader stages, or its staging area cannot hold the archive now. |
| Refused `StagingFailed` | The leader could not write the archive to its staging area. |
| A redirect outcome | The node is not the leader. |
| An outcome at once | A restore under the reference already finished, which answers with its recorded outcome, or still applies on this node, which answers `OutcomeUnknown` with the `StillApplying` cause. |
| The restore's outcome | The archive arrived, and the restore was refused, failed at a step, or completed. |

A refusal is not recorded: sending the stream again stages the archive again. The leader stages
the archive only as fast as the client sends it and refuses, rather than queues, an archive its
staging area cannot hold now. The call is not bounded by the request timeout, because an archive
may take far longer to send than a command takes to run; the Rust client bounds each frame by it
instead, and then the wait for the reply once the last frame was sent. A call that ends before the
last frame arrived changes nothing and releases what the leader staged for it. Once the whole
archive arrived, the restore goes on without the call.
[Backup And Restore](backup-and-restore.md#restoring) describes restores.

The web console makes the same call over a WebSocket of its own on the console listener,
`/console/backups/restore`, authenticated like the console session. A WebSocket client cannot
half-close the stream and still read the reply, so the stream ends with the chunk that completes the
size the start declares, and the node answers on the open connection, then closes it normally. A
connection that closes before that chunk ends the call as a failed gRPC call does.

## Structured Choices

`ChoiceLookupRequest` resolves values for structured client controls without constructing partial
NSPL. It carries a semantic target, typed dependent selections, search text, a page size from 1
through 100, and an optional page cursor. Targets resolve domain pace, placement policy, and a
domain's internal schemas, each wire-schema format, branches, relays, codecs, VHOSTs, signaling
protocols, relay fields, codec output fields, resource catalogs, and completed resource versions.
Ingestor controls additionally ask for a source reference by transport, a decoding codec, relays
with exact branch declarations, and fields of a selected branch's key schema. Each source target
requires a domain and returns clients of its transport, except endpoint ingestion, which returns
endpoints. The ingestor codec target requires a domain and filters out codecs without decoding.
An unbranched relay target requires a domain and also supplies ingestor error relay choices, while
a branched relay target requires a domain then a branch model reference. Branch fields require the
same pair. Decoded fields and output fields use the existing codec-field and relay-field targets.
Placement requires exactly one domain-pace dependency. Schema, wire-schema, branch, relay, VHOST,
signaling-protocol, resource, and codec lookups require exactly one domain reference. A
relay-field lookup requires that domain reference followed by a relay model reference, and returns
the fields of the relay's records in the order its schema declares them; the other configuration
lookups order their models by name. A codec-field lookup requires the domain followed by a codec
model reference and returns the fields of that codec's output schema in declaration order. A
completed-version lookup requires the domain followed by a resource reference. It offers `LATEST`
and each completed uploaded version, excluding applying or
failed uploads. Resource catalog choices include resources staged in the session's attached
transaction. These lookups read the domain's current Models with the
requesting session's attached transaction prefix applied, so a model staged earlier in that
transaction appears before commit. An absent or differently typed dependency, a domain that does
not exist, or a relay or codec the configuration no longer has returns `MissingContext`. A relay
or codec whose output schema the configuration does not declare returns `LookupFailed`.

Each result separates semantics from presentation. `ChoiceValue` carries a domain-pace or
placement-policy variant, a typed domain, resource, or model reference, or a reference to a field
of the record the dependencies select, or a requested resource version as an explicit number or
`LATEST`. `ChoicePresentation` carries its label, optional detail, and
optional group; a relay or codec field's detail is its exact type followed by `OPTIONAL` and
`SENSITIVE` as its schema declares them. A client selects by the typed value and never derives
behavior from the label. `Ready` with no values is an ordinary empty match; `MissingContext`,
`StaleContext`, and
`LookupFailed` remain distinct outcomes.

A page cursor binds the target, every dependent value, search text, application revision, and the
ordered typed candidate set including its presentation metadata. Changing any part returns
`StaleContext` instead of continuing through a different result. Schema, branch, and relay pages
also bind the canonical definition of each matching model, as do VHOST and signaling-protocol
pages. Relay-field pages bind the relay
and its schema, so a change within an attached transaction invalidates a cursor even when names and
field counts stay the same. Wire-schema pages bind their canonical definitions; resource pages
bind names and completed-version counts, and version pages bind the offered version values. Choice
lookups are read-only and can run
concurrently with each other and with commands. The web console additionally correlates
each lookup with its control, draft revision, and session generation, so a late reply cannot
replace the choices for a newer edit or connection.

## Transaction Binding

An NSPL transaction is replicated control-plane state, but its binding to a live session is
leader-local soft state. A session may bind one transaction, and a transaction may be bound to at
most one session. The session protocol can attach by transaction id; the authenticated user must
match the transaction owner. A later attach takes over the binding and the displaced session gets
an explicit takeover error on its next transaction operation.

A transaction also carries the domain it is bound to, and every statement queued in it must name
that domain. The CLI, web console, and Rust client adopt the transaction's domain as their selected
domain when they attach it, and refuse `USE` while a transaction is active.

The CLI, web console, and Rust client retain the transaction id, each append's execution reference
and expected position, and the commit execution reference. They automatically attach after a
redirect or transport reconnect before resuming a command. A leader that has no binding for the
session's transaction answers with a distinct detached result rather than an ordinary error, and
the client attaches again and replays the command instead of surfacing it. An unclean transport loss, node loss,
or leadership change therefore leaves an open transaction intact until attach or idle expiry. A
client matches the attached progress to the outstanding append or commit and does not satisfy that
request from an unrelated transaction count. During election convergence, a client also retries a
bounded interval when a peer cannot yet advertise the new leader. A session that its client closes
cleanly, with no request in flight, reverts the open transaction bound to it on the leader; any
other ending only releases the binding. Ending a session never reverts admitted append work or a
transaction whose replicated state is already `COMMITTING`; the leader finishes it without a
client.

Finished transaction outcomes remain available during tombstone retention. Attach during that
window reports `COMMITTED`, `FAILED`, `REVERTED`, or `EXPIRED` with the structured final status and
the transaction's aggregate outcome. After retention, attach reports an unknown transaction id. See
[Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions) and [Transactions Over
The Protocol](./client-session-protocol.md#transactions-over-the-protocol).
