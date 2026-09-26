#  Sessions

Nervix supports session-local commands over its session protocol.

These commands are not persisted in the registry:

```nspl
CREATE SUBSCRIPTION acme_notifications TO notifications WHERE tenant = 'acme';
CREATE SUBSCRIPTION sampled_telemetry TO telemetry DROPPING BATCH SAMPLE RATE 0.1 WHERE input.tenant = 'acme';
DELETE SUBSCRIPTION acme_notifications;
DESCRIBE RELAY notifications WHERE (tenant = 'acme');
```

Current session behavior:

- subscription creation validates the statement against the relay as the cluster schedule declares
  it, and attaches only while this node executes the relay with that declaration
- subscription names are unique within one connected session and may refer to relays in different domains
- `DELETE SUBSCRIPTION` resolves only the session-local subscription name, independent of the currently active domain
- subscribing to a relay collects records from all active branch groups for that relay
- a branched subscription reports its concrete branch key with each record; sensitive branch-key
  fields are masked using the same rules as sensitive relay fields
- subscriptions are read-only views; only an optional `WHERE` predicate is supported, and a
  selected record is delivered without construction or transformation
- the predicate is an ordinary `BOOL` expression, including membership, range, and null-safe
  equality tests such as `input.status IN ('open', 'held')`; it selects a record only where it is
  true, so a null predicate, such as `IN` over a null field, does not select it
- bare fields, `message.<field>`, and `input.<field>` all read the subscribed relay record; the
  compiler rejects `output`, `branch`, and `relay_state` scopes when the subscription is created
- subscription syntax does not accept `INHERIT`, `SET`, `VALUES`, `INVOKE`, or other side effects
- each admitted subscription batch receives one snapshot from its relay's bound domain clock;
  every predicate expression and volatile UDF call for that batch sees the same instant
- optional `BATCH SAMPLE RATE <rate>` samples arrivals after `WHERE` has been evaluated
- `BLOCKING` delivery waits for room in the session's subscription queue, which holds back the
  relay it reads, while `DROPPING` discards rows when that queue is full and reports how many it
  discarded before its next rows
- subscription events are delivered asynchronously to the connected client session
- the relay owner is the sole subscription fan-out source, so each admitted batch is delivered at
  most once to a subscription even when producers and consumers run on several cluster nodes
- when the subscription's cluster node also hosts a runtime consumer, subscription delivery
  piggybacks on the same owner-to-node Arrow IPC batch instead of adding another serialized copy
- runtime and server errors are also delivered asynchronously
- cluster membership updates are also delivered asynchronously

Sessions are runtime-facing protocol interactions, not part of the persisted namespace model.

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
- **Ended by the server.** When its relay is redefined, so that a field, a type, nullability,
  sensitivity, or the branching changes, the subscription receives `SubscriptionEnded` with reason
  `RelayChanged`. When its relay or the relay's domain is removed, the reason is `RelayRemoved`.
  Either is the last frame about that generation, and no row of a redefined relay reaches a
  subscription announced under its earlier definition. Stopping and starting a domain, and
  rebuilds that keep the relay's definition, keep its subscriptions delivering. An ended
  subscription can still be deleted by name, and its name can be used again at once.
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
subscription reports the rows it discarded, and rows a filter could not evaluate or the encoder
could not write are reported as skipped. Rows in transit can also be lost without a report while
relay ownership moves between nodes or a node-to-node delivery fails.

Typed Row subscription frames carry a `BYTES` field as raw octets in a `BytesCell`; clients read
the value as borrowed bytes, including empty and non-UTF-8 sequences. JSON subscription views
render that field as padded standard base64 text. Sensitive byte fields are redacted in both views.

Persistent administrative commands carry a stable execution reference. The cluster binds that
reference to the authenticated owner, selected domain, and semantic command, continues admitted
work after the session disconnects, and retains one terminal result for the command retry
validity, 15 minutes by default. The reference is a UUIDv7 whose creation time bounds how long it
may be retried. The CLI, web console, and Rust client reuse the reference through redirects and
reconnects. Reuse for changed content fails. While a long command waits, transport keepalives, server events, and
subscription delivery continue independently. See [Command Completion](command-completion.md).

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
range from the display value. A local upload path suggestion asks the native CLI to search its own
filesystem and carries the path fragment's source range. The server sorts and deduplicates the
candidate set before returning a bounded page. A continuation binds the input, cursor, domain,
configuration revision, and candidate set; if any of these changes, the server returns
`StaleContext` instead of silently paging through a different set.

`Ready` with no suggestions means there are no matches. `MissingContext` means a domain required
for a semantic reference is absent or unavailable. `StaleContext` means a transaction binding or
page basis no longer matches. `LookupFailed` means the semantic configuration read failed. These
statuses are part of the suggestion reply, so a client can distinguish them without interpreting
an empty candidate list.

Suggestions are read-only session requests. They can complete while a command is still pending;
they neither enter the command admission gate nor change the transaction queue.

## Transaction Binding

An NSPL transaction is replicated control-plane state, but its binding to a live session is
leader-local soft state. A session may bind one transaction, and a transaction may be bound to at
most one session. The session protocol can attach by transaction id; the authenticated user must
match the transaction owner. A later attach takes over the binding and the displaced session gets
an explicit takeover error on its next transaction operation.

A transaction also carries the domain it is bound to. Attaching adopts that domain as the session's
selected domain, so a session can never queue a statement for a different domain than the
transaction it holds. `USE` remains unavailable while a transaction is active.

The CLI, web console, and Rust client retain the transaction id, each append's execution reference
and expected position, and the commit execution reference. They automatically attach after a
redirect or transport reconnect before resuming a command. A leader that has no binding for the
session's transaction answers with a distinct detached result rather than an ordinary error, and
the client attaches again and replays the command instead of surfacing it. An unclean transport loss, node loss,
or leadership change therefore leaves an open transaction intact until attach or idle expiry. A
client matches the attached progress to the outstanding append or commit and does not satisfy that
request from an unrelated transaction count. During election convergence, a client also retries a
bounded interval when a peer cannot yet advertise the new leader. A clean end of the session
preserves the existing interactive behavior by reverting a bound open transaction. Ending a
session never reverts admitted append work or a transaction whose replicated state is already
`COMMITTING`; the leader finishes it without a client.

Finished transaction outcomes remain available during tombstone retention. Attach during that
window reports `COMMITTED`, `FAILED`, `REVERTED`, or `EXPIRED` and includes structured commit
status plus the retained per-statement results. After retention, attach reports an unknown
transaction id. See
[Replicated NSPL Transactions](control-plane.md#replicated-nspl-transactions).
