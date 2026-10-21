# Ingestors

Ingestors are the entry points into the runtime graph.

A typical ingestor:

```nspl
CREATE BRANCH by_user
  SCHEMA user_branch TTL 5m;

CREATE IF NOT EXISTS KAFKA TOPIC INGESTOR kafka_notifications
  FROM kafka_main
  TOPIC 'notifications'
  OFFSET BY CONSUMER GROUP 'nervix_consumer'
  INSTANCES 1
  MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
  ON QUIESCE SUSPEND DECODE USING notification_codec
  TIMESTAMP NOW
  TO notifications
    INHERIT ALL
    BRANCHED BY by_user
    SET user_id = message.user_id
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

An ingestor statement names the connector and the entity it reads in its head, as one phrase
before `INGESTOR`: `KAFKA TOPIC` consumes a Kafka topic, `HTTP ENDPOINT` serves a Nervix HTTP
endpoint, and `CLIENT PRODUCER` admits batches that applications submit. The body after `FROM`
names no connector and holds only that pair's clauses; see
[Supported Ingestor Types](#supported-ingestor-types). Completion offers each head as one item,
such as `KAFKA TOPIC INGESTOR`. `ALTER` repeats the stored ingestor's connector and entity, so an
alteration never changes what an ingestor reads; to read another connector or entity, drop the
ingestor and create it again. `DROP INGESTOR`, `DESCRIBE INGESTOR`, `SHOW CREATE INGESTOR`,
`SHOW INGESTORS`, and relocation address an ingestor by its name alone.

```nspl,ignore
CREATE [IF NOT EXISTS] <CONNECTOR> <ENTITY> INGESTOR <name>
  FROM <body>
  [DECODE USING <codec>]
  [TIMESTAMP NOW | TIMESTAMP AT <field>]
  [FILTER WHERE <expression>]
  TO <relay> <route body>
  [TO <relay> <route body> ...]
  ON GENERAL ERROR IGNORE | LOG;
```

`DECODE USING` is required under every head except `CLIENT PRODUCER`, which decodes nothing and
does not accept it. A client the body names must be a client of the head's connector, and an
endpoint must have the head's `TYPE`; completion after `FROM` offers only those, and any other
reference is refused when the statement is applied, with a message that names both kinds, such as
`requires a KAFKA client, but client 'cache' is a REDIS client`. `SHOW CREATE INGESTOR` renders the
complete statement under its head. `DESCRIBE INGESTOR` prints the pair and the body on its `from:`
line, such as `from: KAFKA TOPIC kafka_main TOPIC 'Orders' OFFSET BY CONSUMER GROUP 'Readers' MODE
NO ACK ON QUIESCE SUSPEND`, and `SHOW INGESTORS` prints the pair alone, such as `from=KAFKA TOPIC`.

Every ingestor defines:

- the destination relay or relays
- the codec used for decoding, or for a [client ingestor](#client-ingestors) the schema its
  batches carry
- a route-local outgoing branch declaration
- a flush policy for every destination relay
- a message error policy for every destination relay
- the timestamp source
- the connector and entity it reads, and the source body that follows `FROM`
- the delivery mode
- the source-specific quiesce mode and every variable it requires
- optional node-level `FILTER WHERE` and route-local `INHERIT` / `SET` / `WHERE`

`FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` is required after every `TO
<relay>`. The [NSPL Overview](nspl-overview.md) defines the system-owned 100 µs minimum batching
window for `FLUSH IMMEDIATE`. It has no size boundary. Every route also requires its own
`ON MESSAGE ERROR <policy>`. Each route buffers and handles message-specific construction failures
independently. `ON MESSAGE ERROR SEND TO` uses that same route's interval and maximum batch-size
boundaries for error-record delivery. `ON GENERAL ERROR` remains node-level because it handles
source or transport failures that are not tied to one message or output route.

`MAX BATCH SIZE` counts logical Arrow value, offset, and validity bytes. It excludes unused Arrow
buffer capacity and object overhead. In delivery modes, `MAX <n>` is only an in-flight window for
`ACK PARALLEL`; `MODE NO ACK` never accepts it.

At runtime, the ingestor:

- decodes inbound payloads into runtime records; a JAQ-backed codec may
  [unfold](schemas-and-codecs.md#unfolding-payloads) one payload into zero or more records
- collects decoded records into a bounded source ingest group that closes once it holds 1,024 or
  more messages, or earlier when the source goes quiet for 5 ms; the records of one payload always
  join one group together, so a payload that unfolds past the remaining room closes its group
  above 1,024. This bound is independent of every route `FLUSH` policy and caps the first Arrow
  batch built from external input at 1,023 messages plus one payload's records
- executes `FILTER WHERE` once against that whole group
- executes each route's ordered construction and `WHERE` program once against the surviving group
- resolves the concrete branch group from the referenced `CREATE BRANCH`
- accumulates decoded rows independently for every matching destination and branch group
- writes each route's buffered rows when its configured interval or size boundary fires, when the
  documented [`FLUSH IMMEDIATE` system timeout](nspl-overview.md) expires, or when the domain is
  force-flushed by a model alteration, a domain pause, or a planned ownership handoff

Branch execution receives these completed Arrow batches and does not buffer them behind another
flush policy.

Source acknowledgement is per payload. An acknowledged delivery mode acknowledges a payload once
every record decoded from it has been acknowledged on every route it reached, and a negative
acknowledgement of any of those records negatively acknowledges the payload.
`ACK PARALLEL MAX <n>` windows count payloads. A client ingestor's payload is one submitted batch.

Timestamp selection and admission use one domain execution snapshot when the source group is
delivered. `TIMESTAMP NOW` records that snapshot. `TIMESTAMP AT <field>` and connector-owned event
timestamps preserve the external instant exactly; `TIME RATE` never scales them. The internal low
and high watermarks initially equal the selected event time, so source event time remains distinct
from delivery and observation time throughout the graph. The 5 ms source-idle close and the
`FLUSH IMMEDIATE` 100 µs minimum are physical monotonic waits. `FLUSH EACH` and paced admission use
domain-logical time. See [Domains And Time](domains-and-time.md#ingestion-timestamps).
For a decoded group, timestamp selection and paced admission operate on Arrow columns. The group
keeps admitted rows together; each rejected or missing event timestamp follows the ingestor route's
`ON MESSAGE ERROR` policy with code `validation` and operation `admit`, preserving that row's ACK
and source metadata. A batch containing both kinds can therefore deliver its accepted rows while
its rejected rows reach the configured error route.

## Delivery Modes

Every source body declares its delivery mode with `MODE`, polled and listener sources included:

- `NO ACK` is one composed phrase. Nothing waits on an acknowledgement, and it takes no variables.
- `ACK SEQUENTIAL ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>` lets one
  payload at a time await its acknowledgement.
- `ACK PARALLEL MAX <n> BATCH TIMEOUT <duration> ACK TIMEOUT <duration> RETRY POLICY BACKOFF
  <duration> MAX <duration>` lets at most `n` payloads await their acknowledgement; `BATCH TIMEOUT`
  is the longest wait for a partial batch. A `CLIENT PRODUCER` ingestor writes `ACK PARALLEL MAX
  <n> ACK TIMEOUT ...` without `BATCH TIMEOUT`.

Each entity accepts only the modes it can honor, and completion offers only those:

| Entity | `MODE` forms |
| --- | --- |
| `KAFKA TOPIC`, `PULSAR TOPIC`, `MQTT TOPIC` | `NO ACK`, `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` |
| `RABBITMQ QUEUE`, `SQS QUEUE` | `ACK SEQUENTIAL ...` |
| `CLIENT PRODUCER` | `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` without `BATCH TIMEOUT` |
| `HTTP REQUEST`, `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, `WEBSOCKETS CONNECTION`, `REDIS PUBSUB`, `NATS SUBJECT`, `ZEROMQ PULL`, `SYSLOG LISTENER`, `PROMETHEUS QUERY` | `NO ACK` |

In an `ACK` mode an ingestor acknowledges a message on its transport only once every route and error
delivery the message unfolded into has completed, and it reports that progress only once the
transport confirmed the acknowledgement as far as the entity can: an `SQS QUEUE` waits for the
broker's answer to its deletion, a `KAFKA TOPIC` for the answer to its consumer-group commit, or
under `OFFSET BY DOMAIN` for its offsets' state replicas, a `PULSAR TOPIC` for the broker's answer
to its acknowledgement, a `RABBITMQ QUEUE` for its acknowledgement to be written to the connection,
and an `MQTT TOPIC` only for its client to queue the acknowledgement. An acknowledgement the
transport fails to confirm, or a rejection it fails to take back, is a source error that
`DESCRIBE INGESTOR` shows as the transient error until a later acknowledgement is confirmed, and the
messages are delivered again. A node that stops gracefully waits for its ingestors' outstanding
confirmations before it closes them; see [Shutdown And Recovery](shutdown.md#connector-contracts).

## Altering Ingestors

`ALTER` names the connector and entity of the stored ingestor in its head and applies one or more
comma-separated operations in written order:

```nspl,ignore
ALTER <CONNECTOR> <ENTITY> INGESTOR <ingestor>
    SET FROM <complete body of the head's connector and entity>
  | SET QUIESCE <quiesce body>
  | SET DECODE USING <codec>
  | SET TIMESTAMP NOW
  | SET TIMESTAMP AT <field>
  | DROP TIMESTAMP
  | SET FILTER WHERE <expression>
  | DROP FILTER WHERE
  | ADD ROUTE TO <relay> <full ingestor route body>
  | DROP ROUTE TO <relay>
  | REPLACE ROUTE TO <relay> <full ingestor route body>
  | SET GENERAL ERROR IGNORE
  | SET GENERAL ERROR LOG
  [, ...];
```

A head whose connector and entity differ from those the stored ingestor reads is refused, for
example with `the stored ingestor reads HTTP ENDPOINT, so ALTER KAFKA TOPIC INGESTOR cannot alter
it`. An alteration therefore never changes what an ingestor reads; drop the ingestor and create it
again under the other head instead.

`SET FROM` accepts the complete body that follows `FROM` in a `CREATE` statement with the same
head, and only that pair's body. Change a client or endpoint, a Kafka topic or offset mode,
`INSTANCES`, the delivery mode, or source-specific subscription settings by supplying that complete
body, including its required `ON QUIESCE` clause. `SET QUIESCE` changes only the quiesce mode and
its variables. It parses every quiesce body, and the stored source refuses one it does not honor,
for example with `HTTP ENDPOINT ingestors do not support ON QUIESCE SUSPEND`; there is no
`DROP QUIESCE` operation. `SET DECODE USING` exists under every head except `CLIENT PRODUCER`,
which decodes nothing. `ADD ROUTE` appends a route, while `REPLACE ROUTE` preserves the matched
route's position. Duplicate routes to the same relay remain legal, so `DROP ROUTE` and
`REPLACE ROUTE` reject an ambiguous target. An ingestor must retain at least one route.

Every ingestor alteration uses `ENTITY_PAUSE`. Nervix quiesces only the affected ingestor instances
on all live nodes and waits for their already accepted in-flight work to drain before commit. That
work includes route output still held behind a `FLUSH EACH` interval: the hold force-flushes those
branch buffers, so a slow or stopped domain clock cannot keep accepted rows out of the drain.
Schedule application starts the desired source configuration on its assigned nodes; unrelated graph
paths keep flowing. The quiesce mode in effect when the hold begins governs that hold; a mode changed
by the alteration applies to later pauses. A drain or cutover failure leaves the candidate unapplied
and releases the old source. The transient hold also expires automatically, so loss of the
coordinating leader cannot leave an ingestor quiesced indefinitely.

## Quiesce Modes

Every source body ends with a required `ON QUIESCE <body>`, which `DECODE USING` follows under every
transport head. The quiesce *level* says which runtime work pauses for an alteration; an
ingestor's quiesce *mode* says what its external source experiences while that ingestor is paused.
There is no default.

The shared mode vocabulary is:

- `SUSPEND` stops taking payloads. Data waits in the external source where that source retains it;
  the source's retention, queue, or session limits continue to run. ZeroMQ instead keeps its socket
  open and stops reading, leaving transport flow control and the peer's high-water marks to govern
  the backlog. Polling ingestors skip polls. `SUSPEND` has no variables.
- `BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST` keeps a push connection active and
  retains raw payloads plus their ingest context in a per-instance volatile buffer. `MAX SIZE` must
  be positive. A payload larger than the bound is dropped, and a full buffer applies the declared
  side of the overflow policy. Every discard is counted.
- `DROP` keeps the source connection and protocol maintenance active but discards and counts every
  payload received during the quiesce.
- `REJECT RETRY AFTER <duration>` is available only to `HTTP ENDPOINT` and `WEBSOCKETS ENDPOINT`.
  HTTP requests and new WebSocket upgrades receive 503; HTTP includes `Retry-After`, rounded up to
  whole seconds. Established WebSockets close with code 1013 (Try Again Later).
- Endpoint buffering, for `HTTP ENDPOINT` and `WEBSOCKETS ENDPOINT`, is `BUFFER MAX SIZE <bytes>`
  without `ON OVERFLOW`. Requests that fit receive 202 and are held. A full buffer, or a body that
  cannot fit, is rejected with HTTP 503 or a WebSocket 1013 close; an acknowledged body is never
  discarded by buffer policy.

A read already in progress when an ingestor quiesces ends without losing what its source took from
the transport: under `SUSPEND` those payloads still dispatch before the source suspends, and under
`BUFFER` and `DROP` the mode decides on them like any payload received during the quiesce.

A source accepts only the modes it can honor, and completion of its body offers only those.
`SET QUIESCE` parses every body and refuses one the stored source does not honor:

| Entity | Allowed `ON QUIESCE` bodies |
|---|---|
| `KAFKA TOPIC`, `PULSAR TOPIC`, `RABBITMQ QUEUE`, `SQS QUEUE` | `SUSPEND` |
| `MQTT TOPIC` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP`; `SUSPEND` requires `SESSION PERSISTENT QOS 1` |
| `NATS SUBJECT`, `REDIS PUBSUB`, `WEBSOCKETS CONNECTION` | `BUFFER ... ON OVERFLOW ...`, `DROP` |
| `ZEROMQ PULL`, `SYSLOG LISTENER` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP` |
| `HTTP REQUEST`, `PROMETHEUS QUERY` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...` |
| `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT` | `REJECT RETRY AFTER ...`, `BUFFER MAX SIZE ...` |
| `CLIENT PRODUCER` | `SUSPEND` |

The mode is consulted for resumable model-alteration `ENTITY_PAUSE` holds, `DOMAIN_PAUSE` batches,
and memory-pressure shedding. `STOP` and `DROP INGESTOR` terminate the source session. Unexpected
owner loss also terminates it immediately; volatile quiesce buffers and in-memory ACK state do not
survive, so recovery depends on the external source's retention and redelivery contract.

Node drain, graceful-shutdown drain, placement relocation, and explicit relocation with
`RELOCATE` use a planned ownership handoff.
That hold deliberately ignores `ON QUIESCE`: polling and new endpoint admission stop, an already
admitted payload continues through its routes, and connected-source policy does not buffer or drop
new work on behalf of the move. Nervix waits for the moved ingestor's admitted ACK roots to resolve
before it commits the new owner. Kafka offsets, broker acknowledgements, queue deletes, and the
corresponding downstream attached work therefore complete on the former owner before its source
session stops. The destination starts only after the schedule revision commits.

Only moved ingestors take this hold. Every other ingestor keeps its session open and continues
ingesting, including ingestors on the former and destination cluster nodes. A failed fence or drain
releases the former source and leaves the schedule unchanged.

Graceful shutdown then stops intake the same way on every ingestor of the terminating node,
including endpoint ingestors, which serve on every node, and ingestors whose scheduled owner did not
move. It also ignores `ON QUIESCE`. Already admitted payloads continue through their routes, route
buffers are force-flushed whatever their `FLUSH EACH` cadence, and Nervix waits for their ACK roots
to resolve before the source session stops, so offsets, acknowledgements, and deletes complete for
work that reached its sinks. A root still unresolved when the drain timeout passes is negatively
acknowledged, and the source's redelivery contract applies. A raw quiesce buffer is outside runtime
graph work, so a shutdown discards what it retained instead of replaying it. See
[Planned Ownership Handoffs And Failover](control-plane.md#planned-ownership-handoffs-and-failover)
and [Shutdown And Recovery](shutdown.md#connector-contracts) for the per-source acknowledgement and
redelivery contract at shutdown.

Ordinary model-alteration quiesce still does not wait for downstream ACK chains. Already admitted
route batches continue downstream, and a source item not yet acknowledged, committed, or deleted is
eligible for redelivery on resume. A connected mode's raw quiesce buffer is outside runtime graph
work. Ordinary relay backpressure is also independent of `ON QUIESCE`.

Buffered payloads drain in arrival order per instance before that instance admits new live intake.
There is no total order across instances. Decode, timestamp selection, filters, construction,
routing, and domain pacing use the configuration in effect at delivery, so a codec installed by an
alteration decodes payloads buffered during its hold. `TIMESTAMP NOW` therefore records delivery
time. Bounds are per instance, and an endpoint has an independent buffer on every serving node.

A payload stays in its buffer, counted in the buffered records and bytes and within `MAX SIZE`,
until its messages enter their ingest group, and overflow never discards it while it drains. Its
sender was already answered or its source has moved past it, so when its codec's `ON INGESTION`
transformation finds the node's extension workers full, the payload waits for them rather than
being refused, ahead of work that asks after it, and nothing behind it drains first. A new quiesce
or a shutdown ends that wait and leaves the payload at the front of the buffer. A payload its codec
refuses leaves the buffer as a decode rejection; see
[When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).

Memory pressure never adds buffered bytes. During memory-pressure quiesce, a `BUFFER` push source
drops and counts new payloads, polling sources skip polls, and endpoints reject. Payloads already
buffered by an overlapping alteration remain retained. `DROP` continues to discard and `SUSPEND`
continues to hold at the source.

`DESCRIBE INGESTOR` renders `quiesce:`, `quiesce state: none|entity hold|ownership handoff|domain
pause|memory pressure`, `status: quiesced` for every active hold, and the four quiesce counters.
It also renders `nervix_ingestor_transport_redeliveries_total`, the acknowledgements and rejections
the node discarded after a RabbitMQ, SQS or MQTT source reconnected, because the broker redelivers
what the lost connection did not acknowledge; see
[Connector Crates And The Connector Contract](connector-contract.md). It renders
`nervix_ingestor_decode_rejections_total` and `last decode rejection:` as well; see
[When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload). `SHOW CREATE` round-trips the
full clause.

## When The Codec Refuses A Payload

A payload its codec refuses, because it is not in the wire format the codec reads or does not fit
the codec's schema, would be refused again however often its source presented it. The ingestor
therefore consumes it, and a decode rejection takes its place: a message error that the ingestor's
node-wide `ON GENERAL ERROR` policy reports.

- `ON GENERAL ERROR LOG` reports the rejection to every attached session as a server error and logs
  its fields at `warn`, such as `ingestor 'orders_source' general error in domain 'shop': decode
  rejection 0199f1c2-… at 2026-10-17T12:00:00Z: codec 'order_codec' refused the payload from Kafka
  topic 'orders' partition 0 offset 7: codec 'order_codec' failed to parse field 'total': …`.
- `ON GENERAL ERROR IGNORE` reports nothing.

Neither policy makes the source present the payload again, and the other payloads of its batch
continue through the ingestor's routes. An acknowledged delivery mode settles the refused payload's
position on its transport together with the rest of the batch, so a Kafka consumer group commits
past it and a RabbitMQ queue no longer holds it. In `MODE NO ACK` the payload is dropped with its
rejection. A JAQ-backed codec that refuses one message an unfolding payload produced refuses the
whole payload, and so does an unfolding that panics.

A decode rejection carries a stable UUIDv7 reference, the code `validation`, the operation `decode`,
the codec, the fields the codec named at fault, the instant the source took the payload in, and the
codec's diagnostic as its message. When the source's metadata names where on its transport the
payload was read, the rejection names it too: a Kafka record's topic, partition and offset, or the
peer a syslog frame came from. A source whose metadata holds only headers names no position, because
a header value is what its sender wrote. The rejection never holds the payload, and the codec's
diagnostic names the codec and the field at fault, never a payload value.

The node counts the payloads its ingestor consumed this way in
`nervix_ingestor_decode_rejections_total`. `DESCRIBE INGESTOR` renders that count and
`last decode rejection:`, the latest rejection on the node that runs the ingestor, or `-` before
the first.

A payload the node's extension workers have no room to unfold has not been judged by its codec, so
it is not a decode rejection; see [When The Extension Workers Are Full](#when-the-extension-workers-are-full).
An endpoint ingestor in `MODE NO ACK` accepts a body its codec refuses and reports it the same way, including a body
its quiesce buffer retained.

## When The Extension Workers Are Full

A codec with an `ON INGESTION` transformation [unfolds](schemas-and-codecs.md#unfolding-payloads)
every payload on the node's extension workers. When every worker is busy and the class's wait queue
is full, what happens to a payload depends on whether its sender can present it again:

- An acknowledged delivery mode rejects the batch, and its source delivers it again on its
  `RETRY POLICY`.
- An [endpoint ingestor](#http-endpoint-and-websockets-endpoint) answers an HTTP body 503 without
  `Retry-After` and closes an established WebSocket with 1013, so the sender sends it again.
- A payload that `BUFFER` retained waits for a place when its buffer drains; see
  [Quiesce Modes](#quiesce-modes).
- A paced source has already moved past its poll, so the poll waits for a place. Waiting delays the
  next poll, and the domain cadence coalesces the occurrences it missed into one poll at the newest
  due instant.
- A source read in `MODE NO ACK` cannot present a payload again either. The payload waits for a
  place when holding the source's loop keeps its transport connected with bounded memory. Otherwise
  it is refused: the ingestor reports the refusal as an error and counts the payload in
  `nervix_ingestor_unfolding_refused_total`.

| Entity | A payload the extension workers cannot take now | Why |
| --- | --- | --- |
| `HTTP REQUEST`, `PROMETHEUS QUERY` | Waits | Holding the loop only delays the next poll. |
| `NATS SUBJECT` | Waits | The client keeps its connection and reads the subscription while the loop waits, holding up to 65,536 messages before it drops the newest as a slow consumer. |
| `ZEROMQ PULL` | Waits | Unread messages stay in the socket's kernel buffers, and then the pushing peers wait. Nothing is dropped. |
| `SYSLOG LISTENER` | Waits | The listener stays bound. The kernel drops UDP datagrams beyond the socket's receive buffer, and TCP and TLS senders wait once the listener's 64-frame queue is full. |
| `MQTT TOPIC` | Refused and counted | The client sends its keep-alive only while the loop reads, so the broker closes the connection once one and a half keep-alive intervals pass without a packet from it, and what is published until the source reconnects is lost. |
| `REDIS PUBSUB` | Refused and counted | The client reads every published message into an unbounded queue of its own, so a held loop would grow the node's memory without limit. |
| `WEBSOCKETS CONNECTION` | Refused and counted | The client answers the server's pings only while the loop reads, so a server that pings would close the connection and lose what it sent until the source reconnects. |
| `KAFKA TOPIC` in `MODE NO ACK` | Refused and counted | A consumer-group member that stops polling for `max.poll.interval.ms` leaves its group, and an instance reading domain offsets would keep its partitions assigned through an ownership handoff. |
| `PULSAR TOPIC` in `MODE NO ACK` | Refused and counted | A consumer whose receive queue fills cannot complete its close, so a later suspension or stop would wait on it. The refused message stays unacknowledged until the consumer reconnects, when the broker delivers it again. |

A wait never delays a stop or a new quiesce. Stopping the ingestor ends the wait and drops the
payload with the rest of the ingestor's in-flight input. A new quiesce decides on the payload again,
as on input that arrives while quiesced: `BUFFER` retains it, `DROP` drops it and counts it in
`nervix_ingestor_quiesce_dropped_total`, and a suspension, under which a payload the source already
handed over still dispatches, lets it wait on until the workers have room. While a payload waits,
the ones behind it stay with the transport, in order.

## Branch Semantics

Ingestors are where external mixed flows enter branch-isolated processing. Every route independently
constructs a concrete branch or becomes unbranched. The branch declaration owns the key schema,
TTL, and optional LRU eviction policy:

- `CREATE BRANCH <branch> SCHEMA <schema> TTL <duration>` declares the branch key schema and lifetime
- `BRANCHED BY <branch> SET field = expression, ...` constructs the route's key after output
  finalization and route filtering
- `MAX INSTANCES <n> EVICT LRU` may be added to cap active concrete branch instances and evict the least recently used branch when capacity is reached
- `UNBRANCHED` produces an absent branch key; it is not encoded as an empty string or synthetic
  root identifier
- the named branch defines both the branch identity and its key shape; downstream relays and branch-preserving processors must reference that same branch name
- decoded rows are appended to matching destination relays inside that group's branch
- downstream normal processors keep the same group until a `REINGESTOR` or `EMITTER` boundary

Per-group behavior such as downstream deduplication, reordering, and window aggregation stays scoped to that branch.

Batching follows that same rule:

- an ingestor buffers independently per concrete branch group
- `UNBRANCHED` produces one root branch and one batcher

Client-backed ingestors can use resource-mounted client config values for TLS material and other file-based settings. See [Resources](resources.md#client-config-mounts).

## Value Construction and Filters

Ingestors may declare an optional arrival filter and per-route construction clauses:

```nspl
CREATE BRANCH by_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE IF NOT EXISTS HTTP ENDPOINT INGESTOR notifications_in
  FROM ingress MODE NO ACK
  ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
  FILTER WHERE input.active
  TO notifications
    INHERIT ALL EXCEPT raw
    SET amount = message.amount + 1,
        normalized = lower(input.raw)
    WHERE output.tenant = 'acme'
    BRANCHED BY by_tenant SET tenant = message.tenant
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

`FILTER WHERE` runs after codec decoding and before route processing. Each `TO` route starts with an
empty output, applies `INHERIT` and ordered `SET`, finalizes the output, evaluates its `WHERE`, and
then constructs its own branch key.

Supported blocks:

- `INHERIT ALL`, `INHERIT ALL EXCEPT ...`, or explicit `INHERIT field, ...` copies compatible
  same-named decoded fields
- `SET <field> = <expr>, ...` initializes route output fields in order
- `WHERE <expr>`: drops rows whose predicate is false or null

General notes:

- `SET` is a single clause with comma-separated assignments
- assignments execute left to right; repeated destination fields are allowed and later expressions read the latest preceding value
- later assignments may read earlier output values; repeated targets are valid
- `message.field` reads the [working message](working-message.md);
  `input.field` always reads the original decoded row and `output.field` requires prior initialization
- route `WHERE` sees finalized output through bare fields, `message`, or `output`, without fallback
- relay-qualified fields and `UNSET` are invalid
- all node expressions use explicit typing; there is no implicit cast insertion
- nested predicates and nested/chained builtin calls are supported
- leader-side validation requires every required output and branch field to be initialized
- public Models store structured expressions; the runtime never reparses NSPL
- source-specific transport metadata may be exposed through `metadata.<field>` when the ingestor
  supports it; `KAFKA TOPIC` provides `metadata.topic`, `metadata.partition`, and
  `metadata.offset`, while `SYSLOG LISTENER` provides `metadata.peer_addr`
- supported sources expose transport headers through `read_header(name)` and `read_headers(name)`;
  header names may be any `STRING` expression, and these functions may be used in both top-level
  `FILTER WHERE` and per-route expressions

Useful built-ins include string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_like`, `now`, and `uuid_v7`.

See [Expression Functions](filter-map-functions.md) for the full function reference.

Common expression patterns include:

- nested conditions such as `(active AND amount > 5) OR NOT flagged`
- chained calls such as `lower(trim(raw))`
- arithmetic expressions such as `(amount + fee) / divisor`
- explicit casts such as `raw AS INT64`, and tolerant conversions such as
  `TRY_CAST(raw AS INT64)`, which yield a typed null instead of failing the message
- typed reads of JSON text held in a `STRING` field, such as
  `JSON_VALUE(raw, '$.order.id' AS INT64)`; see
  [JSON Documents](filter-map-functions.md#json-documents)

The expression type surface matches the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

`DATETIME` is the internal logical timestamp type. JSON or AVRO string wire values require `ENCODE <field> AS RFC3339` on the codec before they can decode into the named `DATETIME` field.

### Header Context

Some ingestors receive additional source data alongside the decoded payload body. They expose it through two functions:

- `read_header(name)` returns the first value as an optional `STRING`, or `NULL` when the header is absent
- `read_headers(name)` returns every value in transport order as required `LIST<STRING>`, or an
  empty list when the header is absent

The `name` argument can be any expression that returns `STRING`. Both functions are available in top-level `FILTER WHERE` and in per-route `SET` / `WHERE` expressions for these sources:

- `HTTP ENDPOINT` and `HTTP REQUEST` expose HTTP headers with UTF-8 values
- `WEBSOCKETS ENDPOINT` exposes UTF-8 headers from the opening HTTP upgrade request
- `KAFKA TOPIC` exposes Kafka record headers
- `NATS SUBJECT` exposes message headers
- `PULSAR TOPIC` exposes message properties
- `RABBITMQ QUEUE` exposes AMQP message headers, converting typed AMQP values to strings
- `SQS QUEUE` exposes message attributes, converting string, number, and binary values to strings

The same captured source envelope remains available while a supported ingestor constructs an
`ON MESSAGE ERROR SEND TO` record, so its error-route `SET` may also call `read_header` and
`read_headers`.

Header values are non-sensitive strings and do not propagate through relays unless explicitly
assigned to schema-backed fields:

```nspl,ignore
SET route = read_header(lower(input.route_header))
WHERE read_header("tenant") = output.tenant
```

`MQTT TOPIC`, `REDIS PUBSUB`, `PROMETHEUS QUERY`, `SYSLOG LISTENER`, `ZEROMQ PULL`,
`WEBSOCKETS CONNECTION`, and `CLIENT PRODUCER` ingestors do not support header reads. Leader-side
validation rejects these functions for a source without header support, with a message that names
the source's phrase, such as `MQTT TOPIC ingestors do not support read_header or read_headers`.

## TLS Client Configuration

For outbound ingestor clients, TLS is configured on the `CLIENT`, not on the `INGESTOR`.

General pattern:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <kind>
  MOUNT <tls_resource> VERSION <n>|LATEST
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific schemes and keys:

- `KAFKA`: pass-through to librdkafka. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and if needed `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `HTTP`: use an `https://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `PROMETHEUS`: use an `https://...` `addr`. Nervix honors `tls_ca_file`, `tls_cert_file` and `tls_key_file`, and the client requires `timeout_ms`; see [Prometheus](#prometheus) for every key it declares.
- `WEBSOCKETS`: use a `wss://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`. Nervix requires `tls_ca_file` for server trust and also supports `tls_cert_file` plus `tls_key_file` for mTLS.
- `NATS`: use `tls://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`. Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`. Pulsar client certificate authentication is not currently exposed.
- `RABBITMQ`: use `amqps://...` in `addr`. Nervix honors `tls_ca_file`.
- `REDIS`: use `rediss://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `SQS`: use an `https://...` `endpoint`. Nervix honors `tls_ca_file`. This is primarily useful for SQS-compatible local/test endpoints.
- `SYSLOG`: select `'protocol' = 'tls'`. An ingestor requires `tls_cert_file` and
  `tls_key_file`; optional `tls_ca_file` enables required client-certificate verification.

`HTTP REQUEST` and `PROMETHEUS QUERY` ingestors resolve endpoint names through the node's
configured DNS resolver.
A client `timeout_ms` covers name resolution, connection establishment, TLS, and the response.
The endpoint name remains the HTTP authority and HTTPS certificate name after resolution.
RabbitMQ resolves the host of its `addr` the same way and verifies an `amqps` broker certificate
against that host; see [RabbitMQ](#rabbitmq).
Redis Pub/Sub also resolves its `addr` hostname through the node resolver for each dedicated
subscription connection; see [Redis Pub/Sub](#redis-pubsub).
MQTT resolves the host of its `addr` through the node resolver for every connection and verifies
an `mqtts` broker certificate against that host; see [MQTT](#mqtt).

Example Kafka TLS client:

```nspl
CREATE IF NOT EXISTS CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls VERSION 1
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

Example HTTP TLS client:

```nspl
CREATE IF NOT EXISTS CLIENT http_tls
  TYPE HTTP
  MOUNT dev_tls VERSION 1
  CONFIG {
    'endpoint' = 'https://127.0.0.1:18443/http/notifications',
    'method' = 'GET',
    'timeout_ms' = 5000,
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

## Client Deadlines

A client's `timeout_ms` and `connect_timeout_ms` mean the same thing for every connector that reads
them. `timeout_ms` bounds the whole of one request or operation: name resolution, connection setup,
TLS and the answer. `connect_timeout_ms` bounds establishing a connection: name resolution, the
address attempts, TLS and the protocol's own handshake. Each value is a positive whole number of
milliseconds.

`HTTP` and `PROMETHEUS` clients read `timeout_ms`. `RABBITMQ`, `REDIS` and `WEBSOCKETS` clients read
`connect_timeout_ms`, and allow 30 seconds when it is left out. A `RABBITMQ` client's deadline
covers resolving the broker, the address attempts, TLS, the AMQP handshake, opening the ingestor's
channel, setting its prefetch window and starting its consumer. A request or connection that runs
out of its deadline becomes the ingestor's transient status, and the ingestor retries on its
policy; a connection that runs out of `connect_timeout_ms` is reported naming the key and its
bound. [Connector Capabilities](connector-capabilities.md) lists each entity's network waits and
what bounds each one, including the waits nothing bounds yet.

## Client Ingestors

The [Client Ingestors And Emitters](./client-io-architecture.md) architecture chapter follows a
complete native graph through admission, application ACK, credit, forwarding and recovery.

A client ingestor, declared under the `CLIENT PRODUCER` head, admits typed batches that applications
submit through a Nervix session instead of reading an external system. There is no
`CREATE CLIENT`, codec, `DECODE USING`, transport header, or `NO ACK` mode: a submitted batch
already carries the ingestor's input schema, and every batch is acknowledged.

```nspl
CREATE SCHEMA order_in (
  region STRING, order_id STRING, amount I64, card STRING SENSITIVE
);
CREATE SCHEMA region_key (region STRING);
CREATE BRANCH by_region SCHEMA region_key TTL 5m;

CREATE CLIENT PRODUCER INGESTOR orders_in
  FROM SCHEMA order_in
    MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s
    ON QUIESCE SUSPEND
  TIMESTAMP NOW
  TO orders_by_region
    INHERIT region, order_id, card
    SET amount_cents = input.amount * 100
    BRANCHED BY by_region SET region = message.region
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

<!-- connector-matrix:ingest-client-producer:start -->
```plexus,ignore
CREATE CLIENT PRODUCER INGESTOR <name>
  FROM SCHEMA <schema>
  MODE <mode>
  ON QUIESCE <quiesce>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | SCHEMA &lt;schema&gt; |
| Locator and validation rule | Not offered: The body references the native schema; there is no external topic, queue or table to locate. |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | Not offered: Application batches carry their declared schema; there is no separate transport metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | batches one producer submits ahead of their outcomes: at most 1024 batches, within 32 MiB, per producer grant; when full, holds back what fills it |
| Network waits | the acknowledgement of an admitted batch: `ACK TIMEOUT` |
| Driver limits | None declared. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [CLIENT PRODUCER capability and mirror reference](connector-capabilities.md#client-producer-ingest).
<!-- connector-matrix:ingest-client-producer:end -->

Every clause is required and there are no defaults:

- `SCHEMA` names the input schema. A batch must carry exactly its fields, in order, with their
  types and optionality, and a producer must declare exactly those fields, including which are
  `SENSITIVE`, before it may submit anything.
- `ACK SEQUENTIAL` admits one batch at a time; `ACK PARALLEL MAX <n>` lets at most `n` admitted
  batches await their acknowledgement. The window belongs to the ingestor's one execution and every
  producer shares it, so opening more producers never widens it. Producers with queued batches take
  the window in turn.
- `ACK TIMEOUT` bounds how long an admitted batch may make no acknowledgement progress. When it
  passes, the batch is reported as failed with `ack_timeout`; its admitted work is not cancelled
  and may still complete. Downstream work that reports it is alive keeps the batch from timing
  out.
- `RETRY POLICY BACKOFF <duration> MAX <duration>` is the physical backoff a producer applies
  before sending again a batch that was refused only temporarily. It starts at `BACKOFF` and
  doubles up to `MAX`, which must not be shorter than `BACKOFF`. Producers are told it when they
  open.
- `ON QUIESCE SUSPEND` is the only quiesce mode. Admission stops for the hold: producers are told
  that admission is suspended, queued batches that were not admitted yet are refused as
  `suspended`, and the producer keeps them to send again once admission reopens.

Timestamp selection, `FILTER WHERE`, route construction, `WHERE`, branch construction, route
`FLUSH`, `ON MESSAGE ERROR`, and `ON GENERAL ERROR` behave as for every ingestor; `message` and
`input` read the submitted row. Client batches carry no transport headers or metadata, so
`read_header`, `read_headers`, and `metadata.*` are unavailable. The ingestor executes on its one
scheduled cluster node. A producer is opened on a session that any live node may serve, and that
node forwards the producer's batches to the ingestor's node; a console session, like every console
session, is served only by the leader.

`ALTER CLIENT PRODUCER INGESTOR ... SET FROM SCHEMA ...` replaces the complete source body, and
`SET QUIESCE SUSPEND` restates the only quiesce mode. `SHOW CREATE INGESTOR` renders the
`CLIENT PRODUCER` head and the whole `FROM SCHEMA` body.

### Submitted Batches

A batch is one canonical Arrow IPC stream: the schema message, exactly one record batch, and the
end-of-stream marker. It is uncompressed and has no dictionary or extension encodings, and its
Arrow schema is exactly the input schema in Nervix's Arrow representation of each type, with no
field metadata. A batch carries at most 65,536 rows and at most the bytes one submission may carry,
which is the smaller of the producer's granted bytes and what one session frame holds. The node
that executes the ingestor validates the whole batch, its structure, bounds, and every column,
before any row is admitted; a batch that fails is refused whole and has no effect on the graph.
Nothing is coerced, cast, or partially admitted.

### Outcomes

One batch is one source acknowledgement unit. It has exactly one terminal outcome:

| Outcome | Meaning | Cause |
| --- | --- | --- |
| not admitted | No row entered the graph. | `invalid batch: <defect>`, `suspended`, `busy`, `draining`, `producer ended`, `credit exceeded` |
| completed | Every route and acknowledging sink its rows reached confirmed them under the graph's rules. | — |
| processing failed | The batch was admitted and its acknowledgement failed. Some of its effects may have happened. | `ack_timeout`, `rejected` |
| unknown outcome | The batch may have been admitted and processed, but no terminal result can be established. | `interrupted`, `owner_lost`; a client whose session ended adds `session_lost` |

Transport receipt is never an outcome. A batch completes under the normal acknowledgement rules: a
row a filter drops is resolved; a detached boundary resolves where it detaches; and an
acknowledging sink resolves its rows at its own success boundary, such as the complete response
headers of an HTTP emitter. The defects of an invalid batch are `malformed`, `unexpected message`,
`compressed`, `schema mismatch`, `not one batch`, `too many rows`, `too large`, and `invalid data`.

A row that fails on a route of an admitted batch is handled by that route's `ON MESSAGE ERROR`
policy exactly as for any other ingestor: `IGNORE` acknowledges it, `LOG` negatively acknowledges
it, so its batch fails processing as `rejected`, and `SEND TO` acknowledges it once its error record
is published to the error relay. The other rows of the batch are processed either way.

Only `suspended` and `busy` are temporary: sending the same batch again on the same producer may
succeed, and the Rust client does so on the declared backoff. Every other outcome is final for that
attempt. Nervix never replays a batch that failed or whose outcome is unknown; replaying it is the
application's decision, and it may duplicate the batch's effects. The application therefore keeps
its replayable source data until a batch completes.

### Producers

A producer is opened on a session with an explicit domain and ingestor, the fields it expects, and
the credit it asks for: how many batches and how many bytes it may have outstanding. The open is
answered with the input schema, the domain's START generation, the identity of the endpoint
contract, the attachment, the policy above, the granted credit, and whether admission is open now.
A later `USE` does not move a producer. An open is refused, with nothing left attached, when the
domain does not exist or is stopped, the ingestor does not exist, reads a transport, or is not
running on its scheduled node, the expected fields differ, the credit is larger than one producer
may ask for, the session or node has no room left, or the session holds a transaction.

| Limit | Value |
| --- | --- |
| Producers per session | 32 |
| Outstanding bytes per session | 32 MiB |
| Outstanding bytes per node, including batches retained for forwarded producers | 128 MiB |
| Batches one producer may ask to have outstanding | 1,024 |
| Rows per batch | 65,536 |

A batch holds its share of the credit from the moment it is sent until its outcome is observed. A
batch sent beyond the credit is refused as `credit exceeded` without reaching the graph, and the
producer is ended as a protocol violation; the batches it already submitted still receive their
outcomes.

A producer stays attached while the ingestor's endpoint contract holds. The contract is the
ingestor as producers see it — its input schema, mode, timestamp, filter, routes, error policies,
and the branch declarations its routes construct — without the routes' `FLUSH` cadence. The server
ends a producer, as the last event about it, with one of these reasons:

| Reason | When |
| --- | --- |
| `endpoint changed` | An alteration changed the endpoint contract, or made the ingestor read a transport. |
| `endpoint removed` | The ingestor or its domain was dropped. |
| `domain stopped` | The domain stopped, or a new `START` replaced the generation the producer attached under. |
| `relocated` | A planned ownership handoff moved the ingestor to another node. |
| `shutting down` | The serving node, or the node that executes the ingestor, is shutting down. |
| `owner lost` | The node that executes the ingestor, or the connection to it, was lost. |
| `protocol violated` | The producer sent a batch beyond its credit. |

An ended producer's queued batches are refused as `producer ended`, and its admitted batches whose
acknowledgement is unresolved have an unknown outcome. That holds when the node that executes the
ingestor dies or stops answering, too: the node that forwards a producer's batches to it clears each
batch before it may be admitted, so after the loss it refuses every batch it never cleared as
`producer ended`, and only the cleared ones have an unknown outcome with cause `owner_lost`. A new
producer can be opened as soon as the ingestor runs again. An alteration that keeps the contract, such as one that changes only a route's
`FLUSH`, suspends admission for its hold and reopens it afterwards with every producer attached. A
planned ownership handoff stops intake for good on the former owner: batches that arrive are
refused as `draining`, admitted ones complete there, and its producers end as `relocated` once the
new owner is committed.

### Observing Client Ingestors

`SHOW INGESTORS` lists every ingestor of the domain with the connector and entity it reads, its
codec or schema, the node that executes it, and its state. A client ingestor adds whether it admits
batches, its attached producers, the batches and bytes they have outstanding, and the batches
holding a slot of its window:

```text
orders_in from=CLIENT PRODUCER schema=order_in owner=node-2 status=running admission=open producers=2 outstanding_batches=3 outstanding_bytes=2412 admitted_batches=2
```

`DESCRIBE INGESTOR` renders `from:` with the head's phrase and the complete body, such as
`from: CLIENT PRODUCER SCHEMA order_in MODE ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF
100ms MAX 1s ON QUIESCE SUSPEND`, and `schema:`, `mode:`, `owner:`, `admission:`, `producers:`,
`forwarded producers:`, `outstanding batches:`, `outstanding bytes:`, and `admitted batches:`. The
node that executes the ingestor also exports the same counts and every answered batch as metrics;
see [Metrics And Observability](metrics-and-observability.md#client-ingestors).

## External Names

An ingestor names the external entity it reads with a string literal: a topic, consumer group,
subscription, queue, channel, subject, queue group, or MQTT topic filter. Nervix keeps the name
exactly as written, letter case included, and sends it to the external system as written. Each
entity validates its names when `CREATE` or `ALTER` applies, by the external system's own rule
where it has one:

| Entity | Name | Rule |
| --- | --- | --- |
| `KAFKA TOPIC` | `TOPIC` | 1 to 249 ASCII letters, digits, `.`, `_`, and `-`, and neither `.` nor `..`: the broker's own rule |
| `KAFKA TOPIC` | `CONSUMER GROUP` | 1 to 255 bytes without control characters |
| `PULSAR TOPIC` | `TOPIC` | A local name without `/`, which the client places in its `namespace` (`public/default` unless the client configures one), or a fully qualified `persistent://tenant/namespace/topic` or `non-persistent://tenant/namespace/topic`. At most 1,024 bytes, without whitespace, control characters, or an empty level |
| `PULSAR TOPIC` | `SUBSCRIPTION` | 1 to 255 bytes without control characters |
| `RABBITMQ QUEUE` | `QUEUE` | 1 to 255 bytes: the AMQP 0-9-1 limit |
| `SQS QUEUE` | `QUEUE` | 1 to 80 ASCII letters, digits, `-`, and `_`, and a FIFO queue's name ends in `.fifo`: SQS's own rule |
| `REDIS PUBSUB` | `CHANNEL` | 1 to 1,024 bytes of any text; a `*` is the character itself, and pattern subscriptions are not offered |
| `NATS SUBJECT` | `SUBJECT` | At most 1,024 bytes of `.`-separated tokens, none empty, without whitespace or control characters; `*` stands for one token and `>` for the rest, each only as a whole token, and `>` only last |
| `NATS SUBJECT` | `QUEUE GROUP` | 1 to 255 bytes without whitespace or control characters |
| `MQTT TOPIC` | `TOPIC` | A topic filter of 1 to 65,535 bytes without NUL; `+` stands for one level and `#` for the rest, each only as a whole level, and `#` only last. Levels are joined by `/` and may be empty |

A reference to a Nervix model, such as a client, endpoint, schema, codec, or relay, stays a bare
identifier.

## Supported Ingestor Types

Each subsection shows a head and the body that follows its `FROM`; `SET FROM` takes the same body
under the same head. `DECODE USING <codec>`, the timestamp, the filter, the routes, and
`ON GENERAL ERROR` follow the body as in every ingestor statement.

| Head | Reads | Section |
| --- | --- | --- |
| `HTTP REQUEST` | Responses to the requests a `TYPE HTTP` client sends on a cadence | [`HTTP REQUEST`](#http-request) |
| `HTTP ENDPOINT` | Requests to a `TYPE HTTP` endpoint | [Endpoints](#http-endpoint-and-websockets-endpoint) |
| `WEBSOCKETS ENDPOINT` | Frames on connections to a `TYPE WEBSOCKETS` endpoint | [Endpoints](#http-endpoint-and-websockets-endpoint) |
| `WEBSOCKETS CONNECTION` | Frames on the outbound connection of a `TYPE WEBSOCKETS` client | [`WEBSOCKETS CONNECTION`](#websockets-connection) |
| `KAFKA TOPIC` | A topic, through a `TYPE KAFKA` client | [Kafka](#kafka) |
| `PULSAR TOPIC` | A topic, through a `TYPE PULSAR` client | [Pulsar](#pulsar) |
| `RABBITMQ QUEUE` | A queue, through a `TYPE RABBITMQ` client | [RabbitMQ](#rabbitmq) |
| `REDIS PUBSUB` | A channel, through a `TYPE REDIS` client | [Redis Pub/Sub](#redis-pubsub) |
| `MQTT TOPIC` | A topic filter, through a `TYPE MQTT` client | [MQTT](#mqtt) |
| `NATS SUBJECT` | A subject, through a `TYPE NATS` client | [NATS](#nats) |
| `ZEROMQ PULL` | The PULL socket of a `TYPE ZEROMQ` client | [ZeroMQ](#zeromq) |
| `SYSLOG LISTENER` | The listener a `TYPE SYSLOG` client declares | [Syslog](#syslog) |
| `SQS QUEUE` | A queue, through a `TYPE SQS` client | [SQS](#sqs) |
| `PROMETHEUS QUERY` | A PromQL query, through a `TYPE PROMETHEUS` client | [Prometheus](#prometheus) |
| `CLIENT PRODUCER` | Batches applications submit through their sessions | [Client Ingestors](#client-ingestors) |

### `HTTP REQUEST`

<!-- connector-matrix:ingest-http-request:start -->
```plexus,ignore
CREATE HTTP REQUEST INGESTOR <name>
  FROM <client>
  (EVERY 1s)
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | HTTP client |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | `EVERY 1s` (duration); required |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the response body: unbounded |
| Network waits | a request: name resolution, connection, TLS and the response: `timeout_ms`; unbounded when omitted |
| Driver limits | Response heads above 100 headers or 417,792 bytes are refused.<br>A request follows at most 10 redirects. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [HTTP REQUEST capability and mirror reference](connector-capabilities.md#http-request-ingest).
<!-- connector-matrix:ingest-http-request:end -->

- polls a configured HTTP endpoint periodically
- issues the first request immediately, then follows an anchored domain-clock cadence; slow
  responses coalesce missed occurrences into one request at the newest due instant
- `204 No Content` is treated as no message
- `SUSPEND` skips polls and therefore creates a sampling gap
- `BUFFER` keeps polling on the declared cadence and drains retained poll payloads in order
- a poll whose `ON INGESTION` unfolding finds the node's extension workers full waits for them; see
  [When The Extension Workers Are Full](#when-the-extension-workers-are-full)

### Kafka

<!-- connector-matrix:ingest-kafka-topic:start -->
```plexus,ignore
CREATE KAFKA TOPIC INGESTOR <name>
  FROM <client> TOPIC 'orders'
  (OFFSET BY DOMAIN | OFFSET BY CONSUMER GROUP 'readers')
  [INSTANCES 2]
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | KAFKA client |
| Locator and validation rule | TOPIC 'orders' (kafka_topic) |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `OFFSET BY DOMAIN` (keyword choice); `OFFSET BY CONSUMER GROUP 'readers'` (string literal checked by kafka_consumer_group); required<br>`INSTANCES 2` (positive count); optional; default 1 |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.partition: I32 OPTIONAL<br>metadata.offset: I64 OPTIONAL |
| Every live node | no; scheduled owner |
| Position class | replayable |
| Acknowledgement confirmation | answered |
| Buffers | messages librdkafka prefetches: driver default, queued.min.messages 100000 per partition, queued.max.messages.kbytes 65536; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection setup: TCP, TLS and SASL: driver default, socket.connection.setup.timeout.ms, 30 s<br>a fetch: driver default, socket.timeout.ms, 60 s<br>a consumer-group commit under ACK: fixed 30s<br>offset lookups, watermarks and seeks: fixed 5s<br>subscribe, assign and unsubscribe: unbounded |
| Driver limits | Every librdkafka key passes through, so a client can change each driver default above.<br>receive.message.max.bytes 100000000 |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [KAFKA TOPIC capability and mirror reference](connector-capabilities.md#kafka-topic-ingest).
<!-- connector-matrix:ingest-kafka-topic:end -->

Kafka is the richest ingestion surface today.

Offset modes:

- `CONSUMER GROUP '<group>'`: Kafka manages offsets
- `DOMAIN`: Nervix stores the next offset in replicated runtime state and commits partition-to-instance assignment in the Raft-backed domain schedule

Kafka client configuration is passed through to librdkafka. Nervix does not override
`auto.offset.reset`; set it explicitly, for example to `earliest`, when a new consumer group must
read records that may already exist.

In an ACK mode, Nervix commits a Kafka position only after its downstream acknowledgement
completes. If downstream work rejects a record, the ingestor seeks back to it. A consumer-group
rebalance can move the record's partition to another group member while its batch is in flight.
The ingestor then does not seek: whichever member is assigned the partition next, this ingestor
included, resumes it from the committed offset, which never passes a rejected record. If Kafka
cannot seek back on a partition the ingestor still holds, the ingestor stops polling, refreshes
its assignment, and retries that seek. It cannot commit a later offset while the rejected
position remains unresolved. Replays after a crash or assignment change may repeat records whose
output was already written.

Under `OFFSET BY CONSUMER GROUP` in an ACK mode, the ingestor commits each acknowledged batch's
positions and reports progress for the batch only once the group's coordinator answered the commit.
That costs one commit round trip per batch, which `ACK PARALLEL` spreads over every message of a
batch. A commit the broker refuses, or does not answer within thirty seconds, is the ingestor's
transient status in `DESCRIBE INGESTOR`, and the ingestor reads the batch again, so its output can
repeat; the status clears once a later commit is answered. A graceful stop or an ownership move
waits for the answer to every commit of the work it drains, within the drain timeout.

Under `OFFSET BY CONSUMER GROUP`, a consumer that stops heartbeating without leaving its group,
because its node was killed, paused or cut off from the broker, keeps its partitions until the group
coordinator expires its session, `session.timeout.ms` after its last heartbeat. librdkafka's default
is 45 seconds; the client's pass-through configuration can change it. When the ingestor moves to
another node, the new owner's consumer joins the group as soon as it starts, but receives those
partitions, and records flow again, only after that expiry. A graceful stop or an ownership move
leaves the group first, so its partitions move without that wait. `OFFSET BY DOMAIN` assigns
partitions through the domain schedule and waits for no group session.

Under `OFFSET BY CONSUMER GROUP`, `MODE NO ACK` is the consumer's auto-commit: the consumer
commits the positions it has read on its own schedule, `auto.commit.interval.ms`, without waiting
for downstream acknowledgement, so a failure can lose records whose positions were committed before
their downstream work completed. Suspending or stopping the ingestor commits the positions it has
read since the last of those commits, and waits for the broker's answer before the consumer leaves
its group.

`OFFSET BY DOMAIN` is at-least-once. A commit records the partition's next offset in memory. Nervix persists the offsets on the runtime state snapshot interval and whenever a node stops executing the domain, and when the ingestor has state replicas, a commit completes only after every replica has acknowledged it. Crash recovery may therefore restart from a slightly stale persisted offset snapshot. The leader watches Kafka partition topology and commits any rebalance through the strongly consistent domain schedule, which is persisted through the control-plane Raft/Fjall path. Executing ingestors consume only the committed partition assignment.

A commit or start-point reset waits for its replica acknowledgements within the native checkpoint
bulk operation's thirty-second budget. A missing acknowledgement fails the commit at that deadline;
the source applies its existing rejection and retry policy. Large native checkpoints use bounded
disk staging and transfer chunks, so checkpoint length is independent of the Bulk memory ceiling.

Offset recovery details:

- persisted per-partition offsets are clamped to the partition's currently available Kafka watermark range on reassignment
- if a partition appears later and has no stored domain offset yet, unpaced domains start from the normal default behavior, while paced domains seek from the domain's current logical time

Quiescing leaves a consumer group, or unassigns domain-owned partitions, and resume continues from
committed offsets. Topic time and size retention continue during the pause. For consumer-group
offsets that age out, the client's pass-through `auto.offset.reset` setting decides whether resume
uses the earliest retained record, skips to new records, or raises a general source error. Domain
offsets clamp to the live watermark range and resume at the earliest retained record, exposing a
gap. Other members of a user-named group may consume and commit while Nervix is absent. Quiescing
does not change the auto-commit loss windows of `MODE NO ACK`.

### Pulsar

<!-- connector-matrix:ingest-pulsar-topic:start -->
```plexus,ignore
CREATE PULSAR TOPIC INGESTOR <name>
  FROM <client> TOPIC 'orders'
  (SUBSCRIPTION 'readers' OWNED BY INGESTOR | SUBSCRIPTION 'readers' PROVISIONED)
  [INSTANCES 2]
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | PULSAR client |
| Locator and validation rule | TOPIC 'orders' (pulsar_topic) |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `SUBSCRIPTION 'readers' OWNED BY INGESTOR` (string literal checked by pulsar_subscription); `SUBSCRIPTION 'readers' PROVISIONED` (string literal checked by pulsar_subscription); required<br>`INSTANCES 2` (positive count); optional; default 1 |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | Not offered: The current topic declaration exposes properties as headers but no separate typed broker metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | replayable |
| Acknowledgement confirmation | answered |
| Buffers | messages each partition consumer receives ahead of the host: driver default, a receiver queue of 1000 messages; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection: TCP, TLS and CONNECT: driver default, 10 s per address, 12 retries<br>topic lookup and subscribe: driver default, 30 s per attempt, retried without limit<br>an acknowledgement under ACK: driver default, 30 s operation timeout<br>close: unbounded |
| Driver limits | Inbound frames have no size limit. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [PULSAR TOPIC capability and mirror reference](connector-capabilities.md#pulsar-topic-ingest).
<!-- connector-matrix:ingest-pulsar-topic:end -->

Pulsar ingestors read through a shared, durable subscription. The subscription name is required by
Pulsar, but the subscription type is not exposed in Plexus. Every statement also declares who owns
the subscription, because the broker cannot say later who created one:

- `OWNED BY INGESTOR`: the ingestor owns the subscription. Its consumers create it when it is
  absent, starting at the earliest message the topic retains, and dropping the ingestor or its
  domain removes it.
- `PROVISIONED`: an operator provisioned the subscription, and Nervix never creates or removes it.
  Before an instance joins it, the instance checks that it exists. A missing subscription is the
  ingestor's transient error, as in `PULSAR subscription 'readers' of topic
  'persistent://public/default/orders' does not exist`, and the instance checks again after its
  retry policy's backoff, or on the host's reconnect cadence in `MODE NO ACK`, until the operator
  provisions it. A missing subscription does not keep the ingestor or its domain from starting.

The check subscribes once with a non-durable cursor of the same name: the broker refuses that while
the durable subscription exists, and otherwise opens a cursor that the check closes again. The
driver logs the expected refusal at `ERROR`. Like a subscribe, the check of a topic the broker does
not have creates the topic when the broker allows automatic topic creation, which is Pulsar's
default. While a check holds its cursor, the broker refuses to create a subscription of that name
as a conflict, so an operator's request that arrives in that moment fails with HTTP 409 and
succeeds when repeated. A subscription that disappears between the check and the join is created
by the join.

Each instance's consumer registers under `<domain>~<ingestor>-<instance>`, with zero-based instance
indices, so ingestors of the same name in different domains are told apart in the broker's
statistics. A consumer name does not separate what ingestors read: ingestors that name the same
subscription of the same topic share it, and each message reaches one of them.

`DROP INGESTOR` of an ingestor that owns its subscription removes the subscription, including after
`STOP`, and so does dropping its domain. Once the ingestor's consumers have closed, the node that
ran it joins the subscription with a durable consumer and unsubscribes, and the broker removes the
subscription with its backlog. Removal is best-effort and ends within the driver's ten-second
connection timeout: a broker that cannot be reached, or one that refuses because another consumer
is still connected to the subscription, is reported as a runtime error that attached sessions
observe, and does not prevent the drop. A node removes owned subscriptions one at a time and
applies the drop once its removals have ended, so a drop whose broker cannot be reached completes
only after that timeout for each owned subscription it removes. The subscription then remains until
the operator removes it or the broker expires it. `STOP`, a node restart, a relocation and a restore
keep the subscription. Replacing the source with `ALTER PULSAR TOPIC INGESTOR ... SET FROM ...`
removes the preceding owned subscription after its consumers stop and before the replacement
starts, unless the replacement reads the same subscription of the same qualified topic at the
same resolved broker address. Client names, route, codec, delivery-mode and instance-count changes
do not change that identity. Short topics use their client's namespace, so a short name and its
fully qualified spelling identify the same topic. Broker addresses compare their URL scheme,
host and effective port; an omitted port means 6650 for `pulsar` and 6651 for `pulsar+ssl`.
Different configured host names are distinct addresses even if DNS resolves them alike.
Declaring the retained subscription `PROVISIONED` hands its ownership to the operator and keeps
its backlog. Removal on replacement has the same timeout and best-effort reporting as removal
on drop; a failure does not prevent starting the replacement. `DESCRIBE INGESTOR`
shows the ownership as `pulsar subscription ownership: OWNED BY INGESTOR` or `PROVISIONED`.

Client config currently supports:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

In an `ACK` mode the ingestor acknowledges the messages a batch read from each topic in one
acknowledgement and waits for the broker's answer, within the client's 30-second operation
timeout. An acknowledgement the broker refuses shows its server error and reason as the transient
error, as in `the Pulsar broker refused an acknowledgement with NotAllowedError: ...`, and one it
does not answer in time, or whose connection closes first, shows `the Pulsar broker did not confirm
an acknowledgement`. Either way the messages are rejected and delivered again. A rejection asks the
broker to redeliver the messages and returns once the consumer sent that request, because the
broker does not answer it; a request the consumer cannot send, such as while it reconnects to a
broker it cannot reach, shows `the Pulsar consumer did not confirm sending a redelivery request`,
and the broker redelivers the messages once the consumer reconnects. `MODE NO ACK` acknowledges
each message without waiting for the broker's answer.

Suspension disconnects the consumer. The durable subscription retains its backlog and redelivers
unacknowledged in-flight messages. Broker message TTL, backlog quota policy, and inactive-subscription
expiry remain in force. If an owned subscription expires, resume creates it again at the earliest
message the topic retains, which can deliver earlier messages again, and what the topic no longer
retains from the suspension window is lost.
A provisioned subscription that expires is the ingestor's transient error until the operator
provisions it again.

### RabbitMQ

<!-- connector-matrix:ingest-rabbitmq-queue:start -->
```plexus,ignore
CREATE RABBITMQ QUEUE INGESTOR <name>
  FROM <client> QUEUE 'orders'
  [INSTANCES 2]
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | RABBITMQ client |
| Locator and validation rule | QUEUE 'orders' (rabbitmq_queue) |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | Not offered: The current queue declaration exposes transport headers but no typed AMQP delivery metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | written |
| Buffers | deliveries the broker sends ahead of their acknowledgement: at most 1 delivery; when full, holds back what fills it |
| Network waits | connection: name resolution, address attempts, TLS, the AMQP handshake, channel open and consume: `connect_timeout_ms`; 30s when omitted<br>a silent connection: driver default, twice the negotiated heartbeat |
| Driver limits | A frame above the negotiated frame_max ends the connection. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |

See the [RABBITMQ QUEUE capability and mirror reference](connector-capabilities.md#rabbitmq-queue-ingest).
<!-- connector-matrix:ingest-rabbitmq-queue:end -->

Each instance consumes with a prefetch window of one delivery, which it acknowledges once the
records decoded from it have been acknowledged through the graph. A delivery whose acknowledgement
fails, or whose payload cannot be decoded, is negatively acknowledged with requeue, so RabbitMQ
delivers it again and the ingestor retries it on its declared `RETRY POLICY` cadence.

Suspension cancels the consumer and RabbitMQ requeues unacknowledged in-flight deliveries. Other
consumers on the same queue continue. Queue length, message TTL, overflow, and auto-expiry policies
remain in force. If auto-expiry deletes the queue, resume reports a source error until an operator
re-provisions it; Nervix never creates it.

A connection that closes while a delivery is still unacknowledged, because the broker restarted,
closed it, or the network dropped it, also makes RabbitMQ requeue that delivery. The acknowledgement
or the requeue the ingestor still owes it can only be sent on the connection it arrived on, so the
instance reconnects on its `RETRY POLICY` cadence, discards what it owed, counts it in
`nervix_ingestor_transport_redeliveries_total`, which `DESCRIBE INGESTOR` also prints, and consumes
again. RabbitMQ then delivers the message again: a delivery that failed in the graph is retried,
and one the graph had already acknowledged is ingested a second time, so delivery across a
reconnect is at least once.

Each instance resolves the host of the client's `addr` through the node's configured DNS resolver
every time it connects, so a changed DNS answer takes effect on the next connection, and tries the
addresses it receives in order. A literal IPv4 address, or an IPv6 address in brackets, is
connected to as written. Resolution, the TCP connection, for `amqps` the TLS handshake, the AMQP
handshake, opening the channel, setting the prefetch window and starting the consumer share the
client's `connect_timeout_ms`, 30 seconds when it is left out; the broker certificate must name the
host `addr` names. A connection that fails or runs out of that deadline, including a name that does
not resolve, is a source error that `DESCRIBE INGESTOR` shows as the transient error, and the
instance tries again on its `RETRY POLICY` cadence without acknowledging anything.

### Redis Pub/Sub

<!-- connector-matrix:ingest-redis-pubsub:start -->
```plexus,ignore
CREATE REDIS PUBSUB INGESTOR <name>
  FROM <client> CHANNEL 'orders'
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | REDIS client |
| Locator and validation rule | CHANNEL 'orders' (redis_channel) |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Emit channel names currently use the unquoted emitter grammar. |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: The current Pub/Sub declaration delivers payloads without a typed channel metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | messages the driver reads ahead of the host: unbounded |
| Network waits | subscription: name resolution, address attempts, TLS, Redis setup and SUBSCRIBE: `connect_timeout_ms`; 30s when omitted<br>reading the next message: unbounded |
| Driver limits | None declared. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |

See the [REDIS PUBSUB capability and mirror reference](connector-capabilities.md#redis-pubsub-ingest).
<!-- connector-matrix:ingest-redis-pubsub:end -->

Redis Pub/Sub has no retained backlog, so it cannot suspend honestly. Both modes keep reading and
keep the subscriber healthy; payloads are either retained locally within the declared bound or
discarded and counted. A `TYPE REDIS` client declares connection-pool bounds even when only
ingestors reference it; see [Database Client Connection Pools](database-client-pools.md).
Each subscription owns a separate connection. It resolves the `addr` hostname through the node's
asynchronous DNS resolver on initial subscribe and every resume after a disconnect; it does not
consume a pooled command connection. For `rediss://`, TLS verifies the original hostname and
uses the configured CA and optional client identity. DNS and connection failures follow the
source's existing retry cadence; Redis Pub/Sub does not replay messages missed while disconnected.

### MQTT

<!-- connector-matrix:ingest-mqtt-topic:start -->
```plexus,ignore
CREATE MQTT TOPIC INGESTOR <name>
  FROM <client> TOPIC 'devices/+/orders'
  [INSTANCES 2]
  [SESSION CLEAN | SESSION PERSISTENT]
  [QOS 0 | QOS 1]
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | MQTT client |
| Locator and validation rule | TOPIC 'devices/+/orders' (mqtt_topic_filter) |
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1)<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1) |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1<br>`SESSION CLEAN` (keyword choice); `SESSION PERSISTENT` (keyword choice); optional; default CLEAN<br>`QOS 0` (keyword choice); `QOS 1` (keyword choice); optional; default 0 |
| Headers | Not offered: Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND (requires SESSION PERSISTENT and QOS 1)<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: The current topic declaration exposes the payload and configured subscription, with no typed publish-property metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | queued |
| Buffers | SUBSCRIBE and PUBACK requests for the connection: driver default, 1024 requests; when full, holds back what fills it |
| Network waits | connection: name resolution, TCP, TLS and CONNECT: driver default, 5 s<br>SUBACK: unbounded<br>a silent connection: driver default, keep-alive of 60 s, or the broker's |
| Driver limits | An incoming packet above 10 KiB ends the connection. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [MQTT TOPIC capability and mirror reference](connector-capabilities.md#mqtt-topic-ingest).
<!-- connector-matrix:ingest-mqtt-topic:end -->

The topic filter is a string literal; see [External Names](#external-names) for the levels and
wildcards it may hold.

Delivery constraints:

- `NO ACK` defaults to `SESSION CLEAN QOS 0`; explicit `SESSION` and `QOS` may be supplied before `MODE`
- `NO ACK` has no in-flight ACK window, so it never accepts `MAX <n>`
- `ACK` modes require `SESSION PERSISTENT QOS 1`
- `ACK PARALLEL MAX <n>` is the in-flight ACK window and `BATCH TIMEOUT` is the maximum partial-batch wait;
  a quiesce change or a stop that meets a partial batch takes in the publishes it gathered at once
  rather than after its batch timeout
- `INSTANCES <count>` controls Nervix consumer parallelism; MQTT delivery always uses Nervix-managed shared subscription groups so instances do not duplicate messages

Without a `client_id` in the client configuration, one instance connects as
`<domain>~<ingestor>`. Multiple instances connect as `<domain>~<ingestor>~<instance>`, with
zero-based instance indices. An explicit identity shared by multiple instances must contain
`{{instance}}`. Identities remain stable across node ownership changes.

`DROP INGESTOR` releases the broker sessions, including after `STOP`. Nervix stops the source,
opens a clean cleanup connection for each distinct identity, sends `UNSUBSCRIBE` for its filter,
and sends `DISCONNECT` with session expiry zero. Cleanup is best-effort: an unreachable broker is
reported and does not prevent the drop. Its retained state can remain until the broker expires it
or the operator clears it.

Changing the input with `ALTER MQTT TOPIC INGESTOR ... SET FROM ...` retires the preceding filter
and prepares the changed sessions at the new endpoint before the replacement starts. A failure to
clean up the preceding endpoint does not prevent moving to a reachable new endpoint. Sessions
whose broker, identity, filter, session mode and QoS still match are retained, including across
an instance-count change. Changing a filter discards the retired filter's broker queue.
Ordinary `STOP`, reconnect and ownership transfer retain persistent broker sessions.

`SUSPEND` is valid only with `SESSION PERSISTENT QOS 1`. It disconnects while asking the broker to
retain the session; broker offline-queue bounds and session expiry limit what can return. If the
session expires, resume establishes a fresh session and the interim is lost. `BUFFER` and `DROP`
remain connected under any valid session declaration. In ACK modes, Nervix acknowledges a payload
when it is buffered or deliberately dropped, trading broker redelivery for the declared connected
behavior.

Each instance resolves the host of its client's `addr` through the node's asynchronous DNS
resolver whenever it connects: on its first subscription and on every resume after a lost
connection. It dials the answers in order within the client's five-second connect timeout, which
also covers TLS and the MQTT handshake. For `mqtts`, TLS verifies the configured host name. A name
that does not resolve is a connection failure, never a message rejection: the instance resumes
again on its declared `RETRY POLICY`, or on the host's reconnect cadence in `MODE NO ACK`. See
[DNS for MQTT](connector-contract.md#dns-for-mqtt).

### NATS

<!-- connector-matrix:ingest-nats-subject:start -->
```plexus,ignore
CREATE NATS SUBJECT INGESTOR <name>
  FROM <client> SUBJECT 'orders.created'
  (QUEUE GROUP 'workers')
  (INSTANCES 2)
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | NATS client |
| Locator and validation rule | SUBJECT 'orders.created' (nats_subject) |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | `QUEUE GROUP 'workers'` (string literal checked by nats_queue_group); required<br>`INSTANCES 2` (positive count); required |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: The current subject declaration exposes message headers but no typed subject or JetStream metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | messages the subscription holds ahead of the host: driver default, 65,536 messages; when full, drops what does not fit without counting it |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>the subscription's flush: unbounded<br>a silent connection: driver default, a ping every 60 s; the connection ends after 2 go unanswered |
| Driver limits | None declared. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [NATS SUBJECT capability and mirror reference](connector-capabilities.md#nats-subject-ingest).
<!-- connector-matrix:ingest-nats-subject:end -->

NATS ingestors use Core NATS queue subscriptions. `QUEUE GROUP` and `INSTANCES`
are mandatory; use `INSTANCES 1` for a single queue member.

Core NATS retains no backlog, so `SUSPEND` is unavailable. A quiesced instance remains a member of
its queue group and keeps receiving its share; that share is buffered or discarded rather than
shifted to other members.

A lost connection to the server ends the instance's subscription. `DESCRIBE INGESTOR` shows the
loss, and then each failed reconnect, as its transient error, and the instance connects and
subscribes again on the host's reconnect cadence: the delay starts at 250 ms, doubles after each
failed attempt up to 30 s, and `DESCRIBE INGESTOR` shows it as the reconnect backoff. Messages the
client received before the loss are still delivered. Core NATS retains nothing for a subscription
that is not connected, so messages published while the instance reconnects never reach it.

### ZeroMQ

<!-- connector-matrix:ingest-zeromq-pull:start -->
```plexus,ignore
CREATE ZEROMQ PULL INGESTOR <name>
  FROM <client>
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | ZEROMQ client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: Payload frames have no broker position or typed transport metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the frame a peer is sending: unbounded |
| Network waits | connecting, retried without limit: unbounded<br>the ZMTP handshake: unbounded<br>receiving the next message: unbounded |
| Driver limits | The driver has no high-water mark, maximum message size or heartbeat. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [ZEROMQ PULL capability and mirror reference](connector-capabilities.md#zeromq-pull-ingest).
<!-- connector-matrix:ingest-zeromq-pull:end -->

The source is a PULL socket. `SUSPEND` keeps it open but unread, so transport buffers fill and the
peer's own high-water-mark policy determines whether sends queue, block, or drop. Buffered transport
data is read on resume. `BUFFER` and `DROP` keep reading locally.

### Syslog

<!-- connector-matrix:ingest-syslog-listener:start -->
```plexus,ignore
CREATE SYSLOG LISTENER INGESTOR <name>
  FROM <client>
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | SYSLOG client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.peer_addr: STRING OPTIONAL |
| Every live node | yes |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | frames read from TCP and TLS connections: at most 64 frames; when full, holds back what fills it |
| Network waits | the TLS handshake of an accepted connection: unbounded<br>reading a connection: unbounded |
| Driver limits | Datagrams wait in the kernel's UDP receive buffer, which drops what does not fit. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [SYSLOG LISTENER capability and mirror reference](connector-capabilities.md#syslog-listener-ingest).
<!-- connector-matrix:ingest-syslog-listener:end -->

The client binds a UDP, TCP, or TLS listener. UDP treats one datagram as one message; TCP detects
RFC 6587 octet-counted or LF-terminated framing per frame; TLS uses RFC 5425 octet counting.
Messages expose optional `metadata.peer_addr`. The source takes no `INSTANCES` clause and has no
application acknowledgment. Every live cluster node runs the listener; it is independent of the
leader and restarts or joins with its owning node. See [Syslog](syslog.md) for the client keys,
cluster lifecycle, framing, TLS, limits, and failure semantics.
An invalid client setting or unreadable TLS file fails listener startup with the Syslog key or
file and its underlying cause. A frame or connection failure keeps its cause in the source
diagnostic. These failures do not acknowledge a message; Syslog has no application ACK.

### SQS

<!-- connector-matrix:ingest-sqs-queue:start -->
```plexus,ignore
CREATE SQS QUEUE INGESTOR <name>
  FROM <client> QUEUE 'orders'
  [INSTANCES 2]
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | SQS client |
| Locator and validation rule | QUEUE 'orders' (sqs_queue) |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `INSTANCES 2` (positive count); optional; default 1 |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | Not offered: The current queue declaration exposes message attributes through headers but no separate typed receipt metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | answered |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | connection: name resolution, TCP and TLS: driver default, 3.1 s<br>the queue URL lookup, a receive and a delete: unbounded |
| Driver limits | A request that stalls below 1 byte a second for 5 s is cancelled. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [SQS QUEUE capability and mirror reference](connector-capabilities.md#sqs-queue-ingest).
<!-- connector-matrix:ingest-sqs-queue:end -->

Starting the ingestor sends no request to the service. Each source instance looks its queue up
before its first poll and keeps the queue's URL from then on, suspensions included. The source
resolves the host of its client's `endpoint` through the node's asynchronous resolver each time it
opens a connection, and dials the answers as [Dialling The
Answers](name-resolution.md#dialling-the-answers) describes for Smithy's client. Every request is
still signed for the configured host, and over HTTPS the service certificate must name that host.
Without `tls_ca_file` the client trusts the platform's native roots and follows the `HTTP_PROXY`,
`HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY` environment variables; with it, the client trusts that CA
alone and connects directly. A missing name, a silent name server or an unreachable answer fails the
queue lookup, a poll, or a deletion. The AWS SDK's standard retry mode makes up to three attempts at
each such request. A failed lookup or poll is then a transient source failure that
`DESCRIBE INGESTOR` shows and the source retries on its `RETRY POLICY`, so an ingestor started while
the service is unreachable consumes once the service answers. A queue that does not exist fails the
lookup with its name, such as `SQS queue 'orders' does not exist`, and is retried the same way;
Nervix never creates it.

Each poll is a `ReceiveMessage` for one message with all of its attributes, which the service
answers as soon as a message arrives or after a one-second wait without one. The service hides the
message it returns from every other receive until the queue's visibility timeout ends, and the
source deletes it only once it is acknowledged, so a failed lookup or poll never removes a message
from the queue. A message whose acknowledgement fails or does not arrive within the mode's
`ACK TIMEOUT` is rejected, which leaves it in the queue. So is one whose deletion fails, which
`DESCRIBE INGESTOR` shows as a transient error; the source does not repeat the deletion. Either way
the message becomes visible again once its visibility timeout ends and is received again, as a
duplicate if the graph had already processed it, and the source polls again after its
`RETRY POLICY` delay. A queue whose visibility timeout is shorter than the mode's `ACK TIMEOUT` can
also hand a message that still awaits its acknowledgement to another receive.

Suspension stops polling. Messages wait in the queue only for its configured retention period, and
the ingestor resumes by polling past anything the service expired. A poll still waiting when the
ingestor quiesces or stops is never abandoned, because the service hides the message it answers
with until its visibility timeout ends: the source sends no further poll and waits for that answer,
which a responsive service gives within the poll's one-second wait, and a message it brings
dispatches before the source suspends or closes. Only a service that has not answered within
10 seconds of the quiesce change or the stop ends that poll unanswered, which `DESCRIBE INGESTOR`
shows as a transient error; a message the poll was returning then stays invisible until its
visibility timeout ends.

### Prometheus

<!-- connector-matrix:ingest-prometheus-query:start -->
```plexus,ignore
CREATE PROMETHEUS QUERY INGESTOR <name>
  FROM <client>
  (QUERY 'up')
  [MAX BODY SIZE 16MiB]
  (EVERY 15s)
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | PROMETHEUS client |
| Locator and validation rule | Not offered: QUERY is a PromQL expression option rather than an external entity name. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | `QUERY 'up'` (string literal); required<br>`MAX BODY SIZE 16MiB` (byte size); optional; default 16MiB<br>`EVERY 15s` (duration); required |
| Headers | Not offered: The Prometheus query API is polled for samples; no Prometheus remote-write emitter is declared. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |
| Metadata | Not offered: The current query result is decoded as payload; no separate typed query or evaluation-time metadata namespace is declared. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the query response: set by MAX BODY SIZE; when exceeded, fails the poll; the next poll reads again<br>samples the host takes in as one intake batch: at most 1024 samples; when full, holds back what fills it |
| Network waits | a query: name resolution, connection, TLS and the response: `timeout_ms`; required |
| Driver limits | Response heads above 100 headers or 417,792 bytes are refused. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [PROMETHEUS QUERY capability and mirror reference](connector-capabilities.md#prometheus-query-ingest).
<!-- connector-matrix:ingest-prometheus-query:end -->

Prometheus samples are flattened into JSON before codec decoding.
`MAX BODY SIZE <bytes>`, written between `QUERY` and `EVERY`, bounds how many bytes of one query
response a poll reads; a statement that leaves it out reads at most `16MiB`. The size is a whole
number greater than zero followed by `B`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB` or `TiB`, and
`SHOW CREATE` renders it as written. A response whose announced length or whose body exceeds the
bound fails the poll before any of its samples is delivered: `DESCRIBE INGESTOR` shows the failure
as the ingestor's transient error, such as `transient error: failed to read from prometheus source:
Prometheus query response exceeds MAX BODY SIZE 16MiB`, and the next poll queries again.
`DESCRIBE INGESTOR` prints the bound in effect, whether the statement wrote it or not, as
`prometheus max body size: 16MiB`.
The samples of one response enter the graph in intake batches of at most 1024 samples, and each
batch is decoded and routed as its own group, so a large vector is never taken in as one batch of
any size. The remaining samples wait while a batch is routed, and the next query waits until every
sample of the current one has entered the graph.
The first query becomes due after one `EVERY` interval. Nervix supplies each anchored due instant
as Prometheus's evaluation time; a slow query coalesces missed occurrences and returned samples are
evaluated with a fresh domain execution snapshot.
`SUSPEND` skips scrapes and leaves a sampling gap. `BUFFER` keeps querying at the declared cadence
and delivers retained results in order after resume. A scrape whose `ON INGESTION` unfolding finds
the node's extension workers full waits for them; see
[When The Extension Workers Are Full](#when-the-extension-workers-are-full).

The ingestor reads through a `TYPE PROMETHEUS` client. Every key of its `CONFIG` is Nervix's own:

| Key | Required | Value |
| --- | --- | --- |
| `addr` | Yes | The root of the Prometheus HTTP API: an `http` or `https` URL with a host, whose path is the prefix the API is served under, such as `/monitoring` behind a reverse proxy. Each query is sent to `api/v1/query` beneath it, so the URL carries no query string and no fragment. |
| `timeout_ms` | Yes | How long one query may take, as a positive whole number of milliseconds. It covers name resolution, connection setup, TLS and the response. |
| `tls_ca_file` | No | A PEM certificate authority that HTTPS trusts in addition to the roots the client already trusts. |
| `tls_cert_file`, `tls_key_file` | Together | The PEM certificate chain and private key the client presents to authenticate over HTTPS. |

```nspl
CREATE CLIENT prom_main
  TYPE PROMETHEUS
  CONFIG {
    'addr' = 'https://prometheus.example.com/monitoring',
    'timeout_ms' = 5000
  };
```

`CREATE CLIENT` refuses a configuration that cannot configure a query source, and names the key:
a key the client does not declare, a key written twice, a missing `addr` or `timeout_ms`, an
`addr` that is not such a URL, a `timeout_ms` that is not a positive whole number of
milliseconds, or one of `tls_cert_file` and `tls_key_file` without the other. For example,
`PROMETHEUS client 'prom_main' in domain 'shop' cannot use its CONFIG: 'timeout_ms' is required`.
Keys are matched without regard to ASCII case, as the client reads them. A value can name a file
of the client's `MOUNT`, as in `'tls_ca_file' = '{{ dev_tls }}/ca.pem'`. When a source opens, it
checks the configuration again with those values rendered, so the address it queries is the one
the rules admit.

### `HTTP ENDPOINT` And `WEBSOCKETS ENDPOINT`

#### HTTP ENDPOINT

<!-- connector-matrix:ingest-http-endpoint:start -->
```plexus,ignore
CREATE HTTP ENDPOINT INGESTOR <name>
  FROM <endpoint>
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | Http endpoint |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK<br>ACK MAX 8 ACK TIMEOUT 5s |
| Options in statement order | No entity-specific options. |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |
| Metadata | Not offered: HTTP headers are available through header functions; this implementation declares no typed request or response metadata fields. |
| Every live node | yes |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | a request body: unbounded<br>unresolved requests under MODE ACK, per node and ingestor: set by MODE ACK MAX; when full, immediately refuses the request for its sender to retry |
| Network waits | a client's request head and body: unbounded<br>graph acknowledgement under MODE ACK: `ACK TIMEOUT, or the exact intake lifetime ending` |
| Driver limits | Request heads above 100 headers are refused. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [HTTP ENDPOINT capability and mirror reference](connector-capabilities.md#http-endpoint-ingest).
<!-- connector-matrix:ingest-http-endpoint:end -->

#### WEBSOCKETS ENDPOINT

<!-- connector-matrix:ingest-websockets-endpoint:start -->
```plexus,ignore
CREATE WEBSOCKETS ENDPOINT INGESTOR <name>
  FROM <endpoint>
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | Websockets endpoint |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK<br>ACK MAX 8 ACK TIMEOUT 5s |
| Options in statement order | No entity-specific options. |
| Headers | `read_header`, `read_headers` |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |
| Metadata | Not offered: The current frame declaration has no typed frame or handshake metadata namespace. |
| Every live node | yes |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | unresolved data frames under MODE ACK, per node and ingestor: set by MODE ACK MAX; when full, immediately refuses the request for its sender to retry |
| Network waits | graph acknowledgement under MODE ACK: `ACK TIMEOUT, or the exact intake lifetime ending`<br>the signaling handshake: `the signaling protocol's TIMEOUT`<br>reading the next frame: unbounded |
| Driver limits | A message above 64 MiB or a frame above 16 MiB ends the session. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [WEBSOCKETS ENDPOINT capability and mirror reference](connector-capabilities.md#websockets-endpoint-ingest).
<!-- connector-matrix:ingest-websockets-endpoint:end -->

This is how Nervix receives inbound HTTP requests and WebSocket connections on its own server-side
endpoints. An `HTTP ENDPOINT` ingestor reads an endpoint declared `TYPE HTTP`, and a
`WEBSOCKETS ENDPOINT` ingestor reads one declared `TYPE WEBSOCKETS`; completion after `FROM` offers
only endpoints of that type. An endpoint of the other type is refused when the statement is
applied, with a message such as `requires a TYPE HTTP endpoint, but endpoint 'stream' is TYPE
WEBSOCKETS`. Both heads take the same body.

Both endpoint entities accept `MODE NO ACK` or `MODE ACK MAX <n> ACK TIMEOUT <duration>`.
The ACK window is explicit: `MAX` is 1–65,535 outstanding payloads per ingestor on each node.
An acknowledged endpoint requires `ON QUIESCE REJECT RETRY AFTER <duration>`. The sender owns
retry; there is no endpoint `RETRY POLICY`, `ACK PARALLEL`, or `BATCH` clause. `BUFFER MAX SIZE`
is available with `MODE NO ACK`. Transport headers are available through `read_header` and
`read_headers`; these declarations have no typed request metadata namespace. Both ingestors
execute on every cluster node and are excluded from placement members and placement completion.

For example, this Plexus declaration holds the HTTP response while its payload's acknowledgement
root waits for every attached downstream route:

```plexus
CREATE HTTP ENDPOINT INGESTOR acknowledged
  FROM intake MODE ACK MAX 8 ACK TIMEOUT 5s
  ON QUIESCE REJECT RETRY AFTER 1s
  DECODE USING event_codec
  TO events INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
```

| HTTP result in `MODE ACK` | Meaning |
| --- | --- |
| 200 | The payload's graph acknowledgement root completed under the declared error policies. |
| 400 | The codec or local route construction refused the input. |
| 503 with `Retry-After` | The window or execution capacity is full, intake is quiesced, downstream negatively acknowledged, the ACK timeout expired, or the exact intake ended. |

Every ACK response has a JSON object containing exactly `outcome` and `cause`, using the existing
[client submission outcome vocabulary](client-io-architecture.md). For example, a local refusal
is `{"outcome":"processing_failed","cause":"rejected"}`. Downstream rejection uses that same
object with status 503. Timeout uses `processing_failed` / `ack_timeout`; a full window uses
`not_admitted` / `busy`; intake ending uses `outcome_unknown` / `interrupted`. The response contains
no payload or diagnostic details. Runtime events retain the ingestor's ordinary diagnostics.
A 503 uses the declared retry delay, rounded up to whole seconds. A retry can duplicate output
that already completed; timeout or interruption does not roll back admitted work.

The physical ACK timeout starts when a complete payload obtains its window slot. It is positive
and fits the supported physical deadline range. Downstream keepalive does not restart it. The
window uses immediate admission: it never queues a request for a response slot. A window already
full at the HTTP head refuses before reading the body; dispatch checks capacity again after the
body completes, so competing requests cannot overrun the window. It does not bound
HTTP request-head or body reads. HTTP request bodies retain their separately declared unbounded
limit, and WebSocket messages retain the driver's message and frame limits.

In `MODE NO ACK`, admission still answers HTTP 202 without waiting for downstream acknowledgement.
A codec refusal becomes a decode rejection consumed by `ON GENERAL ERROR`; it also answers 202.
With `BUFFER`, bodies that fit receive 202 and drain after resume. A full buffer rejects new intake
without discarding bodies already accepted. If extension workers cannot unfold a fresh payload,
HTTP answers 503 and a WebSocket closes with 1013; buffered payloads instead retain their place
until workers can take them. See [When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).

Quiesce is enforced independently on every serving node. `REJECT` refuses new requests and
WebSocket upgrades with 503. A stopped or absent intake refuses new requests too. A route shared
by unacknowledged ingestors accepts when at least one accepts; it includes `Retry-After` only when
all refusals supply a delay. When an acknowledged binding participates, success waits for its root;
a failure after any binding admitted the input is not reported as `not_admitted`.

A WebSocket endpoint in `MODE ACK` requires an explicit signaling protocol. Its outbound wire
format encodes the same `outcome` / `cause` object for each data frame, both during `ACCEPT DATA`
and after signaling. JSON and YAML use text; CBOR and protobuf use binary. RAW carries the object's
JSON text. A protobuf outbound message must represent both string fields; startup validates every
fixed outcome before binding intake. The protocol's control frames retain their declared meaning.

A WebSocket retains its selected route and signaling protocol throughout the connection. Stopping
or replacing its source ends that exact intake, including signaling data intake. Held acknowledged
responses resolve with 503 (or the equivalent outcome frame) when intake stops or ends; the next
payload on a retained ended route closes with 1013. Open a new connection for a replacement source.
Stopping a domain withdraws only its routes and intakes; other domains on the same host and path
continue receiving requests. A withdrawn route returns HTTP 404.

Server-side endpoints are hosted under a `VHOST`. A plain VHOST serves HTTP and WS on the HTTP listener. A TLS-enabled VHOST serves HTTPS and WSS on the separate HTTPS listener.

TLS is configured on the VHOST itself:

```nspl
CREATE IF NOT EXISTS VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle VERSION 3;
```

`WITH TLS` requires a version. `VERSION LATEST` binds the highest completed version of the bundle
when the statement is applied:

```nspl
CREATE IF NOT EXISTS VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle VERSION LATEST;
```

Either way the VHOST stores one concrete version and keeps presenting that certificate after later
uploads; `SHOW CREATE VHOST` renders the number it bound. Rotate the certificate by uploading a new
version and moving the VHOST to it with `REBIND RESOURCE`, which refreshes the HTTPS listener on
every node without pausing ingestion. See [Resources](./resources.md#rebinding-existing-models).

The referenced resource bundle must contain:

- `tls.crt`
- `tls.key`
- `ca.crt`

### `WEBSOCKETS CONNECTION`

<!-- connector-matrix:ingest-websockets-connection:start -->
```plexus,ignore
CREATE WEBSOCKETS CONNECTION INGESTOR <name>
  FROM <client>
  MODE <mode>
  ON QUIESCE <quiesce>
  DECODE USING <codec>
  TO <relay> UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table. Choose a compatible quiesce form and satisfy its option requirements.

| Capability | Declaration |
| --- | --- |
| Reference | WEBSOCKETS client |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: A frame carries its payload; handshake headers are not a per-frame header map. |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: The current frame declaration has no typed frame or handshake metadata namespace. |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | frames that arrive while the signaling handshake runs: unbounded |
| Network waits | connection: name resolution, address attempts, TLS and the opening upgrade: `connect_timeout_ms`; 30s when omitted<br>the signaling handshake: `the signaling protocol's TIMEOUT`<br>reading the next frame: unbounded<br>close: fixed 1s |
| Driver limits | A message above 64 MiB or a frame above 16 MiB ends the connection. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |

See the [WEBSOCKETS CONNECTION capability and mirror reference](connector-capabilities.md#websockets-connection-ingest).
<!-- connector-matrix:ingest-websockets-connection:end -->

This opens an outbound WebSocket connection through a `TYPE WEBSOCKETS` client and decodes text or
binary frames.
For `ws` and `wss`, each initial connection and resume resolves the URL host through the node's
asynchronous resolver. It tries the returned addresses in order within one 30-second budget for
DNS, TCP, TLS and the opening upgrade. The upgrade still uses the configured URL for the HTTP Host
header, TLS server name, path and query; custom TLS roots and client identity remain in effect.
An expired DNS answer is refreshed on a new attempt, while a healthy connection stays open.
Resolution, connection and read failures are transient source failures. A peer Close records its
code and reason in `DESCRIBE INGESTOR`; EOF also triggers recovery. Reconnects wait 250 ms initially,
doubling to at most 30 seconds while attempts keep failing. A successful upgrade alone does not
reset the delay or clear the last failure: a delivered batch does. Cancelling or stopping the
source drops a pending connection attempt.
Both quiesce modes continue polling the connection so keepalives and reconnect behavior remain live.
A stop-reading mode is unavailable because it would starve protocol maintenance and become a
disconnect.

Suspension and terminal close send a Close frame and wait up to one second for the peer's reply,
including the time needed to send the frame, before releasing the connection. A peer that does
not answer cannot hold shutdown indefinitely. Terminal close also clears queued signaling
payloads; suspension retains them.

A `WEBSOCKETS CONNECTION` ingestor is a single-owner ingestor. It keeps its existing primary and
replica assignment through schedule recomputation while every assigned cluster node remains live,
just like Kafka, MQTT, and the other client sources. Creating or changing another runtime node, a
cluster node joining or being uncordoned, and a soft placement-policy change do not move its
outbound session. Failover, draining its owner, an explicit `RELOCATE`, or a newly effective
`REQUIRE COLOCATION` group can move it; a real move closes the former owner's session and opens a
new session on the new owner.
`HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors are different because
their listeners execute on every live cluster node.

Outbound WebSocket clients can declare `WITH SIGNALING PROTOCOL <name>` after
`TYPE WEBSOCKETS`. Server-side WebSocket endpoints can declare the same clause
after `TYPE WEBSOCKETS`.

A signaling protocol declares a wire format and expresses the whole handshake as
[JAQ](./schemas-and-codecs.md#jaq-transformations) programs:

```nspl
CREATE SIGNALING PROTOCOL bybit_subscribe
  FORMAT JSON
  ON CONNECT
  SEND JAQ '{op: "subscribe", args: ["publicTrade.BTCUSDT"]}'
  WAIT JAQ '.op == "subscribe" and .success == true'
    FAIL JAQ 'select(.success == false) | .ret_msg'
    ACCEPT DATA
  TIMEOUT 5s;
```

Steps run strictly in the order they are written: a step completes before the
next one starts, which is what lets a request depend on an earlier reply. A
`SEND` step writes its frames; a `WAIT` step blocks until every matcher it lists
is satisfied, in any arrival order.

Each `SEND JAQ` program must produce exactly one value, sent as a frame in the
declared format. `JSON`, `YAML`, `TOML`, `XML`, and `RAW` travel as text frames;
`CBOR` and `PROTOBUF` travel as binary frames. A `RAW` program must produce a
string, which is sent verbatim — that is how plain-text handshakes are
expressed.

Every incoming frame is decoded with the same format into exactly one value,
which for `XML` is the frame's root element, and offered to the current step. A
textual format also reads payloads delivered as binary frames, since peers
commonly frame text that way; a binary format reads binary frames only.
Failure matchers run first, so a rejection is never swallowed by a lenient
acknowledgement matcher: a `FAIL JAQ` guard on the step is checked before the
protocol-wide one, and the first to produce a value that is neither `null` nor
`false` aborts the handshake carrying that value as the reason. Otherwise the
step's outstanding matchers are tried and the first satisfied one is consumed.
Because matchers assert only the fields they name, acknowledgements carrying
connection ids, timestamps, or echoed parameters still match.

A matcher that errors on a frame of a different shape counts as a non-match
rather than a connection failure. On timeout, the error names the matchers the
current step was still waiting on.
Invalid signaling programs fail validation with the protocol, clause, step, and compiler cause.
Handshake send, capture, encoding, and transport failures retain their causes in the source
diagnostic. A failed handshake delivers no frame to the relay.

### Data Arriving During The Handshake

Peers commonly start streaming before they finish acknowledging. `ACCEPT DATA`
says where payload starts flowing to the relay:

```nspl,ignore
ON CONNECT ACCEPT DATA                                  -- from the first frame
WAIT JAQ '<matcher>' [FAIL JAQ '<matcher>'] [CAPTURE '<program>'] ACCEPT DATA
```

On `ON CONNECT` the relay is open before anything is negotiated. On a `WAIT`
step it opens when that step completes, which is the usual case: completing the
step is what proves the peer is streaming. Because a step may list several
matchers, a client can subscribe to two streams at once and open the relay when
the first subscription is confirmed while the second is still outstanding.

Frames that arrive before the relay opens are not payload the graph asked for,
so they are dropped rather than held. Nothing accumulates in memory, and nothing
reaches a relay from a connection that was never established. Without
`ACCEPT DATA` anywhere, payload starts flowing once the handshake finishes.

### Sequencing And Captured State

`CAPTURE` records values from the frame that satisfied its matcher, and every
program can read what has been captured so far through the `$state` variable:

```nspl
CREATE SIGNALING PROTOCOL exchange_login
  FORMAT JSON
  ON CONNECT
  SEND JAQ '{op: "auth", key: "..."}'
  WAIT JAQ '.op == "auth" and .success' CAPTURE '{token: .data.token}'
  SEND JAQ '{op: "subscribe", token: $state.token, id: 1}',
           '{op: "subscribe", token: $state.token, id: 2}'
  WAIT JAQ '.id == 1' ACCEPT DATA
  WAIT JAQ '.id == 2'
  TIMEOUT 5s;
```

`$state` starts as an empty object; each `CAPTURE` must produce an object, whose
entries are merged in with later values winning. A capture that fails or yields
a non-object fails the handshake, because its matcher already accepted the frame.
`CAPTURE` attaches to a single-matcher `WAIT` only, since it describes the one
frame that matched.

A handshake may wait before it sends anything, which is how a server-initiated
challenge is answered — wait for the challenge, capture it, then reply:

```nspl
CREATE SIGNALING PROTOCOL challenge_response
  FORMAT JSON
  ON CONNECT
  WAIT JAQ '.challenge' CAPTURE '{nonce: .challenge}'
  SEND JAQ '{op: "answer", nonce: $state.nonce}'
  WAIT JAQ '.accepted'
  TIMEOUT 5s;
```

The `TIMEOUT` is one budget for the whole handshake, and captured state lives
only for its duration — it is never logged, and does not reach ingestion.

`PROTOBUF` signaling declares its resource, the version it compiles, and one message type per
direction:

```nspl,ignore
CREATE SIGNALING PROTOCOL protobuf_subscribe
  FORMAT PROTOBUF USING RESOURCE proto_bundle VERSION 1
    CONFIG {'file' = 'signaling.proto', 'include' = '.'}
    SEND MESSAGE 'nervix.test.Subscribe'
    WAIT MESSAGE 'nervix.test.Ack'
  ON CONNECT
  SEND JAQ '{id: 1}'
  WAIT JAQ '.id == 1'
  TIMEOUT 5s;
```

`VERSION` is mandatory. `VERSION LATEST` binds the highest completed version when the statement is
applied, and the protocol stores that number. `SHOW CREATE SIGNALING PROTOCOL` renders it.

Protobuf decoding is permissive: unknown fields are kept and missing fields take
their proto3 defaults, so nearly any binary frame decodes as the `WAIT MESSAGE`
type. Write matchers that test meaningful field values rather than relying on a
decode failure to reject a frame.

## Instancing

The following entities declare `INSTANCES`; presence and defaults follow the vocabulary.

<!-- connector-matrix:instancing:start -->
| Entity | INSTANCES |
| --- | --- |
| KAFKA TOPIC | optional; default 1 |
| PULSAR TOPIC | optional; default 1 |
| MQTT TOPIC | optional; default 1 |
| NATS SUBJECT | required |
| RABBITMQ QUEUE | optional; default 1 |
| SQS QUEUE | optional; default 1 |
<!-- connector-matrix:instancing:end -->
