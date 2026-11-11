# Ingestors

*Plexus, the Nervix graph definition language.*

Ingestors are the entry points into the runtime graph.

A typical ingestor:

```plexus
CREATE BRANCH by_user
  SCHEMA user_branch TTL 5m;

CREATE IF NOT EXISTS KAFKA TOPIC INGESTOR kafka_notifications
  FROM kafka_main
  TOPIC 'notifications'
  OFFSET BY CONSUMER GROUP 'nervix_consumer' START AT LATEST
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

```plexus,ignore
CREATE [IF NOT EXISTS] <CONNECTOR> <ENTITY> INGESTOR <name>
  FROM <body>
  [DECODE USING <codec>]
  [TIMESTAMP NOW | TIMESTAMP AT <field> | TIMESTAMP AT metadata.<field>]
  [FILTER WHERE <expression>]
  TO <relay> <route body>
  [TO <relay> <route body> ...]
  ON GENERAL ERROR IGNORE | LOG;
```

`DECODE USING` is required under every head except `CLIENT PRODUCER`, which decodes nothing and
does not accept it. A client the body names must be a client of the head's connector, and an
endpoint must have the kind selected by the head; completion after `FROM` offers only those.
Any other reference is refused when the statement is applied, naming both kinds, such as
`requires a KAFKA client, but client 'cache' is a REDIS client`. `SHOW CREATE INGESTOR` renders the
complete statement under its head. `DESCRIBE INGESTOR` prints the pair, reference and locator on
its `from:` line, such as `from: KAFKA TOPIC kafka_main TOPIC 'Orders'`. The following rows show
`options: OFFSET BY CONSUMER GROUP 'Readers' START AT LATEST INSTANCES 1`, `mode: NO ACK`, and
`quiesce: SUSPEND`.
The shared [inspection block](connector-contract.md#shared-entity-presentation) then reports its
envelope, batching, output flush policies and error policies. `SHOW INGESTORS` prints the pair
alone, such as `from=KAFKA TOPIC`.

Every ingestor defines:

- the destination relay or relays
- the codec used for decoding, or for a [client ingestor](#client-ingestors) the schema its
  batches carry
- a route-local outgoing branch declaration
- a flush policy for every destination relay
- a message error policy for every destination relay
- the timestamp source
- the connector and entity it reads, and the ingestor body that follows `FROM`
- the delivery mode
- the ingestor-specific quiesce mode and every variable it requires
- optional node-level `FILTER WHERE` and route-local `INHERIT` / `SET` / `WHERE`

`FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` is required after every `TO
<relay>`. The [Plexus Overview](plexus-overview.md) defines the system-owned 100 µs minimum batching
window for `FLUSH IMMEDIATE`. It has no size boundary. Every route also requires its own
`ON MESSAGE ERROR <policy>`. Each route buffers and handles message-specific construction failures
independently. `ON MESSAGE ERROR SEND TO` uses that same route's interval and maximum batch-size
boundaries for error-record delivery. `ON GENERAL ERROR` remains node-level because it handles
ingestor or transport failures that are not tied to one message or output route.

`MAX BATCH SIZE` counts logical Arrow value, offset, and validity bytes. It excludes unused Arrow
buffer capacity and object overhead. In delivery modes, `MAX <n>` bounds the in-flight window of
`ACK PARALLEL` or endpoint `ACK`; `MODE NO ACK` never accepts it.

At runtime, the ingestor:

- decodes inbound payloads into runtime records; a JAQ-backed codec may
  [unfold](schemas-and-codecs.md#unfolding-payloads) one payload into zero or more records
- collects decoded records into a bounded ingest group that closes once it holds 1,024 or
  more messages, or earlier when the ingestor goes quiet for 5 ms; the records of one payload always
  join one group together, so a payload that unfolds past the remaining room closes its group
  above 1,024. This bound is independent of every route `FLUSH` policy and caps the first Arrow
  batch built from external input at 1,023 messages plus one payload's records
- executes `FILTER WHERE` once against that whole group
- executes each route's ordered construction and `WHERE` program once against the surviving group
- resolves the concrete branch group from the referenced `CREATE BRANCH`
- accumulates decoded rows independently for every matching destination and branch group
- writes each route's buffered rows when its configured interval or size boundary fires, when the
  documented [`FLUSH IMMEDIATE` system timeout](plexus-overview.md) expires, or when the domain is
  force-flushed by a model alteration, a domain pause, or a planned ownership handoff

Branch execution receives these completed Arrow batches and does not buffer them behind another
flush policy.

Ingestor acknowledgement is per payload. An acknowledged delivery mode acknowledges a payload once
every record decoded from it has been acknowledged on every route it reached, and a negative
acknowledgement of any of those records negatively acknowledges the payload.
`ACK PARALLEL MAX <n>` windows count payloads. A client ingestor's payload is one submitted batch.

Timestamp selection and admission use one domain execution snapshot when the ingest group is
delivered. `TIMESTAMP NOW` records that snapshot. `TIMESTAMP AT <field>` reads a decoded payload
field and `TIMESTAMP AT metadata.<field>` a metadata field the ingestor's entity declares; the field
must be `DATETIME`, and `CREATE` and `ALTER` refuse an undeclared metadata field or one of another
type, naming the fields the entity declares. A message that carries no value for the field is
rejected as a null payload timestamp is, and one whose connector hands over a value no `DATETIME`
holds is refused before selection (see the metadata rules under
[Value Construction and Filters](#value-construction-and-filters)). These and connector-owned event
timestamps preserve the
external instant exactly; `TIME RATE` never scales them. The internal low
and high watermarks initially equal the selected event time, so source event time remains distinct
from delivery and observation time throughout the graph. The 5 ms idle close and the
`FLUSH IMMEDIATE` 100 µs minimum are physical monotonic waits. `FLUSH EACH` and paced admission use
domain-logical time. See [Domains And Time](domains-and-time.md#ingestion-timestamps).
For a decoded group, timestamp selection and paced admission operate on Arrow columns. The group
keeps admitted rows together; each rejected or missing event timestamp follows the ingestor route's
`ON MESSAGE ERROR` policy with code `validation` and operation `admit`, preserving that row's ACK
and ingestor metadata. A batch containing both kinds can therefore deliver its accepted rows while
its rejected rows reach the configured error route.

## Delivery Modes

Every ingestor body declares its delivery mode with `MODE`, polled and listener ingestors included:

- `NO ACK` is one composed phrase. Nothing waits on an acknowledgement, and it takes no variables.
- `ACK SEQUENTIAL ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>` lets one
  payload at a time await its acknowledgement.
- `ACK PARALLEL MAX <n> BATCH TIMEOUT <duration> ACK TIMEOUT <duration> RETRY POLICY BACKOFF
  <duration> MAX <duration>` lets at most `n` payloads await their acknowledgement; `BATCH TIMEOUT`
  is the longest wait for a partial batch. A `CLIENT PRODUCER` ingestor writes `ACK PARALLEL MAX
  <n> ACK TIMEOUT ...` without `BATCH TIMEOUT`.
- `ACK MAX <n> ACK TIMEOUT <duration>` is the endpoint response mode. It holds at most `n`
  outstanding payloads per ingestor on each node and requires `ON QUIESCE REJECT RETRY AFTER
  <duration>`. The sender owns retry.

For `ACK SEQUENTIAL` and `ACK PARALLEL`, `ACK TIMEOUT` bounds how long a payload may make no
acknowledgement progress, not how long its processing takes. Work under way on its records reports
them alive on the physical clock: a node that received a record from another node does for as long
as it holds it, the node delivering a
record to another node does until that node has admitted it, and an emitter whose publish waits for
a retry does for the records it holds. A payload whose records report nothing for its `ACK TIMEOUT`
is delivered again, and its earlier delivery is not cancelled and may still complete. The ingestor
reports that as a timeout, not as a failed acknowledgement: its error event names the expired
`ACK TIMEOUT`, and `nervix_ingestor_ack_outcomes_total` counts it as `timed_out` beside the
`acknowledged`, `failed` and `shutdown` waits, while a tree the graph failed keeps the reason its
failing branch gave. See [Ingestor Acknowledgement And
Lifecycle](metrics-and-observability.md#ingestor-acknowledgement-and-lifecycle).

Endpoint `ACK TIMEOUT` instead bounds the complete response wait on the physical clock; progress
does not restart it. Expiry reports `processing_failed` with cause `ack_timeout` without undoing
admitted effects. See [endpoint ingestion](#http-endpoint-and-websockets-endpoint).

Each entity accepts only the modes it can honor, and completion offers only those:

`CREATE` and `ALTER` validate the complete delivery policy before accepting it. Durations must be
readable and fit the schedulable deadline bound of 9,223,372,036,854,775,807 nanoseconds. ACK timeouts
and both retry durations must be positive, and `RETRY POLICY MAX` must be at least `BACKOFF`.
`BATCH TIMEOUT 0s` is allowed and gathers available messages without waiting. A refused alteration
keeps the ingestor's accepted policy.

| Entity | `MODE` forms |
| --- | --- |
| `KAFKA TOPIC`, `PULSAR TOPIC`, `MQTT TOPIC`, `RABBITMQ QUEUE` | `NO ACK`, `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` |
| `SQS QUEUE` | `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` with `MAX` at most 10 |
| `NATS STREAM` | `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` |
| `CLIENT PRODUCER` | `ACK SEQUENTIAL ...`, `ACK PARALLEL ...` without `BATCH TIMEOUT` |
| `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT` | `NO ACK`, `ACK MAX <n> ACK TIMEOUT <duration>` |
| `HTTP REQUEST`, `WEBSOCKETS CONNECTION`, `REDIS PUBSUB`, `NATS SUBJECT`, `ZEROMQ PULL`, `SYSLOG LISTENER`, `PROMETHEUS QUERY` | `NO ACK` |

In an `ACK` mode an ingestor acknowledges a message on its transport only once every route and error
delivery the message unfolded into has completed, and it reports that progress only once the
transport confirmed the acknowledgement as far as the entity can: an `SQS QUEUE` waits for the
broker's answer to its deletion, a `KAFKA TOPIC` for the answer to its consumer-group commit, or
under `OFFSET BY DOMAIN` for its offsets' state replicas, a `PULSAR TOPIC` for the broker's answer
to its acknowledgement, a `NATS STREAM` for the server's answer to its acknowledgement, and a
`RABBITMQ QUEUE` and an `MQTT TOPIC` for their acknowledgements to be written to the connection,
which neither broker answers. An acknowledgement the transport fails to
confirm, or a rejection it fails to take back, is an ingestor error that `DESCRIBE INGESTOR` shows as
the transient error until a later acknowledgement is confirmed, and the messages are delivered
again. `DESCRIBE INGESTOR`'s `source:` section and the matching Prometheus families count each
transport acknowledgement and rejection with its duration, each resume and suspension of the
connector, the positions an ingestor stopped before its transport confirmed them, and whether the
ingestor currently has a transient failure. A node that stops gracefully waits for its ingestors' outstanding confirmations before it
closes them; see [Shutdown And Recovery](shutdown.md#connector-contracts).

## Altering Ingestors

`ALTER` names the connector and entity of the stored ingestor in its head and applies one or more
comma-separated operations in written order:

```plexus,ignore
ALTER <CONNECTOR> <ENTITY> INGESTOR <ingestor>
    SET FROM <complete body of the head's connector and entity>
  | SET CLIENT <client>
  | SET <locator>
  | SET <option>
  | DROP <option>
  | SET MODE <delivery mode>
  | SET START AT EARLIEST | LATEST
  | SET QUIESCE <quiesce body>
  | SET DECODE USING <codec>
  | SET TIMESTAMP NOW
  | SET TIMESTAMP AT <field>
  | SET TIMESTAMP AT metadata.<field>
  | DROP TIMESTAMP
  | SET FILTER WHERE <expression>
  | DROP FILTER WHERE
  | ADD ROUTE TO <relay> <full ingestor route body>
  | DROP ROUTE TO <relay>
  | REPLACE ROUTE TO <relay> <full ingestor route body>
  | ALTER ROUTE TO <relay> <construction change>
  | SET GENERAL ERROR IGNORE
  | SET GENERAL ERROR LOG
  [, ...];

<construction change> ::=
    SET INHERIT <inheritance>
  | DROP INHERIT
  | ADD SET <field> = <expression>
  | DROP SET <field>
  | ADD INVOKE <call>
  | DROP INVOKE <n>
  | SET WHERE <expression>
  | DROP WHERE
```

A head whose connector and entity differ from those the stored ingestor reads is refused, for
example with `the stored ingestor reads HTTP ENDPOINT, so ALTER KAFKA TOPIC INGESTOR cannot alter
it`. An alteration therefore never changes what an ingestor reads; drop the ingestor and create it
again under the other head instead.

`SET FROM` accepts the complete body that follows `FROM` in a `CREATE` statement with the same
head, and only that pair's body, including its required `ON QUIESCE` clause. The other operations
change one part of the stored body and keep the rest of it as it was:

- `SET CLIENT` names the client the ingestor reads through. It exists under every head whose entity
  reads through a client, and the client must configure the head's connector.
- `SET <locator>` writes the locator the entity declares, as its body writes it, such as
  `SET TOPIC 'orders'` or `SET SUBJECT 'orders.created'`. A `REDIS PUBSUB` ingestor takes both
  `SET CHANNEL '<channel>'` and `SET PATTERN '<pattern>'`, and either replaces the subscription,
  because a body holds exactly one.
- `SET <option>` writes one option the entity declares, in a form its body accepts, such as
  `SET OFFSET BY CONSUMER GROUP 'readers' START AT LATEST`, `SET SUBSCRIPTION …`, `SET SESSION
  PERSISTENT`, `SET QOS 1`, `SET QUEUE GROUP 'workers'`, `SET METHOD 'POST'`, `SET HEADERS { … }`,
  `SET MAX BODY SIZE 1MiB`, `SET QUERY '…'` or `SET EVERY 30s`. `DROP <option>` removes an optional
  option, so its default or absence applies again: `DROP HEADERS` or `DROP MAX BODY SIZE`. Dropping
  an option the stored body does not write is refused, for example with `the stored HTTP REQUEST
  ingestor writes no HEADERS, so there is no HEADERS to drop`, and a required option has no
  `DROP`. `INSTANCES` changes only through `SET FROM`.
- `SET MODE` writes one of the delivery modes the entity declares, with its complete policy.
  Completion offers exactly those modes, and any other is refused where it is written.
- `SET START AT` changes where a `KAFKA TOPIC` consumer group or an owned `PULSAR TOPIC`
  subscription starts a partition it holds no position for; see [Kafka](#kafka) and
  [Pulsar](#pulsar).
- `SET QUIESCE` changes only the quiesce mode and its variables, and offers and accepts exactly the
  quiesce modes the entity declares; there is no `DROP QUIESCE` operation.
- `ALTER ROUTE TO <relay>` changes the construction of the one route to `relay` where it stands,
  as [Value Construction and Filters](#value-construction-and-filters) describes it: `SET INHERIT`
  replaces what the route inherits, `ADD SET` appends an assignment after those it writes,
  `DROP SET <field>` removes the one assignment to `field`, `ADD INVOKE` appends a call,
  `DROP INVOKE <n>` removes the `n`th call counted from one in written order, and `SET WHERE` and
  `DROP WHERE` replace and remove the route's filter. A route the ingestor does not write, or
  writes more than once, is refused, and so is a `DROP` of a part the route does not write, such
  as ``the construction writes no assignment to `note` ``.

An option, mode or quiesce mode whose pairing with the rest of the body the entity does not keep
is refused once every operation of the statement has applied, by the registry check every
`CREATE` result also passes. An `MQTT TOPIC` ingestor that moves from `NO ACK` to an acknowledged
mode therefore writes the session, the quality of service and the mode in one statement:
`SET SESSION PERSISTENT, SET QOS 1, SET MODE ACK SEQUENTIAL …`. A declared quiesce mode the
stored delivery mode cannot honor is refused the same way: `SET QUIESCE BUFFER MAX SIZE 1MiB` on
an `HTTP ENDPOINT` ingestor in `MODE ACK MAX 8 ACK TIMEOUT 5s` fails with `HTTP ENDPOINT ingestors
do not support ON QUIESCE BUFFER`.

`SET DECODE USING` exists under every head except `CLIENT PRODUCER`, which decodes nothing.
`ADD ROUTE` appends a route, while `REPLACE ROUTE` preserves the matched route's position.
Duplicate routes to the same relay remain legal, so `DROP ROUTE`, `REPLACE ROUTE` and `ALTER ROUTE`
reject an ambiguous target. An ingestor must retain at least one route. The altered ingestor is
validated against the candidate graph exactly as the `CREATE` that writes it would be.

Every ingestor alteration uses `ENTITY_PAUSE`. Nervix quiesces only the affected ingestor instances
on all live nodes and waits for their already accepted in-flight work to drain before commit. That
work includes route output still held behind a `FLUSH EACH` interval: the hold force-flushes those
branch buffers, so a slow or stopped domain clock cannot keep accepted rows out of the drain.
Schedule application starts the desired ingestor configuration on its assigned nodes; unrelated graph
paths keep flowing. The quiesce mode in effect when the hold begins governs that hold; a mode changed
by the alteration applies to later pauses. A drain or cutover failure leaves the candidate unapplied
and releases the old ingestor. The transient hold also expires automatically, so loss of the
coordinating leader cannot leave an ingestor quiesced indefinitely.

## Quiesce Modes

Every ingestor body ends with a required `ON QUIESCE <body>`, which `DECODE USING` follows under every
transport head. The quiesce *level* says which runtime work pauses for an alteration; an
ingestor's quiesce *mode* says what its external system experiences while that ingestor is paused.
There is no default.

The shared mode vocabulary is:

- `SUSPEND` stops taking payloads. Data waits in the external system where that system retains it;
  the external system's retention, queue, or session limits continue to run. ZeroMQ instead keeps its socket
  open and stops reading, leaving transport flow control and the peer's high-water marks to govern
  the backlog. Polling ingestors skip polls. `SUSPEND` has no variables.
- `BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST` keeps a push connection active and
  retains raw payloads plus their ingest context in a per-instance volatile buffer. `MAX SIZE` must
  be positive. A payload larger than the bound is dropped, and a full buffer applies the declared
  side of the overflow policy. Every discard is counted.
- `DROP` keeps the ingestor connection and protocol maintenance active but discards and counts every
  payload received during the quiesce.
- `REJECT RETRY AFTER <duration>` is available only to `HTTP ENDPOINT` and `WEBSOCKETS ENDPOINT`.
  HTTP requests and new WebSocket upgrades receive 503; HTTP includes `Retry-After`, rounded up to
  whole seconds. Established WebSockets close with code 1013 (Try Again Later).
- Endpoint buffering, for `HTTP ENDPOINT` and `WEBSOCKETS ENDPOINT`, is `BUFFER MAX SIZE <bytes>`
  without `ON OVERFLOW`, available only with `MODE NO ACK`. HTTP requests that fit receive 202 and
  are held; WebSocket data frames that fit are held without a per-frame acknowledgement. A full
  buffer, or a body that cannot fit, is rejected with HTTP 503 or a WebSocket 1013 close; an accepted
  body is never discarded by buffer policy. `MODE ACK` requires `REJECT RETRY AFTER <duration>`.

A read already in progress when an ingestor quiesces ends without losing what it took from
the transport: under `SUSPEND` those payloads still dispatch before the ingestor suspends, and under
`BUFFER` and `DROP` the mode decides on them like any payload received during the quiesce.

An ingestor accepts only the modes it can honor, and completion of its body offers only those.
`SET QUIESCE` parses every body and refuses one the stored ingestor does not honor:

| Entity | Allowed `ON QUIESCE` bodies |
|---|---|
| `KAFKA TOPIC`, `PULSAR TOPIC`, `RABBITMQ QUEUE`, `SQS QUEUE` | `SUSPEND` |
| `MQTT TOPIC` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP`; `SUSPEND` requires `SESSION PERSISTENT` at `QOS 1` or `QOS 2` |
| `NATS SUBJECT`, `REDIS PUBSUB`, `WEBSOCKETS CONNECTION` | `BUFFER ... ON OVERFLOW ...`, `DROP` |
| `ZEROMQ PULL`, `SYSLOG LISTENER`, `NATS STREAM` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP` |
| `HTTP REQUEST`, `PROMETHEUS QUERY` | `SUSPEND`, `BUFFER ... ON OVERFLOW ...` |
| `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT` | `REJECT RETRY AFTER ...`; `BUFFER MAX SIZE ...` only with `MODE NO ACK` |
| `CLIENT PRODUCER` | `SUSPEND` |

For `SYSLOG LISTENER`, suspension retains frames already held by the listener, while UDP arrivals
remain subject to the kernel receive buffer. Resuming the same listener and replacing or stopping
it have different retention outcomes; see [Syslog quiesce](syslog.md#quiesce) before relying on them.

The mode is consulted for resumable model-alteration `ENTITY_PAUSE` holds, `DOMAIN_PAUSE` batches,
and memory-pressure shedding. `STOP` and `DROP INGESTOR` terminate the ingestor session. Unexpected
owner loss also terminates it immediately; volatile quiesce buffers and in-memory ACK state do not
survive, so recovery depends on the external system's retention and redelivery contract.

Node drain, graceful-shutdown drain, placement relocation, and explicit relocation with
`RELOCATE` use a planned ownership handoff.
That hold deliberately ignores `ON QUIESCE`: polling and new endpoint admission stop, an already
admitted payload continues through its routes, and connected-ingestor policy does not buffer or drop
new work on behalf of the move. Nervix waits for the moved ingestor's admitted ACK roots to resolve
before it commits the new owner. Kafka offsets, broker acknowledgements, queue deletes, and the
corresponding downstream attached work therefore complete on the former owner before its ingestor
session stops. The destination starts only after the schedule revision commits.

Only moved ingestors take this hold. Every other ingestor keeps its session open and continues
ingesting, including ingestors on the former and destination cluster nodes. A failed fence or drain
releases the former ingestor and leaves the schedule unchanged.

Graceful shutdown then stops intake the same way on every ingestor of the terminating node,
including endpoint ingestors, which serve on every node, and ingestors whose scheduled owner did not
move. It also ignores `ON QUIESCE`. Already admitted payloads continue through their routes, route
buffers are force-flushed whatever their `FLUSH EACH` cadence, and Nervix waits for their ACK roots
to resolve before the ingestor session stops, so offsets, acknowledgements, and deletes complete for
work that reached its emitters. A root still unresolved when the drain timeout passes is negatively
acknowledged, and the ingestor's redelivery contract applies. A raw quiesce buffer is outside runtime
graph work, so a shutdown discards what it retained instead of replaying it. See
[Planned Ownership Handoffs And Failover](control-plane.md#planned-ownership-handoffs-and-failover)
and [Shutdown And Recovery](shutdown.md#connector-contracts) for the per-ingestor acknowledgement and
redelivery contract at shutdown.

Ordinary model-alteration quiesce still does not wait for downstream ACK chains. Already admitted
route batches continue downstream, and an ingestor item not yet acknowledged, committed, or deleted is
eligible for redelivery on resume. A connected mode's raw quiesce buffer is outside runtime graph
work. Ordinary relay backpressure is also independent of `ON QUIESCE`.

Buffered payloads drain in arrival order per instance before that instance admits new live intake.
There is no total order across instances. Decode, timestamp selection, filters, construction,
routing, and domain pacing use the configuration in effect at delivery, so a codec installed by an
alteration decodes payloads buffered during its hold. `TIMESTAMP NOW` therefore records delivery
time. Bounds are per instance, and an endpoint has an independent buffer on every serving node.

A payload stays in its buffer, counted in the buffered records and bytes and within `MAX SIZE`,
until its messages enter their ingest group, and overflow never discards it while it drains. Its
sender was already answered or its ingestor has moved past it, so when its codec's `ON INGESTION`
transformation finds the node's extension workers full, the payload waits for them rather than
being refused, ahead of work that asks after it, and nothing behind it drains first. A new quiesce
or a shutdown ends that wait and leaves the payload at the front of the buffer. A payload its codec
refuses leaves the buffer as a decode rejection; see
[When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).

Memory pressure never adds buffered bytes. During memory-pressure quiesce, a `BUFFER` push ingestor
drops and counts new payloads, polling ingestors skip polls, and endpoints reject. Payloads already
buffered by an overlapping alteration remain retained. `DROP` continues to discard and `SUSPEND`
continues to hold at the ingestor.

`DESCRIBE INGESTOR` renders `quiesce:`, `quiesce state: none|entity hold|ownership handoff|domain
pause|memory pressure`, `status: quiesced` for every active hold, and the four quiesce counters.
It also renders `nervix_ingestor_transport_redeliveries_total`, the acknowledgements and rejections
the node discarded after a RabbitMQ, SQS or MQTT ingestor reconnected, because the broker redelivers
what the lost connection did not acknowledge; see
[Connector Crates And The Connector Contract](connector-contract.md). It renders
`nervix_ingestor_decode_rejections_total` and `last decode rejection:` as well; see
[When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload). It renders
`nervix_ingestor_oversized_payloads_total`, the payloads the node's ingestor dropped because they
exceed their client's message size bound, such as a Syslog UDP datagram larger than
`max_message_size`; see [Oversized payloads](connector-contract.md#oversized-payloads).
`SHOW CREATE` round-trips the full clause.

## When The Codec Refuses A Payload

A payload its codec refuses, because it is not in the wire format the codec reads or does not fit
the codec's schema, would be refused again however often its ingestor presented it. The ingestor
therefore consumes it, and a decode rejection takes its place: a message error that the ingestor's
node-wide `ON GENERAL ERROR` policy reports.

- `ON GENERAL ERROR LOG` reports the rejection to every attached session as a server error and logs
  its fields at `warn`, such as `ingestor 'orders_source' general error in domain 'shop': decode
  rejection 0199f1c2-… at 2026-10-17T12:00:00Z: codec 'order_codec' refused the payload from Kafka
  topic 'orders' partition 0 offset 7: codec 'order_codec' failed to parse field 'total': …`.
- `ON GENERAL ERROR IGNORE` reports nothing.

Neither policy makes the ingestor present the payload again, and the other payloads of its batch
continue through the ingestor's routes. An acknowledged delivery mode settles the refused payload's
position on its transport together with the rest of the batch, so a Kafka consumer group commits
past it and a RabbitMQ queue no longer holds it. In `MODE NO ACK` the payload is dropped with its
rejection. A JAQ-backed codec that refuses one message an unfolding payload produced refuses the
whole payload, and so does an unfolding that panics.

A decode rejection carries a stable UUIDv7 reference, the code `validation`, the operation `decode`,
the codec, the fields the codec named at fault, the instant the ingestor took the payload in, and the
codec's diagnostic as its message. When the ingestor's metadata names where on its transport the
payload was read, the rejection names it too: a Kafka record's topic, partition and offset, or the
peer a syslog frame came from. An ingestor whose metadata holds only headers names no position, because
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

- An acknowledged delivery mode counts the refused raw payloads in
  `nervix_ingestor_unfolding_refused_total`, rejects the batch, and delivers it again on its
  `RETRY POLICY`.
- An [endpoint ingestor](#http-endpoint-and-websockets-endpoint) answers an HTTP body 503 without
  `Retry-After` and closes an established WebSocket with 1013, counts the refused request payload,
  and lets the sender send it again.
- A payload that `BUFFER` retained waits for a place when its buffer drains; see
  [Quiesce Modes](#quiesce-modes).
- A paced ingestor has already moved past its poll, so the poll waits for a place. Waiting delays the
  next poll, and the domain cadence coalesces the occurrences it missed into one poll at the newest
  due instant.
- An ingestor read in `MODE NO ACK` cannot present a payload again either. The payload waits for a
  place when holding the ingestor's loop keeps its transport connected with bounded memory. Otherwise
  it is refused: the ingestor reports the refusal as an error and counts the payload in
  `nervix_ingestor_unfolding_refused_total`.

| Entity | A payload the extension workers cannot take now | Why |
| --- | --- | --- |
| `HTTP REQUEST`, `PROMETHEUS QUERY` | Waits | Holding the loop only delays the next poll. |
| `NATS SUBJECT` | Waits | The client keeps its connection and reads the subscription while the loop waits, holding up to 65,536 messages before it drops the newest as a slow consumer. |
| `ZEROMQ PULL` | Waits | Unread messages stay in the socket's kernel buffers, and then the pushing peers wait. Nothing is dropped. |
| `SYSLOG LISTENER` | Waits | The listener stays bound. The kernel drops UDP datagrams beyond the socket's receive buffer, and TCP and TLS senders wait once the listener's 64-frame queue is full. |
| `MQTT TOPIC` | Refused and counted | The connection keeps sending its keep-alive while the loop is held, but reads only 64 publishes ahead of it and then stops reading, and its keep-alive with it, so the broker closes the connection once one and a half keep-alive intervals pass without a packet from it, and what is published until the ingestor reconnects is lost. |
| `REDIS PUBSUB` | Refused and counted | The subscription reader holds at most `subscription_buffer_messages` messages. The host keeps polling it so its same-connection idle probes can continue instead of waiting for extension-worker admission. |
| `WEBSOCKETS CONNECTION` | Refused and counted | The client answers the server's pings only while the loop reads, so a server that pings would close the connection and lose what it sent until the ingestor reconnects. |
| `KAFKA TOPIC` in `MODE NO ACK` | Refused and counted | A consumer-group member that stops polling for `max.poll.interval.ms` leaves its group, and an instance reading domain offsets would keep its partitions assigned through an ownership handoff. |
| `PULSAR TOPIC` in `MODE NO ACK` | Refused and counted | A consumer whose receive queue fills cannot complete its close, so a later suspension or stop would wait on it. The refused message stays unacknowledged until the consumer reconnects, when the broker delivers it again. |

A wait never delays a stop or a new quiesce. Stopping the ingestor ends the wait and drops the
payload with the rest of the ingestor's in-flight input. A new quiesce decides on the payload again,
as on input that arrives while quiesced: `BUFFER` retains it, `DROP` drops it and counts it in
`nervix_ingestor_quiesce_dropped_total`, and a suspension, under which a payload the ingestor already
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

```plexus
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
- public Models store structured expressions; the runtime never reparses Plexus
- each ingestor entity declares the typed transport metadata its messages expose as
  `metadata.<field>`, listed under Metadata in its reference below: `KAFKA TOPIC` declares
  `metadata.topic`, `metadata.partition`, `metadata.offset`, `metadata.key`, `metadata.timestamp`,
  `metadata.timestamp_type`, and `metadata.tombstone`, `MQTT TOPIC` declares the delivered packet's
  topic, flags, quality of service and MQTT 5 properties, `RABBITMQ QUEUE` declares the delivery's
  envelope and its AMQP basic properties, `SQS QUEUE` declares the message identity and its system
  attributes, `PULSAR TOPIC` declares the [message identity, times, keys and delivery fields](#pulsar),
  `SYSLOG LISTENER` declares `metadata.peer_addr`, `metadata.transport`, `metadata.tls_peer`, and
  `metadata.remote_ip`,
  `HTTP REQUEST` declares `metadata.status` and `metadata.polled_at`,
  `HTTP ENDPOINT` declares `metadata.method`, `metadata.path`, `metadata.query`,
  `metadata.remote_addr`, `metadata.host`, and `metadata.remote_ip`, `PROMETHEUS QUERY` declares `metadata.query` and
  `metadata.evaluated_at`, `WEBSOCKETS CONNECTION` declares `metadata.frame_type`,
  `WEBSOCKETS ENDPOINT` declares `metadata.frame_type`, `metadata.remote_addr`, and
  `metadata.path` and `metadata.remote_ip`, and `REDIS PUBSUB` declares `metadata.channel` and `metadata.pattern`; every
  field is optional and none is sensitive
- a route expression may read any declared field; `CREATE` and `ALTER` refuse a route that reads a
  field its entity does not declare, naming the ingestor, the route's relay and the fields the
  entity does declare. `FILTER WHERE` reads the same declared metadata fields and, when the
  entity offers headers, its headers
- an ingestor builds a metadata column only for the fields its arrival filter, route expressions or
  `TIMESTAMP AT metadata.<field>` read; an ingestor that reads none visits no metadata values and
  builds no metadata columns
- a message whose connector hands over a value its field's declared type cannot hold, such as a
  `RABBITMQ QUEUE` AMQP timestamp after 2262-04-11 or a `KAFKA TOPIC` record timestamp before
  1677-09-21 or after 2262-04-11, is refused once `FILTER WHERE` keeps it, wherever a route or
  `TIMESTAMP AT` reads that field: every route's `ON MESSAGE ERROR` policy receives a message error
  with code `validation` and operation `admit` that names `metadata.<field>` and describes the
  value. The field never reads as null for it, and a field no program reads refuses nothing. An
  `SQS QUEUE` system attribute out of range fails the read instead, as its reference states
- supported ingestors expose transport headers through `read_header(name)` and `read_headers(name)`;
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

`DATETIME` is the internal logical timestamp type. JSON or AVRO `string` and CBOR `text` wire values require `ENCODE <field> AS RFC3339` on the codec before they can decode into the named `DATETIME` field.

### Payload Facts

Every ingestor entity declares `payload.bytes: U64`, required and not sensitive. It counts the
original bytes supplied by its transport: a Kafka record's value without its key or headers; a
Pulsar, MQTT, NATS, RabbitMQ, Redis or SQS message body; the first frame of a ZeroMQ message; a Syslog frame; a WebSocket data
message; an HTTP request body; or an HTTP or Prometheus poll's response body. Decompressed HTTP
bodies count after decompression. A Prometheus poll's samples share the size of its original
response body. A `CLIENT PRODUCER` counts the exact submitted Arrow IPC batch body and alone
also declares `payload.rows: U64`, the original batch's row count.

These facts are available in `FILTER WHERE`, route construction and route `WHERE`. A payload that
decodes into several rows repeats its byte count on every row; filtering and construction do not
change the count, and client rows retain their whole batch's row count. The host builds only the
fact columns a program reads, shares them across selected views and routes, and never copies a
body to measure it. For example:

```plexus,ignore
FILTER WHERE payload.bytes > (1024 AS U64)
TO measured INHERIT ALL SET size = payload.bytes
  UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
```

`CREATE` and `ALTER` refuse an undeclared payload field, naming the entity and its declared fields.
In particular, `payload.rows` is available only on `CLIENT PRODUCER`.

`HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT` and `SYSLOG LISTENER` also expose
`metadata.remote_ip: STRING OPTIONAL`. It is the actual peer's IP without its port: `127.0.0.1`
for an IPv4 loopback peer and `::1` for an IPv6 loopback peer. Their existing address fields retain
the port. These peer facts describe the accepted connection or datagram and do not read forwarded
HTTP headers.

### Header Context

Some ingestors receive transport metadata alongside the decoded payload body. They expose it through two functions:

- `read_header(name)` returns the first value as an optional `STRING`, or `NULL` when the header is absent
- `read_headers(name)` returns every value in transport order as required `LIST<STRING>`, or an
  empty list when the header is absent

The `name` argument can be any expression that returns `STRING`. Both functions are available in top-level `FILTER WHERE` and in per-route `SET` / `WHERE` expressions for these ingestors:

- `HTTP ENDPOINT` and `HTTP REQUEST` expose HTTP headers with UTF-8 values; an `HTTP ENDPOINT`
  drops a request header value that is not UTF-8 and counts it in
  `nervix_ingestor_header_values_dropped_total`
- `WEBSOCKETS ENDPOINT` exposes UTF-8 headers from the opening HTTP upgrade request, dropping and
  counting a value that is not UTF-8 the same way
- `WEBSOCKETS CONNECTION` exposes printable ASCII headers from its opening HTTP upgrade response
- `KAFKA TOPIC` exposes Kafka record headers
- `MQTT TOPIC` exposes MQTT 5 user properties
- `NATS SUBJECT` exposes message headers
- `NATS STREAM` exposes the headers each message was published with
- `PULSAR TOPIC` exposes message properties
- `RABBITMQ QUEUE` exposes AMQP message headers, converting typed AMQP values to strings
- `SQS QUEUE` exposes message attributes as text: a `String` or `Number` attribute's value as SQS
  carries it, and a `Binary` attribute's bytes read as UTF-8, each invalid sequence replaced by
  U+FFFD

Each of these ingestors declares a [header model](./connector-contract.md#header-models), which
`DESCRIBE INGESTOR` shows as its `headers:` line. The model decides what the two functions find:

<!-- connector-matrix:ingest-headers:start -->
| Ingestor | Header model | What `read_header` and `read_headers` find |
| --- | --- | --- |
| `HTTP REQUEST` | `repeats_by_name`, lowercase names | every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| `HTTP ENDPOINT` | `repeats_by_name`, lowercase names | every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| `KAFKA TOPIC` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `PULSAR TOPIC` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `MQTT TOPIC` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `NATS SUBJECT` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `NATS STREAM` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `RABBITMQ QUEUE` | `sorted_by_name`, exact names | one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| `SQS QUEUE` | `last_wins`, exact names | one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |
| `WEBSOCKETS CONNECTION` | `repeats_by_name`, lowercase names | every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| `WEBSOCKETS ENDPOINT` | `repeats_by_name`, lowercase names | every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |

`CLIENT PRODUCER`, `REDIS PUBSUB`, `PROMETHEUS QUERY`, `ZEROMQ PULL`, `SYSLOG LISTENER` carry no transport headers and refuse `read_header` and `read_headers`.
<!-- connector-matrix:ingest-headers:end -->

`read_headers` returns a name's values in the order its model keeps them, at most one of them where
the model keeps one value per name. An ingestor whose model records names in lowercase is read in
lowercase: `read_header("x-tenant")` finds the field an HTTP peer sent as `X-Tenant`, while
`read_header("X-Tenant")` finds nothing. Every other ingestor matches a name exactly, letter case
included.

The same captured ingestor envelope remains available while a supported ingestor constructs an
`ON MESSAGE ERROR SEND TO` record, so its error-route `SET` may also call `read_header` and
`read_headers`.

Header values are non-sensitive strings and do not propagate through relays unless explicitly
assigned to schema-backed fields:

```plexus,ignore
SET route = read_header(lower(input.route_header))
WHERE read_header("tenant") = output.tenant
```

Leader-side validation rejects these functions for an ingestor that carries no transport headers, the
ingestors the table above lists after it, with a message that names the ingestor's phrase, such as
`REDIS PUBSUB ingestors do not support read_header or read_headers`.

## TLS Client Configuration

For outbound ingestor clients, TLS is configured on the `CLIENT`, not on the `INGESTOR`.

General pattern:

```plexus,ignore
CREATE [IF NOT EXISTS] <kind> CLIENT <name>
  MOUNT <tls_resource> VERSION <n>|LATEST
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific schemes and keys:

- `KAFKA`: librdkafka properties with the [reserved keys and consumer settings](#kafka) described below. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and if needed `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `HTTP`: use an `https://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `PROMETHEUS`: use an `https://...` `addr`. Nervix honors `tls_ca_file`, `tls_cert_file` and `tls_key_file`, and the client requires `timeout_ms`; see [Prometheus](#prometheus) for every key it declares.
- `WEBSOCKETS`: use a `wss://...` endpoint. Nervix trusts `tls_ca_file` beside the bundled WebPKI
  roots and the platform's native roots, and presents `tls_cert_file` with `tls_key_file` as its
  client certificate. A plaintext `ws://...` endpoint reads no TLS file, so `CREATE WEBSOCKETS
  CLIENT` refuses all three with it; see [`WEBSOCKETS CONNECTION`](#websockets-connection).
- `MQTT`: use `mqtts://...` in `addr`. Nervix requires `tls_ca_file` for server trust and also supports `tls_cert_file` plus `tls_key_file` for mTLS. A plaintext `mqtt://` address reads no TLS file, so `CREATE MQTT CLIENT` refuses all three with it.
- `NATS`: use `tls://...` in `addr`. Nervix trusts the platform's native roots, or only `tls_ca_file` when it is set, and presents `tls_cert_file` with `tls_key_file` as its client certificate; a `nats://` address uses TLS only when the server requires it. See [NATS](#nats).
- `PULSAR`: use `pulsar+ssl://...` in `addr`. Nervix honors `tls_ca_file`, presents `tls_cert_file` with `tls_key_file` as its client certificate, and honors optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`, all of which `CREATE PULSAR CLIENT` refuses with a plaintext `pulsar://...` address; see [Pulsar](#pulsar).
- `RABBITMQ`: use `amqps://...` in `addr`. Nervix honors `tls_ca_file` and presents `tls_cert_file` with `tls_key_file` as its client certificate; see [RabbitMQ](#rabbitmq).
- `REDIS`: use `rediss://...` in `addr`. Nervix trusts the host's trust store, or only
  `tls_ca_file` when it is set, and presents `tls_cert_file` with `tls_key_file` as its client
  certificate. A plaintext `redis://` address reads none of the three files; see
  [Redis Pub/Sub](#redis-pubsub).
- `SQS`: use an `https://...` `endpoint`. Nervix trusts the platform's native roots, or only
  `tls_ca_file` when it is set, so a CA file written for an SQS-compatible local or test endpoint
  does not also trust AWS's certificates. `CREATE SQS CLIENT` refuses a file that holds no
  certificate it could trust. An SQS connection presents no client certificate, so
  `CREATE SQS CLIENT` refuses `tls_cert_file` and `tls_key_file`; see [SQS](#sqs).
- `SYSLOG`: select `'protocol' = 'tls'`. An ingestor requires `tls_cert_file` and
  `tls_key_file`; optional `tls_client_ca_file` enables required client-certificate verification.

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
A `WEBSOCKETS CONNECTION` resolves the host of its client's `endpoint` the same way for every
connection and resume, and verifies a `wss` server certificate against that host; see
[`WEBSOCKETS CONNECTION`](#websockets-connection).

Example Kafka TLS client:

```plexus
CREATE IF NOT EXISTS KAFKA CLIENT kafka_tls
  MOUNT dev_tls VERSION 1
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

Example HTTP TLS client:

```plexus
CREATE IF NOT EXISTS HTTP CLIENT http_tls
  MOUNT dev_tls VERSION 1
  CONFIG {
    'endpoint' = 'https://127.0.0.1:18443/http/notifications',
    'timeout_ms' = 5000,
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

## Client Deadlines

A client's `timeout_ms`, `connect_timeout_ms` and `idle_timeout_ms` mean the same thing for every
connector that reads them. `timeout_ms` bounds the whole of one request or operation: name
resolution, connection setup, TLS and the answer. `connect_timeout_ms` bounds establishing a
connection: name resolution, the address attempts, TLS and the protocol's own handshake.
`idle_timeout_ms` bounds how long a connection may carry nothing while its connector waits to read
from it. Each value is a positive whole number of milliseconds.

`HTTP` and `PROMETHEUS` clients read `timeout_ms`. `RABBITMQ`, `REDIS` and `WEBSOCKETS` clients read
`connect_timeout_ms`, and allow 30 seconds when it is left out. A `WEBSOCKETS` client also reads
`idle_timeout_ms`: where it is left out, three `ping_interval_ms` intervals bound the silence, or 30
seconds without configured pings; see [`WEBSOCKETS CONNECTION`](#websockets-connection). A `ZEROMQ PULL` ingestor reads
none of these keys, so nothing bounds its connection; see [ZeroMQ](#zeromq). A `RABBITMQ` client's
deadline covers resolving the broker, the address attempts, TLS, the AMQP handshake, opening the
ingestor's channel, setting its prefetch window and starting its consumer. A request or connection
that runs out of its deadline becomes the ingestor's transient status, and the ingestor retries on
its policy; a connection that runs out of `connect_timeout_ms` is reported naming the key and its
bound.

A `SYSLOG LISTENER` serves many senders, so a sender that stalls is not the ingestor's status. A TLS
listener's client bounds the TLS handshake of each connection it accepts by `connect_timeout_ms`, 30
seconds when it is left out, and a TCP or TLS listener's client may set `idle_timeout_ms`. A
connection that runs out of either is closed, and the listener goes on reading the others; see
[Syslog](syslog.md). [Connector Capabilities](connector-capabilities.md) lists each entity's network
waits and what bounds each one, including the waits nothing bounds yet.

For a `KAFKA` client, `socket.timeout.ms` also bounds each admitted ingestor control job, including
executor queueing, metadata and partition inspection, offset lookups, watermarks and seeks. Its
native default is 60 seconds, and librdkafka accepts 10 through 300000 milliseconds. The job's
native waits share its remaining budget rather than restarting the timeout per partition.
Exhausted executor admission and an expired budget become transient ingestor failures and follow
its retry policy. Kafka control jobs have their own bounded workers, so a slow broker cannot take
filesystem or consensus worker permits. This does not bound a consumer-group commit's native
retry lifetime: the caller waits at most 30 seconds and retains that one commit until its exact
answer arrives before submitting another. Native subscribe, assign and unsubscribe cannot be
cancelled; their caller's budget limits waiting while an already-running call keeps its worker.

Redis Pub/Sub requires `idle_timeout_ms` in its ingestor client's `CONFIG`. It bounds silence on an
established subscription, including its same-connection liveness probe; time waiting for room in
the bounded read-ahead handoff is outside that active read. A failed probe reports the key and its
bound as a transient status and the host reconnects on its policy.

## Client Ingestors

The [Client Ingestors And Emitters](./client-io-architecture.md) architecture chapter follows a
complete native graph through admission, application ACK, credit, forwarding and recovery.

A client ingestor, declared under the `CLIENT PRODUCER` head, admits typed batches that applications
submit through a Nervix session instead of reading an external system. There is no
external connector client, codec, `DECODE USING`, transport header, or `NO ACK` mode: a submitted batch
already carries the ingestor's input schema, and every batch is acknowledged.

```plexus
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
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.user: STRING OPTIONAL |
| Payload facts | payload.bytes: U64<br>payload.rows: U64 |
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
`input` read the submitted row. `metadata.user` is the opening session's authenticated user name,
shared by every row of every batch that producer submits. It is a `STRING` field with the metadata
namespace's optional type, and an authenticated producer always supplies it. The name is canonical
lowercase, including when the application signed in with uppercase letters. A submitted field
cannot replace this identity. `FILTER WHERE metadata.user = 'alice'` selects Alice's batches;
`SET who = metadata.user` copies the identity into an optional `STRING` output field.

Client batches carry no transport headers and refuse `read_header` and `read_headers`.
The ingestor executes on its one scheduled cluster node. A producer is opened on a session that
any live node may serve, and that
node forwards the producer's batches to the ingestor's node; a console session, like every console
session, is served only by the leader. Forwarding preserves each producer's authenticated identity
independently, including when producers of different users share the same node-to-node link.

`ALTER CLIENT PRODUCER INGESTOR ... SET FROM SCHEMA ...` replaces the complete ingestor body, and
`SET QUIESCE SUSPEND` restates the only quiesce mode. `SHOW CREATE INGESTOR` renders the
`CLIENT PRODUCER` head and the whole `FROM SCHEMA` body.

### Submitted Batches

A batch is one canonical Arrow IPC stream: the schema message, exactly one record batch, and the
end-of-stream marker. It is uncompressed and has no dictionary or extension encodings, and its
Arrow schema is exactly the input schema in Nervix's Arrow representation of each type, with no
field metadata. A batch carries at most the rows in its producer grant (65,536 by default) and at most the bytes one submission may carry,
which is the smaller of the producer's granted bytes and what one session frame holds. The node
that executes the ingestor validates the whole batch, its structure, bounds, and every column,
before any row is admitted; a batch that fails is refused whole and has no effect on the graph.
Nothing is coerced, cast, or partially admitted.

### Outcomes

One batch is one ingestor acknowledgement unit. It has exactly one terminal outcome:

| Outcome | Meaning | Cause |
| --- | --- | --- |
| not admitted | No row entered the graph. | `invalid batch: <defect>`, `suspended`, `busy`, `draining`, `producer ended`, `credit exceeded` |
| completed | Every route and acknowledging emitter its rows reached confirmed them under the graph's rules. | — |
| processing failed | The batch was admitted and its acknowledgement failed. Some of its effects may have happened. | `ack_timeout`, `rejected` |
| unknown outcome | The batch may have been admitted and processed, but no terminal result can be established. | `interrupted`, `owner_lost`; a client whose session ended adds `session_lost` |

Transport receipt is never an outcome. A batch completes under the normal acknowledgement rules: a
row a filter drops is resolved; a detached boundary resolves where it detaches; and an
acknowledging emitter resolves its rows at its own success boundary, such as the complete response
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
| Outstanding bytes per session | Committed producer cap, 32 MiB by default |
| Outstanding bytes per node, including batches retained for forwarded producers | Resolved producer node credit; see [Execution Capabilities](node-settings.md#execution-capabilities) |
| Batches one producer may ask to have outstanding | 1,024 |
| Rows per batch | Grant's committed client-row cap, 65,536 by default |

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

`DESCRIBE INGESTOR` renders `from: CLIENT PRODUCER SCHEMA order_in`, then `options: none`, the
complete acknowledgement window on `mode:`, and `quiesce: SUSPEND`. Its runtime rows include
`schema:`, `owner:`, `admission:`, `producers:`, `forwarded producers:`, `outstanding batches:`,
`outstanding bytes:`, and `admitted batches:`. The
node that executes the ingestor also exports the same counts and every answered batch as metrics;
see [Metrics And Observability](metrics-and-observability.md#client-ingestors). An `HTTP ENDPOINT`
or `WEBSOCKETS ENDPOINT` ingestor answers an acknowledged request in this vocabulary too, and counts
every request and frame it answers in `nervix_ingestor_endpoint_requests_total` by outcome; see
[`HTTP ENDPOINT` And `WEBSOCKETS ENDPOINT`](#http-endpoint-and-websockets-endpoint).

## External Names

An ingestor names the external entity it reads with a string literal: a topic, consumer group,
subscription, queue, channel, channel pattern, subject, queue group, JetStream stream or consumer,
or MQTT topic filter. Nervix keeps the name exactly as written, letter case included, and sends it
to the external system as written. Each entity validates its names when `CREATE` or `ALTER`
applies, by the external system's own rule where it has one:

| Entity | Name | Rule |
| --- | --- | --- |
| `KAFKA TOPIC` | `TOPIC` | 1 to 249 ASCII letters, digits, `.`, `_`, and `-`, and neither `.` nor `..`: the broker's own rule |
| `KAFKA TOPIC` | `CONSUMER GROUP` | 1 to 255 bytes without control characters |
| `PULSAR TOPIC` | `TOPIC` | A local name without `/`, which the client places in its `namespace` (`public/default` unless the client configures one), or a fully qualified `persistent://tenant/namespace/topic` or `non-persistent://tenant/namespace/topic`. At most 1,024 bytes, without whitespace, control characters, or an empty level |
| `PULSAR TOPIC` | `SUBSCRIPTION` | 1 to 255 bytes without control characters |
| `RABBITMQ QUEUE` | `QUEUE` | 1 to 255 bytes: the AMQP 0-9-1 limit |
| `SQS QUEUE` | `QUEUE` | 1 to 80 ASCII letters, digits, `-`, and `_`, and a FIFO queue's name ends in `.fifo`: SQS's own rule |
| `REDIS PUBSUB` | `CHANNEL` | 1 to 1,024 bytes of any text; a `*` is the character itself |
| `REDIS PUBSUB` | `PATTERN` | 1 to 1,024 bytes of any text, which Redis matches against channel names as a glob; see [Redis Pub/Sub](#redis-pubsub) |
| `NATS SUBJECT` | `SUBJECT` | At most 1,024 UTF-8 bytes of `.`-separated tokens, none empty, without whitespace or control characters; `*` stands for one token and `>` for the rest, each only as a whole token, and `>` only last. The byte bound and embedded-wildcard refusal are Nervix restrictions, not NATS server limits |
| `NATS SUBJECT` | `QUEUE GROUP` | 1 to 255 bytes without whitespace or control characters |
| `NATS STREAM` | `STREAM` | 1 to 255 bytes without whitespace, control characters, `.`, `*`, `>`, `/` or `\`: the JetStream server's rule, stricter only in refusing every whitespace and control character |
| `NATS STREAM` | `CONSUMER` | The rule of a stream name |
| `MQTT TOPIC` | `TOPIC` | A topic filter of 1 to 65,535 bytes without NUL; `+` stands for one level and `#` for the rest, each only as a whole level, and `#` only last. Levels are joined by `/` and may be empty. The first level is not `$share`, since Nervix forms the shared subscription itself |

A reference to a Nervix model, such as a client, endpoint, schema, codec, or relay, stays a bare
identifier.

## Supported Ingestor Types

Each subsection shows a head and the body that follows its `FROM`; `SET FROM` takes the same body
under the same head. `DECODE USING <codec>`, the timestamp, the filter, the routes, and
`ON GENERAL ERROR` follow the body as in every ingestor statement.

| Head | Reads | Section |
| --- | --- | --- |
| `HTTP REQUEST` | Responses to the requests an `HTTP CLIENT` sends on a cadence | [`HTTP REQUEST`](#http-request) |
| `HTTP ENDPOINT` | Requests to an `HTTP ENDPOINT` | [Endpoints](#http-endpoint-and-websockets-endpoint) |
| `WEBSOCKETS ENDPOINT` | Frames on connections to a `WEBSOCKETS ENDPOINT` | [Endpoints](#http-endpoint-and-websockets-endpoint) |
| `WEBSOCKETS CONNECTION` | Frames on the outbound connection of a `WEBSOCKETS CLIENT` | [`WEBSOCKETS CONNECTION`](#websockets-connection) |
| `KAFKA TOPIC` | A topic, through a `KAFKA CLIENT` | [Kafka](#kafka) |
| `PULSAR TOPIC` | A topic, through a `PULSAR CLIENT` | [Pulsar](#pulsar) |
| `RABBITMQ QUEUE` | A queue, through a `RABBITMQ CLIENT` | [RabbitMQ](#rabbitmq) |
| `REDIS PUBSUB` | A channel, or every channel a pattern matches, through a `REDIS CLIENT` | [Redis Pub/Sub](#redis-pubsub) |
| `MQTT TOPIC` | A topic filter, through an `MQTT CLIENT` | [MQTT](#mqtt) |
| `NATS SUBJECT` | A subject, through a `NATS CLIENT` | [NATS](#nats) |
| `NATS STREAM` | A durable pull consumer of a JetStream stream, through a `NATS CLIENT` | [NATS](#nats) |
| `ZEROMQ PULL` | The PULL socket of a `ZEROMQ CLIENT` | [ZeroMQ](#zeromq) |
| `SYSLOG LISTENER` | The listener a `SYSLOG CLIENT` declares | [Syslog](#syslog) |
| `SQS QUEUE` | A queue, through an `SQS CLIENT` | [SQS](#sqs) |
| `PROMETHEUS QUERY` | A PromQL query, through a `PROMETHEUS CLIENT` | [Prometheus](#prometheus) |
| `CLIENT PRODUCER` | Batches applications submit through their sessions | [Client Ingestors](#client-ingestors) |

### `HTTP REQUEST`

<!-- connector-matrix:ingest-http-request:start -->
```plexus,ignore
CREATE HTTP REQUEST INGESTOR <name>
  FROM <client>
  (METHOD 'GET')
  [HEADERS { 'accept' = 'application/json' }]
  [MAX BODY SIZE 1MiB]
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
| Locator and validation rule | Not offered: The client's endpoint URL names the polled resource, its path and query included, so this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | `METHOD 'GET'` (string literal); required<br>`HEADERS { 'accept' = 'application/json' }` (named string literals or client secrets); optional; omitted when unset<br>`MAX BODY SIZE 1MiB` (byte size); optional; default 16MiB<br>`EVERY 1s` (duration); required |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |
| Metadata | metadata.status: U16 OPTIONAL<br>metadata.polled_at: DATETIME OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the response body: set by MAX BODY SIZE; when exceeded, fails the poll; the next poll reads again |
| Network waits | a request and the redirects it follows: name resolution, connection, TLS and each response: `timeout_ms`; required |
| Driver limits | HTTP/1.1 response heads above 100 fields or 417,792 bytes, and HTTP/2 header lists of 16,384 bytes or more as HTTP/2 counts them, are refused.<br>Redirects and proxies require the client's explicit opt-in. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [HTTP REQUEST capability and mirror reference](connector-capabilities.md#http-request-ingest).
<!-- connector-matrix:ingest-http-request:end -->

An `HTTP REQUEST` ingestor sends one request through an `HTTP CLIENT` on a cadence of domain time
and delivers the body of each successful response as one payload. One scheduled node runs it. Every
poll asks the endpoint again, so `NO ACK` is its only mode: nothing acknowledges a response, and an
instant whose poll failed or was skipped delivers nothing.

```plexus
CREATE SCHEMA order_status ( order_id I64, state STRING );
CREATE SCHEMA polled_order_status (
  order_id I64,
  state STRING,
  http_status U16 OPTIONAL,
  polled_at DATETIME OPTIONAL,
  request_id STRING OPTIONAL
);
CREATE WIRE JSON SCHEMA order_status_wire MODE STRICT ( order_id integer, state string );
CREATE WIRE JSON CODEC order_status_codec FROM WIRE SCHEMA order_status_wire TO SCHEMA order_status;
CREATE RELAY order_statuses SCHEMA polled_order_status UNBRANCHED;

CREATE HTTP CLIENT orders_api CONFIG {
  'endpoint' = 'https://api.example.com/orders/latest',
  'timeout_ms' = 5000,
  'secret.api_token' = 'Bearer example-token'
};

CREATE HTTP REQUEST INGESTOR order_status_poll
  FROM orders_api
  METHOD 'GET'
  HEADERS { 'accept' = 'application/json', 'authorization' = CLIENT SECRET 'secret.api_token' }
  MAX BODY SIZE 1MiB
  EVERY 30s
  MODE NO ACK
  ON QUIESCE SUSPEND
  DECODE USING order_status_codec
  TIMESTAMP AT metadata.polled_at
  TO order_statuses INHERIT ALL
    SET http_status = metadata.status,
        polled_at = metadata.polled_at,
        request_id = read_header('x-request-id')
  UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

#### HTTP Polling Requests

Each poll sends one request, which the statement's options and its client describe:

- `METHOD '<method>'` is required and sent with the spelling it is written in. It is an HTTP token
  of 1 to 64 ASCII bytes other than `CONNECT` and `TRACE` in any letter case, and `CREATE` and
  `ALTER` refuse any other method, naming the ingestor. A poll sends no body and no
  `content-length` field, whatever its method, so an endpoint that requires a length on a `POST`
  can refuse the poll.
- The request goes to the client's `endpoint` as written, its path and query included, so one client
  names one resource; see [`HTTP CLIENT`](#http-client).
- `HEADERS { '<name>' = <value>, … }` is optional and writes request fields. A name is an HTTP field
  name, compared without ASCII case; `CREATE` and `ALTER` refuse a name written twice and the names
  the transport owns: `host`, `content-length`, `transfer-encoding`, `connection`, `keep-alive`,
  `proxy-connection`, `te`, `trailer`, `upgrade` and `expect`. A value is a string literal or
  `CLIENT SECRET '<key>'`, and what it sends is UTF-8 text with no control character but a tab and
  no space or tab at either end; an empty value is sent empty. A statement writes at most 128 fields
  and 32 KiB of names and values together, the bounds an
  [HTTP emitter](emitters.md#http-request-configuration) holds its application headers to. A
  refusal names the ingestor and the header and never quotes a value, such as
  `ingestor 'order_status_poll' in domain 'shop' cannot use its HEADERS: header 'X-Event' is set more than once`.
- `MAX BODY SIZE <bytes>` bounds the response body; see
  [HTTP Polling Response Bound](#http-polling-response-bound).
- `EVERY <duration>` is required and sets the cadence; see
  [HTTP Polling Cadence](#http-polling-cadence).

Beside the fields `HEADERS` writes, a request carries the `host` field, or the HTTP/2 authority,
`accept: */*` unless `HEADERS` writes `accept`, and `accept-encoding: gzip,deflate,br` unless
`HEADERS` writes `accept-encoding`. It writes no `user-agent`.

A literal is part of the statement, so `SHOW CREATE` and `DESCRIBE INGESTOR` print it, and a
credential does not belong in one. `CLIENT SECRET` names a key of the ingestor's client and sends
that key's value, as `authorization` does in the example above. The operator names each
`secret.<name>` key of an `HTTP` client. Inspection prints the reference: `SHOW CREATE`,
`DESCRIBE INGESTOR` and the console show `CLIENT SECRET 'secret.api_token'`, and `SHOW CREATE CLIENT`
and `DESCRIBE CLIENT` mask the key as `********`. `CREATE` and `ALTER` refuse a reference to a key
the client does not hold, to a key that is not a secret, and to a value that cannot be a header,
naming the header and the key and never quoting the value. A value that keeps a `{{ … }}` template
is rendered and checked where the ingestor opens instead, and one that cannot be a header fails the
open. A poll sends the value only to the client's origin, because it follows a redirect nowhere
else. Complete serialization keeps the value, as it keeps every client secret, so a backup archive
holds it; see [Backup And Restore](backup-and-restore.md).

`ALTER HTTP REQUEST INGESTOR` changes the client, method, headers, response bound or cadence
alone with `SET CLIENT`, `SET METHOD`, `SET|DROP HEADERS`, `SET|DROP MAX BODY SIZE` or `SET EVERY`,
or all of them together with `SET FROM <complete body>`, and the next poll sends the new request.

#### HTTP Poll Responses

The final response of a poll decides what it delivers:

- A `2xx` status other than `204 No Content` delivers the response body as one payload, which the
  ingestor's codec decodes. An empty body is an empty payload, which the codec decodes or refuses
  like any other; see [When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).
- `204 No Content` delivers nothing. The poll still succeeded.
- Every other status fails the poll, a redirect the client does not follow included; see
  [When An HTTP Poll Fails](#when-an-http-poll-fails).

A route reads what answered its payload. `read_header` and `read_headers` read the response's
header fields under their lowercase names, every value of a repeated name together and in the
order the response carried them. A value is read as UTF-8 text, non-ASCII characters included. A
value that is not valid UTF-8 is left out without discarding the payload or the response's other
fields, and is counted in `nervix_ingestor_header_values_dropped_total`. `metadata.status` is the
response's status as a `U16`, and `metadata.polled_at` is the instant the poll was due, as a
`DATETIME`, rather than the instant its response arrived, so `TIMESTAMP AT metadata.polled_at` gives
each payload the instant of its poll.

A response whose `content-encoding` is exactly `gzip`, `deflate` or `br` is decompressed before
Nervix reads it, and its `content-encoding` and `content-length` fields are removed, so the payload
is the decompressed body and `read_header` reads neither field. A response in any other encoding,
or in several, arrives as it was sent, with those fields.

#### HTTP Polling Cadence

The first poll is sent as soon as the ingestor starts, where a Prometheus query first becomes due
after one interval. Later polls stay on that anchored schedule of the domain clock, so a paced
domain polls at the instants its own clock reaches. A poll slower than `EVERY` coalesces the
occurrences it missed into one poll at the newest due instant.

#### When An HTTP Poll Fails

A poll that cannot deliver its response fails as a whole. `DESCRIBE INGESTOR` shows the failure as
the ingestor's transient error, in the form `transient error: failed to read from http ingestor:
<failure>`, and every attached session receives it as a server error. The next poll that succeeds,
one answered with `204` included, clears it. A failed poll is not sent again: the next poll runs at
the next due instant, so the failed instant delivers nothing.

| What the poll met | `<failure>` |
| --- | --- |
| A status that is neither `2xx` nor a redirect its client follows | `HTTP ingestor returned status 503 Service Unavailable` |
| A redirect its client does not follow | `HTTP ingestor returned status 302 Found` |
| A response above `MAX BODY SIZE` | `HTTP ingestor response exceeds MAX BODY SIZE 1MiB` |
| A request that could not be sent: a name that did not resolve, a connection or TLS session that failed, or a response head that did not arrive within `timeout_ms` | `failed to send HTTP ingestor request: error sending request`, then the transport's causes, such as `client error (Connect): dns error: error resolving DNS: resolving 'api.example.com' failed: the name does not exist`, `client error (Connect): tcp connect error: Connection refused (os error 111)` or `operation timed out` |
| A response body that ended early or outlasted `timeout_ms` | `failed to read HTTP ingestor response body`, then the transport's causes, such as `error decoding response body: request or response body error: operation timed out` |

A lookup failure names the endpoint's host, as the node's resolver reports it; see
[Lookup Outcomes](name-resolution.md#lookup-outcomes). No failure names the request's URL, its path
or query, a header value or a client secret.

#### HTTP Polling Response Bound

`MAX BODY SIZE <bytes>`, written between `HEADERS` and `EVERY`, bounds how many bytes of one
response body a poll reads; a statement that leaves it out reads at most `16MiB`. The size is a
whole number greater than zero followed by `B`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB` or
`TiB`, and `SHOW CREATE` renders it in the unit it was written in. The bound counts the body Nervix
reads, after the decompression above, so it limits the payload rather than the size of its transfer.
A response whose `content-length` announces more than the bound fails before its body is read, and
one whose body exceeds the bound fails when it does.

`DESCRIBE INGESTOR` prints the bound in effect, whether the statement wrote it or not, in
`options: METHOD 'GET' MAX BODY SIZE 16MiB EVERY 1s`. `DROP MAX BODY SIZE` restores that bound,
and so does a `SET FROM` whose new body leaves `MAX BODY SIZE` out, because it replaces the whole
body.

#### HTTP Polling Intake And Quiesce

A poll delivers at most one payload, which enters the graph as an intake batch of its own. The next
poll waits until that payload has entered the graph or, while a `BUFFER` quiesce holds the ingestor,
has been handed to its buffer. A poll whose `ON INGESTION` unfolding finds the node's extension
workers full waits for them; see [When The Extension Workers Are Full](#when-the-extension-workers-are-full).

`SUSPEND` skips polls and leaves a gap. `BUFFER` keeps polling on the declared cadence and delivers
retained payloads in order after resume. It retains each payload whole: `MAX SIZE` counts the bytes
of the response bodies it holds, and `ON OVERFLOW` drops whole payloads. Memory-pressure quiesce
skips polls under either mode; see [Quiesce Modes](#quiesce-modes).

A quiesce change or a stop that meets a poll in flight abandons that poll and keeps nothing of its
response. A hold that begins or ends during a poll therefore leaves a gap of one occurrence, under
`BUFFER` too.

#### `HTTP CLIENT`

Every key of an `HTTP CLIENT`'s `CONFIG` is Nervix's own. One client serves `HTTP REQUEST`
ingestors and `HTTP REQUEST` emitters, and each key means the same for both:

| Key | Required | Value |
| --- | --- | --- |
| `endpoint` | Yes | An `http` or `https` URL with a host. A poll is sent to it as written, its path and query included. An [HTTP emitter](emitters.md#http-request-configuration) requires an origin, a scheme, host and optional port with no user information, path beyond `/`, query or fragment, beneath which each record's `PATH` names its target. |
| `timeout_ms` | Yes | A positive whole number of milliseconds that bounds one request and the redirects it follows, as [Client Deadlines](#client-deadlines) defines. For a poll it covers name resolution, connection setup, TLS, the request and the whole response body, and for an emitted request everything up to its complete final response headers. |
| `follow_redirects` | No | `true` follows a redirect to the client's own origin; `false`, or leaving the key out, makes a redirect the response. See [HTTP client transport](emitters.md#http-client-transport). |
| `proxy` | No | An `http` or `https` URL with a host, naming the proxy that every request of the client goes through. User information in it is sent to the proxy alone, as `Basic` proxy credentials. |
| `tls_ca_file` | No | PEM certificate authorities that HTTPS trusts in addition to the Mozilla roots Nervix ships and the host's native roots, for polling and emitting alike. |
| `tls_cert_file`, `tls_key_file` | Together | The PEM certificate chain and private key the client presents to authenticate over HTTPS. |
| `secret.<name>` | No | A confidential string the operator names after the `secret.` prefix. A polling header sends it through `CLIENT SECRET 'secret.<name>'`; an emitter sends none of them. |

`CREATE HTTP CLIENT` refuses a configuration that cannot configure the client, and names the key: a
key the client does not declare, a key written twice or not in lowercase, a missing `endpoint` or
`timeout_ms`, a URL of another scheme or without a host, a `follow_redirects` that is not a boolean,
a `timeout_ms` that is not a positive whole number of milliseconds, or one of `tls_cert_file` and
`tls_key_file` without the other. For example,
`client 'orders_api' configuration is invalid in domain 'shop': missing required client config key 'timeout_ms'`.
A value can name a file of the client's `MOUNT`, as in `'tls_ca_file' = '{{ api_tls }}/ca.pem'`.
The rendered configuration must satisfy these rules before commitment, and a poll checks it again
when its ingestor opens.

`SHOW CREATE CLIENT` and `DESCRIBE CLIENT` print every key but mask as `********` each
`secret.<name>` key and an `endpoint` or `proxy` that carries user information or a query string.
Complete serialization keeps every value, so a backup archive holds them; see
[Backup And Restore](backup-and-restore.md).

#### HTTP Polling Transport

A poll travels through the HTTP client Nervix shares with Prometheus, Sentry and OTEL over HTTP, and
speaks the [transport contract](emitters.md#http-client-transport) one `HTTP CLIENT` declares for
polling and emitting alike:

- The host of `endpoint` is resolved through the node's DNS resolver. It stays the HTTP authority
  and the name HTTPS verifies, as [Connection Identity](name-resolution.md#connection-identity)
  describes.
- A poll speaks HTTP/2 when the endpoint's TLS negotiates it, and HTTP/1.1 otherwise.
- A poll follows a redirect only when its client sets `follow_redirects = true`, only to the
  client's own origin and at most 10 times, sending the same method and fields again. It sends
  through the declared `proxy` alone, and no proxy environment variable has any effect.
- Basic credentials in the user information of `endpoint`, as in
  `https://reader:secret@api.example.com/orders/latest`, are sent in the `authorization` field of
  every poll and leave the URL the request names.

### Kafka

Kafka topic ingestors can construct [inherited branches](partitioning.md). With `OFFSET BY DOMAIN
INSTANCES PER PARTITION`, each reader takes its group's external partitions and is placed with the
matching branch partition. Consumer-group readers using a fixed instance count still inherit the
native partition and route records to its owner.

<!-- connector-matrix:ingest-kafka-topic:start -->
```plexus,ignore
CREATE KAFKA TOPIC INGESTOR <name>
  FROM <client> TOPIC 'orders'
  (OFFSET BY DOMAIN | OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST | OFFSET BY CONSUMER GROUP 'readers' START AT LATEST)
  [INSTANCES 2 | INSTANCES PER PARTITION]
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
| Options in statement order | `OFFSET BY DOMAIN` (keyword choice); `OFFSET BY CONSUMER GROUP 'readers' START AT EARLIEST` (string literal checked by kafka_consumer_group); `OFFSET BY CONSUMER GROUP 'readers' START AT LATEST` (string literal checked by kafka_consumer_group); required<br>`INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); optional; default 1 |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | Enumerable; native coordinate from metadata.partition |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.partition: I32 OPTIONAL<br>metadata.offset: I64 OPTIONAL<br>metadata.key: BYTES OPTIONAL<br>metadata.timestamp: DATETIME OPTIONAL<br>metadata.timestamp_type: STRING OPTIONAL<br>metadata.tombstone: BOOL OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | replayable |
| Acknowledgement confirmation | answered |
| Buffers | an inherited consumer group's unadmitted native partition position: at most 1 position per inherited group reader; when full, holds back what fills it<br>a record conversion report after a gathered prefix: at most 1 report per ingestor instance; when full, holds back what fills it<br>messages librdkafka prefetches: driver default, queued.min.messages 100000 per partition, queued.max.messages.kbytes 65536; when full, holds back what fills it<br>admitted synchronous control jobs per node: at most 1024 pending jobs; when full, immediately refuses the request for its sender to retry |
| Network waits | name resolution: unbounded<br>connection setup: TCP, TLS and SASL: driver default, socket.connection.setup.timeout.ms, 30 s<br>a fetch: driver default, socket.timeout.ms, 60 s<br>a consumer-group offset commit: fixed 30s<br>metadata, partition inspection, offset lookups, watermarks and seeks including executor admission: driver default, socket.timeout.ms, 60 s per control job<br>subscribe, assign, pause and unsubscribe: unbounded<br>destroying a consumer that joined its group, for its LeaveGroup answer: driver default, 5 s per LeaveGroup request |
| Driver limits | Kafka clients accept one name per property and refuse Nervix-owned keys. The ingestor supplies group.id, auto.offset.reset and enable.auto.commit, and disables enable.partition.eof and allow.auto.create.topics.<br>An inherited consumer-group reader disables automatic offset storage and stores only positions in its committed external partition set. It pauses and rewinds an unadmitted partition through connector control execution, retaining its position until that work finishes. The broker confirms that exact unread starting offset before the reader can retire, so even a subsequent START AT LATEST join cannot skip it; replacing the reader after a topology commit admits the partition.<br>Connector control execution has two workers per node by default. A commit still unanswered after its caller's 30 s deadline retains its worker and the ingestor's one in-flight commit until the native call ends. Native subscribe, assign, pause and unsubscribe have no cancellable deadline; their caller waits socket.timeout.ms at most.<br>A consumer whose last handle is gone is destroyed by one connector control job, which waits for a place rather than being refused; no Nervix key changes librdkafka's 5 s LeaveGroup timeout.<br>receive.message.max.bytes 100000000 |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [KAFKA TOPIC capability and mirror reference](connector-capabilities.md#kafka-topic-ingest).
<!-- connector-matrix:ingest-kafka-topic:end -->

`TOPIC` takes a string literal that Kafka accepts as a topic name: 1 to 249 ASCII letters, digits,
`.`, `_` and `-`, other than `.` and `..`. Nervix preserves its case in both directions, so
`TOPIC 'Orders.EU'` reads exactly that topic. `OFFSET BY CONSUMER GROUP` takes the group's name as
a string literal of 1 to 255 bytes without control characters, passed to Kafka as written, and
requires `START AT EARLIEST` or `START AT LATEST`. The ingestor names `ON QUIESCE SUSPEND`, the
only quiesce mode it declares: the topic keeps its records after they are read, so a suspended
ingestor resumes from its position, within the topic's retention. It reads a record's headers
through `read_header` and `read_headers`.

The record's metadata also exposes its key as `metadata.key` (`BYTES`), its broker timestamp as
`metadata.timestamp` (`DATETIME`), its timestamp kind as `metadata.timestamp_type` (`STRING`), and
whether its value is null as `metadata.tombstone` (`BOOL`). A missing key is null; an empty key is
an empty byte sequence. `hex_encode(metadata.key)` or `bytes_to_utf8(metadata.key)` explicitly
converts a key for a string field. Kafka timestamps count Unix milliseconds; Nervix preserves
that instant exactly. The timestamp kind is `create_time` or `log_append_time`; a record without
a timestamp has null timestamp and timestamp kind. `TIMESTAMP AT metadata.timestamp` uses that
instant as the record's event time, while `TIMESTAMP NOW` uses the domain's execution time. An
unavailable timestamp cannot satisfy `TIMESTAMP AT metadata.timestamp`; it never falls back to
`NOW`. A present timestamp whose milliseconds name an instant before 1677-09-21 or after
2262-04-11, which no `DATETIME` holds, such as microseconds a producer wrote where Kafka specifies
milliseconds, keeps its record. Wherever a route or `TIMESTAMP AT` reads `metadata.timestamp`, the
record is refused with a `validation` message error of operation `admit` that names the field and
those milliseconds. Under an acknowledged mode, send each such route's `ON MESSAGE ERROR` to a
relay: `LOG` negatively acknowledges the record, so the ingestor reads it again before any later
record. `metadata.timestamp_type` still reads its kind, and an ingestor that reads neither field
ingests the record like any other. A topic whose `message.timestamp.after.max.ms` is shorter than
such a timestamp's distance from the broker's clock refuses the record when it is produced, so it
never reaches an ingestor; the setting's default in Kafka 3.9 is 9223372036854775807, which accepts
it.

A null Kafka value is a tombstone and reaches the codec as an empty payload with
`metadata.tombstone = true`. An empty non-null value has `metadata.tombstone = false`. The codec
still determines how many rows the payload produces: an empty JSON payload produces none, while
an empty TOML document can produce a row through its JAQ transformation. Metadata is available
to each resulting row's route. Kafka declares only `SUSPEND` on quiesce, so it never copies these
rows into a host quiesce buffer.

Offset modes:

- `CONSUMER GROUP '<group>' START AT EARLIEST|LATEST`: Kafka manages offsets, and the statement says where the group starts a partition it holds no offset for
- `DOMAIN`: Nervix stores the next offset in replicated runtime state and commits partition-to-instance assignment in the Raft-backed domain schedule

`START AT` applies only to a partition the group has no usable committed offset for: a new group,
a partition added to the topic, or an offset the topic's retention has passed. `EARLIEST` starts it
at the earliest retained record, and `LATEST` at the partition's end, so a new group declared
`LATEST` skips what the topic retains and reads what is produced after it takes the partition up.
A partition the group holds an offset for resumes there whichever position the statement declares.
`ALTER KAFKA TOPIC INGESTOR <name> SET START AT EARLIEST|LATEST` changes the position for the next
such partition without moving any committed offset, and `SHOW CREATE` and `DESCRIBE INGESTOR`
print the declared position, not a report of the group's current one. Writing `START AT` with
`OFFSET BY DOMAIN` is refused at CREATE and `SET START AT` on such an ingestor is refused at ALTER,
leaving its definition unchanged: the domain's start point places it.

`CREATE KAFKA CLIENT` requires `bootstrap.servers`, the brokers that each consumer and producer of
the client contacts first, and refuses a client without it, which could reach no broker. It also
refuses each of these Nervix-owned keys, even when its value agrees with the value Nervix sets:

| Reserved client key | What owns its value |
| --- | --- |
| `group.id` | The ingestor's `OFFSET BY` mode supplies the consumer group identity; partition inspectors use their own internal identity. |
| `auto.offset.reset` | The consumer group's `START AT` supplies the start position; domain-offset consumers use `earliest`. The refusal names `START AT`, the statement clause that sets it. |
| `enable.auto.commit` | Only `OFFSET BY CONSUMER GROUP` with `MODE NO ACK` enables automatic commits. An `ACK` mode commits after downstream acknowledgement. |
| `enable.partition.eof` | Every consumer disables partition EOF events. |
| `allow.auto.create.topics` | Every consumer disables topic auto-creation; the producer keeps its driver default, as [Kafka emission](emitters.md#kafka) explains. |
| `delivery.report.only.error` | Every producer requests success and failure reports so it can release each retained record, in both publishing modes. |

Other property names and values pass through after validation. The declared key types are checked
first; librdkafka then checks native property names and values offline before commitment, without
opening a consumer, producer or broker connection. An unknown property or invalid value refuses
`CREATE KAFKA CLIENT` and names its key. For example, `'fetch.wait.max.ms' = '100'` is accepted,
while `'message.max.bytes' = 'big'` is refused. The generated
[Kafka client reference](connector-capabilities.md#kafka-client) lists the declared types, reserved
keys, accepted driver names and defaults. A successful offline check does not establish that a
broker is reachable, credentials work or the named topic exists.

An accepted native property without an explicit key declaration remains confidential: for example,
`'statistics.interval.ms' = '100'` is accepted, but public inspection shows its value as `********`.
Invalid configuration reports name the client and key without quoting its value. The typed
librdkafka cause remains internal, because a driver's description can include configuration values;
see [Errors And Diagnostics](errors-and-diagnostics.md).

librdkafka reads some properties under two names and applies whichever it is handed last, and a
client's entries reach it in no fixed order. A Kafka client therefore accepts each such property
under one name, the one the Apache Kafka Java client uses, and refuses the other even when it is
written alone, naming both: write `bootstrap.servers`, not `metadata.broker.list`, and
`delivery.timeout.ms`, not `message.timeout.ms`. The
[Kafka client reference](./connector-capabilities.md#kafka-client) lists every refused name with
the name to write. librdkafka also reads a default-topic property written under a `topic.`
prefix, so the client refuses a key written that way, except librdkafka's own global properties
whose names begin with `topic.`, such as `topic.metadata.refresh.interval.ms`. Write a
default-topic property under its own name. The topic-level `auto.commit.interval.ms`,
`auto.commit.enable` and `offset.store.method` serve only librdkafka's legacy consumer, which
Nervix does not run, and the default topic takes `compression.type` from the client's global
setting. Each property a client sets therefore has one name and one value on every node.

The statement's offset mode supplies the consumer's `group.id`. Only `OFFSET BY CONSUMER GROUP`
with `MODE NO ACK` enables `enable.auto.commit`. Every consumer disables `enable.partition.eof`
and `allow.auto.create.topics`. `START AT` sets `auto.offset.reset` under
`OFFSET BY CONSUMER GROUP`, and every `OFFSET BY DOMAIN` consumer sets it to `earliest`, so a
position the topic's retention passed while the consumer read resumes at the next retained record.
These overrides also take precedence over the configurable driver defaults in the table above.

A Kafka ingestor never creates the topic it reads. Every consumer Nervix runs for it sets
`allow.auto.create.topics` to `false`, whatever the broker allows, and `CREATE KAFKA CLIENT` refuses a
Kafka client whose `CONFIG` sets that key. Provision the topic before the ingestor starts. Until it
exists, `DESCRIBE INGESTOR` reports `transient error: KAFKA topic '<topic>' does not exist`, and the
ingestor starts reading once the topic is created.

Only `OFFSET BY CONSUMER GROUP '<group>'` joins a broker-managed group and commits offsets to it.
That group belongs to the operator: dropping the ingestor leaves its committed offsets on Kafka
for later consumers. `OFFSET BY DOMAIN` uses direct partition assignments, and the partition
inspectors only fetch metadata. Their internal `nervix_domain_...` and watch group identifiers
never become groups on the broker, so running or dropping those consumers creates or deletes no
broker group.

In an ACK mode, Nervix commits a Kafka position only after its downstream acknowledgement
completes. If downstream work rejects a record, the ingestor seeks back to it. If Kafka cannot
seek back on a partition the ingestor still holds, the ingestor stops polling, refreshes its
assignment, and retries that seek. It cannot commit a later offset while the rejected position
remains unresolved. Under `OFFSET BY CONSUMER GROUP` the group can take the partition away before
the rejected record is read again: a rebalance can move it to another member while its batch is
in flight, refreshing the assignment leaves and rejoins the group, and a broker outage that
outlasts the group session revokes it. Whenever the group assigns the partition to the ingestor
again, the ingestor starts it at its earliest rejected record, until a commit passes that record,
rather than at the group's committed offset or, for a group that committed nothing, where
`START AT` would start it. Another member that takes the partition resumes it from the
committed offset, which never passes a rejected record; where the group has committed nothing for
the partition, that member's `START AT` decides, and `LATEST` skips the rejected
records. Replays after a crash or assignment change may repeat records whose output was already
written.

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

Stopping or dropping a Kafka ingestor returns without waiting for its consumers to be destroyed.
The node destroys each one on its connector control workers, where a consumer that joined its group
waits up to five seconds for the coordinator to answer its LeaveGroup request; see
[Kafka client retirement](./connector-contract.md#kafka-client-retirement).

Under `OFFSET BY CONSUMER GROUP`, `MODE NO ACK` is the consumer's auto-commit: the consumer
commits the positions it has read on its own schedule, `auto.commit.interval.ms`, without waiting
for downstream acknowledgement, so a failure can lose records whose positions were committed before
their downstream work completed. Suspending or stopping the ingestor commits the positions it has
read since the last of those commits, and waits for the broker's answer before the consumer leaves
its group.

`MODE NO ACK` declares no retry policy, so a Kafka ingestor in that mode that fails to resume or to
read reconnects on the host's reconnect cadence: it waits 250 ms after its first failure and
doubles the wait after each further one, up to 30 seconds, until a batch is read again.
`DESCRIBE INGESTOR` shows the current wait as its reconnect backoff.

`OFFSET BY DOMAIN` is at-least-once. A commit records the partition's next offset in memory. Nervix
persists the offsets on the runtime state snapshot interval and whenever a node stops executing
the domain, and when the ingestor has state replicas, a commit completes only after every replica
has acknowledged it. Crash recovery may therefore restart from a slightly stale persisted offset
snapshot. The leader watches Kafka partition topology and commits any rebalance through the
strongly consistent domain schedule, which is persisted through the control-plane Raft/Fjall path.
Executing ingestors consume only the committed partition assignment.

A commit or start-point reset waits for its replica acknowledgements within the native checkpoint
bulk operation's thirty-second budget. A missing acknowledgement fails the commit at that deadline;
the ingestor applies its existing rejection and retry policy. Large native checkpoints use bounded
disk staging and transfer chunks, so checkpoint length is independent of the Bulk memory ceiling.

Offset recovery details:

- `START AT <timestamp>` and `START AT NOW` apply once per domain start generation across all instances. The first initialization persists the resolved partition positions and the applied generation together before reading. Quiescence, reconnects, rebalances, ownership changes and restarts in that generation resume acknowledged positions; they do not apply the timestamp again. A new partition without a recorded position starts at that generation's original timestamp. A later explicit `START AT` begins a new generation and replaces the positions again.
- A delayed instance receives the same explicit-start cut after reconnect, placement or a plain `START`; pending positions retain the explicit generation even when the domain's current generation advances. Its own positions become durable before its adoption is confirmed.
- persisted per-partition offsets are clamped to the partition's currently available Kafka watermark range on reassignment
- in a generation begun with plain `START`, a partition without a stored domain offset starts in a paced domain at its first record at or after the domain's current logical time, or at its end as it stood before that lookup when it has no such record yet; in an unpaced domain it starts at its earliest retained record, so a plain `START` reads what the topic retains and `START AT NOW` is how an unpaced domain skips it
- the instance that takes such a partition up records its start as the partition's domain offset before it reads, so a reconnect resumes the partition there instead of placing it again past records the instance has not acknowledged

Quiescing leaves a consumer group, or unassigns domain-owned partitions. Domain-owned partitions
resume from their recorded offsets, and consumer-group
partitions resume from the group's committed offsets or an outstanding rejection floor as described
above. Topic time and size retention continue during the pause. For consumer-group
offsets that age out, `START AT` decides whether resume uses the earliest retained record or skips
to new records. Domain
offsets clamp to the partition's current low and high watermarks; records retention has removed
cannot be recovered. Other members of a user-named group may consume and commit while Nervix is
absent. Quiescing does not change the auto-commit loss windows of `MODE NO ACK`.

### Pulsar

Pulsar topic ingestors can construct [inherited branches](partitioning.md). `INSTANCES PER
PARTITION` with `TYPE EXCLUSIVE` or `TYPE FAILOVER` consumes each group's explicit partition topics.
Shared readers using a fixed count continue to route inherited keys to their branch owners.

<!-- connector-matrix:ingest-pulsar-topic:start -->
```plexus,ignore
CREATE PULSAR TOPIC INGESTOR <name>
  FROM <client> TOPIC 'orders'
  (SUBSCRIPTION 'readers' OWNED BY INGESTOR START AT EARLIEST | SUBSCRIPTION 'readers' OWNED BY INGESTOR START AT LATEST | SUBSCRIPTION 'readers' PROVISIONED)
  (TYPE SHARED | TYPE KEY_SHARED | TYPE FAILOVER | TYPE EXCLUSIVE)
  [INSTANCES 2 | INSTANCES PER PARTITION]
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
| Options in statement order | `SUBSCRIPTION 'readers' OWNED BY INGESTOR START AT EARLIEST` (string literal checked by pulsar_subscription); `SUBSCRIPTION 'readers' OWNED BY INGESTOR START AT LATEST` (string literal checked by pulsar_subscription); `SUBSCRIPTION 'readers' PROVISIONED` (string literal checked by pulsar_subscription); required<br>`TYPE SHARED` (keyword choice); `TYPE KEY_SHARED` (keyword choice); `TYPE FAILOVER` (keyword choice); `TYPE EXCLUSIVE` (keyword choice); required<br>`INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); optional; default 1 |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | Enumerable; native coordinate from metadata.partition |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.message_id: STRING OPTIONAL<br>metadata.publish_time: DATETIME OPTIONAL<br>metadata.event_time: DATETIME OPTIONAL<br>metadata.key: STRING OPTIONAL<br>metadata.ordering_key: BYTES OPTIONAL<br>metadata.producer_name: STRING OPTIONAL<br>metadata.sequence_id: I64 OPTIONAL<br>metadata.redelivery_count: I64 OPTIONAL<br>metadata.partition: I32 OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | replayable |
| Acknowledgement confirmation | answered |
| Buffers | messages each partition consumer receives ahead of the host: driver default, a receiver queue of 1000 messages; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection: TCP, TLS and CONNECT: driver default, 10 s per address, one attempt<br>topic lookup and subscribe: driver default, 30 s, one attempt<br>partition topology observation, including resolution and connection: driver default, one operation timeout per connection or metadata lookup<br>an idle inherited group with no assigned partition topics: driver default, the batch timeout or one driver operation timeout; cancellation ends it earlier<br>an acknowledgement under ACK: driver default, 30 s operation timeout<br>close: unbounded |
| Driver limits | Inbound frames have no size limit. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [PULSAR TOPIC capability and mirror reference](connector-capabilities.md#pulsar-topic-ingest).
<!-- connector-matrix:ingest-pulsar-topic:end -->

#### Pulsar topic names

Both ingestion and emission write a quoted, case-preserving `TOPIC` name. A local name such as
`TOPIC 'orders'` selects `persistent://<namespace>/orders`, using the client's `namespace`,
`public/default` when omitted. A fully qualified name such as
`TOPIC 'persistent://tenant-a/events/Orders'` or
`TOPIC 'non-persistent://tenant-a/events/Orders'` is sent as written and does not use that default.

A topic name is 1 to 1,024 UTF-8 bytes, without whitespace or control characters. A local name has
no slash; a qualified name has exactly three nonempty levels after `persistent://` or
`non-persistent://`: tenant, namespace and topic. `CREATE` and `ALTER` refuse other shapes.
Provision the topic before starting the ingestor or emitter, and choose its subscription ownership
separately as described below.

#### Received message metadata

A Pulsar message exposes these nullable metadata fields to routing expressions:

| Field | Type | Value |
| --- | --- | --- |
| `topic` | `STRING` | The qualified topic the consumer received from |
| `message_id` | `STRING` | `ledger:entry:partition`, with `:batch_index` for a batched message |
| `publish_time` | `DATETIME` | The broker message's publish time, in milliseconds |
| `event_time` | `DATETIME` | The producer's optional event time, in milliseconds |
| `key` | `STRING` | The partition key text stored in the message metadata |
| `ordering_key` | `BYTES` | The optional binary ordering key |
| `producer_name` | `STRING` | The producer name carried by the message |
| `sequence_id` | `I64` | The producer sequence id |
| `redelivery_count` | `I64` | The broker's count of prior redeliveries |
| `partition` | `I32` | The native partition index, or `-1` for an unpartitioned topic |

Combine `topic` and `message_id` when identifying messages across topics. An absent partition
renders as Pulsar's `-1`. The acknowledgement retains the complete broker id independently of
this display. The key is the stored text, including base64 text when the producer marks it encoded;
`ordering_key` preserves its bytes. Missing keys read as null, while present empty keys stay empty.
Properties remain available through `read_header` and `read_headers` with their order and repeats.

For batched messages, a member's sequence id and event time take precedence when present,
including zero. An omitted member field uses the corresponding batch value. Event time is null
when both the member and batch omit it.

`TIMESTAMP AT metadata.publish_time` or `TIMESTAMP AT metadata.event_time` uses that message time
for admission and windows. An absent event time is null; a present zero is the Unix epoch. Nervix
does not substitute publish time for an absent event time. Times outside the signed Unix-nanosecond
range and sequence ids above `I64::MAX` produce a typed read error identifying the topic, message
and field. A valid prefix already gathered is delivered before that error. Pulsar's protocol defines
an omitted redelivery count as zero, so the first delivery reports zero and a broker redelivery can
report one. Every protocol `u32` count fits the declared `I64`.

#### Subscriptions and consumers

Pulsar ingestors read through a durable subscription. Every statement declares who owns the
subscription, because the broker cannot say later who created one, and the type the ingestor reads
it as:

- `OWNED BY INGESTOR START AT EARLIEST` or `OWNED BY INGESTOR START AT LATEST`: the ingestor owns
  the subscription and declares where a newly created cursor begins. `EARLIEST` reads the topic's
  retained backlog; `LATEST` starts after the messages present when the subscription is created.
  Dropping the ingestor or its domain removes its owned subscription.
- `PROVISIONED`: an operator provisioned the subscription, and Nervix never creates or removes it.
  Before an instance joins it, the instance checks that it exists. A missing subscription is the
  ingestor's transient error, as in `PULSAR subscription 'readers' of topic
  'persistent://public/default/orders' does not exist`, and the instance checks again after its
  retry policy's backoff, or on the host's reconnect cadence in `MODE NO ACK`, until the operator
  provisions it. A missing subscription does not keep the ingestor or its domain from starting.

`START AT` is required for an owned subscription and is unavailable for a provisioned subscription.
An existing subscription keeps its cursor regardless of the declared start position. `ALTER PULSAR
TOPIC INGESTOR <name> SET START AT EARLIEST|LATEST` changes the policy for the next creation without
seeking the existing cursor or discarding its backlog. The alteration is refused for a provisioned
subscription and leaves its definition unchanged. `SHOW CREATE` and `DESCRIBE INGESTOR` expose the
declared position; it is a creation policy, not a report of the current broker position.

`TYPE` decides how the broker shares the topic among the consumers of the subscription. It is
required, and a statement writes it after the subscription's owner:

| `TYPE` | What the broker delivers | Instances |
| --- | --- | --- |
| `SHARED` | Every consumer takes messages, with no order across them. | Any number |
| `KEY_SHARED` | Every consumer takes messages, and one consumer takes all of a key's, so a key keeps its publication order. Messages without a key all reach one consumer. | Any number |
| `FAILOVER` | One consumer takes messages while the others stand by. When it leaves, a standby continues from what it has not acknowledged. | One, or inherited `PER PARTITION` |
| `EXCLUSIVE` | The broker admits one consumer and refuses any other. | One, or inherited `PER PARTITION` |

`FAILOVER` and `EXCLUSIVE` permit one fixed instance. More fixed instances are refused at `CREATE`
and `ALTER`, as in
`PULSAR subscription TYPE EXCLUSIVE reads through one consumer, so it refuses INSTANCES 2`.
`INSTANCES PER PARTITION` instead requires inherited branches: each reader subscribes to the
partition topics of its committed group, so each topic still has one active consumer.
A `FAILOVER` standby is another ingestor or application that
joins the same subscription.

The broker holds one type per subscription while any consumer is connected. It refuses a consumer
of another type, and a second consumer of an `EXCLUSIVE` subscription. The refusal is the ingestor's
transient error, as in `the PULSAR broker refused a consumer of subscription 'readers' of topic
'persistent://public/default/orders' with ConsumerBusy: Exclusive consumer is already connected`,
and the instance joins again after its retry policy's backoff, or on the host's reconnect cadence
in `MODE NO ACK`, until the broker admits it. A refusal for a subscription another consumer holds,
or for a topic the broker has not yet loaded, does not keep the ingestor or its domain from starting.

`ALTER PULSAR TOPIC INGESTOR <name> SET TYPE SHARED|KEY_SHARED|FAILOVER|EXCLUSIVE` changes the type
the ingestor's next join reads as. The subscription keeps its cursor and its backlog, and the
instance rule above applies to the alteration, which leaves the definition unchanged when refused.
On a running ingestor the consumers leave and join again as the new type, and any other consumer
of the subscription still connected under the old type is refused the same way.

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
subscription of the same topic share it as its type allows, and each message reaches one of them.

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
keep the subscription. Replacing the ingestor with `ALTER PULSAR TOPIC INGESTOR`, by
`SET FROM ...` or by an operation such as `SET TOPIC` or `SET SUBSCRIPTION`, removes the preceding
owned subscription after its consumers stop and before the replacement starts, unless the
replacement reads the same subscription of the same qualified topic at the same resolved broker
address. Client names, route, codec, delivery-mode, subscription type and instance-count changes
do not change that identity. Short topics use their client's namespace, so a short name and its
fully qualified spelling identify the same topic. Broker addresses compare their URL scheme,
host and effective port; an omitted port means 6650 for `pulsar` and 6651 for `pulsar+ssl`.
Different configured host names are distinct addresses even if DNS resolves them alike.
Declaring the retained subscription `PROVISIONED` hands its ownership to the operator and keeps
its backlog. Removal on replacement has the same timeout and best-effort reporting as removal
on drop; a failure does not prevent starting the replacement. `DESCRIBE INGESTOR`
shows the subscription name, its ownership and declared start position, `TYPE` and `INSTANCES`
together in the declared `options:` row.

#### Pulsar client

The same `PULSAR CLIENT` keys configure ingestion and emission:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'auth_token'`: the token the client authenticates with. `DESCRIBE CLIENT` and
  `SHOW CREATE CLIENT` show it redacted.
- optional `'namespace'`: the tenant and namespace, written `'tenant/namespace'`, that a short
  topic name is placed in, so `TOPIC 'orders'` reads `persistent://<namespace>/orders`; defaults to
  `public/default`, which `DESCRIBE CLIENT` shows as a default. Fully qualified topic names are sent
  as written.
- optional `'tls_ca_file'`: PEM-encoded CA bundle a `pulsar+ssl://...` connection trusts beside the
  bundled public roots and the host's trust store
- optional `'tls_cert_file'` and `'tls_key_file'`: the PEM-encoded client certificate the client
  presents and its private key. `CREATE PULSAR CLIENT` refuses one without the other.
- optional `'tls_allow_insecure_connection'`: `true` accepts any certificate a broker presents,
  whoever signed it and whatever it names, so the connection is encrypted but says nothing about
  who holds it; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `false` accepts a certificate from a trusted
  authority whatever address it names; defaults to `true`

The five TLS keys apply only to a `pulsar+ssl://...` address. A plaintext `pulsar://...`
connection uses none of them, so `CREATE PULSAR CLIENT` refuses them there. It also refuses a
`namespace` that is not exactly a tenant and a namespace, each nonempty and without whitespace or
control characters, such as `'tenant-a'` or `'tenant-a/orders/eu'`, because no topic qualified
with it is a valid Pulsar topic.

A client authenticates with what its keys hold, and both directions connect the same way. With
`'auth_token'` the broker takes the client's role from the token. With `'tls_cert_file'` and
`'tls_key_file'` alone it takes the role from the certificate, which the client presents in the TLS
handshake. With both, the token names the role and the certificate satisfies a broker that requires
one on connect. A client with neither presents no credential.

#### Acknowledgements and recovery

The Pulsar driver makes one attempt at a connection or an operation, and the host owns every retry.
An ingestor that loses its broker shows the failure as its transient error and reconnects on its
`RETRY POLICY`, or on the reconnect policy under `MODE NO ACK`. A consumer of one partition of a
partitioned topic that cannot reconnect at once rejoins within 30 seconds instead, while the
partitions it did not lose keep flowing.

An ingestor whose client cannot connect when its domain starts does not start on the node that runs
it, and that node keeps trying until the client connects, so the ingestor comes up once the broker
can be reached. A failure that `START` reports names its cause: a broker that refuses the
connection, such as for a missing or rejected credential, shows
`failed to connect PULSAR client: the PULSAR broker refused the connection with AuthenticationError: ...`,
and a connection that never opened, such as a TLS handshake the broker or the client refused, shows
`no connection to the PULSAR broker opened` with the driver's error beneath it.

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

In `MODE NO ACK`, a payload its codec refuses is consumed through the ingestor's `ON GENERAL ERROR`
policy and counted as a decode rejection. The ingestor queues its acknowledgement without waiting
for the broker's answer. See
[When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload) for reporting and the separate
contract of a message that decoded but failed in a route.

Under `FAILOVER` and `EXCLUSIVE` the broker does not take one message back: a rejection rewinds the
consumer to everything it has not acknowledged. The messages behind a rejected one arrive again,
after those the consumer already received, so delivery stays at least once and neither a single
delivery nor the topic's order holds across a rejection.

Suspension disconnects the consumer. The durable subscription retains its backlog and redelivers
unacknowledged in-flight messages. Broker message TTL, backlog quota policy, and inactive-subscription
expiry remain in force. If an owned subscription expires, resume creates it again at its declared
`START AT` position: `EARLIEST` can deliver retained earlier messages again, while `LATEST` skips
the backlog present at recreation. What the topic no longer retains from the suspension window
is lost.
A provisioned subscription that expires is the ingestor's transient error until the operator
provisions it again.

### RabbitMQ

`RABBITMQ QUEUE` consumes an externally provisioned queue. To receive messages from an exchange,
provision a queue and its exchange bindings first, then consume that queue; an exchange is not an
ingestor entity. [RabbitMQ emitters](emitters.md#rabbitmq) publish either through the default
exchange to a queue or through a named exchange and a computed routing key.

<!-- connector-matrix:ingest-rabbitmq-queue:start -->
```plexus,ignore
CREATE RABBITMQ QUEUE INGESTOR <name>
  FROM <client> QUEUE 'orders'
  [INSTANCES 2 | INSTANCES PER PARTITION]
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
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); optional; default 1 |
| Headers | `read_header`, `read_headers`; `sorted_by_name`, exact names: one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.exchange: STRING OPTIONAL<br>metadata.routing_key: STRING OPTIONAL<br>metadata.delivery_tag: U64 OPTIONAL<br>metadata.redelivered: BOOL OPTIONAL<br>metadata.content_type: STRING OPTIONAL<br>metadata.content_encoding: STRING OPTIONAL<br>metadata.message_id: STRING OPTIONAL<br>metadata.correlation_id: STRING OPTIONAL<br>metadata.reply_to: STRING OPTIONAL<br>metadata.type: STRING OPTIONAL<br>metadata.app_id: STRING OPTIONAL<br>metadata.user_id: STRING OPTIONAL<br>metadata.priority: U8 OPTIONAL<br>metadata.expiration: STRING OPTIONAL<br>metadata.timestamp: DATETIME OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | written |
| Buffers | Lapin's existing consumer delivery queue; broker prefetch bounds it in acknowledged modes, but automatic acknowledgements do not apply that bound: unbounded<br>deliveries gathered for one host read: set by 1 under NO ACK and ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 65,535; when full, holds back what fills it |
| Network waits | gathering further deliveries after the first under ACK PARALLEL; cancellation hands over every delivery already gathered: `BATCH TIMEOUT`<br>connection: name resolution, address attempts, TLS, the AMQP handshake, channel open and consume: `connect_timeout_ms`; 30s when omitted<br>a silent connection: driver default, twice the negotiated heartbeat |
| Driver limits | Consumer prefetch is 1 under ACK SEQUENTIAL and ACK PARALLEL MAX under ACK PARALLEL, at most 65,535; the broker holds back deliveries beyond that window.<br>NO ACK has no prefetch bound; auto-acknowledged deliveries can accumulate in Lapin's existing consumer queue while the host is not reading.<br>A frame above the negotiated frame_max ends the connection. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |

See the [RABBITMQ QUEUE capability and mirror reference](connector-capabilities.md#rabbitmq-queue-ingest).
<!-- connector-matrix:ingest-rabbitmq-queue:end -->

#### Queue names, metadata and headers

`QUEUE` names an existing queue and preserves its spelling, including letter case and Unicode.
Its string literal must contain 1 to 255 UTF-8 bytes, the AMQP short-string limit; a multibyte
character contributes every byte to that limit. `SHOW CREATE INGESTOR` retains the exact queue
name and the configured acknowledgement, retry and instance policies.

Each delivery exposes its envelope and its AMQP basic properties as metadata.
`metadata.exchange` is the exchange it was published to, `''` for the default exchange, which is
AMQP's name for it, and `metadata.routing_key` the routing key it was published with.
`metadata.delivery_tag` is the `U64` tag its consumer channel gave it, counting from 1 on each
channel, so a reconnected instance starts again at 1. `metadata.redelivered` is true when the broker
delivered the message before, such as after a requeue or a lost connection. `metadata.content_type`,
`content_encoding`, `message_id`, `correlation_id`, `reply_to`, `type`, `app_id`, `user_id`,
`priority`, `expiration` and `timestamp` are its basic properties, each null when the publisher did
not set it. `priority` is the `U8` octet the publisher wrote, whatever priorities the queue
supports, and `expiration` keeps the text the publisher wrote. `metadata.timestamp` reads the AMQP
timestamp, a count of whole seconds since the Unix epoch, as that exact `DATETIME` instant, so
`TIMESTAMP AT metadata.timestamp` admits a message at the instant its producer wrote. A count no
`DATETIME` holds, past 2262-04-11, such as milliseconds written where AMQP specifies seconds, refuses
its message wherever the ingestor's route programs or its `TIMESTAMP AT` read `metadata.timestamp`,
with code `validation` and operation `admit`, and refuses nothing in an ingestor that reads none.

`read_header` and `read_headers` read the AMQP header table, separately from these basic properties.
The declared model is `sorted_by_name` with exact names: one value per name, visited in ascending
byte order, with case-sensitive lookup. It does not preserve publisher insertion order or repeated
entries of the same name. Copy a header or metadata value into a schema field when downstream
processing needs it; neither is carried through a relay automatically.

#### Consumption and settlement

Choose `MODE NO ACK`, `MODE ACK SEQUENTIAL ACK TIMEOUT <duration> RETRY POLICY …`, or
`MODE ACK PARALLEL MAX <n> BATCH TIMEOUT <duration> ACK TIMEOUT <duration> RETRY POLICY …`.
`DESCRIBE INGESTOR` prints the complete selected mode.

| Mode | Consumer prefetch per instance | What completes a transport delivery |
| --- | --- | --- |
| `NO ACK` | No prefetch request | The broker sends it; no downstream result can requeue it. |
| `ACK SEQUENTIAL` | 1 | Its graph outcome decides an individual acknowledgement or requeue. |
| `ACK PARALLEL MAX n` | `n`, from 1 through 65,535 | Each delivery's graph outcome settles it independently of its siblings. |

`ACK TIMEOUT` bounds the absence of graph acknowledgement progress, not total processing time;
see [Delivery Modes](#delivery-modes). `BATCH TIMEOUT` bounds gathering after the first delivery of
a parallel read. Neither replaces consumer prefetch. These are consumer controls; an emitter's
`ACK` waits for publisher confirms and does not acknowledge queue consumption.

`NO ACK` registers an automatically acknowledging consumer, sends no prefetch request and sends
no delivery acknowledgements or rejections. RabbitMQ considers a delivery complete when it sends
it, so connection loss, broker restart or a stopped consumer can lose a delivery the graph has not
processed. Lapin's existing delivery queue has no bound in this mode; a publisher that outpaces the
host can grow that queue. A payload the node's extension workers cannot admit is refused and
counted. Use an acknowledged mode when the broker must retain unprocessed deliveries.

`ACK SEQUENTIAL` uses a prefetch of one. `ACK PARALLEL MAX n` uses a prefetch of `n` per instance,
where `n` is in `1..=65535`, the range of AMQP's prefetch field. Each read gathers up to that count,
ending at `BATCH TIMEOUT` after its first delivery. Each delivery keeps its own graph outcome and
is acknowledged or negatively acknowledged individually; one failed delivery does not requeue
successful siblings. A failed or timed-out graph acknowledgement, or a failed transport
acknowledgement, makes the instance try to negatively acknowledge that delivery with requeue.
After settlement the declared `RETRY POLICY` paces another read if anything was requeued. If a
transport settlement fails, the instance suspends and reconnects as described below.
It first keeps the graph waits of deliveries already admitted to their outcomes, so broker replay
cannot overtake downstream work from that connection.

A payload its codec refuses is consumed and acknowledged, and its decode rejection follows
`ON GENERAL ERROR`; see [When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).

#### Suspension and reconnects

Suspension cancels the consumer and RabbitMQ requeues unacknowledged in-flight deliveries. Other
consumers on the same queue continue. Queue length, message TTL, overflow, and auto-expiry policies
remain in force. If auto-expiry deletes the queue, resume reports an ingestor error until an operator
re-provisions it; Nervix never creates it.

A connection that closes while an acknowledged-mode delivery is still unacknowledged, because the broker restarted,
closed it, or the network dropped it, also makes RabbitMQ requeue that delivery. The acknowledgement
or the requeue the ingestor still owes it can only be sent on the connection it arrived on, so the
instance reconnects on its `RETRY POLICY` cadence. Once that reconnect succeeds, it discards the
pending positions instead of retrying them on the new connection, counts them once in
`nervix_ingestor_transport_redeliveries_total`, which `DESCRIBE INGESTOR` also prints, and consumes
again. RabbitMQ then delivers the message again: a delivery that failed in the graph is retried,
and one the graph had already acknowledged is ingested a second time, so delivery across a
reconnect is at least once.

The acknowledgement confirmation is `written`: Nervix waits for its ACK or requeue frame to be
written, but AMQP sends no acknowledgement of that consumer settlement. A connection lost at that
boundary can therefore redeliver a message whose graph work completed. Delivery tags are scoped to
the channel and cannot be used to settle a replay on a replacement channel.

#### RabbitMQ client

One `RABBITMQ CLIENT` configuration serves queue ingestion and both emitter entities. The
[generated client key reference](connector-capabilities.md#rabbitmq-client) lists every supported
key, its value constraints, owner, confidentiality and TLS file role. It is an unpooled client:
consumer instances and emitters open their own connections. Queue names, exchange names, routing,
persistence and message properties belong to their entity statements, not to client `CONFIG`.

Each instance resolves the host of the client's `addr` through the node's configured DNS resolver
every time it connects, so a changed DNS answer takes effect on the next connection, and tries the
addresses it receives in order. A literal IPv4 address, or an IPv6 address in brackets, is
connected to as written. Resolution, the TCP connection, for `amqps` the TLS handshake, the AMQP
handshake, opening the channel, setting the prefetch window and starting the consumer share the
client's `connect_timeout_ms`, 30 seconds when it is left out; the broker certificate must name the
host `addr` names. A connection that fails or runs out of that deadline, including a name that does
not resolve, is an ingestor error that `DESCRIBE INGESTOR` shows as the transient error, and the
instance tries again on its `RETRY POLICY` cadence, or the host's reconnect policy under
`MODE NO ACK`, without acknowledging anything.

The client's `addr` is an AMQP URI whose scheme is `amqp` or `amqps`, which names a host, and whose
port, when it names one, is not 0. Lapin would take `localhost` for an address that names no host,
so `CREATE RABBITMQ CLIENT` refuses such an address before commit, together with any other address
of that kind and any credentials, virtual host or query option (`heartbeat`, `frame_max`,
`channel_max`, `connection_timeout` or `auth_mechanism`) that Lapin cannot read. The refusal names
the key, never the address, which can carry credentials. Every connection reads the address the
same way.

An `amqps` connection trusts the platform roots and the PEM certificates in `tls_ca_file`, and
verifies the broker certificate against the host `addr` names, whichever of its addresses was
dialled. A client names `tls_cert_file` and `tls_key_file` together or not at all. With them, the
connection presents that PEM certificate chain and its unencrypted PEM private key, PKCS #8,
PKCS #1 or SEC1, to a broker that asks for a client certificate. A file that cannot be read, or that
holds no PEM certificate or no private key, fails the connection as a configuration error. A broker
that requires a client certificate and receives none, or one it does not accept, ends the TLS
session with an alert: `DESCRIBE INGESTOR` shows that the broker refused the TLS session, naming the
alert, as the transient error, and the instance tries again on its `RETRY POLICY` cadence, or the
host's reconnect policy under `MODE NO ACK`.
`tls_ca_file`, `tls_cert_file` and `tls_key_file` require an `amqps` address, and certificate and
key must be supplied together. A client that violates either rule is refused before commit,
naming the key and its required constraint without repeating the address or file path.

### Redis Pub/Sub

<!-- connector-matrix:ingest-redis-pubsub:start -->
```plexus,ignore
CREATE REDIS PUBSUB INGESTOR <name>
  FROM <client> (CHANNEL 'orders' | PATTERN 'orders.*')
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
| Locator and validation rule | CHANNEL 'orders' (redis_channel) \| PATTERN 'orders.*' (redis_channel_pattern) |
| Modes (complete bodies after MODE) | NO ACK |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Redis Pub/Sub carries a channel and a payload, and for a pattern subscription the pattern that matched, but no headers. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. An ingestor subscribes to a quoted channel or channel pattern and an emitter publishes to a quoted channel, each case-preserving. |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.channel: STRING OPTIONAL<br>metadata.pattern: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | messages read ahead of intake: set by subscription_buffer_messages; when full, holds back what fills it |
| Network waits | subscription: name resolution, address attempts, TLS, Redis setup and SUBSCRIBE or PSUBSCRIBE: `connect_timeout_ms`; 30s when omitted<br>idle subscription activity, including its PING probe: `idle_timeout_ms`; required |
| Driver limits | Redis closes a subscription whose unsent messages pass the server's client-output-buffer-limit for Pub/Sub clients, by default 32 MiB at once or 8 MiB held for 60 seconds; the messages it held are lost. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |

See the [REDIS PUBSUB capability and mirror reference](connector-capabilities.md#redis-pubsub-ingest).
<!-- connector-matrix:ingest-redis-pubsub:end -->

An ingestor names what it subscribes to with exactly one of `CHANNEL` and `PATTERN`.

`CHANNEL` takes a nonempty string literal of at most 1,024 UTF-8 bytes. Nervix preserves its case,
spaces and punctuation in both directions. For example, `CHANNEL 'Orders:EU/*'` subscribes to that
exact channel: the star is a literal character.

`PATTERN` subscribes to every channel a Redis glob pattern matches, through Redis's `PSUBSCRIBE`.
It takes a nonempty string literal of at most 1,024 UTF-8 bytes, which Nervix sends exactly as
written. Redis matches it byte by byte against the channel each message is published to: `*`
matches any run of bytes, `?` any one byte, `[...]` one byte of a set or range, `[^...]` one byte
outside it, and `\` makes the next byte literal. Redis accepts any text as a pattern, so a pattern
without these characters matches only the channel of that exact name. For example,
`PATTERN 'orders.*'` receives what is published to `orders.eu` and to `orders.us`, but not to
`returns.eu`. An emitter always publishes to one `CHANNEL`.

Every message exposes two typed metadata fields, both `STRING` and optional:

| Field | Value |
| --- | --- |
| `metadata.channel` | The channel the message was published to. For a `CHANNEL` subscription it is the subscribed channel. A pattern can match a channel whose name is not UTF-8 text, published by another client; that message's `metadata.channel` is null. |
| `metadata.pattern` | The pattern that matched the message. It is null for a `CHANNEL` subscription. |

For example, `SET channel = metadata.channel` records which channel delivered each message of a
`PATTERN 'orders.*'` subscription, and the same route's `WHERE channel = 'orders.eu'` then keeps
only that channel's messages. `ALTER REDIS PUBSUB INGESTOR ... SET CHANNEL '<channel>'` or
`SET PATTERN '<pattern>'` replaces the subscription, as a `SET FROM` with a complete body does, so
either moves an ingestor from a channel to a pattern or back; the ingestor then subscribes again
with the new body.

Redis Pub/Sub carries no transport headers, so `read_header` and `read_headers` are refused when
the ingestor is created.

Redis Pub/Sub has no retained backlog, so it cannot suspend honestly. A model alteration's entity
hold and a domain pause apply `ON QUIESCE` and keep the subscription open: `BUFFER` and `DROP`
both keep reading and keep the subscriber healthy. `BUFFER` retains payloads up to its byte bound,
then applies its declared `DROP OLDEST` or `DROP NEWEST` overflow policy; `DROP` discards every
payload during the hold. Discards count in `nervix_ingestor_quiesce_dropped_total`. Memory
pressure also keeps reading and sheds what arrives, as [Quiesce Modes](#quiesce-modes) describes.

An ownership handoff and a graceful shutdown do not consult `ON QUIESCE`. They stop intake by
suspending the ingestor, and suspending a Redis ingestor closes its subscription. A message published
from then until an instance subscribes again reaches no Nervix subscriber and is lost: after a
handoff, until the destination subscribes once the new schedule revision activates, and after a
failed fence or drain, until the former owner subscribes again. A payload intake had already taken
continues through its routes, while messages still waiting in the subscription's read-ahead are
discarded with it.

A `REDIS CLIENT` declares connection-pool bounds even when only ingestors reference it; see
[Database Client Connection Pools](database-client-pools.md). Each subscription owns a separate
connection, which the client's command-pool lifetimes, `pool_idle_timeout_ms` and
`pool_max_lifetime_ms`, never close. It resolves the `addr` hostname through the node's
asynchronous DNS resolver on initial subscribe and every resume after a disconnect; it does not
consume a pooled command connection. DNS and connection failures follow the ingestor's existing
retry cadence; Redis Pub/Sub does not replay messages missed while disconnected.

For `rediss://`, TLS verifies the original hostname. It trusts the host's trust store, or only
the certificates in `tls_ca_file` when the client sets it, and presents `tls_cert_file` with
`tls_key_file` as its client certificate. A plaintext `redis://` address reads none of these
files. The subscription and an emitter's command connections build TLS separately, and they
differ in one case: when part of the host's trust store cannot be read, such as an unreadable file
or a malformed certificate, the subscription logs a warning and trusts the certificates it could
read, while every command connection fails to open. `CREATE REDIS CLIENT` accepts an address with
the driver's `#insecure` fragment, but neither the subscription nor a command connection connects
with it; each reports the failure as its transient error and retries on its policy.

A Redis ingestor's client must set `subscription_buffer_messages` and `idle_timeout_ms` in its
`CONFIG`; neither key has an implicit default. The message capacity bounds read-ahead separately
from `BUFFER MAX SIZE`, which bounds retained payload bytes during quiesce. It is a positive whole
number that fits the bounded channel.
For example, choose `'subscription_buffer_messages' = '1024'` and `'idle_timeout_ms' = '30000'`
for a 1,024-message handoff and a 30-second idle interval. A full message handoff stops TCP reads;
quiesced intake still applies its declared `BUFFER` overflow policy or counts the messages `DROP`
discards. Malformed capacity values are refused at CREATE CLIENT, including after template
rendering. The ingestor still requires both keys when its plan is built. This handoff applies TCP
backpressure when full; it does not discard a message to make room. Redis Pub/Sub still offers no
replay or delivery acknowledgement, so neither buffer recovers messages missed during a disconnect.

Redis does not wait for a subscriber that stops reading. It keeps what it cannot send in the
subscriber's output buffer and closes the subscription once that passes its
`client-output-buffer-limit` for Pub/Sub clients, by default 32 MiB at once or 8 MiB held for 60
seconds. The messages it held are lost. The ingestor hands intake what it had already read, then
reports the closed subscription as a transient read failure and subscribes again on its retry
cadence. Where intake can fall that far behind its publishers, raise the server's limit or expect
that loss.

`idle_timeout_ms` is a positive whole number of schedulable milliseconds. The
subscription sends a same-connection `PING` after half that interval without activity and requires
activity within the remaining half. Activity means a complete Redis message or probe reply.
A failed probe is a transient read failure visible in
`DESCRIBE INGESTOR`, followed by the existing reconnect backoff. A healthy idle connection stays
subscribed. Time spent waiting for read-ahead room is outside an active idle read. Neither the
message capacity nor the quiesce byte bound includes kernel buffers or bytes in a partial wire
frame. Configure both bounds for the expected payload sizes.

### MQTT

<!-- connector-matrix:ingest-mqtt-topic:start -->
```plexus,ignore
CREATE MQTT TOPIC INGESTOR <name>
  FROM <client> TOPIC 'devices/+/orders'
  [INSTANCES 2 | INSTANCES PER PARTITION]
  (SESSION CLEAN | SESSION PERSISTENT)
  (QOS 0 | QOS 1 | QOS 2)
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
| Modes (complete bodies after MODE) | NO ACK<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1 or 2)<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s (requires SESSION PERSISTENT and QOS 1 or 2) |
| Options in statement order | `INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); optional; default 1<br>`SESSION CLEAN` (keyword choice); `SESSION PERSISTENT` (keyword choice); required<br>`QOS 0` (keyword choice); `QOS 1` (keyword choice); `QOS 2` (keyword choice); required |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND (requires SESSION PERSISTENT and QOS 1 or 2)<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.topic: STRING OPTIONAL<br>metadata.retain: BOOL OPTIONAL<br>metadata.qos: U8 OPTIONAL<br>metadata.duplicate: BOOL OPTIONAL<br>metadata.content_type: STRING OPTIONAL<br>metadata.response_topic: STRING OPTIONAL<br>metadata.correlation_data: BYTES OPTIONAL<br>metadata.message_expiry: U32 OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | written |
| Buffers | each raw MQTT frame, whose protocol bound every CONNECT advertises as its Maximum Packet Size; one packet is decoded before admission: at most 268435460 bytes; when full, its peer, told this capacity, discards what does not fit instead of sending it<br>each admitted publish including its packet headers, copied out of raw framing: set by max_packet_size, 10240 bytes when omitted; when full, drops and counts what does not fit<br>ACK-mode QoS 1/2 outcomes in the packet-ordered read-ahead queue: set by the Receive Maximum every CONNECT advertises, ACK PARALLEL MAX or 1 under ACK SEQUENTIAL plus 64 up to 65,535; when full, holds back what fills it<br>ACK-mode QoS 0 outcomes alongside the reserved acknowledged capacity in the same packet-ordered read-ahead queue: at most 64 outcome; when full, drops and counts what does not fit<br>NO ACK outcomes in the packet-ordered read-ahead queue: at most 64 outcome; when full, holds back what fills it<br>the next classified packet outcome held for room in read-ahead: at most 1 outcome; when full, holds back what fills it<br>a deferred or in-progress size rejection retained across reads: at most 1 discard; when full, holds back what fills it<br>SUBSCRIBE, PUBACK and PUBREC requests for the connection: set by the Receive Maximum, or 1 under NO ACK; when full, holds back what fills it |
| Network waits | connection: name resolution, TCP, TLS and CONNECT: driver default, 5 s<br>SUBACK: unbounded<br>retire a refused or downgraded subscription before retrying: driver default, 5 s<br>ordinary and oversized-discard acknowledgements written to the connection: client key `keep_alive_ms`; one interval of it, or of the broker's Server Keep Alive; 60 s when omitted<br>a silent connection: client key `keep_alive_ms`; two intervals of it, pinging after the first, or of the broker's Server Keep Alive; 60 s when omitted |
| Driver limits | A publish above the admission bound is acknowledged before it is counted as oversized; its topic, payload and properties never enter read-ahead. The raw framing bound is MQTT's protocol maximum, separate from the admission bound.<br>ACK-mode QoS 0 overflow is counted immediately and never consumes an acknowledged outcome's reserved capacity. An oversized QoS 0 publish refused here counts as overflow only. NO ACK retains backpressure. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [MQTT TOPIC capability and mirror reference](connector-capabilities.md#mqtt-topic-ingest).
<!-- connector-matrix:ingest-mqtt-topic:end -->

#### MQTT broker and client

The topic filter is a string literal; see [External Names](#external-names) for the levels and
wildcards it may hold. The broker must support MQTT 5 shared subscriptions and permit the
subscription `$share/<domain>~<ingestor>/<filter>` at the declared QoS. Every instance joins that
group, including an ingestor with only one instance. Write just the filter in `TOPIC`: Nervix
adds the share group, and a filter cannot begin with a `$share` level of its own.

The client's `addr` names the broker by its scheme, host and port alone: `mqtt://` or `mqtts://`, a
host, and a nonzero port, such as `mqtt://broker.example.com:1883`. A connection reads nothing else
from it, so `CREATE MQTT CLIENT` refuses an address that leaves out its port or writes credentials,
a path, a query or a fragment. Credentials, the keep-alive and the admitted publish size are keys
of their own, and `CREATE MQTT CLIENT` refuses any key it does not declare:

| Key | What it sets |
| --- | --- |
| `addr` | The broker: `mqtt://` or `mqtts://`, a host and a nonzero port. Required. |
| `client_id` | The identity template every connection renders, at most 65,535 bytes, not empty and without NUL; see the identities below. Omitted, each connection uses its entity's default identity. |
| `username`, `password` | The credentials every `CONNECT` carries, each at most 65,535 bytes. MQTT 5 lets a client write either alone. `DESCRIBE CLIENT` and `SHOW CREATE CLIENT` redact the password. |
| `keep_alive_ms` | The keep-alive every `CONNECT` asks for: whole seconds from 1 s through 65,535 s, written in milliseconds. `60000` when omitted. |
| `max_packet_size` | The largest complete publish, in bytes, an ingestor admits; see the oversized publishes below. From 1 through 268,435,460, `10240` when omitted. Emitters do not read it. |
| `tls_ca_file`, `tls_cert_file`, `tls_key_file` | The TLS files of an `mqtts` connection, refused with `mqtt`. An `mqtts` client requires `tls_ca_file`, its only trust root, and writes the certificate and key together. |

A broker that refuses a client's credentials, or a client that writes none to a broker requiring
them, refuses its connection. The ingestor reports the broker's reason as its transient error, such
as ``Connection refused, return code: `BadUserNamePassword` ``, and connects again on its declared
`RETRY POLICY`, or on the host's reconnect cadence in `MODE NO ACK`; it never subscribes meanwhile.

Every `CONNECT` asks the broker for the client's `keep_alive_ms`. A broker that supplies a Server
Keep Alive in `CONNACK` replaces it for that connection. The connection pings its broker after one
interval without traffic and ends when the following interval passes without an answer, and one
interval bounds each acknowledgement write. A broker-supplied zero disables the keep-alive and the
acknowledgement-write deadline derived from it; it does not make a silent connection fail within a
fixed time.

#### MQTT sessions and delivery modes

`SESSION CLEAN` starts a fresh broker session on every connection and retains no session after
disconnect. `SESSION PERSISTENT` resumes the broker session for the same client identity and
requests the maximum Session Expiry Interval (`0xFFFFFFFF`). The broker's accepted expiry,
offline-queue limits and eviction policy still govern retention. Plexus exposes no session-expiry
setting. An ingestor also accepts a retained broker session when a new owner connects without
local protocol state; the broker redelivers its unacknowledged QoS 1/2 publishes. Nervix keeps
delivery attempts and acknowledgements in memory; a persistent broker session does not persist
that local state. The [emitter's session recovery](emitters.md#mqtt) has a separate boundary.

`SESSION` and `QOS` are required, in that order before `MODE`. The legal combinations are:

| Session | QoS | Mode families | Quiesce choices |
| --- | --- | --- | --- |
| `CLEAN` | `0`, `1`, `2` | `NO ACK` | `BUFFER`, `DROP` |
| `PERSISTENT` | `0` | `NO ACK` | `BUFFER`, `DROP` |
| `PERSISTENT` | `1`, `2` | `NO ACK`, `ACK SEQUENTIAL`, `ACK PARALLEL` | `SUSPEND`, `BUFFER`, `DROP` |

Use the complete mode and quiesce bodies from the declaration above. `NO ACK` at QoS 1 or 2
still uses that QoS on the connection, but settles before graph processing; it has no in-flight
ACK window and never takes `MAX`. `SUSPEND` is valid with persistent QoS 1/2 even under `NO ACK`.

Delivery constraints:

- every statement writes its session, `SESSION CLEAN` or `SESSION PERSISTENT`, and then its quality
  of service, `QOS 0`, `QOS 1` or `QOS 2`, before `MODE`. Neither has a default, and `MODE` names
  only the acknowledgement: `NO ACK` or an `ACK` mode
- `NO ACK` has no in-flight ACK window, so it never accepts `MAX <n>`
- `ACK` modes require `SESSION PERSISTENT` at `QOS 1` or `QOS 2`. CREATE and ALTER refuse any other
  pairing, naming the session and quality of service the statement writes and underlining the
  mode, such as `MQTT TOPIC ingestors name MODE ACK SEQUENTIAL only with SESSION PERSISTENT and
  QOS 1 or 2; the statement writes SESSION CLEAN and QOS 1`. Completion after `MODE` offers only the
  modes the written session and quality of service keep, so any other pairing offers `NO ACK` alone
- `ACK PARALLEL MAX <n>` is the in-flight ACK window and `BATCH TIMEOUT` is the maximum partial-batch wait;
  a quiesce change or a stop that meets a partial batch takes in the publishes it gathered at once
  rather than after its batch timeout
- every `CONNECT` in an `ACK` mode advertises the in-flight window and 64 more as its Receive
  Maximum: 65 under `ACK SEQUENTIAL`, and `MAX` plus 64 under `ACK PARALLEL`, at most MQTT's
  65,535, so the broker sends no further QoS 1 or QoS 2 publish while that many wait on their
  acknowledgements, and a batch above 65,535 gathers at most 65,535 of them before its
  `BATCH TIMEOUT` ends it. The extra capacity lets the connection keep reading while the ingestor
  holds a batch
- `INSTANCES <count>` controls Nervix consumer parallelism within the shared subscription group

A new subscription becomes ready only when the broker grants the declared QoS. A lower grant
fails through the general error policy and `DESCRIBE INGESTOR` names the requested and granted
levels. This applies to `NO ACK` too: its QoS still determines the broker's storage and redelivery
contract. An explicit subscription refusal also fails readiness, even when the broker keeps the
empty persistent session. After either refusal, the next attempt first retires that broker session
through a clean connection, unsubscription and session expiry zero. Cleanup failure keeps retirement pending, so a
retained subscription cannot turn the rejection into a successful resume. Suspension preserves that
pending retirement. Permission granted later permits a fresh subscription attempt. An accepted
persistent session keeps its original grant across reconnects; changing the filter or QoS retires
that session.

#### MQTT metadata and user properties

Each publish's packet supplies its metadata and headers:

- `metadata.topic` is the topic the broker delivered the publish on, with the levels a `+` or `#`
  matched written out. Route on it to tell apart what one wildcard filter receives
- `metadata.retain` is the retain flag its publisher wrote, because every subscription asks for
  Retain As Published; without that option a broker clears the flag on what it forwards. A broker
  sends a shared subscription nothing it retained before, so a publish reads `true` only when its
  publisher set the flag while the subscription existed
- `metadata.qos` is the quality of service of the delivered packet: the lower of what its publisher
  sent and the subscription's `QOS`
- `metadata.duplicate` is the packet's duplicate flag, which a broker sets when it delivers a
  publish again. A publish the ingestor reads again after rejecting it is the same packet and keeps
  the value it arrived with
- `metadata.content_type`, `metadata.response_topic`, `metadata.correlation_data` (`BYTES`) and
  `metadata.message_expiry` (`U32`, in seconds) are its MQTT 5 properties. Each is null where the
  publisher set none, while an empty string, empty bytes or a zero expiry stay values. A broker
  counts the expiry down while it holds the publish, so it reads the lifetime left when the broker
  sent it, not the interval the publisher wrote. `hex_encode(metadata.correlation_data)` or
  `bytes_to_utf8(metadata.correlation_data)` explicitly converts the correlation data for a string
  field
- `read_header` and `read_headers` read its MQTT 5 user properties, in the order the publisher wrote
  them with each repeat of a name in its place. A name matches only a name spelled the same, letter
  case included

An `ON QUIESCE BUFFER` retains a publish's metadata and user properties with its payload, and its
`MAX SIZE` counts the payload alone.

#### MQTT acknowledgement and keep-alive

In an `ACK` mode an ingestor acknowledges a batch once every `PUBACK` its QoS 1 publishes ask for,
and every `PUBREC` its QoS 2 publishes ask for, is written to the connection, within one keep-alive
interval. The broker answers a `PUBACK` with nothing, and a `PUBREC` with a `PUBREL` that the
connection completes with `PUBCOMP` on its own. Nothing is written for a publish the ingestor
rejects: it delivers that publish again from its own replay, ahead of anything the broker sends
next, on the mode's `RETRY POLICY`, and writes its one acknowledgement once the publish is
accepted. Its connection keeps writing those acknowledgements and sending its keep-alive while the
ingestor waits on a batch's acknowledgement trees, however long a downstream emitter holds them, so
the broker keeps the session connected and delivers nothing again. In an `ACK` mode the read-ahead
reserves its full Receive Maximum for QoS 1/2 outcomes and holds up to 64 QoS 0 outcomes alongside
them. A QoS 0 publisher is not held back by the broker, even through a QoS 1 or QoS 2 subscription.
Nervix drops and counts QoS 0 publishes that do not fit, keeping the connection polling and
retaining the reserved capacity and acknowledgements of QoS 1 and QoS 2 publishes. Admitted
outcomes retain packet order.

Under `NO ACK` the connection acknowledges each QoS 1 or QoS 2 publish as it receives it, before
the ingestor reads it. At `QOS 2` the broker sends a publish again only until a connection has
written its `PUBREC`; after that it releases the publish once with `PUBREL`. Nervix keeps the
delivery attempts and acknowledgements of a connection in memory only, so a publish whose
acknowledgement a lost connection never wrote is delivered again once a persistent session
resumes, at QoS 2 as at QoS 1.
The per-node Prometheus counter `nervix_ingestor_read_ahead_dropped_payloads_total`, labeled by
`domain`, `ingestor` and `physical_node_id`, counts each overflow immediately, including during an
acknowledgement wait. It is separate from the transport-redelivery count in `DESCRIBE INGESTOR`.
An oversized QoS 0 publish dropped at this boundary counts as overflow only. Under `NO ACK` the
64-outcome read-ahead applies backpressure when full, which also stops its keep-alive until the
ingestor reads again. See
[MQTT ingestor connections](connector-contract.md#mqtt-ingestor-connections).

Every MQTT connection advertises the full protocol framing limit of 268,435,460 bytes in its
`CONNECT`. Nervix separately admits complete publishes of at most the client's `max_packet_size`
bytes, 10,240 when omitted, including their topic, properties and framing, so the payload must
leave room for that overhead. It drops larger publishes locally, acknowledges them when their QoS
requires it, then counts them in `nervix_ingestor_oversized_payloads_total`, visible in
`DESCRIBE INGESTOR`. They never reach the codec and do not count as decode rejections. Releasing
their broker inflight slots lets a persistent session continue delivering ordinary messages even
after many oversized publishes; a broker told the smaller bound in `CONNECT` could instead keep a
larger publish in its inflight window and stall the session. Raw framing may therefore use
substantially more memory than an admitted publish; the two bounds are declared separately above.

Cancellation retains any discard acknowledgement and its original keep-alive deadline for the
next read. An ordinary partial batch ahead of a discard is returned and acknowledged first,
preserving MQTT acknowledgement order. The following read starts the discard acknowledgement's
deadline and reports its size exactly once after that acknowledgement has been written.

#### MQTT identities, suspension and retirement

Without a `client_id` in the client configuration, one instance connects as
`<domain>~<ingestor>`. Multiple instances connect as `<domain>~<ingestor>~<instance>`, with
zero-based instance indices. A `client_id` is a template: its `{{instance}}` placeholder, however
its braces are spaced, renders the zero-based index of the instance that connects, and an emitter
that shares the client connects as instance 0. Each instance must render an identity of its own, so
an ingestor with more than one instance names `{{instance}}` in its `client_id`. Instances whose
identities render alike never subscribe: each reports
`MQTT client_id '<identity>' is shared by <count> instances` as its transient error. An explicit
identity replaces the whole default, including its domain prefix: choose identities unique to each
live connection on the same broker, across domains too. Identities remain stable across node
ownership changes.

`DROP INGESTOR` releases the broker sessions, including after `STOP`. Nervix stops the ingestor,
opens a clean cleanup connection for each distinct identity, sends `UNSUBSCRIBE` for its filter,
and sends `DISCONNECT` with session expiry zero. Cleanup is best-effort: an unreachable broker is
reported and does not prevent the drop. Its retained state can remain until the broker expires it
or the operator clears it.

Changing the input with `ALTER MQTT TOPIC INGESTOR`, by `SET FROM ...` or by an operation such as
`SET TOPIC`, retires the preceding filter and prepares the changed sessions at the new endpoint before the replacement starts. A failure to
clean up the preceding endpoint does not prevent moving to a reachable new endpoint. Sessions
whose broker, identity, filter, session mode and QoS still match are retained, including across
an instance-count change. Changing a filter discards the retired filter's broker queue.
Ordinary `STOP`, reconnect and ownership transfer retain persistent broker sessions.

`SUSPEND` is valid only with `SESSION PERSISTENT` at `QOS 1` or `QOS 2`. CREATE refuses any other
pairing with `MQTT ON QUIESCE SUSPEND requires SESSION PERSISTENT and QOS 1 or 2`, underlining
`SUSPEND`. An unsupported QoS number is refused with the supplied value and the accepted ingestor
levels, such as `MQTT QOS must be 0, 1 or 2, found 3`. Completion offers only the
session's valid quiesce choices.

`SUSPEND` disconnects while asking the broker to
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

NATS has two ingestor entities, one for each way a NATS server hands out messages, and both read
through a `NATS CLIENT`. `NATS SUBJECT` subscribes to a subject through Core NATS as a member of a
queue group: the server hands each message to one member of the group as it arrives and retains
nothing, so the entity reads in `MODE NO ACK` and quiesces with `BUFFER` or `DROP`. `NATS STREAM`
reads a JetStream stream through a durable pull consumer: the stream stores what is published to
its subjects and keeps it for as long as its operator's retention policy and limits allow, and the
consumer keeps the position, so the entity acknowledges each delivery in `MODE ACK SEQUENTIAL` or
`MODE ACK PARALLEL` and can also quiesce with `SUSPEND`. A stream stores a Core publication to one
of its subjects as it stores a JetStream one, and a subscription receives a JetStream publication
to its subject as it receives a Core one, so either entity reads what either
[NATS emitter](emitters.md#nats) publishes to a subject it reads.

<!-- connector-matrix:ingest-nats-subject:start -->
```plexus,ignore
CREATE NATS SUBJECT INGESTOR <name>
  FROM <client> SUBJECT 'orders.created'
  (QUEUE GROUP 'workers')
  (INSTANCES 2 | INSTANCES PER PARTITION)
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
| Options in statement order | `QUEUE GROUP 'workers'` (string literal checked by nats_queue_group); required<br>`INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); required |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.subject: STRING OPTIONAL<br>metadata.reply: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | messages the subscription holds ahead of the host: driver default, 65,536 messages; when full, drops what does not fit without counting it |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>the subscription's flush: unbounded<br>a silent connection: driver default, a ping every 60 s; the connection ends after 2 go unanswered |
| Driver limits | None declared. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [NATS SUBJECT capability and mirror reference](connector-capabilities.md#nats-subject-ingest).
<!-- connector-matrix:ingest-nats-subject:end -->

`SUBJECT` and `QUEUE GROUP` are string literals Nervix subscribes with exactly as written, letter
case included, within the [external name rules](#external-names). A subscription subject may use
the NATS wildcards as whole tokens: `SUBJECT 'orders.*.EU'` matches one token in the middle, and
`SUBJECT 'orders.>'` every remaining token, so `>` comes last. `metadata.subject` is the concrete
subject of the delivered message, including when the subscription uses a wildcard.
`metadata.reply` is its reply subject, or `NULL` when absent. Both have type `STRING`.
Header reads retain the wire order, including interleaved repeated names, with exact name matching.

`NATS SUBJECT` ingestors use Core NATS queue subscriptions. `QUEUE GROUP` and `INSTANCES`
are mandatory. `INSTANCES <n>` creates that many queue-group members across the cluster; use
`INSTANCES 1` for one member. `INSTANCES PER PARTITION` creates one member for each partition index,
placed with the corresponding partitions of its destination relays, under the
[Instancing](#instancing) rules. Every member subscribes to the same subject and queue group;
Core NATS chooses the member that receives each message.

The NATS client accepts one optional authentication key: `token`, `nkey_seed` for a user NKey
seed, or `credentials_file` for a NATS user credentials file containing its JWT and signing seed.
These three keys are mutually exclusive; `CREATE NATS CLIENT` refuses any pair before connecting.
Mount a resource containing a credentials file and write its resolved path, such as
`'credentials_file' = '{{identity}}/user.creds'`. All three keys are confidential in public
inspection. URL user information remains delegated to the NATS driver.

Ingestors and emitters use the same authentication and TLS options. A `tls://` address requires
TLS on every connection the client makes, reconnections and servers the cluster announces
included, whichever direction it serves; a `nats://` address uses TLS only when the server
announces that it requires it. The client verifies the server's certificate against the
platform's native roots, or only against `tls_ca_file` when it is set, and presents
`tls_cert_file` with `tls_key_file` as its client certificate.
An unreadable or malformed credentials file is a configuration failure; a broker refusing the
identity is a connection failure reported by the boundary's existing general error policy and
retry cadence. Nervix does not create accounts or users on the broker.

Core NATS retains no backlog, so `SUSPEND` is unavailable to `NATS SUBJECT`. A quiesced instance remains a member of
its queue group and keeps receiving its share; that share is buffered or discarded rather than
shifted to other members.

A lost connection to the server ends the instance's subscription, and so does a server that stops
answering the client's pings, once two of them go unanswered. Messages the client received before
the loss are still delivered. `DESCRIBE INGESTOR` shows the loss as its transient error,
`NATS connection lost`, and then each failed attempt to connect and subscribe again, such as
`failed to connect NATS client`, with the delay before the next attempt as its reconnect backoff.
The instance connects and subscribes again on the host's reconnect cadence: the delay starts at
250 ms and doubles after each failure up to 30 s. An instance that cannot connect when the ingestor
starts does not fail the start; it reports the failure and tries again on the same cadence. Once
the instance has subscribed again, `DESCRIBE INGESTOR` shows `ready: true`, while the last transient
error and its backoff stay until the instance takes in its next message, which also returns the
delay to 250 ms. Core NATS retains nothing for a subscription that is not connected, so messages
published while the instance reconnects never reach it.

<!-- connector-matrix:ingest-nats-stream:start -->
```plexus,ignore
CREATE NATS STREAM INGESTOR <name>
  FROM <client> STREAM 'orders'
  (CONSUMER 'nervix')
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
| Locator and validation rule | STREAM 'orders' (nats_stream) |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 8 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `CONSUMER 'nervix'` (string literal checked by nats_consumer); required |
| Headers | `read_header`, `read_headers`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.subject: STRING OPTIONAL<br>metadata.reply: STRING OPTIONAL<br>metadata.stream: STRING OPTIONAL<br>metadata.consumer: STRING OPTIONAL<br>metadata.sequence: U64 OPTIONAL<br>metadata.timestamp: DATETIME OPTIONAL<br>metadata.delivered: U64 OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | answered |
| Buffers | deliveries a pull request brought ahead of the host: set by ACK PARALLEL MAX, 1 under ACK SEQUENTIAL; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>the consumer lookup: driver default, 5 s request timeout<br>the answer to an acknowledgement or a negative acknowledgement: driver default, 10 s request timeout<br>a pull request, and a silent connection: driver default, a ping every 60 s; the connection ends after 2 go unanswered |
| Driver limits | A pull request asks for at most the consumer's MaxRequestBatch and waits at most its MaxRequestExpires, where its operator set them. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [NATS STREAM capability and mirror reference](connector-capabilities.md#nats-stream-ingest).
<!-- connector-matrix:ingest-nats-stream:end -->

`NATS STREAM` reads a JetStream stream through one durable pull consumer of it, and both belong to
an operator: Nervix creates neither. `STREAM` and `CONSUMER` are string literals Nervix names
exactly as written within the [external name rules](#external-names). This entity runs one instance
and accepts no `INSTANCES` clause.

The instance connects, looks the consumer up, and refuses a consumer it cannot read as it declares:
a push consumer, which delivers to a subject of its own, and a consumer whose acknowledgement policy
is not explicit, which would not wait for each message's own acknowledgement. A stream or consumer
that does not exist is reported by name as the ingestor's transient error, such as `NATS consumer
'nervix' of stream 'orders' does not exist`, and the instance looks again on its `RETRY POLICY`
until an operator provisions it.

The instance pulls its deliveries. One pull request at a time asks for as many deliveries as the
delivery mode lets wait on their acknowledgement, one under `ACK SEQUENTIAL` and `MAX` under
`ACK PARALLEL`, and at most the consumer's `MaxRequestBatch` where its operator set one. A pull
request waits without expiry, or for the consumer's `MaxRequestExpires` where its operator set one,
after which the next request asks again. A delivery is acknowledged once every route and error
delivery its records unfolded into has completed, and the acknowledgement is a request the server
answers once it recorded it. A delivery the ingestor rejects, because its `ACK TIMEOUT` passed or a
record of it failed, is negatively acknowledged, so the consumer delivers it again at once and the
ingestor reads it again on its `RETRY POLICY`. Where the consumer's operator set a `MaxDeliver`, it
bounds how often the consumer delivers a message, and JetStream stops delivering one that reached
it.

`SUSPEND` lets the instance's pull go: the instance unsubscribes the inbox its pull requests
deliver to, so the consumer delivers nothing to it while it is suspended. What is published
meanwhile waits in the stream for the consumer, unless the stream's own limits, such as a maximum
age, discard it first. A delivery the instance had received but not yet read when it suspends is
negatively acknowledged, so the consumer delivers it again once the instance resumes, and one
still on its way is delivered again once the consumer's `AckWait` passes. Under `BUFFER` and `DROP` the instance keeps pulling while it is quiesced and
acknowledges each delivery once the host buffered or dropped it, as every acknowledged ingestor does
in those modes.

The consumer's position is the stream's: an instance that resumes, on its own node or on another one
after its owner stopped, continues after the last delivery it acknowledged. A delivery an instance
received and had not acknowledged when its connection was lost is delivered again once the
consumer's `AckWait` passes. A delivery carries the headers it was published with; its stream and
consumer identity, stream sequence, timestamp and delivery count are exposed as typed metadata.
Stream messages expose `metadata.subject` and `metadata.reply` as well as `metadata.stream` and
`metadata.consumer` (`STRING`), `metadata.sequence` and `metadata.delivered` (`U64`), and
`metadata.timestamp` (`DATETIME`). The sequence is the stream sequence; the delivery count is the
number of attempts to deliver this message. The timestamp is when the server accepted the
publication and can supply `TIMESTAMP AT metadata.timestamp`. A stream delivery's reply subject
is the server's `$JS.ACK` acknowledgement subject. Header reads preserve the received order.
Delivery metadata is decoded only where a program reads it. A malformed requested field, a
nonpositive sequence or delivery count, or a timestamp outside Nervix's signed Unix-nanosecond
range refuses that row through `ON MESSAGE ERROR`, after `FILTER WHERE` and before event-time
selection; none becomes a null or a zero. Unread metadata does not change settlement through the
delivery's opaque acknowledgement subject. Buffering retains each field's typed value or failure.

A lost connection ends the instance's pull as it ends a `NATS SUBJECT` instance's subscription, and
`DESCRIBE INGESTOR` shows it as the transient error `NATS connection lost` until the instance has
connected, looked the consumer up and pulled again on its `RETRY POLICY`.

`ALTER NATS STREAM INGESTOR` changes one part of the stored body in place, as
[Altering Ingestors](#altering-ingestors) describes: `SET STREAM '<stream>'`,
`SET CONSUMER '<consumer>'` and `SET MODE` write the parts this entity declares, beside `SET CLIENT`
and `SET QUIESCE`. A position belongs to its consumer, so after `SET STREAM` or `SET CONSUMER` the
ingestor continues from the position of the consumer it now names, and the consumer it left keeps
its own position, with everything published to its stream since pending for it.

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
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | Not offered: The driver exposes no peer identity on receive. Only frame 0 is ingested, with no broker position or typed transport metadata namespace. |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the frame a peer is sending: unbounded |
| Network waits | connecting, retried without limit: unbounded<br>the ZMTP handshake: unbounded<br>receiving the next message: unbounded |
| Driver limits | The driver has no high-water mark, maximum message size or heartbeat. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [ZEROMQ PULL capability and mirror reference](connector-capabilities.md#zeromq-pull-ingest).
<!-- connector-matrix:ingest-zeromq-pull:end -->

The ingestor is a PULL socket on the client's `addr`, a `tcp://<host>:<port>` or `ipc://<path>`
endpoint. With `'bind' = 'true'` the socket binds that address and pushing peers connect to it;
with `'bind' = 'false'`, or when `bind` is omitted, it connects to a peer bound there. `bind`
accepts only `true` or `false`, ignoring letter case; any other value is refused at `CREATE CLIENT`.
`CREATE CLIENT` also checks `addr` with the socket driver's TCP/IPC endpoint parser before storing
the configuration. Rendered address templates pass that same check before commit; it opens no
socket and performs no name lookup. For a TCP hostname, the ZeroMQ driver's connection uses the
system resolver, not the node's configured resolver; see [Name Resolution](name-resolution.md#residual-resolution).
Every value of the client's `CONFIG` can name its pinned `MOUNT`. The ingestor's plan resolves
those values and keeps the mount for as long as the ingestor runs; see
[Client Config Mounts](resources.md#client-config-mounts). A node that cannot provide the mount or
render its values cannot start the ingestor, and `DESCRIBE INGESTOR` shows that failure as its
transient error, naming the entry or the resource. A refusal during pre-commit validation is
returned by `CREATE CLIENT` instead.

Each received message contributes only frame 0 as the payload. Additional multipart frames are
discarded; a message with no frames is skipped. A present but empty first frame is still a payload
and follows the configured codec and message-error policy. ZeroMQ supplies neither transport
headers nor peer identity or a position to acknowledge.

The ingestor reads no deadline key: a `ZEROMQ` client's `timeout_ms` and `connect_timeout_ms` bound
only an emitter's waits. A connecting socket connects when the ingestor starts. The driver retries a
refused connection on its own, without limit and at most about five seconds apart, and waits for
the peer's ZMTP handshake without a deadline. Until a peer accepts, `DESCRIBE INGESTOR` shows the
ingestor not ready, and the refused attempts are not reported as a transient error. Any other
failure to bind or connect, such as another socket holding the address or a host name that does
not resolve, is the ingestor's transient error. The ingestor then opens a new socket on the host's
reconnect cadence: the delay starts at 250 ms, doubles after each failed attempt up to 30 s, and
`DESCRIBE INGESTOR` shows it as the reconnect backoff. A stop or a quiesce change ends a connection
attempt that is still waiting.

The driver has no heartbeat and never reconnects. A bound socket accepts every peer that connects,
including one that reconnects after its connection ended. When several peers push to it, the driver
can leave a message one of them already delivered unread until some peer sends again. A connecting
socket keeps the one
connection it made: when the peer closes it or goes away, the driver reports nothing and does not
connect again, so the ingestor stays ready and receives nothing more until it is started again by
an alteration, a move to another node or a restart of its node. Where a peer can restart, bind the
ingestor's socket and let the pushing peers connect to it. The driver also understands no ZMTP
command after the handshake, so it closes the connection of a peer that sends a heartbeat at the
first one; leave heartbeats off on the peers. A message still in a connection when it ends is lost.

`SUSPEND` keeps the socket open but unread, so transport buffers fill and the peer's own
high-water-mark policy determines whether sends queue, block, or drop. Buffered transport data is
read on resume. `BUFFER` and `DROP` keep reading locally.

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
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer socket address, IP without its port, transport and optional verified TLS client subject; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.peer_addr: STRING OPTIONAL<br>metadata.transport: STRING OPTIONAL<br>metadata.tls_peer: STRING OPTIONAL<br>metadata.remote_ip: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | yes |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | UDP datagrams admitted after a fixed 65,535-byte receive scratch buffer: set by max_message_size; when full, drops and counts what does not fit<br>TCP and TLS frame assembly across unlimited connections (max_message_size + 11 bytes per connection; an oversized frame closes only that connection): unbounded<br>completed TCP and TLS frames waiting for the intake queue (one frame per connection, at most max_message_size bytes; unlimited connections): unbounded<br>frames read from TCP and TLS connections: set by node capability syslog-intake-queue-capacity; when full, holds back what fills it |
| Network waits | the TLS handshake of an accepted connection: `connect_timeout_ms`; 30s when omitted<br>reading a connection: `idle_timeout_ms`; unbounded when omitted |
| Driver limits | Datagrams wait in the kernel's UDP receive buffer, which drops what does not fit. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |

See the [SYSLOG LISTENER capability and mirror reference](connector-capabilities.md#syslog-listener-ingest).
<!-- connector-matrix:ingest-syslog-listener:end -->

The client binds a UDP, TCP, or TLS listener. UDP treats one datagram as one message; TCP detects
RFC 6587 octet-counted or LF-terminated framing per frame unless the client selects one with
`framing`; TLS uses RFC 5425 octet counting.
Messages expose optional `STRING` fields `metadata.peer_addr`, `metadata.transport` (`udp`, `tcp`
or `tls`) and `metadata.tls_peer` (the verified TLS client certificate's subject, null without a
verified client identity). Routes may select a transport in `WHERE` and assign these values to
schema fields; quiesce buffering retains the complete row. The ingestor takes no `INSTANCES` clause
and has no application acknowledgment. Every live cluster node runs the listener; it is independent
of the leader and restarts or joins with its owning node. See [Syslog](syslog.md) for the client keys,
cluster lifecycle, framing, TLS, limits, and failure semantics.
A client key the listener does not read, a host name in its `addr` or a TLS client without a server
identity refuses the statement that binds the client. A TLS file that holds no usable certificate or
key fails listener startup with the file and its underlying cause. A frame or connection failure
keeps its cause in the ingestor
diagnostic. These failures do not acknowledge a message; Syslog has no application ACK.

### SQS

<!-- connector-matrix:ingest-sqs-queue:start -->
```plexus,ignore
CREATE SQS QUEUE INGESTOR <name>
  FROM <client> QUEUE 'orders'
  [INSTANCES 2 | INSTANCES PER PARTITION]
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
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>ACK PARALLEL MAX 10 BATCH TIMEOUT 100ms ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `INSTANCES 2` (positive count); `INSTANCES PER PARTITION` (keyword choice); optional; default 1 |
| Headers | `read_header`, `read_headers`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |
| Connector events | None declared. |
| Instancing | per_partition |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND |
| Metadata | metadata.message_id: STRING OPTIONAL<br>metadata.sent_at: DATETIME OPTIONAL<br>metadata.receive_count: U32 OPTIONAL<br>metadata.first_received_at: DATETIME OPTIONAL<br>metadata.group_id: STRING OPTIONAL<br>metadata.deduplication_id: STRING OPTIONAL<br>metadata.sequence_number: STRING OPTIONAL<br>metadata.sender_id: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | broker_redelivered |
| Acknowledgement confirmation | answered |
| Buffers | messages gathered for one host read: set by 1 under ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 10; when full, holds back what fills it |
| Network waits | gathering further messages after the first under ACK PARALLEL; cancellation hands over every message already gathered: `BATCH TIMEOUT`<br>connection: name resolution, TCP and TLS: driver default, 3.1 s<br>the queue URL lookup, a deletion, a batch deletion and a batch visibility change: `timeout_ms`; unbounded when omitted<br>a receive, after the long poll of at most one second it asks the service to hold it for: `timeout_ms`; unbounded when omitted |
| Driver limits | A request that stalls below 1 byte a second for 5 s is cancelled. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [SQS QUEUE capability and mirror reference](connector-capabilities.md#sqs-queue-ingest).
<!-- connector-matrix:ingest-sqs-queue:end -->

`QUEUE` takes a string literal that SQS accepts as a queue name: 1 to 80 ASCII letters, digits,
`-` and `_`, where a FIFO queue's name ends in `.fifo` and the suffix counts toward the 80. Nervix
preserves its case in both directions, so `QUEUE 'Orders-EU_1.fifo'` reads exactly that queue. The
ingestor reads a message's attributes through `read_header` and `read_headers`, each value as text:
a `String` or `Number` attribute's value exactly as SQS carries it, and a `Binary` attribute's bytes
as UTF-8, with each invalid sequence replaced by U+FFFD, so binary data that is not UTF-8 text does
not survive the read. It names `MODE ACK SEQUENTIAL` or `MODE ACK PARALLEL MAX <n> ...`, and
`ON QUIESCE SUSPEND`, the delivery and quiesce modes it declares: the queue keeps each message until
the ingestor deletes it, so suspending loses nothing. An `ACK PARALLEL` window holds 1 to 10 messages, the most one SQS receive
returns, and `CREATE` refuses a larger one, as in `SQS ACK PARALLEL MAX must be between 1 and 10,
found 11`.

The `SQS CLIENT` an ingestor or emitter names declares `endpoint`, `region`, `access_key_id` and
`secret_access_key`, which are required, and the optional `session_token`, `timeout_ms` and
`tls_ca_file`. `CREATE SQS CLIENT` refuses any other key and a client that omits a required one.
Every request is signed for `region` with `access_key_id` and `secret_access_key`, and carries
`session_token` when those are temporary credentials. `SHOW CREATE CLIENT` and `DESCRIBE` print the
three credentials as `********`. `timeout_ms` bounds every request of both directions, name
resolution, connection setup, TLS and the answer included: the queue lookup of an ingestor or an
emitter, each deletion, batch deletion and batch visibility change, each send, and each receive
once its long poll of at most one second has passed, because a service with no message holds a
receive for its long poll before it answers. Without `timeout_ms` those waits are unbounded, though
connecting, its lookup included, still ends at the SDK's 3.1-second connect timeout.
`CREATE SQS CLIENT` reads `tls_ca_file` before it commits the client, and refuses a file holding a
section that is not a certificate, or no certificate at all, as
`invalid client config key 'tls_ca_file'`: a client that trusted it alone could reach no service.

Each receive requests SQS system attributes separately from the message attributes that
`read_header` and `read_headers` read. Routes can read the following optional, non-sensitive
metadata fields:

| Field | Type | Value |
| --- | --- | --- |
| `metadata.message_id` | `STRING` | The service's message identifier, retained across redelivery; this is not the receipt handle used to delete a delivery. |
| `metadata.sent_at` | `DATETIME` | SQS `SentTimestamp`, converted exactly from Unix milliseconds. |
| `metadata.receive_count` | `U32` | SQS `ApproximateReceiveCount`: receives across queues before deletion, including this receive. |
| `metadata.first_received_at` | `DATETIME` | SQS `ApproximateFirstReceiveTimestamp`, converted exactly from Unix milliseconds and retained across redelivery. |
| `metadata.group_id` | `STRING` | `MessageGroupId`, when supplied by the service. |
| `metadata.deduplication_id` | `STRING` | `MessageDeduplicationId`, when supplied by the service. |
| `metadata.sequence_number` | `STRING` | SQS's FIFO sequence number, preserved as text without narrowing its range. |
| `metadata.sender_id` | `STRING` | The service's sender identifier. |

A field the service omits is null. Invalid numeric system attributes fail the read with a typed
error naming the attribute; they never become zero or null. Timestamps must fit Nervix's signed
Unix-nanosecond range. `TIMESTAMP AT metadata.sent_at` selects the service's send time for event
admission and downstream windows. A route may use `WHERE metadata.receive_count >= (2 AS U32)`
to direct redeliveries to a retry relay. The values remain attached to their exact message through
ingest grouping and an in-flight receive completed during quiesce.

Starting the ingestor sends no request to the service. Each ingestor instance looks its queue up
before its first poll and keeps the queue's URL from then on, suspensions included. The ingestor
resolves the host of its client's `endpoint` through the node's asynchronous resolver each time it
opens a connection, and dials the answers as [Dialling The
Answers](name-resolution.md#dialling-the-answers) describes for Smithy's client. Every request is
still signed for the configured host, and over HTTPS the service certificate must name that host.
Without `tls_ca_file` the client trusts the platform's native roots; with it, the client trusts that
CA alone. Either way it connects directly: an SQS client takes no proxy, and ignores the
`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY` environment variables of its node. A missing
name, a silent name server, an unreachable answer or a service that does not answer within
`timeout_ms` fails the queue lookup, a poll, or a deletion. The AWS SDK makes each request once, so
a failed lookup or poll is at once a transient ingestor failure that `DESCRIBE INGESTOR` shows, such as
`the SQS queue lookup did not finish within timeout_ms 5s`. The ingestor host schedules retries
using the ingestor's `RETRY POLICY`, so an ingestor started while the service is unreachable
consumes once the service answers. A queue that does not exist fails the
lookup with its name, such as `SQS queue 'orders' does not exist`, and is retried the same way;
Nervix never creates it.

Under `ACK SEQUENTIAL` each poll is a `ReceiveMessage` for one message with all of its attributes,
which the service answers as soon as a message arrives or after a one-second wait without one.
Under `ACK PARALLEL MAX <n>` the poll asks for `n` messages, and the service may answer with fewer
than the queue holds. Once the first message arrives the ingestor sends further receives for the
messages its batch still lacks, until the batch holds `n` or its `BATCH TIMEOUT`, counted from that
first message, has passed: a receive waits up to a second while a whole second of the window
remains and does not wait after that, and a receive that did not wait and brought nothing ends the
batch. The batch's messages then wait on their acknowledgements together. The service hides every
message it returns from every other receive until the queue's visibility timeout ends, and the
ingestor deletes a batch's messages only once the batch is acknowledged, with one
`DeleteMessageBatch` request for each ten of them and one `DeleteMessage` request for a message
left alone, as every `ACK SEQUENTIAL` batch is, so a failed lookup or poll never removes a message
from the queue.

A batch whose acknowledgement fails or does not arrive within the mode's `ACK TIMEOUT` is rejected:
one `ChangeMessageVisibilityBatch` request for each ten of its messages makes them visible again at
once, and the ingestor polls again after its `RETRY POLICY` delay, so it receives them again then
instead of after the queue's visibility timeout. Under `INSTANCES` greater than one, another
instance can receive them sooner. A rejected message whose receipt handle no longer holds it
invisible needs no change, because it was deleted or its visibility timeout already ended. A
deletion that fails, or one the service does not carry out for a message, is a transient error that
`DESCRIBE INGESTOR` shows, and the batch is then rejected in the same way, so its messages are
received again, as duplicates if the graph had already processed them. A visibility change that
fails is a transient error too; the ingestor does not repeat it, and such a message becomes visible
again once its visibility timeout ends. Deleting needs the client's credentials to allow
`sqs:DeleteMessage`, and rejecting needs `sqs:ChangeMessageVisibility`, so credentials without the
latter fail each rejection this way. A queue whose visibility timeout is shorter than the mode's
`ACK TIMEOUT` can also hand a message that still awaits its acknowledgement to another receive. A
message the service returns without the receipt handle that settles it, or with a system attribute
that fails its check, stays out of the batch and fails the next poll with a typed error, after the
batch's other messages reach the relay; it becomes visible again once its visibility timeout ends.

Suspension stops polling. Messages wait in the queue only for its configured retention period, and
the ingestor resumes by polling past anything the service expired. A poll still waiting when the
ingestor quiesces or stops is never abandoned, because the service hides the message it answers
with until its visibility timeout ends: the ingestor sends no further poll and waits for that answer,
which a responsive service gives within the poll's wait of at most one second, and the messages it
brings dispatch, with those its batch had already gathered, before the ingestor suspends or closes.
Only a service that has not answered within 10 seconds of the quiesce change or the stop, or within
the poll's `timeout_ms` after its long poll when that ends sooner, ends that poll unanswered, which
`DESCRIBE INGESTOR` shows as a transient error; a message the poll was returning then stays
invisible until its visibility timeout ends.

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
| Headers | Not offered: An HTTP response's headers describe a whole answer rather than one of its samples, and each sample's labels travel in its payload under metric, so a query ingestor reads no transport headers. |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | SUSPEND<br>BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST |
| Metadata | metadata.query: STRING OPTIONAL<br>metadata.evaluated_at: DATETIME OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | the query response: set by MAX BODY SIZE; when exceeded, fails the poll; the next poll reads again<br>samples the host takes in as one intake batch: at most 1024 samples; when full, holds back what fills it |
| Network waits | a query: name resolution, connection, TLS and the response: `timeout_ms`; required |
| Driver limits | HTTP/1.1 response heads above 100 fields or 417,792 bytes, and HTTP/2 header lists of 16,384 bytes or more as HTTP/2 counts them, are refused. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [PROMETHEUS QUERY capability and mirror reference](connector-capabilities.md#prometheus-query-ingest).
<!-- connector-matrix:ingest-prometheus-query:end -->

A `PROMETHEUS QUERY` ingestor evaluates one PromQL instant query on a cadence of domain time,
through a `PROMETHEUS CLIENT`, and delivers one JSON object for each float sample of the answer. One
scheduled node runs it. Every query asks for its own instant, so `NO ACK` is its only mode: nothing
acknowledges a sample, and an instant no query read stays a gap in the series.

```plexus
CREATE SCHEMA sample ( job STRING, value F64, timestamp STRING );
CREATE SCHEMA evaluated_sample (
  job STRING,
  value F64,
  timestamp STRING,
  evaluated_at DATETIME OPTIONAL
);
CREATE JAQ JSON CODEC sample_codec TO SCHEMA sample
  WITH JAQ TRANSFORMATIONS ON INGESTION '{job: .metric.job, value, timestamp}';
CREATE RELAY samples SCHEMA evaluated_sample UNBRANCHED;

CREATE PROMETHEUS CLIENT prom_main
  CONFIG {
    'addr' = 'https://prometheus.example.com/monitoring',
    'timeout_ms' = 5000
  };

CREATE PROMETHEUS QUERY INGESTOR node_up
  FROM prom_main
  QUERY 'up{job="node"}'
  EVERY 15s
  MODE NO ACK
  ON QUIESCE SUSPEND
  DECODE USING sample_codec
  TIMESTAMP AT metadata.evaluated_at
  TO samples INHERIT ALL SET evaluated_at = metadata.evaluated_at
  UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

#### Prometheus Samples

A query ingestor reads the `vector` of float samples an instant query answers. It delivers one object
per sample: what the API's `vector` result holds for the sample, with the sample's instant written
out as text.

```json
{"metric":{"__name__":"up","instance":"node-1:9100","job":"node"},"value":1,"timestamp":"2026-04-07T00:00:00.123Z"}
```

- `metric` holds the series' labels in ascending name order, its `__name__` among them where the
  series has one. A label named `value` or `timestamp` stays under `metric`, beside the sample's own.
- `value` is a JSON number: an integer the API writes as an integer stays that integer, and any other
  value is the shortest text of the same float. JSON has no number for `NaN`, `+Inf` or `-Inf`, so
  each travels as the string the API writes it as, for the codec to judge per record. A `string`
  wire field reads it, and a `number` wire field refuses that record as a
  [decode rejection](#when-the-codec-refuses-a-payload), while the samples beside it are delivered.
- `timestamp` is the sample's own instant as RFC 3339 text in UTC, read from the digits of the API's
  decimal seconds so that no float rounds them. Prometheus reports it to the millisecond.

`WIRE JSON` declares no nested object, so a codec that reads a label is a JAQ codec, as in the
example above. A codec that reads only the sample's own members can stay a `MODE LOOSE` wire schema,
which drops `metric`:

```plexus
CREATE SCHEMA sample_value ( value F64 );
CREATE WIRE JSON SCHEMA sample_wire MODE LOOSE ( value number );
CREATE WIRE JSON CODEC value_codec FROM WIRE SCHEMA sample_wire TO SCHEMA sample_value;
```

A route reads what the sample answers through its metadata. `metadata.query` is the statement's
`QUERY` as written, and `metadata.evaluated_at` is the instant the poll asked the API to evaluate it
at, the due instant of the ingestor's cadence, which `TIMESTAMP AT metadata.evaluated_at` takes as
each sample's event time.

#### Prometheus Cadence

The first query becomes due after one `EVERY` interval, where an `HTTP REQUEST` polls at once. Later
queries stay on that anchored schedule of the domain clock. Nervix sends each due instant as the
query's evaluation time, so a paced domain queries the instant its own clock has reached rather
than the present. A query slower than `EVERY` coalesces the occurrences it missed into one query at
the newest due instant, and returned samples are evaluated with a fresh domain execution snapshot.

#### When A Prometheus Poll Fails

A poll that cannot read its answer fails as a whole, before any sample of the answer is delivered.
`DESCRIBE INGESTOR` shows the failure as the ingestor's transient error, in the form
`transient error: failed to read from prometheus ingestor: <failure>`, and every attached session
receives it as a server error. The next query that succeeds clears it. A failed poll is not sent
again: the next query runs at the next due instant, so the failed instant stays a gap in the series.

| What the poll met | `<failure>` |
| --- | --- |
| A `matrix`, `scalar` or `string` result, which a query ingestor does not read | `Prometheus query returned unsupported result type 'matrix'` |
| A `vector` that holds a native histogram | `Prometheus query returned unsupported result type 'native histogram'` |
| A query the API refuses, in the API's own words | `Prometheus rejected the query with HTTP status 400 Bad Request: bad_data: invalid parameter "query": 1:5: parse error: unclosed left parenthesis` |
| Any other status whose body is not the API's error, such as a proxy's page or a redirect | `Prometheus query failed with HTTP status 503 Service Unavailable` |
| A response above `MAX BODY SIZE` | `Prometheus query response exceeds MAX BODY SIZE 16MiB` |
| A successful status whose body is not the API's answer | `failed to decode Prometheus query response` |
| A request that could not be sent, or had no answer within `timeout_ms` | `failed to send Prometheus query`, then the transport's cause |
| A response body that ended early or outlasted `timeout_ms` | `failed to read Prometheus query response`, then the transport's cause |

No failure names the request's URL, which carries the PromQL, the instant and any credentials `addr`
holds.

One sample can also fail alone, when its value is neither a number nor one of the API's non-finite
strings, or its instant does not fit a timestamp. Prometheus writes neither, though another server
that answers the same API can. That sample is left out and reported to every attached session as a
server error, and the samples beside it are delivered.

#### Prometheus Response Bound

`MAX BODY SIZE <bytes>`, written between `QUERY` and `EVERY`, bounds how many bytes of one query
response a poll reads; a statement that leaves it out reads at most `16MiB`. The size is a whole
number greater than zero followed by `B`, `KB`, `KiB`, `MB`, `MiB`, `GB`, `GiB`, `TB` or `TiB`, and
`SHOW CREATE` renders it in the unit it was written in. The bound counts the bytes of the
response's JSON: a response the server compressed is counted as it is decompressed, so the bound
limits the JSON a poll reads rather than the size of its transfer. A response whose announced
length or whose body exceeds the bound fails its poll.

`DESCRIBE INGESTOR` prints the bound in effect, whether the statement wrote it or not, in
`options: QUERY 'bulk' MAX BODY SIZE 16MiB EVERY 1s`. `DROP MAX BODY SIZE` restores that bound,
and so does a `SET FROM` whose new body leaves `MAX BODY SIZE` out, because it replaces the whole
body.

#### Prometheus Intake And Quiesce

The samples of one response enter the graph in intake batches of at most 1024 samples, and each
batch is decoded and routed as its own group, so a large vector is never taken in as one batch of
any size. The remaining samples wait while a batch is routed, and the next query waits until every
sample of the current one has entered the graph or, while a `BUFFER` quiesce holds the ingestor,
has been handed to its buffer. A query whose `ON INGESTION` unfolding finds the node's extension
workers full waits for them; see [When The Extension Workers Are Full](#when-the-extension-workers-are-full).

`SUSPEND` skips queries and leaves a gap in the series. `BUFFER` keeps querying at the declared
cadence and delivers retained results in order after resume. It retains each intake batch as one
payload: `MAX SIZE` counts the bytes of its samples' JSON, `ON OVERFLOW` drops whole batches, and
`nervix_ingestor_quiesce_buffered_records` and `nervix_ingestor_quiesce_dropped_total` count batches
rather than samples. On resume, each retained batch enters the graph as an ingest group of its own,
oldest first.

A quiesce change or a stop that meets a query in flight abandons that query and keeps nothing of its
answer. A hold that begins or ends during a query therefore leaves a gap of one occurrence, under
`BUFFER` too.

#### `PROMETHEUS CLIENT`

Every key of a `PROMETHEUS CLIENT`'s `CONFIG` is Nervix's own:

| Key | Required | Value |
| --- | --- | --- |
| `addr` | Yes | The root of the Prometheus HTTP API: an `http` or `https` URL with a host, whose path is the prefix the API is served under, such as `/monitoring` behind a reverse proxy. Each query is sent to `api/v1/query` beneath it, so the URL carries no query string and no fragment. |
| `timeout_ms` | Yes | A positive whole number of milliseconds that bounds one query, as [Client Deadlines](#client-deadlines) defines. It covers name resolution, connection setup, TLS and the whole response. |
| `tls_ca_file` | No | PEM certificate authorities that HTTPS trusts in addition to the Mozilla roots Nervix ships and the host's native roots. |
| `tls_cert_file`, `tls_key_file` | Together | The PEM certificate chain and private key the client presents to authenticate over HTTPS. |

`CREATE PROMETHEUS CLIENT` refuses a configuration that cannot configure a query ingestor, and names the key:
a key the client does not declare, a key written twice, a missing `addr` or `timeout_ms`, an
`addr` that is not such a URL, a `timeout_ms` that is not a positive whole number of
milliseconds, or one of `tls_cert_file` and `tls_key_file` without the other. For example,
`client 'prom_main' configuration is invalid in domain 'shop': missing required client config key 'timeout_ms'`.
Keys are lowercase and case-sensitive. A value can name a file
of the client's `MOUNT`, as in `'tls_ca_file' = '{{ dev_tls }}/ca.pem'`. The rendered configuration
must satisfy these rules before commitment, and an ingestor checks it again when opening, so the
address it queries is the one the rules admit.

#### Prometheus Client Transport

A query travels through the HTTP client Nervix shares with Sentry and OTEL over HTTP, and inherits
its [transport contract](connector-contract.md#http-client-transport):

- Each query is one `GET` to `api/v1/query` beneath `addr`, with the PromQL in its `query` parameter
  and the due instant, in decimal seconds, in its `time` parameter. The PromQL travels in the URL, so
  it must fit what the server and any proxy in front of it accept in one. The request carries no
  `timeout` parameter, so the server's own query timeout applies beside `timeout_ms`.
- The host of `addr` is resolved through the node's DNS resolver. It stays the HTTP authority and the
  name HTTPS verifies, as [Connection Identity](name-resolution.md#connection-identity) describes.
- No proxy is used. A `PROMETHEUS CLIENT` declares no `proxy`, and the proxy environment variables
  `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY` and their lowercase forms are never read.
- No redirect is followed, so a query goes only to the host of `addr`. A `3xx` answer fails its poll
  with its status, such as `Prometheus query failed with HTTP status 302 Found`, so `addr` must name
  the root that answers rather than one that redirects to it.
- A client declares no bearer token, and a query writes no headers. A server that requires
  authentication is reached with a client certificate, or with Basic credentials in the user
  information of `addr`, as in `https://reader:secret@prometheus.example.com`. Every query sends
  them in its `Authorization` header. `SHOW CREATE CLIENT` and `DESCRIBE CLIENT` mask such an `addr`
  as `********`, and complete serialization keeps it, so a backup archive holds it; see
  [Backup And Restore](backup-and-restore.md).

### `HTTP ENDPOINT` And `WEBSOCKETS ENDPOINT`

The node's [Listener Bounds](node-settings.md#listener-bounds) apply before endpoint dispatch:
connection, header, TLS, idle and HTTP body limits, accounted body admission, explicit WebSocket
frame/message bounds, and the 413/408/431/503 input outcomes. These are local Nervix capacities;
the endpoint's delivery, quiesce and acknowledgement contract still owns graph admission.

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
| Locator and validation rule | Not offered: The endpoint declaration owns the host and path this cluster serves; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | NO ACK<br>ACK MAX 8 ACK TIMEOUT 5s |
| Options in statement order | No entity-specific options. |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |
| Metadata | metadata.method: STRING OPTIONAL<br>metadata.path: STRING OPTIONAL<br>metadata.query: STRING OPTIONAL<br>metadata.remote_addr: STRING OPTIONAL<br>metadata.host: STRING OPTIONAL<br>metadata.remote_ip: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
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
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | REJECT RETRY AFTER 5s<br>BUFFER MAX SIZE 64MiB |
| Metadata | metadata.frame_type: STRING OPTIONAL<br>metadata.remote_addr: STRING OPTIONAL<br>metadata.path: STRING OPTIONAL<br>metadata.remote_ip: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
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
endpoints. Declare the endpoint kind in its head:

```text
CREATE [IF NOT EXISTS] HTTP ENDPOINT <name> ON <vhost> PATH '<path>';
CREATE [IF NOT EXISTS] WEBSOCKETS ENDPOINT <name> ON <vhost> PATH '<path>'
  [WITH SIGNALING PROTOCOL <protocol>];
```

The path starts with `/`. Only the WebSockets endpoint body accepts a signaling protocol.
`SHOW CREATE ENDPOINT <name>` renders the same kinded declaration, and `DESCRIBE ENDPOINT <name>`
reports `kind: HTTP ENDPOINT` or `kind: WEBSOCKETS ENDPOINT`. Inspection and deletion address an
endpoint by name with `SHOW CREATE ENDPOINT`, `DESCRIBE ENDPOINT`, or `DROP ENDPOINT`.

An `HTTP ENDPOINT` ingestor reads an `HTTP ENDPOINT`, and a `WEBSOCKETS ENDPOINT` ingestor reads a
`WEBSOCKETS ENDPOINT`; completion after `FROM` offers only endpoints of that kind. An endpoint of
the other kind is refused when the statement is applied, with a message such as
`requires a HTTP ENDPOINT, but endpoint 'stream' is WEBSOCKETS ENDPOINT`. Both ingestor heads take
the same body.

Both endpoint entities accept `MODE NO ACK` or `MODE ACK MAX <n> ACK TIMEOUT <duration>`.
The ACK window is explicit: `MAX` is 1–65,535 outstanding payloads per ingestor on each node.
An acknowledged endpoint requires `ON QUIESCE REJECT RETRY AFTER <duration>`. The sender owns
retry; there is no endpoint `RETRY POLICY`, `ACK PARALLEL`, or `BATCH` clause. `BUFFER MAX SIZE`
is available with `MODE NO ACK`. Transport headers are available through `read_header` and
`read_headers`, and each entity declares the typed metadata of a request or a frame. Both ingestors
execute on every cluster node and are excluded from placement members and placement completion.

An `HTTP ENDPOINT` ingestor reads each admitted request's metadata in its route expressions:

| Field | Value |
| --- | --- |
| `metadata.method` | The request's method. The endpoint admits only `POST`, so an admitted request always reads `POST`. |
| `metadata.path` | The path of the request target, exactly as the request wrote it, without its query. Routing matches it exactly, so it is the endpoint's `PATH`. |
| `metadata.query` | The query of the request target after `?`, exactly as written and not decoded: empty for a target that ends with `?`, and absent for one with no `?`. |
| `metadata.remote_addr` | The address and port of the peer whose TCP connection carried the request, such as `127.0.0.1:52144` or `[2001:db8::1]:52144`. It is the connection's peer, not a forwarding header such as `X-Forwarded-For`. |
| `metadata.host` | The host the request was routed by: its `Host` value in ASCII lowercase and without its port, one of the names of the endpoint's `VHOST`. |

A `WEBSOCKETS ENDPOINT` ingestor reads each data frame's metadata:

| Field | Value |
| --- | --- |
| `metadata.frame_type` | `text` or `binary`, the kind of the data frame. |
| `metadata.remote_addr` | The address and port of the peer whose TCP connection opened the WebSocket. |
| `metadata.path` | The path the connection's upgrade request named, which is the endpoint's `PATH`. |

Every field is `STRING OPTIONAL` and declared non-sensitive. Every frame of a connection reads the
same `remote_addr` and `path`, including frames its signaling protocol accepts before the handshake
ends. Each node reads the
requests and frames it receives, so a field never depends on the node that owns the ingestor's
output. A body that `BUFFER` retains keeps its metadata, and its replay reads the values it
arrived with. This route keeps only acme's requests and records the peer that sent each one:

```plexus
CREATE HTTP ENDPOINT INGESTOR tenant_requests
  FROM intake MODE NO ACK
  ON QUIESCE REJECT RETRY AFTER 1s DECODE USING event_codec
  TO acme_events
    INHERIT ALL
    SET peer = metadata.remote_addr, query = metadata.query
    WHERE metadata.query = 'tenant=acme'
    UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Both endpoints record header names in ASCII lowercase and keep every value of a repeated name in
that name's original value order. They declare no ordering between different names. WebSocket
header reads refer to the opening upgrade request, not to later data frames.
A value that is UTF-8, whether or not it is ASCII, reaches `read_header` and `read_headers` unchanged,
and a value that is not UTF-8 is
dropped and counted in `nervix_ingestor_header_values_dropped_total` for every ingestor the request
or WebSocket upgrade was routed to.

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

| HTTP result in `MODE ACK` | `outcome` | `cause` | Meaning |
| --- | --- | --- | --- |
| 200 | `completed` | `none` | The payload's graph acknowledgement root completed under the declared error policies. |
| 400 | `processing_failed` | `rejected` | The codec or local route construction refused the input. |
| 503 | `not_admitted` | `busy` | The response window or execution capacity is full. |
| 503 | `not_admitted` | `suspended` | Intake is quiesced before admission. |
| 503 | `processing_failed` | `rejected` | Downstream negatively acknowledged admitted work. |
| 503 | `processing_failed` | `ack_timeout` | The absolute response deadline expired. |
| 503 | `outcome_unknown` | `interrupted` | The exact intake stopped or ended while the response was held. |

Every ACK response has a JSON object containing exactly `outcome` and `cause`, using the existing
[client submission outcome vocabulary](client-io-architecture.md). For example, a local refusal
is `{"outcome":"processing_failed","cause":"rejected"}`. Downstream rejection uses that same
object with status 503. The response contains no payload or diagnostic details. Runtime events
retain the ingestor's ordinary diagnostics.
A 503 uses the declared retry delay, rounded up to whole seconds. A retry can duplicate output
that already completed; timeout or interruption does not roll back admitted work.

The physical ACK timeout starts when a complete payload obtains its window slot. It is positive
and fits the supported physical deadline range. Downstream keepalive does not restart it. The
window uses immediate admission: it never queues a request for a response slot. A window already
full at the HTTP head refuses before reading the body; dispatch checks capacity again after the
body completes, so competing requests cannot overrun the window. HTTP request-head and body reads,
input memory and WebSocket message and frame sizes follow the node's separately declared
[Listener Bounds](node-settings.md#listener-bounds).

In `MODE NO ACK`, admission still answers HTTP 202 without waiting for downstream acknowledgement.
A codec refusal becomes a decode rejection consumed by `ON GENERAL ERROR`; it also answers 202.
WebSocket data frames receive no per-frame graph-completion reply in this mode; signaling replies
still follow their protocol. With `BUFFER`, HTTP bodies that fit receive 202 and drain after resume.
A full buffer rejects new intake without discarding bodies already accepted. If extension workers
cannot unfold a fresh payload,
HTTP answers 503 and a WebSocket closes with 1013; buffered payloads instead retain their place
until workers can take them. See [When The Codec Refuses A Payload](#when-the-codec-refuses-a-payload).

Quiesce is enforced independently on every serving node. `REJECT` refuses new requests and
WebSocket upgrades with 503. A stopped or absent intake refuses new requests too. A route shared
by unacknowledged ingestors accepts when at least one accepts; it includes `Retry-After` only when
all refusals supply a delay. When an acknowledged binding participates, success waits for its root;
a failure after any binding admitted the input is not reported as `not_admitted`.

Each node counts every request and WebSocket data frame it receives once for each ingestor bound to
the route, in `nervix_ingestor_endpoint_requests_total` under the outcome that ingestor reached:
`accepted`, `busy`, `refused`, `not_admitted`, `decode_rejected` or `processing_failed`. An
ingestor that refuses the requests of a route it shares with an available ingestor counts each of
them once. `DESCRIBE INGESTOR` shows the family under `source:` for the node that answers it;
[Ingestor Source Interactions](metrics-and-observability.md#ingestor-source-interactions) defines
each outcome.

A WebSocket endpoint in `MODE ACK` requires an explicit signaling protocol. Its outbound wire
format encodes the same `outcome` / `cause` object for each data frame, both during `ACCEPT DATA`
and after signaling. JSON and YAML use text; CBOR and protobuf use binary. RAW carries the object's
JSON text. A protobuf outbound message must represent both string fields; startup validates every
fixed outcome before binding intake. The protocol's control frames retain their declared meaning.

A WebSocket retains its selected route and signaling protocol throughout the connection. Stopping
or replacing its ingestor ends that exact intake, including signaling data intake. An interrupted
acknowledgement wait returns HTTP 503 or the equivalent WebSocket outcome frame. A request admitted
before the ending may still finish. The next payload on a retained ended route closes with 1013.
Open a new connection for a replacement ingestor.
Stopping a domain withdraws only its routes and intakes; other domains on the same host and path
continue receiving requests. A withdrawn route returns HTTP 404.

A node answers 404 only once it has installed a revision since it started. Before that, it cannot
tell a route the cluster binds from one no domain declares. This happens, for example, when the
node restarts while its cluster has no quorum to catch up through. Such a node refuses every
request without a published route with HTTP 503 and no retry delay, and a WebSocket upgrade the
same way. A route that is already published is served at once. Stopping the node clears its routes
and returns it to that state.

Server-side endpoints are hosted under a `VHOST`. A plain VHOST serves HTTP and WS on the HTTP listener. A TLS-enabled VHOST serves HTTPS and WSS on the separate HTTPS listener.

TLS is configured on the VHOST itself:

```plexus
CREATE IF NOT EXISTS VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle VERSION 3;
```

`WITH TLS` requires a version. `VERSION LATEST` binds the highest completed version of the bundle
when the statement is applied:

```plexus
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
  [HEADERS { 'authorization' = 'Bearer token' }]
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
| Options in statement order | `HEADERS { 'authorization' = 'Bearer token' }` (composed clause); optional; omitted when unset |
| Headers | `read_header`, `read_headers`; `repeats_by_name`, lowercase names: every value of a name, together and in the order written; the order of different names is not declared; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| Connector events | None declared. |
| Instancing | single |
| External partitions | No enumerable external partition set. |
| Quiesce (complete bodies after ON QUIESCE) | BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST<br>DROP |
| Metadata | metadata.frame_type: STRING OPTIONAL |
| Payload facts | payload.bytes: U64 |
| Every live node | no; scheduled owner |
| Position class | none |
| Acknowledgement confirmation | none |
| Buffers | accepted signaling frames retained for ingestor reads: set by handshake_buffer_capacity messages (default 16), each at most max_message_size bytes; when full, holds back what fills it<br>one accepted frame waiting for handoff room: set by max_message_size bytes (default 67108864); when full, holds back what fills it<br>the incoming message assembled by the transport: set by max_message_size bytes (default 67108864), with each frame bounded by max_frame_size bytes (default 16777216); when exceeded, fails the read and reconnects on the retry policy |
| Network waits | connection: name resolution, address attempts, TLS and the opening upgrade: `connect_timeout_ms`; 30s when omitted<br>the signaling handshake: `the signaling protocol's TIMEOUT`<br>reading the next frame, including one catch-up response window after a missed client ping: `idle_timeout_ms`; when omitted, three ping_interval_ms intervals, or 30 s without configured pings<br>writing a configured ping: fixed 1s<br>close: fixed 1s |
| Driver limits | The opening upgrade response carries at most 124 header fields, and at most 64 KiB arrives before its head ends; a larger response fails that connection attempt.<br>The response head arrives within 512 reads that, after the 64th, average at least 128 bytes; a peer that trickles it fails that connection attempt. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms`<br>`STALLED PEER idle_timeout_ms` |

See the [WEBSOCKETS CONNECTION capability and mirror reference](connector-capabilities.md#websockets-connection-ingest).
<!-- connector-matrix:ingest-websockets-connection:end -->

This opens an outbound WebSocket connection through a `WEBSOCKETS CLIENT` and decodes text or
binary frames. Its optional `HEADERS { 'authorization' = 'Bearer token' }` clause follows `FROM
<client>` and takes literal string names and values. Names are case-insensitive; repeated names,
protocol-owned fields (`Host`, hop-by-hop framing fields and every `Sec-WebSocket-*` field except
`Sec-WebSocket-Protocol`) and malformed values are refused before activation. The committed
`http-header-fields` and `http-header-bytes` parameters bound the field count and the total field
name/value bytes; their defaults are 128 fields and 32 KiB. Admission and parameter changes check
these requirements, so lowering a cap cannot invalidate an existing ingestor. Raising a cap
permits new declarations above those defaults, within the HTTP representation's bounds. Each
value, a credential included, is visible in `DESCRIBE INGESTOR` and `SHOW CREATE INGESTOR`.

`HEADERS { 'sec-websocket-protocol' = 'v2.events, v1.events' }` offers subprotocols in the order
of preference written. The value lists one or more subprotocol tokens (RFC 6455): elements
separated by commas, with optional spaces or tabs around each comma. An empty element, an element
that is not a token, and a token offered twice are refused before activation. The upgrade writes
the offer as its tokens joined by commas alone, so the example sends `v2.events,v1.events`, and
the offer counts toward the field bounds above. The peer must select exactly one of the offered
subprotocols. A peer that selects none of them, selects one the upgrade did not offer, or selects
one when the upgrade offered none fails that connection before any frame is read. The ingestor then
tries none of the endpoint's other addresses: the endpoint answered, so another address would
answer the offer the same way. `DESCRIBE INGESTOR` shows the refusal as the transient error,
without naming a subprotocol, because an offer can carry a credential, and the ingestor reconnects
on the host's cadence below. Refusals of a malformed offer likewise name the element's position,
never its value. The selected subprotocol is the opening response's
`sec-websocket-protocol` header, which `read_header('sec-websocket-protocol')` reads.

Each data frame exposes `metadata.frame_type` as `text` or `binary`. `read_header` and
`read_headers` read the opening response's header values that hold only printable ASCII and tabs,
including repeated values in their transport order; a value holding any other byte is left out.
These values belong to the frame's exact connection: accepted signaling frames
and quiesce-buffered frames retain that snapshot across suspension, reconnection and replay.
Headers are not copied into every frame and are not propagated to a relay unless mapped explicitly.
The connection is ingest-only; it declares no emitter envelope or `BATCH` clause.

This client reaches a TLS endpoint signed by its own certificate authority and pings it every 20
seconds; the connection offers two subprotocols and records the one its peer selects and each
frame's kind:

```plexus
CREATE WEBSOCKETS CLIENT market_feed
  MOUNT feed_tls VERSION 1
  CONFIG {
    'endpoint' = 'wss://stream.example.com/trades',
    'ping_interval_ms' = 20000,
    'tls_ca_file' = '{{ feed_tls }}/ca.pem'
  };

CREATE WEBSOCKETS CONNECTION INGESTOR market_trades
  FROM market_feed
  HEADERS { 'sec-websocket-protocol' = 'v2.trades, v1.trades' }
  MODE NO ACK
  ON QUIESCE BUFFER MAX SIZE 64MiB ON OVERFLOW DROP OLDEST
  DECODE USING trade_codec
  TO trades
    INHERIT ALL
    SET protocol = read_header('sec-websocket-protocol'),
        frame_type = metadata.frame_type
    UNBRANCHED
    FLUSH IMMEDIATE
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

With no `idle_timeout_ms`, a peer that sends nothing for three ping intervals, 60 seconds here, is
reconnected when the client has kept its ping cadence. A client without `ping_interval_ms` only
answers its peer's pings and allows 30 seconds of silence. A missed client ping has the bounded
response opportunity described below.

The client's Nervix-owned `CONFIG` is strict, with lowercase, case-sensitive keys:

| Key | Contract |
| --- | --- |
| `endpoint` | Required `ws://` or `wss://` URL with a host, without a fragment and without user information. Its path and query are sent in the upgrade request. |
| `connect_timeout_ms` | Positive whole milliseconds for DNS, TCP, TLS and upgrade together; omitted means 30000. |
| `ping_interval_ms` | Positive whole milliseconds between client pings after signaling completes; omitted means only answering peer pings. |
| `idle_timeout_ms` | Positive whole milliseconds without an inbound frame before reconnecting, also bounding the response window of one catch-up ping after a missed client interval. When omitted, three configured ping intervals apply, or 30000 without configured pings. |
| `handshake_buffer_capacity` | Accepted signaling messages retained before readiness: 1–65535, default 16. A full queue backpressures the connection within the signaling `TIMEOUT`. |
| `max_message_size` | Positive whole bytes per incoming message, including fragmented messages: at most 4294967295, default 67108864 (64 MiB). |
| `max_frame_size` | Positive whole bytes per incoming frame: at most 4294967295, default 16777216 (16 MiB). |
| `tls_ca_file` | Optional PEM CA file holding at least one certificate; only with a `wss://` endpoint. Its certificates are trusted beside the bundled WebPKI roots and the platform's native roots, never instead of them. |
| `tls_cert_file`, `tls_key_file` | Optional client identity files, supplied together; only with a `wss://` endpoint. |

The client's declared key schema refuses unknown, repeated or mistyped keys and TLS file keys with
`ws://`, and supplies the three declared defaults that `DESCRIBE CLIENT` shows. The WebSocket
configuration then refuses an endpoint with a fragment or user information, a count above its
maximum and three ping intervals beyond the schedulable bound. Durations obey the shared
schedulable physical-duration bound. Resource mounts and configuration templates keep their shared
client contract; rendered values are checked before commitment and again when opening the ingestor.
A ping write has a one-second physical budget and a failure triggers recovery.

No WebSockets key is declared confidential, but `DESCRIBE CLIENT` and `SHOW CREATE CLIENT` show an
`endpoint` that carries a query as `********`, because a query can hold a credential. The upgrade
never sends user information, so `CREATE WEBSOCKETS CLIENT` refuses an `endpoint` that carries it
with `'endpoint' must not carry user information`, without printing the value. A peer that needs
credentials receives them as a `HEADERS` field, such as `authorization`, or in the query.

`CREATE WEBSOCKETS CLIENT` requires each TLS file key, once its resource mount template is
rendered, to name a readable regular file. It also refuses a `tls_ca_file` that holds no PEM
certificate, with `client config key 'tls_ca_file' holds no certificate`: such a file adds no trust
root, so a private CA the client was meant to trust would surface only as a verification failure on
every connection. `REBIND RESOURCE` applies the same checks to the version it moves a client to.
The ingestor parses the files when it opens: a CA certificate it cannot parse, or a client
certificate chain and private key it cannot read or pair, fails the ingestor's start, which
`DESCRIBE INGESTOR` shows as its transient status. The server certificate is always verified
against the endpoint's host.

These checks read the file on the node that validates the statement. A mounted file is the pinned
version every node reads. A path outside a mount names each node's own file, and the ingestor does
not repeat the certificate check when it opens: a file that holds no certificate there adds no
trust root, and the bundled and native roots alone verify the server.

After signaling succeeds, the idle timer starts. An explicit idle timeout overrides the
ping-derived value. Every received frame, including a ping or pong, refreshes activity. Sending a
regular client ping or cancelling a read leaves the inactivity deadline in place.

The host may spend longer than that deadline handing a payload off, or the node may stop polling
the connection while busy. If the deadline expires after two ping intervals without either an
inbound frame or a successfully written client ping, Nervix has missed a full ping interval. It
writes one catch-up ping before judging the peer silent and gives the peer one idle-bound response
window, measured from that successful write. Further pings and read cancellation do not renew this
window. A received frame restores ordinary activity tracking; a peer that does not answer still
expires. The ping write retains its one-second bound.

This rule also applies with an explicit `idle_timeout_ms`. On-schedule pings do not extend the
ordinary idle bound. Without configured client pings, the deadline stays in physical time across
handoff and cancelled reads. A silent connection is closed and retried on the host's reconnect
cadence.

Closing it sends the peer a Close frame with status `1001` (going away) if the connection takes the
frame at once. Nervix waits neither to write the frame nor for the peer's reply, so a peer that has
stopped reading never delays the reconnect.
Choose a bound that permits the peer's expected idle periods or keepalive cadence. Three ping
intervals must fit the shared schedulable bound when they supply the deadline; the signaling
handshake has its own `TIMEOUT`.

Accepted signaling data stays in one bounded handoff until the handshake completes. The ingestor
then delivers those messages in order before reading new socket frames. In addition to the queue,
one accepted frame can wait for room, and the transport can assemble a message within its declared
message and frame limits. If a full handoff prevents signaling from completing, the signaling
`TIMEOUT` fails that connection and clears its undelivered handshake data before retrying. An
oversized incoming frame or message also ends the connection with a transient read failure;
reconnection supplies no durable replay guarantee. Ending it sends the peer a Close frame with
status `1009` (message too big) if the connection takes the frame at once. A frame that breaks the
WebSocket framing rules ends the connection the same way with status `1002` (protocol error), and
message text or a Close reason that is not valid UTF-8 with `1007` (invalid frame payload data). As
with the idle deadline, Nervix waits neither to write the frame nor for the peer's reply, so the
Close never delays the reconnect. A connection whose TCP has already ended or failed is released
without a Close.

A signaling handshake that fails ends its connection the same way, and the ingestor reports the
failure with its cause. A read that fails while signaling earns the status of the read failure
above. A frame a `FAIL JAQ` guard rejects earns status `1008` (policy violation), and the
protocol's `TIMEOUT` earns `1001` (going away), as the idle deadline does. A failure of the
client's own programs earns `1011` (internal error): a `SEND JAQ` program that does not produce
exactly one value, an output the protocol's format cannot encode, or a `CAPTURE` program that
fails or produces no object. A handshake whose TCP has already ended, or whose own write failed, is released without a
Close.

When the peer sends a Close during signaling, Nervix flushes the queued reply before releasing the
connection, within the same one-second control-frame budget used after readiness. The reply echoes
the peer's status and reason, and the transient resume failure retains both as typed values. A
Close without a status remains distinct from a stream that ended without one. The failed attempt
still discards its accepted payloads, and the host reconnects on its policy.

For `ws` and `wss`, each initial connection and resume resolves the URL host through the node's
asynchronous resolver. It tries the returned addresses in order within the configured
`connect_timeout_ms` budget for DNS, TCP, TLS and the opening upgrade. The upgrade still uses the configured URL for the HTTP Host
header, TLS server name, path and query; custom TLS roots and client identity remain in effect.
An address whose connection, TLS handshake or upgrade fails, including an answer other than
`101 Switching Protocols` and an opening response beyond the transport's limits under Driver
limits above, gives way to the next address within the same budget; only a refused subprotocol
offer ends the attempts at once.
An expired DNS answer is refreshed on a new attempt, while a healthy connection stays open.
Resolution, connection and read failures are transient ingestor failures. Nervix answers a peer's
Close before releasing the connection and reports any supplied code and reason in `DESCRIBE INGESTOR`;
EOF also triggers recovery. This `MODE NO ACK` ingestor reconnects on the host's cadence: 250 ms,
500 ms, 1 second, and continuing to double up to 30 seconds. The current wait is visible as the
reconnect backoff in `DESCRIBE INGESTOR`. A successful upgrade alone does not reset the delay or
clear the last failure: a delivered batch does. A peer that repeatedly connects and closes without
delivering a batch therefore receives the growing delay too. Cancelling or stopping the ingestor
drops a pending connection attempt.
Both quiesce modes continue polling the connection so keepalives and reconnect behavior remain live.
A stop-reading mode is unavailable because it would starve protocol maintenance and become a
disconnect.

When the host suspends or closes the ingestor, it sends a Close frame and waits up to one second for
the peer's reply, including the time needed to send the frame, before releasing the connection.
A peer that does not answer cannot hold shutdown indefinitely. Suspension retains queued payloads
from successful signaling and delivers them before opening a replacement connection. Terminal
close clears that queue even if the Close exchange fails or times out.

A `WEBSOCKETS CONNECTION` ingestor is a single-owner ingestor. It keeps its existing primary and
replica assignment through schedule recomputation while every assigned cluster node remains live,
just like Kafka, MQTT, and the other client ingestors. Creating or changing another runtime node, a
cluster node joining or being uncordoned, and a soft placement-policy change do not move its
outbound session. Failover, draining its owner, an explicit `RELOCATE`, or a newly effective
`REQUIRE COLOCATION` group can move it; a real move closes the former owner's session and opens a
new session on the new owner.
`HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors are different because
their listeners execute on every live cluster node.

A `WEBSOCKETS CLIENT` can declare `WITH SIGNALING PROTOCOL <name>` after its name. A server-side `WEBSOCKETS ENDPOINT` can declare the same clause
after its `PATH`.

A signaling protocol declares a wire format and expresses the whole handshake as
[JAQ](./schemas-and-codecs.md#jaq-transformations) programs:

```plexus
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
Handshake send, capture, encoding, and transport failures retain their causes in the ingestor
diagnostic. A failed handshake delivers no frame to the relay. A `WEBSOCKETS CLIENT` connection
whose handshake fails ends with a Close frame naming why, with the statuses listed above.

### Data Arriving During The Handshake

Peers commonly start streaming before they finish acknowledging. `ACCEPT DATA`
marks when incoming data becomes payload to retain during signaling:

```plexus,ignore
ON CONNECT ACCEPT DATA                                  -- from the first frame
WAIT JAQ '<matcher>' [FAIL JAQ '<matcher>'] [CAPTURE '<program>'] ACCEPT DATA
```

On `ON CONNECT`, acceptance starts with the first frame. On a `WAIT` step, it starts after every
matcher in that step has succeeded. To retain data after one subscription is confirmed while
another is still outstanding, use separate `WAIT` steps: mark the first with `ACCEPT DATA` and
wait for the second afterward, as in the captured-state example below.

Before acceptance starts, unmatched data frames are discarded. After acceptance starts, an
outbound `WEBSOCKETS CONNECTION` ingestor holds payloads in its bounded handshake queue, whose
`handshake_buffer_capacity` defaults to 16 messages. A full queue holds back further accepted
data within the whole handshake's `TIMEOUT`. The ingestor reports readiness only after all
signaling steps succeed, then delivers the retained messages in order before reading new socket
frames. A failed or cancelled handshake discards that attempt's queue. Without `ACCEPT DATA`,
unmatched frames are discarded throughout signaling and ordinary payload reads begin after the
handshake succeeds.

### Sequencing And Captured State

`CAPTURE` records values from the frame that satisfied its matcher, and every
program can read what has been captured so far through the `$state` variable:

```plexus
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

```plexus
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

```plexus,ignore
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
| Entity | Capability | INSTANCES |
| --- | --- | --- |
| KAFKA TOPIC | per_partition | optional; default 1 |
| PULSAR TOPIC | per_partition | optional; default 1 |
| MQTT TOPIC | per_partition | optional; default 1 |
| NATS SUBJECT | per_partition | required |
| RABBITMQ QUEUE | per_partition | optional; default 1 |
| SQS QUEUE | per_partition | optional; default 1 |
<!-- connector-matrix:instancing:end -->

`Single`, `Instances`, and `PerPartition` are entity capabilities. A `Single` entity accepts no
`INSTANCES` clause. `Instances` permits a fixed count; `PerPartition` also permits the partition
form. The six broker entities in the table declare `PerPartition`. Polling, connection, listener,
and client-producer entities declare `Single`.

`INSTANCES <n>` uses a positive 64-bit count (1 through 18,446,744,073,709,551,615). Before
placement, Nervix refuses a count whose execution-unit entries cannot be addressed on the target.
The declaration and its encodings retain the complete count. Nervix schedules each global index `0..n-1` as an
independent execution unit with its own primary, replicas, readiness, intake buffer, ACK roots,
branch lifecycle and metric checkpoints. Eligible nodes share the primary work; when fewer nodes
are available, several instances can run on one node. A hard whole-entity colocation rule can
require all its units to share an owner. Connector identities retain the global index when a unit
moves; an owner never renumbers its local instances. Broker competition retains the transport's
own subscription and delivery guarantees.

`INSTANCES PER PARTITION` creates one instance per partition of the branch its routes construct.
Every constructing route must target the same ordered key-field contract and partition count.
Branch names and retention settings may differ. At least one route must construct a branch;
additional unbranched routes do not define its partition structure. Instance `p` is colocated with
partition `p` of every constructed relay. A branch with one partition places instance `0` with the
relay's `Single` unit. The current form follows direct branch partitions; shard inheritance and
topology epochs are separate features.

For Kafka, `PER PARTITION` requires `OFFSET BY DOMAIN`. Each unit owns the offsets of the topic
partitions dealt to its global index, and commits through the originator on that unit's primary.
Changing a domain-offset ingestor's unit count preserves its recorded positions while running or
stopped. Nervix drains the affected units and prepares their redistributed state on the new
primaries and replicas before publishing the layout. A failed preparation retains the preceding
layout. Changing the broker topic's partition set with a fixed instance count retains its existing
rebalance and checkpoint contract.

`DESCRIBE INGESTOR <name>` reports every instance with its node, running state and readiness.
`SHOW INGESTORS` and `SHOW CLUSTER STATUS` list the unit-qualified owners. Generic edge metrics and
ingestor-specific intake metrics carry the `instance` label; see
[Metrics And Observability](metrics-and-observability.md#metric-labels). Moving or stopping one
instance ends its exact ingestor lifetime and leaves sibling ingestor tasks, ACK roots and readiness
handles installed. A count change retains complete metric histories exactly once, with their
original instance and physical-node labels. A retired instance's history remains visible while
one of the current units owns its checkpoint.

The instances of a `KAFKA TOPIC` ingestor under `OFFSET BY CONSUMER GROUP` are members of one group
and share its `START AT`: whichever instance takes a partition the group holds no usable offset for
starts it there, and a partition that moves to another instance resumes from the group's committed
offset. Under `OFFSET BY DOMAIN` the domain's start point places every partition, whichever instance
the schedule assigns it to.

### Inspecting Transport Reads

Read `DESCRIBE INGESTOR`'s `source:` section and the matching Prometheus families to distinguish raw
transport reads from graph traffic. Kafka reads count returned payloads before the filter runs;
HTTP REQUEST and PROMETHEUS QUERY polls also report empty answers and elapsed poll duration.
[Ingestor Source Interactions](metrics-and-observability.md#ingestor-source-interactions) defines
outcomes, labels, timing and replay accounting. The existing graph edge metrics retain their own
row and batch meanings.
