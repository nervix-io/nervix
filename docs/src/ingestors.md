# Ingestors

Ingestors are the entry points into the runtime graph.

A typical ingestor:

```nspl
CREATE BRANCH by_user
  SCHEMA user_branch TTL 5m;

CREATE IF NOT EXISTS INGESTOR kafka_notifications
  FROM KAFKA kafka_main
  TOPIC notifications
  OFFSET BY CONSUMER GROUP nervix_consumer
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

Every ingestor defines:

- the destination relay or relays
- the codec used for decoding
- a route-local outgoing branch declaration
- a flush policy for every destination relay
- a message error policy for every destination relay
- the timestamp source
- the transport-specific source
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
`ACK PARALLEL`; `NO_ACK` modes never accept it.

At runtime, the ingestor:

- decodes inbound payloads into runtime records
- collects decoded records into a bounded source ingest group of at most 1,024 messages, closing
  it earlier when the source goes quiet for 5 ms; this bound is independent of every route
  `FLUSH` policy and caps the size of the first Arrow batch built from external input
- executes `FILTER WHERE` once against that whole group
- executes each route's ordered construction and `WHERE` program once against the surviving group
- resolves the concrete branch group from the referenced `CREATE BRANCH`
- accumulates decoded rows independently for every matching destination and branch group
- writes each route's buffered rows when its configured interval or size boundary fires, or when
  the documented [`FLUSH IMMEDIATE` system timeout](nspl-overview.md) expires

Branch execution receives these completed Arrow batches and does not buffer them behind another
flush policy.

## Altering Ingestors

`ALTER INGESTOR` applies one or more comma-separated operations in written order:

```nspl,ignore
ALTER INGESTOR <ingestor>
    SET FROM <full source clause>
  | SET QUIESCE <source-supported quiesce body>
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

`SET FROM` accepts the complete transport-specific source body that follows `FROM` in `CREATE
INGESTOR`. Change a client, source address, Kafka topic or offset mode, `INSTANCES`, delivery mode,
or source-specific subscription settings by supplying that complete body, including its required
`ON QUIESCE` clause. `SET QUIESCE` changes only the quiesce mode and its variables. The body must be
one offered by the ingestor's current source type; there is no `DROP QUIESCE` operation. `ADD ROUTE`
appends a route, while `REPLACE ROUTE` preserves the matched route's position. Duplicate routes to
the same relay remain legal, so `DROP ROUTE` and `REPLACE ROUTE` reject an ambiguous target. An
ingestor must retain at least one route.

Every ingestor alteration uses `ENTITY_PAUSE`. Nervix quiesces only the affected ingestor instances
on all live nodes and waits for their already accepted in-flight work to drain before commit.
Schedule application starts the desired source configuration on its assigned nodes; unrelated graph
paths keep flowing. The quiesce mode in effect when the hold begins governs that hold; a mode changed
by the alteration applies to later pauses. A drain or cutover failure leaves the candidate unapplied
and releases the old source. The transient hold also expires automatically, so loss of the
coordinating leader cannot leave an ingestor quiesced indefinitely.

## Quiesce Modes

Every source specification ends with a required `ON QUIESCE <body>` immediately before `DECODE
USING`. The quiesce *level* says which runtime work pauses for an alteration; an ingestor's quiesce
*mode* says what its external source experiences while that ingestor is paused. There is no default.

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
- `REJECT RETRY AFTER <duration>` is available only to `ENDPOINT`. HTTP requests and new WebSocket
  upgrades receive 503; HTTP includes `Retry-After`, rounded up to whole seconds. Established
  WebSockets close with code 1013 (Try Again Later).
- Endpoint buffering is `BUFFER MAX SIZE <bytes>` without `ON OVERFLOW`. Requests that fit receive
  202 and are held. A full buffer, or a body that cannot fit, is rejected with HTTP 503 or a
  WebSocket 1013 close; an acknowledged body is never discarded by buffer policy.

Only modes that the source can honor are accepted or offered by completion:

| Source | Allowed `ON QUIESCE` bodies |
|---|---|
| Kafka, Pulsar, RabbitMQ, SQS | `SUSPEND` |
| MQTT | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP`; `SUSPEND` requires `SESSION PERSISTENT QOS 1` |
| NATS, Redis Pub/Sub, WebSocket client | `BUFFER ... ON OVERFLOW ...`, `DROP` |
| ZeroMQ | `SUSPEND`, `BUFFER ... ON OVERFLOW ...`, `DROP` |
| HTTP polling, Prometheus | `SUSPEND`, `BUFFER ... ON OVERFLOW ...` |
| Endpoint | `REJECT RETRY AFTER ...`, `BUFFER MAX SIZE ...` |

The mode is consulted for resumable pauses on the same node: `ENTITY_PAUSE` holds,
`DOMAIN_PAUSE` batches, and memory-pressure shedding. `STOP`, `DROP INGESTOR`, node drain or cordon
moves, failover, and graceful shutdown terminate the source session instead. A graceful termination
delivers payloads the ingestor already admitted, but any volatile quiesce buffer is lost if a
termination or crash interrupts it. The gap until a later start is covered only by the external
source's own retention; ephemeral sources provide none.

A node drain, cordon move, or failover ends the source session only of the ingestors it actually
moved. Every other ingestor in the domain keeps its session open and keeps ingesting throughout the
activation, including ingestors on the former and the new owner of the moved one. A moved ingestor
starts on its new owner as soon as that node activates the revision, without waiting for the rest of
the domain.

Entering quiesce does not wait for downstream ACK chains. Already admitted route batches continue
downstream. A source item not yet acknowledged, committed, or deleted is eligible for redelivery on
resume, so existing at-least-once duplicate windows remain. A quiesced ingestor counts as drained
even while a connected mode is reading into its quiesce buffer; those raw payloads are not runtime
graph work yet. Ordinary relay backpressure is not quiesce and does not consult this policy.

Buffered payloads drain in arrival order per instance before that instance admits new live intake.
There is no total order across instances. Decode, timestamp selection, filters, construction,
routing, and domain pacing use the configuration in effect at delivery, so a codec installed by an
alteration decodes payloads buffered during its hold. `TIMESTAMP NOW` therefore records delivery
time. Bounds are per instance, and an endpoint has an independent buffer on every serving node.

Memory pressure never adds buffered bytes. During memory-pressure quiesce, a `BUFFER` push source
drops and counts new payloads, polling sources skip polls, and endpoints reject. Payloads already
buffered by an overlapping alteration remain retained. `DROP` continues to discard and `SUSPEND`
continues to hold at the source.

`DESCRIBE INGESTOR` renders `quiesce:`, `quiesce state: none|entity hold|domain pause|memory
pressure`, `status: quiesced` for every active mode, and the four quiesce counters. `SHOW CREATE`
round-trips the full clause.

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

CREATE IF NOT EXISTS INGESTOR notifications_in
  FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
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
  supports it; Kafka provides `metadata.topic`, `metadata.partition`, and `metadata.offset`, while
  Syslog provides `metadata.peer_addr`
- supported sources expose transport headers through `read_header(name)` and `read_headers(name)`;
  header names may be any `STRING` expression, and these functions may be used in both top-level
  `FILTER WHERE` and per-route expressions

Useful built-ins include string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_like`, `now`, and `uuid_v7`.

See [Filter-Map Functions](filter-map-functions.md) for the full function reference.

Common expression patterns include:

- nested conditions such as `(active AND amount > 5) OR NOT flagged`
- chained calls such as `lower(trim(raw))`
- arithmetic expressions such as `(amount + fee) / divisor`
- explicit casts such as `raw AS INT64`

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

- HTTP endpoints and HTTP client polling expose HTTP headers with UTF-8 values
- WebSocket endpoints expose UTF-8 headers from the opening HTTP upgrade request
- Kafka exposes Kafka record headers
- NATS exposes message headers
- Pulsar exposes message properties
- RabbitMQ exposes AMQP message headers, converting typed AMQP values to strings
- SQS exposes message attributes, converting string, number, and binary values to strings

The same captured source envelope remains available while a supported ingestor constructs an
`ON MESSAGE ERROR SEND TO` record, so its error-route `SET` may also call `read_header` and
`read_headers`.

Header values are non-sensitive strings and do not propagate through relays unless explicitly
assigned to schema-backed fields:

```nspl,ignore
SET route = read_header(lower(input.route_header))
WHERE read_header("tenant") = output.tenant
```

MQTT, Redis Pub/Sub, Prometheus, Syslog, ZeroMQ, and WebSockets client ingestors do not support
header reads. Leader-side validation rejects these functions for a source without header support.

## TLS Client Configuration

For outbound ingestor clients, TLS is configured on the `CLIENT`, not on the `INGESTOR`.

General pattern:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <kind>
  MOUNT <tls_resource>
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific schemes and keys:

- `KAFKA`: pass-through to librdkafka. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and if needed `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `HTTP`: use an `https://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `PROMETHEUS`: use an `https://...` `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`, and optional `timeout_ms`.
- `WEBSOCKETS`: use a `wss://...` endpoint. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`. Nervix requires `tls_ca_file` for server trust and also supports `tls_cert_file` plus `tls_key_file` for mTLS.
- `NATS`: use `tls://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`. Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`. Pulsar client certificate authentication is not currently exposed.
- `RABBITMQ`: use `amqps://...` in `addr`. Nervix honors `tls_ca_file`.
- `REDIS`: use `rediss://...` in `addr`. Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `SQS`: use an `https://...` `endpoint`. Nervix honors `tls_ca_file`. This is primarily useful for SQS-compatible local/test endpoints.
- `SYSLOG`: select `'protocol' = 'tls'`. An ingestor requires `tls_cert_file` and
  `tls_key_file`; optional `tls_ca_file` enables required client-certificate verification.

Example Kafka TLS client:

```nspl
CREATE IF NOT EXISTS CLIENT kafka_tls
  TYPE KAFKA
  MOUNT dev_tls
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
  MOUNT dev_tls
  CONFIG {
    'endpoint' = 'https://127.0.0.1:18443/http/notifications',
    'method' = 'GET',
    'timeout_ms' = 5000,
    'tls_ca_file' = '{{ dev_tls }}/ca.pem'
  };
```

## Supported Ingestor Types

### HTTP Client Polling

```nspl,ignore
FROM HTTP <client> EVERY <duration>
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
```

- polls a configured HTTP endpoint periodically
- `204 No Content` is treated as no message
- `SUSPEND` skips polls and therefore creates a sampling gap
- `BUFFER` keeps polling on the declared cadence and drains retained poll payloads in order

### Kafka

```nspl,ignore
FROM KAFKA <client>
TOPIC <topic>
OFFSET BY CONSUMER GROUP <group>|DOMAIN
INSTANCES <count>
MODE ACK PARALLEL MAX <n>|ACK SEQUENTIAL|NO_ACK PARALLEL
ON QUIESCE SUSPEND
```

Kafka is the richest ingestion surface today.

Offset modes:

- `CONSUMER GROUP`: Kafka manages offsets
- `DOMAIN`: Nervix stores the next offset in replicated runtime state and commits partition-to-instance assignment in the Raft-backed domain schedule

Kafka client configuration is passed through to librdkafka. Nervix does not override
`auto.offset.reset`; set it explicitly, for example to `earliest`, when a new consumer group must
read records that may already exist.

`OFFSET BY DOMAIN` is at-least-once because crash recovery may restart from a slightly stale persisted offset snapshot. The leader watches Kafka partition topology and commits any rebalance through the strongly consistent domain schedule, which is persisted through the control-plane Raft/Fjall path. Executing ingestors consume only the committed partition assignment.

Offset recovery details:

- persisted per-partition offsets are clamped to the partition's currently available Kafka watermark range on reassignment
- if a partition appears later and has no stored domain offset yet, unpaced domains start from the normal default behavior, while paced domains seek from the domain's current logical time

Quiescing leaves a consumer group, or unassigns domain-owned partitions, and resume continues from
committed offsets. Topic time and size retention continue during the pause. For consumer-group
offsets that age out, the client's pass-through `auto.offset.reset` setting decides whether resume
uses the earliest retained record, skips to new records, or raises a general source error. Domain
offsets clamp to the live watermark range and resume at the earliest retained record, exposing a
gap. Other members of a user-named group may consume and commit while Nervix is absent. Existing
`NO_ACK PARALLEL` auto-commit loss windows are unchanged.

### Pulsar

```nspl,ignore
FROM PULSAR <client>
TOPIC <topic>
SUBSCRIPTION <subscription>
INSTANCES <count>
MODE ACK PARALLEL MAX <n>|ACK SEQUENTIAL|NO_ACK PARALLEL
ON QUIESCE SUSPEND
```

Pulsar ingestors use Nervix-managed shared subscriptions. The subscription
name is still required by Pulsar, but subscription type is not exposed in NSPL.
Client config currently supports:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

Suspension disconnects the consumer. The durable subscription retains its backlog and redelivers
unacknowledged in-flight messages. Broker message TTL, backlog quota policy, and inactive-subscription
expiry remain in force. If the subscription expires, resume creates it again at the broker's new
subscription initial position and the suspension window is lost.

### RabbitMQ

```nspl,ignore
FROM RABBITMQ <client>
QUEUE <queue>
INSTANCES <count>
MODE ACK SEQUENTIAL
ON QUIESCE SUSPEND
```

Suspension cancels the consumer and RabbitMQ requeues unacknowledged in-flight deliveries. Other
consumers on the same queue continue. Queue length, message TTL, overflow, and auto-expiry policies
remain in force. If auto-expiry deletes the queue, resume reports a source error until an operator
re-provisions it; Nervix never creates it.

### Redis Pub/Sub

```nspl,ignore
FROM REDIS PUBSUB <client>
CHANNEL <channel>
MODE NO_ACK SEQUENTIAL
ON QUIESCE BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

Redis Pub/Sub has no retained backlog, so it cannot suspend honestly. Both modes keep reading and
keep the subscriber healthy; payloads are either retained locally within the declared bound or
discarded and counted.

### MQTT

```nspl,ignore
FROM MQTT <client>
TOPIC <topic-filter>
[INSTANCES <count>]
[SESSION CLEAN|PERSISTENT]
[QOS 0|1]
MODE NO_ACK SEQUENTIAL
  | NO_ACK PARALLEL
  | ACK SEQUENTIAL ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>
  | ACK PARALLEL MAX <n> BATCH TIMEOUT <duration> ACK TIMEOUT <duration> RETRY POLICY BACKOFF <duration> MAX <duration>
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

MQTT topic filters may be bare identifiers or string literals for filters containing `/`, `+`, or `#`.

Delivery constraints:

- `NO_ACK` defaults to `SESSION CLEAN QOS 0`; explicit `SESSION` and `QOS` may be supplied before `MODE`
- `NO_ACK` has no in-flight ACK window, so it never accepts `MAX <n>`
- `ACK` modes require `SESSION PERSISTENT QOS 1`
- `ACK PARALLEL MAX <n>` is the in-flight ACK window and `BATCH TIMEOUT` is the maximum partial-batch wait
- `INSTANCES <count>` controls Nervix consumer parallelism; MQTT delivery always uses Nervix-managed shared subscription groups so instances do not duplicate messages

`SUSPEND` is valid only with `SESSION PERSISTENT QOS 1`. It disconnects while asking the broker to
retain the session; broker offline-queue bounds and session expiry limit what can return. If the
session expires, resume establishes a fresh session and the interim is lost. `BUFFER` and `DROP`
remain connected under any valid session declaration. In ACK modes, Nervix acknowledges a payload
when it is buffered or deliberately dropped, trading broker redelivery for the declared connected
behavior.

### NATS

```nspl,ignore
FROM NATS <client>
SUBJECT <subject>
QUEUE GROUP <queue_group>
INSTANCES <count>
MODE NO_ACK SEQUENTIAL
ON QUIESCE BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

NATS ingestors use Core NATS queue subscriptions. `QUEUE GROUP` and `INSTANCES`
are mandatory; use `INSTANCES 1` for a single queue member.

Core NATS retains no backlog, so `SUSPEND` is unavailable. A quiesced instance remains a member of
its queue group and keeps receiving its share; that share is buffered or discarded rather than
shifted to other members.

### ZeroMQ

```nspl,ignore
FROM ZEROMQ <client>
MODE NO_ACK SEQUENTIAL
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

The source is a PULL socket. `SUSPEND` keeps it open but unread, so transport buffers fill and the
peer's own high-water-mark policy determines whether sends queue, block, or drop. Buffered transport
data is read on resume. `BUFFER` and `DROP` keep reading locally.

### Syslog

```nspl,ignore
FROM SYSLOG <client>
MODE NO_ACK SEQUENTIAL
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

The client binds a UDP, TCP, or TLS listener. UDP treats one datagram as one message; TCP detects
RFC 6587 octet-counted or LF-terminated framing per frame; TLS uses RFC 5425 octet counting.
Messages expose optional `metadata.peer_addr`. The source takes no `INSTANCES` clause and has no
application acknowledgment. Every live cluster node runs the listener; it is independent of the
leader and restarts or joins with its owning node. See [Syslog](syslog.md) for the client keys,
cluster lifecycle, framing, TLS, limits, and failure semantics.

### SQS

```nspl,ignore
FROM SQS <client>
QUEUE <queue>
INSTANCES <count>
MODE ACK SEQUENTIAL
ON QUIESCE SUSPEND
```

Suspension stops polling. Messages remain only for the queue's configured retention period. An
already received message remains invisible until its visibility timeout and may then be redelivered
as a duplicate; the ingestor resumes by polling past anything the service expired.

### Prometheus

```nspl,ignore
FROM PROMETHEUS <client>
QUERY '<promql>'
EVERY <duration>
ON QUIESCE SUSPEND
  | BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
```

Prometheus samples are flattened into JSON before codec decoding.
`SUSPEND` skips scrapes and leaves a sampling gap. `BUFFER` keeps querying at the declared cadence
and delivers retained results in order after resume.

### HTTP Endpoints

```nspl,ignore
FROM ENDPOINT <endpoint> MODE NO_ACK SEQUENTIAL
ON QUIESCE REJECT RETRY AFTER <duration>
  | BUFFER MAX SIZE <bytes>
```

This is how Nervix receives inbound HTTP requests on its own server-side endpoints.

Quiesce is enforced independently on every serving node. With `REJECT`, HTTP requests and new
WebSocket upgrades receive 503, while established WebSockets close with 1013. With `BUFFER`, bodies
that fit receive 202 and drain after resume; a full buffer rejects further intake without discarding
anything already acknowledged. A stopped, absent, or otherwise unable ingestor always rejects and
never returns a silent 202. If several ingestors share a route, the request is accepted when at least
one accepts it. It is rejected only when all reject, and `Retry-After` is included only when every
rejecting ingestor declares `REJECT`.

Server-side endpoints are hosted under a `VHOST`. A plain VHOST serves HTTP and WS on the HTTP listener. A TLS-enabled VHOST serves HTTPS and WSS on the separate HTTPS listener.

TLS is configured on the VHOST itself:

```nspl
CREATE IF NOT EXISTS VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle;
```

or with an explicit pinned resource version:

```nspl
CREATE IF NOT EXISTS VHOST edge api.example.com, ws.example.com
  WITH TLS tls_bundle VERSION 3;
```

The referenced resource bundle must contain:

- `tls.crt`
- `tls.key`
- `ca.crt`

### WebSocket Clients

```nspl,ignore
FROM WEBSOCKETS <client> MODE NO_ACK SEQUENTIAL
ON QUIESCE BUFFER MAX SIZE <bytes> ON OVERFLOW DROP OLDEST|DROP NEWEST
  | DROP
```

This opens an outbound WebSocket connection and decodes text or binary frames.
Both quiesce modes continue polling the connection so keepalives and reconnect behavior remain live.
A stop-reading mode is unavailable because it would starve protocol maintenance and become a
disconnect.

A WebSocket-client ingestor is a single-owner ingestor. It keeps its existing primary and replica
assignment through schedule recomputation while every assigned cluster node remains live, just
like Kafka, MQTT, and the other client sources. Creating or changing another runtime node, a
cluster node joining or being uncordoned, and a soft placement-policy change do not move its
outbound session. Failover, draining its owner, or a newly effective `REQUIRE COLOCATION` group can
move it; a real move closes the former owner's session and opens a new session on the new owner.
Endpoint and Syslog ingestors are different because their listeners execute on every live cluster
node.

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

Every incoming frame is decoded with the same format and offered to the current
step. A textual format also reads payloads delivered as binary frames, since
peers commonly frame text that way; a binary format reads binary frames only.
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

`PROTOBUF` signaling declares its resource and one message type per direction:

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

Protobuf decoding is permissive: unknown fields are kept and missing fields take
their proto3 defaults, so nearly any binary frame decodes as the `WAIT MESSAGE`
type. Write matchers that test meaningful field values rather than relying on a
decode failure to reject a frame.

## Instancing

`INSTANCES <count>` is currently supported on source types that can safely scale through competing consumers or parallel pollers on one node:

- Kafka
- MQTT
- RabbitMQ
- SQS

If omitted, the default is `INSTANCES 1`.
