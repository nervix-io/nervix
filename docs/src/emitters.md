# Emitters

*Plexus, the Nervix graph definition language.*

Emitters publish relay records to external systems or to application consumers through a client
session.

A typical emitter:

```plexus
CREATE IF NOT EXISTS KAFKA TOPIC EMITTER kafka_notifications
  FROM notifications
  COLLECT FOR 10ms MAX BATCH SIZE 1MiB
  TO kafka_main TOPIC 'notifications_out'
    MODE ACK PARALLEL MAX 1000 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

An emitter defines:

- one or more source relays that declare the same payload schema
- an optional input collection policy
- the codec used for encoding
- the transport-specific emitter
- the emitter's explicit publishing mode, confirmation window and bound where applicable, and retry
  pacing
- an optional batching clause bounding how many records one external write carries and its encoded
  size, required for database emitters
- the flush policy used to collect a batch before publishing
- whether the branch is `ATTACHED` or `DETACHED`
- route-local codec construction or a direct `VALUES` mapping
- optional ordered header invocations on supported codec emitters
- optional [envelope clauses](#envelope-clauses) and properties its entity declares, which set what
  its transport carries beside each payload
- optional ordered materialized-state dependencies

## Statement heads and external names

Every Plexus emitter names its connector and entity before `EMITTER`:

```plexus,ignore
CREATE [IF NOT EXISTS] [ATTACHED|DETACHED] <CONNECTOR> <ENTITY> EMITTER <name>
  FROM <relays> [COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]]
  TO <reference> [<LOCATOR> '<external name>'] [<entity options>]
    MODE <entity publishing policy>
    <ENCODE USING codec | VALUES {...} | WITHOUT BODY>
  [<construction>]
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH <policy>
  ON MESSAGE ERROR <policy>
  ON GENERAL ERROR <policy>;
```

The declared pairs are `CLIENT CONSUMER`, `HTTP REQUEST`, `KAFKA TOPIC`, `PULSAR TOPIC`,
`RABBITMQ QUEUE`, `RABBITMQ EXCHANGE`, `REDIS PUBSUB`, `MQTT TOPIC`, `NATS SUBJECT`, `ZEROMQ PUSH`, `SQS QUEUE`,
`SENTRY EVENT`, `SYSLOG COLLECTOR`, `OTEL LOGS`, `OTEL TRACES`, `OTEL METRICS`,
`CLICKHOUSE TABLE`, `POSTGRES TABLE`, `MYSQL TABLE`, `MONGODB COLLECTION` and `ICEBERG TABLE`.
A client consumer uses `TO SCHEMA <schema> MODE ACK ...`, with native output in place of a codec.
All other destinations name a configured client. An Iceberg destination names its storage client;
Nervix resolves S3, GCS or Azure Blob from that client's kind. Its `NAMESPACE`, `LOCATION`, `CATALOG` and
`COMMIT EACH` options precede `MODE`. OTEL metrics retain `METRIC '<name>' UNIT ...` before
`MODE`; `VALUES`, `ATTRIBUTES`, `RESOURCE` and `SCOPE` form their payload section after it.
Database conflict clauses follow `VALUES`.

External locators are string literals and retain their exact spelling, including case. Each
entity validates its own name: Kafka and Pulsar topics, RabbitMQ and SQS queues, RabbitMQ
exchanges, Redis channels,
MQTT publication topics and NATS publication subjects. Publication names contain no MQTT or NATS
subscription wildcards. Table names contain 1–128 ASCII letters, digits, `_`, `-`,
`~` or `.`, with uppercase letters preserved. MongoDB collection names follow their own
[namespace rule](#mongodb), including spaces and Unicode: `TABLE 'public.Orders'` and `COLLECTION 'Orders'`
refer to those exact external names. Nervix references such as clients, schemas and codecs remain
bare identifiers. The [capability reference](connector-capabilities.md) gives each locator's rule.

`DESCRIBE EMITTER` exposes the kind and reference on its `to:` line. Dataflow transport labels use
the same connector/entity phrase. `SHOW EMITTERS;` lists the domain's emitters with that phrase
as `to=<CONNECTOR> <ENTITY>`, the codec or `none`, and attachment mode. `SHOW CREATE EMITTER` renders
the complete current statement. The shared [inspection block](connector-contract.md#shared-entity-presentation)
shows options, mode, quiesce, envelope, batch, flush and error policies in that order. Emitter
quiesce and envelope are `none` for the current Models.

## Branch Semantics

An emitter is the terminal consumer for its source relays. The `FROM` list uses the same
source-local predicate form as other relay-consuming nodes:

```plexus
CREATE KAFKA TOPIC EMITTER combined_notifications
  FROM primary_notifications WHERE input.source = 'primary',
       replayed_notifications WHERE input.source = 'replay'
  TO kafka_main TOPIC 'notifications_out'
    MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Every listed relay must declare the exact same schema name. Unlike ordinary multi-input
processors, emitter inputs may be unbranched or use differently named branches. Each source keeps
its own branch identity until its records cross the successful external boundary.

That means:

- the emitter consumes from all concrete branches of every source relay
- each optional source `WHERE` is evaluated only for that relay
- the current branch remains available internally for compatible materialized-state lookup
- `branch.field` is unavailable to successful emitter expressions
- branch identity collapses only after successful external publication

A node-wide materialized-state dependency and an `ON MESSAGE ERROR SEND TO` route must be
exact-branch compatible with every source. Consequently, an emitter whose inputs use differently
named branches cannot configure one branch-bound dependency or error relay across those inputs.

All emitters declare `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE`, and
`DESCRIBE EMITTER` reports the declared policy on its `flush:` line. `FLUSH`
means Nervix collects an in-memory Arrow batch before handing it to the external emitter. `FLUSH EACH`
is a domain-logical duration, so a paced domain's `TIME RATE` accelerates or slows it; retry
backoff, acknowledgement keepalive, and emitter acknowledgement timeouts stay on the physical clock.
See [Domains And Time](domains-and-time.md) for the full clock split. The
[Plexus Overview](plexus-overview.md) defines the `FLUSH IMMEDIATE` 100 µs minimum batching window.
For most emitters the collected batch is encoded and published on the flush boundary. Iceberg
additionally requires `COMMIT EACH <duration> MAX SIZE <bytes>` as part of its emitter clause: flush
writes local Arrow IPC staging files, and commit writes their rows into Parquet data files and
appends those files to the table in one catalog commit. `ON MESSAGE ERROR SEND TO`
buffers failed-message error records separately and delivers them using the emitter's same `FLUSH`
interval or maximum batch-size boundary. Which emitters gain from larger flush batches, and which
publish per record regardless of batch size, is covered by the
[flush tuning guidance](plexus-overview.md).

An emitter may place `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` immediately after the
complete `FROM <relay> [WHERE ...] [, ...]` list. This input policy runs before emitter filtering,
construction, encoding, and the required output `FLUSH` policy. Omission means no additional input
collection: each incoming relay batch enters emitter execution directly. When configured,
collection is independent for each source relay and concrete branch and releases on the timer or
optional size boundary. Equal keys from differently named branches are never collected together.
Branch identity still collapses only after successful publication.

Emitter filtering, construction, headers, direct `VALUES`, encoding, and generated integration
timestamps use the batch's accepted domain execution snapshot. A failed publish keeps that
snapshot with the pending batch across physical retry backoff; retry does not reevaluate
expressions at a later domain time. Explicit source event timestamps remain unchanged. Fields
whose public contract is observation time, such as OTEL `observed_time_unix_nano`, read actual UTC
only at the external export boundary.

## Publishing modes

Every emitter entity requires `MODE <body>` before its body selection, immediately before
`ENCODE USING` when the emitter uses a codec. There is no implicit mode, confirmation window, ACK
timeout, or retry cadence. `SHOW CREATE EMITTER` and `DESCRIBE EMITTER` render the complete mode.

The shared variables are:

- `ACK SEQUENTIAL` publishes and confirms one record before sending the next.
- `ACK PARALLEL MAX <n>` permits at most `n` records to await confirmation, where `n` is at least
  one. Nervix fills the window, waits for the oldest confirmation when it is full, and completes a
  flush only after every record in that flush has been confirmed.
- `ACK TIMEOUT <duration>` bounds one asynchronous broker confirmation. Expiry is ambiguous, not a
  record rejection: Nervix retries the still-unconfirmed records as an infrastructure failure.
  The broker may have accepted a timed-out record, so confirming modes are at least once and can
  duplicate on this path.
- `RETRY POLICY BACKOFF <duration> MAX <duration>` is required by every mode. Infrastructure retry
  delays begin at `BACKOFF`, double on each attempt, and cap at `MAX`. A server-requested delay,
  such as an HTTP rate-limit interval, can extend an individual delay; one the node's monotonic
  clock cannot schedule extends nothing. Retries continue with backpressure until the external
  system recovers or an operator repairs its provisioning.

Request/response emitters—SQS, Sentry, OTEL, HTTP, the databases, and Iceberg—do not take
`ACK TIMEOUT`. Except for Iceberg, their client request timeout bounds the response. SQS clients
expose that bound as the optional `timeout_ms` CONFIG key; Sentry, OTEL, ClickHouse and HTTP
emitter clients require it. An Iceberg emitter's object-store client bounds each storage
call with its own optional `timeout_ms`, while Nervix sets no deadline on its REST catalog
requests, as [Iceberg retries](#iceberg-retries) describes. For every emitter, Nervix accounts for
records individually wherever the transport exposes individual results: delivered records acknowledge
upstream, definitively invalid records follow `ON MESSAGE ERROR`, and a retry resends only records
that are neither delivered nor rejected. An ambiguous or infrastructure-wide failure is never used
to discard a record.

| Emitter | Mode forms | Publish success boundary |
| --- | --- | --- |
| Kafka | `NO ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Local producer-queue acceptance for `NO ACK`; one delivery report per record for `ACK` |
| Pulsar | `NO ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Producer acceptance for `NO ACK`; one broker receipt per record for `ACK` |
| RabbitMQ | `NO ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | The channel's answer to one round trip after each write for `NO ACK`; publisher confirm for `ACK` |
| MQTT | `QOS 0`; `QOS 1 ACK ...`; `QOS 2 ACK ...` | Client acceptance, `PUBACK`, or completion of the QoS 2 handshake respectively |
| NATS | `NO ACK`; `JETSTREAM ACK SEQUENTIAL`; `JETSTREAM ACK PARALLEL MAX <n>` | The Core NATS write's queued bytes drained and flushed on the same connection for `NO ACK`, without a server round trip; JetStream `PubAck` otherwise |
| Redis Pub/Sub | `NO ACK` | Server acceptance of `PUBLISH`; the subscriber count is not a delivery guarantee |
| ZeroMQ | `NO ACK` | Socket acceptance |
| SQS | `SINGLE`; `BATCH` | Successful per-record or per-entry service response |
| Sentry | `ACK` | A `2xx` answer to the event's one-event envelope |
| OTEL | `ACK` | Successful OTLP Export response; `partial_success` is acknowledged with a warning |
| ClickHouse, Postgres, MySQL | `ACK` | Successful insert/write result |
| MongoDB | `ACK` | The server's answer that it applied the document, from a successful write or from a failed one, such as a write whose write concern the deployment could not satisfy |
| Iceberg | `ACK` | Successful catalog commit |
| HTTP | `ACK` | Complete successful response headers |
| Client | `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | A current consumer attempt's explicit application ACK |

### Client emitters

<!-- connector-matrix:emit-client-consumer:start -->
```plexus,ignore
CREATE CLIENT CONSUMER EMITTER <name>
  FROM <relay>
  TO SCHEMA <schema>
    MODE <mode>
  <construction>
  BATCH MAX MESSAGES <count> MAX SIZE <bytes>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | SCHEMA &lt;schema&gt; |
| Locator and validation rule | Not offered: The body references the native schema; there is no external topic, queue or table to locate. |
| Modes (complete bodies after MODE) | ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Native Arrow batches cross an application session; producers submit batches and consumers explicitly acknowledge delivery attempts, with no broker envelope. |
| Body | native Arrow output; no codec |
| BATCH | required |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | batches retained for consumers on a node: at most 134217728 bytes; when full, holds back what fills it |
| Network waits | a delivery attempt: `ACK TIMEOUT` |
| Driver limits | None declared. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [CLIENT CONSUMER capability and mirror reference](connector-capabilities.md#client-consumer-emit).
<!-- connector-matrix:emit-client-consumer:end -->

The [Client Ingestors And Emitters](./client-io-architecture.md) architecture chapter connects
construction, competing consumers, application ACK, buffering and lifecycle in a complete graph.

`CLIENT CONSUMER EMITTER ... TO SCHEMA <output_schema>` constructs native Arrow output for applications. It has no
connector `CLIENT` object, codec, header operations, or direct `VALUES` form. The emitter still
uses its ordinary `FROM` predicates, optional collection, materialized dependencies, ordered
`INHERIT` and `SET`, route `WHERE`, error policies, placement, and `ATTACHED` or `DETACHED`
boundary. A client emitter requires `MODE ACK SEQUENTIAL` or `MODE ACK PARALLEL MAX <n>`, an explicit
`ACK TIMEOUT` and `RETRY POLICY`, `BATCH MAX MESSAGES <1..65536> MAX SIZE <bytes>`, and its
ordinary required `FLUSH` policy. `SHOW CREATE EMITTER` preserves this full contract.

The declared output schema is exact. Construction starts empty; only explicitly inherited or set
fields are exported. A sensitive input cannot be copied into output without explicit
`leak_sensitive(...)`. Branch fields do not become expression values. Every prepared Arrow IPC
stream contains rows from one source relay and one concrete branch, within both declared row and
encoded byte limits. One row larger than the limit follows `ON MESSAGE ERROR`; it does not weaken
the batch bound. A delivery carries an opaque branch fingerprint, not the branch key's values.

Consumers of one emitter compete for its output. `ACK SEQUENTIAL` permits one outstanding batch
per source relay and concrete branch; `ACK PARALLEL MAX <n>` permits at most `n` across all
workers of that source and branch. The application may `ack`, `retry`, or `reject` a live attempt.
Only `ack` confirms the batch. A retry keeps the original IPC bytes, member positions, identity,
and execution snapshot and waits on physical backoff before a fresh attempt. A timeout or lost
consumer revokes its ACK reference before reassignment. A later ACK for that reference is stale;
repeating a confirmed ACK is idempotent while its bounded result is retained. `reject` applies the
route's message error policy to every batch member with a bounded, non-sensitive reason. An
`ATTACHED` emitter keeps its source acknowledgement waiting for the application ACK; a `DETACHED`
emitter keeps its existing earlier source boundary. There is no durable consumer cursor or
delivery history: an owner loss can require upstream replay, and a lost ACK can duplicate an
application effect.

`DESCRIBE EMITTER` reports active consumers, forwarded consumers and their granted credit,
retained batches and IPC bytes, the forwarded subset of that retained work, assigned batches still
awaiting application processing, retries, application ACKs, and application rejections. Retained
work and forwarding counts describe the executing node's current owner generation; outcome
counters remain on that node across emitter restarts.

### HTTP request configuration

The [HTTP Emitter Architecture](./http-emitter-architecture.md) chapter follows the validated
request through host preparation, the connector, retry, lifecycle and qualification evidence.

An HTTP emitter uses an existing `HTTP CLIENT` and declares the request method, path and body
selection in this order:

<!-- connector-matrix:emit-http-request:start -->
```plexus,ignore
CREATE HTTP REQUEST EMITTER <name>
  FROM <relay>
  TO <client>
  (METHOD 'POST')
  (PATH '/events')
    MODE <mode>
    ENCODE USING <codec> | WITHOUT BODY
  <construction>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | HTTP client |
| Locator and validation rule | Not offered: The configured client or endpoint owns the address and path; this body has no separate external-name locator. |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `METHOD 'POST'` (expression); required<br>`PATH '/events'` (expression); required |
| Headers | `write_header`; `sorted_by_name`, lowercase names: one value per name, the last one written for it; names in ascending byte order; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| Body | ENCODE USING &lt;codec&gt; \| WITHOUT BODY |
| BATCH | unavailable: send one request per record |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | a request and the redirects it follows: for each, name resolution within half of what is left, each address's connection within an equal share of what is left after it, the proxy's TLS and tunnel, TLS, writing it and its final response headers: `timeout_ms`; required |
| Driver limits | HTTP/1.1 response heads above 100 fields or 417,792 bytes, and HTTP/2 header lists of 16,384 bytes or more as HTTP/2 counts them, are refused.<br>Redirects and proxies require the client's explicit opt-in. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [HTTP REQUEST capability and mirror reference](connector-capabilities.md#http-request-emit).
<!-- connector-matrix:emit-http-request:end -->

`METHOD` and `PATH` are structured expressions. They may read qualified source fields such as
`input.method` and `input.path`; parentheses, arrays and string literals keep clause words inside
the expression. `MODE ACK` has no acknowledgement window or `ACK TIMEOUT`, and `NO ACK` is not
available. The ordinary route-local `FLUSH` clause is required.

The referenced client must be a `HTTP CLIENT` in the same domain. Its `endpoint` is an
`http://` or `https://` origin with a host and optional port, and no credentials, non-root path,
query or fragment. It requires a positive, schedulable `timeout_ms`; the emitter's `METHOD` is its
own expression. HTTPS retains certificate and hostname verification.
The optional `tls_cert_file` and `tls_key_file` settings must be supplied together, and mounted TLS
files use the client's pinned resource version. The optional `follow_redirects` and `proxy`
settings mean for the emitter what they mean for HTTP polling, as
[HTTP client transport](#http-client-transport) describes. Validation does not probe the
destination.

Method, path and `write_header` name and value expressions must each be exact, non-null `STRING`
values. Request expressions read the original `input` and, with a codec, finalized `output` and
`message`. Without a body, `message` is the source and `output` is unavailable. Sensitive values
in any request field or body require explicit leakage. Branch fields and source-envelope header
reads are unavailable. A literal invalid method, target or header rejects configuration; the same
rules are checked per record for computed values before publication.

Methods are ASCII HTTP tokens of at most 64 bytes. `CONNECT` and `TRACE` are unavailable, and `GET`
and `HEAD` require `WITHOUT BODY`; both rules apply to every ASCII case variant, while any other
method is sent with exactly the spelling it evaluated to. `PATH` begins with exactly one `/` and is
parsed against the client origin. It cannot include a fragment, backslash, invalid percent escape,
whitespace or control character; normalization must keep it on that origin and must not produce a
leading `//`. The normalized target is limited to 8 KiB. `write_header` accepts valid HTTP field
names and UTF-8 values without control characters or leading/trailing whitespace in a nonempty
value. The request carries those values as their exact UTF-8 bytes, including non-ASCII characters.
Transport-owned headers cannot be written. After case-insensitive replacement, at most 128
application headers and 32 KiB of name/value bytes are allowed.

`ENCODE USING` permits the ordinary transforming construction clauses. `WITHOUT BODY` selects an
absent request body and permits `WHERE` and `INVOKE` but no `INHERIT`, `SET` or `VALUES`. HTTP
emitters publish one request per eligible source record, so they do not accept the optional
`BATCH` clause. `SHOW CREATE EMITTER` preserves both request expressions and the explicit body
selection.

For example, the first emitter below sends each record of `outgoing` with its own method, path and
tenant header and a JSON body of two of its fields, and the second deletes without a body.
`api.example.com` stands for an endpoint the operator has already provisioned.

```plexus
CREATE HTTP CLIENT api CONFIG {
  'endpoint' = 'https://api.example.com',
  'timeout_ms' = 5000
};

CREATE ATTACHED HTTP REQUEST EMITTER deliver_event
  FROM outgoing
  TO api
    METHOD input.request_method
    PATH input.request_path
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING event_body_codec
  INHERIT event_id, payload
  INVOKE write_header('Content-Type', 'application/json'),
         write_header('X-Tenant', input.tenant),
         write_header('Idempotency-Key', input.event_id)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;

CREATE HTTP REQUEST EMITTER delete_event
  FROM outgoing WHERE input.request_method = 'DELETE'
  TO api
    METHOD 'DELETE'
    PATH input.request_path
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    WITHOUT BODY
  INVOKE write_header('Idempotency-Key', input.event_id)
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

#### HTTP requests

An HTTP emitter sends one request for each eligible record. For each batch it admits, it resolves
its materialized dependencies once and uses one execution snapshot, and every expression below
reads that snapshot. Each record then passes, in order: its source `WHERE`; construction and
finalization of its codec record; route `WHERE`; `METHOD`; `PATH`; and each `write_header`
invocation as it is written. A record that route `WHERE` filters evaluates no request field and
sends nothing, even when one of its request fields would fail.

The first request field that fails rejects its record through `ON MESSAGE ERROR` before any part of
its request is sent. A failed expression keeps its `evaluation` code; a value that is not a valid
request field has the code `validation`. A method or path failure has the operation `publish` and
names the request field, `method` or `path`, beside the fields its expression reads. A header write
failure has the operation `invoke` and the zero-based position of its invocation. A record whose
body the codec cannot encode when a flush releases it is rejected the same way with the operation
`encode`. The message never quotes a method, target or header value. The error handler of every
such rejection, and of any later rejection of an admitted request, reads the original input and the
materialized state its batch was admitted with; with a codec it also reads the attempted record as
`partial_output`.

The method keeps its spelling. The request goes to exactly the normalized target: encoded
separators such as `%2F`, repeated query parameters, a literal `+`, and the empty query of a
trailing `?` stay as they are. Header names compare without ASCII case and a later write replaces an
earlier value, so the request carries each name once, in lowercase and in ascending order of name;
an empty string is sent as an empty value. Two writes whose names are string literals naming the
same field, such as `write_header('X-Tenant', …)` and `write_header('x-tenant', …)`, are refused when
the statement is validated, because the earlier one could never be sent; a name an expression
computes is replaced at run time. An invalid or reserved write rejects its record even when a later
write would replace it, and the 128-header and 32 KiB bounds apply after every replacement. The body is exactly the bytes the codec produced, with no wrapper, array,
newline, form encoding or compression added; Nervix adds no `Content-Type`, and a declared
`Content-Encoding` does not transform the bytes. `WITHOUT BODY` sends zero content bytes.

A prepared request, with its method, target, headers and body, is kept until the endpoint answers
for it, so every retry resends it byte for byte: neither the request expressions nor the codec run
again, including volatile calls such as `uuid_v4()`. Retained bodies occupy node memory, which
memory pressure accounts for, until their requests complete.

One active emitter execution has at most one request awaiting final response headers, across all
source relays and branches it serves. It sends the requests a flush releases in publication order.
Independent executions have no total order, and an endpoint can apply a request after Nervix loses
its response, so this order does not settle ambiguous remote effects.

The HTTP emitter sends each request through hyper, which speaks HTTP/2 when the endpoint's TLS
negotiates it and HTTP/1.1 otherwise. It creates a connection for each request and closes it after
final headers, so an unread or stalled body cannot be reused as the next response. DNS resolution,
connection acquisition, TLS negotiation, the complete request send, interim responses and
complete final headers share the client's physical `timeout_ms`, and so do the requests of every
redirect the client follows within one attempt; queueing behind an earlier request and host retry
backoff do not consume the next attempt's timeout. Resolving the endpoint's host takes at most half
of what is left of `timeout_ms` when a request starts, whatever the name servers do, and each
address it resolves to is tried in turn within an equal share of what is left, so an address that
never answers leaves the next one time to connect and the request time to be sent and answered. HTTPS validates trust and
the destination hostname and uses the client's pinned CA and optional client certificate mounts.
Starting the emitter reads local configuration and sends no probe.

Over HTTP/1.1 the transport writes `Host` and `Connection: close`, and over HTTP/2 hyper sends the
authority as `:authority`. The request declares `Content-Length`: the body's length when a body
exists, and `0` for a `POST`, `PUT` or `PATCH` declared `WITHOUT BODY`, as servers that require
a length expect; any other method declared `WITHOUT BODY` carries no `Content-Length`. It adds
`Accept: */*` only when the application did not write `Accept`. It adds no `Accept-Encoding` or
`Content-Type`. The application may write `Accept`, `Content-Type`, `Authorization`, and `Cookie`
with `write_header`. Response cookies are
not retained; an authentication challenge sends no additional request. There is no library retry:
every repeat is a separate emitter attempt under its declared policy, and a redirect is followed
only as [HTTP client transport](#http-client-transport) describes.

An HTTP/1.1 response head may carry at most 100 fields in 417,792 bytes, and an HTTP/2 header
list stays under 16,384 bytes as HTTP/2 counts it, 32 bytes for each field beside its name and
value; each interim and final head is held to them. Malformed headers or
final framing, a status above `599` and any exceeded bound fail the attempt, even when the final
status line says `200`. Complete valid final `200`–`299` headers deliver the
record, including `202` and `204`; a stalled or failed body after those headers does not reverse
delivery. The emitter never waits for a body or uses bodies and trailers as graph data.

`408`, `425`, `429`, and `500`–`599` retain the current request and all later work for host retry.
`401`, `403`, and `407` do the same and report an authentication or authorization infrastructure
failure. DNS, connection, TLS, send, timeout, malformed response and loss before complete final
headers are also infrastructure failures, and so are a declared proxy that cannot be reached and a
tunnel the proxy refuses. Other `300`–`499` statuses and `101` reject only their record through
`ON MESSAGE ERROR`; later records proceed after that policy completes. A redirect the client does
not follow is such a status, `304` is not delivery, and `409` never implies an earlier delivery.
Rejection diagnostics include the numeric status but no evaluated URL, header value, request body
or response body.

#### HTTP client transport

An `HTTP CLIENT` means the same transport for HTTP polling and for the HTTP emitter. Both speak
HTTP through maintained implementations, Reqwest for polling and hyper for the emitter, over
HTTP/2 when the endpoint's TLS negotiates it and HTTP/1.1 otherwise, and:

- Without `follow_redirects`, or with `follow_redirects = false`, a redirect answer is the
  response: an emitted record is rejected with its status, and a poll fails with it.
- With `follow_redirects = true`, a `301`, `302`, `307` or `308` answer, and a `303` answer to a
  `GET` or `HEAD`, whose one `Location` names the client's own origin, the same scheme, host and
  port, sends the same request there again: the same method, headers and body, never turning a
  `POST` into a `GET`. A relative `Location` is resolved against the request it answers, and its
  fragment is dropped. One request follows at most 10 redirects, and every request of the chain
  shares its one `timeout_ms`. A `Location` on another origin or carrying credentials, an answer
  with no `Location` or with several, a `303` to any other method, which was handled where it was
  sent, and the answer to the tenth repeat are the response.
- `proxy` names an `http` or `https` proxy that every request of the client goes through. The node
  resolves only the proxy's host; an `http` target is sent to the proxy in absolute form, and an
  `https` target tunnels through it with `CONNECT`, after which TLS still verifies the configured
  host. An `https` proxy is verified with the client's trust. User information in the URL, such as
  `http://user:secret@proxy.internal:3128`, is sent as `Basic` proxy credentials, and
  `SHOW CREATE CLIENT` redacts a proxy URL that carries it. For the emitter, a proxy it cannot reach
  and a tunnel the proxy refuses, `407` included, keep the request pending for the next attempt.
- No proxy environment variable, `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY` or their
  lowercase forms, has any effect on either side.

```plexus
CREATE HTTP CLIENT partner CONFIG {
  'endpoint' = 'https://partner.example.com',
  'timeout_ms' = 5000,
  'follow_redirects' = true,
  'proxy' = 'http://egress.internal:3128'
};
```

#### HTTP retries and acknowledgements

A flush sends its requests in order and stops at the first one whose outcome is unresolved. That
request and every request prepared after it stay retained, and records admitted while they wait
queue behind them. A delivered request completes exactly its own record, and a rejected one resolves
its record through `ON MESSAGE ERROR`; neither is sent again, even when its flush held other records
that are retried. The retry resends the unresolved requests exactly as they were first sent, ahead
of any request prepared later.

Retries wait on the declared physical backoff: the first waits `BACKOFF`, each later one twice the
previous wait up to `MAX`, and a flush that completes resets the wait to `BACKOFF`. There is no
attempt limit; retrying continues until the request resolves or the emitter stops. A retryable
status (`408`, `425`, `429`, `500`–`599`, `401`, `403` or `407`) whose final headers carry exactly
one valid `Retry-After` field asks for a delay of its own: whole seconds, or an HTTP date in
IMF-fixdate, RFC 850 or asctime form, compared with actual UTC when the response arrives. A date
already past asks for no delay. The next attempt waits for the longer of that delay and the
backoff, even beyond `MAX`, and the backoff sequence itself continues unchanged. A `Retry-After`
that is missing, repeated in more than one field, malformed, such as fractional seconds, or
unrepresentable, such as a delay ending after the year 2262, asks for nothing. `Retry-After` never
turns a delivery or a rejection into a retry.

The request timeout, the backoff and a `Retry-After` delay are physical. `TIME RATE` scales the
emitter's `COLLECT FOR` and `FLUSH EACH` cadences and the domain time its expressions read, but
never these waits, and `FLUSH IMMEDIATE` keeps its physical batching window.

An `ATTACHED` emitter keeps the upstream acknowledgement of every unresolved request alive until
the request resolves: a delivery acknowledges it, and a rejection leaves it to the emitter's
message error policy. A `DETACHED` emitter acknowledges upstream at relay fan-out, yet still
retries, keeps later work waiting behind an unresolved request, and routes rejections. Prepared
requests and retry state live in memory only.

An endpoint can apply a request whose outcome Nervix never learns: the response is lost, the
connection fails, or the attempt times out after the endpoint acted. The retry then sends the same
request again, so the endpoint can receive it twice. Nervix generates no idempotency key and does
not interpret an endpoint's deduplication protocol. An endpoint that must recognize duplicates
should receive a stable key taken from the record, such as
`write_header('Idempotency-Key', input.event_id)`. A value generated during evaluation, such as
`uuid_v4()`, stays the same across these retries but not across an upstream redelivery.

`ALTER HTTP REQUEST EMITTER ... SET TO` restates the complete method, path, mode and body selection.
`SET CLIENT` changes the referenced client, `SET MODE` changes the retry policy, and `SET ENCODE
USING` selects a codec body. `DROP ENCODE` is invalid for HTTP; use a complete `SET TO <client> METHOD ...
WITHOUT BODY` replacement. The retained construction and error scopes must be valid for the new
selection.

Changing the HTTP emitter, method, path, client reference, body selection, or publishing mode uses
`ENTITY_PAUSE`. The emitter drains every admitted request with the client and request fields that
prepared it before the replacement starts. If that drain fails, the change fails and the old
definition remains active; an unanswered request is never sent to the proposed destination.
Changing only `FLUSH` is `DYNAMIC`, while adding or removing a source relay uses `DOMAIN_PAUSE`.
Changing the `CLIENT` definition itself follows the domain-pause configuration lifecycle.

`ALTER` retains construction and error clauses when a replacement omits construction, so a body-mode
change must remain valid with them. A complete `SET TO` body may replace construction and header
invocations along with the destination. The complete candidate is
validated before the transaction commits and has the same effective drain obligation as an
`ALTER` replacement. A destination that remains unavailable can prevent that drain. The operator
can repair it, or `STOP` the domain, change the emitter while stopped, and `START` again. Stopping
discards prepared requests and retry state; attached work depends on its source's redelivery
contract, and an endpoint that applied a request before losing its response may see a duplicate.

While confirmations or infrastructure retries are pending, the emitter stops consuming from its
relays and keeps upstream ACK leases alive. `FLUSH` still controls when and how much work enters a
flush; `MODE` controls when each record in that flush counts as published. `ATTACHED` and
`DETACHED` are orthogonal: a detached emitter acknowledges upstream immediately but still performs
its declared confirmations and retries for error visibility and backpressure.

#### HTTP inspection and metrics

`SHOW CREATE EMITTER` and canonical formatting render the method and path expressions, the header
invocations and the construction in canonical Plexus, with the explicit body selection. A configured
expression is rendered as Plexus under the ordinary sensitivity rules, so an explicitly leaked field
appears as its `leak_sensitive(...)` call and never as a value. `DESCRIBE EMITTER`
reports the request contract of the `deliver_event` example above in these lines:

```text
to: HTTP REQUEST api
options: METHOD input.request_method PATH input.request_path
mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
quiesce: none
envelope: none
batch: none
flush: FLUSH EACH 100ms MAX BATCH SIZE 1MiB
errors: ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
from: outgoing
codec: event_body_codec
body: codec
```

`body` reads `codec` for `ENCODE USING`, and `without body`, with `codec: none`, for `WITHOUT BODY`.
Header invocations appear in `SHOW CREATE EMITTER` rather than in `DESCRIBE`.

While a request is pending after a failed attempt — waiting out its backoff or `Retry-After`,
or sent again and not yet answered — `DESCRIBE EMITTER` reports that failure as its
`transient error`, with the `reconnect backoff` the retry waited and the `reconnect wait` still
left, and the node reports it as a runtime event. It clears once the pending request is delivered.
The failure names its cause and, for a response, its status, such as
`HTTP endpoint answered with retryable status 503`,
`HTTP endpoint answered with authentication or authorization status 401`,
`HTTP request timed out before complete final response headers`, or
`HTTP TLS handshake failed: invalid peer certificate: UnknownIssuer`. A DNS, connection, TLS, send
or response-header failure keeps the cause beneath it, such as the resolver's
`resolving 'api.example.com' failed: the name does not exist`. `ON GENERAL ERROR` has no say over a
response: a retryable failure stays pending and keeps attached acknowledgements alive, and a
refusal follows `ON MESSAGE ERROR`, even with `ON GENERAL ERROR IGNORE`.

A record's message error names the operation that rejected it and where applicable the request
field or invocation: `publish` with the field `method` or `path`, `invoke` with the zero-based
position of the header write, `encode` for a body the codec cannot produce, and, for a refused
request, the code `external` with the operation `publish` and a message carrying the numeric status,
such as `HTTP endpoint answered with status 404`. Message errors, emitter status, runtime events,
logs and metric labels never carry an evaluated target, a header value, a credential, a request body
or a response body.

The emitter's `sent` counters count each record once, when its request is delivered, however many
attempts that took, and never a record the endpoint refused. `messages_total` counts delivered
records, not attempts. `bytes_total` counts a codec body's record with the ordinary emitter payload
accounting, the logical Arrow data of the finalized record, while a request without a body adds
no payload bytes. A request's method, target and headers and the response add nothing to either
counter, and the counters carry only the ordinary graph labels, never a request value.

## Batching

An emitter may declare two hard limits for every batch it publishes, written after the complete emitter
clause and its route construction, and before the flush policy:

```plexus,ignore
BATCH MAX MESSAGES <n> MAX SIZE <bytes>
```

`MAX MESSAGES` is the most source records one batch carries, from 1 to 65,536. `MAX SIZE` is the
most bytes its encoded payload may occupy: a positive whole number followed by `B`, `KB`, `KiB`,
`MB`, `MiB`, `GB`, `GiB`, `TB` or `TiB`. Neither limit has a default, neither may be omitted, and
neither is derived from the other or from `FLUSH ... MAX BATCH SIZE`, which measures Arrow memory
rather than encoded bytes.

```plexus,ignore
CREATE KAFKA TOPIC EMITTER kafka_notifications
  FROM notifications
  TO kafka_main TOPIC 'notifications_out'
    MODE ACK PARALLEL MAX 100 ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  BATCH MAX MESSAGES 500 MAX SIZE 1MiB
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

| Emitters | Clause |
| --- | --- |
| Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, Sentry, Syslog, OTEL, Iceberg | Optional |
| ClickHouse, Postgres, MySQL, MongoDB | Required: a database write always carries several rows |
| HTTP | Unavailable: each request contains one source record |

A statement is rejected, naming the offending value, when:

- `MAX MESSAGES` is zero or above 65,536;
- `MAX SIZE` is zero, fractional, written without a unit, or past the 64-bit byte range;
- `MAX SIZE` is above 256 KiB on an SQS emitter, the largest SQS message Nervix sends;
- the clause is absent on a ClickHouse, Postgres, MySQL or MongoDB emitter;
- the clause is present on a Sentry emitter whose codec declares no `ON EMITTING BATCH`
  transformation, because one Sentry envelope carries at most one event and only that
  transformation can say where the members go;
- the clause is present on an emitter whose protobuf codec declares no `BATCH MESSAGE`, because
  protobuf has no self-delimiting sequence. Without an `ON EMITTING BATCH` transformation, the batch
  message must declare exactly one field, `repeated <MESSAGE>`, which the domain build checks
  against the compiled descriptors.

The codec forms are described in [Schemas and codecs](schemas-and-codecs.md#batch-transformations).
These checks cover the whole candidate graph, so replacing a codec that a batching emitter uses
is validated against that emitter too.

`SHOW CREATE EMITTER` renders the clause between the entity clause and `FLUSH`, with the size in the
unit it was written in. `DESCRIBE EMITTER` reports it on its `batch:` line, as
`batch: MAX MESSAGES 500 MAX SIZE 1MiB` or `batch: none`.

The complete contract for batch payloads — packing, containers per wire format, exact size
measurement and failure attribution for every emitter — is defined in
[Optional emitter batching](https://github.com/nervix-io/nervix/blob/main/docs/specifications/emitter-batching.md).
The database emitters bound every insert or bulk write by both limits, as
[Database writes](#database-writes) describes. An OTEL emitter with the clause bounds each export
request; without it, OTEL keeps one request per pending Arrow batch, and with or without it a
request never combines records of two pending Arrow batches. Iceberg keeps its data-file and commit
boundaries.

### Batch payloads

A Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, Sentry or Syslog emitter that declares
the clause publishes batch payloads instead of one payload per record. Each payload is one message,
one event or one frame whose value is the codec's
[batch container](schemas-and-codecs.md#batch-containers): an array for JSON, CBOR, YAML and Avro, a
single `batch` key for TOML, a single `batch` root element for XML, the codec's `BATCH MESSAGE` for
protobuf and one RFC 5424 frame for `SYSLOG`, or the single value an `ON EMITTING BATCH`
transformation built instead.

The emitter walks the Arrow carriers released by one flush in arrival order, and each carrier's
eligible rows in source order. A payload can span carriers only while their source relay, exact
named and concrete branch, key, ordered written headers, ordering group, envelope and, with a
`SYSLOG` codec,
syslog header other than the timestamp agree. A different value seals the open payload before the
next row is considered; no row is skipped over or reordered. A carrier retains its own execution
snapshot and row membership for errors and acknowledgements. A payload with one member keeps the
container shape, such as a one-element array, and a carrier with no eligible row publishes nothing.
Batching adds no timer: `FLUSH` alone decides when records leave, and a partial payload is published
exactly like a full one.

Arrival order is established at the emitter. Each input queue keeps its own order, and ready
inputs can interleave. Sequential `MODE NO ACK` HTTP responses from different ingestors do
not establish a common receive order at the emitter. Compatible rows separated by another source
stay in separate payloads even when their requests were accepted consecutively.

`MAX SIZE` is the exact length of the payload: escaping, UTF-8, base64, field names, separators,
length prefixes and the container's own brackets all count, and a batch transformation is measured
by the bytes of its output. Nervix never estimates the size. It encodes the candidate into a buffer
that refuses to grow past the limit and abandons the encoding at the first byte that would not fit,
so an oversize payload is never built in full and never reaches the destination. A payload of
exactly `MAX SIZE` bytes is published. A candidate whose encoding reached the limit is halved: its
first half is re-encoded under the same limit and the rest returns to the front of the queue. Each
halving is encoded again, because a batch transformation may write more bytes for fewer members, so
a candidate of `n` members takes at most `⌈log2(n)⌉ + 1` encodings. The bound covers the payload
only: keys, headers and the framing a transport adds around the payload are outside it.

Schemaful JSON rows use the same bounded writer as other codecs. Their columnar encoder stops at
the first write over the limit; its string classifier and direct column writes do not change exact
byte measurement, halving, or the per-record error policy.

A record is rejected alone, through `ON MESSAGE ERROR` with operation `encode`, when its member value
cannot be produced — its `ON EMITTING` transformation fails, or, without a batch transformation, its
value is not one the format can write — and when a payload of it alone still exceeds `MAX SIZE`.
The second case has code `validation` and a message naming the codec, its encoding and the limit,
such as `emitter 'bounded_events' codec 'event_codec' JSON payload exceeds MAX SIZE 32B`. The records
around it are packed as usual.

A batch transformation that yields no output, more than one output, fails to evaluate, or yields a
value the format cannot write fails the whole candidate. Every member follows `ON MESSAGE ERROR`
with operation `encode`, the same error reference and a message naming the emitter, the codec, the
cause and the member count, such as
`emitter 'kafka_notifications' codec 'notification_envelope' ON EMITTING BATCH produced no output
for a batch of 3 messages`. The code is `evaluation`, or `validation` for a value the format cannot
write. The message never quotes a payload value, so it does not repeat the program's own error text,
and Nervix does not subdivide such a batch to look for a member to blame; use a smaller
`MAX MESSAGES`, or no batching, where per-record attribution matters.

One payload is one publish. Its confirmation delivers every member, and a destination's rejection
of it rejects every member with one shared error reference. `ACK PARALLEL MAX <n>` therefore counts
payloads, not records. `nervix_messages_total` keeps counting source records.

A payload whose outcome the emitter did not learn — the destination failed, stopped answering, or
its confirmation timed out — is retained exactly as it was written. The retry, or a force flush or
drain before it, writes the same bytes with the same members again, so a duplicate a retry produces
is the payload the destination may already hold, never a regrouped one. Payloads the destination
confirmed or rejected in the failed attempt are not written again, and records that arrive while a
payload is retained go into later payloads. The members of a retained payload keep their upstream
acknowledgements alive until it resolves; a `DETACHED` emitter still acknowledges upstream at relay
fan-out and still retries the payload. Where the destination answers for every record itself, as a
MongoDB bulk write does per document, a retry carries only the records it left unresolved.

Member values and containers are working values that exist only while the released carriers are
encoded; the records themselves stay in Arrow batches. The packer holds at most `MAX MESSAGES`
prepared members in one candidate, even when the members come from successive carriers. For a
codec with jaq transformations, member preparation, batch transformations and every re-encoding
run in the same job on the node's extension workers that already runs `ON EMITTING`, so a slow
program never stalls the emitter task. A node whose extension workers cannot take the job now
refuses it, and the emitter keeps its rows and retries them as it retries a failed publish. Candidate work is bounded by `MAX MESSAGES` and the
encodings above; `MAX SIZE` bounds what is written.

### Broker and message emitters

Kafka, Pulsar, RabbitMQ, Redis Pub/Sub, MQTT, NATS, ZeroMQ and SQS publish each batch payload as
one message through the connector's own driver: one Kafka record value, one Pulsar message
payload, one AMQP message body, one `PUBLISH` to a Redis channel, one MQTT `PUBLISH` packet, one
NATS message on the subject, one single-frame ZeroMQ message or one SQS message body. The message
carries once what its members share. The Kafka record key and the Pulsar partition key are the
members' concrete branch key; the written headers become Kafka and NATS headers, Pulsar
properties, AMQP headers or SQS message attributes; and an SQS FIFO message carries the members'
message group. Without the clause every record stays its own message, carried the same way.

Batching keeps every publishing mode's completion point. `NO ACK` and `QOS 0` complete when the
producer, channel, client or socket accepts the batch message; the confirming modes wait for one
delivery report, broker receipt, publisher confirm, `PUBACK`, completed QoS 2 handshake or
JetStream `PubAck` for it. `ACK TIMEOUT` bounds that one wait, and a batch message whose outcome
stays unknown is retained for the retry described above.

SQS `MODE BATCH` remains a request shape and is independent of the clause. One `SendMessageBatch`
request carries up to ten batch messages as separate entries within the 256 KiB request limit, and
never two messages of one FIFO message group. The service answers for each entry, and an entry's
answer applies to every member of the batch message it carries. Entries are never merged into one
message, and one entry never carries more than one batch message.

`MAX SIZE` bounds the payload, while the destination bounds the whole message it receives. Where
the connector learns the destination's limit, it checks every message against it before writing,
counting what it writes around the payload:

| Emitter | Limit checked before a message is written | Counted around the payload |
| --- | --- | --- |
| Kafka | The producer's `message.max.bytes` client setting | The record key, headers and record overhead |
| MQTT | The Maximum Packet Size the broker declared in its latest `CONNACK`, and the largest packet the protocol can express | The fixed header, topic, packet identifier and property length |
| NATS | The `max_payload` the server announced | The message headers |
| Pulsar | The `maxMessageSize` the broker announced when the producer's connection opened | The message metadata: the properties, the partition key and the producer's own fields |
| SQS | 256 KiB | Message attribute names, types and values, and the FIFO message group |

A batch message that does not fit is never written. Every member follows `ON MESSAGE ERROR` with
code `external`, operation `publish` and one shared reference, and the message names the size and
the limit, such as `mqtt rejected record: MQTT PUBLISH packet of 1200090 bytes exceeds the broker's
maximum packet size of 1048576 bytes`. The messages around it are still written. Declare a
`MAX SIZE` that leaves room for the metadata to keep batches from reaching the limit.

A Pulsar broker announces its `maxMessageSize` on every connection, so a producer that reconnects
to another broker checks against the new one's. A Pulsar topic can also set a smaller
`maxMessageSize` policy of its own. Only the broker applies it, measuring the metadata and payload
with ten bytes of framing, so such a message is written and refused afterwards. In `MODE ACK` the
refusal rejects every member the same way, with the broker's reason, such as
`PULSAR rejected record: the Pulsar broker does not allow the message: Exceed maximum message
size`. In `MODE NO ACK` the message was already delivered when the producer accepted it, so the
refusal is reported at finish through the emitter's typed failure, after that earlier completion.

The remaining limits are not visible to the client. RabbitMQ's `max_message_size` is a broker
setting that AMQP never tells a client, and the broker compares it with the message body alone. A
batch message whose body is larger reaches the broker, which refuses it by closing the channel it
arrived on and names the limit, and Nervix rejects the message on that answer, in every publishing
mode: every member follows `ON MESSAGE ERROR` with code `external`, operation `publish` and one
shared reference, and the message names the size and the limit, such as `rabbitmq rejected record:
message body of 1200090 bytes exceeds the broker's max_message_size of 1048576 bytes`. The broker
discards the messages written after the refused one with the channel, and Nervix writes them again
on a new channel. A message written ahead of it that the broker had not confirmed yet may have
reached its queue, as it can on a quorum queue, so the write then fails as an infrastructure
failure, and its retry carries every message but the rejected one and those already confirmed.
Headers do not count, so a `MAX SIZE` no larger than `max_message_size` keeps every batch message
within it.

Redis rejects a value above its `proto-max-bulk-len` itself, and that rejection follows
`ON MESSAGE ERROR` like the ones above. ZeroMQ fixes no limit; a receiving socket configured with a
maximum message size drops a larger message after the sending socket has accepted it.

### Database writes

ClickHouse, Postgres, MySQL and MongoDB always write several rows at once, so their emitters
require the clause. A flush hands the emitter every run of rows from successive Arrow carriers of one
source relay and concrete branch, in the order the emitter would have published them, and the emitter
writes the run as inserts or bulk writes of at most `MAX MESSAGES` rows. Every source record stays
one row or one document: an array or `VEC` value is one column value, never a set of rows, and no
destination schema changes.

| Emitter | One write | `MAX SIZE` measures |
| --- | --- | --- |
| ClickHouse | One `INSERT INTO <table> FORMAT JSONEachRow` request | The request body, one JSON line and newline per row, before the driver compresses it with LZ4 |
| Postgres | One `INSERT ... SELECT ... FROM unnest(...)` statement binding one text array per mapped column | The statement text and every bound array as the extended-query protocol encodes it: a 20-byte array header, then a four-byte length and the value's text for each row |
| MySQL | One multi-row `INSERT ... VALUES (...), (...)` statement | The statement text and every bound value as the binary protocol encodes it |
| MongoDB | One unordered `insert_many`, or one unordered bulk write of upserts under `ON CONFLICT` | Each inserted document as the driver writes it, with the `_id` it adds to a document that has none, or each upsert's filter and update documents |

Everything around the measured payload is framing outside `MAX SIZE`: HTTP headers and the
statement in the ClickHouse URL, each Postgres parameter's own length word and the protocol
messages, the MySQL packet headers and the parameter types and null bitmap of its execute packet,
and the command and operation fields around MongoDB documents, the fields the driver adds to every
command, and the wire-protocol header. The measured size is exact, and a write of exactly
`MAX SIZE` bytes is sent. A candidate that measures more is halved, and its first half is measured
again while the rest returns to the front of the next candidate. A row whose own write still
measures more follows `ON MESSAGE ERROR` with code `validation`, operation `encode`, and a message
naming the limit, such as `Postgres insert of one row measures 1219 bytes, above MAX SIZE 600B`.
The rows around it are written.

The destination's own limits bound every write too. A MySQL statement binds at most 65,535
placeholders, so an insert carries at most as many rows as fit them: 13,107 rows of five mapped
columns, whatever `MAX MESSAGES` allows. Postgres reads no protocol message above 1,073,741,822
bytes, so a Postgres emitter accepts `MAX SIZE` up to `1073741822B` and is refused above it at
`CREATE`; a write's measured size is never smaller than the message that carries its arrays, so
every Postgres insert stays within the message the server reads. A MongoDB document holds at most
16 MiB, so a row whose document exceeds it follows `ON MESSAGE ERROR` as an `external` `publish`
error naming 16777216 bytes before any write, and the rows around it are written. MongoDB takes
100,000 writes in one command, more than `MAX MESSAGES` allows, and its driver divides a write
larger than the `maxMessageSizeBytes` the server reports into several commands whose documents it
still answers for one by one.

A write that fails for a reason specific to its rows — a constraint, a value its column cannot
hold, a packet the server refuses as too large — is written again one row at a time, so the rows
the destination accepts land and only the rows it refuses follow `ON MESSAGE ERROR`, each with the
destination's own reason. This includes a Postgres `ON CONFLICT DO UPDATE` whose insert carries one
key twice, which Postgres refuses for the whole statement: row by row, the later row updates the
one before it, exactly as the rows would one insert at a time. MySQL applies the rows of one insert
in order,
so a key repeated in one write is updated by its later row under `DO UPDATE` and keeps its first
row under `DO NOTHING`. A MongoDB write is unordered: one server applies its upserts in the order
the write lists them, with the same result, while a sharded deployment may apply them in parallel.
MongoDB names every document it rejects, so its healthy documents are delivered by the write itself
without a row-by-row pass.

A write whose outcome is unknown — the connection failed or the destination stopped answering — is
retried with its rows unresolved. The rows before it stay delivered, so the retry packs the same
rows into the same writes again, and a write the destination already took is taken twice; use a
conflict policy where duplicates matter. Where MongoDB answered for some documents of a write, the
retry carries only the documents it left unresolved. That includes a write whose write concern the
deployment could not satisfy: the documents the server applied stay applied, so they are delivered
and never written again, as [MongoDB](#mongodb) describes.

Each emitter writes a `BYTES` value in its native binary form: the octets themselves in a ClickHouse
`String`, the `bytea` hex format Postgres reads back as the same octets, the octets bound to a MySQL
binary or blob column, and generic BSON binary data in MongoDB. An `ARRAY` or `VEC` value is a JSON
array in ClickHouse's `JSONEachRow` and in the JSON text Postgres and MySQL bind for it, and a BSON
array in MongoDB. A `BYTES` element of a ClickHouse array keeps its own octets, as a `BYTES` value
outside an array does. Inside the JSON text Postgres and MySQL bind, a `BYTES` element is the padded
base64 text every Nervix JSON value carries octets as.

## Altering emitters

`ALTER <CONNECTOR> <ENTITY> EMITTER` applies one or more comma-separated operations in written order:

```plexus,ignore
ALTER <CONNECTOR> <ENTITY> EMITTER <emitter>
    ADD FROM <relay> [WHERE <expr>]
  | DROP FROM <relay>
  | ALTER FROM <relay> SET WHERE <expr>
  | ALTER FROM <relay> DROP WHERE
  | SET TO <complete destination body>
  | SET MODE <transport-specific mode body>
  | SET CLIENT <client>
  | SET ENCODE USING <codec>
  | DROP ENCODE
  | SET COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]
  | DROP COLLECT
  | SET ATTACHED
  | SET DETACHED
  | SET BATCH MAX MESSAGES <n> MAX SIZE <bytes>
  | DROP BATCH
  | SET FLUSH EACH <duration> MAX BATCH SIZE <bytes>
  | SET FLUSH IMMEDIATE
  | SET COMMIT EACH <duration> MAX SIZE <bytes>
  [, ...];
```

The ALTER head must name the stored emitter's connector and entity. A mismatched head is refused
with both phrases in the diagnostic. Changing that kind requires dropping the emitter and creating
one of the new kind.

`SET TO` accepts that entity's complete body after `TO`: reference, quoted locator and entity
options, required `MODE`, its codec, direct `VALUES` mapping or explicit `WITHOUT BODY`, and its
[envelope clauses](#envelope-clauses). It replaces the payload selection and the envelope. An
explicitly written construction replaces the construction; omitting construction keeps the existing
one. Batching and flush policies remain in place.
`SET MODE` accepts only policies that entity represents. `SET CLIENT` changes its client reference.
`SET COMMIT` is available for Iceberg. `SET ENCODE USING` and `DROP ENCODE` are available only for
encoded destinations; dropping an absent codec fails, and HTTP requires an explicit body selection.
`SET BATCH` adds or replaces a clause, while `DROP BATCH` fails if it is absent or required by the
entity. These operations validate atomically against the complete candidate graph.

`ADD FROM` rejects an already configured relay. `DROP FROM` cannot remove the final input.
`ALTER FROM ... SET WHERE` adds or replaces that source's predicate; `ALTER FROM ... DROP WHERE`
fails when the source has no predicate.

Changing only `FLUSH` is a `DYNAMIC` update. The live emitter keeps its pending Arrow batches,
installs the new cadence, and receives a force-flush kick, so buffered output is neither discarded
nor re-encoded. Source-predicate, emitter, publishing-mode, client, codec, collection, batching, and
attachment changes use
`ENTITY_PAUSE`: Nervix gates all of the emitter's source relays, drains collected input and pending
emitter output, replaces that emitter task, and releases the gates. Changing source membership uses
`DOMAIN_PAUSE` because it changes graph topology. Other relays continue flowing during an entity
pause; sibling consumers of a gated source may see bounded backpressure until the gate is released.
The complete candidate graph is validated before any change is committed.

## Codec-emitter construction

Codec emitters are transforming routes. They begin with an empty codec-schema payload and use
explicit inheritance and ordered assignment:

```plexus
CREATE IF NOT EXISTS KAFKA TOPIC EMITTER kafka_notifications
  FROM notifications
  TO kafka_main TOPIC 'notifications_out'
    MODE ACK PARALLEL MAX 1000 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL EXCEPT raw, secret
  SET secret = leak_sensitive(input.secret),
      normalized = lower(input.raw)
  WHERE output.active
  INVOKE write_header("tenant", input.tenant),
         write_header("route", output.normalized)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

`message.field` reads the [working message](working-message.md), `input.field` always reads the
source relay row, and `output.field` requires prior initialization. Relay-qualified fields are
invalid. There is no implicit identity transformation and no `UNSET`; use `INHERIT ALL EXCEPT`.

External sensitivity is strict. Every sensitive payload value requires `leak_sensitive(...)` or an
explicit `INHERIT field LEAK SENSITIVE`, even when the codec target field is also sensitive.

## Direct-emitter values

Database, object-store, and OTEL direct emitters construct external name-keyed mappings:

```plexus,ignore
VALUES {
  "tenant" = input.tenant,
  "normalized" = lower(input.action),
  "secret" = leak_sensitive(input.secret)
}
WHERE input.active
```

Entries are independent and do not create variables. Order does not affect evaluation, duplicate
external keys are invalid, `output` is unavailable, and sensitive values require explicit leakage.
Bare fields, `message.field`, and `input.field` read the source row. Direct emitters reject
`INHERIT` and all current direct emitters reject `INVOKE`.

Nervix checks these mappings against the source schema during `CREATE` and `ALTER`, before opening
the destination. Database and Iceberg columns accept Plexus scalar, array and vector types; OTEL
fields require their documented exact types. For example, an I32 source field can supply
`severity_number`, while the integer literal `9` is inferred as I64 and is refused for that field.
OTEL attributes accept supported scalar values and arrays or vectors of those values; `RESOURCE`
entries must be literal expressions. Mapping types are never implicitly coerced. A refused
alteration preserves the emitter's accepted configuration.

## Header invocations

`write_header` is a side-effect function. It accepts statically non-null `STRING` name and value
expressions and is valid only as a top-level `INVOKE` call. Sensitive values require
`leak_sensitive`. Calls execute left to right after payload finalization and route filtering. Header
mutations are staged in a temporary route-local envelope; invocation failure prevents payload and
partial-envelope publication.

Each emitter that writes transport headers declares a
[header model](./connector-contract.md#header-models), which `DESCRIBE EMITTER` shows as its
`headers:` line:

<!-- connector-matrix:emit-headers:start -->
| Emitter | Header model | What a record's `write_header` calls become |
| --- | --- | --- |
| `HTTP REQUEST` | `sorted_by_name`, lowercase names | one value per name, the last one written for it; names in ascending byte order; a name is recorded in ASCII lowercase, so names that differ only in letter case are one name |
| `KAFKA TOPIC` | `ordered_repeats`, exact names | every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| `PULSAR TOPIC` | `last_wins`, exact names | one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |
| `RABBITMQ QUEUE` | `sorted_by_name`, exact names | one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| `RABBITMQ EXCHANGE` | `sorted_by_name`, exact names | one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| `NATS SUBJECT` | `repeats_by_name`, exact names | every value of a name, together and in the order written; the order of different names is not declared; a name matches only a name spelled the same, letter case included |
| `SQS QUEUE` | `last_wins`, exact names | one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |

`CLIENT CONSUMER`, `REDIS PUBSUB`, `MQTT TOPIC`, `ZEROMQ PUSH`, `SENTRY EVENT`, `SYSLOG COLLECTOR`, `OTEL LOGS`, `OTEL TRACES`, `OTEL METRICS`, `CLICKHOUSE TABLE`, `POSTGRES TABLE`, `MYSQL TABLE`, `MONGODB COLLECTION`, `ICEBERG TABLE` write no transport headers and refuse `write_header`.
<!-- connector-matrix:emit-headers:end -->

Where an emitter keeps one value per name, a second `write_header` whose name is a string literal equal,
as the emitter compares names, to an earlier literal one is refused when the statement is validated,
with a message such as `emitter 'out' in domain 'shop' writes header 'route' at INVOKE #1 and again
at #3, but PULSAR TOPIC emitters keep one value per header name, so the later write would replace
the earlier one on every record`. A name an expression computes is not compared then: at run time a
later write of the name replaces the earlier value, as the model states.

Emitter expressions use the same typed surface as other runtime nodes:

- arithmetic: `+`, `-`, `*`, `/`, `%`
- comparisons and boolean logic: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `NOT`
- explicit conversions: `expr AS TYPE`, and `TRY_CAST(expr AS TYPE)`, which yields a typed null
  instead of failing the message
- JSON extraction: `JSON_VALUE(doc, '$.path' AS TYPE)`, `TRY_JSON_VALUE(doc, '$.path' AS TYPE)`,
  and `JSON_EXISTS(doc, '$.path')`
- built-ins: string, null-handling, numeric, regex, and contextual functions such as `lower`, `coalesce`, `abs`, `regexp_substr`, `now`, and `uuid_v4`

See [Expression Functions](filter-map-functions.md) for the full function reference.

That expression surface applies to the full Nervix internal schema type set:

- `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- `F32`, `F64`
- `BOOL`, `STRING`, `DATETIME`

Nested conditions and chained calls such as `contains(lower(trim(input.raw)), 'warn')` are supported
before encoding.

Client-backed emitters can use resource-mounted client config values for TLS material and other file-based settings. See [Resources](resources.md#client-config-mounts).

## Envelope clauses

An emitter sets what its transport carries beside each payload with envelope clauses. They follow
the payload — `ENCODE USING <codec>`, `VALUES { … }` with its `ON CONFLICT`, `WITHOUT BODY`, or a
client emitter's `MODE` — and come before the construction:

```plexus,ignore
TO <client> [<locator>] [<entity options>]
  MODE <mode>
  (ENCODE USING <codec> | VALUES { … } [ON CONFLICT …] | WITHOUT BODY)
  [PERSISTENT] [MANDATORY] [RETAIN]
  [TIMESTAMP <expression>]
  [DELIVER AFTER <duration>] [DELAY <duration>]
  [REPLY TO <expression>] [MESSAGE ID <expression>] [DEDUPLICATION ID <expression>]
  [PROPERTIES { '<key>' = <expression>, … }]
[INHERIT …] [SET …] [WHERE …] [INVOKE …]
```

A statement writes each clause at most once, in this order, and only where the emitter's entity
declares it; completion offers exactly the clauses the entity declares. `PROPERTIES` sets keys of
the entity's closed property set, each at most once and in any order. A clause or key the entity
does not declare is refused where it is written, with the entity and the clause or key named, and
the registry refuses it again in a Model that reaches it another way. The
[capability matrix](connector-capabilities.md) lists the clauses and properties each entity
declares, and each entity's section below says what its transport does with them. `KAFKA TOPIC`
declares `TIMESTAMP`, which [Kafka](#kafka) writes as each record's create time, and the two
[RabbitMQ](#rabbitmq) entities declare `MANDATORY`. No entity declares another clause or a property
yet, and every emitter refuses each clause its entity does not declare.

| Clause | Written with | Value every record carries |
| --- | --- | --- |
| `PERSISTENT`, `MANDATORY`, `RETAIN` | nothing | the flag, set |
| `TIMESTAMP` | an expression | its non-null `DATETIME` |
| `DELIVER AFTER`, `DELAY` | a duration literal | the duration, greater than zero |
| `REPLY TO`, `MESSAGE ID`, `DEDUPLICATION ID` | an expression | its non-null `STRING` |
| `PROPERTIES` key | an expression | its non-null value of the key's declared type: an unsigned or signed integer of a declared width, `BOOL`, `STRING`, `DATETIME` or `BYTES` |

An envelope expression reads what a header write reads: the source record as `input`, the record
the emitter publishes as `message`, and the output its route constructs as `output` where it
constructs one — a codec or client emitter that writes a construction, or an HTTP emitter with an
encoded body — together with the materialized state, lookups and UDFs the emitter may reach. An
emitter without a construction publishes its source records, so its `message` is the source record
and it has no `output`. Every value has the clause's or key's exact type and is statically non-null,
and a sensitive value requires `leak_sensitive`, as `CREATE` and `ALTER` check with a message such
as `envelope TIMESTAMP requires an exact, non-null, non-sensitive DATETIME value`.

Nervix evaluates the envelope once for every batch it admits, after the route kept and finalized
its rows, with the execution time and materialized-state snapshot the route used. A record whose
envelope expression fails or produces no value is not sent. It follows `ON MESSAGE ERROR` as an
`evaluation` error of the `publish` operation whose message begins `envelope` and names the clause
or key, such as `envelope MESSAGE ID expression failed with division_by_zero at …`, and the
emitter's other records are still sent. A record the route's `WHERE` drops is never evaluated.

Each record reaches its connector with its evaluated envelope. Records whose envelopes differ never
share a [batch payload](#batch-payloads). A payload is retained with its envelope until the
connector answers for it, so every attempt, including the one after an unknown outcome, sends the
envelope the first attempt sent. `DESCRIBE EMITTER` prints the envelope on its `envelope:` line as
the statement writes it, or `envelope: none`, and `SHOW CREATE` writes it in the destination's
section. `ALTER … SET TO` replaces the envelope with the rest of the destination body, so a body
that writes no envelope clause removes them. A changed envelope is a destination change, applied
with `ENTITY_PAUSE`.

## TLS Client Configuration

Emitter TLS is configured on the referenced `CLIENT` exactly the same way as ingestor TLS.

Common pattern:

```plexus,ignore
CREATE [IF NOT EXISTS] <kind> CLIENT <name>
  MOUNT <tls_resource> VERSION <n>|LATEST
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific expectations:

- `KAFKA`: pass-through to librdkafka, except `allow.auto.create.topics`, which `CREATE KAFKA CLIENT` refuses. The emitter's producer keeps librdkafka's producer default, so a broker that allows auto-creation creates a missing topic on the first publish; provision topics explicitly. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and optional `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `RABBITMQ`: use `amqps://...` in `addr`; Nervix honors `tls_ca_file` and presents `tls_cert_file` with `tls_key_file` as its client certificate; see [RabbitMQ](./ingestors.md#rabbitmq).
- `REDIS`: use `rediss://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`; Nervix requires `tls_ca_file` and supports `tls_cert_file` plus `tls_key_file`. A plaintext `mqtt://` address reads no TLS file, so `CREATE MQTT CLIENT` refuses all three with it.
- `NATS`: use `tls://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`; Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`, which `CREATE PULSAR CLIENT` refuses with a plaintext `pulsar://...` address. Pulsar client certificate authentication is not currently exposed.
- `SQS`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file` and optional `timeout_ms`,
  and `CREATE SQS CLIENT` refuses a `tls_ca_file` that holds no certificate it could trust. An SQS
  connection presents no client certificate, so `CREATE SQS CLIENT` refuses `tls_cert_file` and
  `tls_key_file`.
- `SENTRY`: the referenced `SENTRY CLIENT` carries an `https://...` `dsn`; its `tls_ca_file` adds
  a trust root beside the platform's native roots, and `tls_cert_file` with `tls_key_file` presents
  a client certificate.
- `OTEL`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file`, `tls_cert_file`, and
  `tls_key_file` for both OTLP/gRPC and OTLP/HTTP-protobuf. `tls_ca_file` adds a server trust root,
  and TLS file keys with an `http://` endpoint are refused; see [OTEL](#otel).
- `CLICKHOUSE`: use an `https://...` `addr` with mounted `tls_ca_file` and required positive
  `timeout_ms`; HTTP addresses refuse TLS file keys.
- `POSTGRES`: use `sslmode=verify-full` in the `addr` URL; Nervix honors `tls_ca_file`, `tls_cert_file`, and `tls_key_file`. `sslmode=disable` is the only other accepted policy.
- `MYSQL`: enable TLS with `require_ssl=true` in `addr` or with mounted TLS files. Nervix honors `tls_ca_file` and paired `tls_cert_file`/`tls_key_file` client identity files; see [MySQL](#mysql) for the URL verification policy.
- `MONGODB`: Nervix honors `tls_ca_file`, and `tls_cert_file` with `tls_key_file` as its client
  certificate; any of them enables TLS without `tls=true` in `addr`, and an `addr` with `tls=false`
  refuses them. See [MongoDB](#mongodb).
- `SYSLOG`: select `'protocol' = 'tls'`. Optional `tls_ca_file` adds a server trust root;
  optional `tls_cert_file` and `tls_key_file` configure client authentication and must appear
  together.
- `S3`, `GCS`, `AZURE_BLOB` and `ICEBERG_REST`: the object-store and catalog clients of an Iceberg
  emitter use TLS for every HTTPS request: the default AWS and Google endpoints, an `https://`
  `endpoint`, `service_path` or `uri`, and a `wasbs://` or `abfss://` location. Each verifies the
  server against the bundled WebPKI roots, the platform's native roots and its own `tls_ca_file`,
  and `tls_cert_file` with `tls_key_file` presents a client certificate. The catalog client's files
  apply to catalog requests and the object-store client's to data files and credential calls; see
  [Iceberg](#iceberg).

Example Kafka TLS emitter client:

```plexus
CREATE IF NOT EXISTS KAFKA CLIENT kafka_tls
  MOUNT dev_tls VERSION 1
  CONFIG {
    'bootstrap.servers' = '127.0.0.1:9094',
    'security.protocol' = 'ssl',
    'ssl.ca.location' = '{{ dev_tls }}/ca.pem'
  };
```

## Client Deadlines

A client's `timeout_ms` and `connect_timeout_ms` mean the same thing for every connector that reads
them. `timeout_ms` bounds the whole of one request or operation: name resolution, connection setup,
TLS and the answer. `connect_timeout_ms` bounds establishing a connection: name resolution, the
address attempts, TLS and the protocol's own handshake. Each value is a positive whole number of
milliseconds.

`HTTP`, `SENTRY`, `OTEL`, `SQS`, `CLICKHOUSE`, `NATS`, `ZEROMQ`, `SYSLOG` and the Iceberg
emitter's object-store clients read `timeout_ms`; an `HTTP` request emitter's client requires it,
and a `SYSLOG` emitter's client bounds writing and flushing each frame by it. A `NATS` client's
`timeout_ms` bounds a Core NATS write: its hand-over to the client and the client's flush to its
socket. A `RABBITMQ` client's `timeout_ms` bounds the round trip that ends a `NO ACK` write.
`RABBITMQ`, `REDIS`, `SYSLOG` and `ZEROMQ` clients read `connect_timeout_ms`, and allow 30 seconds
when it is left out; a `REDIS` command pool also waits no longer than that to check out a
connection. A `RABBITMQ` client's `connect_timeout_ms` covers resolving the broker, the address
attempts, TLS, the AMQP handshake, opening the emitter's channel, selecting publisher confirms under
`ACK` and checking its queue. A `ZEROMQ` client's `connect_timeout_ms` covers a connect-mode
socket's connection and its ZMTP handshake, and its `timeout_ms` bounds each send. A request, send,
write, connection or round trip that runs out of its deadline becomes the emitter's transient
status, and the emitter retries on its policy; a deadline that runs out is reported naming the key
and its bound. [Connector Capabilities](connector-capabilities.md) lists each entity's network waits
and what bounds each one, including the waits nothing bounds yet.

## Supported Emitter Emitters

### Kafka

<!-- connector-matrix:emit-kafka-topic:start -->
```plexus,ignore
CREATE KAFKA TOPIC EMITTER <name>
  FROM <relay>
  TO <client> TOPIC 'orders'
    MODE <mode>
    ENCODE USING <codec>
    [TIMESTAMP occurred_at]
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | KAFKA client |
| Locator and validation rule | TOPIC 'orders' (kafka_topic) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | `write_header`; `ordered_repeats`, exact names: every header in the order it was written; a repeated name keeps each of its values in its place; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | TIMESTAMP occurred_at |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | records the producer queues under ACK: driver default, queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576; when full, holds back what fills it<br>records the producer queues under NO ACK: driver default, queue.buffering.max.messages 100000, queue.buffering.max.kbytes 1048576; when full, fails the publish for the host to retry<br>records awaiting confirmation under ACK: set by ACK PARALLEL MAX; when full, holds back what fills it |
| Network waits | a produce request: driver default, request.timeout.ms, 30 s<br>delivery under NO ACK: driver default, delivery.timeout.ms, 300 s<br>delivery under ACK: `ACK TIMEOUT`<br>destroying a producer, for its polling thread and the reports of the records its purge failed: driver default, 100 ms poll, then a 500 ms flush |
| Driver limits | Every librdkafka key passes through, so a client can change each driver default above.<br>message.max.bytes 1000000<br>A record whose payload exceeds the whole queue.buffering.max.kbytes is rejected through ON MESSAGE ERROR.<br>A producer whose last handle is gone is destroyed by one connector control job, which waits for a place rather than being refused; no Nervix key changes rust-rdkafka's 100 ms poll or 500 ms flush, and librdkafka then joins its threads, a broker thread still resolving a name included. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [KAFKA TOPIC capability and mirror reference](connector-capabilities.md#kafka-topic-emit).
<!-- connector-matrix:emit-kafka-topic:end -->

`TOPIC` uses the same rule as [Kafka ingestion](ingestors.md#kafka): 1 to 249 ASCII letters,
digits, `.`, `_` and `-`, other than `.` and `..`, produced to exactly as written. `write_header`
writes the record's headers in the order the program writes them. The emitter declares the
`TIMESTAMP` envelope clause, no other envelope clause and no `PROPERTIES` block. The `KAFKA CLIENT`
it names requires `bootstrap.servers` and refuses the Nervix-owned keys the
[Kafka ingestion section](ingestors.md#kafka) lists.

`TIMESTAMP <expression>` sets the time each record carries as its Kafka `CreateTime`. The
expression's `DATETIME` counts nanoseconds and a Kafka record time counts whole milliseconds, so
Nervix writes the millisecond that contains the value: `2001-02-03T04:05:06.789999999Z` becomes
`2001-02-03T04:05:06.789Z`. A record keeps that time on every attempt, so a retry after a refused
or unanswered publish writes the time the first attempt wrote. librdkafka reads a zero timestamp as
the time it takes the record, and a Kafka record carries no time before the Unix epoch, so a record
whose millisecond is not after the epoch, any time before `1970-01-01T00:00:00.001Z`, is rejected
through `ON MESSAGE ERROR` before the producer takes it, as an `external` error of the `publish`
operation whose message names its timestamp, and the emitter's other records are still sent.
Without `TIMESTAMP`, librdkafka gives each record the time the producer takes it. A topic whose
`message.timestamp.type` is `LogAppendTime` replaces either time with the time the broker appends
the record. A broker whose `log.message.timestamp.before.max.ms` or
`log.message.timestamp.after.max.ms` refuses a record's time answers `INVALID_TIMESTAMP`; under
`ACK` that answer rejects the record through `ON MESSAGE ERROR`. Under `NO ACK` the record counts
as delivered once the producer takes it, so it is lost, like any other record its broker refuses
after that.

`ACK` waits for every record's delivery report through the shared confirmation window. If a publish
attempt fails, the window first settles the answers already received and returns only unresolved
members for retry; confirmed records are not sent again by that attempt's retry. An unanswered
report is still ambiguous: the broker may have accepted the record, so a retry can duplicate it.

Kafka's `acks`, idempotence, batching, linger, and compression remain pass-through client
configuration. `ACK TIMEOUT` bounds the report wait and is also the producer's own delivery bound:
the emitter sets the producer's `delivery.timeout.ms` to its
`ACK TIMEOUT`, rounded up to the millisecond, replacing the client's own `delivery.timeout.ms`, the
one name a Kafka client accepts for that librdkafka property. The producer therefore gives up a
record within about a second of
the emitter doing so, the interval at which librdkafka checks message timeouts, instead of
delivering it later beside the copy the retry sends. An `ACK TIMEOUT` longer than the 2147483647
milliseconds librdkafka holds refuses the emitter when it starts. `NO ACK` is fire-and-forget after
local admission: Nervix acknowledges the emitter's `ATTACHED` ACK share when librdkafka accepts the
record into its producer queue and does not wait for a delivery report. A later broker error,
producer timeout, crash, or process exit can therefore lose an accepted record. In `ACK`, a full
local queue applies backpressure: Nervix waits for the oldest outstanding delivery report, settles
it, and tries to enqueue the same refused record again. The effective confirmation window is
limited by both `MAX` and the producer queue's record and byte capacities. If no outstanding report
remains, Nervix waits for the native delivery callback to release its queue place, bounded by
`ACK TIMEOUT`. An unanswered or failed delivery report still ends the attempt after settling every
answer already given, and only unresolved records are retried. In `NO ACK`, a temporarily full
queue is an infrastructure condition paced by the declared retry policy. In either mode, a record
whose payload exceeds the entire `queue.buffering.max.kbytes` capacity is rejected through
`ON MESSAGE ERROR`, with its payload size and the capacity in bytes. This queue capacity counts
the payload alone; `message.max.bytes` separately bounds the encoded Kafka record.

The producer keeps librdkafka's topic auto-creation default. A broker that permits it can therefore
create a missing topic on the first publish, even though Nervix's consumers disable auto-creation
and `CREATE KAFKA CLIENT` refuses `allow.auto.create.topics`. Provision output topics explicitly;
the consumer's no-creation guarantee does not extend to the producer.

When the emitter stops, it flushes what the producer still queues, in both modes and
regardless of `linger.ms`: a `STOP`, an alteration or ownership move that replaces the emitter, and
the node's graceful shutdown each give that flush their deadline. Records still awaiting their
delivery report when the deadline passes are lost; `DESCRIBE EMITTER` reports how many as its
transient error, counting records alone and never the errors or statistics waiting in librdkafka's
queue beside them, and a replacement that cannot flush fails and keeps the running emitter. Because
a record counts until its delivery report arrives, the producer always reports every record.
`delivery.report.only.error` is reserved for Nervix and refused in the client's `CONFIG` at CREATE.
A definitive record-specific
rejection, such as an oversized message, follows `ON MESSAGE ERROR`.
Once the emitter and its flush are done with the producer, the node destroys it on its connector
control workers, never on the task that served the emitter. The destruction purges what the producer
still holds and waits up to half a second for librdkafka to report it, so a record the flush could
not send by its deadline is lost then. A node that cannot reserve the job that will destroy the
producer when the emitter starts reports the emitter's transient error and starts the emitter again
after its retry delay; see
[Kafka client retirement](./connector-contract.md#kafka-client-retirement).

### Pulsar

<!-- connector-matrix:emit-pulsar-topic:start -->
```plexus,ignore
CREATE PULSAR TOPIC EMITTER <name>
  FROM <relay>
  TO <client> TOPIC 'orders'
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | PULSAR client |
| Locator and validation rule | TOPIC 'orders' (pulsar_topic) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | `write_header`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | receipts held under NO ACK: at most 100 receipts; when full, holds back what fills it<br>messages the driver queues for its connection: driver default, 100 messages; when full, fails the publish for the host to retry<br>records awaiting confirmation under ACK: set by ACK PARALLEL MAX; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection: driver default, 10 s per address<br>topic lookup, producer creation and receipts: driver default, the operation timeout: ACK TIMEOUT under ACK, 30 s under NO ACK |
| Driver limits | Messages above the broker's announced maximum are rejected. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [PULSAR TOPIC capability and mirror reference](connector-capabilities.md#pulsar-topic-emit).
<!-- connector-matrix:emit-pulsar-topic:end -->

`ACK` waits for each broker receipt, for up to `ACK TIMEOUT` after that record's hand-over. The
driver uses the same timeout, including values longer than thirty seconds. If a later enqueue
fails, receipts already given are settled before retry, so confirmed records are not sent again.
`NO ACK` acknowledges producer acceptance and does not expose later broker errors as per-record
outcomes; its throughput advantage may be smaller than Kafka's because Pulsar already pipelines
producer work. On stop, Nervix waits for the accepted messages' outstanding receipts until the
stop's deadline. A driver failure or receipts still unanswered then fail the finish and are
reported by the host. This final wait does not change the earlier `NO ACK` completion point.
Receipt bookkeeping is bounded to 100 accepted messages, matching the driver's outbound channel
capacity. At that bound, another hand-over waits for a receipt to free a place; the accepted
records keep their earlier completion point.

A record whose message is larger than the `maxMessageSize` the broker announced for the producer's
connection, counting its metadata and properties, follows `ON MESSAGE ERROR` in either mode: the
producer refuses it before writing it, because the broker would close the connection and fail
every message in flight on it. With `ACK`, a message the broker receives and refuses with
`NotAllowedError`, such as one above the topic's own `maxMessageSize` policy, follows
`ON MESSAGE ERROR` too, while any other broker error is retried.

Pulsar emitters use the same client config surface as Pulsar ingestors:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: the tenant and namespace, written `'tenant/namespace'`, that a short
  topic name is placed in, so `TOPIC 'orders'` writes `persistent://<namespace>/orders`; defaults
  to `public/default`. Fully qualified topic names are sent as written.
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

As the [ingestor manual](ingestors.md#pulsar) states, `CREATE PULSAR CLIENT` refuses the three TLS
keys with a plaintext `pulsar://...` address and a `namespace` that is not exactly a tenant and a
namespace.

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

### RabbitMQ

<!-- connector-matrix:emit-rabbitmq-queue:start -->
```plexus,ignore
CREATE RABBITMQ QUEUE EMITTER <name>
  FROM <relay>
  TO <client> QUEUE 'orders'
    MODE <mode>
    ENCODE USING <codec>
    [MANDATORY]
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | RABBITMQ client |
| Locator and validation rule | QUEUE 'orders' (rabbitmq_queue) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | `write_header`; `sorted_by_name`, exact names: one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | MANDATORY |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | publishes awaiting confirmation: set by ACK PARALLEL MAX, 1 under ACK SEQUENTIAL; when full, holds back what fills it<br>messages the broker returns unrouted under MANDATORY, until the write that published them reads them: set by the messages of one write; when full, holds back what fills it |
| Network waits | connection: name resolution, address attempts, TLS, the AMQP handshake, channel open, confirm select and the queue check: `connect_timeout_ms`; 30s when omitted<br>a publish: unbounded<br>a confirmation under ACK: `ACK TIMEOUT`<br>the round trip that ends a publish under NO ACK: `timeout_ms`; unbounded when omitted |
| Driver limits | A frame above the negotiated frame_max ends the connection. |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |

See the [RABBITMQ QUEUE capability and mirror reference](connector-capabilities.md#rabbitmq-queue-emit).
<!-- connector-matrix:emit-rabbitmq-queue:end -->

<!-- connector-matrix:emit-rabbitmq-exchange:start -->
```plexus,ignore
CREATE RABBITMQ EXCHANGE EMITTER <name>
  FROM <relay>
  TO <client> EXCHANGE 'events'
  (ROUTING KEY input.category)
    MODE <mode>
    ENCODE USING <codec>
    [MANDATORY]
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | RABBITMQ client |
| Locator and validation rule | EXCHANGE 'events' (rabbitmq_exchange) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ROUTING KEY input.category` (expression); required |
| Headers | `write_header`; `sorted_by_name`, exact names: one value per name, the last one written for it; names in ascending byte order; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | MANDATORY |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | publishes awaiting confirmation: set by ACK PARALLEL MAX, 1 under ACK SEQUENTIAL; when full, holds back what fills it<br>messages the broker returns unrouted under MANDATORY, until the write that published them reads them: set by the messages of one write; when full, holds back what fills it |
| Network waits | connection: name resolution, address attempts, TLS, the AMQP handshake, channel open, confirm select and the exchange check: `connect_timeout_ms`; 30s when omitted<br>a publish: unbounded<br>a confirmation under ACK: `ACK TIMEOUT`<br>the round trip that ends a publish under NO ACK: `timeout_ms`; unbounded when omitted |
| Driver limits | A frame above the negotiated frame_max ends the connection.<br>A routing key longer than the 255 bytes an AMQP short string carries is not sent; its record is rejected. |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |

See the [RABBITMQ EXCHANGE capability and mirror reference](connector-capabilities.md#rabbitmq-exchange-emit).
<!-- connector-matrix:emit-rabbitmq-exchange:end -->

`RABBITMQ QUEUE` is the default-exchange shorthand: `QUEUE` names an existing queue, and the
emitter publishes each message to the broker's default exchange with the queue's name as its
routing key, which routes it to that queue. The same case-preserving queue-name type is used for
ingestion and emission: 1 to 255 UTF-8 bytes, including every byte of a multibyte character.
`SHOW CREATE EMITTER` retains the external name and the complete publishing and optional batching
policy.

`RABBITMQ EXCHANGE` publishes to an exchange the operator declared. `EXCHANGE` names it, 1 to 255
UTF-8 bytes kept as written, and `ROUTING KEY` computes the key the exchange routes each record by.
The expression reads what an [envelope expression](#envelope-clauses) reads and must compute an
exact, non-null, non-sensitive `STRING`, as `CREATE` and `ALTER` check with the message
`ROUTING KEY requires an exact, non-null, non-sensitive STRING value`; `leak_sensitive` admits a
sensitive key. Nervix evaluates it once for every batch it admits, together with the envelope,
after the route kept and finalized its rows. A record whose routing key fails or produces no value
is not sent: it follows `ON MESSAGE ERROR` as an `evaluation` error of the `publish` operation whose
message begins `ROUTING KEY`, such as `ROUTING KEY expression produced NULL`. A key longer than the
255 bytes an AMQP short string carries is not sent either: its record is rejected as an `external`
error of the `publish` operation that names the key's length. The exchange's type decides where the
key leads: a direct exchange routes to the queues bound under exactly that key, a topic exchange to
the queues whose binding pattern matches it, and a fanout exchange to every bound queue, whatever
the key. Records whose keys differ never share a [batch payload](#batch-payloads), so a batch
message carries the one key its members share. `SHOW CREATE EMITTER` retains the exchange's name
and the routing key's expression.

Nervix never declares an exchange or a binding. When an exchange emitter opens, it checks that its
exchange exists with a passive declaration; while it is missing, the emitter stays unavailable with
`RabbitMQ exchange '<name>' does not exist` as its transient error, confirms nothing, so its input
stays unacknowledged, and opens on its `RETRY POLICY` backoff once the exchange is declared.

A message no binding routes, such as one whose key no binding of a direct exchange names or one for
a queue deleted under a `RABBITMQ QUEUE` emitter, is the broker's to drop unless the emitter writes
`MANDATORY`. Without it the broker drops the message and answers it as taken, so the emitter reports
its record delivered and the record is lost. With `MANDATORY`, on either entity, the broker returns
the message, and the emitter rejects its record through `ON MESSAGE ERROR` as an `external` error of
the `publish` operation naming the broker's reply, such as `rabbitmq returned the message unrouted
with reply code 312: NO_ROUTE`, in every mode, and keeps publishing the write's other records.
Under `ACK` the broker returns the message ahead of its confirmation. Under `NO ACK` it returns the
message ahead of its answer to the round trip that ends the write, so the emitter reads every return
of the write once that answer arrives, and holds none of them beyond the write that published it.
A return names the message's exchange, routing key, headers and body but not which record it was,
so the emitter attributes it to the first message of the write, in written order, that carries all
four; of messages identical in all four, the earlier one is rejected.

Both entities publish the same way. `ACK` enables publisher confirms and waits for the confirm of
each message. A broker nack is an infrastructure failure and is retried with backpressure. In `NO ACK` the broker confirms nothing,
so once a write's messages are on the channel, the emitter asks the channel for one round trip,
which the broker answers only after it has taken every message written before it, and that answer
acknowledges them. A `NO ACK` write therefore waits for one round trip to the broker however many
messages it carries, and a channel or connection lost before the answer leaves the write's messages
to the retry. When the client sets `timeout_ms`, a round trip the broker does not answer within it
fails the write, reported naming the key and its bound, and leaves the write's messages to the
retry, which reopens the emitter's connection first. Without `timeout_ms` the round trip waits as
long as the connection lives.

The broker's `max_message_size`, 16 MiB by default in RabbitMQ 4.x, bounds each message body;
headers do not count. The broker closes the channel a larger body arrives on, and the emitter
rejects that message through `ON MESSAGE ERROR` in either mode, instead of retrying it or
acknowledging it, then keeps publishing on a new channel of the same connection; see
[Broker and message emitters](#broker-and-message-emitters).

The emitter resolves the host of the client's `addr` through the node's configured DNS resolver
when it opens and whenever it reopens after a failed publish, so a changed DNS answer takes effect
on the next connection, and tries the addresses it receives in order. A literal IPv4 address, or
an IPv6 address in brackets, is connected to as written. Resolution, the TCP connection, for
`amqps` the TLS handshake, the AMQP handshake, opening the channel, selecting publisher confirms
under `ACK` and checking the queue or the exchange share the client's `connect_timeout_ms`, 30 seconds when it is
left out; the broker certificate must name the host `addr` names. A connection that fails or runs
out of that deadline, including a name that does not resolve, leaves the emitter unavailable with
the failure as its transient error; it confirms nothing, so its input stays unacknowledged, and it
reopens on its `RETRY POLICY` backoff. The emitter reads the client's `addr`, trusts its
`tls_ca_file` and presents its `tls_cert_file` and `tls_key_file` as a RabbitMQ source does; see
[RabbitMQ](./ingestors.md#rabbitmq). A broker that refuses the TLS session leaves the emitter
unavailable with that refusal as its transient error. The client's `tls_ca_file`, `tls_cert_file`
and `tls_key_file` require an `amqps` address; combining them with `amqp` is refused before commit.

### Redis Pub/Sub

<!-- connector-matrix:emit-redis-pubsub:start -->
```plexus,ignore
CREATE REDIS PUBSUB EMITTER <name>
  FROM <relay>
  TO <client> CHANNEL 'orders'
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | REDIS client |
| Locator and validation rule | CHANNEL 'orders' (redis_channel) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Redis Pub/Sub carries only a channel and payload. A subscription has no acknowledgement; PUBLISH answers with a subscriber count rather than a delivery receipt. Both directions use quoted, case-preserving Redis channel names. |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | connections the command pool holds: set by POOL SIZE MAX; when full, holds back what fills it |
| Network waits | checking out a connection, a new one's name resolution, connection, TLS and Redis setup included: `connect_timeout_ms`; 30s when omitted<br>the answer to a PUBLISH: driver default, 500 ms |
| Driver limits | None declared. |
| Stalled-peer checks | `STALLED PEER connect_timeout_ms` |

See the [REDIS PUBSUB capability and mirror reference](connector-capabilities.md#redis-pubsub-emit).
<!-- connector-matrix:emit-redis-pubsub:end -->

`CHANNEL` uses the same rule as [Redis ingestion](ingestors.md#redis-pubsub): a nonempty literal
of at most 1,024 UTF-8 bytes, preserved exactly. A star in `CHANNEL 'Orders:EU/*'` is part of the
channel name. Pub/Sub has no transport headers or per-message envelope, so payload fields must
carry application attributes. `BATCH` is optional; the emitter learns no additional size limit
from Redis, whose `proto-max-bulk-len` refusal follows the ordinary record-error policy.

Redis Pub/Sub has no subscriber delivery acknowledgment. The awaited `PUBLISH` response confirms
server acceptance only. A record-specific server rejection follows `ON MESSAGE ERROR`; connection
failures retry the undelivered work. A `REDIS CLIENT` declares its connection-pool bounds; see
[Database Client Connection Pools](database-client-pools.md).
A publish checks a command connection out within the client's `connect_timeout_ms`. The client's
`pool_idle_timeout_ms` and `pool_max_lifetime_ms`, 10 and 30 minutes by default, retire a command
connection that sat idle or grew old; see
[Connection lifetimes](database-client-pools.md#connection-lifetimes).
Each physical pooled command connection resolves the `addr` hostname through the node's
asynchronous DNS resolver when it opens. A replacement connection can use a changed DNS answer;
an established connection stays open until Redis or the network closes it or the pool retires it.
For `rediss://`, TLS still verifies the configured hostname and uses its configured CA and optional
client identity.

### MQTT

The default client identity is `<domain>~<emitter>`. A `client_id` in the client configuration
replaces the whole identity, including its domain prefix. Choose an explicit identity unique to
this connection on the broker, including across domains and ingestors.

Connections use the driver's 60-second keep-alive unless the broker supplies a Server Keep Alive
in `CONNACK`; a broker-supplied zero disables it. Plexus exposes no client setting for this
interval. QoS 0 uses a clean session. QoS 1 and 2 request a persistent session with the maximum
Session Expiry Interval (`0xFFFFFFFF`); the broker's accepted expiry and retention limits
still apply. Plexus exposes no session-expiry setting.

<!-- connector-matrix:emit-mqtt-topic:start -->
```plexus,ignore
CREATE MQTT TOPIC EMITTER <name>
  FROM <relay>
  TO <client> TOPIC 'devices/Orders'
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | MQTT client |
| Locator and validation rule | TOPIC 'devices/Orders' (mqtt_topic) |
| Modes (complete bodies after MODE) | QOS 0 RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 1 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s<br>QOS 2 ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: Subscription options select the session and inbound QoS; emitter QoS is part of its publishing mode. MQTT packets have no generic transport header map, and publish properties are not declared by this implementation. |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | publishes queued for the driver: set by 1 under QOS 0, ACK PARALLEL MAX under QOS 1 and 2; when full, fails the publish for the host to retry |
| Network waits | connection: name resolution, TCP, TLS and CONNECT: driver default, 5 s<br>answers to QOS 1 and 2 publishes: `ACK TIMEOUT`<br>writes to the connection: unbounded |
| Driver limits | Packets above the broker's maximum packet size are rejected. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [MQTT TOPIC capability and mirror reference](connector-capabilities.md#mqtt-topic-emit).
<!-- connector-matrix:emit-mqtt-topic:end -->

`TOPIC` names one literal topic, published to exactly as written, letter case included. It follows
the [ingestion topic filter rule](ingestors.md#external-names) without the `+` and `#` wildcards,
so `TOPIC 'devices/+'` is refused when the emitter is created. The client's `addr` names the broker
by its scheme, host and port alone, as [MQTT ingestion](ingestors.md#mqtt) describes.

CREATE refuses an unsupported QoS number with its value and accepted levels, such as
`MQTT emitter QOS must be 0, 1, or 2, found 3`, underlining the supplied number.

QoS 0 acknowledges client acceptance. When the emitter stops, it waits within the stop or shutdown
deadline for its client to send the publishes it still holds, which a client cut off from its
broker holds until it connects again; publishes still unsent at that deadline fail the stop and are
counted in the emitter's transient error. Sending a QoS 0 publish to the connection does not
confirm that the broker received it.

QoS 1 waits for `PUBACK`; QoS 2 waits for the complete exactly-once handshake. Their persistent
session and stable client identity let the same client resume in-flight publishes across network
reconnects while the broker retains the session. Nervix keeps local delivery attempts and
acknowledgements in memory, so this is not a durable checkpoint of emitter state.
Reconnect pacing follows the declared retry policy. When an answer misses its `ACK TIMEOUT`, or
the broker or client fails it for a reason that
is not the record's own, the emitter keeps every answer the broker already gave and retries only the
records without one; `DESCRIBE EMITTER` shows why, such as
`the broker did not answer within ACK TIMEOUT 10s`. A record the emitter stopped waiting for stays
with the client, which sends it again after a reconnect that resumes the session, beside the
emitter's own retry, so the broker can receive it twice. A definitive record rejection, such as an
invalid topic or payload-format rejection, follows `ON MESSAGE ERROR`. So does a record whose
`PUBLISH` packet would exceed the Maximum Packet Size the broker declared when the client connected,
or the largest packet MQTT can express: the emitter rejects it before handing it to the client,
which would otherwise lose its connection on a packet the broker refuses to receive. The client
resolves the host of its `addr` through the node's asynchronous DNS resolver every time it connects,
first and after every lost connection, and dials the answers in order within its five-second connect
timeout. For `mqtts`, TLS verifies the configured host name. While the name does not resolve, the
client reconnects on the declared retry policy, `DESCRIBE EMITTER` shows the lookup failure, and a
QoS 1 or 2 record stays unacknowledged until the broker acknowledges it.

### NATS

<!-- connector-matrix:emit-nats-subject:start -->
```plexus,ignore
CREATE NATS SUBJECT EMITTER <name>
  FROM <relay>
  TO <client> SUBJECT 'orders.Created'
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | NATS client |
| Locator and validation rule | SUBJECT 'orders.Created' (nats_publish_subject) |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s<br>JETSTREAM ACK SEQUENTIAL ACK TIMEOUT 5s RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | `write_header`; `repeats_by_name`, exact names: every value of a name, together and in the order written; the order of different names is not declared; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | commands queued for the connection: driver default, 2048 commands; when full, holds back what fills it |
| Network waits | name resolution: unbounded<br>connection: driver default, 5 s per address<br>a core publish and its flush to the client's socket, not a server round trip: `timeout_ms`; unbounded when omitted<br>a JetStream acknowledgement: `ACK TIMEOUT` |
| Driver limits | Messages above the server's max_payload are rejected. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [NATS SUBJECT capability and mirror reference](connector-capabilities.md#nats-subject-emit).
<!-- connector-matrix:emit-nats-subject:end -->

`SUBJECT` names one literal subject, published to exactly as written, letter case included. It
follows the [ingestion subject rule](ingestors.md#external-names) without the `*` and `>`
wildcards, so `SUBJECT 'orders.*'` is refused when the emitter is created. A message carries its
subject, the written headers and the payload; the emitter sets no reply subject or JetStream
message id of its own. `BATCH` is optional. The NATS protocol sets no message size of its own:
each server announces its `max_payload`, and the client refuses a batch message above it before
writing it, as [Broker and message emitters](#broker-and-message-emitters) describes.

`NO ACK` publishes through Core NATS. A write delivers its records only after the client's queued
bytes have drained and their connection has flushed. The flush is local: the server answers
nothing, so a delivered record can still be lost if its connection fails before the server reads
it. If the connection is replaced before the write completes, its records stay unresolved and
Nervix retries them on the emitter's policy. A replacement connection's flush cannot certify bytes
discarded with the failed connection. Records queued while the client is already disconnected
can complete on its next connection; another replacement makes that write uncertain too.

The client's optional `timeout_ms` bounds each write, the hand-over and the flush included. A write
that runs out of it, because the client cannot write to a server that went down or stopped reading,
fails as the emitter's transient status, which names `timeout_ms` and its bound, and the emitter
retries it on its policy. The client may still send records from that attempt, while a failed
connection may have discarded others, so a retry can duplicate records the server received. A
write first drains what an earlier attempt left with the client, within its own `timeout_ms`,
before handing over more records. If that pending flush used a replacement connection, the write
reports the uncertainty before the next retry resends the unresolved records. Without
`timeout_ms` a write waits for its flush however long that takes.

The client connects again on its own after it loses its server, on the emitter's `RETRY POLICY`: it
waits `BACKOFF` before its first attempt and doubles the wait for each further attempt up to `MAX`.
When the emitter stops, its finish flushes what the client still holds by the finish deadline; see
[Finishing an emitter](./connector-contract.md#finishing-an-emitter).

`JETSTREAM` waits for one `PubAck` per record. The stream must already exist and capture the
subject; a missing stream is an infrastructure error that remains under backpressure until an
operator provisions it.

`JETSTREAM` hands records to the client one after another, with at most `MAX <n>` of them awaiting
their `PubAck` at once, or one under `ACK SEQUENTIAL`, each until `ACK TIMEOUT` after its
hand-over. A publish that fails, because a `PubAck` failed or did not arrive in time, or because the
client did not take a record within `ACK TIMEOUT` while its connection stopped draining, first
settles every `PubAck` JetStream already returned: a confirmed record is delivered and never sent
again. The retry sends the records whose `PubAck` never arrived, which JetStream may already hold,
so delivery is at least once.

### ZeroMQ

<!-- connector-matrix:emit-zeromq-push:start -->
```plexus,ignore
CREATE ZEROMQ PUSH EMITTER <name>
  FROM <relay>
  TO <client>
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | ZEROMQ client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: The PULL listener receives payload frames and the PUSH socket sends them; neither has a broker acknowledgement or header map. |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: The PUSH socket sends one payload frame. Multipart envelopes and additional per-record envelope clauses are not offered. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | bytes queued for a peer: unbounded |
| Network waits | connecting and the ZMTP handshake: `connect_timeout_ms`; 30s when omitted<br>a send: `timeout_ms`; unbounded when omitted |
| Driver limits | The driver has no high-water mark, maximum message size or heartbeat. |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |

See the [ZEROMQ PUSH capability and mirror reference](connector-capabilities.md#zeromq-push-emit).
<!-- connector-matrix:emit-zeromq-push:end -->

ZeroMQ has no delivery acknowledgment; success is socket acceptance. Transient socket failures are
paced by the declared retry policy.

The emitter is a PUSH socket on the client's `addr`, a `tcp://<host>:<port>` or `ipc://<path>`
endpoint. With `'bind' = 'true'` the socket binds that address and peers connect to it; otherwise
it connects to a peer bound there (`'bind' = 'false'` is also the default when omitted).
Only `true` and `false`, ignoring letter case, are accepted for `bind`; `CREATE CLIENT` refuses
other values. Every value of the client's `CONFIG` is rendered the way an
ingestor renders it, so it can name the client's `MOUNT`; see
[Client Config Mounts](resources.md#client-config-mounts).

`CREATE CLIENT` checks `addr` with the socket driver's TCP/IPC endpoint parser, including after
rendering an address template. This check opens no socket and performs no name lookup. A TCP
hostname on a connecting PUSH socket uses the ZeroMQ driver's system resolver, not the node's
configured resolver; see [Name Resolution](name-resolution.md#residual-resolution).

Each publish sends one payload frame, including when `BATCH` packs several records into one
encoded message. The PUSH socket offers no transport headers, multipart envelope or delivery
receipt from a PULL peer.

A client that connects rather than binds connects when the emitter starts. The driver retries a
refused connection on its own and waits for the peer's ZMTP handshake, so the client's
`connect_timeout_ms`, 30 seconds when it is left out, is what ends the attempt of a peer that never
accepts: the start fails naming the key and its bound, and the emitter opens the socket again on its
retry policy. The driver has no high-water mark, so a send to a peer that stops reading waits until
the connection's buffers drain. The client's `timeout_ms` bounds that wait for each send; a send that
runs out of it fails the publish, and the emitter opens a new socket and sends the message again,
since the one it left partly written on the old connection is never completed. Without `timeout_ms`
a send waits for as long as the peer reads nothing.

The driver has no heartbeat and never reconnects on its own; the emitter's retry is what replaces a
socket. A send that finds its connection broken fails the publish, and the emitter opens a new
socket on its retry policy, which connects again in connect mode. A record whose send completed is
not sent again, even when its connection broke before the peer read it. A bound socket sends only
to peers that have connected to it and finished their handshake, and with none a send fails at
once. The emitter then retries through a new socket, which must bind the address again. That bind
fails while the failed socket still holds the address; once a bind succeeds, the retried publish
goes out at once, usually before any peer has reconnected, and fails the same way. A bound emitter
can therefore keep failing after a single failed publish, such as one for a record that arrived
before its first peer connected. Where you can, let the emitter connect to a bound peer instead.

### Syslog

<!-- connector-matrix:emit-syslog-collector:start -->
```plexus,ignore
CREATE SYSLOG COLLECTOR EMITTER <name>
  FROM <relay>
  TO <client>
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | SYSLOG client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | NO ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: A listener admits UDP datagrams or framed TCP/TLS input and exposes its peer address; a collector writes encoded syslog records. Syslog fields belong to the wire schema rather than a transport header map. |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | A batch is written as one frame within the frame limits below; general BATCH limits still apply. |
| Frame limits | udp: one datagram carries at most 65507 payload bytes<br>tcp and tls with octet-counting framing: an octet count has at most 10 digits, so a frame carries at most 9999999999 payload bytes<br>tcp with non-transparent framing: a payload cannot contain `\n`, which ends each frame |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | connection: name resolution, address attempts and TLS: `connect_timeout_ms`; 30s when omitted<br>writing and flushing a frame: `timeout_ms`; unbounded when omitted |
| Driver limits | None declared. |
| Stalled-peer checks | `STALLED PEER timeout_ms`<br>`STALLED PEER connect_timeout_ms` |

See the [SYSLOG COLLECTOR capability and mirror reference](connector-capabilities.md#syslog-collector-emit).
<!-- connector-matrix:emit-syslog-collector:end -->

The client sends UDP datagrams, persistent RFC 6587 TCP frames, or RFC 5425 TLS frames. TCP uses
octet-counting by default and may select non-transparent framing; TLS always uses octet counting.
Success is local socket acceptance and flush, not remote delivery confirmation. The emitter requires
a codec and rejects header writes. See [Syslog](syslog.md) for client configuration, framing,
message errors, retry behavior, and limits.

With `BATCH`, a `SYSLOG` codec encodes one RFC 5424 frame whose `MSG` is a JSON array of the
members' complete RFC 5424 messages. Members must share every header field except timestamp; the
outer frame uses the first member's timestamp. The same frame travels as one UDP datagram, one
octet-counted TCP/TLS frame, or one non-transparent TCP frame when it contains no LF.

### SQS

<!-- connector-matrix:emit-sqs-queue:start -->
```plexus,ignore
CREATE SQS QUEUE EMITTER <name>
  FROM <relay>
  TO <client> QUEUE 'orders'
  [FIFO GROUP tenant | FIFO GROUP FROM BRANCH]
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | SQS client |
| Locator and validation rule | QUEUE 'orders' (sqs_queue) |
| Modes (complete bodies after MODE) | SINGLE RETRY POLICY BACKOFF 1s MAX 30s<br>BATCH RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `FIFO GROUP tenant` (expression); `FIFO GROUP FROM BRANCH` (keyword choice); optional; omitted when unset |
| Headers | `write_header`; `last_wins`, exact names: one value per name, the last one written for it; the order of names is not declared; a name matches only a name spelled the same, letter case included |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | 256KiB |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads and declared headers; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | connection: name resolution, TCP and TLS: driver default, 3.1 s<br>the queue URL lookup at start and a send: `timeout_ms`; unbounded when omitted |
| Driver limits | A batch request holds at most 10 entries and 256 KiB; a record above 256 KiB is rejected. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [SQS QUEUE capability and mirror reference](connector-capabilities.md#sqs-queue-emit).
<!-- connector-matrix:emit-sqs-queue:end -->

`QUEUE` uses the same rule as [SQS ingestion](ingestors.md#sqs): 1 to 80 ASCII letters, digits,
`-` and `_`, a FIFO queue's `.fifo` suffix included, preserved exactly as written. `write_header`
writes message attributes, and the emitter declares no envelope clause or `PROPERTIES` block. The
`SQS CLIENT` it names declares the keys the [SQS ingestion section](ingestors.md#sqs) lists.

`SINGLE` issues one request per message. `BATCH` groups messages within SQS's fixed limit of ten
entries and 256 KiB per request; both modes issue requests sequentially and acknowledge service
responses. Per-entry transient failures retry only those entries, while invalid entries and a
message larger than 256 KiB, counting its attributes and FIFO message group, follow
`ON MESSAGE ERROR` individually. Nervix checks what SQS refuses before sending: a body that is not
UTF-8, a body or attribute value holding a character SQS forbids, more than ten attributes, an
attribute name SQS does not allow, an empty attribute value, and an invalid FIFO message group. A
later `write_header` of a name replaces the earlier value, so the ten are counted after repeated
names keep their last value: eleven writes of ten names make a message SQS accepts. SQS
accepts the characters of the XML 1.0 `Char` production: tab, line feed, carriage return, U+0020
to U+D7FF, U+E000 to U+FFFD and U+10000 to U+10FFFF. Every other C0 control and the noncharacters
U+FFFE and U+FFFF are forbidden, while the other noncharacters fall inside those ranges and are
sent.

Set the SQS client's optional `timeout_ms` CONFIG key to bound each request, the queue lookup at
start and every send, as both the complete service operation and its single SDK attempt. Nervix
disables the AWS SDK's internal retries in both directions, so a request that runs out of
`timeout_ms` returns to the emitter, which `DESCRIBE EMITTER` shows as
`an SQS send did not finish within timeout_ms 5s`, and the mode's declared `RETRY POLICY` owns all
retry pacing.

The client resolves the host of its `endpoint` through the node's asynchronous resolver each time it
opens a connection, and tries the answers in order; a literal IPv4 or IPv6 address is dialled as
written. Every request is still signed for the configured host, and over HTTPS the service
certificate must name that host, whichever address accepted the connection. The lookup counts
against the SDK's 3.1-second connect timeout and against `timeout_ms`. Without `tls_ca_file` the
client trusts the platform's native roots; with it, the client trusts that CA alone. Either way it
connects directly and ignores the `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`
environment variables of its node. A missing name, a silent name server or an unreachable answer fails the queue lookup or
the send, which the emitter retries on its `RETRY POLICY`; nothing is acknowledged until SQS
answers.

For FIFO queues, one batch request contains at most one record from each message group. A partial
batch failure therefore cannot deliver a later record from a group ahead of the failed record;
other groups may still make progress independently.

A queue name ending in `.fifo` requires `FIFO GROUP`, and `FIFO GROUP` is rejected for a queue
without that suffix. `FROM BRANCH` requires branched input and uses the record's branch key as its
message group. Otherwise the expression must have exact `STRING` type for every record and obey
normal external-sensitivity leakage rules. Nervix relies on content-based deduplication, which the
operator must enable while provisioning the FIFO queue. Sends to a FIFO queue without it fail as
publish errors; Nervix never creates or reconfigures the queue.

`FIFO GROUP` is the emitter's ordering group. Nervix evaluates it against each record's input
row after any `FROM ... WHERE` filter, and every record the emitter sends carries the group its own
input row produced, whatever the emitter's `WHERE` and construction keep or build. A record whose
group expression fails for its row, or that reaches `FROM BRANCH` without a branch key, is not sent.
It follows `ON MESSAGE ERROR` as an `external` error of the `publish` operation whose message begins
`ordering group`, and the emitter's other records are still sent under their own groups. A record
the emitter's `WHERE` drops is never sent, so a failure of its group is never reported.

### Sentry

<!-- connector-matrix:emit-sentry-event:start -->
```plexus,ignore
CREATE SENTRY EVENT EMITTER <name>
  FROM <relay>
  TO <client>
  [PLATFORM 'python']
    MODE <mode>
    ENCODE USING <codec>
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | SENTRY client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `PLATFORM 'python'` (string literal); optional; default other |
| Headers | Not offered: An event is encoded inside its Sentry envelope; arbitrary per-event HTTP header writes are not supported. |
| Body | ENCODE USING &lt;codec&gt; |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | yes |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | a request: name resolution, connection, TLS and the response headers: `timeout_ms`; required |
| Driver limits | An event above 1,000,000 bytes is rejected. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [SENTRY EVENT capability and mirror reference](connector-capabilities.md#sentry-event-emit).
<!-- connector-matrix:emit-sentry-event:end -->

Sentry emission uses a `SENTRY CLIENT` whose required `dsn` names the project and whose required
`timeout_ms` bounds each envelope request:

```plexus
CREATE SENTRY CLIENT sentry_main
  CONFIG {
    'dsn' = 'https://<public-key>@sentry.example.com/<project-id>',
    'timeout_ms' = 5000
  };

CREATE SENTRY EVENT EMITTER sentry_errors
  FROM errors
  TO sentry_main
    PLATFORM 'python'
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING sentry_event_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

#### Sentry client

Nervix owns every key of a `SENTRY CLIENT` and refuses a key it does not declare:

| Key | Required | Meaning |
| --- | --- | --- |
| `dsn` | Yes | The project DSN, `<scheme>://<public-key>[:<secret-key>]@<host>[:<port>]/[<path>/]<project-id>`, with an `http` or `https` scheme. It is confidential, and client inspection redacts it. |
| `timeout_ms` | Yes | A positive whole number of milliseconds that bounds each envelope request, as [Client Deadlines](#client-deadlines) defines. |
| `tls_ca_file` | No | A PEM certificate authority that HTTPS trusts in addition to the platform's native roots. |
| `tls_cert_file`, `tls_key_file` | No | The PEM certificate and private key HTTPS presents as the client's identity, supplied together. |

`CREATE CLIENT` checks the complete DSN with Sentry's own parser, including rendered template
values, before it commits the client and without connecting to the service. A DSN without a
public key or a project, or with another scheme, is refused naming `dsn` without exposing its
value. From the DSN, Nervix derives the envelope endpoint,
`<scheme>://<host>[:<port>]/[<path>/]api/<project-id>/envelope/`, and the `X-Sentry-Auth` header,
which carries the DSN's keys and names the client `nervix/<version>`.

The emitter sends through the HTTP client Nervix shares with Prometheus and OTEL over HTTP, and
inherits its transport contract:

- The DSN's host is resolved through the node's DNS resolver, and `timeout_ms` covers that lookup,
  connection setup, TLS, sending the envelope and the response headers. The response body is
  never read.
- No proxy is used. A `SENTRY CLIENT` declares no `proxy`, and the proxy environment variables
  `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY` and their lowercase forms are never read,
  as [Connection Identity](name-resolution.md#connection-identity) describes.
- No redirect is followed, so the envelope and its `X-Sentry-Auth` header go only to the DSN's
  host. A `3xx` answer is the answer to its envelope and is retried, as the table below retries
  every status it does not list, so a `dsn` whose host redirects holds the emitter in retry until
  it names the host that accepts envelopes.

#### Sentry events

The codec produces one top-level JSON object per payload: per record, or per batch through
`ON EMITTING BATCH`. Its fields use the Sentry event protocol, such as `message`, `level`,
`environment`, `release`, `tags`, `extra`, `user` and `exception`. Nervix keeps every field the
codec wrote with its value, including fields the protocol does not define. A number is written
as the value it reads as: exactly within the 64-bit integer range, and otherwise as the nearest
64-bit float, so digits beyond a float's precision do not survive. Nervix adds only the fields
the codec left out:

| Field | Value when the codec leaves it out |
| --- | --- |
| `event_id` | A random UUID, generated once when the payload is prepared and written as 32 lowercase hexadecimal digits |
| `timestamp` | The batch's domain execution time, in RFC 3339 |
| `platform` | The `PLATFORM` clause's value, or `other` when the statement leaves the clause out |

A supplied `event_id` keeps its exact spelling, including hyphens and letter case. The optional
`PLATFORM '<value>'` clause is written before `MODE`. `DESCRIBE` shows the effective platform,
while `SHOW CREATE` retains whether the clause was written.

Each envelope carries exactly one `event` item: an envelope header with the event's `event_id`,
an item header with the event's byte length, and the event, each followed by a newline. Nervix
sends no other item; Sentry's transaction, attachment, session and check-in items are not
offered. The envelope is prepared once per payload, before the first publish attempt, and
retained: every retry sends the same bytes, the event id and the defaults included, even after
the host reopens the connector following a failed request.

An event is limited to 1,000,000 bytes, Sentry's decompressed event limit. The limit counts the
serialized event after the defaults are added; the envelope header, the item header and the
newline framing do not count.

With `BATCH`, the codec's `ON EMITTING BATCH` transformation builds the one event of a batch from
the candidate array, usually placing its members under `extra`; an emitter whose codec declares
no such transformation is refused, as [Batching](#batching) describes. The batch is then
delivered, rejected or retried as that one event. `MAX SIZE` measures the transformed event JSON,
before the defaults are added.

Use a JSON wire codec or a JAQ-native codec with JSON output; any other output fails preparation
as an invalid event. Sentry emitters require `ENCODE USING`, refuse `write_header`, and still
require explicit leakage for sensitive event fields.

#### Sentry outcomes and retries

Preparation refuses an event before anything is sent when the payload is not a JSON object, when
a field is one the Sentry event protocol cannot read, such as a numeric `level`, an `event_id`
that is not a UUID or a `timestamp` that is neither Unix seconds nor an RFC 3339 string, and when
the event exceeds the 1,000,000-byte limit. Every record the payload carries then follows
`ON MESSAGE ERROR` with one `external` message error of the `publish` operation, which names the
cause; the envelope is neither sent nor retained for a retry.

A write sends its envelopes one at a time, in order, and the answer to each decides its event:

| Answer | Outcome |
| --- | --- |
| A `2xx` status | The event is delivered and its records are acknowledged. |
| `400`, `413` or `422` | Sentry rejected the event. Its records follow `ON MESSAGE ERROR` with an `external` message error of the `publish` operation naming the status, and the write continues with the next envelope. |
| Any other status, such as `401`, `403`, `404`, `408`, `429` or a `5xx` | The write stops. This event and every later one stay retained and are sent again on the mode's `RETRY POLICY`, while the events delivered before it stay delivered. |
| No answer within `timeout_ms`, or a failed lookup, connection or TLS handshake | As for a retried status. |

Such an event is sent again until Sentry delivers or rejects it, so a revoked key or a deleted
project holds the emitter in retry until an operator repairs its client. On every retried status,
Nervix reads `Retry-After`, as a number of seconds or as an HTTP date taken against actual UTC,
and `X-Sentry-Rate-Limits`, taking the longest delay any of its quotas names, whatever their
categories. When the longer of the two exceeds the policy's next backoff delay, the next attempt
waits for it instead, even beyond `MAX`; a value Nervix cannot read, and a delay the node's
monotonic clock cannot schedule, extends nothing.

### OTEL

#### OTEL LOGS

<!-- connector-matrix:emit-otel-logs:start -->
```plexus,ignore
CREATE OTEL LOGS EMITTER <name>
  FROM <relay>
  TO <client>
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ATTRIBUTES {'tenant' = 'eu'}]
  [RESOURCE {'service.name' = 'nervix'}]
  [SCOPE 'nervix' VERSION '1.0']
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | OTEL client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | the export request of a publish without BATCH: unbounded |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |
| Driver limits | gRPC responses above 4 MiB are refused. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |
| VALUES fields, exact types and constraints | LOGS `time`: DATETIME; required; not before the Unix epoch<br>LOGS `severity_text`: STRING; optional; no additional value constraint<br>LOGS `severity_number`: I32; optional; 0..=24<br>LOGS `body`: STRING; required; no additional value constraint<br>LOGS `trace_id`: STRING; optional; 32 hexadecimal characters; not all zero<br>LOGS `span_id`: STRING; optional; 16 hexadecimal characters; not all zero |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |

See the [OTEL LOGS capability and mirror reference](connector-capabilities.md#otel-logs-emit).
<!-- connector-matrix:emit-otel-logs:end -->

#### OTEL TRACES

<!-- connector-matrix:emit-otel-traces:start -->
```plexus,ignore
CREATE OTEL TRACES EMITTER <name>
  FROM <relay>
  TO <client>
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ATTRIBUTES {'tenant' = 'eu'}]
  [RESOURCE {'service.name' = 'nervix'}]
  [SCOPE 'nervix' VERSION '1.0']
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | OTEL client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | the export request of a publish without BATCH: unbounded |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |
| Driver limits | gRPC responses above 4 MiB are refused. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |
| VALUES fields, exact types and constraints | TRACES `trace_id`: STRING; required; 32 hexadecimal characters; not all zero<br>TRACES `span_id`: STRING; required; 16 hexadecimal characters; not all zero<br>TRACES `parent_span_id`: STRING; optional; 16 hexadecimal characters; not all zero<br>TRACES `name`: STRING; required; no additional value constraint<br>TRACES `kind`: STRING; optional; SERVER, CLIENT, INTERNAL, PRODUCER or CONSUMER; case-sensitive<br>TRACES `start_time`: DATETIME; required; not before the Unix epoch<br>TRACES `end_time`: DATETIME; required; not before the Unix epoch<br>TRACES `status_code`: STRING; optional; OK, ERROR or UNSET; case-sensitive<br>TRACES `status_message`: STRING; optional; no additional value constraint |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |

See the [OTEL TRACES capability and mirror reference](connector-capabilities.md#otel-traces-emit).
<!-- connector-matrix:emit-otel-traces:end -->

#### OTEL METRICS

<!-- connector-matrix:emit-otel-metrics:start -->
```plexus,ignore
CREATE OTEL METRICS EMITTER <name>
  FROM <relay>
  TO <client>
  (METRIC 'temperature' UNIT 'Cel' GAUGE | METRIC 'requests' UNIT '1' SUM MONOTONIC DELTA | METRIC 'latency' UNIT 'ms' HISTOGRAM CUMULATIVE)
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ATTRIBUTES {'tenant' = 'eu'}]
  [RESOURCE {'service.name' = 'nervix'}]
  [SCOPE 'nervix' VERSION '1.0']
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | OTEL client |
| Locator and validation rule | Not offered: The configured client owns the transport address; no separate external entity name is written in this body. |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `METRIC 'temperature' UNIT 'Cel' GAUGE` (composed clause); `METRIC 'requests' UNIT '1' SUM MONOTONIC DELTA` (composed clause); `METRIC 'latency' UNIT 'ms' HISTOGRAM CUMULATIVE` (composed clause); required<br>`ATTRIBUTES {'tenant' = 'eu'}` (named expressions); optional; omitted when unset<br>`RESOURCE {'service.name' = 'nervix'}` (named expressions); optional; omitted when unset<br>`SCOPE 'nervix' VERSION '1.0'` (composed clause); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | the export request of a publish without BATCH: unbounded |
| Network waits | an export: name resolution, connection, TLS and the response: `timeout_ms`; required |
| Driver limits | gRPC responses above 4 MiB are refused. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |
| VALUES fields, exact types and constraints | GAUGE / SUM `time`: DATETIME; required; not before the Unix epoch<br>GAUGE / SUM `start_time`: DATETIME; required for DELTA; optional otherwise; not before the Unix epoch<br>GAUGE / SUM `value`: an integer-family, F32, or F64 value; required; integer values must fit I64; floating-point values use OTLP as_double<br>HISTOGRAM `time`: DATETIME; required; not before the Unix epoch<br>HISTOGRAM `start_time`: DATETIME; required for DELTA; optional otherwise; not before the Unix epoch<br>HISTOGRAM `count`: U64; required; the complete U64 range<br>HISTOGRAM `sum`: F32 or F64; optional; no additional value constraint<br>HISTOGRAM `bucket_counts`: a U64 ARRAY or VEC; required; no NULL elements; length equals explicit_bounds length plus one<br>HISTOGRAM `explicit_bounds`: an F32 or F64 ARRAY or VEC; required; no NULL elements<br>HISTOGRAM `min`: F32 or F64; optional; no additional value constraint<br>HISTOGRAM `max`: F32 or F64; optional; no additional value constraint |
| ATTRIBUTES values | a string, boolean, number, datetime or an ARRAY or VEC of those; DATETIME becomes RFC 3339 text; ARRAY and VEC may nest; NULL attributes are omitted; BYTES and maps are refused |
| RESOURCE values | a literal string, boolean, I64, F64, null or ARRAY; literal expressions only, including nested literal arrays; one resource for the emitter |
| Unsupported OTLP features and reasons | exponential histograms and summaries: METRIC declares only GAUGE, SUM and explicit HISTOGRAM<br>per-record resource and scope: RESOURCE is literal-only and SCOPE is fixed for the emitter<br>span events and links: the closed span VALUES table has no event or link mapping<br>trace_state: the closed span VALUES table does not map trace state<br>log event_name and flags: the closed log VALUES table maps neither field<br>exemplars: metric points expose values and attributes without exemplar mappings<br>non-string log bodies: the log body field requires STRING<br>bytes_value and map attributes: attributes admit scalars and nested ARRAY or VEC values; BYTES and maps are unavailable<br>scope attributes: SCOPE declares only a name and optional version<br>OTLP/JSON: the client declares only grpc and http/protobuf |

See the [OTEL METRICS capability and mirror reference](connector-capabilities.md#otel-metrics-emit).
<!-- connector-matrix:emit-otel-metrics:end -->

OTEL emission is a codec-free direct emitter for OTLP logs, traces and metric data points. Each
signal is its own entity, `OTEL LOGS`, `OTEL TRACES` or `OTEL METRICS`, and each one takes
`MODE ACK RETRY POLICY ...` as its only mode. The three generated tables above are the
declaration: a signal's closed `VALUES` keys with their exact types, requiredness and value
constraints, what `ATTRIBUTES` and `RESOURCE` accept, and the OTLP features Nervix does not
support, each with its reason. The [OTEL client](connector-capabilities.md#otel-client) reference
renders the client's `CONFIG` keys from the same declaration.

Every OTEL emitter exports through an `OTEL CLIENT`, which selects OTLP/gRPC or
OTLP/HTTP-protobuf explicitly; there is no default protocol:

```plexus
CREATE OTEL CLIENT otel_main
  CONFIG {
    'endpoint' = 'http://127.0.0.1:4317',
    'protocol' = 'grpc',
    'headers' = 'authorization=Bearer%20<token>',
    'compression' = 'gzip',
    'timeout_ms' = 5000
  };
```

`endpoint`, `protocol` and `timeout_ms` are required, and `protocol` is exactly `grpc` or
`http/protobuf`. `timeout_ms` bounds one export: name resolution, connection setup, TLS and the
answer. For `grpc` it is also the connection timeout. `compression` accepts only `gzip`; without
it, requests are sent uncompressed. For `http/protobuf`, Nervix posts each request to the
endpoint's path, without its trailing slash, followed by `/v1/logs`, `/v1/traces` or `/v1/metrics`.

`headers` holds comma-separated `name=value` pairs in the
[OTLP header format](https://opentelemetry.io/docs/specs/otel/protocol/exporter/#specifying-headers-via-environment-variables).
Nervix splits each pair at its first `=`, trims the whitespace around the name and the value,
lowercases the name and percent-decodes the value once. A literal `+` remains `+`; `%2B` also
decodes to `+`, and `%252C` decodes to the literal `%2C`. Encode a comma inside a value as `%2C`
and a space as `%20`; encoded leading and trailing spaces belong to the value. Empty input supplies
no headers, and a name given more than once sends only its last value. `CREATE CLIENT` and
`ALTER CLIENT` refuse a pair without `=` or with an empty name. When the emitter opens, it refuses
a name that is not a valid HTTP header name; for `grpc`, a name ending in `-bin`, which gRPC
reserves for binary metadata; and a decoded value that holds a control character other than tab.
Client inspection shows `headers` as `********`.

The TLS file keys need an `https` endpoint on either protocol, and `tls_cert_file` and
`tls_key_file` go together; `CREATE CLIENT` and `ALTER CLIENT` check both before reading a file or
opening a transport. Mount the files as resources. The certificate and key are the emitter's client
identity. `tls_ca_file` adds a trust root rather than replacing the built-in ones: `grpc` trusts the
bundled WebPKI roots and the file, and `http/protobuf` trusts the platform's trust store and the
file.

Opening an emitter reads its TLS files and builds its transport, but contacts no endpoint: the
`grpc` channel connects when an export first needs a connection, and the `http/protobuf` client
connects when a request needs one. Both resolve the endpoint's host through the node's configured
DNS resolver for every new connection, and keep that host for the HTTP authority and the TLS server
name. An endpoint that does not resolve, refuses the connection or never answers is therefore never
an initialization error: the export fails, `DESCRIBE EMITTER` reports a transient error, and the
emitter retries the export on its `RETRY POLICY`. A missing or invalid TLS file, or a header the
emitter refuses, fails its initialization instead. Nervix does not provision collectors, indexes,
tenants or vendor-side telemetry objects; the OTLP receiver must already exist.

One log record is mapped as follows:

```plexus
CREATE OTEL LOGS EMITTER audit_to_otel
  FROM audit_events
  TO otel_main
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    'time' = input.event_ts,
    'severity_text' = input.level,
    'severity_number' = input.level_num,
    'body' = input.message,
    'trace_id' = input.trace_id,
    'span_id' = input.span_id
  }
  ATTRIBUTES {
    'user.id' = input.user_id,
    'audit.action' = leak_sensitive(input.action)
  }
  RESOURCE {
    'service.name' = 'checkout-pipeline',
    'deployment.environment.name' = 'prod'
  }
  SCOPE 'nervix/audit' VERSION '1.0' FLUSH EACH 2s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The clause order is fixed: destination and metric options, `MODE`, required `VALUES`, optional
`ATTRIBUTES`, optional `RESOURCE`, then optional `SCOPE '<name>' [VERSION '<version>']`.
`RESOURCE` is literal-only and `SCOPE` is fixed, so every request an emitter sends carries the
same resource and scope. Normal sensitivity rules apply to every expression, including
`ATTRIBUTES`.

`CREATE` and `ALTER` check the mappings against the signal's table above: every key must be
declared and have its exact type, and every required key must be mapped. Literal severities,
identifiers and enum names are also checked against the declared value domains before the
statement is accepted. For example, `(25 AS I32)` is outside the severity range even though it has
the required type. Ranges that depend on a record's values, and a histogram's bucket cardinality,
are checked while that record is encoded.

Each metric emitter exports exactly one metric:

```plexus,ignore
CREATE OTEL METRICS EMITTER requests FROM windows
TO otel_main
METRIC 'http.server.request.count' UNIT '1'
DESCRIPTION 'Completed HTTP requests'
SUM MONOTONIC DELTA
MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
VALUES {
  'time' = input.window_end,
  'start_time' = input.window_start,
  'value' = input.request_count
}
ATTRIBUTES { 'http.route' = input.route }
RESOURCE { 'service.name' = 'checkout-pipeline' }
```

`METRIC` names the metric and its required `UNIT`, with an optional `DESCRIPTION`, followed by its
kind: `GAUGE`, `SUM [MONOTONIC] (DELTA | CUMULATIVE)` or `HISTOGRAM (DELTA | CUMULATIVE)`. A gauge or
sum maps the keys of the generated table's `GAUGE / SUM` rows, and a histogram those of its
`HISTOGRAM` rows. An integer `value` is sent as OTLP `as_int` and a floating-point one as
`as_double`.

BATCH is optional and has no learned protocol maximum. A Collector's common 4 MiB gRPC receive
default is deployment advice: configure the receiver and the emitter's `MAX SIZE` together.
Nervix does not impose that receiver default as a limit on every Export request.

Nervix builds Export requests from one pending Arrow batch at a time. Without `BATCH`, a batch
becomes one Export request carrying the emitter's resource and scope and every record the batch
converted successfully; a metrics request carries the emitter's one metric with a data point for
each such record. With `BATCH`, Nervix takes the batch's converted records in source order and
divides them by `MAX MESSAGES` and the exact protobuf size of each request, keeping the resource and
scope in every one. A request never combines records of two Arrow batches, even when one flush
hands over several batches of the same relay and branch that `MAX MESSAGES` and `MAX SIZE` would
hold together. When a candidate exceeds `MAX SIZE`, it is halved until each request fits, and a
record whose request alone exceeds it follows `ON MESSAGE ERROR` as a `validation` error of the
`encode` operation. The byte limit measures the uncompressed protobuf request before optional gzip;
HTTP and gRPC framing and headers are outside it. `FLUSH ... MAX BATCH SIZE` continues to measure
the Arrow batch.

Nervix prepares each Export request once. It stamps log `observed_time_unix_nano` from the node's
actual UTC clock when it prepares the request, and keeps the request's exact protobuf bytes, and
the records they carry, until the receiver answers for it. A request whose outcome Nervix did not
learn — the connection closed before the answer, the request timed out, or the receiver asked for a
retry — is sent again with the same bytes: the same records in the same order, the same resource,
scope and observed timestamps, compressed the same way. The retry follows the requests the receiver
already answered, never sends them again, and never regroups the kept records with records that
arrived later. A receiver therefore sees a duplicate only as a repeat of a request it may already
hold.

Connection failures, timeouts, lost responses, HTTP `429` and `5xx`, gRPC `RESOURCE_EXHAUSTED`, and
the gRPC codes the OTLP specification lists as retryable — `CANCELLED`, `DEADLINE_EXCEEDED`,
`ABORTED`, `OUT_OF_RANGE`, `UNAVAILABLE` and `DATA_LOSS` — retry with backpressure.
`Retry-After` and gRPC `RetryInfo` can extend the declared retry delay. Bad IDs, enum strings,
severity values, numeric ranges, or histogram shapes reject only the affected record through `ON
MESSAGE ERROR`. HTTP `400` and gRPC `INVALID_ARGUMENT` reject every record in that request without
retry, with one shared error reference. Any other HTTP status or gRPC code the receiver answers
with means the endpoint cannot accept the export as configured: Nervix does not retry it, and the
emitter's unresolved records follow `ON MESSAGE ERROR` as errors of the `publish` operation. HTTP
`413 Payload Too Large` is such an answer, so a request the receiver finds too large is neither
retried nor divided further; declare `BATCH` with a `MAX SIZE` within the receiver's limit. OTLP
`partial_success` cannot be retried safely: Nervix acknowledges every record and logs a warning, so
records rejected by the receiver in that response are lost.
For a local conversion failure, the rejected record identifies its `otel.<key>` field and gives a
bounded reason without quoting that field's value. The conversion cause stays in the connector's
internal report; the failure does not reject other records in the request.

### ClickHouse

<!-- connector-matrix:emit-clickhouse-table:start -->
```plexus,ignore
CREATE CLICKHOUSE TABLE EMITTER <name>
  FROM <relay>
  TO <client> TABLE 'sales.Orders'
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  <construction>
  BATCH MAX MESSAGES <count> MAX SIZE <bytes>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | CLICKHOUSE client |
| Locator and validation rule | TABLE 'sales.Orders' (clickhouse_table) |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | No entity-specific options. |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | required |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | The connector holds no buffer between its transport and the host. |
| Network waits | an insert: connection, TLS, upload and the response: `timeout_ms`; required |
| Driver limits | None declared. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [CLICKHOUSE TABLE capability and mirror reference](connector-capabilities.md#clickhouse-table-emit).
<!-- connector-matrix:emit-clickhouse-table:end -->

```plexus
CREATE CLICKHOUSE TABLE EMITTER to_ch
  FROM notifications
  TO clickhouse_client TABLE 'my_table'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    "clickhouse_user_id" = input.user_id,
    "clickhouse_now" = NOW(),
    "clickhouse_action" = LOWER(input.action)
  } BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

ClickHouse clients use the HTTP endpoint:

```plexus
CREATE CLICKHOUSE CLIENT ch
  CONFIG {
    'addr' = 'http://127.0.0.1:8123',
    'user' = 'default',
    'password' = 'nervix',
    'timeout_ms' = 5000
  };
```

`'addr'` and a positive `'timeout_ms'` are required. Optional keys are `'user'`, `'password'`,
`'database'`, `'tls_ca_file'`, `'tls_cert_file'`, `'tls_key_file'`, `'tcp_keepalive_ms'`, and
`'pool_idle_timeout_ms'`; unknown keys are refused.
The timeout bounds both sending an insert body and waiting for ClickHouse to finish the insert and
return its result.

`'addr'` is the `http` or `https` URL of ClickHouse's HTTP interface. Every insert goes to its host,
port and path with a query string the client composes itself: the `INSERT` statement, `'database'`
when it is set, and `decompress=1`. A query string written in `'addr'` is not sent. ClickHouse
authenticates each insert by `'user'` and `'password'`, which the client sends as the
`X-ClickHouse-User` and `X-ClickHouse-Key` request headers; without `'user'`, ClickHouse applies its
`default` user. `'database'` names the database of a one-part `TABLE`; a two-part `TABLE` names its
own.

An `https` address requires `'tls_ca_file'` pointing to mounted trust material. An `http` address
refuses all three TLS file keys. The certificates in `'tls_ca_file'` are added to the bundled public
roots and the node's own trust store; they do not replace them. A client certificate and key must be
supplied together, as PEM files in `'tls_cert_file'` and `'tls_key_file'`. The client presents that
certificate whenever the server's TLS handshake asks for one, for a server that verifies client
certificates. The certificate identifies the connection, not the user: the client never sends
ClickHouse's `X-ClickHouse-SSL-Certificate-Auth` header, so ClickHouse does not authenticate the user
by the certificate, and a ClickHouse user identified with `ssl_certificate` cannot insert through a
ClickHouse client.

`tcp_keepalive_ms` sets the TCP idle interval before keep-alive probes begin; its declared default
is 60000 milliseconds. `pool_idle_timeout_ms` limits how long an unused HTTP connection may be
reused; its declared default is 2000 milliseconds. The pool idle timeout requires positive,
schedulable whole milliseconds. TCP keep-alive requires whole seconds from 1 through 32767,
expressed in milliseconds, matching the socket's precision and supported range. Set the pool idle
timeout below the server's `keep_alive_timeout`, and set TCP keep-alive below an intervening NAT or
load balancer's idle limit. These settings apply to HTTP
and HTTPS. They govern connection reuse and probing; `timeout_ms` bounds each insert.

A ClickHouse client takes no `POOL SIZE` clause, and no declared bound limits its connections. Each
running ClickHouse emitter builds its own HTTP client from the client's configuration, so separate
emitters of one client never share a connection. An emitter sends one insert at a time and uses one
connection while the insert waits for its answer. The connection then stays open for the emitter's
next insert until it has been idle for `pool_idle_timeout_ms`, and closes after that; the client
keeps idle connections without a count limit. The database clients that declare a bounded pool are
described in [Database client pools](./database-client-pools.md).

`DESCRIBE CLIENT ch` reports the configured keys and effective declared defaults, marking an
omitted default with `(default)` and redacting confidential values. `SHOW CREATE CLIENT ch`
preserves authored configuration, with secrets redacted.

The client resolves the host in `addr` through the node's asynchronous resolver for each new
connection and tries the answers in order; a literal IPv4 or IPv6 address is dialled as written.
Every request keeps `addr` as its authority, and over HTTPS the server certificate must name that
host, whichever address accepted the connection. The connection, lookup included, is made while the
insert waits for its result, so `timeout_ms` bounds it too. A missing name, a silent name server or
an unreachable answer fails the insert, which the emitter retries on its `RETRY POLICY`; nothing is
acknowledged until ClickHouse returns the insert's result. A pooled connection stays in use when its
host's answer changes, and the next connection resolves again.

ClickHouse requires the [batching clause](#batching). A flush is written as inserts of at most
`MAX MESSAGES` rows whose `JSONEachRow` body is at most `MAX SIZE` bytes, as
[Database writes](#database-writes) describes, and each successful insert is an acknowledgment.
The ClickHouse driver compresses each insert body with LZ4, in ClickHouse's compressed-block format,
and `decompress=1` tells the server to decompress it. Compression is the driver's default: Nervix
neither configures it nor offers a key to change it, and `MAX SIZE` measures the body before it is
compressed.
ClickHouse writes each `JSONEachRow` line directly from the mapped Arrow columns in `VALUES`
order, using the same typed JSON column writer as schemaful JSON emission. Null mapped values are
written as `null`, including null list elements. Column names are escaped once for each write, and
string values use their carrier's escape classification. `TABLE` names preserve their case and
contain one part (`'Orders'`) or two nonempty dot-separated parts (`'sales.Orders'`). Nervix quotes
each part with backticks in the insert SQL, so `sales.Orders` is sent as
`` `sales`.`Orders` ``. The table-name alphabet admits no backtick, so no quoted part needs
escaping. A third part or empty part is refused when the emitter is declared.

The ClickHouse `VALUES` encoding contract is:

| Plexus mapped type | `JSONEachRow` representation |
| --- | --- |
| `BOOL` | JSON boolean |
| `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64` | JSON integer number, including the full `U64` range |
| `F32`, `F64` | JSON number; `F32` is widened to `F64`; NaN and infinities become `null` |
| `STRING` | JSON string with escaped quotes, backslashes, and control characters |
| `BYTES` | Raw octets in a JSON string, escaping quote, backslash, and control octets; no base64 conversion |
| `DATETIME` | RFC 3339 JSON string |
| `ARRAY<T>` and `VEC<T>` | JSON array, recursively applying the element encoding, including for nested `BYTES` |

An optional mapped value writes `null`, including an optional list element. Mappings whose inferred
Arrow type is outside this domain are refused before the emitter starts: null-only values, decimals,
structured or dictionary values, large strings or binaries, timestamps outside nanoseconds, and
lists containing a refused element type. A `VALUES` target column name may appear only once.
ClickHouse declares only `MODE ACK`; `ON CONFLICT`, per-record header writes, and an envelope are
unavailable.
`F32` columns keep ClickHouse's JSON number formatting after widening to `F64`; schemaful JSON
codecs format `F32` directly. A `BYTES` value is written as its own octets, which a `String` column
stores unchanged, where schemaful JSON codecs write padded base64.

ClickHouse answers a failed insert with an exception whose name its response text carries in
parentheses, such as `(CANNOT_PARSE_NUMBER)`. The emitter treats an insert as refused for its rows
only when that name is `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`, `CANNOT_PARSE_DATE`,
`CANNOT_PARSE_DATETIME`, `CANNOT_PARSE_NUMBER`, `CANNOT_PARSE_TEXT`, `TOO_LARGE_STRING_SIZE` or
`VIOLATED_CONSTRAINT`, or when the answer's text contains `413 Payload Too Large` or
`413 Request Entity Too Large`, as a proxy in front of ClickHouse answers an oversized body; the
emitter names that refusal `PAYLOAD_TOO_LARGE`. Every other failure is infrastructure-wide: another
exception name, a lost connection, an expired `timeout_ms` or a failed lookup leaves the insert's
rows unresolved, and the emitter retries them on its `RETRY POLICY`.
For ClickHouse, Postgres, and MySQL, a failed multi-row insert is classified first as
record-specific or infrastructure-wide. Infrastructure failures retry with backpressure. A
record-specific failure is isolated by re-executing the insert one record at a time so healthy rows
land and only poison rows follow `ON MESSAGE ERROR`. Postgres and MySQL re-execute them on the
connection the failed insert already holds, so isolation does not wait for the client's pool again,
as [Database client pools](./database-client-pools.md#borrowing-and-waiting) describes. Isolation can
reapply rows from the failed insert; use the emitter's idempotent write facilities where available.
Dividing a flush into inserts also means one flush is not an atomic database transaction.
For a rejected row, the error names a safe ClickHouse error name, Postgres SQLSTATE, or MySQL
SQLSTATE and code. Destination response text can quote a bound value, so it is not included in
diagnostics. Connection and pool failures retain their causes for diagnosing a retry.

### Postgres

```plexus
CREATE POSTGRES TABLE EMITTER to_pg
  FROM notifications
  TO postgres_client TABLE 'my_table'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    "postgres_user_id" = input.user_id,
    "postgres_now" = NOW() AS STRING,
    "postgres_action" = LOWER(input.action)
  } BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Postgres emitters use `VALUES` expressions and insert batches with `INSERT ... SELECT ... FROM
unnest(...)`. The [batching clause](#batching) is required: each insert carries at most
`MAX MESSAGES` records, and its statement text and bound arrays measure at most `MAX SIZE` bytes, as
[Database writes](#database-writes) describes. The insert result acknowledges those records. On the
poison-isolation path, tables without an idempotent `ON CONFLICT` policy may observe duplicates when
healthy records are re-executed.

`TABLE` names a table, or a schema and its table, as one or two nonempty parts separated by `.`:
`TABLE 'Orders'` or `TABLE 'sales.Orders'`, each part on the table identifier alphabet and the whole
name within 128 bytes. Nervix sends each part as its own quoted identifier with its case kept, so
`'sales.Orders'` is the relation `"sales"."Orders"`, and the insert and the lookup of the table's
column types name that same relation. A one-part name resolves through the connection's
`search_path`. An empty part or a third part is refused at `CREATE`.

Each mapped value is bound as text, in the one text array its column binds, and the statement casts
it to the column's declared type:

| Plexus type | Bound text |
| --- | --- |
| `BOOL` | `true` or `false` |
| `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64` | Decimal integer text, through the full `U64` range |
| `F32`, `F64` | The shortest decimal text that reads back as the same `F64`, an `F32` widened first; a non-finite value binds `NULL` |
| `STRING` | The UTF-8 text |
| `BYTES` | `bytea` hex text: `\x`, then two hex digits per octet |
| `DATETIME` | RFC 3339 timestamp text at UTC offset `+00:00` |
| `ARRAY<T>`, `VEC<T>` | Recursive JSON text; `BYTES` elements are padded base64 and non-finite floats are `null` |

A null binds as a `NULL` element. A mapping whose inferred type is none of these is refused at
`CREATE`. A value the column's type cannot read from its text, such as `U64` above a `bigint`'s
range, is a row Postgres rejects with its SQLSTATE.

Postgres emitters may include an insert conflict policy after `VALUES`:

<!-- connector-matrix:emit-postgres-table:start -->
```plexus,ignore
CREATE POSTGRES TABLE EMITTER <name>
  FROM <relay>
  TO <client> TABLE 'sales.Orders'
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ON CONFLICT DO NOTHING | ON CONFLICT ('id') DO UPDATE]
  <construction>
  BATCH MAX MESSAGES <count> MAX SIZE <bytes>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | POSTGRES client |
| Locator and validation rule | TABLE 'sales.Orders' (postgres_table) |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (composed clause); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | required |
| Protocol batch maximum | 1073741822B |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |
| Network waits | a pool acquire: name resolution, connection, TLS and authentication: fixed 30s<br>an insert and its metadata query: unbounded |
| Driver limits | None declared. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [POSTGRES TABLE capability and mirror reference](connector-capabilities.md#postgres-table-emit).
<!-- connector-matrix:emit-postgres-table:end -->

`DO UPDATE` updates every mapped `VALUES` column except the conflict target columns, so it requires
a conflict target and at least one mapped column outside it. `DO NOTHING` may be used with or
without a target; without one, it skips a row that conflicts on any unique constraint. Target
columns are nonempty names of the table's unique index columns, in written order, and need not be
mapped. A clause that breaks these rules is refused at `CREATE`. Postgres refuses a `DO UPDATE`
insert that carries one conflict key twice, so such an insert is written again one record at a time,
and the later record updates the earlier one.

Postgres clients declare their connection-pool bounds and connect with a `postgres://` or
`postgresql://` URL. Nervix owns the URL's scheme and its TLS policy: the URL sets `sslmode`
exactly once, under that name or the driver's `ssl-mode`, to one of two policies: `sslmode=disable`
for an unencrypted connection, or `sslmode=verify-full` for TLS with certificate-chain and hostname
verification. There is no opportunistic fallback and no encrypted connection without peer
verification. `CREATE CLIENT` refuses another scheme, a missing, repeated or other `sslmode`, and
TLS files without `sslmode=verify-full`, each naming the key and the rule rather than the URL. The
driver reads the URL's other query options, and its parser checks them before the client is
committed. See [Database Client Connection Pools](database-client-pools.md) for the accepted
pool counts:

```plexus
CREATE POSTGRES CLIENT pg
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable'
  };
```

For TLS connections, use `sslmode=verify-full`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path. `'tls_cert_file'` and `'tls_key_file'` supply a client identity and must be given together. TLS files require `sslmode=verify-full`.

### MySQL

```plexus
CREATE MYSQL TABLE EMITTER to_mysql
  FROM notifications
  TO mysql_client TABLE 'my_table'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    "mysql_user_id" = input.user_id,
    "mysql_now" = NOW() AS STRING,
    "mysql_action" = LOWER(input.action)
  } BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MySQL emitters use `VALUES` expressions and insert batches with a multi-row `INSERT ... VALUES (?,
...), ...` command. The [batching clause](#batching) is required: each insert carries at most
`MAX MESSAGES` records and no more than fit the 65,535 placeholders one statement binds, and its
statement text and bound values measure at most `MAX SIZE` bytes, as
[Database writes](#database-writes) describes. The insert result acknowledges those records.
An insert and its row-by-row isolation use one borrowed connection, returned before the next
insert. Isolation obtains each row's outcome without a second acquisition from the client's pool,
even when other emitters are waiting. The host handles those outcomes when the write completes.
Conflict clauses are the user's tool for bounding duplicates when poison isolation re-executes a
failed insert. For `c` mapped columns, the placeholder limit allows at most `floor(65535 / c)`
rows per insert, even when `MAX MESSAGES` is larger. `MAX SIZE` measures the SQL text and binary
bound values; it excludes packet headers, parameter types and the execute packet's null bitmap.
Nervix does not learn the server's `max_allowed_packet` to narrow these inserts. Set `MAX SIZE`
below the server's configured packet limit with room for that framing, and keep individual
values within the server's limit. Equal byte limits do not guarantee that every packet fits.
A packet or bound value the driver or server refuses can still fail a write; automatic sizing
against that server setting is outside the current contract.

MySQL emitters may include an insert conflict policy after `VALUES`:

<!-- connector-matrix:emit-mysql-table:start -->
```plexus,ignore
CREATE MYSQL TABLE EMITTER <name>
  FROM <relay>
  TO <client> TABLE 'Orders'
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ON CONFLICT DO NOTHING | ON CONFLICT DO UPDATE]
  <construction>
  BATCH MAX MESSAGES <count> MAX SIZE <bytes>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | MYSQL client |
| Locator and validation rule | TABLE 'Orders' (mysql_table) |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ON CONFLICT DO NOTHING` (keyword choice); `ON CONFLICT DO UPDATE` (keyword choice); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | required |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |
| Network waits | pool checkout (including connection and handshake), initial validation, and each insert: `timeout_ms`; unbounded when omitted |
| Driver limits | Without timeout_ms, pool checkout, connection validation and inserts have no deadline.<br>TLS identity keys use the driver's RSA PKCS#1 private-key format.<br>A prepared insert binds at most 65,535 placeholders; mapped column count narrows its row limit. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [MYSQL TABLE capability and mirror reference](connector-capabilities.md#mysql-table-emit).
<!-- connector-matrix:emit-mysql-table:end -->

`TABLE` names one nonempty table part or two nonempty database/table parts, for example
`'Orders'` or `'shop.Orders'`. The complete name uses 1–128 ASCII bytes from letters, digits,
`_`, `-`, `~` and the separating dot. Nervix preserves case and quotes each part separately:
`'shop.Orders'` becomes `` `shop`.`Orders` ``. Column keys are quoted as single identifiers,
including any dot in a column key. Empty parts and more than two parts are refused when parsed
or decoded.

MySQL and MariaDB resolve conflicts through primary and unique keys already defined on the table,
so the Plexus conflict policy takes no target list. `DO UPDATE` executes
`ON DUPLICATE KEY UPDATE col = VALUES(col)` for every mapped `VALUES` column, including mapped
key columns. `DO NOTHING` executes `ON DUPLICATE KEY UPDATE first = first`, where `first` is
the first mapped column. This is a duplicate-key update and can run update triggers; it is not
an insert-ignore operation. Without `ON CONFLICT`, the connector sends an ordinary insert;
a duplicate-key rejection follows `ON MESSAGE ERROR`. `INSERT IGNORE`, `REPLACE` and `LOAD DATA`
are not offered by this entity.

The connector binds the following exact Plexus types through MySQL's binary protocol. Nullable
values bind as SQL `NULL`. The target table must already exist and provide compatible columns.

| Plexus type | Bound representation |
| --- | --- |
| `BOOL` | Integer `0` or `1` |
| `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `I64` | Signed 64-bit integer |
| `U64` | Signed integer through `i64::MAX`; unsigned integer above it |
| `F32`, `F64` | Double, including non-finite values; the server decides whether its column accepts the value |
| `STRING` | UTF-8 bytes |
| `BYTES` | The original octets |
| `DATETIME` | RFC 3339 timestamp bytes with nanosecond precision where needed |
| `ARRAY`, `VEC` | JSON text, recursively; byte elements use padded base64 and datetime elements use RFC 3339 strings; null elements and non-finite floating-point elements become JSON `null` |

Other Arrow representations are outside the declared direct-value contract. Mapped expressions
are checked at `CREATE` and `ALTER`; duplicate `VALUES` keys are refused before execution.

MySQL clients declare their connection-pool bounds and use a mysql_async connection URL. See
[Database Client Connection Pools](database-client-pools.md) for the accepted counts:

```plexus
CREATE MYSQL CLIENT mysql
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
  };
```

Enable TLS with `require_ssl=true` in `addr`, or by supplying TLS files from a mounted resource.
`tls_ca_file` supplies the server CA path. With `require_ssl=true`, the URL's `verify_ca`,
`verify_identity` and `built_in_roots` choices are preserved when files are added. With files
alone, Nervix enables TLS using the driver's default verification policy: certificate and hostname
verification enabled, with built-in roots trusted. Include `require_ssl=true` to apply custom URL
verification choices. For mutual TLS, set `tls_cert_file` and `tls_key_file` together. The
driver reads a PEM certificate chain and an RSA PKCS#1 private key; for a PEM key this is the
`RSA PRIVATE KEY` form, which OpenSSL can produce with `openssl rsa -traditional`.

The current client CONFIG keys are required `addr` and optional `timeout_ms`, `tls_ca_file`,
`tls_cert_file` and `tls_key_file`. The address must
be a `mysql://` URL accepted by the driver's offline parser. Declare capacity with
`POOL SIZE MIN ... MAX ...`. Nervix installs those bounds on default driver pool options;
[Database Client Connection Pools](database-client-pools.md) details the resulting URL lifetime settings.
Pool-size query parameters such as `pool_min` and `pool_max` are refused before the client is
committed, including their percent-encoded spellings and ASCII case variants. Unknown standalone
CONFIG keys are refused rather than ignored. The generated [MYSQL client key reference](connector-capabilities.md#mysql-client)
lists each key's presence, type and confidentiality; it does not expose additional pool-lifetime
keys. The [pool minimum](database-client-pools.md#how-each-client-type-keeps-its-minimum) keeper waits
one second before each pass, counting idle and checked-out connections together.

`timeout_ms` is a positive, schedulable whole number of milliseconds. It bounds opening and
validating the pool's first connection, each subsequent pool checkout (including connection
establishment and authentication), and each insert. The minimum keeper uses the same checkout
bound. Each of these is a separate operation; an insert's bound starts after checkout, and an
isolated row's insert receives its own bound. Omitting the key leaves these waits unbounded.
An elapsed bound is an infrastructure failure, reported with `timeout_ms` and its duration, and
the emitter retries its pending rows on its declared policy. The driver discards a connection
whose in-flight command was cancelled. A timeout does not establish whether the server committed
an insert; use a stable key and an appropriate `ON CONFLICT` action where retries must converge.

### MongoDB

```plexus
CREATE MONGODB COLLECTION EMITTER to_mongodb
  FROM notifications
  TO mongodb_client COLLECTION 'my_collection'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    "mongodb_user_id" = input.user_id,
    "mongodb_now" = NOW() AS STRING,
    "mongodb_action" = LOWER(input.action)
  } BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MongoDB emitters use `VALUES` expressions and bulk writes. The [batching clause](#batching) is
required: each write carries at most `MAX MESSAGES` documents, which measure at most `MAX SIZE`
bytes, as [Database writes](#database-writes) describes. A document larger than MongoDB's 16 MiB
document limit is rejected before the write that would carry it.

Nervix never creates a collection: provision it, with any validator and indexes it needs, before
the emitter writes to it. Whenever the emitter writes rows, on a flush and again on every retry, it
first looks the collection up by name in the client's database. The lookup needs no privilege
beyond those that let the client's user write the collection. A missing collection fails the write
before any document is sent: every row stays unresolved and is written again on the emitter's
`RETRY POLICY` backoff, and `DESCRIBE EMITTER` shows `MongoDB target collection is not provisioned`
as the emitter's `transient error` until the collection exists. The lookup and the write are
separate commands, so a collection dropped between them can be created again by the write, as
MongoDB creates a missing collection on its first insert.

Every write is unordered, and MongoDB's answer to a write says what became of its documents even
when the write fails, so no separate isolation pass is needed. A document the server applied is
delivered. A document the server refuses for good — one the collection's validator refuses
(code 121), one too large to store (code 10334) or one that repeats a unique key (code 11000) —
follows `ON MESSAGE ERROR` as an `external` `publish` error naming the code, such as
`MongoDB rejected document with code 11000`. Every other document stays unresolved and is written
again on the emitter's `RETRY POLICY` backoff, so a retry never carries a document the server
applied or refused for good. A write the server never answered, such as one whose connection
failed, leaves every document it carried unresolved, so its retry can store a document twice, as
[Database writes](#database-writes) describes.

A write concern the deployment cannot satisfy, such as `w=2` on a single-member replica set or a
`w=majority` write whose `wtimeoutMS` ran out, does not undo the documents the server applied. The
server reports the write concern failure beside its answer for each document, so the write's
documents are delivered, rejected or left unresolved exactly as above, and the documents it applied
complete their acknowledgements although the write failed. The write still fails as an
infrastructure failure: `DESCRIBE EMITTER` shows it as the emitter's `transient error`, naming the
request and the server's code, such as
`MongoDB insert_many write concern failed with code 100 (UnsatisfiableWriteConcern)`, with
`bulk_write` in place of `insert_many` for an `ON CONFLICT` write. The attempt ends with that
write, and the retry carries only the documents it left unresolved together with the rows the
attempt had not written yet. While the write concern keeps failing, each attempt therefore writes
one insert or bulk write and then waits out the backoff, so a lasting failure slows the emitter
instead of writing any document twice.

The write concern is the one the client's `addr` URL asks for, such as `w=majority`, or otherwise
the deployment's default. A document the server applied under a failed write concern is delivered
on that server's answer alone: if the deployment later rolls the write back, for example because
the primary that applied it stepped down before a majority of the replica set held it, the
document is lost, and the emitter can neither see nor repair that.

MongoDB maps values to these native BSON types. Nullability is preserved, including null elements
inside arrays, and BSON field order follows `VALUES` order.

| Nervix value | BSON value |
| --- | --- |
| `BOOL` | Boolean |
| `I8`, `I16`, `I32`, `U8`, `U16` | Int32 |
| `I64`, `U32` | Int64 |
| `U64` | Int64 when at most `i64::MAX`; otherwise a record rejection |
| `F32`, `F64` | Double, including non-finite values |
| `STRING` | String |
| `BYTES` | Binary with the Generic subtype |
| `DATETIME` | Date, rounded down to whole Unix milliseconds |
| `ARRAY`, `VEC` | Array, with each element mapped by this table |

These integer widths and native dates are the current encoding contract. Recreate collections
whose validators, indexes or queries require another stored representation; Nervix provides no
conversion of existing documents. Unsupported mapped types, repeated `VALUES` keys and keys
containing a NUL byte are refused at `CREATE` and `ALTER`, before a graph starts. BSON keys retain
their spelling, including dots, dollar signs and Unicode; Nervix does not rewrite them.

A `U64` value above the signed 64-bit range has no BSON integer. Such a record is rejected before its document is written: it follows
`ON MESSAGE ERROR` with a `validation` error whose affected field is `mongodb.<column>`, it is never
inserted, and its value is never used as an `ON CONFLICT` target. The rejection is permanent, so the
record does not retry. Every other record in the same write is unaffected, and a mapped value that
is genuinely NULL is still written as BSON null.

MongoDB emitters may include an insert conflict policy after `VALUES`:

<!-- connector-matrix:emit-mongodb-collection:start -->
```plexus,ignore
CREATE MONGODB COLLECTION EMITTER <name>
  FROM <relay>
  TO <client> COLLECTION 'Orders'
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  [ON CONFLICT ('id') DO NOTHING | ON CONFLICT ('id') DO UPDATE]
  <construction>
  BATCH MAX MESSAGES <count> MAX SIZE <bytes>
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | MONGODB client |
| Locator and validation rule | COLLECTION 'Orders' (mongodb_collection) |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `ON CONFLICT ('id') DO NOTHING` (keyword choice); `ON CONFLICT ('id') DO UPDATE` (keyword choice); optional; omitted when unset |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | required |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | connections the pool holds: set by POOL SIZE MAX; when full, holds back what fills it |
| Network waits | connection: name resolution, TCP and TLS: driver default, connectTimeoutMS, 10 s<br>server selection: driver default, 30 s<br>the handshake, a pool wait, the collection lookup and a write: unbounded |
| Driver limits | A document above the declared BSON document limit is rejected. |
| Stalled-peer checks | None: no client deadline key bounds a wait. |

See the [MONGODB COLLECTION capability and mirror reference](connector-capabilities.md#mongodb-collection-emit).
<!-- connector-matrix:emit-mongodb-collection:end -->

A conflict policy names at least one target field as a quoted `VALUES` key, such as
`ON CONFLICT ('tenant', 'order_id') DO UPDATE`, and every target field must be mapped in `VALUES`.
`DO UPDATE` must also map at least one field outside its target, because those are the fields it
updates. `CREATE` and `ALTER` refuse a policy without a target, a target field that `VALUES` does
not map, and a `DO UPDATE` whose `VALUES` maps only target fields.

Either policy writes every row as an upsert: it updates one document whose target fields equal the
row's mapped values, or inserts the row's complete mapped document when no document matches.
Matching follows MongoDB's equality queries, so a null target value also matches a document that
lacks the field, and a row updates at most one document even when several match. On a match,
`DO NOTHING` leaves the document as it is, and `DO UPDATE` sets every mapped field outside the
target and keeps the fields `VALUES` does not map. Neither policy has an update-only or a replacing
form: a row that matches no document is always inserted, and a matched document is never replaced
or deleted. The rules need no index, but upserts of the same target that run at the same time, such
as from two emitters, can each insert a document. Create a unique index on exactly the target fields
to keep one document per target.

`COLLECTION` preserves its literal text, including case, spaces and Unicode. A collection name
must be nonempty, contain neither NUL nor `$`, not start with `system.`, and not contain
`.system.`. The database, one separating dot and the collection must fit Nervix's declared 120-byte
namespace budget. The locator reserves at least two bytes for the database and separator; the
complete namespace is checked against the resolved database before the emitter opens. Database
and collection lengths are UTF-8 byte lengths.

Emitters using either MongoDB `ON CONFLICT` form require MongoDB 8.0 or newer because those modes
execute each write as one client bulk write.

MongoDB clients declare their connection-pool bounds and a MongoDB connection URL, and resolve the
database their emitters write into. See [Database Client Connection Pools](database-client-pools.md)
for the accepted counts and how the driver waits for a connection:

```plexus
CREATE MONGODB CLIENT mongodb
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
    'database' = 'nervix'
  };
```

The client requires an `addr` that is a `mongodb://` URL naming its hosts; a `mongodb+srv://`
address, which needs a DNS lookup to find them, is refused. The URL's query options are delegated to
the MongoDB driver and checked by its parser at `CREATE` without resolving a hostname or connecting.
`minPoolSize` and `maxPoolSize` are refused with a `POOL SIZE` diagnostic, because the clause owns
both bounds. Every other option keeps the driver's meaning, such as `w` and `wtimeoutMS` for the
write concern, or `connectTimeoutMS` and `serverSelectionTimeoutMS` for the driver's deadlines.

The database comes from the optional `database` key, or else from the URL's path, such as `nervix`
in `mongodb://127.0.0.1:27017/nervix`; an explicit `database` overrides the path, and a client with
neither is refused at `CREATE`. A database name holds 1 to 63 bytes and none of `/`, `\`, `.`, `"`,
`$`, a space or NUL. Unknown CONFIG keys are refused. Public inspection shows an `addr` that carries
credentials or query options as `********`.

For a private certificate authority, mount a TLS resource and set `'tls_ca_file'` to the mounted CA
path. For mutual TLS, also set `'tls_cert_file'` to the PEM certificate chain and `'tls_key_file'`
to its PEM private key, and supply both identity keys together. Any of these files enables TLS by
itself, so the URL needs no `tls=true`, while a URL with `tls=false` refuses them at `CREATE`. Each
file replaces only its counterpart among the URL's TLS options, `tlsCAFile` for the CA and
`tlsCertificateKeyFile` for the identity, and keeps the driver's other TLS options, such as
`tlsAllowInvalidCertificates`. Without these keys, the URL's own TLS options apply unchanged.

The MongoDB driver reads a combined certificate/key PEM. Nervix prepares that file privately on
its filesystem executor and retains it for the shared client's lifetime, including all clones;
the final client drop removes it. The mounted source files remain separate. This preparation and
the driver's certificate parsing happen on the filesystem executor when the client opens. The connector's offline
configuration check performs no filesystem or network I/O; resource binding keeps its own
validation boundary.

### Iceberg

<!-- connector-matrix:emit-iceberg-table:start -->
```plexus,ignore
CREATE ICEBERG TABLE EMITTER <name>
  FROM <relay>
  TO <storage_client> TABLE 'Orders'
  (NAMESPACE 'Analytics.Events')
  (LOCATION 's3://warehouse/orders')
  (CATALOG catalog)
  (COMMIT EACH 10s MAX SIZE 64MiB)
    MODE <mode>
    VALUES { '<field>' = <expression>, ... }
  <construction>
  [BATCH MAX MESSAGES <count> MAX SIZE <bytes>]
  FLUSH IMMEDIATE
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Angle brackets and alternatives form a synopsis; option values shown are declared examples. Choose one complete mode from the table.

| Capability | Declaration |
| --- | --- |
| Reference | S3 \| GCS \| AZURE_BLOB store and catalog clients |
| Locator and validation rule | TABLE 'Orders' (iceberg_table) |
| Modes (complete bodies after MODE) | ACK RETRY POLICY BACKOFF 1s MAX 30s |
| Options in statement order | `NAMESPACE 'Analytics.Events'` (string literal); required<br>`LOCATION 's3://warehouse/orders'` (string literal); required<br>`CATALOG catalog` (ICEBERG_REST client); required<br>`COMMIT EACH 10s MAX SIZE 64MiB` (composed clause); required |
| Headers | Not offered: The protocol writes structured values or rows; transport headers are not a per-record mapping in this entity. |
| Body | VALUES { '&lt;field&gt;' = &lt;expression&gt;, ... } |
| BATCH | optional |
| Protocol batch maximum | No additional protocol maximum is declared; general BATCH limits still apply. |
| Batch transformation required | no |
| Envelope | Not offered: This implementation maps payloads; it declares no additional per-record envelope clauses for this entity. |
| PROPERTIES keys and exact types | Not offered: This entity declares no typed PROPERTIES block; any encoded record fields belong to its payload or VALUES mapping. |
| Buffers | records staged on disk until a commit: set by COMMIT EACH ... MAX SIZE; when full, holds back what fills it |
| Network waits | catalog requests: unbounded<br>object-store metadata and delete operations: `timeout_ms`; 1m when omitted<br>object-store reads and writes: `timeout_ms`; 10s when omitted |
| Driver limits | None declared. |
| Stalled-peer checks | `STALLED PEER timeout_ms` |

See the [ICEBERG TABLE capability and mirror reference](connector-capabilities.md#iceberg-table-emit).
<!-- connector-matrix:emit-iceberg-table:end -->

```plexus
CREATE S3 CLIENT s3_main
  CONFIG {
    'endpoint' = 'http://127.0.0.1:9900',
    'region' = 'us-east-1',
    'access_key_id' = 'rustfsadmin',
    'secret_access_key' = 'rustfsadmin',
    'path_style_access' = true
  };

CREATE ICEBERG_REST CLIENT iceberg_catalog
  CONFIG {
    'uri' = 'http://127.0.0.1:8181',
    'warehouse' = 's3://nervix-iceberg/warehouse'
  };

CREATE ICEBERG TABLE EMITTER iceberg_notifications
  FROM notifications
  TO s3_main TABLE 'notifications'
  NAMESPACE 'analytics'
  LOCATION 's3://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  } FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Iceberg emitters use explicit `VALUES` expressions and do not declare `ENCODE USING`. The referenced storage client selects the S3, GCS or Azure Blob implementation and supplies the object-store connection for table files. The `CATALOG <client>` clause references a separate `ICEBERG_REST CLIENT` that supplies the REST catalog URI and warehouse. The referenced REST catalog namespace and table must already exist; Nervix loads that table and appends data, but does not create catalog namespaces or tables implicitly. The emitter owns the Iceberg table name, catalog namespace, mapped output columns, table location, catalog client reference, and flush policy. It appends Parquet data files to an unpartitioned table and changes nothing else about it; [Iceberg data files](#iceberg-data-files) describes the files it writes and what it leaves to other tools.

`NAMESPACE '<namespace>'` is required immediately after the table name. Its dot-separated levels
preserve case: `NAMESPACE 'Analytics.Events'` selects the two catalog levels `Analytics` and
`Events`. The namespace is independent of the Nervix domain. Its literal follows the same
1–128-byte identifier alphabet as the table, with no empty level. `SHOW CREATE EMITTER` retains
the declared namespace.

`LOCATION` must be an absolute object-store URL whose scheme matches the storage client: `s3`
for `S3`, `gs` for `GCS`, and `wasb`, `wasbs`, `abfs` or `abfss` for `AZURE_BLOB`. Nervix refuses
a mismatched scheme or a URL without a bucket or container authority at `CREATE` or `ALTER`,
naming `LOCATION`, before opening either client. An Azure location also names its container
before `@` and its account's storage service in its host:
`wasb://<container>@<account>.blob.<suffix>/...` and `wasbs://...` name the Blob service, while
`abfs://<container>@<account>.dfs.<suffix>/...` and `abfss://...` name the Data Lake service. The
emitter checks the container and the service when it opens; a location that lacks either fails the
open as a configuration failure, which `DESCRIBE EMITTER` shows as the emitter's transient error
while the emitter retries the open on its declared retry policy. `COMMIT EACH ... MAX SIZE` uses the
same positive whole-byte size syntax as `BATCH MAX SIZE`.

The four Iceberg client schemas distinguish Nervix-owned options from driver properties.
Nervix validates declared addresses, booleans and deadlines at `CREATE CLIENT`; for example,
`'path_style_access' = 'yes'` and `'no_auth' = 'yes'` are refused. A Boolean option means the value
it states: `'no_auth' = false` keeps the GCS client's authentication, and only `true` makes it send
unsigned requests without loading credentials. Unknown unprefixed keys are
refused rather than treated as driver settings. The generated
[client key tables](./connector-capabilities.md#s3-client) list the current keys and file roles.
S3 delegates `s3.*` and `client.*`, GCS delegates `gcs.*`, and Azure delegates `adls.*`.
The REST catalog delegates `header.*` and those storage-property namespaces. Delegated names
and values keep the driver's interpretation; a declaration is not a connectivity or credential
check, and a property the offline check cannot inspect is checked when its driver opens.
`CREATE CLIENT` also runs the object store's own property reader without opening a connection, so
it refuses an `s3.sse.type` other than `none`, `s3`, `kms` or `custom`, and any
`adls.connection-string`, whether the storage client or the catalog client sets it. The Azure
table's `LOCATION` selects its endpoint; its client supplies account and authentication options.
GCS inline credentials use the driver's Base64-encoded JSON form in `credentials_json`.

Both Iceberg clients verify an HTTPS server's certificate against the bundled WebPKI roots, the
platform's native roots and the client's own `tls_ca_file`. A client that also names `tls_cert_file`
and `tls_key_file` presents that certificate chain and private key as its identity. Each client
uses only its own files: the `ICEBERG_REST` client's for catalog requests, and the `S3`, `GCS` or
`AZURE_BLOB` client's for data-file reads and writes and for the HTTP calls of its credential
provider. Mount the resource that holds the files on each client that names them, as
[Resources](./resources.md) describes. Nervix reads the files when the emitter opens. A file it
cannot read or parse fails the open as a configuration failure that names whether the catalog or
the object-store client's TLS failed; `DESCRIBE EMITTER` shows it as the emitter's transient error,
and the emitter retries the open on its declared retry policy. A client whose address is plain
HTTP never uses its TLS files, though Nervix still reads them when the emitter opens.

The table's file I/O receives the storage client's properties and the catalog client's
storage-namespace properties. Each Nervix-owned storage option supplies one Iceberg property:
`endpoint` supplies `s3.endpoint`, `no_auth` supplies `gcs.no-auth`, `account_name` supplies
`adls.account-name`, and so on. A delegated property supplies itself, as written. The deadline and
TLS-file options configure the clients' own HTTP transport and supply no property. The two clients
must not set the same property. Nervix refuses `CREATE` or `ALTER` of an emitter whose catalog client
sets a property that its storage client supplies, and the refusal names the property, the
storage option that supplies it and both clients. Within a storage client, the last entry for a
property is its value. The REST catalog can supply file I/O properties as well: the configuration
it answers with when the emitter first reaches it, and the configuration it returns with the table,
such as storage credentials it vends, apply wherever neither client sets the same property. For
the catalog's own requests, its configuration answer follows the Iceberg REST protocol: its
`defaults` fill catalog properties the client leaves unset, and its `overrides`, `uri` included,
replace the client's values.

The catalog and object-store HTTP clients resolve endpoint names through the node's configured
DNS resolver. Name resolution changes no URL: catalog requests keep their host names and
authentication, while OpenDAL uses the resolver for object operations and credential HTTP calls.
DNS failures enter the existing emitter initialization or commit retry path; they do not advance
the commit or ACK boundary.

GCS uses the same emitter shape with a `GCS CLIENT` and `gs://` locations:

```plexus
CREATE GCS CLIENT gcs_main
  CONFIG {
    'service_path' = 'https://storage.googleapis.com',
    'token' = '<oauth2-token>'
  };

CREATE ICEBERG_REST CLIENT iceberg_catalog
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'gs://nervix-iceberg/warehouse'
  };

CREATE ICEBERG TABLE EMITTER iceberg_notifications
  FROM notifications
  TO gcs_main TABLE 'notifications'
  NAMESPACE 'analytics'
  LOCATION 'gs://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  } FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Azure uses an `AZURE_BLOB CLIENT` with a `wasbs://` or `abfss://` location, or `wasb://` or
`abfs://` for plain HTTP; [Iceberg object storage](#iceberg-object-storage) describes the endpoint
Nervix reaches for each:

```plexus
CREATE AZURE_BLOB CLIENT azure_main
  CONFIG {
    'account_name' = 'myaccount',
    'account_key' = '<account-key>'
  };

CREATE ICEBERG_REST CLIENT iceberg_catalog
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/warehouse'
  };

CREATE ICEBERG TABLE EMITTER iceberg_notifications
  FROM notifications
  TO azure_main TABLE 'notifications'
  NAMESPACE 'analytics'
  LOCATION 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  } FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The REST catalog is the authority for namespace and table metadata. Nervix does not write a separate object-store catalog pointer file and does not provision catalog entries from the emitter runtime path.

#### Iceberg catalog namespace and table

At open, the emitter loads the declared table from the REST catalog. Nervix creates neither the
namespace nor the table: a catalog that holds no namespace of the declared name, or no table of the
declared name in a namespace it holds, fails the open with the entity it lacks. A REST catalog
answers the load of a table in a missing namespace exactly as it answers the load of a missing
table, so the emitter asks the catalog for the namespace to tell the two apart. `DESCRIBE EMITTER`
shows the outcome as the emitter's transient error, and the emitter retries the open on its declared
retry policy, so it opens once an operator provisions what it lacks. Rows its relays deliver
meanwhile wait for that open, and the emitter commits them once it opens:

| Catalog state | Transient error |
| --- | --- |
| No namespace of the declared name | `ICEBERG namespace 'Analytics.Events' does not exist` |
| The namespace, without a table of the declared name in it | `ICEBERG table 'orders' does not exist in namespace 'Analytics.Events'` |
| A catalog that fails the load | `failed to load ICEBERG table 'orders' in namespace 'Analytics.Events'`, followed by the catalog's error |
| A catalog that does not hold the table, then fails the namespace lookup | `failed to look up ICEBERG namespace 'Analytics.Events'`, followed by the catalog's error |

To move an emitter to a table in another namespace, `ALTER ICEBERG TABLE EMITTER ... SET TO`
restates the complete destination with the new `NAMESPACE` and that table's `LOCATION`, as
[Altering emitters](#altering-emitters) describes for every entity. The replaced emitter opens the
table in the new namespace, which must already exist, and `SHOW CREATE EMITTER` and
`DESCRIBE EMITTER` show the new namespace.

#### Iceberg table schema

At open, the emitter loads the table through the catalog and requires the table's registered
location to equal `LOCATION`. It then matches the mapped columns to the table's current schema by
name, so `VALUES` may list them in any order:

- Every mapped column must exist in the table, and its Iceberg type must equal the table column's
  at every depth: the same primitive type, and for a `VEC` or `ARRAY` value an Iceberg list of the
  same element type.
- A table column `VALUES` leaves out must be optional, and every row writes a null into it.
- A required table column must be mapped from an expression that never produces a null. A field
  declared `OPTIONAL`, `nullif(...)` and `TRY_CAST(...)` can produce one, so they are refused for a
  required column even when no row carries a null; `coalesce(...)` with an argument that is never
  null maps a nullable value into a required column.
- A table column whose nested values are required, such as a list whose elements are required, must
  be mapped from an expression whose nested values cannot be null either.

A table that breaks one of these rules fails the open before any row is staged. `DESCRIBE EMITTER`
shows the failure as the emitter's transient error, naming the column, and the emitter retries the
open on its declared retry policy:

| Failure | Transient error |
| --- | --- |
| A mapped column the table lacks | `mapped column 'user_id' does not exist in the Iceberg table` |
| A mapped column of another type | `mapped column 'user_id' has Iceberg type string, but the table requires long` |
| A required column `VALUES` leaves out | `table column 'user_id' is required, but VALUES does not map it` |
| A required column mapped from an expression that can produce a null | `table column 'user_id' is required, but the VALUES expression mapped into it can produce a null` |
| A column with required nested values mapped from an expression whose nested values can be null | `table column 'tags' requires every value nested in it, but the VALUES expression mapped into it can produce a null one` |

The comparison uses the types the emitter writes. An Iceberg `timestamptz` column stores
microseconds, so Nervix narrows every UTC `DATETIME` from nanoseconds to the microsecond that
contains it, at every depth of a mapped `VEC` or `ARRAY` value such as `VEC<VEC<DATETIME>>`,
including values before the Unix epoch. A `DATETIME` therefore maps to a `timestamptz` column, at
the top level or as a list element, and a `timestamptz_ns` column is refused at open. An integer
that Iceberg holds in a wider integer keeps its value: `I8`, `U8`, `I16` and `U16` write an `int`
column and `U32` writes a `long` column, while a `U64` has no Iceberg type and is refused at open.
An `ARRAY` value writes an Iceberg list of its elements, and `BYTES` writes a `binary` column.

Every commit matches the mapped columns against the table's schema as that commit finds it, by the
same rules, and writes each mapped column into the table column of its name. An optional column
added to the table after open is written as null. A change after open that breaks one of the rules,
such as dropping, renaming or retyping a mapped column or adding a required column `VALUES` does not
map, fails the commit with the transient error that names the column, as it would fail an open.
That commit enters the retry path that [Iceberg retries](#iceberg-retries) describes and keeps its
staged rows and their acknowledgements until the table can take them again or the emitter is
altered.

#### Iceberg staging and commits

Iceberg uses two explicit boundaries. `FLUSH` collects typed in-memory batches and writes them to
local Arrow IPC files under the runtime temporary-file root. `COMMIT EACH <duration> MAX SIZE
<bytes>` reads the staged Arrow IPC files back, writes their rows into Parquet data files, and
appends every one of those data files to the Iceberg table in one catalog commit. The commit
duration starts when a flush stages rows while none are staged, and rows later flushes stage join
that commit without moving it, so a row waits at most `FLUSH EACH` in memory and then at most
`COMMIT EACH` after it is staged. Staged files that reach `MAX SIZE` are committed at once. Both
durations are domain-logical, so a paced domain's `TIME RATE` moves the flush and commit boundaries
together; the maximum batch and commit sizes, the commit retry backoff, and a drain remain
independent of domain pace. The temporary-file root defaults to `/tmp` and can be changed with
`--temp-dir` or `NERVIX_TEMP_DIR`.

With `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`, the emitter measures the complete Parquet encoding
of each candidate file, including its footer, against the table schema it has loaded. It stages
accepted rows in segments of at most `n` rows and the declared encoded file bytes, then writes
each segment as one data file. A single row whose own Parquet file exceeds `MAX SIZE` is rejected
with a `validation` error of the `encode` operation through `ON MESSAGE ERROR`; rows around it
continue to staging. The file's actual size and row count are checked before the catalog append.
If the table schema changes between staging and commit so that a staged segment no longer fits,
the commit fails and retains its staged rows for retry rather than appending an over-limit file.
Without `BATCH`, the Parquet writer uses its 512 MiB rollover target; `DESCRIBE EMITTER` prints
`data file target: 512MiB` alongside `batch: none`. The commit's `MAX SIZE` continues to count
the sum of local staged Arrow IPC file bytes, independently of each Parquet file's bound.

The emitter completion point for `MODE ACK` is the successful catalog commit. Local staging is not an
ACK boundary. Record-level expression and construction errors, and a row too large for its own
Parquet file, are attributed through `ON MESSAGE ERROR` before staging. See [ACK Semantics And Effective
Delivery](#ack-semantics-and-effective-delivery) for attachment and fan-out behavior.

A commit holds the rows it reads back in memory until it has written them into data files, so it
charges them to the node's bulk memory class, which holds 32 MiB. It reads the staged files in
groups, each the run of consecutive files the class can hold together. Before it reads a group it
charges every byte of the group's files, and once their rows are in data files it returns the
charge before it reads the next group; the data files of every group are appended in the one
catalog commit. It takes a charge only when the class has room for it now rather than waiting in
the class's queue, where a commit's charge would take every byte the class frees and hold the
node's other bulk work behind it. When the class has no room, the commit keeps its staged files and
their acknowledgements, `DESCRIBE EMITTER` shows `Iceberg commit waits for room in the bulk memory
class for <bytes> staged bytes` as the emitter's transient error, and the emitter retries the
commit on its declared retry policy. A single staged file holds one input carrier's mapped rows,
or one bounded segment of them when `BATCH` is declared. Without `BATCH` it can be larger than the
class when a route on the same node flushes batches above 32 MiB.
Such a file is read alone once the whole class is free, charged with the whole class: that is every
byte of it the class can account for, and it leaves no room for other bulk work beside the read.
Until then `DESCRIBE EMITTER` shows `Iceberg commit waits for the whole bulk memory class of
<capacity> bytes for a staged file of <bytes> bytes`, and the emitter retries the commit as before.

#### Iceberg data files

Every commit writes Parquet data files, whatever the table's `write.format.default` property says,
with the Parquet library's default writer properties: uncompressed pages, dictionary encoding,
page-level statistics and row groups of at most 1,048,576 rows. The table's `write.parquet.*`
properties do not apply. Without `BATCH`, a commit writes each group of staged files it reads back
together into data files of their own, rolling to a new file once the current one reaches 512 MiB
whatever the table's `write.target-file-size-bytes` property says. With `BATCH`, each staged
segment becomes one data file, as [Iceberg staging and commits](#iceberg-staging-and-commits)
describes.

Data files go to the table's `write.data.path` property, to its `write.folder-storage.path`
property when that is unset, and to `<table location>/data` when neither is set. Each is named
`<emitter>-<table>-<opened>-<random>-<writer>-<count>.parquet`: the emitter's and the table's
names, the Unix time in nanoseconds at which the emitter opened, a random 64-bit number drawn at
that open, the sequence number of the data-file writer that produced the file, which grows from 1
with every writer the emitter starts, and the five-digit count of the files that writer rolled,
from `00000`. Each opening of the emitter therefore names its files apart from every other's. A
commit appends its data files in one fast-append snapshot whose summary carries
`nervix.emitter.append-id`, the identity a retry looks for, as
[Iceberg retries](#iceberg-retries) describes.

The emitter writes nothing else:

- **Partitions.** Data files carry no partition values and name the table's first partition spec,
  so the emitter appends only to a table whose current spec is that first spec, without a field.
  A table whose current spec has a field opens and stages rows, but every commit fails with
  `failed to commit Iceberg staged batches: DataInvalid => Partition value is not compatible with
  partition type` and is retried on the declared retry policy, keeping its staged rows and their
  acknowledgements. A table whose spec was later changed back to an unpartitioned one fails every
  commit too.
- **Schema changes.** The emitter never changes the table's schema; provision every column it
  maps, as [Iceberg table schema](#iceberg-table-schema) describes.
- **Deletes and upserts.** A commit only appends data files. The emitter writes no equality or
  position deletes, replaces no rows and overwrites no files.
- **Other catalogs.** The emitter commits through an Iceberg REST catalog, its `ICEBERG_REST`
  client, and through no other catalog implementation.

A commit writes its data files to object storage before it appends them, and a retry reuses the
files an earlier attempt finished writing for a whole group of staged files. Files no snapshot comes
to reference stay in object storage until an orphan-file cleanup of the table removes them: those
of a group whose writing failed partway, which the retry writes again, and those of a commit that
never succeeds because the emitter ends before a retry does.

#### Iceberg object storage

The storage client's object store reads and writes the table's files in the bucket or container
that `LOCATION` names. Nervix builds it when a commit first writes a data file and keeps it for as
long as the emitter stays open, so a setting only the object store judges, such as a missing S3
region or an Azure account the location does not name, fails each commit rather than the open.

- **S3.** Requests address the bucket as a virtual host, a host name under the `endpoint`, unless
  `path_style_access` is `true`, which puts the bucket in the request path instead; an
  S3-compatible store that serves buckets only as paths needs it, as the example above shows, and
  so does a bucket whose name contains a dot, which no virtual host can name. Without `endpoint`,
  requests go to the AWS endpoint of the region. `client_region` takes precedence over `region`
  when both are set; without either, the node's `AWS_REGION` or `AWS_DEFAULT_REGION` environment
  variable supplies the region, and a commit fails without one. Without `access_key_id` and
  `secret_access_key`, the driver's default credential chain applies: `disable_config_load` stops
  it reading the environment and AWS configuration files, `disable_ec2_metadata` stops it asking
  the EC2 instance metadata service, and `allow_anonymous` set to `true` sends unsigned requests.
  Of the delegated properties, the object store reads `client.assume-role.arn`,
  `client.assume-role.external-id` and `client.assume-role.session-name`, which assume that role
  for its requests, and `s3.sse.type`: `s3` selects SSE-S3, `kms` selects SSE-KMS with
  `s3.sse.key` as its key, and `custom` selects SSE-C with `s3.sse.key` and `s3.sse.md5`.
- **GCS.** `credentials_json` or `token` authenticates the requests; without either, the driver's
  default credentials apply, which `disable_config_load` and `disable_vm_metadata` narrow.
  `no_auth` set to `true` sends unsigned requests and loads no credentials, and `allow_anonymous`
  set to `true` sends unsigned requests. `service_path` replaces the default endpoint.
- **Azure.** Every Azure location is reached through its account's Data Lake endpoint, with its
  container as the file system: `https://<account>.dfs.<suffix>` for `wasbs` and `abfss`, and
  `http://<account>.dfs.<suffix>` for `wasb` and `abfs`. When the client sets `account_name`, the
  location must name that account. `account_key`, `sas_token`, or `tenant_id` with `client_id` and
  `client_secret` authenticate the requests, and `authority_host` replaces the default sign-in
  authority.

Any other delegated property reaches the REST catalog's property map as written, and the object
store ignores it.

#### Iceberg retries

The emitter's declared `RETRY POLICY` is the only retry of a failed open, staging write or commit.
The emitter retries it after the policy's backoff and keeps the acknowledgements of its staged rows
alive meanwhile. The catalog library's own commit retries are disabled, and an object-store call
makes one attempt: OpenDAL has no retry layer, and the object-store HTTP client resends no request.

Set `timeout_ms` in the `S3`, `GCS` or `AZURE_BLOB` client's `CONFIG` to bound each object-store
operation, including DNS, connection, TLS and credential requests. It also bounds each read or
write call of a streaming object. If omitted, metadata and delete operations allow 60 seconds and
reads and writes allow 10 seconds. A call that runs out of its deadline fails the attempt it
belongs to, and with `timeout_ms` set the failure names the key and its bound. Nervix sets no
deadline on REST catalog requests: a request the catalog accepts and never answers waits until its
connection fails or a stop ends the wait. The operator and the initial table's effective storage
settings are retained across table metadata refreshes; changing those settings requires reopening
the emitter.

Commit conflicts, a table change after open that the mapped columns cannot be written into, a
dropped table, and unavailable catalog or object storage are table-level infrastructure failures:
Nervix reports them in runtime and drain status and retries all affected staged records with the
declared policy and backpressure. While a failed
commit waits for its retry, the emitter takes no new input from its relays: a row that reaches a
relay during the backoff waits upstream, unless a stop drains the relays. The rows a flush stages
before the retry are therefore input the emitter's `COLLECT FOR` policy held, which comes due
during the backoff, and the relay input a stop drains. A commit publishes the staged files it found
when it began, so those rows are not in its data files: the retry first appends what the commit
began with and then commits the newer rows in a commit of their own. A retry still publishes
everything staged, and acknowledges only rows a catalog commit holds.

Each commit names its append with an identity it records in the snapshot it adds. A commit whose
catalog answer fails or is lost reloads the table, and the commit and every retry of it look for
that identity in the current snapshot's history before appending, so a retry never appends the
same staged files twice. Rows are still appended again when their source redelivers them: after a
lost upstream ACK, after an attached sibling emitter fails, or after the emitter ends before a
retry learns whether the catalog applied its commit, because it ended without acknowledging the
rows it still held staged. Iceberg appends are not idempotent.

## Codec Behavior On Emission

`ENCODE USING <codec>` follows the emitter it encodes for, because whether a codec applies is a
property of the emitter. Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS and Sentry publish an
encoded payload and require it. ClickHouse, Postgres, MySQL, and MongoDB map columns with `VALUES`
and do not take a codec. OTEL maps signal fields and attributes with `VALUES` and takes no codec.
Iceberg writes typed records and takes none.

JAQ-native codecs can reshape outbound payloads with `ON EMITTING` before writing the selected
format:

```plexus
CREATE IF NOT EXISTS CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
```

That lets the emitter publish a different JSON envelope for each outbound row without changing the declared relay schema.

## ACK Semantics And Effective Delivery

Nervix composes per-hop ACKs. The effective delivery semantics of a source-to-emitter path are the
observable duplicate and loss behavior produced by the source delivery mode, the emitter's
publishing `MODE`, its attachment, and the external service. They are not the ACK mechanics of any
one hop.

Publishing `MODE` selects the emitter completion point at which the emitter considers a record
delivered. Attachment determines whether that outcome participates in the upstream ACK chain:

- `ATTACHED`: emitter success or failure at the selected emitter completion point stays part of the
  upstream ACK chain.
- `DETACHED`: relay fan-out acknowledges upstream immediately. The emitter still waits for its
  declared confirmations, applies its retry policy, error-routes record failures, and exerts local
  backpressure, but that outcome cannot delay, retry, or fail the source ACK.

Confirming broker modes and request/response `ACK` modes are at least once. A confirmation timeout
or lost response is not proof that the service rejected a record, so retry can duplicate it. The
parallel window limits how many records are exposed to that ambiguity at one time, and Nervix
resends only records not yet confirmed or definitively rejected. With the
[batching clause](#batching) the unit is the batch payload: a retry resends exactly the payloads not
yet confirmed or rejected, with the bytes and members they were first written with. `NO ACK`, MQTT QoS 0, Core NATS,
Redis Pub/Sub, and ZeroMQ expose earlier acceptance boundaries and can lose acknowledged records
after a crash or downstream failure. A stop sends what a Kafka producer still queues, waits for
accepted Pulsar messages' receipts, flushes what a Core NATS client still holds after a write ran
out of `timeout_ms`, and waits for an MQTT QoS 0 client to send the publishes it still holds, within
the stop's deadline; see [Finishing an emitter](./connector-contract.md#finishing-an-emitter). External
broker durability and
idempotence settings remain the user's client and service configuration.

When one source record reaches multiple emitters or multiple attached routes, the upstream ACK
completes only after every attached emitter reaches its emitter completion point. A failure on any
attached path reopens source retry for the record on all paths. A emitter that already published
successfully may therefore receive the record again because a sibling emitter failed. This applies to
every emitter without idempotent writes. Iceberg is the canonical case: rows can be appended to the
table again after a sibling emitter fails.

This sibling-retry case assumes a source mode that retries when an attached ACK fails or is lost. A
no-ACK source cannot create that retry duplicate, but it can lose the record instead.

A WASM processor on the path adds one more retry of the same kind. It dispatches a guest callback's
output before the checkpoint of the guest's state completes and holds back only the source ACK, so
a checkpoint that fails negatively acknowledges records whose output an emitter may already have
published, and the source redelivers them. See
[Recovery, Replay And Duplicates](wasm-processor-guests.md#recovery-replay-and-duplicates).

Every `DETACHED` path has a common loss window: a process can fail after relay fan-out acknowledges
upstream but before the emitter reaches its declared emitter completion point. The table below calls
out the additional mode- and transport-specific duplicate and loss conditions.

| Emitter | Duplicate conditions (`ATTACHED`) | Additional loss conditions | Idempotency available in Nervix |
| --- | --- | --- | --- |
| Kafka | `ACK` retry after an ambiguous delivery report or timeout; either mode after a lost upstream ACK or attached sibling failure | `NO ACK` can lose a record after local producer-queue admission; broker durability follows Kafka client and topic configuration | None; Kafka producer idempotence is pass-through client configuration |
| Pulsar | `ACK` retry after an ambiguous broker receipt; either mode after a lost upstream ACK or attached sibling failure | `NO ACK` does not expose broker failures after producer acceptance; retention and durability remain broker policy | None |
| NATS | JetStream retry after an ambiguous `PubAck`; Core `NO ACK` retry after a timed-out write or a connection change, including records the server may already have received; either mode after a lost upstream ACK or attached sibling failure | Core NATS `NO ACK` confirms a local drain and flush on the write's connection, without durable stream acknowledgement; a connection failure after that completion can lose what the server has not read | None |
| RabbitMQ | Confirming `ACK` retry after a nack, timeout, or lost confirm; `NO ACK` retry after its channel or connection is lost before a write's round trip completes; either mode after a lost upstream ACK or attached sibling failure | Without `MANDATORY`, a message no binding routes is dropped by the broker and its record reported delivered; `NO ACK` can lose a record after the broker's channel has taken it; queue durability and message persistence remain broker policy | None |
| SQS | Retry after an ambiguous `SendMessage` result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; SQS retains its own at-least-once behavior | None |
| MQTT | QoS 1 or 2 retry after an ambiguous handshake; any mode after a lost upstream ACK or attached sibling failure | QoS 0 can lose a record after client acceptance; later delivery follows the configured broker and session guarantees | None |
| Redis Pub/Sub | Retry after Redis accepts `PUBLISH` but the Nervix ACK is lost, or after attached sibling failure | Any failure after detached relay acceptance; subscribers that are absent or disconnected miss the message | None |
| ZeroMQ | Retry after socket send acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; socket send does not establish durable receiver storage | None |
| Sentry | Retry after an ambiguous HTTP result, which sends the same envelope and `event_id` again; lost ACK; or attached sibling failure | Any failure after detached relay acceptance; an accepted event can still be subject to Sentry service policy | None |
| OTEL | Retry after an ambiguous Export result, which resends the same request bytes; lost ACK; or attached sibling failure | Any failure after detached relay acceptance; `partial_success` acknowledges the whole request, so receiver-rejected records in that response are lost | None |
| HTTP | Retry after a lost response, failed connection, or timeout following an endpoint that applied the request; a lost ACK or attached sibling failure | Any failure after detached relay acceptance; a `2xx` delivers on its headers, so an endpoint that later fails its own processing loses the record | None; the endpoint can deduplicate by a stable key the emitter writes with `write_header` |
| ClickHouse | Retry after an ambiguous insert result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a crash after insert but before acknowledgement can also leave an inserted batch that later retries | None |
| Postgres | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MySQL | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MongoDB | Retry after a write the server never answered, lost ACK, or attached sibling failure; a failed write concern adds none, since its retry carries only the documents the write left unresolved | Any failure after detached relay acceptance; a committed write can survive a crash before Nervix observes success; a document the server applied under a write concern it could not satisfy is delivered, and lost if the deployment later rolls the write back | `ON CONFLICT` |
| Iceberg | Redelivery after a lost ACK, an attached sibling failure, or an emitter that ended before a retry learned whether the catalog applied its commit; appends repeat rows | Crash before catalog commit loses staged work unless the attached source redelivers; detached mode accepts that loss | None; appends are not idempotent |

The [publishing-mode table](#publishing-modes) names each transport's exact completion point.
`ATTACHED` waits only for that declared point and cannot make an earlier `NO ACK` boundary durable.
Kafka and Pulsar `NO ACK`, MQTT QoS 0, Core NATS, Redis Pub/Sub, and ZeroMQ can still lose a
message after Nervix observes client-side acceptance, and RabbitMQ `NO ACK` after the broker's
channel has taken it. `DETACHED` cannot turn a confirming mode into fire-and-forget inside the
emitter; it changes only whether the result participates upstream.

Emit a stable idempotency key at ingestion, for example with `uuid_v7()`, and carry it through the
graph. Downstream consumers and queries can use that key to suppress retries within that admitted
record's fan-out. A source-provided identifier is stronger because it also survives a fresh source
redelivery. Generate the key once; regenerating it on a downstream route defeats the purpose.

For Postgres, MySQL, and MongoDB, use `ON CONFLICT` against a stable key. This preserves
at-least-once delivery attempts while making the resulting table or collection state
effectively-once for that conflict contract.

Iceberg appends are not idempotent. Deduplicate by the stable key at query time or in downstream
compaction or `MERGE` work. Nervix does not isolate sibling-emitter retries by changing ACK mechanics.
That is an [accepted tradeoff](#accepted-tradeoff-shared-retry).

### Accepted Tradeoff: Shared Retry

One input ACK represents all attached descendants. This keeps ACK composition small and preserves
backpressure across the graph. It also means one attached failure retries successful siblings.
Nervix accepts that coupling instead of maintaining a transactional per-emitter commit ledger.

### Graph Design

When the same records feed a non-idempotent emitter and other emitters, consider `DETACHED` mode for a
non-critical path or separate relays with separate ACK boundaries per emitter so one emitter's failure
does not drive duplicates into another. `DETACHED` makes that path at-most-once relative to the
upstream ACK: the emitter still confirms and retries according to `MODE`, but a crash after the
detached ACK can lose its in-memory work without causing source redelivery.

See [Data Plane](data-plane.md#ack-composition) for the relay fan-out mechanics and
[What It Is Not](what-it-is-not.md) for the persistence boundary.
