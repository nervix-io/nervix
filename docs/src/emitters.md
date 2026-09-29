# Emitters

Emitters publish relay records to external systems or to application consumers through a client
session.

A typical emitter:

```nspl
CREATE IF NOT EXISTS EMITTER kafka_notifications
  FROM notifications
  COLLECT FOR 10ms MAX BATCH SIZE 1MiB
  TO KAFKA kafka_main TOPIC notifications_out
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
- the transport-specific sink
- the sink's explicit publishing mode, confirmation window and bound where applicable, and retry
  pacing
- an optional batching clause bounding how many records one external write carries and its encoded
  size, required for database sinks
- the flush policy used to collect a batch before publishing
- whether the branch is `ATTACHED` or `DETACHED`
- route-local codec construction or a direct `VALUES` mapping
- optional ordered header invocations on supported codec sinks
- optional ordered materialized-state dependencies

## Branch Semantics

An emitter is the terminal consumer for its source relays. The `FROM` list uses the same
source-local predicate form as other relay-consuming nodes:

```nspl
CREATE EMITTER combined_notifications
  FROM primary_notifications WHERE input.source = 'primary',
       replayed_notifications WHERE input.source = 'replay'
  TO KAFKA kafka_main TOPIC notifications_out
    MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
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
means Nervix collects an in-memory Arrow batch before handing it to the external sink. `FLUSH EACH`
is a domain-logical duration, so a paced domain's `TIME RATE` accelerates or slows it; retry
backoff, acknowledgement keepalive, and sink acknowledgement timeouts stay on the physical clock.
See [Domains And Time](domains-and-time.md) for the full clock split. The
[NSPL Overview](nspl-overview.md) defines the `FLUSH IMMEDIATE` 100 µs minimum batching window.
For most emitters the collected batch is encoded and published on the flush boundary. Iceberg
additionally requires `COMMIT EACH <duration> MAX SIZE <bytes>` as part of its sink clause: flush
writes local Arrow IPC staging files, and commit appends the staged data to object storage. `ON MESSAGE ERROR SEND TO`
buffers failed-message error records separately and delivers them using the emitter's same `FLUSH`
interval or maximum batch-size boundary. Which sinks gain from larger flush batches, and which
publish per record regardless of batch size, is covered by the
[flush tuning guidance](nspl-overview.md).

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

Every emitter sink requires `MODE <body>` before its body selection, immediately before
`ENCODE USING` when the sink uses a codec. There is no implicit mode, confirmation window, ACK
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

Request/response sinks—SQS, Sentry, OTEL, HTTP, the databases, and Iceberg—do not take
`ACK TIMEOUT`; their client request timeout bounds the response. SQS, Sentry, OTEL, and ClickHouse
clients expose that bound as the optional `timeout_ms` CONFIG key, and a client an HTTP emitter
uses must declare it. For every sink, Nervix accounts for records individually
wherever the transport exposes individual results: delivered records acknowledge upstream,
definitively invalid records follow `ON MESSAGE ERROR`, and a retry resends only records that are
neither delivered nor rejected. An ambiguous or infrastructure-wide failure is never used to
discard a record.

| Sink | Mode forms | Publish success boundary |
| --- | --- | --- |
| Kafka | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Local producer-queue acceptance for `NO_ACK`; one delivery report per record for `ACK` |
| Pulsar | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | Producer acceptance for `NO_ACK`; one broker receipt per record for `ACK` |
| RabbitMQ | `NO_ACK`; `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | The channel's answer to one round trip after each write for `NO_ACK`; publisher confirm for `ACK` |
| MQTT | `QOS 0`; `QOS 1 ACK ...`; `QOS 2 ACK ...` | Client acceptance, `PUBACK`, or completion of the QoS 2 handshake respectively |
| NATS | `NO_ACK`; `JETSTREAM ACK SEQUENTIAL`; `JETSTREAM ACK PARALLEL MAX <n>` | Core-NATS connection flush for `NO_ACK`; JetStream `PubAck` otherwise |
| Redis Pub/Sub | `NO_ACK` | Server acceptance of `PUBLISH`; the subscriber count is not a delivery guarantee |
| ZeroMQ | `NO_ACK` | Socket acceptance |
| SQS | `SINGLE`; `BATCH` | Successful per-record or per-entry service response |
| Sentry | `ACK` | Successful one-event envelope response |
| OTEL | `ACK` | Successful OTLP Export response; `partial_success` is acknowledged with a warning |
| ClickHouse, Postgres, MySQL, MongoDB | `ACK` | Successful insert/write result |
| Iceberg | `ACK` | Successful catalog commit |
| HTTP | `ACK` | Complete successful response headers |
| Client | `ACK SEQUENTIAL`; `ACK PARALLEL MAX <n>` | A current consumer attempt's explicit application ACK |

### Client emitters

`TO CLIENT SCHEMA <output_schema>` constructs native Arrow output for applications. It has no
connector `CLIENT` object, codec, header operations, or direct `VALUES` form. The emitter still
uses its ordinary `FROM` predicates, optional collection, materialized dependencies, ordered
`INHERIT` and `SET`, route `WHERE`, error policies, placement, and `ATTACHED` or `DETACHED`
boundary. A client sink requires `MODE ACK SEQUENTIAL` or `MODE ACK PARALLEL MAX <n>`, an explicit
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

An HTTP emitter uses an existing `TYPE HTTP` client and declares the request method, path and body
selection in this order:

```nspl,ignore
TO HTTP <client>
  METHOD <string_expression>
  PATH <string_expression>
  MODE ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  (ENCODE USING <codec> | WITHOUT BODY)
```

`METHOD` and `PATH` are structured expressions. They may read qualified source fields such as
`input.method` and `input.path`; parentheses, arrays and string literals keep clause words inside
the expression. `MODE ACK` has no acknowledgement window or `ACK TIMEOUT`, and `NO_ACK` is not
available. The ordinary route-local `FLUSH` clause is required.

The referenced client must be a `TYPE HTTP` client in the same domain. Its `endpoint` is an
`http://` or `https://` origin with a host and optional port, and no credentials, non-root path,
query or fragment. It requires a positive, schedulable `timeout_ms`; the client's polling `method`
setting does not supply the emitter's `METHOD`. HTTPS retains certificate and hostname verification.
The optional `tls_cert_file` and `tls_key_file` settings must be supplied together, and mounted TLS
files use the client's pinned resource version. Validation does not probe the destination.

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
value. Transport-owned headers cannot be written. After case-insensitive replacement, at most 128
application headers and 32 KiB of name/value bytes are allowed.

`ENCODE USING` permits the ordinary transforming construction clauses. `WITHOUT BODY` selects an
absent request body and permits `WHERE` and `INVOKE` but no `INHERIT`, `SET` or `VALUES`. HTTP
emitters publish one request per eligible source record, so they do not accept the optional
`BATCH` clause. `SHOW CREATE EMITTER` preserves both request expressions and the explicit body
selection.

For example, the first emitter below sends each record of `outgoing` with its own method, path and
tenant header and a JSON body of two of its fields, and the second deletes without a body.
`api.example.com` stands for an endpoint the operator has already provisioned.

```nspl
CREATE CLIENT api TYPE HTTP CONFIG {
  'endpoint' = 'https://api.example.com',
  'timeout_ms' = 5000
};

CREATE ATTACHED EMITTER deliver_event
  FROM outgoing
  TO HTTP api
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

CREATE EMITTER delete_event
  FROM outgoing WHERE input.request_method = 'DELETE'
  TO HTTP api
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
earlier value; an empty string is sent as an empty value. An invalid or reserved write rejects its
record even when a later write would replace it, and the 128-header and 32 KiB bounds apply after
every replacement. The body is exactly the bytes the codec produced, with no wrapper, array,
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

The HTTP sink speaks HTTP/1.1. It creates a connection for each attempt and closes it after final
headers, so an unread or stalled body cannot be reused as the next response. DNS resolution,
connection acquisition, TLS negotiation, the complete request send, interim responses and
complete final headers share the client's physical `timeout_ms`; queueing behind an earlier
request and host retry backoff do not consume the next attempt's timeout. HTTPS validates trust and
the destination hostname and uses the client's pinned CA and optional client certificate mounts.
Starting the sink reads local configuration and sends no probe.

The transport writes `Host`, `Connection: close`, and `Content-Length` when a body exists. It adds
`Accept: */*` only when the application did not write `Accept`. It adds no `Accept-Encoding` or
`Content-Type`. The application may write `Accept`, `Content-Type`, `Authorization`, and `Cookie`
with `write_header`. Response cookies are
not retained; an authentication challenge sends no additional request. There is no redirect or
library retry: every repeat is a separate emitter attempt under its declared policy.

Each interim and final response header block may contain at most 128 fields and 64 KiB of field
name and value bytes. Malformed headers or final framing and any exceeded bound fail the attempt,
even when the final status line says `200`. Complete valid final `200`–`299` headers deliver the
record, including `202` and `204`; a stalled or failed body after those headers does not reverse
delivery. The sink never waits for a body or uses bodies and trailers as graph data.

`408`, `425`, `429`, and `500`–`599` retain the current request and all later work for host retry.
`401`, `403`, and `407` do the same and report an authentication or authorization infrastructure
failure. DNS, connection, TLS, send, timeout, malformed response and loss before complete final
headers are also infrastructure failures. Other `300`–`499` statuses and `101` reject only their
record through `ON MESSAGE ERROR`; later records proceed after that policy completes. Redirects
are never followed, `304` is not delivery, and `409` never implies an earlier delivery. Rejection
diagnostics include the numeric status but no evaluated URL, header value, request body or response
body.

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

`ALTER EMITTER ... SET TO HTTP` restates the complete method, path, mode and body selection.
`SET CLIENT` changes the referenced client, `SET MODE` changes the retry policy, and `SET ENCODE
USING` selects a codec body. `DROP ENCODE` is invalid for HTTP; use a complete `SET TO HTTP ...
WITHOUT BODY` replacement. The retained construction and error scopes must be valid for the new
selection.

Changing the HTTP sink, method, path, client reference, body selection, or publishing mode uses
`ENTITY_PAUSE`. The emitter drains every admitted request with the client and request fields that
prepared it before the replacement starts. If that drain fails, the change fails and the old
definition remains active; an unanswered request is never sent to the proposed destination.
Changing only `FLUSH` is `DYNAMIC`, while adding or removing a source relay uses `DOMAIN_PAUSE`.
Changing the `CLIENT` definition itself follows the domain-pause configuration lifecycle.

`ALTER` retains the emitter's `INHERIT`, `SET`, `INVOKE`, and error clauses, so a body-mode change
must remain valid with all of them. To change construction or header invocations, use `DROP
EMITTER` and `CREATE EMITTER` for the same name in one transaction. The complete candidate is
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
invocations and the construction in canonical NSPL, with the explicit body selection. A configured
expression is rendered as NSPL under the ordinary sensitivity rules, so an explicitly leaked field
appears as its `leak_sensitive(...)` call and never as a value. `DESCRIBE EMITTER`
reports the request contract of the `deliver_event` example above in these lines:

```text
codec: event_body_codec
body: codec
sink: HTTP client=api method=input.request_method path=input.request_path
batch: none
flush: FLUSH EACH 100ms MAX BATCH SIZE 1MiB
publishing mode: ACK RETRY POLICY BACKOFF 250ms MAX 30s
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

An emitter may declare two hard limits for every batch it publishes, written after the complete sink
clause and its route construction, and before the flush policy:

```nspl,ignore
BATCH MAX MESSAGES <n> MAX SIZE <bytes>
```

`MAX MESSAGES` is the most source records one batch carries, from 1 to 65,536. `MAX SIZE` is the
most bytes its encoded payload may occupy: a positive whole number followed by `B`, `KB`, `KiB`,
`MB`, `MiB`, `GB`, `GiB`, `TB` or `TiB`. Neither limit has a default, neither may be omitted, and
neither is derived from the other or from `FLUSH ... MAX BATCH SIZE`, which measures Arrow memory
rather than encoded bytes.

```nspl,ignore
CREATE EMITTER kafka_notifications
  FROM notifications
  TO KAFKA kafka_main TOPIC notifications_out
    MODE ACK PARALLEL MAX 100 ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  BATCH MAX MESSAGES 500 MAX SIZE 1MiB
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

| Sinks | Clause |
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

`SHOW CREATE EMITTER` renders the clause between the sink clause and `FLUSH`, with the size in the
unit it was written in. `DESCRIBE EMITTER` reports it on the line after `sink:`, as
`batch: MAX MESSAGES 500 MAX SIZE 1MiB` or `batch: none`.

The complete contract for batch payloads — packing, containers per wire format, exact size
measurement and failure attribution for every sink — is defined in
[Optional emitter batching](https://github.com/nervix-io/nervix/blob/main/docs/specifications/emitter-batching.md).
The database sinks bound every insert or bulk write by both limits, as
[Database writes](#database-writes) describes. An OTEL emitter with the clause bounds each export
request; without it, OTEL keeps one request per pending Arrow batch. Iceberg keeps its data-file
and commit boundaries.

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
named and concrete branch, key, ordered written headers, ordering group and, with a `SYSLOG` codec,
syslog header other than the timestamp agree. A different value seals the open payload before the
next row is considered; no row is skipped over or reordered. A carrier retains its own execution
snapshot and row membership for errors and acknowledgements. A payload with one member keeps the
container shape, such as a one-element array, and a carrier with no eligible row publishes nothing.
Batching adds no timer: `FLUSH` alone decides when records leave, and a partial payload is published
exactly like a full one.

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
run in the same job on Nervix's blocking worker pool that already runs `ON EMITTING`, so a slow
program never stalls the emitter task. Candidate work is bounded by `MAX MESSAGES` and the
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

Batching keeps every publishing mode's completion point. `NO_ACK` and `QOS 0` complete when the
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

| Sink | Limit checked before a message is written | Counted around the payload |
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
`pulsar rejected record: the Pulsar broker does not allow the message: Exceed maximum message
size`. In `MODE NO_ACK` the message was already delivered when the producer accepted it, so the
refusal is not observed.

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
require the clause. A flush hands the sink every run of rows from successive Arrow carriers of one
source relay and concrete branch, in the order the emitter would have published them, and the sink
writes the run as inserts or bulk writes of at most `MAX MESSAGES` rows. Every source record stays
one row or one document: an array or `VEC` value is one column value, never a set of rows, and no
destination schema changes.

| Sink | One write | `MAX SIZE` measures |
| --- | --- | --- |
| ClickHouse | One `INSERT INTO <table> FORMAT JSONEachRow` request | The request body, one JSON line and newline per row, before the client compresses it |
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
bytes, and a write's measured size is never smaller than the message that carries its arrays, so a
Postgres write is kept within that size too; a row whose own insert exceeds it follows
`ON MESSAGE ERROR` as an `external` `publish` error. A MongoDB document holds at most 16 MiB, so a
row whose document exceeds it follows `ON MESSAGE ERROR` as an `external` `publish` error naming
16777216 bytes before any write, and the rows around it are written. MongoDB takes 100,000 writes
in one command, more than `MAX MESSAGES` allows, and its driver divides a write larger than the
`maxMessageSizeBytes` the server reports into several commands whose documents it still answers
for one by one.

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
retry carries only the documents it left unresolved.

Each sink writes a `BYTES` value in its native binary form: the octets themselves in a ClickHouse
`String`, the `bytea` hex format Postgres reads back as the same octets, the octets bound to a MySQL
binary or blob column, and generic BSON binary data in MongoDB. An `ARRAY` or `VEC` value is a JSON
array in ClickHouse's `JSONEachRow` and in the JSON text Postgres and MySQL bind for it, and a BSON
array in MongoDB; inside that JSON text a `BYTES` element is the padded base64 text every Nervix JSON
value carries octets as.

## Altering emitters

`ALTER EMITTER` applies one or more comma-separated operations in written order:

```nspl,ignore
ALTER EMITTER <emitter>
    ADD FROM <relay> [WHERE <expr>]
  | DROP FROM <relay>
  | ALTER FROM <relay> SET WHERE <expr>
  | ALTER FROM <relay> DROP WHERE
  | SET TO <full sink clause>
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

`SET TO` accepts the same complete transport-specific sink body that follows `TO` in `CREATE
EMITTER`, including its required `MODE`, SQS FIFO group, and Iceberg commit policy. The existing
construction, batching clause and output flush policy remain in place, so changing to a database
sink requires the emitter to declare a batching clause, in the same statement if necessary. `SET MODE` changes only
the current sink's publishing mode and rejects a body that the sink does not support. `SET CLIENT`
changes only the client of the current sink kind. `SET COMMIT` is valid only for Iceberg. `DROP
ENCODE` fails if the emitter has no codec configured. `SET BATCH` adds or replaces the batching
clause. `DROP BATCH` fails when the emitter has no clause and when its sink requires one.

`ADD FROM` rejects an already configured relay. `DROP FROM` cannot remove the final input.
`ALTER FROM ... SET WHERE` adds or replaces that source's predicate; `ALTER FROM ... DROP WHERE`
fails when the source has no predicate.

Changing only `FLUSH` is a `DYNAMIC` update. The live emitter keeps its pending Arrow batches,
installs the new cadence, and receives a force-flush kick, so buffered output is neither discarded
nor re-encoded. Source-predicate, sink, publishing-mode, client, codec, collection, batching, and
attachment changes use
`ENTITY_PAUSE`: Nervix gates all of the emitter's source relays, drains collected input and pending
sink output, replaces that emitter task, and releases the gates. Changing source membership uses
`DOMAIN_PAUSE` because it changes graph topology. Other relays continue flowing during an entity
pause; sibling consumers of a gated source may see bounded backpressure until the gate is released.
The complete candidate graph is validated before any change is committed.

## Codec-emitter construction

Codec emitters are transforming routes. They begin with an empty codec-schema payload and use
explicit inheritance and ordered assignment:

```nspl
CREATE IF NOT EXISTS EMITTER kafka_notifications
  FROM notifications
  TO KAFKA kafka_main TOPIC notifications_out
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

```nspl,ignore
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
`INHERIT` and all current direct sinks reject `INVOKE`.

## Header invocations

`write_header` is a side-effect function. It accepts statically non-null `STRING` name and value
expressions and is valid only as a top-level `INVOKE` call. Sensitive values require
`leak_sensitive`. Calls execute left to right after payload finalization and route filtering. Header
mutations are staged in a temporary route-local envelope; invocation failure prevents payload and
partial-envelope publication.

Header output is supported only on codec emitters for Kafka, NATS, Pulsar, RabbitMQ, and SQS.
Kafka and NATS preserve ordered repeated values. Pulsar, RabbitMQ, and SQS use last-write-wins
behavior. Redis, MQTT, Syslog, ZeroMQ, Sentry, OTEL, direct database sinks, and Iceberg reject
header writes.

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

## TLS Client Configuration

Emitter TLS is configured on the referenced `CLIENT` exactly the same way as ingestor TLS.

Common pattern:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <kind>
  MOUNT <tls_resource> VERSION <n>|LATEST
  CONFIG {
    ...
    'tls_ca_file' = '{{ tls_resource }}/ca.pem'
  };
```

Transport-specific expectations:

- `KAFKA`: pass-through to librdkafka. Typically set `'security.protocol' = 'ssl'`, `'ssl.ca.location' = '{{ tls_resource }}/ca.pem'`, and optional `'ssl.certificate.location'` plus `'ssl.key.location'`.
- `RABBITMQ`: use `amqps://...` in `addr`; Nervix honors `tls_ca_file`.
- `REDIS`: use `rediss://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `MQTT`: use `mqtts://...` in `addr`; Nervix requires `tls_ca_file` and supports `tls_cert_file` plus `tls_key_file`.
- `NATS`: use `tls://...` in `addr`; Nervix honors `tls_ca_file`, `tls_cert_file`, `tls_key_file`.
- `PULSAR`: use `pulsar+ssl://...` in `addr`; Nervix honors `tls_ca_file` and optional `tls_allow_insecure_connection` plus `tls_hostname_verification_enabled`. Pulsar client certificate authentication is not currently exposed.
- `SQS`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file` and optional `timeout_ms`.
- `SENTRY`: the referenced `TYPE SENTRY` client carries an `https://...` `dsn`; Nervix honors the
  client's `tls_ca_file`, `tls_cert_file`, and `tls_key_file`.
- `OTEL`: use an `https://...` `endpoint`; Nervix honors `tls_ca_file`, `tls_cert_file`, and
  `tls_key_file` for both OTLP/gRPC and OTLP/HTTP-protobuf.
- `CLICKHOUSE`: use an `https://...` `addr`; Nervix honors `tls_ca_file` and optional `timeout_ms`.
- `POSTGRES`: use `sslmode=verify-full` in the `addr` URL; Nervix honors `tls_ca_file`, `tls_cert_file`, and `tls_key_file`. `sslmode=disable` is the only other accepted policy.
- `MYSQL`: include `require_ssl=true` in `addr`; Nervix honors `tls_ca_file`.
- `SYSLOG`: select `'protocol' = 'tls'`. Optional `tls_ca_file` adds a server trust root;
  optional `tls_cert_file` and `tls_key_file` configure client authentication and must appear
  together.

Example Kafka TLS emitter client:

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

## Supported Emitter Sinks

### Kafka

```nspl,ignore
TO KAFKA <client> TOPIC <topic>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` waits for every record's delivery report. Kafka's `acks`, idempotence, batching, linger, and
compression remain pass-through client configuration; Nervix's `ACK TIMEOUT` independently bounds
the report wait. `NO_ACK` is fire-and-forget after local admission: Nervix acknowledges the
emitter's `ATTACHED` ACK share when librdkafka accepts the record into its producer queue and does
not wait for a delivery report. A later broker error, producer timeout, crash, or process exit can
therefore lose an accepted record. A full local queue is an infrastructure condition paced by the
declared retry policy with backpressure. Graceful shutdown drains queued records within the node
drain bound in both modes. A definitive record-specific rejection, such as an oversized message,
follows `ON MESSAGE ERROR`.

### Pulsar

```nspl,ignore
TO PULSAR <client> TOPIC <topic>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` waits for each broker receipt. `NO_ACK` acknowledges producer acceptance and does not expose
later broker errors; its throughput advantage may be smaller than Kafka's because Pulsar already
pipelines producer work.

A record whose message is larger than the `maxMessageSize` the broker announced for the producer's
connection, counting its metadata and properties, follows `ON MESSAGE ERROR` in either mode: the
producer refuses it before writing it, because the broker would close the connection and fail
every message in flight on it. With `ACK`, a message the broker receives and refuses with
`NotAllowedError`, such as one above the topic's own `maxMessageSize` policy, follows
`ON MESSAGE ERROR` too, while any other broker error is retried.

Pulsar emitters use the same client config surface as Pulsar ingestors:

- `'addr'`: broker address such as `'pulsar://127.0.0.1:6650'`
- optional `'namespace'`: defaults short topic names to `persistent://public/default/<topic>`; fully qualified topic names are accepted as-is
- optional `'tls_ca_file'`: PEM-encoded CA bundle for `pulsar+ssl://...` connections
- optional `'tls_allow_insecure_connection'`: `true` or `false`; defaults to `false`
- optional `'tls_hostname_verification_enabled'`: `true` or `false`; defaults to `true`

Pulsar TLS currently supports server trust configuration only. Nervix does not yet expose Pulsar client certificate authentication.

### RabbitMQ

```nspl,ignore
TO RABBITMQ <client> QUEUE <queue>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`ACK` enables publisher confirms and waits for the confirm of each message. A broker nack is an
infrastructure failure and is retried with backpressure. In `NO_ACK` the broker confirms nothing,
so once a write's messages are on the channel, the emitter asks the channel for one round trip,
which the broker answers only after it has taken every message written before it, and that answer
acknowledges them. A `NO_ACK` write therefore waits for one round trip to the broker however many
messages it carries, and a channel or connection lost before the answer leaves the write's messages
to the retry.

The broker's `max_message_size`, 16 MiB by default in RabbitMQ 4.x, bounds each message body;
headers do not count. The broker closes the channel a larger body arrives on, and the emitter
rejects that message through `ON MESSAGE ERROR` in either mode, instead of retrying it or
acknowledging it, then keeps publishing on a new channel of the same connection; see
[Broker and message emitters](#broker-and-message-emitters).

The emitter resolves the host of the client's `addr` through the node's configured DNS resolver
when it opens and whenever it reopens after a failed publish, so a changed DNS answer takes effect
on the next connection, and tries the addresses it receives in order. A literal IPv4 address, or
an IPv6 address in brackets, is connected to as written. Resolution, the TCP connection and, for
`amqps`, the TLS handshake have 30 seconds together; the broker certificate must name the host
`addr` names. A connection that fails, including a name that does not resolve, leaves the emitter
unavailable with the failure as its transient error; it confirms nothing, so its input stays
unacknowledged, and it reopens on its `RETRY POLICY` backoff.

### Redis Pub/Sub

```nspl,ignore
TO REDIS PUBSUB <client> CHANNEL <channel>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
```

Redis Pub/Sub has no subscriber delivery acknowledgment. The awaited `PUBLISH` response confirms
server acceptance only. A record-specific server rejection follows `ON MESSAGE ERROR`; connection
failures retry the undelivered work. A `TYPE REDIS` client declares its connection-pool bounds; see
[Database Client Connection Pools](database-client-pools.md).
Each physical pooled command connection resolves the `addr` hostname through the node's
asynchronous DNS resolver when it opens. A replacement connection can use a changed DNS answer;
an established connection stays open until Redis or the network closes it. For `rediss://`, TLS
still verifies the configured hostname and uses its configured CA and optional client identity.

### MQTT

```nspl,ignore
TO MQTT <client> TOPIC <topic>
  MODE QOS 0 RETRY POLICY BACKOFF <duration> MAX <duration>
     | QOS (1 | 2) ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

QoS 0 acknowledges client acceptance. QoS 1 waits for `PUBACK`; QoS 2 waits for the complete
exactly-once handshake. QoS 1 and 2 use a persistent session and the emitter client's stable
identity so in-flight messages survive reconnects. Reconnect pacing follows the declared retry
policy. A definitive record rejection, such as an invalid topic or payload-format rejection,
follows `ON MESSAGE ERROR`. So does a record whose `PUBLISH` packet would exceed the Maximum Packet
Size the broker declared when the client connected, or the largest packet MQTT can express: the
emitter rejects it before handing it to the client, which would otherwise lose its connection on a
packet the broker refuses to receive.

### NATS

```nspl,ignore
TO NATS <client> SUBJECT <subject>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
     | JETSTREAM ACK (SEQUENTIAL | PARALLEL MAX <n>) ACK TIMEOUT <duration>
         RETRY POLICY BACKOFF <duration> MAX <duration>
```

`NO_ACK` publishes through Core NATS and acknowledges after the connection flush. `JETSTREAM`
waits for one `PubAck` per record. The stream must already exist and capture the subject; a missing
stream is an infrastructure error that remains under backpressure until an operator provisions it.

### ZeroMQ

```nspl,ignore
TO ZEROMQ <client>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
```

ZeroMQ has no delivery acknowledgment; success is socket acceptance. Transient socket failures are
paced by the declared retry policy.

### Syslog

```nspl,ignore
TO SYSLOG <client>
  MODE NO_ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  ENCODE USING <codec>
```

The client sends UDP datagrams, persistent RFC 6587 TCP frames, or RFC 5425 TLS frames. TCP uses
octet-counting by default and may select non-transparent framing; TLS always uses octet counting.
Success is local socket acceptance and flush, not remote delivery confirmation. The sink requires
a codec and rejects header writes. See [Syslog](syslog.md) for client configuration, framing,
message errors, retry behavior, and limits.

With `BATCH`, a `SYSLOG` codec encodes one RFC 5424 frame whose `MSG` is a JSON array of the
members' complete RFC 5424 messages. Members must share every header field except timestamp; the
outer frame uses the first member's timestamp. The same frame travels as one UDP datagram, one
octet-counted TCP/TLS frame, or one non-transparent TCP frame when it contains no LF.

### SQS

```nspl,ignore
TO SQS <client> QUEUE <queue> [FIFO GROUP (FROM BRANCH | <string_expression>)]
  MODE (SINGLE | BATCH) RETRY POLICY BACKOFF <duration> MAX <duration>
```

`SINGLE` issues one request per message. `BATCH` groups messages within SQS's fixed limit of ten
entries and 256 KiB per request; both modes issue requests sequentially and acknowledge service
responses. Per-entry transient failures retry only those entries, while invalid entries and a
message larger than 256 KiB, counting its attributes and FIFO message group, follow
`ON MESSAGE ERROR` individually. Nervix checks what SQS refuses before sending: a body or attribute
value holding a character SQS forbids, more than ten attributes, an attribute name SQS does not
allow, an empty attribute value, and an invalid FIFO message group.

Set the SQS client's optional `timeout_ms` CONFIG key to bound both the complete service operation
and its single SDK attempt. Nervix disables the AWS SDK's internal retries, so a timeout returns to
the emitter and the mode's declared `RETRY POLICY` owns all retry pacing.

The client resolves the host of its `endpoint` through the node's asynchronous resolver each time it
opens a connection, and tries the answers in order; a literal IPv4 or IPv6 address is dialled as
written. Every request is still signed for the configured host, and over HTTPS the service
certificate must name that host, whichever address accepted the connection. The lookup counts
against the SDK's 3.1-second connect timeout and against `timeout_ms`. Without `tls_ca_file` the
client trusts the platform's native roots and follows the `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`
environment variables; with it, the client trusts that CA alone and connects directly. A missing
name, a silent name server or an unreachable answer fails the queue lookup or the send, which the
emitter retries on its `RETRY POLICY`; nothing is acknowledged until SQS answers.

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

Sentry emission uses a `TYPE SENTRY` client whose required `dsn` contains the project endpoint and
public key:

```nspl
CREATE CLIENT sentry_main
  TYPE SENTRY
  CONFIG {
    'dsn' = 'https://<public-key>@sentry.example.com/<project-id>',
    'timeout_ms' = 5000
  };

CREATE EMITTER sentry_errors
  FROM errors
  TO SENTRY sentry_main
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING sentry_event_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The codec must produce one top-level JSON object per record. Its fields use the Sentry event
protocol, such as `message`, `level`, `environment`, `release`, `tags`, `extra`, `user`, and
`exception`. Nervix preserves the complete object and supplies `event_id`, `timestamp`, and
`platform` when they are omitted. It then creates a Sentry envelope, derives the envelope URL and
authentication header from the DSN, and submits the event. An invalid event object is handled as a
route-local encoding error.

Sentry sends one event per envelope and acknowledges the successful HTTP response. On `429` or
`503`, Nervix honors `Retry-After` and `X-Sentry-Rate-Limits`; the server interval extends the
declared retry delay when it is longer.

With `BATCH`, `ON EMITTING BATCH` builds one event from the candidate array, usually placing its
members under `extra`. The envelope still has exactly one `event` item. `MAX SIZE` measures the
transformed event JSON; Nervix also checks the final serialized event, after default fields are
added, against Sentry's 1 MB decompressed event limit before sending the envelope. An oversized
event follows the emitter's message error policy. The envelope header, item header, and newline
framing are outside `MAX SIZE`.

Use a JSON wire codec or a JAQ-native codec with JSON output. Sentry emitters require `ENCODE
USING`, do not accept `write_header`, and still require explicit leakage for sensitive event
fields. The optional Sentry client keys `timeout_ms`, `tls_ca_file`, `tls_cert_file`, and
`tls_key_file` have their usual meanings.
Sentry resolves the DSN endpoint through the node's configured DNS resolver; its HTTP request
timeout includes that lookup, connection setup, TLS, and the response.

### OTEL

OTEL emission is a codec-free direct sink for OTLP logs, traces, and metric data points. It uses a
`TYPE OTEL` client and supports both OTLP/gRPC and OTLP/HTTP-protobuf. The protocol is always
explicit; there is no hidden default:

```nspl
CREATE CLIENT otel_main
  TYPE OTEL
  CONFIG {
    'endpoint' = 'http://127.0.0.1:4317',
    'protocol' = 'grpc',
    'headers' = 'authorization=Bearer <token>',
    'compression' = 'gzip',
    'timeout_ms' = 5000
  };
```

`endpoint` and `protocol` are required. `protocol` is exactly `grpc` or `http/protobuf`.
`headers` is the OTLP comma-separated `key=value` form, `compression` accepts only `gzip`, and an
absent compression key sends an uncompressed request. `timeout_ms` is an optional positive request
bound. For `http/protobuf`, Nervix appends `/v1/logs`, `/v1/traces`, or `/v1/metrics` to the endpoint
path. Mount TLS files and use `tls_ca_file`, `tls_cert_file`, and `tls_key_file` in the same client;
the certificate and key must be supplied together.
OTLP/HTTP-protobuf resolves through the node's configured DNS resolver. OTLP/gRPC continues to
use its gRPC transport resolver. The configured request timeout covers HTTP DNS and connection
setup as well as the response.

One log record is mapped as follows:

```nspl
CREATE EMITTER audit_to_otel
  FROM audit_events
  TO OTEL otel_main LOGS
  VALUES {
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
  SCOPE 'nervix/audit' VERSION '1.0'
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 2s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The clause order is fixed: signal, required `VALUES`, optional `ATTRIBUTES`, optional `RESOURCE`,
optional `SCOPE '<name>' [VERSION '<version>']`, then `MODE`. `RESOURCE` values must be literals or
literal arrays. `ATTRIBUTES` may use exact-typed `STRING`, `BOOL`, integer-family, `F32`, `F64`,
`DATETIME`, or array values; datetimes become RFC 3339 strings. Null attribute values are omitted.
Normal sensitivity rules apply to every expression, including `ATTRIBUTES`.

The closed log `VALUES` set is:

- `time`: required `DATETIME`
- `body`: required `STRING`
- `severity_text`: optional `STRING`
- `severity_number`: optional `I32` in `0..=24`
- `trace_id`: optional nonzero 32-hex-character `STRING`
- `span_id`: optional nonzero 16-hex-character `STRING`

Trace emitters use `TO OTEL <client> TRACES`. Their closed `VALUES` set requires `trace_id`
(nonzero 32-hex `STRING`), `span_id` (nonzero 16-hex `STRING`), `name` (`STRING`), `start_time`
(`DATETIME`), and `end_time` (`DATETIME`). Optional keys are `parent_span_id` (nonzero 16-hex
`STRING`), `kind` (`SERVER`, `CLIENT`, `INTERNAL`, `PRODUCER`, or `CONSUMER`), `status_code` (`OK`,
`ERROR`, or `UNSET`), and `status_message` (`STRING`). Enum strings are case-sensitive.

Each metric emitter defines exactly one metric stream:

```nspl,ignore
TO OTEL otel_main
METRIC 'http.server.request.count' UNIT '1'
DESCRIPTION 'Completed HTTP requests'
SUM MONOTONIC DELTA
VALUES {
  'time' = input.window_end,
  'start_time' = input.window_start,
  'value' = input.request_count
}
ATTRIBUTES { 'http.route' = input.route }
RESOURCE { 'service.name' = 'checkout-pipeline' }
MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
```

Metric shapes are `GAUGE`, `SUM [MONOTONIC] (DELTA | CUMULATIVE)`, and `HISTOGRAM (DELTA |
CUMULATIVE)`. `DESCRIPTION` is optional and follows the required `UNIT`. Gauge and sum points
require `time` (`DATETIME`) and `value` (an exact integer-family, `F32`, or `F64` value). Integer
values use OTLP `as_int`; floating-point values use `as_double`. `start_time` is required for a
delta sum and optional otherwise.

Histogram points require `time`, integer-family `count`, integer-array `bucket_counts`, and `F32`
or `F64` array `explicit_bounds`. Optional keys are `start_time`, numeric `sum`, `min`, and `max`;
`start_time` is required for delta histograms. At runtime, counts must be non-negative and
`len(bucket_counts)` must equal `len(explicit_bounds) + 1`. Exponential histograms and summaries
are not supported.

Without `BATCH`, each pending Arrow batch becomes one Export request containing one resource, one
scope, and all successfully converted records. With `BATCH`, the connector keeps that resource and
scope in every request, takes successfully converted records in source order, and divides them by
`MAX MESSAGES` and the exact protobuf size of each Export request. When a candidate exceeds `MAX
SIZE`, it is halved until each request fits, and a record whose request alone exceeds it follows
`ON MESSAGE ERROR` as a `validation` error of the `encode` operation. The byte limit measures the uncompressed protobuf request before optional gzip; HTTP and gRPC
framing and headers are outside it. `FLUSH ... MAX BATCH SIZE` continues to measure the Arrow batch.

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
with means the endpoint cannot accept the export as configured, and the emitter's unresolved
records follow `ON MESSAGE ERROR`. OTLP `partial_success` cannot be retried safely: Nervix
acknowledges every record and logs a warning, so records rejected by the receiver in that response
are lost.
For a local conversion failure, the rejected record identifies its `otel.<key>` field and gives a
bounded reason without quoting that field's value. The conversion cause stays in the connector's
internal report; the failure does not reject other records in the request.

Nervix does not provision collectors, indexes, tenants, or vendor-side telemetry objects. The OTLP
endpoint must already exist; an unreachable endpoint remains an initialization or publish error.

### ClickHouse

```nspl
CREATE EMITTER to_ch
  FROM notifications
  TO CLICKHOUSE clickhouse_client INSERT TO TABLE my_table
  VALUES {
    "clickhouse_user_id" = input.user_id,
    "clickhouse_now" = NOW(),
    "clickhouse_action" = LOWER(input.action)
  }
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

ClickHouse clients use the HTTP endpoint:

```nspl
CREATE CLIENT ch
  TYPE CLICKHOUSE
  CONFIG {
    'addr' = 'http://127.0.0.1:8123',
    'user' = 'default',
    'password' = 'nervix',
    'timeout_ms' = 5000
  };
```

Optional config keys are `'user'`, `'password'`, `'database'`, and `'timeout_ms'`. The timeout
bounds both sending an insert body and waiting for ClickHouse to finish the insert and return its
result.
For HTTPS endpoints, mount a TLS resource and set `'tls_ca_file'` to the mounted CA path.

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
ClickHouse writes each `JSONEachRow` line directly from the mapped Arrow columns in `VALUES`
order, using the same typed JSON column writer as schemaful JSON emission. Null mapped values are
written as `null`, including null list elements. Column names are escaped once for each write, and
string values use their carrier's escape classification.
`F32` columns keep ClickHouse's JSON number formatting after widening to `F64`; schemaful JSON
codecs format `F32` directly. A `BYTES` value is written as its own octets, which a `String` column
stores unchanged, where schemaful JSON codecs write padded base64.
For ClickHouse, Postgres, and MySQL, a failed multi-row insert is classified first as
record-specific or infrastructure-wide. Infrastructure failures retry with backpressure. A
record-specific failure is isolated by re-executing the insert one record at a time so healthy rows
land and only poison rows follow `ON MESSAGE ERROR`. Isolation can reapply rows from the failed
insert; use the sink's idempotent write facilities where available. Dividing a flush into inserts
also means one flush is not an atomic database transaction.
For a rejected row, the error names a safe ClickHouse error name, Postgres SQLSTATE, or MySQL
SQLSTATE and code. Destination response text can quote a bound value, so it is not included in
diagnostics. Connection and pool failures retain their causes for diagnosing a retry.

### Postgres

```nspl
CREATE EMITTER to_pg
  FROM notifications
  TO POSTGRES postgres_client INSERT TO TABLE my_table
  VALUES {
    "postgres_user_id" = input.user_id,
    "postgres_now" = NOW() AS STRING,
    "postgres_action" = LOWER(input.action)
  }
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  BATCH MAX MESSAGES 500 MAX SIZE 8MiB
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

Postgres emitters may include an insert conflict policy after `VALUES`:

```nspl,ignore
ON CONFLICT ("postgres_user_id") DO UPDATE
ON CONFLICT ("postgres_user_id") DO NOTHING
ON CONFLICT DO NOTHING
```

`DO UPDATE` updates every mapped `VALUES` column except the conflict target columns, and requires a conflict target. `DO NOTHING` may be used with or without a target.
Postgres refuses a `DO UPDATE` insert that carries one conflict key twice, so such an insert is
written again one record at a time, and the later record updates the earlier one.

Postgres clients declare their connection-pool bounds and connect with a `postgres://` or
`postgresql://` URL. The URL must select one of two TLS policies: `sslmode=disable` for an
unencrypted connection, or `sslmode=verify-full` for TLS with certificate-chain and hostname
verification. There is no opportunistic fallback and no encrypted connection without peer
verification. See [Database Client Connection Pools](database-client-pools.md) for the accepted
pool counts:

```nspl
CREATE CLIENT pg
  TYPE POSTGRES
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable'
  };
```

For TLS connections, use `sslmode=verify-full`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path. `'tls_cert_file'` and `'tls_key_file'` supply a client identity and must be given together. TLS files require `sslmode=verify-full`.

### MySQL

```nspl
CREATE EMITTER to_mysql
  FROM notifications
  TO MYSQL mysql_client INSERT TO TABLE my_table
  VALUES {
    "mysql_user_id" = input.user_id,
    "mysql_now" = NOW() AS STRING,
    "mysql_action" = LOWER(input.action)
  }
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MySQL emitters use `VALUES` expressions and insert batches with a multi-row `INSERT ... VALUES (?,
...), ...` command. The [batching clause](#batching) is required: each insert carries at most
`MAX MESSAGES` records and no more than fit the 65,535 placeholders one statement binds, and its
statement text and bound values measure at most `MAX SIZE` bytes, as
[Database writes](#database-writes) describes. The insert result acknowledges those records.
Conflict clauses are the user's tool for bounding duplicates when poison isolation re-executes a
failed insert. Declare a `MAX SIZE` no larger than the server's `max_allowed_packet`, 64 MiB by
default on MySQL 8.x, which the client applies to every packet it sends.

MySQL emitters may include an insert conflict policy after `VALUES`:

```nspl,ignore
ON CONFLICT DO UPDATE
ON CONFLICT DO NOTHING
```

MySQL and MariaDB resolve conflicts through primary and unique keys already defined on the table, so the NSPL conflict policy does not accept a target list. `DO UPDATE` uses `ON DUPLICATE KEY UPDATE` for all mapped `VALUES` columns. `DO NOTHING` uses a no-op duplicate-key update.

MySQL clients declare their connection-pool bounds and use a mysql_async connection URL. See
[Database Client Connection Pools](database-client-pools.md) for the accepted counts:

```nspl
CREATE CLIENT mysql
  TYPE MYSQL
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix'
  };
```

For TLS connections, include `require_ssl=true`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path.

### MongoDB

```nspl
CREATE EMITTER to_mongodb
  FROM notifications
  TO MONGODB mongodb_client INSERT TO COLLECTION my_collection
  VALUES {
    "mongodb_user_id" = input.user_id,
    "mongodb_now" = NOW() AS STRING,
    "mongodb_action" = LOWER(input.action)
  }
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

MongoDB emitters use `VALUES` expressions and bulk writes. The [batching clause](#batching) is
required: each write carries at most `MAX MESSAGES` documents, which measure at most `MAX SIZE`
bytes, as [Database writes](#database-writes) describes. MongoDB reports per-document outcomes, so
healthy documents acknowledge and poison documents follow `ON MESSAGE ERROR` without a separate
isolation pass. Transient or infrastructure failures retry only the undelivered documents. A
document larger than MongoDB's 16 MiB document limit is rejected before the write that would carry
it.

Every mapped integer is written as a BSON 64-bit signed integer, and a `U64` value above that range
has no BSON integer at all. Such a record is rejected before its document is written: it follows
`ON MESSAGE ERROR` with a `validation` error whose affected field is `mongodb.<column>`, it is never
inserted, and its value is never used as an `ON CONFLICT` target. The rejection is permanent, so the
record does not retry. Every other record in the same write is unaffected, and a mapped value that
is genuinely NULL is still written as BSON null.

MongoDB emitters may include an insert conflict policy after `VALUES`:

```nspl,ignore
ON CONFLICT ("mongodb_user_id") DO UPDATE
ON CONFLICT ("mongodb_user_id") DO NOTHING
```

MongoDB conflict policies require a target list because the emitter must build an explicit upsert filter. Target fields must be mapped in `VALUES`. `DO UPDATE` updates every mapped field except the conflict target fields and inserts the full mapped document when no existing document matches. `DO NOTHING` inserts only when no document matches the target.

Emitters using either MongoDB `ON CONFLICT` form require MongoDB 8.0 or newer because those modes
execute each write as one client bulk write.

MongoDB clients declare their connection-pool bounds and use a MongoDB connection URL and database
name. See [Database Client Connection Pools](database-client-pools.md) for the accepted counts:

```nspl
CREATE CLIENT mongodb
  TYPE MONGODB
  POOL SIZE MIN 2 MAX 8
  CONFIG {
    'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
    'database' = 'nervix'
  };
```

For TLS connections, include `tls=true`, mount a TLS resource, and set `'tls_ca_file'` to the mounted CA path.

### Iceberg

```nspl
CREATE CLIENT s3_main
  TYPE S3
  CONFIG {
    'endpoint' = 'http://127.0.0.1:9900',
    'region' = 'us-east-1',
    'access_key_id' = 'rustfsadmin',
    'secret_access_key' = 'rustfsadmin',
    'path_style_access' = true
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'http://127.0.0.1:8181',
    'warehouse' = 's3://nervix-iceberg/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON S3 s3_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 's3://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Iceberg emitters use explicit `VALUES` expressions and do not declare `ENCODE USING`. The `ON S3`, `ON GCS`, or `ON AZURE_BLOB` backend clause selects the object-store implementation. The referenced blob client supplies the object-store connection for table files. The `CATALOG <client>` clause references a separate `TYPE ICEBERG_REST` client that supplies the REST catalog URI and warehouse. The referenced REST catalog namespace and table must already exist; Nervix loads that table and appends data, but does not create catalog namespaces or tables implicitly. The emitter owns the Iceberg table name, mapped output columns, table location, catalog client reference, and flush policy.

The catalog and object-store HTTP clients resolve endpoint names through the node's configured
DNS resolver. The catalog keeps its configured URL and authentication, while OpenDAL uses the
resolver for object operations and credential HTTP calls. DNS failures enter the existing sink
initialization or commit retry path; they do not advance the commit or ACK boundary.

GCS uses the same emitter shape with a `TYPE GCS` client and `gs://` locations:

```nspl
CREATE CLIENT gcs_main
  TYPE GCS
  CONFIG {
    'service_path' = 'https://storage.googleapis.com',
    'token' = '<oauth2-token>'
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'gs://nervix-iceberg/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON GCS gcs_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 'gs://nervix-iceberg/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Azure Blob uses `TYPE AZURE_BLOB` and `wasbs://` locations. `wasb://` is also accepted for plain-HTTP local endpoints:

```nspl
CREATE CLIENT azure_main
  TYPE AZURE_BLOB
  CONFIG {
    'account_name' = 'myaccount',
    'account_key' = '<account-key>'
  };

CREATE CLIENT iceberg_catalog
  TYPE ICEBERG_REST
  CONFIG {
    'uri' = 'https://iceberg-rest.example.com',
    'warehouse' = 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/warehouse'
  };

CREATE EMITTER iceberg_notifications
  FROM notifications
  TO ICEBERG ON AZURE_BLOB azure_main TABLE notifications
  VALUES {
    'user_id' = input.user_id,
    'action' = input.action
  }
  LOCATION 'wasbs://nervix-iceberg@myaccount.blob.core.windows.net/tables/notifications'
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The REST catalog is the authority for namespace and table metadata. Nervix does not write a separate object-store catalog pointer file and does not provision catalog entries from the emitter runtime path.

Iceberg uses two explicit boundaries. `FLUSH` collects typed in-memory batches and writes them to
local Arrow IPC files under the runtime temporary-file root. `COMMIT EACH <duration> MAX SIZE
<bytes>` reads the staged Arrow IPC batches, concatenates them into one Arrow batch, appends that
batch to the Iceberg table, and commits the catalog update. Both durations are domain-logical, so a
paced domain's `TIME RATE` moves the flush and commit boundaries together; the maximum batch and
commit sizes, the catalog retry backoff, and a drain remain independent of domain pace. The
temporary-file root defaults to `/tmp` and can be changed with `--temp-dir` or `NERVIX_TEMP_DIR`.

The sink completion point for `MODE ACK` is the successful catalog commit. Local staging is not an
ACK boundary. Commit conflicts, incompatible table evolution, a dropped table, and unavailable
catalog or object storage are table-level infrastructure failures: Nervix reports them in runtime
and drain status and retries all affected staged records with the declared policy and
backpressure. Record-level expression and construction errors are attributed through `ON MESSAGE
ERROR` before staging. An ambiguous failure after the catalog commit can append the rows again;
Iceberg appends are not idempotent. See [ACK Semantics And Effective
Delivery](#ack-semantics-and-effective-delivery) for attachment and fan-out behavior.

## Codec Behavior On Emission

`ENCODE USING <codec>` follows the sink it encodes for, because whether a codec applies is a
property of the sink. Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS and Sentry publish an
encoded payload and require it. ClickHouse, Postgres, MySQL, and MongoDB map columns with `VALUES`
and do not take a codec. OTEL maps signal fields and attributes with `VALUES` and takes no codec.
Iceberg writes typed records and takes none.

JAQ-native codecs can reshape outbound payloads with `ON EMITTING` before writing the selected
format:

```nspl
CREATE IF NOT EXISTS CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{payload: .}';
```

That lets the emitter publish a different JSON envelope for each outbound row without changing the declared relay schema.

## ACK Semantics And Effective Delivery

Nervix composes per-hop ACKs. The effective delivery semantics of a source-to-sink path are the
observable duplicate and loss behavior produced by the source delivery mode, the emitter's
publishing `MODE`, its attachment, and the external service. They are not the ACK mechanics of any
one hop.

Publishing `MODE` selects the sink completion point at which the emitter considers a record
delivered. Attachment determines whether that outcome participates in the upstream ACK chain:

- `ATTACHED`: emitter success or failure at the selected sink completion point stays part of the
  upstream ACK chain.
- `DETACHED`: relay fan-out acknowledges upstream immediately. The emitter still waits for its
  declared confirmations, applies its retry policy, error-routes record failures, and exerts local
  backpressure, but that outcome cannot delay, retry, or fail the source ACK.

Confirming broker modes and request/response `ACK` modes are at least once. A confirmation timeout
or lost response is not proof that the service rejected a record, so retry can duplicate it. The
parallel window limits how many records are exposed to that ambiguity at one time, and Nervix
resends only records not yet confirmed or definitively rejected. With the
[batching clause](#batching) the unit is the batch payload: a retry resends exactly the payloads not
yet confirmed or rejected, with the bytes and members they were first written with. `NO_ACK`, MQTT QoS 0, Core NATS,
Redis Pub/Sub, and ZeroMQ expose earlier acceptance boundaries and can lose acknowledged records
after a crash or downstream failure. External broker durability and idempotence settings remain
the user's client and service configuration.

When one source record reaches multiple emitters or multiple attached routes, the upstream ACK
completes only after every attached emitter reaches its sink completion point. A failure on any
attached path reopens source retry for the record on all paths. A sink that already published
successfully may therefore receive the record again because a sibling sink failed. This applies to
every sink without idempotent writes. Iceberg is the canonical case: rows can be appended to the
table again after a sibling emitter fails.

This sibling-retry case assumes a source mode that retries when an attached ACK fails or is lost. A
no-ACK source cannot create that retry duplicate, but it can lose the record instead.

A WASM processor on the path adds one more retry of the same kind. It dispatches a guest callback's
output before the checkpoint of the guest's state completes and holds back only the source ACK, so
a checkpoint that fails negatively acknowledges records whose output a sink may already have
published, and the source redelivers them. See
[Recovery, Replay And Duplicates](wasm-processor-guests.md#recovery-replay-and-duplicates).

Every `DETACHED` path has a common loss window: a process can fail after relay fan-out acknowledges
upstream but before the emitter reaches its declared sink completion point. The table below calls
out the additional mode- and transport-specific duplicate and loss conditions.

| Sink | Duplicate conditions (`ATTACHED`) | Additional loss conditions | Idempotency available in Nervix |
| --- | --- | --- | --- |
| Kafka | `ACK` retry after an ambiguous delivery report or timeout; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` can lose a record after local producer-queue admission; broker durability follows Kafka client and topic configuration | None; Kafka producer idempotence is pass-through client configuration |
| Pulsar | `ACK` retry after an ambiguous broker receipt; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` does not expose broker failures after producer acceptance; retention and durability remain broker policy | None |
| NATS | JetStream retry after an ambiguous `PubAck`; either mode after a lost upstream ACK or attached sibling failure | Core NATS `NO_ACK` connection flush is not durable stream acknowledgement | None |
| RabbitMQ | Confirming `ACK` retry after a nack, timeout, or lost confirm; `NO_ACK` retry after its channel or connection is lost before a write's round trip completes; either mode after a lost upstream ACK or attached sibling failure | `NO_ACK` can lose a record after the broker's channel has taken it; queue durability and message persistence remain broker policy | None |
| SQS | Retry after an ambiguous `SendMessage` result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; SQS retains its own at-least-once behavior | None |
| MQTT | QoS 1 or 2 retry after an ambiguous handshake; any mode after a lost upstream ACK or attached sibling failure | QoS 0 can lose a record after client acceptance; later delivery follows the configured broker and session guarantees | None |
| Redis Pub/Sub | Retry after Redis accepts `PUBLISH` but the Nervix ACK is lost, or after attached sibling failure | Any failure after detached relay acceptance; subscribers that are absent or disconnected miss the message | None |
| ZeroMQ | Retry after socket send acceptance followed by lost ACK or attached sibling failure | Any failure after detached relay acceptance; socket send does not establish durable receiver storage | None |
| Sentry | Retry after an ambiguous HTTP result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; an accepted event can still be subject to Sentry service policy | None |
| OTEL | Retry after an ambiguous Export result, which resends the same request bytes; lost ACK; or attached sibling failure | Any failure after detached relay acceptance; `partial_success` acknowledges the whole request, so receiver-rejected records in that response are lost | None |
| HTTP | Retry after a lost response, failed connection, or timeout following an endpoint that applied the request; a lost ACK or attached sibling failure | Any failure after detached relay acceptance; a `2xx` delivers on its headers, so an endpoint that later fails its own processing loses the record | None; the endpoint can deduplicate by a stable key the emitter writes with `write_header` |
| ClickHouse | Retry after an ambiguous insert result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a crash after insert but before acknowledgement can also leave an inserted batch that later retries | None |
| Postgres | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MySQL | Retry after an ambiguous transaction result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed insert can survive a crash before Nervix observes success | `ON CONFLICT` |
| MongoDB | Retry after an ambiguous write result, lost ACK, or attached sibling failure | Any failure after detached relay acceptance; a committed write can survive a crash before Nervix observes success | `ON CONFLICT` |
| Iceberg | Retry after a commit with a lost ACK or attached sibling failure; appends repeat rows | Crash before catalog commit loses staged work unless the attached source redelivers; detached mode accepts that loss | None; appends are not idempotent |

The [publishing-mode table](#publishing-modes) names each transport's exact completion point.
`ATTACHED` waits only for that declared point and cannot make an earlier `NO_ACK` boundary durable.
MQTT QoS 0, Core NATS, Redis Pub/Sub, and ZeroMQ can still lose a message after Nervix observes
client-side acceptance, and RabbitMQ `NO_ACK` after the broker's channel has taken it. `DETACHED`
cannot turn a confirming mode into fire-and-forget inside the emitter; it changes only whether the
result participates upstream.

Emit a stable idempotency key at ingestion, for example with `uuid_v7()`, and carry it through the
graph. Downstream consumers and queries can use that key to suppress retries within that admitted
record's fan-out. A source-provided identifier is stronger because it also survives a fresh source
redelivery. Generate the key once; regenerating it on a downstream route defeats the purpose.

For Postgres, MySQL, and MongoDB, use `ON CONFLICT` against a stable key. This preserves
at-least-once delivery attempts while making the resulting table or collection state
effectively-once for that conflict contract.

Iceberg appends are not idempotent. Deduplicate by the stable key at query time or in downstream
compaction or `MERGE` work. Nervix does not isolate sibling-sink retries by changing ACK mechanics.
That is an [accepted tradeoff](#accepted-tradeoff-shared-retry).

### Accepted Tradeoff: Shared Retry

One input ACK represents all attached descendants. This keeps ACK composition small and preserves
backpressure across the graph. It also means one attached failure retries successful siblings.
Nervix accepts that coupling instead of maintaining a transactional per-sink commit ledger.

### Graph Design

When the same records feed a non-idempotent sink and other emitters, consider `DETACHED` mode for a
non-critical path or separate relays with separate ACK boundaries per sink so one sink's failure
does not drive duplicates into another. `DETACHED` makes that path at-most-once relative to the
upstream ACK: the emitter still confirms and retries according to `MODE`, but a crash after the
detached ACK can lose its in-memory work without causing source redelivery.

See [Data Plane](data-plane.md#ack-composition) for the relay fan-out mechanics and
[What It Is Not](what-it-is-not.md) for the persistence boundary.
