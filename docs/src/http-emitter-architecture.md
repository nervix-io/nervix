# HTTP Emitter Architecture

An HTTP emitter turns each eligible relay record into one outbound request. Its method, target,
application headers, and optional codec body are evaluated for that record. Complete, valid final
`2xx` response headers are the external delivery boundary. The endpoint can still apply a request
whose response Nervix never receives, so delivery is not an exactly-once external effect.

This chapter follows the implemented HTTP path across the language, decision layer, emitter host,
and HTTP connector. The [Emitters manual](./emitters.md#http-request-configuration) owns the public
NSPL contract and field-level rules. The [Connector Crates And The Connector Contract](./connector-contract.md)
chapter owns the shared host and connector boundary; [Control Plane](./control-plane.md),
[Domain Clock](./domain-clock.md), [Resource Versions And Bindings](./resource-versions.md), and
[Shutdown And Recovery](./shutdown.md) remain authoritative for their respective lifecycle,
time, binding, and recovery rules. The [HTTP emitter acceptance ledger](https://github.com/nervix-io/nervix/blob/main/tests/http-emitter-acceptance-ledger.md#http-emitter-09-qualification)
records the public qualification evidence summarized below.

## From NSPL to an active sink

The language parses `TO HTTP` and its ordered `METHOD`, `PATH`, `MODE ACK RETRY POLICY`, and body
selection into semantic Models. The registry validates the complete candidate against the same
domain's `TYPE HTTP` client, codec, exact expression types, available scopes, sensitivity, source
branches, and error route. The decision layer lowers the validated model into a typed execution
plan. A running data-plane task receives that plan; it does not read a Model or reparse NSPL.
The server composes the HTTP connector with the emitter host. These conversions and their
installation boundary are described in [Execution Plans](./execution-plans.md).

The sink has one request/response mode, with an explicit retry policy and exactly one body choice:

```nspl,ignore
TO HTTP <client>
  METHOD <string_expression>
  PATH <string_expression>
  MODE ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  (ENCODE USING <codec> | WITHOUT BODY)
```

`METHOD` and `PATH` are structured expressions, not client settings. HTTP `MODE ACK` has no ACK
window or `ACK TIMEOUT`; `NO_ACK`, parallel publication, and the optional emitter `BATCH` clause
are unavailable. The emitter still requires a route-local `FLUSH` policy. `COLLECT FOR` may gather
input records before execution, while `FLUSH` governs when prepared work is released. Neither
merges several records into one HTTP body. The `TYPE HTTP` client's polling `method` setting has
no effect on the emitter's required method expression.

### A request with a codec body

Here `outgoing` and `event_body_codec` are already declared in the domain. The codec maps an
`event_body` schema with `event_id` and `payload` into one JSON object. The example uses an
operator-provisioned destination; an input with method `PATCH` and path
`/v1/events/42?notify=true` sends that method and target. Its tenant stays outside the body.

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
         write_header('X-Body-Event', output.event_id),
         write_header('Idempotency-Key', input.event_id)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The client endpoint is an origin only: `http://` or `https://`, a host, optional port, and no
credentials, non-root path, query, or fragment. `PATH` supplies the origin-relative target and
query. The client requires a positive, schedulable `timeout_ms` for every attempt. Creating or
starting the emitter makes no destination probe and provisions nothing at the endpoint. See the
[complete public example](./emitters.md#http-request-configuration) for the schema, wire schema,
codec, and relay declarations.

### A request without a body

This alternative uses the same relay and client. `WITHOUT BODY` uses the source record for
`input`, `message`, and bare fields. There is no output schema, encoded input copy, or dummy body.
It allows route `WHERE` and header invocations, but not `INHERIT`, `SET`, or `VALUES`.

```nspl
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

The receiver gets zero content octets. HTTP framing may omit `Content-Length` for this request;
the transport supplies it when a body is present. `GET` and `HEAD`, in any ASCII case, require
`WITHOUT BODY`, including when the method is computed at runtime. `CONNECT` and `TRACE` are
unavailable in any ASCII case. Other valid methods retain their evaluated spelling.

### Scopes, order, and sensitivity

Method, path, header-name, and header-value expressions must have exact, statically non-null
`STRING` type. There is no implicit cast or null omission. A codec route can read original
`input`, finalized `output`, the working `message` at its finalized-output stage, and declared
materialized state. A bodyless route has `input` and source-valued `message`, but no `output` or
`partial_output` schema. Bare fields follow `message`. Branch fields, relay-name qualifiers,
and source-envelope header reads are unavailable. A route may consume multiple source relays
with the same payload schema, keeping each relay and concrete branch's collection separate.
Materialized dependencies and message-error targets must be compatible with every source's exact
branch declaration.

For an admitted batch, the host resolves materialized dependencies and takes one domain execution
snapshot. Each eligible record then passes through this sequence:

1. Evaluate its source predicate.
2. Construct and finalize the codec record, when there is a codec.
3. Evaluate route `WHERE`. A filtered record evaluates no request field and sends nothing.
4. Evaluate and validate `METHOD`, then `PATH`.
5. Evaluate `write_header` invocations in written order.
6. Release the record on its declared `FLUSH` boundary.
7. Encode the finalized codec record, if present, and send the prepared request.

All these expressions use the batch's execution and materialized-state snapshots. Preparation or
encoding failure sends no part of that record's request. An external sensitive value, whether in
the method, target, header name or value, or codec body, requires explicit `leak_sensitive(...)`.
Headers read by an ingestor do not propagate through a relay unless copied into schema-backed
fields. [Expression Functions](./filter-map-functions.md) owns the public function contracts;
[Errors And Diagnostics](./errors-and-diagnostics.md) owns the safe failure representation.

### Origin, TLS, and resource versions

The connector uses the node's configured resolver, verifies HTTPS trust and hostname, and can
load a CA and a paired client certificate and key from the HTTP client's resource mount. A
client can, for example, bind already uploaded TLS files as follows:

```nspl
CREATE CLIENT secure_api TYPE HTTP
  MOUNT api_tls VERSION 3
  CONFIG {
    'endpoint' = 'https://api.example.com',
    'timeout_ms' = 5000,
    'tls_ca_file' = '{{ api_tls }}/ca.pem',
    'tls_cert_file' = '{{ api_tls }}/client.pem',
    'tls_key_file' = '{{ api_tls }}/client.key'
  };
```

`VERSION LATEST` is also accepted at creation, but resolves to a completed number when the
statement applies. The stored binding and runtime plan keep that number across restart and
relocation. Uploading a newer version does not move the client. A client-definition change or
resource rebind follows the normal configuration-entity domain pause; changing only the
emitter's client reference takes an entity pause. Validation checks the origin, timeout, and
certificate/key pairing before activation, without contacting the endpoint. The
[Resources](./resources.md#client-config-mounts) manual gives the upload and mount syntax; the
[resource-version chapter](./resource-versions.md#the-pinning-invariant) owns pinning and
rebind behavior.

## Request preparation and transport ownership

The emitter host owns intake from Arrow batches, per-source and per-branch collection, route
programs, the original source row reference, the captured materialized state, flush cadence,
prepared request retention, source membership, ACK leases, backpressure, retry scheduling,
metrics, and drain. It projects existing Arrow columns into the codec and request-field programs;
it does not create a row-map payload. The HTTP connector receives a typed, already prepared
request containing the validated method, normalized target, application headers, and either exact
codec bytes or no body. It owns HTTP connection and TLS handling, transport-generated fields,
response parsing, `Retry-After` interpretation, and per-request outcomes. It sees neither graph
branches nor upstream ACKs. The [connector chapter's HTTP sink boundary](./connector-contract.md#sink-boundary)
defines the shared contract and host loop in detail.

One HTTP request is prepared for each eligible source record, even when `COLLECT FOR` or `FLUSH`
groups records. The host stores the request as a prepared payload with one source member, along
with the original record and admitted state snapshot. The method, target, headers, and encoded
bytes are frozen for local retries. Nondeterministic expressions and codecs do not run again.
Retained encoded bodies count toward node memory pressure; `FLUSH MAX BATCH SIZE` measures
logical Arrow bytes and is not an encoded HTTP body limit.

The connector sends prepared requests sequentially, with at most one awaiting final response
headers per active emitter execution across its served relays and branches. Requests released by
one flush preserve their publication order, and unresolved work stays ahead of later work. There
is no total order across independent executions, relays, or branches, nor an ordering promise for
effects an endpoint applies after a connection is lost. Each attempt uses a fresh HTTP/1.1
connection, closed after final headers, so an unread response body cannot become the next
response. The client's physical timeout covers DNS, connect, TLS, send, interim heads, and the
complete final head; queue time and backoff do not consume the next attempt's timeout.

The transport generates `Host`, `Connection: close`, and `Content-Length` for a present body. It
adds `Accept: */*` only if the application did not write `Accept`. It adds no `Accept-Encoding` or
`Content-Type`. It does not store response cookies, issue a second request for an authentication
challenge, follow a redirect, or retry independently of the host. The codec's bytes are the entire
body: there is no added wrapper, newline, form encoding, or compression. Writing
`Content-Encoding` describes bytes the codec already produced and does not transform them.

### Validation and supported bounds

The registry rejects statically known invalid literals when the graph is validated. Dynamic
values undergo the same checks per record before publication. A later `write_header` call replaces
an earlier value for the same case-insensitive name, but an invalid or reserved earlier write
still rejects the record. An empty header value is sent as empty, and a nonempty value cannot have
leading or trailing space or horizontal tab. UTF-8 non-ASCII values are allowed. Header values
cannot contain CR, LF, NUL, DEL, or other disallowed ASCII controls.

| Boundary | Supported limit or rule | Failure |
| --- | --- | --- |
| Method | Nonempty ASCII HTTP token, at most 64 bytes; `CONNECT` and `TRACE` excluded; `GET` and `HEAD` bodyless only | Literal: candidate rejected; dynamic: message error before send |
| Target | Origin-relative, exactly one leading `/`; normalized path and query at most 8 KiB encoded | Literal: candidate rejected; dynamic: message error before send |
| Application headers | At most 128 after replacement and 32 KiB of UTF-8 name and value bytes in total; each invocation also has the 32 KiB bound | Invalid literal: candidate rejected; dynamic violation: message error before send |
| Response headers | At most 128 fields and 64 KiB of field-name and value bytes in **each** interim or final block | Attempt remains unresolved and retries |
| Attempt | One request awaiting final headers per execution; positive, schedulable client `timeout_ms` | Attempt failure retains work; timeout is physical |
| Body | No extra HTTP-specific fixed encoded-body size bound | Encoded bytes remain under node memory pressure until resolved; endpoint `413` rejects the record |

The target is parsed relative to the client's origin and cannot change its scheme, host, or port.
Absolute URLs, `//` authority targets, fragments, backslashes, whitespace, controls, `*`, malformed
percent escapes, and targets normalized to a leading `//` fail before sending. URL normalization
percent-encodes non-ASCII text and removes dot segments, while preserving `%2F`, repeated query
keys, their order, a literal `+`, and an empty query after a trailing `?`. For example,
`/v1/a/../events?tag=a&tag=b&q=a+b` becomes `/v1/events?tag=a&tag=b&q=a+b`.
The [HTTP specification's target table](https://github.com/nervix-io/nervix/blob/main/docs/specifications/http-emitter.md#path-and-query)
shows the boundary cases.

The transport owns `Host`, HTTP/2 pseudoheaders, `Content-Length`, `Transfer-Encoding`,
`Connection`, `Keep-Alive`, `Proxy-Connection`, `TE`, `Trailer`, `Upgrade`, and `Expect`.
`write_header` cannot set them. `Authorization`, `Content-Type`, `Content-Encoding`, `Accept`,
`Cookie`, and application headers are writable. Headers are compared without ASCII case; their
wire casing and order are not guaranteed.

## Outcomes, retry, and acknowledgements

The connector classifies a complete valid final head. Interim heads never acknowledge a record.
Response bodies and trailers do not enter graph data or decide delivery, and the connector does
not wait for or buffer a whole response body. Invalid final framing fails the attempt even if
the status line is `200`.

| Final response or exchange | Result |
| --- | --- |
| `200`–`299`, including `202` and `204` | Deliver exactly this source record when final headers are complete. `202` does not promise later endpoint processing. |
| `408`, `425`, `429`, or `500`–`599` | Retain the request and all later work; retry after the host's wait. |
| `401`, `403`, or `407` | Retain and retry, reporting an authentication or authorization infrastructure cause. |
| Other `300`–`499`, or `101` | Reject only that record through `ON MESSAGE ERROR`, then continue after the policy completes. Redirect `Location` is never followed; `304` and `409` are not delivery. |
| DNS, connection, TLS, send, timeout, malformed response, oversized head, or loss before complete final headers | Retain and retry as an infrastructure failure. |

A delivered record leaves the pending set and is never included in a later local retry. A
definitively rejected record follows its route's message error policy and likewise leaves the
pending set. Only unresolved prepared requests are sent again. A transient failure holds the
current request and later records behind it, across the execution's relays and branches. The HTTP
connector keeps its client after a failed publish, allowing the host to retain the reported
failure while the next attempt is in flight.

The first physical backoff is `BACKOFF`, doubles on repeated failures up to `MAX`, and resets
after a flush completes. There is no attempt cap. On a retryable final status, exactly one valid
`Retry-After` can extend the next wait: ASCII whole seconds, or an IMF-fixdate, RFC 850, or
asctime HTTP date compared with actual UTC when received. A past date contributes zero. The host
waits for the larger of this delay and its backoff, even if the server delay exceeds `MAX`.
Missing, repeated, malformed, or unrepresentable values (including a delay ending after the
supported year 2262 timestamp range) add no delay, and an interim head or a
terminal response cannot change this classification. The connector's
[physical-time boundary](./connector-contract.md) and [Domain Clock](./domain-clock.md) distinguish
actual UTC and monotonic waits from domain time. `TIME RATE` scales `COLLECT FOR` and `FLUSH EACH`
and the expression snapshot; it never scales HTTP timeouts, backoff, or `Retry-After`.
`FLUSH IMMEDIATE` uses the existing physical minimum batching window.

`ATTACHED` keeps each unresolved record's upstream ACK lease until HTTP delivery or its message
error policy resolves it. `DETACHED` acknowledges the source at relay fan-out, but still confirms
HTTP responses, retries unresolved requests, applies backpressure, and routes terminal errors.
Source recovery depends on that source's own acknowledged-delivery contract. A lost final response
may mean the endpoint already applied the request; retrying it can duplicate the external effect.
Nervix supplies no idempotency key. The application can send a stable event identifier, as the
`Idempotency-Key` invocation above does, and arrange endpoint-side deduplication. A generated
value is stable during a *local* retry because preparation is retained; source replay after a
restart may evaluate it again. [ACK Semantics And Effective Delivery](./emitters.md#ack-semantics-and-effective-delivery)
defines the wider source/sink combinations.

## Errors and inspection

A request-field error rejects its record at the first failed field. Failed expressions retain
code `evaluation`; invalid method, target, or header values use `validation`. Method and path
errors use operation `publish` and name `method` or `path`. A header failure uses `invoke` and its
zero-based invocation index. Codec failures use `encode`. A terminal response uses code
`external`, operation `publish`, and its numeric status. These errors identify the emitter and
safe operation or field without quoting the evaluated method, target, header value, credential,
request body, or response body. A message-error route sees the original eligible input and the
captured materialized-state snapshot; a codec route also offers the attempted record as an
all-optional `partial_output`. The error stays in its source branch. A bodyless route has no
`partial_output`. [Errors And Diagnostics](./errors-and-diagnostics.md#runtime-message-errors)
owns the typed propagation and public redaction contract.

`SHOW CREATE EMITTER` and formatting preserve the request expressions, ordered header
invocations, body selection, and retry/flush policies. `DESCRIBE EMITTER` shows the client,
configured method and path, `body: codec` or `body: without body`, `batch: none`, publishing mode,
flush policy, and transient error. The failure stays visible while a request remains pending,
including during its retry attempt, and clears when it is delivered. It carries a status or safe
transport cause and the retry wait, never request values. Normal retryable failures remain pending
regardless of `ON GENERAL ERROR`; that policy cannot turn them into delivery success.

The emitter's `messages_total` sent count increases once per delivered source record, not per
attempt or terminal rejection. Its `bytes_total` sent count uses the ordinary logical Arrow
payload accounting for a codec record. A bodyless delivery adds zero output payload bytes.
Method, target, headers, and response bytes are not payload bytes or metric-label values. See
[HTTP inspection and metrics](./emitters.md#http-inspection-and-metrics) for the public rendering.

## Replacement, drain, and recovery

Changing the sink's method, path, client reference, body selection, or retry mode takes an
`ENTITY_PAUSE`; a `FLUSH`-only change is `DYNAMIC`. Changing source membership or a client
definition has its existing `DOMAIN_PAUSE` contract. `ALTER EMITTER ... SET TO HTTP` restates the
complete method, path, mode, and body selection. `SET CLIENT`, `SET MODE`, and `SET ENCODE USING`
retain their current meanings. `DROP ENCODE` cannot select no body; use `SET TO ... WITHOUT BODY`.
The whole retained construction and error scope must validate against that selection. For
example, an emitter with `INHERIT` cannot switch to `WITHOUT BODY` while retaining that clause.
Changing construction or header invocations requires a complete `DROP` and `CREATE` replacement
inside one transaction. A bad candidate or refused commit leaves the active emitter in place.

For example, this replacement changes the first emitter's method and path without replacing its
codec construction or header invocations:

```nspl
ALTER EMITTER deliver_event
  SET TO HTTP api
    METHOD 'POST'
    PATH '/v1/events'
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING event_body_codec;

SHOW CREATE EMITTER deliver_event;
DESCRIBE EMITTER deliver_event;
```

The retained `INHERIT` still initializes the codec body. A transition to `WITHOUT BODY` would
require a replacement that also removes `INHERIT`, so the operator uses `DROP` and `CREATE` in one
transaction for that change. For a destination that cannot drain, the supported operational path
is `STOP;`, change the emitter while the domain is stopped, then `START;`, after deciding how the
source will replay unresolved work.

An entity pause gates intake and drains already admitted requests under their original client,
resource binding, destination, and prepared bytes. A failed drain leaves the mutation unapplied;
it does not move pending requests to a new endpoint. `DESCRIBE TRANSACTION` reports the required
scope and actual engagement under [Transaction Quiescence And Impact Inspection](./transaction-quiescence.md).
When a destination cannot be repaired in place, an operator can stop the domain, alter the
stopped emitter, and start the domain again. That path discards volatile prepared attempts and
relies on the source's replay contract; it can repeat an effect the destination already applied.
The [Control Plane](./control-plane.md) chapter owns mutation and drain decisions.

Graceful node shutdown stops intake, force-flushes buffered HTTP work, and waits for eligible
requests only within the existing physical drain deadline. Expiry or forced ending cancels an
in-flight attempt without marking it delivered. Prepared method, target, headers, body, retry
position, and ACK state are volatile. An attached acknowledged source can redeliver after restart;
an unacknowledged source cannot recover a record it has lost. The
[Shutdown And Recovery](./shutdown.md) chapter owns stop phases, the former-owner fence, and the
source recovery boundary; this chapter adds no alternate shutdown protocol.

## Qualification and practical limits

The [completed 14-criterion matrix](https://github.com/nervix-io/nervix/blob/main/tests/http-emitter-acceptance-ledger.md#the-completed-matrix)
names each public scenario and the receiver observation that closed it. Every HTTP feature case
ran on one-node and three-node clusters. Each row below identifies its primary public evidence;
the ledger gives the exact scenario names, example counts, and receiver controls.

| Criterion | Primary evidence | Observed result |
| --- | --- | --- |
| 1. Constant and computed requests | `http_emitter.feature` | Two records send distinct methods, targets, headers and exact codec bytes; a constant request and an encoding rejection are observed. |
| 2. Absent body and method rule | `http_emitter.feature` | Bodyless `DELETE`, `GET` and `HEAD` send zero content; literal and computed GET/HEAD with a codec are refused. |
| 3. Scopes and filtering | `http_emitter.feature` | `input` and finalized `output` produce different captured values; a filtered record evaluates no failing request field. |
| 4. Configuration and sensitivity | `http_emitter.feature` | 22 invalid statements are refused; an explicitly leaked method, path, header and body reach the receiver. |
| 5. Header replacement and bounds | `http_emitter.feature` | Case-insensitive replacement, empty and UTF-8 values succeed; 22 invalid, reserved or oversized writes reject without sending. |
| 6. Target normalization and bounds | `http_emitter.feature` | The specified path table, non-ASCII query and repeated keys match; ten unsafe or oversized targets reject before send. |
| 7. Response-head success | `http_emitter_responses.feature` | `200`, `202` and `204` deliver; stalled and 32 MiB bodies are abandoned; malformed or excessive interim and final heads retry. |
| 8. Stable retry membership | `http_emitter_retries.feature`, `http_emitter.feature` | The delivered first record is not resent; the failed second request repeats byte for byte, including a generated header and body. |
| 9. Infrastructure retry and timing | `http_emitter_responses.feature`, `http_emitter_transport.feature`, `http_emitter_retries.feature`, `http_emitter_inspection.feature` | Every retryable status and connection cause remains inspectable while pending; measured physical backoff and `Retry-After` survive domain acceleration. |
| 10. Terminal rejection | `http_emitter_responses.feature`, `http_emitter_retries.feature` | `101`, redirects, `400`, `404`, `409` and `413` route safe status errors with original input, state and branch; `Location` receives no request. |
| 11. TLS and mTLS | `http_emitter_transport.feature` | Mounted client identity succeeds; untrusted, wrong-host and missing-certificate handshakes acknowledge nothing. |
| 12. Relay and branch isolation | `http_emitter.feature`, `http_emitter_transport.feature` | Two relays and two branches retain their own request values and error branches behind collection. |
| 13. Inspection and replacement | `http_emitter_lifecycle.feature`, `http_emitter.feature`, `tools/nspl_format.feature` | Both body modes round-trip; failed ALTER and COMMIT preserve the active emitter; admitted requests drain to their original destination. |
| 14. Drain and replay | `http_emitter_lifecycle.feature`, `http_emitter_retries.feature` | Graceful shutdown force-flushes; forced ending leaves a Kafka offset for replay; a lost response permits the same effect twice. |

The receiver's high-water mark stayed at one request awaiting a final head for one emitter serving
two relays and two branches; a separate regression observed two for two concurrent clients. A
stalled or 32 MiB response body did not delay the next record, and the receiver observed abandoned
connections after timeout, stop, and forced drain. Shuttle checks covered cancelled attempts and
unresolved membership. Attached Kafka offsets remained uncommitted while a request was pending.
The emitter publishes no per-emitter memory figure, so this evidence does not establish a fixed
per-emitter memory ceiling; retained encoded bodies are governed by node memory pressure.

After the receiver fixture update, all seven HTTP features ran together with **180 scenarios and
2,870 steps passing without a retry**. The affected HTTP polling, shared emitter, and shutdown
suites passed another **168 scenarios and 1,764 steps**. The ledger records the exact commands,
focused unit and Shuttle results, documentation build, validation, and an earlier saturated
cluster run whose three-node setup commands met leadership changes. These results qualify the
implemented behavior and its stated limits; the ledger remains the detailed evidence record.
