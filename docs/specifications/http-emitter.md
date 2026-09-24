# HTTP emitter

Status: specified. Not implemented. This is the product contract the HTTP emitter delivery tasks
implement and qualify. [The acceptance ledger](../../tests/http-emitter-acceptance-ledger.md) maps
every criterion below to its owning task and its public evidence.

## Required outcome

An HTTP emitter sends one HTTP request for each eligible relay record. Its method, path with
optional query, headers, and body are independently configurable. Method, path, and header
expressions may vary per record. A codec produces the body, or the emitter explicitly sends no
body. Receipt of complete successful response headers is the external acknowledgement boundary.

HTTP emission uses the existing domain-owned `TYPE HTTP` client and the emitter
lifecycle, branch, sensitivity, materialized-state, flush, and error contracts. The endpoint and
credentials must be provisioned by the operator. Creating or starting the emitter performs no
provisioning or probe request against the destination.

## NSPL surface

The HTTP sink has this form:

```nspl,ignore
TO HTTP <client>
  METHOD <string_expression>
  PATH <string_expression>
  MODE ACK RETRY POLICY BACKOFF <duration> MAX <duration>
  (ENCODE USING <codec> | WITHOUT BODY)
```

`METHOD`, `PATH`, `MODE`, the complete retry policy, and exactly one body selection are required,
in that order. `MODE ACK` is the request/response mode: it takes no acknowledgement window or
`ACK TIMEOUT`. HTTP emission supports neither `NO_ACK` nor a parallel publishing mode. The
referenced client's `timeout_ms` bounds each attempt.

The ordinary emitter clauses surround this sink: source relays and their predicates, optional
`COLLECT FOR` and materialized dependencies, route construction where a codec is used, optional
route `WHERE`, optional `INVOKE`, required `FLUSH`, and message and general error policies.
Attachment is selected by the ordinary `CREATE [ATTACHED|DETACHED] EMITTER` form.

An expression ends at its following clause outside parentheses, arrays, and quoted literals.
Qualified fields such as `input.path`, `input.method`, and `input.mode` remain valid references.
Method names are string values: `METHOD 'POST'` is a constant expression.

### Example: a JSON request

The following proposed configuration consumes records supplied to `outgoing` by an application
graph. `api.example.com` is a deployment placeholder for an already provisioned endpoint.

```nspl,ignore
CREATE UNPACED DOMAIN delivery;
USE delivery;

CREATE SCHEMA outbound_event (
  event_id STRING,
  request_method STRING,
  request_path STRING,
  tenant STRING,
  payload STRING
);
CREATE SCHEMA event_body (event_id STRING, payload STRING);
CREATE WIRE JSON SCHEMA event_body_wire MODE STRICT (
  event_id string,
  payload string
);
CREATE CODEC event_body_codec
  FROM WIRE JSON SCHEMA event_body_wire
  TO SCHEMA event_body;
CREATE RELAY outgoing SCHEMA outbound_event UNBRANCHED;

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
```

For an input with method `PATCH`, path `/v1/events/42?notify=true`, and tenant `north`, the
destination receives that method and request target, the three declared headers, and a JSON
object containing only `event_id` and `payload`. Transport fields need not appear in the body.

### Example: a request without a body

An alternative emitter definition, using the domain, client, and relay from the first example:

```nspl,ignore
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

`WITHOUT BODY` sends zero content octets and does not require a codec or an output schema. It
permits route `WHERE` and `INVOKE`, and rejects `INHERIT`, `SET`, and `VALUES`. It does not encode
the input record or construct a dummy payload. HTTP framing may express the absence of content
with an absent or zero `Content-Length`, as the selected HTTP protocol requires.

## Client and destination

For an emitter, `endpoint` declares the destination origin: an absolute `http://` or `https://`
URL with a host and optional port. An absent path or `/` is valid. Credentials in the URL, a
non-root path, a query, a fragment, backslashes, whitespace, and control characters are invalid.
Paths and queries belong in the emitter's `PATH` expression, so every request has one unambiguous
destination.

`timeout_ms` is required when an HTTP client is bound to an emitter. It is a positive integer
number of milliseconds; a value the node cannot schedule rejects activation. It bounds one attempt
from connection acquisition through DNS, connection and TLS establishment, sending, and receipt
of complete final response headers. Queueing behind earlier records and retry backoff do not
consume the next attempt's timeout. The node's drain deadline can end an attempt sooner.

HTTPS verifies the server certificate and hostname. The existing optional `tls_ca_file`,
`tls_cert_file`, and `tls_key_file` settings apply, with certificate and key supplied together.
Mounted files use the existing explicit resource-version binding contract. Authentication such
as a bearer token is supplied through a declared `Authorization` header.

The polling client's `method` CONFIG key remains an ingestion setting; it does not supply or
override an emitter's required `METHOD` expression. A client used by several nodes must satisfy
each use's contract. Emission does not maintain cookies, answer authentication challenges with
additional requests, or follow redirects. Each application-level resend belongs to the declared
emitter retry policy.

### Method

The evaluated method is a nonempty, case-sensitive ASCII HTTP token, at most 64 bytes. Nervix
preserves its spelling. Standard methods and extension methods are accepted, except `CONNECT`
and `TRACE`, which are outside this request-publishing contract. Their ASCII case variants are
also rejected rather than providing a way around that restriction.

`GET` and `HEAD` require `WITHOUT BODY`. `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, and extension
methods may use either body selection. The same restrictions apply when the method is computed
per record. This deliberately avoids relying on undefined GET or HEAD content semantics.
See [HTTP method semantics](https://www.rfc-editor.org/rfc/rfc9110.html#section-9).

### Path and query

`PATH` evaluates to an origin-relative request target, for example `/v1/events/42?notify=true`.
It begins with exactly one `/`; `/` itself is valid. It cannot change the client's scheme,
host, or port. Absolute URLs, `//host/path`, fragments, backslashes, ASCII whitespace, and control
characters are rejected. `*` is not a supported target, including for `OPTIONS`.

The target is parsed as a URL against the configured origin, using the same URL Standard as
Nervix's URL functions. Non-ASCII characters are percent-encoded, and dot segments are normalized.
Existing valid escapes are not decoded and encoded again: `%2F` remains an encoded slash. Every
`%` must be followed by two hexadecimal digits. The query retains its parameter order, repeated
keys, and literal `+` characters; Nervix does not reconstruct it as a parameter map. A trailing
`?` represents an empty query. The resulting origin must equal the client's origin, and the
normalized path must still begin with exactly one `/`.
See the [URL Standard](https://url.spec.whatwg.org/).

For the client origin `https://api.example.com`, these targets have the following outcomes:

| Evaluated `PATH` | Request target or rejection |
| --- | --- |
| `/v1/a/../events?tag=a&tag=b&q=a+b` | `/v1/events?tag=a&tag=b&q=a+b` |
| `/objects/a%2Fb` | `/objects/a%2Fb` |
| `/café` | `/caf%C3%A9` |
| `/events?` | `/events?` |
| `//other.example/events` | Rejected before sending. |
| `/a/..//events` | Rejected because normalization produces a path beginning with `//`. |
| `/events?value=%ZZ` | Rejected because the percent escape is invalid. |

The normalized path and query together are limited to 8 KiB of encoded bytes. Exceeding the limit
rejects the record before any request is sent.

## Expressions, bodies, and headers

Method and path expressions must have exact, statically non-null `STRING` type. Header names and
values have the same requirement. There are no implicit conversions, null omission rules, or
default method or path. Nullable fields require an explicit expression such as `coalesce`.

With a codec, request expressions may read `input` and finalized `output`, the working `message`,
and explicitly declared materialized state. `message` has the finalized output meaning at this
point. With `WITHOUT BODY`, `input` and `message` read the source record and `output` is unavailable.
Bare fields follow the corresponding working-message scope. Branch fields, relay-name field
qualifiers, and source-envelope header reads are unavailable.

For each admitted batch, Nervix resolves materialized dependencies and selects one domain
execution snapshot. Each record then follows this order:

1. Apply its source predicate.
2. Construct and finalize the codec payload, when a codec is selected.
3. Apply route `WHERE`. A filtered record sends no request and does not evaluate request fields.
4. Evaluate and validate `METHOD`, then `PATH`.
5. Evaluate header invocations in written order.
6. Release the record for publication on the route's flush boundary.
7. Encode the finalized payload, when a codec is selected, and send the request.

Every expression in this sequence observes the same execution snapshot and materialized-state
snapshot. A failure before publication sends no part of that record's request. Each prepared
request retains its evaluated method, target, headers, and body bytes for local retries; retries
do not re-run expressions or the codec, including nondeterministic calls.

### Body

`ENCODE USING` has the ordinary codec-emitter construction and sensitivity rules. The emitted
bytes are the entire request body. HTTP adds no JSON wrapper, array, newline, form encoding,
multipart framing, or content compression. A JSON body containing an array is still the body of
one record and produces one request. Header and path fields enter the body only if the declared
construction and codec include them.

Nervix adds no default `Content-Type`; a header invocation declares the media type expected by
the endpoint. A declared `Content-Encoding` describes the actual bytes produced by the codec;
writing the header does not transform those bytes.

### Headers

`INVOKE write_header(name, value)` is the sole header-writing surface. Constant expressions
provide fixed headers and record expressions provide dynamic ones. Names are compared without
ASCII letter case, and a later invocation replaces an earlier value for that name. An empty
string is an empty header value, not deletion. Applications needing a list-valued header supply
its complete value explicitly. Header-name casing and order on the wire are not guaranteed.

Names must be valid ASCII HTTP field names. Values are sent as UTF-8 bytes. Allowed bytes are
horizontal tab (`0x09`), space through `~` (`0x20` through `0x7E`), and non-ASCII bytes. Other
ASCII control bytes and DEL are rejected, including CR, LF, and NUL. A nonempty value must not
begin or end with a space or horizontal tab. These rules keep the header valid over HTTP/1.1 and
HTTP/2 without trimming or changing its value. Invalid values are never quoted in diagnostics.
See [HTTP field validity](https://www.rfc-editor.org/rfc/rfc9113.html#section-8.2.1).

The transport owns `Host`, HTTP/2 pseudoheaders, `Content-Length`, `Transfer-Encoding`,
`Connection`, `Keep-Alive`, `Proxy-Connection`, `TE`, `Trailer`, `Upgrade`, and `Expect`.
Invocations cannot write them. Invalid or reserved header writes reject the record, even if a
later invocation would replace the value. `Authorization`, `Content-Type`, `Content-Encoding`,
`Accept`, `Cookie`, and application headers are ordinary writable headers. A manually supplied
`Cookie` is independent of response cookies, which are not retained.

After replacement, a request may have at most 128 application headers, totalling at most 32 KiB
of UTF-8 name and value bytes. Each individual invocation is also subject to the 32 KiB bound.
These limits exclude transport-generated framing fields. A violation follows `ON MESSAGE ERROR`.

Sensitive data in the body, method, path, header name, or header value requires explicit leakage.
Ingestion headers do not propagate automatically. Diagnostics never include evaluated targets,
credentials, header values, request bodies, or response bodies.

## Publishing and response handling

Each active execution of an emitter has at most one HTTP request awaiting a response at a time.
This limit covers all source relays and branches served by that execution. It processes the
records released for publication in their existing order. `FLUSH` and `COLLECT FOR` retain their
ordinary batching and branch isolation semantics; they do not combine several records into one
HTTP body.
There is no total ordering guarantee across independent source relays, concrete branches, or
different emitters. A request can continue executing at the destination after Nervix loses its
connection or changes owners, so sequential sending cannot order those ambiguous remote effects.

Success requires complete, valid final response headers with a `2xx` status. `202` means the
endpoint accepted the request; Nervix does not poll for the endpoint's later processing result.
`204` is also success. Interim responses do not acknowledge the record.

Response bodies and trailers are not graph data, do not select success, and are not parsed or
buffered in full. Nervix can stop reading them as soon as final headers determine the outcome.
A body that stalls or fails after complete successful headers does not turn success into a retry.
Connections are reused only when their HTTP framing permits reuse. Response headers are limited
to 128 fields and 64 KiB of field-name and field-value bytes; exceeding either bound fails the
attempt without acknowledging it. These bounds apply separately to each interim or final header
block. Malformed framing in the final headers fails the attempt even if its status is `2xx`.

The following classification is fixed:

| Outcome | Action |
| --- | --- |
| Final `200`–`299` | Mark the record delivered and acknowledge its attached upstream work. |
| `408`, `425`, `429`, or `500`–`599` | Retain and retry the request with backpressure. |
| `401`, `403`, or `407` | Report an authentication or authorization infrastructure failure; retain and retry until corrected. |
| Other final `300`–`499` | Reject this record through `ON MESSAGE ERROR`; do not retry it locally. |
| `101` | Reject this record through `ON MESSAGE ERROR`; an emitter cannot upgrade the connection. |
| DNS, connection, TLS, timeout, malformed response, excessive response headers, or connection loss before complete final headers | Report an infrastructure failure; retain and retry the request. |

No redirect is followed, including a redirect to the same origin. `304` is not delivery success.
`409` is a rejection; Nervix does not infer that it means an earlier attempt succeeded. A status
outside the valid final HTTP status range is a malformed response. Missing routes may therefore
reject records with `404`; unavailable services may hold them with `503`.

A definitive rejection affects only its record. Once that record's error policy has completed,
later eligible records may proceed. A transient failure holds the current request and all later
work on this emitter. Records already delivered or definitively rejected are excluded from local
retries, including when a flush contained several records.

### Retry timing and delivery guarantees

Backoff starts at the declared value, doubles after repeated failures, and is capped by its
declared maximum. Completing a flush resets the backoff. Retry continues until the request
completes or execution ends. There is no hidden attempt limit or HTTP-library retry policy that
bypasses this cadence.

On any retryable HTTP response, one valid `Retry-After` field supplies a minimum delay. Integer
seconds are measured physically. An HTTP date is compared with actual UTC when the response is
received, with a date in the past contributing zero delay. The next attempt waits for the larger
of that delay and the declared backoff. The backoff maximum does not shorten a server-requested
delay. Missing, repeated, malformed, or unrepresentable values contribute no server delay.
`Retry-After` does not turn a terminal response into a retry.
See [Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3).

Request timeouts, backoff, and server retry delays are physical. Domain pacing affects `COLLECT`
`FOR` and `FLUSH EACH` and the execution snapshot used by expressions. It does not accelerate
HTTP deadlines or delays. `FLUSH IMMEDIATE` retains its existing physical batching window.

A timeout or lost response can follow a request the endpoint already applied. HTTP emission
therefore permits duplicate external effects, even when a method is normally described as
idempotent. An endpoint that requires deduplication must implement it, typically using a stable
event identifier supplied with `write_header('Idempotency-Key', input.event_id)`. Nervix neither
generates a key nor interprets the endpoint's idempotency protocol. A value generated during
emitter evaluation remains stable for local retry only; upstream redelivery or a restart can
evaluate it again.

`ATTACHED` includes the request outcome in the upstream acknowledgement chain. `DETACHED`
retains HTTP confirmations, local retries, and local backpressure, but cannot delay or reopen an
upstream acknowledgement. Effective recovery also depends on the source's redelivery contract.

## Errors, lifecycle, and observability

Configuration validation checks the client kind, destination, explicit timeout, required clauses,
codec direction, exact expression types, scopes, branch contracts, and sensitivity. An invalid
literal method, target, or header is rejected during validation when its value is statically
known; record-dependent violations are message errors before sending. Validation of a complete
candidate graph remains atomic.

Expression failures retain their normal error classification. Method and path failures identify
the `publish` operation and the corresponding request field; invalid header writes identify
`invoke` and the invocation index; codec failures identify `encode`. Rejected response statuses
use code `external` and operation `publish`. The diagnostic identifies the emitter and safe
reason, including a numeric status where applicable. HTTP does not add a response-body scope.

Message-error construction sees the original eligible input and the captured materialized-state
snapshot. With a codec, `partial_output` is the all-optional view of the attempted codec payload.
With `WITHOUT BODY`, `partial_output` is unavailable because there is no output schema. Error
routes preserve the source branch and use the emitter's declared flush policy. Error handling
cannot re-enter the same policy if constructing its error record fails.

Infrastructure failures appear in emitter status and runtime events while pending requests retain
their acknowledgement leases. Normal retryable failures stay pending; `ON GENERAL ERROR` does
not silently convert a non-successful HTTP response into delivery success. Existing general-error
semantics govern initialization failures and work abandoned when execution cannot continue.

### Configuration changes

Changing the method or path uses a complete
`ALTER EMITTER ... SET TO HTTP ... MODE ...` sink replacement and uses `ENTITY_PAUSE`. For this
HTTP form, `SET TO` includes exactly one of `ENCODE USING` and `WITHOUT BODY`, so a replacement
never inherits an ambiguous body selection. `SET CLIENT` and `SET ENCODE USING` retain their
existing meanings; `SET ENCODE USING` selects codec-body mode, and `DROP ENCODE` is rejected for
HTTP because changing to no body requires an explicit `WITHOUT BODY` replacement. A change in
body mode must validate the retained route clauses and error scopes together. Client-reference
and body-selection changes also use `ENTITY_PAUSE`; changes to a client's own definition retain
the ordinary configuration-entity lifecycle.

For example, this replacement makes the JSON emitter above use a fixed method and path:

```nspl,ignore
ALTER EMITTER deliver_event
  SET TO HTTP api
    METHOD 'POST'
    PATH '/v1/events'
    MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING event_body_codec;
```

Its `INHERIT` and `INVOKE` clauses remain in place. Replacing its body selection with `WITHOUT`
`BODY` would fail because it retains `INHERIT`. Similarly, selecting a codec with required fields
for a bodyless emitter fails because no construction initializes those fields. These operations
do not add, remove, or infer construction clauses. A change requiring different
construction or header invocations uses a complete emitter replacement through `DROP` and
`CREATE` in one transaction. Replacing the same emitter in a transaction is one modification and
retains the required drain, under the ordinary transaction lifecycle.

An entity pause drains admitted work with the configuration that admitted it before installing
the replacement. If it cannot drain, the administrative operation fails under the existing
lifecycle contract; it does not send pending requests to the replacement destination. Changes
only to `FLUSH` remain dynamic. Changing source membership retains the domain-pause contract.

A permanently unavailable destination can therefore prevent a live alteration from draining.
An operator can restore that destination or stop the domain, change its configuration while
stopped, and restart under the existing source-recovery contract. Stopping does not preserve
pending HTTP requests. This is the same distinction between alteration and stopping described in
[Domains And Time](../src/domains-and-time.md).

Graceful shutdown stops intake, force-flushes pending records, and waits for HTTP completion within
the existing drain deadline. A timed-out drain does not acknowledge unresolved requests as
delivered. Prepared requests and retry state are volatile; restart recovery comes from an
acknowledged source's redelivery, with the usual risk of duplicates after ambiguous completion.

HTTP imposes no additional fixed body-size limit. Encoded bodies retained for retry count toward
node memory pressure; `MAX BATCH SIZE` still measures logical payload bytes and is not a bound on
encoded HTTP body size. A receiver's `413` rejects the affected record. One in-flight request,
the envelope bounds above, and the explicit request timeout bound each HTTP attempt without
requiring an unbounded response-body buffer.

`SHOW CREATE EMITTER` and formatting preserve the method and path expressions and explicit body
selection. Completion follows the HTTP clause order and offers only the applicable mode and body
forms. `DESCRIBE EMITTER` reports the HTTP client, configured request expressions, body selection,
flush and retry policies, and transient failure status. Existing output counters count a record
once on successful delivery, not once per attempt. Codec-body output byte counters keep the
ordinary emitter payload accounting; a bodyless delivery contributes zero output payload bytes.
Request metadata and response bytes do not inflate payload-byte counters. Evaluated URLs and
header values never become metric labels.

## Relationship to optional emitter batching

An HTTP emitter publishes exactly one request per eligible record, so it has no batch container.
The optional `BATCH MAX MESSAGES <n> MAX SIZE <bytes>` clause of
[optional emitter batching](./emitter-batching.md) is not part of the HTTP sink: a statement that
declares it on an HTTP emitter is rejected with a validation error naming the emitter, completion
does not offer it after an HTTP sink, and `ALTER EMITTER ... SET BATCH` on an HTTP emitter fails
the same way. `DESCRIBE EMITTER` reports `batch: none` for every HTTP emitter.

This leaves the batching contract unchanged. Its sink matrix, its validation list, and its
qualification cover the sixteen sinks it names, and none of them gains or loses a form because the
HTTP sink exists. The HTTP emitter still consumes the host machinery both contracts share: a
prepared request is a prepared payload with exactly one member, retained verbatim across local
retries and resolved once by its outcome.

## Acceptance criteria

Observable runtime criteria run through NSPL and an external HTTP receiver on both one-node and
three-node clusters.

1. Constant and computed methods and paths, including a query, reach the receiver with the exact
   codec bytes and configured headers. Two records choose different requests without sharing data.
2. A request without a body reaches the receiver with zero content bytes and its declared headers.
   GET and HEAD are accepted only in this form, including when the method is dynamic.
3. Request fields read the original input and finalized output as specified. A record filtered by
   route `WHERE` evaluates no request expressions and sends nothing.
4. Invalid types, nullable expressions, unavailable scopes, wrong client types, invalid literal
   fields, missing timeout, and implicit sensitive leakage reject configuration. Explicit leakage
   permits the intended external value.
5. Header names compare without case; later writes replace earlier values. Invalid and reserved
   fields, CR/LF injection, leading or trailing whitespace in nonempty values, and every envelope
   limit reject the affected record without a request. Empty values and internal spaces remain
   valid.
6. Path normalization preserves encoded separators and query order. Absolute URLs, authority
   changes, malformed escapes, fragments, backslashes, and paths normalized to a leading `//` fail
   before network publication. The target examples above produce their specified outcomes.
7. `200`, `202`, and `204` complete delivery. A response body that stalls after successful headers
   cannot delay that completion or trigger a duplicate request. Malformed or excessive final headers
   fail the attempt even when the status is `200`.
8. A flush containing several records delivers its first record, receives `503` or `429` for the
   next, then retries only the pending request. Method, target, body, and headers remain identical,
   including an initially generated header value.
9. Physical timeout, connection loss, authentication failure, and each retryable status retain work
   and expose a transient failure. `Retry-After` seconds and dates extend the delay; invalid values
   do not. Domain acceleration does not shorten these physical waits.
10. A terminal `400`, `404`, `409`, `413`, or redirect reaches the configured message-error route
    with safe diagnostics and the original branch. Other records continue after error handling.
    Redirects cause no request to their `Location`.
11. HTTPS trust, hostname verification, and mounted client certificates follow the declared
    configuration. An invalid certificate never produces an acknowledged request.
12. Interleaved branches and multiple eligible source relays retain independent collection and
    request values; error routes obey the exact branch contract.
13. Create, inspect, canonical formatting, and ALTER preserve expressions and body mode. Invalid
    body-mode replacements leave the active emitter unchanged. A successful replacement drains
    requests under their admitted configuration. A transactional emitter replacement validates its
    complete construction and request expressions before activation.
14. A graceful drain completes eligible requests; a forced ending leaves unresolved attached work
    for the source's recovery contract. The tests demonstrate the permitted duplicate after an
    endpoint applies a request whose successful response is lost.
