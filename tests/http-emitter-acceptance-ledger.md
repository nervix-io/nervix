# HTTP emitter acceptance ledger

This is the shared-contract agreement and executable acceptance plan for
[HTTP Emitter 01](https://app.clickup.com/t/86bc78n97), the entry point of the
[HTTP emitter epic](https://app.clickup.com/t/86bc78mmu). The product contract is
[the HTTP emitter specification](../docs/specifications/http-emitter.md). The audit ran against
`6bd2ee58` on 24 September 2026, the tip of `main` when this task started.

This ledger records who owns every overlapping contract, how the HTTP sink relates to optional
emitter batching, what current `main` already provides and what it lacks, the receiver fixture the
delivery tasks share, and which delivery task owns each of the specification's fourteen acceptance
criteria. [HTTP Emitter 02](https://app.clickup.com/t/86bc78n9q) starts from this record.

## Ownership and coordination

Neither the HTTP emitter epic nor any of the eleven epics it overlaps has an assignee recorded in
ClickUp, and neither do the delivery tickets inspected for this audit. No named person is inferred.
Ownership below is by owning ticket: the ticket that changes a shared contract is responsible for
it, and a ticket that consumes it coordinates through that ticket rather than editing it in place.

| Shared contract | Owning ticket | The HTTP emitter's part | HTTP delivery task |
| --- | --- | --- | --- |
| Emitter grammar, completion, formatting and the emitter Model shape | [Emitter Batching 02](https://app.clickup.com/t/86bc73jbb) for the shared optional batching clause and the emitter Model it reshapes | The `TO HTTP` sink form, its `METHOD`/`PATH` expressions, its body selection, its `WITHOUT` and `BODY` keywords, and its `ALTER` forms, built on the reshaped Model | [02](https://app.clickup.com/t/86bc78n9q) |
| Sink capability declaration | The vocabulary's `SinkCapabilities` and `EmitSink` in `crates/models`, owned by whichever ticket adds a sink | The HTTP sink writes headers, accepts only the request/response `MODE ACK` publishing mode, and declares no batch container | [02](https://app.clickup.com/t/86bc78n9q) |
| HTTP client settings, TLS and mounted resources | The connector-crate epic's shared `HttpClientConfig` in `crates/connector`, and the resource-version binding contract | Validation of an emitter-bound client's origin and required `timeout_ms`, extending the shared builder rather than forking it | [03](https://app.clickup.com/t/86bc78na5) |
| Expression scopes, types, sensitivity and URL semantics | The typed-states and columnar VM epics | Exact non-null `STRING` request fields, the codec and bodyless scopes, explicit leakage, and WHATWG normalization through the `url` crate the URL functions already use | [03](https://app.clickup.com/t/86bc78na5), [04](https://app.clickup.com/t/86bc78nb2) |
| Prepared payloads and source membership | [Emitter Batching 06](https://app.clickup.com/t/86bc73jd6) | One prepared request per eligible record, carried as a prepared payload with exactly one member; no second retry carrier or acknowledgement map | [04](https://app.clickup.com/t/86bc78nb2) |
| The outbound HTTP sink in the connector crate | The connector-crate epic, qualified by [Connectors 14](https://app.clickup.com/t/86bc21vve) | The sink half of `crates/connectors/http` and the contract's HTTP request sink; driver, header and response interpretation stay in the crate, and the host keeps lifecycle, retry cadence and acknowledgements | [04](https://app.clickup.com/t/86bc78nb2) sends each prepared request and answers `2xx` as delivered and every other outcome as a failed attempt, so its scenarios reach the receiver; [05](https://app.clickup.com/t/86bc78nbh) classifies responses and qualifies the transport |
| Retry, acknowledgement and backpressure in the emitter host | [Collapse the emitter task loop's repeated publish-outcome handling](https://app.clickup.com/t/86bc6r9m9), consumed through Emitter Batching 06 | Response classification, `Retry-After`, and the one-request-in-flight rule, applied through the consolidated outcome owner | [06](https://app.clickup.com/t/86bc78nqt) |
| Deterministic checks of completion, cancellation and force flush | The Shuttle epic | Extensions to the landed production-type checks wherever HTTP changes completion or cancellation ownership | [06](https://app.clickup.com/t/86bc78nqt) |
| `ALTER` impact classification, gates, drain and transaction inspection | The transaction-quiesce epic | The HTTP sink's `ENTITY_PAUSE`, `DYNAMIC` and `DOMAIN_PAUSE` classification and its drain scenarios, through the existing planner and report | [07](https://app.clickup.com/t/86bc78nr7) |
| Shutdown, drain deadline and source recovery | The canonical [Shutdown And Recovery](../docs/src/shutdown.md) chapter and the WASM durability epic's upstream acknowledgement boundary | Prepared requests stay volatile; a forced ending leaves attached work to the source's redelivery | [07](https://app.clickup.com/t/86bc78nr7) |
| `SHOW CREATE`, `DESCRIBE`, message errors and metrics | The Client Wire epic for transport, the typed-errors epic for error types | HTTP's rendering, its error attribution, and its delivery counters, through the existing typed interfaces | [08](https://app.clickup.com/t/86bc78nrk) |
| Scenario lifecycle, deadlines and cleanup | The Cucumber-lifecycle epic and [Integration Test Lifecycle](../docs/src/integration-test-lifecycle.md) | The [HTTP receiver fixture](#the-http-receiver-fixture) below, which nests inside that lifecycle | This task; every later task consumes it |
| Node-to-node simulation | The Turmoil epic | None: outbound HTTP is tested against a real receiver, never a simulated one | None |
| Qualification of the whole matrix | This epic | Composing every criterion on one and three nodes | [09](https://app.clickup.com/t/86bc78nrz) |
| Architecture documentation | This epic | The consolidated chapter; each delivery task still updates the public pages it changes | [10](https://app.clickup.com/t/86bc78nt8) |

### The batching boundary

This HTTP contract sends one request for each eligible source record. `FLUSH` and `COLLECT FOR`
keep their ordinary meanings and never merge records into one body.

The HTTP sink declares no batch container, so optional batching's clause is rejected on it. The
specification's
[relationship to optional emitter batching](../docs/specifications/http-emitter.md#relationship-to-optional-emitter-batching)
states the public behavior. The rejection lives in the HTTP sink's own capability declaration,
added by HTTP Emitter 02. The batching epic's sink matrix, validation list and qualification keep
exactly the sixteen sinks they name, so neither contract is widened or weakened. The shared
machinery flows the other way: the batching epic owns prepared payloads and membership, and the
HTTP emitter consumes them with one member per payload.

### Prerequisite status

Refreshed on 24 September 2026:

| Prerequisite | Status | Blocks |
| --- | --- | --- |
| [Emitter Batching 02: Add optional batching to NSPL, Models and execution plans](https://app.clickup.com/t/86bc73jbb) | IN PROGRESS | HTTP Emitter 02 |
| [Emitter Batching 06: Preserve batch membership through acknowledgements and retries](https://app.clickup.com/t/86bc73jd6) | FOCUS, itself blocked by Emitter Batching 05 and the publish-outcome consolidation | HTTP Emitter 04 |
| [Connectors 14: Qualify the connector crates end to end and close out the server manifest](https://app.clickup.com/t/86bc21vve) | IN PROGRESS | HTTP Emitter 05 |
| [Collapse the emitter task loop's repeated publish-outcome handling](https://app.clickup.com/t/86bc6r9m9) | PRE-FOCUS | Emitter Batching 06, and through it HTTP Emitter 04 |

None of them is duplicated here. The HTTP chain consumes their results.

### What current main provides

The audit checked every specification claim that depends on existing behavior. These hold on
`main`: `TYPE HTTP` clients with `endpoint`, `method`, `timeout_ms` and paired `tls_cert_file` and
`tls_key_file`; `MOUNT <resource> VERSION <n>`; the request/response `MODE ACK RETRY POLICY` form
that Sentry, OTEL, the databases and Iceberg use; `ALTER EMITTER ... SET TO`, `SET MODE`,
`SET CLIENT`, `SET ENCODE USING` and `DROP ENCODE`; the `DYNAMIC`, `ENTITY_PAUSE` and
`DOMAIN_PAUSE` quiesce levels; message-error code `external` and operations `publish`, `invoke` and
`encode`; `partial_output` for codec emitters only; URL functions that follow the WHATWG URL
Standard through the `url` crate; and the [Domains And Time](../docs/src/domains-and-time.md)
distinction between alteration and stopping.

These gaps are the delivery tasks' work, and none of them contradicts the specification:

| Current main | Owning delivery task |
| --- | --- |
| No `TO HTTP` sink: the emitter grammar accepts sixteen sink keywords after `TO`, and `HTTP` is not one of them | 02 |
| `METHOD`, `WITHOUT` and `BODY` are not language tokens; `PATH` already is | 02 |
| `write_header` is declared only for Kafka, Pulsar, RabbitMQ, NATS and SQS | 02 |
| `timeout_ms` is optional for every HTTP client, and `endpoint` is never validated as a URL | 03 |
| HTTP client configuration is documented only on the ingestor page | 03 |
| `crates/connectors/http` implements only the source contract | 04, which added the request sink its scenarios send through; 05 completes response classification |
| `docs/src/emitters.md` has no HTTP sink and lists request/response sinks without it | 02 through 08, each with its own surface |

## The HTTP receiver fixture

`tests/common/http_receiver.rs` is an in-process HTTP/1.1 receiver that stands in for the
operator-provisioned endpoint. The receiver belongs to the scenario that started it, draws its port
from the scenario's fixture ports, and is stopped in the `stopping` phase before the cluster. The
[Integration Test Lifecycle](../docs/src/integration-test-lifecycle.md#http-receivers) chapter
records its bounds and its cleanup.

| Step | Purpose |
| --- | --- |
| `Given HTTP receiver "<name>" is running` | A plain HTTP receiver. `{{http_receiver.<name>}}` is its origin and `{{http_receiver_port.<name>}}` its port. |
| `Given HTTPS receiver "<name>" is running with a certificate for "<hosts>"` | A TLS receiver whose certificate names only `<hosts>`, so dialing any other name fails hostname verification. |
| `... and requires a client certificate` | The same, refusing any client that does not present the certificate the receiver issued. |
| `Given node "<node>" has the TLS files of HTTP receiver "<name>" in resource directory "<placeholder>"` | Places `ca.pem`, `client.pem` and `client-key.pem` where the node can upload and mount them. |
| `Given HTTP receiver "<name>" answers with` | Appends one scripted response per docstring line, taken by the next requests in order. |
| `Given HTTP receiver "<name>" answers unscripted requests with "<response>"` | Replaces the standing response, a `200` without a body until replaced. |
| `Given HTTP receiver "<name>" answers requests for "<target>" with "<response>"` | Answers every request for one exact path and query with its own response, ahead of the script, for requests of independent branches or relays whose order is not part of the contract. |
| `When HTTP receiver "<name>" releases its held responses with "<response>"` | Answers every request held until released, and every one held later, with the named response. |
| `Then HTTP receiver "<name>" eventually receives at least <n> requests` | Waits up to 60 seconds for the capture count. |
| `Then HTTP receiver "<name>" request <i> is` | Compares one captured request: request line, the named headers exactly, and the exact body. |
| `Then HTTP receiver "<name>" captured one request that is` | Finds the one captured request with the docstring's request line, wherever it arrived, and compares it the same way, for requests of independent branches or relays. |
| `Then HTTP receiver "<name>" request <i> repeats request <j>` | Compares two captures byte for byte: request line, every header field in arrival order, and body. |
| `Then HTTP receiver "<name>" request <i> arrived at least "<duration>" after request <j>` | Measures the two capture times to prove an attempt timeout, retry backoff, or sequential request wait without a silence window. |
| `Then HTTP receiver "<name>" request <i> has no header "<header>"` | Checks that the transport did not add an undeclared request header. |
| `Then HTTP receiver "<name>" request <i> carries header "<header>" and a body containing "<text>"` | Checks a generated header and body that cannot be named in advance. |
| `Then HTTP receiver "<name>" has captured exactly <n> requests` | Counts the captures when the step runs, after the expected requests arrived and every request that must never be sent would have preceded them. |
| `Then HTTP receiver "<name>" eventually records a failed TLS handshake` | Waits up to 60 seconds for a client to fail its handshake. |

Script lines cover every receiver behavior the specification's criteria depend on:

| Script line | Control |
| --- | --- |
| `respond <status>` | Any three-digit status, complete, with `Content-Length` except where the status carries no content |
| `; header <name>: <value>` | Response headers such as `Retry-After`, `Location` or `Set-Cookie` |
| `; retry after date in <duration>` | A `Retry-After` HTTP date that long after the head is written, rounded up to a whole second, so it asks for at least that delay |
| `; body <text>` | A response body |
| `; after <duration>` | A delayed response, for timeouts measured against `timeout_ms` |
| `; interim <status>` | An interim response before the final one |
| `; stall body` | Complete successful headers, then a body that never finishes |
| `; extra headers <n>` | More response header fields than any bound, for excessive-header failures |
| `; header value bytes <n>` | A generated final field value of exactly `n` bytes, for the 64 KiB boundary |
| `; interim extra headers <n>` | Generated fields in the interim block, independent of the final fields |
| `; interim header value bytes <n>` | A generated interim field value of exactly `n` bytes |
| `lose response` | The request is read in full and the connection closes without an answer: an applied request whose response is lost |
| `hold response` | The request is read in full and never answered: a physical timeout |
| `hold response until released` | The request is read in full and answered only once the scenario releases it: an attempt that stays unresolved for exactly as long as a scenario observes it |
| `raw <bytes>` | Arbitrary bytes with `\r`, `\n` and `\\` escapes, for malformed framing and invalid statuses |

Two receivers in one scenario give an `ALTER` a second destination, so a scenario can prove that
admitted requests never move to the replacement.

The fixture is qualified two ways. `just test-harness-liveness http_receiver` runs focused
regressions on real loopback sockets: scripted order, lost and held responses, responses held until
released, answers by target, `Retry-After` dates measured from when their response is written,
stalled bodies, chunked request bodies, interim and raw responses, request bounds recorded as
faults, mutual TLS, hostname verification, and a stop that ends held connections inside its budget
without forcing them.
`tests/features/runtime/http_receiver.feature` drives the receiver with the HTTP client Nervix
already has, the polling ingestor's, over HTTP with a `503` then `200` status sequence, over
mutual TLS with the receiver's files mounted as a resource, and against a client that does not trust
the receiver, on one and three nodes.

The receiver speaks HTTP/1.1 only and offers only `http/1.1` over ALPN. The specification's header
rules are chosen to stay valid over HTTP/2 as well, but no criterion requires an HTTP/2 endpoint.

## Initial failing cases

`tests/features/runtime/http_emitter.feature` held the two initial public cases, each on one and
three nodes:

- `An HTTP emitter sends each record with its own method, path, headers, and codec body` sends two
  records whose methods, targets, headers and bodies all differ, and compares both captured
  requests byte for byte.
- `An HTTP emitter declared without a body sends zero content bytes` sends a constant `DELETE` to a
  record-computed path with a declared header and no body.

On 24 September 2026 both failed at the statement that created the emitter, because the grammar had
no `TO HTTP` sink. Once HTTP Emitter 02 and 03 landed they failed at activation instead, with
`cannot plan emitter 'deliver_event': HTTP emitter requires a HTTP client, found HTTP 'api'`, as did
the request scenarios HTTP Emitter 04 added first, on 27 September 2026 with `--retry 0`. Its
encode-failure scenario came once requests reached the receiver. The same day, on one and three
nodes, both expected requests arrived but the rejected record's error never reached its route,
because the handler read the finalized record, which has no `tenant`, as `input`. HTTP Emitter 04
prepares and sends the requests and keeps each admitted request's source record and state, so
every case now runs in the ordinary suite under `@http_emitter_requests`, and the suite no longer
excludes an expected-failure tag.

## Acceptance matrix

### HTTP Emitter 05 transport qualification

`tests/features/runtime/http_emitter_responses.feature` covers terminal and retryable statuses,
complete `200`, `202`, and `204` headers, stalled bodies, exact and exceeded interim/final field
counts and value-byte limits, and malformed final framing. Its 58 one- and three-node cases pass.
`tests/features/runtime/http_emitter_transport.feature` covers mounted mutual TLS, untrusted and
wrong-host TLS handshakes, physical timeout and lost-response retries, a manually supplied
`Accept`, startup without a probe, and one awaiting request across two source relays. Its 12
one- and three-node cases pass. The status-`400` response case was red against the HTTP Emitter 04
sink because it retried the refusal instead of routing its message error, then green after the
connector change.

The sink uses a bounded HTTP/1.1 exchange per request. This permits inspecting every interim
header block before the final one, while the existing HTTP polling source keeps its shared
reqwest client. The sink uses the shared node DNS resolver and rustls TLS configuration, adds
`Accept: */*` only when the application has not supplied `Accept`, and sends no default
`Accept-Encoding` or `Content-Type`. The connector has no separate retry mechanism; the host
retains the prepared request and owns every resend.

### HTTP Emitter 06 retries, acknowledgements and backpressure

`tests/features/runtime/http_emitter_retries.feature` covers, on one and three nodes:

- a flush whose first request is delivered and whose second answers `503`, where the retry resends
  only the unresolved requests, byte for byte and with the same generated header, ahead of a record
  that arrived on another source relay meanwhile;
- `Retry-After` in whole seconds on `503` and on an authentication failure, and as an HTTP date on
  `429`, each extending the wait beyond the declared `MAX`, and a past date, fractional seconds,
  two fields and a value beyond every representable delay, each adding nothing;
- the declared backoff doubling up to `MAX` across five consecutive failures, without an attempt
  limit;
- an `ATTACHED` emitter whose Kafka source keeps its offset uncommitted, past its one-second
  `ACK TIMEOUT`, while a retried request is held unresolved, and a `DETACHED` emitter whose source
  commits while the same request is held, both delivering exactly the expected requests afterwards;
- terminal `404`, `409`, `413` and `308` responses routed with the original input, the captured
  materialized state and the attempted codec record, followed by the next record, with no request
  to the `Location` receiver and no wait for the response's `Retry-After`;
- terminal responses in two interleaved branches, answered by target, whose error records stay in
  their own branch while the other records are delivered;
- a paced domain at `TIME RATE 100` whose `FLUSH EACH 300s` and `now()` follow domain time while
  the retry backoff, a `Retry-After` delay and the request timeout stay physical;
- an endpoint that applied a request whose response was lost, receiving the identical request
  again with its stable `Idempotency-Key`.

On 27 September 2026, run with `--retry 0` against the fixture and scenarios alone, 14 of its 40
cases failed, every one at a measured gap: each retry the server asked to delay arrived 120 to
150 milliseconds after the response, because the sink did not read `Retry-After`. The other 26
passed, since the host already retried only unresolved requests, held later work, kept attached
leases, routed terminal records and kept its waits physical. With the sink reading `Retry-After`,
all 40 pass, and so do the 102 cases of `http_emitter.feature`, `http_emitter_responses.feature`,
`http_emitter_transport.feature` and `http_receiver.feature`.

No completion or cancellation ownership changed, so the Shuttle checks of prepared payloads and
their answers stay as HTTP Emitter 04 and Emitter Batching 06 left them.

### HTTP Emitter 07 alteration, drain, shutdown and recovery

`tests/features/runtime/http_emitter_lifecycle.feature` exercises each lifecycle contract on one
and three nodes. A held request keeps its original method, path and destination while `ALTER`
waits for the entity drain; a failed drain leaves the original emitter active. The body-mode
cases reject retained `INHERIT`, `SET`, and `partial_output` uses when the replacement has no
body, preserve a valid `INVOKE`, and replace the full body and header construction in one
`DROP` plus `CREATE` transaction. The transaction report and command output expose the expected
quiesce levels: entity for request and mode changes, dynamic for `FLUSH`, and domain for source
membership and client-definition replacement.

The shutdown cases exercise an hourly flush that must publish during a graceful drain, then a
Kafka source whose first applied HTTP request loses its response and whose retry remains held
when the drain deadline expires. The held work remains unacknowledged; after restart the source
redelivers the same request and commits only after the receiver answers it. This covers the
volatile prepared-request boundary without treating the receiver's application of a request as
proof that the attached source may commit.

### HTTP Emitter 08 inspection, errors and metrics

`tests/features/runtime/http_emitter_inspection.feature` covers, on one and three nodes:

- a codec emitter and a bodyless emitter reading one relay whose token field is `SENSITIVE` and
  leaked into an `Authorization` header: a token that is not a valid header value rejects its
  record with operation `invoke` at position `0`, a `404` rejects the next with code `external`,
  operation `publish` and the numeric status, and a `503` leaves the third pending while its retry
  is held. Each error message is matched through its closing quote, so it holds no value;
  `DESCRIBE EMITTER` reports `HTTP endpoint answered with retryable status 503` with its
  `reconnect backoff` for as long as the retry is held, and neither it nor the runtime event names
  the token or the evaluated target. Once the retry is delivered the transient error clears and
  each emitter reports one sent message, in one sent batch for the codec emitter: the retried
  request counts once and the refused one not at all. The codec emitter's sent bytes stay under 64
  although every request carried a 200-byte header, and the bodyless emitter's are `0`, in
  `DESCRIBE` and in Prometheus;
- an `ATTACHED` emitter with `ON GENERAL ERROR IGNORE` whose retried request is held: the emitter
  reports the `503` and the Kafka source's offset stays uncommitted past its one-second
  `ACK TIMEOUT` until the request is delivered.

`http_emitter_transport.feature`'s failed TLS handshakes now also expect
`HTTP TLS handshake failed: invalid peer certificate` with the handshake's own cause in
`DESCRIBE EMITTER`, and `http_emitter.feature`'s configuration round trip reads `DESCRIBE EMITTER`
for both body modes after `CREATE` and `ALTER`: codec, body, client, request expressions, flush and
retry policy.

On 29 September 2026, with `--retry 0` against `main` before the change, all eight inspection and
TLS cases failed at their `DESCRIBE EMITTER` steps. While a retried request was held, the emitter
read `status: OK` and `transient error: -`, because every retry reopened the sink and the
reopening cleared the failure; a TLS failure read `failed to publish emitter batch`, the host's
own context, because the sink attached no description. A probe that stopped short of those steps
then read, once the held request was delivered, `messages_total` `sent` `2` for both emitters,
counting the refused record, and `bytes_total` `sent` `538` with a codec body and `574` without a
body: both counted every request's method, target and headers, and the bodyless emitter also
counted the source records it sent no body for. The configuration round trip already
passed, since `DESCRIBE EMITTER` rendered the request contract before this task.

Every criterion runs through NSPL against the HTTP receiver on one and three nodes unless its row
names a topology. The owning task adds the criterion's scenarios red, turns them green, and keeps
them in the ordinary suite; HTTP Emitter 09 composes the complete matrix and closes it.

| Criterion | Public scenarios | Observable evidence | Receiver control | Owner |
| --- | --- | --- | --- | --- |
| 1. Constant and computed methods, paths and queries reach the receiver with exact codec bytes and headers, and two records choose different requests | `http_emitter.feature`: the dynamic request case above, plus a constant `METHOD 'POST'` and `PATH '/v1/events'` case, and a codec that fails to encode one of three records | Captured request lines, header values and body bytes, differing per record; no request and an `encode` message error for the record whose body cannot be encoded | Capture, standing `204` | 04 |
| 2. A bodyless request carries zero content bytes and its headers; `GET` and `HEAD` are accepted only without a body, including when computed | The bodyless case above; `GET` and `HEAD` bodyless cases; a codec emitter whose computed method is `GET` | Empty captured body; configuration rejection for a literal `GET` with a codec; a message error with operation `publish` and field `method` for a computed one | Capture | 02 grammar, 03 literal rejection, 04 computed rejection and sending |
| 3. Request fields read the original input and the finalized output; a record filtered by route `WHERE` evaluates nothing and sends nothing | A codec emitter whose path reads `output` and whose header reads `input`; a route `WHERE` that filters one of two records whose request fields would fail | The captured values match the finalized output and the original input; the receiver captures one request and no message error is routed | Capture | 04 |
| 4. Invalid types, nullable expressions, unavailable scopes, wrong client types, invalid literals, a missing timeout and implicit sensitive leakage reject configuration; explicit leakage permits the value | Negative `CREATE` and `ALTER` cases for each rejection; one positive leakage case | The command fails with the owning node, route, operation and field, and quotes no value; the leaked value reaches the receiver | Capture for the positive case | 03 |
| 5. Header names compare without case, later writes replace earlier ones, invalid and reserved fields, CR/LF, edge whitespace and every envelope limit reject the record, and empty values and internal spaces stay valid | Records exercising each header rule and the 128-header and 32 KiB bounds | Replaced and empty values in captured requests; message errors with operation `invoke` and the invocation index for each rejection; no request for a rejected record | Capture | 04 |
| 6. Path normalization keeps encoded separators and query order, rejects the listed targets before sending, and matches the specification's target table | One record per row of the target table, plus fragment, backslash and 8 KiB cases | Exact captured targets for the accepted rows; message errors with operation `publish` and field `path` for the rejected ones, with no request | Capture | 04 |
| 7. `200`, `202` and `204` complete delivery; a stalled body after successful headers neither delays completion nor causes a duplicate; malformed or excessive final headers fail the attempt even with `200` | One case per success status; a stalled-body case followed by a second record; malformed and excessive header cases | The next record reaches the receiver while the stalled body is open; exactly one capture of the stalled record; a transient failure in `DESCRIBE EMITTER` for the bad framing, then delivery when the receiver recovers | `respond 202`, `respond 204`, `stall body`, `extra headers 129`, `raw` | 05 |
| 8. A flush of several records delivers the first, receives `503` or `429` for the next, and retries only that request with identical method, target, body and headers, including a generated header value | `http_emitter_retries.feature`: a three-record flush against `respond 200`, then `respond 503; header Retry-After: 3`, with a `uuid_v4()` header, and a record from a second source relay arriving during the wait | Five captures: the retried request byte-identical to its first attempt and at least three seconds after it, the first record captured once, and the later relay's record after the retried flush | Scripted status sequence | 06 |
| 9. Timeout, connection loss, authentication failure and each retryable status keep the work and expose a transient failure; `Retry-After` seconds and dates extend the delay, invalid values do not, and domain acceleration does not shorten the waits | `http_emitter_retries.feature`: `Retry-After` in seconds on `503` and `401`, as a future date on `429`, as a past date, fractional, repeated and unrepresentable; the backoff doubling across five failures; the backoff, a `Retry-After` delay and a timeout on a paced domain. The retryable classes and transport failures are `http_emitter_responses.feature` and `http_emitter_transport.feature` | The measured gap between two captured attempts at least the required delay, asserted as a delay rather than a silence; a prompt retry for every ignored value. `http_emitter_inspection.feature` and the TLS cases of `http_emitter_transport.feature` read the pending failure in `DESCRIBE EMITTER` while a retry is held or failing | `hold response`, `lose response`, `respond 401`, `respond 429`, `respond 503; header Retry-After: ...`, `retry after date in ...` | 05 classifies, 06 retries and paces, 08 reports the status |
| 10. Terminal `400`, `404`, `409`, `413` and redirects reach the message-error route with safe diagnostics and the original branch, other records continue, and no request follows `Location` | `http_emitter_retries.feature`: `404`, `409`, `413` and `308` whose `Location` names a second receiver, with the original input, captured state and attempted body; `404` and `409` in two interleaved branches, answered by target. `http_emitter_responses.feature` covers `101`, `301`, `304` and `400` | Error records with code `external`, operation `publish`, the numeric status, the input-only tenant, the captured state and the attempted body, in the source branch; the next record captured; the second receiver captures nothing | `respond 404`, `respond 308; header Location: ...`, answers by target, a second receiver | 05 classifies, 06 routes |
| 11. HTTPS trust, hostname verification and mounted client certificates follow the configuration, and an invalid certificate never produces an acknowledged request | Mutual TLS with mounted receiver files; an untrusted receiver; a certificate for `localhost` dialed as `127.0.0.1` | Captures over mutual TLS; a failed handshake and a transient failure, with no capture and no acknowledgement, for the untrusted and mismatched cases | HTTPS receivers, client certificate required, `{{http_receiver_port.<name>}}` | 03 validates, 05 connects |
| 12. Interleaved branches and several eligible source relays keep independent collection and request values, and error routes keep the exact branch | `http_emitter.feature`: two concrete branches with one rejected request field per branch; `http_emitter_retries.feature`: terminal responses in two interleaved branches, and two source relays sharing one retried emitter | Per-branch captured values, and error records only in their own branch | Capture, `respond 404`, answers by target | 04 prepares, 06 routes, 09 composes |
| 13. Create, inspect, formatting and `ALTER` keep expressions and body mode; invalid body-mode replacements leave the emitter unchanged; a replacement drains under the admitted configuration; a transactional replacement validates everything first | `http_emitter.feature`: `SHOW CREATE` and `DESCRIBE` round trips for both body modes after `CREATE` and `ALTER`, and `http_emitter_inspection.feature`'s sent counters for both; invalid `SET TO`; `SET TO` with a held request to a second receiver; `DROP` and `CREATE` in one transaction | Round-tripped text; the unchanged emitter still delivering; the held request completing at the first receiver, not the second; transaction inspection | Two receivers, `hold response` | 02 grammar, 07 lifecycle, 08 inspection |
| 14. A graceful drain completes eligible requests; a forced ending leaves unresolved attached work to source recovery; a lost successful response causes the permitted duplicate | Graceful shutdown with requests pending; forced ending with a held request and an acknowledged source; `http_emitter_retries.feature`: `lose response` then `respond 204` with a stable `Idempotency-Key` | Captured requests before shutdown completes; the source redelivers after restart; the applied record captured twice, identically | `lose response`, `hold response` | 06 duplicate, 07 drain and recovery |
