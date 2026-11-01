# HTTP emitter acceptance map

This map relates the [HTTP emitter specification](../docs/specifications/http-emitter.md) to its
receiver fixture, public scenarios and execution bounds. Historical planning, prerequisites, audit
findings and run results are retained on their producing tasks:
[01](https://app.clickup.com/t/86bc78n97), [05](https://app.clickup.com/t/86bc78nbh),
[06](https://app.clickup.com/t/86bc78nqt), [07](https://app.clickup.com/t/86bc78nr7),
[08](https://app.clickup.com/t/86bc78nrk) and [09](https://app.clickup.com/t/86bc78nrz).

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
| `When HTTP receiver "<name>" answers requests for "<target>" with "<response>"` | Replaces the answer for one target mid-scenario, so an endpoint that kept failing one request recovers while the scenario watches its retries. |
| `When HTTP receiver "<name>" releases its held responses with "<response>"` | Answers every request held until released, and every one held later, with the named response. |
| `Then HTTP receiver "<name>" eventually receives at least <n> requests` | Waits up to 60 seconds for the capture count. |
| `Then HTTP receiver "<name>" request <i> is` | Compares one captured request: request line, the named headers exactly, and the exact body. |
| `Then HTTP receiver "<name>" captured one request that is` | Finds the one captured request with the docstring's request line, wherever it arrived, and compares it the same way, for requests of independent branches or relays. |
| `Then HTTP receiver "<name>" eventually captures one request that is` | Waits up to 60 seconds for a request with the docstring's request line, then compares the one such request, for a request that follows retries whose number cannot be named. |
| `Then HTTP receiver "<name>" request <i> repeats request <j>` | Compares two captures byte for byte: request line, every header field in arrival order, and body. |
| `Then HTTP receiver "<name>" request <i> arrived at least "<duration>" after request <j>` | Measures the two capture times to prove an attempt timeout, retry backoff, or sequential request wait without a silence window. |
| `Then HTTP receiver "<name>" request <i> has no header "<header>"` | Checks that the transport did not add an undeclared request header. |
| `Then HTTP receiver "<name>" request <i> carries header "<header>" and a body containing "<text>"` | Checks a generated header and body that cannot be named in advance. |
| `Then HTTP receiver "<name>" has captured exactly <n> requests` | Counts the captures when the step runs, after the expected requests arrived and every request that must never be sent would have preceded them. |
| `Then HTTP receiver "<name>" eventually records a failed TLS handshake` | Waits up to 60 seconds for a client to fail its handshake. |
| `Then HTTP receiver "<name>" never had more than <n> request(s) awaiting a response` | Reads the most requests that ever awaited their final head at once. A request awaits from its capture until the receiver begins writing its final head, or until its connection ends without one, so a sender that waits for each final head never has two. |
| `Then HTTP receiver "<name>" eventually sees the client abandon at least <n> unfinished response(s)` | Waits up to 60 seconds for clients to close held responses, stalled bodies, or bodies still being written. A stop of the receiver abandons nothing. |

Script lines cover every receiver behavior the specification's criteria depend on:

| Script line | Control |
| --- | --- |
| `respond <status>` | Any three-digit status, complete, with `Content-Length` except where the status carries no content |
| `; header <name>: <value>` | Response headers such as `Retry-After`, `Location` or `Set-Cookie` |
| `; retry after date in <duration>` | A `Retry-After` HTTP date that long after the head is written, rounded up to a whole second, so it asks for at least that delay |
| `; body <text>` | A response body |
| `; body bytes <n>` | A generated body of exactly `n` bytes, at most 64 MiB, written in chunks, so a body many times a loopback connection's buffers reaches its end only if the client reads it |
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
faults, the most requests awaiting a response at once, responses a client abandoned at a held
response, a stalled body or a 32 MiB body, waits for a request line, mutual TLS, hostname
verification, and a stop that ends held connections inside its budget without forcing them.
`tests/features/runtime/http_receiver.feature` drives the receiver with the HTTP client Nervix
already has, the polling ingestor's, over HTTP with a `503` then `200` status sequence, over
mutual TLS with the receiver's files mounted as a resource, and against a client that does not trust
the receiver, on one and three nodes.

The receiver reads and answers HTTP/1.1 itself, and a TLS receiver can also offer HTTP/2 over ALPN,
which hyper's HTTP/2 server then serves. The specification's header rules stay valid over HTTP/2,
and `tests/features/runtime/http_client_transport.feature` delivers the emitter's requests over
HTTP/2 to an endpoint whose TLS offers it, directly and inside a declared proxy's tunnel.

## HTTP Emitter 09 qualification

Each criterion below runs through Plexus against the receiver on one and three nodes unless the
row names a topology. [HTTP Emitter 09](https://app.clickup.com/t/86bc78nrz) holds the qualification's
commands, tested revisions, results and affected-suite record.

### The completed matrix

This is a contract-to-scenario map. It does not assert that a run qualified the current revision.

| Criterion | Scenarios | Observable evidence |
| --- | --- | --- |
| 1. Constant and computed methods, paths and queries reach the receiver with exact codec bytes and headers, and two records choose different requests | `http_emitter.feature`: each record with its own method, path, headers and codec body; constant requests, and `GET` and `HEAD` only without a body; a computed method keeps its exact spelling; an unencodable body | `PATCH /v1/events/42?notify=true` and `POST /v2/tenants/south/events` with their own headers and exact JSON bodies; a constant `POST /v1/events` for every record; `purge`, `Patch`, `OPTIONS` and a 64-byte token as evaluated; JAQ-encoded bodies byte for byte |
| 2. A bodyless request carries zero content bytes and its headers; `GET` and `HEAD` only without a body, including when computed | `http_emitter.feature`: without a body; constant requests, and `GET` and `HEAD` only without a body; a computed method; unsafe literal fields | `DELETE /v1/events/7` with no content and its three headers; computed `GET` and `HEAD` sent bodyless and refused with a codec, as is `get`, with operation `publish` and field `method`; literal `GET` and `HEAD` with a codec refused at creation |
| 3. Request fields read the original input and finalized output; a record filtered by route `WHERE` evaluates and sends nothing | `http_emitter.feature`: request fields read input and output; without a body; unsafe literal fields | `PUT /v1/FIRST/1` with `X-Source-Payload: first` and `X-Message-Payload: FIRST`; the filtered record, whose path and ratio would fail, sends and routes nothing; without a body `message.tenant` and bare `payload` read the source record; `output` without a body is refused |
| 4. Invalid types, nullable expressions, unavailable scopes, wrong client types, invalid literals, a missing timeout and implicit leakage reject configuration; explicit leakage permits the value | `http_emitter.feature`: unsafe literal fields; an explicit usable timeout; explicitly leaked values reach the endpoint | Twenty-two refused `CREATE` and `ALTER` statements, each with its reason and no value; `PUT /accounts/acct-7` with `Authorization: Bearer tok-7` and the leaked body field. The inspection scenario's leaked token reaches the receiver too |
| 5. Headers compare without case and later writes replace; invalid and reserved fields, CR/LF, edge whitespace and every envelope limit reject the record; empty values and internal spaces stay valid | `http_emitter.feature`: header writes replace without case | Twenty-two rejected records, each with operation `invoke` and its invocation index, none sent: every transport-owned name, `:authority`, a name with a space, CR/LF, a bare LF, NUL, DEL, a leading space or tab, a trailing space, one field over 32 KiB, 32 KiB in total, and 129 names; `X-Replaced: final`, `X-Empty:`, `X-Spaced: a  b` and `X-City: Zürich` as UTF-8 |
| 6. Path normalization keeps encoded separators and query order; absolute URLs, authority changes, bad escapes, fragments, backslashes and a leading `//` fail before sending; the specification's targets match | `http_emitter.feature`: request targets; field failures keep their typed error | The specification's table exactly; `/v1/caf%C3%A9?city=Z%C3%BCrich&city=Bern`; ten targets rejected with operation `publish` and field `path` and never sent, including 8 KiB plus one byte, whitespace, `*` and a control character |
| 7. `200`, `202` and `204` deliver; a stalled body neither delays completion nor duplicates; malformed or excessive final headers fail even with `200` | `http_emitter_responses.feature`: valid headers deliver; a body is neither awaited nor read; invalid headers fail the attempt | The next record follows at once and each record is captured once; the client abandons a stalled and a 32 MiB body; 101 fields with `200`, a head of more than 417,792 bytes, the interim bounds, malformed framing, a malformed name and status `600` fail the attempt, stay reported while pending, and deliver once the receiver recovers |
| 8. A flush of several records delivers the first, receives `503` or `429` for the next, and retries only that request identically, including a generated header | `http_emitter_retries.feature`: a retry resends only unresolved requests; `http_emitter.feature`: a retried request repeats its generated header and body | Five captures: the retried request byte for byte at least three seconds later, the first record once, the later relay's record after the retried flush; a `uuid_v4()` header, a nonce and an encoding time repeated exactly |
| 9. Timeouts, connection loss, authentication failures and retryable statuses keep the work and expose a transient failure; `Retry-After` seconds and dates extend the delay, invalid values do not, and domain acceleration does not shorten waits | `http_emitter_responses.feature`: retryable responses reported while pending; `http_emitter_transport.feature`: retries after a timeout or a lost response, a pending timeout, a pending lost response, a name that does not resolve or refuses connections; `http_emitter_retries.feature`: `Retry-After` extends and adds nothing, backoff doubles, a paced domain; `http_emitter_inspection.feature` | `DESCRIBE EMITTER` reads each cause while pending: `authentication or authorization status 401`, `403` and `407`, `retryable status 408`, `425`, `429`, `500`, `503` and `599`, `timed out before complete final response headers`, `connection closed before complete final response headers`, `resolving 'api.nervix.test' failed: the name does not exist` and `Connection refused (os error 111)`; each retry repeats its request, later records wait, and the transient error clears once delivered; measured gaps honor `Retry-After` and the physical waits on a `TIME RATE 100` domain |
| 10. Terminal `400`, `404`, `409`, `413` and redirects of a client without `follow_redirects` route through the message-error route with safe diagnostics and the original branch; other records continue; `Location` is not followed | `http_emitter_responses.feature`: terminal responses; `http_emitter_retries.feature`: terminal with original input, state and body, and in interleaved branches; `http_emitter.feature`: interleaved branches of two relays | Code `external`, operation `publish` and `HTTP endpoint answered with status <n>` for `101`, `301`, `304`, `308`, `400`, `404`, `409` and `413`, with the input-only tenant, the captured state and the attempted body, in each record's branch; the next record is captured; the `Location` receiver captures nothing |
| 11. HTTPS trust, hostname verification and mounted client certificates follow the configuration; an invalid certificate never produces an acknowledged request | `http_emitter_transport.feature`: a mounted client certificate; a failed handshake; no client certificate | A request over mutual TLS with mounted files; `UnknownIssuer`, `certificate not valid for name` and `received fatal alert: CertificateRequired` reported while nothing is captured |
| 12. Interleaved branches and several source relays keep independent collection and request values, and error routes keep the exact branch | `http_emitter.feature`: interleaved branches; interleaved branches of two source relays; `http_emitter_transport.feature`: one emitter across its relays | Per-branch and per-relay request lines, headers and bodies behind `COLLECT FOR 200ms`; each refusal in its own branch with its own relay's value; never more than one request awaiting a response |
| 13. Create, inspect, formatting and `ALTER` keep expressions and body mode; invalid body-mode replacements leave the emitter unchanged; a replacement drains under its admitted configuration; a transactional replacement validates first | `http_emitter.feature`: body selections round-trip; `http_emitter_lifecycle.feature`: `ALTER` drains, a failed drain, a body-mode change and transactional replacement, quiesce levels, stopping an undrainable domain; `tools/plexus_format.feature`: HTTP emitters formatted | `SHOW CREATE` and `DESCRIBE` after `CREATE` and `ALTER`; the formatter's canonical text for both body modes; refused `SET TO`, `DROP ENCODE` and `SET BATCH` leave the emitter as it was; the held request completes at its first destination; two refused commits leave the original emitter delivering; the reported quiesce levels |
| 14. A graceful drain completes eligible requests; a forced ending leaves unresolved attached work to source recovery; a lost successful response causes the permitted duplicate | `http_emitter_lifecycle.feature`: graceful shutdown force-flushes; a shutdown deadline leaves work for redelivery; `http_emitter_retries.feature`: a lost response is sent again | The hourly flush publishes during the drain; after the drain deadline the held connection is released, the Kafka offset stays below the record, and after restart the same request is redelivered and committed; the applied record is captured twice, identically |

### Execution bounds

The qualification checks four execution bounds. The receiver observes the first three directly; the
fourth combines what the endpoint and the source observe.

| Bound | Evidence |
| --- | --- |
| One request awaits a response per execution, across its source relays and branches | The receiver's high-water mark of requests awaiting their final head stays at one while every response is delayed: in `One HTTP emitter waits across its source relays`, where the second relay's request arrives at least a second after the first, and in the composed scenario, where six requests of two relays and two branches each wait 100 ms for their answer. The regression `requests_awaiting_a_response_are_counted_until_their_final_head_begins` shows the same measure reading two for two concurrent clients |
| Response bodies are neither awaited nor buffered | The next record is sent while record 1's body stalls or holds 32 MiB, record 1 is captured once, and the receiver sees the client abandon the body. A 32 MiB body is many times what a loopback connection buffers, so the receiver finishes writing it only if the client reads it, and the client never does. The emitter's head reader keeps one bounded head buffer, whose limits `every_header_block_accepts_the_exact_field_limits`, `too_many_fields_and_invalid_framing_fail_before_status_classification` and `interim_headers_are_checked_separately_from_final_headers` pin |
| Resources are released after cancellation | The receiver sees the client abandon every timed-out attempt, the retry still held when the drain deadline forces the node's ending, and the request still held when `STOP` ends the domain. The Shuttle checks `shuttle_a_cancelled_attempt_leaves_each_member_to_resolve_once`, `shuttle_terminal_shutdown_leaves_an_unanswered_request_unacknowledged` and `shuttle_a_drain_never_finds_the_emitter_empty_while_a_member_is_retained` cover the production owner of cancelled attempts |
| Memory and backpressure stay under their owning limits | Later work waits: while record 1 keeps failing, `POST /events/2` is never captured, and a later relay's record follows the retried flush. An attached Kafka source keeps its offset while a request is pending and commits once it resolves, in the attached, `ON GENERAL ERROR IGNORE`, shutdown-deadline and stop scenarios. Each attempt is bounded by `timeout_ms`, by hyper's bounds on each response header block, 100 fields and 417,792 bytes over HTTP/1.1 and a header list under 16,384 bytes over HTTP/2, and by its own connection. A retained request is an ordinary allocation of the node, which the node's memory-pressure supervisor samples; the emitter publishes no memory figure of its own, so no per-emitter memory bound beyond its pending requests is claimed here |
