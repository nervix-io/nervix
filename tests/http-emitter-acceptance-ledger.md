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
| The outbound HTTP sink in the connector crate | The connector-crate epic, qualified by [Connectors 14](https://app.clickup.com/t/86bc21vve) | The sink half of `crates/connectors/http`, which is source-only today; driver, header and response interpretation stay in the crate, and the host keeps lifecycle, retry cadence and acknowledgements | [05](https://app.clickup.com/t/86bc78nbh) |
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
| `crates/connectors/http` implements only the source contract | 05 |
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
| `Then HTTP receiver "<name>" eventually receives at least <n> requests` | Waits up to 60 seconds for the capture count. |
| `Then HTTP receiver "<name>" request <i> is` | Compares one captured request: request line, the named headers exactly, and the exact body. |
| `Then HTTP receiver "<name>" eventually records a failed TLS handshake` | Waits up to 60 seconds for a client to fail its handshake. |

Script lines cover every receiver behavior the specification's criteria depend on:

| Script line | Control |
| --- | --- |
| `respond <status>` | Any three-digit status, complete, with `Content-Length` except where the status carries no content |
| `; header <name>: <value>` | Response headers such as `Retry-After`, `Location` or `Set-Cookie` |
| `; body <text>` | A response body |
| `; after <duration>` | A delayed response, for timeouts measured against `timeout_ms` |
| `; interim <status>` | An interim response before the final one |
| `; stall body` | Complete successful headers, then a body that never finishes |
| `; extra headers <n>` | More response header fields than any bound, for excessive-header failures |
| `lose response` | The request is read in full and the connection closes without an answer: an applied request whose response is lost |
| `hold response` | The request is read in full and never answered: a physical timeout |
| `raw <bytes>` | Arbitrary bytes with `\r`, `\n` and `\\` escapes, for malformed framing and invalid statuses |

Two receivers in one scenario give an `ALTER` a second destination, so a scenario can prove that
admitted requests never move to the replacement.

The fixture is qualified two ways. `just test-harness-liveness http_receiver` runs focused
regressions on real loopback sockets: scripted order, lost and held responses, stalled bodies,
chunked request bodies, interim and raw responses, request bounds recorded as faults, mutual TLS,
hostname verification, and a stop that ends held connections inside its budget without forcing
them.
`tests/features/runtime/http_receiver.feature` drives the receiver with the HTTP client Nervix
already has, the polling ingestor's, over HTTP with a `503` then `200` status sequence, over
mutual TLS with the receiver's files mounted as a resource, and against a client that does not trust
the receiver, on one and three nodes.

The receiver speaks HTTP/1.1 only and offers only `http/1.1` over ALPN. The specification's header
rules are chosen to stay valid over HTTP/2 as well, but no criterion requires an HTTP/2 endpoint.

## Initial failing cases

`tests/features/runtime/http_emitter.feature` holds the two initial public cases, each on one and
three nodes:

- `An HTTP emitter sends each record with its own method, path, headers, and codec body` sends two
  records whose methods, targets, headers and bodies all differ, and compares both captured
  requests byte for byte.
- `An HTTP emitter declared without a body sends zero content bytes` sends a constant `DELETE` to a
  record-computed path with a declared header and no body.

Both carry `@http_emitter_expected_failure`, which the suite excludes unless tags are selected
explicitly, and both fail at the statement that creates the emitter, because the grammar has no
`TO HTTP` sink. On 24 September 2026 all four examples failed that way with `--retry 0`, each at
the `CREATE EMITTER` statement with this parse diagnostic, and each receiver stopped cleanly having
captured nothing:

```text
expected OTEL | CLICKHOUSE | POSTGRES | MYSQL | MONGODB | ICEBERG | KAFKA | PULSAR | RABBITMQ | REDIS | MQTT | NATS | ZEROMQ | SYSLOG | SQS | SENTRY, found HTTP
```

The same feature without a tag selection runs no scenario. They are the red half of the public evidence for criteria 1 and 2. HTTP Emitter 05
removes the tag once the first request reaches the receiver; until then, run them with:

```console
just test-scenarios --input tests/features/runtime/http_emitter.feature --tags @http_emitter_expected_failure
```

## Acceptance matrix

Every criterion runs through NSPL against the HTTP receiver on one and three nodes unless its row
names a topology. The owning task adds the criterion's scenarios red, turns them green, and keeps
them in the ordinary suite; HTTP Emitter 09 composes the complete matrix and closes it.

| Criterion | Public scenarios | Observable evidence | Receiver control | Owner |
| --- | --- | --- | --- | --- |
| 1. Constant and computed methods, paths and queries reach the receiver with exact codec bytes and headers, and two records choose different requests | `http_emitter.feature`: the dynamic request case above, plus a constant `METHOD 'POST'` and `PATH '/v1/events'` case | Captured request lines, header values and body bytes, differing per record | Capture, standing `204` | 04 prepares, 05 sends and removes the tag |
| 2. A bodyless request carries zero content bytes and its headers; `GET` and `HEAD` are accepted only without a body, including when computed | The bodyless case above; `GET` and `HEAD` bodyless cases; a codec emitter whose computed method is `GET` | Empty captured body; configuration rejection for a literal `GET` with a codec; a message error with operation `publish` and field `method` for a computed one | Capture | 02 grammar, 03 literal rejection, 04 computed rejection, 05 sends |
| 3. Request fields read the original input and the finalized output; a record filtered by route `WHERE` evaluates nothing and sends nothing | A codec emitter whose path reads `output` and whose header reads `input`; a route `WHERE` that filters one of two records whose request fields would fail | The captured values match the finalized output and the original input; the receiver captures one request and no message error is routed | Capture | 04 |
| 4. Invalid types, nullable expressions, unavailable scopes, wrong client types, invalid literals, a missing timeout and implicit sensitive leakage reject configuration; explicit leakage permits the value | Negative `CREATE` and `ALTER` cases for each rejection; one positive leakage case | The command fails with the owning node, route, operation and field, and quotes no value; the leaked value reaches the receiver | Capture for the positive case | 03 |
| 5. Header names compare without case, later writes replace earlier ones, invalid and reserved fields, CR/LF, edge whitespace and every envelope limit reject the record, and empty values and internal spaces stay valid | Records exercising each header rule and the 128-header and 32 KiB bounds | Replaced and empty values in captured requests; message errors with operation `invoke` and the invocation index for each rejection; no request for a rejected record | Capture | 04 |
| 6. Path normalization keeps encoded separators and query order, rejects the listed targets before sending, and matches the specification's target table | One record per row of the target table, plus fragment, backslash and 8 KiB cases | Exact captured targets for the accepted rows; message errors with operation `publish` and field `path` for the rejected ones, with no request | Capture | 04 |
| 7. `200`, `202` and `204` complete delivery; a stalled body after successful headers neither delays completion nor causes a duplicate; malformed or excessive final headers fail the attempt even with `200` | One case per success status; a stalled-body case followed by a second record; malformed and excessive header cases | The next record reaches the receiver while the stalled body is open; exactly one capture of the stalled record; a transient failure in `DESCRIBE EMITTER` for the bad framing, then delivery when the receiver recovers | `respond 202`, `respond 204`, `stall body`, `extra headers 129`, `raw` | 05 |
| 8. A flush of several records delivers the first, receives `503` or `429` for the next, and retries only that request with identical method, target, body and headers, including a generated header value | A three-record flush against `respond 200`, `respond 503`, then `respond 200`, with a header computed from a nondeterministic function | Four captures in total, the retried one byte-identical to its first attempt, the first record captured once | Scripted status sequence | 06 |
| 9. Timeout, connection loss, authentication failure and each retryable status keep the work and expose a transient failure; `Retry-After` seconds and dates extend the delay, invalid values do not, and domain acceleration does not shorten the waits | One case per retryable class; `Retry-After` in seconds, as a future date, as a past date and malformed; the same on a paced domain | A transient failure in `DESCRIBE EMITTER` while work is pending; the measured gap between two captured attempts at least the required delay, asserted as a delay rather than a silence | `hold response`, `lose response`, `respond 401`, `respond 408`, `respond 429`, `respond 503; header Retry-After: ...` | 05 classifies, 06 retries and paces |
| 10. Terminal `400`, `404`, `409`, `413` and redirects reach the message-error route with safe diagnostics and the original branch, other records continue, and no request follows `Location` | One case per terminal status and a redirect whose `Location` names a second receiver, on a branched relay | Error records on the route with code `external`, operation `publish` and the numeric status, in the source branch; the next record captured; the second receiver captures nothing | `respond 404`, `respond 301; header Location: ...`, a second receiver | 05 classifies, 06 routes |
| 11. HTTPS trust, hostname verification and mounted client certificates follow the configuration, and an invalid certificate never produces an acknowledged request | Mutual TLS with mounted receiver files; an untrusted receiver; a certificate for `localhost` dialed as `127.0.0.1` | Captures over mutual TLS; a failed handshake and a transient failure, with no capture and no acknowledgement, for the untrusted and mismatched cases | HTTPS receivers, client certificate required, `{{http_receiver_port.<name>}}` | 03 validates, 05 connects |
| 12. Interleaved branches and several eligible source relays keep independent collection and request values, and error routes keep the exact branch | Two concrete branches and two source relays interleaved, with one rejected record per branch | Per-branch captured values, and error records only in their own branch | Capture, `respond 404` | 04 prepares, 06 routes, 09 composes |
| 13. Create, inspect, formatting and `ALTER` keep expressions and body mode; invalid body-mode replacements leave the emitter unchanged; a replacement drains under the admitted configuration; a transactional replacement validates everything first | `SHOW CREATE`, `DESCRIBE` and formatting round trips for both body modes; invalid `SET TO`; `SET TO` with a held request to a second receiver; `DROP` and `CREATE` in one transaction | Round-tripped text; the unchanged emitter still delivering; the held request completing at the first receiver, not the second; transaction inspection | Two receivers, `hold response` | 02 grammar, 07 lifecycle, 08 inspection |
| 14. A graceful drain completes eligible requests; a forced ending leaves unresolved attached work to source recovery; a lost successful response causes the permitted duplicate | Graceful shutdown with requests pending; forced ending with a held request and an acknowledged source; `lose response` then `respond 200` | Captured requests before shutdown completes; the source redelivers after restart; the applied record captured twice | `lose response`, `hold response` | 06 duplicate, 07 drain and recovery |
