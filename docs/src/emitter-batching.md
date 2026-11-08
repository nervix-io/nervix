# Emitter Batching

Nervix batching gives one external publication an explicit source-record count and encoded-byte
limit. The emitter host owns selection, membership and acknowledgements; a connector owns the
external representation and its transport's answer. Arrow carriers remain the retained data
plane throughout preparation and retry.

This chapter owns packing, measurement, retained membership and the interpretation of batching
evidence. [Emitters](./emitters.md#batching) owns the Plexus clauses and per-entity configuration;
[Schemas And Codecs](./schemas-and-codecs.md#batch-containers) owns each codec's value and container.
[Connector Crates And The Connector Contract](./connector-contract.md) owns the host/engine boundary.
The time, concurrency and shutdown contracts remain with [Domain Clock](./domain-clock.md),
[Data-Plane Concurrency](./data-plane-concurrency.md) and [Shutdown And Recovery](./shutdown.md).

## Independent boundaries

An Arrow batch, an external payload, a driver request and a commit are different units:

| Boundary | What it controls | What it measures |
| --- | --- | --- |
| `COLLECT FOR` | Optional input gathering before expression execution | A separate source relay and concrete branch's incoming Arrow work |
| `FLUSH EACH ... MAX BATCH SIZE ...` or `FLUSH IMMEDIATE` | Release of constructed output to publication | Logical cadence and Arrow memory; immediate flush has its documented physical 100 µs minimum window |
| `BATCH MAX MESSAGES ... MAX SIZE ...` | One external container or the entity's declared native write/file | Eligible source records and exact encoded bytes at that entity's boundary |
| Publishing `MODE` | The external completion point, confirmation window and retry policy | Payload confirmations or the entity's native per-record answers |
| Driver grouping | Several publications submitted in one request | The driver's protocol count and byte limits |
| Iceberg `COMMIT EACH ... MAX SIZE ...` | Publication of staged data files in one catalog commit | Logical staging age and staged Arrow IPC bytes |

Batching adds no collection timer and promises no minimum fill. A flush publishes a partial
candidate. An empty selection emits nothing; a single member keeps its batch container, including
array brackets. A smaller byte limit can produce fewer members than `MAX MESSAGES` allows.

`MAX MESSAGES` is positive and no greater than the committed `MAX_BATCH_MESSAGES` cap, whose default
is 65,536. `MAX SIZE` is a positive whole-byte quantity. Both are required when a batching clause
is present. Neither derives from the other's value or from an Arrow-memory threshold. The
vocabulary's entity declaration decides whether the clause is required, optional or unavailable;
grammar, validation, plans, completion and inspection consume that same declaration.

### Current entity coverage

The original batching qualification covered all sixteen external integration families. The current
surface also includes HTTP requests and native client consumers. The entity rows below distinguish
their actual external units; a multi-message driver call does not turn independent messages into
one payload.

| Entity | `BATCH` | One bounded publication |
| --- | --- | --- |
| `KAFKA TOPIC` | Optional | One Kafka record value |
| `PULSAR TOPIC` | Optional | One Pulsar message payload |
| `RABBITMQ QUEUE` | Optional | One AMQP message body to the queue |
| `RABBITMQ EXCHANGE` | Optional | One AMQP message body to the exchange |
| `REDIS PUBSUB` | Optional | One channel publication |
| `MQTT TOPIC` | Optional | One MQTT `PUBLISH` payload |
| `NATS SUBJECT` | Optional | One Core NATS message |
| `NATS STREAM` | Optional | One JetStream message |
| `ZEROMQ PUSH` | Optional | One single-frame message |
| `SQS QUEUE` | Optional | One SQS message body, also bounded by the declared 1 MiB maximum |
| `SYSLOG COLLECTOR` | Optional | One encoded payload/frame |
| `SENTRY EVENT` | Optional; requires a batch transformation | One event in its one-event Sentry envelope |
| `OTEL LOGS` | Optional | One uncompressed protobuf Export request |
| `OTEL TRACES` | Optional | One uncompressed protobuf Export request |
| `OTEL METRICS` | Optional | One uncompressed protobuf Export request |
| `CLICKHOUSE TABLE` | Required | One insert's uncompressed `JSONEachRow` body |
| `POSTGRES TABLE` | Required | One insert's statement text and encoded bound arrays |
| `MYSQL TABLE` | Required | One insert's statement text and encoded bound values |
| `MONGODB COLLECTION` | Required | The inserted documents or upsert filter/update documents of one write |
| `ICEBERG TABLE` | Optional | One complete Parquet data file |
| `CLIENT CONSUMER` | Required | One Arrow IPC stream delivered to an application consumer |
| `HTTP REQUEST` | Unavailable | One request per source record; collection and flush still apply |

Omitting an optional clause preserves that entity's ordinary publication shape. Message emitters
keep one payload per eligible record; OTEL keeps an Export request per pending Arrow carrier;
Iceberg keeps its ordinary Parquet rollover target. Native driver grouping and commit behavior
remain in force. Required database and client policies are explicit even when their count is one.
The generated [Connector Capabilities](./connector-capabilities.md) reference owns the complete
per-entity modes, declared limits and reasons.

## From a validated policy to a payload

The language parses the clause into semantic Models. Registry validation checks the complete
candidate graph: entity legality, the cluster count cap, native maxima, codec batch capability,
exact types and sensitivity. Replacing a codec also revalidates its batching consumers. The
decision layer lowers this once into the typed execution plan. The data plane executes the plan
without reading a Model or parsing executable Plexus; see [Execution Plans](./execution-plans.md).

The host evaluates filtering, construction, materialized dependencies and envelope expressions
under each carrier's accepted execution snapshot. It retains the Arc-backed Arrow columns and
addresses selected rows by carrier and row position. Temporary jaq member values belong only to
encoding; they are not a second retained record representation.

For codec-backed batching, the packer walks released carriers in arrival order and selected rows
in source order. A candidate can span successive carriers only while the source relay, exact
named and concrete branch, destination/key, ordered headers, ordering group and envelope agree.
A SYSLOG member must also share its header apart from timestamp. A rejection or incompatible
member seals the current run. The packer never skips a boundary to combine later compatible rows.
Input queues each retain their order, but different ready inputs can interleave at the emitter;
upstream request completion is not a common receive-order fence across ingestors.

Each carrier keeps its own error context, execution snapshot and source acknowledgements even
when one payload includes rows from several carriers. Explicit source event timestamps are
preserved. Batching cannot expose a sensitive field that construction has not explicitly leaked,
and the batch transform receives neither original rows nor materialized state nor headers.

### Codec values and containers

Without `ON EMITTING BATCH`, the codec writes its members using its format's container:

| Codec format | Container |
| --- | --- |
| `WIRE JSON`, JAQ JSON | One array, using that codec writer's whitespace and number formatting |
| `WIRE CBOR`, JAQ CBOR | One definite-length array |
| JAQ YAML | One document containing a sequence |
| `WIRE AVRO` | One array datum whose item schema is the codec's record schema |
| JAQ TOML | One document with the `batch` key |
| JAQ XML | One `batch` root containing element-valued members |
| Protobuf | One declared `BATCH MESSAGE`, with its single repeated member field |
| SYSLOG | One RFC 5424 message using the common header and first timestamp, with a JSON array of the members' complete messages as `MSG` |

The [codec reference](./schemas-and-codecs.md#batch-containers) gives exact examples. Without a batch
transformation, a protobuf batch descriptor must have exactly one `repeated <MESSAGE>` field.
Descriptor validation happens before the graph runs.

A jaq-backed codec's `ON EMITTING` still produces one value per record. Its optional
`ON EMITTING BATCH` receives the ordered array of those member values and replaces the ordinary
container with exactly one format-valid value. It may return an array or an envelope. Its own
reported count cannot change membership, acknowledgement or the count limit. Sentry requires this
transformation because only the application can say how several records form one valid event.

For example, with `notification`, `notifications` and `kafka_main` already configured and the
destination topic provisioned externally:

```plexus
CREATE JAQ JSON CODEC notification_batch
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON EMITTING '.'
    ON EMITTING BATCH '{count: length, records: .}';

CREATE KAFKA TOPIC EMITTER batched_notifications
  FROM notifications
  TO kafka_main TOPIC 'notifications_out'
    MODE ACK PARALLEL MAX 100 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_batch
  INHERIT ALL
  BATCH MAX MESSAGES 500 MAX SIZE 1MiB
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

This configuration can have up to 100 payloads awaiting broker confirmation. Each payload contains
at most 500 source records and at most 1 MiB of encoded JSON, including its envelope. The separate
1 MiB flush threshold measures Arrow memory. The broker's own limit must still leave room for
the key, headers and record overhead.

## Exact bytes and bounded subdivision

Current codec batching measures by encoding. Its writer refuses the first write beyond `MAX SIZE`
and does not build an entire oversized payload. Escaping, UTF-8, field names, separators, base64,
length prefixes and the outer container all count. A completed payload of exactly the limit fits.
The recorded size bound is that exact length, not a pre-transform Arrow estimate.

An oversized candidate is halved. Its first half is encoded again and its remainder returns to
the front of the queue. Every resulting candidate is measured independently: arbitrary jaq output
can grow when the input shrinks. No monotonic-size assumption or search over presumed fitting
prefixes is valid. Finding a fitting first portion or an oversized singleton takes at most
`ceil(log2(n)) + 1` encodings along that subdivision path; this is not a bound on all encodings
needed to exhaust an entire run. A singleton is refused only after its actual bounded encoding
still cannot fit.

The count limit bounds prepared members in a candidate and the byte limit bounds encoded output.
They do not bound arbitrary jaq working memory or execution time. Transform-backed preparation
and re-encoding use the node's extension workers through the bounded executor. Admission refusal
judges no record: the emitter retains its rows and retries, preserving backpressure. See
[Execution Primitives](./execution-primitives.md) and the
[jaq execution contract](./schemas-and-codecs.md#codecs).

### Native measurement and protocol limits

Database, client, OTEL and Iceberg publications use their declared native representation rather
than a codec array. The [database measurement table](./emitters.md#database-writes) defines the
included statement, parameter and document bytes and the framing excluded from `MAX SIZE`.
ClickHouse measures before LZ4 compression; Postgres includes encoded arrays; MySQL includes
encoded values; MongoDB includes generated `_id` values where applicable. A value that is itself
an array remains one column or document field. No destination schema is rewritten for batching.

Connector protocol limits apply in addition to the user's count and byte limits:

- MySQL narrows a write to at most 65,535 placeholders; Postgres refuses a declared size beyond
  its 1,073,741,822-byte protocol bound. MongoDB rejects a document beyond 16 MiB and its driver
  can divide a larger logical write into protocol commands.
- Kafka, MQTT, NATS, Pulsar and SQS count their documented metadata around the payload when checking
  destination limits. RabbitMQ's body limit is learned from the broker's refusal, which applies
  to every member. Syslog also checks transport framing constraints. The exact boundaries are in
  [Broker and message emitters](./emitters.md#broker-and-message-emitters).
- SQS `SEND BATCH` groups up to ten independent message bodies into one service request, within
  the request's byte limit and with at most one message per FIFO group. Each entry can itself be
  a batching payload; entry outcomes apply to its members.
- OTEL counts successfully mapped source records and uncompressed protobuf Export bytes. It never
  combines two pending Arrow carriers into one request, even within a flush. Its prepared request
  bytes, including observation timestamps, survive an uncertain retry unchanged.
- A native client consumer gets one bounded Arrow IPC stream with retained membership and a
  revocable application-ACK attempt. The [client architecture](./client-io-architecture.md) owns
  credits, forwarding, reassignment and the absence of a durable consumer cursor.
- Iceberg measures the complete Parquet candidate, footer included, against the loaded table schema
  before staging. File row count and actual bytes are checked before catalog append. A schema
  change that invalidates a staged segment fails the commit and retains the rows for retry.

Iceberg's flush, data-file and catalog-commit boundaries remain separate. One temporary Arrow IPC
staging file gathers rows until a commit begins; rows staged during that commit start the next
file. The commit age starts with its first staged rows and later flushes do not postpone it.
`COMMIT ... MAX SIZE` counts staged IPC bytes, whereas `BATCH ... MAX SIZE` bounds each Parquet
file. Staging is not acknowledgement. [Iceberg staging and commits](./emitters.md#iceberg-staging-and-commits)
owns rollover, memory charging, catalog retries and orphan-file behavior.

## Membership, outcomes and retry

Each selected row has one terminal accounting outcome: a member of a delivered publication, a
definitive rejection, or unresolved work retained for another attempt. Host-owned membership maps
those answers back to the original carrier positions and their ACK shares. For an atomic payload,
one confirmation delivers all members and one rejection rejects all members. An ACK window counts
payloads; source-message totals still count records. A native partial outcome applies only to the
members the transport actually identifies.

| Failure | Attribution and next action |
| --- | --- |
| One member cannot be transformed or encoded | Reject that member through `ON MESSAGE ERROR`; seal its run and continue with neighbors |
| A singleton's bounded encoding cannot fit | Reject that member as `validation` / `encode` |
| The batch transformation fails, yields zero or multiple values, or writes an invalid format value | Reject the whole candidate with one shared error reference; do not subdivide to invent a culpable member |
| The destination definitively refuses a payload | Reject every included member with one shared external-publication error reference |
| A native row write reports row-specific refusal | Preserve identified successes; isolate rows where the native contract requires it and reject only refused members |
| The answer is missing or an infrastructure attempt fails | Keep unresolved membership and prepared bytes; apply physical retry backoff |

An ambiguous retry of a retained codec payload, Sentry envelope, OTEL request or client IPC stream
uses its original bytes and members. It does not rerun transforms, refresh observation timestamps,
regroup the payload, resend an already confirmed member or add later arrivals. Native row paths
that reconstruct a request retain their unresolved row set and accepted execution snapshot;
MongoDB's identified successes are excluded even when the write concern was not met.

`ATTACHED` keeps the source acknowledgement waiting until its downstream contract completes.
`DETACHED` retains its earlier relay-fan-out acknowledgement boundary while the emitter still
retries its output. Neither mode makes a lost external answer exactly once. Duplicate prevention
requires an external key, deduplication or conflict policy whose own contract covers the replay.
Errors report non-sensitive semantic context rather than copying a payload or arbitrary jaq error
text. [Errors And Diagnostics](./errors-and-diagnostics.md) owns that reporting boundary.

## Time, updates and recovery

Collection, flush and Iceberg commit cadences follow domain-logical time. Retry delays,
confirmation deadlines, keepalives and the shutdown budget use their physical owners. A partial
buffer or staged acknowledged work keeps source progress alive while it waits; an active retry
supplies its own keepalive. Retrying preserves each carrier's accepted execution snapshot, so
construction is not repeated under a later domain time.

A flush-only alteration is dynamic: pending Arrow work stays with its owner, the cadence changes
and the host receives a force-flush kick. Batching, codec, destination and other entity-policy
changes gate the emitter's sources, drain its collected and pending output, replace the task, and
release the gates. Source-membership changes need the topology-wide pause. The public
[alteration contract](./emitters.md#altering-emitters) and
[transaction impact contract](./transaction-quiescence.md) own the full classification and refusal
behavior. A failed drain is not permission to discard retained work and apply a new policy.

Force flush and graceful drain publish retained prepared bytes unchanged and attempt pending
commit work without waiting for a stopped domain clock. The host gives connector-held work its
finish deadline and reports incomplete output. Branch expiry or eviction withdraws that source
branch's unresolved host work without acknowledging it; it cannot revive when materialized state
later becomes ready. Rows already owned by a staging connector keep that connector's commit
lifecycle. Other branches retain their bytes and membership.

Prepared payloads, ACK maps and pending attempts are volatile. A process or owner loss can require
upstream replay and can duplicate a publication whose answer was lost. Model/execution snapshots
do not persist a prepared payload or turn local temporary staging into a durable delivery queue.
Planned relocation uses the existing drain/handoff contract; forced recovery keeps the transport's
declared delivery guarantees. [Shutdown And Recovery](./shutdown.md) remains the single authority
for shutdown phases and the behavior when its drain budget ends.

## Inspection and tuning

`SHOW CREATE EMITTER` retains the authored policy. `DESCRIBE EMITTER` reports `batch:`, the effective
`batch container:` and its owner-routed `destination:` observations. Use
[Emitter payload preparation](./metrics-and-observability.md#emitter-payload-preparation) and
[Emitter outcomes and lifecycle](./metrics-and-observability.md#emitter-outcomes-and-lifecycle)
for exact family names, labels and observation units:

- Payload and member counters measure successful preparation, before any external answer.
  Encoded bytes and size-bound bytes use the same exact measurement boundary. Prepared bytes can
  include additional content, such as a Sentry envelope.
- A retry of retained bytes adds attempts, not another payload preparation. A native row path
  that reconstructs its request observes its new preparation. Neither count alone proves delivery.
- Retry record totals count unresolved source records presented again, including the first forced
  flush or live drain of an already pending retry. Confirmed, rejected and staged members stay
  outside that total.
- Buffered, prepared, in-flight and staged work gauges overlap; summing them double-counts
  ownership. Gauge withdrawal follows owner teardown; counters retain process-local history.
- Sent source-record counts, destination outcomes, split counts and records-per-payload histograms
  explain whether low fill comes from byte limits, metadata boundaries or release cadence.

Choose count and byte ceilings from the consumer's accepted representation and destination limits
first, leaving room for transport metadata. Then choose a flush cadence from the latency budget.
Increase count to amortize external publication costs only while the byte cap, branch/envelope
fragmentation and cadence still permit useful fill. A longer flush can hold more memory and delay
output; it cannot fix an incompatible envelope or a destination's message limit. Batch-wide
transform errors also increase the number of records attributed to one failure.

### Measured observations

[Emitter Batching 13: Qualify all emitters, wire limits, failure handling and performance](https://app.clickup.com/t/86bc73jrt)
retains source-attributed qualification and raw measurements, delivered with
[PR #1253](https://github.com/nervix-io/nervix/pull/1253) at merged revision
`5e74b48c67676e18c00a68f82057d071dde5fa99`. The measured graph harness revision was
`211a11831bac336701e3d40d8992057cf6ceb4f5`, using the Debian AMD64 product image with content ID
`sha256:01850515f32296e8107a33411fd5cf6924f5903045e4ef7c90745ad5d29dacb7`.
Its graph exercise used one Nervix node, four Kafka
partitions, an external RabbitMQ destination, a backlog capped at 131,072 records, and one
30-second observation after ten seconds of warmup per configuration. Rates include drain and
exact consumer-visible ID/value verification. The narrow-record cases configured a 200-byte
`value` field; the wide case configured 4,096 bytes. The encoded record also carries its ID and
format overhead.

| Policy / flush | Records/s including drain | Mean records/RabbitMQ payload | Sample mean subject CPU cores | Sample maximum cgroup MiB | RabbitMQ publish p99 bucket upper bound, seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Omitted / 10 ms | 38,372 | 1.00 | 2.79 | 466.9 | 5.0 |
| Omitted / 100 ms | 41,526 | 1.00 | 2.82 | 580.6 | 1.0 |
| Count 1, size 64 KiB / 10 ms | 37,052 | 1.00 | 2.74 | 607.2 | 5.0 |
| Count 100, size 64 KiB / 10 ms | 282,173 | 99.71 | 4.76 | 582.9 | 0.1 |
| Count 1,000, size 512 KiB / 100 ms | 309,446 | 987.21 | 4.75 | 594.9 | 0.5 |
| Count 1,000, size 64 KiB, wide / 10 ms | 78,585 | 11.21 | 3.70 | 1,355.8 | 0.1 |

Larger containers amortized publication in this workload; the wide case reached its byte ceiling
well before its count ceiling. All six cases preserved exact output IDs and values, and aggregate
size-bound/encoded-byte ratios were 1.0. These are observations on one workload, not maximum rates,
a statistically established comparison, an end-to-end latency SLO or hard memory bounds. CPU
and cgroup measurements cover only the Nervix subject, excluding Kafka, RabbitMQ and the load
driver; cgroup memory includes cache. The latency values are finite histogram bucket boundaries
from in-window scrape deltas, not exact quantiles. Stage delivery latency is distinct from
whole-pipeline latency. The count-1,000 run's retained session gap is outside the sampled load
interval; whole-command wall time is not presented as a 30-second sample.

RabbitMQ component controls also separate enqueue-only completion from the channel answer that
production `NO ACK` requires. For 200-byte records and writes of 1, 100 and 1,000 records, the
original production means were 250.99, 1,600.76 and 22,478.85 µs per write. The corresponding
channel-answer controls were 92.16, 1,512.33 and 35,882.42 µs. A bounded singleton repeat measured
74.599 µs for production; both observations remain in the evidence. Variation and different
completion guarantees prevent treating enqueue timings, graph throughput and historical
cross-host rates as interchangeable performance claims.

Allocator observations are warmed whole-process jemalloc epoch deltas, not per-thread traces or
peaks. At 200 bytes, count-100 and count-1,000 writes reported 805 and 10,878 allocation requests;
at 4,096 bytes they reported 564 and 11,429. A reported singleton zero lies below counter/tcache
refresh granularity and does not establish allocation-free publication. Jemalloc retained virtual
mappings also do not measure buffered payload bytes. Preserve those distinctions when repeating
the [benchmark workflow](./developing-nervix.md) with a production-like graph.

## Verification boundaries

The qualification links every declared emitter entity and batching behavior to public external
observations. Exact-limit, singleton, empty, escaped/nested/null/bytes, sensitivity, metadata and
branch separation, partial answers, retained retries, ALTER, eviction and drain cases exercise
their production owners. Registered bounded properties cover encoding and complete membership;
production-owner Shuttle checks cover the exercised cancellation/acknowledgement interleavings.
Client conformance covers native application delivery. Paired Rustdoc examples establish specific
current API restrictions, not runtime delivery guarantees.

The qualification task retains the actual revisions and scopes of reused results, independent
property assertions, global discovery failures and linked investigations. In particular, an
ordinary property body passing is not a successful whole-inventory discovery/corpus campaign.
No unrun fuzz or Deloxide campaign is evidence, and the diagnostic detector observes only its
tracked blocking locks. [Property Testing And Fuzzing](./property-testing-and-fuzzing.md) and
[Data-Plane Concurrency](./data-plane-concurrency.md) own those methods. The measurements above
do not replace byte-bound properties or public exact-output assertions.
