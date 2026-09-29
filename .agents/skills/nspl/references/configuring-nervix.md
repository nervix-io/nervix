# Configuring Nervix with NSPL

Use this reference to turn a deployment request into a complete Nervix configuration. Open the
public NSPL documentation index linked from `SKILL.md` and read its relevant Markdown entries for
exact syntax and connector-specific options. Examples here describe the configuration process,
not a second grammar.

## Contents

- [Public documentation routes](#public-documentation-routes)
- [Configuration decisions](#configuration-decisions)
- [Graph construction order](#graph-construction-order)
- [Choosing processing nodes](#choosing-processing-nodes)
- [Correctness checks](#correctness-checks)
- [Verification and troubleshooting](#verification-and-troubleshooting)

## Public documentation routes

Always read `NSPL Overview`. Add the indexed topics relevant to the requested graph:

| User need | Documentation index entry |
| --- | --- |
| Domain timing and lifecycle | `Domains And Time` |
| Administrative durability, storage errors, and recovery of uncertain commits | `Control Plane` → `Durability and recovery` |
| Internal/wire schemas, schema evolution, codecs, JAQ, Protobuf, and type mapping | `Schemas And Codecs` and `Control Plane` |
| Expressions, operators, casts, built-in functions, window aggregates, and approximate sketches | `Expression Functions` |
| Trusted Roto user-defined expression functions | `User-Defined Functions` |
| Roto language syntax for UDF bodies | `Roto Language Reference` |
| Branches, relays, capacity, TTL, and materialized state | `Relay` |
| Resources, uploads, mounts, and TLS files | `Resources` |
| Syslog wire schema, codec fields, UDP/TCP/TLS framing, clients, sources, and sinks | `Common` → `Syslog` |
| Source transports, delivery modes, headers, and ingestor routes | `Ingestors` |
| Typed batches applications publish through client sessions, producer outcomes, and limits | `Ingestors` → `Client Ingestors`, and `Sessions` → `Producers` |
| Junctions, deduplication, ordering, windows, inference, WASM, correlation, reingestion, and error routes | `Runtime Nodes` |
| Timed generation from materialized state | `NSPL Overview` and `Examples` |
| Sink transports, publishing modes, confirmation windows/timeouts, retry pacing, headers, direct values, flush/commit, and ACK behavior | `Emitters` |
| Runtime-node colocation, spreading preferences, path-gated rules, and domain placement defaults | `Placement Policies` and `Control Plane` |
| Hash maps and lookup expressions | `Lookups` |
| Session subscriptions and domain clock attachment | `Sessions`; `Command Line Client` for the CLI `subscribe` and `domain-clock` streams |
| Configuration backups, their archives, `DESCRIBE BACKUP`, and `RESTORE` | `Backup And Restore` |
| Metrics and runtime inspection | `Metrics And Observability` |
| Full graph examples | `Examples` |
| WASM guest ABI and output timing | `WASM Processor Guests` |
| Writing Rust WASM guests with the SDK | `Rust WASM Guest SDK` |

For transaction preview scopes, operation and step impact, actual engagement, and retained
inspection, read [Transaction Quiescence And Impact Inspection](https://docs.nervix.io/transaction-quiescence.html)
directly. It is an architecture chapter and is outside the curated NSPL index.

Prefer the narrow indexed topic over an old copied snippet. Do not leave the immutable version
selected by the documentation index when following related material.

## Configuration decisions

Capture these decisions before choosing syntax:

| Concern | Questions to answer |
| --- | --- |
| Domain | Is input paced by event time or admitted on arrival? What are period, skew, restart semantics, and the default placement policy? |
| Input contract | What sample payload and wire format arrive? Which fields are optional or sensitive? |
| Runtime record | What exact internal type and nullability does each field have? |
| Isolation | Which fields form the branch key? How long should inactive branches live? Is an instance cap required? |
| Source | Which connector/client, external entity, offset policy, delivery mode, ordering, timestamp source, and headers are required? Or does an application publish typed batches through a client ingestor, and with which acknowledgement window, ACK timeout, and retry backoff? |
| Processing | Which records are filtered, transformed, deduplicated, reordered, aggregated, correlated, inferred, enriched, or handled by a trusted Roto UDF? |
| State | Which relays are materialized? Should missing state wait, skip, or use a typed default? |
| Output | Which connector/sink, publishing mode, confirmation window/timeout, retry pacing, payload shape, codec or direct mapping, headers, and sensitivity leaks are required? |
| Placement | Which connected corridors need hard or preferred colocation, which should spread softly, and what rule ranks express precedence? |
| Operations | What input collection and output flush size/cadence, error behavior, TLS resources, metrics, and subscriptions are required? |

If the user supplied a real payload, derive wire and internal schemas field by field and call out
ambiguous types. Do not silently choose numeric width, datetime parsing, optionality, or branch
keys.

## Graph construction order

Use separate execution phases so transaction and active-domain rules stay clear.

1. **Domain bootstrap:** create one paced or unpaced domain, including its optional placement
   default, as its own server command. `CREATE DOMAIN` is never transaction content.
2. **Domain selection:** run `USE <domain>;` as a client-local command outside a transaction. A
   transaction cannot open until a selected domain exists.
3. **Resources:** create resource declarations, then upload local directories as separate client
   actions. Resources are domain-owned, so both act on the selected domain.
4. **Graph transaction:** wrap multiple queueable configuration statements in `BEGIN;` and
   `COMMIT;`. The transaction is bound to the selected domain and every queued statement must
   select it. A consecutive model-mutation run is one atomic candidate-graph update, including
   mixed `CREATE`, supported model `ALTER`, and `DROP`. Each statement is preflighted against the
   queued prefix without applying its effect; a rejection can be corrected before commit. Queued
   model mutations report their own preflighted quiesce levels, and `COMMIT` reports only the
   maximum level actually executed. `CREATE DOMAIN`, `CREATE USER`, other read-only statements,
   subscriptions, domain clock attachment, uploads, backups, restores, and node administration
   remain outside the transaction.
   `DESCRIBE TRANSACTION;` and `SHOW TRANSACTIONS;` run on their own beside an open transaction;
   they read impact or status without becoming content or shifting operation numbers.
5. **Lifecycle:** use `START`, `START AT ...`, or `STOP` against the active domain as intended. A
   paced `START` establishes one replicated clock generation that joining nodes install before
   execution. One committed authority revision identifies the producing node incarnation; owner
   changes preserve the mapping, `STOP` revokes the authority, and automatic ALTER quiescing keeps
   the generation and authority running.
   Run and monitor UTC synchronization on every cluster host. Per-node reads do not move backward
   within a generation, but simultaneous cross-host reads need not match; `SKEW` controls event
   admission and does not compensate for host-clock offset.
   Explicit start timestamps must fit the inclusive signed Unix-nanosecond range from
   `1677-09-21T00:12:43.145224192Z` through `2262-04-11T23:47:16.854775807Z`, and `TIME RATE` must
   be positive and finite.

Within the graph transaction, declare dependencies before consumers:

1. internal and branch-key schemas;
2. named branches;
3. wire schemas and codecs;
4. clients, protocols, vhosts/endpoints, hash maps, and UDF declarations;
5. relays, including materialized relays;
6. ingestors;
7. branch-preserving processors, generators, and reingestors;
8. emitters;
9. placement rules, after every runtime-node member they reference, including relays.

Resource upload paths, credentials, broker addresses, and external object names are deployment
inputs. Keep placeholders obvious and list provisioning that must happen outside Nervix.

## Choosing processing nodes

| Desired behavior | NSPL graph element |
| --- | --- |
| Decode an external feed and construct initial branches | `INGESTOR` |
| Accept typed batches an application publishes and construct initial branches | `INGESTOR ... FROM CLIENT SCHEMA` |
| Filter, transform, or fan out records without changing branch identity | `JUNCTION` |
| Suppress repeated keys for a time bound | `DEDUPLICATOR` |
| Order records by expressions within a time bound | `REORDERER` |
| Produce width/step aggregates | `WINDOW PROCESSOR` |
| Run an ONNX model | `INFERENCER` |
| Run custom guest processing | `WASM PROCESSOR` |
| Reuse trusted batch-column logic inside expressions | `UDF` |
| Match records from left and right relay sets | `CORRELATOR` |
| Change or remove branch grouping | `REINGESTOR` |
| Produce timed records from one materialized relay | `GENERATOR` |
| Publish records outside Nervix | `EMITTER` |
| Read a session-local filtered view | `CREATE SUBSCRIPTION` |
| Follow the active domain's clock from a client session | `ATTACH DOMAIN CLOCK` |

Use materialized relay dependencies when a node needs the latest record from another compatible
relay. Do not use them to scan across branches.

## Correctness checks

- Every referenced name is declared in the active domain before use.
- Every placement rule has non-empty `FROM` and `TO` sets whose members already exist and are
  schedulable runtime nodes. Every relay is eligible and participates in corridor coverage,
  whether or not it has materialized state. Treat coverage as path-gated, allow a valid
  zero-effect rule, use lower `RANK` numbers for stronger claims, and never invent hard separation.
- Endpoint and Syslog ingestors follow live cluster membership because their listeners execute on
  every cluster node. Every client-source ingestor, including an outbound WebSocket client, keeps
  its existing live primary and replicas through ordinary schedule recomputation.
- Every internal schema and every declared JSON, CBOR, or AVRO wire schema is non-empty; types and
  optionality match exactly. Declared wire formats are separate entity kinds even when their names
  coincide. They declare `MODE STRICT|LOOSE` after their names, and a mode-only change uses `ALTER
  WIRE <format> SCHEMA <wire_schema> MODE STRICT|LOOSE`. SYSLOG is a predefined singleton wire
  schema referenced directly with `FROM SYSLOG`; it has no name or model lifecycle.
- Every codec explicitly handles any wire/internal datetime or shape difference. Every JAQ-backed
  codec uses `WITH JAQ TRANSFORMATIONS` and declares `ON INGESTION`, `ON EMITTING`, or both in that
  order; `ON EMITTING BATCH` may follow `ON EMITTING` and yields exactly one value per batch. Every `ON INGESTION` output is an object that fits the internal schema, and a payload that
  unfolds into several messages is decoded, acknowledged, and redelivered as a whole.
- Every codec using the SYSLOG wire schema uses `FROM SYSLOG` and only the exact fixed fields
  documented in `Common` → `Syslog`; keep the format separate from the `TYPE SYSLOG` transport,
  use only `NO_ACK` source/sink modes, and configure TLS identity and framing for the client
  direction that consumes it.
- Every relay declares a schema and explicit branch selection. Its `CAPACITY` is the cluster-wide
  owner-buffer bound, not a per-branch or per-consumer bound; nonowner producer and remote consumer
  nodes each have one additional fixed dispatch slot.
- Every ordinary processor input/output uses the same named branch, or all are unbranched.
- Every multi-input emitter source declares the same payload schema. Its sources may use different
  branch names, but each source retains its own branch through collection and external publish;
  node-wide materialized dependencies and message-error relays match every source branch exactly.
- Every emitter sink declares its transport-supported `MODE` in the documented position and
  supplies the complete retry policy plus the confirmation window and timeout when that mode
  confirms asynchronously. No operational mode variable is inferred.
- ClickHouse, Postgres, MySQL, and MongoDB emitters declare `BATCH MAX MESSAGES <n> MAX SIZE
  <bytes>` before `FLUSH`; any other emitter may, with `MAX MESSAGES` from 1 to 65,536, a positive
  whole-unit `MAX SIZE`, at most `256KiB` for SQS, `ON EMITTING BATCH` in a batching Sentry
  emitter's codec, and `BATCH MESSAGE` in a batching emitter's protobuf codec. A batching Kafka,
  Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, Sentry or Syslog emitter publishes each run of
  compatible records from successive Arrow carriers in one flush, one source relay and one exact
  branch (or unbranched source) as one container (array, TOML `batch` key, XML `batch`
  root, protobuf `BATCH MESSAGE`, one syslog frame) or as its codec's `ON EMITTING BATCH` value, so
  consumers must read that container. `MAX SIZE` is the exact encoded payload length, including
  escaping, the container and any transformation expansion; an oversize candidate is halved, and a
  record that alone exceeds it goes to `ON MESSAGE ERROR` as a `validation` error, so leave headroom
  for the largest record rather than sizing it to a typical one. A failing `ON EMITTING BATCH`
  rejects every member of that batch with one shared error reference. A payload whose outcome is
  unknown is retried with the same bytes and members, so consumers deduplicating a retry see a
  whole repeated batch, never a regrouped one. Keep `MAX SIZE` below the destination's own message
  limit with room for the key, headers or attributes written around the payload: a batch message
  over a limit the client can see (Kafka, MQTT, NATS, Pulsar, SQS) is rejected with every member as
  an `external` `publish` error, as is one a Pulsar topic's own `maxMessageSize` refuses under
  `MODE ACK` and one whose body RabbitMQ refuses as larger than its `max_message_size`. RabbitMQ
  counts the body alone, so a `MAX SIZE` no larger than `max_message_size` suffices there; see
  [Emitters](../../../docs/src/emitters.md#broker-and-message-emitters).
  A ClickHouse, Postgres, MySQL or MongoDB emitter writes the rows of successive carriers of one
  relay and branch as inserts or bulk writes of at most `MAX MESSAGES` rows whose measured request —
  the `JSONEachRow` body, the statement text and bound values, or the member documents — is at most
  `MAX SIZE`; a row that alone exceeds it follows `ON MESSAGE ERROR` as a `validation` error. MySQL
  also caps each insert at the rows whose placeholders fit 65,535, and a MongoDB document above
  16 MiB is rejected before its write; see
  [Database writes](../../../docs/src/emitters.md#database-writes).
  A batching OTEL emitter bounds each export request by the number of successfully mapped source
  records and the exact uncompressed protobuf size, including resource and scope. An oversized
  candidate is divided and an oversized singleton follows `ON MESSAGE ERROR`; without the clause,
  OTEL keeps one export request per pending Arrow batch. With or without it, a request whose outcome
  is unknown, such as one that timed out or lost its response, is retried with the same bytes and
  records rather than prepared again. A batching Sentry emitter places its
  members inside one event in one envelope, and a `SYSLOG` codec places complete member messages
  in one frame's JSON-array `MSG`.
  SQS `.fifo` queue names and `FIFO GROUP` appear together, and `FIFO GROUP FROM BRANCH` is used
  only with branched input.
- Every MongoDB emitter maps integers that fit the BSON signed 64-bit range. A `U64` value above
  that range is rejected through `ON MESSAGE ERROR` instead of being written or used as an
  `ON CONFLICT` target, so map such a column to `STRING` when the full unsigned range must reach
  the collection.
- Every optional `COLLECT FOR` policy follows the complete relay input list, has a positive
  duration, and is absent when immediate input execution is intended. Correlator sides are checked
  independently; ingestors never declare input collection. Treat the duration as domain-logical
  and start it only on an empty-to-buffered source-and-branch collector transition.
- Every route constructs all required output fields. `INHERIT` appears only on a transforming
  route; set-only routes use explicit `SET` assignments.
- Every field scope is valid for its node: use documented `input`, `message`, `output`, `branch`,
  `left`, `right`, `relay_state`, `metadata`, `error`, and `partial_output` availability.
- Every `IF` condition and searched `CASE WHEN` condition is Boolean; simple `CASE` match values
  have the operand's exact type; all result arms have one exact type.
- Every UDF call uses `udf::<name>(...)` and has the declaration's exact arity and argument types.
  UDFs using the domain clock or randomness declare `VOLATILE`; untrusted third-party code remains
  in a WASM processor.
- Every agent-generated UDF includes Roto `test` blocks. Those tests run during `CREATE UDF` and
  must pass before the declaration is persisted.
- Every flush-based route has a flush policy and every route has a message error policy. Treat
  `FLUSH EACH` as a branch-local domain-logical duration and `FLUSH IMMEDIATE` as a branch-local
  physical 100 µs minimum; start either only when its route buffer changes from empty to non-empty.
- Treat an emitter's `FLUSH EACH` and the Iceberg `COMMIT EACH` as domain-logical, and its publish
  retry backoff, acknowledgement keepalive, sink acknowledgement timeout, stop and drain deadlines,
  and any server-supplied HTTP `Retry-After` as physical. A failed attempt keeps the pending
  batches, their acknowledgements, and the unchanged cadence; never describe a paced domain as
  shortening or lengthening a real retry wait.
- Every `MAX BATCH SIZE` is chosen as a logical Arrow payload boundary, excluding unused buffer
  capacity and object overhead. Delivery-mode `MAX <n>` appears only on `ACK PARALLEL`, never on
  `NO_ACK`.
- Every Kafka client states the required `auto.offset.reset` policy explicitly when a new consumer
  group may need records that already exist; Nervix passes the setting through and does not supply
  a hidden default.
- Every transport ingestor source ends with its documented `ON QUIESCE` body immediately before
  `DECODE USING`, with a positive `MAX SIZE`, explicit non-endpoint overflow policy, or endpoint
  `RETRY AFTER` wherever that mode requires it. MQTT `SUSPEND` also declares `SESSION PERSISTENT
  QOS 1`. Do not mix mode bodies between source types or infer a default.
- Every client ingestor declares `FROM CLIENT SCHEMA <schema> MODE ACK SEQUENTIAL|ACK PARALLEL MAX
  <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d> ON QUIESCE SUSPEND` and nothing else as its
  source: no `CREATE CLIENT`, codec, `DECODE USING`, headers, or `NO_ACK`. The schema is the exact
  contract producers declare, including optionality and sensitivity, and a paced domain requires
  its timestamp source. Changing its schema, mode, timestamp, filter, routes other than `FLUSH`, or
  branch declarations ends attached producers. A row that fails on a route follows that route's
  `ON MESSAGE ERROR` policy, so under `LOG` its whole batch fails processing as `rejected`.
- HTTP `EVERY`, Prometheus `EVERY`, and generator `EACH` use domain-logical cadence. HTTP and
  generators run once immediately; Prometheus first runs after one interval. Keep later work on
  the original schedule, coalesce missed periods without a catch-up burst, query Prometheus at the
  due instant, and use a fresh execution snapshot for returned data and generated routes.
- Treat Kafka emitter success as local librdkafka producer-queue admission. Even in `ATTACHED`
  mode, Nervix does not wait for a broker delivery receipt before completing its ACK share.
- Every Sentry emitter references a `TYPE SENTRY` client with a project DSN, encodes one event JSON
  object per record, and has no `write_header` invocation.
- Every custom WASM guest is built for the current ABI, accepts
  `nervix_process_batch(ptr, size)`, validates that exact range against its reusable buffer, and
  declares positive `MAX FUEL` then `MAX MEMORY` limits immediately after `FILE`. Its
  Rust `nervix-wasm-sdk` `Processor` callbacks return `error_stack::Result<_, GuestError>`;
  follow the `Rust WASM Guest SDK` chapter for the callback contract. Its
  `nervix_dump_state` saves only durable computation state, never buffered input, ACK tokens,
  pending output, timeout handles, or latched error state, and reports a failed save with a
  negative code, after which Nervix keeps the state saved last. Its `nervix_load_state` rejects
  unusable saved state only with the reserved `-7` or `-8` codes; read a WASM failure by its
  `<stage> failed` diagnostic, and treat only `snapshot envelope decoding` and `application state
  restoration` as a verdict on the saved state, which Nervix keeps unless the processor declares
  `ON REJECTED STATE RESET` and thereby spends that lifetime's single recovery attempt. Owner loss
  without a surviving checkpoint of the current state generation, or whose recovery the new owner
  cannot prepare, resets the affected branches; a returning former owner or stale replica never
  restores the state of a replaced generation. Treat a WASM processor's input
  acknowledgement as released only after the guest-state checkpoint covering it reached the owner's
  stable storage and every replica the schedule assigns, unless the processor is `DETACHED`, whose
  input relay fan-out acknowledges upstream; a failed checkpoint negatively
  acknowledges its inputs and recreates the guest from the last completed checkpoint. Output is
  dispatched before its checkpoint completes, so a redelivered input can emit again: the path stays
  at least once, and a guest that must not double-count redelivered input has to recognize it.
- Paced ingestors declare their timestamp source.
- External sensitive values use the required explicit leakage operation.
- Transactions queue only the bound domain's replicated configuration statements. Commit progress
  survives leader failover, while only consecutive model-mutation runs receive atomic
  candidate-graph validation and persistence; `CREATE DOMAIN`, `CREATE USER`, read-only, and
  session/client-local commands remain outside. Placement changes that move running owners use an
  effective `ENTITY_PAUSE`, and `COMMIT` also reports the total planned relocations.
- Interdependent schema evolution is one transaction, preserves ALTER operation order, and includes
  all wire schema, internal schema, codec, and dependent-node mutations needed by the new graph.
  Expect it to recreate the runtime state laid out by the altered schemas, such as deduplicator
  keys, windows, materialized records, and WASM guest state; domain-owned Kafka offsets and node
  metric summaries carry over.
- Model-alteration entity holds, domain pauses, and memory-pressure quiescing consult the
  ingestor's mode. Planned drain, placement relocation, and explicit `RELOCATE` ignore that mode:
  they stop new intake only for moved ingestors, drain already admitted ACK work, then switch
  ownership. Graceful shutdown ignores it too: after moving what it can, the terminating node stops
  intake on all of its ingestors and completes already admitted work in place, even when no
  replacement node exists. Stop and drop terminate the source session; unexpected owner loss uses
  immediate failover. Do not emit `PAUSE` or `RESUME` syntax.
- `RELOCATE <selection> ONTO NODE <node_id> FOLLOW PREFERENCES | IGNORE PREFERENCES [FOR <kind>
  <name> ...];` moves a selected subgraph onto a named cluster node as one atomic gated handoff.
  The selection is a kind-qualified list or a `FROM ... TO ...` corridor, `REQUIRE COLOCATION`
  groups always move whole, and the statement is immediate, non-transaction content that is
  mutually exclusive with model changes and `DRAIN NODE` in the same domain. It is a one-time move,
  not a pin.
- External entities and resource contents are provisioned before the graph is started.

## Verification and troubleshooting

Choose checks relevant to the configured graph:

- `SHOW CREATE <kind> <name>;` confirms the stored canonical definition.
- `DESCRIBE RELAY <relay>;` reports the relay owner and optional materialized-state replicas;
  `DESCRIBE RELAY <relay> WHERE (...);` is owner-authoritative for concrete branch state.
- `SHOW RELAY <relay> MATERIALIZED STATE;` inspects materialized data and placement.
- `DESCRIBE INGESTOR`, `DESCRIBE JUNCTION`, other processor-specific `DESCRIBE` commands, and
  `DESCRIBE EMITTER` inspect runtime state and edge metrics. `SHOW INGESTORS;` lists every ingestor
  with its owner and state, and a client ingestor's admission, producers, outstanding batches and
  bytes, and admitted batches.
- The observability server's `/metrics` endpoint reports raw graph-edge counters and histograms,
  including batch-size resolution for tuning collection and flush boundaries. Read `Metrics And
  Observability` for the current histogram buckets. The endpoint also reports
  `nervix_branch_instances` per domain, branch declaration, and physical node, plus
  `nervix_branch_evictions_total` split by `reason="lru"` or `reason="ttl"`.
- `DESCRIBE RESOURCE` confirms uploads and reports `latest`, the completed version `VERSION LATEST`
  would bind now; `SHOW CREATE` shows the version each existing binding stores.
- `BACKUP CLUSTER TO '<file>';` or `BACKUP DOMAIN [<name>] TO '<file>' [WITHOUT RESOURCES]
  [WITHOUT STATE | WITHOUT PAUSE | TIMEOUT <duration>];` writes an archive on the client's machine,
  sent alone from `nervix-cli` or a native client. A normal backup quiesces each running domain
  before capturing WASM guest state, Kafka domain source offsets, and branch lifecycle. `WITHOUT
  PAUSE` reads published checkpoints while execution continues; `WITHOUT STATE` captures only
  configuration. `DESCRIBE BACKUP '<file>';` verifies one offline and inventories its state,
  domains, users, and resource versions. Treat an archive as a secret.
- `RESTORE CLUSTER FROM '<file>' [ON EXISTING USER FAIL | SKIP | REPLACE] [DRY RUN]
  [WITHOUT STATE | WITHOUT SOURCE OFFSETS];` or
  `RESTORE DOMAIN <name> [AS <new_name>] FROM '<file>' [DRY RUN]
  [WITHOUT STATE | WITHOUT SOURCE OFFSETS];` recreates users, domains,
  resource versions under their archived numbers, models, and compatible runtime state from an archive, sent alone from
  `nervix-cli` or a native client. Restored domains are stopped; a domain name that exists is
  refused, so copy a domain with `AS`. A fresh cluster already has its bootstrap user, so a cluster
  restore there needs `ON EXISTING USER SKIP` or `REPLACE`. Run `DRY RUN` first to see the plan and
  each domain's impact report without changing anything.
- `SHOW UDFS`, `DESCRIBE UDF <name>`, and `SHOW CREATE UDF <name>` inspect trusted Roto functions.
  Creation itself is the test gate: a rejecting Roto `test` block prevents persistence.
- `SHOW PLACEMENTS`, `DESCRIBE PLACEMENT <name>`, `SHOW CREATE PLACEMENT <name>`, and
  `DESCRIBE DOMAIN` inspect placement coverage, precedence, effective colocation groups, hosts, and
  the domain default.
- `DESCRIBE RELOCATION ...;` shows the unit, quiesce level, gated relays, corridor coverage, and
  unsatisfied preferences a `RELOCATE` with the same clauses would execute, without moving
  anything.
- `LOOKUP <hash_map> KEY '<key>';` checks a loaded lookup.
- `CREATE SUBSCRIPTION ...` checks live relay output without modifying the graph. A subscription
  ends when its relay is redefined or removed; create it again to read the current definition. In
  the web console the tab turns ended and its resubscribe button does this under the same name.
- `nervix-cli --domain <domain> subscribe <name> <relay>` streams subscription rows to a shell.
- `ATTACH DOMAIN CLOCK;` shows the active domain's `START` generation and clock state, including a
  paced domain's committed origin, UTC anchor, and rate, then reports each state change and newest
  accepted tick until `DETACH DOMAIN CLOCK;`. Use it when paced ingestion rejects `TIMESTAMP AT`
  values: the mapping and tick frontier show which logical centers the admission window has reached.
  A node that is still starting answers it once it has installed the committed domains, so a
  `does not exist` refusal means the cluster has no such domain, not that the node restarted.
- `nervix-cli --domain <domain> domain-clock` follows that clock from a shell until Ctrl-C,
  printing the attach reply, state changes, and tick lines.
- A host using the shared C binding executes attach and detach through `nx_session_execute` and
  reads later states and ticks with `nx_session_next_clock_event`; the binding does not expose the
  initial state from the attach reply as a typed outcome.
- `SHOW CLUSTER STATUS;` checks cluster topology before diagnosing a graph as unavailable.
- `SHOW TRANSACTIONS;` checks open/committing progress and retained commit, revert, failure, or
  expiry outcomes.

For a parse error, follow the reported expected token and compare clause order with the relevant
public example. For a validation error, trace exact types, declaration order, domain ownership,
branch compatibility, construction completeness, and connector capabilities. For missing data,
check domain lifecycle, source offsets, timestamps, filters, branch keys, route filters, flush
boundaries, input collection boundaries, and external entity provisioning in that order.
