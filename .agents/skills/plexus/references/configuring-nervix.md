# Configuring Nervix with Plexus

Use this reference to turn a deployment request into a complete Nervix configuration. Open the
public Plexus documentation index linked from `SKILL.md` and read its relevant Markdown entries for
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

Always read `Plexus Overview`. Add the indexed topics relevant to the requested graph:

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
| Syslog wire schema, codec fields, UDP/TCP/TLS framing, per-connection versus total buffer bounds, clients, sources, and emitters | `Common` → `Syslog`, and `Connector Capabilities` |
| Source transports, delivery modes, headers, and ingestor routes | `Ingestors` |
| Client, ingestor and emitter heads, their connector/entity choice, and kind-preserving ALTER | `Plexus Overview` → `Statement catalog`, then `Connector Capabilities` and the entity's manual section |
| Kafka domain offsets across explicit starts, reconnects, instance changes and restored backups | `Ingestors` → `Kafka`, and `Backup And Restore` |
| Plexus entity modes, ordered options, typed metadata, generated client CONFIG key/type/confidentiality tables and directional differences | `Connector Capabilities`, then the entity's `Ingestors` or `Emitters` section |
| Typed batches applications publish through client sessions, producer outcomes, and limits | `Ingestors` → `Client Ingestors`, and `Sessions` → `Producers` |
| Constructed Arrow output applications receive and explicitly ACK, retry or reject | `Emitters` → `Client emitters`, `Sessions` → `Emitter Consumers`, and `Client Implementation Manual` → `Emitter Consumers` |
| Junctions, deduplication, ordering, windows, inference, WASM, correlation, reingestion, and error routes | `Runtime Nodes` |
| Timed generation from materialized state | `Plexus Overview` and `Examples` |
| Emitter transports, publishing modes, confirmation windows/timeouts, retry pacing, headers, direct values, flush/commit, and ACK behavior | `Emitters` |
| Emitter envelope clauses and `PROPERTIES`: timestamps, flags, delays, reply targets and message identities a transport carries beside each payload | `Emitters` → `Envelope clauses`, then `Connector Capabilities` |
| MongoDB collection names and the lookup before each write, BSON field-name admission, native widths/date precision, conflict filters and upserts, write-concern outcomes, client URL configuration, the client database, paired TLS identity files, pool waits and driver deadlines | `Emitters` → `MongoDB`, `Database Client Connection Pools`, then `Connector Capabilities` |
| MySQL table path parts, exact value bindings, duplicate-key actions, placeholder and packet limits, operation deadlines, TLS verification and identity file formats, URL lifetime settings and maintained pool minimums | `Emitters` → `MySQL`, `Database Client Connection Pools`, then `Connector Capabilities` → `MYSQL client` |
| Sentry platform options, event defaults and limit, batch transformations, strict DSN/timeout configuration, the inherited HTTP transport, rejected versus retried statuses and retry hints | `Emitters` → `Sentry`, then `Connector Capabilities` |
| Runtime-node colocation, spreading preferences, path-gated rules, and domain placement defaults | `Placement Policies` and `Control Plane` |
| Hash maps and lookup expressions | `Lookups` |
| Session subscriptions and domain clock attachment | `Sessions` |
| Configuration backups, their archives, bounded waits and exact-reference recovery, `DESCRIBE BACKUP`, and `RESTORE` | `Backup And Restore`; cut timeout phases and their elapsed measurements: `Backup And Restore Architecture` → `Observability` |
| Metrics and runtime inspection | `Metrics And Observability` |
| Full graph examples | `Examples` |
| Applications that pace producers and consumers by an attached domain clock, and the `TIMESTAMP AT`/`TIMESTAMP NOW` admission they meet | `Paced Simulation Drivers` and `Domains And Time` |
| WASM guest ABI and output timing | `WASM Processor Guests` |
| Writing Rust WASM guests with the SDK | `Rust WASM Guest SDK` |

For transaction preview scopes, operation and step impact, actual engagement, and retained
inspection, read [Transaction Quiescence And Impact Inspection](https://docs.nervix.io/transaction-quiescence.html)
directly. It is an architecture chapter and is outside the curated Plexus index.

For native application graphs, endpoint ownership, Arrow/credit limits, ACK boundaries, ALTER,
restoration and replay, read [Client Ingestors And Emitters](https://docs.nervix.io/client-io-architecture.html)
directly. Follow the same documentation version as the selected index; this architecture chapter
is outside the curated Plexus index.

For the CLI's `subscribe` and `domain-clock` shell streams and their line formats, read
[Command Line Client](https://docs.nervix.io/client-tools-cli.html) directly. Follow the same
documentation version as the selected index; this client chapter is outside the curated Plexus index.

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
| Output | Which connector or native client emitter, publishing mode, confirmation window/timeout, retry pacing, payload shape and size, codec or direct mapping, headers, sensitivity leaks, and application ACK boundary are required? |
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
   Losing the authority's node, including one that also leads consensus, moves the authority to
   another live voter without changing the mapping. Progress resumes once the replacement and each
   peer install the new fence, which they do ahead of graph installation and activation.
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

| Desired behavior | Plexus graph element |
| --- | --- |
| Decode an external feed and construct initial branches | `<CONNECTOR> <ENTITY> INGESTOR`, such as `KAFKA TOPIC INGESTOR` |
| Accept typed batches an application publishes and construct initial branches | `CLIENT PRODUCER INGESTOR ... FROM SCHEMA` |
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
| Publish records outside Nervix | `<CONNECTOR> <ENTITY> EMITTER`, selected from the declared pairs |
| Deliver constructed Arrow batches to competing application consumers | `CLIENT CONSUMER EMITTER ... TO SCHEMA` |
| Read a session-local filtered view | `CREATE SUBSCRIPTION` |
| Follow the active domain's clock from a client session | `ATTACH DOMAIN CLOCK` |

Use materialized relay dependencies when a node needs the latest record from another compatible
relay. Do not use them to scan across branches.

## Correctness checks

Choose each ingestor or emitter's complete connector/entity head from the current catalog, and
keep that pair in its ALTER head. A client head names its connector. Follow the selected entity's
declared reference kind and body; inspection and addressing use the Model kind and name. Read
`Plexus Overview` → `Statement catalog` for the forms.

Client CONFIG keys follow the generated schema: lowercase, case-sensitive, present once, required keys supplied and TLS identity files paired. Only declared driver namespaces accept extra property names. Treat SHOW CREATE as a confidential inspection projection, and backups as complete secret-bearing configuration. Offline CREATE checks need no broker; runtime initialization still owns remote checks.

For `ZEROMQ CLIENT`, use a `tcp://host:port` or `ipc://path` address. Set `'bind' = 'true'`
on a socket that listens for peers and `'bind' = 'false'` on a socket that connects; omission
also selects connect. The Boolean accepts those two values ignoring letter case. `ZEROMQ PULL`
and `ZEROMQ PUSH` both use `MODE NO ACK`; only the first received frame is ingested, with no
transport headers or peer metadata. Messages with no frames are skipped, while additional frames
are discarded. A PUSH publish sends one payload frame and has no receiver delivery receipt. For
TCP hostnames, the ZeroMQ driver uses the system resolver instead of the node resolver. Prefer a
bound PULL ingestor with connecting PUSH peers when a peer can restart: a connecting PULL socket
does not detect or recover a lost peer. Read the ZeroMQ sections of Ingestors and Emitters for the
current connection, retry and deadline limits.

- Every referenced name is declared in the active domain before use.
- Every placement rule has non-empty `FROM` and `TO` sets whose members already exist and are
  schedulable runtime nodes. Every relay is eligible and participates in corridor coverage,
  whether or not it has materialized state. Treat coverage as path-gated, allow a valid
  zero-effect rule, use lower `RANK` numbers for stronger claims, and never invent hard separation.
- `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors follow live cluster
  membership because their listeners execute on every cluster node. Every other ingestor, a
  `WEBSOCKETS CONNECTION` included, keeps its existing live primary and replicas through ordinary
  schedule recomputation.
- For `WEBSOCKETS CONNECTION` literal upgrade headers, subprotocol offers, frame metadata, strict client CONFIG,
  handshake capacity, message/frame limits, inbound idle bounds, recovery and graceful close, read `Ingestors` →
  `WEBSOCKETS CONNECTION` in the selected documentation index.
  A successful upgrade does not establish data delivery or reset repeated-failure backoff.
  Check the effective idle bound together with the ping cadence and the peer's inbound activity.
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
  documented in `Common` → `Syslog`; keep the format separate from the `SYSLOG CLIENT` transport,
  use only `MODE NO ACK` on a `SYSLOG LISTENER` ingestor and `NO ACK` on a syslog emitter,
  configure TLS identity for the client direction that consumes it, and set a key only one side
  reads, such as `framing` or `idle_timeout_ms`, only on a client that side uses; the SYSLOG
  client reference in `Connector Capabilities` names those keys.
- Every relay declares a schema and explicit branch selection. Its `CAPACITY` is the cluster-wide
  owner-buffer bound, not a per-branch or per-consumer bound; nonowner producer and remote consumer
  nodes each have one additional fixed dispatch slot.
  On native 64-bit targets, its positive integer range ends at `18446744073709551615`, and stored,
  replicated, and restarted definitions retain the exact accepted capacity.
- Every ordinary processor input/output uses the same named branch, or all are unbranched.
- Every multi-input emitter source declares the same payload schema. Its sources may use different
  branch names, but each source retains its own branch through collection and external publish;
  node-wide materialized dependencies and message-error relays match every source branch exactly.
- Every emitter declares its transport-supported `MODE` in the documented position and
  supplies the complete retry policy plus the confirmation window and timeout when that mode
  confirms asynchronously. No operational mode variable is inferred.
- Write an envelope clause or `PROPERTIES` key only where the emitter's entity declares it, after
  the payload and before the construction, in the documented order; see
  [Emitters → Envelope clauses](../../../../docs/src/emitters.md#envelope-clauses).
- For Pulsar receipt deadlines and the final wait after `NO ACK` acceptance, read
  [Emitters → Pulsar](../../../../docs/src/emitters.md#pulsar) and
  [Finishing an emitter](../../../../docs/src/connector-contract.md#finishing-an-emitter).
- A RabbitMQ queue ingestor uses automatic `MODE NO ACK`, prefetch-one `MODE ACK SEQUENTIAL`,
  or `MODE ACK PARALLEL MAX <n> BATCH TIMEOUT <d> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d>`.
  Parallel `MAX` is 1 through 65,535 per instance. ACK/NACK follows each delivery's own graph
  outcome; a rejection requeues that delivery. `NO ACK` disables broker prefetch and cannot
  recover a delivery already consumed before downstream completion. See
  [Ingestors → RabbitMQ](../../../../docs/src/ingestors.md#rabbitmq).
- A RabbitMQ emitter publishes to a queue through the default exchange with
  `RABBITMQ QUEUE EMITTER … TO <client> QUEUE '<queue>'`, or to a declared exchange with
  `RABBITMQ EXCHANGE EMITTER … TO <client> EXCHANGE '<exchange>' ROUTING KEY <expression>`, whose
  expression computes every record's key as an exact, non-null, non-sensitive `STRING`. Nervix never
  declares the exchange or its bindings, and an emitter whose exchange is missing stays unavailable
  until it exists. Without `MANDATORY` a message no binding routes is dropped by the broker and its
  record reported delivered; write `MANDATORY` after the payload, on either entity, to reject such a
  record through `ON MESSAGE ERROR` instead. See
  [Emitters → RabbitMQ](../../../../docs/src/emitters.md#rabbitmq).
- For Core NATS completion, reconnect retries and finish outcomes, read
  [Emitters → NATS](../../../../docs/src/emitters.md#nats) and
  [Finishing an emitter](../../../../docs/src/connector-contract.md#finishing-an-emitter).
- CLIENT, ClickHouse, Postgres, MySQL, and MongoDB emitters declare `BATCH MAX MESSAGES <n> MAX SIZE
  <bytes>` before `FLUSH`; other supported emitters may, with `MAX MESSAGES` from 1 to 65,536, a positive
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
  [Emitters](../../../../docs/src/emitters.md#broker-and-message-emitters).
  A ClickHouse, Postgres, MySQL or MongoDB emitter writes the rows of successive carriers of one
  relay and branch as inserts or bulk writes of at most `MAX MESSAGES` rows whose measured request —
  the `JSONEachRow` body, the statement text and bound values, or the member documents — is at most
  `MAX SIZE`; a row that alone exceeds it follows `ON MESSAGE ERROR` as a `validation` error. MySQL
  also caps each insert at the rows whose placeholders fit 65,535, and a MongoDB document above
  16 MiB is rejected before its write; see
  [Database writes](../../../../docs/src/emitters.md#database-writes).
  A batching OTEL emitter bounds each export request by the number of successfully mapped source
  records and the exact uncompressed protobuf size, including resource and scope. An oversized
  candidate is divided and an oversized singleton follows `ON MESSAGE ERROR`; without the clause,
  OTEL keeps one export request per pending Arrow batch. Unlike the database emitters, an OTEL
  request never combines records of two Arrow batches, with or without the clause. A receiver's
  HTTP `413` is not retried or divided, so keep `MAX SIZE` within the receiver's limit. With or
  without the clause, a request whose outcome is unknown, such as one that timed out or lost its
  response, is retried with the same bytes and records rather than prepared again. A batching
  Sentry emitter places its members inside one event in one envelope, and a `SYSLOG` codec places
  complete member messages in one frame's JSON-array `MSG`.
  SQS `.fifo` queue names and `FIFO GROUP` appear together, and `FIFO GROUP FROM BRANCH` is used
  only with branched input.
- Every MongoDB emitter maps integers that fit the BSON signed 64-bit range. A `U64` value above
  that range is rejected through `ON MESSAGE ERROR` instead of being written or used as an
  `ON CONFLICT` target, so map such a column to `STRING` when the full unsigned range must reach
  the collection.
  BSON `VALUES` keys cannot contain NUL. Mutual TLS uses paired `tls_cert_file` and `tls_key_file`
  entries alongside optional `tls_ca_file`; any of these files enables TLS without `tls=true`, and
  explicit files cannot be combined with `tls=false`. A MongoDB client resolves its database from
  `database` or the `addr` path and is refused with neither; `addr` names its hosts with
  `mongodb://`, never `mongodb+srv://`.
- Provision a MongoDB collection before its emitter writes: every write first looks it up, and a
  missing one fails the write as a transient error retried on `RETRY POLICY`. `ON CONFLICT` names
  mapped `VALUES` keys as its target, and `DO UPDATE` also needs one mapped field outside it. Both
  forms upsert by equality with the target values, so back them with a unique index on exactly
  those fields; `DO UPDATE` keeps the document's unmapped fields. See
  [Emitters → MongoDB](../../../../docs/src/emitters.md#mongodb).
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
  retry backoff, acknowledgement keepalive, emitter acknowledgement timeout, stop and drain deadlines,
  and any server-supplied HTTP `Retry-After` as physical. A failed attempt keeps the pending
  batches, their acknowledgements, and the unchanged cadence; never describe a paced domain as
  shortening or lengthening a real retry wait.
- Size an Iceberg emitter's worst-case latency as `FLUSH EACH` plus `COMMIT EACH`: the commit
  duration starts when a flush stages rows while none are staged, not when the rows arrived.
- Keep `MAX BATCH SIZE` well below 32 MiB on routes that feed an Iceberg emitter. A commit charges
  its staged files to the node's 32 MiB bulk memory class and reads a larger staged batch only once
  the whole class is free; `DESCRIBE EMITTER` shows a commit waiting for that memory as its
  transient error while the emitter retries.
- Provision Iceberg table columns with names and types matching the emitter's `VALUES`. A
  required `NAMESPACE` names the catalog namespace independently of the Nervix domain; provision
  each of its case-preserving dot-separated levels before starting the emitter. Until the catalog
  holds the namespace and the table, the emitter retries its open and `DESCRIBE EMITTER` names the
  namespace or the table it lacks. Move an emitter to another namespace with `SET TO`, restating
  its complete destination with the new `NAMESPACE` and that table's `LOCATION`. A
  `DATETIME`, at the top level or inside a `VEC` or `ARRAY`, is written at the containing
  microsecond and needs a `timestamptz` column, not `timestamptz_ns`. A commit writes each mapped
  column into the table column of its name, in any `VALUES` order, and a null into each optional
  column `VALUES` leaves out. Opening the emitter refuses, before staging records, a mapped column
  the table lacks or types differently, a required table column `VALUES` leaves out, and a required
  column, or a column whose nested values are required, mapped from an expression that can produce
  a null, such as an `OPTIONAL` field; wrap it in `coalesce(...)` with a never-null argument. A
  commit refuses a table changed after open in the same way, naming the column, and retries until
  the table can take the mapped columns again.
- Point an Iceberg emitter at an unpartitioned table. Its data files carry no partition values, so
  a table whose current partition spec has a field opens but fails every commit, and the emitter
  retries it without end. The emitter only appends Parquet data files with the Parquet library's
  default writer properties: it writes no deletes or upserts, never changes the table's schema and
  commits only through an `ICEBERG_REST` catalog client. Set `'path_style_access' = true` on an
  `S3` client whose S3-compatible store serves buckets as paths, or whose bucket name contains a
  dot; requests otherwise address the bucket as a virtual host. Read `Emitters` → `Iceberg data
  files` and `Iceberg object storage`.
- Set each Iceberg file I/O property on one client only. An Iceberg emitter whose catalog client
  sets a storage property its storage client already supplies, such as `s3.endpoint` beside the S3
  client's `endpoint`, is refused at `CREATE` or `ALTER`. Put each client's `tls_*` files on that
  client and mount their resource there: the `ICEBERG_REST` client's files apply to catalog requests,
  and the storage client's files apply to data files and credential calls.
- Every `MAX BATCH SIZE` is chosen as a logical Arrow payload boundary, excluding unused buffer
  capacity and object overhead. Delivery-mode `MAX <n>` appears only on `ACK PARALLEL`, never on
  an ingestor's `NO ACK` or an emitter's `NO ACK`.
- Every Kafka client sets `bootstrap.servers`; `CREATE KAFKA CLIENT` refuses a client without it.
  Treat `TOPIC` as a literal, case-preserving Kafka topic name in both directions, and give every
  `KAFKA TOPIC` ingestor `OFFSET BY DOMAIN` or `OFFSET BY CONSUMER GROUP '<group>'` and
  `ON QUIESCE SUSPEND`.
- Every Kafka client states the required `auto.offset.reset` policy explicitly when a new consumer
  group may need records that already exist; Nervix passes the setting through and does not supply
  a hidden default. Under `OFFSET BY DOMAIN` in an unpaced domain the same policy places a
  partition that has no domain offset yet, such as one added to the topic.
- A Kafka client never sets `allow.auto.create.topics`; `CREATE KAFKA CLIENT` refuses it. Provision each
  topic a Kafka ingestor reads: an ingestor never creates its topic and reports it as missing until
  it exists.
- Every Kafka client writes each librdkafka property under the Apache Kafka Java client's name,
  such as `bootstrap.servers`, `linger.ms`, `acks`, `retries`, `compression.type` and
  `delivery.timeout.ms`, and writes no `topic.` prefixed key other than librdkafka's own
  `topic.metadata.*` and `topic.blacklist`. `CREATE KAFKA CLIENT` refuses librdkafka's other
  name for a property, even alone, and a `topic.` prefixed key. Read `Connector Capabilities` →
  `KAFKA client` for each refused name and the name to write.
- For Kafka source control-call deadlines and bounded execution, follow `Ingestors` → `Client
  Deadlines`. Keep `socket.timeout.ms` distinct from the consumer-group commit caller deadline;
  an unanswered commit is retained before another can start.
- Kafka `OFFSET BY DOMAIN` commits and start-point resets wait up to the native checkpoint bulk
  operation's thirty-second budget for all assigned state replicas. A missing acknowledgement
  follows the source's existing rejection and retry policy; this mode remains at least once.
- A Kafka `OFFSET BY CONSUMER GROUP` ingestor in an `ACK` mode waits for the broker to answer each
  batch's commit before it reports progress, one round trip per batch, so prefer `ACK PARALLEL`
  where throughput matters. A refused or unanswered commit is the ingestor's transient status and
  its batch is read again, so downstream output can repeat.
- Every ingestor head names its connector and entity, such as `CREATE KAFKA TOPIC INGESTOR`, and
  its body after `FROM` names no connector. Its client belongs to the head's connector, and an
  `HTTP ENDPOINT` or `WEBSOCKETS ENDPOINT` reads an endpoint of `TYPE HTTP` or `TYPE WEBSOCKETS`.
  Every source body declares `MODE`, with `MODE NO ACK` where nothing waits on an acknowledgement,
  and only a mode its entity accepts. Every ingestor `ALTER` repeats the stored ingestor's head;
  reading another connector or entity takes `DROP INGESTOR` and a new `CREATE`.
- Delivery-policy durations and direct-value keys and types are checked at `CREATE` and `ALTER`.
  For OTEL's per-signal field constraints, histogram types, required client deadline, header
  decoding, TLS pairing and trust roots, receiver answers and unsupported features, read the
  generated `Connector Capabilities` tables and `Emitters` → `OTEL` before constructing mappings.
  Follow `Ingestors` → `Delivery Modes` for duration bounds and retry ordering, and `Emitters` →
  `Direct-emitter values` for exact types and literal-only OTEL resources. For example, supply an
  I32 expression for OTEL `severity_number`; an integer literal is I64. OTEL also checks literal
  field ranges before storing the statement. Read its client header-pair format before supplying
  credentials: values are percent-decoded once, and TLS file keys require an HTTPS endpoint for
  either transport. A refused alteration
  preserves the accepted graph.
- For acknowledged HTTP or WebSocket endpoint intake in Plexus, read `Ingestors` → `HTTP ENDPOINT
  And WEBSOCKETS ENDPOINT` for the explicit response window and deadline, reject-only quiesce,
  shared client outcome labels, and sender retry semantics. Check that a WebSocket endpoint's
  declared signaling format can encode its acknowledgements, including during `ACCEPT DATA`.
- To route endpoint requests by their method, path, query, peer address or host, or WebSocket
  frames by their kind, read the metadata tables in that same section before writing a
  `metadata.<field>`. An endpoint admits only `POST` at its exact `PATH`, so route on
  `metadata.query`, `metadata.host` or `metadata.remote_addr` rather than expecting the method or
  path to vary, and read a forwarding header such as `X-Forwarded-For` with `read_header`, because
  `metadata.remote_addr` is the TCP peer.
- Every external name an ingestor reads, such as a topic, consumer group, subscription, queue,
  channel, subject, queue group, or MQTT topic filter, is a string literal spelled exactly as the
  external system names it and within the rules of `Ingestors` → `External Names`.
- Every transport ingestor source ends with its documented `ON QUIESCE` body immediately before
  `DECODE USING`, with a positive `MAX SIZE`, explicit non-endpoint overflow policy, or endpoint
  `RETRY AFTER` wherever that mode requires it. `MQTT TOPIC` `SUSPEND` also declares `SESSION
  PERSISTENT QOS 1`. Do not mix mode bodies between entities or infer a default.
- MQTT source client identities default to the domain and ingestor, with an instance suffix for
  multiple instances. Account for the broker state that input replacement and `DROP INGESTOR`
  retire; ordinary stop retains a persistent session. Read `Ingestors` → `MQTT` for identities,
  session lifetime, the separate raw-frame and admitted-publish bounds, oversized-payload counts
  and cleanup failures, and `Emitters` → `MQTT` for emitter identities.
- An `MQTT CLIENT`'s `addr` is `mqtt://` or `mqtts://`, a host and a port, with no credentials,
  path or query; its `tls_*` files belong only with `mqtts`. Write MQTT wildcards only in an
  ingestor's `TOPIC`, as whole levels with `#` last and never a leading `$share` level, which
  Nervix forms itself; an emitter's `TOPIC` is one literal topic. Check both against the MQTT
  sections of `Ingestors` and `Emitters`.
- Redis Pub/Sub separates message read-ahead from the byte-bound quiesce buffer. Check the client
  required capacity and idle-probe keys against `Ingestors` → `Redis Pub/Sub`; retain the source's explicit
  `BUFFER` or `DROP` policy in generated Plexus.
- Redis Pub/Sub uses `NO ACK` in both directions. Treat `CHANNEL` as a literal, case-preserving
  name and consult the Redis manual sections for its byte bound and unavailable header operations.
- An `SQS QUEUE` ingestor uses `MODE ACK SEQUENTIAL` with `ON QUIESCE SUSPEND`, and an emitter
  `MODE SINGLE` or `MODE BATCH`. Treat `QUEUE` as a literal, case-preserving SQS name, and check
  `SQS CLIENT` keys against `Ingestors` → `SQS`: `endpoint`, `region`, `access_key_id` and
  `secret_access_key` are required, and it declares no proxy or client certificate keys.
- `NATS SUBJECT` ingestors use `NO ACK` with a required `QUEUE GROUP` and `INSTANCES`, and
  `BUFFER` or `DROP` quiesce. Write NATS wildcards only in an ingestor's `SUBJECT`, as whole
  tokens with `>` last; an emitter's `SUBJECT` is one literal subject. Check both against the NATS
  sections of `Ingestors` and `Emitters`.
- Every `PULSAR TOPIC` ingestor states who owns its subscription after the subscription's name:
  `OWNED BY INGESTOR` when Nervix may create it and should remove it when the ingestor or its
  domain is dropped, or `PROVISIONED` when an operator provisions it, which Nervix only verifies and
  the operator must create before the ingestor reads. Consumers register as
  `<domain>~<ingestor>-<instance>`. Read `Ingestors` → `Pulsar` for missing subscriptions, removal
  failures and which subscription `ALTER` retains or removes. Changing only consumer policy or
  handing the same subscription to `PROVISIONED` preserves its backlog; a changed broker, qualified
  topic or subscription name retires the preceding owned subscription best-effort.
- A `PULSAR CLIENT`'s `namespace` is exactly `'tenant/namespace'` (default `public/default`), and
  its TLS keys require a `pulsar+ssl://` `addr`; check both against `Ingestors` → `Pulsar` before
  generating a client.
- Every client ingestor is a `CLIENT PRODUCER INGESTOR` whose source is `FROM SCHEMA <schema> MODE
  ACK SEQUENTIAL|ACK PARALLEL MAX <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d> ON QUIESCE
  SUSPEND` and nothing else: no external connector client, codec, `DECODE USING`, headers, or `NO ACK`. The
  schema is the exact contract producers declare, including optionality and sensitivity, and a
  paced domain requires its timestamp source. Changing its schema, mode, timestamp, filter, routes
  other than `FLUSH`, or branch declarations ends attached producers. The Rust client may restore a
  producer handle with a fresh attachment after a session gap or relocation only when the domain
  `START` generation and endpoint contract still match; unresolved sent batches remain of unknown
  outcome. A row that fails on a route follows that route's `ON MESSAGE ERROR` policy, so under
  `LOG` its whole batch fails processing as `rejected`.
- Every client emitter declares `CLIENT CONSUMER EMITTER ... TO SCHEMA <output_schema> MODE ACK SEQUENTIAL|ACK
  PARALLEL MAX <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d>`, a required `BATCH` row and
  byte limit, and an explicit `FLUSH` policy. It constructs exactly that schema as native Arrow;
  a sensitive input needs explicit leakage, and a row whose encoded IPC cannot fit alone follows
  `ON MESSAGE ERROR`. There is no external client, codec, header operation, or direct `VALUES`
  body. An attached emitter holds source acknowledgement until the application ACKs; a detached
  emitter releases the source earlier. Consumers compete for output and are volatile across
  session or owner loss. The Rust client may restore a matching consumer handle with a fresh
  attachment, but reports the gap before new output and never sends an old delivery reference to
  the replacement. Applications must handle a repeated delivery identity with a fresh ACK
  reference and uncertain ACK confirmation.
- HTTP `EVERY`, Prometheus `EVERY`, and generator `EACH` use domain-logical cadence. HTTP and
  generators run once immediately; Prometheus first runs after one interval. Keep later work on
  the original schedule, coalesce missed periods without a catch-up burst, query Prometheus at the
  due instant, and use a fresh execution snapshot for returned data and generated routes.
- Every `PROMETHEUS CLIENT` sets `addr` to the API root, an `http` or `https` URL without a
  query string or fragment, and a positive `timeout_ms`. It sets no key beyond `tls_ca_file` and
  the paired `tls_cert_file` and `tls_key_file`; `CREATE PROMETHEUS CLIENT` refuses any other. Read
  `Ingestors` → `Prometheus` for the keys.
- A `PROMETHEUS QUERY` ingestor reads at most `MAX BODY SIZE` bytes of each response, written
  between `QUERY` and `EVERY` and `16MiB` when left out. A larger response fails the poll as the
  ingestor's transient error rather than delivering part of it; raise the bound for a query that
  legitimately returns more. Samples enter the graph in batches of at most 1024.
- Treat Kafka `NO ACK` emitter success as local librdkafka producer-queue admission. Even in
  `ATTACHED` mode, Nervix does not wait for a broker delivery receipt before completing its ACK
  share. `ACK` waits for each record's delivery report, and its `ACK TIMEOUT` replaces the
  client's `delivery.timeout.ms` as the producer's delivery bound. The producer always reports every record, so `delivery.report.only.error` in a client's
  `CONFIG` has no effect.
- Every Sentry emitter references a `SENTRY CLIENT` with a project DSN and a positive `timeout_ms`,
  encodes one event JSON object per record, or per batch through `ON EMITTING BATCH`, and has no
  `write_header` invocation.
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

For a diagnostic node's lock findings, read
[Data-Plane Concurrency](https://docs.nervix.io/data-plane-concurrency.html#diagnostic-deadlock-detection).
Use the local `nervix-deadlock-report inspect`, `triage`, and `qualify` commands on retained
artifacts. A potential cycle requires an explicit source-based correction or infeasibility review
with a retained regression. Preserve the original whole-process evidence: filtered exports cannot
qualify its run. Combine qualification with the actual process outcome and the detector's stated
coverage limits, including requested-read historical edges and untracked async synchronization.

Choose checks relevant to the configured graph:

- `SHOW CREATE <kind> <name>;` confirms the stored canonical definition.
- `DESCRIBE RELAY <relay>;` reports the relay owner and optional materialized-state replicas;
  `DESCRIBE RELAY <relay> WHERE (...);` is owner-authoritative for concrete branch state.
- `SHOW RELAY <relay> MATERIALIZED STATE;` inspects materialized data and placement.
- `DESCRIBE INGESTOR`, `DESCRIBE JUNCTION`, other processor-specific `DESCRIBE` commands, and
  `DESCRIBE EMITTER` inspect runtime state and edge metrics. `SHOW INGESTORS;` lists every ingestor
  with the connector and entity it reads (`from=<CONNECTOR> <ENTITY>`), its owner and state, and a
  client ingestor's admission, producers, outstanding batches and bytes, and admitted batches. A
  client emitter's description also counts active consumers, forwarded credit and retained output,
  incomplete application batches, retries, ACKs and rejections.
- Read `Ingestors` and `Emitters` for the `SHOW INGESTORS`, `SHOW EMITTERS` and `DESCRIBE`
  inspection contracts, including effective options versus canonical `SHOW CREATE` configuration.
- The observability server's `/metrics` endpoint reports raw graph-edge counters and histograms,
  including batch-size resolution for tuning collection and flush boundaries. Read `Metrics And
  Observability` for the current histogram buckets. The endpoint also reports
  `nervix_branch_instances` per domain, branch declaration, and physical node, plus
  `nervix_branch_evictions_total` split by `reason="lru"` or `reason="ttl"`.
- A server error, negative acknowledgement or log names a concrete branch as `branch <fingerprint>`
  or `unbranched`, never by key values. Match the fingerprint to the one `DESCRIBE WASM PROCESSOR`
  and `DESCRIBE BACKUP` print, and read key values from a subscription, which masks `SENSITIVE`
  key fields.
- `DESCRIBE RESOURCE` confirms uploads and reports `latest`, the completed version `VERSION LATEST`
  would bind now; `SHOW CREATE` shows the version each existing binding stores.
- `BACKUP CLUSTER TO '<file>';` or `BACKUP DOMAIN [<name>] TO '<file>' [WITHOUT RESOURCES]
  [WITHOUT STATE | WITHOUT PAUSE | TIMEOUT <duration>];` writes an archive on the client's machine,
  sent alone from `nervix-cli`, a native client, or the web console, which saves it as a verified
  browser download. A normal backup quiesces each running domain
  before capturing WASM guest state, Kafka domain source offsets, branch lifecycle, fresh
  materialized relay generations, deduplicator keys and the rows windows retain. A backup is
  refused while a node that owns the domain's state is down or no node can host part of it, and a
  normal backup also while a node that runs its work is; send it again once the node is back,
  failover has moved its work, or a node is uncordoned.
  Materialized capture also refuses insufficient overlapping bulk memory; release competing bulk
  work and send a fresh backup rather than extending `TIMEOUT`. Read the backup chapter's capacity
  failures before sizing or retrying a capture.
  `WITHOUT PAUSE` reads published checkpoints while execution continues; `WITHOUT STATE` captures
  only configuration. `DESCRIBE BACKUP '<file>';` verifies one offline in `nervix-cli` and inventories
  its state, domains, users, and resource versions. Treat an archive as a secret. Kafka offset and
  branch lifecycle sections stream within the 32 MiB bulk budget up to the 64 MiB record limit;
  their conversion is charged to the node's `restore_metadata` grant, sized below.
- `RESTORE CLUSTER FROM '<file>' [RESUME] [ON EXISTING USER FAIL | SKIP | REPLACE] [DRY RUN]
  [WITHOUT STATE | WITHOUT SOURCE OFFSETS];` or
  `RESTORE DOMAIN <name> [AS <new_name>] FROM '<file>' [RESUME] [DRY RUN]
  [WITHOUT STATE | WITHOUT SOURCE OFFSETS];` recreates users, domains,
  resource versions under their archived numbers, models, and compatible runtime state from an archive, sent alone from
  `nervix-cli`, a native client, or the web console's Backups dialog, which streams an archive file
  you choose. The default leaves restored domains stopped. `RESUME` makes them
  running at the archived start generation and clock mapping after complete publication, preserving
  materialized rows, deduplicator keys and windows. Paced mappings project downtime; normal `START`
  establishes a new generation and clears materialized state. Restored deduplicator keys expire at
  their archived first sighting plus `MAX TIME`; a window whose model or branch incarnation changed
  starts empty and the restore warns about it. A Kafka source restored with its offsets reads again
  the rows its windows retained at the cut. `START` remains blocked until the complete state installation
  succeeds, including after a failed restore or restart. A domain name that exists is
  refused, so copy a domain with `AS`. An archived branch key that is not a key of the branch its
  restored entity declares refuses the restore before anything changes, naming the section, entry
  and field. A fresh cluster already has its bootstrap user, so a cluster
  restore there needs `ON EXISTING USER SKIP` or `REPLACE`. Run `DRY RUN` first to see the plan and
  each domain's impact report without changing anything.
  Failed unpublished checkpoint data is reclaimed on every node while the failed target's
  `START` gate stays closed. The same maintenance also reclaims restored chunks made unreachable
  by resumed checkpoints, module rebinding or entity removal; existing snapshot readers can
  retain their disk data.
  Size the server's `--restore-staging-max-bytes` or
  `NERVIX_RESTORE_STAGING_MAX_BYTES` allowance for all concurrent unpublished checkpoints on that
  node; it defaults to the smaller of `128GiB` and the node database's volume, a larger value
  refuses the node's start, and it counts keys and values, including incomplete chunks. It is
  separate from archive-file staging and does not bound snapshot retention or filesystem
  overhead. Observe `nervix_restore_staging_bytes`, `nervix_restore_staging_limit_bytes` and
  `nervix_restore_staging_reclaimed_bytes_total` at `/metrics`; the backup chapter distinguishes
  logical usage from SST allocation. Recover an uncertain applying restore by sending it again
  under its exact execution reference, as the Rust client's `restore_with_reference` does;
  `nervix-cli restore` starts every run under a fresh reference. After a terminal failed restore,
  restore into a fresh target name.
  Restore metadata preparation has a separate per-node `restore_metadata` memory grant, defaulting
  to 2 GiB and admitted before decoding or planning. Native lifecycle, Kafka, deduplicator and
  window checkpoints stream through quota-owned files and 64 KiB windows with a fixed 2 MiB bulk
  I/O grant per operation, so a keyspace or window larger than the 32 MiB bulk budget restores; a
  deduplicator conversion also charges its resident keyspace to `restore_metadata` while it runs.
  Observe `nervix_execution_memory_reserved_bytes` by class; database and runtime `START` memory
  have separate owners. The backup chapter documents record limits and preparation sizing.
  Decoded materialized snapshots use the independent `state_read` class through installation or
  source evaluation and dispatch. Materialized generators retain unchanged remote generations in
  `state_read_cache` class, whose capacity equals the relay budget, 192 MiB by default. A full
  cache reads the complete generation without retaining it or waiting for cache space. Include
  both read pools in node sizing: the default execution pools total 2,723 MiB, checked at startup.
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
  ends when its relay is redefined or removed, and no client reopens it on its own, even after a
  reconnect; create it again under the same name to read the current definition. In the web console
  the tab turns ended and its resubscribe button does this.
- `nervix-cli --domain <domain> subscribe <name> <relay>` streams subscription rows to a shell.
- `ATTACH DOMAIN CLOCK;` shows the active domain's `START` generation and clock state, including a
  paced domain's committed origin, UTC anchor, and rate, then reports each state change and newest
  accepted tick until `DETACH DOMAIN CLOCK;`. Use it when paced ingestion rejects `TIMESTAMP AT`
  values: the mapping and tick frontier show which logical centers the admission window has reached.
  A node that is still starting answers it once it has installed the committed domains, so a
  `does not exist` refusal means the cluster has no such domain, not that the node restarted.
- `nervix-cli --domain <domain> domain-clock` follows that clock from a shell until Ctrl-C, or until
  the server ends the attachment, printing the attach reply, state changes, and tick lines.
- A host using the shared C binding executes attach and detach through `nx_session_execute`, reads
  the clock the attach reported, generation and paced mapping included, with
  `nx_session_domain_clock` before it uses a tick, and reads later states and ticks with
  `nx_session_next_clock_event`. The clock's projections give the logical time, the wait until a
  logical target, and the `TIMESTAMP AT` values the ingestor admits.
- `SHOW CLUSTER STATUS;` checks cluster topology before diagnosing a graph as unavailable.
- `DESCRIBE CLUSTER;` lists the prerequisites every live node brings and the nodes whose Raft
  heartbeat, gossip interval or clock-skew tolerance differs from the leader's. A node refused as it
  joins, or by its startup checks, names the setting and both values; read
  [Node Settings](https://docs.nervix.io/node-settings.html) directly for the rules, which is
  outside the curated Plexus index.
- `SHOW TRANSACTIONS;` checks open/committing progress and retained commit, revert, failure, or
  expiry outcomes.

For a parse error, follow the reported expected token and compare clause order with the relevant
public example. For an MQTT QoS or session-pairing refusal, use the underlined token and the
connector's Ingestors or Emitters section to choose an accepted value or quiesce mode.
For a validation error, trace exact types, declaration order, domain ownership,
branch compatibility, construction completeness, and connector capabilities. For missing data,
check domain lifecycle, source offsets, timestamps, filters, branch keys, route filters, flush
boundaries, input collection boundaries, and external entity provisioning in that order.

Runtime events and report-bearing log fields show the owning operation followed by its cause
chain. Read the deepest cause together with the node or clock context when diagnosing cadence,
partition inspection, relay delivery, checkpoint, or error-route failures. A failed error-route
dispatch includes the same report chain in its runtime event and negative acknowledgement;
the configured error policy and retry classification still determine recovery.
