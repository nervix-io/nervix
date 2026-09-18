---
name: nspl
description: Design, author, explain, review, and troubleshoot Nervix configurations written in the Nervix Stream Processing Language (NSPL). Use when a user wants to configure domains, schemas, codecs, branches, relays, resources, clients, ingestors, processors, emitters, placement policies, lookups, Roto UDFs, subscriptions, lifecycle commands, or complete Nervix streaming graphs. Produce current, valid NSPL and identify required external provisioning.
license: FCL-1.0-ALv2
---

# Configure Nervix with NSPL

Turn a user's streaming requirements into an explicit, deployable Nervix graph. Open the public
[Nervix NSPL documentation index](https://docs.nervix.io/llms.txt), then read the linked Markdown
needed for the request. Treat that versioned documentation as the authority; never reconstruct
clause order or connector options from memory.

## Gather the configuration contract

Establish these inputs before finalizing NSPL. Ask only for missing details that materially change
the graph; otherwise use conspicuous placeholders and state the assumptions.

- Domain: paced or unpaced, clock period/skew, start behavior, and default placement policy.
- Payload: sample input, wire format, exact internal field types, optional fields, and sensitive
  fields.
- Source and sink: connector kinds, externally provisioned entity names, endpoints, delivery/ACK
  expectations, emitter publishing mode, confirmation window and timeout where applicable, retry
  pacing, ordering, and offsets.
- Isolation: unbranched or a concrete branch key, branch TTL, and optional instance limit.
- Processing: filtering, construction, Roto UDFs, deduplication, ordering, windows, inference,
  WASM fuel and linear-memory budgets, correlation, materialized state, lookup, generation, or
  repartitioning.
- Placement: latency-critical or heavy corridors, colocation enforcement, rule precedence, and
  whether ordinary scheduler heuristics should remain neutral.
- Operations: batching/flush, error routes, credentials/TLS resources, observability, session
  subscriptions, each ingestor's source-supported quiesce behavior, and whether an existing schema
  must be evolved atomically with its dependents.

Read [references/configuring-nervix.md](references/configuring-nervix.md), then use its routing
guidance to select the relevant Markdown entries from the public index.

## Assemble the graph

Run the control plane with `nervix-server` and submit configuration through the separate
`nervix-cli` client. Format saved `.nspl` files with `nervix-nspl-format`.

Build configuration in dependency order:

1. Create the domain, then select it with `USE <domain>;` as a separate client command. `BEGIN`
   is rejected until a selected domain exists.
2. Register and upload resources before statements that reference their versions or mounted files.
   Resources are domain-owned, so declare and upload them in the domain that references them.
   VHOST TLS, protobuf codec, protobuf signaling protocol, inferencer, WASM processor, hash map, and
   client mount bindings require `VERSION <n>` or `VERSION LATEST`. `LATEST` resolves to the highest
   completed version when the statement is applied (at `COMMIT` for queued statements) and the
   model stores that number; a later upload never moves a binding.
   Use `REBIND RESOURCE <name> TO VERSION <n>|LATEST` to move every existing usage atomically, or
   add `FOR <kind> <name>, ...` to select exact kind-qualified usages. Every selected member must
   already bind the resource. The target and all replacements validate together; `LATEST` is
   provisional at queue admission and resolved again at `COMMIT`. Rotating a VHOST certificate
   this way is dynamic: every node's HTTPS listener presents the new bundle and ingestion does not
   pause. Changing VHOST hostnames, adding or removing `WITH TLS`, or binding another TLS resource
   pauses the domain.
3. Define internal schemas, branch-key schemas, branches, wire schemas, and codecs.
4. Define clients, signaling protocols, virtual hosts/endpoints, lookup models, and trusted Roto
   UDFs as needed.
5. Define relays before nodes that read or write them.
6. Define ingestors, processors, generators, and emitters in graph order.
7. Define placement rules after every referenced runtime node, including each relay, exists.
8. Commit the graph, inspect it, and start the active domain only when prerequisites exist.

Use `BEGIN; ... COMMIT;` when sending multiple queueable configuration statements. A transaction
belongs to one already-existing domain: `BEGIN` binds it to the selected domain and every queued
statement must select that same domain. Transactions and commit progress are replicated and
resumable, but their content is deliberately limited to that domain's model mutations, domain
configuration/lifecycle, and `CREATE RESOURCE`. Keep `CREATE DOMAIN`, `CREATE USER`, read-only
statements, subscriptions, `USE`, resource uploads, and node administration outside the
transaction. Use `SHOW TRANSACTIONS;` when transaction state or a retained outcome needs
verification. Queue admission preflights each statement against the replicated prefix without
applying effects. Consecutive model mutations form one atomic run and report the run's effective
base-to-final quiesce level at the current prefix; a lifecycle, domain, or resource statement ends
that run, and a later run cannot repair it. `COMMIT` reports only the maximum level actually
executed and does not repeat statement outputs. Correct a rejected statement and continue the same
transaction. Do not imply that one undivided request can mix those phases.

Treat a successful administrative command as a completed effect. After `UPLOAD RESOURCE`, model or
lookup creation, `START`, `STOP`, placement changes, or `COMMIT` returns `OK`, issue the dependent
operation immediately. Do not add a readiness request, `DESCRIBE` polling loop, arbitrary delay, or
retry before that dependent operation. Transaction statement success means durable validation and
staging only; `COMMIT` supplies the full usable-effect boundary. CLI, Rust-client, and browser
flows retain command execution references, upload identities, transaction append positions, and
commit identity through redirects or reconnects. Do not manufacture a new identity for an
uncertain admitted operation.

For storage failures or uncertain administrative outcomes, consult `Control Plane` → `Durability
and recovery` before suggesting a retry.

For model evolution, read the `Altering Schemas` section of `Schemas And Codecs` and the transaction
and quiesce semantics in `Control Plane`. Put every interdependent `CREATE`, supported `ALTER`, and
`DROP` for one domain in the same transaction, including schema, wire-schema, relay, junction,
deduplicator, reorderer, emitter, ingestor, reingestor, generator, and placement changes;
Nervix classifies the complete model diff and no user-facing pause command exists. Capacity and
expression-only junction changes and emitter flush changes are dynamic; relay schema or branching
changes pause the domain; structural junction, emitter sink/client/codec/collect/publishing-mode or
attachment changes, ingestor,
emitter source-predicate, and relay materialized-state changes gate and drain only affected
entities. Changing emitter `FROM` membership pauses the domain because it changes topology.
Deduplicator key and reorderer ordering changes also use entity pause; their `MAX TIME` changes are
dynamic. In `ALTER INGESTOR`, use a complete transport-specific source body after `SET FROM`, or
change only the current source's mode with `SET QUIESCE <body>`.
Every reingestor and generator ALTER uses entity pause; reingestor route bodies retain their
per-route branch selection, while generator route bodies remain set-only.
`ALTER DOMAIN SET PLACEMENT` is nameless, targets the active domain, and performs a normal schedule
activation; a newly effective hard colocation requirement can relocate runtime nodes.

## Preserve NSPL semantics

- For paced domains, use a positive `PERIOD` no larger than `18446744073709551615ns`; `SKEW` may
  be zero but must fit the same nanosecond duration range. `START AT <timestamp>` is limited to the
  inclusive signed Unix-nanosecond range `1677-09-21T00:12:43.145224192Z` through
  `2262-04-11T23:47:16.854775807Z`. `TIME RATE` must be a positive finite `f64`; scientific notation
  is valid. Tick notifications report progress and never redefine the committed start mapping.
  The mapping is installed on every live node before domain execution and remains bound to its
  `START` generation across joins and automatic ALTER pauses. One replicated authority revision
  identifies the producing node incarnation; owner changes preserve the mapping, and progress is
  accepted only from the matching committed incarnation and authenticated peer after every live
  node installs that revision. `STOP` revokes the authority, while automatic ALTER quiescing leaves
  it running. Never describe a missing, stopped, uninstalled, or stale paced clock as falling back
  to wall time. Unpaced domains receive actual UTC through the same domain-time capability.
  Require synchronized and monitored UTC on every cluster host. Reads are nondecreasing per node
  and generation, but the mapping does not promise identical simultaneous reads or a total order
  across hosts. Host offset affects `START AT NOW`, unpaced observations, and paced projection;
  `TIME RATE` multiplies projection error. `SKEW` is event-admission tolerance, not a host-sync
  allowance.
- For ingestion, read `Domains And Time` → `Ingestion Timestamps`: `TIMESTAMP NOW` uses domain
  time at delivery, including after quiescing; explicit source times remain unchanged. Check
  admission against the newest 256 reached logical centers with inclusive `SKEW`, independently
  of tick notification delivery. Never scale source timestamps by `TIME RATE` or admit against
  an unreached future center.
- Treat HTTP `EVERY`, Prometheus `EVERY`, and generator `EACH` as domain-clock cadence. HTTP and
  generators have an immediate first occurrence; Prometheus first becomes due after one interval.
  Later occurrences stay anchored, coalesce missed periods into one newest-due execution, and
  advance directly to the first future boundary. Prometheus queries at the due instant and
  evaluates returned data with a fresh execution snapshot. The declared quiesce mode still decides
  whether polling is suspended or buffered; withholding work does not pause or re-anchor the
  cadence. Connector timeouts, retries, backoff, and cancellation remain physical.
- Treat domain execution time as one snapshot per accepted unit of work. Every expression in that
  unit uses the same instant, including filters, construction, keys, correlation and window
  programs, inferencer mappings, emitter `VALUES` and SQS FIFO groups, and subscription filters.
  Buffered emitter batches and retries retain their assigned snapshot; work resumed after
  `REQUIRED WAIT` starts with a fresh one. Error occurrence and error-route `SET` share the failing
  snapshot. Generator output and tokenless WASM output use it for generated watermarks, while
  source-token WASM output preserves source metadata. Omitted Sentry timestamps use domain time;
  explicit Sentry timestamps are preserved; OTEL `observed_time_unix_nano` and HTTP-date
  `Retry-After` interpretation use actual UTC.
- Treat a session subscription `WHERE` clause as a predicate over the subscribed relay record.
  Bare fields, `message.<field>`, and `input.<field>` are equivalent there. Do not use `output`,
  `branch`, materialized `relay_state`, construction clauses, or side effects; subscription
  creation rejects them, and a selected record is delivered unchanged before sampling.
- Roto `now()` and the WASM domain-time import receive the owning execution snapshot explicitly.
  There is no context-free engine clock. Guest initialization, input, timeout, flush, and state
  lifecycle operations each use the snapshot selected for that operation; guest timeout delays are
  logical, while Wasmtime fuel and epoch yielding are physical safety controls.
- Declare exact schema types and nullability. Use explicit conversions; never invent implicit
  casts between wire, internal, branch, processor, lookup, state, and sink values.
- Use `IF ... THEN ... ELSE ... END` or searched/simple `CASE` for conditional values. Keep every
  result at one exact type; remember that omitted `CASE ELSE` yields a typed null and requires an
  optional destination.
- Count string positions in `substr`, `split_part`, and `strpos` from 1, but `nth(list, index)`
  from 0. Outside window processors, `count`, `sum`, `first`, `last`, and `nth` take one `ARRAY` or
  `VEC` value; inside a window processor route, `count`, `sum`, `first`, and `last` are window
  aggregates over retained input rows.
- Write datetime units, date parts, `date_bin` widths, time zones, formats, and disambiguations as
  literals. Units from `nanosecond` to `week` have fixed lengths; `month`, `quarter`, and `year` are
  calendar units that only `date_trunc`, `date_add`, and `date_diff` accept. `date_trunc`,
  `date_bin`, and `to_unix` round toward negative infinity, including before the epoch; `date_diff`
  rounds toward zero; a week starts on Monday; and `date_bin` always takes an explicit origin. A
  result outside the `DATETIME` range fails only that message with an `overflow` error. Datetime
  functions compute only from their arguments; pass `now()` for the execution-local domain time.
- Name a zone explicitly for local calendars: `date_part`, `date_trunc`, `date_add`, `date_diff`, and
  `format_datetime` take an optional trailing `'UTC'`, IANA name, or `'+HH:MM'` offset, read in UTC
  without one, and always return UTC instants. Zone rules come from the IANA database bundled into
  Nervix, never from the host. In an IANA zone a `day` or `week` is a local calendar day, a month
  moved past a shorter month lands on its last day, and a calendar `date_diff` counts whole units
  from `start`.
- Read external timestamps with `parse_datetime(format, text[, zone[, disambiguation]])` using
  strftime-style directives such as `%Y-%m-%dT%H:%M:%S%.f%:z`. Reading is strict and never guesses:
  there are no two-digit years or locale formats, and a format must read a complete date. A format
  with `%z`, `%:z`, `%::z`, or `%s` takes no zone; any other format requires one, and a local time
  the zone skips or repeats fails its message unless `'earlier'`, `'later'`, or `'compatible'` is
  given. Give such routes an `ON MESSAGE ERROR` policy for `cast_failed` and `invalid_argument`
  failures. Formats describe values of at most 256 bytes.
- Treat arithmetic and numeric functions as checked at the operands' exact type: integer overflow,
  a zero divisor, and a float or math result that is NaN or infinite fail only that message with a
  per-message error. Give a route whose operands can reach those values an `ON MESSAGE ERROR`
  policy, and cast to a wider type before arithmetic that can exceed the narrower one.
- Expect `round(x, digits)` to round a float's stored binary value exactly, so `round(2.675, 2)` is
  `2.67`. Test for NaN and infinities with `is_nan`, `is_finite`, and `is_infinite`, which accept
  only `F32` and `F64`. Give `bitwise_and`, `bitwise_or`, and `bitwise_xor` two arguments of one
  integer type, and treat shifts as checked: a negative count, or a `shift_left` whose product does
  not fit the value's type, fails that message.
- In window routes, prefer the dedicated aggregates (`AVG`, `COUNT_IF`, `BOOL_AND`, `BOOL_OR`,
  `ARG_MIN`, `ARG_MAX`, `*_POP` and `*_SAMP` variance, deviation, and covariance, `CORR`) over
  hand-built formulas. Check `Processors` → `Window aggregate functions`: a null argument
  contributes nothing while `COUNT` counts every row, and an aggregate that can be null (sample
  statistics, `CORR`, anything over an `OPTIONAL` argument) needs an `OPTIONAL` output field or
  `COALESCE`.
- Use a separate wire schema and codec when transport shape differs from the internal runtime
  schema. Declare datetime encoding explicitly when required.
- For every JAQ-backed codec, use `WITH JAQ TRANSFORMATIONS` and declare `ON INGESTION`,
  `ON EMITTING`, or both in that order. At least one direction is required.
- An `ON INGESTION` program runs once per value a payload holds, and every object it yields becomes
  one message: `.[]` unfolds an array, and a program that yields nothing acknowledges the payload
  without a message. A payload is decoded or rejected as a whole, unfolds into at most 65,536
  messages, and stays the unit of source acknowledgement. `ON EMITTING` must yield exactly one
  value per record.
- Give every signaling protocol an explicit `FORMAT` and express the handshake as JAQ:
  `SEND JAQ` programs must each yield exactly one value, and `WAIT JAQ` matchers accept any output
  that is neither null nor false. Match only the fields that matter so acknowledgements carrying
  connection ids or timestamps still match.
- Write signaling steps in the order they must happen; each completes before the next starts, so a
  send that depends on an earlier reply goes after the wait for it. A `WAIT` step may list several
  matchers when their frames may arrive in any order. Use `CAPTURE` on a single-matcher step to
  record values, and read them in later programs through `$state`.
- Say where payload starts flowing with `ACCEPT DATA`, either on `ON CONNECT` or on the `WAIT` step
  whose completion proves the peer is streaming. Frames arriving before that are dropped, not
  buffered.
- Scope rejection to where it applies: `FAIL JAQ` on a `WAIT` step aborts during that step, and a
  `FAIL JAQ` written before `ON CONNECT` applies throughout the handshake.
- Preserve written operation order in schema, relay, junction, deduplicator, reorderer, emitter,
  ingestor, reingestor, generator, and placement ALTER statements.
  Include every dependent wire, internal, codec, and node mutation required for the candidate graph
  to validate in the same transaction.
- Treat JSON, CBOR, and AVRO wire schemas as distinct entity kinds. Their names may coincide, so
  every create, alter, show, drop, and codec reference must include the exact format.
- Declare wire-schema mode after the entity name with `CREATE WIRE <format> SCHEMA <name> MODE
  STRICT|LOOSE`. Change it with `ALTER WIRE <format> SCHEMA <wire_schema> MODE STRICT|LOOSE`;
  the same format-qualified ALTER form owns field evolution.
- Call UDFs only through `udf::<name>(...)`, keep arguments exact-typed, and use `VOLATILE` only
  when the body needs the domain clock or randomness. Roto UDFs are trusted native code; keep
  untrusted custom processing in WASM.
- Include Roto `test` blocks with every agent-generated UDF. Creation runs those tests and rejects
  the UDF without persisting it when any test rejects.
- Select `BRANCHED BY <branch>` or `UNBRANCHED` explicitly. Normal processors preserve their named
  branch; use a reingestor when the graph must repartition or remove branch grouping.
- Treat placement rules as path-gated overlays, not connectivity-independent groups. Use
  `REQUIRE COLOCATION` only for a hard same-cluster-node constraint; `PREFER COLOCATION` and
  `SUGGEST SEPARATION` are soft, `NEUTRAL` leaves scheduler heuristics active, and no hard
  separation policy exists. Lower `RANK` values are stronger, unranked rules are the weakest rule
  tier, and equal-rank different-policy claims conflict.
- Treat every relay as a scheduled runtime node with one owner. `CAPACITY` bounds its one owner
  buffer cluster-wide, while each producer node and remote consumer node contributes one fixed
  in-flight dispatch slot. Materialized state adds state replicas to that relay; it does not add a
  separate runtime-node kind. All relays are valid placement members and corridor hops.
- Treat Endpoint and Syslog ingestors as cluster-wide listeners. Every client-source ingestor,
  including an outbound WebSocket client, is single-owner and keeps its live assignment across
  ordinary schedule recomputation; use drain, `RELOCATE`, or a hard colocation requirement when it
  must move.
- Use `RELOCATE <selection> ONTO NODE <node_id> FOLLOW PREFERENCES | IGNORE PREFERENCES;` to move
  chosen work onto a named cluster node. The selection is a kind-qualified list or a
  `FROM ... TO ...` corridor, hard colocation groups always move whole, and the whole unit moves in
  one gated handoff or not at all. It is a one-time move, not pinning.
- An emitter may list multiple `FROM <relay> [WHERE <expr>]` inputs when every relay declares the
  same payload schema. Unlike ordinary processors, those inputs may use differently named
  branches. Keep collection separate per source relay and concrete branch, and remember that one
  node-wide materialized dependency or message-error relay must be exact-branch compatible with
  every source.
- Give every emitter an explicit transport-supported `MODE` as the final sink subclause before
  `ENCODE USING`. Include every required variable: all modes declare `RETRY POLICY BACKOFF <d> MAX
  <d>`; asynchronous confirming modes also declare `ACK SEQUENTIAL` or `ACK PARALLEL MAX <n>` and
  `ACK TIMEOUT <d>`. Do not invent a default mode, window, timeout, or retry cadence.
- Request/response emitters do not take `ACK TIMEOUT`. When configuring SQS, Sentry, OTEL, or
  ClickHouse, put `timeout_ms` in the referenced client CONFIG when the request needs an explicit
  bound; the emitter's declared retry policy owns pacing after that request fails. OTEL clients
  must also select `grpc` or `http/protobuf` explicitly with the required `protocol` key.
- Require `WITH MAX BATCH <positive_n>` for ClickHouse, Postgres, MySQL, and MongoDB emitters. For
  SQS, use `FIFO GROUP FROM BRANCH|<string_expression>` exactly when the externally provisioned
  queue name ends in `.fifo`; `FROM BRANCH` requires branched input.
- Give every client resource mount an explicit `MOUNT <resource> VERSION <u64>|LATEST` clause. Put
  database pool bounds before the mount, and keep a WebSocket client's `WITH SIGNALING PROTOCOL`
  clause before the mount. `LATEST` is resolved when the statement is applied; the stored client
  and `SHOW CREATE CLIENT` contain the resulting number, so later uploads and restarts do not move
  the mount.
- Give every hash map an explicit `FROM RESOURCE <resource> VERSION <u64>|LATEST` clause. `LATEST`
  is resolved when the statement is applied, and the stored hash map and `SHOW CREATE HASH MAP`
  contain the resulting number.
- Declare connection-pool bounds on every `POSTGRES`, `MYSQL`, `MONGODB`, and `REDIS` client, after
  `TYPE` and before an optional `MOUNT`: `POOL SIZE MIN <u32> MAX <positive_u32>`, in that order,
  with the minimum no greater than the maximum. The clause is required even when only ingestors
  reference the client, and no other client type accepts it. Never put pool sizing in connector
  `CONFIG` or an address query parameter. One pool serves every local user of a named client on one
  node, so size it for the node's whole workload rather than per emitter, and expect one pool per
  node the client is placed on. Read the dedicated `Common` → `Database Client Connection Pools`
  documentation entry before choosing values.
- Give a `POSTGRES` client an absolute `postgres://` or `postgresql://` `addr` URL that selects
  either `sslmode=disable` or `sslmode=verify-full`; no other mode is accepted and there is no
  opportunistic fallback. Mounted `tls_ca_file`, `tls_cert_file`, and `tls_key_file` are the
  TLS-file interface, certificate and key must be supplied together, and TLS files require
  `verify-full`.
- Treat every route as a newly constructed output. Add `INHERIT` only where that node permits it,
  and initialize every required output field on set-only routes.
- Add a route-local message error policy. Add the required general/global policy for the chosen
  node.
- Add `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` after a relay input list only when the node
  should assemble input batches before execution. Omission means no additional input collection.
  The policy is per source relay and concrete branch; correlators configure each side
  independently. Its duration uses the domain clock and starts when an empty collector receives
  data. Never add it to an ingestor.
- Add `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` to every flush-based
  route. `FLUSH EACH` is domain-logical and branch-local, in emitters and the Iceberg `COMMIT EACH`
  exactly as elsewhere, while emitter retry backoff, acknowledgement keepalive, sink
  acknowledgement timeouts, and an HTTP `Retry-After` stay physical. Treat `FLUSH IMMEDIATE` as the
  physical, system-owned 100 µs minimum batching window, not a one-message batch guarantee; domain
  pacing never scales it. Both timers start when an empty route buffer receives data. `MAX BATCH SIZE`
  counts logical Arrow value, offset, and validity bytes, not unused buffer capacity or object
  overhead. Windows use `WIDTH` and `STEP`; WASM output cadence is controlled by the guest. Choose
  `FLUSH` values as latency and boundary-cost controls, not as a throughput lever: `MAX BATCH SIZE`
  only clamps a batch, and the flush tuning guidance in the docs records which sinks benefit from
  larger batches.
- Use delivery-mode `MAX <n>` only with `ACK PARALLEL`; `NO_ACK` has no in-flight ACK window and
  never accepts `MAX`.
- End every ingestor source specification with an explicit source-supported `ON QUIESCE` body
  immediately before `DECODE USING`. Include positive `MAX SIZE` and, outside `ENDPOINT`, an
  explicit `ON OVERFLOW DROP OLDEST|DROP NEWEST` for `BUFFER`; include `RETRY AFTER` for endpoint
  `REJECT`. Use MQTT `SUSPEND` only with `SESSION PERSISTENT QOS 1`. Do not invent a default or use a
  mode offered by another source type.
- Declare both required WASM limits immediately after `FILE`, in order: `MAX FUEL <positive_u64>
  MAX MEMORY <positive_byte_size>`. Fuel is reset per logical guest operation; memory caps each
  branch guest's Wasmtime linear memory.
- On a flush-based route, treat `ON MESSAGE ERROR SEND TO` as a separately buffered error output
  governed by that route's same interval and maximum batch-size boundaries. General/global errors
  are node-wide and do not inherit route-local `FLUSH`.
- Require explicit sensitive-value leakage for external emission. Never place real credentials in
  an example unless the user explicitly supplied and requested them; prefer obvious placeholders.
- Preserve connector configuration as the documented string key/value surface. Do not translate
  options between different client libraries.
- Use only connector kinds listed in the current Ingestors and Emitters documentation. Treat
  connector syntax retained in older examples or configurations as invalid.
- For syslog, read the dedicated `Common` → `Syslog` documentation entry. Reference the predefined
  singleton wire schema directly with `FROM SYSLOG`, keep the format independent of the UDP/TCP/TLS
  client transport, and use only the documented `NO_ACK` source and sink forms.
- For a Sentry sink, reference a `TYPE SENTRY` client containing the project DSN and use a codec
  that emits one Sentry event JSON object per record. Do not add header writes to the Sentry route.
- List topics, queues, streams, tables, buckets, catalogs, namespaces, collections, and other
  external prerequisites separately. Nervix does not create them as a side effect of starting a
  node.

## Deliver usable configuration

When authoring a graph, provide:

1. Assumptions and external prerequisites.
2. Ordered command phases, separating client-local commands from transactional server statements.
3. Complete NSPL with consistent names and no unexplained ellipses. Use placeholders only for
   genuinely deployment-specific values such as endpoints, credentials, file paths, and external
   entity names.
4. A short verification sequence using the relevant `SHOW`, `DESCRIBE`, lookup, or subscription
   commands.

Use `DESCRIBE RELAY <relay>;` to verify its owner and optional state replicas; an ordinary relay
reports no replicas. Use `DESCRIBE JUNCTION <junction>;` when the verification should include a junction's stored
routing contract, scheduled placement, and local edge metrics.

Use `SHOW PLACEMENTS;`, `DESCRIBE PLACEMENT <placement>;`, and `DESCRIBE DOMAIN;` to verify rule
coverage, effective claims, colocation groups, and the domain default. Use
`DESCRIBE RELOCATION ...;` with the clauses of a planned `RELOCATE` to inspect the unit it would
move, its quiesce level, the relays its hold would gate, and the preferences it would leave
unsatisfied.

Before returning the configuration, trace every reference to its declaration and check schema,
branch, construction, flush, error, sensitivity, transaction, and external-provisioning contracts.
If the public docs do not establish a requested capability, say it is not documented as supported
instead of inventing syntax.
