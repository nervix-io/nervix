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
3. Define internal schemas, branch-key schemas, branches, wire schemas, and codecs.
4. Define clients, signaling protocols, virtual hosts/endpoints, lookup models, and trusted Roto
   UDFs as needed.
5. Define relays before nodes that read or write them.
6. Define ingestors, processors, generators, and emitters in graph order.
7. Define placement rules after every referenced runtime node and materialized relay exists.
8. Commit the graph, inspect it, and start the active domain only when prerequisites exist.

Use `BEGIN; ... COMMIT;` when sending multiple queueable configuration statements. A transaction
belongs to one already-existing domain: `BEGIN` binds it to the selected domain and every queued
statement must select that same domain. Transactions and commit progress are replicated and
resumable, but their content is deliberately limited to that domain's model mutations, domain
configuration/lifecycle, and `CREATE RESOURCE`. Keep `CREATE DOMAIN`, `CREATE USER`, read-only
statements, subscriptions, `USE`, resource uploads, and node administration outside the
transaction. Use `SHOW TRANSACTIONS;` when transaction state or a retained outcome needs
verification. Queue admission preflights each statement against the replicated prefix without
applying effects; a queued model mutation reports its statement-local quiesce level, while
`COMMIT` reports only the maximum level actually executed and does not repeat statement outputs.
Correct a rejected statement and continue the same transaction. Do not imply that one undivided
request can mix those phases.

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

- Declare exact schema types and nullability. Use explicit conversions; never invent implicit
  casts between wire, internal, branch, processor, lookup, state, and sink values.
- Use `IF ... THEN ... ELSE ... END` or searched/simple `CASE` for conditional values. Keep every
  result at one exact type; remember that omitted `CASE ELSE` yields a typed null and requires an
  optional destination.
- Use a separate wire schema and codec when transport shape differs from the internal runtime
  schema. Declare datetime encoding explicitly when required.
- For every JAQ-backed codec, use `WITH JAQ TRANSFORMATIONS` and declare `ON INGESTION`,
  `ON EMITTING`, or both in that order. At least one direction is required.
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
- Treat every route as a newly constructed output. Add `INHERIT` only where that node permits it,
  and initialize every required output field on set-only routes.
- Add a route-local message error policy. Add the required general/global policy for the chosen
  node.
- Add `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` after a relay input list only when the node
  should assemble input batches before execution. Omission means no additional input collection.
  The policy is per source relay and concrete branch; correlators configure each side
  independently. Never add it to an ingestor.
- Add `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` to every flush-based
  route. Treat `FLUSH IMMEDIATE` as the system-owned 100 µs minimum batching window, not a
  one-message batch guarantee. `MAX BATCH SIZE` counts logical Arrow value, offset, and validity
  bytes, not unused buffer capacity or object overhead. Windows use `WIDTH` and `STEP`; WASM output
  cadence is controlled by the guest.
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

Use `DESCRIBE JUNCTION <junction>;` when the verification should include a junction's stored
routing contract, scheduled placement, and local edge metrics.

Use `SHOW PLACEMENTS;`, `DESCRIBE PLACEMENT <placement>;`, and `DESCRIBE DOMAIN;` to verify rule
coverage, effective claims, colocation groups, and the domain default.

Before returning the configuration, trace every reference to its declaration and check schema,
branch, construction, flush, error, sensitivity, transaction, and external-provisioning contracts.
If the public docs do not establish a requested capability, say it is not documented as supported
instead of inventing syntax.
