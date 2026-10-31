---
name: plexus
description: Design, author, explain, review, and troubleshoot Nervix configurations written in Plexus. Use when a user wants to configure domains, schemas, codecs, branches, relays, resources, clients, ingestors, processors, emitters, placement policies, lookups, Roto UDFs, subscriptions, lifecycle commands, or complete Nervix streaming graphs. Produce current, valid Plexus and identify required external provisioning.
license: FCL-1.0-ALv2
---

# Configure Nervix with Plexus

Use Plexus, the Nervix graph definition language, to turn a user's streaming requirements into an explicit, deployable Nervix graph. Open the public
[Nervix Plexus documentation index](https://docs.nervix.io/llms.txt), then read the linked Markdown
needed for the request. Treat that versioned documentation as the authority; never reconstruct
clause order or connector options from memory.

For Plexus connector choices, read [Connector Capabilities](https://docs.nervix.io/connector-capabilities.md)
alongside the entity's Ingestors or Emitters section. Its generated tables show modes, option
requirements, metadata types, each side's header model, batching, buffer bounds, network waits and
the reasons ingest and emit differ. Read headers the way the source's model records them, such as
lowercase HTTP field names, and on an emitter that keeps one value per header name write each literal
header name once: a repeated literal name is refused when the statement is validated.
For outbound WebSockets, read the `WEBSOCKETS CONNECTION` section of Ingestors for literal upgrade
headers, response-header reads, frame metadata and the closed client CONFIG key set.
For ClickHouse HTTP connection tuning, read the ClickHouse Emitters section and the generated
client key reference. `DESCRIBE CLIENT <name>` shows effective defaults and configured values;
`SHOW CREATE CLIENT <name>` preserves authored configuration. Both redact secrets.

For delivery durations and direct-value mappings, follow the Ingestors delivery-mode and Emitters
direct-value sections. Nervix validates the entity's declared limits and exact mapping types at
`CREATE` and `ALTER`; correct a refused declaration before starting the domain.
For MQTT sources, read [Ingestors → MQTT](../../../docs/src/ingestors.md#mqtt) before promising
delivery during downstream stalls; it distinguishes QoS 0 overflow, acknowledged capacity and
the counters that expose each outcome.

For RabbitMQ consumption choose `MODE NO ACK`, `MODE ACK SEQUENTIAL`, or
`MODE ACK PARALLEL MAX <n> BATCH TIMEOUT <d> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d>`.
Parallel `MAX` is 1 through 65,535 and is the broker prefetch per instance. Each delivery settles
on its own graph outcome. Automatic consumption has no prefetch or transport acknowledgement;
work already accepted by the broker can be lost before the graph completes. Read the RabbitMQ
Ingestors section for the exact clause order, host admission and recovery limits.

For interactive authoring, the [CLI](https://docs.nervix.io/client-tools-cli.html) and
[web console](https://docs.nervix.io/client-tools-web-console.html) use the same server completion
contract. Refer users to those chapters for cursor edits, transaction-aware candidates, and
completion status messages.
Read the Ingestors and Emitters chapters for `SHOW INGESTORS`, `SHOW EMITTERS`, and `DESCRIBE`;
their inspection contracts distinguish effective options from canonical `SHOW CREATE` configuration.
The web console's visual Create forms also cover internal schemas, declared JSON/CBOR/AVRO wire
schemas, branches, relays, codecs, signaling protocols, clients, VHOSTs, endpoints, hash maps,
Roto UDFs, ingestors for every connector and entity pair, junctions, reingestors, and session
subscriptions; use the same web-console chapter for their typed fields, resource versions, program
editors, reference lookup, and transaction behavior.

## Gather the configuration contract

Establish these inputs before finalizing Plexus. Ask only for missing details that materially change
the graph; otherwise use conspicuous placeholders and state the assumptions.

- Domain: paced or unpaced, clock period/skew, start behavior, and default placement policy.
- Payload: sample input, wire format, exact internal field types, optional fields, and sensitive
  fields.
- Source and emitter: connector kinds, externally provisioned entity names, whether an operator
  provisions a Pulsar subscription or its ingestor owns it, endpoints, delivery/ACK expectations,
  emitter publishing mode, confirmation window and timeout where applicable, retry pacing,
  ordering, and offsets.
- Isolation: unbranched or a concrete branch key, branch TTL, and optional instance limit. A branch
  currently has one direct execution partition; do not write a `PARTITIONS` clause.
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
For Plexus endpoint ACK declarations, require the explicit response window and reject-only quiesce
policy; WebSocket replies use the endpoint's declared signaling format. The configuring reference
and the Ingestors endpoint section define status, outcome, timeout and retry behavior.
For Kafka domain offsets, retain the documented native checkpoint quorum budget and at-least-once
retry boundary when explaining source recovery.

## Assemble the graph

Run the control plane with `nervix-server` and submit configuration through the separate
`nervix-cli` client. Format saved `.plexus` files with `nervix-plexus-format`.
For a native CLI or Rust-client session addressed by hostname, Hickory resolves the selected
server, seeds, and redirects on connection. Keep the hostname in the `http://` or `https://` server
URL so HTTPS verifies that name. The CLI normally reads the system resolver and hosts files; use
its `--dns-resolver-config`, `--dns-hosts-file`, and repeatable `--dns-name-server` options only
when the session needs an explicitly configured resolver. Browser sessions use browser DNS.

Use the CLI's `subscribe` and `domain-clock` subcommands for shell streams of relay records and
the selected domain's clock. See [Command Line Client](https://docs.nervix.io/client-tools-cli.html)
for their line formats.

For diagnostic-node lock findings, follow the local report inspection and triage guidance in
[references/configuring-nervix.md](references/configuring-nervix.md#verification-and-troubleshooting).
Retain each potential cycle's source evidence and regression-backed review before qualification.

Build configuration in dependency order:

1. Create the domain, then select it with `USE <domain>;` as a separate client command. `BEGIN`
   is rejected until a selected domain exists.
2. Register and upload resources before statements that reference their versions or mounted files.
   Resources are domain-owned, so declare and upload them in the domain that references them.
   For pending replica diagnosis and upload completion, follow the resource lifecycle guidance.
   VHOST TLS, protobuf codec, protobuf signaling protocol, inferencer, WASM processor, hash map, and
   client mount bindings require `VERSION <n>` or `VERSION LATEST`. `LATEST` resolves to the highest
   completed version when the statement is applied (at `COMMIT` for queued statements) and the
   model stores that number; a later upload never moves a binding.
   Use `REBIND RESOURCE <name> TO VERSION <n>|LATEST` to move every existing usage atomically, or
   add `FOR <kind> <name>, ...` to select exact kind-qualified usages. Every selected member must
   already bind the resource. The target and all replacements validate together; `LATEST` is
   provisional at queue admission and resolved again at `COMMIT`. Rotating a VHOST certificate
   this way is dynamic: every node's HTTPS listener presents the new bundle and ingestion does not
   pause. A TLS binding error identifies the failing file kind without exposing its path or
   certificate material. Changing VHOST hostnames, adding or removing `WITH TLS`, or binding
   another TLS resource pauses the domain. Moving a WASM processor usage also discards the saved
   guest state of each branch, because a new module cannot restore what the module it replaces
   wrote; an uncompilable candidate module rejects the whole rebinding with every binding and its
   saved state still in place.
3. Define internal schemas, branch-key schemas, branches, wire schemas, and codecs.
4. Define clients with a `<CONNECTOR> CLIENT` head, such as `CREATE KAFKA CLIENT`. Define signaling
   protocols, virtual hosts/endpoints, lookup models, and trusted Roto UDFs as needed.
   Read the generated client CONFIG key tables in Connector Capabilities. Keys are lowercase and
   case-sensitive; declare required keys exactly once, follow their value types and pair TLS
   certificate/key files. Pass other property names only in an explicitly delegated driver
   namespace. CREATE also runs network-free Kafka/connection-URL checks before storing a client;
   credentials and connectivity still validate at initialization. Public SHOW CREATE and previews
   redact declared secrets and address credentials/query options; backups retain secret-bearing
   complete configuration under the archive contract.

5. Define relays before nodes that read or write them.
6. Define ingestors, processors, generators, and emitters in graph order.
7. Define placement rules after every referenced runtime node, including each relay, exists.
8. Commit the graph, inspect it, and start the active domain only when prerequisites exist.

For backup recovery, `RESTORE ... FROM '<file>' RESUME` (CLI `--resume`) installs compatible
materialized relay rows, deduplicator keys and windows on owners and replicas, then makes the domain
running at its archived start generation and latest start point. Paced recovery retains the
wall/logical mapping and time rate verbatim and projects downtime under a newly selected clock
authority. The default restore leaves the domain stopped; a normal `START` advances the generation
and clears materialized state while preserving compatible WASM state, source offsets, branch
lifecycle, deduplicator keys and windows. `RESUME` precedes the user policy and `DRY RUN`; the dry
run reports the planned status and generation. Read
[Backup And Restore](https://docs.nervix.io/backup-and-restore.html) for format and memory limits.

`BACKUP ... TIMEOUT` bounds each running domain's cut independently. For a cut timeout, inspect its
active phase and completed phase durations before choosing a new budget; the backup architecture's
Observability section explains owner-side attribution. Native clients use a separate
bounded overall backup wait (CLI `--backup-wait-timeout`, default `10m`). If that wait is uncertain,
retain its execution reference and recover with CLI `backup --execution-reference` and the same
selected domain, scope, resources and capture options; the output file or stdout may change.
Waiting again does not extend server retry validity or archive retention. A `WRITE_FAILED` from
`--output -` that names `error.archive` follows a complete download that released the server's
copy: deliver that verified archive instead of recovering by reference. One without a reference
took no backup, as when stdout is `/dev/null` or closed: fix stdout and run the backup again.

Use `BEGIN; ... COMMIT;` when sending multiple queueable configuration statements. A transaction
belongs to one already-existing domain: `BEGIN` binds it to the selected domain and every queued
statement must select that same domain. Transactions and commit progress are replicated and
resumable, but their content is deliberately limited to that domain's model mutations, domain
configuration/lifecycle, `CREATE RESOURCE`, and `RESET WASM PROCESSOR ... STATE`. Keep
`CREATE DOMAIN`, `CREATE USER`, other read-only statements, subscriptions, `USE`,
`ATTACH DOMAIN CLOCK`/`DETACH DOMAIN CLOCK`, resource uploads, backups, restores, and node
administration outside the transaction. Plan restore capacity for independently admitted archive
metadata and streamed checkpoint I/O, following the backup guidance in `references/configuring-nervix.md`.
Size node memory for every execution pool, including active snapshots and optional retained materialized-source reads;
the default startup requirement is 2,723 MiB. Read [Node Settings](https://docs.nervix.io/node-settings.html)
for the complete prerequisite checks.
Use `SHOW TRANSACTIONS;` when transaction state or a retained outcome needs
verification. Use `DESCRIBE TRANSACTION [ '<id>' ] [ OPERATION <n> ] [ FORMAT TEXT | JSON ];` to
explain what an open, committing, or retained transaction requires and changes before or after
`COMMIT`. These two reads are allowed while a transaction is open, are sent on their own, and neither
queues content nor shifts operation numbers. Read `Plexus Overview` for its forms and
[Transaction Quiescence And Impact Inspection](https://docs.nervix.io/transaction-quiescence.html)
for report scopes, execution outcomes, and retained inspection. Treat a successful inspection
as one complete result: selecting an operation changes presentation focus without narrowing the
transaction, and neither text, JSON, the Rust result, nor the browser graph paginates or truncates a
large report. Queue admission preflights each statement against the replicated prefix without
applying effects. Consecutive model mutations form one atomic run and report the run's effective
base-to-final quiesce level at the current prefix; a lifecycle, domain, resource-catalog, or WASM
state-reset statement ends that run, and a later run cannot repair it. `COMMIT` reports only the
maximum level actually executed and does not repeat statement outputs. Correct a rejected statement
and continue the same transaction. A `COMMIT` refused because the preview it expected no longer
describes the transaction applies nothing and leaves the transaction open; inspect the attached
transaction again before retrying the commit against its reviewed basis. Do not imply that one
undivided request can mix
those phases.

Treat a successful administrative command as a completed effect. After `UPLOAD RESOURCE`, model or
lookup creation, `START`, `STOP`, placement changes, or `COMMIT` returns `OK`, issue the dependent
operation immediately. Do not add a readiness request, `DESCRIBE` polling loop, arbitrary delay, or
retry before that dependent operation. Transaction statement success means durable validation and
staging only; `COMMIT` supplies the full usable-effect boundary. CLI, Rust-client, and browser
flows retain command execution references, upload identities, transaction append positions, and
commit identity through redirects or reconnects. Do not manufacture a new identity for an
uncertain admitted operation.

For storage failures or uncertain administrative outcomes, consult `Control Plane` → `Durability
and recovery` before suggesting a retry, and
[Exact Recovery](https://docs.nervix.io/client-session-protocol.html#exact-recovery) for how a retry
under the same execution reference recovers the recorded outcome instead of repeating the effect.

For model evolution, read the `Altering Schemas` section of `Schemas And Codecs` and the transaction
and quiesce semantics in `Control Plane`. Put every interdependent `CREATE`, supported `ALTER`, and
`DROP` for one domain in the same transaction, including schema, wire-schema, relay, junction,
deduplicator, reorderer, emitter, ingestor, reingestor, generator, and placement changes;
Nervix classifies the complete model diff and no user-facing pause command exists. Capacity and
expression-only junction changes and emitter flush changes are dynamic; relay schema or branching
changes pause the domain; structural junction, emitter/client/codec/collect/publishing-mode or
attachment changes, ingestor,
emitter source-predicate, and relay materialized-state changes gate and drain only affected
entities. Changing emitter `FROM` membership pauses the domain because it changes topology.
Deduplicator key and reorderer ordering changes also use entity pause; their `MAX TIME` changes are
dynamic. Write an ingestor's `ALTER` under the stored ingestor's own head, such as
`ALTER KAFKA TOPIC INGESTOR`; an alteration never changes its connector or entity, so reading
another one takes `DROP INGESTOR` and a new `CREATE`. Use that head's complete body after
`SET FROM`, or change only the quiesce mode with `SET QUIESCE <body>`; a client ingestor takes the
complete `SET FROM SCHEMA ...` body under `ALTER CLIENT PRODUCER INGESTOR`, and a change to its
endpoint contract ends its attached producers.
Buffered client routes force-flush and settle their admitted acknowledgements before an alteration
publishes a replacement contract. Keep reading admitted outcomes while admission is suspended;
reopen a producer after a committed contract change, and retain its attachment when an alteration
fails. Unresolved submissions keep their unknown-outcome boundary and must never be replayed
automatically.
Every reingestor and generator ALTER uses entity pause; reingestor route bodies retain their
per-route branch selection, while generator route bodies remain set-only.
`ALTER DOMAIN SET PLACEMENT` is nameless, targets the active domain, and performs a normal schedule
activation; a newly effective hard colocation requirement can relocate runtime nodes.

During node startup, `SHOW CLUSTER STATUS;` and `DESCRIBE CLUSTER;` remain available while live cluster policy is discovered. Status reports `node.initialization=initializing` until initialization finishes; wait for `ready` before policy-dependent commands or transfers.

Use `ALTER CLUSTER SET NODE_UNAVAILABILITY_TIMEOUT = 15s, STATE_SNAPSHOT_INTERVAL = 250ms;`
for committed Nervix cluster policy changes. Keep it outside domain transactions; it needs no
selected domain. Read `Node Settings` for the complete parameter set, positive duration bounds,
existing-model cap refusals, live installation and committed-versus-running inspection. Seed flags
belong only to the first bootstrap, including when their value equals the default. Later starts,
including the returning bootstrap node, omit them.
`HTTP_HEADER_FIELDS` ranges from 1 to 16,384, leaving capacity for client and transport headers. Committed header byte and field caps also protect
literal HTTP polling headers, at model creation and when a later cap is lowered.

## Preserve Plexus semantics

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
  time at delivery, including after quiescing; explicit source times remain unchanged.
  `TIMESTAMP AT metadata.<field>` takes a `DATETIME` metadata field the source's entity declares,
  such as an HTTP poll's `metadata.polled_at`; check the entity's Metadata row in `Ingestors`
  before writing any `metadata.<field>`, because an undeclared field is refused at `CREATE`. Check
  admission against the newest 256 reached logical centers with inclusive `SKEW`, independently
  of tick notification delivery. Never scale source timestamps by `TIME RATE` or admit against
  an unreached future center. Rejected or missing timestamps follow the route's message-error
  policy per row with code `validation` and operation `admit`.
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
  `Retry-After` interpretation use actual UTC, and a retried OTEL Export request keeps the observed
  time it was prepared with.
- Treat a session subscription `WHERE` clause as a predicate over the subscribed relay record.
  Bare fields, `message.<field>`, and `input.<field>` are equivalent there. Do not use `output`,
  `branch`, materialized `relay_state`, construction clauses, or side effects; subscription
  creation rejects them, and a selected record is delivered unchanged before sampling.
- Roto `now()` and the WASM domain-time import receive the owning execution snapshot explicitly.
  There is no context-free engine clock. Guest initialization, input, timeout, flush, and state
  lifecycle operations each use the snapshot selected for that operation; guest timeout delays are
  logical, while Wasmtime fuel and epoch yielding are physical safety controls.
- Do not simulate a targeted WASM guest-state reset with `ALTER`, resource rebinding, or a
  stop/start cycle. A coordinated reset is a control-plane state-lifetime operation with an explicit
  unbranched, concrete-branch, or all-branches target and a stable execution reference. Its one
  public statement is `RESET WASM PROCESSOR ... STATE`, described below, and the
  `ON REJECTED STATE RESET` policy triggers it for one refused branch lifetime; do not invent any
  other reset syntax. Guest code reaches the same operation for its own branch through the SDK's
  `request_state_reset` or the raw `nervix_request_state_reset` import, which discards that
  callback's uncommitted output and input and never re-enters the guest. `REBIND RESOURCE` does
  start a fresh lifetime for every branch of each WASM processor it moves, but only as the
  consequence of changing the module, and it cannot select a branch.
- Declare exact schema types and nullability. Use explicit conversions; never invent implicit
  casts between wire, internal, branch, processor, lookup, state, and emitter values.
- For every expression, read `Expression Functions`: `Where Expressions Run` for the functions each
  context allows, `Function Properties` and `Errors` for optional results, sensitivity, and where a
  failure goes, then the section of each operator and function the expression uses. Check that:
  - literals are `I64` and `F64`, so a narrower operand's literal is cast (`input.count > 5 AS U32`),
    a float is one word (`2.5`, never `2 .5`), and there is no exponent, `DATETIME`, or `BYTES`
    literal;
  - `NOT` binds tighter than comparisons and `AS` tighter than unary minus: write `NOT (a > b)` and
    `(-128) AS I8`;
  - a field, call, or `SET`/`INHERIT` target named like a reserved word (`end`, `from`, `in`, `all`,
    `null`, ...) is written between backticks where it stands bare (`` `end` = input.end ``), while
    `input.end` and `udf::case(...)` need none; a statement keyword such as `to`, `on`, or `by` is an
    ordinary name;
  - `AND`, `OR`, and `coalesce` evaluate every operand, so only an `IF` or `CASE` arm shields an
    operand that can fail, such as a division by a field that can be zero;
  - an optional result (`TRY_CAST`, JSON extraction, `nullif`, `LOOKUP_HASH_MAP`, `regexp_substr`,
    `regexp_extract`, URL components, list `first`/`last`/`nth`/`sum`/`min`/`max`/`mean`, sample
    window statistics, `CASE` without `ELSE`) reaches a required field only through `coalesce`;
  - every route whose functions can fail has an `ON MESSAGE ERROR` policy, which also receives a
    failed ingestor `BRANCHED BY ... SET`, and expressions that can fail stay out of deduplication
    keys, reorderer `BY`, `CORRELATE WHERE`, inferencer `INPUTS`, and a reingestor's
    `BRANCHED BY ... SET`, which have no error route;
  - `IN` sets hold non-null constants of the operand's exact type, and string positions count from
    1 while `nth`, `slice`, and JSON path indexes count from 0;
  - datetime units, parts, `date_bin` widths, zones, formats, and disambiguations are literals, a
    local calendar names its zone, `now()` supplies domain time, and foreign timestamps are read
    with a strict `parse_datetime` format;
  - IP text is parsed once into a `BYTES` field, IPv4-mapped addresses are unmapped before IPv4
    network tests, URL inputs are absolute, and many JSON fields are read from one document field;
  - every sensitive value stored in a field that is not sensitive goes through `leak_sensitive(...)`;
    hashing, counting, or testing a sensitive value keeps the result sensitive.
- In window routes, read `Expression Functions` → `Window Aggregates`. Aggregates appear only in
  window route `SET`, `input` only inside their arguments, and `COUNT`, `SUM`, `FIRST`, `LAST`,
  `MIN`, and `MAX` are aggregates there, never list functions. Compute window output from aggregates
  and constants, and prefer the dedicated aggregates (`AVG`, `COUNT_IF`, `ARG_MIN`, `ARG_MAX`, the
  `*_POP` and `*_SAMP` statistics, `CORR`) over hand-built formulas. For `APPROX_COUNT_DISTINCT`,
  `APPROX_QUANTILE`, and `APPROX_TOP_K`, read `Approximate Sketches`: the window needs
  `MAX STATE SIZE` after duration `WIDTH` and `STEP`, a branched window a branch with
  `MAX INSTANCES <n> EVICT LRU`, and the precision or capacity sets both accuracy and memory.
- Use a separate wire schema and codec when transport shape differs from the internal runtime
  schema. Declare datetime encoding explicitly when required: a JSON or CBOR `string` wire field
  binds a `DATETIME` field only with `ENCODE <field> AS RFC3339`, while an exact `DATETIME` wire
  type, like the exact `U8` through `I64`, `F32` and `F64` types, binds only the internal type of
  its own name. JSON and CBOR carry no NaN or infinity; read `Schemas And Codecs` → `Value
  Fidelity` before relying on non-finite floats or unsigned Avro list elements.
- For every JAQ-backed codec, use `WITH JAQ TRANSFORMATIONS` and declare `ON INGESTION`,
  `ON EMITTING`, or both in that order. At least one direction is required.
- An `ON INGESTION` program runs once per value a payload holds, and every object it yields becomes
  one message: `.[]` unfolds an array, and a program that yields nothing acknowledges the payload
  without a message. A payload is decoded or rejected as a whole, unfolds into at most 65,536
  messages, and stays the unit of source acknowledgement. `ON EMITTING` must yield exactly one
  value per record.
- A payload its codec refuses is consumed, never redelivered: the ingestor's `ON GENERAL ERROR LOG`
  reports it as a decode rejection, `IGNORE` only counts it, and an acknowledged source commits past
  it. Read `Ingestors` → `When The Codec Refuses A Payload` before relying on a broker's
  redelivery or dead-lettering for malformed payloads.
- Unfolding runs on the node's extension workers. Before relying on a `MODE NO ACK` source to keep
  its payloads while those workers are full, read `Ingestors` → `When The Extension Workers Are
  Full`: some sources wait for them, and others refuse the payload and count it.
- Give every signaling protocol an explicit `FORMAT` and express the handshake as JAQ:
  `SEND JAQ` programs must each yield exactly one value, and `WAIT JAQ` matchers accept any output
  that is neither null nor false. Match only the fields that matter so acknowledgements carrying
  connection ids or timestamps still match.
- Write signaling steps in the order they must happen; each completes before the next starts, so a
  send that depends on an earlier reply goes after the wait for it. A `WAIT` step may list several
  matchers when their frames may arrive in any order. Use `CAPTURE` on a single-matcher step to
  record values, and read them in later programs through `$state`.
- Use `ACCEPT DATA` on `ON CONNECT` or on the `WAIT` step that must finish before accepting payload.
  Frames arriving before acceptance are dropped. For an outbound client's bounded handshake
  handoff and delivery after readiness, read `Ingestors` → `WEBSOCKETS CONNECTION`.
- Scope rejection to where it applies: `FAIL JAQ` on a `WAIT` step aborts during that step, and a
  `FAIL JAQ` written before `ON CONNECT` applies throughout the handshake.
- Preserve written operation order in schema, relay, junction, deduplicator, reorderer, emitter,
  ingestor, reingestor, generator, and placement ALTER statements.
  Include every dependent wire, internal, codec, and node mutation required for the candidate graph
  to validate in the same transaction.
- Treat JSON, CBOR, and AVRO wire schemas as distinct entity kinds. Their names may coincide, so
  every create, alter, show, drop, and codec reference must include the exact format.
- Use `BYTES` for arbitrary octets, including `ARRAY` and `VEC` elements. Keep branch key schemas
  free of `BYTES`. JSON and CBOR wire `BYTES` fields carry padded standard base64 text; AVRO uses
  native bytes. A cast never converts `BYTES`; use the functions of `Expression Functions` →
  `Bytes, Encodings And Hashes`.
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
  On native 64-bit targets, `CAPACITY` accepts positive integers through `18446744073709551615`;
  creation and alteration preserve the exact capacity through storage, replay, and restart.
- Treat `HTTP ENDPOINT`, `WEBSOCKETS ENDPOINT`, and `SYSLOG LISTENER` ingestors as cluster-wide
  listeners and exclude them from placement members. Every other ingestor, a `WEBSOCKETS CONNECTION`
  included, is single-owner and keeps its live assignment across ordinary schedule recomputation;
  use drain, `RELOCATE`, or a hard
  colocation requirement when it must move.
- Use `RELOCATE <selection> ONTO NODE <node_id> FOLLOW PREFERENCES | IGNORE PREFERENCES;` to move
  chosen work onto a named cluster node. The selection is a kind-qualified list or a
  `FROM ... TO ...` corridor, hard colocation groups always move whole, and the whole unit moves in
  one gated handoff or not at all. It is a one-time move, not pinning.
- An emitter may list multiple `FROM <relay> [WHERE <expr>]` inputs when every relay declares the
  same payload schema. Unlike ordinary processors, those inputs may use differently named
  branches. Keep collection separate per source relay and concrete branch, and remember that one
  node-wide materialized dependency or message-error relay must be exact-branch compatible with
  every source.
- Give every emitter an explicit transport-supported `MODE` as the final emitter subclause before
  `ENCODE USING`. Include every required variable: all modes declare `RETRY POLICY BACKOFF <d> MAX
  <d>`; asynchronous confirming modes also declare `ACK SEQUENTIAL` or `ACK PARALLEL MAX <n>` and
  `ACK TIMEOUT <d>`. Do not invent a default mode, window, timeout, or retry cadence.
- When an application receives constructed graph output, use `CREATE CLIENT CONSUMER EMITTER <name> FROM <relays> TO SCHEMA <output_schema>
  MODE ACK SEQUENTIAL|ACK PARALLEL MAX <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d>`.
  Follow route construction with required `BATCH MAX MESSAGES <1..65536> MAX SIZE <bytes>` and an
  explicit `FLUSH` policy. The output schema is exact, and every sensitive value copied into a
  non-sensitive output field needs `leak_sensitive(...)`. Do not create a `CLIENT` model, codec,
  header operation, or direct `VALUES` body for this emitter. An application opens a competing
  consumer with the Rust client's `subscribe_emitter` or the session protocol, receives native
  Arrow, then explicitly ACKs, retries, or rejects each attempt. `ATTACHED` holds source ACKs
  until the application ACKs; `DETACHED` releases the source earlier. Retries preserve the bytes
  and delivery identity but use a fresh reference. Consumers are volatile across owner and
  session loss; the Rust client may restore the same handle with a fresh attachment only when the
  domain `START` generation and endpoint contract still match. The first read after a session gap
  or endpoint relocation reports interruption, and an old delivery reference cannot ACK a
  replacement attempt. Design
  application effects for duplicates and uncertain ACK confirmation. Read `Emitters` → `Client emitters`,
  `Sessions` → `Emitter Consumers`, and the Client Implementation Manual for limits and recovery.
- Request/response emitters do not take `ACK TIMEOUT`. ClickHouse, OTEL and Sentry clients require a
  positive `timeout_ms`; SQS or Iceberg object-store clients use it when a request
  needs an explicit bound. A ClickHouse HTTPS address also requires `tls_ca_file` from a mounted
  resource, while HTTP refuses TLS file keys. Its table locator has one or two case-preserving
  dot-separated parts, each quoted by the connector when sent. The emitter's declared retry
  policy owns pacing after that request fails. A ZeroMQ client's `timeout_ms` bounds each send,
  which otherwise waits for as long as the peer reads nothing. Prefer a bound ZeroMQ PULL
  ingestor and a connecting ZeroMQ emitter: the driver never reconnects a connecting PULL socket
  after its peer goes away, and a
  bound emitter can keep failing after one failed publish; read `Ingestors` → `ZeroMQ` and
  `Emitters` → `ZeroMQ`. OTEL clients
  must also select `grpc` or `http/protobuf` explicitly with the required `protocol` key. An OTEL
  endpoint may use a DNS hostname on either protocol: the emitter opens without contacting it, the
  node's resolver answers when an export needs a connection, and the configured hostname remains
  the HTTPS certificate name. An endpoint that does not resolve or answer is a publish failure
  retried on the emitter's declared retry policy, never an initialization error.
- For a ZeroMQ PULL/PUSH pair, use `MODE NO ACK` on both sides and the typed `ZEROMQ CLIENT`
  `addr` and `bind` keys. A PULL ingestor reads frame 0 only and skips messages with no frames;
  PUSH sends one payload frame without a receiver receipt. TCP hostnames use the ZeroMQ driver's
  system resolver. Read [Ingestors](../../../docs/src/ingestors.md#zeromq) and
  [Emitters](../../../docs/src/emitters.md#zeromq) before setting bind direction and deadlines.
- Client deadline keys mean the same thing everywhere: `timeout_ms` bounds a whole request,
  name resolution, connection and TLS included, `connect_timeout_ms` bounds establishing a
  connection, the protocol handshake included, and `idle_timeout_ms` bounds how long a connection
  may carry nothing while its connector waits to read. RabbitMQ, Redis, Syslog emitter, ZeroMQ
  emitter and WebSocket clients take `connect_timeout_ms` and allow 30 seconds without it, while a
  ZeroMQ PULL ingestor reads none of these keys; a RabbitMQ client's deadline also covers the AMQP
  handshake and opening the entity's channel, and its `timeout_ms` bounds the round trip that ends
  a `NO ACK` write. A NATS client's `timeout_ms` bounds a Core NATS write and its flush to the
  client's socket, which is not a server round trip. A Syslog emitter's client bounds each frame
  write by `timeout_ms`; a TLS Syslog listener's client bounds each accepted handshake by
  `connect_timeout_ms`, 30 seconds without it, and a TCP or TLS listener's client may close idle
  connections with `idle_timeout_ms`. Put each key only on a client whose direction performs the
  wait it bounds. Read `Ingestors` or
  `Emitters` → `Client Deadlines`, and `Connector Capabilities` for each entity's waits and what
  bounds them.
- For an HTTP emitter, write `CREATE HTTP REQUEST EMITTER <name> FROM <relay> TO <client> METHOD <string_expression> PATH
  <string_expression> MODE ACK RETRY POLICY BACKOFF <duration> MAX <duration>` followed by exactly
  one of `ENCODE USING <codec>` or `WITHOUT BODY`. Do not add an ACK window, `ACK TIMEOUT`,
  `NO ACK` or `BATCH`; see [Emitters](../../../docs/src/emitters.md#http-request-configuration)
  for client origin and timeout requirements, request-field types and sensitivity, bodyless
  construction, and ALTER rules. Each record is one request: route `WHERE` filters before any
  request field is evaluated, a failed method, path, header write or body encoding rejects the
  record through `ON MESSAGE ERROR` before any part of its request is sent, and a retry resends the
  prepared request unchanged. Declare
  `Content-Type` and any idempotency key with `write_header`; see
  [HTTP requests](../../../docs/src/emitters.md#http-requests). Complete valid `2xx` response
  headers deliver the record without waiting for the response body. `401`, `403`, `407`, `408`,
  `425`, `429`, and `5xx` retain it for retry; other `3xx`/`4xx` statuses and `101` reject that
  record through `ON MESSAGE ERROR`. An HTTP client follows a redirect, on both sides, only with
  `'follow_redirects' = true` and only to its own origin, and uses only its declared `'proxy'`,
  never one from the environment; see
  [HTTP client transport](../../../docs/src/emitters.md#http-client-transport). A retained request holds later records behind it, and an
  `ATTACHED` emitter keeps every unresolved record's upstream acknowledgement open. One valid
  `Retry-After`, in whole seconds or as an HTTP date, can lengthen the physical wait beyond `MAX`.
  A lost response can repeat a request the endpoint applied, so give the endpoint a stable key
  such as `write_header('Idempotency-Key', input.event_id)`; see
  [HTTP retries](../../../docs/src/emitters.md#http-retries-and-acknowledgements). While a request
  is pending after a failed attempt, `DESCRIBE EMITTER` shows that failure's status or transport
  cause as its transient error until the request is delivered. Sent counters count each delivered
  record once and never a refused one, and a `WITHOUT BODY` emitter sends zero payload bytes; see
  [HTTP inspection](../../../docs/src/emitters.md#http-inspection-and-metrics).
- Write a supported emitter's optional `BATCH MAX MESSAGES <1..65536> MAX SIZE <bytes>` after the complete
  emitter clause and route construction, before `FLUSH`; it is required for CLIENT, ClickHouse,
  Postgres, MySQL, and MongoDB emitters and limited to `256KiB` for SQS. A batching Sentry emitter needs a
  codec with `ON EMITTING BATCH`, and a batching protobuf codec needs `BATCH MESSAGE`. Compatible
  rows from successive Arrow carriers in one flush may share a payload, but rows from different
  source relays or concrete branches cannot. For the database emitters, the clause bounds each insert
  or bulk write by row count and the exact size of what it carries; for OTEL, it bounds each
  protobuf export request by successful source-record count and uncompressed encoded size, and an
  OTEL request never combines records of two Arrow carriers; see
  [Emitters](../../../docs/src/emitters.md#batching).
  For SQS, use `FIFO GROUP FROM BRANCH|<string_expression>` exactly when the externally provisioned
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
  the client name and before an optional `MOUNT`: `POOL SIZE MIN <u32> MAX <positive_u32>`, in that order,
  with the minimum no greater than the maximum. The clause is required even when only ingestors
  reference the client, and no other client type accepts it. Never put pool sizing in connector
  `CONFIG` or an address query parameter. One pool serves every local user of a named client on one
  node, so size it for the node's whole workload rather than per emitter, and expect one pool per
  node the client is placed on. Read the dedicated `Common` → `Database Client Connection Pools`
  documentation entry before choosing values.
- A `REDIS` client's command pool takes `pool_idle_timeout_ms` and `pool_max_lifetime_ms`, 10 and
  30 minutes by default, and never applies them to a Pub/Sub ingestor's subscription connection.
  Read `Connection lifetimes` in that same pools entry before choosing values; `DESCRIBE CLIENT
  <name>` shows both, marking an omitted one `(default)`.
- Give a `POSTGRES` client an absolute `postgres://` or `postgresql://` `addr` URL that sets
  `sslmode` once, to either `disable` or `verify-full`; no other mode is accepted and there is no
  opportunistic fallback. Mounted `tls_ca_file`, `tls_cert_file`, and `tls_key_file` are the
  TLS-file interface, certificate and key must be supplied together, and TLS files require
  `verify-full`. A `POSTGRES TABLE` names `'Orders'` or `'sales.Orders'`, each part sent with its
  case kept; `ON CONFLICT (...) DO UPDATE` needs a target and a mapped column outside it, and
  `MAX SIZE` is at most `1073741822B`. Read
  [Emitters → Postgres](../../../docs/src/emitters.md#postgres).
- Give a `RABBITMQ` client an `amqp://` or `amqps://` `addr` that names a host and no port 0; Nervix
  refuses any other address before commit. Mounted `tls_ca_file`, `tls_cert_file` and
  `tls_key_file` require `amqps`; `CREATE` refuses these keys with `amqp`. Certificate and key must
  be supplied together, and the connection presents them to a broker that requires a client
  certificate. Read
  [Ingestors → RabbitMQ](../../../docs/src/ingestors.md#rabbitmq).
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
  exactly as elsewhere, while emitter retry backoff, acknowledgement keepalive, emitter
  acknowledgement timeouts, and an HTTP `Retry-After` stay physical. Treat `FLUSH IMMEDIATE` as the
  physical, system-owned 100 µs minimum batching window, not a one-message batch guarantee; domain
  pacing never scales it. Both timers start when an empty route buffer receives data. `MAX BATCH SIZE`
  counts logical Arrow value, offset, and validity bytes, not unused buffer capacity or object
  overhead. Windows use `WIDTH` and `STEP`; WASM output cadence is controlled by the guest. Choose
  `FLUSH` values as latency and boundary-cost controls, not as a throughput lever: `MAX BATCH SIZE`
  only clamps a batch, and the flush tuning guidance in the docs records which emitters benefit from
  larger batches.
- Use delivery-mode `MAX <n>` only with `ACK PARALLEL`; an ingestor's `NO ACK` and an emitter's
  `NO ACK` have no in-flight ACK window and never accept `MAX`.
- Do not promise that an emitter's `NO ACK` survives a stop on every emitter. A `STOP`, a replacement
  or a node shutdown sends what a Kafka producer still queues, awaits the receipts of what a Pulsar
  producer accepted and waits for an MQTT QoS 0 client to send what it holds, all within its
  deadline, reporting what is outstanding when it passes. Choose an `ACK` mode where a stop must not
  lose emitted records; `Emitters` → `ACK Semantics And Effective Delivery` has the rule.
- Name the connector and entity in every emitter head, such as `CREATE KAFKA TOPIC EMITTER`,
  `CREATE HTTP REQUEST EMITTER`, `CREATE OTEL METRICS EMITTER`, or `CREATE ICEBERG TABLE EMITTER`.
  After `TO`, write its reference, quoted external locator and entity options, then `MODE`, then
  its codec, `VALUES` mapping or `WITHOUT BODY`. Client consumers use `TO SCHEMA <schema> MODE ACK`.
  Iceberg resolves its storage backend from its client; put required `NAMESPACE`, then `LOCATION`,
  `CATALOG` and `COMMIT EACH` before `MODE`. Put OTEL attributes, resource and scope with `VALUES`
  after `MODE`.
  Use the stored kind in every emitter ALTER head. `SET TO` requires that entity's complete body,
  including its payload selection; changing the connector or entity requires drop and create.
  External table and collection names retain case. Keep client/schema/codec references as identifiers.
- Name the connector and entity in every ingestor head, such as `CREATE KAFKA TOPIC INGESTOR`,
  `CREATE HTTP ENDPOINT INGESTOR`, or `CREATE SYSLOG LISTENER INGESTOR`, and write the body after
  `FROM` without a connector keyword. Reference a client of the head's connector, and pair
  `HTTP ENDPOINT` with a `TYPE HTTP` endpoint and `WEBSOCKETS ENDPOINT` with a `TYPE WEBSOCKETS`
  endpoint. Write every source's `MODE`, using `MODE NO ACK` where nothing waits on an
  acknowledgement and only the modes `Ingestors` → `Delivery Modes` lists for that entity.
- Write external names as string literals spelled exactly as the external system names them:
  topics, consumer groups, subscriptions, queues, channels, subjects, queue groups, and MQTT topic
  filters, within the rules in `Ingestors` → `External Names`. Keep Nervix references such as
  clients, endpoints, schemas, codecs, and relays bare identifiers.
- When an application publishes typed batches itself, use a client ingestor:
  `CREATE CLIENT PRODUCER INGESTOR <name> FROM SCHEMA <schema> MODE ACK SEQUENTIAL|ACK PARALLEL MAX
  <n> ACK TIMEOUT <d> RETRY POLICY BACKOFF <d> MAX <d> ON QUIESCE SUSPEND`, with no client, codec,
  `DECODE USING`, headers, or `NO ACK`. Producers are opened through the client library or session
  protocol, not with a statement. Read `Ingestors` → `Client Ingestors` before explaining
  outcomes, limits, or what ends a producer, and never promise that a batch whose outcome is
  unknown or failed had no effect. The Rust client restores a desired producer across a session gap
  or relocation only while the domain `START` generation and endpoint contract still match; it
  never resends a batch merely because its outcome is missing.
- End every transport ingestor source body with an explicit source-supported `ON QUIESCE` body
  immediately before `DECODE USING`. Include positive `MAX SIZE` and, outside `HTTP ENDPOINT` and
  `WEBSOCKETS ENDPOINT`, an explicit `ON OVERFLOW DROP OLDEST|DROP NEWEST` for `BUFFER`; include
  `RETRY AFTER` for endpoint `REJECT`. Use `MQTT TOPIC` `SUSPEND` only with
  `SESSION PERSISTENT QOS 1`. Do not invent a default or use a mode offered by another entity.
- A server endpoint WebSocket keeps its route and signaling protocol for the connection. After
  its source stops or is replaced, reconnect to use the new source; after signaling, the next
  refused payload on the preceding connection closes with 1013. Configured routes without a live
  intake reject with HTTP 503, while withdrawn routes return 404. A node that has not installed a
  revision since it started answers 503 for every unpublished route, never 404. Another domain
  sharing the host and path retains its own intake when a domain is stopped.
- Declare both required WASM limits immediately after `FILE`, in order: `MAX FUEL <positive_u64>
  MAX MEMORY <positive_byte_size>`. Fuel is reset per logical guest operation; memory caps each
  branch guest's Wasmtime linear memory.
- Write the optional `ON REJECTED STATE PRESERVE|RESET` between the last `TO` clause and
  `ON GLOBAL ERROR`. It answers only a guest's own verdict on the snapshot it was handed, and it
  defaults to `PRESERVE`. Offer `RESET` only when the user accepts losing that branch's computation
  state, and say that it replaces the lifetime once rather than retrying. Never present it as a
  recovery for a compile, initialization, fuel, memory, storage, replication, or authority failure;
  the policy discards state for none of those under either value. Only an owner loss whose forced
  recovery cannot prepare the new owner recreates guest state without the guest's verdict.
- To intentionally replace an existing WASM processor's guest state, use
  `RESET WASM PROCESSOR <processor> STATE IN DOMAIN <domain> FOR UNBRANCHED|ALL BRANCHES|BRANCH VALUES { <field> = <literal>, ... };`.
  Choose the explicit scope that matches its declared branch, and supply every branch-key field
  with an exact typed literal for one selected branch. The processor must be running and the
  selected branch must already exist. This command loses the selected computation state; a fresh
  command reference is a new reset, while retrying the same reference returns its original outcome.
  Keep that reference when publication loses leadership while the reset is applying;
  `Command Completion` defines recovery and retained failures.
  See the reset section in `Runtime Nodes` and `Command Completion` before suggesting it.
- To inspect WASM guest-state progress, use `DESCRIBE WASM PROCESSOR <processor> [FORMAT TEXT|JSON];`.
  Read its reset phase and generation beside the checkpoint revision and replica counts; both
  formats use the same typed state inspection. See `Runtime Nodes` and `WASM Processor Guests` for
  the meaning of each stage and the bounded branch details.
- On a flush-based route, treat `ON MESSAGE ERROR SEND TO` as a separately buffered error output
  governed by that route's same interval and maximum batch-size boundaries. General/global errors
  are node-wide and do not inherit route-local `FLUSH`.
- Declare the error relay with the source route's exact named branch, or unbranched for an
  unbranched source. See `Runtime Nodes` for the branch and error-record contract.
- Require explicit sensitive-value leakage for external emission. Never place real credentials in
  an example unless the user explicitly supplied and requested them; prefer obvious placeholders.
- Preserve connector configuration as the documented string key/value surface. Do not translate
  options between different client libraries.
- Use only connector kinds listed in the current Ingestors and Emitters documentation. Treat
  connector syntax retained in older examples or configurations as invalid.
- For syslog, read the dedicated `Common` → `Syslog` documentation entry. Reference the predefined
  singleton wire schema directly with `FROM SYSLOG`, keep the format independent of the UDP/TCP/TLS
  client transport, and use only the documented `SYSLOG LISTENER INGESTOR ... MODE NO ACK` source
  and `NO ACK` emitter forms.
- For a Sentry emitter, reference a `SENTRY CLIENT` containing the project DSN and a positive
  `timeout_ms`, and use a codec that emits one Sentry event JSON object per record. Do not add header
  writes to the Sentry route.
- List topics, queues, streams, tables, buckets, catalogs, namespaces, collections, and other
  external prerequisites separately. Nervix does not create them as a side effect of starting a
  node.

## Deliver usable configuration

When authoring a graph, provide:

1. Assumptions and external prerequisites.
2. Ordered command phases, separating client-local commands from transactional server statements.
3. Complete Plexus with consistent names and no unexplained ellipses. Use placeholders only for
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
