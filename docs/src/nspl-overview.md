# NSPL Overview

NSPL is the language used to define the Nervix graph.

## Language Stability

NSPL has reached alpha. Its period of rapid, broad experimentation is over, and large
backward-incompatible changes are no longer expected. The language is more stable, but alpha is
not a promise of backward compatibility: NSPL will continue to evolve, and focused breaking
changes may still be made when they materially improve the language model or correctness.

AI coding agents can use the portable [NSPL Agent Skill](nspl-agent-skill.md) to design, explain,
review, and troubleshoot Nervix configurations. The guide explains installation without cloning
this repository, skill invocation, useful request details, expected output, and updates.

The current top-level surface includes:

- domain lifecycle statements
- user statements
- create/alter/drop model statements
- resource lifecycle statements
- session subscription statements
- explicit transaction controls: `BEGIN`, `COMMIT`, and `REVERT`
- describe and show commands

Core create statements:

```nspl,ignore
CREATE [IF NOT EXISTS] USER <name> WITH PASSWORD '<password>';

CREATE [IF NOT EXISTS] SCHEMA <name> (<field> <type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] WIRE JSON SCHEMA <name> MODE STRICT|LOOSE (<field> <json_type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] WIRE CBOR SCHEMA <name> MODE STRICT|LOOSE (<field> <cbor_type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] WIRE AVRO SCHEMA <name> MODE STRICT|LOOSE (<field> <avro_type> [OPTIONAL], ...);

CREATE [IF NOT EXISTS] CODEC <name>
  FROM WIRE JSON SCHEMA <wire_schema>
  TO SCHEMA <schema>
  [ENCODE <field> AS RFC3339, ...];

CREATE [IF NOT EXISTS] CODEC <name>
  FROM WIRE CBOR SCHEMA <wire_schema>
  TO SCHEMA <schema>
  [ENCODE <field> AS RFC3339, ...];

CREATE [IF NOT EXISTS] CODEC <name>
  FROM WIRE AVRO SCHEMA <wire_schema>
  TO SCHEMA <schema>
  [ENCODE <field> AS RFC3339, ...];

CREATE [IF NOT EXISTS] CODEC <name>
  FROM JSON|YAML|TOML|XML|CBOR
  TO SCHEMA <schema>
  WITH JAQ TRANSFORMATIONS
  [ON INGESTION '<program>']
  [ON EMITTING '<program>'];

CREATE [IF NOT EXISTS] CODEC <name>
  FROM PROTOBUF
  USING RESOURCE <resource> [VERSION <n>]
  CONFIG {'file' = '<path.proto>', 'include' = '.'}
  MESSAGE '<package.Message>'
  TO SCHEMA <schema>
  WITH JAQ TRANSFORMATIONS
  [ON INGESTION '<program>']
  [ON EMITTING '<program>'];

CREATE [IF NOT EXISTS] CODEC <name>
  FROM SYSLOG
  TO SCHEMA <schema>;

CREATE [IF NOT EXISTS] RELAY <name> SCHEMA <schema> [CAPACITY <n>]
  [WITH MATERIALIZED STATE LAST BY TIMESTAMP];

CREATE [IF NOT EXISTS] PLACEMENT <name>
  FROM <runtime_node> [, <runtime_node> ...]
  TO <runtime_node> [, <runtime_node> ...]
  REQUIRE COLOCATION|PREFER COLOCATION|NEUTRAL|SUGGEST SEPARATION
  [RANK <n>];
```

JAQ-backed codecs require at least one direction. When both are present, `ON INGESTION` precedes
`ON EMITTING`.

Core alter statements:

```nspl,ignore
ALTER DOMAIN SET PLACEMENT
  REQUIRE COLOCATION|PREFER COLOCATION|NEUTRAL|SUGGEST SEPARATION;

ALTER RELAY <name>
  SET CAPACITY <n> |
  SET SCHEMA <schema> |
  SET BRANCHED BY <branch> |
  SET UNBRANCHED |
  SET MATERIALIZED STATE LAST BY TIMESTAMP |
  DROP MATERIALIZED STATE
  [, ...];

ALTER JUNCTION <name>
  ADD FROM <relay> [WHERE <expr>] |
  DROP FROM <relay> |
  ALTER FROM <relay> SET WHERE <expr> |
  ALTER FROM <relay> DROP WHERE |
  SET COLLECT FOR <duration> [MAX BATCH SIZE <bytes>] |
  DROP COLLECT |
  SET FILTER WHERE <expr> |
  DROP FILTER WHERE |
  SET ATTACHED | SET DETACHED |
  SET BRANCHED BY <branch> | SET UNBRANCHED |
  ADD|DROP|ALTER MATERIALIZED STATE ... |
  ADD|DROP|REPLACE ROUTE ...
  [, ...];

ALTER DEDUPLICATOR <name>
  SET DEDUPLICATE ON <expr> [, <expr> ...] |
  SET MAX TIME <duration> |
  ADD|DROP|ALTER FROM ... |
  SET|DROP COLLECT ... |
  SET|DROP FILTER WHERE ... |
  SET ATTACHED | SET DETACHED |
  SET BRANCHED BY <branch> | SET UNBRANCHED |
  ADD|DROP|ALTER MATERIALIZED STATE ... |
  ADD|DROP|REPLACE ROUTE ...
  [, ...];

ALTER REORDERER <name>
  SET BY <expr> [, <expr> ...] |
  SET MAX TIME <duration> |
  ADD|DROP|ALTER FROM ... |
  SET|DROP COLLECT ... |
  SET|DROP FILTER WHERE ... |
  SET ATTACHED | SET DETACHED |
  SET BRANCHED BY <branch> | SET UNBRANCHED |
  ADD|DROP|ALTER MATERIALIZED STATE ... |
  ADD|DROP|REPLACE ROUTE ...
  [, ...];

ALTER EMITTER <name>
  ADD FROM <relay> [WHERE <expr>] |
  DROP FROM <relay> |
  ALTER FROM <relay> SET WHERE <expr> |
  ALTER FROM <relay> DROP WHERE |
  SET TO <full sink clause including MODE> |
  SET MODE <transport-specific publishing mode body> |
  SET CLIENT <client> |
  SET ENCODE USING <codec> | DROP ENCODE |
  SET COLLECT FOR <duration> [MAX BATCH SIZE <bytes>] | DROP COLLECT |
  SET ATTACHED | SET DETACHED |
  SET FLUSH EACH <duration> MAX BATCH SIZE <bytes> | SET FLUSH IMMEDIATE |
  SET COMMIT EACH <duration> MAX SIZE <bytes>
  [, ...];

ALTER INGESTOR <name>
  SET FROM <source> |
  SET DECODE USING <codec> |
  SET TIMESTAMP NOW | SET TIMESTAMP AT <field> | DROP TIMESTAMP |
  SET FILTER WHERE <expr> | DROP FILTER WHERE |
  ADD|DROP|REPLACE ROUTE ... |
  SET GENERAL ERROR IGNORE|LOG
  [, ...];

ALTER REINGESTOR <name>
  ADD|DROP|ALTER FROM ... |
  SET|DROP COLLECT ... |
  SET|DROP FILTER WHERE ... |
  SET ATTACHED | SET DETACHED |
  ADD|DROP|ALTER MATERIALIZED STATE ... |
  ADD|DROP|REPLACE ROUTE TO <relay> <construction> BRANCHED BY <branch>|UNBRANCHED ...
  [, ...];

ALTER GENERATOR <name>
  SET MATERIALIZED STATE <relay> |
  SET EACH <duration> |
  SET BRANCHED BY <branch> | SET UNBRANCHED |
  ADD|DROP|REPLACE ROUTE ...
  [, ...];

ALTER PLACEMENT <placement>
  SET POLICY REQUIRE COLOCATION|PREFER COLOCATION|NEUTRAL|SUGGEST SEPARATION |
  SET RANK <n> |
  DROP RANK |
  SET FROM <runtime_node> [, ...] TO <runtime_node> [, ...] |
  RENAME TO <name>
  [, ...];

ALTER SCHEMA <name>
  ADD FIELD <field> <type> [OPTIONAL] [SENSITIVE],
  DROP FIELD <field>,
  RENAME FIELD <field> TO <field>,
  ALTER FIELD <field> SET TYPE <type>,
  ALTER FIELD <field> SET|DROP OPTIONAL,
  ALTER FIELD <field> SET|DROP SENSITIVE;

ALTER WIRE JSON|CBOR|AVRO SCHEMA <name>
  MODE STRICT|LOOSE,
  ADD FIELD <field> <wire_type> [OPTIONAL],
  DROP FIELD <field>,
  RENAME FIELD <field> TO <field>,
  ALTER FIELD <field> SET TYPE <wire_type>,
  ALTER FIELD <field> SET|DROP OPTIONAL;
```

Operations in one `ALTER RELAY`, `ALTER JUNCTION`, `ALTER DEDUPLICATOR`, `ALTER REORDERER`,
`ALTER EMITTER`, `ALTER INGESTOR`, `ALTER REINGESTOR`, `ALTER GENERATOR`, or `ALTER PLACEMENT`
execute in written order. See
[Streams And State](relay.md#altering-relays) for relay operations,
[Processors](processors.md) for the full processor operation shapes, and
[Emitters](emitters.md#altering-emitters) and
[Ingestors](ingestors.md#altering-ingestors) for boundary-node operations. See
[Schemas And Codecs](schemas-and-codecs.md#altering-schemas) for ordered schema changes and atomic
migrations, and [Placement Policies](placement.md) for placement rank, path coverage, and
enforcement.

All `CREATE` statements may optionally insert `IF NOT EXISTS` immediately after `CREATE`.

When `IF NOT EXISTS` is present and the named entity already exists, the command succeeds as a no-op instead of failing. Command responses also mark that condition explicitly with `already_existed = true`.

The rest of the graph is built with:

- `CREATE DOMAIN`, `CREATE UNPACED DOMAIN`, `CREATE PACED DOMAIN`
- `CREATE USER`
- `CREATE CLIENT`
- `CREATE VHOST`
- `CREATE ENDPOINT`
- `CREATE INGESTOR`
- `CREATE GENERATOR`
- `CREATE INFERENCER`
- `CREATE JUNCTION`
- `CREATE DEDUPLICATOR`
- `CREATE REINGESTOR`
- `CREATE EMITTER`
- `CREATE PLACEMENT`
- `CREATE HASH MAP`

`CREATE DOMAIN <name>` is the short spelling for `CREATE UNPACED DOMAIN <name>`.

Domain creation may declare a placement default, and named placement rules may overlay paths in
the active domain. Omission means `NEUTRAL`, which preserves ordinary scheduler heuristics. See
[Placement Policies](placement.md) for the four policy levels and lifecycle commands.

Multiple NSPL statements in one request must be wrapped in an explicit
transaction. `BEGIN` creates a replicated transaction and returns its id,
`COMMIT` executes the queued statements, and `REVERT` drops them without
applying them. A transaction survives an unclean disconnect or leader failover;
the CLI and web console re-attach before sending another command. Sending
multiple statements without `BEGIN` is rejected.

```nspl,ignore
CREATE DOMAIN production;
USE production;

BEGIN;
CREATE SCHEMA notification (user_id I64);
COMMIT;
```

`BEGIN` binds the transaction to the selected domain, which must already exist,
and every queued statement must select that same domain. `BEGIN` inside an
active transaction is an error. `COMMIT` and `REVERT` also require an active
transaction. Queueable content is limited to that domain's model mutations,
domain configuration and lifecycle, and `CREATE RESOURCE`. `CREATE DOMAIN`,
`CREATE USER`, read-only statements, subscriptions, resource uploads, and node
administration are not valid inside a transaction and must be sent separately.
Use `SHOW TRANSACTIONS;` to inspect live transactions and retained outcomes. See
[Control Plane](control-plane.md#replicated-nspl-transactions) for attach,
failover, commit-step, expiry, and limit semantics.

Ingestors, relay-consuming processors, and generated-output processors use optional node-level
arrival filters and route-local construction. Relay-consuming processors may also attach a
source-level filter to `FROM`:

```nspl,ignore
FROM <relay> [WHERE <expr>], ...
[COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]]
[FILTER WHERE <expr>]
TO <relay>
  [INHERIT ...]
  [SET <field> = <expr>, ...]
  [WHERE <expr>]
  FLUSH ...
  ON MESSAGE ERROR ...
[TO <relay> ...]
```

`COLLECT FOR` is optional on graph nodes that consume relay input through `FROM`. It is unavailable
to ingestors, which read external sources rather than relays. When omitted, each incoming Arrow
batch proceeds directly to node execution. When present, Nervix maintains an input batch
independently for each source relay and concrete branch, then executes the node when the duration
expires or the optional size boundary is reached. Correlators configure the clause independently
after each `LEFT FROM` and `RIGHT FROM` relay list. Emitters place it after their complete
`FROM <relay> [WHERE ...] [, ...]` list. All emitter inputs declare the same payload schema, but
they may belong to differently named branches; collection remains independent for each source
relay and concrete branch. Generators do not read a `FROM` relay and therefore have no input
collection clause.

`FROM ... WHERE` runs first. `FILTER WHERE` runs next, before the node accepts rows into its state,
buffer, inferencer, or guest. Every route then creates a new empty output, performs its own ordered
construction, finalizes the declared schema, and evaluates its route `WHERE`. Required fields must
be initialized; omitted optional fields become typed nulls. There is no implicit identity
transformation and no global `SET` or `INHERIT`.

Input collection assembles relay batches before that execution sequence. Its time and size include
rows that a later source or node filter may reject. `COLLECT FOR` controls input delivery to the
node; route-local `FLUSH` controls output delivery from the node. The two policies are independent.

Transforming routes use one [working message](working-message.md) from input through finalization.
Transforming routes—ingestors after decoding, reingestors, junctions, deduplicators, reorderers,
and codec emitters—may use `INHERIT`. Generators, windows, inferencers, WASM processors,
correlators, and direct emitters are set-only and reject `INHERIT`. Generated inferencer and WASM
state is an immutable read source shared independently by every route; it is not an automatically
initialized output and it is not exposed as `input` or `message`.

This surface is available on:

- `CREATE INGESTOR`
- `CREATE INFERENCER`
- `CREATE JUNCTION`
- `CREATE DEDUPLICATOR`
- `CREATE REINGESTOR`
- `CREATE WASM PROCESSOR`
- `CREATE WINDOW PROCESSOR`
- `CREATE EMITTER`

Every `TO` destination on a flush-based node requires `FLUSH EACH <duration> MAX BATCH SIZE
<bytes>` or `FLUSH IMMEDIATE`; there are no hidden defaults. Window processors use `WIDTH` and
`STEP`, and WASM processors use guest-owned output cadence instead of `FLUSH`.

This is the authoritative `FLUSH IMMEDIATE` timing rule.
During normal processing, `FLUSH IMMEDIATE` starts a system-owned 100 µs minimum batching timeout
when data first enters an empty route buffer. The route flushes when that timeout expires, allowing
nearby arrivals to remain in one Arrow batch instead of collapsing to one batch per message.
`FLUSH IMMEDIATE` has no size boundary; shutdown and error handling may still force pending data
out.

Treat each required `FLUSH` clause as workload-specific operational tuning. `FLUSH IMMEDIATE`
minimizes time spent waiting beyond the system-owned batching window, while `FLUSH EACH <duration>
MAX BATCH SIZE <bytes>` emits when either configured boundary is reached. Choose both values for
the route's traffic, downstream behavior, and branch cardinality. Values in examples are
illustrative, not recommended defaults. Two measured facts decide what a `FLUSH` clause can buy.

`MAX BATCH SIZE` only clamps a batch; it never grows one. The batch a route emits is roughly its
arrival rate multiplied by the `FLUSH EACH` interval, cut off at the byte cap, so raising a cap
that never fires changes nothing and the only way to grow batches is a longer interval, paid in
latency. Upstream of every route, ingestors build their first Arrow batches from source groups of
at most 1,024 messages, or fewer after a 5 ms idle gap, independent of any `FLUSH` policy; larger
batches downstream come only from route buffering across an interval. Read actual batch sizes
from the [`nervix_messages_per_batch` histogram](metrics-and-observability.md) instead of
inferring them.

Larger batches do not make program execution faster beyond about a thousand rows. Execution is
columnar, so its fixed per-batch cost is amortized quickly. The table shows per-row throughput of
the VM batch-size sweep (`crates/nervix-vm/benches/vm.rs`, criterion, one development machine,
September 2026) relative to the 1,024-row value for each program shape: at 64 rows it is 12–60%
of that value and climbs steeply; above 1,024 rows most shapes are flat within measurement noise,
float arithmetic dips, and only the cheapest shape, nullable casts, keeps gaining, because batches
above 1,024 rows execute on the blocking worker pool and its per-batch hand-off is no longer
amortized by more work. Absolute rates are machine-specific; the shape is not.

| program shape | 64 | 256 | 1,024 | 4,096 | 16,384 | 65,536 |
|:--|--:|--:|--:|--:|--:|--:|
| integer arithmetic + filter | 21% | 53% | 100% | 98% | 97% | 100% |
| integer comparison | 19% | 52% | 100% | 96% | 98% | 108% |
| float arithmetic | 28% | 64% | 100% | 84% | 71% | 77% |
| nullable casts | 12% | 41% | 100% | 70% | 133% | 137% |
| string kernels | 24% | 61% | 100% | 82% | 105% | 106% |
| text transforms | 47% | 82% | 100% | 89% | 97% | 105% |
| list builtins | 60% | 87% | 100% | 82% | 93% | 99% |

Larger batches genuinely pay only at boundaries that do work per batch rather than per record:

- OTEL emitters send one export request per batch, so batch size sets request count and the
  compression ratio.
- Iceberg emitters write one staging file per flush and one commit per `COMMIT EACH ... MAX SIZE`
  boundary, so batch and commit sizes set file counts and Parquet file sizes.
- Inferencers whose tensor shapes declare a batch dimension run one model invocation per batch.
- WASM processors serialize one Arrow IPC payload per batch, and cluster interconnect sends one
  frame per batch, with a fixed 8 MiB frame limit.

Broker emitters — Kafka, Pulsar, NATS, RabbitMQ, MQTT, Redis, ZeroMQ, Sentry, and syslog — encode
and publish one record at a time whatever the batch size; wire batching there belongs to the
client, such as Kafka's `linger.ms` and `batch.size`. SQS `BATCH` groups at most ten records per
request, and database sinks split every flush into statements of at most `WITH MAX BATCH <n>`
records. On such routes a larger `MAX BATCH SIZE` or a longer interval buys only fewer flush
cycles, at the cost of latency, memory, and coarser failure and retry granularity: prefer the
shortest interval the sink tolerates and let the byte cap protect memory.

`MAX BATCH SIZE` measures the logical Arrow data in the current batch slice: value buffers plus
the offsets and validity data needed to represent those values. It does not count unused buffer
capacity or Rust and Arrow object overhead.

`SET` assignments execute left to right and repeated targets are valid. A later assignment may read
an earlier value through the bare field or `output.<field>`. `INHERIT ALL`, `INHERIT ALL EXCEPT
...`, and explicit `INHERIT field, ...` copy compatible same-named input fields. `UNSET` is not part
of NSPL.

Supported expression surface:

- literals: `i64`, `f64`, `bool`, `string`
- identifiers: field references from the current row
- arithmetic: `+`, `-`, `*`, `/`, `%`
- comparisons: `=`, `!=`, `>`, `<`, `>=`, `<=`
- boolean logic: `AND`, `OR`, `NOT`
- [conditionals](filter-map-functions.md#conditional-expressions): `IF condition THEN value ELSE value END`, searched
  `CASE WHEN condition THEN value ... [ELSE value] END`, and simple
  `CASE operand WHEN match THEN value ... [ELSE value] END`
- parentheses for nesting and precedence control
- explicit casts only: `expr AS TYPE`

Conditional result arms must have one exact type. Searched `CASE` conditions and the `IF` condition
must be `BOOL`; simple `CASE` match values must have the operand's exact type. Arms are tested in
written order and the first match wins. A null condition or null simple-`CASE` comparison does not
match. Omitting `ELSE` produces a typed null, so the destination must be optional. `IF` always
requires `ELSE`.

The [Conditional Expressions](filter-map-functions.md#conditional-expressions) reference owns the
reserved-word rule for conditional keywords.

Supported filter-map types match the full Nervix internal schema type set:

- integers: `U8`, `I8`, `U16`, `I16`, `U32`, `I32`, `U64`, `I64`
- floating point: `F32`, `F64`
- other scalars: `BOOL`, `STRING`, `DATETIME`

The parser accepts both long and short cast spellings where relevant, for example:

- `AS UINT8` or `AS U8`
- `AS INT32` or `AS I32`
- `AS FLOAT32` or `AS F32`
- `AS STRING`
- `AS BOOL`
- `AS DATETIME`

Supported built-ins include string, null-handling, numeric, regex, and contextual functions such as:

- string transforms: `lower`, `upper`, `trim`, `length`, `concat`
- null handling: `coalesce`, `is_null`, `nullif`
- numeric and predicates: `abs`, `contains`, `starts_with`, `ends_with`
- contextual functions: `now`, `uuid_v4`, `uuid_v7`

See [Filter-Map Functions](filter-map-functions.md) for the full current function list, signatures, and aliases.

User-defined calls always use `udf::<name>(...)`. The explicit namespace means adding a builtin can
never shadow a UDF or change existing user code. See
[Choosing An Extension Tier](filter-map-functions.md#choosing-an-extension-tier).

General expression rules:

- builtin calls may be nested or chained, for example `lower(trim(raw))`
- arithmetic and predicate expressions may also be nested with parentheses
- there is no implicit cast insertion; type mismatches must be resolved with explicit `AS ...`
- relay names are graph references, never expression qualifiers
- language scopes are `message`, `input`, `output`, `branch`, `left`, `right`,
  `relay_state.<relay>`, `metadata`, `partial_output`, and `error`; availability depends on context
- transforming construction reads the [working message](working-message.md); `output.field` reads
  only an already initialized output field
- generated routes allow bare reads from immutable generated state until the same-named output is
  initialized; `message` and `input` are unavailable
- `branch.field` must be explicit and is unavailable in successful emitter expressions
- supported ingestors read headers with `read_header(name)` and `read_headers(name)`; Kafka exposes
  typed `metadata.topic`, `metadata.partition`, and `metadata.offset`, while Syslog exposes
  optional `metadata.peer_addr`
- supported codec emitters stage ordered `write_header(name, value)` calls in `INVOKE`

Example:

```nspl
CREATE BRANCH by_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE INGESTOR notifications_in
  FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
  ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec
  FILTER WHERE input.active
  TO notifications
    INHERIT ALL EXCEPT raw
    SET amount = message.amount + 1,
        normalized = lower(input.raw)
    BRANCHED BY by_tenant SET tenant = message.tenant
    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Another example showing nested conditions and chained calls:

```nspl
CREATE EMITTER outbound
  FROM notifications
  TO KAFKA kafka_main TOPIC notifications_out
    MODE ACK PARALLEL MAX 1000 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL EXCEPT raw
  SET normalized = lower(trim(input.raw)), magnitude = abs(input.amount)
  WHERE (output.active AND output.amount > 5) OR contains(lower(trim(input.raw)), 'urgent')
  INVOKE write_header('tenant', input.tenant)
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

The leader parses and validates these structured expressions immediately when the statement is
applied. Models never store raw executable NSPL, and runtime execution never reparses expressions.

Generators use a narrower surface:

```nspl
CREATE GENERATOR synth_notifications
  USING MATERIALIZED STATE notifications
  EACH 100ms
  BRANCHED BY by_tenant
  TO generated_notifications
    SET user_id = relay_state.notifications.user_id,
        amount = relay_state.notifications.amount
    FLUSH EACH 1s MAX BATCH SIZE 1MiB
    ON MESSAGE ERROR LOG;
```

Generator-specific rules:

- only `SET` is allowed
- exactly one materialized relay is declared and is accessed as
  `relay_state.<relay>.<field>`
- every route sees the same immutable state snapshot for one tick
- `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` is mandatory and controls buffered emission
- paced domains evaluate both generator cadence and flush cadence against the domain clock, while unpaced domains use wall clock time

## Runtime Node Error Policies

Every `TO` route on an ingestor or relay-consuming processor must declare its message error policy after that route's construction clauses:

```nspl,ignore
ON MESSAGE ERROR IGNORE | LOG | SEND TO error_stream
SET error_reference = error.reference,
    error_code = error.code,
    source_id = input.id,
    attempted_total = partial_output.total
```

An ingestor additionally declares its node-level general policy after the source configuration:

```nspl,ignore
ON GENERAL ERROR IGNORE | LOG
```

Emitters attach `ON MESSAGE ERROR` to their single external route and retain `ON GENERAL ERROR` at
node level. WASM processors keep `ON GLOBAL ERROR` at node level for guest failures that are not
tied to a message.

`MESSAGE` errors carry a stable UUIDv7 reference, code, operation, optional operation index, sorted
affected field paths, timestamp, and a non-sensitive message. Error construction can read the
eligible original input, the exact materialized-state snapshot, an all-optional `partial_output`,
and the structured `error` scope. The error route preserves the branch in which the failure
occurred. On flush-based nodes, `SEND TO` records are buffered independently for the owning `TO`
route and concrete branch, then emitted when that route's `FLUSH` interval or maximum batch size
fires. They are not dispatched immediately. Error-record construction failures are logged and
no-acked without recursively invoking the same policy. General and global errors remain node-wide
and do not inherit a route-local `FLUSH` policy.

Client definitions are key-value based and may optionally mount a resource for file-backed settings such as TLS material:

```nspl,ignore
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <client_type>
  [MOUNT <resource>]
  CONFIG {
    '<key>' = '<value>'
  };
```

WebSocket clients and endpoints may also reference a signaling protocol:

```nspl,ignore
CREATE [IF NOT EXISTS] SIGNALING PROTOCOL <name>
  FORMAT JSON | YAML | TOML | XML | CBOR | RAW
       | PROTOBUF USING RESOURCE <resource> [VERSION <version>]
         CONFIG { '<key>' = '<value>' }
         SEND MESSAGE '<message_type>' WAIT MESSAGE '<message_type>'
  ON CONNECT
  ( SEND JAQ '<program>'[, '<program>'...]
  | WAIT JAQ '<matcher>'[, '<matcher>'...]
  | WAIT JAQ '<matcher>' [CAPTURE '<program>'] [ACCEPT DATA] )+
  [FAIL JAQ '<matcher>'[, '<matcher>'...]]
  TIMEOUT <duration>;

CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE WEBSOCKETS WITH SIGNALING PROTOCOL <name>
  CONFIG {
    'endpoint' = 'wss://example.com/ws'
  };
```

Each `SEND JAQ` program must produce exactly one value, which is serialized in the declared format.
Each `WAIT JAQ` matcher is satisfied by any output that is neither `null` nor `false`, so it can
assert the fields that matter and ignore the connection ids and timestamps real services add.
`FAIL JAQ` matchers abort the handshake immediately with the matched value as the reason.

Steps run strictly in written order: a step completes before the next starts, so a request can
depend on an earlier reply. A `WAIT` step completes when every matcher it lists is satisfied, in any
arrival order. `CAPTURE` records values from the matched frame, which every later program reads
through `$state`.

`ACCEPT DATA` says where payload starts flowing to the relay — on `ON CONNECT` for the first frame,
or on the `WAIT` step whose completion proves the peer is streaming. Frames arriving before that are
dropped rather than buffered. A `FAIL JAQ` guard on a step aborts during that step; the optional one
before `ON CONNECT` applies throughout.

Current built-in client transport kinds include:

- `KAFKA`
- `PULSAR`
- `HTTP`
- `SENTRY`
- `OTEL`
- `PROMETHEUS`
- `RABBITMQ`
- `REDIS`
- `MQTT`
- `NATS`
- `ZEROMQ`
- `SYSLOG`
- `SQS`
- `WEBSOCKETS`
- `S3`
- `GCS`
- `AZURE_BLOB`

Resource management commands:

```nspl,ignore
CREATE [IF NOT EXISTS] RESOURCE <name>;
UPLOAD RESOURCE <name> VERSION '<local_directory>';
DESCRIBE RESOURCE <name>;
DESCRIBE RESOURCE <name> VERSION <n>;
```

TLS-capable VHOSTs:

```nspl,ignore
CREATE [IF NOT EXISTS] VHOST <name> <hostname>, ...
  [WITH TLS <resource> [VERSION <n>]];
```

If `VERSION <n>` is omitted from `WITH TLS`, the VHOST resolves the latest uploaded version of that resource.

Session-only commands:

```nspl,ignore
CREATE SUBSCRIPTION <name> TO <relay> [BLOCKING|DROPPING] [BATCH SAMPLE RATE <rate>] [WHERE ...];
DELETE SUBSCRIPTION <name>;
DESCRIBE RELAY <relay> WHERE (...);
DESCRIBE INGESTOR <ingestor>;
DESCRIBE JUNCTION <junction>;
DESCRIBE DEDUPLICATOR <deduplicator>;
DESCRIBE REORDERER <reorderer>;
DESCRIBE WINDOW PROCESSOR <window_processor>;
DESCRIBE HASH MAP <hash_map>;
DESCRIBE PLACEMENT <placement>;
DESCRIBE RELOCATION <selection> ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> FOLLOW PREFERENCES | IGNORE PREFERENCES ...];
DESCRIBE DOMAIN;
LOOKUP <hash_map> KEY '<key>';
```

Show commands:

```nspl,ignore
SHOW CREATE <kind> <name>;
SHOW PLACEMENTS;
SHOW RELAY <name> MATERIALIZED STATE;
SHOW CLUSTER STATUS;
DROP NODE <node_id>;
CORDON NODE <node_id>;
UNCORDON NODE <node_id>;
DRAIN NODE <node_id>;
RELOCATE <selection> ONTO NODE <node_id>
  FOLLOW PREFERENCES | IGNORE PREFERENCES
  [FOR <kind> <name> FOLLOW PREFERENCES | IGNORE PREFERENCES ...];
```

A relocation `<selection>` is a kind-qualified list, `<kind> <name>[, ...]`, or a directed corridor,
`FROM <kind> <name>[, ...] TO <kind> <name>[, ...]`.

For a wire schema, `<kind>` is the exact multi-word kind: `WIRE JSON SCHEMA`, `WIRE CBOR SCHEMA`,
or `WIRE AVRO SCHEMA`. The same exact kind is required by `DROP`.

General notes:

- keywords are case-insensitive
- autocomplete is derived from the parser surface
- transport/client configs are generally preserved as pass-through string key/value pairs
- native schema fields may use the `SENSITIVE` modifier; session subscription output masks those values as `<masked>`, while emitters may send sensitive values to their configured external sink
- `CREATE SUBSCRIPTION` and `DELETE SUBSCRIPTION` are not persisted in the registry
- session subscription names are unique within a connected session; one session may subscribe to relays from multiple domains, and `DELETE SUBSCRIPTION` uses the name rather than repeating subscription parameters
- every `RELAY` is scheduled with one owner; only that owner instantiates its buffer, concrete
  branch presence, fan-out, subscriptions, and metrics
- relay `CAPACITY` bounds the single owner buffer cluster-wide; each producer node and remote
  consumer node adds one fixed in-flight dispatch slot, independent of branch count
- materialized state adds scheduler-selected state replicas to the relay rather than a separate
  runtime-node kind; ordinary relays report no replicas
- ingestors and reingestors construct branch identities, and a relay owner applies branch TTL and
  `MAX INSTANCES` across all producers for that relay
- `DESCRIBE INGESTOR` exposes runtime-facing ingestor state, including memory-backpressure state and committed Kafka `OFFSET BY DOMAIN` partition assignment
