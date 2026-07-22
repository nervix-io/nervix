# NSPL Overview

NSPL is the language used to define the Nervix graph.

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

```nspl
CREATE [IF NOT EXISTS] USER <name> WITH PASSWORD '<password>';

CREATE [IF NOT EXISTS] SCHEMA <name> (<field> <type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] STRICT|LOOSE WIRE JSON SCHEMA <name> (<field> <json_type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] STRICT|LOOSE WIRE CBOR SCHEMA <name> (<field> <cbor_type> [OPTIONAL], ...);
CREATE [IF NOT EXISTS] STRICT|LOOSE WIRE AVRO SCHEMA <name> (<field> <avro_type> [OPTIONAL], ...);

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
  WITH JAQ TRANSFORMATION '<program>';

CREATE [IF NOT EXISTS] CODEC <name>
  FROM PROTOBUF
  USING RESOURCE <resource> [VERSION <n>]
  CONFIG {'file' = '<path.proto>', 'include' = '.'}
  MESSAGE '<package.Message>'
  TO SCHEMA <schema>
  WITH JAQ TRANSFORMATION '<program>';

CREATE [IF NOT EXISTS] RELAY <name> SCHEMA <schema> [CAPACITY <n>]
  [WITH MATERIALIZED STATE LAST BY TIMESTAMP];
```

Core alter statements:

```nspl
ALTER RELAY <name> SET CAPACITY <n>;
```

See [Streams And State](relay.md#capacity) for live relay capacity resize
behavior.

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
- `CREATE HASH MAP`

`CREATE DOMAIN <name>` is the short spelling for `CREATE UNPACED DOMAIN <name>`.

Multiple NSPL statements in one request must be wrapped in an explicit
transaction. `BEGIN` starts a session-local transaction, `COMMIT` executes the
queued statements, and `REVERT` drops the queued statements without applying
them. Sending multiple statements without `BEGIN` is rejected.

```nspl
BEGIN;
CREATE DOMAIN production;
CREATE SCHEMA notification (user_id I64);
COMMIT;
```

`BEGIN` inside an active transaction is an error. `COMMIT` and `REVERT` also
require an active transaction. Client-local commands such as `USE` are not
valid inside a transaction and must be sent separately.

Ingestors, relay-consuming processors, and generated-output processors use optional node-level
arrival filters and route-local construction. Relay-consuming processors may also attach a
source-level filter to `FROM`:

```nspl
FROM <relay> [WHERE <expr>], ...
[FILTER WHERE <expr>]
TO <relay>
  [INHERIT ...]
  [SET <field> = <expr>, ...]
  [WHERE <expr>]
  FLUSH ...
  ON MESSAGE ERROR ...
[TO <relay> ...]
```

`FROM ... WHERE` runs first. `FILTER WHERE` runs next, before the node accepts rows into its state,
buffer, inferencer, or guest. Every route then creates a new empty output, performs its own ordered
construction, finalizes the declared schema, and evaluates its route `WHERE`. Required fields must
be initialized; omitted optional fields become typed nulls. There is no implicit identity
transformation and no global `SET` or `INHERIT`.

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
- parentheses for nesting and precedence control
- explicit casts only: `expr AS TYPE`

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

General expression rules:

- builtin calls may be nested or chained, for example `lower(trim(raw))`
- arithmetic and predicate expressions may also be nested with parentheses
- there is no implicit cast insertion; type mismatches must be resolved with explicit `AS ...`
- relay names are graph references, never expression qualifiers
- language scopes are `message`, `input`, `output`, `branch`, `left`, `right`,
  `relay_state.<relay>`, `metadata`, `partial_output`, and `error`; availability depends on context
- transforming construction uses `message.field` as the working output with exact-compatible input
  fallback; `output.field` reads only an already initialized output field
- generated routes allow bare reads from immutable generated state until the same-named output is
  initialized; `message` and `input` are unavailable
- `branch.field` must be explicit and is unavailable in successful emitter expressions
- supported ingestors read headers with `read_header(name)` and `read_headers(name)`; Kafka also
  exposes typed `metadata.topic`, `metadata.partition`, and `metadata.offset`
- supported codec emitters stage ordered `write_header(name, value)` calls in `INVOKE`

Example:

```nspl
CREATE BRANCH by_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE INGESTOR notifications_in
  FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL
  DECODE USING notification_codec
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
  ENCODE USING notification_codec
  TO KAFKA kafka_main TOPIC notifications_out
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

```nspl
ON MESSAGE ERROR IGNORE | LOG | SEND TO error_stream
SET error_reference = error.reference,
    error_code = error.code,
    source_id = input.id,
    attempted_total = partial_output.total
```

An ingestor additionally declares its node-level general policy after the source configuration:

```nspl
ON GENERAL ERROR IGNORE | LOG
```

Emitters attach `ON MESSAGE ERROR` to their single external route and retain `ON GENERAL ERROR` at
node level. WASM processors keep `ON GLOBAL ERROR` at node level for guest failures that are not
tied to a message.

`MESSAGE` errors carry a stable UUIDv7 reference, code, operation, optional operation index, sorted
affected field paths, timestamp, and a non-sensitive message. Error construction can read the
eligible original input, the exact materialized-state snapshot, an all-optional `partial_output`,
and the structured `error` scope. The error route preserves the branch in which the failure
occurred. Error-record construction failures are logged and no-acked without recursively invoking
the same policy.

Client definitions are key-value based and may optionally mount a resource for file-backed settings such as TLS material:

```nspl
CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE <client_type>
  [MOUNT <resource>]
  CONFIG {
    '<key>' = '<value>'
  };
```

WebSocket clients and endpoints may also reference a signaling protocol:

```nspl
CREATE [IF NOT EXISTS] SIGNALING PROTOCOL <name>
  ON CONNECT
  SEND BODY '<text_body>'[, '<text_body>'...]
  WAIT BODY '<text_body>'[, '<text_body>'...] TIMEOUT <duration>;

CREATE [IF NOT EXISTS] CLIENT <name>
  TYPE WEBSOCKETS WITH SIGNALING PROTOCOL <name>
  CONFIG {
    'endpoint' = 'wss://example.com/ws'
  };
```

Current built-in client transport kinds include:

- `KAFKA`
- `PULSAR`
- `HTTP`
- `PROMETHEUS`
- `RABBITMQ`
- `REDIS`
- `MQTT`
- `NATS`
- `ZEROMQ`
- `SQS`
- `WEBSOCKETS`
- `KINESIS`
- `S3`
- `GCS`
- `AZURE_BLOB`

Resource management commands:

```nspl
CREATE [IF NOT EXISTS] RESOURCE <name>;
UPLOAD RESOURCE <name> VERSION '<local_directory>';
DESCRIBE RESOURCE <name>;
DESCRIBE RESOURCE <name> VERSION <n>;
```

TLS-capable VHOSTs:

```nspl
CREATE [IF NOT EXISTS] VHOST <name> <hostname>, ...
  [WITH TLS <resource> [VERSION <n>]];
```

If `VERSION <n>` is omitted from `WITH TLS`, the VHOST resolves the latest uploaded version of that resource.

Session-only commands:

```nspl
CREATE SUBSCRIPTION <name> TO <relay> [BLOCKING|DROPPING] [BATCH SAMPLE RATE <rate>] [WHERE ...];
DELETE SUBSCRIPTION <name>;
DESCRIBE RELAY <relay> WHERE (...);
DESCRIBE INGESTOR <ingestor>;
DESCRIBE DEDUPLICATOR <deduplicator>;
DESCRIBE REORDERER <reorderer>;
DESCRIBE WINDOW PROCESSOR <window_processor>;
DESCRIBE HASH MAP <hash_map>;
LOOKUP <hash_map> KEY '<key>';
```

Show commands:

```nspl
SHOW CREATE <kind> <name>;
SHOW RELAY <name> MATERIALIZED STATE;
SHOW CLUSTER STATUS;
DROP NODE <node_id>;
CORDON NODE <node_id>;
UNCORDON NODE <node_id>;
DRAIN NODE <node_id>;
```

General notes:

- keywords are case-insensitive
- autocomplete is derived from the parser surface
- transport/client configs are generally preserved as pass-through string key/value pairs
- native schema fields may use the `SENSITIVE` modifier; session subscription output masks those values as `<masked>`, while emitters may send sensitive values to their configured external sink
- `CREATE SUBSCRIPTION` and `DELETE SUBSCRIPTION` are not persisted in the registry
- session subscription names are unique within a connected session; one session may subscribe to relays from multiple domains, and `DELETE SUBSCRIPTION` uses the name rather than repeating subscription parameters
- `RELAY` names a connection between runtime nodes; ingestors and reingestors create branch instances with runtime relay instances inside them
- `DESCRIBE INGESTOR` exposes runtime-facing ingestor state, including memory-backpressure state and committed Kafka `OFFSET BY DOMAIN` partition assignment
