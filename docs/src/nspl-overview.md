# NSPL Overview

NSPL is the language used to define the Nervix graph.

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

Ingestors, relay-consuming processors, and generated-output processors use optional node-level arrival filters and per-output route clauses. Relay-consuming processors may also attach a source-level filter to `FROM`. Emitters use the same row-level filter-map surface on their sink boundary:

```nspl
FROM <relay> [WHERE <expr>], ...
[FILTER WHERE <expr>]
TO <relay> [SET <relay>.<field> = <expr>, ...] [UNSET <input>.<field>, ...] [WHERE <expr>]
[TO <relay> ...]
```

On relay-consuming processors, `FROM ... WHERE` is a source-level input filter and runs first. Most processors may declare multiple comma-separated `FROM` relays, and those relays must share the same schema. Correlators use repeated `LEFT FROM` and `RIGHT FROM` clauses instead; relays within each side must share that side's schema. `FILTER WHERE` runs after source filtering, before the node accepts rows into its buffer, state, or guest execution. `SET` and `UNSET` appear after `TO` because destination schema validation depends on the target relay. Each `TO` route may declare its own optional `WHERE` condition; routes without `WHERE` receive every row produced by the node.

Passthrough inheritance applies to processors that naturally map one input row to one output row, including inferencers. Inferencer routes inherit the inbound record and expose model values through `inner_input` and `inner_output`. Generated-output processors such as windows and WASM processors instead operate on the aggregate record or WASM guest output record.

WASM output routes are generated-output routes with one additional input binding: `SET` may read guest output fields through the destination relay namespace and original source fields through `input.<field>`. WASM output routes support `SET` and `WHERE`; they do not support `UNSET`.

This surface is available on:

- `CREATE INGESTOR`
- `CREATE INFERENCER`
- `CREATE JUNCTION`
- `CREATE DEDUPLICATOR`
- `CREATE REINGESTOR`
- `CREATE WASM PROCESSOR`
- `CREATE WINDOW PROCESSOR`
- `CREATE EMITTER`

The clause acts as a row-level filter-map program:

Every `TO` destination on a flush-based multi-output node requires an explicit `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` clause. Destination buffers and deadlines are independent. Generators and emitters retain their single node-level flush policy; window processors use `WIDTH` and `STEP` instead.

- `SET` overwrites existing fields or appends new fields
- `UNSET` removes fields from the downstream row shape when passthrough inheritance is active
- `WHERE` keeps only rows where the predicate is true

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

General filter-map rules:

- builtin calls may be nested or chained, for example `lower(trim(raw))`
- arithmetic and predicate expressions may also be nested with parentheses
- there is no implicit cast insertion; type mismatches must be resolved with explicit `AS ...`
- ingestor filter-map programs read decoded payload fields as `message.<field>` and, for supported sources, read transport headers with `read_header(name)` or `read_headers(name)`; see [Ingestors](ingestors.md#header-context)
- emitter filter-map programs write encoded payload fields through `message.<field>` and may append string headers with top-level `write_header(name, value)` calls in a final `INVOKE` block for Kafka, Pulsar, RabbitMQ, NATS, and SQS sinks; see [Emitters](emitters.md#filter-map-programs)
- branch-local processors, reingestors, and emitters can read the current branch key as `branch.<key>` when the current relay is branched; `branch` is a reserved namespace and cannot be used as a relay name

Example:

```nspl
CREATE BRANCH by_tenant
  SCHEMA tenant_branch TTL 5m;

CREATE INGESTOR notifications_in
  FILTER WHERE message.active
  TO notifications FLUSH EACH 100ms MAX BATCH SIZE 1MiB
    SET notifications.amount = message.amount + 1, notifications.normalized = lower(message.raw)
    UNSET notifications.raw ON MESSAGE ERROR LOG
  DECODE USING notification_codec
  BRANCHED BY by_tenant VALUES { tenant = notifications.tenant }
  FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON GENERAL ERROR LOG;
```

Another example showing nested conditions and chained calls:

```nspl
CREATE EMITTER outbound
  FROM notifications
  ENCODE USING notification_codec
  TO KAFKA kafka_main TOPIC notifications_out
  SET notifications.normalized = lower(trim(notifications.raw)), notifications.magnitude = abs(notifications.amount)
  UNSET notifications.raw
  WHERE (notifications.active AND notifications.amount > 5) OR contains(lower(trim(notifications.raw)), 'urgent')
  ON MESSAGE ERROR LOG ON GENERAL ERROR LOG
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB;
```

The leader parses and validates these programs immediately when the statement is applied, including output-schema checks after `SET` and `UNSET`.

Generators use a narrower surface:

```nspl
CREATE GENERATOR synth_notifications
  TO generated_notifications
  EACH 100ms
  FLUSH EACH 1s MAX BATCH SIZE 1MiB
  SET generated_notifications.user_id = notifications.user_id,
      generated_notifications.amount = notifications.amount ON MESSAGE ERROR LOG;
```

Generator-specific rules:

- only `SET` is allowed
- the destination relay is explicit with `TO <relay>`
- generator expressions may read only from relays that declare materialized state
- `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` or `FLUSH IMMEDIATE` is mandatory and controls buffered emission
- paced domains evaluate both generator cadence and flush cadence against the domain clock, while unpaced domains use wall clock time

## Runtime Node Error Policies

Every `TO` route on an ingestor or relay-consuming processor must declare its message error policy after that route's construction clauses:

```nspl
ON MESSAGE ERROR IGNORE | LOG | SEND TO error_stream SET error_message = message_error.message
```

An ingestor additionally declares its node-level general policy after the source configuration:

```nspl
ON GENERAL ERROR IGNORE | LOG
```

Emitters retain their message and general policies at node level because they have one external sink rather than relay `TO` routes. WASM processors keep `ON GLOBAL ERROR` at node level for guest failures that are not tied to a message.

`MESSAGE` errors are tied to one concrete message and one output construction, such as decode, transform, or route publication failures for that message. `GENERAL` and `GLOBAL` errors are not tied to a concrete message or `TO` route. `SEND TO` is therefore only valid for `ON MESSAGE ERROR`. Pure processors do not expose `ON GENERAL ERROR` because they do not own external transport/client failures.

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
CREATE SUBSCRIPTION <name> TO <relay> [BLOCKING|DROPPING] [BATCH SAMPLE RATE <rate>] [SET ...] [UNSET ...] [WHERE ...];
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
