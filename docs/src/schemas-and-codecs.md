# Schemas And Codecs

Nervix separates internal runtime schema from wire schema.

## Internal Schemas

An internal schema describes the typed runtime record:

```nspl
CREATE IF NOT EXISTS SCHEMA notification (
  user_id U32,
  created_at DATETIME,
  payload STRING OPTIONAL,
  cpu_last_64 ARRAY<F32, 64>,
  image ARRAY<F32, 3, 224, 224>,
  detections VEC<ARRAY<F32, 6>> OPTIONAL
);
```

Schemas must declare at least one field.

Field names used in expressions are subject to the
[conditional reserved-word rule](filter-map-functions.md#conditional-expressions).

These types are the values Nervix stores in runtime records and uses for subscription matching and processor logic. `BYTES` may be a deduplication or reordering key, ordered lexicographically by octet, but branch key schemas cannot contain `BYTES`, including nested `ARRAY` or `VEC` elements.

`BYTES` holds arbitrary octets in an Arrow `Binary` column. It is distinct from `STRING`. Use
`bytes_from_utf8` and `bytes_to_utf8` for explicit text conversion, or `hex_decode` and
`base64_decode` for encoded text. A `CAST` never converts between `BYTES` and text.
Lookup schemas, materialized-state dependencies, generator source relays, and window aggregate
arguments cannot contain `BYTES`; their declarations fail validation. `LOOKUP_HASH_MAP` key
expressions also cannot produce `BYTES`.

`ARRAY<T, D1, ..., Dn>` is a fixed rectangular array. Each dimension maps to one
nested Arrow `FixedSizeList` level, so `ARRAY<F32, 2, 3>` maps to
`FixedSizeList<FixedSizeList<Float32, 3>, 2>` and remains a 2-by-3 value.
`VEC<T>` is a variable-length sequence and maps to Arrow `List<T>`. The element
type is recursive, so fixed and variable axes can be mixed, for example
`VEC<ARRAY<F32, 6>>` and `ARRAY<VEC<STRING>, 4>`.

`ARRAY` and `VEC` are distinct and are never implicitly converted. Every fixed
axis must have a positive length no greater than 2147483647, the largest length
an Arrow `FixedSizeList` carries, fixed arrays must contain exactly that many
elements at runtime, and dense multidimensional values must retain their nested
shape. JSON and CBOR represent both with nested JSON-style arrays. AVRO uses
nested array schemas with item types inferred recursively from the internal
schema.

Append `OPTIONAL` to either an internal schema field or a wire schema field when the value may be absent. Optional fields are omitted from runtime records and emitted JSON payloads when no value is present.

## Wire Schemas

Wire schemas describe the serialized format on the transport side.

Declared wire schemas are either `STRICT` or `LOOSE`. Strict wire schemas reject payload fields
that are not declared by the wire schema. Loose wire schemas accept extra payload fields and drop
them before decoding into the internal schema.

JSON wire schema:

```nspl
CREATE IF NOT EXISTS WIRE JSON SCHEMA notification_wire MODE STRICT (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);
```

CBOR wire schema:

```nspl
CREATE IF NOT EXISTS WIRE CBOR SCHEMA notification_wire MODE LOOSE (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);
```

AVRO wire schema:

```nspl
CREATE IF NOT EXISTS WIRE AVRO SCHEMA notification_wire MODE STRICT (
  user_id LONG,
  created_at STRING,
  payload STRING OPTIONAL
);
```

JSON, CBOR, and AVRO wire schemas must declare at least one field.

SYSLOG is different: it is a predefined singleton wire schema whose shape comes from the syslog
protocol. It has no user-defined name, `CREATE WIRE` declaration, mode, field list, `ALTER`, or
`DROP` lifecycle. A codec references it directly:

```nspl
CREATE CODEC syslog_codec FROM SYSLOG TO SCHEMA syslog_event;
```

Its fixed field contract is documented in [Syslog](syslog.md).

## Altering Schemas

Internal and declared wire schemas can be changed without dropping their whole dependent graph
first. Operations in one statement run from left to right, and each operation sees the result of
the previous one.

Internal schema operations are:

- `ADD FIELD <field> <type> [OPTIONAL] [SENSITIVE]`
- `DROP FIELD <field>`
- `RENAME FIELD <field> TO <field>`
- `ALTER FIELD <field> SET TYPE <type>`
- `ALTER FIELD <field> SET OPTIONAL` and `ALTER FIELD <field> DROP OPTIONAL`
- `ALTER FIELD <field> SET SENSITIVE` and `ALTER FIELD <field> DROP SENSITIVE`

For example:

```nspl
ALTER SCHEMA notification
  ADD FIELD note STRING OPTIONAL,
  RENAME FIELD created_at TO received_at,
  ALTER FIELD payload SET SENSITIVE;
```

`DROP SENSITIVE` is the explicit way to downgrade a field. Nervix still rebuilds and validates the
whole candidate graph, including every downstream leakage rule. A schema must retain at least one
field, added and renamed names must be unique, and the target of every drop, rename, or field alter
must exist.

Wire schema operations are:

- `ADD FIELD <field> <wire_type> [OPTIONAL]`
- `DROP FIELD <field>`
- `RENAME FIELD <field> TO <field>`
- `ALTER FIELD <field> SET TYPE <wire_type>`
- `ALTER FIELD <field> SET OPTIONAL` and `ALTER FIELD <field> DROP OPTIONAL`

JSON, CBOR, and AVRO wire schemas are separate entity kinds. They may use the same name without
colliding, and the exact format is required for every ALTER:

```nspl
ALTER WIRE JSON SCHEMA notification_wire
  ADD FIELD note string OPTIONAL;
```

Mode changes use that same exact entity kind:

```nspl
ALTER WIRE JSON SCHEMA notification_wire MODE LOOSE;
```

Use one explicit transaction when a type or shape change requires coordinated updates. Model
mutations for one domain—`CREATE`, schema `ALTER`, relay `ALTER`, and `DROP`—are validated against
one candidate graph and committed atomically. The following replacement changes the wire and
internal types together and recreates their codec without exposing an intermediate invalid graph:

```nspl
BEGIN;
ALTER WIRE JSON SCHEMA notification_wire
  ALTER FIELD user_id SET TYPE number;
ALTER SCHEMA notification
  ALTER FIELD user_id SET TYPE F64;
DROP CODEC notification_codec;
CREATE CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification;
COMMIT;
```

If any operation or dependent model fails validation, none of the mutations are persisted.
`SHOW CREATE SCHEMA` and exact wire forms such as
`SHOW CREATE WIRE JSON SCHEMA notification_wire` render the resulting canonical definitions.
Dropping a wire schema is exact-format too, for example
`DROP WIRE JSON SCHEMA notification_wire`.

On a running domain, a schema ALTER is applied through an automatic quiesce cycle: Nervix validates
first, stops new ingestion and generators, force-flushes buffered output, drains in-flight work,
installs the new graph, and resumes. This internal state is not a user-facing lifecycle command.
A drain timeout rejects the batch and resumes the old graph. On a stopped domain, Nervix validates
and persists the change without a quiesce cycle.

Runtime state whose record layout derives from an altered schema is recreated. Independent
stateful nodes retain their state, and so does state that depends on no schema at all: domain-owned
Kafka offsets and the counters behind a node's metrics survive a schema change of their own node.
Persisted schema-bound state is keyed by the fingerprint of the schemas it was written under, so a
stale layout is never restored as the new type. Relay subscriptions closed by the rebuild report
that they must be recreated against the current schema.

## Codecs

A codec maps one transport payload format to one internal schema.

```nspl
CREATE IF NOT EXISTS CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification;
```

Schemaful codecs are type-strict. A JSON `string` wire field does not implicitly decode
into an internal `DATETIME` field. Declare the wire conversion explicitly:

```nspl
CREATE IF NOT EXISTS CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification
  ENCODE created_at AS RFC3339;
```

`created_at` is the internal schema field name. The matching wire field must be a
string, and the internal field must be `DATETIME`.

For an internal `BYTES` field, declare `BYTES` in the JSON or CBOR wire schema. Its wire value is
a canonical RFC 4648 standard base64 string with padding; malformed or unpadded text is a decode
error. AVRO `BYTES` fields use Avro's native byte sequence. A wire `STRING` field cannot bind an
internal `BYTES` field. Nested `ARRAY` and `VEC` byte elements use the same representation.

JAQ-native codecs parse a transport payload in a jaq-supported format and run explicitly directed
JAQ transformations. An ingestion transformation turns every value the payload holds into zero
or more JSON objects, and each object is decoded into the internal schema as one message
([Unfolding Payloads](#unfolding-payloads)):

```nspl
CREATE IF NOT EXISTS CODEC notification_cbor
  FROM CBOR
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON INGESTION '.';

CREATE IF NOT EXISTS CODEC notification_xml
  FROM XML
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON INGESTION '{user_id: (.c[] | select(.t == "user_id").c[0] | tonumber)}';
```

Protobuf codecs compile `.proto` files from an uploaded resource, decode or encode the selected message with `prost-reflect`, and use JAQ to translate between the protobuf JSON view and the internal schema:

```nspl
CREATE IF NOT EXISTS CODEC notification_proto
  FROM PROTOBUF
  USING RESOURCE proto_bundle VERSION 1
  CONFIG {'file' = 'notification.proto', 'include' = '.'}
  MESSAGE 'nervix.test.Notification'
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id: .user_id, payload: .payload}';
```

The resource contains the `.proto` files. `USING RESOURCE` requires `VERSION <n>` or
`VERSION LATEST`, and the codec compiles its descriptors from the one version it stores.
`CONFIG` declares compile parameters; `file`/`files` select source files and `include`/`includes` select import roots, all relative to the resource root. If no file is listed, all `.proto` files in the resource are compiled.

A protobuf codec that a [batching emitter](emitters.md#batching) encodes through names the message
a batch is published as, after `MESSAGE`:

```nspl,ignore
CREATE CODEC notification_proto
  FROM PROTOBUF
  USING RESOURCE proto_bundle VERSION 1
  CONFIG {'file' = 'notification.proto', 'include' = '.'}
  MESSAGE 'nervix.test.Notification'
  BATCH MESSAGE 'nervix.test.NotificationBatch'
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON EMITTING '{user_id: .user_id, action: .action}';
```

`BATCH MESSAGE` names a message in the same compiled descriptors, and the domain build fails when it
does not exist. Without an `ON EMITTING BATCH` transformation it must declare exactly one field,
`repeated <MESSAGE>`, which holds the members. With one, it may have any shape the
transformation's output is a valid instance of.

Current schemaful codec wire formats are:

- `JSON`, with an explicit JSON wire schema
- `CBOR`, with an explicit CBOR wire schema
- `AVRO`, with an explicit AVRO wire schema

Current predefined fixed-contract wire format:

- [`SYSLOG`](syslog.md), a singleton wire schema referenced directly by a codec

Current JAQ-native codec formats are:

- `JSON`
- `YAML`
- `TOML`
- `XML`
- `CBOR`

Current protobuf codec format:

- `PROTOBUF`, with resource-backed `.proto` files, inline compile config, and message name

## JAQ Transformations

JAQ-backed codecs must declare a JAQ transform. The concise
[JAQ Reference](jaq-reference.md) links the full upstream manual for the exact `jaq-core` release
Nervix embeds and summarizes the Nervix-specific boundary:

```nspl
CREATE IF NOT EXISTS CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON INGESTION '.payload'
    ON EMITTING '{payload: .}';
```

Semantics:

- no-wire codecs must use `FROM JSON|YAML|TOML|XML|CBOR ... WITH JAQ ...`
- protobuf codecs must use
  `FROM PROTOBUF USING RESOURCE ... VERSION <n> | LATEST CONFIG {...} MESSAGE ... [BATCH MESSAGE ...] WITH JAQ ...`
- codecs using declared wire schemas must use `FROM WIRE JSON|CBOR|AVRO SCHEMA ...` and do not
  carry JAQ transforms
- codecs using the predefined SYSLOG wire schema must use `FROM SYSLOG TO SCHEMA ...` and do not
  carry JAQ transformations or field encoding rules
- `WITH JAQ TRANSFORMATIONS` requires `ON INGESTION`, `ON EMITTING`, or both in that order, and
  `ON EMITTING` may be followed by `ON EMITTING BATCH` ([Batch Transformations](#batch-transformations))
- `ON INGESTION` runs on every value the parsed native or protobuf payload holds and may yield zero or more JSON objects, each of which becomes one message compatible with the internal schema ([Unfolding Payloads](#unfolding-payloads))
- `ON EMITTING` runs after the runtime record has been converted into JSON and must yield exactly one native-format or protobuf-message value

JAQ-backed encode/decode is dispatched to blocking workers so expensive transforms do not stall async ingestor or emitter tasks.

### Batch Transformations

A JAQ-backed codec — JAQ-native or protobuf — may declare a third transformation for emitters that
declare a [batching clause](emitters.md#batching):

```nspl
CREATE IF NOT EXISTS CODEC notification_envelope
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON EMITTING '{id: .user_id, action: .action}'
    ON EMITTING BATCH '{schema_version: 2, count: length, records: .}';
```

`ON EMITTING BATCH` is written only after `ON EMITTING`, because its input is built from that
transformation's outputs: one array holding the member values of a batch, in publication order. It
must yield exactly one value, valid in the codec's format; for a protobuf codec, a valid instance of
the declared `BATCH MESSAGE`. The program compiles with the codec, so a program that cannot compile
fails the domain build. A batching Sentry emitter requires its codec to declare this
transformation. `ON EMITTING` keeps its meaning exactly: it runs per record and yields one value per
record.

### Unfolding Payloads

A JAQ-backed codec decodes a payload as a stream. The payload is parsed into the values its format
holds, `ON INGESTION` runs on each value in payload order, and every object the program yields
becomes one message, in the order the program yields it:

| Format | Values in one payload |
| --- | --- |
| `JSON` | Whitespace-separated JSON values: one document, newline-delimited JSON, or concatenated JSON |
| `CBOR` | Consecutive CBOR data items |
| `YAML` | The documents of a YAML stream |
| `XML` | The root element; a declaration, document type, comment, or processing instruction outside it is not a value |
| `TOML` | Exactly one document |
| `PROTOBUF` | Exactly one message of the declared type |

An empty JSON, CBOR, YAML, or XML payload holds no values and decodes into no messages. jq stream
semantics apply unchanged: `.[]` unfolds an array into one message per element, a comma yields
several messages, and `select` or `empty` yields no message for the values it rejects.

```nspl
CREATE IF NOT EXISTS CODEC order_lines_codec
  FROM JSON
  TO SCHEMA order_line
  WITH JAQ TRANSFORMATIONS ON INGESTION '.lines[] | select(.quantity > 0)';
```

Every output must be a JSON object that fits the internal schema: surplus keys are dropped, an
absent or `null` value is accepted only for an `OPTIONAL` field, and every value must match its
field's type exactly. Messages carry no position of their own; a program that needs one computes
it, for example with `range(length) as $i | .[$i] + {index: $i}`.

A payload is decoded as a whole or rejected as a whole. A malformed value, a program evaluation
error, an output that is not an object or does not fit the schema, or more than 65,536 messages
rejects the entire payload, including the messages produced before the failure. The rejection is a
decode failure of the payload, not a route message error: it is reported as a runtime error event
and logged, `NO_ACK` modes continue with the next payload, and acknowledged modes retry or
redeliver it as they do any payload that fails to decode. The diagnostic names the codec, the
cause, and the zero-based position of the input value, plus the output's position when an output
is at fault. It never quotes payload values, so a mismatched field is described by its JSON kind
and a program evaluation error omits the evaluator's message, which is logged at `trace` level
instead.

The messages of one payload join one [source ingest group](ingestors.md) together, so they share its
domain execution snapshot, and every one of them sees the payload's source metadata and headers.
Each message is filtered, constructed, branched, routed, and error-handled on its own, and ingestor
metrics count messages. Source acknowledgement stays per payload: a payload is acknowledged once
every message it unfolded into has been acknowledged, a negative acknowledgement of any of them
negatively acknowledges the whole payload so that its already acknowledged messages are delivered
again with it, and a payload that unfolds into no messages is acknowledged as soon as it decodes.
`ACK PARALLEL MAX <n>` admits `n` payloads however many messages each unfolds into.

## Why The Split Matters

The schema split lets Nervix:

- keep runtime typing independent from transport shape
- support multiple wire formats
- normalize awkward inbound JSON without changing the internal data model
- reshape outbound payloads during emission without changing the internal record layout
