# Schemas And Codecs

Nervix separates internal runtime schema from wire schema.

## Internal Schemas

An internal schema describes the typed runtime record:

```nspl
CREATE [IF NOT EXISTS] SCHEMA notification (
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

These types are the values Nervix stores in runtime records and uses for branch grouping, subscription matching, and processor logic.

`ARRAY<T, D1, ..., Dn>` is a fixed rectangular array. Each dimension maps to one
nested Arrow `FixedSizeList` level, so `ARRAY<F32, 2, 3>` maps to
`FixedSizeList<FixedSizeList<Float32, 3>, 2>` and remains a 2-by-3 value.
`VEC<T>` is a variable-length sequence and maps to Arrow `List<T>`. The element
type is recursive, so fixed and variable axes can be mixed, for example
`VEC<ARRAY<F32, 6>>` and `ARRAY<VEC<STRING>, 4>`.

`ARRAY` and `VEC` are distinct and are never implicitly converted. Every fixed
axis must have a positive length, fixed arrays must contain exactly that many
elements at runtime, and dense multidimensional values must retain their nested
shape. JSON and CBOR represent both with nested JSON-style arrays. AVRO uses
nested array schemas with item types inferred recursively from the internal
schema.

Append `OPTIONAL` to either an internal schema field or a wire schema field when the value may be absent. Optional fields are omitted from runtime records and emitted JSON payloads when no value is present.

## Wire Schemas

Wire schemas describe the serialized format on the transport side.

Wire schemas are either `STRICT` or `LOOSE`. Strict wire schemas reject payload fields that are not declared by the wire schema. Loose wire schemas accept extra payload fields and drop them before decoding into the internal schema.

JSON wire schema:

```nspl
CREATE [IF NOT EXISTS] STRICT WIRE JSON SCHEMA notification_wire (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);
```

CBOR wire schema:

```nspl
CREATE [IF NOT EXISTS] LOOSE WIRE CBOR SCHEMA notification_wire (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);
```

AVRO wire schema:

```nspl
CREATE [IF NOT EXISTS] STRICT WIRE AVRO SCHEMA notification_wire (
  user_id LONG,
  created_at STRING,
  payload STRING OPTIONAL
);
```

Wire schemas must also declare at least one field.

## Altering Schemas

Internal and wire schemas can be changed without dropping their whole dependent graph first.
Operations in one statement run from left to right, and each operation sees the result of the
previous one.

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
- `SET STRICT` and `SET LOOSE`

The format is required and must match the stored schema:

```nspl
ALTER WIRE JSON SCHEMA notification_wire
  ADD FIELD note string OPTIONAL,
  SET LOOSE;
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
`SHOW CREATE SCHEMA` and `SHOW CREATE WIRE SCHEMA` render the resulting canonical definitions.

On a running domain, a schema ALTER is applied through an automatic quiesce cycle: Nervix validates
first, stops new ingestion and generators, force-flushes buffered output, drains in-flight work,
installs the new graph, and resumes. This internal state is not a user-facing lifecycle command.
A drain timeout rejects the batch and resumes the old graph. On a stopped domain, Nervix validates
and persists the change without a quiesce cycle.

Runtime state whose record layout derives from an altered schema is recreated. Independent
stateful nodes retain their state. Persisted state carries a schema fingerprint so a stale layout
is never restored as the new type. Relay subscriptions closed by the rebuild report that they must
be recreated against the current schema.

## Codecs

A codec maps one transport payload format to one internal schema.

```nspl
CREATE [IF NOT EXISTS] CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification;
```

Schemaful codecs are type-strict. A JSON `string` wire field does not implicitly decode
into an internal `DATETIME` field. Declare the wire conversion explicitly:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_codec
  FROM WIRE JSON SCHEMA notification_wire
  TO SCHEMA notification
  ENCODE created_at AS RFC3339;
```

`created_at` is the internal schema field name. The matching wire field must be a
string, and the internal field must be `DATETIME`.

JAQ-native codecs parse a transport payload in a jaq-supported format and run explicitly directed
JAQ transformations. An ingestion transformation decodes the resulting JSON object into the
internal schema:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_cbor
  FROM CBOR
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON INGESTION '.';

CREATE [IF NOT EXISTS] CODEC notification_xml
  FROM XML
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON INGESTION '{user_id: (.c[] | select(.t == "user_id").c[0] | tonumber)}';
```

Protobuf codecs compile `.proto` files from an uploaded resource, decode or encode the selected message with `prost-reflect`, and use JAQ to translate between the protobuf JSON view and the internal schema:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_proto
  FROM PROTOBUF
  USING RESOURCE proto_bundle VERSION 1
  CONFIG {'file' = 'notification.proto', 'include' = '.'}
  MESSAGE 'nervix.test.Notification'
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS ON INGESTION '{user_id: .user_id, payload: .payload}';
```

The resource contains the `.proto` files. `CONFIG` declares compile parameters; `file`/`files` select source files and `include`/`includes` select import roots, all relative to the resource root. If no file is listed, all `.proto` files in the resource are compiled.

Current schemaful codec wire formats are:

- `JSON`, with an explicit JSON wire schema
- `CBOR`, with an explicit CBOR wire schema
- `AVRO`, with an explicit AVRO wire schema

Current JAQ-native codec formats are:

- `JSON`
- `YAML`
- `TOML`
- `XML`
- `CBOR`

Current protobuf codec format:

- `PROTOBUF`, with resource-backed `.proto` files, inline compile config, and message name

## JAQ Transformations

JAQ-backed codecs must declare a JAQ transform:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_codec
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON INGESTION '.payload'
    ON EMITTING '{payload: .}';
```

Semantics:

- no-wire codecs must use `FROM JSON|YAML|TOML|XML|CBOR ... WITH JAQ ...`
- protobuf codecs must use `FROM PROTOBUF USING RESOURCE ... CONFIG {...} MESSAGE ... WITH JAQ ...`
- schemaful codecs must use `FROM WIRE JSON|CBOR|AVRO SCHEMA ...` and do not carry JAQ transforms
- `WITH JAQ TRANSFORMATIONS` requires `ON INGESTION`, `ON EMITTING`, or both in that order
- `ON INGESTION` runs after parsing the native/protobuf payload and must yield exactly one JSON object compatible with the internal schema
- `ON EMITTING` runs after the runtime record has been converted into JSON and must yield exactly one native-format or protobuf-message value

JAQ-backed encode/decode is dispatched to blocking workers so expensive transforms do not stall async ingestor or emitter tasks.

## Why The Split Matters

The schema split lets Nervix:

- keep runtime typing independent from transport shape
- support multiple wire formats
- normalize awkward inbound JSON without changing the internal data model
- reshape outbound payloads during emission without changing the internal record layout
