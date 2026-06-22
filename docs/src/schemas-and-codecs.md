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
  labels VEC<STRING> OPTIONAL
);
```

Schemas must declare at least one field.

These types are the values Nervix stores in runtime records and uses for branch grouping, subscription matching, and processor logic.

`ARRAY<T, N>` stores a fixed-size list and maps to Arrow `FixedSizeList<T, N>`.
`VEC<T>` stores a variable-length list and maps to Arrow `List<T>`.
The element type `T` may be any internal primitive type.
JSON and CBOR represent both as JSON-style arrays; AVRO represents both as array fields with item types inferred from the internal schema.

Append `OPTIONAL` to either an internal schema field or a wire schema field when the value may be absent. Optional fields are omitted from runtime records and emitted JSON payloads when no value is present.

## Wire Schemas

Wire schemas describe the serialized format on the transport side.

JSON wire schema:

```nspl
CREATE [IF NOT EXISTS] JSON WIRE SCHEMA notification_wire (
  user_id integer,
  created_at string,
  payload string OPTIONAL
);
```

AVRO wire schema:

```nspl
CREATE [IF NOT EXISTS] AVRO WIRE SCHEMA notification_wire (
  user_id LONG,
  created_at STRING,
  payload STRING OPTIONAL
);
```

Wire schemas must also declare at least one field.

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

JAQ-native codecs parse a transport payload in a jaq-supported format, run a mandatory JAQ transformation, and then decode the resulting JSON object into the internal schema:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_cbor
  FROM CBOR
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '.';

CREATE [IF NOT EXISTS] CODEC notification_xml
  FROM XML
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '{user_id: (.c[] | select(.t == "user_id").c[0] | tonumber)}';
```

Protobuf codecs compile `.proto` files from an uploaded resource, decode or encode the selected message with `prost-reflect`, and use JAQ to translate between the protobuf JSON view and the internal schema:

```nspl
CREATE [IF NOT EXISTS] CODEC notification_proto
  FROM PROTOBUF
  USING RESOURCE proto_bundle VERSION 1
  CONFIG {'file' = 'notification.proto', 'include' = '.'}
  MESSAGE 'nervix.test.Notification'
  TO SCHEMA notification
  WITH JAQ TRANSFORMATION '{user_id: .user_id, payload: .payload}';
```

The resource contains the `.proto` files. `CONFIG` declares compile parameters; `file`/`files` select source files and `include`/`includes` select import roots, all relative to the resource root. If no file is listed, all `.proto` files in the resource are compiled.

Current schemaful codec wire formats are:

- `JSON`, with an explicit JSON wire schema
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
- schemaful codecs must use `FROM WIRE JSON|AVRO SCHEMA ...` and do not carry JAQ transforms
- `WITH JAQ TRANSFORMATION '<program>'` is shorthand for an ingestion transform
- `ON INGESTION` runs after parsing the native/protobuf payload and must yield exactly one JSON object compatible with the internal schema
- `ON EMITTING` runs after the runtime record has been converted into JSON and must yield exactly one native-format or protobuf-message value

JAQ-backed encode/decode is dispatched to blocking workers so expensive transforms do not stall async ingestor or emitter tasks.

## Why The Split Matters

The schema split lets Nervix:

- keep runtime typing independent from transport shape
- support multiple wire formats
- normalize awkward inbound JSON without changing the internal data model
- reshape outbound payloads during emission without changing the internal record layout
