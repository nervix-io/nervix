# Optional emitter batching

Status: specified. Not implemented. This document defines the complete contract for combining
several records into one externally observable batch, for every emitter kind and every wire format.

## Required outcome

An emitter may declare two hard limits — a maximum number of source messages and a maximum encoded
size — and then publishes batches instead of single records. A batch is one externally observable
unit: one broker message, one syslog frame, one Sentry envelope, one OTLP export request, one
insert, one bulk write, or one appended Iceberg data file. Its members are the eligible source
records it carries, in the order the emitter would have published them.

Batching is opt-in. Without the clause an emitter keeps exactly the payload, event, row and request
grouping it has today. Enabling it never changes what a record contains, only how many records the
destination receives at once and how they are framed.

Every batch is bounded twice, by member count and by the exact size of what is written. Neither
bound is derived from the other, neither has a default, and neither may be omitted. Nothing that
exists today stands in for them: `FLUSH ... MAX BATCH SIZE` measures Arrow memory rather than wire
bytes, and a connector's own request grouping bounds neither the members nor the bytes a receiver
sees as one message.

## What does not change

- An emitter without the batching clause publishes exactly as it does today, including per-record
  broker payloads, one Sentry event per envelope, one syslog frame per record, one OTLP export
  request per buffered batch, and Iceberg data files rolled at Iceberg's default target file size.
- One source record is still one payload value, one event, one row, one document or one data point.
  Nervix never merges two records into one, never rewrites a destination schema so that an array
  fits into it, and never creates an external entity.
- `FLUSH`, `COLLECT`, `MODE`, `ATTACHED`/`DETACHED`, `ON MESSAGE ERROR`, `ON GENERAL ERROR`,
  materialized-state dependencies, branch semantics, sensitivity and leakage, header invocations
  and ordering groups keep their current meanings.
- Payloads, prepared attempts and acknowledgement state stay in memory. Nothing about batching is
  persisted beyond the emitter's Model, and the retained data plane stays columnar: a container
  value exists only while one batch is being encoded.
- Transport-level grouping stays what it is. A Kafka producer's own record batching and linger, a
  Pulsar producer's pipelining, an SQS `SendMessageBatch` request and a database driver's
  round-trip grouping all keep working exactly as they do today, on both sides of this clause.
  None of them makes a receiver see fewer messages, which is why none of them satisfies this
  contract and why none of them is changed by it.

One public form is replaced rather than extended: the database sinks' `WITH MAX BATCH <n>` is
deleted, and the batching clause takes over declaring how many records one write carries. Database
emitters must be rewritten to the new clause; there is no second spelling and no defaulting of one
from the other. An emitter Model persisted in the previous shape carries a form that no longer
exists, so it fails to load with an error naming it and is recreated rather than reinterpreted.

## Concepts

- **Member.** One eligible source record carried by a batch. A record is eligible after its source
  predicate, construction, route filter, header invocations and per-record encoding have all
  succeeded. Records rejected before that point are never members.
- **Candidate.** The run of members the emitter offers to the encoder for one batch, taken in
  publication order and bounded by the declared maximum message count.
- **Batch payload.** The bytes the encoder produced for a candidate, or — for a sink that writes
  through a driver rather than a codec — the request that carries the candidate.
- **Batch container.** The shape the batch payload has in the target's own format: a JSON array, a
  CBOR array, an Avro array datum, a protobuf batch message, an `INSERT` carrying many rows, an
  OTLP export request, a Parquet data file. Every container is named in this document; none is
  invented at runtime.
- **Packing order.** The order members occupy in the container: the order the emitter would have
  published them in, which is the row order of the buffered batch they come from.
- **Batch-compatible.** Two records may share a batch only when everything the container carries
  once is equal for both of them.
- **Measured size.** The exact byte length of the batch payload. Nervix does not estimate it.

## The batching clause

### Grammar

```nspl,ignore
BATCH MAX MESSAGES <n> MAX SIZE <bytes>
```

The clause sits between the complete `TO` sink clause and the `FLUSH` policy:

```nspl,ignore
CREATE [IF NOT EXISTS] [ATTACHED | DETACHED] EMITTER <name>
  FROM <relay> [WHERE <expr>] [, ...] [COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]]
  [WITH MATERIALIZED STATE ...]
  TO <sink clause, including MODE, ENCODE USING and construction>
  [BATCH MAX MESSAGES <n> MAX SIZE <bytes>]
  FLUSH EACH <duration> MAX BATCH SIZE <bytes> | FLUSH IMMEDIATE
  ON MESSAGE ERROR <policy>
  ON GENERAL ERROR <policy>;
```

`BATCH MAX MESSAGES` and `MAX SIZE` are composed keyword phrases. `<n>` is a positive integer
literal and `<bytes>` is a byte-size literal in the form `MAX BATCH SIZE` already uses, such as
`1MiB`. Completion offers `BATCH MAX MESSAGES` wherever the clause may begin, and offers it before
`FLUSH EACH` and `FLUSH IMMEDIATE` for a sink that requires it.

### Where the clause is required

| Emitter sinks | Clause | When it is absent |
| --- | --- | --- |
| Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ, SQS, Sentry, Syslog | Optional | One record per message, event or frame |
| OTEL | Optional | One export request per buffered batch, with no membership bound |
| Iceberg | Optional | Data files rolled at Iceberg's 512 MiB default target size |
| ClickHouse, Postgres, MySQL, MongoDB | Required | The statement is rejected: a database write has no unbounded form |

The four database sinks always write several rows in one statement or command, so the bound is not
optional for them; it moves out of the sink clause, where `WITH MAX BATCH <n>` used to sit, into
this clause, and gains the encoded-size bound every other sink gets.

### Validation

A statement is rejected, naming the emitter and the offending value, when:

- `MAX MESSAGES` is zero or above 65,536. The upper bound is the largest batch size Nervix's
  message histograms track, so a batch at the limit is still observable as one batch.
- `MAX SIZE` is zero.
- `MAX SIZE` exceeds a maximum the destination protocol itself fixes. The one such maximum today is
  the SQS message limit Nervix enforces, 256 KiB.
- The clause is absent on a ClickHouse, Postgres, MySQL or MongoDB sink.
- The clause is present on a Sentry emitter whose codec declares no `ON EMITTING BATCH`
  transformation. The Sentry envelope protocol admits at most one `event` item per envelope, so
  Nervix cannot derive a container for it and the codec must state one.
- The clause is present on an emitter whose codec is a protobuf codec that declares no
  `BATCH MESSAGE`. Protobuf has no self-delimiting sequence, so the batch message must be named.

### Altering

`ALTER EMITTER` gains two operations:

```nspl,ignore
ALTER EMITTER <emitter>
    SET BATCH MAX MESSAGES <n> MAX SIZE <bytes>
  | DROP BATCH
  [, ...];
```

`SET BATCH` adds or replaces the clause. `DROP BATCH` fails when the emitter has no clause and when
the sink requires one. Both are `ENTITY_PAUSE` changes: the emitter's sources are gated, collected
input and pending sink output are drained, the emitter task is replaced, and the gates are
released. This is the classification sink, publishing-mode and codec changes already use, and it is
what keeps a payload that was prepared under the old limits from being retried under new ones.
`SET TO <sink clause>` no longer carries a database maximum batch and therefore never changes
batching; changing a sink to one that requires the clause fails when the clause is absent.

### Rendering

`SHOW CREATE EMITTER` renders the clause verbatim between the sink clause and `FLUSH`.
`DESCRIBE EMITTER` gains one line after `sink:`, holding the declared clause:

```text
batch: MAX MESSAGES 500 MAX SIZE 1MiB
```

or, for an emitter that declares none:

```text
batch: none
```

## Packing

### Which records may share a batch

A batch never spans two buffered input batches. Each buffered batch carries one source relay, one
concrete branch or no branch at all, and one accepted domain execution snapshot, so relay identity,
branch identity and the snapshot every expression in the batch was evaluated under are uniform by
construction. A batch never mixes branches, never mixes relays, and never mixes execution
snapshots.

Within one buffered batch, two records are batch-compatible when every attribute the container
carries once is equal for both:

- the message key, which is the record's concrete branch key where the sink exposes one
- the complete ordered sequence of written headers, including repeated values on the sinks that
  preserve them
- the ordering group, where the sink delivers in order per group
- for a codec using the `SYSLOG` wire schema, every syslog header field the codec's schema declares
  except `timestamp`

The emitter walks the buffered batch in row order and adds each eligible record to the open batch.
A record that is not batch-compatible with the open batch closes it and opens a new one. A record
is never skipped over to reach a compatible one later in the batch, so order is preserved
end to end and no record is reordered across a boundary.

Compatibility is what keeps a batch from asserting something untrue about one of its members, and
it is visible in how large batches get. A header written from a per-record value limits batches to
runs of records that share that value, and so does a per-record ordering group; carrying the value
in the payload instead, where every member has its own, restores full batching. An emitter without
written headers, without an ordering group and with one branch per buffered batch has nothing that
can split a batch except its two declared limits.

### Order, cadence and partial batches

`FLUSH` decides when an emitter publishes and how much it holds. Batching decides only how what it
holds is divided into payloads. A flush holding 1,000 eligible records and declaring
`MAX MESSAGES 100` publishes ten payloads; a flush holding three publishes one payload with three
members. A batch is never held back waiting to reach its maximum, and batching adds no timer,
deadline or cadence of its own. A partial batch — one that reached neither limit — is published
exactly like a full one.

`FLUSH IMMEDIATE` is unchanged, including its system-owned 100 µs minimum batching window. With
batching declared, whatever that window collected becomes one batch, which is usually one member.

### Singleton and empty batches

A batch with one member keeps the batch shape: a one-element array, a batch message with one
repeated entry, an insert with one row. The shape is a property of the emitter's declaration, not
of how many records happened to be available.

A buffered batch with no eligible record publishes nothing. There is no empty array, no empty
insert and no empty request.

### Counting

`MAX MESSAGES` counts members: eligible source records, counted after source predicates, input
collection, construction, route filtering, header invocation and per-record encoding.

The count is a property of the members, not of the container. A batch transformation that changes
the cardinality of the value it produces — grouping members, splitting them, emitting a summary —
changes the container and nothing else. Acknowledgement, message metrics, error attribution and
the `MAX MESSAGES` limit all keep counting the source records that went in.

## Encoded size

### Exact measurement

`MAX SIZE` bounds the exact byte length of the batch payload: the bytes the codec produced, or the
bytes of the request a row sink builds. Nervix does not compute a conservative estimate of encoded
size anywhere in this contract, so there is no bound that can be wrong in either direction.
Escaping, separators, whitespace, field names, length prefixes, nesting, bytes encoding and the
container's own brackets are all counted, because they are all present in the bytes being measured.
Arrow payload bytes, which `FLUSH ... MAX BATCH SIZE` and Iceberg's `COMMIT ... MAX SIZE` measure,
are a different quantity and are never substituted for it.

Every total this contract keeps — a running encoded length, a member count, a limit compared
against either — is computed with checked arithmetic. A total that cannot be represented is a
typed failure of that batch, never a wrapped or saturated number that would make an oversize
payload look admissible.

The bound covers the payload, not the framing the sink adds around it. Each sink's row in the
[sink matrix](#sink-matrix) names exactly what the payload is and what framing sits outside it.

### Bounded encoding and subdivision

A candidate is encoded into a buffer that stops at the declared maximum. Encoding that completes
within the bound yields a batch whose measured size is its exact length. Encoding that reaches the
bound is abandoned at that point, so a payload larger than the limit is never fully built and never
held in memory.

An abandoned candidate is subdivided: the first half of its members in packing order becomes the
new candidate, and the rest returns to the front of the queue, preserving order. Subdivision never
assumes that a smaller candidate encodes smaller. A batch transformation may produce more bytes
from fewer members, so each halving is re-encoded and re-measured under the same bound, and the
process stops only when an encoding completes or a single member is left. Each halving strictly
reduces the member count, so a candidate of `n` members needs at most `⌈log2(n)⌉ + 1` encodings,
each bounded by `MAX SIZE`.

### Oversize records

A single member whose batch payload still exceeds `MAX SIZE` after an actual bounded encoding has
been attempted is rejected: it follows `ON MESSAGE ERROR` with a validation error whose message
names the limit and the encoding that exceeded it. Packing then continues with the members that
follow it. A record is never rejected on the strength of an estimate, and never rejected without
the encoder having tried to encode it alone.

A record that fits the emitter's `MAX SIZE` but not the destination's own limit is rejected by the
destination exactly as it is today, through `ON MESSAGE ERROR`.

### Working memory is not wire size

`MAX SIZE` bounds what is written. It bounds neither the memory a batch transformation uses while
it runs nor how long it runs. A batch transformation executes on the same bounded blocking
execution path `ON EMITTING` already uses, under the same admission and memory accounting, and a
transformation that exhausts that budget fails as an evaluation failure of the batch rather than as
an oversize payload.

## Batch containers

### The member value

Every container is built from member values, and a member value is exactly the value the
single-record path would have encoded for that record:

| Codec kind | Member value |
| --- | --- |
| `WIRE JSON`, `WIRE CBOR`, `WIRE AVRO` | The wire object the codec encodes for the record, with its declared field encodings applied |
| JAQ-native (`JSON`, `YAML`, `TOML`, `XML`, `CBOR`) | The single value the codec's `ON EMITTING` transformation produced |
| `PROTOBUF` | The protobuf JSON view the codec's `ON EMITTING` transformation produced |
| `SYSLOG` | The complete RFC 5424 message the record encodes to on its own |

A record whose member value cannot be produced is rejected individually through `ON MESSAGE ERROR`
before packing, exactly as a failed per-record encoding is rejected today.

### Containers by format

| Format | Container | Written form |
| --- | --- | --- |
| JSON | Array of the member values | `[{"user_id": 1}, {"user_id": 2}]` |
| CBOR | Definite-length array of the member values | Major type 4 with the member count, then the members |
| YAML | One document holding a sequence of the member values | `[{user_id: 1}, {user_id: 2}]` |
| AVRO | One Avro datum of type `array` whose items are the codec's declared record schema | A single array block: the member count, the members, then the terminating zero |
| TOML | One document with a single key, `batch`, holding the member values | `[[batch]]` sections, one per member |
| XML | One root element, `batch`, whose children are the member values | `<batch><notification …/><notification …/></batch>` |
| PROTOBUF | One instance of the codec's declared `BATCH MESSAGE` | The members in that message's repeated field |
| SYSLOG | One RFC 5424 message carrying the members' own messages | Described below |

JSON, CBOR, YAML, AVRO, TOML and XML containers are written by the same writer the codec already
uses for one record, with the same whitespace and the same number formatting, so a schemaful JSON
codec writes a compact array and a JAQ-native JSON codec writes the spaced form shown above. A
declared batch transformation may replace any of these containers with an envelope of its own.

TOML and XML have no top-level sequence: a TOML document is a table and an XML document has exactly
one root element. Their containers are therefore the deliberate single-key and single-root forms
above rather than a bare sequence, which for XML would be a document fragment Nervix's own XML
reader would reject as a second root element.

Protobuf has no self-delimiting sequence either, and unlike TOML and XML its container cannot be
derived from the format alone, because the wrapper is a user-defined message. A protobuf codec
therefore names it.

### Protobuf batch messages

```nspl,ignore
CREATE CODEC <name>
  FROM PROTOBUF
  USING RESOURCE <resource> VERSION <n> | LATEST
  CONFIG { ... }
  MESSAGE '<message>'
  BATCH MESSAGE '<batch message>'
  TO SCHEMA <schema>
  WITH JAQ TRANSFORMATIONS ON EMITTING '<program>' [ON EMITTING BATCH '<program>'];
```

`BATCH MESSAGE` names a message in the same compiled descriptors. Without a batch transformation
the batch message must declare exactly one field and that field must be `repeated <message>`;
Nervix fills it with the members in packing order. With a batch transformation, the batch message
may have any shape the transformation's single output is a valid instance of. `CREATE CODEC` fails
when the named message does not exist, and a batching emitter fails validation when the batch
message has no single repeated field and the codec declares no batch transformation.

### The SYSLOG container

A codec using the `SYSLOG` wire schema carries no transformations, so its container is defined by
this document. One batch is one RFC 5424 message:

- `PRI`, `HOSTNAME`, `APP-NAME`, `PROCID`, `MSGID` and `STRUCTURED-DATA` are the members' common
  values. They are common by construction: records that differ in any of them are not
  batch-compatible and open a new batch.
- `TIMESTAMP` is the first member's timestamp, or the RFC 5424 nil value when the codec's schema
  omits the field or the first member's value is null.
- `MSG` is a JSON array of strings. Each element is the complete RFC 5424 message that member
  encodes to on its own, so every member keeps its own timestamp and its own message text, and a
  receiver can split the array back into the individual messages it stands for.

Nothing a member carries is dropped and nothing is merged. The frame's own header is true for every
member it carries, because the compatibility rule made it so. The container is a property of the
codec rather than of the sink, so an emitter that publishes `SYSLOG`-encoded payloads to a broker
batches them into the same message.

## Batch transformations

A JAQ-backed codec — JAQ-native or protobuf — may declare a third transformation:

```nspl,ignore
WITH JAQ TRANSFORMATIONS
  [ON INGESTION '<program>']
  [ON EMITTING '<program>']
  [ON EMITTING BATCH '<program>']
```

The three forms keep their written order, at least one is required, and `ON EMITTING BATCH`
requires `ON EMITTING`, because its input is built from that transformation's outputs. Codecs using
a declared `WIRE` schema and codecs using the `SYSLOG` wire schema carry no transformations and
therefore no batch transformation.

- **Input.** One array: the member values of the candidate, in packing order. The array always
  holds at least one element, because an empty batch is never published, and it holds exactly one
  for a single-member batch.
- **Output.** Exactly one value, which must be valid in the codec's format. For a protobuf codec it
  must be a valid instance of the declared `BATCH MESSAGE`.
- **Zero outputs, more than one output, an evaluation failure, or an output the format cannot
  write** is a batch transformation failure.

The transformation sees member values and nothing else. It cannot reach a sensitive value that was
not already leaked explicitly during construction, and it cannot reach the source row, the working
message, materialized state or headers.

The default container is what the format's row of the [container table](#containers-by-format)
names. A batch transformation replaces it, and both an array mapping and an envelope are ordinary
uses:

```nspl,ignore
ON EMITTING BATCH '{schema_version: 2, records: .}'
ON EMITTING BATCH 'map({id: .user_id, at: .created_at})'
ON EMITTING BATCH '{count: length, records: .}'
```

Single-record `ON EMITTING` keeps its current meaning exactly: it runs per record, produces one
value per record, and is what an emitter without the batching clause publishes.

## Sink matrix

The matrix covers every emitter kind and every variant of it. Variants that change the batch
contract are listed; variants that do not — a Postgres conflict policy, an Iceberg object-store
backend, a MongoDB upsert policy, an OTLP transport — are named so their equivalence is on the
record rather than assumed.

### Container and receiver interpretation

| Sink | Variants | Batch container | Receiver interpretation |
| --- | --- | --- | --- |
| Kafka | `NO_ACK`, `ACK SEQUENTIAL`, `ACK PARALLEL` | One Kafka record whose value is the batch payload, with the members' common key and headers | One record; the consumer decodes the container and sees the members |
| Pulsar | `NO_ACK`, `ACK SEQUENTIAL`, `ACK PARALLEL` | One Pulsar message whose payload is the batch payload | One message carrying the members |
| RabbitMQ | `NO_ACK`, `ACK SEQUENTIAL`, `ACK PARALLEL` | One AMQP message whose body is the batch payload | One message carrying the members |
| Redis Pub/Sub | `NO_ACK` | One `PUBLISH` message whose payload is the batch payload | One channel message carrying the members |
| MQTT | `QOS 0`, `QOS 1`, `QOS 2` | One PUBLISH packet whose payload is the batch payload | One message on the topic carrying the members |
| NATS | `NO_ACK` (core), `JETSTREAM ACK SEQUENTIAL`, `JETSTREAM ACK PARALLEL` | One message on the subject whose payload is the batch payload | One core message or one stream message carrying the members |
| ZeroMQ | `NO_ACK` | One single-frame PUSH message whose frame is the batch payload | One message; the puller reads the first frame and decodes the members |
| SQS | `SINGLE`, `BATCH`; standard and FIFO queues | One SQS message whose body is the batch payload, with the members' common attributes and FIFO group | One queue message carrying the members; `MODE BATCH` still packs up to ten such messages per request, and each stays an independent message |
| Sentry | `ACK` | One envelope with one `event` item, whose event is the single value the codec's batch transformation produced | One Sentry event carrying the members in the field the transformation placed them in, typically `extra` |
| Syslog | `udp`, `tcp` octet-counting, `tcp` non-transparent, `tls` | One transport frame whose content is the batch payload | One syslog message; with a `SYSLOG` codec its `MSG` is the array of the members' own RFC 5424 messages |
| OTEL | `LOGS`, `TRACES`, `METRIC`; `grpc` and `http/protobuf` | One OTLP export request with one resource, one scope and the members as log records, spans or data points | One export of the members |
| ClickHouse | `ACK` | One `INSERT INTO <table> FORMAT JSONEachRow` request whose body holds one JSON object per member | One insert of the members as rows |
| Postgres | `ACK`; `DO UPDATE`, `DO NOTHING`, no conflict policy | One `INSERT ... SELECT ... FROM unnest(...)` statement with one array element per member per mapped column | One insert of the members as rows |
| MySQL | `ACK`; `DO UPDATE`, `DO NOTHING`, no conflict policy | One multi-row `INSERT ... VALUES (...), (...)` statement | One insert of the members as rows |
| MongoDB | `ACK`; insert, `DO UPDATE`, `DO NOTHING` | One insert or bulk write carrying one document per member | One write of the members as documents |
| Iceberg | `ACK`; `ON S3`, `ON GCS`, `ON AZURE_BLOB` | One appended Parquet data file whose rows are the members | One data file in the table's next snapshot; the commit appends every data file it wrote |

For every row sink the relationship to the array is exact: the native repeated-record container
carries the same members, in the same order, counted the same way, confirmed by one response. The
only difference from a record sink is the encoding the destination requires — a table takes rows,
not an array — and Nervix never changes a destination schema to make an array fit one.

### Byte boundary and native limits

| Sink | `MAX SIZE` measures | Framing outside the bound | Native limits |
| --- | --- | --- | --- |
| Kafka | The encoded batch payload written as the record value | The record key, headers and the producer's own record framing | Broker `message.max.bytes` and producer `max.request.size`, both 1 MiB by default |
| Pulsar | The encoded batch payload written as the message payload | Message metadata and properties | Broker `maxMessageSize`, 5 MiB by default |
| RabbitMQ | The encoded batch payload written as the message body | Basic properties and headers | `max_message_size`, 16 MiB by default in RabbitMQ 4.x |
| Redis Pub/Sub | The encoded batch payload written as the message | The RESP command framing | `proto-max-bulk-len`, 512 MiB by default |
| MQTT | The encoded batch payload written as the PUBLISH payload | The fixed and variable headers, including the topic | The broker's `Maximum Packet Size`; the protocol maximum is 268,435,455 bytes |
| NATS | The encoded batch payload written as the message payload | The subject and protocol headers | `max_payload`, 1 MiB by default |
| ZeroMQ | The encoded batch payload written as the frame | The ZMTP frame header | None in the protocol; the receiving socket's `ZMQ_MAXMSGSIZE` |
| SQS | The encoded batch payload written as the message body | Message attributes and the FIFO group, which the service counts against the same limit | 256 KiB per message and per `SendMessageBatch` request, as Nervix enforces them; ten entries per request |
| Sentry | The event JSON the batch transformation produced | The envelope header, the item header and the newline framing Nervix adds | Sentry rejects events above 200 KB compressed or 1 MB decompressed; a Relay deployment may lower it |
| Syslog | The encoded batch payload written as the frame content | The octet-count prefix or the trailing LF | 65,507 bytes per UDP datagram; an octet count of at most ten digits; non-transparent TCP framing rejects a payload containing LF |
| OTEL | The encoded protobuf export request, before optional gzip | The gRPC or HTTP request framing and headers | The receiver's request-size limit; the OpenTelemetry Collector's gRPC default is 4 MiB |
| ClickHouse | The `JSONEachRow` request body | The HTTP request line, headers and the statement | No fixed body limit; `max_query_size` bounds the statement, not the streamed data |
| Postgres | The statement text and every bound parameter value as the protocol encodes it | The extended-query protocol messages around them | 1 GB per value, and a 32-bit protocol message length |
| MySQL | The statement text and every bound value as the protocol encodes it | The packet headers | `max_allowed_packet`, 64 MiB by default on MySQL 8.x |
| MongoDB | The BSON documents of the members and the command's own fields | The wire-protocol message header | 16 MiB per document, and the `maxWriteBatchSize` and `maxMessageSizeBytes` the server reports — 100,000 operations and 48 MB today |
| Iceberg | The Parquet data file as written to object storage | The manifest and snapshot metadata the commit writes | None fixed by Iceberg; without the clause Nervix rolls data files at 512 MiB |

Iceberg is the one sink with two byte bounds, and they measure different things on purpose:
`BATCH ... MAX SIZE` bounds one written data file, while `COMMIT EACH ... MAX SIZE` bounds the
Arrow payload bytes staged before a commit becomes due. A declaration that sets the first above the
second is valid and simply means every commit publishes one data file.

### Success and partial-failure boundary

| Sink | One batch succeeds when | Partial outcomes |
| --- | --- | --- |
| Kafka | The producer queue accepts the record (`NO_ACK`), or its delivery report arrives (`ACK`) | None: the record is the unit, so the batch is delivered or it is not |
| Pulsar | The producer accepts the message (`NO_ACK`), or the broker receipt arrives (`ACK`) | None |
| RabbitMQ | The channel accepts the message (`NO_ACK`), or the publisher confirm arrives (`ACK`) | None |
| Redis Pub/Sub | The server answers `PUBLISH` | None |
| MQTT | The client accepts the packet (QoS 0), `PUBACK` arrives (QoS 1), or the QoS 2 handshake completes | None |
| NATS | The connection flush completes (core), or the `PubAck` arrives (JetStream) | None |
| ZeroMQ | The socket accepts the frame | None |
| SQS | The service answers for that message or that batch entry | Per entry: the batch is one entry, so entry-level results attribute to whole batches, never to members |
| Sentry | The envelope request returns a success status | None |
| Syslog | The local socket accepts and flushes the complete frame | None |
| OTEL | The export response arrives; `partial_success` acknowledges the whole request with a warning, as today | None: OTLP does not identify which data points a partial success rejected |
| ClickHouse | The insert returns | On a record-specific failure the batch is re-executed one row at a time, so healthy rows land and poison rows follow `ON MESSAGE ERROR` |
| Postgres | The statement returns | Same record-specific isolation as today |
| MySQL | The statement returns | Same record-specific isolation as today |
| MongoDB | The write returns | Per document: MongoDB identifies members, so healthy documents acknowledge and poison documents follow `ON MESSAGE ERROR` without an isolation pass |
| Iceberg | The catalog commit succeeds | None: a commit publishes every data file it wrote or none of them |

One successful external confirmation acknowledges every member of the batch it confirms. Native
per-member outcomes apply only where the external contract names the member, which is MongoDB's
per-document write result. Everywhere else the batch is the smallest unit the destination can
speak about, and record-specific isolation — where a sink already has it — is a failure path that
runs after the batch failed, not a second way to publish.

## Interaction with existing policies

| Policy | Interaction |
| --- | --- |
| `FLUSH EACH <duration> MAX BATCH SIZE <bytes>` | Unchanged. It decides when the emitter publishes and how many Arrow bytes it holds; batching divides what it holds. The two bounds measure different quantities and neither is derived from the other. |
| `FLUSH IMMEDIATE` | Unchanged, including the 100 µs minimum batching window. Batching does not extend it. |
| `COLLECT FOR <duration> [MAX BATCH SIZE <bytes>]` | Unchanged. It changes how much one buffered batch holds, and therefore how many members a batch can draw on, and nothing else. Collection stays independent per source relay and concrete branch. |
| `MODE` | Unchanged in form. A confirmation window counts publishes: without batching a publish is one record, with batching it is one batch. `ACK PARALLEL MAX 100` with `MAX MESSAGES 500` therefore exposes up to 50,000 records to one ambiguity window, which is the declaration's meaning, not a hidden multiplier. |
| SQS `MODE BATCH` | Unchanged and orthogonal. `MODE` groups independent SQS messages into one service request; batching decides how many source records one SQS message carries. A request still holds at most ten messages and at most the service's request limit. |
| Database `WITH MAX BATCH <n>` | Deleted. The batching clause declares the same bound for the four database sinks and adds the encoded-size bound. |
| Iceberg `COMMIT EACH <duration> MAX SIZE <bytes>` | Unchanged. It decides when staged rows are published; batching decides how the published rows are divided into data files. One commit may append several data files. |
| `ATTACHED` / `DETACHED` | Unchanged. A detached emitter still acknowledges upstream at relay fan-out and still performs its declared confirmations and retries. |
| `ON MESSAGE ERROR` | Applies per member, as described under [failure semantics](#failure-semantics). `SEND TO` error records travel to a relay, not to the sink, so they are never batched by this clause. |
| `ON GENERAL ERROR` | Unchanged. |
| Materialized state | Unchanged. Dependencies are node-wide and resolved per record before construction. |
| Branches | Unchanged, and reinforced: a batch never spans branches, and branch identity still collapses only at a successful external boundary. |
| Headers and ordering groups | Unchanged per record, and part of batch compatibility, so a batch never carries a header set or ordering group that is untrue for one of its members. |
| Sensitivity | Unchanged. Every external value still requires explicit leakage at construction. Batching introduces no new path to an unleaked value, and a batch transformation sees only values that already passed that rule. |

## Membership, acknowledgement and outcomes

The emitter host owns membership. It selects the members, builds the payload, and applies the
outcome; a connector receives a payload and answers for it.

- One successful external confirmation acknowledges every member of that batch, at the sink
  completion point `MODE` selects.
- A definitive rejection of the batch rejects every member through `ON MESSAGE ERROR`, with one
  shared error reference so an operator can see that they failed together.
- An infrastructure failure or an ambiguous outcome retains the accepted prepared payload and its
  membership verbatim. The retry writes the same bytes with the same members; it does not re-pack,
  re-encode or re-run a batch transformation, so a duplicate that a retry produces is the same
  batch the destination may already hold. Upstream acknowledgement leases stay alive across the
  wait, as they do today.
- Where the external contract names members, a retry carries the members that are neither
  delivered nor rejected, in their original relative order, and may therefore be a smaller batch
  than the first attempt.
- Fan-out is unchanged: an attached source acknowledgement completes only when every attached
  emitter has reached its completion point, and a sibling failure can redeliver records a batching
  emitter already published.

## Failure semantics

| Failure | Attribution | Operation | Effect |
| --- | --- | --- | --- |
| A member value cannot be produced for one record | That record | `encode` | The record follows `ON MESSAGE ERROR`; the batch continues without it |
| The ordering group cannot be evaluated for one record | That record | `publish` | Unchanged from today |
| A batch transformation yields no output, more than one output, an evaluation failure, or a value the format cannot write | Every member of that batch | `encode` | All members follow `ON MESSAGE ERROR` with one shared reference; the diagnostic names the codec and the cause and quotes no payload value |
| A single member still exceeds `MAX SIZE` after a bounded encoding of it alone | That record | `encode` | The record follows `ON MESSAGE ERROR` with a validation error naming the measured limit |
| The destination definitively rejects the batch | Every member of that batch | `publish` | All members follow `ON MESSAGE ERROR` with one shared reference |
| The destination names a member it rejected | That member | `publish` | That member follows `ON MESSAGE ERROR`; the others are unaffected |
| The destination fails for an infrastructure reason, or the outcome is ambiguous | No member | `publish` | The prepared payload is retained and retried on the declared backoff, with backpressure |

A batch transformation failure is a property of the array the transformation received, not of any
one member, so Nervix does not subdivide a batch to look for a member to blame. An operator who
needs per-record attribution for such a failure uses a smaller `MAX MESSAGES` or no batching.

Every diagnostic in this contract names the emitter, the codec where one is involved, the member
count and the measured or declared byte limit, and never a payload value.

## Lifecycle

- Adding, changing or removing the batching clause is an `ENTITY_PAUSE` change, so pending output
  is drained under the old limits and the replaced emitter starts packing under the new ones. A
  prepared payload is never retried under limits it was not built for.
- Quiesce, drain and force flush publish what is buffered, divided into batches exactly as ordinary
  flushes are. A drain therefore ends with partial batches, not with work waiting for a batch to
  fill.
- Branch eviction drops that branch's buffered work, including any prepared payload built from it,
  as it does today.
- Relocation, node shutdown and startup recovery are unchanged. Batch payloads, membership and
  acknowledgement maps are in-memory hot-path state and are never persisted or replicated.
- A domain's execution snapshot is unchanged: a batch inherits the snapshot of the buffered batch
  it came from, and a retry does not re-evaluate expressions at a later domain time.
- Clock ownership is unchanged. `FLUSH EACH` and Iceberg's commit cadence stay domain-logical,
  while retry backoff, acknowledgement keepalive and sink acknowledgement timeouts stay on the
  physical clock. Batching declares no duration and therefore belongs to neither clock.

## Observability

`DESCRIBE EMITTER` reports the declared clause on its `batch:` line. Four raw metric families
describe published batches, for every emitter, with and without the clause:

- `nervix_emitter_payloads_total`: batch payloads published.
- `nervix_emitter_payload_messages`: histogram of members per published payload.
- `nervix_emitter_payload_bytes`: histogram of the measured size of published payloads.
- `nervix_emitter_payload_subdivisions_total`: candidates re-encoded because an encoding reached
  `MAX SIZE`. A rising count means the declared message count and byte size disagree about what a
  batch should be.

`nervix_messages_total` keeps counting source records, and `nervix_bytes_total`,
`nervix_batches_total` and `nervix_messages_per_batch` keep describing the buffered batches an
emitter flushes, in the Arrow terms every other node reports them in. The measured size of what was
written is a new observable rather than a redefinition of an existing one.

Batching is not a lifecycle event: batch composition, subdivision and per-batch sizes are `debug`
or `trace` detail and carry no payload values.

## Limits

| Limit | Value |
| --- | --- |
| `MAX MESSAGES` | 1 to 65,536 |
| `MAX SIZE` | At least 1 byte; at most 256 KiB for SQS |
| Encodings per candidate | At most `⌈log2(n)⌉ + 1` for `n` members, each bounded by `MAX SIZE` |
| Members per batch | At most `MAX MESSAGES`, and never more than one buffered batch holds |
| Batch cadence | None: batching owns no timer, and `FLUSH` alone decides when output leaves |

## Examples

### A broker emitter, with and without the clause

```nspl
CREATE IF NOT EXISTS EMITTER kafka_notifications
  FROM notifications
  TO KAFKA kafka_main TOPIC notifications_out
    MODE ACK PARALLEL MAX 100 ACK TIMEOUT 30s
      RETRY POLICY BACKOFF 250ms MAX 30s
    ENCODE USING notification_codec
  INHERIT ALL
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
  ON MESSAGE ERROR LOG
  ON GENERAL ERROR LOG;
```

Three records in one flush produce three Kafka records:

```json
{"user_id":1,"action":"login"}
{"user_id":2,"action":"logout"}
{"user_id":3,"action":"login"}
```

Adding one clause produces one Kafka record:

```nspl,ignore
  BATCH MAX MESSAGES 500 MAX SIZE 1MiB
  FLUSH EACH 100ms MAX BATCH SIZE 1MiB
```

```json
[{"user_id":1,"action":"login"},{"user_id":2,"action":"logout"},{"user_id":3,"action":"login"}]
```

A fourth record arriving in a later flush is published as `[{"user_id":4,"action":"login"}]`: the
batch shape does not depend on how many records were available.

### An envelope instead of an array

```nspl,ignore
CREATE IF NOT EXISTS CODEC notification_envelope
  FROM JSON
  TO SCHEMA notification
  WITH JAQ TRANSFORMATIONS
    ON EMITTING '{id: .user_id, action: .action}'
    ON EMITTING BATCH '{schema_version: 2, count: length, records: .}';
```

```json
{"schema_version": 2, "count": 3, "records": [{"id": 1, "action": "login"}, {"id": 2, "action": "logout"}, {"id": 3, "action": "login"}]}
```

`count` is the array's length, which the transformation computed. Nervix's own message count,
acknowledgement and metrics still count the three source records, whatever the transformation
reports.

### Other formats

| Format | Three members on the wire |
| --- | --- |
| YAML | `[{user_id: 1}, {user_id: 2}, {user_id: 3}]` |
| CBOR | A definite-length array header for three items, then the three member maps |
| AVRO | One array block of three items in the codec's declared record schema, then the terminating zero |
| TOML | `[[batch]]` sections, one per member, in packing order |
| XML | `<batch><notification user_id="1"/><notification user_id="2"/><notification user_id="3"/></batch>` |

### Protobuf

```proto
message Notification { uint32 user_id = 1; string action = 2; }
message NotificationBatch { repeated Notification notifications = 1; }
```

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

The batch message declares one repeated field of the member message, so no batch transformation is
needed: the consumer parses one `NotificationBatch` holding three `Notification` values.

### Syslog

A codec using the `SYSLOG` wire schema, three members that agree on facility, severity, hostname
and application, in one frame. Without the clause these are three frames:

```text
<134>1 2026-09-24T10:15:00Z app-01 checkout - - - order accepted
<134>1 2026-09-24T10:15:01Z app-01 checkout - - - order accepted
<134>1 2026-09-24T10:15:02Z app-01 checkout - - - order shipped
```

With it they are one:

```text
<134>1 2026-09-24T10:15:00Z app-01 checkout - - - ["<134>1 2026-09-24T10:15:00Z app-01 checkout - - - order accepted","<134>1 2026-09-24T10:15:01Z app-01 checkout - - - order accepted","<134>1 2026-09-24T10:15:02Z app-01 checkout - - - order shipped"]
```

The frame's own header is the one every member shares, its timestamp is the first member's, and
each member keeps its own timestamp and text inside the array. `MSG` begins with `[` here, which
RFC 5424 resolves without ambiguity: `STRUCTURED-DATA` is the nil value immediately before it, and
`MSG` is everything after the space that follows the nil value.

A fourth record with a different severity is not batch-compatible: it closes this frame and opens
the next one.

### Sentry

```nspl,ignore
CREATE CODEC sentry_batch_codec
  FROM JSON
  TO SCHEMA error_event
  WITH JAQ TRANSFORMATIONS
    ON EMITTING '{message: .message, level: .level, environment: .environment}'
    ON EMITTING BATCH '{message: "batched errors", level: "error", extra: {records: .}}';
```

One envelope, one `event` item, one Sentry event whose `extra.records` is the array of the members.
Sentry's envelope protocol admits at most one `event` item per envelope, so a batch is one event
carrying its members and not several events in one request. An emitter that needs one Sentry issue
per record declares no batching clause, which is the default.

### A database emitter

```nspl,ignore
  TO POSTGRES postgres_client INSERT TO TABLE my_table
  VALUES { "user_id" = input.user_id, "action" = LOWER(input.action) }
  ON CONFLICT ("user_id") DO UPDATE
  MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
  BATCH MAX MESSAGES 500 MAX SIZE 8MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
```

The clause replaces `WITH MAX BATCH 500` and adds the bound on the encoded insert. The table
receives the same rows it receives today; no column holds an array and no schema changes.

### OTEL and Iceberg

```nspl,ignore
  BATCH MAX MESSAGES 1000 MAX SIZE 3MiB
```

on an OTEL emitter bounds each export request at a thousand data points and three encoded
megabytes, below a receiver's four-megabyte default, instead of exporting whatever one buffered
batch happened to hold.

```nspl,ignore
  BATCH MAX MESSAGES 250000 MAX SIZE 128MiB
  FLUSH EACH 10s MAX BATCH SIZE 1MiB
  ...
  CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB
```

on an Iceberg emitter makes each appended data file hold at most 250,000 rows and 128 MiB, while
the commit cadence still decides when the files are published. One commit appends every file it
wrote.

## Acceptance matrix

Each area below is one delivery ticket of this epic. Every area starts from a failing public
Cucumber scenario, uses one-node and three-node outlines where the behavior is not
topology-specific, and is observed through the receiver named here — the same external systems the
existing emission scenarios already provision.

| Area | Must be demonstrated | Observed through |
| --- | --- | --- |
| NSPL, Models and execution plans | The clause parses, completes, renders in `SHOW CREATE` and `DESCRIBE`, validates its limits, is required for the four database sinks, and reaches the runtime as a typed plan; `WITH MAX BATCH` no longer exists | NSPL statements and `DESCRIBE EMITTER` |
| Wire-size upper bounds and bounded encoders | Encoding stops at `MAX SIZE`, the measured size is exact for every format, subdivision halves and re-measures without assuming monotonicity, and a singleton is rejected only after a bounded encoding of it alone | Consumed payload bytes and `ON MESSAGE ERROR` records |
| Batch-aware jaq and format-native containers | Every container in the [container table](#containers-by-format), the protobuf batch message with and without a transformation, and the zero-output, multiple-output, evaluation-failure and unwritable-value failures | Kafka, and a protobuf consumer for the batch message |
| Bounded payload assembly | Packing order, batch compatibility on key, headers and ordering group, one buffered batch per batch, singleton shape, nothing for an empty batch, partial batches, and no batch cadence of its own | Kafka and SQS, with interleaved records from at least two branches |
| Membership through acknowledgements and retries | One confirmation acknowledging every member, a retained prepared payload across an ambiguous retry, a smaller retry where the contract names members, and upstream leases kept alive | Kafka with a stalled broker, and MongoDB with a poison document |
| Broker and message emitters | One batch per message on Kafka, Pulsar, RabbitMQ, Redis, MQTT, NATS, ZeroMQ and SQS, in every publishing mode, including SQS `MODE BATCH` with FIFO groups | Kafka, Pulsar, RabbitMQ, Redis, EMQX, NATS, a Nervix ZeroMQ ingestor, ElasticMQ |
| Syslog, Sentry and OTEL | The syslog frame and its compatibility rule over UDP, TCP and TLS; a Sentry envelope accepted with the batch event; bounded OTLP export requests for logs, traces and metrics over both transports | A Nervix syslog ingestor, Bugsink, Quickwit and Jaeger |
| SQL and MongoDB | Bounded inserts and bulk writes, the encoded-size bound, existing conflict policies, and record-specific isolation after a failed batch | ClickHouse, Postgres, MySQL, MongoDB |
| Iceberg staging and commit | Data files bounded by rows and written bytes, several files in one commit, and the commit cadence unchanged | An Iceberg REST catalog over object storage |
| ALTER, drain and shutdown | `SET BATCH` and `DROP BATCH` as entity-pause changes, pending output drained under the old limits, force flush and drain publishing partial batches, and branch eviction dropping a prepared payload | `ALTER EMITTER` with a live emitter, and node drain |
| Configuration, size bounds and metrics | The `batch:` line, the four payload metric families with and without the clause, and the subdivision counter rising when the two limits disagree | `DESCRIBE EMITTER` and the metrics endpoint |
| Qualification | Every sink kind and variant in the [sink matrix](#sink-matrix) against its receiver, every limit in the [limits table](#limits), every failure in [failure semantics](#failure-semantics), and no throughput regression for an emitter without the clause | All of the above, plus the repository's benchmark comparison |

## Shared-contract ownership

The epic's audit of the ten active umbrella epics assigns every overlapping edit. This document
records the boundaries that constrain the delivery tickets; none of them is renegotiated here.

| Shared contract | Owner | This epic's part |
| --- | --- | --- |
| Sink contracts, connector crate boundaries and emitter host lifecycle | The connector-crate epic | Batching behavior only; host integration follows that epic's qualification, and completed extractions are consumed rather than repeated |
| Branch identity, acknowledgement state and MongoDB value conversion | The typed-states epic | Reuse the current typed states and exact conversion errors; no sentinel, no duplicate branch identity, no null-on-conversion-failure |
| Codec, connector, validation and diagnostic error types | The typed-errors epic | Extend the existing reported semantic errors for the new failures; do not repeat that epic's conversion sweep |
| Emitter `ALTER` impact classification, gates, drain and transaction inspection | The transaction-quiesce epic | Contribute the batching clause's classification and its tests; reuse that epic's planner and report owners |
| Acknowledgement, force-flush and cancellation protocols under deterministic scheduling | The Shuttle epic | Extend the landed production-type checks for the new payload ownership; no new concurrency harness |
| Bounded execution and test-build configuration | The Turmoil epic | Reuse the bounded execution owner for encoding and batch transformations; no simulated external brokers |
| Scenario lifecycle, deadlines and diagnostics | The Cucumber-lifecycle epic | Every new scenario consumes it; no independent harness timeout |
| Arrow projection, nested values, bytes and sensitivity | The columnar VM epic | Reuse the current APIs and columns; no row-map runtime and no new scalar functions |
| Rendering and transport of emitter configuration and metrics | The Client Wire epic | Reuse the current typed command and inspection interfaces |
| Upstream acknowledgement, replay and shutdown boundaries for guests | The WASM durability epic | Batch payloads, membership and acknowledgement state stay volatile; no exactly-once promise |

Four adjacent items keep their own identities, and the delivery tickets that touch the same code
consume them rather than reimplementing them: the generic per-record ordering group in the emitter
host, the consolidation of the emitter task loop's repeated publish-outcome handling, anchoring the
Iceberg commit cadence at staging time, and staging one Iceberg file per flush.

## Protocol references

The fixed external contracts in this document were read from their primary sources:

- Syslog message format, TLS transport and TCP framing: [RFC 5424](https://www.rfc-editor.org/rfc/rfc5424),
  [RFC 5425](https://www.rfc-editor.org/rfc/rfc5425), [RFC 5426](https://www.rfc-editor.org/rfc/rfc5426),
  [RFC 6587](https://www.rfc-editor.org/rfc/rfc6587). `MSG` is free-form, so an array of complete
  messages is a valid `MSG`, and neither RFC defines a repeated-message container inside one
  message.
- Sentry envelopes and item rules: [Envelopes](https://develop.sentry.dev/sdk/data-model/envelopes/)
  and [Envelope Items](https://develop.sentry.dev/sdk/foundations/envelopes/envelope-items/), where
  an `event` item "may occur at most once per Envelope"; event size limits:
  [Size Limits](https://docs.sentry.io/concepts/data-management/size-limits/).
- OTLP export requests and partial success:
  [OTLP specification](https://opentelemetry.io/docs/specs/otlp/); the collector's default gRPC
  receive limit: [OTLP receiver](https://github.com/open-telemetry/opentelemetry-collector/tree/main/receiver/otlpreceiver).
- SQS message and batch quotas:
  [Amazon SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)
  and [SendMessageBatch](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html).
  Nervix enforces 256 KiB per message today; the service's current maximum is higher, and raising
  the value Nervix enforces is not part of this contract.
- Broker message limits: Kafka `message.max.bytes` and `max.request.size`, Pulsar `maxMessageSize`,
  RabbitMQ [`max_message_size`](https://www.rabbitmq.com/docs/configure), NATS `max_payload`, Redis
  `proto-max-bulk-len`, and the MQTT 5 `Maximum Packet Size` property.
- Database limits: PostgreSQL's 1 GB per-value limit and 32-bit protocol message length, MySQL's
  `max_allowed_packet`, ClickHouse's `JSONEachRow` input format and `max_query_size`, and
  MongoDB's 16 MiB document limit with the server-reported `maxWriteBatchSize` and
  `maxMessageSizeBytes`.
- Format containers: [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949) for definite-length CBOR
  arrays, the [Avro specification](https://avro.apache.org/docs/current/specification/) for array
  block encoding, [TOML](https://toml.io/en/v1.0.0) for arrays of tables,
  [XML](https://www.w3.org/TR/xml/) for the single-root rule, and the
  [Iceberg table specification](https://iceberg.apache.org/spec/) for the 512 MiB
  `write.target-file-size-bytes` default this document names.
