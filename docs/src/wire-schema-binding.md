# Wire Schema Binding

*Plexus, the Nervix graph definition language.*

A schemaful codec turns an external payload into rows of an internal schema and rows back into
payloads, through a wire schema that declares the payload's shape in the format's own types. This
chapter defines how that works: which layer owns each part, the vocabulary of each format and what
its types are on the wire, the rule that binds a wire type to internal types and its complete
matrix, what an `ENCODE` rule means, how arrays and their elements bind, what a wire schema's mode
does, the value model each format decodes through, the fidelity those paths keep and the bounds the
formats set, the stored shapes and canonical text a wire type reaches, how failures are reported,
the evidence that holds all of this, and what adding a wire type or a rule takes.

Three rules shape the design. A wire type names a representation its format actually has, never a
Nervix type, so a wire schema describes the payload rather than copying the internal schema. A wire
type binds exactly the internal types whose every value its representation carries and returns
unchanged: nothing binds through numeric widening, narrowing or an interchange the representation
does not hold. And every interchange that is not the representation itself, such as a datetime
written as text or bytes written as base64, is an explicit `ENCODE` rule on the codec, which
declares the interchange and selects no alternative.

[Schemas And Codecs](./schemas-and-codecs.md) owns the statements, their syntax, the public binding
table and the user-facing fidelity promises; this chapter is the architecture behind them. [Errors
And Diagnostics](./errors-and-diagnostics.md) owns how codec failures are reported, [Consensus
Storage And Replication](./consensus-storage-and-replication.md) and [Typed States And Validation
Boundaries](./typed-states.md) own the stored-shape fences, [Backup And Restore
Architecture](./backup-and-restore-architecture.md) owns the archive, [Emitters](./emitters.md) owns
batching and `MAX SIZE`, and [Property Testing And Fuzzing](./property-testing-and-fuzzing.md) owns
the Bolero register. `SYSLOG`, the JAQ-native codecs and the protobuf codec are outside the binding
rule: `SYSLOG` is a fixed field contract that [Syslog](./syslog.md) owns, and the other two are
transformations whose programs decide the payload's shape.

## Ownership By Layer

| Layer | Owner | What it owns |
| --- | --- | --- |
| Vocabulary | `nervix-models`, `schema.rs` | One wire type enum per format: `JsonType`, `CborType` and the recursive `AvroType`, whose `Array` boxes its item type. `WireSchemaStrictness`, and the wire schema Models `CreateWireSchema<T>`, `AlterWireSchema<T>` and `WireSchemaField<T>`, generic over the format's type so a field can only hold a type of its own format. |
| Vocabulary | `nervix-models`, `statement.rs` and `canonical.rs` | The codec Model `CreateCodec` with its `CodecWireFormat`, the `CodecEncodingRule` list and `CodecEncoding` (`Rfc3339`, `Base64`); `ResolvedCodecWireFormat` and `WireSchemaLookup`, which pair each format with a wire schema of its own kind; the canonical rendering of wire schemas and rules. |
| Language | `nervix-plexus`, `schema` and `codec` | One type parser per format, the wire schema statements each format instantiates from one generic parser, the required `MODE`, and the codec's `ENCODE` clause. Completion reads the same parsers, so a field position suggests exactly its own format's types. |
| Decisions | Registry validation, `src/registry/validation/wire.rs` | The binding rule: one `WireTypeBinding` implementation per format, the typed `WireBindingRefusal`, the field set and optionality checks, and the checks of every `ENCODE` rule. |
| Decisions | The activation plan, `src/registry/domain_activation_plan.rs` | `PlannedCodec`, which carries the codec's compiled schema, its rules and the wire schema its format resolved to, so the runtime never looks a wire schema up again. |
| Engines and infrastructure | The codec engine, `src/runtime_schema.rs` with `runtime_schema/cbor.rs` and `runtime_schema/batch_container.rs` | Compiled codecs; decoding a JSON payload through simd-json's borrowed values, a CBOR payload through `ciborium::Value` and an Avro payload through an Avro datum, each directly into typed Arrow builders; the CBOR and Avro encoders; batch containers; bounded encoding under `MAX SIZE`. It knows no relay, branch, schedule or registry. |
| Engines and infrastructure | `nervix-columnar-json` | Writing JSON objects straight from typed Arrow columns, which is the `WIRE JSON` encoder. It names no codec or Model. |
| Data plane | Ingest groups and the emitter host | Decoding each payload of a batch into one set of builders, turning a refused payload into a decode rejection, and encoding each record or batch an emitter publishes. |
| Edges | The web console | The structured editors for the three wire formats and the codec form, which build Models and preview their canonical text. |
| Edges | The session protocol | The three wire schema kinds among the configured choices a client lists. It carries no wire type. |
| Edges | Backup and restore | Wire schemas and codecs as canonical Plexus text in an archive. |

Each stage converts once. The language lowers text into the generic Models, the registry validates a
codec against the wire schema its format names and resolves that wire schema into the plan, and the
runtime compiles the plan's codec. A compiled codec holds what its format needs (field keys
resolved for simd-json, the parsed Avro record schema, the CBOR field types) and never reads a Model
again.

## The Vocabularies

Each format has its own enum, its own type parser and its own canonical spelling, and a wire
schema's format is part of its identity: `WIRE JSON SCHEMA`, `WIRE CBOR SCHEMA` and `WIRE AVRO
SCHEMA` are separate Model kinds whose names may coincide.

| Format | Wire type | What it is on the wire |
| --- | --- | --- |
| JSON | `string` | A JSON string. |
| JSON | `number` | Any JSON number. |
| JSON | `integer` | A JSON number that reads as a 64-bit signed or unsigned integer. |
| JSON | `boolean` | `true` or `false`. |
| JSON | `array` | A JSON array, whose items JSON does not type. |
| CBOR | `integer` | An unsigned or negative integer, major types 0 and 1. |
| CBOR | `float` | A half, single or double precision float. |
| CBOR | `boolean` | The simple values `true` and `false`. |
| CBOR | `text` | A UTF-8 text string, major type 3. |
| CBOR | `bytes` | A byte string, major type 2. |
| CBOR | `array` | An array, major type 4, whose items CBOR does not type. |
| AVRO | `boolean`, `int`, `long`, `float`, `double`, `string`, `bytes` | The Avro primitive type of that name. |
| AVRO | `array<item>` | An Avro array whose `items` schema is `item`, any AVRO type, arrays included. |

A JSON or CBOR payload is one object or map whose keys are the field names. An AVRO payload is one
raw Avro datum of the record schema the wire schema declares: a record named after the wire schema,
its fields in declaration order, each with the type the field names, and an optional field as the
union of `null` and its type with a `null` default. The `items` schema of an array is the declared
item type at every depth; nothing in it comes from the internal schema.

Wire type keywords are case-insensitive tokens of the shared lexer. The canonical renderer writes
them in uppercase, an AVRO array with its item type at every depth, `ARRAY<ARRAY<DOUBLE>>`, and
rendered text parses back to the same Model.

## The Binding Rule

A codec binds the fields of its wire schema to the fields of its internal schema by name. Both
schemas hold exactly the same field names, and each wire field's `OPTIONAL` equals its internal
field's. A wire field binds its internal field when every value of the internal type travels in the
wire type's representation and decodes back to itself. `ty.innermost_element()` and `list_depth()`
of `ParseAsType`, and `innermost()` and `array_depth()` of `AvroType`, are the measures the rule
reads for lists.

| Internal type | JSON | CBOR | AVRO |
| --- | --- | --- | --- |
| `U8`, `I8`, `U16`, `I16`, `U32`, `U64` | `integer` | `integer` | none |
| `I32` | `integer` | `integer` | `int` |
| `I64` | `integer` | `integer` | `long` |
| `F32` | `number` | `float` | `float` |
| `F64` | `number` | `float` | `double` |
| `BOOL` | `boolean` | `boolean` | `boolean` |
| `STRING` | `string` | `text` | `string` |
| `DATETIME` | `string` under `RFC3339` | `text` under `RFC3339` | `string` under `RFC3339` |
| `BYTES` | `string` under `BASE64` | `bytes`, no rule | `bytes`, no rule; `string` under `BASE64` |
| `ARRAY` and `VEC`, any nesting | `array` | `array` | `array<item>` nested exactly as deep, with `item` binding the innermost element by this table |

JSON and CBOR `integer` bind every integer width, and the decoder checks each value against the
internal type's range. AVRO binds no narrow or unsigned integer: `int` is exactly `I32` and `long`
exactly `I64`, because binding a narrower type would be the numeric widening the exact-type
invariant forbids, and the decoder refuses those internal types even if a Model reached it. Every
row of the table is the only representation of its internal type in its format, except that AVRO
carries `BYTES` either natively or as base64 text, so for every format but AVRO the internal type
alone identifies the pair.

Validation reports a refusal as `RegistryError::IncompatibleSchema`, naming the codec and the
field, with one of four typed reasons:

- `Type`: the wire type holds another kind of value than the internal type.
- `Depth`: an AVRO array nests to another depth than the internal `ARRAY` and `VEC` levels.
- `EncodingRequired`: the innermost internal values travel as text only under a rule the codec does
  not declare for the field.
- `EncodingRefused`: the codec declares a text rule for bytes the wire type carries natively.

A missing field, unequal optionality and unequal field sets are refusals of the same error. A
codec's format looks its wire schema up through `WireSchemaLookup`, whose one method per format
returns only a wire schema of that format's kind, so no codec is ever validated or compiled against
a definition of another kind; a reference the kind does not hold is `MissingReference`.

## Encoding Rules

`ENCODE <field> AS RFC3339|BASE64`, with further rules after commas, follows a codec's `TO SCHEMA`
clause. Its rules name internal schema fields, and validation refuses, as the codec's
`RegistryError::InvalidModel`, a rule naming no field of the schema, a second rule for one field,
and a rule whose field's innermost element is not the rule's type: `DATETIME` for `RFC3339`,
`BYTES` for `BASE64`. A `SYSLOG` codec takes no rule.

A rule declares the interchange the representation already fixes. For every pair the table binds
there is exactly one representation, and validation requires the rule exactly where that
representation is text and refuses it where the format carries the value natively. The engine
therefore never selects behavior from a rule: it reads the validated pair of wire type and internal
type, and the decoder and encoder of a format never read or write a Nervix type name in a payload.

- `RFC3339` reads any RFC 3339 offset into the same instant and keeps nanoseconds. A value chrono
  cannot parse or represent as a nanosecond timestamp is a decode error. The encoders write UTC
  with a `+00:00` offset and the fractional digits the instant needs.
- `BASE64` is RFC 4648's standard alphabet with padding. The decoder validates the complete text
  before it decodes, so malformed, unpadded or noncanonical text is a decode error, and streams
  validated chunks into the binary builder without an intermediate copy of the value.
- A rule on a list field governs the innermost scalar elements at every depth, and only them.

A JAQ-native or protobuf codec is a transformation: its programs read and write the row's JSON
view, in which a `DATETIME` is always RFC 3339 text and `BYTES` always padded base64 text. The
grammar accepts `ENCODE` rules on these codecs and validation checks them against their fields as
above, but no rule changes that view.

## Arrays And Element Binding

JSON and CBOR do not type the items of an array, so their wire type is the bare `array`, which binds
an `ARRAY` or `VEC` of any nesting. Each element binds the way a scalar field of the innermost
element type would bind in that format: a `DATETIME` element needs the field's `RFC3339` rule in
both formats, and a `BYTES` element needs `BASE64` in JSON and travels as a native byte string,
under no rule, in CBOR.

An Avro array schema always names its items, so an AVRO array is `array<item>`, nested as deep as
the internal list. The item is any AVRO type, an array included, and is always declared. The
innermost item binds the innermost element exactly as an AVRO field of that type would, and the
encoder writes each element as the declared item, which is the `items` schema the codec's record
declares.

Both paths keep the internal shape. `ARRAY<T, n>` decodes only from an array of exactly `n`
elements and encodes only a column whose fixed length is `n`; `VEC<T>` holds any length. No list
element is null in any format: the decoder refuses a null element as a value of the wrong kind and
the encoders refuse a null element. A failure part-way through a list closes the partial list
before the row is abandoned, so its elements never reach another row.

## Mode And Optional Fields

Every declared wire schema states `MODE STRICT` or `MODE LOOSE`; there is no default.

- `WIRE JSON` parses the whole payload as one JSON value, which must be an object; simd-json
  refuses content after it. `STRICT` refuses an object key the wire schema does not declare, and
  `LOOSE` ignores it.
- `WIRE CBOR` decodes one data item from the start of the payload, which must be a map whose keys
  are distinct text strings. `STRICT` refuses an undeclared key, and `LOOSE` ignores it after
  validating its value as every value is validated: no tag at any depth, and no non-text or
  repeated key in a nested map. The decoder does not read past that item.
- `WIRE AVRO` reads the datum field by field as the declared record schema dictates, and checks
  that each position holds the field it declares. A datum carries no field names, so the mode does
  not change how an AVRO payload decodes, and the decoder does not read past the datum.

An absent key, a JSON or CBOR `null`, or the `null` branch of an AVRO union decodes to a typed null
of an optional field and is refused for a required one. The JSON and CBOR encoders omit an optional
field holding null, the AVRO encoder writes the union's `null` branch, and every encoder refuses a
required field holding null.

## Value Models

**JSON.** An ingest group owns one `JsonDecoder`: each payload is copied into scratch the group
retains, and simd-json parses it with parser buffers retained beside that scratch. The decoder walks
simd-json's borrowed values directly into the batch's typed Arrow builders. Field keys are resolved
for simd-json when the codec compiles, each value's JSON kind is checked against its wire type, and
the internal type then decides the builder: integers through a checked conversion to the internal
width, datetimes parsed once, base64 streamed into the binary builder. No serde tree, row map or
second representation of a field is built. On emission, `nervix-columnar-json` writes each object in
schema field order from the batch's columns into the caller's output buffer, with keys quoted once
when the codec compiles and a runtime-selected SIMD classifier marking the bytes that need escaping
once per batch for each string column.

**CBOR.** The payload is parsed into one `ciborium::Value` tree and walked into the typed builders.
Ciborium turns the bignum tags 2 and 3 into plain integers before a `Value` could keep the tag, so
when a payload holds a header byte that could start one, a header scan with `ciborium-ll` runs
first and refuses the tagged field; every other tag survives in the `Value` and is refused there.
Encoding serializes the row through ciborium from the typed columns: `BYTES` as a byte string, a
`DATETIME` as text, lists as definite-length arrays.

**AVRO.** The codec compiles its declared record schema once with `apache-avro`. A payload decodes
with `from_avro_datum` into an Avro record value whose fields the decoder takes by declared
position; encoding builds the record value from the typed columns and writes it with
`to_avro_datum`.

**Batches and bounds.** A batching emitter's member value is exactly the payload the single-record
path writes, and the three formats frame members in their own containers: a JSON array, a
definite-length CBOR array, and one Avro array block of the record schema
([Batch Containers](./schemas-and-codecs.md#batch-containers)). Each container's syntax comes from
its format's maintained encoder, around members written exactly as their records encoded them and
never parsed again: `serde_json`'s compact formatter writes the JSON array's brackets and commas,
`ciborium-ll` the CBOR array head, and `apache-avro` the Avro block count and the zero count that
ends the array. JSON and CBOR stream into a bounded writer that abandons the encoding at the first
write past `MAX SIZE`; AVRO builds the datum and the bounded writer decides whether it fits. Either
way an encoding that does not fit is the typed `Oversize` outcome naming the codec, its encoding and
the limit, never a truncated payload.

## Value Fidelity

A schemaful codec decodes every row it encodes back to the same values: every integer width to its
extremes, booleans, any Unicode text, bytes, datetimes to the nanosecond, nested `ARRAY` and `VEC`
values with their exact shapes, and nulls of optional fields. The formats set the bounds of that
promise, which [Value Fidelity](./schemas-and-codecs.md#value-fidelity) states for users:

- JSON has no number for a NaN or an infinity, so `WIRE JSON` writes a non-finite float as `null`,
  which reads back as a null of an optional field and is refused in a required field or a list
  element. Finite floats are written as the shortest decimal that reads back to their bits.
- A JSON number read into an `F32` names the nearest `F32`; where the 64-bit value a reader parses
  lies exactly halfway between two `F32` values, it names the one whose shortest decimal reads as
  that value, so every `F32` the codec writes reads back to its bits.
- `WIRE CBOR` carries native floats, non-finite values included. Ciborium widens an `F32` before
  writing it, which preserves every `F32` value but turns a signaling NaN into a quiet NaN with the
  same sign and payload. A CBOR float decoded into an `F32` field narrows to the nearest `F32`.
- `WIRE AVRO` keeps every float bit pattern, and each array element travels as exactly the item type
  that binds its one internal element type.
- A datetime travels as RFC 3339 text in UTC, so text written with another offset reads as the same
  instant and is written back in UTC.

## Stored Shapes And Canonical Text

Wire schemas and codecs are Models, so the registry and consensus store them as rkyv archives.
`MODEL_ARCHIVE_HEADER` in the registry's storage identifies the persisted Model shape before an
archive is decoded, and consensus's keyspace namespace identifies the Model shape its state and log
records hold. A change to a wire type enum, `CodecEncoding`, `CodecWireFormat` or the wire schema
Models changes the archived layout, so it changes both fences in the same change. A store written
in another shape then fails to load with `RegistryError::InvalidModelArchive` or as invalid
consensus record storage and is recreated; nothing reinterprets the discriminants of an earlier
shape ([Typed States And Validation Boundaries](./typed-states.md),
[Consensus Storage And Replication](./consensus-storage-and-replication.md)). The same change
advances the domain version of every registered Bolero target whose generated values hold the
changed type.

A backup archive carries Models as canonical Plexus text, not as stored archives: wire schemas in
each format's current vocabulary and codecs with their explicit `RFC3339` and `BASE64` rules. A
restore parses that text through the current language and validates it through the current
registry, so a vocabulary change needs no archive change, and text that cannot form a current Model
fails restore preparation ([Records Mirror The
Contract](./backup-and-restore-architecture.md#records-mirror-the-contract)).

## Failure Semantics

**At validation.** A codec whose wire schema cannot carry its internal schema is refused with
`RegistryError::IncompatibleSchema`, and a codec whose rules are wrong with
`RegistryError::InvalidModel`, each naming the codec and the field. The refusal fails the whole
candidate graph of the transaction, so no partial definition is committed.

**At compilation.** A codec that cannot be compiled while a domain's execution is built keeps its
`CodecError` report beneath `RuntimeError::BuildDomainExecution`, which names the domain.

**At decoding.** A payload decodes completely or not at all. `CodecError` contexts name the codec,
and the field when one is at fault: the format's parse failure (`JsonDecode`, `CborDecode`,
`AvroDecode`), `ExpectedObject`, `UnexpectedField`, `MissingField` and `ParseField`. Beneath them
stay the typed causes: a `FieldDecodeError` for a required null, a value whose kind differs from its
JSON or CBOR wire type, a CBOR tag, a non-text CBOR key and a repeated CBOR key, and a
`RuntimeSchemaError` for a value that does not fit its internal type, an integer outside its range
included, with the value's kind and its location down to the list element, and the length, base64
or datetime cause where one applies. The row a failed payload
started is closed and dropped, so the batch is left exactly as it was. The host turns the refusal
into a decode rejection under the ingestor's general error policy, and no diagnostic carries a
payload value ([Decode Rejections](./errors-and-diagnostics.md#decode-rejections)).

**At encoding.** `CodecError::EncodeField` names the codec and the field, with a `FieldEncodeError`
beneath it for a required null, a null list element or a value the declared AVRO type cannot carry,
or the writer's own failure beneath `ColumnarJsonEncode`, `CborEncode` or `AvroEncode`. The emitter
keeps the report beneath the record it rejects or the encoding it could not start
([Codecs And Ingress](./errors-and-diagnostics.md#codecs-and-ingress)).

## Evidence

| Evidence | What it holds |
| --- | --- |
| Parser and completion tests in `crates/plexus/src/schema` and `crates/plexus/src/codec` | Every wire type and rule parses, a word outside a format's vocabulary is refused, and a field position suggests only its own format's types; `just plexus-completion-walk` completes every branch. |
| Validation tests in `src/registry/validation/wire.rs` | The complete binding matrix for every format, AVRO depths, list elements under each rule, and the rule checks. |
| `tests/features/runtime/wire_schema_lists.feature` | AVRO arrays carry exactly their declared items, JSON and CBOR list elements bind through the codec's rules, and a list field no wire type carries is refused, on one and three nodes. |
| `tests/features/runtime/cbor_wire_schema.feature` | A CBOR byte string crosses the graph without a text interchange. |
| `tests/features/runtime/bytes_functions.feature` | `BASE64` ingestion and emission through JSON and AVRO strings. |
| `tests/features/runtime/schema_type_strictness.feature` | A datetime in a JSON string requires `RFC3339`. |
| `tests/features/runtime/emitter_batch_containers.feature` | Each format's batch container and its read back. |
| `tests/features/web-console/visual_create_schema.feature` and `visual_create_codec.feature` | Each format's editor and the codec form's rules, through the canonical command. |
| Bolero `codec-schemaful-rows` | Every row a generated codec of an accepted shape encodes decodes back to the exact schema and values, whatever order the wire schema lists its fields in. |
| Bolero `codec-schemaful-payload-groups` | Damaged payloads in a group are accepted or refused exactly as alone, with a typed failure of their own bytes. |
| Bolero `codec-json-non-finite-floats`, `codec-bounded-encodings`, `emitter-batch-packing`, `columnar-json-rows` | The JSON projection of non-finite floats, exact bounded encodings and containers, packed batches, and the JSON writer against an independent rendering. |
| Bolero Model and stored-shape targets | `plexus-model`, `plexus-statement`, `plexus-statement-text`, `plexus-archive-model`, `plexus-format-document`, `plexus-format-text`, `models-archived-models`, `registry-archived-models`, `registry-stored-models` and its corruption target, `consensus-state-records` and its corruption target, `consensus-raft-records` and its malformed target, and `backup-complete-archives` round trip the Models that hold wire types. |

The generators live in `crates/arbitrary/src/schema.rs`, which draws every current wire type of
each format, AVRO arrays to a bounded depth, and in `src/runtime_schema/codec_properties.rs`, which
builds codecs of the shapes the registry accepts. The codec paths are pure per-payload functions
with no synchronization, sockets or tracked locks, so Shuttle, Loom, Turmoil and Deloxide evidence
does not apply to them.

## Adding A Wire Type Or An Encoding Rule

1. Name a representation the format has, and at least one internal type whose every value it
   carries exactly. A type no internal type binds is not declared.
2. Add the variant to the format's enum in `crates/models/src/schema.rs`, or to `CodecEncoding`, and
   to the canonical renderer; change `MODEL_ARCHIVE_HEADER` and the consensus keyspace namespace in
   the same change, and advance the domain version of every Bolero target whose values hold it,
   with the register and its pins.
3. Add it to that format's type parser only, as a case-insensitive token of the shared lexer, with
   positive, negative and completion-context tests beside the grammar and a passing
   `just plexus-completion-walk`.
4. Bind it in the format's `WireTypeBinding` implementation: exact types only, text that needs its
   rule, native values that refuse one. Extend the validation matrix tests.
5. Decode and encode it in the format's engine path from the validated pair of wire type and
   internal type, with typed failures and no Nervix type name in a payload.
6. Generate it in `crates/arbitrary`, so the codec and Model properties draw it, and add a public
   scenario on one and three nodes that carries it through the graph and refuses what it cannot
   bind.
7. Update [Schemas And Codecs](./schemas-and-codecs.md), this chapter's tables, the web console's
   editors and the Plexus skill, and delete whatever the new type or rule replaces in the same
   change.
