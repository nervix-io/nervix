//! Generated schemaful codecs and rows through their encoders and decoders.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated `WIRE JSON`, `WIRE CBOR` and `WIRE AVRO` codecs of the shapes the registry
//!   accepts, the round trip of every row of a batch through one of them, and the property that a
//!   group of payloads decodes to exactly what each payload decodes to alone.
//! - **Depends on.** The generated batches and their logical oracle, the codec Models and the
//!   compiled codecs' row encoder and payload decoder.
//! - **Must not know.** Ingestors, emitters, connectors or the ingest group that owns a builder.
//!
//! The JAQ-native, protobuf and syslog codecs are not lossless and stay outside these properties:
//! their contracts are transformations and fixed field projections, which their own tests state.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{
    AvroType, CodecEncoding, CodecEncodingRule, CodecName, CodecWireFormat, CreateAvroWireSchema,
    CreateCodec, CreateJsonWireSchema, JsonType, ParseAsType, ResolvedCodecWireFormat,
    WireSchemaField, WireSchemaName, WireSchemaStrictness,
};
use nervix_primitives::sync::Arc;
use serde_json::Value as JsonValue;

use super::{CodecError, CompiledCodec, JsonDecoder, compile_codec, decode_with_codec};
use crate::runtime_schema::{
    RuntimeRecordBatch,
    generated_batches::{
        Damage, GeneratedDomain, GeneratedSchema, LogicalValue, Place, assert_same_batch,
    },
};

/// The bytes one case reads its codec, rows and damage from.
const CASE_BYTES: usize = 4096;

/// The most damaged payloads one group interleaves with the valid ones.
const DAMAGED_PAYLOADS: usize = 4;

/// A schemaful wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemafulFormat {
    Json,
    Cbor,
    Avro,
}

impl SchemafulFormat {
    const ALL: [Self; 3] = [Self::Json, Self::Cbor, Self::Avro];

    /// The values this format's codecs promise to preserve.
    fn domain(self) -> GeneratedDomain {
        match self {
            Self::Json | Self::Cbor => GeneratedDomain::Json,
            Self::Avro => GeneratedDomain::Avro,
        }
    }
}

/// The wire schema a generated codec reads from and writes to.
#[derive(Debug, Clone)]
pub(crate) enum GeneratedWireSchema {
    Json(CreateJsonWireSchema),
    Cbor(CreateJsonWireSchema),
    Avro(CreateAvroWireSchema),
}

impl GeneratedWireSchema {
    /// The wire format a codec resolves when it names this wire schema.
    pub(crate) fn resolved(&self) -> ResolvedCodecWireFormat<'_> {
        match self {
            Self::Json(schema) => ResolvedCodecWireFormat::Json(schema),
            Self::Cbor(schema) => ResolvedCodecWireFormat::Cbor(schema),
            Self::Avro(schema) => ResolvedCodecWireFormat::Avro(schema),
        }
    }
}

/// A schemaful codec of a generated schema, of the shape the registry accepts, and the codec
/// compiled from it.
#[derive(Debug, Clone)]
pub(crate) struct CodecCase {
    pub(crate) schema: GeneratedSchema,
    pub(crate) format: SchemafulFormat,
    pub(crate) wire: GeneratedWireSchema,
    pub(crate) model: CreateCodec,
    pub(crate) codec: Arc<CompiledCodec>,
}

impl CodecCase {
    pub(crate) fn new(arbitrary: &mut Arbitrary<'_>) -> Self {
        let format = arbitrary.entropy().pick(SchemafulFormat::ALL);
        let schema = format.domain().schema(arbitrary);
        Self::with_schema(arbitrary, format, schema)
    }

    /// A codec of `format` for `schema`, its wire schema drawn from `arbitrary`.
    fn with_schema(
        arbitrary: &mut Arbitrary<'_>,
        format: SchemafulFormat,
        schema: GeneratedSchema,
    ) -> Self {
        let strictness = arbitrary
            .entropy()
            .pick([WireSchemaStrictness::Strict, WireSchemaStrictness::Loose]);
        let wire_name =
            WireSchemaName::parse("generated_wire").assured("the wire schema name is a literal");
        // A wire schema lists the fields in any order; Avro writes them in this order, and every
        // codec still decodes into the internal schema's column order.
        let mut order: Vec<usize> = (0..schema.model.fields.len()).collect();
        for position in (1..order.len()).rev() {
            let other = arbitrary.entropy().count(position);
            order.swap(position, other);
        }
        let mut encoding_rules = Vec::new();
        let wire = match format {
            SchemafulFormat::Json | SchemafulFormat::Cbor => {
                let mut fields = Vec::with_capacity(order.len());
                for index in &order {
                    let field = &schema.model.fields[*index];
                    let ty = json_wire_type(arbitrary.entropy(), &field.ty);
                    if field.ty == ParseAsType::Datetime && ty == JsonType::String {
                        encoding_rules.push(CodecEncodingRule {
                            field: field.name.clone(),
                            encoding: CodecEncoding::Rfc3339,
                        });
                    }
                    fields.push(WireSchemaField {
                        name: field.name.clone(),
                        ty,
                        optional: field.optional,
                    });
                }
                let definition = CreateJsonWireSchema {
                    name: wire_name.clone(),
                    strictness,
                    fields,
                };
                match format {
                    SchemafulFormat::Json => GeneratedWireSchema::Json(definition),
                    _ => GeneratedWireSchema::Cbor(definition),
                }
            }
            SchemafulFormat::Avro => {
                let mut fields = Vec::with_capacity(order.len());
                for index in &order {
                    let field = &schema.model.fields[*index];
                    if field.ty == ParseAsType::Datetime {
                        encoding_rules.push(CodecEncodingRule {
                            field: field.name.clone(),
                            encoding: CodecEncoding::Rfc3339,
                        });
                    }
                    fields.push(WireSchemaField {
                        name: field.name.clone(),
                        ty: avro_wire_type(&field.ty),
                        optional: field.optional,
                    });
                }
                GeneratedWireSchema::Avro(CreateAvroWireSchema {
                    name: wire_name.clone(),
                    strictness,
                    fields,
                })
            }
        };
        let wire_format = match format {
            SchemafulFormat::Json => CodecWireFormat::Json {
                wire_schema: wire_name,
            },
            SchemafulFormat::Cbor => CodecWireFormat::Cbor {
                wire_schema: wire_name,
            },
            SchemafulFormat::Avro => CodecWireFormat::Avro {
                wire_schema: wire_name,
            },
        };
        let model = CreateCodec {
            name: CodecName::parse("generated_codec").assured("the codec name is a literal"),
            wire_format,
            schema: schema.model.name.clone(),
            encoding_rules,
        };
        let codec = compile_codec(&model, schema.compiled.clone(), wire.resolved())
            .assured("a generated codec of an accepted shape compiles");
        Self {
            schema,
            format,
            wire,
            model,
            codec,
        }
    }

    /// The payload the codec writes for every row of `batch`, in order.
    fn encode(&self, batch: &RuntimeRecordBatch) -> Vec<Vec<u8>> {
        let encoder = self
            .codec
            .batch_encoder(batch)
            .assured("a batch of the codec's schema opens an encoder");
        let mut payloads = Vec::with_capacity(batch.batch().num_rows());
        for row in 0..batch.batch().num_rows() {
            let mut payload = encoder.next_payload();
            encoder
                .encode_row_into(row, &mut payload)
                .assured("every value of the codec's domain encodes");
            payloads.push(payload);
        }
        payloads
    }

    /// Decodes `payloads` in order into one batch, as one ingest group decodes them, answering for
    /// each payload whether it decoded into its one message.
    fn decode_group(&self, payloads: &[Vec<u8>]) -> GroupDecoding {
        let mut builder = self.codec.schema().batch_builder(payloads.len());
        let mut decoder = JsonDecoder::default();
        let mut accepted = Vec::with_capacity(payloads.len());
        for payload in payloads {
            match decode_with_codec(&self.codec, payload, &mut decoder, &mut builder) {
                Ok(messages) => {
                    assert_eq!(messages, 1, "a schemaful payload decodes into one message");
                    accepted.push(true);
                }
                Err(refused) => {
                    assert_typed_decode_failure(&refused);
                    accepted.push(false);
                }
            }
        }
        let batch = builder
            .finish()
            .assured("the builder holds whole rows after every payload");
        GroupDecoding { accepted, batch }
    }
}

/// The payloads one group decoded and the batch it decoded them into.
struct GroupDecoding {
    accepted: Vec<bool>,
    batch: RuntimeRecordBatch,
}

/// A failure a decoder reports for the payload it was handed, not one of the codec's own contract.
fn assert_typed_decode_failure(refused: &error_stack::Report<CodecError>) {
    assert!(
        matches!(
            refused.current_context(),
            CodecError::JsonDecode { .. }
                | CodecError::CborDecode { .. }
                | CodecError::AvroDecode { .. }
                | CodecError::ExpectedObject { .. }
                | CodecError::MissingField { .. }
                | CodecError::UnexpectedField { .. }
                | CodecError::ParseField { .. }
        ),
        "a payload is refused with the defect of its own bytes: {refused:?}"
    );
}

/// A JSON or CBOR wire type the registry binds to an internal field of type `ty`: the generic JSON
/// type of its kind, or the exact wire type of the same name.
fn json_wire_type(entropy: &mut Entropy<'_>, ty: &ParseAsType) -> JsonType {
    let exact = entropy.flag();
    match ty {
        ParseAsType::U8 if exact => JsonType::U8,
        ParseAsType::I8 if exact => JsonType::I8,
        ParseAsType::U16 if exact => JsonType::U16,
        ParseAsType::I16 if exact => JsonType::I16,
        ParseAsType::U32 if exact => JsonType::U32,
        ParseAsType::I32 if exact => JsonType::I32,
        ParseAsType::U64 if exact => JsonType::U64,
        ParseAsType::I64 if exact => JsonType::I64,
        ParseAsType::U8
        | ParseAsType::I8
        | ParseAsType::U16
        | ParseAsType::I16
        | ParseAsType::U32
        | ParseAsType::I32
        | ParseAsType::U64
        | ParseAsType::I64 => JsonType::Integer,
        ParseAsType::F32 if exact => JsonType::F32,
        ParseAsType::F64 if exact => JsonType::F64,
        ParseAsType::F32 | ParseAsType::F64 => JsonType::Number,
        ParseAsType::Datetime if exact => JsonType::Datetime,
        ParseAsType::Datetime | ParseAsType::String => JsonType::String,
        ParseAsType::Bool => JsonType::Boolean,
        ParseAsType::Bytes => JsonType::Bytes,
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => JsonType::Array,
    }
}

/// The Avro wire type the registry binds to an internal field of type `ty`, which the Avro domain
/// generates only at the top level.
fn avro_wire_type(ty: &ParseAsType) -> AvroType {
    match ty {
        ParseAsType::Bool => AvroType::Boolean,
        ParseAsType::I32 => AvroType::Int,
        ParseAsType::I64 => AvroType::Long,
        ParseAsType::F32 => AvroType::Float,
        ParseAsType::F64 => AvroType::Double,
        ParseAsType::String | ParseAsType::Datetime => AvroType::String,
        ParseAsType::Bytes => AvroType::Bytes,
        ParseAsType::Array { .. } | ParseAsType::Vec { .. } => AvroType::Array,
        ParseAsType::U8
        | ParseAsType::I8
        | ParseAsType::U16
        | ParseAsType::I16
        | ParseAsType::U32
        | ParseAsType::U64 => {
            unreachable!("the Avro domain declares no narrow or unsigned top-level integer")
        }
    }
}

/// Every row a schemaful codec encodes decodes back into exactly that row: the payloads of a whole
/// batch, decoded in order into one builder as an ingest group decodes them, form a batch of the
/// codec's schema holding every value and null of the original, floats by their bits, datetimes to
/// the nanosecond, bytes, Unicode text and nested lists, whatever order the wire schema lists the
/// fields in.
#[test]
fn bolero_schemaful_codecs_decode_every_row_they_encode() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let case = CodecCase::new(&mut arbitrary);
            let rows = case.format.domain().batch(&mut arbitrary, &case.schema);
            let batch = case.schema.runtime_batch(rows.clone());
            let payloads = case.encode(&batch);
            let decoded = case.decode_group(&payloads);
            assert!(
                decoded.accepted.iter().all(|accepted| *accepted),
                "every encoded row decodes"
            );
            assert_same_batch(decoded.batch.batch(), &rows);
        });
}

/// How one damaged payload of a group differs from a payload the codec wrote.
#[derive(Debug, Clone, Copy)]
enum PayloadDamage {
    /// The bytes are damaged the way any encoding can be.
    Bytes,
    /// One value inside a JSON or CBOR payload holds a value of another kind or out of range, so
    /// the payload still parses and fails, if at all, while its row is being appended.
    Value,
    /// A JSON or CBOR payload loses one key or gains one the wire schema does not declare.
    Key,
}

impl PayloadDamage {
    const ALL: [Self; 3] = [Self::Bytes, Self::Value, Self::Key];
}

/// One damaged payload a group holds beside the codec's own: which of them it is damaged from,
/// how, and where it stands in the group. It is drawn whole before the case, because an ordinary
/// run's few bytes run out while a case is generated and a choice read after that takes its first
/// option, which would leave nearly every group without a damaged payload.
struct DamagedMember {
    source: Place,
    damage: PayloadDamage,
    bytes: Damage,
    leaf: Place,
    replacement: JsonValue,
    remove_key: bool,
    key: Place,
    position: Place,
}

impl DamagedMember {
    fn draw(entropy: &mut Entropy<'_>) -> Self {
        Self {
            source: Place::draw(entropy),
            damage: entropy.pick(PayloadDamage::ALL),
            bytes: Damage::draw(entropy),
            leaf: Place::draw(entropy),
            replacement: replacement_value(entropy),
            remove_key: entropy.flag(),
            key: Place::draw(entropy),
            position: Place::draw(entropy),
        }
    }

    /// `payload`, a payload the codec wrote in `format`, with this member's damage.
    fn apply(&self, format: SchemafulFormat, payload: Vec<u8>) -> Vec<u8> {
        if format == SchemafulFormat::Avro || matches!(self.damage, PayloadDamage::Bytes) {
            return self.bytes.apply(payload);
        }
        let Some(mut value) = read_json_model(format, &payload) else {
            return payload;
        };
        match self.damage {
            PayloadDamage::Bytes => {}
            PayloadDamage::Value => {
                let leaves = count_leaves(&value);
                if leaves > 0 {
                    let target = self.leaf.among(leaves);
                    replace_leaf(&mut value, target, &mut 0, self.replacement.clone());
                }
            }
            PayloadDamage::Key => {
                if let JsonValue::Object(members) = &mut value {
                    let keys: Vec<String> = members.keys().cloned().collect();
                    if self.remove_key && !keys.is_empty() {
                        let key = &keys[self.key.among(keys.len())];
                        members.remove(key);
                    } else {
                        members.insert("undeclared field".to_string(), JsonValue::Bool(true));
                    }
                }
            }
        }
        write_json_model(format, &value)
    }
}

/// A JSON or CBOR payload as the JSON model both formats are read through.
fn read_json_model(format: SchemafulFormat, payload: &[u8]) -> Option<JsonValue> {
    match format {
        SchemafulFormat::Json => serde_json::from_slice(payload).ok(),
        SchemafulFormat::Cbor => ciborium::from_reader(payload).ok(),
        SchemafulFormat::Avro => None,
    }
}

fn write_json_model(format: SchemafulFormat, value: &JsonValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    match format {
        SchemafulFormat::Json => {
            serde_json::to_writer(&mut bytes, value).assured("a JSON value writes into memory");
        }
        SchemafulFormat::Cbor => {
            ciborium::into_writer(value, &mut bytes).assured("a JSON value writes as CBOR");
        }
        SchemafulFormat::Avro => unreachable!("an Avro payload is damaged as bytes"),
    }
    bytes
}

/// How many scalar values `value` holds at any depth.
fn count_leaves(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(items) => items.iter().map(count_leaves).sum(),
        JsonValue::Object(members) => members.values().map(count_leaves).sum(),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => 1,
    }
}

/// Replaces the `target`th scalar of `value`, counting depth first from `seen`.
fn replace_leaf(value: &mut JsonValue, target: usize, seen: &mut usize, replacement: JsonValue) {
    let mut replacement = Some(replacement);
    replace_leaf_with(value, target, seen, &mut replacement);
}

fn replace_leaf_with(
    value: &mut JsonValue,
    target: usize,
    seen: &mut usize,
    replacement: &mut Option<JsonValue>,
) {
    match value {
        JsonValue::Array(items) => {
            for item in items {
                replace_leaf_with(item, target, seen, replacement);
            }
        }
        JsonValue::Object(members) => {
            for member in members.values_mut() {
                replace_leaf_with(member, target, seen, replacement);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {
            if *seen == target
                && let Some(next) = replacement.take()
            {
                *value = next;
            }
            *seen = seen
                .checked_add(1)
                .verified("a bounded payload has few leaves");
        }
    }
}

/// A value of another kind than most leaves hold, or an integer beyond every narrow type.
fn replacement_value(entropy: &mut Entropy<'_>) -> JsonValue {
    let choice = entropy.byte() % 8;
    match choice {
        0 => JsonValue::from(300_u64),
        1 => JsonValue::from(-1_i64),
        2 => JsonValue::from(u64::MAX),
        3 => JsonValue::from(i64::MIN),
        4 => JsonValue::String("not a value of this type".to_string()),
        5 => JsonValue::Bool(true),
        6 => JsonValue::Null,
        _ => JsonValue::from(1.5_f64),
    }
}

/// Payloads that fail to decode leave the group's batch exactly as it was: whatever damaged
/// payloads are interleaved with a codec's own payloads, each payload is accepted or refused in a
/// group exactly as it is alone, with a typed failure of its own bytes, and the group's batch holds
/// exactly the rows the accepted payloads decode to alone, in order. Every payload the codec wrote
/// is accepted and decodes to the row it was written from.
#[test]
fn bolero_damaged_payloads_leave_a_group_as_its_payloads_decode_alone() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let damaged_count = arbitrary.entropy().count(DAMAGED_PAYLOADS);
            let mut damaged_members = Vec::with_capacity(damaged_count);
            for _ in 0..damaged_count {
                damaged_members.push(DamagedMember::draw(arbitrary.entropy()));
            }
            let case = CodecCase::new(&mut arbitrary);
            let rows = case.format.domain().batch(&mut arbitrary, &case.schema);
            let batch = case.schema.runtime_batch(rows.clone());
            let valid = case.encode(&batch);
            let mut group: Vec<(Vec<u8>, Option<usize>)> = valid
                .iter()
                .cloned()
                .enumerate()
                .map(|(row, payload)| (payload, Some(row)))
                .collect();
            for member in &damaged_members {
                if valid.is_empty() {
                    break;
                }
                let source = &valid[member.source.among(valid.len())];
                let damaged = member.apply(case.format, source.clone());
                // A group of `n` payloads has `n + 1` places for one more.
                let places = group
                    .len()
                    .checked_add(1)
                    .assured("a small group has room for one more");
                group.insert(member.position.among(places), (damaged, None));
            }
            let payloads: Vec<Vec<u8>> = group.iter().map(|(payload, _)| payload.clone()).collect();
            let decoded = case.decode_group(&payloads);
            let mut expected_rows = Vec::new();
            for (index, (payload, original)) in group.iter().enumerate() {
                let alone = case.decode_group(std::slice::from_ref(payload));
                assert_eq!(
                    decoded.accepted[index], alone.accepted[0],
                    "payload {index} is accepted in a group exactly as it is alone"
                );
                if let Some(row) = original {
                    assert!(alone.accepted[0], "a payload the codec wrote decodes");
                    assert_same_batch(alone.batch.batch(), &rows.slice(*row, 1));
                }
                if alone.accepted[0] {
                    expected_rows.extend(LogicalValue::rows(alone.batch.batch()));
                }
            }
            assert_eq!(
                LogicalValue::rows(decoded.batch.batch()),
                expected_rows,
                "the group holds exactly the rows its accepted payloads decode to alone"
            );
        });
}

/// A payload of `format` holding `value`, written the way an external producer writes it, so it
/// may hold values the codec itself never writes for the schema.
fn foreign_payload(case: &CodecCase, value: &JsonValue) -> Vec<u8> {
    match case.format {
        SchemafulFormat::Json | SchemafulFormat::Cbor => write_json_model(case.format, value),
        SchemafulFormat::Avro => {
            let super::CompiledWireSchema::Avro(wire) = &case.codec.wire_schema else {
                unreachable!("an Avro case compiles an Avro wire schema")
            };
            let JsonValue::Object(members) = value else {
                unreachable!("a payload value is an object")
            };
            let mut fields = Vec::with_capacity(members.len());
            for (name, member) in members {
                fields.push((name.clone(), avro_value(member)));
            }
            let GeneratedWireSchema::Avro(definition) = &case.wire else {
                unreachable!("an Avro case declares an Avro wire schema")
            };
            // An Avro datum holds its fields in the order the wire schema lists them.
            let mut ordered = Vec::with_capacity(fields.len());
            for field in &definition.fields {
                let position = fields
                    .iter()
                    .position(|(name, _)| name == field.name.as_str())
                    .assured("every field of the wire schema is in the value");
                ordered.push(fields.swap_remove(position));
            }
            apache_avro::to_avro_datum(&wire.schema, apache_avro::types::Value::Record(ordered))
                .assured("the value matches the codec's Avro schema")
        }
    }
}

/// The Avro value of a string, a whole number or a list of them.
fn avro_value(value: &JsonValue) -> apache_avro::types::Value {
    use apache_avro::types::Value as AvroValue;
    match value {
        JsonValue::String(text) => AvroValue::String(text.clone()),
        JsonValue::Number(number) => {
            AvroValue::Long(number.as_i64().assured("the fixture's numbers are whole"))
        }
        JsonValue::Array(items) => AvroValue::Array(items.iter().map(avro_value).collect()),
        other => unreachable!("the fixture holds no {other}"),
    }
}

/// A payload refused part-way through a list nested inside a list leaves none of its elements in
/// the group: in every schemaful format, the payload decoded after it holds exactly its own
/// nested elements.
#[test]
fn a_payload_refused_inside_a_nested_list_leaves_the_next_row_exact() {
    use nervix_models::{CreateSchema, FieldName, SchemaField, SchemaName};
    use serde_json::json;

    use super::generated_batches::LogicalValue as Value;

    let field = |name: &str, ty: ParseAsType| SchemaField {
        name: FieldName::parse(name).assured("a literal field name"),
        ty,
        optional: false,
        sensitive: false,
    };
    let matrix = ParseAsType::Vec {
        element: Box::new(ParseAsType::Vec {
            element: Box::new(ParseAsType::U8),
        }),
    };
    for format in SchemafulFormat::ALL {
        let schema = GeneratedSchema::new(CreateSchema {
            name: SchemaName::parse("nested").assured("a literal schema name"),
            fields: vec![
                field("tenant", ParseAsType::String),
                field("matrix", matrix.clone()),
            ],
        });
        let case =
            CodecCase::with_schema(&mut Arbitrary::new(&[], Domain::Vocabulary), format, schema);
        let refused = foreign_payload(&case, &json!({"tenant": "acme", "matrix": [[1], [2, 300]]}));
        let accepted = foreign_payload(&case, &json!({"tenant": "beta", "matrix": [[3], [4, 5]]}));
        let group = case.decode_group(&[refused, accepted]);
        assert_eq!(group.accepted, [false, true], "{format:?}: 300 is not a U8");
        let elements = |values: &[u8]| {
            Some(Value::Vec(
                values.iter().map(|value| Some(Value::U8(*value))).collect(),
            ))
        };
        assert_eq!(
            Value::rows(group.batch.batch()),
            vec![vec![
                Some(Value::String("beta".to_string())),
                Some(Value::Vec(vec![elements(&[3]), elements(&[4, 5])])),
            ]],
            "{format:?}: the row after a refused payload holds only its own elements"
        );
    }
}

/// A JSON or CBOR number for an `F32` field reads as the `F32` whose shortest decimal it is. The
/// decoder reads the number as an `f64` first; for `7.038531e-26` that `f64` lies exactly halfway
/// between two `F32` values, and rounding it half to even names the wrong one.
#[test]
fn a_number_reads_as_the_f32_whose_shortest_decimal_it_is() {
    use nervix_models::{CreateSchema, FieldName, SchemaField, SchemaName};

    let written = f32::from_bits(0x15ae_43fd);
    let wide: f64 = "7.038531e-26"
        .parse()
        .assured("the literal is a decimal number");
    for format in [SchemafulFormat::Json, SchemafulFormat::Cbor] {
        let schema = GeneratedSchema::new(CreateSchema {
            name: SchemaName::parse("single").assured("a literal schema name"),
            fields: vec![SchemaField {
                name: FieldName::parse("value").assured("a literal field name"),
                ty: ParseAsType::F32,
                optional: false,
                sensitive: false,
            }],
        });
        let case =
            CodecCase::with_schema(&mut Arbitrary::new(&[], Domain::Vocabulary), format, schema);
        let payload = match format {
            SchemafulFormat::Json => br#"{"value":7.038531e-26}"#.to_vec(),
            _ => {
                let mut members = serde_json::Map::new();
                members.insert("value".to_string(), JsonValue::from(wide));
                write_json_model(format, &JsonValue::Object(members))
            }
        };
        let group = case.decode_group(&[payload]);
        assert_eq!(group.accepted, [true]);
        assert_eq!(
            LogicalValue::rows(group.batch.batch()),
            vec![vec![Some(LogicalValue::F32(written.to_bits()))]],
            "{format:?}: the number reads as the F32 it was written for"
        );
    }
}

/// Whether `value` is a non-finite float or a list holding one at any depth.
fn holds_non_finite_float(value: &LogicalValue) -> bool {
    match value {
        LogicalValue::F32(bits) => !f32::from_bits(*bits).is_finite(),
        LogicalValue::F64(bits) => !f64::from_bits(*bits).is_finite(),
        LogicalValue::Array(elements) | LogicalValue::Vec(elements) => {
            elements.iter().flatten().any(holds_non_finite_float)
        }
        LogicalValue::U8(_)
        | LogicalValue::I8(_)
        | LogicalValue::U16(_)
        | LogicalValue::I16(_)
        | LogicalValue::U32(_)
        | LogicalValue::I32(_)
        | LogicalValue::U64(_)
        | LogicalValue::I64(_)
        | LogicalValue::Bool(_)
        | LogicalValue::String(_)
        | LogicalValue::Bytes(_)
        | LogicalValue::Datetime(_) => false,
    }
}

/// What a `WIRE JSON` or `WIRE CBOR` payload of a row decodes to, given the floats JSON has no
/// number for.
#[derive(Debug, PartialEq)]
enum Projected {
    /// The row with each non-finite float of an optional top-level field read as a null.
    Row(Vec<Option<LogicalValue>>),
    /// The payload is refused: a required field or a list element held a non-finite float.
    Refused,
}

impl Projected {
    fn of(schema: &GeneratedSchema, row: Vec<Option<LogicalValue>>) -> Self {
        let mut projected = Vec::with_capacity(row.len());
        for (field, value) in schema.model.fields.iter().zip(row) {
            let Some(value) = value else {
                projected.push(None);
                continue;
            };
            if !holds_non_finite_float(&value) {
                projected.push(Some(value));
                continue;
            }
            let top_level_float = matches!(value, LogicalValue::F32(_) | LogicalValue::F64(_));
            if top_level_float && field.optional {
                projected.push(None);
                continue;
            }
            return Self::Refused;
        }
        Self::Row(projected)
    }
}

/// JSON has no number for a NaN or an infinity, so over every float bit pattern a `WIRE JSON` or
/// `WIRE CBOR` codec follows its projection rather than a round trip: a row whose non-finite floats
/// all sit in optional top-level fields reads back with those fields null and every other value
/// intact, and a row holding one in a required field or a list element is refused with a typed
/// failure of its own payload.
#[test]
fn bolero_non_finite_floats_follow_the_json_projection() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let format = arbitrary
                .entropy()
                .pick([SchemafulFormat::Json, SchemafulFormat::Cbor]);
            let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
            let case = CodecCase::with_schema(&mut arbitrary, format, schema);
            let rows = GeneratedDomain::Arrow.batch(&mut arbitrary, &case.schema);
            let batch = case.schema.runtime_batch(rows.clone());
            let payloads = case.encode(&batch);
            for (row, payload) in LogicalValue::rows(&rows).into_iter().zip(&payloads) {
                let expected = Projected::of(&case.schema, row);
                let alone = case.decode_group(std::slice::from_ref(payload));
                let decoded = if alone.accepted[0] {
                    let mut decoded_rows = LogicalValue::rows(alone.batch.batch());
                    let decoded_row = decoded_rows.pop().assured("an accepted payload is one row");
                    Projected::Row(decoded_row)
                } else {
                    Projected::Refused
                };
                assert_eq!(
                    decoded, expected,
                    "{format:?}: the row follows the JSON projection"
                );
            }
        });
}

/// A JSON or CBOR number beyond the range of an `F32` field rounds to the infinity of its sign, the
/// nearest float, as reading it as an `F32` does.
#[test]
fn a_number_beyond_the_f32_range_reads_as_an_infinity() {
    use nervix_models::{CreateSchema, FieldName, SchemaField, SchemaName};

    for format in [SchemafulFormat::Json, SchemafulFormat::Cbor] {
        let schema = GeneratedSchema::new(CreateSchema {
            name: SchemaName::parse("single").assured("a literal schema name"),
            fields: vec![SchemaField {
                name: FieldName::parse("value").assured("a literal field name"),
                ty: ParseAsType::F32,
                optional: false,
                sensitive: false,
            }],
        });
        let case =
            CodecCase::with_schema(&mut Arbitrary::new(&[], Domain::Vocabulary), format, schema);
        for (number, expected) in [(1e39_f64, f32::INFINITY), (-1e39_f64, f32::NEG_INFINITY)] {
            let mut members = serde_json::Map::new();
            members.insert("value".to_string(), JsonValue::from(number));
            let payload = write_json_model(format, &JsonValue::Object(members));
            let group = case.decode_group(&[payload]);
            assert_eq!(group.accepted, [true]);
            assert_eq!(
                LogicalValue::rows(group.batch.batch()),
                vec![vec![Some(LogicalValue::F32(expected.to_bits()))]],
                "{format:?}: {number} reads as the infinity of its sign"
            );
        }
    }
}
