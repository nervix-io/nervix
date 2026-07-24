//! Cross-language FlatBuffers protocol for the Nervix WASM ABI.
//!
//! Decoding first produces verified borrowed views. Large Arrow IPC vectors stay
//! borrowed from the FlatBuffer until a caller explicitly asks for an owned model.

use flatbuffers::{Allocator, FlatBufferBuilder, WIPOffset};
use thiserror::Error;

mod generated {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/nervix_wasm_generated.rs"
    ));
}

use generated::nervix_wasm as wire;

pub const FILE_IDENTIFIER: &str = wire::MESSAGE_IDENTIFIER;
pub const SERIALIZATION_NAME: &str = "FlatBuffers";

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("WASM message does not have the {FILE_IDENTIFIER} FlatBuffers identifier")]
    InvalidIdentifier,
    #[error("WASM size-prefixed FlatBuffer declares {declared} bytes but received {actual}")]
    LengthMismatch { declared: usize, actual: usize },
    #[error("WASM FlatBuffer failed verification: {0}")]
    InvalidFlatbuffer(#[source] flatbuffers::InvalidFlatbuffer),
    #[error("expected WASM {expected} payload, found {actual}")]
    UnexpectedPayload {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("WASM FlatBuffer contains unknown {kind} value {value}")]
    UnknownEnum { kind: &'static str, value: u8 },
    #[error("WASM processor type '{kind}' is missing its element type")]
    MissingElementType { kind: &'static str },
    #[error("WASM uninitialized output column must use column index 0, found {column_index}")]
    InvalidUninitializedColumnIndex { column_index: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInit {
    pub domain_name: String,
    pub domain_type: String,
    pub branch_key: Option<Vec<u8>>,
    pub input_schema: ProcessorSchema,
    pub output_schemas: Vec<ProcessorSchema>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessorSchema {
    pub name: String,
    pub fields: Vec<ProcessorField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessorField {
    pub name: String,
    pub ty: ProcessorType,
    pub optional: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProcessorType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Datetime,
    F32,
    F64,
    Array { element: Box<Self>, len: u32 },
    Vec { element: Box<Self> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AckToken(pub u64);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AckTokenSet {
    pub tokens: Vec<AckToken>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OutputRow {
    pub tokens: Vec<AckToken>,
    pub source_token: Option<AckToken>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NackSet {
    pub tokens: Vec<AckToken>,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageErrorSet {
    pub tokens: Vec<AckToken>,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AckSidecar {
    pub rows: Vec<OutputRow>,
    pub acked: Vec<AckTokenSet>,
    pub nacked: Vec<NackSet>,
    pub message_errors: Vec<MessageErrorSet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Envelope {
    Input {
        arrow_ipc_batch: Vec<u8>,
        acks: AckSidecar,
    },
    Output {
        generated_arrow_ipc_batch: Vec<u8>,
        outputs: Vec<RoutedOutput>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutedOutput {
    pub output_relay: String,
    pub columns: Vec<OutputColumnRef>,
    pub acks: AckSidecar,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputColumnRef {
    Generated { column_index: u32 },
    Input { column_index: u32 },
    Uninitialized,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GuestSnapshot {
    pub processed_batches: u64,
    pub processed_rows: u64,
    pub pending_start_row: u64,
    pub last_domain_time_nanos: i64,
    pub last_timeout_handle: i64,
    pub pending_batch: Vec<u8>,
    pub init_metadata: Vec<u8>,
    pub saved_state: Vec<u8>,
    pub error_state: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum EnvelopeRef<'a> {
    Input(InputEnvelopeRef<'a>),
    Output(OutputEnvelopeRef<'a>),
}

#[derive(Clone, Copy, Debug)]
pub struct InputEnvelopeRef<'a>(wire::InputEnvelope<'a>);

#[derive(Clone, Copy, Debug)]
pub struct OutputEnvelopeRef<'a>(wire::OutputEnvelope<'a>);

impl BranchInit {
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        self.encode_in(&mut builder);
        builder.finished_data().to_vec()
    }

    /// Encodes and finishes this message in the supplied FlatBuffer builder.
    pub fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>) {
        let payload = build_branch_init(builder, self);
        finish_message(
            builder,
            wire::MessagePayload::BranchInit,
            payload.as_union_value(),
        );
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let message = verified_message(bytes)?;
        let payload =
            message
                .payload_as_branch_init()
                .ok_or_else(|| ProtocolError::UnexpectedPayload {
                    expected: "branch init",
                    actual: payload_name(message.payload_type()),
                })?;
        decode_branch_init(payload)
    }
}

impl Envelope {
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        self.encode_in(&mut builder);
        builder.finished_data().to_vec()
    }

    /// Encodes and finishes this message in the supplied FlatBuffer builder.
    pub fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>) {
        match self {
            Self::Input {
                arrow_ipc_batch,
                acks,
            } => encode_input_in(builder, arrow_ipc_batch, acks),
            Self::Output {
                generated_arrow_ipc_batch,
                outputs,
            } => encode_output_in(builder, generated_arrow_ipc_batch, outputs),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        EnvelopeRef::decode(bytes)?.to_owned()
    }
}

impl<'a> EnvelopeRef<'a> {
    pub fn decode(bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        let message = verified_message(bytes)?;
        if let Some(input) = message.payload_as_input_envelope() {
            return Ok(Self::Input(InputEnvelopeRef(input)));
        }
        if let Some(output) = message.payload_as_output_envelope() {
            return Ok(Self::Output(OutputEnvelopeRef(output)));
        }
        Err(ProtocolError::UnexpectedPayload {
            expected: "envelope",
            actual: payload_name(message.payload_type()),
        })
    }

    pub fn to_owned(self) -> Result<Envelope, ProtocolError> {
        match self {
            Self::Input(input) => Ok(Envelope::Input {
                arrow_ipc_batch: input.arrow_ipc_batch().to_vec(),
                acks: input.acks(),
            }),
            Self::Output(output) => Ok(Envelope::Output {
                generated_arrow_ipc_batch: output.generated_arrow_ipc_batch().to_vec(),
                outputs: output.outputs()?,
            }),
        }
    }
}

impl<'a> InputEnvelopeRef<'a> {
    pub fn arrow_ipc_batch(self) -> &'a [u8] {
        self.0.arrow_ipc_batch().bytes()
    }

    pub fn acks(self) -> AckSidecar {
        decode_ack_sidecar(self.0.acks())
    }
}

impl<'a> OutputEnvelopeRef<'a> {
    pub fn generated_arrow_ipc_batch(self) -> &'a [u8] {
        self.0.generated_arrow_ipc_batch().bytes()
    }

    pub fn outputs(self) -> Result<Vec<RoutedOutput>, ProtocolError> {
        self.0.outputs().iter().map(decode_routed_output).collect()
    }
}

impl GuestSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();
        self.encode_in(&mut builder);
        builder.finished_data().to_vec()
    }

    /// Encodes and finishes this message in the supplied FlatBuffer builder.
    pub fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>) {
        let pending_batch = builder.create_vector(&self.pending_batch);
        let init_metadata = builder.create_vector(&self.init_metadata);
        let saved_state = builder.create_vector(&self.saved_state);
        let error_state = self
            .error_state
            .as_deref()
            .map(|error| builder.create_string(error));
        let payload = wire::GuestSnapshot::create(
            builder,
            &wire::GuestSnapshotArgs {
                processed_batches: self.processed_batches,
                processed_rows: self.processed_rows,
                pending_start_row: self.pending_start_row,
                last_domain_time_nanos: self.last_domain_time_nanos,
                last_timeout_handle: self.last_timeout_handle,
                pending_batch: Some(pending_batch),
                init_metadata: Some(init_metadata),
                saved_state: Some(saved_state),
                error_state,
            },
        );
        finish_message(
            builder,
            wire::MessagePayload::GuestSnapshot,
            payload.as_union_value(),
        );
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let message = verified_message(bytes)?;
        let snapshot = message.payload_as_guest_snapshot().ok_or_else(|| {
            ProtocolError::UnexpectedPayload {
                expected: "guest snapshot",
                actual: payload_name(message.payload_type()),
            }
        })?;
        Ok(Self {
            processed_batches: snapshot.processed_batches(),
            processed_rows: snapshot.processed_rows(),
            pending_start_row: snapshot.pending_start_row(),
            last_domain_time_nanos: snapshot.last_domain_time_nanos(),
            last_timeout_handle: snapshot.last_timeout_handle(),
            pending_batch: snapshot.pending_batch().bytes().to_vec(),
            init_metadata: snapshot.init_metadata().bytes().to_vec(),
            saved_state: snapshot.saved_state().bytes().to_vec(),
            error_state: snapshot.error_state().map(str::to_string),
        })
    }
}

fn verified_message(bytes: &[u8]) -> Result<wire::Message<'_>, ProtocolError> {
    let Some(prefix) = bytes.get(..4) else {
        return Err(ProtocolError::LengthMismatch {
            declared: 0,
            actual: bytes.len(),
        });
    };
    let declared = u32::from_le_bytes(prefix.try_into().expect("prefix has exactly four bytes"));
    let declared = usize::try_from(declared).unwrap_or(usize::MAX);
    let actual = bytes.len().saturating_sub(4);
    if declared != actual {
        return Err(ProtocolError::LengthMismatch { declared, actual });
    }
    if !wire::message_size_prefixed_buffer_has_identifier(bytes) {
        return Err(ProtocolError::InvalidIdentifier);
    }
    wire::size_prefixed_root_as_message(bytes).map_err(ProtocolError::InvalidFlatbuffer)
}

/// Encodes and finishes an input envelope in the supplied FlatBuffer builder.
pub fn encode_input_in<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    arrow_ipc_batch: &[u8],
    acks: &AckSidecar,
) {
    let arrow_ipc_batch = builder.create_vector(arrow_ipc_batch);
    let acks = build_ack_sidecar(builder, acks);
    let input = wire::InputEnvelope::create(
        builder,
        &wire::InputEnvelopeArgs {
            arrow_ipc_batch: Some(arrow_ipc_batch),
            acks: Some(acks),
        },
    );
    finish_message(
        builder,
        wire::MessagePayload::InputEnvelope,
        input.as_union_value(),
    );
}

/// Encodes and finishes an output envelope in the supplied FlatBuffer builder.
pub fn encode_output_in<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    generated_arrow_ipc_batch: &[u8],
    outputs: &[RoutedOutput],
) {
    let generated_arrow_ipc_batch = builder.create_vector(generated_arrow_ipc_batch);
    let outputs = outputs
        .iter()
        .map(|output| build_routed_output(builder, output))
        .collect::<Vec<_>>();
    let outputs = builder.create_vector(&outputs);
    let output = wire::OutputEnvelope::create(
        builder,
        &wire::OutputEnvelopeArgs {
            generated_arrow_ipc_batch: Some(generated_arrow_ipc_batch),
            outputs: Some(outputs),
        },
    );
    finish_message(
        builder,
        wire::MessagePayload::OutputEnvelope,
        output.as_union_value(),
    );
}

fn finish_message<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    payload_type: wire::MessagePayload,
    payload: WIPOffset<flatbuffers::UnionWIPOffset>,
) {
    let message = wire::Message::create(
        builder,
        &wire::MessageArgs {
            payload_type,
            payload: Some(payload),
        },
    );
    wire::finish_size_prefixed_message_buffer(builder, message);
}

fn payload_name(payload: wire::MessagePayload) -> &'static str {
    payload.variant_name().unwrap_or("unknown")
}

fn build_branch_init<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    init: &BranchInit,
) -> WIPOffset<wire::BranchInit<'a>> {
    let domain_name = builder.create_string(&init.domain_name);
    let domain_type = builder.create_string(&init.domain_type);
    let branch_key = init
        .branch_key
        .as_deref()
        .map(|branch_key| builder.create_vector(branch_key));
    let input_schema = build_processor_schema(builder, &init.input_schema);
    let output_schemas = init
        .output_schemas
        .iter()
        .map(|schema| build_processor_schema(builder, schema))
        .collect::<Vec<_>>();
    let output_schemas = builder.create_vector(&output_schemas);
    wire::BranchInit::create(
        builder,
        &wire::BranchInitArgs {
            domain_name: Some(domain_name),
            domain_type: Some(domain_type),
            branch_key,
            input_schema: Some(input_schema),
            output_schemas: Some(output_schemas),
        },
    )
}

fn build_processor_schema<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    schema: &ProcessorSchema,
) -> WIPOffset<wire::ProcessorSchema<'a>> {
    let name = builder.create_string(&schema.name);
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let name = builder.create_string(&field.name);
            let ty = build_processor_type(builder, &field.ty);
            wire::ProcessorField::create(
                builder,
                &wire::ProcessorFieldArgs {
                    name: Some(name),
                    type_: Some(ty),
                    optional: field.optional,
                },
            )
        })
        .collect::<Vec<_>>();
    let fields = builder.create_vector(&fields);
    wire::ProcessorSchema::create(
        builder,
        &wire::ProcessorSchemaArgs {
            name: Some(name),
            fields: Some(fields),
        },
    )
}

fn build_processor_type<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    ty: &ProcessorType,
) -> WIPOffset<wire::ProcessorType<'a>> {
    let (kind, element, array_len) = match ty {
        ProcessorType::U8 => (wire::ProcessorTypeKind::U8, None, 0),
        ProcessorType::I8 => (wire::ProcessorTypeKind::I8, None, 0),
        ProcessorType::U16 => (wire::ProcessorTypeKind::U16, None, 0),
        ProcessorType::I16 => (wire::ProcessorTypeKind::I16, None, 0),
        ProcessorType::U32 => (wire::ProcessorTypeKind::U32, None, 0),
        ProcessorType::I32 => (wire::ProcessorTypeKind::I32, None, 0),
        ProcessorType::U64 => (wire::ProcessorTypeKind::U64, None, 0),
        ProcessorType::I64 => (wire::ProcessorTypeKind::I64, None, 0),
        ProcessorType::Bool => (wire::ProcessorTypeKind::Bool, None, 0),
        ProcessorType::String => (wire::ProcessorTypeKind::String, None, 0),
        ProcessorType::Datetime => (wire::ProcessorTypeKind::Datetime, None, 0),
        ProcessorType::F32 => (wire::ProcessorTypeKind::F32, None, 0),
        ProcessorType::F64 => (wire::ProcessorTypeKind::F64, None, 0),
        ProcessorType::Array { element, len } => (
            wire::ProcessorTypeKind::Array,
            Some(build_processor_type(builder, element)),
            *len,
        ),
        ProcessorType::Vec { element } => (
            wire::ProcessorTypeKind::Vec,
            Some(build_processor_type(builder, element)),
            0,
        ),
    };
    wire::ProcessorType::create(
        builder,
        &wire::ProcessorTypeArgs {
            kind,
            element,
            array_len,
        },
    )
}

fn build_ack_sidecar<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    sidecar: &AckSidecar,
) -> WIPOffset<wire::AckSidecar<'a>> {
    let rows = sidecar
        .rows
        .iter()
        .map(|row| {
            let tokens = row.tokens.iter().map(|token| token.0).collect::<Vec<_>>();
            let tokens = builder.create_vector(&tokens);
            wire::OutputRow::create(
                builder,
                &wire::OutputRowArgs {
                    tokens: Some(tokens),
                    source_token: row.source_token.map(|token| token.0),
                },
            )
        })
        .collect::<Vec<_>>();
    let rows = builder.create_vector(&rows);
    let acked = sidecar
        .acked
        .iter()
        .map(|set| {
            let tokens = set.tokens.iter().map(|token| token.0).collect::<Vec<_>>();
            let tokens = builder.create_vector(&tokens);
            wire::AckTokenSet::create(
                builder,
                &wire::AckTokenSetArgs {
                    tokens: Some(tokens),
                },
            )
        })
        .collect::<Vec<_>>();
    let acked = builder.create_vector(&acked);
    let nacked = sidecar
        .nacked
        .iter()
        .map(|set| {
            let tokens = set.tokens.iter().map(|token| token.0).collect::<Vec<_>>();
            let tokens = builder.create_vector(&tokens);
            let reason = builder.create_string(&set.reason);
            wire::NackSet::create(
                builder,
                &wire::NackSetArgs {
                    tokens: Some(tokens),
                    reason: Some(reason),
                },
            )
        })
        .collect::<Vec<_>>();
    let nacked = builder.create_vector(&nacked);
    let message_errors = sidecar
        .message_errors
        .iter()
        .map(|set| {
            let tokens = set.tokens.iter().map(|token| token.0).collect::<Vec<_>>();
            let tokens = builder.create_vector(&tokens);
            let reason = builder.create_string(&set.reason);
            wire::MessageErrorSet::create(
                builder,
                &wire::MessageErrorSetArgs {
                    tokens: Some(tokens),
                    reason: Some(reason),
                },
            )
        })
        .collect::<Vec<_>>();
    let message_errors = builder.create_vector(&message_errors);
    wire::AckSidecar::create(
        builder,
        &wire::AckSidecarArgs {
            rows: Some(rows),
            acked: Some(acked),
            nacked: Some(nacked),
            message_errors: Some(message_errors),
        },
    )
}

fn build_routed_output<'a, A: Allocator + 'a>(
    builder: &mut FlatBufferBuilder<'a, A>,
    output: &RoutedOutput,
) -> WIPOffset<wire::RoutedOutput<'a>> {
    let output_relay = builder.create_string(&output.output_relay);
    let columns = output
        .columns
        .iter()
        .map(|column| {
            let (source, column_index) = match column {
                OutputColumnRef::Generated { column_index } => {
                    (wire::ColumnSource::Generated, *column_index)
                }
                OutputColumnRef::Input { column_index } => {
                    (wire::ColumnSource::Input, *column_index)
                }
                OutputColumnRef::Uninitialized => (wire::ColumnSource::Uninitialized, 0),
            };
            wire::OutputColumnRef::create(
                builder,
                &wire::OutputColumnRefArgs {
                    source,
                    column_index,
                },
            )
        })
        .collect::<Vec<_>>();
    let columns = builder.create_vector(&columns);
    let acks = build_ack_sidecar(builder, &output.acks);
    wire::RoutedOutput::create(
        builder,
        &wire::RoutedOutputArgs {
            output_relay: Some(output_relay),
            columns: Some(columns),
            acks: Some(acks),
        },
    )
}

fn decode_branch_init(init: wire::BranchInit<'_>) -> Result<BranchInit, ProtocolError> {
    Ok(BranchInit {
        domain_name: init.domain_name().to_string(),
        domain_type: init.domain_type().to_string(),
        branch_key: init.branch_key().map(|key| key.bytes().to_vec()),
        input_schema: decode_processor_schema(init.input_schema())?,
        output_schemas: init
            .output_schemas()
            .iter()
            .map(decode_processor_schema)
            .collect::<Result<_, _>>()?,
    })
}

fn decode_processor_schema(
    schema: wire::ProcessorSchema<'_>,
) -> Result<ProcessorSchema, ProtocolError> {
    Ok(ProcessorSchema {
        name: schema.name().to_string(),
        fields: schema
            .fields()
            .iter()
            .map(|field| {
                Ok(ProcessorField {
                    name: field.name().to_string(),
                    ty: decode_processor_type(field.type_())?,
                    optional: field.optional(),
                })
            })
            .collect::<Result<_, ProtocolError>>()?,
    })
}

fn decode_processor_type(ty: wire::ProcessorType<'_>) -> Result<ProcessorType, ProtocolError> {
    let scalar = match ty.kind() {
        wire::ProcessorTypeKind::U8 => ProcessorType::U8,
        wire::ProcessorTypeKind::I8 => ProcessorType::I8,
        wire::ProcessorTypeKind::U16 => ProcessorType::U16,
        wire::ProcessorTypeKind::I16 => ProcessorType::I16,
        wire::ProcessorTypeKind::U32 => ProcessorType::U32,
        wire::ProcessorTypeKind::I32 => ProcessorType::I32,
        wire::ProcessorTypeKind::U64 => ProcessorType::U64,
        wire::ProcessorTypeKind::I64 => ProcessorType::I64,
        wire::ProcessorTypeKind::Bool => ProcessorType::Bool,
        wire::ProcessorTypeKind::String => ProcessorType::String,
        wire::ProcessorTypeKind::Datetime => ProcessorType::Datetime,
        wire::ProcessorTypeKind::F32 => ProcessorType::F32,
        wire::ProcessorTypeKind::F64 => ProcessorType::F64,
        wire::ProcessorTypeKind::Array => {
            let element = ty
                .element()
                .ok_or(ProtocolError::MissingElementType { kind: "array" })?;
            ProcessorType::Array {
                element: Box::new(decode_processor_type(element)?),
                len: ty.array_len(),
            }
        }
        wire::ProcessorTypeKind::Vec => {
            let element = ty
                .element()
                .ok_or(ProtocolError::MissingElementType { kind: "vec" })?;
            ProcessorType::Vec {
                element: Box::new(decode_processor_type(element)?),
            }
        }
        unknown => {
            return Err(ProtocolError::UnknownEnum {
                kind: "processor type",
                value: unknown.0,
            });
        }
    };
    Ok(scalar)
}

fn decode_ack_sidecar(sidecar: wire::AckSidecar<'_>) -> AckSidecar {
    AckSidecar {
        rows: sidecar
            .rows()
            .iter()
            .map(|row| OutputRow {
                tokens: row.tokens().iter().map(AckToken).collect(),
                source_token: row.source_token().map(AckToken),
            })
            .collect(),
        acked: sidecar
            .acked()
            .iter()
            .map(|set| AckTokenSet {
                tokens: set.tokens().iter().map(AckToken).collect(),
            })
            .collect(),
        nacked: sidecar
            .nacked()
            .iter()
            .map(|set| NackSet {
                tokens: set.tokens().iter().map(AckToken).collect(),
                reason: set.reason().to_string(),
            })
            .collect(),
        message_errors: sidecar
            .message_errors()
            .iter()
            .map(|set| MessageErrorSet {
                tokens: set.tokens().iter().map(AckToken).collect(),
                reason: set.reason().to_string(),
            })
            .collect(),
    }
}

fn decode_routed_output(output: wire::RoutedOutput<'_>) -> Result<RoutedOutput, ProtocolError> {
    Ok(RoutedOutput {
        output_relay: output.output_relay().to_string(),
        columns: output
            .columns()
            .iter()
            .map(|column| match column.source() {
                wire::ColumnSource::Input => Ok(OutputColumnRef::Input {
                    column_index: column.column_index(),
                }),
                wire::ColumnSource::Generated => Ok(OutputColumnRef::Generated {
                    column_index: column.column_index(),
                }),
                wire::ColumnSource::Uninitialized => {
                    if column.column_index() == 0 {
                        Ok(OutputColumnRef::Uninitialized)
                    } else {
                        Err(ProtocolError::InvalidUninitializedColumnIndex {
                            column_index: column.column_index(),
                        })
                    }
                }
                unknown => Err(ProtocolError::UnknownEnum {
                    kind: "output column source",
                    value: unknown.0,
                }),
            })
            .collect::<Result<_, _>>()?,
        acks: decode_ack_sidecar(output.acks()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sidecar() -> AckSidecar {
        AckSidecar {
            rows: vec![OutputRow {
                tokens: vec![AckToken(7), AckToken(8)],
                source_token: Some(AckToken(7)),
            }],
            acked: vec![AckTokenSet {
                tokens: vec![AckToken(9)],
            }],
            nacked: vec![NackSet {
                tokens: vec![AckToken(10)],
                reason: "retry".to_string(),
            }],
            message_errors: vec![MessageErrorSet {
                tokens: vec![AckToken(11)],
                reason: "invalid".to_string(),
            }],
        }
    }

    #[test]
    fn input_envelope_round_trips_and_borrows_arrow_bytes() {
        let envelope = Envelope::Input {
            arrow_ipc_batch: vec![0, 1, 2, 255],
            acks: sidecar(),
        };
        let encoded = envelope.encode();
        assert_eq!(&encoded[8..12], FILE_IDENTIFIER.as_bytes());

        let EnvelopeRef::Input(view) = EnvelopeRef::decode(&encoded).expect("must decode") else {
            panic!("expected input view");
        };
        assert_eq!(view.arrow_ipc_batch(), [0, 1, 2, 255]);
        let start = encoded.as_ptr() as usize;
        let end = start + encoded.len();
        let borrowed = view.arrow_ipc_batch().as_ptr() as usize;
        assert!((start..end).contains(&borrowed));
        assert_eq!(Envelope::decode(&encoded).expect("must own"), envelope);
    }

    #[test]
    fn uninitialized_output_column_round_trips() {
        let envelope = Envelope::Output {
            generated_arrow_ipc_batch: Vec::new(),
            outputs: vec![RoutedOutput {
                output_relay: "events".to_string(),
                columns: vec![OutputColumnRef::Uninitialized],
                acks: sidecar(),
            }],
        };

        let encoded = envelope.encode();

        assert_eq!(Envelope::decode(&encoded).expect("must decode"), envelope);
    }

    #[test]
    fn uninitialized_output_column_rejects_nonzero_index() {
        let mut builder = FlatBufferBuilder::new();
        let column = wire::OutputColumnRef::create(
            &mut builder,
            &wire::OutputColumnRefArgs {
                source: wire::ColumnSource::Uninitialized,
                column_index: 1,
            },
        );
        let columns = builder.create_vector(&[column]);
        let output_relay = builder.create_string("events");
        let acks = build_ack_sidecar(&mut builder, &AckSidecar::default());
        let routed = wire::RoutedOutput::create(
            &mut builder,
            &wire::RoutedOutputArgs {
                output_relay: Some(output_relay),
                columns: Some(columns),
                acks: Some(acks),
            },
        );
        let outputs = builder.create_vector(&[routed]);
        let generated = builder.create_vector(&[] as &[u8]);
        let output = wire::OutputEnvelope::create(
            &mut builder,
            &wire::OutputEnvelopeArgs {
                generated_arrow_ipc_batch: Some(generated),
                outputs: Some(outputs),
            },
        );
        finish_message(
            &mut builder,
            wire::MessagePayload::OutputEnvelope,
            output.as_union_value(),
        );
        let encoded = builder.finished_data().to_vec();

        assert!(matches!(
            Envelope::decode(&encoded),
            Err(ProtocolError::InvalidUninitializedColumnIndex { column_index: 1 })
        ));
    }

    #[test]
    fn invalid_identifier_is_rejected_before_traversal() {
        let mut encoded = Envelope::Input {
            arrow_ipc_batch: Vec::new(),
            acks: AckSidecar::default(),
        }
        .encode();
        encoded[8..12].copy_from_slice(b"CBOR");
        assert!(matches!(
            EnvelopeRef::decode(&encoded),
            Err(ProtocolError::InvalidIdentifier)
        ));
    }
}
