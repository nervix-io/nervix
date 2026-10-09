//! Relay delivery and acknowledgement messages through the bounded wire codec.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated relay grant requests and replies, admission exchanges, acknowledgement
//!   resolutions and connection bindings, with every branch key, record metadata and registration
//!   they carry, and the properties that the bounded rkyv codec keeps each one whole and refuses
//!   damaged bytes with a typed failure that leaves no charge and no allocation behind.
//! - **Depends on.** The wire messages, the bounded codec, the executor and the vocabulary
//!   generators.
//! - **Must not know.** Runtime branches, Arrow decoding or the transport's connection lifecycle.

use std::{collections::VecDeque, num::NonZeroUsize};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_execution::{ChargedBytes, CpuClass, Executor, MemoryClass};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainName, RelayName,
    RemoteAckOutcome, RemoteAckRegistration, RemoteAckResolution, RemoteRuntimeElementValue,
    RemoteRuntimeField, RemoteRuntimeRecordMetadata, RemoteRuntimeValue,
};
use nervix_primitives::sync::StdArc;
use rkyv::{rancor::Error as RkyvError, util::AlignedVec};

use super::{
    ConnectionAccepted, ConnectionHello, RelayAdmissionRequest, RelayAdmissionResponse,
    RelayGrantDisposition, RelayGrantRequest, RelayGrantResponse, RelayMetadata, decode_aligned,
    decode_rkyv, encode_rkyv, validation_depth,
};
use crate::{PoolClass, RelayAdmissionStatus, RelayDelivery, RelayPayload, RelayPayloadKind};

/// The bytes one case reads its messages from.
const CASE_BYTES: usize = 2048;

/// The most rows one generated relay payload describes.
const ROWS: usize = 6;

/// The most fields one generated branch key holds.
const KEY_FIELDS: usize = 3;

/// How deeply generated branch key values nest.
const VALUE_DEPTH: u8 = 2;

/// The most elements one generated branch key list holds.
const ELEMENTS: usize = 3;

/// The most bytes of one generated relay body.
const BODY_BYTES: usize = 64;

fn property_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens")
}

/// A branch key value as a peer compares it: every float by its bits, so a signed zero and every
/// NaN payload are told apart.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactValue {
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    Bool(bool),
    String(String),
    Datetime(String),
    F32(u32),
    F64(u64),
    Array(Vec<ExactValue>),
    Vec(Vec<ExactValue>),
}

impl ExactValue {
    fn of(value: &RemoteRuntimeValue) -> Self {
        match value {
            RemoteRuntimeValue::U8(v) => Self::U8(*v),
            RemoteRuntimeValue::I8(v) => Self::I8(*v),
            RemoteRuntimeValue::U16(v) => Self::U16(*v),
            RemoteRuntimeValue::I16(v) => Self::I16(*v),
            RemoteRuntimeValue::U32(v) => Self::U32(*v),
            RemoteRuntimeValue::I32(v) => Self::I32(*v),
            RemoteRuntimeValue::U64(v) => Self::U64(*v),
            RemoteRuntimeValue::I64(v) => Self::I64(*v),
            RemoteRuntimeValue::Bool(v) => Self::Bool(*v),
            RemoteRuntimeValue::String(v) => Self::String(v.clone()),
            RemoteRuntimeValue::Datetime(v) => Self::Datetime(v.clone()),
            RemoteRuntimeValue::F32(v) => Self::F32(v.to_bits()),
            RemoteRuntimeValue::F64(v) => Self::F64(v.to_bits()),
            RemoteRuntimeValue::Array(values) => {
                Self::Array(values.iter().map(Self::of_element).collect())
            }
            RemoteRuntimeValue::Vec(values) => {
                Self::Vec(values.iter().map(Self::of_element).collect())
            }
        }
    }

    fn of_element(value: &RemoteRuntimeElementValue) -> Self {
        match value {
            RemoteRuntimeElementValue::U8(v) => Self::U8(*v),
            RemoteRuntimeElementValue::I8(v) => Self::I8(*v),
            RemoteRuntimeElementValue::U16(v) => Self::U16(*v),
            RemoteRuntimeElementValue::I16(v) => Self::I16(*v),
            RemoteRuntimeElementValue::U32(v) => Self::U32(*v),
            RemoteRuntimeElementValue::I32(v) => Self::I32(*v),
            RemoteRuntimeElementValue::U64(v) => Self::U64(*v),
            RemoteRuntimeElementValue::I64(v) => Self::I64(*v),
            RemoteRuntimeElementValue::Bool(v) => Self::Bool(*v),
            RemoteRuntimeElementValue::String(v) => Self::String(v.clone()),
            RemoteRuntimeElementValue::Datetime(v) => Self::Datetime(v.clone()),
            RemoteRuntimeElementValue::F32(v) => Self::F32(v.to_bits()),
            RemoteRuntimeElementValue::F64(v) => Self::F64(v.to_bits()),
            RemoteRuntimeElementValue::Array(values) => {
                Self::Array(values.iter().map(Self::of_element).collect())
            }
            RemoteRuntimeElementValue::Vec(values) => {
                Self::Vec(values.iter().map(Self::of_element).collect())
            }
        }
    }

    /// A whole branch key, field by field in its order.
    fn key(key: &Option<Vec<RemoteRuntimeField>>) -> Option<Vec<(String, Self)>> {
        let fields = key.as_ref()?;
        Some(
            fields
                .iter()
                .map(|field| (field.name.clone(), Self::of(&field.value)))
                .collect(),
        )
    }
}

/// Asserts that `actual` is `expected` field by field, the branch key by its exact values.
fn assert_same_metadata(actual: &RelayMetadata, expected: &RelayMetadata) {
    assert_eq!(actual.kind, expected.kind, "the payload role is kept");
    assert_eq!(actual.domain, expected.domain, "the domain is kept");
    assert_eq!(actual.relay, expected.relay, "the relay is kept");
    assert_eq!(
        ExactValue::key(&actual.key),
        ExactValue::key(&expected.key),
        "the branch key is kept to the bit"
    );
    assert_eq!(
        actual.metadata, expected.metadata,
        "every row's watermarks are kept"
    );
    assert_eq!(
        actual.acks, expected.acks,
        "every row's registration is kept"
    );
    assert_eq!(
        actual.admission, expected.admission,
        "the admission is kept"
    );
}

fn assert_same_payload(actual: &RelayPayload, expected: &RelayPayload) {
    assert_eq!(
        actual.delivery, expected.delivery,
        "the delivery position is kept"
    );
    assert_same_metadata(
        &RelayMetadata::from_payload(actual),
        &RelayMetadata::from_payload(expected),
    );
    assert_eq!(
        actual.batch_ipc, expected.batch_ipc,
        "the body bytes are kept"
    );
}

/// Generated wire messages of one case.
struct WireCase {
    payload: RelayPayload,
    sender_epoch: u64,
    receiver_epoch: u64,
    disposition: RelayGrantDisposition,
    status: RelayAdmissionStatus,
    resolution: RemoteAckResolution,
    hello: ConnectionHello,
}

impl WireCase {
    fn new(arbitrary: &mut Arbitrary<'_>, executor: &Executor) -> Self {
        // What selects each message's variant is read first. An ordinary run hands a property at
        // most 64 bytes, and a choice read after they run out takes its first option, which would
        // leave nearly every case with the first disposition, status and outcome and no branch
        // key.
        let disposition_choice = arbitrary.entropy().byte() % 6;
        let status_choice = arbitrary.entropy().byte() % 8;
        let outcome_choice = arbitrary.entropy().byte() % 4;
        let kind = arbitrary.entropy().pick([
            RelayPayloadKind::Routed,
            RelayPayloadKind::SubscriptionFanout,
            RelayPayloadKind::Ingress,
        ]);
        let class = arbitrary.entropy().pick(PoolClass::ALL);
        let key = branch_key(arbitrary);
        let rows = arbitrary.entropy().count(ROWS);
        let mut metadata = Vec::with_capacity(rows);
        let mut acks = Vec::with_capacity(rows);
        for _ in 0..rows {
            metadata.push(RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: arbitrary.timestamp(),
                ingested_at_high_watermark: arbitrary.timestamp(),
            });
            acks.push(if arbitrary.entropy().flag() {
                Some(registration(arbitrary))
            } else {
                None
            });
        }
        let admission = if arbitrary.entropy().flag() {
            Some(registration(arbitrary))
        } else {
            None
        };
        let body_length = arbitrary.entropy().count(BODY_BYTES);
        let mut body = Vec::with_capacity(body_length);
        for _ in 0..body_length {
            body.push(arbitrary.entropy().byte());
        }
        let payload = RelayPayload {
            delivery: delivery(arbitrary.entropy()),
            kind,
            domain: arbitrary.rule_name::<DomainName>(),
            relay: arbitrary.rule_name::<RelayName>(),
            key,
            batch_ipc: executor
                .try_charge_owned(MemoryClass::Relay, body)
                .assured("a bounded body fits the relay budget"),
            metadata,
            acks,
            admission,
        };
        let reason = arbitrary.string();
        let disposition = match disposition_choice {
            0 => RelayGrantDisposition::SendBody {
                grant_id: arbitrary.entropy().any_u64(),
            },
            1 => RelayGrantDisposition::BodyReceived,
            2 => RelayGrantDisposition::Admitted,
            3 => RelayGrantDisposition::Rejected(reason.clone()),
            4 => RelayGrantDisposition::Cancelled,
            _ => RelayGrantDisposition::Retired,
        };
        let status = match status_choice {
            0 => RelayAdmissionStatus::Reserved,
            1 => RelayAdmissionStatus::BodyReceived,
            2 => RelayAdmissionStatus::Admitted,
            3 => RelayAdmissionStatus::Rejected(reason.clone()),
            4 => RelayAdmissionStatus::Cancelled,
            5 => RelayAdmissionStatus::Retired,
            6 => RelayAdmissionStatus::Unknown,
            _ => RelayAdmissionStatus::Indeterminate,
        };
        let outcome = match outcome_choice {
            0 => RemoteAckOutcome::Alive,
            1 => RemoteAckOutcome::Progress {
                sequence: arbitrary.entropy().any_u64(),
                parked: arbitrary.entropy().flag(),
            },
            2 => RemoteAckOutcome::Ack,
            _ => RemoteAckOutcome::NoAck(reason),
        };
        let resolution = registration(arbitrary).resolution(outcome);
        let hello = ConnectionHello {
            fingerprint: arbitrary.digest(),
            class,
            process_epoch: arbitrary.entropy().any_u64(),
            node_id: arbitrary.rule_name::<ClusterNodeName>(),
            advertised_host: arbitrary.string(),
        };
        Self {
            payload,
            sender_epoch: arbitrary.entropy().any_u64(),
            receiver_epoch: arbitrary.entropy().any_u64(),
            disposition,
            status,
            resolution,
            hello,
        }
    }

    fn grant(&self) -> RelayGrantRequest {
        RelayGrantRequest {
            sender_epoch: self.sender_epoch,
            delivery: self.payload.delivery,
            body_bytes: u64::try_from(self.payload.batch_ipc.len()).assured("a bounded body"),
            metadata: RelayMetadata::from_payload(&self.payload),
        }
    }
}

fn delivery(entropy: &mut Entropy<'_>) -> RelayDelivery {
    let mut channel_incarnation = [0; 16];
    for byte in &mut channel_incarnation {
        *byte = entropy.byte();
    }
    RelayDelivery {
        channel_incarnation,
        sequence: entropy.any_u64(),
    }
}

fn registration(arbitrary: &mut Arbitrary<'_>) -> RemoteAckRegistration {
    RemoteAckRegistration {
        ack_id: arbitrary.entropy().any_u64(),
        registrar: arbitrary.node_identity(),
    }
}

/// A branch key as the wire carries it, or none. The wire carries whatever a peer wrote, so a
/// field may hold any name, any datetime text and any float bit pattern; the receiving runtime
/// validates the key against what a concrete branch is.
fn branch_key(arbitrary: &mut Arbitrary<'_>) -> Option<Vec<RemoteRuntimeField>> {
    if !arbitrary.entropy().flag() {
        return None;
    }
    let count = arbitrary.entropy().count(KEY_FIELDS);
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push(RemoteRuntimeField {
            name: arbitrary.string(),
            value: remote_value(arbitrary, VALUE_DEPTH),
        });
    }
    Some(fields)
}

fn remote_value(arbitrary: &mut Arbitrary<'_>, depth: u8) -> RemoteRuntimeValue {
    let collections = if depth == 0 { 0 } else { 2 };
    let choice = arbitrary.entropy().byte() % (13 + collections);
    match choice {
        0 => RemoteRuntimeValue::U8(arbitrary.entropy().byte()),
        1 => RemoteRuntimeValue::I8(i8::from_le_bytes([arbitrary.entropy().byte()])),
        2 => RemoteRuntimeValue::U16(u16::from_le_bytes(bytes(arbitrary.entropy()))),
        3 => RemoteRuntimeValue::I16(i16::from_le_bytes(bytes(arbitrary.entropy()))),
        4 => RemoteRuntimeValue::U32(u32::from_le_bytes(bytes(arbitrary.entropy()))),
        5 => RemoteRuntimeValue::I32(i32::from_le_bytes(bytes(arbitrary.entropy()))),
        6 => RemoteRuntimeValue::U64(arbitrary.entropy().any_u64()),
        7 => RemoteRuntimeValue::I64(arbitrary.entropy().any_i64()),
        8 => RemoteRuntimeValue::Bool(arbitrary.entropy().flag()),
        9 => RemoteRuntimeValue::String(arbitrary.string()),
        10 => RemoteRuntimeValue::Datetime(arbitrary.string()),
        11 => RemoteRuntimeValue::F32(f32::from_bits(u32::from_le_bytes(bytes(
            arbitrary.entropy(),
        )))),
        12 => RemoteRuntimeValue::F64(f64::from_bits(arbitrary.entropy().any_u64())),
        13 => RemoteRuntimeValue::Array(elements(arbitrary, depth)),
        _ => RemoteRuntimeValue::Vec(elements(arbitrary, depth)),
    }
}

fn elements(arbitrary: &mut Arbitrary<'_>, depth: u8) -> Vec<RemoteRuntimeElementValue> {
    let below = depth
        .checked_sub(1)
        .verified("a list is drawn only above depth zero");
    let count = arbitrary.entropy().count(ELEMENTS);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(remote_element(arbitrary, below));
    }
    values
}

fn remote_element(arbitrary: &mut Arbitrary<'_>, depth: u8) -> RemoteRuntimeElementValue {
    let collections = if depth == 0 { 0 } else { 2 };
    let choice = arbitrary.entropy().byte() % (13 + collections);
    match choice {
        0 => RemoteRuntimeElementValue::U8(arbitrary.entropy().byte()),
        1 => RemoteRuntimeElementValue::I8(i8::from_le_bytes([arbitrary.entropy().byte()])),
        2 => RemoteRuntimeElementValue::U16(u16::from_le_bytes(bytes(arbitrary.entropy()))),
        3 => RemoteRuntimeElementValue::I16(i16::from_le_bytes(bytes(arbitrary.entropy()))),
        4 => RemoteRuntimeElementValue::U32(u32::from_le_bytes(bytes(arbitrary.entropy()))),
        5 => RemoteRuntimeElementValue::I32(i32::from_le_bytes(bytes(arbitrary.entropy()))),
        6 => RemoteRuntimeElementValue::U64(arbitrary.entropy().any_u64()),
        7 => RemoteRuntimeElementValue::I64(arbitrary.entropy().any_i64()),
        8 => RemoteRuntimeElementValue::Bool(arbitrary.entropy().flag()),
        9 => RemoteRuntimeElementValue::String(arbitrary.string()),
        10 => RemoteRuntimeElementValue::Datetime(arbitrary.string()),
        11 => RemoteRuntimeElementValue::F32(f32::from_bits(u32::from_le_bytes(bytes(
            arbitrary.entropy(),
        )))),
        12 => RemoteRuntimeElementValue::F64(f64::from_bits(arbitrary.entropy().any_u64())),
        13 => RemoteRuntimeElementValue::Array(elements(arbitrary, depth)),
        _ => RemoteRuntimeElementValue::Vec(elements(arbitrary, depth)),
    }
}

fn bytes<const N: usize>(entropy: &mut Entropy<'_>) -> [u8; N] {
    std::array::from_fn(|_| entropy.byte())
}

/// Every relay delivery message reaches the receiver whole through the bounded codec, under the
/// class and limit the transport sends it with: a grant request with its delivery position, body
/// length and every branch key field, float bits included, every row's watermarks and
/// registration, and the admission; the payload the receiver rebuilds from it and the body it
/// read; and every grant reply, admission exchange, acknowledgement resolution and connection
/// binding. Nothing stays charged once the messages are dropped.
#[test]
fn bolero_relay_messages_reach_the_receiver_whole() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let case = WireCase::new(&mut arbitrary, &executor);
            runtime.block_on(async {
                let relay_limit = executor.limits().relay_encoded_bytes.as_u64();
                let management_limit = executor.limits().management_event_bytes.as_u64();
                let grant = case.grant();
                let encoded = encode_rkyv(
                    &executor,
                    MemoryClass::Relay,
                    CpuClass::Data,
                    relay_limit,
                    grant.clone(),
                )
                .await
                .assured("a bounded grant request encodes");
                let decoded = decode_rkyv::<RelayGrantRequest>(
                    &executor,
                    MemoryClass::Relay,
                    CpuClass::Data,
                    encoded,
                )
                .await
                .assured("an encoded grant request decodes")
                .into_value();
                assert_eq!(decoded.sender_epoch, grant.sender_epoch);
                assert_eq!(decoded.delivery, grant.delivery);
                assert_eq!(decoded.body_bytes, grant.body_bytes);
                assert_same_metadata(&decoded.metadata, &grant.metadata);
                let rebuilt = decoded
                    .metadata
                    .into_payload(decoded.delivery, case.payload.batch_ipc.clone());
                assert_same_payload(&rebuilt, &case.payload);

                let response = RelayGrantResponse {
                    receiver_epoch: case.receiver_epoch,
                    disposition: case.disposition.clone(),
                };
                assert_eq!(
                    management_round_trip(&executor, management_limit, response.clone()).await,
                    response
                );
                let admission_request = RelayAdmissionRequest {
                    sender_epoch: case.sender_epoch,
                    receiver_epoch: case.receiver_epoch,
                    delivery: case.payload.delivery,
                };
                assert_eq!(
                    management_round_trip(&executor, management_limit, admission_request.clone())
                        .await,
                    admission_request
                );
                let admission_response = RelayAdmissionResponse {
                    receiver_epoch: case.receiver_epoch,
                    status: case.status.clone(),
                };
                assert_eq!(
                    management_round_trip(&executor, management_limit, admission_response.clone())
                        .await,
                    admission_response
                );
                assert_eq!(
                    management_round_trip(&executor, management_limit, case.resolution.clone())
                        .await,
                    case.resolution
                );
                assert_eq!(
                    management_round_trip(&executor, management_limit, case.hello.clone()).await,
                    case.hello
                );
                let accepted = ConnectionAccepted {
                    fingerprint: case.hello.fingerprint,
                    process_epoch: case.hello.process_epoch,
                    node_id: case.hello.node_id.clone(),
                };
                assert_eq!(
                    management_round_trip(&executor, management_limit, accepted.clone()).await,
                    accepted
                );
            });
            drop(case);
            let snapshot = executor.snapshot();
            assert_eq!(
                snapshot.relay_memory.reserved_bytes, 0,
                "no relay charge outlives its message"
            );
            assert_eq!(
                snapshot.management_memory.reserved_bytes, 0,
                "no management charge outlives its message"
            );
        });
}

async fn management_round_trip<T>(executor: &Executor, limit: u64, value: T) -> T
where
    T: rkyv::Archive
        + Send
        + 'static
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::ser::writer::IoWriter<nervix_execution::BudgetedBuffer>,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let encoded = encode_rkyv(
        executor,
        MemoryClass::Management,
        CpuClass::Control,
        limit,
        value,
    )
    .await
    .assured("a bounded management message encodes");
    decode_rkyv::<T>(
        executor,
        MemoryClass::Management,
        CpuClass::Control,
        encoded,
    )
    .await
    .assured("an encoded management message decodes")
    .into_value()
}

/// Which message damaged bytes are read as.
#[derive(Debug, Clone, Copy)]
enum ReadAs {
    GrantRequest,
    GrantResponse,
    AdmissionRequest,
    AdmissionResponse,
    Resolution,
    Hello,
}

impl ReadAs {
    const ALL: [Self; 6] = [
        Self::GrantRequest,
        Self::GrantResponse,
        Self::AdmissionRequest,
        Self::AdmissionResponse,
        Self::Resolution,
        Self::Hello,
    ];
}

/// What a damage does to a message's bytes.
#[derive(Debug, Clone, Copy)]
enum MessageDamageKind {
    /// The bytes end early.
    Truncate,
    /// One bit is flipped.
    FlipBit,
    /// One byte holds another value.
    SetByte,
    /// The bytes are arbitrary.
    Arbitrary,
}

/// How damaged bytes differ from a message the codec wrote. It is drawn whole before the message,
/// with its place as a share of the message's length, so that an input that runs out while the
/// message is generated still damages it somewhere.
#[derive(Debug, Clone)]
struct MessageDamage {
    kind: MessageDamageKind,
    /// Where the damage lands, as a share of the message's bytes out of 65536.
    place: u16,
    /// Which bit of the byte at that place a flip changes.
    bit: u8,
    /// The value a set byte holds.
    byte: u8,
    /// The bytes arbitrary damage replaces the message with.
    bytes: Vec<u8>,
}

impl MessageDamage {
    /// The most bytes arbitrary damage replaces a message with.
    const ARBITRARY_BYTES: usize = 256;

    fn draw(entropy: &mut Entropy<'_>) -> Self {
        let kind = entropy.pick([
            MessageDamageKind::Truncate,
            MessageDamageKind::FlipBit,
            MessageDamageKind::SetByte,
            MessageDamageKind::Arbitrary,
        ]);
        let place = u16::from_le_bytes(bytes(entropy));
        let bit = entropy.byte() % 8;
        let byte = entropy.byte();
        let length = match kind {
            MessageDamageKind::Arbitrary => entropy.count(Self::ARBITRARY_BYTES),
            MessageDamageKind::Truncate
            | MessageDamageKind::FlipBit
            | MessageDamageKind::SetByte => 0,
        };
        let replacement = (0..length).map(|_| entropy.byte()).collect();
        Self {
            kind,
            place,
            bit,
            byte,
            bytes: replacement,
        }
    }

    /// `message` with this damage applied.
    fn apply(&self, mut message: Vec<u8>) -> Vec<u8> {
        let position = self.place_in(message.len());
        match self.kind {
            MessageDamageKind::Truncate => message.truncate(position),
            MessageDamageKind::FlipBit => {
                if let Some(byte) = message.get_mut(position) {
                    *byte ^= 1 << self.bit;
                }
            }
            MessageDamageKind::SetByte => {
                if let Some(byte) = message.get_mut(position) {
                    *byte = self.byte;
                }
            }
            MessageDamageKind::Arbitrary => message = self.bytes.clone(),
        }
        message
    }

    /// The byte of a message `length` bytes long this damage lands at: its share of them, which
    /// is below `length` for every message that has a byte.
    fn place_in(&self, length: usize) -> usize {
        let length = u64::try_from(length).assured("supported targets address 64 bits");
        let scaled = u64::from(self.place)
            .checked_mul(length)
            .assured("a share of a message length in memory fits in 64 bits")
            / 65_536;
        usize::try_from(scaled).verified("a share of the bytes is below their count")
    }
}

/// Decodes `bytes` as a `T` on this thread, drops what the decoder returns, and asserts that the
/// decoder freed every allocation it made: a message the codec refuses leaves nothing behind, and
/// neither does one it decodes.
fn assert_decoding_frees_its_allocations<T>(bytes: &[u8], max_depth: NonZeroUsize)
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    // The first decode pays for whatever the codec and its failure report set up once.
    drop(decode_aligned::<T>(&aligned, max_depth));
    let before = alloc_count::stats();
    drop(decode_aligned::<T>(&aligned, max_depth));
    let after = alloc_count::stats();
    let allocated = after
        .alloc_calls
        .checked_sub(before.alloc_calls)
        .assured("a thread's allocation count only grows");
    let freed = after
        .dealloc_calls
        .checked_sub(before.dealloc_calls)
        .assured("a thread's deallocation count only grows");
    assert_eq!(
        freed, allocated,
        "the decoder frees every allocation it made"
    );
}

/// A grant request is refused when the registrar of one of its acknowledgement registrations is
/// no node's name, and the registrations read before that one are freed with the list that held
/// them. A registrar's name is checked when it is read back, after the list was allocated.
#[test]
fn a_grant_request_refused_for_a_registrar_frees_the_registrations_read_before_it() {
    let registered_by = |ack_id: u64, registrar: &str| RemoteAckRegistration {
        ack_id,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse(registrar).assured("a literal node name"),
            ClusterNodeIncarnation::new(1),
        ),
    };
    let refused_registrar = "registrar-no-node-is-named-as";
    let request = RelayGrantRequest {
        sender_epoch: 1,
        delivery: RelayDelivery {
            channel_incarnation: [7; 16],
            sequence: 3,
        },
        body_bytes: 0,
        metadata: RelayMetadata {
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("orders").assured("a literal domain name"),
            relay: RelayName::parse("accepted").assured("a literal relay name"),
            key: None,
            metadata: Vec::new(),
            acks: vec![
                Some(registered_by(1, "registrar-read-before-the-refused-one")),
                None,
                Some(registered_by(2, refused_registrar)),
            ],
            admission: None,
        },
    };
    let mut bytes = rkyv::to_bytes::<RkyvError>(&request).assured("a small message encodes");
    // The archive holds the name's text once, and a name holds no exclamation mark.
    let position = bytes
        .windows(refused_registrar.len())
        .position(|window| window == refused_registrar.as_bytes())
        .assured("the archive holds the registrar's name as its text");
    bytes[position] = b'!';
    let max_depth = validation_depth(&Executor::default()).assured("a positive decoder depth");

    let refused = decode_aligned::<RelayGrantRequest>(&bytes, max_depth)
        .expect_err("no node is named with an exclamation mark");
    assert!(
        matches!(refused.current_context(), crate::TransportError::Decode(_)),
        "the request fails with the codec's decode failure: {refused:?}"
    );
    drop(refused);
    assert_decoding_frees_its_allocations::<RelayGrantRequest>(&bytes, max_depth);
}

/// A plain archived vector must release its earlier elements when a later name is refused.
/// This exercises rkyv's shared list reader, which is used by the other archive boundaries.
#[test]
fn a_refused_name_in_a_plain_archived_vector_frees_earlier_elements() {
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct Registrations {
        list: Vec<RemoteAckRegistration>,
    }

    let registration = |ack_id, registrar: &str| RemoteAckRegistration {
        ack_id,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse(registrar).assured("a literal node name"),
            ClusterNodeIncarnation::new(1),
        ),
    };
    let refused_name = "registrar-no-node-is-named-as";
    let mut bytes = rkyv::to_bytes::<RkyvError>(&Registrations {
        list: vec![
            registration(1, "registrar-read-before-the-refused-one"),
            registration(2, refused_name),
        ],
    })
    .assured("a small list encodes");
    let position = bytes
        .windows(refused_name.len())
        .position(|window| window == refused_name.as_bytes())
        .assured("the archive holds the registrar's name as its text");
    bytes[position] = b'!';
    let max_depth = validation_depth(&Executor::default()).assured("a positive decoder depth");
    assert_decoding_frees_its_allocations::<Registrations>(&bytes, max_depth);
}

/// Boxed and shared slices use the same partial slice reader but own their
/// allocations through different paths. Both must release the initialized
/// prefix and the outer allocation when a later name is refused.
#[test]
fn a_refused_name_in_boxed_and_shared_slices_frees_every_allocation() {
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct Boxed {
        list: Box<[RemoteAckRegistration]>,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct Shared {
        list: StdArc<[RemoteAckRegistration]>,
    }

    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    struct Deque {
        list: VecDeque<RemoteAckRegistration>,
    }

    let registration = |ack_id, registrar: &str| RemoteAckRegistration {
        ack_id,
        registrar: ClusterNodeIdentity::new(
            ClusterNodeName::parse(registrar).assured("a literal node name"),
            ClusterNodeIncarnation::new(1),
        ),
    };
    let refused_name = "registrar-no-node-is-named-as";
    let registrations = vec![
        registration(1, "registrar-read-before-the-refused-one"),
        registration(2, refused_name),
    ];
    let mut boxed = rkyv::to_bytes::<RkyvError>(&Boxed {
        list: registrations.clone().into_boxed_slice(),
    })
    .assured("a small boxed list encodes");
    let mut shared = rkyv::to_bytes::<RkyvError>(&Shared {
        list: StdArc::from(registrations.clone().into_boxed_slice()),
    })
    .assured("a small shared list encodes");
    let mut deque = rkyv::to_bytes::<RkyvError>(&Deque {
        list: VecDeque::from(registrations),
    })
    .assured("a small deque encodes");
    for bytes in [&mut boxed, &mut shared, &mut deque] {
        let position = bytes
            .windows(refused_name.len())
            .position(|window| window == refused_name.as_bytes())
            .assured("the archive holds the registrar's name as its text");
        bytes[position] = b'!';
    }
    let max_depth = validation_depth(&Executor::default()).assured("a positive decoder depth");
    assert_decoding_frees_its_allocations::<Boxed>(&boxed, max_depth);
    assert_decoding_frees_its_allocations::<Shared>(&shared, max_depth);
    assert_decoding_frees_its_allocations::<Deque>(&deque, max_depth);
}

/// Damaged encodings of relay messages, and arbitrary bytes, either fail with the codec's typed
/// decode failure, within the decoder's depth bound and without leaving a charge or an allocation
/// behind, or decode to a message that encodes and decodes back to itself.
#[test]
fn bolero_damaged_relay_messages_fail_typed_or_decode_to_a_message() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // Which message is damaged and how is read before the messages are generated.
            let read_as = arbitrary.entropy().pick(ReadAs::ALL);
            let damage = MessageDamage::draw(arbitrary.entropy());
            let case = WireCase::new(&mut arbitrary, &executor);
            runtime.block_on(async {
                let limit = executor.limits().relay_encoded_bytes.as_u64();
                let valid = match read_as {
                    ReadAs::GrantRequest => {
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            case.grant(),
                        )
                        .await
                    }
                    ReadAs::GrantResponse => {
                        let response = RelayGrantResponse {
                            receiver_epoch: case.receiver_epoch,
                            disposition: case.disposition.clone(),
                        };
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            response,
                        )
                        .await
                    }
                    ReadAs::AdmissionRequest => {
                        let request = RelayAdmissionRequest {
                            sender_epoch: case.sender_epoch,
                            receiver_epoch: case.receiver_epoch,
                            delivery: case.payload.delivery,
                        };
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            request,
                        )
                        .await
                    }
                    ReadAs::AdmissionResponse => {
                        let response = RelayAdmissionResponse {
                            receiver_epoch: case.receiver_epoch,
                            status: case.status.clone(),
                        };
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            response,
                        )
                        .await
                    }
                    ReadAs::Resolution => {
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            case.resolution.clone(),
                        )
                        .await
                    }
                    ReadAs::Hello => {
                        encode_rkyv(
                            &executor,
                            MemoryClass::Relay,
                            CpuClass::Data,
                            limit,
                            case.hello.clone(),
                        )
                        .await
                    }
                }
                .assured("a bounded message encodes");
                let damaged_bytes = damage.apply(valid.to_vec());
                drop(valid);
                let damaged = executor
                    .try_charge_owned(MemoryClass::Relay, damaged_bytes)
                    .assured("a bounded message fits the relay budget");
                match read_as {
                    ReadAs::GrantRequest => {
                        check_damaged::<RelayGrantRequest>(&executor, damaged, |decoded, again| {
                            assert_eq!(decoded.sender_epoch, again.sender_epoch);
                            assert_eq!(decoded.delivery, again.delivery);
                            assert_eq!(decoded.body_bytes, again.body_bytes);
                            assert_same_metadata(&again.metadata, &decoded.metadata);
                        })
                        .await;
                    }
                    ReadAs::GrantResponse => {
                        check_damaged::<RelayGrantResponse>(
                            &executor,
                            damaged,
                            |decoded, again| {
                                assert_eq!(again, decoded);
                            },
                        )
                        .await;
                    }
                    ReadAs::AdmissionRequest => {
                        check_damaged::<RelayAdmissionRequest>(
                            &executor,
                            damaged,
                            |decoded, again| {
                                assert_eq!(again, decoded);
                            },
                        )
                        .await;
                    }
                    ReadAs::AdmissionResponse => {
                        check_damaged::<RelayAdmissionResponse>(
                            &executor,
                            damaged,
                            |decoded, again| {
                                assert_eq!(again, decoded);
                            },
                        )
                        .await;
                    }
                    ReadAs::Resolution => {
                        check_damaged::<RemoteAckResolution>(
                            &executor,
                            damaged,
                            |decoded, again| {
                                assert_eq!(again.registration, decoded.registration);
                                assert_eq!(again.outcome, decoded.outcome);
                            },
                        )
                        .await;
                    }
                    ReadAs::Hello => {
                        check_damaged::<ConnectionHello>(&executor, damaged, |decoded, again| {
                            assert_eq!(again, decoded);
                        })
                        .await;
                    }
                }
            });
            drop(case);
            let snapshot = executor.snapshot();
            assert_eq!(
                snapshot.relay_memory.reserved_bytes, 0,
                "no relay charge outlives its bytes"
            );
        });
}

async fn check_damaged<T>(executor: &Executor, damaged: ChargedBytes, same: impl Fn(&T, &T))
where
    T: rkyv::Archive
        + Clone
        + Send
        + 'static
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::ser::writer::IoWriter<nervix_execution::BudgetedBuffer>,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let max_depth = validation_depth(executor).assured("a positive decoder depth");
    assert_decoding_frees_its_allocations::<T>(damaged.as_ref(), max_depth);
    let decoded =
        match decode_rkyv::<T>(executor, MemoryClass::Relay, CpuClass::Data, damaged).await {
            Ok(decoded) => decoded.into_value(),
            Err(refused) => {
                assert!(
                    matches!(refused.current_context(), crate::TransportError::Decode(_)),
                    "damaged bytes fail with the codec's decode failure: {refused:?}"
                );
                return;
            }
        };
    let limit = executor.limits().relay_encoded_bytes.as_u64();
    let encoded = encode_rkyv(
        executor,
        MemoryClass::Relay,
        CpuClass::Data,
        limit,
        decoded.clone(),
    )
    .await
    .assured("a decoded message encodes again");
    let again = decode_rkyv::<T>(executor, MemoryClass::Relay, CpuClass::Data, encoded)
        .await
        .assured("a re-encoded message decodes")
        .into_value();
    same(&decoded, &again);
}
