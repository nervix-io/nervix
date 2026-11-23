//! Bounded rkyv payload encoding for native HTTP/2 interconnect streams.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Charging, size limits, validation, and rkyv encoding for typed stream payloads.
//! - **Depends on.** The execution budget and the interconnect message vocabulary.
//! - **Must not know.** HTTP/2 connection lifecycle, peer selection, or runtime graph semantics.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{num::NonZeroUsize, time::Duration};

use arch_into::ArchInto as _;
use error_stack::Report;
use nervix_execution::{
    AdmissionError, BudgetedBuffer, Cancellation, ChargedBytes, CpuClass, Executor, MemoryClass,
    QueueAdmission, Reservation,
};
use nervix_models::{
    ClusterNodeName, NodePrerequisites, PrerequisiteMismatch, WireContractFingerprint,
};
use nervix_primitives::time::Instant;
use rkyv::{
    Archive, Deserialize, Serialize,
    api::{access_with_context, deserialize_using, high::HighSerializer},
    de::pooling::Pool,
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::IoWriter},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use super::{
    PoolClass, RelayAdmissionStatus, RelayDelivery, RelayGrantRefusal, RelayPayload,
    RelayPayloadKind, TransportError,
};

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "typed message encoding starts with one 4 KiB accounting granule before charging \
                  its exact encoded size"
    )
)]
const INITIAL_MESSAGE_CHARGE: u64 = 4 * 1024;

/// Changes whenever the one supported interconnect contract changes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the fingerprint names the one wire contract this build speaks; peers compare it \
                  for equality in every connection handshake"
    )
)]
pub const WIRE_CONTRACT: WireContractFingerprint = WireContractFingerprint::from_digest([
    0xd0, 0xa9, 0x8b, 0x97, 0x54, 0xe0, 0x45, 0x75, 0x5a, 0xa8, 0x6b, 0xbf, 0xac, 0xad, 0x7f, 0x6b,
    0x69, 0x42, 0xa1, 0x92, 0x76, 0x6a, 0xb7, 0x0e, 0xbb, 0x62, 0x29, 0x10, 0x82, 0x1c, 0x49, 0xc1,
]);

/// The first message of every connection: who dials, for which pool, and everything the dialing
/// node brings to the cluster.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionHello {
    pub(crate) class: PoolClass,
    pub(crate) process_epoch: u64,
    pub(crate) node_id: ClusterNodeName,
    pub(crate) advertised_host: String,
    pub(crate) prerequisites: NodePrerequisites,
}

/// The accepting node's answer to a hello.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ConnectionAnswer {
    /// The connection is bound, and the accepting node brings these prerequisites.
    Accepted(ConnectionAccepted),
    /// The accepting node cannot run beside the dialing one. The mismatch is seen from the accepting
    /// node: its own value is the local one.
    Refused { mismatch: PrerequisiteMismatch },
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionAccepted {
    pub(crate) process_epoch: u64,
    pub(crate) node_id: ClusterNodeName,
    pub(crate) prerequisites: NodePrerequisites,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub(crate) struct RelayGrantRequest {
    pub(crate) sender_epoch: u64,
    pub(crate) delivery: RelayDelivery,
    pub(crate) body_bytes: u64,
    pub(crate) metadata: RelayMetadata,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RelayGrantResponse {
    pub(crate) receiver_epoch: u64,
    pub(crate) disposition: RelayGrantDisposition,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RelayGrantDisposition {
    Resize(nervix_models::PeerReceiveBounds),
    SendBody { grant_id: u64 },
    BodyReceived,
    Admitted,
    Rejected(String),
    Cancelled,
    Retired,
    Refused(RelayGrantRefusal),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RelayAdmissionRequest {
    pub(crate) sender_epoch: u64,
    pub(crate) receiver_epoch: u64,
    pub(crate) delivery: RelayDelivery,
    /// The timing the delivery's sender captured. A cancellation that fences a position its
    /// receiver never granted keeps the watermark it records for that delivery's retention.
    pub(crate) ack_timing: nervix_models::RemoteAckTiming,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RelayAdmissionResponse {
    pub(crate) receiver_epoch: u64,
    pub(crate) status: RelayAdmissionStatus,
}

/// The outcome of an operation charged only to room its memory class had free, which never waits
/// in the class's line.
pub(crate) enum FreeRoom<T> {
    /// The class had room, and this holds what was charged to it.
    Taken(T),
    /// The class had no free room. Nothing was charged and nothing is held.
    NoRoom,
}

/// A decoded value together with the charge that covers its retained allocations.
pub(crate) struct Decoded<T> {
    value: T,
    _reservation: Reservation,
}

/// An owned rkyv payload with the charge that covers its bytes through encoding or decoding.
pub(crate) struct EncodedPayload {
    bytes: Vec<u8>,
    reservation: Reservation,
}

impl EncodedPayload {
    pub(crate) fn into_parts(self) -> (Vec<u8>, Reservation) {
        (self.bytes, self.reservation)
    }
}

/// Only owned carriers whose bytes retain their reservation may enter a decoding job.
trait ChargedRkyvInput: Send + 'static {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            recurring,
            reason = "borrows the owned input's byte slice without allocation or synchronization"
        )
    )]
    fn as_bytes(&self) -> &[u8];
}

impl ChargedRkyvInput for ChargedBytes {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl ChargedRkyvInput for EncodedPayload {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl<T> Decoded<T> {
    pub(crate) fn into_parts(self) -> (T, Reservation) {
        (self.value, self._reservation)
    }

    pub(crate) fn into_value(self) -> T {
        self.value
    }
}

/// The rkyv metadata admitted before an Arrow relay body stream starts.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub(crate) struct RelayMetadata {
    pub(crate) ack_timing: nervix_models::RemoteAckTiming,
    pub(crate) decoded_bytes: u64,
    pub(crate) kind: RelayPayloadKind,
    pub(crate) domain: nervix_models::DomainName,
    pub(crate) relay: nervix_models::RelayName,
    pub(crate) unit: nervix_models::Unit,
    pub(crate) key: nervix_models::RemoteBranchKey,
    pub(crate) metadata: Vec<nervix_models::RemoteRuntimeRecordMetadata>,
    pub(crate) acks: Vec<Option<nervix_models::RemoteAckRegistration>>,
    pub(crate) admission: Option<nervix_models::RemoteAckRegistration>,
}

impl RelayMetadata {
    pub(crate) fn from_payload(payload: &RelayPayload) -> Self {
        Self {
            ack_timing: payload.ack_timing,
            decoded_bytes: payload.decoded_bytes,
            kind: payload.kind,
            domain: payload.domain.clone(),
            relay: payload.relay.clone(),
            unit: payload.unit,
            key: payload.key.clone(),
            metadata: payload.metadata.clone(),
            acks: payload.acks.clone(),
            admission: payload.admission.clone(),
        }
    }

    pub(crate) fn into_payload(
        self,
        delivery: RelayDelivery,
        batch_ipc: ChargedBytes,
    ) -> RelayPayload {
        RelayPayload {
            ack_timing: self.ack_timing,
            decoded_bytes: self.decoded_bytes,
            delivery,
            kind: self.kind,
            domain: self.domain,
            relay: self.relay,
            unit: self.unit,
            key: self.key,
            batch_ipc,
            metadata: self.metadata,
            acks: self.acks,
            admission: self.admission,
        }
    }
}

/// The encoding job owns the input and its optional charge together. Declaration order returns
/// the charge after the input is dropped, including when a queued job is cancelled.
struct RkyvEncoding<T> {
    value: T,
    _input_charge: Option<Reservation>,
}

impl<T> RkyvEncoding<T>
where
    T: for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    fn encode(
        self,
        charge: Reservation,
        limit: u64,
        cancellation: &Cancellation,
    ) -> Result<BudgetedBuffer, Report<TransportError>> {
        cancellation.check().map_err(|error| {
            TransportError::with_cause(Report::new(error), TransportError::Encode)
        })?;
        let writer = IoWriter::new(BudgetedBuffer::with_limit(charge, limit));
        let writer =
            rkyv::api::high::to_bytes_in::<_, RkyvError>(&self.value, writer).map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Encode)
            })?;
        Ok(writer.into_inner())
    }
}

async fn encode_rkyv_buffer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    admission: QueueAdmission,
    value: T,
) -> Result<BudgetedBuffer, Report<TransportError>>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let reservation = executor
        .reserve(class, INITIAL_MESSAGE_CHARGE.min(limit))
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))?;
    let encoding = RkyvEncoding {
        value,
        _input_charge: None,
    };
    executor
        .run_cpu_with(cpu, admission, reservation, move |charge, cancellation| {
            encoding.encode(charge, limit, cancellation)
        })
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))?
}

/// Encode an envelope while its nested payload remains charged. Refuse overlapping admission
/// instead of queueing behind a charge that needs the retained payload to be released.
pub(crate) async fn encode_stream_request(
    executor: &Executor,
    envelope: crate::request::StreamRequestEnvelope,
    payload_charge: Reservation,
) -> Result<ChargedBytes, Report<TransportError>> {
    let encoding = RkyvEncoding {
        value: envelope,
        _input_charge: Some(payload_charge),
    };
    let class = encoding.value.request.class;
    let limit = class.control_body_limit(executor);
    let reservation = executor
        .try_reserve(class.memory_class(), INITIAL_MESSAGE_CHARGE.min(limit))
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))?;
    let buffer = executor
        .run_cpu(
            class.cpu_class(),
            reservation,
            move |charge, cancellation| encoding.encode(charge, limit, cancellation),
        )
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))??;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// Serialize one typed value directly into a budgeted writer on the owning CPU class.
pub(crate) async fn encode_rkyv<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
) -> Result<ChargedBytes, Report<TransportError>>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    encode_rkyv_with(
        executor,
        class,
        cpu,
        limit,
        QueueAdmission::RefuseWhenFull,
        value,
    )
    .await
}

/// Serialize one typed value as [`encode_rkyv`] does, its job taking a place on the CPU class as
/// `admission` says.
///
/// [`encode_rkyv`] refuses at once when the class has no place, which suits a request: its caller
/// still holds it and is told to retry, and a burst beyond the class keeps no charge. A response
/// carries work the handler already did, so it waits for a place instead of losing that work.
pub(crate) async fn encode_rkyv_with<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    admission: QueueAdmission,
    value: T,
) -> Result<ChargedBytes, Report<TransportError>>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, admission, value).await?;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// What became of an answer [`encode_answer`] was asked to encode.
pub(crate) enum AnswerEncoding {
    /// The answer is encoded and ready to send.
    Encoded(ChargedBytes),
    /// The caller ended the exchange while the encoding waited for a place. No answer can reach it
    /// any more, and the encoding gave back its place in line and its charge.
    Ended,
}

/// Serialize the answer to a request whose handler already ran, as [`encode_rkyv_with`] does.
///
/// The answer carries work the handler already did, so a class whose queue is full holds the
/// encoding back until a place frees rather than throwing that work away. `ended` resolves once
/// the caller ends the exchange, by resetting its stream or closing its connection, and that ends
/// the wait: the encoding is dropped with its place and its charge, and [`AnswerEncoding::Ended`]
/// says nobody is left to answer. A failure `ended` reports instead is returned as it is.
pub(crate) async fn encode_answer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
    ended: impl Future<Output = Result<(), Report<TransportError>>>,
) -> Result<AnswerEncoding, Report<TransportError>>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let encoding = encode_rkyv_with(
        executor,
        class,
        cpu,
        limit,
        QueueAdmission::WaitForPlace,
        value,
    );
    nervix_primitives::select! {
        encoded = encoding => Ok(AnswerEncoding::Encoded(encoded?)),
        ended = ended => {
            ended?;
            Ok(AnswerEncoding::Ended)
        }
    }
}

/// When the request whose answer [`decode_answer`] decodes stops waiting for it.
pub(crate) struct AnswerDeadline<'a> {
    /// The node that answered.
    pub(crate) peer: &'a ClusterNodeName,
    /// The request's own deadline.
    pub(crate) at: Instant,
    /// The whole time the request was given, which a timeout reports.
    pub(crate) timeout: Duration,
}

/// Validate and deserialize the answer to a request this node sent, as [`decode_rkyv_with`] does.
///
/// The answer carries work its peer already did, so a class whose queue is full holds the decoding
/// back until a place frees rather than throwing that work away, but no longer than the request's
/// own deadline. Once the deadline passes the decoding is dropped with its place and its charge,
/// and the request fails with [`TransportError::RequestTimeout`], as it would had the answer
/// arrived late.
pub(crate) async fn decode_answer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    bytes: ChargedBytes,
    deadline: AnswerDeadline<'_>,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let decoding = decode_rkyv_with(executor, class, cpu, QueueAdmission::WaitForPlace, bytes);
    match nervix_primitives::time::timeout_at(deadline.at, decoding).await {
        Ok(decoded) => decoded,
        Err(_) => Err(Report::new(TransportError::RequestTimeout {
            peer: deadline.peer.clone(),
            timeout: deadline.timeout,
        })),
    }
}

/// Serialize a nested typed-request value while retaining its charge beside the resulting Vec, its
/// job taking a place on the CPU class as `admission` says (see [`encode_rkyv_with`]).
pub(crate) async fn encode_rkyv_payload<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    admission: QueueAdmission,
    value: T,
) -> Result<EncodedPayload, Report<TransportError>>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, admission, value).await?;
    let (bytes, reservation) = buffer.into_parts();
    Ok(EncodedPayload { bytes, reservation })
}

/// Validate and deserialize one bounded rkyv payload off the async workers. Its input already
/// holds memory, so additional admission must refuse rather than queue behind that input.
pub(crate) async fn decode_rkyv<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    bytes: ChargedBytes,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    decode_rkyv_with(executor, class, cpu, QueueAdmission::RefuseWhenFull, bytes).await
}

/// Validate and deserialize one bounded rkyv payload as [`decode_rkyv`] does, its job taking a
/// place on the CPU class as `admission` says. The memory charges still refuse at once; only the
/// place on the class's queue is waited for (see [`encode_rkyv_with`]).
pub(crate) async fn decode_rkyv_with<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    admission: QueueAdmission,
    bytes: ChargedBytes,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let decoded_charge = executor
        .try_reserve(class, bytes.len().arch_into())
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    decode_rkyv_charged(executor, cpu, admission, bytes, decoded_charge).await
}

/// Validate and deserialize one bounded rkyv payload as [`decode_rkyv`] does, but charge the
/// decoded value only to room `class` has free now. A class without room is reported rather than
/// waited for, so a request whose answer is that the class is full never waits on that class.
pub(crate) async fn decode_rkyv_in_free_room<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    bytes: ChargedBytes,
) -> Result<FreeRoom<Decoded<T>>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let decoded_charge = match executor.try_reserve(class, bytes.len().arch_into()) {
        Ok(charge) => charge,
        Err(refusal) => {
            if let AdmissionError::BudgetExhausted { .. } = refusal.current_context() {
                return Ok(FreeRoom::NoRoom);
            }
            return Err(TransportError::with_cause(refusal, TransportError::Decode));
        }
    };
    let decoded = decode_rkyv_charged(
        executor,
        cpu,
        QueueAdmission::RefuseWhenFull,
        bytes,
        decoded_charge,
    )
    .await?;
    Ok(FreeRoom::Taken(decoded))
}

/// Validate and deserialize one bounded rkyv payload under `decoded_charge`, which covers the
/// decoded value for as long as it is retained.
async fn decode_rkyv_charged<T>(
    executor: &Executor,
    cpu: CpuClass,
    admission: QueueAdmission,
    bytes: impl ChargedRkyvInput,
    decoded_charge: Reservation,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let max_depth = validation_depth(executor)?;
    executor
        .run_cpu_with(
            cpu,
            admission,
            decoded_charge,
            move |charge, cancellation| {
                cancellation.check().map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Decode)
                })?;
                let bytes = bytes.as_bytes();
                let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
                aligned.extend_from_slice(bytes);
                let value = decode_aligned::<T>(&aligned, max_depth)?;
                Ok(Decoded {
                    value,
                    _reservation: charge,
                })
            },
        )
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?
}

/// Validate and deserialize a nested typed-request payload while its outer envelope remains
/// charged by the caller. The job takes its own input charge because cancellation can end that
/// caller before a submitted CPU job exits. Both overlapping charges refuse without queueing.
pub(crate) async fn decode_rkyv_payload<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    bytes: Vec<u8>,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    decode_rkyv_payload_with(executor, class, cpu, QueueAdmission::RefuseWhenFull, bytes).await
}

/// Validate and deserialize a nested typed-request payload as [`decode_rkyv_payload`] does, its job
/// taking a place on the CPU class as `admission` says (see [`decode_rkyv_with`]).
pub(crate) async fn decode_rkyv_payload_with<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    admission: QueueAdmission,
    bytes: Vec<u8>,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let reservation = executor
        .try_reserve(class, bytes.len().arch_into())
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let bytes = EncodedPayload { bytes, reservation };
    let decoded_charge = executor
        .try_reserve(class, bytes.as_bytes().len().arch_into())
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    decode_rkyv_charged(executor, cpu, admission, bytes, decoded_charge).await
}

fn validation_depth(executor: &Executor) -> Result<NonZeroUsize, Report<TransportError>> {
    let depth = usize::try_from(executor.limits().decoder_depth.get())
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?;
    let depth = NonZeroUsize::new(depth)
        .ok_or_else(|| TransportError::Decode("decoder depth must be nonzero".to_string()))?;
    Ok(depth)
}

fn decode_aligned<T>(bytes: &[u8], max_depth: NonZeroUsize) -> Result<T, Report<TransportError>>
where
    T: Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let mut validator = Validator::new(
        ArchiveValidator::with_max_depth(bytes, Some(max_depth)),
        SharedValidator::new(),
    );
    let archived = access_with_context::<T::Archived, _, RkyvError>(bytes, &mut validator)
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?;
    let mut deserializer = Pool::default();
    let value = deserialize_using::<T, _, RkyvError>(archived, &mut deserializer)
        .map_err(|error| TransportError::with_cause(Report::new(error), TransportError::Decode))?;
    Ok(value)
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
#[path = "wire_properties.rs"]
mod properties;

#[cfg(all(test, not(feature = "turmoil")))]
#[path = "wire_admission_tests.rs"]
mod admission_tests;

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
#[path = "wire_admission_properties.rs"]
mod admission_properties;

#[cfg(all(test, feature = "turmoil"))]
mod simulation_checks {
    use std::{
        io,
        num::NonZeroUsize,
        time::{Duration, SystemTime},
    };

    use meticulous::OptionExt as _;
    use nervix_execution::{CpuClass, Executor, MemoryClass};

    use super::{RelayGrantDisposition, RelayGrantResponse, decode_rkyv, encode_rkyv};
    use crate::simulation_runner::{
        HostSupervisor, NetworkParameters, SimulationBounds, SimulationConfig, Topology,
    };

    #[test]
    fn seeded_wire_round_trips_use_the_execution_owner() {
        for seed in 1..=12 {
            let topology = if seed % 2 == 0 {
                Topology::Ipv6
            } else {
                Topology::Ipv4
            };
            let configuration = SimulationConfig {
                seed,
                epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
                topology,
                network: NetworkParameters::LOSSLESS,
                bounds: SimulationBounds {
                    simulated_duration: Duration::from_secs(1),
                    tick: Duration::from_millis(1),
                    max_steps: NonZeroUsize::new(200).assured("200 is nonzero"),
                    wall_duration: Duration::from_secs(3),
                },
            };
            let result = configuration.run("wire round trip", |simulation| {
                simulation.host("worker", || async {
                    HostSupervisor::run(async {
                        let executor = Executor::default();
                        let response = RelayGrantResponse {
                            receiver_epoch: 41,
                            disposition: RelayGrantDisposition::BodyReceived,
                        };
                        let bytes = encode_rkyv(
                            &executor,
                            MemoryClass::Management,
                            CpuClass::Control,
                            4096,
                            response.clone(),
                        )
                        .await
                        .map_err(|error| io::Error::other(format!("{error:?}")))?;
                        let decoded = decode_rkyv::<RelayGrantResponse>(
                            &executor,
                            MemoryClass::Management,
                            CpuClass::Control,
                            bytes,
                        )
                        .await
                        .map_err(|error| io::Error::other(format!("{error:?}")))?;
                        assert_eq!(decoded.into_value(), response);

                        let snapshot = executor.snapshot();
                        assert_eq!(snapshot.control_cpu.admitted, 2);
                        assert_eq!(snapshot.control_cpu.completed, 2);
                        assert_eq!(snapshot.control_cpu.running, 0);
                        assert_eq!(snapshot.management_memory.reserved_bytes, 0);
                        Ok::<(), io::Error>(())
                    })
                    .await
                });
            });
            assert!(result.is_ok(), "seed {seed}: {result:?}");
        }
    }
}
