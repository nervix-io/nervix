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
    Attempt, BudgetedBuffer, Cancellation, ChargedBytes, CpuClass, Executor, MemoryClass,
    Reservation,
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
    0x24, 0x79, 0x8c, 0xa5, 0xcd, 0x41, 0x72, 0x78, 0x6c, 0x11, 0xf1, 0x86, 0xb8, 0x69, 0x2f, 0xea,
    0xe8, 0x31, 0xd9, 0xc4, 0x4d, 0x49, 0xad, 0x4d, 0xd3, 0xa9, 0xde, 0xb8, 0xac, 0x04, 0xfd, 0xc9,
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
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RelayAdmissionResponse {
    pub(crate) receiver_epoch: u64,
    pub(crate) status: RelayAdmissionStatus,
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

/// The serializer every typed message is written through. It writes into a budgeted buffer the
/// encoding still holds when the write fails, so the encoding can tell a class without room from a
/// value it cannot encode.
pub(crate) type MessageSerializer<'a, 'b> =
    HighSerializer<IoWriter<&'b mut BudgetedBuffer>, ArenaHandle<'a>, RkyvError>;

/// The encoding job owns the input and its optional charge together. Declaration order returns
/// the charge after the input is dropped, including when a queued job is cancelled.
struct RkyvEncoding<T> {
    value: T,
}

impl<T> RkyvEncoding<T>
where
    T: for<'a, 'b> Serialize<MessageSerializer<'a, 'b>>,
{
    /// One attempt at serializing the value under `charge`. An output that outgrows the charge
    /// while its class has no room for more hands the encoding back with the charge it needs.
    fn encode(
        self,
        charge: Reservation,
        limit: u64,
        cancellation: &Cancellation,
    ) -> Attempt<Result<BudgetedBuffer, Report<TransportError>>, Self> {
        if let Err(cancelled) = cancellation.check() {
            return Attempt::Finished(Err(TransportError::with_cause(
                Report::new(cancelled),
                TransportError::Encode,
            )));
        }
        let mut buffer = BudgetedBuffer::with_limit(charge, limit);
        let written =
            rkyv::api::high::to_bytes_in::<_, RkyvError>(&self.value, IoWriter::new(&mut buffer));
        let Err(error) = written else {
            return Attempt::Finished(Ok(buffer));
        };
        let Some(needed) = buffer.shortfall() else {
            return Attempt::Finished(Err(TransportError::with_cause(
                Report::new(error),
                TransportError::Encode,
            )));
        };
        drop(buffer);
        Attempt::Short {
            input: self,
            needed,
        }
    }
}

/// Encode `value` on `cpu`, starting from one small charge of `class` and growing it as the output
/// needs. An output its class has no room for now waits for room while holding nothing, and only an
/// output larger than its limit or its whole class fails.
async fn encode_rkyv_buffer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
) -> Result<BudgetedBuffer, Report<TransportError>>
where
    T: Send + 'static + for<'a, 'b> Serialize<MessageSerializer<'a, 'b>>,
{
    let encoding = RkyvEncoding { value };
    executor
        .run_cpu_growing(
            cpu,
            class,
            INITIAL_MESSAGE_CHARGE.min(limit),
            encoding,
            move |encoding, charge, cancellation| encoding.encode(charge, limit, cancellation),
        )
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))?
}

/// Encode a stream request's envelope, which copies the payload it nests. The caller gives the
/// payload's own charge back first, so the encoding never waits for room while holding it; the
/// request's outbound admission bounds the nested payload until the encoding's charge covers its
/// copy. An output its class has no room for now waits for room while holding nothing.
pub(crate) async fn encode_stream_request(
    executor: &Executor,
    envelope: crate::request::StreamRequestEnvelope,
) -> Result<ChargedBytes, Report<TransportError>> {
    let class = envelope.request.class;
    let limit = class.control_body_limit(executor);
    let buffer = encode_rkyv_buffer(
        executor,
        class.memory_class(),
        class.cpu_class(),
        limit,
        envelope,
    )
    .await?;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// Serialize one typed value directly into a budgeted writer on the owning CPU class. The job waits
/// for a place on the class like every job.
pub(crate) async fn encode_rkyv<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
) -> Result<ChargedBytes, Report<TransportError>>
where
    T: Send + 'static + for<'a, 'b> Serialize<MessageSerializer<'a, 'b>>,
{
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, value).await?;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// What became of an answer [`encode_answer`] was asked to encode.
pub(crate) enum AnswerEncoding {
    /// The answer is encoded and ready to send.
    Encoded(ChargedBytes),
    /// The caller ended the exchange while the encoding waited for a place or for room. No answer
    /// can reach it any more, and the encoding gave back its place in line and its charge.
    Ended,
}

/// Serialize the answer to a request whose handler already ran, as [`encode_rkyv`] does.
///
/// The answer carries work the handler already did, so a full class holds the encoding back until
/// a place or room frees rather than throwing that work away. `ended` resolves once the caller ends
/// the exchange, by resetting its stream or closing its connection, and that ends the wait: the
/// encoding is dropped with its place and its charge, and [`AnswerEncoding::Ended`] says nobody is
/// left to answer. A failure `ended` reports instead is returned as it is.
pub(crate) async fn encode_answer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
    ended: impl Future<Output = Result<(), Report<TransportError>>>,
) -> Result<AnswerEncoding, Report<TransportError>>
where
    T: Send + 'static + for<'a, 'b> Serialize<MessageSerializer<'a, 'b>>,
{
    let encoding = encode_rkyv(executor, class, cpu, limit, value);
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

/// Validate and deserialize the answer to a request this node sent, within `decoding`, the room it
/// was read with (see [`decode_rkyv_within`]).
///
/// The answer carries work its peer already did, so a class whose queue is full holds the decoding
/// back until a place frees rather than throwing that work away, but no longer than the request's
/// own deadline. Once the deadline passes the decoding is dropped with its place and its charge,
/// and the request fails with [`TransportError::RequestTimeout`], as it would had the answer
/// arrived late.
pub(crate) async fn decode_answer<T>(
    executor: &Executor,
    cpu: CpuClass,
    bytes: ChargedBytes,
    decoding: Reservation,
    deadline: AnswerDeadline<'_>,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let decoding = decode_rkyv_within(executor, cpu, bytes, decoding);
    match nervix_primitives::time::timeout_at(deadline.at, decoding).await {
        Ok(decoded) => decoded,
        Err(_) => Err(Report::new(TransportError::RequestTimeout {
            peer: deadline.peer.clone(),
            timeout: deadline.timeout,
        })),
    }
}

/// Serialize a nested typed-request value while retaining its charge beside the resulting Vec.
pub(crate) async fn encode_rkyv_payload<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
) -> Result<EncodedPayload, Report<TransportError>>
where
    T: Send + 'static + for<'a, 'b> Serialize<MessageSerializer<'a, 'b>>,
{
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, value).await?;
    let (bytes, reservation) = buffer.into_parts();
    Ok(EncodedPayload { bytes, reservation })
}

/// Validate and deserialize one body read together with the room its decoding takes, `decoding`
/// (`read_body_to_decode`). The decoding takes no charge of its own, so it never waits for room
/// behind the body it decodes, and `decoding` covers the decoded value for as long as it is
/// retained.
pub(crate) async fn decode_rkyv_within<T>(
    executor: &Executor,
    cpu: CpuClass,
    bytes: ChargedBytes,
    decoding: Reservation,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    decode_rkyv_charged(executor, cpu, bytes, decoding).await
}

/// Validate and deserialize one bounded rkyv payload under `decoded_charge`, which covers the
/// decoded value for as long as it is retained.
async fn decode_rkyv_charged<T>(
    executor: &Executor,
    cpu: CpuClass,
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
        .run_cpu(cpu, decoded_charge, move |charge, cancellation| {
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
        })
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?
}

/// Validate and deserialize a nested typed payload. The job takes its own charge for the payload,
/// because cancellation can end its caller before a submitted CPU job exits, and one for the decoded
/// value: both as one charge, waited for while the caller holds nothing of the class. A caller
/// therefore gives back the charge of the envelope that carried the payload before it decodes it,
/// and the request's admission bounds the payload until this charge covers it.
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
    let length: u64 = bytes.len().arch_into();
    let whole = length.checked_mul(2).ok_or_else(|| {
        Report::new(TransportError::Decode(format!(
            "a payload of {length} bytes and its decoding overflow a charge"
        )))
    })?;
    let charge = executor
        .reserve(class, whole)
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let (reservation, decoded_charge) = charge
        .split(length)
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let bytes = EncodedPayload { bytes, reservation };
    decode_rkyv_charged(executor, cpu, bytes, decoded_charge).await
}

/// Validate and deserialize a nested typed payload within `room`, which its caller took together with
/// the input that carried the payload, for the payload's copy and its decoded value: the decoding
/// takes no charge of its own, so it never waits for room behind that input. Room beyond what the
/// payload needs is given back at once.
pub(crate) async fn decode_rkyv_payload_within<T>(
    executor: &Executor,
    cpu: CpuClass,
    bytes: Vec<u8>,
    room: Reservation,
) -> Result<Decoded<T>, Report<TransportError>>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let length: u64 = bytes.len().arch_into();
    let whole = length.checked_mul(2).ok_or_else(|| {
        Report::new(TransportError::Decode(format!(
            "a payload of {length} bytes and its decoding overflow a charge"
        )))
    })?;
    if room.bytes() < whole {
        return Err(Report::new(TransportError::Decode(format!(
            "a payload of {length} bytes needs {whole} bytes to decode, more than the {} taken \
             for it",
            room.bytes()
        ))));
    }
    let (needed, spare) = room
        .split(whole)
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    drop(spare);
    let (reservation, decoded_charge) = needed
        .split(length)
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    let bytes = EncodedPayload { bytes, reservation };
    decode_rkyv_charged(executor, cpu, bytes, decoded_charge).await
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

    use super::{RelayGrantDisposition, RelayGrantResponse, decode_rkyv_within, encode_rkyv};
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
                        // A receiver reads an answer together with the room its decoding takes.
                        let decoding = executor
                            .try_reserve(
                                MemoryClass::Management,
                                u64::try_from(bytes.len())
                                    .map_err(|error| io::Error::other(format!("{error:?}")))?,
                            )
                            .map_err(|error| io::Error::other(format!("{error:?}")))?;
                        let decoded = decode_rkyv_within::<RelayGrantResponse>(
                            &executor,
                            CpuClass::Control,
                            bytes,
                            decoding,
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
