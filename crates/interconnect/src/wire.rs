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

use std::num::NonZeroUsize;

use arch_into::ArchInto as _;
use error_stack::Report;
use nervix_execution::{
    AdmissionError, BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_models::{
    ClusterNodeName, NodePrerequisites, PrerequisiteMismatch, WireContractFingerprint,
};
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
    0xb7, 0x18, 0xa5, 0xd3, 0xa6, 0x6f, 0xa5, 0x92, 0x29, 0x14, 0x30, 0x7c, 0x35, 0xef, 0x28, 0xb5,
    0xfc, 0x4b, 0x9c, 0x06, 0xf1, 0x08, 0x4d, 0x6f, 0xe0, 0x86, 0x4c, 0x2f, 0x2c, 0x54, 0x78, 0x1d,
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

/// A nested rkyv payload and the charge that covers it until the outer request has been encoded.
pub(crate) struct EncodedPayload {
    bytes: Vec<u8>,
    reservation: Reservation,
}

impl EncodedPayload {
    pub(crate) fn into_parts(self) -> (Vec<u8>, Reservation) {
        (self.bytes, self.reservation)
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
    pub(crate) kind: RelayPayloadKind,
    pub(crate) domain: nervix_models::DomainName,
    pub(crate) relay: nervix_models::RelayName,
    pub(crate) key: Option<Vec<nervix_models::RemoteRuntimeField>>,
    pub(crate) metadata: Vec<nervix_models::RemoteRuntimeRecordMetadata>,
    pub(crate) acks: Vec<Option<nervix_models::RemoteAckRegistration>>,
    pub(crate) admission: Option<nervix_models::RemoteAckRegistration>,
}

impl RelayMetadata {
    pub(crate) fn from_payload(payload: &RelayPayload) -> Self {
        Self {
            kind: payload.kind,
            domain: payload.domain.clone(),
            relay: payload.relay.clone(),
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
            delivery,
            kind: self.kind,
            domain: self.domain,
            relay: self.relay,
            key: self.key,
            batch_ipc,
            metadata: self.metadata,
            acks: self.acks,
            admission: self.admission,
        }
    }
}

async fn encode_rkyv_buffer<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
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
    executor
        .run_cpu(cpu, reservation, move |charge, cancellation| {
            cancellation.check().map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Encode)
            })?;
            let writer = IoWriter::new(BudgetedBuffer::with_limit(charge, limit));
            let writer =
                rkyv::api::high::to_bytes_in::<_, RkyvError>(&value, writer).map_err(|error| {
                    TransportError::with_cause(Report::new(error), TransportError::Encode)
                })?;
            Ok(writer.into_inner())
        })
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Encode))?
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
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, value).await?;
    Ok(ChargedBytes::from_buffer(buffer))
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
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let buffer = encode_rkyv_buffer(executor, class, cpu, limit, value).await?;
    let (bytes, reservation) = buffer.into_parts();
    Ok(EncodedPayload { bytes, reservation })
}

/// Validate and deserialize one bounded rkyv payload off the async workers.
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
    let decoded_charge = executor
        .reserve(class, bytes.len().arch_into())
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    decode_rkyv_charged(executor, cpu, bytes, decoded_charge).await
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
    let decoded = decode_rkyv_charged(executor, cpu, bytes, decoded_charge).await?;
    Ok(FreeRoom::Taken(decoded))
}

/// Validate and deserialize one bounded rkyv payload under `decoded_charge`, which covers the
/// decoded value for as long as it is retained.
async fn decode_rkyv_charged<T>(
    executor: &Executor,
    cpu: CpuClass,
    bytes: ChargedBytes,
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
            let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(bytes.as_ref());
            let value = decode_aligned::<T>(&aligned, max_depth)?;
            Ok(Decoded {
                value,
                _reservation: charge,
            })
        })
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?
}

/// Validate and deserialize a nested typed-request payload while its outer envelope remains
/// charged by the caller.
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
    let max_depth = validation_depth(executor)?;
    let decoded_charge = executor
        .reserve(class, bytes.len().arch_into())
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?;
    executor
        .run_cpu(cpu, decoded_charge, move |charge, cancellation| {
            cancellation.check().map_err(|error| {
                TransportError::with_cause(Report::new(error), TransportError::Decode)
            })?;
            let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(&bytes);
            let value = decode_aligned::<T>(&aligned, max_depth)?;
            Ok(Decoded {
                value,
                _reservation: charge,
            })
        })
        .await
        .map_err(|error| TransportError::with_cause(error, TransportError::Decode))?
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
