//! Bounded rkyv payload encoding for native HTTP/2 interconnect streams.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Charging, size limits, validation, and rkyv encoding for typed stream payloads.
//! - **Depends on.** The execution budget and the interconnect message vocabulary.
//! - **Must not know.** HTTP/2 connection lifecycle, peer selection, or runtime graph semantics.

use std::num::NonZeroUsize;

use arch_into::ArchInto as _;
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, Reservation,
};
use nervix_models::ClusterNodeName;
use rkyv::{
    Archive, Deserialize, Serialize,
    api::{access_with_context, deserialize_using, high::HighSerializer},
    de::pooling::Pool,
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::IoWriter},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use super::{PoolClass, RelayPayload, RelayPayloadKind, TransportError};

const INITIAL_MESSAGE_CHARGE: u64 = 4 * 1024;

/// Changes whenever the one supported interconnect contract changes.
pub(crate) const WIRE_CONTRACT_FINGERPRINT: [u8; 32] = [
    0x1d, 0x7a, 0x0d, 0xa4, 0xc9, 0xbc, 0x7f, 0x4f, 0x1f, 0xdb, 0x1e, 0xd2, 0x02, 0xd5, 0x8a, 0x1e,
    0x37, 0xd3, 0xab, 0xc7, 0x36, 0x99, 0xb4, 0x96, 0xf0, 0xa2, 0xd0, 0xa6, 0xf3, 0xd8, 0x27, 0x5c,
];

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionHello {
    pub(crate) fingerprint: [u8; 32],
    pub(crate) class: PoolClass,
    pub(crate) process_epoch: u64,
    pub(crate) node_id: ClusterNodeName,
    pub(crate) advertised_host: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionAccepted {
    pub(crate) fingerprint: [u8; 32],
    pub(crate) process_epoch: u64,
    pub(crate) node_id: ClusterNodeName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub(crate) struct RelayGrantRequest {
    pub(crate) sender_epoch: u64,
    pub(crate) attempt: u64,
    pub(crate) body_bytes: u64,
    pub(crate) metadata: RelayMetadata,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RelayGrantResponse {
    pub(crate) grant_id: u64,
    pub(crate) receiver_epoch: u64,
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

    pub(crate) fn into_payload(self, batch_ipc: ChargedBytes) -> RelayPayload {
        RelayPayload {
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
) -> Result<BudgetedBuffer, TransportError>
where
    T: Send
        + 'static
        + for<'a> Serialize<HighSerializer<IoWriter<BudgetedBuffer>, ArenaHandle<'a>, RkyvError>>,
{
    let reservation = executor
        .reserve(class, INITIAL_MESSAGE_CHARGE.min(limit))
        .await
        .map_err(|error| TransportError::Encode(error.to_string()))?;
    executor
        .run_cpu(cpu, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .map_err(|error| TransportError::Encode(error.to_string()))?;
            let writer = IoWriter::new(BudgetedBuffer::with_limit(charge, limit));
            let writer = rkyv::api::high::to_bytes_in::<_, RkyvError>(&value, writer)
                .map_err(|error| TransportError::Encode(error.to_string()))?;
            Ok(writer.into_inner())
        })
        .await
        .map_err(|error| TransportError::Encode(error.to_string()))?
}

/// Serialize one typed value directly into a budgeted writer on the owning CPU class.
pub(crate) async fn encode_rkyv<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    limit: u64,
    value: T,
) -> Result<ChargedBytes, TransportError>
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
) -> Result<EncodedPayload, TransportError>
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
) -> Result<Decoded<T>, TransportError>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let max_depth = validation_depth(executor)?;
    let decoded_charge = executor
        .reserve(class, bytes.len().arch_into())
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    executor
        .run_cpu(cpu, decoded_charge, move |charge, cancellation| {
            cancellation
                .check()
                .map_err(|error| TransportError::Decode(error.to_string()))?;
            let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(bytes.as_ref());
            let value = decode_aligned::<T>(&aligned, max_depth)?;
            Ok(Decoded {
                value,
                _reservation: charge,
            })
        })
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?
}

/// Validate and deserialize a nested typed-request payload while its outer envelope remains
/// charged by the caller.
pub(crate) async fn decode_rkyv_payload<T>(
    executor: &Executor,
    class: MemoryClass,
    cpu: CpuClass,
    bytes: Vec<u8>,
) -> Result<Decoded<T>, TransportError>
where
    T: Archive + Send + 'static,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    let max_depth = validation_depth(executor)?;
    let decoded_charge = executor
        .reserve(class, bytes.len().arch_into())
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    executor
        .run_cpu(cpu, decoded_charge, move |charge, cancellation| {
            cancellation
                .check()
                .map_err(|error| TransportError::Decode(error.to_string()))?;
            let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(&bytes);
            let value = decode_aligned::<T>(&aligned, max_depth)?;
            Ok(Decoded {
                value,
                _reservation: charge,
            })
        })
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?
}

fn validation_depth(executor: &Executor) -> Result<NonZeroUsize, TransportError> {
    let depth = usize::try_from(executor.limits().decoder_depth.get())
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    NonZeroUsize::new(depth)
        .ok_or_else(|| TransportError::Decode("decoder depth must be nonzero".to_string()))
}

fn decode_aligned<T>(bytes: &[u8], max_depth: NonZeroUsize) -> Result<T, TransportError>
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
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    let mut deserializer = Pool::default();
    deserialize_using::<T, _, RkyvError>(archived, &mut deserializer)
        .map_err(|error| TransportError::Decode(error.to_string()))
}
