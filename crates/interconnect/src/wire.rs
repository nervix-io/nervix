//! The bytes one interconnect frame is, and the only codec that produces or consumes them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The wire envelope, its framing, and the encoders and decoders for every field a
//!   frame carries.
//! - **Depends on.** The transport's envelope types and the vocabulary values they hold.
//! - **Must not know.** Connection lifetime, peers, or why a frame is exchanged.

use std::{io::Write as _, ops::Range};

use arch_into::ArchInto as _;
use meticulous::OptionExt as _;
use nervix_execution::{
    BudgetedBuffer, ChargedBytes, CpuClass, Executor, MemoryClass, OperationLimits, Reservation,
};
use nervix_models::{
    ClusterNodeName, DomainName, RelayName, RemoteAckRegistration, RemoteRuntimeElementValue,
    RemoteRuntimeField, RemoteRuntimeRecordMetadata, RemoteRuntimeValue, Timestamp,
};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::{
    Envelope, PeerVerifier, RelayPayload, RelayPayloadKind, SignedIntroduction, TransportError,
    WIRE_TAG_ACK, WIRE_TAG_CONTROL, WIRE_TAG_INTRODUCTION, WIRE_TAG_PING, WIRE_TAG_RELAY_PAYLOAD,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WireEnvelope {
    Introduction(SignedIntroduction),
    Ping,
    Payload(Envelope),
}

/// One frame ready for the socket: the header bytes that describe it, and the shared body those
/// bytes describe.
///
/// The body is never copied into the header. A relay batch is encoded once and every destination
/// and retry writes a slice of that same allocation, so a three-destination fanout costs one
/// encode and one copy of the bytes rather than three of each.
#[derive(Debug, Clone)]
pub(crate) struct WireFrame {
    header: ChargedBytes,
    body: Option<ChargedBytes>,
    /// The budget this frame's bytes belong to, decided once where the envelope's kind is known
    /// rather than re-derived from its shape wherever it is charged again.
    class: MemoryClass,
}

/// One frame waiting on a connection's send queue, holding the transient memory its bytes occupy
/// until it leaves. Queues are bounded by bytes as well as items, so a thousand maximum-size frames
/// cannot wait where a thousand small ones fit.
#[derive(Debug)]
pub(crate) struct QueuedFrame {
    frame: WireFrame,
    // Held for exactly as long as the frame waits, and never read.
    _queued: Option<Reservation>,
}

impl QueuedFrame {
    pub(crate) fn new(frame: WireFrame, queued: Reservation) -> Self {
        Self {
            frame,
            _queued: Some(queued),
        }
    }

    /// The connection's own keepalive, which is encoded once when the writer starts and occupies
    /// no queue capacity because it never waits in the queue.
    pub(crate) fn keepalive(frame: &WireFrame) -> Self {
        Self {
            frame: frame.clone(),
            _queued: None,
        }
    }

    pub(crate) fn frame(&self) -> &WireFrame {
        &self.frame
    }
}

impl WireFrame {
    /// The whole frame's length, which is what the length prefix on the socket declares.
    fn framed_len(&self) -> Result<u32, TransportError> {
        let body = match &self.body {
            Some(body) => body.len(),
            None => 0,
        };
        let total = self.header.len().checked_add(body).ok_or_else(|| {
            TransportError::Encode("wire frame exceeds an addressable size".to_string())
        })?;
        u32::try_from(total).map_err(|_| {
            TransportError::Encode(format!("wire frame length {total} exceeds u32::MAX"))
        })
    }

    #[cfg(test)]
    pub(crate) fn header(&self) -> &[u8] {
        self.header.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn body(&self) -> Option<&ChargedBytes> {
        self.body.as_ref()
    }

    /// The budget this frame's bytes are charged to while it waits for a socket.
    pub(crate) fn memory_class(&self) -> MemoryClass {
        self.class
    }

    /// How much transient memory this frame occupies while it waits its turn on a connection.
    pub(crate) fn queued_bytes(&self) -> u64 {
        let header: u64 = self.header.len().arch_into();
        let body: u64 = match &self.body {
            Some(body) => body.len().arch_into(),
            None => 0,
        };
        header
            .checked_add(body)
            .verified("both lengths come from one frame, which the frame limit bounds")
    }
}

pub(crate) async fn write_wire_envelope<W>(
    writer: &mut W,
    frame: &WireFrame,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_u32(frame.framed_len()?)
        .await
        .map_err(TransportError::Io)?;
    writer
        .write_all(frame.header.as_ref())
        .await
        .map_err(TransportError::Io)?;
    if let Some(body) = &frame.body {
        writer
            .write_all(body.as_ref())
            .await
            .map_err(TransportError::Io)?;
    }
    writer.flush().await.map_err(TransportError::Io)
}

/// Read one frame's bytes into a charged buffer, then decode it off the async workers.
///
/// The declared length is checked against the frame limit before a byte is allocated for it, and
/// the allocation is charged to the class the frame belongs to before it is read.
pub(crate) async fn read_wire_envelope<R>(
    reader: &mut R,
    executor: &Executor,
    max_frame_bytes: usize,
) -> Result<WireEnvelope, TransportError>
where
    R: AsyncRead + Unpin,
{
    let frame = read_frame_bytes(reader, executor, max_frame_bytes).await?;
    decode_frame(executor, frame).await
}

/// Read one frame into an allocation charged to the frame's own class.
///
/// The tag arrives first, so the class is known before the rest of the frame is admitted: an
/// acknowledgement is never read out of the relay budget, and a saturated relay budget therefore
/// cannot stop a heartbeat from being received.
pub(crate) async fn read_frame_bytes<R>(
    reader: &mut R,
    executor: &Executor,
    max_frame_bytes: usize,
) -> Result<ChargedBytes, TransportError>
where
    R: AsyncRead + Unpin,
{
    let frame_size: usize = reader
        .read_u32()
        .await
        .map_err(TransportError::Io)?
        .arch_into();
    if frame_size > max_frame_bytes {
        return Err(TransportError::FrameTooLarge {
            size: frame_size,
            limit: max_frame_bytes,
        });
    }
    if frame_size == 0 {
        return Err(TransportError::Decode("wire frame is empty".to_string()));
    }
    let tag = reader.read_u8().await.map_err(TransportError::Io)?;
    let remaining = frame_size
        .checked_sub(1)
        .verified("a frame of at least one byte still has a length after its tag");
    let requested: u64 = frame_size.arch_into();
    // The class is charged for the frame before the socket is allowed to fill it, so an inbound
    // burst cannot allocate past the budget that backs it.
    let reservation = executor
        .reserve(
            frame_memory_class(tag).ok_or_else(|| unknown_tag(tag))?,
            requested,
        )
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    let mut buffer = BudgetedBuffer::with_limit(reservation, requested);
    let region = buffer
        .extend_zeroed(frame_size)
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    let (first, rest) = region
        .split_first_mut()
        .verified("the buffer was extended by the frame's own non-zero length");
    *first = tag;
    reader
        .read_exact(&mut rest[..remaining])
        .await
        .map_err(TransportError::Io)?;
    Ok(ChargedBytes::from_buffer(buffer))
}

/// Which budget an inbound frame's bytes belong to, decided from its tag alone. An unknown tag has
/// no class, and the frame carrying it is refused before it is admitted anywhere.
fn frame_memory_class(tag: u8) -> Option<MemoryClass> {
    match tag {
        WIRE_TAG_RELAY_PAYLOAD => Some(MemoryClass::Relay),
        WIRE_TAG_ACK | WIRE_TAG_INTRODUCTION | WIRE_TAG_PING => Some(MemoryClass::Management),
        WIRE_TAG_CONTROL => Some(MemoryClass::Commands),
        _ => None,
    }
}

fn unknown_tag(tag: u8) -> TransportError {
    TransportError::Decode(format!("unknown wire envelope tag {tag}"))
}

pub(crate) async fn read_and_verify_introduction<R>(
    reader: &mut R,
    executor: &Executor,
    max_frame_bytes: usize,
    verifier: &PeerVerifier,
) -> Result<ClusterNodeName, TransportError>
where
    R: AsyncRead + Unpin,
{
    match read_wire_envelope(reader, executor, max_frame_bytes).await? {
        WireEnvelope::Introduction(intro) => intro.verify(verifier),
        WireEnvelope::Ping => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
        WireEnvelope::Payload(_) => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
    }
}

/// How one envelope's serialization is admitted: the budget its bytes are charged to, the size it
/// may not exceed, and the worker class it runs on when it needs one.
struct EncodeAdmission {
    memory: MemoryClass,
    limit: u64,
    /// Absent for the frames whose size the protocol fixes. Those are the bounded protocol work the
    /// execution policy allows inline: a heartbeat and an acknowledgement are per-message traffic,
    /// and routing them through the single reserved control worker would serialize the very things
    /// that capacity exists to keep moving.
    cpu: Option<CpuClass>,
}

fn encode_admission(envelope: &WireEnvelope, limits: &OperationLimits) -> EncodeAdmission {
    match envelope {
        WireEnvelope::Introduction(_)
        | WireEnvelope::Ping
        | WireEnvelope::Payload(Envelope::Ack(_)) => EncodeAdmission {
            memory: MemoryClass::Management,
            limit: limits.management_event_bytes.as_u64(),
            cpu: None,
        },
        WireEnvelope::Payload(Envelope::RelayPayload(_)) => EncodeAdmission {
            memory: MemoryClass::Relay,
            limit: limits.relay_encoded_bytes.as_u64(),
            cpu: Some(CpuClass::Data),
        },
        WireEnvelope::Payload(Envelope::Control(_)) => EncodeAdmission {
            memory: MemoryClass::Commands,
            limit: limits.command_bytes.as_u64(),
            cpu: Some(CpuClass::Control),
        },
    }
}

/// Serialize one envelope into a frame, charged before it allocates and off the async workers
/// whenever its size is not fixed by the protocol.
///
/// This runs where the envelope is produced rather than on the connection driver, so the driver
/// only ever writes bytes that already exist.
pub(crate) async fn encode_frame(
    executor: &Executor,
    envelope: WireEnvelope,
) -> Result<WireFrame, TransportError> {
    let limits = *executor.limits();
    let admission = encode_admission(&envelope, &limits);
    // A frame header is small next to the operation limit it may not exceed, so the charge starts
    // small and the writer grows it. Reserving the whole limit for every frame would let a dozen
    // of them exhaust a class that comfortably holds thousands.
    let reservation = executor
        .reserve(admission.memory, INITIAL_FRAME_CHARGE.min(admission.limit))
        .await
        .map_err(|error| TransportError::Encode(error.to_string()))?;
    let Some(cpu) = admission.cpu else {
        return encode_wire_envelope(&envelope, reservation, admission.limit, admission.memory);
    };
    let memory = admission.memory;
    let limit = admission.limit;
    executor
        .run_cpu(cpu, reservation, move |charge, cancellation| {
            cancellation
                .check()
                .map_err(|error| TransportError::Encode(error.to_string()))?;
            encode_wire_envelope(&envelope, charge, limit, memory)
        })
        .await
        .map_err(|error| TransportError::Encode(error.to_string()))?
}

pub(crate) async fn decode_frame(
    executor: &Executor,
    frame: ChargedBytes,
) -> Result<WireEnvelope, TransportError> {
    let Some(&tag) = frame.first() else {
        return Err(TransportError::Decode("wire frame is empty".to_string()));
    };
    let limits = *executor.limits();
    // Heartbeats, introductions and acknowledgements are the bounded protocol work the execution
    // policy allows inline. They are already held in a charged frame, and admitting per-message
    // acknowledgements as jobs would serialize them behind the single reserved control worker.
    let cpu = match tag {
        WIRE_TAG_PING | WIRE_TAG_INTRODUCTION | WIRE_TAG_ACK => {
            return decode_wire_envelope(&frame, limits.management_event_bytes.as_u64());
        }
        WIRE_TAG_RELAY_PAYLOAD => CpuClass::Data,
        _ => CpuClass::Control,
    };
    let memory = frame_memory_class(tag).ok_or_else(|| unknown_tag(tag))?;
    // What this operation's decoded value may occupy. A collection the frame declares has to fit
    // it, which is what stops a small frame from naming a very large vector: an acknowledgement
    // slot is one byte absent on the wire and forty once decoded.
    let decoded_budget = match memory {
        MemoryClass::Relay => limits.relay_decoded_bytes.as_u64(),
        MemoryClass::Commands => limits.command_bytes.as_u64(),
        MemoryClass::Management | MemoryClass::Bulk => limits.management_event_bytes.as_u64(),
    };
    // The decoded value is charged separately from the frame it came out of, because a relay body
    // is carried out as a window onto the frame while a control envelope is copied out of it.
    let encoded: u64 = frame.len().arch_into();
    let reservation = executor
        .reserve(memory, encoded)
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?;
    executor
        .run_cpu(cpu, reservation, move |_charge, cancellation| {
            cancellation
                .check()
                .map_err(|error| TransportError::Decode(error.to_string()))?;
            decode_wire_envelope(&frame, decoded_budget)
        })
        .await
        .map_err(|error| TransportError::Decode(error.to_string()))?
}

pub(crate) fn encode_wire_envelope(
    envelope: &WireEnvelope,
    charge: Reservation,
    limit: u64,
    memory: MemoryClass,
) -> Result<WireFrame, TransportError> {
    let mut header = BudgetedBuffer::with_limit(charge, limit);
    let mut body = None;
    match envelope {
        WireEnvelope::Introduction(intro) => {
            write_tagged_rkyv(&mut header, WIRE_TAG_INTRODUCTION, intro)?;
        }
        WireEnvelope::Ping => put(&mut header, &[WIRE_TAG_PING])?,
        WireEnvelope::Payload(Envelope::RelayPayload(payload)) => {
            put(&mut header, &[WIRE_TAG_RELAY_PAYLOAD])?;
            // The header ends with the body's declared length; the body itself follows it on the
            // socket as a slice of the allocation the batch was encoded into.
            encode_stream_payload_header(payload, &mut header)?;
            body = Some(payload.batch_ipc.clone());
        }
        WireEnvelope::Payload(Envelope::Ack(ack)) => {
            write_tagged_rkyv(&mut header, WIRE_TAG_ACK, ack)?;
        }
        WireEnvelope::Payload(Envelope::Control(control)) => {
            write_tagged_rkyv(&mut header, WIRE_TAG_CONTROL, control)?;
        }
    }
    Ok(WireFrame {
        header: ChargedBytes::from_buffer(header),
        body,
        class: memory,
    })
}

fn write_tagged_rkyv<T>(
    header: &mut BudgetedBuffer,
    tag: u8,
    value: &T,
) -> Result<(), TransportError>
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    put(header, &[tag])?;
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map_err(|err| TransportError::Encode(err.to_string()))?;
    put(header, encoded.as_slice())
}

fn decode_wire_envelope(
    frame: &ChargedBytes,
    decoded_budget: u64,
) -> Result<WireEnvelope, TransportError> {
    let Some((&tag, payload)) = frame.split_first() else {
        return Err(TransportError::Decode("wire frame is empty".to_string()));
    };
    match tag {
        WIRE_TAG_INTRODUCTION => Ok(WireEnvelope::Introduction(decode_rkyv(payload)?)),
        WIRE_TAG_PING => {
            if !payload.is_empty() {
                return Err(TransportError::Decode(
                    "ping wire frame must not contain payload".to_string(),
                ));
            }
            Ok(WireEnvelope::Ping)
        }
        WIRE_TAG_RELAY_PAYLOAD => Ok(WireEnvelope::Payload(Envelope::RelayPayload(
            decode_stream_payload(frame, decoded_budget)?,
        ))),
        WIRE_TAG_ACK => Ok(WireEnvelope::Payload(Envelope::Ack(decode_rkyv(payload)?))),
        WIRE_TAG_CONTROL => Ok(WireEnvelope::Payload(Envelope::Control(decode_rkyv(
            payload,
        )?))),
        _ => Err(TransportError::Decode(format!(
            "unknown wire envelope tag {tag}"
        ))),
    }
}

/// The payload's own length bounds the aligned copy, which is why it is allocated from a length
/// the frame already holds rather than from a count the sender declared.
fn decode_rkyv<T>(payload: &[u8]) -> Result<T, TransportError>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&aligned)
        .map_err(|err| TransportError::Decode(err.to_string()))
}

fn put_u8(bytes: &mut BudgetedBuffer, value: u8) -> Result<(), TransportError> {
    put(bytes, &[value])
}

fn put(bytes: &mut BudgetedBuffer, value: &[u8]) -> Result<(), TransportError> {
    bytes
        .write_all(value)
        .map_err(|error| TransportError::Encode(error.to_string()))
}

fn encode_stream_payload_header(
    payload: &RelayPayload,
    bytes: &mut BudgetedBuffer,
) -> Result<(), TransportError> {
    put_u8(bytes, payload.kind.wire_tag())?;
    encode_string(bytes, payload.domain.as_str())?;
    encode_string(bytes, payload.relay.as_str())?;
    encode_branch_key(bytes, &payload.key)?;
    encode_len(bytes, payload.metadata.len())?;
    for metadata in &payload.metadata {
        put(
            bytes,
            &metadata
                .ingested_at_low_watermark
                .unix_nanos()
                .to_be_bytes(),
        )?;
        put(
            bytes,
            &metadata
                .ingested_at_high_watermark
                .unix_nanos()
                .to_be_bytes(),
        )?;
    }
    encode_len(bytes, payload.acks.len())?;
    for ack in &payload.acks {
        match ack {
            Some(ack) => {
                put_u8(bytes, 1)?;
                put(bytes, &ack.ack_id.to_be_bytes())?;
                encode_string(bytes, ack.reply_node_id.as_str())?;
            }
            None => put_u8(bytes, 0)?,
        }
    }
    match &payload.admission {
        Some(admission) => {
            put_u8(bytes, 1)?;
            put(bytes, &admission.ack_id.to_be_bytes())?;
            encode_string(bytes, admission.reply_node_id.as_str())?;
        }
        None => put_u8(bytes, 0)?,
    }
    // Only the body's declared length belongs in the header. The bytes themselves follow it on
    // the socket, straight from the allocation they were encoded into.
    encode_len(bytes, payload.batch_ipc.len())?;
    Ok(())
}

/// Decode a relay frame, refusing every declared count that the frame's own remaining bytes cannot
/// possibly hold before a collection is allocated for it. The body is carried out as a window onto
/// the frame rather than copied into a second allocation.
fn decode_stream_payload(
    frame: &ChargedBytes,
    decoded_budget: u64,
) -> Result<RelayPayload, TransportError> {
    // The leading tag byte is the frame's, not the payload's.
    let mut cursor = WireCursor::new(&frame[1..], decoded_budget);
    let kind = RelayPayloadKind::from_wire_tag(cursor.read_u8()?)?;
    let domain_raw = cursor.read_string()?;
    let relay_raw = cursor.read_string()?;
    let key = cursor.read_branch_key()?;
    let metadata_count = cursor.read_count(METADATA_ELEMENT)?;
    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        metadata.push(RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
            ingested_at_high_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
        });
    }
    let ack_count = cursor.read_count(ACK_ELEMENT)?;
    let mut acks = Vec::with_capacity(ack_count);
    for _ in 0..ack_count {
        match cursor.read_u8()? {
            0 => acks.push(None),
            1 => {
                let ack_id = cursor.read_u64()?;
                let reply_node_raw = cursor.read_string()?;
                let reply_node_id =
                    ClusterNodeName::try_from(reply_node_raw.as_str()).map_err(|error| {
                        TransportError::Decode(format!(
                            "invalid node id '{reply_node_raw}': {error}"
                        ))
                    })?;
                acks.push(Some(RemoteAckRegistration {
                    ack_id,
                    reply_node_id,
                }));
            }
            flag => {
                return Err(TransportError::Decode(format!(
                    "invalid relay ack presence flag {flag}"
                )));
            }
        }
    }
    let admission = match cursor.read_u8()? {
        0 => None,
        1 => {
            let ack_id = cursor.read_u64()?;
            let reply_node_raw = cursor.read_string()?;
            let reply_node_id =
                ClusterNodeName::try_from(reply_node_raw.as_str()).map_err(|error| {
                    TransportError::Decode(format!("invalid node id '{reply_node_raw}': {error}"))
                })?;
            Some(RemoteAckRegistration {
                ack_id,
                reply_node_id,
            })
        }
        flag => {
            return Err(TransportError::Decode(format!(
                "invalid relay admission presence flag {flag}"
            )));
        }
    };
    let body = cursor.read_body_range()?;
    cursor.finish()?;
    // The header's own tag byte offsets every range the cursor reported.
    let batch_ipc = frame
        .slice(
            body.start.checked_add(1).ok_or_else(|| {
                TransportError::Decode("relay body offset exceeds the frame".to_string())
            })?,
            body.end.checked_add(1).ok_or_else(|| {
                TransportError::Decode("relay body offset exceeds the frame".to_string())
            })?,
        )
        .ok_or_else(|| {
            TransportError::Decode("relay body extends past the frame it arrived in".to_string())
        })?;
    let domain = DomainName::try_from(domain_raw.as_str()).map_err(|error| {
        TransportError::Decode(format!("invalid domain '{domain_raw}': {error}"))
    })?;
    let relay = RelayName::try_from(relay_raw.as_str()).map_err(|error| {
        TransportError::Decode(format!("invalid relay identifier '{relay_raw}': {error}"))
    })?;
    Ok(RelayPayload {
        kind,
        domain,
        relay,
        key,
        batch_ipc,
        metadata,
        acks,
        admission,
    })
}

fn encode_len(bytes: &mut BudgetedBuffer, len: usize) -> Result<(), TransportError> {
    let len = u32::try_from(len)
        .map_err(|_| TransportError::Encode(format!("length {len} exceeds u32::MAX")))?;
    put(bytes, &len.to_be_bytes())?;
    Ok(())
}

fn encode_branch_key(
    bytes: &mut BudgetedBuffer,
    key: &Option<Vec<RemoteRuntimeField>>,
) -> Result<(), TransportError> {
    let Some(fields) = key else {
        put_u8(bytes, 0)?;
        return Ok(());
    };
    if fields.is_empty() {
        return Err(TransportError::Encode(
            "branch key must contain at least one field".to_string(),
        ));
    }
    put_u8(bytes, 1)?;
    encode_len(bytes, fields.len())?;
    for field in fields {
        encode_string(bytes, field.name.as_str())?;
        encode_remote_value(bytes, &field.value)?;
    }
    Ok(())
}

fn encode_remote_value(
    bytes: &mut BudgetedBuffer,
    value: &RemoteRuntimeValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeValue::U8(value) => {
            put_u8(bytes, 0)?;
            put_u8(bytes, *value)?;
        }
        RemoteRuntimeValue::I8(value) => {
            put_u8(bytes, 1)?;
            put_u8(bytes, value.to_be_bytes()[0])?;
        }
        RemoteRuntimeValue::U16(value) => {
            put_u8(bytes, 2)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::I16(value) => {
            put_u8(bytes, 3)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::U32(value) => {
            put_u8(bytes, 4)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::I32(value) => {
            put_u8(bytes, 5)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::U64(value) => {
            put_u8(bytes, 6)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::I64(value) => {
            put_u8(bytes, 7)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeValue::Bool(value) => {
            put_u8(bytes, 8)?;
            put_u8(bytes, u8::from(*value))?;
        }
        RemoteRuntimeValue::String(value) => {
            put_u8(bytes, 9)?;
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::Datetime(value) => {
            put_u8(bytes, 10)?;
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::F32(value) => {
            put_u8(bytes, 11)?;
            put(bytes, &value.to_bits().to_be_bytes())?;
        }
        RemoteRuntimeValue::F64(value) => {
            put_u8(bytes, 12)?;
            put(bytes, &value.to_bits().to_be_bytes())?;
        }
        RemoteRuntimeValue::Array(values) => {
            put_u8(bytes, 13)?;
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeValue::Vec(values) => {
            put_u8(bytes, 14)?;
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
    }
    Ok(())
}

fn encode_remote_element_value(
    bytes: &mut BudgetedBuffer,
    value: &RemoteRuntimeElementValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeElementValue::U8(value) => {
            put_u8(bytes, 0)?;
            put_u8(bytes, *value)?;
        }
        RemoteRuntimeElementValue::I8(value) => {
            put_u8(bytes, 1)?;
            put_u8(bytes, value.to_be_bytes()[0])?;
        }
        RemoteRuntimeElementValue::U16(value) => {
            put_u8(bytes, 2)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::I16(value) => {
            put_u8(bytes, 3)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::U32(value) => {
            put_u8(bytes, 4)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::I32(value) => {
            put_u8(bytes, 5)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::U64(value) => {
            put_u8(bytes, 6)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::I64(value) => {
            put_u8(bytes, 7)?;
            put(bytes, &value.to_be_bytes())?;
        }
        RemoteRuntimeElementValue::Bool(value) => {
            put_u8(bytes, 8)?;
            put_u8(bytes, u8::from(*value))?;
        }
        RemoteRuntimeElementValue::String(value) => {
            put_u8(bytes, 9)?;
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::Datetime(value) => {
            put_u8(bytes, 10)?;
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::F32(value) => {
            put_u8(bytes, 11)?;
            put(bytes, &value.to_bits().to_be_bytes())?;
        }
        RemoteRuntimeElementValue::F64(value) => {
            put_u8(bytes, 12)?;
            put(bytes, &value.to_bits().to_be_bytes())?;
        }
        RemoteRuntimeElementValue::Array(values) => {
            put_u8(bytes, 13)?;
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeElementValue::Vec(values) => {
            put_u8(bytes, 14)?;
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
    }
    Ok(())
}

fn encode_bytes(bytes: &mut BudgetedBuffer, value: &[u8]) -> Result<(), TransportError> {
    encode_len(bytes, value.len())?;
    put(bytes, value)?;
    Ok(())
}

fn encode_string(bytes: &mut BudgetedBuffer, value: &str) -> Result<(), TransportError> {
    encode_bytes(bytes, value.as_bytes())
}

/// How many bytes one metadata row always occupies: two nanosecond watermarks.
const METADATA_ENCODED_BYTES: usize = 16;

/// How many bytes each collection on this wire needs, both on the wire and once it lands.
///
/// A declared count has to fit both: the frame must be able to hold that many elements at their
/// smallest encoding, and the operation's charge must be able to hold what they become. An
/// acknowledgement slot is one byte when absent and forty when decoded, so the encoded bound alone
/// would let a small frame declare a very large vector.
struct ElementSize {
    encoded: usize,
    decoded: usize,
}

/// One metadata row: two nanosecond watermarks either way.
const METADATA_ELEMENT: ElementSize = ElementSize {
    encoded: METADATA_ENCODED_BYTES,
    decoded: size_of::<RemoteRuntimeRecordMetadata>(),
};

/// One acknowledgement slot: an absence flag at its smallest.
const ACK_ELEMENT: ElementSize = ElementSize {
    encoded: 1,
    decoded: size_of::<Option<RemoteAckRegistration>>(),
};

/// One branch key field: an empty name and a one-byte value tag at its smallest.
const BRANCH_FIELD_ELEMENT: ElementSize = ElementSize {
    encoded: 5,
    decoded: size_of::<RemoteRuntimeField>(),
};

/// One element of a collection inside a branch key value: its value tag at its smallest.
const VALUE_ELEMENT: ElementSize = ElementSize {
    encoded: 1,
    decoded: size_of::<RemoteRuntimeElementValue>(),
};

/// What a frame's header is charged before it is written. Headers are names, a branch key and
/// per-row metadata, so most are far smaller than this and the rest grow into their operation's
/// limit as they are written.
const INITIAL_FRAME_CHARGE: u64 = 4 * 1024;

/// How deeply a value may nest before the frame is refused. Collections in a branch key are the
/// only recursion the wire has, so this is the bound that keeps a hostile frame from recursing the
/// decoder's stack away.
const MAX_VALUE_DEPTH: u32 = 64;

/// A reader over one frame's bytes that refuses a declared count before it is allocated for.
///
/// Every count on this wire describes elements the same frame must also contain, so multiplying it
/// by the smallest an element can be and comparing that with the bytes actually left is an exact
/// admission test. The decoded size is checked against the operation's own charge in the same
/// place. Both happen before any collection is reserved.
struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    depth: u32,
    /// What the operation reading this frame has been charged. A collection may not claim more of
    /// it than remains.
    decoded_budget: u64,
}

impl<'a> WireCursor<'a> {
    fn new(bytes: &'a [u8], decoded_budget: u64) -> Self {
        Self {
            bytes,
            offset: 0,
            depth: 0,
            decoded_budget,
        }
    }

    /// A declared element count, accepted only if the frame's remaining bytes could hold that many
    /// elements at their smallest encoding and the operation's charge could hold what they decode
    /// into. Whatever it accepts is subtracted from that charge, so several collections in one
    /// frame cannot each claim all of it.
    fn read_count(&mut self, element: ElementSize) -> Result<usize, TransportError> {
        let count = self.read_len()?;
        let required = count.checked_mul(element.encoded).ok_or_else(|| {
            TransportError::Decode(format!(
                "declared count {count} exceeds an addressable size"
            ))
        })?;
        let remaining = self
            .bytes
            .len()
            .checked_sub(self.offset)
            .verified("the cursor never advances past the frame it reads");
        if required > remaining {
            return Err(TransportError::Decode(format!(
                "declared count {count} needs at least {required} bytes but only {remaining} \
                 remain in the frame"
            )));
        }
        let decoded: u64 = count
            .checked_mul(element.decoded)
            .ok_or_else(|| {
                TransportError::Decode(format!(
                    "declared count {count} exceeds an addressable size"
                ))
            })?
            .arch_into();
        let Some(left) = self.decoded_budget.checked_sub(decoded) else {
            return Err(TransportError::Decode(format!(
                "declared count {count} decodes to {decoded} bytes, past the {} this operation \
                 has left",
                self.decoded_budget
            )));
        };
        self.decoded_budget = left;
        Ok(count)
    }

    /// Enter one level of a nested value, refusing a frame that nests past the decoder's bound.
    fn enter(&mut self) -> Result<(), TransportError> {
        let depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| TransportError::Decode("wire frame nesting overflow".to_string()))?;
        if depth > MAX_VALUE_DEPTH {
            return Err(TransportError::Decode(format!(
                "wire frame nests deeper than the {MAX_VALUE_DEPTH} levels the decoder accepts"
            )));
        }
        self.depth = depth;
        Ok(())
    }

    fn leave(&mut self) {
        self.depth = self
            .depth
            .checked_sub(1)
            .verified("every level left was entered first");
    }

    fn read_u8(&mut self) -> Result<u8, TransportError> {
        let bytes = self.read_exact(1)?;
        Ok(bytes[0])
    }

    fn read_i8(&mut self) -> Result<i8, TransportError> {
        Ok(i8::from_be_bytes([self.read_u8()?]))
    }

    fn read_u16(&mut self) -> Result<u16, TransportError> {
        let bytes = self.read_exact(2)?;
        let mut raw = [0u8; 2];
        raw.copy_from_slice(bytes);
        Ok(u16::from_be_bytes(raw))
    }

    fn read_i16(&mut self) -> Result<i16, TransportError> {
        let bytes = self.read_exact(2)?;
        let mut raw = [0u8; 2];
        raw.copy_from_slice(bytes);
        Ok(i16::from_be_bytes(raw))
    }

    fn read_u32(&mut self) -> Result<u32, TransportError> {
        let bytes = self.read_exact(4)?;
        let mut raw = [0u8; 4];
        raw.copy_from_slice(bytes);
        Ok(u32::from_be_bytes(raw))
    }

    fn read_i32(&mut self) -> Result<i32, TransportError> {
        let bytes = self.read_exact(4)?;
        let mut raw = [0u8; 4];
        raw.copy_from_slice(bytes);
        Ok(i32::from_be_bytes(raw))
    }

    fn read_u64(&mut self) -> Result<u64, TransportError> {
        let bytes = self.read_exact(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(raw))
    }

    fn read_i64(&mut self) -> Result<i64, TransportError> {
        let bytes = self.read_exact(8)?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(bytes);
        Ok(i64::from_be_bytes(raw))
    }

    fn read_f32(&mut self) -> Result<f32, TransportError> {
        Ok(f32::from_bits(self.read_u32()?))
    }

    fn read_f64(&mut self) -> Result<f64, TransportError> {
        Ok(f64::from_bits(self.read_u64()?))
    }

    fn read_len(&mut self) -> Result<usize, TransportError> {
        Ok(self.read_u32()?.arch_into())
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], TransportError> {
        let len = self.read_len()?;
        self.read_exact(len)
    }

    /// The window the relay body occupies in the frame, so the caller can carry it out as a slice
    /// of the allocation it arrived in rather than a copy of it.
    fn read_body_range(&mut self) -> Result<Range<usize>, TransportError> {
        let len = self.read_len()?;
        let start = self.offset;
        self.read_exact(len)?;
        Ok(start..self.offset)
    }

    fn read_string(&mut self) -> Result<String, TransportError> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            TransportError::Decode(format!("invalid utf-8 in wire frame: {error}"))
        })
    }

    fn read_branch_key(&mut self) -> Result<Option<Vec<RemoteRuntimeField>>, TransportError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => {
                let len = self.read_count(BRANCH_FIELD_ELEMENT)?;
                if len == 0 {
                    return Err(TransportError::Decode(
                        "branch key must contain at least one field".to_string(),
                    ));
                }
                let mut fields = Vec::with_capacity(len);
                for _ in 0..len {
                    fields.push(RemoteRuntimeField {
                        name: self.read_string()?,
                        value: self.read_remote_value()?,
                    });
                }
                Ok(Some(fields))
            }
            flag => Err(TransportError::Decode(format!(
                "invalid branch key presence flag {flag}"
            ))),
        }
    }

    fn read_remote_value(&mut self) -> Result<RemoteRuntimeValue, TransportError> {
        match self.read_u8()? {
            0 => Ok(RemoteRuntimeValue::U8(self.read_u8()?)),
            1 => Ok(RemoteRuntimeValue::I8(self.read_i8()?)),
            2 => Ok(RemoteRuntimeValue::U16(self.read_u16()?)),
            3 => Ok(RemoteRuntimeValue::I16(self.read_i16()?)),
            4 => Ok(RemoteRuntimeValue::U32(self.read_u32()?)),
            5 => Ok(RemoteRuntimeValue::I32(self.read_i32()?)),
            6 => Ok(RemoteRuntimeValue::U64(self.read_u64()?)),
            7 => Ok(RemoteRuntimeValue::I64(self.read_i64()?)),
            8 => match self.read_u8()? {
                0 => Ok(RemoteRuntimeValue::Bool(false)),
                1 => Ok(RemoteRuntimeValue::Bool(true)),
                value => Err(TransportError::Decode(format!(
                    "invalid bool value {value} in branch key"
                ))),
            },
            9 => Ok(RemoteRuntimeValue::String(self.read_string()?)),
            10 => Ok(RemoteRuntimeValue::Datetime(self.read_string()?)),
            11 => Ok(RemoteRuntimeValue::F32(self.read_f32()?)),
            12 => Ok(RemoteRuntimeValue::F64(self.read_f64()?)),
            13 => {
                self.enter()?;
                let len = self.read_count(VALUE_ELEMENT)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                self.leave();
                Ok(RemoteRuntimeValue::Array(values))
            }
            14 => {
                self.enter()?;
                let len = self.read_count(VALUE_ELEMENT)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                self.leave();
                Ok(RemoteRuntimeValue::Vec(values))
            }
            tag => Err(TransportError::Decode(format!(
                "unknown branch key value tag {tag}"
            ))),
        }
    }

    fn read_remote_element_value(&mut self) -> Result<RemoteRuntimeElementValue, TransportError> {
        match self.read_u8()? {
            0 => Ok(RemoteRuntimeElementValue::U8(self.read_u8()?)),
            1 => Ok(RemoteRuntimeElementValue::I8(self.read_i8()?)),
            2 => Ok(RemoteRuntimeElementValue::U16(self.read_u16()?)),
            3 => Ok(RemoteRuntimeElementValue::I16(self.read_i16()?)),
            4 => Ok(RemoteRuntimeElementValue::U32(self.read_u32()?)),
            5 => Ok(RemoteRuntimeElementValue::I32(self.read_i32()?)),
            6 => Ok(RemoteRuntimeElementValue::U64(self.read_u64()?)),
            7 => Ok(RemoteRuntimeElementValue::I64(self.read_i64()?)),
            8 => match self.read_u8()? {
                0 => Ok(RemoteRuntimeElementValue::Bool(false)),
                1 => Ok(RemoteRuntimeElementValue::Bool(true)),
                value => Err(TransportError::Decode(format!(
                    "invalid bool value {value} in branch key element"
                ))),
            },
            9 => Ok(RemoteRuntimeElementValue::String(self.read_string()?)),
            10 => Ok(RemoteRuntimeElementValue::Datetime(self.read_string()?)),
            11 => Ok(RemoteRuntimeElementValue::F32(self.read_f32()?)),
            12 => Ok(RemoteRuntimeElementValue::F64(self.read_f64()?)),
            13 => {
                self.enter()?;
                let len = self.read_count(VALUE_ELEMENT)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                self.leave();
                Ok(RemoteRuntimeElementValue::Array(values))
            }
            14 => {
                self.enter()?;
                let len = self.read_count(VALUE_ELEMENT)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                self.leave();
                Ok(RemoteRuntimeElementValue::Vec(values))
            }
            tag => Err(TransportError::Decode(format!(
                "unknown branch key element value tag {tag}"
            ))),
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], TransportError> {
        let Some(end) = self.offset.checked_add(len) else {
            return Err(TransportError::Decode(
                "wire frame length overflow".to_string(),
            ));
        };
        if end > self.bytes.len() {
            return Err(TransportError::Decode(
                "wire frame ended unexpectedly".to_string(),
            ));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<(), TransportError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(TransportError::Decode(
                "wire frame contained trailing bytes".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_execution::{Executor, MemoryClass};
    use nervix_models::{DomainName, RelayName};

    use super::*;

    /// What a relay operation's decoded value may occupy, which is the budget the transport's own
    /// decode path passes.
    fn relay_decoded_budget(executor: &Executor) -> u64 {
        executor.limits().relay_decoded_bytes.as_u64()
    }

    fn charged(executor: &Executor, bytes: Vec<u8>) -> ChargedBytes {
        executor
            .try_charge_owned(MemoryClass::Relay, bytes)
            .expect("the relay class has room for a test body")
    }

    fn dummy_stream_payload(executor: &Executor, relay: &str) -> RelayPayload {
        RelayPayload {
            kind: RelayPayloadKind::Routed,
            domain: DomainName::try_from("orders").expect("valid domain"),
            relay: RelayName::try_from(relay).expect("valid relay"),
            key: None,
            batch_ipc: charged(executor, vec![1, 2, 3, 4]),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: None,
        }
    }

    /// Round-trip a relay payload through the framing the socket actually carries.
    async fn roundtrip(executor: &Executor, payload: RelayPayload) -> RelayPayload {
        let frame = encode_frame(
            executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect("payload should encode");
        let mut framed = frame.header.to_vec();
        if let Some(body) = &frame.body {
            framed.extend_from_slice(body);
        }
        let frame = charged(executor, framed);
        match decode_wire_envelope(&frame, relay_decoded_budget(executor))
            .expect("payload should decode")
        {
            WireEnvelope::Payload(Envelope::RelayPayload(payload)) => payload,
            other => panic!("expected a relay payload, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn relay_payload_branch_key_roundtrips_native_fields() {
        let executor = Executor::default();
        let mut payload = dummy_stream_payload(&executor, "orders");
        payload.key = Some(vec![
            RemoteRuntimeField {
                name: "tenant".to_string(),
                value: RemoteRuntimeValue::String("acme".to_string()),
            },
            RemoteRuntimeField {
                name: "user_id".to_string(),
                value: RemoteRuntimeValue::U32(42),
            },
        ]);

        let decoded = roundtrip(&executor, payload.clone()).await;

        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn relay_payload_without_branch_key_roundtrips_as_absent() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");

        let decoded = roundtrip(&executor, payload.clone()).await;

        assert_eq!(decoded.key, None);
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn relay_payload_empty_branch_key_is_rejected() {
        let executor = Executor::default();
        let mut payload = dummy_stream_payload(&executor, "orders");
        payload.key = Some(Vec::new());

        let error = encode_frame(
            &executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect_err("empty branch key must be rejected");

        assert!(error.to_string().contains("at least one field"));
    }

    #[tokio::test]
    async fn a_relay_body_is_carried_as_a_window_onto_the_frame_it_arrived_in() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");

        let decoded = roundtrip(&executor, payload.clone()).await;

        assert_eq!(&*decoded.batch_ipc, &[1, 2, 3, 4]);
        assert_eq!(decoded.batch_ipc.len(), 4);
    }

    /// A frame that declares more metadata rows than its own remaining bytes could hold must be
    /// refused before a collection is reserved for them.
    #[tokio::test]
    async fn a_metadata_count_larger_than_the_frame_is_refused_before_allocation() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");
        let frame = encode_frame(
            &executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect("payload should encode");
        let mut framed = frame.header.to_vec();
        if let Some(body) = &frame.body {
            framed.extend_from_slice(body);
        }
        // The metadata count follows the frame tag, the payload kind, the domain, the relay and
        // the absent branch key.
        let count_at = 1 + 1 + 4 + "orders".len() + 4 + "orders".len() + 1;
        framed[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());

        let frame = charged(&executor, framed);
        let error = decode_wire_envelope(&frame, relay_decoded_budget(&executor))
            .expect_err("an impossible metadata count must be refused");

        assert!(
            error.to_string().contains("declared count 4294967295"),
            "the failure names the count it refused: {error}"
        );
    }

    /// The same for acknowledgement slots, which are one byte each on the wire.
    #[tokio::test]
    async fn an_ack_count_larger_than_the_frame_is_refused_before_allocation() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");
        let frame = encode_frame(
            &executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect("payload should encode");
        let mut framed = frame.header.to_vec();
        if let Some(body) = &frame.body {
            framed.extend_from_slice(body);
        }
        let ack_count_at = 1 + 1 + 4 + "orders".len() + 4 + "orders".len() + 1 + 4;
        framed[ack_count_at..ack_count_at + 4].copy_from_slice(&u32::MAX.to_be_bytes());

        let frame = charged(&executor, framed);
        let error = decode_wire_envelope(&frame, relay_decoded_budget(&executor))
            .expect_err("an impossible ack count must be refused");

        assert!(
            error.to_string().contains("declared count 4294967295"),
            "the failure names the count it refused: {error}"
        );
    }

    /// An acknowledgement slot is one byte absent and far larger decoded, so a frame that declares
    /// more of them than its charge can hold is refused even though its own bytes could hold them.
    #[tokio::test]
    async fn a_count_that_fits_the_frame_but_not_the_charge_is_refused() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");
        let frame = encode_frame(
            &executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect("payload should encode");
        let mut framed = frame.header().to_vec();
        if let Some(body) = frame.body() {
            framed.extend_from_slice(body);
        }
        let ack_count_at = 1 + 1 + 4 + "orders".len() + 4 + "orders".len() + 1 + 4;
        let declared = 128_u32;
        framed[ack_count_at..ack_count_at + 4].copy_from_slice(&declared.to_be_bytes());
        // The frame is padded so the declared slots fit its bytes, leaving only the charge to
        // refuse them.
        framed.extend(std::iter::repeat_n(0_u8, declared.arch_into()));

        let frame = charged(&executor, framed);
        let error = decode_wire_envelope(&frame, 64)
            .expect_err("a count the charge cannot hold must be refused");

        assert!(
            error.to_string().contains("past the 64"),
            "the failure names the charge it would have exceeded: {error}"
        );
    }

    /// A branch key value nested past the decoder's bound is refused rather than recursing the
    /// stack away.
    #[tokio::test]
    async fn a_branch_key_nested_past_the_decoder_bound_is_refused() {
        let executor = Executor::default();
        let payload = dummy_stream_payload(&executor, "orders");
        let frame = encode_frame(
            &executor,
            WireEnvelope::Payload(Envelope::RelayPayload(payload)),
        )
        .await
        .expect("payload should encode");
        let prefix_len = 1 + 1 + 4 + "orders".len() + 4 + "orders".len();
        let mut framed = frame.header[..prefix_len].to_vec();
        // A present branch key with one field named "k", whose value is an array nesting one level
        // deeper than the decoder accepts.
        framed.push(1);
        framed.extend_from_slice(&1_u32.to_be_bytes());
        framed.extend_from_slice(&1_u32.to_be_bytes());
        framed.push(b'k');
        for _ in 0..=MAX_VALUE_DEPTH {
            framed.push(13);
            framed.extend_from_slice(&1_u32.to_be_bytes());
        }

        let frame = charged(&executor, framed);
        let error = decode_wire_envelope(&frame, relay_decoded_budget(&executor))
            .expect_err("a frame nested past the bound must be refused");

        assert!(
            error.to_string().contains("nests deeper than"),
            "the failure names the nesting bound: {error}"
        );
    }
}
