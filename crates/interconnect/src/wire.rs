//! The bytes one interconnect frame is, and the only codec that produces or consumes them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The wire envelope, its framing, and the encoders and decoders for every field a
//!   frame carries.
//! - **Depends on.** The transport's envelope types and the vocabulary values they hold.
//! - **Must not know.** Connection lifetime, peers, or why a frame is exchanged.

use arch_into::ArchInto as _;
use nervix_models::{
    ClusterNodeName, DomainName, RelayName, RemoteAckRegistration, RemoteAckResolution,
    RemoteRuntimeElementValue, RemoteRuntimeField, RemoteRuntimeRecordMetadata, RemoteRuntimeValue,
    Timestamp,
};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::{
    ControlEnvelope, Envelope, PeerVerifier, RelayPayload, RelayPayloadKind, SignedIntroduction,
    TransportError, WIRE_TAG_ACK, WIRE_TAG_CONTROL, WIRE_TAG_INTRODUCTION, WIRE_TAG_PING,
    WIRE_TAG_RELAY_PAYLOAD,
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WireEnvelope {
    Introduction(SignedIntroduction),
    Ping,
    Payload(Envelope),
}

pub(crate) async fn write_wire_envelope<W>(
    writer: &mut W,
    envelope: &WireEnvelope,
) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode_wire_envelope(envelope)?;
    let frame_size = u32::try_from(bytes.len()).map_err(|_| {
        TransportError::Encode(format!(
            "wire envelope length {} exceeds u32::MAX",
            bytes.len()
        ))
    })?;
    writer
        .write_u32(frame_size)
        .await
        .map_err(TransportError::Io)?;
    writer.write_all(&bytes).await.map_err(TransportError::Io)?;
    writer.flush().await.map_err(TransportError::Io)
}

pub(crate) async fn read_wire_envelope<R>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<WireEnvelope, TransportError>
where
    R: AsyncRead + Unpin,
{
    let frame_size = reader
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
    let mut bytes = vec![0u8; frame_size];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(TransportError::Io)?;
    decode_wire_envelope(&bytes)
}

pub(crate) async fn read_and_verify_introduction<R>(
    reader: &mut R,
    max_frame_bytes: usize,
    verifier: &PeerVerifier,
) -> Result<ClusterNodeName, TransportError>
where
    R: AsyncRead + Unpin,
{
    match read_wire_envelope(reader, max_frame_bytes).await? {
        WireEnvelope::Introduction(intro) => intro.verify(verifier),
        WireEnvelope::Ping => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
        WireEnvelope::Payload(_) => Err(TransportError::InvalidHandshake(
            "first message must be an introduction".to_string(),
        )),
    }
}

pub(crate) fn encode_wire_envelope(envelope: &WireEnvelope) -> Result<Vec<u8>, TransportError> {
    match envelope {
        WireEnvelope::Introduction(intro) => {
            let mut bytes = vec![WIRE_TAG_INTRODUCTION];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(intro)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
        WireEnvelope::Ping => Ok(vec![WIRE_TAG_PING]),
        WireEnvelope::Payload(Envelope::RelayPayload(payload)) => {
            let mut bytes = vec![WIRE_TAG_RELAY_PAYLOAD];
            encode_stream_payload(payload, &mut bytes)?;
            Ok(bytes)
        }
        WireEnvelope::Payload(Envelope::Ack(ack)) => {
            let mut bytes = vec![WIRE_TAG_ACK];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(ack)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
        WireEnvelope::Payload(Envelope::Control(control)) => {
            let mut bytes = vec![WIRE_TAG_CONTROL];
            bytes.extend(
                rkyv::to_bytes::<rkyv::rancor::Error>(control)
                    .map(|value| value.to_vec())
                    .map_err(|err| TransportError::Encode(err.to_string()))?,
            );
            Ok(bytes)
        }
    }
}

pub(crate) fn decode_wire_envelope(bytes: &[u8]) -> Result<WireEnvelope, TransportError> {
    let Some((&tag, payload)) = bytes.split_first() else {
        return Err(TransportError::Decode("wire frame is empty".to_string()));
    };
    match tag {
        WIRE_TAG_INTRODUCTION => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let introduction =
                rkyv::from_bytes::<SignedIntroduction, rkyv::rancor::Error>(&aligned)
                    .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Introduction(introduction))
        }
        WIRE_TAG_PING => {
            if !payload.is_empty() {
                return Err(TransportError::Decode(
                    "ping wire frame must not contain payload".to_string(),
                ));
            }
            Ok(WireEnvelope::Ping)
        }
        WIRE_TAG_RELAY_PAYLOAD => Ok(WireEnvelope::Payload(Envelope::RelayPayload(
            decode_stream_payload(payload)?,
        ))),
        WIRE_TAG_ACK => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let ack = rkyv::from_bytes::<RemoteAckResolution, rkyv::rancor::Error>(&aligned)
                .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Payload(Envelope::Ack(ack)))
        }
        WIRE_TAG_CONTROL => {
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(payload);
            let control = rkyv::from_bytes::<ControlEnvelope, rkyv::rancor::Error>(&aligned)
                .map_err(|err| TransportError::Decode(err.to_string()))?;
            Ok(WireEnvelope::Payload(Envelope::Control(control)))
        }
        _ => Err(TransportError::Decode(format!(
            "unknown wire envelope tag {tag}"
        ))),
    }
}

pub(crate) fn encode_stream_payload(
    payload: &RelayPayload,
    bytes: &mut Vec<u8>,
) -> Result<(), TransportError> {
    bytes.push(payload.kind.wire_tag());
    encode_string(bytes, payload.domain.as_str())?;
    encode_string(bytes, payload.relay.as_str())?;
    encode_branch_key(bytes, &payload.key)?;
    encode_len(bytes, payload.metadata.len())?;
    for metadata in &payload.metadata {
        bytes.extend_from_slice(
            &metadata
                .ingested_at_low_watermark
                .unix_nanos()
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &metadata
                .ingested_at_high_watermark
                .unix_nanos()
                .to_be_bytes(),
        );
    }
    encode_len(bytes, payload.acks.len())?;
    for ack in &payload.acks {
        match ack {
            Some(ack) => {
                bytes.push(1);
                bytes.extend_from_slice(&ack.ack_id.to_be_bytes());
                encode_string(bytes, ack.reply_node_id.as_str())?;
            }
            None => bytes.push(0),
        }
    }
    match &payload.admission {
        Some(admission) => {
            bytes.push(1);
            bytes.extend_from_slice(&admission.ack_id.to_be_bytes());
            encode_string(bytes, admission.reply_node_id.as_str())?;
        }
        None => bytes.push(0),
    }
    encode_bytes(bytes, &payload.batch_ipc)?;
    Ok(())
}

pub(crate) fn decode_stream_payload(bytes: &[u8]) -> Result<RelayPayload, TransportError> {
    let mut cursor = WireCursor::new(bytes);
    let kind = RelayPayloadKind::from_wire_tag(cursor.read_u8()?)?;
    let domain_raw = cursor.read_string()?;
    let relay_raw = cursor.read_string()?;
    let key = cursor.read_branch_key()?;
    let metadata_count = cursor.read_len()?;
    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        metadata.push(RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
            ingested_at_high_watermark: Timestamp::from_unix_nanos(cursor.read_i64()?),
        });
    }
    let ack_count = cursor.read_len()?;
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
    let batch_ipc = cursor.read_bytes()?.to_vec();
    cursor.finish()?;
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

fn encode_len(bytes: &mut Vec<u8>, len: usize) -> Result<(), TransportError> {
    let len = u32::try_from(len)
        .map_err(|_| TransportError::Encode(format!("length {len} exceeds u32::MAX")))?;
    bytes.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

fn encode_branch_key(
    bytes: &mut Vec<u8>,
    key: &Option<Vec<RemoteRuntimeField>>,
) -> Result<(), TransportError> {
    let Some(fields) = key else {
        bytes.push(0);
        return Ok(());
    };
    if fields.is_empty() {
        return Err(TransportError::Encode(
            "branch key must contain at least one field".to_string(),
        ));
    }
    bytes.push(1);
    encode_len(bytes, fields.len())?;
    for field in fields {
        encode_string(bytes, field.name.as_str())?;
        encode_remote_value(bytes, &field.value)?;
    }
    Ok(())
}

fn encode_remote_value(
    bytes: &mut Vec<u8>,
    value: &RemoteRuntimeValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeValue::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        RemoteRuntimeValue::I8(value) => {
            bytes.push(1);
            bytes.push(value.to_be_bytes()[0]);
        }
        RemoteRuntimeValue::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I16(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::U32(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I32(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::U64(value) => {
            bytes.push(6);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::I64(value) => {
            bytes.push(7);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeValue::Bool(value) => {
            bytes.push(8);
            bytes.push(u8::from(*value));
        }
        RemoteRuntimeValue::String(value) => {
            bytes.push(9);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::Datetime(value) => {
            bytes.push(10);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeValue::F32(value) => {
            bytes.push(11);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeValue::F64(value) => {
            bytes.push(12);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeValue::Array(values) => {
            bytes.push(13);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeValue::Vec(values) => {
            bytes.push(14);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
    }
    Ok(())
}

fn encode_remote_element_value(
    bytes: &mut Vec<u8>,
    value: &RemoteRuntimeElementValue,
) -> Result<(), TransportError> {
    match value {
        RemoteRuntimeElementValue::U8(value) => {
            bytes.push(0);
            bytes.push(*value);
        }
        RemoteRuntimeElementValue::I8(value) => {
            bytes.push(1);
            bytes.push(value.to_be_bytes()[0]);
        }
        RemoteRuntimeElementValue::U16(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I16(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::U32(value) => {
            bytes.push(4);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I32(value) => {
            bytes.push(5);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::U64(value) => {
            bytes.push(6);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::I64(value) => {
            bytes.push(7);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        RemoteRuntimeElementValue::Bool(value) => {
            bytes.push(8);
            bytes.push(u8::from(*value));
        }
        RemoteRuntimeElementValue::String(value) => {
            bytes.push(9);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::Datetime(value) => {
            bytes.push(10);
            encode_string(bytes, value)?;
        }
        RemoteRuntimeElementValue::F32(value) => {
            bytes.push(11);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeElementValue::F64(value) => {
            bytes.push(12);
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        RemoteRuntimeElementValue::Array(values) => {
            bytes.push(13);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
        RemoteRuntimeElementValue::Vec(values) => {
            bytes.push(14);
            encode_len(bytes, values.len())?;
            for value in values {
                encode_remote_element_value(bytes, value)?;
            }
        }
    }
    Ok(())
}

fn encode_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), TransportError> {
    encode_len(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn encode_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), TransportError> {
    encode_bytes(bytes, value.as_bytes())
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
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
                let len = self.read_len()?;
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
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeValue::Array(values))
            }
            14 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
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
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
                Ok(RemoteRuntimeElementValue::Array(values))
            }
            14 => {
                let len = self.read_len()?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_remote_element_value()?);
                }
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
    use nervix_models::{DomainName, RelayName};

    use super::*;

    fn dummy_stream_payload(relay: &str) -> RelayPayload {
        RelayPayload {
            kind: RelayPayloadKind::Routed,
            domain: DomainName::try_from("orders").expect("valid domain"),
            relay: RelayName::try_from(relay).expect("valid relay"),
            key: None,
            batch_ipc: vec![1, 2, 3, 4],
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: None,
        }
    }

    #[test]
    fn relay_payload_branch_key_roundtrips_native_fields() {
        let mut payload = dummy_stream_payload("orders");
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

        let mut bytes = Vec::new();
        encode_stream_payload(&payload, &mut bytes).expect("payload should encode");
        let decoded = decode_stream_payload(&bytes).expect("payload should decode");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn relay_payload_without_branch_key_roundtrips_as_absent() {
        let payload = dummy_stream_payload("orders");
        let mut bytes = Vec::new();
        encode_stream_payload(&payload, &mut bytes).expect("payload should encode");
        let decoded = decode_stream_payload(&bytes).expect("payload should decode");

        assert_eq!(decoded.key, None);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn relay_payload_empty_branch_key_is_rejected() {
        let mut bytes = Vec::new();
        let error = encode_branch_key(&mut bytes, &Some(Vec::new()))
            .expect_err("empty branch key must be rejected");

        assert!(error.to_string().contains("at least one field"));
    }
}
