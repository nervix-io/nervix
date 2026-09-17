//! Replies larger than one frame.
//!
//! The server encodes the complete reply once, from one read of its state, and cuts that single
//! frame into parts. The client joins the parts in order and verifies the result as a frame of its
//! own, so a transferred reply is exactly as trusted as one that fit a frame, can never mix two
//! revisions of the reply, and is never delivered truncated.

use bytes::Bytes;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use thiserror::Error;

use crate::{
    codec::{DecodeError, Decoder, EncodedUnion, Encoder, wire_size},
    common::RequestId,
    frame::{EncodedFrame, ServerFrame, VerifiedFrame},
    limits::{MIN_FRAME_BYTES, SessionLimits},
    server::{Reply, ServerMessage, finish_server_message},
    wire,
};

/// The bytes a transfer part's frame spends on everything but its chunk: the frame header, the
/// server message, reply and part tables with their vtables, the chunk's length prefix, and
/// alignment padding. A test encodes parts at the largest chunk this allows to keep it honest.
pub(crate) const TRANSFER_PART_ENVELOPE_BYTES: usize = 256;

const _: () = assert!(TRANSFER_PART_ENVELOPE_BYTES < MIN_FRAME_BYTES);

/// The parts of one reply, encoded one at a time as they are taken.
#[derive(Debug)]
pub struct TransferParts {
    request_id: RequestId,
    reply: Bytes,
    offset: usize,
    limits: SessionLimits,
}

impl TransferParts {
    pub(crate) fn new(request_id: RequestId, reply: Bytes, limits: &SessionLimits) -> Self {
        Self {
            request_id,
            reply,
            offset: 0,
            limits: *limits,
        }
    }

    /// The size of the complete reply frame the parts carry.
    pub fn total_bytes(&self) -> usize {
        self.reply.len()
    }

    fn chunk_bytes(&self) -> usize {
        self.limits
            .frame_bytes()
            .checked_sub(TRANSFER_PART_ENVELOPE_BYTES)
            .assured("a const assertion holds the part envelope below the smallest frame limit")
    }
}

impl Iterator for TransferParts {
    type Item = EncodedFrame<ServerFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.reply.len() {
            return None;
        }
        let remaining = self
            .reply
            .len()
            .checked_sub(self.offset)
            .verified("the check above returned unless the offset is inside the reply");
        let chunk_len = remaining.min(self.chunk_bytes());
        let end = self
            .offset
            .checked_add(chunk_len)
            .verified("the chunk ends at or before the end of the reply");
        let mut encoder = Encoder::new(self.limits.frame_bytes(), &self.limits);
        let chunk = encoder
            .bytes("TransferPart.chunk", &self.reply[self.offset..end])
            .assured("a chunk leaves the part envelope room within the frame limit");
        let part = wire::TransferPart::create(
            encoder.fbb(),
            &wire::TransferPartArgs {
                total_bytes: wire_size(self.reply.len()),
                offset: wire_size(self.offset),
                chunk: Some(chunk),
            },
        );
        let reply = wire::Reply::create(
            encoder.fbb(),
            &wire::ReplyArgs {
                request_id: self.request_id.wire(),
                body_type: wire::ReplyBody::TransferPart,
                body: Some(part.as_union_value()),
            },
        );
        let frame =
            finish_server_message(encoder, EncodedUnion::new(wire::ServerBody::Reply, reply))
                .assured("a chunk leaves the part envelope room within the frame limit");
        self.offset = end;
        Some(frame)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self
            .reply
            .len()
            .checked_sub(self.offset)
            .assured("a part never ends past the end of the reply");
        let parts = remaining.div_ceil(self.chunk_bytes());
        (parts, Some(parts))
    }
}

impl ExactSizeIterator for TransferParts {}

/// One part of a reply too large for a single frame, read in place from its frame.
#[derive(Debug, Clone)]
pub struct TransferPart {
    frame: VerifiedFrame<ServerFrame>,
    request_id: RequestId,
    total_bytes: usize,
    offset: usize,
}

impl TransferPart {
    pub(crate) fn decode(
        frame: &VerifiedFrame<ServerFrame>,
        decoder: Decoder<'_>,
        request_id: RequestId,
        part: wire::TransferPart<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let total_bytes = decoder.non_zero("TransferPart.total_bytes", part.total_bytes())?;
        let total_bytes = decoder.size("TransferPart.total_bytes", total_bytes.get())?;
        let offset = decoder.size("TransferPart.offset", part.offset())?;
        let chunk = part.chunk();
        if chunk.is_empty() {
            return Err(Report::new(DecodeError::EmptyCollection {
                field: "TransferPart.chunk",
            }));
        }
        let end = offset.checked_add(chunk.len());
        let Some(end) = end else {
            return Err(Report::new(DecodeError::OutOfRange {
                field: "TransferPart.offset",
                value: part.offset(),
            }));
        };
        if end > total_bytes {
            return Err(Report::new(DecodeError::InvalidValue {
                field: "TransferPart.chunk",
                kind: "chunk within the transfer's total bytes",
            }));
        }
        Ok(Self {
            frame: frame.clone(),
            request_id,
            total_bytes,
            offset,
        })
    }

    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// The size of the complete reply frame.
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Where the chunk starts within the complete reply frame.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// The chunk, read in place from the part's frame.
    pub fn chunk(&self) -> &[u8] {
        let reply = self
            .frame
            .root()
            .body_as_reply()
            .assured("this value is only decoded from a frame whose body is a reply");
        let part = reply
            .body_as_transfer_part()
            .assured("this value is only decoded from a reply whose body is a transfer part");
        part.chunk().bytes()
    }
}

/// Why a transfer could not be reassembled.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TransferError {
    #[error("a part of request {actual} arrived for the transfer of request {expected}")]
    WrongRequest {
        expected: RequestId,
        actual: RequestId,
    },
    #[error("the transfer declares {total_bytes} bytes, above the transfer limit of {limit}")]
    TooLarge { total_bytes: usize, limit: usize },
    #[error("a part declares {actual} total bytes where the transfer declared {expected}")]
    TotalChanged { expected: usize, actual: usize },
    #[error("a part starts at offset {actual} where offset {expected} was expected")]
    OutOfOrder { expected: usize, actual: usize },
    #[error("the transfer received no parts")]
    NoParts,
    #[error("the transfer holds {received} of its {total_bytes} bytes")]
    Incomplete { received: usize, total_bytes: usize },
    #[error("the reassembled frame is not a valid reply")]
    InvalidReply,
    #[error("the reassembled frame is not a complete reply to request {expected}")]
    NotReply { expected: RequestId },
}

/// The parts of one reply received so far.
#[derive(Debug)]
pub struct TransferAssembly {
    request_id: RequestId,
    total_bytes: Option<usize>,
    bytes: Vec<u8>,
    limits: SessionLimits,
}

impl TransferAssembly {
    pub fn new(request_id: RequestId, limits: &SessionLimits) -> Self {
        Self {
            request_id,
            total_bytes: None,
            bytes: Vec::new(),
            limits: *limits,
        }
    }

    /// Appends the next part. Parts must arrive in order, for this request, with one total.
    ///
    /// A refused part leaves the assembly as it was.
    pub fn append(&mut self, part: &TransferPart) -> Result<(), Report<TransferError>> {
        if part.request_id != self.request_id {
            return Err(Report::new(TransferError::WrongRequest {
                expected: self.request_id,
                actual: part.request_id,
            }));
        }
        let first_part = match self.total_bytes {
            Some(total_bytes) if total_bytes != part.total_bytes => {
                return Err(Report::new(TransferError::TotalChanged {
                    expected: total_bytes,
                    actual: part.total_bytes,
                }));
            }
            Some(_) => false,
            None if part.total_bytes > self.limits.transfer_bytes() => {
                return Err(Report::new(TransferError::TooLarge {
                    total_bytes: part.total_bytes,
                    limit: self.limits.transfer_bytes(),
                }));
            }
            None => true,
        };
        if part.offset != self.bytes.len() {
            return Err(Report::new(TransferError::OutOfOrder {
                expected: self.bytes.len(),
                actual: part.offset,
            }));
        }
        if first_part {
            self.total_bytes = Some(part.total_bytes);
            self.bytes.reserve_exact(part.total_bytes);
        }
        self.bytes.extend_from_slice(part.chunk());
        Ok(())
    }

    /// Whether every byte of the reply has arrived.
    pub fn is_complete(&self) -> bool {
        self.total_bytes == Some(self.bytes.len())
    }

    /// The bytes received so far.
    pub fn received_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Verifies the reassembled frame and decodes the reply it holds.
    pub fn finish(self) -> Result<Reply, Report<TransferError>> {
        let Some(total_bytes) = self.total_bytes else {
            return Err(Report::new(TransferError::NoParts));
        };
        if self.bytes.len() != total_bytes {
            return Err(Report::new(TransferError::Incomplete {
                received: self.bytes.len(),
                total_bytes,
            }));
        }
        let verified = VerifiedFrame::<ServerFrame>::verify_within(
            Bytes::from(self.bytes),
            self.limits.transfer_bytes(),
            &self.limits,
        );
        let frame = match verified {
            Ok(frame) => frame,
            Err(error) => return Err(error.change_context(TransferError::InvalidReply)),
        };
        let message = match ServerMessage::decode(&frame) {
            Ok(message) => message,
            Err(error) => return Err(error.change_context(TransferError::InvalidReply)),
        };
        match message {
            ServerMessage::Reply(reply) if reply.request_id == self.request_id => Ok(reply),
            _ => Err(Report::new(TransferError::NotReply {
                expected: self.request_id,
            })),
        }
    }
}
