//! The framing of an Arrow IPC stream, read without decoding what it describes.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Walking a stream message by message and holding every length it declares, each
//!   message's metadata, its body and its column buffers, to the bytes that actually follow.
//! - **Depends on.** Arrow's IPC message schema.
//! - **Must not know.** Which contract a stream travels under, its schema, or its limits.
//!
//! Arrow's stream reader sizes each message's metadata and body from the lengths the stream
//! declares before it reads them, and slices each column buffer at the range its record batch
//! declares. A stream that overstates one would make the reader allocate what the stream claims
//! rather than what it carries, or panic on a range outside the body. Every decoder of a stream it
//! did not write therefore walks the framing here first.
//!
//! The one form accepted is the one a current writer produces: every message opens with the
//! continuation marker, and the end-of-stream marker ends the stream.

use arrow_ipc::{Message, MessageHeader, root_as_message};
use error_stack::Report;
use meticulous::ResultExt as _;
use thiserror::Error;

/// The marker that opens every message of a canonical Arrow IPC stream.
pub(super) const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// The width of the continuation marker and of the metadata length that follows it.
const FRAME_WORD: usize = 4;

/// Why a stream is not framed within the bytes that carry it.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IpcFramingDefect {
    #[error("the stream ends inside a message")]
    Truncated,
    #[error("a message does not open with the continuation marker")]
    Continuation,
    #[error("bytes follow the end-of-stream marker")]
    TrailingBytes,
    #[error("a message declares a negative metadata length")]
    MetadataLength,
    /// The metadata is not an Arrow message, for the reason its verifier gives.
    #[error("{reason}")]
    Message { reason: String },
    #[error("a message declares a body length outside this body")]
    BodyLength,
    #[error("a column buffer lies outside the body of its message")]
    Buffer,
}

/// The messages of an Arrow IPC stream, read one at a time without decoding their bodies, so a
/// stream's framing is checked before any column is allocated.
pub(super) struct IpcMessages<'a> {
    stream: &'a [u8],
    offset: usize,
}

impl<'a> IpcMessages<'a> {
    pub(super) fn new(stream: &'a [u8]) -> Self {
        Self { stream, offset: 0 }
    }

    /// Walks the whole stream: every message is framed within it, and every column buffer a
    /// record batch or a dictionary batch declares lies within the body of its message.
    pub(super) fn check(mut self) -> Result<(), Report<IpcFramingDefect>> {
        // Every message occupies at least its two frame words, so the walk ends within the stream.
        loop {
            if self.next_message()?.is_none() {
                return Ok(());
            }
        }
    }

    /// The next message's header, or `None` at the end-of-stream marker, which must end the
    /// stream. Its body and every column buffer lie within the stream before any caller reads it.
    pub(super) fn next_message(&mut self) -> Result<Option<Message<'a>>, Report<IpcFramingDefect>> {
        let marker = self.take(FRAME_WORD)?;
        if marker != CONTINUATION_MARKER {
            return Err(Report::new(IpcFramingDefect::Continuation));
        }
        let length_bytes = self.take(FRAME_WORD)?;
        let length = i32::from_le_bytes(
            length_bytes
                .try_into()
                .verified("take returned exactly the four bytes it was asked for"),
        );
        if length == 0 {
            if self.offset != self.stream.len() {
                return Err(Report::new(IpcFramingDefect::TrailingBytes));
            }
            return Ok(None);
        }
        let Ok(length) = usize::try_from(length) else {
            return Err(Report::new(IpcFramingDefect::MetadataLength));
        };
        let metadata = self.take(length)?;
        let message = root_as_message(metadata).map_err(|error| {
            Report::new(IpcFramingDefect::Message {
                reason: error.to_string(),
            })
        })?;
        let Ok(body_length) = usize::try_from(message.bodyLength()) else {
            return Err(Report::new(IpcFramingDefect::BodyLength));
        };
        self.take(body_length)?;
        Self::check_buffers(&message)?;
        Ok(Some(message))
    }

    /// The next `length` bytes of the stream.
    fn take(&mut self, length: usize) -> Result<&'a [u8], Report<IpcFramingDefect>> {
        let stream: &'a [u8] = self.stream;
        let Some(end) = self.offset.checked_add(length) else {
            return Err(Report::new(IpcFramingDefect::Truncated));
        };
        let Some(bytes) = stream.get(self.offset..end) else {
            return Err(Report::new(IpcFramingDefect::Truncated));
        };
        self.offset = end;
        Ok(bytes)
    }

    /// Holds every column buffer `message` declares to its body. Only a record batch and a
    /// dictionary batch declare any.
    fn check_buffers(message: &Message<'_>) -> Result<(), Report<IpcFramingDefect>> {
        let batch = match message.header_type() {
            MessageHeader::RecordBatch => message.header_as_record_batch(),
            MessageHeader::DictionaryBatch => {
                let Some(dictionary) = message.header_as_dictionary_batch() else {
                    return Ok(());
                };
                dictionary.data()
            }
            _ => return Ok(()),
        };
        let Some(batch) = batch else {
            return Ok(());
        };
        let Some(buffers) = batch.buffers() else {
            return Ok(());
        };
        let body_length = message.bodyLength();
        // The buffers are a vector of the message's verified metadata, which bounds their count.
        for buffer in buffers {
            let Some(end) = buffer.offset().checked_add(buffer.length()) else {
                return Err(Report::new(IpcFramingDefect::Buffer));
            };
            if buffer.offset() < 0 || buffer.length() < 0 || end > body_length {
                return Err(Report::new(IpcFramingDefect::Buffer));
            }
        }
        Ok(())
    }
}
