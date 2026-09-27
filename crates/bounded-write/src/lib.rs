//! An output buffer that refuses every write that would take it past a byte limit.
//!
//! Layer: primitives.
//!
//! - **Owns.** The bounded buffer, the rule that a write either lands whole within the limit or
//!   is refused, and the classification of an encoding as complete or as having reached the limit.
//! - **Depends on.** The standard library and `thiserror`.
//! - **Must not know.** What is being encoded, which format writes it, or what a caller does with
//!   an encoding that reached its limit.
//!
//! An encoder writes through [`std::io::Write`], so any serializer that streams into a writer can
//! be held to a limit without knowing it is: the first write that would cross the limit fails, the
//! serializer propagates that failure, and nothing past the limit is ever buffered. Because the
//! serializer turns the failure into an error of its own, [`BoundedWriter::write_with`] decides the
//! outcome from the buffer rather than from that error, so an encoding that reached the limit is
//! never mistaken for a malformed value and a malformed value is never mistaken for an oversize one.

use std::{io, num::NonZeroUsize};

use thiserror::Error;

/// The outcome of one bounded encoding.
#[derive(Debug, PartialEq, Eq)]
pub enum BoundedWrite {
    /// The encoding completed within the limit. Its length is the exact encoded size.
    Complete(Vec<u8>),
    /// The encoding tried to write past the limit and was abandoned there.
    LimitReached,
}

/// The failure a [`BoundedWriter`] reports to the encoder writing into it once a write would have
/// crossed its limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("encoded output would exceed its limit of {limit} bytes")]
pub struct LimitReached {
    pub limit: NonZeroUsize,
}

/// A buffer that holds at most `limit` bytes and refuses any write that would hold more.
///
/// A write is all or nothing: either every byte of it fits and it lands, or none of it does and the
/// writer fails from then on. Encoders that call [`io::Write::write`] once and trust the count it
/// returns therefore never truncate silently. The buffer's capacity never exceeds the limit.
#[derive(Debug)]
pub struct BoundedWriter {
    bytes: Vec<u8>,
    limit: NonZeroUsize,
    reached: bool,
}

impl BoundedWriter {
    /// Runs `encode` against a writer bounded by `limit` and classifies what it produced.
    ///
    /// The encoding reached the limit when any write was refused, whatever `encode` returned
    /// afterwards. Otherwise an error from `encode` is its own failure, and success yields the
    /// complete encoding.
    pub fn write_with<E>(
        limit: NonZeroUsize,
        encode: impl FnOnce(&mut Self) -> Result<(), E>,
    ) -> Result<BoundedWrite, E> {
        let mut writer = Self {
            bytes: Vec::new(),
            limit,
            reached: false,
        };
        let encoded = encode(&mut writer);
        if writer.reached {
            return Ok(BoundedWrite::LimitReached);
        }
        encoded?;
        Ok(BoundedWrite::Complete(writer.bytes))
    }

    /// The bytes written so far.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether nothing has been written yet.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn refuse(&mut self) -> io::Error {
        self.reached = true;
        io::Error::other(LimitReached { limit: self.limit })
    }

    /// Grows the buffer so it can hold `required` bytes, doubling as a vector would but never past
    /// the limit, which `required` is already known not to exceed.
    fn reserve_within_limit(&mut self, required: usize) {
        let capacity = self.bytes.capacity();
        if required <= capacity {
            return;
        }
        let limit = self.limit.get();
        // Doubling that overflows `usize` is past any limit, so the limit is the target.
        let doubled = match capacity.checked_mul(2) {
            Some(doubled) => doubled,
            None => limit,
        };
        let target = doubled.max(required).min(limit);
        let additional = target - self.bytes.len();
        self.bytes.reserve_exact(additional);
    }
}

impl io::Write for BoundedWriter {
    fn write(&mut self, chunk: &[u8]) -> io::Result<usize> {
        if self.reached {
            return Err(self.refuse());
        }
        // A length that overflows `usize` is past any limit.
        let Some(required) = self.bytes.len().checked_add(chunk.len()) else {
            return Err(self.refuse());
        };
        if required > self.limit.get() {
            return Err(self.refuse());
        }
        self.reserve_within_limit(required);
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use meticulous::OptionExt as _;

    use super::*;

    fn limit(bytes: usize) -> NonZeroUsize {
        NonZeroUsize::new(bytes).assured("every limit these tests use is a positive literal")
    }

    fn write_chunks(limit_bytes: usize, chunks: &[&[u8]]) -> BoundedWrite {
        let written = BoundedWriter::write_with(limit(limit_bytes), |writer| {
            for chunk in chunks {
                writer.write_all(chunk)?;
            }
            Ok::<(), io::Error>(())
        });
        match written {
            Ok(written) => written,
            Err(error) => panic!("an in-memory write fails only by reaching its limit: {error}"),
        }
    }

    #[test]
    fn an_encoding_of_exactly_the_limit_completes() {
        assert_eq!(
            write_chunks(4, &[b"ab", b"cd"]),
            BoundedWrite::Complete(b"abcd".to_vec())
        );
    }

    #[test]
    fn one_byte_past_the_limit_is_refused() {
        assert_eq!(
            write_chunks(4, &[b"ab", b"cde"]),
            BoundedWrite::LimitReached
        );
    }

    #[test]
    fn a_write_crossing_the_limit_lands_none_of_its_bytes() {
        let mut observed_length = None;
        let written = BoundedWriter::write_with(limit(3), |writer| {
            writer.write_all(b"ab")?;
            let crossing = writer.write(b"cd");
            observed_length = Some(writer.len());
            crossing.map(|_| ())
        });
        assert_eq!(written.ok(), Some(BoundedWrite::LimitReached));
        assert_eq!(observed_length, Some(2));
    }

    #[test]
    fn every_write_after_the_limit_was_reached_is_refused() {
        let mut later_write_failed = false;
        let written = BoundedWriter::write_with(limit(2), |writer| {
            assert!(writer.write_all(b"abc").is_err());
            // An encoder that swallows the refusal still cannot write past the limit.
            later_write_failed = writer.write(b"").is_err() && writer.write_all(b"a").is_err();
            Ok::<(), io::Error>(())
        });
        assert_eq!(written.ok(), Some(BoundedWrite::LimitReached));
        assert!(later_write_failed);
    }

    #[test]
    fn the_refusal_names_the_limit() {
        let mut refusal = None;
        let written = BoundedWriter::write_with(limit(1), |writer| {
            let error = writer.write_all(b"ab").err();
            refusal = error.map(|error| error.to_string());
            Ok::<(), io::Error>(())
        });
        assert_eq!(written.ok(), Some(BoundedWrite::LimitReached));
        assert_eq!(
            refusal.as_deref(),
            Some("encoded output would exceed its limit of 1 bytes")
        );
    }

    #[test]
    fn an_encoder_failure_within_the_limit_is_its_own_error() {
        let written = BoundedWriter::write_with(limit(8), |writer| {
            writer.write_all(b"ab").map_err(|_| "unexpected refusal")?;
            Err("malformed value")
        });
        assert_eq!(written, Err("malformed value"));
    }

    #[test]
    fn an_encoder_failure_after_the_limit_is_classified_as_reaching_it() {
        let written = BoundedWriter::write_with(limit(1), |writer| {
            writer
                .write_all(b"ab")
                .map_err(|_| "the serializer's own wrapper")
        });
        assert_eq!(written, Ok(BoundedWrite::LimitReached));
    }

    #[test]
    fn an_empty_encoding_completes_empty() {
        assert_eq!(write_chunks(1, &[]), BoundedWrite::Complete(Vec::new()));
    }

    /// Sweeps every split of a payload into two writes against every limit around its length:
    /// the outcome depends on the total length alone, never on how the encoder chunked it.
    #[test]
    fn the_outcome_depends_only_on_the_total_length() {
        let payload = b"0123456789abcdef";
        for limit_bytes in 1..=payload.len() + 2 {
            for split in 0..=payload.len() {
                let (head, tail) = payload.split_at(split);
                let written = write_chunks(limit_bytes, &[head, tail]);
                if payload.len() <= limit_bytes {
                    assert_eq!(written, BoundedWrite::Complete(payload.to_vec()));
                } else {
                    assert_eq!(written, BoundedWrite::LimitReached);
                }
            }
        }
    }

    #[test]
    fn capacity_never_exceeds_the_limit() {
        for limit_bytes in [1, 7, 64, 1000, 4096] {
            let mut capacity = 0;
            let written = BoundedWriter::write_with(limit(limit_bytes), |writer| {
                for _ in 0..limit_bytes {
                    writer.write_all(b"x")?;
                    capacity = capacity.max(writer.bytes.capacity());
                }
                Ok::<(), io::Error>(())
            });
            assert_eq!(
                written.ok(),
                Some(BoundedWrite::Complete(vec![b'x'; limit_bytes]))
            );
            assert!(
                capacity <= limit_bytes,
                "capacity {capacity} exceeded limit {limit_bytes}"
            );
        }
    }

    #[test]
    fn reports_the_bytes_written_so_far() {
        let mut lengths = Vec::new();
        let written = BoundedWriter::write_with(limit(4), |writer| {
            lengths.push((writer.len(), writer.is_empty()));
            writer.write_all(b"abc")?;
            writer.flush()?;
            lengths.push((writer.len(), writer.is_empty()));
            Ok::<(), io::Error>(())
        });
        assert_eq!(written.ok(), Some(BoundedWrite::Complete(b"abc".to_vec())));
        assert_eq!(lengths, vec![(0, true), (3, false)]);
    }
}
