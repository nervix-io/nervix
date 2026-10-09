//! Generated encodings written through a bounded writer, compared with a reference of its rule.
//!
//! Layer: test harness.
//!
//! - **Owns.** Generated sequences of writes, encoder failures and refusal handling, and the
//!   reference outcome of each: which writes land, the bytes held, and the classification.
//! - **Depends on.** The bounded writer.
//! - **Must not know.** Any encoder that writes through it.

use std::{io::Write as _, num::NonZeroUsize};

use meticulous::OptionExt as _;

use super::{BoundedWrite, BoundedWriter, LimitReached};

/// One step of a generated encoder.
#[derive(Debug, bolero::TypeGenerator)]
enum Step {
    /// One write call of these bytes.
    Write(#[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=48))] Vec<u8>),
    /// The encoder fails with an error of its own and stops.
    Fail,
}

#[derive(Debug, bolero::TypeGenerator)]
struct Encoding {
    limit: u8,
    /// Whether the encoder keeps writing after a write was refused, as a serializer that buffers
    /// or retries might, instead of propagating the refusal at once.
    keeps_writing: bool,
    #[generator(bolero::generator::produce_with::<Vec<Step>>().len(0_usize..=12))]
    steps: Vec<Step>,
}

/// Why a generated encoder stopped early.
#[derive(Debug, PartialEq, Eq)]
enum EncoderError {
    Refused,
    Own,
}

impl Encoding {
    fn limit(&self) -> NonZeroUsize {
        NonZeroUsize::new(usize::from(self.limit) * 4 + 1)
            .assured("four times a byte, plus one, is at least one")
    }

    /// The outcome the writer's rule gives this encoder: a write lands whole while the bytes it
    /// adds stay within the limit; the first one that would cross it is refused, and so is every
    /// write after it, empty ones included. Reaching the limit decides the outcome whatever the
    /// encoder returns; otherwise the encoder's own failure is its outcome.
    fn reference(&self) -> Result<BoundedWrite, EncoderError> {
        let limit = self.limit().get();
        let mut held = Vec::new();
        let mut reached = false;
        for step in &self.steps {
            match step {
                Step::Write(chunk) => {
                    if !reached && held.len() + chunk.len() <= limit {
                        held.extend_from_slice(chunk);
                        continue;
                    }
                    reached = true;
                    if !self.keeps_writing {
                        return Ok(BoundedWrite::LimitReached);
                    }
                }
                Step::Fail => {
                    if reached {
                        return Ok(BoundedWrite::LimitReached);
                    }
                    return Err(EncoderError::Own);
                }
            }
        }
        if reached {
            return Ok(BoundedWrite::LimitReached);
        }
        Ok(BoundedWrite::Complete(held))
    }

    /// Runs the generated encoder against a bounded writer, checking after every write what it
    /// reports and that its capacity stays within the limit.
    fn encode(&self, writer: &mut BoundedWriter) -> Result<(), EncoderError> {
        let limit = self.limit();
        let mut refused = false;
        let mut held = 0;
        for step in &self.steps {
            match step {
                Step::Write(chunk) => {
                    match writer.write(chunk) {
                        Ok(written) => {
                            assert!(!refused, "a write landed after one was refused");
                            assert_eq!(written, chunk.len(), "a write lands whole");
                            held += chunk.len();
                        }
                        Err(error) => {
                            refused = true;
                            let reached = error
                                .get_ref()
                                .and_then(|inner| inner.downcast_ref::<LimitReached>());
                            assert_eq!(reached, Some(&LimitReached { limit }));
                            if !self.keeps_writing {
                                return Err(EncoderError::Refused);
                            }
                        }
                    }
                    assert_eq!(writer.len(), held, "a refused write lands nothing");
                    assert_eq!(writer.is_empty(), held == 0);
                    assert!(
                        writer.bytes.capacity() <= limit.get(),
                        "capacity past the limit"
                    );
                }
                Step::Fail => return Err(EncoderError::Own),
            }
        }
        Ok(())
    }
}

#[test]
fn bolero_writes_land_whole_within_the_limit_and_classify_the_encoding() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<Encoding>()
        .for_each(|encoding| {
            let outcome =
                BoundedWriter::write_with(encoding.limit(), |writer| encoding.encode(writer));
            let expected = encoding.reference();
            assert_eq!(outcome, expected);
            if let Ok(BoundedWrite::Complete(bytes)) = &outcome {
                assert!(bytes.len() <= encoding.limit().get());
            }
        });
}
