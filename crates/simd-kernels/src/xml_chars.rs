//! XML 1.0 character data: UTF-8 validation and the `Char` production in one pass.
//!
//! Layer: primitives.
//!
//! - **Owns.** Deciding whether bytes are UTF-8 text whose every character the W3C XML 1.0
//!   `Char` production admits, validating UTF-8 and classifying characters over the same span
//!   while it is in the first-level cache, and turning an admitted buffer into a `String` without
//!   validating it a second time.
//! - **Depends on.** `simdutf8`, the shared byte classes and portable SIMD selection.
//! - **Must not know.** Which service or format requires XML characters.
//!
//! `Char` admits tab, line feed, carriage return, U+0020 to U+D7FF, U+E000 to U+FFFD and every
//! supplementary-plane character. Valid UTF-8 cannot encode a surrogate, so once the bytes are
//! UTF-8 the excluded characters are the C0 controls other than those three, each one byte below
//! `0x20`, and the noncharacters U+FFFE and U+FFFF, the only sequences `EF BF BE` and `EF BF BF`.
//! The classifier finds both with vector compares: the controls as a byte class, the
//! noncharacters as three byte masks shifted onto the sequence's last byte, carrying a block's
//! last two lanes into the next block.

use std::ops::RangeInclusive;

use error_stack::Report;
use fearless_simd::{Level, dispatch, prelude::*};
use thiserror::Error;

use crate::{
    LEVEL, WORD_LANES,
    byte_class::{ByteClass, block_mask, tail_mask},
};

/// The bytes validated and classified together before the pass moves on: a whole number of blocks
/// that stays in the first-level cache while both checks read it.
const SPAN: usize = 64 * WORD_LANES;

const _: () = assert!(SPAN.is_multiple_of(WORD_LANES), "a span holds whole blocks");

/// Why bytes are not XML 1.0 character data. Invalid UTF-8 anywhere in the bytes outranks an
/// excluded character anywhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum XmlCharsError {
    #[error("bytes are not valid UTF-8")]
    InvalidUtf8,
    #[error("text contains a character the XML 1.0 Char production excludes")]
    ExcludedCharacter,
}

/// The verdict of one pass over a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum XmlCharsVerdict {
    Admitted,
    InvalidUtf8,
    Excluded,
}

/// Every C0 control except tab, line feed and carriage return.
struct ExcludedControl;

impl ByteClass for ExcludedControl {
    const ORDINARY: RangeInclusive<u8> = 0x20..=u8::MAX;
    const EXCLUDED: &'static [u8] = b"\t\n\r";
}

/// The lead byte of every three-byte sequence from U+F000 to U+FFFF.
struct NoncharacterLead;

impl ByteClass for NoncharacterLead {
    const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
    const LISTED: &'static [u8] = &[0xEF];
}

/// The middle byte of the sequences from U+FFC0 to U+FFFF.
struct NoncharacterMiddle;

impl ByteClass for NoncharacterMiddle {
    const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
    const LISTED: &'static [u8] = &[0xBF];
}

/// The last byte of U+FFFE and of U+FFFF.
struct NoncharacterLast;

impl ByteClass for NoncharacterLast {
    const ORDINARY: RangeInclusive<u8> = u8::MIN..=u8::MAX;
    const LISTED: &'static [u8] = &[0xBE, 0xBF];
}

/// The lead and middle masks of the previous block, whose last lanes may begin a noncharacter
/// that ends in the next block.
#[derive(Debug, Clone, Copy, Default)]
struct SequenceCarry {
    lead: u64,
    middle: u64,
}

/// The four masks one block contributes.
struct BlockMasks {
    controls: u64,
    lead: u64,
    middle: u64,
    last: u64,
}

impl BlockMasks {
    #[inline(always)]
    fn of_block<S: Simd>(simd: S, block: &[u8; WORD_LANES]) -> Self {
        Self {
            controls: block_mask::<S, ExcludedControl>(simd, block),
            lead: block_mask::<S, NoncharacterLead>(simd, block),
            middle: block_mask::<S, NoncharacterMiddle>(simd, block),
            last: block_mask::<S, NoncharacterLast>(simd, block),
        }
    }

    /// The masks of the bytes of `span` from `start`, fewer than a block of them.
    #[inline(always)]
    fn of_tail<S: Simd>(simd: S, span: &[u8], start: usize) -> Self {
        Self {
            controls: tail_mask::<S, ExcludedControl>(simd, span, start),
            lead: tail_mask::<S, NoncharacterLead>(simd, span, start),
            middle: tail_mask::<S, NoncharacterMiddle>(simd, span, start),
            last: tail_mask::<S, NoncharacterLast>(simd, span, start),
        }
    }

    /// The lanes holding an excluded control or the last byte of a noncharacter, with `carry`
    /// from the block before.
    #[inline(always)]
    fn excluded(&self, carry: SequenceCarry) -> u64 {
        // Bit `i` of each shifted mask describes byte `i - 1` or `i - 2`, reaching back into the
        // previous block's highest lanes for the first lanes of this one.
        let middle_before = (self.middle << 1) | (carry.middle >> (WORD_LANES - 1));
        let lead_two_before = (self.lead << 2) | (carry.lead >> (WORD_LANES - 2));
        self.controls | (self.last & middle_before & lead_two_before)
    }

    #[inline(always)]
    fn carry(&self) -> SequenceCarry {
        SequenceCarry {
            lead: self.lead,
            middle: self.middle,
        }
    }
}

/// What classifying one span found, and what it carries into the next span.
struct SpanClassification {
    excluded: bool,
    carry: SequenceCarry,
}

#[inline(always)]
fn classify_span<S: Simd>(simd: S, span: &[u8], carry: SequenceCarry) -> SpanClassification {
    let (blocks, tail) = span.as_chunks::<WORD_LANES>();
    let mut carry = carry;
    let mut excluded = 0_u64;
    for block in blocks {
        let masks = BlockMasks::of_block(simd, block);
        excluded |= masks.excluded(carry);
        carry = masks.carry();
    }
    if !tail.is_empty() {
        // The tail's masks are clear past its last byte, so its excluded lanes are too.
        let masks = BlockMasks::of_tail(simd, span, span.len() - tail.len());
        excluded |= masks.excluded(carry);
        carry = masks.carry();
    }
    SpanClassification {
        excluded: excluded != 0,
        carry,
    }
}

/// The last position at or before `end` that starts a character, or the end of `bytes`. A
/// continuation byte never starts one, and a character is at most four bytes long, so no more
/// than three positions are stepped back. Should the bytes hold four continuation bytes in a row,
/// they are not UTF-8, and the range that begins with the fourth reports so.
fn character_start_before(bytes: &[u8], end: usize) -> usize {
    let mut start = end;
    for _ in 0..3 {
        match bytes.get(start) {
            Some(byte) if byte & 0xC0 == 0x80 => start -= 1,
            Some(_) | None => break,
        }
    }
    start
}

/// The verdict on `bytes` at the SIMD level `level`, from one pass that validates and classifies
/// each span in turn. Once a character is excluded, the pass only validates the rest, because
/// invalid UTF-8 outranks it.
fn verdict(level: Level, bytes: &[u8]) -> XmlCharsVerdict {
    let mut carry = SequenceCarry::default();
    let mut excluded = false;
    let mut validated = 0;
    let mut span_start = 0;
    while span_start < bytes.len() {
        let span_end = bytes.len().min(span_start + SPAN);
        if !excluded {
            let span = &bytes[span_start..span_end];
            let classified = dispatch!(level, simd => classify_span(simd, span, carry));
            excluded = classified.excluded;
            carry = classified.carry;
        }
        let validated_end = character_start_before(bytes, span_end);
        if simdutf8::basic::from_utf8(&bytes[validated..validated_end]).is_err() {
            return XmlCharsVerdict::InvalidUtf8;
        }
        validated = validated_end;
        span_start = span_end;
    }
    if excluded {
        XmlCharsVerdict::Excluded
    } else {
        XmlCharsVerdict::Admitted
    }
}

fn admits_with_level(level: Level, text: &str) -> bool {
    let classified =
        dispatch!(level, simd => classify_span(simd, text.as_bytes(), SequenceCarry::default()));
    !classified.excluded
}

/// XML 1.0 character data: UTF-8 text whose every character the W3C XML 1.0 `Char` production
/// admits.
///
/// ```
/// use nervix_simd_kernels::{XmlChars, XmlCharsError};
///
/// let text = XmlChars::into_string("{\"note\":\"\u{1F600}\"}".as_bytes().to_vec());
/// assert_eq!(text.ok().as_deref(), Some("{\"note\":\"\u{1F600}\"}"));
///
/// let noncharacter = XmlChars::into_string("\u{FFFF}".as_bytes().to_vec());
/// let error = noncharacter.err().map(|report| *report.current_context());
/// assert_eq!(error, Some(XmlCharsError::ExcludedCharacter));
///
/// assert!(!XmlChars::admits("bell\u{7}"));
/// ```
pub struct XmlChars;

impl XmlChars {
    /// Takes `bytes` as text when they are UTF-8 whose every character is admitted, validating and
    /// classifying them in one pass at the SIMD level the process selected.
    pub fn into_string(bytes: Vec<u8>) -> error_stack::Result<String, XmlCharsError> {
        match verdict(*LEVEL.get_or_init(Level::new), &bytes) {
            XmlCharsVerdict::Admitted => {
                // SAFETY: the ranges the pass handed `simdutf8` partition `bytes`, `simdutf8`
                // accepted every one of them as UTF-8, and UTF-8 strings joined end to end are
                // UTF-8.
                Ok(unsafe { String::from_utf8_unchecked(bytes) })
            }
            XmlCharsVerdict::InvalidUtf8 => Err(Report::new(XmlCharsError::InvalidUtf8)),
            XmlCharsVerdict::Excluded => Err(Report::new(XmlCharsError::ExcludedCharacter)),
        }
    }

    /// Whether every character of `text` is admitted.
    pub fn admits(text: &str) -> bool {
        admits_with_level(*LEVEL.get_or_init(Level::new), text)
    }
}

#[cfg(test)]
#[path = "xml_chars_tests.rs"]
mod tests;
