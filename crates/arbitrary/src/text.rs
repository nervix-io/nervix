//! Names, string literals, durations and byte sizes as NSPL and the vocabulary hold them.

use std::{fmt::Debug, num::NonZeroU64, str::FromStr};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DomainClockPeriod, DomainClockSkew, RequestedResourceVersion};

use crate::Arbitrary;

/// The longest name the vocabulary accepts, in bytes. It mirrors the bound every name type
/// enforces; a generated name past it fails to parse, loudly, the moment the two disagree.
const NAME_BYTES: u64 = 128;

/// The characters a generated name continues with after its keyword-proof head.
const NAME_TAIL: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_";

/// Pieces a generated string is assembled from. Beside plain text they hold every character the
/// NSPL lexer treats specially: both quote styles, the dollar-quote delimiters a renderer picks
/// and prefixes of them, backslashes and the escapes they could start, line breaks, a comma and
/// braces that separate configuration entries, and text outside ASCII, including a combining mark
/// and characters Unicode classes as whitespace.
const STRING_PIECES: [&str; 40] = [
    "a", "Z", "0", "9", " ", "  ", "_", "-", ".", "/", "'", "\"", "'\"", "$", "$s", "$s$", "s$",
    "$s_1$", "$roto", "$roto$", "\\", "\\n", "\\t", "\\\\", "\n", "\r", "\r\n", "\t", "\0", ",",
    "{", "}", "=", ";", "//", "é", "中文", "🎉", "e\u{301}", "\u{2028}",
];

/// Units a duration is written with, as NSPL keeps the spelling it was given.
const DURATION_UNITS: [&str; 14] = [
    "ns", "us", "ms", "s", "sec", "m", "min", "h", "hr", "d", "days", "w", "M", "y",
];

/// Units a byte size is written with.
const BYTE_SIZE_UNITS: [&str; 9] = ["B", "KB", "KiB", "MB", "MiB", "GB", "GiB", "TB", "TiB"];

impl Arbitrary<'_> {
    /// A name NSPL spells in every position that reads a name: one lower-case ASCII identifier that
    /// no NSPL keyword can match.
    ///
    /// A name is one letter, a letter and an underscore followed by more, or an underscore
    /// followed by more. No keyword is a single letter or starts with either head, so no
    /// generated name reads as a keyword, and the length reaches the vocabulary's bound.
    pub fn name<N>(&mut self) -> N
    where
        N: FromStr,
        N::Err: Debug,
    {
        let text = self.name_text();
        N::from_str(&text).assured("a generated name is a lower-case identifier within the bound")
    }

    /// The text of a name [`Self::name`] would build.
    pub fn name_text(&mut self) -> String {
        let letter = char::from(
            b'a'.checked_add(self.entropy.byte() % 26)
                .assured("a letter offset below 26 stays inside the ASCII lower-case range"),
        );
        let mut text = String::new();
        match self.entropy.byte() % 3 {
            0 => {
                text.push(letter);
                return text;
            }
            1 => {
                text.push(letter);
                text.push('_');
            }
            _ => text.push('_'),
        }
        let head = u64::try_from(text.len()).assured("a two-byte head fits in u64");
        let room = NAME_BYTES
            .checked_sub(head)
            .verified("the head is shorter than the name bound");
        let tail = self.entropy.boundary_biased(0..=room);
        for _ in 0..tail {
            let tail_count = u64::try_from(NAME_TAIL.len()).assured("a small table fits in u64");
            let chosen = self.entropy.up_to(
                tail_count
                    .checked_sub(1)
                    .assured("the tail alphabet is not empty"),
            );
            let chosen = usize::try_from(chosen).verified("an index below the table length");
            text.push(char::from(NAME_TAIL[chosen]));
        }
        text
    }

    /// Any string, assembled from pieces that stress every quoting and escaping rule and from code
    /// points drawn across the whole Unicode range.
    pub fn string(&mut self) -> String {
        let pieces = self.entropy.count(10);
        let mut text = String::new();
        for _ in 0..pieces {
            if self.entropy.flag() {
                let piece = self.entropy.pick(STRING_PIECES);
                text.push_str(piece);
            } else {
                text.push(self.code_point());
            }
        }
        text
    }

    /// A string holding at least one character.
    pub fn non_empty_string(&mut self) -> String {
        let mut text = self.string();
        if text.is_empty() {
            text.push(self.code_point());
        }
        text
    }

    /// One Unicode scalar value. Surrogate code points are not characters, so a choice among them
    /// takes the replacement character instead.
    fn code_point(&mut self) -> char {
        let value = self.entropy.between(0..=0x10_FFFF);
        let value = u32::try_from(value).verified("the range above ends below u32::MAX");
        match char::from_u32(value) {
            Some(character) => character,
            None => char::REPLACEMENT_CHARACTER,
        }
    }

    /// A duration as NSPL keeps it: a whole number and a unit, spelled as written.
    pub fn duration(&mut self) -> String {
        let count = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
        let unit = self.entropy.pick(DURATION_UNITS);
        format!("{count}{unit}")
    }

    /// A byte size as NSPL keeps it: a whole number and a unit, spelled as written.
    pub fn byte_size(&mut self) -> String {
        let count = self.entropy.boundary_biased(0..=u64::from(u16::MAX));
        let unit = self.entropy.pick(BYTE_SIZE_UNITS);
        format!("{count}{unit}")
    }

    /// A byte size of at least one byte, for a bound where zero would hold nothing.
    pub fn positive_byte_size(&mut self) -> String {
        let count = self.entropy.boundary_biased(1..=u64::from(u16::MAX));
        let unit = self.entropy.pick(BYTE_SIZE_UNITS);
        format!("{count}{unit}")
    }

    /// A positive domain-clock period: any number of nanoseconds a `u64` holds.
    pub fn clock_period(&mut self) -> DomainClockPeriod {
        let nanos = self.entropy.boundary_biased(1..=u64::MAX);
        DomainClockPeriod::from_nanos(
            NonZeroU64::new(nanos).verified("the range above starts at one"),
        )
    }

    /// A domain-clock skew: any number of nanoseconds a `u64` holds, zero included.
    pub fn clock_skew(&mut self) -> DomainClockSkew {
        DomainClockSkew::from_nanos(self.entropy.any_u64())
    }

    /// A positive count a `u64` holds.
    pub fn positive_u64(&mut self) -> NonZeroU64 {
        let value = self.entropy.boundary_biased(1..=u64::MAX);
        NonZeroU64::new(value).verified("the range above starts at one")
    }

    /// A resource version a statement asks for: a number, or the latest completed version.
    pub fn requested_version(&mut self) -> RequestedResourceVersion {
        if self.entropy.flag() {
            RequestedResourceVersion::Latest
        } else {
            RequestedResourceVersion::Number(self.entropy.any_u64())
        }
    }
}
