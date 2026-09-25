//! The optional batching contract an emitter declares.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The two hard limits of `BATCH MAX MESSAGES <n> MAX SIZE <bytes>` as validated
//!   values: the member count, bounded by what the message histograms track, and the exact encoded
//!   size, as a positive whole number of bytes with its canonical NSPL spelling.
//! - **Depends on.** Serialization primitives.
//! - **Must not know.** Which sink or codec a policy is declared on, how a batch is packed, encoded
//!   or measured, or how a connector publishes it.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroU64},
    str::FromStr,
};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Why a batching limit is not a value an emitter can declare.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EmitterBatchLimitError {
    #[error(
        "BATCH MAX MESSAGES must be between 1 and {max}, found {value}",
        max = BatchMessageLimit::MAX
    )]
    MessageLimitOutOfRange { value: u64 },
    #[error("BATCH MAX SIZE must be greater than zero")]
    ZeroSize,
    #[error(
        "BATCH MAX SIZE '{literal}' must be a whole number followed by B, KB, KiB, MB, MiB, GB, \
         GiB, TB or TiB"
    )]
    MalformedSize { literal: String },
    #[error("BATCH MAX SIZE '{literal}' exceeds the largest size a 64-bit byte count can hold")]
    SizeOverflow { literal: String },
}

/// The most members one batch may carry: a count from 1 to [`BatchMessageLimit::MAX`].
///
/// The upper bound is the largest batch the message histograms track, so a batch at the limit is
/// still observable as one batch.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(try_from = "u32", into = "u32")]
pub struct BatchMessageLimit(NonZeroU32);

impl BatchMessageLimit {
    /// The largest member count an emitter may declare.
    pub const MAX: u32 = 65_536;

    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

impl TryFrom<u64> for BatchMessageLimit {
    type Error = EmitterBatchLimitError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let out_of_range = EmitterBatchLimitError::MessageLimitOutOfRange { value };
        let Ok(count) = u32::try_from(value) else {
            return Err(out_of_range);
        };
        if count > Self::MAX {
            return Err(out_of_range);
        }
        let Some(count) = NonZeroU32::new(count) else {
            return Err(out_of_range);
        };
        Ok(Self(count))
    }
}

impl TryFrom<u32> for BatchMessageLimit {
    type Error = EmitterBatchLimitError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::try_from(u64::from(value))
    }
}

impl From<BatchMessageLimit> for u32 {
    fn from(limit: BatchMessageLimit) -> Self {
        limit.0.get()
    }
}

impl fmt::Display for BatchMessageLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A unit a batch size is written in. Units are matched without regard to case, as every
/// byte-size literal is, and rendered in the spelling below.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::Display,
    strum::EnumString,
)]
#[strum(ascii_case_insensitive)]
pub enum ByteSizeUnit {
    B,
    KB,
    KiB,
    MB,
    MiB,
    GB,
    GiB,
    TB,
    TiB,
}

impl ByteSizeUnit {
    /// How many bytes one of this unit holds.
    pub const fn bytes(self) -> u64 {
        match self {
            Self::B => 1,
            Self::KB => 1_000,
            Self::KiB => 1 << 10,
            Self::MB => 1_000_000,
            Self::MiB => 1 << 20,
            Self::GB => 1_000_000_000,
            Self::GiB => 1 << 30,
            Self::TB => 1_000_000_000_000,
            Self::TiB => 1 << 40,
        }
    }
}

/// The exact encoded size a batch payload may reach: a positive whole number of bytes, kept with
/// the unit it was written in.
///
/// It is written as a whole number and a unit, such as `1MiB`, and rendered exactly as written, so
/// `SHOW CREATE` reproduces the declaration. A fractional size, a size past the 64-bit byte range,
/// and a size of zero are rejected rather than rounded, saturated or treated as unbounded. Limits
/// compare by [`PayloadSizeLimit::bytes`]; two spellings of the same size are different
/// declarations.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct PayloadSizeLimit {
    /// The size in bytes, which is always a whole number of `unit`.
    bytes: NonZeroU64,
    unit: ByteSizeUnit,
}

impl PayloadSizeLimit {
    /// `count` of `unit`, or nothing when that many bytes do not fit in 64 bits.
    pub const fn new(count: NonZeroU64, unit: ByteSizeUnit) -> Option<Self> {
        let Some(bytes) = count.get().checked_mul(unit.bytes()) else {
            return None;
        };
        let Some(bytes) = NonZeroU64::new(bytes) else {
            return None;
        };
        Some(Self { bytes, unit })
    }

    pub const fn bytes(self) -> NonZeroU64 {
        self.bytes
    }
}

impl FromStr for PayloadSizeLimit {
    type Err = EmitterBatchLimitError;

    fn from_str(literal: &str) -> Result<Self, Self::Err> {
        let malformed = || EmitterBatchLimitError::MalformedSize {
            literal: literal.to_string(),
        };
        let overflow = || EmitterBatchLimitError::SizeOverflow {
            literal: literal.to_string(),
        };
        let digits_end = literal
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(literal.len());
        let (digits, suffix) = literal.split_at(digits_end);
        if digits.is_empty() {
            return Err(malformed());
        }
        let Ok(unit) = suffix.parse::<ByteSizeUnit>() else {
            return Err(malformed());
        };
        let Ok(count) = digits.parse::<u64>() else {
            return Err(overflow());
        };
        let Some(count) = NonZeroU64::new(count) else {
            return Err(EmitterBatchLimitError::ZeroSize);
        };
        Self::new(count, unit).ok_or_else(overflow)
    }
}

impl fmt::Display for PayloadSizeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.bytes.get() / self.unit.bytes();
        write!(f, "{count}{}", self.unit)
    }
}

/// `BATCH MAX MESSAGES <n> MAX SIZE <bytes>`: the two hard limits every batch an emitter publishes
/// stays within. Neither limit has a default and neither is derived from the other.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct EmitterBatchPolicy {
    pub max_messages: BatchMessageLimit,
    pub max_size: PayloadSizeLimit,
}

impl fmt::Display for EmitterBatchPolicy {
    /// The clause after its leading `BATCH`, as `DESCRIBE EMITTER` reports it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MAX MESSAGES {} MAX SIZE {}",
            self.max_messages, self.max_size
        )
    }
}

/// Whether a sink accepts an emitter without the batching clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterBatchRequirement {
    /// Without the clause the sink publishes one record per message, event or frame, or keeps the
    /// request grouping it has always had.
    Optional,
    /// Every write carries several rows, and there is no unbounded form of it.
    Required,
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::bytes("1B", 1)]
    #[case::lowercase_unit("2kib", 2048)]
    #[case::binary("1MiB", 1 << 20)]
    #[case::decimal("3MB", 3_000_000)]
    #[case::terabytes("1TB", 1_000_000_000_000)]
    #[case::tebibytes("2TiB", 2 << 40)]
    fn parses_whole_sizes(#[case] literal: &str, #[case] bytes: u64) {
        let size = literal.parse::<PayloadSizeLimit>().expect("size parses");

        assert_eq!(size.bytes().get(), bytes);
    }

    #[rstest]
    #[case::fraction("1.5MiB")]
    #[case::no_unit("100")]
    #[case::unknown_unit("1PiB")]
    #[case::no_digits("MiB")]
    #[case::empty("")]
    fn rejects_malformed_sizes(#[case] literal: &str) {
        let error = literal
            .parse::<PayloadSizeLimit>()
            .expect_err("size is malformed");

        assert_eq!(
            error,
            EmitterBatchLimitError::MalformedSize {
                literal: literal.to_string()
            }
        );
    }

    #[rstest]
    #[case::multiplication("16777216TiB")]
    #[case::digits("99999999999999999999B")]
    fn rejects_sizes_past_the_byte_range(#[case] literal: &str) {
        let error = literal
            .parse::<PayloadSizeLimit>()
            .expect_err("size overflows");

        assert_eq!(
            error,
            EmitterBatchLimitError::SizeOverflow {
                literal: literal.to_string()
            }
        );
    }

    #[test]
    fn rejects_a_zero_size() {
        assert_eq!(
            "0MiB".parse::<PayloadSizeLimit>(),
            Err(EmitterBatchLimitError::ZeroSize)
        );
    }

    #[rstest]
    #[case::mebibytes("1MiB")]
    #[case::bytes_of_a_mebibyte("1048576B")]
    #[case::kibibytes_of_a_mebibyte("1024KiB")]
    #[case::decimal("256KB")]
    #[case::terabytes("3TB")]
    fn renders_the_size_as_written(#[case] literal: &str) {
        let size = literal.parse::<PayloadSizeLimit>().expect("size parses");

        assert_eq!(size.to_string(), literal);
    }

    #[test]
    fn renders_the_unit_in_its_own_spelling() {
        let size = "4mib".parse::<PayloadSizeLimit>().expect("size parses");

        assert_eq!(size.to_string(), "4MiB");
        assert_eq!(size.bytes().get(), 4 << 20);
    }

    #[test]
    fn compares_spellings_of_one_size_by_bytes() {
        let binary = "1MiB".parse::<PayloadSizeLimit>().expect("size parses");
        let bytes = "1048576B".parse::<PayloadSizeLimit>().expect("size parses");

        assert_eq!(binary.bytes(), bytes.bytes());
        assert_ne!(binary, bytes);
    }

    #[rstest]
    #[case::one(1)]
    #[case::maximum(65_536)]
    fn accepts_message_limits_in_range(#[case] value: u64) {
        let limit = BatchMessageLimit::try_from(value).expect("limit is in range");

        assert_eq!(u64::from(limit.get().get()), value);
    }

    #[rstest]
    #[case::zero(0)]
    #[case::above_maximum(65_537)]
    #[case::above_u32(u64::from(u32::MAX) + 1)]
    fn rejects_message_limits_out_of_range(#[case] value: u64) {
        assert_eq!(
            BatchMessageLimit::try_from(value),
            Err(EmitterBatchLimitError::MessageLimitOutOfRange { value })
        );
    }

    #[test]
    fn serde_rejects_a_message_limit_out_of_range() {
        let error = serde_json::from_str::<BatchMessageLimit>("65537")
            .expect_err("an out-of-range limit must not load");

        assert!(error.to_string().contains("between 1 and 65536"));
    }

    #[test]
    fn renders_the_policy_as_describe_reports_it() {
        let policy = EmitterBatchPolicy {
            max_messages: BatchMessageLimit(nonzero!(500u32)),
            max_size: PayloadSizeLimit::new(nonzero!(1u64), ByteSizeUnit::MiB)
                .expect("one mebibyte fits in 64 bits"),
        };

        assert_eq!(policy.to_string(), "MAX MESSAGES 500 MAX SIZE 1MiB");
    }
}
