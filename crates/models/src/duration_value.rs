//! The value of a `DURATION`: a signed length of time counted in nanoseconds, and the literal that
//! writes one.
//!
//! Layer: vocabulary.
//! - **Owns.** The `DURATION` value every layer passes, the range it holds, and its literal: one
//!   or more parts, each a number and one of the units `ns`, `us`, `ms`, `s`, `m`, `h` and `d`, such
//!   as `30s`, `250ms` or `1h 30m`. Nothing else reads or writes that literal.
//! - **Depends on.** [`parse_duration_text`], the one reader of duration text, for the length of
//!   time the parts of a literal name.
//! - **Must not know.** Arrow, the expression VM, wire formats, codecs or any client.
//!
//! A literal names lengths of time of a fixed length only. A month or a year varies, so their
//! units are refused by name, and so is every other spelling duration text knows, such as `min`
//! or `w`, because a literal writes each unit one way. The text a value displays is a literal that
//! reads back as the same value: its non-zero parts from days down to nanoseconds, `0s` for zero,
//! and a leading `-` for a value below zero, which Plexus reads as the negation of the literal
//! after it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::fmt;

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator as _;
use thiserror::Error;

use crate::{DurationTextUnit, parse_duration_text};

/// A `DURATION`: a signed count of nanoseconds in 64 bits.
///
/// It is signed because the difference of two instants can be negative. Its order is the order of
/// the lengths of time it holds.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(transparent)]
pub struct DurationValue(i64);

impl DurationValue {
    /// The shortest `DURATION`, the most nanoseconds below zero 64 bits hold.
    pub const MIN: Self = Self(i64::MIN);
    /// The longest `DURATION`, a little over 106,751 days.
    pub const MAX: Self = Self(i64::MAX);
    /// No time at all.
    pub const ZERO: Self = Self(0);

    /// The `DURATION` of exactly `nanoseconds` nanoseconds, below zero when it is negative.
    pub const fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self(nanoseconds)
    }

    /// The signed count of nanoseconds this `DURATION` holds, as Arrow's `Duration(Nanosecond)`
    /// and the session protocol carry it.
    pub const fn nanoseconds(self) -> i64 {
        self.0
    }

    /// The `DURATION` of `count` whole `unit`s, below zero when `count` is negative, or `None`
    /// when that length lies beyond the 64-bit nanosecond range.
    pub fn of_whole_units(count: i64, unit: DurationLiteralUnit) -> Option<Self> {
        let unit_nanoseconds = i64::try_from(unit.nanoseconds())
            .assured("the longest unit, a day, is 86,400,000,000,000 nanoseconds");
        let nanoseconds = count.checked_mul(unit_nanoseconds)?;
        Some(Self(nanoseconds))
    }

    /// The length of time the parts of one literal name together, as `1h 30m` names ninety
    /// minutes.
    ///
    /// A part may hold a fraction, as `1.5s` does, only where duration text reads it exactly; a
    /// fraction of a nanosecond is refused rather than rounded. A literal names a length of time
    /// zero or longer, and one longer than [`DurationValue::MAX`] is refused.
    pub fn from_literal_parts(
        parts: &[DurationLiteralPart<'_>],
    ) -> Result<Self, Report<DurationLiteralError>> {
        let text = DurationLiteralPart::text_of(parts);
        let duration = parse_duration_text(&text).change_context(DurationLiteralError::Text)?;
        let Ok(nanoseconds) = i64::try_from(duration.as_nanos()) else {
            return Err(Report::new(DurationLiteralError::OutOfRange));
        };
        Ok(Self(nanoseconds))
    }
}

impl fmt::Display for DurationValue {
    /// Writes the value in the literal form, its parts from days down to nanoseconds, such as
    /// `1h 30m` or `0s`; the text of a value of zero or longer reads back as that value. A
    /// negative value, which no literal writes, carries a leading `-`, such as `-250ms`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 < 0 {
            formatter.write_str("-")?;
        }
        let mut remaining = self.0.unsigned_abs();
        if remaining == 0 {
            return formatter.write_str("0s");
        }
        let mut written = false;
        for unit in DurationLiteralUnit::iter() {
            let length = unit.nanoseconds();
            let count = remaining / length;
            remaining %= length;
            if count == 0 {
                continue;
            }
            if written {
                formatter.write_str(" ")?;
            }
            write!(formatter, "{count}{}", unit.spelling())?;
            written = true;
        }
        Ok(())
    }
}

/// A unit a `DURATION` literal writes, longest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter)]
pub enum DurationLiteralUnit {
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl DurationLiteralUnit {
    /// How a literal writes this unit.
    pub const fn spelling(self) -> &'static str {
        match self {
            Self::Day => "d",
            Self::Hour => "h",
            Self::Minute => "m",
            Self::Second => "s",
            Self::Millisecond => "ms",
            Self::Microsecond => "us",
            Self::Nanosecond => "ns",
        }
    }

    /// Nanoseconds in one of this unit.
    const fn nanoseconds(self) -> u64 {
        match self {
            Self::Day => 86_400_000_000_000,
            Self::Hour => 3_600_000_000_000,
            Self::Minute => 60_000_000_000,
            Self::Second => 1_000_000_000,
            Self::Millisecond => 1_000_000,
            Self::Microsecond => 1_000,
            Self::Nanosecond => 1,
        }
    }

    /// What the word after the number of a literal part names.
    ///
    /// A word duration text does not read as a unit is [`DurationLiteralWord::NotAUnit`]: it is
    /// whatever follows the number, and the literal is not one. A word it reads as a unit a
    /// literal does not write is refused with the unit it names.
    pub fn of_word(word: &str) -> DurationLiteralWord {
        for unit in Self::iter() {
            if word == unit.spelling() {
                return DurationLiteralWord::Unit(unit);
            }
        }
        let Some(text_unit) = DurationTextUnit::from_spelling(word) else {
            return DurationLiteralWord::NotAUnit;
        };
        let unit = word.to_string();
        if text_unit.has_fixed_length() {
            DurationLiteralWord::Refused(DurationLiteralError::NotALiteralUnit { unit })
        } else {
            DurationLiteralWord::Refused(DurationLiteralError::NoFixedLength { unit })
        }
    }
}

/// What the word after the number of a literal part names.
#[derive(Debug, Clone, PartialEq)]
pub enum DurationLiteralWord {
    /// A unit the literal writes.
    Unit(DurationLiteralUnit),
    /// A unit duration text knows that a literal does not write.
    Refused(DurationLiteralError),
    /// No unit at all.
    NotAUnit,
}

impl DurationLiteralWord {
    /// Whether the word names a unit at all, one a literal writes or one it refuses, so that the
    /// word belongs to the literal rather than to whatever follows it.
    pub const fn names_a_unit(&self) -> bool {
        !matches!(self, Self::NotAUnit)
    }
}

/// One part of a `DURATION` literal: a number as Plexus wrote it and its unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationLiteralPart<'a> {
    pub number: &'a str,
    pub unit: DurationLiteralUnit,
}

impl DurationLiteralPart<'_> {
    /// The text of a literal made of `parts`, each its number and unit, separated by spaces.
    pub fn text_of(parts: &[Self]) -> String {
        let mut text = String::new();
        for part in parts {
            if !text.is_empty() {
                text.push(' ');
            }
            text.push_str(part.number);
            text.push_str(part.unit.spelling());
        }
        text
    }
}

/// The units a literal takes, as every refusal of a unit lists them.
const LITERAL_UNITS: &str = "a DURATION literal takes the units ns, us, ms, s, m, h and d";

/// Why the parts of a literal name no `DURATION`.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum DurationLiteralError {
    /// A month or a year, whose length varies.
    #[error("duration unit '{unit}' has no fixed length; {LITERAL_UNITS}")]
    NoFixedLength { unit: String },
    /// A spelling of a unit, or weeks, which a literal does not write.
    #[error("duration unit '{unit}' is not written in a literal; {LITERAL_UNITS}")]
    NotALiteralUnit { unit: String },
    /// Duration text does not read the parts, such as a fraction of a nanosecond.
    #[error("its parts name no length of time exactly")]
    Text,
    /// The parts name more time than a `DURATION` holds.
    #[error("it does not fit a DURATION, which holds at most {max}", max = DurationValue::MAX)]
    OutOfRange,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::{
        DurationLiteralError, DurationLiteralPart, DurationLiteralUnit, DurationLiteralWord,
        DurationValue,
    };

    fn parts(parts: &[(&'static str, DurationLiteralUnit)]) -> Vec<DurationLiteralPart<'static>> {
        parts
            .iter()
            .map(|(number, unit)| DurationLiteralPart {
                number,
                unit: *unit,
            })
            .collect()
    }

    #[test]
    fn literal_parts_name_the_nanoseconds_they_add_up_to() {
        for (written, nanoseconds) in [
            (
                parts(&[("30", DurationLiteralUnit::Second)]),
                30_000_000_000,
            ),
            (
                parts(&[("250", DurationLiteralUnit::Millisecond)]),
                250_000_000,
            ),
            (
                parts(&[
                    ("1", DurationLiteralUnit::Hour),
                    ("30", DurationLiteralUnit::Minute),
                ]),
                5_400_000_000_000,
            ),
            (
                parts(&[("1.5", DurationLiteralUnit::Second)]),
                1_500_000_000,
            ),
            (parts(&[("0", DurationLiteralUnit::Nanosecond)]), 0),
            (
                parts(&[
                    ("2", DurationLiteralUnit::Day),
                    ("7", DurationLiteralUnit::Nanosecond),
                ]),
                172_800_000_000_007,
            ),
        ] {
            let value = DurationValue::from_literal_parts(&written)
                .assured("every listed literal names a duration a DURATION holds");
            assert_eq!(value, DurationValue::from_nanoseconds(nanoseconds));
        }
    }

    #[test]
    fn a_literal_longer_than_a_duration_holds_is_refused() {
        let error =
            DurationValue::from_literal_parts(&parts(&[("106752", DurationLiteralUnit::Day)]))
                .expect_err("106752 days exceed the longest DURATION");
        assert_eq!(*error.current_context(), DurationLiteralError::OutOfRange);
        let longest = DurationValue::from_literal_parts(&parts(&[
            ("106751", DurationLiteralUnit::Day),
            ("23", DurationLiteralUnit::Hour),
            ("47", DurationLiteralUnit::Minute),
            ("16", DurationLiteralUnit::Second),
            ("854", DurationLiteralUnit::Millisecond),
            ("775", DurationLiteralUnit::Microsecond),
            ("807", DurationLiteralUnit::Nanosecond),
        ]))
        .assured("these parts add up to exactly the longest DURATION");
        assert_eq!(longest, DurationValue::MAX);
    }

    #[test]
    fn whole_units_name_their_nanoseconds_and_nothing_beyond_the_range() {
        for (count, unit, nanoseconds) in [
            (30, DurationLiteralUnit::Second, Some(30_000_000_000)),
            (-250, DurationLiteralUnit::Millisecond, Some(-250_000_000)),
            (7, DurationLiteralUnit::Microsecond, Some(7_000)),
            (2, DurationLiteralUnit::Minute, Some(120_000_000_000)),
            (-3, DurationLiteralUnit::Hour, Some(-10_800_000_000_000)),
            (0, DurationLiteralUnit::Day, Some(0)),
            (i64::MAX, DurationLiteralUnit::Nanosecond, Some(i64::MAX)),
            (i64::MIN, DurationLiteralUnit::Nanosecond, Some(i64::MIN)),
            (
                106_751,
                DurationLiteralUnit::Day,
                Some(9_223_286_400_000_000_000),
            ),
            (
                -106_751,
                DurationLiteralUnit::Day,
                Some(-9_223_286_400_000_000_000),
            ),
            (106_752, DurationLiteralUnit::Day, None),
            (-106_752, DurationLiteralUnit::Day, None),
            (i64::MAX, DurationLiteralUnit::Microsecond, None),
            (i64::MIN, DurationLiteralUnit::Second, None),
        ] {
            assert_eq!(
                DurationValue::of_whole_units(count, unit),
                nanoseconds.map(DurationValue::from_nanoseconds),
                "{count} of {unit:?}"
            );
        }
    }

    #[test]
    fn a_fraction_of_a_nanosecond_is_refused_rather_than_rounded() {
        let error =
            DurationValue::from_literal_parts(&parts(&[("1.5", DurationLiteralUnit::Nanosecond)]))
                .expect_err("half a nanosecond is no whole count of nanoseconds");
        assert_eq!(*error.current_context(), DurationLiteralError::Text);
    }

    #[test]
    fn a_value_displays_as_the_literal_that_reads_back_as_it() {
        for (nanoseconds, text) in [
            (0, "0s"),
            (30_000_000_000, "30s"),
            (5_400_000_000_000, "1h 30m"),
            (1_500_000_000, "1s 500ms"),
            (129_600_000_000_000, "1d 12h"),
            (1, "1ns"),
            (-250_000_000, "-250ms"),
            (i64::MAX, "106751d 23h 47m 16s 854ms 775us 807ns"),
            (i64::MIN, "-106751d 23h 47m 16s 854ms 775us 808ns"),
        ] {
            assert_eq!(
                DurationValue::from_nanoseconds(nanoseconds).to_string(),
                text
            );
        }
    }

    #[test]
    fn a_word_names_a_literal_unit_a_refused_unit_or_none() {
        assert_eq!(
            DurationLiteralUnit::of_word("ms"),
            DurationLiteralWord::Unit(DurationLiteralUnit::Millisecond)
        );
        for word in ["M", "month", "months", "y", "yr", "yrs", "year", "years"] {
            assert_eq!(
                DurationLiteralUnit::of_word(word),
                DurationLiteralWord::Refused(DurationLiteralError::NoFixedLength {
                    unit: word.to_string()
                })
            );
        }
        for word in [
            "w", "weeks", "min", "sec", "seconds", "hours", "nsec", "usec", "msec",
        ] {
            assert_eq!(
                DurationLiteralUnit::of_word(word),
                DurationLiteralWord::Refused(DurationLiteralError::NotALiteralUnit {
                    unit: word.to_string()
                })
            );
        }
        for word in ["MS", "S", "FLUSH", "messages", "x"] {
            assert_eq!(
                DurationLiteralUnit::of_word(word),
                DurationLiteralWord::NotAUnit
            );
        }
    }
}
