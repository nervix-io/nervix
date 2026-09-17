//! Reading text columns as DATETIME values in a compiled format.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The kernel behind `parse_datetime`: one pass over a text column that reads every
//!   valid lane in a compiled format, checks every field it read, and resolves the instant the lane
//!   names, recording why each lane that names no instant failed.
//! - **Depends on.** Compiled datetime parsers, the time zones of the datetime kernels, Jiff's civil
//!   calendar, and Arrow string and timestamp arrays.
//! - **Must not know.** Registers, programs, spans, the host's locale, or how a failed lane is
//!   recorded as a row error.
//!
//! Reading is strict. A lane must match its format from its first byte to its last, every field it
//! reads must lie in its range, its date must exist, and a day of the week it reads must agree with
//! that date. Nothing is guessed or normalized: a lane that breaks a rule fails instead of reading
//! as a nearby instant. Only letter case in names and `AM`/`PM`, and whether an unpadded number
//! carries a leading zero, are not checked.

use std::fmt;

use arrow_array::{Array, StringArray, TimestampNanosecondArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use jiff::{
    civil::{Date, ISOWeekDate, Time, Weekday},
    tz::Offset,
};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    CivilInstant, CivilLayout, ClockLayout, DateLayout, DatetimeField, DatetimeParser, DayPadding,
    FormatDirective, NameLength, Padding, ReadItem, TextExpectation, TextReading,
};
use crate::datetime::zone::{
    OffsetStyle, UnresolvedLocalTime, Zone, digit_value, instant_at_offset, two_digits,
    utc_nanoseconds,
};

const MONTH_NAMES: [&[u8]; 12] = [
    b"january",
    b"february",
    b"march",
    b"april",
    b"may",
    b"june",
    b"july",
    b"august",
    b"september",
    b"october",
    b"november",
    b"december",
];

/// Weekday names from Monday, the order jiff counts weekdays in from zero.
const WEEKDAY_NAMES: [&[u8]; 7] = [
    b"monday",
    b"tuesday",
    b"wednesday",
    b"thursday",
    b"friday",
    b"saturday",
    b"sunday",
];

const NANOSECONDS_PER_SECOND: i128 = 1_000_000_000;

/// The most digits `%s` reads: every signed 64-bit count of seconds.
const LONGEST_UNIX_SECONDS: usize = 19;

/// The most digits `%.f` reads after its `.`: a nanosecond.
const LONGEST_FRACTION: usize = 9;

/// A text column read as DATETIME values, and why every lane that names no instant failed.
pub(crate) struct ParsedColumn {
    /// The read instants. A null lane and a failed lane are null.
    pub(crate) column: TimestampNanosecondArray,
    pub(crate) failures: Vec<FailedText>,
}

/// A lane whose text names no instant.
pub(crate) struct FailedText {
    pub(crate) lane: usize,
    pub(crate) failure: TextFailure,
}

/// Why a text names no DATETIME value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TextFailure {
    /// The text does not match its format, or holds a field or date that does not exist.
    Unreadable(UnreadableText),
    /// The text names a local time its zone skips or repeats, and the parser rejects both.
    Unresolved {
        local_time: UnresolvedLocalTime,
        zone: Zone,
    },
    /// The text names an instant outside the DATETIME range.
    OutOfRange,
}

/// Why a text cannot be read as a date and time in its format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnreadableText {
    /// The text stops matching its format at a byte.
    Mismatch {
        position: usize,
        expected: TextExpectation,
    },
    /// The text continues past the end of its format.
    TrailingText { position: usize },
    /// A field holds a value outside its range, such as a 13th month.
    FieldOutOfRange(DatetimeField),
    /// The fields name a date the calendar does not have, such as February 30.
    NonexistentDate,
    /// A field disagrees with the date the other fields name.
    InconsistentField(DatetimeField),
}

impl fmt::Display for UnreadableText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mismatch { position, expected } => write!(
                formatter,
                "input does not match its format at byte {position}: expected {expected}"
            ),
            Self::TrailingText { position } => {
                write!(
                    formatter,
                    "input continues past its format at byte {position}"
                )
            }
            Self::FieldOutOfRange(field) => write!(formatter, "{field} is out of range"),
            Self::NonexistentDate => formatter.write_str("date does not exist"),
            Self::InconsistentField(field) => write!(formatter, "{field} does not match the date"),
        }
    }
}

impl From<UnreadableText> for TextFailure {
    fn from(unreadable: UnreadableText) -> Self {
        Self::Unreadable(unreadable)
    }
}

/// Reads every valid lane of `texts` with `parser`. A null lane is null, and a lane that names no
/// instant is null and recorded with its failure.
pub(crate) fn parse_datetimes(texts: &StringArray, parser: &DatetimeParser) -> ParsedColumn {
    let lanes = texts.len();
    let mut instants = vec![0_i64; lanes];
    let mut valid = BooleanBufferBuilder::new(lanes);
    let mut failures = Vec::new();
    for (lane, instant) in instants.iter_mut().enumerate() {
        if texts.is_null(lane) {
            valid.append(false);
            continue;
        }
        match parser.read(texts.value(lane).as_bytes()) {
            Ok(read) => {
                *instant = read;
                valid.append(true);
            }
            Err(failure) => {
                failures.push(FailedText { lane, failure });
                valid.append(false);
            }
        }
    }
    let nulls = NullBuffer::new(valid.finish());
    let column = TimestampNanosecondArray::new(ScalarBuffer::from(instants), Some(nulls))
        .with_timezone_utc();
    ParsedColumn { column, failures }
}

/// The raw fields one text holds, before any range is checked.
///
/// A field the format does not read keeps its default. The hour, minute, second and fraction then
/// read as zero, which is what a format without them means, and no other default is ever consulted,
/// because a parser's layout reads the fields it relies on.
#[derive(Debug, Default)]
struct ReadValues {
    year: u32,
    iso_year: u32,
    month: u32,
    day: u32,
    day_of_year: u32,
    weekday: WeekdayReading,
    iso_week: u32,
    hour: u32,
    minute: u32,
    second: u32,
    /// The fraction of the second, scaled to nanoseconds.
    nanosecond: u32,
    /// `true` for `PM`.
    after_noon: bool,
    offset: OffsetReading,
    unix_seconds: i128,
}

/// A day of the week as a text writes it.
#[derive(Debug, Clone, Copy)]
enum WeekdayReading {
    /// A name, counted from Monday as 0.
    Name(u32),
    /// `%u`: a day counted from Monday as 1.
    FromMondayOne(u32),
    /// `%w`: a day counted from Sunday as 0.
    FromSundayZero(u32),
}

impl Default for WeekdayReading {
    fn default() -> Self {
        Self::Name(0)
    }
}

/// A UTC offset as a text writes it.
#[derive(Debug, Default, Clone, Copy)]
struct OffsetReading {
    negative: bool,
    hours: u32,
    minutes: u32,
    seconds: u32,
}

impl DatetimeParser {
    /// The instant a text names, or why it names none.
    fn read(&self, text: &[u8]) -> Result<i64, TextFailure> {
        let mut cursor = TextCursor { text, position: 0 };
        let mut values = ReadValues::default();
        for item in self.items.iter() {
            match item {
                ReadItem::Literal(literal) => cursor.expect_literal(literal)?,
                ReadItem::Field(directive) => cursor.read(*directive, &mut values)?,
            }
        }
        if cursor.position != text.len() {
            return Err(UnreadableText::TrailingText {
                position: cursor.position,
            }
            .into());
        }
        let instant = match &self.reading {
            TextReading::UnixTime => {
                let seconds = values
                    .unix_seconds
                    .checked_mul(NANOSECONDS_PER_SECOND)
                    .assured("a Unix time of at most nineteen digits in nanoseconds fits i128");
                seconds
                    .checked_add(i128::from(values.nanosecond))
                    .assured("a Unix time in nanoseconds plus a fraction of a second fits i128")
            }
            TextReading::Civil { layout, instant } => {
                let time = layout.time_of(&values)?;
                let date = layout.date_of(&values)?;
                let local = date.to_datetime(time);
                match instant {
                    CivilInstant::Offset => {
                        instant_at_offset(utc_nanoseconds(local), values.offset.to_offset()?)
                    }
                    CivilInstant::Zone {
                        zone,
                        disambiguation,
                    } => {
                        let instants = zone.instants_of(local);
                        let resolved = instants.resolve(*disambiguation);
                        resolved.map_err(|local_time| TextFailure::Unresolved {
                            local_time,
                            zone: zone.clone(),
                        })?
                    }
                }
            }
        };
        i64::try_from(instant).map_err(|_| TextFailure::OutOfRange)
    }
}

impl CivilLayout {
    /// The local time of day the read values name.
    fn time_of(&self, values: &ReadValues) -> Result<Time, UnreadableText> {
        let hour = match self.clock {
            ClockLayout::Midnight => 0,
            ClockLayout::TwentyFourHour => {
                check_range(values.hour, 0, 23, DatetimeField::Hour)?;
                values.hour
            }
            ClockLayout::TwelveHour => {
                check_range(values.hour, 1, 12, DatetimeField::TwelveHour)?;
                let past_twelve = values.hour % 12;
                if values.after_noon {
                    past_twelve
                        .checked_add(12)
                        .assured("an hour of the 12-hour clock past twelve is below 12")
                } else {
                    past_twelve
                }
            }
        };
        check_range(values.minute, 0, 59, DatetimeField::Minute)?;
        // A DATETIME has no leap seconds, so no minute has a 60th second.
        check_range(values.second, 0, 59, DatetimeField::Second)?;
        let nanosecond =
            i32::try_from(values.nanosecond).assured("a fraction of a second is below 10^9");
        let time = Time::new(
            civil_field(hour),
            civil_field(values.minute),
            civil_field(values.second),
            nanosecond,
        );
        Ok(time.verified("every field of the time was checked to lie in its range above"))
    }

    /// The date the read values name.
    fn date_of(&self, values: &ReadValues) -> Result<Date, UnreadableText> {
        let date = match self.date {
            DateLayout::Calendar => {
                check_range(values.month, 1, 12, DatetimeField::Month)?;
                check_range(values.day, 1, 31, DatetimeField::Day)?;
                let date = Date::new(
                    civil_year(values.year),
                    civil_field(values.month),
                    civil_field(values.day),
                );
                date.map_err(|_| UnreadableText::NonexistentDate)?
            }
            DateLayout::Ordinal => {
                check_range(values.day_of_year, 1, 366, DatetimeField::DayOfYear)?;
                let january_first = Date::new(civil_year(values.year), 1, 1)
                    .assured("January 1 of a four-digit year is a jiff date");
                let day_of_year = i16::try_from(values.day_of_year)
                    .assured("a day of the year was checked to be at most 366");
                let date = january_first.with().day_of_year(day_of_year).build();
                date.map_err(|_| UnreadableText::NonexistentDate)?
            }
            DateLayout::IsoWeek => {
                check_range(values.iso_week, 1, 53, DatetimeField::IsoWeek)?;
                let weekday = values.weekday.to_weekday()?;
                let week = i8::try_from(values.iso_week)
                    .assured("an ISO week was checked to be at most 53");
                let week_date = ISOWeekDate::new(civil_year(values.iso_year), week, weekday);
                week_date
                    .map_err(|_| UnreadableText::NonexistentDate)?
                    .date()
            }
        };
        if self.checks_weekday && date.weekday() != values.weekday.to_weekday()? {
            return Err(UnreadableText::InconsistentField(DatetimeField::Weekday));
        }
        Ok(date)
    }
}

fn check_range(
    value: u32,
    lowest: u32,
    highest: u32,
    field: DatetimeField,
) -> Result<(), UnreadableText> {
    if value < lowest || value > highest {
        return Err(UnreadableText::FieldOutOfRange(field));
    }
    Ok(())
}

/// A civil field checked to lie in its range, which is at most 59.
fn civil_field(value: u32) -> i8 {
    i8::try_from(value).assured("a civil field checked against its range is at most 59")
}

/// A year read from four digits, which jiff holds.
fn civil_year(value: u32) -> i16 {
    i16::try_from(value).assured("a year read from four digits is at most 9999")
}

impl WeekdayReading {
    fn to_weekday(self) -> Result<Weekday, UnreadableText> {
        let out_of_range = UnreadableText::FieldOutOfRange(DatetimeField::Weekday);
        let weekday = match self {
            Self::Name(day) => {
                let day = i8::try_from(day).assured("a weekday name reads as a day from 0 to 6");
                Weekday::from_monday_zero_offset(day)
            }
            Self::FromMondayOne(day) => {
                let day = i8::try_from(day).assured("a weekday is read from one digit");
                Weekday::from_monday_one_offset(day)
            }
            Self::FromSundayZero(day) => {
                let day = i8::try_from(day).assured("a weekday is read from one digit");
                Weekday::from_sunday_zero_offset(day)
            }
        };
        weekday.map_err(|_| out_of_range)
    }
}

impl OffsetReading {
    fn to_offset(self) -> Result<Offset, UnreadableText> {
        check_range(self.hours, 0, 23, DatetimeField::Offset)?;
        check_range(self.minutes, 0, 59, DatetimeField::Offset)?;
        check_range(self.seconds, 0, 59, DatetimeField::Offset)?;
        let hours = self
            .hours
            .checked_mul(3_600)
            .assured("at most 23 hours in seconds fits u32");
        let minutes = self
            .minutes
            .checked_mul(60)
            .assured("at most 59 minutes in seconds fits u32");
        let whole_minutes = hours
            .checked_add(minutes)
            .assured("less than a day in seconds fits u32");
        let magnitude = whole_minutes
            .checked_add(self.seconds)
            .assured("less than a day in seconds fits u32");
        let magnitude = i32::try_from(magnitude).assured("less than a day in seconds fits i32");
        let seconds = if self.negative {
            magnitude
                .checked_neg()
                .assured("less than a day in seconds negates inside i32")
        } else {
            magnitude
        };
        Ok(Offset::from_seconds(seconds).assured("an offset below a day is a jiff offset"))
    }
}

/// A position in one text being read.
struct TextCursor<'t> {
    text: &'t [u8],
    position: usize,
}

impl<'t> TextCursor<'t> {
    fn mismatch(&self, expected: TextExpectation) -> UnreadableText {
        UnreadableText::Mismatch {
            position: self.position,
            expected,
        }
    }

    fn rest(&self) -> &'t [u8] {
        &self.text[self.position..]
    }

    fn advance(&mut self, length: usize) {
        self.position = self
            .position
            .checked_add(length)
            .assured("a read advances only past bytes the text holds");
    }

    fn expect_literal(&mut self, literal: &triomphe::Arc<str>) -> Result<(), UnreadableText> {
        if !self.rest().starts_with(literal.as_bytes()) {
            return Err(self.mismatch(TextExpectation::Literal(literal.clone())));
        }
        self.advance(literal.len());
        Ok(())
    }

    /// Reads one directive's field into `values`.
    fn read(
        &mut self,
        directive: FormatDirective,
        values: &mut ReadValues,
    ) -> Result<(), UnreadableText> {
        let expected = TextExpectation::Directive(directive);
        match directive {
            FormatDirective::Year => values.year = self.fixed_digits(4, expected)?,
            FormatDirective::IsoYear => values.iso_year = self.fixed_digits(4, expected)?,
            FormatDirective::Month(padding) => values.month = self.number(2, padding, expected)?,
            FormatDirective::MonthName(length) => {
                let index = self.name(&MONTH_NAMES, length, expected)?;
                values.month = index.checked_add(1).assured("a month index is below 12");
            }
            FormatDirective::Day(DayPadding::Zero) => {
                values.day = self.fixed_digits(2, expected)?
            }
            FormatDirective::Day(DayPadding::Unpadded) => {
                values.day = self.number(2, Padding::Unpadded, expected)?;
            }
            FormatDirective::Day(DayPadding::Space) => {
                values.day = self.space_padded_day(expected)?
            }
            FormatDirective::DayOfYear(padding) => {
                values.day_of_year = self.number(3, padding, expected)?;
            }
            FormatDirective::WeekdayName(length) => {
                let index = self.name(&WEEKDAY_NAMES, length, expected)?;
                values.weekday = WeekdayReading::Name(index);
            }
            FormatDirective::IsoWeekday => {
                values.weekday = WeekdayReading::FromMondayOne(self.fixed_digits(1, expected)?);
            }
            FormatDirective::SundayWeekday => {
                values.weekday = WeekdayReading::FromSundayZero(self.fixed_digits(1, expected)?);
            }
            FormatDirective::IsoWeek(padding) => {
                values.iso_week = self.number(2, padding, expected)?;
            }
            FormatDirective::Hour(padding) | FormatDirective::TwelveHour(padding) => {
                values.hour = self.number(2, padding, expected)?;
            }
            FormatDirective::Meridiem => values.after_noon = self.meridiem(expected)?,
            FormatDirective::Minute(padding) => {
                values.minute = self.number(2, padding, expected)?
            }
            FormatDirective::Second(padding) => {
                values.second = self.number(2, padding, expected)?
            }
            FormatDirective::Fraction(digits) => {
                let fraction = self.fixed_digits(digits.count(), expected)?;
                values.nanosecond = fraction
                    .checked_mul(digits.last_digit_nanoseconds())
                    .assured("a fraction of n digits times 10^(9-n) is below 10^9");
            }
            FormatDirective::OptionalFraction => {
                values.nanosecond = self.optional_fraction(expected)?;
            }
            FormatDirective::Offset(style) => values.offset = self.offset(style, expected)?,
            FormatDirective::UnixSeconds => values.unix_seconds = self.unix_seconds(expected)?,
        }
        Ok(())
    }

    /// Reads exactly `count` ASCII digits.
    fn fixed_digits(
        &mut self,
        count: usize,
        expected: TextExpectation,
    ) -> Result<u32, UnreadableText> {
        let Some(digits) = self.rest().get(..count) else {
            return Err(self.mismatch(expected));
        };
        let mut value = 0_u32;
        for byte in digits {
            let Some(digit) = digit_value(*byte) else {
                return Err(self.mismatch(expected));
            };
            let shifted = value
                .checked_mul(10)
                .assured("at most nine digits make a value below 10^9");
            value = shifted
                .checked_add(digit)
                .assured("at most nine digits make a value below 10^9");
        }
        self.advance(count);
        Ok(value)
    }

    /// The number of ASCII digits at the start of the rest of the text, up to `most`.
    fn leading_digits(&self, skip: usize, most: usize) -> usize {
        let rest = self.rest();
        let mut count = 0;
        while count < most {
            let index = skip
                .checked_add(count)
                .assured("an index below the digit limit past a one-byte skip fits usize");
            let Some(byte) = rest.get(index) else {
                break;
            };
            if !byte.is_ascii_digit() {
                break;
            }
            count = count
                .checked_add(1)
                .assured("a count below the digit limit fits usize");
        }
        count
    }

    /// Reads a number of a field at most `width` digits wide: exactly `width` digits when padded,
    /// and otherwise one digit followed by as many more as the width allows.
    fn number(
        &mut self,
        width: usize,
        padding: Padding,
        expected: TextExpectation,
    ) -> Result<u32, UnreadableText> {
        match padding {
            Padding::Zero => self.fixed_digits(width, expected),
            Padding::Unpadded => {
                let count = self.leading_digits(0, width);
                if count == 0 {
                    return Err(self.mismatch(expected));
                }
                self.fixed_digits(count, expected)
            }
        }
    }

    /// Reads `%e`: a space followed by one digit, or two digits.
    fn space_padded_day(&mut self, expected: TextExpectation) -> Result<u32, UnreadableText> {
        if self.rest().first() != Some(&b' ') {
            return self.fixed_digits(2, expected);
        }
        let ones = match self.rest().get(1) {
            Some(byte) => digit_value(*byte),
            None => None,
        };
        let Some(ones) = ones else {
            return Err(self.mismatch(expected));
        };
        self.advance(2);
        Ok(ones)
    }

    /// Reads an English name from `names`, abbreviated to three letters or in full, without regard
    /// to ASCII case, and answers its index.
    fn name(
        &mut self,
        names: &[&[u8]],
        length: NameLength,
        expected: TextExpectation,
    ) -> Result<u32, UnreadableText> {
        for (index, name) in names.iter().enumerate() {
            let spelled = match length {
                NameLength::Abbreviated => &name[..3],
                NameLength::Full => name,
            };
            let matches = match self.rest().get(..spelled.len()) {
                Some(candidate) => candidate.eq_ignore_ascii_case(spelled),
                None => false,
            };
            if matches {
                self.advance(spelled.len());
                return Ok(u32::try_from(index).assured("a name index is below 12"));
            }
        }
        Err(self.mismatch(expected))
    }

    /// Reads `%p`, `AM` or `PM` without regard to ASCII case, and answers whether it is `PM`.
    fn meridiem(&mut self, expected: TextExpectation) -> Result<bool, UnreadableText> {
        let Some(marker) = self.rest().get(..2) else {
            return Err(self.mismatch(expected));
        };
        let after_noon = if marker.eq_ignore_ascii_case(b"AM") {
            false
        } else if marker.eq_ignore_ascii_case(b"PM") {
            true
        } else {
            return Err(self.mismatch(expected));
        };
        self.advance(2);
        Ok(after_noon)
    }

    /// Reads `%.f`: nothing, or `.` followed by one to nine digits.
    fn optional_fraction(&mut self, expected: TextExpectation) -> Result<u32, UnreadableText> {
        if self.rest().first() != Some(&b'.') {
            return Ok(0);
        }
        let digits = self.leading_digits(1, LONGEST_FRACTION);
        if digits == 0 {
            return Err(self.mismatch(expected));
        }
        self.advance(1);
        let fraction = self.fixed_digits(digits, expected)?;
        let digits = u32::try_from(digits).assured("at most nine digits were read");
        let dropped = 9_u32
            .checked_sub(digits)
            .assured("at most nine digits were read");
        let scale = 10_u32.checked_pow(dropped).assured("10^8 fits u32");
        let nanoseconds = fraction
            .checked_mul(scale)
            .assured("a fraction of n digits times 10^(9-n) is below 10^9");
        Ok(nanoseconds)
    }

    /// Reads a UTC offset written in `style`, or `Z` for UTC.
    fn offset(
        &mut self,
        style: OffsetStyle,
        expected: TextExpectation,
    ) -> Result<OffsetReading, UnreadableText> {
        let rest = self.rest();
        let negative = match rest.first() {
            Some(b'Z') => {
                self.advance(1);
                return Ok(OffsetReading::default());
            }
            Some(b'+') => false,
            Some(b'-') => true,
            _ => return Err(self.mismatch(expected)),
        };
        let written = WrittenOffset(rest);
        let read = match style {
            OffsetStyle::Compact => written.compact(),
            OffsetStyle::Colon => written.colon(),
            OffsetStyle::Seconds => written.seconds(),
        };
        let Some(read) = read else {
            return Err(self.mismatch(expected));
        };
        self.advance(read.length);
        Ok(OffsetReading {
            negative,
            hours: read.hours,
            minutes: read.minutes,
            seconds: read.seconds,
        })
    }

    /// Reads `%s`: an optional `-` followed by one to nineteen digits.
    fn unix_seconds(&mut self, expected: TextExpectation) -> Result<i128, UnreadableText> {
        let negative = self.rest().first() == Some(&b'-');
        let sign_length = usize::from(negative);
        let digits = self.leading_digits(sign_length, LONGEST_UNIX_SECONDS);
        if digits == 0 {
            return Err(self.mismatch(expected));
        }
        self.advance(sign_length);
        let mut magnitude = 0_i128;
        for byte in &self.rest()[..digits] {
            let digit = digit_value(*byte).verified("the leading digits were counted above");
            let shifted = magnitude
                .checked_mul(10)
                .assured("nineteen digits make a value below 10^19, which fits i128");
            magnitude = shifted
                .checked_add(i128::from(digit))
                .assured("nineteen digits make a value below 10^19, which fits i128");
        }
        self.advance(digits);
        if negative {
            let negated = magnitude
                .checked_neg()
                .assured("a value below 10^19 negates inside i128");
            return Ok(negated);
        }
        Ok(magnitude)
    }
}

/// The bytes of a UTC offset from its sign on, read in one of the offset styles.
struct WrittenOffset<'t>(&'t [u8]);

/// The parts of an offset read from a text, and how many bytes they span with the sign.
struct OffsetParts {
    hours: u32,
    minutes: u32,
    seconds: u32,
    length: usize,
}

impl WrittenOffset<'_> {
    /// The two digits at `index`, or `None` when either byte is missing or not a digit.
    fn two_digits_at(&self, index: usize) -> Option<u32> {
        let tens = *self.0.get(index)?;
        let ones = *self.0.get(index.checked_add(1)?)?;
        two_digits(tens, ones)
    }

    fn colon_at(&self, index: usize) -> bool {
        self.0.get(index) == Some(&b':')
    }

    /// `+hhmm`, optionally followed by `ss`.
    fn compact(&self) -> Option<OffsetParts> {
        let hours = self.two_digits_at(1)?;
        let minutes = self.two_digits_at(3)?;
        let parts = match self.two_digits_at(5) {
            Some(seconds) => OffsetParts {
                hours,
                minutes,
                seconds,
                length: 7,
            },
            None => OffsetParts {
                hours,
                minutes,
                seconds: 0,
                length: 5,
            },
        };
        Some(parts)
    }

    /// `+hh:mm`, optionally followed by `:ss`.
    fn colon(&self) -> Option<OffsetParts> {
        let hours = self.two_digits_at(1)?;
        if !self.colon_at(3) {
            return None;
        }
        let minutes = self.two_digits_at(4)?;
        let seconds = if self.colon_at(6) {
            self.two_digits_at(7)
        } else {
            None
        };
        let parts = match seconds {
            Some(seconds) => OffsetParts {
                hours,
                minutes,
                seconds,
                length: 9,
            },
            None => OffsetParts {
                hours,
                minutes,
                seconds: 0,
                length: 6,
            },
        };
        Some(parts)
    }

    /// `+hh:mm:ss`.
    fn seconds(&self) -> Option<OffsetParts> {
        let hours = self.two_digits_at(1)?;
        if !self.colon_at(3) || !self.colon_at(6) {
            return None;
        }
        let minutes = self.two_digits_at(4)?;
        let seconds = self.two_digits_at(7)?;
        Some(OffsetParts {
            hours,
            minutes,
            seconds,
            length: 9,
        })
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;
