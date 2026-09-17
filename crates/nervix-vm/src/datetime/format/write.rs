//! Writing DATETIME columns as text in a compiled format.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The kernel behind `format_datetime`: laying a compiled format out for one column in
//!   one zone, and one pass over the column that writes every valid lane's local date and time into
//!   one text buffer sized for the format's longest value.
//! - **Depends on.** Compiled datetime formats, the time zones of the datetime kernels, Jiff's civil
//!   calendar, and Arrow string arrays and buffers.
//! - **Must not know.** Registers, programs, spans, or the host's locale.
//!
//! A format is laid out once per column. Consecutive items that write every value at one width
//! become a template: their literal text, the zone's name, and the offset and abbreviation of a zone
//! that shows one offset are written into it once, and every value copies the template and fills its
//! fields into fixed slots. A common format such as `%Y-%m-%dT%H:%M:%S.%f%:z` in UTC, or `%F %T` in
//! any zone, is one template. Only items whose width varies, such as `%B`, and the offset and
//! abbreviation of a zone whose offset changes are written item by item.

use arrow_array::{Array, StringArray, TimestampNanosecondArray};
use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use jiff::{civil::DateTime, tz::Offset};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    DatetimeFormat, DayPadding, FormatDirective, FormatItem, FractionDigits, NameLength, Padding,
    ZoneDirective,
};
use crate::datetime::zone::{OffsetStyle, ShortText, Zone, ZoneOffsets, ascii_digit, timestamp};

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// Weekday names from Monday, the order jiff counts weekdays in from zero.
const WEEKDAY_NAMES: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];

const NANOSECONDS_PER_SECOND: i64 = 1_000_000_000;

/// A DATETIME column written as text, or a column whose text could exceed what one Arrow string
/// array holds.
pub(crate) enum FormattedColumn {
    Formatted(StringArray),
    /// Every value may take the format's longest length, and that many bytes over all lanes exceed
    /// the 2 GiB of text an Arrow string array addresses.
    TooLarge,
}

/// Writes every valid lane of `values` in `format`, read in `zone`'s local time. A null lane is
/// null.
pub(crate) fn format_datetimes(
    values: &TimestampNanosecondArray,
    format: &DatetimeFormat,
    zone: &Zone,
) -> FormattedColumn {
    let longest = format.longest_value(zone);
    let Some(capacity) = text_capacity(values.len(), longest) else {
        return FormattedColumn::TooLarge;
    };
    let mut layout = ColumnLayout::of(format, zone);
    FormattedColumn::Formatted(layout.write(values, capacity))
}

/// The bytes a column of `lanes` values, each at most `longest` bytes, can take, or `None` when
/// that exceeds the offsets of an Arrow string array.
fn text_capacity(lanes: usize, longest: usize) -> Option<usize> {
    let capacity = lanes.checked_mul(longest)?;
    i32::try_from(capacity).ok()?;
    Some(capacity)
}

/// A format laid out for writing one column in one zone.
struct ColumnLayout<'z> {
    pieces: Vec<LayoutPiece>,
    offsets: ZoneOffsets<'z>,
}

/// One part of a laid-out format, in the order the format writes it.
#[derive(Debug)]
enum LayoutPiece {
    Template(Template),
    Varying(VaryingField),
    /// The abbreviation of a zone whose offset changes.
    Abbreviation,
}

/// Text every value copies, with a slot for each field in it.
#[derive(Debug, Default)]
struct Template {
    text: Vec<u8>,
    slots: Vec<TemplateSlot>,
}

/// The bytes of a template that one field fills.
#[derive(Debug)]
struct TemplateSlot {
    start: usize,
    end: usize,
    field: FixedWidthField,
}

/// A layout while its format's items are laid out.
struct LayoutBuilder<'z> {
    pieces: Vec<LayoutPiece>,
    /// The template the items since the last piece that is not a template write into.
    open: Template,
    offsets: ZoneOffsets<'z>,
}

impl<'z> ColumnLayout<'z> {
    fn of(format: &DatetimeFormat, zone: &'z Zone) -> Self {
        let mut builder = LayoutBuilder {
            pieces: Vec::new(),
            open: Template::default(),
            offsets: zone.offsets(),
        };
        for item in format.items.iter() {
            match item {
                FormatItem::Literal(literal) => {
                    builder.open.text.extend_from_slice(literal.as_bytes());
                }
                FormatItem::Field(directive) => builder.lay_out_field(*directive),
                FormatItem::Zone(ZoneDirective::Abbreviation) => builder.lay_out_abbreviation(),
                FormatItem::Zone(ZoneDirective::Name) => {
                    builder
                        .open
                        .text
                        .extend_from_slice(zone.to_string().as_bytes());
                }
            }
        }
        builder.close_template();
        Self {
            pieces: builder.pieces,
            offsets: builder.offsets,
        }
    }

    /// Writes every valid lane after the previous one into a buffer of `capacity` bytes, which holds
    /// the longest value of every lane.
    fn write(&mut self, values: &TimestampNanosecondArray, capacity: usize) -> StringArray {
        let mut text = Vec::with_capacity(capacity);
        let mut ends = Vec::with_capacity(
            values
                .len()
                .checked_add(1)
                .assured("a column holding lanes in memory has fewer than usize::MAX of them"),
        );
        ends.push(0_i32);
        for lane in 0..values.len() {
            if values.is_valid(lane) {
                self.write_value(&mut text, values.value(lane));
            }
            let end = i32::try_from(text.len()).verified(
                "the text holds at most the longest value per lane, which the caller checked fits \
                 i32",
            );
            ends.push(end);
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(ends));
        StringArray::try_new(offsets, Buffer::from_vec(text), values.nulls().cloned()).assured(
            "the layout writes whole UTF-8 values between ascending offsets that fit i32, as the \
             capacity check before writing bounds them",
        )
    }

    /// Appends the value of one instant.
    fn write_value(&mut self, text: &mut Vec<u8>, instant: i64) {
        let pieces = &self.pieces;
        self.offsets
            .read_offset_and_abbreviation(instant, |offset, abbreviation| {
                let value = LocalValue {
                    instant,
                    local: offset.to_datetime(timestamp(instant)),
                    offset,
                };
                for piece in pieces {
                    match piece {
                        LayoutPiece::Template(template) => template.write(text, value.local),
                        LayoutPiece::Varying(field) => value.write_varying_field(*field, text),
                        LayoutPiece::Abbreviation => {
                            text.extend_from_slice(abbreviation.as_bytes())
                        }
                    }
                }
            });
    }
}

impl LayoutBuilder<'_> {
    /// Reserves a slot in the open template for a field written at one width, writes a constant
    /// offset into it, or ends it before a field whose width varies.
    fn lay_out_field(&mut self, directive: FormatDirective) {
        let written = directive.written_field();
        match written {
            WrittenField::FixedWidth(field) => {
                let start = self.open.text.len();
                let end = start
                    .checked_add(field.width())
                    .assured("a template is no longer than its format's longest value");
                self.open.text.resize(end, b'0');
                self.open.slots.push(TemplateSlot { start, end, field });
            }
            WrittenField::Varying(VaryingField::Offset(style)) => {
                if let ZoneOffsets::Constant(constant) = &self.offsets {
                    let offset = ShortText::offset(constant.offset.seconds(), style);
                    self.open.text.extend_from_slice(offset.as_bytes());
                } else {
                    self.push_piece(LayoutPiece::Varying(VaryingField::Offset(style)));
                }
            }
            WrittenField::Varying(field) => self.push_piece(LayoutPiece::Varying(field)),
        }
    }

    /// Writes the abbreviation of a zone that shows one offset into the open template, or ends the
    /// template before the abbreviation of a zone whose offset changes.
    fn lay_out_abbreviation(&mut self) {
        if let ZoneOffsets::Constant(constant) = &self.offsets {
            let abbreviation = constant.abbreviation.as_bytes();
            self.open.text.extend_from_slice(abbreviation);
        } else {
            self.push_piece(LayoutPiece::Abbreviation);
        }
    }

    /// Ends the open template and adds a piece after it.
    fn push_piece(&mut self, piece: LayoutPiece) {
        self.close_template();
        self.pieces.push(piece);
    }

    /// Adds the open template as a piece, unless it is empty, and opens a new one.
    fn close_template(&mut self) {
        // Every field is at least one byte wide, so a template without text has no slots either.
        if self.open.text.is_empty() {
            return;
        }
        let template = std::mem::take(&mut self.open);
        self.pieces.push(LayoutPiece::Template(template));
    }
}

impl Template {
    /// Appends this template with its fields filled in from `local`.
    fn write(&self, text: &mut Vec<u8>, local: DateTime) {
        let start = text.len();
        text.extend_from_slice(&self.text);
        let written = &mut text[start..];
        for slot in &self.slots {
            slot.field.fill(&mut written[slot.start..slot.end], local);
        }
    }
}

/// An instant, its local date and time, and the offset between them.
struct LocalValue {
    instant: i64,
    local: DateTime,
    offset: Offset,
}

impl LocalValue {
    fn write_varying_field(&self, field: VaryingField, text: &mut Vec<u8>) {
        match field {
            VaryingField::Unpadded(number) => push_unpadded(text, number.of(self.local)),
            VaryingField::FullName(name) => text.extend_from_slice(name.of(self.local).as_bytes()),
            VaryingField::OptionalFraction => {
                let nanoseconds = unsigned(self.local.subsec_nanosecond());
                if nanoseconds != 0 {
                    let mut fraction = [0_u8; 9];
                    fill_digits(&mut fraction, nanoseconds);
                    let last_significant = fraction
                        .iter()
                        .rposition(|digit| *digit != b'0')
                        .verified("a nonzero fraction has a nonzero digit");
                    let significant = last_significant
                        .checked_add(1)
                        .assured("a position among nine digits plus one fits usize");
                    text.push(b'.');
                    text.extend_from_slice(&fraction[..significant]);
                }
            }
            VaryingField::Offset(style) => {
                text.extend_from_slice(ShortText::offset(self.offset.seconds(), style).as_bytes());
            }
            VaryingField::UnixSeconds => {
                push_signed(text, self.instant.div_euclid(NANOSECONDS_PER_SECOND));
            }
        }
    }
}

/// How a directive writes its field: at one width for every value, or at a width that depends on
/// the value or on the offset shown with it.
enum WrittenField {
    FixedWidth(FixedWidthField),
    Varying(VaryingField),
}

/// A field every value writes at one width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedWidthField {
    /// A number padded with zeros to `digits`, which every value of it fits.
    Number {
        number: CivilNumber,
        digits: usize,
    },
    /// The day of the month padded with a space, as `%e` writes it.
    SpacePaddedDay,
    /// The first three letters of a name.
    Abbreviation(CivilName),
    /// `AM` or `PM`.
    Meridiem,
    Fraction(FractionDigits),
}

/// A field whose values differ in width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VaryingField {
    Unpadded(CivilNumber),
    FullName(CivilName),
    OptionalFraction,
    /// A UTC offset, written with seconds only when it has them.
    Offset(OffsetStyle),
    UnixSeconds,
}

/// A number a format writes from a local date and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CivilNumber {
    Year,
    IsoYear,
    Month,
    Day,
    DayOfYear,
    IsoWeekday,
    SundayWeekday,
    IsoWeek,
    Hour,
    TwelveHour,
    Minute,
    Second,
}

/// A name a format writes from a local date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CivilName {
    Month,
    Weekday,
}

impl FormatDirective {
    fn written_field(self) -> WrittenField {
        match self {
            Self::Year => WrittenField::number(CivilNumber::Year, 4, Padding::Zero),
            Self::IsoYear => WrittenField::number(CivilNumber::IsoYear, 4, Padding::Zero),
            Self::Month(padding) => WrittenField::number(CivilNumber::Month, 2, padding),
            Self::MonthName(length) => WrittenField::name(CivilName::Month, length),
            Self::Day(DayPadding::Zero) => WrittenField::number(CivilNumber::Day, 2, Padding::Zero),
            Self::Day(DayPadding::Unpadded) => {
                WrittenField::number(CivilNumber::Day, 2, Padding::Unpadded)
            }
            Self::Day(DayPadding::Space) => {
                WrittenField::FixedWidth(FixedWidthField::SpacePaddedDay)
            }
            Self::DayOfYear(padding) => WrittenField::number(CivilNumber::DayOfYear, 3, padding),
            Self::WeekdayName(length) => WrittenField::name(CivilName::Weekday, length),
            Self::IsoWeekday => WrittenField::number(CivilNumber::IsoWeekday, 1, Padding::Zero),
            Self::SundayWeekday => {
                WrittenField::number(CivilNumber::SundayWeekday, 1, Padding::Zero)
            }
            Self::IsoWeek(padding) => WrittenField::number(CivilNumber::IsoWeek, 2, padding),
            Self::Hour(padding) => WrittenField::number(CivilNumber::Hour, 2, padding),
            Self::TwelveHour(padding) => WrittenField::number(CivilNumber::TwelveHour, 2, padding),
            Self::Meridiem => WrittenField::FixedWidth(FixedWidthField::Meridiem),
            Self::Minute(padding) => WrittenField::number(CivilNumber::Minute, 2, padding),
            Self::Second(padding) => WrittenField::number(CivilNumber::Second, 2, padding),
            Self::Fraction(digits) => WrittenField::FixedWidth(FixedWidthField::Fraction(digits)),
            Self::OptionalFraction => WrittenField::Varying(VaryingField::OptionalFraction),
            Self::Offset(style) => WrittenField::Varying(VaryingField::Offset(style)),
            Self::UnixSeconds => WrittenField::Varying(VaryingField::UnixSeconds),
        }
    }
}

impl WrittenField {
    /// A number padded with zeros to `digits`, which is written at one width, or written unpadded,
    /// which is not.
    fn number(number: CivilNumber, digits: usize, padding: Padding) -> Self {
        match padding {
            Padding::Zero => Self::FixedWidth(FixedWidthField::Number { number, digits }),
            Padding::Unpadded => Self::Varying(VaryingField::Unpadded(number)),
        }
    }

    fn name(name: CivilName, length: NameLength) -> Self {
        match length {
            NameLength::Abbreviated => Self::FixedWidth(FixedWidthField::Abbreviation(name)),
            NameLength::Full => Self::Varying(VaryingField::FullName(name)),
        }
    }
}

impl FixedWidthField {
    fn width(self) -> usize {
        match self {
            Self::Number { digits, .. } => digits,
            Self::SpacePaddedDay | Self::Meridiem => 2,
            Self::Abbreviation(_) => 3,
            Self::Fraction(digits) => digits.count(),
        }
    }

    /// Writes this field of `local` into `slot`, which is exactly as wide as the field.
    fn fill(self, slot: &mut [u8], local: DateTime) {
        match self {
            Self::Number { number, .. } => fill_digits(slot, number.of(local)),
            Self::SpacePaddedDay => {
                let day = CivilNumber::Day.of(local);
                fill_digits(slot, day);
                if day < 10 {
                    slot[0] = b' ';
                }
            }
            Self::Abbreviation(name) => slot.copy_from_slice(&name.of(local).as_bytes()[..3]),
            Self::Meridiem => {
                let marker: &[u8] = if local.hour() < 12 { b"AM" } else { b"PM" };
                slot.copy_from_slice(marker);
            }
            Self::Fraction(_) => {
                let mut fraction = [0_u8; 9];
                fill_digits(&mut fraction, unsigned(local.subsec_nanosecond()));
                slot.copy_from_slice(&fraction[..slot.len()]);
            }
        }
    }
}

impl CivilNumber {
    fn of(self, local: DateTime) -> u32 {
        match self {
            Self::Year => unsigned(local.year()),
            Self::IsoYear => unsigned(local.iso_week_date().year()),
            Self::Month => unsigned(local.month()),
            Self::Day => unsigned(local.day()),
            Self::DayOfYear => unsigned(local.day_of_year()),
            Self::IsoWeekday => unsigned(local.weekday().to_monday_one_offset()),
            Self::SundayWeekday => unsigned(local.weekday().to_sunday_zero_offset()),
            Self::IsoWeek => unsigned(local.iso_week_date().week()),
            Self::Hour => unsigned(local.hour()),
            Self::TwelveHour => {
                let past_twelve = unsigned(local.hour()) % 12;
                if past_twelve == 0 { 12 } else { past_twelve }
            }
            Self::Minute => unsigned(local.minute()),
            Self::Second => unsigned(local.second()),
        }
    }
}

impl CivilName {
    fn of(self, local: DateTime) -> &'static str {
        match self {
            Self::Month => {
                let month = usize::try_from(local.month()).assured("a month is from 1 to 12");
                let index = month.checked_sub(1).assured("a month is at least 1");
                MONTH_NAMES[index]
            }
            Self::Weekday => {
                let index = usize::try_from(local.weekday().to_monday_zero_offset())
                    .assured("a weekday counted from Monday is from 0 to 6");
                WEEKDAY_NAMES[index]
            }
        }
    }
}

/// A civil field jiff reads as a signed integer that is never negative for a date inside the
/// DATETIME range.
fn unsigned<T: Into<i64>>(field: T) -> u32 {
    u32::try_from(field.into()).assured(
        "every civil field of a date inside the DATETIME range is between 0 and 999,999,999",
    )
}

/// Writes the last `digits.len()` decimal digits of `value` into `digits`, padded with zeros.
fn fill_digits(digits: &mut [u8], value: u32) {
    let mut remaining = value;
    for digit in digits.iter_mut().rev() {
        *digit = ascii_digit(remaining % 10);
        remaining /= 10;
    }
}

/// Appends `value` without leading zeros.
fn push_unpadded(text: &mut Vec<u8>, value: u32) {
    let mut digits = [0_u8; 10];
    let mut start = digits.len();
    let mut remaining = value;
    loop {
        start = start
            .checked_sub(1)
            .assured("a u32 has at most ten decimal digits");
        digits[start] = ascii_digit(remaining % 10);
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    text.extend_from_slice(&digits[start..]);
}

/// Appends a signed count of seconds, with a leading `-` when it is negative.
fn push_signed(text: &mut Vec<u8>, value: i64) {
    let mut digits = [0_u8; 20];
    let mut start = digits.len();
    let mut remaining = value.unsigned_abs();
    loop {
        start = start
            .checked_sub(1)
            .assured("a u64 has at most twenty decimal digits");
        let digit = u32::try_from(remaining % 10).assured("a remainder by ten is below ten");
        digits[start] = ascii_digit(digit);
        remaining /= 10;
        if remaining == 0 {
            break;
        }
    }
    if value < 0 {
        text.push(b'-');
    }
    text.extend_from_slice(&digits[start..]);
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
