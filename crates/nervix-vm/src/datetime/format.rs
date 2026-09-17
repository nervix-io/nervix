//! Datetime formats, compiled once when a call is lowered.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The format language `format_datetime` and `parse_datetime` share: reading a written
//!   format into literal text and directives, the fields each directive writes and reads, the checks
//!   that let a format name exactly one instant when it is read, the longest value a format
//!   describes, and, in its submodules, the kernels that write DATETIME columns as text and read text
//!   columns as DATETIME values.
//! - **Depends on.** Jiff's civil calendar, the time zones of the datetime kernels, and Arrow arrays
//!   and buffers.
//! - **Must not know.** The host's locale, registers, programs, spans, or how a failed lane is
//!   recorded as a row error.
//!
//! A format is text in which `%` starts a directive. Every other character is literal text, written
//! as it is and read only where it appears exactly. Month and weekday names are English and never
//! depend on a locale.

use std::{collections::BTreeMap, fmt};

use meticulous::{OptionExt as _, ResultExt as _};

use super::zone::{OffsetStyle, Zone, digit_value};
use crate::program::Disambiguation;

mod read;
mod write;

pub use read::UnreadableText;
pub(crate) use read::{TextFailure, parse_datetimes};
pub(crate) use write::{FormattedColumn, format_datetimes};

/// The longest value, in bytes, a format may describe. A format is rejected when a value it writes
/// or reads could be longer, so a column of formatted values never holds more than this many bytes
/// per row.
pub(crate) const LONGEST_FORMATTED_VALUE: usize = 256;

/// A format `format_datetime` writes DATETIME values in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatetimeFormat {
    items: triomphe::Arc<[FormatItem]>,
}

impl DatetimeFormat {
    /// Compiles a written format for writing values in `zone`, checking that the values it writes
    /// there fit the formatted value limit.
    pub fn compile(written: &str, zone: &Zone) -> Result<Self, FormatDefect> {
        let items = compile_items(written)?;
        let format = Self {
            items: triomphe::Arc::from(items),
        };
        let longest = format.longest_value(zone);
        if longest > LONGEST_FORMATTED_VALUE {
            return Err(FormatDefect::TooLong { longest });
        }
        Ok(format)
    }

    /// The longest value this format writes for an instant in `zone`, in bytes.
    pub(crate) fn longest_value(&self, zone: &Zone) -> usize {
        let mut longest = 0_usize;
        for item in self.items.iter() {
            let item_longest = match item {
                FormatItem::Literal(text) => text.len(),
                FormatItem::Field(directive) => directive.longest(),
                FormatItem::Zone(directive) => directive.longest(zone),
            };
            longest = longest
                .checked_add(item_longest)
                .assured("a value spans at most a few dozen bytes per written byte of its format");
        }
        longest
    }
}

/// A format compiled for reading, before the time zone a call names is attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFormat {
    items: triomphe::Arc<[ReadItem]>,
    layout: ParseLayout,
}

impl ParseFormat {
    /// Compiles a written format for reading, checking that it reads a complete date, reads no field
    /// twice, and describes values that fit the formatted value limit.
    pub fn compile(written: &str) -> Result<Self, FormatDefect> {
        let mut items = Vec::new();
        for item in compile_items(written)? {
            let item = match item {
                FormatItem::Literal(text) => ReadItem::Literal(text),
                FormatItem::Field(directive) => ReadItem::Field(directive),
                FormatItem::Zone(directive) => return Err(FormatDefect::Unreadable(directive)),
            };
            items.push(item);
        }
        let layout = ParseLayout::of(&items)?;
        let mut longest = 0_usize;
        for item in &items {
            let item_longest = match item {
                ReadItem::Literal(text) => text.len(),
                ReadItem::Field(directive) => directive.longest(),
            };
            longest = longest
                .checked_add(item_longest)
                .assured("a value spans at most a few dozen bytes per written byte of its format");
        }
        if longest > LONGEST_FORMATTED_VALUE {
            return Err(FormatDefect::TooLong { longest });
        }
        Ok(Self {
            items: triomphe::Arc::from(items),
            layout,
        })
    }
}

/// A format `parse_datetime` reads text in, together with how a read text names its instant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatetimeParser {
    items: triomphe::Arc<[ReadItem]>,
    reading: TextReading,
}

/// How the fields a parser reads name one instant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum TextReading {
    /// `%s`, and optionally the fraction of the second after it.
    UnixTime,
    /// A local date and time, placed on the time line by a read offset or by the call's zone.
    Civil {
        layout: CivilLayout,
        instant: CivilInstant,
    },
}

/// What places a read local date and time on the time line.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CivilInstant {
    /// A UTC offset the text holds.
    Offset,
    /// The zone the call names, with how a local time it skips or repeats resolves.
    Zone {
        zone: Zone,
        disambiguation: Disambiguation,
    },
}

/// Why a format and a call's time zone disagree about how a read text names its instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserZoneMismatch {
    /// The format reads a UTC offset, but the call names a zone.
    ZoneWithOffset,
    /// The format reads a Unix time, but the call names a zone.
    ZoneWithUnixTime,
    /// The format reads a local date and time without a UTC offset, but the call names no zone.
    MissingZone,
}

impl DatetimeParser {
    /// A parser for `format` that resolves read local times in `zone`, or that reads the instant
    /// from the text when `zone` is `None`.
    pub fn new(
        format: ParseFormat,
        zone: Option<(Zone, Disambiguation)>,
    ) -> Result<Self, ParserZoneMismatch> {
        let reading = match (format.layout, zone) {
            (ParseLayout::UnixTime, None) => TextReading::UnixTime,
            (ParseLayout::UnixTime, Some(_)) => return Err(ParserZoneMismatch::ZoneWithUnixTime),
            (
                ParseLayout::Civil {
                    layout,
                    reads_offset: true,
                },
                None,
            ) => TextReading::Civil {
                layout,
                instant: CivilInstant::Offset,
            },
            (
                ParseLayout::Civil {
                    reads_offset: true, ..
                },
                Some(_),
            ) => return Err(ParserZoneMismatch::ZoneWithOffset),
            (
                ParseLayout::Civil {
                    reads_offset: false,
                    ..
                },
                None,
            ) => return Err(ParserZoneMismatch::MissingZone),
            (
                ParseLayout::Civil {
                    layout,
                    reads_offset: false,
                },
                Some((zone, disambiguation)),
            ) => TextReading::Civil {
                layout,
                instant: CivilInstant::Zone {
                    zone,
                    disambiguation,
                },
            },
        };
        Ok(Self {
            items: format.items,
            reading,
        })
    }
}

/// One piece of a compiled format.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum FormatItem {
    /// Text written as it is.
    Literal(triomphe::Arc<str>),
    /// A field of the local date and time.
    Field(FormatDirective),
    /// Something about the zone, which a format only writes.
    Zone(ZoneDirective),
}

/// One piece of a format compiled for reading.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ReadItem {
    /// Text read only where it appears exactly.
    Literal(triomphe::Arc<str>),
    /// A field of the local date and time.
    Field(FormatDirective),
}

/// A directive of the datetime format language that writes and reads a field of a date and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FormatDirective {
    /// `%Y`: the year, in four digits.
    Year,
    /// `%G`: the ISO 8601 week-numbering year, in four digits.
    IsoYear,
    /// `%m` and `%-m`: the month, from 1 to 12.
    Month(Padding),
    /// `%b` and `%B`: the English name of the month.
    MonthName(NameLength),
    /// `%d`, `%-d` and `%e`: the day of the month, from 1 to 31.
    Day(DayPadding),
    /// `%j` and `%-j`: the day of the year, from 1 to 366.
    DayOfYear(Padding),
    /// `%a` and `%A`: the English name of the day of the week.
    WeekdayName(NameLength),
    /// `%u`: the ISO 8601 day of the week, from Monday as 1 to Sunday as 7.
    IsoWeekday,
    /// `%w`: the day of the week, from Sunday as 0 to Saturday as 6.
    SundayWeekday,
    /// `%V` and `%-V`: the ISO 8601 week, from 1 to 53.
    IsoWeek(Padding),
    /// `%H` and `%-H`: the hour of the 24-hour clock, from 0 to 23.
    Hour(Padding),
    /// `%I` and `%-I`: the hour of the 12-hour clock, from 1 to 12.
    TwelveHour(Padding),
    /// `%p`: `AM` before noon and `PM` from noon.
    Meridiem,
    /// `%M` and `%-M`: the minute, from 0 to 59.
    Minute(Padding),
    /// `%S` and `%-S`: the second, from 0 to 59.
    Second(Padding),
    /// `%f` and `%1f` through `%9f`: the fraction of the second in exactly that many digits.
    Fraction(FractionDigits),
    /// `%.f`: nothing for a whole second, and otherwise `.` followed by the fraction of the second
    /// without trailing zeros.
    OptionalFraction,
    /// `%z`, `%:z` and `%::z`: the UTC offset.
    Offset(OffsetStyle),
    /// `%s`: the whole seconds since the Unix epoch, rounded down.
    UnixSeconds,
}

/// A directive that writes something about the zone rather than a field of the local date and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::Display)]
pub enum ZoneDirective {
    /// `%Z`: the abbreviation the zone shows.
    #[strum(to_string = "%Z")]
    Abbreviation,
    /// `%Q`: the name of the zone.
    #[strum(to_string = "%Q")]
    Name,
}

/// Whether a number is written at its field's full width.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Padding {
    /// Padded with zeros to the field's full width.
    Zero,
    /// Written without padding, as the `-` flag asks.
    Unpadded,
}

/// How `%d`, `%-d` and `%e` write the day of the month.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DayPadding {
    Zero,
    Unpadded,
    /// Padded with a space, as `%e` writes it.
    Space,
}

/// Whether a name is written abbreviated to three letters or in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NameLength {
    Abbreviated,
    Full,
}

/// How many digits of the fraction of a second a directive writes and reads, from 1 to 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FractionDigits(u8);

impl FractionDigits {
    const NANOSECONDS: Self = Self(9);

    pub(crate) fn count(self) -> usize {
        usize::from(self.0)
    }

    /// How many nanoseconds one unit of the last digit is worth: `10^(9 - digits)`.
    pub(crate) fn last_digit_nanoseconds(self) -> u32 {
        let dropped = 9_u8
            .checked_sub(self.0)
            .assured("a fraction has at most nine digits");
        10_u32
            .checked_pow(u32::from(dropped))
            .assured("10^8 fits u32")
    }
}

/// A field of a date and time that a format reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::Display)]
pub enum DatetimeField {
    #[strum(to_string = "year")]
    Year,
    #[strum(to_string = "ISO week-numbering year")]
    IsoYear,
    #[strum(to_string = "month")]
    Month,
    #[strum(to_string = "day of the month")]
    Day,
    #[strum(to_string = "day of the year")]
    DayOfYear,
    #[strum(to_string = "day of the week")]
    Weekday,
    #[strum(to_string = "ISO week")]
    IsoWeek,
    #[strum(to_string = "hour")]
    Hour,
    #[strum(to_string = "hour of the 12-hour clock")]
    TwelveHour,
    #[strum(to_string = "AM or PM marker")]
    Meridiem,
    #[strum(to_string = "minute")]
    Minute,
    #[strum(to_string = "second")]
    Second,
    #[strum(to_string = "fraction of a second")]
    Fraction,
    #[strum(to_string = "UTC offset")]
    Offset,
    #[strum(to_string = "Unix time")]
    UnixTime,
}

impl FormatDirective {
    /// The field this directive writes and reads.
    fn field(self) -> DatetimeField {
        match self {
            Self::Year => DatetimeField::Year,
            Self::IsoYear => DatetimeField::IsoYear,
            Self::Month(_) | Self::MonthName(_) => DatetimeField::Month,
            Self::Day(_) => DatetimeField::Day,
            Self::DayOfYear(_) => DatetimeField::DayOfYear,
            Self::WeekdayName(_) | Self::IsoWeekday | Self::SundayWeekday => DatetimeField::Weekday,
            Self::IsoWeek(_) => DatetimeField::IsoWeek,
            Self::Hour(_) => DatetimeField::Hour,
            Self::TwelveHour(_) => DatetimeField::TwelveHour,
            Self::Meridiem => DatetimeField::Meridiem,
            Self::Minute(_) => DatetimeField::Minute,
            Self::Second(_) => DatetimeField::Second,
            Self::Fraction(_) | Self::OptionalFraction => DatetimeField::Fraction,
            Self::Offset(_) => DatetimeField::Offset,
            Self::UnixSeconds => DatetimeField::UnixTime,
        }
    }

    /// The most bytes this directive writes for an instant inside the DATETIME range, and the most
    /// it reads.
    fn longest(self) -> usize {
        match self {
            Self::Year | Self::IsoYear => 4,
            Self::Month(_)
            | Self::Day(_)
            | Self::IsoWeek(_)
            | Self::Hour(_)
            | Self::TwelveHour(_)
            | Self::Meridiem
            | Self::Minute(_)
            | Self::Second(_) => 2,
            Self::MonthName(NameLength::Abbreviated)
            | Self::WeekdayName(NameLength::Abbreviated) => 3,
            // `September` and `Wednesday`.
            Self::MonthName(NameLength::Full) | Self::WeekdayName(NameLength::Full) => 9,
            Self::DayOfYear(_) => 3,
            Self::IsoWeekday | Self::SundayWeekday => 1,
            Self::Fraction(digits) => digits.count(),
            Self::OptionalFraction => 10,
            Self::Offset(style) => style.longest(),
            // A sign and the 19 digits of the largest signed 64-bit count, the most a read Unix time
            // spans before its range is checked.
            Self::UnixSeconds => 20,
        }
    }
}

impl ZoneDirective {
    /// The most bytes this directive writes in `zone`.
    fn longest(self, zone: &Zone) -> usize {
        match self {
            Self::Abbreviation => zone.longest_abbreviation(),
            Self::Name => zone.to_string().len(),
        }
    }
}

impl fmt::Display for FormatDirective {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Year => formatter.write_str("%Y"),
            Self::IsoYear => formatter.write_str("%G"),
            Self::Month(padding) => write!(formatter, "%{}m", padding.flag()),
            Self::MonthName(NameLength::Abbreviated) => formatter.write_str("%b"),
            Self::MonthName(NameLength::Full) => formatter.write_str("%B"),
            Self::Day(DayPadding::Zero) => formatter.write_str("%d"),
            Self::Day(DayPadding::Unpadded) => formatter.write_str("%-d"),
            Self::Day(DayPadding::Space) => formatter.write_str("%e"),
            Self::DayOfYear(padding) => write!(formatter, "%{}j", padding.flag()),
            Self::WeekdayName(NameLength::Abbreviated) => formatter.write_str("%a"),
            Self::WeekdayName(NameLength::Full) => formatter.write_str("%A"),
            Self::IsoWeekday => formatter.write_str("%u"),
            Self::SundayWeekday => formatter.write_str("%w"),
            Self::IsoWeek(padding) => write!(formatter, "%{}V", padding.flag()),
            Self::Hour(padding) => write!(formatter, "%{}H", padding.flag()),
            Self::TwelveHour(padding) => write!(formatter, "%{}I", padding.flag()),
            Self::Meridiem => formatter.write_str("%p"),
            Self::Minute(padding) => write!(formatter, "%{}M", padding.flag()),
            Self::Second(padding) => write!(formatter, "%{}S", padding.flag()),
            Self::Fraction(FractionDigits::NANOSECONDS) => formatter.write_str("%f"),
            Self::Fraction(digits) => write!(formatter, "%{}f", digits.count()),
            Self::OptionalFraction => formatter.write_str("%.f"),
            Self::Offset(OffsetStyle::Compact) => formatter.write_str("%z"),
            Self::Offset(OffsetStyle::Colon) => formatter.write_str("%:z"),
            Self::Offset(OffsetStyle::Seconds) => formatter.write_str("%::z"),
            Self::UnixSeconds => formatter.write_str("%s"),
        }
    }
}

impl Padding {
    /// The flag that asks for this padding.
    const fn flag(self) -> &'static str {
        match self {
            Self::Zero => "",
            Self::Unpadded => "-",
        }
    }
}

/// Why a written format cannot be compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatDefect {
    /// `%` followed by something that is not a directive.
    UnknownDirective { directive: String, position: usize },
    /// A format that ends inside a directive.
    IncompleteDirective { position: usize },
    /// A directive a format can write but not read.
    Unreadable(ZoneDirective),
    /// A field a format reads twice.
    RepeatedField(DatetimeField),
    /// Two fields that each decide the same part of a date or time.
    ConflictingFields {
        first: DatetimeField,
        second: DatetimeField,
    },
    /// A format that reads no complete date.
    IncompleteDate,
    /// A field read without the field that gives it its meaning.
    MissingField {
        field: DatetimeField,
        required: DatetimeField,
    },
    /// A format whose values can be longer than the formatted value limit.
    TooLong { longest: usize },
}

impl fmt::Display for FormatDefect {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDirective {
                directive,
                position,
            } => write!(
                formatter,
                "unknown directive '{directive}' at byte {position}"
            ),
            Self::IncompleteDirective { position } => {
                write!(formatter, "incomplete directive at byte {position}")
            }
            Self::Unreadable(directive) => {
                write!(formatter, "'{directive}' can be written but not read")
            }
            Self::RepeatedField(field) => write!(formatter, "it reads the {field} more than once"),
            Self::ConflictingFields { first, second } => {
                write!(formatter, "it reads the {first} together with the {second}")
            }
            Self::IncompleteDate => formatter.write_str("it does not read a complete date"),
            Self::MissingField { field, required } => {
                write!(formatter, "it reads the {field} without the {required}")
            }
            Self::TooLong { longest } => write!(
                formatter,
                "its values can be up to {longest} bytes long, longer than the \
                 {LONGEST_FORMATTED_VALUE}-byte limit"
            ),
        }
    }
}

/// What a text had to hold where it stopped matching its format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TextExpectation {
    /// The format's literal text.
    Literal(triomphe::Arc<str>),
    /// A directive's field.
    Directive(FormatDirective),
}

impl fmt::Display for TextExpectation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(text) => write!(formatter, "'{}'", text.escape_debug()),
            Self::Directive(directive) => write!(formatter, "'{directive}'"),
        }
    }
}

/// Reads a written format into its items, joining adjacent literal text into one item.
fn compile_items(written: &str) -> Result<Vec<FormatItem>, FormatDefect> {
    let mut items = ItemsBuilder::default();
    let mut position = 0;
    while let Some(percent) = written[position..].find('%') {
        let directive_position = position
            .checked_add(percent)
            .assured("a position inside the written format is below its length");
        items.push_text(&written[position..directive_position]);
        let past_percent = directive_position
            .checked_add(1)
            .assured("the byte after a '%' is at most the end of the written format");
        let read = read_directive(&written[past_percent..], directive_position)?;
        match read.piece {
            DirectivePiece::Text(text) => items.push_text(text),
            DirectivePiece::Field(directive) => items.push_item(FormatItem::Field(directive)),
            DirectivePiece::Zone(directive) => items.push_item(FormatItem::Zone(directive)),
            DirectivePiece::Expansion(pieces) => {
                for piece in pieces {
                    match piece {
                        ExpandedPiece::Field(directive) => {
                            items.push_item(FormatItem::Field(*directive));
                        }
                        ExpandedPiece::Text(text) => items.push_text(text),
                    }
                }
            }
        }
        position = past_percent
            .checked_add(read.length)
            .assured("a directive ends inside the written format");
    }
    items.push_text(&written[position..]);
    Ok(items.finish())
}

/// The items of a format being compiled, and the literal text not yet closed into an item.
#[derive(Default)]
struct ItemsBuilder {
    items: Vec<FormatItem>,
    literal: String,
}

impl ItemsBuilder {
    fn push_text(&mut self, text: &str) {
        self.literal.push_str(text);
    }

    fn push_item(&mut self, item: FormatItem) {
        self.close_literal();
        self.items.push(item);
    }

    fn close_literal(&mut self) {
        if self.literal.is_empty() {
            return;
        }
        let text = std::mem::take(&mut self.literal);
        self.items
            .push(FormatItem::Literal(triomphe::Arc::from(text.as_str())));
    }

    fn finish(mut self) -> Vec<FormatItem> {
        self.close_literal();
        self.items
    }
}

/// A directive read from a format: what it stands for, and how many bytes past its `%` it spans.
struct ReadDirective {
    piece: DirectivePiece,
    length: usize,
}

enum DirectivePiece {
    /// Literal text a directive stands for, such as `%%`.
    Text(&'static str),
    Field(FormatDirective),
    Zone(ZoneDirective),
    /// The fields, and the literal text between them, a shorthand such as `%F` stands for.
    Expansion(&'static [ExpandedPiece]),
}

enum ExpandedPiece {
    Field(FormatDirective),
    Text(&'static str),
}

/// `%F`: `%Y-%m-%d`.
const DATE_EXPANSION: &[ExpandedPiece] = &[
    ExpandedPiece::Field(FormatDirective::Year),
    ExpandedPiece::Text("-"),
    ExpandedPiece::Field(FormatDirective::Month(Padding::Zero)),
    ExpandedPiece::Text("-"),
    ExpandedPiece::Field(FormatDirective::Day(DayPadding::Zero)),
];

/// `%T`: `%H:%M:%S`.
const TIME_EXPANSION: &[ExpandedPiece] = &[
    ExpandedPiece::Field(FormatDirective::Hour(Padding::Zero)),
    ExpandedPiece::Text(":"),
    ExpandedPiece::Field(FormatDirective::Minute(Padding::Zero)),
    ExpandedPiece::Text(":"),
    ExpandedPiece::Field(FormatDirective::Second(Padding::Zero)),
];

/// `%R`: `%H:%M`.
const HOUR_MINUTE_EXPANSION: &[ExpandedPiece] = &[
    ExpandedPiece::Field(FormatDirective::Hour(Padding::Zero)),
    ExpandedPiece::Text(":"),
    ExpandedPiece::Field(FormatDirective::Minute(Padding::Zero)),
];

/// Reads the directive after a `%` at `position`.
fn read_directive(after_percent: &str, position: usize) -> Result<ReadDirective, FormatDefect> {
    let bytes = after_percent.as_bytes();
    let Some(&first) = bytes.first() else {
        return Err(FormatDefect::IncompleteDirective { position });
    };
    let second = bytes.get(1).copied();
    let read = match first {
        b'%' => ReadDirective::text("%"),
        b'n' => ReadDirective::text("\n"),
        b't' => ReadDirective::text("\t"),
        b'-' => {
            let Some(letter) = second else {
                return Err(FormatDefect::IncompleteDirective { position });
            };
            let directive = match letter {
                b'm' => FormatDirective::Month(Padding::Unpadded),
                b'd' => FormatDirective::Day(DayPadding::Unpadded),
                b'j' => FormatDirective::DayOfYear(Padding::Unpadded),
                b'V' => FormatDirective::IsoWeek(Padding::Unpadded),
                b'H' => FormatDirective::Hour(Padding::Unpadded),
                b'I' => FormatDirective::TwelveHour(Padding::Unpadded),
                b'M' => FormatDirective::Minute(Padding::Unpadded),
                b'S' => FormatDirective::Second(Padding::Unpadded),
                _ => return Err(unknown_directive(after_percent, 1, position)),
            };
            ReadDirective::field(directive, 2)
        }
        b'.' => match second {
            Some(b'f') => ReadDirective::field(FormatDirective::OptionalFraction, 2),
            Some(_) => return Err(unknown_directive(after_percent, 1, position)),
            None => return Err(FormatDefect::IncompleteDirective { position }),
        },
        b'1'..=b'9' => match second {
            Some(b'f') => {
                let digits = digit_value(first).assured("the byte is a digit from '1' to '9'");
                let digits = u8::try_from(digits).assured("a digit value is below 10");
                ReadDirective::field(FormatDirective::Fraction(FractionDigits(digits)), 2)
            }
            Some(_) => return Err(unknown_directive(after_percent, 1, position)),
            None => return Err(FormatDefect::IncompleteDirective { position }),
        },
        b':' => match (second, bytes.get(2).copied()) {
            (Some(b'z'), _) => ReadDirective::field(FormatDirective::Offset(OffsetStyle::Colon), 2),
            (Some(b':'), Some(b'z')) => {
                ReadDirective::field(FormatDirective::Offset(OffsetStyle::Seconds), 3)
            }
            (Some(b':'), Some(_)) => return Err(unknown_directive(after_percent, 2, position)),
            (Some(b':'), None) | (None, _) => {
                return Err(FormatDefect::IncompleteDirective { position });
            }
            (Some(_), _) => return Err(unknown_directive(after_percent, 1, position)),
        },
        b'F' => ReadDirective::expansion(DATE_EXPANSION),
        b'T' => ReadDirective::expansion(TIME_EXPANSION),
        b'R' => ReadDirective::expansion(HOUR_MINUTE_EXPANSION),
        b'Z' => ReadDirective::zone(ZoneDirective::Abbreviation),
        b'Q' => ReadDirective::zone(ZoneDirective::Name),
        letter => {
            let directive = match letter {
                b'Y' => FormatDirective::Year,
                b'G' => FormatDirective::IsoYear,
                b'm' => FormatDirective::Month(Padding::Zero),
                b'b' => FormatDirective::MonthName(NameLength::Abbreviated),
                b'B' => FormatDirective::MonthName(NameLength::Full),
                b'd' => FormatDirective::Day(DayPadding::Zero),
                b'e' => FormatDirective::Day(DayPadding::Space),
                b'j' => FormatDirective::DayOfYear(Padding::Zero),
                b'a' => FormatDirective::WeekdayName(NameLength::Abbreviated),
                b'A' => FormatDirective::WeekdayName(NameLength::Full),
                b'u' => FormatDirective::IsoWeekday,
                b'w' => FormatDirective::SundayWeekday,
                b'V' => FormatDirective::IsoWeek(Padding::Zero),
                b'H' => FormatDirective::Hour(Padding::Zero),
                b'I' => FormatDirective::TwelveHour(Padding::Zero),
                b'p' => FormatDirective::Meridiem,
                b'M' => FormatDirective::Minute(Padding::Zero),
                b'S' => FormatDirective::Second(Padding::Zero),
                b'f' => FormatDirective::Fraction(FractionDigits::NANOSECONDS),
                b'z' => FormatDirective::Offset(OffsetStyle::Compact),
                b's' => FormatDirective::UnixSeconds,
                _ => return Err(unknown_directive(after_percent, 0, position)),
            };
            ReadDirective::field(directive, 1)
        }
    };
    Ok(read)
}

impl ReadDirective {
    const fn text(text: &'static str) -> Self {
        Self {
            piece: DirectivePiece::Text(text),
            length: 1,
        }
    }

    const fn field(directive: FormatDirective, length: usize) -> Self {
        Self {
            piece: DirectivePiece::Field(directive),
            length,
        }
    }

    const fn zone(directive: ZoneDirective) -> Self {
        Self {
            piece: DirectivePiece::Zone(directive),
            length: 1,
        }
    }

    const fn expansion(pieces: &'static [ExpandedPiece]) -> Self {
        Self {
            piece: DirectivePiece::Expansion(pieces),
            length: 1,
        }
    }
}

/// An unknown directive, spelled from its `%` through the character `offending` bytes past it.
fn unknown_directive(after_percent: &str, offending: usize, position: usize) -> FormatDefect {
    let character_length = match after_percent[offending..].chars().next() {
        Some(character) => character.len_utf8(),
        None => 0,
    };
    let end = offending
        .checked_add(character_length)
        .assured("a character ends inside the written format");
    FormatDefect::UnknownDirective {
        directive: format!("%{}", &after_percent[..end]),
        position,
    }
}

/// How the fields a readable format reads name one instant, before the call's zone is attached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParseLayout {
    /// `%s`, and optionally the fraction of the second after it.
    UnixTime,
    /// A local date and time, with or without a UTC offset.
    Civil {
        layout: CivilLayout,
        reads_offset: bool,
    },
}

/// The fields that decide a read local date and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CivilLayout {
    date: DateLayout,
    clock: ClockLayout,
    /// Whether a read day of the week only checks a date the other fields decide.
    checks_weekday: bool,
}

/// The fields that decide a read date.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum DateLayout {
    /// `%Y`, a month and a day of the month.
    Calendar,
    /// `%Y` and `%j`.
    Ordinal,
    /// `%G`, `%V` and a day of the week.
    IsoWeek,
}

/// The field that decides a read hour. Minutes, seconds and the fraction of a second read as zero
/// when a format does not read them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ClockLayout {
    /// No hour: the local time is midnight.
    Midnight,
    TwentyFourHour,
    TwelveHour,
}

/// The fields a format reads, each with the directive that reads it.
struct ReadFields(BTreeMap<DatetimeField, FormatDirective>);

impl ReadFields {
    fn of(items: &[ReadItem]) -> Result<Self, FormatDefect> {
        let mut fields = BTreeMap::new();
        for item in items {
            let ReadItem::Field(directive) = item else {
                continue;
            };
            let field = directive.field();
            if fields.insert(field, *directive).is_some() {
                return Err(FormatDefect::RepeatedField(field));
            }
        }
        Ok(Self(fields))
    }

    fn reads(&self, field: DatetimeField) -> bool {
        self.0.contains_key(&field)
    }

    /// Checks that no field in `others` is read, for a format that reads `field`.
    fn check_alone(
        &self,
        field: DatetimeField,
        others: &[DatetimeField],
    ) -> Result<(), FormatDefect> {
        for other in others {
            if self.reads(*other) {
                return Err(FormatDefect::ConflictingFields {
                    first: field,
                    second: *other,
                });
            }
        }
        Ok(())
    }

    /// Checks that `required` is read wherever `field` is.
    fn check_requires(
        &self,
        field: DatetimeField,
        required: DatetimeField,
    ) -> Result<(), FormatDefect> {
        if self.reads(field) && !self.reads(required) {
            return Err(FormatDefect::MissingField { field, required });
        }
        Ok(())
    }
}

impl ParseLayout {
    /// The layout of a readable format's fields, or why the format cannot name one instant.
    fn of(items: &[ReadItem]) -> Result<Self, FormatDefect> {
        let fields = ReadFields::of(items)?;
        if fields.reads(DatetimeField::UnixTime) {
            fields.check_alone(
                DatetimeField::UnixTime,
                &[
                    DatetimeField::Year,
                    DatetimeField::IsoYear,
                    DatetimeField::Month,
                    DatetimeField::Day,
                    DatetimeField::DayOfYear,
                    DatetimeField::Weekday,
                    DatetimeField::IsoWeek,
                    DatetimeField::Hour,
                    DatetimeField::TwelveHour,
                    DatetimeField::Meridiem,
                    DatetimeField::Minute,
                    DatetimeField::Second,
                    DatetimeField::Offset,
                ],
            )?;
            return Ok(Self::UnixTime);
        }
        let date = Self::date_of(&fields)?;
        let clock = Self::clock_of(&fields)?;
        let checks_weekday = match date {
            DateLayout::Calendar | DateLayout::Ordinal => fields.reads(DatetimeField::Weekday),
            DateLayout::IsoWeek => false,
        };
        Ok(Self::Civil {
            layout: CivilLayout {
                date,
                clock,
                checks_weekday,
            },
            reads_offset: fields.reads(DatetimeField::Offset),
        })
    }

    fn date_of(fields: &ReadFields) -> Result<DateLayout, FormatDefect> {
        let iso_field = if fields.reads(DatetimeField::IsoYear) {
            Some(DatetimeField::IsoYear)
        } else if fields.reads(DatetimeField::IsoWeek) {
            Some(DatetimeField::IsoWeek)
        } else {
            None
        };
        if let Some(iso_field) = iso_field {
            fields.check_alone(
                iso_field,
                &[
                    DatetimeField::Year,
                    DatetimeField::Month,
                    DatetimeField::Day,
                    DatetimeField::DayOfYear,
                ],
            )?;
            let complete = fields.reads(DatetimeField::IsoYear)
                && fields.reads(DatetimeField::IsoWeek)
                && fields.reads(DatetimeField::Weekday);
            if !complete {
                return Err(FormatDefect::IncompleteDate);
            }
            return Ok(DateLayout::IsoWeek);
        }
        if fields.reads(DatetimeField::DayOfYear) {
            fields.check_alone(
                DatetimeField::DayOfYear,
                &[DatetimeField::Month, DatetimeField::Day],
            )?;
            if !fields.reads(DatetimeField::Year) {
                return Err(FormatDefect::IncompleteDate);
            }
            return Ok(DateLayout::Ordinal);
        }
        let complete = fields.reads(DatetimeField::Year)
            && fields.reads(DatetimeField::Month)
            && fields.reads(DatetimeField::Day);
        if !complete {
            return Err(FormatDefect::IncompleteDate);
        }
        Ok(DateLayout::Calendar)
    }

    fn clock_of(fields: &ReadFields) -> Result<ClockLayout, FormatDefect> {
        if fields.reads(DatetimeField::Hour) {
            fields.check_alone(DatetimeField::Hour, &[DatetimeField::TwelveHour])?;
        }
        fields.check_requires(DatetimeField::TwelveHour, DatetimeField::Meridiem)?;
        fields.check_requires(DatetimeField::Meridiem, DatetimeField::TwelveHour)?;
        let clock = if fields.reads(DatetimeField::Hour) {
            ClockLayout::TwentyFourHour
        } else if fields.reads(DatetimeField::TwelveHour) {
            ClockLayout::TwelveHour
        } else {
            ClockLayout::Midnight
        };
        if let ClockLayout::Midnight = clock {
            fields.check_requires(DatetimeField::Minute, DatetimeField::Hour)?;
        }
        fields.check_requires(DatetimeField::Second, DatetimeField::Minute)?;
        fields.check_requires(DatetimeField::Fraction, DatetimeField::Second)?;
        Ok(clock)
    }
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
