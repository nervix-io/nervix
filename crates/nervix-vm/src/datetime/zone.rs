//! The time zones calendar builtins read local dates and times in.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Resolving a written time zone to UTC, a fixed UTC offset, or the rules of an IANA
//!   time zone from the database bundled into the binary; the UTC offset and abbreviation a zone
//!   shows at an instant; and the instants at which a zone shows a local date and time.
//! - **Depends on.** Jiff's time zone rules and civil datetimes, and the IANA Time Zone Database
//!   `jiff-tzdb` bundles.
//! - **Must not know.** The host's time zone, its time zone database or its locale, clocks, Arrow
//!   arrays, registers, or how a failure is recorded as a row error.
//!
//! Every node resolves a zone name against the same bundled database, so one build computes the
//! same local times wherever it runs. A zone is resolved once, when a call is lowered, and execution
//! only reads the rules that resolution produced.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};

use jiff::{
    Timestamp,
    civil::DateTime,
    tz::{AmbiguousOffset, Offset, TimeZone, TimeZoneOffsetInfo},
};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::program::Disambiguation;

const SECONDS_PER_MINUTE: u32 = 60;
const SECONDS_PER_HOUR: u32 = 60 * SECONDS_PER_MINUTE;
const NANOSECONDS_PER_SECOND: i128 = 1_000_000_000;

/// The largest hour a written fixed offset may name, so offsets run from `-23:59` to `+23:59`.
const LARGEST_FIXED_OFFSET_HOUR: u32 = 23;

/// A time zone a calendar builtin reads local dates and times in.
///
/// A call writes its zone as a literal, and the frontend resolves it once, when it lowers the call.
/// Two zones are the same zone when they resolved from the same spelling: an IANA link such as
/// `US/Eastern` stays distinct from `America/New_York` even though their rules agree, because a
/// format that writes the zone's name writes each of them differently.
#[derive(Clone)]
pub struct Zone(ZoneRules);

#[derive(Clone)]
enum ZoneRules {
    /// Coordinated Universal Time, the zone every DATETIME is written in.
    Utc,
    /// A UTC offset that never changes.
    Fixed(Offset),
    /// The rules of one zone of the bundled IANA Time Zone Database, under its canonical name.
    Named { name: &'static str, rules: TimeZone },
}

/// What identifies a zone, which is everything two zones are compared by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ZoneIdentity {
    Utc,
    Fixed(i32),
    Named(&'static str),
}

impl Zone {
    /// UTC, the zone a calendar builtin reads in when its call names no zone.
    pub const UTC: Self = Self(ZoneRules::Utc);

    /// Resolves a written zone: `UTC`, a UTC offset written `+HH:MM` or `-HH:MM`, or the name of an
    /// IANA time zone in the bundled database. Letter case does not matter in a name. Answers
    /// `None` for anything else.
    pub fn resolve(written: &str) -> Option<Self> {
        if written.eq_ignore_ascii_case("UTC") {
            return Some(Self::UTC);
        }
        if let Some(offset) = fixed_offset(written) {
            return Some(Self(ZoneRules::Fixed(offset)));
        }
        let (name, data) = jiff_tzdb::get(written)?;
        let rules = TimeZone::tzif(name, data).assured(
            "every zone of the bundled database is valid TZif data, which the zone tests check \
             for each of them",
        );
        Some(Self(ZoneRules::Named { name, rules }))
    }

    fn identity(&self) -> ZoneIdentity {
        match &self.0 {
            ZoneRules::Utc => ZoneIdentity::Utc,
            ZoneRules::Fixed(offset) => ZoneIdentity::Fixed(offset.seconds()),
            ZoneRules::Named { name, .. } => ZoneIdentity::Named(name),
        }
    }

    /// The UTC offsets this zone shows over time.
    pub(crate) fn offsets(&self) -> ZoneOffsets<'_> {
        match &self.0 {
            ZoneRules::Utc => ZoneOffsets::Constant(ConstantOffset {
                offset: Offset::UTC,
                abbreviation: ShortText::from_str("UTC"),
            }),
            ZoneRules::Fixed(offset) => ZoneOffsets::Constant(ConstantOffset {
                offset: *offset,
                abbreviation: ShortText::offset(offset.seconds(), OffsetStyle::Colon),
            }),
            ZoneRules::Named { rules, .. } => ZoneOffsets::Rules(RuleOffsets { rules, span: None }),
        }
    }

    /// The instants at which this zone shows `local`.
    pub(crate) fn instants_of(&self, local: DateTime) -> LocalInstants {
        let local_nanoseconds = utc_nanoseconds(local);
        match &self.0 {
            ZoneRules::Utc => LocalInstants::Unique(local_nanoseconds),
            ZoneRules::Fixed(offset) => {
                LocalInstants::Unique(instant_at_offset(local_nanoseconds, *offset))
            }
            ZoneRules::Named { rules, .. } => match rules.to_ambiguous_timestamp(local).offset() {
                AmbiguousOffset::Unambiguous { offset } => {
                    LocalInstants::Unique(instant_at_offset(local_nanoseconds, offset))
                }
                // A forward transition raises the offset. Reading the skipped time at the raised
                // offset moves the clock back further, which names the earlier instant.
                AmbiguousOffset::Gap { before, after } => LocalInstants::Skipped {
                    earlier: instant_at_offset(local_nanoseconds, after),
                    later: instant_at_offset(local_nanoseconds, before),
                },
                AmbiguousOffset::Fold { before, after } => LocalInstants::Repeated {
                    earlier: instant_at_offset(local_nanoseconds, before),
                    later: instant_at_offset(local_nanoseconds, after),
                },
            },
        }
    }

    /// The longest abbreviation this zone shows at an instant inside the DATETIME range.
    pub(crate) fn longest_abbreviation(&self) -> usize {
        let ZoneRules::Named { rules, .. } = &self.0 else {
            return self.to_string().len();
        };
        // The whole seconds holding the first and last DATETIME, which jiff's rule lookups read
        // exactly.
        let first = Timestamp::from_second(i64::MIN.div_euclid(1_000_000_000))
            .assured("the first second of the DATETIME range is a jiff timestamp");
        let last = Timestamp::from_second(i64::MAX.div_euclid(1_000_000_000))
            .assured("the last second of the DATETIME range is a jiff timestamp");
        let mut longest = rules.to_offset_info(first).abbreviation().len();
        for transition in rules.following(first) {
            if transition.timestamp() > last {
                break;
            }
            longest = longest.max(transition.abbreviation().len());
        }
        longest
    }
}

impl fmt::Display for Zone {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            ZoneRules::Utc => formatter.write_str("UTC"),
            ZoneRules::Fixed(offset) => {
                let text = ShortText::offset(offset.seconds(), OffsetStyle::Colon);
                formatter.write_str(text.as_str())
            }
            ZoneRules::Named { name, .. } => formatter.write_str(name),
        }
    }
}

impl fmt::Debug for Zone {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Zone({self})")
    }
}

impl PartialEq for Zone {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Zone {}

impl PartialOrd for Zone {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Zone {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity().cmp(&other.identity())
    }
}

impl Hash for Zone {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

/// Reads a UTC offset written `+HH:MM` or `-HH:MM`, with hours from `00` to `23` and minutes from
/// `00` to `59`.
fn fixed_offset(written: &str) -> Option<Offset> {
    let [sign, hour_tens, hour_ones, b':', minute_tens, minute_ones] = written.as_bytes() else {
        return None;
    };
    let negative = match sign {
        b'+' => false,
        b'-' => true,
        _ => return None,
    };
    let hours = two_digits(*hour_tens, *hour_ones)?;
    let minutes = two_digits(*minute_tens, *minute_ones)?;
    if hours > LARGEST_FIXED_OFFSET_HOUR || minutes >= SECONDS_PER_MINUTE {
        return None;
    }
    let hour_seconds = hours
        .checked_mul(SECONDS_PER_HOUR)
        .assured("at most 23 hours in seconds fits u32");
    let minute_seconds = minutes
        .checked_mul(SECONDS_PER_MINUTE)
        .assured("at most 59 minutes in seconds fits u32");
    let magnitude = hour_seconds
        .checked_add(minute_seconds)
        .assured("less than a day in seconds fits u32");
    let magnitude = i32::try_from(magnitude).assured("less than a day in seconds fits i32");
    let seconds = if negative {
        magnitude
            .checked_neg()
            .assured("less than a day in seconds negates inside i32")
    } else {
        magnitude
    };
    Some(Offset::from_seconds(seconds).assured("an offset of less than a day is a jiff offset"))
}

/// The value of two ASCII digits, or `None` when either byte is not a digit.
pub(crate) fn two_digits(tens: u8, ones: u8) -> Option<u32> {
    let tens = digit_value(tens)?;
    let ones = digit_value(ones)?;
    let tens = tens
        .checked_mul(10)
        .assured("a digit times ten is below 100");
    let value = tens
        .checked_add(ones)
        .assured("two digits make a value below 100");
    Some(value)
}

/// The value of one ASCII digit, or `None` when the byte is not a digit.
pub(crate) fn digit_value(byte: u8) -> Option<u32> {
    let value = byte.checked_sub(b'0')?;
    if value > 9 {
        return None;
    }
    Some(u32::from(value))
}

/// The nanoseconds since the Unix epoch at which UTC shows `local`: how far `local` lies past
/// `1970-01-01T00:00:00` on the proleptic Gregorian calendar.
pub(crate) fn utc_nanoseconds(local: DateTime) -> i128 {
    Offset::UTC
        .to_timestamp(local)
        .assured("a civil datetime jiff holds is a jiff timestamp at the UTC offset")
        .as_nanosecond()
}

/// The instant at which a zone whose offset is `offset` shows the local time lying
/// `local_nanoseconds` past the Unix epoch.
pub(crate) fn instant_at_offset(local_nanoseconds: i128, offset: Offset) -> i128 {
    let offset_nanoseconds = i128::from(offset.seconds())
        .checked_mul(NANOSECONDS_PER_SECOND)
        .assured("an offset of less than 26 hours in nanoseconds fits i128");
    local_nanoseconds
        .checked_sub(offset_nanoseconds)
        .assured("a jiff civil datetime in nanoseconds, less than a day, fits i128")
}

/// The jiff timestamp of an instant a DATETIME lane holds.
///
/// It is built from whole seconds and the nanoseconds past them, which a lane computes with 64-bit
/// division, rather than from the nanosecond count, which jiff divides in 128 bits.
pub(crate) fn timestamp(instant: i64) -> Timestamp {
    let second = instant.div_euclid(1_000_000_000);
    let nanosecond = instant.rem_euclid(1_000_000_000);
    let nanosecond =
        i32::try_from(nanosecond).assured("the nanoseconds past a second are below 10^9");
    Timestamp::new(second, nanosecond)
        .assured("jiff timestamps span every signed 64-bit nanosecond count")
}

/// The instants at which a zone shows one local date and time, in nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalInstants {
    /// The zone shows the local time exactly once.
    Unique(i128),
    /// A forward transition skips the local time, so no instant shows it. `earlier` reads it at the
    /// offset after the transition and `later` at the offset before it.
    Skipped { earlier: i128, later: i128 },
    /// A backward transition repeats the local time, so two instants show it.
    Repeated { earlier: i128, later: i128 },
}

/// A local time a zone skips or repeats, which a rejecting disambiguation leaves without an
/// instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnresolvedLocalTime {
    Skipped,
    Repeated,
}

impl LocalInstants {
    /// The one instant `disambiguation` chooses.
    ///
    /// `compatible` chooses the earlier instant of a repeated time, and for a skipped time the
    /// later instant, which lies as far past the transition as the time lies past the start of the
    /// gap. Calendar arithmetic moves local times across transitions the same way.
    pub(crate) fn resolve(
        self,
        disambiguation: Disambiguation,
    ) -> Result<i128, UnresolvedLocalTime> {
        match (self, disambiguation) {
            (Self::Unique(instant), _) => Ok(instant),
            (Self::Skipped { later, .. }, Disambiguation::Compatible | Disambiguation::Later)
            | (Self::Repeated { later, .. }, Disambiguation::Later) => Ok(later),
            (Self::Skipped { earlier, .. }, Disambiguation::Earlier)
            | (
                Self::Repeated { earlier, .. },
                Disambiguation::Compatible | Disambiguation::Earlier,
            ) => Ok(earlier),
            (Self::Skipped { .. }, Disambiguation::Reject) => Err(UnresolvedLocalTime::Skipped),
            (Self::Repeated { .. }, Disambiguation::Reject) => Err(UnresolvedLocalTime::Repeated),
        }
    }
}

/// The UTC offsets a zone shows over time.
pub(crate) enum ZoneOffsets<'z> {
    /// UTC or a fixed offset, which shows one offset at every instant.
    Constant(ConstantOffset),
    /// IANA rules, whose offset changes at transitions.
    Rules(RuleOffsets<'z>),
}

impl ZoneOffsets<'_> {
    /// The UTC offset shown at `instant`.
    pub(crate) fn offset_at(&mut self, instant: i64) -> Offset {
        match self {
            Self::Constant(constant) => constant.offset,
            Self::Rules(rules) => rules.span_at(instant).info.offset(),
        }
    }

    /// Reads the UTC offset shown at `instant` together with the abbreviation the zone shows then.
    pub(crate) fn read_offset_and_abbreviation<R>(
        &mut self,
        instant: i64,
        read: impl FnOnce(Offset, &str) -> R,
    ) -> R {
        match self {
            Self::Constant(constant) => read(constant.offset, constant.abbreviation.as_str()),
            Self::Rules(rules) => {
                let info = &rules.span_at(instant).info;
                read(info.offset(), info.abbreviation())
            }
        }
    }
}

/// The one offset UTC or a fixed offset shows, and the abbreviation shown with it: `UTC`, or the
/// offset written `+HH:MM`.
pub(crate) struct ConstantOffset {
    pub(crate) offset: Offset,
    pub(crate) abbreviation: ShortText,
}

/// The offsets IANA rules show. It remembers the span of time between two transitions that held
/// the last instant, so consecutive instants of a column inside one span look the rules up once.
pub(crate) struct RuleOffsets<'z> {
    rules: &'z TimeZone,
    span: Option<OffsetSpan<'z>>,
}

/// A span of time between two transitions, during which a zone shows one offset.
pub(crate) struct OffsetSpan<'z> {
    /// The transition that starts the span, in nanoseconds, or `None` when none precedes it.
    pub(crate) start: Option<i128>,
    /// The transition that ends the span, or `None` when none follows it.
    end: Option<i128>,
    pub(crate) info: TimeZoneOffsetInfo<'z>,
}

impl OffsetSpan<'_> {
    fn contains(&self, instant: i128) -> bool {
        let after_start = match self.start {
            Some(start) => start <= instant,
            None => true,
        };
        let before_end = match self.end {
            Some(end) => instant < end,
            None => true,
        };
        after_start && before_end
    }
}

impl<'z> RuleOffsets<'z> {
    /// The span of time holding `instant`.
    pub(crate) fn span_at(&mut self, instant: i64) -> &OffsetSpan<'z> {
        let nanoseconds = i128::from(instant);
        let remembered = match &self.span {
            Some(span) => span.contains(nanoseconds),
            None => false,
        };
        if !remembered {
            self.span = Some(self.span_holding(nanoseconds));
        }
        self.span
            .as_ref()
            .verified("the span was replaced above unless it already held the instant")
    }

    /// The span of time holding an instant in jiff's timestamp range.
    pub(crate) fn span_holding(&self, instant: i128) -> OffsetSpan<'z> {
        // Transitions happen at whole seconds, so the second an instant falls in lies in the same
        // span as the instant. Jiff's rule lookups read a timestamp's seconds truncated toward
        // zero, which before the Unix epoch is the following second, so they are asked about the
        // whole second at or before the instant instead.
        let second = instant.div_euclid(NANOSECONDS_PER_SECOND);
        let second = i64::try_from(second)
            .assured("an instant within a day of the DATETIME range in seconds fits i64");
        let at = Timestamp::from_second(second)
            .assured("a second within a day of the DATETIME range is a jiff timestamp");
        let next_second = second
            .checked_add(1)
            .assured("a second within a day of the DATETIME range plus one fits i64");
        let next_second = Timestamp::from_second(next_second)
            .assured("a second within a day of the DATETIME range is a jiff timestamp");
        // `preceding` yields only transitions strictly before the timestamp it starts from, and a
        // transition at the instant's own second starts the span holding it.
        let start = self
            .rules
            .preceding(next_second)
            .next()
            .map(|transition| transition.timestamp().as_nanosecond());
        let end = self
            .rules
            .following(at)
            .next()
            .map(|transition| transition.timestamp().as_nanosecond());
        OffsetSpan {
            start,
            end,
            info: self.rules.to_offset_info(at),
        }
    }
}

/// How `%z`, `%:z` and `%::z` write a UTC offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OffsetStyle {
    /// `+hhmm`, followed by `ss` when the offset has seconds.
    Compact,
    /// `+hh:mm`, followed by `:ss` when the offset has seconds.
    Colon,
    /// `+hh:mm:ss`.
    Seconds,
}

impl OffsetStyle {
    /// The most bytes an offset written in this style takes.
    pub(crate) const fn longest(self) -> usize {
        match self {
            Self::Compact => 7,
            Self::Colon | Self::Seconds => 9,
        }
    }
}

/// A short ASCII text held inline: a UTC offset written in one style, or `UTC`.
pub(crate) struct ShortText {
    bytes: [u8; OffsetStyle::Seconds.longest()],
    len: usize,
}

impl ShortText {
    fn from_str(text: &str) -> Self {
        let mut bytes = [0_u8; OffsetStyle::Seconds.longest()];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        Self {
            bytes,
            len: text.len(),
        }
    }

    /// Writes an offset of `seconds` east of UTC.
    pub(crate) fn offset(seconds: i32, style: OffsetStyle) -> Self {
        let sign = if seconds < 0 { b'-' } else { b'+' };
        let magnitude = seconds.unsigned_abs();
        let hours = magnitude / SECONDS_PER_HOUR;
        let minutes = magnitude % SECONDS_PER_HOUR / SECONDS_PER_MINUTE;
        let remaining_seconds = magnitude % SECONDS_PER_MINUTE;
        let mut text = Self {
            bytes: [0_u8; OffsetStyle::Seconds.longest()],
            len: 0,
        };
        text.push(sign);
        text.push_two_digits(hours);
        if let OffsetStyle::Colon | OffsetStyle::Seconds = style {
            text.push(b':');
        }
        text.push_two_digits(minutes);
        let writes_seconds = match style {
            OffsetStyle::Compact | OffsetStyle::Colon => remaining_seconds != 0,
            OffsetStyle::Seconds => true,
        };
        if writes_seconds {
            if let OffsetStyle::Colon | OffsetStyle::Seconds = style {
                text.push(b':');
            }
            text.push_two_digits(remaining_seconds);
        }
        text
    }

    fn push(&mut self, byte: u8) {
        self.bytes[self.len] = byte;
        self.len = self
            .len
            .checked_add(1)
            .assured("an offset text is at most nine bytes");
    }

    /// Pushes a value below 100 as two digits. Every part of an offset below 26 hours is below 100.
    fn push_two_digits(&mut self, value: u32) {
        self.push(ascii_digit(value / 10 % 10));
        self.push(ascii_digit(value % 10));
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes())
            .assured("an offset text is ASCII signs, digits and colons")
    }
}

/// The ASCII digit for a value below 10.
pub(crate) fn ascii_digit(value: u32) -> u8 {
    let value = u8::try_from(value).assured("a digit value is below 10");
    b'0'.checked_add(value)
        .assured("a digit value below 10 added to b'0' is at most b'9'")
}

#[cfg(test)]
#[path = "zone_tests.rs"]
mod tests;
