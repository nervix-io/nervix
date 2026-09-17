//! Local calendar arithmetic over DATETIME columns.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The kernels that read DATETIME lanes as local dates and times in a time zone:
//!   extracting a local date part, truncating to the start of a calendar unit or of a local unit
//!   under the rules of an IANA zone, moving by calendar days and months, and counting whole
//!   calendar days and months between two instants.
//! - **Depends on.** Jiff's civil calendar, the time zones of the datetime kernels, and the checked
//!   lanes of the numeric kernels.
//! - **Must not know.** Registers, programs, spans, clocks, or how a failed lane is recorded as a
//!   row error.
//!
//! These kernels compute lane by lane, because the length of a local day, month or year depends on
//! the lane's own date and, under the rules of an IANA zone, on the transitions near it.

use std::cmp::Ordering;

use arrow_array::{
    Array, ArrowPrimitiveType, Int64Array, PrimitiveArray, TimestampNanosecondArray,
    types::{Int64Type, TimestampNanosecondType},
};
use arrow_buffer::NullBuffer;
use jiff::{
    Span,
    civil::{Date, DateTime, Time},
    tz::Offset,
};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    UnitCounts, datetime_column, i64_lane,
    zone::{RuleOffsets, Zone, ZoneOffsets, instant_at_offset, timestamp, utc_nanoseconds},
};
use crate::{
    numeric::{Checked, Lanes},
    program::{CalendarUnit, DatePart, DatetimeUnit, Disambiguation, FixedTimeUnit},
};

/// The most days a calendar difference moves its end date toward its start while looking for the
/// latest date whose start time of day does not lie past the end.
///
/// The end's own date needs no move, a start time of day later than the end's needs one, and a
/// transition that shifts local time needs one more. The bundled database shifts local time by less
/// than two days, which the zone tests check for every zone, so three moves always suffice.
const MOST_DAY_CORRECTIONS: i64 = 3;

/// Extracts one date part from every lane, read in the local time `offsets` shows.
pub(super) fn local_date_part(
    values: &TimestampNanosecondArray,
    part: DatePart,
    mut offsets: ZoneOffsets<'_>,
) -> Int64Array {
    let lanes = unary_lanes(values.values(), values.nulls(), |instant: i64| {
        let local = local_datetime(&mut offsets, instant);
        (part.read_local(local), false)
    });
    Checked::<Int64Type>::from_lanes(lanes, values.nulls().cloned()).column
}

/// The local date and time `offsets` shows at `instant`.
fn local_datetime(offsets: &mut ZoneOffsets<'_>, instant: i64) -> DateTime {
    offsets.offset_at(instant).to_datetime(timestamp(instant))
}

impl DatePart {
    /// This part of a local date and time.
    fn read_local(self, local: DateTime) -> i64 {
        match self {
            Self::Year => i64::from(local.year()),
            Self::Quarter => i64::from(month_quarter(local.month())),
            Self::Month => i64::from(local.month()),
            Self::Day => i64::from(local.day()),
            Self::Hour => i64::from(local.hour()),
            Self::Minute => i64::from(local.minute()),
            Self::Second => i64::from(local.second()),
            Self::Millisecond => i64::from(local.subsec_nanosecond() / 1_000_000),
            Self::Microsecond => i64::from(local.subsec_nanosecond() / 1_000),
            Self::Nanosecond => i64::from(local.subsec_nanosecond()),
            Self::DayOfWeek => i64::from(local.weekday().to_sunday_zero_offset()),
            Self::DayOfYear => i64::from(local.day_of_year()),
            Self::IsoYear => i64::from(local.iso_week_date().year()),
            Self::IsoWeek => i64::from(local.iso_week_date().week()),
            Self::IsoDayOfWeek => i64::from(local.weekday().to_monday_one_offset()),
        }
    }
}

/// The quarter, from 1 to 4, that a month from 1 to 12 belongs to.
fn month_quarter(month: i8) -> i8 {
    month
        .checked_add(2)
        .assured("a month is at most 12")
        .checked_div(3)
        .assured("three is not zero")
}

/// Truncates every lane to the start of the month, quarter or year that holds it on a clock `offset`
/// ahead of UTC.
pub(super) fn truncate_to_calendar_unit_at_offset(
    values: &TimestampNanosecondArray,
    unit: CalendarUnit,
    offset: Offset,
) -> Checked<TimestampNanosecondType> {
    let lanes = unary_lanes(values.values(), values.nulls(), |instant: i64| {
        let local = offset.to_datetime(timestamp(instant));
        let start = local_unit_start(local, DatetimeUnit::Calendar(unit));
        i64_lane(instant_at_offset(utc_nanoseconds(start), offset))
    });
    datetime_column(Checked::from_lanes(lanes, values.nulls().cloned()))
}

/// Truncates every lane to the start of the local unit that holds it under the rules of an IANA
/// zone.
pub(super) fn truncate_under_rules(
    values: &TimestampNanosecondArray,
    unit: DatetimeUnit,
    mut rules: RuleOffsets<'_>,
) -> Checked<TimestampNanosecondType> {
    let lanes = unary_lanes(values.values(), values.nulls(), |instant: i64| {
        i64_lane(local_unit_first_instant(&mut rules, instant, unit))
    });
    datetime_column(Checked::from_lanes(lanes, values.nulls().cloned()))
}

/// The first instant of the stretch of time, ending at `instant`, during which the zone's clock
/// showed a local time inside the unit that holds the local time at `instant`.
///
/// Inside one span between transitions the clock runs with UTC, so the unit started where the
/// clock showed the unit's start, unless that lies at or before the transition starting the span.
/// Then the stretch reaches back into the previous span only while the clock there still showed a
/// time inside the unit, and otherwise it starts at the transition itself: a gap that skipped the
/// unit's start, or a jump back into the unit from later in the calendar.
fn local_unit_first_instant(rules: &mut RuleOffsets<'_>, instant: i64, unit: DatetimeUnit) -> i128 {
    let span = rules.span_at(instant);
    let mut offset = span.info.offset();
    let mut span_start = span.start;
    let local = offset.to_datetime(timestamp(instant));
    let unit_start = local_unit_start(local, unit);
    let unit_start_nanoseconds = utc_nanoseconds(unit_start);
    loop {
        let candidate = instant_at_offset(unit_start_nanoseconds, offset);
        let Some(transition) = span_start else {
            return candidate;
        };
        if candidate > transition {
            return candidate;
        }
        let last_before = transition.checked_sub(1).assured(
            "a transition within a day of the DATETIME range less one nanosecond fits i128",
        );
        let previous = rules.span_holding(last_before);
        let previous_offset = previous.info.offset();
        let last_local = previous_offset.to_datetime(
            jiff::Timestamp::from_nanosecond(last_before)
                .assured("an instant within a day of the DATETIME range is a jiff timestamp"),
        );
        if local_unit_start(last_local, unit) != unit_start {
            return transition;
        }
        offset = previous_offset;
        span_start = previous.start;
    }
}

/// The local date and time at which the unit holding `local` starts on the calendar.
fn local_unit_start(local: DateTime, unit: DatetimeUnit) -> DateTime {
    let date = local.date();
    let time = local.time();
    match unit {
        DatetimeUnit::Fixed(FixedTimeUnit::Nanosecond) => local,
        DatetimeUnit::Fixed(FixedTimeUnit::Microsecond) => date.to_datetime(truncated_time(
            time,
            time.subsec_nanosecond() / 1_000 * 1_000,
        )),
        DatetimeUnit::Fixed(FixedTimeUnit::Millisecond) => date.to_datetime(truncated_time(
            time,
            time.subsec_nanosecond() / 1_000_000 * 1_000_000,
        )),
        DatetimeUnit::Fixed(FixedTimeUnit::Second) => date.to_datetime(truncated_time(time, 0)),
        DatetimeUnit::Fixed(FixedTimeUnit::Minute) => {
            date.to_datetime(clock_time(time.hour(), time.minute()))
        }
        DatetimeUnit::Fixed(FixedTimeUnit::Hour) => date.to_datetime(clock_time(time.hour(), 0)),
        DatetimeUnit::Fixed(FixedTimeUnit::Day) => date.to_datetime(Time::midnight()),
        DatetimeUnit::Fixed(FixedTimeUnit::Week) => {
            let days_since_monday = i64::from(date.weekday().to_monday_zero_offset());
            let monday = date
                .checked_sub(Span::new().days(days_since_monday))
                .assured(
                    "a local date inside the DATETIME range has a Monday on the jiff calendar",
                );
            monday.to_datetime(Time::midnight())
        }
        DatetimeUnit::Calendar(CalendarUnit::Month) => {
            date.first_of_month().to_datetime(Time::midnight())
        }
        DatetimeUnit::Calendar(CalendarUnit::Quarter) => {
            let first_month = month_quarter(date.month())
                .checked_sub(1)
                .assured("a quarter is at least 1")
                .checked_mul(3)
                .assured("three quarters of months fit i8")
                .checked_add(1)
                .assured("the first month of a quarter is at most 10");
            Date::new(date.year(), first_month, 1)
                .assured("the first day of a quarter's first month exists in every year")
                .to_datetime(Time::midnight())
        }
        DatetimeUnit::Calendar(CalendarUnit::Year) => {
            date.first_of_year().to_datetime(Time::midnight())
        }
    }
}

/// `time` with its fraction of a second replaced by `subsec_nanosecond`.
fn truncated_time(time: Time, subsec_nanosecond: i32) -> Time {
    Time::new(time.hour(), time.minute(), time.second(), subsec_nanosecond)
        .assured("a fraction truncated from a valid time is a valid fraction")
}

/// The start of a minute of the local clock.
fn clock_time(hour: i8, minute: i8) -> Time {
    Time::new(hour, minute, 0, 0).assured("an hour and minute read from a valid time are valid")
}

/// A step that calendar arithmetic moves a local date by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CalendarStep {
    /// This many local days per unit.
    Days(i64),
    /// This many calendar months per unit.
    Months(i64),
}

/// Moves every lane by its amount of calendar steps in `zone`'s local time, keeping the local time
/// of day and resolving the moved local time compatibly.
pub(super) fn add_calendar_steps(
    amounts: &UnitCounts<'_>,
    values: &TimestampNanosecondArray,
    step: CalendarStep,
    zone: &Zone,
) -> Checked<TimestampNanosecondType> {
    match amounts {
        UnitCounts::UInt8(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::Int8(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::UInt16(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::Int16(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::UInt32(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::Int32(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::UInt64(amounts) => add_steps_lanes(amounts, values, step, zone),
        UnitCounts::Int64(amounts) => add_steps_lanes(amounts, values, step, zone),
    }
}

fn add_steps_lanes<T>(
    amounts: &PrimitiveArray<T>,
    values: &TimestampNanosecondArray,
    step: CalendarStep,
    zone: &Zone,
) -> Checked<TimestampNanosecondType>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let mut offsets = zone.offsets();
    let nulls = NullBuffer::union(amounts.nulls(), values.nulls());
    let lanes = binary_lanes(
        amounts.values(),
        values.values(),
        nulls.as_ref(),
        |amount: T::Native, instant: i64| {
            let moved = moved_instant(&mut offsets, zone, step, amount.into(), instant);
            match moved {
                Some(moved) => i64_lane(moved),
                None => (0, true),
            }
        },
    );
    datetime_column(Checked::from_lanes(lanes, nulls))
}

/// The instant `amount` steps away from `instant` in `zone`'s local time, or `None` when the moved
/// local date lies past the end of the calendar jiff holds, which is far outside the DATETIME range.
///
/// No step leaves the instant itself, even inside a repeated local time, where resolving its local
/// time again could choose the other instant that shows it.
fn moved_instant(
    offsets: &mut ZoneOffsets<'_>,
    zone: &Zone,
    step: CalendarStep,
    amount: i128,
    instant: i64,
) -> Option<i128> {
    if amount == 0 {
        return Some(i128::from(instant));
    }
    let local = local_datetime(offsets, instant);
    let moved_date = step.move_date(local.date(), amount)?;
    let moved = moved_date.to_datetime(local.time());
    let resolved = zone
        .instants_of(moved)
        .resolve(Disambiguation::Compatible)
        .assured("compatible disambiguation chooses an instant for every local time");
    Some(resolved)
}

impl CalendarStep {
    /// `date` moved by `amount` steps, or `None` when the result is not a jiff date. A month step
    /// that lands past the end of a shorter month lands on its last day.
    fn move_date(self, date: Date, amount: i128) -> Option<Date> {
        let span = match self {
            Self::Days(days) => {
                let days = amount
                    .checked_mul(i128::from(days))
                    .assured("an integer operand below 2^64 times seven fits i128");
                Span::new().try_days(i64::try_from(days).ok()?).ok()?
            }
            Self::Months(months) => {
                let months = amount
                    .checked_mul(i128::from(months))
                    .assured("an integer operand below 2^64 times twelve fits i128");
                Span::new().try_months(i64::try_from(months).ok()?).ok()?
            }
        };
        date.checked_add(span).ok()
    }
}

/// Counts the whole calendar steps from every start lane to its end lane in `zone`'s local time,
/// rounding toward zero.
pub(super) fn count_calendar_steps(
    starts: &TimestampNanosecondArray,
    ends: &TimestampNanosecondArray,
    step: CalendarStep,
    zone: &Zone,
) -> Checked<Int64Type> {
    let mut start_offsets = zone.offsets();
    let mut end_offsets = zone.offsets();
    let nulls = NullBuffer::union(starts.nulls(), ends.nulls());
    let lanes = binary_lanes(
        starts.values(),
        ends.values(),
        nulls.as_ref(),
        |start: i64, end: i64| {
            let start_local = local_datetime(&mut start_offsets, start);
            let end_local = local_datetime(&mut end_offsets, end);
            let steps = step.count_between(zone, start_local, end_local, end, end.cmp(&start));
            (steps, false)
        },
    );
    Checked::from_lanes(lanes, nulls)
}

impl CalendarStep {
    /// The whole steps from a start to an end in `zone`'s local time, where `direction` orders the
    /// end instant against the start instant.
    ///
    /// The end's date counts only once its local time of day has reached the start's: the latest
    /// date on the start's side of the end at which the start's time of day, resolved compatibly,
    /// does not lie past the end. Whole months then count from the start's date to that date, a
    /// month being complete once that date has reached the start's day of the month.
    fn count_between(
        self,
        zone: &Zone,
        start_local: DateTime,
        end_local: DateTime,
        end: i64,
        direction: Ordering,
    ) -> i64 {
        if let Ordering::Equal = direction {
            return 0;
        }
        let start_date = start_local.date();
        if start_date == end_local.date() {
            return 0;
        }
        let counted_date = latest_counted_date(zone, start_local, end_local, end, direction);
        match self {
            Self::Days(days) => {
                let elapsed_days = start_date.duration_until(counted_date).as_hours() / 24;
                elapsed_days / days
            }
            Self::Months(months) => months_between(start_date, counted_date) / months,
        }
    }
}

/// The date a calendar difference counts to: the end's date, moved toward the start by as few whole
/// days as it takes for the start's local time of day on that date, resolved compatibly, to not lie
/// past the end.
fn latest_counted_date(
    zone: &Zone,
    start_local: DateTime,
    end_local: DateTime,
    end: i64,
    direction: Ordering,
) -> Date {
    let toward_start = match direction {
        Ordering::Greater => -1,
        Ordering::Less | Ordering::Equal => 1,
    };
    // A time of day on the end's date past the end's own time of day already lies past the end,
    // unless a transition between them moved the clock back.
    let first_correction = match (direction, end_local.time().cmp(&start_local.time())) {
        (Ordering::Greater, Ordering::Less) | (Ordering::Less, Ordering::Greater) => 1,
        _ => 0,
    };
    let end = i128::from(end);
    let mut counted = None;
    for correction in first_correction..=MOST_DAY_CORRECTIONS {
        let days = correction
            .checked_mul(toward_start)
            .assured("a correction of at most three days negates inside i64");
        let date = end_local
            .date()
            .checked_add(Span::new().days(days))
            .assured("a local date inside the DATETIME range moved by three days is a jiff date");
        let candidate = zone
            .instants_of(date.to_datetime(start_local.time()))
            .resolve(Disambiguation::Compatible)
            .assured("compatible disambiguation chooses an instant for every local time");
        let past_end = match direction {
            Ordering::Greater => candidate > end,
            Ordering::Less => candidate < end,
            Ordering::Equal => false,
        };
        if !past_end {
            counted = Some(date);
            break;
        }
    }
    counted.assured(
        "the bundled database shifts local time by less than two days, which the zone tests check \
         for every zone, so three corrections reach a date whose start time does not lie past the \
         end",
    )
}

/// The whole calendar months from `start` to `end`, rounding toward zero. A month is complete once
/// the date has reached the start's day of the month, so from January 31 to February 29 no month
/// is complete, even though adding a month to January 31 lands on February 29.
fn months_between(start: Date, end: Date) -> i64 {
    let start_months = month_index(start);
    let end_months = month_index(end);
    let months = end_months
        .checked_sub(start_months)
        .assured("two jiff month indices differ inside i64");
    match end.cmp(&start) {
        Ordering::Greater if end.day() < start.day() => months
            .checked_sub(1)
            .assured("a positive month count less one fits i64"),
        Ordering::Less if end.day() > start.day() => months
            .checked_add(1)
            .assured("a negative month count plus one fits i64"),
        Ordering::Greater | Ordering::Less | Ordering::Equal => months,
    }
}

/// The months from the start of year zero to `date`'s month.
fn month_index(date: Date) -> i64 {
    i64::from(date.year())
        .checked_mul(12)
        .assured("a jiff year in months fits i64")
        .checked_add(i64::from(date.month()))
        .assured("a jiff year in months plus a month fits i64")
}

/// Computes `lane` for every operand whose lane is valid.
fn unary_lanes<I: Copy, N: Copy + Default>(
    operands: &[I],
    nulls: Option<&NullBuffer>,
    lane: impl FnMut(I) -> (N, bool),
) -> Lanes<N> {
    match nulls {
        Some(nulls) => Lanes::unary_valid(operands, nulls, lane),
        None => Lanes::unary(operands, lane),
    }
}

/// Computes `lane` for every pair of operands whose lane is valid.
fn binary_lanes<L: Copy, R: Copy, N: Copy + Default>(
    left: &[L],
    right: &[R],
    nulls: Option<&NullBuffer>,
    lane: impl FnMut(L, R) -> (N, bool),
) -> Lanes<N> {
    match nulls {
        Some(nulls) => Lanes::binary_valid(left, right, nulls, lane),
        None => Lanes::binary(left, right, lane),
    }
}

#[cfg(test)]
#[path = "calendar_tests.rs"]
mod tests;
