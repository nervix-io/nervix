//! Tests for the local calendar kernels.
//!
//! Layer: test harness.
//!
//! - **Owns.** Known local calendar results across daylight saving gaps and folds, historical local
//!   mean time, skipped days, month ends and leap years, the checks that define where a local unit
//!   starts, and differential checks of local date parts, calendar addition and calendar
//!   differences against jiff's zoned datetimes.
//! - **Depends on.** The datetime kernels, their zones, and jiff as the reference calendar.
//! - **Must not know.** Registers, programs, or how a failed lane is recorded as a row error.

use std::str::FromStr as _;

use arrow_array::{Array, Int64Array, TimestampNanosecondArray, UInt64Array};
use jiff::{Span, Timestamp, Unit, Zoned, civil::DateTime, tz::TimeZone};
use nervix_models::Timestamp as NervixTimestamp;

use super::local_unit_start;
use crate::{
    datetime::{UnitCounts, add, date_part, difference, truncate},
    numeric::Checked,
    program::{CalendarUnit, DatePart, DatetimeUnit, FixedTimeUnit, Zone},
};

const PARTS: [DatePart; 15] = [
    DatePart::Year,
    DatePart::Quarter,
    DatePart::Month,
    DatePart::Day,
    DatePart::Hour,
    DatePart::Minute,
    DatePart::Second,
    DatePart::Millisecond,
    DatePart::Microsecond,
    DatePart::Nanosecond,
    DatePart::DayOfWeek,
    DatePart::DayOfYear,
    DatePart::IsoYear,
    DatePart::IsoWeek,
    DatePart::IsoDayOfWeek,
];

const UNITS: [DatetimeUnit; 11] = [
    DatetimeUnit::Fixed(FixedTimeUnit::Nanosecond),
    DatetimeUnit::Fixed(FixedTimeUnit::Microsecond),
    DatetimeUnit::Fixed(FixedTimeUnit::Millisecond),
    DatetimeUnit::Fixed(FixedTimeUnit::Second),
    DatetimeUnit::Fixed(FixedTimeUnit::Minute),
    DatetimeUnit::Fixed(FixedTimeUnit::Hour),
    DatetimeUnit::Fixed(FixedTimeUnit::Day),
    DatetimeUnit::Fixed(FixedTimeUnit::Week),
    DatetimeUnit::Calendar(CalendarUnit::Month),
    DatetimeUnit::Calendar(CalendarUnit::Quarter),
    DatetimeUnit::Calendar(CalendarUnit::Year),
];

/// Zones whose rules exercise every kind of transition: summer time at 02:00 and at midnight,
/// half-hour and 45-minute offsets, a 30-minute summer time, a skipped day, and local mean time with
/// seconds.
const ZONES: [&str; 12] = [
    "America/New_York",
    "Europe/Berlin",
    "America/Sao_Paulo",
    "America/Havana",
    "America/Santiago",
    "Asia/Beirut",
    "Australia/Lord_Howe",
    "Pacific/Apia",
    "Asia/Kolkata",
    "Asia/Kathmandu",
    "Europe/Amsterdam",
    "+05:30",
];

fn nanoseconds(instant: &str) -> i64 {
    NervixTimestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

/// The same rules jiff reads, from the same bundled database.
fn reference_rules(written: &str) -> TimeZone {
    if written.starts_with(['+', '-']) {
        let sign = if written.starts_with('-') { -1 } else { 1 };
        let hours: i32 = written[1..3].parse().expect("hours");
        let minutes: i32 = written[4..6].parse().expect("minutes");
        let offset = jiff::tz::Offset::from_seconds(sign * (hours * 3_600 + minutes * 60))
            .expect("a written offset");
        return TimeZone::fixed(offset);
    }
    let (name, data) = jiff_tzdb::get(written).expect("a bundled zone");
    TimeZone::tzif(name, data).expect("bundled TZif data")
}

/// The local date and time `rules` show at `instant`.
///
/// Jiff reads a timestamp's seconds truncated toward zero when it looks an offset up, which before
/// the Unix epoch is the following second. Transitions happen at whole seconds, so the offset is
/// looked up for the whole second at or before the instant, which is exact.
fn reference_local(instant: i128, rules: &TimeZone) -> DateTime {
    let second = i64::try_from(instant.div_euclid(1_000_000_000)).expect("fits i64");
    let offset = rules.to_offset(Timestamp::from_second(second).expect("in jiff's range"));
    offset.to_datetime(Timestamp::from_nanosecond(instant).expect("in jiff's range"))
}

/// A jiff zoned datetime at `instant`, for jiff's calendar arithmetic. Its offset is exact only for
/// an instant that is a whole second or not before the epoch, which is all this is used for.
fn reference_zoned(instant: i64, rules: &TimeZone) -> Zoned {
    assert!(
        instant >= 0 || instant % 1_000_000_000 == 0,
        "jiff's zoned offsets are exact for whole seconds or instants after the epoch"
    );
    Timestamp::from_nanosecond(i128::from(instant))
        .expect("in jiff's range")
        .to_zoned(rules.clone())
}

/// The instants of `instants` at which a jiff zoned datetime has its exact offset.
fn whole_or_after_epoch(instants: Vec<i64>) -> Vec<i64> {
    instants
        .into_iter()
        .filter(|instant| *instant >= 0 || instant % 1_000_000_000 == 0)
        .collect()
}

fn datetimes(values: &[i64]) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(values.to_vec()).with_timezone_utc()
}

fn checked_lanes<T: arrow_array::ArrowPrimitiveType>(
    checked: &Checked<T>,
) -> Vec<(Option<T::Native>, bool)> {
    let failed = checked.failed.lanes().collect::<Vec<_>>();
    (0..checked.column.len())
        .map(|lane| {
            let value = (!checked.column.is_null(lane)).then(|| checked.column.value(lane));
            (value, failed.contains(&lane))
        })
        .collect()
}

/// Instants around every transition of `written` from 1850 to 2040, at the transition, one
/// nanosecond before it, and hours, a day and a month to either side, together with instants at
/// both ends of the DATETIME range.
fn instants_around_transitions(written: &str) -> Vec<i64> {
    let rules = reference_rules(written);
    let start = Timestamp::from_nanosecond(i128::from(nanoseconds("1850-01-01T00:00:00Z")))
        .expect("in range");
    let end = Timestamp::from_nanosecond(i128::from(nanoseconds("2040-01-01T00:00:00Z")))
        .expect("in range");
    let hour = 3_600_000_000_000_i64;
    let mut instants = vec![
        i64::MIN,
        i64::MIN + 1,
        nanoseconds("1677-09-21T12:00:00Z"),
        nanoseconds("1900-02-28T23:30:00Z"),
        nanoseconds("2000-02-29T23:30:00Z"),
        nanoseconds("2262-04-10T12:00:00Z"),
        i64::MAX,
    ];
    for transition in rules.following(start) {
        if transition.timestamp() > end {
            break;
        }
        let at = i64::try_from(transition.timestamp().as_nanosecond()).expect("fits i64");
        for delta in [
            -31 * 24 * hour,
            -24 * hour,
            -3 * hour,
            -hour - 1,
            -hour / 2,
            -1,
            0,
            1,
            hour / 2,
            hour + 7,
            3 * hour,
            24 * hour + 13,
        ] {
            instants.push(at + delta);
        }
    }
    instants
}

impl DatePart {
    fn reference(self, local: DateTime) -> i64 {
        match self {
            Self::Year => i64::from(local.year()),
            Self::Quarter => i64::from((local.month() + 2) / 3),
            Self::Month => i64::from(local.month()),
            Self::Day => i64::from(local.day()),
            Self::Hour => i64::from(local.hour()),
            Self::Minute => i64::from(local.minute()),
            Self::Second => i64::from(local.second()),
            Self::Millisecond => i64::from(local.millisecond()),
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

#[test]
fn local_date_parts_match_jiff_around_every_transition() {
    for written in ZONES {
        let rules = reference_rules(written);
        let instants = instants_around_transitions(written);
        let values = datetimes(&instants);
        for part in PARTS {
            let extracted = date_part(&values, part, &zone(written));
            for (lane, instant) in instants.iter().enumerate() {
                let expected = part.reference(reference_local(i128::from(*instant), &rules));
                assert_eq!(
                    extracted.value(lane),
                    expected,
                    "{part} of {instant} in {written}"
                );
            }
        }
    }
}

#[test]
fn local_date_parts_at_a_zero_offset_match_the_utc_kernel() {
    let instants = instants_around_transitions("Europe/Berlin");
    let values = datetimes(&instants);
    for part in PARTS {
        let utc = date_part(&values, part, &Zone::UTC);
        let zero_offset = date_part(&values, part, &zone("+00:00"));
        assert_eq!(utc, zero_offset, "{part}");
    }
}

/// Whether the zone's clock at `instant` showed a time in the unit that starts at `unit_start`.
fn clock_stays_in_unit(
    rules: &TimeZone,
    instant: i128,
    unit: DatetimeUnit,
    unit_start: DateTime,
) -> bool {
    local_unit_start(reference_local(instant, rules), unit) == unit_start
}

#[test]
fn truncation_starts_every_lane_at_the_first_instant_of_its_local_unit() {
    for written in ZONES {
        let rules = reference_rules(written);
        let resolved = zone(written);
        let instants = instants_around_transitions(written);
        let values = datetimes(&instants);
        for unit in UNITS {
            let truncated = checked_lanes(&truncate(&values, unit, &resolved));
            for (lane, instant) in instants.iter().enumerate() {
                let local = reference_local(i128::from(*instant), &rules);
                let unit_start = local_unit_start(local, unit);
                let (start, failed) = truncated[lane];
                let Some(start) = start else {
                    assert!(
                        failed,
                        "{unit} of {instant} in {written} fails when it has no start"
                    );
                    let first = reference_local(i128::from(i64::MIN), &rules);
                    assert_eq!(
                        local_unit_start(first, unit),
                        unit_start,
                        "only a unit that holds the first DATETIME can start before the range"
                    );
                    continue;
                };
                let start = i128::from(start);
                let instant = i128::from(*instant);
                assert!(
                    start <= instant,
                    "{unit} of {instant} in {written} starts after it"
                );
                assert!(
                    clock_stays_in_unit(&rules, start, unit, unit_start),
                    "{unit} of {instant} in {written} starts outside its unit"
                );
                assert!(
                    !clock_stays_in_unit(&rules, start - 1, unit, unit_start),
                    "{unit} of {instant} in {written} starts after the first instant of its unit"
                );
                let start_second =
                    i64::try_from(start.div_euclid(1_000_000_000)).expect("fits i64");
                let start_second = Timestamp::from_second(start_second).expect("in range");
                for transition in rules.following(start_second) {
                    let at = transition.timestamp().as_nanosecond();
                    if at > instant {
                        break;
                    }
                    if at <= start {
                        continue;
                    }
                    assert!(
                        clock_stays_in_unit(&rules, at, unit, unit_start)
                            && clock_stays_in_unit(&rules, at - 1, unit, unit_start),
                        "{unit} of {instant} in {written} leaves its unit at the transition {at}"
                    );
                }
            }
        }
    }
}

#[test]
fn truncation_follows_local_calendars_across_transitions() {
    let cases = [
        // New York skips 02:00 to 03:00 and repeats 01:00 to 02:00.
        (
            "2024-03-10T11:00:00.25Z",
            "America/New_York",
            "hour",
            "2024-03-10T11:00:00Z",
        ),
        (
            "2024-03-10T11:00:00.25Z",
            "America/New_York",
            "day",
            "2024-03-10T05:00:00Z",
        ),
        (
            "2024-11-03T06:30:00Z",
            "America/New_York",
            "day",
            "2024-11-03T04:00:00Z",
        ),
        // Both passes through the repeated hour belong to the hour that started with the first.
        (
            "2024-11-03T05:30:00Z",
            "America/New_York",
            "hour",
            "2024-11-03T05:00:00Z",
        ),
        (
            "2024-11-03T06:30:00Z",
            "America/New_York",
            "hour",
            "2024-11-03T05:00:00Z",
        ),
        (
            "2024-11-03T07:30:00Z",
            "America/New_York",
            "hour",
            "2024-11-03T07:00:00Z",
        ),
        // New York left local mean time at 12:03:58 LMT, back to 12:00:00 EST.
        (
            "1883-11-18T17:01:00Z",
            "America/New_York",
            "hour",
            "1883-11-18T16:56:02Z",
        ),
        (
            "1883-11-18T17:01:00Z",
            "America/New_York",
            "day",
            "1883-11-18T04:56:02Z",
        ),
        (
            "1883-11-18T17:01:00Z",
            "Europe/Berlin",
            "month",
            "1883-10-31T23:06:32Z",
        ),
        // Local months in Berlin start before the UTC ones.
        (
            "2024-01-31T23:30:00Z",
            "Europe/Berlin",
            "month",
            "2024-01-31T23:00:00Z",
        ),
        (
            "2024-03-10T11:00:00Z",
            "Europe/Berlin",
            "quarter",
            "2023-12-31T23:00:00Z",
        ),
        (
            "2024-03-10T11:00:00Z",
            "Europe/Berlin",
            "year",
            "2023-12-31T23:00:00Z",
        ),
        (
            "2000-02-29T12:00:00Z",
            "Pacific/Auckland",
            "month",
            "2000-02-29T11:00:00Z",
        ),
        // Sao Paulo skipped midnight when summer time began on 2018-11-04.
        (
            "2018-11-04T12:00:00Z",
            "America/Sao_Paulo",
            "day",
            "2018-11-04T03:00:00Z",
        ),
        // Samoa skipped 2011-12-30 entirely.
        (
            "2011-12-30T22:00:00Z",
            "Pacific/Apia",
            "day",
            "2011-12-30T10:00:00Z",
        ),
        (
            "2011-12-30T22:00:00Z",
            "Pacific/Apia",
            "week",
            "2011-12-26T10:00:00Z",
        ),
        // India is 5:30 ahead, so its hours start at half past in UTC.
        (
            "2024-07-04T12:10:00Z",
            "Asia/Kolkata",
            "hour",
            "2024-07-04T11:30:00Z",
        ),
        (
            "2024-07-04T12:10:00Z",
            "+05:30",
            "day",
            "2024-07-03T18:30:00Z",
        ),
        (
            "2024-07-04T12:10:00Z",
            "UTC",
            "quarter",
            "2024-07-01T00:00:00Z",
        ),
        (
            "2024-02-10T12:00:00Z",
            "UTC",
            "month",
            "2024-02-01T00:00:00Z",
        ),
    ];
    for (instant, written, unit, expected) in cases {
        let unit = DatetimeUnit::from_str(unit).expect("a unit");
        let truncated = truncate(&datetimes(&[nanoseconds(instant)]), unit, &zone(written));
        assert_eq!(
            checked_lanes(&truncated),
            [(Some(nanoseconds(expected)), false)],
            "{unit} of {instant} in {written}"
        );
    }
}

#[test]
fn truncation_before_the_range_fails_only_its_lane() {
    let values = TimestampNanosecondArray::from(vec![
        Some(nanoseconds("1677-09-21T00:12:43.145224192Z")),
        None,
        Some(nanoseconds("2024-05-05T05:05:05Z")),
        Some(nanoseconds("1677-12-31T23:59:59Z")),
    ])
    .with_timezone_utc();
    let months = truncate(
        &values,
        DatetimeUnit::Calendar(CalendarUnit::Month),
        &Zone::UTC,
    );
    assert_eq!(
        checked_lanes(&months),
        [
            (None, true),
            (None, false),
            (Some(nanoseconds("2024-05-01T00:00:00Z")), false),
            (Some(nanoseconds("1677-12-01T00:00:00Z")), false),
        ]
    );
    // Tokyo kept local mean time, 9:18:59 ahead of UTC, so its 1678 starts in 1677 in UTC.
    let years = truncate(
        &values,
        DatetimeUnit::Calendar(CalendarUnit::Year),
        &zone("Asia/Tokyo"),
    );
    assert_eq!(checked_lanes(&years)[0], (None, true));
    assert_eq!(
        checked_lanes(&years)[3],
        (Some(nanoseconds("1677-12-31T14:41:01Z")), false)
    );
}

fn reference_span(unit: DatetimeUnit, amount: i64) -> Span {
    match unit {
        DatetimeUnit::Fixed(FixedTimeUnit::Day) => Span::new().days(amount),
        DatetimeUnit::Fixed(FixedTimeUnit::Week) => Span::new().weeks(amount),
        DatetimeUnit::Calendar(CalendarUnit::Month) => Span::new().months(amount),
        DatetimeUnit::Calendar(CalendarUnit::Quarter) => Span::new().months(amount * 3),
        DatetimeUnit::Calendar(CalendarUnit::Year) => Span::new().years(amount),
        DatetimeUnit::Fixed(unit) => Span::new().nanoseconds(amount * unit.nanoseconds()),
    }
}

#[test]
fn calendar_addition_matches_jiff_around_every_transition() {
    let amounts = [-25_i64, -13, -12, -3, -1, 0, 1, 2, 3, 11, 12, 13, 400];
    let units = [
        DatetimeUnit::Fixed(FixedTimeUnit::Hour),
        DatetimeUnit::Fixed(FixedTimeUnit::Day),
        DatetimeUnit::Fixed(FixedTimeUnit::Week),
        DatetimeUnit::Calendar(CalendarUnit::Month),
        DatetimeUnit::Calendar(CalendarUnit::Quarter),
        DatetimeUnit::Calendar(CalendarUnit::Year),
    ];
    for written in ZONES {
        let rules = reference_rules(written);
        let resolved = zone(written);
        let instants = whole_or_after_epoch(instants_around_transitions(written));
        let values = datetimes(&instants);
        for unit in units {
            for amount in amounts {
                let counts = Int64Array::from(vec![amount; instants.len()]);
                let added =
                    checked_lanes(&add(&UnitCounts::Int64(&counts), &values, unit, &resolved));
                for (lane, instant) in instants.iter().enumerate() {
                    let moved =
                        reference_zoned(*instant, &rules).checked_add(reference_span(unit, amount));
                    let expected = match moved {
                        Ok(moved) => i64::try_from(moved.timestamp().as_nanosecond()).ok(),
                        Err(_) => None,
                    };
                    let expected = match expected {
                        Some(value) => (Some(value), false),
                        None => (None, true),
                    };
                    assert_eq!(
                        added[lane], expected,
                        "{amount} {unit} from {instant} in {written}"
                    );
                }
            }
        }
    }
}

#[test]
fn calendar_addition_keeps_the_day_of_the_month_unless_the_month_is_shorter() {
    let cases = [
        (
            "2024-01-31T23:30:00Z",
            "month",
            1,
            "UTC",
            "2024-02-29T23:30:00Z",
        ),
        (
            "2023-01-31T12:00:00Z",
            "month",
            1,
            "UTC",
            "2023-02-28T12:00:00Z",
        ),
        (
            "2024-01-31T12:00:00Z",
            "month",
            2,
            "UTC",
            "2024-03-31T12:00:00Z",
        ),
        (
            "2024-03-31T12:00:00Z",
            "month",
            -1,
            "UTC",
            "2024-02-29T12:00:00Z",
        ),
        (
            "2024-02-29T12:00:00Z",
            "year",
            1,
            "UTC",
            "2025-02-28T12:00:00Z",
        ),
        (
            "2024-02-29T12:00:00Z",
            "year",
            4,
            "UTC",
            "2028-02-29T12:00:00Z",
        ),
        (
            "1896-02-29T12:00:00Z",
            "year",
            4,
            "UTC",
            "1900-02-28T12:00:00Z",
        ),
        (
            "2024-11-30T12:00:00Z",
            "quarter",
            1,
            "UTC",
            "2025-02-28T12:00:00Z",
        ),
        (
            "2024-03-10T11:00:00.25Z",
            "month",
            -1,
            "UTC",
            "2024-02-10T11:00:00.25Z",
        ),
        // A local day across a transition keeps the local time of day.
        (
            "2024-03-09T12:00:00Z",
            "day",
            1,
            "America/New_York",
            "2024-03-10T11:00:00Z",
        ),
        (
            "2024-11-02T12:00:00Z",
            "day",
            1,
            "America/New_York",
            "2024-11-03T13:00:00Z",
        ),
        // 02:30 does not exist on 2024-03-10 in New York, so the moved time lands an hour later.
        (
            "2024-03-09T07:30:00Z",
            "day",
            1,
            "America/New_York",
            "2024-03-10T07:30:00Z",
        ),
        // 01:30 happens twice on 2024-11-03 in New York, and the earlier one is chosen.
        (
            "2024-11-02T05:30:00Z",
            "day",
            1,
            "America/New_York",
            "2024-11-03T05:30:00Z",
        ),
        (
            "2024-01-31T23:30:00Z",
            "month",
            1,
            "Europe/Berlin",
            "2024-02-29T23:30:00Z",
        ),
    ];
    for (instant, unit, amount, written, expected) in cases {
        let unit = DatetimeUnit::from_str(unit).expect("a unit");
        let counts = Int64Array::from(vec![amount]);
        let added = add(
            &UnitCounts::Int64(&counts),
            &datetimes(&[nanoseconds(instant)]),
            unit,
            &zone(written),
        );
        assert_eq!(
            checked_lanes(&added),
            [(Some(nanoseconds(expected)), false)],
            "{amount} {unit} from {instant} in {written}"
        );
    }
}

#[test]
fn calendar_addition_outside_the_range_fails_only_its_lane() {
    let values = TimestampNanosecondArray::from(vec![
        Some(nanoseconds("2262-03-15T00:00:00Z")),
        Some(nanoseconds("2000-01-01T00:00:00Z")),
        None,
        Some(nanoseconds("1677-10-15T00:00:00Z")),
        Some(nanoseconds("2000-01-01T00:00:00Z")),
    ])
    .with_timezone_utc();
    let amounts = UInt64Array::from(vec![Some(1), Some(u64::MAX), Some(1), Some(0), Some(3)]);
    let months = add(
        &UnitCounts::UInt64(&amounts),
        &values,
        DatetimeUnit::Calendar(CalendarUnit::Month),
        &zone("Europe/Berlin"),
    );
    assert_eq!(
        checked_lanes(&months),
        [
            (None, true),
            (None, true),
            (None, false),
            (Some(nanoseconds("1677-10-15T00:00:00Z")), false),
            (Some(nanoseconds("2000-03-31T23:00:00Z")), false),
        ]
    );
    let signed = Int64Array::from(vec![Some(-1), Some(i64::MIN), Some(0), Some(-1), None]);
    let years = add(
        &UnitCounts::Int64(&signed),
        &values,
        DatetimeUnit::Calendar(CalendarUnit::Year),
        &Zone::UTC,
    );
    assert_eq!(
        checked_lanes(&years),
        [
            (Some(nanoseconds("2261-03-15T00:00:00Z")), false),
            (None, true),
            (None, false),
            (None, true),
            (None, false),
        ]
    );
}

fn reference_count(unit: DatetimeUnit, start: &Zoned, end: &Zoned) -> i64 {
    let largest = match unit {
        DatetimeUnit::Fixed(FixedTimeUnit::Day) => Unit::Day,
        DatetimeUnit::Fixed(FixedTimeUnit::Week) => Unit::Week,
        DatetimeUnit::Calendar(CalendarUnit::Month | CalendarUnit::Quarter) => Unit::Month,
        DatetimeUnit::Calendar(CalendarUnit::Year) => Unit::Year,
        DatetimeUnit::Fixed(_) => unreachable!("only calendar counts have a jiff reference here"),
    };
    let span = start
        .until((largest, end))
        .expect("jiff counts between two instants in range");
    match unit {
        DatetimeUnit::Fixed(FixedTimeUnit::Day) => i64::from(span.get_days()),
        DatetimeUnit::Fixed(FixedTimeUnit::Week) => i64::from(span.get_weeks()),
        DatetimeUnit::Calendar(CalendarUnit::Month) => i64::from(span.get_months()),
        DatetimeUnit::Calendar(CalendarUnit::Quarter) => i64::from(span.get_months()) / 3,
        DatetimeUnit::Calendar(CalendarUnit::Year) => i64::from(span.get_years()),
        DatetimeUnit::Fixed(_) => unreachable!("only calendar counts have a jiff reference here"),
    }
}

#[test]
fn calendar_differences_match_jiff_around_every_transition() {
    let units = [
        DatetimeUnit::Fixed(FixedTimeUnit::Day),
        DatetimeUnit::Fixed(FixedTimeUnit::Week),
        DatetimeUnit::Calendar(CalendarUnit::Month),
        DatetimeUnit::Calendar(CalendarUnit::Quarter),
        DatetimeUnit::Calendar(CalendarUnit::Year),
    ];
    let hour = 3_600_000_000_000_i64;
    for written in [
        "America/New_York",
        "Europe/Berlin",
        "Pacific/Apia",
        "America/Havana",
        "UTC",
    ] {
        let rules = reference_rules(written);
        let resolved = zone(written);
        let anchors = whole_or_after_epoch(instants_around_transitions(written));
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for anchor in anchors.iter().filter(|anchor| {
            **anchor > nanoseconds("1700-01-01T00:00:00Z")
                && **anchor < nanoseconds("2200-01-01T00:00:00Z")
        }) {
            for delta in [
                -800 * 24 * hour,
                -45 * 24 * hour - 3 * hour,
                -24 * hour - 1_000_000_000,
                -23 * hour,
                -hour,
                hour / 2,
                23 * hour,
                24 * hour,
                25 * hour + 1,
                31 * 24 * hour + 2 * hour,
                366 * 24 * hour,
            ] {
                let end = anchor + delta;
                if end < 0 && end % 1_000_000_000 != 0 {
                    continue;
                }
                starts.push(*anchor);
                ends.push(end);
            }
        }
        let start_values = datetimes(&starts);
        let end_values = datetimes(&ends);
        for unit in units {
            let counted = checked_lanes(&difference(&start_values, &end_values, unit, &resolved));
            for (lane, (start, end)) in starts.iter().zip(&ends).enumerate() {
                let start_zoned = reference_zoned(*start, &rules);
                let end_zoned = reference_zoned(*end, &rules);
                let expected = if start_zoned.date() == end_zoned.date() {
                    0
                } else {
                    reference_count(unit, &start_zoned, &end_zoned)
                };
                assert_eq!(
                    counted[lane],
                    (Some(expected), false),
                    "{unit} from {start} to {end} in {written}"
                );
            }
        }
    }
}

#[test]
fn calendar_differences_count_whole_local_days_and_months() {
    let cases = [
        (
            "2023-12-31T23:30:00Z",
            "2024-01-31T23:30:00Z",
            "month",
            "UTC",
            1,
        ),
        (
            "2024-01-31T00:00:00Z",
            "2024-02-29T00:00:00Z",
            "month",
            "UTC",
            0,
        ),
        (
            "2024-02-29T00:00:00Z",
            "2024-01-31T00:00:00Z",
            "month",
            "UTC",
            0,
        ),
        (
            "2024-01-31T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "month",
            "UTC",
            1,
        ),
        (
            "2024-03-01T00:00:00Z",
            "2024-01-31T00:00:00Z",
            "month",
            "UTC",
            -1,
        ),
        (
            "2020-02-29T12:00:00Z",
            "2021-02-28T12:00:00Z",
            "year",
            "UTC",
            0,
        ),
        (
            "2020-02-29T12:00:00Z",
            "2021-03-01T12:00:00Z",
            "year",
            "UTC",
            1,
        ),
        (
            "2024-01-15T00:00:00Z",
            "2024-12-14T00:00:00Z",
            "quarter",
            "UTC",
            3,
        ),
        // 23 hours across the spring transition are a whole local day.
        (
            "2024-03-09T12:00:00Z",
            "2024-03-10T11:00:00Z",
            "day",
            "America/New_York",
            1,
        ),
        (
            "2024-03-09T12:00:00Z",
            "2024-03-10T11:00:00Z",
            "day",
            "UTC",
            0,
        ),
        // 25 hours across the autumn transition are one local day.
        (
            "2024-11-02T05:30:00Z",
            "2024-11-03T06:30:00Z",
            "day",
            "America/New_York",
            1,
        ),
        // The local clock advanced only 23:56:02 when New York left local mean time.
        (
            "1883-11-17T17:01:00Z",
            "1883-11-18T17:01:00Z",
            "day",
            "America/New_York",
            0,
        ),
        // Forty minutes that cross the repeated hour stay on one local date.
        (
            "2024-11-03T05:30:00Z",
            "2024-11-03T06:10:00Z",
            "day",
            "America/New_York",
            0,
        ),
        (
            "2024-01-01T00:00:00Z",
            "2024-01-15T00:00:00Z",
            "week",
            "Europe/Berlin",
            2,
        ),
        (
            "2024-01-15T00:00:00Z",
            "2024-01-01T00:00:01Z",
            "week",
            "Europe/Berlin",
            -1,
        ),
    ];
    for (start, end, unit, written, expected) in cases {
        let unit = DatetimeUnit::from_str(unit).expect("a unit");
        let counted = difference(
            &datetimes(&[nanoseconds(start)]),
            &datetimes(&[nanoseconds(end)]),
            unit,
            &zone(written),
        );
        assert_eq!(
            checked_lanes(&counted),
            [(Some(expected), false)],
            "{unit} from {start} to {end} in {written}"
        );
    }
}

#[test]
fn calendar_kernels_skip_null_lanes_and_keep_sliced_offsets() {
    let values = TimestampNanosecondArray::from(vec![
        Some(nanoseconds("2024-01-31T23:30:00Z")),
        None,
        Some(nanoseconds("2024-03-10T11:00:00Z")),
        Some(nanoseconds("2024-11-03T06:30:00Z")),
    ])
    .with_timezone_utc();
    let sliced = values.slice(1, 3);
    let new_york = zone("America/New_York");

    let hours = date_part(&sliced, DatePart::Hour, &new_york);
    assert!(hours.is_null(0));
    assert_eq!(hours.value(1), 7);
    assert_eq!(hours.value(2), 1);

    let days = truncate(&sliced, DatetimeUnit::Fixed(FixedTimeUnit::Day), &new_york);
    assert_eq!(
        checked_lanes(&days),
        [
            (None, false),
            (Some(nanoseconds("2024-03-10T05:00:00Z")), false),
            (Some(nanoseconds("2024-11-03T04:00:00Z")), false),
        ]
    );

    let amounts = Int64Array::from(vec![Some(1), Some(1), None, Some(1)]).slice(1, 3);
    let moved = add(
        &UnitCounts::Int64(&amounts),
        &sliced,
        DatetimeUnit::Fixed(FixedTimeUnit::Day),
        &new_york,
    );
    assert_eq!(
        checked_lanes(&moved),
        [
            (None, false),
            (None, false),
            (Some(nanoseconds("2024-11-04T06:30:00Z")), false),
        ]
    );
    assert_eq!(moved.column.timezone(), Some("+00:00"));
}
