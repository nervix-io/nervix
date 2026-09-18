//! Tests for the datetime kernels.
//!
//! Layer: test harness.
//!
//! - **Owns.** Known results of every datetime kernel at the Unix epoch, before it, on leap days,
//!   at ISO week boundaries and at both ends of the DATETIME range, and differential checks of
//!   every kernel against a scalar model computed in `i128` over boundary operands.
//! - **Depends on.** The datetime kernels, their resolved vocabulary, and chrono as the reference
//!   calendar.
//! - **Must not know.** Registers, programs, or how a failed lane is recorded as a row error.

use std::{num::NonZeroU64, str::FromStr as _};

use arrow_array::{
    Array, Int8Array, Int64Array, PrimitiveArray, TimestampNanosecondArray, UInt64Array,
    types::TimestampNanosecondType,
};
use chrono::{DateTime, Datelike as _, Timelike as _};
use nervix_models::Timestamp;

use super::{UnitCounts, add, bin, date_part, difference, from_unix, to_unix, truncate};
use crate::{
    numeric::Checked,
    program::{DateBinWidth, DatePart, DatetimeUnit, FixedTimeUnit, Zone},
};

const UNITS: [FixedTimeUnit; 8] = [
    FixedTimeUnit::Nanosecond,
    FixedTimeUnit::Microsecond,
    FixedTimeUnit::Millisecond,
    FixedTimeUnit::Second,
    FixedTimeUnit::Minute,
    FixedTimeUnit::Hour,
    FixedTimeUnit::Day,
    FixedTimeUnit::Week,
];

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

/// Instants at every boundary a datetime kernel treats specially: both ends of the range, the
/// epoch and the nanoseconds around it, day and week starts, leap days in leap and non-leap
/// centuries, and ISO weeks that belong to a neighbouring year.
const BOUNDARY_INSTANTS: [&str; 22] = [
    "1677-09-21T00:12:43.145224192Z",
    "1677-09-21T00:12:43.145224193Z",
    "1677-09-21T00:13:00Z",
    "1900-02-28T23:59:59.999999999Z",
    "1900-03-01T00:00:00Z",
    "1968-02-29T12:00:00Z",
    "1969-12-29T00:00:00Z",
    "1969-12-31T23:59:59.999999999Z",
    "1970-01-01T00:00:00Z",
    "1970-01-01T00:00:00.000000001Z",
    "1970-01-04T23:59:59.999999999Z",
    "1970-01-05T00:00:00Z",
    "2000-02-29T00:00:00Z",
    "2000-02-29T23:52:30.250Z",
    "2000-03-01T00:00:00Z",
    "2021-01-03T23:59:59Z",
    "2024-12-30T00:00:00Z",
    "2100-02-28T23:59:59.999999999Z",
    "2100-03-01T00:00:00Z",
    "2262-04-11T00:00:00Z",
    "2262-04-11T23:47:16.854775806Z",
    "2262-04-11T23:47:16.854775807Z",
];

fn nanoseconds(instant: &str) -> i64 {
    Timestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn boundary_nanoseconds() -> Vec<i64> {
    BOUNDARY_INSTANTS
        .iter()
        .map(|instant| nanoseconds(instant))
        .collect()
}

fn datetimes(values: &[i64]) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(values.to_vec()).with_timezone_utc()
}

/// The value of every lane of a checked column, `None` for a null lane, and whether each lane
/// failed.
fn checked_lanes<T>(checked: &Checked<T>) -> Vec<(Option<T::Native>, bool)>
where
    T: arrow_array::ArrowPrimitiveType,
{
    let failed = checked.failed.lanes().collect::<Vec<_>>();
    (0..checked.column.len())
        .map(|lane| {
            let value = (!checked.column.is_null(lane)).then(|| checked.column.value(lane));
            (value, failed.contains(&lane))
        })
        .collect()
}

/// The lane a scalar model expects: its value, or a failed null lane when the model has no value.
fn expected_lane(value: Option<i64>) -> (Option<i64>, bool) {
    match value {
        Some(value) => (Some(value), false),
        None => (None, true),
    }
}

fn fits_i64(value: i128) -> Option<i64> {
    i64::try_from(value).ok()
}

fn unit_nanoseconds(unit: FixedTimeUnit) -> i128 {
    i128::from(unit.nanoseconds())
}

fn model_date_part(value: i64, part: DatePart) -> i64 {
    let instant = DateTime::from_timestamp_nanos(value);
    match part {
        DatePart::Year => i64::from(instant.year()),
        DatePart::Quarter => i64::from(instant.quarter()),
        DatePart::Month => i64::from(instant.month()),
        DatePart::Day => i64::from(instant.day()),
        DatePart::Hour => i64::from(instant.hour()),
        DatePart::Minute => i64::from(instant.minute()),
        DatePart::Second => i64::from(instant.second()),
        DatePart::Millisecond => i64::from(instant.nanosecond() / 1_000_000),
        DatePart::Microsecond => i64::from(instant.nanosecond() / 1_000),
        DatePart::Nanosecond => i64::from(instant.nanosecond()),
        DatePart::DayOfWeek => i64::from(instant.weekday().num_days_from_sunday()),
        DatePart::DayOfYear => i64::from(instant.ordinal()),
        DatePart::IsoYear => i64::from(instant.iso_week().year()),
        DatePart::IsoWeek => i64::from(instant.iso_week().week()),
        DatePart::IsoDayOfWeek => i64::from(instant.weekday().number_from_monday()),
    }
}

/// The start of the bin holding `value` for bins `stride` wide aligned to `origin`, by floor
/// division of the exact distance from the origin.
fn model_bin_start(value: i64, origin: i128, stride: i128) -> Option<i64> {
    let distance = i128::from(value) - origin;
    fits_i64(origin + distance.div_euclid(stride) * stride)
}

fn model_truncate(value: i64, unit: FixedTimeUnit) -> Option<i64> {
    let origin = match unit {
        FixedTimeUnit::Week => i128::from(nanoseconds("1970-01-05T00:00:00Z")),
        _ => 0,
    };
    model_bin_start(value, origin, unit_nanoseconds(unit))
}

fn width(count: u64, unit: FixedTimeUnit) -> DateBinWidth {
    DateBinWidth::new(
        NonZeroU64::new(count).expect("test widths are positive"),
        unit,
    )
    .expect("test widths fit the DATETIME range")
}

#[test]
fn extracts_every_date_part_of_a_leap_day() {
    let values = datetimes(&[nanoseconds("2000-02-29T23:59:59.999999999Z")]);
    let expected = [
        (DatePart::Year, 2000),
        (DatePart::Quarter, 1),
        (DatePart::Month, 2),
        (DatePart::Day, 29),
        (DatePart::Hour, 23),
        (DatePart::Minute, 59),
        (DatePart::Second, 59),
        (DatePart::Millisecond, 999),
        (DatePart::Microsecond, 999_999),
        (DatePart::Nanosecond, 999_999_999),
        (DatePart::DayOfWeek, 2),
        (DatePart::DayOfYear, 60),
        (DatePart::IsoYear, 2000),
        (DatePart::IsoWeek, 9),
        (DatePart::IsoDayOfWeek, 2),
    ];
    for (part, value) in expected {
        assert_eq!(
            date_part(&values, part, &Zone::UTC).value(0),
            value,
            "{part}"
        );
    }
}

#[test]
fn extracts_date_parts_before_the_epoch_and_across_iso_years() {
    let values = datetimes(&[
        nanoseconds("1969-12-31T23:59:59.999999999Z"),
        nanoseconds("2021-01-03T12:00:00Z"),
        nanoseconds("1900-03-01T00:00:00Z"),
        nanoseconds("2000-12-31T00:00:00Z"),
        nanoseconds("1677-09-21T00:12:43.145224192Z"),
        nanoseconds("2262-04-11T23:47:16.854775807Z"),
    ]);
    let expected = [
        (DatePart::Year, [1969, 2021, 1900, 2000, 1677, 2262]),
        (
            DatePart::Nanosecond,
            [999_999_999, 0, 0, 0, 145_224_192, 854_775_807],
        ),
        (DatePart::DayOfWeek, [3, 0, 4, 0, 2, 5]),
        (DatePart::DayOfYear, [365, 3, 60, 366, 264, 101]),
        (DatePart::IsoYear, [1970, 2020, 1900, 2000, 1677, 2262]),
        (DatePart::IsoWeek, [1, 53, 9, 52, 38, 15]),
        (DatePart::IsoDayOfWeek, [3, 7, 4, 7, 2, 5]),
    ];
    for (part, parts) in expected {
        let extracted = date_part(&values, part, &Zone::UTC);
        assert_eq!(extracted.values().as_ref(), parts.as_slice(), "{part}");
    }
}

#[test]
fn date_parts_match_the_calendar_at_every_boundary_and_keep_nulls() {
    let boundaries = boundary_nanoseconds();
    let mut with_nulls = boundaries.iter().copied().map(Some).collect::<Vec<_>>();
    with_nulls[2] = None;
    with_nulls[9] = None;
    let values = TimestampNanosecondArray::from(with_nulls.clone()).with_timezone_utc();
    for part in PARTS {
        let extracted = date_part(&values, part, &Zone::UTC);
        for (lane, value) in with_nulls.iter().enumerate() {
            let expected = value.map(|value| model_date_part(value, part));
            let actual = (!extracted.is_null(lane)).then(|| extracted.value(lane));
            assert_eq!(actual, expected, "{part} of lane {lane}");
        }
    }
}

#[test]
fn truncates_days_and_weeks_before_and_after_the_epoch() {
    let values = datetimes(&[
        nanoseconds("1969-12-31T23:59:59.999999999Z"),
        nanoseconds("1970-01-04T23:59:59.999999999Z"),
        nanoseconds("1970-01-05T00:00:00Z"),
        nanoseconds("2000-02-29T23:52:30.250Z"),
    ]);

    let days = truncate(&values, DatetimeUnit::Fixed(FixedTimeUnit::Day), &Zone::UTC);
    assert_eq!(
        days.column.values().as_ref(),
        [
            nanoseconds("1969-12-31T00:00:00Z"),
            nanoseconds("1970-01-04T00:00:00Z"),
            nanoseconds("1970-01-05T00:00:00Z"),
            nanoseconds("2000-02-29T00:00:00Z"),
        ]
    );
    let weeks = truncate(
        &values,
        DatetimeUnit::Fixed(FixedTimeUnit::Week),
        &Zone::UTC,
    );
    assert_eq!(
        weeks.column.values().as_ref(),
        [
            nanoseconds("1969-12-29T00:00:00Z"),
            nanoseconds("1969-12-29T00:00:00Z"),
            nanoseconds("1970-01-05T00:00:00Z"),
            nanoseconds("2000-02-28T00:00:00Z"),
        ]
    );
    assert_eq!(days.failed.lanes().count(), 0);
    assert_eq!(weeks.failed.lanes().count(), 0);
    assert_eq!(days.column.timezone(), Some("+00:00"));
}

#[test]
fn truncation_fails_where_the_unit_starts_before_the_range() {
    let values = datetimes(&[nanoseconds("1677-09-21T00:12:43.145224192Z")]);

    assert_eq!(
        checked_lanes(&truncate(
            &values,
            DatetimeUnit::Fixed(FixedTimeUnit::Nanosecond),
            &Zone::UTC
        )),
        [(Some(i64::MIN), false)]
    );
    for unit in [
        FixedTimeUnit::Microsecond,
        FixedTimeUnit::Second,
        FixedTimeUnit::Week,
    ] {
        assert_eq!(
            checked_lanes(&truncate(&values, DatetimeUnit::Fixed(unit), &Zone::UTC)),
            [(None, true)],
            "{unit}"
        );
    }
}

#[test]
fn truncation_matches_floor_division_at_every_boundary() {
    let boundaries = boundary_nanoseconds();
    let values = datetimes(&boundaries);
    for unit in UNITS {
        let expected = boundaries
            .iter()
            .map(|value| expected_lane(model_truncate(*value, unit)))
            .collect::<Vec<_>>();
        assert_eq!(
            checked_lanes(&truncate(&values, DatetimeUnit::Fixed(unit), &Zone::UTC)),
            expected,
            "{unit}"
        );
    }
}

#[test]
fn bins_align_to_their_origin_on_both_sides_of_it() {
    let quarter_hour = width(15, FixedTimeUnit::Minute);
    let values = datetimes(&[
        nanoseconds("2000-02-29T23:52:30.250Z"),
        nanoseconds("2000-02-29T09:59:59.999999999Z"),
        nanoseconds("2000-02-29T10:05:00Z"),
        nanoseconds("1969-12-31T23:59:59.999999999Z"),
    ]);
    let origins = datetimes(&[
        nanoseconds("2000-02-29T00:05:00Z"),
        nanoseconds("2000-02-29T10:00:00Z"),
        nanoseconds("2000-02-29T10:05:00Z"),
        nanoseconds("1970-01-01T00:00:00Z"),
    ]);

    let binned = bin(&values, &origins, quarter_hour);

    assert_eq!(
        binned.column.values().as_ref(),
        [
            nanoseconds("2000-02-29T23:50:00Z"),
            nanoseconds("2000-02-29T09:45:00Z"),
            nanoseconds("2000-02-29T10:05:00Z"),
            nanoseconds("1969-12-31T23:45:00Z"),
        ]
    );
    assert_eq!(binned.failed.lanes().count(), 0);
}

#[test]
fn bins_match_floor_division_for_every_boundary_pair() {
    let boundaries = boundary_nanoseconds();
    let pairs = boundaries
        .iter()
        .flat_map(|value| boundaries.iter().map(move |origin| (*value, *origin)))
        .collect::<Vec<_>>();
    let values = datetimes(&pairs.iter().map(|pair| pair.0).collect::<Vec<_>>());
    let origins = datetimes(&pairs.iter().map(|pair| pair.1).collect::<Vec<_>>());
    let widest_weeks = u64::try_from(i64::MAX / FixedTimeUnit::Week.nanoseconds())
        .expect("a positive quotient fits u64");
    let widths = [
        width(1, FixedTimeUnit::Nanosecond),
        width(1, FixedTimeUnit::Second),
        width(15, FixedTimeUnit::Minute),
        width(1, FixedTimeUnit::Week),
        width(widest_weeks, FixedTimeUnit::Week),
        width(
            u64::try_from(i64::MAX).expect("i64::MAX fits u64"),
            FixedTimeUnit::Nanosecond,
        ),
    ];
    for width in widths {
        let stride = i128::from(width.nanoseconds());
        let expected = pairs
            .iter()
            .map(|(value, origin)| {
                expected_lane(model_bin_start(*value, i128::from(*origin), stride))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            checked_lanes(&bin(&values, &origins, width)),
            expected,
            "width of {stride} nanoseconds"
        );
    }
}

#[test]
fn bin_widths_are_bounded_by_the_signed_nanosecond_range() {
    let widest_weeks = u64::try_from(i64::MAX / FixedTimeUnit::Week.nanoseconds())
        .expect("a positive quotient fits u64");
    let widest = NonZeroU64::new(widest_weeks).expect("the widest week count is positive");
    let past_widest = NonZeroU64::new(widest_weeks + 1).expect("one past a count is positive");
    let past_nanoseconds = NonZeroU64::new(1_u64 << 63).expect("2^63 is positive");

    assert!(DateBinWidth::new(widest, FixedTimeUnit::Week).is_some());
    assert!(DateBinWidth::new(past_widest, FixedTimeUnit::Week).is_none());
    assert!(DateBinWidth::new(past_nanoseconds, FixedTimeUnit::Nanosecond).is_none());
    assert!(DateBinWidth::new(NonZeroU64::MAX, FixedTimeUnit::Week).is_none());
}

#[test]
fn addition_is_exact_across_the_whole_range() {
    let values = datetimes(&[
        nanoseconds("2000-02-29T23:52:30.250Z"),
        i64::MIN,
        nanoseconds("1678-01-01T00:00:00Z"),
        i64::MAX,
    ]);
    let amounts = UInt64Array::from(vec![450_000, u64::MAX, 9_300_000_000_000, 1]);

    let milliseconds = add(
        &UnitCounts::UInt64(&amounts),
        &values,
        DatetimeUnit::Fixed(FixedTimeUnit::Millisecond),
        &Zone::UTC,
    );

    // 9.3 * 10^18 nanoseconds do not fit `i64` on their own, but 1678 moved by them is in 1972.
    let past_offset_range =
        i128::from(nanoseconds("1678-01-01T00:00:00Z")) + 9_300_000_000_000_i128 * 1_000_000;
    assert_eq!(
        checked_lanes(&milliseconds),
        [
            (Some(nanoseconds("2000-03-01T00:00:00.250Z")), false),
            (None, true),
            (fits_i64(past_offset_range), false),
            (None, true),
        ]
    );

    let across_range = UInt64Array::from(vec![0, u64::MAX, 0, 0]);
    let nanoseconds_added = add(
        &UnitCounts::UInt64(&across_range),
        &values,
        DatetimeUnit::Fixed(FixedTimeUnit::Nanosecond),
        &Zone::UTC,
    );
    assert_eq!(
        checked_lanes(&nanoseconds_added)[1],
        (Some(i64::MAX), false)
    );
}

#[test]
fn addition_matches_the_exact_sum_for_every_boundary_and_amount() {
    let boundaries = boundary_nanoseconds();
    let signed_amounts = [i64::MIN, -1_000_000_007, -1, 0, 1, 86_400, i64::MAX];
    let unsigned_amounts = [0, 1, 1_u64 << 40, u64::MAX];
    for unit in UNITS {
        for amount in signed_amounts {
            let values = datetimes(&boundaries);
            let amounts = Int64Array::from(vec![amount; boundaries.len()]);
            let expected = boundaries
                .iter()
                .map(|value| {
                    let instant = i128::from(*value) + i128::from(amount) * unit_nanoseconds(unit);
                    expected_lane(fits_i64(instant))
                })
                .collect::<Vec<_>>();
            let added = add(
                &UnitCounts::Int64(&amounts),
                &values,
                DatetimeUnit::Fixed(unit),
                &Zone::UTC,
            );
            assert_eq!(checked_lanes(&added), expected, "{amount} {unit}");
        }
        for amount in unsigned_amounts {
            let values = datetimes(&boundaries);
            let amounts = UInt64Array::from(vec![amount; boundaries.len()]);
            let expected = boundaries
                .iter()
                .map(|value| {
                    let instant = i128::from(*value) + i128::from(amount) * unit_nanoseconds(unit);
                    expected_lane(fits_i64(instant))
                })
                .collect::<Vec<_>>();
            let added = add(
                &UnitCounts::UInt64(&amounts),
                &values,
                DatetimeUnit::Fixed(unit),
                &Zone::UTC,
            );
            assert_eq!(checked_lanes(&added), expected, "{amount} {unit}");
        }
    }
}

#[test]
fn differences_round_toward_zero_in_both_directions() {
    let origin = nanoseconds("2000-02-29T00:05:00Z");
    let reading = nanoseconds("2000-02-29T23:52:30.250Z");
    let epoch = nanoseconds("1970-01-01T00:00:00Z");
    let before_epoch = nanoseconds("1969-12-31T23:59:59.999999999Z");
    let starts = datetimes(&[origin, reading, epoch, i64::MIN, i64::MIN]);
    let ends = datetimes(&[reading, origin, before_epoch, i64::MAX, i64::MAX]);

    let seconds = difference(
        &starts,
        &ends,
        DatetimeUnit::Fixed(FixedTimeUnit::Second),
        &Zone::UTC,
    );
    assert_eq!(
        checked_lanes(&seconds)[..3],
        [
            (Some(85_650), false),
            (Some(-85_650), false),
            (Some(0), false)
        ]
    );

    let nanoseconds_apart = difference(
        &starts,
        &ends,
        DatetimeUnit::Fixed(FixedTimeUnit::Nanosecond),
        &Zone::UTC,
    );
    assert_eq!(checked_lanes(&nanoseconds_apart)[3], (None, true));
    let microseconds_apart = difference(
        &starts,
        &ends,
        DatetimeUnit::Fixed(FixedTimeUnit::Microsecond),
        &Zone::UTC,
    );
    assert_eq!(
        checked_lanes(&microseconds_apart)[4],
        (Some(18_446_744_073_709_551), false)
    );
}

#[test]
fn differences_match_truncated_division_for_every_boundary_pair() {
    let boundaries = boundary_nanoseconds();
    let pairs = boundaries
        .iter()
        .flat_map(|start| boundaries.iter().map(move |end| (*start, *end)))
        .collect::<Vec<_>>();
    let starts = datetimes(&pairs.iter().map(|pair| pair.0).collect::<Vec<_>>());
    let ends = datetimes(&pairs.iter().map(|pair| pair.1).collect::<Vec<_>>());
    for unit in UNITS {
        let expected = pairs
            .iter()
            .map(|(start, end)| {
                let elapsed = i128::from(*end) - i128::from(*start);
                expected_lane(fits_i64(elapsed / unit_nanoseconds(unit)))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            checked_lanes(&difference(
                &starts,
                &ends,
                DatetimeUnit::Fixed(unit),
                &Zone::UTC
            )),
            expected,
            "{unit}"
        );
    }
}

#[test]
fn unix_counts_round_down_before_the_epoch() {
    let values = datetimes(&[
        nanoseconds("1969-12-31T23:59:59.999999999Z"),
        nanoseconds("1969-12-31T12:00:00Z"),
        nanoseconds("2000-02-29T23:52:30.250Z"),
    ]);

    assert_eq!(
        to_unix(&values, FixedTimeUnit::Second).values().as_ref(),
        [-1, -43_200, 951_868_350]
    );
    assert_eq!(
        to_unix(&values, FixedTimeUnit::Millisecond)
            .values()
            .as_ref(),
        [-1, -43_200_000, 951_868_350_250]
    );
    assert_eq!(
        to_unix(&values, FixedTimeUnit::Day).values().as_ref(),
        [-1, -1, 11_016]
    );
}

#[test]
fn unix_counts_match_floor_division_and_round_trip_whole_units() {
    let boundaries = boundary_nanoseconds();
    let values = datetimes(&boundaries);
    for unit in UNITS {
        let counts = to_unix(&values, unit);
        let expected = boundaries
            .iter()
            .map(|value| {
                fits_i64(i128::from(*value).div_euclid(unit_nanoseconds(unit)))
                    .expect("a floor quotient of an i64 by a positive unit fits i64")
            })
            .collect::<Vec<_>>();
        assert_eq!(counts.values().as_ref(), expected.as_slice(), "{unit}");

        let restored = from_unix(&UnitCounts::Int64(&counts), unit);
        let truncated = boundaries
            .iter()
            .map(|value| expected_lane(model_bin_start(*value, 0, unit_nanoseconds(unit))))
            .collect::<Vec<_>>();
        assert_eq!(checked_lanes(&restored), truncated, "{unit}");
    }
}

#[test]
fn unix_conversion_fails_outside_the_range() {
    let seconds = Int64Array::from(vec![
        9_223_372_036,
        9_223_372_037,
        -9_223_372_036,
        -9_223_372_037,
    ]);
    assert_eq!(
        checked_lanes(&from_unix(
            &UnitCounts::Int64(&seconds),
            FixedTimeUnit::Second
        )),
        [
            (Some(nanoseconds("2262-04-11T23:47:16Z")), false),
            (None, true),
            (Some(nanoseconds("1677-09-21T00:12:44Z")), false),
            (None, true),
        ]
    );

    let nanoseconds_since_epoch = UInt64Array::from(vec![0, u64::MAX]);
    assert_eq!(
        checked_lanes(&from_unix(
            &UnitCounts::UInt64(&nanoseconds_since_epoch),
            FixedTimeUnit::Nanosecond
        )),
        [(Some(0), false), (None, true)]
    );

    let weeks = Int8Array::from(vec![i8::MIN, -1, i8::MAX]);
    assert_eq!(
        checked_lanes(&from_unix(&UnitCounts::Int8(&weeks), FixedTimeUnit::Week)),
        [
            (Some(-128 * FixedTimeUnit::Week.nanoseconds()), false),
            (Some(-FixedTimeUnit::Week.nanoseconds()), false),
            (Some(127 * FixedTimeUnit::Week.nanoseconds()), false),
        ]
    );
}

#[test]
fn null_and_sliced_lanes_never_fail() {
    let values = TimestampNanosecondArray::from(vec![
        Some(i64::MAX),
        None,
        Some(i64::MIN),
        Some(nanoseconds("2000-02-29T23:52:30.250Z")),
        None,
        Some(i64::MAX),
    ])
    .with_timezone_utc();
    let amounts = Int64Array::from(vec![Some(1), Some(1), None, Some(1), Some(1), Some(1)]);
    let sliced_values = values.slice(1, 5);
    let sliced_amounts = amounts.slice(1, 5);

    let added = add(
        &UnitCounts::Int64(&sliced_amounts),
        &sliced_values,
        DatetimeUnit::Fixed(FixedTimeUnit::Day),
        &Zone::UTC,
    );

    assert_eq!(
        checked_lanes(&added),
        [
            (None, false),
            (None, false),
            (Some(nanoseconds("2000-03-01T23:52:30.250Z")), false),
            (None, false),
            (None, true),
        ]
    );

    let truncated = truncate(
        &sliced_values,
        DatetimeUnit::Fixed(FixedTimeUnit::Week),
        &Zone::UTC,
    );
    assert_eq!(checked_lanes(&truncated)[0], (None, false));
    assert_eq!(checked_lanes(&truncated)[1], (None, true));
    let extracted = date_part(&sliced_values, DatePart::DayOfYear, &Zone::UTC);
    assert!(extracted.is_null(0));
    assert_eq!(extracted.value(2), 60);
}

#[test]
fn datetime_results_carry_the_utc_timezone() {
    let values = datetimes(&[0]);
    let counts = Int64Array::from(vec![0]);
    let columns: [PrimitiveArray<TimestampNanosecondType>; 4] = [
        truncate(
            &values,
            DatetimeUnit::Fixed(FixedTimeUnit::Hour),
            &Zone::UTC,
        )
        .column,
        bin(&values, &values, width(1, FixedTimeUnit::Day)).column,
        add(
            &UnitCounts::Int64(&counts),
            &values,
            DatetimeUnit::Fixed(FixedTimeUnit::Second),
            &Zone::UTC,
        )
        .column,
        from_unix(&UnitCounts::Int64(&counts), FixedTimeUnit::Second).column,
    ];
    for column in columns {
        assert_eq!(column.timezone(), Some("+00:00"));
    }
}
