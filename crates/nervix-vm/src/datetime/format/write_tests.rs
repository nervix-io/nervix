//! Tests for writing DATETIME columns in a format.
//!
//! Layer: test harness.
//!
//! - **Owns.** The text every directive writes at known instants, in UTC, at fixed offsets and under
//!   IANA rules with local mean time, at both ends of the DATETIME range, how formats lay out as
//!   templates around the items whose width varies, the bound on a column's text, and differential
//!   checks against jiff's own formatting.
//! - **Depends on.** The format writer, compiled formats, zones, and jiff as the reference.
//! - **Must not know.** Registers, programs, or row errors.

use std::str::FromStr as _;

use arrow_array::{Array, StringArray, TimestampNanosecondArray};
use jiff::{Timestamp, tz::TimeZone};
use nervix_models::Timestamp as NervixTimestamp;

use super::{ColumnLayout, FormattedColumn, LayoutPiece, format_datetimes, text_capacity};
use crate::datetime::{DatetimeFormat, Zone};

fn nanoseconds(instant: &str) -> i64 {
    NervixTimestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

fn format(written: &str, zone: &Zone) -> DatetimeFormat {
    DatetimeFormat::compile(written, zone).unwrap_or_else(|defect| panic!("`{written}`: {defect}"))
}

fn written(instants: &[Option<i64>], written_format: &str, zone_name: &str) -> Vec<Option<String>> {
    let zone = zone(zone_name);
    let values = TimestampNanosecondArray::from(instants.to_vec()).with_timezone_utc();
    let FormattedColumn::Formatted(column) =
        format_datetimes(&values, &format(written_format, &zone), &zone)
    else {
        panic!("a small column always fits");
    };
    column
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

fn write_one(instant: &str, written_format: &str, zone_name: &str) -> String {
    let texts = written(&[Some(nanoseconds(instant))], written_format, zone_name);
    let [Some(text)] = texts.as_slice() else {
        panic!("a valid lane writes text");
    };
    text.clone()
}

#[test]
fn every_directive_writes_its_field() {
    let instant = "2024-03-10T07:05:09.0123Z";
    let cases = [
        ("%Y", "2024"),
        ("%G", "2024"),
        ("%m", "03"),
        ("%-m", "3"),
        ("%b", "Mar"),
        ("%B", "March"),
        ("%d", "10"),
        ("%-d", "10"),
        ("%e", "10"),
        ("%j", "070"),
        ("%-j", "70"),
        ("%a", "Sun"),
        ("%A", "Sunday"),
        ("%u", "7"),
        ("%w", "0"),
        ("%V", "10"),
        ("%-V", "10"),
        ("%H", "07"),
        ("%-H", "7"),
        ("%I", "07"),
        ("%-I", "7"),
        ("%p", "AM"),
        ("%M", "05"),
        ("%-M", "5"),
        ("%S", "09"),
        ("%-S", "9"),
        ("%f", "012300000"),
        ("%1f", "0"),
        ("%3f", "012"),
        ("%6f", "012300"),
        ("%.f", ".0123"),
        ("%z", "+0000"),
        ("%:z", "+00:00"),
        ("%::z", "+00:00:00"),
        ("%Z", "UTC"),
        ("%Q", "UTC"),
        ("%s", "1710054309"),
        ("%%", "%"),
        ("%n%t", "\n\t"),
        ("%F %T", "2024-03-10 07:05:09"),
        ("%R", "07:05"),
        ("day %j of %Y ✓", "day 070 of 2024 ✓"),
    ];
    for (directive, expected) in cases {
        assert_eq!(
            write_one(instant, directive, "UTC"),
            expected,
            "{directive}"
        );
    }
}

#[test]
fn values_are_written_in_the_local_time_of_their_zone() {
    let cases = [
        // New York's local mean time was 4:56:02 behind UTC, until noon on 1883-11-18.
        (
            "1883-11-18T16:59:59Z",
            "America/New_York",
            "%F %T %z %:z %Z",
            "1883-11-18 12:03:57 -045602 -04:56:02 LMT",
        ),
        (
            "1883-11-18T17:00:00Z",
            "America/New_York",
            "%F %T %z %Z",
            "1883-11-18 12:00:00 -0500 EST",
        ),
        (
            "2024-03-10T11:00:00.25Z",
            "America/New_York",
            "%a %d %b %Y %H:%M:%S%.f %Z",
            "Sun 10 Mar 2024 07:00:00.25 EDT",
        ),
        (
            "2024-11-03T05:30:00Z",
            "America/New_York",
            "%I:%M %p %Z %Q",
            "01:30 AM EDT America/New_York",
        ),
        (
            "2024-11-03T06:30:00Z",
            "US/Eastern",
            "%I:%M %p %Z %Q",
            "01:30 AM EST US/Eastern",
        ),
        (
            "2024-07-04T12:10:00Z",
            "Asia/Kathmandu",
            "%H:%M %:z",
            "17:55 +05:45",
        ),
        (
            "2024-07-04T12:10:00Z",
            "+05:30",
            "%FT%T%:z %Z %Q",
            "2024-07-04T17:40:00+05:30 +05:30 +05:30",
        ),
        ("2024-07-04T12:10:00Z", "-00:30", "%H:%M %z", "11:40 -0030"),
        (
            "2011-12-30T09:59:59Z",
            "Pacific/Apia",
            "%F %T %Z",
            "2011-12-29 23:59:59 -10",
        ),
        (
            "2011-12-30T10:00:00Z",
            "Pacific/Apia",
            "%F %T %Z",
            "2011-12-31 00:00:00 +14",
        ),
        // A Unix time does not depend on the zone, and rounds down before the epoch.
        ("1969-12-31T23:59:59.5Z", "Europe/Berlin", "%s%.f", "-1.5"),
        ("1970-01-01T00:00:00Z", "Asia/Tokyo", "%s", "0"),
        // Noon and midnight on the 12-hour clock.
        ("2024-01-01T00:30:00Z", "UTC", "%I:%M %p", "12:30 AM"),
        ("2024-01-01T12:30:00Z", "UTC", "%I:%M %p", "12:30 PM"),
        ("2024-01-05T09:00:00Z", "UTC", "%e|%-d|%d", " 5|5|05"),
        // Week 53 of 2020 and week 1 of 2025 fall in neighbouring calendar years.
        ("2021-01-03T12:00:00Z", "UTC", "%G-W%V-%u", "2020-W53-7"),
        ("2024-12-30T12:00:00Z", "UTC", "%G-W%V-%u", "2025-W01-1"),
    ];
    for (instant, zone_name, written_format, expected) in cases {
        assert_eq!(
            write_one(instant, written_format, zone_name),
            expected,
            "`{written_format}` of {instant} in {zone_name}"
        );
    }
}

#[test]
fn both_ends_of_the_range_are_written_in_every_zone() {
    let full = "%Y-%m-%dT%H:%M:%S.%f%:z %G-W%V-%u %j %s";
    let cases = [
        (
            i64::MIN,
            "UTC",
            "1677-09-21T00:12:43.145224192+00:00 1677-W38-2 264 -9223372037",
        ),
        (
            i64::MAX,
            "UTC",
            "2262-04-11T23:47:16.854775807+00:00 2262-W15-5 101 9223372036",
        ),
        (
            i64::MIN,
            "-23:59",
            "1677-09-20T00:13:43.145224192-23:59 1677-W38-1 263 -9223372037",
        ),
        (
            i64::MAX,
            "+23:59",
            "2262-04-12T23:46:16.854775807+23:59 2262-W15-6 102 9223372036",
        ),
    ];
    for (instant, zone_name, expected) in cases {
        let texts = written(&[Some(instant)], full, zone_name);
        assert_eq!(
            texts,
            [Some(expected.to_string())],
            "{instant} in {zone_name}"
        );
    }
}

#[test]
fn null_lanes_stay_null_and_sliced_columns_keep_their_offsets() {
    let values = TimestampNanosecondArray::from(vec![
        Some(nanoseconds("2000-02-29T12:00:00Z")),
        None,
        Some(nanoseconds("2024-11-03T06:30:00Z")),
        None,
    ])
    .with_timezone_utc();
    let sliced = values.slice(1, 3);
    for (written_format, zone_name, expected) in [
        // Fixed width in a constant zone.
        (
            "%FT%T%:z",
            "UTC",
            [None, Some("2024-11-03T06:30:00+00:00"), None],
        ),
        // Variable width.
        (
            "%B %-d %Z",
            "America/New_York",
            [None, Some("November 3 EST"), None],
        ),
    ] {
        let resolved = zone(zone_name);
        let FormattedColumn::Formatted(column) =
            format_datetimes(&sliced, &format(written_format, &resolved), &resolved)
        else {
            panic!("a small column always fits");
        };
        let texts = column.iter().collect::<Vec<_>>();
        assert_eq!(texts, expected, "{written_format}");
        assert_eq!(column.null_count(), 2);
    }
}

/// The pieces `written_format` lays out as in `zone_name`: a template's text with zeros where its
/// fields go, and every other piece by name.
fn laid_out(written_format: &str, zone_name: &str) -> Vec<String> {
    let resolved = zone(zone_name);
    let compiled = format(written_format, &resolved);
    let layout = ColumnLayout::of(&compiled, &resolved);
    let mut pieces = Vec::new();
    for piece in &layout.pieces {
        let described = match piece {
            LayoutPiece::Template(template) => {
                String::from_utf8(template.text.clone()).expect("a template is UTF-8 text")
            }
            LayoutPiece::Varying(field) => format!("{field:?}"),
            LayoutPiece::Abbreviation => "Abbreviation".to_string(),
        };
        pieces.push(described);
    }
    pieces
}

#[test]
fn formats_lay_out_as_templates_around_the_items_whose_width_varies() {
    let zoned_names = "%a %d %b %Y %H:%M:%S%.f %Z (%Q)";
    let cases: [(&str, &str, &[&str]); 8] = [
        (
            "%Y-%m-%dT%H:%M:%S.%f%:z",
            "UTC",
            &["0000-00-00T00:00:00.000000000+00:00"],
        ),
        ("%F %T", "America/New_York", &["0000-00-00 00:00:00"]),
        ("%FT%T%z %Z", "+05:30", &["0000-00-00T00:00:00+0530 +05:30"]),
        // Under IANA rules the offset and the abbreviation change with the offset shown.
        (
            "%FT%T%z",
            "Asia/Kolkata",
            &["0000-00-00T00:00:00", "Offset(Compact)"],
        ),
        (
            zoned_names,
            "UTC",
            &["000 00 000 0000 00:00:00", "OptionalFraction", " UTC (UTC)"],
        ),
        (
            zoned_names,
            "America/New_York",
            &[
                "000 00 000 0000 00:00:00",
                "OptionalFraction",
                " ",
                "Abbreviation",
                " (America/New_York)",
            ],
        ),
        (
            "%B %-d, %Y %s",
            "UTC",
            &[
                "FullName(Month)",
                " ",
                "Unpadded(Day)",
                ", 0000 ",
                "UnixSeconds",
            ],
        ),
        ("", "UTC", &[]),
    ];
    for (written_format, zone_name, expected) in cases {
        assert_eq!(
            laid_out(written_format, zone_name),
            expected,
            "`{written_format}` in {zone_name}"
        );
    }
}

#[test]
fn a_column_whose_text_could_exceed_a_string_array_is_too_large() {
    assert_eq!(text_capacity(1_024, 256), Some(262_144));
    assert_eq!(text_capacity(8_388_607, 256), Some(2_147_483_392));
    assert_eq!(text_capacity(8_388_608, 256), None);
    assert_eq!(text_capacity(usize::MAX, 2), None);
    assert_eq!(text_capacity(0, 256), Some(0));
}

#[test]
fn directives_write_what_jiff_writes() {
    let directives = "%Y|%m|%-m|%d|%-d|%e|%j|%-j|%H|%-H|%I|%-I|%M|%-M|%S|%-S|%p|%a|%A|%b|%B|%u|%\
                      w|%G|%V|%-V|%z|%:z|%::z|%Z|%s";
    for zone_name in [
        "America/New_York",
        "Europe/Amsterdam",
        "Australia/Lord_Howe",
        "Asia/Kolkata",
    ] {
        let (name, data) = jiff_tzdb::get(zone_name).expect("a bundled zone");
        let rules = TimeZone::tzif(name, data).expect("bundled TZif data");
        let mut instants = Vec::new();
        let mut instant = nanoseconds("1880-01-01T00:00:00Z");
        while instant < nanoseconds("2060-01-01T00:00:00Z") {
            instants.push(Some(instant));
            instant += 86_400_000_000_000 * 11 + 3_600_000_000_000 * 7 + 60_000_000_000 * 13;
        }
        let texts = written(&instants, directives, zone_name);
        for (text, instant) in texts.iter().zip(&instants) {
            let instant = instant.expect("every lane is valid");
            // Whole seconds, which jiff's zoned offsets read exactly.
            let zoned = Timestamp::from_nanosecond(i128::from(instant))
                .expect("in jiff's range")
                .to_zoned(rules.clone());
            let expected = zoned.strftime(directives).to_string();
            assert_eq!(
                text.as_deref(),
                Some(expected.as_str()),
                "{instant} in {zone_name}"
            );
        }
    }
}

#[test]
fn formatted_columns_hold_valid_text_for_every_lane() {
    let values =
        TimestampNanosecondArray::from(vec![Some(0), None, Some(i64::MAX)]).with_timezone_utc();
    let resolved = zone("Asia/Tokyo");
    let FormattedColumn::Formatted(column) = format_datetimes(
        &values,
        &format("%A %B %e, %Y — %H時%M分 %Z", &resolved),
        &resolved,
    ) else {
        panic!("a small column always fits");
    };
    let expected = StringArray::from(vec![
        Some("Thursday January  1, 1970 — 09時00分 JST"),
        None,
        Some("Saturday April 12, 2262 — 08時47分 JST"),
    ]);
    assert_eq!(column, expected);
}
