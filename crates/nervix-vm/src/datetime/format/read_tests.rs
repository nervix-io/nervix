//! Tests for reading text columns as DATETIME values.
//!
//! Layer: test harness.
//!
//! - **Owns.** Round trips through writing and reading across zones and the DATETIME range, the
//!   instant every readable layout names, every way a text fails with the position and field it
//!   fails at, the resolution of skipped and repeated local times, and columns with null and failed
//!   lanes.
//! - **Depends on.** The format reader and writer, compiled parsers, and zones.
//! - **Must not know.** Registers, programs, or how a failure is recorded as a row error.

use std::str::FromStr as _;

use arrow_array::{StringArray, TimestampNanosecondArray};
use nervix_models::Timestamp as NervixTimestamp;

use super::{TextFailure, UnreadableText, parse_datetimes};
use crate::{
    datetime::{
        DatetimeFormat, DatetimeParser, FormatDirective, ParseFormat, TextExpectation,
        UnresolvedLocalTime, Zone,
        format::{DayPadding, FormattedColumn, Padding, format_datetimes},
        zone::OffsetStyle,
    },
    program::Disambiguation,
};

fn nanoseconds(instant: &str) -> i64 {
    NervixTimestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

fn parser(written: &str, local_time: Option<(&str, Disambiguation)>) -> DatetimeParser {
    let format =
        ParseFormat::compile(written).unwrap_or_else(|defect| panic!("`{written}`: {defect}"));
    let local_time =
        local_time.map(|(zone_name, disambiguation)| (zone(zone_name), disambiguation));
    DatetimeParser::new(format, local_time).expect("the format and zone agree")
}

fn read(parser: &DatetimeParser, text: &str) -> Result<i64, TextFailure> {
    parser.read(text.as_bytes())
}

fn mismatch(position: usize, expected: TextExpectation) -> Result<i64, TextFailure> {
    Err(TextFailure::Unreadable(UnreadableText::Mismatch {
        position,
        expected,
    }))
}

fn literal(text: &str) -> TextExpectation {
    TextExpectation::Literal(triomphe::Arc::from(text))
}

fn unreadable(reason: UnreadableText) -> Result<i64, TextFailure> {
    Err(TextFailure::Unreadable(reason))
}

#[test]
fn written_values_read_back_as_the_same_instants() {
    let mut instants = vec![i64::MIN, -1, 0, 1, i64::MAX];
    let mut instant = nanoseconds("1700-01-01T00:00:00Z");
    while instant < nanoseconds("2250-01-01T00:00:00Z") {
        instants.push(instant);
        instant += 86_400_000_000_000 * 53 + 3_600_000_000_000 * 11 + 987_654_321;
    }
    let values = TimestampNanosecondArray::from(instants.clone()).with_timezone_utc();
    let cases = [
        ("%Y-%m-%dT%H:%M:%S.%f%:z", "Europe/Berlin", None),
        ("%a, %d %b %Y %H:%M:%S%.f %z", "America/New_York", None),
        (
            "%G-W%V-%u %I:%M:%S %p %9f %::z",
            "Australia/Lord_Howe",
            None,
        ),
        ("%Y%j %H%M%S%.f %z", "Asia/Kathmandu", None),
        ("%s%.f", "Pacific/Apia", None),
        ("%s.%f", "UTC", None),
        (
            "%A %B %e %Y %-H:%-M:%-S.%f",
            "UTC",
            Some(("UTC", Disambiguation::Reject)),
        ),
        (
            "%F %T.%f",
            "+05:30",
            Some(("+05:30", Disambiguation::Reject)),
        ),
    ];
    for (written_format, zone_name, local_time) in cases {
        let resolved = zone(zone_name);
        let writing =
            DatetimeFormat::compile(written_format, &resolved).expect("a writable format");
        let FormattedColumn::Formatted(texts) = format_datetimes(&values, &writing, &resolved)
        else {
            panic!("a small column always fits");
        };
        let reading = parser(written_format, local_time);
        let parsed = parse_datetimes(&texts, &reading);
        assert!(
            parsed.failures.is_empty(),
            "`{written_format}` in {zone_name} reads every value it writes: {:?}",
            parsed
                .failures
                .iter()
                .map(|failed| (texts.value(failed.lane), failed.failure.clone()))
                .collect::<Vec<_>>()
        );
        assert_eq!(parsed.column, values, "`{written_format}` in {zone_name}");
    }
}

#[test]
fn every_layout_reads_the_instant_its_fields_name() {
    let berlin = Some(("Europe/Berlin", Disambiguation::Reject));
    let utc = Some(("UTC", Disambiguation::Reject));
    let cases = [
        ("%Y-%m-%d", "2024-02-29", utc, "2024-02-29T00:00:00Z"),
        ("%Y-%m-%d", "2024-02-29", berlin, "2024-02-28T23:00:00Z"),
        (
            "%d/%b/%Y:%H:%M:%S %z",
            "04/Jul/2024:12:30:00 -0400",
            None,
            "2024-07-04T16:30:00Z",
        ),
        (
            "%d/%b/%Y:%H:%M:%S %z",
            "04/JUL/2024:12:30:00 Z",
            None,
            "2024-07-04T12:30:00Z",
        ),
        (
            "%FT%T%:z",
            "2024-07-04T12:30:00+05:30",
            None,
            "2024-07-04T07:00:00Z",
        ),
        (
            "%FT%T%:z",
            "1883-11-18T12:03:57-04:56:02",
            None,
            "1883-11-18T16:59:59Z",
        ),
        (
            "%FT%T%z",
            "1883-11-18T12:03:57-045602",
            None,
            "1883-11-18T16:59:59Z",
        ),
        (
            "%FT%T%::z",
            "2024-07-04T12:30:00+05:30:00",
            None,
            "2024-07-04T07:00:00Z",
        ),
        ("%Y-%j", "2024-366", utc, "2024-12-31T00:00:00Z"),
        ("%u %Y-%j", "4 2024-186", utc, "2024-07-04T00:00:00Z"),
        ("%G-W%V-%u", "2020-W53-7", utc, "2021-01-03T00:00:00Z"),
        ("%G-W%V-%a", "2025-W01-mon", utc, "2024-12-30T00:00:00Z"),
        (
            "%A %B %e %Y %I:%M %p",
            "thursday july  4 2024 12:30 am",
            utc,
            "2024-07-04T00:30:00Z",
        ),
        (
            "%A %B %e %Y %I:%M %p",
            "Thursday July 04 2024 12:30 PM",
            utc,
            "2024-07-04T12:30:00Z",
        ),
        (
            "%-d/%-m/%Y %-H:%-M",
            "4/7/2024 9:05",
            utc,
            "2024-07-04T09:05:00Z",
        ),
        (
            "%-d/%-m/%Y %-H:%-M",
            "04/07/2024 09:05",
            utc,
            "2024-07-04T09:05:00Z",
        ),
        (
            "%F %T%.f",
            "2024-07-04 12:30:00",
            utc,
            "2024-07-04T12:30:00Z",
        ),
        (
            "%F %T%.f",
            "2024-07-04 12:30:00.5",
            utc,
            "2024-07-04T12:30:00.5Z",
        ),
        (
            "%F %T%.f",
            "2024-07-04 12:30:00.000000001",
            utc,
            "2024-07-04T12:30:00.000000001Z",
        ),
        (
            "%F %T.%3f",
            "2024-07-04 12:30:00.250",
            utc,
            "2024-07-04T12:30:00.25Z",
        ),
        ("%s", "-1", None, "1969-12-31T23:59:59Z"),
        ("%s%.f", "-1.5", None, "1969-12-31T23:59:59.5Z"),
        (
            "%s%.f",
            "-9223372037.145224192",
            None,
            "1677-09-21T00:12:43.145224192Z",
        ),
        ("%s", "9223372036", None, "2262-04-11T23:47:16Z"),
        (
            "%F %T",
            "2024-07-04 12:30:00",
            Some(("+05:30", Disambiguation::Reject)),
            "2024-07-04T07:00:00Z",
        ),
    ];
    for (written_format, text, local_time, expected) in cases {
        let parser = parser(written_format, local_time);
        assert_eq!(
            read(&parser, text),
            Ok(nanoseconds(expected)),
            "`{text}` in `{written_format}`"
        );
    }
}

#[test]
fn texts_that_do_not_match_their_format_fail_where_they_stop_matching() {
    let utc = Some(("UTC", Disambiguation::Reject));
    let local = parser("%Y-%m-%d %H:%M:%S", utc);
    assert_eq!(
        read(&local, "2024/07/04 12:30:00"),
        mismatch(4, literal("-"))
    );
    assert_eq!(
        read(&local, "2024-07-04 12:30:00Z"),
        unreadable(UnreadableText::TrailingText { position: 19 })
    );
    assert_eq!(
        read(&local, ""),
        mismatch(0, TextExpectation::Directive(FormatDirective::Year))
    );
    assert_eq!(
        read(&local, "2024-7-04 12:30:00"),
        mismatch(
            5,
            TextExpectation::Directive(FormatDirective::Month(Padding::Zero))
        )
    );
    assert_eq!(read(&local, "2024-07-04 12:30"), mismatch(16, literal(":")));
    assert_eq!(
        read(&local, " 2024-07-04 12:30:00"),
        mismatch(0, TextExpectation::Directive(FormatDirective::Year))
    );

    let names = parser("%a %d %B %Y", utc);
    assert_eq!(
        read(&names, "Thu 04 Jul 2024"),
        mismatch(
            7,
            TextExpectation::Directive(FormatDirective::MonthName(super::NameLength::Full))
        )
    );
    assert_eq!(
        read(&names, "Thr 04 July 2024"),
        mismatch(
            0,
            TextExpectation::Directive(FormatDirective::WeekdayName(
                super::NameLength::Abbreviated
            ))
        )
    );

    let spaced = parser("%e.%m.%Y", utc);
    assert_eq!(
        read(&spaced, " 4.07.2024"),
        Ok(nanoseconds("2024-07-04T00:00:00Z"))
    );
    assert_eq!(
        read(&spaced, "4.07.2024"),
        mismatch(
            0,
            TextExpectation::Directive(FormatDirective::Day(DayPadding::Space))
        )
    );

    let fraction = parser("%T%.f %F", utc);
    assert_eq!(
        read(&fraction, "12:30:00. 2024-07-04"),
        mismatch(
            8,
            TextExpectation::Directive(FormatDirective::OptionalFraction)
        )
    );
    assert_eq!(
        read(&fraction, "12:30:00.1234567890 2024-07-04"),
        mismatch(18, literal(" "))
    );

    let offsets = parser("%F %z", None);
    assert_eq!(
        read(&offsets, "2024-07-04 +05:30"),
        mismatch(
            11,
            TextExpectation::Directive(FormatDirective::Offset(OffsetStyle::Compact))
        )
    );
    assert_eq!(
        read(&offsets, "2024-07-04 05:30"),
        mismatch(
            11,
            TextExpectation::Directive(FormatDirective::Offset(OffsetStyle::Compact))
        )
    );
    let colon = parser("%F %:z", None);
    assert_eq!(
        read(&colon, "2024-07-04 +0530"),
        mismatch(
            11,
            TextExpectation::Directive(FormatDirective::Offset(OffsetStyle::Colon))
        )
    );

    let unix = parser("%s", None);
    assert_eq!(
        read(&unix, "-"),
        mismatch(0, TextExpectation::Directive(FormatDirective::UnixSeconds))
    );
    assert_eq!(
        read(&unix, "12345678901234567890"),
        unreadable(UnreadableText::TrailingText { position: 19 })
    );
}

#[test]
fn fields_outside_their_ranges_and_dates_that_do_not_exist_fail() {
    use crate::datetime::DatetimeField;

    let utc = Some(("UTC", Disambiguation::Reject));
    let out_of_range = |field| unreadable(UnreadableText::FieldOutOfRange(field));
    let cases = [
        (
            "%F %T",
            "2024-13-01 00:00:00",
            out_of_range(DatetimeField::Month),
        ),
        (
            "%F %T",
            "2024-00-01 00:00:00",
            out_of_range(DatetimeField::Month),
        ),
        (
            "%F %T",
            "2024-01-32 00:00:00",
            out_of_range(DatetimeField::Day),
        ),
        (
            "%F %T",
            "2024-07-04 24:00:00",
            out_of_range(DatetimeField::Hour),
        ),
        (
            "%F %T",
            "2024-07-04 23:60:00",
            out_of_range(DatetimeField::Minute),
        ),
        // A DATETIME has no leap seconds.
        (
            "%F %T",
            "2016-12-31 23:59:60",
            out_of_range(DatetimeField::Second),
        ),
        (
            "%F %T",
            "2023-02-29 00:00:00",
            unreadable(UnreadableText::NonexistentDate),
        ),
        (
            "%F %T",
            "1900-02-29 00:00:00",
            unreadable(UnreadableText::NonexistentDate),
        ),
        (
            "%F %T",
            "2024-04-31 00:00:00",
            unreadable(UnreadableText::NonexistentDate),
        ),
        (
            "%Y-%j",
            "2023-366",
            unreadable(UnreadableText::NonexistentDate),
        ),
        ("%Y-%j", "2024-367", out_of_range(DatetimeField::DayOfYear)),
        ("%Y-%j", "2024-000", out_of_range(DatetimeField::DayOfYear)),
        (
            "%G-W%V-%u",
            "2021-W53-1",
            unreadable(UnreadableText::NonexistentDate),
        ),
        (
            "%G-W%V-%u",
            "2020-W54-1",
            out_of_range(DatetimeField::IsoWeek),
        ),
        (
            "%G-W%V-%u",
            "2020-W00-1",
            out_of_range(DatetimeField::IsoWeek),
        ),
        (
            "%G-W%V-%u",
            "2020-W01-8",
            out_of_range(DatetimeField::Weekday),
        ),
        (
            "%G-W%V-%u",
            "2020-W01-0",
            out_of_range(DatetimeField::Weekday),
        ),
        (
            "%G-W%V-%w",
            "2020-W01-7",
            out_of_range(DatetimeField::Weekday),
        ),
        (
            "%a %F",
            "Tue 2024-07-04",
            unreadable(UnreadableText::InconsistentField(DatetimeField::Weekday)),
        ),
        (
            "%u %Y-%j",
            "1 2024-186",
            unreadable(UnreadableText::InconsistentField(DatetimeField::Weekday)),
        ),
        (
            "%F %I:%M %p",
            "2024-07-04 13:00 PM",
            out_of_range(DatetimeField::TwelveHour),
        ),
        (
            "%F %I:%M %p",
            "2024-07-04 00:30 AM",
            out_of_range(DatetimeField::TwelveHour),
        ),
        // The first field out of range fails the text, even before a nonexistent date.
        (
            "%F %T",
            "2023-02-29 24:00:00",
            out_of_range(DatetimeField::Hour),
        ),
    ];
    for (written_format, text, expected) in cases {
        assert_eq!(
            read(&parser(written_format, utc), text),
            expected,
            "`{text}`"
        );
    }

    let offsets = [
        ("2024-07-04 +2400", out_of_range(DatetimeField::Offset)),
        ("2024-07-04 +0560", out_of_range(DatetimeField::Offset)),
        ("2024-07-04 +053060", out_of_range(DatetimeField::Offset)),
    ];
    for (text, expected) in offsets {
        assert_eq!(read(&parser("%F %z", None), text), expected, "`{text}`");
    }
}

#[test]
fn instants_outside_the_range_fail_as_out_of_range() {
    let utc = Some(("UTC", Disambiguation::Reject));
    let cases = [
        (parser("%F %T", utc), "2262-04-12 00:00:00"),
        (parser("%F %T%.f", utc), "1677-09-21 00:12:43.145224191"),
        (parser("%F %T %z", None), "2262-04-11 23:47:17 +0000"),
        (parser("%F %T %z", None), "1677-09-21 00:12:43 +0001"),
        (parser("%s", None), "-9223372037"),
        (parser("%s", None), "9223372037"),
        (parser("%s", None), "9999999999999999999"),
    ];
    for (parser, text) in cases {
        assert_eq!(
            read(&parser, text),
            Err(TextFailure::OutOfRange),
            "`{text}`"
        );
    }
    assert_eq!(
        read(&parser("%F %T%.f", utc), "1677-09-21 00:12:43.145224192"),
        Ok(i64::MIN)
    );
}

#[test]
fn local_times_a_zone_skips_or_repeats_resolve_as_the_call_says() {
    let skipped = "2024-03-10 02:30:00";
    let repeated = "2024-11-03 01:30:00";
    let rejected = parser("%F %T", Some(("America/New_York", Disambiguation::Reject)));
    let new_york = zone("America/New_York");
    assert_eq!(
        read(&rejected, skipped),
        Err(TextFailure::Unresolved {
            local_time: UnresolvedLocalTime::Skipped,
            zone: new_york.clone(),
        })
    );
    assert_eq!(
        read(&rejected, repeated),
        Err(TextFailure::Unresolved {
            local_time: UnresolvedLocalTime::Repeated,
            zone: new_york,
        })
    );
    let cases = [
        (
            Disambiguation::Compatible,
            "2024-03-10T07:30:00Z",
            "2024-11-03T05:30:00Z",
        ),
        (
            Disambiguation::Earlier,
            "2024-03-10T06:30:00Z",
            "2024-11-03T05:30:00Z",
        ),
        (
            Disambiguation::Later,
            "2024-03-10T07:30:00Z",
            "2024-11-03T06:30:00Z",
        ),
    ];
    for (disambiguation, skipped_instant, repeated_instant) in cases {
        let resolving = parser("%F %T", Some(("America/New_York", disambiguation)));
        assert_eq!(
            read(&resolving, skipped),
            Ok(nanoseconds(skipped_instant)),
            "{disambiguation}"
        );
        assert_eq!(
            read(&resolving, repeated),
            Ok(nanoseconds(repeated_instant)),
            "{disambiguation}"
        );
    }
    // Midnight did not exist in Sao Paulo when summer time began on 2018-11-04.
    assert_eq!(
        read(
            &parser("%F", Some(("America/Sao_Paulo", Disambiguation::Later))),
            "2018-11-04"
        ),
        Ok(nanoseconds("2018-11-04T03:00:00Z"))
    );
    // A text with its own offset never meets a skipped time.
    assert_eq!(
        read(&parser("%F %T %:z", None), "2024-03-10 02:30:00 -05:00"),
        Ok(nanoseconds("2024-03-10T07:30:00Z"))
    );
}

#[test]
fn columns_with_null_and_failed_lanes_read_every_other_lane() {
    let texts = StringArray::from(vec![
        Some("2024-07-04 12:30:00"),
        None,
        Some("2024/07/04 12:30:00"),
        Some("2024-11-03 01:30:00"),
        Some("2024-07-05 00:00:00"),
        Some("2262-04-12 00:00:00"),
    ]);
    let sliced = texts.slice(1, 5);
    let reading = parser("%F %T", Some(("America/New_York", Disambiguation::Reject)));
    let parsed = parse_datetimes(&sliced, &reading);
    assert_eq!(
        parsed.column.iter().collect::<Vec<_>>(),
        [
            None,
            None,
            None,
            Some(nanoseconds("2024-07-05T04:00:00Z")),
            None
        ]
    );
    assert_eq!(parsed.column.timezone(), Some("+00:00"));
    let failures = parsed
        .failures
        .iter()
        .map(|failed| (failed.lane, failed.failure.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        failures,
        [
            (
                1,
                TextFailure::Unreadable(UnreadableText::Mismatch {
                    position: 4,
                    expected: literal("-"),
                })
            ),
            (
                2,
                TextFailure::Unresolved {
                    local_time: UnresolvedLocalTime::Repeated,
                    zone: zone("America/New_York"),
                }
            ),
            (4, TextFailure::OutOfRange),
        ]
    );
}

#[test]
fn unreadable_texts_explain_where_and_why_without_quoting_the_text() {
    use crate::datetime::DatetimeField;

    let messages = [
        (
            UnreadableText::Mismatch {
                position: 4,
                expected: literal("-"),
            },
            "input does not match its format at byte 4: expected '-'",
        ),
        (
            UnreadableText::Mismatch {
                position: 21,
                expected: TextExpectation::Directive(FormatDirective::Offset(OffsetStyle::Compact)),
            },
            "input does not match its format at byte 21: expected '%z'",
        ),
        (
            UnreadableText::Mismatch {
                position: 0,
                expected: literal("\t"),
            },
            "input does not match its format at byte 0: expected '\\t'",
        ),
        (
            UnreadableText::TrailingText { position: 19 },
            "input continues past its format at byte 19",
        ),
        (
            UnreadableText::FieldOutOfRange(DatetimeField::Hour),
            "hour is out of range",
        ),
        (UnreadableText::NonexistentDate, "date does not exist"),
        (
            UnreadableText::InconsistentField(DatetimeField::Weekday),
            "day of the week does not match the date",
        ),
    ];
    for (reason, message) in messages {
        assert_eq!(reason.to_string(), message);
    }
}
