//! Tests for compiling datetime formats.
//!
//! Layer: test harness.
//!
//! - **Owns.** The items every directive compiles to, the positions unknown and incomplete
//!   directives are reported at, the longest value a format describes and the limit it is held to,
//!   the fields a readable format must read, and how a parser's format and zone must agree.
//! - **Depends on.** The format compiler and the time zones it measures zone directives in.
//! - **Must not know.** Arrow arrays, kernels, or row errors.

use super::{
    DatetimeField, DatetimeFormat, DatetimeParser, FormatDefect, FormatItem, ParseFormat,
    ParserZoneMismatch, ReadItem, compile_items,
};
use crate::program::{Disambiguation, Zone};

/// The items of a written format, each written back as its literal text or its directive.
fn written_items(written: &str) -> Vec<String> {
    compile_items(written)
        .unwrap_or_else(|defect| panic!("`{written}` compiles: {defect}"))
        .iter()
        .map(|item| match item {
            FormatItem::Literal(text) => format!("literal {text:?}"),
            FormatItem::Field(directive) => directive.to_string(),
            FormatItem::Zone(directive) => directive.to_string(),
        })
        .collect()
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

#[test]
fn every_directive_compiles_to_its_own_item() {
    let directives = [
        "%Y", "%G", "%m", "%-m", "%b", "%B", "%d", "%-d", "%e", "%j", "%-j", "%a", "%A", "%u",
        "%w", "%V", "%-V", "%H", "%-H", "%I", "%-I", "%p", "%M", "%-M", "%S", "%-S", "%f", "%1f",
        "%3f", "%6f", "%.f", "%z", "%:z", "%::z", "%Z", "%Q", "%s",
    ];
    for directive in directives {
        assert_eq!(
            written_items(directive),
            [directive.to_string()],
            "{directive}"
        );
    }
    // `%9f` is `%f`.
    assert_eq!(written_items("%9f"), ["%f"]);
}

#[test]
fn shorthands_and_literals_join_into_one_sequence() {
    assert_eq!(
        written_items("%FT%T%:z"),
        [
            "%Y",
            "literal \"-\"",
            "%m",
            "literal \"-\"",
            "%d",
            "literal \"T\"",
            "%H",
            "literal \":\"",
            "%M",
            "literal \":\"",
            "%S",
            "%:z"
        ]
    );
    assert_eq!(written_items("%R"), ["%H", "literal \":\"", "%M"]);
    assert_eq!(
        written_items("at %% %H%nand%tthen"),
        ["literal \"at % \"", "%H", "literal \"\\nand\\tthen\""]
    );
    assert_eq!(
        written_items("Uhrzeit: %H Uhr ✓"),
        ["literal \"Uhrzeit: \"", "%H", "literal \" Uhr ✓\""]
    );
    assert_eq!(
        written_items("no directives"),
        ["literal \"no directives\""]
    );
    assert!(written_items("").is_empty());
}

#[test]
fn unknown_and_incomplete_directives_name_their_position() {
    let defects = [
        (
            "%Y-%q",
            FormatDefect::UnknownDirective {
                directive: "%q".to_string(),
                position: 3,
            },
        ),
        (
            "%d.%m.%y",
            FormatDefect::UnknownDirective {
                directive: "%y".to_string(),
                position: 6,
            },
        ),
        (
            "%-e",
            FormatDefect::UnknownDirective {
                directive: "%-e".to_string(),
                position: 0,
            },
        ),
        (
            "%.3f",
            FormatDefect::UnknownDirective {
                directive: "%.3".to_string(),
                position: 0,
            },
        ),
        (
            "%0f",
            FormatDefect::UnknownDirective {
                directive: "%0".to_string(),
                position: 0,
            },
        ),
        (
            "%12f",
            FormatDefect::UnknownDirective {
                directive: "%12".to_string(),
                position: 0,
            },
        ),
        (
            "%:::z",
            FormatDefect::UnknownDirective {
                directive: "%:::".to_string(),
                position: 0,
            },
        ),
        (
            "x%é",
            FormatDefect::UnknownDirective {
                directive: "%é".to_string(),
                position: 1,
            },
        ),
        (
            "%c",
            FormatDefect::UnknownDirective {
                directive: "%c".to_string(),
                position: 0,
            },
        ),
        ("%H:%", FormatDefect::IncompleteDirective { position: 3 }),
        ("%-", FormatDefect::IncompleteDirective { position: 0 }),
        ("%:", FormatDefect::IncompleteDirective { position: 0 }),
        ("%::", FormatDefect::IncompleteDirective { position: 0 }),
        ("%3", FormatDefect::IncompleteDirective { position: 0 }),
    ];
    for (written, defect) in defects {
        assert_eq!(compile_items(written), Err(defect.clone()), "{written}");
        assert_eq!(
            DatetimeFormat::compile(written, &Zone::UTC),
            Err(defect.clone())
        );
        assert_eq!(ParseFormat::compile(written), Err(defect), "{written}");
    }
}

#[test]
fn formats_describe_values_up_to_the_formatted_value_limit() {
    let cases = [
        ("%Y-%m-%dT%H:%M:%S%.f%:z", "UTC", 38),
        ("%A, %d %B %Y", "UTC", 28),
        ("%s", "UTC", 20),
        ("%Z", "UTC", 3),
        ("%Z %Q", "+05:30", 13),
        ("%Q", "America/Argentina/ComodRivadavia", 32),
        ("%Z", "America/New_York", 3),
    ];
    for (written, zone_name, longest) in cases {
        let format = DatetimeFormat::compile(written, &zone(zone_name))
            .unwrap_or_else(|defect| panic!("`{written}` compiles: {defect}"));
        assert_eq!(
            format.longest_value(&zone(zone_name)),
            longest,
            "{written} in {zone_name}"
        );
    }

    let at_limit = "x".repeat(256);
    assert!(DatetimeFormat::compile(&at_limit, &Zone::UTC).is_ok());
    let past_limit = "%B".repeat(30);
    assert_eq!(
        DatetimeFormat::compile(&past_limit, &Zone::UTC),
        Err(FormatDefect::TooLong { longest: 270 })
    );
    let names = format!("{}%Q", "x".repeat(230));
    assert!(DatetimeFormat::compile(&names, &Zone::UTC).is_ok());
    assert_eq!(
        DatetimeFormat::compile(&names, &zone("America/Argentina/ComodRivadavia")),
        Err(FormatDefect::TooLong { longest: 262 })
    );
    assert_eq!(
        ParseFormat::compile("%F %Y"),
        Err(FormatDefect::RepeatedField(DatetimeField::Year))
    );
    assert_eq!(
        ParseFormat::compile(&format!("%F{}", "x".repeat(247))),
        Err(FormatDefect::TooLong { longest: 257 })
    );
    assert!(ParseFormat::compile(&format!("%F{}", "x".repeat(246))).is_ok());
}

#[test]
fn readable_formats_name_one_instant() {
    let readable = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S%.f",
        "%F %T %z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%A %B %e %Y %I:%M %p",
        "%Y-%j",
        "%u %Y-%j",
        "%G-W%V-%u",
        "%G-W%V-%a %H",
        "%s",
        "%s.%f",
        "%s%.f",
        "%-d/%-m/%Y %-H:%-M:%-S",
        "%d/%b/%Y:%H:%M:%S %z",
    ];
    for written in readable {
        assert!(
            ParseFormat::compile(written).is_ok(),
            "`{written}` is readable: {:?}",
            ParseFormat::compile(written)
        );
    }

    let unreadable = [
        (
            "%F %T %Z",
            FormatDefect::Unreadable(super::ZoneDirective::Abbreviation),
        ),
        (
            "%F %T %Q",
            FormatDefect::Unreadable(super::ZoneDirective::Name),
        ),
        (
            "%d %e %m %Y",
            FormatDefect::RepeatedField(DatetimeField::Day),
        ),
        (
            "%a %u %F",
            FormatDefect::RepeatedField(DatetimeField::Weekday),
        ),
        (
            "%m %b %Y %d",
            FormatDefect::RepeatedField(DatetimeField::Month),
        ),
        (
            "%F %.f %f",
            FormatDefect::RepeatedField(DatetimeField::Fraction),
        ),
        (
            "%F %z %:z",
            FormatDefect::RepeatedField(DatetimeField::Offset),
        ),
        (
            "%s %Y",
            FormatDefect::ConflictingFields {
                first: DatetimeField::UnixTime,
                second: DatetimeField::Year,
            },
        ),
        (
            "%s %z",
            FormatDefect::ConflictingFields {
                first: DatetimeField::UnixTime,
                second: DatetimeField::Offset,
            },
        ),
        (
            "%Y-%j-%m",
            FormatDefect::ConflictingFields {
                first: DatetimeField::DayOfYear,
                second: DatetimeField::Month,
            },
        ),
        (
            "%G-%V-%u %Y",
            FormatDefect::ConflictingFields {
                first: DatetimeField::IsoYear,
                second: DatetimeField::Year,
            },
        ),
        (
            "%F %H %I %p",
            FormatDefect::ConflictingFields {
                first: DatetimeField::Hour,
                second: DatetimeField::TwelveHour,
            },
        ),
        ("%Y-%m", FormatDefect::IncompleteDate),
        ("%H:%M", FormatDefect::IncompleteDate),
        ("%m-%d", FormatDefect::IncompleteDate),
        ("%j", FormatDefect::IncompleteDate),
        ("%G-%V", FormatDefect::IncompleteDate),
        ("%V-%u", FormatDefect::IncompleteDate),
        ("", FormatDefect::IncompleteDate),
        (
            "%F %I:%M",
            FormatDefect::MissingField {
                field: DatetimeField::TwelveHour,
                required: DatetimeField::Meridiem,
            },
        ),
        (
            "%F %H %p",
            FormatDefect::MissingField {
                field: DatetimeField::Meridiem,
                required: DatetimeField::TwelveHour,
            },
        ),
        (
            "%F %M",
            FormatDefect::MissingField {
                field: DatetimeField::Minute,
                required: DatetimeField::Hour,
            },
        ),
        (
            "%F %H %S",
            FormatDefect::MissingField {
                field: DatetimeField::Second,
                required: DatetimeField::Minute,
            },
        ),
        (
            "%F %H:%M%.f",
            FormatDefect::MissingField {
                field: DatetimeField::Fraction,
                required: DatetimeField::Second,
            },
        ),
    ];
    for (written, defect) in unreadable {
        assert_eq!(ParseFormat::compile(written), Err(defect), "{written}");
    }
    // Every format writes, whether or not it can be read.
    for written in ["%H:%M", "%Z", "%s %Y", "%F %H %I %p", ""] {
        assert!(
            DatetimeFormat::compile(written, &Zone::UTC).is_ok(),
            "{written}"
        );
    }
}

#[test]
fn a_parser_takes_a_zone_exactly_when_its_format_reads_local_times() {
    let with_zone = || Some((zone("Europe/Berlin"), Disambiguation::Reject));
    let compiled = |written: &str| {
        ParseFormat::compile(written).unwrap_or_else(|defect| panic!("`{written}`: {defect}"))
    };

    assert!(DatetimeParser::new(compiled("%F %T"), with_zone()).is_ok());
    assert!(DatetimeParser::new(compiled("%F %T %z"), None).is_ok());
    assert!(DatetimeParser::new(compiled("%s"), None).is_ok());
    assert_eq!(
        DatetimeParser::new(compiled("%F %T"), None),
        Err(ParserZoneMismatch::MissingZone)
    );
    assert_eq!(
        DatetimeParser::new(compiled("%F %T %:z"), with_zone()),
        Err(ParserZoneMismatch::ZoneWithOffset)
    );
    assert_eq!(
        DatetimeParser::new(compiled("%s%.f"), with_zone()),
        Err(ParserZoneMismatch::ZoneWithUnixTime)
    );

    let parser = DatetimeParser::new(compiled("%F"), with_zone()).expect("a zoned parser");
    let items = parser
        .items
        .iter()
        .filter(|item| matches!(item, ReadItem::Field(_)))
        .count();
    assert_eq!(items, 3);
}

#[test]
fn format_defects_explain_what_the_format_lacks() {
    let messages = [
        (
            FormatDefect::UnknownDirective {
                directive: "%q".to_string(),
                position: 3,
            },
            "unknown directive '%q' at byte 3",
        ),
        (
            FormatDefect::IncompleteDirective { position: 5 },
            "incomplete directive at byte 5",
        ),
        (
            FormatDefect::Unreadable(super::ZoneDirective::Abbreviation),
            "'%Z' can be written but not read",
        ),
        (
            FormatDefect::RepeatedField(DatetimeField::Day),
            "it reads the day of the month more than once",
        ),
        (
            FormatDefect::ConflictingFields {
                first: DatetimeField::UnixTime,
                second: DatetimeField::Offset,
            },
            "it reads the Unix time together with the UTC offset",
        ),
        (
            FormatDefect::IncompleteDate,
            "it does not read a complete date",
        ),
        (
            FormatDefect::MissingField {
                field: DatetimeField::TwelveHour,
                required: DatetimeField::Meridiem,
            },
            "it reads the hour of the 12-hour clock without the AM or PM marker",
        ),
        (
            FormatDefect::TooLong { longest: 270 },
            "its values can be up to 270 bytes long, longer than the 256-byte limit",
        ),
    ];
    for (defect, message) in messages {
        assert_eq!(defect.to_string(), message);
    }
}
