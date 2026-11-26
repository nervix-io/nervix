//! Tests for lowering calendar, time zone, format and duration datetime calls.
//!
//! Layer: test harness.
//!
//! - **Owns.** The units, zones, compiled formats, parsers and bin widths a datetime call lowers
//!   its literal arguments into, the row-valued arguments it keeps, and the typed failure of every
//!   literal argument that does not select a computation, with the messages that name what a call
//!   accepts.
//! - **Depends on.** The VM frontend, the Plexus expression parser, and the compiled datetime
//!   constants.
//! - **Must not know.** Compilation, execution, or registers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{DurationValue, Expression as ModelExpression};

use super::{
    ArgumentCount, DatetimeLiteral, FrontendErrorKind, FrontendResult, SemanticScopePolicy,
    lower_expression,
};
use crate::{
    datetime::{
        DatetimeFormat, DatetimeParser, FormatDefect, ParseFormat, TextExpectation, UnreadableText,
    },
    program::{
        CalendarUnit, DateBinWidth, DatePart, DatetimeFunction, DatetimeFunctionName, DatetimeUnit,
        Disambiguation, DurationUnit, Expr, FieldRef, FixedTimeUnit, FunctionName, SpannedExpr,
        UnitRounding, Zone,
    },
};

fn expression(source: &str) -> ModelExpression {
    nervix_plexus::parse_expression(source).assured("test expressions are valid Plexus")
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

fn lowered(source: &str) -> (DatetimeFunction, Vec<String>) {
    let lowered = lower_expression(&expression(source), SemanticScopePolicy::read_only("input"))
        .unwrap_or_else(|error| panic!("`{source}` lowers: {error:?}"));
    let Expr::Call {
        function: FunctionName::Datetime(function),
        args,
    } = lowered.inner
    else {
        panic!("`{source}` lowers to a datetime call");
    };
    (function, args.iter().map(input_field).collect())
}

fn input_field(expr: &SpannedExpr) -> String {
    let Expr::FieldRef(FieldRef { relay, field }) = &expr.inner else {
        panic!("expected an input field reference, found {expr:?}");
    };
    assert_eq!(relay, "input");
    field.clone()
}

fn assert_failure<T: std::fmt::Debug>(result: FrontendResult<T>, expected: &FrontendErrorKind) {
    let error = result.expect_err("the call must be rejected");
    assert_eq!(&error.current_context().kind, expected);
}

#[test]
fn calendar_and_zone_calls_resolve_their_literals_when_lowered() {
    let calls = [
        (
            "date_part('hour', input.occurred_at, 'america/new_york')",
            DatetimeFunction::DatePart {
                part: DatePart::Hour,
                zone: zone("America/New_York"),
            },
            vec!["occurred_at"],
        ),
        (
            "DATE_TRUNC('Month', input.occurred_at)",
            DatetimeFunction::DateTrunc {
                unit: DatetimeUnit::Calendar(CalendarUnit::Month),
                zone: Zone::UTC,
            },
            vec!["occurred_at"],
        ),
        (
            "date_trunc('day', input.occurred_at, '+05:30')",
            DatetimeFunction::DateTrunc {
                unit: DatetimeUnit::Fixed(FixedTimeUnit::Day),
                zone: zone("+05:30"),
            },
            vec!["occurred_at"],
        ),
        (
            "date_add('quarter', input.amount, input.occurred_at, 'Europe/Berlin')",
            DatetimeFunction::DateAdd {
                unit: DatetimeUnit::Calendar(CalendarUnit::Quarter),
                zone: zone("Europe/Berlin"),
            },
            vec!["amount", "occurred_at"],
        ),
        (
            "date_diff('year', input.born_at, input.occurred_at, 'UTC')",
            DatetimeFunction::DateDiff {
                unit: DatetimeUnit::Calendar(CalendarUnit::Year),
                zone: Zone::UTC,
            },
            vec!["born_at", "occurred_at"],
        ),
        (
            "format_datetime('%F %Z', input.occurred_at, 'Asia/Tokyo')",
            DatetimeFunction::FormatDatetime {
                format: DatetimeFormat::compile("%F %Z", &zone("Asia/Tokyo")).assured("valid"),
                zone: zone("Asia/Tokyo"),
            },
            vec!["occurred_at"],
        ),
        (
            "format_datetime('%s', input.occurred_at)",
            DatetimeFunction::FormatDatetime {
                format: DatetimeFormat::compile("%s", &Zone::UTC).assured("valid"),
                zone: Zone::UTC,
            },
            vec!["occurred_at"],
        ),
        (
            "parse_datetime('%F %T', input.text, 'America/New_York', 'Later')",
            DatetimeFunction::ParseDatetime(
                DatetimeParser::new(
                    ParseFormat::compile("%F %T").assured("valid"),
                    Some((zone("America/New_York"), Disambiguation::Later)),
                )
                .assured("a local time format takes a zone"),
            ),
            vec!["text"],
        ),
        (
            "parse_datetime('%F %T', input.text, 'America/New_York')",
            DatetimeFunction::ParseDatetime(
                DatetimeParser::new(
                    ParseFormat::compile("%F %T").assured("valid"),
                    Some((zone("America/New_York"), Disambiguation::Reject)),
                )
                .assured("a local time format takes a zone"),
            ),
            vec!["text"],
        ),
        (
            "parse_datetime('%FT%T%:z', input.text)",
            DatetimeFunction::ParseDatetime(
                DatetimeParser::new(ParseFormat::compile("%FT%T%:z").assured("valid"), None)
                    .assured("an offset format takes no zone"),
            ),
            vec!["text"],
        ),
    ];
    for (source, expected, fields) in calls {
        let (function, args) = lowered(source);
        assert_eq!(function, expected, "{source}");
        assert_eq!(args, fields, "{source}");
    }

    // A call without a zone and a call naming UTC compute the same thing.
    assert_eq!(
        lowered("date_part('hour', input.occurred_at)").0,
        lowered("date_part('hour', input.occurred_at, 'UTC')").0
    );
    assert_ne!(
        lowered("format_datetime('%Q', input.occurred_at, 'UTC')").0,
        lowered("format_datetime('%Q', input.occurred_at, '+00:00')").0
    );
}

#[test]
fn calendar_zone_and_format_literal_failures_have_semantic_contexts() {
    let failures = [
        (
            "date_part('hour', input.occurred_at, 'Mars/Olympus_Mons')",
            FrontendErrorKind::UnknownTimeZone {
                function: DatetimeFunctionName::DatePart,
                zone: "Mars/Olympus_Mons".to_string(),
            },
        ),
        (
            "date_trunc('day', input.occurred_at, input.zone)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateTrunc,
                argument: DatetimeLiteral::TimeZone,
            },
        ),
        (
            "date_diff('day', input.start, input.finish, NULL)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateDiff,
                argument: DatetimeLiteral::TimeZone,
            },
        ),
        (
            "date_bin('month', 1, input.occurred_at, input.origin)",
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DateBin,
                unit: "month".to_string(),
            },
        ),
        (
            "to_unix('year', input.occurred_at)",
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::ToUnix,
                unit: "year".to_string(),
            },
        ),
        (
            "date_add('month', 1, input.occurred_at, 'UTC', 'later')",
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::DateAdd,
                expected: ArgumentCount { fewest: 3, most: 4 },
                found: 5,
            },
        ),
        (
            "format_datetime(input.pattern, input.occurred_at)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::FormatDatetime,
                argument: DatetimeLiteral::Format,
            },
        ),
        (
            "format_datetime('%Y-%q', input.occurred_at)",
            FrontendErrorKind::InvalidDatetimeFormat {
                function: DatetimeFunctionName::FormatDatetime,
                format: "%Y-%q".to_string(),
                defect: FormatDefect::UnknownDirective {
                    directive: "%q".to_string(),
                    position: 3,
                },
            },
        ),
        (
            "parse_datetime('%H:%M', input.text, 'UTC')",
            FrontendErrorKind::InvalidDatetimeFormat {
                function: DatetimeFunctionName::ParseDatetime,
                format: "%H:%M".to_string(),
                defect: FormatDefect::IncompleteDate,
            },
        ),
        (
            "parse_datetime('%FT%T%:z', input.text, 'UTC')",
            FrontendErrorKind::ParseFormatWithOffsetAndZone {
                format: "%FT%T%:z".to_string(),
            },
        ),
        (
            "parse_datetime('%s', input.text, 'UTC')",
            FrontendErrorKind::ParseFormatWithUnixTimeAndZone {
                format: "%s".to_string(),
            },
        ),
        (
            "parse_datetime('%F %T', input.text)",
            FrontendErrorKind::ParseFormatWithoutZone {
                format: "%F %T".to_string(),
            },
        ),
        (
            "parse_datetime('%F %T', input.text, 'America/New_York', 'first')",
            FrontendErrorKind::UnknownDisambiguation {
                disambiguation: "first".to_string(),
            },
        ),
        (
            "parse_datetime('%F %T', input.text, 'America/New_York', input.choice)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::ParseDatetime,
                argument: DatetimeLiteral::Disambiguation,
            },
        ),
        (
            "parse_datetime('%F')",
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::ParseDatetime,
                expected: ArgumentCount { fewest: 2, most: 4 },
                found: 1,
            },
        ),
    ];
    for (source, kind) in failures {
        assert_failure(
            lower_expression(&expression(source), SemanticScopePolicy::read_only("input")),
            &kind,
        );
    }
}

#[test]
fn calendar_zone_and_format_failures_name_what_the_call_accepts() {
    let messages = [
        (
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DateBin,
                unit: "month".to_string(),
            },
            "function 'date_bin' does not accept time unit 'month'; expected one of nanosecond, \
             microsecond, millisecond, second, minute, hour, day, week",
        ),
        (
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DateAdd,
                unit: "fortnight".to_string(),
            },
            "function 'date_add' does not accept time unit 'fortnight'; expected one of \
             nanosecond, microsecond, millisecond, second, minute, hour, day, week, month, \
             quarter, year",
        ),
        (
            FrontendErrorKind::UnknownTimeZone {
                function: DatetimeFunctionName::DateTrunc,
                zone: "Mars/Olympus_Mons".to_string(),
            },
            "function 'date_trunc' does not accept time zone 'Mars/Olympus_Mons'; expected an \
             IANA time zone name, UTC, or a UTC offset such as '+05:30'",
        ),
        (
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateTrunc,
                argument: DatetimeLiteral::TimeZone,
            },
            "function 'date_trunc' requires its time zone to be a STRING literal",
        ),
        (
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::FormatDatetime,
                argument: DatetimeLiteral::Format,
            },
            "function 'format_datetime' requires its format to be a STRING literal",
        ),
        (
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::ParseDatetime,
                argument: DatetimeLiteral::Disambiguation,
            },
            "function 'parse_datetime' requires its disambiguation to be a STRING literal",
        ),
        (
            FrontendErrorKind::InvalidDatetimeFormat {
                function: DatetimeFunctionName::FormatDatetime,
                format: "%Y-%q".to_string(),
                defect: FormatDefect::UnknownDirective {
                    directive: "%q".to_string(),
                    position: 3,
                },
            },
            "function 'format_datetime' does not accept format '%Y-%q': unknown directive '%q' at \
             byte 3",
        ),
        (
            FrontendErrorKind::ParseFormatWithOffsetAndZone {
                format: "%FT%T%:z".to_string(),
            },
            "function 'parse_datetime' format '%FT%T%:z' reads its UTC offset from the input, so \
             it takes no time zone",
        ),
        (
            FrontendErrorKind::ParseFormatWithUnixTimeAndZone {
                format: "%s".to_string(),
            },
            "function 'parse_datetime' format '%s' reads a Unix time from the input, so it takes \
             no time zone",
        ),
        (
            FrontendErrorKind::ParseFormatWithoutZone {
                format: "%F %T".to_string(),
            },
            "function 'parse_datetime' format '%F %T' reads no UTC offset or Unix time from the \
             input, so it requires a time zone",
        ),
        (
            FrontendErrorKind::UnknownDisambiguation {
                disambiguation: "first".to_string(),
            },
            "function 'parse_datetime' does not accept disambiguation 'first'; expected one of \
             compatible, earlier, later, reject",
        ),
        (
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::DatePart,
                expected: ArgumentCount { fewest: 2, most: 3 },
                found: 1,
            },
            "function 'date_part' expects 2 or 3 arguments, found 1",
        ),
        (
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::ParseDatetime,
                expected: ArgumentCount { fewest: 2, most: 4 },
                found: 5,
            },
            "function 'parse_datetime' expects 2 to 4 arguments, found 5",
        ),
    ];
    for (kind, message) in messages {
        assert_eq!(kind.to_string(), message);
    }

    // Row failures of `parse_datetime` quote positions and directives, never the text.
    let unreadable = UnreadableText::Mismatch {
        position: 4,
        expected: TextExpectation::Literal(nervix_primitives::sync::Arc::from("-")),
    };
    assert_eq!(
        crate::SideErrorReason::UnreadableDatetime(unreadable).to_string(),
        "parse_datetime input does not match its format at byte 4: expected '-'"
    );
}

fn bin_width(count: u64, unit: FixedTimeUnit) -> DateBinWidth {
    let count = NonZeroU64::new(count).assured("test widths are positive");
    DateBinWidth::new(count, unit).assured("test widths fit i64 nanoseconds")
}

#[test]
fn duration_conversions_and_a_duration_bin_width_resolve_their_literals_when_lowered() {
    let calls = [
        (
            "duration_floor('second', input.wait)",
            DatetimeFunction::DurationUnits {
                unit: DurationUnit::Second,
                rounding: UnitRounding::Floor,
            },
            vec!["wait"],
        ),
        (
            "DURATION_CEIL('Minute', input.wait)",
            DatetimeFunction::DurationUnits {
                unit: DurationUnit::Minute,
                rounding: UnitRounding::Ceil,
            },
            vec!["wait"],
        ),
        (
            "duration_round('day', input.wait)",
            DatetimeFunction::DurationUnits {
                unit: DurationUnit::Day,
                rounding: UnitRounding::Round,
            },
            vec!["wait"],
        ),
        (
            "to_duration('nanosecond', input.count)",
            DatetimeFunction::ToDuration(DurationUnit::Nanosecond),
            vec!["count"],
        ),
        (
            "date_bin(15m, input.occurred_at, input.origin)",
            DatetimeFunction::DateBin(bin_width(900_000_000_000, FixedTimeUnit::Nanosecond)),
            vec!["occurred_at", "origin"],
        ),
        (
            "date_bin(1h 30m, input.occurred_at, input.origin)",
            DatetimeFunction::DateBin(bin_width(5_400_000_000_000, FixedTimeUnit::Nanosecond)),
            vec!["occurred_at", "origin"],
        ),
        (
            "date_bin('minute', 15, input.occurred_at, input.origin)",
            DatetimeFunction::DateBin(bin_width(15, FixedTimeUnit::Minute)),
            vec!["occurred_at", "origin"],
        ),
    ];
    for (source, function, args) in calls {
        let args = args.into_iter().map(str::to_string).collect::<Vec<_>>();
        assert_eq!(lowered(source), (function, args), "{source}");
    }
    // Both forms of a width bin identically, whichever way the width is written.
    assert_eq!(
        bin_width(900_000_000_000, FixedTimeUnit::Nanosecond).nanoseconds(),
        bin_width(15, FixedTimeUnit::Minute).nanoseconds()
    );
}

#[test]
fn duration_conversion_and_duration_width_failures_have_semantic_contexts() {
    let failures = [
        (
            "duration_floor('week', input.wait)",
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DurationFloor,
                unit: "week".to_string(),
            },
        ),
        (
            "duration_round('seconds', input.wait)",
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DurationRound,
                unit: "seconds".to_string(),
            },
        ),
        (
            "to_duration('month', input.count)",
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::ToDuration,
                unit: "month".to_string(),
            },
        ),
        (
            "duration_ceil(input.unit, input.wait)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DurationCeil,
                argument: DatetimeLiteral::TimeUnit,
            },
        ),
        (
            "to_duration('second')",
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::ToDuration,
                expected: ArgumentCount { fewest: 2, most: 2 },
                found: 1,
            },
        ),
        (
            "duration_floor('second', input.wait, 'UTC')",
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::DurationFloor,
                expected: ArgumentCount { fewest: 2, most: 2 },
                found: 3,
            },
        ),
        (
            "date_bin(0s, input.occurred_at, input.origin)",
            FrontendErrorKind::NonPositiveDateBinDuration {
                width: DurationValue::ZERO,
            },
        ),
        (
            "date_bin(-90s, input.occurred_at, input.origin)",
            FrontendErrorKind::NonPositiveDateBinDuration {
                width: DurationValue::from_nanoseconds(-90_000_000_000),
            },
        ),
        (
            "date_bin(input.width, input.occurred_at, input.origin)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateBin,
                argument: DatetimeLiteral::DurationWidth,
            },
        ),
        (
            "date_bin(15, input.occurred_at, input.origin)",
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateBin,
                argument: DatetimeLiteral::DurationWidth,
            },
        ),
        (
            "date_bin(input.occurred_at, input.origin)",
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::DateBin,
                expected: ArgumentCount { fewest: 3, most: 4 },
                found: 2,
            },
        ),
    ];
    for (source, kind) in failures {
        assert_failure(
            lower_expression(&expression(source), SemanticScopePolicy::read_only("input")),
            &kind,
        );
    }
}

#[test]
fn duration_conversion_and_duration_width_failures_name_what_the_call_accepts() {
    let messages = [
        (
            FrontendErrorKind::UnknownTimeUnit {
                function: DatetimeFunctionName::DurationFloor,
                unit: "week".to_string(),
            },
            "function 'duration_floor' does not accept time unit 'week'; expected one of \
             nanosecond, microsecond, millisecond, second, minute, hour, day",
        ),
        (
            FrontendErrorKind::NonPositiveDateBinDuration {
                width: DurationValue::from_nanoseconds(-90_000_000_000),
            },
            "function 'date_bin' requires a positive width, found -1m 30s",
        ),
        (
            FrontendErrorKind::NonLiteralDatetimeArgument {
                function: DatetimeFunctionName::DateBin,
                argument: DatetimeLiteral::DurationWidth,
            },
            "function 'date_bin' requires its width to be a DURATION literal",
        ),
        (
            FrontendErrorKind::DatetimeArity {
                function: DatetimeFunctionName::DateBin,
                expected: ArgumentCount { fewest: 3, most: 4 },
                found: 2,
            },
            "function 'date_bin' expects 3 or 4 arguments, found 2",
        ),
    ];
    for (kind, message) in messages {
        assert_eq!(kind.to_string(), message);
    }
}
