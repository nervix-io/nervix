//! Duration text as Nervix reads it, without the overflow its grammar's library panics on.
//!
//! A duration is one or more spans, each a whole or fractional number and a unit, as `humantime`
//! reads them. `humantime` 2.4 carries a fractional second into the seconds only once the
//! fractions exceed one second, so spans that add up to the last second a `Duration` holds and
//! fractions of exactly one more second make `Duration::new` overflow and panic. Every duration
//! Nervix reads is far shorter, so text is refused as too long, before `humantime` reads it, whenever
//! an upper bound on what it names reaches that second.
//!
//! [`parse_duration_text`] is the only reader of duration text in Nervix: NSPL literals, Model
//! settings, node command-line values and harness inputs all go through it. Clippy rejects a direct
//! call to `humantime::parse_duration` and every use of `humantime::Duration`, whose `FromStr`
//! reads through the same unguarded parser.

use std::time::Duration;

use error_stack::Report;
use thiserror::Error;

/// Why duration text names no duration.
///
/// A variant describes only the reason. Whoever reads the text names it, and the setting it
/// belongs to, in its own diagnostic.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum DurationTextError {
    /// The text does not follow the duration grammar.
    #[error("{reason}")]
    Malformed { reason: humantime::DurationError },
    /// The spans could name more seconds than a duration holds.
    #[error("it is longer than a duration can be")]
    TooLong,
}

/// Nanoseconds in the most seconds a `Duration` holds. Text that could name this many is refused.
const LONGEST_NANOS: u128 = Duration::from_secs(u64::MAX).as_nanos();

/// Nanoseconds in one of the largest unit the grammar has, a year of 365.25 days.
const YEAR_NANOS: u128 = 31_557_600_000_000_000;

/// Reads duration text: one or more spans such as `250ms`, `1h 30m` or `1.5s`.
///
/// Text within one unit per span of the most seconds a `Duration` holds is refused as too long
/// even where it would fit, since the bound that rules the overflow out errs high. No such span
/// is anywhere near a duration Nervix can hold.
pub fn parse_duration_text(text: &str) -> Result<Duration, Report<DurationTextError>> {
    let within_bound = match SpanBound::of(text) {
        Some(bound) => bound < LONGEST_NANOS,
        None => false,
    };
    if !within_bound {
        return Err(Report::new(DurationTextError::TooLong));
    }
    read_admitted(text)
}

/// Reads text the span bound admitted. This is the one place `humantime` reads duration text.
#[expect(
    clippy::disallowed_methods,
    reason = "parse_duration_text admits only text whose span bound rules out humantime's overflow"
)]
fn read_admitted(text: &str) -> Result<Duration, Report<DurationTextError>> {
    humantime::parse_duration(text)
        .map_err(|reason| Report::new(DurationTextError::Malformed { reason }))
}

/// An upper bound, in nanoseconds, on what duration text names, gathered one span at a time.
///
/// Each span contributes one more of its unit than its whole number, which covers any fraction,
/// and a unit the grammar does not know counts as a year, the largest it has. Text the grammar
/// rejects part of the way through still has every span counted, so the bound only ever errs
/// high.
#[derive(Default)]
struct SpanBound {
    total: u128,
    whole: Option<u128>,
    in_fraction: bool,
    unit: String,
}

impl SpanBound {
    /// The bound for `text`, or nothing when it does not fit in 128 bits.
    fn of(text: &str) -> Option<u128> {
        let mut bound = Self::default();
        for character in text.chars() {
            if let Some(digit) = character.to_digit(10) {
                if !bound.unit.is_empty() {
                    bound.close_span()?;
                }
                if !bound.in_fraction {
                    let whole = bound.whole.unwrap_or(0);
                    let whole = whole.checked_mul(10)?.checked_add(u128::from(digit))?;
                    bound.whole = Some(whole);
                }
            } else if character == '.' {
                bound.in_fraction = true;
            } else if character.is_whitespace() {
                if !bound.unit.is_empty() {
                    bound.close_span()?;
                }
            } else {
                bound.unit.push(character);
            }
        }
        bound.close_span()?;
        Some(bound.total)
    }

    /// Adds the span read so far and starts the next one.
    fn close_span(&mut self) -> Option<()> {
        if let Some(whole) = self.whole {
            let unit_nanos = Self::unit_nanos(&self.unit);
            let span = whole.checked_add(1)?.checked_mul(unit_nanos)?;
            self.total = self.total.checked_add(span)?;
        }
        self.whole = None;
        self.in_fraction = false;
        self.unit.clear();
        Some(())
    }

    /// Nanoseconds in one of `unit`, as the grammar spells its units.
    fn unit_nanos(unit: &str) -> u128 {
        match unit {
            "nanos" | "nsec" | "ns" => 1,
            "usec" | "us" | "µs" => 1_000,
            "millis" | "msec" | "ms" => 1_000_000,
            "seconds" | "second" | "secs" | "sec" | "s" => 1_000_000_000,
            "minutes" | "minute" | "min" | "mins" | "m" => 60_000_000_000,
            "hours" | "hour" | "hr" | "hrs" | "h" => 3_600_000_000_000,
            "days" | "day" | "d" => 86_400_000_000_000,
            "weeks" | "week" | "wk" | "wks" | "w" => 604_800_000_000_000,
            "months" | "month" | "M" => 2_630_016_000_000_000,
            _ => YEAR_NANOS,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{DurationTextError, parse_duration_text, read_admitted};

    /// Seconds below the longest duration within which the text `humantime` writes for a duration
    /// may be refused as too long. That text has at most one span per unit, and each span adds one
    /// of its unit to the bound: a year, a month, a day, an hour, a minute and a second, one more
    /// second for the milliseconds, microseconds and nanoseconds together, and one for the
    /// duration's own fraction of a second.
    const CANONICAL_REFUSAL_MARGIN_SECS: u64 =
        31_557_600 + 2_630_016 + 86_400 + 3_600 + 60 + 1 + 1 + 1;

    /// Seconds in a year of 365.25 days, which bounds the durations Nervix is configured with.
    const YEAR_SECS: u64 = 31_557_600;

    /// Pieces arbitrary duration text is assembled from: numbers at the edges of what a span
    /// holds, fractions, every unit spelling the grammar has and some it does not, whitespace, and
    /// characters outside the grammar.
    const PIECES: [&str; 64] = [
        "0",
        "1",
        "5",
        "9",
        "60",
        "999999999",
        "1000000000",
        "584542046090",
        "18446744073709551615",
        "18446744073709551616",
        ".",
        ".5",
        ".05",
        ".000000001",
        ".9999999999",
        "nanos",
        "nsec",
        "ns",
        "usec",
        "us",
        "µs",
        "millis",
        "msec",
        "ms",
        "seconds",
        "second",
        "secs",
        "sec",
        "s",
        "minutes",
        "minute",
        "min",
        "mins",
        "m",
        "hours",
        "hour",
        "hr",
        "hrs",
        "h",
        "days",
        "day",
        "d",
        "weeks",
        "week",
        "wk",
        "wks",
        "w",
        "months",
        "month",
        "M",
        "years",
        "year",
        "yr",
        "yrs",
        "y",
        " ",
        "\t",
        "\u{2003}",
        "x",
        "S",
        "-",
        "+",
        "_",
        "é",
    ];

    #[test]
    fn text_reads_as_the_duration_it_names() {
        for (text, duration) in [
            ("250ms", Duration::from_millis(250)),
            ("1h 30m", Duration::from_secs(5_400)),
            ("1h30m", Duration::from_secs(5_400)),
            ("1.5s", Duration::from_millis(1_500)),
            ("18446744073709551615ns", Duration::from_nanos(u64::MAX)),
            ("584y", Duration::from_secs(584 * 31_557_600)),
        ] {
            assert_eq!(
                parse_duration_text(text).ok(),
                Some(duration),
                "{text} must read"
            );
        }
    }

    #[test]
    fn spans_adding_up_to_the_last_second_are_refused_rather_than_overflowing() {
        // Each of these made `humantime` panic: the fractions reach exactly one second on top of
        // the last second a duration holds.
        for text in [
            "18446744073709551615.5s0.5s1d",
            "18446744073709551615.5s0.5s",
            "18446744073709551615.5s500000000ns",
            "18446744073709551615s 1000000000ns",
            "18446744073709551615s 400ms 0.01m",
        ] {
            let report = parse_duration_text(text).expect_err("the text is too long");
            assert_eq!(
                report.current_context(),
                &DurationTextError::TooLong,
                "{text}"
            );
            assert_eq!(
                report.current_context().to_string(),
                "it is longer than a duration can be"
            );
        }
    }

    #[test]
    fn text_outside_the_grammar_is_malformed_for_the_reason_humantime_gives() {
        for (text, reason) in [
            ("", "value was empty"),
            ("abc", "expected number at 0"),
            ("5", "time unit needed, for example 5sec or 5ms"),
            ("1.s", "invalid character at 1"),
            (
                "5 parsecs",
                "unknown time unit \"parsecs\", supported units: ns, us/µs, ms, sec, min, hours, \
                 days, weeks, months, years (and few variations)",
            ),
        ] {
            let report = parse_duration_text(text).expect_err("the text is not a duration");
            assert!(
                matches!(
                    report.current_context(),
                    DurationTextError::Malformed { .. }
                ),
                "{text}: {report:?}"
            );
            assert_eq!(report.current_context().to_string(), reason, "{text:?}");
        }
    }

    #[test]
    fn canonical_text_is_refused_only_within_the_margin_below_the_longest_duration() {
        let longest = Duration::new(u64::MAX, 999_999_999);
        let text = humantime::format_duration(longest).to_string();
        let report = parse_duration_text(&text).expect_err("the longest duration is refused");
        assert_eq!(report.current_context(), &DurationTextError::TooLong);

        let below_margin = Duration::new(u64::MAX - CANONICAL_REFUSAL_MARGIN_SECS, 999_999_999);
        let text = humantime::format_duration(below_margin).to_string();
        assert_eq!(
            parse_duration_text(&text).ok(),
            Some(below_margin),
            "{text}"
        );
    }

    /// Duration text assembled from one piece per byte.
    fn assembled_text(bytes: &[u8]) -> String {
        let mut text = String::new();
        for byte in bytes {
            let piece = PIECES[usize::from(*byte) % PIECES.len()];
            text.push_str(piece);
        }
        text
    }

    #[test]
    fn bolero_duration_text_reads_as_humantime_reads_it_or_fails_typed() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(64)
            .for_each(|bytes: &[u8]| {
                let text = assembled_text(bytes);
                // Any panic while reading fails the property: the text a guarded read refuses is
                // exactly the text it must never hand to `humantime`.
                let report = match parse_duration_text(&text) {
                    Ok(duration) => {
                        let library = read_admitted(&text).ok();
                        assert_eq!(library, Some(duration), "{text:?}");
                        return;
                    }
                    Err(report) => report,
                };
                match report.current_context() {
                    DurationTextError::Malformed { .. } => {
                        let library = read_admitted(&text)
                            .expect_err("humantime refuses the text the guarded read refused");
                        assert_eq!(
                            library.current_context(),
                            report.current_context(),
                            "{text:?}"
                        );
                    }
                    // The bound errs high, so text that would still fit may be refused; how far
                    // below the longest duration is the round-trip property's contract.
                    DurationTextError::TooLong => {}
                }
            });
    }

    /// Seconds of a generated duration, spread over the durations Nervix is configured with, the
    /// stretch below the longest duration where canonical text may be refused, and the whole
    /// range.
    fn generated_seconds(seed: u64) -> u64 {
        let offset = seed / 4;
        match seed % 4 {
            0 => offset % YEAR_SECS,
            // The remainder is below twice the margin, so the subtraction cannot underflow.
            1 => u64::MAX - offset % (2 * CANONICAL_REFUSAL_MARGIN_SECS),
            _ => seed,
        }
    }

    #[test]
    fn bolero_durations_round_trip_through_their_canonical_text() {
        bolero::check!()
            .with_iterations(256)
            .with_type::<(u64, u32)>()
            .for_each(|&(seed, nanos)| {
                let duration = Duration::new(generated_seconds(seed), nanos % 1_000_000_000);
                let text = humantime::format_duration(duration).to_string();
                match parse_duration_text(&text) {
                    Ok(read) => assert_eq!(read, duration, "{text}"),
                    Err(report) => {
                        assert_eq!(report.current_context(), &DurationTextError::TooLong);
                        assert!(
                            duration.as_secs() > u64::MAX - CANONICAL_REFUSAL_MARGIN_SECS,
                            "{text} was refused {} seconds below the longest duration",
                            u64::MAX - duration.as_secs()
                        );
                    }
                }
            });
    }
}
