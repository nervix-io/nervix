//! Duration text as NSPL writes it, read without the overflow its grammar's library panics on.
//!
//! A duration is one or more spans, each a whole or fractional number and a unit, as `humantime`
//! reads them. `humantime` 2.4 carries a fractional second into the seconds only once the
//! fractions exceed one second, so spans that add up to the last second a `Duration` holds and
//! fractions of exactly one more second make `Duration::new` overflow and panic. Every duration
//! Nervix reads is far shorter, so text is refused as too long, before `humantime` reads it, whenever
//! an upper bound on what it names reaches that second.

use std::time::Duration;

use error_stack::Report;
use thiserror::Error;

/// Why duration text names no duration.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum DurationTextError {
    /// The text does not follow the duration grammar.
    #[error("invalid duration '{text}': {reason}")]
    Malformed {
        text: String,
        reason: humantime::DurationError,
    },
    /// The spans could name more seconds than a duration holds.
    #[error("invalid duration '{text}': it is longer than a duration can be")]
    TooLong { text: String },
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
        return Err(Report::new(DurationTextError::TooLong {
            text: text.to_string(),
        }));
    }
    humantime::parse_duration(text).map_err(|reason| {
        Report::new(DurationTextError::Malformed {
            text: text.to_string(),
            reason,
        })
    })
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

    use super::{DurationTextError, parse_duration_text};

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
            "18446744073709551615.5s500000000ns",
            "18446744073709551615s 1000000000ns",
        ] {
            let report = parse_duration_text(text).expect_err("the text is too long");
            assert_eq!(
                report.current_context(),
                &DurationTextError::TooLong {
                    text: text.to_string()
                }
            );
        }
    }

    #[test]
    fn text_outside_the_grammar_is_malformed() {
        for text in ["", "abc", "5", "5 parsecs", "1.s"] {
            let report = parse_duration_text(text).expect_err("the text is not a duration");
            assert!(
                matches!(
                    report.current_context(),
                    DurationTextError::Malformed { .. }
                ),
                "{text}: {report:?}"
            );
        }
    }
}
