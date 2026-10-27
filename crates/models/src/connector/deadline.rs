//! The one rule every client deadline key's value follows.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The physical bound a client's deadline key sets, and why a value cannot be one.
//! - **Depends on.** Durations and the semantic error vocabulary.
//! - **Must not know.** Which key or connector a deadline bounds, or how a wait applies it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use thiserror::Error;

/// Why the value of a client's deadline key cannot bound a wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ClientDeadlineError {
    #[error("not a positive whole number of milliseconds")]
    NotPositiveWholeMilliseconds,
    #[error("longer than the longest deadline a wait can be scheduled with")]
    Unschedulable,
}

/// The physical bound a client's deadline key sets on the waits it bounds: a positive whole number
/// of milliseconds no longer than [`ClientDeadline::LONGEST`].
///
/// This is the one rule for every deadline key, so a deadline the registry checks and one a
/// connector reads are held to it here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientDeadline(Duration);

impl ClientDeadline {
    /// The longest deadline a wait can be scheduled with: half the nanoseconds a `u64` holds, about
    /// 146 years.
    pub const LONGEST: Duration = Duration::from_nanos(u64::MAX / 2);

    /// The timeout `milliseconds` states, written as ASCII digits alone: no sign, fraction, unit or
    /// whitespace.
    pub fn parse(milliseconds: &str) -> Result<Self, Report<ClientDeadlineError>> {
        if milliseconds.is_empty() || !milliseconds.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Report::new(
                ClientDeadlineError::NotPositiveWholeMilliseconds,
            ));
        }
        // Only ASCII digits remain, so the one way the count can fail to parse is by exceeding a
        // `u64`, which is longer than the longest timeout too.
        let count = milliseconds
            .parse::<u64>()
            .change_context(ClientDeadlineError::Unschedulable)?;
        if count == 0 {
            return Err(Report::new(
                ClientDeadlineError::NotPositiveWholeMilliseconds,
            ));
        }
        let timeout = Duration::from_millis(count);
        if timeout > Self::LONGEST {
            return Err(Report::new(ClientDeadlineError::Unschedulable));
        }
        Ok(Self(timeout))
    }

    pub const fn duration(self) -> Duration {
        self.0
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn a_client_deadline_is_a_positive_whole_number_of_schedulable_milliseconds() {
        for (milliseconds, expected) in [
            ("1", Duration::from_millis(1)),
            ("5000", Duration::from_secs(5)),
            ("0005000", Duration::from_secs(5)),
        ] {
            let timeout = ClientDeadline::parse(milliseconds)
                .assured("each table entry is a positive whole number of milliseconds");
            assert_eq!(timeout.duration(), expected, "{milliseconds}");
        }

        let longest = ClientDeadline::LONGEST.as_millis();
        let timeout = ClientDeadline::parse(&longest.to_string())
            .assured("the longest whole number of milliseconds within the bound is admitted");
        assert!(timeout.duration() <= ClientDeadline::LONGEST);
        let beyond = longest
            .checked_add(1)
            .assured("the bound in milliseconds is far below the u128 maximum");
        let Err(report) = ClientDeadline::parse(&beyond.to_string()) else {
            panic!("one millisecond past the bound must be refused");
        };
        assert_eq!(
            report.current_context(),
            &ClientDeadlineError::Unschedulable
        );

        for (milliseconds, expected) in [
            ("", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("0", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("000", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("-5", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("+5", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("5.0", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("5s", ClientDeadlineError::NotPositiveWholeMilliseconds),
            (" 5", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("٥", ClientDeadlineError::NotPositiveWholeMilliseconds),
            ("18446744073709551616", ClientDeadlineError::Unschedulable),
        ] {
            let Err(report) = ClientDeadline::parse(milliseconds) else {
                panic!("'{milliseconds}' must not be a client deadline");
            };
            assert_eq!(report.current_context(), &expected, "{milliseconds}");
        }
    }
}
