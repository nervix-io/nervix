//! Command-line value parsing at the server edge.
//!
//! Layer: edges.
//!
//! - **Owns.** Parsing operator-supplied duration, byte quantity and trace sampling values,
//!   with typed errors that retain the reason a value was rejected.
//! - **Depends on.** Vocabulary duration parsing, byte quantities and typed error reports.
//! - **Must not know.** Runtime state, sessions, graph planning or deadline policy.

use std::time::Duration;

use error_stack::Report;
use nervix_models::{DurationTextError, parse_duration_text};

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum CliValueError {
    #[error("invalid duration: {source}")]
    Duration { source: DurationTextError },
    #[error("invalid byte quantity")]
    Bytes,
    #[error("invalid trace sample ratio: {source}")]
    TraceSampleRatio { source: std::num::ParseFloatError },
    #[error("trace sample ratio must be between 0.0 and 1.0")]
    TraceSampleRatioRange,
}

/// Reads a duration option. Clap prints only the outermost context of a rejected value, so that
/// context carries the reason the text names no duration.
pub(in crate::application) fn parse_human_duration(
    input: &str,
) -> error_stack::Result<Duration, CliValueError> {
    match parse_duration_text(input) {
        Ok(duration) => Ok(duration),
        Err(report) => {
            let source = report.current_context().clone();
            Err(report.change_context(CliValueError::Duration { source }))
        }
    }
}

pub(in crate::application) fn parse_human_bytes(
    input: &str,
) -> error_stack::Result<ubyte::ByteUnit, CliValueError> {
    input
        .parse::<ubyte::ByteUnit>()
        .map_err(|_| Report::new(CliValueError::Bytes))
}

pub(in crate::application) fn parse_trace_sample_ratio(
    input: &str,
) -> error_stack::Result<f64, CliValueError> {
    let ratio = input
        .parse::<f64>()
        .map_err(|source| Report::new(CliValueError::TraceSampleRatio { source }))?;
    if (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err(Report::new(CliValueError::TraceSampleRatioRange))
    }
}
