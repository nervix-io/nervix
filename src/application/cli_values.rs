//! Command-line value parsing at the server edge.
//!
//! Layer: edges.
//!
//! - **Owns.** Parsing operator-supplied duration, byte quantity and trace sampling values,
//!   with typed errors that retain the reason a value was rejected.
//! - **Depends on.** Vocabulary duration parsing, byte quantities and typed error reports.
//! - **Must not know.** Runtime state, sessions, graph planning or deadline policy.

use std::{fmt, time::Duration};

use error_stack::{Report, ResultExt as _};
use nervix_models::parse_duration_text;

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum CliValueError {
    #[error("invalid duration")]
    Duration,
    #[error("invalid byte quantity")]
    Bytes,
    #[error("invalid trace sample ratio")]
    TraceSampleRatio { source: std::num::ParseFloatError },
    #[error("trace sample ratio must be between 0.0 and 1.0")]
    TraceSampleRatioRange,
}

/// A command-line value the node refuses, in the form clap shows an operator.
///
/// Clap prints the error a value parser returns with its plain `Display`, which for a report is
/// only the outermost context. A refusal displays the whole chain instead, so the reason beneath
/// the context, such as why the text names no duration, is shown once.
#[derive(Debug)]
pub(in crate::application) struct RefusedCliValue(Report<CliValueError>);

impl fmt::Display for RefusedCliValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for RefusedCliValue {}

impl From<Report<CliValueError>> for RefusedCliValue {
    fn from(report: Report<CliValueError>) -> Self {
        Self(report)
    }
}

/// Reads a duration option, keeping the reason the text names no duration beneath the refusal.
pub(in crate::application) fn parse_human_duration(
    input: &str,
) -> Result<Duration, RefusedCliValue> {
    let duration = parse_duration_text(input).change_context(CliValueError::Duration)?;
    Ok(duration)
}

pub(in crate::application) fn parse_human_bytes(
    input: &str,
) -> Result<ubyte::ByteUnit, RefusedCliValue> {
    let bytes = input
        .parse::<ubyte::ByteUnit>()
        .map_err(|_| Report::new(CliValueError::Bytes))?;
    Ok(bytes)
}

pub(in crate::application) fn parse_trace_sample_ratio(
    input: &str,
) -> Result<f64, RefusedCliValue> {
    let ratio = input
        .parse::<f64>()
        .map_err(|source| Report::new(CliValueError::TraceSampleRatio { source }))?;
    if (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err(Report::new(CliValueError::TraceSampleRatioRange).into())
    }
}
