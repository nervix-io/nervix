//! Command-line value parsing at the server edge.
//!
//! Layer: edges.
//!
//! - **Owns.** Parsing operator-supplied duration, byte quantity and trace sampling values,
//!   with typed errors that retain the reason a value was rejected, rendering durations and byte
//!   quantities in the form those parsers read back exactly, and the transport mode an operator
//!   selects for the node's gRPC listener.
//! - **Depends on.** Vocabulary duration parsing, byte quantities and typed error reports.
//! - **Must not know.** Runtime state, sessions, graph planning or deadline policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{fmt, time::Duration};

use clap::ValueEnum;
use error_stack::{Report, ResultExt as _};
use nervix_models::{SettingDefinition as _, parse_duration_text};

use super::tracing_setup::TraceSampleRatio;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
#[clap(rename_all = "lower")]
pub enum InternalTransportMode {
    #[default]
    Http,
    Https,
}

impl InternalTransportMode {
    pub(in crate::application) fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    pub(in crate::application) fn is_tls(self) -> bool {
        matches!(self, Self::Https)
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum CliValueError {
    #[error("invalid duration")]
    Duration,
    #[error("invalid byte quantity")]
    Bytes,
    #[error("invalid trace sample ratio")]
    TraceSampleRatio,
}

/// A command-line value the node refuses, in the form clap shows an operator.
///
/// Clap prints the error a value parser returns with its plain `Display`, which for a report is
/// only the outermost context. A refusal displays the whole chain instead, so the reason beneath
/// the context, such as why the text names no duration, is shown once.
#[derive(Debug)]
pub struct RefusedCliValue(Report<CliValueError>);

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

/// A duration as the node's options write it: one whole number of the largest unit that holds
/// it exactly, such as `250ms`, `1500ms` or `15m`.
///
/// Help output prints a default through this rendering, and [`parse_human_duration`] reads every
/// rendering back to the same duration. A fractional duration whose subsecond span exceeds the
/// parser's 64-bit nanosecond accumulator is written as whole seconds and remaining nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(pub Duration);

/// The units [`HumanDuration`] writes, largest first, with the nanoseconds in one of each.
const DURATION_UNITS: [(&str, u128); 7] = [
    ("d", 86_400_000_000_000),
    ("h", 3_600_000_000_000),
    ("m", 60_000_000_000),
    ("s", 1_000_000_000),
    ("ms", 1_000_000),
    ("us", 1_000),
    ("ns", 1),
];

impl fmt::Display for HumanDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nanos = self.0.as_nanos();
        if nanos == 0 {
            return formatter.write_str("0s");
        }
        for (suffix, unit) in DURATION_UNITS {
            if !nanos.is_multiple_of(unit) {
                continue;
            }
            // An exact unit of a fractional duration is subsecond. The parser multiplies its
            // count into u64 nanoseconds before carrying whole seconds.
            if self.0.subsec_nanos() != 0 && u64::try_from(nanos).is_err() {
                break;
            }
            let Ok(count) = u64::try_from(nanos / unit) else {
                break;
            };
            return write!(formatter, "{count}{suffix}");
        }
        write!(
            formatter,
            "{}s {}ns",
            self.0.as_secs(),
            self.0.subsec_nanos()
        )
    }
}

impl std::str::FromStr for HumanDuration {
    type Err = RefusedCliValue;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_human_duration(input).map(Self)
    }
}

/// A byte quantity as the node's options write it: one whole number of the largest binary unit
/// that holds it exactly, such as `64MiB` or `1GiB`, or of bytes when no unit does.
///
/// Help output prints a default through this rendering, and [`parse_human_bytes`] reads every
/// rendering back to the same quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanBytes(pub ubyte::ByteUnit);

/// The binary units [`HumanBytes`] writes, largest first.
const BYTE_UNITS: [(&str, ubyte::ByteUnit); 6] = [
    ("EiB", ubyte::ByteUnit::EiB),
    ("PiB", ubyte::ByteUnit::PiB),
    ("TiB", ubyte::ByteUnit::TiB),
    ("GiB", ubyte::ByteUnit::GiB),
    ("MiB", ubyte::ByteUnit::MiB),
    ("KiB", ubyte::ByteUnit::KiB),
];

impl fmt::Display for HumanBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.as_u64();
        if bytes != 0 {
            for (suffix, unit) in BYTE_UNITS {
                let unit = unit.as_u64();
                if bytes.is_multiple_of(unit) {
                    return write!(formatter, "{}{suffix}", bytes / unit);
                }
            }
        }
        write!(formatter, "{bytes}B")
    }
}

impl std::str::FromStr for HumanBytes {
    type Err = RefusedCliValue;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_human_bytes(input).map(Self)
    }
}

/// Reads a trace sample ratio, refusing one its setting's definition does not accept.
pub(in crate::application) fn parse_trace_sample_ratio(
    input: &str,
) -> Result<f64, RefusedCliValue> {
    let ratio = input
        .parse::<f64>()
        .change_context(CliValueError::TraceSampleRatio)?;
    TraceSampleRatio::validate(&ratio, &()).change_context(CliValueError::TraceSampleRatio)?;
    Ok(ratio)
}
