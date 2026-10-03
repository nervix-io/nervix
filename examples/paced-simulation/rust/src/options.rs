//! The command line of the simulation, and the settings a run takes from it.
//!
//! - **Owns.** Every option, its default and its validation, and the endpoint names a run uses.
//! - **Depends on.** The vocabulary's duration text and names.
//! - **Must not know.** Sessions, clocks or files.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    time::Duration,
};

use clap::{Parser, ValueEnum};
use nervix_models::{DomainName, EmitterName, IngestorName, parse_duration_text};
use thiserror::Error;

/// The most rows one batch of the example's ingestors may carry.
const MAX_BATCH_ROWS: u64 = 65_536;

/// Paced sensor simulation over a Nervix client ingestor and attached client emitters.
///
/// Attaches to the domain clock, submits readings stamped with each tick center the clock reaches,
/// and applies and acknowledges the readings the graph constructs.
#[derive(Debug, Parser)]
#[command(name = "nervix-paced-simulation", version)]
pub struct Options {
    /// The gRPC session endpoint of any live node.
    #[arg(long, default_value = "http://127.0.0.1:47391")]
    pub(crate) server: String,
    /// The user the session authenticates as.
    #[arg(long, env = "NERVIX_USERNAME")]
    pub(crate) username: Option<String>,
    /// The password of that user.
    #[arg(long, env = "NERVIX_PASSWORD", hide_env_values = true)]
    pub(crate) password: Option<String>,
    /// The paced domain the example graph runs in.
    #[arg(long, default_value = "paced_simulation")]
    pub(crate) domain: String,
    /// The event-time source of the ingestor readings go to: `at` submits to the `TIMESTAMP AT`
    /// ingestor, `now` to the `TIMESTAMP NOW` one.
    #[arg(long, value_enum, default_value_t = TimestampSource::At)]
    pub(crate) timestamps: TimestampSource,
    /// The client ingestor readings are submitted to, instead of the one `--timestamps` selects.
    #[arg(long)]
    pub(crate) ingestor: Option<String>,
    /// The attached client emitter the constructed readings are consumed from.
    #[arg(long, default_value = "observed_readings")]
    pub(crate) emitter: String,
    /// The attached client emitter rejection notices are consumed from.
    #[arg(long, default_value = "rejection_notices")]
    pub(crate) rejections: String,
    /// How many tick centers to submit readings for. Zero submits only what `--replay` resubmits.
    #[arg(long, default_value_t = 50)]
    pub(crate) ticks: u64,
    /// How many sensors report at each tick center; each is a concrete branch of the graph.
    #[arg(long, default_value_t = 3)]
    pub(crate) sensors: u32,
    /// How many readings each sensor reports at each tick center.
    #[arg(long, default_value_t = 1)]
    pub(crate) burst: u32,
    /// Stamp the first reading of every Nth tick before the admission window, deliberately.
    #[arg(long, default_value_t = 0)]
    pub(crate) invalid_every: u64,
    /// How many competing consumers of the output emitter run.
    #[arg(long, default_value_t = 1)]
    pub(crate) consumers: u32,
    /// How long the output consumers wait before they attach, while readings are submitted.
    #[arg(long, value_parser = duration, default_value = "0s")]
    pub(crate) consumer_delay: Duration,
    /// The last output consumer leaves after acknowledging this many deliveries.
    #[arg(long)]
    pub(crate) consumer_leave_after: Option<u64>,
    /// How long the application works on each delivery before it records its effect and
    /// acknowledges it.
    #[arg(long, value_parser = duration, default_value = "0s")]
    pub(crate) processing_time: Duration,
    /// How many batches the producer may have outstanding.
    #[arg(long, default_value_t = 8)]
    pub(crate) credit_batches: u32,
    /// How many bytes of Arrow IPC the producer may have outstanding, such as `1MiB`.
    #[arg(long, value_parser = byte_size, default_value = "1MiB")]
    pub(crate) credit_bytes: u64,
    /// Inspect the ingestor and the output emitter on the session at this interval.
    #[arg(long, value_parser = duration)]
    pub(crate) inspect_every: Option<Duration>,
    /// The application-owned input ledger: every reading before it is submitted, and every
    /// outcome.
    #[arg(long, default_value = "paced-simulation-ledger.jsonl")]
    pub(crate) ledger: PathBuf,
    /// The idempotent effect store the consumers apply readings and rejection notices to.
    #[arg(long, default_value = "paced-simulation-effects.jsonl")]
    pub(crate) effects: PathBuf,
    /// Before simulating, resubmit the ledger's readings of the current START generation whose
    /// outcome was not completed.
    #[arg(long)]
    pub(crate) replay: bool,
    /// After the domain stops and starts again, open new endpoints under the new START generation
    /// and continue there, instead of finishing.
    #[arg(long)]
    pub(crate) follow_generations: bool,
    /// How long to wait for the outcomes still outstanding once the simulation stops submitting.
    #[arg(long, value_parser = duration, default_value = "10m")]
    pub(crate) deadline: Duration,
}

/// The event-time source of the ingestor readings are submitted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum TimestampSource {
    /// `TIMESTAMP AT occurred_at`: the reading's own event time, admitted within the window.
    At,
    /// `TIMESTAMP NOW`: the domain time the reading arrives at.
    Now,
}

impl TimestampSource {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::At => "at",
            Self::Now => "now",
        }
    }

    /// The example graph's ingestor of this event-time source.
    const fn ingestor(self) -> &'static str {
        match self {
            Self::At => "simulated_readings",
            Self::Now => "live_readings",
        }
    }
}

/// Why one argument of the command line is not a value of its option.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ArgumentError {
    #[error("'{text}' is not a duration such as 250ms, 1s or 5m")]
    Duration { text: String },
    #[error("'{text}' is not a byte size such as 65536, 64KiB or 1MiB")]
    ByteSize { text: String },
    #[error("'{text}' is more bytes than a size can hold")]
    TooManyBytes { text: String },
}

/// Why the command line describes no run.
#[derive(Debug, Error)]
pub(crate) enum OptionsError {
    #[error("'{name}' is not a valid {kind} name")]
    InvalidName { kind: &'static str, name: String },
    #[error("--{option} must be at least one")]
    Zero { option: &'static str },
    #[error(
        "--sensors {sensors} with --burst {burst} puts {rows} readings in one batch, more than \
         the {MAX_BATCH_ROWS} a batch may carry"
    )]
    TooManyRows { sensors: u32, burst: u32, rows: u64 },
    #[error("--consumer-leave-after needs a second consumer to stay; pass --consumers 2 or more")]
    NobodyStays,
}

/// The validated settings of one run.
#[derive(Debug)]
pub(crate) struct Settings {
    pub(crate) server: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) domain: DomainName,
    pub(crate) timestamps: TimestampSource,
    pub(crate) ingestor: IngestorName,
    pub(crate) emitter: EmitterName,
    pub(crate) rejections: EmitterName,
    pub(crate) ticks: u64,
    pub(crate) sensors: NonZeroU32,
    pub(crate) burst: NonZeroU32,
    pub(crate) invalid_every: Option<NonZeroU64>,
    pub(crate) consumers: NonZeroU32,
    pub(crate) consumer_delay: Duration,
    pub(crate) consumer_leave_after: Option<u64>,
    pub(crate) processing_time: Duration,
    pub(crate) credit_batches: NonZeroU32,
    pub(crate) credit_bytes: NonZeroU64,
    pub(crate) inspect_every: Option<Duration>,
    pub(crate) ledger: PathBuf,
    pub(crate) effects: PathBuf,
    pub(crate) replay: bool,
    pub(crate) follow_generations: bool,
    pub(crate) deadline: Duration,
}

impl TryFrom<Options> for Settings {
    type Error = OptionsError;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let domain = DomainName::parse(&options.domain).map_err(|_| OptionsError::InvalidName {
            kind: "domain",
            name: options.domain.clone(),
        })?;
        let ingestor_text = match &options.ingestor {
            Some(ingestor) => ingestor.clone(),
            None => options.timestamps.ingestor().to_string(),
        };
        let ingestor =
            IngestorName::parse(&ingestor_text).map_err(|_| OptionsError::InvalidName {
                kind: "ingestor",
                name: ingestor_text.clone(),
            })?;
        let emitter =
            EmitterName::parse(&options.emitter).map_err(|_| OptionsError::InvalidName {
                kind: "emitter",
                name: options.emitter.clone(),
            })?;
        let rejections =
            EmitterName::parse(&options.rejections).map_err(|_| OptionsError::InvalidName {
                kind: "emitter",
                name: options.rejections.clone(),
            })?;
        let sensors =
            NonZeroU32::new(options.sensors).ok_or(OptionsError::Zero { option: "sensors" })?;
        let burst = NonZeroU32::new(options.burst).ok_or(OptionsError::Zero { option: "burst" })?;
        let rows = u64::from(sensors.get())
            .checked_mul(u64::from(burst.get()))
            .ok_or(OptionsError::TooManyRows {
                sensors: sensors.get(),
                burst: burst.get(),
                rows: u64::MAX,
            })?;
        if rows > MAX_BATCH_ROWS {
            return Err(OptionsError::TooManyRows {
                sensors: sensors.get(),
                burst: burst.get(),
                rows,
            });
        }
        let consumers = NonZeroU32::new(options.consumers).ok_or(OptionsError::Zero {
            option: "consumers",
        })?;
        if options.consumer_leave_after.is_some() && consumers.get() < 2 {
            return Err(OptionsError::NobodyStays);
        }
        let credit_batches = NonZeroU32::new(options.credit_batches).ok_or(OptionsError::Zero {
            option: "credit-batches",
        })?;
        let credit_bytes = NonZeroU64::new(options.credit_bytes).ok_or(OptionsError::Zero {
            option: "credit-bytes",
        })?;
        Ok(Self {
            server: options.server,
            username: options.username,
            password: options.password,
            domain,
            timestamps: options.timestamps,
            ingestor,
            emitter,
            rejections,
            ticks: options.ticks,
            sensors,
            burst,
            invalid_every: NonZeroU64::new(options.invalid_every),
            consumers,
            consumer_delay: options.consumer_delay,
            consumer_leave_after: options.consumer_leave_after,
            processing_time: options.processing_time,
            credit_batches,
            credit_bytes,
            inspect_every: options.inspect_every,
            ledger: options.ledger,
            effects: options.effects,
            replay: options.replay,
            follow_generations: options.follow_generations,
            deadline: options.deadline,
        })
    }
}

/// Reads duration text such as `250ms`, `1s` or `5m`.
fn duration(text: &str) -> Result<Duration, ArgumentError> {
    match parse_duration_text(text) {
        Ok(duration) => Ok(duration),
        Err(_) => Err(ArgumentError::Duration {
            text: text.to_string(),
        }),
    }
}

/// Reads a byte size: a count of bytes, or a count of `KiB`, `MiB` or `GiB`.
fn byte_size(text: &str) -> Result<u64, ArgumentError> {
    let units: [(&str, u64); 3] = [
        ("GiB", 1024 * 1024 * 1024),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
    ];
    let mut digits = text;
    let mut multiplier = 1;
    for (suffix, unit) in units {
        if let Some(stripped) = text.strip_suffix(suffix) {
            digits = stripped;
            multiplier = unit;
            break;
        }
    }
    let Ok(count) = digits.parse::<u64>() else {
        return Err(ArgumentError::ByteSize {
            text: text.to_string(),
        });
    };
    let Some(bytes) = count.checked_mul(multiplier) else {
        return Err(ArgumentError::TooManyBytes {
            text: text.to_string(),
        });
    };
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_sizes_read_plain_counts_and_binary_units() {
        assert_eq!(byte_size("65536"), Ok(65_536));
        assert_eq!(byte_size("128KiB"), Ok(131_072));
        assert_eq!(byte_size("1MiB"), Ok(1_048_576));
        assert_eq!(byte_size("2GiB"), Ok(2_147_483_648));
        assert_eq!(
            byte_size("1MB"),
            Err(ArgumentError::ByteSize {
                text: "1MB".to_string()
            })
        );
        assert!(byte_size("KiB").is_err());
        assert_eq!(
            byte_size("18446744073709551615GiB"),
            Err(ArgumentError::TooManyBytes {
                text: "18446744073709551615GiB".to_string()
            })
        );
        assert_eq!(duration("250ms"), Ok(Duration::from_millis(250)));
        assert!(duration("soon").is_err());
    }

    #[test]
    fn the_timestamp_source_selects_the_example_ingestor_unless_one_is_named() {
        let at = Settings::try_from(Options::parse_from(["driver"])).ok();
        let now = Settings::try_from(Options::parse_from(["driver", "--timestamps", "now"])).ok();
        let named = Settings::try_from(Options::parse_from([
            "driver",
            "--timestamps",
            "now",
            "--ingestor",
            "custom_readings",
        ]))
        .ok();
        let ingestor = |settings: Option<Settings>| match settings {
            Some(settings) => settings.ingestor.as_str().to_string(),
            None => String::new(),
        };
        assert_eq!(ingestor(at), "simulated_readings");
        assert_eq!(ingestor(now), "live_readings");
        assert_eq!(ingestor(named), "custom_readings");
    }

    #[test]
    fn a_run_that_cannot_happen_is_refused_before_it_connects() {
        let refused = |arguments: &[&str]| {
            let mut command = vec!["driver"];
            command.extend_from_slice(arguments);
            Settings::try_from(Options::parse_from(command)).err()
        };
        assert!(matches!(
            refused(&["--sensors", "0"]),
            Some(OptionsError::Zero { option: "sensors" })
        ));
        assert!(matches!(
            refused(&["--sensors", "1000", "--burst", "1000"]),
            Some(OptionsError::TooManyRows {
                rows: 1_000_000,
                ..
            })
        ));
        assert!(matches!(
            refused(&["--consumer-leave-after", "3"]),
            Some(OptionsError::NobodyStays)
        ));
        assert!(matches!(
            refused(&["--domain", "not a name"]),
            Some(OptionsError::InvalidName { kind: "domain", .. })
        ));
    }
}
