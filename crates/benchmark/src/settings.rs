use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use nervix_approx_into::CheckedApproxInto as _;
use nervix_models::parse_duration_text;
use thiserror::Error;

use crate::{BenchmarkDefinition, LoadDuration};

const DEFAULT_MINIMUM_DURATION: Duration = Duration::from_secs(30);
const FLUSH_CYCLES: f64 = 12.0;

#[derive(Debug, Clone)]
pub struct RunSettings {
    pub duration_seconds: u64,
    pub parameters: toml::Table,
}

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("parameter override '{override_value}' must have the form name=value")]
    InvalidOverride { override_value: String },

    #[error("benchmark has no parameter named '{name}'")]
    UnknownParameter { name: String },

    /// A parameter whose value cannot be read in the form the parameter takes. Why stays beneath
    /// it in the report: the parser's own error, a [`ByteSizeError`], or a
    /// [`ParameterValueError`].
    #[error("parameter '{name}' has invalid value '{value}'")]
    InvalidParameter { name: String, value: String },

    #[error("duration override must be positive")]
    InvalidDuration,
}

impl SettingsError {
    fn invalid_parameter(name: &str, value: &str) -> Self {
        Self::InvalidParameter {
            name: name.to_string(),
            value: value.to_string(),
        }
    }
}

/// Why a parameter's value cannot serve what the benchmark reads it for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ParameterValueError {
    #[error("only scalar string, integer, float, and boolean parameters may be overridden")]
    NotScalar,
    #[error("expected a string")]
    NotString,
    #[error("it exceeds the template integer range")]
    TemplateIntegerRange,
    #[error("flush interval demands a run longer than any benchmark can measure")]
    FlushIntervalTooLong,
}

/// Why a parameter value is not a binary byte size such as `8MiB`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ByteSizeError {
    #[error("expected a positive binary byte size such as 8MiB")]
    MissingAmount,
    #[error("the byte amount does not fit in 64 bits")]
    AmountRange,
    #[error("byte size must be positive")]
    Zero,
    #[error("expected a B, KiB, MiB, or GiB suffix")]
    Suffix,
    #[error("byte size overflowed")]
    Overflow,
}

impl RunSettings {
    pub fn resolve(
        definition: &BenchmarkDefinition,
        overrides: &[String],
        duration_override: Option<u64>,
    ) -> error_stack::Result<Self, SettingsError> {
        if duration_override == Some(0) {
            return Err(Report::new(SettingsError::InvalidDuration));
        }
        let mut parameters = definition.parameters.clone();
        for override_value in overrides {
            let (name, value) =
                override_value
                    .split_once('=')
                    .ok_or_else(|| SettingsError::InvalidOverride {
                        override_value: override_value.clone(),
                    })?;
            if name.is_empty() || value.is_empty() {
                return Err(Report::new(SettingsError::InvalidOverride {
                    override_value: override_value.clone(),
                }));
            }
            let current = parameters
                .get(name)
                .ok_or_else(|| SettingsError::UnknownParameter {
                    name: name.to_string(),
                })?;
            let parsed = parse_like(name, value, current)?;
            parameters.insert(name.to_string(), parsed);
        }

        add_derived_parameters(&mut parameters)?;
        let duration_seconds = match duration_override {
            Some(seconds) => seconds,
            None => match definition.load.duration {
                LoadDuration::Seconds(seconds) => seconds,
                LoadDuration::Auto => automatic_duration(&parameters)?,
            },
        };

        Ok(Self {
            duration_seconds,
            parameters,
        })
    }
}

fn parse_like(
    name: &str,
    value: &str,
    current: &toml::Value,
) -> error_stack::Result<toml::Value, SettingsError> {
    let invalid = || SettingsError::invalid_parameter(name, value);
    match current {
        toml::Value::String(_) => Ok(toml::Value::String(value.to_string())),
        toml::Value::Integer(_) => value
            .parse::<i64>()
            .map(toml::Value::Integer)
            .change_context_lazy(invalid),
        toml::Value::Float(_) => value
            .parse::<f64>()
            .map(toml::Value::Float)
            .change_context_lazy(invalid),
        toml::Value::Boolean(_) => value
            .parse::<bool>()
            .map(toml::Value::Boolean)
            .change_context_lazy(invalid),
        _ => Err(Report::new(ParameterValueError::NotScalar).change_context(invalid())),
    }
}

/// Restates duration and binary-size parameters in the units a competitive implementation's
/// configuration takes, so one manifest value drives every implementation.
fn add_derived_parameters(parameters: &mut toml::Table) -> error_stack::Result<(), SettingsError> {
    if let Some(value) = string_parameter(parameters, "emitter_flush_each")? {
        let duration = parse_duration_parameter("emitter_flush_each", value)?;
        parameters.insert(
            "emitter_flush_seconds".to_string(),
            toml::Value::Float(duration.as_secs_f64()),
        );
    }
    if let Some(value) = string_parameter(parameters, "window_max_delay")? {
        let duration = parse_duration_parameter("window_max_delay", value)?;
        let milliseconds = i64::try_from(duration.as_millis())
            .change_context(ParameterValueError::TemplateIntegerRange)
            .change_context_lazy(|| SettingsError::invalid_parameter("window_max_delay", value))?;
        parameters.insert(
            "window_max_delay_seconds".to_string(),
            toml::Value::Float(duration.as_secs_f64()),
        );
        parameters.insert(
            "window_max_delay_ms".to_string(),
            toml::Value::Integer(milliseconds),
        );
    }
    if let Some(value) = string_parameter(parameters, "emitter_max_batch_size")? {
        let invalid = || SettingsError::invalid_parameter("emitter_max_batch_size", value);
        let bytes = parse_binary_bytes(value).change_context_lazy(invalid)?;
        let bytes = i64::try_from(bytes)
            .change_context(ParameterValueError::TemplateIntegerRange)
            .change_context_lazy(invalid)?;
        parameters.insert(
            "emitter_max_batch_bytes".to_string(),
            toml::Value::Integer(bytes),
        );
    }
    Ok(())
}

fn automatic_duration(parameters: &toml::Table) -> error_stack::Result<u64, SettingsError> {
    let mut seconds = DEFAULT_MINIMUM_DURATION.as_secs();
    for (name, value) in parameters {
        if !name.ends_with("_flush_each") {
            continue;
        }
        let toml::Value::String(value) = value else {
            return Err(Report::new(ParameterValueError::NotString)
                .change_context(SettingsError::invalid_parameter(name, &value.to_string())));
        };
        let flush = parse_duration_parameter(name, value)?;
        let required: Option<u64> = (flush.as_secs_f64() * FLUSH_CYCLES)
            .ceil()
            .checked_approx_into();
        let Some(required) = required else {
            return Err(Report::new(ParameterValueError::FlushIntervalTooLong)
                .change_context(SettingsError::invalid_parameter(name, value)));
        };
        seconds = seconds.max(required);
    }
    Ok(seconds)
}

fn parse_duration_parameter(
    name: &str,
    value: &str,
) -> error_stack::Result<Duration, SettingsError> {
    parse_duration_text(value).change_context_lazy(|| SettingsError::invalid_parameter(name, value))
}

fn string_parameter<'a>(
    parameters: &'a toml::Table,
    name: &str,
) -> error_stack::Result<Option<&'a str>, SettingsError> {
    match parameters.get(name) {
        None => Ok(None),
        Some(toml::Value::String(value)) => Ok(Some(value)),
        Some(value) => Err(Report::new(ParameterValueError::NotString)
            .change_context(SettingsError::invalid_parameter(name, &value.to_string()))),
    }
}

fn parse_binary_bytes(value: &str) -> error_stack::Result<u64, ByteSizeError> {
    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return Err(Report::new(ByteSizeError::MissingAmount));
    }
    let amount = value[..digit_count]
        .parse::<u64>()
        .change_context(ByteSizeError::AmountRange)?;
    if amount == 0 {
        return Err(Report::new(ByteSizeError::Zero));
    }
    let multiplier = match &value[digit_count..] {
        "B" => 1_u64,
        "KiB" => 1_u64 << 10,
        "MiB" => 1_u64 << 20,
        "GiB" => 1_u64 << 30,
        _ => return Err(Report::new(ByteSizeError::Suffix)),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| Report::new(ByteSizeError::Overflow))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{LoadConfiguration, LoadShape, NervixImplementation, definition::Implementation};

    fn definition(duration: LoadDuration) -> BenchmarkDefinition {
        BenchmarkDefinition {
            name: "flush benchmark".to_string(),
            description: "test".to_string(),
            dependencies: vec![crate::BenchmarkDependency::Kafka],
            load: LoadConfiguration {
                duration,
                warmup_seconds: 1,
                partitions: 1,
                value_bytes: 1,
                max_backlog_messages: 1,
                wait_timeout_seconds: 1,
                shape: LoadShape::UniformPassthrough,
            },
            parameters: [
                (
                    "ingestor_flush_each".to_string(),
                    toml::Value::String("10ms".to_string()),
                ),
                (
                    "emitter_flush_each".to_string(),
                    toml::Value::String("20s".to_string()),
                ),
                (
                    "emitter_max_batch_size".to_string(),
                    toml::Value::String("1GiB".to_string()),
                ),
                (
                    "window_max_delay".to_string(),
                    toml::Value::String("1s500ms".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
            implementations: BTreeMap::from([(
                "nervix".to_string(),
                Implementation::Nervix(NervixImplementation {
                    template: "graph.nspl".into(),
                    nodes: 1,
                    after_start: None,
                }),
            )]),
        }
    }

    #[test]
    fn auto_duration_covers_twelve_slowest_flush_cycles() {
        let settings = RunSettings::resolve(&definition(LoadDuration::Auto), &[], None)
            .expect("settings should resolve");
        assert_eq!(settings.duration_seconds, 240);
        assert_eq!(
            settings.parameters["emitter_max_batch_bytes"].as_integer(),
            Some(1_073_741_824)
        );
        assert_eq!(
            settings.parameters["emitter_flush_seconds"].as_float(),
            Some(20.0)
        );
        assert_eq!(
            settings.parameters["window_max_delay_ms"].as_integer(),
            Some(1_500)
        );
    }

    #[test]
    fn scalar_override_drives_duration_and_derived_values() {
        let settings = RunSettings::resolve(
            &definition(LoadDuration::Auto),
            &[
                "emitter_flush_each=250ms".to_string(),
                "emitter_max_batch_size=64MiB".to_string(),
                "window_max_delay=250ms".to_string(),
            ],
            Some(7),
        )
        .expect("settings should resolve");
        assert_eq!(settings.duration_seconds, 7);
        assert_eq!(
            settings.parameters["emitter_max_batch_bytes"].as_integer(),
            Some(67_108_864)
        );
        assert_eq!(
            settings.parameters["emitter_flush_seconds"].as_float(),
            Some(0.25)
        );
        assert_eq!(
            settings.parameters["window_max_delay_ms"].as_integer(),
            Some(250)
        );
    }

    #[test]
    fn a_duration_that_names_no_duration_names_the_parameter_and_the_reason() {
        for (value, why) in [
            ("oops", "expected number at 0"),
            (
                "18446744073709551615s 1000000000ns",
                "it is longer than a duration can be",
            ),
        ] {
            // A derived parameter reads the window delay, and the automatic run length reads
            // every flush interval.
            for parameter in ["window_max_delay", "ingestor_flush_each"] {
                let error = RunSettings::resolve(
                    &definition(LoadDuration::Auto),
                    &[format!("{parameter}={value}")],
                    None,
                )
                .expect_err("the override names no duration");
                assert!(error.contains::<nervix_models::DurationTextError>());
                assert_eq!(
                    format!("{error:#}"),
                    format!("parameter '{parameter}' has invalid value '{value}': {why}")
                );
            }
        }
    }

    #[test]
    fn a_zero_duration_or_an_override_that_names_nothing_is_refused() {
        let benchmark = definition(LoadDuration::Auto);

        let error = RunSettings::resolve(&benchmark, &[], Some(0))
            .expect_err("a run cannot last no time at all");
        assert!(matches!(
            error.current_context(),
            SettingsError::InvalidDuration
        ));

        for override_value in ["emitter_flush_each", "=20s", "emitter_flush_each="] {
            let error = RunSettings::resolve(&benchmark, &[override_value.to_string()], None)
                .expect_err("an override names both a parameter and a value");
            assert!(
                matches!(
                    error.current_context(),
                    SettingsError::InvalidOverride { override_value: refused }
                        if refused == override_value
                ),
                "{error:?}"
            );
        }

        let error = RunSettings::resolve(&benchmark, &["missing=1".to_string()], None)
            .expect_err("an override names a parameter the benchmark has");
        assert!(matches!(
            error.current_context(),
            SettingsError::UnknownParameter { name } if name == "missing"
        ));
    }

    #[test]
    fn a_value_that_cannot_serve_its_parameter_names_why() {
        let mut parameters = definition(LoadDuration::Auto).parameters;
        parameters.insert(
            "partition_weights".to_string(),
            toml::Value::Array(vec![toml::Value::Integer(1)]),
        );
        let cases = [
            (
                parameters.clone(),
                "partition_weights=2",
                ParameterValueError::NotScalar,
                "parameter 'partition_weights' has invalid value '2': only scalar string, \
                 integer, float, and boolean parameters may be overridden",
            ),
            (
                with_parameter(&parameters, "window_max_delay", toml::Value::Integer(5)),
                "",
                ParameterValueError::NotString,
                "parameter 'window_max_delay' has invalid value '5': expected a string",
            ),
            (
                with_parameter(&parameters, "ingestor_flush_each", toml::Value::Integer(5)),
                "",
                ParameterValueError::NotString,
                "parameter 'ingestor_flush_each' has invalid value '5': expected a string",
            ),
            // The conversion's own error follows the range, in the standard library's words.
            (
                parameters.clone(),
                "window_max_delay=10000000000000000s",
                ParameterValueError::TemplateIntegerRange,
                "parameter 'window_max_delay' has invalid value '10000000000000000s': it exceeds \
                 the template integer range: ",
            ),
            (
                parameters.clone(),
                "emitter_max_batch_size=8589934592GiB",
                ParameterValueError::TemplateIntegerRange,
                "parameter 'emitter_max_batch_size' has invalid value '8589934592GiB': it exceeds \
                 the template integer range: ",
            ),
            (
                parameters.clone(),
                "ingestor_flush_each=2000000000000000000s",
                ParameterValueError::FlushIntervalTooLong,
                "parameter 'ingestor_flush_each' has invalid value '2000000000000000000s': flush \
                 interval demands a run longer than any benchmark can measure",
            ),
        ];
        for (parameters, override_value, expected, message) in cases {
            let mut benchmark = definition(LoadDuration::Auto);
            benchmark.parameters = parameters;
            let overrides = match override_value {
                "" => Vec::new(),
                override_value => vec![override_value.to_string()],
            };
            let error = RunSettings::resolve(&benchmark, &overrides, None)
                .expect_err("the parameter value cannot serve its parameter");

            assert!(matches!(
                error.current_context(),
                SettingsError::InvalidParameter { .. }
            ));
            assert_eq!(
                error.downcast_ref::<ParameterValueError>(),
                Some(&expected),
                "{override_value}"
            );
            let rendered = format!("{error:#}");
            assert!(rendered.starts_with(message), "{rendered}");
        }
    }

    #[test]
    fn a_scalar_override_that_does_not_parse_keeps_the_parser_error() {
        let mut benchmark = definition(LoadDuration::Auto);
        for (name, value) in [
            ("partitions", toml::Value::Integer(1)),
            ("ratio", toml::Value::Float(0.5)),
            ("enabled", toml::Value::Boolean(true)),
        ] {
            benchmark.parameters.insert(name.to_string(), value);
        }
        for (override_value, message) in [
            (
                "partitions=many",
                "parameter 'partitions' has invalid value 'many': invalid digit found in string",
            ),
            (
                "ratio=half",
                "parameter 'ratio' has invalid value 'half': invalid float literal",
            ),
            (
                "enabled=maybe",
                "parameter 'enabled' has invalid value 'maybe': provided string was not `true` or \
                 `false`",
            ),
        ] {
            let error = RunSettings::resolve(&benchmark, &[override_value.to_string()], None)
                .expect_err("the override does not parse as its parameter's type");

            assert_eq!(format!("{error:#}"), message);
        }
    }

    fn with_parameter(parameters: &toml::Table, name: &str, value: toml::Value) -> toml::Table {
        let mut parameters = parameters.clone();
        parameters.insert(name.to_string(), value);
        parameters
    }

    #[test]
    fn binary_byte_sizes_name_the_part_they_break() {
        let cases = [
            ("MiB", ByteSizeError::MissingAmount),
            ("99999999999999999999B", ByteSizeError::AmountRange),
            ("0MiB", ByteSizeError::Zero),
            ("8MB", ByteSizeError::Suffix),
            ("17179869184GiB", ByteSizeError::Overflow),
        ];
        for (value, expected) in cases {
            let error = parse_binary_bytes(value).expect_err("the byte size is invalid");
            assert_eq!(error.current_context(), &expected, "value {value}");
        }
        assert_eq!(parse_binary_bytes("8KiB").ok(), Some(8_192));
    }

    #[test]
    fn an_invalid_byte_size_override_names_the_parameter_and_the_reason() {
        let error = RunSettings::resolve(
            &definition(LoadDuration::Auto),
            &["emitter_max_batch_size=8MB".to_string()],
            None,
        )
        .expect_err("the byte size suffix is invalid");

        assert!(matches!(
            error.current_context(),
            SettingsError::InvalidParameter { .. }
        ));
        assert_eq!(
            error.downcast_ref::<ByteSizeError>(),
            Some(&ByteSizeError::Suffix)
        );
        assert_eq!(
            format!("{error:#}"),
            "parameter 'emitter_max_batch_size' has invalid value '8MB': expected a B, KiB, MiB, \
             or GiB suffix"
        );
    }
}
