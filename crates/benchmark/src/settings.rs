use std::time::Duration;

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

    #[error("parameter '{name}' has invalid value '{value}': {reason}")]
    InvalidParameter {
        name: String,
        value: String,
        reason: String,
    },

    #[error("duration override must be positive")]
    InvalidDuration,
}

impl RunSettings {
    pub fn resolve(
        definition: &BenchmarkDefinition,
        overrides: &[String],
        duration_override: Option<u64>,
    ) -> Result<Self, SettingsError> {
        if duration_override == Some(0) {
            return Err(SettingsError::InvalidDuration);
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
                return Err(SettingsError::InvalidOverride {
                    override_value: override_value.clone(),
                });
            }
            let current = parameters
                .get(name)
                .ok_or_else(|| SettingsError::UnknownParameter {
                    name: name.to_string(),
                })?;
            let parsed = parse_like(name, value, current)?;
            parameters.insert(name.to_string(), parsed);
        }

        add_emitter_derivatives(&mut parameters)?;
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
) -> Result<toml::Value, SettingsError> {
    let invalid = |reason: String| SettingsError::InvalidParameter {
        name: name.to_string(),
        value: value.to_string(),
        reason,
    };
    match current {
        toml::Value::String(_) => Ok(toml::Value::String(value.to_string())),
        toml::Value::Integer(_) => value
            .parse::<i64>()
            .map(toml::Value::Integer)
            .map_err(|error| invalid(error.to_string())),
        toml::Value::Float(_) => value
            .parse::<f64>()
            .map(toml::Value::Float)
            .map_err(|error| invalid(error.to_string())),
        toml::Value::Boolean(_) => value
            .parse::<bool>()
            .map(toml::Value::Boolean)
            .map_err(|error| invalid(error.to_string())),
        _ => Err(invalid(
            "only scalar string, integer, float, and boolean parameters may be overridden"
                .to_string(),
        )),
    }
}

fn add_emitter_derivatives(parameters: &mut toml::Table) -> Result<(), SettingsError> {
    if let Some(value) = string_parameter(parameters, "emitter_flush_each")? {
        let duration =
            humantime::parse_duration(value).map_err(|error| SettingsError::InvalidParameter {
                name: "emitter_flush_each".to_string(),
                value: value.to_string(),
                reason: error.to_string(),
            })?;
        parameters.insert(
            "emitter_flush_seconds".to_string(),
            toml::Value::Float(duration.as_secs_f64()),
        );
    }
    if let Some(value) = string_parameter(parameters, "emitter_max_batch_size")? {
        let bytes =
            parse_binary_bytes(value).map_err(|reason| SettingsError::InvalidParameter {
                name: "emitter_max_batch_size".to_string(),
                value: value.to_string(),
                reason,
            })?;
        let bytes = i64::try_from(bytes).map_err(|_| SettingsError::InvalidParameter {
            name: "emitter_max_batch_size".to_string(),
            value: value.to_string(),
            reason: "byte size exceeds the template integer range".to_string(),
        })?;
        parameters.insert(
            "emitter_max_batch_bytes".to_string(),
            toml::Value::Integer(bytes),
        );
    }
    Ok(())
}

fn automatic_duration(parameters: &toml::Table) -> Result<u64, SettingsError> {
    let mut duration = DEFAULT_MINIMUM_DURATION;
    for (name, value) in parameters {
        if !name.ends_with("_flush_each") {
            continue;
        }
        let toml::Value::String(value) = value else {
            return Err(SettingsError::InvalidParameter {
                name: name.clone(),
                value: value.to_string(),
                reason: "flush intervals must be duration strings".to_string(),
            });
        };
        let flush =
            humantime::parse_duration(value).map_err(|error| SettingsError::InvalidParameter {
                name: name.clone(),
                value: value.clone(),
                reason: error.to_string(),
            })?;
        duration = duration.max(Duration::from_secs_f64(flush.as_secs_f64() * FLUSH_CYCLES));
    }
    Ok(duration.as_secs_f64().ceil() as u64)
}

fn string_parameter<'a>(
    parameters: &'a toml::Table,
    name: &str,
) -> Result<Option<&'a str>, SettingsError> {
    match parameters.get(name) {
        None => Ok(None),
        Some(toml::Value::String(value)) => Ok(Some(value)),
        Some(value) => Err(SettingsError::InvalidParameter {
            name: name.to_string(),
            value: value.to_string(),
            reason: "expected a string".to_string(),
        }),
    }
}

fn parse_binary_bytes(value: &str) -> Result<u64, String> {
    let digit_count = value.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return Err("expected a positive binary byte size such as 8MiB".to_string());
    }
    let amount = value[..digit_count]
        .parse::<u64>()
        .map_err(|error| error.to_string())?;
    if amount == 0 {
        return Err("byte size must be positive".to_string());
    }
    let multiplier = match &value[digit_count..] {
        "B" => 1_u64,
        "KiB" => 1_u64 << 10,
        "MiB" => 1_u64 << 20,
        "GiB" => 1_u64 << 30,
        _ => return Err("expected a B, KiB, MiB, or GiB suffix".to_string()),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| "byte size overflowed".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{LoadConfiguration, NervixImplementation, definition::Implementation};

    fn definition(duration: LoadDuration) -> BenchmarkDefinition {
        BenchmarkDefinition {
            name: "flush benchmark".to_string(),
            description: "test".to_string(),
            dependencies: vec![crate::BenchmarkDependency::Kafka],
            load: LoadConfiguration {
                duration,
                partitions: 1,
                value_bytes: 1,
                max_backlog_messages: 1,
                wait_timeout_seconds: 1,
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
            ]
            .into_iter()
            .collect(),
            implementations: BTreeMap::from([(
                "nervix".to_string(),
                Implementation::Nervix(NervixImplementation {
                    template: "graph.nspl".into(),
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
    }

    #[test]
    fn scalar_override_drives_duration_and_derived_values() {
        let settings = RunSettings::resolve(
            &definition(LoadDuration::Auto),
            &[
                "emitter_flush_each=250ms".to_string(),
                "emitter_max_batch_size=64MiB".to_string(),
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
    }
}
