use std::{collections::BTreeMap, fmt, path::PathBuf};

use serde::{Deserialize, Deserializer, de};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkDefinition {
    pub name: String,
    pub description: String,
    pub dependencies: Vec<BenchmarkDependency>,
    pub load: LoadConfiguration,
    pub parameters: toml::Table,
    #[serde(default)]
    pub implementations: BTreeMap<String, Implementation>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkDependency {
    Kafka,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadConfiguration {
    pub duration: LoadDuration,
    pub partitions: u32,
    pub value_bytes: u64,
    pub max_backlog_messages: u64,
    pub wait_timeout_seconds: u64,
    pub shape: LoadShape,
}

/// The payload the load driver generates and the output the measured path owes it in return.
///
/// The driver produces indivisible *cycles* of input messages, each cycle written to one Kafka
/// partition, and every shape declares how many output records one complete cycle must yield.
/// Parity is exact against that contract, so a shape whose graph drops records has to state the
/// drop rate here rather than assume one output for every input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum LoadShape {
    /// Identical payloads, one output message for every accepted input message.
    UniformPassthrough,

    /// Cycles of distinct keys, each key produced `copies_per_key` times, with `retained_keys` of
    /// every `keys_per_cycle` keys carrying the retain marker in their padded value. A cycle
    /// therefore survives filtering and deduplication as exactly `retained_keys` records, which a
    /// window processor aggregates into summaries carrying `count_field`.
    KeyedWindowed {
        keys_per_cycle: u64,
        retained_keys: u64,
        copies_per_key: u64,
        count_field: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadDuration {
    Auto,
    Seconds(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Implementation {
    Nervix(NervixImplementation),
    Container(ContainerImplementation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NervixImplementation {
    pub template: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerImplementation {
    pub image: String,
    pub template: PathBuf,
    pub config_path: PathBuf,
    pub command: Option<Vec<String>>,
    pub readiness_port: Option<u16>,
    pub readiness_path: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SerializedImplementation {
    Nervix {
        template: PathBuf,
    },
    Container {
        image: String,
        template: PathBuf,
        config_path: PathBuf,
        command: Option<Vec<String>>,
        readiness_port: Option<u16>,
        readiness_path: Option<String>,
    },
}

impl<'de> Deserialize<'de> for Implementation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match SerializedImplementation::deserialize(deserializer)? {
            SerializedImplementation::Nervix { template } => {
                Self::Nervix(NervixImplementation { template })
            }
            SerializedImplementation::Container {
                image,
                template,
                config_path,
                command,
                readiness_port,
                readiness_path,
            } => Self::Container(ContainerImplementation {
                image,
                template,
                config_path,
                command,
                readiness_port,
                readiness_path,
            }),
        })
    }
}

impl<'de> Deserialize<'de> for LoadDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LoadDurationVisitor;

        impl<'de> de::Visitor<'de> for LoadDurationVisitor {
            type Value = LoadDuration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("the string 'auto' or a positive integer number of seconds")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "auto" {
                    Ok(LoadDuration::Auto)
                } else {
                    Err(E::invalid_value(de::Unexpected::Str(value), &self))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value > 0 {
                    Ok(LoadDuration::Seconds(value))
                } else {
                    Err(E::invalid_value(de::Unexpected::Unsigned(value), &self))
                }
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                u64::try_from(value)
                    .ok()
                    .filter(|value| *value > 0)
                    .map(LoadDuration::Seconds)
                    .ok_or_else(|| E::invalid_value(de::Unexpected::Signed(value), &self))
            }
        }

        deserializer.deserialize_any(LoadDurationVisitor)
    }
}

impl BenchmarkDefinition {
    pub(crate) fn validate(&self, slug: &str) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("name must not be empty".to_string());
        }
        if self.description.trim().is_empty() {
            return Err("description must not be empty".to_string());
        }
        let mut dependencies = std::collections::BTreeSet::new();
        for dependency in &self.dependencies {
            if !dependencies.insert(*dependency) {
                return Err(format!(
                    "dependency '{dependency:?}' is declared more than once"
                ));
            }
        }
        if !dependencies.contains(&BenchmarkDependency::Kafka) {
            return Err(
                "the current Kafka-to-Kafka load driver requires dependency 'kafka'".to_string(),
            );
        }
        if self.load.partitions == 0 {
            return Err("load.partitions must be positive".to_string());
        }
        if self.load.partitions > i32::MAX as u32 {
            return Err("load.partitions exceeds Kafka's supported range".to_string());
        }
        if self.load.value_bytes == 0 {
            return Err("load.value_bytes must be positive".to_string());
        }
        if self.load.max_backlog_messages == 0 {
            return Err("load.max_backlog_messages must be positive".to_string());
        }
        self.load.shape.validate()?;
        let partition_cycle = u64::from(self.load.partitions)
            .checked_mul(self.load.shape.messages_per_cycle())
            .ok_or_else(|| "load.shape cycle exceeds the supported message count".to_string())?;
        if self.load.max_backlog_messages < partition_cycle {
            return Err(format!(
                "load.max_backlog_messages must admit one cycle per partition ({partition_cycle})"
            ));
        }
        if self.load.wait_timeout_seconds == 0 {
            return Err("load.wait_timeout_seconds must be positive".to_string());
        }
        if self.implementations.is_empty() {
            return Err("at least one implementation is required".to_string());
        }
        for (name, implementation) in &self.implementations {
            if !is_slug(name) {
                return Err(format!(
                    "implementation name '{name}' must be a lowercase hyphenated slug"
                ));
            }
            if let Implementation::Container(container) = implementation {
                if container.image.trim().is_empty() {
                    return Err(format!(
                        "container implementation '{name}' must declare a non-empty image"
                    ));
                }
                if !container.config_path.is_absolute()
                    || container.config_path.file_name().is_none()
                    || container.config_path.components().any(|component| {
                        matches!(
                            component,
                            std::path::Component::CurDir | std::path::Component::ParentDir
                        )
                    })
                {
                    return Err(format!(
                        "container implementation '{name}' config_path must be an absolute file \
                         path"
                    ));
                }
                if container.command.as_ref().is_some_and(|command| {
                    command.is_empty() || command.iter().any(|argument| argument.is_empty())
                }) {
                    return Err(format!(
                        "container implementation '{name}' command must contain non-empty \
                         arguments"
                    ));
                }
                if container.readiness_port.is_some() != container.readiness_path.is_some() {
                    return Err(format!(
                        "container implementation '{name}' must declare readiness_port and \
                         readiness_path together"
                    ));
                }
                if container.readiness_port == Some(0) {
                    return Err(format!(
                        "container implementation '{name}' readiness_port must be positive"
                    ));
                }
                if let Some(path) = &container.readiness_path
                    && (!path.starts_with('/') || path.contains(char::is_whitespace))
                {
                    return Err(format!(
                        "container implementation '{name}' readiness_path must be an absolute \
                         HTTP path without whitespace"
                    ));
                }
            }
        }
        debug_assert!(is_slug(slug), "catalog validates benchmark slugs first");
        Ok(())
    }
}

impl LoadShape {
    /// Input messages the driver writes to one partition as one indivisible unit.
    #[must_use]
    pub fn messages_per_cycle(&self) -> u64 {
        match self {
            Self::UniformPassthrough => 1,
            Self::KeyedWindowed {
                keys_per_cycle,
                copies_per_key,
                ..
            } => keys_per_cycle.saturating_mul(*copies_per_key),
        }
    }

    /// Records one complete cycle must produce on the output topic.
    #[must_use]
    pub fn output_records_per_cycle(&self) -> u64 {
        match self {
            Self::UniformPassthrough => 1,
            Self::KeyedWindowed { retained_keys, .. } => *retained_keys,
        }
    }

    /// Records the measured path owes for `cycles` complete cycles.
    #[must_use]
    pub fn expected_output_records(&self, cycles: u64) -> u64 {
        cycles.saturating_mul(self.output_records_per_cycle())
    }

    /// Input messages `records` output records account for, used as the live backlog signal while
    /// load is being generated.
    #[must_use]
    pub fn input_messages_for_output_records(&self, records: u64) -> u64 {
        (records / self.output_records_per_cycle()).saturating_mul(self.messages_per_cycle())
    }

    fn validate(&self) -> Result<(), String> {
        let Self::KeyedWindowed {
            keys_per_cycle,
            retained_keys,
            copies_per_key,
            count_field,
        } = self
        else {
            return Ok(());
        };
        if *keys_per_cycle == 0 {
            return Err("load.shape.keys_per_cycle must be positive".to_string());
        }
        if *copies_per_key == 0 {
            return Err("load.shape.copies_per_key must be positive".to_string());
        }
        if *retained_keys == 0 {
            return Err("load.shape.retained_keys must be positive".to_string());
        }
        if retained_keys > keys_per_cycle {
            return Err(
                "load.shape.retained_keys must not exceed load.shape.keys_per_cycle".to_string(),
            );
        }
        if keys_per_cycle.checked_mul(*copies_per_key).is_none() {
            return Err("load.shape cycle exceeds the supported message count".to_string());
        }
        if count_field.is_empty()
            || !count_field
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(
                "load.shape.count_field must be a lowercase underscore-separated field name"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl Implementation {
    pub(crate) fn template(&self) -> &PathBuf {
        match self {
            Self::Nervix(implementation) => &implementation.template,
            Self::Container(implementation) => &implementation.template,
        }
    }
}

pub(crate) fn is_slug(value: &str) -> bool {
    !value.is_empty()
        && value.split('-').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
}
