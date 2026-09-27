use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
};

use error_stack::Report;
use meticulous::OptionExt as _;
use serde::{Deserialize, Deserializer, de};
use thiserror::Error;

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

/// A load setting that must be positive, named by its manifest key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display)]
pub enum LoadSetting {
    #[strum(serialize = "load.partitions")]
    Partitions,
    #[strum(serialize = "load.warmup_seconds")]
    WarmupSeconds,
    #[strum(serialize = "load.value_bytes")]
    ValueBytes,
    #[strum(serialize = "load.max_backlog_messages")]
    MaxBacklogMessages,
    #[strum(serialize = "load.wait_timeout_seconds")]
    WaitTimeoutSeconds,
    #[strum(serialize = "load.shape.outputs_per_input")]
    OutputsPerInput,
    #[strum(serialize = "load.shape.keys_per_cycle")]
    KeysPerCycle,
    #[strum(serialize = "load.shape.copies_per_key")]
    CopiesPerKey,
    #[strum(serialize = "load.shape.retained_keys")]
    RetainedKeys,
}

/// Why a benchmark manifest describes a benchmark the harness cannot run.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DefinitionError {
    #[error("name must not be empty")]
    EmptyName,
    #[error("description must not be empty")]
    EmptyDescription,
    #[error("dependency '{dependency:?}' is declared more than once")]
    DuplicateDependency { dependency: BenchmarkDependency },
    #[error("the current Kafka-to-Kafka load driver requires dependency 'kafka'")]
    MissingKafkaDependency,
    #[error("{setting} must be positive")]
    NotPositive { setting: LoadSetting },
    #[error("load.partitions exceeds Kafka's supported range")]
    PartitionRange { partitions: u32 },
    #[error("load.shape cycle exceeds the supported message count")]
    CycleOverflow,
    #[error("load.max_backlog_messages must admit one cycle per partition ({partition_cycle})")]
    BacklogBelowCycle { partition_cycle: u64 },
    #[error("load.shape.retained_keys must not exceed load.shape.keys_per_cycle")]
    RetainedKeysExceedCycle,
    #[error("load.shape.count_field must be a lowercase underscore-separated field name")]
    CountField,
    #[error("at least one implementation is required")]
    NoImplementations,
    #[error("implementation name '{implementation}' must be a lowercase hyphenated slug")]
    ImplementationName { implementation: String },
    #[error("nervix implementation '{implementation}' nodes must be between one and three")]
    NervixNodes { implementation: String },
    #[error("container implementation '{implementation}' must declare a non-empty image")]
    ContainerImage { implementation: String },
    #[error(
        "container implementation '{implementation}' config_path must be an absolute file path"
    )]
    ContainerConfigPath { implementation: String },
    #[error("container implementation '{implementation}' command must contain non-empty arguments")]
    ContainerCommand { implementation: String },
    #[error(
        "container implementation '{implementation}' must declare readiness_port and \
         readiness_path together"
    )]
    ContainerReadinessPair { implementation: String },
    #[error("container implementation '{implementation}' readiness_port must be positive")]
    ContainerReadinessPort { implementation: String },
    #[error(
        "container implementation '{implementation}' readiness_path must be an absolute HTTP path \
         without whitespace"
    )]
    ContainerReadinessPath { implementation: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadConfiguration {
    pub duration: LoadDuration,
    pub warmup_seconds: u64,
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

    /// Identical payloads, each copied to a fixed number of output consumers.
    UniformFanout { outputs_per_input: u64 },

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
    pub nodes: u8,
    pub after_start: Option<PathBuf>,
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
        #[serde(default = "default_nervix_nodes")]
        nodes: u8,
        #[serde(default)]
        after_start: Option<PathBuf>,
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
            SerializedImplementation::Nervix {
                template,
                nodes,
                after_start,
            } => Self::Nervix(NervixImplementation {
                template,
                nodes,
                after_start,
            }),
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

const fn default_nervix_nodes() -> u8 {
    1
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
                let seconds = match u64::try_from(value) {
                    Ok(seconds) if seconds > 0 => seconds,
                    _ => return Err(E::invalid_value(de::Unexpected::Signed(value), &self)),
                };
                Ok(LoadDuration::Seconds(seconds))
            }
        }

        deserializer.deserialize_any(LoadDurationVisitor)
    }
}

impl BenchmarkDefinition {
    pub(crate) fn validate(&self, slug: &str) -> error_stack::Result<(), DefinitionError> {
        if self.name.trim().is_empty() {
            return Err(Report::new(DefinitionError::EmptyName));
        }
        if self.description.trim().is_empty() {
            return Err(Report::new(DefinitionError::EmptyDescription));
        }
        let mut dependencies = std::collections::BTreeSet::new();
        for dependency in &self.dependencies {
            if !dependencies.insert(*dependency) {
                return Err(Report::new(DefinitionError::DuplicateDependency {
                    dependency: *dependency,
                }));
            }
        }
        if !dependencies.contains(&BenchmarkDependency::Kafka) {
            return Err(Report::new(DefinitionError::MissingKafkaDependency));
        }
        self.load.validate()?;
        if self.implementations.is_empty() {
            return Err(Report::new(DefinitionError::NoImplementations));
        }
        for (name, implementation) in &self.implementations {
            if !is_slug(name) {
                return Err(Report::new(DefinitionError::ImplementationName {
                    implementation: name.clone(),
                }));
            }
            implementation.validate(name)?;
        }
        debug_assert!(is_slug(slug), "catalog validates benchmark slugs first");
        Ok(())
    }
}

impl LoadConfiguration {
    fn validate(&self) -> error_stack::Result<(), DefinitionError> {
        if self.partitions == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::Partitions,
            }));
        }
        if self.warmup_seconds == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::WarmupSeconds,
            }));
        }
        if i32::try_from(self.partitions).is_err() {
            return Err(Report::new(DefinitionError::PartitionRange {
                partitions: self.partitions,
            }));
        }
        if self.value_bytes == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::ValueBytes,
            }));
        }
        if self.max_backlog_messages == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::MaxBacklogMessages,
            }));
        }
        self.shape.validate()?;
        let Some(partition_cycle) =
            u64::from(self.partitions).checked_mul(self.shape.messages_per_cycle())
        else {
            return Err(Report::new(DefinitionError::CycleOverflow));
        };
        if self.max_backlog_messages < partition_cycle {
            return Err(Report::new(DefinitionError::BacklogBelowCycle {
                partition_cycle,
            }));
        }
        if self.wait_timeout_seconds == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::WaitTimeoutSeconds,
            }));
        }
        Ok(())
    }
}

impl LoadShape {
    /// Input messages the driver writes to one partition as one indivisible unit.
    #[must_use]
    pub fn messages_per_cycle(&self) -> u64 {
        match self {
            Self::UniformPassthrough | Self::UniformFanout { .. } => 1,
            Self::KeyedWindowed {
                keys_per_cycle,
                copies_per_key,
                ..
            } => keys_per_cycle.checked_mul(*copies_per_key).verified(
                "validate rejects a load shape whose cycle exceeds the supported message count",
            ),
        }
    }

    /// Records one complete cycle must produce on the output topic.
    #[must_use]
    pub fn output_records_per_cycle(&self) -> u64 {
        match self {
            Self::UniformPassthrough => 1,
            Self::UniformFanout { outputs_per_input } => *outputs_per_input,
            Self::KeyedWindowed { retained_keys, .. } => *retained_keys,
        }
    }

    /// Records the measured path owes for `cycles` complete cycles, or `None` when the count does
    /// not fit in the report's `u64` counter.
    #[must_use]
    pub fn expected_output_records(&self, cycles: u64) -> Option<u64> {
        cycles.checked_mul(self.output_records_per_cycle())
    }

    /// Input messages `records` output records account for, used as the live backlog signal while
    /// load is being generated.
    #[must_use]
    pub fn input_messages_for_output_records(&self, records: u64) -> u64 {
        (records / self.output_records_per_cycle())
            .checked_mul(self.messages_per_cycle())
            .assured("the records counted here were produced by this same run")
    }

    fn validate(&self) -> error_stack::Result<(), DefinitionError> {
        if let Self::UniformFanout { outputs_per_input } = self {
            if *outputs_per_input == 0 {
                return Err(Report::new(DefinitionError::NotPositive {
                    setting: LoadSetting::OutputsPerInput,
                }));
            }
            return Ok(());
        }
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
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::KeysPerCycle,
            }));
        }
        if *copies_per_key == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::CopiesPerKey,
            }));
        }
        if *retained_keys == 0 {
            return Err(Report::new(DefinitionError::NotPositive {
                setting: LoadSetting::RetainedKeys,
            }));
        }
        if retained_keys > keys_per_cycle {
            return Err(Report::new(DefinitionError::RetainedKeysExceedCycle));
        }
        if keys_per_cycle.checked_mul(*copies_per_key).is_none() {
            return Err(Report::new(DefinitionError::CycleOverflow));
        }
        if count_field.is_empty()
            || !count_field
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Report::new(DefinitionError::CountField));
        }
        Ok(())
    }
}

impl ContainerImplementation {
    fn validate(&self, name: &str) -> error_stack::Result<(), DefinitionError> {
        if self.image.trim().is_empty() {
            return Err(Report::new(DefinitionError::ContainerImage {
                implementation: name.to_string(),
            }));
        }
        let config_path_is_absolute_file = self.config_path.is_absolute()
            && self.config_path.file_name().is_some()
            && !self.config_path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            });
        if !config_path_is_absolute_file {
            return Err(Report::new(DefinitionError::ContainerConfigPath {
                implementation: name.to_string(),
            }));
        }
        if self.command.as_ref().is_some_and(|command| {
            command.is_empty() || command.iter().any(|argument| argument.is_empty())
        }) {
            return Err(Report::new(DefinitionError::ContainerCommand {
                implementation: name.to_string(),
            }));
        }
        if self.readiness_port.is_some() != self.readiness_path.is_some() {
            return Err(Report::new(DefinitionError::ContainerReadinessPair {
                implementation: name.to_string(),
            }));
        }
        if self.readiness_port == Some(0) {
            return Err(Report::new(DefinitionError::ContainerReadinessPort {
                implementation: name.to_string(),
            }));
        }
        if let Some(path) = &self.readiness_path
            && (!path.starts_with('/') || path.contains(char::is_whitespace))
        {
            return Err(Report::new(DefinitionError::ContainerReadinessPath {
                implementation: name.to_string(),
            }));
        }
        Ok(())
    }
}

impl Implementation {
    fn validate(&self, name: &str) -> error_stack::Result<(), DefinitionError> {
        match self {
            Self::Nervix(nervix) => {
                if !(1..=3).contains(&nervix.nodes) {
                    return Err(Report::new(DefinitionError::NervixNodes {
                        implementation: name.to_string(),
                    }));
                }
            }
            Self::Container(container) => container.validate(name)?,
        }
        Ok(())
    }

    pub(crate) fn template(&self) -> &Path {
        match self {
            Self::Nervix(implementation) => implementation.template.as_path(),
            Self::Container(implementation) => implementation.template.as_path(),
        }
    }

    pub(crate) fn after_start_template(&self) -> Option<&Path> {
        match self {
            Self::Nervix(implementation) => implementation.after_start.as_deref(),
            Self::Container(_) => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    const SLUG: &str = "benchmark";

    fn container() -> ContainerImplementation {
        ContainerImplementation {
            image: "vector:tag".to_string(),
            template: "vector.yaml".into(),
            config_path: "/etc/vector/vector.yaml".into(),
            command: None,
            readiness_port: Some(8686),
            readiness_path: Some("/health".to_string()),
        }
    }

    fn keyed(keys_per_cycle: u64, retained_keys: u64, copies_per_key: u64) -> LoadShape {
        LoadShape::KeyedWindowed {
            keys_per_cycle,
            retained_keys,
            copies_per_key,
            count_field: "record_count".to_string(),
        }
    }

    fn definition() -> BenchmarkDefinition {
        BenchmarkDefinition {
            name: "Valid".to_string(),
            description: "Every contract holds".to_string(),
            dependencies: vec![BenchmarkDependency::Kafka],
            load: LoadConfiguration {
                duration: LoadDuration::Auto,
                warmup_seconds: 1,
                partitions: 2,
                value_bytes: 1,
                max_backlog_messages: 2,
                wait_timeout_seconds: 1,
                shape: LoadShape::UniformPassthrough,
            },
            parameters: toml::Table::new(),
            implementations: BTreeMap::from([
                (
                    "nervix".to_string(),
                    Implementation::Nervix(NervixImplementation {
                        template: "graph.nspl.upon".into(),
                        nodes: 1,
                        after_start: None,
                    }),
                ),
                ("vector".to_string(), Implementation::Container(container())),
            ]),
        }
    }

    fn with_container(container: ContainerImplementation) -> BenchmarkDefinition {
        let mut candidate = definition();
        candidate
            .implementations
            .insert("vector".to_string(), Implementation::Container(container));
        candidate
    }

    fn violation(candidate: &BenchmarkDefinition) -> DefinitionError {
        let error = candidate
            .validate(SLUG)
            .expect_err("the definition breaks a contract");
        error.current_context().clone()
    }

    #[test]
    fn a_definition_that_keeps_every_contract_is_valid() {
        definition()
            .validate(SLUG)
            .expect("the definition keeps every contract");
    }

    #[test]
    fn a_definition_names_the_identity_and_dependency_it_breaks() {
        let mut unnamed = definition();
        unnamed.name = " ".to_string();
        assert_eq!(violation(&unnamed), DefinitionError::EmptyName);

        let mut undescribed = definition();
        undescribed.description = String::new();
        assert_eq!(violation(&undescribed), DefinitionError::EmptyDescription);

        let mut duplicated = definition();
        duplicated.dependencies.push(BenchmarkDependency::Kafka);
        assert_eq!(
            violation(&duplicated),
            DefinitionError::DuplicateDependency {
                dependency: BenchmarkDependency::Kafka
            }
        );

        let mut without_kafka = definition();
        without_kafka.dependencies.clear();
        assert_eq!(
            violation(&without_kafka),
            DefinitionError::MissingKafkaDependency
        );
    }

    #[test]
    fn a_load_names_the_setting_it_breaks() {
        let mut no_partitions = definition();
        no_partitions.load.partitions = 0;
        assert_eq!(
            violation(&no_partitions),
            DefinitionError::NotPositive {
                setting: LoadSetting::Partitions
            }
        );
        assert_eq!(
            violation(&no_partitions).to_string(),
            "load.partitions must be positive"
        );

        let mut no_warmup = definition();
        no_warmup.load.warmup_seconds = 0;
        assert_eq!(
            violation(&no_warmup),
            DefinitionError::NotPositive {
                setting: LoadSetting::WarmupSeconds
            }
        );

        let mut too_many_partitions = definition();
        too_many_partitions.load.partitions = u32::MAX;
        assert_eq!(
            violation(&too_many_partitions),
            DefinitionError::PartitionRange {
                partitions: u32::MAX
            }
        );

        let mut no_value = definition();
        no_value.load.value_bytes = 0;
        assert_eq!(
            violation(&no_value),
            DefinitionError::NotPositive {
                setting: LoadSetting::ValueBytes
            }
        );

        let mut no_backlog = definition();
        no_backlog.load.max_backlog_messages = 0;
        assert_eq!(
            violation(&no_backlog),
            DefinitionError::NotPositive {
                setting: LoadSetting::MaxBacklogMessages
            }
        );

        let mut short_backlog = definition();
        short_backlog.load.max_backlog_messages = 1;
        assert_eq!(
            violation(&short_backlog),
            DefinitionError::BacklogBelowCycle { partition_cycle: 2 }
        );

        let mut overflowing_cycle = definition();
        overflowing_cycle.load.partitions = 1 << 30;
        overflowing_cycle.load.shape = keyed(1 << 20, 1, 1 << 20);
        assert_eq!(
            violation(&overflowing_cycle),
            DefinitionError::CycleOverflow
        );

        let mut no_timeout = definition();
        no_timeout.load.wait_timeout_seconds = 0;
        assert_eq!(
            violation(&no_timeout),
            DefinitionError::NotPositive {
                setting: LoadSetting::WaitTimeoutSeconds
            }
        );
    }

    #[test]
    fn a_load_shape_names_the_contract_it_breaks() {
        let cases = [
            (
                LoadShape::UniformFanout {
                    outputs_per_input: 0,
                },
                DefinitionError::NotPositive {
                    setting: LoadSetting::OutputsPerInput,
                },
            ),
            (
                keyed(0, 0, 1),
                DefinitionError::NotPositive {
                    setting: LoadSetting::KeysPerCycle,
                },
            ),
            (
                keyed(1, 1, 0),
                DefinitionError::NotPositive {
                    setting: LoadSetting::CopiesPerKey,
                },
            ),
            (
                keyed(1, 0, 1),
                DefinitionError::NotPositive {
                    setting: LoadSetting::RetainedKeys,
                },
            ),
            (keyed(1, 2, 1), DefinitionError::RetainedKeysExceedCycle),
            (keyed(u64::MAX, 1, 2), DefinitionError::CycleOverflow),
            (
                LoadShape::KeyedWindowed {
                    keys_per_cycle: 1,
                    retained_keys: 1,
                    copies_per_key: 1,
                    count_field: "Record-Count".to_string(),
                },
                DefinitionError::CountField,
            ),
        ];
        for (shape, expected) in cases {
            let mut candidate = definition();
            candidate.load.shape = shape;
            assert_eq!(violation(&candidate), expected);
        }
    }

    #[test]
    fn an_implementation_names_the_contract_it_breaks() {
        let mut empty = definition();
        empty.implementations.clear();
        assert_eq!(violation(&empty), DefinitionError::NoImplementations);

        let mut misnamed = definition();
        misnamed.implementations.insert(
            "Not_A_Slug".to_string(),
            Implementation::Container(container()),
        );
        assert_eq!(
            violation(&misnamed),
            DefinitionError::ImplementationName {
                implementation: "Not_A_Slug".to_string()
            }
        );

        let mut crowded = definition();
        crowded.implementations.insert(
            "nervix".to_string(),
            Implementation::Nervix(NervixImplementation {
                template: "graph.nspl.upon".into(),
                nodes: 4,
                after_start: None,
            }),
        );
        assert_eq!(
            violation(&crowded),
            DefinitionError::NervixNodes {
                implementation: "nervix".to_string()
            }
        );

        let implementation = || "vector".to_string();
        let cases = [
            (
                ContainerImplementation {
                    image: " ".to_string(),
                    ..container()
                },
                DefinitionError::ContainerImage {
                    implementation: implementation(),
                },
            ),
            (
                ContainerImplementation {
                    config_path: "/etc/vector/../vector.yaml".into(),
                    ..container()
                },
                DefinitionError::ContainerConfigPath {
                    implementation: implementation(),
                },
            ),
            (
                ContainerImplementation {
                    command: Some(vec!["--config".to_string(), String::new()]),
                    ..container()
                },
                DefinitionError::ContainerCommand {
                    implementation: implementation(),
                },
            ),
            (
                ContainerImplementation {
                    readiness_path: None,
                    ..container()
                },
                DefinitionError::ContainerReadinessPair {
                    implementation: implementation(),
                },
            ),
            (
                ContainerImplementation {
                    readiness_port: Some(0),
                    ..container()
                },
                DefinitionError::ContainerReadinessPort {
                    implementation: implementation(),
                },
            ),
            (
                ContainerImplementation {
                    readiness_path: Some("/health check".to_string()),
                    ..container()
                },
                DefinitionError::ContainerReadinessPath {
                    implementation: implementation(),
                },
            ),
        ];
        for (container, expected) in cases {
            assert_eq!(violation(&with_container(container)), expected);
        }
    }
}
