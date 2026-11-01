//! Semantic cluster policy and structured administrative Plexus assignments.
//!
//! Layer: vocabulary.
//! - **Owns.** The class-typed parameter values, their definitions, unique assignments and encodings.
//! - **Depends on.** Semantic durations, setting classes, names and serialization.
//! - **Must not know.** Parsing, consensus storage, validation of graphs or service installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeSet, fmt, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
};
use serde::{Deserialize, Serialize};
use strum::EnumIter;
use thiserror::Error;

use crate::{Setting, SettingDefinition};

mod archive;
mod durations;
mod limits;
mod publication;
#[cfg(test)]
mod tests;

pub use durations::*;
pub use limits::*;
pub use publication::*;

/// One committed parameter publication. Revisions order installations and gossip observations.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ClusterParameterRecord {
    pub revision: std::num::NonZeroU64,
    pub values: ClusterParameterValues,
}

/// The literal family a parameter accepts in Plexus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterParameterValueKind {
    Duration,
    Count,
    Bytes,
}

/// A pure parameter construction or assignment refusal.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Error,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "parameter construction returns typed value and assignment refusals"
    )
)]
pub enum ClusterParameterError {
    #[error("{parameter} must be greater than zero")]
    NonPositiveDuration { parameter: ClusterParameterKey },
    #[error("{parameter} must not exceed {maximum:?}")]
    DurationTooLong {
        parameter: ClusterParameterKey,
        maximum: Duration,
    },
    #[error("{parameter} must be between {minimum} and {maximum}, found {value}")]
    OutOfRange {
        parameter: ClusterParameterKey,
        value: u64,
        minimum: u64,
        maximum: u64,
    },
    #[error("ALTER CLUSTER SET requires at least one parameter")]
    Empty,
    #[error("ALTER CLUSTER SET assigns {parameter} more than once")]
    Duplicate { parameter: ClusterParameterKey },
}

/// One parameter and its printable current value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedClusterParameter {
    pub parameter: ClusterParameterKey,
    pub value: String,
}

macro_rules! parameter_values {
    ($($field:ident: $definition:ident($value:ty) => $kind:ident,)+) => {
        /// The complete cluster policy. Each field has one class, default and validation owner.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
        #[serde(deny_unknown_fields)]
        pub struct ClusterParameterValues {
            $(
                #[serde(with = "archive")]
                #[rkyv(with = archive::ValidatedSetting)]
                $field: Setting<$definition>,
            )+
        }

        /// Optional first-bootstrap inputs; absence takes the definition's own default.
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct ClusterParameterSeedValues {
            $(pub $field: Option<$value>,)+
        }

        impl ClusterParameterSeedValues {
            pub fn explicit_parameters(&self) -> Vec<ClusterParameterKey> {
                let mut keys = Vec::new();
                $(if self.$field.is_some() { keys.push(ClusterParameterKey::$definition); })+
                keys
            }

            pub fn retain_sources<'a>(&mut self, source: &Self, additional: impl IntoIterator<Item = &'a str>) {
                let additional: BTreeSet<&str> = additional.into_iter().collect();
                $(if source.$field.is_none() && !additional.contains(&$definition::NAME) { self.$field = None; })+
            }
        }

        /// A parameter's semantic identity, shared by assignments, diagnostics and descriptions.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, EnumIter, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
        pub enum ClusterParameterKey { $($definition,)+ }

        /// One proposed value. The variant determines its value type.
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
        pub enum ClusterParameterChange { $($definition($value),)+ }

        impl ClusterParameterValues {
            pub fn resolve(seed: ClusterParameterSeedValues) -> Result<Self, Report<ClusterParameterError>> {
                Ok(Self { $($field: Setting::resolve(seed.$field, &())?,)+ })
            }

            $(pub fn $field(&self) -> $value { *self.$field.value() })+

            /// Validate every proposed value before returning the complete candidate.
            pub fn changed(&self, updates: &ClusterParameterUpdates) -> Result<Self, Report<ClusterParameterError>> {
                let mut next = *self;
                for update in updates.iter() {
                    match update { $(ClusterParameterChange::$definition(value) => { next.$field = Setting::new(*value, &())?; },)+ }
                }
                Ok(next)
            }

            pub fn assignments(&self) -> ClusterParameterUpdates {
                ClusterParameterUpdates(vec![$(ClusterParameterChange::$definition(self.$field()),)+])
            }

            pub fn described(&self) -> Vec<DescribedClusterParameter> {
                vec![$(
                    DescribedClusterParameter {
                        parameter: ClusterParameterKey::$definition,
                        value: self.$field.value().display_parameter(ClusterParameterValueKind::$kind),
                    },
                )+]
            }
        }

        impl Default for ClusterParameterValues {
            fn default() -> Self {
                Self::resolve(ClusterParameterSeedValues::default())
                    .assured("every cluster parameter default is within its definition's declared bounds")
            }
        }

        impl ClusterParameterKey {
            pub fn default_text(self) -> String {
                match self { $(Self::$definition => $definition::default_value(&()).display_parameter(ClusterParameterValueKind::$kind),)+ }
            }
            pub const fn setting_name(self) -> &'static str {
                match self { $(Self::$definition => $definition::NAME,)+ }
            }

            pub const fn value_kind(self) -> ClusterParameterValueKind {
                match self { $(Self::$definition => ClusterParameterValueKind::$kind,)+ }
            }
        }

        impl ClusterParameterChange {
            pub const fn parameter(&self) -> ClusterParameterKey {
                match self { $(Self::$definition(_) => ClusterParameterKey::$definition,)+ }
            }

            pub fn value_text(&self) -> String {
                match self { $(Self::$definition(value) => value.display_parameter(ClusterParameterValueKind::$kind),)+ }
            }
        }
    };
}

parameter_values! {
    node_unavailability_timeout: NodeUnavailabilityTimeout(Duration) => Duration,
    domain_drain_timeout: DomainDrainTimeout(Duration) => Duration,
    entity_gate_deadline: EntityGateDeadline(Duration) => Duration,
    transaction_idle_timeout: TransactionIdleTimeout(Duration) => Duration,
    transaction_tombstone_retention: TransactionTombstoneRetention(Duration) => Duration,
    command_retry_validity: CommandRetryValidity(Duration) => Duration,
    state_snapshot_interval: StateSnapshotInterval(Duration) => Duration,
    transaction_max_statements: TransactionMaxStatements(u64) => Count,
    transaction_max_source_bytes: TransactionMaxSourceBytes(u64) => Bytes,
    transaction_max_open: TransactionMaxOpen(u64) => Count,
    command_execution_capacity: CommandExecutionCapacity(u64) => Count,
    replica_count: ReplicaCount(u64) => Count,
    resource_max_archive_bytes: ResourceMaxArchiveBytes(u64) => Bytes,
    resource_max_extracted_bytes: ResourceMaxExtractedBytes(u64) => Bytes,
    resource_max_file_count: ResourceMaxFileCount(u64) => Count,
    max_batch_messages: MaxBatchMessages(u64) => Count,
    client_producer_session_bytes: ClientProducerSessionBytes(u64) => Bytes,
    client_consumer_session_bytes: ClientConsumerSessionBytes(u64) => Bytes,
    udf_code_bytes: UdfCodeBytes(u64) => Bytes,
    http_header_bytes: HttpHeaderBytes(u64) => Bytes,
    http_header_fields: HttpHeaderFields(u64) => Count,
    client_batch_rows: ClientBatchRows(u64) => Count,
}

trait ParameterDisplay {
    fn display_parameter(&self, kind: ClusterParameterValueKind) -> String;
}

impl ParameterDisplay for Duration {
    fn display_parameter(&self, _: ClusterParameterValueKind) -> String {
        crate::timing_text(*self)
    }
}

impl ParameterDisplay for u64 {
    fn display_parameter(&self, kind: ClusterParameterValueKind) -> String {
        match kind {
            ClusterParameterValueKind::Bytes => format!("{self}B"),
            ClusterParameterValueKind::Count | ClusterParameterValueKind::Duration => {
                self.to_string()
            }
        }
    }
}

impl ClusterParameterKey {
    pub fn keyword(self) -> String {
        self.setting_name().replace('-', "_").to_ascii_uppercase()
    }

    fn validate_duration(self, value: Duration) -> Result<(), Report<ClusterParameterError>> {
        if value.is_zero() {
            return Err(Report::new(ClusterParameterError::NonPositiveDuration {
                parameter: self,
            }));
        }
        let maximum = Duration::from_secs(u64::from(u32::MAX));
        if value > maximum {
            return Err(Report::new(ClusterParameterError::DurationTooLong {
                parameter: self,
                maximum,
            }));
        }
        Ok(())
    }

    fn validate_count(
        self,
        value: u64,
        minimum: u64,
        maximum: u64,
    ) -> Result<(), Report<ClusterParameterError>> {
        if value < minimum || value > maximum {
            return Err(Report::new(ClusterParameterError::OutOfRange {
                parameter: self,
                value,
                minimum,
                maximum,
            }));
        }
        Ok(())
    }
}

impl fmt::Display for ClusterParameterKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.setting_name())
    }
}

/// At least one assignment, with every parameter present once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize)]
#[serde(
    try_from = "Vec<ClusterParameterChange>",
    into = "Vec<ClusterParameterChange>"
)]
pub struct ClusterParameterUpdates(Vec<ClusterParameterChange>);

impl ClusterParameterUpdates {
    pub fn iter(&self) -> impl Iterator<Item = &ClusterParameterChange> {
        self.0.iter()
    }
}

impl TryFrom<Vec<ClusterParameterChange>> for ClusterParameterUpdates {
    type Error = Report<ClusterParameterError>;

    fn try_from(updates: Vec<ClusterParameterChange>) -> Result<Self, Self::Error> {
        if updates.is_empty() {
            return Err(Report::new(ClusterParameterError::Empty));
        }
        let mut assigned = BTreeSet::new();
        for update in &updates {
            let parameter = update.parameter();
            if !assigned.insert(parameter) {
                return Err(Report::new(ClusterParameterError::Duplicate { parameter }));
            }
        }
        Ok(Self(updates))
    }
}

impl From<ClusterParameterUpdates> for Vec<ClusterParameterChange> {
    fn from(updates: ClusterParameterUpdates) -> Self {
        updates.0
    }
}

impl<D> RkyvDeserialize<ClusterParameterUpdates, D> for ArchivedClusterParameterUpdates
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ClusterParameterUpdates, D::Error> {
        let values: Vec<ClusterParameterChange> = self.0.deserialize(deserializer)?;
        ClusterParameterUpdates::try_from(values)
            .map_err(|report| D::Error::new(report.into_error()))
    }
}

/// One atomic cluster policy transition, independent of a domain transaction.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct AlterCluster {
    pub parameters: ClusterParameterUpdates,
}

impl AlterCluster {
    pub fn to_canonical_plexus(&self) -> String {
        let assignments: Vec<_> = self
            .parameters
            .iter()
            .map(|change| format!("{} = {}", change.parameter().keyword(), change.value_text()))
            .collect();
        format!("ALTER CLUSTER SET {};", assignments.join(", "))
    }
}

impl ClusterParameterValues {
    /// The validated bounds a host captures together for one HTTP publication.
    pub fn http_header_limits(&self) -> crate::HttpHeaderLimits {
        crate::HttpHeaderLimits::new(self.http_header_bytes, self.http_header_fields)
    }
}
