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

use crate::SettingDefinition;

#[doc(hidden)]
pub mod value;
mod value_archive;
use value::{ParameterSetting, ParameterValue};
mod durations;
mod limits;
mod publication;
mod remote_ack;
pub use remote_ack::*;
#[cfg(test)]
mod tests;
mod tls;

pub use durations::*;
pub use limits::*;
pub use publication::*;
pub use tls::*;

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
    AlgorithmNames,
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
    #[error(
        "remote-ack-silence-timeout must exceed twice remote-ack-dispatch-timeout plus \
         remote-ack-alive-interval"
    )]
    AckReportWindow,
    #[error(
        "remote-ack-sweep-interval must divide remote-ack-silence-timeout and \
         remote-relay-total-timeout exactly"
    )]
    AckSweepDivisibility,
    #[error(
        "remote-relay-total-timeout must cover remote-ack-dispatch-timeout and \
         remote-relay-instantiation-wait"
    )]
    AckAdmissionWindow,
    #[error(
        "{parameter} ({requested:?}) exceeds node '{node}' drain/shutdown budget ({available:?})"
    )]
    NodeDeadline {
        parameter: ClusterParameterKey,
        node: crate::ClusterNodeName,
        requested: Duration,
        available: Duration,
    },
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
    #[error("invalid {parameter}")]
    TlsSelection { parameter: ClusterParameterKey },
    #[error("tls-cipher-suites ({suites}) must include a TLS 1.3 cipher suite")]
    NoTls13CipherSuite {
        suites: nervix_crypto_policy::TlsCipherSuites,
    },
    #[error(
        "tls-key-exchange-groups ({groups}) must include a TLS 1.2 group for tls-cipher-suites \
         ({suites})"
    )]
    NoTls12KeyExchangeGroup {
        suites: nervix_crypto_policy::TlsCipherSuites,
        groups: nervix_crypto_policy::TlsKeyExchangeGroups,
    },
}

/// One parameter and its printable current value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedClusterParameter {
    pub parameter: ClusterParameterKey,
    pub value: String,
}

macro_rules! parameter_text {
    ($value:expr, Duration) => {
        crate::timing_text(*$value)
    };
    ($value:expr, Count) => {
        $value.to_string()
    };
    ($value:expr, Bytes) => {
        format!("{}B", $value)
    };
    ($value:expr, AlgorithmNames) => {{
        let names: Vec<_> = $value
            .iter()
            .map(|algorithm| format!("'{}'", algorithm.name()))
            .collect();
        format!("({})", names.join(", "))
    }};
}

macro_rules! parameter_cli_text {
    ($value:expr, AlgorithmNames) => {
        $value.to_string()
    };
    ($value:expr, $kind:ident) => {
        parameter_text!($value, $kind)
    };
}

macro_rules! parameter_storage {
    ($value:ty) => {
        <$value as ParameterValue>::Stored
    };
    ($value:ty, $stored:ty) => {
        $stored
    };
}

macro_rules! parameter_values {
    ($($field:ident: $definition:ident($value:ty) => $kind:ident $(<$stored:ty>)?,)+) => {
        /// The complete cluster policy. Each field has one class, default and validation owner.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
        #[serde(deny_unknown_fields)]
        pub struct ClusterParameterValues {
            $(
                #[serde(with = "value_archive")]
                #[rkyv(with = value_archive::ValidatedSetting)]
                $field: ParameterSetting<$definition, parameter_storage!($value $(, $stored)?)>,
            )+
        }

        impl<'de> Deserialize<'de> for ClusterParameterValues {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                use serde::de::Error as _;
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Fields { $(#[serde(with = "value_archive")] $field: ParameterSetting<$definition, parameter_storage!($value $(, $stored)?)>,)+ }
                let fields = Fields::deserialize(deserializer)?;
                let values = Self {
                    $($field: fields.$field,)+
                };
                values.validate_relationships().map_err(|error| D::Error::custom(format_args!("{error:#}")))?;
                Ok(values)
            }
        }

        impl<D> RkyvDeserialize<ClusterParameterValues, D> for ArchivedClusterParameterValues
        where D: Fallible + ?Sized, D::Error: Source,
        {
            fn deserialize(&self, deserializer: &mut D) -> Result<ClusterParameterValues, D::Error> {
                let values = ClusterParameterValues {
                    $($field: ParameterSetting::from_stored(self.$field.deserialize(deserializer)?)
                        .map_err(|error| D::Error::new(error.into_error()))?,)+
                };
                values.validate_relationships().map_err(|error| D::Error::new(error.into_error()))?;
                Ok(values)
            }
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
                let values = Self { $($field: ParameterSetting::resolve(seed.$field, &())?,)+ };
                values.validate_relationships()?;
                Ok(values)
            }

            $(pub fn $field(&self) -> $value { self.$field.value() })+

            /// Validate every proposed value before returning the complete candidate.
            pub fn changed(&self, updates: &ClusterParameterUpdates) -> Result<Self, Report<ClusterParameterError>> {
                let mut next = *self;
                for update in updates.iter() {
                    match update { $(ClusterParameterChange::$definition(value) => { next.$field = ParameterSetting::new(*value, &())?; },)+ }
                }
                next.validate_relationships()?;
                Ok(next)
            }

            pub fn assignments(&self) -> ClusterParameterUpdates {
                ClusterParameterUpdates(vec![$(ClusterParameterChange::$definition(self.$field()),)+])
            }

            pub fn described(&self) -> Vec<DescribedClusterParameter> {
                vec![$(
                    DescribedClusterParameter {
                        parameter: ClusterParameterKey::$definition,
                        value: parameter_text!(&self.$field.value(), $kind),
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
            pub fn default_cli_text(self) -> String {
                match self { $(Self::$definition => {
                    let value = $definition::default_value(&());
                    parameter_cli_text!(&value, $kind)
                },)+ }
            }

            pub fn default_text(self) -> String {
                match self { $(Self::$definition => {
                    let value = $definition::default_value(&());
                    parameter_text!(&value, $kind)
                },)+ }
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
                match self { $(Self::$definition(value) => parameter_text!(value, $kind),)+ }
            }
        }
    };
}

parameter_values! {
    licensing_request_timeout: LicensingRequestTimeout(Duration) => Duration,
    licensing_retry_backoff_max: LicensingRetryBackoffMax(Duration) => Duration,
    licensing_stream_idle_timeout: LicensingStreamIdleTimeout(Duration) => Duration,
    payload_unfold_limit: PayloadUnfoldLimit(u64) => Count<u32>,
    remote_ack_alive_interval: RemoteAckAliveInterval(Duration) => Duration,
    remote_ack_dispatch_timeout: RemoteAckDispatchTimeout(Duration) => Duration,
    remote_ack_silence_timeout: RemoteAckSilenceTimeout(Duration) => Duration,
    remote_ack_sweep_interval: RemoteAckSweepInterval(Duration) => Duration,
    remote_relay_total_timeout: RemoteRelayTotalTimeout(Duration) => Duration,
    remote_relay_instantiation_wait: RemoteRelayInstantiationWait(Duration) => Duration,
    node_unavailability_timeout: NodeUnavailabilityTimeout(Duration) => Duration,
    domain_drain_timeout: DomainDrainTimeout(Duration) => Duration,
    entity_gate_deadline: EntityGateDeadline(Duration) => Duration,
    transaction_idle_timeout: TransactionIdleTimeout(Duration) => Duration,
    transaction_tombstone_retention: TransactionTombstoneRetention(Duration) => Duration,
    command_retry_validity: CommandRetryValidity(Duration) => Duration,
    state_snapshot_interval: StateSnapshotInterval(Duration) => Duration,
    transaction_max_statements: TransactionMaxStatements(u64) => Count<u32>,
    transaction_max_source_bytes: TransactionMaxSourceBytes(u64) => Bytes,
    transaction_max_open: TransactionMaxOpen(u64) => Count<u32>,
    command_execution_capacity: CommandExecutionCapacity(u64) => Count<u32>,
    replica_count: ReplicaCount(u64) => Count<u32>,
    resource_max_archive_bytes: ResourceMaxArchiveBytes(u64) => Bytes,
    resource_max_extracted_bytes: ResourceMaxExtractedBytes(u64) => Bytes,
    resource_max_file_count: ResourceMaxFileCount(u64) => Count,
    max_batch_messages: MaxBatchMessages(u64) => Count<u32>,
    client_producer_session_bytes: ClientProducerSessionBytes(u64) => Bytes,
    client_consumer_session_bytes: ClientConsumerSessionBytes(u64) => Bytes,
    udf_code_bytes: UdfCodeBytes(u64) => Bytes,
    http_header_bytes: HttpHeaderBytes(u64) => Bytes,
    http_header_fields: HttpHeaderFields(u64) => Count<u32>,
    client_batch_rows: ClientBatchRows(u64) => Count<u32>,
    tls_cipher_suites: TlsCipherSuites(nervix_crypto_policy::TlsCipherSuites) => AlgorithmNames,
    tls_key_exchange_groups: TlsKeyExchangeGroups(nervix_crypto_policy::TlsKeyExchangeGroups) => AlgorithmNames,
    password_iterations: PasswordIterations(u64) => Count<u32>,
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
        crate::HttpHeaderLimits::new(
            crate::Setting::new(self.http_header_bytes(), &())
                .assured("the committed byte bound is validated"),
            crate::Setting::new(self.http_header_fields(), &())
                .assured("the committed field bound is validated"),
        )
    }
}

impl ClusterParameterValues {
    fn validate_relationships(&self) -> Result<(), Report<ClusterParameterError>> {
        self.validate_tls()?;
        RemoteAckTiming::from_parameters(self).validate()
    }

    /// Validate the local lifecycle budgets captured in the same commit fence as the policy.
    pub fn validate_runtime_capabilities<'a>(
        &self,
        records: impl IntoIterator<Item = &'a crate::NodeCapabilityRecord>,
    ) -> Result<(), Report<ClusterParameterError>> {
        for record in records {
            self.validate_runtime_budget(record.identity.node_id(), record.capabilities.shutdown)?;
        }
        Ok(())
    }

    /// Apply the same admission contract before a node publishes its incarnation-bound claim.
    pub fn validate_runtime_budget(
        &self,
        node: &crate::ClusterNodeName,
        shutdown: crate::ShutdownCapabilities,
    ) -> Result<(), Report<ClusterParameterError>> {
        let available = shutdown.drain_timeout.min(shutdown.shutdown_timeout);
        for (parameter, requested) in [
            (
                ClusterParameterKey::DomainDrainTimeout,
                self.domain_drain_timeout(),
            ),
            (
                ClusterParameterKey::EntityGateDeadline,
                self.entity_gate_deadline(),
            ),
        ] {
            if requested > available {
                return Err(Report::new(ClusterParameterError::NodeDeadline {
                    parameter,
                    node: node.clone(),
                    requested,
                    available,
                }));
            }
        }
        Ok(())
    }
}
