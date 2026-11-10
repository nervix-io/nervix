//! Cluster parameter duration definitions and their positive bounds.
//!
//! Layer: vocabulary.
//! - **Owns.** Cluster parameter duration definitions and their positive bounds.
//! - **Depends on.** Setting classes, semantic values and typed validation reports.
//! - **Must not know.** Parsing, consensus, command-line options or running services.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{ClusterParameterError, ClusterParameterKey};
use crate::{ClusterParameters, SettingDefinition};

/// How long an unobserved member remains available.
pub struct NodeUnavailabilityTimeout;

impl SettingDefinition for NodeUnavailabilityTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "node-unavailability-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(10)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::NodeUnavailabilityTimeout.validate_duration(*value)
    }
}

/// How long a domain drain may wait for accepted work.
pub struct DomainDrainTimeout;

impl SettingDefinition for DomainDrainTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "domain-drain-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::DomainDrainTimeout.validate_duration(*value)
    }
}

/// How long an entity gate may take to engage and drain.
pub struct EntityGateDeadline;

impl SettingDefinition for EntityGateDeadline {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "entity-gate-deadline";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(60)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::EntityGateDeadline.validate_duration(*value)
    }
}

/// How long an open transaction may wait for its next statement.
pub struct TransactionIdleTimeout;

impl SettingDefinition for TransactionIdleTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "transaction-idle-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(900)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::TransactionIdleTimeout.validate_duration(*value)
    }
}

/// The minimum retention of a terminal transaction.
pub struct TransactionTombstoneRetention;

impl SettingDefinition for TransactionTombstoneRetention {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "transaction-tombstone-retention";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(900)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::TransactionTombstoneRetention.validate_duration(*value)
    }
}

/// How long a completed command remains recoverable by its execution reference.
pub struct CommandRetryValidity;

impl SettingDefinition for CommandRetryValidity {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "command-retry-validity";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(900)
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::CommandRetryValidity.validate_duration(*value)
    }
}

/// How often a runtime state owner publishes a checkpoint.
pub struct StateSnapshotInterval;

impl SettingDefinition for StateSnapshotInterval {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "state-snapshot-interval";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Self::DEFAULT
    }

    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::StateSnapshotInterval.validate_duration(*value)
    }
}

impl DomainDrainTimeout {
    /// The compile-time source of this setting definition's default.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the setting definition owns this compile-time default, not an additional \
                      policy input"
        )
    )]
    pub const DEFAULT: Duration = Duration::from_secs(60);
}

impl StateSnapshotInterval {
    /// The compile-time source of this setting definition's default.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the setting definition owns this compile-time default, not an additional \
                      policy input"
        )
    )]
    pub const DEFAULT: Duration = Duration::from_secs(30);
}

/// Budget for a licensing protocol request, including name resolution.
pub struct LicensingRequestTimeout;
impl SettingDefinition for LicensingRequestTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "licensing-request-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::LicensingRequestTimeout.validate_duration(*value)
    }
}

/// Maximum backoff between licensing request retries.
pub struct LicensingRetryBackoffMax;
impl SettingDefinition for LicensingRetryBackoffMax {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "licensing-retry-backoff-max";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(600)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::LicensingRetryBackoffMax.validate_duration(*value)
    }
}

/// Maximum idle interval on the licensing notification stream.
pub struct LicensingStreamIdleTimeout;
impl SettingDefinition for LicensingStreamIdleTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "licensing-stream-idle-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(90)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::LicensingStreamIdleTimeout.validate_duration(*value)
    }
}

/// Exact compact duration carried by an ownership-recovery request. The receiving boundary
/// validates its policy range before establishing the deadline. The transport value preserves
/// nanoseconds in eight bytes without allocating.
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
#[serde(try_from = "Duration", into = "Duration")]
pub struct ClusterParameterDuration(u64);

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("cluster duration {value:?} cannot be represented as u64 nanoseconds")]
pub struct ClusterParameterDurationError {
    value: Duration,
}

impl TryFrom<Duration> for ClusterParameterDuration {
    type Error = Report<ClusterParameterDurationError>;
    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        let nanos = u64::try_from(value.as_nanos()).map_err(|error| {
            Report::new(error).change_context(ClusterParameterDurationError { value })
        })?;
        Ok(Self(nanos))
    }
}
impl From<ClusterParameterDuration> for Duration {
    fn from(value: ClusterParameterDuration) -> Self {
        value.as_duration()
    }
}
impl ClusterParameterDuration {
    pub fn as_duration(self) -> Duration {
        Duration::from_nanos(self.0)
    }
}

macro_rules! coordination_duration {
    ($definition:ident, $name:literal, $default:expr) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = ClusterParameters;
            type Value = Duration;
            type Context = ();
            type Error = ClusterParameterError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> Duration {
                $default
            }
            fn validate(value: &Duration, _: &()) -> Result<(), Report<ClusterParameterError>> {
                ClusterParameterKey::$definition.validate_duration(*value)
            }
        }
    };
}
coordination_duration!(
    ForcedRecoveryBudget,
    "forced-recovery-budget",
    Duration::from_secs(5)
);
coordination_duration!(
    OwnershipHandoffPollInterval,
    "ownership-handoff-poll-interval",
    Duration::from_millis(250)
);
coordination_duration!(
    EntityGatePollInterval,
    "entity-gate-poll-interval",
    Duration::from_millis(25)
);
coordination_duration!(
    DomainDrainPollInterval,
    "domain-drain-poll-interval",
    Duration::from_millis(50)
);
coordination_duration!(
    RuntimeReadinessPropagationBound,
    "runtime-readiness-propagation-bound",
    Duration::from_secs(30)
);
