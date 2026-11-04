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
