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
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{ClusterParameterError, ClusterParameterKey};
use crate::{ClusterParameters, SettingDefinition};

/// The exact nanoseconds of a bounded cluster duration. All admitted durations are at most
/// u32::MAX seconds and therefore fit u64 nanoseconds without loss. The compact value keeps the
/// complete policy and the Raft commands that carry it inline, with no allocation.
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
    pub(super) fn resolve(
        parameter: ClusterParameterKey,
        value: Duration,
    ) -> Result<Self, Report<ClusterParameterError>> {
        parameter.validate_duration(value)?;
        Ok(Self::try_from(value).assured("at most u32::MAX seconds fit u64 nanoseconds exactly"))
    }
}

macro_rules! duration_parameter {
    ($definition:ident, $name:literal, $default:expr) => {
        pub struct $definition;
        impl $definition {
            /// Resolves a semantic duration once into this definition's exact compact value.
            pub fn resolve_duration(
                value: Option<Duration>,
            ) -> Result<crate::Setting<Self>, Report<ClusterParameterError>> {
                let value = value
                    .map(|value| {
                        ClusterParameterDuration::resolve(ClusterParameterKey::$definition, value)
                    })
                    .transpose()?;
                crate::Setting::resolve(value, &())
            }
        }
        impl SettingDefinition for $definition {
            type Class = ClusterParameters;
            type Value = ClusterParameterDuration;
            type Context = ();
            type Error = ClusterParameterError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> Self::Value {
                ClusterParameterDuration::resolve(ClusterParameterKey::$definition, $default)
                    .assured(
                        "the duration definition's default is positive and within its declared \
                         bound",
                    )
            }
            fn validate(value: &Self::Value, _: &()) -> Result<(), Report<Self::Error>> {
                ClusterParameterKey::$definition.validate_duration(value.as_duration())
            }
        }
    };
}
duration_parameter!(
    ForcedRecoveryBudget,
    "forced-recovery-budget",
    Duration::from_secs(5)
);
duration_parameter!(
    OwnershipHandoffPollInterval,
    "ownership-handoff-poll-interval",
    Duration::from_millis(250)
);
duration_parameter!(
    EntityGatePollInterval,
    "entity-gate-poll-interval",
    Duration::from_millis(25)
);
duration_parameter!(
    DomainDrainPollInterval,
    "domain-drain-poll-interval",
    Duration::from_millis(50)
);
duration_parameter!(
    RuntimeReadinessPropagationBound,
    "runtime-readiness-propagation-bound",
    Duration::from_secs(30)
);
duration_parameter!(
    NodeUnavailabilityTimeout,
    "node-unavailability-timeout",
    Duration::from_secs(10)
);
duration_parameter!(DomainDrainTimeout, "domain-drain-timeout", Self::DEFAULT);
duration_parameter!(
    EntityGateDeadline,
    "entity-gate-deadline",
    Duration::from_secs(60)
);
duration_parameter!(
    TransactionIdleTimeout,
    "transaction-idle-timeout",
    Duration::from_secs(900)
);
duration_parameter!(
    TransactionTombstoneRetention,
    "transaction-tombstone-retention",
    Duration::from_secs(900)
);
duration_parameter!(
    CommandRetryValidity,
    "command-retry-validity",
    Duration::from_secs(900)
);
duration_parameter!(
    StateSnapshotInterval,
    "state-snapshot-interval",
    Self::DEFAULT
);

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
