//! Class-typed operational capacity of one concrete node run.
//!
//! Layer: vocabulary.
//! - **Owns.** Immutable control-plane, session and client-link capability declarations and claims.
//! - **Depends on.** Setting classes, scalar values and validated serialization.
//! - **Must not know.** Machines, command lines, consensus, transports or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        vocabulary,
        reason = "owns immutable operational capability vocabulary"
    )
)]

use std::{num::NonZeroU64, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{NodeCapabilities, Setting, SettingDefinition, settings::archive};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NodeOperationalError {
    #[error("{setting} must be positive and at most {maximum:?}, found {value:?}")]
    Duration {
        setting: &'static str,
        value: Duration,
        maximum: Duration,
    },
    #[error("{setting} must be at most {maximum}, found {value}")]
    Count {
        setting: &'static str,
        value: u64,
        maximum: u64,
    },
    #[error("{setting} must be greater than zero")]
    NonPositive { setting: &'static str },
    #[error("invalid session bounds")]
    SessionBounds,
    #[error("session-queue-memory-bytes must hold a maximum-sized frame on each lane")]
    QueueMemory,
    #[error("the derived session queue capacity exceeds the supported channel capacity")]
    QueueCapacity,
    #[error("session-subscription-rows-per-frame must not exceed session-collection-entries")]
    SubscriptionRows,
}

fn validate_duration(
    name: &'static str,
    value: Duration,
) -> Result<(), Report<NodeOperationalError>> {
    let maximum = Duration::from_secs(u64::from(u32::MAX));
    if value.is_zero() || value > maximum {
        return Err(Report::new(NodeOperationalError::Duration {
            setting: name,
            value,
            maximum,
        }));
    }
    Ok(())
}

/// Counts fit the channel and semaphore representations on every supported target.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "portable bounded channels reserve three high usize bits for their state"
    )
)]
const MAX_CHANNEL_CAPACITY: u64 = 0x1fff_ffff;

fn validate_count(
    name: &'static str,
    value: NonZeroU64,
) -> Result<(), Report<NodeOperationalError>> {
    if value.get() > MAX_CHANNEL_CAPACITY {
        return Err(Report::new(NodeOperationalError::Count {
            setting: name,
            value: value.get(),
            maximum: MAX_CHANNEL_CAPACITY,
        }));
    }
    Ok(())
}

macro_rules! capability_value_type {
    (Duration) => {
        Duration
    };
    (Count) => {
        NonZeroU64
    };
}

macro_rules! capability_text {
    ($value:expr, Duration) => {
        crate::timing_text(*$value)
    };
    ($value:expr, Count) => {
        $value.to_string()
    };
}

macro_rules! capability_definition {
    ($definition:ident, $name:literal, Duration, $default:expr) => {
        pub struct $definition;
        impl $definition {
            #[cfg_attr(
                nervix_lint,
                nervix::constant(
                    fixed,
                    reason = "this setting definition owns its single compile-time default"
                )
            )]
            pub const DEFAULT: Duration = $default;
        }
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = Duration;
            type Context = ();
            type Error = NodeOperationalError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> Duration {
                Self::DEFAULT
            }
            fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
                validate_duration(Self::NAME, *value)
            }
        }
    };
    ($definition:ident, $name:literal, Count, $default:expr) => {
        pub struct $definition;
        impl $definition {
            #[cfg_attr(
                nervix_lint,
                nervix::constant(
                    fixed,
                    reason = "this setting definition owns its single compile-time default"
                )
            )]
            pub const DEFAULT: u64 = $default;
        }
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroU64;
            type Context = ();
            type Error = NodeOperationalError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> NonZeroU64 {
                NonZeroU64::new(Self::DEFAULT).assured("the definition's default count is positive")
            }
            fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
                validate_count(Self::NAME, *value)
            }
        }
    };
}

macro_rules! capability_group {
    ($group:ident, $values:ident { $($field:ident: $definition:ident => ($name:literal, $kind:ident, $default:expr),)+ }) => {
        $(capability_definition!($definition, $name, $kind, $default);)+

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $group {
            $(#[serde(with = "archive")] #[rkyv(with = archive::ValidatedSetting)] $field: Setting<$definition>,)+
        }

        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub struct $values { $(pub $field: Option<capability_value_type!($kind)>,)+ }

        impl $group {
            pub fn resolve(values: $values) -> Result<Self, Report<NodeOperationalError>> {
                Ok(Self { $($field: Setting::resolve(values.$field, &())?,)+ })
            }
            $(pub fn $field(&self) -> capability_value_type!($kind) { *self.$field.value() })+
            pub fn values(&self) -> $values {
                $values { $($field: Some(self.$field()),)+ }
            }
            pub fn described(&self) -> Vec<(&'static str, String)> {
                vec![$(($definition::NAME, capability_text!(self.$field.value(), $kind)),)+]
            }
        }
        impl Default for $group {
            fn default() -> Self {
                Self::resolve($values::default()).assured("each capability definition declares a valid default")
            }
        }
    };
}

capability_group! {
    ControlPlaneCapabilities, ControlPlaneCapabilityValues {
        membership_interval: MembershipReconcileInterval => ("control-plane-membership-interval", Duration, Duration::from_secs(1)),
        scheduling_interval: SchedulingReconcileInterval => ("control-plane-scheduling-interval", Duration, Duration::from_secs(1)),
        resource_replication_interval: ResourceReconcileInterval => ("control-plane-resource-replication-interval", Duration, Duration::from_secs(1)),
        health_probe_interval: HealthProbeInterval => ("control-plane-health-probe-interval", Duration, Duration::from_secs(1)),
        leadership_sync_retry: LeadershipSyncRetry => ("control-plane-leadership-sync-retry", Duration, Duration::from_millis(250)),
        transaction_sweep_interval: TransactionSweepInterval => ("control-plane-transaction-sweep-interval", Duration, Duration::from_millis(250)),
        restore_staging_sweep_interval: RestoreStagingSweepInterval => ("control-plane-restore-staging-sweep-interval", Duration, Duration::from_secs(1)),
        transaction_recovery_concurrency: TransactionRecoveryConcurrency => ("control-plane-transaction-recovery-concurrency", Count, 8),
        resource_replication_concurrency: ResourceReplicationConcurrency => ("control-plane-resource-replication-concurrency", Count, 4),
        entity_gate_release_retry_interval: EntityGateReleaseRetryInterval => ("control-plane-entity-gate-release-retry-interval", Duration, Duration::from_millis(100)),
        entity_gate_engagement_retry_interval: EntityGateEngagementRetryInterval => ("control-plane-entity-gate-engagement-retry-interval", Duration, Duration::from_millis(100)),
        transaction_application_retry_interval: TransactionApplicationRetryInterval => ("control-plane-transaction-application-retry-interval", Duration, Duration::from_millis(100)),
        domain_clock_progress_retry_interval: DomainClockProgressRetryInterval => ("control-plane-domain-clock-progress-retry-interval", Duration, Duration::from_millis(200)),
        backup_retry_interval: BackupRetryInterval => ("control-plane-backup-retry-interval", Duration, Duration::from_millis(50)),
        restore_admission_wait: RestoreAdmissionWait => ("control-plane-restore-admission-wait", Duration, Duration::from_secs(30)),
        restore_admission_retry_interval: RestoreAdmissionRetryInterval => ("control-plane-restore-admission-retry-interval", Duration, Duration::from_millis(50)),
        model_inspection_timeout: ModelInspectionTimeout => ("control-plane-model-inspection-timeout", Duration, Duration::from_secs(30)),
        kafka_partition_watch_interval: LeaderKafkaPartitionWatchInterval => ("control-plane-kafka-partition-watch-interval", Duration, Duration::from_secs(1)),
        tls_reload_interval: InterconnectTlsReloadInterval => ("interconnect-tls-reload-interval", Duration, Duration::from_secs(1)),
        authentication_attempts_per_second: AuthenticationAttemptsPerSecond => ("authentication-attempts-per-second", Count, 10),
    }
}

capability_group! {
    ClientLinkCapabilities, ClientLinkCapabilityValues {
        heartbeat_interval: ClientLinkHeartbeatInterval => ("client-link-heartbeat-interval", Duration, Duration::from_secs(2)),
        producer_open_timeout: ClientProducerLinkOpenTimeout => ("client-link-producer-open-timeout", Duration, Duration::from_secs(20)),
        consumer_open_timeout: ClientConsumerLinkOpenTimeout => ("client-link-consumer-open-timeout", Duration, Duration::from_secs(20)),
        producer_answer_capacity: ClientProducerLinkAnswerCapacity => ("client-link-producer-answer-capacity", Count, 64),
        consumer_answer_capacity: ClientConsumerLinkAnswerCapacity => ("client-link-consumer-answer-capacity", Count, 64),
        consumer_writer_capacity: ClientConsumerLinkWriterCapacity => ("client-link-consumer-writer-capacity", Count, 64),
    }
}

/// The sender's validated cadence, announced once when a link opens.
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
pub struct AnnouncedLinkHeartbeat(
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    Setting<ClientLinkHeartbeatInterval>,
);

impl AnnouncedLinkHeartbeat {
    pub fn new(interval: Duration) -> Result<Self, Report<NodeOperationalError>> {
        Setting::new(interval, &()).map(Self)
    }

    pub fn interval(self) -> Duration {
        *self.0.value()
    }

    pub fn silence_limit(self) -> Duration {
        self.interval()
            .checked_mul(5)
            .assured("the supported cadence bound fits five peer heartbeat intervals")
    }
}

impl ClientLinkCapabilities {
    pub fn announced_heartbeat(&self) -> AnnouncedLinkHeartbeat {
        AnnouncedLinkHeartbeat(self.heartbeat_interval)
    }
}

mod session;
pub use session::*;

/// Required operational values of one incarnation; no stored field has a default.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct NodeOperationalCapabilities {
    pub control_plane: ControlPlaneCapabilities,
    pub session: SessionCapabilities,
    pub links: ClientLinkCapabilities,
}

impl NodeOperationalCapabilities {
    pub fn described(&self) -> Vec<(&'static str, String)> {
        let mut values = self.control_plane.described();
        values.extend(self.session.described());
        values.extend(self.links.described());
        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_the_session_queue_and_download_capacities() {
        let capabilities = NodeOperationalCapabilities::default();
        assert_eq!(capabilities.session.queue_capacity(), 16);
        assert_eq!(capabilities.session.download_capacity(), 4);
        assert_eq!(
            capabilities.links.announced_heartbeat().silence_limit(),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn dependent_session_defaults_follow_the_validated_inputs() {
        let capabilities = SessionCapabilities::resolve(SessionCapabilityValues {
            bounds: SessionBoundValues {
                frame_bytes: NonZeroU64::new(1 << 20),
                transfer_bytes: NonZeroU64::new(100 << 20),
                collection_entries: NonZeroU64::new(8),
                ..Default::default()
            },
            ..Default::default()
        })
        .expect("the inputs satisfy each relationship");
        assert_eq!(capabilities.bounds().string_bytes().get(), 100 << 20);
        assert_eq!(capabilities.subscription_rows_per_frame().get(), 8);
        assert_eq!(capabilities.queue_capacity(), 64);
        assert_eq!(capabilities.download_capacity(), 16);
    }

    #[test]
    fn session_memory_and_rows_are_checked_before_becoming_capabilities() {
        let error = SessionCapabilities::resolve(SessionCapabilityValues {
            queue_memory_bytes: NonZeroU64::new(1),
            ..Default::default()
        })
        .expect_err("each lane must hold a frame");
        assert_eq!(error.current_context(), &NodeOperationalError::QueueMemory);
        let error = SessionCapabilities::resolve(SessionCapabilityValues {
            bounds: SessionBoundValues {
                collection_entries: NonZeroU64::new(8),
                ..Default::default()
            },
            subscription_rows_per_frame: NonZeroU64::new(9),
            ..Default::default()
        })
        .expect_err("a frame's rows stay within its entry bound");
        assert_eq!(
            error.current_context(),
            &NodeOperationalError::SubscriptionRows
        );
        assert!(AnnouncedLinkHeartbeat::new(Duration::ZERO).is_err());
    }
}
