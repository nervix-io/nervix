//! Capacity and deadline declarations of this node's consensus engines.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Append admission, snapshot and membership budgets and explicit OpenRaft sizing.
//! - **Depends on.** Vocabulary setting classes, execution limits and duration/byte quantities.
//! - **Must not know.** Command-line parsing, graph decisions or membership candidates.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns consensus capacity declarations")
)]
use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use thiserror::Error;
use ubyte::ByteUnit;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RaftOperationError {
    #[error("{setting} must be positive and fit its engine representation")]
    Value { setting: &'static str },
    #[error("raft-outstanding-append-bytes ({capacity}) cannot hold a replication batch ({batch})")]
    AppendCapacity { capacity: ByteUnit, batch: ByteUnit },
    #[error("raft-replication-backoff-maximum must be at least raft-replication-backoff")]
    Backoff,
}
/// Validate the exact representation the consensus engine accepts, without rounding a claim.
fn validate_milliseconds(
    setting: &'static str,
    value: Duration,
) -> Result<(), Report<RaftOperationError>> {
    let milliseconds = u64::try_from(value.as_millis())
        .map_err(|_| Report::new(RaftOperationError::Value { setting }))?;
    if milliseconds == 0 || Duration::from_millis(milliseconds) != value {
        return Err(Report::new(RaftOperationError::Value { setting }));
    }
    Ok(())
}

/// The node's `append-batch-entries` budget.
pub struct AppendBatchEntries;
impl SettingDefinition for AppendBatchEntries {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-append-batch-entries";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        256
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `outstanding-append-batches` budget.
pub struct OutstandingAppendBatches;
impl SettingDefinition for OutstandingAppendBatches {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-outstanding-append-batches";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        16
    }
    fn validate(value: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0 || *value > nervix_primitives::sync::Semaphore::MAX_PERMITS {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `outstanding-append-bytes` budget.
pub struct OutstandingAppendBytes;
impl SettingDefinition for OutstandingAppendBytes {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-outstanding-append-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(16)
    }
    fn validate(value: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        if value.as_u64() == 0 || usize::try_from(value.as_u64()).is_err() {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `append-idle-bound` budget.
pub struct AppendIdleBound;
impl SettingDefinition for AppendIdleBound {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-append-idle-bound";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `snapshot-transfer-timeout` budget.
pub struct SnapshotTransferTimeout;
impl SettingDefinition for SnapshotTransferTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-snapshot-transfer-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `membership-mutation-timeout` budget.
pub struct MembershipMutationTimeout;
impl SettingDefinition for MembershipMutationTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-membership-mutation-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(10)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `retention-interval` budget.
pub struct RetentionInterval;
impl SettingDefinition for RetentionInterval {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-retention-interval";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `retention-admission-timeout` budget.
pub struct RetentionAdmissionTimeout;
impl SettingDefinition for RetentionAdmissionTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-retention-admission-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(30)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The retention wait checks frequently enough for its interval and admission budget.
pub struct RetentionAdmissionPoll;
#[derive(Debug, Clone, Copy)]
pub struct RetentionPollContext {
    pub interval: Duration,
    pub admission: Duration,
}
impl SettingDefinition for RetentionAdmissionPoll {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = RetentionPollContext;
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-retention-admission-poll";
    const DERIVATION: Option<&'static str> = Some(
        "min(retention interval / 100, admission timeout / 600), at least the engine's one \
         millisecond tick",
    );
    fn default_value(context: &RetentionPollContext) -> Duration {
        (context.interval / 100)
            .min(context.admission / 600)
            .max(Duration::from_millis(1))
    }
    fn validate(_: &Duration, _: &RetentionPollContext) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

/// The buffered consensus transitions one session may lag behind.
pub struct ConsensusEventCapacity;
impl SettingDefinition for ConsensusEventCapacity {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-event-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        256
    }
    fn validate(value: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0 || *value > nervix_primitives::sync::Semaphore::MAX_PERMITS {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}

/// The node's `purge-batch-entries` budget.
pub struct PurgeBatchEntries;
impl SettingDefinition for PurgeBatchEntries {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-purge-batch-entries";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        1
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `api-channel-capacity` budget.
pub struct ApiChannelCapacity;
impl SettingDefinition for ApiChannelCapacity {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-api-channel-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        65536
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `notification-channel-capacity` budget.
pub struct NotificationChannelCapacity;
impl SettingDefinition for NotificationChannelCapacity {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-notification-channel-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        65536
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `state-machine-channel-capacity` budget.
pub struct StateMachineChannelCapacity;
impl SettingDefinition for StateMachineChannelCapacity {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-state-machine-channel-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        1024
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `api-batch-capacity` budget.
pub struct ApiBatchCapacity;
impl SettingDefinition for ApiBatchCapacity {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-api-batch-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        4096
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<Self::Error>> {
        if *value == 0
            || !matches!(usize::try_from(*value), Ok(value) if value <= nervix_primitives::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Report::new(RaftOperationError::Value {
                setting: Self::NAME,
            }));
        }
        Ok(())
    }
}
/// The node's `replication-backoff` budget.
pub struct ReplicationBackoff;
impl SettingDefinition for ReplicationBackoff {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-replication-backoff";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_millis(200)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `replication-backoff-maximum` budget.
pub struct ReplicationBackoffMaximum;
impl SettingDefinition for ReplicationBackoffMaximum {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-replication-backoff-maximum";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(1)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
/// The node's `membership-step-down-delay` budget.
pub struct MembershipStepDownDelay;
impl SettingDefinition for MembershipStepDownDelay {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = RaftOperationError;
    const NAME: &'static str = "raft-membership-step-down-delay";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_millis(150)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        validate_milliseconds(Self::NAME, *value)
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftOperationValues {
    pub event_capacity: Option<usize>,
    pub append_batch_entries: Option<u64>,
    pub outstanding_append_batches: Option<usize>,
    pub outstanding_append_bytes: Option<ByteUnit>,
    pub append_idle_bound: Option<Duration>,
    pub snapshot_transfer_timeout: Option<Duration>,
    pub membership_mutation_timeout: Option<Duration>,
    pub retention_interval: Option<Duration>,
    pub retention_admission_timeout: Option<Duration>,
    pub purge_batch_entries: Option<u64>,
    pub api_channel_capacity: Option<u64>,
    pub notification_channel_capacity: Option<u64>,
    pub state_machine_channel_capacity: Option<u64>,
    pub api_batch_capacity: Option<u64>,
    pub replication_backoff: Option<Duration>,
    pub replication_backoff_maximum: Option<Duration>,
    pub membership_step_down_delay: Option<Duration>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftOperationSettings {
    pub event_capacity: Setting<ConsensusEventCapacity>,
    pub append_batch_entries: Setting<AppendBatchEntries>,
    pub outstanding_append_batches: Setting<OutstandingAppendBatches>,
    pub outstanding_append_bytes: Setting<OutstandingAppendBytes>,
    pub append_idle_bound: Setting<AppendIdleBound>,
    pub snapshot_transfer_timeout: Setting<SnapshotTransferTimeout>,
    pub membership_mutation_timeout: Setting<MembershipMutationTimeout>,
    pub retention_interval: Setting<RetentionInterval>,
    pub retention_admission_timeout: Setting<RetentionAdmissionTimeout>,
    pub retention_admission_poll: Setting<RetentionAdmissionPoll>,
    pub purge_batch_entries: Setting<PurgeBatchEntries>,
    pub api_channel_capacity: Setting<ApiChannelCapacity>,
    pub notification_channel_capacity: Setting<NotificationChannelCapacity>,
    pub state_machine_channel_capacity: Setting<StateMachineChannelCapacity>,
    pub api_batch_capacity: Setting<ApiBatchCapacity>,
    pub replication_backoff: Setting<ReplicationBackoff>,
    pub replication_backoff_maximum: Setting<ReplicationBackoffMaximum>,
    pub membership_step_down_delay: Setting<MembershipStepDownDelay>,
}
impl RaftOperationSettings {
    pub fn capabilities(
        &self,
        snapshot_entries: u64,
        limits: &nervix_execution::OperationLimits,
    ) -> nervix_models::ConsensusCapabilities {
        nervix_models::ConsensusCapabilities {
            event_capacity: u64::try_from(*self.event_capacity.value())
                .assured("the configured event capacity fits u64"),
            append_batch_entries: *self.append_batch_entries.value(),
            outstanding_append_batches: u64::try_from(*self.outstanding_append_batches.value())
                .assured("supported targets have at most 64-bit capacities"),
            outstanding_append_bytes: self.outstanding_append_bytes.value().as_u64(),
            append_idle_bound: *self.append_idle_bound.value(),
            snapshot_transfer_timeout: *self.snapshot_transfer_timeout.value(),
            membership_mutation_timeout: *self.membership_mutation_timeout.value(),
            retention_interval: *self.retention_interval.value(),
            retention_admission_timeout: *self.retention_admission_timeout.value(),
            retention_admission_poll: *self.retention_admission_poll.value(),
            purge_batch_entries: *self.purge_batch_entries.value(),
            api_channel_capacity: *self.api_channel_capacity.value(),
            notification_channel_capacity: *self.notification_channel_capacity.value(),
            state_machine_channel_capacity: *self.state_machine_channel_capacity.value(),
            api_batch_capacity: *self.api_batch_capacity.value(),
            snapshot_chunk_bytes: limits.bulk_chunk_bytes.as_u64(),
            replication_backoff: *self.replication_backoff.value(),
            replication_backoff_maximum: *self.replication_backoff_maximum.value(),
            membership_step_down_delay: *self.membership_step_down_delay.value(),
            replication_lag_threshold: Self::replication_lag_threshold(snapshot_entries),
        }
    }
    pub fn resolve(values: RaftOperationValues) -> Result<Self, Report<RaftOperationError>> {
        let interval = Setting::<RetentionInterval>::resolve(values.retention_interval, &())?;
        let admission =
            Setting::<RetentionAdmissionTimeout>::resolve(values.retention_admission_timeout, &())?;
        let poll_context = RetentionPollContext {
            interval: *interval.value(),
            admission: *admission.value(),
        };
        let result = Self {
            event_capacity: Setting::resolve(values.event_capacity, &())?,
            append_batch_entries: Setting::resolve(values.append_batch_entries, &())?,
            outstanding_append_batches: Setting::resolve(values.outstanding_append_batches, &())?,
            outstanding_append_bytes: Setting::resolve(values.outstanding_append_bytes, &())?,
            append_idle_bound: Setting::resolve(values.append_idle_bound, &())?,
            snapshot_transfer_timeout: Setting::resolve(values.snapshot_transfer_timeout, &())?,
            membership_mutation_timeout: Setting::resolve(values.membership_mutation_timeout, &())?,
            retention_interval: interval,
            retention_admission_timeout: admission,
            retention_admission_poll: Setting::resolve(None, &poll_context)?,
            purge_batch_entries: Setting::resolve(values.purge_batch_entries, &())?,
            api_channel_capacity: Setting::resolve(values.api_channel_capacity, &())?,
            notification_channel_capacity: Setting::resolve(
                values.notification_channel_capacity,
                &(),
            )?,
            state_machine_channel_capacity: Setting::resolve(
                values.state_machine_channel_capacity,
                &(),
            )?,
            api_batch_capacity: Setting::resolve(values.api_batch_capacity, &())?,
            replication_backoff: Setting::resolve(values.replication_backoff, &())?,
            replication_backoff_maximum: Setting::resolve(values.replication_backoff_maximum, &())?,
            membership_step_down_delay: Setting::resolve(values.membership_step_down_delay, &())?,
        };
        if result.replication_backoff_maximum.value() < result.replication_backoff.value() {
            return Err(Report::new(RaftOperationError::Backoff));
        }
        Ok(result)
    }
    pub fn validate_execution(
        &self,
        limits: &nervix_execution::OperationLimits,
    ) -> Result<(), Report<RaftOperationError>> {
        let capacity = *self.outstanding_append_bytes.value();
        let batch = limits.replication_batch_bytes;
        if capacity < batch {
            return Err(Report::new(RaftOperationError::AppendCapacity {
                capacity,
                batch,
            }));
        }
        Ok(())
    }
    /// A follower beyond two snapshot intervals receives the snapshot, never one before it exists.
    pub fn replication_lag_threshold(snapshot_entries: u64) -> u64 {
        nervix_primitives::expect_lint!(
            nervix::clamped_arithmetic,
            "OpenRaft's u64 lag limit caps the doubled threshold; validation rejects a snapshot \
             threshold of MAX so this limit remains strictly greater",
            snapshot_entries.saturating_mul(2)
        )
    }
    pub fn backoff(&self) -> String {
        format!(
            "{}ms ...{}ms",
            self.replication_backoff.value().as_millis(),
            self.replication_backoff_maximum.value().as_millis()
        )
    }
}
impl Default for RaftOperationSettings {
    fn default() -> Self {
        Self::resolve(RaftOperationValues::default())
            .assured("declared consensus defaults fit their representations")
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;
    #[test]
    fn explicit_replication_pacing_preserves_the_engine_default_series() {
        let settings = RaftOperationSettings::default();
        let configured = openraft::Config {
            backoff: settings.backoff(),
            ..Default::default()
        }
        .validate()
        .assured("the declared backoff is valid");
        let reference = openraft::Config::default()
            .validate()
            .assured("the maintained engine defaults are valid");
        assert_eq!(
            configured.build_backoff().take(6).collect::<Vec<_>>(),
            reference.build_backoff().take(6).collect::<Vec<_>>()
        );
    }

    #[test]
    fn engine_durations_preserve_their_exact_millisecond_claim() {
        for value in [Duration::ZERO, Duration::from_micros(1500), Duration::MAX] {
            assert!(
                RaftOperationSettings::resolve(RaftOperationValues {
                    snapshot_transfer_timeout: Some(value),
                    ..Default::default()
                })
                .is_err()
            );
        }
        let timeout = Duration::from_millis(1500);
        let settings = RaftOperationSettings::resolve(RaftOperationValues {
            snapshot_transfer_timeout: Some(timeout),
            ..Default::default()
        })
        .assured("an exact positive engine millisecond value");
        assert_eq!(*settings.snapshot_transfer_timeout.value(), timeout);
    }

    #[test]
    fn retention_poll_and_snapshot_lag_follow_their_owners() {
        let settings = RaftOperationSettings::resolve(RaftOperationValues {
            retention_interval: Some(Duration::from_secs(2)),
            retention_admission_timeout: Some(Duration::from_secs(6)),
            ..Default::default()
        })
        .assured("positive budgets fit milliseconds");
        assert_eq!(
            *settings.retention_admission_poll.value(),
            Duration::from_millis(10)
        );
        assert_eq!(
            RaftOperationSettings::replication_lag_threshold(10_000),
            20_000
        );
        assert_eq!(
            RaftOperationSettings::replication_lag_threshold(u64::MAX - 1),
            u64::MAX
        );
        assert!(
            crate::RaftRetentionSettings::resolve(crate::RaftRetentionBounds {
                snapshot_entry_threshold: Some(u64::MAX),
                ..Default::default()
            })
            .is_err()
        );
    }
    #[test]
    fn an_append_window_cannot_wait_for_capacity_it_can_never_have() {
        let settings = RaftOperationSettings::resolve(RaftOperationValues {
            outstanding_append_bytes: Some(ByteUnit::Byte(1)),
            ..Default::default()
        })
        .assured("one byte is representable");
        assert!(matches!(
            settings
                .validate_execution(&nervix_execution::OperationLimits::default())
                .err()
                .assured("a replication batch cannot fit")
                .current_context(),
            RaftOperationError::AppendCapacity { .. }
        ));
        assert!(
            RaftOperationSettings::resolve(RaftOperationValues {
                outstanding_append_batches: Some(0),
                ..Default::default()
            })
            .is_err()
        );
    }
}
