//! Consensus engine budgets at the command-line edge.
//!
//! Layer: edges.
//! - **Owns.** Parsing and rendering consensus capacity and deadline inputs.
//! - **Depends on.** Consensus declarations and edge quantities.
//! - **Must not know.** Raft, replication streams or database ownership.
#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns consensus budget arguments")
)]
use error_stack::Report;
use nervix_consensus::{RaftOperationError, RaftOperationSettings, RaftOperationValues};
use nervix_models::SettingDefinition as _;

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};
#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Consensus capacities")]
pub struct ConsensusOperationArgs {
    #[arg(long = nervix_consensus::ConsensusEventCapacity::NAME, env = "NERVIX_RAFT_EVENT_CAPACITY", help = super::settings::capability_help::<nervix_consensus::ConsensusEventCapacity>("Event capacity", nervix_consensus::ConsensusEventCapacity::default_value(&())))]
    pub event_capacity: Option<usize>,
    #[arg(long = nervix_consensus::AppendBatchEntries::NAME, env = "NERVIX_RAFT_APPEND_BATCH_ENTRIES", help = super::settings::capability_help::<nervix_consensus::AppendBatchEntries>("Append batch entries", nervix_consensus::AppendBatchEntries::default_value(&())))]
    pub append_batch_entries: Option<u64>,
    #[arg(long = nervix_consensus::OutstandingAppendBatches::NAME, env = "NERVIX_RAFT_OUTSTANDING_APPEND_BATCHES", help = super::settings::capability_help::<nervix_consensus::OutstandingAppendBatches>("Outstanding append batches", nervix_consensus::OutstandingAppendBatches::default_value(&())))]
    pub outstanding_append_batches: Option<usize>,
    #[arg(long = nervix_consensus::OutstandingAppendBytes::NAME, env = "NERVIX_RAFT_OUTSTANDING_APPEND_BYTES", help = super::settings::capability_help::<nervix_consensus::OutstandingAppendBytes>("Outstanding append bytes", HumanBytes(nervix_consensus::OutstandingAppendBytes::default_value(&()))))]
    pub outstanding_append_bytes: Option<HumanBytes>,
    #[arg(long = nervix_consensus::AppendIdleBound::NAME, env = "NERVIX_RAFT_APPEND_IDLE_BOUND", help = super::settings::capability_help::<nervix_consensus::AppendIdleBound>("Append idle bound", HumanDuration(nervix_consensus::AppendIdleBound::default_value(&()))))]
    pub append_idle_bound: Option<HumanDuration>,
    #[arg(long = nervix_consensus::SnapshotTransferTimeout::NAME, env = "NERVIX_RAFT_SNAPSHOT_TRANSFER_TIMEOUT", help = super::settings::capability_help::<nervix_consensus::SnapshotTransferTimeout>("Snapshot transfer timeout", HumanDuration(nervix_consensus::SnapshotTransferTimeout::default_value(&()))))]
    pub snapshot_transfer_timeout: Option<HumanDuration>,
    #[arg(long = nervix_consensus::MembershipMutationTimeout::NAME, env = "NERVIX_RAFT_MEMBERSHIP_MUTATION_TIMEOUT", help = super::settings::capability_help::<nervix_consensus::MembershipMutationTimeout>("Membership mutation timeout", HumanDuration(nervix_consensus::MembershipMutationTimeout::default_value(&()))))]
    pub membership_mutation_timeout: Option<HumanDuration>,
    #[arg(long = nervix_consensus::RetentionInterval::NAME, env = "NERVIX_RAFT_RETENTION_INTERVAL", help = super::settings::capability_help::<nervix_consensus::RetentionInterval>("Retention interval", HumanDuration(nervix_consensus::RetentionInterval::default_value(&()))))]
    pub retention_interval: Option<HumanDuration>,
    #[arg(long = nervix_consensus::RetentionAdmissionTimeout::NAME, env = "NERVIX_RAFT_RETENTION_ADMISSION_TIMEOUT", help = super::settings::capability_help::<nervix_consensus::RetentionAdmissionTimeout>("Retention admission timeout", HumanDuration(nervix_consensus::RetentionAdmissionTimeout::default_value(&()))))]
    pub retention_admission_timeout: Option<HumanDuration>,
    #[arg(long = nervix_consensus::PurgeBatchEntries::NAME, env = "NERVIX_RAFT_PURGE_BATCH_ENTRIES", help = super::settings::capability_help::<nervix_consensus::PurgeBatchEntries>("Purge batch entries", nervix_consensus::PurgeBatchEntries::default_value(&())))]
    pub purge_batch_entries: Option<u64>,
    #[arg(long = nervix_consensus::ApiChannelCapacity::NAME, env = "NERVIX_RAFT_API_CHANNEL_CAPACITY", help = super::settings::capability_help::<nervix_consensus::ApiChannelCapacity>("Api channel capacity", nervix_consensus::ApiChannelCapacity::default_value(&())))]
    pub api_channel_capacity: Option<u64>,
    #[arg(long = nervix_consensus::NotificationChannelCapacity::NAME, env = "NERVIX_RAFT_NOTIFICATION_CHANNEL_CAPACITY", help = super::settings::capability_help::<nervix_consensus::NotificationChannelCapacity>("Notification channel capacity", nervix_consensus::NotificationChannelCapacity::default_value(&())))]
    pub notification_channel_capacity: Option<u64>,
    #[arg(long = nervix_consensus::StateMachineChannelCapacity::NAME, env = "NERVIX_RAFT_STATE_MACHINE_CHANNEL_CAPACITY", help = super::settings::capability_help::<nervix_consensus::StateMachineChannelCapacity>("State machine channel capacity", nervix_consensus::StateMachineChannelCapacity::default_value(&())))]
    pub state_machine_channel_capacity: Option<u64>,
    #[arg(long = nervix_consensus::ApiBatchCapacity::NAME, env = "NERVIX_RAFT_API_BATCH_CAPACITY", help = super::settings::capability_help::<nervix_consensus::ApiBatchCapacity>("Api batch capacity", nervix_consensus::ApiBatchCapacity::default_value(&())))]
    pub api_batch_capacity: Option<u64>,
    #[arg(long = nervix_consensus::ReplicationBackoff::NAME, env = "NERVIX_RAFT_REPLICATION_BACKOFF", help = super::settings::capability_help::<nervix_consensus::ReplicationBackoff>("Replication backoff", HumanDuration(nervix_consensus::ReplicationBackoff::default_value(&()))))]
    pub replication_backoff: Option<HumanDuration>,
    #[arg(long = nervix_consensus::ReplicationBackoffMaximum::NAME, env = "NERVIX_RAFT_REPLICATION_BACKOFF_MAXIMUM", help = super::settings::capability_help::<nervix_consensus::ReplicationBackoffMaximum>("Replication backoff maximum", HumanDuration(nervix_consensus::ReplicationBackoffMaximum::default_value(&()))))]
    pub replication_backoff_maximum: Option<HumanDuration>,
    #[arg(long = nervix_consensus::MembershipStepDownDelay::NAME, env = "NERVIX_RAFT_MEMBERSHIP_STEP_DOWN_DELAY", help = super::settings::capability_help::<nervix_consensus::MembershipStepDownDelay>("Membership step down delay", HumanDuration(nervix_consensus::MembershipStepDownDelay::default_value(&()))))]
    pub membership_step_down_delay: Option<HumanDuration>,
}
impl ConsensusOperationArgs {
    pub(super) fn settings(&self) -> Result<RaftOperationSettings, Report<RaftOperationError>> {
        RaftOperationSettings::resolve(RaftOperationValues {
            event_capacity: self.event_capacity,
            append_batch_entries: self.append_batch_entries,
            outstanding_append_batches: self.outstanding_append_batches,
            outstanding_append_bytes: self.outstanding_append_bytes.map(|value| value.0),
            append_idle_bound: self.append_idle_bound.map(|value| value.0),
            snapshot_transfer_timeout: self.snapshot_transfer_timeout.map(|value| value.0),
            membership_mutation_timeout: self.membership_mutation_timeout.map(|value| value.0),
            retention_interval: self.retention_interval.map(|value| value.0),
            retention_admission_timeout: self.retention_admission_timeout.map(|value| value.0),
            purge_batch_entries: self.purge_batch_entries,
            api_channel_capacity: self.api_channel_capacity,
            notification_channel_capacity: self.notification_channel_capacity,
            state_machine_channel_capacity: self.state_machine_channel_capacity,
            api_batch_capacity: self.api_batch_capacity,
            replication_backoff: self.replication_backoff.map(|value| value.0),
            replication_backoff_maximum: self.replication_backoff_maximum.map(|value| value.0),
            membership_step_down_delay: self.membership_step_down_delay.map(|value| value.0),
        })
    }
    pub(super) fn from_settings(settings: &RaftOperationSettings) -> Self {
        Self {
            event_capacity: Some(*settings.event_capacity.value()),
            append_batch_entries: Some(*settings.append_batch_entries.value()),
            outstanding_append_batches: Some(*settings.outstanding_append_batches.value()),
            outstanding_append_bytes: Some(HumanBytes(*settings.outstanding_append_bytes.value())),
            append_idle_bound: Some(HumanDuration(*settings.append_idle_bound.value())),
            snapshot_transfer_timeout: Some(HumanDuration(
                *settings.snapshot_transfer_timeout.value(),
            )),
            membership_mutation_timeout: Some(HumanDuration(
                *settings.membership_mutation_timeout.value(),
            )),
            retention_interval: Some(HumanDuration(*settings.retention_interval.value())),
            retention_admission_timeout: Some(HumanDuration(
                *settings.retention_admission_timeout.value(),
            )),
            purge_batch_entries: Some(*settings.purge_batch_entries.value()),
            api_channel_capacity: Some(*settings.api_channel_capacity.value()),
            notification_channel_capacity: Some(*settings.notification_channel_capacity.value()),
            state_machine_channel_capacity: Some(*settings.state_machine_channel_capacity.value()),
            api_batch_capacity: Some(*settings.api_batch_capacity.value()),
            replication_backoff: Some(HumanDuration(*settings.replication_backoff.value())),
            replication_backoff_maximum: Some(HumanDuration(
                *settings.replication_backoff_maximum.value(),
            )),
            membership_step_down_delay: Some(HumanDuration(
                *settings.membership_step_down_delay.value(),
            )),
        }
    }
    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            nervix_consensus::ConsensusEventCapacity::NAME,
            self.event_capacity,
        );
        command_line.optional(
            nervix_consensus::AppendBatchEntries::NAME,
            self.append_batch_entries,
        );
        command_line.optional(
            nervix_consensus::OutstandingAppendBatches::NAME,
            self.outstanding_append_batches,
        );
        command_line.optional(
            nervix_consensus::OutstandingAppendBytes::NAME,
            self.outstanding_append_bytes,
        );
        command_line.optional(
            nervix_consensus::AppendIdleBound::NAME,
            self.append_idle_bound,
        );
        command_line.optional(
            nervix_consensus::SnapshotTransferTimeout::NAME,
            self.snapshot_transfer_timeout,
        );
        command_line.optional(
            nervix_consensus::MembershipMutationTimeout::NAME,
            self.membership_mutation_timeout,
        );
        command_line.optional(
            nervix_consensus::RetentionInterval::NAME,
            self.retention_interval,
        );
        command_line.optional(
            nervix_consensus::RetentionAdmissionTimeout::NAME,
            self.retention_admission_timeout,
        );
        command_line.optional(
            nervix_consensus::PurgeBatchEntries::NAME,
            self.purge_batch_entries,
        );
        command_line.optional(
            nervix_consensus::ApiChannelCapacity::NAME,
            self.api_channel_capacity,
        );
        command_line.optional(
            nervix_consensus::NotificationChannelCapacity::NAME,
            self.notification_channel_capacity,
        );
        command_line.optional(
            nervix_consensus::StateMachineChannelCapacity::NAME,
            self.state_machine_channel_capacity,
        );
        command_line.optional(
            nervix_consensus::ApiBatchCapacity::NAME,
            self.api_batch_capacity,
        );
        command_line.optional(
            nervix_consensus::ReplicationBackoff::NAME,
            self.replication_backoff,
        );
        command_line.optional(
            nervix_consensus::ReplicationBackoffMaximum::NAME,
            self.replication_backoff_maximum,
        );
        command_line.optional(
            nervix_consensus::MembershipStepDownDelay::NAME,
            self.membership_step_down_delay,
        );
    }
}
