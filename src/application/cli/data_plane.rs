//! Command-line projection of local data-plane settings.
//!
//! Layer: edges.
//! - **Owns.** Human-readable local runtime overrides and their typed conversion.
//! - **Depends on.** Vocabulary setting definitions and CLI scalar forms.
//! - **Must not know.** Runtime task construction or cluster parameter installation.

use error_stack::Report;
use nervix_models::{
    DataPlaneSettingOverrides, DataPlaneSettings, DataPlaneSettingsError, SettingDefinition as _,
};

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Data-plane runtime capabilities")]
pub struct DataPlaneArgs {
    #[arg(id = nervix_models::RuntimeDeliveryCapacity::NAME, long = nervix_models::RuntimeDeliveryCapacity::NAME, env = "NERVIX_RUNTIME_DELIVERY_CAPACITY", help = super::settings::capability_help::<nervix_models::RuntimeDeliveryCapacity>("Maximum concurrent remote deliveries in each correlation lane", <nervix_models::RuntimeDeliveryCapacity as nervix_models::SettingDefinition>::default_value(&())))]
    pub delivery_capacity: Option<u64>,
    #[arg(id = nervix_models::RuntimeIngestGroupMaxRows::NAME, long = nervix_models::RuntimeIngestGroupMaxRows::NAME, env = "NERVIX_RUNTIME_INGEST_GROUP_MAX_ROWS", help = super::settings::capability_help::<nervix_models::RuntimeIngestGroupMaxRows>("Decoded rows that close an ingest group", <nervix_models::RuntimeIngestGroupMaxRows as nervix_models::SettingDefinition>::default_value(&())))]
    pub ingest_group_max_rows: Option<u64>,
    #[arg(id = nervix_models::RuntimeIngestGroupIdleFlush::NAME, long = nervix_models::RuntimeIngestGroupIdleFlush::NAME, env = "NERVIX_RUNTIME_INGEST_GROUP_IDLE_FLUSH", help = super::settings::capability_help::<nervix_models::RuntimeIngestGroupIdleFlush>("Idle deadline of a partial ingest group", HumanDuration(<nervix_models::RuntimeIngestGroupIdleFlush as nervix_models::SettingDefinition>::default_value(&()))))]
    pub ingest_group_idle_flush: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeUnacknowledgedPollBatch::NAME, long = nervix_models::RuntimeUnacknowledgedPollBatch::NAME, env = "NERVIX_RUNTIME_UNACKNOWLEDGED_POLL_BATCH", help = super::settings::capability_help::<nervix_models::RuntimeUnacknowledgedPollBatch>("Messages requested per unacknowledged broker poll", <nervix_models::RuntimeUnacknowledgedPollBatch as nervix_models::SettingDefinition>::default_value(&())))]
    pub unacknowledged_poll_batch: Option<u64>,
    #[arg(id = nervix_models::RuntimeIngestorErrorRetry::NAME, long = nervix_models::RuntimeIngestorErrorRetry::NAME, env = "NERVIX_RUNTIME_INGESTOR_ERROR_RETRY", help = super::settings::capability_help::<nervix_models::RuntimeIngestorErrorRetry>("Delay after a transient ingestor error", HumanDuration(<nervix_models::RuntimeIngestorErrorRetry as nervix_models::SettingDefinition>::default_value(&()))))]
    pub ingestor_error_retry: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeReconnectInitial::NAME, long = nervix_models::RuntimeReconnectInitial::NAME, env = "NERVIX_RUNTIME_RECONNECT_INITIAL", help = super::settings::capability_help::<nervix_models::RuntimeReconnectInitial>("Initial reconnect delay for host-owned retry", HumanDuration(<nervix_models::RuntimeReconnectInitial as nervix_models::SettingDefinition>::default_value(&()))))]
    pub reconnect_initial: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeReconnectMax::NAME, long = nervix_models::RuntimeReconnectMax::NAME, env = "NERVIX_RUNTIME_RECONNECT_MAX", help = super::settings::capability_help::<nervix_models::RuntimeReconnectMax>("Maximum reconnect delay for host-owned retry", HumanDuration(<nervix_models::RuntimeReconnectMax as nervix_models::SettingDefinition>::default_value(&()))))]
    pub reconnect_max: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeBranchExpirationScan::NAME, long = nervix_models::RuntimeBranchExpirationScan::NAME, env = "NERVIX_RUNTIME_BRANCH_EXPIRATION_SCAN", help = super::settings::capability_help::<nervix_models::RuntimeBranchExpirationScan>("Cadence of branch lifetime expiration scans", HumanDuration(<nervix_models::RuntimeBranchExpirationScan as nervix_models::SettingDefinition>::default_value(&()))))]
    pub branch_expiration_scan: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeReplicationPoll::NAME, long = nervix_models::RuntimeReplicationPoll::NAME, env = "NERVIX_RUNTIME_REPLICATION_POLL", help = super::settings::capability_help::<nervix_models::RuntimeReplicationPoll>("Cadence of state replication polls", HumanDuration(<nervix_models::RuntimeReplicationPoll as nervix_models::SettingDefinition>::default_value(&()))))]
    pub replication_poll: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeCatchUpConcurrency::NAME, long = nervix_models::RuntimeCatchUpConcurrency::NAME, env = "NERVIX_RUNTIME_CATCH_UP_CONCURRENCY", help = super::settings::capability_help::<nervix_models::RuntimeCatchUpConcurrency>("Checkpoint fetches admitted in one catch-up round", <nervix_models::RuntimeCatchUpConcurrency as nervix_models::SettingDefinition>::default_value(&())))]
    pub catch_up_concurrency: Option<u64>,
    #[arg(id = nervix_models::RuntimeCatchUpPages::NAME, long = nervix_models::RuntimeCatchUpPages::NAME, env = "NERVIX_RUNTIME_CATCH_UP_PAGES", help = super::settings::capability_help::<nervix_models::RuntimeCatchUpPages>("Catalog pages visited in one catch-up round", <nervix_models::RuntimeCatchUpPages as nervix_models::SettingDefinition>::default_value(&())))]
    pub catch_up_pages: Option<u64>,
    #[arg(id = nervix_models::RuntimeRetainedRemovals::NAME, long = nervix_models::RuntimeRetainedRemovals::NAME, env = "NERVIX_RUNTIME_RETAINED_REMOVALS", help = super::settings::capability_help::<nervix_models::RuntimeRetainedRemovals>("Catalog removals retained for incremental replication", <nervix_models::RuntimeRetainedRemovals as nervix_models::SettingDefinition>::default_value(&())))]
    pub retained_removals: Option<u64>,
    #[arg(id = nervix_models::RuntimeSnapshotStagingBytes::NAME, long = nervix_models::RuntimeSnapshotStagingBytes::NAME, env = "NERVIX_RUNTIME_SNAPSHOT_STAGING_BYTES", help = super::settings::capability_help::<nervix_models::RuntimeSnapshotStagingBytes>("Concurrent temporary snapshot staging quota", HumanBytes(ubyte::ByteUnit::Byte(<nervix_models::RuntimeSnapshotStagingBytes as nervix_models::SettingDefinition>::default_value(&())))))]
    pub snapshot_staging_bytes: Option<HumanBytes>,
    #[arg(id = nervix_models::RuntimeSnapshotBytes::NAME, long = nervix_models::RuntimeSnapshotBytes::NAME, env = "NERVIX_RUNTIME_SNAPSHOT_BYTES", help = super::settings::capability_help::<nervix_models::RuntimeSnapshotBytes>("Largest snapshot staged by one transfer", HumanBytes(ubyte::ByteUnit::Byte(<nervix_models::RuntimeSnapshotBytes as nervix_models::SettingDefinition>::default_value(&())))))]
    pub snapshot_bytes: Option<HumanBytes>,
    #[arg(id = nervix_models::RuntimeSnapshotDescribeTimeout::NAME, long = nervix_models::RuntimeSnapshotDescribeTimeout::NAME, env = "NERVIX_RUNTIME_SNAPSHOT_DESCRIBE_TIMEOUT", help = super::settings::capability_help::<nervix_models::RuntimeSnapshotDescribeTimeout>("Deadline of describing a sealed state snapshot", HumanDuration(<nervix_models::RuntimeSnapshotDescribeTimeout as nervix_models::SettingDefinition>::default_value(&()))))]
    pub snapshot_describe_timeout: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeSnapshotFetchTimeout::NAME, long = nervix_models::RuntimeSnapshotFetchTimeout::NAME, env = "NERVIX_RUNTIME_SNAPSHOT_FETCH_TIMEOUT", help = super::settings::capability_help::<nervix_models::RuntimeSnapshotFetchTimeout>("Deadline of fetching a sealed state snapshot", HumanDuration(<nervix_models::RuntimeSnapshotFetchTimeout as nervix_models::SettingDefinition>::default_value(&()))))]
    pub snapshot_fetch_timeout: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeWasmCheckpointTimeout::NAME, long = nervix_models::RuntimeWasmCheckpointTimeout::NAME, env = "NERVIX_RUNTIME_WASM_CHECKPOINT_TIMEOUT", help = super::settings::capability_help::<nervix_models::RuntimeWasmCheckpointTimeout>("Deadline until every guest checkpoint replica confirms", HumanDuration(<nervix_models::RuntimeWasmCheckpointTimeout as nervix_models::SettingDefinition>::default_value(&()))))]
    pub wasm_checkpoint_timeout: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeEventCapacity::NAME, long = nervix_models::RuntimeEventCapacity::NAME, env = "NERVIX_RUNTIME_EVENT_CAPACITY", help = super::settings::capability_help::<nervix_models::RuntimeEventCapacity>("Runtime events retained for a lagging observer", <nervix_models::RuntimeEventCapacity as nervix_models::SettingDefinition>::default_value(&())))]
    pub event_capacity: Option<u64>,
    #[arg(id = nervix_models::RuntimeWasmRecoveryCapacity::NAME, long = nervix_models::RuntimeWasmRecoveryCapacity::NAME, env = "NERVIX_RUNTIME_WASM_RECOVERY_CAPACITY", help = super::settings::capability_help::<nervix_models::RuntimeWasmRecoveryCapacity>("Guest-state recovery requests awaiting a decision", <nervix_models::RuntimeWasmRecoveryCapacity as nervix_models::SettingDefinition>::default_value(&())))]
    pub wasm_recovery_capacity: Option<u64>,
    #[arg(id = nervix_models::RuntimeImmediateFlushMinimum::NAME, long = nervix_models::RuntimeImmediateFlushMinimum::NAME, env = "NERVIX_RUNTIME_IMMEDIATE_FLUSH_MINIMUM", help = super::settings::capability_help::<nervix_models::RuntimeImmediateFlushMinimum>("Minimum timer interval for immediate branch flushes", HumanDuration(<nervix_models::RuntimeImmediateFlushMinimum as nervix_models::SettingDefinition>::default_value(&()))))]
    pub immediate_flush_minimum: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeCheckpointBytes::NAME, long = nervix_models::RuntimeCheckpointBytes::NAME, env = "NERVIX_RUNTIME_CHECKPOINT_BYTES", help = super::settings::capability_help::<nervix_models::RuntimeCheckpointBytes>("Largest state checkpoint accepted by an in-memory transfer", HumanBytes(ubyte::ByteUnit::Byte(<nervix_models::RuntimeCheckpointBytes as nervix_models::SettingDefinition>::default_value(&())))))]
    pub checkpoint_bytes: Option<HumanBytes>,
    #[arg(id = nervix_models::RuntimeWasmEpochTickInterval::NAME, long = nervix_models::RuntimeWasmEpochTickInterval::NAME, env = "NERVIX_RUNTIME_WASM_EPOCH_TICK_INTERVAL", help = super::settings::capability_help::<nervix_models::RuntimeWasmEpochTickInterval>("WASM epoch preemption quantum", HumanDuration(<nervix_models::RuntimeWasmEpochTickInterval as nervix_models::SettingDefinition>::default_value(&()))))]
    pub wasm_epoch_tick_interval: Option<HumanDuration>,
    #[arg(id = nervix_models::RuntimeWasmEpochDeadlineTicks::NAME, long = nervix_models::RuntimeWasmEpochDeadlineTicks::NAME, env = "NERVIX_RUNTIME_WASM_EPOCH_DEADLINE_TICKS", help = super::settings::capability_help::<nervix_models::RuntimeWasmEpochDeadlineTicks>("WASM epoch ticks before an asynchronous yield", <nervix_models::RuntimeWasmEpochDeadlineTicks as nervix_models::SettingDefinition>::default_value(&())))]
    pub wasm_epoch_deadline_ticks: Option<u64>,
    #[arg(id = nervix_models::RuntimeWasmGuestBufferBytes::NAME, long = nervix_models::RuntimeWasmGuestBufferBytes::NAME, env = "NERVIX_RUNTIME_WASM_GUEST_BUFFER_BYTES", help = super::settings::capability_help::<nervix_models::RuntimeWasmGuestBufferBytes>("Maximum guest invocation buffer", HumanBytes(ubyte::ByteUnit::Byte(<nervix_models::RuntimeWasmGuestBufferBytes as nervix_models::SettingDefinition>::default_value(&())))))]
    pub wasm_guest_buffer_bytes: Option<HumanBytes>,
    #[arg(id = nervix_models::RuntimeWasmOptimize::NAME, long = nervix_models::RuntimeWasmOptimize::NAME, env = "NERVIX_RUNTIME_WASM_OPTIMIZE", help = super::settings::capability_help::<nervix_models::RuntimeWasmOptimize>("Enable WASM compilation optimisation", <nervix_models::RuntimeWasmOptimize as nervix_models::SettingDefinition>::default_value(&())), action = clap::ArgAction::Set)]
    pub wasm_optimize: Option<bool>,
}
impl DataPlaneArgs {
    pub(super) fn settings(&self) -> Result<DataPlaneSettings, Report<DataPlaneSettingsError>> {
        DataPlaneSettings::resolve(DataPlaneSettingOverrides {
            wasm_epoch_tick_interval: self.wasm_epoch_tick_interval.map(|value| value.0),
            wasm_epoch_deadline_ticks: self.wasm_epoch_deadline_ticks,
            wasm_guest_buffer_bytes: self.wasm_guest_buffer_bytes.map(|value| value.0.as_u64()),
            wasm_optimize: self.wasm_optimize,

            delivery_capacity: self.delivery_capacity,
            ingest_group_max_rows: self.ingest_group_max_rows,
            ingest_group_idle_flush: self.ingest_group_idle_flush.map(|value| value.0),
            unacknowledged_poll_batch: self.unacknowledged_poll_batch,
            ingestor_error_retry: self.ingestor_error_retry.map(|value| value.0),
            reconnect_initial: self.reconnect_initial.map(|value| value.0),
            reconnect_max: self.reconnect_max.map(|value| value.0),
            branch_expiration_scan: self.branch_expiration_scan.map(|value| value.0),
            replication_poll: self.replication_poll.map(|value| value.0),
            catch_up_concurrency: self.catch_up_concurrency,
            catch_up_pages: self.catch_up_pages,
            retained_removals: self.retained_removals,
            snapshot_staging_bytes: self.snapshot_staging_bytes.map(|value| value.0.as_u64()),
            snapshot_bytes: self.snapshot_bytes.map(|value| value.0.as_u64()),
            snapshot_describe_timeout: self.snapshot_describe_timeout.map(|value| value.0),
            snapshot_fetch_timeout: self.snapshot_fetch_timeout.map(|value| value.0),
            wasm_checkpoint_timeout: self.wasm_checkpoint_timeout.map(|value| value.0),
            event_capacity: self.event_capacity,
            wasm_recovery_capacity: self.wasm_recovery_capacity,
            immediate_flush_minimum: self.immediate_flush_minimum.map(|value| value.0),
            checkpoint_bytes: self.checkpoint_bytes.map(|value| value.0.as_u64()),
        })
    }
    pub(super) fn from_settings(settings: DataPlaneSettings) -> Self {
        Self {
            wasm_epoch_tick_interval: Some(HumanDuration(settings.wasm_epoch_tick_interval())),
            wasm_epoch_deadline_ticks: Some(settings.wasm_epoch_deadline_ticks()),
            wasm_guest_buffer_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.wasm_guest_buffer_bytes(),
            ))),
            wasm_optimize: Some(settings.wasm_optimize()),

            delivery_capacity: Some(settings.delivery_capacity()),
            ingest_group_max_rows: Some(settings.ingest_group_max_rows()),
            ingest_group_idle_flush: Some(HumanDuration(settings.ingest_group_idle_flush())),
            unacknowledged_poll_batch: Some(settings.unacknowledged_poll_batch()),
            ingestor_error_retry: Some(HumanDuration(settings.ingestor_error_retry())),
            reconnect_initial: Some(HumanDuration(settings.reconnect_initial())),
            reconnect_max: Some(HumanDuration(settings.reconnect_max())),
            branch_expiration_scan: Some(HumanDuration(settings.branch_expiration_scan())),
            replication_poll: Some(HumanDuration(settings.replication_poll())),
            catch_up_concurrency: Some(settings.catch_up_concurrency()),
            catch_up_pages: Some(settings.catch_up_pages()),
            retained_removals: Some(settings.retained_removals()),
            snapshot_staging_bytes: settings
                .requested_snapshot_staging_bytes()
                .map(|value| HumanBytes(ubyte::ByteUnit::Byte(value))),
            snapshot_bytes: settings
                .requested_snapshot_bytes()
                .map(|value| HumanBytes(ubyte::ByteUnit::Byte(value))),
            snapshot_describe_timeout: Some(HumanDuration(settings.snapshot_describe_timeout())),
            snapshot_fetch_timeout: Some(HumanDuration(settings.snapshot_fetch_timeout())),
            wasm_checkpoint_timeout: Some(HumanDuration(settings.wasm_checkpoint_timeout())),
            event_capacity: Some(settings.event_capacity()),
            wasm_recovery_capacity: Some(settings.wasm_recovery_capacity()),
            immediate_flush_minimum: Some(HumanDuration(settings.immediate_flush_minimum())),
            checkpoint_bytes: Some(HumanBytes(ubyte::ByteUnit::Byte(
                settings.checkpoint_bytes(),
            ))),
        }
    }
    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            nervix_models::RuntimeWasmEpochTickInterval::NAME,
            self.wasm_epoch_tick_interval,
        );
        command_line.optional(
            nervix_models::RuntimeWasmEpochDeadlineTicks::NAME,
            self.wasm_epoch_deadline_ticks,
        );
        command_line.optional(
            nervix_models::RuntimeWasmGuestBufferBytes::NAME,
            self.wasm_guest_buffer_bytes,
        );
        command_line.optional(nervix_models::RuntimeWasmOptimize::NAME, self.wasm_optimize);

        command_line.optional(
            nervix_models::RuntimeDeliveryCapacity::NAME,
            self.delivery_capacity,
        );
        command_line.optional(
            nervix_models::RuntimeIngestGroupMaxRows::NAME,
            self.ingest_group_max_rows,
        );
        command_line.optional(
            nervix_models::RuntimeIngestGroupIdleFlush::NAME,
            self.ingest_group_idle_flush,
        );
        command_line.optional(
            nervix_models::RuntimeUnacknowledgedPollBatch::NAME,
            self.unacknowledged_poll_batch,
        );
        command_line.optional(
            nervix_models::RuntimeIngestorErrorRetry::NAME,
            self.ingestor_error_retry,
        );
        command_line.optional(
            nervix_models::RuntimeReconnectInitial::NAME,
            self.reconnect_initial,
        );
        command_line.optional(nervix_models::RuntimeReconnectMax::NAME, self.reconnect_max);
        command_line.optional(
            nervix_models::RuntimeBranchExpirationScan::NAME,
            self.branch_expiration_scan,
        );
        command_line.optional(
            nervix_models::RuntimeReplicationPoll::NAME,
            self.replication_poll,
        );
        command_line.optional(
            nervix_models::RuntimeCatchUpConcurrency::NAME,
            self.catch_up_concurrency,
        );
        command_line.optional(
            nervix_models::RuntimeCatchUpPages::NAME,
            self.catch_up_pages,
        );
        command_line.optional(
            nervix_models::RuntimeRetainedRemovals::NAME,
            self.retained_removals,
        );
        command_line.optional(
            nervix_models::RuntimeSnapshotStagingBytes::NAME,
            self.snapshot_staging_bytes,
        );
        command_line.optional(
            nervix_models::RuntimeSnapshotBytes::NAME,
            self.snapshot_bytes,
        );
        command_line.optional(
            nervix_models::RuntimeSnapshotDescribeTimeout::NAME,
            self.snapshot_describe_timeout,
        );
        command_line.optional(
            nervix_models::RuntimeSnapshotFetchTimeout::NAME,
            self.snapshot_fetch_timeout,
        );
        command_line.optional(
            nervix_models::RuntimeWasmCheckpointTimeout::NAME,
            self.wasm_checkpoint_timeout,
        );
        command_line.optional(
            nervix_models::RuntimeEventCapacity::NAME,
            self.event_capacity,
        );
        command_line.optional(
            nervix_models::RuntimeWasmRecoveryCapacity::NAME,
            self.wasm_recovery_capacity,
        );
        command_line.optional(
            nervix_models::RuntimeImmediateFlushMinimum::NAME,
            self.immediate_flush_minimum,
        );
        command_line.optional(
            nervix_models::RuntimeCheckpointBytes::NAME,
            self.checkpoint_bytes,
        );
    }
}
