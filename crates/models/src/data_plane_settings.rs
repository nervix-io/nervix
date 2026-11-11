//! Node-local data-plane capacities and their validated setting definitions.
//!
//! Layer: vocabulary.
//! - **Owns.** Runtime queue, grouping, retry, staging and checkpoint capabilities.
//! - **Depends on.** Setting classes, duration and byte representations and serialization.
//! - **Must not know.** Runtime tasks, storage, connectors, consensus or command-line parsing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns local runtime capability vocabulary")
)]

use std::time::Duration;

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{NodeCapabilities, Setting, SettingDefinition};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DataPlaneSettingsError {
    #[error("{setting} must be positive and fit the unsigned 32-bit collection bound")]
    Count { setting: &'static str },
    #[error("{setting} must be positive and no longer than u32::MAX seconds")]
    Duration { setting: &'static str },
    #[error("{setting} must be positive and fit a signed 64-bit byte count")]
    Bytes { setting: &'static str },
    #[error("runtime-reconnect-initial must not exceed runtime-reconnect-max")]
    ReconnectOrder,
    #[error("runtime-snapshot-bytes must not exceed runtime-snapshot-staging-bytes")]
    SnapshotOrder,
    #[error("{setting} ({quota} bytes) exceeds the temporary volume capacity ({capacity} bytes)")]
    SnapshotVolume {
        setting: &'static str,
        quota: u64,
        capacity: u64,
    },
}

macro_rules! validate_value {
    ($value:expr, $name:expr, count) => {
        if *$value == 0 || *$value > u64::from(u32::MAX) {
            return Err(Report::new(DataPlaneSettingsError::Count {
                setting: $name,
            }));
        }
    };
    ($value:expr, $name:expr, bytes) => {
        if *$value == 0 || *$value > i64::MAX.unsigned_abs() {
            return Err(Report::new(DataPlaneSettingsError::Bytes {
                setting: $name,
            }));
        }
    };
    ($value:expr, $name:expr, duration) => {
        if $value.is_zero() || *$value > Duration::from_secs(u64::from(u32::MAX)) {
            return Err(Report::new(DataPlaneSettingsError::Duration {
                setting: $name,
            }));
        }
    };
    ($value:expr, $name:expr, boolean) => {
        let _ = $value;
    };
}

macro_rules! runtime_value_text {
    ($value:expr, duration) => {
        crate::timing_text($value)
    };
    ($value:expr, bytes) => {
        format!("{}B", $value)
    };
    ($value:expr, $kind:ident) => {
        $value.to_string()
    };
}

macro_rules! runtime_capabilities {
    ($(#[doc = $doc:literal] $field:ident: $definition:ident($value:ty) = $default:expr => $kind:ident,)+) => {
        $(
            #[doc = $doc]
            pub struct $definition;
            impl SettingDefinition for $definition {
                type Class = NodeCapabilities;
                type Value = $value;
                type Context = ();
                type Error = DataPlaneSettingsError;
                const NAME: &'static str = setting_name!($field);
                const DERIVATION: Option<&'static str> = setting_derivation!($field);
                fn default_value(_: &()) -> $value { $default }
                fn validate(value: &$value, _: &()) -> Result<(), Report<Self::Error>> {
                    validate_value!(value, Self::NAME, $kind);
                    Ok(())
                }
            }
        )+
        /// Validated capabilities resolved once for one node incarnation.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct DataPlaneSettings {
            $(pub $field: Setting<$definition>,)+
            snapshot_staging_explicit: bool,
            snapshot_explicit: bool,
        }
        /// Explicit operator values, resolved through their definitions.
        #[derive(Debug, Default, Clone, Copy)]
        pub struct DataPlaneSettingOverrides { $(pub $field: Option<$value>,)+ }
        /// The immutable capacity claim announced by a node.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize)]
        pub struct DataPlaneCapabilities { $(pub $field: $value,)+ }
        impl DataPlaneSettings {
            pub fn resolve(values: DataPlaneSettingOverrides) -> Result<Self, Report<DataPlaneSettingsError>> {
                let mut settings = Self {
                    $($field: Setting::resolve(values.$field, &())?,)+
                    snapshot_staging_explicit: values.snapshot_staging_bytes.is_some(),
                    snapshot_explicit: values.snapshot_bytes.is_some(),
                };
                if !settings.snapshot_explicit {
                    settings.snapshot_bytes = Setting::resolve(Some(settings.snapshot_bytes().min(settings.snapshot_staging_bytes())), &())?;
                }
                if settings.reconnect_initial() > settings.reconnect_max() {
                    return Err(Report::new(DataPlaneSettingsError::ReconnectOrder));
                }
                if settings.snapshot_bytes() > settings.snapshot_staging_bytes() {
                    return Err(Report::new(DataPlaneSettingsError::SnapshotOrder));
                }
                Ok(settings)
            }
            $(pub fn $field(&self) -> $value { *self.$field.value() })+
            pub fn capabilities(self) -> DataPlaneCapabilities {
                DataPlaneCapabilities { $($field: self.$field(),)+ }
            }
        }
        impl DataPlaneCapabilities {
            pub fn described(self) -> Vec<(&'static str, String)> {
                vec![$(($definition::NAME, runtime_value_text!(self.$field, $kind)),)+]
            }
        }
        impl Default for DataPlaneSettings {
            fn default() -> Self {
                Self::resolve(DataPlaneSettingOverrides::default()).assured("runtime defaults validate together")
            }
        }
    };
}

macro_rules! setting_derivation {
    (snapshot_staging_bytes) => {
        Some("without an override, limited by the temporary volume capacity")
    };
    (snapshot_bytes) => {
        Some("without an override, limited by the staging quota and temporary volume capacity")
    };
    ($field:ident) => {
        None
    };
}

// Option names are declared once beside the typed field vocabulary.
macro_rules! setting_name {
    (delivery_capacity) => {
        "runtime-delivery-capacity"
    };
    (ingest_group_max_rows) => {
        "runtime-ingest-group-max-rows"
    };
    (ingest_group_idle_flush) => {
        "runtime-ingest-group-idle-flush"
    };
    (unacknowledged_poll_batch) => {
        "runtime-unacknowledged-poll-batch"
    };
    (ingestor_error_retry) => {
        "runtime-ingestor-error-retry"
    };
    (reconnect_initial) => {
        "runtime-reconnect-initial"
    };
    (reconnect_max) => {
        "runtime-reconnect-max"
    };
    (branch_expiration_scan) => {
        "runtime-branch-expiration-scan"
    };
    (replication_poll) => {
        "runtime-replication-poll"
    };
    (catch_up_concurrency) => {
        "runtime-catch-up-concurrency"
    };
    (catch_up_pages) => {
        "runtime-catch-up-pages"
    };
    (retained_removals) => {
        "runtime-retained-removals"
    };
    (snapshot_staging_bytes) => {
        "runtime-snapshot-staging-bytes"
    };
    (snapshot_bytes) => {
        "runtime-snapshot-bytes"
    };
    (snapshot_describe_timeout) => {
        "runtime-snapshot-describe-timeout"
    };
    (snapshot_fetch_timeout) => {
        "runtime-snapshot-fetch-timeout"
    };
    (wasm_checkpoint_timeout) => {
        "runtime-wasm-checkpoint-timeout"
    };
    (event_capacity) => {
        "runtime-event-capacity"
    };
    (wasm_recovery_capacity) => {
        "runtime-wasm-recovery-capacity"
    };
    (immediate_flush_minimum) => {
        "runtime-immediate-flush-minimum"
    };
    (checkpoint_bytes) => {
        "runtime-checkpoint-bytes"
    };
    (wasm_epoch_tick_interval) => {
        "runtime-wasm-epoch-tick-interval"
    };
    (wasm_epoch_deadline_ticks) => {
        "runtime-wasm-epoch-deadline-ticks"
    };
    (wasm_guest_buffer_bytes) => {
        "runtime-wasm-guest-buffer-bytes"
    };
    (wasm_optimize) => {
        "runtime-wasm-optimize"
    };
}

runtime_capabilities! {
    /// Maximum concurrent remote deliveries in each correlation lane.
    delivery_capacity: RuntimeDeliveryCapacity(u64) = 8192 => count,
    /// Decoded rows that close an ingest group.
    ingest_group_max_rows: RuntimeIngestGroupMaxRows(u64) = 1024 => count,
    /// Idle deadline of a partial ingest group.
    ingest_group_idle_flush: RuntimeIngestGroupIdleFlush(Duration) = Duration::from_millis(5) => duration,
    /// Messages requested per unacknowledged broker poll.
    unacknowledged_poll_batch: RuntimeUnacknowledgedPollBatch(u64) = 1 => count,
    /// Delay after a transient ingestor error.
    ingestor_error_retry: RuntimeIngestorErrorRetry(Duration) = Duration::from_millis(100) => duration,
    /// Initial reconnect delay for host-owned retry.
    reconnect_initial: RuntimeReconnectInitial(Duration) = Duration::from_millis(250) => duration,
    /// Maximum reconnect delay for host-owned retry.
    reconnect_max: RuntimeReconnectMax(Duration) = Duration::from_secs(30) => duration,
    /// Cadence of branch lifetime expiration scans.
    branch_expiration_scan: RuntimeBranchExpirationScan(Duration) = Duration::from_secs(30) => duration,
    /// Cadence of state replication polls.
    replication_poll: RuntimeReplicationPoll(Duration) = Duration::from_secs(1) => duration,
    /// Checkpoint fetches admitted in one catch-up round.
    catch_up_concurrency: RuntimeCatchUpConcurrency(u64) = 16 => count,
    /// Catalog pages visited in one catch-up round.
    catch_up_pages: RuntimeCatchUpPages(u64) = 64 => count,
    /// Catalog removals retained for incremental replication.
    retained_removals: RuntimeRetainedRemovals(u64) = 1024 => count,
    /// Concurrent temporary snapshot staging quota.
    snapshot_staging_bytes: RuntimeSnapshotStagingBytes(u64) = 128 * 1024 * 1024 * 1024 => bytes,
    /// Largest snapshot staged by one transfer.
    snapshot_bytes: RuntimeSnapshotBytes(u64) = 64 * 1024 * 1024 * 1024 => bytes,
    /// Deadline of describing a sealed state snapshot.
    snapshot_describe_timeout: RuntimeSnapshotDescribeTimeout(Duration) = Duration::from_secs(30) => duration,
    /// Deadline of fetching a sealed state snapshot.
    snapshot_fetch_timeout: RuntimeSnapshotFetchTimeout(Duration) = Duration::from_secs(30) => duration,
    /// Deadline until every guest checkpoint replica confirms.
    wasm_checkpoint_timeout: RuntimeWasmCheckpointTimeout(Duration) = Duration::from_secs(90) => duration,
    /// Runtime events retained for a lagging observer.
    event_capacity: RuntimeEventCapacity(u64) = 256 => count,
    /// Guest-state recovery requests awaiting a decision.
    wasm_recovery_capacity: RuntimeWasmRecoveryCapacity(u64) = 32 => count,
    /// Minimum timer interval for immediate branch flushes.
    immediate_flush_minimum: RuntimeImmediateFlushMinimum(Duration) = Duration::from_micros(100) => duration,
    /// Largest state checkpoint accepted by an in-memory transfer.
    checkpoint_bytes: RuntimeCheckpointBytes(u64) = 256 * 1024 * 1024 => bytes,
    /// WASM epoch preemption quantum.
    wasm_epoch_tick_interval: RuntimeWasmEpochTickInterval(Duration) = Duration::from_millis(1) => duration,
    /// WASM epoch ticks before an asynchronous yield.
    wasm_epoch_deadline_ticks: RuntimeWasmEpochDeadlineTicks(u64) = 1 => count,
    /// Maximum guest invocation buffer.
    wasm_guest_buffer_bytes: RuntimeWasmGuestBufferBytes(u64) = 64 * 1024 * 1024 => bytes,
    /// Enable WASM compilation optimisation.
    wasm_optimize: RuntimeWasmOptimize(bool) = true => boolean,
}

// The explicitness bits occupy existing padding, never enlarging a runtime's retained settings.
const _: () = assert!(
    std::mem::size_of::<DataPlaneSettings>() == std::mem::size_of::<DataPlaneCapabilities>()
);

impl DataPlaneSettings {
    pub fn requested_snapshot_staging_bytes(&self) -> Option<u64> {
        if self.snapshot_staging_explicit {
            Some(self.snapshot_staging_bytes())
        } else {
            None
        }
    }

    pub fn requested_snapshot_bytes(&self) -> Option<u64> {
        if self.snapshot_explicit {
            Some(self.snapshot_bytes())
        } else {
            None
        }
    }

    /// Resolve omitted disk quotas once the startup owner has measured the temporary volume.
    /// Explicit values are never reduced; they must fit both the volume and the staging quota.
    pub fn for_temporary_volume(
        mut self,
        capacity: u64,
    ) -> Result<Self, Report<DataPlaneSettingsError>> {
        let staging = match self.requested_snapshot_staging_bytes() {
            Some(value) => value,
            None => RuntimeSnapshotStagingBytes::default_value(&()).min(capacity),
        };
        let snapshot = match self.requested_snapshot_bytes() {
            Some(value) => value,
            None => RuntimeSnapshotBytes::default_value(&())
                .min(staging)
                .min(capacity),
        };
        for (setting, quota) in [
            (RuntimeSnapshotStagingBytes::NAME, staging),
            (RuntimeSnapshotBytes::NAME, snapshot),
        ] {
            if quota > capacity {
                return Err(Report::new(DataPlaneSettingsError::SnapshotVolume {
                    setting,
                    quota,
                    capacity,
                }));
            }
        }
        self.snapshot_staging_bytes = Setting::resolve(Some(staging), &())?;
        self.snapshot_bytes = Setting::resolve(Some(snapshot), &())?;
        if snapshot > staging {
            return Err(Report::new(DataPlaneSettingsError::SnapshotOrder));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn related_local_capacities_refuse_an_incoherent_group() {
        for overrides in [
            DataPlaneSettingOverrides {
                reconnect_initial: Some(Duration::from_secs(31)),
                ..Default::default()
            },
            DataPlaneSettingOverrides {
                snapshot_staging_bytes: Some(1),
                snapshot_bytes: Some(2),
                ..Default::default()
            },
            DataPlaneSettingOverrides {
                delivery_capacity: Some(0),
                ..Default::default()
            },
            DataPlaneSettingOverrides {
                wasm_guest_buffer_bytes: Some(0),
                ..Default::default()
            },
        ] {
            assert!(DataPlaneSettings::resolve(overrides).is_err());
        }
        let settings = DataPlaneSettings::resolve(DataPlaneSettingOverrides {
            snapshot_bytes: Some(1024),
            snapshot_staging_bytes: Some(2048),
            reconnect_initial: Some(Duration::from_millis(1)),
            reconnect_max: Some(Duration::from_millis(2)),
            ..Default::default()
        })
        .assured("ordered capacities validate");
        assert_eq!(settings.snapshot_bytes(), 1024);
        assert_eq!(settings.reconnect_max(), Duration::from_millis(2));
    }

    #[test]
    fn snapshot_defaults_fit_the_detected_volume_and_preserve_omission() {
        let settings = DataPlaneSettings::default()
            .for_temporary_volume(1024)
            .assured("defaults fit a small volume");
        assert_eq!(settings.snapshot_staging_bytes(), 1024);
        assert_eq!(settings.snapshot_bytes(), 1024);
        assert_eq!(settings.capabilities().snapshot_staging_bytes, 1024);
        assert_eq!(settings.requested_snapshot_staging_bytes(), None);
        assert_eq!(settings.requested_snapshot_bytes(), None);
        let explicit = DataPlaneSettings::resolve(DataPlaneSettingOverrides {
            snapshot_staging_bytes: Some(512),
            ..Default::default()
        })
        .assured("the omitted snapshot bound follows staging")
        .for_temporary_volume(1024)
        .assured("explicit staging fits");
        assert_eq!(explicit.snapshot_bytes(), 512);
        assert_eq!(explicit.requested_snapshot_staging_bytes(), Some(512));
        assert_eq!(explicit.requested_snapshot_bytes(), None);
        let large = DataPlaneSettings::default()
            .for_temporary_volume(u64::MAX)
            .assured("a large volume keeps the ceilings");
        assert_eq!(
            large.snapshot_staging_bytes(),
            RuntimeSnapshotStagingBytes::default_value(&())
        );
        assert_eq!(
            large.snapshot_bytes(),
            RuntimeSnapshotBytes::default_value(&())
        );
    }

    #[test]
    fn explicit_snapshot_quotas_above_the_volume_are_refused_without_reduction() {
        for overrides in [
            DataPlaneSettingOverrides {
                snapshot_staging_bytes: Some(2048),
                ..Default::default()
            },
            DataPlaneSettingOverrides {
                snapshot_bytes: Some(2048),
                ..Default::default()
            },
        ] {
            let settings =
                DataPlaneSettings::resolve(overrides).assured("the configured group is ordered");
            assert!(matches!(
                settings
                    .for_temporary_volume(1024)
                    .expect_err("the explicit quota exceeds disk")
                    .current_context(),
                DataPlaneSettingsError::SnapshotVolume {
                    quota: 2048,
                    capacity: 1024,
                    ..
                }
            ));
        }
    }
}
