//! The Raft timings and log retention a node is configured with, declared as settings.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The setting definitions of the Raft heartbeat and election timeouts, which a node
//!   must bring to reach the live cluster, and of the snapshot cadence and covered-log retention
//!   bounds, which are what this node's storage carries; their single defaults and the groups that
//!   hold them.
//! - **Depends on.** The vocabulary's setting classes and byte quantities.
//! - **Must not know.** Command-line parsing, or how the server's edge names a value it refuses.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{NodeCapabilities, Prerequisites, Setting, SettingDefinition};
use ubyte::ByteUnit;

use crate::RaftRetentionPolicy;

/// How often a leader tells its followers it is still leading.
pub struct RaftHeartbeatInterval;

impl SettingDefinition for RaftHeartbeatInterval {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-heartbeat-interval";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(250)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The shortest time a follower waits without hearing from a leader before it stands for
/// election.
pub struct RaftElectionTimeoutMin;

impl SettingDefinition for RaftElectionTimeoutMin {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-election-timeout-min";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(1500)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The longest time a follower waits without hearing from a leader before it stands for
/// election.
pub struct RaftElectionTimeoutMax;

impl SettingDefinition for RaftElectionTimeoutMax {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-election-timeout-max";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(3000)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The heartbeat and election timeouts a node brings to the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftTimings {
    pub heartbeat_interval: Setting<RaftHeartbeatInterval>,
    pub election_timeout_min: Setting<RaftElectionTimeoutMin>,
    pub election_timeout_max: Setting<RaftElectionTimeoutMax>,
}

impl RaftTimings {
    /// Resolves each timing that is not given through its definition's default.
    pub fn resolve(
        heartbeat_interval: Option<Duration>,
        election_timeout_min: Option<Duration>,
        election_timeout_max: Option<Duration>,
    ) -> Result<Self, Report<Infallible>> {
        let heartbeat_interval = Setting::resolve(heartbeat_interval, &())?;
        let election_timeout_min = Setting::resolve(election_timeout_min, &())?;
        let election_timeout_max = Setting::resolve(election_timeout_max, &())?;
        Ok(Self {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
        })
    }

    pub fn heartbeat_interval(&self) -> Duration {
        *self.heartbeat_interval.value()
    }

    pub fn election_timeout_min(&self) -> Duration {
        *self.election_timeout_min.value()
    }

    pub fn election_timeout_max(&self) -> Duration {
        *self.election_timeout_max.value()
    }
}

impl Default for RaftTimings {
    fn default() -> Self {
        Self::resolve(None, None, None).assured("the Raft timing defaults declare no validation")
    }
}

/// Committed entries since the completed snapshot that start a new one.
pub struct RaftSnapshotEntryThreshold;

impl SettingDefinition for RaftSnapshotEntryThreshold {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-snapshot-entry-threshold";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        10_000
    }

    fn validate(_: &u64, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// Appended bytes since the completed snapshot that start a new one.
pub struct RaftSnapshotByteThreshold;

impl SettingDefinition for RaftSnapshotByteThreshold {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-snapshot-byte-threshold";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(64)
    }

    fn validate(_: &ByteUnit, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// Snapshot-covered entries the node keeps.
pub struct RaftCoveredLogEntriesRetained;

impl SettingDefinition for RaftCoveredLogEntriesRetained {
    type Class = NodeCapabilities;
    type Value = u64;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-covered-log-entries-retained";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        1_000
    }

    fn validate(_: &u64, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// Snapshot-covered bytes the node keeps. Whichever covered bound is reached first decides.
pub struct RaftCoveredLogBytesRetained;

impl SettingDefinition for RaftCoveredLogBytesRetained {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-covered-log-bytes-retained";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(64)
    }

    fn validate(_: &ByteUnit, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The retained log size above which new mutations wait for reclamation.
pub struct RaftRetainedLogCap;

impl SettingDefinition for RaftRetainedLogCap {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "raft-retained-log-cap";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Gibibyte(1)
    }

    fn validate(_: &ByteUnit, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// What this node keeps of its Raft log, and when it snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftRetentionSettings {
    pub snapshot_entry_threshold: Setting<RaftSnapshotEntryThreshold>,
    pub snapshot_byte_threshold: Setting<RaftSnapshotByteThreshold>,
    pub covered_entries_retained: Setting<RaftCoveredLogEntriesRetained>,
    pub covered_bytes_retained: Setting<RaftCoveredLogBytesRetained>,
    pub retained_log_cap: Setting<RaftRetainedLogCap>,
}

impl RaftRetentionSettings {
    /// Resolves each bound that is not given through its definition's default.
    pub fn resolve(bounds: RaftRetentionBounds) -> Result<Self, Report<Infallible>> {
        Ok(Self {
            snapshot_entry_threshold: Setting::resolve(bounds.snapshot_entry_threshold, &())?,
            snapshot_byte_threshold: Setting::resolve(bounds.snapshot_byte_threshold, &())?,
            covered_entries_retained: Setting::resolve(bounds.covered_entries_retained, &())?,
            covered_bytes_retained: Setting::resolve(bounds.covered_bytes_retained, &())?,
            retained_log_cap: Setting::resolve(bounds.retained_log_cap, &())?,
        })
    }

    /// The retention machinery's policy: these bounds, with the reclamation wait it owns.
    pub fn policy(&self) -> RaftRetentionPolicy {
        RaftRetentionPolicy::with_bounds(
            *self.snapshot_entry_threshold.value(),
            self.snapshot_byte_threshold.value().as_u64(),
            *self.covered_entries_retained.value(),
            self.covered_bytes_retained.value().as_u64(),
            self.retained_log_cap.value().as_u64(),
        )
    }
}

impl Default for RaftRetentionSettings {
    fn default() -> Self {
        Self::resolve(RaftRetentionBounds::default())
            .assured("the Raft retention defaults declare no validation")
    }
}

/// Retention bounds an operator or a test gives; an absent bound takes its definition's default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftRetentionBounds {
    pub snapshot_entry_threshold: Option<u64>,
    pub snapshot_byte_threshold: Option<ByteUnit>,
    pub covered_entries_retained: Option<u64>,
    pub covered_bytes_retained: Option<ByteUnit>,
    pub retained_log_cap: Option<ByteUnit>,
}
