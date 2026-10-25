//! The Raft timings and log retention a node is configured with, declared as settings.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The setting definition of the Raft heartbeat, which a node must bring to reach the
//!   live cluster, the election timeouts it derives from it by fixed factors, the durability
//!   promise its log appends keep, and the definitions of the snapshot cadence and covered-log
//!   retention bounds, which are what this node's storage carries; their single defaults and the
//!   groups that hold them.
//! - **Depends on.** The vocabulary's setting classes, prerequisites and byte quantities.
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
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    NodeCapabilities, Prerequisites, RaftLogDurability, Setting, SettingDefinition,
    TIMING_COMPATIBILITY_FACTOR, TimingPrerequisite,
};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::RaftRetentionPolicy;

/// How many heartbeat intervals a follower waits, at least, without hearing from a leader before
/// it stands for election.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the election timeouts derive from the heartbeat by fixed factors, so every \
                  node's pairing of heartbeat and election stays inside the compatibility bound"
    )
)]
pub const ELECTION_TIMEOUT_MIN_HEARTBEATS: u32 = 6;

/// How many heartbeat intervals a follower waits, at most, without hearing from a leader before it
/// stands for election.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the election timeouts derive from the heartbeat by fixed factors, so every \
                  node's pairing of heartbeat and election stays inside the compatibility bound"
    )
)]
pub const ELECTION_TIMEOUT_MAX_HEARTBEATS: u32 = 12;

const _: () = assert!(
    ELECTION_TIMEOUT_MIN_HEARTBEATS < ELECTION_TIMEOUT_MAX_HEARTBEATS,
    "the randomized election window must be nonempty"
);
const _: () = assert!(
    TIMING_COMPATIBILITY_FACTOR * 3 <= ELECTION_TIMEOUT_MIN_HEARTBEATS,
    "the slowest heartbeat a compatible leader sends must stay within a third of every follower's \
     minimum election timeout"
);

/// What every Raft log append is synchronized with before the node acknowledges it.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the consensus store commits every append with a full synchronization, the \
                  promise Raft's acknowledgements rely on"
    )
)]
pub const RAFT_LOG_DURABILITY: RaftLogDurability = RaftLogDurability::FsyncEveryAppend;

/// Why a Raft heartbeat cannot run. Each variant names the setting by its option name, with the
/// value given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RaftTimingError {
    #[error(
        "{heartbeat} ({heartbeat_interval:?}) must be at least 1ms, the resolution consensus runs \
         timings at",
        heartbeat = RaftHeartbeatInterval::NAME
    )]
    HeartbeatBelowOneMillisecond { heartbeat_interval: Duration },
    #[error(
        "{heartbeat} ({heartbeat_interval:?}) times {ELECTION_TIMEOUT_MAX_HEARTBEATS} must fit the \
         milliseconds consensus counts an election timeout in",
        heartbeat = RaftHeartbeatInterval::NAME
    )]
    HeartbeatTooLong { heartbeat_interval: Duration },
}

/// The whole milliseconds consensus runs a timing with.
fn whole_milliseconds(timing: Duration) -> u128 {
    timing.as_millis()
}

/// How often a leader tells its followers it is still leading.
pub struct RaftHeartbeatInterval;

impl SettingDefinition for RaftHeartbeatInterval {
    type Class = Prerequisites;
    type Value = Duration;
    type Context = ();
    type Error = RaftTimingError;

    const NAME: &'static str = TimingPrerequisite::RaftHeartbeatInterval.setting_name();
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_millis(250)
    }

    fn validate(heartbeat_interval: &Duration, _: &()) -> Result<(), Report<RaftTimingError>> {
        let milliseconds = whole_milliseconds(*heartbeat_interval);
        if milliseconds == 0 {
            return Err(Report::new(RaftTimingError::HeartbeatBelowOneMillisecond {
                heartbeat_interval: *heartbeat_interval,
            }));
        }
        let longest_election = milliseconds
            .checked_mul(u128::from(ELECTION_TIMEOUT_MAX_HEARTBEATS))
            .assured("a duration's milliseconds fit 75 bits, so twelve of them fit 128");
        if u64::try_from(longest_election).is_err() {
            return Err(Report::new(RaftTimingError::HeartbeatTooLong {
                heartbeat_interval: *heartbeat_interval,
            }));
        }
        Ok(())
    }
}

/// The heartbeat a node brings to the cluster, and the election timeouts derived from it.
///
/// The election timeouts are never configured on their own: they are
/// [`ELECTION_TIMEOUT_MIN_HEARTBEATS`] and [`ELECTION_TIMEOUT_MAX_HEARTBEATS`] heartbeats, so two
/// nodes whose heartbeats stay within the compatibility bound also keep each other's elections
/// apart from their heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftTimings {
    pub heartbeat_interval: Setting<RaftHeartbeatInterval>,
}

impl RaftTimings {
    /// Resolves the heartbeat through its definition's default when it is not given, then
    /// validates it.
    pub fn resolve(heartbeat_interval: Option<Duration>) -> Result<Self, Report<RaftTimingError>> {
        Ok(Self {
            heartbeat_interval: Setting::resolve(heartbeat_interval, &())?,
        })
    }

    pub fn heartbeat_interval(&self) -> Duration {
        *self.heartbeat_interval.value()
    }

    /// The shortest time a follower waits without hearing from a leader before it stands for
    /// election.
    pub fn election_timeout_min(&self) -> Duration {
        self.heartbeats(ELECTION_TIMEOUT_MIN_HEARTBEATS)
    }

    /// The longest time a follower waits without hearing from a leader before it stands for
    /// election.
    pub fn election_timeout_max(&self) -> Duration {
        self.heartbeats(ELECTION_TIMEOUT_MAX_HEARTBEATS)
    }

    fn heartbeats(&self, count: u32) -> Duration {
        self.heartbeat_interval().checked_mul(count).verified(
            "the heartbeat's validation keeps twelve of them inside consensus milliseconds",
        )
    }
}

impl Default for RaftTimings {
    fn default() -> Self {
        Self::resolve(None).assured("the default heartbeat is at least one millisecond and short")
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::OptionExt as _;
    use nervix_models::SettingDefinition as _;

    use super::{RaftHeartbeatInterval, RaftTimingError, RaftTimings};

    #[test]
    fn the_election_timeouts_are_fixed_multiples_of_the_heartbeat() {
        let resolved =
            RaftTimings::resolve(Some(Duration::from_millis(100))).expect("a 100ms heartbeat runs");
        assert_eq!(resolved.heartbeat_interval(), Duration::from_millis(100));
        assert_eq!(resolved.election_timeout_min(), Duration::from_millis(600));
        assert_eq!(resolved.election_timeout_max(), Duration::from_millis(1200));

        let defaults = RaftTimings::default();
        assert_eq!(defaults.heartbeat_interval(), Duration::from_millis(250));
        assert_eq!(defaults.election_timeout_min(), Duration::from_millis(1500));
        assert_eq!(defaults.election_timeout_max(), Duration::from_millis(3000));
        assert_eq!(RaftHeartbeatInterval::NAME, "raft-heartbeat-interval");
    }

    #[test]
    fn a_heartbeat_below_one_whole_millisecond_is_refused_naming_it() {
        for heartbeat in [Duration::ZERO, Duration::from_micros(999)] {
            let refused = RaftTimings::resolve(Some(heartbeat))
                .err()
                .assured("consensus runs this heartbeat as zero milliseconds");
            assert_eq!(
                refused.current_context(),
                &RaftTimingError::HeartbeatBelowOneMillisecond {
                    heartbeat_interval: heartbeat,
                }
            );
        }
        let refused = RaftTimings::resolve(Some(Duration::from_micros(500)))
            .err()
            .assured("half a millisecond rounds to none");
        assert_eq!(
            format!("{refused:#}"),
            "raft-heartbeat-interval (500µs) must be at least 1ms, the resolution consensus runs \
             timings at"
        );
        assert!(RaftTimings::resolve(Some(Duration::from_millis(1))).is_ok());
    }

    #[test]
    fn a_heartbeat_whose_election_timeout_exceeds_consensus_milliseconds_is_refused() {
        let longest = Duration::from_millis(u64::MAX / 12);
        assert!(RaftTimings::resolve(Some(longest)).is_ok());
        let too_long = Duration::from_millis(u64::MAX / 12 + 1);
        let refused = RaftTimings::resolve(Some(too_long))
            .err()
            .assured("twelve of these heartbeats exceed u64 milliseconds");
        assert_eq!(
            refused.current_context(),
            &RaftTimingError::HeartbeatTooLong {
                heartbeat_interval: too_long,
            }
        );
    }
}
