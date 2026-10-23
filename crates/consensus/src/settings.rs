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
use thiserror::Error;
use ubyte::ByteUnit;

use crate::RaftRetentionPolicy;

/// Why Raft timings cannot run together. Each variant names both settings by their option names,
/// with the values given, because the relationship between them is what failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RaftTimingError {
    #[error(
        "{min} ({election_timeout_min:?}) must exceed {heartbeat} ({heartbeat_interval:?})",
        min = RaftElectionTimeoutMin::NAME,
        heartbeat = RaftHeartbeatInterval::NAME
    )]
    ElectionTimeoutNotAboveHeartbeat {
        election_timeout_min: Duration,
        heartbeat_interval: Duration,
    },
    #[error(
        "{max} ({election_timeout_max:?}) must exceed {min} ({election_timeout_min:?})",
        max = RaftElectionTimeoutMax::NAME,
        min = RaftElectionTimeoutMin::NAME
    )]
    ElectionTimeoutMaxNotAboveMin {
        election_timeout_max: Duration,
        election_timeout_min: Duration,
    },
}

/// The whole milliseconds consensus runs a timing with, which is how two timings are compared.
fn whole_milliseconds(timing: Duration) -> u128 {
    timing.as_millis()
}

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
    /// The heartbeat interval the minimum must exceed.
    type Context = Duration;
    type Error = RaftTimingError;

    const NAME: &'static str = "raft-election-timeout-min";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &Duration) -> Duration {
        Duration::from_millis(1500)
    }

    fn validate(
        election_timeout_min: &Duration,
        heartbeat_interval: &Duration,
    ) -> Result<(), Report<RaftTimingError>> {
        if whole_milliseconds(*election_timeout_min) > whole_milliseconds(*heartbeat_interval) {
            return Ok(());
        }
        Err(Report::new(
            RaftTimingError::ElectionTimeoutNotAboveHeartbeat {
                election_timeout_min: *election_timeout_min,
                heartbeat_interval: *heartbeat_interval,
            },
        ))
    }
}

/// The longest time a follower waits without hearing from a leader before it stands for
/// election.
pub struct RaftElectionTimeoutMax;

impl SettingDefinition for RaftElectionTimeoutMax {
    type Class = Prerequisites;
    type Value = Duration;
    /// The minimum election timeout the maximum must exceed.
    type Context = Duration;
    type Error = RaftTimingError;

    const NAME: &'static str = "raft-election-timeout-max";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &Duration) -> Duration {
        Duration::from_millis(3000)
    }

    fn validate(
        election_timeout_max: &Duration,
        election_timeout_min: &Duration,
    ) -> Result<(), Report<RaftTimingError>> {
        if whole_milliseconds(*election_timeout_max) > whole_milliseconds(*election_timeout_min) {
            return Ok(());
        }
        Err(Report::new(
            RaftTimingError::ElectionTimeoutMaxNotAboveMin {
                election_timeout_max: *election_timeout_max,
                election_timeout_min: *election_timeout_min,
            },
        ))
    }
}

/// The heartbeat and election timeouts a node brings to the cluster.
///
/// Its construction validates, in the whole milliseconds consensus runs with, that the heartbeat
/// interval is shorter than the minimum election timeout and the minimum shorter than the maximum,
/// so consensus never opens with timings it would reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftTimings {
    pub heartbeat_interval: Setting<RaftHeartbeatInterval>,
    pub election_timeout_min: Setting<RaftElectionTimeoutMin>,
    pub election_timeout_max: Setting<RaftElectionTimeoutMax>,
}

impl RaftTimings {
    /// Resolves each timing that is not given through its definition's default, then validates
    /// each against the one it depends on.
    pub fn resolve(
        heartbeat_interval: Option<Duration>,
        election_timeout_min: Option<Duration>,
        election_timeout_max: Option<Duration>,
    ) -> Result<Self, Report<RaftTimingError>> {
        let heartbeat_interval = Setting::<RaftHeartbeatInterval>::resolve(heartbeat_interval, &())
            .assured("the heartbeat interval declares no validation of its own");
        let election_timeout_min =
            Setting::resolve(election_timeout_min, heartbeat_interval.value())?;
        let election_timeout_max =
            Setting::resolve(election_timeout_max, election_timeout_min.value())?;
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
        Self::resolve(None, None, None).assured(
            "the default heartbeat is shorter than the default election minimum, which is shorter \
             than the default maximum",
        )
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

    use super::{RaftTimingError, RaftTimings};

    fn timings(
        heartbeat_millis: u64,
        minimum_millis: u64,
        maximum_millis: u64,
    ) -> Result<RaftTimings, error_stack::Report<RaftTimingError>> {
        RaftTimings::resolve(
            Some(Duration::from_millis(heartbeat_millis)),
            Some(Duration::from_millis(minimum_millis)),
            Some(Duration::from_millis(maximum_millis)),
        )
    }

    #[test]
    fn timings_in_order_resolve_and_the_defaults_are_in_order() {
        let resolved = timings(100, 300, 600).expect("ordered timings are valid");
        assert_eq!(resolved.heartbeat_interval(), Duration::from_millis(100));
        assert_eq!(resolved.election_timeout_min(), Duration::from_millis(300));
        assert_eq!(resolved.election_timeout_max(), Duration::from_millis(600));

        let defaults = RaftTimings::default();
        assert_eq!(defaults.heartbeat_interval(), Duration::from_millis(250));
        assert_eq!(defaults.election_timeout_min(), Duration::from_millis(1500));
        assert_eq!(defaults.election_timeout_max(), Duration::from_millis(3000));
    }

    #[test]
    fn an_election_minimum_that_does_not_exceed_the_heartbeat_names_both() {
        for minimum in [200, 250] {
            let refused = timings(250, minimum, 3000)
                .err()
                .assured("the minimum does not exceed the heartbeat");
            assert_eq!(
                refused.current_context(),
                &RaftTimingError::ElectionTimeoutNotAboveHeartbeat {
                    election_timeout_min: Duration::from_millis(minimum),
                    heartbeat_interval: Duration::from_millis(250),
                }
            );
        }
        let refused = timings(250, 200, 3000)
            .err()
            .assured("the minimum is below the heartbeat");
        assert_eq!(
            format!("{refused:#}"),
            "raft-election-timeout-min (200ms) must exceed raft-heartbeat-interval (250ms)"
        );
    }

    #[test]
    fn an_election_maximum_that_does_not_exceed_the_minimum_names_both() {
        let refused = timings(250, 1500, 1000)
            .err()
            .assured("the maximum is below the minimum");

        assert_eq!(
            refused.current_context(),
            &RaftTimingError::ElectionTimeoutMaxNotAboveMin {
                election_timeout_max: Duration::from_secs(1),
                election_timeout_min: Duration::from_millis(1500),
            }
        );
        assert_eq!(
            format!("{refused:#}"),
            "raft-election-timeout-max (1s) must exceed raft-election-timeout-min (1.5s)"
        );
    }

    #[test]
    fn timings_are_compared_in_the_whole_milliseconds_consensus_runs_with() {
        // Consensus runs both timings as 250 whole milliseconds, where the minimum would not
        // exceed the heartbeat, so the finer difference cannot make them valid.
        let refused = RaftTimings::resolve(
            Some(Duration::from_micros(250_100)),
            Some(Duration::from_micros(250_900)),
            None,
        )
        .err()
        .assured("both timings are 250 whole milliseconds");

        assert!(matches!(
            refused.current_context(),
            RaftTimingError::ElectionTimeoutNotAboveHeartbeat { .. }
        ));
    }
}
