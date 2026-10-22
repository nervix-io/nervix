//! The consensus options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the Raft timings and the Raft log retention bounds, their
//!   conversion into the consensus settings groups, and the rendering of those groups back into
//!   options.
//! - **Depends on.** The consensus settings definitions and the edge's duration and byte forms.
//! - **Must not know.** What consensus does with the values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_consensus::{
    RaftCoveredLogBytesRetained, RaftCoveredLogEntriesRetained, RaftElectionTimeoutMax,
    RaftElectionTimeoutMin, RaftHeartbeatInterval, RaftRetainedLogCap, RaftRetentionBounds,
    RaftRetentionSettings, RaftSnapshotByteThreshold, RaftSnapshotEntryThreshold, RaftTimings,
};
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::cli_values::{HumanBytes, HumanDuration};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Consensus")]
pub struct ConsensusArgs {
    #[arg(
        long = RaftHeartbeatInterval::NAME,
        env = "NERVIX_RAFT_HEARTBEAT_INTERVAL",
        default_value_t = HumanDuration(RaftHeartbeatInterval::default_value(&())),
        help = "How often a leader tells its followers it is still leading"
    )]
    pub raft_heartbeat_interval: HumanDuration,
    #[arg(
        long = RaftElectionTimeoutMin::NAME,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MIN",
        default_value_t = HumanDuration(RaftElectionTimeoutMin::default_value(&())),
        help = "The shortest time a follower waits without hearing from a leader before it stands \
                for election"
    )]
    pub raft_election_timeout_min: HumanDuration,
    #[arg(
        long = RaftElectionTimeoutMax::NAME,
        env = "NERVIX_RAFT_ELECTION_TIMEOUT_MAX",
        default_value_t = HumanDuration(RaftElectionTimeoutMax::default_value(&())),
        help = "The longest time a follower waits without hearing from a leader before it stands \
                for election"
    )]
    pub raft_election_timeout_max: HumanDuration,
    #[arg(
        long = RaftSnapshotEntryThreshold::NAME,
        env = "NERVIX_RAFT_SNAPSHOT_ENTRY_THRESHOLD",
        default_value_t = RaftSnapshotEntryThreshold::default_value(&()),
        help = "Committed Raft entries since the last snapshot that start a new one"
    )]
    pub raft_snapshot_entry_threshold: u64,
    #[arg(
        long = RaftSnapshotByteThreshold::NAME,
        env = "NERVIX_RAFT_SNAPSHOT_BYTE_THRESHOLD",
        default_value_t = HumanBytes(RaftSnapshotByteThreshold::default_value(&())),
        help = "Appended Raft log bytes since the last snapshot that start a new one"
    )]
    pub raft_snapshot_byte_threshold: HumanBytes,
    #[arg(
        long = RaftCoveredLogEntriesRetained::NAME,
        env = "NERVIX_RAFT_COVERED_LOG_ENTRIES_RETAINED",
        default_value_t = RaftCoveredLogEntriesRetained::default_value(&()),
        help = "Raft log entries a snapshot covers that the node keeps"
    )]
    pub raft_covered_log_entries_retained: u64,
    #[arg(
        long = RaftCoveredLogBytesRetained::NAME,
        env = "NERVIX_RAFT_COVERED_LOG_BYTES_RETAINED",
        default_value_t = HumanBytes(RaftCoveredLogBytesRetained::default_value(&())),
        help = "Raft log bytes a snapshot covers that the node keeps; whichever covered bound is \
                reached first decides"
    )]
    pub raft_covered_log_bytes_retained: HumanBytes,
    #[arg(
        long = RaftRetainedLogCap::NAME,
        env = "NERVIX_RAFT_RETAINED_LOG_CAP",
        default_value_t = HumanBytes(RaftRetainedLogCap::default_value(&())),
        help = "Retained Raft log bytes above which new mutations wait for reclamation"
    )]
    pub raft_retained_log_cap: HumanBytes,
}

impl ConsensusArgs {
    pub(super) fn timings(&self) -> RaftTimings {
        RaftTimings::resolve(
            Some(self.raft_heartbeat_interval.0),
            Some(self.raft_election_timeout_min.0),
            Some(self.raft_election_timeout_max.0),
        )
        .assured("the Raft timings declare no validation")
    }

    pub(super) fn retention(&self) -> RaftRetentionSettings {
        RaftRetentionSettings::resolve(RaftRetentionBounds {
            snapshot_entry_threshold: Some(self.raft_snapshot_entry_threshold),
            snapshot_byte_threshold: Some(self.raft_snapshot_byte_threshold.0),
            covered_entries_retained: Some(self.raft_covered_log_entries_retained),
            covered_bytes_retained: Some(self.raft_covered_log_bytes_retained.0),
            retained_log_cap: Some(self.raft_retained_log_cap.0),
        })
        .assured("the Raft retention bounds declare no validation")
    }

    pub(super) fn from_settings(timings: &RaftTimings, retention: &RaftRetentionSettings) -> Self {
        Self {
            raft_heartbeat_interval: HumanDuration(timings.heartbeat_interval()),
            raft_election_timeout_min: HumanDuration(timings.election_timeout_min()),
            raft_election_timeout_max: HumanDuration(timings.election_timeout_max()),
            raft_snapshot_entry_threshold: *retention.snapshot_entry_threshold.value(),
            raft_snapshot_byte_threshold: HumanBytes(*retention.snapshot_byte_threshold.value()),
            raft_covered_log_entries_retained: *retention.covered_entries_retained.value(),
            raft_covered_log_bytes_retained: HumanBytes(*retention.covered_bytes_retained.value()),
            raft_retained_log_cap: HumanBytes(*retention.retained_log_cap.value()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(RaftHeartbeatInterval::NAME, self.raft_heartbeat_interval);
        command_line.option(RaftElectionTimeoutMin::NAME, self.raft_election_timeout_min);
        command_line.option(RaftElectionTimeoutMax::NAME, self.raft_election_timeout_max);
        command_line.option(
            RaftSnapshotEntryThreshold::NAME,
            self.raft_snapshot_entry_threshold,
        );
        command_line.option(
            RaftSnapshotByteThreshold::NAME,
            self.raft_snapshot_byte_threshold,
        );
        command_line.option(
            RaftCoveredLogEntriesRetained::NAME,
            self.raft_covered_log_entries_retained,
        );
        command_line.option(
            RaftCoveredLogBytesRetained::NAME,
            self.raft_covered_log_bytes_retained,
        );
        command_line.option(RaftRetainedLogCap::NAME, self.raft_retained_log_cap);
    }
}
