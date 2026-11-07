//! Node settings across the command line that renders and reads them.
//!
//! Layer: test harness.
//! - **Owns.** Generation of current, valid node settings for every settings group, with their
//!   relationships held, and the complete round-trip oracle of rendering them as options and
//!   reading them back, together with the duration and byte quantity forms help prints defaults in.
//! - **Depends on.** The settings groups, the options that render and read them, and the edge's
//!   duration and byte forms.
//! - **Must not know.** What any owner does with its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the node settings command-line boundary")
)]

use std::time::Duration;

use nervix_arbitrary::Entropy;
use nervix_consensus::{RaftRetentionBounds, RaftRetentionSettings, RaftTimings};
use ubyte::ByteUnit;

use super::*;
use crate::{
    application::{
        CommandExecutionPolicy, NodeSettings, SchedulingSettings, ShutdownSettings,
        TransactionBounds, TransactionLimits,
        cli_values::{HumanBytes, HumanDuration},
    },
    capacity::CapacitySettings,
    cluster::{MembershipSettings, MembershipValues},
    memory_pressure::MemoryPressureSettings,
    resource::ResourceSettings,
    runtime::RuntimeSettings,
};

const CASE_BYTES: usize = 512;

/// The most whole seconds a generated duration spans, far beyond any timing a node runs with and
/// well inside what duration text can name.
const LONGEST_SECONDS: u64 = 1_000_000_000_000;

/// The most whole milliseconds a generated timing prerequisite spans above its floor.
const LONGEST_TIMING_MILLIS: u64 = 1_000_000_000;

/// Builds current node settings, each group valid on its own and every relationship held.
struct GeneratedSettings<'bytes> {
    entropy: Entropy<'bytes>,
}

impl<'bytes> GeneratedSettings<'bytes> {
    fn new(bytes: &'bytes [u8]) -> Self {
        Self {
            entropy: Entropy::new(bytes),
        }
    }

    fn duration(&mut self) -> Duration {
        let seconds = self.entropy.boundary_biased(0..=LONGEST_SECONDS);
        let nanos = self.entropy.between(0..=999_999_999);
        let nanos = u32::try_from(nanos).verified("the range above ends below a second");
        Duration::new(seconds, nanos)
    }

    /// A duration of at least one nanosecond.
    fn positive_duration(&mut self) -> Duration {
        let duration = self.duration();
        if duration.is_zero() {
            return Duration::from_nanos(1);
        }
        duration
    }

    /// A duration whose whole milliseconds are at least `floor_millis`, with any sub-millisecond
    /// remainder.
    fn timing_from(&mut self, floor_millis: u64) -> Duration {
        let extra = self.entropy.boundary_biased(0..=LONGEST_TIMING_MILLIS);
        let millis = floor_millis
            .checked_add(extra)
            .assured("the floor and the extra are each far below u64::MAX");
        let sub_millisecond = self.entropy.between(0..=999_999);
        let sub_millisecond =
            u32::try_from(sub_millisecond).verified("the range above ends below a millisecond");
        Duration::from_millis(millis)
            .checked_add(Duration::from_nanos(u64::from(sub_millisecond)))
            .assured("a bounded timing and less than a millisecond fit a duration")
    }

    fn bytes(&mut self) -> ByteUnit {
        let unit = self.entropy.pick([
            ByteUnit::B,
            ByteUnit::KiB,
            ByteUnit::MiB,
            ByteUnit::GiB,
            ByteUnit::TiB,
        ]);
        let count = self.entropy.boundary_biased(0..=u64::MAX / unit.as_u64());
        ByteUnit::Byte(
            count
                .checked_mul(unit.as_u64())
                .verified("the count is bounded by the unit it multiplies"),
        )
    }

    fn count(&mut self) -> u64 {
        self.entropy.any_u64()
    }

    fn raft(&mut self) -> RaftTimings {
        let heartbeat = self.timing_from(1);
        RaftTimings::resolve(Some(heartbeat)).assured(
            "the heartbeat is at least a millisecond and twelve of it fit u64 milliseconds",
        )
    }

    fn membership(&mut self) -> MembershipSettings {
        MembershipSettings::resolve(MembershipValues {
            node_unavailability_timeout: Some(self.cluster_duration()),
            gossip_interval: Some(self.timing_from(1)),
            clock_skew_tolerance: Some(self.timing_from(1)),
        })
        .assured("each membership timing is at least a millisecond and bounded")
    }

    fn raft_retention(&mut self) -> RaftRetentionSettings {
        RaftRetentionSettings::resolve(RaftRetentionBounds {
            snapshot_entry_threshold: Some(self.count()),
            snapshot_byte_threshold: Some(self.bytes()),
            covered_entries_retained: Some(self.count()),
            covered_bytes_retained: Some(self.bytes()),
            retained_log_cap: Some(self.bytes()),
        })
        .assured("the Raft retention bounds declare no validation")
    }

    fn capacity(&mut self) -> CapacitySettings {
        let memory_limit = if self.entropy.flag() {
            Some(ByteUnit::Byte(self.entropy.between(1..=u64::MAX)))
        } else {
            None
        };
        CapacitySettings::resolve(memory_limit)
            .assured("a memory limit, when given, is at least one byte")
    }

    fn memory_pressure(&mut self) -> MemoryPressureSettings {
        let watermarks = if self.entropy.flag() {
            let high = self.entropy.between(1..=u64::MAX);
            let below_high = high
                .checked_sub(1)
                .verified("the high watermark is at least one");
            let low = self.entropy.between(0..=below_high);
            Some((ByteUnit::Byte(high), ByteUnit::Byte(low)))
        } else {
            None
        };
        let check_interval = self.positive_duration();
        let resume_jitter = self.duration();
        MemoryPressureSettings::resolve(
            watermarks.map(|(high, _)| high),
            watermarks.map(|(_, low)| low),
            Some(check_interval),
            Some(resume_jitter),
        )
        .assured("the watermarks are paired, ordered and checked at a positive interval")
    }

    fn shutdown(&mut self) -> ShutdownSettings {
        let shutdown_timeout = self.duration();
        // A drain this long is far longer than any node takes, and never longer than the
        // shutdown.
        let longest_drain = u64::try_from(shutdown_timeout.as_nanos()).unwrap_or(u64::MAX);
        let drain_timeout = Duration::from_nanos(self.entropy.boundary_biased(0..=longest_drain));
        ShutdownSettings::resolve(Some(shutdown_timeout), Some(drain_timeout))
            .assured("the drain never exceeds the shutdown")
    }

    fn cluster_duration(&mut self) -> Duration {
        let seconds = self.entropy.boundary_biased(0..=u64::from(u32::MAX));
        let nanos = if seconds == u64::from(u32::MAX) {
            0
        } else {
            self.entropy.boundary_biased(0..=999_999_999)
        };
        let nanos = if seconds == 0 { nanos.max(1) } else { nanos };
        Duration::new(
            seconds,
            u32::try_from(nanos).assured("generated nanoseconds are below one second"),
        )
    }

    fn cluster_count(&mut self) -> usize {
        usize::try_from(self.entropy.boundary_biased(0..=u64::from(u32::MAX)))
            .assured("a native target represents the cluster count domain")
    }

    fn positive_bytes(&mut self) -> ubyte::ByteUnit {
        ubyte::ByteUnit::Byte(self.bytes().as_u64().max(1))
    }

    fn node_settings(&mut self) -> NodeSettings {
        let raft = self.raft();
        let raft_retention = self.raft_retention();
        let membership = self.membership();
        let scheduling = SchedulingSettings::resolve(Some(self.cluster_count()))
            .assured("the scheduling settings declare no validation");
        let restore_staging_max_bytes = if self.entropy.flag() {
            Some(self.bytes())
        } else {
            None
        };
        let runtime =
            RuntimeSettings::resolve(Some(self.cluster_duration()), restore_staging_max_bytes)
                .assured("the runtime settings declare no validation");
        let transactions = TransactionLimits::resolve(TransactionBounds {
            idle_timeout: Some(self.cluster_duration()),
            tombstone_retention: Some(self.cluster_duration()),
            max_statements: Some(self.cluster_count()),
            max_source_bytes: Some(self.count()),
            max_open: Some(self.cluster_count()),
        })
        .assured("the transaction limits declare no validation");
        let commands = CommandExecutionPolicy::resolve(
            Some(self.cluster_duration()),
            Some(self.cluster_count().max(1)),
        )
        .assured("the command execution settings declare no validation");
        let memory_pressure = self.memory_pressure();
        let shutdown = self.shutdown();
        let resources = ResourceSettings::resolve(
            Some(self.positive_bytes()),
            Some(self.positive_bytes()),
            Some(self.count().max(1)),
        )
        .assured("the resource settings declare no validation");
        let capacity = self.capacity();
        let mut settings = NodeSettings {
            parameter_seeds: Default::default(),
            raft,
            raft_retention,
            membership,
            scheduling,
            runtime,
            transactions,
            commands,
            memory_pressure,
            shutdown,
            resources,
            capacity,
        };
        let args = super::SettingsArgs::from(&settings);
        settings.parameter_seeds = args.parameter_seeds();
        settings
    }
}

#[test]
fn bolero_node_settings_survive_their_command_line() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|bytes: &[u8]| {
            let mut generated = GeneratedSettings::new(bytes);
            let settings = generated.node_settings();
            let read = settings
                .with_overrides(&[])
                .assured("every generated setting renders as an option the command line reads");
            assert_eq!(
                read, settings,
                "every setting of every group survives its command line"
            );

            let duration = generated.duration();
            let read_duration = HumanDuration(duration)
                .to_string()
                .parse::<HumanDuration>()
                .assured("a rendered duration is duration text");
            assert_eq!(read_duration, HumanDuration(duration));

            let bytes = generated.bytes();
            let read_bytes = HumanBytes(bytes)
                .to_string()
                .parse::<HumanBytes>()
                .assured("a rendered byte quantity is byte text");
            assert_eq!(read_bytes, HumanBytes(bytes));
        });
}
