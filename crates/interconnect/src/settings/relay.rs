//! Relay channel retention: how long a peer's relay protocol owner keeps what reconciling a relay
//! channel needs.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The retention of each retained relay item, derived from the total admission budget
//!   its delivery captured or from the committed one, and the sweep cadence that honours it.
//! - **Depends on.** The vocabulary's captured acknowledgement timing and committed parameters.
//! - **Must not know.** Runtime records, acknowledgement ownership or graph policy.
#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns derived relay reconciliation timing")
)]
use std::time::Duration;

use nervix_models::{ClusterParameterValues, RemoteAckTiming};

/// The shortest interval a sweep waits between passes.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "one nanosecond is the shortest wait the monotonic clock expresses, and keeps a \
                  sweep of the shortest retention from waiting no time at all"
    )
)]
const SHORTEST_SWEEP_INTERVAL: Duration = Duration::from_nanos(1);

/// How long a peer's relay protocol owner keeps one retained item after it last made progress or
/// was last recorded or consulted: an attempt, an outbound correlation or a channel watermark.
///
/// An item keeps the retention of the delivery that last recorded it, twice the total admission
/// budget that delivery captured, so it outlasts the delivery and the idle rotation of its
/// sender's channel. Before any of a peer's deliveries registered, the owner applies the retention
/// the committed parameters give a delivery that starts now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelayChannelRetention(Duration);

impl RelayChannelRetention {
    /// The retention of an item whose delivery captured `timing`.
    pub fn captured_by(timing: RemoteAckTiming) -> Self {
        Self(timing.relay_channel_retention())
    }

    /// The retention the committed `values` give a delivery that starts now.
    pub fn committed(values: &ClusterParameterValues) -> Self {
        Self::captured_by(RemoteAckTiming::from_parameters(values))
    }

    pub fn duration(self) -> Duration {
        self.0
    }

    /// One tenth of the retention, and never less than one nanosecond: how often a sweep passes so
    /// that an item is forgotten within a tenth of its retention after that retention ends.
    pub fn sweep_interval(self) -> Duration {
        (self.0 / 10).max(SHORTEST_SWEEP_INTERVAL)
    }
}

impl Default for RelayChannelRetention {
    /// The retention the default parameters give, which a transport applies until the node
    /// installs its committed parameters.
    fn default() -> Self {
        Self::committed(&ClusterParameterValues::default())
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn retention_is_twice_the_captured_total_admission_budget_and_sweeps_at_a_tenth() {
        let timing = RemoteAckTiming {
            remote_relay_total_timeout: Duration::from_secs(30),
            ..RemoteAckTiming::default()
        };
        let retention = RelayChannelRetention::captured_by(timing);
        assert_eq!(retention.duration(), Duration::from_secs(60));
        assert_eq!(retention.sweep_interval(), Duration::from_secs(6));
        assert_eq!(
            RelayChannelRetention::captured_by(RemoteAckTiming {
                remote_relay_total_timeout: Duration::from_nanos(1),
                ..RemoteAckTiming::default()
            })
            .sweep_interval(),
            SHORTEST_SWEEP_INTERVAL
        );
    }

    #[test]
    fn the_committed_retention_follows_the_committed_total_admission_budget() {
        assert_eq!(
            RelayChannelRetention::committed(&ClusterParameterValues::default()).duration(),
            Duration::from_secs(600)
        );
        let longer = ClusterParameterValues::resolve(nervix_models::ClusterParameterSeedValues {
            remote_relay_total_timeout: Some(Duration::from_secs(900)),
            ..nervix_models::ClusterParameterSeedValues::default()
        })
        .assured("a longer total admission budget fits every acknowledgement relationship");
        assert_eq!(
            RelayChannelRetention::committed(&longer).duration(),
            Duration::from_secs(1800)
        );
    }
}
