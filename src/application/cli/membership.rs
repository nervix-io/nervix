//! The membership options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the node unavailability timeout, the gossip interval, the
//!   clock-skew tolerance, the gossip receive queue capacity and the gossip tombstone grace
//!   period, their conversion into the membership settings group, and the rendering of that group
//!   back into options.
//! - **Depends on.** The membership settings definition and the edge's duration form.
//! - **Must not know.** How membership observes a node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroUsize;

use error_stack::Report;
use nervix_models::SettingDefinition;

use super::settings::{CommandLine, capability_help};
use crate::{
    application::cli_values::HumanDuration,
    cluster::{
        ClockSkewTolerance, GossipInterval, GossipReceiveQueueCapacity, GossipTombstoneGracePeriod,
        MembershipSettings, MembershipSettingsError, MembershipValues, NodeUnavailabilityTimeout,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Membership")]
pub struct MembershipArgs {
    #[arg(
        long = NodeUnavailabilityTimeout::NAME,
        env = "NERVIX_NODE_UNAVAILABILITY_TIMEOUT",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::NodeUnavailabilityTimeout, "How long a member may go unobserved before the cluster treats it as unavailable")
    )]
    pub node_unavailability_timeout: Option<HumanDuration>,
    #[arg(
        long = GossipInterval::NAME,
        env = "NERVIX_GOSSIP_INTERVAL",
        default_value_t = HumanDuration(GossipInterval::default_value(&())),
        help = "How often the node exchanges its gossip record with its peers; every pairing of \
                nodes keeps it within a factor of two"
    )]
    pub gossip_interval: HumanDuration,
    #[arg(
        long = ClockSkewTolerance::NAME,
        env = "NERVIX_CLOCK_SKEW_TOLERANCE",
        default_value_t = HumanDuration(ClockSkewTolerance::default_value(&())),
        help = "The physical clock skew the node tolerates against the peer it joins the cluster \
                through"
    )]
    pub clock_skew_tolerance: HumanDuration,
    #[arg(
        long = GossipReceiveQueueCapacity::NAME,
        env = "NERVIX_GOSSIP_RECEIVE_QUEUE_CAPACITY",
        help = capability_help::<GossipReceiveQueueCapacity>(
            "Incoming gossip datagrams queued between the interconnect and gossip, each at most \
             60 KiB",
            GossipReceiveQueueCapacity::default_value(&()),
        )
    )]
    pub gossip_receive_queue_capacity: Option<NonZeroUsize>,
    #[arg(
        long = GossipTombstoneGracePeriod::NAME,
        env = "NERVIX_GOSSIP_TOMBSTONE_GRACE_PERIOD",
        help = capability_help::<GossipTombstoneGracePeriod>(
            "How long a deleted gossip key's tombstone stays before gossip collects it",
            HumanDuration(GossipTombstoneGracePeriod::default_value(&())),
        )
    )]
    pub gossip_tombstone_grace_period: Option<HumanDuration>,
}

impl MembershipArgs {
    pub(super) fn settings(&self) -> Result<MembershipSettings, Report<MembershipSettingsError>> {
        MembershipSettings::resolve(MembershipValues {
            node_unavailability_timeout: self.node_unavailability_timeout.map(|value| value.0),
            gossip_interval: Some(self.gossip_interval.0),
            clock_skew_tolerance: Some(self.clock_skew_tolerance.0),
            gossip_receive_queue_capacity: self.gossip_receive_queue_capacity,
            gossip_tombstone_grace_period: self.gossip_tombstone_grace_period.map(|value| value.0),
        })
    }

    pub(super) fn from_settings(settings: &MembershipSettings) -> Self {
        Self {
            node_unavailability_timeout: Some(HumanDuration(
                settings.node_unavailability_timeout(),
            )),
            gossip_interval: HumanDuration(settings.gossip_interval()),
            clock_skew_tolerance: HumanDuration(settings.clock_skew_tolerance()),
            gossip_receive_queue_capacity: Some(settings.gossip_receive_queue_capacity()),
            gossip_tombstone_grace_period: Some(HumanDuration(
                settings.gossip_tombstone_grace_period(),
            )),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            NodeUnavailabilityTimeout::NAME,
            self.node_unavailability_timeout,
        );
        command_line.option(GossipInterval::NAME, self.gossip_interval);
        command_line.option(ClockSkewTolerance::NAME, self.clock_skew_tolerance);
        command_line.optional(
            GossipReceiveQueueCapacity::NAME,
            self.gossip_receive_queue_capacity,
        );
        command_line.optional(
            GossipTombstoneGracePeriod::NAME,
            self.gossip_tombstone_grace_period,
        );
    }
}
