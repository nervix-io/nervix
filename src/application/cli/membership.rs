//! The membership options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the node unavailability timeout, its conversion into the
//!   membership settings group, and the rendering of that group back into options.
//! - **Depends on.** The membership settings definition and the edge's duration form.
//! - **Must not know.** How membership observes a node.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::cli_values::HumanDuration,
    cluster::{MembershipSettings, NodeUnavailabilityTimeout},
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Membership")]
pub struct MembershipArgs {
    #[arg(
        long = NodeUnavailabilityTimeout::NAME,
        env = "NERVIX_NODE_UNAVAILABILITY_TIMEOUT",
        default_value_t = HumanDuration(NodeUnavailabilityTimeout::default_value(&())),
        help = "How long a member may go unobserved before the cluster treats it as unavailable"
    )]
    pub node_unavailability_timeout: HumanDuration,
}

impl MembershipArgs {
    pub(super) fn settings(&self) -> MembershipSettings {
        MembershipSettings::resolve(Some(self.node_unavailability_timeout.0))
            .assured("the membership settings declare no validation")
    }

    pub(super) fn from_settings(settings: &MembershipSettings) -> Self {
        Self {
            node_unavailability_timeout: HumanDuration(settings.node_unavailability_timeout()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(
            NodeUnavailabilityTimeout::NAME,
            self.node_unavailability_timeout,
        );
    }
}
