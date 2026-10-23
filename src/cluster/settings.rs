//! How long the cluster waits before it treats a silent member as unavailable, declared as a
//! setting.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node unavailability timeout's definition, its single default and the group
//!   that holds it.
//! - **Depends on.** The vocabulary's setting classes.
//! - **Must not know.** Command-line parsing or how membership observes a node.

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
use nervix_models::{ClusterParameters, Setting, SettingDefinition};

/// How long a member may go unobserved before the cluster treats it as unavailable.
pub struct NodeUnavailabilityTimeout;

impl SettingDefinition for NodeUnavailabilityTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "node-unavailability-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(10)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// How the cluster judges whether its members are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipSettings {
    pub node_unavailability_timeout: Setting<NodeUnavailabilityTimeout>,
}

impl MembershipSettings {
    /// Resolves the timeout through its definition's default when it is not given.
    pub fn resolve(
        node_unavailability_timeout: Option<Duration>,
    ) -> Result<Self, Report<Infallible>> {
        Ok(Self {
            node_unavailability_timeout: Setting::resolve(node_unavailability_timeout, &())?,
        })
    }

    pub fn node_unavailability_timeout(&self) -> Duration {
        *self.node_unavailability_timeout.value()
    }
}

impl Default for MembershipSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the membership defaults declare no validation")
    }
}
