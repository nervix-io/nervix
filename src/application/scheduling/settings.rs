//! How many replicas of each piece of runtime state the scheduler places, declared as a setting.
//!
//! Layer: edges.
//!
//! - **Owns.** The replica count's definition, its single default and the group that holds it.
//! - **Depends on.** The vocabulary's setting classes.
//! - **Must not know.** Command-line parsing or how a schedule is computed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::convert::Infallible;

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{ClusterParameters, Setting, SettingDefinition};

/// How many nodes besides its owner hold a replica of each piece of runtime state.
pub struct ReplicaCount;

impl SettingDefinition for ReplicaCount {
    type Class = ClusterParameters;
    type Value = usize;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "replica-count";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> usize {
        0
    }

    fn validate(_: &usize, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// What the scheduler places beside each owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulingSettings {
    pub replica_count: Setting<ReplicaCount>,
}

impl SchedulingSettings {
    /// Resolves the replica count through its definition's default when it is not given.
    pub fn resolve(replica_count: Option<usize>) -> Result<Self, Report<Infallible>> {
        Ok(Self {
            replica_count: Setting::resolve(replica_count, &())?,
        })
    }

    pub fn replica_count(&self) -> usize {
        *self.replica_count.value()
    }
}

impl Default for SchedulingSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the scheduling defaults declare no validation")
    }
}
