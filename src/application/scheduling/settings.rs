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

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::Setting;

/// What the scheduler places beside each owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulingSettings {
    pub replica_count: Setting<ReplicaCount>,
}

impl SchedulingSettings {
    /// Resolves the replica count through its definition's default when it is not given.
    pub fn resolve(
        replica_count: Option<usize>,
    ) -> Result<Self, Report<nervix_models::ClusterParameterError>> {
        Ok(Self {
            replica_count: Setting::resolve(replica_count.map(|value| value.arch_into()), &())?,
        })
    }

    pub fn replica_count(&self) -> usize {
        usize::try_from(*self.replica_count.value())
            .assured("the validated count fits a supported native target")
    }
}

impl Default for SchedulingSettings {
    fn default() -> Self {
        Self::resolve(None).assured("the scheduling defaults have valid bounds")
    }
}

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
pub use nervix_models::ReplicaCount;
