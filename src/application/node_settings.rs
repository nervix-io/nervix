//! Every setting one node runs with, each in the group of its owner.
//!
//! Layer: edges.
//!
//! - **Owns.** The composition of the class-typed settings groups an application carries, and
//!   the join terms its settings make the node bring to the cluster.
//! - **Depends on.** Each owner's settings group, which declares its settings' classes,
//!   defaults and validation.
//! - **Must not know.** Command-line parsing, or how any owner uses its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use arch_into::ArchInto as _;
use nervix_consensus::{RAFT_LOG_DURABILITY, RaftRetentionSettings, RaftTimings};
use nervix_interconnect::JoinTerms;
use nervix_models::PrerequisiteTimings;

use super::{
    CommandExecutionPolicy, ShutdownSettings, scheduling::SchedulingSettings,
    transaction::TransactionLimits,
};
use crate::{
    capacity::CapacitySettings, cluster::MembershipSettings,
    memory_pressure::MemoryPressureSettings, resource::ResourceSettings, runtime::RuntimeSettings,
};

/// Every setting one node runs with, each in the group of the owner that declares it.
///
/// Its default resolves every definition's own default; nothing else writes one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeSettings {
    /// Explicit first-bootstrap seeds, retained separately from resolved local defaults.
    pub parameter_seeds: nervix_models::ClusterParameterSeedValues,
    pub raft: RaftTimings,
    pub raft_retention: RaftRetentionSettings,
    pub membership: MembershipSettings,
    pub scheduling: SchedulingSettings,
    pub runtime: RuntimeSettings,
    pub transactions: TransactionLimits,
    pub commands: CommandExecutionPolicy,
    pub memory_pressure: MemoryPressureSettings,
    pub shutdown: ShutdownSettings,
    pub resources: ResourceSettings,
    pub capacity: CapacitySettings,
}

impl NodeSettings {
    /// The prerequisites these settings make the node bring to the cluster beside what its
    /// transport owns: the durability its Raft log promises and its timings.
    pub fn join_terms(&self) -> JoinTerms {
        JoinTerms {
            durability: RAFT_LOG_DURABILITY,
            timings: PrerequisiteTimings {
                raft_heartbeat_interval: self.raft.heartbeat_interval(),
                gossip_interval: self.membership.gossip_interval(),
                clock_skew_tolerance: self.membership.clock_skew_tolerance(),
            },
        }
    }
}

impl NodeSettings {
    /// Resolve a new cluster's values from its bootstrap inputs, never from another node's flags.
    pub(crate) fn bootstrap_parameters(
        &self,
    ) -> error_stack::Result<
        nervix_models::ClusterParameterValues,
        nervix_models::ClusterParameterError,
    > {
        let seed = nervix_models::ClusterParameterSeedValues {
            node_unavailability_timeout: Some(self.membership.node_unavailability_timeout()),
            state_snapshot_interval: Some(self.runtime.state_snapshot_interval()),
            transaction_idle_timeout: Some(self.transactions.idle_timeout()),
            transaction_tombstone_retention: Some(self.transactions.tombstone_retention()),
            transaction_max_statements: Some(self.transactions.max_statements().arch_into()),
            transaction_max_source_bytes: Some(self.transactions.max_source_bytes()),
            transaction_max_open: Some(self.transactions.max_open().arch_into()),
            command_retry_validity: Some(self.commands.retry_validity()),
            command_execution_capacity: Some(self.commands.capacity().arch_into()),
            replica_count: Some(self.scheduling.replica_count().arch_into()),
            resource_max_archive_bytes: Some(self.resources.limits().max_archive_bytes),
            resource_max_extracted_bytes: Some(self.resources.limits().max_extracted_bytes),
            resource_max_file_count: Some(self.resources.limits().max_file_count),
            ..self.parameter_seeds
        };
        nervix_models::ClusterParameterValues::resolve(seed)
    }
}
