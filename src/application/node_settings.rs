//! Every setting one node runs with, each in the group of its owner.
//!
//! Layer: edges.
//!
//! - **Owns.** The composition of the class-typed settings groups an application carries.
//! - **Depends on.** Each owner's settings group, which declares its settings' classes,
//!   defaults and validation.
//! - **Must not know.** Command-line parsing, or how any owner uses its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_consensus::{RaftRetentionSettings, RaftTimings};

use super::{
    CommandExecutionPolicy, ShutdownSettings, scheduling::SchedulingSettings,
    transaction::TransactionLimits,
};
use crate::{
    cluster::MembershipSettings, memory_pressure::MemoryPressureSettings,
    resource::ResourceSettings, runtime::RuntimeSettings,
};

/// Every setting one node runs with, each in the group of the owner that declares it.
///
/// Its default resolves every definition's own default; nothing else writes one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeSettings {
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
}
