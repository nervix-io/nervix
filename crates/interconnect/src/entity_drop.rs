//! The typed node-to-node request that collects what each node discarded when it destroyed the
//! entities a change dropped.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The drop report wire contract and its pool, quota, deadline and coordination
//!   identity declarations.
//! - **Depends on.** The authenticated request surface and typed vocabulary identities.
//! - **Must not know.** How a node destroys an entity, what it counts, or how an answer presents
//!   the report.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::{CoordinationIdentity, DiscardedWork, DomainName};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, PoolClass, RemoteOperationFailure, RequestSubquota};

/// What a node discarded when it applied the runtime revision that dropped entities of a domain.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum DropReportAnswer {
    /// The node has not applied the revision yet.
    Pending,
    /// The node holds no report for the revision: it restarted after the revision applied, so
    /// what an earlier run of it discarded is not known.
    Unknown,
    /// Everything the node discarded, entity by entity and kind by kind; empty when it ran none of
    /// the dropped entities or they held nothing.
    Reported(Vec<DiscardedWork>),
}

/// The report the coordinator of a change collects from a node once the change's runtime revision
/// is applied.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropReportRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    /// The runtime revision whose application dropped the entities.
    pub revision: u64,
}

impl InterconnectRequest for DropReportRequest {
    type Response = Result<DropReportAnswer, RemoteOperationFailure>;
    const NAME: &'static str = "entity_drop_report";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Coordination;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod wire_properties;
