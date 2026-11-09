//! A confirmed pause fault under a model command's admitted mutation lease.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Proposing one armed pause and recording its confirmed delivery.
//! - **Depends on.** The consensus proposer, exact borrowed lease, typed domain and fault owner.
//! - **Must not know.** Session routing, command outcomes or production pause recovery.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the scenario controls one pause proposal and observes its confirmation"
    )
)]

use error_stack::Report;
use futures_util::future::BoxFuture;
use nervix_consensus::{ConsensusError, DomainMutationLease, Proposer};
use nervix_models::DomainName;

use crate::fault_injection::FaultInjection;

pub(super) fn inject<'a>(
    consensus: &'a Proposer,
    faults: &'a FaultInjection,
    domain: DomainName,
    mutation: &'a DomainMutationLease,
) -> BoxFuture<'a, Result<DomainName, Report<ConsensusError>>> {
    Box::pin(async move {
        if let Err(report) = consensus.pause_domain(domain.clone(), Some(mutation)).await {
            faults.orphan_next_model_command_pause(domain);
            return Err(report);
        }
        faults.mark_orphaned_model_command_pause_delivered(&domain);
        Ok(domain)
    })
}
