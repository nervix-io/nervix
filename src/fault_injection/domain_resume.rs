//! Controlled domain-pause release boundaries for public recovery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Node- and domain-scoped barriers around a resume proposal and its refusal.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Plexus syntax, transaction outcomes or recovery policy.

use nervix_models::{ClusterNodeName, DomainName};

use super::{CommandPausePoint, FaultInjection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomainResumeBoundary {
    Proposal,
    Failure,
}

impl FaultInjection {
    pub fn pause_domain_resume(
        &self,
        node: ClusterNodeName,
        domain: DomainName,
        boundary: DomainResumeBoundary,
    ) {
        self.arm_command_pause(CommandPausePoint::DomainResume {
            node,
            domain,
            boundary,
        });
    }

    pub async fn wait_for_domain_resume_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: DomainResumeBoundary,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::DomainResume {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }

    pub fn release_domain_resume_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: DomainResumeBoundary,
    ) {
        self.release_command_pause(&CommandPausePoint::DomainResume {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        });
    }

    pub(crate) async fn pause_domain_resume_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: DomainResumeBoundary,
    ) {
        self.pause_command_if_armed(CommandPausePoint::DomainResume {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }
}
