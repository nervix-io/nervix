//! Controlled standalone command transaction boundaries for public recovery scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Node- and domain-scoped barriers after a standalone command's transaction lookup and
//!   after the preflight of its statement.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Plexus syntax, transaction outcomes or recovery policy.

use nervix_models::{ClusterNodeName, DomainName};

use super::{CommandPausePoint, FaultInjection};

/// Where a coordinator preparing a standalone command's transaction is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StandaloneTransactionBoundary {
    /// The coordinator has looked its transaction up and not yet acted on what it observed.
    Lookup,
    /// The coordinator has preflighted the command's statement and not yet acted on the result.
    Preflight,
}

impl FaultInjection {
    pub fn pause_standalone_transaction(
        &self,
        node: ClusterNodeName,
        domain: DomainName,
        boundary: StandaloneTransactionBoundary,
    ) {
        self.arm_command_pause(CommandPausePoint::StandaloneTransaction {
            node,
            domain,
            boundary,
        });
    }

    pub async fn wait_for_standalone_transaction_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StandaloneTransactionBoundary,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::StandaloneTransaction {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }

    pub fn release_standalone_transaction_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StandaloneTransactionBoundary,
    ) {
        self.release_command_pause(&CommandPausePoint::StandaloneTransaction {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        });
    }

    pub(crate) async fn pause_standalone_transaction_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
        boundary: StandaloneTransactionBoundary,
    ) {
        self.pause_command_if_armed(CommandPausePoint::StandaloneTransaction {
            node: node.clone(),
            domain: domain.clone(),
            boundary,
        })
        .await;
    }
}
