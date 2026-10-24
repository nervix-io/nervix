//! Controlled transaction tombstone cleanup for public expiry observations.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** A node- and domain-scoped barrier before finished transaction cleanup.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Transaction outcomes, retention policy or Plexus syntax.

use nervix_models::{ClusterNodeName, DomainName};

use super::{CommandPausePoint, FaultInjection};

impl FaultInjection {
    pub fn pause_transaction_cleanup(&self, node: ClusterNodeName, domain: DomainName) {
        self.arm_command_pause(CommandPausePoint::TransactionCleanup { node, domain });
    }

    pub async fn wait_for_transaction_cleanup_pause(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::TransactionCleanup {
            node: node.clone(),
            domain: domain.clone(),
        })
        .await;
    }

    pub fn release_transaction_cleanup_pause(&self, node: &ClusterNodeName, domain: &DomainName) {
        self.release_command_pause(&CommandPausePoint::TransactionCleanup {
            node: node.clone(),
            domain: domain.clone(),
        });
    }

    pub(crate) async fn pause_transaction_cleanup_if_armed(
        &self,
        node: &ClusterNodeName,
        domain: &DomainName,
    ) {
        self.pause_command_if_armed(CommandPausePoint::TransactionCleanup {
            node: node.clone(),
            domain: domain.clone(),
        })
        .await;
    }
}
