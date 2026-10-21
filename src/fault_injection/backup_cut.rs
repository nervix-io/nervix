//! Controlled backup cut boundaries for public backup scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Domain-scoped barriers a backup cut reaches once it holds its lease and before its
//!   owners capture state.
//! - **Depends on.** The shared one-shot pause owner and vocabulary identities.
//! - **Must not know.** Archive contents, the nodes a cut must reach or backup recovery policy.

use nervix_models::DomainName;

use super::{CommandPausePoint, FaultInjection};

/// Where a backup cut of a domain waits for a scenario to release it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackupCutBoundary {
    /// Once a quiesced cut holds its domain's lease, before it checks the nodes it must reach and
    /// before it pauses the domain.
    Lease,
    /// Before any owner captures state: a quiesced cut after its drain, and a stopped cut once it
    /// holds its lease.
    Capture,
}

impl FaultInjection {
    /// Holds the next cut of `domain` that reaches `boundary`, once.
    pub fn pause_backup_cut_on(&self, domain: DomainName, boundary: BackupCutBoundary) {
        self.arm_command_pause(CommandPausePoint::BackupCut { domain, boundary });
    }

    pub async fn wait_for_backup_cut_pause(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
    ) {
        self.wait_for_command_pause(&CommandPausePoint::BackupCut {
            domain: domain.clone(),
            boundary,
        })
        .await;
    }

    pub fn release_backup_cut_pause(&self, domain: &DomainName, boundary: BackupCutBoundary) {
        self.release_command_pause(&CommandPausePoint::BackupCut {
            domain: domain.clone(),
            boundary,
        });
    }

    pub(crate) async fn pause_backup_cut_if_armed(
        &self,
        domain: &DomainName,
        boundary: BackupCutBoundary,
    ) {
        self.pause_command_if_armed(CommandPausePoint::BackupCut {
            domain: domain.clone(),
            boundary,
        })
        .await;
    }
}
