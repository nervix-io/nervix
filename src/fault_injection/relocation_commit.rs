//! A leader's refusal of a relocation's schedule commit, for public relocation scenarios.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The domain-scoped, one-shot refusal of the next relocation commit and the record of
//!   whether it was delivered.
//! - **Depends on.** Vocabulary identities.
//! - **Must not know.** Relocation planning, ownership handoffs, consensus or command recovery.

use nervix_models::DomainName;

use super::FaultInjection;

/// Whether the refusal armed for a domain's next relocation commit has reached that commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelocationCommitRefusal {
    Armed,
    Delivered,
}

impl FaultInjection {
    /// Makes the leader refuse the next relocation commit in `domain` the way a leader whose lease
    /// lapsed refuses a write: before appending it, and without naming a leader.
    pub fn refuse_next_relocation_commit(&self, domain: DomainName) {
        self.inner
            .refused_relocation_commits
            .insert(domain, RelocationCommitRefusal::Armed);
    }

    /// Whether the refusal armed for `domain`'s next relocation commit was delivered.
    pub fn relocation_commit_refusal_delivered(&self, domain: &DomainName) -> bool {
        let Some(refusal) = self.inner.refused_relocation_commits.get(domain) else {
            return false;
        };
        *refusal == RelocationCommitRefusal::Delivered
    }

    /// Delivers the refusal armed for `domain`'s next relocation commit once, and records that it
    /// was delivered.
    pub(crate) fn take_relocation_commit_refusal(&self, domain: &DomainName) -> bool {
        let Some(mut refusal) = self.inner.refused_relocation_commits.get_mut(domain) else {
            return false;
        };
        if *refusal != RelocationCommitRefusal::Armed {
            return false;
        }
        *refusal = RelocationCommitRefusal::Delivered;
        true
    }
}
