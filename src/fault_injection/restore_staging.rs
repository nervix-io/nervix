//! Fault controls at the checkpoint staging boundary of restored domain state.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** Failures of restored checkpoint staging, scoped to one domain and one checkpoint
//!   kind, and their delivery observations. Durable publication failures and staging pauses never
//!   consume them.
//! - **Depends on.** Vocabulary identities and the shared test injector.
//! - **Must not know.** Restore steps, command recovery, runtime state kinds, consensus or the
//!   state store.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "owns test checkpoint staging controls and their observations"
    )
)]

use nervix_models::DomainName;

use super::FaultInjection;

/// The kind of restored checkpoint a staging failure meets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestoreStagingKind {
    /// A WASM processor's guest state.
    Guest,
    /// A materialized relay's state.
    Materialized,
    /// A deduplicator's keyspace or a window processor's state.
    BranchState,
}

/// The checkpoints of one kind that one restored domain stages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct RestoreStagingTarget {
    domain: DomainName,
    kind: RestoreStagingKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestoreStagingFailure {
    Armed,
    Delivered,
}

impl FaultInjection {
    /// Keeps every staging of `domain`'s restored checkpoints of `kind` failing, before the store
    /// stages it, until the failure is cleared. A restore that a new leader or a retry resumes
    /// under the same execution reference stages again and meets the same failure, so the failure
    /// stays the restore's outcome when recording it is interrupted. Each kind of one domain is
    /// armed, delivered and cleared on its own.
    pub fn fail_restore_checkpoint_stagings(&self, domain: DomainName, kind: RestoreStagingKind) {
        self.inner.failed_restore_checkpoint_stagings.insert(
            RestoreStagingTarget { domain, kind },
            RestoreStagingFailure::Armed,
        );
    }

    /// Ends the injected condition without changing a recorded restore outcome.
    pub fn clear_restore_checkpoint_staging_failure(
        &self,
        domain: &DomainName,
        kind: RestoreStagingKind,
    ) {
        let target = RestoreStagingTarget {
            domain: domain.clone(),
            kind,
        };
        self.inner
            .failed_restore_checkpoint_stagings
            .remove(&target);
    }

    /// Distinguishes an unarmed control from a pending or delivered failure.
    pub fn restore_checkpoint_staging_failure_delivered(
        &self,
        domain: &DomainName,
        kind: RestoreStagingKind,
    ) -> Option<bool> {
        let target = RestoreStagingTarget {
            domain: domain.clone(),
            kind,
        };
        self.inner
            .failed_restore_checkpoint_stagings
            .get(&target)
            .map(|state| *state == RestoreStagingFailure::Delivered)
    }

    /// Whether staging a restored checkpoint of `kind` for `domain` fails, recording that a
    /// staging met the failure.
    pub(crate) fn restore_checkpoint_staging_fails(
        &self,
        domain: &DomainName,
        kind: RestoreStagingKind,
    ) -> bool {
        let target = RestoreStagingTarget {
            domain: domain.clone(),
            kind,
        };
        let Some(mut state) = self
            .inner
            .failed_restore_checkpoint_stagings
            .get_mut(&target)
        else {
            return false;
        };
        *state = RestoreStagingFailure::Delivered;
        true
    }
}
