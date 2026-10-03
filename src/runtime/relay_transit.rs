//! The batches a relay holds on this node between admitting them and handing them on.
//!
//! Layer: data plane.
//!
//! - **Owns.** The admitted and handed-on totals of one relay on this node, and the guards that move
//!   a batch through them.
//! - **Depends on.** The primitive boundary's atomics and shared ownership.
//! - **Must not know.** Relay channels, consumers, branches, routing, or why a drain reads them.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "every batch a relay admits on this node moves through its retained transit"
    )
)]

use super::*;

/// The batches one relay admitted on this node, and the batches it handed on.
///
/// Both totals only rise, and the relay holds their difference: a batch counts from the moment its
/// publisher admits it until the relay's owner has handed it to its consumers here and elsewhere,
/// or refused it. Neither total can wrap: a relay admitting a billion batches a second takes more
/// than five centuries to reach `u64::MAX`.
#[derive(Debug, Default)]
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "every batch a relay admits on this node moves through its retained transit"
    )
)]
pub(super) struct RelayTransit {
    admitted: AtomicU64,
    handed_on: AtomicU64,
}

impl RelayTransit {
    fn admit(&self) {
        self.admitted.fetch_add(1, Ordering::Release);
    }

    /// Releases what the relay did with the batch before handing it on, such as admitting it to a
    /// consumer, to the drain that reads the handed-on total.
    fn hand_on(&self) {
        self.handed_on.fetch_add(1, Ordering::Release);
    }

    /// The batches the relay holds now.
    ///
    /// The handed-on total is read first. A batch the relay handed on after that read is still
    /// counted, and one it handed on before has already reached what the relay hands it to.
    pub(super) fn held(&self) -> usize {
        let handed_on = self.handed_on.load(Ordering::Acquire);
        let admitted = self.admitted.load(Ordering::Acquire);
        let held = admitted.checked_sub(handed_on).verified(
            "every batch is admitted before it is handed on, and the handed-on total is read \
             first, so the admitted total read after it covers every batch it counts",
        );
        held.arch_into()
    }
}

/// A batch a publisher admitted to the relay's owner buffer. Unless the owner buffer accepted it,
/// dropping the admission hands the refused batch on.
pub(super) struct RelayOwnerAdmission {
    transit: Arc<RelayTransit>,
    accepted: bool,
}

impl RelayOwnerAdmission {
    pub(super) fn new(transit: Arc<RelayTransit>) -> Self {
        transit.admit();
        Self {
            transit,
            accepted: false,
        }
    }

    /// The owner buffer holds the batch, and the owner task hands it on once it has fanned it out.
    pub(super) fn accept(mut self) {
        self.accepted = true;
    }
}

impl Drop for RelayOwnerAdmission {
    fn drop(&mut self) {
        if !self.accepted {
            self.transit.hand_on();
        }
    }
}

/// A batch the relay's owner task took from its owner buffer, handed on once it is dropped.
pub(super) struct RelayOwnerBatchCompletion {
    transit: Arc<RelayTransit>,
}

impl RelayOwnerBatchCompletion {
    pub(super) fn new(transit: Arc<RelayTransit>) -> Self {
        Self { transit }
    }
}

impl Drop for RelayOwnerBatchCompletion {
    fn drop(&mut self) {
        self.transit.hand_on();
    }
}
