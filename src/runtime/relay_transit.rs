//! The batches a relay holds on this node between admitting them and handing them on.
//!
//! Layer: data plane.
//!
//! - **Owns.** The admitted and handed-on totals of one relay on this node, the guards that move a
//!   batch through them, and the admission sequence a drain compares across its reads.
//! - **Depends on.** The primitive boundary's atomics and shared ownership.
//! - **Must not know.** Relay channels, consumers, branches, routing, or why a drain reads.

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
/// publisher or a routed delivery admits it until the relay has handed it to its consumers here and
/// elsewhere, or refused it. Neither total can wrap: a relay admitting a billion batches a second
/// takes more than five centuries to reach `u64::MAX`.
///
/// The admitted total is also the relay's admission sequence on this node. A drain reads it before
/// and after everything else it reads, so a batch that entered the relay while the drain read
/// changes it, however its move between the relay and a node's counts interleaved with those reads.
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

    /// How many batches entered the relay on this node so far.
    pub(super) fn admissions(&self) -> u64 {
        self.admitted.load(Ordering::Acquire)
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

/// A batch another node routed to this relay's consumers here. The relay holds it until it is
/// dropped, once the batch reached those consumers or was refused.
pub(super) struct RelayRoutedAdmission<'a> {
    transit: &'a RelayTransit,
}

impl<'a> RelayRoutedAdmission<'a> {
    pub(super) fn new(transit: &'a RelayTransit) -> Self {
        transit.admit();
        Self { transit }
    }
}

impl Drop for RelayRoutedAdmission<'_> {
    fn drop(&mut self) {
        self.transit.hand_on();
    }
}

/// The admission sequence of each relay of one domain on this node, read in one pass.
#[derive(Debug, Default)]
pub(super) struct RelayAdmissions {
    relays: BTreeMap<DomainNodeRef, RelayAdmissionReading>,
}

#[derive(Debug)]
struct RelayAdmissionReading {
    /// Kept so that a relay replaced between two readings is told apart from its replacement,
    /// even when the replacement reached the same sequence.
    transit: Arc<RelayTransit>,
    admissions: u64,
}

impl RelayAdmissions {
    /// Records the admission sequence of `relay` as it reads now.
    pub(super) fn record(&mut self, relay: DomainNodeRef, transit: Arc<RelayTransit>) {
        let admissions = transit.admissions();
        self.relays.insert(
            relay,
            RelayAdmissionReading {
                transit,
                admissions,
            },
        );
    }

    /// How many relays admitted a batch between `earlier` and this reading. A relay only one of
    /// the readings saw, or one replaced in between, counts as a relay that admitted.
    pub(super) fn relays_admitting_since(&self, earlier: &Self) -> usize {
        let mut admitting = 0_usize;
        for (relay, reading) in &self.relays {
            let unchanged = match earlier.relays.get(relay) {
                Some(before) => {
                    Arc::ptr_eq(&before.transit, &reading.transit)
                        && before.admissions == reading.admissions
                }
                None => false,
            };
            if !unchanged {
                admitting = admitting
                    .checked_add(1)
                    .assured("the count is bounded by the relays this reading holds");
            }
        }
        for relay in earlier.relays.keys() {
            if !self.relays.contains_key(relay) {
                admitting = admitting
                    .checked_add(1)
                    .assured("the count is bounded by the relays both readings hold");
            }
        }
        admitting
    }
}
