//! The newest checkpoint revision of every branch state one node owns for one branch-keyed entity.
//!
//! Layer: data plane.
//! - **Owns.** Each owned branch state's newest replicable revision, the order in which those
//!   revisions changed and branch states went away, the cursor a replica resumes from, and the
//!   registrations that keep a replaced branch state from changing its successor's entry.
//! - **Depends on.** Typed branch keys, runtime-state identities, persistent maps and the primitive
//!   publication boundary.
//! - **Must not know.** What a checkpoint holds, how it is persisted, announced, fetched or
//!   installed, schedules, the interconnect, or NSPL.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc as StdArc,
};

use imbl::{GenericHashMap, GenericOrdMap, shared_ptr::DefaultSharedPtr};
use meticulous::OptionExt as _;
use nervix_interconnect::{BranchCheckpointCursor, RuntimeState};
use nervix_primitives::publication::ArcSwap;
use triomphe::Arc;

use super::BranchKey;

/// How many removals a catalog keeps for the replicas that have not learned of them yet. A replica
/// whose cursor is older than the oldest removal the catalog kept is sent the complete catalog.
const RETAINED_REMOVALS: usize = 1_024;

/// The entries of one catalog, by branch. Unbranched work is the absent key. A persistent map shares
/// its structure with the catalog it was derived from, so a change copies only the path to the
/// branch it changes.
type Entries =
    GenericHashMap<Option<BranchKey>, CatalogEntry, ahash::RandomState, DefaultSharedPtr>;

/// Branches by the sequence of the change that concerns them.
type ChangeOrder = GenericOrdMap<u64, Option<BranchKey>, DefaultSharedPtr>;

/// The newest checkpoint revision of every branch state this node owns for one branch-keyed
/// entity, so a replica learns which of those checkpoints it lags on with one read, however many
/// branches the entity has.
///
/// Each branch state registers when it is created, records every revision it publishes for its
/// replicas, and removes its entry when it goes away. A replica reads what changed after the cursor
/// the previous read returned, and a read that finds nothing changed costs no work per branch.
#[derive(Debug, Clone)]
pub(super) struct BranchCheckpointCatalog {
    /// Replaced whole, so a reader keeps the catalog it loaded without holding anything, and a
    /// change never waits for a reader.
    published: Arc<ArcSwap<CatalogState>>,
}

/// One branch state's entry in the catalog of the entity it belongs to.
///
/// The branch state holds it for as long as it lives. Dropping it removes the entry, unless a
/// newer branch state of the same branch registered meanwhile.
#[derive(Debug)]
pub(super) struct CatalogRegistration {
    catalog: BranchCheckpointCatalog,
    branch: Option<BranchKey>,
    registration: NonZeroU64,
}

/// What a replica learns of an entity's branch checkpoints from one catalog read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CheckpointListing {
    /// The replica's cursor was absent, belonged to another catalog, or was older than the oldest
    /// removal the catalog kept, so the replica forgets what it knew and learns the catalog from
    /// its beginning, starting with this page. A page of a restarted listing lists no removals:
    /// the replica already forgot every branch.
    Restarted(CheckpointPage),
    /// The next changes after the replica's cursor.
    Continued(CheckpointPage),
}

/// Changes of one catalog in the order they happened, at most one listing page of them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CheckpointPage {
    /// Where the replica stands once it applied this page.
    pub(super) cursor: BranchCheckpointCursor,
    /// The branches whose state went away.
    pub(super) removed: Vec<Option<BranchKey>>,
    /// The branch checkpoints that changed, each at its newest revision. A branch whose state went
    /// away and was registered again appears in both lists, and its checkpoint is the newer fact.
    pub(super) revised: Vec<CatalogedCheckpoint>,
    /// Whether further changes follow the cursor.
    pub(super) more: bool,
}

/// The newest checkpoint of one branch state, as its owner catalogs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CatalogedCheckpoint {
    pub(super) branch: Option<BranchKey>,
    /// The state the checkpoint belongs to, which names the lifetime of guest state for a WASM
    /// processor branch.
    pub(super) state: RuntimeState,
    pub(super) lsm: u64,
}

/// Everything one catalog holds at one moment.
#[derive(Debug, Clone)]
struct CatalogState {
    /// Chosen when the catalog is created, so a cursor another catalog returned is never taken
    /// for one of this catalog's.
    epoch: u64,
    /// How many changes the catalog recorded; each change is numbered by the count it raised.
    sequence: u64,
    /// The newest removal the catalog discarded, or zero before it discarded one. A cursor older
    /// than it may miss a removal, so it is answered with the complete catalog.
    discarded_through: u64,
    /// How many registrations the catalog handed out.
    registrations: u64,
    entries: Entries,
    /// Every present branch by the sequence of its latest change.
    revisions: ChangeOrder,
    /// The removals the catalog kept, by the sequence of each. At most [`RETAINED_REMOVALS`].
    removals: ChangeOrder,
}

#[derive(Debug, Clone)]
struct CatalogEntry {
    registration: NonZeroU64,
    state: RuntimeState,
    lsm: u64,
    changed_at: u64,
}

impl BranchCheckpointCatalog {
    /// An empty catalog with a fresh epoch.
    pub(super) fn new() -> Self {
        Self::with_epoch(fastrand::u64(..))
    }

    /// An empty catalog whose cursors carry `epoch`.
    pub(super) fn with_epoch(epoch: u64) -> Self {
        Self {
            published: Arc::new(ArcSwap::from_pointee(CatalogState {
                epoch,
                sequence: 0,
                discarded_through: 0,
                registrations: 0,
                entries: Entries::default(),
                revisions: ChangeOrder::default(),
                removals: ChangeOrder::default(),
            })),
        }
    }

    /// Catalog the branch state of `branch` that `state` names, which starts at revision `lsm`.
    ///
    /// The returned registration is the branch state's entry: it records the revisions the state
    /// publishes and removes the entry when it is dropped. A state registered earlier for the same
    /// branch no longer changes the entry.
    #[must_use = "dropping the registration removes the branch state from the catalog"]
    pub(super) fn register(
        &self,
        branch: Option<BranchKey>,
        state: RuntimeState,
        lsm: u64,
    ) -> CatalogRegistration {
        let previous = self.published.rcu(|current| {
            let mut next = CatalogState::clone(current);
            let registration = next.next_registration();
            let changed_at = next.next_sequence();
            let entry = CatalogEntry {
                registration,
                state,
                lsm,
                changed_at,
            };
            if let Some(replaced) = next.entries.insert(branch.clone(), entry) {
                next.revisions.remove(&replaced.changed_at);
            }
            next.revisions.insert(changed_at, branch.clone());
            next
        });
        let registrations = previous
            .registrations
            .checked_add(1)
            .assured("the catalog raised this count by one without overflowing just above");
        let registration = NonZeroU64::new(registrations)
            .assured("a count raised by one from zero or more is never zero");
        CatalogRegistration {
            catalog: self.clone(),
            branch,
            registration,
        }
    }

    /// The first changes after `cursor`, at most `page` of them, or the first `page` checkpoints
    /// of the catalog when the cursor cannot say what changed after it.
    pub(super) fn changes_after(
        &self,
        cursor: Option<BranchCheckpointCursor>,
        page: NonZeroUsize,
    ) -> CheckpointListing {
        let current = self.published.load_full();
        match current.incremental_from(cursor) {
            Some(after) => CheckpointListing::Continued(current.page_after(after, page)),
            None => CheckpointListing::Restarted(current.first_page(page)),
        }
    }

    /// Change the entry `registration` holds, when it still holds one, through `change`, which
    /// returns whether it changed anything.
    fn update_entry(
        &self,
        registration: &CatalogRegistration,
        mut change: impl FnMut(&mut CatalogState, CatalogEntry) -> bool,
    ) {
        self.published.rcu(|current| {
            let Some(entry) = current.entries.get(&registration.branch) else {
                return StdArc::clone(current);
            };
            if entry.registration != registration.registration {
                return StdArc::clone(current);
            }
            let entry = entry.clone();
            let mut next = CatalogState::clone(current);
            if !change(&mut next, entry) {
                return StdArc::clone(current);
            }
            StdArc::new(next)
        });
    }
}

impl Default for BranchCheckpointCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogRegistration {
    /// Record that the branch state published revision `lsm` for its replicas. A revision that is
    /// not newer than the one recorded last changes nothing.
    pub(super) fn record(&self, lsm: u64) {
        self.catalog.update_entry(self, |next, mut entry| {
            if entry.lsm >= lsm {
                return false;
            }
            next.revisions.remove(&entry.changed_at);
            let changed_at = next.next_sequence();
            entry.lsm = lsm;
            entry.changed_at = changed_at;
            next.entries.insert(self.branch.clone(), entry);
            next.revisions.insert(changed_at, self.branch.clone());
            true
        });
    }
}

impl Drop for CatalogRegistration {
    fn drop(&mut self) {
        let registration: &Self = self;
        registration
            .catalog
            .update_entry(registration, |next, entry| {
                next.entries.remove(&registration.branch);
                next.revisions.remove(&entry.changed_at);
                let removed_at = next.next_sequence();
                next.removals
                    .insert(removed_at, registration.branch.clone());
                next.discard_old_removals();
                true
            });
    }
}

impl CatalogState {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self
            .sequence
            .checked_add(1)
            .assured("a catalog records one change per branch publication, far fewer than 2^64");
        self.sequence
    }

    fn next_registration(&mut self) -> NonZeroU64 {
        self.registrations = self
            .registrations
            .checked_add(1)
            .assured("a catalog registers one branch state per branch start, far fewer than 2^64");
        NonZeroU64::new(self.registrations)
            .assured("a count just raised from zero or more is never zero")
    }

    /// Keep at most [`RETAINED_REMOVALS`] removals, discarding the oldest.
    fn discard_old_removals(&mut self) {
        while self.removals.len() > RETAINED_REMOVALS {
            let (oldest, remaining) = self.removals.without_min_with_key();
            let (removed_at, _) = oldest.verified("a map longer than its bound has a first entry");
            self.removals = remaining;
            self.discarded_through = removed_at;
        }
    }

    /// The sequence to list changes after, when `cursor` belongs to this catalog and nothing it
    /// has to learn was discarded.
    fn incremental_from(&self, cursor: Option<BranchCheckpointCursor>) -> Option<u64> {
        let cursor = cursor?;
        if cursor.epoch != self.epoch {
            return None;
        }
        if cursor.sequence < self.discarded_through || cursor.sequence > self.sequence {
            return None;
        }
        Some(cursor.sequence)
    }

    /// The first `page` changes after sequence `after`, removals and revisions in the order they
    /// happened.
    fn page_after(&self, after: u64, page: NonZeroUsize) -> CheckpointPage {
        let mut removals = self.removals.range((Excluded(after), Unbounded)).peekable();
        let mut revisions = self
            .revisions
            .range((Excluded(after), Unbounded))
            .peekable();
        let mut removed = Vec::new();
        let mut revised = Vec::new();
        let mut listed = 0_usize;
        let mut last = after;
        while listed < page.get() {
            let next_removal = removals.peek().map(|(sequence, _)| **sequence);
            let next_revision = revisions.peek().map(|(sequence, _)| **sequence);
            let removal_first = match (next_removal, next_revision) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some(removal), Some(revision)) => removal < revision,
            };
            if removal_first {
                let (sequence, branch) = removals
                    .next()
                    .verified("the peek above found this removal");
                removed.push(branch.clone());
                last = *sequence;
            } else {
                let (sequence, branch) = revisions
                    .next()
                    .verified("the peek above found this revision");
                revised.push(self.cataloged(branch));
                last = *sequence;
            }
            listed = listed
                .checked_add(1)
                .assured("a page lists fewer changes than its size, which is a usize");
        }
        let more = removals.peek().is_some() || revisions.peek().is_some();
        self.page(removed, revised, more, last)
    }

    /// The first `page` checkpoints of the catalog in the order they last changed.
    fn first_page(&self, page: NonZeroUsize) -> CheckpointPage {
        let mut revisions = self.revisions.iter().peekable();
        let mut revised = Vec::new();
        let mut last = 0;
        while revised.len() < page.get() {
            let Some((sequence, branch)) = revisions.next() else {
                break;
            };
            revised.push(self.cataloged(branch));
            last = *sequence;
        }
        let more = revisions.peek().is_some();
        self.page(Vec::new(), revised, more, last)
    }

    /// A page listing `removed` and `revised`, the changes up to sequence `last`, with the cursor
    /// that follows it. A page that leaves nothing after it moves the cursor to the latest change.
    fn page(
        &self,
        removed: Vec<Option<BranchKey>>,
        revised: Vec<CatalogedCheckpoint>,
        more: bool,
        last: u64,
    ) -> CheckpointPage {
        let sequence = if more { last } else { self.sequence };
        CheckpointPage {
            cursor: BranchCheckpointCursor {
                epoch: self.epoch,
                sequence,
            },
            removed,
            revised,
            more,
        }
    }

    fn cataloged(&self, branch: &Option<BranchKey>) -> CatalogedCheckpoint {
        let entry = self
            .entries
            .get(branch)
            .assured("every branch ordered by its latest change has an entry");
        CatalogedCheckpoint {
            branch: branch.clone(),
            state: entry.state,
            lsm: entry.lsm,
        }
    }
}

#[cfg(test)]
#[path = "branch_checkpoint_catalog_tests.rs"]
mod tests;
