//! The branch membership one owner task publishes for the observers of its branches.
//!
//! Layer: data plane.
//! - **Owns.** Keeping an owner's published membership equal to its live branch instances,
//!   publishing it as one immutable value whenever it changes, and the owner lifetimes that stop a
//!   replaced owner from publishing over its successor.
//! - **Depends on.** The branch instance registry, vocabulary timestamps, the primitive publication
//!   boundary, and persistent hash sets.
//! - **Must not know.** What a branch instance holds, who observes the membership, relays,
//!   payloads, or NSPL.

use std::{
    fmt,
    hash::Hash,
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use imbl::{GenericHashSet, shared_ptr::DefaultSharedPtr};
use meticulous::OptionExt as _;
use nervix_models::Timestamp;
use nervix_primitives::{
    publication::ArcSwap,
    sync::{Arc, StdArc},
};

use crate::{BranchInstanceRegistry, GetOrCreateBranchInstance};

/// The concrete branches of one membership. A persistent set shares its structure with the
/// membership it was derived from, so admitting or releasing one branch copies only the path to
/// that branch rather than every branch the owner holds.
type BranchSet<K> = GenericHashSet<K, ahash::RandomState, DefaultSharedPtr>;

/// One owner lifetime of a presence. Claiming a presence starts the next lifetime, and only the
/// owner of the current lifetime changes what the presence publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OwnerLifetime(NonZeroU64);

impl OwnerLifetime {
    /// The lifetime a claim starts after `previous`, the lifetime the presence published last.
    fn after(previous: Option<Self>) -> Self {
        let Some(previous) = previous else {
            return Self(NonZeroU64::MIN);
        };
        Self(
            previous
                .0
                .checked_add(1)
                .assured("an owner task starts once per claim, and no presence sees 2^64 of them"),
        )
    }
}

/// A complete branch membership, as the owner of one lifetime published it.
#[derive(Clone)]
pub struct BranchMembership<K> {
    /// The owner lifetime that published this membership; absent before any owner claimed it.
    owner: Option<OwnerLifetime>,
    /// Whether the owner admitted unbranched work, which has no concrete branch.
    unbranched: bool,
    branches: BranchSet<K>,
}

/// A membership reports its size, never its branch keys, whose fields may carry sensitive values.
impl<K> fmt::Debug for BranchMembership<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BranchMembership")
            .field("owner", &self.owner)
            .field("unbranched", &self.unbranched)
            .field("branches", &self.branches.len())
            .finish()
    }
}

impl<K> BranchMembership<K>
where
    K: Clone + Eq + Hash,
{
    fn empty(owner: Option<OwnerLifetime>) -> Self {
        Self {
            owner,
            unbranched: false,
            branches: BranchSet::with_hasher(ahash::RandomState::new()),
        }
    }

    fn is_empty(&self) -> bool {
        !self.unbranched && self.branches.is_empty()
    }

    /// Whether the owner holds the branch `key` names. An absent key names unbranched work.
    pub fn contains(&self, key: Option<&K>) -> bool {
        match key {
            None => self.unbranched,
            Some(key) => self.branches.contains(key),
        }
    }

    /// The concrete branches the owner holds, in no particular order.
    pub fn branches(&self) -> impl Iterator<Item = &K> {
        self.branches.iter()
    }

    /// How many concrete branches the owner holds.
    pub fn branch_count(&self) -> usize {
        self.branches.len()
    }
}

/// The published branch membership of one set of owned branches.
///
/// Observers read it without a lock. Only the owner that claimed it last changes it, and it does
/// so by replacing the whole membership, so an observer sees either the preceding membership or
/// its complete replacement.
#[derive(Debug)]
pub struct BranchPresence<K> {
    published: ArcSwap<BranchMembership<K>>,
}

impl<K> BranchPresence<K>
where
    K: Clone + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            published: ArcSwap::from_pointee(BranchMembership::empty(None)),
        }
    }

    /// The membership the current owner published last.
    pub fn load(&self) -> StdArc<BranchMembership<K>> {
        self.published.load_full()
    }

    /// Whether the current owner holds the branch `key` names. An absent key names unbranched
    /// work.
    pub fn contains(&self, key: Option<&K>) -> bool {
        self.published.load().contains(key)
    }

    /// Start the next owner lifetime by publishing its empty membership, which replaces whatever
    /// an earlier owner left behind.
    fn claim(&self) -> OwnerLifetime {
        let previous = self
            .published
            .rcu(|current| BranchMembership::<K>::empty(Some(OwnerLifetime::after(current.owner))));
        OwnerLifetime::after(previous.owner)
    }

    /// Publish `membership` in place of the current one, unless a later owner has claimed the
    /// presence since `membership`'s owner did.
    ///
    /// Between a claim and the next one only the claiming owner publishes, so a replacement that
    /// finds the published value changed under it finds a successor's claim and leaves it.
    fn publish(&self, membership: &BranchMembership<K>) {
        let next = StdArc::new(membership.clone());
        loop {
            let current = self.published.load();
            if current.owner != membership.owner {
                return;
            }
            let previous = self
                .published
                .compare_and_swap(&*current, StdArc::clone(&next));
            if StdArc::ptr_eq(&*previous, &*current) {
                return;
            }
        }
    }
}

impl<K> Default for BranchPresence<K>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

/// The branch instances one owner task holds, and the membership it publishes for them.
///
/// Every change to the instances goes through this owner, which updates its membership in the same
/// step and publishes it once for each step that changed it. A batch for a branch the owner already
/// holds refreshes that branch's activity and publishes nothing. Dropping the owner releases the
/// presence: its last publication is empty, unless a successor has claimed the presence.
pub struct OwnedBranches<K, V>
where
    K: Clone + Eq + Hash,
{
    presence: Arc<BranchPresence<K>>,
    instances: BranchInstanceRegistry<K, V>,
    membership: BranchMembership<K>,
}

/// What admitting one batch did to an owner's branches.
pub struct BranchAdmission<K, V> {
    /// The state of the batch's concrete branch. Unbranched work has none.
    pub branch: Option<Arc<V>>,
    /// The branches released to stay within capacity, least recently used first.
    pub evicted: Vec<(K, Arc<V>)>,
}

impl<K, V> OwnedBranches<K, V>
where
    K: Clone + Eq + Hash,
{
    /// Become the owner of `presence`. Its membership starts empty, replacing whatever an earlier
    /// owner left behind, and from here on only this owner changes it.
    pub fn claim(presence: Arc<BranchPresence<K>>) -> Self {
        let owner = presence.claim();
        Self {
            presence,
            instances: BranchInstanceRegistry::new(),
            membership: BranchMembership::empty(Some(owner)),
        }
    }

    /// Admit one batch for the branch `key` names at `now`, creating the branch with `create` when
    /// this owner does not hold it, then release the least recently used branches beyond
    /// `capacity`. The membership is published once if the batch changed it.
    pub fn admit<E>(
        &mut self,
        key: Option<&K>,
        now: Timestamp,
        capacity: Option<NonZeroUsize>,
        create: impl FnOnce(&K, u64) -> Result<V, E>,
    ) -> Result<BranchAdmission<K, V>, E> {
        let mut changed = false;
        let mut branch = None;
        if let Some(key) = key {
            let admitted = self.admit_branch(key, now, create)?;
            changed |= admitted.created;
            branch = Some(admitted.state);
        } else {
            changed |= self.admit_unbranched();
        }
        let evicted = self.release_beyond(capacity);
        changed |= !evicted.is_empty();
        if changed {
            self.presence.publish(&self.membership);
        }
        Ok(BranchAdmission { branch, evicted })
    }

    /// Release every branch idle for at least `max_idle` at `now`, oldest first. The membership is
    /// published once if any branch expired.
    pub fn expire(&mut self, now: Timestamp, max_idle: Duration) -> Vec<(K, Arc<V>)> {
        let expired = self.instances.expire(now, max_idle);
        if expired.is_empty() {
            return expired;
        }
        for (key, _) in &expired {
            self.membership.branches.remove(key);
        }
        self.presence.publish(&self.membership);
        expired
    }

    /// Refresh the branch `key` names, or create it when this owner does not hold it. Only a
    /// created branch changes the membership.
    fn admit_branch<E>(
        &mut self,
        key: &K,
        now: Timestamp,
        create: impl FnOnce(&K, u64) -> Result<V, E>,
    ) -> Result<GetOrCreateBranchInstance<V>, E> {
        if let Some(state) = self.instances.touch(key, now) {
            return Ok(GetOrCreateBranchInstance {
                state,
                created: false,
            });
        }
        let created = self
            .instances
            .get_or_try_create_with(key.clone(), now, create)?;
        self.membership.branches.insert(key.clone());
        Ok(created)
    }

    /// Record that this owner holds unbranched work, and whether that is new.
    fn admit_unbranched(&mut self) -> bool {
        if self.membership.unbranched {
            return false;
        }
        self.membership.unbranched = true;
        true
    }

    /// Release the least recently used branches beyond `capacity`.
    fn release_beyond(&mut self, capacity: Option<NonZeroUsize>) -> Vec<(K, Arc<V>)> {
        let Some(capacity) = capacity else {
            return Vec::new();
        };
        let evicted = self.instances.evict_lru_to_capacity(capacity);
        for (key, _) in &evicted {
            self.membership.branches.remove(key);
        }
        evicted
    }
}

impl<K, V> Drop for OwnedBranches<K, V>
where
    K: Clone + Eq + Hash,
{
    fn drop(&mut self) {
        if self.membership.is_empty() {
            return;
        }
        let released = BranchMembership::empty(self.membership.owner);
        self.presence.publish(&released);
    }
}

#[cfg(test)]
#[path = "membership_tests.rs"]
mod tests;
