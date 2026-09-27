//! Concrete branch lifetime registry and least-recently-used eviction order.
//!
//! Layer: data plane.
//! - **Owns.** Branch instance identity, incarnation assignment, activity order, and eviction.
//! - **Depends on.** Typed branch keys, domain timestamps, and branch-owned runtime handles.
//! - **Must not know.** NSPL parsing, control-plane transactions, connector protocols, or
//!   persisted payloads.

use std::{hash::Hash, num::NonZeroUsize, time::Duration};

use indexmap::IndexMap;
use meticulous::OptionExt as _;
use nervix_models::Timestamp;
use triomphe::Arc;

pub(super) struct BranchInstanceRegistry<K, V>
where
    K: Clone + Eq + Hash,
{
    entries: IndexMap<K, BranchInstanceEntry<V>, ahash::RandomState>,
    version: u64,
}

struct BranchInstanceEntry<V> {
    last_ingestion: Timestamp,
    incarnation: u64,
    state: Arc<V>,
}

/// The branch lifetime recorded by the lifecycle checkpoint, independent of later LRU touches.
#[derive(Debug, Clone)]
pub(super) struct BranchInstanceSnapshotEntry<K> {
    pub(super) key: K,
    pub(super) last_ingestion: Timestamp,
    pub(super) incarnation: u64,
}

pub(super) struct GetOrCreateBranchInstance<V> {
    pub(super) state: Arc<V>,
    pub(super) created: bool,
}

impl<K, V> BranchInstanceRegistry<K, V>
where
    K: Clone + Eq + Hash,
{
    pub(super) fn new() -> Self {
        Self {
            entries: IndexMap::default(),
            version: 0,
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn states(&self) -> Vec<Arc<V>> {
        self.entries
            .values()
            .map(|entry| entry.state.clone())
            .collect()
    }

    pub(super) fn version(&self) -> u64 {
        self.version
    }

    pub(super) fn next_incarnation(&self) -> u64 {
        self.next_version()
    }

    pub(super) fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub(super) fn snapshot_entries(&self) -> Vec<BranchInstanceSnapshotEntry<K>> {
        self.entries
            .iter()
            .map(|(key, entry)| BranchInstanceSnapshotEntry {
                key: key.clone(),
                last_ingestion: entry.last_ingestion,
                incarnation: entry.incarnation,
            })
            .collect()
    }

    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    pub(super) fn insert_restored(
        &mut self,
        key: K,
        last_ingestion: Timestamp,
        incarnation: u64,
        state: V,
    ) -> Arc<V> {
        let state = Arc::new(state);
        self.entries.insert(
            key,
            BranchInstanceEntry {
                last_ingestion,
                incarnation,
                state: state.clone(),
            },
        );
        state
    }

    /// Insert a branch whose lifecycle changed while it was absent from this registry.
    ///
    /// Unlike restore from a checkpoint or an ownership handoff, this advances the lifecycle
    /// snapshot revision. A replica may already hold the preceding revision, so reusing it for a
    /// different branch set would let the replica acknowledge the preceding payload as current.
    pub(super) fn insert_changed(&mut self, key: K, last_ingestion: Timestamp, state: V) -> Arc<V> {
        let incarnation = self.next_version();
        let state = self.insert_restored(key, last_ingestion, incarnation, state);
        self.bump_version();
        state
    }

    #[cfg(test)]
    pub(super) fn get_or_create_with(
        &mut self,
        key: K,
        now: Timestamp,
        create: impl FnOnce(&K) -> V,
    ) -> GetOrCreateBranchInstance<V> {
        match self.get_or_try_create_with(key, now, |key, _| Ok::<_, ()>(create(key))) {
            Ok(result) => result,
            Err(()) => unreachable!("infallible branch_instance constructor cannot fail"),
        }
    }

    pub(super) fn get_or_try_create_with<E>(
        &mut self,
        key: K,
        now: Timestamp,
        create: impl FnOnce(&K, u64) -> Result<V, E>,
    ) -> Result<GetOrCreateBranchInstance<V>, E> {
        if let Some(state) = self.touch(&key, now) {
            return Ok(GetOrCreateBranchInstance {
                state,
                created: false,
            });
        }

        let incarnation = self.next_version();
        let state = Arc::new(create(&key, incarnation)?);
        self.entries.insert(
            key,
            BranchInstanceEntry {
                last_ingestion: now,
                incarnation,
                state: state.clone(),
            },
        );
        self.bump_version();
        Ok(GetOrCreateBranchInstance {
            state,
            created: true,
        })
    }

    /// Refresh one existing branch without constructing it. A caller that opens persisted state
    /// asynchronously can perform that work only when this returns `None`, then insert the branch.
    pub(super) fn touch(&mut self, key: &K, now: Timestamp) -> Option<Arc<V>> {
        let index = self.entries.get_index_of(key)?;
        let state = {
            let entry = self
                .entries
                .get_index_mut(index)
                .verified("the index was just returned by get_index_of on this same map")
                .1;
            entry.last_ingestion = now;
            entry.state.clone()
        };
        self.bump_version();
        let last_index = self
            .entries
            .len()
            .checked_sub(1)
            .verified("the entry looked up above is still in the map");
        if index != last_index {
            self.entries.move_index(index, last_index);
        }
        Some(state)
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let entry = self.entries.shift_remove(key);
        if entry.is_some() {
            self.bump_version();
        }
        entry.map(|entry| entry.state)
    }

    pub(super) fn expire(&mut self, now: Timestamp, max_idle: Duration) -> Vec<(K, Arc<V>)> {
        let mut expired = Vec::new();
        while let Some((key, entry)) = self.entries.get_index(0) {
            let Ok(idle) = now
                .into_datetime()
                .signed_duration_since(entry.last_ingestion.into_datetime())
                .to_std()
            else {
                break;
            };
            if idle < max_idle {
                break;
            }
            let key = key.clone();
            let (_, entry) = self
                .entries
                .shift_remove_index(0)
                .verified("the loop above observed a front entry in this same map");
            expired.push((key, entry.state));
        }
        if !expired.is_empty() {
            self.bump_version();
        }
        expired
    }

    pub(super) fn evict_lru_to_capacity(&mut self, max_entries: NonZeroUsize) -> Vec<(K, Arc<V>)> {
        let mut evicted = Vec::new();
        while self.entries.len() > max_entries.get() {
            let (key, entry) = self
                .entries
                .shift_remove_index(0)
                .verified("the loop above observed a front entry in this same map");
            evicted.push((key, entry.state));
        }
        if !evicted.is_empty() {
            self.bump_version();
        }
        evicted
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        if !self.entries.is_empty() {
            self.bump_version();
        }
        self.entries.clear();
    }

    pub(super) fn drain(&mut self) -> Vec<(K, Arc<V>)> {
        let entries = std::mem::take(&mut self.entries);
        if !entries.is_empty() {
            self.bump_version();
        }
        entries
            .into_iter()
            .map(|(key, entry)| (key, entry.state))
            .collect()
    }

    fn next_version(&self) -> u64 {
        self.version
            .checked_add(1)
            .assured("a registry cannot record 2^64 branch instance changes")
    }

    fn bump_version(&mut self) {
        self.version = self.next_version();
    }
}

impl<K, V> Default for BranchInstanceRegistry<K, V>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use meticulous::OptionExt as _;
    use nervix_models::Timestamp;
    use triomphe::Arc;

    use super::BranchInstanceRegistry;

    #[derive(Debug)]
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl<K, V> BranchInstanceRegistry<K, V>
    where
        K: Clone + Eq + std::hash::Hash,
    {
        fn ordered_keys(&self) -> Vec<K> {
            self.entries.keys().cloned().collect()
        }
    }

    fn timestamp(seconds: i64, nanos: u32) -> Timestamp {
        Timestamp::from_unix_nanos(
            seconds
                .checked_mul(1_000_000_000)
                .and_then(|nanos_part| nanos_part.checked_add(i64::from(nanos)))
                .assured("the test timestamps are small second counts"),
        )
    }

    #[test]
    fn reusing_branch_instance_promotes_it_to_the_back() {
        let mut registry = BranchInstanceRegistry::<String, usize>::new();
        let now = timestamp(1, 0);

        registry.get_or_create_with("acme".to_string(), now, |_| 1);
        registry.get_or_create_with("globex".to_string(), now, |_| 2);
        let result = registry.get_or_create_with("acme".to_string(), now, |_| 3);

        assert!(!result.created);
        assert_eq!(*result.state, 1);
        assert_eq!(
            registry.ordered_keys(),
            vec!["globex".to_string(), "acme".to_string()]
        );
    }

    #[test]
    fn recreated_key_gets_a_new_incarnation_while_a_touch_keeps_its_lifetime() {
        let mut registry = BranchInstanceRegistry::<String, usize>::new();
        registry.get_or_create_with("acme".to_string(), timestamp(1, 0), |_| 1);
        let first = registry.snapshot_entries()[0].incarnation;
        registry.get_or_create_with("acme".to_string(), timestamp(2, 0), |_| 2);
        assert_eq!(registry.snapshot_entries()[0].incarnation, first);
        registry.remove(&"acme".to_string());
        registry.get_or_create_with("acme".to_string(), timestamp(3, 0), |_| 3);
        assert!(registry.snapshot_entries()[0].incarnation > first);
    }

    #[test]
    fn expire_removes_the_oldest_idle_branch_instances() {
        let base = timestamp(31, 0);
        let mut registry = BranchInstanceRegistry::<String, usize>::new();

        registry.get_or_create_with("acme".to_string(), timestamp(0, 0), |_| 1);
        registry.get_or_create_with("globex".to_string(), timestamp(26, 0), |_| 2);
        registry.get_or_create_with("initech".to_string(), base, |_| 3);

        let expired = registry.expire(base, Duration::from_secs(30));

        assert_eq!(
            expired
                .into_iter()
                .map(|(key, state)| (key, *state))
                .collect::<Vec<_>>(),
            vec![("acme".to_string(), 1)]
        );
        assert_eq!(
            registry.ordered_keys(),
            vec!["globex".to_string(), "initech".to_string()]
        );
    }

    #[test]
    fn evict_lru_to_capacity_removes_front_entries() {
        let mut registry = BranchInstanceRegistry::<String, usize>::new();

        registry.get_or_create_with("acme".to_string(), timestamp(1, 0), |_| 1);
        registry.get_or_create_with("globex".to_string(), timestamp(2, 0), |_| 2);
        registry.get_or_create_with("initech".to_string(), timestamp(3, 0), |_| 3);

        let evicted = registry.evict_lru_to_capacity(NonZeroUsize::MIN);

        assert_eq!(
            evicted
                .into_iter()
                .map(|(key, state)| (key, *state))
                .collect::<Vec<_>>(),
            vec![("acme".to_string(), 1), ("globex".to_string(), 2)]
        );
        assert_eq!(registry.ordered_keys(), vec!["initech".to_string()]);
    }

    #[test]
    fn evict_lru_to_capacity_keeps_recently_touched_entries() {
        let mut registry = BranchInstanceRegistry::<String, usize>::new();

        registry.get_or_create_with("acme".to_string(), timestamp(1, 0), |_| 1);
        registry.get_or_create_with("globex".to_string(), timestamp(2, 0), |_| 2);
        registry.get_or_create_with("acme".to_string(), timestamp(3, 0), |_| 1);

        let evicted = registry.evict_lru_to_capacity(NonZeroUsize::MIN);

        assert_eq!(
            evicted
                .into_iter()
                .map(|(key, state)| (key, *state))
                .collect::<Vec<_>>(),
            vec![("globex".to_string(), 2)]
        );
        assert_eq!(registry.ordered_keys(), vec!["acme".to_string()]);
    }

    #[test]
    fn clear_and_drop_release_state_once() {
        let drops = Arc::new(AtomicUsize::new(0));

        {
            let mut registry = BranchInstanceRegistry::<String, DropCounter>::new();
            registry.get_or_create_with("acme".to_string(), timestamp(1, 0), |_| {
                DropCounter(drops.clone())
            });
            registry.get_or_create_with("globex".to_string(), timestamp(2, 0), |_| {
                DropCounter(drops.clone())
            });

            registry.clear();
            assert_eq!(drops.load(Ordering::Relaxed), 2);

            registry.get_or_create_with("initech".to_string(), timestamp(3, 0), |_| {
                DropCounter(drops.clone())
            });
        }

        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }
}
