use std::{hash::Hash, time::Duration};

use indexmap::IndexMap;
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
    state: Arc<V>,
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

    pub(super) fn set_version(&mut self, version: u64) {
        self.version = version;
    }

    pub(super) fn snapshot_entries(&self) -> Vec<(K, Timestamp)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.last_ingestion))
            .collect()
    }

    pub(super) fn insert_restored(
        &mut self,
        key: K,
        last_ingestion: Timestamp,
        state: V,
    ) -> Arc<V> {
        let state = Arc::new(state);
        self.entries.insert(
            key,
            BranchInstanceEntry {
                last_ingestion,
                state: state.clone(),
            },
        );
        state
    }

    #[cfg(test)]
    pub(super) fn get_or_create_with(
        &mut self,
        key: K,
        now: Timestamp,
        create: impl FnOnce(&K) -> V,
    ) -> GetOrCreateBranchInstance<V> {
        match self.get_or_try_create_with(key, now, |key| Ok::<_, ()>(create(key))) {
            Ok(result) => result,
            Err(()) => unreachable!("infallible branch_instance constructor cannot fail"),
        }
    }

    pub(super) fn get_or_try_create_with<E>(
        &mut self,
        key: K,
        now: Timestamp,
        create: impl FnOnce(&K) -> Result<V, E>,
    ) -> Result<GetOrCreateBranchInstance<V>, E> {
        if let Some(index) = self.entries.get_index_of(&key) {
            let state = {
                let entry = self
                    .entries
                    .get_index_mut(index)
                    .expect("index from get_index_of must be valid")
                    .1;
                entry.last_ingestion = now;
                entry.state.clone()
            };
            self.bump_version();
            let last_index = self.entries.len().saturating_sub(1);
            if index != last_index {
                self.entries.move_index(index, last_index);
            }
            return Ok(GetOrCreateBranchInstance {
                state,
                created: false,
            });
        }

        let state = Arc::new(create(&key)?);
        self.entries.insert(
            key,
            BranchInstanceEntry {
                last_ingestion: now,
                state: state.clone(),
            },
        );
        self.bump_version();
        Ok(GetOrCreateBranchInstance {
            state,
            created: true,
        })
    }

    pub(super) fn expire(&mut self, now: Timestamp, max_idle: Duration) -> Vec<(K, Arc<V>)> {
        let mut expired = Vec::new();
        loop {
            let Some((key, entry)) = self.entries.get_index(0) else {
                break;
            };
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
                .expect("front entry must be removable");
            expired.push((key, entry.state));
        }
        if !expired.is_empty() {
            self.bump_version();
        }
        expired
    }

    pub(super) fn evict_lru_to_capacity(&mut self, max_entries: usize) -> Vec<(K, Arc<V>)> {
        let mut evicted = Vec::new();
        while self.entries.len() > max_entries {
            let (key, entry) = self
                .entries
                .shift_remove_index(0)
                .expect("front entry must be removable while over capacity");
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

    fn bump_version(&mut self) {
        self.version = self.version.saturating_add(1);
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
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

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
        Timestamp::from_unix_nanos(seconds.saturating_mul(1_000_000_000) + i64::from(nanos))
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

        let evicted = registry.evict_lru_to_capacity(1);

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

        let evicted = registry.evict_lru_to_capacity(1);

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
