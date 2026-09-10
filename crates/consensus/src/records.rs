//! Immutable revisions of keyed consensus records.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Structural sharing, record mutation and incremental persistence of replicated values.
//! - **Depends on.** Vocabulary records, persistent ordered maps, and the durable batch owner.
//! - **Must not know.** Raft transport, graph execution or domain lifecycle policy.

use std::{borrow::Borrow, collections::BTreeMap, io};

use imbl::{OrdMap, ordmap::DiffItem};
use nervix_models::{
    ClusterSchedule, DomainName, DomainSchedule, ResourceId, ResourceName, ResourceNodeStatus,
    ResourceReplicaKey, ResourceVersion, ResourceVersionCounter, ResourceVersionStatus,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use triomphe::Arc;

use crate::durable_batch::DurableBatch;

/// Each leaf shares its record too: copying a tree path never copies unrelated record contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Records<K: Ord + Clone, V> {
    entries: OrdMap<K, Arc<V>>,
}

impl<K: Ord + Clone, V> Default for Records<K, V> {
    fn default() -> Self {
        Self {
            entries: OrdMap::new(),
        }
    }
}

impl<K: Ord + Clone, V: Clone> Records<K, V> {
    pub(crate) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.entries.get(key).map(Arc::as_ref)
    }

    pub(crate) fn get_mut<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
    {
        self.entries.get_mut(key).map(Arc::make_mut)
    }

    pub(crate) fn contains_key<Q: Ord + ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.entries.contains_key(key)
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key, Arc::new(value));
    }

    pub(crate) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q)
    where
        K: Borrow<Q>,
    {
        self.entries.remove(key);
    }

    pub(crate) fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let preceding = self.clone();
        for (key, value) in preceding.iter() {
            if !keep(key, value) {
                self.remove(key);
            }
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.entries
            .iter()
            .map(|(key, value)| (key, value.as_ref()))
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.entries.keys()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values().map(Arc::as_ref)
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

impl<K: Ord + Clone, V: Clone> FromIterator<(K, V)> for Records<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(values: T) -> Self {
        Self {
            entries: values
                .into_iter()
                .map(|(key, value)| (key, Arc::new(value)))
                .collect(),
        }
    }
}

impl<K: Ord + Clone, V: Clone> From<&Records<K, V>> for BTreeMap<K, V> {
    fn from(records: &Records<K, V>) -> Self {
        records
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl<K, V> Records<K, V>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Clone + PartialEq + Serialize + DeserializeOwned,
{
    pub(crate) fn write_changes(
        &self,
        preceding: &Self,
        tag: u8,
        batch: &mut DurableBatch,
        keyspace: &fjall::Keyspace,
    ) -> io::Result<()> {
        for change in preceding.entries.diff(&self.entries) {
            match change {
                DiffItem::Add(key, value)
                | DiffItem::Update {
                    new: (key, value), ..
                } => {
                    batch.insert(keyspace, &Self::key(tag, key)?, value.as_ref())?;
                }
                DiffItem::Remove(key, _) => batch.remove(keyspace, &Self::key(tag, key)?)?,
            }
        }
        Ok(())
    }

    pub(crate) fn load(tag: u8, keyspace: &fjall::Keyspace) -> io::Result<Self> {
        let mut records = Self::default();
        for item in keyspace.prefix([tag]) {
            let (key, value) = item.into_inner().map_err(io::Error::other)?;
            let key = storekey::deserialize(&key[1..]).map_err(io::Error::other)?;
            records.insert(key, crate::storage_decode(&value)?);
        }
        Ok(records)
    }

    fn key(tag: u8, key: &K) -> io::Result<Vec<u8>> {
        let mut encoded = vec![tag];
        encoded.extend(storekey::serialize(key).map_err(io::Error::other)?);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ScheduleRecords {
    pub(crate) domains: Records<DomainName, DomainSchedule>,
}

impl ScheduleRecords {
    pub(crate) fn domain(&self, domain: &DomainName) -> Option<&DomainSchedule> {
        self.domains.get(domain)
    }
}

impl From<ClusterSchedule> for ScheduleRecords {
    fn from(schedule: ClusterSchedule) -> Self {
        Self {
            domains: schedule.domains.into_iter().collect(),
        }
    }
}

impl From<&ScheduleRecords> for ClusterSchedule {
    fn from(schedule: &ScheduleRecords) -> Self {
        Self {
            domains: (&schedule.domains).into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct ResourceCatalogKey {
    domain: DomainName,
    identifier: ResourceName,
}

impl ResourceCatalogKey {
    fn new(domain: &DomainName, identifier: &ResourceName) -> Self {
        Self {
            domain: domain.clone(),
            identifier: identifier.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResourceRecords {
    pub(crate) counters: Records<ResourceCatalogKey, u64>,
    pub(crate) versions: Records<ResourceId, ResourceVersion>,
    pub(crate) replicas: Records<ResourceReplicaKey, ResourceNodeStatus>,
}

impl ResourceRecords {
    pub(crate) fn is_declared(&self, domain: &DomainName, identifier: &ResourceName) -> bool {
        self.counters
            .contains_key(&ResourceCatalogKey::new(domain, identifier))
    }

    pub(crate) fn ensure_catalog(&mut self, domain: &DomainName, identifier: &ResourceName) {
        let key = ResourceCatalogKey::new(domain, identifier);
        if !self.counters.contains_key(&key) {
            self.counters.insert(key, 1);
        }
    }

    pub(crate) fn advance_version(&mut self, domain: &DomainName, identifier: &ResourceName) {
        use meticulous::OptionExt as _;
        let key = ResourceCatalogKey::new(domain, identifier);
        let next = self
            .counters
            .get(&key)
            .copied()
            .unwrap_or(1)
            .checked_add(1)
            .assured("a resource cannot be replaced 2^64 times in the lifetime of a cluster");
        self.counters.insert(key, next);
    }
}

impl From<&ResourceRecords> for ResourceVersionStatus {
    fn from(resources: &ResourceRecords) -> Self {
        Self {
            next_version_by_resource: resources
                .counters
                .iter()
                .map(|(key, next_version)| ResourceVersionCounter {
                    domain: key.domain.clone(),
                    identifier: key.identifier.clone(),
                    next_version: *next_version,
                })
                .collect(),
            versions: resources.versions.values().cloned().collect(),
            replicas: resources.replicas.values().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct Tracked {
        clones: Arc<AtomicUsize>,
    }
    impl Clone for Tracked {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                clones: self.clones.clone(),
            }
        }
    }

    #[test]
    fn a_new_revision_copies_only_the_record_mutated() {
        use meticulous::OptionExt as _;
        let clones = Arc::new(AtomicUsize::new(0));
        let preceding: Records<usize, Tracked> = (0..10_000)
            .map(|key| {
                (
                    key,
                    Tracked {
                        clones: clones.clone(),
                    },
                )
            })
            .collect();
        let mut next = preceding.clone();
        assert!(preceding.entries.ptr_eq(&next.entries));
        let record = next
            .get_mut(&123)
            .verified("the fixture populated every key below 10000");
        assert!(Arc::ptr_eq(&record.clones, &clones));
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        assert!(!preceding.entries.ptr_eq(&next.entries));
        assert!(Arc::ptr_eq(
            preceding.entries.get(&124).verified("fixture key"),
            next.entries.get(&124).verified("fixture key")
        ));
    }
}
