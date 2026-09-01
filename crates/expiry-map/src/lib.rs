use std::{borrow::Borrow, fmt, hash::Hash};

use ahash::HashMap;
use intrusive_collections::{LinkedList, LinkedListAtomicLink, UnsafeRef, intrusive_adapter};
use triomphe::Arc;

struct Entry<K, V> {
    link: LinkedListAtomicLink,
    key: Arc<K>,
    value: V,
}

#[repr(transparent)]
#[derive(PartialEq, Eq, Hash)]
struct KeyRef<K>(K);

impl<K> KeyRef<K> {
    fn from_key(key: &K) -> &Self {
        // SAFETY: `KeyRef<K>` is transparent over `K`, so the shared reference
        // has the same address, alignment, validity, and lifetime.
        unsafe { &*(key as *const K).cast::<Self>() }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct SharedKey<K>(Arc<K>);

impl<K> Borrow<KeyRef<K>> for SharedKey<K> {
    fn borrow(&self) -> &KeyRef<K> {
        KeyRef::from_key(self.0.as_ref())
    }
}

intrusive_adapter!(
    EntryAdapter<K, V> = UnsafeRef<Entry<K, V>>: Entry<K, V> {
        link => LinkedListAtomicLink
    }
);

/// A hash-indexed map that keeps values in caller-defined expiration order.
///
/// Insertion appends to the newest end of the order. Callers can inspect or
/// remove the oldest value in constant time without shifting the remaining
/// entries. Keys and values are immutable while stored so intrusive pointers
/// always refer to stable data.
pub struct ExpiryMap<K, V> {
    // This field must be dropped before `entries`: the list borrows its nodes.
    order: LinkedList<EntryAdapter<K, V>>,
    entries: HashMap<SharedKey<K>, Arc<Entry<K, V>>>,
}

impl<K, V> ExpiryMap<K, V>
where
    K: Eq + Hash,
{
    /// Creates an empty map.
    pub fn new() -> Self {
        Self {
            order: LinkedList::new(EntryAdapter::new()),
            entries: HashMap::default(),
        }
    }

    /// Inserts a new entry at the newest end of the expiration order.
    ///
    /// Returns `false` and leaves the existing entry unchanged when the key is
    /// already present.
    pub fn insert(&mut self, key: K, value: V) -> bool {
        let key = SharedKey(Arc::new(key));
        let std::collections::hash_map::Entry::Vacant(slot) = self.entries.entry(key) else {
            return false;
        };

        let node = Arc::new(Entry {
            link: LinkedListAtomicLink::new(),
            key: slot.key().0.clone(),
            value,
        });
        let node_ptr = Arc::as_ptr(slot.insert(node));

        // SAFETY: `node_ptr` points into an Arc now owned by `entries`, so moving
        // or rehashing the map cannot move or exclusively retag the Entry. The
        // Entry is unlinked here, remains immutable while linked, and every
        // removal keeps the Arc alive until after unlinking.
        self.order
            .push_back(unsafe { UnsafeRef::from_raw(node_ptr) });
        true
    }

    /// Reports whether the key is present.
    pub fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(KeyRef::from_key(key))
    }

    /// Returns the value stored for a key.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .get(KeyRef::from_key(key))
            .map(|entry| &entry.value)
    }

    /// Returns the oldest entry.
    pub fn oldest(&self) -> Option<(&K, &V)> {
        self.order
            .front()
            .get()
            .map(|entry| (entry.key.as_ref(), &entry.value))
    }

    /// Removes the oldest entry.
    pub fn remove_oldest(&mut self) -> Option<V> {
        let oldest = self.order.front().get()?;
        let key = oldest.key.clone();
        let node_ptr = oldest as *const Entry<K, V>;
        let (stored_key, node) = self
            .entries
            .remove_entry(KeyRef::from_key(key.as_ref()))
            .expect("linked entry must exist in the hash index");
        debug_assert_eq!(Arc::as_ptr(&node), node_ptr);

        let linked = self
            .order
            .pop_front()
            .expect("indexed oldest entry must remain linked");
        debug_assert_eq!(UnsafeRef::into_raw(linked).cast_const(), node_ptr);
        drop(key);
        drop(stored_key);

        let Ok(node) = Arc::try_unwrap(node) else {
            unreachable!("the hash index must own the only Entry Arc");
        };
        let Entry {
            link: _,
            key: node_key,
            value,
        } = node;
        drop(node_key);
        Some(value)
    }

    /// Removes an entry by key.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let key = KeyRef::from_key(key);
        let node_ptr = self.entries.get(key).map(Arc::as_ptr)?;
        let (stored_key, node) = self
            .entries
            .remove_entry(key)
            .expect("entry found by key must still exist during exclusive removal");
        debug_assert_eq!(Arc::as_ptr(&node), node_ptr);
        self.unlink(node_ptr);
        drop(stored_key);

        let Ok(node) = Arc::try_unwrap(node) else {
            unreachable!("the hash index must own the only Entry Arc");
        };
        let Entry {
            link: _,
            key: node_key,
            value,
        } = node;
        drop(node_key);
        Some(value)
    }

    /// Iterates from the oldest entry to the newest.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.order
            .iter()
            .map(|entry| (entry.key.as_ref(), &entry.value))
    }

    /// Removes every entry.
    pub fn clear(&mut self) {
        self.order.clear();
        self.entries.clear();
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn unlink(&mut self, node_ptr: *const Entry<K, V>) {
        // SAFETY: the pointer was read from `entries` under exclusive access to
        // this map. Every indexed Entry is linked exactly once, and the Arc that
        // owns it remains alive in the caller until this cursor removes the sole
        // UnsafeRef from `order`.
        let linked = unsafe { self.order.cursor_mut_from_ptr(node_ptr) }
            .remove()
            .expect("indexed entry must remain linked");
        debug_assert_eq!(UnsafeRef::into_raw(linked).cast_const(), node_ptr);
    }
}

impl<K, V> Default for ExpiryMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> fmt::Debug for ExpiryMap<K, V>
where
    K: Eq + Hash + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use ahash::HashMap;

    use super::ExpiryMap;

    #[test]
    fn insertion_and_duplicate_rejection_preserve_expiration_order() {
        let mut map = ExpiryMap::default();
        assert!(map.insert("first", 10));
        assert!(map.insert("second", 20));
        assert!(map.insert("third", 30));
        assert!(!map.insert("second", 200));

        assert_eq!(map.oldest(), Some((&"first", &10)));
        assert_eq!(map.get(&"second"), Some(&20));
        assert_eq!(
            map.iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![("first", 10), ("second", 20), ("third", 30)]
        );
        assert_eq!(map.len(), 3);
        assert!(!map.is_empty());
        assert_eq!(
            format!("{map:?}"),
            "{\"first\": 10, \"second\": 20, \"third\": 30}"
        );
    }

    #[test]
    fn removing_oldest_updates_the_list_and_hash_index() {
        let mut map = ExpiryMap::default();
        let entry_count = if cfg!(miri) { 256 } else { 4_096 };
        for key in 0..entry_count {
            assert!(map.insert(key, key * 10));
        }

        for key in 0..entry_count {
            assert_eq!(map.remove_oldest(), Some(key * 10));
            assert!(!map.contains_key(&key));
        }
        assert_eq!(map.remove_oldest(), None);
        assert!(map.is_empty());
    }

    #[test]
    fn keyed_removal_unlinks_front_middle_and_back() {
        let mut map = ExpiryMap::default();
        for key in 0..5 {
            assert!(map.insert(key, key));
        }

        assert_eq!(map.remove(&0), Some(0));
        assert_eq!(map.remove(&2), Some(2));
        assert_eq!(map.remove(&4), Some(4));
        assert_eq!(map.remove(&9), None);
        assert!(map.insert(2, 20));

        assert_eq!(
            map.iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(1, 1), (3, 3), (2, 20)]
        );
    }

    #[test]
    fn moving_the_map_does_not_move_linked_values() {
        let mut original = ExpiryMap::default();
        for key in 0..128 {
            assert!(original.insert(key, key));
        }

        let mut moved = original;
        for key in 0..128 {
            assert_eq!(moved.remove_oldest(), Some(key));
        }
    }

    #[test]
    fn clear_remove_and_drop_release_every_value_once() {
        #[derive(Debug)]
        struct DropProbe(Arc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        {
            let mut map = ExpiryMap::default();
            for key in 0..8 {
                assert!(map.insert(key, DropProbe(drops.clone())));
            }
            drop(map.remove(&2));
            drop(map.remove_oldest());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            map.clear();
            assert_eq!(drops.load(Ordering::SeqCst), 8);
        }
        assert_eq!(drops.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn randomized_operations_match_a_safe_reference_model() {
        let mut map = ExpiryMap::default();
        let mut values = HashMap::default();
        let mut order = VecDeque::new();
        let mut random = 0x4d59_5df4_d0f3_3173_u64;
        let steps = if cfg!(miri) { 2_000 } else { 50_000 };

        for step in 0..steps {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = ((random >> 32) % 257) as u16;
            match random & 3 {
                0 => {
                    let value = step as u32;
                    let expected = match values.entry(key) {
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(value);
                            order.push_back(key);
                            true
                        }
                        std::collections::hash_map::Entry::Occupied(_) => false,
                    };
                    assert_eq!(map.insert(key, value), expected);
                }
                1 => {
                    let expected = values.remove(&key);
                    if expected.is_some() {
                        order.retain(|candidate| *candidate != key);
                    }
                    assert_eq!(map.remove(&key), expected);
                }
                2 => {
                    let expected = order.pop_front().map(|oldest| {
                        values
                            .remove(&oldest)
                            .expect("ordered key must exist in reference map")
                    });
                    assert_eq!(map.remove_oldest(), expected);
                }
                _ => assert_eq!(map.contains_key(&key), values.contains_key(&key)),
            }

            let expected = order
                .iter()
                .map(|key| {
                    (
                        *key,
                        *values.get(key).expect("ordered key must have a value"),
                    )
                })
                .collect::<Vec<_>>();
            let actual = map
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(map.len(), values.len());
            assert_eq!(map.is_empty(), values.is_empty());
            assert_eq!(
                map.oldest().map(|(key, value)| (*key, *value)),
                expected.first().copied()
            );
        }
    }

    #[test]
    fn map_is_send_when_keys_and_values_are_thread_safe() {
        fn assert_send<T: Send>() {}
        assert_send::<ExpiryMap<String, u64>>();
    }

    #[test]
    fn ahash_lookup_matches_the_shared_key_hash() {
        let key = super::SharedKey(triomphe::Arc::new(7_i32));
        let hash_builder = ahash::RandomState::default();
        assert_eq!(
            hash_builder.hash_one(&key),
            hash_builder.hash_one(super::KeyRef::from_key(&7))
        );
        let mut index = ahash::HashMap::with_hasher(hash_builder);
        index.insert(key, ());

        assert!(index.contains_key(super::KeyRef::from_key(&7)));
    }
}
