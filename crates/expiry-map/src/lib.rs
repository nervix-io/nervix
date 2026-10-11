//! A hash-indexed map that keeps its values in a caller-defined expiration order.
//!
//! Layer: primitives.
//!
//! - **Owns.** The map: hash lookup, the intrusive order, and the pointer discipline that keeps the
//!   two views of an entry consistent.
//! - **Depends on.** The standard library, `ahash`, `intrusive-collections`, and shared ownership
//!   from `nervix-primitives`.
//! - **Must not know.** What expires, or why. The caller decides the order; the map holds no clock,
//!   no branch and no Nervix type.

use std::{borrow::Borrow, fmt, hash::Hash, ptr};

use ahash::HashMap;
use intrusive_collections::{LinkedList, LinkedListAtomicLink, UnsafeRef, intrusive_adapter};
use meticulous::OptionExt as _;
use nervix_primitives::sync::Arc;

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
        unsafe { &*ptr::from_ref(key).cast::<Self>() }
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
        self.insert_shared(Arc::new(key), value)
    }

    /// Inserts a new entry at the newest end of the expiration order, keeping
    /// the key in the allocation the caller already shares.
    ///
    /// A key read through [`Self::iter_shared`] therefore enters another map
    /// without being copied. Returns `false` and leaves the existing entry
    /// unchanged when the key is already present.
    pub fn insert_shared(&mut self, key: Arc<K>, value: V) -> bool {
        let key = SharedKey(key);
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
        let node_ptr = ptr::from_ref(oldest);
        let (stored_key, node) = self
            .entries
            .remove_entry(KeyRef::from_key(key.as_ref()))
            .verified(
                "the map inserts into the index and the order list together and holds &mut self \
                 here",
            );
        debug_assert_eq!(Arc::as_ptr(&node), node_ptr);

        let linked = self.order.pop_front().verified(
            "the map inserts into the index and the order list together and holds &mut self here",
        );
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
        let (stored_key, node) = self.entries.remove_entry(key).verified(
            "the map inserts into the index and the order list together and holds &mut self here",
        );
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

    /// Iterates from the oldest entry to the newest. The iterator knows how many entries remain,
    /// and a clone of it walks the same entries again.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            order: self.order.iter(),
            remaining: self.entries.len(),
        }
    }

    /// Iterates from the oldest entry to the newest, yielding each key as the
    /// allocation the map shares it through.
    ///
    /// Cloning a yielded key shares it rather than copying it. Keys are
    /// immutable, so a holder outside the map sees the key the map stored.
    pub fn iter_shared(&self) -> impl Iterator<Item = (&Arc<K>, &V)> {
        self.order.iter().map(|entry| (&entry.key, &entry.value))
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
            .verified(
                "the map inserts into the index and the order list together and holds &mut self \
                 here",
            );
        debug_assert_eq!(UnsafeRef::into_raw(linked).cast_const(), node_ptr);
    }
}

impl<K, V> ExpiryMap<K, V> {
    /// The entry's shared allocation: its reference count, its order link, its key handle and its
    /// value.
    const ENTRY_ALLOCATION_BYTES: usize =
        std::mem::size_of::<usize>() + std::mem::size_of::<Entry<K, V>>();

    /// The key's shared allocation: its reference count and the inline key.
    const KEY_ALLOCATION_BYTES: usize = std::mem::size_of::<usize>() + std::mem::size_of::<K>();

    /// One slot of the index: the key handle and the entry handle it stores, and its control byte.
    const INDEX_SLOT_BYTES: usize =
        std::mem::size_of::<SharedKey<K>>() + std::mem::size_of::<Arc<Entry<K, V>>>() + 1;

    /// What the map holds for one entry beside the heap data the key itself owns: the entry's and
    /// the key's shared allocations, and two index slots, for the room the index keeps while it
    /// grows.
    pub const ENTRY_BYTES: usize =
        Self::ENTRY_ALLOCATION_BYTES + Self::KEY_ALLOCATION_BYTES + 2 * Self::INDEX_SLOT_BYTES;
}

/// The entries of an [`ExpiryMap`], from the oldest to the newest.
pub struct Iter<'a, K, V> {
    order: intrusive_collections::linked_list::Iter<'a, EntryAdapter<K, V>>,
    remaining: usize,
}

impl<K, V> Clone for Iter<'_, K, V> {
    fn clone(&self) -> Self {
        Self {
            order: self.order.clone(),
            remaining: self.remaining,
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.order.next()?;
        self.remaining = self
            .remaining
            .checked_sub(1)
            .assured("the order list links exactly the entries the index holds");
        Some((entry.key.as_ref(), &entry.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<K, V> ExactSizeIterator for Iter<'_, K, V> {}

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
    use std::cell::Cell;

    use indexmap::IndexMap;
    use meticulous::OptionExt as _;
    use nervix_primitives::sync::{
        Arc, StdArc,
        atomic::{AtomicUsize, Ordering},
    };

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
    fn iteration_counts_the_remaining_entries_and_walks_them_again_when_cloned() {
        let mut map = ExpiryMap::default();
        for key in 0..4 {
            assert!(map.insert(key, key * 10));
        }
        assert_eq!(map.remove(&1), Some(10));
        let mut entries = map.iter();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.next(), Some((&0, &0)));
        let again = entries.clone();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            vec![(2, 20), (3, 30)]
        );
        assert_eq!(again.len(), 2);
        assert_eq!(
            again.map(|(key, value)| (*key, *value)).collect::<Vec<_>>(),
            vec![(2, 20), (3, 30)]
        );
    }

    /// On a 64-bit target, an entry of `u64` keys and values is charged for its entry allocation (a
    /// reference count, a key handle and the value, 24 bytes beside the order link), its 16-byte key
    /// allocation (a reference count and the key), and two 17-byte index slots (two handles and a
    /// control byte each).
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn an_entry_is_charged_for_both_allocations_and_two_index_slots() {
        type Map = ExpiryMap<u64, u64>;
        let link = std::mem::size_of::<intrusive_collections::LinkedListAtomicLink>();
        assert_eq!(Map::ENTRY_ALLOCATION_BYTES, 24 + link);
        assert_eq!(Map::KEY_ALLOCATION_BYTES, 16);
        assert_eq!(Map::INDEX_SLOT_BYTES, 17);
        assert_eq!(Map::ENTRY_BYTES, 74 + link);
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
        struct DropProbe(StdArc<AtomicUsize>);

        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = StdArc::new(AtomicUsize::new(0));
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

    /// The keys a generated sequence names. A small space makes inserts meet present keys and
    /// removals find them, while still growing the index through several resizes.
    const KEY_SPACE: u8 = 96;

    /// The most steps one generated sequence holds.
    const MAX_STEPS: usize = 480;

    /// How many steps pass under Miri between complete comparisons with the reference. Every step
    /// runs its operation and compares its own answer; Miri interprets every step slowly, so there
    /// the map's complete state is compared every sixteenth step and once the sequence ends, and
    /// everywhere else after every step.
    const MIRI_STATE_CHECK_INTERVAL: usize = 16;

    /// One generated step. `kind` selects the operation with weights under which the map grows
    /// through several index resizes before removals and the occasional clear shrink it, and
    /// `key` names a key of [`KEY_SPACE`].
    #[derive(Debug, bolero::TypeGenerator)]
    struct Step {
        kind: u8,
        key: u8,
    }

    /// One operation on the map under test, or on the map its shared keys are copied into.
    #[derive(Debug)]
    enum Operation {
        Insert(String),
        InsertShared(String),
        Remove(String),
        RemoveOldest,
        Lookup(String),
        ShareInto,
        Clear,
    }

    impl Step {
        fn operation(&self) -> Operation {
            let key = format!("key-{}", self.key % KEY_SPACE);
            match self.kind {
                0..=99 => Operation::Insert(key),
                100..=139 => Operation::InsertShared(key),
                140..=179 => Operation::Remove(key),
                180..=209 => Operation::RemoveOldest,
                210..=239 => Operation::Lookup(key),
                240..=251 => Operation::ShareInto,
                252..=u8::MAX => Operation::Clear,
            }
        }
    }

    /// How many times each generated value has been dropped, by the value's identity.
    struct DropLedger {
        drops: Vec<Cell<u32>>,
    }

    impl DropLedger {
        fn new(values: usize) -> Self {
            Self {
                drops: (0..values).map(|_| Cell::new(0)).collect(),
            }
        }

        fn drops(&self, identity: usize) -> u32 {
            self.drops[identity].get()
        }
    }

    /// A stored value that records its own drop, so every value the map takes is released exactly
    /// once: when it is removed, rejected, cleared or dropped with the map.
    struct Tracked<'a> {
        identity: usize,
        ledger: &'a DropLedger,
    }

    impl Drop for Tracked<'_> {
        fn drop(&mut self) {
            let drops = &self.ledger.drops[self.identity];
            let dropped = drops
                .get()
                .checked_add(1)
                .assured("a value is dropped at most once, so its count stays far below u32::MAX");
            drops.set(dropped);
        }
    }

    /// The map under test beside a safe reference model of it. The reference keeps the expiration
    /// order with `IndexMap`, whose order-preserving removal stands in for the intrusive list.
    struct ModelCheck<'a> {
        ledger: &'a DropLedger,
        map: ExpiryMap<String, Tracked<'a>>,
        reference: IndexMap<String, usize>,
        shared: ExpiryMap<String, usize>,
        shared_reference: IndexMap<String, usize>,
        created: usize,
    }

    impl<'a> ModelCheck<'a> {
        fn new(ledger: &'a DropLedger) -> Self {
            Self {
                ledger,
                map: ExpiryMap::default(),
                reference: IndexMap::new(),
                shared: ExpiryMap::new(),
                shared_reference: IndexMap::new(),
                created: 0,
            }
        }

        fn value(&mut self) -> Tracked<'a> {
            let identity = self.created;
            self.created = identity.checked_add(1).assured(
                "each operation creates at most one value, so the count is bounded by the sequence",
            );
            Tracked {
                identity,
                ledger: self.ledger,
            }
        }

        fn apply(&mut self, step: usize, operation: &Operation) {
            match operation {
                Operation::Insert(key) => {
                    let key = key.clone();
                    let value = self.value();
                    let identity = value.identity;
                    let expected = !self.reference.contains_key(&key);
                    if expected {
                        self.reference.insert(key.clone(), identity);
                    }
                    assert_eq!(self.map.insert(key, value), expected);
                }
                Operation::InsertShared(key) => {
                    let key = Arc::new(key.clone());
                    let value = self.value();
                    let identity = value.identity;
                    let expected = !self.reference.contains_key(key.as_ref());
                    if expected {
                        self.reference.insert(key.as_ref().clone(), identity);
                    }
                    assert_eq!(self.map.insert_shared(key.clone(), value), expected);
                    if expected {
                        let (stored, _) = self
                            .map
                            .iter_shared()
                            .last()
                            .verified("the insert above appended an entry");
                        assert!(Arc::ptr_eq(stored, &key));
                    }
                }
                Operation::Remove(key) => {
                    let expected = self.reference.shift_remove(key);
                    let removed = self.map.remove(key);
                    assert_eq!(removed.map(|value| value.identity), expected);
                }
                Operation::RemoveOldest => {
                    let expected = self.reference.shift_remove_index(0);
                    let removed = self.map.remove_oldest();
                    let expected_identity = expected.map(|(_, identity)| identity);
                    assert_eq!(removed.map(|value| value.identity), expected_identity);
                }
                Operation::Lookup(key) => {
                    let expected = self.reference.get(key).copied();
                    assert_eq!(self.map.contains_key(key), expected.is_some());
                    assert_eq!(self.map.get(key).map(|value| value.identity), expected);
                }
                Operation::ShareInto => self.share_into(),
                Operation::Clear => {
                    self.reference.clear();
                    self.map.clear();
                }
            }
            if !cfg!(miri) || step.is_multiple_of(MIRI_STATE_CHECK_INTERVAL) {
                self.check_state();
            }
        }

        /// Copies every entry of the map under test into the shared map through the keys' own
        /// allocations: a key the shared map lacks is appended without being copied, and a key it
        /// holds keeps its earlier entry.
        fn share_into(&mut self) {
            for (key, value) in self.map.iter_shared() {
                let expected = !self.shared_reference.contains_key(key.as_ref());
                if expected {
                    self.shared_reference
                        .insert(key.as_ref().clone(), value.identity);
                }
                assert_eq!(
                    self.shared.insert_shared(key.clone(), value.identity),
                    expected
                );
                if expected {
                    let (stored, _) = self
                        .shared
                        .iter_shared()
                        .last()
                        .verified("the insert above appended an entry");
                    assert!(Arc::ptr_eq(stored, key));
                }
            }
        }

        /// Compares the complete state of both maps with their references, and the drop ledger with
        /// the values the reference still holds.
        fn check_state(&self) {
            let expected = self
                .reference
                .iter()
                .map(|(key, identity)| (key.as_str(), *identity))
                .collect::<Vec<_>>();
            assert_eq!(self.map.len(), expected.len());
            assert_eq!(self.map.is_empty(), expected.is_empty());
            let oldest = self
                .map
                .oldest()
                .map(|(key, value)| (key.as_str(), value.identity));
            assert_eq!(oldest, expected.first().copied());

            let mut entries = self.map.iter();
            let again = entries.clone();
            let mut walked = Vec::with_capacity(expected.len());
            loop {
                let remaining = expected
                    .len()
                    .checked_sub(walked.len())
                    .verified("the walk stops once it has yielded every expected entry");
                assert_eq!(entries.size_hint(), (remaining, Some(remaining)));
                let Some((key, value)) = entries.next() else {
                    break;
                };
                walked.push((key.as_str(), value.identity));
            }
            assert_eq!(walked, expected);
            let walked_again = again
                .map(|(key, value)| (key.as_str(), value.identity))
                .collect::<Vec<_>>();
            assert_eq!(walked_again, expected);
            let shared_keys = self
                .map
                .iter_shared()
                .map(|(key, value)| (key.as_str(), value.identity))
                .collect::<Vec<_>>();
            assert_eq!(shared_keys, expected);

            let shared_expected = self
                .shared_reference
                .iter()
                .map(|(key, identity)| (key.as_str(), *identity))
                .collect::<Vec<_>>();
            let shared_actual = self
                .shared
                .iter()
                .map(|(key, identity)| (key.as_str(), *identity))
                .collect::<Vec<_>>();
            assert_eq!(shared_actual, shared_expected);

            let mut live = vec![false; self.created];
            for identity in self.reference.values() {
                live[*identity] = true;
            }
            for (identity, live) in live.into_iter().enumerate() {
                let expected_drops = if live { 0 } else { 1 };
                assert_eq!(
                    self.ledger.drops(identity),
                    expected_drops,
                    "value {identity}"
                );
            }
        }
    }

    #[test]
    fn bolero_operation_sequences_match_a_safe_reference_model() {
        let steps = bolero::generator::produce_with::<Vec<Step>>().len(0..=MAX_STEPS);
        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .with_generator(steps)
            .for_each(|steps| {
                let ledger = DropLedger::new(steps.len());
                let mut check = ModelCheck::new(&ledger);
                for (index, step) in steps.iter().enumerate() {
                    check.apply(index, &step.operation());
                }
                check.check_state();
                assert_eq!(
                    format!("{:?}", check.shared),
                    format!("{:?}", check.shared_reference)
                );
                let created = check.created;
                drop(check);
                for identity in 0..steps.len() {
                    let expected_drops = if identity < created { 1 } else { 0 };
                    assert_eq!(ledger.drops(identity), expected_drops, "value {identity}");
                }
            });
    }

    #[test]
    fn shared_keys_move_between_maps_without_being_copied() {
        let mut original = ExpiryMap::default();
        assert!(original.insert("first".to_string(), 10));
        assert!(original.insert("second".to_string(), 20));

        let mut shared = ExpiryMap::default();
        for (key, value) in original.iter_shared() {
            assert!(shared.insert_shared(key.clone(), *value));
        }
        assert!(!shared.insert_shared(nervix_primitives::sync::Arc::new("first".to_string()), 100));

        for ((original_key, _), (shared_key, _)) in original.iter_shared().zip(shared.iter_shared())
        {
            assert!(nervix_primitives::sync::Arc::ptr_eq(
                original_key,
                shared_key
            ));
        }
        drop(original);
        assert_eq!(
            shared
                .iter()
                .map(|(key, value)| (key.as_str(), *value))
                .collect::<Vec<_>>(),
            vec![("first", 10), ("second", 20)]
        );
        assert_eq!(shared.remove(&"second".to_string()), Some(20));
        assert_eq!(shared.remove_oldest(), Some(10));
        assert!(shared.is_empty());
    }

    #[test]
    fn map_is_send_when_keys_and_values_are_thread_safe() {
        fn assert_send<T: Send>() {}
        assert_send::<ExpiryMap<String, u64>>();
    }

    #[test]
    fn ahash_lookup_matches_the_shared_key_hash() {
        let key = super::SharedKey(nervix_primitives::sync::Arc::new(7_i32));
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
