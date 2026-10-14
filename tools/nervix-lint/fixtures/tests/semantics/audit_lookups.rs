//! Audit fixture: keyed lookups resolved by receiver place, key value and branch reachability.
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "the audit calibration runs these examples once")
)]

use std::collections::{BTreeMap, HashMap};

use nervix_lint_fixtures::MapAlias;

pub struct Panes {
    panes: BTreeMap<u64, Vec<u64>>,
    totals: HashMap<String, u64>,
}

impl Panes {
    /// A miss looks the pane up twice and repeats the update: an emulated entry.
    #[cfg_attr(
        nervix_lint,
        nervix::context(recurring, reason = "every admitted row reaches its pane")
    )]
    pub fn admit(&mut self, pane: u64, value: u64) {
        if let Some(values) = self.panes.get_mut(&pane) {
            values.push(value);
        } else {
            let mut values = Vec::new();
            values.push(value);
            self.panes.insert(pane, values);
        }
    }

    /// A test, then an indexed read of the same key in the selected branch.
    pub fn total(&self, name: &str) -> u64 {
        if self.totals.contains_key(name) {
            return self.totals[name];
        }
        0
    }

    /// One entry is one lookup.
    pub fn count(&mut self, name: String) {
        *self.totals.entry(name).or_default() += 1;
    }

    /// Different keys reach different slots.
    pub fn rename(&mut self, from: &str, to: String) {
        if let Some(value) = self.totals.remove(from) {
            self.totals.insert(to, value);
        }
    }

    /// Exclusive branches never both run.
    pub fn reset(&mut self, pane: u64, keep: bool) {
        if keep {
            self.panes.insert(pane, Vec::new());
        } else {
            self.panes.remove(&pane);
        }
    }

    /// The key changes between the two operations.
    pub fn advance(&mut self, mut pane: u64) {
        self.panes.remove(&pane);
        pane += 1;
        self.panes.insert(pane, Vec::new());
    }

    /// Another collection with the same key is another place.
    pub fn mirror(&mut self, other: &mut BTreeMap<u64, Vec<u64>>, pane: u64) {
        if let Some(values) = self.panes.remove(&pane) {
            other.insert(pane, values);
        }
    }
}

/// A shared map reached twice with one key takes two shard guards.
pub fn shared(registry: &MapAlias, key: u32) {
    if registry.get(&key).is_none() {
        registry.insert(key, 0);
    }
}

/// UFCS resolves the same owner and key.
pub fn qualified(map: &mut HashMap<u64, u64>, key: u64) {
    if !HashMap::contains_key(map, &key) {
        HashMap::insert(map, key, 1);
    }
}

/// A custom type with map-like method names is not a keyed collection.
pub struct Registry;

impl Registry {
    pub fn get(&self, _key: &u64) -> Option<u64> {
        None
    }

    pub fn insert(&mut self, _key: u64, _value: u64) {}
}

pub fn custom(registry: &mut Registry, key: u64) {
    if registry.get(&key).is_none() {
        registry.insert(key, 0);
    }
}
