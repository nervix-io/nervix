//! Audit fixture: owned keys and copies that a borrowed form would avoid.
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "the audit calibration runs these examples once")
)]

use std::collections::{BTreeMap, HashMap};

use nervix_lint_fixtures::SharedValue;

/// A borrowed lookup that allocates its key first.
pub fn formatted(totals: &HashMap<String, u64>, name: &str) -> bool {
    totals.contains_key(&name.to_string())
}

/// An entry whose cloned key a hit discards.
pub fn counted(counts: &mut BTreeMap<Vec<u8>, u64>, key: &Vec<u8>) {
    *counts.entry(key.clone()).or_default() += 1;
}

/// A copy that exists only to be borrowed.
pub fn measured(name: &String) -> usize {
    let copy_length = name.clone().len();
    let borrowed = describe(&name.to_owned());
    copy_length + borrowed
}

fn describe(value: &str) -> usize {
    value.len()
}

/// Borrowed lookups, owned insertion keys, copies of Copy values and shared handles are not
/// candidates.
pub fn borrowed(
    totals: &HashMap<String, u64>,
    inserted: &mut HashMap<String, u64>,
    name: &str,
    pane: u64,
    shared: &SharedValue<u64>,
) -> usize {
    let present = totals.contains_key(name);
    inserted.insert(name.to_owned(), 1);
    let copied = pane.clone();
    let handle = SharedValue::clone(shared);
    let measured = describe(&format!("{copied}{handle}"));
    measured + usize::from(present)
}
