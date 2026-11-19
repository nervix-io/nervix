//! Branch state stored in a processor-owned collection.

use std::collections::{BTreeMap as Lookup, BTreeSet};

use nervix_lint_fixtures::{BranchIncarnation, BranchKey, BranchSnapshot, BranchTask, PayloadRow};

pub struct BranchKeyWrapper(pub BranchKey);

pub struct Directory<K> {
    pub entries: Lookup<K, u32>,
    pub maximum: usize,
}

type BranchDirectory = Directory<BranchKey>;

pub struct Processor {
    pub timers: Lookup<Option<BranchKeyWrapper>, u32>,
    pub tasks: Vec<BranchTask>,
    pub tasks_with_data: Vec<(BranchTask, PayloadRow)>,
    pub deadlines: Lookup<BranchIncarnation, u32>,
    pub directory: BranchDirectory,
}

macro_rules! retained_state {
    () => {
        pub struct MacroBranchLists {
            pub branches: BTreeSet<BranchKey>,
        }
    };
}

retained_state!();

pub static GLOBAL: BTreeSet<BranchKey> = BTreeSet::new();

pub fn keyed_by_lifetime(snapshot: BranchSnapshot, timeouts: &mut Lookup<u64, u32>) {
    let lifetime = snapshot.incarnation;
    Lookup::insert(timeouts, lifetime, 1);
}
