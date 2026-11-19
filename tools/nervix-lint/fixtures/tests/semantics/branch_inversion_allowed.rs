//! Same collection APIs with ordinary identities and an individual branch key.

use std::collections::BTreeMap as Lookup;

use nervix_lint_fixtures::{BranchKey as ActualBranchKey, PayloadRow};

pub struct BranchKey(pub u32);
pub struct BranchTask(pub u32);

pub struct OtherSnapshot {
    pub incarnation: u64,
}

pub fn keyed_by_ordinary_number(snapshot: OtherSnapshot, values: &mut Lookup<u64, u32>) {
    let lifetime = snapshot.incarnation;
    Lookup::insert(values, lifetime, 1);
}

pub struct ReceivedRow {
    pub branch: ActualBranchKey,
    pub payload: Vec<u8>,
}

pub struct Processor {
    pub timers: Lookup<Option<BranchKey>, u32>,
    pub tasks: Vec<BranchTask>,
    pub deadlines: Lookup<u64, u32>,
    pub key: ActualBranchKey,
    pub received: Vec<ReceivedRow>,
    pub payload: Vec<(ActualBranchKey, PayloadRow)>,
}

pub enum Report {
    Selected {
        #[cfg_attr(
            nervix_lint,
            expect(
                nervix::branch_inversion,
                reason = "fixture report temporarily holds a set; repair https://app.clickup.com/t/fixture"
            )
        )]
        keys: std::collections::BTreeSet<ActualBranchKey>,
    },
}

impl Report {
    pub fn selected(key: ActualBranchKey) -> Self {
        let keys = std::collections::BTreeSet::from([key]);
        Self::Selected { keys }
    }
}
