//! A branch ownership expectation belongs to one existing collection declaration.

use std::collections::BTreeMap;

use nervix_lint_fixtures::BranchKey;

pub struct MissingReason {
    #[cfg_attr(nervix_lint, expect(nervix::branch_inversion))]
    pub entries: BTreeMap<BranchKey, usize>,
}

pub struct Unused {
    #[cfg_attr(
        nervix_lint,
        expect(nervix::branch_inversion, reason = "fixture repair https://app.clickup.com/t/fixture")
    )]
    pub value: BranchKey,
}

#[cfg_attr(
    nervix_lint,
    expect(nervix::branch_inversion, reason = "fixture repair https://app.clickup.com/t/fixture")
)]
pub struct TooWide {
    pub first: BTreeMap<BranchKey, usize>,
    pub second: BTreeMap<BranchKey, usize>,
}

#[cfg_attr(nervix_lint, nervix::branch_storage(reason = "it is a fourth table"))]
pub struct BranchTableView {
    pub branches: BTreeMap<BranchKey, usize>,
}
