//! Installation-only state cannot use a replaceable read-write lock representation.

use nervix_lint_fixtures::{SharedValue, StateLock};

pub struct Installed {
    #[cfg_attr(nervix_lint, nervix::state(installed, reason = "installed once before execution"))]
    pub handle: StateLock<Option<SharedValue<u32>>>,
}
