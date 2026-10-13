use nervix_lint_fixtures::{SharedValue, StateLock};
type Slot<T> = Option<StateLock<Option<SharedValue<T>>>>;
pub struct State { pub slot: Slot<u32> }
