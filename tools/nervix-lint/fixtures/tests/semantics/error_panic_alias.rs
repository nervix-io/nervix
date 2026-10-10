//! An inferred function item alias retains its resolved definition.
pub fn run(value: Result<u32, ()>) -> u32 {
    let take = Result::unwrap;
    take(value)
}
