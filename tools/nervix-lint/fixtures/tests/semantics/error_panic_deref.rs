//! Deref reaches the canonical panic API.
pub fn run(value: Box<Option<u32>>) -> u32 { value.expect("present") }
