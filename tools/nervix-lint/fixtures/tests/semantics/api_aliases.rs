//! Canonical definitions survive type aliases, callable aliases and authored macro expansion.
type Word = u32;
pub fn alias(value: Word) -> Word { Word::saturating_add(value, 1) }
pub fn callable(value: u32) -> u32 {
    let add: fn(u32, u32) -> u32 = u32::wrapping_add;
    add(value, 1)
}
macro_rules! wrap { ($value:expr) => { $value.wrapping_sub(1) }; }
pub fn expanded(value: u32) -> u32 { wrap!(value) }
