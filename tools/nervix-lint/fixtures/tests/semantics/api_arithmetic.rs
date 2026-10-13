//! Real arithmetic operations retain their owner through aliases and UFCS.
pub fn integer(value: u32) -> u32 { u32::wrapping_add(value, 1) }
pub fn duration(value: std::time::Duration) -> std::time::Duration {
    value.saturating_mul(2)
}
