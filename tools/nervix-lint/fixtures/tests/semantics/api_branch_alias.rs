//! Function-pointer erasure preserves the canonical branch operation at its reference.
pub fn branch(value: Option<u32>) -> u32 {
    let choose: fn(Option<u32>, u32, fn(u32) -> u32) -> u32 = Option::map_or;
    choose(value, 0, u32::from)
}
