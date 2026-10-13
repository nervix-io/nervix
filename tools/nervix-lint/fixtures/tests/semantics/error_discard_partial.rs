//! A tuple wildcard drops the failure even when another field is retained.
pub fn run(result: std::io::Result<()>) -> u32 {
    let (_, value) = (result, 5_u32);
    value
}
