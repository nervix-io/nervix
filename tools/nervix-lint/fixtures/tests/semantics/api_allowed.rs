//! Matching words on unrelated receivers and one transformation carry no governed decision.
pub struct Domain;
impl Domain {
    pub fn wrapping_add(&self, _: u32) -> u32 { 1 }
    pub fn map_or(&self, _: u32) -> u32 { 2 }
}
pub fn custom(value: &Domain) -> u32 { value.wrapping_add(1) + value.map_or(0) }
pub fn pointer(value: *const u8) -> *const u8 { value.wrapping_add(1) }
pub fn projection(value: Option<u32>) -> Option<u32> { value.map(|item| item + 1) }
pub fn pipeline(values: &[u32]) -> Vec<u32> {
    values.iter().map(|item| item + 1).filter(|item| *item > 0).collect()
}
pub fn shaped(value: Result<u32, std::io::Error>) -> Result<u32, String> {
    value.map_err(|error| error.to_string())
}
