//! Identically named methods on ordinary values carry no failure rule.
pub struct Ordinary;
impl Ordinary {
    pub fn unwrap(self) -> u32 { 7 }
    pub fn expect(self, _reason: &str) -> u32 { 7 }
    pub fn discarded(self) {}
}
pub struct Associated;
impl Associated {
    pub fn unwrap(value: Option<u32>) -> u32 { value.unwrap_or(7) }
    pub fn expect(value: Result<u32, ()>, _reason: &str) -> u32 { value.unwrap_or(7) }
}
pub fn run() -> u32 {
    Ordinary.discarded();
    Ordinary.unwrap() + Ordinary.expect("label")
        + Associated::unwrap(Some(7)) + Associated::expect(Ok(7), "label")
}
