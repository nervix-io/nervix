//! Canonical Option/Result calls sequence decisions through either call syntax.
pub fn chain(value: Option<u32>) -> u32 {
    Option::unwrap_or(Option::map(value, |item| item + 1), 0)
}
pub fn statements(value: Option<u32>) -> Option<u32> {
    value.map(|item| { let next = item + 1; next })
}
