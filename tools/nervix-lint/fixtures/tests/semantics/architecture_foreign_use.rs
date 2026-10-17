//! Source-contract compiler fixture; no runtime behavior is asserted.

#![cfg_attr(nervix_lint, nervix::layer(data_plane, reason = "typed plan execution"))]

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "called for every batch"))]
pub fn batch() -> u32 {
    // SAFETY: the fixture is only checked, never linked or run.
    let parsed = unsafe { callee::language::parse_foreign() };
    // SAFETY: as above.
    unsafe { callee::installation::install_foreign() };
    // SAFETY: as above.
    let counted = unsafe { callee::native::count_foreign() };
    parsed + counted
}
