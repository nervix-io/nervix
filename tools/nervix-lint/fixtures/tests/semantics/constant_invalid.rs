//! A fixed-value contract belongs on exactly one constant, with a meaningful reason.

#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = ""))]
pub const EMPTY: usize = 4;

#[cfg_attr(nervix_lint, nervix::constant(tunable, reason = "a local preference"))]
pub const KIND: usize = 4;

#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI"))]
pub fn misplaced() {}

#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI"))]
#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI"))]
pub const DUPLICATE: usize = 4;
