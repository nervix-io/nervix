//! Fixed values name the contract that sets their value.

#[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI encodes its tag in four bytes"))]
pub const TAG_BYTES: usize = 4;

pub struct Frame;
impl Frame {
    #[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI encodes its tag in four bytes"))]
    pub const TAG_BYTES: usize = 4;
}

pub trait Wire {
    #[cfg_attr(nervix_lint, nervix::constant(fixed, reason = "the frame ABI encodes its tag in four bytes"))]
    const TAG_BYTES: usize = 4;
}
