//! Implementation contracts preserve the trait's promised frequency.
#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "batch callback"))]
pub trait Batch { fn run(&self); }
pub struct Processor;
impl Batch for Processor {
    #[cfg_attr(nervix_lint, nervix::context(outside, reason = "implementation installation"))]
    fn run(&self) {}
}
