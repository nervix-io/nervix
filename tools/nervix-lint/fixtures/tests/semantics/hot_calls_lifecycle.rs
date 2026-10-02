//! Compiler qualification of a recurring caller entering a lifecycle-only contract.

#[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "install the retained service once"))]
fn install() {}

#[cfg_attr(nervix_lint, nervix::context(recurring, reason = "called for every batch"))]
pub fn batch() {
    install();
}
