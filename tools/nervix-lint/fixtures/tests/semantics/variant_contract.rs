//! An enum value does not own an execution context.
pub enum Value {
    #[cfg_attr(nervix_lint, nervix::context(lifecycle, reason = "variant value"))]
    Ready,
}
