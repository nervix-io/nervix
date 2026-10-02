//! A binding cannot assign one context to multiple independent bodies.
pub fn install() {
    #[cfg_attr(nervix_lint, nervix::context(recurring, reason = "two independent tasks"))]
    let tasks = (|| {}, || {});
    tasks.0();
}
