//! Primitive layer.
//! Owns: a stable statement boundary for one expression's compiler lint expectation.
//! Depends on: Rust macro and attribute syntax only.
//! Must not know: lint rules, execution frequency, runtime ownership or repair tickets.

/// Evaluate one operation with a source-owned lint expectation during Nervix analysis.
///
/// Rust's stable expression grammar does not accept conditional attributes in every expression
/// position. This macro supplies a single binding instead. The caller supplies the lint and its
/// reason; the compiler's expectation and cardinality checks still apply to the complete operation.
/// Ordinary builds evaluate and return the operation once, without a runtime wrapper.
///
/// ```
/// let value = nervix_primitives::expect_lint!(
///     nervix::sync_acquisition,
///     "the owning protocol review explains this one operation",
///     42,
/// );
/// assert_eq!(value, 42);
/// ```
#[macro_export]
macro_rules! expect_lint {
    ($lint:path, $reason:literal, $operation:expr $(,)?) => {{
        #[cfg_attr(nervix_lint, expect($lint, reason = $reason))]
        let operation = $operation;
        operation
    }};
}
