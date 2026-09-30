//! Compiler fixture outside the product layer order.
//! Owns: an external macro that expands its caller's authored tokens.
//! Depends on: Rust macro expansion only.
//! Must not know: synchronization policy or runtime state.

#[macro_export]
macro_rules! pass_authored_tokens {
    ($($tokens:tt)*) => { $($tokens)* };
}
