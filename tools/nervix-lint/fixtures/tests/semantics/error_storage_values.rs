//! Ordinary ownership and type-only markers do not discard failure outcomes.
use std::marker::PhantomData;
pub fn run() {
    let _ = Box::new(Some(7_u32));
    let _ = vec![7_u32];
    let _ = PhantomData::<std::io::Result<()>>;
}
