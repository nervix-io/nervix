//! Allocation accounting for malformed current archives.
//!
//! Layer: test harness.
//! - **Owns.** Per-thread allocation and deallocation equality around archive decoding.
//! - **Depends on.** The test allocator installed by the server and a decoder supplied by its owner.
//! - **Must not know.** Registry, runtime, or interconnect record shapes.

use meticulous::OptionExt as _;

pub(crate) fn assert_decode_frees_allocations<F, T>(decode: F)
where
    F: Fn() -> T,
{
    // Decoder setup may allocate once on this thread.
    drop(decode());
    let before = alloc_count::stats();
    drop(decode());
    let after = alloc_count::stats();
    let allocated = after
        .alloc_calls
        .checked_sub(before.alloc_calls)
        .assured("a thread's allocation count only grows");
    let freed = after
        .dealloc_calls
        .checked_sub(before.dealloc_calls)
        .assured("a thread's deallocation count only grows");
    assert_eq!(freed, allocated, "archive decoding retains no allocation");
}
