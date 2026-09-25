//! One-shot allocation evidence for the VM Criterion fixtures.
//!
//! This module is built only by `just bench-vm-alloc`. Its global allocator counts requested
//! bytes while one fixture executes. Criterion timing uses the ordinary allocator build.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use meticulous::OptionExt as _;

use super::*;

struct MeteredAllocator;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: MeteredAllocator = MeteredAllocator;

impl MeteredAllocator {
    fn record(pointer: *mut u8, bytes: usize) {
        if !pointer.is_null() && ACTIVE.load(Ordering::Relaxed) {
            // Each probe starts from zero and executes one in-memory batch, whose requested
            // allocation total is bounded far below usize::MAX on supported platforms.
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(bytes, Ordering::Relaxed);
        }
    }
}

// SAFETY: This wrapper delegates every layout and pointer to System unchanged. The counters do
// not allocate and are advisory measurements; they never affect allocation or deallocation.
unsafe impl GlobalAlloc for MeteredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies the allocation layout required by GlobalAlloc.
        let pointer = unsafe { System.alloc(layout) };
        Self::record(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies the allocation layout required by GlobalAlloc.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        Self::record(pointer, layout.size());
        pointer
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller owns this allocation and supplies its current layout and new size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        Self::record(resized, new_size);
        resized
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The caller owns this allocation and supplies its original layout.
        unsafe { System.dealloc(pointer, layout) }
    }
}

pub(super) fn measure(
    runtime: &tokio::runtime::Runtime,
    name: &str,
    shape: &str,
    program: &Arc<CompiledProgram>,
    batch: &TypedBatch,
) {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Release);
    let output = runtime
        .block_on(execute_benchmark_program(program, batch))
        .assured("the validated benchmark fixture executes successfully");
    ACTIVE.store(false, Ordering::Release);

    let mut output_column_bytes = 0_usize;
    for column in output.columns() {
        output_column_bytes = output_column_bytes
            .checked_add(column.to_array_ref().get_array_memory_size())
            .assured("one benchmark batch fits addressable memory");
    }
    eprintln!(
        "vm_allocation_evidence name={name} shape={shape} rows={} allocations={} \
         allocated_bytes={} output_column_bytes={output_column_bytes}",
        batch.row_count(),
        ALLOCATIONS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
    );
}
