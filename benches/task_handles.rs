//! Same-host timing and allocation samples of retained production task handles.
//!
//! Layer: test harness.
//! - **Owns.** Warm operation samples and their raw timing/allocation report.
//! - **Depends on.** Opaque server fixtures, jemalloc observations and primitive monotonic time.
//! - **Must not know.** Graph construction or task implementation details.

use std::{collections::BTreeMap, fs, path::PathBuf};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;
use nervix_server::runtime::task_handle_benchmark::{TASK_HANDLE_PATHS, TaskHandleBenchmark};
use serde::Serialize;
use tikv_jemalloc_ctl::thread;

#[derive(Serialize)]
struct Sample {
    nanoseconds: u64,
    allocated_bytes: u64,
}

fn main() {
    let output = match std::env::args().nth(1) {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from("target/task-handles.json"),
    };
    let mut fixture = TaskHandleBenchmark::new();
    let allocated = thread::allocatedp::read().assured("the server benchmark uses jemalloc");
    let mut report = BTreeMap::new();
    for path in TASK_HANDLE_PATHS {
        for _ in 0..100 {
            fixture.run(path);
        }
        let mut samples = Vec::with_capacity(100);
        for _ in 0..100 {
            let before = allocated.get();
            let started = Instant::now();
            for _ in 0..1_000 {
                fixture.run(path);
            }
            let elapsed = started.elapsed();
            let bytes = allocated
                .get()
                .checked_sub(before)
                .assured("the allocation counter does not wrap during a sample");
            samples.push(Sample {
                nanoseconds: u64::try_from(elapsed.as_nanos())
                    .assured("one sample fits u64 nanoseconds"),
                allocated_bytes: bytes,
            });
        }
        report.insert(format!("{path:?}"), samples);
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).assured("the report directory is writable");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).assured("samples serialize"),
    )
    .assured("the report is writable");
    println!(
        "100 samples of 1000 operations per path: {}",
        output.display()
    );
}
