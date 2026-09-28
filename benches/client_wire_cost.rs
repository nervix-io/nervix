//! Reproducible component costs of the public Row wire path.
//!
//! Outside the layer order: a benchmark harness. Product code must not name it.
//!
//! - **Owns.** Fixed Arrow workloads, raw timing samples, frame sizes and retained allocation
//!   observations for the current Row protocol.
//! - **Depends on.** The public wire views and the server's benchmark-only Arrow fixture.
//! - **Must not know.** Session scheduling, registry state or transport implementation internals.

use std::{
    collections::BTreeMap, fs, hint::black_box, num::NonZeroUsize, path::PathBuf, time::Instant,
};

use bytes::{Bytes, BytesMut};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CellView, EncodedFrame, RowSchema, ServerEvent, ServerFrame, ServerMessage, SessionLimits,
    SubscriptionRows, VerifiedFrame,
};
use nervix_server::subscription_row::benchmark::SubscriptionRowBenchmark;
use serde::Serialize;
use tikv_jemalloc_ctl::{epoch, stats, thread};

const SAMPLES: usize = 100;

#[derive(Serialize)]
struct Stage {
    samples_nanoseconds: Vec<u64>,
    p50_nanoseconds: u64,
    p95_nanoseconds: u64,
    p99_nanoseconds: u64,
    allocated_bytes_samples: Vec<u64>,
    p50_allocated_bytes: u64,
}

#[derive(Serialize)]
struct Retention {
    allocated_before_bytes: usize,
    allocated_with_frames_bytes: usize,
    allocated_after_release_bytes: usize,
    resident_before_bytes: usize,
    resident_with_frames_bytes: usize,
    resident_after_release_bytes: usize,
}

#[derive(Serialize)]
struct Case {
    name: &'static str,
    source_rows: usize,
    selected_rows: usize,
    rows_per_frame: usize,
    schema_fields: usize,
    frame_count: usize,
    wire_bytes: usize,
    websocket_data_pointer_reused: bool,
    stages: BTreeMap<&'static str, Stage>,
    retention: Retention,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    git_commit: String,
    rustc: String,
    samples_per_stage: usize,
    cases: Vec<Case>,
}

fn measure(mut operation: impl FnMut()) -> Stage {
    for _ in 0..10 {
        operation();
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut allocated_bytes_samples = Vec::with_capacity(SAMPLES);
    let allocated =
        thread::allocatedp::read().assured("the benchmark uses jemalloc as its global allocator");
    for _ in 0..SAMPLES {
        let allocated_before = allocated.get();
        let start = Instant::now();
        operation();
        samples.push(
            u64::try_from(start.elapsed().as_nanos())
                .assured("a benchmark iteration is shorter than 584 years"),
        );
        allocated_bytes_samples.push(
            allocated
                .get()
                .checked_sub(allocated_before)
                .assured("the jemalloc thread allocation counter does not wrap during one sample"),
        );
    }
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    let mut sorted_allocated = allocated_bytes_samples.clone();
    sorted_allocated.sort_unstable();
    let rank = |values: &[u64], percent: usize| {
        let scaled = values
            .len()
            .checked_mul(percent)
            .assured("100 samples fit usize");
        let index = scaled.div_ceil(100) - 1;
        values[index]
    };
    Stage {
        samples_nanoseconds: samples,
        p50_nanoseconds: rank(&sorted, 50),
        p95_nanoseconds: rank(&sorted, 95),
        p99_nanoseconds: rank(&sorted, 99),
        allocated_bytes_samples,
        p50_allocated_bytes: rank(&sorted_allocated, 50),
    }
}

fn allocator_snapshot() -> (usize, usize) {
    epoch::advance().assured("jemalloc statistics are available to the server benchmark");
    (
        stats::allocated::read().assured("jemalloc allocated statistic is available"),
        stats::resident::read().assured("jemalloc resident statistic is available"),
    )
}

fn retention(benchmark: &SubscriptionRowBenchmark, selected: Option<&[usize]>) -> Retention {
    let (allocated_before_bytes, resident_before_bytes) = allocator_snapshot();
    let frames = encode(benchmark, selected);
    let (allocated_with_frames_bytes, resident_with_frames_bytes) = allocator_snapshot();
    black_box(&frames);
    drop(frames);
    let (allocated_after_release_bytes, resident_after_release_bytes) = allocator_snapshot();
    Retention {
        allocated_before_bytes,
        allocated_with_frames_bytes,
        allocated_after_release_bytes,
        resident_before_bytes,
        resident_with_frames_bytes,
        resident_after_release_bytes,
    }
}

fn encode(
    benchmark: &SubscriptionRowBenchmark,
    selected: Option<&[usize]>,
) -> Vec<EncodedFrame<ServerFrame>> {
    match selected {
        Some(selected) => benchmark.encode_selected_rows(selected),
        None => benchmark.encode_typed_rows(),
    }
}

fn decoded_rows(frames: &[EncodedFrame<ServerFrame>], schema: &RowSchema) -> Vec<SubscriptionRows> {
    let mut rows = Vec::with_capacity(frames.len());
    for frame in frames {
        let verified = VerifiedFrame::verify(frame.bytes().clone(), &SessionLimits::DEFAULT)
            .assured("the benchmark encoder produces valid frames");
        let ServerMessage::Event(ServerEvent::SubscriptionRows(batch)) =
            ServerMessage::decode(&verified).assured("a benchmark frame decodes")
        else {
            panic!("the benchmark produces Row events");
        };
        batch
            .batch()
            .conform(schema)
            .assured("the benchmark rows follow the schema");
        rows.push(batch);
    }
    rows
}

#[derive(Debug)]
enum OwnedCell {
    Null,
    Redacted,
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
}

impl OwnedCell {
    fn byte_len(&self) -> usize {
        match self {
            Self::Null | Self::Redacted => 0,
            Self::I64(value) => value.to_ne_bytes().len(),
            Self::Text(value) => value.len(),
            Self::Bytes(value) => value.len(),
        }
    }
}

fn materialize(rows: &[SubscriptionRows]) -> Vec<Vec<OwnedCell>> {
    let mut result = Vec::new();
    for frame in rows {
        for row in frame.batch().rows() {
            let mut cells = Vec::with_capacity(row.len());
            for cell in row.iter() {
                let owned = match cell {
                    CellView::Null => OwnedCell::Null,
                    CellView::Redacted => OwnedCell::Redacted,
                    CellView::I64(value) => OwnedCell::I64(value),
                    CellView::String(value) => OwnedCell::Text(value.to_owned()),
                    CellView::Bytes(value) => OwnedCell::Bytes(value.to_vec()),
                    _ => panic!("the fixed workloads use string, bytes, i64, null and redaction"),
                };
                cells.push(owned);
            }
            result.push(cells);
        }
    }
    result
}

fn run_case(
    name: &'static str,
    benchmark: SubscriptionRowBenchmark,
    source_rows: usize,
    selected: Option<Vec<usize>>,
) -> Case {
    let selection = selected.as_deref();
    let selected_rows = match &selected {
        Some(selected) => selected.len(),
        None => source_rows,
    };
    let frames = encode(&benchmark, selection);
    let wire_bytes = frames.iter().map(EncodedFrame::len).sum();
    let unique_frame = encode(&benchmark, selection)
        .into_iter()
        .next()
        .assured("every workload selects at least one row");
    let unique_bytes = unique_frame.into_bytes();
    let data_pointer = unique_bytes.as_ptr();
    let websocket_bytes = Vec::from(unique_bytes);
    let websocket_data_pointer_reused = websocket_bytes.as_ptr() == data_pointer;
    let rows = decoded_rows(&frames, benchmark.schema());
    let actual_rows: usize = rows.iter().map(|batch| batch.batch().len()).sum();
    assert_eq!(actual_rows, selected_rows);
    let owned = materialize(&rows);
    let owned_bytes: usize = owned
        .iter()
        .flat_map(|row| row.iter())
        .map(OwnedCell::byte_len)
        .sum();
    assert!(owned_bytes > 0);

    let mut stages = BTreeMap::new();
    stages.insert(
        "arrow_to_wire",
        measure(|| drop(black_box(encode(&benchmark, selection)))),
    );
    stages.insert(
        "frame_copy",
        measure(|| {
            for frame in &frames {
                let mut buffer = BytesMut::with_capacity(frame.len());
                buffer.extend_from_slice(frame.bytes());
                black_box(buffer);
            }
        }),
    );
    stages.insert(
        "grpc_small_frame_detach",
        measure(|| {
            for frame in &frames {
                let received = if frame.len() < 64 * 1024 {
                    Bytes::copy_from_slice(frame.bytes())
                } else {
                    frame.bytes().clone()
                };
                black_box(received);
            }
        }),
    );
    stages.insert(
        "websocket_owned_message",
        measure(|| {
            for frame in &frames {
                black_box(Vec::from(frame.bytes().clone()));
            }
        }),
    );
    stages.insert(
        "verify_structure_utf8",
        measure(|| {
            for frame in &frames {
                black_box(
                    VerifiedFrame::<ServerFrame>::verify(
                        frame.bytes().clone(),
                        &SessionLimits::DEFAULT,
                    )
                    .assured("a benchmark frame verifies"),
                );
            }
        }),
    );
    stages.insert(
        "decode_and_check",
        measure(|| drop(black_box(decoded_rows(&frames, benchmark.schema())))),
    );
    stages.insert(
        "borrowed_selective",
        measure(|| {
            for batch in &rows {
                let first = batch.batch().row(0).assured("every frame has a row");
                black_box(first.get(2).assured("every row has a detail cell"));
            }
        }),
    );
    stages.insert(
        "borrowed_full_scan",
        measure(|| {
            let mut seen = 0;
            for batch in &rows {
                for row in batch.batch().rows() {
                    for cell in row.iter() {
                        black_box(cell);
                        seen += 1;
                    }
                }
            }
            black_box(seen);
        }),
    );
    stages.insert(
        "owning_materialization",
        measure(|| drop(black_box(materialize(&rows)))),
    );
    stages.insert(
        "presentation_json_text",
        measure(|| {
            for batch in &rows {
                drop(black_box(
                    batch
                        .batch()
                        .display_lines(benchmark.schema())
                        .assured("the benchmark batch renders"),
                ));
            }
        }),
    );

    Case {
        name,
        source_rows,
        selected_rows,
        rows_per_frame: benchmark.rows_per_frame(),
        schema_fields: benchmark.schema().fields.len(),
        frame_count: frames.len(),
        wire_bytes,
        websocket_data_pointer_reused,
        stages,
        retention: retention(&benchmark, selection),
    }
}

fn command_output(program: &str, args: &[&str]) -> String {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .assured("git and rustc are installed for repository benchmarks");
    assert!(output.status.success(), "{program} failed");
    String::from_utf8(output.stdout)
        .assured("git and rustc print UTF-8")
        .trim()
        .to_owned()
}

fn main() {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/client-wire-cost.json"));
    let rows = |count| NonZeroUsize::new(count).assured("every benchmark row count is nonzero");
    let cases = vec![
        run_case(
            "task01_alternating",
            SubscriptionRowBenchmark::new(rows(100), rows(1024)),
            100,
            None,
        ),
        run_case(
            "narrow_batched",
            SubscriptionRowBenchmark::batched(rows(1024), rows(16)),
            1024,
            None,
        ),
        run_case(
            "wide_nullable_redacted",
            SubscriptionRowBenchmark::wide(rows(100), rows(64)),
            100,
            None,
        ),
        run_case(
            "large_batch",
            SubscriptionRowBenchmark::batched(rows(4096), rows(1024)),
            4096,
            None,
        ),
        run_case(
            "byte_limited_batch",
            SubscriptionRowBenchmark::batched(rows(256), rows(16384)),
            256,
            None,
        ),
        run_case(
            "selected_quarter",
            SubscriptionRowBenchmark::batched(rows(1024), rows(16)),
            1024,
            Some((0..1024).step_by(4).collect()),
        ),
    ];
    let report = Report {
        schema_version: 1,
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        rustc: command_output("rustc", &["-Vv"]),
        samples_per_stage: SAMPLES,
        cases,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).assured("the benchmark output directory is writable");
    }
    fs::write(
        &output,
        serde_json::to_vec_pretty(&report).assured("the report serializes"),
    )
    .assured("the benchmark output file is writable");
    eprintln!("client wire cost report: {}", output.display());
}
