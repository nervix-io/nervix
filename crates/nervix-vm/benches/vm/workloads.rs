//! Input-shape probes for the existing VM Criterion harness.
//!
//! These cases hold the compiled expression fixed while changing the Arrow carrier. They make
//! costs hidden by the usual contiguous, ASCII, medium-sized batches visible during A/B runs.

use super::*;

const INLINE_ROWS: usize = SPAWN_BLOCKING_ROW_THRESHOLD;

fn sliced_batch(batch: &TypedBatch, offset: usize, rows: usize) -> TypedBatch {
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let sliced = column.to_array_ref().slice(offset, rows);
            TypedArray::try_from_array_ref(sliced)
                .assured("a slice retains the typed Arrow column's declared type")
        })
        .collect();
    TypedBatch::try_new(batch.schema().clone(), columns)
        .assured("every sliced column has the requested row count")
}

fn search_batch(
    rows: usize,
    text_bytes: usize,
    unicode: bool,
    null_every_other: bool,
) -> TypedBatch {
    let padding = if unicode {
        "é".repeat(text_bytes / 2)
    } else {
        "a".repeat(text_bytes)
    };
    let text = StringArray::from_iter((0..rows).map(|row| {
        if null_every_other && row % 2 == 1 {
            None
        } else {
            Some(format!("{padding}needle7"))
        }
    }));
    let pattern = StringArray::from_iter((0..rows).map(|_| Some("needle7")));
    TypedBatch::try_new(
        regex_schema(),
        vec![TypedArray::Utf8(text), TypedArray::Utf8(pattern)],
    )
    .assured("the search inputs match the declared STRING fields")
}

fn repeat_batch(rows: usize, copies: i64) -> TypedBatch {
    let text = StringArray::from_iter((0..rows).map(|_| Some("éa")));
    let count = Int64Array::from_iter((0..rows).map(|_| Some(copies)));
    TypedBatch::try_new(
        StdArc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ])),
        vec![TypedArray::Utf8(text), TypedArray::Int64(count)],
    )
    .assured("the repeat inputs match the declared text and count fields")
}

fn repeat_program() -> Arc<CompiledProgram> {
    let input = repeat_batch(1, 1);
    compile_numeric_program(
        "SET repeated = repeat(input.text, input.count)",
        input.schema().clone(),
        &[("repeated", DataType::Utf8)],
    )
}

fn measure_batch(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    runtime: &tokio::runtime::Runtime,
    name: &str,
    shape: &str,
    program: &Arc<CompiledProgram>,
    batch: &TypedBatch,
) {
    runtime
        .block_on(execute_benchmark_program(program, batch))
        .assured("the benchmark fixture and compiled output schema agree");
    #[cfg(feature = "benchmark-allocations")]
    allocation_probe::measure(runtime, name, shape, program, batch);
    group.throughput(Throughput::Elements(batch.row_count().arch_into()));
    group.bench_with_input(BenchmarkId::new(name, shape), batch, |bencher, input| {
        bencher.iter(|| {
            black_box(
                runtime
                    .block_on(execute_benchmark_program(
                        black_box(program),
                        black_box(input),
                    ))
                    .assured("the validated benchmark fixture executes successfully"),
            )
        });
    });
}

pub(super) fn workload_shape_benches(c: &mut Criterion) {
    let runtime = benchmark_runtime();
    let arithmetic = compile_arithmetic(CompileOptions::default());
    let search = compile_multi_pattern_search(false);
    let repeat = repeat_program();
    let list = compile_ragged_collection();
    let mut group = c.benchmark_group("vm_workload_shape");

    // The existing 64–65,536 sweep remains the main throughput curve. These three points expose
    // fixed per-batch cost and the exact transition from inline execution to the blocking pool.
    for rows in [1, 8, INLINE_ROWS + 1] {
        let batch = arithmetic_batch(rows);
        measure_batch(
            &mut group,
            &runtime,
            "arithmetic_rows",
            &rows.to_string(),
            &arithmetic,
            &batch,
        );
    }

    let arithmetic_plain = arithmetic_batch(INLINE_ROWS);
    let arithmetic_sliced = sliced_batch(&arithmetic_batch(INLINE_ROWS + 17), 17, INLINE_ROWS);
    measure_batch(
        &mut group,
        &runtime,
        "arithmetic_layout",
        "contiguous",
        &arithmetic,
        &arithmetic_plain,
    );
    measure_batch(
        &mut group,
        &runtime,
        "arithmetic_layout",
        "sliced_17",
        &arithmetic,
        &arithmetic_sliced,
    );

    let ragged_plain = collection_batch(INLINE_ROWS);
    let ragged_sliced = sliced_batch(&collection_batch(INLINE_ROWS + 17), 17, INLINE_ROWS);
    measure_batch(
        &mut group,
        &runtime,
        "ragged_layout",
        "contiguous",
        &list,
        &ragged_plain,
    );
    measure_batch(
        &mut group,
        &runtime,
        "ragged_layout",
        "sliced_17",
        &list,
        &ragged_sliced,
    );

    for bytes in [32, 1_024] {
        for unicode in [false, true] {
            for null_every_other in [false, true] {
                let batch = search_batch(INLINE_ROWS, bytes, unicode, null_every_other);
                let alphabet = if unicode { "unicode" } else { "ascii" };
                let density = if null_every_other {
                    "half_null"
                } else {
                    "no_null"
                };
                let shape = format!("{alphabet}_{bytes}_{density}");
                measure_batch(&mut group, &runtime, "search_text", &shape, &search, &batch);
            }
        }
    }

    for copies in [1, 8, 64] {
        let batch = repeat_batch(INLINE_ROWS, copies);
        measure_batch(
            &mut group,
            &runtime,
            "repeat_expansion",
            &copies.to_string(),
            &repeat,
            &batch,
        );
    }
    group.finish();
}
