use std::sync::Arc as StdArc;

use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use nervix_nspl::vm_program::parse_program;
use nervix_vm::{
    CompileBinding, CompileOptions, CompiledProgram, TypedArray, TypedBatch,
    compile_program_with_options_for_bindings, execute_program,
};
use triomphe::Arc;

/// Row counts spanning `SPAWN_BLOCKING_ROW_THRESHOLD` so the sweep shows both the
/// amortization curve below it and the cost of the blocking hop above it.
const SWEEP_ROW_COUNTS: [usize; 6] = [64, 256, 1_024, 4_096, 16_384, 65_536];

fn arithmetic_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, true),
        Field::new("right", DataType::Int64, true),
        Field::new("divisor", DataType::Int64, true),
        Field::new("keep", DataType::Boolean, true),
    ]))
}

fn arithmetic_output_schema() -> StdArc<Schema> {
    let mut fields = arithmetic_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Int64, true),
        Field::new("quotient", DataType::Int64, true),
        Field::new("magnitude", DataType::Int64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn arithmetic_batch(row_count: usize) -> TypedBatch {
    let left = Int64Array::from_iter((0..row_count).map(|row| Some((row % 97) as i64 + 1)));
    let right = Int64Array::from_iter((0..row_count).map(|row| Some((row % 13) as i64 + 3)));
    let divisor = Int64Array::from_iter((0..row_count).map(|row| Some((row % 7) as i64 + 1)));
    let keep = BooleanArray::from_iter((0..row_count).map(|row| Some(row % 3 != 0)));

    TypedBatch::try_new(
        arithmetic_schema(),
        vec![
            TypedArray::Int64(left),
            TypedArray::Int64(right),
            TypedArray::Int64(divisor),
            TypedArray::Boolean(keep),
        ],
    )
    .expect("benchmark batch must build")
}

fn string_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("primary", DataType::Utf8, true),
        Field::new("fallback", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("needle", DataType::Utf8, true),
        Field::new("prefix", DataType::Utf8, true),
        Field::new("suffix", DataType::Utf8, true),
    ]))
}

fn string_output_schema() -> StdArc<Schema> {
    let mut fields = string_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("chosen", DataType::Utf8, true),
        Field::new("was_null", DataType::Boolean, true),
        Field::new("maybe", DataType::Utf8, true),
        Field::new("has", DataType::Boolean, true),
        Field::new("starts", DataType::Boolean, true),
        Field::new("ends", DataType::Boolean, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn string_batch(row_count: usize) -> TypedBatch {
    let primary = StringArray::from_iter((0..row_count).map(|row| {
        if row % 5 == 0 {
            None
        } else {
            Some(format!("value-{row}"))
        }
    }));
    let fallback =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("fallback-{row}"))));
    let text =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("prefix-{row}-suffix"))));
    let needle = StringArray::from_iter((0..row_count).map(|_| Some("-")));
    let prefix = StringArray::from_iter((0..row_count).map(|_| Some("prefix-")));
    let suffix = StringArray::from_iter((0..row_count).map(|_| Some("-suffix")));

    TypedBatch::try_new(
        string_schema(),
        vec![
            TypedArray::Utf8(primary),
            TypedArray::Utf8(fallback),
            TypedArray::Utf8(text),
            TypedArray::Utf8(needle),
            TypedArray::Utf8(prefix),
            TypedArray::Utf8(suffix),
        ],
    )
    .expect("benchmark batch must build")
}

fn long_tail_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("from_chars", DataType::Utf8, true),
        Field::new("to_chars", DataType::Utf8, true),
        Field::new("fill", DataType::Utf8, true),
        Field::new("delimiter", DataType::Utf8, true),
        Field::new("needle", DataType::Utf8, true),
        Field::new("count", DataType::Int64, true),
        Field::new("width", DataType::Int64, true),
        Field::new("start", DataType::Int64, true),
        Field::new("length", DataType::Int64, true),
        Field::new("integer", DataType::Int64, true),
        Field::new("numeric", DataType::Float64, true),
    ]))
}

fn long_tail_output_schema() -> StdArc<Schema> {
    let mut fields = long_tail_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("translated", DataType::Utf8, true),
        Field::new("hexed", DataType::Utf8, true),
        Field::new("lefted", DataType::Utf8, true),
        Field::new("righted", DataType::Utf8, true),
        Field::new("padded", DataType::Utf8, true),
        Field::new("joined", DataType::Utf8, true),
        Field::new("piece", DataType::Utf8, true),
        Field::new("digest", DataType::Utf8, true),
        Field::new("titled", DataType::Utf8, true),
        Field::new("reversed", DataType::Utf8, true),
        Field::new("part", DataType::Utf8, true),
        Field::new("position", DataType::Int64, true),
        Field::new("cosine", DataType::Float64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn long_tail_batch(row_count: usize) -> TypedBatch {
    let text =
        StringArray::from_iter((0..row_count).map(|row| Some(format!("alpha-{row}-beta-gamma"))));
    let from_chars = StringArray::from_iter((0..row_count).map(|_| Some("abg-")));
    let to_chars = StringArray::from_iter((0..row_count).map(|_| Some("ABG_")));
    let fill = StringArray::from_iter((0..row_count).map(|_| Some("xy")));
    let delimiter = StringArray::from_iter((0..row_count).map(|_| Some("-")));
    let needle = StringArray::from_iter((0..row_count).map(|_| Some("beta")));
    let count = Int64Array::from_iter((0..row_count).map(|_| Some(8)));
    let width = Int64Array::from_iter((0..row_count).map(|_| Some(32)));
    let start = Int64Array::from_iter((0..row_count).map(|_| Some(3)));
    let length = Int64Array::from_iter((0..row_count).map(|_| Some(12)));
    let integer = Int64Array::from_iter((0..row_count).map(|row| Some(row as i64 + 1)));
    let numeric = Float64Array::from_iter((0..row_count).map(|row| Some((row % 100) as f64)));

    TypedBatch::try_new(
        long_tail_schema(),
        vec![
            TypedArray::Utf8(text),
            TypedArray::Utf8(from_chars),
            TypedArray::Utf8(to_chars),
            TypedArray::Utf8(fill),
            TypedArray::Utf8(delimiter),
            TypedArray::Utf8(needle),
            TypedArray::Int64(count),
            TypedArray::Int64(width),
            TypedArray::Int64(start),
            TypedArray::Int64(length),
            TypedArray::Int64(integer),
            TypedArray::Float64(numeric),
        ],
    )
    .expect("long-tail benchmark batch must build")
}

fn compile_arithmetic(options: CompileOptions) -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.total = input.left + input.right, input.quotient = (input.left + input.right) \
         / input.divisor, input.magnitude = abs(input.left - input.right) WHERE input.keep;",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        arithmetic_output_schema(),
        [CompileBinding::writable("input", arithmetic_schema())],
        options,
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_string(options: CompileOptions) -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.chosen = coalesce(input.primary, input.fallback), input.was_null = \
         is_null(input.primary), input.maybe = nullif(input.primary, input.fallback), input.has = \
         contains(input.text, input.needle), input.starts = starts_with(input.text, \
         input.prefix), input.ends = ends_with(input.text, input.suffix);",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        string_output_schema(),
        [CompileBinding::writable("input", string_schema())],
        options,
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_long_tail() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.translated = translate(input.text, input.from_chars, input.to_chars), \
         input.hexed = to_hex(input.integer), input.lefted = left(input.text, input.count), \
         input.righted = right(input.text, input.count), input.padded = lpad(input.text, \
         input.width, input.fill), input.joined = concat(input.text, input.fill, input.text), \
         input.piece = substr(input.text, input.start, input.length), input.digest = \
         md5(input.text), input.titled = initcap(input.text), input.reversed = \
         reverse(input.text), input.part = split_part(input.text, input.delimiter, input.count), \
         input.position = strpos(input.text, input.needle), input.cosine = cos(input.numeric);",
    )
    .expect("long-tail benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        long_tail_output_schema(),
        [CompileBinding::writable("input", long_tail_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("long-tail benchmark program must compile")
}

/// Filters on a numeric comparison and writes text-case builtins, so the sweep also covers
/// the paths that evaluate row by row rather than through an Arrow kernel.
fn compile_numeric_compare() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.total = input.left + input.right, input.quotient = input.left / input.divisor, \
         input.magnitude = abs(input.left) WHERE input.left > input.divisor;",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        arithmetic_output_schema(),
        [CompileBinding::writable("input", arithmetic_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_text_case() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.chosen = upper(input.primary), input.was_null = is_null(input.primary), \
         input.maybe = lower(input.fallback), input.has = contains(input.text, input.needle), \
         input.starts = starts_with(input.text, input.prefix), input.ends = ends_with(input.text, \
         input.suffix);",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        string_output_schema(),
        [CompileBinding::writable("input", string_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn unoptimized_options() -> CompileOptions {
    CompileOptions {
        optimize_temp_registers: false,
        ..CompileOptions::default()
    }
}

fn benchmark_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime must build")
}

fn execute_benches(c: &mut Criterion) {
    let arithmetic_compiled = compile_arithmetic(CompileOptions::default());
    let arithmetic_unoptimized = compile_arithmetic(unoptimized_options());
    let arithmetic_batch = arithmetic_batch(8_192);

    let string_compiled = compile_string(CompileOptions::default());
    let string_unoptimized = compile_string(unoptimized_options());
    let string_batch = string_batch(8_192);
    let long_tail_compiled = compile_long_tail();
    let long_tail_batch = long_tail_batch(8_192);
    let runtime = benchmark_runtime();

    let mut group = c.benchmark_group("execute_program");
    group.bench_function("arithmetic_filter_optimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_program(
                black_box(&arithmetic_compiled),
                black_box(&arithmetic_batch),
            ))
        })
    });
    group.bench_function("arithmetic_filter_unoptimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_program(
                black_box(&arithmetic_unoptimized),
                black_box(&arithmetic_batch),
            ))
        })
    });
    group.bench_function("string_builtins_optimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_program(
                black_box(&string_compiled),
                black_box(&string_batch),
            ))
        })
    });
    group.bench_function("string_builtins_unoptimized_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_program(
                black_box(&string_unoptimized),
                black_box(&string_batch),
            ))
        })
    });
    group.bench_function("long_tail_builtins_8192", |b| {
        b.iter(|| {
            runtime.block_on(execute_program(
                black_box(&long_tail_compiled),
                black_box(&long_tail_batch),
            ))
        })
    });
    group.finish();
}

/// Sweeps batch size for the same programs so throughput is reported per row instead of
/// per batch. This is what shows whether feeding the VM larger batches keeps paying.
fn batch_size_sweep_benches(c: &mut Criterion) {
    let arithmetic_compiled = compile_arithmetic(CompileOptions::default());
    let string_compiled = compile_string(CompileOptions::default());
    let numeric_compare_compiled = compile_numeric_compare();
    let text_case_compiled = compile_text_case();
    let runtime = benchmark_runtime();

    let mut group = c.benchmark_group("execute_program_batch_size");
    for rows in SWEEP_ROW_COUNTS {
        group.throughput(Throughput::Elements(rows as u64));

        let batch = arithmetic_batch(rows);
        group.bench_with_input(
            BenchmarkId::new("arithmetic_filter", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    runtime.block_on(execute_program(
                        black_box(&arithmetic_compiled),
                        black_box(&batch),
                    ))
                })
            },
        );
        group.bench_with_input(BenchmarkId::new("numeric_compare", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&numeric_compare_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = string_batch(rows);
        group.bench_with_input(BenchmarkId::new("string_builtins", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&string_compiled),
                    black_box(&batch),
                ))
            })
        });
        group.bench_with_input(BenchmarkId::new("text_case", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&text_case_compiled),
                    black_box(&batch),
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, execute_benches, batch_size_sweep_benches);
criterion_main!(benches);
