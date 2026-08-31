use std::sync::Arc as StdArc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, StringArray, types::Int64Type,
};
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

fn float_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("left", DataType::Float64, true),
        Field::new("right", DataType::Float64, true),
        Field::new("divisor", DataType::Float64, true),
    ]))
}

fn float_arithmetic_output_schema() -> StdArc<Schema> {
    let mut fields = float_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Float64, true),
        Field::new("difference", DataType::Float64, true),
        Field::new("product", DataType::Float64, true),
        Field::new("quotient", DataType::Float64, true),
        Field::new("remainder", DataType::Float64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn nullable_cast_output_schema() -> StdArc<Schema> {
    let mut fields = float_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("left_int", DataType::Int64, true),
        Field::new("right_f32", DataType::Float32, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn float_batch(row_count: usize) -> TypedBatch {
    let left = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 17 != 0).then_some((row % 97) as f64 + 1.25)),
    );
    let right = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 19 != 0).then_some((row % 13) as f64 + 0.5)),
    );
    let divisor = Float64Array::from_iter(
        (0..row_count).map(|row| (row % 23 != 0).then_some((row % 7) as f64 + 1.0)),
    );

    TypedBatch::try_new(
        float_schema(),
        vec![
            TypedArray::Float64(left),
            TypedArray::Float64(right),
            TypedArray::Float64(divisor),
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
        Field::new("number", DataType::Int64, true),
        Field::new("numeric_text", DataType::Utf8, true),
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
        Field::new("trimmed", DataType::Utf8, true),
        Field::new("characters", DataType::Int64, true),
        Field::new("replaced", DataType::Utf8, true),
        Field::new("number_text", DataType::Utf8, true),
        Field::new("parsed_number", DataType::Int64, true),
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
        StringArray::from_iter((0..row_count).map(|row| Some(format!(" prefix-{row}-suffix "))));
    let needle = StringArray::from_iter((0..row_count).map(|_| Some("-")));
    let prefix = StringArray::from_iter((0..row_count).map(|_| Some("prefix-")));
    let suffix = StringArray::from_iter((0..row_count).map(|_| Some("-suffix")));
    let number = Int64Array::from_iter((0..row_count).map(|row| Some(row as i64)));
    let numeric_text = StringArray::from_iter((0..row_count).map(|row| Some(row.to_string())));

    TypedBatch::try_new(
        string_schema(),
        vec![
            TypedArray::Utf8(primary),
            TypedArray::Utf8(fallback),
            TypedArray::Utf8(text),
            TypedArray::Utf8(needle),
            TypedArray::Utf8(prefix),
            TypedArray::Utf8(suffix),
            TypedArray::Int64(number),
            TypedArray::Utf8(numeric_text),
        ],
    )
    .expect("benchmark batch must build")
}

fn list_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new(
            "values",
            DataType::List(StdArc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("index", DataType::Int64, true),
    ]))
}

fn list_output_schema() -> StdArc<Schema> {
    let mut fields = list_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend([
        Field::new("total", DataType::Int64, true),
        Field::new("first_value", DataType::Int64, true),
        Field::new("last_value", DataType::Int64, true),
        Field::new("nth_value", DataType::Int64, true),
        Field::new("value_count", DataType::Int64, true),
    ]);
    StdArc::new(Schema::new(fields))
}

fn list_batch(row_count: usize) -> TypedBatch {
    let values = ListArray::from_iter_primitive::<Int64Type, _, _>((0..row_count).map(|row| {
        (row % 7 != 0).then(|| {
            vec![
                Some(row as i64),
                Some(row as i64 + 1),
                (row % 5 != 0).then_some(row as i64 + 2),
                Some(row as i64 + 3),
            ]
        })
    }));
    let index = Int64Array::from_iter((0..row_count).map(|row| Some((row % 5) as i64)));

    TypedBatch::try_new(
        list_schema(),
        vec![
            TypedArray::Generic(StdArc::new(values) as ArrayRef),
            TypedArray::Int64(index),
        ],
    )
    .expect("benchmark batch must build")
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

fn compile_float_arithmetic() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.total = input.left + input.right, input.difference = input.left - input.right, \
         input.product = input.left * input.right, input.quotient = input.left / input.divisor, \
         input.remainder = input.left % input.divisor;",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        float_arithmetic_output_schema(),
        [CompileBinding::writable("input", float_schema())],
        CompileOptions::default(),
    )
    .map(Arc::new)
    .expect("benchmark program must compile")
}

fn compile_nullable_casts() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.left_int = input.left AS INT64, input.right_f32 = input.right AS FLOAT32;",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        nullable_cast_output_schema(),
        [CompileBinding::writable("input", float_schema())],
        CompileOptions::default(),
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

fn compile_text_transform() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.trimmed = trim(input.text), input.characters = length(input.text), \
         input.replaced = replace(input.text, input.needle, input.suffix), input.number_text = \
         input.number AS STRING, input.parsed_number = input.numeric_text AS INT64;",
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

fn compile_list() -> Arc<CompiledProgram> {
    let program = parse_program(
        "SET input.total = sum(input.values), input.first_value = first(input.values), \
         input.last_value = last(input.values), input.nth_value = nth(input.values, input.index), \
         input.value_count = count(input.values);",
    )
    .expect("benchmark program must parse");
    compile_program_with_options_for_bindings(
        &program,
        list_output_schema(),
        [CompileBinding::writable("input", list_schema())],
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
    group.finish();
}

/// Sweeps batch size for the same programs so throughput is reported per row instead of
/// per batch. This is what shows whether feeding the VM larger batches keeps paying.
fn batch_size_sweep_benches(c: &mut Criterion) {
    let arithmetic_compiled = compile_arithmetic(CompileOptions::default());
    let string_compiled = compile_string(CompileOptions::default());
    let numeric_compare_compiled = compile_numeric_compare();
    let float_arithmetic_compiled = compile_float_arithmetic();
    let nullable_casts_compiled = compile_nullable_casts();
    let text_transform_compiled = compile_text_transform();
    let list_compiled = compile_list();
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

        let batch = float_batch(rows);
        group.bench_with_input(BenchmarkId::new("float_arithmetic", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&float_arithmetic_compiled),
                    black_box(&batch),
                ))
            })
        });
        group.bench_with_input(
            BenchmarkId::new("nullable_kernel_casts", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    runtime.block_on(execute_program(
                        black_box(&nullable_casts_compiled),
                        black_box(&batch),
                    ))
                })
            },
        );

        let batch = string_batch(rows);
        group.bench_with_input(BenchmarkId::new("string_builtins", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&string_compiled),
                    black_box(&batch),
                ))
            })
        });
        group.bench_with_input(BenchmarkId::new("text_transform", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&text_transform_compiled),
                    black_box(&batch),
                ))
            })
        });

        let batch = list_batch(rows);
        group.bench_with_input(BenchmarkId::new("list_builtins", rows), &rows, |b, _| {
            b.iter(|| {
                runtime.block_on(execute_program(
                    black_box(&list_compiled),
                    black_box(&batch),
                ))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, execute_benches, batch_size_sweep_benches);
criterion_main!(benches);
