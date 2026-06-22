use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use nervix_nspl::vm_program::parse_program;
use nervix_vm::{
    CompileBinding, CompileOptions, TypedArray, TypedBatch, compile_program_for_bindings,
    compile_program_with_options_for_bindings, execute_program,
};

fn arithmetic_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("left", DataType::Int64, true),
        Field::new("right", DataType::Int64, true),
        Field::new("divisor", DataType::Int64, true),
        Field::new("keep", DataType::Boolean, true),
    ]))
}

fn arithmetic_output_schema() -> Arc<Schema> {
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
    Arc::new(Schema::new(fields))
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

fn string_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("primary", DataType::Utf8, true),
        Field::new("fallback", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("needle", DataType::Utf8, true),
        Field::new("prefix", DataType::Utf8, true),
        Field::new("suffix", DataType::Utf8, true),
    ]))
}

fn string_output_schema() -> Arc<Schema> {
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
    Arc::new(Schema::new(fields))
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

fn execute_benches(c: &mut Criterion) {
    let arithmetic_program = parse_program(
        "SET input.total = input.left + input.right, input.quotient = (input.left + input.right) \
         / input.divisor, input.magnitude = abs(input.left - input.right) WHERE input.keep;",
    )
    .expect("benchmark program must parse");
    let arithmetic_input_schema = arithmetic_schema();
    let arithmetic_output_schema = arithmetic_output_schema();
    let arithmetic_compiled = compile_program_for_bindings(
        &arithmetic_program,
        arithmetic_output_schema.clone(),
        [CompileBinding::writable(
            "input",
            arithmetic_input_schema.clone(),
        )],
    )
    .expect("optimized benchmark must compile");
    let arithmetic_unoptimized = compile_program_with_options_for_bindings(
        &arithmetic_program,
        arithmetic_output_schema,
        [CompileBinding::writable("input", arithmetic_input_schema)],
        CompileOptions {
            optimize_temp_registers: false,
            ..CompileOptions::default()
        },
    )
    .expect("unoptimized benchmark must compile");
    let arithmetic_batch = arithmetic_batch(8_192);

    let string_program = parse_program(
        "SET input.chosen = coalesce(input.primary, input.fallback), input.was_null = \
         is_null(input.primary), input.maybe = nullif(input.primary, input.fallback), input.has = \
         contains(input.text, input.needle), input.starts = starts_with(input.text, \
         input.prefix), input.ends = ends_with(input.text, input.suffix);",
    )
    .expect("benchmark program must parse");
    let string_input_schema = string_schema();
    let string_output_schema = string_output_schema();
    let string_compiled = compile_program_for_bindings(
        &string_program,
        string_output_schema.clone(),
        [CompileBinding::writable(
            "input",
            string_input_schema.clone(),
        )],
    )
    .expect("optimized benchmark must compile");
    let string_unoptimized = compile_program_with_options_for_bindings(
        &string_program,
        string_output_schema,
        [CompileBinding::writable("input", string_input_schema)],
        CompileOptions {
            optimize_temp_registers: false,
            ..CompileOptions::default()
        },
    )
    .expect("unoptimized benchmark must compile");
    let string_batch = string_batch(8_192);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("benchmark runtime must build");

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

criterion_group!(benches, execute_benches);
criterion_main!(benches);
