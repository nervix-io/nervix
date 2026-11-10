//! Measurements of fixed and ragged list arithmetic through compiled VM programs.
//!
//! Layer: benchmark harness.
//!
//! - **Owns.** Bounded list shapes and the measured end-to-end VM calls for those shapes.
//! - **Depends on.** The public VM compiler, Arrow arrays and the benchmark executor.
//! - **Must not know.** Server graph ownership or a selected SIMD backend.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "measures public list arithmetic execution")
)]

use arrow_array::FixedSizeListArray;

use super::*;

pub(super) fn benches(criterion: &mut Criterion) {
    let runtime = benchmark_runtime();
    let mut group = criterion.benchmark_group("list_arithmetic");
    for width in [64_usize, 4_096] {
        for fixed in [true, false] {
            let child = StdArc::new(Field::new("item", DataType::Int64, false));
            let data_type = if fixed {
                DataType::FixedSizeList(
                    child.clone(),
                    i32::try_from(width).assured("the benchmark widths fit i32"),
                )
            } else {
                DataType::List(child.clone())
            };
            let source = StdArc::new(Schema::new(vec![Field::new(
                "items",
                data_type.clone(),
                false,
            )]));
            let lengths = [width, width / 2, width, 0];
            let lengths = if fixed { [width; 4] } else { lengths };
            let count: usize = lengths.iter().sum();
            let values = StdArc::new(Int64Array::from_iter_values((0..count).map(|index| {
                i64::try_from(index % 127).assured("the benchmark values are below 127")
            })));
            let items: arrow_array::ArrayRef = if fixed {
                StdArc::new(FixedSizeListArray::new(
                    child,
                    i32::try_from(width).assured("the fixed widths fit i32"),
                    values,
                    None,
                ))
            } else {
                StdArc::new(ListArray::new(
                    child,
                    OffsetBuffer::from_lengths(lengths),
                    values,
                    None,
                ))
            };
            let batch = TypedBatch::try_new(source.clone(), vec![TypedArray::Generic(items)])
                .verified("the benchmark matches its schema");
            for (name, expression) in [
                ("add", "list_add(input.items, input.items)"),
                ("scalar", "list_mul_scalar(input.items, 2)"),
                ("absolute", "list_abs(input.items)"),
            ] {
                let output = StdArc::new(Schema::new(vec![Field::new(
                    "result",
                    data_type.clone(),
                    false,
                )]));
                let program = parse_program(&format!("SET result = {expression}"))
                    .verified("the benchmark expression is valid Plexus");
                let compiled = compile_program_with_options_for_bindings(
                    &program,
                    output,
                    [CompileBinding::writable("input", source.clone())],
                    CompileOptions::default(),
                )
                .verified("the benchmark has exact numeric types");
                let compiled = Arc::new(compiled);
                assert!(runtime.execute(&compiled, &batch).is_ok());
                let container = if fixed { "fixed" } else { "ragged" };
                group.throughput(Throughput::Elements(
                    u64::try_from(count).assured("bounded benchmark lane count"),
                ));
                group.bench_with_input(
                    BenchmarkId::new(format!("{container}/{name}"), width),
                    &width,
                    |bencher, _| {
                        bencher.iter(|| runtime.execute(black_box(&compiled), black_box(&batch)))
                    },
                );
            }
        }
    }
    group.finish();
}
