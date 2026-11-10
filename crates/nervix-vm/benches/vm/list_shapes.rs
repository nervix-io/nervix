//! List shape timing and allocation fixtures.
//!
//! Layer: test harness.
//!
//! - **Owns.** Measuring reshape, fixed flatten and transpose over contiguous child buffers.
//! - **Depends on.** The public VM compiler, executor and Arrow arrays.
//! - **Must not know.** Server topology or inference execution.

use arrow_array::{Array, FixedSizeListArray};
use meticulous::OptionExt as _;

use super::*;

fn fixed_type(element: DataType, width: i32) -> DataType {
    DataType::FixedSizeList(StdArc::new(Field::new("item", element, false)), width)
}

fn matrix_batch(rows: usize) -> TypedBatch {
    let values = Float64Array::from_iter_values(
        (0..rows
            .checked_mul(512)
            .assured("the benchmark batch is bounded"))
            .map(benchmark_row_f64),
    );
    let inner = FixedSizeListArray::try_new(
        StdArc::new(Field::new("item", DataType::Float64, false)),
        8,
        StdArc::new(values),
        None,
    )
    .assured("the benchmark has eight values per inner array");
    let matrix = FixedSizeListArray::try_new(
        StdArc::new(Field::new("item", inner.data_type().clone(), false)),
        64,
        StdArc::new(inner),
        None,
    )
    .assured("the benchmark has 64 arrays per matrix");
    TypedBatch::try_new(
        StdArc::new(Schema::new(vec![Field::new(
            "matrix",
            matrix.data_type().clone(),
            false,
        )])),
        vec![TypedArray::Generic(StdArc::new(matrix))],
    )
    .assured("the benchmark's matrix matches its schema")
}

pub(super) fn list_shape_benches(criterion: &mut Criterion) {
    let runtime = benchmark_runtime();
    let mut group = criterion.benchmark_group("vm_list_shape");
    for rows in [1, VmInlineRowLimit::DEFAULT] {
        let batch = matrix_batch(rows);
        for (name, expression, output_type) in [
            (
                "reshape",
                "list_reshape(input.matrix, 8, 64)",
                fixed_type(fixed_type(DataType::Float64, 64), 8),
            ),
            (
                "flatten",
                "list_flatten(input.matrix)",
                fixed_type(DataType::Float64, 512),
            ),
            (
                "transpose",
                "list_transpose(input.matrix)",
                fixed_type(fixed_type(DataType::Float64, 64), 8),
            ),
        ] {
            let program = compile_numeric_program(
                &format!("SET shaped = {expression}"),
                batch.schema().clone(),
                &[("shaped", output_type)],
            );
            runtime
                .execute(&program, &batch)
                .assured("the validated shape fixture executes");
            #[cfg(feature = "benchmark-allocations")]
            allocation_probe::measure(&runtime, name, &rows.to_string(), &program, &batch);
            group.throughput(Throughput::Elements(rows.arch_into()));
            group.bench_with_input(BenchmarkId::new(name, rows), &batch, |bencher, input| {
                bencher.iter(|| {
                    black_box(
                        runtime
                            .execute(black_box(&program), black_box(input))
                            .assured("the validated shape fixture executes"),
                    )
                });
            });
        }
    }
    group.finish();
}
