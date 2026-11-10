//! List conversion probes for the VM Criterion harness.
//!
//! An element conversion converts the child values of a list and keeps its offsets and validity,
//! and a container conversion keeps the element values and only rebuilds or slices the layout
//! around them. Both run over eight-element lists at 64 and 4,096 rows.

use arrow_array::FixedSizeListArray;
use meticulous::OptionExt as _;

use super::*;

/// The elements every generated list holds.
const ELEMENTS_PER_LIST: usize = 8;

fn item(data_type: DataType) -> StdArc<Field> {
    StdArc::new(Field::new("item", data_type, false))
}

fn list_conversion_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![
        Field::new("readings", DataType::List(item(DataType::Int64)), false),
        Field::new(
            "window",
            DataType::FixedSizeList(item(DataType::Int64), 8),
            false,
        ),
    ]))
}

/// `rows` lists of eight elements, as a `VEC<I64>` and as an `ARRAY<I64, 8>` over the same values.
fn list_conversion_batch(rows: usize) -> TypedBatch {
    let element_count = rows
        .checked_mul(ELEMENTS_PER_LIST)
        .assured("a benchmark batch of at most 4,096 lists of eight elements fits in memory");
    let elements: arrow_array::ArrayRef = StdArc::new(Int64Array::from_iter_values(
        (0..element_count).map(benchmark_row_i64),
    ));
    let readings = ListArray::try_new(
        item(DataType::Int64),
        OffsetBuffer::from_lengths(std::iter::repeat_n(ELEMENTS_PER_LIST, rows)),
        StdArc::clone(&elements),
        None,
    )
    .assured("every list holds eight of the generated elements");
    let window = FixedSizeListArray::try_new(item(DataType::Int64), 8, elements, None)
        .assured("the generated elements fill every eight-element array");
    TypedBatch::try_new(
        list_conversion_schema(),
        vec![
            TypedArray::Generic(StdArc::new(readings)),
            TypedArray::Generic(StdArc::new(window)),
        ],
    )
    .assured("the list inputs match the declared VEC and ARRAY fields")
}

pub(super) fn list_conversion_benches(c: &mut Criterion) {
    let runtime = benchmark_runtime();
    let element = compile_numeric_program(
        "SET widened = input.readings AS VEC<F64>",
        list_conversion_schema(),
        &[("widened", DataType::List(item(DataType::Float64)))],
    );
    let container = compile_numeric_program(
        "SET fixed = input.readings AS ARRAY<I64, 8>, flexible = input.window AS VEC<I64>",
        list_conversion_schema(),
        &[
            ("fixed", DataType::FixedSizeList(item(DataType::Int64), 8)),
            ("flexible", DataType::List(item(DataType::Int64))),
        ],
    );
    let mut group = c.benchmark_group("vm_list_conversions");
    for rows in [64, 4_096] {
        let batch = list_conversion_batch(rows);
        for (name, program) in [("element", &element), ("container", &container)] {
            runtime
                .execute(program, &batch)
                .assured("the benchmark fixture and compiled output schema agree");
            #[cfg(feature = "benchmark-allocations")]
            allocation_probe::measure(&runtime, name, &rows.to_string(), program, &batch);
            let elements = rows
                .checked_mul(ELEMENTS_PER_LIST)
                .assured("a benchmark batch of at most 4,096 lists of eight elements fits");
            group.throughput(Throughput::Elements(elements.arch_into()));
            group.bench_with_input(BenchmarkId::new(name, rows), &batch, |bencher, input| {
                bencher.iter(|| {
                    black_box(
                        runtime
                            .execute(black_box(program), black_box(input))
                            .assured("the validated benchmark fixture executes successfully"),
                    )
                });
            });
        }
    }
    group.finish();
}
