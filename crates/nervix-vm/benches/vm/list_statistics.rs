//! Statistics over one list in the VM Criterion harness.
//!
//! Each case runs one statistic over a batch of lists of 64 and of 4,096 `F64` values, so the
//! per-list cost of the centered moments, the quantile selection and the extreme scan reads
//! separately from the per-batch cost of the program around them.

use super::*;

/// The lists in every measured batch.
const LISTS_PER_BATCH: usize = 64;

/// The two list lengths each statistic is measured at.
const LIST_LENGTHS: [usize; 2] = [64, 4_096];

fn statistics_schema() -> StdArc<Schema> {
    StdArc::new(Schema::new(vec![Field::new(
        "values",
        DataType::List(StdArc::new(Field::new("item", DataType::Float64, false))),
        true,
    )]))
}

/// A batch of lists of `length` values each, in an order that leaves no list sorted.
fn statistics_batch(length: usize) -> TypedBatch {
    let mut values = Vec::with_capacity(LISTS_PER_BATCH * length);
    for list in 0..LISTS_PER_BATCH {
        for position in 0..length {
            let scattered = (list * 7_919 + position * 104_729) % 10_007;
            values.push(scattered.approx_into::<f64>() / 7.0);
        }
    }
    let lists = ListArray::new(
        StdArc::new(Field::new("item", DataType::Float64, false)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(length, LISTS_PER_BATCH)),
        StdArc::new(Float64Array::from(values)),
        None,
    );
    TypedBatch::try_new(
        statistics_schema(),
        vec![TypedArray::Generic(StdArc::new(lists))],
    )
    .assured("the lists match the declared VEC<F64> field")
}

pub(super) fn list_statistic_benches(c: &mut Criterion) {
    let runtime = benchmark_runtime();
    let mut group = c.benchmark_group("vm_list_statistics");
    for (name, expression, result) in [
        ("var_pop", "list_var_pop(input.values)", DataType::Float64),
        (
            "stddev_samp",
            "list_stddev_samp(input.values)",
            DataType::Float64,
        ),
        ("median", "list_median(input.values)", DataType::Float64),
        (
            "quantile_90",
            "list_quantile(input.values, 90)",
            DataType::Float64,
        ),
        ("argmin", "list_argmin(input.values)", DataType::Int64),
        ("argmax", "list_argmax(input.values)", DataType::Int64),
    ] {
        let program = compile_numeric_program(
            &format!("SET result = {expression}"),
            statistics_schema(),
            &[("result", result)],
        );
        for length in LIST_LENGTHS {
            let batch = statistics_batch(length);
            let elements = LISTS_PER_BATCH * length;
            group.throughput(Throughput::Elements(elements.arch_into()));
            group.bench_with_input(BenchmarkId::new(name, length), &length, |b, _| {
                b.iter(|| runtime.execute(black_box(&program), black_box(&batch)))
            });
        }
    }
    group.finish();
}
