//! Measures columnar JSON encoding beside serde's direct row serialization.
//!
//! Layer: benchmark harness, outside the product layer order.
//!
//! - **Owns.** Repeatable JSON encoding inputs and throughput measurements.
//! - **Depends on.** The columnar JSON engine, Arrow, Criterion and serde.
//! - **Must not know.** Nervix graph plans, connector lifecycle or cluster state.

use std::{hint::black_box, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{Field, Schema};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_columnar_json::{FieldNulls, JsonColumnSpec, JsonColumns, NestedNulls};
use serde::Serialize;

const ROWS: usize = 1_024;

#[derive(Serialize)]
struct ReferenceRow<'a> {
    id: u64,
    clean: &'a str,
    escaped: &'a str,
}

fn input_batch() -> RecordBatch {
    let clean = (0..ROWS)
        .map(|row| format!("plain Unicode café record {row}"))
        .collect::<Vec<_>>();
    let escaped = (0..ROWS)
        .map(|row| {
            if row % 4 == 0 {
                format!("quoted \" and slash \\ with newline \n record {row}")
            } else {
                format!("ordinary record {row}")
            }
        })
        .collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            0..u64::try_from(ROWS).assured("benchmark row count fits u64"),
        )),
        Arc::new(StringArray::from_iter_values(clean)),
        Arc::new(StringArray::from_iter_values(escaped)),
    ];
    let fields = [
        Field::new("id", columns[0].data_type().clone(), false),
        Field::new("clean", columns[1].data_type().clone(), false),
        Field::new("escaped", columns[2].data_type().clone(), false),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns)
        .assured("benchmark columns have equal row counts and declared Arrow types")
}

fn json_encode(criterion: &mut Criterion) {
    let batch = input_batch();
    let specs =
        ["id", "clean", "escaped"].map(|name| JsonColumnSpec::new(name, FieldNulls::Reject));
    let mut group = criterion.benchmark_group("json_encode_rows");
    group.throughput(Throughput::Elements(
        u64::try_from(ROWS).assured("benchmark row count fits u64"),
    ));
    group.bench_function("columnar", |b| {
        b.iter(|| {
            let columns = JsonColumns::new(black_box(&batch), &specs, NestedNulls::Reject)
                .assured("benchmark arrays have supported Arrow types");
            let mut output = Vec::with_capacity(128);
            for row in 0..ROWS {
                output.clear();
                columns
                    .write_row(row, &mut output)
                    .assured("benchmark fields are never null");
                black_box(&output);
            }
        });
    });
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .assured("benchmark id column is UInt64");
    let clean = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .assured("benchmark clean column is Utf8");
    let escaped = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .assured("benchmark escaped column is Utf8");
    group.bench_function("serde_rows", |b| {
        b.iter(|| {
            let mut output = Vec::with_capacity(128);
            for row in 0..ROWS {
                output.clear();
                serde_json::to_writer(
                    &mut output,
                    &ReferenceRow {
                        id: ids.value(row),
                        clean: clean.value(row),
                        escaped: escaped.value(row),
                    },
                )
                .assured("benchmark Vec writer and reference row cannot fail");
                black_box(&output);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, json_encode);
criterion_main!(benches);
