//! Exact Parquet file measurement for the Iceberg emitter's declared BATCH byte limit.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Counting the bytes a projected Arrow batch writes with the same Parquet properties
//!   as the table data-file writer, including the footer.
//! - **Depends on.** Arrow batches and the Parquet writer.
//! - **Must not know.** Catalog commits, staging lifetimes, acknowledgements or graph state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::io::{self, Write};

use arrow_array::RecordBatch;
use parquet::{arrow::ArrowWriter, errors::Result, file::properties::WriterProperties};

/// A sink for the real Parquet encoding that retains only its byte count. The encoding itself may
/// buffer a row group, but measuring does not keep a second copy of its finished file in memory.
#[derive(Default)]
struct CountedBytes(u64);

impl Write for CountedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("Parquet write length exceeds u64"))?;
        self.0 = self
            .0
            .checked_add(written)
            .ok_or_else(|| io::Error::other("Parquet file length exceeds u64"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The complete bytes the table's default Parquet writer would write for one data file. The
/// batch has already been projected onto the current table schema, including its field IDs.
pub(super) fn measured_file_size(batch: &RecordBatch) -> Result<u64> {
    let mut writer = ArrowWriter::try_new(
        CountedBytes::default(),
        batch.schema(),
        Some(WriterProperties::builder().build()),
    )?;
    writer.write(batch)?;
    Ok(writer.into_inner()?.0)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_schema::{DataType, Field, Schema};
    use nervix_primitives::sync::StdArc;
    use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

    use super::measured_file_size;

    /// The staged Arrow IPC file preserves every mapped row, and the Parquet measurement is the
    /// exact size of the file that will be emitted after loading those rows.
    #[test]
    fn bolero_staged_iceberg_file_encoding_round_trips_and_measures_exactly() {
        let rows = bolero::generator::produce_with::<Vec<Vec<u8>>>().len(1_usize..=8);
        bolero::check!()
            .with_iterations(256)
            .with_max_len(512)
            .with_generator(rows)
            .for_each(|values| {
                let schema = StdArc::new(Schema::new(vec![
                    Field::new("user_id", DataType::Int64, false),
                    Field::new("action", DataType::Utf8, false),
                ]));
                let ids = (0..values.len())
                    .map(|id| i64::try_from(id).expect("at most eight generated rows"))
                    .collect::<Vec<_>>();
                let actions = values
                    .iter()
                    .map(|value| String::from_utf8_lossy(value).into_owned())
                    .collect::<Vec<_>>();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        StdArc::new(Int64Array::from(ids)),
                        StdArc::new(StringArray::from(actions)),
                    ],
                )
                .expect("generated rows match their staged schema");

                let mut ipc = Vec::new();
                let mut staged = StreamWriter::try_new(&mut ipc, &schema)
                    .expect("the staged schema is an Arrow IPC schema");
                staged.write(&batch).expect("generated rows encode to IPC");
                staged.finish().expect("the staged IPC stream closes");
                drop(staged);
                let loaded = StreamReader::try_new(Cursor::new(ipc), None)
                    .expect("the staged IPC stream reopens")
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .expect("every staged batch decodes");
                assert_eq!(loaded, vec![batch.clone()]);

                let measured = measured_file_size(&loaded[0])
                    .expect("the decoded staged rows encode as Parquet");
                let mut writer = ArrowWriter::try_new(
                    Vec::new(),
                    loaded[0].schema(),
                    Some(WriterProperties::builder().build()),
                )
                .expect("the projected schema can write a Parquet file");
                writer.write(&loaded[0]).expect("the rows write to Parquet");
                let actual = u64::try_from(writer.into_inner().expect("the footer closes").len())
                    .expect("a bounded test file fits u64");
                assert_eq!(measured, actual);
            });
    }
}
