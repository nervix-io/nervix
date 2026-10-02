//! The ingestion watermarks of a batch's rows, carried as Arrow buffers of Unix nanoseconds.

use arrow_array::TimestampNanosecondArray;
use arrow_buffer::ScalarBuffer;
use nervix_models::{RemoteRuntimeRecordMetadata, Timestamp};
use nervix_simd_kernels::latest_instant;

use crate::runtime_schema::RuntimeRecordMetadata;

/// The low and high ingestion watermarks of every row of one batch, row for row with the batch's
/// payload.
///
/// Each column is the Arrow values buffer a non-null `TimestampNanosecondArray` holds: Unix
/// nanoseconds, shared rather than copied when the batch is cloned. A kernel reads the high
/// watermarks as one `i64` slice. A row's watermarks become a typed [`RuntimeRecordMetadata`] only
/// where that one row is addressed on its own.
#[derive(Debug, Clone)]
pub(crate) struct RecordMetadataColumns {
    low: ScalarBuffer<i64>,
    high: ScalarBuffer<i64>,
}

impl RecordMetadataColumns {
    /// The columns of `rows`, in order.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies an iterator of immutable row metadata")
    )]
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = RuntimeRecordMetadata>) -> Self {
        let rows = rows.into_iter();
        let mut low = Vec::with_capacity(rows.size_hint().0);
        let mut high = Vec::with_capacity(rows.size_hint().0);
        for metadata in rows {
            low.push(metadata.ingested_at_low_watermark().unix_nanos());
            high.push(metadata.ingested_at_high_watermark().unix_nanos());
        }
        Self::from_nanos(low, high)
    }

    /// The columns of every part, one after another.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies iteration over selected metadata parts \
                                   in this admitted batch")
    )]
    pub(crate) fn concat<'a>(parts: impl IntoIterator<Item = &'a Self>) -> Self {
        let mut low = Vec::new();
        let mut high = Vec::new();
        for part in parts {
            low.extend_from_slice(&part.low);
            high.extend_from_slice(&part.high);
        }
        Self::from_nanos(low, high)
    }

    pub(crate) fn from_nanos(low: Vec<i64>, high: Vec<i64>) -> Self {
        Self {
            low: ScalarBuffer::from(low),
            high: ScalarBuffer::from(high),
        }
    }

    /// Ingest rows begin with the same selected instant in both watermark columns.
    pub(crate) fn from_ingestion_nanos(instants: Vec<i64>) -> Self {
        let instants = ScalarBuffer::from(instants);
        Self {
            low: instants.clone(),
            high: instants,
        }
    }

    /// Uses a selected timestamp column as both initial watermarks without copying its values.
    pub(crate) fn from_timestamp_column(instants: &TimestampNanosecondArray) -> Self {
        let values = instants.values().clone();
        Self {
            low: values.clone(),
            high: values,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.high.len()
    }

    /// The watermarks of `row`, or `None` past the last row.
    pub(crate) fn row(&self, row: usize) -> Option<RuntimeRecordMetadata> {
        let low = self.low.get(row)?;
        let high = self.high.get(row)?;
        Some(RuntimeRecordMetadata::from_ingested_at_watermarks(
            Timestamp::from_unix_nanos(*low),
            Timestamp::from_unix_nanos(*high),
        ))
    }

    /// The watermarks of every row, in order.
    pub(crate) fn rows(&self) -> impl Iterator<Item = RuntimeRecordMetadata> + '_ {
        self.low.iter().zip(self.high.iter()).map(|(low, high)| {
            RuntimeRecordMetadata::from_ingested_at_watermarks(
                Timestamp::from_unix_nanos(*low),
                Timestamp::from_unix_nanos(*high),
            )
        })
    }

    /// The watermarks of `rows`, in the order given, or `None` when one of them is past the last
    /// row.
    pub(crate) fn take(&self, rows: &[usize]) -> Option<Self> {
        let mut low = Vec::with_capacity(rows.len());
        let mut high = Vec::with_capacity(rows.len());
        for &row in rows {
            low.push(*self.low.get(row)?);
            high.push(*self.high.get(row)?);
        }
        Some(Self::from_nanos(low, high))
    }

    /// The columns of a relay payload's watermarks as another node sent them, one entry per row.
    pub(crate) fn from_remote(rows: Vec<RemoteRuntimeRecordMetadata>) -> Self {
        Self::from_rows(rows.into_iter().map(RuntimeRecordMetadata::from_remote))
    }

    /// Every row's watermarks as a relay payload carries them to another node.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies an iterator of immutable row metadata")
    )]
    pub(crate) fn to_remote(&self) -> Vec<RemoteRuntimeRecordMetadata> {
        self.rows().map(|metadata| metadata.to_remote()).collect()
    }

    /// Every row's high watermark in Unix nanoseconds.
    pub(crate) fn high_watermarks(&self) -> &[i64] {
        &self.high
    }

    /// The low-watermark values as an Arrow timestamp column, sharing the existing buffer.
    pub(crate) fn low_timestamp_column(&self) -> TimestampNanosecondArray {
        TimestampNanosecondArray::new(self.low.clone(), None)
    }

    /// The latest high watermark of any row, or `None` for no rows.
    pub(crate) fn latest_high_watermark(&self) -> Option<Timestamp> {
        latest_instant(&self.high).map(Timestamp::from_unix_nanos)
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{RemoteRuntimeRecordMetadata, Timestamp};

    use super::RecordMetadataColumns;
    use crate::runtime_schema::RuntimeRecordMetadata;

    fn metadata(low: i64, high: i64) -> RuntimeRecordMetadata {
        RuntimeRecordMetadata::from_ingested_at_watermarks(
            Timestamp::from_unix_nanos(low),
            Timestamp::from_unix_nanos(high),
        )
    }

    fn watermarks(columns: &RecordMetadataColumns) -> Vec<(i64, i64)> {
        columns
            .rows()
            .map(|row| {
                (
                    row.ingested_at_low_watermark().unix_nanos(),
                    row.ingested_at_high_watermark().unix_nanos(),
                )
            })
            .collect()
    }

    #[test]
    fn rows_keep_both_watermarks_across_the_whole_nanosecond_range() {
        let columns = RecordMetadataColumns::from_rows([
            metadata(i64::MIN, i64::MAX),
            metadata(-1, 0),
            metadata(10, 20),
        ]);

        assert_eq!(columns.len(), 3);
        assert_eq!(
            watermarks(&columns),
            [(i64::MIN, i64::MAX), (-1, 0), (10, 20)]
        );
        assert_eq!(columns.high_watermarks(), [i64::MAX, 0, 20]);
        let second = columns.row(1).expect("the second row exists");
        assert_eq!(
            second.ingested_at_low_watermark(),
            Timestamp::from_unix_nanos(-1)
        );
        assert!(columns.row(3).is_none());
        assert_eq!(
            columns.latest_high_watermark(),
            Some(Timestamp::from_unix_nanos(i64::MAX))
        );
    }

    #[test]
    fn taking_rows_follows_their_order_and_refuses_one_past_the_end() {
        let columns =
            RecordMetadataColumns::from_rows([metadata(1, 2), metadata(3, 4), metadata(5, 6)]);

        let taken = columns.take(&[2, 0, 2]).expect("every row is inside");
        assert_eq!(watermarks(&taken), [(5, 6), (1, 2), (5, 6)]);
        assert!(columns.take(&[0, 3]).is_none());
    }

    #[test]
    fn concatenation_appends_every_part_in_order() {
        let first = RecordMetadataColumns::from_rows([metadata(1, 2)]);
        let empty = RecordMetadataColumns::from_rows([]);
        let second = RecordMetadataColumns::from_rows([metadata(3, 4), metadata(5, 6)]);

        let concatenated = RecordMetadataColumns::concat([&first, &empty, &second]);

        assert_eq!(watermarks(&concatenated), [(1, 2), (3, 4), (5, 6)]);
        assert_eq!(empty.latest_high_watermark(), None);
    }

    #[test]
    fn the_wire_form_round_trips_every_row() {
        let columns = RecordMetadataColumns::from_rows([metadata(7, 9), metadata(-3, 11)]);

        let remote = columns.to_remote();
        assert_eq!(
            remote,
            [
                RemoteRuntimeRecordMetadata {
                    ingested_at_low_watermark: Timestamp::from_unix_nanos(7),
                    ingested_at_high_watermark: Timestamp::from_unix_nanos(9),
                },
                RemoteRuntimeRecordMetadata {
                    ingested_at_low_watermark: Timestamp::from_unix_nanos(-3),
                    ingested_at_high_watermark: Timestamp::from_unix_nanos(11),
                },
            ]
        );
        let restored = RecordMetadataColumns::from_remote(remote);
        assert_eq!(watermarks(&restored), [(7, 9), (-3, 11)]);
    }
}
