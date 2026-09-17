//! The fixed-range histogram behind `PERCENTILE_LINEAR_HISTOGRAM`.
//!
//! Layer: data plane.
//!
//! - **Owns.** Bucketing present values over the configured range, keeping a stepped row counted
//!   until its configured delay elapses on the domain clock, and the percentile read from the
//!   buckets.
//! - **Depends on.** The argument columns of admitted and retracted rows, and domain timestamps.
//! - **Must not know.** Which rows a window retains, or when it emits.

use std::ops::Range;

use arrow_array::Float64Array;

use super::*;

/// Bucket counts over a fixed range, with the removals that wait out their delay.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct LinearHistogram {
    buckets: Vec<u64>,
    total: u64,
    min: f64,
    max: f64,
    width: f64,
    delay: Duration,
    /// Removals of stepped rows that stay counted until they expire, earliest expiry first.
    delayed_removals: VecDeque<DelayedRemoval>,
}

#[derive(Debug, Clone, Copy)]
struct DelayedRemoval {
    expires_at: Timestamp,
    bucket: usize,
}

impl LinearHistogram {
    pub(super) fn new(config: &WindowLinearHistogramConfig) -> Self {
        let buckets = config.buckets.get();
        let bucket_count: f64 = buckets.approx_into();
        Self {
            buckets: vec![0; buckets],
            total: 0,
            min: config.min,
            max: config.max,
            width: (config.max - config.min) / bucket_count,
            delay: config.delay,
            delayed_removals: VecDeque::new(),
        }
    }

    /// The bucket a finite `value` falls in. Values at or beyond either end of the range fall in
    /// the bucket at that end.
    fn bucket(&self, value: f64) -> usize {
        let last = self
            .buckets
            .len()
            .checked_sub(1)
            .verified("the histogram config requires at least one bucket");
        if value <= self.min {
            return 0;
        }
        if value >= self.max {
            return last;
        }
        let bucket: usize = ((value - self.min) / self.width)
            .floor()
            .checked_approx_into()
            .verified("a finite value inside the range divides into a non-negative bucket index");
        bucket.min(last)
    }

    pub(super) fn admit(&mut self, column: &ArgumentColumn, rows: Range<usize>) {
        for row in rows {
            let Some(value) = column.number_at(row) else {
                continue;
            };
            let bucket = self.bucket(value);
            self.count_into(bucket);
        }
    }

    fn count_into(&mut self, bucket: usize) {
        let count = self
            .buckets
            .get_mut(bucket)
            .verified("a bucket index is computed within the histogram's buckets");
        *count = count
            .checked_add(1)
            .assured("a window cannot retain 2^64 rows in memory");
        self.total = self
            .total
            .checked_add(1)
            .assured("a window cannot retain 2^64 rows in memory");
    }

    fn uncount_from(&mut self, bucket: usize) {
        let count = self
            .buckets
            .get_mut(bucket)
            .verified("a bucket index is computed within the histogram's buckets");
        *count = count
            .checked_sub(1)
            .verified("a row leaves only the bucket it was counted into");
        self.total = self
            .total
            .checked_sub(1)
            .verified("the total counts every row the buckets count");
    }

    /// Remove stepped rows, immediately without a delay and once `removed_at + delay` passes with
    /// one.
    pub(super) fn retract(
        &mut self,
        column: &ArgumentColumn,
        rows: Range<usize>,
        removed_at: Timestamp,
    ) {
        for row in rows {
            let Some(value) = column.number_at(row) else {
                continue;
            };
            let bucket = self.bucket(value);
            if self.delay.is_zero() {
                self.uncount_from(bucket);
            } else {
                self.delayed_removals.push_back(DelayedRemoval {
                    expires_at: checked_add_duration_to_timestamp(removed_at, self.delay),
                    bucket,
                });
            }
        }
    }

    /// Uncount every delayed removal due at `now`, answering whether any was.
    pub(super) fn purge_expired(&mut self, now: Timestamp) -> bool {
        let mut purged = false;
        while let Some(removal) = self.delayed_removals.front().copied()
            && removal.expires_at <= now
        {
            self.delayed_removals.pop_front();
            self.uncount_from(removal.bucket);
            purged = true;
        }
        purged
    }

    pub(super) fn next_deadline(&self) -> Option<Timestamp> {
        let removal = self.delayed_removals.front()?;
        Some(removal.expires_at)
    }

    /// The bucket midpoint at the `percentile` rank of the counted values, or a typed null when no
    /// value is counted.
    pub(super) fn evaluate(&self, percentile: Option<f64>) -> ArrayRef {
        let percentile = percentile
            .verified("a histogram invocation carries the constant percentile lowering checked");
        let Some(last_rank) = self.total.checked_sub(1) else {
            let array: ArrayRef = StdArc::new(Float64Array::from(vec![None::<f64>]));
            return array;
        };
        let last_rank: f64 = last_rank.approx_into();
        let rank: u64 = ((percentile / 100.0) * last_rank)
            .round()
            .checked_approx_into()
            .verified("a percentile between 0 and 100 ranks within the counted values");
        let mut seen = 0u64;
        let mut midpoint = None;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen
                .checked_add(*count)
                .assured("a window cannot retain 2^64 rows in memory");
            if seen > rank {
                let index: f64 = index.approx_into();
                midpoint = Some((self.min + (index + 0.5) * self.width).clamp(self.min, self.max));
                break;
            }
        }
        let midpoint =
            midpoint.verified("the buckets hold every counted value, and the rank is below them");
        let array: ArrayRef = StdArc::new(Float64Array::from(vec![midpoint]));
        array
    }

    pub(super) fn to_snapshot(&self) -> WindowAccumulatorSnapshot {
        WindowAccumulatorSnapshot::LinearHistogram {
            delayed_removals: self
                .delayed_removals
                .iter()
                .map(|removal| LinearHistogramDelayedRemovalSnapshot {
                    expires_at: removal.expires_at,
                    bucket: removal.bucket,
                })
                .collect(),
        }
    }

    /// Count the removals a published window was still delaying, on top of its retained rows.
    pub(super) fn restore_delayed_removals(
        &mut self,
        delayed_removals: &[LinearHistogramDelayedRemovalSnapshot],
    ) -> error_stack::Result<(), WindowProcessorError> {
        for removal in delayed_removals {
            if removal.bucket >= self.buckets.len() {
                return Err(Report::new(WindowProcessorError::SnapshotHistogramBucket {
                    bucket: removal.bucket,
                    buckets: self.buckets.len(),
                }));
            }
            self.count_into(removal.bucket);
            self.delayed_removals.push_back(DelayedRemoval {
                expires_at: removal.expires_at,
                bucket: removal.bucket,
            });
        }
        Ok(())
    }
}
