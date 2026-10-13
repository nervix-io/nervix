//! Same-host pane sketch admission, for Criterion.
//!
//! Layer: test harness.
//!
//! - **Owns.** Retained rows over a chosen pane distribution and their admission into empty
//!   panes.
//! - **Depends on.** The production pane sketches, retained rows and argument columns.
//! - **Must not know.** Window emission, snapshots or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "benchmark fixture outside product execution"
    )
)]

use std::num::NonZeroUsize;

use arrow_array::Int64Array;

use super::*;

/// The sketch a measured window keeps.
#[derive(Clone, Copy, Debug)]
pub enum SketchKind {
    Distinct,
    Quantile,
    TopK,
}

/// The nanoseconds one pane spans in the measured layout.
const PANE_NANOS: i64 = 1_000;

/// Distinct argument values; later rows repeat earlier ones, as frequency candidates do.
const DISTINCT_VALUES: i64 = 1_024;

/// One window's retained rows, oldest first.
struct BenchmarkRows(Vec<WindowRow>);

impl RetainedWindowRows for BenchmarkRows {
    fn retained(&self) -> usize {
        self.0.len()
    }

    fn retained_row(&self, position: usize) -> &WindowRow {
        &self.0[position]
    }
}

/// One window's retained rows and the pane sketches that admit them.
pub struct WindowSketchBenchmark {
    sketches: PaneSketches,
    rows: BenchmarkRows,
}

impl WindowSketchBenchmark {
    /// `rows` retained rows of one argument batch, `rows_per_pane` consecutive rows to a pane.
    pub fn new(kind: SketchKind, rows: NonZeroUsize, rows_per_pane: NonZeroUsize) -> Self {
        let capacity = NonZeroUsize::new(128).verified("128 is not zero");
        let config = match kind {
            SketchKind::Distinct => WindowSketchConfig::Distinct { precision: 10 },
            SketchKind::Quantile => WindowSketchConfig::Quantile { capacity },
            SketchKind::TopK => WindowSketchConfig::TopK {
                k: NonZeroUsize::new(4).verified("4 is not zero"),
                capacity,
            },
        };
        let mut values = Vec::with_capacity(rows.get());
        let mut timestamps = Vec::with_capacity(rows.get());
        for row in 0..rows.get() {
            let index = i64::try_from(row).assured("a benchmark window holds fewer than 2^63 rows");
            let pane = i64::try_from(row / rows_per_pane.get())
                .assured("a benchmark window holds fewer than 2^63 panes");
            values.push(index % DISTINCT_VALUES);
            timestamps.push(Timestamp::from_unix_nanos(pane * PANE_NANOS));
        }
        let array: ArrayRef = StdArc::new(Int64Array::from(values));
        let arguments = Arc::new(WindowArgumentColumns::single(array));
        let mut retained = Vec::with_capacity(rows.get());
        for (row, timestamp) in timestamps.into_iter().enumerate() {
            retained.push(WindowRow {
                sequence: u64::try_from(row).assured("a benchmark window holds few rows"),
                timestamp,
                arguments: Arc::clone(&arguments),
                row,
            });
        }
        let layout = WindowPaneLayout {
            pane_nanos: PANE_NANOS,
            maximum_panes: u64::try_from(rows.get()).assured("the row count fits u64"),
        };
        Self {
            sketches: PaneSketches::new(config, layout),
            rows: BenchmarkRows(retained),
        }
    }

    /// Admits every retained row into empty panes, as a retraction's rebuild does, and answers how
    /// many panes the rows created.
    pub fn admit(&mut self) -> usize {
        self.sketches.clear();
        self.sketches.admit(0, &self.rows, 0..self.rows.retained());
        self.sketches.pane_count()
    }
}
