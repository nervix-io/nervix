//! The branch-local accumulators a window keeps for its aggregate demands.
//!
//! Layer: data plane.
//!
//! - **Owns.** The one contract every window aggregate structure honours — admit rows, retract the
//!   oldest rows, reset, answer an aggregate, and restore from the rows the window retains —
//!   together with the numerically stable structure behind every aggregate function.
//! - **Depends on.** The window aggregate plan, the Arrow argument columns rows are admitted from,
//!   and the processor error that owns every window failure.
//! - **Must not know.** Relays, acknowledgements, output routes, branch tasks, or how a snapshot is
//!   encoded.
//!
//! # The accumulator contract
//!
//! - **Branch ownership.** An accumulator is a field of the live window of exactly one concrete
//!   branch. Only the task that owns that branch changes it, through `&mut self`; it takes no lock
//!   and is never shared. What leaves the task is the immutable window the task publishes.
//! - **Admission and expiry.** Rows are admitted oldest first, in runs of consecutive rows of one
//!   evaluated argument batch, and leave only from the front, oldest first, when the window steps.
//!   A structure never retracts a row it did not admit. Evicting a branch drops its whole window.
//!   The histogram percentile alone keeps a stepped row counted until its configured delay has
//!   elapsed on the domain clock.
//! - **Nullability.** A row contributes to an aggregate only when every argument the aggregate
//!   reads is present; `COUNT` counts every retained row regardless. An aggregate that no retained
//!   row contributed to is a typed null.
//! - **Empty windows.** A window emits only while it retains a row. `COUNT` and `COUNT_IF` are
//!   zero, not null. Sample statistics need two contributing rows, and a correlation also needs
//!   variation in both of its arguments.
//! - **Numerical stability.** Row counts, boolean counts, and integer sums are exact and retract by
//!   subtraction. Floating-point sums and moments never retract by subtraction: they are mergeable
//!   aggregates, and a two-stack window recomputes the aggregate of the surviving rows from merges,
//!   so a value that left the window leaves no rounding behind. Moments are kept centered, so no
//!   variance is ever the difference of two large sums of squares.
//! - **Snapshot and restore.** A published window carries its retained rows with their argument
//!   values. Restoring re-admits those rows in order, which rebuilds every structure; only what the
//!   retained rows cannot reproduce, the histogram's delayed removals, is carried beside them.

mod arguments;
mod counts;
mod extremes;
mod histogram;
mod moments;
mod sketches;
mod sums;
mod two_stacks;

use std::ops::Range;

pub(super) use arguments::{ArgumentColumn, WindowArgumentColumns};
use counts::{RowCounter, TruthCounter};
use extremes::{ExtremeOrder, RowExtremes};
use histogram::LinearHistogram;
use moments::{CoMoments, Moments};
use sketches::PaneSketches;
use sums::{CompensatedSum, IntegerSum};
use two_stacks::TwoStacks;

use super::*;

/// What a window processor's accumulators are built from: every demand of the window's routes, in
/// written route order, with the exact types its arguments evaluate to.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct WindowAccumulatorPlan {
    demands: Vec<CompiledWindowDemand>,
    pub(super) sketch_layout: Option<WindowPaneLayout>,
    pub(super) max_state_bytes: Option<std::num::NonZeroU64>,
}

impl WindowAccumulatorPlan {
    /// The plan of a window whose routes compiled into `routes`, in written route order.
    pub(super) fn new<'a>(
        routes: impl IntoIterator<Item = &'a CompiledWindowRoute>,
        sketch_layout: Option<WindowPaneLayout>,
        max_state_bytes: Option<std::num::NonZeroU64>,
    ) -> Self {
        let mut demands = Vec::new();
        for route in routes {
            demands.extend(route.demands.iter().cloned());
        }
        Self {
            demands,
            sketch_layout,
            max_state_bytes,
        }
    }

    pub(super) fn demands(&self) -> &[CompiledWindowDemand] {
        &self.demands
    }

    /// The accumulators of an empty window.
    pub(super) fn empty_accumulators(&self) -> Vec<WindowAccumulator> {
        self.demands
            .iter()
            .map(|demand| WindowAccumulator::new(demand, self.sketch_layout))
            .collect()
    }
}

/// One row a window retains: when it arrived, the sequence it was admitted under, and where its
/// aggregate arguments live.
#[derive(Debug, Clone)]
pub(super) struct WindowRow {
    /// Admission order within the branch. Retained rows carry consecutive sequences.
    pub(super) sequence: u64,
    /// The row's ingestion low watermark, which orders `FIRST` and `LAST`.
    pub(super) timestamp: Timestamp,
    /// The argument columns of the batch the row was admitted from.
    pub(super) arguments: Arc<WindowArgumentColumns>,
    /// The row's index within `arguments`.
    pub(super) row: usize,
}

/// The rows a window retains, oldest first.
pub(super) trait RetainedWindowRows {
    fn retained(&self) -> usize;

    /// The retained row at `position`, counting from the oldest.
    fn retained_row(&self, position: usize) -> &WindowRow;
}

/// Consecutive retained rows that were admitted from one argument batch, next to each other in it.
#[derive(Debug, Clone)]
pub(super) struct WindowRowRun<'a> {
    pub(super) arguments: &'a WindowArgumentColumns,
    /// The rows' indexes within `arguments`.
    pub(super) rows: Range<usize>,
    /// The retained position of the run's first row.
    pub(super) first_position: usize,
}

impl WindowRowRun<'_> {
    /// Each row of the run with its retained position, oldest first.
    fn positioned_rows(&self) -> impl DoubleEndedIterator<Item = PositionedRow> + '_ {
        self.rows
            .clone()
            .enumerate()
            .map(|(offset, row)| PositionedRow {
                row,
                position: self
                    .first_position
                    .checked_add(offset)
                    .assured("a run's positions index rows the window already retains in memory"),
            })
    }
}

/// One row of a run: its index in the argument batch and its retained position.
#[derive(Debug, Clone, Copy)]
struct PositionedRow {
    row: usize,
    position: usize,
}

/// The retained rows at `positions`, grouped into runs, oldest first.
pub(super) fn retained_runs<R: RetainedWindowRows>(
    rows: &R,
    positions: Range<usize>,
) -> RetainedRuns<'_, R> {
    RetainedRuns { rows, positions }
}

/// Iterates retained rows as runs, oldest first.
pub(super) struct RetainedRuns<'a, R> {
    rows: &'a R,
    /// The positions not yet grouped into a run.
    positions: Range<usize>,
}

impl<'a, R: RetainedWindowRows> Iterator for RetainedRuns<'a, R> {
    type Item = WindowRowRun<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.positions.is_empty() {
            return None;
        }
        let first_position = self.positions.start;
        let first = self.rows.retained_row(first_position);
        let mut run_end = first_position
            .checked_add(1)
            .assured("positions index rows the window already retains in memory");
        let mut next_row = first
            .row
            .checked_add(1)
            .assured("row indexes address a batch the window already retains in memory");
        while run_end < self.positions.end {
            let candidate = self.rows.retained_row(run_end);
            let same_batch = Arc::ptr_eq(&candidate.arguments, &first.arguments);
            if !same_batch || candidate.row != next_row {
                break;
            }
            run_end = run_end
                .checked_add(1)
                .assured("positions index rows the window already retains in memory");
            next_row = next_row
                .checked_add(1)
                .assured("row indexes address a batch the window already retains in memory");
        }
        self.positions.start = run_end;
        Some(WindowRowRun {
            arguments: &first.arguments,
            rows: first.row..next_row,
            first_position,
        })
    }
}

/// The accumulator a window keeps for one demand.
#[derive(Debug, Clone)]
pub(super) enum WindowAccumulator {
    Counter(RowCounter),
    TruthCounter(TruthCounter),
    IntegerSum(IntegerSum),
    FloatSum(TwoStacks<CompensatedSum>),
    Moments(TwoStacks<Moments>),
    CoMoments(TwoStacks<CoMoments>),
    Extremes(RowExtremes),
    Histogram(LinearHistogram),
    Sketch(PaneSketches),
}

impl WindowAccumulator {
    /// The accumulator of an empty window for `demand`.
    pub(super) fn new(demand: &CompiledWindowDemand, layout: Option<WindowPaneLayout>) -> Self {
        match demand.storage {
            WindowAggregateStorageKind::DistinctSketch
            | WindowAggregateStorageKind::QuantileSketch
            | WindowAggregateStorageKind::TopKSketch => {
                let config = demand
                    .sketch
                    .verified("a sketch storage demand carries its validated config");
                let layout =
                    layout.verified("registry validation requires duration panes for sketches");
                Self::Sketch(PaneSketches::new(config, layout))
            }
            WindowAggregateStorageKind::Counter => Self::Counter(RowCounter::default()),
            WindowAggregateStorageKind::TruthCounter => Self::TruthCounter(TruthCounter::default()),
            WindowAggregateStorageKind::Sum => {
                if IntegerSum::reads(&demand.arguments.first().data_type) {
                    Self::IntegerSum(IntegerSum::default())
                } else {
                    Self::FloatSum(TwoStacks::new())
                }
            }
            WindowAggregateStorageKind::Moments => Self::Moments(TwoStacks::new()),
            WindowAggregateStorageKind::CoMoments => Self::CoMoments(TwoStacks::new()),
            WindowAggregateStorageKind::Sequence => {
                Self::Extremes(RowExtremes::new(ExtremeOrder::Arrival, &demand.functions))
            }
            WindowAggregateStorageKind::Extremes => {
                Self::Extremes(RowExtremes::new(ExtremeOrder::Value, &demand.functions))
            }
            WindowAggregateStorageKind::ArgExtremes => {
                Self::Extremes(RowExtremes::new(ExtremeOrder::Key, &demand.functions))
            }
            WindowAggregateStorageKind::Histogram => {
                let config = demand.linear_histogram.as_ref().verified(
                    "the histogram storage kind is only chosen for a demand that carries the \
                     histogram config",
                );
                Self::Histogram(LinearHistogram::new(config))
            }
        }
    }

    /// Fold the retained rows at `positions`, which the window has just admitted, into this
    /// accumulator. `admitted_at` is the latest watermark among them, which expires the histogram's
    /// delayed removals that are due.
    pub(super) fn admit<R: RetainedWindowRows>(
        &mut self,
        demand: usize,
        rows: &R,
        positions: Range<usize>,
        admitted_at: Timestamp,
    ) {
        if let Self::Histogram(histogram) = self {
            histogram.purge_expired(admitted_at);
        }
        self.fold(demand, rows, positions);
    }

    /// Fold the retained rows at `positions` without expiring anything, which is how both
    /// admission and restoring a published window count rows.
    fn fold<R: RetainedWindowRows>(&mut self, demand: usize, rows: &R, positions: Range<usize>) {
        for run in retained_runs(rows, positions.clone()) {
            let arguments = run.arguments.demand(demand);
            match self {
                Self::Counter(counter) => counter.admit(run.rows.len()),
                Self::TruthCounter(counter) => counter.admit(arguments.first(), run.rows),
                Self::IntegerSum(sum) => sum.admit(arguments.first(), run.rows),
                Self::FloatSum(sums) => {
                    sums.admit(CompensatedSum::of_rows(arguments.first(), run.rows))
                }
                Self::Moments(moments) => {
                    moments.admit(Moments::of_rows(arguments.first(), run.rows))
                }
                Self::CoMoments(moments) => moments.admit(CoMoments::of_rows(arguments, run.rows)),
                Self::Extremes(extremes) => extremes.admit(demand, rows, &run),
                Self::Histogram(histogram) => histogram.admit(arguments.first(), run.rows),
                Self::Sketch(_) => {}
            }
        }
        if let Self::Sketch(sketch) = self {
            sketch.admit(demand, rows, positions);
        }
    }

    /// Remove the `count` oldest retained rows. The rows are still retained while this runs, and
    /// the window drops them right after. `removed_at` is the watermark the window stepped at,
    /// from which the histogram delays its removals.
    pub(super) fn retract_oldest<R: RetainedWindowRows>(
        &mut self,
        demand: usize,
        rows: &R,
        count: usize,
        removed_at: Timestamp,
    ) {
        let retained = rows.retained();
        match self {
            Self::Counter(counter) => counter.retract(count),
            Self::TruthCounter(counter) => {
                for run in retained_runs(rows, 0..count) {
                    counter.retract(run.arguments.demand(demand).first(), run.rows);
                }
            }
            Self::IntegerSum(sum) => {
                for run in retained_runs(rows, 0..count) {
                    sum.retract(run.arguments.demand(demand).first(), run.rows);
                }
            }
            Self::FloatSum(sums) => sums.retract_oldest(count, retained, |position| {
                let row = rows.retained_row(position);
                CompensatedSum::of_row(row.arguments.demand(demand).first(), row.row)
            }),
            Self::Moments(moments) => moments.retract_oldest(count, retained, |position| {
                let row = rows.retained_row(position);
                Moments::of_row(row.arguments.demand(demand).first(), row.row)
            }),
            Self::CoMoments(moments) => moments.retract_oldest(count, retained, |position| {
                let row = rows.retained_row(position);
                CoMoments::of_row(row.arguments.demand(demand), row.row)
            }),
            Self::Extremes(extremes) => extremes.retract_oldest(rows, count),
            Self::Histogram(histogram) => {
                histogram.purge_expired(removed_at);
                for run in retained_runs(rows, 0..count) {
                    histogram.retract(run.arguments.demand(demand).first(), run.rows, removed_at);
                }
            }
            Self::Sketch(sketch) => sketch.retain_after(demand, rows, count),
        }
    }

    /// Expire the histogram's delayed removals that are due at `now`, answering whether any was.
    pub(super) fn purge_expired(&mut self, now: Timestamp) -> bool {
        match self {
            Self::Histogram(histogram) => histogram.purge_expired(now),
            Self::Counter(_)
            | Self::TruthCounter(_)
            | Self::IntegerSum(_)
            | Self::FloatSum(_)
            | Self::Moments(_)
            | Self::CoMoments(_)
            | Self::Extremes(_)
            | Self::Sketch(_) => false,
        }
    }

    /// When the next delayed removal becomes due, if one is pending.
    pub(super) fn next_deadline(&self) -> Option<Timestamp> {
        match self {
            Self::Histogram(histogram) => histogram.next_deadline(),
            Self::Counter(_)
            | Self::TruthCounter(_)
            | Self::IntegerSum(_)
            | Self::FloatSum(_)
            | Self::Moments(_)
            | Self::CoMoments(_)
            | Self::Extremes(_)
            | Self::Sketch(_) => None,
        }
    }

    /// The one-row result of `invocation` over the retained rows, of exactly `output_type`.
    pub(super) fn evaluate<R: RetainedWindowRows>(
        &self,
        demand: usize,
        invocation: &WindowAggregateInvocation,
        output_type: &ArrowDataType,
        rows: &R,
    ) -> error_stack::Result<ArrayRef, WindowProcessorError> {
        let function = invocation.function;
        match self {
            Self::Counter(counter) => Ok(counter.evaluate()),
            Self::TruthCounter(counter) => Ok(counter.evaluate(function)),
            Self::IntegerSum(sum) => sum.evaluate(output_type),
            Self::FloatSum(sums) => sums.aggregate().evaluate(output_type),
            Self::Moments(moments) => moments.aggregate().evaluate(function),
            Self::CoMoments(moments) => moments.aggregate().evaluate(function),
            Self::Extremes(extremes) => Ok(extremes.evaluate(demand, function, rows, output_type)),
            Self::Histogram(histogram) => Ok(histogram.evaluate(invocation.percentile)),
            Self::Sketch(sketch) => sketch.evaluate(demand, invocation, output_type, rows),
        }
    }

    /// What this accumulator publishes beyond the retained rows.
    pub(super) fn to_snapshot(&self) -> WindowAccumulatorSnapshot {
        match self {
            Self::Histogram(histogram) => histogram.to_snapshot(),
            Self::Counter(_)
            | Self::TruthCounter(_)
            | Self::IntegerSum(_)
            | Self::FloatSum(_)
            | Self::Moments(_)
            | Self::CoMoments(_)
            | Self::Extremes(_)
            | Self::Sketch(_) => WindowAccumulatorSnapshot::Retained,
        }
    }

    /// Rebuild the accumulator for `plan`'s demand at index `demand` from every retained row and
    /// what it published beyond them.
    pub(super) fn restore<R: RetainedWindowRows>(
        plan: &CompiledWindowDemand,
        demand: usize,
        rows: &R,
        snapshot: &WindowAccumulatorSnapshot,
        layout: Option<WindowPaneLayout>,
    ) -> error_stack::Result<Self, WindowProcessorError> {
        let mut accumulator = Self::new(plan, layout);
        accumulator.fold(demand, rows, 0..rows.retained());
        match (&mut accumulator, snapshot) {
            (
                Self::Histogram(histogram),
                WindowAccumulatorSnapshot::LinearHistogram { delayed_removals },
            ) => {
                histogram.restore_delayed_removals(delayed_removals)?;
            }
            (
                Self::Counter(_)
                | Self::TruthCounter(_)
                | Self::IntegerSum(_)
                | Self::FloatSum(_)
                | Self::Moments(_)
                | Self::CoMoments(_)
                | Self::Extremes(_)
                | Self::Sketch(_),
                WindowAccumulatorSnapshot::Retained,
            ) => {}
            _ => {
                return Err(Report::new(WindowProcessorError::SnapshotAccumulator {
                    demand,
                    storage: plan.storage,
                }));
            }
        }
        Ok(accumulator)
    }
}
