//! The retained rows holding a window's smallest and largest keys.
//!
//! Layer: data plane.
//!
//! - **Owns.** Keeping, for `FIRST`, `LAST`, `MIN`, `MAX`, `ARG_MIN` and `ARG_MAX`, only the
//!   retained rows that can still hold the extreme key once older rows leave, resolving ties toward
//!   the earliest admitted row, and returning the extreme row's value in its own type.
//! - **Depends on.** The rows a window retains and their argument columns.
//! - **Must not know.** Which output a result feeds, or when a window emits.
//!
//! Each extreme is a queue of candidate rows in admission order whose keys never improve toward
//! the back. A new row removes every candidate it strictly beats, since those rows leave the window
//! before it does and can never be the extreme again. The front candidate is always the extreme,
//! and it leaves only when its row leaves.

use std::cmp::Ordering;

use super::*;

/// What orders the rows of one demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ExtremeOrder {
    /// The row's low watermark, then its admission: `FIRST` and `LAST` of the first argument.
    Arrival,
    /// The first argument itself: `MIN` and `MAX`.
    Value,
    /// The second argument: `ARG_MIN` and `ARG_MAX`, which return the first argument.
    Key,
}

/// The candidate rows for the smallest and largest key of one demand's retained rows.
#[derive(Debug, Clone)]
pub(in crate::runtime) struct RowExtremes {
    order: ExtremeOrder,
    /// Sequences of the rows that can still hold the smallest key, when the demand needs it.
    smallest: Option<VecDeque<u64>>,
    /// Sequences of the rows that can still hold the largest key, when the demand needs it.
    largest: Option<VecDeque<u64>>,
}

impl RowExtremes {
    pub(super) fn new(order: ExtremeOrder, functions: &SortedSet<WindowAggregateFunction>) -> Self {
        let mut smallest = None;
        let mut largest = None;
        for function in functions.iter() {
            match function {
                WindowAggregateFunction::First
                | WindowAggregateFunction::Min
                | WindowAggregateFunction::ArgMin => smallest = Some(VecDeque::new()),
                WindowAggregateFunction::Last
                | WindowAggregateFunction::Max
                | WindowAggregateFunction::ArgMax => largest = Some(VecDeque::new()),
                _ => {}
            }
        }
        Self {
            order,
            smallest,
            largest,
        }
    }

    /// Consider every row of `run`, which the window has just admitted.
    pub(super) fn admit<R: RetainedWindowRows>(
        &mut self,
        demand: usize,
        rows: &R,
        run: &WindowRowRun<'_>,
    ) {
        let arguments = run.arguments.demand(demand);
        for positioned in run.positioned_rows() {
            if !self.contributes(arguments, positioned.row) {
                continue;
            }
            let sequence = rows.retained_row(positioned.position).sequence;
            if let Some(smallest) = self.smallest.as_mut() {
                Self::push_candidate(
                    self.order,
                    demand,
                    rows,
                    smallest,
                    positioned.position,
                    sequence,
                    Ordering::Greater,
                );
            }
            if let Some(largest) = self.largest.as_mut() {
                Self::push_candidate(
                    self.order,
                    demand,
                    rows,
                    largest,
                    positioned.position,
                    sequence,
                    Ordering::Less,
                );
            }
        }
    }

    /// Whether the row at `row` of `arguments` holds every argument this demand reads.
    fn contributes(&self, arguments: &WindowArguments<ArgumentColumn>, row: usize) -> bool {
        match self.order {
            ExtremeOrder::Arrival | ExtremeOrder::Value => arguments.first().is_present(row),
            ExtremeOrder::Key => {
                let key = arguments
                    .second()
                    .verified("ARG_MIN and ARG_MAX are planned with their key argument");
                arguments.first().is_present(row) && key.is_present(row)
            }
        }
    }

    /// Append the row at `position` to `candidates`, first removing every candidate whose key
    /// compares to it as `beaten`. Candidates with an equal key stay, so an earlier row keeps a tie.
    fn push_candidate<R: RetainedWindowRows>(
        order: ExtremeOrder,
        demand: usize,
        rows: &R,
        candidates: &mut VecDeque<u64>,
        position: usize,
        sequence: u64,
        beaten: Ordering,
    ) {
        while let Some(back) = candidates.back() {
            let back_position = Self::position_of(rows, *back);
            if Self::compare(order, demand, rows, back_position, position) != beaten {
                break;
            }
            candidates.pop_back();
        }
        candidates.push_back(sequence);
    }

    /// Order the keys of the retained rows at `left` and `right`.
    fn compare<R: RetainedWindowRows>(
        order: ExtremeOrder,
        demand: usize,
        rows: &R,
        left: usize,
        right: usize,
    ) -> Ordering {
        let left_row = rows.retained_row(left);
        let right_row = rows.retained_row(right);
        match order {
            ExtremeOrder::Arrival => (left_row.timestamp, left_row.sequence)
                .cmp(&(right_row.timestamp, right_row.sequence)),
            ExtremeOrder::Value => {
                let left_column = left_row.arguments.demand(demand).first();
                let right_column = right_row.arguments.demand(demand).first();
                left_column.compare(left_row.row, right_column, right_row.row)
            }
            ExtremeOrder::Key => {
                let key = "ARG_MIN and ARG_MAX are planned with their key argument";
                let left_column = left_row.arguments.demand(demand).second().verified(key);
                let right_column = right_row.arguments.demand(demand).second().verified(key);
                left_column.compare(left_row.row, right_column, right_row.row)
            }
        }
    }

    /// The retained position of the row admitted under `sequence`.
    fn position_of<R: RetainedWindowRows>(rows: &R, sequence: u64) -> usize {
        let oldest = rows.retained_row(0).sequence;
        let offset = sequence
            .checked_sub(oldest)
            .verified("every candidate is a retained row, and retained rows follow the oldest");
        usize::try_from(offset)
            .assured("a retained row's offset indexes rows the window holds in memory")
    }

    /// Forget the `count` oldest retained rows. The rows are still retained while this runs.
    pub(super) fn retract_oldest<R: RetainedWindowRows>(&mut self, rows: &R, count: usize) {
        let survivor = if count < rows.retained() {
            Some(rows.retained_row(count).sequence)
        } else {
            None
        };
        for candidates in [self.smallest.as_mut(), self.largest.as_mut()]
            .into_iter()
            .flatten()
        {
            match survivor {
                Some(survivor) => {
                    while let Some(front) = candidates.front()
                        && *front < survivor
                    {
                        candidates.pop_front();
                    }
                }
                None => candidates.clear(),
            }
        }
    }

    /// The value `function` answers: the first argument of the extreme row, or a typed null when
    /// no retained row contributed.
    pub(super) fn evaluate<R: RetainedWindowRows>(
        &self,
        demand: usize,
        function: WindowAggregateFunction,
        rows: &R,
        output_type: &ArrowDataType,
    ) -> ArrayRef {
        let candidates = match function {
            WindowAggregateFunction::First
            | WindowAggregateFunction::Min
            | WindowAggregateFunction::ArgMin => self.smallest.as_ref(),
            WindowAggregateFunction::Last
            | WindowAggregateFunction::Max
            | WindowAggregateFunction::ArgMax => self.largest.as_ref(),
            _ => None,
        };
        let candidates = candidates.verified(
            "an extreme is kept for every function its demand serves, and extremes serve only \
             FIRST, LAST, MIN, MAX, ARG_MIN and ARG_MAX",
        );
        let Some(extreme) = candidates.front() else {
            return new_null_array(output_type, 1);
        };
        let row = rows.retained_row(Self::position_of(rows, *extreme));
        row.arguments.demand(demand).first().slice(row.row)
    }
}
