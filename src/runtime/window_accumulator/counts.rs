//! Exact counts a window retracts by subtraction.
//!
//! Layer: data plane.
//!
//! - **Owns.** Counting retained rows for `COUNT`, counting true and false rows for `COUNT_IF`,
//!   `BOOL_AND` and `BOOL_OR`, and each of those functions' results.
//! - **Depends on.** The argument columns of the rows a window admits and retracts.
//! - **Must not know.** Which rows a window retains beyond their counts, or when it emits.

use std::ops::Range;

use arrow_array::Int64Array;

use super::*;

/// The number of rows a window retains, for `COUNT`.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct RowCounter {
    rows: u64,
}

impl RowCounter {
    pub(super) fn admit(&mut self, rows: usize) {
        let rows: u64 = rows.arch_into();
        self.rows = self
            .rows
            .checked_add(rows)
            .assured("a window cannot retain 2^64 rows in memory");
    }

    pub(super) fn retract(&mut self, rows: usize) {
        let rows: u64 = rows.arch_into();
        self.rows = self
            .rows
            .checked_sub(rows)
            .verified("a window retracts only rows it retains, and it counted every one of them");
    }

    /// `COUNT`: every retained row, whatever its argument holds.
    pub(super) fn evaluate(&self) -> ArrayRef {
        let rows = i64::try_from(self.rows).assured("a window cannot retain 2^63 rows in memory");
        let array: ArrayRef = StdArc::new(Int64Array::from(vec![rows]));
        array
    }
}

/// The numbers of true and false rows of one BOOL argument, for `COUNT_IF`, `BOOL_AND` and
/// `BOOL_OR`. A null argument counts toward neither.
#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct TruthCounter {
    true_rows: u64,
    false_rows: u64,
}

impl TruthCounter {
    pub(super) fn admit(&mut self, column: &ArgumentColumn, rows: Range<usize>) {
        for row in rows {
            match column.boolean_at(row) {
                Some(true) => {
                    self.true_rows = self
                        .true_rows
                        .checked_add(1)
                        .assured("a window cannot retain 2^64 rows in memory");
                }
                Some(false) => {
                    self.false_rows = self
                        .false_rows
                        .checked_add(1)
                        .assured("a window cannot retain 2^64 rows in memory");
                }
                None => {}
            }
        }
    }

    pub(super) fn retract(&mut self, column: &ArgumentColumn, rows: Range<usize>) {
        for row in rows {
            match column.boolean_at(row) {
                Some(true) => {
                    self.true_rows = self
                        .true_rows
                        .checked_sub(1)
                        .verified("a retracted true row was counted when the window admitted it");
                }
                Some(false) => {
                    self.false_rows = self
                        .false_rows
                        .checked_sub(1)
                        .verified("a retracted false row was counted when the window admitted it");
                }
                None => {}
            }
        }
    }

    pub(super) fn evaluate(&self, function: WindowAggregateFunction) -> ArrayRef {
        let counted = self
            .true_rows
            .checked_add(self.false_rows)
            .assured("a window cannot retain 2^64 rows in memory");
        let result: Option<ArrayRef> = match function {
            WindowAggregateFunction::CountIf => {
                let true_rows = i64::try_from(self.true_rows)
                    .assured("a window cannot retain 2^63 rows in memory");
                Some(StdArc::new(Int64Array::from(vec![true_rows])))
            }
            WindowAggregateFunction::BoolAnd => {
                let every_true = if counted == 0 {
                    None
                } else {
                    Some(self.false_rows == 0)
                };
                Some(StdArc::new(BooleanArray::from(vec![every_true])))
            }
            WindowAggregateFunction::BoolOr => {
                let any_true = if counted == 0 {
                    None
                } else {
                    Some(self.true_rows > 0)
                };
                Some(StdArc::new(BooleanArray::from(vec![any_true])))
            }
            _ => None,
        };
        result.verified("a truth counter serves only COUNT_IF, BOOL_AND and BOOL_OR")
    }
}
