use arrow_schema::DataType;
use nervix_nspl::vm_program::Span;
use thiserror::Error;

use crate::ir::RegisterRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    DivisionByZero,
    Overflow,
    CastFailed,
    InvalidArgument,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DivisionByZero => "division_by_zero",
            Self::Overflow => "overflow",
            Self::CastFailed => "cast_failed",
            Self::InvalidArgument => "invalid_argument",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideError {
    pub code: ErrorCode,
    pub message: String,
    pub span: Span,
}

/// Row-aligned side errors recorded while a program executes.
///
/// Executions that record no error are the common case, so the per-row storage is
/// materialized only when the first error arrives. Until then the channel carries just a
/// row count, which keeps construction and cloning independent of the batch row count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowErrors {
    row_count: usize,
    rows: Vec<Vec<SideError>>,
}

/// Borrowed row-error membership without materializing a dense boolean vector.
#[derive(Debug, Clone, Copy)]
pub struct RowErrorMask<'a> {
    row_count: usize,
    rows: &'a [Vec<SideError>],
}

/// Per-row error counts captured before a conditional instruction runs.
#[derive(Debug, Clone)]
pub struct RowErrorLengths(Vec<usize>);

impl RowErrors {
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// True when no row carries an error.
    pub fn is_error_free(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn first(&self) -> Option<&SideError> {
        self.rows.iter().flatten().next()
    }

    pub fn mask(&self) -> RowErrorMask<'_> {
        RowErrorMask {
            row_count: self.row_count,
            rows: &self.rows,
        }
    }

    pub fn row(&self, row: usize) -> &[SideError] {
        self.rows.get(row).map_or(&[], Vec::as_slice)
    }

    pub fn get(&self, row: usize) -> Option<&[SideError]> {
        (row < self.row_count).then(|| self.row(row))
    }

    pub fn iter(&self) -> impl Iterator<Item = &[SideError]> {
        (0..self.row_count).map(|row| self.row(row))
    }

    pub fn push(&mut self, row: usize, error: SideError) {
        if self.rows.is_empty() {
            self.rows = vec![Vec::new(); self.row_count];
        }
        self.rows[row].push(error);
    }

    /// Builds the channel for a filtered batch, keeping only `rows` in the order given.
    pub fn select_rows(&self, rows: &[usize]) -> Self {
        if self.rows.is_empty() {
            return Self::new(rows.len());
        }
        let selected = rows.iter().map(|&row| self.row(row).to_vec()).collect();
        Self::from_materialized_rows(rows.len(), selected)
    }

    fn from_materialized_rows(row_count: usize, mut rows: Vec<Vec<SideError>>) -> Self {
        if rows.iter().all(Vec::is_empty) {
            rows.clear();
        }
        Self { row_count, rows }
    }

    pub fn row_lengths(&self) -> RowErrorLengths {
        RowErrorLengths(self.rows.iter().map(Vec::len).collect())
    }

    /// Drops errors recorded past `lengths` for every row the instruction did not select,
    /// so a conditional arm cannot leak errors from a branch it did not take.
    pub fn restore_unselected(
        &mut self,
        lengths: &RowErrorLengths,
        selected: impl Fn(usize) -> bool,
    ) {
        for (row, errors) in self.rows.iter_mut().enumerate() {
            if !selected(row) {
                errors.truncate(lengths.0.get(row).copied().unwrap_or_default());
            }
        }
        if self.rows.iter().all(Vec::is_empty) {
            self.rows.clear();
        }
    }
}

impl<'a> RowErrorMask<'a> {
    pub fn none(row_count: usize) -> Self {
        Self {
            row_count,
            rows: &[],
        }
    }

    pub fn len(self) -> usize {
        self.row_count
    }

    pub fn is_empty(self) -> bool {
        self.row_count == 0
    }

    pub fn is_error_free(self) -> bool {
        self.rows.is_empty()
    }

    pub fn contains(self, row: usize) -> bool {
        self.rows.get(row).is_some_and(|errors| !errors.is_empty())
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = bool> + 'a {
        let row_count = self.row_count;
        let rows = self.rows;
        (0..row_count).map(move |row| rows.get(row).is_some_and(|errors| !errors.is_empty()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message} at {span}")]
pub struct CompileError {
    pub code: &'static str,
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("batch schema does not match compiled schema")]
    SchemaMismatch,
    #[error("invalid batch: {message}")]
    InvalidBatch { message: String },
    #[error("required output column '{column}' is uninitialized")]
    UninitializedRequiredColumn { column: String },
    #[error("required output column '{column}' contains null values")]
    NullForRequiredColumn { column: String },
    #[error("missing register {reg}")]
    MissingRegister { reg: RegisterRef },
    #[error("register {reg} does not contain {expected}")]
    InvalidRegisterType {
        reg: RegisterRef,
        expected: &'static str,
    },
    #[error("unsupported column type {data_type:?}")]
    UnsupportedColumnType { data_type: DataType },
    #[error("blocking execution task failed: {message}")]
    BlockingExecutionFailed { message: String },
    #[error("function '{function}' requires caller-supplied values")]
    MissingFunctionInjector { function: String },
    #[error(
        "caller supplied invalid result for function '{function}': expected {expected_type:?} \
         with {expected_rows} rows, got {actual_type:?} with {actual_rows} rows"
    )]
    InvalidInjectedResult {
        function: String,
        expected_type: DataType,
        actual_type: DataType,
        expected_rows: usize,
        actual_rows: usize,
    },
    #[error(
        "caller supplied an invalid side error for function '{function}' at row {row}; batch has \
         {row_count} rows"
    )]
    InvalidInjectedSideError {
        function: String,
        row: usize,
        row_count: usize,
    },
    #[error("injected function '{function}' failed: {message}")]
    InjectedFunctionFailed { function: String, message: String },
}

#[cfg(test)]
mod tests {
    use super::{ErrorCode, RowErrors, SideError};

    #[test]
    fn error_code_strings_are_stable() {
        assert_eq!(ErrorCode::DivisionByZero.as_str(), "division_by_zero");
        assert_eq!(ErrorCode::Overflow.as_str(), "overflow");
        assert_eq!(ErrorCode::CastFailed.as_str(), "cast_failed");
        assert_eq!(ErrorCode::InvalidArgument.as_str(), "invalid_argument");
    }

    #[test]
    fn sparse_error_masks_do_not_materialize_clean_rows() {
        let mut errors = RowErrors::new(3);

        assert!(errors.is_error_free());
        assert_eq!(
            errors.mask().iter().collect::<Vec<_>>(),
            [false, false, false]
        );

        errors.push(
            1,
            SideError {
                code: ErrorCode::InvalidArgument,
                message: "invalid input".to_string(),
                span: (0..1).into(),
            },
        );

        let mask = errors.mask();
        assert!(!mask.is_error_free());
        assert_eq!(mask.iter().collect::<Vec<_>>(), [false, true, false]);
        assert!(!mask.contains(0));
        assert!(mask.contains(1));

        let clean = errors.select_rows(&[0, 2]);
        assert_eq!(clean.row_count(), 2);
        assert!(clean.is_error_free());
    }
}
