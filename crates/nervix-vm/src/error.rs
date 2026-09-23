use std::iter;

use arrow_buffer::BooleanBuffer;
use arrow_schema::DataType;
use strum::IntoStaticStr;
use thiserror::Error;

use crate::{
    datetime::{UnreadableText, Zone},
    ir::{RegisterRef, RegisterType},
    program::Span,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ErrorCode {
    DivisionByZero,
    Overflow,
    CastFailed,
    InvalidArgument,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// One row's failure, recorded where the operation that failed executed.
///
/// The failure keeps the typed reason execution observed. Its text is formatted only where the
/// error is reported, so a batch whose rows fail builds no message for any of them.
#[derive(Debug, Clone, PartialEq)]
pub struct SideError {
    pub reason: SideErrorReason,
    pub span: Span,
}

impl SideError {
    pub fn code(&self) -> ErrorCode {
        self.reason.code()
    }
}

/// Why one row of an operation failed.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum SideErrorReason {
    #[error("integer {0} overflowed")]
    IntegerOverflow(IntegerOperation),
    #[error("integer {0} by zero")]
    DivisionByZero(DivisionOperation),
    #[error("integer {0} by a negative count")]
    NegativeShiftCount(ShiftOperation),
    #[error("{0} produced a non-finite result")]
    NonFiniteResult(FloatOperation),
    #[error("cannot cast value to {target}")]
    CastFailed { target: RegisterType },
    #[error("invalid regular expression: {0}")]
    InvalidRegularExpression(regex::Error),
    /// A datetime builtin whose result lies outside the signed Unix-nanosecond range.
    #[error("{0} result is outside the DATETIME range")]
    DatetimeOutOfRange(DatetimeOperation),
    /// A `date_diff` whose count of units does not fit its `I64` result.
    #[error("date_diff result does not fit I64")]
    DateDiffOverflow,
    /// A `parse_datetime` input that is not a date and time in the call's format.
    #[error("parse_datetime {0}")]
    UnreadableDatetime(UnreadableText),
    /// A `parse_datetime` input naming a local time that its zone skips.
    #[error("parse_datetime local time does not exist in {zone}")]
    SkippedLocalTime { zone: Zone },
    /// A `parse_datetime` input naming a local time that its zone repeats.
    #[error("parse_datetime local time is ambiguous in {zone}")]
    RepeatedLocalTime { zone: Zone },
    /// A failure an injected function reported, with the code and text that function chose.
    #[error("{message}")]
    Injected { code: ErrorCode, message: String },
}

impl SideErrorReason {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::IntegerOverflow(_) | Self::DatetimeOutOfRange(_) | Self::DateDiffOverflow => {
                ErrorCode::Overflow
            }
            Self::DivisionByZero(_) => ErrorCode::DivisionByZero,
            Self::NegativeShiftCount(_)
            | Self::NonFiniteResult(_)
            | Self::InvalidRegularExpression(_)
            | Self::SkippedLocalTime { .. }
            | Self::RepeatedLocalTime { .. } => ErrorCode::InvalidArgument,
            Self::CastFailed { .. } | Self::UnreadableDatetime(_) => ErrorCode::CastFailed,
            Self::Injected { code, .. } => *code,
        }
    }
}

/// An integer operation whose result did not fit its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum IntegerOperation {
    #[strum(to_string = "addition")]
    Addition,
    #[strum(to_string = "subtraction")]
    Subtraction,
    #[strum(to_string = "multiplication")]
    Multiplication,
    #[strum(to_string = "division")]
    Division,
    #[strum(to_string = "negation")]
    Negation,
    #[strum(to_string = "absolute value")]
    AbsoluteValue,
    #[strum(to_string = "sum")]
    Sum,
    #[strum(to_string = "left shift")]
    LeftShift,
    /// `round` with a negative number of digits, which rounds to a multiple of a power of ten.
    #[strum(to_string = "rounding")]
    Rounding,
}

/// An integer operation that shifts a value's bits by a count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum ShiftOperation {
    #[strum(to_string = "left shift")]
    LeftShift,
    #[strum(to_string = "right shift")]
    RightShift,
}

/// An integer operation that divides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum DivisionOperation {
    #[strum(to_string = "division")]
    Division,
    #[strum(to_string = "remainder")]
    Remainder,
}

/// A datetime builtin whose result is a DATETIME.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum DatetimeOperation {
    #[strum(to_string = "date_trunc")]
    DateTrunc,
    #[strum(to_string = "date_bin")]
    DateBin,
    #[strum(to_string = "date_add")]
    DateAdd,
    #[strum(to_string = "from_unix")]
    FromUnix,
    #[strum(to_string = "parse_datetime")]
    ParseDatetime,
}

/// A floating-point operation whose result has to be finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum FloatOperation {
    /// An arithmetic operator.
    #[strum(to_string = "floating-point operation")]
    Arithmetic,
    #[strum(to_string = "floating-point absolute value")]
    AbsoluteValue,
    #[strum(to_string = "floating-point sum")]
    Sum,
    #[strum(to_string = "ceil")]
    Ceil,
    #[strum(to_string = "floor")]
    Floor,
    #[strum(to_string = "round")]
    Round,
    #[strum(to_string = "trunc")]
    Trunc,
    #[strum(to_string = "sign")]
    Sign,
    #[strum(to_string = "acos")]
    Acos,
    #[strum(to_string = "asin")]
    Asin,
    #[strum(to_string = "atan")]
    Atan,
    #[strum(to_string = "atan2")]
    Atan2,
    #[strum(to_string = "cos")]
    Cos,
    #[strum(to_string = "exp")]
    Exp,
    #[strum(to_string = "ln")]
    Ln,
    /// `log`, with one argument or two.
    #[strum(to_string = "log")]
    Log,
    #[strum(to_string = "log2")]
    Log2,
    #[strum(to_string = "pow")]
    Pow,
    #[strum(to_string = "radians")]
    Radians,
    #[strum(to_string = "degrees")]
    Degrees,
    #[strum(to_string = "sin")]
    Sin,
    #[strum(to_string = "sqrt")]
    Sqrt,
    #[strum(to_string = "tan")]
    Tan,
}

/// Row-aligned side errors recorded while a program executes.
///
/// Executions that record no error are the common case, so the per-row storage is
/// materialized only when the first error arrives. Until then the channel carries just a
/// row count, which keeps construction and cloning independent of the batch row count.
#[derive(Debug, Clone, Default, PartialEq)]
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
        match self.rows.get(row) {
            Some(errors) => errors.as_slice(),
            None => &[],
        }
    }

    pub fn get(&self, row: usize) -> Option<&[SideError]> {
        (row < self.row_count).then(|| self.row(row))
    }

    pub fn iter(&self) -> impl Iterator<Item = &[SideError]> {
        (0..self.row_count).map(|row| self.row(row))
    }

    pub fn push(&mut self, row: usize, error: SideError) {
        if self.rows.is_empty() {
            // `vec![Vec::new(); n]` clones the empty row once per row, which costs far more than
            // constructing each one.
            self.rows = iter::repeat_with(Vec::new).take(self.row_count).collect();
        }
        self.rows[row].push(error);
    }

    /// The rows holding an error recorded inside `span`, as a bitmap over the batch, or `None`
    /// when no row does. A batch without errors answers without visiting a row.
    pub(crate) fn rows_failed_within(&self, span: Span) -> Option<BooleanBuffer> {
        if self.rows.is_empty() {
            return None;
        }
        let failed = BooleanBuffer::collect_bool(self.row_count, |row| {
            self.row(row).iter().any(|error| span.contains(error.span))
        });
        if failed.count_set_bits() == 0 {
            return None;
        }
        Some(failed)
    }

    /// Records one error for every row in `rows`, built from that row alone, so an operation
    /// constructs an error only for a row that failed.
    pub(crate) fn push_failures(
        &mut self,
        rows: impl IntoIterator<Item = usize>,
        span: Span,
        reason: impl Fn(usize) -> SideErrorReason,
    ) {
        for row in rows {
            let error = SideError {
                reason: reason(row),
                span,
            };
            self.push(row, error);
        }
    }

    /// Builds the channel for a filtered batch, keeping only `rows` in the order given.
    pub fn select_rows(&self, rows: &[usize]) -> Self {
        if self.rows.is_empty() {
            return Self::new(rows.len());
        }
        let mut selected = Vec::with_capacity(rows.len());
        for &row in rows {
            let errors = self.row(row);
            // Copying an empty row still clones through `to_vec`, so only failed rows are copied.
            if errors.is_empty() {
                selected.push(Vec::new());
            } else {
                selected.push(errors.to_vec());
            }
        }
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

    /// Moves every error into `target`, on the row of `target` that `rows` names for each row of
    /// this channel in turn, so the errors of an instruction narrowed to the rows its arm
    /// selects land on the rows that selected it.
    pub(crate) fn scatter_into(self, target: &mut RowErrors, rows: impl Iterator<Item = usize>) {
        if self.rows.is_empty() {
            return;
        }
        for (errors, row) in self.rows.into_iter().zip(rows) {
            for error in errors {
                target.push(row, error);
            }
        }
    }

    /// Drops errors recorded past `lengths` for every row the instruction did not select, so an
    /// arm that runs a vectorized kernel over the whole batch reports nothing for a row it did
    /// not select.
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
    /// A batch whose formatted datetimes could exceed the 2 GiB of text one STRING column holds.
    #[error(
        "format_datetime values for {rows} messages of up to {longest} bytes each could exceed \
         the text one STRING column holds"
    )]
    FormattedDatetimesTooLarge { rows: usize, longest: usize },
}

#[cfg(test)]
mod tests {
    use super::{
        DatetimeOperation, DivisionOperation, ErrorCode, FloatOperation, IntegerOperation,
        RowErrors, ShiftOperation, SideError, SideErrorReason,
    };
    use crate::ir::RegisterType;

    #[test]
    fn error_code_strings_are_stable() {
        assert_eq!(ErrorCode::DivisionByZero.as_str(), "division_by_zero");
        assert_eq!(ErrorCode::Overflow.as_str(), "overflow");
        assert_eq!(ErrorCode::CastFailed.as_str(), "cast_failed");
        assert_eq!(ErrorCode::InvalidArgument.as_str(), "invalid_argument");
    }

    #[test]
    fn side_error_reasons_render_their_published_messages_and_codes() {
        let integer_overflows = [
            (IntegerOperation::Addition, "integer addition overflowed"),
            (
                IntegerOperation::Subtraction,
                "integer subtraction overflowed",
            ),
            (
                IntegerOperation::Multiplication,
                "integer multiplication overflowed",
            ),
            (IntegerOperation::Division, "integer division overflowed"),
            (IntegerOperation::Negation, "integer negation overflowed"),
            (
                IntegerOperation::AbsoluteValue,
                "integer absolute value overflowed",
            ),
            (IntegerOperation::Sum, "integer sum overflowed"),
            (IntegerOperation::LeftShift, "integer left shift overflowed"),
            (IntegerOperation::Rounding, "integer rounding overflowed"),
        ];
        for (operation, message) in integer_overflows {
            let reason = SideErrorReason::IntegerOverflow(operation);
            assert_eq!(reason.to_string(), message);
            assert_eq!(reason.code(), ErrorCode::Overflow);
        }

        let divisions = [
            (DivisionOperation::Division, "integer division by zero"),
            (DivisionOperation::Remainder, "integer remainder by zero"),
        ];
        for (operation, message) in divisions {
            let reason = SideErrorReason::DivisionByZero(operation);
            assert_eq!(reason.to_string(), message);
            assert_eq!(reason.code(), ErrorCode::DivisionByZero);
        }

        let negative_shift_counts = [
            (
                ShiftOperation::LeftShift,
                "integer left shift by a negative count",
            ),
            (
                ShiftOperation::RightShift,
                "integer right shift by a negative count",
            ),
        ];
        for (operation, message) in negative_shift_counts {
            let reason = SideErrorReason::NegativeShiftCount(operation);
            assert_eq!(reason.to_string(), message);
            assert_eq!(reason.code(), ErrorCode::InvalidArgument);
        }

        let non_finite = [
            (
                FloatOperation::Arithmetic,
                "floating-point operation produced a non-finite result",
            ),
            (
                FloatOperation::AbsoluteValue,
                "floating-point absolute value produced a non-finite result",
            ),
            (
                FloatOperation::Sum,
                "floating-point sum produced a non-finite result",
            ),
            (FloatOperation::Ceil, "ceil produced a non-finite result"),
            (FloatOperation::Floor, "floor produced a non-finite result"),
            (FloatOperation::Round, "round produced a non-finite result"),
            (FloatOperation::Trunc, "trunc produced a non-finite result"),
            (FloatOperation::Sign, "sign produced a non-finite result"),
            (FloatOperation::Acos, "acos produced a non-finite result"),
            (FloatOperation::Asin, "asin produced a non-finite result"),
            (FloatOperation::Atan, "atan produced a non-finite result"),
            (FloatOperation::Atan2, "atan2 produced a non-finite result"),
            (FloatOperation::Cos, "cos produced a non-finite result"),
            (FloatOperation::Exp, "exp produced a non-finite result"),
            (FloatOperation::Ln, "ln produced a non-finite result"),
            (FloatOperation::Log, "log produced a non-finite result"),
            (FloatOperation::Log2, "log2 produced a non-finite result"),
            (FloatOperation::Pow, "pow produced a non-finite result"),
            (
                FloatOperation::Radians,
                "radians produced a non-finite result",
            ),
            (
                FloatOperation::Degrees,
                "degrees produced a non-finite result",
            ),
            (FloatOperation::Sin, "sin produced a non-finite result"),
            (FloatOperation::Sqrt, "sqrt produced a non-finite result"),
            (FloatOperation::Tan, "tan produced a non-finite result"),
        ];
        for (operation, message) in non_finite {
            let reason = SideErrorReason::NonFiniteResult(operation);
            assert_eq!(reason.to_string(), message);
            assert_eq!(reason.code(), ErrorCode::InvalidArgument);
        }

        let datetime_ranges = [
            (
                DatetimeOperation::DateTrunc,
                "date_trunc result is outside the DATETIME range",
            ),
            (
                DatetimeOperation::DateBin,
                "date_bin result is outside the DATETIME range",
            ),
            (
                DatetimeOperation::DateAdd,
                "date_add result is outside the DATETIME range",
            ),
            (
                DatetimeOperation::FromUnix,
                "from_unix result is outside the DATETIME range",
            ),
        ];
        for (operation, message) in datetime_ranges {
            let reason = SideErrorReason::DatetimeOutOfRange(operation);
            assert_eq!(reason.to_string(), message);
            assert_eq!(reason.code(), ErrorCode::Overflow);
        }
        assert_eq!(
            SideErrorReason::DateDiffOverflow.to_string(),
            "date_diff result does not fit I64"
        );
        assert_eq!(
            SideErrorReason::DateDiffOverflow.code(),
            ErrorCode::Overflow
        );

        let cast = SideErrorReason::CastFailed {
            target: RegisterType::Int64,
        };
        assert_eq!(cast.to_string(), "cannot cast value to Int64");
        assert_eq!(cast.code(), ErrorCode::CastFailed);

        let pattern_error = regex::Error::CompiledTooBig(1024);
        let expected = format!("invalid regular expression: {pattern_error}");
        let invalid_pattern = SideErrorReason::InvalidRegularExpression(pattern_error);
        assert_eq!(invalid_pattern.to_string(), expected);
        assert_eq!(invalid_pattern.code(), ErrorCode::InvalidArgument);

        let injected = SideErrorReason::Injected {
            code: ErrorCode::DivisionByZero,
            message: "UDF 'ratio': div failed: division by zero".to_string(),
        };
        assert_eq!(
            injected.to_string(),
            "UDF 'ratio': div failed: division by zero"
        );
        assert_eq!(injected.code(), ErrorCode::DivisionByZero);
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
                reason: SideErrorReason::NonFiniteResult(FloatOperation::Sqrt),
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
