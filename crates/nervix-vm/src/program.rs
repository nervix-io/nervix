//! Layer: engines and infrastructure.
//!
//! - **Owns.** The typed program vocabulary consumed by the expression compiler and runtime.
//! - **Depends on.** Primitive Rust and Arrow value types, and the datetime constants a call
//!   resolves when it is lowered: time zones and compiled datetime formats.
//! - **Must not know.** NSPL tokens, parser diagnostics, Models, or the execution graph.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    num::NonZeroU64,
    ops::{Deref, DerefMut, Range},
};

use arrow_schema::DataType;
use meticulous::OptionExt as _;
use strum::{AsRefStr, EnumString, IntoStaticStr, VariantNames};

pub use crate::datetime::{DatetimeFormat, DatetimeParser, Zone};
use crate::json::JsonExtraction;

/// Identifies one semantic operation in a lowered VM program.
///
/// The range is assigned by the VM frontend and is independent of source text. Runtime side
/// errors use it to recover the route-local operation metadata prepared by the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    /// Whether `other` lies entirely inside this span, as the span of a nested operation does.
    pub fn contains(self, other: Self) -> bool {
        other.start >= self.start && other.end <= self.end
    }
}

impl From<Range<usize>> for Span {
    fn from(range: Range<usize>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<Span> for Range<usize> {
    fn from(span: Span) -> Self {
        span.start..span.end
    }
}

impl fmt::Display for Span {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..{}", self.start, self.end)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpannedNode<T> {
    pub inner: T,
    pub span: Span,
}

impl<T> Deref for SpannedNode<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for SpannedNode<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub type SpannedExpr = SpannedNode<Expr>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldRef {
    pub relay: String,
    pub field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InternalFieldNamespace {
    LookupHashMap,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InternalFieldRef {
    pub namespace: InternalFieldNamespace,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub filter: Option<SpannedExpr>,
    pub set: Vec<(FieldRef, SpannedExpr)>,
    pub invoke: Vec<SpannedInvocation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Invocation {
    pub function: FunctionName,
    pub args: Vec<SpannedExpr>,
}

pub type SpannedInvocation = SpannedNode<Invocation>;

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Literal),
    FieldRef(FieldRef),
    InternalFieldRef(InternalFieldRef),
    Unary {
        op: UnaryOp,
        expr: Box<SpannedExpr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<SpannedExpr>,
        right: Box<SpannedExpr>,
    },
    Cast {
        expr: Box<SpannedExpr>,
        data_type: DataType,
        on_failure: CastFailure,
    },
    Call {
        function: FunctionName,
        args: Vec<SpannedExpr>,
    },
    Case {
        operand: Option<Box<SpannedExpr>>,
        branches: Vec<CaseArm>,
        else_result: Option<Box<SpannedExpr>>,
    },
    /// `IN`: whether the operand equals an element of a set of constants. `NOT IN` is the negation
    /// of this test.
    Membership {
        operand: Box<SpannedExpr>,
        set: Vec<SpannedExpr>,
    },
    /// `BETWEEN`: whether the operand lies in the inclusive range from `low` to `high`. `NOT
    /// BETWEEN` is the negation of this test.
    Between {
        operand: Box<SpannedExpr>,
        low: Box<SpannedExpr>,
        high: Box<SpannedExpr>,
    },
    /// One extraction from the JSON text `document` holds. Every extraction a program makes from
    /// one document column is answered by a single parse of each of its documents.
    Json {
        document: Box<SpannedExpr>,
        extraction: JsonExtraction,
    },
}

/// What a cast yields for a value its target type cannot hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CastFailure {
    /// The row reports a `cast_failed` error and yields null, as `expr AS TYPE` does.
    Error,
    /// The row yields a typed null and reports nothing, as `TRY_CAST(expr AS TYPE)` does.
    Null,
}

impl CastFailure {
    /// Whether a value that does not convert reports a per-row error.
    pub const fn reports_error(self) -> bool {
        match self {
            Self::Error => true,
            Self::Null => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CaseArm {
    pub when: SpannedExpr,
    pub result: SpannedExpr,
}

/// Compares two expressions ignoring their operation ranges.
///
/// Ranges locate route-local operation metadata, not what an expression computes. Two routes of
/// the same node can lower an identical `LOOKUP_HASH_MAP` key at different positions, so
/// range-sensitive comparison would report them as distinct and defeat any sharing keyed on
/// expression identity.
fn cmp_spanned(left: &SpannedExpr, right: &SpannedExpr) -> Ordering {
    left.inner.cmp(&right.inner)
}

fn cmp_spanned_slice(left: &[SpannedExpr], right: &[SpannedExpr]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| {
        left.iter()
            .zip(right)
            .map(|(left, right)| cmp_spanned(left, right))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal)
    })
}

fn cmp_spanned_option(left: Option<&SpannedExpr>, right: Option<&SpannedExpr>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => cmp_spanned(left, right),
    }
}

impl Ord for CaseArm {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_spanned(&self.when, &other.when).then_with(|| cmp_spanned(&self.result, &other.result))
    }
}

impl PartialOrd for CaseArm {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CaseArm {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for CaseArm {}

impl Expr {
    /// Whether evaluating this expression under a conditional arm may leave a null, rather than
    /// its value, on the rows the arm does not select. A call out of the VM is made for the
    /// selected rows only, a cast that yields null for a failure may convert only those rows, and
    /// a JSON extraction parses only their documents.
    /// Every other operation confined to the selected rows can report an error, and an expression
    /// that can report one is never shared in the first place.
    pub(crate) fn may_answer_selected_rows_only(&self) -> bool {
        match self {
            Self::Literal(_) | Self::FieldRef(_) | Self::InternalFieldRef(_) => false,
            Self::Unary { expr, .. } => expr.inner.may_answer_selected_rows_only(),
            Self::Cast {
                expr, on_failure, ..
            } => match on_failure {
                CastFailure::Null => true,
                CastFailure::Error => expr.inner.may_answer_selected_rows_only(),
            },
            Self::Binary { left, right, .. } => {
                left.inner.may_answer_selected_rows_only()
                    || right.inner.may_answer_selected_rows_only()
            }
            Self::Call { function, args } => {
                if function.is_injected() {
                    return true;
                }
                for argument in args {
                    if argument.inner.may_answer_selected_rows_only() {
                        return true;
                    }
                }
                false
            }
            Self::Case {
                operand,
                branches,
                else_result,
            } => {
                if let Some(operand) = operand
                    && operand.inner.may_answer_selected_rows_only()
                {
                    return true;
                }
                for branch in branches {
                    if branch.when.inner.may_answer_selected_rows_only()
                        || branch.result.inner.may_answer_selected_rows_only()
                    {
                        return true;
                    }
                }
                if let Some(else_result) = else_result {
                    return else_result.inner.may_answer_selected_rows_only();
                }
                false
            }
            Self::Membership { operand, set } => {
                if operand.inner.may_answer_selected_rows_only() {
                    return true;
                }
                for element in set {
                    if element.inner.may_answer_selected_rows_only() {
                        return true;
                    }
                }
                false
            }
            Self::Between { operand, low, high } => {
                operand.inner.may_answer_selected_rows_only()
                    || low.inner.may_answer_selected_rows_only()
                    || high.inner.may_answer_selected_rows_only()
            }
            // A scan parses only the documents of the rows its arm selects.
            Self::Json { .. } => true,
        }
    }

    const fn discriminant(&self) -> u8 {
        match self {
            Self::Literal(_) => 0,
            Self::FieldRef(_) => 1,
            Self::InternalFieldRef(_) => 2,
            Self::Unary { .. } => 3,
            Self::Binary { .. } => 4,
            Self::Cast { .. } => 5,
            Self::Call { .. } => 6,
            Self::Case { .. } => 7,
            Self::Membership { .. } => 8,
            Self::Between { .. } => 9,
            Self::Json { .. } => 10,
        }
    }
}

impl Ord for Expr {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Literal(left), Self::Literal(right)) => left.cmp(right),
            (Self::FieldRef(left), Self::FieldRef(right)) => left.cmp(right),
            (Self::InternalFieldRef(left), Self::InternalFieldRef(right)) => left.cmp(right),
            (
                Self::Unary {
                    op: left_op,
                    expr: left_expr,
                },
                Self::Unary {
                    op: right_op,
                    expr: right_expr,
                },
            ) => left_op
                .cmp(right_op)
                .then_with(|| cmp_spanned(left_expr, right_expr)),
            (
                Self::Binary {
                    op: left_op,
                    left: left_left,
                    right: left_right,
                },
                Self::Binary {
                    op: right_op,
                    left: right_left,
                    right: right_right,
                },
            ) => left_op
                .cmp(right_op)
                .then_with(|| cmp_spanned(left_left, right_left))
                .then_with(|| cmp_spanned(left_right, right_right)),
            (
                Self::Cast {
                    expr: left_expr,
                    data_type: left_type,
                    on_failure: left_failure,
                },
                Self::Cast {
                    expr: right_expr,
                    data_type: right_type,
                    on_failure: right_failure,
                },
            ) => left_type
                .cmp(right_type)
                .then_with(|| left_failure.cmp(right_failure))
                .then_with(|| cmp_spanned(left_expr, right_expr)),
            (
                Self::Call {
                    function: left_function,
                    args: left_args,
                },
                Self::Call {
                    function: right_function,
                    args: right_args,
                },
            ) => left_function
                .cmp(right_function)
                .then_with(|| cmp_spanned_slice(left_args, right_args)),
            (
                Self::Case {
                    operand: left_operand,
                    branches: left_branches,
                    else_result: left_else,
                },
                Self::Case {
                    operand: right_operand,
                    branches: right_branches,
                    else_result: right_else,
                },
            ) => cmp_spanned_option(left_operand.as_deref(), right_operand.as_deref())
                .then_with(|| left_branches.cmp(right_branches))
                .then_with(|| cmp_spanned_option(left_else.as_deref(), right_else.as_deref())),
            (
                Self::Membership {
                    operand: left_operand,
                    set: left_set,
                },
                Self::Membership {
                    operand: right_operand,
                    set: right_set,
                },
            ) => cmp_spanned(left_operand, right_operand)
                .then_with(|| cmp_spanned_slice(left_set, right_set)),
            (
                Self::Between {
                    operand: left_operand,
                    low: left_low,
                    high: left_high,
                },
                Self::Between {
                    operand: right_operand,
                    low: right_low,
                    high: right_high,
                },
            ) => cmp_spanned(left_operand, right_operand)
                .then_with(|| cmp_spanned(left_low, right_low))
                .then_with(|| cmp_spanned(left_high, right_high)),
            (
                Self::Json {
                    document: left_document,
                    extraction: left_extraction,
                },
                Self::Json {
                    document: right_document,
                    extraction: right_extraction,
                },
            ) => left_extraction
                .cmp(right_extraction)
                .then_with(|| cmp_spanned(left_document, right_document)),
            _ => self.discriminant().cmp(&other.discriminant()),
        }
    }
}

impl PartialOrd for Expr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Expr {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Expr {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FunctionName {
    Now,
    UuidV4,
    UuidV7,
    Lower,
    Upper,
    Trim,
    Btrim,
    Ltrim,
    Rtrim,
    Length,
    CharLength,
    BitLength,
    Ascii,
    Coalesce,
    IsNull,
    NullIf,
    Abs,
    Acos,
    Asin,
    Atan,
    Ceil,
    Cos,
    Exp,
    Floor,
    Initcap,
    Left,
    Ln,
    Log,
    Lpad,
    Md5,
    BytesFromUtf8,
    BytesToUtf8,
    Base64Encode,
    Base64Decode,
    HexEncode,
    HexDecode,
    Sha256,
    Xxh3_64,
    IpFromString,
    IsIpAddress,
    IpToString,
    IpFamily,
    IpTrunc,
    IpInNetwork,
    IpUnmap,
    UrlScheme,
    UrlHost,
    UrlPort,
    UrlPath,
    UrlQuery,
    UrlFragment,
    UrlQueryValue,
    UrlQueryValues,
    UrlDecode,
    IsUrl,
    Pow,
    Repeat,
    Replace,
    Reverse,
    Right,
    Round,
    Rpad,
    SplitPart,
    Sqrt,
    Strpos,
    Substr,
    Tan,
    ToHex,
    Translate,
    Sin,
    Atan2,
    Log2,
    Radians,
    Degrees,
    Sign,
    Trunc,
    IsNan,
    IsFinite,
    IsInfinite,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseNot,
    ShiftLeft,
    ShiftRight,
    BitCount,
    Greatest,
    Least,
    Clamp,
    Concat,
    Array,
    Vec,
    Overlap,
    Slice,
    Min,
    Max,
    Mean,
    Dot,
    Distance,
    Sum,
    Last,
    First,
    Count,
    Nth,
    Contains,
    StartsWith,
    EndsWith,
    RegexpLike,
    RegexpReplace,
    RegexpSubstr,
    Datetime(DatetimeFunction),
    LeakSensitive,
    LookupHashMap,
    ReadHeader,
    ReadHeaders,
    WriteHeader,
    WindowAggregate(WindowAggregateInvocation),
    Udf(String),
    Unknown(String),
}

/// A datetime builtin with the constant arguments that select what it computes.
///
/// A call names its unit, date part, bin width, time zone, format and disambiguation with
/// literals. The frontend reads those literals once, when it lowers the call, into units, resolved
/// time zones and compiled formats, so the call keeps only the arguments that vary by row and
/// execution never interprets text or looks a zone up.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DatetimeFunction {
    /// `date_part(part, value[, zone])`.
    DatePart { part: DatePart, zone: Zone },
    /// `date_trunc(unit, value[, zone])`.
    DateTrunc { unit: DatetimeUnit, zone: Zone },
    /// `date_bin(unit, width, value, origin)`.
    DateBin(DateBinWidth),
    /// `date_add(unit, amount, value[, zone])`.
    DateAdd { unit: DatetimeUnit, zone: Zone },
    /// `date_diff(unit, start, end[, zone])`.
    DateDiff { unit: DatetimeUnit, zone: Zone },
    /// `to_unix(unit, value)`.
    ToUnix(FixedTimeUnit),
    /// `from_unix(unit, count)`.
    FromUnix(FixedTimeUnit),
    /// `format_datetime(format, value[, zone])`.
    FormatDatetime { format: DatetimeFormat, zone: Zone },
    /// `parse_datetime(format, text[, zone[, disambiguation]])`.
    ParseDatetime(DatetimeParser),
}

impl DatetimeFunction {
    pub const fn name(&self) -> DatetimeFunctionName {
        match self {
            Self::DatePart { .. } => DatetimeFunctionName::DatePart,
            Self::DateTrunc { .. } => DatetimeFunctionName::DateTrunc,
            Self::DateBin(_) => DatetimeFunctionName::DateBin,
            Self::DateAdd { .. } => DatetimeFunctionName::DateAdd,
            Self::DateDiff { .. } => DatetimeFunctionName::DateDiff,
            Self::ToUnix(_) => DatetimeFunctionName::ToUnix,
            Self::FromUnix(_) => DatetimeFunctionName::FromUnix,
            Self::FormatDatetime { .. } => DatetimeFunctionName::FormatDatetime,
            Self::ParseDatetime(_) => DatetimeFunctionName::ParseDatetime,
        }
    }
}

/// The name of a datetime builtin, before its constant arguments are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, IntoStaticStr, strum::Display)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum DatetimeFunctionName {
    DatePart,
    DateTrunc,
    DateBin,
    DateAdd,
    DateDiff,
    ToUnix,
    FromUnix,
    FormatDatetime,
    ParseDatetime,
}

const NANOSECONDS_PER_MICROSECOND: i64 = 1_000;
const NANOSECONDS_PER_MILLISECOND: i64 = 1_000 * NANOSECONDS_PER_MICROSECOND;
const NANOSECONDS_PER_SECOND: i64 = 1_000 * NANOSECONDS_PER_MILLISECOND;
const NANOSECONDS_PER_MINUTE: i64 = 60 * NANOSECONDS_PER_SECOND;
const NANOSECONDS_PER_HOUR: i64 = 60 * NANOSECONDS_PER_MINUTE;
const NANOSECONDS_PER_DAY: i64 = 24 * NANOSECONDS_PER_HOUR;
const NANOSECONDS_PER_WEEK: i64 = 7 * NANOSECONDS_PER_DAY;

/// A unit of fixed length that datetime arithmetic counts in.
///
/// A DATETIME is a UTC instant without leap seconds, so every UTC day is exactly 86,400 seconds and
/// every UTC week exactly seven days. A calendar month, quarter or year has no fixed length and is a
/// [`CalendarUnit`] instead.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumString,
    VariantNames,
    strum::Display,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum FixedTimeUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl FixedTimeUnit {
    /// The exact length of one unit in nanoseconds.
    pub const fn nanoseconds(self) -> i64 {
        match self {
            Self::Nanosecond => 1,
            Self::Microsecond => NANOSECONDS_PER_MICROSECOND,
            Self::Millisecond => NANOSECONDS_PER_MILLISECOND,
            Self::Second => NANOSECONDS_PER_SECOND,
            Self::Minute => NANOSECONDS_PER_MINUTE,
            Self::Hour => NANOSECONDS_PER_HOUR,
            Self::Day => NANOSECONDS_PER_DAY,
            Self::Week => NANOSECONDS_PER_WEEK,
        }
    }
}

/// A calendar unit, whose length depends on the calendar month it counts from.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumString,
    VariantNames,
    strum::Display,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum CalendarUnit {
    Month,
    Quarter,
    Year,
}

impl CalendarUnit {
    /// How many calendar months one unit spans.
    pub const fn months(self) -> i64 {
        match self {
            Self::Month => 1,
            Self::Quarter => 3,
            Self::Year => 12,
        }
    }
}

/// A unit `date_trunc`, `date_add` and `date_diff` count in: a unit of fixed length, or a calendar
/// unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, strum::Display)]
pub enum DatetimeUnit {
    #[strum(to_string = "{0}")]
    Fixed(FixedTimeUnit),
    #[strum(to_string = "{0}")]
    Calendar(CalendarUnit),
}

impl std::str::FromStr for DatetimeUnit {
    type Err = strum::ParseError;

    /// Reads a unit name, without regard to letter case.
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        if let Ok(unit) = name.parse::<FixedTimeUnit>() {
            return Ok(Self::Fixed(unit));
        }
        name.parse::<CalendarUnit>().map(Self::Calendar)
    }
}

/// How `parse_datetime` chooses an instant for a local date and time that its time zone skips or
/// repeats.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumString,
    VariantNames,
    strum::Display,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum Disambiguation {
    /// The earlier instant of a repeated time, and the time moved past a skipped span by the span's
    /// length, as calendar arithmetic moves local times.
    Compatible,
    /// The earlier of the two instants the local time could mean.
    Earlier,
    /// The later of the two instants the local time could mean.
    Later,
    /// No instant: a skipped or repeated local time fails its row.
    Reject,
}

/// A part of the local date and time `date_part` reads from a DATETIME in a time zone.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    EnumString,
    VariantNames,
    strum::Display,
)]
#[strum(ascii_case_insensitive, serialize_all = "snake_case")]
pub enum DatePart {
    /// The proleptic Gregorian year.
    Year,
    /// The quarter of the year, from 1 to 4.
    Quarter,
    /// The month, from 1 to 12.
    Month,
    /// The day of the month, from 1 to 31.
    Day,
    /// The hour of the day, from 0 to 23.
    Hour,
    /// The minute of the hour, from 0 to 59.
    Minute,
    /// The whole seconds of the minute, from 0 to 59.
    Second,
    /// The whole milliseconds past the second, from 0 to 999.
    Millisecond,
    /// The whole microseconds past the second, from 0 to 999,999.
    Microsecond,
    /// The nanoseconds past the second, from 0 to 999,999,999.
    Nanosecond,
    /// The day of the week, from Sunday as 0 to Saturday as 6.
    DayOfWeek,
    /// The day of the year, from 1 to 366.
    DayOfYear,
    /// The ISO 8601 week-numbering year the ISO week belongs to.
    IsoYear,
    /// The ISO 8601 week, from 1 to 53.
    IsoWeek,
    /// The ISO 8601 day of the week, from Monday as 1 to Sunday as 7.
    IsoDayOfWeek,
}

/// The width of the bins `date_bin` places datetimes in: a positive count of one fixed unit, no
/// longer than `i64::MAX` nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateBinWidth {
    count: NonZeroU64,
    unit: FixedTimeUnit,
}

impl DateBinWidth {
    /// The width of `count` units, or `None` when it is longer than `i64::MAX` nanoseconds.
    pub fn new(count: NonZeroU64, unit: FixedTimeUnit) -> Option<Self> {
        let width = Self { count, unit };
        // A width exists exactly when its length in nanoseconds does.
        width.checked_nanoseconds()?;
        Some(width)
    }

    /// The exact width in nanoseconds.
    pub fn nanoseconds(self) -> i64 {
        self.checked_nanoseconds()
            .assured("construction rejected every width longer than i64::MAX nanoseconds")
    }

    fn checked_nanoseconds(self) -> Option<i64> {
        let count = i64::try_from(self.count.get()).ok()?;
        count.checked_mul(self.unit.nanoseconds())
    }
}

/// Every aggregate a window route can compute over the rows its window retains.
///
/// Variants are declared in name order, which is the order a shared aggregate structure lists
/// the functions it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, AsRefStr, EnumString)]
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WindowAggregateFunction {
    ArgMax,
    ArgMin,
    Avg,
    BoolAnd,
    BoolOr,
    Corr,
    Count,
    CountIf,
    CovarPop,
    CovarSamp,
    First,
    Last,
    Max,
    Min,
    PercentileLinearHistogram,
    StddevPop,
    StddevSamp,
    Sum,
    VarPop,
    VarSamp,
}

#[derive(Debug, Clone)]
pub struct WindowAggregateInvocation {
    pub demand_id: usize,
    pub function: WindowAggregateFunction,
    pub percentile: Option<f64>,
}

impl PartialEq for WindowAggregateInvocation {
    fn eq(&self, other: &Self) -> bool {
        self.demand_id == other.demand_id
            && self.function == other.function
            && self.percentile.map(f64::to_bits) == other.percentile.map(f64::to_bits)
    }
}

impl Eq for WindowAggregateInvocation {}

impl Hash for WindowAggregateInvocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.demand_id.hash(state);
        self.function.hash(state);
        self.percentile.map(f64::to_bits).hash(state);
    }
}

impl Ord for WindowAggregateInvocation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.demand_id
            .cmp(&other.demand_id)
            .then_with(|| self.function.cmp(&other.function))
            .then_with(|| {
                self.percentile
                    .map(f64::to_bits)
                    .cmp(&other.percentile.map(f64::to_bits))
            })
    }
}

impl PartialOrd for WindowAggregateInvocation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl WindowAggregateFunction {
    pub fn nspl_name(&self) -> &str {
        self.as_ref()
    }

    pub const fn expected_arity(self) -> usize {
        match self {
            Self::PercentileLinearHistogram => 6,
            Self::ArgMax | Self::ArgMin | Self::Corr | Self::CovarPop | Self::CovarSamp => 2,
            Self::Avg
            | Self::BoolAnd
            | Self::BoolOr
            | Self::Count
            | Self::CountIf
            | Self::First
            | Self::Last
            | Self::Max
            | Self::Min
            | Self::StddevPop
            | Self::StddevSamp
            | Self::Sum
            | Self::VarPop
            | Self::VarSamp => 1,
        }
    }

    /// Whether the function reads a second per-row argument. The histogram percentile's trailing
    /// arguments are constants of its configuration, not per-row arguments.
    pub const fn reads_argument_pair(self) -> bool {
        match self {
            Self::ArgMax | Self::ArgMin | Self::Corr | Self::CovarPop | Self::CovarSamp => true,
            Self::Avg
            | Self::BoolAnd
            | Self::BoolOr
            | Self::Count
            | Self::CountIf
            | Self::First
            | Self::Last
            | Self::Max
            | Self::Min
            | Self::PercentileLinearHistogram
            | Self::StddevPop
            | Self::StddevSamp
            | Self::Sum
            | Self::VarPop
            | Self::VarSamp => false,
        }
    }
}

impl FunctionName {
    /// Whether a call to this function is answered by an injected function outside the VM rather
    /// than by a builtin kernel: a header read, a window aggregate or a UDF.
    pub const fn is_injected(&self) -> bool {
        matches!(
            self,
            Self::ReadHeader | Self::ReadHeaders | Self::WindowAggregate(_) | Self::Udf(_)
        )
    }

    pub fn parse(name: &str) -> Self {
        match name.to_ascii_lowercase().as_str() {
            "now" => Self::Now,
            "uuid_v4" => Self::UuidV4,
            "uuid_v7" => Self::UuidV7,
            "lower" => Self::Lower,
            "upper" => Self::Upper,
            "trim" => Self::Trim,
            "btrim" => Self::Btrim,
            "ltrim" => Self::Ltrim,
            "rtrim" => Self::Rtrim,
            "length" => Self::Length,
            "char_length" => Self::CharLength,
            "bit_length" => Self::BitLength,
            "ascii" => Self::Ascii,
            "coalesce" => Self::Coalesce,
            "is_null" => Self::IsNull,
            "nullif" => Self::NullIf,
            "abs" => Self::Abs,
            "acos" => Self::Acos,
            "asin" => Self::Asin,
            "atan" => Self::Atan,
            "ceil" | "ceiling" => Self::Ceil,
            "cos" => Self::Cos,
            "exp" => Self::Exp,
            "floor" => Self::Floor,
            "initcap" => Self::Initcap,
            "left" => Self::Left,
            "ln" => Self::Ln,
            "log" => Self::Log,
            "lpad" => Self::Lpad,
            "md5" => Self::Md5,
            "bytes_from_utf8" => Self::BytesFromUtf8,
            "bytes_to_utf8" => Self::BytesToUtf8,
            "base64_encode" => Self::Base64Encode,
            "base64_decode" => Self::Base64Decode,
            "hex_encode" => Self::HexEncode,
            "hex_decode" => Self::HexDecode,
            "sha256" => Self::Sha256,
            "xxh3_64" => Self::Xxh3_64,
            "ip_from_string" => Self::IpFromString,
            "is_ip_address" => Self::IsIpAddress,
            "ip_to_string" => Self::IpToString,
            "ip_family" => Self::IpFamily,
            "ip_trunc" => Self::IpTrunc,
            "ip_in_network" => Self::IpInNetwork,
            "ip_unmap" => Self::IpUnmap,
            "url_scheme" => Self::UrlScheme,
            "url_host" => Self::UrlHost,
            "url_port" => Self::UrlPort,
            "url_path" => Self::UrlPath,
            "url_query" => Self::UrlQuery,
            "url_fragment" => Self::UrlFragment,
            "url_query_value" => Self::UrlQueryValue,
            "url_query_values" => Self::UrlQueryValues,
            "url_decode" => Self::UrlDecode,
            "is_url" => Self::IsUrl,
            "pow" | "power" => Self::Pow,
            "repeat" => Self::Repeat,
            "replace" => Self::Replace,
            "reverse" => Self::Reverse,
            "right" => Self::Right,
            "round" => Self::Round,
            "rpad" => Self::Rpad,
            "split_part" => Self::SplitPart,
            "sqrt" => Self::Sqrt,
            "strpos" => Self::Strpos,
            "substr" | "substring" => Self::Substr,
            "tan" => Self::Tan,
            "to_hex" => Self::ToHex,
            "translate" => Self::Translate,
            "sin" => Self::Sin,
            "atan2" => Self::Atan2,
            "log2" => Self::Log2,
            "radians" => Self::Radians,
            "degrees" => Self::Degrees,
            "sign" => Self::Sign,
            "trunc" => Self::Trunc,
            "is_nan" => Self::IsNan,
            "is_finite" => Self::IsFinite,
            "is_infinite" => Self::IsInfinite,
            "bitwise_and" => Self::BitwiseAnd,
            "bitwise_or" => Self::BitwiseOr,
            "bitwise_xor" => Self::BitwiseXor,
            "bitwise_not" => Self::BitwiseNot,
            "shift_left" => Self::ShiftLeft,
            "shift_right" => Self::ShiftRight,
            "bit_count" => Self::BitCount,
            "greatest" => Self::Greatest,
            "least" => Self::Least,
            "clamp" => Self::Clamp,
            "concat" => Self::Concat,
            "array" => Self::Array,
            "vec" => Self::Vec,
            "overlap" => Self::Overlap,
            "slice" => Self::Slice,
            "min" => Self::Min,
            "max" => Self::Max,
            "mean" => Self::Mean,
            "dot" => Self::Dot,
            "distance" => Self::Distance,
            "sum" => Self::Sum,
            "last" => Self::Last,
            "first" => Self::First,
            "count" => Self::Count,
            "nth" => Self::Nth,
            "contains" => Self::Contains,
            "starts_with" => Self::StartsWith,
            "ends_with" => Self::EndsWith,
            "regexp_like" => Self::RegexpLike,
            "regexp_replace" => Self::RegexpReplace,
            "regexp_substr" => Self::RegexpSubstr,
            "leak_sensitive" => Self::LeakSensitive,
            "lookup_hash_map" => Self::LookupHashMap,
            "read_header" => Self::ReadHeader,
            "read_headers" => Self::ReadHeaders,
            "write_header" => Self::WriteHeader,
            _ => Self::Unknown(name.to_string()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Now => "now",
            Self::UuidV4 => "uuid_v4",
            Self::UuidV7 => "uuid_v7",
            Self::Lower => "lower",
            Self::Upper => "upper",
            Self::Trim => "trim",
            Self::Btrim => "btrim",
            Self::Ltrim => "ltrim",
            Self::Rtrim => "rtrim",
            Self::Length => "length",
            Self::CharLength => "char_length",
            Self::BitLength => "bit_length",
            Self::Ascii => "ascii",
            Self::Coalesce => "coalesce",
            Self::IsNull => "is_null",
            Self::NullIf => "nullif",
            Self::Abs => "abs",
            Self::Acos => "acos",
            Self::Asin => "asin",
            Self::Atan => "atan",
            Self::Ceil => "ceil",
            Self::Cos => "cos",
            Self::Exp => "exp",
            Self::Floor => "floor",
            Self::Initcap => "initcap",
            Self::Left => "left",
            Self::Ln => "ln",
            Self::Log => "log",
            Self::Lpad => "lpad",
            Self::Md5 => "md5",
            Self::BytesFromUtf8 => "bytes_from_utf8",
            Self::BytesToUtf8 => "bytes_to_utf8",
            Self::Base64Encode => "base64_encode",
            Self::Base64Decode => "base64_decode",
            Self::HexEncode => "hex_encode",
            Self::HexDecode => "hex_decode",
            Self::Sha256 => "sha256",
            Self::Xxh3_64 => "xxh3_64",
            Self::IpFromString => "ip_from_string",
            Self::IsIpAddress => "is_ip_address",
            Self::IpToString => "ip_to_string",
            Self::IpFamily => "ip_family",
            Self::IpTrunc => "ip_trunc",
            Self::IpInNetwork => "ip_in_network",
            Self::IpUnmap => "ip_unmap",
            Self::UrlScheme => "url_scheme",
            Self::UrlHost => "url_host",
            Self::UrlPort => "url_port",
            Self::UrlPath => "url_path",
            Self::UrlQuery => "url_query",
            Self::UrlFragment => "url_fragment",
            Self::UrlQueryValue => "url_query_value",
            Self::UrlQueryValues => "url_query_values",
            Self::UrlDecode => "url_decode",
            Self::IsUrl => "is_url",
            Self::Pow => "pow",
            Self::Repeat => "repeat",
            Self::Replace => "replace",
            Self::Reverse => "reverse",
            Self::Right => "right",
            Self::Round => "round",
            Self::Rpad => "rpad",
            Self::SplitPart => "split_part",
            Self::Sqrt => "sqrt",
            Self::Strpos => "strpos",
            Self::Substr => "substr",
            Self::Tan => "tan",
            Self::ToHex => "to_hex",
            Self::Translate => "translate",
            Self::Sin => "sin",
            Self::Atan2 => "atan2",
            Self::Log2 => "log2",
            Self::Radians => "radians",
            Self::Degrees => "degrees",
            Self::Sign => "sign",
            Self::Trunc => "trunc",
            Self::IsNan => "is_nan",
            Self::IsFinite => "is_finite",
            Self::IsInfinite => "is_infinite",
            Self::BitwiseAnd => "bitwise_and",
            Self::BitwiseOr => "bitwise_or",
            Self::BitwiseXor => "bitwise_xor",
            Self::BitwiseNot => "bitwise_not",
            Self::ShiftLeft => "shift_left",
            Self::ShiftRight => "shift_right",
            Self::BitCount => "bit_count",
            Self::Greatest => "greatest",
            Self::Least => "least",
            Self::Clamp => "clamp",
            Self::Concat => "concat",
            Self::Array => "array",
            Self::Vec => "vec",
            Self::Overlap => "overlap",
            Self::Slice => "slice",
            Self::Min => "min",
            Self::Max => "max",
            Self::Mean => "mean",
            Self::Dot => "dot",
            Self::Distance => "distance",
            Self::Sum => "sum",
            Self::Last => "last",
            Self::First => "first",
            Self::Count => "count",
            Self::Nth => "nth",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
            Self::RegexpLike => "regexp_like",
            Self::RegexpReplace => "regexp_replace",
            Self::RegexpSubstr => "regexp_substr",
            Self::Datetime(function) => function.name().into(),
            Self::LeakSensitive => "leak_sensitive",
            Self::LookupHashMap => "lookup_hash_map",
            Self::ReadHeader => "read_header",
            Self::ReadHeaders => "read_headers",
            Self::WriteHeader => "write_header",
            Self::WindowAggregate(invocation) => invocation.function.as_ref(),
            Self::Udf(name) => name.as_str(),
            Self::Unknown(name) => name.as_str(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Float64(f64),
    Bool(bool),
    String(String),
    Null,
}

impl Literal {
    /// Orders variants by declaration, with `Float64` keyed on its bit pattern so the type has a
    /// total order. The order is a stable identity, not a numeric comparison.
    const fn discriminant(&self) -> u8 {
        match self {
            Self::Int64(_) => 0,
            Self::Float64(_) => 1,
            Self::Bool(_) => 2,
            Self::String(_) => 3,
            Self::Null => 4,
        }
    }
}

impl Ord for Literal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Int64(left), Self::Int64(right)) => left.cmp(right),
            (Self::Float64(left), Self::Float64(right)) => left.to_bits().cmp(&right.to_bits()),
            (Self::Bool(left), Self::Bool(right)) => left.cmp(right),
            (Self::String(left), Self::String(right)) => left.cmp(right),
            (Self::Null, Self::Null) => Ordering::Equal,
            _ => self.discriminant().cmp(&other.discriminant()),
        }
    }
}

impl PartialOrd for Literal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Literal {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    And,
    Or,
    /// `IS DISTINCT FROM`: never null, and true unless both operands are null or both are equal.
    IsDistinctFrom,
    /// `IS NOT DISTINCT FROM`: never null, and true when both operands are null or both are equal.
    IsNotDistinctFrom,
}

pub(crate) fn spanned<T>(inner: T, span: Span) -> SpannedNode<T> {
    SpannedNode { inner, span }
}
