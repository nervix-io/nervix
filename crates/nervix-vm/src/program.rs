//! Layer: engines and infrastructure.
//!
//! - **Owns.** The typed program vocabulary consumed by the expression compiler and runtime.
//! - **Depends on.** Primitive Rust and Arrow value types.
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
                },
                Self::Cast {
                    expr: right_expr,
                    data_type: right_type,
                },
            ) => left_type
                .cmp(right_type)
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
    Concat,
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
/// A call names its unit, date part or bin width with a literal. The frontend reads that literal
/// once, when it lowers the call, so the call keeps only the arguments that vary by row and
/// execution never interprets text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DatetimeFunction {
    /// `date_part(part, value)`.
    DatePart(DatePart),
    /// `date_trunc(unit, value)`.
    DateTrunc(FixedTimeUnit),
    /// `date_bin(unit, width, value, origin)`.
    DateBin(DateBinWidth),
    /// `date_add(unit, amount, value)`.
    DateAdd(FixedTimeUnit),
    /// `date_diff(unit, start, end)`.
    DateDiff(FixedTimeUnit),
    /// `to_unix(unit, value)`.
    ToUnix(FixedTimeUnit),
    /// `from_unix(unit, count)`.
    FromUnix(FixedTimeUnit),
}

impl DatetimeFunction {
    pub const fn name(self) -> DatetimeFunctionName {
        match self {
            Self::DatePart(_) => DatetimeFunctionName::DatePart,
            Self::DateTrunc(_) => DatetimeFunctionName::DateTrunc,
            Self::DateBin(_) => DatetimeFunctionName::DateBin,
            Self::DateAdd(_) => DatetimeFunctionName::DateAdd,
            Self::DateDiff(_) => DatetimeFunctionName::DateDiff,
            Self::ToUnix(_) => DatetimeFunctionName::ToUnix,
            Self::FromUnix(_) => DatetimeFunctionName::FromUnix,
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
/// A DATETIME is a UTC instant without leap seconds, so every day is exactly 86,400 seconds and
/// every week exactly seven days. A calendar month or year has no fixed length and is not a unit.
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

/// A part of a DATETIME that `date_part` reads, in UTC.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, AsRefStr, EnumString)]
#[strum(ascii_case_insensitive, serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum WindowAggregateFunction {
    Count,
    First,
    Last,
    Max,
    Min,
    PercentileLinearHistogram,
    Sum,
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
            Self::Count | Self::First | Self::Last | Self::Max | Self::Min | Self::Sum => 1,
        }
    }
}

impl FunctionName {
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
            "concat" => Self::Concat,
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
            Self::Concat => "concat",
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
}

pub(crate) fn spanned<T>(inner: T, span: Span) -> SpannedNode<T> {
    SpannedNode { inner, span }
}
