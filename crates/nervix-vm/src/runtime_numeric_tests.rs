//! Differential tests for checked numeric execution.
//!
//! Layer: test harness.
//!
//! - **Owns.** Executing every numeric operator and builtin over generated batches of each numeric
//!   width, failure density, null density and slice offset, and comparing every row with a scalar
//!   model of the same operation.
//! - **Depends on.** The VM compiler and runtime entry points.
//! - **Must not know.** How an operation traverses its Arrow buffers.

use std::{fmt, sync::Arc as StdArc};

use arrow_array::{
    Array, ArrowPrimitiveType, PrimitiveArray,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, Field, Schema};
use nervix_approx_into::ApproxInto as _;

use super::execute_program_sync;
use crate::{
    CompileBinding, CompiledProgram, SideError, TypedArray, TypedBatch,
    compile_program_for_bindings, numeric::DecimalRounding, test_support::parse_program,
};

/// Rows in every generated batch. It is not a multiple of 64, so the last, partial word of every
/// bitmap is exercised, and it holds every pair of boundary operands of each width.
const LANES: usize = 203;

/// Rows ahead of a sliced batch. A kernel that ignores the slice offset reads these instead of
/// the batch and disagrees with the model.
const SLICE_OFFSETS: [usize; 2] = [0, 7];

#[derive(Debug, Clone, Copy)]
enum FailureDensity {
    /// Operands that no numeric operator fails on.
    None,
    /// One row in 17 pairs boundary operands.
    Sparse,
    /// Every row pairs boundary operands, covering every pair of them.
    Dense,
}

impl FailureDensity {
    const ALL: [Self; 3] = [Self::None, Self::Sparse, Self::Dense];

    /// The index of the boundary operand pair row `row` holds, or `None` for ordinary operands.
    fn boundary_pair(self, row: usize) -> Option<usize> {
        match self {
            Self::None => None,
            Self::Sparse => (row % 17 == 5).then_some(row / 17),
            Self::Dense => Some(row),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum NullDensity {
    None,
    Sparse,
    Half,
    All,
}

impl NullDensity {
    const ALL: [Self; 4] = [Self::None, Self::Sparse, Self::Half, Self::All];

    fn left_is_null(self, row: usize) -> bool {
        match self {
            Self::None => false,
            Self::Sparse => row % 13 == 4,
            Self::Half => row % 2 == 1,
            Self::All => true,
        }
    }

    fn right_is_null(self, row: usize) -> bool {
        match self {
            Self::None => false,
            Self::Sparse => row % 11 == 7,
            Self::Half => row.is_multiple_of(3),
            Self::All => true,
        }
    }
}

/// One value in an operation's output, compared exactly: floats by their bits, so a sign of zero
/// or a last-place difference is a disagreement.
#[derive(Clone, Copy)]
enum LaneValue {
    Integer(i128),
    Float(f64),
    Boolean(bool),
}

impl PartialEq for LaneValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => left == right,
            (Self::Float(left), Self::Float(right)) => left.to_bits() == right.to_bits(),
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            _ => false,
        }
    }
}

impl fmt::Debug for LaneValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => write!(formatter, "Integer({value})"),
            Self::Float(value) => {
                write!(formatter, "Float({value:e}, bits {:#x})", value.to_bits())
            }
            Self::Boolean(value) => write!(formatter, "Boolean({value})"),
        }
    }
}

/// What one row of an operation is expected to produce.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Value(LaneValue),
    Null,
    Failure { code: &'static str, message: String },
}

impl Outcome {
    fn failure(code: &'static str, message: &str) -> Self {
        Self::Failure {
            code,
            message: message.to_string(),
        }
    }

    fn finite(value: f64, message: &str) -> Self {
        if value.is_finite() {
            Self::Value(LaneValue::Float(value))
        } else {
            Self::failure("invalid_argument", message)
        }
    }

    fn observed(column: &TypedArray, errors: &[SideError], row: usize) -> Self {
        if let [error] = errors {
            return Self::Failure {
                code: error.code().as_str(),
                message: error.reason.to_string(),
            };
        }
        assert!(
            errors.is_empty(),
            "one operation reported {} errors for row {row}: {errors:?}",
            errors.len()
        );
        let value = match column {
            TypedArray::UInt8(array) => lane_integer(array, row),
            TypedArray::Int8(array) => lane_integer(array, row),
            TypedArray::UInt16(array) => lane_integer(array, row),
            TypedArray::Int16(array) => lane_integer(array, row),
            TypedArray::UInt32(array) => lane_integer(array, row),
            TypedArray::Int32(array) => lane_integer(array, row),
            TypedArray::UInt64(array) => lane_integer(array, row),
            TypedArray::Int64(array) => lane_integer(array, row),
            TypedArray::Float32(array) => {
                (!array.is_null(row)).then(|| LaneValue::Float(f64::from(array.value(row))))
            }
            TypedArray::Float64(array) => {
                (!array.is_null(row)).then(|| LaneValue::Float(array.value(row)))
            }
            TypedArray::Boolean(array) => {
                (!array.is_null(row)).then(|| LaneValue::Boolean(array.value(row)))
            }
            other => panic!("numeric operation produced {:?}", other.data_type()),
        };
        match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
        }
    }
}

fn lane_integer<T>(array: &PrimitiveArray<T>, row: usize) -> Option<LaneValue>
where
    T: ArrowPrimitiveType,
    i128: From<T::Native>,
{
    (!array.is_null(row)).then(|| LaneValue::Integer(i128::from(array.value(row))))
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Neg,
    Abs,
    Ceil,
    Floor,
    Round,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Acos,
    Asin,
    Atan,
    Cos,
    Exp,
    Ln,
    Log10,
    LogBase,
    Pow,
    Sqrt,
    Tan,
    Sin,
    Atan2,
    Log2,
    Radians,
    Degrees,
    Sign,
    Trunc,
    /// `round` to one decimal place. Every ordinary float operand is a multiple of 0.25, so a
    /// quarter rounds as an exact tie, and no integer changes.
    RoundToPlaces,
    /// `round` to a multiple of ten, which the largest values of an integer type overflow.
    RoundToTens,
    /// `round` by the right operand as the count of digits. Integers only, since a count is an
    /// integer.
    RoundToDigits,
    IsNan,
    IsFinite,
    IsInfinite,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseNot,
    BitCount,
    ShiftLeft,
    ShiftRight,
}

impl Operation {
    const ALL: [Self; 47] = [
        Self::Add,
        Self::Sub,
        Self::Mul,
        Self::Div,
        Self::Rem,
        Self::Neg,
        Self::Abs,
        Self::Ceil,
        Self::Floor,
        Self::Round,
        Self::Eq,
        Self::NotEq,
        Self::Lt,
        Self::LtEq,
        Self::Gt,
        Self::GtEq,
        Self::Acos,
        Self::Asin,
        Self::Atan,
        Self::Cos,
        Self::Exp,
        Self::Ln,
        Self::Log10,
        Self::LogBase,
        Self::Pow,
        Self::Sqrt,
        Self::Tan,
        Self::Sin,
        Self::Atan2,
        Self::Log2,
        Self::Radians,
        Self::Degrees,
        Self::Sign,
        Self::Trunc,
        Self::RoundToPlaces,
        Self::RoundToTens,
        Self::RoundToDigits,
        Self::IsNan,
        Self::IsFinite,
        Self::IsInfinite,
        Self::BitwiseAnd,
        Self::BitwiseOr,
        Self::BitwiseXor,
        Self::BitwiseNot,
        Self::BitCount,
        Self::ShiftLeft,
        Self::ShiftRight,
    ];

    /// Whether the operation accepts operands of `W`: negation takes a signed type, classification
    /// a float, and the bit operations and a column count of digits an integer.
    fn applies_to<W: Width>(self) -> bool
    where
        W::Native: PartialOrd,
    {
        match self {
            Self::Neg => W::SIGNED,
            Self::IsNan | Self::IsFinite | Self::IsInfinite => !W::INTEGRAL,
            Self::RoundToDigits
            | Self::BitwiseAnd
            | Self::BitwiseOr
            | Self::BitwiseXor
            | Self::BitwiseNot
            | Self::BitCount
            | Self::ShiftLeft
            | Self::ShiftRight => W::INTEGRAL,
            _ => true,
        }
    }

    fn expression(self) -> &'static str {
        match self {
            Self::Add => "input.left + input.right",
            Self::Sub => "input.left - input.right",
            Self::Mul => "input.left * input.right",
            Self::Div => "input.left / input.right",
            Self::Rem => "input.left % input.right",
            Self::Neg => "-input.left",
            Self::Abs => "abs(input.left)",
            Self::Ceil => "ceil(input.left)",
            Self::Floor => "floor(input.left)",
            Self::Round => "round(input.left)",
            Self::Eq => "input.left = input.right",
            Self::NotEq => "input.left != input.right",
            Self::Lt => "input.left < input.right",
            Self::LtEq => "input.left <= input.right",
            Self::Gt => "input.left > input.right",
            Self::GtEq => "input.left >= input.right",
            Self::Acos => "acos(input.left)",
            Self::Asin => "asin(input.left)",
            Self::Atan => "atan(input.left)",
            Self::Cos => "cos(input.left)",
            Self::Exp => "exp(input.left)",
            Self::Ln => "ln(input.left)",
            Self::Log10 => "log(input.left)",
            Self::LogBase => "log(input.left, input.right)",
            Self::Pow => "pow(input.left, input.right)",
            Self::Sqrt => "sqrt(input.left)",
            Self::Tan => "tan(input.left)",
            Self::Sin => "sin(input.left)",
            Self::Atan2 => "atan2(input.left, input.right)",
            Self::Log2 => "log2(input.left)",
            Self::Radians => "radians(input.left)",
            Self::Degrees => "degrees(input.left)",
            Self::Sign => "sign(input.left)",
            Self::Trunc => "trunc(input.left)",
            Self::RoundToPlaces => "round(input.left, 1)",
            Self::RoundToTens => "round(input.left, -1)",
            Self::RoundToDigits => "round(input.left, input.right)",
            Self::IsNan => "is_nan(input.left)",
            Self::IsFinite => "is_finite(input.left)",
            Self::IsInfinite => "is_infinite(input.left)",
            Self::BitwiseAnd => "bitwise_and(input.left, input.right)",
            Self::BitwiseOr => "bitwise_or(input.left, input.right)",
            Self::BitwiseXor => "bitwise_xor(input.left, input.right)",
            Self::BitwiseNot => "bitwise_not(input.left)",
            Self::BitCount => "bit_count(input.left)",
            Self::ShiftLeft => "shift_left(input.left, input.right)",
            Self::ShiftRight => "shift_right(input.left, input.right)",
        }
    }

    fn output_type(self, operand: &DataType) -> DataType {
        match self {
            Self::Add
            | Self::Sub
            | Self::Mul
            | Self::Div
            | Self::Rem
            | Self::Neg
            | Self::Abs
            | Self::Ceil
            | Self::Floor
            | Self::Round
            | Self::Sign
            | Self::Trunc
            | Self::RoundToPlaces
            | Self::RoundToTens
            | Self::RoundToDigits
            | Self::BitwiseAnd
            | Self::BitwiseOr
            | Self::BitwiseXor
            | Self::BitwiseNot
            | Self::ShiftLeft
            | Self::ShiftRight => operand.clone(),
            Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::IsNan
            | Self::IsFinite
            | Self::IsInfinite => DataType::Boolean,
            Self::BitCount => DataType::Int64,
            Self::Acos
            | Self::Asin
            | Self::Atan
            | Self::Cos
            | Self::Exp
            | Self::Ln
            | Self::Log10
            | Self::LogBase
            | Self::Pow
            | Self::Sqrt
            | Self::Tan
            | Self::Sin
            | Self::Atan2
            | Self::Log2
            | Self::Radians
            | Self::Degrees => DataType::Float64,
        }
    }

    fn reads_right(self) -> bool {
        match self {
            Self::Add
            | Self::Sub
            | Self::Mul
            | Self::Div
            | Self::Rem
            | Self::Eq
            | Self::NotEq
            | Self::Lt
            | Self::LtEq
            | Self::Gt
            | Self::GtEq
            | Self::LogBase
            | Self::Pow
            | Self::Atan2
            | Self::RoundToDigits
            | Self::BitwiseAnd
            | Self::BitwiseOr
            | Self::BitwiseXor
            | Self::ShiftLeft
            | Self::ShiftRight => true,
            Self::Neg
            | Self::Abs
            | Self::Ceil
            | Self::Floor
            | Self::Round
            | Self::Acos
            | Self::Asin
            | Self::Atan
            | Self::Cos
            | Self::Exp
            | Self::Ln
            | Self::Log10
            | Self::Sqrt
            | Self::Tan
            | Self::Sin
            | Self::Log2
            | Self::Radians
            | Self::Degrees
            | Self::Sign
            | Self::Trunc
            | Self::RoundToPlaces
            | Self::RoundToTens
            | Self::IsNan
            | Self::IsFinite
            | Self::IsInfinite
            | Self::BitwiseNot
            | Self::BitCount => false,
        }
    }

    fn compile(self, operand: &DataType) -> CompiledProgram {
        let input_schema = StdArc::new(Schema::new(vec![
            Field::new("left", operand.clone(), true),
            Field::new("right", operand.clone(), true),
        ]));
        let output_schema = StdArc::new(Schema::new(vec![
            Field::new("left", operand.clone(), true),
            Field::new("right", operand.clone(), true),
            Field::new("out", self.output_type(operand), true),
        ]));
        let source = format!("SET out = {}", self.expression());
        let parsed = parse_program(&source).expect("numeric operation must parse");
        compile_program_for_bindings(
            &parsed,
            output_schema,
            [CompileBinding::writable("input", input_schema)],
        )
        .unwrap_or_else(|error| panic!("`{source}` over {operand:?} must compile: {error:?}"))
    }

    /// The scalar model: the outcome of this operation for one row's operands.
    fn expected<W: Width>(self, left: Option<W::Native>, right: Option<W::Native>) -> Outcome {
        let Some(left) = left else {
            return Outcome::Null;
        };
        let right = match (self.reads_right(), right) {
            (true, Some(right)) => right,
            (true, None) => return Outcome::Null,
            (false, _) => left,
        };
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Rem => {
                W::arithmetic(self, left, right)
            }
            Self::Neg => W::negation(left),
            Self::Abs => W::absolute_value(left),
            Self::Ceil | Self::Floor | Self::Round => W::rounding(self, left),
            Self::Eq => Outcome::Value(LaneValue::Boolean(left == right)),
            Self::NotEq => Outcome::Value(LaneValue::Boolean(left != right)),
            Self::Lt => Outcome::Value(LaneValue::Boolean(left < right)),
            Self::LtEq => Outcome::Value(LaneValue::Boolean(left <= right)),
            Self::Gt => Outcome::Value(LaneValue::Boolean(left > right)),
            Self::GtEq => Outcome::Value(LaneValue::Boolean(left >= right)),
            Self::Acos => {
                Outcome::finite(W::to_f64(left).acos(), "acos produced a non-finite result")
            }
            Self::Asin => {
                Outcome::finite(W::to_f64(left).asin(), "asin produced a non-finite result")
            }
            Self::Atan => {
                Outcome::finite(W::to_f64(left).atan(), "atan produced a non-finite result")
            }
            Self::Cos => Outcome::finite(W::to_f64(left).cos(), "cos produced a non-finite result"),
            Self::Exp => Outcome::finite(W::to_f64(left).exp(), "exp produced a non-finite result"),
            Self::Ln => Outcome::finite(W::to_f64(left).ln(), "ln produced a non-finite result"),
            Self::Log10 => {
                Outcome::finite(W::to_f64(left).log10(), "log produced a non-finite result")
            }
            Self::LogBase => Outcome::finite(
                W::to_f64(right).log(W::to_f64(left)),
                "log produced a non-finite result",
            ),
            Self::Pow => Outcome::finite(
                W::to_f64(left).powf(W::to_f64(right)),
                "pow produced a non-finite result",
            ),
            Self::Sqrt => {
                Outcome::finite(W::to_f64(left).sqrt(), "sqrt produced a non-finite result")
            }
            Self::Tan => Outcome::finite(W::to_f64(left).tan(), "tan produced a non-finite result"),
            Self::Sin => Outcome::finite(W::to_f64(left).sin(), "sin produced a non-finite result"),
            Self::Atan2 => Outcome::finite(
                W::to_f64(left).atan2(W::to_f64(right)),
                "atan2 produced a non-finite result",
            ),
            Self::Log2 => {
                Outcome::finite(W::to_f64(left).log2(), "log2 produced a non-finite result")
            }
            Self::Radians => Outcome::finite(
                W::to_f64(left).to_radians(),
                "radians produced a non-finite result",
            ),
            Self::Degrees => Outcome::finite(
                W::to_f64(left).to_degrees(),
                "degrees produced a non-finite result",
            ),
            Self::Sign => W::sign(left),
            Self::Trunc => W::rounding(self, left),
            Self::RoundToPlaces => W::round_to_places(left, 1),
            Self::RoundToTens => W::round_to_places(left, -1),
            Self::RoundToDigits => W::round_to_digits(left, right),
            Self::IsNan | Self::IsFinite | Self::IsInfinite => W::classify(self, left),
            Self::BitwiseAnd | Self::BitwiseOr | Self::BitwiseXor | Self::BitwiseNot => {
                W::bitwise(self, left, right)
            }
            Self::BitCount => W::bit_count(left),
            Self::ShiftLeft | Self::ShiftRight => W::shift(self, left, right),
        }
    }
}

/// One numeric register width, with the operands the batches draw from and the scalar model of
/// the operations whose outcome depends on how the width represents a value.
trait Width: ArrowPrimitiveType + Sized
where
    Self::Native: PartialOrd,
{
    const SIGNED: bool;

    const INTEGRAL: bool;

    /// Operands no numeric operator fails on. Left operands are larger in magnitude than right
    /// ones, so unsigned subtraction does not underflow, and every product fits `I8`.
    fn ordinary(row: usize, left: bool) -> Self::Native;

    /// The boundary operands of the width, paired with each other by dense batches.
    fn boundaries() -> Vec<Self::Native>;

    fn typed(array: PrimitiveArray<Self>) -> TypedArray;

    fn to_f64(value: Self::Native) -> f64;

    fn arithmetic(operation: Operation, left: Self::Native, right: Self::Native) -> Outcome;

    fn negation(value: Self::Native) -> Outcome;

    fn absolute_value(value: Self::Native) -> Outcome;

    fn rounding(operation: Operation, value: Self::Native) -> Outcome;

    fn sign(value: Self::Native) -> Outcome;

    /// `round(value, digits)` for a literal count of digits.
    fn round_to_places(value: Self::Native, digits: i16) -> Outcome;

    /// `round(value, digits)`, where the count of digits has the value's own type.
    fn round_to_digits(value: Self::Native, digits: Self::Native) -> Outcome;

    fn classify(operation: Operation, value: Self::Native) -> Outcome;

    fn bitwise(operation: Operation, left: Self::Native, right: Self::Native) -> Outcome;

    fn bit_count(value: Self::Native) -> Outcome;

    /// A shift of `value` by `count`, where the count has the value's own type.
    fn shift(operation: Operation, value: Self::Native, count: Self::Native) -> Outcome;
}

/// The multiple of `10^-digits` nearest to `value`, with halves away from zero, as an exact
/// integer. A count of zero or more leaves an integer unchanged.
fn integer_rounded_to_digits(value: i128, digits: i128) -> i128 {
    if digits >= 0 {
        return value;
    }
    let exponent = digits.unsigned_abs();
    // Every tested integer is below 2^64 in magnitude, below half of 10^20.
    if exponent > 30 {
        return 0;
    }
    let power = 10_u128.pow(u32::try_from(exponent).expect("at most 30"));
    let magnitude = value.unsigned_abs();
    let remainder = magnitude % power;
    let below = magnitude - remainder;
    let rounded = if 2 * remainder >= power {
        below + power
    } else {
        below
    };
    let rounded = i128::try_from(rounded).expect("below 10^31");
    if value < 0 { -rounded } else { rounded }
}

/// `value × 2^count` for a left shift, or `value ÷ 2^count` rounded toward negative infinity for a
/// right shift, as an exact integer, or `None` for a product no 64-bit type holds.
fn integer_shifted(left_shift: bool, value: i128, count: i128) -> Option<i128> {
    assert!(count >= 0, "a negative count fails before shifting");
    // Every count above u32::MAX shifts exactly as u32::MAX does.
    let exponent = u32::try_from(count).unwrap_or(u32::MAX);
    if left_shift {
        if value == 0 {
            return Some(0);
        }
        if exponent > 64 {
            return None;
        }
        return value.checked_mul(1_i128 << exponent);
    }
    if exponent > 126 {
        return Some(if value < 0 { -1 } else { 0 });
    }
    Some(value >> exponent)
}

fn integer_outcome<N>(value: Option<N>, message: &str) -> Outcome
where
    i128: From<N>,
{
    match value {
        Some(value) => Outcome::Value(LaneValue::Integer(i128::from(value))),
        None => Outcome::failure("overflow", message),
    }
}

macro_rules! integer_width {
    (
        $arrow:ty,
        $native:ty,
        $variant:ident,
        signed: $signed:literal,
        to_f64: $to_f64:expr,
        boundaries: [$($boundary:expr),+ $(,)?]
    ) => {
        impl Width for $arrow {
            const SIGNED: bool = $signed;

            const INTEGRAL: bool = true;

            fn ordinary(row: usize, left: bool) -> $native {
                let magnitude = if left { 10 + row % 4 } else { 1 + row % 9 };
                let magnitude = <$native>::try_from(magnitude)
                    .expect("ordinary operands stay below 14");
                let negative = if left { row % 2 == 1 } else { (row / 2) % 2 == 1 };
                if $signed && negative {
                    <$native>::default().wrapping_sub(magnitude)
                } else {
                    magnitude
                }
            }

            fn boundaries() -> Vec<$native> {
                vec![$($boundary),+]
            }

            fn typed(array: PrimitiveArray<Self>) -> TypedArray {
                TypedArray::$variant(array)
            }

            fn to_f64(value: $native) -> f64 {
                ($to_f64)(value)
            }

            fn arithmetic(operation: Operation, left: $native, right: $native) -> Outcome {
                match operation {
                    Operation::Add => {
                        integer_outcome(left.checked_add(right), "integer addition overflowed")
                    }
                    Operation::Sub => {
                        integer_outcome(left.checked_sub(right), "integer subtraction overflowed")
                    }
                    Operation::Mul => integer_outcome(
                        left.checked_mul(right),
                        "integer multiplication overflowed",
                    ),
                    Operation::Div if right == 0 => {
                        Outcome::failure("division_by_zero", "integer division by zero")
                    }
                    Operation::Div => {
                        integer_outcome(left.checked_div(right), "integer division overflowed")
                    }
                    Operation::Rem if right == 0 => {
                        Outcome::failure("division_by_zero", "integer remainder by zero")
                    }
                    // A remainder by a nonzero divisor always exists. Only the quotient of the
                    // minimum value over -1 overflows, and its remainder is exactly 0.
                    Operation::Rem => Outcome::Value(LaneValue::Integer(i128::from(
                        left.checked_rem(right).unwrap_or(0),
                    ))),
                    other => panic!("{other:?} is not arithmetic"),
                }
            }

            fn negation(value: $native) -> Outcome {
                integer_outcome(
                    <$native>::default().checked_sub(value),
                    "integer negation overflowed",
                )
            }

            fn absolute_value(value: $native) -> Outcome {
                if value >= <$native>::default() {
                    return Outcome::Value(LaneValue::Integer(i128::from(value)));
                }
                integer_outcome(
                    <$native>::default().checked_sub(value),
                    "integer absolute value overflowed",
                )
            }

            fn rounding(_operation: Operation, value: $native) -> Outcome {
                Outcome::Value(LaneValue::Integer(i128::from(value)))
            }

            fn sign(value: $native) -> Outcome {
                let sign = match i128::from(value) {
                    positive if positive > 0 => 1,
                    0 => 0,
                    _ => -1,
                };
                Outcome::Value(LaneValue::Integer(sign))
            }

            fn round_to_places(value: $native, digits: i16) -> Outcome {
                let rounded = integer_rounded_to_digits(i128::from(value), i128::from(digits));
                match <$native>::try_from(rounded) {
                    Ok(rounded) => Outcome::Value(LaneValue::Integer(i128::from(rounded))),
                    Err(_) => Outcome::failure("overflow", "integer rounding overflowed"),
                }
            }

            fn round_to_digits(value: $native, digits: $native) -> Outcome {
                let rounded = integer_rounded_to_digits(i128::from(value), i128::from(digits));
                match <$native>::try_from(rounded) {
                    Ok(rounded) => Outcome::Value(LaneValue::Integer(i128::from(rounded))),
                    Err(_) => Outcome::failure("overflow", "integer rounding overflowed"),
                }
            }

            fn classify(operation: Operation, _value: $native) -> Outcome {
                panic!("{operation:?} does not apply to integers")
            }

            fn bitwise(operation: Operation, left: $native, right: $native) -> Outcome {
                let value = match operation {
                    Operation::BitwiseAnd => left & right,
                    Operation::BitwiseOr => left | right,
                    Operation::BitwiseXor => left ^ right,
                    Operation::BitwiseNot => !left,
                    other => panic!("{other:?} is not a bitwise operation"),
                };
                Outcome::Value(LaneValue::Integer(i128::from(value)))
            }

            fn bit_count(value: $native) -> Outcome {
                Outcome::Value(LaneValue::Integer(i128::from(value.count_ones())))
            }

            fn shift(operation: Operation, value: $native, count: $native) -> Outcome {
                let left_shift = match operation {
                    Operation::ShiftLeft => true,
                    Operation::ShiftRight => false,
                    other => panic!("{other:?} is not a shift"),
                };
                let count = i128::from(count);
                if count < 0 {
                    let message = if left_shift {
                        "integer left shift by a negative count"
                    } else {
                        "integer right shift by a negative count"
                    };
                    return Outcome::failure("invalid_argument", message);
                }
                let shifted = integer_shifted(left_shift, i128::from(value), count)
                    .and_then(|shifted| <$native>::try_from(shifted).ok());
                match shifted {
                    Some(shifted) => Outcome::Value(LaneValue::Integer(i128::from(shifted))),
                    None => Outcome::failure("overflow", "integer left shift overflowed"),
                }
            }
        }
    };
}

integer_width!(
    UInt8Type,
    u8,
    UInt8,
    signed: false,
    to_f64: f64::from,
    boundaries: [0, 1, 2, u8::MAX - 1, u8::MAX]
);
integer_width!(
    Int8Type,
    i8,
    Int8,
    signed: true,
    to_f64: f64::from,
    boundaries: [i8::MIN, i8::MIN + 1, -1, 0, 1, i8::MAX - 1, i8::MAX]
);
integer_width!(
    UInt16Type,
    u16,
    UInt16,
    signed: false,
    to_f64: f64::from,
    boundaries: [0, 1, 2, u16::MAX - 1, u16::MAX]
);
integer_width!(
    Int16Type,
    i16,
    Int16,
    signed: true,
    to_f64: f64::from,
    boundaries: [i16::MIN, i16::MIN + 1, -1, 0, 1, i16::MAX - 1, i16::MAX]
);
integer_width!(
    UInt32Type,
    u32,
    UInt32,
    signed: false,
    to_f64: f64::from,
    boundaries: [0, 1, 2, u32::MAX - 1, u32::MAX]
);
integer_width!(
    Int32Type,
    i32,
    Int32,
    signed: true,
    to_f64: f64::from,
    boundaries: [i32::MIN, i32::MIN + 1, -1, 0, 1, i32::MAX - 1, i32::MAX]
);
integer_width!(
    UInt64Type,
    u64,
    UInt64,
    signed: false,
    to_f64: |value: u64| -> f64 { value.approx_into() },
    boundaries: [0, 1, 2, u64::MAX - 1, u64::MAX]
);
integer_width!(
    Int64Type,
    i64,
    Int64,
    signed: true,
    to_f64: |value: i64| -> f64 { value.approx_into() },
    boundaries: [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX]
);

macro_rules! float_width {
    ($arrow:ty, $native:ident, $variant:ident, $to_f64:expr) => {
        impl Width for $arrow {
            const SIGNED: bool = true;

            const INTEGRAL: bool = false;

            fn ordinary(row: usize, left: bool) -> $native {
                let magnitude = if left {
                    10.0 + <$native>::from(u8::try_from(row % 4).expect("below 4")) * 0.75
                } else {
                    1.0 + <$native>::from(u8::try_from(row % 9).expect("below 9")) * 0.5
                };
                let negative = if left {
                    row % 2 == 1
                } else {
                    (row / 2) % 2 == 1
                };
                if negative { -magnitude } else { magnitude }
            }

            fn boundaries() -> Vec<$native> {
                vec![
                    $native::NAN,
                    $native::INFINITY,
                    $native::NEG_INFINITY,
                    0.0,
                    -0.0,
                    $native::MAX,
                    $native::MIN,
                    $native::MIN_POSITIVE,
                    $native::from_bits(1),
                    1.0,
                    -1.0,
                    0.5,
                ]
            }

            fn typed(array: PrimitiveArray<Self>) -> TypedArray {
                TypedArray::$variant(array)
            }

            fn to_f64(value: $native) -> f64 {
                ($to_f64)(value)
            }

            fn arithmetic(operation: Operation, left: $native, right: $native) -> Outcome {
                let value = match operation {
                    Operation::Add => left + right,
                    Operation::Sub => left - right,
                    Operation::Mul => left * right,
                    Operation::Div => left / right,
                    Operation::Rem => left % right,
                    other => panic!("{other:?} is not arithmetic"),
                };
                if value.is_finite() {
                    Outcome::Value(LaneValue::Float(f64::from(value)))
                } else {
                    Outcome::failure(
                        "invalid_argument",
                        "floating-point operation produced a non-finite result",
                    )
                }
            }

            fn negation(value: $native) -> Outcome {
                Outcome::Value(LaneValue::Float(f64::from(-value)))
            }

            fn absolute_value(value: $native) -> Outcome {
                let magnitude = value.abs();
                if magnitude.is_finite() {
                    Outcome::Value(LaneValue::Float(f64::from(magnitude)))
                } else {
                    Outcome::failure(
                        "invalid_argument",
                        "floating-point absolute value produced a non-finite result",
                    )
                }
            }

            fn rounding(operation: Operation, value: $native) -> Outcome {
                let (rounded, message) = match operation {
                    Operation::Ceil => (value.ceil(), "ceil produced a non-finite result"),
                    Operation::Floor => (value.floor(), "floor produced a non-finite result"),
                    Operation::Round => (value.round(), "round produced a non-finite result"),
                    Operation::Trunc => (value.trunc(), "trunc produced a non-finite result"),
                    other => panic!("{other:?} is not rounding"),
                };
                if rounded.is_finite() {
                    Outcome::Value(LaneValue::Float(f64::from(rounded)))
                } else {
                    Outcome::failure("invalid_argument", message)
                }
            }

            fn sign(value: $native) -> Outcome {
                if value.is_nan() {
                    return Outcome::failure(
                        "invalid_argument",
                        "sign produced a non-finite result",
                    );
                }
                let sign = if value > 0.0 {
                    1.0
                } else if value < 0.0 {
                    -1.0
                } else {
                    value
                };
                Outcome::Value(LaneValue::Float(f64::from(sign)))
            }

            /// The model rounds through the value's exact decimal expansion, which the reference
            /// cases of the decimal rounding kernel check against exact decimal arithmetic.
            fn round_to_places(value: $native, digits: i16) -> Outcome {
                if !value.is_finite() {
                    return Outcome::failure(
                        "invalid_argument",
                        "round produced a non-finite result",
                    );
                }
                if value == 0.0 {
                    return Outcome::Value(LaneValue::Float(f64::from(value)));
                }
                let rounded = value.rounded_by_decimal_expansion(value.binary_parts(), digits);
                Outcome::finite(f64::from(rounded), "round produced a non-finite result")
            }

            fn round_to_digits(_value: $native, _digits: $native) -> Outcome {
                panic!("a float is not a count of digits")
            }

            fn classify(operation: Operation, value: $native) -> Outcome {
                let class = match operation {
                    Operation::IsNan => value.is_nan(),
                    Operation::IsFinite => value.is_finite(),
                    Operation::IsInfinite => value.is_infinite(),
                    other => panic!("{other:?} is not a classification"),
                };
                Outcome::Value(LaneValue::Boolean(class))
            }

            fn bitwise(operation: Operation, _left: $native, _right: $native) -> Outcome {
                panic!("{operation:?} does not apply to floats")
            }

            fn bit_count(_value: $native) -> Outcome {
                panic!("bit_count does not apply to floats")
            }

            fn shift(operation: Operation, _value: $native, _count: $native) -> Outcome {
                panic!("{operation:?} does not apply to floats")
            }
        }
    };
}

float_width!(Float32Type, f32, Float32, f64::from);
float_width!(Float64Type, f64, Float64, |value: f64| value);

/// The two operand columns of one batch, before slicing, with the options each row holds.
struct Operands<W: Width>
where
    W::Native: PartialOrd,
{
    left: Vec<Option<W::Native>>,
    right: Vec<Option<W::Native>>,
}

impl<W: Width> Operands<W>
where
    W::Native: PartialOrd,
{
    fn generate(failures: FailureDensity, nulls: NullDensity, offset: usize) -> Self {
        let boundaries = W::boundaries();
        let rows = offset + LANES + 3;
        let mut left = Vec::with_capacity(rows);
        let mut right = Vec::with_capacity(rows);
        for row in 0..rows {
            let lane = row.wrapping_sub(offset);
            let pair = if row < offset || row >= offset + LANES {
                Some(row)
            } else {
                failures.boundary_pair(lane)
            };
            let (left_value, right_value) = match pair {
                Some(pair) => (
                    boundaries[pair % boundaries.len()],
                    boundaries[(pair / boundaries.len()) % boundaries.len()],
                ),
                None => (W::ordinary(lane, true), W::ordinary(lane, false)),
            };
            left.push((!nulls.left_is_null(lane)).then_some(left_value));
            right.push((!nulls.right_is_null(lane)).then_some(right_value));
        }
        Self { left, right }
    }

    fn column(values: &[Option<W::Native>], offset: usize) -> PrimitiveArray<W> {
        PrimitiveArray::<W>::from_iter(values.iter().copied()).slice(offset, LANES)
    }
}

fn check_width<W: Width>()
where
    W::Native: PartialOrd,
{
    for operation in Operation::ALL {
        if !operation.applies_to::<W>() {
            continue;
        }
        let compiled = operation.compile(&W::DATA_TYPE);
        for failures in FailureDensity::ALL {
            for nulls in NullDensity::ALL {
                for offset in SLICE_OFFSETS {
                    let operands = Operands::<W>::generate(failures, nulls, offset);
                    let batch = TypedBatch::try_new(
                        compiled.input_schema.clone(),
                        vec![
                            W::typed(Operands::<W>::column(&operands.left, offset)),
                            W::typed(Operands::<W>::column(&operands.right, offset)),
                        ],
                    )
                    .expect("operand batch must build");

                    let output =
                        execute_program_sync(&compiled, &batch).expect("execution must succeed");

                    let out = output.column(2);
                    for lane in 0..LANES {
                        let expected = operation.expected::<W>(
                            operands.left[offset + lane],
                            operands.right[offset + lane],
                        );
                        let observed = Outcome::observed(out, output.errors().row(lane), lane);
                        assert_eq!(
                            observed,
                            expected,
                            "{operation:?} over {:?} with {failures:?} failures, {nulls:?} nulls \
                             and offset {offset} disagrees at row {lane} for operands {:?} and \
                             {:?}",
                            W::DATA_TYPE,
                            operands.left[offset + lane],
                            operands.right[offset + lane],
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn u8_numeric_operations_match_the_scalar_model() {
    check_width::<UInt8Type>();
}

#[test]
fn i8_numeric_operations_match_the_scalar_model() {
    check_width::<Int8Type>();
}

#[test]
fn u16_numeric_operations_match_the_scalar_model() {
    check_width::<UInt16Type>();
}

#[test]
fn i16_numeric_operations_match_the_scalar_model() {
    check_width::<Int16Type>();
}

#[test]
fn u32_numeric_operations_match_the_scalar_model() {
    check_width::<UInt32Type>();
}

#[test]
fn i32_numeric_operations_match_the_scalar_model() {
    check_width::<Int32Type>();
}

#[test]
fn u64_numeric_operations_match_the_scalar_model() {
    check_width::<UInt64Type>();
}

#[test]
fn i64_numeric_operations_match_the_scalar_model() {
    check_width::<Int64Type>();
}

#[test]
fn f32_numeric_operations_match_the_scalar_model() {
    check_width::<Float32Type>();
}

#[test]
fn f64_numeric_operations_match_the_scalar_model() {
    check_width::<Float64Type>();
}

/// A call over literals that folds at compile time and the same call over columns holding those
/// values produce the same result, since both apply one value contract.
#[test]
fn folded_and_executed_classification_and_bit_calls_agree() {
    let source = "SET folded_and = bitwise_and(12, 10), executed_and = bitwise_and(input.twelve, \
                  input.ten), folded_xor = bitwise_xor(12, 10), executed_xor = \
                  bitwise_xor(input.twelve, input.ten), folded_not = bitwise_not(12), \
                  executed_not = bitwise_not(input.twelve), folded_count = bit_count(4095), \
                  executed_count = bit_count(input.mask), folded_finite = is_finite(1.5), \
                  executed_finite = is_finite(input.ratio), folded_nan = is_nan(1.5), \
                  executed_nan = is_nan(input.ratio)";
    let input_schema = StdArc::new(Schema::new(vec![
        Field::new("twelve", DataType::Int64, true),
        Field::new("ten", DataType::Int64, true),
        Field::new("mask", DataType::Int64, true),
        Field::new("ratio", DataType::Float64, true),
    ]));
    let mut output_fields = input_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for name in ["and", "xor", "not", "count"] {
        output_fields.push(Field::new(format!("folded_{name}"), DataType::Int64, true));
        output_fields.push(Field::new(
            format!("executed_{name}"),
            DataType::Int64,
            true,
        ));
    }
    for name in ["finite", "nan"] {
        output_fields.push(Field::new(
            format!("folded_{name}"),
            DataType::Boolean,
            true,
        ));
        output_fields.push(Field::new(
            format!("executed_{name}"),
            DataType::Boolean,
            true,
        ));
    }
    let parsed = parse_program(source).expect("program must parse");
    let compiled = compile_program_for_bindings(
        &parsed,
        StdArc::new(Schema::new(output_fields)),
        [CompileBinding::writable("input", input_schema.clone())],
    )
    .expect("program must compile");
    // Only the six calls over columns remain builtins: every call over literals folded.
    let builtins = compiled
        .instructions
        .iter()
        .filter(|instruction| matches!(instruction.kind, crate::InstructionKind::Builtin { .. }))
        .count();
    assert_eq!(builtins, 6);
    let batch = TypedBatch::try_new(
        input_schema,
        vec![
            TypedArray::Int64(arrow_array::Int64Array::from(vec![12])),
            TypedArray::Int64(arrow_array::Int64Array::from(vec![10])),
            TypedArray::Int64(arrow_array::Int64Array::from(vec![4095])),
            TypedArray::Float64(arrow_array::Float64Array::from(vec![1.5])),
        ],
    )
    .expect("batch must build");

    let output = execute_program_sync(&compiled, &batch).expect("execution must succeed");

    let schema = output.schema();
    for name in ["and", "xor", "not", "count", "finite", "nan"] {
        let folded = schema
            .index_of(&format!("folded_{name}"))
            .expect("the folded field exists");
        let executed = schema
            .index_of(&format!("executed_{name}"))
            .expect("the executed field exists");
        let folded = Outcome::observed(output.column(folded), output.errors().row(0), 0);
        let executed = Outcome::observed(output.column(executed), output.errors().row(0), 0);
        assert_eq!(folded, executed, "{name}");
    }
}
