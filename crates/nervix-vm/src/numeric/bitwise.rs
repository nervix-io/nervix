//! Integer bit operations over Arrow buffers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The batch kernels behind `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not`,
//!   `bit_count`, `shift_left` and `shift_right`, and which lanes of a shift fail and why.
//! - **Depends on.** Arrow arrays and buffers, the bit value contracts of the semantic catalog, the
//!   checked lane loop of the numeric kernels, and the side-error reasons of the VM.
//! - **Must not know.** Registers, programs, spans, or how a failed lane is recorded as a row
//!   error.
//!
//! `bitwise_and`, `bitwise_or`, `bitwise_xor`, `bitwise_not` and `bit_count` cannot fail, so each is
//! one Arrow arity kernel over the operands' value buffers, applying the value contract that
//! constant folding applies too. A shift fails for a negative count, and a left shift also fails
//! when the shifted value does not fit its type, so shifts use the checked lane loop instead.

use arrow_arith::arity::binary;
use arrow_array::{Array, ArrowPrimitiveType, Int64Array, PrimitiveArray, types::Int64Type};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use meticulous::ResultExt as _;

use super::{Checked, Lanes};
use crate::{
    batch::TypedArray,
    error::{IntegerOperation, ShiftOperation, SideErrorReason},
    semantics::{BitwiseOperation, IntegerBits},
};

impl BitwiseOperation {
    /// Combines two integer columns of one batch lane by lane. A lane is null when either operand
    /// is null.
    pub(crate) fn evaluate<T>(
        self,
        left: &PrimitiveArray<T>,
        right: &PrimitiveArray<T>,
    ) -> PrimitiveArray<T>
    where
        T: ArrowPrimitiveType,
        T::Native: IntegerBits,
    {
        let combined = match self {
            Self::And => binary(left, right, |left, right| Self::And.apply(left, right)),
            Self::Or => binary(left, right, |left, right| Self::Or.apply(left, right)),
            Self::Xor => binary(left, right, |left, right| Self::Xor.apply(left, right)),
        };
        combined.verified(
            "both operands are registers of one batch, which all hold one lane per row, so the \
             kernel's length check passes",
        )
    }
}

/// Inverts every bit of every lane at the column's own width.
pub(crate) fn bitwise_complement<T>(input: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: IntegerBits,
{
    input.unary(IntegerBits::complement)
}

/// Counts the set bits of every lane at the column's own width.
pub(crate) fn bit_count<T>(input: &PrimitiveArray<T>) -> Int64Array
where
    T: ArrowPrimitiveType,
    T::Native: IntegerBits,
{
    input.unary::<_, Int64Type>(IntegerBits::one_bits)
}

/// The widest supported integer type has 64 bits, so a shift by 64 moves every bit of every
/// supported value out, and every larger count shifts exactly as 64 does.
const WIDEST_SHIFT: u8 = 64;

/// One lane's shift count.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ShiftCount {
    /// The count, with every count above [`WIDEST_SHIFT`] held as [`WIDEST_SHIFT`]. A negative
    /// count holds zero.
    amount: u8,
    /// Whether the count was negative, which fails the lane.
    negative: bool,
}

/// An integer type a shift count is read from.
pub(crate) trait ShiftCountOperand: ArrowNativeType {
    fn shift_count(self) -> ShiftCount;
}

macro_rules! signed_shift_count {
    ($($native:ty),+ $(,)?) => {
        $(
            impl ShiftCountOperand for $native {
                fn shift_count(self) -> ShiftCount {
                    if self < 0 {
                        return ShiftCount {
                            amount: 0,
                            negative: true,
                        };
                    }
                    let amount = match u8::try_from(self) {
                        Ok(amount) => amount.min(WIDEST_SHIFT),
                        Err(_) => WIDEST_SHIFT,
                    };
                    ShiftCount {
                        amount,
                        negative: false,
                    }
                }
            }
        )+
    };
}

signed_shift_count!(i8, i16, i32, i64);

macro_rules! unsigned_shift_count {
    ($($native:ty),+ $(,)?) => {
        $(
            impl ShiftCountOperand for $native {
                fn shift_count(self) -> ShiftCount {
                    let amount = match u8::try_from(self) {
                        Ok(amount) => amount.min(WIDEST_SHIFT),
                        Err(_) => WIDEST_SHIFT,
                    };
                    ShiftCount {
                        amount,
                        negative: false,
                    }
                }
            }
        )+
    };
}

unsigned_shift_count!(u16, u32, u64);

impl ShiftCountOperand for u8 {
    fn shift_count(self) -> ShiftCount {
        ShiftCount {
            amount: self.min(WIDEST_SHIFT),
            negative: false,
        }
    }
}

/// The count operand of `shift_left` or `shift_right`: one count per lane, read from a column of
/// any integer type, with that column's validity.
pub(crate) struct ShiftCounts {
    lanes: Vec<ShiftCount>,
    nulls: Option<NullBuffer>,
}

impl ShiftCounts {
    /// Reads the counts of an integer column, or answers `None` for a column that is not integral.
    pub(crate) fn from_typed(input: &TypedArray) -> Option<Self> {
        match input {
            TypedArray::UInt8(array) => Some(Self::new(array)),
            TypedArray::Int8(array) => Some(Self::new(array)),
            TypedArray::UInt16(array) => Some(Self::new(array)),
            TypedArray::Int16(array) => Some(Self::new(array)),
            TypedArray::UInt32(array) => Some(Self::new(array)),
            TypedArray::Int32(array) => Some(Self::new(array)),
            TypedArray::UInt64(array) => Some(Self::new(array)),
            TypedArray::Int64(array) => Some(Self::new(array)),
            TypedArray::Float32(_)
            | TypedArray::Float64(_)
            | TypedArray::Boolean(_)
            | TypedArray::Utf8(_)
            | TypedArray::Binary(_)
            | TypedArray::Datetime(_)
            | TypedArray::Generic(_)
            | TypedArray::Uninitialized { .. } => None,
        }
    }

    fn new<T>(input: &PrimitiveArray<T>) -> Self
    where
        T: ArrowPrimitiveType,
        T::Native: ShiftCountOperand,
    {
        Self {
            lanes: input
                .values()
                .iter()
                .map(|count| count.shift_count())
                .collect(),
            nulls: input.nulls().cloned(),
        }
    }
}

/// A shift of an integer's two's complement bits by a count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shift {
    /// `shift_left(value, count)`: the value multiplied by 2 to the power of the count.
    Left,
    /// `shift_right(value, count)`: the value divided by 2 to the power of the count, rounded
    /// toward negative infinity, so a signed value keeps its sign.
    Right,
}

impl Shift {
    /// Shifts every lane of an integer column by the count in the same lane. A lane is null when
    /// either operand is null.
    pub(crate) fn evaluate<T>(self, values: &PrimitiveArray<T>, counts: &ShiftCounts) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: ShiftedInteger,
    {
        let value_lanes: &[T::Native] = values.values();
        let lanes = match self {
            Self::Left => Lanes::binary(value_lanes, &counts.lanes, T::Native::lane_left_shift),
            Self::Right => Lanes::binary(value_lanes, &counts.lanes, T::Native::lane_right_shift),
        };
        Checked::from_lanes(
            lanes,
            NullBuffer::union(values.nulls(), counts.nulls.as_ref()),
        )
    }

    /// Why a failed lane of this shift failed, given the counts the shift read.
    pub(crate) fn failure(self, counts: &ShiftCounts, lane: usize) -> SideErrorReason {
        match self {
            Self::Left => {
                if counts.lanes[lane].negative {
                    SideErrorReason::NegativeShiftCount(ShiftOperation::LeftShift)
                } else {
                    SideErrorReason::IntegerOverflow(IntegerOperation::LeftShift)
                }
            }
            // Dividing by a power of two always fits the type, so only a negative count fails a
            // right shift.
            Self::Right => SideErrorReason::NegativeShiftCount(ShiftOperation::RightShift),
        }
    }
}

/// An integer type the shift kernels compute over.
///
/// Both lane operations are branch-free selections over one shift of at most the type's width
/// minus one, so no lane shifts by the width or more, which Rust's shift operators reject.
pub(crate) trait ShiftedInteger: ArrowNativeType + Default {
    /// Fails for a negative count, and for a shifted value that does not fit the type: a bit
    /// shifted out, or a signed value whose sign would change.
    fn lane_left_shift(self, count: ShiftCount) -> (Self, bool);

    /// Fails for a negative count only.
    fn lane_right_shift(self, count: ShiftCount) -> (Self, bool);
}

/// What a right shift by a count of at least the type's width yields.
macro_rules! right_shift_fill {
    // A signed shift fills with the sign bit, so a shift by the width minus one already yields 0 or
    // -1, which is what every larger count yields too.
    (sign_bit, $shifted:ident, $amount:ident, $width:ident) => {
        $shifted
    };
    // An unsigned shift fills with zeros, so every count of the width or more yields 0.
    (zero, $shifted:ident, $amount:ident, $width:ident) => {
        if $amount < $width { $shifted } else { 0 }
    };
}

macro_rules! shifted_integer {
    ($($native:ty => $fill:ident),+ $(,)?) => {
        $(
            impl ShiftedInteger for $native {
                fn lane_left_shift(self, count: ShiftCount) -> (Self, bool) {
                    let width = <$native>::BITS;
                    let amount = u32::from(count.amount);
                    let within_width = amount < width;
                    let bounded = amount.min(width - 1);
                    let shifted = self << bounded;
                    // Shifting back restores the value exactly when no bit, including the sign
                    // of a signed value, left the type.
                    let restored = shifted >> bounded;
                    let lost_bits = restored != self;
                    // A count of the width or more moves every bit out, which only zero survives.
                    let lost_every_bit = !within_width && self != 0;
                    let value = if within_width { shifted } else { 0 };
                    (value, count.negative | lost_bits | lost_every_bit)
                }

                fn lane_right_shift(self, count: ShiftCount) -> (Self, bool) {
                    let width = <$native>::BITS;
                    let amount = u32::from(count.amount);
                    let bounded = amount.min(width - 1);
                    let shifted = self >> bounded;
                    let value = right_shift_fill!($fill, shifted, amount, width);
                    (value, count.negative)
                }
            }
        )+
    };
}

shifted_integer!(
    u8 => zero,
    i8 => sign_bit,
    u16 => zero,
    i16 => sign_bit,
    u32 => zero,
    i32 => sign_bit,
    u64 => zero,
    i64 => sign_bit,
);

#[cfg(test)]
#[path = "bitwise_tests.rs"]
mod tests;
