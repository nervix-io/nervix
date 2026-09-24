//! The counts and positions string and list builtins read from an integer argument.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading an integer operand of any width and signedness as a sign and a magnitude, so
//!   no integer type narrows into another: an unsigned value above the signed range keeps its
//!   value, and the most negative signed value keeps its distance below zero.
//! - **Depends on.** Kernel operands and Arrow's integer arrays.
//! - **Must not know.** Registers, programs, or what a builtin counts or positions with a value.

use std::num::NonZeroUsize;

use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrowPrimitiveType, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use meticulous::OptionExt as _;

use crate::operand::Operand;

/// An integer argument as a sign and a magnitude.
///
/// A magnitude counts characters or list elements, which a supported host addresses with a 64-bit
/// `usize`, so every integer type converts to it without narrowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignedCount {
    /// Zero or a positive value.
    NonNegative(usize),
    /// A negative value, by its distance below zero.
    Negative(NonZeroUsize),
}

/// An integer type a count is read from.
trait CountValue: Copy {
    fn signed_count(self) -> SignedCount;
}

macro_rules! unsigned_count_value {
    ($($native:ty),+ $(,)?) => {
        $(
            impl CountValue for $native {
                fn signed_count(self) -> SignedCount {
                    SignedCount::NonNegative(self.arch_into())
                }
            }
        )+
    };
}

unsigned_count_value!(u8, u16, u32, u64);

macro_rules! signed_count_value {
    ($($native:ty),+ $(,)?) => {
        $(
            impl CountValue for $native {
                fn signed_count(self) -> SignedCount {
                    let magnitude: usize = self.unsigned_abs().arch_into();
                    if self < 0 {
                        SignedCount::Negative(
                            NonZeroUsize::new(magnitude)
                                .verified("a negative value lies at least one below zero"),
                        )
                    } else {
                        SignedCount::NonNegative(magnitude)
                    }
                }
            }
        )+
    };
}

signed_count_value!(i8, i16, i32, i64);

/// A count or position operand of a builtin: a column, or one value every row shares, of any
/// integer type.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CountOperand<'a> {
    UInt8(Operand<'a, UInt8Array>),
    Int8(Operand<'a, Int8Array>),
    UInt16(Operand<'a, UInt16Array>),
    Int16(Operand<'a, Int16Array>),
    UInt32(Operand<'a, UInt32Array>),
    Int32(Operand<'a, Int32Array>),
    UInt64(Operand<'a, UInt64Array>),
    Int64(Operand<'a, Int64Array>),
}

impl<'a> CountOperand<'a> {
    /// The operand read as counts, or `None` when it holds values of another type.
    pub(crate) fn of(operand: Operand<'a, dyn Array>) -> Option<Self> {
        match operand.array().data_type() {
            DataType::UInt8 => operand.downcast().map(Self::UInt8),
            DataType::Int8 => operand.downcast().map(Self::Int8),
            DataType::UInt16 => operand.downcast().map(Self::UInt16),
            DataType::Int16 => operand.downcast().map(Self::Int16),
            DataType::UInt32 => operand.downcast().map(Self::UInt32),
            DataType::Int32 => operand.downcast().map(Self::Int32),
            DataType::UInt64 => operand.downcast().map(Self::UInt64),
            DataType::Int64 => operand.downcast().map(Self::Int64),
            _ => None,
        }
    }

    /// The count for `row`, or `None` when its value is null.
    pub(crate) fn value(self, row: usize) -> Option<SignedCount> {
        match self {
            Self::UInt8(operand) => count_at(operand, row),
            Self::Int8(operand) => count_at(operand, row),
            Self::UInt16(operand) => count_at(operand, row),
            Self::Int16(operand) => count_at(operand, row),
            Self::UInt32(operand) => count_at(operand, row),
            Self::Int32(operand) => count_at(operand, row),
            Self::UInt64(operand) => count_at(operand, row),
            Self::Int64(operand) => count_at(operand, row),
        }
    }
}

/// The count one integer operand holds for `row`.
fn count_at<T>(operand: Operand<'_, PrimitiveArray<T>>, row: usize) -> Option<SignedCount>
where
    T: ArrowPrimitiveType,
    T::Native: CountValue,
{
    let values = operand.array();
    let index = operand.index(row);
    if values.is_null(index) {
        return None;
    }
    Some(values.value(index).signed_count())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow_array::{
        Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, UInt8Array, UInt16Array,
        UInt32Array, UInt64Array,
    };

    use super::{CountOperand, SignedCount};
    use crate::operand::Operand;

    fn negative(magnitude: usize) -> SignedCount {
        SignedCount::Negative(NonZeroUsize::new(magnitude).expect("a negative count is nonzero"))
    }

    /// The counts every row of `values` holds, read as a column.
    fn column_counts(values: &(dyn Array + 'static)) -> Vec<Option<SignedCount>> {
        let operand =
            CountOperand::of(Operand::Column(values)).expect("an integer array reads as counts");
        (0..values.len()).map(|row| operand.value(row)).collect()
    }

    #[test]
    fn every_integer_type_keeps_its_extremes() {
        assert_eq!(
            column_counts(&UInt8Array::from(vec![Some(0), Some(u8::MAX), None])),
            [
                Some(SignedCount::NonNegative(0)),
                Some(SignedCount::NonNegative(255)),
                None
            ]
        );
        assert_eq!(
            column_counts(&Int8Array::from(vec![i8::MIN, -1, i8::MAX])),
            [
                Some(negative(128)),
                Some(negative(1)),
                Some(SignedCount::NonNegative(127))
            ]
        );
        assert_eq!(
            column_counts(&UInt16Array::from(vec![u16::MAX])),
            [Some(SignedCount::NonNegative(65_535))]
        );
        assert_eq!(
            column_counts(&Int16Array::from(vec![i16::MIN, i16::MAX])),
            [
                Some(negative(32_768)),
                Some(SignedCount::NonNegative(32_767))
            ]
        );
        assert_eq!(
            column_counts(&UInt32Array::from(vec![u32::MAX])),
            [Some(SignedCount::NonNegative(4_294_967_295))]
        );
        assert_eq!(
            column_counts(&Int32Array::from(vec![i32::MIN, i32::MAX])),
            [
                Some(negative(2_147_483_648)),
                Some(SignedCount::NonNegative(2_147_483_647))
            ]
        );
        assert_eq!(
            column_counts(&UInt64Array::from(vec![
                Some(u64::MAX),
                Some(1 << 63),
                None
            ])),
            [
                Some(SignedCount::NonNegative(18_446_744_073_709_551_615)),
                Some(SignedCount::NonNegative(9_223_372_036_854_775_808)),
                None
            ]
        );
        assert_eq!(
            column_counts(&Int64Array::from(vec![i64::MIN, -1, 0, i64::MAX])),
            [
                Some(negative(9_223_372_036_854_775_808)),
                Some(negative(1)),
                Some(SignedCount::NonNegative(0)),
                Some(SignedCount::NonNegative(9_223_372_036_854_775_807))
            ]
        );
    }

    #[test]
    fn a_scalar_count_answers_every_row() {
        let shared = UInt64Array::from(vec![u64::MAX]);
        let shared: &dyn Array = &shared;
        let operand =
            CountOperand::of(Operand::Scalar(shared)).expect("an integer scalar reads as a count");

        assert_eq!(
            operand.value(7),
            Some(SignedCount::NonNegative(18_446_744_073_709_551_615))
        );

        let null = Int64Array::from(vec![None]);
        let null: &dyn Array = &null;
        let operand =
            CountOperand::of(Operand::Scalar(null)).expect("an integer scalar reads as a count");
        assert_eq!(operand.value(3), None);
    }

    #[test]
    fn values_of_another_type_are_not_counts() {
        let texts = StringArray::from(vec!["3"]);
        let texts: &dyn Array = &texts;

        assert!(CountOperand::of(Operand::Column(texts)).is_none());
    }
}
