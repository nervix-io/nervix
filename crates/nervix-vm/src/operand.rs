//! The two shapes a kernel reads a value in: a column, or one scalar every row shares.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The distinction between a value per row and a value shared by every row, the
//!   per-row index each shape answers, and the broadcast that expands a scalar into a column of a
//!   given row count.
//! - **Depends on.** Arrow arrays and the Arrow `Datum` contract.
//! - **Must not know.** Registers, instructions, programs, or which kernel reads an operand.

use std::iter;

use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Datum, PrimitiveArray,
    StringArray, UInt32Array,
};
use arrow_buffer::BooleanBuffer;
use arrow_select::take::take;
use meticulous::ResultExt as _;

/// One operand of a kernel.
///
/// A column holds one value per row of the batch. A scalar holds one value every row shares, as a
/// one-row array, so a literal or an execution-wide value such as `now()` is never copied once per
/// row. Both shapes implement Arrow's [`Datum`], so a kernel that accepts a datum reads a scalar
/// as a scalar, and a kernel that walks rows asks [`Operand::index`] which array index holds the
/// value for a row.
#[derive(Debug)]
pub(crate) enum Operand<'a, A: ?Sized> {
    /// One value per row.
    Column(&'a A),
    /// One value every row shares, held as a one-row array.
    Scalar(&'a A),
}

// An operand is two references, so it is copied freely whatever array it borrows.
impl<A: ?Sized> Clone for Operand<'_, A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A: ?Sized> Copy for Operand<'_, A> {}

impl<'a, A: ?Sized> Operand<'a, A> {
    /// The array holding the operand's values: one per row for a column, one for a scalar.
    pub(crate) fn array(self) -> &'a A {
        match self {
            Self::Column(array) | Self::Scalar(array) => array,
        }
    }

    /// The array index holding the value for `row`, which is `row` itself for a column and the
    /// single index of a scalar.
    pub(crate) fn index(self, row: usize) -> usize {
        match self {
            Self::Column(_) => row,
            Self::Scalar(_) => 0,
        }
    }
}

impl<'a, A: Array + 'static> Operand<'a, A> {
    /// The same operand with its concrete array type erased.
    pub(crate) fn erased(self) -> Operand<'a, dyn Array> {
        match self {
            Self::Column(array) => {
                let array: &dyn Array = array;
                Operand::Column(array)
            }
            Self::Scalar(array) => {
                let array: &dyn Array = array;
                Operand::Scalar(array)
            }
        }
    }
}

impl<A: Array + ?Sized> Operand<'_, A> {
    /// Whether the value for `row` is null.
    pub(crate) fn is_null(self, row: usize) -> bool {
        self.array().is_null(self.index(row))
    }

    /// Which of a batch's `rows` rows hold a value: the valid rows of a column, and every row or
    /// none for a scalar.
    pub(crate) fn validity(self, rows: usize) -> BooleanBuffer {
        match self {
            Self::Column(array) => match array.logical_nulls() {
                Some(nulls) => nulls.inner().clone(),
                None => BooleanBuffer::new_set(rows),
            },
            Self::Scalar(array) => {
                if array.is_null(0) {
                    BooleanBuffer::new_unset(rows)
                } else {
                    BooleanBuffer::new_set(rows)
                }
            }
        }
    }
}

impl<'a> Operand<'a, dyn Array> {
    /// The operand narrowed to one concrete array type, or `None` when it holds another.
    pub(crate) fn downcast<A: Array + 'static>(self) -> Option<Operand<'a, A>> {
        match self {
            Self::Column(array) => array.as_any().downcast_ref::<A>().map(Operand::Column),
            Self::Scalar(array) => array.as_any().downcast_ref::<A>().map(Operand::Scalar),
        }
    }
}

impl<A: Array> Datum for Operand<'_, A> {
    fn get(&self) -> (&dyn Array, bool) {
        match self {
            Self::Column(array) => (*array, false),
            Self::Scalar(array) => (*array, true),
        }
    }
}

impl Datum for Operand<'_, dyn Array> {
    fn get(&self) -> (&dyn Array, bool) {
        match self {
            Self::Column(array) => (*array, false),
            Self::Scalar(array) => (*array, true),
        }
    }
}

/// Expands a one-row array into a column that repeats its value on every row.
pub(crate) trait Broadcast: Sized {
    /// A column of `rows` rows, each holding this array's single value or its null.
    fn broadcast(&self, rows: usize) -> Self;
}

impl<T: ArrowPrimitiveType> Broadcast for PrimitiveArray<T> {
    fn broadcast(&self, rows: usize) -> Self {
        // The data type is carried over because a timestamp array keeps its time zone there.
        if self.is_null(0) {
            return Self::new_null(rows).with_data_type(self.data_type().clone());
        }
        Self::from_value(self.value(0), rows).with_data_type(self.data_type().clone())
    }
}

impl Broadcast for BooleanArray {
    fn broadcast(&self, rows: usize) -> Self {
        if self.is_null(0) {
            return Self::new_null(rows);
        }
        if self.value(0) {
            Self::new(BooleanBuffer::new_set(rows), None)
        } else {
            Self::new(BooleanBuffer::new_unset(rows), None)
        }
    }
}

impl Broadcast for StringArray {
    fn broadcast(&self, rows: usize) -> Self {
        if self.is_null(0) {
            return Self::new_null(rows);
        }
        Self::from_iter_values(iter::repeat_n(self.value(0), rows))
    }
}

impl Broadcast for BinaryArray {
    fn broadcast(&self, rows: usize) -> Self {
        if self.is_null(0) {
            return Self::new_null(rows);
        }
        Self::from_iter_values(iter::repeat_n(self.value(0), rows))
    }
}

impl Broadcast for ArrayRef {
    fn broadcast(&self, rows: usize) -> Self {
        let first_row = UInt32Array::from_value(0, rows);
        take(self.as_ref(), &first_row, None).assured(
            "index 0 is in bounds of a one-row array, and take is defined for every array type a \
             register can hold",
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, BooleanArray, Int64Array, ListArray, StringArray,
        TimestampNanosecondArray, types::Int64Type,
    };
    use arrow_schema::{DataType, TimeUnit};

    use super::{Broadcast, Operand};

    #[test]
    fn scalar_operands_answer_index_zero_for_every_row() {
        let values = StringArray::from(vec![Some("shared")]);
        let operand = Operand::Scalar(&values);

        assert_eq!(operand.index(0), 0);
        assert_eq!(operand.index(17), 0);
        assert!(!operand.is_null(17));
        assert_eq!(operand.array().value(operand.index(17)), "shared");

        let column = Operand::Column(&values);
        assert_eq!(column.index(0), 0);
        assert_eq!(column.index(17), 17);
    }

    #[test]
    fn erased_operands_downcast_back_to_their_array_type() {
        let values = Int64Array::from(vec![Some(4)]);
        let erased = Operand::Scalar(&values).erased();

        let narrowed = erased
            .downcast::<Int64Array>()
            .expect("an Int64 operand downcasts to Int64Array");
        assert!(matches!(narrowed, Operand::Scalar(_)));
        assert_eq!(narrowed.array().value(0), 4);
        assert!(erased.downcast::<StringArray>().is_none());
    }

    #[test]
    fn broadcast_repeats_values_and_nulls_for_every_register_type() {
        let ints = Int64Array::from(vec![Some(7)]).broadcast(3);
        assert_eq!(ints.values().as_ref(), &[7, 7, 7]);
        assert_eq!(ints.null_count(), 0);

        let null_ints = Int64Array::from(vec![None]).broadcast(2);
        assert_eq!(null_ints.len(), 2);
        assert_eq!(null_ints.null_count(), 2);

        let flags = BooleanArray::from(vec![Some(true)]).broadcast(2);
        assert_eq!(flags.iter().collect::<Vec<_>>(), [Some(true), Some(true)]);
        let unset = BooleanArray::from(vec![Some(false)]).broadcast(2);
        assert_eq!(unset.iter().collect::<Vec<_>>(), [Some(false), Some(false)]);
        let null_flags = BooleanArray::from(vec![None]).broadcast(2);
        assert_eq!(null_flags.null_count(), 2);

        let texts = StringArray::from(vec![Some("x")]).broadcast(2);
        assert_eq!(texts.iter().collect::<Vec<_>>(), [Some("x"), Some("x")]);
        let null_texts = StringArray::from(vec![None::<&str>]).broadcast(2);
        assert_eq!(null_texts.null_count(), 2);

        let instants = TimestampNanosecondArray::from(vec![Some(5)])
            .with_timezone_utc()
            .broadcast(2);
        assert_eq!(
            instants.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
        );
        assert_eq!(instants.values().as_ref(), &[5, 5]);

        let list =
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), Some(2)])]);
        let list_ref: ArrayRef = Arc::new(list);
        let lists = list_ref.broadcast(3);
        assert_eq!(lists.len(), 3);
        assert_eq!(lists.null_count(), 0);

        let empty = Int64Array::from(vec![Some(7)]).broadcast(0);
        assert!(empty.is_empty());
    }
}
