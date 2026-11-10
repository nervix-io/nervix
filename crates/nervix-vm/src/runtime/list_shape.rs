//! Column kernels for fixed shapes and one-level list flattening.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reusing child buffers for shape-only changes and gathering transposed child blocks.
//! - **Depends on.** Arrow arrays, the runtime's list view and validated shape vocabulary.
//! - **Must not know.** Models, graph ownership or scheduling policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "executes list shape operations over Arrow child columns"
    )
)]

use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, UInt64Array};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use arrow_select::take::take;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::StdArc;

use super::{ListColumn, collection_arrow_error, collection_too_large};
use crate::{
    CollectionLimit, RuntimeError, TypedArray,
    program::{ArrayDimensions, ListShapeFunction},
};

impl ListShapeFunction {
    pub(super) fn execute(
        &self,
        input: &TypedArray,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let list = ListColumn::from_typed(input)?;
        match self {
            Self::Reshape(dimensions) => dimensions.reshape(list),
            Self::Flatten => self.flatten(list),
            Self::Transpose => self.transpose(list),
        }
    }

    fn flatten(&self, outer: ListColumn<'_>) -> error_stack::Result<TypedArray, RuntimeError> {
        let field = match outer.element_data_type() {
            DataType::List(field) | DataType::FixedSizeList(field, _) => field.clone(),
            actual => {
                return Err(Report::new(RuntimeError::CollectionExpectedList {
                    actual: actual.clone(),
                }));
            }
        };
        let child = TypedArray::Generic(outer.values().clone());
        let inner = ListColumn::from_typed(&child)?;
        if let (ListColumn::Fixed(outer), ListColumn::Fixed(inner)) = (outer, inner) {
            let width = outer
                .value_length()
                .checked_mul(inner.value_length())
                .ok_or_else(|| collection_too_large("list_flatten", CollectionLimit::FixedWidth))?;
            let output = FixedSizeListArray::try_new(
                field,
                width,
                inner.values().clone(),
                outer.nulls().cloned(),
            )
            .map_err(|error| collection_arrow_error("list_flatten fixed construction", error))?;
            return Ok(TypedArray::Generic(StdArc::new(output)));
        }
        self.flatten_variable(outer, inner, field)
    }

    fn flatten_variable(
        &self,
        outer: ListColumn<'_>,
        inner: ListColumn<'_>,
        field: StdArc<Field>,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let capacity = outer
            .len()
            .checked_add(1)
            .assured("an Arrow array leaves address space for its final offset");
        let mut offsets = Vec::with_capacity(capacity);
        let values = if inner.values().is_nullable() && !field.is_nullable() {
            // Fixed arrays may have physical null children hidden by a null container. A VEC's
            // required child column cannot carry those nulls, even outside a valid row's range.
            let mut selected = Vec::new();
            offsets.push(0_i32);
            for row in 0..outer.len() {
                if !outer.is_null(row) {
                    let range = outer.value_range(row);
                    for index in inner.boundary(range.start)..inner.boundary(range.end) {
                        selected.push(u64::try_from(index).assured("an Arrow index fits u64"));
                    }
                }
                offsets.push(i32::try_from(selected.len()).map_err(|_| {
                    collection_too_large("list_flatten", CollectionLimit::VectorOffsets)
                })?);
            }
            let indices = UInt64Array::from_iter_values(selected);
            take(inner.values().as_ref(), &indices, None)
                .map_err(|error| collection_arrow_error("list_flatten visible children", error))?
        } else {
            // Nested list ranges are contiguous. Composing their boundaries shares all child
            // buffers, including when the input's visible first offset is nonzero.
            for row in 0..outer.len() {
                let first = inner.boundary(outer.value_range(row).start);
                offsets.push(i32::try_from(first).map_err(|_| {
                    collection_too_large("list_flatten", CollectionLimit::VectorOffsets)
                })?);
            }
            let last = inner.boundary(outer.boundary(outer.len()));
            offsets.push(i32::try_from(last).map_err(|_| {
                collection_too_large("list_flatten", CollectionLimit::VectorOffsets)
            })?);
            inner.values().clone()
        };
        let output = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            outer.nulls().cloned(),
        )
        .map_err(|error| collection_arrow_error("list_flatten vector construction", error))?;
        Ok(TypedArray::Generic(StdArc::new(output)))
    }

    fn transpose(&self, outer: ListColumn<'_>) -> error_stack::Result<TypedArray, RuntimeError> {
        let outer = outer.fixed("list_transpose")?;
        let child = TypedArray::Generic(outer.values().clone());
        let inner = ListColumn::from_typed(&child)?.fixed("list_transpose")?;
        let first = usize::try_from(outer.value_length()).assured("fixed dimensions are positive");
        let second = usize::try_from(inner.value_length()).assured("fixed dimensions are positive");
        let stride = first
            .checked_mul(second)
            .assured("the two fixed levels fit their concrete child array");
        let mut indices = Vec::with_capacity(inner.values().len());
        for row in 0..outer.len() {
            let base = row
                .checked_mul(stride)
                .assured("the input row lies in the concrete child array");
            for column in 0..second {
                for index in 0..first {
                    let offset = index
                        .checked_mul(second)
                        .assured("the first-axis position fits the concrete child array");
                    let offset = offset
                        .checked_add(column)
                        .assured("both axes lie in the concrete child array");
                    let selected = base
                        .checked_add(offset)
                        .assured("the row and both axes lie in the concrete child array");
                    indices.push(u64::try_from(selected).assured("an Arrow index fits u64"));
                }
            }
        }
        let values = take(
            inner.values().as_ref(),
            &UInt64Array::from_iter_values(indices),
            None,
        )
        .map_err(|error| collection_arrow_error("list_transpose child gather", error))?;
        let inner_nulls = outer.nulls().map(|nulls| nulls.expand(second));
        let rebuilt_inner = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", values.data_type().clone(), false)),
            outer.value_length(),
            values,
            inner_nulls,
        )
        .map_err(|error| collection_arrow_error("list_transpose inner construction", error))?;
        let output = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", rebuilt_inner.data_type().clone(), false)),
            inner.value_length(),
            StdArc::new(rebuilt_inner),
            outer.nulls().cloned(),
        )
        .map_err(|error| collection_arrow_error("list_transpose outer construction", error))?;
        Ok(TypedArray::Generic(StdArc::new(output)))
    }
}

impl ArrayDimensions {
    fn reshape(&self, input: ListColumn<'_>) -> error_stack::Result<TypedArray, RuntimeError> {
        let outer = input.fixed("list_reshape")?;
        let mut values = outer.values().clone();
        while let Some(fixed) = values.as_any().downcast_ref::<FixedSizeListArray>() {
            values = fixed.values().clone();
        }
        self.wrap(values, outer.nulls().cloned())
    }

    /// Rebuild only list levels; a mask on each new level protects physical null children that
    /// the original null outer container hid. The leaf's value buffers are never copied.
    fn wrap(
        &self,
        mut values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let mut prefix = self.element_count();
        for width in self.widths().iter().rev() {
            let size = usize::try_from(*width).assured("validated dimensions are positive");
            prefix /= size;
            let level_nulls = nulls.as_ref().map(|mask| mask.expand(prefix));
            let level = FixedSizeListArray::try_new(
                StdArc::new(Field::new("item", values.data_type().clone(), false)),
                *width,
                values,
                level_nulls,
            )
            .map_err(|error| collection_arrow_error("list_reshape level construction", error))?;
            values = StdArc::new(level);
        }
        Ok(TypedArray::Generic(values))
    }
}

impl<'a> ListColumn<'a> {
    fn fixed(
        self,
        operation: &'static str,
    ) -> error_stack::Result<&'a FixedSizeListArray, RuntimeError> {
        match self {
            Self::Fixed(array) => Ok(array),
            Self::Variable(array) => Err(Report::new(RuntimeError::CollectionExpectedFixed {
                operation,
                actual: array.data_type().clone(),
            })),
        }
    }

    /// The child offset at a row boundary, including the boundary after the final visible row.
    fn boundary(self, row: usize) -> usize {
        match self {
            Self::Variable(array) => usize::try_from(array.value_offsets()[row])
                .assured("Arrow validates nonnegative offsets"),
            Self::Fixed(array) => row
                .checked_mul(
                    usize::try_from(array.value_length()).assured("fixed widths are positive"),
                )
                .assured("the boundary lies within the concrete child array"),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;
