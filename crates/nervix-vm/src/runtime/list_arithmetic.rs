//! Checked element arithmetic over list child columns.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Fixed-run and segmented arithmetic, exact list shape and one failure per row.
//! - **Depends on.** Arrow list views and the VM's existing checked numeric lane contracts.
//! - **Must not know.** Graphs, branches, clocks, registers or scheduling policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns list arithmetic execution")
)]

use super::*;

/// A list's active child span, with only fixed ARRAY levels between it and its numeric leaves.
struct Shape<'a> {
    list: ListColumn<'a>,
    field: &'a arrow_schema::FieldRef,
    leaf: ArrayRef,
    item_start: usize,
    item_count: usize,
    lanes_per_item: usize,
}

impl<'a> Shape<'a> {
    fn new(input: &'a TypedArray) -> error_stack::Result<Self, RuntimeError> {
        let list = ListColumn::from_typed(input)?;
        let data_type = match list {
            ListColumn::Variable(array) => array.data_type(),
            ListColumn::Fixed(array) => array.data_type(),
        };
        let (DataType::List(field) | DataType::FixedSizeList(field, _)) = data_type else {
            return Err(Report::new(RuntimeError::CollectionExpectedList {
                actual: data_type.clone(),
            }));
        };
        let items = match list {
            ListColumn::Variable(array) => {
                let offsets = array.value_offsets();
                let first = usize::try_from(offsets[0]).assured("Arrow offsets are nonnegative");
                let last =
                    usize::try_from(offsets[array.len()]).assured("Arrow offsets are nonnegative");
                first..last
            }
            ListColumn::Fixed(array) => 0..array.values().len(),
        };
        let mut leaf = list.values().slice(items.start, items.len());
        let mut lanes_per_item = if items.is_empty() { 0_usize } else { 1 };
        while let DataType::FixedSizeList(_, _) = leaf.data_type() {
            let array = leaf
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .verified("Arrow's fixed-list type has fixed-list backing");
            let width =
                usize::try_from(array.value_length()).assured("Arrow widths are nonnegative");
            lanes_per_item = lanes_per_item.checked_mul(width).ok_or_else(|| {
                collection_too_large("list arithmetic", CollectionLimit::AddressableLength)
            })?;
            leaf = array.values().clone();
        }
        Ok(Self {
            list,
            field,
            leaf,
            item_start: items.start,
            item_count: items.len(),
            lanes_per_item,
        })
    }

    fn lanes(&self, row: usize) -> Range<usize> {
        let items = self.list.value_range(row);
        let start = items
            .start
            .checked_sub(self.item_start)
            .verified("the row is in the active child span");
        let end = items
            .end
            .checked_sub(self.item_start)
            .verified("the row is in the active child span");
        let start = start
            .checked_mul(self.lanes_per_item)
            .assured("the row begins inside the validated child column");
        let end = end
            .checked_mul(self.lanes_per_item)
            .assured("the row ends inside the validated child column");
        start..end
    }

    fn numbers<T: Number>(&self) -> &PrimitiveArray<T> {
        self.leaf
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .verified("dispatch selects the numeric leaf's exact Arrow type")
    }

    fn is_fixed(&self) -> bool {
        matches!(self.list, ListColumn::Fixed(_))
    }

    fn wrap(
        &self,
        leaf: ArrayRef,
        mut nulls: Option<NullBuffer>,
        variable: bool,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let element = self.list.element_data_type();
        let child = Self::fixed_children(element, leaf, self.item_count)?;
        let field = self.field.clone();
        if !variable && let ListColumn::Fixed(array) = self.list {
            // Width zero has no child value from which Arrow could infer the parent length.
            if array.value_length() == 0 && nulls.is_none() {
                nulls = Some(NullBuffer::new_valid(array.len()));
            }
            let output = FixedSizeListArray::try_new(field, array.value_length(), child, nulls)
                .map_err(|error| collection_arrow_error("list arithmetic ARRAY", error))?;
            return Ok(TypedArray::Generic(StdArc::new(output)));
        }
        let offsets = match self.list {
            ListColumn::Variable(array) if self.item_start == 0 => array.offsets().clone(),
            ListColumn::Variable(array) => {
                let start = array.value_offsets()[0];
                let offsets = array
                    .value_offsets()
                    .iter()
                    .map(|offset| {
                        offset
                            .checked_sub(start)
                            .verified("Arrow offsets are monotonic")
                    })
                    .collect::<Vec<_>>();
                OffsetBuffer::new(ScalarBuffer::from(offsets))
            }
            ListColumn::Fixed(array) => {
                let width =
                    usize::try_from(array.value_length()).assured("Arrow widths are nonnegative");
                Self::fixed_offsets(width, array.len())?
            }
        };
        let output = ListArray::try_new(field, offsets, child, nulls)
            .map_err(|error| collection_arrow_error("list arithmetic VEC", error))?;
        Ok(TypedArray::Generic(StdArc::new(output)))
    }

    fn fixed_offsets(
        width: usize,
        rows: usize,
    ) -> error_stack::Result<OffsetBuffer<i32>, RuntimeError> {
        let items = width.checked_mul(rows).ok_or_else(|| {
            collection_too_large("list arithmetic", CollectionLimit::AddressableLength)
        })?;
        Self::require_vector_items(items)?;
        Ok(OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)))
    }

    fn require_vector_items(items: usize) -> error_stack::Result<(), RuntimeError> {
        i32::try_from(items)
            .map_err(|_| collection_too_large("list arithmetic", CollectionLimit::VectorOffsets))?;
        Ok(())
    }

    fn fixed_children(
        data_type: &DataType,
        leaf: ArrayRef,
        items: usize,
    ) -> error_stack::Result<ArrayRef, RuntimeError> {
        let DataType::FixedSizeList(field, width) = data_type else {
            return Ok(leaf);
        };
        let width_usize = usize::try_from(*width).assured("Arrow widths are nonnegative");
        let child_items = items.checked_mul(width_usize).ok_or_else(|| {
            collection_too_large("list arithmetic", CollectionLimit::AddressableLength)
        })?;
        let child = Self::fixed_children(field.data_type(), leaf, child_items)?;
        let nulls = if *width == 0 {
            Some(NullBuffer::new_valid(items))
        } else {
            None
        };
        let output = FixedSizeListArray::try_new(field.clone(), *width, child, nulls)
            .map_err(|error| collection_arrow_error("list arithmetic nested ARRAY", error))?;
        Ok(StdArc::new(output))
    }
}

/// Parent validity is retained as a shared bitmap until an arithmetic failure changes it.
struct Validity {
    base: Option<NullBuffer>,
    failed: Option<Vec<bool>>,
    rows: usize,
}

impl Validity {
    fn present(&self, row: usize) -> bool {
        match &self.base {
            Some(base) => base.is_valid(row),
            None => true,
        }
    }

    fn fail(&mut self, row: usize) {
        if self.failed.is_none() {
            self.failed = Some((0..self.rows).map(|row| self.present(row)).collect());
        }
        self.failed
            .as_mut()
            .verified("failure validity was initialized above")[row] = false;
    }

    fn finish(self) -> Option<NullBuffer> {
        match self.failed {
            Some(validity) => Some(NullBuffer::from(validity)),
            None => self.base,
        }
    }
}

enum Arguments<'a> {
    Pair {
        operation: Arithmetic,
        left: Shape<'a>,
        right: Shape<'a>,
    },
    Scalar {
        operation: Arithmetic,
        left: Shape<'a>,
        right: Operand<'a, dyn Array>,
    },
    Absolute(Shape<'a>),
}

impl<'a> Arguments<'a> {
    fn shape(&self) -> &Shape<'a> {
        match self {
            Self::Pair { left, .. } | Self::Scalar { left, .. } | Self::Absolute(left) => left,
        }
    }

    fn variable(&self) -> bool {
        match self {
            Self::Pair { left, right, .. } => !left.is_fixed() || !right.is_fixed(),
            _ => !self.shape().is_fixed(),
        }
    }

    fn validity(&self) -> Validity {
        let left = self.shape();
        let base = match self {
            Self::Pair { right, .. } => NullBuffer::union(left.list.nulls(), right.list.nulls()),
            Self::Scalar {
                right: Operand::Column(right),
                ..
            } => NullBuffer::union(left.list.nulls(), right.logical_nulls().as_ref()),
            Self::Scalar {
                right: Operand::Scalar(right),
                ..
            } if right.is_null(0) => Some(NullBuffer::new_null(left.list.len())),
            _ => left.list.nulls().cloned(),
        };
        Validity {
            base,
            failed: None,
            rows: left.list.len(),
        }
    }

    fn fixed_run<T: Number>(&self) -> Option<Checked<T>> {
        let left = self.shape().numbers::<T>();
        match self {
            Self::Pair {
                operation,
                left: shape,
                right,
            } if shape.is_fixed() && right.is_fixed() => Some(T::binary(
                *operation,
                Operand::Column(left),
                Operand::Column(right.numbers::<T>()),
            )),
            Self::Scalar {
                operation,
                left: shape,
                right: Operand::Scalar(right),
            } if shape.is_fixed() => {
                let right = right
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .verified("the scalar has the exact numeric leaf type");
                Some(T::binary(
                    *operation,
                    Operand::Column(left),
                    Operand::Scalar(right),
                ))
            }
            Self::Absolute(_) => Some(T::absolute(left)),
            _ => None,
        }
    }

    fn failure<T: Number>(&self, row: usize, lane: usize) -> SideErrorReason {
        match self {
            Self::Pair {
                operation, right, ..
            } => T::binary_failure(*operation, right.numbers::<T>().value(lane)),
            Self::Scalar {
                operation, right, ..
            } => {
                let right = right
                    .downcast::<PrimitiveArray<T>>()
                    .verified("the scalar has the numeric leaf type");
                T::binary_failure(*operation, right.array().value(right.index(row)))
            }
            Self::Absolute(_) => T::absolute_failure(),
        }
    }

    fn fixed<T: Number>(
        &self,
        checked: Checked<T>,
        errors: &mut RowErrors,
        span: Span,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let shape = self.shape();
        let mut validity = self.validity();
        let mut failures = checked.failed.lanes().peekable();
        for row in 0..shape.list.len() {
            let range = shape.lanes(row);
            while failures.peek().is_some_and(|lane| *lane < range.start) {
                failures
                    .next()
                    .verified("peek identified a pending failed lane");
            }
            if validity.present(row)
                && let Some(lane) = failures.peek()
                && *lane < range.end
            {
                let reason = self.failure::<T>(row, *lane);
                errors.push(row, SideError { reason, span });
                validity.fail(row);
            }
            while failures.peek().is_some_and(|lane| *lane < range.end) {
                failures
                    .next()
                    .verified("peek identified a pending failed lane");
            }
        }
        // Failure validity belongs to the message, never to a list element. Checked lanes already
        // replaced failed values; stripping their null bitmap keeps required children physical.
        let child = PrimitiveArray::<T>::new(checked.column.values().clone(), None);
        shape.wrap(StdArc::new(child), validity.finish(), self.variable())
    }

    fn segmented<T: Number>(
        &self,
        errors: &mut RowErrors,
        span: Span,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        let shape = self.shape();
        let left = shape.numbers::<T>();
        let mut output = Segmented::<T> {
            values: vec![T::Native::default(); left.len()],
            validity: self.validity(),
            errors,
            span,
        };
        for row in 0..shape.list.len() {
            if !output.validity.present(row) {
                continue;
            }
            let range = shape.lanes(row);
            match self {
                Self::Pair {
                    operation, right, ..
                } => {
                    let left_count = shape.list.value_range(row).len();
                    let right_count = right.list.value_range(row).len();
                    if left_count != right_count {
                        output.fail(
                            row,
                            SideErrorReason::VectorLengthMismatch {
                                left: left_count,
                                right: right_count,
                            },
                        );
                        continue;
                    }
                    let right_values = right.numbers::<T>();
                    let right_start = right.lanes(row).start;
                    let left_start = range.start;
                    output.row(
                        row,
                        range,
                        |lane| {
                            let offset = lane
                                .checked_sub(left_start)
                                .verified("the lane lies in this row");
                            let right_lane = right_start
                                .checked_add(offset)
                                .assured("matching list lengths keep the right lane in bounds");
                            let right = right_values.value(right_lane);
                            T::lane_binary(*operation, left.value(lane), right)
                        },
                        |lane| {
                            let offset = lane
                                .checked_sub(left_start)
                                .verified("the failed lane lies in this row");
                            let right_lane = right_start
                                .checked_add(offset)
                                .assured("matching list lengths keep the right lane in bounds");
                            T::binary_failure(*operation, right_values.value(right_lane))
                        },
                    );
                }
                Self::Scalar {
                    operation, right, ..
                } => {
                    let right = right
                        .downcast::<PrimitiveArray<T>>()
                        .verified("the scalar has the exact leaf type");
                    let right = right.array().value(right.index(row));
                    output.row(
                        row,
                        range,
                        |lane| T::lane_binary(*operation, left.value(lane), right),
                        |_| T::binary_failure(*operation, right),
                    );
                }
                Self::Absolute(_) => {
                    output.row(
                        row,
                        range,
                        |lane| T::lane_absolute(left.value(lane)),
                        |_| T::absolute_failure(),
                    );
                }
            }
        }
        let child = PrimitiveArray::<T>::new(ScalarBuffer::from(output.values), None);
        shape.wrap(
            StdArc::new(child),
            output.validity.finish(),
            self.variable(),
        )
    }

    fn execute<T: Number>(
        &self,
        errors: &mut RowErrors,
        span: Span,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        match self.fixed_run::<T>() {
            Some(checked) => self.fixed(checked, errors, span),
            None => self.segmented::<T>(errors, span),
        }
    }

    fn dispatch(
        &self,
        errors: &mut RowErrors,
        span: Span,
    ) -> error_stack::Result<TypedArray, RuntimeError> {
        if self.variable() {
            Shape::require_vector_items(self.shape().item_count)?;
        }
        match self.shape().leaf.data_type() {
            DataType::UInt8 => self.execute::<UInt8Type>(errors, span),
            DataType::Int8 => self.execute::<Int8Type>(errors, span),
            DataType::UInt16 => self.execute::<UInt16Type>(errors, span),
            DataType::Int16 => self.execute::<Int16Type>(errors, span),
            DataType::UInt32 => self.execute::<UInt32Type>(errors, span),
            DataType::Int32 => self.execute::<Int32Type>(errors, span),
            DataType::UInt64 => self.execute::<UInt64Type>(errors, span),
            DataType::Int64 => self.execute::<Int64Type>(errors, span),
            DataType::Float32 => self.execute::<Float32Type>(errors, span),
            DataType::Float64 => self.execute::<Float64Type>(errors, span),
            actual => Err(Report::new(RuntimeError::CollectionNonNumericElement {
                operation: "list arithmetic",
                actual: actual.clone(),
            })),
        }
    }
}

struct Segmented<'a, T: Number> {
    values: Vec<T::Native>,
    validity: Validity,
    errors: &'a mut RowErrors,
    span: Span,
}

impl<T: Number> Segmented<'_, T> {
    fn fail(&mut self, row: usize, reason: SideErrorReason) {
        self.validity.fail(row);
        self.errors.push(
            row,
            SideError {
                reason,
                span: self.span,
            },
        );
    }

    fn row(
        &mut self,
        row: usize,
        range: Range<usize>,
        mut apply: impl FnMut(usize) -> (T::Native, bool),
        failure: impl Fn(usize) -> SideErrorReason,
    ) {
        for lane in range {
            let (value, failed) = apply(lane);
            if failed {
                self.fail(row, failure(lane));
                break;
            }
            self.values[lane] = value;
        }
    }
}

pub(super) fn pair(
    operation: Arithmetic,
    left: &TypedArray,
    right: &TypedArray,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<TypedArray, RuntimeError> {
    let left = Shape::new(left)?;
    let right = Shape::new(right)?;
    left.list
        .require_compatible(right.list, "list arithmetic")?;
    Arguments::Pair {
        operation,
        left,
        right,
    }
    .dispatch(errors, span)
}

pub(super) fn scalar(
    operation: Arithmetic,
    left: &TypedArray,
    right: Operand<'_, dyn Array>,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<TypedArray, RuntimeError> {
    let left = Shape::new(left)?;
    if right.array().data_type() != left.leaf.data_type() {
        return Err(Report::new(RuntimeError::CollectionTypeMismatch {
            operation: "list arithmetic scalar",
            expected: left.leaf.data_type().clone(),
            actual: right.array().data_type().clone(),
        }));
    }
    Arguments::Scalar {
        operation,
        left,
        right,
    }
    .dispatch(errors, span)
}

pub(super) fn absolute(
    input: &TypedArray,
    errors: &mut RowErrors,
    span: Span,
) -> error_stack::Result<TypedArray, RuntimeError> {
    let shape = Shape::new(input)?;
    if matches!(
        shape.leaf.data_type(),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    ) {
        return Ok(input.clone());
    }
    Arguments::Absolute(shape).dispatch(errors, span)
}

/// Each supported width delegates arithmetic and failure classification to the scalar owner.
trait Number: ArrowPrimitiveType + Sized {
    fn binary(
        operation: Arithmetic,
        left: Operand<'_, PrimitiveArray<Self>>,
        right: Operand<'_, PrimitiveArray<Self>>,
    ) -> Checked<Self>;
    fn lane_binary(
        operation: Arithmetic,
        left: Self::Native,
        right: Self::Native,
    ) -> (Self::Native, bool);
    fn binary_failure(operation: Arithmetic, right: Self::Native) -> SideErrorReason;
    fn absolute(input: &PrimitiveArray<Self>) -> Checked<Self>;
    fn lane_absolute(value: Self::Native) -> (Self::Native, bool);
    fn absolute_failure() -> SideErrorReason;
}

macro_rules! integer_number {
    ($ty:ty, $absolute:expr, $lane_absolute:expr) => {
        impl Number for $ty {
            fn binary(
                operation: Arithmetic,
                left: Operand<'_, PrimitiveArray<Self>>,
                right: Operand<'_, PrimitiveArray<Self>>,
            ) -> Checked<Self> {
                operation.evaluate_integers(left, right)
            }
            fn lane_binary(
                operation: Arithmetic,
                left: Self::Native,
                right: Self::Native,
            ) -> (Self::Native, bool) {
                operation.integer_lane(left, right)
            }
            fn binary_failure(operation: Arithmetic, right: Self::Native) -> SideErrorReason {
                operation.integer_failure(right)
            }
            fn absolute(input: &PrimitiveArray<Self>) -> Checked<Self> {
                ($absolute)(input)
            }
            fn lane_absolute(value: Self::Native) -> (Self::Native, bool) {
                ($lane_absolute)(value)
            }
            fn absolute_failure() -> SideErrorReason {
                SideErrorReason::IntegerOverflow(IntegerOperation::AbsoluteValue)
            }
        }
    };
}

macro_rules! unsigned_numbers {
    ($($ty:ty),+) => { $( integer_number!($ty,
        |input: &PrimitiveArray<$ty>| Checked::from_lanes(numeric::Lanes::unary(input.values(), |value| (value, false)), input.nulls().cloned()),
        |value| (value, false)); )+ };
}
unsigned_numbers!(UInt8Type, UInt16Type, UInt32Type, UInt64Type);
integer_number!(
    Int8Type,
    numeric::integer_absolute_value,
    SignedInteger::lane_absolute_value
);
integer_number!(
    Int16Type,
    numeric::integer_absolute_value,
    SignedInteger::lane_absolute_value
);
integer_number!(
    Int32Type,
    numeric::integer_absolute_value,
    SignedInteger::lane_absolute_value
);
integer_number!(
    Int64Type,
    numeric::integer_absolute_value,
    SignedInteger::lane_absolute_value
);

macro_rules! float_numbers {
    ($($ty:ty),+) => { $(
        impl Number for $ty {
            fn binary(operation: Arithmetic, left: Operand<'_, PrimitiveArray<Self>>, right: Operand<'_, PrimitiveArray<Self>>) -> Checked<Self> { operation.evaluate_floats(left, right) }
            fn lane_binary(operation: Arithmetic, left: Self::Native, right: Self::Native) -> (Self::Native, bool) { operation.float_lane(left, right) }
            fn binary_failure(_: Arithmetic, _: Self::Native) -> SideErrorReason { SideErrorReason::NonFiniteResult(FloatOperation::Arithmetic) }
            fn absolute(input: &PrimitiveArray<Self>) -> Checked<Self> { numeric::float_absolute_value(input) }
            fn lane_absolute(value: Self::Native) -> (Self::Native, bool) { value.magnitude().finite_lane() }
            fn absolute_failure() -> SideErrorReason { SideErrorReason::NonFiniteResult(FloatOperation::AbsoluteValue) }
        }
    )+ };
}
float_numbers!(Float32Type, Float64Type);

#[cfg(test)]
mod tests;

#[cfg(test)]
mod properties;
