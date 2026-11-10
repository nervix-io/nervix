//! Bounded differential checks against independent scalar arithmetic.
//!
//! Layer: test harness.
//!
//! - **Owns.** Complete list values, shape, validity and first-failure reference assertions.
//! - **Depends on.** Production list kernels, Arrow arrays and Bolero.
//! - **Must not know.** Runtime graphs or synchronization protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies list arithmetic against scalar references"
    )
)]

use super::*;

struct Bytes<'a> {
    data: &'a [u8],
    position: usize,
}

impl Bytes<'_> {
    fn byte(&mut self) -> u8 {
        if self.data.is_empty() {
            return 0;
        }
        let value = self.data[self.position % self.data.len()];
        self.position = self
            .position
            .checked_add(1)
            .assured("the bounded case reads fewer than ten thousand bytes");
        value
    }
    fn array<const N: usize>(&mut self) -> [u8; N] {
        std::array::from_fn(|_| self.byte())
    }
}

trait Reference: Number {
    fn sample(bytes: &mut Bytes<'_>) -> Self::Native;
    fn reference(
        operation: Arithmetic,
        left: Self::Native,
        right: Self::Native,
    ) -> (Self::Native, Option<SideErrorReason>);
    fn reference_abs(value: Self::Native) -> (Self::Native, Option<SideErrorReason>);
    fn equal(left: Self::Native, right: Self::Native) -> bool;
}

macro_rules! integer_reference {
    ($ty:ty, $native:ty, $abs:expr) => {
        impl Reference for $ty {
            fn sample(bytes: &mut Bytes<'_>) -> Self::Native {
                <$native>::from_le_bytes(bytes.array())
            }
            fn reference(
                operation: Arithmetic,
                left: Self::Native,
                right: Self::Native,
            ) -> (Self::Native, Option<SideErrorReason>) {
                let value = match operation {
                    Arithmetic::Add => left.checked_add(right),
                    Arithmetic::Sub => left.checked_sub(right),
                    Arithmetic::Mul => left.checked_mul(right),
                    Arithmetic::Div => left.checked_div(right),
                    Arithmetic::Rem => left.checked_rem(right),
                };
                if let Some(value) = value {
                    return (value, None);
                }
                let reason = match operation {
                    Arithmetic::Add => SideErrorReason::IntegerOverflow(IntegerOperation::Addition),
                    Arithmetic::Sub => {
                        SideErrorReason::IntegerOverflow(IntegerOperation::Subtraction)
                    }
                    Arithmetic::Mul => {
                        SideErrorReason::IntegerOverflow(IntegerOperation::Multiplication)
                    }
                    Arithmetic::Div if right == 0 => {
                        SideErrorReason::DivisionByZero(crate::DivisionOperation::Division)
                    }
                    Arithmetic::Div => SideErrorReason::IntegerOverflow(IntegerOperation::Division),
                    Arithmetic::Rem => {
                        SideErrorReason::DivisionByZero(crate::DivisionOperation::Remainder)
                    }
                };
                (0, Some(reason))
            }
            fn reference_abs(value: Self::Native) -> (Self::Native, Option<SideErrorReason>) {
                match ($abs)(value) {
                    Some(value) => (value, None),
                    None => (
                        0,
                        Some(SideErrorReason::IntegerOverflow(
                            IntegerOperation::AbsoluteValue,
                        )),
                    ),
                }
            }
            fn equal(left: Self::Native, right: Self::Native) -> bool {
                left == right
            }
        }
    };
}
integer_reference!(UInt8Type, u8, Some);
integer_reference!(Int8Type, i8, i8::checked_abs);
integer_reference!(UInt16Type, u16, Some);
integer_reference!(Int16Type, i16, i16::checked_abs);
integer_reference!(UInt32Type, u32, Some);
integer_reference!(Int32Type, i32, i32::checked_abs);
integer_reference!(UInt64Type, u64, Some);
integer_reference!(Int64Type, i64, i64::checked_abs);

macro_rules! float_reference {
    ($ty:ty, $native:ty, $bits:ty) => {
        impl Reference for $ty {
            fn sample(bytes: &mut Bytes<'_>) -> Self::Native {
                <$native>::from_bits(<$bits>::from_le_bytes(bytes.array()))
            }
            fn reference(
                operation: Arithmetic,
                left: Self::Native,
                right: Self::Native,
            ) -> (Self::Native, Option<SideErrorReason>) {
                let value = match operation {
                    Arithmetic::Add => left + right,
                    Arithmetic::Sub => left - right,
                    Arithmetic::Mul => left * right,
                    Arithmetic::Div => left / right,
                    Arithmetic::Rem => left % right,
                };
                if value.is_finite() {
                    (value, None)
                } else {
                    (
                        0.0,
                        Some(SideErrorReason::NonFiniteResult(FloatOperation::Arithmetic)),
                    )
                }
            }
            fn reference_abs(value: Self::Native) -> (Self::Native, Option<SideErrorReason>) {
                let value = value.abs();
                if value.is_finite() {
                    (value, None)
                } else {
                    (
                        0.0,
                        Some(SideErrorReason::NonFiniteResult(
                            FloatOperation::AbsoluteValue,
                        )),
                    )
                }
            }
            fn equal(left: Self::Native, right: Self::Native) -> bool {
                left.to_bits() == right.to_bits()
            }
        }
    };
}
float_reference!(Float32Type, f32, u32);
float_reference!(Float64Type, f64, u64);

enum Expected<N> {
    Null,
    Values(Vec<N>),
    Failed(SideErrorReason),
}

impl<N: Copy> Expected<N> {
    fn values(
        left: &[N],
        mut operation: impl FnMut(usize, N) -> (N, Option<SideErrorReason>),
    ) -> Self {
        let mut values = Vec::with_capacity(left.len());
        for (index, left) in left.iter().copied().enumerate() {
            let (value, failure) = operation(index, left);
            if let Some(failure) = failure {
                return Self::Failed(failure);
            }
            values.push(value);
        }
        Self::Values(values)
    }
}

struct Case<T: Reference> {
    left: Vec<Vec<T::Native>>,
    right: Vec<Vec<T::Native>>,
    left_present: Vec<bool>,
    right_present: Vec<bool>,
    scalars: Vec<Option<T::Native>>,
    left_fixed: bool,
    right_fixed: bool,
    nested: bool,
}

impl<T: Reference> Case<T> {
    fn read(bytes: &mut Bytes<'_>) -> Self {
        let flags = bytes.byte();
        let left_fixed = flags & 1 != 0;
        let right_fixed = flags & 2 != 0;
        let nested = flags & 4 != 0;
        let lanes = if nested { 2 } else { 1 };
        let rows = usize::from(bytes.byte() % 8)
            .checked_add(1)
            .assured("at most eight rows");
        let mut case = Self {
            left: Vec::new(),
            right: Vec::new(),
            left_present: Vec::new(),
            right_present: Vec::new(),
            scalars: Vec::new(),
            left_fixed,
            right_fixed,
            nested,
        };
        for _ in 0..rows {
            let left_length = if left_fixed {
                2
            } else {
                usize::from(bytes.byte() % 5)
            };
            let right_length = if right_fixed {
                2
            } else {
                usize::from(bytes.byte() % 5)
            };
            case.left.push(
                (0..left_length
                    .checked_mul(lanes)
                    .assured("at most eight lanes"))
                    .map(|_| T::sample(bytes))
                    .collect(),
            );
            case.right.push(
                (0..right_length
                    .checked_mul(lanes)
                    .assured("at most eight lanes"))
                    .map(|_| T::sample(bytes))
                    .collect(),
            );
            case.left_present.push(bytes.byte() & 7 != 0);
            case.right_present.push(bytes.byte() & 7 != 0);
            case.scalars.push(if bytes.byte() & 7 == 0 {
                None
            } else {
                Some(T::sample(bytes))
            });
        }
        case
    }

    fn element_type(&self) -> DataType {
        if self.nested {
            DataType::FixedSizeList(StdArc::new(Field::new("item", T::DATA_TYPE, false)), 2)
        } else {
            T::DATA_TYPE
        }
    }

    fn field(&self) -> arrow_schema::FieldRef {
        StdArc::new(
            Field::new("item", self.element_type(), false).with_metadata(
                [("purpose".to_owned(), "exact list arithmetic".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        )
    }

    fn list_type(&self, fixed: bool) -> DataType {
        let field = self.field();
        if fixed {
            DataType::FixedSizeList(field, 2)
        } else {
            DataType::List(field)
        }
    }

    fn array(&self, rows: &[Vec<T::Native>], present: &[bool], fixed: bool) -> TypedArray {
        let lanes = if self.nested { 2 } else { 1 };
        // Prefix and suffix rows stay outside the sliced parent. Their values and bitmap bits
        // must never enter an output or a failure assertion for the active rows.
        let mut values = vec![T::Native::default(); 2 * lanes];
        let mut lengths = vec![2_usize];
        for row in rows {
            values.extend_from_slice(row);
            lengths.push(row.len() / lanes);
        }
        values.extend(std::iter::repeat_n(T::Native::default(), 2 * lanes));
        lengths.push(2);
        let mut validity = vec![true];
        validity.extend_from_slice(present);
        validity.push(true);
        let mut child: ArrayRef = StdArc::new(PrimitiveArray::<T>::from_iter_values(values));
        if self.nested {
            child = StdArc::new(FixedSizeListArray::new(
                StdArc::new(Field::new("item", T::DATA_TYPE, false)),
                2,
                child,
                None,
            ));
        }
        let field = self.field();
        let array: ArrayRef = if fixed {
            StdArc::new(FixedSizeListArray::new(
                field,
                2,
                child,
                Some(NullBuffer::from(validity)),
            ))
        } else {
            StdArc::new(ListArray::new(
                field,
                OffsetBuffer::from_lengths(lengths),
                child,
                Some(NullBuffer::from(validity)),
            ))
        };
        TypedArray::Generic(array.slice(1, rows.len()))
    }

    fn assert(
        &self,
        result: TypedArray,
        errors: RowErrors,
        expected: Vec<Expected<T::Native>>,
        data_type: DataType,
    ) {
        assert_eq!(result.data_type(), data_type);
        assert_eq!(result.len(), expected.len());
        let array = result.as_array();
        for (row, expected) in expected.into_iter().enumerate() {
            match expected {
                Expected::Null => {
                    assert!(array.is_null(row));
                    assert!(errors.row(row).is_empty());
                }
                Expected::Failed(reason) => {
                    assert!(array.is_null(row));
                    assert_eq!(
                        errors.row(row),
                        &[SideError {
                            reason,
                            span: (0..1).into()
                        }]
                    );
                }
                Expected::Values(values) => {
                    assert!(!array.is_null(row));
                    assert!(errors.row(row).is_empty());
                    let mut actual = match data_type {
                        DataType::List(_) => array
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .verified("vector result")
                            .value(row),
                        DataType::FixedSizeList(_, _) => array
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .verified("fixed result")
                            .value(row),
                        _ => panic!("arithmetic returns a list"),
                    };
                    if self.nested {
                        actual = actual
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .verified("nested fixed shape")
                            .values()
                            .clone();
                    }
                    assert_eq!(actual.null_count(), 0);
                    let actual = actual
                        .as_any()
                        .downcast_ref::<PrimitiveArray<T>>()
                        .verified("exact numeric output type");
                    assert_eq!(actual.len(), values.len());
                    for (actual, expected) in actual.values().iter().copied().zip(values) {
                        assert!(T::equal(actual, expected), "{actual:?} != {expected:?}");
                    }
                }
            }
        }
    }

    fn check(&self) {
        let left = self.array(&self.left, &self.left_present, self.left_fixed);
        let right = self.array(&self.right, &self.right_present, self.right_fixed);
        for operation in [
            Arithmetic::Add,
            Arithmetic::Sub,
            Arithmetic::Mul,
            Arithmetic::Div,
        ] {
            let mut expected = Vec::new();
            for row in 0..self.left.len() {
                let value = if !self.left_present[row] || !self.right_present[row] {
                    Expected::Null
                } else if self.left[row].len() != self.right[row].len() {
                    let lanes = if self.nested { 2 } else { 1 };
                    Expected::Failed(SideErrorReason::VectorLengthMismatch {
                        left: self.left[row].len() / lanes,
                        right: self.right[row].len() / lanes,
                    })
                } else {
                    Expected::values(&self.left[row], |index, left| {
                        T::reference(operation, left, self.right[row][index])
                    })
                };
                expected.push(value);
            }
            let mut errors = RowErrors::new(self.left.len());
            let result = pair(operation, &left, &right, &mut errors, (0..1).into())
                .verified("valid numeric list pair");
            self.assert(
                result,
                errors,
                expected,
                self.list_type(self.left_fixed && self.right_fixed),
            );
            self.check_scalars(operation, &left);
        }
        let expected = self
            .left
            .iter()
            .zip(&self.left_present)
            .map(|(row, present)| {
                if *present {
                    Expected::values(row, |_, value| T::reference_abs(value))
                } else {
                    Expected::Null
                }
            })
            .collect();
        let mut errors = RowErrors::new(self.left.len());
        let result =
            absolute(&left, &mut errors, (0..1).into()).verified("numeric list absolute value");
        self.assert(result, errors, expected, self.list_type(self.left_fixed));
    }

    fn check_scalars(&self, operation: Arithmetic, left: &TypedArray) {
        let column = PrimitiveArray::<T>::from_iter(self.scalars.iter().copied());
        let shared = PrimitiveArray::<T>::from_iter([self.scalars[0]]);
        for (operand, scalar_values) in [
            (Operand::Column(&column), self.scalars.clone()),
            (
                Operand::Scalar(&shared),
                vec![self.scalars[0]; self.left.len()],
            ),
        ] {
            let mut expected = Vec::new();
            for (row, right) in scalar_values.into_iter().enumerate() {
                let value = match right {
                    Some(right) if self.left_present[row] => {
                        Expected::values(&self.left[row], |_, left| {
                            T::reference(operation, left, right)
                        })
                    }
                    _ => Expected::Null,
                };
                expected.push(value);
            }
            let mut errors = RowErrors::new(self.left.len());
            let result = scalar(
                operation,
                left,
                operand.erased(),
                &mut errors,
                (0..1).into(),
            )
            .verified("exact numeric scalar operand");
            self.assert(result, errors, expected, self.list_type(self.left_fixed));
        }
    }
}

fn check(bytes: &[u8]) {
    let mut bytes = Bytes {
        data: bytes,
        position: 0,
    };
    Case::<UInt8Type>::read(&mut bytes).check();
    Case::<Int8Type>::read(&mut bytes).check();
    Case::<UInt16Type>::read(&mut bytes).check();
    Case::<Int16Type>::read(&mut bytes).check();
    Case::<UInt32Type>::read(&mut bytes).check();
    Case::<Int32Type>::read(&mut bytes).check();
    Case::<UInt64Type>::read(&mut bytes).check();
    Case::<Int64Type>::read(&mut bytes).check();
    Case::<Float32Type>::read(&mut bytes).check();
    Case::<Float64Type>::read(&mut bytes).check();
}

#[test]
fn bolero_list_arithmetic_matches_scalar_lanes() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .for_each(|bytes: &[u8]| check(bytes));
}

#[test]
fn list_arithmetic_scalar_reference_edges() {
    for bytes in [
        &[][..],
        &[0_u8; 64][..],
        &[255_u8; 64][..],
        &[1, 2, 3, 4, 5, 6, 7, 8][..],
        &[7, 3, 128, 0, 0, 0, 0, 0][..],
    ] {
        check(bytes);
    }
}
