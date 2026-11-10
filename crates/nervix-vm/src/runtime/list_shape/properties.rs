//! Bounded shape laws and an independent transposition oracle.
//!
//! Layer: test harness.
//!
//! - **Owns.** Complete logical float-bit and validity agreement across shape operations.
//! - **Depends on.** The production shape kernels and bounded Arrow fixtures.
//! - **Must not know.** Graph execution, persistence or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies shape laws over bounded current Arrow columns"
    )
)]

use arrow_array::Float64Array;

use super::{
    tests::{fixed, fixed_type, logical_rows, reshape},
    *,
};

#[derive(Debug, bolero::TypeGenerator)]
struct ShapeCase {
    first: u8,
    second: u8,
    third: u8,
    rows: u8,
    nulls: u8,
    bits: Vec<u64>,
}

impl ShapeCase {
    fn check(&self) {
        let first = i32::from(self.first % 4)
            .checked_add(1)
            .verified("a reduced byte plus one is in 1..=4");
        let second = i32::from(self.second % 4)
            .checked_add(1)
            .verified("a reduced byte plus one is in 1..=4");
        let third = i32::from(self.third % 4)
            .checked_add(1)
            .verified("a reduced byte plus one is in 1..=4");
        let first_two = first
            .checked_mul(second)
            .verified("two widths have product at most 16");
        let elements = first_two
            .checked_mul(third)
            .verified("three widths have product at most 64");
        let rows = usize::from(self.rows % 5);
        let width = usize::try_from(elements)
            .verified("three widths in 1..=4 have a positive product at most 64");
        let count = rows
            .checked_mul(width)
            .verified("at most four rows contain 256 scalars");
        let values = Float64Array::from_iter_values((0..count).map(|index| {
            let bits = if self.bits.is_empty() {
                0
            } else {
                self.bits[index % self.bits.len()]
            };
            f64::from_bits(bits)
        }));
        let mask = NullBuffer::from_iter((0..rows).map(|row| self.nulls & (1_u8 << row) == 0));
        let inner = fixed(
            StdArc::new(values),
            third,
            Some(mask.expand(usize::try_from(first_two).verified("two widths are at most 16"))),
        );
        let middle = fixed(
            inner,
            second,
            Some(mask.expand(usize::try_from(first).verified("a width is at most four"))),
        );
        let original = fixed(middle, first, Some(mask));
        let input = TypedArray::Generic(original.clone());
        let expected = logical_rows(&original);

        let reshaped = reshape(&input, vec![i64::from(first_two), i64::from(third)]);
        let flattened = ListShapeFunction::Flatten
            .execute(&reshaped)
            .verified("the generated reshape has two fixed levels");
        let TypedArray::Generic(flat) = &flattened else {
            panic!("flatten returns ARRAY");
        };
        assert_eq!(flat.data_type(), &fixed_type(DataType::Float64, elements));
        assert_eq!(logical_rows(flat), expected);

        let restored = reshape(
            &reshaped,
            vec![i64::from(first), i64::from(second), i64::from(third)],
        );
        let TypedArray::Generic(restored) = restored else {
            panic!("reshape returns ARRAY");
        };
        assert_eq!(restored.data_type(), original.data_type());
        assert_eq!(logical_rows(&restored), expected);

        let transposed = ListShapeFunction::Transpose
            .execute(&input)
            .verified("the generated input has three fixed levels");
        let TypedArray::Generic(transposed_array) = &transposed else {
            panic!("transpose returns ARRAY");
        };
        assert_eq!(
            transposed_array.data_type(),
            &fixed_type(
                fixed_type(fixed_type(DataType::Float64, third), first),
                second
            )
        );
        let mut reference = Vec::with_capacity(rows);
        for row in &expected {
            let Some(values) = row else {
                reference.push(None);
                continue;
            };
            let mut reordered = Vec::with_capacity(width);
            for column in 0..second {
                for index in 0..first {
                    let block = index
                        .checked_mul(second)
                        .verified("the reference has at most sixteen second-level blocks");
                    let block = block
                        .checked_add(column)
                        .verified("both reference axes lie within sixteen blocks");
                    let start = usize::try_from(
                        block
                            .checked_mul(third)
                            .verified("the reference has at most 64 elements"),
                    )
                    .verified("the reference indexes at most 64 elements");
                    let end = start
                        .checked_add(usize::try_from(third).verified("the third width is positive"))
                        .verified("the reference range lies within the generated row");
                    reordered.extend_from_slice(&values[start..end]);
                }
            }
            reference.push(Some(reordered));
        }
        assert_eq!(logical_rows(transposed_array), reference);
        let twice = ListShapeFunction::Transpose
            .execute(&transposed)
            .verified("the transposed array still has three fixed axes");
        let TypedArray::Generic(twice) = twice else {
            panic!("transpose returns ARRAY");
        };
        assert_eq!(twice.data_type(), original.data_type());
        assert_eq!(logical_rows(&twice), expected);
    }
}

#[test]
fn bolero_shape_laws_preserve_complete_values_and_validity() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(512)
        .with_type::<ShapeCase>()
        .for_each(ShapeCase::check);
}
