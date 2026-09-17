//! Integer bit operation kernel tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Direct tests of the bit kernels: every lane at bitmap word boundaries, extremes of
//!   each signedness, shift counts that are negative, zero, the width and beyond, and the failure
//!   reasons of each shift.
//! - **Depends on.** The bit kernels and Arrow arrays.
//! - **Must not know.** Programs, registers or how the runtime records a failed lane.

use arrow_array::{
    Array, Int8Array, Int16Array, Int64Array, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};

use super::*;
use crate::error::{IntegerOperation, ShiftOperation, SideErrorReason};

/// Lane counts on both sides of every bitmap word boundary.
const LANE_COUNTS: [usize; 6] = [1, 63, 64, 65, 128, 130];

fn counts_of(values: Vec<i64>) -> ShiftCounts {
    ShiftCounts::from_typed(&TypedArray::Int64(Int64Array::from(values)))
        .expect("an integer column holds counts")
}

#[test]
fn bitwise_operators_combine_every_lane_with_the_folding_contract() {
    for lanes in LANE_COUNTS {
        // Scattered bit patterns across the whole I16 range, sign bit included.
        let pattern = |lane: usize, multiplier: usize| {
            u16::try_from(lane * multiplier % 65_536)
                .expect("reduced below 65536")
                .cast_signed()
        };
        let left = Int16Array::from_iter_values((0..lanes).map(|lane| pattern(lane, 40_503)));
        let right = Int16Array::from_iter_values((0..lanes).map(|lane| pattern(lane + 7, 21_011)));
        for operation in [
            BitwiseOperation::And,
            BitwiseOperation::Or,
            BitwiseOperation::Xor,
        ] {
            let combined = operation.evaluate(&left, &right);
            assert_eq!(combined.len(), lanes);
            for lane in 0..lanes {
                let expected = match operation {
                    BitwiseOperation::And => left.value(lane) & right.value(lane),
                    BitwiseOperation::Or => left.value(lane) | right.value(lane),
                    BitwiseOperation::Xor => left.value(lane) ^ right.value(lane),
                };
                assert_eq!(combined.value(lane), expected, "{operation:?} lane {lane}");
            }
        }
    }
}

#[test]
fn a_null_operand_nulls_the_combined_lane() {
    let left = UInt32Array::from(vec![Some(0b1100), None, Some(u32::MAX)]);
    let right = UInt32Array::from(vec![Some(0b1010), Some(1), None]);

    let combined = BitwiseOperation::Xor.evaluate(&left, &right);

    assert_eq!(combined.value(0), 0b0110);
    assert!(combined.is_null(1));
    assert!(combined.is_null(2));
}

#[test]
fn complement_and_bit_count_read_the_twos_complement_bits_at_the_type_width() {
    let unsigned = UInt8Array::from(vec![Some(0), Some(0b1010_0101), None, Some(u8::MAX)]);
    let signed = Int64Array::from(vec![0, -1, i64::MIN, i64::MAX]);

    let unsigned_complement = bitwise_complement(&unsigned);
    let signed_complement = bitwise_complement(&signed);
    let unsigned_count = bit_count(&unsigned);
    let signed_count = bit_count(&signed);

    assert_eq!(unsigned_complement.value(0), u8::MAX);
    assert_eq!(unsigned_complement.value(1), 0b0101_1010);
    assert!(unsigned_complement.is_null(2));
    assert_eq!(unsigned_complement.value(3), 0);
    assert_eq!(
        signed_complement.values().as_ref(),
        &[-1, 0, i64::MAX, i64::MIN]
    );
    assert_eq!(unsigned_count.value(0), 0);
    assert_eq!(unsigned_count.value(1), 4);
    assert!(unsigned_count.is_null(2));
    assert_eq!(unsigned_count.value(3), 8);
    assert_eq!(signed_count.values().as_ref(), &[0, 64, 1, 63]);
}

#[test]
fn a_left_shift_multiplies_by_a_power_of_two_and_fails_when_the_product_does_not_fit() {
    let values = Int8Array::from(vec![1, 1, 63, 64, -1, -1, -64, -65, 0, 1, 0, 5]);
    let counts = counts_of(vec![0, 6, 1, 1, 7, 8, 1, 1, 8, 7, 1_000, -1]);

    let shifted = Shift::Left.evaluate(&values, &counts);

    assert_eq!(shifted.column.value(0), 1);
    assert_eq!(shifted.column.value(1), 64);
    assert_eq!(shifted.column.value(2), 126);
    // 64 × 2 is 128, which an I8 cannot hold, and 1 × 2^7 changes the sign bit.
    assert!(shifted.column.is_null(3));
    assert_eq!(shifted.column.value(4), -128);
    assert!(shifted.column.is_null(5));
    assert_eq!(shifted.column.value(6), -128);
    assert!(shifted.column.is_null(7));
    // Zero shifted by the width or any larger count is still zero.
    assert_eq!(shifted.column.value(8), 0);
    assert!(shifted.column.is_null(9));
    assert_eq!(shifted.column.value(10), 0);
    assert!(shifted.column.is_null(11));
    assert_eq!(shifted.failed.lanes().collect::<Vec<_>>(), [3, 5, 7, 9, 11]);
    assert_eq!(
        Shift::Left.failure(&counts, 3),
        SideErrorReason::IntegerOverflow(IntegerOperation::LeftShift)
    );
    assert_eq!(
        Shift::Left.failure(&counts, 11),
        SideErrorReason::NegativeShiftCount(ShiftOperation::LeftShift)
    );
}

#[test]
fn an_unsigned_left_shift_fails_for_every_bit_shifted_out() {
    let values = UInt64Array::from(vec![1, 1, u64::MAX >> 1, u64::MAX, 3]);
    let counts = ShiftCounts::from_typed(&TypedArray::UInt64(UInt64Array::from(vec![
        63,
        64,
        1,
        1,
        u64::MAX,
    ])))
    .expect("an integer column holds counts");

    let shifted = Shift::Left.evaluate(&values, &counts);

    assert_eq!(shifted.column.value(0), 1 << 63);
    assert_eq!(shifted.column.value(2), u64::MAX - 1);
    assert_eq!(shifted.failed.lanes().collect::<Vec<_>>(), [1, 3, 4]);
}

#[test]
fn a_right_shift_rounds_toward_negative_infinity_and_fails_only_negative_counts() {
    let signed = Int16Array::from(vec![-5, -1, 5, i16::MIN, i16::MIN, 7, 7]);
    let signed_counts = counts_of(vec![1, 100, 1, 15, 16, 0, i64::MIN]);
    let unsigned = UInt16Array::from(vec![u16::MAX, u16::MAX, u16::MAX, 8]);
    let unsigned_counts = counts_of(vec![15, 16, i64::MAX, 3]);

    let signed_shifted = Shift::Right.evaluate(&signed, &signed_counts);
    let unsigned_shifted = Shift::Right.evaluate(&unsigned, &unsigned_counts);

    assert_eq!(signed_shifted.column.value(0), -3);
    assert_eq!(signed_shifted.column.value(1), -1);
    assert_eq!(signed_shifted.column.value(2), 2);
    assert_eq!(signed_shifted.column.value(3), -1);
    assert_eq!(signed_shifted.column.value(4), -1);
    assert_eq!(signed_shifted.column.value(5), 7);
    assert!(signed_shifted.column.is_null(6));
    assert_eq!(signed_shifted.failed.lanes().collect::<Vec<_>>(), [6]);
    assert_eq!(
        Shift::Right.failure(&signed_counts, 6),
        SideErrorReason::NegativeShiftCount(ShiftOperation::RightShift)
    );
    assert_eq!(unsigned_shifted.column.values().as_ref(), &[1, 0, 0, 1]);
    assert_eq!(unsigned_shifted.failed.lanes().count(), 0);
}

#[test]
fn a_null_value_or_count_nulls_the_shift_without_failing_it() {
    let values = Int64Array::from(vec![Some(i64::MAX), None, Some(1)]);
    let counts = ShiftCounts::from_typed(&TypedArray::Int64(Int64Array::from(vec![
        None,
        Some(-1),
        Some(2),
    ])))
    .expect("an integer column holds counts");

    let shifted = Shift::Left.evaluate(&values, &counts);

    assert!(shifted.column.is_null(0));
    assert!(shifted.column.is_null(1));
    assert_eq!(shifted.column.value(2), 4);
    assert_eq!(shifted.failed.lanes().count(), 0);
}
