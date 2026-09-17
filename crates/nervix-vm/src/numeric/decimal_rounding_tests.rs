//! Decimal rounding kernel tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Reference cases for `round(value, digits)` computed with exact decimal arithmetic,
//!   agreement between the significand arithmetic and the decimal expansion it falls back to, the
//!   integer overflow and tie boundaries, and how the kernels treat digit columns and nulls.
//! - **Depends on.** The decimal rounding kernels and Arrow arrays.
//! - **Must not know.** Programs, registers or how the runtime records a failed lane.

use arrow_array::{
    Array, Float32Array, Float64Array, Int8Array, Int64Array, UInt8Array, UInt64Array,
};

use super::*;
use crate::batch::TypedArray;

/// `(value, digits, expected bits)`, where each expected value is the `F64` nearest to the exact
/// decimal rounding of the value's binary value, computed with Python's `decimal` module at 3000
/// digits of precision and `ROUND_HALF_UP`, which rounds halves away from zero.
const F64_REFERENCES: [(f64, i16, u64); 33] = [
    // 0.15 is stored below 0.15, so it rounds down although its decimal digits look like a tie.
    (0.15, 1, 0x3fb9_9999_9999_999a),
    (2.675, 2, 0x4005_5c28_f5c2_8f5c),
    (1.005, 2, 0x3ff0_0000_0000_0000),
    // 0.125 is exactly a tie at two places, and a tie rounds away from zero.
    (0.125, 2, 0x3fc0_a3d7_0a3d_70a4),
    (-0.125, 2, 0xbfc0_a3d7_0a3d_70a4),
    (2.5, 0, 0x4008_0000_0000_0000),
    (-2.5, 0, 0xc008_0000_0000_0000),
    (0.499_999_999_999_999_94, 0, 0x0000_0000_0000_0000),
    (1_234.567_8, -2, 0x4092_c000_0000_0000),
    (1_250.0, -2, 0x4094_5000_0000_0000),
    (-1_250.0, -2, 0xc094_5000_0000_0000),
    // A negative value that rounds to zero keeps its sign.
    (-0.004, 2, 0x8000_0000_0000_0000),
    (123_456_789.123_456_79, 4, 0x419d_6f34_547e_76c9),
    (5e-324, 323, 0x0000_0000_0000_0000),
    (5e-324, 324, 0x0000_0000_0000_0001),
    // Rounding the largest value to a multiple of 10^308 overflows to an infinity.
    (f64::MAX, -308, 0x7ff0_0000_0000_0000),
    (1e22, -22, 0x4480_f0cf_064d_d592),
    (4.5e22, -23, 0x0000_0000_0000_0000),
    // 5e22 is stored below 5e22, so it is below half of 10^23.
    (5e22, -23, 0x0000_0000_0000_0000),
    (5.5e22, -23, 0x44b5_2d02_c7e1_4af6),
    (1.234_567_890_123_456_8e-10, 25, 0x3de0_f7bf_e5e2_538c),
    (5.960_464_477_539_063e-8, 23, 0x3e70_0000_0000_0000),
    (-5.960_464_477_539_063e-8, 23, 0xbe70_0000_0000_0000),
    (1.2345e30, -25, 0x462f_29c4_eeb1_55e5),
    (1e23, -22, 0x44b5_2d02_c7e1_4af6),
    (0.1, 20, 0x3fb9_9999_9999_999a),
    (0.1, 17, 0x3fb9_9999_9999_999a),
    (1e-7, 7, 0x3e7a_d7f2_9abc_af48),
    (4.567_89, 3, 0x4012_45a1_cac0_8312),
    (-4.567_89, 3, 0xc012_45a1_cac0_8312),
    (6.022_140_76e23, -20, 0x44df_e154_f457_ea13),
    (1.5, -400, 0x0000_0000_0000_0000),
    (f64::MAX, 400, 0x7fef_ffff_ffff_ffff),
];

/// `(value bits, digits, expected bits)` for `F32`, where each expected value is the `F32` nearest
/// to the exact decimal rounding of the value's binary value.
const F32_REFERENCES: [(u32, i16, u32); 16] = [
    // The F32 nearest to 0.15 is above 0.15, so it rounds up.
    (0x3e19_999a, 1, 0x3e4c_cccd),
    (0x402b_3333, 2, 0x402a_e148),
    (0x3e00_0000, 2, 0x3e05_1eb8),
    (0xbe00_0000, 2, 0xbe05_1eb8),
    (0x4020_0000, 0, 0x4040_0000),
    (0x449a_522b, -2, 0x4496_0000),
    (0xbb83_126f, 2, 0x8000_0000),
    (0x0000_0001, 44, 0x0000_0000),
    (0x0000_0001, 45, 0x0000_0001),
    (0x7f7f_ffff, -38, 0x7f61_b1e6),
    (0x2f07_bdff, 15, 0x2f07_be0e),
    (0x7f7f_c99e, -10, 0x7f7f_c99e),
    (0x7f7f_c99e, -11, 0x7f7f_c99e),
    (0x4b80_0000, -1, 0x4b80_0002),
    (0x3dcc_cccd, 9, 0x3dcc_cccd),
    (0x3fc0_0000, -60, 0x0000_0000),
];

#[test]
fn floats_round_exactly_to_the_reference_decimal_rounding() {
    for (value, digits, expected) in F64_REFERENCES {
        let rounded = value.rounded_to_digits(digits);
        assert_eq!(
            rounded.to_bits(),
            expected,
            "round({value:e}, {digits}) gave {rounded:e}, expected {:e}",
            f64::from_bits(expected)
        );
    }
    for (bits, digits, expected) in F32_REFERENCES {
        let value = f32::from_bits(bits);
        let rounded = value.rounded_to_digits(digits);
        assert_eq!(
            rounded.to_bits(),
            expected,
            "round({value:e}, {digits}) gave {rounded:e}, expected {:e}",
            f32::from_bits(expected)
        );
    }
}

#[test]
fn the_decimal_expansion_reproduces_every_reference_including_ties() {
    for (value, digits, expected) in F64_REFERENCES {
        let rounded = value.rounded_by_decimal_expansion(value.binary_parts(), digits);
        assert_eq!(rounded.to_bits(), expected, "round({value:e}, {digits})");
    }
    for (bits, digits, expected) in F32_REFERENCES {
        let value = f32::from_bits(bits);
        let rounded = value.rounded_by_decimal_expansion(value.binary_parts(), digits);
        assert_eq!(rounded.to_bits(), expected, "round({value:e}, {digits})");
    }
}

/// A deterministic xorshift64* sequence, so every run checks the same values.
struct XorShift64Star(u64);

impl Iterator for XorShift64Star {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        // The multiplication is the generator's output mixer, so wrapping is its meaning.
        Some(self.0.wrapping_mul(0x2545_f491_4f6c_dd1d))
    }
}

/// Digit counts on both sides of every exactly representable power of ten.
fn differential_digits(draw: u64) -> i16 {
    let spread = i16::try_from(draw % 81).expect("below 81");
    spread - 40
}

#[test]
fn significand_arithmetic_agrees_with_the_decimal_expansion() {
    let mut draws = XorShift64Star(0x9e37_79b9_7f4a_7c15);
    for _ in 0..40_000 {
        let pattern = draws.next().expect("the sequence is endless");
        let digits = differential_digits(draws.next().expect("the sequence is endless"));
        let wide = f64::from_bits(pattern);
        if wide.is_finite() && wide != 0.0 {
            let expected = wide.rounded_by_decimal_expansion(wide.binary_parts(), digits);
            let rounded = wide.rounded_to_digits(digits);
            assert_eq!(
                rounded.to_bits(),
                expected.to_bits(),
                "round({wide:e}, {digits})"
            );
        }
        let narrow = f32::from_bits(u32::try_from(pattern >> 32).expect("32 bits"));
        if narrow.is_finite() && narrow != 0.0 {
            let expected = narrow.rounded_by_decimal_expansion(narrow.binary_parts(), digits);
            let rounded = narrow.rounded_to_digits(digits);
            assert_eq!(
                rounded.to_bits(),
                expected.to_bits(),
                "round({narrow:e}, {digits})"
            );
        }
        // Decimal-looking values: a short decimal significand at a moderate exponent.
        let significand = i32::try_from(pattern % 2_000_001).expect("below 2000001") - 1_000_000;
        let exponent = i32::try_from((pattern >> 40) % 25).expect("below 25") - 12;
        let decimal: f64 = format!("{significand}e{exponent}")
            .parse()
            .expect("a decimal literal parses");
        if decimal != 0.0 {
            let expected = decimal.rounded_by_decimal_expansion(decimal.binary_parts(), digits);
            let rounded = decimal.rounded_to_digits(digits);
            assert_eq!(
                rounded.to_bits(),
                expected.to_bits(),
                "round({decimal:e}, {digits})"
            );
        }
    }
}

#[test]
fn non_finite_values_and_zeros_are_returned_unchanged() {
    for digits in [-400, -3, 0, 2, 400] {
        assert!(f64::NAN.rounded_to_digits(digits).is_nan());
        assert_eq!(f64::INFINITY.rounded_to_digits(digits), f64::INFINITY);
        assert_eq!(
            (-0.0_f64).rounded_to_digits(digits).to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            f32::NEG_INFINITY.rounded_to_digits(digits),
            f32::NEG_INFINITY
        );
    }
}

#[test]
fn the_power_of_two_exponent_of_each_power_of_ten_is_exact() {
    for exponent_of_ten in 0..=38_i32 {
        let power = 10_u128.pow(u32::try_from(exponent_of_ten).expect("nonnegative"));
        let expected = i32::try_from(power.ilog2()).expect("below 128");
        assert_eq!(
            binary_exponent_of_power_of_ten(exponent_of_ten),
            expected,
            "10^{exponent_of_ten}"
        );
    }
    for exponent_of_ten in 39..=i32::from(DIGITS_BOUND) {
        let expected = (f64::from(exponent_of_ten) * std::f64::consts::LOG2_10).floor();
        assert_eq!(
            f64::from(binary_exponent_of_power_of_ten(exponent_of_ten)),
            expected,
            "10^{exponent_of_ten}"
        );
    }
}

#[test]
fn integers_round_to_multiples_of_powers_of_ten_with_halves_away_from_zero() {
    let cases: [(i64, i16, Option<i64>); 12] = [
        (1_234, 0, Some(1_234)),
        (1_234, 3, Some(1_234)),
        (1_234, -1, Some(1_230)),
        (1_235, -1, Some(1_240)),
        (-1_235, -1, Some(-1_240)),
        (-1_234, -2, Some(-1_200)),
        (499, -3, Some(0)),
        (500, -3, Some(1_000)),
        (i64::MIN, -18, Some(-9_000_000_000_000_000_000)),
        (i64::MAX, -19, None),
        (i64::MAX, -20, Some(0)),
        (i64::MIN, -400, Some(0)),
    ];
    for (value, digits, expected) in cases {
        let (rounded, failed) = value.lane_rounded_to_digits(digits);
        match expected {
            Some(expected) => {
                assert!(!failed, "round({value}, {digits}) must not fail");
                assert_eq!(rounded, expected, "round({value}, {digits})");
            }
            None => assert!(failed, "round({value}, {digits}) must overflow"),
        }
    }

    assert_eq!(124_i8.lane_rounded_to_digits(-1), (120, false));
    assert!(125_i8.lane_rounded_to_digits(-1).1);
    assert!((-125_i8).lane_rounded_to_digits(-1).1);
    assert_eq!((-124_i8).lane_rounded_to_digits(-1), (-120, false));
    assert_eq!(254_u8.lane_rounded_to_digits(-1), (250, false));
    assert!(255_u8.lane_rounded_to_digits(-1).1);
    assert!(250_u8.lane_rounded_to_digits(-2).1);
    assert_eq!(249_u8.lane_rounded_to_digits(-2), (200, false));
    assert_eq!(
        14_999_999_999_999_999_999_u64.lane_rounded_to_digits(-19),
        (10_000_000_000_000_000_000, false)
    );
    assert!(u64::MAX.lane_rounded_to_digits(-19).1);
}

#[test]
fn digit_columns_of_every_integer_type_are_held_within_the_bound() {
    let wide = RoundingDigits::from_typed(&TypedArray::Int64(Int64Array::from(vec![
        i64::MIN,
        -3,
        2,
        i64::MAX,
    ])))
    .expect("an integer column holds digits");
    assert_eq!(wide.lanes, [-400, -3, 2, 400]);

    let unsigned =
        RoundingDigits::from_typed(&TypedArray::UInt64(UInt64Array::from(vec![0, u64::MAX])))
            .expect("an integer column holds digits");
    assert_eq!(unsigned.lanes, [0, 400]);

    let narrow = RoundingDigits::from_typed(&TypedArray::Int8(Int8Array::from(vec![-128, 127])))
        .expect("an integer column holds digits");
    assert_eq!(narrow.lanes, [-128, 127]);

    assert!(
        RoundingDigits::from_typed(&TypedArray::Float64(Float64Array::from(vec![1.0]))).is_none()
    );
}

#[test]
fn a_null_value_or_digit_count_nulls_the_lane_without_failing_it() {
    let values = Float64Array::from(vec![Some(2.675), None, Some(f64::NAN), Some(1.25)]);
    let digits = RoundingDigits::from_typed(&TypedArray::UInt8(UInt8Array::from(vec![
        Some(2),
        Some(2),
        Some(1),
        None,
    ])))
    .expect("an integer column holds digits");

    let rounded = digits.round_floats(&values);

    assert_eq!(rounded.column.value(0).to_bits(), 2.67_f64.to_bits());
    assert!(rounded.column.is_null(1));
    assert!(rounded.column.is_null(3));
    assert_eq!(rounded.failed.lanes().collect::<Vec<_>>(), [2]);

    let narrow = Float32Array::from(vec![Some(0.125_f32), Some(f32::MAX)]);
    let negative_digits =
        RoundingDigits::from_typed(&TypedArray::Int64(Int64Array::from(vec![2, -38])))
            .expect("an integer column holds digits");
    let rounded = negative_digits.round_floats(&narrow);
    assert_eq!(rounded.column.value(0).to_bits(), 0x3e05_1eb8);
    assert_eq!(rounded.column.value(1).to_bits(), 0x7f61_b1e6);
    assert_eq!(rounded.failed.lanes().count(), 0);

    let integers = Int8Array::from(vec![Some(125), Some(124), None]);
    let tens = RoundingDigits::from_typed(&TypedArray::Int8(Int8Array::from(vec![-1, -1, -1])))
        .expect("an integer column holds digits");
    let rounded = tens.round_integers(&integers);
    assert_eq!(rounded.failed.lanes().collect::<Vec<_>>(), [0]);
    assert!(rounded.column.is_null(0));
    assert_eq!(rounded.column.value(1), 120);
    assert!(rounded.column.is_null(2));
}
