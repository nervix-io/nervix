//! Kernel tests for the sign, truncation, classification and trigonometric builtins.
//!
//! Layer: test harness.
//!
//! - **Owns.** Reference cases for `sign`, `trunc`, `is_nan`, `is_finite`, `is_infinite`, `sin`,
//!   `atan2`, `log2`, `radians` and `degrees`: signed zeros, infinities, NaN, extremes, and the lanes
//!   each one fails.
//! - **Depends on.** The numeric kernels and Arrow arrays.
//! - **Must not know.** Programs, registers or how the runtime records a failed lane.

use std::f64::consts::{FRAC_PI_2, FRAC_PI_4, FRAC_PI_6, PI};

use arrow_array::{
    Array, Float32Array, Float64Array, Int8Array, Int64Array, UInt16Array, UInt64Array,
};

use super::*;

fn failed_lanes<T: ArrowPrimitiveType>(checked: &Checked<T>) -> Vec<usize> {
    checked.failed.lanes().collect()
}

/// Whether `value` lies within `units` units in the last place of `expected`.
fn within_units(value: f64, expected: f64, units: u32) -> bool {
    let mut low = expected;
    let mut high = expected;
    for _ in 0..units {
        low = low.next_down();
        high = high.next_up();
    }
    (low..=high).contains(&value)
}

#[test]
fn the_angle_conversion_constants_are_the_nearest_f64_to_their_exact_values() {
    let radians_per_degree: f64 =
        "0.0174532925199432957692369076848861271344287188854172545609719144"
            .parse()
            .expect("a decimal expansion parses");
    let degrees_per_radian: f64 =
        "57.2957795130823208767981548141051703324054724665643215491602438"
            .parse()
            .expect("a decimal expansion parses");

    assert_eq!(RADIANS_PER_DEGREE.to_bits(), radians_per_degree.to_bits());
    assert_eq!(DEGREES_PER_RADIAN.to_bits(), degrees_per_radian.to_bits());
}

#[test]
fn signs_are_minus_one_zero_or_one_at_the_input_width() {
    let signed = Int8Array::from(vec![Some(i8::MIN), Some(-1), Some(0), Some(i8::MAX), None]);
    let unsigned = UInt64Array::from(vec![0, 1, u64::MAX]);

    let signed_sign = integer_sign(&signed);
    let unsigned_sign = integer_sign(&unsigned);

    assert_eq!(&signed_sign.values()[..4], &[-1, -1, 0, 1]);
    assert!(signed_sign.is_null(4));
    assert_eq!(unsigned_sign.values().as_ref(), &[0, 1, 1]);
}

#[test]
fn a_float_sign_keeps_the_sign_of_zero_and_fails_only_nan() {
    let input = Float64Array::from(vec![
        Some(-3.5),
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(1)),
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        Some(f64::NAN),
        None,
    ]);

    let sign = float_sign(&input);

    assert_eq!(sign.column.value(0), -1.0);
    assert_eq!(sign.column.value(1).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(sign.column.value(2).to_bits(), 0.0_f64.to_bits());
    assert_eq!(sign.column.value(3), 1.0);
    assert_eq!(sign.column.value(4), -1.0);
    assert_eq!(sign.column.value(5), 1.0);
    assert!(sign.column.is_null(6));
    assert!(sign.column.is_null(7));
    assert_eq!(failed_lanes(&sign), [6]);

    let narrow = float_sign(&Float32Array::from(vec![-0.0_f32, f32::MIN_POSITIVE]));
    assert_eq!(narrow.column.value(0).to_bits(), (-0.0_f32).to_bits());
    assert_eq!(narrow.column.value(1), 1.0);
}

#[test]
fn truncation_rounds_toward_zero_and_fails_only_non_finite_operands() {
    let input = Float64Array::from(vec![-2.7, -0.5, 0.5, 2.7, 1e300, f64::INFINITY, f64::NAN]);

    let truncated = Rounding::Trunc.evaluate_floats(&input);

    assert_eq!(truncated.column.value(0), -2.0);
    assert_eq!(truncated.column.value(1).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(truncated.column.value(2).to_bits(), 0.0_f64.to_bits());
    assert_eq!(truncated.column.value(3), 2.0);
    assert_eq!(truncated.column.value(4), 1e300);
    assert_eq!(failed_lanes(&truncated), [5, 6]);
    assert_eq!(
        Rounding::Trunc.float_failure(),
        SideErrorReason::NonFiniteResult(FloatOperation::Trunc)
    );
}

#[test]
fn classification_covers_every_class_without_failing_or_hiding_nulls() {
    let values = [
        f64::NAN,
        -f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        f64::MAX,
        f64::from_bits(1),
    ];
    let mut lanes = values.iter().copied().map(Some).collect::<Vec<_>>();
    lanes.push(None);
    let input = Float64Array::from(lanes);

    let nan = FloatClass::Nan.evaluate(&input);
    let finite = FloatClass::Finite.evaluate(&input);
    let infinite = FloatClass::Infinite.evaluate(&input);

    let expected_nan = [true, true, false, false, false, false, false, false];
    let expected_finite = [false, false, false, false, true, true, true, true];
    let expected_infinite = [false, false, true, true, false, false, false, false];
    for lane in 0..values.len() {
        assert_eq!(nan.value(lane), expected_nan[lane], "is_nan lane {lane}");
        assert_eq!(
            finite.value(lane),
            expected_finite[lane],
            "is_finite lane {lane}"
        );
        assert_eq!(
            infinite.value(lane),
            expected_infinite[lane],
            "is_infinite lane {lane}"
        );
    }
    for classified in [&nan, &finite, &infinite] {
        assert!(classified.is_null(values.len()));
    }

    let narrow = FloatClass::Infinite.evaluate(&Float32Array::from(vec![f32::INFINITY, f32::MAX]));
    assert!(narrow.value(0));
    assert!(!narrow.value(1));
}

#[test]
fn sine_meets_its_reference_values_and_fails_only_non_finite_operands() {
    let input = Float64Array::from(vec![
        0.0,
        -0.0,
        FRAC_PI_6,
        FRAC_PI_2,
        1e-300,
        f64::INFINITY,
        f64::NAN,
    ]);

    let sine = MathFunction::Sin.evaluate(&F64Operand::new(&input));

    assert_eq!(sine.column.value(0).to_bits(), 0.0_f64.to_bits());
    assert_eq!(sine.column.value(1).to_bits(), (-0.0_f64).to_bits());
    assert!(within_units(sine.column.value(2), 0.5, 2));
    assert!(within_units(sine.column.value(3), 1.0, 2));
    assert_eq!(sine.column.value(4), 1e-300);
    assert_eq!(failed_lanes(&sine), [5, 6]);
    assert_eq!(
        MathFunction::Sin.failure(),
        SideErrorReason::NonFiniteResult(FloatOperation::Sin)
    );
}

#[test]
fn atan2_selects_the_quadrant_from_both_signs_and_fails_only_nan() {
    let y = F64Operand::new(&Float64Array::from(vec![
        Some(1.0),
        Some(0.0),
        Some(-0.0),
        Some(f64::INFINITY),
        Some(1.0),
        Some(f64::NAN),
        None,
    ]));
    let x = F64Operand::new(&Int64Array::from(vec![
        Some(1),
        Some(-1),
        Some(-1),
        Some(-1),
        Some(0),
        Some(1),
        Some(1),
    ]));

    let angle = BinaryMathFunction::Atan2.evaluate(&y, &x);

    assert!(within_units(angle.column.value(0), FRAC_PI_4, 2));
    assert!(within_units(angle.column.value(1), PI, 2));
    assert!(within_units(angle.column.value(2), -PI, 2));
    assert!(within_units(angle.column.value(3), FRAC_PI_2, 2));
    assert!(within_units(angle.column.value(4), FRAC_PI_2, 2));
    assert_eq!(failed_lanes(&angle), [5]);
    assert!(angle.column.is_null(6));

    let infinities = BinaryMathFunction::Atan2.evaluate(
        &F64Operand::new(&Float64Array::from(vec![f64::INFINITY])),
        &F64Operand::new(&Float64Array::from(vec![f64::NEG_INFINITY])),
    );
    assert!(within_units(infinities.column.value(0), 3.0 * FRAC_PI_4, 2));
}

#[test]
fn log2_is_exact_for_powers_of_two_and_fails_outside_its_domain() {
    let input = Float64Array::from(vec![
        8.0,
        1.0,
        0.5,
        f64::from_bits(1),
        3.0,
        0.0,
        -1.0,
        f64::INFINITY,
    ]);

    let logarithm = MathFunction::Log2.evaluate(&F64Operand::new(&input));

    assert_eq!(logarithm.column.value(0), 3.0);
    assert_eq!(logarithm.column.value(1).to_bits(), 0.0_f64.to_bits());
    assert_eq!(logarithm.column.value(2), -1.0);
    assert_eq!(logarithm.column.value(3), -1074.0);
    assert!(within_units(
        logarithm.column.value(4),
        1.584_962_500_721_156,
        2
    ));
    assert_eq!(failed_lanes(&logarithm), [5, 6, 7]);
}

#[test]
fn angle_conversions_multiply_by_one_constant_and_fail_non_finite_results() {
    let degrees = F64Operand::new(&UInt16Array::from(vec![180, 90, 45, 0]));
    let radians = F64Operand::new(&Float64Array::from(vec![
        PI,
        FRAC_PI_2,
        -0.0,
        f64::MAX,
        f64::NEG_INFINITY,
    ]));

    let to_radians = MathFunction::Radians.evaluate(&degrees);
    let to_degrees = MathFunction::Degrees.evaluate(&radians);

    assert_eq!(to_radians.column.value(0).to_bits(), PI.to_bits());
    assert_eq!(to_radians.column.value(1).to_bits(), FRAC_PI_2.to_bits());
    assert_eq!(to_radians.column.value(2).to_bits(), FRAC_PI_4.to_bits());
    assert_eq!(to_radians.column.value(3).to_bits(), 0.0_f64.to_bits());
    assert_eq!(to_degrees.column.value(0), 180.0);
    assert_eq!(to_degrees.column.value(1), 90.0);
    assert_eq!(to_degrees.column.value(2).to_bits(), (-0.0_f64).to_bits());
    // The largest F64 times 180/π overflows.
    assert_eq!(failed_lanes(&to_degrees), [3, 4]);
    assert_eq!(
        MathFunction::Degrees.failure(),
        SideErrorReason::NonFiniteResult(FloatOperation::Degrees)
    );
}
