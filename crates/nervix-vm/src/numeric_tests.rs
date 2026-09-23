//! Checked numeric kernel tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Direct tests of the numeric kernels: bitmap word boundaries, sliced operands, null
//!   lanes, the placeholder a failed lane holds, and the failure reasons of each operation.
//! - **Depends on.** The numeric kernels and Arrow arrays.
//! - **Must not know.** Programs, registers or how the runtime records a failed lane.

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int32Array, Int64Array,
    UInt16Array, UInt64Array,
};

use super::*;
use crate::operand::Operand;

/// Lane counts on both sides of every bitmap word boundary.
const LANE_COUNTS: [usize; 7] = [0, 1, 63, 64, 65, 128, 130];

fn failed_lanes<T: ArrowPrimitiveType>(checked: &Checked<T>) -> Vec<usize> {
    checked.failed.lanes().collect()
}

#[test]
fn integer_sum_fails_exactly_the_overflowing_lanes_at_every_word_boundary() {
    for lanes in LANE_COUNTS {
        let left = Int8Array::from_iter_values((0..lanes).map(|lane| {
            if lane % 5 == 3 {
                i8::MAX
            } else {
                i8::try_from(lane % 100).expect("below 100")
            }
        }));
        let right = Int8Array::from_iter_values((0..lanes).map(|_| 1));

        let checked =
            Arithmetic::Add.evaluate_integers(Operand::Column(&left), Operand::Column(&right));

        let expected = (0..lanes).filter(|lane| lane % 5 == 3).collect::<Vec<_>>();
        assert_eq!(failed_lanes(&checked), expected, "{lanes} lanes");
        assert_eq!(checked.column.len(), lanes);
        assert_eq!(checked.column.null_count(), expected.len(), "{lanes} lanes");
        for lane in 0..lanes {
            if lane % 5 == 3 {
                assert!(checked.column.is_null(lane));
            } else {
                assert_eq!(checked.column.value(lane), left.value(lane) + 1);
            }
        }
    }
}

#[test]
fn a_failed_lane_holds_the_default_value_instead_of_the_wrapped_result() {
    let left = Int64Array::from(vec![i64::MAX, 2, i64::MIN]);
    let right = Int64Array::from(vec![1, 3, -1]);

    let sum = Arithmetic::Add.evaluate_integers(Operand::Column(&left), Operand::Column(&right));
    let product =
        Arithmetic::Mul.evaluate_integers(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(failed_lanes(&sum), [0, 2]);
    assert_eq!(sum.column.values().as_ref(), &[0, 5, 0]);
    assert_eq!(failed_lanes(&product), [2]);
    assert_eq!(product.column.values().as_ref(), &[i64::MAX, 6, 0]);
}

#[test]
fn null_lanes_never_fail_whatever_their_operands_hold() {
    // Every lane divides by zero, but only the valid lanes have operands to divide.
    let left = Int32Array::from(vec![Some(7), None, Some(9), None]);
    let right = Int32Array::from(vec![Some(0), Some(0), None, None]);

    let quotient =
        Arithmetic::Div.evaluate_integers(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(failed_lanes(&quotient), [0]);
    assert!((0..4).all(|lane| quotient.column.is_null(lane)));
}

#[test]
fn a_clean_kernel_shares_its_operand_validity_and_allocates_no_failure_bitmap() {
    let input = Float64Array::from(vec![Some(1.5), None, Some(-2.5)]);

    let magnitude = float_absolute_value(&input);

    assert!(magnitude.failed.0.is_none());
    let operand_nulls = input.nulls().expect("the operand has a null");
    let column_nulls = magnitude.column.nulls().expect("the column keeps the null");
    assert!(column_nulls.inner().ptr_eq(operand_nulls.inner()));
    assert_eq!(magnitude.column.value(0), 1.5);
    assert_eq!(magnitude.column.value(2), 2.5);
}

#[test]
fn sliced_operands_are_read_from_their_offset() {
    let left = UInt16Array::from(vec![u16::MAX, 10, 20, 30, u16::MAX]).slice(1, 3);
    let right = UInt16Array::from(vec![u16::MAX, 3, 30, 7, u16::MAX]).slice(1, 3);

    let difference =
        Arithmetic::Sub.evaluate_integers(Operand::Column(&left), Operand::Column(&right));
    let less = Comparison::Lt.evaluate(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(failed_lanes(&difference), [1]);
    assert_eq!(difference.column.value(0), 7);
    assert!(difference.column.is_null(1));
    assert_eq!(difference.column.value(2), 23);
    assert_eq!(less, BooleanArray::from(vec![false, true, false]));
}

#[test]
fn the_remainder_of_the_minimum_value_by_minus_one_is_zero() {
    let left = Int64Array::from(vec![i64::MIN, i64::MIN, 7, 7]);
    let right = Int64Array::from(vec![-1, 0, -1, 0]);

    let remainder =
        Arithmetic::Rem.evaluate_integers(Operand::Column(&left), Operand::Column(&right));
    let quotient =
        Arithmetic::Div.evaluate_integers(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(failed_lanes(&remainder), [1, 3]);
    assert_eq!(remainder.column.value(0), 0);
    assert_eq!(remainder.column.value(2), 0);
    assert_eq!(failed_lanes(&quotient), [0, 1, 3]);
    assert_eq!(quotient.column.value(2), -7);
}

#[test]
fn integer_failures_name_the_operation_that_failed() {
    assert_eq!(
        Arithmetic::Add.integer_failure(1_i32),
        SideErrorReason::IntegerOverflow(IntegerOperation::Addition)
    );
    assert_eq!(
        Arithmetic::Sub.integer_failure(1_i32),
        SideErrorReason::IntegerOverflow(IntegerOperation::Subtraction)
    );
    assert_eq!(
        Arithmetic::Mul.integer_failure(1_i32),
        SideErrorReason::IntegerOverflow(IntegerOperation::Multiplication)
    );
    assert_eq!(
        Arithmetic::Div.integer_failure(0_u8),
        SideErrorReason::DivisionByZero(DivisionOperation::Division)
    );
    assert_eq!(
        Arithmetic::Div.integer_failure(-1_i8),
        SideErrorReason::IntegerOverflow(IntegerOperation::Division)
    );
    assert_eq!(
        Arithmetic::Rem.integer_failure(0_u64),
        SideErrorReason::DivisionByZero(DivisionOperation::Remainder)
    );
}

#[test]
fn signed_negation_and_absolute_value_fail_only_the_minimum_value() {
    let input = Int8Array::from(vec![Some(i8::MIN), Some(-5), None, Some(i8::MAX), Some(0)]);

    let negated = integer_negation(&input);
    let magnitude = integer_absolute_value(&input);

    assert_eq!(failed_lanes(&negated), [0]);
    assert_eq!(negated.column.value(1), 5);
    assert_eq!(negated.column.value(3), -i8::MAX);
    assert_eq!(failed_lanes(&magnitude), [0]);
    assert_eq!(magnitude.column.value(1), 5);
    assert_eq!(magnitude.column.value(4), 0);
    assert!(magnitude.column.is_null(2));
}

#[test]
fn float_arithmetic_fails_non_finite_results_and_keeps_the_exact_width() {
    let left = Float32Array::from(vec![f32::MAX, 1.0, 0.0, f32::NAN, 16_777_216.0]);
    let right = Float32Array::from(vec![f32::MAX, 0.0, 0.0, 1.0, 1.0]);

    let sum = Arithmetic::Add.evaluate_floats(Operand::Column(&left), Operand::Column(&right));
    let quotient = Arithmetic::Div.evaluate_floats(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(failed_lanes(&sum), [0, 3]);
    // `F32` arithmetic rounds at 32 bits: 16777217 is a tie between the two nearest `F32` values
    // and rounds to the even one, where `F64` arithmetic would represent it exactly.
    assert_eq!(sum.column.value(4).to_bits(), 16_777_216.0_f32.to_bits());
    assert_eq!(failed_lanes(&quotient), [1, 2, 3]);
    assert_eq!(quotient.column.value(0), 1.0);
}

#[test]
fn float_negation_never_fails_and_keeps_the_sign_of_zero() {
    let input = Float64Array::from(vec![0.0, -0.0, f64::NAN, f64::INFINITY]);

    let negated = float_negation(&input);

    assert_eq!(negated.value(0).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(negated.value(1).to_bits(), 0.0_f64.to_bits());
    assert!(negated.value(2).is_nan());
    assert_eq!(negated.value(3), f64::NEG_INFINITY);
    assert_eq!(negated.null_count(), 0);
}

#[test]
fn float_absolute_value_clears_the_sign_of_zero_and_fails_non_finite_operands() {
    let input = Float64Array::from(vec![-0.0, -2.5, f64::NEG_INFINITY, f64::NAN]);

    let magnitude = float_absolute_value(&input);

    assert_eq!(magnitude.column.value(0).to_bits(), 0.0_f64.to_bits());
    assert_eq!(magnitude.column.value(1), 2.5);
    assert_eq!(failed_lanes(&magnitude), [2, 3]);
}

#[test]
fn rounding_rounds_halves_away_from_zero_and_fails_only_non_finite_operands() {
    let input = Float64Array::from(vec![2.5, -2.5, -0.4, f64::INFINITY, 1.5]);

    let rounded = Rounding::Round.evaluate_floats(&input);
    let ceiling = Rounding::Ceil.evaluate_floats(&input);
    let floor = Rounding::Floor.evaluate_floats(&input);

    assert_eq!(rounded.column.value(0), 3.0);
    assert_eq!(rounded.column.value(1), -3.0);
    assert_eq!(rounded.column.value(2).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(rounded.column.value(4), 2.0);
    assert_eq!(ceiling.column.value(2).to_bits(), (-0.0_f64).to_bits());
    assert_eq!(floor.column.value(2), -1.0);
    for checked in [&rounded, &ceiling, &floor] {
        assert_eq!(failed_lanes(checked), [3]);
    }
    assert_eq!(
        Rounding::Floor.float_failure(),
        SideErrorReason::NonFiniteResult(FloatOperation::Floor)
    );
}

#[test]
fn float_comparison_follows_ieee_754_rather_than_the_total_order() {
    let left = Float64Array::from(vec![Some(f64::NAN), Some(0.0), Some(-1.0), None]);
    let right = Float64Array::from(vec![Some(f64::NAN), Some(-0.0), Some(f64::NAN), Some(1.0)]);

    let equal = Comparison::Eq.evaluate(Operand::Column(&left), Operand::Column(&right));
    let not_equal = Comparison::NotEq.evaluate(Operand::Column(&left), Operand::Column(&right));
    let at_least = Comparison::GtEq.evaluate(Operand::Column(&left), Operand::Column(&right));
    let less = Comparison::Lt.evaluate(Operand::Column(&left), Operand::Column(&right));

    assert_eq!(
        equal,
        BooleanArray::from(vec![Some(false), Some(true), Some(false), None])
    );
    assert_eq!(
        not_equal,
        BooleanArray::from(vec![Some(true), Some(false), Some(true), None])
    );
    assert_eq!(
        at_least,
        BooleanArray::from(vec![Some(false), Some(true), Some(false), None])
    );
    assert_eq!(
        less,
        BooleanArray::from(vec![Some(false), Some(false), Some(false), None])
    );
}

#[test]
fn math_functions_evaluate_only_valid_lanes_with_the_same_results_as_every_lane() {
    let values = (0..130)
        .map(|lane| f64::from(u8::try_from(lane).expect("below 130")) * 0.37 - 20.0)
        .collect::<Vec<_>>();
    let dense = Float64Array::from(values.clone());
    let sparse = Float64Array::from_iter(
        values
            .iter()
            .enumerate()
            .map(|(lane, value)| (lane % 3 != 1).then_some(*value)),
    );

    for function in [
        MathFunction::Acos,
        MathFunction::Asin,
        MathFunction::Atan,
        MathFunction::Cos,
        MathFunction::Exp,
        MathFunction::Ln,
        MathFunction::Log10,
        MathFunction::Sqrt,
        MathFunction::Tan,
    ] {
        let every_lane = function.evaluate(&F64Operand::new(&dense));
        let valid_lanes = function.evaluate(&F64Operand::new(&sparse));

        let every_failed = failed_lanes(&every_lane)
            .into_iter()
            .filter(|lane| lane % 3 != 1)
            .collect::<Vec<_>>();
        assert_eq!(failed_lanes(&valid_lanes), every_failed, "{function:?}");
        for lane in (0..130).filter(|lane| lane % 3 != 1) {
            assert_eq!(
                valid_lanes.column.is_null(lane),
                every_lane.column.is_null(lane),
                "{function:?} lane {lane}"
            );
            assert_eq!(
                valid_lanes.column.value(lane).to_bits(),
                every_lane.column.value(lane).to_bits(),
                "{function:?} lane {lane}"
            );
        }
        assert!(
            (0..130)
                .filter(|lane| lane % 3 == 1)
                .all(|lane| valid_lanes.column.is_null(lane))
        );
    }
}

#[test]
fn math_operands_read_integers_as_the_nearest_f64() {
    let wide = UInt64Array::from(vec![Some(u64::MAX), None, Some(9_007_199_254_740_993)]);

    let operand = F64Operand::new(&wide);
    let root = MathFunction::Sqrt.evaluate(&operand);

    assert_eq!(
        operand.values[0].to_bits(),
        18_446_744_073_709_551_616.0_f64.to_bits()
    );
    assert_eq!(
        operand.values[2].to_bits(),
        9_007_199_254_740_992.0_f64.to_bits()
    );
    assert_eq!(root.column.value(0), 4_294_967_296.0);
    assert!(root.column.is_null(1));
}

#[test]
fn binary_math_functions_evaluate_log_with_its_base_first() {
    let base = F64Operand::new(&Float64Array::from(vec![
        Some(2.0),
        Some(1.0),
        None,
        Some(-8.0),
    ]));
    let value = F64Operand::new(&Int64Array::from(vec![
        Some(1024),
        Some(10),
        Some(10),
        Some(3),
    ]));

    let logarithm = BinaryMathFunction::Log.evaluate(&base, &value);
    let power = BinaryMathFunction::Pow.evaluate(&base, &value);

    assert_eq!(
        logarithm.column.value(0).to_bits(),
        1024.0_f64.log(2.0).to_bits()
    );
    // A base of 1 has no logarithm, since ln(10) / ln(1) is an infinity, and a negative base has
    // a NaN logarithm.
    assert_eq!(failed_lanes(&logarithm), [1, 3]);
    assert!(logarithm.column.is_null(2));
    // 2 to the 1024th overflows `F64`.
    assert_eq!(failed_lanes(&power), [0]);
    assert_eq!(power.column.value(1), 1.0);
    assert_eq!(power.column.value(3), -512.0);
    assert_eq!(
        BinaryMathFunction::Pow.failure(),
        SideErrorReason::NonFiniteResult(FloatOperation::Pow)
    );
}

#[test]
fn scalar_operands_agree_with_their_broadcast_columns() {
    let column = Int64Array::from(vec![Some(7), None, Some(i64::MAX), Some(-4)]);
    let scalar = Int64Array::from(vec![Some(3)]);
    let broadcast = Int64Array::from(vec![Some(3); 4]);

    for operator in [
        Arithmetic::Add,
        Arithmetic::Sub,
        Arithmetic::Mul,
        Arithmetic::Div,
        Arithmetic::Rem,
    ] {
        let with_scalar =
            operator.evaluate_integers(Operand::Column(&column), Operand::Scalar(&scalar));
        let with_column =
            operator.evaluate_integers(Operand::Column(&column), Operand::Column(&broadcast));
        assert_eq!(with_scalar.column, with_column.column, "{operator:?}");
        assert_eq!(failed_lanes(&with_scalar), failed_lanes(&with_column));

        let scalar_left =
            operator.evaluate_integers(Operand::Scalar(&scalar), Operand::Column(&column));
        let column_left =
            operator.evaluate_integers(Operand::Column(&broadcast), Operand::Column(&column));
        assert_eq!(scalar_left.column, column_left.column, "{operator:?}");
        assert_eq!(failed_lanes(&scalar_left), failed_lanes(&column_left));
    }

    for comparison in [
        Comparison::Eq,
        Comparison::NotEq,
        Comparison::Lt,
        Comparison::LtEq,
        Comparison::Gt,
        Comparison::GtEq,
    ] {
        assert_eq!(
            comparison.evaluate(Operand::Column(&column), Operand::Scalar(&scalar)),
            comparison.evaluate(Operand::Column(&column), Operand::Column(&broadcast)),
            "{comparison:?}"
        );
        assert_eq!(
            comparison.evaluate(Operand::Scalar(&scalar), Operand::Column(&column)),
            comparison.evaluate(Operand::Column(&broadcast), Operand::Column(&column)),
            "{comparison:?}"
        );
    }
}

#[test]
fn a_null_scalar_operand_nulls_every_lane_without_failing_one() {
    let column = Int64Array::from(vec![Some(7), Some(0), Some(i64::MAX)]);
    let null = Int64Array::from(vec![None]);

    let quotient =
        Arithmetic::Div.evaluate_integers(Operand::Column(&column), Operand::Scalar(&null));
    assert_eq!(quotient.column.null_count(), 3);
    assert!(failed_lanes(&quotient).is_empty());

    let product =
        Arithmetic::Mul.evaluate_integers(Operand::Scalar(&null), Operand::Column(&column));
    assert_eq!(product.column.len(), 3);
    assert_eq!(product.column.null_count(), 3);
    assert!(failed_lanes(&product).is_empty());

    let less = Comparison::Lt.evaluate(Operand::Column(&column), Operand::Scalar(&null));
    assert_eq!(less.len(), 3);
    assert_eq!(less.null_count(), 3);
}

#[test]
fn float_scalar_operands_keep_ieee_comparison_and_finite_checks() {
    let column = Float64Array::from(vec![Some(1.5), Some(f64::NAN), None, Some(-0.0)]);
    let scalar = Float64Array::from(vec![Some(0.0)]);

    let equal = Comparison::Eq.evaluate(Operand::Column(&column), Operand::Scalar(&scalar));
    assert_eq!(
        equal.iter().collect::<Vec<_>>(),
        [Some(false), Some(false), None, Some(true)]
    );

    let quotient =
        Arithmetic::Div.evaluate_floats(Operand::Column(&column), Operand::Scalar(&scalar));
    assert_eq!(failed_lanes(&quotient), [0, 1, 3]);
    assert!(quotient.column.is_null(2));
}
