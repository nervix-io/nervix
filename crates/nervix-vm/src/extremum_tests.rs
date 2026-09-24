use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, StringArray,
    TimestampNanosecondArray, UInt8Array,
};

use super::{Extremum, clamp};
use crate::{batch::TypedArray, operand::Operand};

fn floats(values: &ArrayRef) -> Vec<Option<f64>> {
    values
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float arguments select a float column")
        .iter()
        .collect()
}

fn float_bits(values: &ArrayRef) -> Vec<Option<u64>> {
    floats(values)
        .into_iter()
        .map(|value| value.map(f64::to_bits))
        .collect()
}

#[test]
fn extrema_skip_null_arguments_and_are_null_only_where_every_argument_is() {
    let first = TypedArray::Float64(Float64Array::from(vec![Some(3.5), None, None, Some(1.0)]));
    let second = Float64Array::from(vec![None, Some(2.0), None, Some(4.0)]);
    let third = Float64Array::from(vec![Some(-2.0), Some(9.0), None, None]);
    let rest = [
        Operand::Column(&second).erased(),
        Operand::Column(&third).erased(),
    ];

    let greatest = Extremum::Greatest
        .select(&first, &rest)
        .expect("float arguments select");
    assert_eq!(floats(&greatest), [Some(3.5), Some(9.0), None, Some(4.0)]);
    let least = Extremum::Least
        .select(&first, &rest)
        .expect("float arguments select");
    assert_eq!(floats(&least), [Some(-2.0), Some(2.0), None, Some(1.0)]);
}

#[test]
fn nan_is_above_every_float_and_equal_zeros_keep_the_earliest_argument() {
    let first = TypedArray::Float64(Float64Array::from(vec![1.0, f64::NAN, -0.0, 0.0]));
    let second = Float64Array::from(vec![f64::NAN, 5.0, 0.0, -0.0]);
    let rest = [Operand::Column(&second).erased()];

    let greatest = Extremum::Greatest
        .select(&first, &rest)
        .expect("float arguments select");
    let greatest = floats(&greatest);
    assert!(greatest[0].is_some_and(f64::is_nan));
    assert!(greatest[1].is_some_and(f64::is_nan));
    assert_eq!(
        float_bits(
            &Extremum::Greatest
                .select(&first, &rest)
                .expect("float arguments select")
        )[2..],
        [Some((-0.0_f64).to_bits()), Some(0.0_f64.to_bits())]
    );

    let least = Extremum::Least
        .select(&first, &rest)
        .expect("float arguments select");
    assert_eq!(
        float_bits(&least),
        [
            Some(1.0_f64.to_bits()),
            Some(5.0_f64.to_bits()),
            Some((-0.0_f64).to_bits()),
            Some(0.0_f64.to_bits())
        ]
    );

    let narrow = TypedArray::Float32(Float32Array::from(vec![f32::NAN, 2.0]));
    let other = Float32Array::from(vec![1.0_f32, f32::NAN]);
    let selected = Extremum::Least
        .select(&narrow, &[Operand::Column(&other).erased()])
        .expect("float arguments select");
    let selected = selected
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("F32 arguments select an F32 column");
    assert_eq!(selected.values().as_ref(), &[1.0, 2.0]);
}

#[test]
fn extrema_order_integers_strings_booleans_and_datetimes_naturally() {
    let integers = Extremum::Greatest
        .select(
            &TypedArray::Int32(Int32Array::from(vec![-5, 7])),
            &[Operand::Scalar(&Int32Array::from(vec![0])).erased()],
        )
        .expect("integer arguments select");
    assert_eq!(
        integers
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("I32 arguments select an I32 column")
            .values()
            .as_ref(),
        &[0, 7]
    );

    let labels = Extremum::Least
        .select(
            &TypedArray::Utf8(StringArray::from(vec![Some("beta"), None, Some("Zeta")])),
            &[Operand::Column(&StringArray::from(vec![
                Some("alpha"),
                Some("x"),
                Some("a"),
            ]))
            .erased()],
        )
        .expect("text arguments select");
    let labels = labels
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("STRING arguments select a STRING column");
    assert_eq!(
        labels.iter().collect::<Vec<_>>(),
        [Some("alpha"), Some("x"), Some("Zeta")]
    );

    let flags = Extremum::Greatest
        .select(
            &TypedArray::Boolean(BooleanArray::from(vec![None, Some(true), Some(false)])),
            &[Operand::Scalar(&BooleanArray::from(vec![false])).erased()],
        )
        .expect("flag arguments select");
    let flags = flags
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("BOOL arguments select a BOOL column");
    assert_eq!(
        flags.iter().collect::<Vec<_>>(),
        [Some(false), Some(true), Some(false)]
    );

    let instants = Extremum::Greatest
        .select(
            &TypedArray::Datetime(TimestampNanosecondArray::from(vec![5, 1]).with_timezone_utc()),
            &[
                Operand::Column(&TimestampNanosecondArray::from(vec![3, 9]).with_timezone_utc())
                    .erased(),
            ],
        )
        .expect("datetime arguments select");
    assert_eq!(
        instants
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("DATETIME arguments select a DATETIME column")
            .values()
            .as_ref(),
        &[5, 9]
    );
}

#[test]
fn clamp_bounds_values_and_reports_rows_with_invalid_bounds() {
    let value = TypedArray::Float64(Float64Array::from(vec![
        Some(-2.0),
        Some(15.0),
        Some(5.0),
        Some(f64::NAN),
        Some(-0.0),
        None,
        Some(5.0),
        Some(5.0),
    ]));
    let low = Float64Array::from(vec![
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(10.0),
        Some(f64::NAN),
    ]);
    let high = Float64Array::from(vec![
        Some(10.0),
        Some(10.0),
        Some(10.0),
        Some(10.0),
        Some(10.0),
        Some(10.0),
        Some(0.0),
        Some(10.0),
    ]);
    let clamped = clamp(
        &value,
        Operand::Column(&low).erased(),
        Operand::Column(&high).erased(),
    )
    .expect("float arguments clamp");

    let values = floats(&clamped.values);
    assert_eq!(values[..3], [Some(0.0), Some(10.0), Some(5.0)]);
    assert!(values[3].is_some_and(f64::is_nan));
    assert_eq!(values[4].map(f64::to_bits), Some((-0.0_f64).to_bits()));
    assert_eq!(values[5..], [None, None, None]);
    assert_eq!(
        clamped.lower_above_upper.set_indices().collect::<Vec<_>>(),
        [6]
    );
    assert_eq!(clamped.nan_bound.set_indices().collect::<Vec<_>>(), [7]);
}

#[test]
fn clamp_reports_invalid_bounds_only_where_every_argument_is_present() {
    let value = TypedArray::UInt8(UInt8Array::from(vec![None, Some(1), Some(200)]));
    let low = UInt8Array::from(vec![9]);
    let high = UInt8Array::from(vec![3]);
    let clamped = clamp(
        &value,
        Operand::Scalar(&low).erased(),
        Operand::Scalar(&high).erased(),
    )
    .expect("integer arguments clamp");
    assert_eq!(clamped.values.null_count(), 3);
    assert_eq!(
        clamped.lower_above_upper.set_indices().collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(clamped.nan_bound.count_set_bits(), 0);

    let text = TypedArray::Utf8(StringArray::from(vec!["apple", "mango", "zebra"]));
    let low: ArrayRef = Arc::new(StringArray::from(vec!["banana"]));
    let high: ArrayRef = Arc::new(StringArray::from(vec!["peach"]));
    let clamped = clamp(
        &text,
        Operand::Scalar(low.as_ref()),
        Operand::Scalar(high.as_ref()),
    )
    .expect("text arguments clamp");
    let clamped = clamped
        .values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("STRING arguments clamp to a STRING column")
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(clamped, [Some("banana"), Some("mango"), Some("peach")]);
}
