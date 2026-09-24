use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt8Array,
};

use super::{Elements, Members, MembershipSet, SMALL_SET_CAPACITY};
use crate::{batch::TypedArray, ir::RegisterType};

fn int64_elements(values: &[i64]) -> Vec<TypedArray> {
    values
        .iter()
        .map(|value| TypedArray::Int64(Int64Array::from_value(*value, 1)))
        .collect()
}

fn float64_elements(values: &[f64]) -> Vec<TypedArray> {
    values
        .iter()
        .map(|value| TypedArray::Float64(Float64Array::from_value(*value, 1)))
        .collect()
}

fn string_elements(values: &[&str]) -> Vec<TypedArray> {
    values
        .iter()
        .map(|value| TypedArray::Utf8(StringArray::from_iter_values([*value])))
        .collect()
}

fn membership(set: &MembershipSet, operand: &TypedArray) -> Vec<Option<bool>> {
    set.evaluate(operand)
        .expect("the operand has the set's type")
        .iter()
        .collect()
}

#[test]
fn small_sets_compare_each_element_and_large_sets_look_values_up() {
    let few: Vec<i64> = (0..8).collect();
    let small = MembershipSet::prepare(RegisterType::Int64, &int64_elements(&few))
        .expect("every element is an I64");
    assert!(matches!(
        small.elements.as_ref(),
        Elements::Int64(Members::Few(members)) if members.len() == SMALL_SET_CAPACITY
    ));

    let many: Vec<i64> = (0..40).map(|value| value * 3).collect();
    let large = MembershipSet::prepare(RegisterType::Int64, &int64_elements(&many))
        .expect("every element is an I64");
    assert!(matches!(
        large.elements.as_ref(),
        Elements::Int64(Members::Keyed(members)) if members.len() == 40
    ));

    let operand = TypedArray::Int64(Int64Array::from(vec![Some(7), Some(9), None, Some(117)]));
    assert_eq!(
        membership(&small, &operand),
        [Some(true), Some(false), None, Some(false)]
    );
    assert_eq!(
        membership(&large, &operand),
        [Some(false), Some(true), None, Some(true)]
    );
}

#[test]
fn duplicate_elements_are_one_member_and_text_is_always_keyed() {
    let set = MembershipSet::prepare(
        RegisterType::Utf8,
        &string_elements(&["open", "held", "open"]),
    )
    .expect("every element is a STRING");
    assert!(matches!(
        set.elements.as_ref(),
        Elements::Utf8(Members::Keyed(members)) if members.len() == 2
    ));
    let operand = TypedArray::Utf8(StringArray::from(vec![
        Some("open"),
        Some("closed"),
        None,
        Some("held"),
    ]));
    assert_eq!(
        membership(&set, &operand),
        [Some(true), Some(false), None, Some(true)]
    );
}

#[test]
fn no_value_is_a_member_of_an_empty_set_not_even_a_null_one() {
    let set = MembershipSet::prepare(RegisterType::Utf8, &[]).expect("an empty set has no type");
    let operand = TypedArray::Utf8(StringArray::from(vec![Some("open"), None]));
    let result = set
        .evaluate(&operand)
        .expect("an empty set accepts every operand");
    assert_eq!(result.null_count(), 0);
    assert_eq!(
        result.iter().collect::<Vec<_>>(),
        [Some(false), Some(false)]
    );
}

#[test]
fn floats_are_members_under_the_equality_of_the_comparison_operator() {
    let set = MembershipSet::prepare(
        RegisterType::Float64,
        &float64_elements(&[0.0, 1.5, f64::NAN]),
    )
    .expect("every element is an F64");
    let operand = TypedArray::Float64(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(1.5),
        Some(f64::NAN),
        Some(-1.5),
        None,
    ]));
    assert_eq!(
        membership(&set, &operand),
        [
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            None
        ]
    );

    let negative_zero = MembershipSet::prepare(
        RegisterType::Float32,
        &[TypedArray::Float32(Float32Array::from_value(-0.0, 1))],
    )
    .expect("the element is an F32");
    let operand = TypedArray::Float32(Float32Array::from(vec![0.0_f32, -0.0, f32::NAN]));
    assert_eq!(
        membership(&negative_zero, &operand),
        [Some(true), Some(true), Some(false)]
    );
}

#[test]
fn a_set_of_only_nan_holds_no_value_but_keeps_null_operands_null() {
    let set = MembershipSet::prepare(RegisterType::Float64, &float64_elements(&[f64::NAN]))
        .expect("the element is an F64");
    let operand = TypedArray::Float64(Float64Array::from(vec![Some(f64::NAN), None]));
    assert_eq!(membership(&set, &operand), [Some(false), None]);
}

#[test]
fn every_scalar_type_prepares_and_tests_its_own_values() {
    let narrow = MembershipSet::prepare(
        RegisterType::UInt8,
        &[TypedArray::UInt8(UInt8Array::from_value(255, 1))],
    )
    .expect("the element is a U8");
    assert_eq!(
        membership(
            &narrow,
            &TypedArray::UInt8(UInt8Array::from(vec![255, 254]))
        ),
        [Some(true), Some(false)]
    );

    let flags = MembershipSet::prepare(
        RegisterType::Boolean,
        &[TypedArray::Boolean(BooleanArray::from(vec![true]))],
    )
    .expect("the element is a BOOL");
    assert_eq!(
        membership(
            &flags,
            &TypedArray::Boolean(BooleanArray::from(vec![Some(false), Some(true), None]))
        ),
        [Some(false), Some(true), None]
    );

    let instants = MembershipSet::prepare(
        RegisterType::Datetime,
        &[TypedArray::Datetime(
            TimestampNanosecondArray::from_value(5, 1).with_timezone_utc(),
        )],
    )
    .expect("the element is a DATETIME");
    assert_eq!(
        membership(
            &instants,
            &TypedArray::Datetime(TimestampNanosecondArray::from(vec![5, 6]).with_timezone_utc())
        ),
        [Some(true), Some(false)]
    );
}

#[test]
fn elements_of_another_type_or_null_do_not_prepare_a_set() {
    assert!(MembershipSet::prepare(RegisterType::Int32, &int64_elements(&[1])).is_none());
    assert!(
        MembershipSet::prepare(
            RegisterType::Int32,
            &[TypedArray::Int32(Int32Array::from(vec![None]))]
        )
        .is_none()
    );
    let set = MembershipSet::prepare(RegisterType::Int64, &int64_elements(&[1]))
        .expect("the element is an I64");
    assert!(
        set.evaluate(&TypedArray::Int32(Int32Array::from(vec![1])))
            .is_none()
    );
}
