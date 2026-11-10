//! Fixed collection through the retained-window production owner.
//!
//! Layer: harness.
//! - **Owns.** Exact counts, shared arguments, nested column fidelity and restored emissions.
//! - **Depends on.** Production compilation, retention and collection with explicit count widths.
//! - **Must not know.** Placement, transport or checkpoint publication scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "fixed window collection owner tests")
)]

use std::num::{NonZeroU32, NonZeroU64};

use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int64Array, ListArray,
    cast::AsArray as _,
    types::{Float32Type, Int64Type},
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::Executor;
use nervix_models::{ParseAsType, Timestamp};
use nervix_primitives::sync::StdArc;

use crate::runtime::{
    WindowProcessorState,
    test_fixtures::{OptionalTestField, TestWindow},
    window_processor::{WindowProcessorError, evaluate_window_aggregate},
};

fn required(name: &'static str, ty: ParseAsType) -> OptionalTestField {
    OptionalTestField {
        name,
        ty,
        optional: false,
    }
}

fn fixed(element: ParseAsType, len: u32) -> ParseAsType {
    ParseAsType::Array {
        element: Box::new(element),
        len: NonZeroU32::new(len).assured("these fixtures declare positive dimensions"),
    }
}

fn vector(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

#[nervix_primitives::test]
async fn fixed_collection_shares_arguments_and_restores_ordered_sliced_batches() {
    let mut window = TestWindow::with_message_width(
        "SET fixed = COLLECT_ARRAY(input.value), variable = COLLECT_VEC(input.value), head = \
         FIRST(input.value), tail = LAST(input.value)",
        &[required("value", ParseAsType::I64)],
        &[
            required("fixed", fixed(ParseAsType::I64, 3)),
            required("variable", vector(ParseAsType::I64)),
            required("head", ParseAsType::I64),
            required("tail", ParseAsType::I64),
        ],
        NonZeroU64::new(3),
    );
    assert_eq!(window.plan.demands().len(), 1);
    assert_eq!(window.state.accumulators.len(), 1);
    let first = Int64Array::from(vec![-1, 30, -2]);
    assert!(
        window
            .admit(vec![StdArc::new(first.slice(1, 1))], &[3])
            .await
            .is_empty()
    );
    assert!(
        window
            .admit(vec![StdArc::new(Int64Array::from(vec![10, 20]))], &[1, 2])
            .await
            .is_empty()
    );
    let before = window.emitted().await;
    let list = before.batch().column(0).as_fixed_size_list();
    assert_eq!(list.len(), 1);
    assert_eq!(list.value_length(), 3);
    assert_eq!(list.null_count(), 0);
    assert_eq!(
        list.value(0).as_primitive::<Int64Type>().values(),
        &[10, 20, 30]
    );
    assert_eq!(
        before
            .batch()
            .column(1)
            .as_list::<i32>()
            .value(0)
            .as_primitive::<Int64Type>()
            .values(),
        &[10, 20, 30]
    );
    assert_eq!(
        before
            .batch()
            .column(2)
            .as_primitive::<Int64Type>()
            .value(0),
        10
    );
    assert_eq!(
        before
            .batch()
            .column(3)
            .as_primitive::<Int64Type>()
            .value(0),
        30
    );

    let saved = window
        .state
        .to_snapshot()
        .assured("the retained argument schema is valid");
    window.state = WindowProcessorState::from_snapshot(&window.plan, &window.input_schema, &saved)
        .assured("the same count-window plan restores its retained columns");
    let restored = window.emitted().await;
    assert_eq!(before.schema(), restored.schema());
    for (before, restored) in before
        .batch()
        .columns()
        .iter()
        .zip(restored.batch().columns())
    {
        assert_eq!(before.to_data(), restored.to_data());
    }
    window.step(1, 4);
    assert!(
        window
            .admit(vec![StdArc::new(Int64Array::from(vec![40]))], &[4])
            .await
            .is_empty()
    );
    let stepped = window.emitted().await;
    assert_eq!(
        stepped
            .batch()
            .column(0)
            .as_fixed_size_list()
            .value(0)
            .as_primitive::<Int64Type>()
            .values(),
        &[10, 20, 40]
    );
}

#[nervix_primitives::test]
async fn fixed_collection_refuses_a_partial_or_overfilled_window_before_gathering() {
    for count in [2, 4] {
        nervix_primitives::task::consume_budget().await;
        let mut window = TestWindow::with_message_width(
            "SET values = COLLECT_ARRAY(input.value)",
            &[required("value", ParseAsType::I64)],
            &[required("values", fixed(ParseAsType::I64, 3))],
            NonZeroU64::new(3),
        );
        let values = vec![7; count];
        let timestamps = vec![1; count];
        assert!(
            window
                .admit(vec![StdArc::new(Int64Array::from(values))], &timestamps)
                .await
                .is_empty()
        );
        let error = evaluate_window_aggregate(
            &Executor::default(),
            &window.compiled,
            &window.state,
            &window.output_schema,
            Timestamp::from_unix_nanos(42),
        )
        .await
        .expect_err("a fixed array cannot hide a different retained count");
        let expected_rows = i32::try_from(count).assured("the fixture count is at most four");
        assert!(error.frames().any(|frame| {
            matches!(
                frame.downcast_ref::<WindowProcessorError>(),
                Some(WindowProcessorError::CollectionLength { expected: 3, rows })
                    if *rows == expected_rows
            )
        }));
    }
}

#[nervix_primitives::test]
async fn fixed_collection_keeps_nested_float_bits_from_a_sliced_argument() {
    let mut window = TestWindow::with_message_width(
        "SET values = COLLECT_ARRAY(input.value)",
        &[required("value", fixed(ParseAsType::F32, 2))],
        &[required("values", fixed(fixed(ParseAsType::F32, 2), 2))],
        NonZeroU64::new(2),
    );
    let bits = [
        0x7fc0_1234,
        (-0.0_f32).to_bits(),
        0.0_f32.to_bits(),
        f32::INFINITY.to_bits(),
    ];
    let values = Float32Array::from(vec![
        99.0,
        99.0,
        f32::from_bits(bits[0]),
        f32::from_bits(bits[1]),
        f32::from_bits(bits[2]),
        f32::from_bits(bits[3]),
        88.0,
        88.0,
    ]);
    let pairs = FixedSizeListArray::try_new(
        StdArc::new(Field::new("item", DataType::Float32, false)),
        2,
        StdArc::new(values),
        None,
    )
    .assured("the child column holds complete required pairs");
    assert!(
        window
            .admit(vec![StdArc::new(pairs.slice(1, 2))], &[2, 1])
            .await
            .is_empty()
    );
    let output = window.emitted().await;
    let nested = output.batch().column(0).as_fixed_size_list().value(0);
    let leaves = nested
        .as_fixed_size_list()
        .values()
        .as_primitive::<Float32Type>();
    let actual = leaves
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    assert_eq!(actual, [bits[2], bits[3], bits[0], bits[1]]);
}

#[nervix_primitives::test]
async fn fixed_collection_keeps_empty_and_nonempty_vectors_as_its_elements() {
    let mut window = TestWindow::with_message_width(
        "SET values = COLLECT_ARRAY(input.value)",
        &[required("value", vector(ParseAsType::I64))],
        &[required("values", fixed(vector(ParseAsType::I64), 3))],
        NonZeroU64::new(3),
    );
    let vectors = ListArray::try_new(
        StdArc::new(Field::new("item", DataType::Int64, false)),
        OffsetBuffer::new(vec![0, 2, 2, 5].into()),
        StdArc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        None,
    )
    .assured("the three vectors have valid offsets and required elements");
    assert!(
        window
            .admit(vec![StdArc::new(vectors)], &[1, 2, 3])
            .await
            .is_empty()
    );
    let output = window.emitted().await;
    let elements = output.batch().column(0).as_fixed_size_list().value(0);
    let vectors = elements.as_list::<i32>();
    assert_eq!(vectors.len(), 3);
    assert_eq!(vectors.null_count(), 0);
    assert_eq!(
        vectors.value(0).as_primitive::<Int64Type>().values(),
        &[1, 2]
    );
    assert_eq!(vectors.value(1).len(), 0);
    assert_eq!(
        vectors.value(2).as_primitive::<Int64Type>().values(),
        &[3, 4, 5]
    );
}
