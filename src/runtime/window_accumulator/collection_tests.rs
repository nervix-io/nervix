//! Tests of complete collected values through the branch-owned production window.
//!
//! Layer: harness.
//! - **Owns.** Ordering, stepped rows, shared demands, sliced nested columns and restored output.
//! - **Depends on.** The production window and its test fixture, Arrow arrays and declared types.
//! - **Must not know.** Placement, transport or checkpoint publication policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the branch-owned window")
)]

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, cast::AsArray as _,
};
use arrow_schema::{DataType, Field};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ParseAsType;
use nervix_primitives::sync::StdArc;

use crate::runtime::{
    WindowProcessorState,
    test_fixtures::{OptionalTestField, TestWindow},
};

fn required(name: &'static str, ty: ParseAsType) -> OptionalTestField {
    OptionalTestField {
        name,
        ty,
        optional: false,
    }
}

fn vector(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

#[nervix_primitives::test]
async fn collection_shares_first_last_arguments_and_orders_steps_and_restores() {
    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value), first_value = FIRST(input.value), last_value = \
         LAST(input.value)",
        &[required("value", ParseAsType::I64)],
        &[
            required("collected", vector(ParseAsType::I64)),
            required("first_value", ParseAsType::I64),
            required("last_value", ParseAsType::I64),
        ],
    );
    assert_eq!(window.plan.demands().len(), 1);
    assert_eq!(window.state.accumulators.len(), 1);
    let values = Int64Array::from(vec![-1, 30, 10, 20, -2]);
    let refused = window
        .admit(vec![StdArc::new(values.slice(1, 3))], &[3, 1, 2])
        .await;
    assert!(refused.is_empty(), "{refused:?}");
    let refused = window
        .admit(vec![StdArc::new(Int64Array::from(vec![40, 50]))], &[2, 4])
        .await;
    assert!(refused.is_empty(), "{refused:?}");
    let emitted = window.emitted().await;
    let collected = emitted.batch().column(0).as_list::<i32>().value(0);
    assert_eq!(
        collected
            .as_primitive::<arrow_array::types::Int64Type>()
            .values(),
        &[10, 20, 40, 30, 50]
    );
    assert_eq!(
        emitted
            .batch()
            .column(1)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        10
    );
    assert_eq!(
        emitted
            .batch()
            .column(2)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        50
    );

    window.step(2, 5);
    let emitted = window.emitted().await;
    let collected = emitted.batch().column(0).as_list::<i32>().value(0);
    assert_eq!(
        collected
            .as_primitive::<arrow_array::types::Int64Type>()
            .values(),
        &[20, 40, 50]
    );
    let snapshot = window
        .state
        .to_snapshot()
        .assured("the fixture holds valid evaluated arguments");
    window.state =
        WindowProcessorState::from_snapshot(&window.plan, &window.input_schema, &snapshot)
            .assured("the unchanged plan reopens its own snapshot");
    let restored = window.emitted().await;
    assert_eq!(restored.batch(), emitted.batch());
}

#[nervix_primitives::test]
async fn collection_preserves_sliced_nested_array_values() {
    let element = ParseAsType::Array {
        element: Box::new(ParseAsType::I64),
        len: std::num::NonZeroU32::new(2).assured("two elements are nonzero"),
    };
    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value)",
        &[required("value", element.clone())],
        &[required("collected", vector(element))],
    );
    let values: ArrayRef = StdArc::new(
        FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![
                -9, -8, 10, 11, 20, 21, 30, 31, -7, -6,
            ])),
            None,
        )
        .assured("five complete required pairs form a fixed-size list"),
    );
    let selected = values.slice(1, 3);
    let refused = window.admit(vec![selected.clone()], &[1, 2, 3]).await;
    assert!(refused.is_empty(), "{refused:?}");
    let emitted = window.emitted().await;
    let collected = emitted.batch().column(0).as_list::<i32>();
    assert_eq!(collected.null_count(), 0);
    assert_eq!(collected.value(0).as_ref(), selected.as_ref());
}

#[nervix_primitives::test]
async fn collection_keeps_every_float_bit_including_non_finite_values() {
    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value)",
        &[required("value", ParseAsType::F32)],
        &[required("collected", vector(ParseAsType::F32))],
    );
    let bits = [0x8000_0000, 0x7fc0_0123, 0xff80_0000, 0x7f80_0000];
    let values = Float32Array::from_iter_values(bits.map(f32::from_bits));
    let refused = window.admit(vec![StdArc::new(values)], &[0, 1, 2, 3]).await;
    assert!(refused.is_empty(), "{refused:?}");
    let emitted = window.emitted().await;
    let collected = emitted.batch().column(0).as_list::<i32>().value(0);
    let actual = collected
        .as_primitive::<arrow_array::types::Float32Type>()
        .values()
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    assert_eq!(actual, bits);
}

#[nervix_primitives::test]
async fn collection_preserves_empty_vectors_and_sliced_string_offsets() {
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;

    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value)",
        &[required("value", vector(ParseAsType::String))],
        &[required("collected", vector(vector(ParseAsType::String)))],
    );
    let strings: ArrayRef = StdArc::new(arrow_array::StringArray::from(vec![
        "padding", "é", "x", "tail",
    ]));
    let values = ListArray::try_new(
        StdArc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::new(vec![0, 1, 1, 3, 4].into()),
        strings,
        None,
    )
    .assured("the required strings form four vectors");
    let refused = window
        .admit(vec![StdArc::new(values.slice(1, 2))], &[2, 1])
        .await;
    assert!(refused.is_empty(), "{refused:?}");
    let emitted = window.emitted().await;
    let collected = emitted.batch().column(0).as_list::<i32>().value(0);
    let collected = collected.as_list::<i32>();
    assert_eq!(collected.value_offsets(), &[0, 2, 2]);
    assert_eq!(
        collected
            .values()
            .as_string::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("é"), Some("x")]
    );
}

#[nervix_primitives::test]
async fn collection_refuses_decoded_and_encoded_outputs_before_publication() {
    use nervix_execution::{ExecutionConfig, Executor, OperationLimits};
    use ubyte::ByteUnit;

    use crate::runtime::{WindowProcessorError, window_processor::evaluate_window_aggregate};

    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value)",
        &[required("value", ParseAsType::I64)],
        &[required("collected", vector(ParseAsType::I64))],
    );
    let refused = window
        .admit(
            vec![StdArc::new(Int64Array::from_iter_values(0..32))],
            &(0..32).collect::<Vec<_>>(),
        )
        .await;
    assert!(refused.is_empty(), "{refused:?}");
    for decoded in [true, false] {
        let mut limits = OperationLimits::default();
        if decoded {
            limits.relay_decoded_bytes = ByteUnit::Byte(64);
        } else {
            limits.relay_encoded_bytes = ByteUnit::Byte(64);
        }
        let executor = Executor::new(ExecutionConfig {
            limits,
            ..ExecutionConfig::default()
        })
        .assured("the smaller operation bounds fit the default budgets");
        let error = evaluate_window_aggregate(
            &executor,
            &window.compiled,
            &window.state,
            &window.output_schema,
            nervix_models::Timestamp::from_unix_nanos(42),
        )
        .await
        .expect_err("the complete collection exceeds the selected carriage bound");
        if decoded {
            assert!(
                matches!(
                    error.current_context(),
                    WindowProcessorError::CollectionOutputLimit { .. }
                ),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(
                    error.current_context(),
                    WindowProcessorError::CollectionOutput
                ),
                "{error:?}"
            );
            let cause = error
                .downcast_ref::<crate::runtime_schema::ArrowBodyError>()
                .verified("the typed IPC encoding cause survives window qualification");
            assert!(
                matches!(cause, crate::runtime_schema::ArrowBodyError::Encode { .. }),
                "{error:?}"
            );
        }
        assert_eq!(
            window.state.entries.len(),
            32,
            "route evaluation leaves failure handling to its window owner"
        );
    }
}

#[nervix_primitives::test]
async fn collection_empty_vectors_reserve_no_absent_fixed_width_children() {
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;

    let width = 1_000_000;
    let pair_type = ParseAsType::Array {
        element: Box::new(ParseAsType::I64),
        len: std::num::NonZeroU32::new(width).verified("the fixed width is positive"),
    };
    let mut window = TestWindow::new(
        "SET collected = COLLECT_VEC(input.value)",
        &[required("value", vector(pair_type.clone()))],
        &[required("collected", vector(vector(pair_type)))],
    );
    let fixed_type = DataType::FixedSizeList(
        StdArc::new(Field::new("item", DataType::Int64, false)),
        i32::try_from(width).assured("the fixture width fits an Arrow type"),
    );
    let empty = ListArray::try_new(
        StdArc::new(Field::new("item", fixed_type.clone(), false)),
        OffsetBuffer::new(vec![0, 0].into()),
        arrow_array::new_empty_array(&fixed_type),
        None,
    )
    .assured("the vector holds no fixed-width elements");
    let refused = window.admit(vec![StdArc::new(empty)], &[0]).await;
    assert!(refused.is_empty(), "{refused:?}");
    let emitted = window.emitted().await;
    let values = emitted.batch().column(0).as_list::<i32>().value(0);
    let absent = values
        .as_list::<i32>()
        .values()
        .as_fixed_size_list()
        .values();
    assert_eq!(absent.len(), 0);
    assert_eq!(
        absent.get_buffer_memory_size(),
        0,
        "an empty vector must allocate no absent fixed-width payload"
    );
}
