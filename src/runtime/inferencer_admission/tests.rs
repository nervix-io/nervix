//! Owner evidence for admission-time inference through a concrete branch's processor node.
//!
//! Layer: test harness.
//! Owns: one model run per admitted message whatever the routes, the worker class it runs on, and
//! the delivery of that one result to routes with different flush policies.
//! Depends on: the shared inferencer branch fixture and the generated ONNX fixtures.
//! Must not know: schedules, interconnect or control-plane placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "drives one inferencer branch through its production owner"
    )
)]

use std::{num::NonZeroU32, time::Duration};

use meticulous::OptionExt as _;
use nervix_models::{FlushPolicy, InferencerTensorDimension, ParseAsType};
use nervix_primitives::{sync::Arc, time::timeout};

use crate::{
    runtime::{
        RelayRecordBatch,
        inferencer_benchmark::{InferencerBranchFixture, InferencerFixtureModel},
    },
    runtime_ack::{AckOutcome, AckRootTracker, AckSet},
    runtime_schema::RuntimeValue,
};

fn fixed(size: u32) -> InferencerTensorDimension {
    InferencerTensorDimension::Fixed(NonZeroU32::new(size).verified("test axes are positive"))
}

fn pair() -> ParseAsType {
    ParseAsType::Array {
        element: Box::new(ParseAsType::F32),
        len: NonZeroU32::new(2).verified("a pair has two elements"),
    }
}

/// The score a delivered one-message batch carries.
fn delivered_score(batch: &RelayRecordBatch) -> f32 {
    let Some(RuntimeValue::Array(values)) = batch
        .batch
        .value(0, "score")
        .expect("a delivered batch carries the score field")
    else {
        panic!("the score is a fixed array");
    };
    let [RuntimeValue::F32(score)] = values.as_slice() else {
        panic!("the score holds one F32");
    };
    score.into_inner()
}

#[nervix_primitives::test]
async fn one_model_run_on_the_extension_workers_serves_routes_with_different_flush_policies() {
    let mut fixture = InferencerBranchFixture::new(InferencerFixtureModel {
        file: "simple_score.onnx",
        input_fields: vec![("features", pair())],
        inputs: vec![("features", vec![fixed(2)])],
        state: Vec::new(),
        outputs: vec![("score", vec![fixed(1)])],
        routes: vec![
            ("scored_now", FlushPolicy::Immediate),
            (
                "scored_by_size",
                FlushPolicy::Each {
                    interval: "1h".to_owned(),
                    max_batch_size: "1B".to_owned(),
                },
            ),
        ],
    })
    .await;
    let tracker = Arc::new(AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker);
    let mut acks = Some(acks);
    let batch = fixture.pairs_batch(vec![("features", vec![[1.0, 0.0]])], || {
        acks.take().verified("the batch holds one message")
    });

    let before = fixture.runtime.executor().snapshot();
    fixture.admit(batch).await;
    let after = fixture.runtime.executor().snapshot();

    let extension_runs = after.extension_cpu.completed - before.extension_cpu.completed;
    let data_runs = after.data_cpu.completed - before.data_cpu.completed;
    assert_eq!(
        extension_runs + data_runs,
        1,
        "one admitted message runs the model once, whatever its routes"
    );
    assert_eq!(extension_runs, 1, "the model runs on the extension workers");
    for downstream in &mut fixture.downstream {
        let delivered = timeout(Duration::from_secs(5), downstream.recv())
            .await
            .expect("every route delivers its result")
            .assured("the fixture relay stays open");
        assert_eq!(delivered.message_count(), 1);
        assert_eq!(delivered_score(&delivered), 0.875);
        delivered.ack_success();
    }
    assert_eq!(
        timeout(Duration::from_secs(5), completion.wait())
            .await
            .expect("the message completes once both routes delivered it"),
        AckOutcome::Ack
    );
    fixture.stop().await;
}

/// A route failure's error record carries the admitted message its generated row came from, not
/// the generated tensors.
#[nervix_primitives::test]
async fn a_route_failure_reports_the_admitted_input_message() {
    use crate::runtime::processor_output::ProcessorOutputErrorSource as _;

    let fixture = InferencerBranchFixture::new(InferencerFixtureModel {
        file: "simple_score.onnx",
        input_fields: vec![("features", pair())],
        inputs: vec![("features", vec![fixed(2)])],
        state: Vec::new(),
        outputs: vec![("score", vec![fixed(1)])],
        routes: vec![("scored", FlushPolicy::Immediate)],
    })
    .await;
    let admitted = fixture.pairs_batch(
        vec![("features", vec![[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]])],
        AckSet::empty,
    );
    // The model accepted the first and third admitted messages; the second failed alone.
    let accepted = [0, 2];
    let source = crate::runtime::inferencer_admission::AdmittedInputRows::new(&admitted, &accepted);
    let generated = fixture.pairs_batch(
        vec![("features", vec![[9.0, 9.0], [9.0, 9.0]])],
        AckSet::empty,
    );
    let record = source
        .error_input(&generated, 1)
        .expect("the second generated row came from an admitted message");
    assert_eq!(
        record
            .value("features")
            .expect("the admitted message's field"),
        Some(RuntimeValue::Array(vec![
            RuntimeValue::F32(ordered_float::OrderedFloat(3.0)),
            RuntimeValue::F32(ordered_float::OrderedFloat(30.0)),
        ]))
    );
    assert!(source.error_input(&generated, 2).is_err());
    fixture.stop().await;
}

/// The scores of the next `count` messages the fixture's first route delivers, in order.
async fn delivered_scores(fixture: &mut InferencerBranchFixture, count: usize) -> Vec<f32> {
    let mut scores = Vec::with_capacity(count);
    while scores.len() < count {
        let delivered = timeout(Duration::from_secs(5), fixture.downstream[0].recv())
            .await
            .expect("the route delivers its result")
            .assured("the fixture relay stays open");
        for row in 0..delivered.batch.batch().num_rows() {
            let Some(RuntimeValue::Array(values)) = delivered
                .batch
                .value(row, "score")
                .expect("a delivered batch carries the score field")
            else {
                panic!("the score is a fixed array");
            };
            let [RuntimeValue::F32(score)] = values.as_slice() else {
                panic!("the score holds one F32");
            };
            scores.push(score.into_inner());
        }
        delivered.ack_success();
    }
    scores
}

/// `STATE` is a variation of one message at a time: the branch carries its tensors from message to
/// message in arrival order, within an admitted batch and across admitted batches.
#[nervix_primitives::test]
async fn a_stateful_branch_carries_its_tensors_from_message_to_message() {
    let mut fixture = InferencerBranchFixture::new(InferencerFixtureModel {
        file: "running_sum.onnx",
        input_fields: vec![("features", pair())],
        inputs: vec![("features", vec![fixed(2)])],
        state: vec![("state_in", "state_out", vec![fixed(2)])],
        outputs: vec![("score", vec![fixed(1)])],
        routes: vec![("scored", FlushPolicy::Immediate)],
    })
    .await;
    let first = fixture.pairs_batch(
        vec![("features", vec![[1.0, 10.0], [2.0, 20.0]])],
        AckSet::empty,
    );
    fixture.admit(first).await;
    assert_eq!(delivered_scores(&mut fixture, 2).await, [11.0, 33.0]);
    let second = fixture.pairs_batch(vec![("features", vec![[3.0, 30.0]])], AckSet::empty);
    fixture.admit(second).await;
    assert_eq!(delivered_scores(&mut fixture, 1).await, [66.0]);
    fixture.stop().await;
}
