//! Processor input dispatch failures.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for accepted processor input that cannot reach a branch task and the
//!   internal error the processor reports for it.
//! - **Depends on.** The production processor dispatch path and the runtime's test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

use nervix_models::{ModelKind, ModelName, NodeRef, ParseAsType, RelayName};
use nervix_primitives::sync::mpsc;

use super::*;
use crate::{
    runtime::RuntimeEvent,
    runtime_ack::{AckOutcome, AckSet},
    runtime_schema::{RuntimeValue, test_runtime_row},
};

/// Dispatches one batch to the `route_orders` junction through `instances`, reading its domain
/// time from `domain_clock`, and returns what the processor reported and the batch's outcome.
async fn report_of_dispatched_input(
    runtime: &Runtime,
    domain: &DomainName,
    domain_clock: &DomainClock,
    instances: &mut BranchInstanceRegistry<Option<BranchKey>, ProcessorBranchTask>,
) -> (String, AckOutcome) {
    let processor = named::<ModelName>("route_orders");
    let input_relay = named::<RelayName>("orders");
    let template = junction_branch_template(processor.as_str(), input_relay.as_str());
    let counters =
        runtime.node_quiesce_counters(domain, NodeRef::new(ModelKind::Junction, &processor));
    let mut events = runtime.subscribe_events();
    let (acks, completion) = AckSet::root();
    let batch = RelayRecordBatch::single(
        test_schema(&[("value", ParseAsType::I64)]),
        None,
        test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]),
        acks,
    )
    .expect("one test row forms a relay batch");

    dispatch_processor_node_input(
        ProcessorNodeDispatchContext {
            runtime_handle: runtime,
            domain,
            template: &template,
            domain_clock,
        },
        instances,
        input_relay,
        batch,
        NodeQuiesceWorkGuard::begin(counters),
    )
    .await;

    let RuntimeEvent::Error(message) = events
        .recv()
        .await
        .expect("the processor reports the failure to the node's observers");
    (message, completion.wait().await)
}

#[nervix_primitives::test]
async fn processor_input_accepted_after_its_clock_stopped_fails_as_an_internal_error() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the fixture installs a running unpaced clock");
    runtime
        .domain_clock_lifecycle(&domain)
        .expect("the fixture installs the domain's clock")
        .stop(0);
    let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();

    let (message, outcome) =
        report_of_dispatched_input(&runtime, &domain, &domain_clock, &mut instances).await;

    let expected = "junction 'route_orders' internal error in domain 'default': could not read \
                    the domain time of accepted input (unbranched): domain 'default' clock \
                    generation 0 is stopped";
    assert_eq!(message, expected);
    assert_eq!(outcome, AckOutcome::NoAck(expected.to_string()));
}

#[nervix_primitives::test]
async fn processor_input_for_a_branch_task_that_stopped_fails_as_an_internal_error() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let domain_clock = runtime
        .bind_domain_clock(&domain)
        .expect("the fixture installs a running unpaced clock");
    let (input_tx, input_rx) = mpsc::channel(1);
    drop(input_rx);
    let (commands, _command_rx) = mpsc::channel(1);
    let task = nervix_primitives::task::spawn(std::future::pending::<()>());
    let mut instances = BranchInstanceRegistry::<Option<BranchKey>, ProcessorBranchTask>::new();
    instances.insert_restored(
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
        1,
        ProcessorBranchTask {
            input: input_tx,
            commands,
            task: nervix_primitives::sync::blocking::Mutex::new(Some(AbortOnDropHandle::new(task))),
        },
    );

    let (message, outcome) =
        report_of_dispatched_input(&runtime, &domain, &domain_clock, &mut instances).await;

    let expected = "junction 'route_orders' internal error in domain 'default': the processor \
                    task is unavailable (unbranched)";
    assert_eq!(message, expected);
    assert_eq!(outcome, AckOutcome::NoAck(expected.to_string()));
}
