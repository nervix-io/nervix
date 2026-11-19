//! The order of a domain's stop at its runtime owner.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that applying a stopping domain holds its intake while its execution
//!   and every task run on, and that only the committed ending signals the domain's tasks, once
//!   the teardown recorded what it cuts.
//! - **Depends on.** The production schedule application and the runtime's test fixtures.
//! - **Must not know.** The stop's owner, consensus or the interconnect.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_interconnect::domain_stop::StopReportAnswer;
use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, ClusterSchedule, CommandExecutionReference,
    CreateJunction, CreateRelay, CreateSchema, DomainSchedule, DomainStatus, DomainStop,
    DomainStopCause, DomainStopEnding, DomainStopping, OperatorStopRequest, ParseAsType,
    ProcessorInputs, ProcessorOutputs, RelayBranching, RelayName, SchemaField, SchemaName,
    StopEnding, StopRequestId, Timestamp,
};
use nonzero_ext::nonzero;

use super::*;

const DOMAIN: &str = "stopping";

fn state(status: DomainStatus, stop: Option<DomainStop>) -> DomainState {
    let mut state = unpaced_domain_state(DOMAIN);
    state.status = status;
    state.start_version = 1;
    state.stop = stop;
    state
}

fn relay(name: &str, schema: &SchemaName) -> nervix_models::ScheduledNode {
    scheduled_model(nervix_models::Model::Relay(CreateRelay {
        labels: Default::default(),
        name: named::<RelayName>(name),
        schema: schema.clone(),
        buffer: nonzero!(2usize),
        branching: RelayBranching::unbranched(),
        materialized_state: None,
    }))
}

/// A relay of events and a junction forwarding them to another, whose branch task the stop's
/// teardown ends.
fn schedule() -> ClusterSchedule {
    let schema = named::<SchemaName>("event");
    let mut routes = ProcessorOutputs::single(named("forwarded"));
    routes.routes[0].construction.inherit = Some(nervix_models::Inheritance::All);
    let routes = routes.with_flush_policy(nervix_models::FlushPolicy::Each {
        interval: "1h".to_string(),
        max_batch_size: "1MiB".to_string(),
    });
    ClusterSchedule::from_iter([DomainSchedule::new(
        domain(DOMAIN),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: schema.clone(),
                fields: vec![SchemaField {
                    name: named("value"),
                    ty: ParseAsType::I64,
                    optional: false,
                    sensitive: false,
                }],
            })),
            relay("events", &schema),
            relay("forwarded", &schema),
            scheduled_model(nervix_models::Model::Junction(CreateJunction {
                labels: Default::default(),
                name: named("forward"),
                from: ProcessorInputs::single(named("events")),
                output_routes: routes,
                branched_by: BranchSelection::unbranched(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
        ],
        Vec::new(),
    )])
}

/// Applies the committed revision, then lets the domain's tasks run until each waits, as a node's
/// running tasks do between two revisions.
async fn apply(runtime: &Runtime, revision: u64, state: DomainState) {
    runtime
        .apply_cluster_state(
            &ClusterNodeName::parse("node-1").expect("valid name"),
            revision,
            &BTreeMap::from([(domain(DOMAIN), state)]),
            &BTreeMap::new(),
            &schedule(),
        )
        .await
        .expect("the committed revision applies");
    for _ in 0..64 {
        nervix_primitives::task::yield_now().await;
    }
}

#[nervix_primitives::test]
async fn a_stopping_domain_runs_its_tasks_until_its_stop_ends() {
    let runtime = Runtime::default();
    let domain = domain(DOMAIN);
    apply(&runtime, 1, state(DomainStatus::Running, None)).await;
    let mut tasks = runtime
        .inner
        .executions
        .get(&domain)
        .expect("the running domain has an execution")
        .shutdown
        .subscribe();

    let reference = CommandExecutionReference::parse("operator-stop").expect("valid reference");
    let stopping = DomainStopping {
        began_at_revision: 2,
        operators: BTreeMap::from([(
            reference.clone(),
            OperatorStopRequest {
                requested_at: Timestamp::from_unix_nanos(1),
                limit: None,
            },
        )]),
        licensing: None,
    };
    apply(
        &runtime,
        2,
        state(DomainStatus::Stopping, Some(DomainStop::Stopping(stopping))),
    )
    .await;
    assert!(
        !*tasks.borrow_and_update(),
        "a stopping domain's tasks run on until its stop ends"
    );
    {
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("a stopping domain keeps its execution");
        assert!(!execution.passive_only);
        assert!(
            execution.domain_clock.holds_intake(),
            "a stopping domain holds its intake"
        );
    }
    assert_eq!(
        runtime.stop_report(&domain, 1),
        StopReportAnswer::Pending,
        "a node whose execution still runs has nothing to report yet"
    );

    let ending = DomainStopEnding {
        cause: DomainStopCause::Operator,
        ending: StopEnding::Completed,
        resolved: vec![StopRequestId::Operator(reference)],
        report: None,
    };
    let mut events = runtime.events().subscribe();
    apply(
        &runtime,
        3,
        state(DomainStatus::Stopped, Some(DomainStop::Ended(ending))),
    )
    .await;
    assert!(
        *tasks.borrow_and_update(),
        "the committed ending signals the domain's tasks"
    );
    // A drained branch holds no accepted output, so its end without a running clock loses
    // nothing and reports no error.
    let mut received = std::pin::pin!(events.recv());
    assert!(
        futures_util::poll!(received.as_mut()).is_pending(),
        "a graceful stop's teardown raised a runtime event"
    );
    assert!(
        runtime
            .inner
            .executions
            .get(&domain)
            .expect("a stopped domain keeps its passive execution")
            .passive_only
    );
    assert_eq!(
        runtime.stop_report(&domain, 1),
        StopReportAnswer::Reported(Vec::new()),
        "the teardown recorded what it cut, here nothing"
    );
}
