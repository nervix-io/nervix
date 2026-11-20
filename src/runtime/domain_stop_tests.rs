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

#[nervix_primitives::test]
async fn a_node_reports_what_its_stop_cut_only_once_its_teardown_ended() {
    let runtime = Runtime::default();
    let domain = domain(DOMAIN);
    apply(&runtime, 1, state(DomainStatus::Running, None)).await;
    let teardown = StopTeardown {
        start_version: 1,
        enforced: false,
    };

    runtime.begin_stop_ledger(&domain, teardown);
    assert_eq!(runtime.stop_report(&domain, 1), StopReportAnswer::Pending);
    runtime.record_stop_cuts(&domain, teardown);
    assert_eq!(
        runtime.stop_report(&domain, 1),
        StopReportAnswer::Pending,
        "the cuts are recorded, but the teardown still ends the tasks and writes the final \
         snapshot"
    );
    runtime.complete_stop_ledger(&domain, teardown);
    assert_eq!(
        runtime.stop_report(&domain, 1),
        StopReportAnswer::Reported(Vec::new())
    );
}

#[nervix_primitives::test]
async fn a_node_takes_no_drain_step_before_it_applied_the_stop() {
    use nervix_interconnect::domain_stop::StopDrainAction;

    let runtime = Runtime::default();
    let domain = domain(DOMAIN);
    apply(&runtime, 1, state(DomainStatus::Running, None)).await;
    let emitter = nervix_models::NodeRef::new(
        nervix_models::ModelKind::Emitter,
        named::<nervix_models::EmitterName>("forwarded"),
    );
    let counters = runtime.node_quiesce_counters(&domain, emitter);
    let mut flush = runtime.force_flush_participant(&domain, counters);

    // The stop's owner can ask before this node applied the stop. The node answers with its view
    // and releases nothing: an emitter that still sees the domain running would take the flush for
    // an ordinary drain's and wait out an unavailable destination.
    let early = runtime.stop_drain_status(&domain, StopDrainAction::Confirm);
    let early_generation = flush
        .pending_completion()
        .assured("the domain's flush coordinator stays open while the domain runs");
    assert!(
        early_generation.is_none(),
        "a node that has not applied the stop starts no force-flush generation"
    );
    // Its view is all it reports, so a node with no intake and nothing in motion does not hold the
    // drain open while its installation of the stop still waits.
    assert!(
        !early.moving(),
        "a node with no intake and no work is at rest before it applied the stop"
    );

    let stopping = DomainStopping {
        began_at_revision: 2,
        operators: BTreeMap::from([(
            CommandExecutionReference::parse("operator-stop").expect("valid reference"),
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
    // Whatever applying the stop asked of the participant is done before the owner's round.
    let applying = flush
        .pending_completion()
        .assured("the domain's flush coordinator stays open while the domain stops");
    if let Some(completion) = applying {
        completion.complete();
    }

    let applied = runtime.stop_drain_status(&domain, StopDrainAction::Confirm);
    let confirming = flush
        .pending_completion()
        .assured("the domain's flush coordinator stays open while the domain stops");
    assert!(
        confirming.is_some(),
        "once it applied the stop, the node starts the confirming generation its owner asks for"
    );
    assert!(!applied.moving(), "the node still holds nothing in motion");
}

#[test]
fn a_teardown_cuts_each_kind_of_work_by_how_its_stop_ended() {
    use nervix_interconnect::domain_stop::{StopWaitEntry, StopWaitKind};
    use nervix_models::{ModelKind, ModelName, NodeRef, StopCut, StopCutKind};

    /// One kind of entry a teardown's view can hold, and what the teardown cuts of it when its
    /// stop completed and when its stop was enforced.
    struct Case {
        kind: StopWaitKind,
        entity: ModelKind,
        completed: Option<StopCutKind>,
        enforced: Option<StopCutKind>,
    }
    fn case(
        kind: StopWaitKind,
        entity: ModelKind,
        completed: Option<StopCutKind>,
        enforced: Option<StopCutKind>,
    ) -> Case {
        Case {
            kind,
            entity,
            completed,
            enforced,
        }
    }
    let cases = [
        // Intake still admitting and generators still producing hold no work of their own.
        case(
            StopWaitKind::AdmittingIngestor,
            ModelKind::Ingestor,
            None,
            None,
        ),
        case(
            StopWaitKind::ActiveGenerator,
            ModelKind::Generator,
            None,
            None,
        ),
        // Work in motion is cut as what it is when it moved after a completed stop's drain rested,
        // and as enforced when the stop was forced.
        case(
            StopWaitKind::AdmittingRelay,
            ModelKind::Relay,
            Some(StopCutKind::RelayBatches),
            Some(StopCutKind::Enforced),
        ),
        case(
            StopWaitKind::RelayBatches,
            ModelKind::Relay,
            Some(StopCutKind::RelayBatches),
            Some(StopCutKind::Enforced),
        ),
        case(
            StopWaitKind::NodeWork,
            ModelKind::Junction,
            Some(StopCutKind::NodeBatches),
            Some(StopCutKind::Enforced),
        ),
        case(
            StopWaitKind::EmitterBuffer,
            ModelKind::Emitter,
            Some(StopCutKind::EmitterUndelivered),
            Some(StopCutKind::Enforced),
        ),
        // A publish still on the wire when the teardown reads it is cut by force either way.
        case(
            StopWaitKind::PublishOnTheWire,
            ModelKind::Emitter,
            Some(StopCutKind::Enforced),
            Some(StopCutKind::Enforced),
        ),
        // Held work keeps its own kind either way.
        case(
            StopWaitKind::RequiredWait,
            ModelKind::Junction,
            Some(StopCutKind::RequiredWaitMessages),
            Some(StopCutKind::RequiredWaitMessages),
        ),
        case(
            StopWaitKind::EmitterRetry,
            ModelKind::Emitter,
            Some(StopCutKind::EmitterUndelivered),
            Some(StopCutKind::EmitterUndelivered),
        ),
        case(
            StopWaitKind::RelayBatchesHeld,
            ModelKind::Relay,
            Some(StopCutKind::RelayBatches),
            Some(StopCutKind::RelayBatches),
        ),
        case(
            StopWaitKind::NodeWorkHeld,
            ModelKind::Junction,
            Some(StopCutKind::NodeBatches),
            Some(StopCutKind::NodeBatches),
        ),
        case(
            StopWaitKind::ProcessorHeld,
            ModelKind::Reorderer,
            Some(StopCutKind::ReordererRows),
            Some(StopCutKind::ReordererRows),
        ),
        case(
            StopWaitKind::ProcessorHeld,
            ModelKind::Correlator,
            Some(StopCutKind::CorrelatorRecords),
            Some(StopCutKind::CorrelatorRecords),
        ),
        case(
            StopWaitKind::ProcessorHeld,
            ModelKind::WindowProcessor,
            Some(StopCutKind::WindowAcknowledgements),
            Some(StopCutKind::WindowAcknowledgements),
        ),
        case(
            StopWaitKind::ProcessorHeld,
            ModelKind::Junction,
            Some(StopCutKind::NodeBatches),
            Some(StopCutKind::NodeBatches),
        ),
    ];
    let completed = StopTeardown {
        start_version: 1,
        enforced: false,
    };
    let enforced = StopTeardown {
        start_version: 1,
        enforced: true,
    };
    let entity = named::<ModelName>("held");

    for case in &cases {
        let entry = StopWaitEntry {
            entity: Some(NodeRef::new(case.entity, entity.clone())),
            kind: case.kind,
            items: 3,
        };
        for (teardown, expected) in [(completed, case.completed), (enforced, case.enforced)] {
            let expected = expected.map(|kind| StopCut {
                entity_kind: case.entity,
                entity: entity.clone(),
                kind,
                items: 3,
            });
            assert_eq!(
                teardown.cut_of(&entry),
                expected,
                "{:?} of a {:?} when the stop was enforced: {}",
                case.kind,
                case.entity,
                teardown.enforced
            );
        }
    }

    // A count of the whole domain names no entity, and no teardown cuts it.
    let domain_wide = StopWaitEntry {
        entity: None,
        kind: StopWaitKind::NodeWork,
        items: 1,
    };
    assert_eq!(completed.cut_of(&domain_wide), None);
    assert_eq!(enforced.cut_of(&domain_wide), None);
}
