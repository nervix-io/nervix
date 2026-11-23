//! A domain that a failed application brought to its revision, at the runtime.
//!
//! Layer: test harness.
//!
//! - **Owns.** Tests that a domain an application failing on a later domain had already brought to
//!   the new revision takes a later change in place, through the test-only application path that
//!   records what each domain's execution runs as the control plane does.
//! - **Depends on.** The production schedule application path, the swap fault injection and the
//!   runtime's test fixtures.
//! - **Must not know.** Production control-plane orchestration or connector behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeMap;

use nervix_models::{
    AckMode, BranchSelection, ClusterNodeName, ClusterSchedule, CreateDeduplicator, CreateRelay,
    CreateSchema, DeduplicatorName, DomainSchedule, ModelKind, ModelName, NodeRef, NodeUnitRef,
    ParseAsType, ProcessorInputs, ProcessorOutputs, RelayBranching, SchemaField, SchemaName, Unit,
};
use nonzero_ext::nonzero;

use super::*;

/// A deduplicator keyed on `key` between two relays of one schema, all on node-1.
fn deduplicating_schedule(domain: &DomainName, key: &str) -> DomainSchedule {
    let event_schema = named::<SchemaName>("event");
    let field = |name: &str| SchemaField {
        name: named(name),
        ty: ParseAsType::String,
        optional: false,
        sensitive: false,
    };
    let relay = |name: &str| {
        scheduled_model(nervix_models::Model::Relay(CreateRelay {
            labels: Default::default(),
            name: named(name),
            schema: event_schema.clone(),
            buffer: nonzero!(2usize),
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        }))
    };
    DomainSchedule::new(
        domain.clone(),
        vec![
            scheduled_model(nervix_models::Model::Schema(CreateSchema {
                labels: Default::default(),
                name: event_schema.clone(),
                fields: vec![field("event_id"), field("content")],
            })),
            relay("events"),
            relay("unique_events"),
            scheduled_model(nervix_models::Model::Deduplicator(CreateDeduplicator {
                labels: Default::default(),
                name: named::<DeduplicatorName>("unique"),
                from: ProcessorInputs::single(named("events")),
                output_routes: with_inherit_all(ProcessorOutputs::single(named("unique_events")))
                    .with_flush_policy(FlushPolicy::Each {
                        interval: "100ms".to_string(),
                        max_batch_size: "1MiB".to_string(),
                    }),
                branched_by: BranchSelection::unbranched(),
                deduplicate_on: vec![expression(key)],
                max_time: "10m".to_string(),
                mode: AckMode::Attached,
                filter_where: None,
                materialized_state: Vec::new(),
            })),
        ],
        Vec::new(),
    )
}

fn deduplicator() -> NodeRef {
    NodeRef {
        kind: ModelKind::Deduplicator,
        identifier: named::<ModelName>("unique"),
    }
}

fn revision_digest(schedule: &DomainSchedule) -> [u8; 32] {
    ExecutionRevision::from_schedule(schedule)
        .assured("the deduplicating fixture schedules a complete revision")
        .source_digest
}

/// A node applies a revision one domain after another, so an application that fails on a later
/// domain has already brought the earlier ones to the revision. A later revision that changes such
/// a domain again reaches it in place, by the change from the revision it runs, rather than
/// stopping and rebuilding its execution because it no longer runs the revision the node recorded
/// as applied.
#[nervix_primitives::test]
async fn a_domain_a_failed_application_moved_forward_takes_a_later_change_in_place() {
    let runtime = Runtime::default();
    let node = ClusterNodeName::parse("node-1").expect("valid name");
    attach_loopback_cluster(&runtime, &node).await;
    // The failing domain sorts after the moving one, so an application reaches it second.
    let moving = domain("moving");
    let failing = domain("moving_late");
    let domains = BTreeMap::from([
        (moving.clone(), unpaced_domain_state(moving.as_str())),
        (failing.clone(), unpaced_domain_state(failing.as_str())),
    ]);
    let both_by_id = ClusterSchedule::from_iter([
        deduplicating_schedule(&moving, "input.event_id"),
        deduplicating_schedule(&failing, "input.event_id"),
    ]);
    runtime
        .apply_cluster_state(&node, 1, &domains, &BTreeMap::new(), &both_by_id)
        .await
        .expect("both domains build");

    runtime
        .inner
        .fault_injection
        .fail_entity_swaps_while_replacing_on(&node, &failing, deduplicator());
    let both_by_content = ClusterSchedule::from_iter([
        deduplicating_schedule(&moving, "input.content"),
        deduplicating_schedule(&failing, "input.content"),
    ]);
    let failure = runtime
        .apply_cluster_state(&node, 2, &domains, &BTreeMap::new(), &both_by_content)
        .await
        .expect_err("the failing domain's swap fails the application");
    assert!(
        format!("{failure:#}").contains("injected entity-level schedule apply failure"),
        "the application fails on the failing domain's swap: {failure:#}"
    );
    let moving_shutdown = {
        let execution = runtime
            .inner
            .executions
            .get(&moving)
            .expect("the moving domain keeps its execution");
        assert_eq!(
            execution.revision.source_digest,
            revision_digest(&deduplicating_schedule(&moving, "input.content")),
            "the failed application brought the moving domain to its revision"
        );
        execution.shutdown.subscribe()
    };

    let moving_by_id_again = ClusterSchedule::from_iter([
        deduplicating_schedule(&moving, "input.event_id"),
        deduplicating_schedule(&failing, "input.content"),
    ]);
    let failure = runtime
        .apply_cluster_state(&node, 3, &domains, &BTreeMap::new(), &moving_by_id_again)
        .await
        .expect_err("the failing domain's swap still fails the application");
    assert!(
        format!("{failure:#}").contains("injected entity-level schedule apply failure"),
        "the later application fails only on the failing domain's swap: {failure:#}"
    );
    {
        let execution = runtime
            .inner
            .executions
            .get(&moving)
            .expect("the moving domain keeps its execution");
        assert_eq!(
            execution.revision.source_digest,
            revision_digest(&deduplicating_schedule(&moving, "input.event_id")),
            "the later change reached the moving domain"
        );
        assert!(
            execution
                .node_tasks
                .contains_key(&NodeUnitRef::new(deduplicator(), Unit::SINGLE)),
            "the moving domain runs its changed deduplicator"
        );
    }
    assert!(
        matches!(moving_shutdown.has_changed(), Ok(false)),
        "the moving domain took the later change in place: its execution was never stopped"
    );

    runtime
        .inner
        .fault_injection
        .clear_entity_swap_failures_on(&node, &failing);
    runtime
        .apply_cluster_state(&node, 3, &domains, &BTreeMap::new(), &moving_by_id_again)
        .await
        .expect("the node applies the revision once the failing domain's swap succeeds");
    assert!(
        matches!(moving_shutdown.has_changed(), Ok(false)),
        "applying the revision again changed nothing in the moving domain"
    );
    let failing_digest = runtime
        .inner
        .executions
        .get(&failing)
        .expect("the failing domain keeps its execution")
        .revision
        .source_digest;
    assert_eq!(
        failing_digest,
        revision_digest(&deduplicating_schedule(&failing, "input.content")),
        "the failing domain took its change once its swap could succeed"
    );
}
