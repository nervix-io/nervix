//! Resource rebinding transaction-planner tests.
//!
//! Test harness outside the product layer order.

use nervix_models::{
    ClusterNodeName, ConcreteBranchCoverage, ModelKind, NodeRef, ParseAsType, RebindResource,
    RebindResourceMembers, RebindResourceSelection, ResourceId, StatePurge, StateResetImpact,
};

use super::{
    tests::{
        FixtureUploadOutcome, fixture_uploads, node_ref, preserve_schedule, scheduled_snapshot,
        snapshot, stored_tls_vhost, tls_bundle_uploads,
    },
    *,
};
use crate::registry::test_fixtures::{
    branch, branch_schema_with_types, named, relay_branched_by, schema, wasm_processor_branched_by,
};

fn rebind_tls(version: RequestedResourceVersion, members: Option<Vec<NodeRef>>) -> Statement {
    let selection = match members {
        Some(members) => {
            let mut members = members.into_iter();
            let first = members
                .next()
                .verified("fixture exact selections always contain at least one member");
            RebindResourceSelection::Members(RebindResourceMembers::new(first, members))
        }
        None => RebindResourceSelection::All,
    };
    Statement::RebindResource(RebindResource {
        resource: named("tls_bundle"),
        version,
        selection,
    })
}

#[test]
fn rebind_latest_replaces_every_usage_in_one_model_plan() {
    let mut snapshot = snapshot(
        DomainStatus::Stopped,
        [stored_tls_vhost("api", 1), stored_tls_vhost("admin", 1)],
    );
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[
        (1, FixtureUploadOutcome::Completed),
        (2, FixtureUploadOutcome::Completed),
    ]);

    let plan = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(RequestedResourceVersion::Latest, None)],
        0,
        false,
        preserve_schedule,
    )
    .assured("both VHOST bindings can move atomically");

    let operation = &plan.operations()[0];
    assert!(matches!(
        operation.operation,
        TransactionOperation::RebindResource {
            requested: RequestedResourceVersion::Latest,
            version: 2,
            ..
        }
    ));
    assert_eq!(operation.reasons.len(), 2);
    assert!(operation.reasons.iter().all(|reason| matches!(
        reason,
        OperationImpactReason::ResourceRebinding {
            from_version: 1,
            to_version: 2,
            ..
        }
    )));
    let PlannedTransactionStepKind::Models { plan: model_plan } = &plan
        .first_step()
        .verified("the rebind forms one model run")
        .kind
    else {
        unreachable!("a rebind produces a model plan");
    };
    let planned = model_plan
        .planned
        .as_ref()
        .verified("the complete rebind carries a model plan");
    assert_eq!(planned.changed_models().len(), 2);
    assert!(
        planned
            .changed_models()
            .iter()
            .all(|model| { model.resource_version(&named("tls_bundle")) == Some(2) })
    );
}

#[test]
fn rebind_for_selects_exact_usages_and_reports_unchanged_members() {
    let mut snapshot = snapshot(
        DomainStatus::Stopped,
        [stored_tls_vhost("api", 1), stored_tls_vhost("admin", 2)],
    );
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[(2, FixtureUploadOutcome::Completed)]);
    let members = vec![node_ref(ModelKind::Vhost, "admin")];

    let plan = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(
            RequestedResourceVersion::Number(2),
            Some(members),
        )],
        0,
        false,
        preserve_schedule,
    )
    .assured("an unchanged selected usage is a successful no-op");

    let operation = &plan.operations()[0];
    assert_eq!(operation.reasons.len(), 1);
    assert!(matches!(
        &operation.reasons[0],
        OperationImpactReason::ResourceRebinding {
            node,
            from_version: 2,
            to_version: 2,
            ..
        } if node == &node_ref(ModelKind::Vhost, "admin")
    ));
    let PlannedTransactionStepKind::Models { plan: model_plan } = &plan
        .first_step()
        .verified("the rebind forms one model run")
        .kind
    else {
        unreachable!("a rebind produces a model plan");
    };
    assert!(model_plan.no_op_operations.contains(&operation.number));
    assert!(
        model_plan
            .planned
            .as_ref()
            .verified("a no-op still carries a plan")
            .is_noop()
    );
}

#[test]
fn rebind_for_rejects_a_member_that_does_not_bind_the_resource() {
    let mut snapshot = snapshot(DomainStatus::Stopped, [schema("events")]);
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[(1, FixtureUploadOutcome::Completed)]);

    let error = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(
            RequestedResourceVersion::Number(1),
            Some(vec![node_ref(ModelKind::Schema, "events")]),
        )],
        0,
        false,
        preserve_schedule,
    )
    .expect_err("every selected member must bind the resource");

    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::RebindMemberDoesNotBind { node, resource }
            if node == &node_ref(ModelKind::Schema, "events")
                && resource == &named("tls_bundle")
    ));
}

#[test]
fn rebind_rejects_a_resource_that_does_not_exist() {
    let snapshot = snapshot(DomainStatus::Stopped, [stored_tls_vhost("api", 1)]);

    let error = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(RequestedResourceVersion::Number(1), None)],
        0,
        false,
        preserve_schedule,
    )
    .expect_err("REBIND requires an existing resource");

    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::ResourceNotFound { resource }
            if resource == &named("tls_bundle")
    ));
    assert_eq!(error.to_string(), "resource 'tls_bundle' does not exist");
}

#[test]
fn rebind_for_rejects_a_member_that_does_not_exist() {
    let mut snapshot = snapshot(DomainStatus::Stopped, []);
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[(1, FixtureUploadOutcome::Completed)]);

    let error = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(
            RequestedResourceVersion::Number(1),
            Some(vec![node_ref(ModelKind::Vhost, "missing")]),
        )],
        0,
        false,
        preserve_schedule,
    )
    .expect_err("every selected member must exist");

    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::RebindMemberNotFound { domain, node }
            if domain == &named("default")
                && node == &node_ref(ModelKind::Vhost, "missing")
    ));
    assert_eq!(
        error.to_string(),
        "VHOST 'missing' does not exist in domain 'default'"
    );
}

#[test]
fn rebind_without_usages_is_a_successful_no_op() {
    let mut snapshot = snapshot(DomainStatus::Stopped, []);
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[(1, FixtureUploadOutcome::Completed)]);

    let plan = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(RequestedResourceVersion::Number(1), None)],
        0,
        false,
        preserve_schedule,
    )
    .assured("an existing resource with no usages is a successful no-op");

    let operation = &plan.operations()[0];
    assert!(operation.reasons.is_empty());
    let PlannedTransactionStepKind::Models { plan: model_plan } = &plan
        .first_step()
        .verified("the rebind forms one model run")
        .kind
    else {
        unreachable!("a rebind produces a model plan");
    };
    assert!(model_plan.no_op_operations.contains(&operation.number));
    assert!(
        model_plan
            .planned
            .as_ref()
            .verified("a no-op still carries a plan")
            .is_noop()
    );
}

#[test]
fn rebind_rejects_a_version_that_has_not_completed() {
    let mut snapshot = snapshot(DomainStatus::Stopped, [stored_tls_vhost("api", 1)]);
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[
        (1, FixtureUploadOutcome::Completed),
        (2, FixtureUploadOutcome::Applying),
    ]);

    let error = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(RequestedResourceVersion::Number(2), None)],
        0,
        false,
        preserve_schedule,
    )
    .expect_err("an applying version cannot be a rebind target");

    assert!(matches!(
        error.current_context(),
        TransactionPlanningError::ResourceVersion {
            operation,
            error: ResourceVersionResolutionError::NotCompleted(id),
        } if operation.get() == 1
            && id == &ResourceId::new(named("default"), named("tls_bundle"), 2)
    ));
    assert!(
        error
            .to_string()
            .contains("resource 'tls_bundle@2' is not a completed version in domain 'default'")
    );
}

#[test]
fn a_tls_rebinding_refreshes_the_https_listener_without_pausing_a_running_domain() {
    let domain = named("default");
    let node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let mut snapshot = scheduled_snapshot(DomainStatus::Running, [stored_tls_vhost("api", 1)]);
    snapshot.resources.insert(named("tls_bundle"));
    snapshot.resource_uploads = tls_bundle_uploads(&[
        (1, FixtureUploadOutcome::Completed),
        (2, FixtureUploadOutcome::Completed),
    ]);

    let plan = Registry::plan_transaction(
        snapshot,
        &[rebind_tls(RequestedResourceVersion::Number(2), None)],
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(&domain, std::slice::from_ref(&node), 0, placement)
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the VHOST binding can move to a completed version");

    let step = plan.first_step().verified("the rebind forms one model run");
    let planned = step.impact.planned();
    assert_eq!(planned.pause, PauseRequirement::NoPause);
    assert_eq!(planned.pause.level(), QuiesceLevel::Dynamic);
    let activations = planned.effects.activations.as_slice();
    assert_eq!(activations.len(), 1);
    assert_eq!(
        activations[0].node,
        ImpactNodeCoverage::configuration(node_ref(ModelKind::Vhost, "api"))
    );
    assert_eq!(
        activations[0].action,
        ActivationAction::RefreshHttpsListener
    );
    assert!(planned.effects.rebuilds.is_empty());
    assert!(planned.effects.force_flushes.is_empty());
    let PlannedTransactionStepKind::Models { plan: model_plan } = &step.kind else {
        unreachable!("a rebind produces a model plan");
    };
    assert!(model_plan.model_gate.affected_entities().is_empty());
}

/// Rebinding a WASM processor's module is the state effect the impact report has to carry: the
/// batch replaces the guest state of every concrete branch the processor executes, so the report
/// names the reset over that whole branch and the batch pauses the processor to activate it.
#[test]
fn a_wasm_rebinding_reports_a_guest_state_reset_over_every_concrete_branch() {
    let domain = named("default");
    let node = ClusterNodeName::parse("node-a")
        .assured("the scheduler fixture node is an identifier-shaped literal");
    let mut snapshot = scheduled_snapshot(
        DomainStatus::Running,
        [
            schema("event_schema"),
            branch_schema_with_types("tenant_schema", &[("tenant", ParseAsType::String)]),
            branch("by_tenant", "tenant_schema"),
            relay_branched_by("raw_events", "event_schema", "by_tenant"),
            relay_branched_by("filtered_events", "event_schema", "by_tenant"),
            wasm_processor_branched_by(
                "filter_events",
                "raw_events",
                "filtered_events",
                "by_tenant",
            ),
        ],
    );
    snapshot.resources.insert(named("wasm_filter"));
    snapshot.resource_uploads = fixture_uploads(
        "wasm_filter",
        &[
            (1, FixtureUploadOutcome::Completed),
            (2, FixtureUploadOutcome::Completed),
        ],
    );

    let plan = Registry::plan_transaction(
        snapshot,
        &[Statement::RebindResource(RebindResource {
            resource: named("wasm_filter"),
            version: RequestedResourceVersion::Number(2),
            selection: RebindResourceSelection::All,
        })],
        0,
        false,
        move |graph, placement, _current, _attribution| TransactionScheduleDecision {
            schedule: graph.map(|graph| {
                graph.schedule_for_domain(&domain, std::slice::from_ref(&node), 0, placement)
            }),
            ownership_moves: CanonicalImpactSet::default(),
        },
    )
    .assured("the WASM module binding can move to a completed version");

    let step = plan.first_step().verified("the rebind forms one model run");
    let planned = step.impact.planned();
    assert_eq!(planned.pause.level(), QuiesceLevel::EntityPause);
    assert_eq!(
        planned.effects.state_resets.as_slice(),
        &[StateResetImpact {
            node: ImpactNodeCoverage::execution(
                node_ref(ModelKind::WasmProcessor, "filter_events"),
                ConcreteBranchCoverage::AllOfBranch {
                    branch: named("by_tenant"),
                },
            ),
            state: StatePurge::WasmGuestState,
            attribution: ImpactAttribution::single(
                TransactionOperationNumber::from_index(0)
                    .assured("the first operation of a transaction is addressable"),
            ),
        }]
    );
}

/// A WASM processor whose module binding is untouched keeps its guest state, so a rebinding that
/// leaves it at the version it already holds reports no reset at all.
#[test]
fn a_wasm_rebinding_to_the_bound_version_reports_no_guest_state_reset() {
    let mut snapshot = scheduled_snapshot(
        DomainStatus::Running,
        [
            schema("event_schema"),
            branch_schema_with_types("tenant_schema", &[("tenant", ParseAsType::String)]),
            branch("by_tenant", "tenant_schema"),
            relay_branched_by("raw_events", "event_schema", "by_tenant"),
            relay_branched_by("filtered_events", "event_schema", "by_tenant"),
            wasm_processor_branched_by(
                "filter_events",
                "raw_events",
                "filtered_events",
                "by_tenant",
            ),
        ],
    );
    snapshot.resources.insert(named("wasm_filter"));
    snapshot.resource_uploads = fixture_uploads(
        "wasm_filter",
        &[
            (1, FixtureUploadOutcome::Completed),
            (2, FixtureUploadOutcome::Completed),
        ],
    );

    let plan = Registry::plan_transaction(
        snapshot,
        &[Statement::RebindResource(RebindResource {
            resource: named("wasm_filter"),
            version: RequestedResourceVersion::Number(1),
            selection: RebindResourceSelection::All,
        })],
        0,
        false,
        preserve_schedule,
    )
    .assured("rebinding to the version already bound changes nothing");

    let planned = plan
        .first_step()
        .verified("the rebind forms one model run")
        .impact
        .planned();
    assert_eq!(planned.pause.level(), QuiesceLevel::Dynamic);
    assert!(planned.effects.state_resets.is_empty());
}
