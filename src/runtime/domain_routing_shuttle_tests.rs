//! Atomic typed processor-plan publication, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariant that readers observe one complete domain routing revision.
//! - **Depends on.** The production routing publisher, cache and typed processor plan.
//! - **Must not know.** Schedule application, processor execution or graph Models.

use ahash::HashMap;
use nervix_models::{AckMode, ErrorPolicies, ModelKind, ModelName, NodeRef};
use nervix_primitives::{sync::StdArc, thread};

use super::*;
use crate::{
    registry::{
        BranchedProcessorNodeSpec, BranchedProcessorOperationSpec, BranchedProcessorOutputsSpec,
        BranchedProcessorSpec,
    },
    shuttle_test::check_interleavings,
};

const MODEL_THREAD_JOINS: &str =
    "Shuttle fails the execution when a model thread panics, so no join observes one";

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).assured("the model uses valid names")
}

fn plan(processor: &str, input: &str) -> StdArc<PublishedProcessorPlan> {
    StdArc::new(PublishedProcessorPlan {
        source: BranchedProcessorNodeSpec {
            spec: BranchedProcessorSpec {
                kind: ModelKind::Junction,
                processor: named(processor),
                input_relays: vec![named(input)],
                input_collect_policies: HashMap::default(),
                mode: AckMode::Attached,
                error_policies: ErrorPolicies::handled_by_log(),
                from_where: HashMap::default(),
                filter_where: None,
                materialized_state: Vec::new(),
                operation: BranchedProcessorOperationSpec::Junction {
                    output_routes: BranchedProcessorOutputsSpec { routes: Vec::new() },
                },
            },
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            wasm_state_reset: None,
            binding: Default::default(),
        },
        template: StdArc::new(junction_branch_template(processor, input)),
    })
}

fn a_reader_observes_one_complete_typed_routing_revision() {
    let route_identity = NodeRef::new(ModelKind::Junction, named::<ModelName>("route_events"));
    let audit_identity = NodeRef::new(ModelKind::Junction, named::<ModelName>("audit_events"));
    let first_route_plan = plan("route_events", "incoming");
    let first_audit_plan = plan("audit_events", "audit_incoming");
    let second_route_plan = plan("route_events", "incoming");
    let second_audit_plan = plan("audit_events", "audit_incoming");
    let mut routing = DomainRouting::new(DomainRoutingSnapshot {
        passive_only: false,
        processor_plans: [
            (route_identity.clone(), first_route_plan.clone()),
            (audit_identity.clone(), first_audit_plan.clone()),
        ]
        .into_iter()
        .collect(),
        ..DomainRoutingSnapshot::default()
    });
    let published = routing.shared();

    let writer_route_identity = route_identity.clone();
    let writer_audit_identity = audit_identity.clone();
    let writer_route_plan = second_route_plan.clone();
    let writer_audit_plan = second_audit_plan.clone();
    let writer = thread::spawn(move || {
        routing.passive_only = true;
        thread::yield_now();
        routing
            .processor_plans
            .insert(writer_route_identity, writer_route_plan);
        thread::yield_now();
        routing
            .processor_plans
            .insert(writer_audit_identity, writer_audit_plan);
        thread::yield_now();
        routing.publish();
    });
    let reader = thread::spawn(move || {
        let mut cache = DomainRoutingCache::new(published);
        for _ in 0..3 {
            let snapshot = cache.load();
            let observed_route = snapshot
                .processor_plans
                .get(&route_identity)
                .assured("every published revision contains the route plan");
            let observed_audit = snapshot
                .processor_plans
                .get(&audit_identity)
                .assured("every published revision contains the audit plan");
            if snapshot.passive_only {
                assert!(StdArc::ptr_eq(observed_route, &second_route_plan));
                assert!(StdArc::ptr_eq(observed_audit, &second_audit_plan));
            } else {
                assert!(StdArc::ptr_eq(observed_route, &first_route_plan));
                assert!(StdArc::ptr_eq(observed_audit, &first_audit_plan));
            }
            thread::yield_now();
        }
    });

    writer.join().assured(MODEL_THREAD_JOINS);
    reader.join().assured(MODEL_THREAD_JOINS);
}

#[test]
fn shuttle_a_reader_observes_one_complete_typed_routing_revision() {
    check_interleavings(a_reader_observes_one_complete_typed_routing_revision);
}
