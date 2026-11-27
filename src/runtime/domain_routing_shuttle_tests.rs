//! Atomic typed processor-plan publication and the wait for a domain's routing, explored under
//! Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants that readers observe one complete domain routing revision and that a
//!   wait for a domain's routing is woken by its publication.
//! - **Depends on.** The production routing publisher, cache, domain routing table and typed
//!   processor plan.
//! - **Must not know.** Schedule application, processor execution or graph Models.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use ahash::HashMap;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{AckMode, ErrorPolicies, ModelKind, ModelName, NodeRef};
use nervix_primitives::{sync::StdArc, thread};

use super::*;
use crate::registry::{
    BranchedProcessorNodeSpec, BranchedProcessorOperationSpec, BranchedProcessorOutputsSpec,
    BranchedProcessorSpec,
};

const MODEL_THREAD_JOINS: &str =
    "Shuttle fails the execution when a model thread panics, so no join observes one";
const MODEL_TASK_JOINS: &str =
    "Shuttle fails the execution when a model task panics, so no join observes one";

fn named<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str>,
    for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
{
    N::try_from(raw).assured("the model uses valid names")
}

fn plan(processor: &str, input: &str) -> StdArc<PublishedProcessorPlan> {
    StdArc::new(PublishedProcessorPlan {
        source: Arc::new(BranchedProcessorNodeSpec {
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
            parametrizer: None,
            branch_ttl: None,
            branch_max_instances: None,
            binding: Default::default(),
        }),
        unit_revision: ProcessorUnitRevision::Stateless,
        template: StdArc::new(junction_branch_template(processor, input)),
    })
}

fn a_reader_observes_one_complete_typed_routing_revision() {
    let route_identity = NodeUnitRef::new(
        NodeRef::new(ModelKind::Junction, named::<ModelName>("route_events")),
        Unit::SINGLE,
    );
    let audit_identity = NodeUnitRef::new(
        NodeRef::new(ModelKind::Junction, named::<ModelName>("audit_events")),
        Unit::SINGLE,
    );
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

/// A routing for installation to publish, holding no processor plan.
fn unpublished_routing() -> SharedDomainRouting {
    StdArc::new(ArcSwap::from_pointee(DomainRoutingSnapshot::default()))
}

/// A held delivery waits for its domain's routing while installation publishes another domain's
/// routing and then its own. A publication that lands between the waiter's check and its wait must
/// still end the wait: lost, the waiter would wait forever and Shuttle would report the deadlock.
/// The wait ends with exactly its own domain's routing, never another domain's.
fn a_routing_published_between_a_waiters_check_and_its_wait_ends_it() {
    shuttle::future::block_on(async {
        let routings = Arc::new(DomainRoutings::default());
        let waited = DomainName::parse("orders")
            .assured("the fixture domain name follows the public name grammar");
        let routing = unpublished_routing();

        let waiter = nervix_primitives::task::spawn({
            let routings = Arc::clone(&routings);
            let waited = waited.clone();
            let expected = StdArc::clone(&routing);
            async move {
                let published = routings.published(&waited).await;
                assert!(
                    StdArc::ptr_eq(&published, &expected),
                    "a wait for a domain's routing ended with another routing"
                );
            }
        });
        let installer = nervix_primitives::task::spawn(async move {
            let other = DomainName::parse("audit")
                .assured("the fixture domain name follows the public name grammar");
            routings.insert(other, unpublished_routing());
            routings.insert(waited, routing);
        });

        installer.await.assured(MODEL_TASK_JOINS);
        waiter.await.assured(MODEL_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_routing_published_between_a_waiters_check_and_its_wait_is_never_missed() {
    check_interleavings(a_routing_published_between_a_waiters_check_and_its_wait_ends_it);
}
