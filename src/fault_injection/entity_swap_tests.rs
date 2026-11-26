//! Entity swap fault arming qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: which entities a scenario may arm a swap failure for.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: the entity swap, schedules or the runtime.

use meticulous::ResultExt as _;
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, NodeRef};

use super::FaultInjection;

fn node() -> ClusterNodeName {
    ClusterNodeName::parse("node-1").assured("the test node is a valid literal name")
}

fn domain() -> DomainName {
    DomainName::parse("swaps").assured("the test domain is a valid literal name")
}

fn entity(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::try_from(name).assured("the test entity is a valid literal name"),
    )
}

#[test]
fn a_swap_failure_armed_for_a_processor_fails_only_its_swap_until_cleared() {
    let faults = FaultInjection::default();
    let dedupe = entity(ModelKind::Deduplicator, "dedupe");
    faults.fail_entity_swaps_while_replacing_on(&node(), &domain(), dedupe.clone());
    assert!(faults.entity_swap_fails_while_replacing(&node(), &domain(), &dedupe));
    assert!(!faults.entity_swap_fails_while_replacing(
        &node(),
        &domain(),
        &entity(ModelKind::Reingestor, "dedupe")
    ));
    faults.clear_entity_swap_failures_on(&node(), &domain());
    assert!(!faults.entity_swap_fails_while_replacing(&node(), &domain(), &dedupe));
}

#[test]
#[should_panic(expected = "an ingestor's step of a committed change cannot fail")]
fn a_swap_failure_cannot_be_armed_for_an_ingestor() {
    FaultInjection::default().fail_entity_swaps_while_replacing_on(
        &node(),
        &domain(),
        entity(ModelKind::Ingestor, "events"),
    );
}
