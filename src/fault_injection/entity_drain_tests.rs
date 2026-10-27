//! Forced entity drain timeout qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: which drain observations one armed forced timeout refuses, and when it ends.
//! Depends on: the host's fault controls, vocabulary identities, the durable mutation owner and the
//! gate purpose.
//! Must not know: entity gates, transactions, command recovery or the runtime.

use meticulous::ResultExt as _;
use nervix_consensus::DomainMutationOwner;
use nervix_interconnect::EntityGatePurpose;
use nervix_models::{
    CommandExecutionReference, DomainName, EmitterName, IngestorName, JunctionName, ModelKind,
    NodeRef, RelayName,
};

use super::{FaultInjection, ObservedEntityDrain};

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain is a valid literal name")
}

fn command(reference: &str) -> DomainMutationOwner {
    DomainMutationOwner::command(
        CommandExecutionReference::parse(reference)
            .assured("the test reference is a valid literal execution reference"),
    )
}

fn relay(name: &str) -> RelayName {
    RelayName::parse(name).assured("the test relay is a valid literal name")
}

fn ingestor(name: &str) -> NodeRef {
    NodeRef::new(
        ModelKind::Ingestor,
        IngestorName::parse(name).assured("the test ingestor is a valid literal name"),
    )
}

#[test]
fn a_forced_drain_timeout_refuses_its_drain_whenever_its_execution_observes_it_again() {
    let faults = FaultInjection::default();
    let altered = domain("altered");
    let alteration = command("01a179a3-713f-7521-b0f6-c7cde3e04f02");
    let later = DomainMutationOwner::transaction("later-transaction".to_string());
    let relays = [relay("syslog_events")];
    let entities = [ingestor("syslog_intake")];
    let alteration_drain = ObservedEntityDrain {
        owner: Some(&alteration),
        purpose: EntityGatePurpose::ModelAlteration,
        relays: &relays,
        affected_entities: &entities,
    };
    assert!(!faults.entity_drain_timeout_is_forced(&altered));
    assert!(!faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));

    faults.force_next_entity_drain_timeout(altered.clone());
    assert!(faults.entity_drain_timeout_is_forced(&altered));
    assert!(faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));
    // Its refusal was never recorded, so a new leader or a retry under the same execution
    // reference runs the alteration again, and meets the same refusal.
    assert!(faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));
    assert!(faults.entity_drain_timeout_is_forced(&altered));

    // Another execution owns the domain once the refused one reached its terminal outcome.
    let later_drain = ObservedEntityDrain {
        owner: Some(&later),
        ..alteration_drain
    };
    assert!(!faults.take_forced_entity_drain_timeout(&altered, &later_drain));
    assert!(!faults.entity_drain_timeout_is_forced(&altered));
    let alteration_drain = ObservedEntityDrain {
        owner: Some(&alteration),
        ..later_drain
    };
    assert!(!faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));
}

#[test]
fn a_forced_drain_timeout_leaves_the_other_drains_of_its_execution_alone() {
    let faults = FaultInjection::default();
    let drained = domain("drained");
    let node_drain = command("01a179a3-9000-7000-8000-000000000001");
    let stalled = [NodeRef::new(
        ModelKind::Emitter,
        EmitterName::parse("stalled_emitter").assured("the test emitter is a valid literal name"),
    )];
    let movable = [NodeRef::new(
        ModelKind::Junction,
        JunctionName::parse("movable_junction")
            .assured("the test junction is a valid literal name"),
    )];
    let relays = [relay("staged")];
    let stalled_handoff = ObservedEntityDrain {
        owner: Some(&node_drain),
        purpose: EntityGatePurpose::OwnershipHandoff,
        relays: &relays,
        affected_entities: &stalled,
    };
    faults.force_next_entity_drain_timeout(drained.clone());
    assert!(faults.take_forced_entity_drain_timeout(&drained, &stalled_handoff));

    // The other unit the same node drain moves waits for other entities.
    let movable_handoff = ObservedEntityDrain {
        affected_entities: &movable,
        ..stalled_handoff
    };
    assert!(!faults.take_forced_entity_drain_timeout(&drained, &movable_handoff));
    // A source-intake drain waits for the same entities without their relays.
    let intake = ObservedEntityDrain {
        relays: &[],
        ..stalled_handoff
    };
    assert!(!faults.take_forced_entity_drain_timeout(&drained, &intake));
    let alteration = ObservedEntityDrain {
        purpose: EntityGatePurpose::ModelAlteration,
        ..stalled_handoff
    };
    assert!(!faults.take_forced_entity_drain_timeout(&drained, &alteration));

    assert!(faults.entity_drain_timeout_is_forced(&drained));
    assert!(faults.take_forced_entity_drain_timeout(&drained, &stalled_handoff));
}

#[test]
fn a_forced_drain_timeout_ends_once_no_execution_owns_its_domain() {
    let faults = FaultInjection::default();
    let altered = domain("altered");
    let alteration = command("01a179a3-713f-7521-b0f6-c7cde3e04f02");
    let entities = [ingestor("syslog_intake")];
    let alteration_drain = ObservedEntityDrain {
        owner: Some(&alteration),
        purpose: EntityGatePurpose::ModelAlteration,
        relays: &[],
        affected_entities: &entities,
    };
    faults.force_next_entity_drain_timeout(altered.clone());
    assert!(faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));

    let unowned_drain = ObservedEntityDrain {
        owner: None,
        ..alteration_drain
    };
    assert!(!faults.take_forced_entity_drain_timeout(&altered, &unowned_drain));
    assert!(!faults.entity_drain_timeout_is_forced(&altered));
    let alteration_drain = ObservedEntityDrain {
        owner: Some(&alteration),
        ..unowned_drain
    };
    assert!(!faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));
}

#[test]
fn a_forced_drain_timeout_refuses_a_drain_no_execution_owns_once() {
    let faults = FaultInjection::default();
    let drained = domain("drained");
    let later = command("01a179a3-9000-7000-8000-000000000001");
    let entities = [ingestor("syslog_intake")];
    let unowned_drain = ObservedEntityDrain {
        owner: None,
        purpose: EntityGatePurpose::OwnershipHandoff,
        relays: &[],
        affected_entities: &entities,
    };
    faults.force_next_entity_drain_timeout(drained.clone());

    assert!(faults.take_forced_entity_drain_timeout(&drained, &unowned_drain));
    assert!(!faults.entity_drain_timeout_is_forced(&drained));
    assert!(!faults.take_forced_entity_drain_timeout(&drained, &unowned_drain));
    let owned_drain = ObservedEntityDrain {
        owner: Some(&later),
        ..unowned_drain
    };
    assert!(!faults.take_forced_entity_drain_timeout(&drained, &owned_drain));
}

#[test]
fn a_forced_drain_timeout_refuses_only_drains_of_its_domain() {
    let faults = FaultInjection::default();
    let altered = domain("altered");
    let unrelated = domain("unrelated");
    let alteration = command("01a179a3-713f-7521-b0f6-c7cde3e04f02");
    let entities = [ingestor("syslog_intake")];
    let alteration_drain = ObservedEntityDrain {
        owner: Some(&alteration),
        purpose: EntityGatePurpose::ModelAlteration,
        relays: &[],
        affected_entities: &entities,
    };
    faults.force_next_entity_drain_timeout(altered.clone());

    assert!(!faults.entity_drain_timeout_is_forced(&unrelated));
    assert!(!faults.take_forced_entity_drain_timeout(&unrelated, &alteration_drain));
    assert!(faults.take_forced_entity_drain_timeout(&altered, &alteration_drain));
}
