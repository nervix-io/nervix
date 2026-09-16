//! Relay admission boundaries for an entity-scoped pause.
//!
//! Layer: decisions.
//!
//! - **Owns.** Pure derivation of the relay gates required by a scheduled entity scope.
//! - **Depends on.** Scheduled Models and their typed node identities.
//! - **Must not know.** Runtime tasks, gate leases, cluster coordination or persistence.

use ahash::HashSet;
use nervix_models::{DomainSchedule, Model, ModelKind, NodeRef, RelayName};

pub(crate) fn entity_pause_relays_for_schedule(
    schedule: &DomainSchedule,
    affected_entities: &[NodeRef],
) -> Vec<RelayName> {
    let mut relays = Vec::new();
    for entity in affected_entities {
        if entity.kind == ModelKind::Relay {
            relays.push(RelayName::from(&entity.identifier));
            continue;
        }
        let Some(node) = schedule.nodes.get(entity) else {
            continue;
        };
        relays.extend(entity_input_relays(node.config.as_ref()));
    }
    relays.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    relays.dedup();
    relays
}

pub(crate) fn ownership_handoff_relays_for_schedule(
    schedule: &DomainSchedule,
    affected_entities: &[NodeRef],
) -> Vec<RelayName> {
    let mut relays = entity_pause_relays_for_schedule(schedule, affected_entities);
    let affected = affected_entities.iter().cloned().collect::<HashSet<_>>();
    relays.retain(|relay| {
        let mut has_producer = false;
        let mut has_unaffected_producer = false;
        for node in schedule.nodes.values() {
            let produces_relay = node
                .config
                .output_routes()
                .is_some_and(|outputs| outputs.relays().any(|output| output == relay));
            if !produces_relay {
                continue;
            }

            has_producer = true;
            if !has_unaffected_producer {
                has_unaffected_producer = !affected.contains(&node.identity());
            }
        }

        !has_producer || has_unaffected_producer
    });
    relays
}

fn entity_input_relays(model: &Model) -> Vec<RelayName> {
    match model {
        Model::Emitter(emitter) => emitter.from.relays().to_vec(),
        Model::Reingestor(reingestor) => reingestor.from.relays().to_vec(),
        Model::Generator(generator) => vec![generator.materialized_relay.clone()],
        Model::Inferencer(inferencer) => inferencer.from.relays().to_vec(),
        Model::WasmProcessor(processor) => processor.from.relays().to_vec(),
        Model::Junction(junction) => junction.from.relays().to_vec(),
        Model::Deduplicator(deduplicator) => deduplicator.from.relays().to_vec(),
        Model::Correlator(correlator) => correlator
            .left
            .relays()
            .iter()
            .chain(correlator.right.relays())
            .cloned()
            .collect(),
        Model::Reorderer(reorderer) => reorderer.from.relays().to_vec(),
        Model::WindowProcessor(processor) => processor.from.relays().to_vec(),
        _ => Vec::new(),
    }
}
