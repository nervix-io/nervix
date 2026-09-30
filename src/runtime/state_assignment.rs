//! Entity state identity and checkpoint assignment publication.
//!
//! Layer: data plane.
//! - **Owns.** One immutable identity and its checkpoint execution and replica boundaries.
//! - **Depends on.** State identity vocabulary and the primitive publication boundary.
//! - **Must not know.** Guest execution, persistence, transactions or replica progress.

use std::collections::BTreeSet;

use nervix_models::ClusterNodeName;
use nervix_primitives::publication::ArcSwapOption;
use triomphe::Arc;

use super::ScheduledStateIdentity;

/// The identity and checkpoint owners committed for one entity, published together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ScheduledStateAssignment {
    pub(in crate::runtime) identity: ScheduledStateIdentity,
    pub(in crate::runtime) checkpoint_owners: Option<WasmCheckpointOwners>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct WasmCheckpointOwners {
    pub(in crate::runtime) executors: BTreeSet<ClusterNodeName>,
    pub(in crate::runtime) replicas: BTreeSet<ClusterNodeName>,
}

pub(in crate::runtime) type SharedStateAssignment = Arc<ArcSwapOption<ScheduledStateAssignment>>;

#[cfg(test)]
mod tests {
    use nervix_models::{SchemaFingerprint, WasmStateGeneration, WasmStateGenerations};

    use super::*;
    use crate::runtime::{state_replication::StateReplicationError, *};

    #[test]
    fn retained_checkpoint_assignment_fences_replaced_identity_and_entity_removal() {
        let runtime = Runtime::new();
        let domain = domain("retained_guest");
        let processor: ModelName = named("guest");
        let entity =
            DomainNodeRef::node_in(domain.clone(), ModelKind::WasmProcessor, processor.clone());
        let schema = SchemaFingerprint::from_digest([1; 32]);
        runtime.publish_state_assignment(
            entity.clone(),
            ScheduledStateAssignment {
                identity: ScheduledStateIdentity {
                    schema_fingerprint: schema,
                    wasm_state_generations: Some(WasmStateGenerations::first()),
                },
                checkpoint_owners: None,
            },
        );
        let state = runtime
            .replicated_wasm_processor_state(RuntimeStatePlacement {
                domain: domain.clone(),
                kind: ModelKind::WasmProcessor,
                identifier: processor,
                branch_key: None,
                state: RuntimeState::WasmProcessor {
                    schema,
                    generation: WasmStateGeneration::FIRST,
                },
            })
            .assured("the guest binds the current assignment");
        assert!(matches!(
            runtime
                .wasm_checkpoint_boundary(&state)
                .assured("the current guest reaches local durability"),
            wasm_state::WasmCheckpointBoundary::LocalStorage
        ));

        runtime.publish_state_assignment(
            entity,
            ScheduledStateAssignment {
                identity: ScheduledStateIdentity {
                    schema_fingerprint: SchemaFingerprint::from_digest([2; 32]),
                    wasm_state_generations: Some(WasmStateGenerations::first()),
                },
                checkpoint_owners: None,
            },
        );
        let superseded = runtime
            .wasm_checkpoint_boundary(&state)
            .err()
            .assured("a replaced identity fences the guest's retained assignment");
        assert!(matches!(
            superseded.current_context(),
            StateReplicationError::Superseded { .. }
        ));
        runtime.clear_state_identities(&domain);
        assert!(state.assignment.load().is_none());
        assert!(matches!(
            runtime
                .wasm_checkpoint_boundary(&state)
                .err()
                .assured("entity removal also fences the retained assignment")
                .current_context(),
            StateReplicationError::Superseded { .. }
        ));
    }
}
