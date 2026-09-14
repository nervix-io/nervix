//! Runtime-state ownership handoff identities and activation state.
//!
//! Layer: data plane.
//!
//! - **Owns.** Exact prepared and activated ownership-handoff transitions.
//! - **Depends on.** Shared coordination identities, schedules and persisted runtime state.
//! - **Must not know.** Handoff transport, schedule planning or state-store key encoding.

use super::*;

#[derive(Debug, Clone)]
pub(in crate::runtime) struct PreparedRuntimeStateHandoff {
    pub(in crate::runtime) coordination: CoordinationIdentity,
    pub(in crate::runtime) operation_id: String,
    pub(in crate::runtime) source: ClusterNodeName,
    pub(in crate::runtime) destination: ClusterNodeName,
    pub(in crate::runtime) source_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) destination_incarnation: ClusterNodeIncarnation,
    pub(in crate::runtime) base_schedule_fingerprint: [u8; 32],
    pub(in crate::runtime) target_schedule_fingerprint: [u8; 32],
    pub(in crate::runtime) activation_authorization: OwnershipHandoffActivationAuthorization,
    pub(in crate::runtime) activation: watch::Sender<OwnershipHandoffActivation>,
    pub(in crate::runtime) checkpoints: Vec<(RuntimeStatePlacement, PersistedRuntimeStateEntry)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum OwnershipHandoffActivationAuthorization {
    AuthorizedByPreparation,
    RecoveredAwaitingRequest,
    RecoveredAuthorized,
}

impl OwnershipHandoffActivationAuthorization {
    pub(super) fn is_authorized(self) -> bool {
        self != Self::RecoveredAwaitingRequest
    }

    /// Authorizes activation and reports whether the recovered schedule must be rebuilt.
    ///
    /// A recovered preparation continues to request a rebuild until activation removes it. This
    /// keeps a failed or cancelled rebuild retriable without making an ordinary handoff race its
    /// normal schedule reconciliation with a second rebuild.
    pub(super) fn authorize(&mut self) -> bool {
        match self {
            Self::AuthorizedByPreparation => false,
            Self::RecoveredAwaitingRequest => {
                *self = Self::RecoveredAuthorized;
                true
            }
            Self::RecoveredAuthorized => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum OwnershipHandoffActivation {
    Prepared,
    Activated,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OwnershipHandoffTransitionRef<'a> {
    pub(super) coordination: &'a CoordinationIdentity,
    pub(super) operation_id: &'a str,
    pub(super) source: &'a ClusterNodeName,
    pub(super) destination: &'a ClusterNodeName,
    pub(super) source_incarnation: ClusterNodeIncarnation,
    pub(super) destination_incarnation: ClusterNodeIncarnation,
    pub(super) domain: &'a DomainName,
    pub(super) entity: &'a NodeRef,
    pub(super) base_schedule_fingerprint: [u8; 32],
    pub(super) target_schedule_fingerprint: [u8; 32],
}

impl<'a> From<&'a nervix_interconnect::ActivateOwnershipHandoffStateRequest>
    for OwnershipHandoffTransitionRef<'a>
{
    fn from(request: &'a nervix_interconnect::ActivateOwnershipHandoffStateRequest) -> Self {
        Self {
            coordination: &request.coordination,
            operation_id: &request.operation_id,
            source: &request.source,
            destination: &request.destination,
            source_incarnation: request.source_incarnation,
            destination_incarnation: request.destination_incarnation,
            domain: &request.domain,
            entity: &request.entity,
            base_schedule_fingerprint: request.base_schedule_fingerprint,
            target_schedule_fingerprint: request.target_schedule_fingerprint,
        }
    }
}

impl<'a> From<&'a nervix_interconnect::ConfirmOwnershipHandoffStateRequest>
    for OwnershipHandoffTransitionRef<'a>
{
    fn from(request: &'a nervix_interconnect::ConfirmOwnershipHandoffStateRequest) -> Self {
        Self {
            coordination: &request.coordination,
            operation_id: &request.operation_id,
            source: &request.source,
            destination: &request.destination,
            source_incarnation: request.source_incarnation,
            destination_incarnation: request.destination_incarnation,
            domain: &request.domain,
            entity: &request.entity,
            base_schedule_fingerprint: request.base_schedule_fingerprint,
            target_schedule_fingerprint: request.target_schedule_fingerprint,
        }
    }
}

impl PreparedRuntimeStateHandoff {
    pub(super) fn matches(&self, transition: OwnershipHandoffTransitionRef<'_>) -> bool {
        self.coordination == *transition.coordination
            && self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.source_incarnation == transition.source_incarnation
            && self.destination_incarnation == transition.destination_incarnation
            && self.base_schedule_fingerprint == transition.base_schedule_fingerprint
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }

    pub(super) fn belongs_to_committed_transition(
        &self,
        node: &ScheduledNode,
        schedule_fingerprint: [u8; 32],
    ) -> bool {
        if self.target_schedule_fingerprint != schedule_fingerprint
            || !node.is_primary_on(&self.destination)
        {
            return false;
        }
        let Some(transition) = node.ownership_transition.as_ref() else {
            return false;
        };
        transition.id == self.operation_id
            && transition.source == self.source
            && transition.destination == self.destination
            && transition.state_recovery == OwnershipStateRecoveryOutcome::Complete
            && transition.resets.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct ActivatedRuntimeStateHandoff {
    pub(super) coordination: CoordinationIdentity,
    pub(super) operation_id: String,
    pub(super) source: ClusterNodeName,
    pub(super) destination: ClusterNodeName,
    pub(super) source_incarnation: ClusterNodeIncarnation,
    pub(super) destination_incarnation: ClusterNodeIncarnation,
    pub(super) base_schedule_fingerprint: [u8; 32],
    pub(super) target_schedule_fingerprint: [u8; 32],
}

impl ActivatedRuntimeStateHandoff {
    pub(super) fn matches(&self, transition: OwnershipHandoffTransitionRef<'_>) -> bool {
        self.coordination == *transition.coordination
            && self.operation_id == transition.operation_id
            && self.source == *transition.source
            && self.destination == *transition.destination
            && self.source_incarnation == transition.source_incarnation
            && self.destination_incarnation == transition.destination_incarnation
            && self.base_schedule_fingerprint == transition.base_schedule_fingerprint
            && self.target_schedule_fingerprint == transition.target_schedule_fingerprint
    }
}

impl Runtime {
    /// Reconciles destination preparations against one applied schedule under the current leader
    /// process. An exact committed transition always wins. An uncommitted preparation remains only
    /// while the same coordinator process and both participant processes are still present and its
    /// exact gate is held.
    pub(crate) fn reconcile_prepared_ownership_handoffs(
        &self,
        authority: &CoordinationIdentity,
        schedule: &ClusterSchedule,
        node_incarnations: &BTreeMap<ClusterNodeName, ClusterNodeIncarnation>,
    ) -> OwnershipHandoffResult<usize> {
        let preparations = self
            .inner
            .prepared_runtime_state_handoffs
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect::<Vec<_>>();
        let mut discarded = 0_usize;
        for (entity, prepared) in preparations {
            let committed_or_active = if let Some(domain_schedule) = schedule.domain(&entity.domain)
            {
                let schedule_fingerprint =
                    Self::ownership_handoff_schedule_fingerprint(domain_schedule)?;
                let scheduled = domain_schedule.nodes.get(&entity.node);
                let committed = scheduled.is_some_and(|scheduled| {
                    prepared.belongs_to_committed_transition(scheduled, schedule_fingerprint)
                });
                if committed {
                    true
                } else {
                    let source_incarnation = node_incarnations.get(&prepared.source);
                    let destination_incarnation = node_incarnations.get(&prepared.destination);
                    let participants_match = source_incarnation
                        == Some(&prepared.source_incarnation)
                        && destination_incarnation == Some(&prepared.destination_incarnation);
                    let source_still_owns_entity =
                        scheduled.and_then(ScheduledNode::primary_node) == Some(&prepared.source);
                    prepared.coordination.same_process_as(authority)
                        && participants_match
                        && prepared.base_schedule_fingerprint == schedule_fingerprint
                        && source_still_owns_entity
                        && self.entity_gate_operation_owns_entity(
                            &prepared.coordination,
                            &entity.domain,
                            &entity.node,
                            EntityGatePurpose::OwnershipHandoff,
                        )
                }
            } else {
                false
            };
            if committed_or_active {
                continue;
            }
            self.discard_prepared_ownership_handoff_state(
                &prepared.coordination,
                &prepared.operation_id,
                &entity.domain,
                &entity.node,
            )
            .map_err(|error| OwnershipHandoffError::persistence(error.current_context().clone()))?;
            discarded = discarded
                .checked_add(1)
                .assured("the discarded count cannot exceed the collected preparation count");
            debug!(
                domain = entity.domain.as_str(),
                kind = entity.kind().as_str(),
                name = entity.identifier().as_str(),
                coordination = %prepared.coordination,
                operation_id = prepared.operation_id,
                authority = %authority,
                "discarded an abandoned durable ownership handoff preparation"
            );
        }
        Ok(discarded)
    }
}
