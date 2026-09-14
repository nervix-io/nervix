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
