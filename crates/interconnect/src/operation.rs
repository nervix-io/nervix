//! Why one node could not answer another node's control operation.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The four classes a refused control operation reports, and the subject each one names.
//! - **Depends on.** The vocabulary the subjects quote and the runtime state kinds they address.
//! - **Must not know.** Which operation asked, or what the asking node does about the answer.

use std::fmt;

use nervix_models::{ClusterNodeIdentity, DomainName, ModelKind, ModelName, NodeRef, RelayName};
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

use crate::{RuntimeStateKind, StatePlacementEnvelope};

/// What a remote control operation was asked about.
///
/// The subject travels with every failure, so the asking node names what failed without holding on
/// to the request it sent.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum RemoteOperationSubject {
    /// A whole domain, for operations that span every entity it owns.
    Domain { domain: DomainName },
    /// One entity of a domain's execution graph.
    Entity { domain: DomainName, entity: NodeRef },
    /// One kind of runtime state held for one entity.
    State {
        domain: DomainName,
        entity: NodeRef,
        state: RuntimeStateKind,
    },
    /// One subscriber's interest in one relay.
    SubscriptionInterest {
        domain: DomainName,
        relay: RelayName,
        subscriber: ClusterNodeIdentity,
    },
}

impl RemoteOperationSubject {
    /// Everything `domain` owns.
    pub fn domain(domain: &DomainName) -> Self {
        Self::Domain {
            domain: domain.clone(),
        }
    }

    /// The entity of `kind` named `identifier` inside `domain`.
    pub fn entity(domain: &DomainName, kind: ModelKind, identifier: impl Into<ModelName>) -> Self {
        Self::Entity {
            domain: domain.clone(),
            entity: NodeRef::new(kind, identifier),
        }
    }

    /// The runtime state one placement addresses.
    pub fn state(placement: &StatePlacementEnvelope) -> Self {
        Self::State {
            domain: placement.domain.clone(),
            entity: NodeRef::new(placement.kind, placement.identifier.clone()),
            state: placement.state,
        }
    }

    /// One subscriber's interest in one relay.
    pub fn subscription_interest(
        domain: &DomainName,
        relay: &RelayName,
        subscriber: &ClusterNodeIdentity,
    ) -> Self {
        Self::SubscriptionInterest {
            domain: domain.clone(),
            relay: relay.clone(),
            subscriber: subscriber.clone(),
        }
    }
}

impl fmt::Display for RemoteOperationSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain { domain } => write!(formatter, "domain '{domain}'"),
            Self::Entity { domain, entity } => write!(
                formatter,
                "{} '{}' in domain '{domain}'",
                entity.kind.as_str(),
                entity.identifier.as_str()
            ),
            Self::State {
                domain,
                entity,
                state,
            } => write!(
                formatter,
                "{} state of {} '{}' in domain '{domain}'",
                state.as_str(),
                entity.kind.as_str(),
                entity.identifier.as_str()
            ),
            Self::SubscriptionInterest {
                domain,
                relay,
                subscriber,
            } => write!(
                formatter,
                "subscription interest of node '{subscriber}' in relay '{relay}' in domain \
                 '{domain}'"
            ),
        }
    }
}

/// Why a node could not answer a control operation.
///
/// The class is what the asking node acts on. A rejection means this node is not the one to ask, an
/// unavailable subject is not present here at all, a not-ready subject is present but cannot answer
/// yet, and a failure ran the operation and lost. Only `Failed` carries the answering node's own
/// description, because a failure inside another node's subsystem is opaque to the caller and the
/// text is for the operator reading it; every other class is fully described by its typed fields.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq, Error)]
pub enum RemoteOperationFailure {
    #[error("{subject} is not served by the node that answered")]
    Rejected { subject: RemoteOperationSubject },
    #[error("{subject} does not exist")]
    Unavailable { subject: RemoteOperationSubject },
    #[error("{subject} is not ready")]
    NotReady { subject: RemoteOperationSubject },
    #[error("{subject} failed: {reason}")]
    Failed {
        subject: RemoteOperationSubject,
        reason: String,
    },
}

impl RemoteOperationFailure {
    /// The answering node does not serve `subject` at all.
    pub fn rejected(subject: RemoteOperationSubject) -> Self {
        Self::Rejected { subject }
    }

    /// `subject` does not exist on the answering node.
    pub fn unavailable(subject: RemoteOperationSubject) -> Self {
        Self::Unavailable { subject }
    }

    /// `subject` exists on the answering node but cannot answer yet.
    pub fn not_ready(subject: RemoteOperationSubject) -> Self {
        Self::NotReady { subject }
    }

    /// The operation ran on the answering node and lost.
    pub fn failed(subject: RemoteOperationSubject, reason: impl Into<String>) -> Self {
        Self::Failed {
            subject,
            reason: reason.into(),
        }
    }
}
