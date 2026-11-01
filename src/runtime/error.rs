//! Why the node refused to start or run something.
//!
//! Layer: data plane.
//!
//! - **Owns.** The one error the runtime's entry points return.
//! - **Depends on.** The vocabulary its variants quote and typed engine failures it retains.
//! - **Must not know.** How a caller reports or recovers from a failure.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

/// Why a runtime entry point failed. Each variant is a context of an `error_stack` report, and the
/// failure that caused it, such as the [`ExecutionBuildError`] step of a domain build or an
/// ingestor's, emitter's or generator's start report, stays beneath it.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[cfg(feature = "testing")]
    #[error("the armed test refused runtime installation on node '{node}'")]
    RuntimeInstallationRefused { node: ClusterNodeName },
    #[error("failed to retire the source of ingestor '{ingestor}' in domain '{domain}'")]
    RetireIngestorEntity {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("ingestor '{ingestor}' in domain '{domain}' is not running")]
    IngestorNotRunning {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("relay '{relay}' in domain '{domain}' is not instantiated")]
    RelayNotInstantiated {
        domain: DomainName,
        relay: RelayName,
    },
    #[error(
        "relay '{relay}' in domain '{domain}' was redefined after its definition was read; read \
         it again"
    )]
    RelayRedefined {
        domain: DomainName,
        relay: RelayName,
    },
    /// Building or changing the execution of a domain failed. The step that failed is beneath.
    #[error("failed to build domain execution for '{domain}'")]
    BuildDomainExecution { domain: DomainName },
    /// The committed schedule revision could not be planned against the one applied before it.
    #[error("failed to plan the committed schedule revision")]
    PlanScheduleRevision,
    /// This node could not apply the admitted runtime revision. Its failure is beneath.
    #[error("failed to apply runtime revision {revision}")]
    ApplyRevision { revision: u64 },
    /// The ingestors of running domains did not start once the revision was prepared. The
    /// ingestor's start report is beneath.
    #[error("failed to start the ingestors of runtime revision {revision}")]
    StartIngestors { revision: u64 },
    #[error(
        "timed out waiting for runtime revision {revision} to be prepared on nodes \
         {pending_nodes:?}"
    )]
    RuntimeRevisionPreparation {
        revision: u64,
        pending_nodes: Vec<ClusterNodeName>,
    },
    #[error(
        "timed out waiting for runtime revision {revision} to become ready on nodes \
         {pending_nodes:?}"
    )]
    RuntimeRevisionReadiness {
        revision: u64,
        pending_nodes: Vec<ClusterNodeName>,
    },
    #[error(
        "cannot represent a runtime revision readiness deadline from node-unavailability timeout \
         {node_unavailability_timeout:?} and readiness propagation bound \
         {readiness_propagation_bound:?}"
    )]
    RuntimeRevisionReadinessDeadlineOverflow {
        node_unavailability_timeout: Duration,
        readiness_propagation_bound: Duration,
    },
    /// The deadline of the application's caller ended the wait it names. A local installation or
    /// activation the application had begun finished before the wait, so this node holds no part
    /// of a revision; the nodes keep applying the newest committed state on their own.
    #[error("the caller's deadline ended the runtime application while it waited for {wait}")]
    ApplicationDeadline { wait: ApplicationWait },
    /// The relay budget refused the acknowledgement watches of a batch another node delivered: they
    /// exceed the whole budget, or it had no room for them within the sender's admission bound. The
    /// execution admission refusal is beneath.
    #[error("could not own remote acknowledgements in domain '{domain}'")]
    RemoteAckAdmission { domain: DomainName },
    /// A relay payload another node sent did not decode into a batch of its relay. The
    /// `RemoteRelayDecodeError` beneath it names what the payload got wrong and keeps the
    /// decoder's own failure beneath that.
    #[error("failed to decode remote relay '{relay}' in domain '{domain}'")]
    DecodeRemoteRelay {
        domain: DomainName,
        relay: RelayName,
    },
    /// The local boundary of a relay refused a batch another node sent, after it was decoded.
    #[error(
        "failed to dispatch remote relay '{relay}' in domain '{domain}': the local relay boundary \
         rejected the batch"
    )]
    DispatchRemoteRelay {
        domain: DomainName,
        relay: RelayName,
    },
}

/// The wait of an application of the committed runtime state that its caller's deadline ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum ApplicationWait {
    /// For this node's next installation slot, before it read the state to install.
    #[strum(to_string = "an installation slot")]
    Installation,
    /// For every live node to prepare `revision`, which this node had prepared.
    #[strum(to_string = "every live node to prepare runtime revision {revision}")]
    Preparation { revision: u64 },
    /// For this node's installation slot to activate `revision`.
    #[strum(to_string = "the installation slot to activate runtime revision {revision}")]
    Activation { revision: u64 },
    /// For every live node to ready `revision`, which this node had readied.
    #[strum(to_string = "every live node to ready runtime revision {revision}")]
    Readiness { revision: u64 },
}

impl RuntimeError {
    /// The context of every failure to build or change `domain`'s execution.
    pub(super) fn build_domain_execution(domain: &DomainName) -> Self {
        Self::BuildDomainExecution {
            domain: domain.clone(),
        }
    }

    pub(super) fn decode_remote_relay(domain: &DomainName, relay: &RelayName) -> Self {
        Self::DecodeRemoteRelay {
            domain: domain.clone(),
            relay: relay.clone(),
        }
    }

    pub(super) fn relay_not_instantiated(domain: &DomainName, relay: &RelayName) -> Self {
        Self::RelayNotInstantiated {
            domain: domain.clone(),
            relay: relay.clone(),
        }
    }
}
