//! Why the node refused to start or run something.
//!
//! Layer: data plane.
//!
//! - **Owns.** The one error the runtime's entry points return.
//! - **Depends on.** The vocabulary its variants quote and typed engine failures it retains.
//! - **Must not know.** How a caller reports or recovers from a failure.

use super::*;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("ingestor '{ingestor}' in domain '{domain}' is not running")]
    IngestorNotRunning { domain: String, ingestor: String },
    /// An ingestor did not start. The report names the ingestor and keeps every cause beneath it.
    #[error("{report:#}")]
    IngestorStart { report: Report<IngestorStartError> },
    #[error("relay '{relay}' in domain '{domain}' is not instantiated")]
    RelayNotInstantiated { domain: String, relay: String },
    #[error(
        "relay '{relay}' in domain '{domain}' was redefined after its definition was read; read \
         it again"
    )]
    RelayRedefined {
        domain: DomainName,
        relay: RelayName,
    },
    #[error("failed to build domain execution for '{domain}': {reason}")]
    BuildDomainExecution { domain: String, reason: String },
    #[error("failed to build domain execution for '{domain}': {report}")]
    SignalingProtocolCompile {
        domain: DomainName,
        report: Report<nervix_connector_websockets::SignalingProtocolCompileError>,
    },
    /// A codec of the domain did not compile. The report keeps the rule or declaration the codec
    /// breaks beneath the codec's own context.
    #[error("failed to build domain execution for '{domain}': {report:#}")]
    CodecCompile {
        domain: DomainName,
        report: Report<CodecError>,
    },
    #[error("failed to build domain execution for '{domain}': {reason}")]
    VmCompile {
        domain: String,
        reason: String,
        report: Report<nervix_vm::CompileError>,
    },
    #[error(
        "failed to build domain execution for '{domain}': failed to compile domain UDFs: {report}"
    )]
    CompileDomainUdfs {
        domain: String,
        report: Report<nervix_roto::UdfError>,
    },
    #[error("failed to bind an ingestor or reingestor of domain '{domain}': {report}")]
    EntrypointBinding {
        domain: DomainName,
        report: Report<EntrypointBindingError>,
    },
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
    #[error("could not own remote acknowledgements in domain '{domain}': {report:#}")]
    RemoteAckAdmission {
        domain: DomainName,
        report: Report<nervix_execution::AdmissionError>,
    },
    /// A relay payload another node sent did not decode into a batch of its relay. The report
    /// names what the payload got wrong and keeps the decoder's own failure beneath it.
    #[error("failed to decode remote relay '{relay}' in domain '{domain}': {report:#}")]
    DecodeRemoteRelay {
        domain: DomainName,
        relay: RelayName,
        report: Report<RemoteRelayDecodeError>,
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

impl RuntimeError {
    pub(super) fn entrypoint_binding(
        domain: &DomainName,
        report: Report<EntrypointBindingError>,
    ) -> Self {
        Self::EntrypointBinding {
            domain: domain.clone(),
            report,
        }
    }

    pub(super) fn decode_remote_relay(
        domain: &DomainName,
        relay: &RelayName,
        report: Report<RemoteRelayDecodeError>,
    ) -> Self {
        Self::DecodeRemoteRelay {
            domain: domain.clone(),
            relay: relay.clone(),
            report,
        }
    }
}
