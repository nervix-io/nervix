//! Why the node refused to start or run something.
//!
//! Layer: data plane.
//!
//! - **Owns.** The one error the runtime's entry points return.
//! - **Depends on.** The vocabulary its variants quote.
//! - **Must not know.** How a caller reports or recovers from a failure.

use super::*;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("ingestor '{ingestor}' in domain '{domain}' is already running")]
    IngestorAlreadyRunning { domain: String, ingestor: String },
    #[error("ingestor '{ingestor}' in domain '{domain}' is not running")]
    IngestorNotRunning { domain: String, ingestor: String },
    #[error("failed to initialize ingestor '{ingestor}' in domain '{domain}': {reason}")]
    StartIngestor {
        domain: String,
        ingestor: String,
        reason: String,
    },
    #[error("codec '{codec}' in domain '{domain}' is not instantiated")]
    CodecNotInstantiated { domain: String, codec: String },
    #[error("relay '{relay}' in domain '{domain}' is not instantiated")]
    RelayNotInstantiated { domain: String, relay: String },
    #[error("failed to build domain execution for '{domain}': {reason}")]
    BuildDomainExecution { domain: String, reason: String },
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
    #[error("failed to decode remote relay '{relay}' in domain '{domain}': {reason}")]
    DecodeRemoteRelay {
        domain: String,
        relay: String,
        reason: String,
    },
}
