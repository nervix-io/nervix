//! What a connector's host handle answers when it cannot hand a service over.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The typed refusal of a service a host did not provide to the connector that asked
//!   for it.
//! - **Depends on.** The vocabulary's connector kinds and typed reports.
//! - **Must not know.** How a host provides a service, which connector asks for it, or any driver.
//!
//! Every connector component reaches the node only through its host handle: an ingestor through
//! `IngestorHost`, an emitter through `EmitterHost`, a pooled client through `PooledClientHost` and
//! a partition inspector through `PartitionInspectorHost`. Every public method of a handle hands
//! over one named service and says so in a `nervix::host_service` source contract. The code that
//! calls one declares the services it reaches in a `nervix::host_services` contract on its own
//! module, type or function, and the compiler analysis refuses a call to a service the narrowest
//! such contract does not list. A plan therefore carries configuration only: no resolver,
//! executor, pool, policy or recorder travels in one.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::ConnectorKind;
use thiserror::Error;

/// Why a host handle could not hand a service to the connector that asked for it.
///
/// A connector takes its services while it opens or resumes, so a refusal fails that step as an
/// initialization failure of the connector and judges no record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HostServiceError {
    /// The node runs without its resolver, so nothing it hosts can resolve a name.
    #[error("the node DNS resolver is not installed")]
    DnsNotInstalled,
    /// The host leased no pooled client before it opened this emitter.
    #[error("the host leased no pooled client for this emitter")]
    PooledClientNotLeased,
    /// The pooled client the host leased belongs to another connector than the one asking.
    #[error(
        "the host leased a pooled {leased} client, and the emitter asked for a {requested} one"
    )]
    PooledClientOfOtherConnector {
        leased: ConnectorKind,
        requested: ConnectorKind,
    },
    /// The host keeps no domain-owned offsets for this ingestor.
    #[error("the host keeps no domain offsets for this ingestor")]
    DomainOffsetsNotKept,
    /// The signaling protocol the host compiled is not the representation the connector reads.
    #[error("the host's compiled signaling protocol is not one this connector reads")]
    SignalingProtocolOfOtherConnector,
}
