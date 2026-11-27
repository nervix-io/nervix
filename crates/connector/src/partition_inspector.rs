//! The handle a host gives the partition inspector of a connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The services a connector's partition inspector takes from its node.
//! - **Depends on.** The node's bounded executor and its committed outward TLS policy.
//! - **Must not know.** Which source is inspected, how its partitions are decided or published,
//!   who watches them, or any driver.
//!
//! A partition inspector asks a broker how a source's topic is split, so that the leader can deal
//! the topic's partitions before any instance of the source's ingestor reads them. It runs outside
//! every ingestor instance and therefore has no instance's handle to ask: the host hands it this
//! one instead. Like every other handle, each method hands over one named service, and the code
//! that calls it declares that service in a `nervix::host_services` contract.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_execution::Executor;
use nervix_tls::OutwardTlsPolicy;

/// What a partition inspector opens and asks through on its node.
#[cfg_attr(
    nervix_lint,
    nervix::value(
        host_handle,
        reason = "the handle a host gives the partition inspector it opens"
    )
)]
pub struct PartitionInspectorHost {
    executor: Executor,
    tls_policy: OutwardTlsPolicy,
}

impl PartitionInspectorHost {
    pub fn new(executor: Executor, tls_policy: OutwardTlsPolicy) -> Self {
        Self {
            executor,
            tls_policy,
        }
    }

    /// The node's bounded executor, through which an inspector admits its driver's synchronous
    /// work. An inspector keeps the handle it is given for as long as it runs work through it.
    #[cfg_attr(
        nervix_lint,
        nervix::host_service(executor, reason = "hands over the node's bounded executor")
    )]
    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    /// The node's committed outward TLS policy, which every TLS client an inspector builds
    /// captures for its handshakes. It is the node's own publication, so a client built from it
    /// follows a later commit.
    #[cfg_attr(
        nervix_lint,
        nervix::host_service(
            tls_policy,
            reason = "hands over the node's committed outward TLS policy"
        )
    )]
    pub fn tls_policy(&self) -> &OutwardTlsPolicy {
        &self.tls_policy
    }
}
