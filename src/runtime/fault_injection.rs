//! What the injection boundary answers in a build without the test harness.
//!
//! Layer: data plane.
//!
//! - **Owns.** The zero-sized injector's answers: nothing is armed, nothing is paused, and every
//!   configured value is used as configured.
//! - **Depends on.** The marker the crate root stores in product structs.
//! - **Must not know.** The harness that replaces it in a test build.

#[cfg(not(feature = "testing"))]
use nervix_models::{ClusterNodeName, DomainName, EmitterName, IngestorName};
#[cfg(not(feature = "testing"))]
use tokio::time::Duration;

#[cfg(not(feature = "testing"))]
use crate::ConfiguredFaultInjection;

#[cfg(not(feature = "testing"))]
impl ConfiguredFaultInjection {
    pub(in crate::runtime) fn emitter_should_fail(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn emitter_should_stall(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn ingestor_is_failed(&self, _ingestor: &IngestorName) -> bool {
        false
    }

    pub(in crate::runtime) fn otel_client_is_unavailable(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn syslog_ingestor_bind_addr(
        &self,
        _node_id: &ClusterNodeName,
        configured: &str,
    ) -> String {
        configured.to_string()
    }

    pub(in crate::runtime) fn state_replica_polling_is_paused(&self) -> bool {
        false
    }

    pub(in crate::runtime) fn branch_instance_expiration_scan_interval(&self) -> Option<Duration> {
        None
    }

    pub(in crate::runtime) fn domain_drain_timeout(&self) -> Option<Duration> {
        None
    }

    pub(in crate::runtime) fn entity_gate_deadline(&self) -> Option<Duration> {
        None
    }

    pub(in crate::runtime) async fn pause_remote_relay_admission_if_armed(
        &self,
        _domain: &DomainName,
        _branch: Option<&str>,
    ) {
    }
}
