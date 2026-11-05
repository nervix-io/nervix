//! What the injection boundary answers in a build without the test harness.
//!
//! Layer: data plane.
//!
//! - **Owns.** The zero-sized injector's answers: nothing is armed, nothing is paused, and every
//!   configured value is used as configured.
//! - **Depends on.** The marker the crate root stores in product structs.
//! - **Must not know.** The harness that replaces it in a test build.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

#[cfg(not(feature = "testing"))]
use std::time::Duration;

#[cfg(not(feature = "testing"))]
use nervix_models::{
    ClusterNodeName, DomainName, DomainNodeRef, EmitterName, IngestorName, ModelKind, ModelName,
    RelayName, Unit,
};

#[cfg(not(feature = "testing"))]
use crate::ConfiguredFaultInjection;

#[cfg(not(feature = "testing"))]
impl ConfiguredFaultInjection {
    pub(in crate::runtime) fn wasm_reset_publication_failure_is_enabled(
        &self,
        _domain: &DomainName,
    ) -> bool {
        false
    }

    pub(in crate::runtime) fn deliver_wasm_reset_publication_failure(&self, _domain: &DomainName) {}

    pub(in crate::runtime) fn take_wasm_reset_publication_authority_loss(
        &self,
        _domain: &DomainName,
    ) -> bool {
        false
    }

    pub(in crate::runtime) async fn pause_ingestor_route_poll_if_armed(
        &self,
        _domain: &DomainName,
        _kind: ModelKind,
        _source: &RelayName,
    ) {
    }

    pub(in crate::runtime) async fn pause_ownership_handoff_flush_if_armed(
        &self,
        _domain: &DomainName,
    ) {
    }

    pub(in crate::runtime) fn emitter_should_fail(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn emitter_should_stall(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn ingestor_is_failed(&self, _ingestor: &IngestorName) -> bool {
        false
    }

    pub(in crate::runtime) fn source_confirmation_delay(
        &self,
        _ingestor: &DomainNodeRef,
    ) -> Option<Duration> {
        None
    }

    pub(in crate::runtime) fn stopped_state_write_delay(
        &self,
        _domain: &DomainName,
        _kind: ModelKind,
        _entity: &ModelName,
    ) -> Option<Duration> {
        None
    }

    pub(in crate::runtime) async fn delay_materialized_dependency_read_if_armed(
        &self,
        _domain: &DomainName,
        _kind: ModelKind,
        _processor: &ModelName,
    ) {
    }

    pub(in crate::runtime) fn record_materialized_wait_if_armed(
        &self,
        _domain: &DomainName,
        _kind: ModelKind,
        _processor: &ModelName,
    ) {
    }

    pub(in crate::runtime) async fn hold_materialized_snapshot_fetch_if_armed(
        &self,
        _node_id: &ClusterNodeName,
    ) {
    }

    pub(in crate::runtime) fn emitter_client_is_unavailable(&self, _emitter: &EmitterName) -> bool {
        false
    }

    pub(in crate::runtime) fn take_emitter_connector_stall(
        &self,
        _emitter: &EmitterName,
    ) -> Option<usize> {
        None
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

    pub(in crate::runtime) fn state_checkpoint_announcements_are_lost(&self) -> bool {
        false
    }

    pub(in crate::runtime) fn wasm_checkpoint_storage_fails(&self) -> bool {
        false
    }

    pub(in crate::runtime) fn wasm_state_reset_fresh_initialization_fails(&self) -> bool {
        false
    }

    pub(in crate::runtime) fn state_replica_installation_fails(&self) -> bool {
        false
    }

    pub(in crate::runtime) fn native_metadata_capture_interruption(
        &self,
        _domain: &DomainName,
        _entity: &ModelName,
    ) -> Option<u64> {
        None
    }

    pub(in crate::runtime) fn guest_save_capture_interruption(
        &self,
        _domain: &DomainName,
        _entity: &ModelName,
    ) -> Option<u64> {
        None
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

    pub(in crate::runtime) async fn pause_remote_relay_dispatch_if_armed(
        &self,
        _domain: &DomainName,
    ) {
    }

    pub(in crate::runtime) fn loses_remote_acknowledgement(
        &self,
        _resolver: &ClusterNodeName,
        _registrar: &ClusterNodeName,
    ) -> bool {
        false
    }

    pub(in crate::runtime) fn misses_live_run(
        &self,
        _observer: &ClusterNodeName,
        _missing: &ClusterNodeName,
    ) -> bool {
        false
    }

    pub(in crate::runtime) fn fill_relay_memory_before_acknowledgement_watches_if_armed(
        &self,
        _node_id: &ClusterNodeName,
        _executor: &nervix_execution::Executor,
    ) {
    }

    pub(in crate::runtime) fn fill_relay_memory_before_record_storage_if_armed(
        &self,
        _node_id: &ClusterNodeName,
        _executor: &nervix_execution::Executor,
    ) {
    }

    pub(in crate::runtime) fn register_relay_memory_owner(
        &self,
        _node_id: &ClusterNodeName,
        _executor: &nervix_execution::Executor,
    ) {
    }

    pub(in crate::runtime) async fn pause_owner_relay_fanout_if_armed(&self, _domain: &DomainName) {
    }

    pub(in crate::runtime) async fn pause_owner_relay_partition_fanout_if_armed(
        &self,
        _domain: &DomainName,
        _relay: &RelayName,
        _unit: Unit,
    ) {
    }

    pub(in crate::runtime) fn mark_owner_relay_fanout_complete(&self, _domain: &DomainName) {}
}
