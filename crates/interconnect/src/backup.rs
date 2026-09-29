//! Typed node-to-node requests for a domain backup cut and restore state installation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The drain, capture, inventory, section fetch, and state install wire contracts and
//!   their pool, quota, deadline, and coordination identity declarations.
//! - **Depends on.** The authenticated request surface and typed vocabulary identities.
//! - **Must not know.** Archive encoding, owner staging, or how a restored graph runs.

use std::time::Duration;

use nervix_models::{CoordinationIdentity, DomainName};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    InterconnectRequest, InterconnectStreamRequest, PoolClass, RemoteOperationFailure,
    RequestSubquota, StatePlacementEnvelope,
};

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureDomainStateRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub revision: u64,
    pub quiesced: bool,
}

/// A node's complete admitted-work view during the backup's domain drain.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupDrainStatus {
    pub admitting_ingestors: u64,
    pub active_generators: u64,
    pub admitted_acks: u64,
    pub buffered_relay_batches: u64,
    pub node_work_items: u64,
    pub buffered_emitter_messages: u64,
    pub publishing_emitters: u64,
    pub parked_required_waits: u64,
    pub force_flush_obligations: u64,
}

impl BackupDrainStatus {
    /// Parked dependency waits and the confirming flush do not constitute admitted records.
    pub fn holds_admitted_work(&self) -> bool {
        self.admitting_ingestors != 0
            || self.active_generators != 0
            || self.admitted_acks != 0
            || self.buffered_relay_batches != 0
            || self.node_work_items != 0
            || self.buffered_emitter_messages != 0
            || self.publishing_emitters != 0
    }
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupDrainAction {
    FlushIfIdle,
    Confirm,
    Observe,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct BackupDrainStatusRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub action: BackupDrainAction,
}

impl InterconnectRequest for BackupDrainStatusRequest {
    type Response = Result<BackupDrainStatus, RemoteOperationFailure>;
    const NAME: &'static str = "backup_domain_drain_status";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Liveness;
    const TIMEOUT: Duration = Duration::from_secs(2);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for CaptureDomainStateRequest {
    type Response = Result<(), RemoteOperationFailure>;
    const NAME: &'static str = "backup_capture_domain_state";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureInventoryRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapturedSectionInventory {
    pub path: String,
    pub length: u64,
    pub digest: [u8; 32],
    pub kind: CapturedStateSectionKind,
}

#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapturedStateSectionKind {
    WasmDescriptor,
    KafkaOffsets,
    BranchLifecycle,
    WasmGuestBlob,
}

impl InterconnectRequest for CaptureInventoryRequest {
    type Response = Result<Vec<CapturedSectionInventory>, RemoteOperationFailure>;
    const NAME: &'static str = "backup_capture_inventory";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchCapturedSection {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub path: String,
}

impl InterconnectStreamRequest for FetchCapturedSection {
    const NAME: &'static str = "backup_fetch_captured_section";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct InstallRestoredStateRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub action: InstallRestoredStateAction,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum InstallRestoredStateAction {
    PurgeDomain,
    Begin {
        placement: StatePlacementEnvelope,
        branch_fingerprint: Option<[u8; 32]>,
        revision: u64,
        length: u64,
        digest: [u8; 32],
    },
    Chunk {
        offset: u64,
        payload: Vec<u8>,
    },
    Finish,
}

impl InterconnectRequest for InstallRestoredStateRequest {
    type Response = Result<(), RemoteOperationFailure>;
    const NAME: &'static str = "backup_install_restored_state";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[cfg(test)]
mod tests {
    use super::BackupDrainStatus;

    #[test]
    fn parked_waits_and_flush_completion_are_separate_from_admitted_work() {
        let mut status = BackupDrainStatus {
            admitting_ingestors: 0,
            active_generators: 0,
            admitted_acks: 0,
            buffered_relay_batches: 0,
            node_work_items: 0,
            buffered_emitter_messages: 0,
            publishing_emitters: 0,
            parked_required_waits: 2,
            force_flush_obligations: 1,
        };
        assert!(!status.holds_admitted_work());
        status.admitted_acks = 1;
        assert!(status.holds_admitted_work());
        status.admitted_acks = 0;
        status.node_work_items = 1;
        assert!(status.holds_admitted_work());
        status.node_work_items = 0;
        status.publishing_emitters = 1;
        assert!(status.holds_admitted_work());
    }
}
