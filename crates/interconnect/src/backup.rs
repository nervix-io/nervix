//! Typed node-to-node requests for a domain backup cut and restore state installation.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The drain, capture, inventory, section fetch, and state install wire contracts and
//!   their pool, quota, deadline, and coordination identity declarations.
//! - **Depends on.** The authenticated request surface and typed vocabulary identities.
//! - **Must not know.** Archive encoding, owner staging, or how a restored graph runs.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::Duration;

use nervix_models::{CoordinationIdentity, DomainName, RestoreStateAuthority};
use rkyv::{Archive, Deserialize, Serialize};

/// The complete checkpoint set a receiving node must validate before replacing domain state.
#[derive(Debug, Clone, Copy, Default, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestoreStateInventory {
    pub checkpoints: u64,
    pub payload_bytes: u64,
}

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
    /// Relays that admitted a batch while the node read this view, so work may have reached a
    /// count after the read that would have counted it.
    pub admitting_relays: u64,
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
            || self.admitting_relays != 0
            || self.buffered_relay_batches != 0
            || self.node_work_items != 0
            || self.buffered_emitter_messages != 0
            || self.publishing_emitters != 0
    }
}

/// What a node does for the backup's domain drain before it reports its view.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackupDrainAction {
    /// Release the admitted work the node still holds: while it holds any, the node starts a
    /// force-flush generation unless one is already running. A node that holds none starts none.
    FlushHeldWork,
    /// Start the force-flush generation that confirms nothing is still moving.
    Confirm,
    /// Report the view without starting a generation.
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
    MaterializedDescriptor,
    MaterializedIdentities,
    MaterializedColumns,
    DeduplicatorDescriptor,
    DeduplicatorKeys,
    WindowDescriptor,
    WindowInputRows,
    WindowArgumentColumns,
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
    /// The bulk pool's shared stream and admission rather than the Snapshot subquota. A quiesced
    /// cut publishes new checkpoint revisions, and a replica catching up one above the bulk budget
    /// holds the peer connection's one Snapshot stream for as long as its transfer takes, which
    /// would otherwise keep the coordinator from opening a completed cut's sections.
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    const TIMEOUT: Duration = Duration::from_secs(30);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct InstallRestoredStateRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub authority: RestoreStateAuthority,
    pub action: InstallRestoredStateAction,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum InstallRestoredStateAction {
    Publish {
        inventory: RestoreStateInventory,
    },
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

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod capture_properties;

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod wire_properties {
    use meticulous::ResultExt as _;
    use nervix_execution::Executor;
    use nervix_models::{
        ClusterNodeName, CommandExecutionReference, ModelKind, ModelName, RemoteRuntimeField,
        RemoteRuntimeValue, SchemaFingerprint, WasmStateGeneration,
    };

    use super::{capture_properties::assert_round_trip, *};
    use crate::{RemoteOperationSubject, RuntimeState};

    #[derive(Debug, bolero::TypeGenerator)]
    struct RestoreWireCase {
        term: u64,
        lease: u64,
        generation: u64,
        revision: u64,
        value: i64,
        byte: u8,
        length: u8,
        declared_length: u64,
        digest: [u8; 32],
    }

    #[test]
    fn bolero_restore_installation_requests_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the ordinary test runtime builds");
        bolero::check!()
            .with_iterations(128)
            .with_max_len(128)
            .with_type::<RestoreWireCase>()
            .for_each(|case| {
                runtime.block_on(async {
                    let executor = Executor::default();
                    let leader = ClusterNodeName::parse(&format!("node-{}", case.byte))
                        .assured("the generated node name is valid");
                    let domain = DomainName::parse("orders").assured("the domain is valid");
                    let authority = RestoreStateAuthority {
                        leader: leader.clone(),
                        term: case.term.max(1),
                        execution: CommandExecutionReference::parse(format!(
                            "restore-{}",
                            case.generation
                        ))
                        .assured("the generated reference is valid"),
                        mutation_revision: case.lease.max(1),
                        generation: case.generation.max(1),
                    };
                    let json = serde_json::to_vec(&authority).assured("the authority encodes");
                    assert_eq!(
                        serde_json::from_slice::<RestoreStateAuthority>(&json)
                            .assured("the authority decodes"),
                        authority
                    );
                    let schema = SchemaFingerprint::from_digest(case.digest);
                    let states = [
                        RuntimeState::BranchAggregated,
                        RuntimeState::Correlator { schema },
                        RuntimeState::Deduplicator { schema },
                        RuntimeState::KafkaOffset,
                        RuntimeState::MaterializedRelay { schema },
                        RuntimeState::WasmProcessor {
                            schema,
                            generation: WasmStateGeneration::try_from(case.generation.max(1))
                                .assured("the generated state generation is positive"),
                        },
                        RuntimeState::WindowProcessor { schema },
                        RuntimeState::BranchLru { schema },
                    ];
                    let mut actions = Vec::new();
                    for state in states {
                        nervix_primitives::task::consume_budget().await;
                        let kind = match state {
                            RuntimeState::BranchAggregated
                            | RuntimeState::MaterializedRelay { .. }
                            | RuntimeState::BranchLru { .. } => ModelKind::Relay,
                            RuntimeState::Correlator { .. } => ModelKind::Correlator,
                            RuntimeState::Deduplicator { .. } => ModelKind::Deduplicator,
                            RuntimeState::KafkaOffset => ModelKind::Ingestor,
                            RuntimeState::WasmProcessor { .. } => ModelKind::WasmProcessor,
                            RuntimeState::WindowProcessor { .. } => ModelKind::WindowProcessor,
                        };
                        for branched in [false, true] {
                            let branch_key = branched.then(|| {
                                vec![RemoteRuntimeField {
                                    name: "tenant".to_string(),
                                    value: RemoteRuntimeValue::I64(case.value),
                                }]
                            });
                            for length in [0, u64::MAX, case.declared_length] {
                                actions.push(InstallRestoredStateAction::Begin {
                                    placement: StatePlacementEnvelope {
                                        domain: domain.clone(),
                                        state,
                                        kind,
                                        identifier: ModelName::parse("accumulator")
                                            .assured("the processor name is valid"),
                                        unit: nervix_models::Unit::Single,
                                        branch_key: branch_key.clone(),
                                    },
                                    branch_fingerprint: branched.then_some(case.digest),
                                    revision: case.revision,
                                    length,
                                    digest: case.digest,
                                });
                            }
                        }
                    }
                    actions.extend([
                        InstallRestoredStateAction::Chunk {
                            offset: case.revision,
                            payload: vec![case.byte; usize::from(case.length)],
                        },
                        InstallRestoredStateAction::Finish,
                        InstallRestoredStateAction::Publish {
                            inventory: RestoreStateInventory {
                                checkpoints: case.revision,
                                payload_bytes: u64::from(case.length),
                            },
                        },
                    ]);
                    for action in actions {
                        nervix_primitives::task::consume_budget().await;
                        let request = InstallRestoredStateRequest {
                            coordination: CoordinationIdentity::new(
                                leader.clone(),
                                case.term.max(1),
                                case.revision,
                            ),
                            domain: domain.clone(),
                            authority: authority.clone(),
                            action,
                        };
                        assert_round_trip(&executor, InstallRestoredStateRequest::CLASS, request)
                            .await;
                    }
                    let class = InstallRestoredStateRequest::CLASS;
                    assert_round_trip(&executor, class, Ok::<(), RemoteOperationFailure>(())).await;
                    let subject = RemoteOperationSubject::domain(&domain);
                    for failure in [
                        RemoteOperationFailure::rejected(subject.clone()),
                        RemoteOperationFailure::unavailable(subject.clone()),
                        RemoteOperationFailure::not_ready(subject.clone()),
                        RemoteOperationFailure::failed(subject, format!("restore-{}", case.value)),
                    ] {
                        nervix_primitives::task::consume_budget().await;
                        assert_round_trip(&executor, class, Err::<(), _>(failure)).await;
                    }
                });
            });
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
            admitting_relays: 0,
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
        status.admitting_relays = 1;
        assert!(status.holds_admitted_work());
        status.admitting_relays = 0;
        status.node_work_items = 1;
        assert!(status.holds_admitted_work());
        status.node_work_items = 0;
        status.publishing_emitters = 1;
        assert!(status.holds_admitted_work());
    }
}
