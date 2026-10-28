//! Backup capture messages through the bounded production wire codec.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current capture, drain, inventory and fetch cases and full-value oracles.
//! - **Depends on.** The backup wire contracts, typed identities and the bounded executor.
//! - **Must not know.** Archive storage, runtime capture fencing or transport connections.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::fmt::Debug;

use meticulous::ResultExt as _;
use nervix_execution::Executor;
use nervix_models::ClusterNodeName;

use super::*;
use crate::{RemoteOperationSubject, request::RkyvMessage};

#[derive(Debug, bolero::TypeGenerator)]
struct CaptureWireCase {
    node: u16,
    epoch: u64,
    fence: u64,
    revision: u64,
    quiesced: bool,
    counters: [u64; 10],
    length: u64,
    digest: [u8; 32],
    path_chars: [char; 16],
    path_length: u8,
}

impl CaptureWireCase {
    fn coordination(&self) -> CoordinationIdentity {
        let node = ClusterNodeName::parse(&format!("node-{}", self.node))
            .assured("a generated decimal suffix makes a valid node name");
        CoordinationIdentity::new(node, self.epoch.max(1), self.fence)
    }

    fn domain(&self) -> DomainName {
        DomainName::parse(&format!("backup_{}", self.node))
            .assured("a generated decimal suffix makes a valid domain name")
    }

    fn path(&self) -> String {
        let length = usize::from(self.path_length % 17);
        let suffix: String = self.path_chars[..length]
            .iter()
            .map(|character| match character {
                '\0' | '/' | '\\' => 'x',
                character => *character,
            })
            .collect();
        format!("domains/备份/é🦀/section{suffix}")
    }

    fn failures(&self) -> [RemoteOperationFailure; 4] {
        let subject = RemoteOperationSubject::domain(&self.domain());
        [
            RemoteOperationFailure::rejected(subject.clone()),
            RemoteOperationFailure::unavailable(subject.clone()),
            RemoteOperationFailure::not_ready(subject.clone()),
            RemoteOperationFailure::failed(subject, self.path()),
        ]
    }

    fn inventory(&self) -> Vec<CapturedSectionInventory> {
        let mut inventory = Vec::new();
        for kind in SECTION_KINDS {
            for length in [0, u64::MAX, self.length] {
                inventory.push(CapturedSectionInventory {
                    path: format!("{}/{kind:?}/{length}", self.path()),
                    length,
                    digest: self.digest,
                    kind,
                });
            }
        }
        inventory
    }

    fn drain_status(counters: [u64; 10]) -> BackupDrainStatus {
        BackupDrainStatus {
            admitting_ingestors: counters[0],
            active_generators: counters[1],
            admitted_acks: counters[2],
            admitting_relays: counters[3],
            buffered_relay_batches: counters[4],
            node_work_items: counters[5],
            buffered_emitter_messages: counters[6],
            publishing_emitters: counters[7],
            parked_required_waits: counters[8],
            force_flush_obligations: counters[9],
        }
    }

    fn capture(&self) -> CaptureDomainStateRequest {
        CaptureDomainStateRequest {
            coordination: self.coordination(),
            domain: self.domain(),
            revision: self.revision,
            quiesced: self.quiesced,
        }
    }

    fn drain(&self, action: BackupDrainAction) -> BackupDrainStatusRequest {
        BackupDrainStatusRequest {
            coordination: self.coordination(),
            domain: self.domain(),
            action,
        }
    }

    fn inventory_request(&self) -> CaptureInventoryRequest {
        CaptureInventoryRequest {
            coordination: self.coordination(),
            domain: self.domain(),
        }
    }

    fn fetch(&self) -> FetchCapturedSection {
        FetchCapturedSection {
            coordination: self.coordination(),
            domain: self.domain(),
            path: self.path(),
        }
    }
}

const SECTION_KINDS: [CapturedStateSectionKind; 12] = [
    CapturedStateSectionKind::WasmDescriptor,
    CapturedStateSectionKind::KafkaOffsets,
    CapturedStateSectionKind::BranchLifecycle,
    CapturedStateSectionKind::WasmGuestBlob,
    CapturedStateSectionKind::MaterializedDescriptor,
    CapturedStateSectionKind::MaterializedIdentities,
    CapturedStateSectionKind::MaterializedColumns,
    CapturedStateSectionKind::DeduplicatorDescriptor,
    CapturedStateSectionKind::DeduplicatorKeys,
    CapturedStateSectionKind::WindowDescriptor,
    CapturedStateSectionKind::WindowInputRows,
    CapturedStateSectionKind::WindowArgumentColumns,
];

pub(super) async fn assert_round_trip<M>(executor: &Executor, class: PoolClass, message: M)
where
    M: RkyvMessage + Clone + Debug + PartialEq,
{
    let (bytes, charge) = message
        .clone()
        .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
        .await
        .assured("the bounded current message fits its declared class limit");
    let (decoded, decoded_charge) = M::decode_rkyv(executor.clone(), class, bytes)
        .await
        .assured("the production encoder's current message decodes");
    assert_eq!(decoded, message);
    drop(decoded);
    drop(decoded_charge);
    drop(charge);
    assert_charges_released(executor);
}

fn assert_charges_released(executor: &Executor) {
    let snapshot = executor.snapshot();
    assert_eq!(snapshot.management_memory.reserved_bytes, 0);
    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
}

fn property_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the ordinary property runtime builds")
}

#[test]
fn bolero_backup_capture_messages_round_trip() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .with_type::<CaptureWireCase>()
        .for_each(|case| {
            runtime.block_on(async {
                let executor = Executor::default();
                let class = CaptureDomainStateRequest::CLASS;
                for revision in [0, u64::MAX, case.revision] {
                    nervix_primitives::task::consume_budget().await;
                    let mut request = case.capture();
                    request.revision = revision;
                    assert_round_trip(&executor, class, request).await;
                }
                assert_round_trip(&executor, class, Ok::<(), RemoteOperationFailure>(())).await;
                for action in [
                    BackupDrainAction::FlushHeldWork,
                    BackupDrainAction::Confirm,
                    BackupDrainAction::Observe,
                ] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        case.drain(action),
                    )
                    .await;
                }
                for counters in [[0; 10], [u64::MAX; 10], case.counters] {
                    nervix_primitives::task::consume_budget().await;
                    let status = CaptureWireCase::drain_status(counters);
                    assert_round_trip(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        Ok::<_, RemoteOperationFailure>(status),
                    )
                    .await;
                }
                assert_round_trip(
                    &executor,
                    CaptureInventoryRequest::CLASS,
                    case.inventory_request(),
                )
                .await;
                for inventory in [Vec::new(), case.inventory()] {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(
                        &executor,
                        CaptureInventoryRequest::CLASS,
                        Ok::<_, RemoteOperationFailure>(inventory),
                    )
                    .await;
                }
                for failure in case.failures() {
                    nervix_primitives::task::consume_budget().await;
                    assert_round_trip(&executor, class, Err::<(), _>(failure.clone())).await;
                    assert_round_trip(
                        &executor,
                        BackupDrainStatusRequest::CLASS,
                        Err::<BackupDrainStatus, _>(failure.clone()),
                    )
                    .await;
                    assert_round_trip(
                        &executor,
                        CaptureInventoryRequest::CLASS,
                        Err::<Vec<CapturedSectionInventory>, _>(failure),
                    )
                    .await;
                }
                assert_round_trip(&executor, FetchCapturedSection::CLASS, case.fetch()).await;
            });
        });
}

mod malformed;
