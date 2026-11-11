//! The requests through which the units of a branch-keyed entity move the keys a branch's previous
//! partition count placed elsewhere, and through which a node reports how far that move is.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The subject of a claim between two units of one entity, the claim exchange (a
//!   release followed by a forget) with its typed outcomes, the catalog shard a claimant reads from
//!   each unit of its entity, and the holdings a node reports for a branch's transition.
//! - **Depends on.** The vocabulary a unit, a key and an epoch name, the runtime state envelopes,
//!   and the typed request contract.
//! - **Must not know.** How a runtime parks input, stops a branch, stores the state it moves or
//!   decides that a transition retires.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::{
    BranchKeyFingerprint, BranchName, DomainName, NodeRef, PartitionEpochNumber,
    RemoteRuntimeRecordMetadata, Timestamp, Unit,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    InterconnectRequest, RemoteOperationFailure, RequestDeadline, RequestSubquota,
    runtime_state::{StatePlacementEnvelope, StateSnapshotEnvelope},
};

/// The two units of one entity a claim concerns, in the epoch whose count placed the key in the
/// claimant while its previous count placed it in the holder.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionClaimSubject {
    pub domain: DomainName,
    /// The processor or relay whose units the key moves between.
    pub entity: NodeRef,
    pub epoch: PartitionEpochNumber,
    /// The unit that may hold the key under the previous count.
    pub holder: Unit,
    /// The unit the current count places the key in.
    pub claimant: Unit,
}

/// One step of a claim. A holder keeps the key's state after releasing it, until the claimant
/// installed it and asks the holder to forget it, so neither side's failure loses the state.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionClaimStep {
    Release,
    Forget,
}

/// One checkpoint of a released key's state, as its holder persisted it. Its bytes travel through
/// the bulk pool as a `FetchStateCheckpoint` of the holder's placement.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct ReleasedCheckpoint {
    pub placement: StatePlacementEnvelope,
    pub snapshot: StateSnapshotEnvelope,
}

/// The state a holding unit of a processor released for one key's branch.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct ReleasedBranch {
    /// The registry lifetime the key's branch had in its holder, which its state is stamped with.
    pub incarnation: u64,
    /// The domain time of the key's last input in its holder.
    pub last_activity: Timestamp,
    /// Every checkpoint the key's branch state consists of.
    pub checkpoints: Vec<ReleasedCheckpoint>,
}

/// The row a holding unit of a materialized relay released for one key.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct ReleasedRow {
    /// The row as a one-row Arrow IPC stream of the relay's schema.
    pub batch: Vec<u8>,
    pub metadata: RemoteRuntimeRecordMetadata,
}

/// What a holding unit released for one key.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum ReleasedKey {
    Branch(ReleasedBranch),
    Row(ReleasedRow),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum PartitionClaimOutcome {
    /// The holding unit holds no state for the key: it expired there or never reached it.
    NotHeld,
    Released(ReleasedKey),
    Forgotten,
}

/// One key of a catalog page: its fingerprint, which orders the shard, and its fields, which name
/// the key in a claim.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct PartitionCatalogKey {
    pub fingerprint: BranchKeyFingerprint,
    pub key: nervix_models::RemoteBranchKey,
}

/// One page of a catalog shard: the keys one holding unit holds that the current count places in
/// the claimant, in fingerprint order.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct PartitionCatalogShard {
    pub keys: Vec<PartitionCatalogKey>,
    /// Present when the shard continues after this page.
    pub next: Option<BranchKeyFingerprint>,
}

/// The holdings of one unit of one entity a node owns.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitPartitionHoldings {
    pub entity: NodeRef,
    pub unit: Unit,
    /// Keys the unit holds outside their partition under the current count.
    pub pending: u64,
    /// Keys the unit claimed from another unit in this epoch.
    pub claimed: u64,
}

/// What one unit's owner is asked about a branch's partition transition.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum PartitionTransitionRequest {
    /// Locate one demanded key when the external publisher's partitioner is unknown. This never
    /// broadcasts a complete key catalog to every destination partition.
    Locate {
        subject: PartitionClaimSubject,
        fingerprint: BranchKeyFingerprint,
    },
    /// Release or forget one key at its holding unit.
    Claim {
        subject: PartitionClaimSubject,
        key: nervix_models::RemoteBranchKey,
        step: PartitionClaimStep,
    },
    /// The keys one holding unit holds that land in the claimant's unit, after the cursor a
    /// previous page returned.
    CatalogShard {
        subject: PartitionClaimSubject,
        after: Option<BranchKeyFingerprint>,
    },
    /// The keys every unit of the branch's entities this node owns still holds outside their
    /// partition, and the keys each claimed in the epoch.
    Holdings {
        domain: DomainName,
        branch: BranchName,
        epoch: PartitionEpochNumber,
    },
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum PartitionTransitionAnswer {
    Located(Option<PartitionKeyLocation>),
    Claim(PartitionClaimOutcome),
    CatalogShard(PartitionCatalogShard),
    Holdings(Vec<UnitPartitionHoldings>),
}

/// The admitted origin of a retained copy, used to select the newest installed claimant when
/// several topology changes occur before a previous holder has forgotten its copy.
#[derive(Debug, Clone, Copy, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionKeyLocation {
    pub origin: Option<nervix_models::InheritedPartitionOrigin>,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct PartitionTransitionResponse {
    pub result: Result<PartitionTransitionAnswer, RemoteOperationFailure>,
}

impl InterconnectRequest for PartitionTransitionRequest {
    type Response = PartitionTransitionResponse;

    const NAME: &'static str = "partition_transition";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Shared;
    /// A release stops the key's branch, which drains its pending work and persists its final
    /// state within the branch's shutdown grace, before the answer can describe that state.
    const DEADLINE: RequestDeadline = RequestDeadline::Request;
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
mod properties {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_execution::Executor;
    use nervix_models::{
        ModelKind, ModelName, RemoteRuntimeField, RemoteRuntimeValue, SchemaFingerprint,
    };

    use super::*;
    use crate::{
        RemoteOperationSubject, RkyvMessage, RuntimeState,
        runtime_state::{StatePlacementEnvelope, StateSnapshotEnvelope},
    };

    /// The values one generated case varies every message of the claim exchange by.
    #[derive(Debug, bolero::TypeGenerator)]
    struct ClaimWireCase {
        epoch: u64,
        holder: u32,
        claimant: u32,
        value: i64,
        fingerprint: [u8; 32],
        lsm: u64,
        length: u64,
        incarnation: u64,
        watermark: i64,
        pending: u64,
        claimed: u64,
        keys: u8,
        row: Vec<u8>,
        last_page: bool,
    }

    impl ClaimWireCase {
        fn subject(&self) -> PartitionClaimSubject {
            PartitionClaimSubject {
                domain: DomainName::parse("orders").assured("the domain name is valid"),
                entity: NodeRef::new(
                    ModelKind::Relay,
                    ModelName::parse("profiles").assured("the relay name is valid"),
                ),
                epoch: PartitionEpochNumber::new(
                    std::num::NonZeroU64::new(self.epoch.max(1)).assured("the epoch is positive"),
                ),
                holder: Unit::partition(self.holder),
                claimant: Unit::partition(self.claimant),
            }
        }

        fn key(&self) -> nervix_models::RemoteBranchKey {
            let fields = vec![RemoteRuntimeField {
                name: "tenant".to_string(),
                value: RemoteRuntimeValue::I64(self.value),
            }]
            .into_boxed_slice();
            let origin =
                (!self.keys.is_multiple_of(2)).then(|| nervix_models::InheritedPartitionOrigin {
                    partition: nervix_models::ExternalPartition::new(self.holder),
                    epoch: self.subject().epoch,
                    unit: self.subject().holder,
                });
            nervix_models::RemoteBranchKey::branched(fields, origin)
        }

        fn requests(&self) -> Vec<PartitionTransitionRequest> {
            let fingerprint = BranchKeyFingerprint::new(self.fingerprint);
            vec![
                PartitionTransitionRequest::Claim {
                    subject: self.subject(),
                    key: self.key(),
                    step: PartitionClaimStep::Release,
                },
                PartitionTransitionRequest::Claim {
                    subject: self.subject(),
                    key: self.key(),
                    step: PartitionClaimStep::Forget,
                },
                PartitionTransitionRequest::CatalogShard {
                    subject: self.subject(),
                    after: None,
                },
                PartitionTransitionRequest::CatalogShard {
                    subject: self.subject(),
                    after: Some(fingerprint),
                },
                PartitionTransitionRequest::Holdings {
                    domain: self.subject().domain,
                    branch: BranchName::parse("by_tenant").assured("the branch name is valid"),
                    epoch: self.subject().epoch,
                },
            ]
        }

        fn answers(&self) -> Vec<Result<PartitionTransitionAnswer, RemoteOperationFailure>> {
            let metadata = RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: Timestamp::from_unix_nanos(self.watermark),
                ingested_at_high_watermark: Timestamp::from_unix_nanos(self.watermark),
            };
            let checkpoint = ReleasedCheckpoint {
                placement: StatePlacementEnvelope {
                    domain: self.subject().domain,
                    state: RuntimeState::Deduplicator {
                        schema: SchemaFingerprint::from_digest(self.fingerprint),
                    },
                    kind: ModelKind::Deduplicator,
                    identifier: ModelName::parse("distinct").assured("the name is valid"),
                    unit: Unit::partition(self.holder),
                    branch_key: self.key(),
                },
                snapshot: StateSnapshotEnvelope {
                    lsm: self.lsm,
                    length: self.length,
                    digest: self.fingerprint,
                },
            };
            let mut keys = Vec::new();
            for index in 0..self.keys % 4 {
                let mut fingerprint = self.fingerprint;
                fingerprint[0] = index;
                keys.push(PartitionCatalogKey {
                    fingerprint: BranchKeyFingerprint::new(fingerprint),
                    key: self.key(),
                });
            }
            let next = if self.last_page {
                None
            } else {
                Some(BranchKeyFingerprint::new(self.fingerprint))
            };
            vec![
                Ok(PartitionTransitionAnswer::Claim(
                    PartitionClaimOutcome::NotHeld,
                )),
                Ok(PartitionTransitionAnswer::Claim(
                    PartitionClaimOutcome::Forgotten,
                )),
                Ok(PartitionTransitionAnswer::Claim(
                    PartitionClaimOutcome::Released(ReleasedKey::Row(ReleasedRow {
                        batch: self.row.clone(),
                        metadata,
                    })),
                )),
                Ok(PartitionTransitionAnswer::Claim(
                    PartitionClaimOutcome::Released(ReleasedKey::Branch(ReleasedBranch {
                        incarnation: self.incarnation,
                        last_activity: Timestamp::from_unix_nanos(self.watermark),
                        checkpoints: vec![checkpoint],
                    })),
                )),
                Ok(PartitionTransitionAnswer::CatalogShard(
                    PartitionCatalogShard { keys, next },
                )),
                Ok(PartitionTransitionAnswer::Holdings(vec![
                    UnitPartitionHoldings {
                        entity: self.subject().entity,
                        unit: Unit::partition(self.holder),
                        pending: self.pending,
                        claimed: self.claimed,
                    },
                ])),
                Err(RemoteOperationFailure::not_ready(
                    RemoteOperationSubject::Entity {
                        domain: self.subject().domain,
                        entity: self.subject().entity,
                    },
                )),
            ]
        }
    }

    async fn assert_round_trip<M>(executor: &Executor, message: M)
    where
        M: RkyvMessage + Clone + std::fmt::Debug + PartialEq,
    {
        let class = <PartitionTransitionRequest as InterconnectRequest>::CLASS;
        let limit = class.payload_limit(executor);
        let (bytes, charge) = message
            .clone()
            .encode_rkyv(executor.clone(), class, limit)
            .await
            .assured("a bounded claim exchange message fits its class's payload limit");
        let (decoded, decoded_charge) = M::decode_rkyv(executor.clone(), class, bytes)
            .await
            .assured("the production encoder's message decodes");
        assert_eq!(decoded, message);
        drop(decoded_charge);
        drop(charge);
    }

    /// Every request and answer of the claim exchange keeps its complete value through the
    /// production wire codec of its class: every claim step, catalog cursor and holdings request,
    /// and every claim outcome, catalog page and holdings answer, including a refusal.
    #[test]
    fn bolero_partition_transition_messages_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("the ordinary test runtime builds");
        bolero::check!()
            .with_iterations(256)
            .with_max_len(256)
            .with_type::<ClaimWireCase>()
            .for_each(|case| {
                runtime.block_on(async {
                    let executor = Executor::default();
                    for request in case.requests() {
                        nervix_primitives::task::consume_budget().await;
                        assert_round_trip(&executor, request).await;
                    }
                    for result in case.answers() {
                        nervix_primitives::task::consume_budget().await;
                        assert_round_trip(&executor, PartitionTransitionResponse { result }).await;
                    }
                    let snapshot = executor.snapshot();
                    assert_eq!(snapshot.management_memory.reserved_bytes, 0);
                    assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
                });
            });
    }
}
