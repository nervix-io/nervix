//! The bounded owner-page operations for materialized relay and processor state inspection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed page requests, captured page descriptors and the body identities their
//!   streams carry.
//! - **Depends on.** State identities, native snapshot containers and interconnect bulk admission.
//! - **Must not know.** Plexus, Row frames, rendering, graph mutation or snapshot installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use std::time::Duration;

use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, DomainName, NodeRef, ProcessorStateSection,
    RelayName, RemoteRuntimeField, SchemaField, SchemaFingerprint, StateInspectionBranchSelection,
    StateInspectionCounts, StateInspectionOrigin, StateInspectionRejection, Unit,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectStreamRequest, PoolClass, RequestSubquota};

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ReadMaterializedStatePage {
    pub domain: DomainName,
    pub relay: RelayName,
    pub unit: Unit,
    pub control_revision: u64,
    pub schema: SchemaFingerprint,
    pub selection: StateInspectionBranchSelection,
    pub after: Option<(Option<BranchKeyFingerprint>, u64)>,
    pub max_rows: u64,
    pub max_bytes: u64,
    pub target: ClusterNodeIdentity,
}

impl InterconnectStreamRequest for ReadMaterializedStatePage {
    const NAME: &'static str = "read_materialized_state_page";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    #[inline]
    fn default_timeout() -> Duration {
        <nervix_models::StateInspectionReadTimeout as nervix_models::SettingDefinition>::default_value(&())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct MaterializedStatePageDescriptor {
    pub origin: StateInspectionOrigin,
    pub fence: u64,
    pub counts: StateInspectionCounts,
    pub offset: u64,
    pub last_branch: Option<BranchKeyFingerprint>,
    pub more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct MaterializedStatePageTransfer {
    pub page: MaterializedStatePageDescriptor,
    pub body: Option<StatePageBody>,
}

/// The identity of one native Arrow body a page stream carries after its descriptor: its exact
/// length and SHA-256 digest, verified before the body is decoded.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct StatePageBody {
    pub bytes: u64,
    pub digest: [u8; 32],
}

pub type MaterializedStatePageReply =
    Result<MaterializedStatePageTransfer, StateInspectionRejection>;

/// A read of one execution unit's reorderer or correlator state from the unit's owner process.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ReadProcessorStatePage {
    pub domain: DomainName,
    /// The inspected reorderer or correlator.
    pub processor: NodeRef,
    pub unit: Unit,
    pub control_revision: u64,
    pub schema: SchemaFingerprint,
    pub selection: StateInspectionBranchSelection,
    pub after: Option<(Option<BranchKeyFingerprint>, u64)>,
    pub max_rows: u64,
    pub max_bytes: u64,
    pub target: ClusterNodeIdentity,
}

impl InterconnectStreamRequest for ReadProcessorStatePage {
    const NAME: &'static str = "read_processor_state_page";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    #[inline]
    fn default_timeout() -> Duration {
        <nervix_models::StateInspectionReadTimeout as nervix_models::SettingDefinition>::default_value(&())
    }
}

/// What one unit's processor page holds: the read origin, the complete counts, where the page
/// ends, the returned rows as runs in the unit's row order, and every section's fields and body.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ProcessorStatePageDescriptor {
    pub origin: StateInspectionOrigin,
    pub counts: StateInspectionCounts,
    pub offset: u64,
    pub last_branch: Option<BranchKeyFingerprint>,
    pub more: bool,
    pub runs: Vec<ProcessorStateRunTransfer>,
    pub sections: Vec<ProcessorStateSectionTransfer>,
}

/// Consecutive returned rows of one section and one concrete branch, or of the unbranched state.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ProcessorStateRunTransfer {
    pub section: ProcessorStateSection,
    /// The branch key's typed fields; absent for unbranched state.
    pub branch: Option<Vec<RemoteRuntimeField>>,
    pub rows: u64,
}

/// One section of a processor page: every row of it the unit holds, its complete fields, and the
/// identity of the Arrow body holding its returned rows, absent when the page returns none.
#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct ProcessorStateSectionTransfer {
    pub section: ProcessorStateSection,
    pub rows: u64,
    pub fields: Vec<SchemaField>,
    pub body: Option<StatePageBody>,
}

pub type ProcessorStatePageReply = Result<ProcessorStatePageDescriptor, StateInspectionRejection>;

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "turmoil", feature = "loom"))
))]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::Executor;
    use nervix_models::{ClusterNodeIncarnation, ModelKind, ModelName, StateInspectionStage};

    use super::*;
    use crate::RkyvMessage;

    /// A rejection a processor page reply can carry.
    fn rejection(choice: u8) -> StateInspectionRejection {
        match choice % 4 {
            0 => StateInspectionRejection::Deadline,
            1 => StateInspectionRejection::StaleContinuation,
            2 => StateInspectionRejection::OwnerUnavailable,
            _ => StateInspectionRejection::NotReady,
        }
    }

    /// Encodes `message` with the admitted production codec, decodes it, and asserts the decoded
    /// value is the original and encodes to the same bytes: branch-key floats compare by their bits
    /// through the bytes, so a signed zero cannot be normalized away.
    async fn assert_round_trip<M>(executor: &Executor, message: M)
    where
        M: RkyvMessage + Clone + PartialEq + std::fmt::Debug,
    {
        let class = PoolClass::Bulk;
        let (encoded, _charge) = message
            .clone()
            .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
            .await
            .assured("a bounded message encodes");
        let (decoded, _charge) = M::decode_rkyv(executor.clone(), class, encoded.clone())
            .await
            .assured("the encoded message decodes");
        assert_eq!(decoded, message);
        let (again, _charge) = decoded
            .encode_rkyv(executor.clone(), class, class.payload_limit(executor))
            .await
            .assured("the decoded message encodes");
        assert_eq!(
            again, encoded,
            "every bit of the message survives the round trip"
        );
    }

    #[test]
    fn bolero_processor_inspection_page_messages_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("ordinary fixture runtime");
        bolero::check!()
            .with_iterations(128)
            .with_max_len(1024)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary =
                    nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
                let branch =
                    BranchKeyFingerprint::new(std::array::from_fn(|_| arbitrary.entropy().byte()));
                let target = ClusterNodeIdentity::new(
                    arbitrary.name(),
                    ClusterNodeIncarnation::new(arbitrary.positive_u64().get()),
                );
                let kind = match arbitrary.entropy().byte() % 4 {
                    0 => ModelKind::Reorderer,
                    1 => ModelKind::Correlator,
                    2 => ModelKind::Deduplicator,
                    _ => ModelKind::WindowProcessor,
                };
                let after = if arbitrary.entropy().flag() {
                    let position = arbitrary.entropy().flag().then_some(branch);
                    Some((position, u64::from(arbitrary.entropy().byte())))
                } else {
                    None
                };
                let request = ReadProcessorStatePage {
                    domain: arbitrary.name(),
                    processor: NodeRef::new(kind, arbitrary.name::<ModelName>()),
                    unit: Unit::partition(u32::from(arbitrary.entropy().byte())),
                    control_revision: arbitrary.positive_u64().get(),
                    schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                        arbitrary.entropy().byte()
                    })),
                    selection: match arbitrary.entropy().byte() % 3 {
                        0 => StateInspectionBranchSelection::All,
                        1 => StateInspectionBranchSelection::Unbranched,
                        _ => StateInspectionBranchSelection::Branch(branch),
                    },
                    after,
                    max_rows: u64::from(arbitrary.entropy().byte()),
                    max_bytes: arbitrary.positive_u64().get(),
                    target: target.clone(),
                };
                let reply: ProcessorStatePageReply = if arbitrary.entropy().flag() {
                    Err(rejection(arbitrary.entropy().byte()))
                } else {
                    let sections = match kind {
                        ModelKind::Reorderer => vec![ProcessorStateSection::Pending],
                        ModelKind::Correlator => {
                            vec![ProcessorStateSection::Left, ProcessorStateSection::Right]
                        }
                        ModelKind::Deduplicator => vec![ProcessorStateSection::Keys],
                        ModelKind::WindowProcessor => vec![
                            ProcessorStateSection::Rows,
                            ProcessorStateSection::Header,
                            ProcessorStateSection::Accumulators,
                        ],
                        _ => unreachable!("the generated processor kind is one of the four above"),
                    };
                    let mut runs = Vec::new();
                    for _ in 0..arbitrary.entropy().byte() % 4 {
                        let section =
                            sections[usize::from(arbitrary.entropy().byte()) % sections.len()];
                        runs.push(ProcessorStateRunTransfer {
                            section,
                            branch: arbitrary.remote_branch_key(),
                            rows: u64::from(arbitrary.entropy().byte()),
                        });
                    }
                    let mut transfers = Vec::new();
                    for section in sections {
                        let body = arbitrary.entropy().flag().then(|| StatePageBody {
                            bytes: arbitrary.positive_u64().get(),
                            digest: std::array::from_fn(|_| arbitrary.entropy().byte()),
                        });
                        transfers.push(ProcessorStateSectionTransfer {
                            section,
                            rows: arbitrary.positive_u64().get(),
                            fields: arbitrary.schema_fields(1),
                            body,
                        });
                    }
                    let last_branch = arbitrary.entropy().flag().then_some(branch);
                    Ok(ProcessorStatePageDescriptor {
                        origin: StateInspectionOrigin {
                            node: target,
                            fingerprint: std::array::from_fn(|_| arbitrary.entropy().byte()),
                            generation: arbitrary.positive_u64().get(),
                            revision: arbitrary.positive_u64().get(),
                            stage: StateInspectionStage::Live,
                        },
                        counts: StateInspectionCounts {
                            branches: arbitrary.positive_u64().get(),
                            rows: arbitrary.positive_u64().get(),
                            bytes: arbitrary.positive_u64().get(),
                        },
                        offset: arbitrary.positive_u64().get(),
                        last_branch,
                        more: arbitrary.entropy().flag(),
                        runs,
                        sections: transfers,
                    })
                };
                runtime.block_on(async {
                    let executor = Executor::default();
                    assert_round_trip(&executor, request).await;
                    assert_round_trip(&executor, reply).await;
                });
            });
    }

    #[test]
    fn bolero_materialized_inspection_page_messages_round_trip() {
        let runtime = nervix_primitives::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .assured("ordinary fixture runtime");
        bolero::check!()
            .with_iterations(128)
            .with_max_len(512)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary =
                    nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Vocabulary);
                let branch =
                    BranchKeyFingerprint::new(std::array::from_fn(|_| arbitrary.entropy().byte()));
                let target = ClusterNodeIdentity::new(
                    arbitrary.name(),
                    ClusterNodeIncarnation::new(arbitrary.positive_u64().get()),
                );
                let request = ReadMaterializedStatePage {
                    domain: arbitrary.name(),
                    relay: arbitrary.name(),
                    unit: Unit::partition(u32::from(arbitrary.entropy().byte())),
                    control_revision: arbitrary.positive_u64().get(),
                    schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                        arbitrary.entropy().byte()
                    })),
                    selection: match arbitrary.entropy().byte() % 3 {
                        0 => StateInspectionBranchSelection::All,
                        1 => StateInspectionBranchSelection::Unbranched,
                        _ => StateInspectionBranchSelection::Branch(branch),
                    },
                    after: arbitrary
                        .entropy()
                        .flag()
                        .then(|| (Some(branch), u64::from(arbitrary.entropy().byte()))),
                    max_rows: u64::from(arbitrary.entropy().byte()) + 1,
                    max_bytes: arbitrary.positive_u64().get(),
                    target: target.clone(),
                };
                let start = request.after.map_or(0, |(_, offset)| offset);
                let returned = u64::from(arbitrary.entropy().byte()).min(request.max_rows);
                let remaining = u64::from(arbitrary.entropy().byte());
                let rows = start + returned + remaining;
                let reply: MaterializedStatePageReply = if arbitrary.entropy().flag() {
                    Err(StateInspectionRejection::Deadline)
                } else {
                    Ok(MaterializedStatePageTransfer {
                        page: MaterializedStatePageDescriptor {
                            origin: StateInspectionOrigin {
                                node: target,
                                fingerprint: std::array::from_fn(|_| arbitrary.entropy().byte()),
                                generation: arbitrary.positive_u64().get(),
                                revision: arbitrary.positive_u64().get(),
                                stage: if arbitrary.entropy().flag() {
                                    StateInspectionStage::Live
                                } else {
                                    StateInspectionStage::Stored
                                },
                            },
                            fence: arbitrary.positive_u64().get(),
                            counts: StateInspectionCounts {
                                branches: rows,
                                rows,
                                bytes: arbitrary.positive_u64().get(),
                            },
                            offset: start + returned,
                            last_branch: Some(branch),
                            more: remaining != 0,
                        },
                        body: (returned != 0).then(|| StatePageBody {
                            bytes: request.max_bytes,
                            digest: std::array::from_fn(|_| arbitrary.entropy().byte()),
                        }),
                    })
                };
                runtime.block_on(async {
                    let executor = Executor::default();
                    let class = PoolClass::Bulk;
                    let (encoded, _charge) = request
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                        .await
                        .assured("bounded request encodes");
                    let (decoded, _charge) =
                        ReadMaterializedStatePage::decode_rkyv(executor.clone(), class, encoded)
                            .await
                            .assured("request decodes");
                    assert_eq!(decoded, request);
                    let (encoded, _charge) = reply
                        .clone()
                        .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
                        .await
                        .assured("bounded descriptor encodes");
                    let (decoded, _charge) =
                        MaterializedStatePageReply::decode_rkyv(executor, class, encoded)
                            .await
                            .assured("descriptor decodes");
                    assert_eq!(decoded, reply);
                });
            });
    }
}
