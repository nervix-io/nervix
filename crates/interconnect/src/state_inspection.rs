//! The bounded owner-page operation for materialized relay inspection.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed page requests, captured page descriptors and stream framing.
//! - **Depends on.** State identities, native snapshot containers and interconnect bulk admission.
//! - **Must not know.** Plexus, Row frames, rendering, graph mutation or snapshot installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the boundary declared above")
)]

use std::time::Duration;

use nervix_models::{
    BranchKeyFingerprint, ClusterNodeIdentity, DomainName, RelayName, SchemaFingerprint,
    StateInspectionBranchSelection, StateInspectionCounts, StateInspectionOrigin,
    StateInspectionRejection, Unit,
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

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "turmoil", feature = "loom"))
))]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_execution::Executor;
    use nervix_models::{ClusterNodeIncarnation, StateInspectionStage};

    use super::*;
    use crate::RkyvMessage;

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
