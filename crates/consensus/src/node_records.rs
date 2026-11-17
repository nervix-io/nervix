//! Committed node records and their admission fence.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Committing one immutable node record per run, with the revision that committed it,
//!   and replacing it on a newer run.
//! - **Depends on.** Membership, node identities and replicated records.
//! - **Must not know.** Detection, placement scores or peer delivery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns replicated node record publication")
)]

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct NodeRecordAnnouncement {
    record: NodeRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
enum NodeRecordAnnouncementReply {
    Committed,
    Refused(ConsensusConflict),
    Retry,
    SenderMismatch,
}

impl nervix_interconnect::InterconnectRequest for NodeRecordAnnouncement {
    type Response = NodeRecordAnnouncementReply;
    const NAME: &'static str = "consensus.announce_node_record";
    const CLASS: nervix_interconnect::PoolClass = nervix_interconnect::PoolClass::Commands;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this required trait projection is exactly the existing runtime-admission \
                      read deadline; it defines no independent numeric policy"
        )
    )]
    const DEADLINE: nervix_interconnect::RequestDeadline =
        <wire::RuntimeAdmissionRead as nervix_interconnect::InterconnectRequest>::DEADLINE;
}

impl Consensus {
    pub(super) fn register_node_record_announcement_handler(
        &self,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let proposer = self.proposer();
        self.inner.interconnect.register_handler::<NodeRecordAnnouncement, _, _>(move |context, request| {
            let proposer = proposer.clone();
            async move {
                if request.record.identity().node_id() != context.peer_node_id() {
                    return NodeRecordAnnouncementReply::SenderMismatch;
                }
                match proposer.inner.client_write(ConsensusCommand::AnnounceNodeRecord { record: Box::new(request.record) }).await {
                    Ok(response) => match response.data {
                        ConsensusResponse::Applied => NodeRecordAnnouncementReply::Committed,
                        ConsensusResponse::Conflict(conflict) => NodeRecordAnnouncementReply::Refused(conflict),
                        ConsensusResponse::Transaction(_) | ConsensusResponse::OrphanedPauseRecovery(_) => unreachable!("node record publication returns only its own applied or conflict response"),
                    },
                    Err(error) => {
                        debug!(error = %format_args!("{error:#}"), "node record publication will retry against the current leader");
                        NodeRecordAnnouncementReply::Retry
                    }
                }
            }
        })
    }
}

/// Commits `record` at the revision of the entry being applied. One run has one immutable record:
/// announcing it again changes nothing, and a different record for a run that has one is refused.
pub(super) fn announce(
    state: &mut StateMachineData,
    record: &NodeRecord,
) -> AppliedConsensusCommand {
    let identity = record.identity();
    if !state
        .last_membership
        .membership()
        .nodes()
        .any(|(node, _)| node == identity.node_id())
    {
        return AppliedConsensusCommand::conflict(ConsensusConflict::NodeRecordNotAdmitted);
    }
    if let Some(fence) = state.node_admission_fences.get(identity.node_id())
        && identity.incarnation() <= *fence
    {
        return AppliedConsensusCommand::conflict(ConsensusConflict::NodeRecordIncarnationFenced);
    }
    if let Some(current) = state.node_records.get(identity.node_id()) {
        if current.record == *record {
            return AppliedConsensusCommand::applied(StateMachineChanges::default());
        }
        if current.record.identity().incarnation() >= identity.incarnation() {
            return AppliedConsensusCommand::conflict(
                ConsensusConflict::NodeRecordIncarnationFenced,
            );
        }
    }
    let revision = match &state.last_applied_log_id {
        Some(log_id) => log_id.index,
        None => 0,
    };
    state.node_records.insert(
        identity.node_id().clone(),
        CommittedNodeRecord {
            revision,
            record: record.clone(),
        },
    );
    AppliedConsensusCommand::applied(StateMachineChanges::default())
}

impl Observer {
    pub fn node_record(&self, node: &ClusterNodeName) -> Option<CommittedNodeRecord> {
        self.inner
            .store
            .inner
            .state()
            .node_records
            .get(node)
            .cloned()
    }

    /// One coherent committed view, including the last record of departed nodes.
    pub fn node_records(&self) -> BTreeMap<ClusterNodeName, CommittedNodeRecord> {
        (&self.inner.store.inner.state().node_records).into()
    }

    /// The capacity claim of every committed record, the part peers adopt receive bounds from.
    pub fn node_capability_claims(&self) -> BTreeMap<ClusterNodeName, NodeCapabilityRecord> {
        let state = self.inner.store.inner.state();
        let mut claims = BTreeMap::new();
        for (node, committed) in state.node_records.iter() {
            claims.insert(node.clone(), committed.record.claim.clone());
        }
        claims
    }
}

impl Proposer {
    pub async fn announce_node_record(
        &self,
        record: NodeRecord,
    ) -> error_stack::Result<(), ConsensusError> {
        if let Some(leader) = self.current_leader().await
            && leader != *self.local_node_id()
        {
            let reply = self
                .inner
                .interconnect
                .request(&leader, NodeRecordAnnouncement { record })
                .await
                .change_context(ConsensusError::NodeRecordAnnouncementPending)?;
            return match reply {
                NodeRecordAnnouncementReply::Committed => Ok(()),
                NodeRecordAnnouncementReply::Refused(conflict) => {
                    Self::applied_response(ConsensusResponse::Conflict(conflict))
                }
                NodeRecordAnnouncementReply::Retry => {
                    Err(Report::new(ConsensusError::NodeRecordAnnouncementPending))
                }
                NodeRecordAnnouncementReply::SenderMismatch => {
                    Err(Report::new(ConsensusError::NodeRecordSenderMismatch))
                }
            };
        }
        let response = self
            .inner
            .client_write(ConsensusCommand::AnnounceNodeRecord {
                record: Box::new(record),
            })
            .await?;
        Self::applied_response(response.data)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use nervix_models::{
        CudaProviderRegistration, Detected, ExecutionProviders, GpuDevice, GpuDevices,
        GpuInventory, MAX_NODE_TAGS, MAX_PROPERTY_LIST_ITEMS, MAX_PROPERTY_TEXT_BYTES,
        MAX_TAG_KEY_BYTES, MAX_TAG_VALUE_BYTES, NodeCapabilityRecord, NodeProperties, NodeTags,
        OnnxRuntime, OnnxRuntimeDetection, PropertyText, TagKey, TagValue, UndetectedReason,
    };

    use super::*;

    fn properties() -> NodeProperties {
        NodeProperties {
            architecture: PropertyText::new("x86_64").expect("short printable text"),
            cpu_model: Detected::Undetected(UndetectedReason::NotReported),
            gpus: GpuInventory::Undetected(UndetectedReason::LibraryUnavailable),
            onnx: OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable),
        }
    }

    fn record_with_tags(incarnation: u64, tags: &str) -> NodeRecord {
        let node = ClusterNodeName::parse("worker").expect("node name");
        NodeRecord {
            claim: NodeCapabilityRecord {
                identity: ClusterNodeIdentity::new(node, ClusterNodeIncarnation::new(incarnation)),
                capabilities: nervix_models::NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    engines: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_engine_capabilities(),
                    connector_host: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_connector_host_capabilities(),
                    listeners: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .listener_bounds(),
                    cpu_count: NonZeroU64::new(4),
                    memory_bytes: NonZeroU64::new(8 << 30),
                    receive: nervix_interconnect::PeerReceiveSettings::default().bounds(),
                },
            },
            properties: properties(),
            tags: tags.parse().expect("valid tags"),
        }
    }

    fn record(incarnation: u64) -> NodeRecord {
        record_with_tags(incarnation, "pool=gpu")
    }

    fn admitted_state(record: &NodeRecord) -> StateMachineData {
        let nodes = BTreeMap::from([(
            record.identity().node_id().clone(),
            BasicNode::new("localhost:9000"),
        )]);
        let membership = openraft::Membership::new(vec![nodes.keys().cloned().collect()], nodes)
            .expect("one admitted voter");
        StateMachineData {
            last_membership: Arc::new(StoredMembership::new(None, membership)),
            ..StateMachineData::default()
        }
    }

    fn leader(record: &NodeRecord) -> CommittedLeaderIdOf<TypeConfig> {
        use openraft::vote::RaftLeaderId as _;

        CommittedLeaderIdOf::<TypeConfig>::new(1, record.identity().node_id().clone())
    }

    fn committed<'state>(
        state: &'state StateMachineData,
        record: &NodeRecord,
    ) -> Option<&'state CommittedNodeRecord> {
        state.node_records.get(record.identity().node_id())
    }

    #[test]
    fn announcement_is_idempotent_fenced_and_retained_through_departure() {
        let first = record(7);
        let mut state = admitted_state(&first);
        announce(&mut state, &first);
        let fence = state.schedule_topology_inputs();
        let unchanged = state.clone();
        announce(&mut state, &first);
        assert_eq!(state, unchanged);
        announce(&mut state, &record(6));
        assert_eq!(state, unchanged);
        let mut changed = first.clone();
        changed.claim.capabilities.cpu_count = NonZeroU64::new(8);
        announce(&mut state, &changed);
        assert_eq!(state, unchanged, "one incarnation has one immutable record");
        let retagged = record_with_tags(7, "pool=cpu");
        announce(&mut state, &retagged);
        assert_eq!(state, unchanged, "a run cannot change its tags");
        let next = record(8);
        announce(&mut state, &next);
        assert_ne!(state.schedule_topology_inputs(), fence);
        state.last_membership = Arc::new(StoredMembership::default());
        assert_eq!(
            committed(&state, &next).map(|committed| &committed.record),
            Some(&next)
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::FenceNodeAdmission {
                identity: next.identity().clone(),
            },
        );
        assert!(committed(&state, &next).is_none());
        state.last_membership = admitted_state(&next).last_membership;
        announce(&mut state, &next);
        assert!(committed(&state, &next).is_none());
    }

    #[test]
    fn a_newer_run_replaces_the_record_and_its_tags_at_the_revision_that_commits_it() {
        let first = record_with_tags(1, "pool=gpu,zone=eu");
        let mut state = admitted_state(&first);
        state.last_applied_log_id = Some(LogIdOf::new(leader(&first), 41));
        announce(&mut state, &first);
        let committed_first = committed(&state, &first).expect("the first run committed");
        assert_eq!(committed_first.revision, 41);
        assert_eq!(committed_first.record.tags.to_string(), "pool=gpu,zone=eu");

        let second = record_with_tags(2, "pool=cpu");
        state.last_applied_log_id = Some(LogIdOf::new(leader(&first), 57));
        announce(&mut state, &second);
        let committed_second = committed(&state, &second).expect("the second run committed");
        assert_eq!(committed_second.revision, 57);
        assert_eq!(committed_second.record, second);
        assert!(committed_second.record.tags.get("zone").is_none());
        assert_eq!(
            state
                .schedule_topology_inputs()
                .records()
                .get(second.identity().node_id()),
            Some(committed_second),
            "the planning fence carries the committed record"
        );
    }

    #[test]
    fn engine_and_connector_claims_change_only_with_the_incarnation() {
        let first = record(7);
        let mut state = admitted_state(&first);
        announce(&mut state, &first);
        let unchanged = state.clone();
        let mut changed = first.clone();
        changed.claim.capabilities.engines.vm_inline_row_limit = 42;
        changed
            .claim
            .capabilities
            .connector_host
            .retry_jitter_percent = 20;
        announce(&mut state, &changed);
        assert_eq!(state, unchanged);
        changed.claim.identity = record(8).claim.identity;
        announce(&mut state, &changed);
        assert_eq!(
            committed(&state, &changed).map(|committed| &committed.record),
            Some(&changed)
        );
    }

    #[test]
    fn an_unadmitted_run_cannot_publish_its_record() {
        let mut state = StateMachineData::default();
        announce(&mut state, &record(1));
        assert_eq!(state.node_records.len(), 0);
    }

    /// The largest record the bounds admit: every tag and every listed device and provider at
    /// their byte bounds.
    fn largest_record() -> NodeRecord {
        let text =
            |byte: char, length: usize| -> String { std::iter::repeat_n(byte, length).collect() };
        let mut tags = BTreeMap::new();
        for index in 0..MAX_NODE_TAGS {
            let prefix = format!("{index:02}");
            let key = format!("{prefix}{}", text('k', MAX_TAG_KEY_BYTES - prefix.len()));
            let key = TagKey::parse(&key).expect("a key at its bound");
            let value = TagValue::parse(&key, &text('v', MAX_TAG_VALUE_BYTES))
                .expect("a value at its bound");
            tags.insert(key, value);
        }
        let property =
            || PropertyText::new(text('p', MAX_PROPERTY_TEXT_BYTES)).expect("text at its bound");
        let devices = (0..MAX_PROPERTY_LIST_ITEMS)
            .map(|_| GpuDevice {
                model: Detected::Value(property()),
                memory_bytes: Detected::Value(NonZeroU64::MAX),
            })
            .collect::<Vec<_>>();
        let providers = (0..MAX_PROPERTY_LIST_ITEMS)
            .map(|_| property())
            .collect::<Vec<_>>();
        let mut largest = record(u64::MAX);
        largest.tags = NodeTags::try_from(tags).expect("the tag count bound");
        largest.properties = NodeProperties {
            architecture: property(),
            cpu_model: Detected::Value(property()),
            gpus: GpuInventory::Devices {
                devices: GpuDevices::try_from(devices).expect("the device list bound"),
                driver: Detected::Value(property()),
            },
            onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
                version: Detected::Value(property()),
                providers: Detected::Value(
                    ExecutionProviders::try_from(providers).expect("the provider list bound"),
                ),
                cuda: CudaProviderRegistration::Registered,
            }),
        };
        largest
    }

    #[test]
    fn the_largest_record_the_bounds_admit_is_one_command_under_the_command_limit() {
        use crate::durable_batch::DurableBatch;

        let command_bytes = nervix_execution::OperationLimits::default()
            .command_bytes
            .as_u64();
        let largest = largest_record();
        let command = ConsensusCommand::AnnounceNodeRecord {
            record: Box::new(largest.clone()),
        };
        DurableBatch::encode(&command, command_bytes)
            .expect("the largest node record's announcement fits one command");
        DurableBatch::encode(&NodeRecordAnnouncement { record: largest }, command_bytes)
            .expect("the largest node record's request fits one command");
    }

    #[test]
    fn bolero_node_records_preserve_every_committed_value() {
        use nervix_arbitrary::{Arbitrary, Domain};

        use crate::durable_batch::DurableBatch;
        bolero::check!()
            .with_iterations(128)
            .with_max_len(2048)
            .for_each(|bytes: &[u8]| {
                let committed = Arbitrary::new(bytes, Domain::Vocabulary).committed_node_record();
                let encoded = DurableBatch::encode(&committed, 1 << 20)
                    .expect("a bounded current record fits the record budget");
                let decoded = storage_decode::<CommittedNodeRecord>(&encoded)
                    .expect("a current stored record decodes");
                assert_eq!(decoded, committed);
                let encoded_again = DurableBatch::encode(&decoded, 1 << 20)
                    .expect("a decoded record fits the record budget");
                assert_eq!(encoded_again, encoded, "record encoding is canonical");
                let request = NodeRecordAnnouncement {
                    record: committed.record,
                };
                let encoded = DurableBatch::encode(&request, 1 << 20)
                    .expect("a typed announcement fits the request budget");
                assert_eq!(
                    storage_decode::<NodeRecordAnnouncement>(&encoded)
                        .expect("a current announcement decodes"),
                    request
                );
                for reply in [
                    NodeRecordAnnouncementReply::Committed,
                    NodeRecordAnnouncementReply::Retry,
                    NodeRecordAnnouncementReply::SenderMismatch,
                    NodeRecordAnnouncementReply::Refused(ConsensusConflict::NodeRecordNotAdmitted),
                    NodeRecordAnnouncementReply::Refused(
                        ConsensusConflict::NodeRecordIncarnationFenced,
                    ),
                ] {
                    let encoded = DurableBatch::encode(&reply, 4096).expect("a typed outcome fits");
                    assert_eq!(
                        storage_decode::<NodeRecordAnnouncementReply>(&encoded)
                            .expect("a current outcome decodes"),
                        reply
                    );
                }
            });
    }
}
