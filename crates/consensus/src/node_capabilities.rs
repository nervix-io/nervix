//! Committed capacity claims and their admission fence.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Publishing one immutable claim per run and replacing it on a newer run.
//! - **Depends on.** Membership, node identities and replicated records.
//! - **Must not know.** Detection, placement scores or peer delivery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns replicated capability publication")
)]

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
struct CapabilityAnnouncement {
    record: NodeCapabilityRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
enum CapabilityAnnouncementReply {
    Committed,
    Refused(ConsensusConflict),
    Retry,
    SenderMismatch,
}

impl nervix_interconnect::InterconnectRequest for CapabilityAnnouncement {
    type Response = CapabilityAnnouncementReply;
    const NAME: &'static str = "consensus.announce_node_capabilities";
    const CLASS: nervix_interconnect::PoolClass = nervix_interconnect::PoolClass::Commands;
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this required trait projection is exactly the existing runtime-admission \
                      read deadline; it defines no independent numeric policy"
        )
    )]
    const TIMEOUT: Duration =
        <wire::RuntimeAdmissionRead as nervix_interconnect::InterconnectRequest>::TIMEOUT;
}

impl Consensus {
    pub(super) fn register_capability_announcement_handler(
        &self,
    ) -> Result<(), Report<HandlerRegistrationError>> {
        let proposer = self.proposer();
        self.inner.interconnect.register_handler::<CapabilityAnnouncement, _, _>(move |context, request| {
            let proposer = proposer.clone();
            async move {
                if request.record.identity.node_id() != context.peer_node_id() {
                    return CapabilityAnnouncementReply::SenderMismatch;
                }
                match proposer.inner.client_write(ConsensusCommand::AnnounceNodeCapabilities { record: request.record }).await {
                    Ok(response) => match response.data {
                        ConsensusResponse::Applied => CapabilityAnnouncementReply::Committed,
                        ConsensusResponse::Conflict(conflict) => CapabilityAnnouncementReply::Refused(conflict),
                        ConsensusResponse::Transaction(_) => unreachable!("capability publication never mutates a transaction"),
                    },
                    Err(error) => {
                        debug!(error = %format_args!("{error:#}"), "node capability publication will retry against the current leader");
                        CapabilityAnnouncementReply::Retry
                    }
                }
            }
        })
    }
}

pub(super) fn announce(
    state: &mut StateMachineData,
    record: &NodeCapabilityRecord,
) -> AppliedConsensusCommand {
    let identity = &record.identity;
    if !state
        .last_membership
        .membership()
        .nodes()
        .any(|(node, _)| node == identity.node_id())
    {
        return AppliedConsensusCommand::conflict(ConsensusConflict::CapabilityNodeNotAdmitted);
    }
    if let Some(fence) = state.node_admission_fences.get(identity.node_id())
        && identity.incarnation() <= *fence
    {
        return AppliedConsensusCommand::conflict(ConsensusConflict::CapabilityIncarnationFenced);
    }
    if let Some(current) = state.node_capabilities.get(identity.node_id()) {
        if current == record {
            return AppliedConsensusCommand::applied(StateMachineChanges::default());
        }
        if current.identity.incarnation() >= identity.incarnation() {
            return AppliedConsensusCommand::conflict(
                ConsensusConflict::CapabilityIncarnationFenced,
            );
        }
    }
    state
        .node_capabilities
        .insert(identity.node_id().clone(), record.clone());
    AppliedConsensusCommand::applied(StateMachineChanges::default())
}

impl Observer {
    pub fn node_capability(&self, node: &ClusterNodeName) -> Option<NodeCapabilityRecord> {
        self.inner
            .store
            .inner
            .state()
            .node_capabilities
            .get(node)
            .cloned()
    }
    /// One coherent committed view, including the last claim of departed nodes.
    pub fn node_capabilities(&self) -> BTreeMap<ClusterNodeName, NodeCapabilityRecord> {
        (&self.inner.store.inner.state().node_capabilities).into()
    }
}

impl Proposer {
    pub async fn announce_node_capabilities(
        &self,
        record: NodeCapabilityRecord,
    ) -> error_stack::Result<(), ConsensusError> {
        if let Some(leader) = self.current_leader().await
            && leader != *self.local_node_id()
        {
            let reply = self
                .inner
                .interconnect
                .request(&leader, CapabilityAnnouncement { record })
                .await
                .change_context(ConsensusError::CapabilityAnnouncementPending)?;
            return match reply {
                CapabilityAnnouncementReply::Committed => Ok(()),
                CapabilityAnnouncementReply::Refused(conflict) => {
                    Self::applied_response(ConsensusResponse::Conflict(conflict))
                }
                CapabilityAnnouncementReply::Retry => {
                    Err(Report::new(ConsensusError::CapabilityAnnouncementPending))
                }
                CapabilityAnnouncementReply::SenderMismatch => {
                    Err(Report::new(ConsensusError::CapabilitySenderMismatch))
                }
            };
        }
        let response = self
            .inner
            .client_write(ConsensusCommand::AnnounceNodeCapabilities { record })
            .await?;
        Self::applied_response(response.data)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    fn record(incarnation: u64) -> NodeCapabilityRecord {
        let node = ClusterNodeName::parse("worker").expect("node name");
        NodeCapabilityRecord {
            identity: ClusterNodeIdentity::new(node, ClusterNodeIncarnation::new(incarnation)),
            capabilities: nervix_models::NodeCapabilities {
                cpu_count: NonZeroU64::new(4),
                memory_bytes: NonZeroU64::new(8 << 30),
                receive: nervix_interconnect::PeerReceiveSettings::default().bounds(),
            },
        }
    }

    fn admitted_state(record: &NodeCapabilityRecord) -> StateMachineData {
        let nodes = BTreeMap::from([(
            record.identity.node_id().clone(),
            BasicNode::new("localhost:9000"),
        )]);
        let membership = openraft::Membership::new(vec![nodes.keys().cloned().collect()], nodes)
            .expect("one admitted voter");
        StateMachineData {
            last_membership: Arc::new(StoredMembership::new(None, membership)),
            ..StateMachineData::default()
        }
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
        changed.capabilities.cpu_count = NonZeroU64::new(8);
        announce(&mut state, &changed);
        assert_eq!(state, unchanged, "one incarnation has one immutable claim");
        let next = record(8);
        announce(&mut state, &next);
        assert_ne!(state.schedule_topology_inputs(), fence);
        state.last_membership = Arc::new(StoredMembership::default());
        assert_eq!(
            state.node_capabilities.get(next.identity.node_id()),
            Some(&next)
        );
        apply_consensus_command(
            &mut state,
            &ConsensusCommand::FenceNodeAdmission {
                identity: next.identity.clone(),
            },
        );
        assert!(
            state
                .node_capabilities
                .get(next.identity.node_id())
                .is_none()
        );
        state.last_membership = admitted_state(&next).last_membership;
        announce(&mut state, &next);
        assert!(
            state
                .node_capabilities
                .get(next.identity.node_id())
                .is_none()
        );
    }

    #[test]
    fn an_unadmitted_run_cannot_publish_capacity() {
        let mut state = StateMachineData::default();
        announce(&mut state, &record(1));
        assert_eq!(state.node_capabilities.len(), 0);
    }

    #[test]
    fn bolero_node_capability_records_preserve_every_committed_value() {
        use nervix_arbitrary::{Arbitrary, Domain};

        use crate::durable_batch::DurableBatch;
        bolero::check!()
            .with_iterations(128)
            .with_max_len(256)
            .for_each(|bytes: &[u8]| {
                let record = Arbitrary::new(bytes, Domain::Vocabulary).node_capability_record();
                let encoded = DurableBatch::encode(&record, 4096)
                    .expect("bounded current claim fits the record budget");
                let decoded = storage_decode::<NodeCapabilityRecord>(&encoded)
                    .expect("current stored claim decodes");
                assert_eq!(decoded, record);
                let encoded_again = DurableBatch::encode(&decoded, 4096)
                    .expect("decoded claim fits the record budget");
                assert_eq!(encoded_again, encoded, "record encoding is canonical");
                let request = CapabilityAnnouncement { record };
                let encoded = DurableBatch::encode(&request, 4096)
                    .expect("typed announcement fits the request budget");
                assert_eq!(
                    storage_decode::<CapabilityAnnouncement>(&encoded)
                        .expect("current announcement decodes"),
                    request
                );
                for reply in [
                    CapabilityAnnouncementReply::Committed,
                    CapabilityAnnouncementReply::Retry,
                    CapabilityAnnouncementReply::SenderMismatch,
                    CapabilityAnnouncementReply::Refused(
                        ConsensusConflict::CapabilityNodeNotAdmitted,
                    ),
                    CapabilityAnnouncementReply::Refused(
                        ConsensusConflict::CapabilityIncarnationFenced,
                    ),
                ] {
                    let encoded = DurableBatch::encode(&reply, 4096).expect("typed outcome fits");
                    assert_eq!(
                        storage_decode::<CapabilityAnnouncementReply>(&encoded)
                            .expect("current outcome decodes"),
                        reply
                    );
                }
            });
    }
}
