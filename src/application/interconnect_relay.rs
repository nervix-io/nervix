//! The lane a remote relay payload travels through on its way into this node.
//!
//! Layer: edges.
//!
//! - **Owns.** Admission, decoding and dispatch of one interconnect relay stream.
//! - **Depends on.** The runtime's relay boundary and the interconnect transport.
//! - **Must not know.** Session commands, transactions or scheduling.

use std::collections::VecDeque;

use ahash::HashMap;
use error_stack::Report;
use futures_util::{StreamExt, stream::FuturesUnordered};
use nervix_interconnect::{ControlEnvelope, Envelope, RelayAdmission, RelayPayload};
use nervix_models::{ClusterNodeName, DomainName, RelayName};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::session_service::SessionServiceImpl;
use crate::runtime::Runtime;
#[derive(Debug)]
pub(in crate::application) struct InterconnectRelayPayload {
    peer_node_id: ClusterNodeName,
    payload: RelayPayload,
    admission: RelayAdmission,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum InterconnectRelayBranch {
    Valid(Option<crate::runtime::BranchKey>),
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InterconnectRelayChannel {
    peer_node_id: ClusterNodeName,
    kind: nervix_interconnect::RelayPayloadKind,
    domain: DomainName,
    relay: RelayName,
    branch: InterconnectRelayBranch,
}

impl InterconnectRelayChannel {
    fn from_message(message: &InterconnectRelayPayload) -> Self {
        let branch = match crate::runtime::BranchKey::from_remote_key(message.payload.key.clone()) {
            Ok(branch) => InterconnectRelayBranch::Valid(branch),
            Err(_) => InterconnectRelayBranch::Invalid,
        };
        Self {
            peer_node_id: message.peer_node_id.clone(),
            kind: message.payload.kind,
            domain: message.payload.domain.clone(),
            relay: message.payload.relay.clone(),
            branch,
        }
    }
}

pub(in crate::application) struct InterconnectRelayPayloadLane {
    sender: mpsc::UnboundedSender<InterconnectRelayPayload>,
}

impl InterconnectRelayPayloadLane {
    pub(in crate::application) fn new() -> (Self, mpsc::UnboundedReceiver<InterconnectRelayPayload>)
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, receiver)
    }

    /// Moves relay payload work off the ordered control lane without awaiting it. The dedicated
    /// receiver preserves arrival order within each authenticated logical channel and runs
    /// different channels concurrently, while gate release and status controls remain runnable.
    pub(in crate::application) fn route(
        &self,
        peer_node_id: &ClusterNodeName,
        envelope: Envelope,
        relay_admission: Option<RelayAdmission>,
    ) -> Option<Envelope> {
        let Envelope::RelayPayload(payload) = envelope else {
            return Some(envelope);
        };
        let Some(admission) = relay_admission else {
            warn!("interconnect relay payload is missing its transport admission");
            return None;
        };
        if self
            .sender
            .send(InterconnectRelayPayload {
                peer_node_id: peer_node_id.clone(),
                payload,
                admission,
            })
            .is_err()
        {
            warn!("interconnect relay payload lane is unavailable");
        }
        None
    }

    async fn handle(
        runtime: Runtime,
        channel: InterconnectRelayChannel,
        message: InterconnectRelayPayload,
    ) -> (
        InterconnectRelayChannel,
        Result<(), Report<crate::runtime::RuntimeError>>,
    ) {
        let result = runtime
            .handle_remote_stream(message.payload, message.admission)
            .await;
        (channel, result)
    }

    pub(in crate::application) async fn run(
        mut receiver: mpsc::UnboundedReceiver<InterconnectRelayPayload>,
        runtime: Runtime,
        shutdown: CancellationToken,
    ) {
        let mut queued =
            HashMap::<InterconnectRelayChannel, VecDeque<InterconnectRelayPayload>>::default();
        let mut active = FuturesUnordered::new();
        let mut receiving = true;

        loop {
            tokio::task::consume_budget().await;
            if !receiving && active.is_empty() {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled() => break,
                message = receiver.recv(), if receiving => {
                    let Some(message) = message else {
                        receiving = false;
                        continue;
                    };
                    let channel = InterconnectRelayChannel::from_message(&message);
                    if let Some(channel_queue) = queued.get_mut(&channel) {
                        channel_queue.push_back(message);
                        continue;
                    }
                    queued.insert(channel.clone(), VecDeque::new());
                    active.push(Self::handle(runtime.clone(), channel, message));
                }
                completed = active.next(), if !active.is_empty() => {
                    let Some((channel, result)) = completed else {
                        continue;
                    };
                    if let Err(error) = result {
                        warn!(error = %error, "failed to process remote relay payload");
                    }
                    let next = queued
                        .get_mut(&channel)
                        .and_then(VecDeque::pop_front);
                    if let Some(payload) = next {
                        active.push(Self::handle(runtime.clone(), channel, payload));
                    } else {
                        queued.remove(&channel);
                    }
                }
            }
        }
    }
}

impl SessionServiceImpl {
    pub(in crate::application) async fn dispatch_interconnect_control(
        &self,
        node_id: &ClusterNodeName,
        envelope: ControlEnvelope,
    ) -> Result<(), String> {
        self.inner
            .interconnect
            .send(node_id, Envelope::Control(envelope))
            .await
            .map_err(|error| format!("failed to send interconnect control to '{node_id}': {error}"))
    }
}

#[cfg(test)]
mod tests {
    use nervix_execution::MemoryClass;
    use nervix_interconnect::{ControlEnvelope, Envelope};

    use super::{super::test_fixtures::named, *};

    #[test]
    fn interconnect_control_lane_rejects_a_relay_without_transport_admission() {
        let (lane, mut payloads) = InterconnectRelayPayloadLane::new();
        let peer_node_id = ClusterNodeName::parse("node-2").expect("valid node name");
        let routed = RelayPayload {
            delivery: nervix_interconnect::RelayDelivery {
                channel_incarnation: [1; 16],
                sequence: 0,
            },
            kind: nervix_interconnect::RelayPayloadKind::Routed,
            domain: DomainName::parse("default").expect("valid domain"),
            relay: named("incoming"),
            key: None,
            batch_ipc: nervix_execution::Executor::default()
                .try_charge_owned(MemoryClass::Relay, Vec::new())
                .expect("an empty test body always fits the relay class"),
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: None,
        };

        assert!(
            lane.route(&peer_node_id, Envelope::RelayPayload(routed), None)
                .is_none()
        );
        assert!(payloads.try_recv().is_err());
        assert!(matches!(
            lane.route(
                &peer_node_id,
                Envelope::Control(ControlEnvelope::Terminate),
                None,
            ),
            Some(Envelope::Control(ControlEnvelope::Terminate))
        ));
    }
}
