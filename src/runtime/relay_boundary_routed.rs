//! Remote relay payload construction and consumer dispatch classification.
//!
//! Layer: data plane.
//!
//! - **Owns.** Destination-specific relay envelopes and the acknowledgement meaning of routed input.
//! - **Depends on.** Typed relay delivery, shared encoded batches and runtime consumer handles.
//! - **Must not know.** Plexus text, placement policy, connector drivers or persistence.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "owns routed relay envelopes and dispatch classification"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "retained relay routing handles are installed at lifecycle boundaries"
    )
)]

use super::*;

/// One destination's share of a relay fanout.
///
/// The body is a parameter rather than something this builds, because the fanout encodes it once
/// and every destination carries a handle to that one allocation. Only the target relay and the
/// acknowledgement obligations differ between destinations.
pub(in crate::runtime) struct RoutedDelivery<'a> {
    pub(in crate::runtime) ack_timing: nervix_models::RemoteAckTiming,
    pub(in crate::runtime) decoded_bytes: u64,
    pub(in crate::runtime) delivery: RelayDelivery,
    pub(in crate::runtime) domain: &'a DomainName,
    /// The unit of the relay whose owner routes the batch.
    pub(in crate::runtime) unit: Unit,
    pub(in crate::runtime) consumer: &'a RemoteRuntimeConsumer,
    pub(in crate::runtime) batch: &'a RelayRecordBatch,
    pub(in crate::runtime) batch_ipc: ChargedBytes,
    pub(in crate::runtime) acks: Vec<Option<RemoteAckRegistration>>,
}

#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "retained relay handles route each admitted record and keep channel ordering and \
                  fences"
    )
)]
pub(in crate::runtime) fn routed_payload(delivery: RoutedDelivery<'_>) -> RelayPayload {
    RelayPayload {
        ack_timing: delivery.ack_timing,
        decoded_bytes: delivery.decoded_bytes,
        delivery: delivery.delivery,
        kind: RelayPayloadKind::Routed,
        domain: delivery.domain.clone(),
        relay: delivery.consumer.relay.clone(),
        unit: delivery.unit,
        key: BranchKey::to_remote_key(&delivery.batch.key),
        batch_ipc: delivery.batch_ipc,
        metadata: delivery.batch.metadata.to_remote(),
        acks: delivery.acks,
        admission: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::runtime) struct RemoteRuntimeConsumer {
    pub(in crate::runtime) node_id: ClusterNodeName,
    pub(in crate::runtime) relay: RelayName,
    pub(in crate::runtime) mode: Option<AckMode>,
    pub(in crate::runtime) unit_modes: BTreeMap<Unit, AckMode>,
}

impl RemoteRuntimeConsumer {
    pub(in crate::runtime) fn mode_for(&self, unit: Unit) -> Option<AckMode> {
        match (self.mode, self.unit_modes.get(&unit).copied()) {
            (Some(AckMode::Attached), _) | (_, Some(AckMode::Attached)) => Some(AckMode::Attached),
            (Some(AckMode::Detached), _) | (_, Some(AckMode::Detached)) => Some(AckMode::Detached),
            (None, None) => None,
        }
    }
}

/// How a batch reaches this node's runtime consumers of a relay, which decides what reaching no
/// attached consumer here means.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) enum RuntimeConsumerDispatch {
    /// The relay owner fans out a batch of its own relay. Its remote consumers can take what no
    /// local consumer does, and a relay that no attached consumer reads leaves nothing to wait for.
    Owner,
    /// The owner of the relay on another node routed the batch here, because its schedule places an
    /// attached consumer of the relay on this node.
    Routed,
}

impl RuntimeConsumerDispatch {
    /// Whether a batch that carries record acknowledgements must reach an attached consumer on this
    /// node.
    ///
    /// A routed batch that finds none was routed for a consumer that has since left the node, as
    /// when a forced recovery moves it off a node the scheduler judged unavailable while the batch
    /// was in flight. Nothing here can complete the work its acknowledgements stand for, so
    /// completing them would report records no consumer processed.
    pub(super) fn requires_local_attached_consumer(self) -> bool {
        match self {
            Self::Owner => false,
            Self::Routed => true,
        }
    }
}
