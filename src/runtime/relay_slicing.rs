//! Ordered relay re-slicing after a definitive pre-admission receive-bound refusal.
//!
//! Layer: data plane.
//! - **Owns.** Contiguous row ranges, unchanged delivery identities on refusal and admitted prefixes.
//! - **Depends on.** Runtime batch slicing, retained channels and remote admission ownership.
//! - **Must not know.** Models, placement decisions or connector protocols.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns bounded relay re-slicing")
)]

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{RelayPayload, RelaySliceCursor};
use nervix_models::ClusterNodeName;
use nervix_primitives::time::Instant;

use super::{
    relay_boundary::{RelayOutboundSlot, UnusedRelayPosition},
    relay_send_attempts::GrantCapacityRefusals,
    remote_dispatch::{PendingRemoteAckDelivery, RemoteDispatchError, RemoteDispatcher},
};
use crate::runtime_schema::{RuntimeRecordBatch, batch_payload_bytes};

impl RemoteDispatcher {
    /// The ordinary path sends the original shared body once. A refusal creates no receiver
    /// attempt, so its sequence can carry a smaller prefix; accepted prefixes keep their ACKs.
    #[expect(
        clippy::too_many_arguments,
        reason = "the encoded payload, borrowed rows, channel gate and ACK registrations retain \
                  independent owners; the caller supplies its deadline and capacity-refusal policy"
    )]
    pub(super) async fn dispatch_sliced_relay_payload(
        &self,
        node: &ClusterNodeName,
        mut payload: RelayPayload,
        batch: &RuntimeRecordBatch,
        channel: &RelayOutboundSlot,
        deadline: Instant,
        capacity: GrantCapacityRefusals,
        mut acknowledgements: Option<&mut PendingRemoteAckDelivery>,
    ) -> Result<(), Report<RemoteDispatchError>> {
        let mut unused_position = UnusedRelayPosition::new(channel, payload.delivery);
        #[cfg(feature = "testing")]
        self.fault_injection
            .pause_relay_before_grant_if_armed(&payload.domain)
            .await;
        unused_position.disarm();
        match self
            .dispatch_admitted_relay_payload(node, &mut payload, channel, deadline, capacity)
            .await
        {
            Ok(()) => {
                #[cfg(feature = "testing")]
                self.fault_injection.observe_capability_relay_admission(
                    &payload.domain,
                    node,
                    batch.batch().num_rows(),
                    payload.batch_ipc.len(),
                );
                if let Some(owner) = acknowledgements {
                    owner.admit();
                }
                Ok(())
            }
            Err(error) => {
                let RemoteDispatchError::ReceiveBounds { bounds, .. } = error.current_context()
                else {
                    return Err(error);
                };
                let mut cursor = RelaySliceCursor::new(batch.batch().num_rows());
                cursor.refuse(*bounds);
                let mut delivery = payload.delivery;
                unused_position.retain(delivery);
                if cursor.total_rows() == 0 {
                    return Err(error);
                }
                while cursor.admitted_rows() < cursor.total_rows() {
                    nervix_primitives::task::consume_budget().await;
                    if Instant::now() >= deadline {
                        return Err(Report::new(RemoteDispatchError::AdmissionTotalTimeout {
                            target: node.clone(),
                            timeout: super::remote_dispatch::REMOTE_RELAY_TOTAL_TIMEOUT,
                        }));
                    }
                    let selected = batch
                        .slice(cursor.admitted_rows(), cursor.part_rows())
                        .change_context(RemoteDispatchError::Send {
                            target: node.clone(),
                        })?;
                    let decoded = batch_payload_bytes(selected.batch());
                    let bounds = cursor
                        .receive_bounds()
                        .assured("a sizing refusal supplied receive bounds");
                    if decoded > bounds.relay_decoded_bytes.get() {
                        if cursor.shrink() {
                            continue;
                        }
                        #[cfg(feature = "testing")]
                        self.fault_injection.observe_capability_relay_refusal(
                            &payload.domain,
                            node,
                            delivery,
                            batch.batch().num_rows(),
                        );
                        return Err(Report::new(RemoteDispatchError::RowAboveReceiveBounds {
                            target: node.clone(),
                        }));
                    }
                    let encoded = selected
                        .encode_arrow_ipc(self.executor())
                        .await
                        .change_context(RemoteDispatchError::Send {
                            target: node.clone(),
                        })?;
                    if u64::try_from(encoded.len()).assured("body length fits u64")
                        > bounds.relay_batch_bytes.get()
                    {
                        if cursor.shrink() {
                            continue;
                        }
                        #[cfg(feature = "testing")]
                        self.fault_injection.observe_capability_relay_refusal(
                            &payload.domain,
                            node,
                            delivery,
                            batch.batch().num_rows(),
                        );
                        return Err(Report::new(RemoteDispatchError::RowAboveReceiveBounds {
                            target: node.clone(),
                        }));
                    }
                    let end = cursor
                        .admitted_rows()
                        .checked_add(cursor.part_rows())
                        .assured("the selected range fits the batch");
                    let mut part = RelayPayload {
                        delivery,
                        decoded_bytes: decoded,
                        kind: payload.kind,
                        domain: payload.domain.clone(),
                        relay: payload.relay.clone(),
                        unit: payload.unit,
                        key: payload.key.clone(),
                        batch_ipc: encoded,
                        metadata: payload.metadata[cursor.admitted_rows()..end].to_vec(),
                        acks: payload.acks[cursor.admitted_rows()..end].to_vec(),
                        admission: None,
                    };
                    unused_position.disarm();
                    match self
                        .dispatch_admitted_relay_payload(
                            node, &mut part, channel, deadline, capacity,
                        )
                        .await
                    {
                        Ok(()) => {
                            #[cfg(feature = "testing")]
                            self.fault_injection.observe_capability_relay_admission(
                                &payload.domain,
                                node,
                                cursor.part_rows(),
                                part.batch_ipc.len(),
                            );
                            cursor.admit();
                            if let Some(owner) = acknowledgements.as_mut() {
                                owner.admit_prefix(cursor.admitted_rows());
                            }
                            if cursor.admitted_rows() < cursor.total_rows() {
                                delivery = channel.next_delivery();
                                unused_position.retain(delivery);
                            }
                        }
                        Err(error) => {
                            if let RemoteDispatchError::ReceiveBounds { bounds, .. } =
                                error.current_context()
                            {
                                unused_position.retain(delivery);
                                cursor.refuse(*bounds);
                            } else {
                                return Err(error);
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests exact row admission ownership")
)]
mod tests {
    use super::*;
    use crate::{
        runtime::{
            Runtime, remote_dispatch::RemoteAckStorage, test_fixtures::attach_loopback_cluster,
        },
        runtime_ack::{AckOutcome, AckSet},
    };

    #[nervix_primitives::test]
    async fn cancelling_after_a_prefix_admission_fails_only_the_unadmitted_rows() {
        let runtime = Runtime::default();
        let local = ClusterNodeName::parse("sender").expect("node name");
        attach_loopback_cluster(&runtime, &local).await;
        let dispatcher = runtime
            .inner
            .remote_dispatcher
            .load_full()
            .expect("attached dispatcher");
        let receiver = ClusterNodeName::parse("receiver").expect("node name");
        let (first, first_completion) = AckSet::root();
        let (second, second_completion) = AckSet::root();
        let (third, third_completion) = AckSet::root();
        let storage = dispatcher
            .store_pending_acks(
                &receiver,
                vec![first, second, third],
                RemoteDispatcher::delivery_deadline(),
                std::future::pending(),
            )
            .await
            .expect("three rows fit");
        let RemoteAckStorage::Stored(mut delivery) = storage else {
            panic!("the uncancelled delivery stores every row");
        };
        let registrations = delivery.registrations().to_vec();
        delivery.admit_prefix(1);
        delivery.admit_prefix(1);
        assert_eq!(delivery.admitted_rows(), 1);
        drop(delivery);
        let admitted = registrations[0].as_ref().expect("first row registration");
        assert!(dispatcher.registry.holds_ack(admitted.ack_id));
        for registration in registrations[1..].iter().flatten() {
            assert!(!dispatcher.registry.holds_ack(registration.ack_id));
        }
        assert!(
            dispatcher
                .registry
                .resolve_ack(admitted.ack_id, AckOutcome::Ack)
        );
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
        for completion in [second_completion, third_completion] {
            assert_eq!(
                completion.wait().await,
                AckOutcome::NoAck("remote delivery ended before admission".to_string())
            );
        }
        assert_eq!(
            dispatcher.executor.snapshot().relay_memory.reserved_bytes,
            0
        );
        runtime.shutdown(runtime.domain_drain_timeout()).await;
    }
}
