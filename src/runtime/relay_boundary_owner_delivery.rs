//! Delivery of a relay batch to the owner of its relay unit on another node.
//!
//! Layer: data plane.
//!
//! - **Owns.** One owner delivery of a producer's batch, from its dispatch gates through the
//!   storage of its acknowledgement records to its admission, and how it ends: admitted, failed,
//!   or carried on to the unit's owner on the route that replaced the one it was sent on.
//! - **Depends on.** The relay boundary's channels, routes and dispatch gates, the remote
//!   dispatcher and the interconnect's report of whether a send reached its receiver.
//! - **Must not know.** Plexus text, placement policy, transactions or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        data_plane,
        reason = "owns delivery of relay batches to their unit's owner"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "relay membership and channel installation establish retained concrete branch \
                  and delivery handles"
    )
)]

use super::*;
use crate::runtime::remote_dispatch::RemoteDispatchError;

/// How a delivery of a batch to the owner of its relay unit on another node ended.
#[derive(Debug)]
pub(in crate::runtime) enum OwnerDelivery {
    /// The owner admitted every row of the batch.
    Admitted,
    /// The rows no owner admitted, whose acknowledgements the delivery failed.
    Failed(Box<RelayRecordBatch>),
    /// The unit's route changed before the delivery reached the owner it had named, so no owner
    /// admitted these rows. Their acknowledgements are untouched, and they go to the unit's owner
    /// on its current route.
    Rerouted(Box<RelayRecordBatch>),
}

impl RelayBoundaryServices {
    /// Delivers `batch` to the owner of `unit` on another node.
    ///
    /// The owner reports the batch's records alive to their sources once it has admitted them, and
    /// not before. Until then this node holds them on their sources' behalf, so it reports them
    /// alive itself while the delivery waits for its gates, its channel, the storage of its
    /// records, a grant or the owner's admission. An ingestor's `ACK TIMEOUT` therefore measures how
    /// long its records make no progress, not how long they take to reach their owner.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the retained channel serializes sequence and publication changes",
            key = "delivery channel incarnation and sequence",
            bound = "one admitted delivery is serialized against channel replacement and its \
                     dispatch gate"
        )
    )]
    pub(in crate::runtime) async fn dispatch_to_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
        unit: Unit,
    ) -> OwnerDelivery {
        let branch = self.channels.bind_relay(&batch.key, unit);
        self.dispatch_to_owner_through(domain, relay, unit, batch, &branch, None)
            .await
    }

    /// Delivers `batch` to the owner of `unit` on another node through the channels of the branch
    /// incarnation `branch`, as [`Self::dispatch_to_owner`] does, for the producer whose own
    /// lifetime `producer_lifetime` names when it has one.
    pub(super) async fn dispatch_to_owner_through(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        batch: &RelayRecordBatch,
        branch: &RelayBranchChannels,
        producer_lifetime: Option<&CancellationToken>,
    ) -> OwnerDelivery {
        self.remote_ack_timing
            .keep_alive_while_pending(
                batch,
                self.deliver_to_owner(domain, relay, unit, batch, branch, producer_lifetime),
            )
            .await
    }

    async fn deliver_to_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        unit: Unit,
        batch: &RelayRecordBatch,
        branch: &RelayBranchChannels,
        producer_lifetime: Option<&CancellationToken>,
    ) -> OwnerDelivery {
        let unit_gate = self.unit_dispatch_gate(unit);
        let dispatch_gate = self.fanout.dispatch_gate();
        let permits = async {
            let unit = match &unit_gate {
                Some(gate) => Some(gate.acquire_dispatch().await),
                None => None,
            };
            let relay = dispatch_gate.acquire_dispatch().await;
            let branches = self.fanout.acquire_branch_dispatch_gates(&batch.key).await;
            (unit, relay, branches)
        };
        let permits = match producer_lifetime {
            Some(lifetime) => lifetime.run_until_cancelled(permits).await,
            None => Some(permits.await),
        };
        let Some(_permits) = permits else {
            return reject_owner_delivery(batch, "relay producer lifetime ended");
        };
        let Some(owner_node) = self.owner_of(unit) else {
            return reject_owner_delivery(batch, "relay owner is unavailable");
        };
        let Some(dispatcher) = &self.remote_dispatcher else {
            return reject_owner_delivery(
                batch,
                "remote dispatcher is unavailable for relay owner delivery",
            );
        };
        if owner_node == *dispatcher.local_node_id() {
            // A route change that the gates held this delivery back for moved the unit to this
            // node. Its owner buffer here takes the batch.
            return OwnerDelivery::Rerouted(Box::new(batch.clone()));
        }
        // The channel is taken under the gates, so it belongs to the route the owner above was
        // read from, including a route change the gates held this delivery back for.
        let ingress_slot = branch.ingress();
        let channel = RelayDeliveryChannel::new(&ingress_slot, producer_lifetime);
        let Some(_slot) = channel
            .while_producer_lives(channel.slot.lock_for_delivery())
            .await
            .flatten()
        else {
            return self.end_unsent_owner_delivery(unit, branch, batch, channel);
        };
        // The delivery's records are stored before its body is encoded, so a delivery that waits
        // for room to store them holds no encoded body meanwhile.
        let timing = dispatcher.registry.timing();
        let deadline = dispatcher.delivery_deadline(timing);
        let forwarded = batch
            .acks
            .iter()
            .map(RemoteDispatcher::forwarded_ack)
            .collect();
        let storage = dispatcher
            .store_pending_acks(
                &owner_node,
                forwarded,
                deadline,
                timing,
                channel.cancelled(),
            )
            .await;
        let mut remote_acks = match storage {
            Ok(RemoteAckStorage::Stored(registrations)) => registrations,
            Ok(RemoteAckStorage::Cancelled { unstored }) => {
                // Nothing was stored for the owner, so the shares this delivery forwarded end
                // here, and each record keeps only the shares its batch holds, which the batch's
                // next delivery or its failure decides.
                for forwarded in unstored {
                    forwarded.ack_success();
                }
                return self.end_unsent_owner_delivery(unit, branch, batch, channel);
            }
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    scope = %BranchScope::from(&batch.key),
                    target_node = %owner_node,
                    error = %format_args!("{error:#}"),
                    "failed to store the acknowledgements of a relay batch for its owner"
                );
                let reason = format!("{error:#}");
                for ack in &batch.acks {
                    ack.no_ack(reason.clone());
                }
                return OwnerDelivery::Failed(Box::new(batch.clone()));
            }
        };
        let encoding = batch.batch.encode_arrow_ipc(dispatcher.executor());
        let batch_ipc = match channel.while_producer_lives(encoding).await {
            Some(Ok(bytes)) => bytes,
            Some(Err(error)) => {
                for registration in remote_acks.registrations().iter().flatten() {
                    dispatcher.clear_pending_ack(registration);
                }
                for ack in batch.acks.iter() {
                    ack.no_ack(format!("{error:#}"));
                }
                return OwnerDelivery::Failed(Box::new(batch.clone()));
            }
            None => return reject_owner_delivery(batch, channel.cancellation_reason()),
        };
        let delivery = channel.slot.next_delivery_with_policy(remote_acks.timing());
        let admission_result = dispatcher
            .dispatch_sliced_relay_payload(
                &owner_node,
                RelayPayload {
                    ack_timing: remote_acks.timing(),
                    decoded_bytes: crate::runtime_schema::batch_payload_bytes(batch.batch.batch()),
                    delivery,
                    kind: RelayPayloadKind::Ingress,
                    domain: domain.clone(),
                    relay: relay.clone(),
                    unit,
                    key: BranchKey::to_remote_key(&batch.key),
                    batch_ipc,
                    metadata: batch.metadata.to_remote(),
                    acks: remote_acks.registrations().to_vec(),
                    admission: None,
                },
                &batch.batch,
                channel,
                deadline,
                Some(&mut remote_acks),
            )
            .await;
        let Err(error) = admission_result else {
            return OwnerDelivery::Admitted;
        };
        let admitted = remote_acks.admitted_rows();
        let remaining = batch
            .acks
            .len()
            .checked_sub(admitted)
            .verified("the admission owner counts rows of this batch");
        let unadmitted = Box::new(
            batch
                .slice(admitted, remaining)
                .verified("the unadmitted suffix lies within this batch"),
        );
        if let RemoteDispatchError::Unreached { .. } = error.current_context()
            && channel.slot.cancellation().is_cancelled()
            && !channel.producer_ended()
            && branch.is_live()
        {
            // A route change retired the channel while the delivery was still trying to reach
            // the owner it had named, and nothing of it reached that owner. The rows no owner
            // admitted go to the unit's owner on the new route instead.
            debug!(
                domain = domain.as_str(),
                relay = relay.as_str(),
                scope = %BranchScope::from(&batch.key),
                former_owner = %owner_node,
                rows = remaining,
                "relay batch follows its unit's new route before reaching its former owner"
            );
            remote_acks.release_unadmitted();
            return OwnerDelivery::Rerouted(unadmitted);
        }
        warn!(
            domain = domain.as_str(),
            relay = relay.as_str(),
            scope = %BranchScope::from(&batch.key),
            target_node = %owner_node,
            error = %format_args!("{error:#}"),
            "failed to admit relay batch on its owner"
        );
        let reason = format!("{error:#}");
        for registration in remote_acks.registrations()[admitted..].iter().flatten() {
            dispatcher.clear_pending_ack(registration);
        }
        for ack in batch.acks[admitted..].iter() {
            ack.no_ack(reason.clone());
        }
        OwnerDelivery::Failed(unadmitted)
    }

    /// Ends a delivery whose channel ended before the delivery sent anything on it. While a route
    /// change is being installed for the unit of a live branch, and the producer still lives, the
    /// batch goes to the owner that change installs: the change holds the unit's dispatch gate, so
    /// the next delivery waits for it there. Otherwise the producer, the channel's branch or its
    /// relay ended, and the batch fails.
    fn end_unsent_owner_delivery(
        &self,
        unit: Unit,
        branch: &RelayBranchChannels,
        batch: &RelayRecordBatch,
        channel: RelayDeliveryChannel<'_>,
    ) -> OwnerDelivery {
        if !channel.producer_ended() && branch.is_live() && self.dispatch_is_fenced(unit) {
            return OwnerDelivery::Rerouted(Box::new(batch.clone()));
        }
        reject_owner_delivery(batch, channel.cancellation_reason())
    }
}

/// Fails every record of an owner delivery for `reason`, as [`reject_relay_delivery`] does.
fn reject_owner_delivery(batch: &RelayRecordBatch, reason: &'static str) -> OwnerDelivery {
    for ack in &batch.acks {
        ack.no_ack(reason);
    }
    OwnerDelivery::Failed(Box::new(batch.clone()))
}
