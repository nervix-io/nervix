//! The relay owner's routes to the runtime consumers of its relays on other nodes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Delivering an owner's batch to each remote consumer destination of its unit, and
//!   replacing those destinations when a schedule publishes them.
//! - **Depends on.** The relay's routing channels, the interconnect dispatcher and the remote
//!   acknowledgement registrations.
//! - **Must not know.** Plexus text, transactions, consensus operations, or connector internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
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

impl RelayBoundaryServices {
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
    pub(super) async fn dispatch_remote_runtime_consumers(
        &self,
        domain: &DomainName,
        unit: Unit,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        let remote_runtime_consumers = self.remote_runtime_consumers.load_full();
        if remote_runtime_consumers.is_empty() {
            return Ok(());
        }
        // A consumer's node reports the batch's records alive to their sources once it has admitted
        // them, and not before, so this node reports them alive while their deliveries wait.
        self.remote_ack_timing
            .keep_alive_while_pending(
                batch,
                self.deliver_to_remote_runtime_consumers(
                    domain,
                    unit,
                    batch,
                    &remote_runtime_consumers,
                ),
            )
            .await
    }

    async fn deliver_to_remote_runtime_consumers(
        &self,
        domain: &DomainName,
        unit: Unit,
        batch: &RelayRecordBatch,
        remote_runtime_consumers: &[RemoteRuntimeConsumer],
    ) -> RelayDispatchResult {
        let Some(dispatcher) = &self.remote_dispatcher else {
            if remote_runtime_consumers
                .iter()
                .any(|consumer| consumer.mode_for(unit) == Some(AckMode::Attached))
            {
                for ack in batch.acks.iter() {
                    ack.no_ack("remote dispatcher unavailable for attached delivery");
                }
                return Err(Box::new(batch.clone()));
            }
            return Ok(());
        };
        // Every consumer of this relay receives the same columns; only the acknowledgement
        // obligations differ, so the body is encoded once for the whole fanout and each
        // destination carries a handle to that one allocation.
        //
        // The encode happens inside the first destination's outbound slot rather than ahead of
        // the loop. A destination's slot is what orders the batches published to it, and an
        // encode is long enough that hoisting it out lets a later batch finish serializing first
        // and reach the slot ahead of an earlier one, delivering the relay out of order.
        let mut encoded_body: Option<(ChargedBytes, u64)> = None;
        for consumer in remote_runtime_consumers.iter() {
            let Some(mode) = consumer.mode_for(unit) else {
                continue;
            };
            nervix_primitives::task::consume_budget().await;
            let outbound_slot = self.outbound_slot(
                &consumer.node_id,
                &consumer.relay,
                RelayPayloadKind::Routed,
                &batch.key,
                unit,
            );
            let Some(_slot) = outbound_slot.lock_for_delivery().await else {
                for ack in batch.acks.iter() {
                    ack.no_ack("relay channel lifetime ended");
                }
                return Err(Box::new(batch.clone()));
            };
            let remote_batch = match mode {
                AckMode::Attached => batch.attached(),
                AckMode::Detached => batch.detached(),
            };
            // The records are stored before the body is encoded, so the first destination that
            // waits for room to store them holds no encoded body meanwhile.
            let timing = dispatcher.registry.timing();
            let deadline = dispatcher.delivery_deadline(timing);
            let mut registration_owner = if mode == AckMode::Attached {
                let storage = dispatcher
                    .store_pending_acks(
                        &consumer.node_id,
                        remote_batch.acks.to_vec(),
                        deadline,
                        timing,
                        outbound_slot.cancellation().cancelled(),
                    )
                    .await;
                match storage {
                    Ok(RemoteAckStorage::Stored(owner)) => Some(owner),
                    Ok(RemoteAckStorage::Cancelled { unstored }) => {
                        for ack in unstored.iter().chain(batch.acks.iter()) {
                            ack.no_ack("relay channel lifetime ended");
                        }
                        return Err(Box::new(batch.clone()));
                    }
                    Err(error) => {
                        let reason = format!("{error:#}");
                        for ack in &remote_batch.acks {
                            ack.no_ack(reason.clone());
                        }
                        return Err(Box::new(batch.clone()));
                    }
                }
            } else {
                None
            };
            let (batch_ipc, decoded_bytes) = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(dispatcher.executor()).await {
                    Ok(bytes) => {
                        let bytes = (
                            bytes,
                            crate::runtime_schema::batch_payload_bytes(batch.batch.batch()),
                        );
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        if let Some(owner) = &registration_owner {
                            for registration in owner.registrations().iter().flatten() {
                                dispatcher.clear_pending_ack(registration);
                            }
                        }
                        if remote_runtime_consumers
                            .iter()
                            .any(|consumer| consumer.mode_for(unit) == Some(AckMode::Attached))
                        {
                            for ack in batch.acks.iter() {
                                ack.no_ack(format!("{error:#}"));
                            }
                            return Err(Box::new(batch.clone()));
                        }
                        warn!(
                            error = %format_args!("{error:#}"),
                            "failed to serialize detached remote relay batch"
                        );
                        return Ok(());
                    }
                },
            };
            let remote_acks = match &registration_owner {
                Some(owner) => owner.registrations().to_vec(),
                None => vec![None; remote_batch.acks.len()],
            };
            let delivery = outbound_slot.next_delivery_with_policy(timing);
            let result = dispatcher
                .dispatch_sliced_relay_payload(
                    &consumer.node_id,
                    routed_payload(RoutedDelivery {
                        ack_timing: timing,
                        decoded_bytes,
                        delivery,
                        domain,
                        unit,
                        consumer,
                        batch: &remote_batch,
                        batch_ipc: batch_ipc.clone(),
                        acks: remote_acks.clone(),
                    }),
                    &remote_batch.batch,
                    RelayDeliveryChannel::new(&outbound_slot, None),
                    deadline,
                    GrantCapacityRefusals::Hold,
                    registration_owner.as_mut(),
                )
                .await;

            match (mode, result) {
                (AckMode::Attached, Ok(())) => {}
                (AckMode::Attached, Err(error)) => {
                    let reason = format!("{error:#}");
                    let admitted = registration_owner
                        .as_ref()
                        .verified("attached delivery has a registration owner")
                        .admitted_rows();
                    for (ack_set, remote_ack) in remote_batch.acks[admitted..]
                        .iter()
                        .zip(remote_acks[admitted..].iter())
                    {
                        if let Some(remote_ack) = remote_ack {
                            dispatcher.clear_pending_ack(remote_ack);
                        }
                        ack_set.no_ack(reason.clone());
                    }
                    return Err(Box::new(batch.clone()));
                }
                (AckMode::Detached, Err(error)) => {
                    warn!(
                        error = %format_args!("{error:#}"),
                        target_node = %consumer.node_id,
                        "detached remote delivery failed"
                    );
                }
                (AckMode::Detached, Ok(())) => {}
            }
        }

        Ok(())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "schedule publication replaces the relay's consumer routes"
        )
    )]
    pub(in crate::runtime) fn replace_remote_runtime_consumers(
        &self,
        consumers: Vec<RemoteRuntimeConsumer>,
    ) {
        if *self.remote_runtime_consumers.load().as_ref() == consumers {
            return;
        }
        self.channels
            .replace_owner_of(&self.changed_consumer_units(&consumers));
        self.remote_runtime_consumers.store(StdArc::new(consumers));
    }

    /// Stops routing new batches to the remote consumers a drop destroyed, by installing
    /// `remaining`, this relay's remote consumers without them. It is installed only when it
    /// withdraws destinations and changes nothing else: every destination it keeps for a unit was
    /// routed that unit before, with the same mode or detached where it was attached. Any other
    /// change waits for the schedule application that installs it behind its dispatch fence.
    ///
    /// No routing generation is replaced, so no channel of a destination that stays is
    /// interrupted, and a fan-out that already holds a withdrawn destination's channel delivers
    /// there, where the share of a consumer the drop destroyed counts as done. A withdrawn
    /// destination's idle channels end with their branch or with the unit's next routing
    /// generation.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "a drop withdraws the relay's routes to the consumers it destroyed"
        )
    )]
    pub(in crate::runtime) fn withdraw_dropped_remote_runtime_consumers(
        &self,
        remaining: Vec<RemoteRuntimeConsumer>,
    ) {
        let current = self.remote_runtime_consumers.load_full();
        if *current == remaining {
            return;
        }
        if !self.only_withdraws(&current, &remaining) {
            return;
        }
        self.remote_runtime_consumers.store(StdArc::new(remaining));
    }

    /// Whether `next` routes each unit to a subset of the destinations `current` routes it to,
    /// each with the mode it had there or detached where it was attached.
    fn only_withdraws(
        &self,
        current: &[RemoteRuntimeConsumer],
        next: &[RemoteRuntimeConsumer],
    ) -> bool {
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        struct Destination<'a> {
            node_id: &'a ClusterNodeName,
            relay: &'a RelayName,
        }
        let mut routed = BTreeMap::new();
        for consumer in current {
            let destination = Destination {
                node_id: &consumer.node_id,
                relay: &consumer.relay,
            };
            routed.insert(destination, consumer);
        }
        let units = self.partitioning.units();
        for consumer in next {
            let destination = Destination {
                node_id: &consumer.node_id,
                relay: &consumer.relay,
            };
            let Some(before) = routed.get(&destination) else {
                return false;
            };
            for unit in &units {
                let Some(after) = consumer.mode_for(*unit) else {
                    continue;
                };
                match (before.mode_for(*unit), after) {
                    (Some(AckMode::Attached), _) | (Some(AckMode::Detached), AckMode::Detached) => {
                    }
                    (Some(AckMode::Detached), AckMode::Attached) | (None, _) => return false,
                }
            }
        }
        true
    }

    pub(super) fn changed_consumer_units(&self, next: &[RemoteRuntimeConsumer]) -> Vec<Unit> {
        let current = self.remote_runtime_consumers.load();
        self.partitioning
            .units()
            .into_iter()
            .filter(|unit| {
                let before = current.iter().filter_map(|consumer| {
                    consumer
                        .mode_for(*unit)
                        .map(|mode| (&consumer.node_id, &consumer.relay, mode))
                });
                let after = next.iter().filter_map(|consumer| {
                    consumer
                        .mode_for(*unit)
                        .map(|mode| (&consumer.node_id, &consumer.relay, mode))
                });
                !before.eq(after)
            })
            .collect()
    }
}
