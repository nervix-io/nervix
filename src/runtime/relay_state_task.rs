//! The task that keeps one materialized relay unit's rows on the node that owns the unit.
//!
//! Layer: data plane.
//!
//! - **Owns.** The state task's lifetime and stop, applying the unit's batches to its replicated
//!   materialized state as the latest record by timestamp, and expiring and evicting the unit's
//!   branches by the relay's TTL and `MAX INSTANCES`.
//! - **Depends on.** The relay's fan-in, its replicated materialized state, the relay interaction,
//!   the domain clock and the unit's ownership freeze.
//! - **Must not know.** Plexus text, transaction planning, consensus, or how a materialized read
//!   reaches the rows.

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

use super::{
    relay_boundary::{RelayRetention, RelayTaskKind, RelayTaskStopError},
    *,
};

/// One materialized relay unit's runtime task: the relay and unit it serves, the replicated state
/// it maintains, the branch retention limits it enforces, and the fan-in it consumes.
pub(super) struct RelayStateTaskSpec {
    pub(super) relay: RelayName,
    /// The unit whose keys this task materializes. Its fan-in registers with this partition's
    /// fixed audience, so sibling partitions occupy none of its queue capacity.
    pub(super) unit: Unit,
    pub(super) partitioning: RelayPartitioning,
    pub(super) state: MaterializedRelayStateOriginator,
    pub(super) retention: RelayRetention,
    pub(super) receiver: RelayRuntimeFanIn,
}

pub(super) struct RelayStateTask {
    pub(super) unit: Unit,
    pub(super) shutdown: watch::Sender<bool>,
    pub(super) task: JoinHandle<()>,
}

impl RelayStateTask {
    pub(super) async fn stop(
        mut self,
        grace: Duration,
    ) -> error_stack::Result<(), RelayTaskStopError> {
        self.shutdown.send_replace(true);
        match nervix_primitives::time::timeout(grace, &mut self.task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(Report::new(error).change_context(RelayTaskStopError::Join {
                task: RelayTaskKind::State,
            })),
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("relay state").await;
                Err(Report::new(RelayTaskStopError::DrainTimeout {
                    task: RelayTaskKind::State,
                    grace,
                }))
            }
        }
    }
}

impl Runtime {
    pub(in crate::runtime) fn spawn_relay_state_task(
        &self,
        domain: &DomainName,
        spec: RelayStateTaskSpec,
    ) -> RelayStateTask {
        let RelayStateTaskSpec {
            relay,
            unit,
            partitioning,
            mut state,
            retention:
                RelayRetention {
                    branch_ttl,
                    branch_capacity,
                },
            receiver,
        } = spec;
        let runtime = self.clone();
        let domain = domain.clone();
        let expiration_scan_interval = self.inner.branch_instance_expiration_scan_interval;
        let (shutdown, shutdown_rx) = watch::channel(false);
        let quiesce_counters =
            self.node_quiesce_counters(&domain, NodeRef::new(ModelKind::Relay, &relay));
        let force_flush = self.force_flush_participant(&domain, quiesce_counters.clone());
        let task = nervix_primitives::task::spawn(async move {
            let interaction_input = RelayInteractionInput::immediate(relay.clone(), receiver);
            let mut interaction = RelayInteraction::new(
                vec![interaction_input],
                shutdown_rx,
                Some(force_flush),
                Some(quiesce_counters),
            )
            .verified(
                "the registry validated this input, and a non-empty input list builds an \
                 interaction",
            );
            let relay_model_name = ModelName::from(&relay);
            let ownership_entity =
                DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay_model_name);
            let freeze_watch = OwnershipHandoffFreezeWatch::for_unit(
                &runtime,
                DomainNodeUnitRef::new(ownership_entity, unit),
            );
            let clock = match runtime.bind_domain_clock(&domain) {
                Ok(clock) => clock,
                Err(error) => {
                    runtime.events().report_error(format!(
                        "materialized relay '{}' in domain '{}' could not bind its clock: \
                         {error:#}",
                        relay.as_str(),
                        domain.as_str()
                    ));
                    return;
                }
            };
            let activated_at = match clock.snapshot() {
                Ok(snapshot) => snapshot.now(),
                Err(error) => {
                    runtime.events().report_error(format!(
                        "materialized relay '{}' in domain '{}' could not read its activation \
                         time: {error:#}",
                        relay.as_str(),
                        domain.as_str(),
                    ));
                    return;
                }
            };
            let mut branch_instances = state.read().restored_branch_instances(activated_at);
            let mut next_expiration_scan = Instant::now() + expiration_scan_interval;
            'state_task: loop {
                nervix_primitives::task::consume_budget().await;
                if !freeze_watch.observe().is_frozen()
                    && let Some(branch_ttl) = branch_ttl
                    && Instant::now() >= next_expiration_scan
                {
                    let now = match clock.snapshot().map(|snapshot| snapshot.now()) {
                        Ok(now) => now,
                        Err(error) => {
                            runtime.events().report_error(format!(
                                "materialized relay '{}' in domain '{}' lost its clock: {error:#}",
                                relay.as_str(),
                                domain.as_str(),
                            ));
                            break 'state_task;
                        }
                    };
                    for (key, _) in branch_instances.expire(now, branch_ttl) {
                        nervix_primitives::task::consume_budget().await;
                        if let Err(error) = runtime.delete_materialized_stream_key(&mut state, &key)
                        {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %format_args!("{error:#}"),
                                "materialized relay assignment changed during branch expiration"
                            );
                            break 'state_task;
                        }
                    }
                    next_expiration_scan = Instant::now() + expiration_scan_interval;
                    continue;
                }
                let expiration_sleep = if freeze_watch.observe().is_frozen() {
                    Some(OWNERSHIP_HANDOFF_FREEZE_RECHECK_INTERVAL)
                } else {
                    branch_ttl.map(|_| {
                        next_expiration_scan
                            .checked_duration_since(Instant::now())
                            .unwrap_or(Duration::ZERO)
                    })
                };
                let mut wake = RuntimeWake::never();
                if let Some(sleep) = expiration_sleep {
                    match RuntimeWake::after(sleep) {
                        Ok(scheduled) => wake = scheduled,
                        Err(error) => {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %format_args!("{error:#}"),
                                "materialized relay state task could not schedule its next scan"
                            );
                            break 'state_task;
                        }
                    }
                }
                let work = match interaction.next(wake).await {
                    Ok(work) => work,
                    Err(failure) => {
                        let reason = format!("{:#}", failure.error);
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %reason,
                            "materialized relay interaction failed"
                        );
                        failure.acks.no_ack(format!(
                            "state task for relay '{}' failed: {reason}",
                            relay.as_str()
                        ));
                        continue;
                    }
                };
                let (event, _work) = work.into_parts();
                let batch = match event {
                    RelayInteractionEvent::Batch {
                        relay: input_relay,
                        batch,
                    } => {
                        debug_assert_eq!(input_relay, relay);
                        // The fan-in carries the batches of every unit this node owns and of the
                        // units whose owners route to its consumers; another unit's keys are that
                        // unit's state, never this one's.
                        if partitioning.unit_of(&batch.key).ok() != Some(unit) {
                            continue;
                        }
                        batch
                    }
                    RelayInteractionEvent::Wake => continue,
                    RelayInteractionEvent::ForceFlush(completion) => {
                        completion.complete();
                        continue;
                    }
                    RelayInteractionEvent::Command(command) => match command {},
                    RelayInteractionEvent::Stopped(reason) => {
                        debug!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            ?reason,
                            "materialized relay interaction stopped"
                        );
                        break;
                    }
                };
                let branch_key = batch.key.clone();
                let now = match clock.snapshot().map(|snapshot| snapshot.now()) {
                    Ok(now) => now,
                    Err(error) => {
                        let reason = format!(
                            "materialized relay '{}' in domain '{}' lost its clock: {error:#}",
                            relay.as_str(),
                            domain.as_str(),
                        );
                        runtime.events().report_error(reason.clone());
                        for ack in batch.acks.iter() {
                            ack.no_ack(reason.clone());
                        }
                        break;
                    }
                };
                branch_instances
                    .get_or_try_create_with(branch_key.clone(), now, |_, _| {
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .assured("the tracking closure's error type is Infallible");
                if let Some(branch_capacity) = branch_capacity {
                    for (evicted_key, _) in branch_instances.evict_lru_to_capacity(branch_capacity)
                    {
                        nervix_primitives::task::consume_budget().await;
                        if let Err(error) =
                            runtime.delete_materialized_stream_key(&mut state, &evicted_key)
                        {
                            warn!(
                                domain = domain.as_str(),
                                relay = relay.as_str(),
                                error = %format_args!("{error:#}"),
                                "materialized relay assignment changed during branch eviction"
                            );
                            break 'state_task;
                        }
                    }
                }
                let messages = match batch.try_into_messages() {
                    Ok(messages) => messages,
                    Err(error_and_batch) => {
                        let error = error_and_batch.error;
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            scope = %BranchScope::from(&branch_key),
                            error = %format_args!("{error:#}"),
                            "failed to decode scheduled materialized relay batch"
                        );
                        continue;
                    }
                };
                let records = messages.into_iter().map(|message| message.record);
                if let Err(error) = runtime
                    .apply_materialized_stream_records(&mut state, &branch_key, records)
                    .await
                {
                    warn!(
                        domain = domain.as_str(),
                        relay = relay.as_str(),
                        error = %format_args!("{error:#}"),
                        "materialized relay assignment changed while applying a batch"
                    );
                    break 'state_task;
                }
            }
        });
        RelayStateTask {
            unit,
            shutdown,
            task,
        }
    }
}
