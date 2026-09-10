//! Remote relay dispatch, admission, and acknowledgement coordination.
//!
//! Layer: data plane.
//!
//! - **Owns.** Relay wire encoding and decoding, epoch-scoped admission reconciliation,
//!   cancellation, progress handling, and remote acknowledgement correlation.
//! - **Depends on.** Branch-local relay boundaries, execution admission, cluster membership, and
//!   the authenticated interconnect.
//! - **Must not know.** NSPL text, transactions, scheduling policy, or connector internals.

use super::*;

pub(super) const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);

pub(super) const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);

pub(super) const REMOTE_ACK_ALIVE_INTERVAL: Duration = Duration::from_millis(100);

pub(super) const REMOTE_RELAY_TOTAL_TIMEOUT: Duration = Duration::from_secs(300);

const REMOTE_RELAY_BRANCH_EVICTED: &str = "relay branch generation was evicted before admission";

enum FailedRelayAdmissionResolution {
    Admitted,
    Failed(String),
    Indeterminate(String),
}

#[derive(Debug, Clone)]
pub(super) enum RelayAdmissionUpdate {
    Pending,
    Alive,
    Admitted,
    Rejected(String),
}

struct RemoteRelayAdmissionContext<'a> {
    registration: &'a RemoteAckRegistration,
    transport: &'a RelayAdmission,
    admitted: &'a mut bool,
}

/// Node identity and the remote acknowledgement correlation registry. The runtime and the
/// `RemoteDispatcher` it attaches must observe one instance of this: the dispatcher allocates the
/// correlation ids that the runtime resolves when acknowledgements come back over the
/// interconnect, and both answer questions about which node they are running on.
pub(super) struct RemoteDispatchRegistry {
    pub(super) local_node_id: RwLock<Option<ClusterNodeName>>,
    pub(super) local_node_incarnation: RwLock<Option<ClusterNodeIncarnation>>,
    pub(super) next_ack_id: AtomicU64,
    pub(super) pending_acks: DashMap<u64, AckSet, RandomState>,
    pub(super) pending_relay_admissions:
        DashMap<u64, watch::Sender<RelayAdmissionUpdate>, RandomState>,
}

pub(super) struct RemoteDispatcher {
    pub(super) cluster: Arc<cluster::ClusterHandle>,
    pub(super) interconnect: Transport,
    /// The same admission the attaching runtime holds, so a body this dispatcher encodes is
    /// charged against the same budgets the runtime's own work is.
    pub(super) executor: Executor,
    /// The same registry the attaching runtime holds, so an acknowledgement this dispatcher sent
    /// a correlation id for resolves against the entry the runtime is waiting on.
    pub(super) registry: Arc<RemoteDispatchRegistry>,
}

impl std::fmt::Debug for RemoteDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDispatcher").finish_non_exhaustive()
    }
}

impl RemoteDispatcher {
    pub(super) const DISPATCH_RETRY_INTERVAL: Duration = Duration::from_millis(25);
    pub(super) const DISPATCH_TIMEOUT: Duration = Duration::from_secs(5);

    /// The node's bounded execution and memory admission, which every body this dispatcher
    /// encodes or decodes is submitted through.
    pub(super) fn executor(&self) -> &Executor {
        &self.executor
    }

    pub(super) fn local_node_id(&self) -> Option<ClusterNodeName> {
        self.registry.local_node_id.read().clone()
    }

    pub(super) fn next_ack_id(&self) -> u64 {
        self.registry.next_ack_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn register_pending_ack(&self, ack_id: u64, acks: AckSet) {
        self.registry.pending_acks.insert(ack_id, acks);
    }

    pub(super) fn forwarded_ack(acks: &AckSet) -> AckSet {
        acks.attached()
    }

    pub(super) fn clear_pending_ack(&self, ack_id: u64) {
        self.registry.pending_acks.remove(&ack_id);
    }

    pub(super) fn register_pending_relay_admission(
        &self,
        admission_id: u64,
    ) -> watch::Receiver<RelayAdmissionUpdate> {
        let (sender, receiver) = watch::channel(RelayAdmissionUpdate::Pending);
        self.registry
            .pending_relay_admissions
            .insert(admission_id, sender);
        receiver
    }

    pub(super) fn clear_pending_relay_admission(&self, admission_id: u64) {
        self.registry.pending_relay_admissions.remove(&admission_id);
    }

    pub(super) async fn request_with_timeout<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
        timeout: Duration,
    ) -> Result<M::Response, String>
    where
        M: InterconnectRequest,
    {
        self.interconnect
            .request_with_timeout(node_id, message, timeout)
            .await
            .map_err(|error| error.to_string())
    }

    /// Open a bounded, flow-controlled stream of one bulk response's bytes.
    pub(super) async fn request_stream<M>(
        &self,
        node_id: &ClusterNodeName,
        message: M,
    ) -> Result<nervix_interconnect::IncomingByteStream, String>
    where
        M: nervix_interconnect::InterconnectStreamRequest,
    {
        self.interconnect
            .request_stream(node_id, message)
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn dispatch_admitted_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        mut payload: RelayPayload,
        branch_channel: &RelayOutboundSlot,
    ) -> Result<(), String> {
        if branch_channel.cancellation().is_cancelled() {
            return Err(REMOTE_RELAY_BRANCH_EVICTED.to_string());
        }
        let local_node_id = self
            .local_node_id()
            .ok_or_else(|| "local node id is unavailable for relay delivery".to_string())?;
        let admission_id = self.next_ack_id();
        let delivery = payload.delivery;
        let mut cancellation_guard = self
            .interconnect
            .relay_cancellation_guard(node_id.clone(), delivery);
        payload.admission = Some(RemoteAckRegistration {
            ack_id: admission_id,
            reply_node_id: local_node_id,
        });
        let admission = self.register_pending_relay_admission(admission_id);
        let dispatch = self.dispatch(node_id, Envelope::RelayPayload(payload));
        tokio::pin!(dispatch);
        let dispatch_result = tokio::select! {
            biased;
            result = &mut dispatch => result,
            () = branch_channel.cancellation().cancelled() => {
                Err(REMOTE_RELAY_BRANCH_EVICTED.to_string())
            },
        };
        if let Err(error) = dispatch_result {
            self.clear_pending_relay_admission(admission_id);
            let resolution = self
                .resolve_failed_relay_admission(node_id, delivery, error, &mut cancellation_guard)
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(()),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    branch_channel.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        let admission = Self::await_relay_admission(node_id, admission, Self::DISPATCH_TIMEOUT);
        tokio::pin!(admission);
        let result = tokio::select! {
            biased;
            result = &mut admission => result,
            () = branch_channel.cancellation().cancelled() => {
                Err(REMOTE_RELAY_BRANCH_EVICTED.to_string())
            },
        };
        if let Err(error) = result {
            self.clear_pending_relay_admission(admission_id);
            let resolution = self
                .resolve_failed_relay_admission(node_id, delivery, error, &mut cancellation_guard)
                .await;
            return match resolution {
                FailedRelayAdmissionResolution::Admitted => Ok(()),
                FailedRelayAdmissionResolution::Failed(error) => Err(error),
                FailedRelayAdmissionResolution::Indeterminate(error) => {
                    branch_channel.reopen_delivery_channel();
                    Err(error)
                }
            };
        }
        cancellation_guard.disarm();
        Ok(())
    }

    async fn resolve_failed_relay_admission(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
        original_error: String,
        cancellation_guard: &mut RelayCancellationGuard,
    ) -> FailedRelayAdmissionResolution {
        let deadline = Instant::now()
            .checked_add(Self::DISPATCH_TIMEOUT)
            .assured("the fixed relay cancellation deadline fits the monotonic clock");
        let status = loop {
            tokio::task::consume_budget().await;
            let cancellation = tokio::time::timeout_at(
                deadline,
                self.interconnect.cancel_relay(node_id, delivery),
            )
            .await;
            match cancellation {
                Ok(Ok(
                    status @ (RelayAdmissionStatus::Admitted
                    | RelayAdmissionStatus::Rejected(_)
                    | RelayAdmissionStatus::Cancelled
                    | RelayAdmissionStatus::Retired
                    | RelayAdmissionStatus::Indeterminate),
                )) => break status,
                Ok(Ok(
                    RelayAdmissionStatus::Reserved
                    | RelayAdmissionStatus::BodyReceived
                    | RelayAdmissionStatus::Unknown,
                )) => {}
                Ok(Err(error)) => {
                    return FailedRelayAdmissionResolution::Indeterminate(format!(
                        "{original_error}; relay admission outcome is indeterminate because \
                         cancellation failed: {error}"
                    ));
                }
                Err(_) => {
                    return FailedRelayAdmissionResolution::Indeterminate(format!(
                        "{original_error}; relay admission outcome is indeterminate because \
                         cancellation did not resolve within {:?}",
                        Self::DISPATCH_TIMEOUT
                    ));
                }
            }
            if Instant::now() >= deadline {
                return FailedRelayAdmissionResolution::Indeterminate(format!(
                    "{original_error}; relay admission outcome remained unresolved for {:?}",
                    Self::DISPATCH_TIMEOUT
                ));
            }
            sleep(Self::DISPATCH_RETRY_INTERVAL).await;
        };
        match status {
            RelayAdmissionStatus::Admitted => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Admitted
            }
            RelayAdmissionStatus::Rejected(reason) => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Failed(reason)
            }
            RelayAdmissionStatus::Cancelled => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Failed(original_error)
            }
            RelayAdmissionStatus::Retired | RelayAdmissionStatus::Indeterminate => {
                cancellation_guard.disarm();
                FailedRelayAdmissionResolution::Indeterminate(format!(
                    "{original_error}; the receiver no longer retains the exact relay admission \
                     outcome"
                ))
            }
            RelayAdmissionStatus::Reserved
            | RelayAdmissionStatus::BodyReceived
            | RelayAdmissionStatus::Unknown => {
                FailedRelayAdmissionResolution::Indeterminate(format!(
                    "{original_error}; relay cancellation returned a non-terminal admission state"
                ))
            }
        }
    }

    pub(super) async fn await_relay_admission(
        node_id: &ClusterNodeName,
        mut admission: watch::Receiver<RelayAdmissionUpdate>,
        inactivity_timeout: Duration,
    ) -> Result<(), String> {
        let total_deadline = Instant::now()
            .checked_add(REMOTE_RELAY_TOTAL_TIMEOUT)
            .assured("the fixed relay total timeout fits the monotonic clock");
        loop {
            tokio::task::consume_budget().await;
            let inactivity_deadline = Instant::now()
                .checked_add(inactivity_timeout)
                .assured("a bounded relay inactivity timeout fits the monotonic clock");
            let deadline = inactivity_deadline.min(total_deadline);
            match tokio::time::timeout_at(deadline, admission.changed()).await {
                Ok(Ok(())) => {
                    let update = admission.borrow_and_update().clone();
                    match update {
                        RelayAdmissionUpdate::Pending | RelayAdmissionUpdate::Alive => {}
                        RelayAdmissionUpdate::Admitted => return Ok(()),
                        RelayAdmissionUpdate::Rejected(error) => return Err(error),
                    }
                }
                Ok(Err(_)) => {
                    return Err("relay admission response channel closed".to_string());
                }
                Err(_) => {
                    if Instant::now() >= total_deadline {
                        return Err(format!(
                            "timed out after {REMOTE_RELAY_TOTAL_TIMEOUT:?} waiting for cluster \
                             node '{node_id}' to admit a relay batch"
                        ));
                    }
                    return Err(format!(
                        "timed out waiting for cluster node '{node_id}' to admit a relay batch"
                    ));
                }
            }
        }
    }

    pub(super) async fn dispatch_subscription_fanout(
        &self,
        services: &RelayBoundaryServices,
        domain: &DomainName,
        relay: &RelayName,
        batch: &RelayRecordBatch,
        excluded_nodes: &BTreeSet<ClusterNodeName>,
    ) {
        let Some(local_node_id) = self.local_node_id() else {
            return;
        };
        let interested_nodes = self
            .cluster
            .nodes_with_subscription_interest(domain.as_str(), relay.as_str())
            .await;
        // One encode for the whole fanout. Every interested node carries this same allocation,
        // and the first one serializes it inside its own outbound slot: the slot is what orders
        // the batches a node receives, so an encode performed ahead of it lets a later batch
        // overtake an earlier one.
        let mut encoded_body: Option<ChargedBytes> = None;
        for node_id in interested_nodes {
            tokio::task::consume_budget().await;
            if node_id == local_node_id || excluded_nodes.contains(&node_id) {
                continue;
            }
            let outbound_slot = services.outbound_slot(
                &node_id,
                relay,
                RelayPayloadKind::SubscriptionFanout,
                &batch.key,
            );
            let _slot = outbound_slot.gate.lock().await;
            let batch_ipc = match encoded_body.clone() {
                Some(bytes) => bytes,
                None => match batch.batch.encode_arrow_ipc(self.executor()).await {
                    Ok(bytes) => {
                        encoded_body = Some(bytes.clone());
                        bytes
                    }
                    Err(error) => {
                        warn!(
                            domain = domain.as_str(),
                            relay = relay.as_str(),
                            error = %error,
                            "failed to serialize remote subscription batch"
                        );
                        return;
                    }
                },
            };
            let delivery = outbound_slot.next_delivery();
            if let Err(error) = self
                .dispatch_admitted_relay_payload(
                    &node_id,
                    RelayPayload {
                        delivery,
                        kind: RelayPayloadKind::SubscriptionFanout,
                        domain: domain.clone(),
                        relay: relay.clone(),
                        key: BranchKey::to_remote_key(&batch.key),
                        batch_ipc: batch_ipc.clone(),
                        metadata: batch
                            .metadata
                            .iter()
                            .map(RuntimeRecordMetadata::to_remote)
                            .collect(),
                        acks: vec![None; batch.acks.len()],
                        admission: None,
                    },
                    &outbound_slot,
                )
                .await
            {
                warn!(
                    target_node = %node_id,
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to dispatch remote subscription payload"
                );
            }
        }
    }

    pub(super) async fn dispatch(
        &self,
        node_id: &ClusterNodeName,
        envelope: Envelope,
    ) -> Result<(), String> {
        let deadline = Instant::now()
            .checked_add(Self::DISPATCH_TIMEOUT)
            .assured("the fixed remote dispatch timeout fits the monotonic clock");
        loop {
            tokio::task::consume_budget().await;
            let result = tokio::time::timeout_at(
                deadline,
                self.interconnect.send(node_id, envelope.clone()),
            )
            .await;
            let error = match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => format!("failed to send remote relay payload: {error}"),
                Err(_) => {
                    return Err(format!(
                        "timed out dispatching remote relay payload to '{node_id}'"
                    ));
                }
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            tokio::select! {
                _ = sleep_until(deadline) => return Err(error),
                _ = sleep(Self::DISPATCH_RETRY_INTERVAL) => {}
            }
        }
    }
}

pub(super) fn push_remote_runtime_consumer(
    consumers: &mut Vec<RemoteRuntimeConsumer>,
    node_id: &ClusterNodeName,
    relay: &RelayName,
    mode: AckMode,
) {
    if let Some(existing) = consumers
        .iter_mut()
        .find(|consumer| consumer.node_id == node_id.clone() && consumer.relay == *relay)
    {
        if mode == AckMode::Attached {
            existing.mode = AckMode::Attached;
        }
        return;
    }

    consumers.push(RemoteRuntimeConsumer {
        node_id: node_id.clone(),
        relay: relay.clone(),
        mode,
    });
}

/// The instantiated relay a remote payload is delivered into: the registry that accepts it, the
/// boundary services that own it, and the schema its Arrow batch must decode against.
pub(in crate::runtime) struct RemoteRelayTarget {
    pub(super) registry: RelayRegistry,
    pub(super) services: Arc<RelayBoundaryServices>,
    pub(super) schema: Arc<CompiledSchema>,
}

impl Runtime {
    pub fn attach_remote_dispatcher(
        &self,
        local_node_id: ClusterNodeName,
        cluster: Arc<cluster::ClusterHandle>,
        interconnect: Transport,
    ) {
        *self.inner.remote_dispatch.local_node_id.write() = Some(local_node_id);
        *self.inner.remote_dispatch.local_node_incarnation.write() =
            Some(cluster.local_incarnation());
        *self.inner.remote_dispatcher.write() = Some(Arc::new(RemoteDispatcher {
            cluster,
            interconnect,
            executor: self.inner.executor.clone(),
            registry: self.inner.remote_dispatch.clone(),
        }));
    }

    pub(in crate::runtime) async fn inject_remote_stream_boundary_message(
        &self,
        services: &RelayBoundaryServices,
        batch: &RelayRecordBatch,
    ) -> RelayDispatchResult {
        services.inject_remote_message(batch).await
    }

    pub async fn handle_remote_stream(
        &self,
        payload: RelayPayload,
        transport_admission: RelayAdmission,
    ) -> Result<(), Report<RuntimeError>> {
        let admission =
            payload
                .admission
                .clone()
                .ok_or_else(|| RuntimeError::DecodeRemoteRelay {
                    domain: payload.domain.as_str().to_string(),
                    relay: payload.relay.as_str().to_string(),
                    reason: "relay payload is missing its admission registration".to_string(),
                })?;
        let branch = match BranchKey::from_remote_key(payload.key.clone()) {
            Ok(Some(branch)) => Some(branch.as_str().to_string()),
            Ok(None) | Err(_) => None,
        };
        self.inner
            .fault_injection
            .pause_remote_relay_admission_if_armed(&payload.domain, branch.as_deref())
            .await;
        let mut admitted = false;
        let result = match payload.kind {
            RelayPayloadKind::Routed => {
                self.handle_remote_stream_payload_with_admission(
                    payload,
                    false,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
            RelayPayloadKind::SubscriptionFanout => {
                self.handle_remote_subscription_payload_with_admission(
                    payload,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
            RelayPayloadKind::Ingress => {
                self.handle_remote_stream_payload_with_admission(
                    payload,
                    true,
                    Some(RemoteRelayAdmissionContext {
                        registration: &admission,
                        transport: &transport_admission,
                        admitted: &mut admitted,
                    }),
                )
                .await
            }
        };
        if !admitted && let Err(error) = &result {
            self.send_remote_relay_admission_outcome(
                &admission,
                RemoteAckOutcome::NoAck(error.to_string()),
            )
            .await;
        }
        result?;
        Ok(())
    }

    pub(super) async fn send_remote_relay_admission_outcome(
        &self,
        admission: &RemoteAckRegistration,
        outcome: RemoteAckOutcome,
    ) {
        let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
            return;
        };
        if let Err(error) = dispatcher
            .dispatch(
                &admission.reply_node_id,
                Envelope::Ack(RemoteAckResolution {
                    ack_id: admission.ack_id,
                    outcome,
                }),
            )
            .await
        {
            warn!(
                target_node = %admission.reply_node_id,
                admission_id = admission.ack_id,
                error = %error,
                "failed to return relay admission response"
            );
        }
    }

    pub(in crate::runtime) fn remote_stream_target(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<RemoteRelayTarget, RuntimeError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if execution.passive_only {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let Some(registry) = execution.relay_registries.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        let Some(services) = execution.relay_services.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        let Some(schema) = execution.relay_schemas.get(relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        Ok(RemoteRelayTarget {
            registry,
            services,
            schema,
        })
    }

    pub(in crate::runtime) async fn wait_for_remote_stream_target(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<RemoteRelayTarget, RuntimeError> {
        let deadline = Instant::now() + REMOTE_RELAY_INSTANTIATION_WAIT;
        loop {
            tokio::task::consume_budget().await;
            match self.remote_stream_target(domain, relay) {
                Ok(target) => return Ok(target),
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                }
            }
            sleep(REMOTE_RELAY_INSTANTIATION_POLL).await;
        }
    }

    #[cfg(test)]
    pub(super) async fn handle_remote_stream_payload_with_owner_ingress(
        &self,
        remote: RelayPayload,
        owner_ingress: bool,
    ) -> Result<(), RuntimeError> {
        self.handle_remote_stream_payload_with_admission(remote, owner_ingress, None)
            .await
    }

    async fn handle_remote_stream_payload_with_admission(
        &self,
        remote: RelayPayload,
        owner_ingress: bool,
        admission: Option<RemoteRelayAdmissionContext<'_>>,
    ) -> Result<(), RuntimeError> {
        let RemoteRelayTarget {
            registry,
            services,
            schema,
        } = self
            .wait_for_remote_stream_target(&remote.domain, &remote.relay)
            .await?;
        if owner_ingress
            && !services.is_owned_by(self.inner.remote_dispatch.local_node_id.read().as_ref())
        {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        }
        let decoded_batch = schema
            .decode_arrow_body(self.executor(), remote.batch_ipc.clone())
            .await
            .map_err(|error| RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: error.to_string(),
            })?;
        if remote.metadata.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote metadata count {} does not match batch row count {}",
                    remote.metadata.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote ack count {} does not match batch row count {}",
                    remote.acks.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        let branch_key = BranchKey::from_remote_key(remote.key).map_err(|reason| {
            RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            }
        })?;
        let acks = remote
            .acks
            .into_iter()
            .map(|ack| {
                if let Some(ack) = ack {
                    let (acks, completion) = self.tracked_ack_root(&remote.domain);
                    self.spawn_remote_ack_watcher(remote.domain.clone(), completion, Some(ack));
                    acks
                } else {
                    AckSet::empty()
                }
            })
            .collect::<Vec<_>>();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            branch_key,
            decoded_batch,
            remote
                .metadata
                .into_iter()
                .map(RuntimeRecordMetadata::from_remote)
                .collect(),
            acks,
        )
        .map_err(|reason| RuntimeError::DecodeRemoteRelay {
            domain: remote.domain.as_str().to_string(),
            relay: remote.relay.as_str().to_string(),
            reason,
        })?;
        let dispatch = async {
            let dispatch = if owner_ingress {
                self.ingest_stream_boundary_message(
                    &remote.domain,
                    &remote.relay,
                    &registry,
                    &services,
                    &batch,
                )
                .await
            } else {
                self.inject_remote_stream_boundary_message(&services, &batch)
                    .await
            };
            if dispatch.is_ok() {
                for ack in batch.acks.iter() {
                    ack.ack_success();
                }
                return Ok(());
            }
            for ack in batch.acks.iter() {
                ack.no_ack("failed to dispatch remote relay message through local runtime");
            }
            Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: "local relay boundary rejected the batch".to_string(),
            })
        };
        if let Some(admission) = admission {
            if admission.transport.admit() == RelayAdmissionDecision::Cancelled {
                return Ok(());
            }
            *admission.admitted = true;
            self.send_remote_relay_admission_outcome(admission.registration, RemoteAckOutcome::Ack)
                .await;
            return dispatch.await;
        }
        dispatch.await
    }

    async fn handle_remote_subscription_payload_with_admission(
        &self,
        remote: RelayPayload,
        admission: Option<RemoteRelayAdmissionContext<'_>>,
    ) -> Result<(), RuntimeError> {
        let Some(execution) = self.inner.executions.get(&remote.domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let Some(services) = execution.relay_services.get(&remote.relay) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let Some(schema) = execution.relay_schemas.get(&remote.relay).cloned() else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
            });
        };
        let decoded_batch = schema
            .decode_arrow_body(self.executor(), remote.batch_ipc.clone())
            .await
            .map_err(|error| RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: error.to_string(),
            })?;
        if remote.metadata.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote metadata count {} does not match batch row count {}",
                    remote.metadata.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.len() != decoded_batch.batch().num_rows() {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: format!(
                    "remote ack count {} does not match batch row count {}",
                    remote.acks.len(),
                    decoded_batch.batch().num_rows()
                ),
            });
        }
        if remote.acks.iter().any(Option::is_some) {
            return Err(RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason: "subscription fanout payload must not carry remote ack registrations"
                    .to_string(),
            });
        }
        let branch_key = BranchKey::from_remote_key(remote.key).map_err(|reason| {
            RuntimeError::DecodeRemoteRelay {
                domain: remote.domain.as_str().to_string(),
                relay: remote.relay.as_str().to_string(),
                reason,
            }
        })?;
        let ack_count = remote.acks.len();
        let batch = RelayRecordBatch::from_runtime_batch(
            schema,
            branch_key,
            decoded_batch,
            remote
                .metadata
                .into_iter()
                .map(RuntimeRecordMetadata::from_remote)
                .collect(),
            vec![AckSet::empty(); ack_count],
        )
        .map_err(|reason| RuntimeError::DecodeRemoteRelay {
            domain: remote.domain.as_str().to_string(),
            relay: remote.relay.as_str().to_string(),
            reason,
        })?;
        let dispatch = services.fanout_local_subscriptions(&batch);
        if let Some(admission) = admission {
            if admission.transport.admit() == RelayAdmissionDecision::Cancelled {
                return Ok(());
            }
            *admission.admitted = true;
            self.send_remote_relay_admission_outcome(admission.registration, RemoteAckOutcome::Ack)
                .await;
            dispatch.await;
            return Ok(());
        }
        dispatch.await;
        Ok(())
    }

    pub(crate) fn handle_remote_ack_resolution(&self, ack: RemoteAckResolution) {
        if let RemoteAckOutcome::Alive = &ack.outcome {
            if let Some(admission) = self
                .inner
                .remote_dispatch
                .pending_relay_admissions
                .get(&ack.ack_id)
            {
                if admission.is_closed() {
                    drop(admission);
                    self.inner
                        .remote_dispatch
                        .pending_relay_admissions
                        .remove(&ack.ack_id);
                } else {
                    admission.send_if_modified(|update| {
                        if let RelayAdmissionUpdate::Admitted | RelayAdmissionUpdate::Rejected(_) =
                            update
                        {
                            return false;
                        }
                        *update = RelayAdmissionUpdate::Alive;
                        true
                    });
                }
                return;
            }
            let Some(pending) = self.inner.remote_dispatch.pending_acks.get(&ack.ack_id) else {
                warn!(
                    ack_id = ack.ack_id,
                    "received remote ack alive for unknown ack id"
                );
                return;
            };
            trace!(ack_id = ack.ack_id, "received remote ack alive");
            pending.ack_alive();
            return;
        }

        if let Some((_, admission)) = self
            .inner
            .remote_dispatch
            .pending_relay_admissions
            .remove(&ack.ack_id)
        {
            let terminal_update = match ack.outcome {
                RemoteAckOutcome::Ack => RelayAdmissionUpdate::Admitted,
                RemoteAckOutcome::NoAck(error) => RelayAdmissionUpdate::Rejected(error),
                RemoteAckOutcome::Alive => return,
            };
            admission.send_if_modified(|update| {
                if let RelayAdmissionUpdate::Admitted | RelayAdmissionUpdate::Rejected(_) = update {
                    return false;
                }
                *update = terminal_update;
                true
            });
            return;
        }

        let Some((_, pending)) = self.inner.remote_dispatch.pending_acks.remove(&ack.ack_id) else {
            warn!(
                ack_id = ack.ack_id,
                "received remote ack resolution for unknown ack id"
            );
            return;
        };
        trace!(ack_id = ack.ack_id, outcome = ?ack.outcome, "resolving remote ack");
        match ack.outcome {
            RemoteAckOutcome::Ack => pending.ack_success(),
            RemoteAckOutcome::NoAck(error) => pending.no_ack(error),
            RemoteAckOutcome::Alive => unreachable!("alive ack outcome is handled before removal"),
        }
    }

    pub(in crate::runtime) fn spawn_remote_ack_watcher(
        &self,
        domain: DomainName,
        completion: AckCompletion,
        ack: Option<RemoteAckRegistration>,
    ) {
        let Some(ack) = ack else {
            return;
        };
        let Some(dispatcher) = self.inner.remote_dispatcher.read().clone() else {
            return;
        };
        self.spawn_remote_ack_watcher_task(async move {
            let mut completion = completion;
            loop {
                tokio::select! {
                    _ = sleep(REMOTE_ACK_ALIVE_INTERVAL) => {
                        trace!(
                            domain = domain.as_str(),
                            ack_id = ack.ack_id,
                            target_node = %ack.reply_node_id,
                            "sending remote ack alive"
                        );
                        if let Err(error) = dispatcher
                            .dispatch(
                                &ack.reply_node_id,
                                Envelope::Ack(RemoteAckResolution {
                                    ack_id: ack.ack_id,
                                    outcome: RemoteAckOutcome::Alive,
                                }),
                            )
                            .await
                        {
                            warn!(
                                domain = domain.as_str(),
                                ack_id = ack.ack_id,
                                target_node = %ack.reply_node_id,
                                error = %error,
                                "failed to return remote ack alive"
                            );
                        }
                    }
                    progress = completion.wait_for_progress() => {
                        match progress {
                            AckProgress::Alive => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = %ack.reply_node_id,
                                    "forwarding remote ack alive"
                                );
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        &ack.reply_node_id,
                                        Envelope::Ack(RemoteAckResolution {
                                            ack_id: ack.ack_id,
                                            outcome: RemoteAckOutcome::Alive,
                                        }),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = %ack.reply_node_id,
                                        error = %error,
                                        "failed to forward remote ack alive"
                                    );
                                }
                            }
                            AckProgress::Complete(outcome) => {
                                trace!(
                                    domain = domain.as_str(),
                                    ack_id = ack.ack_id,
                                    target_node = %ack.reply_node_id,
                                    outcome = ?outcome,
                                    "sending remote ack resolution"
                                );
                                if let Err(error) = dispatcher
                                    .dispatch(
                                        &ack.reply_node_id,
                                        Envelope::Ack(RemoteAckResolution {
                                            ack_id: ack.ack_id,
                                            outcome: match outcome {
                                                AckOutcome::Ack => RemoteAckOutcome::Ack,
                                                AckOutcome::NoAck(error) => RemoteAckOutcome::NoAck(error),
                                            },
                                        }),
                                    )
                                    .await
                                {
                                    warn!(
                                        domain = domain.as_str(),
                                        ack_id = ack.ack_id,
                                        target_node = %ack.reply_node_id,
                                        error = %error,
                                        "failed to return remote ack resolution"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn spawn_remote_ack_watcher_task(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let shutdown = self.inner.remote_ack_watcher_shutdown.clone();
        if shutdown.is_cancelled() {
            return;
        }
        self.inner.remote_ack_watcher_tasks.spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {}
                _ = task => {}
            }
        });
    }

    pub(in crate::runtime) fn remote_runtime_consumers_for_schedule(
        schedule: &DomainSchedule,
        local_node_id: &ClusterNodeName,
    ) -> HashMap<RelayName, Vec<RemoteRuntimeConsumer>> {
        let mut consumers = HashMap::<RelayName, Vec<RemoteRuntimeConsumer>>::new();
        let owned_relays = schedule
            .nodes
            .values()
            .filter(|node| node.execution_node() == Some(local_node_id))
            .filter(|node| node.kind() == ModelKind::Relay)
            .map(|node| RelayName::from(&node.identifier))
            .collect::<HashSet<_>>();
        let processor_specs = branched_node_specs_from_scheduled_nodes(&schedule.nodes);
        for spec in processor_specs.processors {
            let Some(node) = schedule
                .nodes
                .get(&NodeRef::new(spec.spec.kind, spec.spec.processor.clone()))
            else {
                continue;
            };
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            for relay in &spec.spec.input_relays {
                if !owned_relays.contains(relay) || node.executes_on(local_node_id) {
                    continue;
                }
                push_remote_runtime_consumer(
                    consumers.entry(relay.clone()).or_default(),
                    target_node,
                    relay,
                    spec.spec.mode,
                );
            }
        }
        for node in schedule.nodes.values() {
            let Some(target_node) = node.execution_node() else {
                continue;
            };
            match node.config.as_ref() {
                Model::Emitter(emitter) => {
                    for relay in emitter.from.relays() {
                        if !owned_relays.contains(relay) || node.executes_on(local_node_id) {
                            continue;
                        }
                        push_remote_runtime_consumer(
                            consumers.entry(relay.clone()).or_default(),
                            target_node,
                            relay,
                            emitter.mode,
                        );
                    }
                }
                Model::Reingestor(reingestor) => {
                    for relay in reingestor.from.relays() {
                        if !owned_relays.contains(relay) || node.executes_on(local_node_id) {
                            continue;
                        }
                        push_remote_runtime_consumer(
                            consumers.entry(relay.clone()).or_default(),
                            target_node,
                            relay,
                            reingestor.mode,
                        );
                    }
                }
                _ => {}
            }
        }
        consumers
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{AckMode, ClusterNodeName, RemoteAckOutcome, RemoteAckResolution};
    use tokio::{
        sync::{oneshot, watch},
        time::{Duration, Instant, sleep, timeout},
    };

    use super::*;
    use crate::runtime_ack::{AckOutcome, AckSet};

    struct DropNotice(Option<oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                sender
                    .send(())
                    .means_peer_left("remote ACK watcher cancellation test");
            }
        }
    }

    #[tokio::test]
    async fn runtime_shutdown_cancels_remote_ack_watcher_tasks() {
        let runtime = Runtime::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        runtime.spawn_remote_ack_watcher_task(async move {
            let _notice = DropNotice(Some(dropped_tx));
            started_tx
                .send(())
                .means_peer_left("remote ACK watcher cancellation test starter");
            std::future::pending::<()>().await;
        });
        started_rx
            .await
            .assured("the tracked watcher sends before entering its pending state");

        runtime.shutdown().await;

        let dropped = timeout(Duration::from_secs(1), dropped_rx).await;
        assert!(
            dropped.is_ok(),
            "runtime shutdown should cancel a pending remote ACK watcher"
        );
        dropped
            .verified("the watcher cancellation deadline was checked by the assertion above")
            .assured("dropping the watcher always sends its drop notice");
        assert!(runtime.inner.remote_ack_watcher_tasks.is_empty());
    }

    #[tokio::test]
    async fn ack_alive_resets_ingestor_ack_timeout() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (acks, completion) = AckSet::root();
        let ack_task = acks.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            ack_task.ack_alive();
            sleep(Duration::from_millis(150)).await;
            ack_task.ack_success();
        });

        assert_eq!(
            Runtime::await_ack_completion(
                &mut shutdown_rx,
                completion,
                Duration::from_millis(200),
            )
            .await,
            Some(AckOutcome::Ack)
        );
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn remote_ack_alive_packet_resets_ingestor_ack_timeout() {
        let runtime = Runtime::default();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (acks, completion) = AckSet::root();
        runtime.inner.remote_dispatch.pending_acks.insert(7, acks);
        let runtime_task = runtime.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
                ack_id: 7,
                outcome: RemoteAckOutcome::Alive,
            });
            sleep(Duration::from_millis(150)).await;
            runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
                ack_id: 7,
                outcome: RemoteAckOutcome::Ack,
            });
        });

        assert_eq!(
            Runtime::await_ack_completion(
                &mut shutdown_rx,
                completion,
                Duration::from_millis(200),
            )
            .await,
            Some(AckOutcome::Ack)
        );
        assert!(
            runtime.inner.remote_dispatch.pending_acks.get(&7).is_none(),
            "terminal ack must clear the pending remote ack"
        );
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn remote_relay_admission_alive_resets_dispatch_timeout() {
        let runtime = Runtime::default();
        let (admission_tx, admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
        runtime
            .inner
            .remote_dispatch
            .pending_relay_admissions
            .insert(9, admission_tx);
        let runtime_task = runtime.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
                ack_id: 9,
                outcome: RemoteAckOutcome::Alive,
            });
            sleep(Duration::from_millis(150)).await;
            runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
                ack_id: 9,
                outcome: RemoteAckOutcome::Ack,
            });
        });

        assert_eq!(
            RemoteDispatcher::await_relay_admission(
                &ClusterNodeName::parse("relay-owner").expect("valid name"),
                admission_rx,
                Duration::from_millis(200),
            )
            .await,
            Ok(())
        );
        assert!(
            runtime
                .inner
                .remote_dispatch
                .pending_relay_admissions
                .get(&9)
                .is_none(),
            "terminal admission ack must clear pending admission state"
        );
    }

    #[tokio::test]
    async fn remote_relay_admission_progress_is_coalesced() {
        let runtime = Runtime::default();
        let (admission_tx, mut admission_rx) = watch::channel(RelayAdmissionUpdate::Pending);
        runtime
            .inner
            .remote_dispatch
            .pending_relay_admissions
            .insert(10, admission_tx);

        for _ in 0..100 {
            runtime.handle_remote_ack_resolution(RemoteAckResolution {
                ack_id: 10,
                outcome: RemoteAckOutcome::Alive,
            });
        }
        assert!(
            admission_rx
                .has_changed()
                .expect("sender should remain open")
        );
        assert!(matches!(
            &*admission_rx.borrow_and_update(),
            RelayAdmissionUpdate::Alive
        ));
        assert!(
            !admission_rx
                .has_changed()
                .expect("sender should remain open"),
            "replaceable progress must occupy one pending update"
        );

        runtime.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 10,
            outcome: RemoteAckOutcome::Ack,
        });
        admission_rx
            .changed()
            .await
            .expect("the terminal admission outcome should remain observable");
        assert!(matches!(
            &*admission_rx.borrow_and_update(),
            RelayAdmissionUpdate::Admitted
        ));
    }

    #[tokio::test]
    async fn forwarding_to_a_remote_relay_owner_extends_the_ack_chain() {
        let (acks, completion) = AckSet::root();
        let forwarded = RemoteDispatcher::forwarded_ack(&acks);
        let completion = completion.wait();
        tokio::pin!(completion);

        acks.ack_success();
        assert!(
            timeout(Duration::from_millis(50), &mut completion)
                .await
                .is_err(),
            "local producer completion must wait for the remote relay path"
        );
        forwarded.ack_success();
        assert_eq!(
            timeout(Duration::from_secs(1), &mut completion)
                .await
                .expect("remote relay completion should resolve the producer ACK"),
            AckOutcome::Ack
        );
    }

    #[tokio::test]
    async fn relay_gate_holds_nonowner_dispatch_before_the_remote_slot() {
        let services = test_relay_boundary_services();
        services.replace_owner_node(Some(ClusterNodeName::parse("node-2").expect("valid name")));
        let gate = services.fanout.dispatch_gate();
        let lease = RelayDispatchGateLease::engage(
            gate,
            Instant::now() + Duration::from_secs(1),
            "relay owner is moving",
        );
        let task_services = services.clone();
        let dispatch = tokio::spawn(async move {
            task_services
                .dispatch_to_owner(&domain("default"), &named("orders"), &quiesce_test_batch())
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !dispatch.is_finished(),
            "a gated nonowner must not enter its remote dispatch slot"
        );

        drop(lease);
        timeout(Duration::from_secs(1), dispatch)
            .await
            .expect("releasing the gate should wake the nonowner dispatch")
            .expect("dispatch task should join")
            .expect_err("the isolated test has no remote dispatcher");
    }

    #[tokio::test]
    async fn relay_gate_allows_admitted_owner_batches_to_reach_consumers() {
        let runtime = Runtime::default();
        let domain = domain("default");
        let relay = named("orders");
        let services = test_relay_boundary_services();
        let mut consumer = services.add_local_runtime_consumer(AckMode::Attached);
        let gate = services.fanout.dispatch_gate();
        let mut lease = RelayDispatchGateLease::engage(
            gate,
            Instant::now() + Duration::from_secs(1),
            "relay owner is moving",
        );
        assert!(lease.wait_quiescent().await);

        timeout(
            Duration::from_millis(100),
            services.fanout_owner_batch(
                &runtime.inner.metrics,
                &domain,
                &relay,
                None,
                &quiesce_test_batch(),
            ),
        )
        .await
        .expect("the gate must not pause a batch already admitted to the owner buffer")
        .expect("the owner should fan out the admitted batch");
        timeout(Duration::from_secs(1), consumer.recv())
            .await
            .expect("the attached consumer should receive the admitted batch")
            .expect("the attached consumer should remain open");

        services.remove_local_runtime_consumer(AckMode::Attached);
    }
}
