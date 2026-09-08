use super::*;

pub(super) const REMOTE_RELAY_INSTANTIATION_WAIT: Duration = Duration::from_secs(5);

pub(super) const REMOTE_RELAY_INSTANTIATION_POLL: Duration = Duration::from_millis(25);

pub(super) const REMOTE_ACK_ALIVE_INTERVAL: Duration = Duration::from_millis(100);

/// Node identity and the remote acknowledgement correlation registry. The runtime and the
/// `RemoteDispatcher` it attaches must observe one instance of this: the dispatcher allocates the
/// correlation ids that the runtime resolves when acknowledgements come back over the
/// interconnect, and both answer questions about which node they are running on.
pub(super) struct RemoteDispatchRegistry {
    pub(super) local_node_id: RwLock<Option<ClusterNodeName>>,
    pub(super) next_ack_id: AtomicU64,
    pub(super) pending_acks: DashMap<u64, AckSet, RandomState>,
    pub(super) pending_relay_admissions:
        DashMap<u64, mpsc::UnboundedSender<RemoteAckOutcome>, RandomState>,
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
    ) -> mpsc::UnboundedReceiver<RemoteAckOutcome> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.registry
            .pending_relay_admissions
            .insert(admission_id, sender);
        receiver
    }

    pub(super) fn clear_pending_relay_admission(&self, admission_id: u64) {
        self.registry.pending_relay_admissions.remove(&admission_id);
    }

    pub(super) async fn dispatch_admitted_relay_payload(
        &self,
        node_id: &ClusterNodeName,
        mut payload: RelayPayload,
    ) -> Result<(), String> {
        let local_node_id = self
            .local_node_id()
            .ok_or_else(|| "local node id is unavailable for relay delivery".to_string())?;
        let admission_id = self.next_ack_id();
        payload.admission = Some(RemoteAckRegistration {
            ack_id: admission_id,
            reply_node_id: local_node_id,
        });
        let admission = self.register_pending_relay_admission(admission_id);
        if let Err(error) = self
            .dispatch(node_id, Envelope::RelayPayload(payload))
            .await
        {
            self.clear_pending_relay_admission(admission_id);
            return Err(error);
        }
        let result = Self::await_relay_admission(node_id, admission, Self::DISPATCH_TIMEOUT).await;
        if result.is_err() {
            self.clear_pending_relay_admission(admission_id);
        }
        result
    }

    pub(super) async fn await_relay_admission(
        node_id: &ClusterNodeName,
        mut admission: mpsc::UnboundedReceiver<RemoteAckOutcome>,
        inactivity_timeout: Duration,
    ) -> Result<(), String> {
        loop {
            tokio::task::consume_budget().await;
            match tokio::time::timeout(inactivity_timeout, admission.recv()).await {
                Ok(Some(RemoteAckOutcome::Alive)) => {}
                Ok(Some(RemoteAckOutcome::Ack)) => return Ok(()),
                Ok(Some(RemoteAckOutcome::NoAck(error))) => return Err(error),
                Ok(None) => {
                    return Err("relay admission response channel closed".to_string());
                }
                Err(_) => {
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
        // One encode for the whole fanout. Every interested node carries this same allocation.
        let batch_ipc = match batch.batch.encode_arrow_ipc(self.executor()).await {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    relay = relay.as_str(),
                    error = %error,
                    "failed to serialize remote subscription batch"
                );
                return;
            }
        };
        let interested_nodes = self
            .cluster
            .nodes_with_subscription_interest(domain.as_str(), relay.as_str())
            .await;
        for node_id in interested_nodes {
            tokio::task::consume_budget().await;
            if node_id == local_node_id || excluded_nodes.contains(&node_id) {
                continue;
            }
            let outbound_slot = services.outbound_slot(&node_id);
            let _slot = outbound_slot.lock().await;
            if let Err(error) = self
                .dispatch_admitted_relay_payload(
                    &node_id,
                    RelayPayload {
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
        let deadline = Instant::now() + Self::DISPATCH_TIMEOUT;
        loop {
            tokio::task::consume_budget().await;
            let result = async {
                let node = self
                    .cluster
                    .gossip_state()
                    .await
                    .live_nodes
                    .into_iter()
                    .find(|node| node.node_id == node_id.clone())
                    .ok_or_else(|| {
                        format!("remote node '{node_id}' is not present in gossip membership")
                    })?;
                let target_addr = node.interconnect_advertise_addr.parse().map_err(|error| {
                    format!("invalid interconnect address for '{node_id}': {error}")
                })?;
                let mode = match node.interconnect_mode.as_str() {
                    "https" => InterconnectTransportMode::Tls,
                    _ => InterconnectTransportMode::Plain,
                };
                let connection = self
                    .interconnect
                    .connection_for(node_id, target_addr, "localhost", mode)
                    .map_err(|error| {
                        format!("failed to connect interconnect for '{node_id}': {error}")
                    })?;
                connection
                    .send(envelope.clone())
                    .await
                    .map_err(|error| format!("failed to send remote relay payload: {error}"))
            }
            .await;
            let error = match result {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if Instant::now() >= deadline {
                return Err(error);
            }
            sleep(Self::DISPATCH_RETRY_INTERVAL).await;
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

    pub async fn handle_remote_stream(&self, payload: RelayPayload) -> Result<(), RuntimeError> {
        let admission =
            payload
                .admission
                .clone()
                .ok_or_else(|| RuntimeError::DecodeRemoteRelay {
                    domain: payload.domain.as_str().to_string(),
                    relay: payload.relay.as_str().to_string(),
                    reason: "relay payload is missing its admission registration".to_string(),
                })?;
        let handling = async {
            match payload.kind {
                RelayPayloadKind::Routed => self.handle_remote_stream_payload(payload).await,
                RelayPayloadKind::SubscriptionFanout => {
                    self.handle_remote_subscription_payload(payload).await
                }
                RelayPayloadKind::Ingress => {
                    self.handle_remote_stream_payload_with_owner_ingress(payload, true)
                        .await
                }
            }
        };
        let heartbeats = async {
            loop {
                tokio::task::consume_budget().await;
                sleep(REMOTE_ACK_ALIVE_INTERVAL).await;
                self.send_remote_relay_admission_outcome(&admission, RemoteAckOutcome::Alive)
                    .await;
            }
        };
        tokio::pin!(handling);
        tokio::pin!(heartbeats);
        let result = tokio::select! {
            biased;
            result = &mut handling => result,
            _ = &mut heartbeats => unreachable!("relay admission heartbeat loop cannot complete"),
        };
        self.resolve_remote_relay_admission(&admission, &result)
            .await;
        result
    }

    pub(super) async fn resolve_remote_relay_admission(
        &self,
        admission: &RemoteAckRegistration,
        result: &Result<(), RuntimeError>,
    ) {
        let outcome = match result {
            Ok(()) => RemoteAckOutcome::Ack,
            Err(error) => RemoteAckOutcome::NoAck(error.to_string()),
        };
        self.send_remote_relay_admission_outcome(admission, outcome)
            .await;
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

    pub(in crate::runtime) async fn handle_remote_stream_payload(
        &self,
        remote: RelayPayload,
    ) -> Result<(), RuntimeError> {
        self.handle_remote_stream_payload_with_owner_ingress(remote, false)
            .await
    }

    pub(super) async fn handle_remote_stream_payload_with_owner_ingress(
        &self,
        remote: RelayPayload,
        owner_ingress: bool,
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
            ack.no_ack("failed to admit remote relay message into local runtime");
        }
        Err(RuntimeError::DecodeRemoteRelay {
            domain: remote.domain.as_str().to_string(),
            relay: remote.relay.as_str().to_string(),
            reason: "local relay boundary rejected the batch".to_string(),
        })
    }

    pub(in crate::runtime) async fn handle_remote_subscription_payload(
        &self,
        remote: RelayPayload,
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
        services.fanout_local_subscriptions(&batch).await;
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
                if admission.send(RemoteAckOutcome::Alive).is_err() {
                    drop(admission);
                    self.inner
                        .remote_dispatch
                        .pending_relay_admissions
                        .remove(&ack.ack_id);
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
            let _ = admission.send(ack.outcome);
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
        tokio::spawn(async move {
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

    pub(in crate::runtime) fn remote_runtime_consumers_for_schedule(
        schedule: &DomainSchedule,
        local_node_id: &ClusterNodeName,
    ) -> HashMap<RelayName, Vec<RemoteRuntimeConsumer>> {
        let mut consumers = HashMap::<RelayName, Vec<RemoteRuntimeConsumer>>::new();
        let owned_relays = schedule
            .nodes
            .values()
            .filter(|node| node.execution_node() == Some(local_node_id))
            .filter(|node| node.kind == ModelKind::Relay)
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
        sync::{mpsc, watch},
        time::{Duration, Instant, sleep, timeout},
    };

    use super::*;
    use crate::runtime_ack::{AckOutcome, AckSet};
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
        let (admission_tx, admission_rx) = mpsc::unbounded_channel();
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
