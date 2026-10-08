//! Reliable relay admission and transfer over authenticated peer connections.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Relay grants, body transfer, runtime-admission progress, cancellation fences,
//!   process-epoch reconciliation, channel sequence watermarks, and terminal outcome retirement.
//! - **Depends on.** The parent interconnect connection state, execution admission, and relay wire
//!   models.
//! - **Must not know.** Runtime graphs, schedules, branch processing state, or external connectors.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "relay grants, reconciliation, watermarks and payload attempts recur on \
                  data-plane frames"
    )
)]

use futures_util::StreamExt as _;

use super::*;

impl TransportState {
    pub(crate) async fn cancel_relay(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
    ) -> Result<RelayAdmissionStatus, Report<TransportError>> {
        self.relay_admission_operation(node_id, delivery, RELAY_CANCEL_PATH)
            .await
    }

    pub(crate) async fn relay_admission_status(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
    ) -> Result<RelayAdmissionStatus, Report<TransportError>> {
        self.relay_admission_operation(node_id, delivery, RELAY_STATUS_PATH)
            .await
    }

    pub(super) async fn cancel_relay_until_resolved(
        &self,
        node_id: ClusterNodeName,
        delivery: RelayDelivery,
    ) {
        let deadline = Instant::now()
            .checked_add(RELAY_CANCELLATION_RETRY_WINDOW)
            .assured("the fixed relay cancellation retry window fits the monotonic clock");
        let mut backoff = self.options.reconnect_backoff;
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.cancel_relay(&node_id, delivery).await {
                Ok(
                    RelayAdmissionStatus::Admitted
                    | RelayAdmissionStatus::Rejected(_)
                    | RelayAdmissionStatus::Cancelled
                    | RelayAdmissionStatus::Retired
                    | RelayAdmissionStatus::Indeterminate,
                ) => return,
                Ok(
                    RelayAdmissionStatus::Reserved
                    | RelayAdmissionStatus::BodyReceived
                    | RelayAdmissionStatus::Unknown,
                ) => {}
                Err(error) => {
                    debug!(
                        target_node = %node_id,
                        ?error,
                        "relay cancellation remains unresolved"
                    );
                }
            }
            let now = Instant::now();
            if now >= deadline {
                debug!(
                    target_node = %node_id,
                    ?delivery,
                    "relay cancellation retry window expired"
                );
                return;
            }
            let retry_at = now
                .checked_add(backoff)
                .assured("a configured relay retry delay fits the monotonic clock")
                .min(deadline);
            nervix_primitives::select! {
                _ = self.admission_closed.cancelled() => return,
                _ = sleep_until(retry_at) => {}
            }
            backoff = backoff
                .checked_mul(2)
                .unwrap_or(self.options.max_reconnect_backoff)
                .min(self.options.max_reconnect_backoff);
        }
    }

    async fn relay_admission_operation(
        &self,
        node_id: &ClusterNodeName,
        delivery: RelayDelivery,
        path: &str,
    ) -> Result<RelayAdmissionStatus, Report<TransportError>> {
        let timeout_duration = self.options.request_timeout;
        let deadline = Instant::now()
            .checked_add(timeout_duration)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "relay admission deadline exceeds the monotonic clock range".to_string(),
            })?;
        let lease = self
            .lease(
                node_id,
                PoolClass::Management,
                if path == RELAY_CANCEL_PATH {
                    RequestSubquota::Cancellation
                } else {
                    RequestSubquota::Liveness
                },
                deadline,
            )
            .await?;
        let outbound_key = OutboundRelayKey {
            peer_node_id: node_id.clone(),
            delivery,
        };
        let relay_owner = lease
            .connection
            .relay_owner
            .as_ref()
            .assured("a leased connection completed its authenticated binding");
        let receiver_epoch = relay_owner
            .outbound_epoch(&outbound_key)
            .unwrap_or(lease.connection.peer_epoch);
        if receiver_epoch != lease.connection.peer_epoch {
            relay_owner.retire_outbound(&outbound_key);
            return Ok(RelayAdmissionStatus::Indeterminate);
        }
        let request = wire::encode_rkyv(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            self.executor.limits().management_event_bytes.as_u64(),
            RelayAdmissionRequest {
                sender_epoch: self.process_epoch,
                receiver_epoch,
                delivery,
            },
        )
        .await?;
        let response = lease
            .request_raw(
                self,
                RawRequest {
                    path,
                    body: Some(request),
                    response_class: PoolClass::Management,
                    response_limit: self.executor.limits().management_event_bytes.as_u64(),
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    headers: &[],
                },
            )
            .await?;
        let response = wire::decode_rkyv::<RelayAdmissionResponse>(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            response,
        )
        .await?
        .into_value();
        if response.receiver_epoch != receiver_epoch {
            relay_owner.retire_outbound(&outbound_key);
            return Ok(RelayAdmissionStatus::Indeterminate);
        }
        if response.status.is_terminal()
            || matches!(
                &response.status,
                RelayAdmissionStatus::Unknown | RelayAdmissionStatus::Indeterminate
            )
        {
            relay_owner.retire_outbound(&outbound_key);
        }
        Ok(response.status)
    }

    pub(super) async fn send_relay(
        &self,
        node_id: &ClusterNodeName,
        payload: RelayPayload,
    ) -> Result<(), Report<TransportError>> {
        let timeout_duration = self.options.request_timeout;
        let deadline = Instant::now()
            .checked_add(timeout_duration)
            .ok_or_else(|| TransportError::InvalidOptions {
                reason: "request deadline exceeds the monotonic clock range".to_string(),
            })?;
        // The relay connection and local stream slot are leased before receiver memory is asked
        // for, so a saturated pool never holds a remote application grant.
        let relay = self
            .lease(node_id, PoolClass::Relay, RequestSubquota::Shared, deadline)
            .await?;
        let grant = RelayGrantRequest {
            sender_epoch: self.process_epoch,
            delivery: payload.delivery,
            body_bytes: payload
                .batch_ipc
                .len()
                .try_into()
                .assured("an in-memory allocation length fits in u64"),
            metadata: wire::RelayMetadata::from_payload(&payload),
        };
        let metadata = wire::encode_rkyv(
            &self.executor,
            MemoryClass::Relay,
            CpuClass::Data,
            self.executor.limits().relay_encoded_bytes.as_u64(),
            grant,
        )
        .await?;
        let management = self
            .lease(
                node_id,
                PoolClass::Management,
                RequestSubquota::Admission,
                deadline,
            )
            .await?;
        let outbound_key = OutboundRelayKey {
            peer_node_id: node_id.clone(),
            delivery: payload.delivery,
        };
        let admission = payload.admission.as_ref().ok_or_else(|| {
            TransportError::RelayGrant(
                "relay payload is missing its admission registration".to_string(),
            )
        })?;
        let admission_key = RelayAdmissionKey {
            peer_node_id: node_id.clone(),
            registration: admission.clone(),
        };
        let relay_owner = management
            .connection
            .relay_owner
            .as_ref()
            .assured("a leased connection completed its authenticated binding");
        relay_owner.register_outbound(
            &outbound_key,
            management.connection.peer_epoch,
            &admission_key,
        )?;
        let grant_response = management
            .request_raw(
                self,
                RawRequest {
                    path: RELAY_GRANT_PATH,
                    body: Some(metadata),
                    response_class: PoolClass::Management,
                    response_limit: self.executor.limits().management_event_bytes.as_u64(),
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    headers: &[],
                },
            )
            .await;
        let grant_response = match grant_response {
            Ok(response) => response,
            Err(error) => {
                // An explicit HTTP refusal created no grant. An interrupted exchange remains
                // correlated until status/cancellation determines whether it reached the peer.
                if matches!(
                    error.current_context(),
                    TransportError::RemoteRejected { .. }
                ) {
                    relay_owner.retire_outbound(&outbound_key);
                }
                return Err(error);
            }
        };
        let grant = wire::decode_rkyv::<RelayGrantResponse>(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            grant_response,
        )
        .await?
        .into_value();
        if grant.receiver_epoch != management.connection.peer_epoch {
            relay_owner.retire_outbound(&outbound_key);
            return Err(Report::new(TransportError::RelayIndeterminate));
        }
        let grant_id = match grant.disposition {
            RelayGrantDisposition::SendBody { grant_id } => grant_id,
            RelayGrantDisposition::BodyReceived => return Ok(()),
            RelayGrantDisposition::Admitted => {
                self.deliver_terminal_incoming(
                    management.connection.peer_addr,
                    node_id.clone(),
                    Envelope::Ack(admission.resolution(RemoteAckOutcome::Ack)),
                    None,
                )
                .await?;
                relay_owner.retire_outbound(&outbound_key);
                return Ok(());
            }
            RelayGrantDisposition::Rejected(reason) => {
                relay_owner.retire_outbound(&outbound_key);
                return Err(Report::new(TransportError::RelayRejected(reason)));
            }
            RelayGrantDisposition::Cancelled => {
                relay_owner.retire_outbound(&outbound_key);
                return Err(Report::new(TransportError::RelayCancelled));
            }
            RelayGrantDisposition::Retired => {
                relay_owner.retire_outbound(&outbound_key);
                return Err(Report::new(TransportError::RelayIndeterminate));
            }
        };
        let path = format!("{RELAY_PATH_PREFIX}{grant_id}");
        let sender_epoch = self.process_epoch.to_string();
        let receiver_epoch = grant.receiver_epoch.to_string();
        relay
            .request_raw(
                self,
                RawRequest {
                    path: &path,
                    body: Some(payload.batch_ipc),
                    response_class: PoolClass::Relay,
                    response_limit: RESPONSE_LIMIT,
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    headers: &[
                        ("x-nervix-sender-epoch", sender_epoch.as_str()),
                        ("x-nervix-receiver-epoch", receiver_epoch.as_str()),
                    ],
                },
            )
            .await?;
        Ok(())
    }

    pub(super) async fn report_relay_progress(self) {
        struct ProgressReport {
            target: ClusterNodeName,
            admission_id: u64,
            failure: Option<String>,
        }

        let mut interval = nervix_primitives::time::interval(RELAY_PROGRESS_INTERVAL);
        interval.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = self.admission_closed.cancelled() => break,
                _ = interval.tick() => {}
            }

            // Report in registration order, not map order, so every process sends the same reports
            // in the same sequence.
            let owners = self.relay_owners.load_full();
            let mut registrations = BTreeSet::new();
            for owner in owners.owners.values() {
                registrations.extend(owner.progress_registrations());
            }
            let mut reports = FuturesUnordered::new();
            for registration in registrations {
                nervix_primitives::task::consume_budget().await;
                let state = self.clone();
                reports.push(async move {
                    let target = registration.registrar.node_id().clone();
                    let ack_id = registration.ack_id;
                    let result = timeout(
                        RELAY_PROGRESS_SEND_TIMEOUT,
                        state.send(
                            &target,
                            Envelope::Ack(registration.resolution(RemoteAckOutcome::Alive)),
                        ),
                    )
                    .await;
                    let failure = match result {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error.to_string()),
                        Err(_) => Some(format!(
                            "relay progress send exceeded {RELAY_PROGRESS_SEND_TIMEOUT:?}"
                        )),
                    };
                    ProgressReport {
                        target,
                        admission_id: ack_id,
                        failure,
                    }
                });
            }
            while !reports.is_empty() {
                nervix_primitives::task::consume_budget().await;
                let completed = nervix_primitives::select! {
                    _ = self.admission_closed.cancelled() => return,
                    completed = reports.next() => completed,
                };
                let Some(report) = completed else {
                    break;
                };
                if let Some(error) = report.failure {
                    debug!(
                        target_node = %report.target,
                        admission_id = report.admission_id,
                        error,
                        "failed to report queued relay progress"
                    );
                }
            }
        }
    }

    pub(super) async fn retire_idle_relay_channels(self) {
        let mut interval = nervix_primitives::time::interval(RELAY_CHANNEL_SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = self.admission_closed.cancelled() => break,
                _ = interval.tick() => {}
            }
            let owners = self.relay_owners.load_full();
            for owner in owners.owners.values() {
                owner.sweep(Instant::now());
            }
            self.prune_ended_relay_owners();
        }
    }

    pub(super) async fn handle_relay_admission_control(
        &self,
        peer_node_id: ClusterNodeName,
        peer_epoch: u64,
        relay_owner: StdArc<RelayPeerOwner>,
        cancel: bool,
        body: RecvStream,
        respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let encoded = read_body(
            &self.executor,
            MemoryClass::Management,
            self.executor.limits().management_event_bytes.as_u64(),
            self.options.progress_timeout,
            body,
        )
        .await?;
        let request = wire::decode_rkyv::<RelayAdmissionRequest>(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            encoded,
        )
        .await?
        .into_value();
        if request.sender_epoch != peer_epoch || request.receiver_epoch != self.process_epoch {
            return self
                .send_relay_admission_response(respond, RelayAdmissionStatus::Indeterminate)
                .await;
        }
        let attempt = self.relay_attempt_key(peer_node_id, request.sender_epoch, request.delivery);
        let (status, record_to_retire) = relay_owner.control(&attempt, cancel);
        self.send_relay_admission_response(respond, status.clone())
            .await?;
        if let Some(record) = record_to_retire
            && status.is_terminal()
        {
            self.retire_relay_record(&record, status);
        }
        Ok(())
    }

    async fn send_relay_admission_response(
        &self,
        respond: server::SendResponse<Bytes>,
        status: RelayAdmissionStatus,
    ) -> Result<(), Report<TransportError>> {
        let response = wire::encode_rkyv(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            self.executor.limits().management_event_bytes.as_u64(),
            RelayAdmissionResponse {
                receiver_epoch: self.process_epoch,
                status,
            },
        )
        .await?;
        send_response(
            respond,
            StatusCode::OK,
            Some(response),
            self.options.progress_timeout,
        )
        .await?;
        Ok(())
    }

    async fn send_relay_grant_response(
        &self,
        respond: server::SendResponse<Bytes>,
        disposition: RelayGrantDisposition,
    ) -> Result<(), Report<TransportError>> {
        let response = wire::encode_rkyv(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            self.executor.limits().management_event_bytes.as_u64(),
            RelayGrantResponse {
                receiver_epoch: self.process_epoch,
                disposition,
            },
        )
        .await?;
        send_response(
            respond,
            StatusCode::OK,
            Some(response),
            self.options.progress_timeout,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn handle_relay_grant(
        &self,
        peer_node_id: ClusterNodeName,
        peer_epoch: u64,
        relay_owner: StdArc<RelayPeerOwner>,
        body: RecvStream,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let encoded = read_body(
            &self.executor,
            MemoryClass::Relay,
            self.executor.limits().relay_encoded_bytes.as_u64(),
            self.options.progress_timeout,
            body,
        )
        .await?;
        let (grant, metadata_memory) = wire::decode_rkyv::<RelayGrantRequest>(
            &self.executor,
            MemoryClass::Relay,
            CpuClass::Data,
            encoded,
        )
        .await?
        .into_parts();
        if grant.sender_epoch != peer_epoch
            || grant.body_bytes > self.executor.limits().relay_encoded_bytes.as_u64()
        {
            send_response(
                respond,
                StatusCode::BAD_REQUEST,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let Some(admission) = grant.metadata.admission.as_ref() else {
            send_response(
                respond,
                StatusCode::BAD_REQUEST,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        };
        if admission.registrar.node_id() != &peer_node_id {
            send_response(
                respond,
                StatusCode::FORBIDDEN,
                None,
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let admission_key = RelayAdmissionKey {
            peer_node_id: peer_node_id.clone(),
            registration: admission.clone(),
        };
        let attempt = self.relay_attempt_key(peer_node_id, peer_epoch, grant.delivery);
        if !relay_owner.accepts_epoch(peer_epoch) {
            return self
                .send_relay_grant_response(respond, RelayGrantDisposition::Cancelled)
                .await;
        }
        if let Some(status) = relay_owner.retired_status(&attempt) {
            let disposition = match status {
                RelayAdmissionStatus::Admitted => RelayGrantDisposition::Admitted,
                RelayAdmissionStatus::Rejected(reason) => RelayGrantDisposition::Rejected(reason),
                RelayAdmissionStatus::Cancelled => RelayGrantDisposition::Cancelled,
                RelayAdmissionStatus::Retired => RelayGrantDisposition::Retired,
                RelayAdmissionStatus::Reserved
                | RelayAdmissionStatus::BodyReceived
                | RelayAdmissionStatus::Unknown
                | RelayAdmissionStatus::Indeterminate => RelayGrantDisposition::Retired,
            };
            return self.send_relay_grant_response(respond, disposition).await;
        }
        let existing = relay_owner.attempt(&attempt);
        if let Some(existing) = existing {
            if existing.body_bytes != grant.body_bytes || existing.metadata != grant.metadata {
                send_static_error(
                    &mut respond,
                    StatusCode::CONFLICT,
                    "relay delivery identity names different content",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            let status = existing.status();
            let disposition = existing.grant_disposition();
            self.send_relay_grant_response(respond, disposition).await?;
            if status.is_terminal() {
                self.retire_relay_record(&existing, status);
            }
            return Ok(());
        }
        if relay_owner.channel_busy(&attempt) {
            send_static_error(
                &mut respond,
                StatusCode::TOO_MANY_REQUESTS,
                "relay channel already has an unadmitted batch",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        if !relay_owner.can_follow(&attempt) {
            send_static_error(
                &mut respond,
                StatusCode::CONFLICT,
                "relay channel incarnation is unknown or its sequence is out of order",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let operation_bytes = grant
            .body_bytes
            .checked_add(self.executor.limits().relay_decoded_bytes.as_u64())
            .ok_or_else(|| {
                TransportError::RelayGrant("relay operation limits overflow".to_string())
            })?;
        let operation_bytes = operation_bytes
            .checked_add(self.executor.limits().relay_scratch_bytes.as_u64())
            .ok_or_else(|| {
                TransportError::RelayGrant("relay operation limits overflow".to_string())
            })?;
        let reservation = match self
            .executor
            .try_reserve(MemoryClass::Relay, operation_bytes)
        {
            Ok(reservation) => reservation,
            Err(error) => {
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay admission is full",
                    self.options.progress_timeout,
                )
                .await?;
                debug!(?error, "relay grant refused by memory admission");
                return Ok(());
            }
        };
        let item = match StdArc::clone(&self.relay_items).try_acquire_owned() {
            Ok(item) => item,
            Err(_) => {
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay item admission is full",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
        };
        let terminal = match StdArc::clone(&self.terminal_outcomes).try_acquire_owned() {
            Ok(terminal) => terminal,
            Err(_) => {
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay terminal-outcome admission is full",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
        };
        if relay_owner.has_admission(&admission_key) {
            send_static_error(
                &mut respond,
                StatusCode::CONFLICT,
                "relay admission acknowledgement is already active",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let grant_id = self.next_grant_id(&relay_owner);
        let record = StdArc::new(RelayAdmissionRecord {
            attempt: attempt.clone(),
            admission_key: admission_key.clone(),
            body_bytes: grant.body_bytes,
            metadata: grant.metadata,
            _metadata_memory: metadata_memory,
            choice: AdmissionChoice::new(),
            state: nervix_primitives::sync::blocking::Mutex::new(RelayAdmissionProtocol {
                phase: RelayBodyPhase::Reserved { grant_id },
                rejection: None,
                capacity: Some(RelayAdmissionCapacity {
                    _item: item,
                    _terminal: terminal,
                }),
                last_progress: Instant::now(),
            }),
            cancellation: CancellationToken::new(),
            reserved_at: Instant::now(),
            observations: self.observations.clone(),
            owner: StdArc::downgrade(&relay_owner),
        });
        let expiry = CancellationToken::new();
        let grant = RelayGrant {
            expires_at: Instant::now()
                .checked_add(RELAY_GRANT_LIFETIME)
                .assured("the fixed relay grant lifetime fits the monotonic clock"),
            reservation,
            admission: StdArc::clone(&record),
            _expiry: CancelOnDrop::new(expiry.clone()),
        };
        match relay_owner.register(grant_id, grant) {
            RelayGrantRegistration::Registered => {}
            RelayGrantRegistration::Existing(existing) => {
                if existing.body_bytes != record.body_bytes || existing.metadata != record.metadata
                {
                    send_static_error(
                        &mut respond,
                        StatusCode::CONFLICT,
                        "relay delivery identity names different content",
                        self.options.progress_timeout,
                    )
                    .await?;
                    return Ok(());
                }
                let status = existing.status();
                let disposition = existing.grant_disposition();
                self.send_relay_grant_response(respond, disposition).await?;
                if status.is_terminal() {
                    self.retire_relay_record(&existing, status);
                }
                return Ok(());
            }
            RelayGrantRegistration::Retired(disposition) => {
                self.send_relay_grant_response(respond, disposition).await?;
                return Ok(());
            }
            RelayGrantRegistration::InvalidSequence => {
                send_static_error(
                    &mut respond,
                    StatusCode::CONFLICT,
                    "relay channel incarnation is unknown or its sequence is out of order",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            RelayGrantRegistration::ChannelBusy => {
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay channel already has an unadmitted batch",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            RelayGrantRegistration::AdmissionBusy => {
                send_static_error(
                    &mut respond,
                    StatusCode::CONFLICT,
                    "relay admission acknowledgement is already active",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
        }
        let expiring_owner = relay_owner.clone();
        self.tasks.spawn(async move {
            nervix_primitives::select! {
                _ = expiry.cancelled() => {}
                _ = sleep(RELAY_GRANT_LIFETIME) => {
                    expiring_owner.expire_grant(grant_id, Instant::now());
                }
            }
        });
        self.send_relay_grant_response(respond, RelayGrantDisposition::SendBody { grant_id })
            .await
    }

    fn next_grant_id(&self, owner: &RelayPeerOwner) -> u64 {
        loop {
            let id = self.options.entropy.next_u64();
            if id != 0 && !owner.has_grant(id) {
                return id;
            }
        }
    }

    fn relay_attempt_key(
        &self,
        peer_node_id: ClusterNodeName,
        sender_epoch: u64,
        delivery: RelayDelivery,
    ) -> RelayAttemptKey {
        RelayAttemptKey {
            channel: RelayChannelKey {
                peer_node_id,
                sender_epoch,
                receiver_epoch: self.process_epoch,
                channel_incarnation: delivery.channel_incarnation,
            },
            sequence: delivery.sequence,
        }
    }

    pub(super) async fn handle_relay_body(
        &self,
        peer: InboundPeer,
        grant_id: u64,
        request: Request<RecvStream>,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), Report<TransportError>> {
        let sender_epoch = header_u64(&request, "x-nervix-sender-epoch")?;
        let receiver_epoch = header_u64(&request, "x-nervix-receiver-epoch")?;
        let claimed = if sender_epoch == peer.process_epoch {
            peer.relay_owner
                .claim_grant(grant_id, sender_epoch, receiver_epoch, Instant::now())
        } else {
            None
        };
        let Some(grant) = claimed else {
            respond.send_reset(Reason::REFUSED_STREAM);
            return Ok(());
        };
        drop(grant._expiry);
        let mut body_completion = RelayBodyCompletionGuard::new(StdArc::clone(&grant.admission));

        let encoded_limit = self.executor.limits().relay_encoded_bytes.as_u64();
        let encoded_bytes = grant.admission.body_bytes;
        let (encoded_reservation, overlap) = grant
            .reservation
            .split(encoded_bytes)
            .map_err(|error| TransportError::with_cause(error, TransportError::RelayGrant))?;
        let mut buffer = BudgetedBuffer::with_limit(encoded_reservation, encoded_limit);
        {
            let reading = read_body_into(
                &mut buffer,
                self.options.progress_timeout,
                request.into_body(),
            );
            tokio::pin!(reading);
            nervix_primitives::select! {
                _ = grant.admission.cancellation.cancelled() => {
                    respond.send_reset(Reason::CANCEL);
                    return Ok(());
                }
                result = &mut reading => result?,
            }
        }
        let actual: u64 = buffer
            .len()
            .try_into()
            .assured("an in-memory allocation length fits in u64");
        if actual != grant.admission.body_bytes {
            respond.send_reset(Reason::ENHANCE_YOUR_CALM);
            return Err(Report::new(TransportError::RelayGrant(format!(
                "relay body length {actual} differs from granted length {}",
                grant.admission.body_bytes
            ))));
        }
        let (body, body_reservation) = buffer.into_parts();
        let operation_reservation = body_reservation
            .merge(overlap)
            .map_err(|error| TransportError::with_cause(error, TransportError::RelayGrant))?;
        let body = ChargedBytes::from_owned(body, operation_reservation);
        let payload = grant
            .admission
            .metadata
            .clone()
            .into_payload(grant.admission.attempt.delivery(), body);
        if !grant.admission.mark_body_received() {
            respond.send_reset(Reason::CANCEL);
            return Ok(());
        }
        let received = ReceivedEnvelope::new_relay(
            peer.addr,
            peer.node_id,
            payload,
            RelayAdmission {
                record: StdArc::clone(&grant.admission),
            },
        );
        if self.incoming_tx.try_send(received).is_err() {
            grant
                .admission
                .reject("the application ingress queue is full".to_string());
            respond.send_reset(Reason::ENHANCE_YOUR_CALM);
            return Err(Report::new(TransportError::IncomingQueueFull));
        }
        body_completion.complete();
        send_response(
            respond,
            StatusCode::NO_CONTENT,
            None,
            self.options.progress_timeout,
        )
        .await
    }
}

fn header_u64<B>(request: &Request<B>, name: &'static str) -> Result<u64, Report<TransportError>> {
    let value = request
        .headers()
        .get(name)
        .ok_or_else(|| TransportError::RelayGrant(format!("missing {name} header")))?;
    let value = value.to_str().map_err(|error| {
        TransportError::with_cause(Report::new(error), TransportError::RelayGrant)
    })?;
    let parsed = value.parse().map_err(|error| {
        TransportError::with_cause(Report::new(error), |reason| {
            TransportError::RelayGrant(format!("invalid {name} header: {reason}"))
        })
    })?;
    Ok(parsed)
}

#[cfg(test)]
mod header_tests {
    use super::*;

    #[test]
    fn relay_grant_headers_report_missing_invalid_and_non_text_values() {
        let missing = Request::new(());
        let error = header_u64(&missing, "x-relay-count").expect_err("the header is required");
        assert!(matches!(
            error.current_context(),
            TransportError::RelayGrant(_)
        ));

        let invalid = Request::builder()
            .header("x-relay-count", "not-a-number")
            .body(())
            .expect("test request should be valid");
        let error = header_u64(&invalid, "x-relay-count").expect_err("a number is required");
        assert!(matches!(
            error.current_context(),
            TransportError::RelayGrant(_)
        ));
        assert!(error.contains::<std::num::ParseIntError>());

        let non_text = Request::builder()
            .header(
                "x-relay-count",
                http::HeaderValue::from_bytes(&[0xff]).expect("obs-text is a valid header value"),
            )
            .body(())
            .expect("test request should be valid");
        let error = header_u64(&non_text, "x-relay-count").expect_err("text is required");
        assert!(matches!(
            error.current_context(),
            TransportError::RelayGrant(_)
        ));
        assert!(error.contains::<http::header::ToStrError>());

        let valid = Request::builder()
            .header("x-relay-count", "42")
            .body(())
            .expect("test request should be valid");
        let Ok(parsed) = header_u64(&valid, "x-relay-count") else {
            panic!("a valid numeric grant header must parse");
        };
        assert_eq!(parsed, 42);
    }
}
