//! Reliable relay admission and transfer over authenticated peer connections.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Relay grants, body transfer, runtime-admission progress, cancellation fences,
//!   process-epoch reconciliation, channel sequence watermarks, and terminal outcome retirement.
//! - **Depends on.** The parent interconnect connection state, execution admission, and relay wire
//!   models.
//! - **Must not know.** Runtime graphs, schedules, branch processing state, or external connectors.

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
            tokio::task::consume_budget().await;
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
            tokio::select! {
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
        let receiver_epoch = if let Some(epoch) = self.outbound_relay_epochs.get(&outbound_key) {
            *epoch
        } else {
            lease.connection.peer_epoch
        };
        if receiver_epoch != lease.connection.peer_epoch {
            self.retire_outbound_relay(&outbound_key);
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
            self.retire_outbound_relay(&outbound_key);
            return Ok(RelayAdmissionStatus::Indeterminate);
        }
        if response.status.is_terminal()
            || matches!(
                &response.status,
                RelayAdmissionStatus::Unknown | RelayAdmissionStatus::Indeterminate
            )
        {
            self.retire_outbound_relay(&outbound_key);
        }
        Ok(response.status)
    }

    pub(super) async fn send_relay(
        &self,
        node_id: &ClusterNodeName,
        payload: RelayPayload,
    ) -> Result<(), TransportError> {
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
        match self.outbound_relay_epochs.entry(outbound_key.clone()) {
            Entry::Occupied(entry) => {
                if *entry.get() != management.connection.peer_epoch {
                    drop(entry);
                    self.retire_outbound_relay(&outbound_key);
                    return Err(TransportError::RelayIndeterminate);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(management.connection.peer_epoch);
            }
        }
        let admission = payload.admission.as_ref().ok_or_else(|| {
            TransportError::RelayGrant(
                "relay payload is missing its admission registration".to_string(),
            )
        })?;
        let admission_key = RelayAdmissionKey {
            peer_node_id: node_id.clone(),
            ack_id: admission.ack_id,
        };
        match self.outbound_relay_admissions.entry(admission_key.clone()) {
            Entry::Occupied(entry) => {
                if entry.get() != &outbound_key {
                    return Err(TransportError::RelayGrant(
                        "relay admission acknowledgement names another delivery".to_string(),
                    ));
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(outbound_key.clone());
            }
        }
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
            .await?;
        let grant = wire::decode_rkyv::<RelayGrantResponse>(
            &self.executor,
            MemoryClass::Management,
            CpuClass::Control,
            grant_response,
        )
        .await?
        .into_value();
        if grant.receiver_epoch != management.connection.peer_epoch {
            self.retire_outbound_relay(&outbound_key);
            return Err(TransportError::RelayIndeterminate);
        }
        let grant_id = match grant.disposition {
            RelayGrantDisposition::SendBody { grant_id } => grant_id,
            RelayGrantDisposition::BodyReceived => return Ok(()),
            RelayGrantDisposition::Admitted => {
                self.deliver_terminal_incoming(
                    management.connection.key.target.addr,
                    node_id.clone(),
                    Envelope::Ack(nervix_models::RemoteAckResolution {
                        ack_id: admission.ack_id,
                        outcome: RemoteAckOutcome::Ack,
                    }),
                    None,
                )
                .await?;
                self.retire_outbound_relay(&outbound_key);
                return Ok(());
            }
            RelayGrantDisposition::Rejected(reason) => {
                self.retire_outbound_relay(&outbound_key);
                return Err(TransportError::RelayRejected(reason));
            }
            RelayGrantDisposition::Cancelled => {
                self.retire_outbound_relay(&outbound_key);
                return Err(TransportError::RelayCancelled);
            }
            RelayGrantDisposition::Retired => {
                self.retire_outbound_relay(&outbound_key);
                return Err(TransportError::RelayIndeterminate);
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

        let mut interval = tokio::time::interval(RELAY_PROGRESS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = self.admission_closed.cancelled() => break,
                _ = interval.tick() => {}
            }

            let registrations = self
                .relay_attempts
                .iter()
                .filter_map(|record| record.progress_registration())
                .collect::<Vec<_>>();
            let mut reports = FuturesUnordered::new();
            for registration in registrations {
                tokio::task::consume_budget().await;
                let state = self.clone();
                reports.push(async move {
                    let target = registration.reply_node_id.clone();
                    let ack_id = registration.ack_id;
                    let result = timeout(
                        RELAY_PROGRESS_SEND_TIMEOUT,
                        state.send(
                            &target,
                            Envelope::Ack(nervix_models::RemoteAckResolution {
                                ack_id,
                                outcome: RemoteAckOutcome::Alive,
                            }),
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
                tokio::task::consume_budget().await;
                let completed = tokio::select! {
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
        let mut interval = tokio::time::interval(RELAY_CHANNEL_SWEEP_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = self.admission_closed.cancelled() => break,
                _ = interval.tick() => {}
            }
            let now = Instant::now();
            self.relay_watermarks.retain(|channel, watermark| {
                if self.active_relay_channels.contains_key(channel) {
                    return true;
                }
                now.checked_duration_since(watermark.reconciled_at)
                    .assured("a relay channel's reconciliation time does not move backwards")
                    < RELAY_CHANNEL_RETENTION
            });
        }
    }

    pub(super) async fn handle_relay_admission_control(
        &self,
        peer_node_id: ClusterNodeName,
        peer_epoch: u64,
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
        let mut record_to_retire = None;
        let mut remove_cancellation_fence = false;
        let status = match self.relay_attempts.entry(attempt.clone()) {
            Entry::Occupied(entry) => {
                let existing = entry.get().clone();
                drop(entry);
                match existing {
                    RelayAttemptEntry::Active(record) => {
                        let status = if cancel {
                            let grant_id = record.reserved_grant_id();
                            let status = record.cancel();
                            if let Some(grant_id) = grant_id {
                                self.grants.remove_if(&grant_id, |_, grant| {
                                    StdArc::ptr_eq(&grant.admission, &record)
                                });
                            }
                            status
                        } else {
                            record.status()
                        };
                        record_to_retire = Some(record);
                        status
                    }
                    RelayAttemptEntry::CancellationFence => RelayAdmissionStatus::Cancelled,
                }
            }
            Entry::Vacant(entry) => {
                if let Some(status) = self.retired_relay_status(&attempt) {
                    status
                } else if cancel
                    && !self.active_relay_channels.contains_key(&attempt.channel())
                    && self.relay_sequence_follows_watermark(&attempt)
                {
                    let fence = entry.insert(RelayAttemptEntry::CancellationFence);
                    self.record_relay_watermark(&attempt, RelayAdmissionStatus::Cancelled);
                    drop(fence);
                    remove_cancellation_fence = true;
                    RelayAdmissionStatus::Cancelled
                } else {
                    RelayAdmissionStatus::Unknown
                }
            }
        };
        if remove_cancellation_fence {
            self.relay_attempts.remove_if(&attempt, |_, candidate| {
                matches!(candidate, RelayAttemptEntry::CancellationFence)
            });
        }
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
        let grant = wire::decode_rkyv::<RelayGrantRequest>(
            &self.executor,
            MemoryClass::Relay,
            CpuClass::Data,
            encoded,
        )
        .await?
        .into_value();
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
        if admission.reply_node_id != peer_node_id {
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
            ack_id: admission.ack_id,
        };
        let attempt = self.relay_attempt_key(peer_node_id.clone(), peer_epoch, grant.delivery);
        if let Some(status) = self.retired_relay_status(&attempt) {
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
        let existing = self
            .relay_attempts
            .get(&attempt)
            .map(|entry| entry.value().clone());
        if let Some(existing) = existing {
            match existing {
                RelayAttemptEntry::Active(record) => {
                    if record.body_bytes != grant.body_bytes || record.metadata != grant.metadata {
                        send_static_error(
                            &mut respond,
                            StatusCode::CONFLICT,
                            "relay delivery identity names different content",
                            self.options.progress_timeout,
                        )
                        .await?;
                        return Ok(());
                    }
                    let status = record.status();
                    let disposition = record.grant_disposition();
                    self.send_relay_grant_response(respond, disposition).await?;
                    if status.is_terminal() {
                        self.retire_relay_record(&record, status);
                    }
                }
                RelayAttemptEntry::CancellationFence => {
                    self.send_relay_grant_response(respond, RelayGrantDisposition::Cancelled)
                        .await?;
                }
            }
            return Ok(());
        }
        let channel = attempt.channel();
        if let Some(active_attempt) = self
            .active_relay_channels
            .get(&channel)
            .map(|entry| entry.value().clone())
        {
            let still_unadmitted = self
                .relay_attempts
                .get(&active_attempt)
                .is_some_and(|record| record.is_unadmitted());
            if still_unadmitted {
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay channel already has an unadmitted batch",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            self.active_relay_channels
                .remove_if(&channel, |_, current| current == &active_attempt);
        }
        if !self.relay_sequence_follows_watermark(&attempt) {
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
        if self.relay_admissions.contains_key(&admission_key) {
            send_static_error(
                &mut respond,
                StatusCode::CONFLICT,
                "relay admission acknowledgement is already active",
                self.options.progress_timeout,
            )
            .await?;
            return Ok(());
        }
        let grant_id = self.next_grant_id();
        let record = StdArc::new(RelayAdmissionRecord {
            attempt: attempt.clone(),
            admission_key: admission_key.clone(),
            body_bytes: grant.body_bytes,
            metadata: grant.metadata.clone(),
            state: parking_lot::Mutex::new(RelayAdmissionState::Reserved { grant_id }),
            cancellation: CancellationToken::new(),
            _item: item,
            _terminal: terminal,
        });
        let expiry = CancellationToken::new();
        self.grants.insert(
            grant_id,
            RelayGrant {
                expires_at: Instant::now()
                    .checked_add(RELAY_GRANT_LIFETIME)
                    .assured("the fixed relay grant lifetime fits the monotonic clock"),
                reservation,
                admission: StdArc::clone(&record),
                _expiry: CancelOnDrop::new(expiry.clone()),
            },
        );
        let attempt_entry = match self.relay_attempts.entry(attempt) {
            Entry::Occupied(entry) => {
                self.grants.remove(&grant_id);
                let existing = entry.get().clone();
                drop(entry);
                match existing {
                    RelayAttemptEntry::Active(existing) => {
                        if existing.body_bytes != record.body_bytes
                            || existing.metadata != record.metadata
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
                    }
                    RelayAttemptEntry::CancellationFence => {
                        self.send_relay_grant_response(respond, RelayGrantDisposition::Cancelled)
                            .await?;
                    }
                }
                return Ok(());
            }
            Entry::Vacant(entry) => {
                if let Some(status) = self.retired_relay_status(&record.attempt) {
                    drop(entry);
                    self.grants.remove(&grant_id);
                    let disposition = match status {
                        RelayAdmissionStatus::Admitted => RelayGrantDisposition::Admitted,
                        RelayAdmissionStatus::Rejected(reason) => {
                            RelayGrantDisposition::Rejected(reason)
                        }
                        RelayAdmissionStatus::Cancelled => RelayGrantDisposition::Cancelled,
                        RelayAdmissionStatus::Retired
                        | RelayAdmissionStatus::Reserved
                        | RelayAdmissionStatus::BodyReceived
                        | RelayAdmissionStatus::Unknown
                        | RelayAdmissionStatus::Indeterminate => RelayGrantDisposition::Retired,
                    };
                    self.send_relay_grant_response(respond, disposition).await?;
                    return Ok(());
                }
                if !self.relay_sequence_follows_watermark(&record.attempt) {
                    drop(entry);
                    self.grants.remove(&grant_id);
                    send_static_error(
                        &mut respond,
                        StatusCode::CONFLICT,
                        "relay channel incarnation is unknown or its sequence is out of order",
                        self.options.progress_timeout,
                    )
                    .await?;
                    return Ok(());
                }
                entry.insert(RelayAttemptEntry::Active(StdArc::clone(&record)))
            }
        };
        match self.active_relay_channels.entry(record.attempt.channel()) {
            Entry::Occupied(_) => {
                drop(attempt_entry);
                self.relay_attempts.remove(&record.attempt);
                self.grants.remove(&grant_id);
                send_static_error(
                    &mut respond,
                    StatusCode::TOO_MANY_REQUESTS,
                    "relay channel already has an unadmitted batch",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            Entry::Vacant(entry) => {
                entry.insert(record.attempt.clone());
            }
        }
        match self.relay_admissions.entry(admission_key) {
            Entry::Occupied(_) => {
                drop(attempt_entry);
                self.relay_attempts.remove(&record.attempt);
                self.active_relay_channels
                    .remove_if(&record.attempt.channel(), |_, attempt| {
                        attempt == &record.attempt
                    });
                self.grants.remove(&grant_id);
                send_static_error(
                    &mut respond,
                    StatusCode::CONFLICT,
                    "relay admission acknowledgement is already active",
                    self.options.progress_timeout,
                )
                .await?;
                return Ok(());
            }
            Entry::Vacant(entry) => {
                entry.insert(StdArc::clone(&record));
            }
        }
        drop(attempt_entry);
        let grants = self.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                _ = expiry.cancelled() => {}
                _ = sleep(RELAY_GRANT_LIFETIME) => {
                    let expired = grants
                        .grants
                        .remove_if(&grant_id, |_, grant| Instant::now() >= grant.expires_at);
                    if let Some((_, grant)) = expired {
                        let status = grant.admission.cancel();
                        grants.retire_relay_record(&grant.admission, status);
                    }
                }
            }
        });
        self.send_relay_grant_response(respond, RelayGrantDisposition::SendBody { grant_id })
            .await
    }

    fn next_grant_id(&self) -> u64 {
        loop {
            let id = OsRng.next_u64();
            if id != 0 && !self.grants.contains_key(&id) {
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
            peer_node_id,
            sender_epoch,
            receiver_epoch: self.process_epoch,
            delivery,
        }
    }

    fn retire_outbound_relay(&self, key: &OutboundRelayKey) {
        self.outbound_relay_epochs.remove(key);
        self.outbound_relay_admissions
            .retain(|_, delivery| delivery != key);
    }

    pub(super) fn retire_outbound_relay_admission(&self, admission_key: &RelayAdmissionKey) {
        if let Some((_, key)) = self.outbound_relay_admissions.remove(admission_key) {
            self.outbound_relay_epochs.remove(&key);
        }
    }

    fn retired_relay_status(&self, attempt: &RelayAttemptKey) -> Option<RelayAdmissionStatus> {
        let mut watermark = self.relay_watermarks.get_mut(&attempt.channel())?;
        watermark.reconciled_at = Instant::now();
        if attempt.delivery.sequence < watermark.sequence {
            return Some(RelayAdmissionStatus::Retired);
        }
        if attempt.delivery.sequence == watermark.sequence {
            return Some(watermark.status.clone());
        }
        None
    }

    fn relay_sequence_follows_watermark(&self, attempt: &RelayAttemptKey) -> bool {
        let Some(watermark) = self.relay_watermarks.get(&attempt.channel()) else {
            return attempt.delivery.sequence == 0;
        };
        let Some(next_sequence) = watermark.sequence.checked_add(1) else {
            return false;
        };
        attempt.delivery.sequence == next_sequence
    }

    fn record_relay_watermark(&self, attempt: &RelayAttemptKey, status: RelayAdmissionStatus) {
        match self.relay_watermarks.entry(attempt.channel()) {
            Entry::Occupied(mut entry) => {
                if attempt.delivery.sequence >= entry.get().sequence {
                    entry.insert(RelayChannelWatermark {
                        sequence: attempt.delivery.sequence,
                        status,
                        reconciled_at: Instant::now(),
                    });
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(RelayChannelWatermark {
                    sequence: attempt.delivery.sequence,
                    status,
                    reconciled_at: Instant::now(),
                });
            }
        }
    }

    fn retire_relay_record(
        &self,
        record: &StdArc<RelayAdmissionRecord>,
        status: RelayAdmissionStatus,
    ) {
        self.record_relay_watermark(&record.attempt, status);
        self.relay_attempts
            .remove_if(&record.attempt, |_, candidate| {
                if let RelayAttemptEntry::Active(candidate) = candidate {
                    StdArc::ptr_eq(candidate, record)
                } else {
                    false
                }
            });
        self.active_relay_channels
            .remove_if(&record.attempt.channel(), |_, attempt| {
                attempt == &record.attempt
            });
        self.relay_admissions
            .remove_if(&record.admission_key, |_, candidate| {
                StdArc::ptr_eq(candidate, record)
            });
    }

    pub(super) fn retire_relay_admission(&self, admission_key: &RelayAdmissionKey) {
        let Some(record) = self
            .relay_admissions
            .get(admission_key)
            .map(|record| StdArc::clone(record.value()))
        else {
            return;
        };
        self.retire_relay_record(&record, record.status());
    }

    pub(super) async fn handle_relay_body(
        &self,
        peer_addr: SocketAddr,
        peer_node_id: ClusterNodeName,
        peer_epoch: u64,
        grant_id: u64,
        request: Request<RecvStream>,
        mut respond: server::SendResponse<Bytes>,
    ) -> Result<(), TransportError> {
        let sender_epoch = header_u64(&request, "x-nervix-sender-epoch")?;
        let receiver_epoch = header_u64(&request, "x-nervix-receiver-epoch")?;
        let claimed = self.grants.remove_if(&grant_id, |_, grant| {
            grant.admission.attempt.peer_node_id == peer_node_id
                && grant.admission.attempt.sender_epoch == sender_epoch
                && grant.admission.attempt.sender_epoch == peer_epoch
                && grant.admission.attempt.receiver_epoch == receiver_epoch
                && Instant::now() < grant.expires_at
        });
        let Some((_, grant)) = claimed else {
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
            .map_err(|error| TransportError::RelayGrant(error.to_string()))?;
        let mut buffer = BudgetedBuffer::with_limit(encoded_reservation, encoded_limit);
        {
            let reading = read_body_into(
                &mut buffer,
                self.options.progress_timeout,
                request.into_body(),
            );
            tokio::pin!(reading);
            tokio::select! {
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
            return Err(TransportError::RelayGrant(format!(
                "relay body length {actual} differs from granted length {}",
                grant.admission.body_bytes
            )));
        }
        let (body, body_reservation) = buffer.into_parts();
        let operation_reservation = body_reservation
            .merge(overlap)
            .map_err(|error| TransportError::RelayGrant(error.to_string()))?;
        let body = ChargedBytes::from_owned(body, operation_reservation);
        let payload = grant
            .admission
            .metadata
            .clone()
            .into_payload(grant.admission.attempt.delivery, body);
        if !grant.admission.mark_body_received() {
            respond.send_reset(Reason::CANCEL);
            return Ok(());
        }
        let received = ReceivedEnvelope::new_relay(
            peer_addr,
            peer_node_id,
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
            return Err(TransportError::IncomingQueueFull);
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
