use rumqttc::{
    AckMode, AsyncClient, BrokerSessionResumePolicy, Event, Incoming, MqttOptions, Publish, QoS,
    SessionMode, SubscribeReasonCode, TlsConfiguration, Transport as MqttTransport,
};
use url::{Host, Url};

use super::super::*;

pub(in crate::runtime) struct MqttIngestor;

const MQTT_INSTANCE_PLACEHOLDER: &str = "{{instance}}";

#[derive(Debug, PartialEq, Eq)]
pub(in crate::runtime) struct MqttIngestorAddr {
    pub(in crate::runtime) host: String,
    pub(in crate::runtime) port: u16,
    pub(in crate::runtime) tls: bool,
}

#[derive(Clone)]
struct MqttTaskContext {
    runtime: Runtime,
    domain: Domain,
    ingestor: Identifier,
    error_policies: ErrorPolicies,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    codec: Arc<CompiledCodec>,
    branched_senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
    events: broadcast::Sender<RuntimeEvent>,
    quiesce: Arc<IngestorQuiesceControl>,
}

#[derive(Clone)]
struct MqttClientSettings {
    session: MqttSession,
    manual_acks: bool,
}

struct MqttBatchEntry {
    publish: Publish,
    record: RuntimeRecordBatch,
}

enum MqttNextPublish {
    Publish(Box<Publish>),
    Flush,
    Shutdown,
    Reconnect,
    Suspend,
}

enum MqttSubscriptionState {
    Ready,
    Shutdown,
    Reconnect,
}

impl MqttIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientMqtt,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (topic, instances, mode) = match &ingestor.source {
            IngestSource::Mqtt {
                topic,
                instances,
                mode,
                ..
            } => (topic.clone(), *instances, mode.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected MQTT ingestor source".to_string(),
                });
            }
        };
        let ack_timeout = match &mode {
            MqttIngestMode::AckParallel { timeout, .. }
            | MqttIngestMode::AckSequential { timeout, .. } => {
                Some(Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?)
            }
            MqttIngestMode::NoAckParallel { .. } | MqttIngestMode::NoAckSequential { .. } => None,
        };
        let retry_policy = match &mode {
            MqttIngestMode::AckParallel { retry_policy, .. }
            | MqttIngestMode::AckSequential { retry_policy, .. } => Some(
                Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            ),
            MqttIngestMode::NoAckParallel { .. } | MqttIngestMode::NoAckSequential { .. } => None,
        };
        let batch_timeout = match &mode {
            MqttIngestMode::AckParallel { batch_timeout, .. } => {
                Some(Runtime::parse_duration_setting(
                    domain,
                    &ingestor.name,
                    "batch timeout",
                    batch_timeout,
                )?)
            }
            _ => None,
        };

        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        runtime
            .resolve_client_config_with_instance(domain, client.mount.as_ref(), &client.config, 0)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        runtime.prepare_ingestor_readiness(domain, &ingestor.name, instances);
        if let Err(error) =
            Self::client_id_template(&client.config, ingestor.name.as_str(), instances)
        {
            runtime.record_ingestor_transient_error(domain, &ingestor.name, error);
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task = tokio::spawn(async move {
                loop {
                    tokio::task::consume_budget().await;
                    if shutdown_rx.changed().await.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    "stopped mqtt ingestor"
                );
            });
            runtime.ingestors.insert(
                key,
                IngestorRuntime::Background {
                    shutdown: shutdown_tx,
                    branched: Vec::new(),
                    tasks: vec![task],
                },
            );
            return Ok(());
        }
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let branched_senders = branched_runtime.senders.clone();
        let output_routes = dependencies.output_routes;
        let filter_where = dependencies.filter_where;
        let codec = dependencies.codec;
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .expect("scheduled MQTT ingestor must have quiesce control");

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances as usize);
        let subscribe_filter = Self::subscribe_filter(&topic, domain, &ingestor.name);
        let settings = MqttClientSettings {
            session: mode.session(),
            manual_acks: mode.is_ack(),
        };

        for instance_idx in 0..instances {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_context = MqttTaskContext {
                runtime: runtime.clone(),
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                error_policies: internal_processor_error_policies(
                    ingestor.general_error_policy.clone(),
                ),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: output_routes.clone(),
                filter_where: filter_where.clone(),
                codec: codec.clone(),
                branched_senders: branched_senders.clone(),
                events: runtime.events.clone(),
                quiesce: quiesce.clone(),
            };
            let task_topic = topic.clone();
            let task_subscribe_filter = subscribe_filter.clone();
            let task_config = client.config.clone();
            let task_client_mount = client.mount.clone();
            let task_mode = mode.clone();
            let task_settings = settings.clone();
            let task_ack_timeout = ack_timeout;
            let task_retry_policy = retry_policy.unwrap_or(ParsedRetryPolicy {
                backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            });
            let task_batch_timeout = batch_timeout;
            let task = tokio::spawn(async move {
                let qos = Self::qos(task_mode.qos());
                let mut backoff = RuntimeReconnectBackoff::default();
                let mut ingest_collector =
                    IngestRouteCollector::new(IngestMetadataKind::Headers, INGEST_GROUP_MAX_ROWS);

                info!(
                    domain = task_context.domain.as_str(),
                    ingestor = task_context.ingestor.as_str(),
                    topic = task_topic.as_str(),
                    subscription = task_subscribe_filter.as_str(),
                    instance = instance_idx,
                    "started mqtt ingestor"
                );

                'outer: loop {
                    tokio::task::consume_budget().await;
                    task_context.runtime.mark_ingestor_instance_unready(
                        &task_context.domain,
                        &task_context.ingestor,
                        instance_idx,
                    );
                    if task_context
                        .runtime
                        .wait_if_ingestor_faulted(
                            &task_context.domain,
                            &task_context.ingestor,
                            &mut shutdown_rx,
                        )
                        .await
                    {
                        break;
                    }
                    if task_context
                        .runtime
                        .ingestor_faults
                        .is_failed(&task_context.ingestor)
                    {
                        continue;
                    }
                    if task_context.quiesce.should_suspend_intake() {
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = task_context.quiesce.wait_until_not_suspended() => {}
                        }
                        continue;
                    }
                    let resolved_client =
                        match task_context.runtime.resolve_client_config_with_instance(
                            &task_context.domain,
                            task_client_mount.as_ref(),
                            &task_config,
                            instance_idx,
                        ) {
                            Ok(resolved) => resolved,
                            Err(error) => {
                                task_context
                                    .runtime
                                    .record_ingestor_transient_error_with_backoff(
                                        &task_context.domain,
                                        &task_context.ingestor,
                                        format!("mqtt client config failed: {error}"),
                                        backoff.next_delay(),
                                    );
                                warn!(
                                    domain = task_context.domain.as_str(),
                                    ingestor = task_context.ingestor.as_str(),
                                    error = %error,
                                    "failed to render mqtt client config"
                                );
                                if !backoff.wait(&mut shutdown_rx).await {
                                    break;
                                }
                                continue;
                            }
                        };
                    let _client_mounts = resolved_client.mounts.clone();
                    let (client_handle, mut eventloop) = match Self::client_from_config(
                        &resolved_client.entries,
                        task_context.ingestor.as_str(),
                        &task_settings,
                    ) {
                        Ok(client) => client,
                        Err(error) => {
                            task_context
                                .runtime
                                .record_ingestor_transient_error_with_backoff(
                                    &task_context.domain,
                                    &task_context.ingestor,
                                    format!("mqtt connect failed: {error}"),
                                    backoff.next_delay(),
                                );
                            warn!(
                                domain = task_context.domain.as_str(),
                                ingestor = task_context.ingestor.as_str(),
                                error = %error,
                                "failed to create mqtt client"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    match Self::establish_subscription(
                        &client_handle,
                        task_subscribe_filter.as_str(),
                        qos,
                        &mut eventloop,
                        &mut shutdown_rx,
                        &task_context,
                        instance_idx,
                    )
                    .await
                    {
                        MqttSubscriptionState::Ready => {}
                        MqttSubscriptionState::Shutdown => break 'outer,
                        MqttSubscriptionState::Reconnect => {
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    }

                    task_context.runtime.clear_ingestor_transient_error(
                        &task_context.domain,
                        &task_context.ingestor,
                    );
                    backoff.reset();

                    loop {
                        tokio::task::consume_budget().await;
                        if let Some(payload) = task_context.quiesce.pop_buffered(instance_idx) {
                            if let Err(error) = task_context
                                .runtime
                                .dispatch_raw_ingest_payload(RawIngestDispatch {
                                    domain: &task_context.domain,
                                    ingestor: &task_context.ingestor,
                                    timestamp_source: task_context.timestamp_source.as_ref(),
                                    output_routes: &task_context.output_routes,
                                    filter_where: task_context.filter_where.as_ref(),
                                    branched_senders: &task_context.branched_senders,
                                    codec: task_context.codec.clone(),
                                    payload: &payload,
                                    collector: &mut ingest_collector,
                                    flush: task_mode.is_ack(),
                                })
                                .await
                            {
                                let _ = task_context.events.send(RuntimeEvent::Error(format!(
                                    "failed to dispatch buffered mqtt payload for ingestor '{}' \
                                     in domain '{}': {}",
                                    task_context.ingestor.as_str(),
                                    task_context.domain.as_str(),
                                    error
                                )));
                            }
                            continue;
                        }
                        let next_flush = if task_mode.is_ack() {
                            None
                        } else {
                            ingest_collector.next_flush()
                        };
                        let publish = match Self::next_publish(
                            &mut eventloop,
                            &mut shutdown_rx,
                            &task_context,
                            next_flush,
                        )
                        .await
                        {
                            MqttNextPublish::Publish(publish) => *publish,
                            MqttNextPublish::Flush => {
                                let _ = Self::flush_collector(&task_context, &mut ingest_collector)
                                    .await;
                                continue;
                            }
                            MqttNextPublish::Shutdown => {
                                let _ = Self::flush_collector(&task_context, &mut ingest_collector)
                                    .await;
                                break 'outer;
                            }
                            MqttNextPublish::Reconnect => {
                                let _ = Self::flush_collector(&task_context, &mut ingest_collector)
                                    .await;
                                break;
                            }
                            MqttNextPublish::Suspend => {
                                let _ = Self::flush_collector(&task_context, &mut ingest_collector)
                                    .await;
                                break;
                            }
                        };

                        let Some(publish) = Self::apply_quiesce_to_publish(
                            &task_context,
                            &client_handle,
                            instance_idx,
                            task_mode.is_ack(),
                            publish,
                        )
                        .await
                        else {
                            continue;
                        };

                        match &task_mode {
                            MqttIngestMode::NoAckSequential { .. }
                            | MqttIngestMode::NoAckParallel { .. } => {
                                Self::handle_no_ack_publish(
                                    &task_context,
                                    publish,
                                    &mut ingest_collector,
                                )
                                .await;
                            }
                            MqttIngestMode::AckSequential { .. } => {
                                if !Self::handle_ack_sequential_publish(
                                    &task_context,
                                    &client_handle,
                                    &mut shutdown_rx,
                                    publish,
                                    task_ack_timeout.expect("ack timeout must exist"),
                                    task_retry_policy,
                                    &mut backoff,
                                )
                                .await
                                {
                                    break 'outer;
                                }
                            }
                            MqttIngestMode::AckParallel { max, .. } => {
                                let mut batch =
                                    match Self::decode_publish(&task_context, publish).await {
                                        Some(entry) => vec![entry],
                                        None => {
                                            if !backoff.wait(&mut shutdown_rx).await {
                                                break 'outer;
                                            }
                                            break;
                                        }
                                    };
                                let deadline = Instant::now()
                                    + task_batch_timeout.expect("batch timeout must exist");
                                while batch.len() < (*max as usize).max(1) {
                                    tokio::task::consume_budget().await;
                                    tokio::select! {
                                        _ = sleep_until(deadline) => break,
                                        next = Self::next_publish(&mut eventloop, &mut shutdown_rx, &task_context, None) => {
                                            match next {
                                                MqttNextPublish::Publish(publish) => {
                                                    let Some(publish) = Self::apply_quiesce_to_publish(
                                                        &task_context,
                                                        &client_handle,
                                                        instance_idx,
                                                        true,
                                                        *publish,
                                                    ).await else {
                                                        continue;
                                                    };
                                                    if let Some(entry) = Self::decode_publish(&task_context, publish).await {
                                                        batch.push(entry);
                                                    } else {
                                                        if !backoff.wait(&mut shutdown_rx).await {
                                                            break 'outer;
                                                        }
                                                        break;
                                                    }
                                                }
                                                MqttNextPublish::Flush => {}
                                                MqttNextPublish::Shutdown => break 'outer,
                                                MqttNextPublish::Reconnect => break,
                                                MqttNextPublish::Suspend => break,
                                            }
                                        }
                                    }
                                }
                                if !Self::handle_ack_parallel_batch(
                                    &task_context,
                                    &client_handle,
                                    &mut shutdown_rx,
                                    batch,
                                    task_ack_timeout.expect("ack timeout must exist"),
                                    task_retry_policy,
                                    &mut backoff,
                                )
                                .await
                                {
                                    break 'outer;
                                }
                            }
                        }
                    }
                    if task_context.quiesce.should_suspend_intake() {
                        continue;
                    }
                    if !backoff.wait(&mut shutdown_rx).await {
                        break;
                    }
                }

                info!(
                    domain = task_context.domain.as_str(),
                    ingestor = task_context.ingestor.as_str(),
                    instance = instance_idx,
                    "stopped mqtt ingestor"
                );
                task_context.runtime.mark_ingestor_instance_unready(
                    &task_context.domain,
                    &task_context.ingestor,
                    instance_idx,
                );
            });
            tasks.push(task);
        }

        runtime.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks,
            },
        );

        Ok(())
    }

    async fn establish_subscription(
        client: &AsyncClient,
        subscribe_filter: &str,
        qos: QoS,
        eventloop: &mut rumqttc::EventLoop,
        shutdown_rx: &mut watch::Receiver<bool>,
        context: &MqttTaskContext,
        instance_idx: u64,
    ) -> MqttSubscriptionState {
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return MqttSubscriptionState::Shutdown;
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                            if connack.session_present {
                                // A broker-only persistent-session resume cannot allocate a new
                                // SUBSCRIBE packet ID. The broker retained the existing
                                // subscription, so the successful CONNACK is the readiness
                                // boundary and queued QoS messages can be consumed immediately.
                                context.runtime.mark_ingestor_instance_ready(
                                    &context.domain,
                                    &context.ingestor,
                                    instance_idx,
                                );
                                return MqttSubscriptionState::Ready;
                            }
                            if let Err(error) = client.subscribe(subscribe_filter, qos).await {
                                context.runtime.record_ingestor_transient_error(
                                    &context.domain,
                                    &context.ingestor,
                                    format!("mqtt subscribe failed: {error}"),
                                );
                                warn!(
                                    domain = context.domain.as_str(),
                                    ingestor = context.ingestor.as_str(),
                                    error = %error,
                                    "failed to subscribe mqtt source"
                                );
                                return MqttSubscriptionState::Reconnect;
                            }
                        }
                        Ok(Event::Incoming(Incoming::SubAck(suback))) => {
                            if suback
                                .return_codes
                                .iter()
                                .all(|code| {
                                    let SubscribeReasonCode::Success(_) = code else {
                                        return false;
                                    };
                                    true
                                })
                            {
                                context.runtime.mark_ingestor_instance_ready(
                                    &context.domain,
                                    &context.ingestor,
                                    instance_idx,
                                );
                                return MqttSubscriptionState::Ready;
                            }
                            let error = format!("mqtt subscribe failed: {suback:?}");
                            let _ = context.events.send(RuntimeEvent::Error(format!(
                                "failed to subscribe mqtt source for ingestor '{}' in domain '{}': {}",
                                context.ingestor.as_str(),
                                context.domain.as_str(),
                                error
                            )));
                            warn!(
                                domain = context.domain.as_str(),
                                ingestor = context.ingestor.as_str(),
                                error = %error,
                                "failed to subscribe mqtt source"
                            );
                            context.runtime.record_ingestor_transient_error(
                                &context.domain,
                                &context.ingestor,
                                error,
                            );
                            return MqttSubscriptionState::Reconnect;
                        }
                        Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) | Ok(Event::Auth(_)) => {}
                        Err(error) => {
                            let _ = context.events.send(RuntimeEvent::Error(format!(
                                "failed to subscribe mqtt source for ingestor '{}' in domain '{}': {}",
                                context.ingestor.as_str(),
                                context.domain.as_str(),
                                error
                            )));
                            warn!(
                                domain = context.domain.as_str(),
                                ingestor = context.ingestor.as_str(),
                                error = %error,
                                "failed to subscribe mqtt source"
                            );
                            context.runtime.record_ingestor_transient_error(
                                &context.domain,
                                &context.ingestor,
                                format!("mqtt subscribe failed: {error}"),
                            );
                            return MqttSubscriptionState::Reconnect;
                        }
                    }
                }
            }
        }
    }

    async fn next_publish(
        eventloop: &mut rumqttc::EventLoop,
        shutdown_rx: &mut watch::Receiver<bool>,
        context: &MqttTaskContext,
        flush_at: Option<Instant>,
    ) -> MqttNextPublish {
        loop {
            tokio::task::consume_budget().await;
            if context.quiesce.should_suspend_intake() {
                return MqttNextPublish::Suspend;
            }
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return MqttNextPublish::Shutdown;
                    }
                }
                _ = sleep_until(
                    flush_at.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400)),
                ), if flush_at.is_some() => {
                    return MqttNextPublish::Flush;
                }
                _ = context.quiesce.wait_for_change() => {
                    if context.quiesce.should_suspend_intake() {
                        return MqttNextPublish::Suspend;
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Incoming::Publish(publish))) => {
                            return MqttNextPublish::Publish(Box::new(publish));
                        }
                        Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) | Ok(Event::Auth(_)) => {}
                        Err(error) => {
                            let _ = context.events.send(RuntimeEvent::Error(format!(
                                "failed to receive mqtt message for ingestor '{}' in domain '{}': {}",
                                context.ingestor.as_str(),
                                context.domain.as_str(),
                                error
                            )));
                            warn!(
                                domain = context.domain.as_str(),
                                ingestor = context.ingestor.as_str(),
                                error = %error,
                                "failed to receive mqtt message"
                            );
                            context.runtime.record_ingestor_transient_error(
                                &context.domain,
                                &context.ingestor,
                                format!("mqtt receive failed: {error}"),
                            );
                            return MqttNextPublish::Reconnect;
                        }
                    }
                }
            }
        }
    }

    async fn apply_quiesce_to_publish(
        context: &MqttTaskContext,
        client: &AsyncClient,
        instance_idx: u64,
        manual_ack: bool,
        publish: Publish,
    ) -> Option<Publish> {
        let payload = BufferedIngestPayload::new(
            publish.payload.as_ref(),
            BufferedIngestMetadata::without_headers(),
        );
        match context.quiesce.intake(instance_idx, payload, false) {
            IngestorQuiesceIntake::Dispatch(_) => Some(publish),
            IngestorQuiesceIntake::Buffered | IngestorQuiesceIntake::Dropped => {
                if manual_ack && let Err(error) = client.ack(&publish).await {
                    context.runtime.record_ingestor_transient_error(
                        &context.domain,
                        &context.ingestor,
                        format!("mqtt quiesce acknowledgement failed: {error}"),
                    );
                    let _ = context.events.send(RuntimeEvent::Error(format!(
                        "failed to acknowledge mqtt payload under quiesce for ingestor '{}' in \
                         domain '{}': {}",
                        context.ingestor.as_str(),
                        context.domain.as_str(),
                        error
                    )));
                }
                None
            }
            IngestorQuiesceIntake::Rejected { .. } => None,
        }
    }

    async fn handle_no_ack_publish(
        context: &MqttTaskContext,
        publish: Publish,
        collector: &mut IngestRouteCollector,
    ) {
        let Some(entry) = Self::decode_publish(context, publish).await else {
            return;
        };
        if let Err(error) =
            Self::dispatch_entry(context, entry.record, AckSet::empty(), collector).await
        {
            let _ = context.events.send(RuntimeEvent::Error(format!(
                "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                context.ingestor.as_str(),
                context.domain.as_str(),
                error
            )));
        } else if collector.len() >= INGEST_GROUP_MAX_ROWS {
            let _ = Self::flush_collector(context, collector).await;
        }
    }

    async fn handle_ack_sequential_publish(
        context: &MqttTaskContext,
        client_handle: &AsyncClient,
        shutdown_rx: &mut watch::Receiver<bool>,
        publish: Publish,
        ack_timeout: Duration,
        retry_policy: ParsedRetryPolicy,
        backoff: &mut RuntimeReconnectBackoff,
    ) -> bool {
        let Some(entry) = Self::decode_publish(context, publish).await else {
            return backoff.wait(shutdown_rx).await;
        };
        loop {
            tokio::task::consume_budget().await;
            let (acks, completion) = context
                .runtime
                .tracked_ingestor_ack_root(&context.domain, &context.ingestor);
            // One acknowledged message is one group.
            let mut collector = IngestRouteCollector::new(IngestMetadataKind::Headers, 1);
            let dispatch_result = Self::dispatch_entry(
                context,
                entry.record.clone(),
                if !context.branched_senders.is_empty() {
                    acks.attached()
                } else {
                    acks.clone()
                },
                &mut collector,
            )
            .await;
            let flush_result = Self::flush_collector(context, &mut collector).await;
            let dispatched = dispatch_result
                .and(flush_result)
                .map(|()| true)
                .unwrap_or_else(|error| {
                    let _ = context.events.send(RuntimeEvent::Error(format!(
                        "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                        context.ingestor.as_str(),
                        context.domain.as_str(),
                        error
                    )));
                    false
                });
            if dispatched {
                acks.ack_success();
                match Runtime::await_ack_completion(shutdown_rx, completion, ack_timeout).await {
                    Some(AckOutcome::Ack) => {
                        if let Err(error) = client_handle.ack(&entry.publish).await {
                            let _ = context.events.send(RuntimeEvent::Error(format!(
                                "failed to acknowledge mqtt message for ingestor '{}' in domain \
                                 '{}': {}",
                                context.ingestor.as_str(),
                                context.domain.as_str(),
                                error
                            )));
                            if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                                return false;
                            }
                        } else {
                            backoff.reset();
                            return true;
                        }
                    }
                    Some(AckOutcome::NoAck(error)) => {
                        let _ = context.events.send(RuntimeEvent::Error(format!(
                            "mqtt ack chain failed for ingestor '{}' in domain '{}': {}",
                            context.ingestor.as_str(),
                            context.domain.as_str(),
                            error
                        )));
                        if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                            return false;
                        }
                    }
                    None => return false,
                }
            } else {
                context.runtime.handle_general_error_for_acks(
                    &context.domain,
                    "ingestor",
                    &context.ingestor,
                    &context.error_policies,
                    std::iter::once(&acks),
                    "mqtt runtime dispatch failed".to_string(),
                );
                if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                    return false;
                }
            }
        }
    }

    async fn handle_ack_parallel_batch(
        context: &MqttTaskContext,
        client_handle: &AsyncClient,
        shutdown_rx: &mut watch::Receiver<bool>,
        batch: Vec<MqttBatchEntry>,
        ack_timeout: Duration,
        retry_policy: ParsedRetryPolicy,
        backoff: &mut RuntimeReconnectBackoff,
    ) -> bool {
        let mut publishes = Vec::with_capacity(batch.len());
        let mut initial_records = Vec::with_capacity(batch.len());
        for entry in batch {
            publishes.push(entry.publish);
            initial_records.push(entry.record);
        }
        let mut initial_records = Some(initial_records);
        'retry: loop {
            tokio::task::consume_budget().await;
            let records = if let Some(records) = initial_records.take() {
                records
            } else {
                let mut records = Vec::with_capacity(publishes.len());
                for publish in &publishes {
                    tokio::task::consume_budget().await;
                    let Some(record) = Self::decode_publish_record(context, publish).await else {
                        if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                            return false;
                        }
                        continue 'retry;
                    };
                    records.push(record);
                }
                records
            };
            let mut completions = Vec::with_capacity(records.len());
            let mut batch_failure = None::<String>;
            // The poll group is one ingest group.
            let mut collector =
                IngestRouteCollector::new(IngestMetadataKind::Headers, records.len());

            for record in records {
                tokio::task::consume_budget().await;
                let (acks, completion) = context
                    .runtime
                    .tracked_ingestor_ack_root(&context.domain, &context.ingestor);
                let dispatched = Self::dispatch_entry(
                    context,
                    record,
                    if !context.branched_senders.is_empty() {
                        acks.attached()
                    } else {
                        acks.clone()
                    },
                    &mut collector,
                )
                .await
                .map(|()| true)
                .unwrap_or_else(|error| {
                    let _ = context.events.send(RuntimeEvent::Error(format!(
                        "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                        context.ingestor.as_str(),
                        context.domain.as_str(),
                        error
                    )));
                    false
                });
                if dispatched {
                    acks.ack_success();
                    completions.push(completion);
                } else {
                    context.runtime.handle_general_error_for_acks(
                        &context.domain,
                        "ingestor",
                        &context.ingestor,
                        &context.error_policies,
                        std::iter::once(&acks),
                        "mqtt runtime dispatch failed".to_string(),
                    );
                    batch_failure = Some("mqtt runtime dispatch failed".to_string());
                    break;
                }
            }

            if let Err(error) = Self::flush_collector(context, &mut collector).await {
                batch_failure = Some(error);
            }

            if batch_failure.is_none() {
                for completion in completions {
                    tokio::task::consume_budget().await;
                    match Runtime::await_ack_completion(shutdown_rx, completion, ack_timeout).await
                    {
                        Some(AckOutcome::Ack) => {}
                        Some(AckOutcome::NoAck(error)) => {
                            batch_failure = Some(error);
                            break;
                        }
                        None => return false,
                    }
                }
            }

            if let Some(error) = batch_failure {
                let _ = context.events.send(RuntimeEvent::Error(format!(
                    "mqtt ack batch failed for ingestor '{}' in domain '{}': {}",
                    context.ingestor.as_str(),
                    context.domain.as_str(),
                    error
                )));
                if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                    return false;
                }
            } else {
                let mut ack_failure = None::<String>;
                for publish in &publishes {
                    if let Err(error) = client_handle.ack(publish).await {
                        ack_failure = Some(error.to_string());
                        let _ = context.events.send(RuntimeEvent::Error(format!(
                            "failed to acknowledge mqtt message for ingestor '{}' in domain '{}': \
                             {}",
                            context.ingestor.as_str(),
                            context.domain.as_str(),
                            error
                        )));
                        break;
                    }
                }
                if ack_failure.is_none() {
                    backoff.reset();
                    return true;
                }
                if !Self::wait_retry(shutdown_rx, retry_policy, backoff).await {
                    return false;
                }
            }
        }
    }

    async fn wait_retry(
        shutdown_rx: &mut watch::Receiver<bool>,
        retry_policy: ParsedRetryPolicy,
        backoff: &mut RuntimeReconnectBackoff,
    ) -> bool {
        let delay = backoff.next_delay().max(retry_policy.backoff);
        let next = next_retry_delay(delay, retry_policy);
        tokio::select! {
            changed = shutdown_rx.changed() => !(changed.is_err() || *shutdown_rx.borrow()),
            _ = sleep(delay) => {
                backoff.next = next;
                true
            }
        }
    }

    async fn decode_publish(context: &MqttTaskContext, publish: Publish) -> Option<MqttBatchEntry> {
        let record = Self::decode_publish_record(context, &publish).await?;
        Some(MqttBatchEntry { publish, record })
    }

    async fn decode_publish_record(
        context: &MqttTaskContext,
        publish: &Publish,
    ) -> Option<RuntimeRecordBatch> {
        let key = publish.topic.clone();
        let payload = publish.payload.as_ref();

        trace!(
            domain = context.domain.as_str(),
            ingestor = context.ingestor.as_str(),
            topic = %String::from_utf8_lossy(publish.topic.as_ref()),
            key = ?key,
            payload = String::from_utf8_lossy(payload).to_string(),
            "received mqtt message"
        );

        match decode_ingested_payload(context.codec.clone(), payload).await {
            Ok(record) => Some(record),
            Err(error) => {
                let _ = context.events.send(RuntimeEvent::Error(format!(
                    "failed to decode message for ingestor '{}' in domain '{}': {}",
                    context.ingestor.as_str(),
                    context.domain.as_str(),
                    error
                )));
                warn!(
                    domain = context.domain.as_str(),
                    ingestor = context.ingestor.as_str(),
                    error = %error,
                    "failed to decode mqtt message"
                );
                None
            }
        }
    }

    async fn dispatch_entry(
        context: &MqttTaskContext,
        record: RuntimeRecordBatch,
        acks: AckSet,
        collector: &mut IngestRouteCollector,
    ) -> Result<(), String> {
        context
            .runtime
            .dispatch_ingested_records(IngestGroupDispatch {
                collector,
                domain: &context.domain,
                ingestor: &context.ingestor,
                timestamp_source: context.timestamp_source.as_ref(),
                output_routes: &context.output_routes,
                filter_where: context.filter_where.as_ref(),
                records: vec![record],
                metadata: &[IngestMetadataRow::Headers {
                    headers: &NoIngestHeaders,
                }],
                ingested_at: current_timestamp(),
                acks: vec![acks],
            })
            .await
    }

    async fn flush_collector(
        context: &MqttTaskContext,
        collector: &mut IngestRouteCollector,
    ) -> Result<(), String> {
        let result = context
            .runtime
            .flush_ingest_collector(
                &context.domain,
                &context.ingestor,
                &context.branched_senders,
                collector,
            )
            .await;
        if let Err(error) = &result {
            let _ = context.events.send(RuntimeEvent::Error(format!(
                "failed to flush messages for ingestor '{}' in domain '{}': {}",
                context.ingestor.as_str(),
                context.domain.as_str(),
                error
            )));
        }
        result
    }

    fn qos(qos: MqttQos) -> QoS {
        match qos {
            MqttQos::AtMostOnce => QoS::AtMostOnce,
            MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        }
    }

    fn subscribe_filter(topic: &str, domain: &Domain, ingestor: &Identifier) -> String {
        format!("$share/{}~{}/{topic}", domain.as_str(), ingestor.as_str())
    }

    #[cfg(test)]
    pub(in crate::runtime) fn client_from_client(
        client: &CreateClientMqtt,
        default_client_id: &str,
    ) -> Result<(AsyncClient, rumqttc::EventLoop), String> {
        Self::client_from_config(
            &client.config,
            default_client_id,
            &MqttClientSettings {
                session: MqttSession::Clean,
                manual_acks: false,
            },
        )
    }

    fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
        default_client_id: &str,
        settings: &MqttClientSettings,
    ) -> Result<(AsyncClient, rumqttc::EventLoop), String> {
        let addr = client_config_value(config, "addr", || {
            "missing MQTT client config key 'addr'".to_string()
        })?;
        let client_id = optional_client_config_value(config, "client_id")
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| default_client_id.to_string());

        let mqtt_addr = Self::parse_addr(&addr)?;
        let mut options = MqttOptions::new(client_id, (mqtt_addr.host, mqtt_addr.port));
        options.set_session_mode(if settings.session == MqttSession::Clean {
            SessionMode::Clean
        } else {
            SessionMode::Persistent
        });
        if settings.session == MqttSession::Persistent {
            // Nervix deliberately keeps in-flight messages and ACK state in memory. After an
            // owner change, accept the broker's retained session and let it redeliver pending
            // QoS messages instead of requiring a local rumqttc protocol checkpoint.
            options.set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly);
        }
        options.set_ack_mode(if settings.manual_acks {
            AckMode::Manual
        } else {
            AckMode::Automatic
        });
        if mqtt_addr.tls {
            let tls = client_tls_paths(config);
            let ca = if let Some(ca_file) = tls.ca_file.as_ref() {
                read_tls_file(ca_file, "TLS CA certificate")?
            } else {
                return Err("MQTT TLS requires client config key 'tls_ca_file'".to_string());
            };
            let client_auth =
                match (&tls.cert_file, &tls.key_file) {
                    (Some(cert_file), Some(key_file)) => Some((
                        read_tls_file(cert_file, "TLS certificate")?,
                        read_tls_file(key_file, "TLS private key")?,
                    )),
                    (None, None) => None,
                    _ => {
                        return Err("MQTT TLS client authentication requires both \
                                    'tls_cert_file' and 'tls_key_file'"
                            .to_string());
                    }
                };
            options.set_transport(MqttTransport::Tls(TlsConfiguration::Simple {
                ca,
                alpn: None,
                client_auth,
            }));
        }
        AsyncClient::builder(options)
            .capacity(1024)
            .try_build()
            .map_err(|error| format!("invalid MQTT client config: {error}"))
    }

    fn client_id_template(
        config: &[nervix_models::ClientConfigEntry],
        default_client_id: &str,
        instances: u64,
    ) -> Result<String, String> {
        let configured = optional_client_config_value(config, "client_id");
        if instances <= 1 {
            return Ok(configured
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| default_client_id.to_string()));
        }
        let Some(client_id) = configured else {
            return Err(format!(
                "MQTT client_id is required for multi-instance MQTT ingestors; use \
                 {MQTT_INSTANCE_PLACEHOLDER} in client_id"
            ));
        };
        if !client_id.contains(MQTT_INSTANCE_PLACEHOLDER) {
            return Err(format!(
                "MQTT client_id '{client_id}' is shared by {instances} instances; use \
                 {MQTT_INSTANCE_PLACEHOLDER} in client_id for multi-instance MQTT ingestors"
            ));
        }
        Ok(client_id.to_string())
    }

    pub(in crate::runtime) fn parse_addr(addr: &str) -> Result<MqttIngestorAddr, String> {
        let url = Url::parse(addr).map_err(|_| format!("invalid MQTT addr '{addr}'"))?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(format!(
                "unsupported MQTT addr scheme '{}', expected mqtt:// or mqtts://",
                url.scheme()
            ));
        };
        let host = url
            .host()
            .map(|host| match host {
                Host::Domain(domain) => domain.to_string(),
                Host::Ipv4(addr) => addr.to_string(),
                Host::Ipv6(addr) => addr.to_string(),
            })
            .filter(|host| !host.is_empty())
            .ok_or_else(|| format!("missing host in MQTT addr '{addr}'"))?;
        let port = url
            .port()
            .ok_or_else(|| format!("missing port in MQTT addr '{addr}'"))?;
        Ok(MqttIngestorAddr { host, port, tls })
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClientConfigEntry, MqttSession};
    use rumqttc::BrokerSessionResumePolicy;

    use super::{MQTT_INSTANCE_PLACEHOLDER, MqttClientSettings, MqttIngestor};

    fn config_with_client_id(client_id: &str) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "client_id".to_string(),
            value: client_id.to_string(),
        }]
    }

    #[test]
    fn persistent_sessions_allow_broker_only_resume() {
        let config = vec![
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "mqtt://127.0.0.1:1883".to_string(),
            },
            ClientConfigEntry {
                key: "client_id".to_string(),
                value: "persistent-client".to_string(),
            },
        ];
        let (_, eventloop) = MqttIngestor::client_from_config(
            &config,
            "fallback",
            &MqttClientSettings {
                session: MqttSession::Persistent,
                manual_acks: true,
            },
        )
        .expect("persistent MQTT client must be valid");

        assert_eq!(
            eventloop.options.broker_session_resume_policy(),
            BrokerSessionResumePolicy::AllowBrokerOnly
        );
    }

    #[test]
    fn multi_instance_mqtt_client_id_requires_instance_template() {
        let error =
            MqttIngestor::client_id_template(&config_with_client_id("fixed-client"), "fallback", 2)
                .expect_err("fixed multi-instance client_id must be rejected");

        assert_eq!(
            error,
            "MQTT client_id 'fixed-client' is shared by 2 instances; use {{instance}} in \
             client_id for multi-instance MQTT ingestors"
        );
    }

    #[test]
    fn multi_instance_mqtt_client_id_template_is_preserved_for_rendering() {
        let template = MqttIngestor::client_id_template(
            &config_with_client_id("templated-{{instance}}"),
            "fallback",
            2,
        )
        .expect("templated multi-instance client_id must be accepted");

        assert_eq!(
            template.replace(MQTT_INSTANCE_PLACEHOLDER, "1"),
            "templated-1"
        );
    }

    #[test]
    fn single_instance_mqtt_client_id_uses_default_when_omitted() {
        let template = MqttIngestor::client_id_template(&[], "fallback", 1)
            .expect("single-instance default client_id must be accepted");

        assert_eq!(template, "fallback");
    }
}
