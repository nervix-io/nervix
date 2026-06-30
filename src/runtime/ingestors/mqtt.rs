use rumqttc::{
    AsyncClient, Event, Incoming, MqttOptions, Publish, QoS, SubscribeReasonCode, TlsConfiguration,
    Transport as MqttTransport,
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
    parameterization: Vec<Identifier>,
    parameter_value_mappings: Vec<ParameterValueMapping>,
    parameterized_senders: HashMap<Identifier, mpsc::Sender<ParameterizedEntrypointInput>>,
    events: broadcast::Sender<RuntimeEvent>,
}

#[derive(Clone)]
struct MqttClientSettings {
    session: MqttSession,
    manual_acks: bool,
}

struct MqttBatchEntry {
    publish: Publish,
    record: DecodedRecord,
}

enum MqttNextPublish {
    Publish(Publish),
    Shutdown,
    Reconnect,
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
            .resolve_client_config_with_instance(client.mount.as_ref(), &client.config, 0)
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
                    parameterized: Vec::new(),
                    tasks: vec![task],
                },
            );
            return Ok(());
        }
        let parameterized_runtime = runtime.start_parameterized_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.parameterized_templates,
        );
        let parameterized_senders = parameterized_runtime.senders.clone();
        let output_routes = dependencies.output_routes;
        let filter_where = dependencies.filter_where;
        let codec = dependencies.codec;
        let parameterization = dependencies.parameterization;

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
                error_policies: ingestor.error_policies.clone(),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: output_routes.clone(),
                filter_where: filter_where.clone(),
                codec: codec.clone(),
                parameterization: parameterization.clone(),
                parameter_value_mappings: dependencies.parameter_value_mappings.clone(),
                parameterized_senders: parameterized_senders.clone(),
                events: runtime.events.clone(),
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
                    let resolved_client =
                        match task_context.runtime.resolve_client_config_with_instance(
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
                    if let Err(error) = client_handle
                        .subscribe(task_subscribe_filter.as_str(), qos)
                        .await
                    {
                        task_context
                            .runtime
                            .record_ingestor_transient_error_with_backoff(
                                &task_context.domain,
                                &task_context.ingestor,
                                format!("mqtt subscribe failed: {error}"),
                                backoff.next_delay(),
                            );
                        warn!(
                            domain = task_context.domain.as_str(),
                            ingestor = task_context.ingestor.as_str(),
                            error = %error,
                            "failed to subscribe mqtt source"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }

                    match Self::wait_for_subscription_ack(
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
                        let publish = match Self::next_publish(
                            &mut eventloop,
                            &mut shutdown_rx,
                            &task_context,
                        )
                        .await
                        {
                            MqttNextPublish::Publish(publish) => publish,
                            MqttNextPublish::Shutdown => break 'outer,
                            MqttNextPublish::Reconnect => break,
                        };

                        match &task_mode {
                            MqttIngestMode::NoAckSequential { .. }
                            | MqttIngestMode::NoAckParallel { .. } => {
                                Self::handle_no_ack_publish(&task_context, publish).await;
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
                                        next = Self::next_publish(&mut eventloop, &mut shutdown_rx, &task_context) => {
                                            match next {
                                                MqttNextPublish::Publish(publish) => {
                                                    if let Some(entry) = Self::decode_publish(&task_context, publish).await {
                                                        batch.push(entry);
                                                    } else {
                                                        if !backoff.wait(&mut shutdown_rx).await {
                                                            break 'outer;
                                                        }
                                                        break;
                                                    }
                                                }
                                                MqttNextPublish::Shutdown => break 'outer,
                                                MqttNextPublish::Reconnect => break,
                                            }
                                        }
                                    }
                                }
                                if !Self::handle_ack_parallel_batch(
                                    &task_context,
                                    &client_handle,
                                    &mut shutdown_rx,
                                    &batch,
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
                parameterized: parameterized_runtime.runtimes,
                tasks,
            },
        );

        Ok(())
    }

    async fn wait_for_subscription_ack(
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
                        Ok(Event::Incoming(Incoming::SubAck(suback))) => {
                            if suback
                                .return_codes
                                .iter()
                                .all(|code| *code != SubscribeReasonCode::Failure)
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
                        Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {}
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
    ) -> MqttNextPublish {
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return MqttNextPublish::Shutdown;
                    }
                }
                event = eventloop.poll() => {
                    match event {
                        Ok(Event::Incoming(Incoming::Publish(publish))) => {
                            return MqttNextPublish::Publish(publish);
                        }
                        Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) => {}
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

    async fn handle_no_ack_publish(context: &MqttTaskContext, publish: Publish) {
        let Some(entry) = Self::decode_publish(context, publish).await else {
            return;
        };
        if let Err(error) = Self::dispatch_entry(context, entry.record, AckSet::empty()).await {
            let _ = context.events.send(RuntimeEvent::Error(format!(
                "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                context.ingestor.as_str(),
                context.domain.as_str(),
                error
            )));
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
            let (acks, completion) = AckSet::root();
            let dispatched = Self::dispatch_entry(
                context,
                entry.record.clone(),
                if !context.parameterized_senders.is_empty() {
                    acks.attached()
                } else {
                    acks.clone()
                },
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
        batch: &[MqttBatchEntry],
        ack_timeout: Duration,
        retry_policy: ParsedRetryPolicy,
        backoff: &mut RuntimeReconnectBackoff,
    ) -> bool {
        loop {
            tokio::task::consume_budget().await;
            let mut completions = Vec::with_capacity(batch.len());
            let mut batch_failure = None::<String>;

            for entry in batch {
                tokio::task::consume_budget().await;
                let (acks, completion) = AckSet::root();
                let dispatched = Self::dispatch_entry(
                    context,
                    entry.record.clone(),
                    if !context.parameterized_senders.is_empty() {
                        acks.attached()
                    } else {
                        acks.clone()
                    },
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
                for entry in batch {
                    if let Err(error) = client_handle.ack(&entry.publish).await {
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
        let key = publish.topic.clone();
        let payload = publish.payload.as_ref();

        trace!(
            domain = context.domain.as_str(),
            ingestor = context.ingestor.as_str(),
            topic = publish.topic,
            key = key,
            payload = String::from_utf8_lossy(payload).to_string(),
            "received mqtt message"
        );

        match decode_ingested_payload(context.codec.clone(), payload).await {
            Ok(record) => Some(MqttBatchEntry { publish, record }),
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
        record: DecodedRecord,
        acks: AckSet,
    ) -> Result<(), String> {
        let mut output_routes = context.output_routes.clone();
        context
            .runtime
            .dispatch_ingested_record(IngestDispatch {
                domain: &context.domain,
                ingestor: &context.ingestor,
                timestamp_source: context.timestamp_source.as_ref(),
                parameterization: &context.parameterization,
                parameter_value_mappings: Some(&context.parameter_value_mappings),
                output_routes: &mut output_routes,
                filter_where: context.filter_where.as_ref(),
                parameterized_senders: &context.parameterized_senders,
                record,
                filter_map_metadata: None,
                ingested_at: current_timestamp(),
                acks,
            })
            .await
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
        let mut options = MqttOptions::new(client_id, mqtt_addr.host, mqtt_addr.port);
        options.set_clean_session(settings.session == MqttSession::Clean);
        options.set_manual_acks(settings.manual_acks);
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
        Ok(AsyncClient::new(options, 1024))
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
    use nervix_models::ClientConfigEntry;

    use super::{MQTT_INSTANCE_PLACEHOLDER, MqttIngestor};

    fn config_with_client_id(client_id: &str) -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "client_id".to_string(),
            value: client_id.to_string(),
        }]
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
