use pulsar::{
    Consumer as PulsarConsumer, ConsumerOptions as PulsarConsumerOptions, Pulsar,
    SubType as PulsarSubType, TlsOptions as PulsarTlsOptions, TokioExecutor,
    consumer::{InitialPosition as PulsarInitialPosition, Message as PulsarMessage},
};

use super::super::*;

pub(in crate::runtime) struct PulsarIngestor;

impl PulsarIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientPulsar,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (topic, subscription, instances, ack_mode) = match &ingestor.source {
            IngestSource::Pulsar {
                topic,
                subscription,
                instances,
                mode,
                ..
            } => (
                topic.clone(),
                subscription.clone(),
                *instances,
                mode.clone(),
            ),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected pulsar ingestor source".to_string(),
                });
            }
        };
        let ack_timeout = match &ack_mode {
            PulsarIngestMode::AckParallel { timeout, .. }
            | PulsarIngestMode::AckSequential { timeout, .. } => {
                Some(Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?)
            }
            PulsarIngestMode::NoAckParallel { .. } => None,
        };
        let retry_policy = match &ack_mode {
            PulsarIngestMode::AckParallel { retry_policy, .. }
            | PulsarIngestMode::AckSequential { retry_policy, .. } => Some(
                Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            ),
            PulsarIngestMode::NoAckParallel { .. } => None,
        };
        let batch_timeout = match &ack_mode {
            PulsarIngestMode::AckParallel { batch_timeout, .. } => {
                Some(Runtime::parse_duration_setting(
                    domain,
                    &ingestor.name,
                    "batch timeout",
                    batch_timeout,
                )?)
            }
            _ => None,
        };
        let (sender_relay, filter_map, codec, parameterization, parameterized_template) =
            runtime.ingestor_dependencies(domain, &ingestor).await?;
        let parameterized_runtime = runtime.start_parameterized_ingestor_runtime(
            domain,
            &ingestor.name,
            parameterized_template,
        );
        let resolved_client = runtime
            .resolve_client_config(client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let pulsar = Self::client_from_config(&resolved_client.entries)
            .await
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let topic_name = Self::topic_from_config(&resolved_client.entries, topic.as_str());

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances as usize);

        for instance_idx in 0..instances {
            let consumer_name = format!("{}-{instance_idx}", ingestor.name.as_str());
            let mut consumer: PulsarConsumer<Vec<u8>, TokioExecutor> = pulsar
                .consumer()
                .with_topic(topic_name.as_str())
                .with_consumer_name(consumer_name.clone())
                .with_subscription(subscription.as_str())
                .with_subscription_type(PulsarSubType::Shared)
                .with_options(
                    PulsarConsumerOptions::default()
                        .with_initial_position(PulsarInitialPosition::Earliest),
                )
                .build()
                .await
                .map_err(|source| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: source.to_string(),
                })?;

            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_topic = topic_name.clone();
            let task_subscription = subscription.clone();
            let task_events = runtime.events.clone();
            let task_sender = sender_relay.clone();
            let task_filter_map = filter_map.clone();
            let task_codec = codec.clone();
            let task_parameterization = parameterization.clone();
            let task_parameter_value_mappings = ingestor.parameterized_by.values().to_vec();
            let task_parameterized_sender = parameterized_runtime
                .as_ref()
                .map(|runtime| runtime.sender());
            let task_ack_mode = ack_mode.clone();
            let task_ack_timeout = ack_timeout;
            let task_retry_policy = retry_policy.unwrap_or(ParsedRetryPolicy {
                backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            });
            let task_batch_timeout = batch_timeout;
            let task_client_mounts = resolved_client.mounts.clone();
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;

                struct PulsarBatchEntry {
                    message: PulsarMessage<Vec<u8>>,
                    record: DecodedRecord,
                    filter_map_metadata: IngestFilterMapMetadata,
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    topic = task_topic.as_str(),
                    subscription = task_subscription.as_str(),
                    instance = instance_idx,
                    "started pulsar ingestor"
                );

                let ack_parallel_limit = match &task_ack_mode {
                    PulsarIngestMode::AckParallel { max, .. }
                    | PulsarIngestMode::NoAckParallel { max } => (*max).max(1) as usize,
                    PulsarIngestMode::AckSequential { .. } => 1,
                };
                let ack_timeout = task_ack_timeout;
                let retry_policy = task_retry_policy;
                let batch_timeout = task_batch_timeout;
                let mut retry_delay = retry_policy.backoff;

                'ingest: loop {
                    tokio::task::consume_budget().await;
                    if task_runtime
                        .wait_if_ingestor_faulted(&task_domain, &task_ingestor, &mut shutdown_rx)
                        .await
                    {
                        break;
                    }
                    if task_runtime.ingestor_faults.is_failed(&task_ingestor) {
                        continue;
                    }
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        message = consumer.next() => {
                            match message {
                                Some(Ok(message)) => {
                                    task_runtime
                                        .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                    match &task_ack_mode {
                                        PulsarIngestMode::NoAckParallel { .. } => {
                                            match Self::decode_message(
                                                task_codec.clone(),
                                                &task_domain,
                                                &task_ingestor,
                                                &message,
                                            )
                                            .await
                                            {
                                                Ok(record) => {
                                                    let filter_map_metadata =
                                                        IngestFilterMapMetadata::from_headers(
                                                            Self::headers_from_message(&message),
                                                        );
                                                    let entry = PulsarBatchEntry {
                                                        message,
                                                        record,
                                                        filter_map_metadata,
                                                    };
                                                    if let Err(error) = task_runtime
                                                        .dispatch_ingested_record(IngestDispatch {
                                                            domain: &task_domain,
                                                            ingestor: &task_ingestor,
                                                            timestamp_source: task_timestamp_source
                                                                .as_ref(),
                                                            parameterization:
                                                                &task_parameterization,
                                                            parameter_value_mappings: Some(&task_parameter_value_mappings),
                                                            sender_relay: &task_sender,
                                                            filter_map: task_filter_map.as_ref(),
                                                            parameterized_sender:
                                                                task_parameterized_sender.as_ref(),
                                                            record: entry.record,
                                                            filter_map_metadata: Some(
                                                                entry.filter_map_metadata.clone(),
                                                            ),
                                                            ingested_at: current_timestamp(),
                                                            acks: AckSet::empty(),
                                                        })
                                                        .await
                                                    {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        )));
                                                        if let Err(nack_error) = consumer.nack(&entry.message).await {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                nack_error
                                                            )));
                                                        }
                                                        sleep(retry_delay).await;
                                                        retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                    } else if let Err(error) = consumer.ack(&entry.message).await {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to ack pulsar message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        )));
                                                        sleep(retry_delay).await;
                                                        retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                    } else {
                                                        retry_delay = retry_policy.backoff;
                                                    }
                                                }
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    if let Err(nack_error) = consumer.nack(&message).await {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            nack_error
                                                        )));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                        PulsarIngestMode::AckSequential { .. } => {
                                            let entry = match Self::decode_message(
                                                task_codec.clone(),
                                                &task_domain,
                                                &task_ingestor,
                                                &message,
                                            )
                                            .await
                                            {
                                                Ok(record) => {
                                                    let filter_map_metadata =
                                                        IngestFilterMapMetadata::from_headers(
                                                            Self::headers_from_message(&message),
                                                        );
                                                    PulsarBatchEntry {
                                                        message,
                                                        record,
                                                        filter_map_metadata,
                                                    }
                                                }
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    if let Err(nack_error) = consumer.nack(&message).await {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            nack_error
                                                        )));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                    continue 'ingest;
                                                }
                                            };

                                            loop {
                                                tokio::task::consume_budget().await;
                                                let (acks, completion) = AckSet::root();
                                                let dispatched = task_runtime
                                                    .dispatch_ingested_record(IngestDispatch {
                                                        domain: &task_domain,
                                                        ingestor: &task_ingestor,
                                                        timestamp_source: task_timestamp_source
                                                            .as_ref(),
                                                        parameterization:
                                                            &task_parameterization,
                                                        parameter_value_mappings: Some(&task_parameter_value_mappings),
                                                        sender_relay: &task_sender,
                                                        filter_map: task_filter_map.as_ref(),
                                                        parameterized_sender:
                                                            task_parameterized_sender.as_ref(),
                                                        record: entry.record.clone(),
                                                        filter_map_metadata: Some(
                                                            entry.filter_map_metadata.clone(),
                                                        ),
                                                        ingested_at: current_timestamp(),
                                                        acks: if task_parameterized_sender.is_some() {
                                                            acks.attached()
                                                        } else {
                                                            acks.clone()
                                                        },
                                                    })
                                                    .await
                                                    .map(|()| true)
                                                    .unwrap_or_else(|error| {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        )));
                                                        false
                                                    });
                                                if dispatched {
                                                    acks.ack_success();
                                                    match Runtime::await_ack_completion(
                                                        &mut shutdown_rx,
                                                        completion,
                                                        ack_timeout.expect("ack timeout must exist"),
                                                    ).await {
                                                        Some(AckOutcome::Ack) => {
                                                            if let Err(error) = consumer.ack(&entry.message).await {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to ack pulsar message for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                )));
                                                                sleep(retry_delay).await;
                                                                retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                            } else {
                                                                retry_delay = retry_policy.backoff;
                                                                break;
                                                            }
                                                        }
                                                        Some(AckOutcome::NoAck(error)) => {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "pulsar ack chain failed for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            )));
                                                            if let Err(nack_error) = consumer.nack(&entry.message).await {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    nack_error
                                                                )));
                                                            }
                                                            sleep(retry_delay).await;
                                                            retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                        }
                                                        None => break 'ingest,
                                                    }
                                                } else {
                                                    if let Err(nack_error) = consumer.nack(&entry.message).await {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            nack_error
                                                        )));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                        PulsarIngestMode::AckParallel { .. } => {
                                            let mut batch = Vec::with_capacity(ack_parallel_limit);
                                            let first = match Self::decode_message(
                                                task_codec.clone(),
                                                &task_domain,
                                                &task_ingestor,
                                                &message,
                                            )
                                            .await
                                            {
                                                Ok(record) => {
                                                    let filter_map_metadata =
                                                        IngestFilterMapMetadata::from_headers(
                                                            Self::headers_from_message(&message),
                                                        );
                                                    PulsarBatchEntry {
                                                        message,
                                                        record,
                                                        filter_map_metadata,
                                                    }
                                                }
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    if let Err(nack_error) = consumer.nack(&message).await {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            nack_error
                                                        )));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                    continue 'ingest;
                                                }
                                            };
                                            batch.push(first);
                                            let batch_deadline =
                                                Instant::now() + batch_timeout.expect("batch timeout must exist");

                                            while batch.len() < ack_parallel_limit {
                                                tokio::task::consume_budget().await;
                                                tokio::select! {
                                                    changed = shutdown_rx.changed() => {
                                                        if changed.is_err() || *shutdown_rx.borrow() {
                                                            break 'ingest;
                                                        }
                                                    }
                                                    _ = sleep_until(batch_deadline) => break,
                                                    next = consumer.next() => {
                                                        match next {
                                                            Some(Ok(next_message)) => {
                                                                match Self::decode_message(
                                                                    task_codec.clone(),
                                                                    &task_domain,
                                                                    &task_ingestor,
                                                                    &next_message,
                                                                )
                                                                .await
                                                                {
                                                                    Ok(record) => {
                                                                        let filter_map_metadata =
                                                                            IngestFilterMapMetadata::from_headers(
                                                                                Self::headers_from_message(
                                                                                    &next_message,
                                                                                ),
                                                                            );
                                                                        batch.push(PulsarBatchEntry {
                                                                            message: next_message,
                                                                            record,
                                                                            filter_map_metadata,
                                                                        });
                                                                    }
                                                                    Err(error) => {
                                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                            "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                                            task_ingestor.as_str(),
                                                                            task_domain.as_str(),
                                                                            error
                                                                        )));
                                                                        if let Err(nack_error) = consumer.nack(&next_message).await {
                                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                                "failed to nack pulsar message for ingestor '{}' in domain '{}': {}",
                                                                                task_ingestor.as_str(),
                                                                                task_domain.as_str(),
                                                                                nack_error
                                                                            )));
                                                                        }
                                                                        sleep(retry_delay).await;
                                                                        retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                                        continue 'ingest;
                                                                    }
                                                                }
                                                            }
                                                            Some(Err(error)) => {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to receive pulsar message for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                )));
                                                            }
                                                            None => break,
                                                        }
                                                    }
                                                }
                                            }

                                            loop {
                                                tokio::task::consume_budget().await;
                                                let mut completions = Vec::with_capacity(batch.len());
                                                let mut batch_failure = None::<String>;

                                                for entry in &batch {
                                                    tokio::task::consume_budget().await;
                                                    let (acks, completion) = AckSet::root();
                                                    let dispatched = task_runtime
                                                        .dispatch_ingested_record(IngestDispatch {
                                                            domain: &task_domain,
                                                            ingestor: &task_ingestor,
                                                            timestamp_source: task_timestamp_source
                                                                .as_ref(),
                                                            parameterization:
                                                                &task_parameterization,
                                                            parameter_value_mappings: Some(&task_parameter_value_mappings),
                                                            sender_relay: &task_sender,
                                                            filter_map: task_filter_map.as_ref(),
                                                            parameterized_sender:
                                                                task_parameterized_sender.as_ref(),
                                                            record: entry.record.clone(),
                                                            filter_map_metadata: Some(
                                                                entry.filter_map_metadata.clone(),
                                                            ),
                                                            ingested_at: current_timestamp(),
                                                            acks: if task_parameterized_sender
                                                                .is_some()
                                                            {
                                                                acks.attached()
                                                            } else {
                                                                acks.clone()
                                                            },
                                                        })
                                                        .await
                                                        .map(|()| true)
                                                        .unwrap_or_else(|error| {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            )));
                                                            false
                                                        });
                                                    if dispatched {
                                                        acks.ack_success();
                                                        completions.push(completion);
                                                    } else {
                                                        batch_failure =
                                                            Some("pulsar runtime dispatch failed".to_string());
                                                        break;
                                                    }
                                                }

                                                if batch_failure.is_none() {
                                                    for completion in completions {
                                                        tokio::task::consume_budget().await;
                                                        match Runtime::await_ack_completion(
                                                            &mut shutdown_rx,
                                                            completion,
                                                            ack_timeout.expect("ack timeout must exist"),
                                                        ).await {
                                                            Some(AckOutcome::Ack) => {}
                                                            Some(AckOutcome::NoAck(error)) => {
                                                                batch_failure = Some(error);
                                                                break;
                                                            }
                                                            None => break 'ingest,
                                                        }
                                                    }
                                                }

                                                if let Some(error) = batch_failure {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "pulsar ack batch failed for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    for entry in &batch {
                                                        if let Err(nack_error) = consumer.nack(&entry.message).await {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to nack pulsar batch message for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                nack_error
                                                            )));
                                                        }
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                } else {
                                                    retry_delay = retry_policy.backoff;
                                                    let mut ack_failure = None::<String>;
                                                    for entry in &batch {
                                                        if let Err(error) = consumer.ack(&entry.message).await {
                                                            ack_failure = Some(error.to_string());
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to ack pulsar message for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            )));
                                                            break;
                                                        }
                                                    }
                                                    if ack_failure.is_none() {
                                                        break;
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Err(error)) => {
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        format!("pulsar receive failed: {error}"),
                                    );
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to receive pulsar message for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        error = %error,
                                        "failed to receive pulsar message"
                                    );
                                    sleep(Duration::from_millis(100)).await;
                                }
                                None => {
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        "pulsar consumer closed",
                                    );
                                    sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }
                    }
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_idx,
                    "stopped pulsar ingestor"
                );
            });
            tasks.push(task);
        }

        runtime.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                parameterized: parameterized_runtime,
                tasks,
            },
        );

        Ok(())
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<Pulsar<TokioExecutor>, String> {
        let addr = client_config_value(config, "addr", || {
            "missing Pulsar client config key 'addr'".to_string()
        })?;
        let mut builder = Pulsar::builder(addr, TokioExecutor);
        if let Some(tls_options) = Self::tls_options_from_config(config)? {
            if let Some(certificate_chain) = tls_options.certificate_chain {
                builder = builder.with_certificate_chain(certificate_chain);
            }
            builder = builder
                .with_allow_insecure_connection(tls_options.allow_insecure_connection)
                .with_tls_hostname_verification_enabled(
                    tls_options.tls_hostname_verification_enabled,
                );
        }
        builder.build().await.map_err(|source| source.to_string())
    }

    fn topic_from_config(config: &[nervix_models::ClientConfigEntry], topic: &str) -> String {
        if topic.contains("://") {
            return topic.to_string();
        }

        let namespace =
            optional_client_config_value(config, "namespace").unwrap_or("public/default");
        format!("persistent://{namespace}/{topic}")
    }

    fn tls_options_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<Option<PulsarTlsOptions>, String> {
        let tls = client_tls_paths(config);
        if tls.cert_file.is_some() || tls.key_file.is_some() {
            return Err(
                "Pulsar TLS currently supports only 'tls_ca_file'; client authentication via \
                 'tls_cert_file' and 'tls_key_file' is not supported"
                    .to_string(),
            );
        }

        let allow_insecure_connection =
            optional_bool_client_config_value(config, "tls_allow_insecure_connection")?;
        let tls_hostname_verification_enabled =
            optional_bool_client_config_value(config, "tls_hostname_verification_enabled")?;

        if tls.ca_file.is_none()
            && allow_insecure_connection.is_none()
            && tls_hostname_verification_enabled.is_none()
        {
            return Ok(None);
        }

        let mut tls_options = PulsarTlsOptions::default();
        if let Some(ca_file) = tls.ca_file.as_ref() {
            tls_options.certificate_chain = Some(read_tls_file(ca_file, "TLS CA certificate")?);
        }
        if let Some(allow_insecure_connection) = allow_insecure_connection {
            tls_options.allow_insecure_connection = allow_insecure_connection;
        }
        if let Some(tls_hostname_verification_enabled) = tls_hostname_verification_enabled {
            tls_options.tls_hostname_verification_enabled = tls_hostname_verification_enabled;
        }
        Ok(Some(tls_options))
    }

    async fn decode_message(
        codec: Arc<CompiledCodec>,
        domain: &Domain,
        ingestor: &Identifier,
        message: &PulsarMessage<Vec<u8>>,
    ) -> Result<DecodedRecord, CodecError> {
        let key = message.key().unwrap_or_default();
        let payload = message.payload.data.clone();

        trace!(
            domain = domain.as_str(),
            ingestor = ingestor.as_str(),
            topic = message.topic.as_str(),
            key = key,
            payload = String::from_utf8_lossy(&payload).to_string(),
            "received pulsar message"
        );

        decode_ingested_payload(codec, &payload).await
    }

    fn headers_from_message(message: &PulsarMessage<Vec<u8>>) -> IngestHeaders {
        message
            .metadata()
            .properties
            .iter()
            .map(|property| (property.key.clone(), property.value.clone()))
            .collect()
    }
}
