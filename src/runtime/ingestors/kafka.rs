use std::future;

use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Headers, Message},
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::super::*;

pub(crate) struct KafkaIngestor;

impl KafkaIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientKafka,
        ingestor: CreateIngestor,
        kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (topic, offset_mode, instances, ack_mode) = match &ingestor.source {
            IngestSource::Kafka {
                topic,
                offset_mode,
                instances,
                mode,
                ..
            } => (topic.clone(), offset_mode.clone(), *instances, mode.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected kafka ingestor source".to_string(),
                });
            }
        };
        let ack_timeout = match &ack_mode {
            KafkaIngestMode::AckParallel { timeout, .. }
            | KafkaIngestMode::AckSequential { timeout, .. } => {
                Some(Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?)
            }
            KafkaIngestMode::NoAckParallel { .. } => None,
        };
        let retry_policy = match &ack_mode {
            KafkaIngestMode::AckParallel { retry_policy, .. }
            | KafkaIngestMode::AckSequential { retry_policy, .. } => Some(
                Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            ),
            KafkaIngestMode::NoAckParallel { .. } => None,
        };
        let batch_timeout = match &ack_mode {
            KafkaIngestMode::AckParallel { batch_timeout, .. } => {
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

        let (shutdown_tx, _) = watch::channel(false);
        let rebalance_tx = if let KafkaOffsetMode::Domain = &offset_mode {
            Some(watch::channel(0_u64).0)
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(instances as usize);

        if let Some(rebalance_tx) = rebalance_tx.as_ref() {
            let mut watcher_config = ClientConfig::new();
            for entry in &resolved_client.entries {
                watcher_config.set(&entry.key, &entry.value);
            }
            watcher_config.set(
                "group.id",
                format!(
                    "nervix_domain_watch_{}_{}",
                    domain.as_str(),
                    ingestor.name.as_str()
                ),
            );
            watcher_config.set("enable.partition.eof", "false");
            watcher_config.set("enable.auto.commit", "false");
            watcher_config.set("auto.offset.reset", "earliest");
            let watcher_consumer: StreamConsumer =
                watcher_config
                    .create()
                    .map_err(|source| RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: source.to_string(),
                    })?;
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_topic = topic.clone();
            let task_events = runtime.events.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            let rebalance_tx = rebalance_tx.clone();
            let watcher = tokio::spawn(async move {
                let mut observed_partitions =
                    match Self::topic_partitions(&watcher_consumer, task_topic.as_str()) {
                        Ok(mut partitions) => {
                            partitions.sort_unstable();
                            partitions
                        }
                        Err(error) => {
                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                "failed to inspect kafka partitions for ingestor '{}' in domain \
                                 '{}': {}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                                error
                            )));
                            Vec::new()
                        }
                    };
                loop {
                    tokio::task::consume_budget().await;
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        _ = sleep(DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL) => {}
                    }

                    let current_partitions =
                        match Self::topic_partitions(&watcher_consumer, task_topic.as_str()) {
                            Ok(mut partitions) => {
                                partitions.sort_unstable();
                                partitions
                            }
                            Err(error) => {
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to inspect kafka partitions for ingestor '{}' in \
                                     domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                continue;
                            }
                        };

                    if current_partitions != observed_partitions {
                        observed_partitions = current_partitions.clone();
                        let rebalance_epoch = rebalance_tx.borrow().saturating_add(1);
                        let _ = rebalance_tx.send(rebalance_epoch);
                        info!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            topic = task_topic.as_str(),
                            partitions = ?current_partitions,
                            rebalance_epoch,
                            "detected kafka partition topology change"
                        );
                    }
                }
            });
            tasks.push(watcher);
        }

        for instance_idx in 0..instances {
            let mut client_config = ClientConfig::new();
            for entry in &resolved_client.entries {
                client_config.set(&entry.key, &entry.value);
            }
            let group_id = match &offset_mode {
                KafkaOffsetMode::ConsumerGroup(consumer_group) => {
                    consumer_group.as_str().to_string()
                }
                KafkaOffsetMode::Domain => {
                    format!(
                        "nervix_domain_{}_{}",
                        domain.as_str(),
                        ingestor.name.as_str()
                    )
                }
            };
            client_config.set("group.id", &group_id);
            client_config.set("enable.partition.eof", "false");
            client_config.set(
                "enable.auto.commit",
                if let KafkaOffsetMode::Domain = &offset_mode {
                    "false"
                } else if let KafkaIngestMode::NoAckParallel { .. } = &ack_mode {
                    "true"
                } else {
                    "false"
                },
            );
            client_config.set("auto.offset.reset", "earliest");

            let consumer: StreamConsumer =
                client_config
                    .create()
                    .map_err(|source| RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: source.to_string(),
                    })?;
            if let KafkaOffsetMode::ConsumerGroup(_) = &offset_mode {
                consumer.subscribe(&[topic.as_str()]).map_err(|source| {
                    RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: source.to_string(),
                    }
                })?;
            }

            let (initial_observed_start_version, initial_consumer_ready) =
                if let Some(state) = kafka_offset_state.as_ref() {
                    let (start_version, ready) = runtime
                        .initialize_domain_kafka_consumer_offsets(
                            domain,
                            &ingestor.name,
                            topic.as_str(),
                            &consumer,
                            state,
                            instance_idx,
                        )
                        .await
                        .map_err(|reason| RuntimeError::StartIngestor {
                            domain: domain.as_str().to_string(),
                            ingestor: ingestor.name.as_str().to_string(),
                            reason,
                        })?;
                    (Some(start_version), ready)
                } else {
                    (None, true)
                };

            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_topic = topic.clone();
            let task_events = runtime.events.clone();
            let task_sender = sender_relay.clone();
            let task_filter_map = filter_map.clone();
            let task_codec = codec.clone();
            let task_parameterization = parameterization.clone();
            let task_parameter_value_mappings = ingestor.parameterized_by.values().to_vec();
            let task_parameterized_sender = parameterized_runtime
                .as_ref()
                .map(|runtime| runtime.sender());
            let task_kafka_offset_state = kafka_offset_state.clone();
            let task_ack_mode = ack_mode.clone();
            let task_ack_timeout = ack_timeout;
            let task_retry_policy = retry_policy.unwrap_or(ParsedRetryPolicy {
                backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
            });
            let task_batch_timeout = batch_timeout;
            let task_client_mounts = resolved_client.mounts.clone();
            let mut rebalance_rx = rebalance_tx.as_ref().map(watch::Sender::subscribe);
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                #[derive(Clone, Debug)]
                struct KafkaBatchEntry {
                    topic: String,
                    partition: i32,
                    offset: i64,
                    next_offset: i64,
                    record: DecodedRecord,
                    filter_map_metadata: IngestFilterMapMetadata,
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    topic = task_topic.as_str(),
                    instance = instance_idx,
                    "started kafka ingestor"
                );

                let ack_parallel_limit = match &task_ack_mode {
                    KafkaIngestMode::AckParallel { max, .. } => (*max).max(1) as usize,
                    _ => 1,
                };
                let ack_timeout = task_ack_timeout;
                let retry_policy = task_retry_policy;
                let batch_timeout = task_batch_timeout;
                let mut retry_delay = retry_policy.backoff;
                let mut observed_start_version = initial_observed_start_version;
                let mut consumer_ready = initial_consumer_ready;
                let mut assignment_refresh_pending = false;

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
                    if let Some(state) = task_kafka_offset_state.as_ref() {
                        let current_start_version = task_runtime
                            .domains
                            .get(&task_domain)
                            .map(|domain_state| domain_state.start_version)
                            .unwrap_or(0);
                        if observed_start_version != Some(current_start_version)
                            || assignment_refresh_pending
                        {
                            match task_runtime
                                .initialize_domain_kafka_consumer_offsets(
                                    &task_domain,
                                    &task_ingestor,
                                    task_topic.as_str(),
                                    &consumer,
                                    state,
                                    instance_idx,
                                )
                                .await
                            {
                                Ok((start_version, ready)) => {
                                    observed_start_version = Some(start_version);
                                    consumer_ready = ready;
                                    assignment_refresh_pending = false;
                                }
                                Err(error) => {
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to reset kafka domain offsets for ingestor '{}' \
                                         in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    sleep(retry_delay).await;
                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                    continue;
                                }
                            }
                        }
                        if !consumer_ready {
                            match task_runtime
                                .initialize_domain_kafka_consumer_offsets(
                                    &task_domain,
                                    &task_ingestor,
                                    task_topic.as_str(),
                                    &consumer,
                                    state,
                                    instance_idx,
                                )
                                .await
                            {
                                Ok((start_version, ready)) => {
                                    observed_start_version = Some(start_version);
                                    consumer_ready = ready;
                                    assignment_refresh_pending = false;
                                    if !consumer_ready {
                                        tokio::select! {
                                            changed = shutdown_rx.changed() => {
                                                if changed.is_err() || *shutdown_rx.borrow() {
                                                    break;
                                                }
                                            }
                                            changed = Self::rebalance_changed(rebalance_rx.as_mut()) => {
                                                if changed.is_ok() {
                                                    assignment_refresh_pending = true;
                                                }
                                            }
                                            _ = sleep(Duration::from_millis(100)) => {}
                                        }
                                        continue;
                                    }
                                }
                                Err(error) => {
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to initialize kafka domain offsets for ingestor \
                                         '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    sleep(retry_delay).await;
                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                    continue;
                                }
                            }
                        }
                    }
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        changed = Self::rebalance_changed(rebalance_rx.as_mut()) => {
                            if changed.is_ok() {
                                assignment_refresh_pending = true;
                                continue;
                            }
                        }
                        message = consumer.recv() => {
                            match message {
                                Ok(message) => {
                                    task_runtime
                                        .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                    let build_entry = |message: &rdkafka::message::BorrowedMessage<'_>| {
                                        let topic = message.topic().to_string();
                                        let partition = message.partition();
                                        let offset = message.offset();
                                        let next_offset = offset + 1;
                                        let key = message
                                            .key_view::<str>()
                                            .and_then(Result::ok)
                                            .map(ToOwned::to_owned)
                                            .or_else(|| message.key().map(|bytes| String::from_utf8_lossy(bytes).to_string()))
                                            .unwrap_or_default();
                                        let payload = message.payload().unwrap_or_default().to_vec();
                                        let headers = message
                                            .headers()
                                            .map(|headers| {
                                                headers
                                                    .iter()
                                                    .map(|header| {
                                                        (
                                                            header.key.to_string(),
                                                            header
                                                                .value
                                                                .map(String::from_utf8_lossy)
                                                                .map(|value| value.to_string())
                                                                .unwrap_or_default(),
                                                        )
                                                    })
                                                    .collect::<IngestHeaders>()
                                            })
                                            .unwrap_or_default();

                                        trace!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            topic = topic.as_str(),
                                            partition,
                                            offset,
                                            key = key,
                                            payload = String::from_utf8_lossy(&payload).to_string(),
                                            "received kafka message"
                                        );

                                        let codec = task_codec.clone();
                                        async move {
                                            let filter_map_metadata = IngestFilterMapMetadata::kafka(
                                                topic.clone(),
                                                partition,
                                                offset,
                                                if key.is_empty() {
                                                    None
                                                } else {
                                                    Some(key.clone())
                                                },
                                                headers,
                                            );
                                            decode_ingested_payload(codec, &payload)
                                                .await
                                                .map(|record| KafkaBatchEntry {
                                                    topic,
                                                    partition,
                                                    offset,
                                                    next_offset,
                                                    record,
                                                    filter_map_metadata,
                                                })
                                        }
                                    };

                                    match &task_ack_mode {
                                        KafkaIngestMode::NoAckParallel { .. } => {
                                            match build_entry(&message).await {
                                                Ok(entry) => {
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
                                                                entry.filter_map_metadata,
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
                                                    } else if let Some(state) =
                                                        task_kafka_offset_state.as_ref()
                                                        && let Err(error) = task_runtime
                                                            .commit_domain_kafka_offset(
                                                                state,
                                                                entry.topic.as_str(),
                                                                entry.partition,
                                                                entry.next_offset,
                                                            )
                                                            .await
                                                    {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to persist kafka domain offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        )));
                                                        let _ = Self::seek_offset(
                                                            &consumer,
                                                            entry.topic.as_str(),
                                                            entry.partition,
                                                            entry.offset,
                                                        );
                                                    }
                                                }
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                }
                                            }
                                        }
                                        KafkaIngestMode::AckSequential { .. } => {
                                            let entry = match build_entry(&message).await {
                                                Ok(entry) => entry,
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    if let Err(seek_error) = Self::seek_offset(&consumer, message.topic(), message.partition(), message.offset()) {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            seek_error
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
                                                        parameterization: &task_parameterization,
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
                                                    match Runtime::await_ack_completion(
                                                        &mut shutdown_rx,
                                                        completion,
                                                        ack_timeout.expect("ack timeout must exist"),
                                                    ).await {
                                                        Some(AckOutcome::Ack) => {
                                                            let commit_result = if let Some(state) =
                                                                task_kafka_offset_state.as_ref()
                                                            {
                                                                task_runtime
                                                                    .commit_domain_kafka_offset(
                                                                        state,
                                                                        entry.topic.as_str(),
                                                                        entry.partition,
                                                                        entry.next_offset,
                                                                    )
                                                                    .await
                                                            } else {
                                                                Self::commit_offset(
                                                                    &consumer,
                                                                    entry.topic.as_str(),
                                                                    entry.partition,
                                                                    entry.next_offset,
                                                                )
                                                            };
                                                            if let Err(error) = commit_result {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to commit kafka offset for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                )));
                                                                if let Err(seek_error) = Self::seek_offset(
                                                                    &consumer,
                                                                    entry.topic.as_str(),
                                                                    entry.partition,
                                                                    entry.offset,
                                                                ) {
                                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                        "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                        task_ingestor.as_str(),
                                                                        task_domain.as_str(),
                                                                        seek_error
                                                                    )));
                                                                }
                                                                sleep(retry_delay).await;
                                                                retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                            } else {
                                                                retry_delay = retry_policy.backoff;
                                                                break;
                                                            }
                                                        }
                                                        Some(AckOutcome::NoAck(error)) => {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "kafka ack chain failed for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            )));
                                                            if let Err(seek_error) = Self::seek_offset(
                                                                &consumer,
                                                                entry.topic.as_str(),
                                                                entry.partition,
                                                                entry.offset,
                                                            ) {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    seek_error
                                                                )));
                                                            }
                                                            sleep(retry_delay).await;
                                                            retry_delay =
                                                                next_retry_delay(retry_delay, retry_policy);
                                                        }
                                                        None => break 'ingest,
                                                    }
                                                } else {
                                                    if let Err(seek_error) = Self::seek_offset(
                                                        &consumer,
                                                        entry.topic.as_str(),
                                                        entry.partition,
                                                        entry.offset,
                                                    ) {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            seek_error
                                                        )));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                        KafkaIngestMode::AckParallel { .. } => {
                                            let mut batch = Vec::with_capacity(ack_parallel_limit);
                                            let first = match build_entry(&message).await {
                                                Ok(entry) => entry,
                                                Err(error) => {
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    if let Err(seek_error) = Self::seek_offset(&consumer, message.topic(), message.partition(), message.offset()) {
                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                            "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            seek_error
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
                                                    next = consumer.recv() => {
                                                        match next {
                                                            Ok(next_message) => {
                                                                match build_entry(&next_message).await {
                                                                    Ok(entry) => batch.push(entry),
                                                                    Err(error) => {
                                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                            "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                                            task_ingestor.as_str(),
                                                                            task_domain.as_str(),
                                                                            error
                                                                        )));
                                                                        if let Err(seek_error) = Self::seek_offset(
                                                                            &consumer,
                                                                            next_message.topic(),
                                                                            next_message.partition(),
                                                                            next_message.offset(),
                                                                        ) {
                                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                                "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                                task_ingestor.as_str(),
                                                                                task_domain.as_str(),
                                                                                seek_error
                                                                            )));
                                                                        }
                                                                        sleep(retry_delay).await;
                                                                        retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                                        continue 'ingest;
                                                                    }
                                                                }
                                                            }
                                                            Err(error) => {
                                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                    "failed to receive kafka message for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                )));
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            let mut batch_commit_offsets = HashMap::<(String, i32), i64>::new();
                                            let mut batch_start_offsets = HashMap::<(String, i32), i64>::new();

                                            for entry in &batch {
                                                tokio::task::consume_budget().await;
                                                batch_commit_offsets
                                                    .entry((entry.topic.clone(), entry.partition))
                                                    .and_modify(|offset| *offset = (*offset).max(entry.next_offset))
                                                    .or_insert(entry.next_offset);
                                                batch_start_offsets
                                                    .entry((entry.topic.clone(), entry.partition))
                                                    .and_modify(|offset| *offset = (*offset).min(entry.offset))
                                                    .or_insert(entry.offset);
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
                                                            Some("kafka runtime dispatch failed".to_string());
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
                                                        "kafka ack batch failed for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    for ((topic, partition), offset) in &batch_start_offsets {
                                                        if let Err(seek_error) = Self::seek_offset(
                                                            &consumer,
                                                            topic.as_str(),
                                                            *partition,
                                                            *offset,
                                                        ) {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to seek kafka batch offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                seek_error
                                                            )));
                                                        }
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay =
                                                        next_retry_delay(retry_delay, retry_policy);
                                                } else {
                                                    retry_delay = retry_policy.backoff;
                                                    for ((topic, partition), next_offset) in
                                                        &batch_commit_offsets
                                                    {
                                                        let commit_result = if let Some(state) =
                                                            task_kafka_offset_state.as_ref()
                                                        {
                                                            task_runtime
                                                                .commit_domain_kafka_offset(
                                                                    state,
                                                                    topic.as_str(),
                                                                    *partition,
                                                                    *next_offset,
                                                                )
                                                                .await
                                                        } else {
                                                            Self::commit_offset(
                                                                &consumer,
                                                                topic.as_str(),
                                                                *partition,
                                                                *next_offset,
                                                            )
                                                        };
                                                        if let Err(error) = commit_result {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to commit kafka offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            )));
                                                            batch_failure = Some(error);
                                                            break;
                                                        }
                                                    }
                                                    if batch_failure.is_none() {
                                                        break;
                                                    }
                                                    for ((topic, partition), offset) in &batch_start_offsets {
                                                        if let Err(seek_error) = Self::seek_offset(
                                                            &consumer,
                                                            topic.as_str(),
                                                            *partition,
                                                            *offset,
                                                        ) {
                                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                "failed to seek kafka batch offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                seek_error
                                                            )));
                                                        }
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay =
                                                        next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(error) => {
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        format!("kafka receive failed: {error}"),
                                    );
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to receive kafka message for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        error = %error,
                                        "failed to receive kafka message"
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
                    "stopped kafka ingestor"
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

    fn commit_offset(
        consumer: &StreamConsumer,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(), String> {
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(topic, partition, Offset::Offset(next_offset))
            .map_err(|source| source.to_string())?;
        consumer
            .commit(&offsets, CommitMode::Async)
            .map_err(|source| source.to_string())
    }

    pub(in crate::runtime) fn assign_offsets_for_instance(
        consumer: &StreamConsumer,
        topic: &str,
        offsets: &HashMap<(String, i32), Offset>,
        schedule: Option<&KafkaPartitionSchedule>,
        instance_idx: u64,
    ) -> Result<bool, String> {
        let mut partitions = offsets
            .iter()
            .filter_map(|((entry_topic, partition), offset)| {
                if entry_topic == topic {
                    Some((*partition, *offset))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        partitions.sort_by_key(|(partition, _)| *partition);
        let has_topic_partitions = schedule.is_some() && !partitions.is_empty();
        let assigned_partitions = schedule
            .and_then(|schedule| {
                schedule
                    .instance_assignments
                    .get(usize::try_from(instance_idx).unwrap_or_default())
            })
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<HashSet<_>>();

        let mut assignment = TopicPartitionList::new();
        let mut assigned_any = false;
        for (partition, offset) in partitions {
            if assigned_partitions.contains(&partition) {
                assignment
                    .add_partition_offset(topic, partition, offset)
                    .map_err(|source| source.to_string())?;
                assigned_any = true;
            }
        }

        if assigned_any {
            consumer
                .assign(&assignment)
                .map_err(|source| source.to_string())?;
        } else {
            consumer.unassign().map_err(|source| source.to_string())?;
        }

        Ok(has_topic_partitions)
    }

    pub(crate) fn topic_partitions(
        consumer: &StreamConsumer,
        topic: &str,
    ) -> Result<Vec<i32>, String> {
        let metadata = consumer
            .fetch_metadata(Some(topic), Duration::from_secs(5))
            .map_err(|source| source.to_string())?;
        let Some(topic_metadata) = metadata.topics().iter().find(|entry| entry.name() == topic)
        else {
            return Err(format!("missing kafka topic metadata for '{topic}'"));
        };
        Ok(topic_metadata
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect())
    }

    async fn rebalance_changed(
        rebalance_rx: Option<&mut watch::Receiver<u64>>,
    ) -> Result<(), watch::error::RecvError> {
        match rebalance_rx {
            Some(rx) => rx.changed().await,
            None => future::pending().await,
        }
    }

    pub(in crate::runtime) fn offsets_by_timestamp(
        consumer: &StreamConsumer,
        topic: &str,
        timestamp: Timestamp,
    ) -> Result<HashMap<(String, i32), Offset>, String> {
        Self::offsets_for_partitions_by_timestamp(
            consumer,
            topic,
            Self::topic_partitions(consumer, topic)?,
            timestamp,
        )
    }

    fn offsets_for_partitions_by_timestamp<I>(
        consumer: &StreamConsumer,
        topic: &str,
        partitions: I,
        timestamp: Timestamp,
    ) -> Result<HashMap<(String, i32), Offset>, String>
    where
        I: IntoIterator<Item = i32>,
    {
        let mut query = TopicPartitionList::new();
        let timestamp_ms = timestamp.unix_nanos().div_euclid(1_000_000);
        for partition in partitions {
            query
                .add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
                .map_err(|source| source.to_string())?;
        }
        let resolved = consumer
            .offsets_for_times(query, Duration::from_secs(5))
            .map_err(|source| source.to_string())?;
        let mut offsets = HashMap::default();
        for element in resolved.elements() {
            let offset = match element.offset() {
                Offset::Invalid => Offset::End,
                other => other,
            };
            offsets.insert((element.topic().to_string(), element.partition()), offset);
        }
        Ok(offsets)
    }

    fn normalized_resume_offset(
        consumer: &StreamConsumer,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<Offset, String> {
        let (low, high) = consumer
            .fetch_watermarks(topic, partition, Duration::from_secs(5))
            .map_err(|source| source.to_string())?;
        let clamped = next_offset.clamp(low, high);
        Ok(Offset::Offset(clamped))
    }

    pub(in crate::runtime) fn resume_offsets_from_state(
        consumer: &StreamConsumer,
        topic: &str,
        state: &ReplicatedKafkaOffsetState,
        missing_partition_timestamp: Option<Timestamp>,
    ) -> Result<HashMap<(String, i32), Offset>, String> {
        let mut offsets = HashMap::default();
        let mut missing_partitions = Vec::new();
        for partition in Self::topic_partitions(consumer, topic)? {
            if let Some(next_offset) = state.next_offset(topic, partition) {
                offsets.insert(
                    (topic.to_string(), partition),
                    Self::normalized_resume_offset(consumer, topic, partition, next_offset)?,
                );
            } else {
                missing_partitions.push(partition);
            }
        }
        if let Some(timestamp) = missing_partition_timestamp {
            offsets.extend(Self::offsets_for_partitions_by_timestamp(
                consumer,
                topic,
                missing_partitions.iter().copied(),
                timestamp,
            )?);
        } else {
            for partition in missing_partitions {
                offsets.insert((topic.to_string(), partition), Offset::Beginning);
            }
        }
        Ok(offsets)
    }

    pub(in crate::runtime) fn concrete_next_offsets_from_assignment(
        consumer: &StreamConsumer,
        topic: &str,
        offsets: &HashMap<(String, i32), Offset>,
    ) -> Result<HashMap<(String, i32), i64>, String> {
        let mut concrete = HashMap::default();
        for ((entry_topic, partition), offset) in offsets {
            if entry_topic != topic {
                continue;
            }
            let next_offset = match offset {
                Offset::Offset(value) => *value,
                Offset::Beginning => consumer
                    .fetch_watermarks(entry_topic, *partition, Duration::from_secs(5))
                    .map(|(low, _)| low)
                    .map_err(|source| source.to_string())?,
                Offset::End | Offset::Invalid => consumer
                    .fetch_watermarks(entry_topic, *partition, Duration::from_secs(5))
                    .map(|(_, high)| high)
                    .map_err(|source| source.to_string())?,
                Offset::Stored | Offset::OffsetTail(_) => {
                    return Err("unsupported kafka domain offset assignment".to_string());
                }
            };
            concrete.insert((entry_topic.clone(), *partition), next_offset);
        }
        Ok(concrete)
    }

    fn seek_offset(
        consumer: &StreamConsumer,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), String> {
        consumer
            .seek(
                topic,
                partition,
                Offset::Offset(offset),
                std::time::Duration::from_secs(5),
            )
            .map_err(|source| source.to_string())
    }
}
