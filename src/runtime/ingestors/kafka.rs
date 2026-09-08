use std::{borrow::Cow, future};

use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::{Headers, Message},
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::super::*;

pub(crate) struct KafkaIngestor;

/// The headers of one borrowed Kafka message.
///
/// Appending reads them out of the source message, so a message without headers costs
/// nothing and a value only allocates when it is not valid UTF-8.
struct KafkaMessageHeaders<'a>(Option<&'a rdkafka::message::BorrowedHeaders>);

impl IngestMessageHeaders for KafkaMessageHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(headers) = self.0 else {
            return;
        };
        for header in headers.iter() {
            let value = match header.value {
                Some(value) => String::from_utf8_lossy(value),
                None => std::borrow::Cow::Borrowed(""),
            };
            visit(header.key, value.as_ref());
        }
    }
}

impl KafkaIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: KafkaIngestorStartPlan,
        kafka_offset_state: Option<KafkaOffsetStateOriginator>,
    ) -> Result<(), RuntimeError> {
        let KafkaIngestorStartPlan {
            ingestor,
            client,
            topic,
            offset_mode,
            instances,
            mode: ack_mode,
            offset_state_placement: _,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let ack_timeout = match &ack_mode {
            KafkaIngestMode::AckParallel { timeout, .. }
            | KafkaIngestMode::AckSequential { timeout, .. } => {
                Some(Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?)
            }
            KafkaIngestMode::NoAckParallel => None,
        };
        let retry_policy = match &ack_mode {
            KafkaIngestMode::AckParallel { retry_policy, .. }
            | KafkaIngestMode::AckSequential { retry_policy, .. } => Some(
                Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            ),
            KafkaIngestMode::NoAckParallel => None,
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
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let output_routes = dependencies.output_routes;
        let filter_where = dependencies.filter_where;
        let codec = dependencies.codec;
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
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
        let mut tasks = Vec::with_capacity(instances.get().arch_into());

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
            let task_events = runtime.events().clone();
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
                            task_events.report_error(format!(
                                "failed to inspect kafka partitions for ingestor '{}' in domain \
                                 '{}': {}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                                error
                            ));
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
                                task_events.report_error(format!(
                                    "failed to inspect kafka partitions for ingestor '{}' in \
                                     domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                ));
                                continue;
                            }
                        };

                    if current_partitions != observed_partitions {
                        observed_partitions = current_partitions.clone();
                        let rebalance_epoch = rebalance_tx
                            .borrow()
                            .checked_add(1)
                            .assured("an ingestor cannot observe 2^64 partition rebalances");
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

        for instance_idx in 0..instances.get() {
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
                } else if let KafkaIngestMode::NoAckParallel = &ack_mode {
                    "true"
                } else {
                    "false"
                },
            );
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
            let task_events = runtime.events().clone();
            let task_output_routes = output_routes.clone();
            let task_filter_where = filter_where.clone();
            let task_codec = codec.clone();
            let task_branched_senders = branched_runtime.senders.clone();
            let task_kafka_offset_state = kafka_offset_state.clone();
            let task_ack_mode = ack_mode.clone();
            let task_offset_mode = offset_mode.clone();
            let task_quiesce = quiesce.clone();
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
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    topic = task_topic.as_str(),
                    instance = instance_idx,
                    "started kafka ingestor"
                );

                let ack_parallel_limit = match &task_ack_mode {
                    KafkaIngestMode::AckParallel { max, .. } => addressable_count(*max),
                    _ => NonZeroUsize::MIN,
                };
                let ack_timeout = task_ack_timeout;
                let retry_policy = task_retry_policy;
                let batch_timeout = task_batch_timeout;
                let mut retry_delay = retry_policy.backoff;
                let mut observed_start_version = initial_observed_start_version;
                let mut consumer_ready = initial_consumer_ready;
                let mut assignment_refresh_pending = false;
                // Decoded records accumulate here across consecutive polls. The group
                // executes once and becomes one Arrow batch per (relay, branch key)
                // instead of entering the VM and downstream channel once per record.
                // Size and idle-time bounds keep partial groups bounded.
                let mut ingest_collector =
                    IngestRouteCollector::new(IngestMetadataKind::Kafka, INGEST_GROUP_MAX_ROWS);

                'ingest: loop {
                    tokio::task::consume_budget().await;
                    if task_runtime
                        .wait_if_ingestor_faulted(&task_domain, &task_ingestor, &mut shutdown_rx)
                        .await
                    {
                        break;
                    }
                    if task_runtime.inner.ingestor_faults.is_failed(&task_ingestor) {
                        continue;
                    }
                    if task_quiesce.should_suspend_intake() {
                        let _ = task_runtime
                            .flush_ingest_collector(
                                &task_domain,
                                &task_ingestor,
                                &task_branched_senders,
                                &mut ingest_collector,
                            )
                            .await;
                        match &task_offset_mode {
                            KafkaOffsetMode::ConsumerGroup(_) => consumer.unsubscribe(),
                            KafkaOffsetMode::Domain => {
                                if let Err(error) = consumer.unassign() {
                                    task_events.report_error(format!(
                                        "failed to unassign kafka source while quiescing ingestor \
                                         '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    ));
                                }
                                consumer_ready = false;
                                assignment_refresh_pending = true;
                            }
                        }
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = task_quiesce.wait_until_not_suspended() => {}
                        }
                        if let KafkaOffsetMode::ConsumerGroup(_) = &task_offset_mode
                            && let Err(error) = consumer.subscribe(&[task_topic.as_str()])
                        {
                            task_events.report_error(format!(
                                "failed to resubscribe kafka source after quiesce for ingestor \
                                 '{}' in domain '{}': {}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                                error
                            ));
                        }
                        continue;
                    }
                    if let Some(state) = task_kafka_offset_state.as_ref() {
                        let current_start_version =
                            match task_runtime.inner.domains.get(&task_domain) {
                                Some(domain_state) => domain_state.start_version,
                                None => 0,
                            };
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
                                    task_events.report_error(format!(
                                        "failed to reset kafka domain offsets for ingestor '{}' \
                                         in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    ));
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
                                    task_events.report_error(format!(
                                        "failed to initialize kafka domain offsets for ingestor \
                                         '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    ));
                                    sleep(retry_delay).await;
                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                    continue;
                                }
                            }
                        }
                    }
                    let next_flush = ingest_collector.next_flush();
                    let flush_at =
                        next_flush.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
                    tokio::select! {
                        _ = task_quiesce.wait_for_change() => {
                            continue;
                        }
                        changed = shutdown_rx.changed() => {
                            let _ = task_runtime.flush_ingest_collector(
                                &task_domain,
                                &task_ingestor,
                                &task_branched_senders,
                                &mut ingest_collector,
                            ).await;
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
                        _ = sleep_until(flush_at), if next_flush.is_some() => {
                            if let Err(error) = task_runtime.flush_ingest_collector(
                                &task_domain,
                                &task_ingestor,
                                &task_branched_senders,
                                &mut ingest_collector,
                            ).await {
                                task_events.report_error(format!(
                                    "failed to flush ingest group for ingestor '{}' in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                ));
                            }
                        }
                        message = consumer.recv() => {
                            match message {
                                Ok(message) => {
                                    task_runtime
                                        .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                    // Decoding appends one row to the group's own record builder,
                                    // so a poll group is one set of Arrow columns. The payload is
                                    // copied because the codec may leave the reactor while the
                                    // source message stays borrowed from the consumer; every other
                                    // value a mode needs is read from the borrowed message where
                                    // it is used.
                                    let trace_message = |message: &rdkafka::message::BorrowedMessage<'_>| {
                                        let key = match message.key_view::<str>() {
                                            Some(Ok(key)) => key.to_owned(),
                                            Some(Err(_)) | None => match message.key() {
                                                Some(bytes) => {
                                                    String::from_utf8_lossy(bytes).to_string()
                                                }
                                                None => String::new(),
                                            },
                                        };
                                        trace!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            topic = message.topic(),
                                            partition = message.partition(),
                                            offset = message.offset(),
                                            key,
                                            payload = String::from_utf8_lossy(message.payload().unwrap_or_default()).to_string(),
                                            "received kafka message"
                                        );
                                    };

                                    match &task_ack_mode {
                                        KafkaIngestMode::NoAckParallel => {
                                            trace_message(&message);
                                            match ingest_collector
                                                .decode_payload(
                                                    &task_codec,
                                                    Cow::Borrowed(message.payload().unwrap_or_default()),
                                                )
                                                .await
                                            {
                                                Ok(()) => {
                                                    let headers = KafkaMessageHeaders(message.headers());
                                                    let metadata = [IngestMetadataRow::Kafka {
                                                        topic: message.topic(),
                                                        partition: message.partition(),
                                                        offset: message.offset(),
                                                        headers: &headers,
                                                    }];
                                                    let dispatched = if let Err(error) = task_runtime
                                                        .dispatch_ingested_records(IngestGroupDispatch {
                                                            collector: &mut ingest_collector,
                                                            domain: &task_domain,
                                                            ingestor: &task_ingestor,
                                                            timestamp_source: task_timestamp_source
                                                                .as_ref(),
                                                            output_routes: &task_output_routes,
                                                            filter_where: task_filter_where.as_ref(),
                                                            metadata: &metadata,
                                                            ingested_at: current_timestamp(),
                                                            acks: vec![AckSet::empty()],
                                                        })
                                                        .await
                                                    {
                                                        task_events.report_error(format!(
                                                            "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        ));
                                                        false
                                                    } else {
                                                        true
                                                    };
                                                    let flushed = if dispatched
                                                        && ingest_collector.len()
                                                            >= INGEST_GROUP_MAX_ROWS
                                                    {
                                                        if let Err(error) = task_runtime
                                                            .flush_ingest_collector(
                                                                &task_domain,
                                                                &task_ingestor,
                                                                &task_branched_senders,
                                                                &mut ingest_collector,
                                                            )
                                                            .await
                                                        {
                                                            task_events.report_error(format!(
                                                                "failed to flush ingest group for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            ));
                                                            false
                                                        } else {
                                                            true
                                                        }
                                                    } else {
                                                        dispatched
                                                    };
                                                    if flushed
                                                        && let Some(state) =
                                                        task_kafka_offset_state.as_ref()
                                                        && let Err(error) = task_runtime
                                                            .commit_domain_kafka_offset(
                                                                state,
                                                                message.topic(),
                                                                message.partition(),
                                                                message.offset() + 1,
                                                            )
                                                            .await
                                                    {
                                                        task_events.report_error(format!(
                                                            "failed to persist kafka domain offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        ));
                                                        let _ = Self::seek_offset(
                                                            &consumer,
                                                            message.topic(),
                                                            message.partition(),
                                                            message.offset(),
                                                        );
                                                    }
                                                }
                                                Err(error) => {
                                                    task_events.report_error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    ));
                                                }
                                            }
                                        }
                                        KafkaIngestMode::AckSequential { .. } => {
                                            trace_message(&message);
                                            let payload = message.payload().unwrap_or_default().to_vec();

                                            let headers = KafkaMessageHeaders(message.headers());
                                            let metadata = [IngestMetadataRow::Kafka {
                                                topic: message.topic(),
                                                partition: message.partition(),
                                                offset: message.offset(),
                                                headers: &headers,
                                            }];

                                            loop {
                                            tokio::task::consume_budget().await;
                                                // One acknowledged message is one group, and a replay
                                                // decodes into the group it replays into.
                                                let mut collector =
                                                    IngestRouteCollector::new(IngestMetadataKind::Kafka, 1);
                                                if let Err(error) = collector
                                                    .decode_payload(&task_codec, Cow::Borrowed(&payload))
                                                    .await
                                                {
                                                    task_events.report_error(format!(
                                                        "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    ));
                                                    if let Err(seek_error) = Self::seek_offset(&consumer, message.topic(), message.partition(), message.offset()) {
                                                        task_events.report_error(format!(
                                                            "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            seek_error
                                                        ));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                    continue 'ingest;
                                                }
                                                let (acks, completion) = task_runtime
                                                    .tracked_ingestor_ack_root(
                                                        &task_domain,
                                                        &task_ingestor,
                                                    );
                                                let dispatch_result = task_runtime
                                                    .dispatch_ingested_records(IngestGroupDispatch {
                                                        collector: &mut collector,
                                                        domain: &task_domain,
                                                        ingestor: &task_ingestor,
                                                        timestamp_source: task_timestamp_source
                                                            .as_ref(),
                                                        output_routes: &task_output_routes,
                                                        filter_where: task_filter_where.as_ref(),
                                                        metadata: &metadata,
                                                        ingested_at: current_timestamp(),
                                                        acks: vec![if !task_branched_senders.is_empty() {
                                                            acks.attached()
                                                        } else {
                                                            acks.clone()
                                                        }],
                                                    })
                                                    .await;
                                                let flush_result = task_runtime
                                                    .flush_ingest_collector(
                                                        &task_domain,
                                                        &task_ingestor,
                                                        &task_branched_senders,
                                                        &mut collector,
                                                    )
                                                    .await;
                                                let dispatched = match dispatch_result
                                                    .and(flush_result)
                                                {
                                                    Ok(()) => true,
                                                    Err(error) => {
                                                        task_events.report_error(format!(
                                                            "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        ));
                                                        false
                                                    }
                                                };
                                                if dispatched {
                                                    acks.ack_success();
                                                    match Runtime::await_ack_completion(
                                                        &mut shutdown_rx,
                                                        completion,
                                                        ack_timeout.verified("this branch runs only for an ACK mode, and every ACK mode parses a timeout above"),
                                                    ).await {
                                                        Some(AckOutcome::Ack) => {
                                                            let commit_result = if let Some(state) =
                                                                task_kafka_offset_state.as_ref()
                                                            {
                                                                task_runtime
                                                                    .commit_domain_kafka_offset(
                                                                        state,
                                                                        message.topic(),
                                                                        message.partition(),
                                                                        message.offset() + 1,
                                                                    )
                                                                    .await
                                                            } else {
                                                                Self::commit_offset(
                                                                    &consumer,
                                                                    message.topic(),
                                                                    message.partition(),
                                                                    message.offset() + 1,
                                                                )
                                                            };
                                                            if let Err(error) = commit_result {
                                                                task_events.report_error(format!(
                                                                    "failed to commit kafka offset for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                ));
                                                                if let Err(seek_error) = Self::seek_offset(
                                                                    &consumer,
                                                                    message.topic(),
                                                                    message.partition(),
                                                                    message.offset(),
                                                                ) {
                                                                    task_events.report_error(format!(
                                                                        "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                        task_ingestor.as_str(),
                                                                        task_domain.as_str(),
                                                                        seek_error
                                                                    ));
                                                                }
                                                                sleep(retry_delay).await;
                                                                retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                            } else {
                                                                retry_delay = retry_policy.backoff;
                                                                break;
                                                            }
                                                        }
                                                        Some(AckOutcome::NoAck(error)) => {
                                                            task_events.report_error(format!(
                                                                "kafka ack chain failed for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            ));
                                                            if let Err(seek_error) = Self::seek_offset(
                                                                &consumer,
                                                                message.topic(),
                                                                message.partition(),
                                                                message.offset(),
                                                            ) {
                                                                task_events.report_error(format!(
                                                                    "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    seek_error
                                                                ));
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
                                                        message.topic(),
                                                        message.partition(),
                                                        message.offset(),
                                                    ) {
                                                        task_events.report_error(format!(
                                                            "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            seek_error
                                                        ));
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                }
                                            }
                                        }
                                        KafkaIngestMode::AckParallel { .. } => {
                                            // The poll group is one ingest group, so every message
                                            // it polls decodes into the same record builder.
                                            let mut collector = IngestRouteCollector::new(
                                                IngestMetadataKind::Kafka,
                                                ack_parallel_limit.get(),
                                            );
                                            let mut messages = Vec::with_capacity(ack_parallel_limit.get());
                                            trace_message(&message);
                                            if let Err(error) = collector
                                                .decode_payload(
                                                    &task_codec,
                                                    Cow::Borrowed(message.payload().unwrap_or_default()),
                                                )
                                                .await
                                            {
                                                task_events.report_error(format!(
                                                    "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                    task_ingestor.as_str(),
                                                    task_domain.as_str(),
                                                    error
                                                ));
                                                if let Err(seek_error) = Self::seek_offset(&consumer, message.topic(), message.partition(), message.offset()) {
                                                    task_events.report_error(format!(
                                                        "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        seek_error
                                                    ));
                                                }
                                                sleep(retry_delay).await;
                                                retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                continue 'ingest;
                                            }
                                            messages.push(message);
                                            let batch_deadline =
                                                Instant::now() + batch_timeout.verified("this branch runs only for the parallel ACK mode, which parses a batch timeout above");

                                            while messages.len() < ack_parallel_limit.get() {
                                                tokio::task::consume_budget().await;
                                                tokio::select! {
                                                    _ = task_quiesce.wait_for_change() => {
                                                        continue 'ingest;
                                                    }
                                                    changed = shutdown_rx.changed() => {
                                                        if changed.is_err() || *shutdown_rx.borrow() {
                                                            break 'ingest;
                                                        }
                                                    }
                                                    _ = sleep_until(batch_deadline) => break,
                                                    next = consumer.recv() => {
                                                        match next {
                                                            Ok(next_message) => {
                                                                trace_message(&next_message);
                                                                match collector
                                                                    .decode_payload(
                                                                        &task_codec,
                                                                        Cow::Borrowed(next_message.payload().unwrap_or_default()),
                                                                    )
                                                                    .await
                                                                {
                                                                    Ok(()) => messages.push(next_message),
                                                                    Err(error) => {
                                                                        task_events.report_error(format!(
                                                                            "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                                            task_ingestor.as_str(),
                                                                            task_domain.as_str(),
                                                                            error
                                                                        ));
                                                                        if let Err(seek_error) = Self::seek_offset(
                                                                            &consumer,
                                                                            next_message.topic(),
                                                                            next_message.partition(),
                                                                            next_message.offset(),
                                                                        ) {
                                                                            task_events.report_error(format!(
                                                                                "failed to seek kafka offset for ingestor '{}' in domain '{}': {}",
                                                                                task_ingestor.as_str(),
                                                                                task_domain.as_str(),
                                                                                seek_error
                                                                            ));
                                                                        }
                                                                        sleep(retry_delay).await;
                                                                        retry_delay = next_retry_delay(retry_delay, retry_policy);
                                                                        continue 'ingest;
                                                                    }
                                                                }
                                                            }
                                                            Err(error) => {
                                                                task_events.report_error(format!(
                                                                    "failed to receive kafka message for ingestor '{}' in domain '{}': {}",
                                                                    task_ingestor.as_str(),
                                                                    task_domain.as_str(),
                                                                    error
                                                                ));
                                                                continue;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            // The group holds its poll's messages while it
                                            // dispatches, so offsets and metadata are read
                                            // from them instead of copies taken per message.
                                            let mut batch_commit_offsets = HashMap::<(&str, i32), i64>::new();
                                            let mut batch_start_offsets = HashMap::<(&str, i32), i64>::new();

                                            for message in &messages {
                                                tokio::task::consume_budget().await;
                                                let partition_key = (message.topic(), message.partition());
                                                let offset = message.offset();
                                                batch_commit_offsets
                                                    .entry(partition_key)
                                                    .and_modify(|commit| *commit = (*commit).max(offset + 1))
                                                    .or_insert(offset + 1);
                                                batch_start_offsets
                                                    .entry(partition_key)
                                                    .and_modify(|start| *start = (*start).min(offset))
                                                    .or_insert(offset);
                                            }

                                            tokio::task::consume_budget().await;
                                                let mut completions = Vec::with_capacity(messages.len());
                                                let mut batch_failure = None::<String>;
                                                let ingested_at = current_timestamp();

                                                // Every message keeps its own ack root; the group only
                                                // shares the dispatch call, so an ack still resolves per
                                                // message.
                                                let mut roots = Vec::with_capacity(messages.len());
                                                let mut dispatch_acks = Vec::with_capacity(messages.len());
                                                for _ in &messages {
                                                    tokio::task::consume_budget().await;
                                                    let (acks, completion) = task_runtime
                                                        .tracked_ingestor_ack_root(
                                                            &task_domain,
                                                            &task_ingestor,
                                                        );
                                                    dispatch_acks.push(
                                                        if !task_branched_senders.is_empty() {
                                                            acks.attached()
                                                        } else {
                                                            acks.clone()
                                                        },
                                                    );
                                                    roots.push(acks);
                                                    completions.push(completion);
                                                }
                                                let headers = messages
                                                    .iter()
                                                    .map(|message| KafkaMessageHeaders(message.headers()))
                                                    .collect::<Vec<_>>();
                                                let metadata = messages
                                                    .iter()
                                                    .zip(&headers)
                                                    .map(|(message, headers)| IngestMetadataRow::Kafka {
                                                        topic: message.topic(),
                                                        partition: message.partition(),
                                                        offset: message.offset(),
                                                        headers,
                                                    })
                                                    .collect::<Vec<_>>();
                                                let dispatch_result = task_runtime
                                                    .dispatch_ingested_records(IngestGroupDispatch {
                                                        collector: &mut collector,
                                                        domain: &task_domain,
                                                        ingestor: &task_ingestor,
                                                        timestamp_source: task_timestamp_source
                                                            .as_ref(),
                                                        output_routes: &task_output_routes,
                                                        filter_where: task_filter_where.as_ref(),
                                                        metadata: &metadata,
                                                        acks: dispatch_acks,
                                                        ingested_at,
                                                    })
                                                    .await;
                                                let dispatched = match dispatch_result {
                                                    Ok(()) => true,
                                                    Err(error) => {
                                                        task_events.report_error(format!(
                                                            "failed to dispatch message group for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        ));
                                                        false
                                                    }
                                                };
                                                // Dispatch has taken its own reference to every message,
                                                // so the root each one was created with is released here.
                                                for acks in roots {
                                                    acks.ack_success();
                                                }
                                                if !dispatched {
                                                    batch_failure =
                                                        Some("kafka runtime dispatch failed".to_string());
                                                }

                                                if let Err(error) = task_runtime
                                                    .flush_ingest_collector(
                                                        &task_domain,
                                                        &task_ingestor,
                                                        &task_branched_senders,
                                                        &mut collector,
                                                    )
                                                    .await
                                                {
                                                    batch_failure = Some(error);
                                                }

                                                if batch_failure.is_none() {
                                                    for completion in completions {
                                                        tokio::task::consume_budget().await;
                                                        match Runtime::await_ack_completion(
                                                            &mut shutdown_rx,
                                                            completion,
                                                            ack_timeout.verified("this branch runs only for an ACK mode, and every ACK mode parses a timeout above"),
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
                                                    task_events.report_error(format!(
                                                        "kafka ack batch failed for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    ));
                                                    for ((topic, partition), offset) in &batch_start_offsets {
                                                        if let Err(seek_error) = Self::seek_offset(
                                                            &consumer,
                                                            topic,
                                                            *partition,
                                                            *offset,
                                                        ) {
                                                            task_events.report_error(format!(
                                                                "failed to seek kafka batch offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                seek_error
                                                            ));
                                                        }
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay =
                                                        next_retry_delay(retry_delay, retry_policy);
                                                    continue 'ingest;
                                                }

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
                                                                    topic,
                                                                    *partition,
                                                                    *next_offset,
                                                                )
                                                                .await
                                                        } else {
                                                            Self::commit_offset(
                                                                &consumer,
                                                                topic,
                                                                *partition,
                                                                *next_offset,
                                                            )
                                                        };
                                                        if let Err(error) = commit_result {
                                                            task_events.report_error(format!(
                                                                "failed to commit kafka offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                error
                                                            ));
                                                            batch_failure = Some(error);
                                                            break;
                                                        }
                                                    }
                                                if batch_failure.is_some() {
                                                    for ((topic, partition), offset) in &batch_start_offsets {
                                                        if let Err(seek_error) = Self::seek_offset(
                                                            &consumer,
                                                            topic,
                                                            *partition,
                                                            *offset,
                                                        ) {
                                                            task_events.report_error(format!(
                                                                "failed to seek kafka batch offset for ingestor '{}' in domain '{}': {}",
                                                                task_ingestor.as_str(),
                                                                task_domain.as_str(),
                                                                seek_error
                                                            ));
                                                        }
                                                    }
                                                    sleep(retry_delay).await;
                                                    retry_delay =
                                                        next_retry_delay(retry_delay, retry_policy);
                                                    continue 'ingest;
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
                                    task_events.report_error(format!(
                                        "failed to receive kafka message for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    ));
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

        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
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
        offsets: &HashMap<KafkaTopicPartition, Offset>,
        schedule: Option<&KafkaPartitionSchedule>,
        instance_idx: u64,
    ) -> Result<bool, String> {
        let mut partitions = Vec::new();
        for (key, offset) in offsets {
            if key.topic == topic {
                partitions.push((key.partition, *offset));
            }
        }
        partitions.sort_by_key(|(partition, _)| *partition);
        let has_topic_partitions = schedule.is_some() && !partitions.is_empty();
        let assigned_partitions = if let Some(schedule) = schedule
            && let Ok(instance_idx) = usize::try_from(instance_idx)
            && let Some(assignments) = schedule.instance_assignments.get(instance_idx)
        {
            assignments.iter().copied().collect::<HashSet<_>>()
        } else {
            HashSet::default()
        };

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
        // The fetch above asked for this one topic, so the response carries at most that topic.
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
    ) -> Result<HashMap<KafkaTopicPartition, Offset>, String> {
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
    ) -> Result<HashMap<KafkaTopicPartition, Offset>, String>
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
            offsets.insert(
                KafkaTopicPartition {
                    topic: element.topic().to_string(),
                    partition: element.partition(),
                },
                offset,
            );
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
        state: &KafkaOffsetStateRead,
        missing_partition_timestamp: Option<Timestamp>,
    ) -> Result<HashMap<KafkaTopicPartition, Offset>, String> {
        let mut offsets = HashMap::default();
        let mut missing_partitions = Vec::new();
        for partition in Self::topic_partitions(consumer, topic)? {
            if let Some(next_offset) = state.next_offset(topic, partition) {
                offsets.insert(
                    KafkaTopicPartition {
                        topic: topic.to_string(),
                        partition,
                    },
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
                offsets.insert(
                    KafkaTopicPartition {
                        topic: topic.to_string(),
                        partition,
                    },
                    Offset::Beginning,
                );
            }
        }
        Ok(offsets)
    }

    pub(in crate::runtime) fn concrete_next_offsets_from_assignment(
        consumer: &StreamConsumer,
        topic: &str,
        offsets: &HashMap<KafkaTopicPartition, Offset>,
    ) -> Result<HashMap<KafkaTopicPartition, i64>, String> {
        let mut concrete = HashMap::default();
        for (key, offset) in offsets {
            if key.topic != topic {
                continue;
            }
            let next_offset = match offset {
                Offset::Offset(value) => *value,
                Offset::Beginning => consumer
                    .fetch_watermarks(&key.topic, key.partition, Duration::from_secs(5))
                    .map(|(low, _)| low)
                    .map_err(|source| source.to_string())?,
                Offset::End | Offset::Invalid => consumer
                    .fetch_watermarks(&key.topic, key.partition, Duration::from_secs(5))
                    .map(|(_, high)| high)
                    .map_err(|source| source.to_string())?,
                Offset::Stored | Offset::OffsetTail(_) => {
                    return Err("unsupported kafka domain offset assignment".to_string());
                }
            };
            concrete.insert(key.clone(), next_offset);
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
