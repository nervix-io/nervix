use async_nats::Client as NatsClient;

use super::super::*;

pub(in crate::runtime) struct NatsIngestor;

impl NatsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientNats,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (subject, queue_group, instances) = match &ingestor.source {
            IngestSource::Nats {
                subject,
                queue_group,
                instances,
                ..
            } => (subject.clone(), queue_group.clone(), *instances),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected NATS ingestor source".to_string(),
                });
            }
        };
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;

        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
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
            .expect("scheduled NATS ingestor must have quiesce control");

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances as usize);
        for instance_idx in 0..instances {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_subject = subject.clone();
            let task_queue_group = queue_group.clone();
            let task_events = runtime.events.clone();
            let task_config = resolved_client.entries.clone();
            let task_client_mounts = resolved_client.mounts.clone();
            let task_output_routes = output_routes.clone();
            let task_filter_where = filter_where.clone();
            let task_codec = codec.clone();
            let task_branched_senders = branched_senders.clone();
            let task_quiesce = quiesce.clone();
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                let mut backoff = RuntimeReconnectBackoff::default();
                let mut collector =
                    IngestRouteCollector::new(IngestMetadataKind::Headers, INGEST_GROUP_MAX_ROWS);

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    subject = task_subject.as_str(),
                    queue_group = task_queue_group.as_str(),
                    instance = instance_idx,
                    "started nats ingestor instance"
                );

                'outer: loop {
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
                    if task_quiesce.should_suspend_intake() {
                        let _ = task_runtime
                            .flush_ingest_collector(
                                &task_domain,
                                &task_ingestor,
                                &task_branched_senders,
                                &mut collector,
                            )
                            .await;
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = task_quiesce.wait_until_not_suspended() => {}
                        }
                        continue;
                    }
                    let task_client = match Self::client_from_config(&task_config).await {
                        Ok(client) => client,
                        Err(error) => {
                            task_runtime.record_ingestor_transient_error(
                                &task_domain,
                                &task_ingestor,
                                format!("nats connect failed: {error}"),
                            );
                            warn!(
                                domain = task_domain.as_str(),
                                ingestor = task_ingestor.as_str(),
                                instance = instance_idx,
                                error = %error,
                                "failed to connect nats source"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    let mut subscriber = match task_client
                        .queue_subscribe(
                            task_subject.as_str().to_string(),
                            task_queue_group.as_str().to_string(),
                        )
                        .await
                    {
                        Ok(subscriber) => subscriber,
                        Err(error) => {
                            task_runtime.record_ingestor_transient_error(
                                &task_domain,
                                &task_ingestor,
                                format!("nats queue subscribe failed: {error}"),
                            );
                            warn!(
                                domain = task_domain.as_str(),
                                ingestor = task_ingestor.as_str(),
                                instance = instance_idx,
                                error = %error,
                                "failed to queue subscribe nats source"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    if let Err(error) = task_client.flush().await {
                        task_runtime.record_ingestor_transient_error(
                            &task_domain,
                            &task_ingestor,
                            format!("nats flush failed: {error}"),
                        );
                        warn!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            instance = instance_idx,
                            error = %error,
                            "failed to flush nats subscription"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                    task_runtime.clear_ingestor_transient_error(&task_domain, &task_ingestor);
                    backoff.reset();
                    loop {
                        tokio::task::consume_budget().await;
                        if let Some(payload) = task_quiesce.pop_buffered(instance_idx) {
                            if let Err(error) = task_runtime
                                .dispatch_raw_ingest_payload(RawIngestDispatch {
                                    domain: &task_domain,
                                    ingestor: &task_ingestor,
                                    timestamp_source: task_timestamp_source.as_ref(),
                                    output_routes: &task_output_routes,
                                    filter_where: task_filter_where.as_ref(),
                                    branched_senders: &task_branched_senders,
                                    codec: task_codec.clone(),
                                    payload: &payload,
                                    collector: &mut collector,
                                    flush: false,
                                })
                                .await
                            {
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to dispatch buffered nats payload for ingestor '{}' \
                                     in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                            }
                            continue;
                        }
                        let next_flush = collector.next_flush();
                        let flush_at = next_flush
                            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
                        tokio::select! {
                            _ = task_quiesce.wait_for_change() => {
                                if task_quiesce.should_suspend_intake() {
                                    let _ = task_runtime
                                        .flush_ingest_collector(
                                            &task_domain,
                                            &task_ingestor,
                                            &task_branched_senders,
                                            &mut collector,
                                        )
                                        .await;
                                    break;
                                }
                            }
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    let _ = task_runtime
                                        .flush_ingest_collector(
                                            &task_domain,
                                            &task_ingestor,
                                            &task_branched_senders,
                                            &mut collector,
                                        )
                                        .await;
                                    break 'outer;
                                }
                            }
                            _ = sleep_until(flush_at), if next_flush.is_some() => {
                                if let Err(error) = task_runtime
                                    .flush_ingest_collector(
                                        &task_domain,
                                        &task_ingestor,
                                        &task_branched_senders,
                                        &mut collector,
                                    )
                                    .await
                                {
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to flush messages for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                }
                            }
                            message = subscriber.next() => {
                                match message {
                                    Some(message) => {
                                        task_runtime
                                            .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                        backoff.reset();
                                        let key = message.subject.to_string();
                                        let payload = message.payload.as_ref();
                                        let headers = Self::headers_from_message(&message);

                                        trace!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            instance = instance_idx,
                                            subject = message.subject.as_str(),
                                            key = key,
                                            payload = String::from_utf8_lossy(payload).to_string(),
                                            "received nats message"
                                        );

                                        let payload = BufferedIngestPayload::new(
                                            payload,
                                            BufferedIngestMetadata::Headers(headers),
                                        );
                                        if let IngestorQuiesceIntake::Dispatch(payload) =
                                            task_quiesce.intake(instance_idx, payload, false)
                                        {
                                            if let Err(error) = task_runtime
                                                .dispatch_raw_ingest_payload(RawIngestDispatch {
                                                    domain: &task_domain,
                                                    ingestor: &task_ingestor,
                                                    timestamp_source: task_timestamp_source.as_ref(),
                                                    output_routes: &task_output_routes,
                                                    filter_where: task_filter_where.as_ref(),
                                                    branched_senders: &task_branched_senders,
                                                    codec: task_codec.clone(),
                                                    payload: &payload,
                                                    collector: &mut collector,
                                                    flush: false,
                                                })
                                                .await
                                            {
                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                    "failed to dispatch message for ingestor '{}' in domain '{}': {}",
                                                    task_ingestor.as_str(),
                                                    task_domain.as_str(),
                                                    error
                                                )));
                                            }
                                                if collector.len() >= INGEST_GROUP_MAX_ROWS
                                                    && let Err(error) = task_runtime
                                                        .flush_ingest_collector(
                                                            &task_domain,
                                                            &task_ingestor,
                                                            &task_branched_senders,
                                                            &mut collector,
                                                        )
                                                        .await
                                                {
                                                    let _ = task_events.send(RuntimeEvent::Error(
                                                        format!(
                                                            "failed to flush messages for ingestor '{}' in domain '{}': {}",
                                                            task_ingestor.as_str(),
                                                            task_domain.as_str(),
                                                            error
                                                        ),
                                                    ));
                                                }
                                        }
                                    }
                                    None => {
                                        let _ = task_runtime
                                            .flush_ingest_collector(
                                                &task_domain,
                                                &task_ingestor,
                                                &task_branched_senders,
                                                &mut collector,
                                            )
                                            .await;
                                        task_runtime.record_ingestor_transient_error(
                                            &task_domain,
                                            &task_ingestor,
                                            "nats subscription closed",
                                        );
                                        warn!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            instance = instance_idx,
                                            "nats subscription closed; reconnecting"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if !backoff.wait(&mut shutdown_rx).await {
                        break;
                    }
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_idx,
                    "stopped nats ingestor instance"
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

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<NatsClient, String> {
        let addr = client_config_value(config, "addr", || {
            "missing NATS client config key 'addr'".to_string()
        })?;
        let mut options = async_nats::ConnectOptions::new();
        let tls = client_tls_paths(config);
        if let Some(ca_file) = tls.ca_file.as_ref() {
            options = options.add_root_certificates(ca_file.clone());
        }
        match (&tls.cert_file, &tls.key_file) {
            (Some(cert_file), Some(key_file)) => {
                options = options.add_client_certificate(cert_file.clone(), key_file.clone());
            }
            (None, None) => {}
            _ => {
                return Err(
                    "NATS TLS client authentication requires both 'tls_cert_file' and \
                     'tls_key_file'"
                        .to_string(),
                );
            }
        }
        if ServiceUrl::new(&addr, "NATS addr").has_scheme("tls")? {
            options = options.require_tls(true);
        }
        options
            .connect(addr)
            .await
            .map_err(|source| source.to_string())
    }

    fn headers_from_message(message: &async_nats::Message) -> IngestHeaders {
        message
            .headers
            .as_ref()
            .map(|headers| {
                headers
                    .iter()
                    .flat_map(|(name, values)| {
                        values
                            .iter()
                            .map(|value| (name.to_string(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}
