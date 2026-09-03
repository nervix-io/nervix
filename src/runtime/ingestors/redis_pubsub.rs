use redis::{Client as RedisClient, ClientTlsConfig, TlsCertificates as RedisTlsCertificates};

use super::super::*;

pub(in crate::runtime) struct RedisPubSubIngestor;

impl RedisPubSubIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientRedis,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let addr = client_config_value(&resolved_client.entries, "addr", || {
            "missing Redis client config key 'addr'".to_string()
        })
        .map_err(|reason| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.name.as_str().to_string(),
            reason,
        })?;
        let channel = match &ingestor.source {
            IngestSource::RedisPubSub { channel, .. } => channel.clone(),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected Redis Pub/Sub ingestor source".to_string(),
                });
            }
        };
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
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
            .expect("scheduled Redis Pub/Sub ingestor must have quiesce control");

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_runtime = runtime.clone();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_channel = channel.clone();
        let task_events = runtime.events.clone();
        let task_addr = addr.clone();
        let task_config = resolved_client.entries.clone();
        let task_client_mounts = resolved_client.mounts.clone();
        let task_quiesce = quiesce.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let mut backoff = RuntimeReconnectBackoff::default();
            let mut collector =
                IngestRouteCollector::new(IngestMetadataKind::Headers, INGEST_GROUP_MAX_ROWS);

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                channel = task_channel.as_str(),
                "started redis pubsub ingestor"
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
                            &branched_senders,
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
                let client = match Self::client_from_config(&task_addr, &task_config) {
                    Ok(client) => client,
                    Err(error) => {
                        task_runtime.record_ingestor_transient_error(
                            &task_domain,
                            &task_ingestor,
                            format!("redis client failed: {error}"),
                        );
                        warn!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            error = %error,
                            "failed to create redis pubsub client"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };
                let mut pubsub = match client.get_async_pubsub().await {
                    Ok(pubsub) => pubsub,
                    Err(error) => {
                        task_runtime.record_ingestor_transient_error(
                            &task_domain,
                            &task_ingestor,
                            format!("redis pubsub connect failed: {error}"),
                        );
                        warn!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            error = %error,
                            "failed to connect redis pubsub source"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };
                if let Err(error) = pubsub.subscribe(task_channel.as_str()).await {
                    task_runtime.record_ingestor_transient_error(
                        &task_domain,
                        &task_ingestor,
                        format!("redis subscribe failed: {error}"),
                    );
                    warn!(
                        domain = task_domain.as_str(),
                        ingestor = task_ingestor.as_str(),
                        error = %error,
                        "failed to subscribe redis pubsub source"
                    );
                    if !backoff.wait(&mut shutdown_rx).await {
                        break;
                    }
                    continue;
                }
                task_runtime.clear_ingestor_transient_error(&task_domain, &task_ingestor);
                backoff.reset();
                let mut relay = pubsub.on_message();
                loop {
                    tokio::task::consume_budget().await;
                    if let Some(payload) = task_quiesce.pop_buffered(0) {
                        if let Err(error) = task_runtime
                            .dispatch_raw_ingest_payload(RawIngestDispatch {
                                domain: &task_domain,
                                ingestor: &task_ingestor,
                                timestamp_source: task_timestamp_source.as_ref(),
                                output_routes: &output_routes,
                                filter_where: filter_where.as_ref(),
                                branched_senders: &branched_senders,
                                codec: codec.clone(),
                                payload: &payload,
                                collector: &mut collector,
                                flush: false,
                            })
                            .await
                        {
                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                "failed to dispatch buffered redis pubsub payload for ingestor \
                                 '{}' in domain '{}': {}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                                error
                            )));
                        }
                        continue;
                    }
                    let next_flush = collector.next_flush();
                    let flush_at =
                        next_flush.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
                    tokio::select! {
                        _ = task_quiesce.wait_for_change() => {
                            if task_quiesce.should_suspend_intake() {
                                let _ = task_runtime
                                    .flush_ingest_collector(
                                        &task_domain,
                                        &task_ingestor,
                                        &branched_senders,
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
                                        &branched_senders,
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
                                    &branched_senders,
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
                        message = relay.next() => {
                            match message {
                                Some(message) => {
                                    let key = message.get_channel_name().to_string();
                                    let payload = message.get_payload_bytes();

                                    trace!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        channel = task_channel.as_str(),
                                        key = key,
                                        payload = String::from_utf8_lossy(payload).to_string(),
                                        "received redis pubsub message"
                                    );

                                    let payload = BufferedIngestPayload::new(
                                        payload,
                                        BufferedIngestMetadata::without_headers(),
                                    );
                                    if let IngestorQuiesceIntake::Dispatch(payload) =
                                        task_quiesce.intake(0, payload, false)
                                    {
                                        if let Err(error) = task_runtime
                                            .dispatch_raw_ingest_payload(RawIngestDispatch {
                                                domain: &task_domain,
                                                ingestor: &task_ingestor,
                                                timestamp_source: task_timestamp_source.as_ref(),
                                                output_routes: &output_routes,
                                                filter_where: filter_where.as_ref(),
                                                branched_senders: &branched_senders,
                                                codec: codec.clone(),
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
                                                        &branched_senders,
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
                                            &branched_senders,
                                            &mut collector,
                                        )
                                        .await;
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        "redis pubsub stream closed",
                                    );
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        "redis pubsub stream closed; reconnecting"
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
                "stopped redis pubsub ingestor"
            );
        });

        runtime.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![task],
            },
        );

        Ok(())
    }

    fn client_from_config(
        addr: &str,
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<RedisClient, String> {
        let tls = client_tls_paths(config);
        if ServiceUrl::new(addr, "Redis addr").has_scheme("rediss")?
            && (tls.ca_file.is_some() || tls.cert_file.is_some() || tls.key_file.is_some())
        {
            RedisClient::build_with_tls(
                addr,
                RedisTlsCertificates {
                    client_tls: match (&tls.cert_file, &tls.key_file) {
                        (Some(cert_file), Some(key_file)) => Some(ClientTlsConfig {
                            client_cert: read_tls_file(cert_file, "TLS certificate")?,
                            client_key: read_tls_file(key_file, "TLS private key")?,
                        }),
                        (None, None) => None,
                        _ => {
                            return Err("Redis TLS client authentication requires both \
                                        'tls_cert_file' and 'tls_key_file'"
                                .to_string());
                        }
                    },
                    root_cert: match tls.ca_file.as_ref() {
                        Some(ca_file) => Some(read_tls_file(ca_file, "TLS CA certificate")?),
                        None => None,
                    },
                },
            )
            .map_err(|source| source.to_string())
        } else {
            RedisClient::open(addr).map_err(|source| source.to_string())
        }
    }
}
