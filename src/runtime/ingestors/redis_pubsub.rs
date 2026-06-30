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
            .resolve_client_config(client.mount.as_ref(), &client.config)
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

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_runtime = runtime.clone();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_parameter_value_mappings = dependencies.parameter_value_mappings.clone();
        let task_channel = channel.clone();
        let task_events = runtime.events.clone();
        let task_addr = addr.clone();
        let task_config = resolved_client.entries.clone();
        let task_client_mounts = resolved_client.mounts.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let mut backoff = RuntimeReconnectBackoff::default();

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
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break 'outer;
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

                                    match decode_ingested_payload(codec.clone(), payload).await {
                                        Ok(record) => {
                                            let mut output_routes = output_routes.clone();
                                            if let Err(error) = task_runtime
                                                .dispatch_ingested_record(IngestDispatch {
                                                    domain: &task_domain,
                                                    ingestor: &task_ingestor,
                                                    timestamp_source: task_timestamp_source.as_ref(),
                                                    parameterization: &parameterization,
                                                    parameter_value_mappings: Some(&task_parameter_value_mappings),
                                                    output_routes: &mut output_routes,
                                                    filter_where: filter_where.as_ref(),
                                                    parameterized_senders:
                                                        &parameterized_senders,
                                                    record,
                                                    filter_map_metadata: None,
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
                                            }
                                        }
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to decode message for ingestor '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            warn!(
                                                domain = task_domain.as_str(),
                                                ingestor = task_ingestor.as_str(),
                                                error = %error,
                                                "failed to decode redis pubsub message"
                                            );
                                        }
                                    }
                                }
                                None => {
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
                parameterized: parameterized_runtime.runtimes,
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
