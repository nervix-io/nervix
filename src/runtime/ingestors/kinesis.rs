use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_kinesis::{Client as KinesisClient, types::ShardIteratorType};

use super::super::*;

pub(in crate::runtime) struct KinesisIngestor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::runtime) enum KinesisStartPosition {
    Latest,
    TrimHorizon,
}

impl KinesisIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientKinesis,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (relay, instances, ack_mode) = match &ingestor.source {
            IngestSource::Kinesis {
                relay,
                instances,
                mode,
                ..
            } => (relay.clone(), *instances, mode.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected Kinesis ingestor source".to_string(),
                });
            }
        };
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let parameterized_runtime = runtime.start_parameterized_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.parameterized_templates,
        );
        let output_routes = dependencies.output_routes;
        let filter_where = dependencies.filter_where;
        let codec = dependencies.codec;
        let parameterization = dependencies.parameterization;
        let (ack_timeout, retry_policy) = match &ack_mode {
            KinesisIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => (
                Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            ),
        };

        let resolved_client = runtime
            .resolve_client_config(client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let start_position =
            Self::start_position_from_config(&resolved_client.entries).map_err(|reason| {
                RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason,
                }
            })?;
        let client = Self::client_from_config(&resolved_client.entries)
            .await
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let shard_ids = Self::open_shard_ids(&client, relay.as_str())
            .await
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;

        let instance_count = instances.max(1) as usize;
        let mut assigned_shards = vec![Vec::<String>::new(); instance_count];
        for (index, shard_id) in shard_ids.into_iter().enumerate() {
            assigned_shards[index % instance_count].push(shard_id);
        }

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instance_count);

        for (instance_idx, shards) in assigned_shards.into_iter().enumerate() {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_error_policies = ingestor.error_policies.clone();
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_relay = relay.clone();
            let task_events = runtime.events.clone();
            let task_output_routes = output_routes.clone();
            let task_filter_where = filter_where.clone();
            let task_codec = codec.clone();
            let task_parameterization = parameterization.clone();
            let task_parameter_value_mappings = ingestor.parameterized_by.values().to_vec();
            let task_parameterized_senders = parameterized_runtime.senders.clone();
            let task_client = client.clone();
            let task_retry_policy = retry_policy;
            let task_client_mounts = resolved_client.mounts.clone();
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;

                #[derive(Debug)]
                struct KinesisShardState {
                    shard_id: String,
                    iterator: Option<String>,
                    last_committed_sequence: Option<String>,
                    retry_delay: Duration,
                }

                let mut shard_states = Vec::with_capacity(shards.len());
                for shard_id in shards {
                    let iterator = match Self::rebuild_shard_iterator(
                        &task_client,
                        task_relay.as_str(),
                        &shard_id,
                        None,
                        start_position,
                    )
                    .await
                    {
                        Ok(iterator) => iterator,
                        Err(error) => {
                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                "failed to initialize kinesis shard iterator for ingestor '{}' in \
                                 domain '{}': {}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                                error
                            )));
                            None
                        }
                    };
                    shard_states.push(KinesisShardState {
                        shard_id,
                        iterator,
                        last_committed_sequence: None,
                        retry_delay: task_retry_policy.backoff,
                    });
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    relay = task_relay.as_str(),
                    instance = instance_idx,
                    shard_count = shard_states.len(),
                    "started kinesis ingestor"
                );

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
                    let mut made_progress = false;

                    for shard in &mut shard_states {
                        tokio::task::consume_budget().await;
                        if *shutdown_rx.borrow() {
                            break 'ingest;
                        }

                        let Some(iterator) = shard.iterator.clone() else {
                            continue;
                        };

                        let response = tokio::select! {
                            changed = shutdown_rx.changed() => {
                                let _ = changed;
                                break 'ingest;
                            }
                            response = task_client
                                .get_records()
                                .shard_iterator(iterator)
                                .limit(100)
                                .send() => response
                        };

                        match response {
                            Ok(response) => {
                                task_runtime
                                    .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                shard.iterator =
                                    response.next_shard_iterator().map(ToOwned::to_owned);
                                let records = response.records();
                                if records.is_empty() {
                                    continue;
                                }
                                made_progress = true;

                                for record in records {
                                    tokio::task::consume_budget().await;
                                    let sequence_number = record.sequence_number().to_string();
                                    let key = record.partition_key().to_string();
                                    let payload = record.data().as_ref();

                                    trace!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        relay = task_relay.as_str(),
                                        shard = shard.shard_id.as_str(),
                                        sequence_number = sequence_number.as_str(),
                                        key = key,
                                        payload = String::from_utf8_lossy(payload).to_string(),
                                        "received kinesis record"
                                    );

                                    let decoded =
                                        decode_ingested_payload(task_codec.clone(), payload).await;
                                    let Ok(record) = decoded else {
                                        let error = decoded.expect_err("checked above");
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "failed to decode kinesis record for ingestor '{}' in \
                                             domain '{}': {}",
                                            task_ingestor.as_str(),
                                            task_domain.as_str(),
                                            error
                                        )));
                                        shard.last_committed_sequence = Some(sequence_number);
                                        shard.retry_delay = task_retry_policy.backoff;
                                        continue;
                                    };

                                    let (acks, completion) = AckSet::root();
                                    let mut output_routes = task_output_routes.clone();
                                    let dispatched = task_runtime
                                        .dispatch_ingested_record(IngestDispatch {
                                            domain: &task_domain,
                                            ingestor: &task_ingestor,
                                            timestamp_source: task_timestamp_source.as_ref(),
                                            parameterization: &task_parameterization,
                                            parameter_value_mappings: Some(
                                                &task_parameter_value_mappings,
                                            ),
                                            output_routes: &mut output_routes,
                                            filter_where: task_filter_where.as_ref(),
                                            parameterized_senders: &task_parameterized_senders,
                                            record,
                                            filter_map_metadata: None,
                                            ingested_at: current_timestamp(),
                                            acks: if !task_parameterized_senders.is_empty() {
                                                acks.attached()
                                            } else {
                                                acks.clone()
                                            },
                                        })
                                        .await
                                        .map(|()| true)
                                        .unwrap_or_else(|error| {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to dispatch kinesis record for ingestor \
                                                 '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            false
                                        });
                                    if !dispatched {
                                        task_runtime.handle_general_error_for_acks(
                                            &task_domain,
                                            "ingestor",
                                            &task_ingestor,
                                            &task_error_policies,
                                            std::iter::once(&acks),
                                            "kinesis runtime dispatch failed".to_string(),
                                        );
                                        shard.iterator = Self::rebuild_shard_iterator(
                                            &task_client,
                                            task_relay.as_str(),
                                            &shard.shard_id,
                                            shard.last_committed_sequence.as_deref(),
                                            start_position,
                                        )
                                        .await
                                        .unwrap_or(None);
                                        sleep(shard.retry_delay).await;
                                        shard.retry_delay =
                                            next_retry_delay(shard.retry_delay, task_retry_policy);
                                        break;
                                    }

                                    acks.ack_success();
                                    match Runtime::await_ack_completion(
                                        &mut shutdown_rx,
                                        completion,
                                        ack_timeout,
                                    )
                                    .await
                                    {
                                        Some(AckOutcome::Ack) => {
                                            shard.last_committed_sequence = Some(sequence_number);
                                            shard.retry_delay = task_retry_policy.backoff;
                                        }
                                        Some(AckOutcome::NoAck(error)) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "kinesis ack chain failed for ingestor '{}' in \
                                                 domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            shard.iterator = Self::rebuild_shard_iterator(
                                                &task_client,
                                                task_relay.as_str(),
                                                &shard.shard_id,
                                                shard.last_committed_sequence.as_deref(),
                                                start_position,
                                            )
                                            .await
                                            .unwrap_or(None);
                                            sleep(shard.retry_delay).await;
                                            shard.retry_delay = next_retry_delay(
                                                shard.retry_delay,
                                                task_retry_policy,
                                            );
                                            break;
                                        }
                                        None => break 'ingest,
                                    }
                                }
                            }
                            Err(error) => {
                                task_runtime.record_ingestor_transient_error(
                                    &task_domain,
                                    &task_ingestor,
                                    format!("kinesis receive failed: {error}"),
                                );
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to receive kinesis records for ingestor '{}' in \
                                     domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                shard.iterator = Self::rebuild_shard_iterator(
                                    &task_client,
                                    task_relay.as_str(),
                                    &shard.shard_id,
                                    shard.last_committed_sequence.as_deref(),
                                    start_position,
                                )
                                .await
                                .unwrap_or(None);
                                sleep(shard.retry_delay).await;
                                shard.retry_delay =
                                    next_retry_delay(shard.retry_delay, task_retry_policy);
                            }
                        }
                    }

                    if !made_progress {
                        tokio::select! {
                            changed = shutdown_rx.changed() => {
                                let _ = changed;
                                break;
                            }
                            _ = sleep(Duration::from_millis(200)) => {}
                        }
                    }
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    relay = task_relay.as_str(),
                    instance = instance_idx,
                    "stopped kinesis ingestor"
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

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<KinesisClient, String> {
        let region = optional_client_config_value(config, "region")
            .unwrap_or("us-east-1")
            .to_string();
        let access_key_id = optional_client_config_value(config, "access_key_id")
            .unwrap_or("x")
            .to_string();
        let secret_access_key = optional_client_config_value(config, "secret_access_key")
            .unwrap_or("x")
            .to_string();

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_kinesis::config::Region::new(region))
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-kinesis",
            ));
        if let Some(endpoint) = optional_client_config_value(config, "endpoint") {
            loader = loader.endpoint_url(endpoint.to_string());
        }
        if let Some(ca_file) = client_tls_paths(config).ca_file.as_ref() {
            let ca_pem = read_tls_file(ca_file, "TLS CA certificate")?;
            let tls_context = aws_smithy_http_client::tls::TlsContext::builder()
                .with_trust_store(
                    aws_smithy_http_client::tls::TrustStore::empty().with_pem_certificate(ca_pem),
                )
                .build()
                .map_err(|source| source.to_string())?;
            let http_client = aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                ))
                .tls_context(tls_context)
                .build_https();
            loader = loader.http_client(http_client);
        }
        let sdk_config = loader.load().await;
        Ok(KinesisClient::new(&sdk_config))
    }

    pub(in crate::runtime) fn start_position_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<KinesisStartPosition, String> {
        match optional_client_config_value(config, "start_position")
            .unwrap_or("latest")
            .to_ascii_lowercase()
            .as_str()
        {
            "latest" => Ok(KinesisStartPosition::Latest),
            "trim_horizon" => Ok(KinesisStartPosition::TrimHorizon),
            other => Err(format!(
                "unsupported Kinesis client start_position '{other}', expected 'latest' or \
                 'trim_horizon'"
            )),
        }
    }

    async fn open_shard_ids(client: &KinesisClient, relay: &str) -> Result<Vec<String>, String> {
        let mut shard_ids = Vec::new();
        let mut next_token = None::<String>;

        loop {
            let mut request = client.list_shards().stream_name(relay.to_string());
            if let Some(token) = next_token.as_ref() {
                request = request.next_token(token.clone());
            }
            let response = request.send().await.map_err(|source| source.to_string())?;
            for shard in response.shards() {
                let shard_id = shard.shard_id();
                let is_open = shard
                    .sequence_number_range()
                    .and_then(|range| range.ending_sequence_number())
                    .is_none();
                if is_open {
                    shard_ids.push(shard_id.to_string());
                }
            }
            next_token = response.next_token().map(ToOwned::to_owned);
            if next_token.is_none() {
                break;
            }
        }

        shard_ids.sort();
        Ok(shard_ids)
    }

    async fn rebuild_shard_iterator(
        client: &KinesisClient,
        relay: &str,
        shard_id: &str,
        last_committed_sequence: Option<&str>,
        start_position: KinesisStartPosition,
    ) -> Result<Option<String>, String> {
        let mut request = client
            .get_shard_iterator()
            .stream_name(relay.to_string())
            .shard_id(shard_id.to_string());
        if let Some(sequence_number) = last_committed_sequence {
            request = request
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(sequence_number.to_string());
        } else {
            request = request.shard_iterator_type(match start_position {
                KinesisStartPosition::Latest => ShardIteratorType::Latest,
                KinesisStartPosition::TrimHorizon => ShardIteratorType::TrimHorizon,
            });
        }
        let response = request.send().await.map_err(|source| source.to_string())?;
        Ok(response.shard_iterator().map(ToOwned::to_owned))
    }
}
