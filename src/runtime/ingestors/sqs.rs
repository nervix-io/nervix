use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    types::{Message as SqsMessage, MessageAttributeValue},
};

use super::super::*;

pub(in crate::runtime) struct SqsIngestor;

impl SqsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientSqs,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (queue, instances, ack_mode) = match &ingestor.source {
            IngestSource::Sqs {
                queue,
                instances,
                mode,
                ..
            } => (queue.clone(), *instances, mode.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected SQS ingestor source".to_string(),
                });
            }
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
        let branching = dependencies.branching;
        let ack_timeout = match &ack_mode {
            SqsIngestMode::AckSequential { timeout, .. } => {
                Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?
            }
        };

        let resolved_client = runtime
            .resolve_client_config(client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let client = Self::client_from_config(&resolved_client.entries)
            .await
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let queue_url = Self::queue_url(&client, queue.as_str())
            .await
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances as usize);

        for instance_idx in 0..instances {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_error_policies = ingestor.error_policies.clone();
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_queue = queue.clone();
            let task_events = runtime.events.clone();
            let task_output_routes = output_routes.clone();
            let task_filter_where = filter_where.clone();
            let task_codec = codec.clone();
            let task_branching = branching.clone();
            let task_branch_value_mappings = dependencies.branch_value_mappings.clone();
            let task_branched_senders = branched_runtime.senders.clone();
            let task_ack_mode = ack_mode.clone();
            let task_client = client.clone();
            let task_queue_url = queue_url.clone();
            let task_client_mounts = resolved_client.mounts.clone();
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                let mut backoff = RuntimeReconnectBackoff::default();
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    queue = task_queue.as_str(),
                    instance = instance_idx,
                    "started sqs ingestor"
                );

                loop {
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
                        response = task_client
                            .receive_message()
                            .queue_url(task_queue_url.clone())
                            .max_number_of_messages(1)
                            .message_attribute_names("All")
                            .wait_time_seconds(1)
                            .send() => {
                            match response {
                                Ok(response) => {
                                    task_runtime
                                        .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                    backoff.reset();
                                    for message in response.messages() {
                                        tokio::task::consume_budget().await;
                                        let headers = Self::headers_from_message(message);
                                        let payload = message.body().unwrap_or_default().as_bytes();

                                        trace!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            queue = task_queue.as_str(),
                                            payload = String::from_utf8_lossy(payload).to_string(),
                                            "received sqs message"
                                        );

                                        match decode_ingested_payload(task_codec.clone(), payload).await {
                                            Ok(record) => {
                                                match &task_ack_mode {
                                                    SqsIngestMode::AckSequential { .. } => {
                                                        let mut output_routes =
                                                            task_output_routes.clone();
                                                        let (acks, completion) = AckSet::root();
                                                        let dispatched = task_runtime
                                                            .dispatch_ingested_record(IngestDispatch {
                                                                domain: &task_domain,
                                                                ingestor: &task_ingestor,
                                                                timestamp_source: task_timestamp_source.as_ref(),
                                                                branching: &task_branching,
                                                                branch_value_mappings: Some(&task_branch_value_mappings),
                                                                output_routes: &mut output_routes,
                                                                filter_where: task_filter_where.as_ref(),
                                                                branched_senders: &task_branched_senders,
                                                                record,
                                                                filter_map_metadata: Some(
                                                                    IngestFilterMapMetadata::from_headers(
                                                                        headers.clone(),
                                                                    ),
                                                                ),
                                                                ingested_at: current_timestamp(),
                                                                acks: if !task_branched_senders.is_empty() {
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
                                                                ack_timeout,
                                                            ).await {
                                                                Some(AckOutcome::Ack) => {
                                                                    if let Some(receipt_handle) = message.receipt_handle()
                                                                        && let Err(error) = task_client
                                                                            .delete_message()
                                                                            .queue_url(task_queue_url.clone())
                                                                            .receipt_handle(receipt_handle)
                                                                            .send()
                                                                            .await
                                                                    {
                                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                            "failed to acknowledge sqs message for ingestor '{}' in domain '{}': {}",
                                                                            task_ingestor.as_str(),
                                                                            task_domain.as_str(),
                                                                            error
                                                                        )));
                                                                    }
                                                                }
                                                                Some(AckOutcome::NoAck(error)) => {
                                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                        "sqs ack chain failed for ingestor '{}' in domain '{}': {}",
                                                                        task_ingestor.as_str(),
                                                                        task_domain.as_str(),
                                                                        error
                                                                    )));
                                                                }
                                                                None => break,
                                                            }
                                                        } else {
                                                            task_runtime.handle_general_error_for_acks(
                                                                &task_domain,
                                                                "ingestor",
                                                                &task_ingestor,
                                                                &task_error_policies,
                                                                std::iter::once(&acks),
                                                                "sqs runtime dispatch failed".to_string(),
                                                            );
                                                        }
                                                    }
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
                                                    "failed to decode sqs message"
                                                );
                                            }
                                        }

                                    }
                                }
                                Err(error) => {
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        format!("sqs receive failed: {error}"),
                                    );
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to receive sqs message for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        error = %error,
                                        "failed to receive sqs message"
                                    );
                                    if !backoff.wait(&mut shutdown_rx).await {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_idx,
                    "stopped sqs ingestor"
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
    ) -> Result<SqsClient, String> {
        let endpoint = client_config_value(config, "endpoint", || {
            "missing SQS client config key 'endpoint'".to_string()
        })?;
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
            .region(aws_sdk_sqs::config::Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "nervix-sqs",
            ));
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
        Ok(SqsClient::new(&sdk_config))
    }

    async fn queue_url(client: &SqsClient, queue: &str) -> Result<String, String> {
        client
            .get_queue_url()
            .queue_name(queue)
            .send()
            .await
            .map_err(|source| source.to_string())?
            .queue_url()
            .map(ToOwned::to_owned)
            .ok_or_else(|| format!("SQS queue '{queue}' has no URL"))
    }

    fn headers_from_message(message: &SqsMessage) -> IngestHeaders {
        message
            .message_attributes()
            .map(|attributes| {
                attributes
                    .iter()
                    .map(|(name, value)| (name.clone(), Self::attribute_value_to_string(value)))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn attribute_value_to_string(value: &MessageAttributeValue) -> String {
        if let Some(value) = value.string_value() {
            return value.to_string();
        }
        if let Some(value) = value.binary_value() {
            return String::from_utf8_lossy(value.as_ref()).to_string();
        }
        if !value.string_list_values().is_empty() {
            return value.string_list_values().join(",");
        }
        if !value.binary_list_values().is_empty() {
            return value
                .binary_list_values()
                .iter()
                .map(|value| String::from_utf8_lossy(value.as_ref()).to_string())
                .collect::<Vec<_>>()
                .join(",");
        }
        String::new()
    }
}
