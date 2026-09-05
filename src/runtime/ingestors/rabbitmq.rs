use std::borrow::Cow;

use lapin::{
    Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions},
    tcp::OwnedTLSConfig,
    types::{AMQPValue, FieldTable},
};

use super::super::*;

pub(in crate::runtime) struct RabbitMqIngestor;

/// The AMQP headers of one borrowed delivery.
///
/// Appending reads them out of the delivery properties, so a delivery without headers
/// costs nothing and a value only allocates when its AMQP type is not already a string.
struct RabbitMqDeliveryHeaders<'a>(&'a lapin::message::Delivery);

impl IngestMessageHeaders for RabbitMqDeliveryHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(headers) = self.0.properties.headers().as_ref() else {
            return;
        };
        for (name, value) in headers {
            visit(
                name.as_str(),
                RabbitMqIngestor::header_value(value).as_ref(),
            );
        }
    }
}

impl RabbitMqIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientRabbitMq,
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
        let (queue, instances, ack_mode) = match &ingestor.source {
            IngestSource::RabbitMq {
                queue,
                instances,
                mode,
                ..
            } => (queue.clone(), *instances, mode.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected RabbitMQ ingestor source".to_string(),
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
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .expect("scheduled RabbitMQ ingestor must have quiesce control");
        let ack_timeout = match &ack_mode {
            RabbitMqIngestMode::AckSequential { timeout, .. } => {
                Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?
            }
        };

        let (shutdown_tx, _) = watch::channel(false);
        let mut tasks = Vec::with_capacity(instances as usize);

        for instance_idx in 0..instances {
            let mut shutdown_rx = shutdown_tx.subscribe();
            let task_runtime = runtime.clone();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_error_policies =
                internal_processor_error_policies(ingestor.general_error_policy.clone());
            let task_timestamp_source = ingestor.timestamp_source.clone();
            let task_queue = queue.clone();
            let task_events = runtime.events.clone();
            let task_output_routes = output_routes.clone();
            let task_filter_where = filter_where.clone();
            let task_codec = codec.clone();
            let task_branched_senders = branched_runtime.senders.clone();
            let task_ack_mode = ack_mode.clone();
            let task_config = resolved_client.entries.clone();
            let task_client_mounts = resolved_client.mounts.clone();
            let task_quiesce = quiesce.clone();
            let task = tokio::spawn(async move {
                let _client_mounts = task_client_mounts;
                let mut backoff = RuntimeReconnectBackoff::default();

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    queue = task_queue.as_str(),
                    instance = instance_idx,
                    "started rabbitmq ingestor"
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
                    let connection = match Self::connection_from_config(&task_config).await {
                        Ok(connection) => connection,
                        Err(error) => {
                            task_runtime.record_ingestor_transient_error(
                                &task_domain,
                                &task_ingestor,
                                format!("rabbitmq connect failed: {error}"),
                            );
                            warn!(
                                domain = task_domain.as_str(),
                                ingestor = task_ingestor.as_str(),
                                error = %error,
                                "failed to connect rabbitmq source"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    let channel = match connection.create_channel().await {
                        Ok(channel) => channel,
                        Err(error) => {
                            task_runtime.record_ingestor_transient_error(
                                &task_domain,
                                &task_ingestor,
                                format!("rabbitmq channel failed: {error}"),
                            );
                            warn!(
                                domain = task_domain.as_str(),
                                ingestor = task_ingestor.as_str(),
                                error = %error,
                                "failed to create rabbitmq channel"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    if let Err(error) = channel.basic_qos(1, BasicQosOptions::default()).await {
                        task_runtime.record_ingestor_transient_error(
                            &task_domain,
                            &task_ingestor,
                            format!("rabbitmq qos failed: {error}"),
                        );
                        warn!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            error = %error,
                            "failed to configure rabbitmq qos"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                    let mut consumer = match channel
                        .basic_consume(
                            task_queue.as_str().into(),
                            format!("{}-{instance_idx}", task_ingestor.as_str()).into(),
                            BasicConsumeOptions::default(),
                            FieldTable::default(),
                        )
                        .await
                    {
                        Ok(consumer) => consumer,
                        Err(error) => {
                            task_runtime.record_ingestor_transient_error(
                                &task_domain,
                                &task_ingestor,
                                format!("rabbitmq consume failed: {error}"),
                            );
                            warn!(
                                domain = task_domain.as_str(),
                                ingestor = task_ingestor.as_str(),
                                error = %error,
                                "failed to consume rabbitmq source"
                            );
                            if !backoff.wait(&mut shutdown_rx).await {
                                break;
                            }
                            continue;
                        }
                    };
                    task_runtime.clear_ingestor_transient_error(&task_domain, &task_ingestor);
                    backoff.reset();
                    loop {
                        tokio::task::consume_budget().await;
                        tokio::select! {
                            _ = task_quiesce.wait_for_change() => {
                                if task_quiesce.should_suspend_intake() {
                                    break;
                                }
                            }
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break 'outer;
                                }
                            }
                            delivery = consumer.next() => {
                                match delivery {
                                    Some(Ok(delivery)) => {
                                        let key = delivery.routing_key.as_str().to_string();
                                        let headers = RabbitMqDeliveryHeaders(&delivery);
                                        let payload = delivery.data.as_slice();

                                        trace!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            queue = task_queue.as_str(),
                                            delivery_tag = delivery.delivery_tag,
                                            key = key,
                                            payload = String::from_utf8_lossy(payload).to_string(),
                                            "received rabbitmq message"
                                        );

                                        match decode_ingested_payload(task_codec.clone(), payload).await {
                                            Ok(record) => {
                                                match &task_ack_mode {
                                                    RabbitMqIngestMode::AckSequential { .. } => {
                                                        // One acknowledged message is one group.
                                                        let mut collector = IngestRouteCollector::new(
                                                            IngestMetadataKind::Headers,
                                                            1,
                                                        );
                                                        let metadata = [IngestMetadataRow::Headers {
                                                            headers: &headers,
                                                        }];
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
                                                                records: vec![record],
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
                                                        let dispatched = dispatch_result
                                                            .and(flush_result)
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
                                                                    if let Err(error) = delivery.ack(BasicAckOptions::default()).await {
                                                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                            "failed to acknowledge rabbitmq message for ingestor '{}' in domain '{}': {}",
                                                                            task_ingestor.as_str(),
                                                                            task_domain.as_str(),
                                                                            error
                                                                        )));
                                                                    }
                                                                }
                                                                Some(AckOutcome::NoAck(error)) => {
                                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                                        "rabbitmq ack chain failed for ingestor '{}' in domain '{}': {}",
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
                                                                "rabbitmq runtime dispatch failed".to_string(),
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
                                                    "failed to decode rabbitmq message"
                                                );
                                            }
                                        }

                                    }
                                    Some(Err(error)) => {
                                        task_runtime.record_ingestor_transient_error(
                                            &task_domain,
                                            &task_ingestor,
                                            format!("rabbitmq receive failed: {error}"),
                                        );
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "failed to receive rabbitmq message for ingestor '{}' in domain '{}': {}",
                                            task_ingestor.as_str(),
                                            task_domain.as_str(),
                                            error
                                        )));
                                        warn!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            error = %error,
                                            "failed to receive rabbitmq message"
                                        );
                                        break;
                                    }
                                    None => {
                                        task_runtime.record_ingestor_transient_error(
                                            &task_domain,
                                            &task_ingestor,
                                            "rabbitmq consumer closed",
                                        );
                                        warn!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            "rabbitmq consumer closed; reconnecting"
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    drop(channel);
                    drop(connection);
                    if task_quiesce.should_suspend_intake() {
                        continue;
                    }
                    if !backoff.wait(&mut shutdown_rx).await {
                        break;
                    }
                }

                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_idx,
                    "stopped rabbitmq ingestor"
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

    async fn connection_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<Connection, String> {
        let addr = client_config_value(config, "addr", || {
            "missing RabbitMQ client config key 'addr'".to_string()
        })?;
        if ServiceUrl::new(&addr, "RabbitMQ addr").has_scheme("amqps")? {
            let tls = client_tls_paths(config);
            let cert_chain = if let Some(ca_file) = tls.ca_file.as_ref() {
                Some(
                    String::from_utf8(read_tls_file(ca_file, "TLS CA certificate")?)
                        .map_err(|source| format!("failed to parse RabbitMQ CA PEM: {source}"))?,
                )
            } else {
                None
            };
            Connection::connect_with_config(
                &addr,
                ConnectionProperties::default(),
                OwnedTLSConfig {
                    identity: None,
                    cert_chain,
                },
                lapin::runtime::default_runtime().map_err(|source| source.to_string())?,
            )
            .await
            .map_err(|source| source.to_string())
        } else {
            Connection::connect(&addr, ConnectionProperties::default())
                .await
                .map_err(|source| source.to_string())
        }
    }

    fn header_value(value: &AMQPValue) -> Cow<'_, str> {
        match value {
            AMQPValue::Boolean(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortShortInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortShortUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongLongInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::Float(value) => Cow::Owned(value.to_string()),
            AMQPValue::Double(value) => Cow::Owned(value.to_string()),
            AMQPValue::DecimalValue(value) => {
                Cow::Owned(format!("{}:{}", value.scale, value.value))
            }
            AMQPValue::ShortString(value) => Cow::Borrowed(value.as_str()),
            AMQPValue::LongString(value) => String::from_utf8_lossy(value.as_bytes()),
            AMQPValue::FieldArray(value) => Cow::Owned(
                value
                    .as_slice()
                    .iter()
                    .map(|value| Self::header_value(value).into_owned())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            AMQPValue::FieldTable(value) => Cow::Owned(
                value
                    .into_iter()
                    .map(|(name, value)| format!("{}={}", name.as_str(), Self::header_value(value)))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            AMQPValue::Timestamp(value) => Cow::Owned(value.to_string()),
            AMQPValue::ByteArray(value) => String::from_utf8_lossy(value.as_slice()),
            AMQPValue::Void => Cow::Borrowed(""),
        }
    }
}
