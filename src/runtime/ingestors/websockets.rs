use tokio_tungstenite::{Connector, connect_async, connect_async_tls_with_config};

use super::super::*;

pub(in crate::runtime) struct WebsocketsIngestor;

impl WebsocketsIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientWebsockets,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        match &ingestor.source {
            IngestSource::Websockets { .. } => {}
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected WebSockets ingestor source".to_string(),
                });
            }
        }

        let resolved_client = runtime
            .resolve_client_config(client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let endpoint = Self::endpoint_from_config(&resolved_client.entries).map_err(|reason| {
            RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            }
        })?;
        let signaling_protocol =
            if let Some(signaling_protocol) = client.signaling_protocol.as_ref() {
                Some(
                    runtime
                        .signaling_protocol(domain, signaling_protocol)
                        .await
                        .ok_or_else(|| RuntimeError::StartIngestor {
                            domain: domain.as_str().to_string(),
                            ingestor: ingestor.name.as_str().to_string(),
                            reason: format!(
                                "missing signaling protocol '{}'",
                                signaling_protocol.as_str()
                            ),
                        })?,
                )
            } else {
                None
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
        let task_signaling_protocol = signaling_protocol.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_parameter_value_mappings = ingestor.parameterized_by.values().to_vec();
        let task_events = runtime.events.clone();
        let task_endpoint_requires_tls =
            match ServiceUrl::new(endpoint.as_str(), "WebSockets endpoint")
                .scheme()
                .map_err(|reason| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason,
                })?
                .as_str()
            {
                "ws" => false,
                "wss" => true,
                scheme => {
                    return Err(RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: format!(
                            "unsupported WebSockets endpoint scheme '{scheme}', expected ws:// or \
                             wss://"
                        ),
                    });
                }
            };
        let task_tls_connector = if task_endpoint_requires_tls {
            Some(Connector::Rustls(
                RustlsClientConfigSource::new(&resolved_client.entries)
                    .build_with_default_roots()
                    .map_err(|reason| RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason,
                    })?,
            ))
        } else {
            None
        };
        let task_client_mounts = resolved_client.mounts.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let mut backoff = RuntimeReconnectBackoff::default();
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                endpoint = endpoint.as_str(),
                "started websockets ingestor"
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
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break 'outer;
                        }
                    }
                    connect = async {
                        if task_endpoint_requires_tls {
                            connect_async_tls_with_config(
                                endpoint.as_str(),
                                None,
                                false,
                                task_tls_connector.clone(),
                            ).await
                        } else {
                            connect_async(endpoint.as_str()).await
                        }
                    } => {
                        match connect {
                            Ok((mut relay, _)) => {
                                let buffered_payloads = if let Some(protocol) =
                                    task_signaling_protocol.as_ref()
                                {
                                    let session = match WebsocketSignalingSession::new(
                                        protocol.clone(),
                                    ) {
                                        Ok(session) => Some(session),
                                        Err(error) => {
                                            task_runtime.record_ingestor_transient_error(
                                                &task_domain,
                                                &task_ingestor,
                                                format!("websocket signaling failed: {error}"),
                                            );
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "websocket signaling failed for ingestor '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            warn!(
                                                domain = task_domain.as_str(),
                                                ingestor = task_ingestor.as_str(),
                                                error = %error,
                                                "websocket signaling failed"
                                            );
                                            None
                                        }
                                    };
                                    match session {
                                        Some(session) => match session.run(&mut relay).await {
                                            Ok(buffered_payloads) => Some(buffered_payloads),
                                            Err(error) => {
                                                task_runtime.record_ingestor_transient_error(
                                                    &task_domain,
                                                    &task_ingestor,
                                                    format!("websocket signaling failed: {error}"),
                                                );
                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                    "websocket signaling failed for ingestor '{}' in domain '{}': {}",
                                                    task_ingestor.as_str(),
                                                    task_domain.as_str(),
                                                    error
                                                )));
                                                warn!(
                                                    domain = task_domain.as_str(),
                                                    ingestor = task_ingestor.as_str(),
                                                    error = %error,
                                                    "websocket signaling failed"
                                                );
                                                None
                                            }
                                        },
                                        None => None,
                                    }
                                } else {
                                    Some(Vec::new())
                                };

                                let Some(buffered_payloads) = buffered_payloads else {
                                    if !backoff.wait(&mut shutdown_rx).await {
                                        break 'outer;
                                    }
                                    continue;
                                };

                                task_runtime
                                    .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                backoff.reset();
                                for payload in buffered_payloads {
                                    Self::dispatch_payload(
                                        &task_runtime,
                                        &task_domain,
                                        &task_ingestor,
                                        task_timestamp_source.as_ref(),
                                        &parameterization,
                                        &task_parameter_value_mappings,
                                        &output_routes,
                                        filter_where.as_ref(),
                                        &parameterized_senders,
                                        codec.clone(),
                                        payload.as_slice(),
                                        &task_events,
                                    )
                                    .await;
                                }

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
                                                Some(Ok(message)) => {
                                                    let payload = match message {
                                                        tokio_tungstenite::tungstenite::Message::Text(text) => {
                                                            Some(text.to_string().into_bytes())
                                                        }
                                                        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                                                            Some(bytes.to_vec())
                                                        }
                                                        tokio_tungstenite::tungstenite::Message::Close(_) => None,
                                                        tokio_tungstenite::tungstenite::Message::Ping(_) => None,
                                                        tokio_tungstenite::tungstenite::Message::Pong(_) => None,
                                                        tokio_tungstenite::tungstenite::Message::Frame(_) => None,
                                                    };

                                                    let Some(payload) = payload else {
                                                        break;
                                                    };

                                                    Self::dispatch_payload(
                                                        &task_runtime,
                                                        &task_domain,
                                                        &task_ingestor,
                                                        task_timestamp_source.as_ref(),
                                                        &parameterization,
                                                        &task_parameter_value_mappings,
                                                        &output_routes,
                                                        filter_where.as_ref(),
                                                        &parameterized_senders,
                                                        codec.clone(),
                                                        &payload,
                                                        &task_events,
                                                    )
                                                    .await;
                                                }
                                                Some(Err(error)) => {
                                                    task_runtime.record_ingestor_transient_error(
                                                        &task_domain,
                                                        &task_ingestor,
                                                        format!("websocket receive failed: {error}"),
                                                    );
                                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                                        "websocket receive failed for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                    warn!(
                                                        domain = task_domain.as_str(),
                                                        ingestor = task_ingestor.as_str(),
                                                        error = %error,
                                                        "websocket receive failed"
                                                    );
                                                    break;
                                                }
                                                None => break,
                                            }
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                task_runtime.record_ingestor_transient_error(
                                    &task_domain,
                                    &task_ingestor,
                                    format!("websocket connect failed: {error}"),
                                );
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to connect websocket source for ingestor '{}' in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                warn!(
                                    domain = task_domain.as_str(),
                                    ingestor = task_ingestor.as_str(),
                                    error = %error,
                                    "failed to connect websocket source"
                                );
                            }
                        }

                        if !backoff.wait(&mut shutdown_rx).await {
                            break 'outer;
                        }
                    }
                }
            }

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped websockets ingestor"
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

    async fn dispatch_payload(
        runtime: &Runtime,
        domain: &Domain,
        ingestor: &Identifier,
        timestamp_source: Option<&IngestTimestampSource>,
        parameterization: &[Identifier],
        parameter_value_mappings: &[ParameterValueMapping],
        output_routes: &RelayProcessorOutputsNode,
        filter_where: Option<&CompiledProgramWithMaterializedInterest>,
        parameterized_senders: &HashMap<Identifier, mpsc::Sender<ParameterizedEntrypointInput>>,
        codec: Arc<CompiledCodec>,
        payload: &[u8],
        events: &broadcast::Sender<RuntimeEvent>,
    ) {
        match decode_ingested_payload(codec, payload).await {
            Ok(record) => {
                let mut output_routes = output_routes.clone();
                if let Err(error) = runtime
                    .dispatch_ingested_record(IngestDispatch {
                        domain,
                        ingestor,
                        timestamp_source,
                        parameterization,
                        parameter_value_mappings: Some(parameter_value_mappings),
                        output_routes: &mut output_routes,
                        filter_where,
                        parameterized_senders,
                        record,
                        filter_map_metadata: None,
                        ingested_at: current_timestamp(),
                        acks: AckSet::empty(),
                    })
                    .await
                {
                    let _ = events.send(RuntimeEvent::Error(format!(
                        "failed to dispatch websocket payload for ingestor '{}' in domain '{}': {}",
                        ingestor.as_str(),
                        domain.as_str(),
                        error
                    )));
                }
            }
            Err(error) => {
                let _ = events.send(RuntimeEvent::Error(format!(
                    "failed to decode websocket payload for ingestor '{}' in domain '{}': {}",
                    ingestor.as_str(),
                    domain.as_str(),
                    error
                )));
                warn!(
                    domain = domain.as_str(),
                    ingestor = ingestor.as_str(),
                    error = %error,
                    "failed to decode websocket payload"
                );
            }
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn endpoint_from_client(
        client: &CreateClientWebsockets,
    ) -> Result<String, String> {
        Self::endpoint_from_config(&client.config)
    }

    fn endpoint_from_config(config: &[nervix_models::ClientConfigEntry]) -> Result<String, String> {
        client_config_value(config, "endpoint", || {
            "missing WebSockets client config key 'endpoint'".to_string()
        })
    }
}
