use zeromq::{PullSocket, Socket, SocketRecv};

use super::super::*;

pub(in crate::runtime) struct ZeroMqIngestor;

impl ZeroMqIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientZeroMq,
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
            IngestSource::ZeroMq { .. } => {}
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected ZeroMQ ingestor source".to_string(),
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
        let branching = dependencies.branching;
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_runtime = runtime.clone();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_branch_value_mappings = dependencies.branch_value_mappings.clone();
        let task_events = runtime.events.clone();
        let task = tokio::spawn(async move {
            let mut backoff = RuntimeReconnectBackoff::default();
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "started zeromq ingestor"
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
                let mut socket = match Self::pull_socket_from_client(&client).await {
                    Ok(socket) => socket,
                    Err(error) => {
                        task_runtime.record_ingestor_transient_error(
                            &task_domain,
                            &task_ingestor,
                            format!("zeromq connect failed: {error}"),
                        );
                        warn!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            error = %error,
                            "failed to connect zeromq source"
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
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break 'outer;
                            }
                        }
                        frame = socket.recv() => {
                            match frame {
                                Ok(message) => {
                                    let payload = message.into_vec();
                                    let Some(payload) = payload.first() else {
                                        continue;
                                    };

                                    trace!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        payload = String::from_utf8_lossy(payload).to_string(),
                                        "received zeromq message"
                                    );

                                    match decode_ingested_payload(codec.clone(), payload).await {
                                        Ok(record) => {
                                            let mut output_routes = output_routes.clone();
                                            if let Err(error) = task_runtime
                                                .dispatch_ingested_record(IngestDispatch {
                                                    domain: &task_domain,
                                                    ingestor: &task_ingestor,
                                                    timestamp_source: task_timestamp_source.as_ref(),
                                                    branching: &branching,
                                                    branch_value_mappings: Some(&task_branch_value_mappings),
                                                    output_routes: &mut output_routes,
                                                    filter_where: filter_where.as_ref(),
                                                    branched_senders:
                                                        &branched_senders,
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
                                                "failed to decode zeromq message"
                                            );
                                        }
                                    }
                                }
                                Err(error) => {
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        format!("zeromq receive failed: {error}"),
                                    );
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to receive zeromq message for ingestor '{}' in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        error = %error,
                                        "failed to receive zeromq message"
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
                "stopped zeromq ingestor"
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

    async fn pull_socket_from_client(client: &CreateClientZeroMq) -> Result<PullSocket, String> {
        let addr = Self::addr_from_client(client)?;
        let bind = Self::bind_from_client(client);
        let mut socket = PullSocket::new();
        if bind {
            socket
                .bind(&addr)
                .await
                .map_err(|source| source.to_string())?;
        } else {
            socket
                .connect(&addr)
                .await
                .map_err(|source| source.to_string())?;
        }
        Ok(socket)
    }

    pub(in crate::runtime) fn addr_from_client(
        client: &CreateClientZeroMq,
    ) -> Result<String, String> {
        client_config_value(&client.config, "addr", || {
            "missing ZeroMQ client config key 'addr'".to_string()
        })
    }

    pub(in crate::runtime) fn bind_from_client(client: &CreateClientZeroMq) -> bool {
        optional_client_config_value(&client.config, "bind")
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }
}
