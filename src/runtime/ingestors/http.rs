use reqwest::Client as HttpClient;

use super::super::*;

pub(in crate::runtime) struct HttpIngestor;

impl HttpIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientHttp,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let every = match &ingestor.source {
            IngestSource::Http { every, .. } => every.clone(),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected HTTP ingestor source".to_string(),
                });
            }
        };
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;

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
        let method = Self::method_from_config(&resolved_client.entries).map_err(|reason| {
            RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            }
        })?;
        let http_client = Self::client_from_config(&resolved_client.entries).map_err(|reason| {
            RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            }
        })?;
        let interval =
            humantime::parse_duration(&every).map_err(|source| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: source.to_string(),
            })?;
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
        let task_events = runtime.events.clone();
        let task_client_mounts = resolved_client.mounts.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let mut ticker = tokio::time::interval(interval);

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                endpoint = endpoint.as_str(),
                every = every.as_str(),
                "started http ingestor"
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
                    _ = ticker.tick() => {
                        match http_client.request(method.clone(), endpoint.as_str()).send().await {
                            Ok(response) => {
                                if response.status() == reqwest::StatusCode::NO_CONTENT {
                                    task_runtime
                                        .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                    continue;
                                }

                                if !response.status().is_success() {
                                    let status = response.status();
                                    task_runtime.record_ingestor_transient_error(
                                        &task_domain,
                                        &task_ingestor,
                                        format!("http source returned status {status}"),
                                    );
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "http ingestor '{}' in domain '{}' received unexpected status {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        status
                                    )));
                                    warn!(
                                        domain = task_domain.as_str(),
                                        ingestor = task_ingestor.as_str(),
                                        status = %status,
                                        "http ingestor received unexpected status"
                                    );
                                    continue;
                                }

                                let headers = Self::headers_from_response(&response);
                                match response.bytes().await {
                                    Ok(payload) => match decode_ingested_payload(codec.clone(), payload.as_ref()).await {
                                        Ok(record) => {
                                            task_runtime.clear_ingestor_transient_error(
                                                &task_domain,
                                                &task_ingestor,
                                            );
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
                                                    filter_map_metadata: Some(
                                                        IngestFilterMapMetadata::from_headers(
                                                            headers.clone(),
                                                        ),
                                                    ),
                                                    ingested_at: current_timestamp(),
                                                    acks: AckSet::empty(),
                                                })
                                                .await
                                            {
                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                    "failed to dispatch http payload for ingestor '{}' in domain '{}': {}",
                                                    task_ingestor.as_str(),
                                                    task_domain.as_str(),
                                                    error
                                                )));
                                            }
                                        }
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to decode http payload for ingestor '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            warn!(
                                                domain = task_domain.as_str(),
                                                ingestor = task_ingestor.as_str(),
                                                error = %error,
                                                "failed to decode http payload"
                                            );
                                        }
                                    },
                                    Err(error) => {
                                        task_runtime.record_ingestor_transient_error(
                                            &task_domain,
                                            &task_ingestor,
                                            format!("http response body read failed: {error}"),
                                        );
                                        let _ = task_events.send(RuntimeEvent::Error(format!(
                                            "failed to read http response body for ingestor '{}' in domain '{}': {}",
                                            task_ingestor.as_str(),
                                            task_domain.as_str(),
                                            error
                                        )));
                                        warn!(
                                            domain = task_domain.as_str(),
                                            ingestor = task_ingestor.as_str(),
                                            error = %error,
                                            "failed to read http response body"
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                task_runtime.record_ingestor_transient_error(
                                    &task_domain,
                                    &task_ingestor,
                                    format!("http request failed: {error}"),
                                );
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to request http source for ingestor '{}' in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                warn!(
                                    domain = task_domain.as_str(),
                                    ingestor = task_ingestor.as_str(),
                                    error = %error,
                                    "failed to request http source"
                                );
                            }
                        }
                    }
                }
            }

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped http ingestor"
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

    #[cfg(test)]
    pub(in crate::runtime) fn endpoint_from_client(
        client: &CreateClientHttp,
    ) -> Result<String, String> {
        Self::endpoint_from_config(&client.config)
    }

    fn headers_from_response(response: &reqwest::Response) -> IngestHeaders {
        response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect()
    }

    fn endpoint_from_config(config: &[nervix_models::ClientConfigEntry]) -> Result<String, String> {
        client_config_value(config, "endpoint", || {
            "missing HTTP client config key 'endpoint'".to_string()
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn method_from_client(
        client: &CreateClientHttp,
    ) -> Result<reqwest::Method, String> {
        Self::method_from_config(&client.config)
    }

    fn method_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<reqwest::Method, String> {
        let method = optional_client_config_value(config, "method").unwrap_or("GET");
        reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|_| format!("invalid HTTP method '{method}'"))
    }

    fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<HttpClient, String> {
        HttpClientConfig::new(config, "HTTP").build()
    }
}
