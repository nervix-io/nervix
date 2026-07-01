use reqwest::Client as HttpClient;
use serde::Deserialize;
use url::Url;

use super::super::*;

pub(in crate::runtime) struct PrometheusIngestor;

#[derive(Debug, Deserialize)]
pub(in crate::runtime) struct PrometheusQueryResponse {
    status: String,
    data: PrometheusQueryData,
}

#[derive(Debug, Deserialize)]
pub(in crate::runtime) struct PrometheusQueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusVectorResult>,
}

#[derive(Debug, Deserialize)]
pub(in crate::runtime) struct PrometheusVectorResult {
    pub(in crate::runtime) metric: std::collections::BTreeMap<String, String>,
    pub(in crate::runtime) value: (f64, String),
}

impl PrometheusIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientPrometheus,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let (query, every) = match &ingestor.source {
            IngestSource::Prometheus { query, every, .. } => (query.clone(), every.clone()),
            _ => {
                return Err(RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: "expected Prometheus ingestor source".to_string(),
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
        let http_client = HttpClientConfig::new(&resolved_client.entries, "Prometheus")
            .build()
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let addr = Self::addr_from_config(&resolved_client.entries).map_err(|reason| {
            RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            }
        })?;
        let logical_interval =
            humantime::parse_duration(&every).map_err(|source| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: source.to_string(),
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
        let branching = dependencies.branching;

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_runtime = runtime.clone();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_branch_value_mappings = dependencies.branch_value_mappings.clone();
        let task_events = runtime.events.clone();
        let task_client_mounts = resolved_client.mounts.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let logical_interval_nanos =
                u64::try_from(logical_interval.as_nanos()).unwrap_or(u64::MAX);
            let mut next_logical_query = None::<Timestamp>;

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                query = query.as_str(),
                every = every.as_str(),
                "started prometheus ingestor"
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
                let mut query_time = current_timestamp();
                let paced_state = task_runtime.domains.get(&task_domain).map(|domain_state| {
                    (
                        domain_state.config.pace,
                        domain_state.clock.clone(),
                        domain_state.ticks.lock().back().cloned(),
                    )
                });
                let sleep_duration =
                    if let Some((DomainPace::Paced, clock, latest_tick)) = paced_state {
                        let Some(clock) = clock else {
                            next_logical_query = None;
                            tokio::select! {
                                changed = shutdown_rx.changed() => {
                                    if changed.is_err() || *shutdown_rx.borrow() {
                                        break;
                                    }
                                }
                                _ = sleep(Duration::from_millis(50)) => {}
                            }
                            continue;
                        };
                        let current_logical = match current_domain_logical_time(
                            &clock,
                            latest_tick.as_ref(),
                            current_timestamp(),
                        ) {
                            Ok(value) => value,
                            Err(error) => {
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to resolve prometheus domain clock for ingestor '{}' \
                                     in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                tokio::select! {
                                    changed = shutdown_rx.changed() => {
                                        if changed.is_err() || *shutdown_rx.borrow() {
                                            break;
                                        }
                                    }
                                    _ = sleep(Duration::from_millis(100)) => {}
                                }
                                continue;
                            }
                        };
                        let next_logical = next_logical_query.unwrap_or(current_logical);
                        query_time = current_logical;
                        if current_logical >= next_logical {
                            next_logical_query = current_logical
                                .into_datetime()
                                .checked_add_signed(TimeDelta::nanoseconds(
                                    logical_interval_nanos.min(i64::MAX as u64) as i64,
                                ))
                                .map(Timestamp::from);
                            Duration::ZERO
                        } else {
                            match wall_duration_until_logical_target(
                                &clock,
                                current_logical,
                                next_logical,
                            ) {
                                Ok(duration) => duration,
                                Err(error) => {
                                    let _ = task_events.send(RuntimeEvent::Error(format!(
                                        "failed to resolve prometheus cadence for ingestor '{}' \
                                         in domain '{}': {}",
                                        task_ingestor.as_str(),
                                        task_domain.as_str(),
                                        error
                                    )));
                                    Duration::from_millis(100)
                                }
                            }
                        }
                    } else {
                        next_logical_query = None;
                        logical_interval
                    };

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(sleep_duration) => {
                        let query_time = if let Some(domain_state) = task_runtime.domains.get(&task_domain) {
                            if let DomainPace::Paced = domain_state.config.pace {
                                Some(query_time)
                            } else {
                                Some(current_timestamp())
                            }
                        } else {
                            Some(current_timestamp())
                        };
                        match Self::query_vector(&http_client, &addr, &query, query_time).await {
                            Ok(samples) => {
                                task_runtime
                                    .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                for sample in samples {
                                    tokio::task::consume_budget().await;
                                    match Self::sample_payload(&sample) {
                                        Ok(payload) => match decode_ingested_payload(codec.clone(), &payload).await {
                                            Ok(record) => {
                                                let mut output_routes = output_routes.clone();
                                                if let Err(error) = task_runtime
                                                .dispatch_ingested_record(IngestDispatch {
                                                    domain: &task_domain,
                                                    ingestor: &task_ingestor,
                                                    timestamp_source: task_timestamp_source
                                                        .as_ref(),
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
                                                        "failed to dispatch prometheus sample for ingestor '{}' in domain '{}': {}",
                                                        task_ingestor.as_str(),
                                                        task_domain.as_str(),
                                                        error
                                                    )));
                                                }
                                            }
                                            Err(error) => {
                                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                                    "failed to decode prometheus sample for ingestor '{}' in domain '{}': {}",
                                                    task_ingestor.as_str(),
                                                    task_domain.as_str(),
                                                    error
                                                )));
                                                warn!(
                                                    domain = task_domain.as_str(),
                                                    ingestor = task_ingestor.as_str(),
                                                    error = %error,
                                                    "failed to decode prometheus sample"
                                                );
                                            }
                                        },
                                        Err(error) => {
                                            let _ = task_events.send(RuntimeEvent::Error(format!(
                                                "failed to materialize prometheus sample for ingestor '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            )));
                                            warn!(
                                                domain = task_domain.as_str(),
                                                ingestor = task_ingestor.as_str(),
                                                error = %error,
                                                "failed to materialize prometheus sample"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                task_runtime.record_ingestor_transient_error(
                                    &task_domain,
                                    &task_ingestor,
                                    format!("prometheus query failed: {error}"),
                                );
                                let _ = task_events.send(RuntimeEvent::Error(format!(
                                    "failed to query prometheus for ingestor '{}' in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                )));
                                warn!(
                                    domain = task_domain.as_str(),
                                    ingestor = task_ingestor.as_str(),
                                    error = %error,
                                    "failed to query prometheus"
                                );
                            }
                        }
                    }
                }
            }

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped prometheus ingestor"
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

    #[cfg(test)]
    pub(in crate::runtime) fn addr_from_client(
        client: &CreateClientPrometheus,
    ) -> Result<String, String> {
        Self::addr_from_config(&client.config)
    }

    #[cfg(test)]
    pub(in crate::runtime) fn client_from_client(
        client: &CreateClientPrometheus,
    ) -> Result<HttpClient, String> {
        HttpClientConfig::new(&client.config, "Prometheus").build()
    }

    fn addr_from_config(config: &[nervix_models::ClientConfigEntry]) -> Result<String, String> {
        client_config_value(config, "addr", || {
            "missing Prometheus client config key 'addr'".to_string()
        })
    }

    async fn query_vector(
        client: &HttpClient,
        addr: &str,
        query: &str,
        query_time: Option<Timestamp>,
    ) -> Result<Vec<PrometheusVectorResult>, String> {
        let mut params = vec![("query".to_string(), query.to_string())];
        if let Some(query_time) = query_time {
            let seconds = query_time.unix_nanos() as f64 / 1_000_000_000.0;
            params.push(("time".to_string(), format!("{seconds:.9}")));
        }
        let url = Self::query_url(addr, params)?;
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|source| source.to_string())?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(format!("prometheus query failed with {status}: {body}"));
        }
        let payload = response
            .json::<PrometheusQueryResponse>()
            .await
            .map_err(|source| source.to_string())?;
        if payload.status != "success" {
            return Err(format!(
                "prometheus query returned status '{}'",
                payload.status
            ));
        }
        if payload.data.result_type != "vector" {
            return Err(format!(
                "prometheus query returned unsupported resultType '{}'",
                payload.data.result_type
            ));
        }
        Ok(payload.data.result)
    }

    pub(in crate::runtime) fn query_url(
        addr: &str,
        params: Vec<(String, String)>,
    ) -> Result<Url, String> {
        let mut url = Url::parse(addr).map_err(|source| source.to_string())?;
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/api/v1/query");
        url.set_path(&path);
        url.set_query(None);
        url.query_pairs_mut().extend_pairs(params);
        Ok(url)
    }

    pub(in crate::runtime) fn sample_payload(
        sample: &PrometheusVectorResult,
    ) -> Result<Vec<u8>, String> {
        let mut object = serde_json::Map::new();
        for (key, value) in &sample.metric {
            object.insert(key.clone(), serde_json::Value::String(value.clone()));
        }

        let value = sample
            .value
            .1
            .parse::<f64>()
            .map_err(|_| format!("invalid prometheus sample value '{}'", sample.value.1))?;
        let value = serde_json::Number::from_f64(value)
            .ok_or_else(|| format!("non-finite prometheus sample value '{}'", sample.value.1))?;
        object.insert("value".to_string(), serde_json::Value::Number(value));
        object.insert(
            "timestamp".to_string(),
            serde_json::Value::String(Self::timestamp_to_rfc3339(sample.value.0)?),
        );

        serde_json::to_vec(&serde_json::Value::Object(object)).map_err(|source| source.to_string())
    }

    pub(in crate::runtime) fn timestamp_to_rfc3339(timestamp: f64) -> Result<String, String> {
        if !timestamp.is_finite() {
            return Err(format!("invalid prometheus timestamp '{timestamp}'"));
        }
        let secs = timestamp.trunc() as i64;
        let nanos = ((timestamp.fract().abs()) * 1_000_000_000.0).round() as u32;
        let datetime = Utc
            .timestamp_opt(secs, nanos.min(999_999_999))
            .single()
            .ok_or_else(|| format!("invalid prometheus timestamp '{timestamp}'"))?;
        Ok(datetime.to_rfc3339())
    }
}
