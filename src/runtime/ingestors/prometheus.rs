use reqwest::Client as HttpClient;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
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
        plan: PrometheusIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let PrometheusIngestorStartPlan {
            ingestor,
            client,
            query,
            every,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;

        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
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
        let cadence = runtime
            .bind_domain_cadence(domain, &every, DomainCadenceStart::AfterInterval)
            .map_err(|source| RuntimeError::StartIngestor {
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
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_runtime = runtime.clone();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let task_timestamp_source = ingestor.timestamp_source.clone();
        let task_events = runtime.events().clone();
        let task_client_mounts = resolved_client.mounts.clone();
        let task_quiesce = quiesce.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = task_client_mounts;
            let mut cadence = cadence;
            let cadence_cancellation = CancellationToken::new();

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
                if task_runtime
                    .inner
                    .fault_injection
                    .ingestor_is_failed(&task_ingestor)
                {
                    continue;
                }
                let mut buffered_collector =
                    IngestRouteCollector::new(IngestMetadataKind::Headers, INGEST_GROUP_MAX_ROWS);
                let mut drained_buffer = false;
                while let Some(payload) = task_quiesce.pop_buffered(0) {
                    tokio::task::consume_budget().await;
                    drained_buffer = true;
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
                            collector: &mut buffered_collector,
                            flush: false,
                        })
                        .await
                    {
                        task_events.report_error(format!(
                            "failed to dispatch buffered prometheus payload for ingestor '{}' in \
                             domain '{}': {}",
                            task_ingestor.as_str(),
                            task_domain.as_str(),
                            error
                        ));
                    }
                }
                if drained_buffer {
                    if let Err(error) = task_runtime
                        .flush_ingest_collector(
                            &task_domain,
                            &task_ingestor,
                            &branched_senders,
                            &mut buffered_collector,
                        )
                        .await
                    {
                        task_events.report_error(format!(
                            "failed to flush buffered prometheus payloads for ingestor '{}' in \
                             domain '{}': {}",
                            task_ingestor.as_str(),
                            task_domain.as_str(),
                            error
                        ));
                    }
                    continue;
                }
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    occurrence = cadence.next(&cadence_cancellation) => {
                        let occurrence = match occurrence {
                            Ok(occurrence) => occurrence,
                            Err(error) => {
                                task_events.report_error(format!(
                                    "prometheus ingestor '{}' in domain '{}' could not advance \
                                     its cadence: {error}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                ));
                                break;
                            }
                        };
                        if task_quiesce.should_skip_poll() {
                            continue;
                        }
                        let query = Self::query_vector(
                            &http_client,
                            &addr,
                            &query,
                            Some(occurrence.due_at()),
                        );
                        tokio::pin!(query);
                        let result = tokio::select! {
                            biased;
                            changed = shutdown_rx.changed() => {
                                if changed.is_err() || *shutdown_rx.borrow() {
                                    break;
                                }
                                continue;
                            }
                            _ = task_quiesce.wait_for_change() => continue,
                            result = &mut query => result,
                        };
                        match result {
                            Ok(samples) => {
                                task_runtime
                                    .clear_ingestor_transient_error(&task_domain, &task_ingestor);
                                let mut entries = Vec::with_capacity(samples.len());
                                for sample in samples {
                                    tokio::task::consume_budget().await;
                                    match Self::sample_payload(&sample) {
                                        Ok(payload) => {
                                            entries.push((
                                                payload,
                                                BufferedIngestMetadata::without_headers(),
                                            ));
                                        }
                                        Err(error) => {
                                            task_events.report_error(format!(
                                                "failed to materialize prometheus sample for ingestor '{}' in domain '{}': {}",
                                                task_ingestor.as_str(),
                                                task_domain.as_str(),
                                                error
                                            ));
                                            warn!(
                                                domain = task_domain.as_str(),
                                                ingestor = task_ingestor.as_str(),
                                                error = %error,
                                                "failed to materialize prometheus sample"
                                            );
                                        }
                                    }
                                }
                                if entries.is_empty() {
                                    continue;
                                }
                                let payload = BufferedIngestPayload::batch(entries);
                                if let IngestorQuiesceIntake::Dispatch(payload) =
                                    task_quiesce.intake(0, payload, false)
                                {
                                    let mut collector = IngestRouteCollector::new(
                                        IngestMetadataKind::Headers,
                                        payload.len(),
                                    );
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
                                            flush: true,
                                        })
                                        .await
                                    {
                                        task_events.report_error(format!(
                                            "failed to dispatch prometheus poll result for ingestor '{}' in domain '{}': {}",
                                            task_ingestor.as_str(),
                                            task_domain.as_str(),
                                            error
                                        ));
                                    }
                                }
                            }
                            Err(error) => {
                                task_runtime.record_ingestor_transient_error(
                                    &task_domain,
                                    &task_ingestor,
                                    format!("prometheus query failed: {error}"),
                                );
                                task_events.report_error(format!(
                                    "failed to query prometheus for ingestor '{}' in domain '{}': {}",
                                    task_ingestor.as_str(),
                                    task_domain.as_str(),
                                    error
                                ));
                                warn!(
                                    domain = task_domain.as_str(),
                                    ingestor = task_ingestor.as_str(),
                                    error = %error,
                                    "failed to query prometheus"
                                );
                            }
                        }
                    }
                    _ = task_quiesce.wait_for_change() => {}
                }
            }

            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped prometheus ingestor"
            );
        });

        runtime.inner.ingestors.insert(
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
    pub(in crate::runtime) fn client_from_config_for_test(
        config: &[ClientConfigEntry],
    ) -> Result<HttpClient, String> {
        HttpClientConfig::new(config, "Prometheus").build()
    }

    pub(in crate::runtime) fn addr_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> Result<String, String> {
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
            params.push(("time".to_string(), Self::query_time_seconds(query_time)));
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

    /// Renders an evaluation instant as the decimal number of seconds Prometheus expects.
    ///
    /// Nanosecond Unix time passed the `f64` mantissa in 1970, so the digits are laid out from the
    /// integer. Routing them through a float would round the sub-microsecond ones away, and a
    /// paced domain clock queries at instants that differ by less than that.
    pub(in crate::runtime) fn query_time_seconds(query_time: Timestamp) -> String {
        let unix_nanos = query_time.unix_nanos();
        let seconds = unix_nanos / 1_000_000_000;
        let fraction = (unix_nanos % 1_000_000_000).unsigned_abs();
        let sign = if unix_nanos < 0 && seconds == 0 {
            "-"
        } else {
            ""
        };
        format!("{sign}{seconds}.{fraction:09}")
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
        let secs: i64 = timestamp
            .trunc()
            .checked_approx_into()
            .ok_or_else(|| format!("invalid prometheus timestamp '{timestamp}'"))?;
        let nanos: u32 = (timestamp.fract().abs() * 1_000_000_000.0)
            .round()
            .checked_approx_into()
            .verified("a fractional part scaled by a billion stays inside the u32 range");
        let datetime = Utc
            .timestamp_opt(secs, nanos.min(999_999_999))
            .single()
            .ok_or_else(|| format!("invalid prometheus timestamp '{timestamp}'"))?;
        Ok(datetime.to_rfc3339())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nervix_models::Timestamp;

    use super::*;

    #[test]
    fn prometheus_helpers_render_payload_and_validate_inputs() {
        let sample = ingestors::prometheus::PrometheusVectorResult {
            metric: BTreeMap::from([("source".to_string(), "local".to_string())]),
            value: (1_735_782_245.25, "12.5".to_string()),
        };

        let timestamp =
            ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(sample.value.0)
                .expect("valid ts");
        assert!(timestamp.starts_with("2025-"));

        let payload = ingestors::prometheus::PrometheusIngestor::sample_payload(&sample)
            .expect("must render");
        let value: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
        assert_eq!(value["source"], "local");
        assert_eq!(value["value"], 12.5);
        assert_eq!(value["timestamp"], timestamp);

        let bad_value = ingestors::prometheus::PrometheusVectorResult {
            metric: BTreeMap::new(),
            value: (1.0, "NaN".to_string()),
        };
        assert!(ingestors::prometheus::PrometheusIngestor::sample_payload(&bad_value).is_err());
        assert!(
            ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(f64::INFINITY).is_err()
        );
    }

    #[test]
    fn prometheus_query_time_keeps_every_nanosecond_digit() {
        let render = |unix_nanos: i64| {
            ingestors::prometheus::PrometheusIngestor::query_time_seconds(
                Timestamp::from_unix_nanos(unix_nanos),
            )
        };

        assert_eq!(render(1_788_765_595_123_456_789), "1788765595.123456789");
        assert_ne!(
            render(1_788_765_595_123_456_789),
            render(1_788_765_595_123_456_790)
        );
        assert_eq!(render(0), "0.000000000");
        assert_eq!(render(-500_000_000), "-0.500000000");
        assert_eq!(render(-1_500_000_000), "-1.500000000");
    }

    #[test]
    fn prometheus_query_url_uses_url_parser_for_path_and_query() {
        let url = ingestors::prometheus::PrometheusIngestor::query_url(
            "http://prometheus:9090/base/?stale=true",
            vec![("query".to_string(), "vector(1)".to_string())],
        )
        .expect("must build url");
        assert_eq!(
            url.as_str(),
            "http://prometheus:9090/base/api/v1/query?query=vector%281%29"
        );
    }
}
