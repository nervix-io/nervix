//! Prometheus paced source connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Prometheus client configuration interpretation, instant queries, sample parsing,
//!   payload encoding and source-boundary arrival observation.
//! - **Depends on.** The connector contract, vocabulary configuration and the Prometheus HTTP
//!   API.
//! - **Must not know.** Domain clocks, runtime collectors, relays, branches, schedules, registry
//!   state or placement computation.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{TimeZone as _, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_approx_into::CheckedApproxInto as _;
use nervix_connector::{
    ClientConfigResult, HttpClientConfig, HttpClientConfigError, PacedSourceConnector,
    RetainedIngestHeaders, SourceConnector, SourceError, SourcePoll, SourcePollMessage,
    SourceResult, client_config_value, physical_time::actual_utc_now,
};
use nervix_models::{ClientConfigEntry, Timestamp};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

const PROMETHEUS: &str = "prometheus";

pub struct PrometheusSourcePlan {
    pub config: Vec<ClientConfigEntry>,
    pub query: String,
}

pub struct PrometheusSource {
    client: HttpClient,
    addr: String,
    query: String,
}

#[derive(Debug, Error)]
pub enum PrometheusSourceError {
    #[error("invalid Prometheus service address")]
    InvalidAddress,
    #[error("failed to send Prometheus query")]
    QueryRequest,
    #[error("Prometheus query failed with HTTP status {status}")]
    QueryStatus { status: reqwest::StatusCode },
    #[error("failed to decode Prometheus query response")]
    DecodeResponse,
    #[error("Prometheus query returned status '{status}'")]
    QueryRejected { status: String },
    #[error("Prometheus query returned unsupported result type '{result_type}'")]
    UnsupportedResultType { result_type: String },
    #[error("invalid Prometheus sample value")]
    InvalidSampleValue,
    #[error("non-finite Prometheus sample value")]
    NonFiniteSampleValue,
    #[error("failed to encode Prometheus sample")]
    EncodeSample,
    #[error("invalid Prometheus timestamp")]
    InvalidTimestamp,
}

#[derive(Debug, Deserialize)]
struct PrometheusQueryResponse {
    status: String,
    data: PrometheusQueryData,
}

#[derive(Debug, Deserialize)]
struct PrometheusQueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusVectorResult>,
}

#[derive(Debug, Deserialize)]
struct PrometheusVectorResult {
    metric: BTreeMap<String, String>,
    value: (f64, String),
}

#[async_trait]
impl SourceConnector for PrometheusSource {
    type Plan = PrometheusSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let client = Self::client_from_config(&plan.config).change_context(SourceError::Open {
            connector: PROMETHEUS,
        })?;
        let addr = Self::addr_from_config(&plan.config).change_context(SourceError::Open {
            connector: PROMETHEUS,
        })?;
        Ok(Self {
            client,
            addr,
            query: plan.query.clone(),
        })
    }
}

#[async_trait]
impl PacedSourceConnector for PrometheusSource {
    async fn poll(&mut self, scheduled_at: Timestamp) -> SourceResult<SourcePoll> {
        let samples =
            self.query_vector(Some(scheduled_at))
                .await
                .change_context(SourceError::Read {
                    connector: PROMETHEUS,
                })?;
        let mut messages = Vec::with_capacity(samples.len());
        let mut failures = Vec::new();
        for sample in samples {
            tokio::task::consume_budget().await;
            match Self::sample_payload(&sample) {
                Ok(payload) => messages.push(SourcePollMessage {
                    payload,
                    headers: RetainedIngestHeaders::none(),
                }),
                Err(error) => failures.push(error.change_context(SourceError::Read {
                    connector: PROMETHEUS,
                })),
            }
        }
        Ok(SourcePoll {
            messages,
            failures,
            observed_at: actual_utc_now(),
        })
    }
}

impl PrometheusSource {
    fn client_from_config(
        config: &[ClientConfigEntry],
    ) -> Result<HttpClient, Report<HttpClientConfigError>> {
        HttpClientConfig::new(config, "Prometheus").build()
    }

    fn addr_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "addr", "Prometheus")
    }

    async fn query_vector(
        &self,
        query_time: Option<Timestamp>,
    ) -> Result<Vec<PrometheusVectorResult>, Report<PrometheusSourceError>> {
        let mut params = vec![("query".to_string(), self.query.clone())];
        if let Some(query_time) = query_time {
            params.push(("time".to_string(), Self::query_time_seconds(query_time)));
        }
        let url = Self::query_url(&self.addr, params)?;
        let response = self.client.get(url).send().await.map_err(|source| {
            Report::new(PrometheusSourceError::QueryRequest).attach_printable(source.to_string())
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(Report::new(PrometheusSourceError::QueryStatus { status }));
        }
        let payload = response
            .json::<PrometheusQueryResponse>()
            .await
            .map_err(|source| {
                Report::new(PrometheusSourceError::DecodeResponse)
                    .attach_printable(source.to_string())
            })?;
        if payload.status != "success" {
            return Err(Report::new(PrometheusSourceError::QueryRejected {
                status: payload.status,
            }));
        }
        if payload.data.result_type != "vector" {
            return Err(Report::new(PrometheusSourceError::UnsupportedResultType {
                result_type: payload.data.result_type,
            }));
        }
        Ok(payload.data.result)
    }

    /// Renders an evaluation instant as the decimal number of seconds Prometheus expects.
    ///
    /// Nanosecond Unix time passed the `f64` mantissa in 1970, so the digits are laid out from the
    /// integer. Routing them through a float would round the sub-microsecond ones away, and a
    /// paced domain clock queries at instants that differ by less than that.
    fn query_time_seconds(query_time: Timestamp) -> String {
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

    fn query_url(
        addr: &str,
        params: Vec<(String, String)>,
    ) -> Result<Url, Report<PrometheusSourceError>> {
        let mut url = Url::parse(addr).map_err(|source| {
            Report::new(PrometheusSourceError::InvalidAddress).attach_printable(source.to_string())
        })?;
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/api/v1/query");
        url.set_path(&path);
        url.set_query(None);
        url.query_pairs_mut().extend_pairs(params);
        Ok(url)
    }

    fn sample_payload(
        sample: &PrometheusVectorResult,
    ) -> Result<Vec<u8>, Report<PrometheusSourceError>> {
        let mut object = serde_json::Map::new();
        for (key, value) in &sample.metric {
            object.insert(key.clone(), serde_json::Value::String(value.clone()));
        }

        let value = sample.value.1.parse::<f64>().map_err(|source| {
            Report::new(PrometheusSourceError::InvalidSampleValue)
                .attach_printable(source.to_string())
        })?;
        let value = serde_json::Number::from_f64(value)
            .ok_or_else(|| Report::new(PrometheusSourceError::NonFiniteSampleValue))?;
        object.insert("value".to_string(), serde_json::Value::Number(value));
        object.insert(
            "timestamp".to_string(),
            serde_json::Value::String(Self::timestamp_to_rfc3339(sample.value.0)?),
        );

        serde_json::to_vec(&serde_json::Value::Object(object)).map_err(|source| {
            Report::new(PrometheusSourceError::EncodeSample).attach_printable(source.to_string())
        })
    }

    fn timestamp_to_rfc3339(timestamp: f64) -> Result<String, Report<PrometheusSourceError>> {
        if !timestamp.is_finite() {
            return Err(Report::new(PrometheusSourceError::InvalidTimestamp));
        }
        let secs: i64 = timestamp
            .trunc()
            .checked_approx_into()
            .ok_or_else(|| Report::new(PrometheusSourceError::InvalidTimestamp))?;
        let nanos: u32 = (timestamp.fract().abs() * 1_000_000_000.0)
            .round()
            .checked_approx_into()
            .verified("a fractional part scaled by a billion stays inside the u32 range");
        let datetime = Utc
            .timestamp_opt(secs, nanos.min(999_999_999))
            .single()
            .ok_or_else(|| Report::new(PrometheusSourceError::InvalidTimestamp))?;
        Ok(datetime.to_rfc3339())
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[test]
    fn prometheus_source_reads_address_and_validates_client_configuration() {
        let address = vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "http://prometheus:9090".to_string(),
        }];
        assert_eq!(
            PrometheusSource::addr_from_config(&address).expect("address"),
            "http://prometheus:9090"
        );
        assert!(
            PrometheusSource::addr_from_config(&[])
                .expect_err("missing Prometheus address")
                .to_string()
                .contains("missing Prometheus client config key 'addr'")
        );

        let timeout = vec![ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "oops".to_string(),
        }];
        let error =
            PrometheusSource::client_from_config(&timeout).expect_err("invalid Prometheus timeout");
        assert!(error.to_string().contains("Prometheus timeout_ms"));
    }

    #[test]
    fn prometheus_helpers_render_payload_and_validate_inputs() {
        let sample = PrometheusVectorResult {
            metric: BTreeMap::from([("source".to_string(), "local".to_string())]),
            value: (1_735_782_245.25, "12.5".to_string()),
        };

        let timestamp = PrometheusSource::timestamp_to_rfc3339(sample.value.0).expect("valid ts");
        assert!(timestamp.starts_with("2025-"));

        let payload = PrometheusSource::sample_payload(&sample).expect("must render");
        let value: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
        assert_eq!(value["source"], "local");
        assert_eq!(value["value"], 12.5);
        assert_eq!(value["timestamp"], timestamp);

        let bad_value = PrometheusVectorResult {
            metric: BTreeMap::new(),
            value: (1.0, "NaN".to_string()),
        };
        assert!(PrometheusSource::sample_payload(&bad_value).is_err());
        assert!(PrometheusSource::timestamp_to_rfc3339(f64::INFINITY).is_err());
    }

    #[test]
    fn prometheus_query_time_keeps_every_nanosecond_digit() {
        let render = |unix_nanos: i64| {
            PrometheusSource::query_time_seconds(Timestamp::from_unix_nanos(unix_nanos))
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
        let url = PrometheusSource::query_url(
            "http://prometheus:9090/base/?stale=true",
            vec![("query".to_string(), "vector(1)".to_string())],
        )
        .expect("must build url");
        assert_eq!(
            url.as_str(),
            "http://prometheus:9090/base/api/v1/query?query=vector%281%29"
        );
    }

    async fn query_response(
        status: &str,
        body: &str,
    ) -> Result<Vec<PrometheusVectorResult>, Report<PrometheusSourceError>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test Prometheus listener should bind");
        let address = listener
            .local_addr()
            .expect("test Prometheus listener should have an address");
        let status = status.to_string();
        let body = body.to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("test Prometheus listener should accept");
            let mut request = [0_u8; 2048];
            let _ = stream
                .read(&mut request)
                .await
                .expect("test Prometheus request should be readable");
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("test Prometheus response should be writable");
        });
        let source = PrometheusSource {
            client: HttpClient::new(),
            addr: format!("http://{address}"),
            query: "up".to_string(),
        };
        let result = source.query_vector(None).await;
        server
            .await
            .expect("test Prometheus response task should finish");
        result
    }

    #[tokio::test]
    async fn prometheus_query_failures_have_distinct_typed_contexts() {
        let unavailable = PrometheusSource {
            client: HttpClient::new(),
            addr: "http://127.0.0.1:1".to_string(),
            query: "up".to_string(),
        }
        .query_vector(None)
        .await
        .expect_err("an unavailable endpoint must fail the request");
        assert!(matches!(
            unavailable.current_context(),
            PrometheusSourceError::QueryRequest
        ));

        let status = query_response("503 Service Unavailable", "{}")
            .await
            .expect_err("a non-success status must fail");
        assert!(matches!(
            status.current_context(),
            PrometheusSourceError::QueryStatus { .. }
        ));

        let decode = query_response("200 OK", "not-json")
            .await
            .expect_err("a malformed response must fail decoding");
        assert!(matches!(
            decode.current_context(),
            PrometheusSourceError::DecodeResponse
        ));

        let rejected = query_response(
            "200 OK",
            r#"{"status":"error","data":{"resultType":"vector","result":[]}}"#,
        )
        .await
        .expect_err("an unsuccessful Prometheus payload must fail");
        assert!(matches!(
            rejected.current_context(),
            PrometheusSourceError::QueryRejected { .. }
        ));

        let result_type = query_response(
            "200 OK",
            r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
        )
        .await
        .expect_err("a non-vector Prometheus payload must fail");
        assert!(matches!(
            result_type.current_context(),
            PrometheusSourceError::UnsupportedResultType { .. }
        ));
    }

    #[test]
    fn prometheus_helpers_classify_parse_failures() {
        let address = PrometheusSource::query_url("not a URL", Vec::new())
            .expect_err("an invalid address must fail");
        assert!(matches!(
            address.current_context(),
            PrometheusSourceError::InvalidAddress
        ));

        let sample = PrometheusVectorResult {
            metric: BTreeMap::new(),
            value: (1.0, "not-a-number".to_string()),
        };
        let value = PrometheusSource::sample_payload(&sample)
            .expect_err("an invalid sample value must fail");
        assert!(matches!(
            value.current_context(),
            PrometheusSourceError::InvalidSampleValue
        ));
    }
}
