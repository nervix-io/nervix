//! Prometheus paced ingestor connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Prometheus client configuration interpretation, instant queries, the bounded read
//!   of each response, sample parsing, payload encoding and ingestor-boundary arrival observation.
//! - **Depends on.** The connector contract, vocabulary configuration and the Prometheus HTTP
//!   API.
//! - **Must not know.** Domain clocks, runtime collectors, relays, branches, schedules, registry
//!   state or placement computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{TimeZone as _, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::CheckedApproxInto as _;
use nervix_connector::{
    HttpClientConfig, HttpClientConfigError, IngestorConnector, IngestorError, IngestorPoll,
    IngestorPollMessage, IngestorResult, PacedIngestorConnector, RetainedIngestHeaders,
    physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, PayloadSizeLimit, PrometheusApiRoot, Timestamp};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

const PROMETHEUS: &str = "prometheus";

pub struct PrometheusQueryIngestorPlan {
    pub config: Vec<ClientConfigEntry>,
    pub query: String,
    /// The most bytes of one query response a poll reads.
    pub max_body_size: PayloadSizeLimit,
    pub dns: DnsResolver,
}

pub struct PrometheusQueryIngestor {
    client: HttpClient,
    api_root: PrometheusApiRoot,
    query: String,
    max_body_size: PayloadSizeLimit,
}

#[derive(Debug, Error)]
pub enum PrometheusQueryIngestorError {
    #[error("failed to send Prometheus query")]
    QueryRequest,
    #[error("Prometheus query failed with HTTP status {status}")]
    QueryStatus { status: reqwest::StatusCode },
    /// The response announced or carried more than the ingestor's `MAX BODY SIZE`. It names the
    /// bound and never the body.
    #[error("Prometheus query response exceeds MAX BODY SIZE {limit}")]
    ResponseTooLarge { limit: PayloadSizeLimit },
    #[error("failed to read Prometheus query response")]
    ReadResponse,
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
impl IngestorConnector for PrometheusQueryIngestor {
    const KIND: nervix_models::IngestorEntityKind = nervix_models::IngestorEntityKind::Prometheus(
        nervix_models::PrometheusIngestorEntity::Query,
    );

    type Plan = PrometheusQueryIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        let api_root = PrometheusApiRoot::of_client_config(&plan.config).change_context(
            IngestorError::Open {
                connector: PROMETHEUS,
            },
        )?;
        let client = Self::client_from_config(&plan.config, &plan.dns).change_context(
            IngestorError::Open {
                connector: PROMETHEUS,
            },
        )?;
        Ok(Self {
            client,
            api_root,
            query: plan.query.clone(),
            max_body_size: plan.max_body_size,
        })
    }
}

#[async_trait]
impl PacedIngestorConnector for PrometheusQueryIngestor {
    async fn poll(&mut self, scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        let samples =
            self.query_vector(Some(scheduled_at))
                .await
                .change_context(IngestorError::Read {
                    connector: PROMETHEUS,
                })?;
        let mut messages = Vec::with_capacity(samples.len());
        let mut failures = Vec::new();
        for sample in samples {
            nervix_primitives::task::consume_budget().await;
            match Self::sample_payload(&sample) {
                Ok(payload) => messages.push(IngestorPollMessage {
                    payload,
                    headers: RetainedIngestHeaders::none(),
                    metadata: nervix_connector::RetainedIngestMetadata::none(),
                }),
                Err(error) => failures.push(error.change_context(IngestorError::Read {
                    connector: PROMETHEUS,
                })),
            }
        }
        Ok(IngestorPoll {
            messages,
            failures,
            observed_at: actual_utc_now(),
        })
    }
}

impl PrometheusQueryIngestor {
    fn client_from_config(
        config: &[ClientConfigEntry],
        dns: &DnsResolver,
    ) -> Result<HttpClient, Report<HttpClientConfigError>> {
        HttpClientConfig::new(config, "Prometheus", dns).build()
    }

    async fn query_vector(
        &self,
        query_time: Option<Timestamp>,
    ) -> Result<Vec<PrometheusVectorResult>, Report<PrometheusQueryIngestorError>> {
        let mut params = vec![("query".to_string(), self.query.clone())];
        if let Some(query_time) = query_time {
            params.push(("time".to_string(), Self::query_time_seconds(query_time)));
        }
        let url = Self::query_url(&self.api_root, params);
        let response = self.client.get(url).send().await.map_err(|source| {
            Report::new(PrometheusQueryIngestorError::QueryRequest)
                .attach_printable(source.to_string())
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(Report::new(PrometheusQueryIngestorError::QueryStatus {
                status,
            }));
        }
        let body = Self::bounded_body(response, self.max_body_size).await?;
        // The decoder's own message can quote the payload, so it stays an attachment and out of
        // the transient error the ingestor reports.
        let payload =
            serde_json::from_slice::<PrometheusQueryResponse>(&body).map_err(|source| {
                Report::new(PrometheusQueryIngestorError::DecodeResponse)
                    .attach_printable(source.to_string())
            })?;
        if payload.status != "success" {
            return Err(Report::new(PrometheusQueryIngestorError::QueryRejected {
                status: payload.status,
            }));
        }
        if payload.data.result_type != "vector" {
            return Err(Report::new(
                PrometheusQueryIngestorError::UnsupportedResultType {
                    result_type: payload.data.result_type,
                },
            ));
        }
        Ok(payload.data.result)
    }

    /// The body of `response`, read only while it stays within `limit`.
    ///
    /// A response that announces a longer body fails before any of it is read, and one that
    /// carries more than it announced, or announces nothing, fails at the first chunk that takes it
    /// past the bound, so a poll never holds more than `limit` bytes of it. The client's
    /// `timeout_ms` still bounds the whole read.
    async fn bounded_body(
        mut response: reqwest::Response,
        limit: PayloadSizeLimit,
    ) -> Result<Vec<u8>, Report<PrometheusQueryIngestorError>> {
        let limit_bytes = limit.bytes().get();
        if let Some(announced) = response.content_length()
            && announced > limit_bytes
        {
            return Err(Report::new(
                PrometheusQueryIngestorError::ResponseTooLarge { limit },
            ));
        }
        let mut body = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let chunk = match response.chunk().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => return Ok(body),
                Err(source) => {
                    return Err(Report::new(source.without_url())
                        .change_context(PrometheusQueryIngestorError::ReadResponse));
                }
            };
            let read = u64::try_from(body.len())
                .assured("a length held in memory fits u64 on every supported target");
            let remaining = limit_bytes
                .checked_sub(read)
                .verified("every chunk taken so far kept the body within the limit");
            let chunk_bytes = u64::try_from(chunk.len())
                .assured("a length held in memory fits u64 on every supported target");
            if chunk_bytes > remaining {
                return Err(Report::new(
                    PrometheusQueryIngestorError::ResponseTooLarge { limit },
                ));
            }
            body.extend_from_slice(&chunk);
        }
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

    /// The instant-query endpoint beneath `api_root`, carrying `params` as its query. The root
    /// has no query of its own, so the endpoint carries exactly these.
    fn query_url(api_root: &PrometheusApiRoot, params: Vec<(String, String)>) -> Url {
        let mut url = api_root.url().clone();
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/api/v1/query");
        url.set_path(&path);
        url.query_pairs_mut().extend_pairs(params);
        url
    }

    fn sample_payload(
        sample: &PrometheusVectorResult,
    ) -> Result<Vec<u8>, Report<PrometheusQueryIngestorError>> {
        let mut object = serde_json::Map::new();
        for (key, value) in &sample.metric {
            object.insert(key.clone(), serde_json::Value::String(value.clone()));
        }

        let value = sample.value.1.parse::<f64>().map_err(|source| {
            Report::new(PrometheusQueryIngestorError::InvalidSampleValue)
                .attach_printable(source.to_string())
        })?;
        let value = serde_json::Number::from_f64(value)
            .ok_or_else(|| Report::new(PrometheusQueryIngestorError::NonFiniteSampleValue))?;
        object.insert("value".to_string(), serde_json::Value::Number(value));
        object.insert(
            "timestamp".to_string(),
            serde_json::Value::String(Self::timestamp_to_rfc3339(sample.value.0)?),
        );

        serde_json::to_vec(&serde_json::Value::Object(object)).map_err(|source| {
            Report::new(PrometheusQueryIngestorError::EncodeSample)
                .attach_printable(source.to_string())
        })
    }

    fn timestamp_to_rfc3339(
        timestamp: f64,
    ) -> Result<String, Report<PrometheusQueryIngestorError>> {
        if !timestamp.is_finite() {
            return Err(Report::new(PrometheusQueryIngestorError::InvalidTimestamp));
        }
        let secs: i64 = timestamp
            .trunc()
            .checked_approx_into()
            .ok_or_else(|| Report::new(PrometheusQueryIngestorError::InvalidTimestamp))?;
        let nanos: u32 = (timestamp.fract().abs() * 1_000_000_000.0)
            .round()
            .checked_approx_into()
            .verified("a fractional part scaled by a billion stays inside the u32 range");
        let datetime = Utc
            .timestamp_opt(secs, nanos.min(999_999_999))
            .single()
            .ok_or_else(|| Report::new(PrometheusQueryIngestorError::InvalidTimestamp))?;
        Ok(datetime.to_rfc3339())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroU64;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[nervix_primitives::test]
    async fn prometheus_source_opens_only_with_a_config_its_client_declares()
    -> Result<(), Report<nervix_dns::DnsConfigurationError>> {
        let dns = DnsResolver::load(nervix_dns::DnsConfiguration::system()).await?;
        let plan = |config: Vec<ClientConfigEntry>| PrometheusQueryIngestorPlan {
            config,
            query: "up".to_string(),
            max_body_size: nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
            dns: dns.clone(),
        };
        let entry = |key: &str, value: &str| ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        };

        let source = PrometheusQueryIngestor::open(
            &plan(vec![
                entry("addr", "http://prometheus:9090/base"),
                entry("timeout_ms", "5000"),
            ]),
            0,
        )
        .await
        .expect("a declared configuration opens");
        assert_eq!(
            source.api_root.url().as_str(),
            "http://prometheus:9090/base"
        );

        let Err(refused) = PrometheusQueryIngestor::open(
            &plan(vec![entry("addr", "http://prometheus:9090/?stale=true")]),
            0,
        )
        .await
        else {
            panic!("an address with a query string must not open");
        };
        assert!(matches!(
            refused.current_context(),
            IngestorError::Open { .. }
        ));
        assert!(
            refused
                .downcast_ref::<nervix_models::PrometheusClientConfigError>()
                .is_some()
        );

        let timeout = vec![ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "oops".to_string(),
        }];
        let error = PrometheusQueryIngestor::client_from_config(&timeout, &dns)
            .expect_err("invalid Prometheus timeout");
        assert!(error.to_string().contains("Prometheus timeout_ms"));
        Ok(())
    }

    #[test]
    fn prometheus_helpers_render_payload_and_validate_inputs() {
        let sample = PrometheusVectorResult {
            metric: BTreeMap::from([("source".to_string(), "local".to_string())]),
            value: (1_735_782_245.25, "12.5".to_string()),
        };

        let timestamp =
            PrometheusQueryIngestor::timestamp_to_rfc3339(sample.value.0).expect("valid ts");
        assert!(timestamp.starts_with("2025-"));

        let payload = PrometheusQueryIngestor::sample_payload(&sample).expect("must render");
        let value: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
        assert_eq!(value["source"], "local");
        assert_eq!(value["value"], 12.5);
        assert_eq!(value["timestamp"], timestamp);

        let bad_value = PrometheusVectorResult {
            metric: BTreeMap::new(),
            value: (1.0, "NaN".to_string()),
        };
        assert!(PrometheusQueryIngestor::sample_payload(&bad_value).is_err());
        assert!(PrometheusQueryIngestor::timestamp_to_rfc3339(f64::INFINITY).is_err());
    }

    #[test]
    fn prometheus_query_time_keeps_every_nanosecond_digit() {
        let render = |unix_nanos: i64| {
            PrometheusQueryIngestor::query_time_seconds(Timestamp::from_unix_nanos(unix_nanos))
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
    fn prometheus_query_url_extends_the_api_root_path() {
        let api_root = PrometheusApiRoot::parse("http://prometheus:9090/base/")
            .expect("the test address is an API root");
        let url = PrometheusQueryIngestor::query_url(
            &api_root,
            vec![("query".to_string(), "vector(1)".to_string())],
        );
        assert_eq!(
            url.as_str(),
            "http://prometheus:9090/base/api/v1/query?query=vector%281%29"
        );
    }

    /// How a test server's response carries its body.
    #[derive(Debug, Clone, Copy)]
    enum Framing {
        /// Behind a `Content-Length` header that announces its length.
        Announced,
        /// In chunks of a few bytes, with no announced length.
        Chunked,
        /// Behind a `Content-Length` header that announces the whole body, of which the server
        /// sends only the first half before it closes the connection.
        Truncated,
    }

    impl Framing {
        /// The whole response, its status line and headers included.
        fn response(self, status: &str, body: &str) -> String {
            match self {
                Self::Announced => format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
                     {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ),
                Self::Chunked => {
                    let mut response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: \
                         application/json\r\nTransfer-Encoding: chunked\r\nConnection: \
                         close\r\n\r\n"
                    );
                    for piece in body.as_bytes().chunks(7) {
                        let piece = std::str::from_utf8(piece).expect("the test bodies are ASCII");
                        response.push_str(&format!("{:x}\r\n{piece}\r\n", piece.len()));
                    }
                    response.push_str("0\r\n\r\n");
                    response
                }
                Self::Truncated => {
                    let sent = &body[..body.len() / 2];
                    format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
                         {}\r\nConnection: close\r\n\r\n{sent}",
                        body.len()
                    )
                }
            }
        }
    }

    async fn query_response(
        status: &str,
        body: &str,
    ) -> Result<Vec<PrometheusVectorResult>, Report<PrometheusQueryIngestorError>> {
        query_response_within(
            status,
            body,
            Framing::Announced,
            nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        )
        .await
    }

    /// What an ingestor bounded by `max_body_size` reads from a server that answers its one query
    /// with `status` and `body`, framed as `framing` says.
    async fn query_response_within(
        status: &str,
        body: &str,
        framing: Framing,
        max_body_size: PayloadSizeLimit,
    ) -> Result<Vec<PrometheusVectorResult>, Report<PrometheusQueryIngestorError>> {
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test Prometheus listener should bind");
        let address = listener
            .local_addr()
            .expect("test Prometheus listener should have an address");
        let response = framing.response(status, body);
        let server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("test Prometheus listener should accept");
            let mut request = [0_u8; 2048];
            let _ = stream
                .read(&mut request)
                .await
                .expect("test Prometheus request should be readable");
            stream
                .write_all(response.as_bytes())
                .await
                .expect("test Prometheus response should be writable");
        });
        let source = PrometheusQueryIngestor {
            client: HttpClient::new(),
            api_root: PrometheusApiRoot::parse(&format!("http://{address}"))
                .expect("the test listener's address is an API root"),
            query: "up".to_string(),
            max_body_size,
        };
        let result = source.query_vector(None).await;
        server
            .await
            .expect("test Prometheus response task should finish");
        result
    }

    /// A body of exactly its bound is read whole, however it is framed, and one byte more fails
    /// the poll with the typed error that names the bound and nothing of the body.
    #[nervix_primitives::test]
    async fn a_response_is_read_only_while_it_stays_within_max_body_size() {
        let body = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"source":"bounded"},"value":[1735782245.25,"12.5"]}]}}"#;
        let bytes = u64::try_from(body.len()).expect("the test body's length fits u64");
        let one_short = bytes
            .checked_sub(1)
            .expect("the test body is longer than one byte");
        let exact = PayloadSizeLimit::new(
            NonZeroU64::new(bytes).expect("the test body is not empty"),
            nervix_models::ByteSizeUnit::B,
        )
        .expect("the test body's length is a byte count");
        let short = PayloadSizeLimit::new(
            NonZeroU64::new(one_short).expect("the test body is longer than one byte"),
            nervix_models::ByteSizeUnit::B,
        )
        .expect("one byte less is a byte count");

        for framing in [Framing::Announced, Framing::Chunked] {
            let samples = match query_response_within("200 OK", body, framing, exact).await {
                Ok(samples) => samples,
                Err(error) => panic!("{framing:?}: a body of its bound reads: {error:?}"),
            };
            assert_eq!(samples.len(), 1, "{framing:?}");
            assert_eq!(
                samples[0].metric.get("source").map(String::as_str),
                Some("bounded"),
                "{framing:?}"
            );

            let refused = query_response_within("200 OK", body, framing, short)
                .await
                .expect_err("a body one byte over its bound fails the poll");
            assert!(
                matches!(
                    refused.current_context(),
                    PrometheusQueryIngestorError::ResponseTooLarge { limit } if *limit == short
                ),
                "{framing:?}: {refused:?}"
            );
            assert_eq!(
                format!("{refused:#}"),
                format!("Prometheus query response exceeds MAX BODY SIZE {one_short}B"),
                "{framing:?}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn prometheus_query_failures_have_distinct_typed_contexts() {
        let unavailable = PrometheusQueryIngestor {
            client: HttpClient::new(),
            api_root: PrometheusApiRoot::parse("http://127.0.0.1:1")
                .expect("the unused port's address is an API root"),
            query: "up".to_string(),
            max_body_size: nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        }
        .query_vector(None)
        .await
        .expect_err("an unavailable endpoint must fail the request");
        assert!(matches!(
            unavailable.current_context(),
            PrometheusQueryIngestorError::QueryRequest
        ));

        let status = query_response("503 Service Unavailable", "{}")
            .await
            .expect_err("a non-success status must fail");
        assert!(matches!(
            status.current_context(),
            PrometheusQueryIngestorError::QueryStatus { .. }
        ));

        let truncated = query_response_within(
            "200 OK",
            r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#,
            Framing::Truncated,
            nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        )
        .await
        .expect_err("a body that ends before its announced length must fail the read");
        assert!(
            matches!(
                truncated.current_context(),
                PrometheusQueryIngestorError::ReadResponse
            ),
            "{truncated:?}"
        );
        assert!(
            truncated.downcast_ref::<reqwest::Error>().is_some(),
            "the transport's failure stays beneath the read failure: {truncated:?}"
        );

        let decode = query_response("200 OK", "not-json")
            .await
            .expect_err("a malformed response must fail decoding");
        assert!(matches!(
            decode.current_context(),
            PrometheusQueryIngestorError::DecodeResponse
        ));

        let rejected = query_response(
            "200 OK",
            r#"{"status":"error","data":{"resultType":"vector","result":[]}}"#,
        )
        .await
        .expect_err("an unsuccessful Prometheus payload must fail");
        assert!(matches!(
            rejected.current_context(),
            PrometheusQueryIngestorError::QueryRejected { .. }
        ));

        let result_type = query_response(
            "200 OK",
            r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
        )
        .await
        .expect_err("a non-vector Prometheus payload must fail");
        assert!(matches!(
            result_type.current_context(),
            PrometheusQueryIngestorError::UnsupportedResultType { .. }
        ));
    }

    #[test]
    fn prometheus_helpers_classify_parse_failures() {
        let sample = PrometheusVectorResult {
            metric: BTreeMap::new(),
            value: (1.0, "not-a-number".to_string()),
        };
        let value = PrometheusQueryIngestor::sample_payload(&sample)
            .expect_err("an invalid sample value must fail");
        assert!(matches!(
            value.current_context(),
            PrometheusQueryIngestorError::InvalidSampleValue
        ));
    }
}
