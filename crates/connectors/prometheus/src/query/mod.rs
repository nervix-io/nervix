//! Prometheus paced ingestor connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Prometheus client configuration interpretation, instant queries, the bounded read
//!   of each response, the metadata of a poll and ingestor-boundary arrival observation. Reading an
//!   answer into samples belongs to [`answer`].
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

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    HttpClientConfig, HttpClientConfigError, IngestorConnector, IngestorError, IngestorPoll,
    IngestorPollMessage, IngestorResult, PacedIngestorConnector, RetainedIngestHeaders,
    RetainedIngestMetadata, RetainedIngestMetadataValue, physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::{
    ClientConfigEntry, PayloadSizeLimit, PrometheusApiRoot, PrometheusUnsupportedResult, Timestamp,
};
use reqwest::Client as HttpClient;
use thiserror::Error;
use url::Url;

mod answer;
use answer::PrometheusQueryResponse;

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
    /// An answer that is not the API's own, such as a proxy's page, has only its status.
    #[error("Prometheus query failed with HTTP status {status}")]
    QueryStatus { status: reqwest::StatusCode },
    /// The API refused the query, in the words of its own `errorType` and `error`. The answer that
    /// holds them was read within `MAX BODY SIZE`.
    #[error("Prometheus rejected the query with HTTP status {status}: {error_type}: {error}")]
    QueryRejected {
        status: reqwest::StatusCode,
        error_type: String,
        error: String,
    },
    /// The response announced or carried more than the ingestor's `MAX BODY SIZE`. It names the
    /// bound and never the body.
    #[error("Prometheus query response exceeds MAX BODY SIZE {limit}")]
    ResponseTooLarge { limit: PayloadSizeLimit },
    #[error("failed to read Prometheus query response")]
    ReadResponse,
    #[error("failed to decode Prometheus query response")]
    DecodeResponse,
    #[error("Prometheus query returned unsupported result type '{result_type}'")]
    UnsupportedResultType {
        result_type: PrometheusUnsupportedResult,
    },
    #[error("invalid Prometheus sample value")]
    InvalidSampleValue,
    #[error("failed to encode Prometheus sample")]
    EncodeSample,
    #[error("invalid Prometheus timestamp")]
    InvalidTimestamp,
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
        let read = || IngestorError::Read {
            connector: PROMETHEUS,
        };
        let response = self
            .query_at(scheduled_at)
            .await
            .change_context_lazy(read)?;
        let samples = response.samples().change_context_lazy(read)?;
        let metadata = Self::poll_metadata(&self.query, scheduled_at);
        let mut messages = Vec::with_capacity(samples.len());
        let mut failures = Vec::new();
        for sample in samples {
            nervix_primitives::task::consume_budget().await;
            match sample.payload() {
                Ok(payload) => messages.push(IngestorPollMessage {
                    payload,
                    headers: RetainedIngestHeaders::none(),
                    metadata: metadata.clone(),
                }),
                Err(error) => failures.push(error.change_context(read())),
            }
        }
        Ok(IngestorPoll {
            messages,
            failures,
            dropped_header_values: 0,
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

    /// The values of the fields a query source declares, in their declared order, for one poll:
    /// the same for every sample of it, so its messages share one copy however many there are.
    fn poll_metadata(query: &str, evaluated_at: Timestamp) -> RetainedIngestMetadata {
        RetainedIngestMetadata::from_values(vec![
            RetainedIngestMetadataValue::String(query.to_string()),
            RetainedIngestMetadataValue::Datetime(evaluated_at),
        ])
    }

    /// The response to this ingestor's query evaluated at `evaluated_at`, its body read within
    /// `MAX BODY SIZE`.
    ///
    /// The URL carries the PromQL and the instant, and its root can carry credentials, so a request
    /// that fails keeps the transport's typed cause without it.
    async fn query_at(
        &self,
        evaluated_at: Timestamp,
    ) -> Result<PrometheusQueryResponse, Report<PrometheusQueryIngestorError>> {
        let params = vec![
            ("query".to_string(), self.query.clone()),
            ("time".to_string(), Self::query_time_seconds(evaluated_at)),
        ];
        let url = Self::query_url(&self.api_root, params);
        let response = self.client.get(url).send().await.map_err(|source| {
            Report::new(source.without_url())
                .change_context(PrometheusQueryIngestorError::QueryRequest)
        })?;
        let status = response.status();
        match Self::bounded_body(response, self.max_body_size).await {
            Ok(body) => Ok(PrometheusQueryResponse { status, body }),
            Err(unread) if status.is_success() => Err(unread),
            Err(unread) => {
                Err(unread.change_context(PrometheusQueryIngestorError::QueryStatus { status }))
            }
        }
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
    ) -> Result<PrometheusQueryResponse, Report<PrometheusQueryIngestorError>> {
        query_response_within(
            status,
            body,
            Framing::Announced,
            nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        )
        .await
    }

    /// An ingestor of `query`, bounded by `max_body_size`, that queries `address`.
    fn ingestor_of(
        address: std::net::SocketAddr,
        query: &str,
        max_body_size: PayloadSizeLimit,
    ) -> PrometheusQueryIngestor {
        PrometheusQueryIngestor {
            client: HttpClient::new(),
            api_root: PrometheusApiRoot::parse(&format!("http://{address}"))
                .expect("the test listener's address is an API root"),
            query: query.to_string(),
            max_body_size,
        }
    }

    /// Answers one request on `listener` with `response`, and returns what the request said.
    fn answer_once(
        listener: nervix_primitives::net::TcpListener,
        response: String,
    ) -> nervix_primitives::task::JoinHandle<String> {
        nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .expect("test Prometheus listener should accept");
            let mut request = [0_u8; 2048];
            let read = stream
                .read(&mut request)
                .await
                .expect("test Prometheus request should be readable");
            stream
                .write_all(response.as_bytes())
                .await
                .expect("test Prometheus response should be writable");
            String::from_utf8_lossy(&request[..read]).into_owned()
        })
    }

    /// What an ingestor bounded by `max_body_size` reads from a server that answers its one query
    /// with `status` and `body`, framed as `framing` says.
    async fn query_response_within(
        status: &str,
        body: &str,
        framing: Framing,
        max_body_size: PayloadSizeLimit,
    ) -> Result<PrometheusQueryResponse, Report<PrometheusQueryIngestorError>> {
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test Prometheus listener should bind");
        let address = listener
            .local_addr()
            .expect("test Prometheus listener should have an address");
        let server = answer_once(listener, framing.response(status, body));
        let source = ingestor_of(address, "up", max_body_size);
        let result = source.query_at(Timestamp::from_unix_nanos(0)).await;
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
            let response = match query_response_within("200 OK", body, framing, exact).await {
                Ok(response) => response,
                Err(error) => panic!("{framing:?}: a body of its bound reads: {error:?}"),
            };
            assert_eq!(response.body, body.as_bytes(), "{framing:?}");

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
        .query_at(Timestamp::from_unix_nanos(0))
        .await
        .expect_err("an unavailable endpoint must fail the request");
        assert!(matches!(
            unavailable.current_context(),
            PrometheusQueryIngestorError::QueryRequest
        ));
        assert!(
            unavailable.downcast_ref::<reqwest::Error>().is_some(),
            "the transport's failure stays beneath the request failure: {unavailable:?}"
        );

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
    }

    /// A status whose body cannot be read still reports the status, with what stopped the read
    /// beneath it, and the body of a refused query is read within the same bound.
    #[nervix_primitives::test]
    async fn a_refusal_whose_body_cannot_be_read_reports_its_status() {
        let response = query_response("503 Service Unavailable", "{}")
            .await
            .expect("a status with a readable body is a response");
        assert_eq!(response.status, reqwest::StatusCode::SERVICE_UNAVAILABLE);

        let bound = PayloadSizeLimit::new(
            NonZeroU64::new(1).expect("one is not zero"),
            nervix_models::ByteSizeUnit::B,
        )
        .expect("one byte is a byte count");
        let unread = query_response_within(
            "503 Service Unavailable",
            "<html>a page past the bound</html>",
            Framing::Announced,
            bound,
        )
        .await
        .expect_err("a body past the bound is not read");
        assert!(
            matches!(
                unread.current_context(),
                PrometheusQueryIngestorError::QueryStatus { status }
                    if *status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            ),
            "{unread:?}"
        );
        assert!(
            unread
                .downcast_ref::<PrometheusQueryIngestorError>()
                .is_some(),
            "{unread:?}"
        );
        assert_eq!(
            format!("{unread:#}"),
            "Prometheus query failed with HTTP status 503 Service Unavailable: Prometheus query \
             response exceeds MAX BODY SIZE 1B"
        );
    }

    /// One poll against a loopback endpoint: the instant it was scheduled for is the instant it
    /// asks the API to evaluate, every sample is its declared object, and every message of the poll
    /// carries the entity's declared metadata, sharing one copy of it.
    #[nervix_primitives::test]
    async fn a_poll_delivers_every_sample_with_the_metadata_of_that_poll() {
        let body = r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"value":"a label"},"value":[1775520000.123,"1.5"]},{"metric":{},"value":[1775520000.123,"+Inf"]},{"metric":{},"value":[1775520000.123,"not-a-number"]}]}}"#;
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test Prometheus listener should bind");
        let address = listener
            .local_addr()
            .expect("test Prometheus listener should have an address");
        let server = answer_once(listener, Framing::Announced.response("200 OK", body));
        let mut source = ingestor_of(
            address,
            "vector(1)",
            nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        );
        let evaluated_at = Timestamp::from_unix_nanos(1_775_520_000_123_456_789);

        let poll = source
            .poll(evaluated_at)
            .await
            .expect("the answer is a vector");

        let request = server
            .await
            .expect("test Prometheus response task should finish");
        assert!(
            request.starts_with(
                "GET /api/v1/query?query=vector%281%29&time=1775520000.123456789 HTTP/1.1"
            ),
            "{request}"
        );

        let payloads: Vec<&str> = poll
            .messages
            .iter()
            .map(|message| std::str::from_utf8(&message.payload).expect("a payload is JSON"))
            .collect();
        assert_eq!(
            payloads,
            [
                r#"{"metric":{"value":"a label"},"value":1.5,"timestamp":"2026-04-07T00:00:00.123Z"}"#,
                r#"{"metric":{},"value":"+Inf","timestamp":"2026-04-07T00:00:00.123Z"}"#,
            ]
        );
        let [first, second] = poll.messages.as_slice() else {
            panic!("two of the three samples are valid");
        };
        assert_eq!(
            first.metadata.values(),
            [
                RetainedIngestMetadataValue::String("vector(1)".to_string()),
                RetainedIngestMetadataValue::Datetime(evaluated_at),
            ]
        );
        assert_eq!(
            first.metadata.values().as_ptr(),
            second.metadata.values().as_ptr(),
            "the messages of one poll share one copy of its metadata"
        );

        let [failure] = poll.failures.as_slice() else {
            panic!("one of the three samples is not a number");
        };
        assert!(matches!(
            failure.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(matches!(
            failure.downcast_ref::<PrometheusQueryIngestorError>(),
            Some(PrometheusQueryIngestorError::InvalidSampleValue)
        ));
    }

    /// The URL carries the PromQL and the instant, and an API root can carry credentials, so a
    /// failed request's report holds none of them in any rendering of it.
    #[nervix_primitives::test]
    async fn a_failed_request_reports_nothing_of_its_url() {
        let unavailable = PrometheusQueryIngestor {
            client: HttpClient::new(),
            api_root: PrometheusApiRoot::parse("http://reader:hunter2@127.0.0.1:1/behind-a-proxy")
                .expect("the unused port's address is an API root"),
            query: "secret_metric".to_string(),
            max_body_size: nervix_models::PROMETHEUS_DEFAULT_MAX_BODY_SIZE,
        }
        .query_at(Timestamp::from_unix_nanos(1_775_520_000_000_000_000))
        .await
        .expect_err("an unavailable endpoint must fail the request");

        for rendered in [format!("{unavailable:#}"), format!("{unavailable:?}")] {
            assert!(
                rendered.contains("failed to send Prometheus query"),
                "{rendered}"
            );
            for leaked in [
                "api/v1/query",
                "secret_metric",
                "hunter2",
                "behind-a-proxy",
                "1775520000",
            ] {
                assert!(!rendered.contains(leaked), "{leaked} in {rendered}");
            }
        }
    }
}
