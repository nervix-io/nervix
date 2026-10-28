//! HTTP paced ingestor and request emitter connectors.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** HTTP client configuration interpretation, polling, response bodies, response-header
//!   semantics, and ingestor-boundary arrival observation for the ingestor; sending prepared requests
//!   and answering for each of them for the emitter.
//! - **Depends on.** The connector contract, HTTP client settings, vocabulary configuration and
//!   request fields, and `reqwest`.
//! - **Must not know.** Domain clocks, runtime collectors, relays, branches, schedules, registry
//!   state, or placement computation.

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

mod emitter;

use async_trait::async_trait;
pub use emitter::{HttpRequestEmitter, HttpRequestEmitterConfig};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ClientConfigResult, HttpClientConfig, IngestMessageHeaders, IngestorConnector, IngestorError,
    IngestorPoll, IngestorPollMessage, IngestorPollMetadata, IngestorResult,
    PacedIngestorConnector, RetainedIngestHeaders, client_config_value,
    physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, HttpRequestHeader, PayloadSizeLimit, Timestamp};
use reqwest::{
    Client as HttpClient, Method, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use thiserror::Error;

const HTTP: &str = "http";

pub struct HttpRequestIngestorPlan {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
    pub method: String,
    pub headers: Vec<HttpRequestHeader>,
    pub max_body_size: PayloadSizeLimit,
}

pub struct HttpRequestIngestor {
    client: HttpClient,
    endpoint: String,
    method: Method,
    headers: HeaderMap,
    max_body_size: PayloadSizeLimit,
}

#[derive(Debug, Error)]
pub enum HttpRequestIngestorError {
    #[error("invalid HTTP method")]
    InvalidMethod,
    #[error("invalid HTTP request header")]
    InvalidHeader,
    #[error("failed to send HTTP ingestor request")]
    Request,
    #[error("HTTP ingestor returned status {status}")]
    ResponseStatus { status: StatusCode },
    #[error("failed to read HTTP ingestor response body")]
    ResponseBody,
    #[error("HTTP ingestor response exceeds MAX BODY SIZE {limit}")]
    ResponseTooLarge { limit: PayloadSizeLimit },
}

/// The headers of one borrowed HTTP response, skipping values that are not UTF-8.
struct HttpResponseHeaders<'a>(&'a HeaderMap);

impl IngestMessageHeaders for HttpResponseHeaders<'_> {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for (name, value) in self.0 {
            if let Ok(value) = value.to_str() {
                visit(name.as_str(), value);
            }
        }
    }
}

#[async_trait]
impl IngestorConnector for HttpRequestIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Http(nervix_models::HttpIngestorEntity::Request);

    type Plan = HttpRequestIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        let endpoint = Self::endpoint_from_config(&plan.config)
            .change_context(IngestorError::Open { connector: HTTP })?;
        let method = Method::from_bytes(plan.method.as_bytes())
            .map_err(|source| {
                Report::new(source).change_context(HttpRequestIngestorError::InvalidMethod)
            })
            .change_context(IngestorError::Open { connector: HTTP })?;
        let mut headers = HeaderMap::new();
        for header in &plan.headers {
            let name = HeaderName::from_bytes(header.name.as_bytes())
                .map_err(|source| {
                    Report::new(source).change_context(HttpRequestIngestorError::InvalidHeader)
                })
                .change_context(IngestorError::Open { connector: HTTP })?;
            let value = HeaderValue::from_str(&header.value)
                .map_err(|source| {
                    Report::new(source).change_context(HttpRequestIngestorError::InvalidHeader)
                })
                .change_context(IngestorError::Open { connector: HTTP })?;
            headers.insert(name, value);
        }
        let client = HttpClientConfig::new(&plan.config, "HTTP", &plan.dns)
            .build_http_request()
            .change_context(IngestorError::Open { connector: HTTP })?;
        Ok(Self {
            client,
            endpoint,
            method,
            headers,
            max_body_size: plan.max_body_size,
        })
    }
}

#[async_trait]
impl PacedIngestorConnector for HttpRequestIngestor {
    async fn poll(&mut self, scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        let response = self
            .client
            .request(self.method.clone(), self.endpoint.as_str())
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|source| {
                Report::new(source.without_url()).change_context(HttpRequestIngestorError::Request)
            })
            .change_context(IngestorError::Read { connector: HTTP })?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(IngestorPoll {
                messages: Vec::new(),
                failures: Vec::new(),
                observed_at: actual_utc_now(),
            });
        }
        if !response.status().is_success() {
            return Err(Report::new(HttpRequestIngestorError::ResponseStatus {
                status: response.status(),
            })
            .change_context(IngestorError::Read { connector: HTTP }));
        }

        let headers = RetainedIngestHeaders::capture(&HttpResponseHeaders(response.headers()));
        let status = response.status().as_u16();
        let payload = Self::bounded_body(response, self.max_body_size)
            .await
            .change_context(IngestorError::Read { connector: HTTP })?;
        Ok(IngestorPoll {
            messages: vec![IngestorPollMessage {
                payload,
                headers,
                metadata: IngestorPollMetadata::Http {
                    status,
                    polled_at: scheduled_at,
                },
            }],
            failures: Vec::new(),
            observed_at: actual_utc_now(),
        })
    }
}

impl HttpRequestIngestor {
    fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "HTTP")
    }

    async fn bounded_body(
        mut response: reqwest::Response,
        limit: PayloadSizeLimit,
    ) -> Result<Vec<u8>, Report<HttpRequestIngestorError>> {
        let limit_bytes = limit.bytes().get();
        if response
            .content_length()
            .is_some_and(|size| size > limit_bytes)
        {
            return Err(Report::new(HttpRequestIngestorError::ResponseTooLarge {
                limit,
            }));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|source| {
            Report::new(source.without_url()).change_context(HttpRequestIngestorError::ResponseBody)
        })? {
            nervix_primitives::task::consume_budget().await;
            let read = u64::try_from(body.len())
                .assured("a length held in memory fits u64 on every supported target");
            let remaining = limit_bytes
                .checked_sub(read)
                .verified("every chunk taken so far kept the body within the limit");
            let chunk_bytes = u64::try_from(chunk.len())
                .assured("a length held in memory fits u64 on every supported target");
            if chunk_bytes > remaining {
                return Err(Report::new(HttpRequestIngestorError::ResponseTooLarge {
                    limit,
                }));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use reqwest::header::{HeaderName, HeaderValue};
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    use super::*;

    /// HTTP REQUEST ingestors declare repeats by name with lowercase names: a polled response's
    /// fields are visited under their lowercase names, every value of a repeated field together
    /// and in the order the response carried them.
    #[test]
    fn response_fields_are_visited_lowercase_with_every_value_of_a_field_together() {
        let mut fields = HeaderMap::new();
        for (name, value) in [
            ("X-Trace", "a"),
            ("Tenant", "x"),
            ("x-trace", "b"),
            ("TENANT", "y"),
        ] {
            fields.append(
                HeaderName::from_bytes(name.as_bytes()).expect("each name is a valid field name"),
                HeaderValue::from_static(value),
            );
        }
        let mut visited: Vec<(String, String)> = Vec::new();
        HttpResponseHeaders(&fields)
            .visit(&mut |name, value| visited.push((name.to_string(), value.to_string())));

        // The order of different names is not declared, so only each name's values are compared,
        // after checking that a name's values arrive together.
        let mut finished = std::collections::BTreeSet::new();
        let mut current: Option<&str> = None;
        let mut values: std::collections::BTreeMap<&str, Vec<&str>> =
            std::collections::BTreeMap::new();
        for (name, value) in &visited {
            if current != Some(name.as_str()) {
                if let Some(previous) = current {
                    finished.insert(previous);
                }
                assert!(
                    !finished.contains(name.as_str()),
                    "every value of {name} is visited together: {visited:?}"
                );
                current = Some(name);
            }
            values
                .entry(name.as_str())
                .or_default()
                .push(value.as_str());
        }
        let expected = std::collections::BTreeMap::from([
            ("tenant", vec!["x", "y"]),
            ("x-trace", vec!["a", "b"]),
        ]);
        assert_eq!(values, expected);
    }

    #[test]
    fn http_source_reads_endpoint_configuration() {
        let config = vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "https://example.com/api".to_string(),
        }];
        assert_eq!(
            HttpRequestIngestor::endpoint_from_config(&config).expect("endpoint"),
            "https://example.com/api"
        );
        assert!(
            HttpRequestIngestor::endpoint_from_config(&[])
                .expect_err("missing HTTP endpoint")
                .to_string()
                .contains("missing HTTP client config key 'endpoint'")
        );
    }

    async fn response_from_wire(wire: &'static [u8]) -> reqwest::Response {
        let listener =
            nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .expect("loopback listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("request connection");
            let mut request = tokio::io::BufReader::new(&mut stream);
            let mut line = Vec::new();
            loop {
                line.clear();
                assert!(
                    request
                        .read_until(b'\n', &mut line)
                        .await
                        .expect("request line")
                        > 0,
                    "request ends with an HTTP head"
                );
                if line == b"\r\n" {
                    break;
                }
            }
            drop(request);
            stream.write_all(wire).await.expect("response bytes");
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test client")
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("response headers");
        server.await.expect("response server");
        response
    }

    #[nervix_primitives::test]
    async fn response_body_limit_checks_both_declared_and_streamed_lengths() {
        let limit = "4B".parse().expect("body limit");
        for wire in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"[..],
            &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"[..],
        ] {
            let response = response_from_wire(wire).await;
            let error = HttpRequestIngestor::bounded_body(response, limit)
                .await
                .expect_err("five bytes exceed the four-byte limit");
            assert!(matches!(
                error.current_context(),
                HttpRequestIngestorError::ResponseTooLarge { limit: found } if *found == limit
            ));
        }
        let response =
            response_from_wire(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nbody").await;
        assert_eq!(
            HttpRequestIngestor::bounded_body(response, limit)
                .await
                .expect("body at the bound"),
            b"body"
        );
    }
}
