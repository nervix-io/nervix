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
use nervix_connector::{
    ClientConfigResult, HttpClientConfig, IngestMessageHeaders, IngestorConnector, IngestorError,
    IngestorPoll, IngestorPollMessage, IngestorResult, PacedIngestorConnector,
    RetainedIngestHeaders, client_config_value, optional_client_config_value,
    physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, Timestamp};
use reqwest::{Client as HttpClient, Method, StatusCode, header::HeaderMap};
use thiserror::Error;

const HTTP: &str = "http";

pub struct HttpRequestIngestorPlan {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

pub struct HttpRequestIngestor {
    client: HttpClient,
    endpoint: String,
    method: Method,
}

#[derive(Debug, Error)]
pub enum HttpRequestIngestorError {
    #[error("invalid HTTP method")]
    InvalidMethod,
    #[error("failed to send HTTP ingestor request")]
    Request,
    #[error("HTTP ingestor returned status {status}")]
    ResponseStatus { status: StatusCode },
    #[error("failed to read HTTP ingestor response body")]
    ResponseBody,
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
        let method = Self::method_from_config(&plan.config)
            .change_context(IngestorError::Open { connector: HTTP })?;
        let client = HttpClientConfig::new(&plan.config, "HTTP", &plan.dns)
            .build()
            .change_context(IngestorError::Open { connector: HTTP })?;
        Ok(Self {
            client,
            endpoint,
            method,
        })
    }
}

#[async_trait]
impl PacedIngestorConnector for HttpRequestIngestor {
    async fn poll(&mut self, _scheduled_at: Timestamp) -> IngestorResult<IngestorPoll> {
        let response = self
            .client
            .request(self.method.clone(), self.endpoint.as_str())
            .send()
            .await
            .map_err(|source| {
                Report::new(HttpRequestIngestorError::Request).attach_printable(source.to_string())
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
        let payload = response
            .bytes()
            .await
            .map_err(|source| {
                Report::new(HttpRequestIngestorError::ResponseBody)
                    .attach_printable(source.to_string())
            })
            .change_context(IngestorError::Read { connector: HTTP })?;
        Ok(IngestorPoll {
            messages: vec![IngestorPollMessage {
                payload: payload.to_vec(),
                headers,
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

    fn method_from_config(
        config: &[ClientConfigEntry],
    ) -> Result<Method, Report<HttpRequestIngestorError>> {
        let method = optional_client_config_value(config, "method").unwrap_or("GET");
        Method::from_bytes(method.as_bytes())
            .map_err(|_| Report::new(HttpRequestIngestorError::InvalidMethod))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use reqwest::header::{HeaderName, HeaderValue};

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
    fn http_source_reads_endpoint_and_method_configuration() {
        let config = vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "https://example.com/api".to_string(),
        }];
        assert_eq!(
            HttpRequestIngestor::endpoint_from_config(&config).expect("endpoint"),
            "https://example.com/api"
        );
        assert_eq!(
            HttpRequestIngestor::method_from_config(&config).expect("default method"),
            Method::GET
        );

        let post = vec![ClientConfigEntry {
            key: "method".to_string(),
            value: "POST".to_string(),
        }];
        assert_eq!(
            HttpRequestIngestor::method_from_config(&post).expect("post method"),
            Method::POST
        );

        let invalid = vec![ClientConfigEntry {
            key: "method".to_string(),
            value: "NOT A METHOD".to_string(),
        }];
        assert!(HttpRequestIngestor::method_from_config(&invalid).is_err());
        assert!(
            HttpRequestIngestor::endpoint_from_config(&[])
                .expect_err("missing HTTP endpoint")
                .to_string()
                .contains("missing HTTP client config key 'endpoint'")
        );
    }
}
