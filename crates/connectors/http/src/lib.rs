//! HTTP paced source connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** HTTP client configuration interpretation, polling, response bodies, response-header
//!   semantics, and source-boundary arrival observation.
//! - **Depends on.** The connector contract, HTTP client settings, vocabulary configuration and
//!   `reqwest`.
//! - **Must not know.** Domain clocks, runtime collectors, relays, branches, schedules, registry
//!   state, or placement computation.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    ClientConfigResult, HttpClientConfig, IngestMessageHeaders, PacedSourceConnector,
    RetainedIngestHeaders, SourceConnector, SourceError, SourcePoll, SourcePollMessage,
    SourceResult, client_config_value, optional_client_config_value, physical_time::actual_utc_now,
};
use nervix_models::{ClientConfigEntry, Timestamp};
use reqwest::{Client as HttpClient, Method, StatusCode, header::HeaderMap};
use thiserror::Error;

const HTTP: &str = "http";

pub struct HttpSourcePlan {
    pub config: Vec<ClientConfigEntry>,
}

pub struct HttpSource {
    client: HttpClient,
    endpoint: String,
    method: Method,
}

#[derive(Debug, Error)]
pub enum HttpSourceError {
    #[error("invalid HTTP method")]
    InvalidMethod,
    #[error("failed to send HTTP source request")]
    Request,
    #[error("HTTP source returned status {status}")]
    ResponseStatus { status: StatusCode },
    #[error("failed to read HTTP source response body")]
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
impl SourceConnector for HttpSource {
    type Plan = HttpSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let endpoint = Self::endpoint_from_config(&plan.config)
            .change_context(SourceError::Open { connector: HTTP })?;
        let method = Self::method_from_config(&plan.config)
            .change_context(SourceError::Open { connector: HTTP })?;
        let client = HttpClientConfig::new(&plan.config, "HTTP")
            .build()
            .change_context(SourceError::Open { connector: HTTP })?;
        Ok(Self {
            client,
            endpoint,
            method,
        })
    }
}

#[async_trait]
impl PacedSourceConnector for HttpSource {
    async fn poll(&mut self, _scheduled_at: Timestamp) -> SourceResult<SourcePoll> {
        let response = self
            .client
            .request(self.method.clone(), self.endpoint.as_str())
            .send()
            .await
            .map_err(|source| {
                Report::new(HttpSourceError::Request).attach_printable(source.to_string())
            })
            .change_context(SourceError::Read { connector: HTTP })?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(SourcePoll {
                messages: Vec::new(),
                failures: Vec::new(),
                observed_at: actual_utc_now(),
            });
        }
        if !response.status().is_success() {
            return Err(Report::new(HttpSourceError::ResponseStatus {
                status: response.status(),
            })
            .change_context(SourceError::Read { connector: HTTP }));
        }

        let headers = RetainedIngestHeaders::capture(&HttpResponseHeaders(response.headers()));
        let payload = response
            .bytes()
            .await
            .map_err(|source| {
                Report::new(HttpSourceError::ResponseBody).attach_printable(source.to_string())
            })
            .change_context(SourceError::Read { connector: HTTP })?;
        Ok(SourcePoll {
            messages: vec![SourcePollMessage {
                payload: payload.to_vec(),
                headers,
            }],
            failures: Vec::new(),
            observed_at: actual_utc_now(),
        })
    }
}

impl HttpSource {
    fn endpoint_from_config(config: &[ClientConfigEntry]) -> ClientConfigResult<String> {
        client_config_value(config, "endpoint", "HTTP")
    }

    fn method_from_config(config: &[ClientConfigEntry]) -> Result<Method, Report<HttpSourceError>> {
        let method = optional_client_config_value(config, "method").unwrap_or("GET");
        Method::from_bytes(method.as_bytes())
            .map_err(|_| Report::new(HttpSourceError::InvalidMethod))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_source_reads_endpoint_and_method_configuration() {
        let config = vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "https://example.com/api".to_string(),
        }];
        assert_eq!(
            HttpSource::endpoint_from_config(&config).expect("endpoint"),
            "https://example.com/api"
        );
        assert_eq!(
            HttpSource::method_from_config(&config).expect("default method"),
            Method::GET
        );

        let post = vec![ClientConfigEntry {
            key: "method".to_string(),
            value: "POST".to_string(),
        }];
        assert_eq!(
            HttpSource::method_from_config(&post).expect("post method"),
            Method::POST
        );

        let invalid = vec![ClientConfigEntry {
            key: "method".to_string(),
            value: "NOT A METHOD".to_string(),
        }];
        assert!(HttpSource::method_from_config(&invalid).is_err());
        assert!(
            HttpSource::endpoint_from_config(&[])
                .expect_err("missing HTTP endpoint")
                .to_string()
                .contains("missing HTTP client config key 'endpoint'")
        );
    }
}
