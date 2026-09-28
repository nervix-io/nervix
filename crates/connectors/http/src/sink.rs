//! HTTP request sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Sending each prepared request in the order the host hands them over, with exactly
//!   its method, target, application headers and body, over a client that never follows a
//!   redirect, and the answer for each: complete `2xx` response headers deliver its record, and any
//!   other response or a failed exchange fails the attempt, leaving that request and every later
//!   one for the host to send again.
//! - **Depends on.** The connector contract, the shared HTTP client settings, the vocabulary's
//!   request fields and `reqwest`.
//! - **Must not know.** How the host evaluated the request fields or encoded the body, runtime
//!   batches, relays, branches, registry state, or when the host retries.

use async_trait::async_trait;
use error_stack::Report;
use nervix_connector::{
    HttpClientConfig, HttpRequestSink, PerRecordOutcome, RejectedSinkRecord, SinkHost,
    SinkHttpRequest, SinkLifecycle, SinkPublishError, SinkRecordId, SinkStartError,
    SinkStartResult,
};
use nervix_dns::DnsResolver;
use nervix_models::ClientConfigEntry;
use reqwest::{
    Body, Client as HttpClient, Method, Request,
    header::{HeaderName, HeaderValue},
};
use thiserror::Error;

const HTTP_SINK: &str = "HTTP";

/// What one HTTP sink sends with: the entries naming its client settings, and the node's resolver.
pub struct HttpSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
}

/// The sink of an HTTP emitter, which sends one request at a time and waits for its response
/// headers before it sends the next one.
pub struct HttpSink {
    client: HttpClient,
}

/// Why a prepared request cannot be written as an HTTP request. The host validated every field,
/// so none of these is expected; each rejects only the request it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
enum HttpRequestWriteError {
    #[error("HTTP method cannot be written")]
    Method,
    #[error("HTTP header name cannot be written")]
    HeaderName,
    #[error("HTTP header value cannot be written")]
    HeaderValue,
}

impl HttpSink {
    pub fn new(config: HttpSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let client = HttpClientConfig::new(&config.config, HTTP_SINK, &config.dns)
            .build_without_redirects()
            .map_err(|error| {
                let message = error.current_context().to_string();
                error
                    .change_context(SinkStartError::InvalidConfiguration { sink: HTTP_SINK })
                    .attach_printable(message)
            })?;
        Ok(Self { client })
    }

    /// The HTTP request a prepared request describes, or the rejection of a request that cannot
    /// be written.
    fn request(request: SinkHttpRequest) -> Result<Request, RejectedSinkRecord<SinkRecordId>> {
        let SinkHttpRequest {
            id,
            method,
            target,
            headers,
            body,
            occurred_at,
        } = request;
        let rejected = |error: HttpRequestWriteError| {
            RejectedSinkRecord::external(id, occurred_at, error.to_string())
        };
        let Ok(method) = Method::from_bytes(method.as_str().as_bytes()) else {
            return Err(rejected(HttpRequestWriteError::Method));
        };
        let mut written = Request::new(method, target.url().clone());
        for (name, value) in headers.iter() {
            let Ok(name) = HeaderName::from_bytes(name.as_str().as_bytes()) else {
                return Err(rejected(HttpRequestWriteError::HeaderName));
            };
            let Ok(value) = HeaderValue::from_bytes(value.as_str().as_bytes()) else {
                return Err(rejected(HttpRequestWriteError::HeaderValue));
            };
            written.headers_mut().insert(name, value);
        }
        if let Some(body) = body {
            *written.body_mut() = Some(Body::from(body));
        }
        Ok(written)
    }

    fn publish_error(message: String) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: HTTP_SINK }).attach_printable(message)
    }
}

/// A failed exchange, described by the error and every cause under it without the request's URL,
/// which carries an evaluated target.
fn exchange_failure(error: reqwest::Error) -> String {
    let error = error.without_url();
    let mut description = error.to_string();
    let mut cause = std::error::Error::source(&error);
    while let Some(current) = cause {
        description.push_str(": ");
        description.push_str(&current.to_string());
        cause = current.source();
    }
    description
}

#[async_trait]
impl SinkLifecycle for HttpSink {}

#[async_trait]
impl HttpRequestSink for HttpSink {
    async fn publish(&mut self, requests: Vec<SinkHttpRequest>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(requests.len());
        for request in requests {
            tokio::task::consume_budget().await;
            let id = request.id;
            let written = match Self::request(request) {
                Ok(written) => written,
                Err(rejected) => {
                    outcome.reject(rejected);
                    continue;
                }
            };
            let response = match self.client.execute(written).await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(Self::publish_error(format!(
                        "HTTP request failed: {}",
                        exchange_failure(error)
                    )));
                    return outcome;
                }
            };
            let status = response.status();
            if status.is_success() {
                outcome.deliver(id);
                continue;
            }
            outcome.fail(Self::publish_error(format!(
                "HTTP endpoint answered with status {}",
                status.as_u16()
            )));
            return outcome;
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        HttpApplicationHeaders, HttpBodyMode, HttpHeaderName, HttpHeaderValue, HttpMethod,
        HttpOrigin, Timestamp,
    };

    use super::*;

    fn prepared(method: &str, body: Option<&[u8]>) -> SinkHttpRequest {
        let origin = HttpOrigin::parse("https://api.example.com")
            .expect("the test origin has an HTTPS scheme and a host");
        let mut headers = HttpApplicationHeaders::default();
        headers
            .insert(
                HttpHeaderName::parse("X-Tenant").expect("X-Tenant is a valid field name"),
                HttpHeaderValue::parse("north").expect("north is a valid field value"),
            )
            .expect("one short header is within the envelope");
        SinkHttpRequest {
            id: SinkRecordId::new(3),
            method: HttpMethod::parse(method, HttpBodyMode::WithoutBody)
                .expect("the test method is a valid token"),
            target: origin
                .target("/v1/a/../events?q=a+b")
                .expect("the test target is origin-relative"),
            headers,
            body: body.map(<[u8]>::to_vec),
            occurred_at: Timestamp::from_unix_nanos(1),
        }
    }

    #[test]
    fn a_prepared_request_is_written_with_exactly_its_fields() {
        let written = HttpSink::request(prepared("PATCH", Some(br#"{"id":1}"#)))
            .expect("every field of the prepared request can be written");

        assert_eq!(written.method().as_str(), "PATCH");
        assert_eq!(
            written.url().as_str(),
            "https://api.example.com/v1/events?q=a+b"
        );
        assert_eq!(
            written.headers().get("x-tenant").map(HeaderValue::as_bytes),
            Some(b"north".as_slice())
        );
        assert_eq!(
            written.headers().len(),
            1,
            "no header is added to the prepared ones"
        );
        assert_eq!(
            written.body().and_then(Body::as_bytes),
            Some(br#"{"id":1}"#.as_slice())
        );
    }

    #[test]
    fn an_extension_method_keeps_its_spelling_and_a_bodyless_request_carries_no_body() {
        let written = HttpSink::request(prepared("purge", None))
            .expect("an extension method token can be written");

        assert_eq!(written.method().as_str(), "purge");
        assert!(written.body().is_none());
    }

    #[tokio::test]
    async fn an_exchange_failure_is_described_without_the_request_url() {
        let closed =
            std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback port can be bound");
        let address = closed
            .local_addr()
            .expect("a bound listener has a local address");
        drop(closed);
        let failure = reqwest::Client::new()
            .get(format!("http://{address}/secret/target"))
            .send()
            .await
            .expect_err("nothing listens on a port whose listener was dropped");
        assert!(
            failure.to_string().contains("secret"),
            "reqwest names the URL of a failed request: {failure}"
        );

        let description = exchange_failure(failure);

        assert!(!description.contains("secret"), "{description}");
    }
}
