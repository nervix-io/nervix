//! Sentry sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Sentry DSN and envelope endpoint, envelope encoding with its event defaults,
//!   per-record response classification, and the retry delay a rate-limited project asks for.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, `reqwest`
//!   and `sentry-types`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::time::Duration;

use async_trait::async_trait;
use error_stack::Report;
use nervix_connector::{
    HttpClientConfig, PerRecordOutcome, RecordSink, SinkHost, SinkLifecycle, SinkPublishError,
    SinkRecord, SinkRetryDelay, SinkStartError, SinkStartResult, client_config_value,
    physical_time::actual_utc_now,
};
use nervix_models::{ClientConfigEntry, Timestamp};
use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_TYPE, HeaderValue, RETRY_AFTER},
};
use sentry_types::{Dsn, protocol::v7::Event};
use thiserror::Error;

const SENTRY: &str = "sentry";
const SENTRY_AUTH_HEADER: &str = "x-sentry-auth";
const SENTRY_ENVELOPE_CONTENT_TYPE: &str = "application/x-sentry-envelope";
const SENTRY_CLIENT_AGENT: &str = concat!("nervix/", env!("CARGO_PKG_VERSION"));
const SENTRY_RATE_LIMITS_HEADER: &str = "x-sentry-rate-limits";

/// What one Sentry sink sends with: the entries naming its project DSN and HTTP client settings.
pub struct SentrySinkConfig {
    pub config: Vec<ClientConfigEntry>,
}

pub struct SentrySink {
    client: HttpClient,
    envelope_url: url::Url,
    auth: HeaderValue,
}

/// Why one record's payload is not a Sentry event this sink can send.
#[derive(Debug, Error)]
enum SentryEventError {
    #[error("Sentry codec payload is not a valid event JSON object: {source}")]
    EventJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Sentry codec payload is not a valid event: {source}")]
    Event {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize Sentry event: {source}")]
    SerializeEvent {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize Sentry envelope header: {source}")]
    SerializeEnvelopeHeader {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize Sentry envelope item header: {source}")]
    SerializeItemHeader {
        #[source]
        source: serde_json::Error,
    },
}

impl SentrySink {
    pub fn new(config: SentrySinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let config = config.config.as_slice();
        let dsn = Self::config_value(config, "dsn")?
            .parse::<Dsn>()
            .map_err(|error| Self::config_error(format!("invalid Sentry dsn: {error}")))?;
        let client = HttpClientConfig::new(config, "Sentry")
            .build()
            .map_err(|error| {
                let message = error.current_context().to_string();
                error
                    .change_context(SinkStartError::InvalidConfiguration { sink: SENTRY })
                    .attach_printable(message)
            })?;
        let envelope_url = dsn.envelope_api_url();
        let auth = HeaderValue::from_str(&dsn.to_auth(Some(SENTRY_CLIENT_AGENT)).to_string())
            .map_err(|error| {
                Self::config_error(format!("invalid Sentry authentication header: {error}"))
            })?;
        Ok(Self {
            client,
            envelope_url,
            auth,
        })
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        client_config_value(config, key, "Sentry").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: SENTRY })
                .attach_printable(message)
        })
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: SENTRY })
            .attach_printable(error.to_string())
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: SENTRY }).attach_printable(error.to_string())
    }

    fn is_record_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::BAD_REQUEST
                | StatusCode::PAYLOAD_TOO_LARGE
                | StatusCode::UNPROCESSABLE_ENTITY
        )
    }

    fn server_retry_delay(
        retry_after: Option<&str>,
        sentry_rate_limits: Option<&str>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Option<Duration> {
        let retry_after = if let Some(value) = retry_after {
            let value = value.trim();
            if let Ok(seconds) = value.parse::<f64>()
                && seconds.is_finite()
                && seconds >= 0.0
                && let Ok(delay) = Duration::try_from_secs_f64(seconds)
            {
                Some(delay)
            } else if let Ok(deadline) = chrono::DateTime::parse_from_rfc2822(value) {
                deadline
                    .with_timezone(&chrono::Utc)
                    .signed_duration_since(now)
                    .to_std()
                    .ok()
            } else {
                None
            }
        } else {
            None
        };
        let sentry_rate_limits = if let Some(value) = sentry_rate_limits {
            let mut longest: Option<Duration> = None;
            for quota in value.split(',') {
                let Some(seconds) = quota.trim().split(':').next() else {
                    continue;
                };
                let Ok(seconds) = seconds.trim().parse::<f64>() else {
                    continue;
                };
                if !seconds.is_finite() || seconds < 0.0 {
                    continue;
                }
                let Ok(delay) = Duration::try_from_secs_f64(seconds) else {
                    continue;
                };
                longest = Some(match longest {
                    Some(current) => current.max(delay),
                    None => delay,
                });
            }
            longest
        } else {
            None
        };
        retry_after.into_iter().chain(sentry_rate_limits).max()
    }

    fn encode_envelope(
        payload: &[u8],
        occurred_at: Timestamp,
    ) -> Result<Vec<u8>, Report<SentryEventError>> {
        let mut event =
            serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(payload)
                .map_err(|source| Report::new(SentryEventError::EventJson { source }))?;
        let parsed =
            serde_json::from_value::<Event<'static>>(serde_json::Value::Object(event.clone()))
                .map_err(|source| Report::new(SentryEventError::Event { source }))?;
        let event_id = parsed.event_id.simple().to_string();
        event.insert(
            "event_id".to_string(),
            serde_json::Value::String(event_id.clone()),
        );
        event
            .entry("platform".to_string())
            .or_insert_with(|| serde_json::Value::String("other".to_string()));
        if !event.contains_key("timestamp") {
            event.insert(
                "timestamp".to_string(),
                serde_json::Value::String(occurred_at.as_datetime().to_rfc3339()),
            );
        }

        let event = serde_json::to_vec(&event)
            .map_err(|source| Report::new(SentryEventError::SerializeEvent { source }))?;
        let envelope_header = serde_json::json!({ "event_id": event_id });
        let item_header = serde_json::json!({
            "type": "event",
            "length": event.len(),
            "content_type": "application/json",
        });
        let mut envelope = serde_json::to_vec(&envelope_header)
            .map_err(|source| Report::new(SentryEventError::SerializeEnvelopeHeader { source }))?;
        envelope.push(b'\n');
        serde_json::to_writer(&mut envelope, &item_header)
            .map_err(|source| Report::new(SentryEventError::SerializeItemHeader { source }))?;
        envelope.push(b'\n');
        envelope.extend_from_slice(&event);
        envelope.push(b'\n');
        Ok(envelope)
    }
}

#[async_trait]
impl SinkLifecycle for SentrySink {}

#[async_trait]
impl RecordSink for SentrySink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            let body = match Self::encode_envelope(&record.payload, record.occurred_at) {
                Ok(body) => body,
                Err(error) => {
                    outcome.reject(record.rejected(error.current_context().to_string()));
                    continue;
                }
            };
            let request = self
                .client
                .post(self.envelope_url.clone())
                .header(SENTRY_AUTH_HEADER, self.auth.clone())
                .header(CONTENT_TYPE, SENTRY_ENVELOPE_CONTENT_TYPE)
                .body(body)
                .send();
            let response = match request.await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(Self::publish_error(format!(
                        "Sentry envelope request failed: {error}"
                    )));
                    return outcome;
                }
            };
            let status = response.status();
            if status.is_success() {
                outcome.deliver(record.position);
                continue;
            }
            if Self::is_record_status(status) {
                outcome.reject(record.rejected(format!(
                    "Sentry rejected the event with HTTP status {status}"
                )));
                continue;
            }
            let retry_delay = Self::server_retry_delay(
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
                response
                    .headers()
                    .get(SENTRY_RATE_LIMITS_HEADER)
                    .and_then(|value| value.to_str().ok()),
                actual_utc_now().into_datetime(),
            );
            let error = Self::publish_error(format!(
                "Sentry envelope request returned HTTP status {status}"
            ));
            outcome.fail(match retry_delay {
                Some(delay) => error.attach(SinkRetryDelay(delay)),
                None => error,
            });
            return outcome;
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_preserves_event_fields_and_adds_protocol_defaults() {
        let occurred_at = Timestamp::from_unix_nanos(946_684_800_000_000_000);
        let envelope = SentrySink::encode_envelope(
            br#"{"message":"failed","environment":"test","future":{"nested":true}}"#,
            occurred_at,
        )
        .expect("event should encode");
        let mut lines = envelope.split(|byte| *byte == b'\n');
        let header: serde_json::Value =
            serde_json::from_slice(lines.next().expect("envelope header")).expect("valid header");
        let item_header: serde_json::Value =
            serde_json::from_slice(lines.next().expect("item header")).expect("valid item header");
        let event_bytes = lines.next().expect("event payload");
        let event: serde_json::Value = serde_json::from_slice(event_bytes).expect("valid event");

        assert_eq!(header["event_id"], event["event_id"]);
        assert_eq!(item_header["type"], "event");
        assert_eq!(item_header["length"], event_bytes.len());
        assert_eq!(event["platform"], "other");
        assert_eq!(event["timestamp"], occurred_at.as_datetime().to_rfc3339());
        assert_eq!(event["future"]["nested"], true);
    }

    #[test]
    fn envelope_preserves_an_explicit_event_timestamp() {
        let envelope = SentrySink::encode_envelope(
            br#"{"message":"failed","timestamp":"2010-05-06T07:08:09Z"}"#,
            Timestamp::from_unix_nanos(946_684_800_000_000_000),
        )
        .expect("event should encode");
        let event_bytes = envelope
            .split(|byte| *byte == b'\n')
            .nth(2)
            .expect("envelope must contain an event payload");
        let event: serde_json::Value =
            serde_json::from_slice(event_bytes).expect("event payload must be JSON");

        assert_eq!(event["timestamp"], "2010-05-06T07:08:09Z");
    }

    #[test]
    fn classifies_only_definitive_client_responses_as_record_errors() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::PAYLOAD_TOO_LARGE,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert!(SentrySink::is_record_status(status));
        }
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(!SentrySink::is_record_status(status));
        }
    }

    #[test]
    fn server_retry_headers_extend_to_the_longest_requested_delay() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&chrono::Utc);
        let delay = SentrySink::server_retry_delay(
            Some("15"),
            Some("60:error;transaction:organization:quota, 30::project"),
            now,
        );

        assert_eq!(delay, Some(Duration::from_secs(60)));
    }

    #[test]
    fn retry_after_http_dates_are_supported() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&chrono::Utc);
        let delay =
            SentrySink::server_retry_delay(Some("Wed, 05 Aug 2026 12:00:20 +0000"), None, now);

        assert_eq!(delay, Some(Duration::from_secs(20)));
    }
}
