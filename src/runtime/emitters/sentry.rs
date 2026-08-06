use reqwest::{
    Client as HttpClient, StatusCode,
    header::{CONTENT_TYPE, HeaderValue, RETRY_AFTER},
};
use sentry_types::{Dsn, protocol::v7::Event};

use super::*;
use crate::runtime::http_client::HttpClientConfig;

const SENTRY_AUTH_HEADER: &str = "x-sentry-auth";
const SENTRY_ENVELOPE_CONTENT_TYPE: &str = "application/x-sentry-envelope";
const SENTRY_CLIENT_AGENT: &str = concat!("nervix/", env!("CARGO_PKG_VERSION"));
const SENTRY_RATE_LIMITS_HEADER: &str = "x-sentry-rate-limits";

pub(in crate::runtime) struct SentryEmitter {
    client: Option<HttpClient>,
    dsn: Dsn,
}

impl SentryEmitter {
    pub(in crate::runtime) fn new(
        client: &CreateClientSentry,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let config = resolved
            .map(|config| config.entries.as_slice())
            .unwrap_or(client.config.as_slice());
        let dsn = emitter_config_value(config, "dsn", || {
            "missing Sentry client config key 'dsn'".to_string()
        })?
        .parse::<Dsn>()
        .map_err(|error| emitter_config_error(format!("invalid Sentry dsn: {error}")))?;
        let client = HttpClientConfig::new(config, "Sentry")
            .build()
            .map_err(emitter_config_error)?;
        Ok(Self {
            client: Some(client),
            dsn,
        })
    }

    pub(super) async fn publish(
        &mut self,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(client) = self.client.as_mut() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized Sentry sink client"),
            );
            return outcome;
        };
        let auth =
            match HeaderValue::from_str(&self.dsn.to_auth(Some(SENTRY_CLIENT_AGENT)).to_string()) {
                Ok(auth) => auth,
                Err(error) => {
                    outcome.fail(emitter_config_error(format!(
                        "invalid Sentry authentication header: {error}"
                    )));
                    return outcome;
                }
            };

        for record in records {
            tokio::task::consume_budget().await;
            let position = (record.batch_index, record.row_index);
            let body = match Self::encode_envelope(&record.payload) {
                Ok(body) => body,
                Err(error) => {
                    outcome.reject(position, emitter_error_message(&error));
                    continue;
                }
            };
            let request = client
                .post(self.dsn.envelope_api_url())
                .header(SENTRY_AUTH_HEADER, auth.clone())
                .header(CONTENT_TYPE, SENTRY_ENVELOPE_CONTENT_TYPE)
                .body(body)
                .send();
            let response = match await_emitter_confirmation(&record.acks, request).await {
                Ok(response) => response,
                Err(error) => {
                    outcome.fail(emitter_publish_error(format!(
                        "Sentry envelope request failed: {error}"
                    )));
                    return outcome;
                }
            };
            let status = response.status();
            if status.is_success() {
                outcome.deliver(position);
                continue;
            }
            if Self::is_record_status(status) {
                outcome.reject(
                    position,
                    format!("Sentry rejected the event with HTTP status {status}"),
                );
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
                chrono::Utc::now(),
            );
            let error = format!("Sentry envelope request returned HTTP status {status}");
            outcome.fail(match retry_delay {
                Some(delay) => emitter_publish_error_with_minimum_retry_delay(error, delay),
                None => emitter_publish_error(error),
            });
            return outcome;
        }
        outcome
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
        let retry_after = retry_after.and_then(|value| {
            value
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
                .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
                .or_else(|| {
                    chrono::DateTime::parse_from_rfc2822(value.trim())
                        .ok()
                        .and_then(|deadline| {
                            deadline
                                .with_timezone(&chrono::Utc)
                                .signed_duration_since(now)
                                .to_std()
                                .ok()
                        })
                })
        });
        let sentry_rate_limits = sentry_rate_limits.and_then(|value| {
            value
                .split(',')
                .filter_map(|quota| quota.trim().split(':').next())
                .filter_map(|seconds| seconds.trim().parse::<f64>().ok())
                .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
                .filter_map(|seconds| Duration::try_from_secs_f64(seconds).ok())
                .max()
        });
        retry_after.into_iter().chain(sentry_rate_limits).max()
    }

    fn encode_envelope(payload: &[u8]) -> EmitterRuntimeResult<Vec<u8>> {
        let mut event = serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(
            payload,
        )
        .map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("Sentry codec payload is not a valid event JSON object: {error}"),
            )
        })?;
        let parsed =
            serde_json::from_value::<Event<'static>>(serde_json::Value::Object(event.clone()))
                .map_err(|error| {
                    emitter_report(
                        EmitterRuntimeError::EncodeBatch,
                        format!("Sentry codec payload is not a valid event: {error}"),
                    )
                })?;
        let event_id = parsed.event_id.simple().to_string();
        event.insert(
            "event_id".to_string(),
            serde_json::Value::String(event_id.clone()),
        );
        event
            .entry("platform".to_string())
            .or_insert_with(|| serde_json::Value::String("other".to_string()));
        if !event.contains_key("timestamp") {
            let normalized = serde_json::to_value(&parsed).map_err(|error| {
                emitter_report(
                    EmitterRuntimeError::EncodeBatch,
                    format!("failed to normalize Sentry event: {error}"),
                )
            })?;
            let timestamp = normalized.get("timestamp").cloned().ok_or_else(|| {
                emitter_report(
                    EmitterRuntimeError::EncodeBatch,
                    "normalized Sentry event omitted its timestamp",
                )
            })?;
            event.insert("timestamp".to_string(), timestamp);
        }

        let event = serde_json::to_vec(&event).map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("failed to serialize Sentry event: {error}"),
            )
        })?;
        let envelope_header = serde_json::json!({ "event_id": event_id });
        let item_header = serde_json::json!({
            "type": "event",
            "length": event.len(),
            "content_type": "application/json",
        });
        let mut envelope = serde_json::to_vec(&envelope_header).map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("failed to serialize Sentry envelope header: {error}"),
            )
        })?;
        envelope.push(b'\n');
        serde_json::to_writer(&mut envelope, &item_header).map_err(|error| {
            emitter_report(
                EmitterRuntimeError::EncodeBatch,
                format!("failed to serialize Sentry envelope item header: {error}"),
            )
        })?;
        envelope.push(b'\n');
        envelope.extend_from_slice(&event);
        envelope.push(b'\n');
        Ok(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_preserves_event_fields_and_adds_protocol_defaults() {
        let envelope = SentryEmitter::encode_envelope(
            br#"{"message":"failed","environment":"test","future":{"nested":true}}"#,
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
        assert!(event.get("timestamp").is_some());
        assert_eq!(event["future"]["nested"], true);
    }

    #[test]
    fn classifies_only_definitive_client_responses_as_record_errors() {
        for status in [
            reqwest::StatusCode::BAD_REQUEST,
            reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert!(SentryEmitter::is_record_status(status));
        }
        for status in [
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::FORBIDDEN,
            reqwest::StatusCode::NOT_FOUND,
            reqwest::StatusCode::REQUEST_TIMEOUT,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(!SentryEmitter::is_record_status(status));
        }
    }

    #[test]
    fn server_retry_headers_extend_to_the_longest_requested_delay() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&chrono::Utc);
        let delay = SentryEmitter::server_retry_delay(
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
            SentryEmitter::server_retry_delay(Some("Wed, 05 Aug 2026 12:00:20 +0000"), None, now);

        assert_eq!(delay, Some(Duration::from_secs(20)));
    }

    #[test]
    fn server_delay_attachment_survives_publish_error_classification() {
        let error = emitter_publish_error_with_minimum_retry_delay(
            "Sentry rate limited the request",
            Duration::from_secs(45),
        );

        assert!(emitter_publish_error_is_retryable(&error));
        assert_eq!(emitter_minimum_retry_delay(&error), Duration::from_secs(45));
    }
}
