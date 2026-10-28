//! Sentry emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Sentry DSN and envelope endpoint, envelope encoding with its event defaults,
//!   one event identity per prepared payload, per-record response classification, and the retry
//!   delay a rate-limited project asks for.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, `reqwest`
//!   and `sentry-types`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

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

use std::time::Duration;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecord, EmitterRecordId, EmitterRetryDelay, EmitterStartResult, EmitterWriteError,
    HttpClientConfig, OfflineClientConfigError, PerRecordOutcome, RecordEmitter,
    client_config_value, physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_models::{
    ClientConfigEntry, ConnectorKind, MessageErrorCode, MessageErrorOperation,
    StructuredMessageError, Timestamp,
};
use nervix_primitives::time::Instant;
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

/// What one Sentry emitter sends with: the entries naming its project DSN and HTTP client settings.
pub struct SentryEventEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub dns: DnsResolver,
    /// The plan's resolved platform, used only when the encoded event omits its own.
    pub platform: String,
}

pub struct SentryEventEmitter {
    client: HttpClient,
    envelope_url: url::Url,
    auth: HeaderValue,
    platform: String,
}

/// Validate the complete DSN with the event driver's parser without connecting or resolving a host.
/// The public refusal names the key; the driver's typed cause remains in the internal report.
pub fn validate_client_configuration(
    config: &[ClientConfigEntry],
) -> error_stack::Result<(), OfflineClientConfigError> {
    SentryEventEmitter::dsn(config).map(|_| ())
}

/// Why one record's payload is not a Sentry event this emitter can send.
#[derive(Debug, Error)]
enum SentryEventError {
    #[error("Sentry codec payload is not a valid event JSON object")]
    EventJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Sentry codec payload is not a valid event")]
    Event {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize Sentry event")]
    SerializeEvent {
        #[source]
        source: serde_json::Error,
    },
    #[error("encoded Sentry event is {size} bytes; maximum is {maximum}")]
    OversizedEvent { size: usize, maximum: usize },
    #[error("failed to serialize Sentry envelope header")]
    SerializeEnvelopeHeader {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize Sentry envelope item header")]
    SerializeItemHeader {
        #[source]
        source: serde_json::Error,
    },
}

impl SentryEventEmitter {
    pub fn new(config: SentryEventEmitterConfig, _host: EmitterHost) -> EmitterStartResult<Self> {
        let dns = config.dns;
        let platform = config.platform;
        let config = config.config.as_slice();
        let dsn = Self::dsn(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: SENTRY })?;
        let client = {
            let mapping_input = HttpClientConfig::new(config, "Sentry", &dns).build();
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(EmitterOpenError::InvalidConfiguration {
                            connector: SENTRY,
                        })
                        .attach_printable(message)
                }),
            }
        }?;
        let envelope_url = dsn.envelope_api_url();
        let auth = HeaderValue::from_str(&dsn.to_auth(Some(SENTRY_CLIENT_AGENT)).to_string())
            .map_err(|error| {
                Self::config_error(format!("invalid Sentry authentication header: {error}"))
            })?;
        Ok(Self {
            client,
            envelope_url,
            auth,
            platform,
        })
    }

    fn dsn(config: &[ClientConfigEntry]) -> error_stack::Result<Dsn, OfflineClientConfigError> {
        let dsn = client_config_value(config, "dsn", ConnectorKind::Sentry.keyword())
            .change_context(OfflineClientConfigError::key("dsn"))?;
        dsn.parse::<Dsn>()
            .change_context(OfflineClientConfigError::key("dsn"))
    }

    fn config_error(error: impl std::fmt::Display) -> Report<EmitterOpenError> {
        Report::new(EmitterOpenError::InvalidConfiguration { connector: SENTRY })
            .attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external Sentry driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<EmitterWriteError> {
        Report::new(EmitterWriteError::Publish { connector: SENTRY })
            .attach_printable(error.to_string())
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
        platform: &str,
    ) -> Result<Vec<u8>, Report<SentryEventError>> {
        let mut event =
            serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(payload)
                .map_err(|source| Report::new(SentryEventError::EventJson { source }))?;
        let parsed =
            serde_json::from_value::<Event<'static>>(serde_json::Value::Object(event.clone()))
                .map_err(|source| Report::new(SentryEventError::Event { source }))?;
        let event_id = event
            .entry("event_id".to_string())
            .or_insert_with(|| serde_json::Value::String(parsed.event_id.simple().to_string()))
            .clone();
        event
            .entry("platform".to_string())
            .or_insert_with(|| serde_json::Value::String(platform.to_owned()));
        if !event.contains_key("timestamp") {
            event.insert(
                "timestamp".to_string(),
                serde_json::Value::String(occurred_at.as_datetime().to_rfc3339()),
            );
        }

        let event = serde_json::to_vec(&event)
            .map_err(|source| Report::new(SentryEventError::SerializeEvent { source }))?;
        if event.len() > nervix_models::SENTRY_MAX_EVENT_BYTES {
            return Err(Report::new(SentryEventError::OversizedEvent {
                size: event.len(),
                maximum: nervix_models::SENTRY_MAX_EVENT_BYTES,
            }));
        }
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

    fn envelope_request(&self, payload: Vec<u8>) -> reqwest::RequestBuilder {
        self.client
            .post(self.envelope_url.clone())
            .header(SENTRY_AUTH_HEADER, self.auth.clone())
            .header(CONTENT_TYPE, SENTRY_ENVELOPE_CONTENT_TYPE)
            .body(payload)
    }
}

impl SentryEventEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Sentry(nervix_models::SentryEmitterEntity::Event);
}

#[async_trait]
impl EmitterLifecycle for SentryEventEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each envelope is answered before the publish returns, so the client holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RecordEmitter for SentryEventEmitter {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "projects the complete Sentry preparation report into its message rejection"
        )
    )]
    fn prepare_payload(
        &mut self,
        payload: Vec<u8>,
        occurred_at: Timestamp,
    ) -> Result<Vec<u8>, StructuredMessageError> {
        Self::encode_envelope(&payload, occurred_at, &self.platform).map_err(|error| {
            StructuredMessageError {
                reference: uuid::Uuid::now_v7(),
                code: MessageErrorCode::External,
                message: format!("{error:#}"),
                operation: MessageErrorOperation::Publish,
                operation_index: None,
                fields: Default::default(),
                occurred_at,
            }
        })
    }

    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for mut record in records {
            nervix_primitives::task::consume_budget().await;
            let request = self
                .envelope_request(std::mem::take(&mut record.payload))
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
                outcome.deliver(record.id);
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
                Some(delay) => error.attach(EmitterRetryDelay(delay)),
                None => error,
            });
            return outcome;
        }
        outcome
    }
}

#[cfg(test)]
mod envelope_properties;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use super::*;

    const PLATFORM: &str = nervix_models::SENTRY_DEFAULT_PLATFORM;

    #[test]
    fn dsn_validation_is_offline_and_preserves_the_typed_key_refusal() {
        let config = |value: &str| {
            [ClientConfigEntry {
                key: "dsn".to_owned(),
                value: value.to_owned(),
            }]
        };
        for value in [
            "https://key@sentry.invalid/prefix/42",
            "https://key@sentry.invalid/project",
        ] {
            assert!(validate_client_configuration(&config(value)).is_ok());
        }
        for value in [
            "https://private-key@sentry.invalid/",
            "https://sentry.invalid/42",
            "ftp://private-key@sentry.invalid/42",
        ] {
            let Err(error) = validate_client_configuration(&config(value)) else {
                panic!("the event driver must refuse {value}");
            };
            assert!(error.contains::<sentry_types::ParseDsnError>());
            assert!(matches!(
                error.current_context(),
                OfflineClientConfigError::InvalidKey { key } if key == "dsn"
            ));
            assert_eq!(error.to_string(), "invalid client config key 'dsn'");
        }
    }

    #[test]
    fn envelope_preserves_event_fields_and_adds_protocol_defaults() {
        let occurred_at = Timestamp::from_unix_nanos(946_684_800_000_000_000);
        let envelope = SentryEventEmitter::encode_envelope(
            br#"{"message":"failed","environment":"test","future":{"nested":true}}"#,
            occurred_at,
            PLATFORM,
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
        let envelope = SentryEventEmitter::encode_envelope(
            br#"{"message":"failed","timestamp":"2010-05-06T07:08:09Z"}"#,
            Timestamp::from_unix_nanos(946_684_800_000_000_000),
            PLATFORM,
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
    fn envelope_keeps_every_digit_of_an_event_number() {
        let envelope = SentryEventEmitter::encode_envelope(
            br#"{"message":"failed","extra":{"reading":1.4000000000000001}}"#,
            Timestamp::from_unix_nanos(946_684_800_000_000_000),
            PLATFORM,
        )
        .expect("event should encode");
        let event_bytes = envelope
            .split(|byte| *byte == b'\n')
            .nth(2)
            .expect("envelope must contain an event payload");
        let event = std::str::from_utf8(event_bytes).expect("event payload must be UTF-8");

        assert!(
            event.contains(r#""reading":1.4000000000000001"#),
            "the event keeps its number: {event}"
        );
    }

    #[test]
    fn a_payload_that_is_not_an_event_names_its_parser_error_once() {
        let occurred_at = Timestamp::from_unix_nanos(946_684_800_000_000_000);
        let not_json = SentryEventEmitter::encode_envelope(b"not json", occurred_at, PLATFORM)
            .expect_err("text that is not JSON is not an event");
        let json_cause =
            serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(b"not json")
                .expect_err("text that is not JSON does not parse");
        let wrong_level =
            SentryEventEmitter::encode_envelope(br#"{"level":5}"#, occurred_at, PLATFORM)
                .expect_err("a numeric level is not a Sentry level");
        let event_cause = serde_json::from_value::<Event<'static>>(serde_json::json!({"level": 5}))
            .expect_err("a numeric level does not read as an event");

        assert_eq!(
            format!("{not_json:#}"),
            format!("Sentry codec payload is not a valid event JSON object: {json_cause}")
        );
        assert_eq!(
            format!("{wrong_level:#}"),
            format!("Sentry codec payload is not a valid event: {event_cause}")
        );
    }

    #[test]
    fn envelope_rejects_an_event_above_sentrys_decompressed_limit() {
        let payload = format!(
            r#"{{"message":"{}"}}"#,
            "x".repeat(nervix_models::SENTRY_MAX_EVENT_BYTES)
        );
        let result = SentryEventEmitter::encode_envelope(
            payload.as_bytes(),
            Timestamp::from_unix_nanos(946_684_800_000_000_000),
            PLATFORM,
        );
        assert!(
            result.is_err(),
            "an oversized event must be rejected locally"
        );
    }

    #[test]
    fn classifies_only_definitive_client_responses_as_record_errors() {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::PAYLOAD_TOO_LARGE,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert!(SentryEventEmitter::is_record_status(status));
        }
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(!SentryEventEmitter::is_record_status(status));
        }
    }

    #[test]
    fn server_retry_headers_extend_to_the_longest_requested_delay() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-08-05T12:00:00Z")
            .expect("fixed timestamp should parse")
            .with_timezone(&chrono::Utc);
        let delay = SentryEventEmitter::server_retry_delay(
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
        let delay = SentryEventEmitter::server_retry_delay(
            Some("Wed, 05 Aug 2026 12:00:20 +0000"),
            None,
            now,
        );

        assert_eq!(delay, Some(Duration::from_secs(20)));
    }
}
