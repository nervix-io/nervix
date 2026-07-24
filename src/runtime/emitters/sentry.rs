use reqwest::{
    Client as HttpClient,
    header::{CONTENT_TYPE, HeaderValue},
};
use sentry_types::{Dsn, protocol::v7::Event};

use super::*;
use crate::runtime::http_client::HttpClientConfig;

const SENTRY_AUTH_HEADER: &str = "x-sentry-auth";
const SENTRY_ENVELOPE_CONTENT_TYPE: &str = "application/x-sentry-envelope";
const SENTRY_CLIENT_AGENT: &str = concat!("nervix/", env!("CARGO_PKG_VERSION"));

pub(in crate::runtime) struct SentryEmitter {
    client: Option<HttpClient>,
    dsn: Dsn,
}

impl SentryEmitter {
    pub(in crate::runtime) fn new(
        client: &CreateClientHttp,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let config = resolved
            .map(|config| config.entries.as_slice())
            .unwrap_or(client.config.as_slice());
        let dsn = emitter_config_value(config, "dsn", || {
            "missing Sentry HTTP client config key 'dsn'".to_string()
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

    pub(in crate::runtime) async fn publish(&mut self, payload: &[u8]) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized Sentry sink client"));
        };
        let body = Self::encode_envelope(payload)?;
        let auth = HeaderValue::from_str(&self.dsn.to_auth(Some(SENTRY_CLIENT_AGENT)).to_string())
            .map_err(|error| {
                emitter_config_error(format!("invalid Sentry authentication header: {error}"))
            })?;

        client
            .post(self.dsn.envelope_api_url())
            .header(SENTRY_AUTH_HEADER, auth)
            .header(CONTENT_TYPE, SENTRY_ENVELOPE_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ())
            .map_err(emitter_publish_error)
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
        let event: serde_json::Value =
            serde_json::from_slice(lines.next().expect("event payload")).expect("valid event");

        assert_eq!(header["event_id"], event["event_id"]);
        assert_eq!(item_header["type"], "event");
        assert_eq!(item_header["length"], envelope_event_bytes(&event).len());
        assert_eq!(event["platform"], "other");
        assert!(event.get("timestamp").is_some());
        assert_eq!(event["future"]["nested"], true);
    }

    fn envelope_event_bytes(event: &serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(
            event
                .as_object()
                .expect("encoded Sentry event must be an object"),
        )
        .expect("event should serialize")
    }
}
