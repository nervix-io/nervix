//! Bounded qualification of prepared Sentry events and the bytes every attempt sends.
//!
//! Layer: test harness.
//! - **Owns.** The complete event/default oracle, envelope framing and attempt-byte property.
//! - **Depends on.** The production Sentry preparation and HTTP request builder, and Bolero.
//! - **Must not know.** Relays, schedules, persisted state or another connector.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies the production connector outside product execution"
    )
)]

use meticulous::{OptionExt as _, ResultExt as _};
use serde_json::{Map, Value};
use uuid::Uuid;

use super::*;

struct EnvelopeCase {
    event: Map<String, Value>,
    occurred_at: Timestamp,
    platform: String,
}

impl SentryEventEmitter {
    fn fixture(platform: &str) -> Self {
        Self {
            client: HttpClient::new(),
            envelope_url: "http://127.0.0.1:1/api/1/envelope/"
                .parse()
                .assured("the fixture supplies a literal HTTP URL"),
            auth: HeaderValue::from_static("Sentry sentry_key=test"),
            platform: platform.to_owned(),
        }
    }
}

impl EnvelopeCase {
    fn draw(input: &[u8]) -> Self {
        let choice = input.first().copied().unwrap_or(0);
        let mut event = serde_json::json!({
            "message": String::from_utf8_lossy(input),
            "extra": {"bytes": input, "nested": [null, true, 1.4000000000000001]}
        })
        .as_object()
        .assured("the case constructs an event object")
        .clone();
        let mut id_bytes = [0_u8; 16];
        for (destination, source) in id_bytes.iter_mut().zip(input) {
            *destination = *source;
        }
        let id = Uuid::from_bytes(id_bytes);
        match choice % 3 {
            0 => {}
            1 => {
                event.insert("event_id".into(), Value::String(id.simple().to_string()));
            }
            2 => {
                event.insert(
                    "event_id".into(),
                    Value::String(id.hyphenated().to_string()),
                );
            }
            _ => unreachable!("a remainder modulo three is below three"),
        }
        if choice & 4 != 0 {
            event.insert(
                "timestamp".into(),
                Value::String("2001-02-03T04:05:06Z".into()),
            );
            event.insert("platform".into(), Value::String("rust".into()));
        }
        Self {
            event,
            occurred_at: Timestamp::from_unix_nanos(946_684_800_000_000_000),
            platform: String::from_utf8_lossy(input).into_owned(),
        }
    }

    fn assert_attempts(&self) {
        let mut connector = SentryEventEmitter::fixture(&self.platform);
        let codec_bytes = serde_json::to_vec(&self.event)
            .assured("the bounded event holds only serializable JSON values");
        let prepared = connector
            .prepare_payload(codec_bytes, self.occurred_at)
            .assured("the generated event has a valid UUID, timestamp and bounded JSON body");
        let mut lines = prepared.split(|byte| *byte == b'\n');
        let header: Value =
            serde_json::from_slice(lines.next().assured("a prepared envelope has its header"))
                .assured("the envelope header is JSON");
        let item: Value = serde_json::from_slice(
            lines
                .next()
                .assured("a prepared envelope has its item header"),
        )
        .assured("the item header is JSON");
        let event_bytes = lines.next().assured("a prepared envelope has its event");
        let event: Value =
            serde_json::from_slice(event_bytes).assured("the complete event is JSON");
        assert_eq!(lines.next(), Some(b"".as_slice()));
        assert_eq!(lines.next(), None);

        let id = header["event_id"]
            .as_str()
            .assured("the envelope id is text");
        Uuid::parse_str(id).assured("the envelope carries a valid UUID");
        assert_eq!(header, serde_json::json!({"event_id": id}));
        assert_eq!(
            item,
            serde_json::json!({
                "type": "event",
                "length": event_bytes.len(),
                "content_type": "application/json"
            })
        );
        let mut expected = self.event.clone();
        expected
            .entry("event_id".to_string())
            .or_insert(header["event_id"].clone());
        expected
            .entry("platform".to_string())
            .or_insert(Value::String(self.platform.clone()));
        expected
            .entry("timestamp".to_string())
            .or_insert(Value::String(self.occurred_at.as_datetime().to_rfc3339()));
        assert_eq!(event, Value::Object(expected));

        for _ in 0..4 {
            let request = connector
                .envelope_request(prepared.clone())
                .build()
                .assured("the prepared Sentry URL, headers and bytes form an HTTP request");
            let body = request.body().assured("a Sentry envelope has a body");
            assert_eq!(body.as_bytes(), Some(prepared.as_slice()));
        }
    }
}

#[test]
fn invalid_event_json_is_a_definitive_preparation_refusal_with_its_complete_cause() {
    let mut connector = SentryEventEmitter::fixture(nervix_models::SENTRY_DEFAULT_PLATFORM);
    let occurred_at = Timestamp::from_unix_nanos(946_684_800_000_000_000);
    let payload = b"not json";
    let Err(refusal) = connector.prepare_payload(payload.to_vec(), occurred_at) else {
        panic!("non-JSON bytes must be refused before any request");
    };
    let Err(cause) = serde_json::from_slice::<Map<String, Value>>(payload) else {
        panic!("the fixture supplies non-JSON bytes");
    };
    assert_eq!(refusal.code, MessageErrorCode::External);
    assert_eq!(refusal.operation, MessageErrorOperation::Publish);
    assert_eq!(refusal.occurred_at, occurred_at);
    assert_eq!(
        refusal.message,
        format!("Sentry codec payload is not a valid event JSON object: {cause}")
    );
}

#[test]
fn bolero_prepared_envelopes_preserve_complete_events_across_attempts() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(128)
        .for_each(|input: &[u8]| EnvelopeCase::draw(input).assert_attempts());
}
