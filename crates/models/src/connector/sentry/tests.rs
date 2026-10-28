//! Sentry event configuration and complete representation contracts.
//!
//! Layer: test harness.
//! - **Owns.** Authored platform presence, its effective default and complete JSON/archive equality.
//! - **Depends on.** The Sentry vocabulary, its production encodings and bounded Bolero inputs.
//! - **Must not know.** Drivers, network services, execution plans or runtime preparation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "qualifies the Sentry event vocabulary boundary")
)]

use meticulous::ResultExt as _;

use super::*;

impl SentryEventEmitter {
    fn assert_representation(&self) {
        let json = serde_json::to_value(self)
            .assured("the current event Model contains only serializable strings and its mode");
        let expected = match &self.platform {
            Some(platform) => serde_json::Value::String(platform.clone()),
            None => serde_json::Value::Null,
        };
        assert_eq!(json.get("platform"), Some(&expected));
        let decoded: Self = serde_json::from_value(json)
            .assured("the current serialized event Model reads through its production decoder");
        assert_eq!(&decoded, self);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .assured("the bounded current event Model fits its archive");
        let decoded = rkyv::from_bytes::<Self, rkyv::rancor::Error>(&archive)
            .assured("the production archive decoder reads the current event Model");
        assert_eq!(&decoded, self);
    }

    fn fixture(platform: Option<String>) -> Self {
        Self {
            client: ClientName::parse("sentry").assured("sentry is a valid client identifier"),
            mode: SentryEventPublishingMode::Ack {
                retry_policy: crate::RetryPolicy {
                    backoff: "100ms".into(),
                    max_backoff: "1s".into(),
                },
            },
            platform,
        }
    }
}

#[test]
fn the_effective_platform_preserves_every_explicit_string() {
    for (authored, expected) in [
        (None, "other"),
        (Some(""), ""),
        (Some("other"), "other"),
        (Some("Py'thon.東京\n"), "Py'thon.東京\n"),
    ] {
        let event = SentryEventEmitter::fixture(authored.map(str::to_owned));
        assert_eq!(event.platform(), expected);
        event.assert_representation();
    }
}

#[test]
fn bolero_event_models_preserve_the_authored_platform() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(128)
        .for_each(|input: &[u8]| {
            let platform = if input.first().copied().unwrap_or(0) & 1 == 0 {
                None
            } else {
                Some(String::from_utf8_lossy(input).into_owned())
            };
            SentryEventEmitter::fixture(platform).assert_representation();
        });
}
