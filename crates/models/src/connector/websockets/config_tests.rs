//! Owner tests for the WebSockets client's closed CONFIG schema.
//!
//! Layer: test harness.
//! - **Owns.** Current WebSockets vocabulary validation evidence.
//! - **Depends on.** The public vocabulary and standard value types.
//! - **Must not know.** Network drivers or runtime plans.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns current vocabulary validation evidence")
)]

use meticulous::ResultExt as _;
use strum::IntoEnumIterator as _;

use super::config::{WebsocketsClientConfig, WebsocketsClientConfigError, WebsocketsClientKey};
use crate::ClientConfigEntry;

fn config(entries: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    entries
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

#[test]
fn every_config_key_is_case_insensitive_and_every_supplied_duration_is_positive() {
    use std::str::FromStr as _;
    for key in WebsocketsClientKey::iter() {
        assert_eq!(
            WebsocketsClientKey::from_str(&key.to_string().to_ascii_uppercase()),
            Ok(key)
        );
    }
    let parsed = WebsocketsClientConfig::parse(&config(&[
        ("ENDPOINT", "wss://example.test/events?token=opaque"),
        ("connect_timeout_ms", "123"),
        ("ping_interval_ms", "456"),
        ("tls_ca_file", "{{ certs }}/ca.pem"),
        ("tls_cert_file", "{{ certs }}/cert.pem"),
        ("tls_key_file", "{{ certs }}/key.pem"),
    ]))
    .assured("all declared keys are set once with usable values");
    assert_eq!(
        parsed.connect_timeout(),
        std::time::Duration::from_millis(123)
    );
    assert_eq!(
        parsed.ping_interval(),
        Some(std::time::Duration::from_millis(456))
    );
    assert_eq!(parsed.endpoint().path(), "/events");
    for key in [
        WebsocketsClientKey::ConnectTimeoutMs,
        WebsocketsClientKey::PingIntervalMs,
    ] {
        for value in [
            "0",
            "-1",
            "+1",
            "1.5",
            "1s",
            "18446744073709551615",
            "18446744073709551616",
        ] {
            let Err(report) = WebsocketsClientConfig::parse(&config(&[
                ("endpoint", "ws://example.test"),
                (key.as_ref(), value),
            ])) else {
                panic!("a timer is a positive whole u64 number of milliseconds");
            };
            assert_eq!(
                report.current_context(),
                &WebsocketsClientConfigError::InvalidDuration { key }
            );
        }
    }
}

#[test]
fn config_refusals_keep_the_offending_key_and_never_credential_values() {
    let cases = [
        (config(&[]), WebsocketsClientConfigError::MissingEndpoint),
        (
            config(&[("endpoint", "http://secret.example.test")]),
            WebsocketsClientConfigError::InvalidEndpoint,
        ),
        (
            config(&[("endpoint", "ws://example.test/#secret")]),
            WebsocketsClientConfigError::InvalidEndpoint,
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("tls_ca_file", "secret")]),
            WebsocketsClientConfigError::TlsRequiresSecureEndpoint {
                key: WebsocketsClientKey::TlsCaFile,
            },
        ),
        (
            config(&[
                ("endpoint", "wss://example.test"),
                ("tls_cert_file", "secret"),
            ]),
            WebsocketsClientConfigError::UnpairedTlsIdentity,
        ),
        (
            config(&[
                ("endpoint", "wss://example.test"),
                ("tls_key_file", "secret"),
            ]),
            WebsocketsClientConfigError::UnpairedTlsIdentity,
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("ENDPOINT", "secret")]),
            WebsocketsClientConfigError::RepeatedKey {
                key: WebsocketsClientKey::Endpoint,
            },
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("unexpected", "secret")]),
            WebsocketsClientConfigError::UndeclaredKey {
                key: "unexpected".to_string(),
            },
        ),
    ];
    for (entries, expected) in cases {
        let Err(report) = WebsocketsClientConfig::parse(&entries) else {
            panic!("the supplied configuration breaks its declared rule");
        };
        assert_eq!(report.current_context(), &expected);
        assert!(!format!("{report:#}").contains("secret"));
    }
    let defaults = WebsocketsClientConfig::parse(&config(&[("endpoint", "ws://example.test")]))
        .assured("endpoint is the only required key");
    assert_eq!(
        defaults.connect_timeout(),
        WebsocketsClientConfig::CONNECT_TIMEOUT
    );
    assert_eq!(defaults.ping_interval(), None);
}

#[test]
fn bolero_client_config_preserves_every_valid_connection_setting() {
    bolero::check!().with_type::<[u16; 4]>().for_each(|input| {
        let secure = input[0].is_multiple_of(2);
        let scheme = if secure { "wss" } else { "ws" };
        let endpoint = format!(
            "{scheme}://peer.example.test/events/{}?stream={}",
            input[1], input[2]
        );
        let connect = u64::from(input[2]) + 1;
        let ping = u64::from(input[3]) + 1;
        let mut entries = config(&[
            ("endpoint", &endpoint),
            ("connect_timeout_ms", &connect.to_string()),
            ("ping_interval_ms", &ping.to_string()),
        ]);
        if secure {
            entries.extend(config(&[
                ("tls_ca_file", "ca.pem"),
                ("tls_cert_file", "cert.pem"),
                ("tls_key_file", "key.pem"),
            ]));
        }
        let parsed = WebsocketsClientConfig::parse(&entries).assured(
            "the bounded domain supplies every required setting with positive milliseconds and a \
             usable TLS pair",
        );
        assert_eq!(parsed.endpoint().as_str(), endpoint);
        assert_eq!(
            parsed.connect_timeout(),
            std::time::Duration::from_millis(connect)
        );
        assert_eq!(
            parsed.ping_interval(),
            Some(std::time::Duration::from_millis(ping))
        );
    });
}
