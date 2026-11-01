//! Owner tests for the WebSockets client's declared CONFIG schema and its typed configuration.
//!
//! Layer: test harness.
//! - **Owns.** Current WebSockets vocabulary validation evidence.
//! - **Depends on.** The public vocabulary and standard value types.
//! - **Must not know.** Network drivers or runtime plans.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns current vocabulary validation evidence")
)]

use std::time::Duration;

use meticulous::ResultExt as _;

use super::config::{CLIENT_CONFIG, WebsocketsClientConfig, WebsocketsClientConfigError};
use crate::{ClientConfigEntry, ClientConfigSchemaError};

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
fn supplied_values_convert_once_and_omitted_counts_take_their_declared_defaults() {
    let parsed = WebsocketsClientConfig::parse(&config(&[
        ("endpoint", "wss://example.test/events?token=opaque"),
        ("connect_timeout_ms", "123"),
        ("ping_interval_ms", "456"),
        ("tls_ca_file", "{{ certs }}/ca.pem"),
        ("tls_cert_file", "{{ certs }}/cert.pem"),
        ("tls_key_file", "{{ certs }}/key.pem"),
    ]))
    .assured("all declared keys are set once with usable values");
    assert_eq!(parsed.connect_timeout(), Duration::from_millis(123));
    assert_eq!(parsed.ping_interval(), Some(Duration::from_millis(456)));
    assert_eq!(parsed.idle_timeout(), Duration::from_millis(3 * 456));
    assert_eq!(parsed.endpoint().path(), "/events");

    let defaults = WebsocketsClientConfig::parse(&config(&[("endpoint", "ws://example.test")]))
        .assured("endpoint is the only required key");
    assert_eq!(defaults.connect_timeout(), crate::DEFAULT_CONNECT_TIMEOUT);
    assert_eq!(defaults.ping_interval(), None);
    assert_eq!(defaults.idle_timeout(), Duration::from_secs(30));
    assert_eq!(defaults.handshake_buffer_capacity(), 16);
    assert_eq!(defaults.max_message_size(), 64 * 1024 * 1024);
    assert_eq!(defaults.max_frame_size(), 16 * 1024 * 1024);

    // Inspection shows the same declared defaults; the idle deadline depends on the ping interval,
    // so it has none.
    let effective = CLIENT_CONFIG.effective_entries(&config(&[("endpoint", "ws://example.test")]));
    let effective: Vec<_> = effective
        .iter()
        .map(|entry| (entry.key.as_str(), entry.value.as_str()))
        .collect();
    assert_eq!(
        effective,
        [
            ("endpoint", "ws://example.test"),
            ("handshake_buffer_capacity", "16"),
            ("max_frame_size", "16777216"),
            ("max_message_size", "67108864"),
        ]
    );
}

#[test]
fn the_declared_schema_refuses_what_the_client_cannot_read_and_never_prints_values() {
    let mut cases = vec![
        (
            config(&[]),
            ClientConfigSchemaError::MissingRequired { key: "endpoint" },
        ),
        (
            config(&[("endpoint", "http://secret.example.test")]),
            ClientConfigSchemaError::InvalidValue {
                key: "endpoint".to_string(),
                expected: "URL",
            },
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("tls_ca_file", "secret")]),
            ClientConfigSchemaError::RequiresScheme {
                key: "tls_ca_file",
                address: "endpoint",
                scheme: "wss",
            },
        ),
        (
            config(&[
                ("endpoint", "wss://example.test"),
                ("tls_cert_file", "secret"),
            ]),
            ClientConfigSchemaError::RequiresKey {
                key: "tls_cert_file",
                required: "tls_key_file",
            },
        ),
        (
            config(&[
                ("endpoint", "wss://example.test"),
                ("tls_key_file", "secret"),
            ]),
            ClientConfigSchemaError::RequiresKey {
                key: "tls_key_file",
                required: "tls_cert_file",
            },
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("endpoint", "secret")]),
            ClientConfigSchemaError::DuplicateKey {
                key: "endpoint".to_string(),
            },
        ),
        (
            config(&[("endpoint", "ws://example.test"), ("unexpected", "secret")]),
            ClientConfigSchemaError::UnknownKey {
                key: "unexpected".to_string(),
            },
        ),
    ];
    for key in ["connect_timeout_ms", "ping_interval_ms", "idle_timeout_ms"] {
        for value in [
            "0",
            "-1",
            "+1",
            "1.5",
            "1s",
            "18446744073709551615",
            "18446744073709551616",
        ] {
            cases.push((
                config(&[("endpoint", "ws://example.test"), (key, value)]),
                ClientConfigSchemaError::InvalidValue {
                    key: key.to_string(),
                    expected: "positive, schedulable whole milliseconds",
                },
            ));
        }
    }
    for key in [
        "handshake_buffer_capacity",
        "max_message_size",
        "max_frame_size",
    ] {
        for value in ["0", "-1", "+1", "1.5", "1MiB"] {
            cases.push((
                config(&[("endpoint", "ws://example.test"), (key, value)]),
                ClientConfigSchemaError::InvalidValue {
                    key: key.to_string(),
                    expected: "positive integer",
                },
            ));
        }
    }
    for (entries, expected) in cases {
        let Err(report) = WebsocketsClientConfig::parse(&entries) else {
            panic!("the supplied configuration breaks a declared key rule");
        };
        assert_eq!(
            report.current_context(),
            &WebsocketsClientConfigError::Schema
        );
        assert_eq!(
            report.downcast_ref::<ClientConfigSchemaError>(),
            Some(&expected)
        );
        assert!(!format!("{report:#}").contains("secret"));
    }
}

#[test]
fn the_typed_configuration_refuses_what_the_schema_cannot_state() {
    let Err(fragment) =
        WebsocketsClientConfig::parse(&config(&[("endpoint", "ws://example.test/#secret")]))
    else {
        panic!("a WebSocket URI never has a fragment");
    };
    assert_eq!(
        fragment.current_context(),
        &WebsocketsClientConfigError::EndpointFragment
    );
    assert!(!format!("{fragment:#}").contains("secret"));

    for (key, largest, above, maximum) in [
        ("handshake_buffer_capacity", "65535", "65536", 65_535),
        ("max_message_size", "4294967295", "4294967296", u32::MAX),
        ("max_frame_size", "4294967295", "4294967296", u32::MAX),
    ] {
        WebsocketsClientConfig::parse(&config(&[("endpoint", "ws://peer.test"), (key, largest)]))
            .assured("the largest count the representation holds is accepted");
        let Err(report) =
            WebsocketsClientConfig::parse(&config(&[("endpoint", "ws://peer.test"), (key, above)]))
        else {
            panic!("a count above its representation is refused");
        };
        assert_eq!(
            report.current_context(),
            &WebsocketsClientConfigError::AboveMaximum { key, maximum }
        );
    }

    let derived = WebsocketsClientConfig::parse(&config(&[
        ("endpoint", "ws://peer.test"),
        ("ping_interval_ms", "10"),
    ]))
    .assured("the derived 30ms bound is schedulable");
    assert_eq!(derived.idle_timeout(), Duration::from_millis(30));
    let explicit = WebsocketsClientConfig::parse(&config(&[
        ("endpoint", "ws://peer.test"),
        ("ping_interval_ms", "10"),
        ("idle_timeout_ms", "75"),
    ]))
    .assured("an explicit idle value controls the read");
    assert_eq!(explicit.idle_timeout(), Duration::from_millis(75));
    let maximum = (crate::ClientDeadline::LONGEST.as_millis() / 2).to_string();
    let Err(report) = WebsocketsClientConfig::parse(&config(&[
        ("endpoint", "ws://peer.test"),
        ("ping_interval_ms", &maximum),
    ])) else {
        panic!("three intervals exceed the schedulable deadline bound");
    };
    assert_eq!(
        report.current_context(),
        &WebsocketsClientConfigError::IdleDerivationTooLong
    );
}

#[test]
fn bolero_client_config_preserves_every_valid_connection_setting() {
    bolero::check!().with_type::<[u16; 8]>().for_each(|input| {
        let secure = input[0].is_multiple_of(2);
        let scheme = if secure { "wss" } else { "ws" };
        let endpoint = format!(
            "{scheme}://peer.example.test/events/{}?stream={}",
            input[1], input[2]
        );
        let connect = u64::from(input[2]) + 1;
        let ping = u64::from(input[3]) + 1;
        let idle = u64::from(input[4]) + 1;
        let capacity = input[5].max(1);
        let message_size = u32::from(input[6]) + 1;
        let frame_size = u32::from(input[7]) + 1;
        let mut entries = config(&[
            ("endpoint", &endpoint),
            ("connect_timeout_ms", &connect.to_string()),
            ("ping_interval_ms", &ping.to_string()),
            ("handshake_buffer_capacity", &capacity.to_string()),
            ("max_message_size", &message_size.to_string()),
            ("max_frame_size", &frame_size.to_string()),
        ]);
        let explicit_idle = input[0].is_multiple_of(3);
        if explicit_idle {
            entries.extend(config(&[("idle_timeout_ms", &idle.to_string())]));
        }
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
        let expected_idle = if explicit_idle { idle } else { ping * 3 };
        assert_eq!(
            parsed.idle_timeout(),
            std::time::Duration::from_millis(expected_idle)
        );
        assert_eq!(parsed.handshake_buffer_capacity(), usize::from(capacity));
        assert_eq!(
            parsed.max_message_size(),
            usize::try_from(message_size)
                .assured("the generated u16-sized count fits every supported target")
        );
        assert_eq!(
            parsed.max_frame_size(),
            usize::try_from(frame_size)
                .assured("the generated u16-sized count fits every supported target")
        );
    });
}
