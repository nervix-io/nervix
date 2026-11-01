//! Owner tests for the MQTT client's declared CONFIG schema and its typed configuration.
//!
//! Layer: test harness.
//! - **Owns.** Current MQTT client vocabulary validation evidence.
//! - **Depends on.** The public vocabulary and standard value types.
//! - **Must not know.** The MQTT driver, templates or runtime plans.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns current vocabulary validation evidence")
)]

use std::{
    num::{NonZeroU16, NonZeroU32},
    path::Path,
};

use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    CLIENT_CONFIG, MQTT_MAXIMUM_PACKET_BYTES,
    config::{MqttClientConfig, MqttClientConfigError, MqttTransport},
};
use crate::{CLIENT_CONFIG_REDACTION, ClientConfigEntry, ClientConfigSchemaError};

fn config(entries: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    entries
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

/// The refusal `entries` meet.
fn refusal(entries: &[(&str, &str)]) -> MqttClientConfigError {
    let refused = MqttClientConfig::parse(&config(entries))
        .expect_err("the configuration breaks an MQTT rule");
    refused.current_context().clone()
}

#[test]
fn omitted_keys_take_their_declared_defaults_and_no_credentials() {
    let parsed = MqttClientConfig::parse(&config(&[("addr", "mqtt://broker.example.test:1883")]))
        .assured("addr is the only required key");
    assert_eq!(parsed.broker().host(), "broker.example.test");
    assert_eq!(parsed.broker().port().get(), 1883);
    assert_eq!(parsed.transport(), &MqttTransport::Tcp);
    assert!(!parsed.transport().uses_tls());
    assert_eq!(parsed.client_id(), None);
    assert_eq!(parsed.username(), None);
    assert!(parsed.password().is_none());
    assert_eq!(parsed.keep_alive_seconds().get(), 60);
    assert_eq!(parsed.max_packet_size().get(), 10_240);

    // Inspection shows the same declared defaults the typed configuration takes.
    let effective =
        CLIENT_CONFIG.effective_entries(&config(&[("addr", "mqtt://broker.example.test:1883")]));
    let effective: Vec<_> = effective
        .iter()
        .map(|entry| (entry.key.as_str(), entry.value.as_str()))
        .collect();
    assert_eq!(
        effective,
        vec![
            ("addr", "mqtt://broker.example.test:1883"),
            ("keep_alive_ms", "60000"),
            ("max_packet_size", "10240"),
        ]
    );
}

#[test]
fn supplied_values_convert_once_into_the_connection_they_describe() {
    let parsed = MqttClientConfig::parse(&config(&[
        ("addr", "mqtts://[::1]:8883"),
        ("client_id", "orders-reader-0"),
        ("username", "nervix"),
        ("password", "s3cret"),
        ("keep_alive_ms", "7000"),
        ("max_packet_size", "32768"),
        ("tls_ca_file", "/certs/ca.pem"),
        ("tls_cert_file", "/certs/client.pem"),
        ("tls_key_file", "/certs/client.key"),
    ]))
    .assured("every declared key is set once with a usable value");
    assert_eq!(parsed.broker().host(), "::1");
    assert_eq!(parsed.broker().port().get(), 8883);
    assert!(parsed.transport().uses_tls());
    let MqttTransport::Tls(files) = parsed.transport() else {
        panic!("an mqtts address connects over TLS");
    };
    assert_eq!(files.certificate_authority(), Path::new("/certs/ca.pem"));
    let identity = files
        .identity()
        .assured("the configuration presents a client certificate");
    assert_eq!(identity.certificate(), Path::new("/certs/client.pem"));
    assert_eq!(identity.private_key(), Path::new("/certs/client.key"));
    assert_eq!(parsed.client_id(), Some("orders-reader-0"));
    assert_eq!(parsed.username(), Some("nervix"));
    assert_eq!(
        parsed.password().map(|password| password.expose()),
        Some("s3cret")
    );
    assert_eq!(parsed.keep_alive_seconds().get(), 7);
    assert_eq!(parsed.max_packet_size().get(), 32_768);
}

#[test]
fn a_password_alone_is_what_mqtt_5_lets_a_client_write() {
    let parsed = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("password", "token"),
    ]))
    .assured("MQTT 5 carries a password without a username");
    assert_eq!(parsed.username(), None);
    assert_eq!(
        parsed.password().map(|password| password.expose()),
        Some("token")
    );
}

#[test]
fn a_password_never_appears_in_a_debug_rendering_or_public_inspection() {
    let entries = config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("username", "nervix"),
        ("password", "private-mqtt-password"),
    ]);
    let parsed = MqttClientConfig::parse(&entries).assured("the credentials are MQTT strings");
    let rendered = format!("{parsed:?}");
    assert!(!rendered.contains("private-mqtt-password"), "{rendered}");
    assert!(rendered.contains(CLIENT_CONFIG_REDACTION), "{rendered}");
    let inspected = CLIENT_CONFIG.inspection_entries(&entries);
    let password = inspected
        .iter()
        .find(|entry| entry.key == "password")
        .assured("inspection keeps every entry");
    assert_eq!(password.value, CLIENT_CONFIG_REDACTION);
    let username = inspected
        .iter()
        .find(|entry| entry.key == "username")
        .assured("inspection keeps every entry");
    assert_eq!(username.value, "nervix");
}

#[test]
fn a_keep_alive_must_be_whole_seconds_mqtt_can_carry() {
    for keep_alive in ["1500", "999", "65536000", "86400000"] {
        assert_eq!(
            refusal(&[
                ("addr", "mqtt://127.0.0.1:1883"),
                ("keep_alive_ms", keep_alive)
            ]),
            MqttClientConfigError::KeepAlive,
            "keep_alive_ms {keep_alive}"
        );
    }
    for (keep_alive, seconds) in [("1000", 1), ("65535000", 65_535)] {
        let parsed = MqttClientConfig::parse(&config(&[
            ("addr", "mqtt://127.0.0.1:1883"),
            ("keep_alive_ms", keep_alive),
        ]))
        .assured("a whole number of seconds from 1 through 65,535");
        assert_eq!(parsed.keep_alive_seconds().get(), seconds);
    }
    let zero = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("keep_alive_ms", "0"),
    ]))
    .expect_err("a keep-alive of zero turns the mechanism off");
    assert_eq!(zero.current_context(), &MqttClientConfigError::Schema);
    assert!(matches!(
        zero.downcast_ref::<ClientConfigSchemaError>(),
        Some(ClientConfigSchemaError::InvalidValue { key, .. }) if key == "keep_alive_ms"
    ));
}

#[test]
fn a_publish_admission_bound_must_fit_mqtts_largest_packet() {
    let largest = MQTT_MAXIMUM_PACKET_BYTES.to_string();
    let parsed = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("max_packet_size", largest.as_str()),
    ]))
    .assured("MQTT's largest packet is a usable bound");
    assert_eq!(
        u64::from(parsed.max_packet_size().get()),
        MQTT_MAXIMUM_PACKET_BYTES
    );
    let beyond = (MQTT_MAXIMUM_PACKET_BYTES + 1).to_string();
    for bound in [beyond.as_str(), "18446744073709551615"] {
        assert_eq!(
            refusal(&[
                ("addr", "mqtt://127.0.0.1:1883"),
                ("max_packet_size", bound)
            ]),
            MqttClientConfigError::PacketSizeAboveMaximum {
                maximum: MQTT_MAXIMUM_PACKET_BYTES
            },
            "max_packet_size {bound}"
        );
    }
}

#[test]
fn an_identity_and_a_username_must_be_strings_mqtt_can_carry() {
    assert_eq!(
        refusal(&[("addr", "mqtt://127.0.0.1:1883"), ("client_id", "")]),
        MqttClientConfigError::EmptyClientId
    );
    assert_eq!(
        refusal(&[("addr", "mqtt://127.0.0.1:1883"), ("client_id", "a\0b")]),
        MqttClientConfigError::NulCharacter { key: "client_id" }
    );
    assert_eq!(
        refusal(&[("addr", "mqtt://127.0.0.1:1883"), ("username", "a\0b")]),
        MqttClientConfigError::NulCharacter { key: "username" }
    );
    let longest = "x".repeat(65_535);
    let parsed = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("client_id", longest.as_str()),
        ("username", longest.as_str()),
        ("password", longest.as_str()),
    ]))
    .assured("65,535 bytes fit MQTT's Two Byte Integer length");
    assert_eq!(parsed.client_id().map(str::len), Some(65_535));
    let too_long = "x".repeat(65_536);
    for key in ["client_id", "username", "password"] {
        assert_eq!(
            refusal(&[("addr", "mqtt://127.0.0.1:1883"), (key, too_long.as_str())]),
            MqttClientConfigError::TooLong { key },
            "{key}"
        );
    }
    // An empty username is a zero-length MQTT string, which a CONNECT can carry.
    let parsed = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("username", ""),
    ]))
    .assured("a zero-length username is an MQTT string");
    assert_eq!(parsed.username(), Some(""));
}

#[test]
fn a_refusal_states_the_limit_mqtt_sets_and_no_configured_value() {
    assert_eq!(
        MqttClientConfigError::TooLong { key: "password" }.to_string(),
        "'password' must be at most 65535 bytes"
    );
    assert_eq!(
        MqttClientConfigError::KeepAlive.to_string(),
        "'keep_alive_ms' must be whole seconds from 1 s through 65535 s"
    );
    assert_eq!(
        MqttClientConfigError::PacketSizeAboveMaximum {
            maximum: MQTT_MAXIMUM_PACKET_BYTES
        }
        .to_string(),
        "'max_packet_size' must be at most 268435460"
    );
}

#[test]
fn a_configuration_the_schema_refuses_keeps_the_schemas_refusal_beneath() {
    let refused = MqttClientConfig::parse(&config(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("keepalive", "30"),
    ]))
    .expect_err("an undeclared key is refused");
    assert_eq!(refused.current_context(), &MqttClientConfigError::Schema);
    assert!(matches!(
        refused.downcast_ref::<ClientConfigSchemaError>(),
        Some(ClientConfigSchemaError::UnknownKey { key }) if key == "keepalive"
    ));
}

#[test]
fn bolero_mqtt_client_config_preserves_every_valid_connection_setting() {
    bolero::check!().with_type::<[u32; 8]>().for_each(|input| {
        let secure = !input[0].is_multiple_of(2);
        let scheme = if secure { "mqtts" } else { "mqtt" };
        let hosts = ["broker.example.test", "192.0.2.7", "2001:db8::7"];
        let host_index = usize::try_from(input[0] / 2 % 3)
            .assured("an index below three fits every supported target");
        let host = hosts[host_index];
        let authority_host = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        let port = u16::try_from(input[1] % 65_535 + 1)
            .assured("a port from 1 through 65,535 fits sixteen bits");
        let address = format!("{scheme}://{authority_host}:{port}");
        let mut entries = config(&[("addr", address.as_str())]);
        let flags = input[2];
        let client_id = format!("nervix-ünïcødé-{}", input[3]);
        if flags & 1 != 0 {
            entries.extend(config(&[("client_id", client_id.as_str())]));
        }
        let username = format!("reader-{}", input[4]);
        if flags & 2 != 0 {
            entries.extend(config(&[("username", username.as_str())]));
        }
        let password = format!("p\u{e4}ss-{}", input[5]);
        if flags & 4 != 0 {
            entries.extend(config(&[("password", password.as_str())]));
        }
        let keep_alive_seconds = input[6] % 65_535 + 1;
        let keep_alive_ms = (u64::from(keep_alive_seconds) * 1000).to_string();
        if flags & 8 != 0 {
            entries.extend(config(&[("keep_alive_ms", keep_alive_ms.as_str())]));
        }
        let max_packet_size = u64::from(input[7]) % MQTT_MAXIMUM_PACKET_BYTES + 1;
        let max_packet_size_text = max_packet_size.to_string();
        if flags & 16 != 0 {
            entries.extend(config(&[(
                "max_packet_size",
                max_packet_size_text.as_str(),
            )]));
        }
        let identity = flags & 32 != 0;
        if secure {
            entries.extend(config(&[("tls_ca_file", "/certs/ca.pem")]));
            if identity {
                entries.extend(config(&[
                    ("tls_cert_file", "/certs/client.pem"),
                    ("tls_key_file", "/certs/client.key"),
                ]));
            }
        }
        let parsed = MqttClientConfig::parse(&entries).assured(
            "the bounded domain writes a host-and-port address, MQTT strings, whole keep-alive \
             seconds, a bound within MQTT's largest packet and the TLS files its scheme needs",
        );
        assert_eq!(parsed.broker().host(), host);
        assert_eq!(parsed.broker().port().get(), port);
        match parsed.transport() {
            MqttTransport::Tcp => assert!(!secure),
            MqttTransport::Tls(files) => {
                assert!(secure);
                assert_eq!(files.certificate_authority(), Path::new("/certs/ca.pem"));
                assert_eq!(files.identity().is_some(), identity);
            }
        }
        let expected_client_id = (flags & 1 != 0).then_some(client_id.as_str());
        assert_eq!(parsed.client_id(), expected_client_id);
        let expected_username = (flags & 2 != 0).then_some(username.as_str());
        assert_eq!(parsed.username(), expected_username);
        let expected_password = (flags & 4 != 0).then_some(password.as_str());
        assert_eq!(
            parsed.password().map(|password| password.expose()),
            expected_password
        );
        let expected_keep_alive = if flags & 8 != 0 {
            u16::try_from(keep_alive_seconds).assured("at most 65,535 seconds were generated")
        } else {
            60
        };
        assert_eq!(
            parsed.keep_alive_seconds(),
            NonZeroU16::new(expected_keep_alive).assured("the keep-alive is positive")
        );
        let expected_bound = if flags & 16 != 0 {
            u32::try_from(max_packet_size).assured("MQTT's largest packet fits thirty-two bits")
        } else {
            10_240
        };
        assert_eq!(
            parsed.max_packet_size(),
            NonZeroU32::new(expected_bound).assured("the bound is positive")
        );
    });
}
