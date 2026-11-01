//! Owner tests for the options one MQTT client configuration builds.

use std::time::Duration;

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::ClientConfigError;
use nervix_models::{ClientConfigEntry, MqttClientConfigError};
use rumqttc::{ConnectAuth, TlsConfiguration, Transport as DriverTransport};

use super::{MqttClientOptions, MqttClientOptionsError};

fn config(entries: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
    entries
        .iter()
        .map(|(key, value)| ClientConfigEntry {
            key: (*key).to_string(),
            value: (*value).to_string(),
        })
        .collect()
}

fn options(entries: &[(&str, &str)]) -> MqttClientOptions {
    MqttClientOptions::from_config(&config(entries), "sales~orders")
        .assured("the configuration is a valid MQTT client configuration")
}

#[test]
fn a_connection_presents_the_declared_identity_or_its_entitys_default() {
    let declared = options(&[
        ("addr", "mqtt://broker.example.test:1883"),
        ("client_id", "orders-reader-1"),
    ]);
    assert_eq!(declared.options.client_id(), "orders-reader-1");
    assert_eq!(
        declared.options.broker().tcp_address(),
        Some(("broker.example.test", 1883))
    );
    let default = options(&[("addr", "mqtt://[2001:db8::7]:1884")]);
    assert_eq!(default.options.client_id(), "sales~orders");
    assert_eq!(
        default.options.broker().tcp_address(),
        Some(("2001:db8::7", 1884))
    );
}

#[test]
fn a_connection_authenticates_with_exactly_the_credentials_the_client_declares() {
    let both = options(&[
        ("addr", "mqtt://127.0.0.1:1883"),
        ("username", "nervix"),
        ("password", "s3cret"),
    ]);
    assert_eq!(
        both.options.auth(),
        &ConnectAuth::UsernamePassword {
            username: "nervix".to_string(),
            password: Bytes::from_static(b"s3cret"),
        }
    );
    let username = options(&[("addr", "mqtt://127.0.0.1:1883"), ("username", "nervix")]);
    assert_eq!(
        username.options.auth(),
        &ConnectAuth::Username {
            username: "nervix".to_string(),
        }
    );
    let password = options(&[("addr", "mqtt://127.0.0.1:1883"), ("password", "token")]);
    assert_eq!(
        password.options.auth(),
        &ConnectAuth::Password {
            password: Bytes::from_static(b"token"),
        }
    );
    let anonymous = options(&[("addr", "mqtt://127.0.0.1:1883")]);
    assert_eq!(anonymous.options.auth(), &ConnectAuth::None);
}

#[test]
fn a_connection_asks_for_the_declared_keep_alive_or_its_default() {
    let declared = options(&[("addr", "mqtt://127.0.0.1:1883"), ("keep_alive_ms", "7000")]);
    assert_eq!(declared.options.keep_alive(), Duration::from_secs(7));
    let default = options(&[("addr", "mqtt://127.0.0.1:1883")]);
    assert_eq!(default.options.keep_alive(), Duration::from_secs(60));
}

#[test]
fn a_plain_address_connects_without_tls() {
    let plain = options(&[("addr", "mqtt://127.0.0.1:1883")]);
    assert!(!plain.config().transport().uses_tls());
    assert!(matches!(plain.options.transport(), DriverTransport::Tcp));
    assert_eq!(plain.config().max_packet_size().get(), 10_240);
}

#[test]
fn a_tls_address_carries_its_trust_root_and_the_identity_it_presents() {
    let files = tempfile::tempdir().assured("a temporary directory can be created");
    let ca = files.path().join("ca.pem");
    let certificate = files.path().join("client.pem");
    let private_key = files.path().join("client.key");
    std::fs::write(&ca, b"test CA").assured("the CA fixture can be written");
    std::fs::write(&certificate, b"test certificate")
        .assured("the certificate fixture can be written");
    std::fs::write(&private_key, b"test key").assured("the key fixture can be written");
    let ca = ca.to_str().assured("a temporary path is UTF-8");
    let certificate = certificate.to_str().assured("a temporary path is UTF-8");
    let private_key = private_key.to_str().assured("a temporary path is UTF-8");

    let presenting = options(&[
        ("addr", "mqtts://broker.example.test:8883"),
        ("tls_ca_file", ca),
        ("tls_cert_file", certificate),
        ("tls_key_file", private_key),
    ]);
    assert!(presenting.config().transport().uses_tls());
    let DriverTransport::Tls(TlsConfiguration::Simple {
        ca: trust_root,
        alpn,
        client_auth,
    }) = presenting.options.transport()
    else {
        panic!("an mqtts address connects through TLS");
    };
    assert_eq!(trust_root, b"test CA".to_vec());
    assert_eq!(alpn, None);
    assert_eq!(
        client_auth,
        Some((b"test certificate".to_vec(), b"test key".to_vec()))
    );

    let trusting = options(&[
        ("addr", "mqtts://broker.example.test:8883"),
        ("tls_ca_file", ca),
    ]);
    let DriverTransport::Tls(TlsConfiguration::Simple {
        ca: trust_root,
        client_auth,
        ..
    }) = trusting.options.transport()
    else {
        panic!("an mqtts address connects through TLS");
    };
    assert_eq!(trust_root, b"test CA".to_vec());
    assert_eq!(client_auth, None);
}

#[test]
fn an_unreadable_tls_file_is_refused_with_the_file_and_its_cause() {
    let refused = MqttClientOptions::from_config(
        &config(&[
            ("addr", "mqtts://127.0.0.1:8883"),
            ("tls_ca_file", "/nonexistent/nervix-mqtt-ca.pem"),
        ]),
        "sales~orders",
    )
    .err()
    .assured("no CA file exists at the configured path");
    assert!(matches!(
        refused.current_context(),
        MqttClientOptionsError::TlsFile {
            file: "TLS CA certificate"
        }
    ));
    assert!(matches!(
        refused.downcast_ref::<ClientConfigError>(),
        Some(ClientConfigError::ReadTlsFile { .. })
    ));
}

#[test]
fn a_configuration_mqtt_cannot_carry_is_refused_with_the_vocabularys_cause() {
    let refused = MqttClientOptions::from_config(
        &config(&[("addr", "mqtt://127.0.0.1:1883"), ("keep_alive_ms", "1500")]),
        "sales~orders",
    )
    .err()
    .assured("a keep-alive of a second and a half is not whole seconds");
    assert!(matches!(
        refused.current_context(),
        MqttClientOptionsError::Configuration
    ));
    assert_eq!(
        refused.downcast_ref::<MqttClientConfigError>(),
        Some(&MqttClientConfigError::KeepAlive)
    );
}
