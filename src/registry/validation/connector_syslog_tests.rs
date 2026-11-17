//! Validation of the SYSLOG client a listener binds.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a listener accepts a client whose keys its side reads, refuses a key
//!   only collectors read and a host name in its bind address, naming the key and the side, and
//!   leaves a templated address to the check it runs once the client is rendered.
//! - **Depends on.** The connector validation, the registry and its test fixtures.
//! - **Must not know.** How a listener binds or reads its socket.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::fs;

use nervix_models::{
    ClientConfigSchemaError, GeneralErrorPolicy, IngestorEntity, SyslogListenerIngestor,
};

use super::*;
use crate::registry::{
    storage::Registry,
    test_fixtures::{
        codec, named, relay, schema, temp_db_path, unbranched_transforming_outputs, wire_schema,
    },
};

fn syslog_client(name: &str, config: &[(&str, &str)]) -> Model {
    Model::ClientSyslog(nervix_models::CreateClientSyslog {
        labels: Default::default(),
        name: named(name),
        mount: None,
        config: config
            .iter()
            .map(|(key, value)| nervix_models::ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect(),
    })
}

fn syslog_listener(name: &str, client: &str) -> Model {
    Model::Ingestor(CreateIngestor {
        labels: Default::default(),
        name: named(name),
        output_routes: unbranched_transforming_outputs("notifications"),
        input: nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
            source: IngestorEntity::from(SyslogListenerIngestor {
                client: named(client),
                mode: nervix_models::SyslogIngestMode::NoAck,
                quiesce: nervix_models::IngestQuiesceMode::Drop,
            }),
            codec: named("event_codec"),
        }),
        timestamp_source: None,
        general_error_policy: GeneralErrorPolicy::Log,
        filter_where: None,
    })
}

/// Applies a listener bound to a SYSLOG client that declares `config`.
fn apply_syslog_listener(config: &[(&str, &str)]) -> Result<(), Report<RegistryError>> {
    let path = temp_db_path();
    let registry = Registry::open(&path).expect("registry should open");
    let domain = DomainName::parse("default").expect("valid domain");
    let applied = registry.apply_batch(
        &domain,
        vec![
            schema("event_schema"),
            wire_schema("event_wire"),
            codec("event_codec", "event_schema"),
            syslog_client("syslog_main", config),
            relay("notifications", "event_schema"),
            syslog_listener("syslog_ing", "syslog_main"),
        ],
    );
    let _ = fs::remove_dir_all(path);
    applied.map(|_| ())
}

#[test]
fn a_syslog_listener_accepts_the_keys_its_side_reads() {
    apply_syslog_listener(&[
        ("protocol", "tcp"),
        ("addr", "127.0.0.1:5514"),
        ("framing", "non-transparent"),
        ("max_message_size", "4096"),
        ("idle_timeout_ms", "1000"),
    ])
    .expect("a listener reads every key of this client");
}

#[test]
fn a_syslog_listener_refuses_a_key_only_collectors_read_naming_the_key_and_side() {
    let error = apply_syslog_listener(&[
        ("protocol", "tcp"),
        ("addr", "127.0.0.1:5514"),
        ("timeout_ms", "1000"),
    ])
    .expect_err("a listener writes nothing for timeout_ms to bound");
    assert_eq!(
        error.current_context(),
        &RegistryError::IngestorClientConfig {
            domain: DomainName::parse("default").expect("valid domain"),
            ingestor: named("syslog_ing"),
            kind: nervix_models::IngestorEntityKind::Syslog(
                nervix_models::SyslogIngestorEntity::Listener
            ),
            client: named("syslog_main"),
        }
    );
    assert_eq!(
        error.downcast_ref::<ClientConfigSchemaError>(),
        Some(&ClientConfigSchemaError::ReadByOtherSide {
            key: "timeout_ms",
            readers: nervix_models::ClientConfigReaders::Emitters,
        })
    );
    assert_eq!(
        format!("{error:#}"),
        "SYSLOG LISTENER ingestor 'syslog_ing' in domain 'default' cannot use client \
         'syslog_main': client config key 'timeout_ms' is read only by emitters"
    );
}

#[test]
fn a_syslog_listener_refuses_a_host_name_in_its_bind_address() {
    let error = apply_syslog_listener(&[("protocol", "udp"), ("addr", "localhost:5514")])
        .expect_err("binding a listener asks no resolver");
    assert_eq!(
        error.downcast_ref::<ClientConfigSchemaError>(),
        Some(&ClientConfigSchemaError::InvalidValue {
            key: "addr".to_string(),
            expected: "literal IP address and port",
        })
    );
}

#[test]
fn a_syslog_listener_judges_a_templated_address_when_it_starts() {
    apply_syslog_listener(&[("protocol", "udp"), ("addr", "{{ bind }}:5514")])
        .expect("a template is judged once it is rendered");
}
