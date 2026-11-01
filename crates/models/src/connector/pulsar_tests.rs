//! Tests of the Pulsar vocabulary's executable topic contract.
//!
//! Layer: test harness.
//! - **Owns.** Pulsar declaration, delivery-mode, client configuration and topic-name contract
//!   checks through the public vocabulary.
//! - **Depends on.** Pulsar Models, entity kinds and shared declaration types.
//! - **Must not know.** The Pulsar driver, parsing, planning or how a topic is consumed or
//!   produced to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigFile, ClientConfigOwner, ClientConfigPresence,
    ClientConfigSchema, ClientConfigSchemaError, ClientConfigValue, ClientPooling,
    EmitterEntityKind, EmitterPublishingModeKind, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityKind,
};

#[test]
fn topic_declarations_match_the_subscription_and_producer_transport() {
    let ingest = IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic).capabilities();

    assert_eq!(
        ingest.delivery,
        &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ]
    );
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::AckRequest),
        None
    );
    assert_eq!(ingest.position_class, PositionClass::Replayable);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::Answered
    );
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(ingest.quiesce, &[DeclaredQuiesce::SUSPEND]);
    for undeclared in [
        IngestQuiesceKind::Buffer,
        IngestQuiesceKind::Drop,
        IngestQuiesceKind::Reject,
    ] {
        assert_eq!(ingest.declared_quiesce(undeclared), None);
    }
    assert_eq!(
        emit.publishing_modes,
        &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::BrokerAck
        ]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert_eq!(
        emit.batch,
        EmitterBatchLegality::Optional { maximum: None },
        "the producer learns the broker's maxMessageSize when it connects"
    );
    assert!(!emit.requires_batch_transformation);
}

#[test]
fn both_topic_directions_share_the_topic_rule_and_the_client() {
    let ingest = IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic).capabilities();
    let topic = Some(LocatorDeclaration {
        keywords: "TOPIC",
        name: LocatorName::External(PulsarTopicName::DECLARATION),
    });

    assert_eq!(ingest.locator, topic);
    assert_eq!(emit.locator, topic);
    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Pulsar)
    );
    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Pulsar)
    );
    assert_eq!(
        CreateClientPulsar::<u64>::DECLARATION.pooling,
        ClientPooling::Unpooled
    );
    assert!(CreateClientPulsar::<u64>::DECLARATION.options.is_empty());
}

#[test]
fn topic_declares_message_properties_as_headers_and_no_envelope() {
    let ingest = IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic).capabilities();

    assert_eq!(
        ingest.headers,
        Some(HeaderModel::exact(HeaderLayout::OrderedRepeats))
    );
    assert_eq!(
        emit.headers,
        Some(HeaderModel::exact(HeaderLayout::LastWins))
    );
    assert_eq!(ingest.metadata, IngestorMetadataDeclaration::NONE);
    assert!(ingest.metadata.fields().is_empty());
    assert!(emit.options.is_empty());
    assert!(emit.envelope.is_empty());
    assert!(emit.properties.is_empty());
}

#[test]
fn the_subscription_is_required_with_its_owner_and_instances_are_optional() {
    let ingest = IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic).capabilities();
    let [subscription, instances] = ingest.options else {
        panic!("a Pulsar topic ingestor declares its subscription and its instance count");
    };

    assert_eq!(subscription.keywords, "SUBSCRIPTION");
    assert_eq!(subscription.presence, OptionPresence::Required);
    let OptionForm::Choice([owned, provisioned]) = subscription.form else {
        panic!("SUBSCRIPTION names its subscription and then who owns it");
    };
    assert_eq!(owned.name, PulsarSubscriptionOwnership::Ingestor.keywords());
    assert_eq!(
        owned.value,
        Some(OptionValue::Name(PulsarSubscriptionName::DECLARATION))
    );
    assert_eq!(
        provisioned.name,
        PulsarSubscriptionOwnership::Provisioned.keywords()
    );
    assert_eq!(
        provisioned.value,
        Some(OptionValue::Name(PulsarSubscriptionName::DECLARATION))
    );
    assert_eq!(*instances, OptionDeclaration::INSTANCES);
}

#[test]
fn no_client_deadline_key_bounds_a_pulsar_wait() {
    let ingest = IngestorEntityKind::Pulsar(PulsarIngestorEntity::Topic).capabilities();
    let emit = EmitterEntityKind::Pulsar(PulsarEmitterEntity::Topic).capabilities();

    assert!(ingest.bounds.deadline_keys().is_empty());
    assert!(emit.bounds.deadline_keys().is_empty());
}

fn retry_policy() -> RetryPolicy {
    RetryPolicy {
        backoff: "1s".to_string(),
        max_backoff: "30s".to_string(),
    }
}

#[test]
fn each_delivery_mode_acknowledges_with_its_own_window() {
    assert!(matches!(
        PulsarIngestMode::NoAck.acknowledgement(),
        IngestAcknowledgement::Unacknowledged
    ));

    let sequential = PulsarIngestMode::AckSequential {
        timeout: "5s".to_string(),
        retry_policy: retry_policy(),
    };
    let IngestAcknowledgement::Sequential { timeout, retry } = sequential.acknowledgement() else {
        panic!("ACK SEQUENTIAL keeps one acknowledgement outstanding");
    };
    assert_eq!(timeout, "5s");
    assert_eq!(retry, &retry_policy());

    let max = NonZeroU64::new(8).assured("eight is nonzero");
    let parallel = PulsarIngestMode::AckParallel {
        max,
        batch_timeout: "100ms".to_string(),
        timeout: "5s".to_string(),
        retry_policy: retry_policy(),
    };
    let IngestAcknowledgement::Parallel {
        max: window,
        batch_timeout,
        timeout,
        retry,
    } = parallel.acknowledgement()
    else {
        panic!("ACK PARALLEL keeps up to MAX acknowledgements outstanding");
    };
    assert_eq!(window, max);
    assert_eq!(batch_timeout, "100ms");
    assert_eq!(timeout, "5s");
    assert_eq!(retry, &retry_policy());
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn client_schema() -> ClientConfigSchema {
    CreateClientPulsar::<u64>::DECLARATION.config
}

#[test]
fn client_schema_declares_exactly_the_keys_both_directions_read() {
    let config = client_schema();
    let declared: Vec<&str> = config.keys().iter().map(|key| key.name).collect();

    assert_eq!(
        declared,
        vec![
            "addr",
            "auth_token",
            "namespace",
            "tls_allow_insecure_connection",
            "tls_ca_file",
            "tls_cert_file",
            "tls_hostname_verification_enabled",
            "tls_key_file",
        ]
    );
    let addr = config
        .key("addr")
        .assured("the declared key list above names the broker address");
    assert_eq!(addr.presence, ClientConfigPresence::Required);
    assert_eq!(
        addr.value,
        ClientConfigValue::Url(&["pulsar", "pulsar+ssl"])
    );
    let namespace = config
        .key("namespace")
        .assured("the declared key list above names the namespace");
    assert_eq!(
        namespace.presence,
        ClientConfigPresence::Defaulted("public/default")
    );
    assert_eq!(
        namespace.value,
        ClientConfigValue::Levels("tenant/namespace")
    );
    let trust = config
        .key("tls_ca_file")
        .assured("the declared key list above names the trusted CA file");
    assert_eq!(
        trust.value,
        ClientConfigValue::File(ClientConfigFile::CertificateAuthority)
    );
    let certificate = config
        .key("tls_cert_file")
        .assured("the declared key list above names the client certificate");
    assert_eq!(
        certificate.value,
        ClientConfigValue::File(ClientConfigFile::Certificate)
    );
    let private_key = config
        .key("tls_key_file")
        .assured("the declared key list above names the client certificate's key");
    assert_eq!(
        private_key.value,
        ClientConfigValue::File(ClientConfigFile::PrivateKey)
    );
    let token = config
        .key("auth_token")
        .assured("the declared key list above names the token");
    assert_eq!(token.presence, ClientConfigPresence::Optional);
    assert_eq!(token.value, ClientConfigValue::String);
    for flag in [
        "tls_allow_insecure_connection",
        "tls_hostname_verification_enabled",
    ] {
        let key = config
            .key(flag)
            .assured("the declared key list above names both TLS flags");
        assert_eq!(key.value, ClientConfigValue::Boolean);
    }
    for tls in [
        "tls_allow_insecure_connection",
        "tls_ca_file",
        "tls_cert_file",
        "tls_hostname_verification_enabled",
        "tls_key_file",
    ] {
        let key = config
            .key(tls)
            .assured("the declared key list above names every TLS key");
        assert_eq!(
            key.presence,
            ClientConfigPresence::OptionalForScheme {
                address: "addr",
                scheme: "pulsar+ssl",
            },
            "{tls} applies only to a TLS connection"
        );
    }
    for key in config.keys() {
        assert_eq!(key.owner, ClientConfigOwner::Nervix);
        assert_eq!(
            key.confidential,
            key.name == "auth_token",
            "only the token is a secret, not {}",
            key.name
        );
    }
    assert_eq!(config.ownership_label(), "Nervix");
    assert!(config.namespaces().is_empty());
    assert!(config.reserved().is_empty());
}

#[test]
fn client_schema_accepts_every_declared_key_on_a_tls_address() {
    client_schema()
        .validate(&[
            config_entry("addr", "pulsar+ssl://broker.example.com:6651"),
            config_entry("auth_token", "issued-token"),
            config_entry("namespace", "tenant-a/orders"),
            config_entry("tls_allow_insecure_connection", "false"),
            config_entry("tls_ca_file", "trust.pem"),
            config_entry("tls_cert_file", "client.pem"),
            config_entry("tls_hostname_verification_enabled", "true"),
            config_entry("tls_key_file", "client-key.pem"),
        ])
        .assured(
            "every declared Pulsar key with a valid value is accepted before mount resolution",
        );
}

#[rstest]
#[case("tls_allow_insecure_connection", "true")]
#[case("tls_ca_file", "trust.pem")]
#[case("tls_hostname_verification_enabled", "false")]
fn tls_keys_require_a_tls_address(#[case] key: &str, #[case] value: &str) {
    let refusal = client_schema()
        .validate(&[
            config_entry("addr", "pulsar://broker.example.com:6650"),
            config_entry(key, value),
        ])
        .expect_err("a plaintext Pulsar connection uses no TLS setting");

    assert!(
        matches!(
            refusal.current_context(),
            ClientConfigSchemaError::RequiresScheme {
                key: refused,
                address: "addr",
                scheme: "pulsar+ssl",
            } if *refused == key
        ),
        "{key} is refused for its plaintext address: {refusal:?}"
    );
}

#[test]
fn a_client_certificate_requires_a_tls_address() {
    let refusal = client_schema()
        .validate(&[
            config_entry("addr", "pulsar://broker.example.com:6650"),
            config_entry("tls_cert_file", "client.pem"),
            config_entry("tls_key_file", "client-key.pem"),
        ])
        .expect_err("a plaintext Pulsar connection presents no client certificate");

    assert!(
        matches!(
            refusal.current_context(),
            ClientConfigSchemaError::RequiresScheme {
                address: "addr",
                scheme: "pulsar+ssl",
                ..
            }
        ),
        "a client certificate is refused for its plaintext address: {refusal:?}"
    );
}

#[rstest]
#[case("tls_cert_file", "tls_key_file")]
#[case("tls_key_file", "tls_cert_file")]
fn half_a_client_certificate_is_refused(#[case] present: &str, #[case] missing: &str) {
    let refusal = client_schema()
        .validate(&[
            config_entry("addr", "pulsar+ssl://broker.example.com:6651"),
            config_entry(present, "identity.pem"),
        ])
        .expect_err("a client certificate and its key are a pair");

    assert!(
        matches!(
            refusal.current_context(),
            ClientConfigSchemaError::RequiresKey { key, required }
                if *key == present && *required == missing
        ),
        "{present} is refused without {missing}: {refusal:?}"
    );
}

#[test]
fn inspection_redacts_the_token() {
    let entries = [
        config_entry("addr", "pulsar://broker.example.com:6650"),
        config_entry("auth_token", "issued-token"),
    ];

    let inspected = client_schema().inspection_entries(&entries);

    assert_eq!(
        inspected[1],
        config_entry("auth_token", crate::CLIENT_CONFIG_REDACTION)
    );
}

#[test]
fn an_omitted_namespace_takes_the_declared_default() {
    let schema = client_schema();
    let authored = [config_entry("addr", "pulsar://broker.example.com:6650")];

    let effective = schema.effective_entries(&authored);
    assert_eq!(
        effective,
        [
            authored[0].clone(),
            config_entry("namespace", "public/default")
        ]
    );
    assert_eq!(schema.effective_entries(&effective), effective);
    schema
        .validate_rendered(&effective)
        .assured("the declared default namespace is a valid namespace");

    let written = [
        config_entry("addr", "pulsar://broker.example.com:6650"),
        config_entry("namespace", "tenant-a/orders"),
    ];
    assert_eq!(schema.effective_entries(&written), written);
}

#[rstest]
#[case("public/default")]
#[case("tenant-a/orders")]
#[case("Tenant_A/Orders.EU")]
#[case("tenant:a/orders=eu")]
fn a_namespace_names_a_tenant_and_a_namespace(#[case] namespace: &str) {
    client_schema()
        .validate(&[
            config_entry("addr", "pulsar://broker.example.com:6650"),
            config_entry("namespace", namespace),
        ])
        .assured("a namespace of two nonempty levels is accepted");

    let qualified = format!("persistent://{namespace}/orders");
    PulsarTopicName::try_from(qualified.as_str())
        .assured("a short topic placed in an accepted namespace is a qualified topic");
}

#[rstest]
#[case("")]
#[case("tenant-a")]
#[case("tenant-a/orders/eu")]
#[case("tenant-a/")]
#[case("/orders")]
#[case("tenant-a//orders")]
#[case("tenant a/orders")]
#[case("tenant-a/or\tders")]
#[case("tenant-a/orders\u{7}")]
fn a_namespace_that_cannot_qualify_a_topic_is_refused(#[case] namespace: &str) {
    let refusal = client_schema()
        .validate(&[
            config_entry("addr", "pulsar://broker.example.com:6650"),
            config_entry("namespace", namespace),
        ])
        .expect_err("only a tenant and a namespace qualify a short topic");

    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::InvalidValue {
            key: "namespace".to_string(),
            expected: "tenant/namespace",
        }
    );
}

#[rstest]
#[case("auth_params")]
#[case("tls_trust_certs_file_path")]
#[case("namespcae")]
fn client_schema_refuses_an_undeclared_key(#[case] key: &str) {
    let refusal = client_schema()
        .validate(&[
            config_entry("addr", "pulsar+ssl://broker.example.com:6651"),
            config_entry(key, "value"),
        ])
        .expect_err("a Pulsar client is strict: only declared keys configure it");

    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::UnknownKey {
            key: key.to_string()
        }
    );
}

#[test]
fn client_schema_requires_a_pulsar_address() {
    let missing = client_schema()
        .validate(&[config_entry("namespace", "public/default")])
        .expect_err("a Pulsar client cannot reach a broker without its address");
    assert_eq!(
        missing.current_context(),
        &ClientConfigSchemaError::MissingRequired { key: "addr" }
    );

    for addr in ["http://broker.example.com:6650", "broker.example.com:6650"] {
        let refusal = client_schema()
            .validate(&[config_entry("addr", addr)])
            .expect_err("a Pulsar address uses the pulsar or pulsar+ssl scheme");
        assert!(
            matches!(refusal.current_context(), ClientConfigSchemaError::InvalidValue { key, .. } if key == "addr"),
            "{addr} is not a Pulsar address: {refusal:?}"
        );
    }
}

#[test]
fn topic_and_subscription_names_fit_their_byte_bounds() {
    let topic_at_bound = "t".repeat(1024);
    PulsarTopicName::try_from(topic_at_bound.as_str()).assured("1,024 bytes fit the topic bound");
    let topic_above_bound = "t".repeat(1025);
    assert_eq!(
        PulsarTopicName::try_from(topic_above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 1024,
            actual: 1025,
        })
    );

    let subscription_at_bound = "s".repeat(255);
    PulsarSubscriptionName::try_from(subscription_at_bound.as_str())
        .assured("255 bytes fit the subscription bound");
    let subscription_above_bound = "s".repeat(256);
    assert_eq!(
        PulsarSubscriptionName::try_from(subscription_above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 255,
            actual: 256,
        })
    );
    assert_eq!(
        PulsarSubscriptionName::try_from("readers\u{7}"),
        Err(ExternalNameError::InvalidChar { ch: '\u{7}' })
    );
}
