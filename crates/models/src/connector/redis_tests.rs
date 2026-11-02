//! Tests of the Redis vocabulary's executable Pub/Sub contract.
//!
//! Layer: test harness.
//! - **Owns.** Redis declaration and channel-name contract checks through the public vocabulary.
//! - **Depends on.** Redis Models, entity kinds and shared declaration types.
//! - **Must not know.** Redis drivers, parsing, planning or runtime subscriptions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigKey, ClientConfigOwner, ClientConfigPresence,
    ClientConfigReaders, ClientConfigSchemaError, ClientConfigValue, ClientPooling,
    CreateClientRedis, EmitterEntityKind, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityKind,
};

#[test]
fn pubsub_declarations_match_the_unacknowledged_transport() {
    let ingest = IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).capabilities();
    let emit = EmitterEntityKind::Redis(RedisEmitterEntity::PubSub).capabilities();

    assert_eq!(ingest.delivery, &[DeclaredDelivery::NO_ACK]);
    assert_eq!(ingest.position_class, PositionClass::None);
    assert!(!ingest.executes_on_every_cluster_node);
    assert_eq!(
        ingest.acknowledgement_confirmation,
        AcknowledgementConfirmation::None
    );
    assert_eq!(
        ingest.declared_delivery(IngestDeliveryKind::NoAck),
        Some(&DeclaredDelivery::NO_ACK)
    );
    assert_eq!(
        ingest.quiesce,
        &[DeclaredQuiesce::BUFFER, DeclaredQuiesce::DROP]
    );
    assert_eq!(
        ingest.declared_quiesce(IngestQuiesceKind::Buffer),
        Some(&DeclaredQuiesce::BUFFER)
    );
    assert_eq!(
        emit.publishing_modes,
        &[crate::EmitterPublishingModeKind::NoAck]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert_eq!(emit.batch, EmitterBatchLegality::Optional { maximum: None });
    assert!(!emit.requires_batch_transformation);
}

#[test]
fn both_pubsub_directions_share_the_channel_and_client_contract() {
    let ingest = IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).capabilities();
    let emit = EmitterEntityKind::Redis(RedisEmitterEntity::PubSub).capabilities();
    let channel = Some(LocatorDeclaration {
        keywords: "CHANNEL",
        name: LocatorName::External(RedisChannelName::DECLARATION),
    });

    assert_eq!(ingest.locator, channel);
    assert_eq!(emit.locator, channel);
    assert_eq!(
        IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).locators(),
        [
            LocatorDeclaration {
                keywords: "CHANNEL",
                name: LocatorName::External(RedisChannelName::DECLARATION),
            },
            LocatorDeclaration {
                keywords: "PATTERN",
                name: LocatorName::External(RedisChannelPattern::DECLARATION),
            },
        ]
    );
    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Redis)
    );
    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Redis)
    );
    assert_eq!(
        CreateClientRedis::<u64>::DECLARATION.pooling,
        ClientPooling::Pooled
    );
    let config = CreateClientRedis::<u64>::DECLARATION.config;
    assert_eq!(
        config
            .key("addr")
            .map(|key| (key.presence, key.value, key.owner)),
        Some((
            ClientConfigPresence::Required,
            ClientConfigValue::DriverAddress,
            ClientConfigOwner::Nervix
        ))
    );
    assert_eq!(
        config.ownership_label(),
        "Nervix; address query delegated to driver"
    );
    assert!(config.namespaces().is_empty());
    assert!(CreateClientRedis::<u64>::DECLARATION.options.is_empty());
}

#[test]
fn pubsub_declares_payloads_without_headers_or_an_envelope() {
    let ingest = IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).capabilities();
    let emit = EmitterEntityKind::Redis(RedisEmitterEntity::PubSub).capabilities();

    assert_eq!(ingest.headers, None);
    assert_eq!(emit.headers, None);
    assert_eq!(
        ingest.metadata.fields(),
        [
            IngestorMetadataField::new("channel", IngestorMetadataType::String),
            IngestorMetadataField::new("pattern", IngestorMetadataType::String),
        ]
    );
    assert!(ingest.options.is_empty());
    assert!(emit.options.is_empty());
    assert!(emit.envelope.is_empty());
    assert!(emit.properties.is_empty());
}

#[test]
fn pubsub_declares_its_read_ahead_bound_and_each_connection_deadline() {
    let ingest = IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).capabilities();
    let emit = EmitterEntityKind::Redis(RedisEmitterEntity::PubSub).capabilities();

    assert_eq!(ingest.bounds.buffers.len(), 1);
    assert_eq!(
        ingest.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("subscription_buffer_messages"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert_eq!(
        ingest.bounds.deadline_keys(),
        vec![DeadlineKey::ConnectTimeout, DeadlineKey::IdleTimeout]
    );
    assert_eq!(
        ingest.bounds.waits[0].bound,
        WaitBound::Key {
            key: DeadlineKey::ConnectTimeout,
            omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
        }
    );
    assert_eq!(
        ingest.bounds.waits[1].bound,
        WaitBound::Key {
            key: DeadlineKey::IdleTimeout,
            omitted: OmittedDeadline::Required,
        }
    );
    assert_eq!(
        emit.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert_eq!(
        emit.bounds.deadline_keys(),
        vec![DeadlineKey::ConnectTimeout]
    );
    assert_eq!(
        emit.bounds.waits[0].bound,
        WaitBound::Key {
            key: DeadlineKey::ConnectTimeout,
            omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
        }
    );
    assert_eq!(emit.bounds.waits[1].bound, WaitBound::Driver("500 ms"));
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn client_schema_keeps_subscription_options_optional_and_typed() {
    let schema = CreateClientRedis::<u64>::DECLARATION.config;
    let address = config_entry("addr", "redis://localhost:6379/2?protocol=resp3");
    schema
        .validate(std::slice::from_ref(&address))
        .assured("an emitter client needs no subscription-only settings");
    schema
        .validate(&[
            address,
            config_entry("connect_timeout_ms", "1000"),
            config_entry("idle_timeout_ms", "30000"),
            config_entry("subscription_buffer_messages", "16"),
        ])
        .assured("the shared schema admits the current Redis connection and intake settings");
}

#[test]
fn client_schema_declares_the_command_pool_lifetimes_with_defaults() {
    let schema = CreateClientRedis::<u64>::DECLARATION.config;
    for (name, default) in [
        ("pool_idle_timeout_ms", "600000"),
        ("pool_max_lifetime_ms", "1800000"),
    ] {
        let declared = ClientConfigKey {
            name,
            presence: ClientConfigPresence::Defaulted(default),
            value: ClientConfigValue::Milliseconds,
            owner: ClientConfigOwner::Nervix,
            confidential: false,
            readers: ClientConfigReaders::Every,
        };
        assert_eq!(schema.key(name), Some(&declared));
    }

    let address = config_entry("addr", "redis://localhost:6379");
    assert_eq!(
        schema.effective_entries(std::slice::from_ref(&address)),
        vec![
            address.clone(),
            config_entry("pool_idle_timeout_ms", "600000"),
            config_entry("pool_max_lifetime_ms", "1800000"),
        ]
    );
    let authored = vec![address, config_entry("pool_max_lifetime_ms", "5000")];
    let mut effective = authored.clone();
    effective.push(config_entry("pool_idle_timeout_ms", "600000"));
    assert_eq!(schema.effective_entries(&authored), effective);
    schema
        .validate(&effective)
        .assured("declared lifetimes are positive, schedulable milliseconds");
}

#[rstest]
#[case("connect_timeout_ms", "0")]
#[case("idle_timeout_ms", "not-a-duration")]
#[case("pool_idle_timeout_ms", "0")]
#[case("pool_idle_timeout_ms", "1.5s")]
#[case("pool_max_lifetime_ms", "-1")]
#[case("pool_max_lifetime_ms", "18446744073709551616")]
#[case("subscription_buffer_messages", "0")]
fn client_schema_refuses_invalid_nervix_options(#[case] key: &str, #[case] value: &str) {
    let refusal = CreateClientRedis::<u64>::DECLARATION
        .config
        .validate(&[
            config_entry("addr", "redis://localhost:6379"),
            config_entry(key, value),
        ])
        .expect_err("Nervix-owned options must satisfy their declared value contract");
    assert!(
        matches!(refusal.current_context(), ClientConfigSchemaError::InvalidValue { key: refused, .. } if refused == key)
    );
}

#[test]
fn client_schema_requires_both_halves_of_a_mounted_tls_identity() {
    let schema = CreateClientRedis::<u64>::DECLARATION.config;
    let address = config_entry("addr", "rediss://localhost:6379");
    schema
        .validate(&[
            address.clone(),
            config_entry("tls_ca_file", "trust.pem"),
            config_entry("tls_cert_file", "identity.pem"),
            config_entry("tls_key_file", "identity.key"),
        ])
        .assured("the complete TLS file declaration is accepted before mount resolution");
    for (present, required) in [
        ("tls_cert_file", "tls_key_file"),
        ("tls_key_file", "tls_cert_file"),
    ] {
        let refusal = schema
            .validate(&[address.clone(), config_entry(present, "identity.pem")])
            .expect_err("one half of a TLS client identity cannot configure Redis");
        assert!(
            matches!(refusal.current_context(), ClientConfigSchemaError::RequiresKey { key, required: missing } if *key == present && *missing == required)
        );
    }
}

#[rstest]
#[case("Orders:EU/*")]
#[case(" Channel with spaces ")]
#[case("通道:Événements")]
fn channel_names_preserve_every_literal_byte(#[case] raw: &str) {
    let channel = RedisChannelName::try_from(raw)
        .assured("each fixture is nonempty and fits the declared channel byte bound");
    assert_eq!(channel.as_str(), raw);
    assert_eq!(channel.to_string(), raw);
}

#[test]
fn channel_length_is_bounded_in_utf8_bytes() {
    let at_bound = "é".repeat(512);
    let channel = RedisChannelName::try_from(at_bound.as_str())
        .assured("512 two-byte characters fit the declared 1024-byte bound");
    assert_eq!(channel.as_str(), at_bound);

    let above_bound = format!("{at_bound}a");
    assert_eq!(
        RedisChannelName::try_from(above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 1024,
            actual: 1025,
        })
    );
    assert_eq!(
        RedisChannelName::try_from(""),
        Err(ExternalNameError::Empty)
    );
    assert_eq!(
        RedisChannelName::try_from("a")
            .assured("one byte is a nonempty channel within the byte bound")
            .as_str(),
        "a"
    );
}

#[rstest]
#[case("orders.*")]
#[case("Orders:[EU]?/[^x]*")]
#[case("literal\\*star")]
#[case(" pattern with spaces ")]
fn channel_patterns_preserve_every_literal_byte(#[case] raw: &str) {
    let pattern = RedisChannelPattern::try_from(raw)
        .assured("each fixture is nonempty and fits the declared pattern byte bound");
    assert_eq!(pattern.as_str(), raw);
    assert_eq!(pattern.to_string(), raw);
}

#[test]
fn channel_pattern_length_is_bounded_in_utf8_bytes() {
    let at_bound = "é".repeat(512);
    let pattern = RedisChannelPattern::try_from(at_bound.as_str())
        .assured("512 two-byte characters fit the declared 1024-byte bound");
    assert_eq!(pattern.as_str(), at_bound);
    assert_eq!(
        RedisChannelPattern::try_from(format!("{at_bound}*").as_str()),
        Err(ExternalNameError::TooLong {
            max: 1024,
            actual: 1025,
        })
    );
    assert_eq!(
        RedisChannelPattern::try_from(""),
        Err(ExternalNameError::Empty)
    );
}

#[test]
fn a_subscription_names_the_locator_it_is_written_with() {
    let channel = RedisSubscriptionTarget::Channel(
        RedisChannelName::try_from("orders").assured("a plain channel is within the bound"),
    );
    let pattern = RedisSubscriptionTarget::Pattern(
        RedisChannelPattern::try_from("orders.*").assured("a plain pattern is within the bound"),
    );
    assert_eq!(channel.locator().keywords, "CHANNEL");
    assert_eq!(pattern.locator().keywords, "PATTERN");
    assert_eq!(channel.as_str(), "orders");
    assert_eq!(pattern.as_str(), "orders.*");
    assert_eq!(channel.to_string(), "channel 'orders'");
    assert_eq!(pattern.to_string(), "pattern 'orders.*'");
    let locators = IngestorEntityKind::Redis(RedisIngestorEntity::PubSub).locators();
    assert!(locators.contains(&channel.locator()));
    assert!(locators.contains(&pattern.locator()));
}
