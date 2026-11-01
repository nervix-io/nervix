//! RabbitMQ's executable declaration contract.
//!
//! Layer: test harness.
//! - **Owns.** Queue- and exchange-name protocol boundaries and the current client, ingest and emit
//!   contracts.
//! - **Depends on.** The RabbitMQ vocabulary and shared declaration types.
//! - **Must not know.** The AMQP driver, graph execution or language grammar.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "qualifies RabbitMQ vocabulary declarations and external names"
    )
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigPresence, ClientConfigSchemaError, ClientConfigValue,
    ClientPooling, EmitterEntityKind, EmitterPublishingModeKind, ExternalNameError,
    RuntimeCapability,
};

#[rstest]
#[case::mixed_case("Orders.Mixed-01".to_owned())]
#[case::quoted("Orders 'north' \\ \"east\"".to_owned())]
#[case::ascii_limit("Q".repeat(255))]
#[case::utf8_limit(format!("{}Z", "é".repeat(127)))]
fn queue_names_preserve_the_complete_external_spelling(#[case] text: String) {
    let queue = RabbitMqQueueName::parse(&text).assured("the case is a valid AMQP queue name");
    assert_eq!(queue.as_str(), text);
    assert_eq!(queue.to_string(), text);
}

#[rstest]
#[case::empty(String::new(), ExternalNameError::Empty)]
#[case::ascii_over_limit("Q".repeat(256), ExternalNameError::TooLong { max: 255, actual: 256 })]
#[case::utf8_over_limit("é".repeat(128), ExternalNameError::TooLong { max: 255, actual: 256 })]
fn queue_names_report_the_protocol_byte_boundary(
    #[case] text: String,
    #[case] expected: ExternalNameError,
) {
    let failure = RabbitMqQueueName::parse(&text).expect_err("the case exceeds the name contract");
    assert_eq!(failure.current_context(), &expected);
}

#[rstest]
#[case::mixed_case("Events.Mixed-01".to_owned())]
#[case::predeclared("amq.topic".to_owned())]
#[case::ascii_limit("X".repeat(255))]
#[case::utf8_limit(format!("{}Z", "é".repeat(127)))]
fn exchange_names_preserve_the_complete_external_spelling(#[case] text: String) {
    let exchange =
        RabbitMqExchangeName::parse(&text).assured("the case is a valid AMQP exchange name");
    assert_eq!(exchange.as_str(), text);
}

/// The default exchange's empty name is the one a QUEUE emitter publishes through, never one an
/// EXCHANGE emitter names.
#[rstest]
#[case::default_exchange(String::new(), ExternalNameError::Empty)]
#[case::ascii_over_limit("X".repeat(256), ExternalNameError::TooLong { max: 255, actual: 256 })]
#[case::utf8_over_limit("é".repeat(128), ExternalNameError::TooLong { max: 255, actual: 256 })]
fn exchange_names_report_the_protocol_byte_boundary(
    #[case] text: String,
    #[case] expected: ExternalNameError,
) {
    let failure =
        RabbitMqExchangeName::parse(&text).expect_err("the case exceeds the name contract");
    assert_eq!(failure.current_context(), &expected);
}

#[test]
fn client_configuration_keeps_uri_queries_at_the_driver_boundary() {
    let declaration = CreateClientRabbitMq::<u64>::DECLARATION;
    assert_eq!(
        CreateClientRabbitMq::<u64>::CONNECTOR,
        ConnectorKind::RabbitMq
    );
    assert_eq!(declaration.pooling, ClientPooling::Unpooled);
    assert_eq!(
        declaration.config.key("addr").map(|key| key.value),
        Some(ClientConfigValue::DriverAddress)
    );
    assert_eq!(declaration.options, &[]);
}

#[rstest]
#[case::trust(&["tls_ca_file"], "tls_ca_file")]
#[case::identity(&["tls_cert_file", "tls_key_file"], "tls_cert_file")]
#[case::trust_and_identity(&["tls_ca_file", "tls_cert_file", "tls_key_file"], "tls_ca_file")]
fn tls_files_require_amqps_at_each_configuration_boundary(
    #[case] files: &[&str],
    #[case] refused_key: &'static str,
    #[values(false, true)] rendered: bool,
) {
    let schema = CreateClientRabbitMq::<u64>::DECLARATION.config;
    let mut config = vec![ClientConfigEntry {
        key: "addr".to_owned(),
        value: "amqp://private-user:private-password@private-broker:5672/private-vhost".to_owned(),
    }];
    for key in files {
        config.push(ClientConfigEntry {
            key: (*key).to_owned(),
            value: format!("/private-files/{key}.pem"),
        });
    }
    let result = if rendered {
        schema.validate_rendered(&config)
    } else {
        schema.validate(&config)
    };
    let failure = result.expect_err("TLS files require an amqps address before file inspection");
    assert_eq!(
        failure.current_context(),
        &ClientConfigSchemaError::RequiresScheme {
            key: refused_key,
            address: "addr",
            scheme: "amqps",
        }
    );
    assert_eq!(
        format!("{failure:#}"),
        format!("client config key '{refused_key}' requires 'addr' to use amqps")
    );

    config[0].value =
        "amqps://private-user:private-password@private-broker:5671/private-vhost".to_owned();
    let result = if rendered {
        schema.validate_rendered(&config)
    } else {
        schema.validate(&config)
    };
    result.assured("the same complete TLS configuration is valid with amqps");
    for key in files {
        let declaration = schema.key(key).assured("RabbitMQ declares each TLS file");
        assert_eq!(
            declaration.presence,
            ClientConfigPresence::OptionalForScheme {
                address: "addr",
                scheme: "amqps",
            }
        );
    }
}

#[test]
fn queue_ingestion_declares_current_consumer_modes_and_transport_bounds() {
    let declaration = RabbitMqQueueIngestor::CAPABILITIES;
    assert_eq!(
        declaration.reference,
        IngestorReferenceKind::Client(ConnectorKind::RabbitMq)
    );
    assert_eq!(
        declaration.delivery,
        &[
            DeclaredDelivery::NO_ACK,
            DeclaredDelivery::ACK_SEQUENTIAL,
            DeclaredDelivery::ACK_PARALLEL,
        ]
    );
    assert_eq!(declaration.quiesce, &[DeclaredQuiesce::SUSPEND]);
    assert_eq!(declaration.position_class, PositionClass::BrokerRedelivered);
    assert_eq!(
        declaration.acknowledgement_confirmation,
        AcknowledgementConfirmation::Written
    );
    assert!(!declaration.executes_on_every_cluster_node);
    assert_eq!(declaration.options, &[OptionDeclaration::INSTANCES]);
    let mut metadata = Vec::new();
    for field in declaration.metadata.fields() {
        metadata.push((field.name(), field.ty()));
    }
    assert_eq!(
        metadata,
        [
            ("exchange", IngestorMetadataType::String),
            ("routing_key", IngestorMetadataType::String),
            ("delivery_tag", IngestorMetadataType::U64),
            ("redelivered", IngestorMetadataType::Bool),
            ("content_type", IngestorMetadataType::String),
            ("content_encoding", IngestorMetadataType::String),
            ("message_id", IngestorMetadataType::String),
            ("correlation_id", IngestorMetadataType::String),
            ("reply_to", IngestorMetadataType::String),
            ("type", IngestorMetadataType::String),
            ("app_id", IngestorMetadataType::String),
            ("user_id", IngestorMetadataType::String),
            ("priority", IngestorMetadataType::U8),
            ("expiration", IngestorMetadataType::String),
            ("timestamp", IngestorMetadataType::Datetime),
        ]
    );
    assert_eq!(
        declaration.headers,
        Some(HeaderModel::exact(HeaderLayout::SortedByName))
    );
    assert_eq!(
        declaration.locator,
        Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(RabbitMqQueueName::DECLARATION),
        })
    );
    assert_eq!(declaration.bounds.buffers.len(), 2);
    assert_eq!(declaration.bounds.buffers[0].bound, BufferBound::Unbounded);
    assert_eq!(
        declaration.bounds.buffers[1].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting(
                "1 under NO ACK and ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 65,535",
            ),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert_eq!(declaration.bounds.waits.len(), 3);
    assert_eq!(
        declaration.bounds.waits[0].bound,
        WaitBound::Clause("BATCH TIMEOUT")
    );
    assert_eq!(
        declaration.bounds.waits[1].bound,
        WaitBound::Key {
            key: DeadlineKey::ConnectTimeout,
            omitted: OmittedDeadline::Default(DEFAULT_CONNECT_TIMEOUT),
        }
    );
    assert_eq!(
        declaration.bounds.waits[2].bound,
        WaitBound::Driver("twice the negotiated heartbeat")
    );
    assert_eq!(
        declaration.bounds.deadline_keys(),
        vec![DeadlineKey::ConnectTimeout]
    );
}

#[test]
fn queue_emission_declares_codec_batches_and_broker_confirmation_modes() {
    let declaration = RabbitMqQueueEmitter::CAPABILITIES;
    assert_eq!(
        declaration.reference,
        EmitterReferenceKind::Client(ConnectorKind::RabbitMq)
    );
    assert_eq!(declaration.body, EmitterBodyForm::Codec);
    assert_eq!(
        declaration.batch,
        EmitterBatchLegality::Optional { maximum: None }
    );
    assert!(!declaration.requires_batch_transformation);
    assert_eq!(
        declaration.publishing_modes,
        &[
            EmitterPublishingModeKind::NoAck,
            EmitterPublishingModeKind::BrokerAck,
        ]
    );
    assert_eq!(
        declaration.headers,
        Some(HeaderModel::exact(HeaderLayout::SortedByName))
    );
    assert_eq!(
        declaration.locator,
        RabbitMqQueueIngestor::CAPABILITIES.locator
    );
    assert_eq!(declaration.options, &[]);
    assert_eq!(declaration.envelope, &[EnvelopeClauseKind::Mandatory]);
    assert_eq!(declaration.properties, &[]);
    assert_emitter_buffers(declaration.bounds);
    assert_eq!(
        declaration.bounds.deadline_keys(),
        vec![DeadlineKey::Timeout, DeadlineKey::ConnectTimeout]
    );
}

/// Both emitter entities hold the same two buffers: the confirmation window, and the messages the
/// broker returns under MANDATORY, which a write reads before it ends.
fn assert_emitter_buffers(bounds: DeclaredBounds) {
    assert_eq!(bounds.buffers.len(), 2);
    assert_eq!(
        bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert_eq!(
        bounds.buffers[1].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("the messages of one write"),
            overflow: BufferOverflow::Backpressure,
        }
    );
}

#[test]
fn exchange_emission_declares_its_locator_routing_key_and_the_queue_emitters_contract() {
    let declaration = RabbitMqExchangeEmitter::CAPABILITIES;
    let queue = RabbitMqQueueEmitter::CAPABILITIES;
    assert_eq!(declaration.reference, queue.reference);
    assert_eq!(declaration.body, EmitterBodyForm::Codec);
    assert_eq!(declaration.batch, queue.batch);
    assert!(!declaration.requires_batch_transformation);
    assert_eq!(declaration.publishing_modes, queue.publishing_modes);
    assert_eq!(declaration.headers, queue.headers);
    assert_eq!(
        declaration.locator,
        Some(LocatorDeclaration {
            keywords: "EXCHANGE",
            name: LocatorName::External(RabbitMqExchangeName::DECLARATION),
        })
    );
    let [routing_key] = declaration.options else {
        panic!("an exchange emitter declares one option");
    };
    assert_eq!(routing_key.keywords, "ROUTING KEY");
    assert_eq!(routing_key.presence, OptionPresence::Required);
    assert_eq!(
        routing_key.form,
        OptionForm::Value {
            value: OptionValue::Expression,
            example: "ROUTING KEY input.category",
        }
    );
    assert_eq!(declaration.envelope, &[EnvelopeClauseKind::Mandatory]);
    assert_eq!(declaration.properties, &[]);
    assert_emitter_buffers(declaration.bounds);
    assert_eq!(declaration.bounds.waits.len(), queue.bounds.waits.len());
    assert!(
        declaration.bounds.waits[0]
            .wait
            .ends_with("the exchange check"),
        "{}",
        declaration.bounds.waits[0].wait
    );
    assert_eq!(
        declaration.bounds.deadline_keys(),
        vec![DeadlineKey::Timeout, DeadlineKey::ConnectTimeout]
    );
    assert_eq!(
        declaration.runtime_capabilities(),
        vec![
            RuntimeCapability::Publishing(EmitterPublishingModeKind::NoAck),
            RuntimeCapability::Publishing(EmitterPublishingModeKind::BrokerAck),
            RuntimeCapability::Option {
                keywords: "ROUTING KEY"
            },
            RuntimeCapability::Batch,
            RuntimeCapability::Envelope(EnvelopeClauseKind::Mandatory),
            RuntimeCapability::StalledPeer(DeadlineKey::Timeout),
            RuntimeCapability::StalledPeer(DeadlineKey::ConnectTimeout),
        ]
    );
}

#[test]
fn only_an_exchange_emitter_computes_a_routing_key() {
    let routing_key = crate::Expression::Literal(crate::Literal::String("orders".to_string()));
    let exchange = crate::EmitterEntity::from(RabbitMqExchangeEmitter {
        mode: RabbitMqExchangePublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "1s".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: ClientName::parse("broker").assured("broker is a client name"),
        exchange: RabbitMqExchangeName::parse("events").assured("events is an exchange name"),
        routing_key: routing_key.clone(),
    });
    assert_eq!(exchange.routing_key(), Some(&routing_key));
    assert_eq!(
        exchange.kind(),
        EmitterEntityKind::RabbitMq(RabbitMqEmitterEntity::Exchange)
    );

    let queue = crate::EmitterEntity::from(RabbitMqQueueEmitter {
        mode: RabbitMqQueuePublishingMode::NoAck {
            retry_policy: RetryPolicy {
                backoff: "1s".to_string(),
                max_backoff: "30s".to_string(),
            },
        },
        client: ClientName::parse("broker").assured("broker is a client name"),
        queue: RabbitMqQueueName::parse("orders").assured("orders is a queue name"),
    });
    assert_eq!(queue.routing_key(), None);
}
