//! RabbitMQ's executable declaration contract.
//!
//! Layer: test harness.
//! - **Owns.** Queue-name protocol boundaries and the current client, ingest and emit contracts.
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
    ClientPooling, EmitterPublishingModeKind, ExternalNameError,
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
    assert_eq!(declaration.metadata.fields(), vec![]);
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
    assert_eq!(declaration.envelope, &[]);
    assert_eq!(declaration.properties, &[]);
    assert_eq!(declaration.bounds.buffers.len(), 2);
    assert_eq!(
        declaration.bounds.buffers[0].bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting("ACK PARALLEL MAX, 1 under ACK SEQUENTIAL"),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert_eq!(declaration.bounds.buffers[1].bound, BufferBound::Unbounded);
    assert_eq!(
        declaration.bounds.deadline_keys(),
        vec![DeadlineKey::Timeout, DeadlineKey::ConnectTimeout]
    );
}
