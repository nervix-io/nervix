//! Tests of the SQS vocabulary's executable queue contract.
//!
//! Layer: test harness.
//! - **Owns.** SQS declaration, client configuration and queue-name contract checks through the
//!   public vocabulary.
//! - **Depends on.** SQS Models, entity kinds and shared declaration types.
//! - **Must not know.** The SQS SDK, parsing, planning or how a queue is received from or sent to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use rstest::rstest;

use super::*;
use crate::{
    ClientConfigEntry, ClientConfigFile, ClientConfigOwner, ClientConfigPresence,
    ClientConfigSchemaError, ClientConfigValue, ClientPooling, CreateClientSqs, EmitterEntityKind,
    EmitterPublishingModeKind, HeaderNames, IngestDeliveryKind, IngestQuiesceKind,
    IngestorEntityKind,
};

#[test]
fn queue_declarations_match_the_acknowledged_transport() {
    let ingest = IngestorEntityKind::Sqs(SqsIngestorEntity::Queue).capabilities();
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();

    assert_eq!(
        ingest.delivery,
        &[DeclaredDelivery::ACK_SEQUENTIAL, SQS_ACK_PARALLEL]
    );
    assert!(
        SQS_ACK_PARALLEL
            .example
            .starts_with(&format!("ACK PARALLEL MAX {} ", SqsReceiveMax::MAX)),
        "the declared window example is the largest one receive returns"
    );
    for undeclared in [IngestDeliveryKind::NoAck, IngestDeliveryKind::AckRequest] {
        assert_eq!(ingest.declared_delivery(undeclared), None);
    }
    assert_eq!(ingest.position_class, PositionClass::BrokerRedelivered);
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
    // Every request is answered, so the emitter publishes in the shared request/response mode,
    // and its request shape is the SEND option.
    assert_eq!(
        emit.publishing_modes,
        &[EmitterPublishingModeKind::RequestAck]
    );
    assert_eq!(emit.body, EmitterBodyForm::Codec);
    assert!(!emit.requires_batch_transformation);
}

#[test]
fn both_queue_directions_share_the_queue_rule_and_the_client() {
    let ingest = IngestorEntityKind::Sqs(SqsIngestorEntity::Queue).capabilities();
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let queue = Some(LocatorDeclaration {
        keywords: "QUEUE",
        name: LocatorName::External(SqsQueueName::DECLARATION),
    });

    assert_eq!(ingest.locator, queue);
    assert_eq!(emit.locator, queue);
    assert_eq!(
        ingest.reference,
        IngestorReferenceKind::Client(ConnectorKind::Sqs)
    );
    assert_eq!(
        emit.reference,
        EmitterReferenceKind::Client(ConnectorKind::Sqs)
    );
    assert_eq!(
        CreateClientSqs::<u64>::DECLARATION.pooling,
        ClientPooling::Unpooled
    );
    assert!(CreateClientSqs::<u64>::DECLARATION.options.is_empty());
}

#[test]
fn queue_declares_message_attributes_as_headers_with_a_delay_and_a_deduplication_id() {
    let ingest = IngestorEntityKind::Sqs(SqsIngestorEntity::Queue).capabilities();
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();

    assert_eq!(
        ingest.headers,
        Some(HeaderModel::exact(HeaderLayout::LastWins))
    );
    assert_eq!(ingest.metadata, IngestorMetadataDeclaration::NONE);
    assert!(ingest.metadata.fields().is_empty());
    assert_eq!(ingest.options, &[OptionDeclaration::INSTANCES]);
    assert_eq!(
        emit.envelope,
        &[
            EnvelopeClauseKind::Delay,
            EnvelopeClauseKind::DeduplicationId
        ]
    );
    assert!(emit.properties.is_empty());
}

#[test]
fn emitted_attributes_carry_strings_integers_and_bytes_as_their_sqs_data_types() {
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let Some(attributes) = emit.headers else {
        panic!("an SQS queue emitter writes message attributes");
    };

    assert_eq!(attributes.layout, HeaderLayout::LastWins);
    assert_eq!(attributes.names, HeaderNames::Exact);
    assert_eq!(
        attributes.values.carried().as_deref(),
        Some("STRING as String; U8, I8, U16, I16, U32, I32, U64, I64 as Number; BYTES as Binary")
    );
    // A boolean and a datetime have no SQS data type that carries them exactly.
    let declared = attributes.values.types();
    for refused in [PropertyType::Bool, PropertyType::Datetime] {
        assert!(!declared.contains(&refused), "{refused}");
    }
}

#[test]
fn fifo_group_is_an_optional_expression_or_the_branch() {
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let [fifo_group, _] = emit.options else {
        panic!("an SQS queue emitter declares its FIFO GROUP, then its SEND");
    };

    assert_eq!(fifo_group.keywords, "FIFO GROUP");
    assert_eq!(
        fifo_group.presence,
        OptionPresence::Optional { default: None }
    );
    let OptionForm::Choice([expression, branch]) = fifo_group.form else {
        panic!("FIFO GROUP is a choice between an expression and the record's branch");
    };
    assert_eq!(expression.value, Some(OptionValue::Expression));
    assert_eq!(branch.name, "FROM BRANCH");
    assert_eq!(branch.value, None);
}

#[test]
fn send_is_a_required_choice_between_one_request_per_message_and_batch_requests() {
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let [_, send] = emit.options else {
        panic!("an SQS queue emitter declares its FIFO GROUP, then its SEND");
    };

    assert_eq!(send.keywords, "SEND");
    assert!(send.is_required());
    let OptionForm::Choice([single, batch]) = send.form else {
        panic!("SEND is a choice between SINGLE and BATCH");
    };
    assert_eq!((single.name, single.value), ("SINGLE", None));
    assert_eq!((batch.name, batch.value), ("BATCH", None));
    assert_eq!(SqsSend::Single.keyword(), single.name);
    assert_eq!(SqsSend::Batch.keyword(), batch.name);
}

#[test]
fn a_delay_is_at_most_the_fifteen_minutes_sqs_gives_a_message() {
    assert_eq!(SQS_DELAY_MAXIMUM.as_secs(), 15 * 60);
}

#[test]
fn batching_is_optional_up_to_the_largest_message_sqs_accepts() {
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let EmitterBatchLegality::Optional {
        maximum: Some(maximum),
    } = emit.batch
    else {
        panic!("an SQS queue emitter batches optionally within the SQS message limit");
    };

    assert_eq!(maximum, SQS_MESSAGE_SIZE_MAXIMUM);
    assert_eq!(maximum.bytes().get(), 1024 * 1024);
}

#[test]
fn every_request_of_both_directions_is_bounded_by_timeout_ms() {
    let ingest = IngestorEntityKind::Sqs(SqsIngestorEntity::Queue).capabilities();
    let emit = EmitterEntityKind::Sqs(SqsEmitterEntity::Queue).capabilities();
    let timeout = WaitBound::Key {
        key: DeadlineKey::Timeout,
        omitted: OmittedDeadline::Unbounded,
    };

    let [gathered] = ingest.bounds.buffers else {
        panic!("an SQS queue ingestor holds only the messages it gathers for one host read");
    };
    assert_eq!(
        gathered.bound,
        BufferBound::Bounded {
            capacity: BufferCapacity::Setting(
                "1 under ACK SEQUENTIAL; ACK PARALLEL MAX otherwise, at most 10",
            ),
            overflow: BufferOverflow::Backpressure,
        }
    );
    assert!(emit.bounds.buffers.is_empty());
    assert_eq!(ingest.bounds.deadline_keys(), vec![DeadlineKey::Timeout]);
    let ingest_waits: Vec<WaitBound> = ingest.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(
        ingest_waits,
        vec![
            WaitBound::Clause("BATCH TIMEOUT"),
            WaitBound::Driver("3.1 s"),
            timeout,
            timeout
        ]
    );
    assert_eq!(emit.bounds.deadline_keys(), vec![DeadlineKey::Timeout]);
    let emit_waits: Vec<WaitBound> = emit.bounds.waits.iter().map(|wait| wait.bound).collect();
    assert_eq!(emit_waits, vec![WaitBound::Driver("3.1 s"), timeout]);
}

#[rstest]
#[case(1)]
#[case(7)]
#[case(10)]
fn receive_windows_from_one_to_ten_hold_their_count(#[case] count: u64) {
    let window = SqsReceiveMax::try_from(count).expect("the window is within one receive");

    assert_eq!(window.get().get(), count);
    assert_eq!(u64::from(window), count);
    assert_eq!(window.to_string(), count.to_string());
}

#[rstest]
#[case(0)]
#[case(11)]
#[case(256)]
#[case(u64::MAX)]
fn receive_windows_outside_one_to_ten_are_refused(#[case] count: u64) {
    assert_eq!(
        SqsReceiveMax::try_from(count),
        Err(SqsReceiveMaxError::OutOfRange { value: count })
    );
}

#[test]
fn the_largest_window_takes_what_one_receive_returns() {
    assert_eq!(
        u64::from(SqsReceiveMax::LARGEST),
        u64::from(SqsReceiveMax::MAX)
    );
    assert_eq!(
        SqsReceiveMaxError::OutOfRange { value: 11 }.to_string(),
        "SQS ACK PARALLEL MAX must be between 1 and 10, found 11"
    );
}

#[test]
fn each_delivery_mode_states_its_acknowledgement() {
    let retry_policy = RetryPolicy {
        backoff: "1s".to_string(),
        max_backoff: "30s".to_string(),
    };
    let sequential = SqsIngestMode::AckSequential {
        timeout: "5s".to_string(),
        retry_policy: retry_policy.clone(),
    };
    let parallel = SqsIngestMode::AckParallel {
        max: SqsReceiveMax::LARGEST,
        batch_timeout: "100ms".to_string(),
        timeout: "5s".to_string(),
        retry_policy: retry_policy.clone(),
    };

    assert_eq!(
        sequential.acknowledgement(),
        IngestAcknowledgement::Sequential {
            timeout: "5s",
            retry: &retry_policy,
        }
    );
    assert_eq!(
        parallel.acknowledgement(),
        IngestAcknowledgement::Parallel {
            max: NonZeroU64::new(10).expect("ten is positive"),
            batch_timeout: "100ms",
            timeout: "5s",
            retry: &retry_policy,
        }
    );
}

fn config_entry(key: &str, value: &str) -> ClientConfigEntry {
    ClientConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn client_schema_declares_exactly_the_keys_both_directions_read() {
    let config = CreateClientSqs::<u64>::DECLARATION.config;
    let declared: Vec<&str> = config.keys().iter().map(|key| key.name).collect();

    assert_eq!(
        declared,
        vec![
            "access_key_id",
            "endpoint",
            "region",
            "secret_access_key",
            "session_token",
            "timeout_ms",
            "tls_ca_file"
        ]
    );
    let endpoint = config
        .key("endpoint")
        .assured("the declared key list above names the endpoint");
    assert_eq!(endpoint.presence, ClientConfigPresence::Required);
    assert_eq!(endpoint.value, ClientConfigValue::Url(&["http", "https"]));
    assert_eq!(endpoint.owner, ClientConfigOwner::Nervix);
    let region = config
        .key("region")
        .assured("the declared key list above names the region");
    assert_eq!(region.presence, ClientConfigPresence::Required);
    assert!(!region.confidential);
    for credential in ["access_key_id", "secret_access_key"] {
        let key = config
            .key(credential)
            .assured("the declared key list above names both credentials");
        assert!(key.confidential, "{credential} is redacted by inspection");
        assert_eq!(key.presence, ClientConfigPresence::Required);
    }
    let session_token = config
        .key("session_token")
        .assured("the declared key list above names the session token");
    assert!(session_token.confidential);
    assert_eq!(session_token.presence, ClientConfigPresence::Optional);
    assert_eq!(session_token.value, ClientConfigValue::String);
    let trust = config
        .key("tls_ca_file")
        .assured("the declared key list above names the trusted CA file");
    assert_eq!(
        trust.value,
        ClientConfigValue::File(ClientConfigFile::CertificateAuthority)
    );
    assert_eq!(config.ownership_label(), "Nervix");
    assert!(config.namespaces().is_empty());
    assert!(config.reserved().is_empty());
}

#[test]
fn client_schema_accepts_every_declared_key() {
    CreateClientSqs::<u64>::DECLARATION
        .config
        .validate(&[
            config_entry("endpoint", "https://sqs.eu-west-1.amazonaws.com"),
            config_entry("region", "eu-west-1"),
            config_entry("access_key_id", "AKIDEXAMPLE"),
            config_entry("secret_access_key", "declared-secret"),
            config_entry("session_token", "declared-session-token"),
            config_entry("timeout_ms", "5000"),
            config_entry("tls_ca_file", "trust.pem"),
        ])
        .assured("every declared SQS key with a valid value is accepted before mount resolution");
}

/// The keys every SQS client names: the endpoint it reaches and what each request is signed with.
fn signing_entries() -> Vec<ClientConfigEntry> {
    vec![
        config_entry("endpoint", "http://127.0.0.1:9324"),
        config_entry("region", "eu-west-1"),
        config_entry("access_key_id", "AKIDEXAMPLE"),
        config_entry("secret_access_key", "declared-secret"),
    ]
}

#[rstest]
#[case("endpoint")]
#[case("region")]
#[case("access_key_id")]
#[case("secret_access_key")]
fn client_schema_requires_the_endpoint_and_what_every_request_is_signed_with(
    #[case] key: &'static str,
) {
    let mut entries = signing_entries();
    entries.retain(|entry| entry.key != key);

    let refusal = CreateClientSqs::<u64>::DECLARATION
        .config
        .validate(&entries)
        .expect_err("an SQS client reaches the service only with its endpoint, region and keys");

    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::MissingRequired { key }
    );
}

#[rstest]
#[case("endpoint", "ftp://127.0.0.1:9324")]
#[case("endpoint", "sqs.eu-west-1.amazonaws.com")]
#[case("timeout_ms", "0")]
#[case("timeout_ms", "soon")]
fn client_schema_refuses_values_outside_their_declared_type(
    #[case] key: &str,
    #[case] value: &str,
) {
    let mut entries = signing_entries();
    entries.retain(|entry| entry.key != key);
    entries.push(config_entry(key, value));

    let refusal = CreateClientSqs::<u64>::DECLARATION
        .config
        .validate(&entries)
        .expect_err("a declared key holds only values of its declared type");
    assert!(
        matches!(refusal.current_context(), ClientConfigSchemaError::InvalidValue { key: refused, .. } if refused == key)
    );
}

#[test]
fn client_schema_refuses_an_undeclared_key() {
    let refusal = CreateClientSqs::<u64>::DECLARATION
        .config
        .validate(&[signing_entries(), vec![config_entry("regoin", "eu-west-1")]].concat())
        .expect_err("an SQS client is strict: only declared keys configure it");

    assert_eq!(
        refusal.current_context(),
        &ClientConfigSchemaError::UnknownKey {
            key: "regoin".to_string()
        }
    );
}

#[rstest]
#[case("orders")]
#[case("Orders-EU_1")]
#[case("Orders-EU_1.fifo")]
#[case("a")]
fn queue_names_preserve_every_literal_character(#[case] raw: &str) {
    let queue = SqsQueueName::try_from(raw).assured("each fixture is a valid SQS queue name");

    assert_eq!(queue.as_str(), raw);
    assert_eq!(queue.to_string(), raw);
}

#[test]
fn the_fifo_suffix_counts_against_the_80_character_bound() {
    let fifo_at_bound = format!("{}.fifo", "q".repeat(75));
    let fifo = SqsQueueName::try_from(fifo_at_bound.as_str())
        .assured("75 characters and the suffix fit the 80-character bound");
    assert!(fifo.is_fifo());
    let standard_at_bound = "q".repeat(80);
    let standard = SqsQueueName::try_from(standard_at_bound.as_str())
        .assured("80 characters fit the 80-character bound");
    assert!(!standard.is_fifo());

    let fifo_above_bound = format!("{}.fifo", "q".repeat(76));
    assert_eq!(
        SqsQueueName::try_from(fifo_above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 80,
            actual: 81,
        })
    );
    let standard_above_bound = "q".repeat(81);
    assert_eq!(
        SqsQueueName::try_from(standard_above_bound.as_str()),
        Err(ExternalNameError::TooLong {
            max: 80,
            actual: 81,
        })
    );
}

#[rstest]
#[case("", ExternalNameError::Empty)]
#[case(".fifo", ExternalNameError::EmptyBeforeSuffix { suffix: ".fifo" })]
#[case("orders.v2", ExternalNameError::InvalidChar { ch: '.' })]
#[case("orders.FIFO", ExternalNameError::InvalidChar { ch: '.' })]
#[case("Orders EU", ExternalNameError::InvalidChar { ch: ' ' })]
#[case("orders/eu", ExternalNameError::InvalidChar { ch: '/' })]
#[case("commandes-été", ExternalNameError::InvalidChar { ch: 'é' })]
fn queue_names_outside_the_sqs_rule_are_refused(
    #[case] raw: &str,
    #[case] refusal: ExternalNameError,
) {
    assert_eq!(SqsQueueName::try_from(raw), Err(refusal));
}

#[test]
fn only_a_name_ending_in_the_fifo_suffix_names_a_fifo_queue() {
    let fifo = SqsQueueName::try_from("orders.fifo").assured("a FIFO queue name is valid");
    let standard = SqsQueueName::try_from("orders-fifo").assured("a standard queue name is valid");

    assert!(fifo.is_fifo());
    assert!(!standard.is_fifo());
}
