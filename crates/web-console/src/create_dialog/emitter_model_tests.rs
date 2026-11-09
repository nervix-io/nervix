//! Complete emitter draft and canonical representation checks.
//!
//! Layer: test harness, outside product layer order.
//! - **Owns.** Independent semantic-to-form fixtures and equality oracles for all emitter entities.
//! - **Depends on.** Current vocabulary generators, visual drafts and canonical Plexus parsing.
//! - **Must not know.** External drivers or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns complete emitter draft representation tests")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::*;
use nervix_plexus::client_statement::{ClientStatement, parse_client_statement};

use super::{
    SelectedReference,
    emitter_destination_draft::{ConflictDraft, EmitterDestinationDraft, MetricChoice, ScopeDraft},
    emitter_draft::{BodyChoice, EmitterDraft, EmitterDraftError},
    emitter_envelope_draft::{EnvelopeValueDraft, ValueDraft},
    emitter_publication_draft::PublicationDraft,
    ingestor_route_draft::{
        AssignmentDraft, FlushDraft, InheritDraft, InheritedFieldDraft, InvocationDraft,
        MessageErrorDraft,
    },
    processor_draft::{StateDependencyDraft, StatePolicyDraft},
    processor_input_draft::InputDraft,
};

fn expression(value: &Expression) -> String {
    expression_to_plexus(value).assured("a current generated expression has canonical Plexus")
}

fn assignments(values: &[Assignment]) -> Vec<AssignmentDraft> {
    values
        .iter()
        .map(|value| AssignmentDraft {
            scope: value.target.scope,
            field: Some(SelectedReference::chosen(value.target.field.clone())),
            expression: expression(&value.value),
        })
        .collect()
}

fn mappings(values: &[EmitterValueMapping]) -> Vec<ValueDraft> {
    values
        .iter()
        .map(|value| ValueDraft {
            name: value.column.clone(),
            expression: expression(&value.expression),
        })
        .collect()
}

fn destination(entity: &EmitterEntity) -> EmitterDestinationDraft {
    let mut draft = EmitterDestinationDraft {
        kind: Some(entity.kind()),
        publication: PublicationDraft::from_publication(entity.publishing()),
        ..EmitterDestinationDraft::default()
    };
    match entity.reference() {
        EmitterReference::StorageClient(client) => {
            draft.reference = Some(SelectedReference::chosen(ModelName::from(client)))
        }
        EmitterReference::Client(client) => {
            draft.reference = Some(SelectedReference::chosen(ModelName::from(client.client)))
        }
        EmitterReference::Schema(schema) => {
            draft.reference = Some(SelectedReference::chosen(ModelName::from(schema)))
        }
    }
    match entity {
        EmitterEntity::Client(_) | EmitterEntity::ZeroMq(_) | EmitterEntity::Syslog(_) => {}
        EmitterEntity::Http(HttpEmitter::Request(value)) => {
            draft.method = expression(&value.method);
            draft.path = expression(&value.path);
        }
        EmitterEntity::Kafka(KafkaEmitter::Topic(value)) => draft.locator = value.topic.to_string(),
        EmitterEntity::Pulsar(PulsarEmitter::Topic(value)) => {
            draft.locator = value.topic.to_string()
        }
        EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(value)) => {
            draft.locator = value.queue.to_string()
        }
        EmitterEntity::RabbitMq(RabbitMqEmitter::Exchange(value)) => {
            draft.locator = value.exchange.to_string();
            draft.routing_key = expression(&value.routing_key);
        }
        EmitterEntity::Redis(RedisEmitter::PubSub(value)) => {
            draft.locator = value.channel.to_string()
        }
        EmitterEntity::Mqtt(MqttEmitter::Topic(value)) => {
            draft.locator = value.topic.to_string();
            draft.qos = Some(value.qos);
        }
        EmitterEntity::Nats(NatsEmitter::Subject(value)) => {
            draft.locator = value.subject.to_string()
        }
        EmitterEntity::Nats(NatsEmitter::Stream(value)) => {
            draft.locator = value.stream.to_string();
            draft.subject = value.subject.to_string();
        }
        EmitterEntity::Sqs(SqsEmitter::Queue(value)) => {
            draft.locator = value.queue.to_string();
            draft.fifo_group = value.fifo_group.as_ref().map(expression);
            draft.send = Some(value.send);
        }
        EmitterEntity::Sentry(SentryEmitter::Event(value)) => {
            draft.platform = value.platform.clone()
        }
        EmitterEntity::Otel(OtelEmitter::Logs(value)) => otel(&mut draft, &value.mappings),
        EmitterEntity::Otel(OtelEmitter::Traces(value)) => otel(&mut draft, &value.mappings),
        EmitterEntity::Otel(OtelEmitter::Metrics(value)) => {
            otel(&mut draft, &value.mappings);
            metric(&mut draft, &value.metric);
        }
        EmitterEntity::ClickHouse(ClickHouseEmitter::Table(value)) => {
            draft.locator = value.table.to_string();
            draft.values = mappings(&value.values);
        }
        EmitterEntity::Postgres(PostgresEmitter::Table(value)) => {
            draft.locator = value.table.to_string();
            draft.values = mappings(&value.values);
            postgres_conflict(&mut draft, &value.conflict_action);
        }
        EmitterEntity::MySql(MySqlEmitter::Table(value)) => {
            draft.locator = value.table.to_string();
            draft.values = mappings(&value.values);
            draft.conflict = match value.conflict_action {
                MySqlConflictAction::None => ConflictDraft::None,
                MySqlConflictAction::DoNothing => ConflictDraft::DoNothing,
                MySqlConflictAction::DoUpdate => ConflictDraft::DoUpdate,
            };
        }
        EmitterEntity::MongoDb(MongoDbEmitter::Collection(value)) => {
            draft.locator = value.collection.to_string();
            draft.values = mappings(&value.values);
            mongodb_conflict(&mut draft, &value.conflict_action);
        }
        EmitterEntity::Iceberg(IcebergEmitter::Table(value)) => {
            draft.locator = value.table.to_string();
            draft.values = mappings(&value.values);
            draft.namespace = value.namespace.to_string();
            draft.location = value.location.clone();
            let IcebergCatalog::Rest { client } = &value.catalog;
            draft.catalog = Some(SelectedReference::chosen(client.clone()));
            draft.commit_each = value.commit_each.clone();
            draft.max_commit_size = value.max_commit_size.to_string();
        }
    }
    draft
}

fn postgres_conflict(draft: &mut EmitterDestinationDraft, value: &PostgresConflictAction) {
    match value {
        PostgresConflictAction::None => {}
        PostgresConflictAction::DoNothing { target } => {
            draft.conflict = ConflictDraft::DoNothing;
            draft.conflict_targets = target.clone();
        }
        PostgresConflictAction::DoUpdate { target } => {
            draft.conflict = ConflictDraft::DoUpdate;
            draft.conflict_targets = target.clone();
        }
    }
}

fn mongodb_conflict(draft: &mut EmitterDestinationDraft, value: &MongoDbConflictAction) {
    match value {
        MongoDbConflictAction::None => {}
        MongoDbConflictAction::DoNothing { target } => {
            draft.conflict = ConflictDraft::DoNothing;
            draft.conflict_targets = target.clone();
        }
        MongoDbConflictAction::DoUpdate { target } => {
            draft.conflict = ConflictDraft::DoUpdate;
            draft.conflict_targets = target.clone();
        }
    }
}

fn otel(draft: &mut EmitterDestinationDraft, value: &OtelMappings) {
    draft.values = mappings(&value.values);
    draft.attributes = mappings(&value.attributes);
    draft.resource = mappings(&value.resource);
    draft.scope = value.scope.as_ref().map(|scope| ScopeDraft {
        name: scope.name.clone(),
        version: scope.version.clone(),
    });
}

fn metric(draft: &mut EmitterDestinationDraft, value: &OtelMetric) {
    draft.metric_name = value.name.clone();
    draft.metric_unit = value.unit.clone();
    draft.metric_description = value.description.clone();
    match value.kind {
        OtelMetricKind::Gauge => draft.metric_kind = Some(MetricChoice::Gauge),
        OtelMetricKind::Sum {
            monotonic,
            temporality,
        } => {
            draft.metric_kind = Some(MetricChoice::Sum);
            draft.metric_monotonic = Some(monotonic);
            draft.metric_temporality = Some(temporality);
        }
        OtelMetricKind::Histogram { temporality } => {
            draft.metric_kind = Some(MetricChoice::Histogram);
            draft.metric_temporality = Some(temporality);
        }
    }
}

fn filled(value: &CreateEmitter) -> EmitterDraft {
    let mut draft = EmitterDraft {
        name: value.name.to_string(),
        destination: destination(&value.connector),
        collect: value.from.collect_policy.clone(),
        instances: if value.instances.is_one() {
            String::new()
        } else {
            value.instances.to_string()
        },
        mode: value.mode,
        predicate: match &value.construction.where_clause {
            Some(value) => expression(value),
            None => String::new(),
        },
        general_error: Some(value.error_policies.general.clone()),
        assignments: assignments(&value.construction.assignments),
        ..EmitterDraft::default()
    };
    let predicates = value
        .from
        .r#where
        .iter()
        .map(|item| (item.relay.clone(), expression(&item.where_clause)))
        .collect::<std::collections::BTreeMap<_, _>>();
    draft.inputs = value
        .from
        .from
        .iter()
        .map(|relay| InputDraft {
            relay: Some(SelectedReference::chosen(relay.clone())),
            where_clause: predicates.get(relay).cloned().unwrap_or_default(),
        })
        .collect();
    draft.body = Some(match &value.body {
        EmitterBody::Codec { codec } => {
            draft.codec = Some(SelectedReference::chosen(codec.clone()));
            BodyChoice::Codec
        }
        EmitterBody::WithoutBody => BodyChoice::WithoutBody,
        EmitterBody::Values => BodyChoice::Values,
        EmitterBody::Client => BodyChoice::Client,
    });
    draft.inherit = match &value.construction.inherit {
        None => InheritDraft::None,
        Some(Inheritance::All) => InheritDraft::All,
        Some(Inheritance::AllExcept(fields)) => InheritDraft::AllExcept(
            fields
                .iter()
                .cloned()
                .map(SelectedReference::chosen)
                .collect(),
        ),
        Some(Inheritance::Fields(fields)) => InheritDraft::Fields(
            fields
                .iter()
                .map(|field| InheritedFieldDraft {
                    field: SelectedReference::chosen(field.field.clone()),
                    leak_sensitive: field.leak_sensitive,
                })
                .collect(),
        ),
    };
    draft.invocations = value
        .construction
        .invocations
        .iter()
        .map(|call| InvocationDraft {
            function: call.function.to_string(),
            arguments: call.arguments.iter().map(expression).collect(),
        })
        .collect();
    draft.state = value
        .materialized_state
        .iter()
        .map(|state| StateDependencyDraft {
            relay: Some(SelectedReference::chosen(state.relay.clone())),
            policy: match &state.policy {
                MaterializedStatePolicy::RequiredSkip => StatePolicyDraft::RequiredSkip,
                MaterializedStatePolicy::RequiredWait => StatePolicyDraft::RequiredWait,
                MaterializedStatePolicy::Default(values) => {
                    StatePolicyDraft::Default(assignments(values))
                }
            },
        })
        .collect();
    draft.flush = match &value.flush_policy {
        FlushPolicy::Immediate => FlushDraft::Immediate,
        FlushPolicy::Each {
            interval,
            max_batch_size,
        } => FlushDraft::Each {
            interval: interval.clone(),
            max_batch_size: max_batch_size.clone(),
        },
    };
    draft.message_error = match &value.error_policies.message {
        MessageErrorPolicy::Ignore => MessageErrorDraft::Ignore,
        MessageErrorPolicy::Log => MessageErrorDraft::Log,
        MessageErrorPolicy::Dlq {
            relay,
            assignments: values,
        } => MessageErrorDraft::SendTo {
            relay: Some(SelectedReference::chosen(relay.clone())),
            assignments: assignments(values),
        },
    };
    if let Some(batch) = &value.batch {
        draft.batch.enabled = true;
        draft.batch.messages = batch.max_messages.to_string();
        draft.batch.size = batch.max_size.to_string();
    }
    for clause in value.envelope.clauses() {
        let form = match clause {
            EnvelopeClause::Key(v)
            | EnvelopeClause::Timestamp(v)
            | EnvelopeClause::ReplyTo(v)
            | EnvelopeClause::MessageId(v)
            | EnvelopeClause::DeduplicationId(v) => EnvelopeValueDraft::Expression(expression(v)),
            EnvelopeClause::Delay(v) | EnvelopeClause::DeliverAfter(v) => {
                EnvelopeValueDraft::Duration(v.to_string())
            }
            _ => EnvelopeValueDraft::Flag,
        };
        draft.envelope.clauses.insert(clause.kind(), form);
    }
    draft.envelope.properties = value
        .envelope
        .properties
        .iter()
        .map(|property| ValueDraft {
            name: property.name.clone(),
            expression: expression(&property.expression),
        })
        .collect();
    draft
}

fn round_trip(value: CreateEmitter, conditional: bool) -> bool {
    let mut draft = filled(&value);
    draft.if_not_exists = conditional;
    if let Err(expected) = value.validate_batch() {
        let Err(actual) = draft.build() else {
            panic!("a draft must refuse an out-of-contract batch");
        };
        assert_eq!(
            actual.current_context(),
            &EmitterDraftError::Invalid("batch policy")
        );
        assert_eq!(
            actual.downcast_ref::<EmitterBatchContractError>(),
            Some(expected.current_context())
        );
        let batch = value
            .batch
            .as_ref()
            .assured("the generated emitter always writes required batching");
        assert_eq!(draft.batch.messages, batch.max_messages.to_string());
        assert_eq!(draft.batch.size, batch.max_size.to_string());
        return false;
    }
    let actual = draft
        .build()
        .assured("every complete in-contract emitter is representable by its visual form");
    assert_eq!(actual, value, "{}", value.connector.kind());
    let statement = Statement::Create(CreateStatement::new(
        Box::new(Model::Emitter(actual)),
        conditional,
    ));
    let source = statement
        .to_canonical_plexus()
        .assured("every visual emitter renders canonical Plexus");
    assert_eq!(
        parse_client_statement(&source)
            .assured("canonical emitters reparse without losing a field"),
        ClientStatement::Server(statement),
        "{source}"
    );
    true
}

#[test]
fn bolero_emitter_drafts_preserve_every_complete_entity() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary =
                nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Plexus);
            for kind in EmitterEntityKind::all() {
                let value = arbitrary.create_emitter_to(kind);
                round_trip(value, arbitrary.entropy().flag());
            }
        });
}

#[test]
fn missing_and_changed_choices_keep_the_draft_editable() {
    let mut draft = EmitterDraft {
        name: "deliver".to_string(),
        ..EmitterDraft::default()
    };
    assert_eq!(
        draft
            .build()
            .err()
            .assured("an entity is required")
            .current_context(),
        &EmitterDraftError::Missing("emitter entity type")
    );
    let mut arbitrary =
        nervix_arbitrary::Arbitrary::new(&[42; 4096], nervix_arbitrary::Domain::Plexus);
    let value = arbitrary.create_emitter_to(EmitterEntityKind::Http(HttpEmitterEntity::Request));
    draft = filled(&value);
    draft.invalidate_references();
    assert_eq!(
        draft
            .build()
            .err()
            .assured("scope changes invalidate the reference")
            .current_context(),
        &EmitterDraftError::Changed("emitter reference")
    );
    assert_eq!(draft.name, value.name.to_string());
    assert_eq!(
        draft.destination.method,
        expression(match value.connector.as_ref() {
            EmitterEntity::Http(HttpEmitter::Request(value)) => &value.method,
            _ => unreachable!("the fixture selects HTTP"),
        })
    );
}

fn body_choices(form: EmitterBodyForm) -> &'static [BodyChoice] {
    match form {
        EmitterBodyForm::Codec => &[BodyChoice::Codec],
        EmitterBodyForm::CodecOrWithoutBody => &[BodyChoice::Codec, BodyChoice::WithoutBody],
        EmitterBodyForm::Values => &[BodyChoice::Values],
        EmitterBodyForm::Native => &[BodyChoice::Client],
    }
}

fn batch_choices(legality: EmitterBatchLegality) -> &'static [bool] {
    match legality {
        EmitterBatchLegality::Required { .. } => &[true],
        EmitterBatchLegality::Optional { .. } => &[false, true],
        EmitterBatchLegality::Unavailable { .. } => &[false],
    }
}

fn window_choices(mode: EmitterPublishingModeKind) -> &'static [&'static str] {
    match mode {
        EmitterPublishingModeKind::ClientAck | EmitterPublishingModeKind::BrokerAck => {
            &["sequential", "parallel"]
        }
        EmitterPublishingModeKind::NoAck | EmitterPublishingModeKind::RequestAck => &["none"],
    }
}

fn assert_publication(
    actual: EmitterPublishing<'_>,
    mode: EmitterPublishingModeKind,
    window: &str,
) {
    assert_eq!(actual.kind(), mode);
    assert_eq!(
        actual.retry_policy(),
        &RetryPolicy {
            backoff: "1ms".to_string(),
            max_backoff: "1s".to_string()
        }
    );
    match actual {
        EmitterPublishing::ClientAck { window: actual, .. }
        | EmitterPublishing::BrokerAck { window: actual, .. } => match (actual, window) {
            (AckWindow::Sequential, "sequential") => {}
            (AckWindow::Parallel { max }, "parallel") => assert_eq!(max.get(), 3),
            _ => panic!("the selected ACK window must retain its shape"),
        },
        EmitterPublishing::NoAck { .. } | EmitterPublishing::RequestAck { .. } => {
            assert_eq!(window, "none")
        }
    }
}

#[test]
fn every_declared_publication_body_and_batch_shape_has_complete_form_coverage() {
    use super::emitter_publication_draft::WindowDraft;
    let mut arbitrary = nervix_arbitrary::Arbitrary::new(&[], nervix_arbitrary::Domain::Plexus);
    for kind in EmitterEntityKind::all() {
        let fixture = arbitrary.create_emitter_to(kind);
        let capabilities = kind.capabilities();
        for publishing in capabilities.publishing_modes {
            for window in window_choices(publishing.kind) {
                for body in body_choices(capabilities.body) {
                    for batch in batch_choices(capabilities.batch) {
                        let mut draft = filled(&fixture);
                        draft.destination.publication = PublicationDraft {
                            kind: Some(publishing.kind),
                            window: match *window {
                                "sequential" => WindowDraft::Sequential,
                                "parallel" => WindowDraft::Parallel("3".to_string()),
                                _ => WindowDraft::Unselected,
                            },
                            ack_timeout: "5s".to_string(),
                            retry_backoff: "1ms".to_string(),
                            retry_max: "1s".to_string(),
                        };
                        for requirement in publishing.requires {
                            let choice = requirement
                                .choices
                                .first()
                                .assured("a declared requirement offers a choice");
                            draft.destination.choose_option(requirement.option, choice);
                        }
                        draft.choose_body(*body);
                        draft.inherit = InheritDraft::None;
                        if *body == BodyChoice::Codec {
                            draft.codec = Some(SelectedReference::chosen(
                                CodecName::parse("encoder").assured("valid codec name"),
                            ));
                        }
                        draft.batch.enabled = *batch;
                        draft.batch.messages = "1".to_string();
                        draft.batch.size = "1B".to_string();
                        let actual = draft.build().assured(
                            "the explicit declaration matrix uses a valid publication, body and \
                             batch shape",
                        );
                        assert_eq!(actual.connector.kind(), kind);
                        assert_publication(actual.connector.publishing(), publishing.kind, window);
                        assert_eq!(actual.batch.is_some(), *batch);
                        match (&actual.body, body) {
                            (EmitterBody::Codec { codec }, BodyChoice::Codec) => {
                                assert_eq!(codec.as_str(), "encoder")
                            }
                            (EmitterBody::WithoutBody, BodyChoice::WithoutBody)
                            | (EmitterBody::Values, BodyChoice::Values)
                            | (EmitterBody::Client, BodyChoice::Client) => {}
                            _ => panic!("the selected body must retain its form"),
                        }
                        assert!(round_trip(actual, true));
                    }
                }
            }
        }
    }
}

#[test]
fn wide_window_limits_and_invalid_policy_fields_keep_exact_values() {
    use super::emitter_publication_draft::WindowDraft;
    let mut arbitrary =
        nervix_arbitrary::Arbitrary::new(&[99; 4096], nervix_arbitrary::Domain::Plexus);
    let model = arbitrary.create_emitter_to(EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic));
    let mut draft = filled(&model);
    draft.destination.publication.kind = Some(EmitterPublishingModeKind::BrokerAck);
    draft.destination.publication.window = WindowDraft::Parallel(u64::MAX.to_string());
    draft.destination.publication.ack_timeout = "5s".to_string();
    let rebuilt = draft
        .build()
        .assured("the entire nonzero u64 ACK range is representable");
    assert!(
        matches!(rebuilt.connector.publishing(),EmitterPublishing::BrokerAck {window: AckWindow::Parallel {max},..} if max.get() == u64::MAX)
    );
    draft.destination.publication.window = WindowDraft::Parallel("0".to_string());
    let Err(error) = draft.build() else {
        panic!("zero ACK maximum must remain a local draft error");
    };
    assert_eq!(
        error.current_context(),
        &EmitterDraftError::Invalid("ACK maximum")
    );
    assert_eq!(draft.destination.publication.ack_timeout, "5s");
    draft.destination.publication.window = WindowDraft::Sequential;
    draft.destination.publication.retry_backoff.clear();
    let Err(error) = draft.build() else {
        panic!("retry must be explicit");
    };
    assert_eq!(
        error.current_context(),
        &EmitterDraftError::Missing("retry backoff")
    );
}
