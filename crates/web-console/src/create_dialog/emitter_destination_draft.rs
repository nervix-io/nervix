//! Incomplete entity-specific settings for a visual emitter.
//!
//! Layer: edges.
//! - **Owns.** Destination references, external names, direct values and declared entity options.
//! - **Depends on.** The vocabulary's emitter union, declarations and expression parser.
//! - **Must not know.** Connector implementations, pools or external provisioning.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns browser emitter destination settings")
)]

use error_stack::{Report, ResultExt as _};
use nervix_models as vocabulary;
use nervix_models::{
    ClientName, EmitterEntity, EmitterEntityKind, EmitterReferenceKind, ExternalName, ModelKind,
    ModelName, NodeRef,
};
use nervix_plexus::parse_expression;
use strum::IntoEnumIterator as _;

use super::{
    SelectedReference,
    emitter_draft::{EmitterDraftError, required},
    emitter_envelope_draft::ValueDraft,
    emitter_publication_draft::PublicationDraft,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) enum ConflictDraft {
    #[default]
    None,
    DoNothing,
    DoUpdate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetricChoice {
    Gauge,
    Sum,
    Histogram,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ScopeDraft {
    pub(super) name: String,
    pub(super) version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EmitterDestinationDraft {
    pub(super) kind: Option<EmitterEntityKind>,
    pub(super) reference: Option<SelectedReference<ModelName>>,
    pub(super) catalog: Option<SelectedReference<ClientName>>,
    pub(super) publication: PublicationDraft,
    pub(super) locator: String,
    pub(super) subject: String,
    pub(super) method: String,
    pub(super) path: String,
    pub(super) routing_key: String,
    pub(super) qos: Option<vocabulary::MqttQos>,
    pub(super) send: Option<vocabulary::SqsSend>,
    pub(super) fifo_group: Option<String>,
    pub(super) platform: Option<String>,
    pub(super) values: Vec<ValueDraft>,
    pub(super) attributes: Vec<ValueDraft>,
    pub(super) resource: Vec<ValueDraft>,
    pub(super) scope: Option<ScopeDraft>,
    pub(super) metric_name: String,
    pub(super) metric_unit: String,
    pub(super) metric_description: Option<String>,
    pub(super) metric_kind: Option<MetricChoice>,
    pub(super) metric_monotonic: Option<bool>,
    pub(super) metric_temporality: Option<vocabulary::OtelAggregationTemporality>,
    pub(super) conflict: ConflictDraft,
    pub(super) conflict_targets: Vec<String>,
    pub(super) namespace: String,
    pub(super) location: String,
    pub(super) commit_each: String,
    pub(super) max_commit_size: String,
}

impl EmitterDestinationDraft {
    pub(super) fn select_reference(&mut self, node: &NodeRef) {
        let Some(kind) = self.kind else {
            return;
        };
        let expected = match kind.capabilities().reference {
            EmitterReferenceKind::Schema => ModelKind::Schema,
            EmitterReferenceKind::Client(_) | EmitterReferenceKind::StorageClient(_) => {
                ModelKind::Client
            }
        };
        if node.kind == expected {
            self.reference = Some(SelectedReference::chosen(node.identifier.clone()));
        }
    }

    pub(super) fn select_catalog(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::Client {
            self.catalog = Some(SelectedReference::chosen(ClientName::from(
                &node.identifier,
            )));
        }
    }

    pub(super) fn invalidate_references(&mut self) {
        if let Some(reference) = &mut self.reference {
            reference.invalidate();
        }
        if let Some(catalog) = &mut self.catalog {
            catalog.invalidate();
        }
    }

    fn reference(&self) -> error_stack::Result<&ModelName, EmitterDraftError> {
        let reference = self.reference.as_ref().ok_or_else(|| {
            Report::new(EmitterDraftError::Missing(
                "emitter client or output schema",
            ))
        })?;
        reference
            .current_name()
            .ok_or_else(|| Report::new(EmitterDraftError::Changed("emitter reference")))
    }

    fn client(&self) -> error_stack::Result<ClientName, EmitterDraftError> {
        Ok(ClientName::from(self.reference()?))
    }

    fn locator<N: ExternalName>(&self) -> error_stack::Result<N, EmitterDraftError> {
        N::parse(&self.locator).change_context(EmitterDraftError::Invalid("external locator"))
    }

    pub(super) fn mappings(
        rows: &[ValueDraft],
    ) -> error_stack::Result<Vec<vocabulary::EmitterValueMapping>, EmitterDraftError> {
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            values.push(row.build()?);
        }
        Ok(values)
    }

    fn otel_mappings(&self) -> error_stack::Result<vocabulary::OtelMappings, EmitterDraftError> {
        let scope = self.scope.as_ref().map(|scope| vocabulary::OtelScope {
            name: scope.name.clone(),
            version: scope.version.clone(),
        });
        Ok(vocabulary::OtelMappings {
            values: Self::mappings(&self.values)?,
            attributes: Self::mappings(&self.attributes)?,
            resource: Self::mappings(&self.resource)?,
            scope,
        })
    }

    fn metric(&self) -> error_stack::Result<vocabulary::OtelMetric, EmitterDraftError> {
        let kind = match self.metric_kind {
            Some(MetricChoice::Gauge) => vocabulary::OtelMetricKind::Gauge,
            Some(MetricChoice::Sum) => vocabulary::OtelMetricKind::Sum {
                monotonic: self.metric_monotonic.ok_or_else(|| {
                    Report::new(EmitterDraftError::Missing("metric monotonicity"))
                })?,
                temporality: self
                    .metric_temporality
                    .ok_or_else(|| Report::new(EmitterDraftError::Missing("metric temporality")))?,
            },
            Some(MetricChoice::Histogram) => vocabulary::OtelMetricKind::Histogram {
                temporality: self
                    .metric_temporality
                    .ok_or_else(|| Report::new(EmitterDraftError::Missing("metric temporality")))?,
            },
            None => return Err(Report::new(EmitterDraftError::Missing("metric kind"))),
        };
        Ok(vocabulary::OtelMetric {
            name: self.metric_name.clone(),
            unit: self.metric_unit.clone(),
            description: self.metric_description.clone(),
            kind,
        })
    }

    fn catalog(&self) -> error_stack::Result<vocabulary::IcebergCatalog, EmitterDraftError> {
        let selected = self
            .catalog
            .as_ref()
            .ok_or_else(|| Report::new(EmitterDraftError::Missing("catalog client")))?;
        let client = selected
            .current_name()
            .ok_or_else(|| Report::new(EmitterDraftError::Changed("catalog client")))?
            .clone();
        Ok(vocabulary::IcebergCatalog::Rest { client })
    }

    fn sqs_send(&self) -> error_stack::Result<vocabulary::SqsSend, EmitterDraftError> {
        if let Some(send) = self.send {
            return Ok(send);
        }
        let kind = self
            .kind
            .ok_or_else(|| Report::new(EmitterDraftError::Missing("emitter entity type")))?;
        if let Some(option) = kind.capabilities().option("SEND")
            && let vocabulary::OptionPresence::Optional {
                default: Some(default),
            } = option.presence
        {
            match default {
                "SINGLE" => return Ok(vocabulary::SqsSend::Single),
                "BATCH" => return Ok(vocabulary::SqsSend::Batch),
                _ => {
                    return Err(Report::new(EmitterDraftError::Invalid(
                        "declared SEND default",
                    )));
                }
            }
        }
        Err(Report::new(EmitterDraftError::Missing(
            "SEND request shape",
        )))
    }

    pub(super) fn choose_option(&mut self, option: &str, choice: &str) {
        match option {
            "QOS" => self.qos = vocabulary::MqttQos::iter().find(|qos| qos.spelling() == choice),
            "SEND" => {
                self.send = match choice {
                    "SINGLE" => Some(vocabulary::SqsSend::Single),
                    "BATCH" => Some(vocabulary::SqsSend::Batch),
                    _ => None,
                }
            }
            "ON CONFLICT" => {
                self.conflict = match choice {
                    "DO NOTHING" => ConflictDraft::DoNothing,
                    "DO UPDATE" => ConflictDraft::DoUpdate,
                    _ => ConflictDraft::None,
                }
            }
            "METRIC" => {
                self.metric_kind = match choice {
                    "GAUGE" => Some(MetricChoice::Gauge),
                    "SUM" => Some(MetricChoice::Sum),
                    "HISTOGRAM" => Some(MetricChoice::Histogram),
                    _ => None,
                }
            }
            _ => {}
        }
    }

    pub(super) fn chosen_option(&self, option: &str) -> Option<&'static str> {
        match option {
            "QOS" => self.qos.map(vocabulary::MqttQos::spelling),
            "SEND" => self.send.map(Into::into),
            "ON CONFLICT" => match self.conflict {
                ConflictDraft::None => None,
                ConflictDraft::DoNothing => Some("DO NOTHING"),
                ConflictDraft::DoUpdate => Some("DO UPDATE"),
            },
            "METRIC" => match self.metric_kind {
                Some(MetricChoice::Gauge) => Some("GAUGE"),
                Some(MetricChoice::Sum) => Some("SUM"),
                Some(MetricChoice::Histogram) => Some("HISTOGRAM"),
                None => None,
            },
            _ => None,
        }
    }

    pub(super) fn publication_allowed(&self, mode: vocabulary::EmitterPublishingModeKind) -> bool {
        let Some(kind) = self.kind else {
            return false;
        };
        kind.capabilities().declared_publishing(mode).is_some()
            && kind
                .capabilities()
                .unmet_publishing(kind, mode, &|option| self.chosen_option(option))
                .is_none()
    }

    pub(super) fn build(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        let kind = self
            .kind
            .ok_or_else(|| Report::new(EmitterDraftError::Missing("emitter entity type")))?;
        let publishing = self
            .publication
            .kind
            .ok_or_else(|| Report::new(EmitterDraftError::Missing("publishing mode")))?;
        if !kind.capabilities().accepts_publishing_mode_kind(publishing) {
            return Err(Report::new(EmitterDraftError::Invalid("publishing mode")));
        }
        let entity = self.build_entity(kind)?;
        if let Some(requirement) = entity.unmet_publishing() {
            return Err(
                Report::new(requirement).change_context(EmitterDraftError::Invalid(
                    "publishing mode and entity option combination",
                )),
            );
        }
        Ok(entity)
    }

    fn build_entity(
        &self,
        kind: EmitterEntityKind,
    ) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        match kind {
            EmitterEntityKind::Client(vocabulary::ClientEmitterEntity::Consumer) => {
                self.client_entity()
            }
            EmitterEntityKind::Http(vocabulary::HttpEmitterEntity::Request) => self.http_entity(),
            EmitterEntityKind::Kafka(vocabulary::KafkaEmitterEntity::Topic) => self.kafka_entity(),
            EmitterEntityKind::Pulsar(vocabulary::PulsarEmitterEntity::Topic) => {
                self.pulsar_entity()
            }
            EmitterEntityKind::RabbitMq(vocabulary::RabbitMqEmitterEntity::Queue) => {
                self.rabbitmq_queue_entity()
            }
            EmitterEntityKind::RabbitMq(vocabulary::RabbitMqEmitterEntity::Exchange) => {
                self.rabbitmq_exchange_entity()
            }
            EmitterEntityKind::Redis(vocabulary::RedisEmitterEntity::PubSub) => self.redis_entity(),
            EmitterEntityKind::Mqtt(vocabulary::MqttEmitterEntity::Topic) => self.mqtt_entity(),
            EmitterEntityKind::Nats(vocabulary::NatsEmitterEntity::Subject) => {
                self.nats_subject_entity()
            }
            EmitterEntityKind::Nats(vocabulary::NatsEmitterEntity::Stream) => {
                self.nats_stream_entity()
            }
            EmitterEntityKind::ZeroMq(vocabulary::ZeroMqEmitterEntity::Push) => {
                self.zeromq_entity()
            }
            EmitterEntityKind::Sqs(vocabulary::SqsEmitterEntity::Queue) => self.sqs_entity(),
            EmitterEntityKind::Sentry(vocabulary::SentryEmitterEntity::Event) => {
                self.sentry_entity()
            }
            EmitterEntityKind::Syslog(vocabulary::SyslogEmitterEntity::Collector) => {
                self.syslog_entity()
            }
            EmitterEntityKind::Otel(vocabulary::OtelEmitterEntity::Logs) => self.otel_logs_entity(),
            EmitterEntityKind::Otel(vocabulary::OtelEmitterEntity::Traces) => {
                self.otel_traces_entity()
            }
            EmitterEntityKind::Otel(vocabulary::OtelEmitterEntity::Metrics) => {
                self.otel_metrics_entity()
            }
            EmitterEntityKind::ClickHouse(vocabulary::ClickHouseEmitterEntity::Table) => {
                self.clickhouse_entity()
            }
            EmitterEntityKind::Postgres(vocabulary::PostgresEmitterEntity::Table) => {
                self.postgres_entity()
            }
            EmitterEntityKind::MySql(vocabulary::MySqlEmitterEntity::Table) => self.mysql_entity(),
            EmitterEntityKind::MongoDb(vocabulary::MongoDbEmitterEntity::Collection) => {
                self.mongodb_entity()
            }
            EmitterEntityKind::Iceberg(vocabulary::IcebergEmitterEntity::Table) => {
                self.iceberg_entity()
            }
        }
    }
    fn client_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Client(vocabulary::ClientEmitter::Consumer(
            vocabulary::ClientConsumerEmitter {
                mode: self.publication.client()?,
                schema: vocabulary::SchemaName::from(self.reference()?),
            },
        )))
    }

    fn http_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Http(vocabulary::HttpEmitter::Request(
            vocabulary::HttpRequestEmitter {
                mode: self.publication.http()?,
                client: self.client()?,
                method: parse_expression(&self.method)
                    .change_context(EmitterDraftError::Invalid("HTTP method expression"))?,
                path: parse_expression(&self.path)
                    .change_context(EmitterDraftError::Invalid("HTTP path expression"))?,
            },
        )))
    }

    fn kafka_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Kafka(vocabulary::KafkaEmitter::Topic(
            vocabulary::KafkaTopicEmitter {
                mode: self.publication.kafka()?,
                client: self.client()?,
                topic: self.locator()?,
            },
        )))
    }

    fn pulsar_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Pulsar(vocabulary::PulsarEmitter::Topic(
            vocabulary::PulsarTopicEmitter {
                mode: self.publication.pulsar()?,
                client: self.client()?,
                topic: self.locator()?,
            },
        )))
    }

    fn rabbitmq_queue_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::RabbitMq(vocabulary::RabbitMqEmitter::Queue(
            vocabulary::RabbitMqQueueEmitter {
                mode: self.publication.rabbitmq_queue()?,
                client: self.client()?,
                queue: self.locator()?,
            },
        )))
    }

    fn rabbitmq_exchange_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::RabbitMq(
            vocabulary::RabbitMqEmitter::Exchange(vocabulary::RabbitMqExchangeEmitter {
                mode: self.publication.rabbitmq_exchange()?,
                client: self.client()?,
                exchange: self.locator()?,
                routing_key: parse_expression(&self.routing_key)
                    .change_context(EmitterDraftError::Invalid("routing key expression"))?,
            }),
        ))
    }

    fn redis_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Redis(vocabulary::RedisEmitter::PubSub(
            vocabulary::RedisPubSubEmitter {
                mode: self.publication.redis()?,
                client: self.client()?,
                channel: self.locator()?,
            },
        )))
    }

    fn mqtt_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Mqtt(vocabulary::MqttEmitter::Topic(
            vocabulary::MqttTopicEmitter {
                mode: self.publication.mqtt()?,
                client: self.client()?,
                topic: self.locator()?,
                qos: self
                    .qos
                    .ok_or_else(|| Report::new(EmitterDraftError::Missing("QOS")))?,
            },
        )))
    }

    fn nats_subject_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Nats(vocabulary::NatsEmitter::Subject(
            vocabulary::NatsSubjectEmitter {
                mode: self.publication.nats_subject()?,
                client: self.client()?,
                subject: self.locator()?,
            },
        )))
    }

    fn nats_stream_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Nats(vocabulary::NatsEmitter::Stream(
            vocabulary::NatsStreamEmitter {
                mode: self.publication.nats_stream()?,
                client: self.client()?,
                stream: self.locator()?,
                subject: vocabulary::NatsPublishSubjectName::parse(&self.subject)
                    .change_context(EmitterDraftError::Invalid("publication subject"))?,
            },
        )))
    }

    fn zeromq_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::ZeroMq(vocabulary::ZeroMqEmitter::Push(
            vocabulary::ZeroMqPushEmitter {
                mode: self.publication.zeromq()?,
                client: self.client()?,
            },
        )))
    }

    fn sentry_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Sentry(vocabulary::SentryEmitter::Event(
            vocabulary::SentryEventEmitter {
                mode: self.publication.sentry()?,
                client: self.client()?,
                platform: self.platform.clone(),
            },
        )))
    }

    fn syslog_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Syslog(vocabulary::SyslogEmitter::Collector(
            vocabulary::SyslogCollectorEmitter {
                mode: self.publication.syslog()?,
                client: self.client()?,
            },
        )))
    }

    fn otel_logs_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Otel(vocabulary::OtelEmitter::Logs(
            vocabulary::OtelLogsEmitter {
                mode: self.publication.otel_logs()?,
                client: self.client()?,
                mappings: self.otel_mappings()?,
            },
        )))
    }

    fn otel_traces_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Otel(vocabulary::OtelEmitter::Traces(
            vocabulary::OtelTracesEmitter {
                mode: self.publication.otel_traces()?,
                client: self.client()?,
                mappings: self.otel_mappings()?,
            },
        )))
    }

    fn otel_metrics_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Otel(vocabulary::OtelEmitter::Metrics(
            vocabulary::OtelMetricsEmitter {
                mode: self.publication.otel_metrics()?,
                client: self.client()?,
                metric: self.metric()?,
                mappings: self.otel_mappings()?,
            },
        )))
    }

    fn clickhouse_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::ClickHouse(
            vocabulary::ClickHouseEmitter::Table(vocabulary::ClickHouseTableEmitter {
                mode: self.publication.clickhouse()?,
                client: self.client()?,
                table: self.locator()?,
                values: Self::mappings(&self.values)?,
            }),
        ))
    }

    fn iceberg_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        Ok(EmitterEntity::Iceberg(vocabulary::IcebergEmitter::Table(
            vocabulary::IcebergTableEmitter {
                mode: self.publication.iceberg()?,
                client: self.client()?,
                table: self.locator()?,
                namespace: vocabulary::IcebergNamespace::parse(&self.namespace)
                    .change_context(EmitterDraftError::Invalid("namespace"))?,
                values: Self::mappings(&self.values)?,
                location: self.location.clone(),
                catalog: self.catalog()?,
                commit_each: required(&self.commit_each, "commit cadence")?,
                max_commit_size: self
                    .max_commit_size
                    .parse::<vocabulary::PayloadSizeLimit>()
                    .change_context(EmitterDraftError::Invalid("maximum commit size"))?,
            },
        )))
    }

    fn sqs_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        let fifo_group = match &self.fifo_group {
            Some(expression) => Some(
                parse_expression(expression)
                    .change_context(EmitterDraftError::Invalid("FIFO group expression"))?,
            ),
            None => None,
        };
        Ok(EmitterEntity::Sqs(vocabulary::SqsEmitter::Queue(
            vocabulary::SqsQueueEmitter {
                mode: self.publication.sqs()?,
                client: self.client()?,
                queue: self.locator()?,
                fifo_group,
                send: self.sqs_send()?,
            },
        )))
    }

    fn postgres_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        let conflict_action = match self.conflict {
            ConflictDraft::None => vocabulary::PostgresConflictAction::None,
            ConflictDraft::DoNothing => vocabulary::PostgresConflictAction::DoNothing {
                target: self.conflict_targets.clone(),
            },
            ConflictDraft::DoUpdate => vocabulary::PostgresConflictAction::DoUpdate {
                target: self.conflict_targets.clone(),
            },
        };
        Ok(EmitterEntity::Postgres(vocabulary::PostgresEmitter::Table(
            vocabulary::PostgresTableEmitter {
                mode: self.publication.postgres()?,
                client: self.client()?,
                table: self.locator()?,
                values: Self::mappings(&self.values)?,
                conflict_action,
            },
        )))
    }

    fn mysql_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        let conflict_action = match self.conflict {
            ConflictDraft::None => vocabulary::MySqlConflictAction::None,
            ConflictDraft::DoNothing => vocabulary::MySqlConflictAction::DoNothing,
            ConflictDraft::DoUpdate => vocabulary::MySqlConflictAction::DoUpdate,
        };
        Ok(EmitterEntity::MySql(vocabulary::MySqlEmitter::Table(
            vocabulary::MySqlTableEmitter {
                mode: self.publication.mysql()?,
                client: self.client()?,
                table: self.locator()?,
                values: Self::mappings(&self.values)?,
                conflict_action,
            },
        )))
    }

    fn mongodb_entity(&self) -> error_stack::Result<EmitterEntity, EmitterDraftError> {
        let conflict_action = match self.conflict {
            ConflictDraft::None => vocabulary::MongoDbConflictAction::None,
            ConflictDraft::DoNothing => vocabulary::MongoDbConflictAction::DoNothing {
                target: self.conflict_targets.clone(),
            },
            ConflictDraft::DoUpdate => vocabulary::MongoDbConflictAction::DoUpdate {
                target: self.conflict_targets.clone(),
            },
        };
        Ok(EmitterEntity::MongoDb(
            vocabulary::MongoDbEmitter::Collection(vocabulary::MongoDbCollectionEmitter {
                mode: self.publication.mongodb()?,
                client: self.client()?,
                collection: self.locator()?,
                values: Self::mappings(&self.values)?,
                conflict_action,
            }),
        ))
    }
}
