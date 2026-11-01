//! Presentation of the entity a configured ingestor reads or emitter writes.
//!
//! Layer: vocabulary.
//! - **Owns.** The entity reference, locator, declared options and effective mode shared by
//!   canonical Plexus and inspection, including the distinction between a written option and
//!   its declared default.
//! - **Depends on.** Current semantic Models, their entity declarations and canonical value
//!   rendering.
//! - **Must not know.** Parsing, registry validation, runtime status, drivers or browser controls.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::BTreeMap;

use meticulous::OptionExt as _;

use super::{
    CanonicalPlexusError, Clause, IngestorBodyPlexus, duration_literal, expression_to_plexus,
    ingest_quiesce_to_plexus, retry_policy_to_plexus, string_literal, value_mapping_items,
};
use crate::{
    ClickHouseEmitter, ClientProducerIngestor, EmitterEntity, EmitterEntityKind, EmitterReference,
    HttpEmitter, HttpEndpointIngestor, HttpIngestor, HttpRequestIngestor, IcebergCatalog,
    IcebergEmitter, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration, IngestorEntity,
    IngestorEntityBody, IngestorEntityKind, IngestorEntityModel, IngestorInput, KafkaEmitter,
    KafkaIngestor, KafkaOffsetMode, KafkaTopicIngestor, LocatorDeclaration, MongoDbEmitter,
    MqttEmitter, MqttIngestor, MqttTopicIngestor, MySqlEmitter, NatsEmitter, NatsIngestor,
    NatsSubjectIngestor, OptionDeclaration, OptionPresence, OtelEmitter, OtelMetric,
    OtelMetricKind, PostgresEmitter, PrometheusIngestor, PrometheusQueryIngestor, PulsarEmitter,
    PulsarIngestor, PulsarTopicIngestor, RabbitMqEmitter, RabbitMqIngestor, RabbitMqQueueIngestor,
    RedisEmitter, RedisIngestor, RedisPubSubIngestor, SentryEmitter, SqsEmitter, SqsFifoGroup,
    SqsIngestor, SqsQueueIngestor, SyslogIngestor, SyslogListenerIngestor,
    WebsocketsConnectionIngestor, WebsocketsEndpointIngestor, WebsocketsIngestor, ZeroMqIngestor,
    ZeroMqPullIngestor,
};

#[derive(Debug, Clone, Copy)]
enum EntityBoundary {
    Ingestor(IngestorEntityKind),
    Emitter(EmitterEntityKind),
}

impl EntityBoundary {
    fn phrase(self) -> &'static str {
        match self {
            Self::Ingestor(kind) => kind.phrase(),
            Self::Emitter(kind) => kind.phrase(),
        }
    }

    fn locator(self) -> Option<LocatorDeclaration> {
        match self {
            Self::Ingestor(kind) => kind.capabilities().locator,
            Self::Emitter(kind) => kind.capabilities().locator,
        }
    }

    /// Every locator the entity declares, its alternatives included.
    fn locators(self) -> Vec<LocatorDeclaration> {
        match self {
            Self::Ingestor(kind) => kind.locators(),
            Self::Emitter(kind) => kind.capabilities().locator.into_iter().collect(),
        }
    }

    fn options(self) -> &'static [OptionDeclaration] {
        match self {
            Self::Ingestor(kind) => kind.capabilities().options,
            Self::Emitter(kind) => kind.capabilities().options,
        }
    }

    fn reference_label(self) -> &'static str {
        match self {
            Self::Ingestor(_) => "from",
            Self::Emitter(_) => "to",
        }
    }
}

#[derive(Debug, Clone)]
enum PayloadOption {
    Mapping(Vec<String>),
    Text(String),
}

impl PayloadOption {
    fn value(&self) -> String {
        match self {
            Self::Mapping(items) => format!("{{ {} }}", items.join(", ")),
            Self::Text(text) => text.clone(),
        }
    }

    fn clause(&self, keywords: &'static str) -> Clause {
        match self {
            Self::Mapping(items) => Clause::braced(keywords, items.clone()),
            Self::Text(text) => Clause::line(format!("{keywords} {text}")),
        }
    }
}

#[derive(Debug, Clone)]
enum PresentedOption {
    Written(String),
    Default(&'static str),
    Payload(PayloadOption),
}

impl PresentedOption {
    fn value(&self) -> String {
        match self {
            Self::Written(value) => value.clone(),
            Self::Default(value) => (*value).to_owned(),
            Self::Payload(value) => value.value(),
        }
    }
}

/// One entity's configuration projected for canonical Plexus and human inspection.
///
/// The declaration supplies the phrase, locator keywords, option order and defaults. A default
/// is visible to inspection while canonical text preserves an omitted optional Model field.
#[derive(Debug, Clone)]
pub struct EntityPresentation {
    boundary: EntityBoundary,
    reference: String,
    locator: Option<String>,
    options: BTreeMap<&'static str, PresentedOption>,
    mode: String,
    quiesce: Option<String>,
    conflict: Option<String>,
    /// The envelope section an emitter writes, as one line of canonical Plexus. Absent for an
    /// ingestor and for an emitter that writes none.
    envelope: Option<String>,
}

impl EntityPresentation {
    fn ingestor<T: IngestorDeclaration + IngestorEntityBody + IngestorEntityModel>(
        entity: &T,
    ) -> Self {
        Self {
            boundary: EntityBoundary::Ingestor(T::KIND),
            reference: entity.reference().name().to_string(),
            locator: None,
            options: BTreeMap::new(),
            mode: entity.acknowledgement().to_canonical_plexus(),
            quiesce: Some(ingest_quiesce_to_plexus(entity.quiesce())),
            conflict: None,
            envelope: None,
        }
    }

    fn declaration(&self, keywords: &str) -> &'static OptionDeclaration {
        // A current entity declares at most six options. Callers below name the options of
        // their concrete Model; generated canonical round trips exercise every entity.
        self.boundary
            .options()
            .iter()
            .find(|option| option.keywords == keywords)
            .assured("the concrete entity adapter names an option in its constant declaration")
    }

    fn external_locator(&mut self, name: &str) {
        let declaration = self
            .boundary
            .locator()
            .assured("this concrete entity declares its external locator");
        self.locator = Some(format!("{} {}", declaration.keywords, string_literal(name)));
    }

    /// Writes `name` after the keywords of `locator`, the one of its entity's declared locators the
    /// concrete Model holds.
    fn declared_locator(&mut self, locator: LocatorDeclaration, name: &str) {
        // An entity declares one locator and at most a handful of alternatives in its constant
        // declaration, so the scan is bounded by that declaration.
        let declared = self
            .boundary
            .locators()
            .into_iter()
            .find(|candidate| *candidate == locator)
            .assured("the concrete Model names a locator its entity declares");
        self.locator = Some(format!("{} {}", declared.keywords, string_literal(name)));
    }

    fn written_option(&mut self, keywords: &str, value: String) {
        let declaration = self.declaration(keywords);
        self.options
            .insert(declaration.keywords, PresentedOption::Written(value));
    }

    fn defaulted_option(&mut self, keywords: &str, value: String) {
        let declaration = self.declaration(keywords);
        let presentation = match declaration.presence {
            OptionPresence::Optional {
                default: Some(default),
            } if default == value => PresentedOption::Default(default),
            OptionPresence::Required | OptionPresence::Optional { .. } => {
                PresentedOption::Written(value)
            }
        };
        self.options.insert(declaration.keywords, presentation);
    }

    fn optional_option(&mut self, keywords: &str, value: Option<String>) {
        let declaration = self.declaration(keywords);
        match value {
            Some(value) => self.written_option(keywords, value),
            None => {
                if let OptionPresence::Optional {
                    default: Some(default),
                } = declaration.presence
                {
                    self.options
                        .insert(declaration.keywords, PresentedOption::Default(default));
                }
            }
        }
    }

    fn payload_option(&mut self, keywords: &str, value: PayloadOption) {
        let declaration = self.declaration(keywords);
        self.options
            .insert(declaration.keywords, PresentedOption::Payload(value));
    }

    /// MQTT's current body writes SESSION and QOS together when either differs from its default.
    fn write_choices_together(&mut self, keywords: &[&str]) {
        // Both bounds come from one MQTT declaration: two named choices among three options.
        let written = keywords.iter().any(|keyword| {
            matches!(
                self.options.get(*keyword),
                Some(PresentedOption::Written(_))
            )
        });
        if written {
            for keyword in keywords {
                if let Some(option @ PresentedOption::Default(_)) = self.options.get_mut(*keyword) {
                    *option = PresentedOption::Written(option.value());
                }
            }
        }
    }

    /// The connector and entity phrase, from the vocabulary's kind.
    pub fn phrase(&self) -> &'static str {
        self.boundary.phrase()
    }

    /// The entity phrase, configured reference and external locator, without its options or mode.
    pub fn location_to_plexus(&self) -> String {
        let mut location = format!("{} {}", self.phrase(), self.reference);
        if let Some(locator) = &self.locator {
            location.push(' ');
            location.push_str(locator);
        }
        location
    }

    /// Effective options in declared order, including declared defaults and payload options.
    pub fn options_to_plexus(&self) -> String {
        let mut options = Vec::new();
        for declaration in self.boundary.options() {
            if let Some(option) = self.options.get(declaration.keywords) {
                options.push(format!("{} {}", declaration.keywords, option.value()));
            }
        }
        if let Some(conflict) = &self.conflict {
            options.push(conflict.clone());
        }
        options.join(" ")
    }

    /// The same ordered configuration rows for either side of a connector.
    pub fn describe_lines(&self) -> Vec<String> {
        let options = self.options_to_plexus();
        let options = if options.is_empty() { "none" } else { &options };
        let quiesce = self.quiesce.as_deref().unwrap_or("none");
        vec![
            format!(
                "{}: {}",
                self.boundary.reference_label(),
                self.location_to_plexus()
            ),
            format!("options: {options}"),
            format!("mode: {}", self.mode),
            format!("quiesce: {quiesce}"),
            format!("envelope: {}", self.envelope.as_deref().unwrap_or("none")),
        ]
    }

    /// This presentation of an emitter's entity with the envelope section the emitter writes.
    pub fn with_envelope(
        mut self,
        envelope: &crate::EmitterEnvelope,
    ) -> error_stack::Result<Self, CanonicalPlexusError> {
        if !envelope.is_empty() {
            self.envelope = Some(envelope.to_canonical_plexus()?);
        }
        Ok(self)
    }

    pub(super) fn reference_to_plexus(&self) -> String {
        let mut words = vec![self.reference.clone()];
        if let Some(locator) = &self.locator {
            words.push(locator.clone());
        }
        for declaration in self.boundary.options() {
            if let Some(PresentedOption::Written(value)) = self.options.get(declaration.keywords) {
                words.push(format!("{} {value}", declaration.keywords));
            }
        }
        words.join(" ")
    }

    pub(super) fn ingestor_body_to_plexus(&self) -> String {
        let mut body = format!("{} MODE {}", self.reference_to_plexus(), self.mode);
        if let Some(quiesce) = &self.quiesce {
            body.push_str(" ON QUIESCE ");
            body.push_str(quiesce);
        }
        body
    }

    pub(super) fn payload_clauses(&self) -> Vec<Clause> {
        let mut clauses = Vec::new();
        for declaration in self.boundary.options() {
            if let Some(PresentedOption::Payload(value)) = self.options.get(declaration.keywords) {
                clauses.push(value.clause(declaration.keywords));
            }
        }
        if let Some(conflict) = &self.conflict {
            clauses.push(Clause::line(conflict.clone()));
        }
        clauses
    }
}

impl IngestAcknowledgement<'_> {
    /// The complete MODE value of a transport ingestor.
    pub fn to_canonical_plexus(self) -> String {
        let phrase = self.kind().phrase();
        match self {
            Self::Unacknowledged => phrase.to_owned(),
            Self::Request { max, timeout } => format!("{phrase} MAX {max} ACK TIMEOUT {timeout}"),
            Self::Sequential { timeout, retry } => {
                format!(
                    "{phrase} ACK TIMEOUT {timeout} RETRY POLICY {}",
                    retry_policy_to_plexus(retry)
                )
            }
            Self::Parallel {
                max,
                batch_timeout,
                timeout,
                retry,
            } => format!(
                "{phrase} MAX {max} BATCH TIMEOUT {batch_timeout} ACK TIMEOUT {timeout} RETRY \
                 POLICY {}",
                retry_policy_to_plexus(retry)
            ),
        }
    }
}

pub(super) trait IngestorPresentationModel {
    fn entity_presentation(&self) -> EntityPresentation;
}

impl<T: IngestorPresentationModel> IngestorBodyPlexus for T {
    fn body_to_plexus(&self) -> String {
        self.entity_presentation().ingestor_body_to_plexus()
    }
}

impl IngestorPresentationModel for ClientProducerIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        EntityPresentation {
            boundary: EntityBoundary::Ingestor(Self::KIND),
            reference: format!("SCHEMA {}", self.schema.as_str()),
            locator: None,
            options: BTreeMap::new(),
            mode: self.mode.to_canonical_plexus(),
            quiesce: Some(ingest_quiesce_to_plexus(&IngestQuiesceMode::Suspend)),
            conflict: None,
            envelope: None,
        }
    }
}

macro_rules! reference_ingestor_presentations {
    ($($Model:ty),+ $(,)?) => {
        $(impl IngestorPresentationModel for $Model {
            fn entity_presentation(&self) -> EntityPresentation {
                EntityPresentation::ingestor(self)
            }
        })+
    };
}

reference_ingestor_presentations!(
    HttpEndpointIngestor,
    WebsocketsEndpointIngestor,
    ZeroMqPullIngestor,
    SyslogListenerIngestor,
);

impl IngestorPresentationModel for WebsocketsConnectionIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        if !self.headers.is_empty() {
            let fields: Vec<_> = self
                .headers
                .iter()
                .map(|header| {
                    format!(
                        "{} = {}",
                        string_literal(&header.name),
                        string_literal(&header.value)
                    )
                })
                .collect();
            presentation.written_option("HEADERS", format!("{{ {} }}", fields.join(", ")));
        }
        presentation
    }
}

impl IngestorPresentationModel for HttpRequestIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.written_option("METHOD", string_literal(&self.method));
        if !self.headers.is_empty() {
            let fields: Vec<_> = self
                .headers
                .iter()
                .map(|header| {
                    format!(
                        "{} = {}",
                        string_literal(&header.name),
                        string_literal(&header.value)
                    )
                })
                .collect();
            presentation.written_option("HEADERS", format!("{{ {} }}", fields.join(", ")));
        }
        presentation.optional_option(
            "MAX BODY SIZE",
            self.max_body_size.map(|limit| limit.to_string()),
        );
        presentation.written_option("EVERY", duration_literal(self.every.as_nanos()));
        presentation
    }
}

impl IngestorPresentationModel for KafkaTopicIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.external_locator(self.topic.as_str());
        let offset = match &self.offset_mode {
            KafkaOffsetMode::Domain => "DOMAIN".to_owned(),
            KafkaOffsetMode::ConsumerGroup(group) => {
                format!("CONSUMER GROUP {}", string_literal(group.as_str()))
            }
        };
        presentation.written_option("OFFSET BY", offset);
        presentation.defaulted_option("INSTANCES", self.instances.to_string());
        presentation
    }
}

impl IngestorPresentationModel for PulsarTopicIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.external_locator(self.topic.as_str());
        presentation.written_option(
            "SUBSCRIPTION",
            format!(
                "{} {}",
                string_literal(self.subscription.name.as_str()),
                self.subscription.ownership.keywords()
            ),
        );
        presentation.defaulted_option("INSTANCES", self.instances.to_string());
        presentation
    }
}

impl IngestorPresentationModel for MqttTopicIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.external_locator(self.topic.as_str());
        presentation.defaulted_option("INSTANCES", self.instances.to_string());
        presentation.defaulted_option("SESSION", self.mode.session().keyword().to_owned());
        presentation.defaulted_option("QOS", self.mode.qos().spelling().to_owned());
        presentation.write_choices_together(&["SESSION", "QOS"]);
        presentation
    }
}

impl IngestorPresentationModel for NatsSubjectIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.external_locator(self.subject.as_str());
        presentation.written_option("QUEUE GROUP", string_literal(self.queue_group.as_str()));
        presentation.defaulted_option("INSTANCES", self.instances.to_string());
        presentation
    }
}

macro_rules! queue_ingestor_presentations {
    ($($Model:ty),+ $(,)?) => {
        $(impl IngestorPresentationModel for $Model {
            fn entity_presentation(&self) -> EntityPresentation {
                let mut presentation = EntityPresentation::ingestor(self);
                presentation.external_locator(self.queue.as_str());
                presentation.defaulted_option("INSTANCES", self.instances.to_string());
                presentation
            }
        })+
    };
}

queue_ingestor_presentations!(RabbitMqQueueIngestor, SqsQueueIngestor);

impl IngestorPresentationModel for RedisPubSubIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.declared_locator(self.subscription.locator(), self.subscription.as_str());
        presentation
    }
}

impl IngestorPresentationModel for PrometheusQueryIngestor {
    fn entity_presentation(&self) -> EntityPresentation {
        let mut presentation = EntityPresentation::ingestor(self);
        presentation.written_option("QUERY", string_literal(&self.query));
        let limit = self.max_body_size.map(|limit| limit.to_string());
        presentation.optional_option("MAX BODY SIZE", limit);
        presentation.written_option("EVERY", duration_literal(self.every.as_nanos()));
        presentation
    }
}

impl IngestorEntity {
    /// The shared presentation of this current transport entity.
    pub fn entity_presentation(&self) -> EntityPresentation {
        match self {
            Self::Http(HttpIngestor::Request(entity)) => entity.entity_presentation(),
            Self::Http(HttpIngestor::Endpoint(entity)) => entity.entity_presentation(),
            Self::Kafka(KafkaIngestor::Topic(entity)) => entity.entity_presentation(),
            Self::Pulsar(PulsarIngestor::Topic(entity)) => entity.entity_presentation(),
            Self::Mqtt(MqttIngestor::Topic(entity)) => entity.entity_presentation(),
            Self::Nats(NatsIngestor::Subject(entity)) => entity.entity_presentation(),
            Self::RabbitMq(RabbitMqIngestor::Queue(entity)) => entity.entity_presentation(),
            Self::Redis(RedisIngestor::PubSub(entity)) => entity.entity_presentation(),
            Self::Prometheus(PrometheusIngestor::Query(entity)) => entity.entity_presentation(),
            Self::ZeroMq(ZeroMqIngestor::Pull(entity)) => entity.entity_presentation(),
            Self::Sqs(SqsIngestor::Queue(entity)) => entity.entity_presentation(),
            Self::Websockets(WebsocketsIngestor::Connection(entity)) => {
                entity.entity_presentation()
            }
            Self::Websockets(WebsocketsIngestor::Endpoint(entity)) => entity.entity_presentation(),
            Self::Syslog(SyslogIngestor::Listener(entity)) => entity.entity_presentation(),
        }
    }
}

impl IngestorInput {
    /// The same entity configuration used by SHOW CREATE and DESCRIBE.
    pub fn entity_presentation(&self) -> EntityPresentation {
        match self {
            Self::Transport(input) => input.source.entity_presentation(),
            Self::Client(entity) => entity.entity_presentation(),
        }
    }
}

impl EmitterEntity {
    /// The same declared entity configuration used by canonical Plexus and inspection.
    pub fn entity_presentation(
        &self,
    ) -> error_stack::Result<EntityPresentation, CanonicalPlexusError> {
        let reference = match self.reference() {
            EmitterReference::Client(reference) => reference.client.as_str().to_owned(),
            EmitterReference::StorageClient(client) => client.as_str().to_owned(),
            EmitterReference::Schema(schema) => format!("SCHEMA {}", schema.as_str()),
        };
        let mut presentation = EntityPresentation {
            boundary: EntityBoundary::Emitter(self.kind()),
            reference,
            locator: None,
            options: BTreeMap::new(),
            mode: self.publishing().to_canonical_plexus(),
            quiesce: None,
            conflict: None,
            envelope: None,
        };
        self.present_options(&mut presentation)?;
        Ok(presentation)
    }

    fn present_options(
        &self,
        presentation: &mut EntityPresentation,
    ) -> error_stack::Result<(), CanonicalPlexusError> {
        match self {
            Self::Client(_) | Self::ZeroMq(_) | Self::Syslog(_) => {}
            Self::Sentry(SentryEmitter::Event(entity)) => {
                presentation
                    .optional_option("PLATFORM", entity.platform.as_deref().map(string_literal));
            }
            Self::Http(HttpEmitter::Request(entity)) => {
                presentation.written_option("METHOD", expression_to_plexus(&entity.method)?);
                presentation.written_option("PATH", expression_to_plexus(&entity.path)?);
            }
            Self::Kafka(KafkaEmitter::Topic(entity)) => {
                presentation.external_locator(entity.topic.as_str())
            }
            Self::Pulsar(PulsarEmitter::Topic(entity)) => {
                presentation.external_locator(entity.topic.as_str())
            }
            Self::RabbitMq(RabbitMqEmitter::Queue(entity)) => {
                presentation.external_locator(entity.queue.as_str())
            }
            Self::RabbitMq(RabbitMqEmitter::Exchange(entity)) => {
                presentation.external_locator(entity.exchange.as_str());
                presentation
                    .written_option("ROUTING KEY", expression_to_plexus(&entity.routing_key)?);
            }
            Self::Redis(RedisEmitter::PubSub(entity)) => {
                presentation.external_locator(entity.channel.as_str())
            }
            Self::Mqtt(MqttEmitter::Topic(entity)) => {
                presentation.external_locator(entity.topic.as_str())
            }
            Self::Nats(NatsEmitter::Subject(entity)) => {
                presentation.external_locator(entity.subject.as_str())
            }
            Self::Sqs(SqsEmitter::Queue(entity)) => {
                presentation.external_locator(entity.queue.as_str());
                let group = match &entity.fifo_group {
                    Some(SqsFifoGroup::FromBranch) => Some("FROM BRANCH".to_owned()),
                    Some(SqsFifoGroup::Expression(expression)) => {
                        Some(expression_to_plexus(expression)?)
                    }
                    None => None,
                };
                presentation.optional_option("FIFO GROUP", group);
                presentation.written_option("SEND", entity.send.keyword().to_owned());
            }
            Self::Otel(entity) => presentation.otel_options(entity)?,
            Self::ClickHouse(ClickHouseEmitter::Table(entity)) => {
                presentation.external_locator(entity.table.as_str())
            }
            Self::Postgres(PostgresEmitter::Table(entity)) => {
                presentation.external_locator(entity.table.as_str());
                presentation.conflict = entity.conflict_action.conflict_clause();
            }
            Self::MySql(MySqlEmitter::Table(entity)) => {
                presentation.external_locator(entity.table.as_str());
                presentation.conflict = entity.conflict_action.conflict_clause();
            }
            Self::MongoDb(MongoDbEmitter::Collection(entity)) => {
                presentation.external_locator(entity.collection.as_str());
                presentation.conflict = entity.conflict_action.conflict_clause();
            }
            Self::Iceberg(IcebergEmitter::Table(entity)) => {
                presentation.external_locator(entity.table.as_str());
                presentation.written_option("NAMESPACE", string_literal(entity.namespace.as_str()));
                presentation.written_option("LOCATION", string_literal(&entity.location));
                let IcebergCatalog::Rest { client } = &entity.catalog;
                presentation.written_option("CATALOG", client.as_str().to_owned());
                presentation.written_option(
                    "COMMIT EACH",
                    format!("{} MAX SIZE {}", entity.commit_each, entity.max_commit_size),
                );
            }
        }
        Ok(())
    }
}

impl EntityPresentation {
    fn otel_options(
        &mut self,
        entity: &OtelEmitter,
    ) -> error_stack::Result<(), CanonicalPlexusError> {
        if let OtelEmitter::Metrics(entity) = entity {
            self.written_option("METRIC", Self::metric_value(&entity.metric));
        }
        let mappings = entity.mappings();
        if !mappings.attributes.is_empty() {
            let items = value_mapping_items(&mappings.attributes)?;
            self.payload_option("ATTRIBUTES", PayloadOption::Mapping(items));
        }
        if !mappings.resource.is_empty() {
            let items = value_mapping_items(&mappings.resource)?;
            self.payload_option("RESOURCE", PayloadOption::Mapping(items));
        }
        if let Some(scope) = &mappings.scope {
            let mut value = string_literal(&scope.name);
            if let Some(version) = &scope.version {
                value.push_str(" VERSION ");
                value.push_str(&string_literal(version));
            }
            self.payload_option("SCOPE", PayloadOption::Text(value));
        }
        Ok(())
    }

    fn metric_value(metric: &OtelMetric) -> String {
        let mut words = vec![
            string_literal(&metric.name),
            "UNIT".to_owned(),
            string_literal(&metric.unit),
        ];
        if let Some(description) = &metric.description {
            words.push("DESCRIPTION".to_owned());
            words.push(string_literal(description));
        }
        match &metric.kind {
            OtelMetricKind::Gauge => words.push("GAUGE".to_owned()),
            OtelMetricKind::Sum {
                monotonic,
                temporality,
            } => {
                words.push("SUM".to_owned());
                if *monotonic {
                    words.push("MONOTONIC".to_owned());
                }
                words.push(temporality.as_ref().to_owned());
            }
            OtelMetricKind::Histogram { temporality } => {
                words.push("HISTOGRAM".to_owned());
                words.push(temporality.as_ref().to_owned());
            }
        }
        words.join(" ")
    }
}
