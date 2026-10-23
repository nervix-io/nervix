//! Decides the complete specification for starting one emitter's sink.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an emitter and the client Models its sink names into one typed start
//!   plan: each connector's clients, sink parameters and publishing mode, lowered row mappings,
//!   and the emitter's retry policy. It also binds every planned client to the configuration the
//!   host resolved for it.
//! - **Depends on.** The emitter and client Models, runtime vocabulary values, and the contract
//!   crate's resolved client configuration and parsed retry policy.
//! - **Must not know.** Tokio, locks, shared maps, connector I/O, how a resource mount is
//!   resolved, or task spawning.
//!
//! A plan is decided in two steps. [`EmitterStartPlan::decide`] reads the Models once and yields a
//! plan whose clients carry what their Models declare: the resource version each one mounts and the
//! entries rendered against that mount. The host resolves those mounts, and
//! [`EmitterStartPlan::resolve_clients`] binds the resolved paths into the plan that the emitter
//! task and its sink constructors receive. Neither of them sees a Model.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{num::NonZeroUsize, time::Duration};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, ParsedRetryPolicy, ResolvedClientConfig,
    optional_client_config_value,
};
use nervix_connector_iceberg::IcebergCommitPolicy;
use nervix_connector_mongodb::MongoDbConflictAction;
use nervix_connector_mqtt::MqttPublishingMode;
use nervix_connector_mysql::MySqlConflictAction;
use nervix_connector_nats::NatsPublishingMode;
use nervix_connector_otel::{
    OtelAggregationTemporality, OtelLiteral, OtelMetric, OtelMetricKind, OtelResourceAttribute,
    OtelScope, OtelSignal,
};
use nervix_connector_postgres::PostgresConflictAction;
use nervix_connector_sqs::SqsPublishingMode;
use nervix_models::{
    AckWindow, Assignment, AssignmentTarget, CLIENT_CONSUMER_SESSION_BYTES, ClickHouseSink,
    ClickHouseTableSink, ClickHouseValueMapping, ClientConfigEntry, ClientConsumerSink, ClientName,
    ClientPoolBounds, ClientResourceMount, ClientSink, CreateEmitter, EmitSink, EmitterBatchPolicy,
    Expression, FieldName, HttpOrigin, HttpRequestSink, HttpSink, IcebergCatalog, IcebergSink,
    IcebergStorageBackend, IcebergTableSink, KafkaSink, KafkaTopicSink, Literal, Model,
    MongoDbCollectionSink, MongoDbSink, MqttSink, MqttTopicSink, MySqlSink, MySqlTableSink,
    NatsSink, NatsSubjectSink, OtelMappings, OtelMetricsSink, OtelSink, PostgresSink,
    PostgresTableSink, PulsarSink, PulsarTopicSink, RabbitMqQueueSink, RabbitMqSink,
    RedisPubSubSink, RedisSink, RetryPolicy, RouteConstruction, SchemaName, SentryEventSink,
    SentrySink, SqsQueueSink, SqsSink, SyslogCollectorSink, SyslogSink, ZeroMqPushSink, ZeroMqSink,
    parse_duration_text,
};
use nervix_vm::{
    SemanticScopePolicy, lower_route_construction,
    program::{Program, SpannedNode},
};
use thiserror::Error;

/// A duration an emitter's publishing mode declares, named the way its diagnostics read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(super) enum EmitterDurationSetting {
    #[strum(serialize = "retry backoff")]
    RetryBackoff,
    #[strum(serialize = "retry max backoff")]
    RetryMaxBackoff,
    #[strum(serialize = "ack timeout")]
    AckTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(super) enum IcebergCommitSetting {
    #[strum(serialize = "commit_each")]
    CommitEach,
    #[strum(serialize = "max_commit_size")]
    MaxCommitSize,
}

impl EmitterDurationSetting {
    /// The duration `value` declares for this setting.
    fn parse(self, value: &str) -> Result<Duration, Report<EmitterStartPlanError>> {
        parse_duration_text(value).change_context_lazy(|| EmitterStartPlanError::InvalidDuration {
            setting: self,
            value: value.to_string(),
        })
    }
}

/// Why an emitter's sink cannot be planned from its Model and the client Models it names.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum EmitterStartPlanError {
    #[error("{sink} emitter client '{client}' does not exist")]
    MissingClient {
        sink: &'static str,
        client: ClientName,
    },
    #[error("{sink} emitter requires a {expected} client, found {found} '{client}'")]
    ClientKindMismatch {
        sink: &'static str,
        expected: nervix_models::SinkReferenceKind,
        found: &'static str,
        client: ClientName,
    },
    #[error("emitter client reference '{expected}' resolved to client '{found}'")]
    ClientIdentityMismatch {
        expected: ClientName,
        found: ClientName,
    },
    #[error("Iceberg catalog client '{client}' does not exist")]
    MissingCatalogClient { client: ClientName },
    #[error("Iceberg catalog client '{client}' must be an ICEBERG_REST client, found {found}")]
    CatalogClientKindMismatch {
        client: ClientName,
        found: &'static str,
    },
    #[error("invalid {setting} '{value}'")]
    InvalidDuration {
        setting: EmitterDurationSetting,
        value: String,
    },
    #[error("retry backoff must be greater than zero")]
    ZeroRetryBackoff,
    #[error("retry max backoff must be greater than or equal to retry backoff")]
    RetryMaxBackoffBelowBackoff,
    #[error("ack timeout must be greater than zero")]
    ZeroAckTimeout,
    #[error("{} emitter batch maximum {bytes} bytes exceeds the 32 MiB consumer window", <ClientConsumerSink as nervix_models::EmitterEntityModel>::KIND)]
    ClientBatchTooLarge { bytes: u64 },
    #[error("{} emitter client '{client}' declares no http or https origin endpoint", <HttpRequestSink as nervix_models::EmitterEntityModel>::KIND)]
    InvalidHttpEndpoint { client: ClientName },
    #[error("{sink} emitter '{emitter}' requires at least one VALUES mapping")]
    EmptyValues {
        sink: &'static str,
        emitter: nervix_models::EmitterName,
    },
    #[error("{sink} emitter '{emitter}' has invalid VALUES mappings")]
    InvalidValues {
        sink: &'static str,
        emitter: nervix_models::EmitterName,
    },
    #[error("{kind} emitter '{emitter}' has a nonliteral RESOURCE attribute '{attribute}'")]
    InvalidOtelResource {
        kind: nervix_models::EmitSinkKind,
        emitter: nervix_models::EmitterName,
        attribute: String,
    },
    #[error("{} emitter '{emitter}' has invalid {setting} '{value}'", <IcebergTableSink as nervix_models::EmitterEntityModel>::KIND)]
    InvalidIcebergCommit {
        emitter: nervix_models::EmitterName,
        setting: IcebergCommitSetting,
        value: String,
    },
}

/// A row sink's ordered external columns and its already lowered expression program.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MappedValuesPlan {
    pub(super) columns: Vec<String>,
    pub(super) program: SpannedNode<Program>,
}

impl MappedValuesPlan {
    pub(crate) fn decide(
        emitter: &nervix_models::EmitterName,
        sink: &'static str,
        namespace: &'static str,
        mappings: &[ClickHouseValueMapping],
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        if mappings.is_empty() {
            return Err(Report::new(EmitterStartPlanError::EmptyValues {
                sink,
                emitter: emitter.clone(),
            }));
        }
        let assignments = mappings
            .iter()
            .enumerate()
            .map(|(index, mapping)| {
                let field = FieldName::parse(&format!("c{index}"))
                    .assured("c followed by decimal digits is a valid field name");
                Assignment {
                    target: AssignmentTarget::bare(field),
                    value: mapping.expression.clone(),
                }
            })
            .collect();
        let program = lower_route_construction(
            &RouteConstruction {
                assignments,
                ..RouteConstruction::default()
            },
            SemanticScopePolicy::read_write("input", namespace),
        )
        .change_context(EmitterStartPlanError::InvalidValues {
            sink,
            emitter: emitter.clone(),
        })?;
        Ok(Self {
            columns: mappings
                .iter()
                .map(|mapping| mapping.column.clone())
                .collect(),
            program,
        })
    }
}

fn otel_resource_attributes(
    emitter: &CreateEmitter,
    resource: &[ClickHouseValueMapping],
) -> Result<Vec<OtelResourceAttribute>, Report<EmitterStartPlanError>> {
    resource
        .iter()
        .map(|mapping| {
            let value = otel_literal(&mapping.expression).ok_or_else(|| {
                Report::new(EmitterStartPlanError::InvalidOtelResource {
                    kind: emitter.sink.kind(),
                    emitter: emitter.name.clone(),
                    attribute: mapping.column.clone(),
                })
            })?;
            Ok(OtelResourceAttribute {
                key: mapping.column.clone(),
                value,
            })
        })
        .collect()
}

fn otel_literal(expression: &Expression) -> Option<OtelLiteral> {
    match expression {
        Expression::Literal(Literal::I64(value)) => Some(OtelLiteral::I64(*value)),
        Expression::Literal(Literal::F64(value)) => Some(OtelLiteral::F64(value.value())),
        Expression::Literal(Literal::Bool(value)) => Some(OtelLiteral::Bool(*value)),
        Expression::Literal(Literal::String(value)) => Some(OtelLiteral::String(value.clone())),
        Expression::Literal(Literal::Null) => Some(OtelLiteral::Null),
        Expression::Array(items) => Some(OtelLiteral::Array(
            items.iter().map(otel_literal).collect::<Option<Vec<_>>>()?,
        )),
        _ => None,
    }
}

fn iceberg_commit_policy(
    emitter: &CreateEmitter,
    commit_each: &str,
    max_commit_size: &str,
) -> Result<IcebergCommitPolicy, Report<EmitterStartPlanError>> {
    let interval = parse_duration_text(commit_each).change_context_lazy(|| {
        EmitterStartPlanError::InvalidIcebergCommit {
            emitter: emitter.name.clone(),
            setting: IcebergCommitSetting::CommitEach,
            value: commit_each.to_string(),
        }
    })?;
    let max_size = max_commit_size
        .parse::<ubyte::ByteUnit>()
        .map_err(|source| {
            Report::new(EmitterStartPlanError::InvalidIcebergCommit {
                emitter: emitter.name.clone(),
                setting: IcebergCommitSetting::MaxCommitSize,
                value: max_commit_size.to_string(),
            })
            .attach_printable(source)
        })?
        .as_u64();
    Ok(IcebergCommitPolicy { interval, max_size })
}

impl EmitterStartPlanError {
    /// The report for a sink whose client name resolved to a Model of another kind.
    fn client_kind_mismatch(sink: &EmitSink, client: &Model) -> Report<Self> {
        Report::new(Self::ClientKindMismatch {
            sink: sink.transport_label(),
            expected: sink.capabilities().reference,
            found: Self::found_label(client),
            client: sink
                .client()
                .verified("only external sinks resolve client Models")
                .clone(),
        })
    }

    /// How a Model that a client name resolved to reads in a kind mismatch: the connector it
    /// configures, or its model kind when the name resolved to something other than a client.
    fn found_label(model: &Model) -> &'static str {
        match model.connector_kind() {
            Some(connector) => connector.keyword(),
            None => model.kind().into(),
        }
    }
}

/// A client's configuration as its Model declares it: the resource version it mounts, and the
/// entries the host renders against that mount when it resolves the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DeclaredClientConfig {
    pub(super) mount: Option<ClientResourceMount>,
    pub(super) entries: Vec<ClientConfigEntry>,
}

/// One client a sink connects through.
///
/// `Config` is what the plan knows of the client's configuration: what its Model declares until
/// the host resolves the client, and afterwards the rendered entries together with the mounted
/// paths they read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EmitterClientSpec<Config = ResolvedClientConfig> {
    pub(super) name: ClientName,
    pub(super) config: Config,
}

impl EmitterClientSpec<DeclaredClientConfig> {
    /// The client a sink references by `expected`, as the Model registered under that name
    /// declares it.
    fn declared(
        expected: &ClientName,
        name: &ClientName,
        mount: Option<&ClientResourceMount>,
        entries: &[ClientConfigEntry],
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        if expected != name {
            return Err(Report::new(EmitterStartPlanError::ClientIdentityMismatch {
                expected: expected.clone(),
                found: name.clone(),
            }));
        }
        Ok(Self {
            name: name.clone(),
            config: DeclaredClientConfig {
                mount: mount.cloned(),
                entries: entries.to_vec(),
            },
        })
    }

    /// This client bound to the configuration `resolve` returns for its declaration.
    fn resolve<Failure>(
        self,
        resolve: &mut impl FnMut(&Self) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<EmitterClientSpec, Failure> {
        let config = resolve(&self)?;
        Ok(EmitterClientSpec {
            name: self.name,
            config,
        })
    }
}

/// The transports whose drivers own a connection pool sized by declared bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PooledTransport {
    Postgres,
    MySql,
    MongoDb,
    Redis,
}

/// Which pooled transport a sink's client speaks, and the bounds its pool is opened with.
///
/// The pooled sink plans decide this from the client's Model, so the shared-client registry opens
/// what it is handed and never works out which driver it is talking to from a Model of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PooledClientPlan {
    pub(super) transport: PooledTransport,
    pub(super) bounds: ClientPoolBounds,
}

/// Declares the plan of a sink that connects through one client, together with the step that binds
/// that client to the configuration the host resolved for it.
macro_rules! single_client_sink_plan {
    ($(#[$doc:meta])* $name:ident { $($field:ident: $type:ty),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq)]
        pub(super) struct $name<Config = ResolvedClientConfig> {
            pub(super) client: EmitterClientSpec<Config>,
            $(pub(super) $field: $type,)*
        }

        impl $name<DeclaredClientConfig> {
            fn resolve_clients<Failure>(
                self,
                resolve: &mut impl FnMut(
                    &EmitterClientSpec<DeclaredClientConfig>,
                ) -> Result<ResolvedClientConfig, Failure>,
            ) -> Result<$name, Failure> {
                let client = self.client.resolve(resolve)?;
                Ok($name {
                    client,
                    $($field: self.$field,)*
                })
            }
        }
    };
}

single_client_sink_plan! {
    /// An HTTP sink: the origin its client sends every request to.
    HttpSinkPlan {
        origin: HttpOrigin,
    }
}

single_client_sink_plan! {
    /// A Kafka sink: the topic it produces to, and how it learns that a record was accepted.
    KafkaSinkPlan {
        topic: nervix_models::KafkaTopicName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A Pulsar sink: the topic its producer publishes to, and how it learns that a record was
    /// accepted.
    PulsarSinkPlan {
        topic: nervix_models::PulsarTopicName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A RabbitMQ sink: the queue it publishes to, and how it learns that a record was accepted.
    RabbitMqSinkPlan {
        queue: nervix_models::RabbitMqQueueName,
        mode: BrokerPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A Redis sink: the channel it publishes to, and the bounds of the pool its client shares.
    RedisSinkPlan {
        pool: ClientPoolBounds,
        channel: nervix_models::RedisChannelName,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// An MQTT sink: the topic it publishes to, and the quality of service it publishes at.
    MqttSinkPlan {
        topic: nervix_models::MqttTopicName,
        mode: MqttPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A NATS sink: the subject it publishes to, and whether JetStream confirms each record.
    NatsSinkPlan {
        subject: nervix_models::NatsPublishSubjectName,
        mode: NatsPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A ZeroMQ sink, which pushes every record through the socket its client configures.
    ZeroMqSinkPlan {
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A Syslog sink, which sends every record to the collector its client configures.
    SyslogSinkPlan {
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// An SQS sink: the queue it sends to and whether it batches its requests.
    SqsSinkPlan {
        queue: nervix_models::SqsQueueName,
        mode: SqsPublishingMode,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A Sentry sink, which sends every record as an event to the project its client names.
    SentrySinkPlan {
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// An OpenTelemetry sink: the signal it exports and the mappings that build each item.
    OtelSinkPlan {
        signal: OtelSignal,
        mapping: MappedValuesPlan,
        values: Vec<String>,
        attributes: Vec<String>,
        resource: Vec<OtelResourceAttribute>,
        scope: Option<OtelScope>,
        batch: Option<EmitterBatchPolicy>,
    }
}

single_client_sink_plan! {
    /// A ClickHouse sink: the table it inserts into, the mapping of each column, and the most rows
    /// one insert carries.
    ClickHouseSinkPlan {
        table: nervix_models::ClickHouseTableName,
        mapping: MappedValuesPlan,
        batch: EmitterBatchPolicy,
    }
}

single_client_sink_plan! {
    /// A Postgres sink: the table it inserts into, the mapping of each column, what a conflicting
    /// row does, the most rows one insert carries, and the bounds of the pool its client shares.
    PostgresSinkPlan {
        pool: ClientPoolBounds,
        table: nervix_models::PostgresTableName,
        mapping: MappedValuesPlan,
        conflict_action: PostgresConflictAction,
        batch: EmitterBatchPolicy,
    }
}

single_client_sink_plan! {
    /// A MySQL sink: the table it inserts into, the mapping of each column, what a conflicting row
    /// does, the most rows one insert carries, and the bounds of the pool its client shares.
    MySqlSinkPlan {
        pool: ClientPoolBounds,
        table: nervix_models::MySqlTableName,
        mapping: MappedValuesPlan,
        conflict_action: MySqlConflictAction,
        batch: EmitterBatchPolicy,
    }
}

single_client_sink_plan! {
    /// A MongoDB sink: the collection it writes to, the mapping of each field, what a conflicting
    /// document does, the most documents one write carries, and the bounds of the pool its client
    /// shares.
    MongoDbSinkPlan {
        pool: ClientPoolBounds,
        collection: nervix_models::MongoDbCollectionName,
        mapping: MappedValuesPlan,
        conflict_action: MongoDbConflictAction,
        batch: EmitterBatchPolicy,
    }
}

/// The OTLP signal an emitter exports, as its connector states it. The entity of the sink's Model
/// names the signal, and a metrics entity carries the metric beside it.
fn otel_signal(sink: &OtelSink) -> OtelSignal {
    match sink {
        OtelSink::Logs(_) => OtelSignal::Logs,
        OtelSink::Traces(_) => OtelSignal::Traces,
        OtelSink::Metrics(OtelMetricsSink { metric, .. }) => OtelSignal::Metric(OtelMetric {
            name: metric.name.clone(),
            unit: metric.unit.clone(),
            description: metric.description.clone(),
            kind: otel_metric_kind(&metric.kind),
        }),
    }
}

fn otel_metric_kind(kind: &nervix_models::OtelMetricKind) -> OtelMetricKind {
    match kind {
        nervix_models::OtelMetricKind::Gauge => OtelMetricKind::Gauge,
        nervix_models::OtelMetricKind::Sum {
            monotonic,
            temporality,
        } => OtelMetricKind::Sum {
            monotonic: *monotonic,
            temporality: otel_temporality(*temporality),
        },
        nervix_models::OtelMetricKind::Histogram { temporality } => OtelMetricKind::Histogram {
            temporality: otel_temporality(*temporality),
        },
    }
}

fn otel_temporality(
    temporality: nervix_models::OtelAggregationTemporality,
) -> OtelAggregationTemporality {
    match temporality {
        nervix_models::OtelAggregationTemporality::Delta => OtelAggregationTemporality::Delta,
        nervix_models::OtelAggregationTemporality::Cumulative => {
            OtelAggregationTemporality::Cumulative
        }
    }
}

/// The instrumentation scope an emitter's records carry, as its connector states it.
fn otel_scope(scope: &nervix_models::OtelScope) -> OtelScope {
    OtelScope {
        name: scope.name.clone(),
        version: scope.version.clone(),
    }
}

/// What a MongoDB write does with a document the target collection already holds, as its connector
/// states it.
fn mongodb_conflict_action(action: &nervix_models::MongoDbConflictAction) -> MongoDbConflictAction {
    match action {
        nervix_models::MongoDbConflictAction::None => MongoDbConflictAction::None,
        nervix_models::MongoDbConflictAction::DoNothing { target } => {
            MongoDbConflictAction::DoNothing {
                target: target.clone(),
            }
        }
        nervix_models::MongoDbConflictAction::DoUpdate { target } => {
            MongoDbConflictAction::DoUpdate {
                target: target.clone(),
            }
        }
    }
}

/// What a Postgres insert does with a row the target table already holds, as its connector states
/// it.
fn postgres_conflict_action(
    action: &nervix_models::PostgresConflictAction,
) -> PostgresConflictAction {
    match action {
        nervix_models::PostgresConflictAction::None => PostgresConflictAction::None,
        nervix_models::PostgresConflictAction::DoNothing { target } => {
            PostgresConflictAction::DoNothing {
                target: target.clone(),
            }
        }
        nervix_models::PostgresConflictAction::DoUpdate { target } => {
            PostgresConflictAction::DoUpdate {
                target: target.clone(),
            }
        }
    }
}

/// What a MySQL insert does with a row the target table already holds, as its connector states it.
fn mysql_conflict_action(action: &nervix_models::MySqlConflictAction) -> MySqlConflictAction {
    match action {
        nervix_models::MySqlConflictAction::None => MySqlConflictAction::None,
        nervix_models::MySqlConflictAction::DoNothing => MySqlConflictAction::DoNothing,
        nervix_models::MySqlConflictAction::DoUpdate => MySqlConflictAction::DoUpdate,
    }
}

impl<Config> RedisSinkPlan<Config> {
    /// The shared pool this sink leases its command connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::Redis,
            bounds: self.pool,
        }
    }
}

impl<Config> PostgresSinkPlan<Config> {
    /// The shared pool this sink leases its connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::Postgres,
            bounds: self.pool,
        }
    }
}

impl<Config> MySqlSinkPlan<Config> {
    /// The shared pool this sink leases its connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::MySql,
            bounds: self.pool,
        }
    }
}

impl<Config> MongoDbSinkPlan<Config> {
    /// The shared client this sink leases, whose driver pools connections per server.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::MongoDb,
            bounds: self.pool,
        }
    }
}

/// An Iceberg sink: the object store it stages data files in, the REST catalog it commits
/// through, and the table, mappings, location and commit cadence it writes with.
///
#[derive(Debug, Clone, PartialEq)]
pub(super) struct IcebergSinkPlan<Config = ResolvedClientConfig> {
    pub(super) backend: IcebergStorageBackend,
    pub(super) storage: EmitterClientSpec<Config>,
    pub(super) catalog: EmitterClientSpec<Config>,
    pub(super) table: nervix_models::IcebergTableName,
    pub(super) mapping: MappedValuesPlan,
    pub(super) location: String,
    pub(super) commit: IcebergCommitPolicy,
    pub(super) batch: Option<EmitterBatchPolicy>,
}

impl IcebergSinkPlan<DeclaredClientConfig> {
    fn resolve_clients<Failure>(
        self,
        resolve: &mut impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<IcebergSinkPlan, Failure> {
        let storage = self.storage.resolve(resolve)?;
        let catalog = self.catalog.resolve(resolve)?;
        Ok(IcebergSinkPlan {
            backend: self.backend,
            storage,
            catalog,
            table: self.table,
            mapping: self.mapping,
            location: self.location,
            commit: self.commit,
            batch: self.batch,
        })
    }
}

/// The sink one emitter publishes to, with each connector's typed configuration.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum EmitterSinkPlan<Config = ResolvedClientConfig> {
    Client(ClientSinkPlan),
    Http(HttpSinkPlan<Config>),
    Kafka(KafkaSinkPlan<Config>),
    Pulsar(PulsarSinkPlan<Config>),
    RabbitMq(RabbitMqSinkPlan<Config>),
    Redis(RedisSinkPlan<Config>),
    Mqtt(MqttSinkPlan<Config>),
    Nats(NatsSinkPlan<Config>),
    ZeroMq(ZeroMqSinkPlan<Config>),
    Syslog(SyslogSinkPlan<Config>),
    Sqs(SqsSinkPlan<Config>),
    Sentry(SentrySinkPlan<Config>),
    Otel(OtelSinkPlan<Config>),
    ClickHouse(ClickHouseSinkPlan<Config>),
    Postgres(PostgresSinkPlan<Config>),
    MySql(MySqlSinkPlan<Config>),
    MongoDb(MongoDbSinkPlan<Config>),
    /// Boxed because the Iceberg sink carries two clients beside its table, mappings, location and
    /// commit cadence, which would otherwise set the size of every sink variant.
    Iceberg(Box<IcebergSinkPlan<Config>>),
}

impl<Config> EmitterSinkPlan<Config> {
    /// The batching clause this sink publishes under, absent when it publishes one record per
    /// message.
    pub(super) fn batch(&self) -> Option<EmitterBatchPolicy> {
        match self {
            Self::Client(plan) => Some(plan.batch),
            // Every request carries one source record.
            Self::Http(_) => None,
            Self::Kafka(plan) => plan.batch,
            Self::Pulsar(plan) => plan.batch,
            Self::RabbitMq(plan) => plan.batch,
            Self::Redis(plan) => plan.batch,
            Self::Mqtt(plan) => plan.batch,
            Self::Nats(plan) => plan.batch,
            Self::ZeroMq(plan) => plan.batch,
            Self::Syslog(plan) => plan.batch,
            Self::Sqs(plan) => plan.batch,
            Self::Sentry(plan) => plan.batch,
            Self::Otel(plan) => plan.batch,
            Self::ClickHouse(plan) => Some(plan.batch),
            Self::Postgres(plan) => Some(plan.batch),
            Self::MySql(plan) => Some(plan.batch),
            Self::MongoDb(plan) => Some(plan.batch),
            Self::Iceberg(plan) => plan.batch,
        }
    }

    /// The transport this sink publishes over, as the emitter's diagnostics name it.
    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::Client(_) => "client",
            Self::Http(_) => "http",
            Self::Kafka(_) => "kafka",
            Self::Pulsar(_) => "pulsar",
            Self::RabbitMq(_) => "rabbitmq",
            Self::Redis(_) => "redis",
            Self::Mqtt(_) => "mqtt",
            Self::Nats(_) => "nats",
            Self::ZeroMq(_) => "zeromq",
            Self::Syslog(_) => "syslog",
            Self::Sqs(_) => "sqs",
            Self::Sentry(_) => "sentry",
            Self::Otel(_) => "otel",
            Self::ClickHouse(_) => "clickhouse",
            Self::Postgres(_) => "postgres",
            Self::MySql(_) => "mysql",
            Self::MongoDb(_) => "mongodb",
            Self::Iceberg(_) => "iceberg",
        }
    }
}

impl EmitterSinkPlan<DeclaredClientConfig> {
    fn resolve_clients<Failure>(
        self,
        resolve: &mut impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<EmitterSinkPlan, Failure> {
        let resolved = match self {
            Self::Client(plan) => EmitterSinkPlan::Client(plan),
            Self::Http(plan) => EmitterSinkPlan::Http(plan.resolve_clients(resolve)?),
            Self::Kafka(plan) => EmitterSinkPlan::Kafka(plan.resolve_clients(resolve)?),
            Self::Pulsar(plan) => EmitterSinkPlan::Pulsar(plan.resolve_clients(resolve)?),
            Self::RabbitMq(plan) => EmitterSinkPlan::RabbitMq(plan.resolve_clients(resolve)?),
            Self::Redis(plan) => EmitterSinkPlan::Redis(plan.resolve_clients(resolve)?),
            Self::Mqtt(plan) => EmitterSinkPlan::Mqtt(plan.resolve_clients(resolve)?),
            Self::Nats(plan) => EmitterSinkPlan::Nats(plan.resolve_clients(resolve)?),
            Self::ZeroMq(plan) => EmitterSinkPlan::ZeroMq(plan.resolve_clients(resolve)?),
            Self::Syslog(plan) => EmitterSinkPlan::Syslog(plan.resolve_clients(resolve)?),
            Self::Sqs(plan) => EmitterSinkPlan::Sqs(plan.resolve_clients(resolve)?),
            Self::Sentry(plan) => EmitterSinkPlan::Sentry(plan.resolve_clients(resolve)?),
            Self::Otel(plan) => EmitterSinkPlan::Otel(plan.resolve_clients(resolve)?),
            Self::ClickHouse(plan) => EmitterSinkPlan::ClickHouse(plan.resolve_clients(resolve)?),
            Self::Postgres(plan) => EmitterSinkPlan::Postgres(plan.resolve_clients(resolve)?),
            Self::MySql(plan) => EmitterSinkPlan::MySql(plan.resolve_clients(resolve)?),
            Self::MongoDb(plan) => EmitterSinkPlan::MongoDb(plan.resolve_clients(resolve)?),
            Self::Iceberg(plan) => {
                EmitterSinkPlan::Iceberg(Box::new((*plan).resolve_clients(resolve)?))
            }
        };
        Ok(resolved)
    }
}

/// The client Models an emitter's sink names, as the host found them among its domain's clients.
#[derive(Debug, Clone, Copy)]
pub(super) struct EmitterClientModels<'a> {
    /// The client registered under the name the sink publishes through.
    pub(super) client: Option<&'a Model>,
    /// The client registered under the name of an Iceberg sink's REST catalog.
    pub(super) catalog_client: Option<&'a Model>,
}

/// Everything one emitter's sink is started with.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct EmitterStartPlan<Config = ResolvedClientConfig> {
    /// The backoff the emitter waits between publish retries and sink reconnects.
    pub(super) retry_policy: ParsedRetryPolicy,
    pub(super) sink: EmitterSinkPlan<Config>,
}

/// The native output contract whose deliveries are acknowledged by session consumers.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ClientSinkPlan {
    pub(super) schema: SchemaName,
    pub(super) window: AckWindow,
    pub(super) ack_timeout: Duration,
    pub(super) batch: EmitterBatchPolicy,
}

impl EmitterStartPlan<DeclaredClientConfig> {
    /// Decides the plan for `emitter` from the client Models its sink names.
    pub(super) fn decide(
        emitter: &CreateEmitter,
        clients: EmitterClientModels<'_>,
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        let sink = emitter.sink.as_ref();
        let policy = sink.publishing();
        let retry_policy = Self::decide_retry_policy(policy.retry_policy())?;
        if let EmitSink::Client(ClientSink::Consumer(ClientConsumerSink { schema, mode })) = sink {
            let nervix_models::ClientConsumerPublishingMode::Ack {
                window,
                ack_timeout,
                ..
            } = mode;
            let ack_timeout = EmitterDurationSetting::AckTimeout.parse(ack_timeout)?;
            if ack_timeout.is_zero() {
                return Err(Report::new(EmitterStartPlanError::ZeroAckTimeout));
            }
            let batch = emitter
                .batch
                .verified("the registry checked this entity's required BATCH declaration");
            if batch.max_size.bytes().get() > CLIENT_CONSUMER_SESSION_BYTES {
                return Err(Report::new(EmitterStartPlanError::ClientBatchTooLarge {
                    bytes: batch.max_size.bytes().get(),
                }));
            }
            return Ok(Self {
                retry_policy,
                sink: EmitterSinkPlan::Client(ClientSinkPlan {
                    schema: schema.clone(),
                    window: *window,
                    ack_timeout,
                    batch,
                }),
            });
        }
        let client_name = sink
            .client()
            .verified("every non-CLIENT sink names an external client");
        let Some(client) = clients.client else {
            return Err(Report::new(EmitterStartPlanError::MissingClient {
                sink: sink.transport_label(),
                client: client_name.clone(),
            }));
        };
        let sink_plan = match (sink, client) {
            (
                EmitSink::Http(HttpSink::Request(HttpRequestSink {
                    client: expected, ..
                })),
                Model::ClientHttp(client),
            ) => EmitterSinkPlan::Http(HttpSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                origin: Self::decide_http_origin(expected, &client.config)?,
            }),
            (
                EmitSink::Kafka(KafkaSink::Topic(KafkaTopicSink {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientKafka(client),
            ) => EmitterSinkPlan::Kafka(KafkaSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_kafka_publication(mode)?,
            }),
            (
                EmitSink::Pulsar(PulsarSink::Topic(PulsarTopicSink {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientPulsar(client),
            ) => EmitterSinkPlan::Pulsar(PulsarSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_pulsar_publication(mode)?,
            }),
            (
                EmitSink::RabbitMq(RabbitMqSink::Queue(RabbitMqQueueSink {
                    client: expected,
                    queue,
                    mode,
                    ..
                })),
                Model::ClientRabbitMq(client),
            ) => EmitterSinkPlan::RabbitMq(RabbitMqSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                queue: queue.clone(),
                mode: decide_rabbitmq_publication(mode)?,
            }),
            (
                EmitSink::Redis(RedisSink::PubSub(RedisPubSubSink {
                    client: expected,
                    channel,
                    ..
                })),
                Model::ClientRedis(client),
            ) => EmitterSinkPlan::Redis(RedisSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                pool: client.pool,
                channel: channel.clone(),
            }),
            (
                EmitSink::Mqtt(MqttSink::Topic(MqttTopicSink {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientMqtt(client),
            ) => EmitterSinkPlan::Mqtt(MqttSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_mqtt_publishing_mode(mode)?,
            }),
            (
                EmitSink::Nats(NatsSink::Subject(NatsSubjectSink {
                    client: expected,
                    subject,
                    mode,
                    ..
                })),
                Model::ClientNats(client),
            ) => EmitterSinkPlan::Nats(NatsSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                subject: subject.clone(),
                mode: decide_nats_publishing_mode(mode)?,
            }),
            (
                EmitSink::ZeroMq(ZeroMqSink::Push(ZeroMqPushSink {
                    client: expected, ..
                })),
                Model::ClientZeroMq(client),
            ) => EmitterSinkPlan::ZeroMq(ZeroMqSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            }),
            (
                EmitSink::Syslog(SyslogSink::Collector(SyslogCollectorSink {
                    client: expected,
                    ..
                })),
                Model::ClientSyslog(client),
            ) => EmitterSinkPlan::Syslog(SyslogSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            }),
            (
                EmitSink::Sqs(SqsSink::Queue(SqsQueueSink {
                    client: expected,
                    queue,
                    mode,
                    ..
                })),
                Model::ClientSqs(client),
            ) => EmitterSinkPlan::Sqs(SqsSinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                queue: queue.clone(),
                mode: decide_sqs_publishing_mode(mode),
            }),
            (
                EmitSink::Sentry(SentrySink::Event(SentryEventSink {
                    client: expected, ..
                })),
                Model::ClientSentry(client),
            ) => EmitterSinkPlan::Sentry(SentrySinkPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            }),
            (EmitSink::Otel(otel), Model::ClientOtel(client)) => {
                let OtelMappings {
                    values,
                    attributes,
                    resource,
                    scope,
                } = otel.mappings();
                EmitterSinkPlan::Otel(OtelSinkPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        otel.client(),
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    signal: otel_signal(otel),
                    mapping: MappedValuesPlan::decide(
                        &emitter.name,
                        "OTEL",
                        "otel",
                        &values.iter().chain(attributes).cloned().collect::<Vec<_>>(),
                    )?,
                    values: values
                        .iter()
                        .map(|mapping| mapping.column.clone())
                        .collect(),
                    attributes: attributes
                        .iter()
                        .map(|mapping| mapping.column.clone())
                        .collect(),
                    resource: otel_resource_attributes(emitter, resource)?,
                    scope: scope.as_ref().map(otel_scope),
                })
            }
            (
                EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
                    client: expected,
                    table,
                    values,
                    ..
                })),
                Model::ClientClickHouse(client),
            ) => EmitterSinkPlan::ClickHouse(ClickHouseSinkPlan {
                batch: emitter
                    .batch
                    .verified("the registry checked this entity's required BATCH declaration"),
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                table: table.clone(),
                mapping: MappedValuesPlan::decide(
                    &emitter.name,
                    "ClickHouse",
                    "clickhouse",
                    values,
                )?,
            }),
            (
                EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientPostgres(client),
            ) => EmitterSinkPlan::Postgres(PostgresSinkPlan {
                batch: emitter
                    .batch
                    .verified("the registry checked this entity's required BATCH declaration"),
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                pool: client.pool,
                table: table.clone(),
                mapping: MappedValuesPlan::decide(&emitter.name, "Postgres", "postgres", values)?,
                conflict_action: postgres_conflict_action(conflict_action),
            }),
            (
                EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientMySql(client),
            ) => EmitterSinkPlan::MySql(MySqlSinkPlan {
                batch: emitter
                    .batch
                    .verified("the registry checked this entity's required BATCH declaration"),
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                pool: client.pool,
                table: table.clone(),
                mapping: MappedValuesPlan::decide(&emitter.name, "MySQL", "mysql", values)?,
                conflict_action: mysql_conflict_action(conflict_action),
            }),
            (
                EmitSink::MongoDb(MongoDbSink::Collection(MongoDbCollectionSink {
                    client: expected,
                    collection,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientMongoDb(client),
            ) => EmitterSinkPlan::MongoDb(MongoDbSinkPlan {
                batch: emitter
                    .batch
                    .verified("the registry checked this entity's required BATCH declaration"),
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                pool: client.pool,
                collection: collection.clone(),
                mapping: MappedValuesPlan::decide(&emitter.name, "MongoDB", "mongodb", values)?,
                conflict_action: mongodb_conflict_action(conflict_action),
            }),
            (
                EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                    client: expected,
                    table,
                    values,
                    location,
                    catalog: IcebergCatalog::Rest { client: catalog },
                    commit_each,
                    max_commit_size,
                    ..
                })),
                storage,
            ) => {
                let (backend, storage) = Self::decide_iceberg_storage(sink, expected, storage)?;
                let catalog = Self::decide_iceberg_catalog(catalog, clients.catalog_client)?;
                EmitterSinkPlan::Iceberg(Box::new(IcebergSinkPlan {
                    batch: emitter.batch,
                    backend,
                    storage,
                    catalog,
                    table: table.clone(),
                    mapping: MappedValuesPlan::decide(&emitter.name, "Iceberg", "iceberg", values)?,
                    location: location.clone(),
                    commit: iceberg_commit_policy(emitter, commit_each, max_commit_size)?,
                }))
            }
            (sink, client) => {
                return Err(EmitterStartPlanError::client_kind_mismatch(sink, client));
            }
        };
        Ok(Self {
            retry_policy,
            sink: sink_plan,
        })
    }

    /// Binds every client of this plan to the configuration `resolve` returns for its
    /// declaration, in the order the sink names them: its own client first, then an Iceberg
    /// sink's catalog.
    ///
    /// The host resolves each declared mount into the paths its entries render against, so the
    /// returned plan carries the mounted paths the sink's connectors read from.
    pub(super) fn resolve_clients<Failure>(
        self,
        mut resolve: impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<EmitterStartPlan, Failure> {
        let sink = self.sink.resolve_clients(&mut resolve)?;
        Ok(EmitterStartPlan {
            retry_policy: self.retry_policy,
            sink,
        })
    }

    /// The backoff an emitter's `RETRY POLICY` declares.
    fn decide_retry_policy(
        policy: &RetryPolicy,
    ) -> Result<ParsedRetryPolicy, Report<EmitterStartPlanError>> {
        let backoff = EmitterDurationSetting::RetryBackoff.parse(&policy.backoff)?;
        let max_backoff = EmitterDurationSetting::RetryMaxBackoff.parse(&policy.max_backoff)?;
        if backoff.is_zero() {
            return Err(Report::new(EmitterStartPlanError::ZeroRetryBackoff));
        }
        if max_backoff < backoff {
            return Err(Report::new(
                EmitterStartPlanError::RetryMaxBackoffBelowBackoff,
            ));
        }
        Ok(ParsedRetryPolicy {
            backoff,
            max_backoff,
        })
    }

    /// The origin an HTTP sink's client sends every request to: its `endpoint`, which the
    /// registry accepted only as an `http` or `https` origin.
    fn decide_http_origin(
        client: &ClientName,
        config: &[ClientConfigEntry],
    ) -> Result<HttpOrigin, Report<EmitterStartPlanError>> {
        let invalid = || {
            Report::new(EmitterStartPlanError::InvalidHttpEndpoint {
                client: client.clone(),
            })
        };
        let Some(endpoint) = optional_client_config_value(config, "endpoint") else {
            return Err(invalid());
        };
        HttpOrigin::parse(endpoint).map_err(|_| invalid())
    }

    /// Resolves the configured storage client into its backend and declared configuration.
    fn decide_iceberg_storage(
        destination: &EmitSink,
        expected: &ClientName,
        storage: &Model,
    ) -> Result<
        (
            IcebergStorageBackend,
            EmitterClientSpec<DeclaredClientConfig>,
        ),
        Report<EmitterStartPlanError>,
    > {
        let (backend, client) = match storage {
            Model::ClientS3(client) => (
                IcebergStorageBackend::S3,
                EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            ),
            Model::ClientGcs(client) => (
                IcebergStorageBackend::Gcs,
                EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            ),
            Model::ClientAzureBlob(client) => (
                IcebergStorageBackend::AzureBlob,
                EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            ),
            _ => {
                return Err(EmitterStartPlanError::client_kind_mismatch(
                    destination,
                    storage,
                ));
            }
        };
        Ok((backend, client))
    }

    /// The REST catalog client an Iceberg sink commits through.
    fn decide_iceberg_catalog(
        expected: &ClientName,
        catalog_client: Option<&Model>,
    ) -> Result<EmitterClientSpec<DeclaredClientConfig>, Report<EmitterStartPlanError>> {
        let Some(catalog_client) = catalog_client else {
            return Err(Report::new(EmitterStartPlanError::MissingCatalogClient {
                client: expected.clone(),
            }));
        };
        let Model::ClientIcebergRest(client) = catalog_client else {
            return Err(Report::new(
                EmitterStartPlanError::CatalogClientKindMismatch {
                    client: expected.clone(),
                    found: EmitterStartPlanError::found_label(catalog_client),
                },
            ));
        };
        EmitterClientSpec::declared(
            expected,
            &client.name,
            client.mount.as_ref(),
            &client.config,
        )
    }
}

/// The confirmation window and timeout an acknowledging publishing mode declares.
fn decide_ack_confirmation(
    window: &AckWindow,
    ack_timeout: &str,
) -> Result<AckConfirmation, Report<EmitterStartPlanError>> {
    let timeout = EmitterDurationSetting::AckTimeout.parse(ack_timeout)?;
    if timeout.is_zero() {
        return Err(Report::new(EmitterStartPlanError::ZeroAckTimeout));
    }
    let max_in_flight = match window {
        AckWindow::Sequential => NonZeroUsize::MIN,
        AckWindow::Parallel { max } => NonZeroUsize::new(max.get().arch_into())
            .assured("the configured non-zero ACK window fits the supported target pointer width"),
    };
    Ok(AckConfirmation {
        max_in_flight,
        timeout,
    })
}

/// Converts each broker entity's closed policy into the connector's delivery plan.
macro_rules! broker_publication {
    ($function:ident, $Mode:ident) => {
        fn $function(
            mode: &nervix_models::$Mode,
        ) -> Result<BrokerPublishingMode, Report<EmitterStartPlanError>> {
            match mode {
                nervix_models::$Mode::NoAck { .. } => Ok(BrokerPublishingMode::NoAck),
                nervix_models::$Mode::Ack {
                    window,
                    ack_timeout,
                    ..
                } => Ok(BrokerPublishingMode::Ack(decide_ack_confirmation(
                    window,
                    ack_timeout,
                )?)),
            }
        }
    };
}
broker_publication!(decide_kafka_publication, KafkaTopicPublishingMode);
broker_publication!(decide_pulsar_publication, PulsarTopicPublishingMode);
broker_publication!(decide_rabbitmq_publication, RabbitMqQueuePublishingMode);

/// The quality of service `mode` declares for an MQTT `sink`.
fn decide_mqtt_publishing_mode(
    mode: &nervix_models::MqttTopicPublishingMode,
) -> Result<MqttPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        nervix_models::MqttTopicPublishingMode::Qos0 { .. } => Ok(MqttPublishingMode::Qos0),
        nervix_models::MqttTopicPublishingMode::Qos1 {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos1(confirmation))
        }
        nervix_models::MqttTopicPublishingMode::Qos2 {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos2(confirmation))
        }
    }
}

/// The NATS delivery `mode` declares for a NATS `sink`.
fn decide_nats_publishing_mode(
    mode: &nervix_models::NatsSubjectPublishingMode,
) -> Result<NatsPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        nervix_models::NatsSubjectPublishingMode::NoAck { .. } => Ok(NatsPublishingMode::Core),
        nervix_models::NatsSubjectPublishingMode::JetStream {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(NatsPublishingMode::JetStream(confirmation))
        }
    }
}

/// The request shape `mode` declares for an SQS `sink`.
fn decide_sqs_publishing_mode(mode: &nervix_models::SqsQueuePublishingMode) -> SqsPublishingMode {
    match mode {
        nervix_models::SqsQueuePublishingMode::Single { .. } => SqsPublishingMode::Single,
        nervix_models::SqsQueuePublishingMode::Batch { .. } => SqsPublishingMode::Batch,
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        AckMode, BatchMessageLimit, CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs,
        CreateClientHttp, CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb,
        CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientOtel,
        CreateClientPostgres, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
        CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
        CreateClientZeroMq, EmitterBatchLegality, ErrorPolicies, FlushPolicy, OtelLogsSink,
        ProcessorInputs, RouteConstruction,
    };
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;

    fn expression(raw: &str) -> Expression {
        nervix_nspl::parse_expression(raw).expect("valid semantic expression")
    }

    fn value_mapping(column: &str, raw: &str) -> ClickHouseValueMapping {
        ClickHouseValueMapping {
            column: column.to_string(),
            expression: expression(raw),
        }
    }

    fn named<T>(value: &str) -> T
    where
        T: TryFrom<String>,
        <T as TryFrom<String>>::Error: std::fmt::Debug,
    {
        T::try_from(value.to_string()).expect("fixture name must be valid")
    }

    fn retry_policy(backoff: &str, max_backoff: &str) -> RetryPolicy {
        RetryPolicy {
            backoff: backoff.to_string(),
            max_backoff: max_backoff.to_string(),
        }
    }

    /// Pool bounds for the pooled client fixtures, whose subject is the plan rather than the
    /// declared capacity.
    fn pool_bounds() -> ClientPoolBounds {
        ClientPoolBounds::new(1, nonzero!(4u32)).assured("one does not exceed four")
    }

    fn client_mount() -> Option<ClientResourceMount> {
        Some(ClientResourceMount {
            resource: named("certs"),
            version: 3,
        })
    }

    fn client_config() -> Vec<ClientConfigEntry> {
        vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "{{ certs }}/endpoint".to_string(),
        }]
    }

    fn http_client_config(endpoint: &str) -> Vec<ClientConfigEntry> {
        vec![
            ClientConfigEntry {
                key: "Endpoint".to_string(),
                value: endpoint.to_string(),
            },
            ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "5000".to_string(),
            },
        ]
    }

    fn batch_policy() -> EmitterBatchPolicy {
        EmitterBatchPolicy {
            max_messages: BatchMessageLimit::try_from(100u32).expect("100 is a valid limit"),
            max_size: "1MiB".parse().expect("1MiB is a valid size"),
        }
    }

    fn declared_client(name: &str) -> EmitterClientSpec<DeclaredClientConfig> {
        EmitterClientSpec {
            name: named(name),
            config: DeclaredClientConfig {
                mount: client_mount(),
                entries: client_config(),
            },
        }
    }

    fn emitter(sink: EmitSink) -> CreateEmitter {
        CreateEmitter {
            name: named("orders_out"),
            from: ProcessorInputs::single(named("orders")),
            body: nervix_models::EmitterBody::Values,
            sink: Box::new(sink),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        }
    }

    fn iceberg_sink() -> EmitSink {
        EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
            mode: nervix_models::IcebergTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "1s".to_string(),
                },
            },
            client: named("upstream"),
            table: named("orders"),
            values: vec![value_mapping("id", "input.id")],
            location: "s3://warehouse/orders".to_string(),
            catalog: IcebergCatalog::Rest {
                client: named("catalog"),
            },
            commit_each: "1s".to_string(),
            max_commit_size: "1MiB".to_string(),
        }))
    }

    fn iceberg_catalog() -> Model {
        Model::ClientIcebergRest(CreateClientIcebergRest {
            name: named("catalog"),
            mount: client_mount(),
            config: client_config(),
        })
    }

    /// One sink an emitter can declare, with the client Models that sink names.
    struct SinkCase {
        sink: EmitSink,
        client: Model,
        catalog_client: Option<Model>,
    }

    impl SinkCase {
        /// The emitter publishing to this case's sink, declaring the batching clause a database
        /// sink requires and leaving it out everywhere else.
        fn emitter(&self) -> CreateEmitter {
            let mut emitter = emitter(self.sink.clone());
            if let EmitterBatchLegality::Required { .. } = self.sink.batch_legality() {
                emitter.batch = Some(batch_policy());
            }
            emitter
        }

        fn decide(
            &self,
        ) -> Result<EmitterStartPlan<DeclaredClientConfig>, Report<EmitterStartPlanError>> {
            EmitterStartPlan::decide(
                &self.emitter(),
                EmitterClientModels {
                    client: Some(&self.client),
                    catalog_client: self.catalog_client.as_ref(),
                },
            )
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum SinkKind {
        Http,
        Kafka,
        Pulsar,
        RabbitMq,
        Redis,
        Mqtt,
        Nats,
        ZeroMq,
        Syslog,
        Sqs,
        Sentry,
        Otel,
        ClickHouse,
        Postgres,
        MySql,
        MongoDb,
        IcebergS3,
        IcebergGcs,
        IcebergAzureBlob,
    }

    impl SinkKind {
        fn label(self) -> &'static str {
            match self {
                Self::Http => "http",
                Self::Kafka => "kafka",
                Self::Pulsar => "pulsar",
                Self::RabbitMq => "rabbitmq",
                Self::Redis => "redis",
                Self::Mqtt => "mqtt",
                Self::Nats => "nats",
                Self::ZeroMq => "zeromq",
                Self::Syslog => "syslog",
                Self::Sqs => "sqs",
                Self::Sentry => "sentry",
                Self::Otel => "otel",
                Self::ClickHouse => "clickhouse",
                Self::Postgres => "postgres",
                Self::MySql => "mysql",
                Self::MongoDb => "mongodb",
                Self::IcebergS3 | Self::IcebergGcs | Self::IcebergAzureBlob => "iceberg",
            }
        }

        fn case(self) -> SinkCase {
            let client = || named::<ClientName>("upstream");
            match self {
                Self::Http => SinkCase {
                    sink: EmitSink::Http(HttpSink::Request(HttpRequestSink {
                        mode: nervix_models::HttpRequestPublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        method: expression("input.method"),
                        path: expression("concat('/orders/', input.id)"),
                    })),
                    client: Model::ClientHttp(CreateClientHttp {
                        name: client(),
                        mount: client_mount(),
                        config: http_client_config("https://orders.example.com:8443"),
                    }),
                    catalog_client: None,
                },
                Self::Kafka => SinkCase {
                    sink: EmitSink::Kafka(KafkaSink::Topic(KafkaTopicSink {
                        mode: nervix_models::KafkaTopicPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        topic: named("orders"),
                    })),
                    client: Model::ClientKafka(CreateClientKafka {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Pulsar => SinkCase {
                    sink: EmitSink::Pulsar(PulsarSink::Topic(PulsarTopicSink {
                        mode: nervix_models::PulsarTopicPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        topic: named("orders"),
                    })),
                    client: Model::ClientPulsar(CreateClientPulsar {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::RabbitMq => SinkCase {
                    sink: EmitSink::RabbitMq(RabbitMqSink::Queue(RabbitMqQueueSink {
                        mode: nervix_models::RabbitMqQueuePublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        queue: named("orders"),
                    })),
                    client: Model::ClientRabbitMq(CreateClientRabbitMq {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Redis => SinkCase {
                    sink: EmitSink::Redis(RedisSink::PubSub(RedisPubSubSink {
                        mode: nervix_models::RedisPubSubPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        channel: named("orders"),
                    })),
                    client: Model::ClientRedis(CreateClientRedis {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Mqtt => SinkCase {
                    sink: EmitSink::Mqtt(MqttSink::Topic(MqttTopicSink {
                        mode: nervix_models::MqttTopicPublishingMode::Qos0 {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        topic: named("orders"),
                    })),
                    client: Model::ClientMqtt(CreateClientMqtt {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Nats => SinkCase {
                    sink: EmitSink::Nats(NatsSink::Subject(NatsSubjectSink {
                        mode: nervix_models::NatsSubjectPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        subject: named("orders"),
                    })),
                    client: Model::ClientNats(CreateClientNats {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::ZeroMq => SinkCase {
                    sink: EmitSink::ZeroMq(ZeroMqSink::Push(ZeroMqPushSink {
                        mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                    })),
                    client: Model::ClientZeroMq(CreateClientZeroMq {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Syslog => SinkCase {
                    sink: EmitSink::Syslog(SyslogSink::Collector(SyslogCollectorSink {
                        mode: nervix_models::SyslogCollectorPublishingMode::NoAck {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                    })),
                    client: Model::ClientSyslog(CreateClientSyslog {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Sqs => SinkCase {
                    sink: EmitSink::Sqs(SqsSink::Queue(SqsQueueSink {
                        mode: nervix_models::SqsQueuePublishingMode::Batch {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        queue: nervix_models::SqsQueueName::parse("orders").expect("valid queue"),
                        fifo_group: None,
                    })),
                    client: Model::ClientSqs(CreateClientSqs {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Sentry => SinkCase {
                    sink: EmitSink::Sentry(SentrySink::Event(SentryEventSink {
                        mode: nervix_models::SentryEventPublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                    })),
                    client: Model::ClientSentry(CreateClientSentry {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Otel => SinkCase {
                    sink: EmitSink::from(OtelLogsSink {
                        mode: nervix_models::OtelLogsPublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        mappings: OtelMappings {
                            values: vec![value_mapping("id", "input.id")],
                            attributes: Vec::new(),
                            resource: Vec::new(),
                            scope: None,
                        },
                    }),
                    client: Model::ClientOtel(CreateClientOtel {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::ClickHouse => SinkCase {
                    sink: EmitSink::ClickHouse(ClickHouseSink::Table(ClickHouseTableSink {
                        mode: nervix_models::ClickHouseTablePublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        table: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                    })),
                    client: Model::ClientClickHouse(CreateClientClickHouse {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Postgres => SinkCase {
                    sink: EmitSink::Postgres(PostgresSink::Table(PostgresTableSink {
                        mode: nervix_models::PostgresTablePublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        table: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                        conflict_action: nervix_models::PostgresConflictAction::None,
                    })),
                    client: Model::ClientPostgres(CreateClientPostgres {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::MySql => SinkCase {
                    sink: EmitSink::MySql(MySqlSink::Table(MySqlTableSink {
                        mode: nervix_models::MySqlTablePublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        table: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                        conflict_action: nervix_models::MySqlConflictAction::None,
                    })),
                    client: Model::ClientMySql(CreateClientMySql {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::MongoDb => SinkCase {
                    sink: EmitSink::MongoDb(MongoDbSink::Collection(MongoDbCollectionSink {
                        mode: nervix_models::MongoDbCollectionPublishingMode::Ack {
                            retry_policy: nervix_models::RetryPolicy {
                                backoff: "100ms".to_string(),
                                max_backoff: "1s".to_string(),
                            },
                        },
                        client: client(),
                        collection: named("orders"),
                        values: vec![value_mapping("id", "input.id")],
                        conflict_action: nervix_models::MongoDbConflictAction::None,
                    })),
                    client: Model::ClientMongoDb(CreateClientMongoDb {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::IcebergS3 => SinkCase {
                    sink: iceberg_sink(),
                    client: Model::ClientS3(CreateClientS3 {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergGcs => SinkCase {
                    sink: iceberg_sink(),
                    client: Model::ClientGcs(CreateClientGcs {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergAzureBlob => SinkCase {
                    sink: iceberg_sink(),
                    client: Model::ClientAzureBlob(CreateClientAzureBlob {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
            }
        }
    }

    #[rstest]
    #[case::http(SinkKind::Http)]
    #[case::kafka(SinkKind::Kafka)]
    #[case::pulsar(SinkKind::Pulsar)]
    #[case::rabbitmq(SinkKind::RabbitMq)]
    #[case::redis(SinkKind::Redis)]
    #[case::mqtt(SinkKind::Mqtt)]
    #[case::nats(SinkKind::Nats)]
    #[case::zeromq(SinkKind::ZeroMq)]
    #[case::syslog(SinkKind::Syslog)]
    #[case::sqs(SinkKind::Sqs)]
    #[case::sentry(SinkKind::Sentry)]
    #[case::otel(SinkKind::Otel)]
    #[case::clickhouse(SinkKind::ClickHouse)]
    #[case::postgres(SinkKind::Postgres)]
    #[case::mysql(SinkKind::MySql)]
    #[case::mongodb(SinkKind::MongoDb)]
    #[case::iceberg_s3(SinkKind::IcebergS3)]
    #[case::iceberg_gcs(SinkKind::IcebergGcs)]
    #[case::iceberg_azure_blob(SinkKind::IcebergAzureBlob)]
    fn decides_each_sink_from_its_client_model(#[case] kind: SinkKind) {
        let plan = kind
            .case()
            .decide()
            .expect("a sink with a matching client must be planned");

        assert_eq!(plan.sink.label(), kind.label());
        assert_eq!(
            plan.retry_policy,
            ParsedRetryPolicy {
                backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
            }
        );
    }

    #[test]
    fn every_row_sink_carries_its_lowered_mapping_into_the_start_plan() {
        for kind in [
            SinkKind::ClickHouse,
            SinkKind::Postgres,
            SinkKind::MySql,
            SinkKind::MongoDb,
            SinkKind::IcebergS3,
        ] {
            let plan = kind
                .case()
                .decide()
                .expect("the row sink should be planned");
            let mapping = match &plan.sink {
                EmitterSinkPlan::ClickHouse(sink) => &sink.mapping,
                EmitterSinkPlan::Postgres(sink) => &sink.mapping,
                EmitterSinkPlan::MySql(sink) => &sink.mapping,
                EmitterSinkPlan::MongoDb(sink) => &sink.mapping,
                EmitterSinkPlan::Iceberg(sink) => &sink.mapping,
                _ => panic!("the case must be a row sink"),
            };
            assert_eq!(mapping.columns, ["id"], "{}", kind.label());
            assert_eq!(mapping.program.inner.set.len(), 1, "{}", kind.label());
            assert_eq!(
                mapping.program.inner.set[0].0.field,
                "c0",
                "{}",
                kind.label()
            );
        }
    }

    #[test]
    fn otel_plan_combines_values_and_attributes_in_declared_order() {
        let mut case = SinkKind::Otel.case();
        let EmitSink::Otel(OtelSink::Logs(OtelLogsSink { mappings, .. })) = &mut case.sink else {
            panic!("the case must be OTEL logs");
        };
        mappings
            .attributes
            .push(value_mapping("customer", "input.customer"));
        mappings
            .resource
            .push(value_mapping("service.name", "'orders'"));
        let plan = case.decide().expect("OTEL mappings should be planned");
        let EmitterSinkPlan::Otel(sink) = plan.sink else {
            panic!("the plan must be OTEL");
        };
        assert_eq!(sink.mapping.columns, ["id", "customer"]);
        assert_eq!(sink.values, ["id"]);
        assert_eq!(sink.attributes, ["customer"]);
        assert_eq!(
            sink.resource,
            [OtelResourceAttribute {
                key: "service.name".to_string(),
                value: OtelLiteral::String("orders".to_string()),
            }]
        );
    }

    #[test]
    fn otel_resource_requires_a_literal_during_planning() {
        let mut case = SinkKind::Otel.case();
        let EmitSink::Otel(OtelSink::Logs(OtelLogsSink { mappings, .. })) = &mut case.sink else {
            panic!("the case must be OTEL logs");
        };
        mappings
            .resource
            .push(value_mapping("service.name", "input.customer"));
        let error = case
            .decide()
            .expect_err("resource attributes must be fixed values");
        assert_eq!(
            *error.current_context(),
            EmitterStartPlanError::InvalidOtelResource {
                kind: case.sink.kind(),
                emitter: named("orders_out"),
                attribute: "service.name".to_string(),
            }
        );
    }

    /// Duration text `humantime` panicked on instead of refusing: spans that add up to the last
    /// second a duration holds, and fractions of exactly one more second.
    const TOO_LONG_DURATION_TEXT: &str = "18446744073709551615s 1000000000ns";

    #[test]
    fn iceberg_commit_cadence_keeps_why_its_text_names_no_duration() {
        for (commit_each, why) in [
            ("oops", "expected number at 0"),
            (
                TOO_LONG_DURATION_TEXT,
                "it is longer than a duration can be",
            ),
        ] {
            let mut case = SinkKind::IcebergS3.case();
            let EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
                commit_each: cadence,
                ..
            })) = &mut case.sink
            else {
                panic!("the case must be Iceberg");
            };
            *cadence = commit_each.to_string();
            let error = case.decide().expect_err("the commit cadence must be valid");
            assert_eq!(
                *error.current_context(),
                EmitterStartPlanError::InvalidIcebergCommit {
                    emitter: named("orders_out"),
                    setting: IcebergCommitSetting::CommitEach,
                    value: commit_each.to_string(),
                }
            );
            assert_eq!(
                error
                    .downcast_ref::<nervix_models::DurationTextError>()
                    .map(ToString::to_string),
                Some(why.to_string())
            );
        }
    }

    #[test]
    fn iceberg_commit_limits_are_parsed_during_planning() {
        let mut case = SinkKind::IcebergS3.case();
        let EmitSink::Iceberg(IcebergSink::Table(IcebergTableSink {
            max_commit_size, ..
        })) = &mut case.sink
        else {
            panic!("the case must be Iceberg");
        };
        *max_commit_size = "invalid".to_string();
        let error = case.decide().expect_err("the commit size must be valid");
        assert_eq!(
            *error.current_context(),
            EmitterStartPlanError::InvalidIcebergCommit {
                emitter: named("orders_out"),
                setting: IcebergCommitSetting::MaxCommitSize,
                value: "invalid".to_string(),
            }
        );
    }

    #[test]
    fn carries_the_batch_clause_into_the_plan_of_every_sink() {
        for kind in [
            SinkKind::Kafka,
            SinkKind::Sentry,
            SinkKind::Otel,
            SinkKind::IcebergS3,
        ] {
            let case = kind.case();
            let unbatched = case.decide().expect("the sink must be planned");
            assert_eq!(unbatched.sink.batch(), None, "{}", kind.label());

            let mut emitter = case.emitter();
            emitter.batch = Some(batch_policy());
            let batched = EmitterStartPlan::decide(
                &emitter,
                EmitterClientModels {
                    client: Some(&case.client),
                    catalog_client: case.catalog_client.as_ref(),
                },
            )
            .expect("the batching sink must be planned");
            assert_eq!(
                batched.sink.batch(),
                Some(batch_policy()),
                "{}",
                kind.label()
            );
        }

        for kind in [
            SinkKind::ClickHouse,
            SinkKind::Postgres,
            SinkKind::MySql,
            SinkKind::MongoDb,
        ] {
            let plan = kind
                .case()
                .decide()
                .expect("the database sink must be planned");
            assert_eq!(plan.sink.batch(), Some(batch_policy()), "{}", kind.label());
        }
    }

    #[test]
    fn plans_an_http_sink_with_its_client_origin() {
        let case = SinkKind::Http.case();
        let emitter = case.emitter();

        let plan = EmitterStartPlan::decide(
            &emitter,
            EmitterClientModels {
                client: Some(&case.client),
                catalog_client: None,
            },
        )
        .expect("an HTTP sink with an HTTP client must be planned");

        assert_eq!(plan.sink.batch(), None);
        let EmitterSinkPlan::Http(sink) = plan.sink else {
            panic!("an HTTP sink must be planned as HTTP");
        };
        assert_eq!(
            sink.origin,
            HttpOrigin::parse("https://orders.example.com:8443")
                .expect("the fixture endpoint is an origin")
        );
        assert_eq!(sink.client.name, named::<ClientName>("upstream"));
    }

    #[test]
    fn an_http_client_without_an_origin_endpoint_is_not_planned() {
        for config in [
            Vec::new(),
            http_client_config("https://orders.example.com/v1"),
        ] {
            let mut case = SinkKind::Http.case();
            case.client = Model::ClientHttp(CreateClientHttp {
                name: named("upstream"),
                mount: None,
                config,
            });

            let error = case
                .decide()
                .expect_err("an HTTP sink needs an origin endpoint");

            assert_eq!(
                *error.current_context(),
                EmitterStartPlanError::InvalidHttpEndpoint {
                    client: named("upstream"),
                }
            );
        }
    }

    #[test]
    fn a_database_emitter_requires_the_batch_its_entity_declares() {
        let case = SinkKind::MongoDb.case();
        let mut emitter = case.emitter();
        emitter.batch = None;
        let error = emitter
            .validate_batch()
            .expect_err("this entity requires a batch");
        assert_eq!(
            error.current_context(),
            &nervix_models::EmitterBatchContractError::Required {
                sink: emitter.sink.kind().phrase()
            },
        );
    }

    #[test]
    fn carries_the_declared_client_and_the_sink_parameters() {
        let plan = SinkKind::Postgres
            .case()
            .decide()
            .expect("a Postgres sink with a Postgres client must be planned");

        let EmitterSinkPlan::Postgres(sink) = plan.sink else {
            panic!("a Postgres sink must be planned as Postgres");
        };
        assert_eq!(sink.client, declared_client("upstream"));
        assert_eq!(
            sink.table,
            named::<nervix_models::PostgresTableName>("orders")
        );
        assert_eq!(sink.conflict_action, PostgresConflictAction::None);
        assert_eq!(sink.batch, batch_policy());
        assert_eq!(
            sink.pooled_client(),
            PooledClientPlan {
                transport: PooledTransport::Postgres,
                bounds: pool_bounds(),
            }
        );
    }

    #[test]
    fn plans_an_iceberg_sink_with_its_storage_and_catalog_clients() {
        let plan = SinkKind::IcebergGcs
            .case()
            .decide()
            .expect("an Iceberg sink with GCS storage and a REST catalog must be planned");

        let EmitterSinkPlan::Iceberg(sink) = plan.sink else {
            panic!("an Iceberg sink must be planned as Iceberg");
        };
        assert_eq!(sink.backend, IcebergStorageBackend::Gcs);
        assert_eq!(sink.storage, declared_client("upstream"));
        assert_eq!(sink.catalog, declared_client("catalog"));
        assert_eq!(sink.location, "s3://warehouse/orders");
        assert_eq!(sink.commit.interval, Duration::from_secs(1));
        assert_eq!(sink.commit.max_size, 1_048_576);
    }

    #[test]
    fn decides_a_broker_confirmation_window_timeout_and_retry_policy() {
        let mut case = SinkKind::Kafka.case();
        let EmitSink::Kafka(KafkaSink::Topic(destination)) = &mut case.sink else {
            panic!("expected Kafka destination");
        };
        destination.mode = nervix_models::KafkaTopicPublishingMode::Ack {
            window: AckWindow::Parallel {
                max: nonzero!(17u64),
            },
            ack_timeout: "3s".to_string(),
            retry_policy: retry_policy("25ms", "2s"),
        };

        let plan = case
            .decide()
            .expect("a Kafka sink accepts an acknowledged broker mode");

        assert_eq!(
            plan.retry_policy,
            ParsedRetryPolicy {
                backoff: Duration::from_millis(25),
                max_backoff: Duration::from_secs(2),
            }
        );
        let EmitterSinkPlan::Kafka(sink) = plan.sink else {
            panic!("a Kafka sink must be planned as Kafka");
        };
        assert_eq!(
            sink.mode,
            BrokerPublishingMode::Ack(AckConfirmation {
                max_in_flight: nonzero!(17usize),
                timeout: Duration::from_secs(3),
            })
        );
    }

    #[test]
    fn decides_transport_specific_mqtt_nats_and_sqs_modes() {
        let mut mqtt = SinkKind::Mqtt.case();
        let EmitSink::Mqtt(MqttSink::Topic(destination)) = &mut mqtt.sink else {
            panic!("expected Mqtt destination");
        };
        destination.mode = nervix_models::MqttTopicPublishingMode::Qos2 {
            window: AckWindow::Sequential,
            ack_timeout: "7s".to_string(),
            retry_policy: retry_policy("10ms", "1s"),
        };
        let EmitterSinkPlan::Mqtt(mqtt) = mqtt.decide().expect("valid MQTT mode").sink else {
            panic!("an MQTT sink must be planned as MQTT");
        };
        assert_eq!(
            mqtt.mode,
            MqttPublishingMode::Qos2(AckConfirmation {
                max_in_flight: nonzero!(1usize),
                timeout: Duration::from_secs(7),
            })
        );

        let mut jetstream = SinkKind::Nats.case();
        let EmitSink::Nats(NatsSink::Subject(destination)) = &mut jetstream.sink else {
            panic!("expected Nats destination");
        };
        destination.mode = nervix_models::NatsSubjectPublishingMode::JetStream {
            window: AckWindow::Parallel {
                max: nonzero!(23u64),
            },
            ack_timeout: "11s".to_string(),
            retry_policy: retry_policy("10ms", "1s"),
        };
        let EmitterSinkPlan::Nats(jetstream) =
            jetstream.decide().expect("valid JetStream mode").sink
        else {
            panic!("a NATS sink must be planned as NATS");
        };
        assert_eq!(
            jetstream.mode,
            NatsPublishingMode::JetStream(AckConfirmation {
                max_in_flight: nonzero!(23usize),
                timeout: Duration::from_secs(11),
            })
        );

        let EmitterSinkPlan::Nats(core) = SinkKind::Nats
            .case()
            .decide()
            .expect("a NATS sink accepts NO_ACK")
            .sink
        else {
            panic!("a NATS sink must be planned as NATS");
        };
        assert_eq!(core.mode, NatsPublishingMode::Core);

        let EmitterSinkPlan::Sqs(sqs) = SinkKind::Sqs
            .case()
            .decide()
            .expect("an SQS sink accepts BATCH")
            .sink
        else {
            panic!("an SQS sink must be planned as SQS");
        };
        assert_eq!(sqs.mode, SqsPublishingMode::Batch);
    }

    #[rstest]
    #[case::inverted(
        retry_policy("2s", "25ms"),
        EmitterStartPlanError::RetryMaxBackoffBelowBackoff
    )]
    #[case::zero_backoff(retry_policy("0s", "1s"), EmitterStartPlanError::ZeroRetryBackoff)]
    #[case::unparseable(
        retry_policy("oops", "1s"),
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::RetryBackoff,
            value: "oops".to_string(),
        }
    )]
    #[case::too_long(
        retry_policy("1s", TOO_LONG_DURATION_TEXT),
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::RetryMaxBackoff,
            value: TOO_LONG_DURATION_TEXT.to_string(),
        }
    )]
    fn rejects_an_unusable_retry_policy(
        #[case] policy: RetryPolicy,
        #[case] expected: EmitterStartPlanError,
    ) {
        let mut case = SinkKind::Kafka.case();
        let EmitSink::Kafka(KafkaSink::Topic(destination)) = &mut case.sink else {
            panic!("expected Kafka destination");
        };
        destination.mode = nervix_models::KafkaTopicPublishingMode::NoAck {
            retry_policy: policy,
        };

        let error = case
            .decide()
            .expect_err("an unusable retry policy must not be planned");

        assert_eq!(error.current_context(), &expected);
    }

    #[rstest]
    #[case::zero("0s", EmitterStartPlanError::ZeroAckTimeout)]
    #[case::unparseable(
        "oops",
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::AckTimeout,
            value: "oops".to_string(),
        }
    )]
    #[case::too_long(
        TOO_LONG_DURATION_TEXT,
        EmitterStartPlanError::InvalidDuration {
            setting: EmitterDurationSetting::AckTimeout,
            value: TOO_LONG_DURATION_TEXT.to_string(),
        }
    )]
    fn rejects_an_unusable_ack_timeout(
        #[case] ack_timeout: &str,
        #[case] expected: EmitterStartPlanError,
    ) {
        let mut case = SinkKind::RabbitMq.case();
        let EmitSink::RabbitMq(RabbitMqSink::Queue(destination)) = &mut case.sink else {
            panic!("expected Kafka destination");
        };
        destination.mode = nervix_models::RabbitMqQueuePublishingMode::Ack {
            window: AckWindow::Sequential,
            ack_timeout: ack_timeout.to_string(),
            retry_policy: retry_policy("25ms", "2s"),
        };

        let error = case
            .decide()
            .expect_err("an unusable ack timeout must not be planned");

        assert_eq!(error.current_context(), &expected);
    }

    #[test]
    fn reports_a_missing_client() {
        let case = SinkKind::Kafka.case();

        let error = EmitterStartPlan::decide(
            &case.emitter(),
            EmitterClientModels {
                client: None,
                catalog_client: None,
            },
        )
        .expect_err("a sink without its client must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::MissingClient {
                sink: "KAFKA TOPIC",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_a_client_of_another_kind() {
        let mut case = SinkKind::Kafka.case();
        case.client = SinkKind::Pulsar.case().client;

        let error = case
            .decide()
            .expect_err("a Kafka sink must not be planned over a Pulsar client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientKindMismatch {
                sink: "KAFKA TOPIC",
                expected: nervix_models::SinkReferenceKind::Client(
                    nervix_models::ConnectorKind::Kafka
                ),
                found: "PULSAR",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_an_iceberg_storage_client_of_another_kind() {
        let mut case = SinkKind::IcebergS3.case();
        case.client = SinkKind::Kafka.case().client;

        let error = case
            .decide()
            .expect_err("an Iceberg destination needs an object-store client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientKindMismatch {
                sink: "ICEBERG TABLE",
                expected: nervix_models::SinkReferenceKind::StorageClient(&[
                    nervix_models::ConnectorKind::S3,
                    nervix_models::ConnectorKind::Gcs,
                    nervix_models::ConnectorKind::AzureBlob
                ]),
                found: "KAFKA",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_a_missing_iceberg_catalog_client() {
        let mut case = SinkKind::IcebergS3.case();
        case.catalog_client = None;

        let error = case
            .decide()
            .expect_err("an Iceberg sink without its catalog must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::MissingCatalogClient {
                client: named("catalog"),
            }
        );
    }

    #[test]
    fn reports_an_iceberg_catalog_client_of_another_kind() {
        let mut case = SinkKind::IcebergS3.case();
        case.catalog_client = Some(SinkKind::IcebergS3.case().client);

        let error = case
            .decide()
            .expect_err("an Iceberg catalog must be an ICEBERG_REST client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::CatalogClientKindMismatch {
                client: named("catalog"),
                found: "S3",
            }
        );
    }

    #[test]
    fn reports_a_client_model_registered_under_another_name() {
        let mut case = SinkKind::Sentry.case();
        case.client = Model::ClientSentry(CreateClientSentry {
            name: named("different"),
            mount: None,
            config: Vec::new(),
        });

        let error = case
            .decide()
            .expect_err("a differently named client must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientIdentityMismatch {
                expected: named("upstream"),
                found: named("different"),
            }
        );
    }

    #[test]
    fn binds_every_client_to_its_resolved_configuration_in_sink_order() {
        let decided = SinkKind::IcebergAzureBlob
            .case()
            .decide()
            .expect("an Iceberg sink with Azure Blob storage and a REST catalog must be planned");
        let mut resolved_names = Vec::new();

        let plan = decided
            .resolve_clients(|client| {
                resolved_names.push(client.name.clone());
                let mut entries = client.config.entries.clone();
                entries.push(ClientConfigEntry {
                    key: "resolved".to_string(),
                    value: client.name.as_str().to_string(),
                });
                Ok::<_, std::convert::Infallible>(ResolvedClientConfig {
                    entries,
                    mounts: None,
                })
            })
            .expect("an infallible resolution must bind every client");

        assert_eq!(
            resolved_names,
            vec![named::<ClientName>("upstream"), named("catalog")]
        );
        let EmitterSinkPlan::Iceberg(sink) = plan.sink else {
            panic!("resolution must keep the Iceberg sink");
        };
        assert_eq!(sink.storage.name, named::<ClientName>("upstream"));
        assert_eq!(sink.storage.config.entries[1].value, "upstream");
        assert_eq!(sink.catalog.name, named::<ClientName>("catalog"));
        assert_eq!(sink.catalog.config.entries[1].value, "catalog");
    }

    #[test]
    fn a_failed_resolution_fails_the_plan() {
        let decided = SinkKind::Kafka
            .case()
            .decide()
            .expect("a Kafka sink with a Kafka client must be planned");

        let failure = decided
            .resolve_clients(|client| Err(client.name.clone()))
            .expect_err("a failed resolution must not yield a plan");

        assert_eq!(failure, named::<ClientName>("upstream"));
    }
}
