//! Decides the complete specification for starting one emitter's emitter.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an emitter and the client Models its emitter names into one typed start
//!   plan: each connector's clients, emitter parameters and publishing mode, lowered row mappings,
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
//! task and its emitter constructors receive. Neither of them sees a Model.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

mod entities;
use std::{num::NonZeroUsize, time::Duration};

use arch_into::ArchInto as _;
pub(super) use entities::*;
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
    OtelScope,
};
use nervix_connector_postgres::PostgresConflictAction;
use nervix_connector_sqs::SqsPublishingMode;
use nervix_models::{
    AckWindow, Assignment, AssignmentTarget, ClickHouseEmitter, ClickHouseTableEmitter,
    ClickHouseValueMapping, ClientConfigEntry, ClientConsumerEmitter, ClientEmitter, ClientName,
    ClientPoolBounds, ClientResourceMount, CreateEmitter, EmitterBatchPolicy, EmitterEntity,
    Expression, FieldName, HttpEmitter, HttpOrigin, HttpRequestEmitter, IcebergCatalog,
    IcebergEmitter, IcebergStorageBackend, IcebergTableEmitter, KafkaEmitter, KafkaTopicEmitter,
    Literal, Model, MongoDbCollectionEmitter, MongoDbEmitter, MqttEmitter, MqttTopicEmitter,
    MySqlEmitter, MySqlTableEmitter, NatsEmitter, NatsSubjectEmitter, OtelEmitter, OtelMappings,
    OtelMetricsEmitter, PostgresEmitter, PostgresTableEmitter, PulsarEmitter, PulsarTopicEmitter,
    RabbitMqEmitter, RabbitMqQueueEmitter, RedisEmitter, RedisPubSubEmitter, RetryPolicy,
    RouteConstruction, SchemaName, SentryEmitter, SentryEventEmitter, SqsEmitter, SqsQueueEmitter,
    SyslogCollectorEmitter, SyslogEmitter, ZeroMqEmitter, ZeroMqPushEmitter, parse_duration_text,
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

/// Why an emitter's emitter cannot be planned from its Model and the client Models it names.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum EmitterStartPlanError {
    #[error("{connector} emitter client '{client}' does not exist")]
    MissingClient {
        connector: &'static str,
        client: ClientName,
    },
    #[error("{connector} emitter requires a {expected} client, found {found} '{client}'")]
    ClientKindMismatch {
        connector: &'static str,
        expected: nervix_models::EmitterReferenceKind,
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
    #[error("{} emitter client '{client}' declares no http or https origin endpoint", <HttpRequestEmitter as nervix_models::EmitterEntityModel>::KIND)]
    InvalidHttpEndpoint { client: ClientName },
    #[error("{connector} emitter '{emitter}' requires at least one VALUES mapping")]
    EmptyValues {
        connector: &'static str,
        emitter: nervix_models::EmitterName,
    },
    #[error("{connector} emitter '{emitter}' has invalid VALUES mappings")]
    InvalidValues {
        connector: &'static str,
        emitter: nervix_models::EmitterName,
    },
    #[error("{kind} emitter '{emitter}' has a nonliteral RESOURCE attribute '{attribute}'")]
    InvalidOtelResource {
        kind: nervix_models::EmitterEntityKind,
        emitter: nervix_models::EmitterName,
        attribute: String,
    },
    #[error("{} emitter '{emitter}' has invalid {setting} '{value}'", <IcebergTableEmitter as nervix_models::EmitterEntityModel>::KIND)]
    InvalidIcebergCommit {
        emitter: nervix_models::EmitterName,
        setting: IcebergCommitSetting,
        value: String,
    },
}

/// A row emitter's ordered external columns and its already lowered expression program.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct MappedValuesPlan {
    pub(super) columns: Vec<String>,
    pub(super) program: SpannedNode<Program>,
}

impl MappedValuesPlan {
    pub(crate) fn decide(
        emitter: &nervix_models::EmitterName,
        connector: &'static str,
        namespace: &'static str,
        mappings: &[ClickHouseValueMapping],
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        if mappings.is_empty() {
            return Err(Report::new(EmitterStartPlanError::EmptyValues {
                connector,
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
            connector,
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
                    kind: emitter.connector.kind(),
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
    max_commit_size: &nervix_models::PayloadSizeLimit,
) -> Result<IcebergCommitPolicy, Report<EmitterStartPlanError>> {
    let interval = parse_duration_text(commit_each).change_context_lazy(|| {
        EmitterStartPlanError::InvalidIcebergCommit {
            emitter: emitter.name.clone(),
            setting: IcebergCommitSetting::CommitEach,
            value: commit_each.to_string(),
        }
    })?;
    let max_size = max_commit_size.bytes().get();
    Ok(IcebergCommitPolicy { interval, max_size })
}

impl EmitterStartPlanError {
    /// The report for an emitter whose client name resolved to a Model of another kind.
    fn client_kind_mismatch(connector: &EmitterEntity, client: &Model) -> Report<Self> {
        Report::new(Self::ClientKindMismatch {
            connector: connector.transport_label(),
            expected: connector.capabilities().reference,
            found: Self::found_label(client),
            client: connector
                .client()
                .verified("only external emitters resolve client Models")
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

/// One client an emitter connects through.
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
    /// The client an emitter references by `expected`, as the Model registered under that name
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

/// Which pooled transport an emitter's client speaks, and the bounds its pool is opened with.
///
/// The pooled emitter plans decide this from the client's Model, so the shared-client registry opens
/// what it is handed and never works out which driver it is talking to from a Model of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PooledClientPlan {
    pub(super) transport: PooledTransport,
    pub(super) bounds: ClientPoolBounds,
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

impl<Config> RedisPubSubEmitterPlan<Config> {
    /// The shared pool this emitter leases its command connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::Redis,
            bounds: self.pool,
        }
    }
}

impl<Config> PostgresTableEmitterPlan<Config> {
    /// The shared pool this emitter leases its connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::Postgres,
            bounds: self.pool,
        }
    }
}

impl<Config> MySqlTableEmitterPlan<Config> {
    /// The shared pool this emitter leases its connections from.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::MySql,
            bounds: self.pool,
        }
    }
}

impl<Config> MongoDbCollectionEmitterPlan<Config> {
    /// The shared client this emitter leases, whose driver pools connections per server.
    pub(super) fn pooled_client(&self) -> PooledClientPlan {
        PooledClientPlan {
            transport: PooledTransport::MongoDb,
            bounds: self.pool,
        }
    }
}

/// An Iceberg emitter: the object store it stages data files in, the REST catalog it commits
/// through, and the table, mappings, location and commit cadence it writes with.
///
#[derive(Debug, Clone, PartialEq)]
pub(super) struct IcebergTableEmitterPlan<Config = ResolvedClientConfig> {
    pub(super) backend: IcebergStorageBackend,
    pub(super) storage: EmitterClientSpec<Config>,
    pub(super) catalog: EmitterClientSpec<Config>,
    pub(super) table: nervix_models::IcebergTableName,
    pub(super) namespace: nervix_models::IcebergNamespace,
    pub(super) mapping: MappedValuesPlan,
    pub(super) location: String,
    pub(super) commit: IcebergCommitPolicy,
    pub(super) batch: Option<EmitterBatchPolicy>,
}

impl IcebergTableEmitterPlan<DeclaredClientConfig> {
    fn resolve_clients<Failure>(
        self,
        resolve: &mut impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<IcebergTableEmitterPlan, Failure> {
        let storage = self.storage.resolve(resolve)?;
        let catalog = self.catalog.resolve(resolve)?;
        Ok(IcebergTableEmitterPlan {
            backend: self.backend,
            storage,
            catalog,
            table: self.table,
            namespace: self.namespace,
            mapping: self.mapping,
            location: self.location,
            commit: self.commit,
            batch: self.batch,
        })
    }
}

/// The emitter one emitter publishes to, with each connector's typed configuration.
/// The consumer entity planned for Client execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ClientEmitterPlan {
    Consumer(ClientConsumerEmitterPlan),
}

/// The request entity planned for Http execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum HttpEmitterPlan<Config = ResolvedClientConfig> {
    Request(HttpRequestEmitterPlan<Config>),
}

/// The topic entity planned for Kafka execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum KafkaEmitterPlan<Config = ResolvedClientConfig> {
    Topic(KafkaTopicEmitterPlan<Config>),
}

/// The topic entity planned for Pulsar execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum PulsarEmitterPlan<Config = ResolvedClientConfig> {
    Topic(PulsarTopicEmitterPlan<Config>),
}

/// The queue entity planned for RabbitMq execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RabbitMqEmitterPlan<Config = ResolvedClientConfig> {
    Queue(RabbitMqQueueEmitterPlan<Config>),
}

/// The pubsub entity planned for Redis execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RedisEmitterPlan<Config = ResolvedClientConfig> {
    PubSub(RedisPubSubEmitterPlan<Config>),
}

/// The topic entity planned for Mqtt execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum MqttEmitterPlan<Config = ResolvedClientConfig> {
    Topic(MqttTopicEmitterPlan<Config>),
}

/// The subject entity planned for Nats execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum NatsEmitterPlan<Config = ResolvedClientConfig> {
    Subject(NatsSubjectEmitterPlan<Config>),
}

/// The push entity planned for ZeroMq execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ZeroMqEmitterPlan<Config = ResolvedClientConfig> {
    Push(ZeroMqPushEmitterPlan<Config>),
}

/// The collector entity planned for Syslog execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SyslogEmitterPlan<Config = ResolvedClientConfig> {
    Collector(SyslogCollectorEmitterPlan<Config>),
}

/// The queue entity planned for Sqs execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SqsEmitterPlan<Config = ResolvedClientConfig> {
    Queue(SqsQueueEmitterPlan<Config>),
}

/// The event entity planned for Sentry execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum SentryEmitterPlan<Config = ResolvedClientConfig> {
    Event(SentryEventEmitterPlan<Config>),
}

/// The table entity planned for ClickHouse execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum ClickHouseEmitterPlan<Config = ResolvedClientConfig> {
    Table(ClickHouseTableEmitterPlan<Config>),
}

/// The table entity planned for Postgres execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum PostgresEmitterPlan<Config = ResolvedClientConfig> {
    Table(PostgresTableEmitterPlan<Config>),
}

/// The table entity planned for MySql execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum MySqlEmitterPlan<Config = ResolvedClientConfig> {
    Table(MySqlTableEmitterPlan<Config>),
}

/// The collection entity planned for MongoDb execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum MongoDbEmitterPlan<Config = ResolvedClientConfig> {
    Collection(MongoDbCollectionEmitterPlan<Config>),
}

/// The table entity planned for Iceberg execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum IcebergEmitterPlan<Config = ResolvedClientConfig> {
    Table(Box<IcebergTableEmitterPlan<Config>>),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum EmitterConnectorPlan<Config = ResolvedClientConfig> {
    Client(ClientEmitterPlan),
    Http(HttpEmitterPlan<Config>),
    Kafka(KafkaEmitterPlan<Config>),
    Pulsar(PulsarEmitterPlan<Config>),
    RabbitMq(RabbitMqEmitterPlan<Config>),
    Redis(RedisEmitterPlan<Config>),
    Mqtt(MqttEmitterPlan<Config>),
    Nats(NatsEmitterPlan<Config>),
    ZeroMq(ZeroMqEmitterPlan<Config>),
    Syslog(SyslogEmitterPlan<Config>),
    Sqs(SqsEmitterPlan<Config>),
    Sentry(SentryEmitterPlan<Config>),
    Otel(OtelEmitterPlan<Config>),
    ClickHouse(ClickHouseEmitterPlan<Config>),
    Postgres(PostgresEmitterPlan<Config>),
    MySql(MySqlEmitterPlan<Config>),
    MongoDb(MongoDbEmitterPlan<Config>),
    /// Boxed because the Iceberg emitter carries two clients beside its table, mappings, location and
    /// commit cadence, which would otherwise set the size of every emitter variant.
    Iceberg(IcebergEmitterPlan<Config>),
}

impl<Config> EmitterConnectorPlan<Config> {
    /// The batching clause this emitter publishes under, absent when it publishes one record per
    /// message.
    pub(super) fn batch(&self) -> Option<EmitterBatchPolicy> {
        match self {
            Self::Client(ClientEmitterPlan::Consumer(plan)) => Some(plan.batch),
            // Every request carries one source record.
            Self::Http(HttpEmitterPlan::Request(_)) => None,
            Self::Kafka(KafkaEmitterPlan::Topic(plan)) => plan.batch,
            Self::Pulsar(PulsarEmitterPlan::Topic(plan)) => plan.batch,
            Self::RabbitMq(RabbitMqEmitterPlan::Queue(plan)) => plan.batch,
            Self::Redis(RedisEmitterPlan::PubSub(plan)) => plan.batch,
            Self::Mqtt(MqttEmitterPlan::Topic(plan)) => plan.batch,
            Self::Nats(NatsEmitterPlan::Subject(plan)) => plan.batch,
            Self::ZeroMq(ZeroMqEmitterPlan::Push(plan)) => plan.batch,
            Self::Syslog(SyslogEmitterPlan::Collector(plan)) => plan.batch,
            Self::Sqs(SqsEmitterPlan::Queue(plan)) => plan.batch,
            Self::Sentry(SentryEmitterPlan::Event(plan)) => plan.batch,
            Self::Otel(plan) => plan.export().batch,
            Self::ClickHouse(ClickHouseEmitterPlan::Table(plan)) => Some(plan.batch),
            Self::Postgres(PostgresEmitterPlan::Table(plan)) => Some(plan.batch),
            Self::MySql(MySqlEmitterPlan::Table(plan)) => Some(plan.batch),
            Self::MongoDb(MongoDbEmitterPlan::Collection(plan)) => Some(plan.batch),
            Self::Iceberg(IcebergEmitterPlan::Table(plan)) => plan.batch,
        }
    }

    /// The vocabulary entity this decided plan composes.
    pub(super) fn kind(&self) -> nervix_models::EmitterEntityKind {
        use nervix_models::EmitterEntityModel;
        match self {
            Self::Client(ClientEmitterPlan::Consumer(_)) => {
                <nervix_models::ClientConsumerEmitter as EmitterEntityModel>::KIND
            }
            Self::Http(HttpEmitterPlan::Request(_)) => {
                <nervix_models::HttpRequestEmitter as EmitterEntityModel>::KIND
            }
            Self::Kafka(KafkaEmitterPlan::Topic(_)) => {
                <nervix_models::KafkaTopicEmitter as EmitterEntityModel>::KIND
            }
            Self::Pulsar(PulsarEmitterPlan::Topic(_)) => {
                <nervix_models::PulsarTopicEmitter as EmitterEntityModel>::KIND
            }
            Self::RabbitMq(RabbitMqEmitterPlan::Queue(_)) => {
                <nervix_models::RabbitMqQueueEmitter as EmitterEntityModel>::KIND
            }
            Self::Redis(RedisEmitterPlan::PubSub(_)) => {
                <nervix_models::RedisPubSubEmitter as EmitterEntityModel>::KIND
            }
            Self::Mqtt(MqttEmitterPlan::Topic(_)) => {
                <nervix_models::MqttTopicEmitter as EmitterEntityModel>::KIND
            }
            Self::Nats(NatsEmitterPlan::Subject(_)) => {
                <nervix_models::NatsSubjectEmitter as EmitterEntityModel>::KIND
            }
            Self::ZeroMq(ZeroMqEmitterPlan::Push(_)) => {
                <nervix_models::ZeroMqPushEmitter as EmitterEntityModel>::KIND
            }
            Self::Syslog(SyslogEmitterPlan::Collector(_)) => {
                <nervix_models::SyslogCollectorEmitter as EmitterEntityModel>::KIND
            }
            Self::Sqs(SqsEmitterPlan::Queue(_)) => {
                <nervix_models::SqsQueueEmitter as EmitterEntityModel>::KIND
            }
            Self::Sentry(SentryEmitterPlan::Event(_)) => {
                <nervix_models::SentryEventEmitter as EmitterEntityModel>::KIND
            }
            Self::ClickHouse(ClickHouseEmitterPlan::Table(_)) => {
                <nervix_models::ClickHouseTableEmitter as EmitterEntityModel>::KIND
            }
            Self::Postgres(PostgresEmitterPlan::Table(_)) => {
                <nervix_models::PostgresTableEmitter as EmitterEntityModel>::KIND
            }
            Self::MySql(MySqlEmitterPlan::Table(_)) => {
                <nervix_models::MySqlTableEmitter as EmitterEntityModel>::KIND
            }
            Self::MongoDb(MongoDbEmitterPlan::Collection(_)) => {
                <nervix_models::MongoDbCollectionEmitter as EmitterEntityModel>::KIND
            }
            Self::Iceberg(IcebergEmitterPlan::Table(_)) => {
                <nervix_models::IcebergTableEmitter as EmitterEntityModel>::KIND
            }
            Self::Otel(OtelEmitterPlan::Logs(_)) => {
                <nervix_models::OtelLogsEmitter as EmitterEntityModel>::KIND
            }
            Self::Otel(OtelEmitterPlan::Traces(_)) => {
                <nervix_models::OtelTracesEmitter as EmitterEntityModel>::KIND
            }
            Self::Otel(OtelEmitterPlan::Metrics(_)) => {
                <nervix_models::OtelMetricsEmitter as EmitterEntityModel>::KIND
            }
        }
    }
}

impl EmitterConnectorPlan<DeclaredClientConfig> {
    fn resolve_clients<Failure>(
        self,
        resolve: &mut impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<EmitterConnectorPlan, Failure> {
        let resolved = match self {
            Self::Client(ClientEmitterPlan::Consumer(plan)) => {
                EmitterConnectorPlan::Client(ClientEmitterPlan::Consumer(plan))
            }
            Self::Http(HttpEmitterPlan::Request(plan)) => {
                EmitterConnectorPlan::Http(HttpEmitterPlan::Request(plan.resolve_clients(resolve)?))
            }
            Self::Kafka(KafkaEmitterPlan::Topic(plan)) => {
                EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(plan.resolve_clients(resolve)?))
            }
            Self::Pulsar(PulsarEmitterPlan::Topic(plan)) => EmitterConnectorPlan::Pulsar(
                PulsarEmitterPlan::Topic(plan.resolve_clients(resolve)?),
            ),
            Self::RabbitMq(RabbitMqEmitterPlan::Queue(plan)) => EmitterConnectorPlan::RabbitMq(
                RabbitMqEmitterPlan::Queue(plan.resolve_clients(resolve)?),
            ),
            Self::Redis(RedisEmitterPlan::PubSub(plan)) => EmitterConnectorPlan::Redis(
                RedisEmitterPlan::PubSub(plan.resolve_clients(resolve)?),
            ),
            Self::Mqtt(MqttEmitterPlan::Topic(plan)) => {
                EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(plan.resolve_clients(resolve)?))
            }
            Self::Nats(NatsEmitterPlan::Subject(plan)) => {
                EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(plan.resolve_clients(resolve)?))
            }
            Self::ZeroMq(ZeroMqEmitterPlan::Push(plan)) => EmitterConnectorPlan::ZeroMq(
                ZeroMqEmitterPlan::Push(plan.resolve_clients(resolve)?),
            ),
            Self::Syslog(SyslogEmitterPlan::Collector(plan)) => EmitterConnectorPlan::Syslog(
                SyslogEmitterPlan::Collector(plan.resolve_clients(resolve)?),
            ),
            Self::Sqs(SqsEmitterPlan::Queue(plan)) => {
                EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(plan.resolve_clients(resolve)?))
            }
            Self::Sentry(SentryEmitterPlan::Event(plan)) => EmitterConnectorPlan::Sentry(
                SentryEmitterPlan::Event(plan.resolve_clients(resolve)?),
            ),
            Self::Otel(plan) => EmitterConnectorPlan::Otel(plan.resolve_clients(resolve)?),
            Self::ClickHouse(ClickHouseEmitterPlan::Table(plan)) => {
                EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(
                    plan.resolve_clients(resolve)?,
                ))
            }
            Self::Postgres(PostgresEmitterPlan::Table(plan)) => EmitterConnectorPlan::Postgres(
                PostgresEmitterPlan::Table(plan.resolve_clients(resolve)?),
            ),
            Self::MySql(MySqlEmitterPlan::Table(plan)) => {
                EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(plan.resolve_clients(resolve)?))
            }
            Self::MongoDb(MongoDbEmitterPlan::Collection(plan)) => EmitterConnectorPlan::MongoDb(
                MongoDbEmitterPlan::Collection(plan.resolve_clients(resolve)?),
            ),
            Self::Iceberg(IcebergEmitterPlan::Table(plan)) => EmitterConnectorPlan::Iceberg(
                IcebergEmitterPlan::Table(Box::new((*plan).resolve_clients(resolve)?)),
            ),
        };
        Ok(resolved)
    }
}

/// The client Models an emitter's emitter names, as the host found them among its domain's clients.
#[derive(Debug, Clone, Copy)]
pub(super) struct EmitterClientModels<'a> {
    /// The client registered under the name the emitter publishes through.
    pub(super) client: Option<&'a Model>,
    /// The client registered under the name of an Iceberg emitter's REST catalog.
    pub(super) catalog_client: Option<&'a Model>,
}

/// Everything one emitter's emitter is started with.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct EmitterStartPlan<Config = ResolvedClientConfig> {
    /// The declared connector label, allocated once when the plan is decided.
    pub(super) label: String,
    /// The backoff the emitter waits between publish retries and emitter reconnects.
    pub(super) retry_policy: ParsedRetryPolicy,
    pub(super) connector: EmitterConnectorPlan<Config>,
}

/// The native output contract whose deliveries are acknowledged by session consumers.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ClientConsumerEmitterPlan {
    pub(super) schema: SchemaName,
    pub(super) window: AckWindow,
    pub(super) ack_timeout: Duration,
    pub(super) batch: EmitterBatchPolicy,
}

impl EmitterStartPlan<DeclaredClientConfig> {
    /// Decides the plan for `emitter` from the client Models its emitter names.
    pub(super) fn decide(
        emitter: &CreateEmitter,
        clients: EmitterClientModels<'_>,
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        let connector = emitter.connector.as_ref();
        let policy = connector.publishing();
        let retry_policy = Self::decide_retry_policy(policy.retry_policy())?;
        if let EmitterEntity::Client(ClientEmitter::Consumer(ClientConsumerEmitter {
            schema,
            mode,
        })) = connector
        {
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

            return Ok(Self {
                label: <ClientConsumerEmitter as nervix_models::EmitterEntityModel>::KIND
                    .connector()
                    .keyword()
                    .to_ascii_lowercase(),
                retry_policy,
                connector: EmitterConnectorPlan::Client(ClientEmitterPlan::Consumer(
                    ClientConsumerEmitterPlan {
                        schema: schema.clone(),
                        window: *window,
                        ack_timeout,
                        batch,
                    },
                )),
            });
        }
        let client_name = connector
            .client()
            .verified("every non-CLIENT emitter names an external client");
        let Some(client) = clients.client else {
            return Err(Report::new(EmitterStartPlanError::MissingClient {
                connector: connector.transport_label(),
                client: client_name.clone(),
            }));
        };
        let emitter_plan = match (connector, client) {
            (
                EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
                    client: expected,
                    ..
                })),
                Model::ClientHttp(client),
            ) => EmitterConnectorPlan::Http(HttpEmitterPlan::Request(HttpRequestEmitterPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                origin: Self::decide_http_origin(expected, &client.config)?,
            })),
            (
                EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientKafka(client),
            ) => EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(KafkaTopicEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_kafka_publication(mode)?,
            })),
            (
                EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientPulsar(client),
            ) => EmitterConnectorPlan::Pulsar(PulsarEmitterPlan::Topic(PulsarTopicEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_pulsar_publication(mode)?,
            })),
            (
                EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(RabbitMqQueueEmitter {
                    client: expected,
                    queue,
                    mode,
                    ..
                })),
                Model::ClientRabbitMq(client),
            ) => EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Queue(
                RabbitMqQueueEmitterPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    queue: queue.clone(),
                    mode: decide_rabbitmq_publication(mode)?,
                },
            )),
            (
                EmitterEntity::Redis(RedisEmitter::PubSub(RedisPubSubEmitter {
                    client: expected,
                    channel,
                    ..
                })),
                Model::ClientRedis(client),
            ) => EmitterConnectorPlan::Redis(RedisEmitterPlan::PubSub(RedisPubSubEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                pool: client.pool,
                channel: channel.clone(),
            })),
            (
                EmitterEntity::Mqtt(MqttEmitter::Topic(MqttTopicEmitter {
                    client: expected,
                    topic,
                    mode,
                    ..
                })),
                Model::ClientMqtt(client),
            ) => EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(MqttTopicEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_mqtt_publishing_mode(mode)?,
            })),
            (
                EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
                    client: expected,
                    subject,
                    mode,
                    ..
                })),
                Model::ClientNats(client),
            ) => EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(NatsSubjectEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                subject: subject.clone(),
                mode: decide_nats_publishing_mode(mode)?,
            })),
            (
                EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
                    client: expected,
                    ..
                })),
                Model::ClientZeroMq(client),
            ) => EmitterConnectorPlan::ZeroMq(ZeroMqEmitterPlan::Push(ZeroMqPushEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            })),
            (
                EmitterEntity::Syslog(SyslogEmitter::Collector(SyslogCollectorEmitter {
                    client: expected,
                    ..
                })),
                Model::ClientSyslog(client),
            ) => EmitterConnectorPlan::Syslog(SyslogEmitterPlan::Collector(
                SyslogCollectorEmitterPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                },
            )),
            (
                EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
                    client: expected,
                    queue,
                    mode,
                    ..
                })),
                Model::ClientSqs(client),
            ) => EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(SqsQueueEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                queue: queue.clone(),
                mode: decide_sqs_publishing_mode(mode),
            })),
            (
                EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
                    client: expected,
                    ..
                })),
                Model::ClientSentry(client),
            ) => EmitterConnectorPlan::Sentry(SentryEmitterPlan::Event(SentryEventEmitterPlan {
                batch: emitter.batch,
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
            })),
            (EmitterEntity::Otel(otel), Model::ClientOtel(client)) => {
                let OtelMappings {
                    values,
                    attributes,
                    resource,
                    scope,
                } = otel.mappings();
                let export = OtelExportPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        otel.client(),
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
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
                };
                EmitterConnectorPlan::Otel(match otel {
                    OtelEmitter::Logs(_) => OtelEmitterPlan::Logs(OtelLogsEmitterPlan { export }),
                    OtelEmitter::Traces(_) => {
                        OtelEmitterPlan::Traces(OtelTracesEmitterPlan { export })
                    }
                    OtelEmitter::Metrics(OtelMetricsEmitter { metric, .. }) => {
                        OtelEmitterPlan::Metrics(OtelMetricsEmitterPlan {
                            export,
                            metric: OtelMetric {
                                name: metric.name.clone(),
                                unit: metric.unit.clone(),
                                description: metric.description.clone(),
                                kind: otel_metric_kind(&metric.kind),
                            },
                        })
                    }
                })
            }
            (
                EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
                    client: expected,
                    table,
                    values,
                    ..
                })),
                Model::ClientClickHouse(client),
            ) => EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(
                ClickHouseTableEmitterPlan {
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
                },
            )),
            (
                EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientPostgres(client),
            ) => EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(
                PostgresTableEmitterPlan {
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
                    mapping: MappedValuesPlan::decide(
                        &emitter.name,
                        "Postgres",
                        "postgres",
                        values,
                    )?,
                    conflict_action: postgres_conflict_action(conflict_action),
                },
            )),
            (
                EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientMySql(client),
            ) => EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(MySqlTableEmitterPlan {
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
            })),
            (
                EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
                    client: expected,
                    collection,
                    values,
                    conflict_action,
                    ..
                })),
                Model::ClientMongoDb(client),
            ) => EmitterConnectorPlan::MongoDb(MongoDbEmitterPlan::Collection(
                MongoDbCollectionEmitterPlan {
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
                },
            )),
            (
                EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
                    client: expected,
                    table,
                    namespace,
                    values,
                    location,
                    catalog: IcebergCatalog::Rest { client: catalog },
                    commit_each,
                    max_commit_size,
                    ..
                })),
                storage,
            ) => {
                let (backend, storage) =
                    Self::decide_iceberg_storage(connector, expected, storage)?;
                let catalog = Self::decide_iceberg_catalog(catalog, clients.catalog_client)?;
                EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(Box::new(
                    IcebergTableEmitterPlan {
                        batch: emitter.batch,
                        backend,
                        storage,
                        catalog,
                        table: table.clone(),
                        namespace: namespace.clone(),
                        mapping: MappedValuesPlan::decide(
                            &emitter.name,
                            "Iceberg",
                            "iceberg",
                            values,
                        )?,
                        location: location.clone(),
                        commit: iceberg_commit_policy(emitter, commit_each, max_commit_size)?,
                    },
                )))
            }
            (connector, client) => {
                return Err(EmitterStartPlanError::client_kind_mismatch(
                    connector, client,
                ));
            }
        };
        Ok(Self {
            label: emitter_plan
                .kind()
                .connector()
                .keyword()
                .to_ascii_lowercase(),
            retry_policy,
            connector: emitter_plan,
        })
    }

    /// Binds every client of this plan to the configuration `resolve` returns for its
    /// declaration, in the order the emitter names them: its own client first, then an Iceberg
    /// emitter's catalog.
    ///
    /// The host resolves each declared mount into the paths its entries render against, so the
    /// returned plan carries the mounted paths the emitter's connectors read from.
    pub(super) fn resolve_clients<Failure>(
        self,
        mut resolve: impl FnMut(
            &EmitterClientSpec<DeclaredClientConfig>,
        ) -> Result<ResolvedClientConfig, Failure>,
    ) -> Result<EmitterStartPlan, Failure> {
        let connector = self.connector.resolve_clients(&mut resolve)?;
        Ok(EmitterStartPlan {
            label: self.label,
            retry_policy: self.retry_policy,
            connector,
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

    /// The origin an HTTP emitter's client sends every request to: its `endpoint`, which the
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
        HttpOrigin::parse(endpoint).map_err(|cause| {
            cause.change_context(EmitterStartPlanError::InvalidHttpEndpoint {
                client: client.clone(),
            })
        })
    }

    /// Resolves the configured storage client into its backend and declared configuration.
    fn decide_iceberg_storage(
        destination: &EmitterEntity,
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

    /// The REST catalog client an Iceberg emitter commits through.
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

/// The quality of service `mode` declares for an MQTT `emitter`.
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

/// The NATS delivery `mode` declares for a NATS `emitter`.
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

/// The request shape `mode` declares for an SQS `emitter`.
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
        CreateClientZeroMq, EmitterBatchLegality, ErrorPolicies, FlushPolicy, OtelLogsEmitter,
        ProcessorInputs, RouteConstruction,
    };
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;

    fn expression(raw: &str) -> Expression {
        nervix_plexus::parse_expression(raw).expect("valid semantic expression")
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

    fn emitter(connector: EmitterEntity) -> CreateEmitter {
        CreateEmitter {
            name: named("orders_out"),
            from: ProcessorInputs::single(named("orders")),
            body: nervix_models::EmitterBody::Values,
            envelope: Box::new(nervix_models::EmitterEnvelope::default()),
            connector: Box::new(connector),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        }
    }

    fn iceberg_emitter() -> EmitterEntity {
        EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
            mode: nervix_models::IcebergTablePublishingMode::Ack {
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "1s".to_string(),
                },
            },
            client: named("upstream"),
            table: named("orders"),
            namespace: named("Analytics.Events"),
            values: vec![value_mapping("id", "input.id")],
            location: "s3://warehouse/orders".to_string(),
            catalog: IcebergCatalog::Rest {
                client: named("catalog"),
            },
            commit_each: "1s".to_string(),
            max_commit_size: "1MiB".parse().expect("valid commit size"),
        }))
    }

    fn iceberg_catalog() -> Model {
        Model::ClientIcebergRest(CreateClientIcebergRest {
            name: named("catalog"),
            mount: client_mount(),
            config: client_config(),
        })
    }

    /// One emitter an emitter can declare, with the client Models that emitter names.
    struct EmitterCase {
        connector: EmitterEntity,
        client: Model,
        catalog_client: Option<Model>,
    }

    impl EmitterCase {
        /// The emitter publishing to this case's emitter, declaring the batching clause a database
        /// emitter requires and leaving it out everywhere else.
        fn emitter(&self) -> CreateEmitter {
            let mut emitter = emitter(self.connector.clone());
            if let EmitterBatchLegality::Required { .. } = self.connector.batch_legality() {
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
    enum EmitterKind {
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

    impl EmitterKind {
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

        fn case(self) -> EmitterCase {
            let client = || named::<ClientName>("upstream");
            match self {
                Self::Http => EmitterCase {
                    connector: EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter {
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
                Self::Kafka => EmitterCase {
                    connector: EmitterEntity::Kafka(KafkaEmitter::Topic(KafkaTopicEmitter {
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
                Self::Pulsar => EmitterCase {
                    connector: EmitterEntity::Pulsar(PulsarEmitter::Topic(PulsarTopicEmitter {
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
                Self::RabbitMq => EmitterCase {
                    connector: EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(
                        RabbitMqQueueEmitter {
                            mode: nervix_models::RabbitMqQueuePublishingMode::NoAck {
                                retry_policy: nervix_models::RetryPolicy {
                                    backoff: "100ms".to_string(),
                                    max_backoff: "1s".to_string(),
                                },
                            },
                            client: client(),
                            queue: named("orders"),
                        },
                    )),
                    client: Model::ClientRabbitMq(CreateClientRabbitMq {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Redis => EmitterCase {
                    connector: EmitterEntity::Redis(RedisEmitter::PubSub(RedisPubSubEmitter {
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
                Self::Mqtt => EmitterCase {
                    connector: EmitterEntity::Mqtt(MqttEmitter::Topic(MqttTopicEmitter {
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
                Self::Nats => EmitterCase {
                    connector: EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
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
                Self::ZeroMq => EmitterCase {
                    connector: EmitterEntity::ZeroMq(ZeroMqEmitter::Push(ZeroMqPushEmitter {
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
                Self::Syslog => EmitterCase {
                    connector: EmitterEntity::Syslog(SyslogEmitter::Collector(
                        SyslogCollectorEmitter {
                            mode: nervix_models::SyslogCollectorPublishingMode::NoAck {
                                retry_policy: nervix_models::RetryPolicy {
                                    backoff: "100ms".to_string(),
                                    max_backoff: "1s".to_string(),
                                },
                            },
                            client: client(),
                        },
                    )),
                    client: Model::ClientSyslog(CreateClientSyslog {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Sqs => EmitterCase {
                    connector: EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
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
                Self::Sentry => EmitterCase {
                    connector: EmitterEntity::Sentry(SentryEmitter::Event(SentryEventEmitter {
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
                Self::Otel => EmitterCase {
                    connector: EmitterEntity::from(OtelLogsEmitter {
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
                Self::ClickHouse => EmitterCase {
                    connector: EmitterEntity::ClickHouse(ClickHouseEmitter::Table(
                        ClickHouseTableEmitter {
                            mode: nervix_models::ClickHouseTablePublishingMode::Ack {
                                retry_policy: nervix_models::RetryPolicy {
                                    backoff: "100ms".to_string(),
                                    max_backoff: "1s".to_string(),
                                },
                            },
                            client: client(),
                            table: named("orders"),
                            values: vec![value_mapping("id", "input.id")],
                        },
                    )),
                    client: Model::ClientClickHouse(CreateClientClickHouse {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Postgres => EmitterCase {
                    connector: EmitterEntity::Postgres(PostgresEmitter::Table(
                        PostgresTableEmitter {
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
                        },
                    )),
                    client: Model::ClientPostgres(CreateClientPostgres {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::MySql => EmitterCase {
                    connector: EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
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
                Self::MongoDb => EmitterCase {
                    connector: EmitterEntity::MongoDb(MongoDbEmitter::Collection(
                        MongoDbCollectionEmitter {
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
                        },
                    )),
                    client: Model::ClientMongoDb(CreateClientMongoDb {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::IcebergS3 => EmitterCase {
                    connector: iceberg_emitter(),
                    client: Model::ClientS3(CreateClientS3 {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergGcs => EmitterCase {
                    connector: iceberg_emitter(),
                    client: Model::ClientGcs(CreateClientGcs {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergAzureBlob => EmitterCase {
                    connector: iceberg_emitter(),
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
    #[case::http(EmitterKind::Http)]
    #[case::kafka(EmitterKind::Kafka)]
    #[case::pulsar(EmitterKind::Pulsar)]
    #[case::rabbitmq(EmitterKind::RabbitMq)]
    #[case::redis(EmitterKind::Redis)]
    #[case::mqtt(EmitterKind::Mqtt)]
    #[case::nats(EmitterKind::Nats)]
    #[case::zeromq(EmitterKind::ZeroMq)]
    #[case::syslog(EmitterKind::Syslog)]
    #[case::sqs(EmitterKind::Sqs)]
    #[case::sentry(EmitterKind::Sentry)]
    #[case::otel(EmitterKind::Otel)]
    #[case::clickhouse(EmitterKind::ClickHouse)]
    #[case::postgres(EmitterKind::Postgres)]
    #[case::mysql(EmitterKind::MySql)]
    #[case::mongodb(EmitterKind::MongoDb)]
    #[case::iceberg_s3(EmitterKind::IcebergS3)]
    #[case::iceberg_gcs(EmitterKind::IcebergGcs)]
    #[case::iceberg_azure_blob(EmitterKind::IcebergAzureBlob)]
    fn decides_each_emitter_from_its_client_model(#[case] kind: EmitterKind) {
        let plan = kind
            .case()
            .decide()
            .expect("an emitter with a matching client must be planned");

        assert_eq!(plan.label, kind.label());
        assert_eq!(
            plan.retry_policy,
            ParsedRetryPolicy {
                backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
            }
        );
    }

    #[test]
    fn every_row_emitter_carries_its_lowered_mapping_into_the_start_plan() {
        for kind in [
            EmitterKind::ClickHouse,
            EmitterKind::Postgres,
            EmitterKind::MySql,
            EmitterKind::MongoDb,
            EmitterKind::IcebergS3,
        ] {
            let plan = kind
                .case()
                .decide()
                .expect("the row emitter should be planned");
            let mapping = match &plan.connector {
                EmitterConnectorPlan::ClickHouse(ClickHouseEmitterPlan::Table(connector)) => {
                    &connector.mapping
                }
                EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(connector)) => {
                    &connector.mapping
                }
                EmitterConnectorPlan::MySql(MySqlEmitterPlan::Table(connector)) => {
                    &connector.mapping
                }
                EmitterConnectorPlan::MongoDb(MongoDbEmitterPlan::Collection(connector)) => {
                    &connector.mapping
                }
                EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) => {
                    &connector.mapping
                }
                _ => panic!("the case must be a row emitter"),
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
        let mut case = EmitterKind::Otel.case();
        let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
            &mut case.connector
        else {
            panic!("the case must be OTEL logs");
        };
        mappings
            .attributes
            .push(value_mapping("customer", "input.customer"));
        mappings
            .resource
            .push(value_mapping("service.name", "'orders'"));
        let plan = case.decide().expect("OTEL mappings should be planned");
        let EmitterConnectorPlan::Otel(OtelEmitterPlan::Logs(OtelLogsEmitterPlan {
            export: connector,
        })) = plan.connector
        else {
            panic!("the plan must be OTEL");
        };
        assert_eq!(connector.mapping.columns, ["id", "customer"]);
        assert_eq!(connector.values, ["id"]);
        assert_eq!(connector.attributes, ["customer"]);
        assert_eq!(
            connector.resource,
            [OtelResourceAttribute {
                key: "service.name".to_string(),
                value: OtelLiteral::String("orders".to_string()),
            }]
        );
    }

    #[test]
    fn otel_resource_requires_a_literal_during_planning() {
        let mut case = EmitterKind::Otel.case();
        let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
            &mut case.connector
        else {
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
                kind: case.connector.kind(),
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
            let mut case = EmitterKind::IcebergS3.case();
            let EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
                commit_each: cadence,
                ..
            })) = &mut case.connector
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
    fn iceberg_plan_preserves_namespace_and_exact_commit_bytes() {
        let case = EmitterKind::IcebergS3.case();
        let plan = case.decide().expect("valid Iceberg plan");
        let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(table)) = plan.connector else {
            panic!("the plan must be Iceberg");
        };
        assert_eq!(table.namespace.as_str(), "Analytics.Events");
        assert_eq!(table.commit.max_size, 1_048_576);
    }

    #[test]
    fn carries_the_batch_clause_into_the_plan_of_every_emitter() {
        for kind in [
            EmitterKind::Kafka,
            EmitterKind::Sentry,
            EmitterKind::Otel,
            EmitterKind::IcebergS3,
        ] {
            let case = kind.case();
            let unbatched = case.decide().expect("the emitter must be planned");
            assert_eq!(unbatched.connector.batch(), None, "{}", kind.label());

            let mut emitter = case.emitter();
            emitter.batch = Some(batch_policy());
            let batched = EmitterStartPlan::decide(
                &emitter,
                EmitterClientModels {
                    client: Some(&case.client),
                    catalog_client: case.catalog_client.as_ref(),
                },
            )
            .expect("the batching emitter must be planned");
            assert_eq!(
                batched.connector.batch(),
                Some(batch_policy()),
                "{}",
                kind.label()
            );
        }

        for kind in [
            EmitterKind::ClickHouse,
            EmitterKind::Postgres,
            EmitterKind::MySql,
            EmitterKind::MongoDb,
        ] {
            let plan = kind
                .case()
                .decide()
                .expect("the database emitter must be planned");
            assert_eq!(
                plan.connector.batch(),
                Some(batch_policy()),
                "{}",
                kind.label()
            );
        }
    }

    #[test]
    fn plans_an_http_emitter_with_its_client_origin() {
        let case = EmitterKind::Http.case();
        let emitter = case.emitter();

        let plan = EmitterStartPlan::decide(
            &emitter,
            EmitterClientModels {
                client: Some(&case.client),
                catalog_client: None,
            },
        )
        .expect("an HTTP emitter with an HTTP client must be planned");

        assert_eq!(plan.connector.batch(), None);
        let EmitterConnectorPlan::Http(HttpEmitterPlan::Request(connector)) = plan.connector else {
            panic!("an HTTP emitter must be planned as HTTP");
        };
        assert_eq!(
            connector.origin,
            HttpOrigin::parse("https://orders.example.com:8443")
                .expect("the fixture endpoint is an origin")
        );
        assert_eq!(connector.client.name, named::<ClientName>("upstream"));
    }

    #[test]
    fn an_http_client_without_an_origin_endpoint_is_not_planned() {
        for config in [
            Vec::new(),
            http_client_config("https://orders.example.com/v1"),
        ] {
            let mut case = EmitterKind::Http.case();
            case.client = Model::ClientHttp(CreateClientHttp {
                name: named("upstream"),
                mount: None,
                config,
            });

            let error = case
                .decide()
                .expect_err("an HTTP emitter needs an origin endpoint");

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
        let case = EmitterKind::MongoDb.case();
        let mut emitter = case.emitter();
        emitter.batch = None;
        let error = emitter
            .validate_batch()
            .expect_err("this entity requires a batch");
        assert_eq!(
            error.current_context(),
            &nervix_models::EmitterBatchContractError::Required {
                connector: emitter.connector.kind().phrase()
            },
        );
    }

    #[test]
    fn carries_the_declared_client_and_the_emitter_parameters() {
        let plan = EmitterKind::Postgres
            .case()
            .decide()
            .expect("a Postgres emitter with a Postgres client must be planned");

        let EmitterConnectorPlan::Postgres(PostgresEmitterPlan::Table(connector)) = plan.connector
        else {
            panic!("a Postgres emitter must be planned as Postgres");
        };
        assert_eq!(connector.client, declared_client("upstream"));
        assert_eq!(
            connector.table,
            named::<nervix_models::PostgresTableName>("orders")
        );
        assert_eq!(connector.conflict_action, PostgresConflictAction::None);
        assert_eq!(connector.batch, batch_policy());
        assert_eq!(
            connector.pooled_client(),
            PooledClientPlan {
                transport: PooledTransport::Postgres,
                bounds: pool_bounds(),
            }
        );
    }

    #[test]
    fn plans_an_iceberg_emitter_with_its_storage_and_catalog_clients() {
        let plan = EmitterKind::IcebergGcs
            .case()
            .decide()
            .expect("an Iceberg emitter with GCS storage and a REST catalog must be planned");

        let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) = plan.connector
        else {
            panic!("an Iceberg emitter must be planned as Iceberg");
        };
        assert_eq!(connector.backend, IcebergStorageBackend::Gcs);
        assert_eq!(connector.storage, declared_client("upstream"));
        assert_eq!(connector.catalog, declared_client("catalog"));
        assert_eq!(connector.location, "s3://warehouse/orders");
        assert_eq!(connector.commit.interval, Duration::from_secs(1));
        assert_eq!(connector.commit.max_size, 1_048_576);
    }

    #[test]
    fn decides_a_broker_confirmation_window_timeout_and_retry_policy() {
        let mut case = EmitterKind::Kafka.case();
        let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = &mut case.connector else {
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
            .expect("a Kafka emitter accepts an acknowledged broker mode");

        assert_eq!(
            plan.retry_policy,
            ParsedRetryPolicy {
                backoff: Duration::from_millis(25),
                max_backoff: Duration::from_secs(2),
            }
        );
        let EmitterConnectorPlan::Kafka(KafkaEmitterPlan::Topic(connector)) = plan.connector else {
            panic!("a Kafka emitter must be planned as Kafka");
        };
        assert_eq!(
            connector.mode,
            BrokerPublishingMode::Ack(AckConfirmation {
                max_in_flight: nonzero!(17usize),
                timeout: Duration::from_secs(3),
            })
        );
    }

    #[test]
    fn decides_transport_specific_mqtt_nats_and_sqs_modes() {
        let mut mqtt = EmitterKind::Mqtt.case();
        let EmitterEntity::Mqtt(MqttEmitter::Topic(destination)) = &mut mqtt.connector else {
            panic!("expected Mqtt destination");
        };
        destination.mode = nervix_models::MqttTopicPublishingMode::Qos2 {
            window: AckWindow::Sequential,
            ack_timeout: "7s".to_string(),
            retry_policy: retry_policy("10ms", "1s"),
        };
        let EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(mqtt)) =
            mqtt.decide().expect("valid MQTT mode").connector
        else {
            panic!("an MQTT emitter must be planned as MQTT");
        };
        assert_eq!(
            mqtt.mode,
            MqttPublishingMode::Qos2(AckConfirmation {
                max_in_flight: nonzero!(1usize),
                timeout: Duration::from_secs(7),
            })
        );

        let mut jetstream = EmitterKind::Nats.case();
        let EmitterEntity::Nats(NatsEmitter::Subject(destination)) = &mut jetstream.connector
        else {
            panic!("expected Nats destination");
        };
        destination.mode = nervix_models::NatsSubjectPublishingMode::JetStream {
            window: AckWindow::Parallel {
                max: nonzero!(23u64),
            },
            ack_timeout: "11s".to_string(),
            retry_policy: retry_policy("10ms", "1s"),
        };
        let EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(jetstream)) =
            jetstream.decide().expect("valid JetStream mode").connector
        else {
            panic!("a NATS emitter must be planned as NATS");
        };
        assert_eq!(
            jetstream.mode,
            NatsPublishingMode::JetStream(AckConfirmation {
                max_in_flight: nonzero!(23usize),
                timeout: Duration::from_secs(11),
            })
        );

        let EmitterConnectorPlan::Nats(NatsEmitterPlan::Subject(core)) = EmitterKind::Nats
            .case()
            .decide()
            .expect("a NATS emitter accepts NO_ACK")
            .connector
        else {
            panic!("a NATS emitter must be planned as NATS");
        };
        assert_eq!(core.mode, NatsPublishingMode::Core);

        let EmitterConnectorPlan::Sqs(SqsEmitterPlan::Queue(sqs)) = EmitterKind::Sqs
            .case()
            .decide()
            .expect("an SQS emitter accepts BATCH")
            .connector
        else {
            panic!("an SQS emitter must be planned as SQS");
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
        let mut case = EmitterKind::Kafka.case();
        let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = &mut case.connector else {
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
        let mut case = EmitterKind::RabbitMq.case();
        let EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(destination)) = &mut case.connector
        else {
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
        let case = EmitterKind::Kafka.case();

        let error = EmitterStartPlan::decide(
            &case.emitter(),
            EmitterClientModels {
                client: None,
                catalog_client: None,
            },
        )
        .expect_err("an emitter without its client must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::MissingClient {
                connector: "KAFKA TOPIC",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_a_client_of_another_kind() {
        let mut case = EmitterKind::Kafka.case();
        case.client = EmitterKind::Pulsar.case().client;

        let error = case
            .decide()
            .expect_err("a Kafka emitter must not be planned over a Pulsar client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientKindMismatch {
                connector: "KAFKA TOPIC",
                expected: nervix_models::EmitterReferenceKind::Client(
                    nervix_models::ConnectorKind::Kafka
                ),
                found: "PULSAR",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_an_iceberg_storage_client_of_another_kind() {
        let mut case = EmitterKind::IcebergS3.case();
        case.client = EmitterKind::Kafka.case().client;

        let error = case
            .decide()
            .expect_err("an Iceberg destination needs an object-store client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientKindMismatch {
                connector: "ICEBERG TABLE",
                expected: nervix_models::EmitterReferenceKind::StorageClient(&[
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
        let mut case = EmitterKind::IcebergS3.case();
        case.catalog_client = None;

        let error = case
            .decide()
            .expect_err("an Iceberg emitter without its catalog must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::MissingCatalogClient {
                client: named("catalog"),
            }
        );
    }

    #[test]
    fn reports_an_iceberg_catalog_client_of_another_kind() {
        let mut case = EmitterKind::IcebergS3.case();
        case.catalog_client = Some(EmitterKind::IcebergS3.case().client);

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
        let mut case = EmitterKind::Sentry.case();
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
    fn binds_every_client_to_its_resolved_configuration_in_emitter_order() {
        let decided = EmitterKind::IcebergAzureBlob.case().decide().expect(
            "an Iceberg emitter with Azure Blob storage and a REST catalog must be planned",
        );
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
        let EmitterConnectorPlan::Iceberg(IcebergEmitterPlan::Table(connector)) = plan.connector
        else {
            panic!("resolution must keep the Iceberg emitter");
        };
        assert_eq!(connector.storage.name, named::<ClientName>("upstream"));
        assert_eq!(connector.storage.config.entries[1].value, "upstream");
        assert_eq!(connector.catalog.name, named::<ClientName>("catalog"));
        assert_eq!(connector.catalog.config.entries[1].value, "catalog");
    }

    #[test]
    fn a_failed_resolution_fails_the_plan() {
        let decided = EmitterKind::Kafka
            .case()
            .decide()
            .expect("a Kafka emitter with a Kafka client must be planned");

        let failure = decided
            .resolve_clients(|client| Err(client.name.clone()))
            .expect_err("a failed resolution must not yield a plan");

        assert_eq!(failure, named::<ClientName>("upstream"));
    }
}
