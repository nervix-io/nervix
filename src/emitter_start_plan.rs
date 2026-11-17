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
use nervix_connector_otel::{
    OtelAggregationTemporality, OtelLiteral, OtelMetric, OtelMetricKind, OtelResourceAttribute,
    OtelScope,
};
use nervix_connector_postgres::PostgresConflictAction;
use nervix_models::{
    AckWindow, Assignment, AssignmentTarget, ClickHouseEmitter, ClickHouseTableEmitter,
    ClickHouseValueMapping, ClientConfigEntry, ClientConsumerEmitter, ClientEmitter, ClientName,
    ClientPoolBounds, ClientResourceMount, CreateEmitter, EmitterBatchPolicy,
    EmitterDeclaration as _, EmitterEntity, Expression, FieldName, HttpEmitter, HttpOrigin,
    HttpRequestEmitter, IcebergCatalog, IcebergEmitter, IcebergStorageBackend, IcebergTableEmitter,
    KafkaEmitter, KafkaTopicEmitter, Literal, Model, MongoDbCollectionEmitter, MongoDbEmitter,
    MqttEmitter, MqttQos, MqttTopicEmitter, MqttTopicPublishingMode, MySqlEmitter,
    MySqlTableEmitter, NatsEmitter, NatsStreamEmitter, NatsSubjectEmitter, OtelEmitter,
    OtelMappings, OtelMetricsEmitter, PostgresEmitter, PostgresTableEmitter, PulsarEmitter,
    PulsarTopicEmitter, RabbitMqEmitter, RabbitMqExchangeEmitter, RabbitMqQueueEmitter,
    RedisEmitter, RedisPubSubEmitter, RetryPolicy, RouteConstruction, SchemaName, SentryEmitter,
    SqsEmitter, SqsQueueEmitter, SyslogCollectorEmitter, SyslogEmitter, UnmetPublishingRequirement,
    ZeroMqEmitter, ZeroMqPushEmitter, parse_duration_text,
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
    #[error("{0}")]
    UnmetPublishing(UnmetPublishingRequirement),
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

/// The queue and exchange entities planned for RabbitMq execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum RabbitMqEmitterPlan<Config = ResolvedClientConfig> {
    Queue(RabbitMqQueueEmitterPlan<Config>),
    Exchange(RabbitMqExchangeEmitterPlan<Config>),
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

/// The NATS entity planned for execution.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum NatsEmitterPlan<Config = ResolvedClientConfig> {
    Subject(NatsSubjectEmitterPlan<Config>),
    Stream(NatsStreamEmitterPlan<Config>),
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
            Self::RabbitMq(RabbitMqEmitterPlan::Exchange(plan)) => plan.batch,
            Self::Redis(RedisEmitterPlan::PubSub(plan)) => plan.batch,
            Self::Mqtt(MqttEmitterPlan::Topic(plan)) => plan.batch,
            Self::Nats(NatsEmitterPlan::Subject(plan)) => plan.batch,
            Self::Nats(NatsEmitterPlan::Stream(plan)) => plan.batch,
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
            Self::RabbitMq(RabbitMqEmitterPlan::Exchange(_)) => {
                <nervix_models::RabbitMqExchangeEmitter as EmitterEntityModel>::KIND
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
            Self::Nats(NatsEmitterPlan::Stream(_)) => {
                <nervix_models::NatsStreamEmitter as EmitterEntityModel>::KIND
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
            Self::RabbitMq(RabbitMqEmitterPlan::Exchange(plan)) => EmitterConnectorPlan::RabbitMq(
                RabbitMqEmitterPlan::Exchange(plan.resolve_clients(resolve)?),
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
            Self::Nats(NatsEmitterPlan::Stream(plan)) => {
                EmitterConnectorPlan::Nats(NatsEmitterPlan::Stream(plan.resolve_clients(resolve)?))
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
                partition_from_branch: emitter.envelope.partition_from_branch,
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
                partition_from_branch: emitter.envelope.partition_from_branch,
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
                EmitterEntity::RabbitMq(RabbitMqEmitter::Exchange(RabbitMqExchangeEmitter {
                    client: expected,
                    exchange,
                    mode,
                    ..
                })),
                Model::ClientRabbitMq(client),
            ) => EmitterConnectorPlan::RabbitMq(RabbitMqEmitterPlan::Exchange(
                RabbitMqExchangeEmitterPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    exchange: exchange.clone(),
                    mode: decide_rabbitmq_exchange_publication(mode)?,
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
            (EmitterEntity::Mqtt(MqttEmitter::Topic(destination)), Model::ClientMqtt(client)) => {
                EmitterConnectorPlan::Mqtt(MqttEmitterPlan::Topic(MqttTopicEmitterPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        &destination.client,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    topic: destination.topic.clone(),
                    mode: decide_mqtt_publishing_mode(destination)?,
                }))
            }
            (
                EmitterEntity::Nats(NatsEmitter::Subject(NatsSubjectEmitter {
                    client: expected,
                    subject,
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
            })),
            (
                EmitterEntity::Nats(NatsEmitter::Stream(NatsStreamEmitter {
                    client: expected,
                    stream,
                    subject,
                    mode,
                })),
                Model::ClientNats(client),
            ) => {
                let nervix_models::NatsStreamPublishingMode::Ack {
                    window,
                    ack_timeout,
                    ..
                } = mode;
                EmitterConnectorPlan::Nats(NatsEmitterPlan::Stream(NatsStreamEmitterPlan {
                    batch: emitter.batch,
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    stream: stream.clone(),
                    subject: subject.clone(),
                    confirmation: decide_ack_confirmation(window, ack_timeout)?,
                }))
            }
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
                    send,
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
                send: *send,
            })),
            (EmitterEntity::Sentry(SentryEmitter::Event(event)), Model::ClientSentry(client)) => {
                EmitterConnectorPlan::Sentry(SentryEmitterPlan::Event(SentryEventEmitterPlan {
                    batch: emitter.batch,
                    platform: event.platform().to_owned(),
                    client: EmitterClientSpec::declared(
                        &event.client,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                }))
            }
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
broker_publication!(
    decide_rabbitmq_exchange_publication,
    RabbitMqExchangePublishingMode
);

/// The quality of service an MQTT `destination` publishes at, with the confirmation its mode
/// declares for an answered one. The plan holds both in one value, so a pairing the declaration
/// refuses has no plan.
fn decide_mqtt_publishing_mode(
    destination: &MqttTopicEmitter,
) -> Result<MqttPublishingMode, Report<EmitterStartPlanError>> {
    match (destination.qos, &destination.mode) {
        (MqttQos::Qos0, MqttTopicPublishingMode::NoAck { .. }) => Ok(MqttPublishingMode::Qos0),
        (
            MqttQos::Qos1,
            MqttTopicPublishingMode::Ack {
                window,
                ack_timeout,
                ..
            },
        ) => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos1(confirmation))
        }
        (
            MqttQos::Qos2,
            MqttTopicPublishingMode::Ack {
                window,
                ack_timeout,
                ..
            },
        ) => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos2(confirmation))
        }
        (MqttQos::Qos0, MqttTopicPublishingMode::Ack { .. })
        | (MqttQos::Qos1 | MqttQos::Qos2, MqttTopicPublishingMode::NoAck { .. }) => {
            let unmet = destination.unmet_publishing().assured(
                "the MQTT TOPIC declaration requires another quality of service for exactly these \
                 pairings",
            );
            Err(Report::new(EmitterStartPlanError::UnmetPublishing(unmet)))
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
