//! Decides the complete specification for starting one emitter's sink.
//!
//! Layer: decisions.
//!
//! - **Owns.** Resolving an emitter and the client Models its sink names into one typed start
//!   plan: each connector's clients, sink parameters and publishing mode, and the emitter's retry
//!   policy. It also binds every planned client to the configuration the host resolved for it.
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

use error_stack::Report;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, ParsedRetryPolicy, ResolvedClientConfig,
};
use nervix_connector_mqtt::MqttPublishingMode;
use nervix_connector_nats::NatsPublishingMode;
use nervix_connector_sqs::SqsPublishingMode;
use nervix_models::{ChannelName, CollectionName, QueueName, SubjectName, TableName, TopicName};

use super::*;

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

impl EmitterDurationSetting {
    /// The duration `value` declares for this setting.
    fn parse(self, value: &str) -> Result<Duration, Report<EmitterStartPlanError>> {
        humantime::parse_duration(value).map_err(|source| {
            Report::new(EmitterStartPlanError::InvalidDuration {
                setting: self,
                value: value.to_string(),
            })
            .attach_printable(source)
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
        expected: &'static str,
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
    #[error("MODE {mode} is not supported by the {sink} sink")]
    UnsupportedPublishingMode {
        mode: &'static str,
        sink: &'static str,
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
}

impl EmitterStartPlanError {
    /// The report for a publishing mode that `sink` does not publish with.
    fn unsupported_mode(sink: &EmitSink, mode: &EmitterPublishingMode) -> Report<Self> {
        Report::new(Self::UnsupportedPublishingMode {
            mode: mode.kind_label(),
            sink: sink.transport_label(),
        })
    }

    /// The report for a sink whose client name resolved to a Model of another kind.
    fn client_kind_mismatch(sink: &EmitSink, client: &Model) -> Report<Self> {
        Report::new(Self::ClientKindMismatch {
            sink: sink.transport_label(),
            expected: sink.expected_client_type(),
            found: Self::found_label(client),
            client: sink.client().clone(),
        })
    }

    /// How a Model that a client name resolved to reads in a kind mismatch: its client type, or
    /// its model kind when the name resolved to something other than a client.
    fn found_label(model: &Model) -> &'static str {
        match model.client_type_label() {
            Some(label) => label,
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
        #[derive(Debug, Clone, PartialEq, Eq)]
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
    /// A Kafka sink: the topic it produces to, and how it learns that a record was accepted.
    KafkaSinkPlan {
        topic: TopicName,
        mode: BrokerPublishingMode,
    }
}

single_client_sink_plan! {
    /// A Pulsar sink: the topic its producer publishes to, and how it learns that a record was
    /// accepted.
    PulsarSinkPlan {
        topic: TopicName,
        mode: BrokerPublishingMode,
    }
}

single_client_sink_plan! {
    /// A RabbitMQ sink: the queue it publishes to, and how it learns that a record was accepted.
    RabbitMqSinkPlan {
        queue: QueueName,
        mode: BrokerPublishingMode,
    }
}

single_client_sink_plan! {
    /// A Redis sink: the channel it publishes to, and the bounds of the pool its client shares.
    RedisSinkPlan {
        pool: ClientPoolBounds,
        channel: ChannelName,
    }
}

single_client_sink_plan! {
    /// An MQTT sink: the topic it publishes to, and the quality of service it publishes at.
    MqttSinkPlan {
        topic: TopicName,
        mode: MqttPublishingMode,
    }
}

single_client_sink_plan! {
    /// A NATS sink: the subject it publishes to, and whether JetStream confirms each record.
    NatsSinkPlan {
        subject: SubjectName,
        mode: NatsPublishingMode,
    }
}

single_client_sink_plan! {
    /// A ZeroMQ sink, which pushes every record through the socket its client configures.
    ZeroMqSinkPlan {}
}

single_client_sink_plan! {
    /// A Syslog sink, which sends every record to the collector its client configures.
    SyslogSinkPlan {}
}

single_client_sink_plan! {
    /// An SQS sink: the queue it sends to, and whether it batches its requests.
    SqsSinkPlan {
        queue: String,
        mode: SqsPublishingMode,
    }
}

single_client_sink_plan! {
    /// A Sentry sink, which sends every record as an event to the project its client names.
    SentrySinkPlan {}
}

single_client_sink_plan! {
    /// An OpenTelemetry sink: the signal it exports and the mappings that build each item.
    OtelSinkPlan {
        signal: OtelSignal,
        values: Vec<OtelValueMapping>,
        attributes: Vec<OtelValueMapping>,
        resource: Vec<OtelValueMapping>,
        scope: Option<OtelScope>,
    }
}

single_client_sink_plan! {
    /// A ClickHouse sink: the table it inserts into, the mapping of each column, and the most rows
    /// one insert carries.
    ClickHouseSinkPlan {
        table: TableName,
        values: Vec<ClickHouseValueMapping>,
        max_batch: NonZeroU64,
    }
}

single_client_sink_plan! {
    /// A Postgres sink: the table it inserts into, the mapping of each column, what a conflicting
    /// row does, the most rows one insert carries, and the bounds of the pool its client shares.
    PostgresSinkPlan {
        pool: ClientPoolBounds,
        table: TableName,
        values: Vec<PostgresValueMapping>,
        conflict_action: PostgresConflictAction,
        max_batch: NonZeroU64,
    }
}

single_client_sink_plan! {
    /// A MySQL sink: the table it inserts into, the mapping of each column, what a conflicting row
    /// does, the most rows one insert carries, and the bounds of the pool its client shares.
    MySqlSinkPlan {
        pool: ClientPoolBounds,
        table: TableName,
        values: Vec<MySqlValueMapping>,
        conflict_action: MySqlConflictAction,
        max_batch: NonZeroU64,
    }
}

single_client_sink_plan! {
    /// A MongoDB sink: the collection it writes to, the mapping of each field, what a conflicting
    /// document does, the most documents one write carries, and the bounds of the pool its client
    /// shares.
    MongoDbSinkPlan {
        pool: ClientPoolBounds,
        collection: CollectionName,
        values: Vec<MongoDbValueMapping>,
        conflict_action: MongoDbConflictAction,
        max_batch: NonZeroU64,
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
/// The commit cadence and size stay as declared: the sink resolves them when it opens, where a
/// value it cannot use is reported as that sink's initialization failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct IcebergSinkPlan<Config = ResolvedClientConfig> {
    pub(super) backend: IcebergStorageBackend,
    pub(super) storage: EmitterClientSpec<Config>,
    pub(super) catalog: EmitterClientSpec<Config>,
    pub(super) table: TableName,
    pub(super) values: Vec<IcebergValueMapping>,
    pub(super) location: String,
    pub(super) commit_each: String,
    pub(super) max_commit_size: String,
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
            values: self.values,
            location: self.location,
            commit_each: self.commit_each,
            max_commit_size: self.max_commit_size,
        })
    }
}

/// The sink one emitter publishes to, with each connector's typed configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EmitterSinkPlan<Config = ResolvedClientConfig> {
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
    /// The transport this sink publishes over, as the emitter's diagnostics name it.
    pub(super) fn label(&self) -> &'static str {
        match self {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EmitterStartPlan<Config = ResolvedClientConfig> {
    /// The backoff the emitter waits between publish retries and sink reconnects.
    pub(super) retry_policy: ParsedRetryPolicy,
    pub(super) sink: EmitterSinkPlan<Config>,
}

impl EmitterStartPlan<DeclaredClientConfig> {
    /// Decides the plan for `emitter` from the client Models its sink names.
    pub(super) fn decide(
        emitter: &CreateEmitter,
        clients: EmitterClientModels<'_>,
    ) -> Result<Self, Report<EmitterStartPlanError>> {
        let sink = emitter.sink.as_ref();
        let mode = &emitter.publishing_mode;
        let retry_policy = Self::decide_retry_policy(mode.retry_policy())?;
        let Some(client) = clients.client else {
            return Err(Report::new(EmitterStartPlanError::MissingClient {
                sink: sink.transport_label(),
                client: sink.client().clone(),
            }));
        };
        let sink_plan = match (sink, client) {
            (
                EmitSink::Kafka {
                    client: expected,
                    topic,
                },
                Model::ClientKafka(client),
            ) => EmitterSinkPlan::Kafka(KafkaSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_broker_publishing_mode(sink, mode)?,
            }),
            (
                EmitSink::Pulsar {
                    client: expected,
                    topic,
                },
                Model::ClientPulsar(client),
            ) => EmitterSinkPlan::Pulsar(PulsarSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_broker_publishing_mode(sink, mode)?,
            }),
            (
                EmitSink::RabbitMq {
                    client: expected,
                    queue,
                },
                Model::ClientRabbitMq(client),
            ) => EmitterSinkPlan::RabbitMq(RabbitMqSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                queue: queue.clone(),
                mode: decide_broker_publishing_mode(sink, mode)?,
            }),
            (
                EmitSink::Redis {
                    client: expected,
                    channel,
                },
                Model::ClientRedis(client),
            ) => {
                Self::require_no_ack(sink, mode)?;
                EmitterSinkPlan::Redis(RedisSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    pool: client.pool,
                    channel: channel.clone(),
                })
            }
            (
                EmitSink::Mqtt {
                    client: expected,
                    topic,
                },
                Model::ClientMqtt(client),
            ) => EmitterSinkPlan::Mqtt(MqttSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                topic: topic.clone(),
                mode: decide_mqtt_publishing_mode(sink, mode)?,
            }),
            (
                EmitSink::Nats {
                    client: expected,
                    subject,
                },
                Model::ClientNats(client),
            ) => EmitterSinkPlan::Nats(NatsSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                subject: subject.clone(),
                mode: decide_nats_publishing_mode(sink, mode)?,
            }),
            (EmitSink::ZeroMq { client: expected }, Model::ClientZeroMq(client)) => {
                Self::require_no_ack(sink, mode)?;
                EmitterSinkPlan::ZeroMq(ZeroMqSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                })
            }
            (EmitSink::Syslog { client: expected }, Model::ClientSyslog(client)) => {
                Self::require_no_ack(sink, mode)?;
                EmitterSinkPlan::Syslog(SyslogSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                })
            }
            (
                EmitSink::Sqs {
                    client: expected,
                    queue,
                    ..
                },
                Model::ClientSqs(client),
            ) => EmitterSinkPlan::Sqs(SqsSinkPlan {
                client: EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )?,
                queue: queue.clone(),
                mode: decide_sqs_publishing_mode(sink, mode)?,
            }),
            (EmitSink::Sentry { client: expected }, Model::ClientSentry(client)) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::Sentry(SentrySinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                })
            }
            (
                EmitSink::Otel {
                    client: expected,
                    signal,
                    values,
                    attributes,
                    resource,
                    scope,
                },
                Model::ClientOtel(client),
            ) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::Otel(OtelSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    signal: signal.clone(),
                    values: values.clone(),
                    attributes: attributes.clone(),
                    resource: resource.clone(),
                    scope: scope.clone(),
                })
            }
            (
                EmitSink::ClickHouse {
                    client: expected,
                    table,
                    values,
                    max_batch,
                },
                Model::ClientClickHouse(client),
            ) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::ClickHouse(ClickHouseSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    table: table.clone(),
                    values: values.clone(),
                    max_batch: *max_batch,
                })
            }
            (
                EmitSink::Postgres {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    max_batch,
                },
                Model::ClientPostgres(client),
            ) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::Postgres(PostgresSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    pool: client.pool,
                    table: table.clone(),
                    values: values.clone(),
                    conflict_action: conflict_action.clone(),
                    max_batch: *max_batch,
                })
            }
            (
                EmitSink::MySql {
                    client: expected,
                    table,
                    values,
                    conflict_action,
                    max_batch,
                },
                Model::ClientMySql(client),
            ) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::MySql(MySqlSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    pool: client.pool,
                    table: table.clone(),
                    values: values.clone(),
                    conflict_action: conflict_action.clone(),
                    max_batch: *max_batch,
                })
            }
            (
                EmitSink::MongoDb {
                    client: expected,
                    collection,
                    values,
                    conflict_action,
                    max_batch,
                },
                Model::ClientMongoDb(client),
            ) => {
                Self::require_request_ack(sink, mode)?;
                EmitterSinkPlan::MongoDb(MongoDbSinkPlan {
                    client: EmitterClientSpec::declared(
                        expected,
                        &client.name,
                        client.mount.as_ref(),
                        &client.config,
                    )?,
                    pool: client.pool,
                    collection: collection.clone(),
                    values: values.clone(),
                    conflict_action: conflict_action.clone(),
                    max_batch: *max_batch,
                })
            }
            (
                EmitSink::Iceberg {
                    backend,
                    client: expected,
                    table,
                    values,
                    location,
                    catalog: IcebergCatalog::Rest { client: catalog },
                    commit_each,
                    max_commit_size,
                },
                storage,
            ) => {
                Self::require_request_ack(sink, mode)?;
                let storage = Self::decide_iceberg_storage(sink, *backend, expected, storage)?;
                let catalog = Self::decide_iceberg_catalog(catalog, clients.catalog_client)?;
                EmitterSinkPlan::Iceberg(Box::new(IcebergSinkPlan {
                    backend: *backend,
                    storage,
                    catalog,
                    table: table.clone(),
                    values: values.clone(),
                    location: location.clone(),
                    commit_each: commit_each.clone(),
                    max_commit_size: max_commit_size.clone(),
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

    /// Accepts `MODE NO_ACK`, the one mode of a sink that cannot confirm what it publishes.
    fn require_no_ack(
        sink: &EmitSink,
        mode: &EmitterPublishingMode,
    ) -> Result<(), Report<EmitterStartPlanError>> {
        if let EmitterPublishingMode::NoAck { .. } = mode {
            Ok(())
        } else {
            Err(EmitterStartPlanError::unsupported_mode(sink, mode))
        }
    }

    /// Accepts `MODE ACK`, the one mode of a sink whose response to each request is its
    /// acknowledgement.
    fn require_request_ack(
        sink: &EmitSink,
        mode: &EmitterPublishingMode,
    ) -> Result<(), Report<EmitterStartPlanError>> {
        if let EmitterPublishingMode::RequestAck { .. } = mode {
            Ok(())
        } else {
            Err(EmitterStartPlanError::unsupported_mode(sink, mode))
        }
    }

    /// The object-store client an Iceberg sink stages data files through, which must be the
    /// client kind its storage backend names.
    fn decide_iceberg_storage(
        sink: &EmitSink,
        backend: IcebergStorageBackend,
        expected: &ClientName,
        storage: &Model,
    ) -> Result<EmitterClientSpec<DeclaredClientConfig>, Report<EmitterStartPlanError>> {
        match (backend, storage) {
            (IcebergStorageBackend::S3, Model::ClientS3(client)) => EmitterClientSpec::declared(
                expected,
                &client.name,
                client.mount.as_ref(),
                &client.config,
            ),
            (IcebergStorageBackend::Gcs, Model::ClientGcs(client)) => EmitterClientSpec::declared(
                expected,
                &client.name,
                client.mount.as_ref(),
                &client.config,
            ),
            (IcebergStorageBackend::AzureBlob, Model::ClientAzureBlob(client)) => {
                EmitterClientSpec::declared(
                    expected,
                    &client.name,
                    client.mount.as_ref(),
                    &client.config,
                )
            }
            _ => Err(EmitterStartPlanError::client_kind_mismatch(sink, storage)),
        }
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
    window: &EmitterAckWindow,
    ack_timeout: &str,
) -> Result<AckConfirmation, Report<EmitterStartPlanError>> {
    let timeout = EmitterDurationSetting::AckTimeout.parse(ack_timeout)?;
    if timeout.is_zero() {
        return Err(Report::new(EmitterStartPlanError::ZeroAckTimeout));
    }
    let max_in_flight = match window {
        EmitterAckWindow::Sequential => NonZeroUsize::MIN,
        EmitterAckWindow::Parallel { max } => addressable_count(*max),
    };
    Ok(AckConfirmation {
        max_in_flight,
        timeout,
    })
}

/// The broker mode `mode` declares for `sink`, which publishes with `MODE NO_ACK` or `MODE ACK`.
fn decide_broker_publishing_mode(
    sink: &EmitSink,
    mode: &EmitterPublishingMode,
) -> Result<BrokerPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        EmitterPublishingMode::NoAck { .. } => Ok(BrokerPublishingMode::NoAck),
        EmitterPublishingMode::BrokerAck {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(BrokerPublishingMode::Ack(confirmation))
        }
        _ => Err(EmitterStartPlanError::unsupported_mode(sink, mode)),
    }
}

/// The quality of service `mode` declares for an MQTT `sink`.
fn decide_mqtt_publishing_mode(
    sink: &EmitSink,
    mode: &EmitterPublishingMode,
) -> Result<MqttPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        EmitterPublishingMode::MqttQos0 { .. } => Ok(MqttPublishingMode::Qos0),
        EmitterPublishingMode::MqttQos1 {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos1(confirmation))
        }
        EmitterPublishingMode::MqttQos2 {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(MqttPublishingMode::Qos2(confirmation))
        }
        _ => Err(EmitterStartPlanError::unsupported_mode(sink, mode)),
    }
}

/// The NATS delivery `mode` declares for a NATS `sink`.
fn decide_nats_publishing_mode(
    sink: &EmitSink,
    mode: &EmitterPublishingMode,
) -> Result<NatsPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        EmitterPublishingMode::NoAck { .. } => Ok(NatsPublishingMode::Core),
        EmitterPublishingMode::NatsJetStream {
            window,
            ack_timeout,
            ..
        } => {
            let confirmation = decide_ack_confirmation(window, ack_timeout)?;
            Ok(NatsPublishingMode::JetStream(confirmation))
        }
        _ => Err(EmitterStartPlanError::unsupported_mode(sink, mode)),
    }
}

/// The request shape `mode` declares for an SQS `sink`.
fn decide_sqs_publishing_mode(
    sink: &EmitSink,
    mode: &EmitterPublishingMode,
) -> Result<SqsPublishingMode, Report<EmitterStartPlanError>> {
    match mode {
        EmitterPublishingMode::SqsSingle { .. } => Ok(SqsPublishingMode::Single),
        EmitterPublishingMode::SqsBatch { .. } => Ok(SqsPublishingMode::Batch),
        _ => Err(EmitterStartPlanError::unsupported_mode(sink, mode)),
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        CreateClientClickHouse, CreateClientMongoDb, CreateClientMySql, CreateClientPostgres,
        ProcessorInputs,
    };
    use nonzero_ext::nonzero;
    use rstest::rstest;

    use super::*;

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

    fn no_ack() -> EmitterPublishingMode {
        EmitterPublishingMode::NoAck {
            retry_policy: retry_policy("100ms", "1s"),
        }
    }

    fn request_ack() -> EmitterPublishingMode {
        EmitterPublishingMode::RequestAck {
            retry_policy: retry_policy("100ms", "1s"),
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

    fn declared_client(name: &str) -> EmitterClientSpec<DeclaredClientConfig> {
        EmitterClientSpec {
            name: named(name),
            config: DeclaredClientConfig {
                mount: client_mount(),
                entries: client_config(),
            },
        }
    }

    fn emitter(sink: EmitSink, publishing_mode: EmitterPublishingMode) -> CreateEmitter {
        CreateEmitter {
            name: named("orders_out"),
            from: ProcessorInputs::single(named("orders")),
            encode_using_codec: None,
            sink: Box::new(sink),
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode,
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        }
    }

    fn iceberg_sink(backend: IcebergStorageBackend) -> EmitSink {
        EmitSink::Iceberg {
            backend,
            client: named("upstream"),
            table: named("orders"),
            values: Vec::new(),
            location: "s3://warehouse/orders".to_string(),
            catalog: IcebergCatalog::Rest {
                client: named("catalog"),
            },
            commit_each: "1s".to_string(),
            max_commit_size: "1MiB".to_string(),
        }
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
        mode: EmitterPublishingMode,
        client: Model,
        catalog_client: Option<Model>,
    }

    impl SinkCase {
        fn emitter(&self) -> CreateEmitter {
            emitter(self.sink.clone(), self.mode.clone())
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
                Self::Kafka => SinkCase {
                    sink: EmitSink::Kafka {
                        client: client(),
                        topic: named("orders"),
                    },
                    mode: no_ack(),
                    client: Model::ClientKafka(CreateClientKafka {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Pulsar => SinkCase {
                    sink: EmitSink::Pulsar {
                        client: client(),
                        topic: named("orders"),
                    },
                    mode: no_ack(),
                    client: Model::ClientPulsar(CreateClientPulsar {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::RabbitMq => SinkCase {
                    sink: EmitSink::RabbitMq {
                        client: client(),
                        queue: named("orders"),
                    },
                    mode: no_ack(),
                    client: Model::ClientRabbitMq(CreateClientRabbitMq {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Redis => SinkCase {
                    sink: EmitSink::Redis {
                        client: client(),
                        channel: named("orders"),
                    },
                    mode: no_ack(),
                    client: Model::ClientRedis(CreateClientRedis {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Mqtt => SinkCase {
                    sink: EmitSink::Mqtt {
                        client: client(),
                        topic: named("orders"),
                    },
                    mode: EmitterPublishingMode::MqttQos0 {
                        retry_policy: retry_policy("100ms", "1s"),
                    },
                    client: Model::ClientMqtt(CreateClientMqtt {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Nats => SinkCase {
                    sink: EmitSink::Nats {
                        client: client(),
                        subject: named("orders"),
                    },
                    mode: no_ack(),
                    client: Model::ClientNats(CreateClientNats {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::ZeroMq => SinkCase {
                    sink: EmitSink::ZeroMq { client: client() },
                    mode: no_ack(),
                    client: Model::ClientZeroMq(CreateClientZeroMq {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Syslog => SinkCase {
                    sink: EmitSink::Syslog { client: client() },
                    mode: no_ack(),
                    client: Model::ClientSyslog(CreateClientSyslog {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Sqs => SinkCase {
                    sink: EmitSink::Sqs {
                        client: client(),
                        queue: "orders".to_string(),
                        fifo_group: None,
                    },
                    mode: EmitterPublishingMode::SqsBatch {
                        retry_policy: retry_policy("100ms", "1s"),
                    },
                    client: Model::ClientSqs(CreateClientSqs {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Sentry => SinkCase {
                    sink: EmitSink::Sentry { client: client() },
                    mode: request_ack(),
                    client: Model::ClientSentry(CreateClientSentry {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Otel => SinkCase {
                    sink: EmitSink::Otel {
                        client: client(),
                        signal: OtelSignal::Logs,
                        values: Vec::new(),
                        attributes: Vec::new(),
                        resource: Vec::new(),
                        scope: None,
                    },
                    mode: request_ack(),
                    client: Model::ClientOtel(CreateClientOtel {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::ClickHouse => SinkCase {
                    sink: EmitSink::ClickHouse {
                        client: client(),
                        table: named("orders"),
                        values: Vec::new(),
                        max_batch: nonzero!(100u64),
                    },
                    mode: request_ack(),
                    client: Model::ClientClickHouse(CreateClientClickHouse {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::Postgres => SinkCase {
                    sink: EmitSink::Postgres {
                        client: client(),
                        table: named("orders"),
                        values: Vec::new(),
                        conflict_action: PostgresConflictAction::None,
                        max_batch: nonzero!(100u64),
                    },
                    mode: request_ack(),
                    client: Model::ClientPostgres(CreateClientPostgres {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::MySql => SinkCase {
                    sink: EmitSink::MySql {
                        client: client(),
                        table: named("orders"),
                        values: Vec::new(),
                        conflict_action: MySqlConflictAction::None,
                        max_batch: nonzero!(100u64),
                    },
                    mode: request_ack(),
                    client: Model::ClientMySql(CreateClientMySql {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::MongoDb => SinkCase {
                    sink: EmitSink::MongoDb {
                        client: client(),
                        collection: named("orders"),
                        values: Vec::new(),
                        conflict_action: MongoDbConflictAction::None,
                        max_batch: nonzero!(100u64),
                    },
                    mode: request_ack(),
                    client: Model::ClientMongoDb(CreateClientMongoDb {
                        name: client(),
                        pool: pool_bounds(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: None,
                },
                Self::IcebergS3 => SinkCase {
                    sink: iceberg_sink(IcebergStorageBackend::S3),
                    mode: request_ack(),
                    client: Model::ClientS3(CreateClientS3 {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergGcs => SinkCase {
                    sink: iceberg_sink(IcebergStorageBackend::Gcs),
                    mode: request_ack(),
                    client: Model::ClientGcs(CreateClientGcs {
                        name: client(),
                        mount: client_mount(),
                        config: client_config(),
                    }),
                    catalog_client: Some(iceberg_catalog()),
                },
                Self::IcebergAzureBlob => SinkCase {
                    sink: iceberg_sink(IcebergStorageBackend::AzureBlob),
                    mode: request_ack(),
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
    fn carries_the_declared_client_and_the_sink_parameters() {
        let plan = SinkKind::Postgres
            .case()
            .decide()
            .expect("a Postgres sink with a Postgres client must be planned");

        let EmitterSinkPlan::Postgres(sink) = plan.sink else {
            panic!("a Postgres sink must be planned as Postgres");
        };
        assert_eq!(sink.client, declared_client("upstream"));
        assert_eq!(sink.table, named::<TableName>("orders"));
        assert_eq!(sink.conflict_action, PostgresConflictAction::None);
        assert_eq!(sink.max_batch, nonzero!(100u64));
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
        assert_eq!(sink.commit_each, "1s");
        assert_eq!(sink.max_commit_size, "1MiB");
    }

    #[test]
    fn decides_a_broker_confirmation_window_timeout_and_retry_policy() {
        let mut case = SinkKind::Kafka.case();
        case.mode = EmitterPublishingMode::BrokerAck {
            window: EmitterAckWindow::Parallel {
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
        mqtt.mode = EmitterPublishingMode::MqttQos2 {
            window: EmitterAckWindow::Sequential,
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
        jetstream.mode = EmitterPublishingMode::NatsJetStream {
            window: EmitterAckWindow::Parallel {
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
    #[case::broker_sink(
        SinkKind::Kafka,
        EmitterPublishingMode::MqttQos0 { retry_policy: retry_policy("25ms", "2s") },
        "QOS 0",
        "KAFKA"
    )]
    #[case::no_ack_sink(
        SinkKind::Redis,
        EmitterPublishingMode::RequestAck { retry_policy: retry_policy("25ms", "2s") },
        "ACK",
        "REDIS"
    )]
    #[case::request_sink(SinkKind::Postgres, no_ack(), "NO_ACK", "POSTGRES")]
    fn rejects_a_mode_its_sink_does_not_publish_with(
        #[case] kind: SinkKind,
        #[case] mode: EmitterPublishingMode,
        #[case] expected_mode: &'static str,
        #[case] expected_sink: &'static str,
    ) {
        let mut case = kind.case();
        case.mode = mode;

        let error = case
            .decide()
            .expect_err("a foreign publishing mode must not be planned");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::UnsupportedPublishingMode {
                mode: expected_mode,
                sink: expected_sink,
            }
        );
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
    fn rejects_an_unusable_retry_policy(
        #[case] policy: RetryPolicy,
        #[case] expected: EmitterStartPlanError,
    ) {
        let mut case = SinkKind::Kafka.case();
        case.mode = EmitterPublishingMode::NoAck {
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
    fn rejects_an_unusable_ack_timeout(
        #[case] ack_timeout: &str,
        #[case] expected: EmitterStartPlanError,
    ) {
        let mut case = SinkKind::RabbitMq.case();
        case.mode = EmitterPublishingMode::BrokerAck {
            window: EmitterAckWindow::Sequential,
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
                sink: "KAFKA",
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
                sink: "KAFKA",
                expected: "KAFKA",
                found: "PULSAR",
                client: named("upstream"),
            }
        );
    }

    #[test]
    fn reports_iceberg_storage_of_another_backend() {
        let mut case = SinkKind::IcebergS3.case();
        case.client = SinkKind::IcebergGcs.case().client;

        let error = case
            .decide()
            .expect_err("an S3 Iceberg sink must not be planned over a GCS client");

        assert_eq!(
            error.current_context(),
            &EmitterStartPlanError::ClientKindMismatch {
                sink: "ICEBERG",
                expected: "S3",
                found: "GCS",
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
