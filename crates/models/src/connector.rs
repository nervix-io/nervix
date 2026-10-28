//! The connectors Nervix exchanges data with, and the entities each one reads or writes.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The kind of every connector, the union of ingestor Models and the union of emitter Models
//!   nested connector by entity, the kinds that name each connector and entity pair, the
//!   alterations of the ingestors that read each pair, the shape of the names an external system
//!   gives the entities a ingestor reads, and what a pair declares whatever its statement says:
//!   the header model of each side, the metadata namespace, where a ingestor executes, what an emitter
//!   requires of its emitter, and what a body references. Each connector's own module owns its
//!   client Model, its entity Models, their modes and the rules its external names follow.
//! - **Depends on.** Validated names, expressions, schema types, and the acknowledgement, retry and
//!   batching vocabulary the entity Models share.
//! - **Must not know.** How a connector is planned, validated, hosted or driven, or any connector
//!   crate. A capability the registry validates is declared here so that validation never names
//!   the crate that implements it.
//!
//! A ingestor or an emitter is addressed by two things: the connector that serves it and the entity it
//! consumes. `IngestorEntity::Redis(RedisIngestor::PubSub(..))` states both in its shape, so a statement
//! of one pair can never hold the fields of another.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

mod bounds;
mod clickhouse;
mod client;
mod config_schema;
mod configured_client;
mod deadline;
mod declaration;
mod emitter;
mod entity_union;
mod external_name;
mod header_model;
mod header_pairs;
mod http;
mod iceberg;
mod ingestor;
mod ingestor_metadata;
mod kafka;
mod mapped_values;
mod mongodb;
mod mqtt;
mod mysql;
mod nats;
mod otel;
mod postgres;
mod prometheus;
mod publishing;
mod pulsar;
mod rabbitmq;
mod redis;
mod sentry;
mod sqs;
mod syslog;
mod websockets;
mod zeromq;

use strum::{AsRefStr, Display, EnumIter, IntoStaticStr};

pub use self::{
    bounds::*, clickhouse::*, client::*, config_schema::*, configured_client::*, deadline::*,
    declaration::*, emitter::*, external_name::ExternalNameError, header_model::*, header_pairs::*,
    http::*, iceberg::*, ingestor::*, ingestor_metadata::*, kafka::*, mapped_values::*, mongodb::*,
    mqtt::*, mysql::*, nats::*, otel::*, postgres::*, prometheus::*, pulsar::*, rabbitmq::*,
    redis::*, sentry::*, sqs::*, syslog::*, websockets::*, zeromq::*,
};
pub(crate) use self::{entity_union::ALTERED_PAIR_CHECKED, ingestor::AlteredIngestorEntity};

/// The kind of external system, or of Nervix-served boundary, that a client connects to and a
/// ingestor or emitter exchanges data with.
///
/// The keyword of a kind is how Plexus spells it, and it is what diagnostics name a client by and
/// what the phrase naming a ingestor or an emitter starts with. A kind displays as its keyword.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    AsRefStr,
    IntoStaticStr,
    EnumIter,
    Display,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum ConnectorKind {
    /// Application sessions, which produce into a client ingestor and consume from a client
    /// emitter. No client Model configures it.
    Client,
    Http,
    Websockets,
    Kafka,
    Pulsar,
    #[strum(serialize = "RABBITMQ")]
    RabbitMq,
    Redis,
    Mqtt,
    Nats,
    #[strum(serialize = "ZEROMQ")]
    ZeroMq,
    Sqs,
    Syslog,
    Prometheus,
    Sentry,
    Otel,
    #[strum(serialize = "CLICKHOUSE")]
    ClickHouse,
    Postgres,
    #[strum(serialize = "MYSQL")]
    MySql,
    #[strum(serialize = "MONGODB")]
    MongoDb,
    /// Iceberg tables. Its emitter writes through an object-store client and commits through a
    /// catalog client, so no client Model is of this kind itself.
    Iceberg,
    /// The S3 object store an Iceberg emitter writes data files to.
    S3,
    /// The Google Cloud Storage object store an Iceberg emitter writes data files to.
    #[strum(serialize = "GCS")]
    Gcs,
    /// The Azure Blob Storage object store an Iceberg emitter writes data files to.
    AzureBlob,
    /// The Iceberg REST catalog an Iceberg emitter commits through.
    IcebergRest,
}

impl ConnectorKind {
    /// How Plexus spells this kind.
    pub fn keyword(self) -> &'static str {
        self.into()
    }

    /// What every client of this connector declares. Application sessions reach the `CLIENT`
    /// connector without a client Model, and an Iceberg emitter writes through an object-store
    /// client and commits through a catalog client, so neither has a client of its own.
    pub const fn client(self) -> Option<ClientDeclaration> {
        match self {
            Self::Client | Self::Iceberg => None,
            Self::Http => Some(<CreateClientHttp>::DECLARATION),
            Self::Websockets => Some(<CreateClientWebsockets>::DECLARATION),
            Self::Kafka => Some(<CreateClientKafka>::DECLARATION),
            Self::Pulsar => Some(<CreateClientPulsar>::DECLARATION),
            Self::RabbitMq => Some(<CreateClientRabbitMq>::DECLARATION),
            Self::Redis => Some(<CreateClientRedis>::DECLARATION),
            Self::Mqtt => Some(<CreateClientMqtt>::DECLARATION),
            Self::Nats => Some(<CreateClientNats>::DECLARATION),
            Self::ZeroMq => Some(<CreateClientZeroMq>::DECLARATION),
            Self::Sqs => Some(<CreateClientSqs>::DECLARATION),
            Self::Syslog => Some(<CreateClientSyslog>::DECLARATION),
            Self::Prometheus => Some(<CreateClientPrometheus>::DECLARATION),
            Self::Sentry => Some(<CreateClientSentry>::DECLARATION),
            Self::Otel => Some(<CreateClientOtel>::DECLARATION),
            Self::ClickHouse => Some(<CreateClientClickHouse>::DECLARATION),
            Self::Postgres => Some(<CreateClientPostgres>::DECLARATION),
            Self::MySql => Some(<CreateClientMySql>::DECLARATION),
            Self::MongoDb => Some(<CreateClientMongoDb>::DECLARATION),
            Self::S3 => Some(<CreateClientS3>::DECLARATION),
            Self::Gcs => Some(<CreateClientGcs>::DECLARATION),
            Self::AzureBlob => Some(<CreateClientAzureBlob>::DECLARATION),
            Self::IcebergRest => Some(<CreateClientIcebergRest>::DECLARATION),
        }
    }
}
