//! Every Model family, chosen evenly and built in full.

use std::num::NonZeroUsize;

use meticulous::OptionExt as _;
use nervix_models::{Model, RequestedResourceVersion};

use crate::Arbitrary;

/// Builds an unpooled client of the named type from the parts every client declares.
macro_rules! unpooled_client {
    ($arbitrary:ident, $Variant:ident, $Client:ident) => {{
        let parts = $arbitrary.client_parts();
        Model::$Variant(nervix_models::$Client {
            name: parts.name,
            mount: parts.mount,
            config: parts.config,
        })
    }};
}

/// Builds a pooled client of the named type, which declares its connection-pool bounds too.
macro_rules! pooled_client {
    ($arbitrary:ident, $Variant:ident, $Client:ident) => {{
        let parts = $arbitrary.client_parts();
        let pool = $arbitrary.pool_bounds();
        Model::$Variant(nervix_models::$Client {
            name: parts.name,
            pool,
            mount: parts.mount,
            config: parts.config,
        })
    }};
}

/// Every variant of [`Model`], so a property reaches each family as often as any other and a
/// coverage check can ask for each one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum ModelVariant {
    Schema,
    WireJsonSchema,
    WireCborSchema,
    WireAvroSchema,
    Codec,
    ClientKafka,
    ClientPulsar,
    ClientHttp,
    ClientSentry,
    ClientOtel,
    ClientPrometheus,
    ClientMqtt,
    ClientNats,
    ClientRabbitMq,
    ClientRedis,
    ClientZeroMq,
    ClientSqs,
    ClientWebsockets,
    ClientSyslog,
    ClientClickHouse,
    ClientPostgres,
    ClientMySql,
    ClientMongoDb,
    ClientS3,
    ClientGcs,
    ClientAzureBlob,
    ClientIcebergRest,
    Vhost,
    Branch,
    Endpoint,
    SignalingProtocol,
    Generator,
    Inferencer,
    WasmProcessor,
    Ingestor,
    Reingestor,
    Relay,
    Lookup,
    Junction,
    Deduplicator,
    Correlator,
    Reorderer,
    WindowProcessor,
    Emitter,
    Placement,
    Udf,
}

impl ModelVariant {
    /// Every variant, in declaration order.
    pub const ALL: [Self; 46] = [
        Self::Schema,
        Self::WireJsonSchema,
        Self::WireCborSchema,
        Self::WireAvroSchema,
        Self::Codec,
        Self::ClientKafka,
        Self::ClientPulsar,
        Self::ClientHttp,
        Self::ClientSentry,
        Self::ClientOtel,
        Self::ClientPrometheus,
        Self::ClientMqtt,
        Self::ClientNats,
        Self::ClientRabbitMq,
        Self::ClientRedis,
        Self::ClientZeroMq,
        Self::ClientSqs,
        Self::ClientWebsockets,
        Self::ClientSyslog,
        Self::ClientClickHouse,
        Self::ClientPostgres,
        Self::ClientMySql,
        Self::ClientMongoDb,
        Self::ClientS3,
        Self::ClientGcs,
        Self::ClientAzureBlob,
        Self::ClientIcebergRest,
        Self::Vhost,
        Self::Branch,
        Self::Endpoint,
        Self::SignalingProtocol,
        Self::Generator,
        Self::Inferencer,
        Self::WasmProcessor,
        Self::Ingestor,
        Self::Reingestor,
        Self::Relay,
        Self::Lookup,
        Self::Junction,
        Self::Deduplicator,
        Self::Correlator,
        Self::Reorderer,
        Self::WindowProcessor,
        Self::Emitter,
        Self::Placement,
        Self::Udf,
    ];

    /// The variant `model` is. The match is exhaustive, so a new Model family does not compile
    /// until the generator is taught to build it.
    pub fn of<Version>(model: &Model<Version>) -> Self {
        match model {
            Model::Schema(_) => Self::Schema,
            Model::WireJsonSchema(_) => Self::WireJsonSchema,
            Model::WireCborSchema(_) => Self::WireCborSchema,
            Model::WireAvroSchema(_) => Self::WireAvroSchema,
            Model::Codec(_) => Self::Codec,
            Model::ClientKafka(_) => Self::ClientKafka,
            Model::ClientPulsar(_) => Self::ClientPulsar,
            Model::ClientHttp(_) => Self::ClientHttp,
            Model::ClientSentry(_) => Self::ClientSentry,
            Model::ClientOtel(_) => Self::ClientOtel,
            Model::ClientPrometheus(_) => Self::ClientPrometheus,
            Model::ClientMqtt(_) => Self::ClientMqtt,
            Model::ClientNats(_) => Self::ClientNats,
            Model::ClientRabbitMq(_) => Self::ClientRabbitMq,
            Model::ClientRedis(_) => Self::ClientRedis,
            Model::ClientZeroMq(_) => Self::ClientZeroMq,
            Model::ClientSqs(_) => Self::ClientSqs,
            Model::ClientWebsockets(_) => Self::ClientWebsockets,
            Model::ClientSyslog(_) => Self::ClientSyslog,
            Model::ClientClickHouse(_) => Self::ClientClickHouse,
            Model::ClientPostgres(_) => Self::ClientPostgres,
            Model::ClientMySql(_) => Self::ClientMySql,
            Model::ClientMongoDb(_) => Self::ClientMongoDb,
            Model::ClientS3(_) => Self::ClientS3,
            Model::ClientGcs(_) => Self::ClientGcs,
            Model::ClientAzureBlob(_) => Self::ClientAzureBlob,
            Model::ClientIcebergRest(_) => Self::ClientIcebergRest,
            Model::Vhost(_) => Self::Vhost,
            Model::Branch(_) => Self::Branch,
            Model::Endpoint(_) => Self::Endpoint,
            Model::SignalingProtocol(_) => Self::SignalingProtocol,
            Model::Generator(_) => Self::Generator,
            Model::Inferencer(_) => Self::Inferencer,
            Model::WasmProcessor(_) => Self::WasmProcessor,
            Model::Ingestor(_) => Self::Ingestor,
            Model::Reingestor(_) => Self::Reingestor,
            Model::Relay(_) => Self::Relay,
            Model::Lookup(_) => Self::Lookup,
            Model::Junction(_) => Self::Junction,
            Model::Deduplicator(_) => Self::Deduplicator,
            Model::Correlator(_) => Self::Correlator,
            Model::Reorderer(_) => Self::Reorderer,
            Model::WindowProcessor(_) => Self::WindowProcessor,
            Model::Emitter(_) => Self::Emitter,
            Model::Placement(_) => Self::Placement,
            Model::Udf(_) => Self::Udf,
        }
    }
}

impl Arbitrary<'_> {
    /// A Model of any family, each family as likely as any other.
    pub fn model(&mut self) -> Model<RequestedResourceVersion> {
        let count = NonZeroUsize::new(ModelVariant::ALL.len()).assured("there are Model families");
        let variant = ModelVariant::ALL[self.entropy.index(count)];
        self.model_of(variant)
    }

    /// A Model of the requested family, with every field it declares generated.
    pub fn model_of(&mut self, variant: ModelVariant) -> Model<RequestedResourceVersion> {
        match variant {
            ModelVariant::Schema => Model::Schema(self.create_schema()),
            ModelVariant::WireJsonSchema => Model::WireJsonSchema(self.json_wire_schema()),
            ModelVariant::WireCborSchema => Model::WireCborSchema(self.json_wire_schema()),
            ModelVariant::WireAvroSchema => Model::WireAvroSchema(self.avro_wire_schema()),
            ModelVariant::Codec => Model::Codec(self.create_codec()),
            ModelVariant::ClientKafka => unpooled_client!(self, ClientKafka, CreateClientKafka),
            ModelVariant::ClientPulsar => unpooled_client!(self, ClientPulsar, CreateClientPulsar),
            ModelVariant::ClientHttp => unpooled_client!(self, ClientHttp, CreateClientHttp),
            ModelVariant::ClientSentry => unpooled_client!(self, ClientSentry, CreateClientSentry),
            ModelVariant::ClientOtel => unpooled_client!(self, ClientOtel, CreateClientOtel),
            ModelVariant::ClientPrometheus => {
                unpooled_client!(self, ClientPrometheus, CreateClientPrometheus)
            }
            ModelVariant::ClientMqtt => unpooled_client!(self, ClientMqtt, CreateClientMqtt),
            ModelVariant::ClientNats => unpooled_client!(self, ClientNats, CreateClientNats),
            ModelVariant::ClientRabbitMq => {
                unpooled_client!(self, ClientRabbitMq, CreateClientRabbitMq)
            }
            ModelVariant::ClientRedis => pooled_client!(self, ClientRedis, CreateClientRedis),
            ModelVariant::ClientZeroMq => unpooled_client!(self, ClientZeroMq, CreateClientZeroMq),
            ModelVariant::ClientSqs => unpooled_client!(self, ClientSqs, CreateClientSqs),
            ModelVariant::ClientWebsockets => Model::ClientWebsockets(self.websockets_client()),
            ModelVariant::ClientSyslog => unpooled_client!(self, ClientSyslog, CreateClientSyslog),
            ModelVariant::ClientClickHouse => {
                unpooled_client!(self, ClientClickHouse, CreateClientClickHouse)
            }
            ModelVariant::ClientPostgres => {
                pooled_client!(self, ClientPostgres, CreateClientPostgres)
            }
            ModelVariant::ClientMySql => pooled_client!(self, ClientMySql, CreateClientMySql),
            ModelVariant::ClientMongoDb => pooled_client!(self, ClientMongoDb, CreateClientMongoDb),
            ModelVariant::ClientS3 => unpooled_client!(self, ClientS3, CreateClientS3),
            ModelVariant::ClientGcs => unpooled_client!(self, ClientGcs, CreateClientGcs),
            ModelVariant::ClientAzureBlob => {
                unpooled_client!(self, ClientAzureBlob, CreateClientAzureBlob)
            }
            ModelVariant::ClientIcebergRest => {
                unpooled_client!(self, ClientIcebergRest, CreateClientIcebergRest)
            }
            ModelVariant::Vhost => Model::Vhost(self.create_vhost()),
            ModelVariant::Branch => Model::Branch(self.create_branch()),
            ModelVariant::Endpoint => Model::Endpoint(self.create_endpoint()),
            ModelVariant::SignalingProtocol => {
                Model::SignalingProtocol(self.create_signaling_protocol())
            }
            ModelVariant::Generator => Model::Generator(self.create_generator()),
            ModelVariant::Inferencer => Model::Inferencer(self.create_inferencer()),
            ModelVariant::WasmProcessor => Model::WasmProcessor(self.create_wasm_processor()),
            ModelVariant::Ingestor => Model::Ingestor(self.create_ingestor()),
            ModelVariant::Reingestor => Model::Reingestor(self.create_reingestor()),
            ModelVariant::Relay => Model::Relay(self.create_relay()),
            ModelVariant::Lookup => Model::Lookup(self.create_lookup()),
            ModelVariant::Junction => Model::Junction(self.create_junction()),
            ModelVariant::Deduplicator => Model::Deduplicator(self.create_deduplicator()),
            ModelVariant::Correlator => Model::Correlator(self.create_correlator()),
            ModelVariant::Reorderer => Model::Reorderer(self.create_reorderer()),
            ModelVariant::WindowProcessor => Model::WindowProcessor(self.create_window_processor()),
            ModelVariant::Emitter => Model::Emitter(self.create_emitter()),
            ModelVariant::Placement => Model::Placement(self.create_placement()),
            ModelVariant::Udf => Model::Udf(self.create_udf()),
        }
    }
}
