//! The resource versions a Model binds, as a statement writes them and as a stored Model pins them.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** Rebuilding a binding Model with its versions mapped from one written form to
//!   another, and writing a stored Model back as the statement that creates it.
//! - **Depends on.** The binding Models and [`RequestedResourceVersion`].
//! - **Must not know.** Which versions a domain has completed, or how planning resolves `LATEST`.

use std::convert::Infallible;

use crate::{
    ClientResourceMount, CodecProtobufConfig, CodecWireFormat, CreateClientAzureBlob,
    CreateClientClickHouse, CreateClientGcs, CreateClientHttp, CreateClientIcebergRest,
    CreateClientKafka, CreateClientMongoDb, CreateClientMqtt, CreateClientMySql, CreateClientNats,
    CreateClientOtel, CreateClientPostgres, CreateClientPrometheus, CreateClientPulsar,
    CreateClientRabbitMq, CreateClientRedis, CreateClientS3, CreateClientSentry, CreateClientSqs,
    CreateClientSyslog, CreateClientWebsockets, CreateClientZeroMq, CreateCodec, CreateInferencer,
    CreateLookup, CreateSignalingProtocol, CreateVhost, CreateWasmProcessor, Model,
    RequestedResourceVersion, ResourceName, SignalingProtobufConfig, SignalingWireFormat,
    VhostTlsResource,
};

/// One stored Model rebuilt with a different version of the resource it binds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRebinding {
    pub model: Model,
    pub previous_version: u64,
}

impl<Version> ClientResourceMount<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<ClientResourceMount<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let version = map(&self.resource, self.version)?;
        Ok(ClientResourceMount {
            resource: self.resource,
            version,
        })
    }
}

macro_rules! impl_client_resource_versions {
    ($($Client:ident { $($extra:ident),* $(,)? };)+) => {
        $(
            impl<Version> $Client<Version> {
                pub(crate) fn try_map_resource_versions<Next, Error, Map>(
                    self,
                    map: &mut Map,
                ) -> Result<$Client<Next>, Error>
                where
                    Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
                {
                    let mount = match self.mount {
                        Some(mount) => Some(mount.try_map_version(map)?),
                        None => None,
                    };
                    Ok($Client {
                        name: self.name,
                        $($extra: self.$extra,)*
                        mount,
                        config: self.config,
                    })
                }
            }
        )+
    };
}

impl_client_resource_versions! {
    CreateClientKafka {};
    CreateClientPulsar {};
    CreateClientHttp {};
    CreateClientSentry {};
    CreateClientOtel {};
    CreateClientPrometheus {};
    CreateClientMqtt {};
    CreateClientNats {};
    CreateClientRabbitMq {};
    CreateClientZeroMq {};
    CreateClientSqs {};
    CreateClientSyslog {};
    CreateClientClickHouse {};
    CreateClientS3 {};
    CreateClientGcs {};
    CreateClientAzureBlob {};
    CreateClientIcebergRest {};
    CreateClientRedis { pool };
    CreateClientPostgres { pool };
    CreateClientMySql { pool };
    CreateClientMongoDb { pool };
    CreateClientWebsockets { signaling_protocol };
}

impl<Version> CreateLookup<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateLookup<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let resource_version = map(&self.resource, self.resource_version)?;
        Ok(CreateLookup {
            name: self.name,
            key_field: self.key_field,
            resource: self.resource,
            resource_version,
            path: self.path,
            decode_using_codec: self.decode_using_codec,
        })
    }
}

impl<Version> VhostTlsResource<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<VhostTlsResource<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let version = map(&self.resource, self.version)?;
        Ok(VhostTlsResource {
            resource: self.resource,
            version,
        })
    }
}

impl<Version> CreateVhost<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateVhost<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let tls = match self.tls {
            Some(tls) => Some(tls.try_map_version(map)?),
            None => None,
        };
        Ok(CreateVhost {
            name: self.name,
            hostnames: self.hostnames,
            tls,
        })
    }
}

impl<Version> CodecProtobufConfig<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CodecProtobufConfig<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let resource_version = map(&self.resource, self.resource_version)?;
        Ok(CodecProtobufConfig {
            resource: self.resource,
            resource_version,
            config: self.config,
            message: self.message,
            transformations: self.transformations,
        })
    }
}

impl<Version> CodecWireFormat<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CodecWireFormat<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let mapped = match self {
            Self::Json { wire_schema } => CodecWireFormat::Json { wire_schema },
            Self::Cbor { wire_schema } => CodecWireFormat::Cbor { wire_schema },
            Self::Avro { wire_schema } => CodecWireFormat::Avro { wire_schema },
            Self::Syslog => CodecWireFormat::Syslog,
            Self::JaqNative {
                format,
                transformations,
            } => CodecWireFormat::JaqNative {
                format,
                transformations,
            },
            Self::Protobuf(config) => CodecWireFormat::Protobuf(config.try_map_version(map)?),
        };
        Ok(mapped)
    }
}

impl<Version> CreateCodec<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateCodec<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let wire_format = self.wire_format.try_map_version(map)?;
        Ok(CreateCodec {
            name: self.name,
            wire_format,
            schema: self.schema,
            encoding_rules: self.encoding_rules,
        })
    }
}

impl<Version> SignalingProtobufConfig<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<SignalingProtobufConfig<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let resource_version = map(&self.resource, self.resource_version)?;
        Ok(SignalingProtobufConfig {
            resource: self.resource,
            resource_version,
            config: self.config,
            send_message: self.send_message,
            wait_message: self.wait_message,
        })
    }
}

impl<Version> SignalingWireFormat<Version> {
    fn try_map_version<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<SignalingWireFormat<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let mapped = match self {
            Self::Json => SignalingWireFormat::Json,
            Self::Yaml => SignalingWireFormat::Yaml,
            Self::Toml => SignalingWireFormat::Toml,
            Self::Xml => SignalingWireFormat::Xml,
            Self::Cbor => SignalingWireFormat::Cbor,
            Self::Raw => SignalingWireFormat::Raw,
            Self::Protobuf(config) => SignalingWireFormat::Protobuf(config.try_map_version(map)?),
        };
        Ok(mapped)
    }
}

impl<Version> CreateSignalingProtocol<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateSignalingProtocol<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let format = self.format.try_map_version(map)?;
        Ok(CreateSignalingProtocol {
            name: self.name,
            format,
            on_connect: self.on_connect,
        })
    }
}

impl<Version> CreateInferencer<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateInferencer<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let resource_version = map(&self.resource, self.resource_version)?;
        Ok(CreateInferencer {
            name: self.name,
            from: self.from,
            output_routes: self.output_routes,
            branched_by: self.branched_by,
            resource: self.resource,
            resource_version,
            file: self.file,
            inputs: self.inputs,
            output_schema: self.output_schema,
            mode: self.mode,
            filter_where: self.filter_where,
            materialized_state: self.materialized_state,
        })
    }
}

impl<Version> CreateWasmProcessor<Version> {
    pub(crate) fn try_map_resource_versions<Next, Error, Map>(
        self,
        map: &mut Map,
    ) -> Result<CreateWasmProcessor<Next>, Error>
    where
        Map: FnMut(&ResourceName, Version) -> Result<Next, Error>,
    {
        let resource_version = map(&self.resource, self.resource_version)?;
        Ok(CreateWasmProcessor {
            name: self.name,
            from: self.from,
            output_routes: self.output_routes,
            branched_by: self.branched_by,
            resource: self.resource,
            resource_version,
            file: self.file,
            limits: self.limits,
            global_error_policy: self.global_error_policy,
            rejected_state_policy: self.rejected_state_policy,
            mode: self.mode,
            filter_where: self.filter_where,
            materialized_state: self.materialized_state,
        })
    }
}

/// A stored Model written as the statement that creates it, naming every version it binds.
impl From<Model> for Model<RequestedResourceVersion> {
    fn from(model: Model) -> Self {
        let written: Result<Self, Infallible> = model
            .try_map_resource_versions(|_, version| Ok(RequestedResourceVersion::Number(version)));
        let Ok(written) = written;
        written
    }
}

impl Model {
    /// Returns the concrete version of `resource` this model binds, when it binds that resource.
    pub fn resource_version(&self, resource: &ResourceName) -> Option<u64> {
        let mut version = None;
        let mapped: Result<Model, Infallible> =
            self.clone()
                .try_map_resource_versions(|bound_resource, bound_version| {
                    if bound_resource == resource {
                        version = Some(bound_version);
                    }
                    Ok(bound_version)
                });
        let Ok(_) = mapped;
        version
    }

    /// Rebuilds this model with `resource` bound to `version`.
    pub fn rebind_resource(
        &self,
        resource: &ResourceName,
        version: u64,
    ) -> Option<ResourceRebinding> {
        let mut previous_version = None;
        let rebound: Result<Model, Infallible> =
            self.clone()
                .try_map_resource_versions(|bound_resource, bound_version| {
                    if bound_resource == resource {
                        previous_version = Some(bound_version);
                        Ok(version)
                    } else {
                        Ok(bound_version)
                    }
                });
        let Ok(model) = rebound;
        previous_version.map(|previous_version| ResourceRebinding {
            model,
            previous_version,
        })
    }
}

impl<Version: Clone> Model<Version> {
    /// Whether this model binds `resource`, independent of the version representation.
    pub fn binds_resource(&self, resource: &ResourceName) -> bool {
        let mut binds = false;
        let mapped: Result<Model<Version>, Infallible> =
            self.clone()
                .try_map_resource_versions(|bound_resource, version| {
                    binds |= bound_resource == resource;
                    Ok(version)
                });
        let Ok(_) = mapped;
        binds
    }
}
