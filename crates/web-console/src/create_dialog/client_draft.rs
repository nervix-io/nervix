//! Browser draft and semantic conversion for every current client transport.
//!
//! Layer: edges.
//!
//! - **Owns.** Transport selection, conditional pool and signaling fields, connector CONFIG rows,
//!   an optional resource mount, and redacted command presentation.
//! - **Depends on.** Current client Models and typed resource and signaling references.
//! - **Must not know.** Connector initialization, external service provisioning, or graph planning.

use std::num::NonZeroU32;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClientConfigEntry, ClientName, ClientPoolBounds, ClientPoolBoundsError, ClientResourceMount,
    CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb, CreateClientMqtt,
    CreateClientMySql, CreateClientNats, CreateClientOtel, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, Model, ModelKind, NodeRef,
    RequestedResourceVersion, SignalingProtocolName,
};
use thiserror::Error;

use super::{
    SelectedReference,
    resource_pin_draft::{ResourcePinDraft, ResourcePinError},
};

macro_rules! declare_transports {
    (
        unpooled { $($variant:ident => $model:ident($ty:ident) [$key:literal, $label:literal],)+ }
        pooled { $($pooled:ident => $pooled_model:ident($pooled_ty:ident) [$pooled_key:literal, $pooled_label:literal],)+ }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(super) enum ClientTransport {
            $($variant,)+
            $($pooled,)+
            Websockets,
        }

        impl ClientTransport {
            pub(super) const ALL: [Self; 22] = [
                $(Self::$variant,)+
                $(Self::$pooled,)+
                Self::Websockets,
            ];

            pub(super) fn key(self) -> &'static str {
                match self {
                    $(Self::$variant => $key,)+
                    $(Self::$pooled => $pooled_key,)+
                    Self::Websockets => "websockets",
                }
            }

            pub(super) fn label(self) -> &'static str {
                match self {
                    $(Self::$variant => $label,)+
                    $(Self::$pooled => $pooled_label,)+
                    Self::Websockets => "WEBSOCKETS",
                }
            }

            pub(super) fn pooled(self) -> bool {
                matches!(self, $(Self::$pooled)|+)
            }

            pub(super) fn websockets(self) -> bool {
                self == Self::Websockets
            }

            fn model(
                self,
                name: ClientName,
                pool: Option<ClientPoolBounds>,
                mount: Option<ClientResourceMount<RequestedResourceVersion>>,
                signaling_protocol: Option<SignalingProtocolName>,
                config: Vec<ClientConfigEntry>,
            ) -> Model<RequestedResourceVersion> {
                match self {
                    $(Self::$variant => Model::$model($ty { name, mount, config }),)+
                    $(Self::$pooled => Model::$pooled_model($pooled_ty {
                        name,
                        pool: pool.verified("the pooled draft validated its required bounds"),
                        mount,
                        config,
                    }),)+
                    Self::Websockets => Model::ClientWebsockets(CreateClientWebsockets {
                        name, mount, signaling_protocol, config,
                    }),
                }
            }
        }
    };
}

declare_transports! {
    unpooled {
        Kafka => ClientKafka(CreateClientKafka) ["kafka", "KAFKA"],
        Pulsar => ClientPulsar(CreateClientPulsar) ["pulsar", "PULSAR"],
        Http => ClientHttp(CreateClientHttp) ["http", "HTTP"],
        Sentry => ClientSentry(CreateClientSentry) ["sentry", "SENTRY"],
        Otel => ClientOtel(CreateClientOtel) ["otel", "OTEL"],
        Prometheus => ClientPrometheus(CreateClientPrometheus) ["prometheus", "PROMETHEUS"],
        Mqtt => ClientMqtt(CreateClientMqtt) ["mqtt", "MQTT"],
        Nats => ClientNats(CreateClientNats) ["nats", "NATS"],
        RabbitMq => ClientRabbitMq(CreateClientRabbitMq) ["rabbitmq", "RABBITMQ"],
        ZeroMq => ClientZeroMq(CreateClientZeroMq) ["zeromq", "ZEROMQ"],
        Sqs => ClientSqs(CreateClientSqs) ["sqs", "SQS"],
        Syslog => ClientSyslog(CreateClientSyslog) ["syslog", "SYSLOG"],
        ClickHouse => ClientClickHouse(CreateClientClickHouse) ["clickhouse", "CLICKHOUSE"],
        S3 => ClientS3(CreateClientS3) ["s3", "S3"],
        Gcs => ClientGcs(CreateClientGcs) ["gcs", "GCS"],
        AzureBlob => ClientAzureBlob(CreateClientAzureBlob) ["azure-blob", "AZURE_BLOB"],
        IcebergRest => ClientIcebergRest(CreateClientIcebergRest) ["iceberg-rest", "ICEBERG_REST"],
    }
    pooled {
        Redis => ClientRedis(CreateClientRedis) ["redis", "REDIS"],
        Postgres => ClientPostgres(CreateClientPostgres) ["postgres", "POSTGRES"],
        MySql => ClientMySql(CreateClientMySql) ["mysql", "MYSQL"],
        MongoDb => ClientMongoDb(CreateClientMongoDb) ["mongodb", "MONGODB"],
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ClientConfigDraft {
    pub(super) key: String,
    pub(super) value: String,
    pub(super) secret: bool,
}

impl ClientConfigDraft {
    pub(super) fn is_secret(&self) -> bool {
        if self.secret {
            return true;
        }
        let key = self.key.to_ascii_lowercase();
        if [
            "password",
            "secret",
            "token",
            "credential",
            "private_key",
            "dsn",
        ]
        .iter()
        .any(|part| key.contains(part))
        {
            return true;
        }
        if let Ok(address) = url::Url::parse(&self.value) {
            return address.password().is_some();
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ClientDraft {
    pub(super) name: String,
    pub(super) if_not_exists: bool,
    pub(super) transport: Option<ClientTransport>,
    pub(super) minimum: String,
    pub(super) maximum: String,
    pub(super) mount_enabled: bool,
    pub(super) mount: ResourcePinDraft,
    pub(super) signaling_protocol: Option<SelectedReference<SignalingProtocolName>>,
    pub(super) config: Vec<ClientConfigDraft>,
}

pub(super) struct CompletedClient {
    pub(super) actual: Model<RequestedResourceVersion>,
    pub(super) presentation: Model<RequestedResourceVersion>,
}

impl ClientDraft {
    pub(super) fn set_transport(&mut self, transport: ClientTransport) {
        if self.transport == Some(transport) {
            return;
        }
        self.transport = Some(transport);
        self.config.clear();
        self.signaling_protocol = None;
        self.minimum.clear();
        self.maximum.clear();
    }

    pub(super) fn select_signaling(&mut self, node: &NodeRef) {
        if node.kind == ModelKind::SignalingProtocol {
            self.signaling_protocol = Some(SelectedReference::chosen(
                SignalingProtocolName::parse(node.identifier.as_str())
                    .verified("a signaling protocol NodeRef carries a validated model name"),
            ));
        }
    }

    pub(super) fn selects_signaling(&self, node: &NodeRef) -> bool {
        self.signaling_protocol
            .as_ref()
            .is_some_and(|selected| selected.selects(ModelKind::SignalingProtocol, node))
    }

    pub(super) fn invalidate_references(&mut self) {
        self.mount.invalidate();
        if let Some(signaling) = &mut self.signaling_protocol {
            signaling.invalidate();
        }
    }

    pub(super) fn current_resource(&self) -> Option<&nervix_models::ResourceName> {
        if self.mount_enabled {
            self.mount.current_resource()
        } else {
            None
        }
    }

    pub(super) fn build(&self) -> error_stack::Result<CompletedClient, ClientDraftError> {
        let name =
            ClientName::parse(self.name.trim()).map_err(|_| Report::new(ClientDraftError::Name))?;
        let transport = self
            .transport
            .ok_or_else(|| Report::new(ClientDraftError::Transport))?;
        let pool = if transport.pooled() {
            let minimum = self
                .minimum
                .trim()
                .parse::<u32>()
                .map_err(|_| Report::new(ClientDraftError::PoolMinimum))?;
            let maximum = self
                .maximum
                .trim()
                .parse::<NonZeroU32>()
                .map_err(|_| Report::new(ClientDraftError::PoolMaximum))?;
            Some(ClientPoolBounds::new(minimum, maximum).map_err(|error| {
                let context = ClientDraftError::PoolBounds(error.current_context().clone());
                error.change_context(context)
            })?)
        } else {
            None
        };
        let mount = if self.mount_enabled {
            let pin = self.mount.build().map_err(|error| {
                let context = ClientDraftError::Mount(error.current_context().clone());
                error.change_context(context)
            })?;
            Some(ClientResourceMount {
                resource: pin.resource,
                version: pin.version,
            })
        } else {
            None
        };
        let signaling = if transport.websockets() {
            match &self.signaling_protocol {
                Some(selected) => Some(
                    selected
                        .current_name()
                        .ok_or_else(|| Report::new(ClientDraftError::SignalingChanged))?
                        .clone(),
                ),
                None => None,
            }
        } else {
            None
        };
        if self.config.is_empty() {
            return Err(Report::new(ClientDraftError::ConfigRequired));
        }
        let mut config = Vec::with_capacity(self.config.len());
        let mut presented_config = Vec::with_capacity(self.config.len());
        for (index, entry) in self.config.iter().enumerate() {
            let key = entry.key.trim();
            if key.is_empty() {
                return Err(Report::new(ClientDraftError::ConfigKey {
                    entry: index + 1,
                }));
            }
            config.push(ClientConfigEntry {
                key: key.to_string(),
                value: entry.value.clone(),
            });
            presented_config.push(ClientConfigEntry {
                key: key.to_string(),
                value: if entry.is_secret() {
                    "********".to_string()
                } else {
                    entry.value.clone()
                },
            });
        }
        let actual = transport.model(name.clone(), pool, mount.clone(), signaling.clone(), config);
        let presentation = transport.model(name, pool, mount, signaling, presented_config);
        Ok(CompletedClient {
            actual,
            presentation,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum ClientDraftError {
    #[error("Client name is invalid")]
    Name,
    #[error("Choose a client transport")]
    Transport,
    #[error("Pool minimum must be a nonnegative whole number")]
    PoolMinimum,
    #[error("Pool maximum must be a positive whole number")]
    PoolMaximum,
    #[error("{0}")]
    PoolBounds(#[from] ClientPoolBoundsError),
    #[error("{0}")]
    Mount(#[from] ResourcePinError),
    #[error("The signaling protocol belongs to a changed context; select it again")]
    SignalingChanged,
    #[error("Add at least one connector configuration entry")]
    ConfigRequired,
    #[error("Configuration entry {entry} needs a key")]
    ConfigKey { entry: usize },
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        CreateStatement, ModelKind, ModelName, NodeRef, RequestedResourceVersion, ResourceName,
        Statement,
    };
    use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

    use super::{ClientConfigDraft, ClientDraft, ClientDraftError, ClientTransport};

    fn ready(transport: ClientTransport) -> ClientDraft {
        let mut draft = ClientDraft {
            name: "visual_client".to_string(),
            ..ClientDraft::default()
        };
        draft.set_transport(transport);
        if transport.pooled() {
            draft.minimum = "1".to_string();
            draft.maximum = "4".to_string();
        }
        draft.config.push(ClientConfigDraft {
            key: "endpoint".to_string(),
            value: "http://example.com".to_string(),
            secret: false,
        });
        draft
    }

    #[test]
    fn every_offered_client_transport_builds_its_model_and_round_trips() {
        assert_eq!(ClientTransport::ALL.len(), 22);
        for transport in ClientTransport::ALL {
            let draft = ready(transport);
            let completed = draft
                .build()
                .assured("the transport's required fields are set");
            assert_eq!(
                completed.actual.client_type_label(),
                Some(transport.label())
            );
            let statement =
                Statement::Create(CreateStatement::new(Box::new(completed.actual), false));
            let source = statement.to_canonical_nspl().assured("client renders");
            assert!(source.contains(&format!("TYPE {}", transport.label())));
            assert_eq!(
                parse_client_statement(&source).assured("canonical client must parse"),
                ClientStatement::Server(statement),
            );
        }
    }

    #[test]
    fn pool_mount_signaling_and_secret_presentation_keep_their_exact_semantics() {
        let mut pooled = ready(ClientTransport::Redis);
        pooled.maximum.clear();
        assert_eq!(
            pooled
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ClientDraftError::PoolMaximum),
        );
        pooled.maximum = "0".to_string();
        assert_eq!(
            pooled
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ClientDraftError::PoolMaximum),
        );
        pooled.maximum = "4".to_string();
        pooled.minimum = "5".to_string();
        assert!(matches!(
            pooled
                .build()
                .err()
                .map(|error| error.current_context().clone()),
            Some(ClientDraftError::PoolBounds(_)),
        ));

        let mut draft = ready(ClientTransport::Websockets);
        draft.mount_enabled = true;
        let resource = ResourceName::parse("tls_bundle").assured("valid resource name");
        draft.mount.select_resource(resource.clone());
        draft
            .mount
            .select_version(RequestedResourceVersion::Number(3));
        draft.select_signaling(&NodeRef::new(
            ModelKind::SignalingProtocol,
            ModelName::parse("handshake").assured("valid protocol name"),
        ));
        draft.config[0].key = "password".to_string();
        draft.config[0].value = "private value".to_string();
        let completed = draft.build().assured("client has mount and configuration");
        let actual = Statement::Create(CreateStatement::new(Box::new(completed.actual), false))
            .to_canonical_nspl()
            .assured("actual client renders");
        let presented = Statement::Create(CreateStatement::new(
            Box::new(completed.presentation),
            false,
        ))
        .to_canonical_nspl()
        .assured("presented client renders");
        assert!(actual.contains("MOUNT tls_bundle VERSION 3"));
        assert!(actual.contains("WITH SIGNALING PROTOCOL handshake"));
        assert!(actual.contains("private value"));
        assert!(!presented.contains("private value"));
        assert!(presented.contains("********"));
        draft
            .mount
            .select_resource(ResourceName::parse("another_bundle").assured("valid name"));
        assert!(draft.build().is_err());
        draft.mount.select_version(RequestedResourceVersion::Latest);
        draft.invalidate_references();
        assert!(draft.build().is_err());

        draft.set_transport(ClientTransport::Http);
        assert!(draft.config.is_empty());
        assert!(draft.signaling_protocol.is_none());
        assert!(draft.minimum.is_empty());
        assert!(draft.maximum.is_empty());
    }
}
