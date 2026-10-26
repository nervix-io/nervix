//! The Iceberg connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The table emitter Model with its storage backend, catalog and commit policy, the
//!   object-store and catalog client Models it writes and commits through, and what the emitter
//!   declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The Iceberg, object-store or catalog drivers, or
//!   how data files are staged and committed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeadlineKey, DeclaredBounds, DeclaredBuffer,
        DeclaredWait, OmittedDeadline, WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EmitterValueMapping, EmitterValueMappings, LocatorDeclaration,
    LocatorName, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
};

configured_client!(S3: unpooled, config CLIENT_CONFIG_S3, CreateClientS3 => S3ConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG_S3: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("access_key_id", V::String).secret().driver(),
            K::optional("allow_anonymous", V::Boolean).driver(),
            K::optional("client_region", V::String).driver(),
            K::optional("disable_ec2_metadata", V::Boolean).driver(),
            K::optional("endpoint", V::Url(&["http", "https"])).driver(),
            K::optional("path_style_access", V::Boolean).driver(),
            K::optional("region", V::String).driver(),
            K::optional("secret_access_key", V::String)
                .secret()
                .driver(),
            K::optional("session_token", V::String).secret().driver(),
            K::TIMEOUT,
        ],
        &[ClientConfigNamespace::All],
        &[],
    )
};
configured_client!(Gcs: unpooled, config CLIENT_CONFIG_GCS, CreateClientGcs => GcsConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG_GCS: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional(
                "credential_path",
                V::File(crate::ClientConfigFile::MountedFile),
            )
            .secret()
            .driver(),
            K::optional("no_auth", V::Boolean).driver(),
            K::optional("service_path", V::Url(&["http", "https"])).driver(),
            K::TIMEOUT,
            K::optional("token", V::String).secret().driver(),
        ],
        &[ClientConfigNamespace::All],
        &[],
    )
};
configured_client!(AzureBlob: unpooled, config CLIENT_CONFIG_AZUREBLOB, CreateClientAzureBlob => AzureBlobConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG_AZUREBLOB: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("account_key", V::String).secret().driver(),
            K::optional("account_name", V::String).driver(),
            K::optional("authority_host", V::String).driver(),
            K::optional("client_id", V::String).driver(),
            K::optional("client_secret", V::String).secret().driver(),
            K::optional("connection_string", V::String)
                .secret()
                .driver(),
            K::optional("endpoint", V::Url(&["http", "https"])).driver(),
            K::optional("sas_token", V::String).secret().driver(),
            K::optional("tenant_id", V::String).driver(),
            K::TIMEOUT,
        ],
        &[ClientConfigNamespace::All],
        &[],
    )
};
configured_client!(IcebergRest: unpooled, config CLIENT_CONFIG_ICEBERGREST, CreateClientIcebergRest => IcebergRestConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG_ICEBERGREST: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace, ClientConfigSchema, ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("credential", V::String).secret().driver(),
            K::optional("oauth2-server-uri", V::Url(&["http", "https"])).driver(),
            K::optional("scope", V::String).driver(),
            K::optional("token", V::String).secret().driver(),
            K::required("uri", V::Url(&["http", "https"])).driver(),
            K::optional("warehouse", V::String).driver(),
        ],
        &[ClientConfigNamespace::All],
        &[],
    )
};

pub type IcebergValueMapping = EmitterValueMapping;

/// One Iceberg table, whose data files an object-store client writes and whose snapshots a
/// catalog client commits.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct IcebergTableEmitter {
    pub mode: IcebergTablePublishingMode,
    /// The object-store client, whose connector selects the planned storage backend.
    pub client: ClientName,
    pub table: IcebergTableName,
    pub values: Vec<IcebergValueMapping>,
    pub location: String,
    pub catalog: IcebergCatalog,
    pub commit_each: String,
    pub max_commit_size: String,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum IcebergCatalog {
    Rest { client: ClientName },
}

impl IcebergCatalog {
    /// The connector the catalog's client configures.
    pub const fn connector(&self) -> ConnectorKind {
        match self {
            Self::Rest { .. } => ConnectorKind::IcebergRest,
        }
    }

    /// The client the catalog is reached through, with the connector it must configure.
    pub const fn client_reference(&self) -> EmitterClientReference<'_> {
        match self {
            Self::Rest { client } => EmitterClientReference {
                client,
                connector: self.connector(),
            },
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    AsRefStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum IcebergStorageBackend {
    S3,
    #[strum(serialize = "GCS")]
    Gcs,
    #[strum(serialize = "AZURE_BLOB")]
    AzureBlob,
}

impl IcebergStorageBackend {
    /// The connector an object-store client of this backend configures.
    pub const fn connector(self) -> ConnectorKind {
        match self {
            Self::S3 => ConnectorKind::S3,
            Self::Gcs => ConnectorKind::Gcs,
            Self::AzureBlob => ConnectorKind::AzureBlob,
        }
    }
}

/// What the connector of a ICEBERG TABLE holds and how long it waits.
const ICEBERG_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "records staged on disk until a commit",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("COMMIT EACH ... MAX SIZE"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "catalog requests",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "object-store metadata and delete operations",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Default(Duration::from_secs(60)),
            },
        },
        DeclaredWait {
            wait: "object-store reads and writes",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Default(Duration::from_secs(10)),
            },
        },
    ],
    driver_limits: &[],
};

impl EmitterDeclaration for IcebergTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::StorageClient(&[
            ConnectorKind::S3,
            ConnectorKind::Gcs,
            ConnectorKind::AzureBlob,
        ]),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: IcebergTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(IcebergTableName::DECLARATION),
        }),
        options: &[
            OptionDeclaration {
                keywords: "LOCATION",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Text,
                    example: "LOCATION 's3://warehouse/orders'",
                },
            },
            OptionDeclaration {
                keywords: "CATALOG",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Client(ConnectorKind::IcebergRest),
                    example: "CATALOG catalog",
                },
            },
            OptionDeclaration {
                keywords: "COMMIT EACH",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Clause,
                    example: "COMMIT EACH 10s MAX SIZE 64MiB",
                },
            },
        ],
        envelope: &[],
        properties: &[],
        bounds: ICEBERG_BOUNDS,
    };
}

impl EmitterEntityBody for IcebergTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::StorageClient(&self.client)
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn catalog_client(&self) -> Option<EmitterClientReference<'_>> {
        Some(self.catalog.client_reference())
    }

    fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        Some(crate::MappedValueTypes::Columns)
    }

    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings::values(&self.values)
    }

    fn commit_policy(&self) -> Option<(&str, &str)> {
        Some((self.commit_each.as_str(), self.max_commit_size.as_str()))
    }
}

connector_emitters! {
    connector Iceberg = "ICEBERG";
    /// What an Iceberg emitter writes.
    pub enum IcebergEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum IcebergTableEmitterMode;
    /// The entities an Iceberg emitter writes.
    pub enum IcebergEmitterEntity {
        Table(IcebergTableEmitter, IcebergTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum IcebergTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct IcebergTableName => super::external_name::external_identifier,
        label "iceberg_table", example "Orders";
}
