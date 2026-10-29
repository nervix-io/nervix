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

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;
use thiserror::Error;
use url::Url;

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
    LocatorName, OptionDeclaration, OptionForm, OptionPresence, OptionValue, PayloadSizeLimit,
};

configured_client!(S3: unpooled, config CLIENT_CONFIG_S3, CreateClientS3 => S3ConfigEntry);

/// Nervix validates its options; namespaced Iceberg properties retain their driver's contract.
const CLIENT_CONFIG_S3: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace as N, ClientConfigSchema,
        ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("access_key_id", V::String).secret(),
            K::optional("allow_anonymous", V::Boolean),
            K::optional("client_region", V::String),
            K::optional("disable_config_load", V::Boolean),
            K::optional("disable_ec2_metadata", V::Boolean),
            K::optional("endpoint", V::Url(&["http", "https"])),
            K::optional("path_style_access", V::Boolean),
            K::optional("region", V::String),
            K::optional("secret_access_key", V::String).secret(),
            K::optional("session_token", V::String).secret(),
            K::TIMEOUT,
        ],
        &[N::Prefix("s3."), N::Prefix("client.")],
        &[],
    )
};
configured_client!(Gcs: unpooled, config CLIENT_CONFIG_GCS, CreateClientGcs => GcsConfigEntry);

/// Credentials are inline JSON; the configured driver namespace is `gcs.*`.
const CLIENT_CONFIG_GCS: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace as N, ClientConfigSchema,
        ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("allow_anonymous", V::Boolean),
            K::optional("credentials_json", V::String).secret(),
            K::optional("disable_config_load", V::Boolean),
            K::optional("disable_vm_metadata", V::Boolean),
            K::optional("no_auth", V::Boolean),
            K::optional("service_path", V::Url(&["http", "https"])),
            K::TIMEOUT,
            K::optional("token", V::String).secret(),
        ],
        &[N::Prefix("gcs.")],
        &[],
    )
};
configured_client!(AzureBlob: unpooled, config CLIENT_CONFIG_AZUREBLOB, CreateClientAzureBlob => AzureBlobConfigEntry);

/// The table location selects the Azure endpoint; this client supplies its authentication.
const CLIENT_CONFIG_AZUREBLOB: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace as N, ClientConfigSchema,
        ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("account_key", V::String).secret(),
            K::optional("account_name", V::String),
            K::optional("authority_host", V::String),
            K::optional("client_id", V::String),
            K::optional("client_secret", V::String).secret(),
            K::optional("sas_token", V::String).secret(),
            K::optional("tenant_id", V::String),
            K::TIMEOUT,
        ],
        &[N::Prefix("adls.")],
        &[],
    )
};
configured_client!(IcebergRest: unpooled, config CLIENT_CONFIG_ICEBERGREST, CreateClientIcebergRest => IcebergRestConfigEntry);

/// The catalog owns its locator; catalog headers and file-I/O properties are delegated.
const CLIENT_CONFIG_ICEBERGREST: crate::ClientConfigSchema = {
    use crate::{
        ClientConfigKey as K, ClientConfigNamespace as N, ClientConfigSchema,
        ClientConfigValue as V,
    };
    ClientConfigSchema::new(
        &[
            K::optional("audience", V::String).driver(),
            K::optional("credential", V::String).secret().driver(),
            K::optional("disable-header-redaction", V::Boolean).driver(),
            K::optional("oauth2-server-uri", V::Url(&["http", "https"])),
            K::optional("prefix", V::String).driver(),
            K::optional("resource", V::String).driver(),
            K::optional("scope", V::String).driver(),
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
            K::optional("token", V::String).secret().driver(),
            K::required("uri", V::Url(&["http", "https"])),
            K::optional("warehouse", V::String),
        ],
        &[
            N::Prefix("header."),
            N::Prefix("s3."),
            N::Prefix("client."),
            N::Prefix("gcs."),
            N::Prefix("adls."),
        ],
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
    pub namespace: IcebergNamespace,
    pub values: Vec<IcebergValueMapping>,
    pub location: String,
    pub catalog: IcebergCatalog,
    pub commit_each: String,
    pub max_commit_size: PayloadSizeLimit,
}

impl IcebergTableEmitter {
    /// The Parquet writer's data-file rollover target when `BATCH` is absent.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "Iceberg TableProperties fixes the default write target file size at 512 MiB"
        )
    )]
    pub const DEFAULT_DATA_FILE_TARGET_BYTES: u64 = 512 * 1024 * 1024;
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

    /// The object-location URL schemes this backend accepts.
    pub const fn location_schemes(self) -> &'static [&'static str] {
        match self {
            Self::S3 => &["s3"],
            Self::Gcs => &["gs"],
            Self::AzureBlob => &["wasb", "wasbs", "abfs", "abfss"],
        }
    }

    /// Checks a table location against the backend the declared storage client selects.
    pub fn validate_location(self, location: &str) -> Result<(), Report<IcebergLocationError>> {
        let url = Url::parse(location).map_err(|source| {
            Report::new(source).change_context(IcebergLocationError::Malformed)
        })?;
        if !self.location_schemes().contains(&url.scheme()) {
            return Err(Report::new(IcebergLocationError::Scheme {
                backend: self,
                found: url.scheme().to_string(),
            }));
        }
        if url.host_str().is_none() {
            return Err(Report::new(IcebergLocationError::MissingAuthority));
        }
        Ok(())
    }
}

/// Why a connector kind cannot select an Iceberg object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "converting a connector kind is a pure selection refusal"
    )
)]
#[error("{found} is not an Iceberg object-store client")]
pub struct IcebergStorageBackendError {
    pub found: ConnectorKind,
}

impl TryFrom<ConnectorKind> for IcebergStorageBackend {
    type Error = IcebergStorageBackendError;

    fn try_from(found: ConnectorKind) -> Result<Self, Self::Error> {
        match found {
            ConnectorKind::S3 => Ok(Self::S3),
            ConnectorKind::Gcs => Ok(Self::Gcs),
            ConnectorKind::AzureBlob => Ok(Self::AzureBlob),
            _ => Err(IcebergStorageBackendError { found }),
        }
    }
}

/// Why a declared location cannot identify an object under its storage client.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IcebergLocationError {
    #[error("LOCATION must be an absolute object-store URL")]
    Malformed,
    #[error("LOCATION scheme '{found}' does not match {} storage; expected {}", .backend.connector(), .backend.location_schemes().join(", "))]
    Scheme {
        backend: IcebergStorageBackend,
        found: String,
    },
    #[error("LOCATION must name an object-store bucket or container authority")]
    MissingAuthority,
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
                keywords: "NAMESPACE",
                presence: OptionPresence::Required,
                form: OptionForm::Value {
                    value: OptionValue::Text,
                    example: "NAMESPACE 'Analytics.Events'",
                },
            },
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

    fn commit_policy(&self) -> Option<(&str, PayloadSizeLimit)> {
        Some((self.commit_each.as_str(), self.max_commit_size))
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
    /// The case-preserving, dot-separated catalog namespace of a table.
    pub struct IcebergNamespace => iceberg_namespace_rule,
        label "iceberg_namespace", example "Analytics.Events";
}

fn iceberg_namespace_rule(text: &str) -> Result<(), super::external_name::ExternalNameError> {
    super::external_name::external_identifier(text)?;
    if text.split('.').any(str::is_empty) {
        return Err(super::external_name::ExternalNameError::EmptyLevel { separator: '.' });
    }
    Ok(())
}

impl IcebergNamespace {
    /// Catalog levels, preserving each level's case and spelling.
    pub fn levels(&self) -> impl Iterator<Item = &str> {
        self.as_str().split('.')
    }
}

#[cfg(test)]
#[path = "iceberg_tests.rs"]
mod tests;
