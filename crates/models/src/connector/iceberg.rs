//! The Iceberg connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The table sink Model with its storage backend, catalog and commit policy, the
//!   object-store and catalog client Models it writes and commits through, and what the sink
//!   declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The Iceberg, object-store or catalog drivers, their configuration keys, or
//!   how data files are staged and committed.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use strum::AsRefStr;

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, OptionValue, SinkBody, SinkClientReference, SinkEntity,
    SinkReference, SinkValueMapping, SinkValueMappings, TableName,
};

configured_client!(S3: unpooled, config NervixWithPropertyNamespaces, CreateClientS3 => S3ConfigEntry);
configured_client!(Gcs: unpooled, config NervixWithPropertyNamespaces, CreateClientGcs => GcsConfigEntry);
configured_client!(AzureBlob: unpooled, config NervixWithPropertyNamespaces, CreateClientAzureBlob => AzureBlobConfigEntry);
configured_client!(IcebergRest: unpooled, config NervixWithPropertyNamespaces, CreateClientIcebergRest => IcebergRestConfigEntry);

pub type IcebergValueMapping = SinkValueMapping;

/// One Iceberg table, whose data files an object-store client writes and whose snapshots a
/// catalog client commits.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct IcebergTableSink {
    pub backend: IcebergStorageBackend,
    /// The object-store client, which configures the connector `backend` names.
    pub client: ClientName,
    pub table: TableName,
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
    pub const fn client_reference(&self) -> SinkClientReference<'_> {
        match self {
            Self::Rest { client } => SinkClientReference {
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

impl SinkEntity for IcebergTableSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::RequestAck],
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::Unquoted {
                label: "table_name",
            },
        }),
        options: &[
            OptionDeclaration {
                keywords: "ON",
                presence: OptionPresence::Required,
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: "S3",
                        value: Some(OptionValue::Client(ConnectorKind::S3)),
                        example: "ON S3",
                    },
                    OptionChoice {
                        name: "GCS",
                        value: Some(OptionValue::Client(ConnectorKind::Gcs)),
                        example: "ON GCS",
                    },
                    OptionChoice {
                        name: "AZURE_BLOB",
                        value: Some(OptionValue::Client(ConnectorKind::AzureBlob)),
                        example: "ON AZURE_BLOB",
                    },
                ]),
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
    };
}

impl SinkBody for IcebergTableSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: self.backend.connector(),
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn catalog_client(&self) -> Option<SinkClientReference<'_>> {
        Some(self.catalog.client_reference())
    }

    fn value_mappings(&self) -> SinkValueMappings<'_> {
        SinkValueMappings::values(&self.values)
    }

    fn commit_policy(&self) -> Option<(&str, &str)> {
        Some((self.commit_each.as_str(), self.max_commit_size.as_str()))
    }
}

connector_sinks! {
    connector Iceberg = "ICEBERG";
    /// What an Iceberg sink writes.
    pub enum IcebergSink;
    /// The entities an Iceberg sink writes.
    pub enum IcebergSinkEntity {
        Table(IcebergTableSink) = "TABLE";
    }
}
