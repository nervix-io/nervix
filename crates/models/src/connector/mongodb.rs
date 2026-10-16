//! The MongoDB connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MongoDB client Model with its connection pool, the collection sink Model with
//!   its conflict action, and what it declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The MongoDB driver, its configuration keys, or how documents are written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, CollectionName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality,
    EmitterBodyForm, EmitterPublishingModeKind, LocatorDeclaration, LocatorName, OptionChoice,
    OptionDeclaration, OptionForm, OptionPresence, SinkBody, SinkClientReference, SinkEntity,
    SinkReference, SinkValueMapping, SinkValueMappings,
};

configured_client!(MongoDb: pooled, config NervixWithAddressQuery, CreateClientMongoDb => MongoDbConfigEntry);

pub type MongoDbValueMapping = SinkValueMapping;

/// One collection a MongoDB client writes documents into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MongoDbCollectionSink {
    pub client: ClientName,
    pub collection: CollectionName,
    pub values: Vec<MongoDbValueMapping>,
    pub conflict_action: MongoDbConflictAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MongoDbConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

impl SinkEntity for MongoDbCollectionSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::RequestAck],
        locator: Some(LocatorDeclaration {
            keywords: "INSERT TO COLLECTION",
            name: LocatorName::Unquoted {
                label: "collection_name",
            },
        }),
        options: &[OptionDeclaration {
            keywords: "ON CONFLICT",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Choice(&[
                OptionChoice {
                    name: "DO NOTHING",
                    value: None,
                    example: "ON CONFLICT ('id') DO NOTHING",
                },
                OptionChoice {
                    name: "DO UPDATE",
                    value: None,
                    example: "ON CONFLICT ('id') DO UPDATE",
                },
            ]),
        }],
        envelope: &[],
        properties: &[],
    };
}

impl SinkBody for MongoDbCollectionSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::MongoDb,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn value_mappings(&self) -> SinkValueMappings<'_> {
        SinkValueMappings::values(&self.values)
    }
}

connector_sinks! {
    connector MongoDb = "MONGODB";
    /// What a MongoDB sink writes.
    pub enum MongoDbSink;
    /// The entities a MongoDB sink writes.
    pub enum MongoDbSinkEntity {
        Collection(MongoDbCollectionSink) = "COLLECTION";
    }
}
