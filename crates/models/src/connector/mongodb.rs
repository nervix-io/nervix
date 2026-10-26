//! The MongoDB connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MongoDB client Model with its connection pool, the collection emitter Model with
//!   its conflict action, and what it declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The MongoDB driver, or how documents are written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_emitters,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EmitterValueMapping, EmitterValueMappings, LocatorDeclaration,
    LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence,
};

configured_client!(MongoDb: pooled, config CLIENT_CONFIG, CreateClientMongoDb => MongoDbConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("addr", V::Url(&["mongodb", "mongodb+srv"])),
            K::optional("database", V::String),
            K::TLS_CA,
        ],
        &[],
        &[],
    )
};

pub type MongoDbValueMapping = EmitterValueMapping;

/// One collection a MongoDB client writes documents into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MongoDbCollectionEmitter {
    pub mode: MongoDbCollectionPublishingMode,
    pub client: ClientName,
    pub collection: MongoDbCollectionName,
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

/// What the connector of a MONGODB COLLECTION holds and how long it waits.
const MONGODB_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, TCP and TLS",
            bound: WaitBound::Driver("connectTimeoutMS, 10 s"),
        },
        DeclaredWait {
            wait: "server selection",
            bound: WaitBound::Driver("30 s"),
        },
        DeclaredWait {
            wait: "the handshake, a pool wait and a write",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["A document above 16 MiB is rejected."],
};

impl EmitterDeclaration for MongoDbCollectionEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::MongoDb),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: MongoDbCollectionPublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "COLLECTION",
            name: LocatorName::External(MongoDbCollectionName::DECLARATION),
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
        bounds: MONGODB_BOUNDS,
    };
}

impl EmitterEntityBody for MongoDbCollectionEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::MongoDb,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        Some(crate::MappedValueTypes::Columns)
    }

    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings::values(&self.values)
    }
}

connector_emitters! {
    connector MongoDb = "MONGODB";
    /// What a MongoDB emitter writes.
    pub enum MongoDbEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum MongoDbCollectionEmitterMode;
    /// The entities a MongoDB emitter writes.
    pub enum MongoDbEmitterEntity {
        Collection(MongoDbCollectionEmitter, MongoDbCollectionPublishingMode) = "COLLECTION";
    }
}

super::publishing::emitter_modes! {
    pub enum MongoDbCollectionPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external collection identifier this connector publishes to.
    pub struct MongoDbCollectionName => super::external_name::external_identifier,
        label "mongodb_collection", example "Orders";
}
