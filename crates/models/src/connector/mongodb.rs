//! The MongoDB connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MongoDB client Model with its connection pool, the collection emitter Model with
//!   its conflict action, native BSON mapping, field-name admission, collection namespace budget
//!   and what it declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The MongoDB driver, or how documents are written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use meticulous::OptionExt as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{
        BufferBound, BufferCapacity, BufferOverflow, DeclaredBounds, DeclaredBuffer, DeclaredWait,
        WaitBound,
    },
    configured_client::configured_client,
    entity_union::connector_emitters,
    external_name::{ExternalNameError, require_length},
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
            K::required("addr", V::DriverAddress),
            K::optional("database", V::String),
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

pub type MongoDbValueMapping = EmitterValueMapping;

impl MongoDbCollectionEmitter {
    /// BSON encodes a field name as a NUL-terminated string. Other characters retain their
    /// external spelling; server-side query and index restrictions are separate contracts.
    pub fn admits_value_key(&self, key: &str) -> bool {
        !key.contains('\0')
    }
}

/// The BSON document limit, including an identifier the driver or server adds.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the BSON document limit enforced by MongoDB is 16 MiB"
    )
)]
pub const MONGODB_MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;

/// The public Nervix collection contract bounds database, separator and collection together.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the Plexus MONGODB COLLECTION namespace contract fixes its UTF-8 budget at 120 \
                  bytes"
    )
)]
pub const MONGODB_NAMESPACE_MAX_BYTES: usize = 120;

// A database is nonempty and its separator is one byte.
const MAX_COLLECTION_BYTES: usize = MONGODB_NAMESPACE_MAX_BYTES - 2;
const _: () = assert!(MAX_COLLECTION_BYTES > 0);

/// One collection a MongoDB client writes documents into.
///
/// Its direct-value contract is the native BSON mapping below. Nulls retain their type's null
/// position, including within sequences. Other mapped types are refused by the column contract.
///
/// | Plexus type | BSON representation |
/// | --- | --- |
/// | BOOL | Boolean |
/// | I8, I16, I32, U8, U16 | Int32 |
/// | I64, U32 | Int64 |
/// | U64 | Int64 up to i64::MAX; otherwise a permanent row rejection |
/// | F32, F64 | Double, including non-finite values |
/// | STRING | String |
/// | BYTES | Generic Binary |
/// | DATETIME | Date, rounded down to whole Unix milliseconds |
/// | ARRAY, VEC | Array, recursively following this table |
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
            wait: "the handshake, a pool wait, the collection lookup and a write",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["A document above the declared BSON document limit is rejected."],
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

/// Why an `ON CONFLICT` clause cannot be written for a MongoDB collection's `VALUES`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "conflict-clause validation returns pure declaration refusals"
    )
)]
pub enum MongoDbConflictError {
    #[error("MongoDB ON CONFLICT target column '{column}' is not mapped in VALUES")]
    UnmappedTarget { column: String },
    #[error(
        "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict VALUES field to update"
    )]
    NothingToUpdate,
}

impl MongoDbConflictAction {
    /// Checks this clause against the fields its write maps: every target field is mapped, and
    /// `DO UPDATE` leaves a mapped field outside its target to update.
    pub fn validate(&self, values: &[MongoDbValueMapping]) -> Result<(), MongoDbConflictError> {
        let target = match self {
            Self::None => return Ok(()),
            Self::DoNothing { target } | Self::DoUpdate { target } => target,
        };
        let mapped: std::collections::BTreeSet<&str> = values
            .iter()
            .map(|mapping| mapping.column.as_str())
            .collect();
        for column in target {
            if !mapped.contains(column.as_str()) {
                return Err(MongoDbConflictError::UnmappedTarget {
                    column: column.clone(),
                });
            }
        }
        if let Self::DoUpdate { target } = self {
            let targeted: std::collections::BTreeSet<&str> =
                target.iter().map(String::as_str).collect();
            let updates_a_field = values
                .iter()
                .any(|mapping| !targeted.contains(mapping.column.as_str()));
            if !updates_a_field {
                return Err(MongoDbConflictError::NothingToUpdate);
            }
        }
        Ok(())
    }
}

/// One change an `ALTER MONGODB COLLECTION EMITTER` writes to the body of the emitter it changes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MongoDbCollectionEmitterChange {
    /// `SET COLLECTION '<collection>'`.
    Collection(MongoDbCollectionName),
    /// `SET ON CONFLICT … DO NOTHING|UPDATE`, or `DROP ON CONFLICT` where the action is the
    /// ordinary insert a statement that writes no `ON CONFLICT` makes.
    OnConflict(MongoDbConflictAction),
    /// `SET MODE <mode>`.
    Mode(MongoDbCollectionPublishingMode),
}

impl crate::EmitterEntityChange for MongoDbCollectionEmitterChange {
    type Entity = MongoDbCollectionEmitter;

    fn apply_to(
        &self,
        entity: &mut MongoDbCollectionEmitter,
    ) -> error_stack::Result<(), crate::AlterEmitterError> {
        match self {
            Self::Collection(collection) => entity.collection = collection.clone(),
            Self::OnConflict(action) => {
                if *action == MongoDbConflictAction::None
                    && entity.conflict_action == MongoDbConflictAction::None
                {
                    return Err(error_stack::Report::new(
                        crate::AlterEmitterError::OptionNotWritten {
                            entity: <MongoDbCollectionEmitter as crate::EmitterEntityModel>::KIND,
                            option: "ON CONFLICT",
                        },
                    ));
                }
                action.validate(&entity.values).map_err(|error| {
                    error_stack::Report::new(error).change_context(
                        crate::AlterEmitterError::ConflictAction {
                            entity: <MongoDbCollectionEmitter as crate::EmitterEntityModel>::KIND,
                        },
                    )
                })?;
                entity.conflict_action = action.clone();
            }
            Self::Mode(mode) => entity.mode = mode.clone(),
        }
        Ok(())
    }

    fn to_canonical_plexus(&self) -> error_stack::Result<String, crate::CanonicalPlexusError> {
        let text = match self {
            Self::Collection(collection) => format!(
                "SET COLLECTION {}",
                crate::string_literal(collection.as_str())
            ),
            Self::OnConflict(action) => match action.conflict_clause() {
                Some(clause) => format!("SET {clause}"),
                None => "DROP ON CONFLICT".to_owned(),
            },
            Self::Mode(mode) => format!("SET MODE {}", mode.publishing().to_canonical_plexus()),
        };
        Ok(text)
    }
}

connector_emitters! {
    connector MongoDb = "MONGODB";
    /// What a MongoDB emitter writes.
    pub enum MongoDbEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum MongoDbCollectionEmitterMode;
    /// One change to the body of one of this connector's emitter entities.
    pub enum MongoDbEmitterChange;
    /// The entities a MongoDB emitter writes.
    pub enum MongoDbEmitterEntity {
        Collection(MongoDbCollectionEmitter, MongoDbCollectionPublishingMode, MongoDbCollectionEmitterChange) = "COLLECTION";
    }
}

super::publishing::emitter_modes! {
    pub enum MongoDbCollectionPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external collection identifier this connector publishes to.
    pub struct MongoDbCollectionName => mongodb_collection_rule,
        label "mongodb_collection", example "Orders";
}

fn mongodb_collection_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, MAX_COLLECTION_BYTES)?;
    if let Some(ch) = text.chars().find(|ch| matches!(ch, '\0' | '$')) {
        return Err(ExternalNameError::InvalidChar { ch });
    }
    if text.starts_with("system.") || text.contains(".system.") {
        return Err(ExternalNameError::Reserved {
            name: "a system collection",
        });
    }
    Ok(())
}

impl MongoDbCollectionName {
    /// Check the complete namespace after the client's database has been resolved.
    pub fn validate_namespace(&self, database: &str) -> error_stack::Result<(), ExternalNameError> {
        if database.is_empty() {
            return Err(error_stack::Report::new(ExternalNameError::Empty));
        }
        let bytes = database
            .len()
            .checked_add(1)
            .assured("a Rust string is at most isize::MAX bytes before adding its separator");
        let bytes = bytes.checked_add(self.as_str().len()).assured(
            "the database fits isize::MAX and the validated collection holds at most 118 bytes, \
             so their namespace fits usize",
        );
        if bytes > MONGODB_NAMESPACE_MAX_BYTES {
            return Err(error_stack::Report::new(ExternalNameError::TooLong {
                max: MONGODB_NAMESPACE_MAX_BYTES,
                actual: bytes,
            }));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
