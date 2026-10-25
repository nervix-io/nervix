//! The PostgreSQL connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The PostgreSQL client Model with its connection pool, the table emitter Model with
//!   its conflict action, and what it declares.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The PostgreSQL driver, its configuration keys, or how rows are inserted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::time::Duration;

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

configured_client!(Postgres: pooled, config NervixWithAddressQuery, CreateClientPostgres => PostgresConfigEntry);

pub type PostgresValueMapping = EmitterValueMapping;

/// One table a PostgreSQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PostgresTableEmitter {
    pub mode: PostgresTablePublishingMode,
    pub client: ClientName,
    pub table: PostgresTableName,
    pub values: Vec<PostgresValueMapping>,
    pub conflict_action: PostgresConflictAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum PostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

/// What the connector of a POSTGRES TABLE holds and how long it waits.
const POSTGRES_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[
        DeclaredWait {
            wait: "a pool acquire: name resolution, connection, TLS and authentication",
            bound: WaitBound::Fixed(Duration::from_secs(30)),
        },
        DeclaredWait {
            wait: "an insert and its metadata query",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &[],
};

impl EmitterDeclaration for PostgresTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Postgres),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: PostgresTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(PostgresTableName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "ON CONFLICT",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Choice(&[
                OptionChoice {
                    name: "DO NOTHING",
                    value: None,
                    example: "ON CONFLICT DO NOTHING",
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
        bounds: POSTGRES_BOUNDS,
    };
}

impl EmitterEntityBody for PostgresTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Postgres,
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
    connector Postgres = "POSTGRES";
    /// What a PostgreSQL emitter writes.
    pub enum PostgresEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum PostgresTableEmitterMode;
    /// The entities a PostgreSQL emitter writes.
    pub enum PostgresEmitterEntity {
        Table(PostgresTableEmitter, PostgresTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum PostgresTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct PostgresTableName => super::external_name::external_identifier,
        label "postgres_table", example "Orders";
}
