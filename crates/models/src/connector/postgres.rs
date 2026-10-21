//! The PostgreSQL connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The PostgreSQL client Model with its connection pool, the table sink Model with
//!   its conflict action, and what it declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
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
    entity_union::connector_sinks,
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkReferenceKind, SinkValueMapping, SinkValueMappings, TableName,
};

configured_client!(Postgres: pooled, config NervixWithAddressQuery, CreateClientPostgres => PostgresConfigEntry);

pub type PostgresValueMapping = SinkValueMapping;

/// One table a PostgreSQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct PostgresTableSink {
    pub client: ClientName,
    pub table: TableName,
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

impl SinkEntity for PostgresTableSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        reference: SinkReferenceKind::Client(ConnectorKind::Postgres),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: &[EmitterPublishingModeKind::RequestAck],
        locator: Some(LocatorDeclaration {
            keywords: "INSERT TO TABLE",
            name: LocatorName::Unquoted {
                label: "table_name",
            },
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

impl SinkBody for PostgresTableSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Postgres,
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
    connector Postgres = "POSTGRES";
    /// What a PostgreSQL sink writes.
    pub enum PostgresSink;
    /// The entities a PostgreSQL sink writes.
    pub enum PostgresSinkEntity {
        Table(PostgresTableSink) = "TABLE";
    }
}
