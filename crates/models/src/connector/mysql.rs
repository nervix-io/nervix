//! The MySQL connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The MySQL client Model with its connection pool, the table sink Model with its
//!   conflict action, and what it declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The MySQL driver, its configuration keys, or how rows are inserted.

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

configured_client!(MySql: pooled, config NervixWithAddressQuery, CreateClientMySql => MySqlConfigEntry);

pub type MySqlValueMapping = EmitterValueMapping;

/// One table a MySQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MySqlTableEmitter {
    pub mode: MySqlTablePublishingMode,
    pub client: ClientName,
    pub table: MySqlTableName,
    pub values: Vec<MySqlValueMapping>,
    pub conflict_action: MySqlConflictAction,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum MySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

/// What the connector of a MYSQL TABLE holds and how long it waits.
const MYSQL_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[DeclaredBuffer {
        holds: "connections the pool holds",
        bound: BufferBound::Bounded {
            capacity: BufferCapacity::Setting("POOL SIZE MAX"),
            overflow: BufferOverflow::Backpressure,
        },
    }],
    waits: &[DeclaredWait {
        wait: "connection, its handshake and an insert",
        bound: WaitBound::Unbounded,
    }],
    driver_limits: &["The driver has no connect, read or write timeout."],
};

impl EmitterDeclaration for MySqlTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::MySql),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: MySqlTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(MySqlTableName::DECLARATION),
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
                    example: "ON CONFLICT DO UPDATE",
                },
            ]),
        }],
        envelope: &[],
        properties: &[],
        bounds: MYSQL_BOUNDS,
    };
}

impl EmitterEntityBody for MySqlTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::MySql,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }

    fn value_mappings(&self) -> EmitterValueMappings<'_> {
        EmitterValueMappings::values(&self.values)
    }
}

connector_emitters! {
    connector MySql = "MYSQL";
    /// What a MySQL sink writes.
    pub enum MySqlEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum MySqlEmitterMode;
    /// The entities a MySQL sink writes.
    pub enum MySqlEmitterEntity {
        Table(MySqlTableEmitter, MySqlTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum MySqlTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct MySqlTableName => super::external_name::external_identifier,
        label "mysql_table", example "Orders";
}
