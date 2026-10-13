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

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkValueMapping, SinkValueMappings, TableName,
};

configured_client!(Postgres: pooled CreateClientPostgres => PostgresConfigEntry);

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

impl SinkEntity for PostgresTableSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        requires_codec: false,
        batch_requirement: EmitterBatchRequirement::Required,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(mode, EmitterPublishingMode::RequestAck { .. })
    }
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
