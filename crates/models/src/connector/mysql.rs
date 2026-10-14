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

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkValueMapping, SinkValueMappings, TableName,
};

configured_client!(MySql: pooled CreateClientMySql => MySqlConfigEntry);

pub type MySqlValueMapping = SinkValueMapping;

/// One table a MySQL client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct MySqlTableSink {
    pub client: ClientName,
    pub table: TableName,
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

impl SinkEntity for MySqlTableSink {
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

impl SinkBody for MySqlTableSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::MySql,
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
    connector MySql = "MYSQL";
    /// What a MySQL sink writes.
    pub enum MySqlSink;
    /// The entities a MySQL sink writes.
    pub enum MySqlSinkEntity {
        Table(MySqlTableSink) = "TABLE";
    }
}
