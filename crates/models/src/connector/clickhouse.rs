//! The ClickHouse connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ClickHouse client Model, the table sink Model, and what it declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The ClickHouse driver, its configuration keys, or how rows are inserted.

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

configured_client!(ClickHouse: unpooled CreateClientClickHouse => ClickHouseConfigEntry);

pub type ClickHouseValueMapping = SinkValueMapping;

/// One table a ClickHouse client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClickHouseTableSink {
    pub client: ClientName,
    pub table: TableName,
    pub values: Vec<ClickHouseValueMapping>,
}

impl SinkEntity for ClickHouseTableSink {
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

impl SinkBody for ClickHouseTableSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::ClickHouse,
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
    connector ClickHouse = "CLICKHOUSE";
    /// What a ClickHouse sink writes.
    pub enum ClickHouseSink;
    /// The entities a ClickHouse sink writes.
    pub enum ClickHouseSinkEntity {
        Table(ClickHouseTableSink) = "TABLE";
    }
}
