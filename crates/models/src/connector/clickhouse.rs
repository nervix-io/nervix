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

use super::{
    bounds::{DeadlineKey, DeclaredBounds, DeclaredWait, OmittedDeadline, WaitBound},
    configured_client::configured_client,
    entity_union::connector_sinks,
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, LocatorDeclaration, LocatorName, SinkBody, SinkClientReference,
    SinkEntity, SinkReference, SinkReferenceKind, SinkValueMapping, SinkValueMappings, TableName,
};

configured_client!(ClickHouse: unpooled, config Nervix, CreateClientClickHouse => ClickHouseConfigEntry);

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

/// What the connector of a CLICKHOUSE TABLE holds and how long it waits.
const CLICKHOUSE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[DeclaredWait {
        wait: "an insert: connection, TLS, upload and the response",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Unbounded,
        },
    }],
    driver_limits: &[],
};

impl SinkEntity for ClickHouseTableSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        reference: SinkReferenceKind::Client(ConnectorKind::ClickHouse),
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
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: CLICKHOUSE_BOUNDS,
    };
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
