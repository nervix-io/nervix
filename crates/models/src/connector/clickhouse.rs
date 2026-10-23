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
    LocatorDeclaration, LocatorName, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkReferenceKind, SinkValueMapping, SinkValueMappings,
};

configured_client!(ClickHouse: unpooled, config Nervix, CreateClientClickHouse => ClickHouseConfigEntry);

pub type ClickHouseValueMapping = SinkValueMapping;

/// One table a ClickHouse client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClickHouseTableSink {
    pub mode: ClickHouseTablePublishingMode,
    pub client: ClientName,
    pub table: ClickHouseTableName,
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
        headers: None,
        reference: SinkReferenceKind::Client(ConnectorKind::ClickHouse),
        body: EmitterBodyForm::Values,
        batch: EmitterBatchLegality::Required { maximum: None },
        requires_batch_transformation: false,
        publishing_modes: ClickHouseTablePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "TABLE",
            name: LocatorName::External(ClickHouseTableName::DECLARATION),
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

    fn mapped_value_types(&self) -> Option<crate::MappedValueTypes> {
        Some(crate::MappedValueTypes::Columns)
    }

    fn value_mappings(&self) -> SinkValueMappings<'_> {
        SinkValueMappings::values(&self.values)
    }
}

connector_sinks! {
    connector ClickHouse = "CLICKHOUSE";
    /// What a ClickHouse sink writes.
    pub enum ClickHouseSink;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum ClickHouseEmitterMode;
    /// The entities a ClickHouse sink writes.
    pub enum ClickHouseSinkEntity {
        Table(ClickHouseTableSink, ClickHouseTablePublishingMode) = "TABLE";
    }
}

super::publishing::emitter_modes! {
    pub enum ClickHouseTablePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

super::external_name::external_names! {
    /// The case-preserving external table identifier this connector publishes to.
    pub struct ClickHouseTableName => super::external_name::external_identifier,
        label "clickhouse_table", example "Orders";
}
