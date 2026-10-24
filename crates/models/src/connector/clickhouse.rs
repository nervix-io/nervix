//! The ClickHouse connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The ClickHouse client Model, the table emitter Model, and what it declares.
//! - **Depends on.** The shared emitter declarations and the names a body references.
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
    entity_union::connector_emitters,
};
use crate::{
    ClientName, ConnectorKind, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EmitterValueMapping, EmitterValueMappings, LocatorDeclaration,
    LocatorName,
};

configured_client!(ClickHouse: unpooled, config Nervix, CreateClientClickHouse => ClickHouseConfigEntry);

pub type ClickHouseValueMapping = EmitterValueMapping;

/// One table a ClickHouse client inserts rows into.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClickHouseTableEmitter {
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

impl EmitterDeclaration for ClickHouseTableEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::ClickHouse),
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

impl EmitterEntityBody for ClickHouseTableEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::ClickHouse,
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
    connector ClickHouse = "CLICKHOUSE";
    /// What a ClickHouse emitter writes.
    pub enum ClickHouseEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum ClickHouseTableEmitterMode;
    /// The entities a ClickHouse emitter writes.
    pub enum ClickHouseEmitterEntity {
        Table(ClickHouseTableEmitter, ClickHouseTablePublishingMode) = "TABLE";
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
