//! The Sentry connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Sentry client Model, the event sink Model, and what it declares.
//! - **Depends on.** The shared sink declarations and the names a body references.
//! - **Must not know.** The Sentry envelope protocol, its configuration keys, or how an event is
//!   submitted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{configured_client::configured_client, entity_union::connector_sinks};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm,
    EmitterPublishingModeKind, SinkBody, SinkClientReference, SinkEntity, SinkReference,
    SinkReferenceKind,
};

configured_client!(Sentry: unpooled, config Nervix, CreateClientSentry => SentryConfigEntry);

/// The events of the Sentry project a client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SentryEventSink {
    pub client: ClientName,
}

impl SinkEntity for SentryEventSink {
    /// A Sentry envelope carries at most one `event` item, so a batch is one event, and only the
    /// codec's batch transformation can say where in that event the members go.
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        reference: SinkReferenceKind::Client(ConnectorKind::Sentry),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: true,
        publishing_modes: &[EmitterPublishingModeKind::RequestAck],
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
    };
}

impl SinkBody for SentryEventSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Sentry,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Sentry = "SENTRY";
    /// What a Sentry sink writes.
    pub enum SentrySink;
    /// The entities a Sentry sink writes.
    pub enum SentrySinkEntity {
        Event(SentryEventSink) = "EVENT";
    }
}
