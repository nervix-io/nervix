//! The Sentry connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The Sentry client Model, the event emitter Model, and what it declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared emitter declarations and the names a body references.
//! - **Must not know.** The Sentry envelope protocol or how an event is submitted.

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
    EmitterReferenceKind,
};

configured_client!(Sentry: unpooled, config CLIENT_CONFIG, CreateClientSentry => SentryConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::required("dsn", V::Url(&["http", "https"])).secret(),
            K::TIMEOUT,
            K::TLS_CA,
            K::TLS_CERT,
            K::TLS_KEY,
        ],
        &[],
        &[],
    )
};

/// The events of the Sentry project a client's configuration addresses.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SentryEventEmitter {
    pub mode: SentryEventPublishingMode,
    pub client: ClientName,
}

/// What the connector of a SENTRY EVENT holds and how long it waits.
const SENTRY_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[DeclaredWait {
        wait: "a request: name resolution, connection, TLS and the response headers",
        bound: WaitBound::Key {
            key: DeadlineKey::Timeout,
            omitted: OmittedDeadline::Unbounded,
        },
    }],
    driver_limits: &["An event above 1,000,000 bytes is rejected."],
};

impl EmitterDeclaration for SentryEventEmitter {
    /// A Sentry envelope carries at most one `event` item, so a batch is one event, and only the
    /// codec's batch transformation can say where in that event the members go.
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        headers: None,
        reference: EmitterReferenceKind::Client(ConnectorKind::Sentry),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional { maximum: None },
        requires_batch_transformation: true,
        publishing_modes: SentryEventPublishingMode::KINDS,
        locator: None,
        options: &[],
        envelope: &[],
        properties: &[],
        bounds: SENTRY_BOUNDS,
    };
}

impl EmitterEntityBody for SentryEventEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Sentry,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Sentry = "SENTRY";
    /// What a Sentry emitter writes.
    pub enum SentryEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SentryEventEmitterMode;
    /// The entities a Sentry emitter writes.
    pub enum SentryEmitterEntity {
        Event(SentryEventEmitter, SentryEventPublishingMode) = "EVENT";
    }
}

super::publishing::emitter_modes! {
    pub enum SentryEventPublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}
