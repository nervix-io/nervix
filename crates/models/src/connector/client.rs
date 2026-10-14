//! The vocabulary of application sessions as a source and a sink.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The producer source Model with its acknowledgement mode, the consumer sink Model,
//!   and what each entity declares.
//! - **Depends on.** The shared source and sink declarations, the acknowledgement and retry
//!   vocabulary, and the schema name a body references.
//! - **Must not know.** How a session attaches, how a batch is submitted, admitted or delivered,
//!   or which node serves a producer or a consumer.
//!
//! No client Model configures this connector: an application reaches it through its own session.
//! A producer's batches already carry the ingestor's input schema, so its source is held by
//! [`IngestorInput`](crate::IngestorInput) beside the transport contract and never pairs with a
//! codec.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::entity_union::{connector_sinks, connector_source_kinds};
use crate::{
    AckWindow, ClientName, EmitSinkCapabilities, EmitterBatchRequirement, EmitterPublishingMode,
    IngestSourceCapabilities, PositionClass, RetryPolicy, SchemaName, SinkBody, SinkEntity,
    SinkReference, SourceEntity, SourceMetadataScope, SourceReferenceKind,
};

/// Application producers submitting native Arrow batches of one internal schema.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientProducerSource {
    /// The internal schema every submitted batch matches exactly, before route construction.
    pub schema: SchemaName,
    pub mode: ClientIngestMode,
}

/// How many submitted batches may await their acknowledgement across every producer of one
/// client ingestor, how long one waits without progress, and how a producer retries a batch that
/// was refused before admission.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientIngestMode {
    pub window: AckWindow,
    pub ack_timeout: String,
    pub retry_policy: RetryPolicy,
}

impl ClientIngestMode {
    /// The acknowledgement window as a count: one batch for `ACK SEQUENTIAL`, `max` for
    /// `ACK PARALLEL MAX <max>`.
    pub fn window_size(&self) -> NonZeroU64 {
        match self.window {
            AckWindow::Sequential => NonZeroU64::MIN,
            AckWindow::Parallel { max } => max,
        }
    }
}

impl SourceEntity for ClientProducerSource {
    /// A submitted batch carries no transport headers, so its rows expose an empty header list
    /// that no program may read.
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: false,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Schema,
        position_class: PositionClass::None,
    };
}

connector_source_kinds! {
    connector Client = "CLIENT";
    /// The entities a client source reads.
    pub enum ClientSourceEntity {
        Producer(ClientProducerSource) = "PRODUCER", "Client batches";
    }
}

/// Application consumers receiving this emitter's native Arrow output.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ClientConsumerSink {
    pub schema: SchemaName,
}

impl SinkEntity for ClientConsumerSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: false,
        requires_codec: false,
        batch_requirement: EmitterBatchRequirement::Required,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(mode, EmitterPublishingMode::ClientAck { .. })
    }
}

impl SinkBody for ClientConsumerSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Schema(&self.schema)
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        None
    }
}

connector_sinks! {
    connector Client = "CLIENT";
    /// What a client sink writes.
    pub enum ClientSink;
    /// The entities a client sink writes.
    pub enum ClientSinkEntity {
        Consumer(ClientConsumerSink) = "CONSUMER";
    }
}
