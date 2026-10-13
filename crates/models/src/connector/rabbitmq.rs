//! The RabbitMQ connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The RabbitMQ client Model, the queue source and sink Models with their delivery
//!   mode, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations and the names a body references.
//! - **Must not know.** The AMQP driver, its configuration keys, or how a queue is consumed or
//!   published to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::num::NonZeroU64;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
};
use crate::{
    ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    QueueName, RetryPolicy, SinkBody, SinkEntity, SinkReference, SourceBody, SourceEntity,
    SourceMetadataScope, SourceReference, SourceReferenceKind,
};

configured_client!(RabbitMq: unpooled CreateClientRabbitMq => RabbitMqConfigEntry);

/// One queue a RabbitMQ client consumes.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueSource {
    pub client: ClientName,
    pub queue: QueueName,
    pub instances: NonZeroU64,
    pub mode: RabbitMqIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for RabbitMqQueueSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::RabbitMq),
    };
}

impl SourceBody for RabbitMqQueueSource {
    fn reference(&self) -> SourceReference<'_> {
        SourceReference::Client(&self.client)
    }

    fn quiesce(&self) -> &IngestQuiesceMode {
        &self.quiesce
    }

    fn quiesce_mut(&mut self) -> &mut IngestQuiesceMode {
        &mut self.quiesce
    }

    fn supports_quiesce(&self, quiesce: &IngestQuiesceMode) -> bool {
        matches!(quiesce, IngestQuiesceMode::Suspend)
    }

    fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        self.mode.acknowledgement()
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RabbitMqIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl RabbitMqIngestMode {
    pub fn acknowledgement(&self) -> IngestAcknowledgement<'_> {
        match self {
            Self::AckSequential {
                timeout,
                retry_policy,
            } => IngestAcknowledgement::Sequential {
                timeout,
                retry: retry_policy,
            },
        }
    }
}

connector_sources! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ source reads.
    pub enum RabbitMqSource;
    /// The entities a RabbitMQ source reads.
    pub enum RabbitMqSourceEntity {
        Queue(RabbitMqQueueSource) = "QUEUE", "RabbitMQ";
    }
}

/// One queue a RabbitMQ client publishes to.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RabbitMqQueueSink {
    pub client: ClientName,
    pub queue: QueueName,
}

impl SinkEntity for RabbitMqQueueSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: true,
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: None,
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(
            mode,
            EmitterPublishingMode::NoAck { .. } | EmitterPublishingMode::BrokerAck { .. }
        )
    }
}

impl SinkBody for RabbitMqQueueSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client {
            client: &self.client,
            connector: ConnectorKind::RabbitMq,
        }
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector RabbitMq = "RABBITMQ";
    /// What a RabbitMQ sink writes.
    pub enum RabbitMqSink;
    /// The entities a RabbitMQ sink writes.
    pub enum RabbitMqSinkEntity {
        Queue(RabbitMqQueueSink) = "QUEUE";
    }
}
