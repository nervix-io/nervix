//! The SQS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The SQS client Model, the queue source and sink Models with their delivery mode
//!   and FIFO grouping, the message size SQS itself admits, and what each entity declares.
//! - **Depends on.** The shared source and sink declarations, expressions and the names a body
//!   references.
//! - **Must not know.** The SQS driver, its configuration keys, or how a queue is received from or
//!   sent to.

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
    ByteSizeUnit, ClientName, ConnectorKind, EmitSinkCapabilities, EmitterBatchRequirement,
    EmitterPublishingMode, Expression, IngestAcknowledgement, IngestQuiesceMode,
    IngestSourceCapabilities, PayloadSizeLimit, QueueName, RetryPolicy, SinkBody,
    SinkClientReference, SinkEntity, SinkReference, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(Sqs: unpooled CreateClientSqs => SqsConfigEntry);

/// One queue an SQS client receives from.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SqsQueueSource {
    pub client: ClientName,
    pub queue: QueueName,
    pub instances: NonZeroU64,
    pub mode: SqsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

impl SourceEntity for SqsQueueSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        reads_headers: true,
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Sqs),
    };
}

impl SourceBody for SqsQueueSource {
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
pub enum SqsIngestMode {
    AckSequential {
        timeout: String,
        retry_policy: RetryPolicy,
    },
}

impl SqsIngestMode {
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
    connector Sqs = "SQS";
    /// What an SQS source reads.
    pub enum SqsSource;
    /// The entities an SQS source reads.
    pub enum SqsSourceEntity {
        Queue(SqsQueueSource) = "QUEUE", "SQS";
    }
}

/// The SQS message size limit Nervix enforces, 256 KiB, which a batch payload cannot exceed.
const SQS_MESSAGE_SIZE_MAXIMUM: PayloadSizeLimit =
    match PayloadSizeLimit::new(nonzero_ext::nonzero!(256u64), ByteSizeUnit::KiB) {
        Some(maximum) => maximum,
        None => panic!("256 KiB fits in a 64-bit byte count"),
    };

/// One queue an SQS client sends to. The queue is kept as written: words and whole numbers joined
/// by hyphens, with an optional `.fifo` suffix.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SqsQueueSink {
    pub client: ClientName,
    pub queue: String,
    pub fifo_group: Option<SqsFifoGroup>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SqsFifoGroup {
    FromBranch,
    Expression(Expression),
}

impl SinkEntity for SqsQueueSink {
    const CAPABILITIES: EmitSinkCapabilities = EmitSinkCapabilities {
        writes_headers: true,
        requires_codec: true,
        batch_requirement: EmitterBatchRequirement::Optional,
        requires_batch_transformation: false,
        batch_size_maximum: Some(SQS_MESSAGE_SIZE_MAXIMUM),
    };

    fn accepts_publishing_mode(mode: &EmitterPublishingMode) -> bool {
        matches!(
            mode,
            EmitterPublishingMode::SqsSingle { .. } | EmitterPublishingMode::SqsBatch { .. }
        )
    }
}

impl SinkBody for SqsQueueSink {
    fn reference(&self) -> SinkReference<'_> {
        SinkReference::Client(SinkClientReference {
            client: &self.client,
            connector: ConnectorKind::Sqs,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_sinks! {
    connector Sqs = "SQS";
    /// What an SQS sink writes.
    pub enum SqsSink;
    /// The entities an SQS sink writes.
    pub enum SqsSinkEntity {
        Queue(SqsQueueSink) = "QUEUE";
    }
}
