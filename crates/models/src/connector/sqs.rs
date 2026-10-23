//! The SQS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The SQS client Model, the queue source and sink Models with their delivery mode
//!   and FIFO grouping, the queue name a source receives with and the rule SQS holds it to, the
//!   message size SQS itself admits, and what each entity declares.
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
    bounds::{DeadlineKey, DeclaredBounds, DeclaredWait, OmittedDeadline, WaitBound},
    configured_client::configured_client,
    entity_union::{connector_sinks, connector_sources},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ByteSizeUnit, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EmitSinkCapabilities, EmitterBatchLegality, EmitterBodyForm, Expression,
    HeaderLayout, HeaderModel, IngestAcknowledgement, IngestQuiesceMode, IngestSourceCapabilities,
    LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration, OptionForm, OptionPresence,
    OptionValue, PayloadSizeLimit, PositionClass, RetryPolicy, SinkBody, SinkClientReference,
    SinkEntity, SinkReference, SinkReferenceKind, SourceBody, SourceEntity, SourceMetadataScope,
    SourceReference, SourceReferenceKind,
};

configured_client!(Sqs: unpooled, config Nervix, CreateClientSqs => SqsConfigEntry);

/// The longest queue name SQS admits, its `.fifo` suffix included.
const SQS_QUEUE_MAX: usize = 80;
/// The suffix the name of every FIFO queue ends in.
const SQS_FIFO_SUFFIX: &str = ".fifo";

external_names! {
    /// An SQS queue: 1 to 80 ASCII letters, digits, `-` and `_`, where the name of a FIFO queue
    /// ends in `.fifo`, as SQS itself requires.
    pub struct SqsQueueName => sqs_queue_rule, label "sqs_queue", example "orders";
}

fn sqs_queue_rule(text: &str) -> Result<(), ExternalNameError> {
    require_length(text, SQS_QUEUE_MAX)?;
    let base = text.strip_suffix(SQS_FIFO_SUFFIX).unwrap_or(text);
    if base.is_empty() {
        return Err(ExternalNameError::EmptyBeforeSuffix {
            suffix: SQS_FIFO_SUFFIX,
        });
    }
    let refused = base
        .chars()
        .find(|ch| !(ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_')));
    match refused {
        Some(ch) => Err(ExternalNameError::InvalidChar { ch }),
        None => Ok(()),
    }
}

/// One queue an SQS client receives from.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SqsQueueSource {
    pub client: ClientName,
    pub queue: SqsQueueName,
    pub instances: NonZeroU64,
    pub mode: SqsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a SQS QUEUE ingestor holds and how long it waits.
const SQS_SOURCE_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, TCP and TLS",
            bound: WaitBound::Driver("3.1 s"),
        },
        DeclaredWait {
            wait: "the queue URL lookup, a receive and a delete",
            bound: WaitBound::Unbounded,
        },
    ],
    driver_limits: &["A request that stalls below 1 byte a second for 5 s is cancelled."],
};

/// What the connector of a SQS QUEUE emitter holds and how long it waits.
const SQS_SINK_BOUNDS: DeclaredBounds = DeclaredBounds {
    buffers: &[],
    waits: &[
        DeclaredWait {
            wait: "connection: name resolution, TCP and TLS",
            bound: WaitBound::Driver("3.1 s"),
        },
        DeclaredWait {
            wait: "the queue URL lookup at start",
            bound: WaitBound::Unbounded,
        },
        DeclaredWait {
            wait: "a send",
            bound: WaitBound::Key {
                key: DeadlineKey::Timeout,
                omitted: OmittedDeadline::Unbounded,
            },
        },
    ],
    driver_limits: &[
        "A batch request holds at most 10 entries and 256 KiB; a record above 256 KiB is rejected.",
    ],
};

impl SourceEntity for SqsQueueSource {
    const CAPABILITIES: IngestSourceCapabilities = IngestSourceCapabilities {
        // An SQS message's attributes: one value per name, in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::LastWins)),
        metadata: SourceMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: SourceReferenceKind::Client(ConnectorKind::Sqs),
        position_class: PositionClass::BrokerRedelivered,
        acknowledgement_confirmation: AcknowledgementConfirmation::Answered,
        delivery: &[DeclaredDelivery::ACK_SEQUENTIAL],
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(SqsQueueName::DECLARATION),
        }),
        options: &[OptionDeclaration::INSTANCES],
        bounds: SQS_SOURCE_BOUNDS,
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
    /// The alterations of the ingestors that read an SQS entity.
    pub enum SqsIngestorAlteration;
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
    pub mode: SqsQueuePublishingMode,
    pub client: ClientName,
    pub queue: SqsQueueName,
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
        // An SQS message's attributes: one value per name, the last written, in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::LastWins)),
        reference: SinkReferenceKind::Client(ConnectorKind::Sqs),
        body: EmitterBodyForm::Codec,
        batch: EmitterBatchLegality::Optional {
            maximum: Some(SQS_MESSAGE_SIZE_MAXIMUM),
        },
        requires_batch_transformation: false,
        publishing_modes: SqsQueuePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(SqsQueueName::DECLARATION),
        }),
        options: &[OptionDeclaration {
            keywords: "FIFO GROUP",
            presence: OptionPresence::Optional { default: None },
            form: OptionForm::Choice(&[
                OptionChoice {
                    name: "<expression>",
                    value: Some(OptionValue::Expression),
                    example: "FIFO GROUP tenant",
                },
                OptionChoice {
                    name: "FROM BRANCH",
                    value: None,
                    example: "FIFO GROUP FROM BRANCH",
                },
            ]),
        }],
        envelope: &[],
        properties: &[],
        bounds: SQS_SINK_BOUNDS,
    };
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
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SqsEmitterMode;
    /// The entities an SQS sink writes.
    pub enum SqsSinkEntity {
        Queue(SqsQueueSink, SqsQueuePublishingMode) = "QUEUE";
    }
}

super::publishing::emitter_modes! {
    pub enum SqsQueuePublishingMode {
        Single { retry_policy: crate::RetryPolicy } => SqsSingle,
        Batch { retry_policy: crate::RetryPolicy } => SqsBatch,
    }
}

impl SqsQueueName {
    /// Whether this name identifies a FIFO queue, whose publication states its group.
    pub fn is_fifo(&self) -> bool {
        self.as_str().ends_with(SQS_FIFO_SUFFIX)
    }
}
