//! The SQS connector's vocabulary.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The SQS client Model, the queue source and emitter Models with their delivery mode,
//!   request shape and FIFO grouping, the queue name a source receives with and the rule SQS holds
//!   it to, the message size and delay SQS itself admits, the attribute types it carries, and what
//!   each entity declares.
//!   Client declarations also own public configuration keys, types and confidentiality.
//! - **Depends on.** The shared source and emitter declarations, expressions and the names a body
//!   references.
//! - **Must not know.** The SQS driver, or how a queue is received from or
//!   sent to.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{num::NonZeroU64, time::Duration};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{
    bounds::{DeadlineKey, DeclaredBounds, DeclaredWait, OmittedDeadline, WaitBound},
    configured_client::configured_client,
    entity_union::{connector_emitters, connector_ingestors},
    external_name::{ExternalNameError, external_names, require_length},
};
use crate::{
    AcknowledgementConfirmation, ByteSizeUnit, ClientName, ConnectorKind, DeclaredDelivery,
    DeclaredQuiesce, EmitterBatchLegality, EmitterBodyForm, EmitterClientReference,
    EmitterDeclaration, EmitterEntityBody, EmitterEntityCapabilities, EmitterReference,
    EmitterReferenceKind, EnvelopeClauseKind, Expression, HeaderLayout, HeaderModel,
    HeaderValueType, IngestAcknowledgement, IngestQuiesceMode, IngestorDeclaration,
    IngestorEntityBody, IngestorEntityCapabilities, IngestorMetadataScope, IngestorReference,
    IngestorReferenceKind, LocatorDeclaration, LocatorName, OptionChoice, OptionDeclaration,
    OptionForm, OptionPresence, OptionValue, PayloadSizeLimit, PositionClass, PropertyType,
    RetryPolicy,
};

configured_client!(Sqs: unpooled, config CLIENT_CONFIG, CreateClientSqs => SqsConfigEntry);

/// The current keys this connector reads, shared by validation, completion and inspection. An SQS
/// connection trusts `tls_ca_file`, or the platform's roots without it, and presents no client
/// certificate, so the schema declares no client-identity key.
const CLIENT_CONFIG: crate::ClientConfigSchema = {
    use crate::{ClientConfigKey as K, ClientConfigSchema, ClientConfigValue as V};
    ClientConfigSchema::new(
        &[
            K::optional("access_key_id", V::String).secret(),
            K::required("endpoint", V::Url(&["http", "https"])),
            K::optional("region", V::String),
            K::optional("secret_access_key", V::String).secret(),
            K::TIMEOUT,
            K::TLS_CA,
        ],
        &[],
        &[],
    )
};

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
pub struct SqsQueueIngestor {
    pub client: ClientName,
    pub queue: SqsQueueName,
    pub instances: NonZeroU64,
    pub mode: SqsIngestMode,
    pub quiesce: IngestQuiesceMode,
}

/// What the connector of a SQS QUEUE ingestor holds and how long it waits.
const SQS_INGESTOR_BOUNDS: DeclaredBounds = DeclaredBounds {
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
const SQS_EMITTER_BOUNDS: DeclaredBounds = DeclaredBounds {
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
        "A batch request holds at most 10 entries and 1 MiB; a record above 1 MiB is rejected.",
    ],
};

impl IngestorDeclaration for SqsQueueIngestor {
    const CAPABILITIES: IngestorEntityCapabilities = IngestorEntityCapabilities {
        // An SQS message's attributes: one value per name, in no order.
        headers: Some(HeaderModel::exact(HeaderLayout::LastWins)),
        metadata: IngestorMetadataScope::Headers,
        executes_on_every_cluster_node: false,
        reference: IngestorReferenceKind::Client(ConnectorKind::Sqs),
        position_class: PositionClass::BrokerRedelivered,
        acknowledgement_confirmation: AcknowledgementConfirmation::Answered,
        delivery: &[DeclaredDelivery::ACK_SEQUENTIAL],
        // The queue keeps each message until the source deletes it, so suspending loses nothing.
        quiesce: &[DeclaredQuiesce::SUSPEND],
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(SqsQueueName::DECLARATION),
        }),
        options: &[OptionDeclaration::INSTANCES],
        bounds: SQS_INGESTOR_BOUNDS,
    };
}

impl IngestorEntityBody for SqsQueueIngestor {
    fn reference(&self) -> IngestorReference<'_> {
        IngestorReference::Client(&self.client)
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

connector_ingestors! {
    connector Sqs = "SQS";
    /// What an SQS source reads.
    pub enum SqsIngestor;
    /// The alterations of the ingestors that read an SQS entity.
    pub enum SqsQueueIngestorAlteration;
    /// The entities an SQS source reads.
    pub enum SqsIngestorEntity {
        Queue(SqsQueueIngestor) = "QUEUE", "SQS";
    }
}

/// The largest message SQS accepts, 1 MiB counting its body and attributes, which is also the most
/// one `SendMessageBatch` request carries and the largest batch payload.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "SQS accepts a message, and a SendMessageBatch request, of at most 1 MiB"
    )
)]
pub const SQS_MESSAGE_SIZE_MAXIMUM: PayloadSizeLimit =
    match PayloadSizeLimit::new(nonzero_ext::nonzero!(1u64), ByteSizeUnit::MiB) {
        Some(maximum) => maximum,
        None => panic!("1 MiB fits in a 64-bit byte count"),
    };

/// The longest delay SQS gives one message, in the whole seconds `DelaySeconds` counts.
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "SQS DelaySeconds accepts whole seconds from 0 to 900")
)]
pub const SQS_DELAY_MAXIMUM: Duration = Duration::from_secs(900);

/// The types an SQS message attribute's value carries, each under the data type SQS names it by: a
/// string as `String`, an integer as `Number` and bytes as `Binary`.
pub const SQS_ATTRIBUTE_VALUES: &[HeaderValueType] = &[
    HeaderValueType {
        ty: PropertyType::String,
        carried_as: "String",
    },
    HeaderValueType {
        ty: PropertyType::U8,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::I8,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::U16,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::I16,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::U32,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::I32,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::U64,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::I64,
        carried_as: "Number",
    },
    HeaderValueType {
        ty: PropertyType::Bytes,
        carried_as: "Binary",
    },
];

/// One queue an SQS client sends to. The queue is kept as written: words and whole numbers joined
/// by hyphens, with an optional `.fifo` suffix.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct SqsQueueEmitter {
    pub mode: SqsQueuePublishingMode,
    pub client: ClientName,
    pub queue: SqsQueueName,
    pub fifo_group: Option<SqsFifoGroup>,
    pub send: SqsSend,
}

/// The request shape an SQS emitter sends its messages in.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    strum::IntoStaticStr,
)]
pub enum SqsSend {
    /// One `SendMessage` request for each message.
    #[strum(serialize = "SINGLE")]
    Single,
    /// `SendMessageBatch` requests of up to ten messages.
    #[strum(serialize = "BATCH")]
    Batch,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum SqsFifoGroup {
    FromBranch,
    Expression(Expression),
}

impl EmitterDeclaration for SqsQueueEmitter {
    const CAPABILITIES: EmitterEntityCapabilities = EmitterEntityCapabilities {
        // An SQS message's attributes: one value per name, the last written, in no order, each a
        // string, a number or bytes.
        headers: Some(
            HeaderModel::exact(HeaderLayout::LastWins).with_typed_values(SQS_ATTRIBUTE_VALUES),
        ),
        reference: EmitterReferenceKind::Client(ConnectorKind::Sqs),
        body: EmitterBodyForm::Codec,
        // A batch payload is one SQS message, so it fits within the message size SQS admits.
        batch: EmitterBatchLegality::Optional {
            maximum: Some(SQS_MESSAGE_SIZE_MAXIMUM),
        },
        requires_batch_transformation: false,
        publishing_modes: SqsQueuePublishingMode::KINDS,
        locator: Some(LocatorDeclaration {
            keywords: "QUEUE",
            name: LocatorName::External(SqsQueueName::DECLARATION),
        }),
        options: &[
            OptionDeclaration {
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
            },
            OptionDeclaration {
                keywords: "SEND",
                presence: OptionPresence::Required,
                form: OptionForm::Choice(&[
                    OptionChoice {
                        name: "SINGLE",
                        value: None,
                        example: "SEND SINGLE",
                    },
                    OptionChoice {
                        name: "BATCH",
                        value: None,
                        example: "SEND BATCH",
                    },
                ]),
            },
        ],
        // A standard queue takes a per-message delay and a FIFO queue a deduplication id; the
        // registry holds each clause to the queue that takes it.
        envelope: &[
            EnvelopeClauseKind::Delay,
            EnvelopeClauseKind::DeduplicationId,
        ],
        properties: &[],
        bounds: SQS_EMITTER_BOUNDS,
    };
}

impl EmitterEntityBody for SqsQueueEmitter {
    fn reference(&self) -> EmitterReference<'_> {
        EmitterReference::Client(EmitterClientReference {
            client: &self.client,
            connector: ConnectorKind::Sqs,
        })
    }

    fn client_mut(&mut self) -> Option<&mut ClientName> {
        Some(&mut self.client)
    }
}

connector_emitters! {
    connector Sqs = "SQS";
    /// What an SQS emitter writes.
    pub enum SqsEmitter;
    /// A publishing-policy change for one of this connector's emitter entities.
    pub enum SqsQueueEmitterMode;
    /// The entities an SQS emitter writes.
    pub enum SqsEmitterEntity {
        Queue(SqsQueueEmitter, SqsQueuePublishingMode) = "QUEUE";
    }
}

super::publishing::emitter_modes! {
    pub enum SqsQueuePublishingMode {
        Ack { retry_policy: crate::RetryPolicy } => RequestAck,
    }
}

impl SqsQueueName {
    /// Whether this name identifies a FIFO queue, whose publication states its group.
    pub fn is_fifo(&self) -> bool {
        self.as_str().ends_with(SQS_FIFO_SUFFIX)
    }
}

impl SqsSend {
    /// The choice as a statement writes it after `SEND`.
    pub fn keyword(self) -> &'static str {
        self.into()
    }
}

#[cfg(test)]
#[path = "sqs_tests.rs"]
mod tests;
