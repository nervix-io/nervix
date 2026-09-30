//! Session frames for native client emitter consumers.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The verified, bounded request and reply shapes for opening a consumer, reading one
//!   Arrow delivery, settling its current attempt, and closing the consumer.
//! - **Depends on.** The shared session schema, frame limits, row fields, and vocabulary values.
//! - **Must not know.** Emitter tasks, sessions, assignment, or the application processing loop.

use std::{fmt, num::NonZeroU32, time::Duration};

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{
    AckWindow, ClientConsumerLimits, ClientEndpointContract, DomainName, EmitterName, RelayName,
    SchemaField, Timestamp,
};
use uuid::Uuid;

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::RequestId,
    frame::{ServerFrame, VerifiedFrame},
    row::{decode_field, encode_field},
    wire,
};

const REQUEST_FIELD_DEPTH: usize = 3;
const OPENED_FIELD_DEPTH: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConsumerId(RequestId);

impl ConsumerId {
    pub const fn opened_by(open: RequestId) -> Self {
        Self(open)
    }
    pub const fn open_request(self) -> RequestId {
        self.0
    }
    fn wire(self) -> u64 {
        self.0.wire()
    }
    fn decode(
        decoder: Decoder<'_>,
        field: &'static str,
        value: u64,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self(RequestId::decode(decoder, field, value)?))
    }
}

impl fmt::Display for ConsumerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenEmitterRequest {
    pub domain: DomainName,
    pub emitter: EmitterName,
    pub expected_fields: Vec<SchemaField>,
    pub limits: ClientConsumerLimits,
}

impl OpenEmitterRequest {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::OpenEmitterRequest<'fbb>>, Report<WireEncodeError>> {
        if self.expected_fields.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "OpenEmitterRequest.expected_fields",
            }));
        }
        let domain = encoder.text("OpenEmitterRequest.domain", self.domain.as_str())?;
        let emitter = encoder.text("OpenEmitterRequest.emitter", self.emitter.as_str())?;
        let expected_fields = encoder.table_vector(
            "OpenEmitterRequest.expected_fields",
            &self.expected_fields,
            |field, encoder| encode_field(field, encoder, REQUEST_FIELD_DEPTH),
        )?;
        Ok(wire::OpenEmitterRequest::create(
            encoder.fbb(),
            &wire::OpenEmitterRequestArgs {
                domain: Some(domain),
                emitter: Some(emitter),
                expected_fields: Some(expected_fields),
                max_outstanding_batches: self.limits.batches.get(),
                max_outstanding_bytes: self.limits.bytes.get(),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        open: wire::OpenEmitterRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let domain = decoder.name("OpenEmitterRequest.domain", open.domain())?;
        let emitter = decoder.name("OpenEmitterRequest.emitter", open.emitter())?;
        let expected_fields = decoder.table_vector(
            "OpenEmitterRequest.expected_fields",
            open.expected_fields(),
            |field| decode_field(decoder, field),
        )?;
        if expected_fields.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "OpenEmitterRequest.expected_fields",
            }));
        }
        let Some(batches) = NonZeroU32::new(open.max_outstanding_batches()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "OpenEmitterRequest.max_outstanding_batches",
            }));
        };
        let bytes = decoder.non_zero(
            "OpenEmitterRequest.max_outstanding_bytes",
            open.max_outstanding_bytes(),
        )?;
        Ok(Self {
            domain,
            emitter,
            expected_fields,
            limits: ClientConsumerLimits { batches, bytes },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadEmitterBatchRequest {
    pub consumer: ConsumerId,
}
impl ReadEmitterBatchRequest {
    pub(crate) fn encode<'fbb>(
        self,
        encoder: &mut Encoder<'fbb>,
    ) -> WIPOffset<wire::ReadEmitterBatchRequest<'fbb>> {
        wire::ReadEmitterBatchRequest::create(
            encoder.fbb(),
            &wire::ReadEmitterBatchRequestArgs {
                consumer: self.consumer.wire(),
            },
        )
    }
    pub(crate) fn decode(
        decoder: Decoder<'_>,
        request: wire::ReadEmitterBatchRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            consumer: ConsumerId::decode(
                decoder,
                "ReadEmitterBatchRequest.consumer",
                request.consumer(),
            )?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitterBatchDecision {
    Ack,
    Retry,
    Reject(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleEmitterBatchRequest {
    pub consumer: ConsumerId,
    pub reference: Uuid,
    pub decision: EmitterBatchDecision,
}

impl SettleEmitterBatchRequest {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::SettleEmitterBatchRequest<'fbb>>, Report<WireEncodeError>> {
        let reference = encoder.bytes(
            "SettleEmitterBatchRequest.reference",
            self.reference.as_bytes(),
        )?;
        let (decision, reason) = match &self.decision {
            EmitterBatchDecision::Ack => (wire::EmitterBatchDecision::Ack, ""),
            EmitterBatchDecision::Retry => (wire::EmitterBatchDecision::Retry, ""),
            EmitterBatchDecision::Reject(reason) => {
                (wire::EmitterBatchDecision::Reject, reason.as_str())
            }
        };
        if matches!(self.decision, EmitterBatchDecision::Reject(_))
            && (reason.is_empty() || reason.len() > 1024)
        {
            return Err(Report::new(WireEncodeError::InvalidValue {
                field: "SettleEmitterBatchRequest.reason",
                kind: "bounded nonempty rejection reason",
            }));
        }
        let reason = encoder.text("SettleEmitterBatchRequest.reason", reason)?;
        Ok(wire::SettleEmitterBatchRequest::create(
            encoder.fbb(),
            &wire::SettleEmitterBatchRequestArgs {
                consumer: self.consumer.wire(),
                reference: Some(reference),
                decision: Some(decision),
                reason: Some(reason),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        request: wire::SettleEmitterBatchRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let consumer = ConsumerId::decode(
            decoder,
            "SettleEmitterBatchRequest.consumer",
            request.consumer(),
        )?;
        let reference = uuid_from_bytes(
            "SettleEmitterBatchRequest.reference",
            request.reference().bytes(),
        )?;
        let reason = decoder.text("SettleEmitterBatchRequest.reason", request.reason())?;
        let decision = match request.decision() {
            Some(wire::EmitterBatchDecision::Ack) if reason.is_empty() => EmitterBatchDecision::Ack,
            Some(wire::EmitterBatchDecision::Retry) if reason.is_empty() => {
                EmitterBatchDecision::Retry
            }
            Some(wire::EmitterBatchDecision::Reject)
                if !reason.is_empty() && reason.len() <= 1024 =>
            {
                EmitterBatchDecision::Reject(reason)
            }
            _ => {
                return Err(Report::new(WireDecodeError::InvalidValue {
                    field: "SettleEmitterBatchRequest.decision",
                    kind: "decision with its required bounded reason",
                }));
            }
        };
        Ok(Self {
            consumer,
            reference,
            decision,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseEmitterRequest {
    pub consumer: ConsumerId,
}
impl CloseEmitterRequest {
    pub(crate) fn encode<'fbb>(
        self,
        encoder: &mut Encoder<'fbb>,
    ) -> WIPOffset<wire::CloseEmitterRequest<'fbb>> {
        wire::CloseEmitterRequest::create(
            encoder.fbb(),
            &wire::CloseEmitterRequestArgs {
                consumer: self.consumer.wire(),
            },
        )
    }
    pub(crate) fn decode(
        decoder: Decoder<'_>,
        request: wire::CloseEmitterRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            consumer: ConsumerId::decode(
                decoder,
                "CloseEmitterRequest.consumer",
                request.consumer(),
            )?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterOpenRefusal {
    DomainNotFound,
    DomainStopped,
    EmitterNotFound,
    NotClientEmitter,
    EndpointUnavailable,
    SchemaMismatch,
    TooManyConsumers,
    SessionCapacityExhausted,
    NodeCapacityExhausted,
    InvalidLimits,
    InTransaction,
}

wire_enum!(ALL_EMITTER_OPEN_REFUSALS: EmitterOpenRefusal => wire::EmitterOpenRefusal {
    DomainNotFound, DomainStopped, EmitterNotFound, NotClientEmitter,
    EndpointUnavailable, SchemaMismatch, TooManyConsumers,
    SessionCapacityExhausted, NodeCapacityExhausted, InvalidLimits, InTransaction,
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitterOpened {
    pub domain: DomainName,
    pub emitter: EmitterName,
    pub fields: Vec<SchemaField>,
    pub generation: u64,
    pub contract: ClientEndpointContract,
    pub window: AckWindow,
    pub ack_timeout: Duration,
    pub retry_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub granted: ClientConsumerLimits,
    pub max_batch_bytes: u64,
    pub max_batch_rows: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenEmitterDisposition {
    Opened(Box<EmitterOpened>),
    Refused(EmitterOpenRefusal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenEmitterOutcome {
    pub disposition: OpenEmitterDisposition,
    pub message: String,
}

impl OpenEmitterOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            OpenEmitterDisposition::Opened(opened) => EncodedUnion::new(
                wire::OpenEmitterDisposition::EmitterOpened,
                opened.encode(encoder)?,
            ),
            OpenEmitterDisposition::Refused(refusal) => EncodedUnion::new(
                wire::OpenEmitterDisposition::EmitterRefused,
                wire::EmitterRefused::create(
                    encoder.fbb(),
                    &wire::EmitterRefusedArgs {
                        refusal: Some((*refusal).into()),
                    },
                ),
            ),
        };
        let message = encoder.text("OpenEmitterOutcome.message", &self.message)?;
        let outcome = wire::OpenEmitterOutcome::create(
            encoder.fbb(),
            &wire::OpenEmitterOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::OpenEmitterOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::OpenEmitterOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(opened) = outcome.disposition_as_emitter_opened() {
            OpenEmitterDisposition::Opened(Box::new(EmitterOpened::decode(decoder, opened)?))
        } else if let Some(refused) = outcome.disposition_as_emitter_refused() {
            OpenEmitterDisposition::Refused(
                decoder.required_enumeration("EmitterRefused.refusal", refused.refusal())?,
            )
        } else {
            return Err(decoder.unknown_union(
                "OpenEmitterOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        Ok(Self {
            disposition,
            message: decoder.text("OpenEmitterOutcome.message", outcome.message())?,
        })
    }
}

impl EmitterOpened {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::EmitterOpened<'fbb>>, Report<WireEncodeError>> {
        if self.fields.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "EmitterOpened.fields",
            }));
        }
        let domain = encoder.text("EmitterOpened.domain", self.domain.as_str())?;
        let emitter = encoder.text("EmitterOpened.emitter", self.emitter.as_str())?;
        let fields =
            encoder.table_vector("EmitterOpened.fields", &self.fields, |field, encoder| {
                encode_field(field, encoder, OPENED_FIELD_DEPTH)
            })?;
        let contract = encoder.fingerprint("EmitterOpened.contract", self.contract.as_digest())?;
        let window = match self.window {
            AckWindow::Sequential => EncodedUnion::new(
                wire::ConsumerWindow::SequentialConsumerWindow,
                wire::SequentialConsumerWindow::create(
                    encoder.fbb(),
                    &wire::SequentialConsumerWindowArgs {},
                ),
            ),
            AckWindow::Parallel { max } => EncodedUnion::new(
                wire::ConsumerWindow::ParallelConsumerWindow,
                wire::ParallelConsumerWindow::create(
                    encoder.fbb(),
                    &wire::ParallelConsumerWindowArgs { max: max.get() },
                ),
            ),
        };
        Ok(wire::EmitterOpened::create(
            encoder.fbb(),
            &wire::EmitterOpenedArgs {
                domain: Some(domain),
                emitter: Some(emitter),
                fields: Some(fields),
                generation: self.generation,
                contract: Some(contract),
                window_type: window.discriminant,
                window: Some(window.value),
                ack_timeout_nanos: encode_nanos(
                    "EmitterOpened.ack_timeout_nanos",
                    self.ack_timeout,
                )?,
                retry_backoff_nanos: encode_nanos(
                    "EmitterOpened.retry_backoff_nanos",
                    self.retry_backoff,
                )?,
                retry_max_backoff_nanos: encode_nanos(
                    "EmitterOpened.retry_max_backoff_nanos",
                    self.retry_max_backoff,
                )?,
                granted_batches: self.granted.batches.get(),
                granted_bytes: self.granted.bytes.get(),
                max_batch_bytes: self.max_batch_bytes,
                max_batch_rows: self.max_batch_rows,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        opened: wire::EmitterOpened<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let domain = decoder.name("EmitterOpened.domain", opened.domain())?;
        let emitter = decoder.name("EmitterOpened.emitter", opened.emitter())?;
        let fields = decoder.table_vector("EmitterOpened.fields", opened.fields(), |field| {
            decode_field(decoder, field)
        })?;
        if fields.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "EmitterOpened.fields",
            }));
        }
        let contract = ClientEndpointContract::from_digest(
            decoder.fingerprint("EmitterOpened.contract", opened.contract())?,
        );
        let window = if opened.window_as_sequential_consumer_window().is_some() {
            AckWindow::Sequential
        } else if let Some(parallel) = opened.window_as_parallel_consumer_window() {
            AckWindow::Parallel {
                max: decoder.non_zero("ParallelConsumerWindow.max", parallel.max())?,
            }
        } else {
            return Err(decoder.unknown_union("EmitterOpened.window", opened.window_type().0));
        };
        let ack_timeout = decode_nanos(
            decoder,
            "EmitterOpened.ack_timeout_nanos",
            opened.ack_timeout_nanos(),
        )?;
        let retry_backoff = decode_nanos(
            decoder,
            "EmitterOpened.retry_backoff_nanos",
            opened.retry_backoff_nanos(),
        )?;
        let retry_max_backoff = decode_nanos(
            decoder,
            "EmitterOpened.retry_max_backoff_nanos",
            opened.retry_max_backoff_nanos(),
        )?;
        if retry_max_backoff < retry_backoff {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "EmitterOpened.retry_max_backoff_nanos",
                kind: "longest backoff no shorter than first",
            }));
        }
        let Some(batches) = NonZeroU32::new(opened.granted_batches()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "EmitterOpened.granted_batches",
            }));
        };
        let bytes = decoder.non_zero("EmitterOpened.granted_bytes", opened.granted_bytes())?;
        let max_batch_bytes = decoder
            .non_zero("EmitterOpened.max_batch_bytes", opened.max_batch_bytes())?
            .get();
        if max_batch_bytes > bytes.get() {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "EmitterOpened.max_batch_bytes",
                kind: "batch within granted bytes",
            }));
        }
        let max_batch_rows = opened.max_batch_rows();
        if max_batch_rows == 0 {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "EmitterOpened.max_batch_rows",
            }));
        }
        Ok(Self {
            domain,
            emitter,
            fields,
            generation: opened.generation(),
            contract,
            window,
            ack_timeout,
            retry_backoff,
            retry_max_backoff,
            granted: ClientConsumerLimits { batches, bytes },
            max_batch_bytes,
            max_batch_rows,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitterBatchReceived {
    pub identity: Uuid,
    pub reference: Uuid,
    pub source_relay: RelayName,
    /// Opaque identity of the concrete branch. Branch key values never cross the client boundary.
    pub branch_fingerprint: Option<[u8; 32]>,
    pub batch: Bytes,
    pub members: u32,
    pub execution_now: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadEmitterDisposition {
    Batch(EmitterBatchReceived),
    Ended,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadEmitterBatchOutcome {
    pub disposition: ReadEmitterDisposition,
    pub message: String,
}

impl ReadEmitterBatchOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            ReadEmitterDisposition::Batch(batch) => EncodedUnion::new(
                wire::ReadEmitterDisposition::EmitterBatchReceived,
                batch.encode(encoder)?,
            ),
            ReadEmitterDisposition::Ended => EncodedUnion::new(
                wire::ReadEmitterDisposition::EmitterConsumerEnded,
                wire::EmitterConsumerEnded::create(
                    encoder.fbb(),
                    &wire::EmitterConsumerEndedArgs {},
                ),
            ),
        };
        let message = encoder.text("ReadEmitterBatchOutcome.message", &self.message)?;
        let outcome = wire::ReadEmitterBatchOutcome::create(
            encoder.fbb(),
            &wire::ReadEmitterBatchOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::ReadEmitterBatchOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        frame: &VerifiedFrame<ServerFrame>,
        decoder: Decoder<'_>,
        outcome: wire::ReadEmitterBatchOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(batch) = outcome.disposition_as_emitter_batch_received() {
            ReadEmitterDisposition::Batch(EmitterBatchReceived::decode(frame, decoder, batch)?)
        } else if outcome.disposition_as_emitter_consumer_ended().is_some() {
            ReadEmitterDisposition::Ended
        } else {
            return Err(decoder.unknown_union(
                "ReadEmitterBatchOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        Ok(Self {
            disposition,
            message: decoder.text("ReadEmitterBatchOutcome.message", outcome.message())?,
        })
    }
}

impl EmitterBatchReceived {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::EmitterBatchReceived<'fbb>>, Report<WireEncodeError>> {
        if self.batch.is_empty() || self.members == 0 {
            return Err(Report::new(WireEncodeError::InvalidValue {
                field: "EmitterBatchReceived.batch",
                kind: "nonempty batch with members",
            }));
        }
        let identity = encoder.bytes("EmitterBatchReceived.identity", self.identity.as_bytes())?;
        let reference =
            encoder.bytes("EmitterBatchReceived.reference", self.reference.as_bytes())?;
        let source_relay = encoder.text(
            "EmitterBatchReceived.source_relay",
            self.source_relay.as_str(),
        )?;
        let branch_fingerprint = self
            .branch_fingerprint
            .as_ref()
            .map(|branch| encoder.bytes("EmitterBatchReceived.branch_fingerprint", branch))
            .transpose()?;
        let batch = encoder.bytes("EmitterBatchReceived.batch", &self.batch)?;
        Ok(wire::EmitterBatchReceived::create(
            encoder.fbb(),
            &wire::EmitterBatchReceivedArgs {
                identity: Some(identity),
                reference: Some(reference),
                source_relay: Some(source_relay),
                branch_fingerprint,
                batch: Some(batch),
                members: self.members,
                execution_now_unix_nanos: self.execution_now.unix_nanos(),
            },
        ))
    }

    fn decode(
        frame: &VerifiedFrame<ServerFrame>,
        decoder: Decoder<'_>,
        batch: wire::EmitterBatchReceived<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let identity = uuid_from_bytes("EmitterBatchReceived.identity", batch.identity().bytes())?;
        let reference =
            uuid_from_bytes("EmitterBatchReceived.reference", batch.reference().bytes())?;
        let source_relay =
            decoder.name("EmitterBatchReceived.source_relay", batch.source_relay())?;
        let branch_fingerprint = batch
            .branch_fingerprint()
            .map(|branch| {
                branch.bytes().try_into().map_err(|_| {
                    Report::new(WireDecodeError::InvalidValue {
                        field: "EmitterBatchReceived.branch_fingerprint",
                        kind: "32-byte fingerprint",
                    })
                })
            })
            .transpose()?;
        let body = batch.batch().bytes();
        if body.is_empty() || batch.members() == 0 {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "EmitterBatchReceived.batch",
                kind: "nonempty batch with members",
            }));
        }
        Ok(Self {
            identity,
            reference,
            source_relay,
            branch_fingerprint,
            batch: frame.bytes().slice_ref(body),
            members: batch.members(),
            execution_now: Timestamp::from_unix_nanos(batch.execution_now_unix_nanos()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterSettlement {
    Confirmed,
    StaleReference,
    WrongConsumer,
    InvalidReason,
    ConsumerEnded,
}
wire_enum!(ALL_EMITTER_SETTLEMENTS: EmitterSettlement => wire::EmitterSettlement {
    Confirmed, StaleReference, WrongConsumer, InvalidReason, ConsumerEnded,
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleEmitterBatchOutcome {
    pub disposition: EmitterSettlement,
    pub message: String,
}
impl SettleEmitterBatchOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let message = encoder.text("SettleEmitterBatchOutcome.message", &self.message)?;
        let outcome = wire::SettleEmitterBatchOutcome::create(
            encoder.fbb(),
            &wire::SettleEmitterBatchOutcomeArgs {
                disposition: Some(self.disposition.into()),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::SettleEmitterBatchOutcome,
            outcome,
        ))
    }
    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::SettleEmitterBatchOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            disposition: decoder.required_enumeration(
                "SettleEmitterBatchOutcome.disposition",
                outcome.disposition(),
            )?,
            message: decoder.text("SettleEmitterBatchOutcome.message", outcome.message())?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmitterCloseDisposition {
    Closed,
    NotOpen,
}
wire_enum!(ALL_EMITTER_CLOSE_DISPOSITIONS: EmitterCloseDisposition => wire::EmitterCloseDisposition {
    Closed, NotOpen,
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseEmitterOutcome {
    pub disposition: EmitterCloseDisposition,
    pub message: String,
}
impl CloseEmitterOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let message = encoder.text("CloseEmitterOutcome.message", &self.message)?;
        let outcome = wire::CloseEmitterOutcome::create(
            encoder.fbb(),
            &wire::CloseEmitterOutcomeArgs {
                disposition: Some(self.disposition.into()),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::CloseEmitterOutcome,
            outcome,
        ))
    }
    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::CloseEmitterOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            disposition: decoder
                .required_enumeration("CloseEmitterOutcome.disposition", outcome.disposition())?,
            message: decoder.text("CloseEmitterOutcome.message", outcome.message())?,
        })
    }
}

fn uuid_from_bytes(field: &'static str, bytes: &[u8]) -> Result<Uuid, Report<WireDecodeError>> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        Report::new(WireDecodeError::InvalidValue {
            field,
            kind: "16-byte identity",
        })
    })?;
    Ok(Uuid::from_bytes(bytes))
}

fn encode_nanos(field: &'static str, duration: Duration) -> Result<u64, Report<WireEncodeError>> {
    u64::try_from(duration.as_nanos()).map_err(|_| {
        Report::new(WireEncodeError::InvalidValue {
            field,
            kind: "duration representable in nanoseconds",
        })
    })
}

fn decode_nanos(
    decoder: Decoder<'_>,
    field: &'static str,
    nanos: u64,
) -> Result<Duration, Report<WireDecodeError>> {
    decoder.non_zero(field, nanos)?;
    Ok(Duration::from_nanos(nanos))
}
