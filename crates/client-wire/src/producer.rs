//! Client producers: opening one against a client ingestor, submitting its batches, closing it, the
//! terminal outcome of every batch, and the server's reports about a producer's admission and end.
//!
//! A producer is named by the request identity of the request that opened it, which a session
//! never reuses, so a request or event about a producer can never reach another producer of the
//! same session. A batch travels as the Arrow IPC bytes the client wrote; this crate carries them
//! without reading them.

use std::{fmt, num::NonZeroU32, time::Duration};

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::OptionExt as _;
use nervix_models::{
    AckWindow, ClientAttachmentId, ClientBatchDefect, ClientEndpointContract,
    ClientOutcomeUncertainty, ClientProcessingFailure, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerEndReason, ClientProducerGrant, ClientProducerLimits,
    ClientProducerPolicy, ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal,
    DomainName, IngestorName, SchemaField,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::RequestId,
    frame::{ClientFrame, EncodedFrame, ServerFrame, VerifiedFrame},
    limits::{MIN_FRAME_BYTES, SessionLimits},
    row::{decode_field, encode_field},
    server::finish_server_message,
    wire,
};

/// The depth of an expected field in an open request: the client message, the request, and the
/// field.
const REQUEST_FIELD_DEPTH: usize = 3;

/// The depth of a field in an opened producer's reply: the server message, the reply, the open
/// outcome, the opened producer, and the field.
const OPENED_FIELD_DEPTH: usize = 5;

/// The bytes a submission frame holds besides its batch: the client message and request tables
/// with their vtables, the batch vector's length prefix, the root offset and identifier, and
/// alignment padding. A test encodes a batch of the largest size this leaves and finds it fits.
const SUBMISSION_ENVELOPE_BYTES: usize = 256;

const _: () = assert!(
    SUBMISSION_ENVELOPE_BYTES < MIN_FRAME_BYTES,
    "every frame a session admits must carry a batch beside its envelope",
);

impl SessionLimits {
    /// The largest batch one submission frame carries under these limits.
    pub fn max_submitted_batch_bytes(&self) -> usize {
        self.frame_bytes()
            .checked_sub(SUBMISSION_ENVELOPE_BYTES)
            .assured("every frame limit is at least MIN_FRAME_BYTES, which exceeds the envelope")
    }
}

/// A producer within one session: the identity of the request that opened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProducerId(RequestId);

impl ProducerId {
    /// The producer the request `open` opened.
    pub const fn opened_by(open: RequestId) -> Self {
        Self(open)
    }

    /// The request that opened the producer.
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

impl fmt::Display for ProducerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

wire_enum!(ALL_PRODUCER_ADMISSIONS: ClientProducerAdmission => wire::ProducerAdmission {
    Open,
    Suspended,
});

wire_enum!(ALL_PRODUCER_REFUSALS: ClientProducerRefusal => wire::ProducerRefusal {
    DomainNotFound,
    DomainStopped,
    IngestorNotFound,
    NotClientIngestor,
    EndpointUnavailable,
    SchemaMismatch,
    TooManyProducers,
    SessionCapacityExhausted,
    NodeCapacityExhausted,
    InvalidLimits,
    InTransaction,
});

wire_enum!(ALL_PROCESSING_FAILURES: ClientProcessingFailure => wire::ProcessingFailure {
    AckTimedOut,
    Rejected,
});

wire_enum!(ALL_OUTCOME_UNCERTAINTIES: ClientOutcomeUncertainty => wire::OutcomeUncertainty {
    Interrupted,
    OwnerLost,
});

wire_enum!(ALL_PRODUCER_END_REASONS: ClientProducerEndReason => wire::ProducerEndReason {
    EndpointChanged,
    EndpointRemoved,
    DomainStopped,
    Relocated,
    ShuttingDown,
    OwnerLost,
    ProtocolViolated,
});

/// Attaches a producer to a client ingestor of a running domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenIngestorRequest {
    pub domain: DomainName,
    pub ingestor: IngestorName,
    /// The fields every batch carries, in order. Never empty.
    pub expected_fields: Vec<SchemaField>,
    pub limits: ClientProducerLimits,
}

impl OpenIngestorRequest {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::OpenIngestorRequest<'fbb>>, Report<WireEncodeError>> {
        if self.expected_fields.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "OpenIngestorRequest.expected_fields",
            }));
        }
        let domain = encoder.text("OpenIngestorRequest.domain", self.domain.as_str())?;
        let ingestor = encoder.text("OpenIngestorRequest.ingestor", self.ingestor.as_str())?;
        let expected_fields = encoder.table_vector(
            "OpenIngestorRequest.expected_fields",
            &self.expected_fields,
            |field, encoder| encode_field(field, encoder, REQUEST_FIELD_DEPTH),
        )?;
        Ok(wire::OpenIngestorRequest::create(
            encoder.fbb(),
            &wire::OpenIngestorRequestArgs {
                domain: Some(domain),
                ingestor: Some(ingestor),
                expected_fields: Some(expected_fields),
                max_outstanding_batches: self.limits.batches.get(),
                max_outstanding_bytes: self.limits.bytes.get(),
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        open: wire::OpenIngestorRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let domain = decoder.name("OpenIngestorRequest.domain", open.domain())?;
        let ingestor = decoder.name("OpenIngestorRequest.ingestor", open.ingestor())?;
        let expected_fields = decoder.table_vector(
            "OpenIngestorRequest.expected_fields",
            open.expected_fields(),
            |field| decode_field(decoder, field),
        )?;
        if expected_fields.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "OpenIngestorRequest.expected_fields",
            }));
        }
        let Some(batches) = NonZeroU32::new(open.max_outstanding_batches()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "OpenIngestorRequest.max_outstanding_batches",
            }));
        };
        let bytes = decoder.non_zero(
            "OpenIngestorRequest.max_outstanding_bytes",
            open.max_outstanding_bytes(),
        )?;
        Ok(Self {
            domain,
            ingestor,
            expected_fields,
            limits: ClientProducerLimits { batches, bytes },
        })
    }
}

/// Submits one batch of an open producer. Its terminal reply is a [`SubmissionOutcome`], which
/// always follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitBatchRequest {
    pub producer: ProducerId,
    /// One canonical Arrow IPC stream. A decoded request shares the bytes of the frame it arrived
    /// in rather than copying them.
    pub batch: Bytes,
}

impl SubmitBatchRequest {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::SubmitBatchRequest<'fbb>>, Report<WireEncodeError>> {
        if self.batch.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "SubmitBatchRequest.batch",
            }));
        }
        let batch = encoder.bytes("SubmitBatchRequest.batch", &self.batch)?;
        Ok(wire::SubmitBatchRequest::create(
            encoder.fbb(),
            &wire::SubmitBatchRequestArgs {
                producer: self.producer.wire(),
                batch: Some(batch),
            },
        ))
    }

    pub(crate) fn decode(
        frame: &VerifiedFrame<ClientFrame>,
        decoder: Decoder<'_>,
        submit: wire::SubmitBatchRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let producer = ProducerId::decode(decoder, "SubmitBatchRequest.producer", submit.producer())?;
        let batch = submit.batch().bytes();
        if batch.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "SubmitBatchRequest.batch",
            }));
        }
        // The vector lies inside the frame's own buffer, which verification bounded, so the slice
        // is a window onto those bytes rather than a copy of them.
        let batch = frame.bytes().slice_ref(batch);
        Ok(Self { producer, batch })
    }
}

/// Stops admission for a producer and releases it once every batch it submitted has its terminal
/// reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloseIngestorRequest {
    pub producer: ProducerId,
}

impl CloseIngestorRequest {
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> WIPOffset<wire::CloseIngestorRequest<'fbb>> {
        wire::CloseIngestorRequest::create(
            encoder.fbb(),
            &wire::CloseIngestorRequestArgs {
                producer: self.producer.wire(),
            },
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        close: wire::CloseIngestorRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            producer: ProducerId::decode(decoder, "CloseIngestorRequest.producer", close.producer())?,
        })
    }
}

/// An opened producer: the ingestor it is attached to and everything the open established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerOpened {
    pub domain: DomainName,
    pub ingestor: IngestorName,
    pub description: ClientProducerDescription,
}

/// What became of a request to open a producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenIngestorDisposition {
    Opened(Box<ProducerOpened>),
    /// Nothing was attached.
    Refused(ClientProducerRefusal),
}

/// The reply to a request to open a producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenIngestorOutcome {
    pub disposition: OpenIngestorDisposition,
    pub message: String,
}

impl OpenIngestorOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            OpenIngestorDisposition::Opened(opened) => EncodedUnion::new(
                wire::OpenIngestorDisposition::ProducerOpened,
                opened.encode(encoder)?,
            ),
            OpenIngestorDisposition::Refused(refusal) => {
                let refused = wire::ProducerRefused::create(
                    encoder.fbb(),
                    &wire::ProducerRefusedArgs {
                        refusal: Some((*refusal).into()),
                    },
                );
                EncodedUnion::new(wire::OpenIngestorDisposition::ProducerRefused, refused)
            }
        };
        let message = encoder.text("OpenIngestorOutcome.message", &self.message)?;
        let outcome = wire::OpenIngestorOutcome::create(
            encoder.fbb(),
            &wire::OpenIngestorOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::OpenIngestorOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::OpenIngestorOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(opened) = outcome.disposition_as_producer_opened() {
            OpenIngestorDisposition::Opened(Box::new(ProducerOpened::decode(decoder, opened)?))
        } else if let Some(refused) = outcome.disposition_as_producer_refused() {
            OpenIngestorDisposition::Refused(
                decoder.required_enumeration("ProducerRefused.refusal", refused.refusal())?,
            )
        } else {
            return Err(decoder.unknown_union(
                "OpenIngestorOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("OpenIngestorOutcome.message", outcome.message())?;
        Ok(Self {
            disposition,
            message,
        })
    }
}

impl ProducerOpened {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::ProducerOpened<'fbb>>, Report<WireEncodeError>> {
        let description = &self.description;
        if description.fields.is_empty() {
            return Err(Report::new(WireEncodeError::EmptyCollection {
                field: "ProducerOpened.fields",
            }));
        }
        let domain = encoder.text("ProducerOpened.domain", self.domain.as_str())?;
        let ingestor = encoder.text("ProducerOpened.ingestor", self.ingestor.as_str())?;
        let fields = encoder.table_vector(
            "ProducerOpened.fields",
            &description.fields,
            |field, encoder| encode_field(field, encoder, OPENED_FIELD_DEPTH),
        )?;
        let contract =
            encoder.fingerprint("ProducerOpened.contract", description.contract.as_digest())?;
        let attachment = encoder.bytes(
            "ProducerOpened.attachment",
            &description.attachment.as_u128().to_be_bytes(),
        )?;
        let window = match description.policy.window {
            AckWindow::Sequential => EncodedUnion::new(
                wire::ProducerWindow::SequentialProducerWindow,
                wire::SequentialProducerWindow::create(
                    encoder.fbb(),
                    &wire::SequentialProducerWindowArgs {},
                ),
            ),
            AckWindow::Parallel { max } => EncodedUnion::new(
                wire::ProducerWindow::ParallelProducerWindow,
                wire::ParallelProducerWindow::create(
                    encoder.fbb(),
                    &wire::ParallelProducerWindowArgs { max: max.get() },
                ),
            ),
        };
        let policy = &description.policy;
        let ack_timeout_nanos = encode_nanos("ProducerOpened.ack_timeout_nanos", policy.ack_timeout)?;
        let retry_backoff_nanos =
            encode_nanos("ProducerOpened.retry_backoff_nanos", policy.retry_backoff)?;
        let retry_max_backoff_nanos = encode_nanos(
            "ProducerOpened.retry_max_backoff_nanos",
            policy.retry_max_backoff,
        )?;
        let grant = &description.grant;
        Ok(wire::ProducerOpened::create(
            encoder.fbb(),
            &wire::ProducerOpenedArgs {
                domain: Some(domain),
                ingestor: Some(ingestor),
                fields: Some(fields),
                generation: description.generation,
                contract: Some(contract),
                attachment: Some(attachment),
                window_type: window.discriminant,
                window: Some(window.value),
                ack_timeout_nanos,
                retry_backoff_nanos,
                retry_max_backoff_nanos,
                granted_batches: grant.batches.get(),
                granted_bytes: grant.bytes.get(),
                max_batch_bytes: grant.max_batch_bytes.get(),
                max_batch_rows: grant.max_batch_rows.get(),
                admission: Some(description.admission.into()),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        opened: wire::ProducerOpened<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let domain = decoder.name("ProducerOpened.domain", opened.domain())?;
        let ingestor = decoder.name("ProducerOpened.ingestor", opened.ingestor())?;
        let fields = decoder.table_vector("ProducerOpened.fields", opened.fields(), |field| {
            decode_field(decoder, field)
        })?;
        if fields.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "ProducerOpened.fields",
            }));
        }
        let contract = ClientEndpointContract::from_digest(
            decoder.fingerprint("ProducerOpened.contract", opened.contract())?,
        );
        let Ok(attachment) = <[u8; 16]>::try_from(opened.attachment().bytes()) else {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ProducerOpened.attachment",
                kind: "16-byte attachment identity",
            }));
        };
        let attachment = ClientAttachmentId::from_u128(u128::from_be_bytes(attachment));
        let window = if opened.window_as_sequential_producer_window().is_some() {
            AckWindow::Sequential
        } else if let Some(parallel) = opened.window_as_parallel_producer_window() {
            AckWindow::Parallel {
                max: decoder.non_zero("ParallelProducerWindow.max", parallel.max())?,
            }
        } else {
            return Err(decoder.unknown_union("ProducerOpened.window", opened.window_type().0));
        };
        let ack_timeout = decode_nanos(
            decoder,
            "ProducerOpened.ack_timeout_nanos",
            opened.ack_timeout_nanos(),
        )?;
        let retry_backoff = decode_nanos(
            decoder,
            "ProducerOpened.retry_backoff_nanos",
            opened.retry_backoff_nanos(),
        )?;
        let retry_max_backoff = decode_nanos(
            decoder,
            "ProducerOpened.retry_max_backoff_nanos",
            opened.retry_max_backoff_nanos(),
        )?;
        if retry_max_backoff < retry_backoff {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ProducerOpened.retry_max_backoff_nanos",
                kind: "longest backoff no shorter than the first",
            }));
        }
        let Some(batches) = NonZeroU32::new(opened.granted_batches()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "ProducerOpened.granted_batches",
            }));
        };
        let bytes = decoder.non_zero("ProducerOpened.granted_bytes", opened.granted_bytes())?;
        let max_batch_bytes =
            decoder.non_zero("ProducerOpened.max_batch_bytes", opened.max_batch_bytes())?;
        if max_batch_bytes > bytes {
            return Err(Report::new(WireDecodeError::InvalidValue {
                field: "ProducerOpened.max_batch_bytes",
                kind: "batch size within the granted bytes",
            }));
        }
        let Some(max_batch_rows) = NonZeroU32::new(opened.max_batch_rows()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "ProducerOpened.max_batch_rows",
            }));
        };
        let admission =
            decoder.required_enumeration("ProducerOpened.admission", opened.admission())?;
        Ok(Self {
            domain,
            ingestor,
            description: ClientProducerDescription {
                attachment,
                fields,
                generation: opened.generation(),
                contract,
                policy: ClientProducerPolicy {
                    window,
                    ack_timeout,
                    retry_backoff,
                    retry_max_backoff,
                },
                grant: ClientProducerGrant {
                    batches,
                    bytes,
                    max_batch_bytes,
                    max_batch_rows,
                },
                admission,
            },
        })
    }
}

/// A duration in whole nanoseconds, which is how the schema carries every policy duration.
fn encode_nanos(field: &'static str, duration: Duration) -> Result<u64, Report<WireEncodeError>> {
    let Ok(nanos) = u64::try_from(duration.as_nanos()) else {
        return Err(Report::new(WireEncodeError::InvalidValue {
            field,
            kind: "duration of at most u64::MAX nanoseconds",
        }));
    };
    if nanos == 0 {
        return Err(Report::new(WireEncodeError::InvalidValue {
            field,
            kind: "non-zero duration",
        }));
    }
    Ok(nanos)
}

fn decode_nanos(
    decoder: Decoder<'_>,
    field: &'static str,
    nanos: u64,
) -> Result<Duration, Report<WireDecodeError>> {
    let nanos = decoder.non_zero(field, nanos)?;
    Ok(Duration::from_nanos(nanos.get()))
}

/// The terminal reply to one submitted batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionOutcome {
    pub outcome: ClientSubmissionOutcome,
    /// A bounded, non-sensitive description, empty for a completed batch.
    pub message: String,
}

impl SubmissionOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match self.outcome {
            ClientSubmissionOutcome::NotAdmitted(refusal) => {
                let not_admitted = wire::SubmissionNotAdmitted::create(
                    encoder.fbb(),
                    &wire::SubmissionNotAdmittedArgs {
                        refusal: Some(wire_refusal(refusal)),
                    },
                );
                EncodedUnion::new(
                    wire::SubmissionDisposition::SubmissionNotAdmitted,
                    not_admitted,
                )
            }
            ClientSubmissionOutcome::Completed => EncodedUnion::new(
                wire::SubmissionDisposition::SubmissionCompleted,
                wire::SubmissionCompleted::create(
                    encoder.fbb(),
                    &wire::SubmissionCompletedArgs {},
                ),
            ),
            ClientSubmissionOutcome::ProcessingFailed(failure) => {
                let failed = wire::SubmissionFailed::create(
                    encoder.fbb(),
                    &wire::SubmissionFailedArgs {
                        failure: Some(failure.into()),
                    },
                );
                EncodedUnion::new(wire::SubmissionDisposition::SubmissionFailed, failed)
            }
            ClientSubmissionOutcome::OutcomeUnknown(cause) => {
                let unknown = wire::SubmissionOutcomeUnknown::create(
                    encoder.fbb(),
                    &wire::SubmissionOutcomeUnknownArgs {
                        cause: Some(cause.into()),
                    },
                );
                EncodedUnion::new(
                    wire::SubmissionDisposition::SubmissionOutcomeUnknown,
                    unknown,
                )
            }
        };
        let message = encoder.text("SubmissionOutcome.message", &self.message)?;
        let outcome = wire::SubmissionOutcome::create(
            encoder.fbb(),
            &wire::SubmissionOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::SubmissionOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::SubmissionOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let decoded = if let Some(not_admitted) = outcome.disposition_as_submission_not_admitted()
        {
            let refusal = decoder.required(
                "SubmissionNotAdmitted.refusal",
                not_admitted.refusal(),
            )?;
            let Some(refusal) = submission_refusal(refusal) else {
                return Err(Report::new(WireDecodeError::UnknownEnumValue {
                    field: "SubmissionNotAdmitted.refusal",
                    value: refusal.0,
                }));
            };
            ClientSubmissionOutcome::NotAdmitted(refusal)
        } else if outcome.disposition_as_submission_completed().is_some() {
            ClientSubmissionOutcome::Completed
        } else if let Some(failed) = outcome.disposition_as_submission_failed() {
            ClientSubmissionOutcome::ProcessingFailed(
                decoder.required_enumeration("SubmissionFailed.failure", failed.failure())?,
            )
        } else if let Some(unknown) = outcome.disposition_as_submission_outcome_unknown() {
            ClientSubmissionOutcome::OutcomeUnknown(
                decoder.required_enumeration("SubmissionOutcomeUnknown.cause", unknown.cause())?,
            )
        } else {
            return Err(decoder.unknown_union(
                "SubmissionOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("SubmissionOutcome.message", outcome.message())?;
        Ok(Self {
            outcome: decoded,
            message,
        })
    }
}

/// The schema value naming a refusal. The schema lists the defects of an invalid batch beside the
/// other refusals, so an invalid batch reads as one value.
pub(crate) fn wire_refusal(refusal: ClientSubmissionRefusal) -> wire::SubmissionRefusal {
    match refusal {
        ClientSubmissionRefusal::InvalidBatch(defect) => match defect {
            ClientBatchDefect::Malformed => wire::SubmissionRefusal::MalformedBatch,
            ClientBatchDefect::UnexpectedMessage => wire::SubmissionRefusal::UnexpectedMessage,
            ClientBatchDefect::Compressed => wire::SubmissionRefusal::CompressedBatch,
            ClientBatchDefect::SchemaMismatch => wire::SubmissionRefusal::SchemaMismatch,
            ClientBatchDefect::NotOneBatch => wire::SubmissionRefusal::NotOneBatch,
            ClientBatchDefect::TooManyRows => wire::SubmissionRefusal::TooManyRows,
            ClientBatchDefect::TooLarge => wire::SubmissionRefusal::BatchTooLarge,
            ClientBatchDefect::InvalidData => wire::SubmissionRefusal::InvalidData,
        },
        ClientSubmissionRefusal::Suspended => wire::SubmissionRefusal::Suspended,
        ClientSubmissionRefusal::Busy => wire::SubmissionRefusal::Busy,
        ClientSubmissionRefusal::Draining => wire::SubmissionRefusal::Draining,
        ClientSubmissionRefusal::ProducerEnded => wire::SubmissionRefusal::ProducerEnded,
        ClientSubmissionRefusal::CreditExceeded => wire::SubmissionRefusal::CreditExceeded,
    }
}

/// The refusal a schema value names, or `None` for a value the schema does not declare.
fn submission_refusal(refusal: wire::SubmissionRefusal) -> Option<ClientSubmissionRefusal> {
    let refusal = match refusal {
        wire::SubmissionRefusal::MalformedBatch => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::Malformed)
        }
        wire::SubmissionRefusal::UnexpectedMessage => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::UnexpectedMessage)
        }
        wire::SubmissionRefusal::CompressedBatch => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::Compressed)
        }
        wire::SubmissionRefusal::SchemaMismatch => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::SchemaMismatch)
        }
        wire::SubmissionRefusal::NotOneBatch => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::NotOneBatch)
        }
        wire::SubmissionRefusal::TooManyRows => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::TooManyRows)
        }
        wire::SubmissionRefusal::BatchTooLarge => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::TooLarge)
        }
        wire::SubmissionRefusal::InvalidData => {
            ClientSubmissionRefusal::InvalidBatch(ClientBatchDefect::InvalidData)
        }
        wire::SubmissionRefusal::Suspended => ClientSubmissionRefusal::Suspended,
        wire::SubmissionRefusal::Busy => ClientSubmissionRefusal::Busy,
        wire::SubmissionRefusal::Draining => ClientSubmissionRefusal::Draining,
        wire::SubmissionRefusal::ProducerEnded => ClientSubmissionRefusal::ProducerEnded,
        wire::SubmissionRefusal::CreditExceeded => ClientSubmissionRefusal::CreditExceeded,
        _ => return None,
    };
    Some(refusal)
}

/// Every refusal the schema can carry, in schema order.
#[cfg(test)]
pub(crate) fn all_submission_refusals() -> Vec<ClientSubmissionRefusal> {
    let mut refusals = Vec::new();
    for value in wire::SubmissionRefusal::ENUM_VALUES {
        let refusal =
            submission_refusal(*value).assured("every value the schema declares names a refusal");
        refusals.push(refusal);
    }
    refusals
}

/// What became of a request to close a producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseIngestorDisposition {
    /// Every batch the producer submitted has its terminal reply, and the producer is released.
    Closed,
    /// The session holds no open producer by that identity. Nothing changed.
    NotOpen,
}

/// The reply to a request to close a producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseIngestorOutcome {
    pub disposition: CloseIngestorDisposition,
    pub message: String,
}

impl CloseIngestorOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match self.disposition {
            CloseIngestorDisposition::Closed => EncodedUnion::new(
                wire::CloseIngestorDisposition::ProducerClosed,
                wire::ProducerClosed::create(encoder.fbb(), &wire::ProducerClosedArgs {}),
            ),
            CloseIngestorDisposition::NotOpen => EncodedUnion::new(
                wire::CloseIngestorDisposition::ProducerNotOpen,
                wire::ProducerNotOpen::create(encoder.fbb(), &wire::ProducerNotOpenArgs {}),
            ),
        };
        let message = encoder.text("CloseIngestorOutcome.message", &self.message)?;
        let outcome = wire::CloseIngestorOutcome::create(
            encoder.fbb(),
            &wire::CloseIngestorOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::CloseIngestorOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::CloseIngestorOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if outcome.disposition_as_producer_closed().is_some() {
            CloseIngestorDisposition::Closed
        } else if outcome.disposition_as_producer_not_open().is_some() {
            CloseIngestorDisposition::NotOpen
        } else {
            return Err(decoder.unknown_union(
                "CloseIngestorOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("CloseIngestorOutcome.message", outcome.message())?;
        Ok(Self {
            disposition,
            message,
        })
    }
}

/// Whether an open producer's batches are admitted changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerAdmissionChanged {
    pub producer: ProducerId,
    pub admission: ClientProducerAdmission,
}

impl ProducerAdmissionChanged {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let changed = wire::ProducerAdmissionChanged::create(
            encoder.fbb(),
            &wire::ProducerAdmissionChangedArgs {
                producer: self.producer.wire(),
                admission: Some(self.admission.into()),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::ProducerAdmissionChanged, changed),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        changed: wire::ProducerAdmissionChanged<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            producer: ProducerId::decode(
                decoder,
                "ProducerAdmissionChanged.producer",
                changed.producer(),
            )?,
            admission: decoder
                .required_enumeration("ProducerAdmissionChanged.admission", changed.admission())?,
        })
    }
}

/// The server ended a producer. Every batch it submitted already has its terminal reply, and this
/// is the last frame about the producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerEnded {
    pub producer: ProducerId,
    pub reason: ClientProducerEndReason,
    pub message: String,
}

impl ProducerEnded {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let message = encoder.text("ProducerEnded.message", &self.message)?;
        let ended = wire::ProducerEnded::create(
            encoder.fbb(),
            &wire::ProducerEndedArgs {
                producer: self.producer.wire(),
                reason: Some(self.reason.into()),
                message: Some(message),
            },
        );
        finish_server_message(
            encoder,
            EncodedUnion::new(wire::ServerBody::ProducerEnded, ended),
        )
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        ended: wire::ProducerEnded<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        Ok(Self {
            producer: ProducerId::decode(decoder, "ProducerEnded.producer", ended.producer())?,
            reason: decoder.required_enumeration("ProducerEnded.reason", ended.reason())?,
            message: decoder.text("ProducerEnded.message", ended.message())?,
        })
    }
}
