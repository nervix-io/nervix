//! Reply bodies for completion, inspection, subscription lifecycle, cancellation and rejection.

use error_stack::Report;
use flatbuffers::WIPOffset;
use nervix_models::{DomainName, RelayName, TransactionInspection, TransactionInspectionRejection};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    common::{Diagnostic, LeaderRedirect, RequestId},
    impact::{decode_report, encode_report},
    row::RowSchema,
    subscription::{SubscriptionHandle, SubscriptionType},
    transaction::{
        decode_operation_number, decode_transaction_status, encode_operation_number,
        encode_transaction_status,
    },
    wire,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SuggestionKind {
    /// Text to insert at the cursor.
    Text,
    /// A path fragment the client completes against its own file system.
    LocalDirectoryLookup,
}

wire_enum!(ALL_SUGGESTION_KINDS: SuggestionKind => wire::SuggestionKind {
    Text,
    LocalDirectoryLookup,
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suggestion {
    pub value: String,
    pub kind: SuggestionKind,
}

impl Suggestion {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::Suggestion<'fbb>>, Report<WireEncodeError>> {
        let value = encoder.text("Suggestion.value", &self.value)?;
        Ok(wire::Suggestion::create(
            encoder.fbb(),
            &wire::SuggestionArgs {
                value: Some(value),
                kind: Some(self.kind.into()),
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        suggestion: wire::Suggestion<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let value = decoder.text("Suggestion.value", suggestion.value())?;
        let kind = decoder.required_enumeration("Suggestion.kind", suggestion.kind())?;
        Ok(Self { value, kind })
    }
}

/// The completions offered at a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuggestOutcome {
    pub suggestions: Vec<Suggestion>,
}

impl SuggestOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let suggestions = encoder.table_vector(
            "SuggestOutcome.suggestions",
            &self.suggestions,
            Suggestion::encode,
        )?;
        let outcome = wire::SuggestOutcome::create(
            encoder.fbb(),
            &wire::SuggestOutcomeArgs {
                suggestions: Some(suggestions),
            },
        );
        Ok(EncodedUnion::new(wire::ReplyBody::SuggestOutcome, outcome))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::SuggestOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let suggestions = decoder.table_vector(
            "SuggestOutcome.suggestions",
            outcome.suggestions(),
            |suggestion| Suggestion::decode(decoder, suggestion),
        )?;
        Ok(Self { suggestions })
    }
}

wire_enum!(
    ALL_INSPECTION_REJECTIONS: TransactionInspectionRejection => wire::InspectionRejection {
        NoAttachedTransaction,
        TransactionNotFound,
        NotOwner,
        OperationNotFound,
        ReportUnavailable,
    }
);

/// The outcome of an inspection request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InspectionOutcome {
    Inspected(Box<TransactionInspection>),
    Rejected {
        rejection: TransactionInspectionRejection,
        message: String,
    },
    NotLeader(LeaderRedirect),
}

impl InspectionOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match self {
            Self::Inspected(inspection) => {
                let transaction = encode_transaction_status(encoder, &inspection.transaction)?;
                let report = encode_report(encoder, &inspection.report)?;
                let operation = inspection.operation.map(encode_operation_number);
                let inspected = wire::TransactionInspected::create(
                    encoder.fbb(),
                    &wire::TransactionInspectedArgs {
                        transaction: Some(transaction),
                        operation,
                        report: Some(report),
                    },
                );
                EncodedUnion::new(wire::InspectionDisposition::TransactionInspected, inspected)
            }
            Self::Rejected { rejection, message } => {
                let message = encoder.text("InspectionRejected.message", message)?;
                let rejected = wire::InspectionRejected::create(
                    encoder.fbb(),
                    &wire::InspectionRejectedArgs {
                        rejection: Some((*rejection).into()),
                        message: Some(message),
                    },
                );
                EncodedUnion::new(wire::InspectionDisposition::InspectionRejected, rejected)
            }
            Self::NotLeader(redirect) => EncodedUnion::new(
                wire::InspectionDisposition::LeaderRedirect,
                redirect.encode(encoder)?,
            ),
        };
        let outcome = wire::InspectionOutcome::create(
            encoder.fbb(),
            &wire::InspectionOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::InspectionOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::InspectionOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(inspected) = outcome.disposition_as_transaction_inspected() {
            let transaction = decode_transaction_status(decoder, inspected.transaction())?;
            let operation = match inspected.operation() {
                Some(operation) => Some(decode_operation_number(
                    decoder,
                    "TransactionInspected.operation",
                    operation,
                )?),
                None => None,
            };
            let report = decode_report(decoder, inspected.report())?;
            return Ok(Self::Inspected(Box::new(TransactionInspection {
                transaction,
                operation,
                report,
            })));
        }
        if let Some(rejected) = outcome.disposition_as_inspection_rejected() {
            let rejection = decoder
                .required_enumeration("InspectionRejected.rejection", rejected.rejection())?;
            let message = decoder.text("InspectionRejected.message", rejected.message())?;
            return Ok(Self::Rejected { rejection, message });
        }
        if let Some(redirect) = outcome.disposition_as_leader_redirect() {
            return Ok(Self::NotLeader(LeaderRedirect::decode(decoder, redirect)?));
        }
        Err(decoder.unknown_union(
            "InspectionOutcome.disposition",
            outcome.disposition_type().0,
        ))
    }
}

/// A subscription the server opened, and the schema its rows follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOpened {
    pub subscription: SubscriptionHandle,
    pub domain: DomainName,
    pub relay: RelayName,
    /// The type the request selected, confirmed.
    pub subscription_type: SubscriptionType,
    pub schema: RowSchema,
}

/// What became of a subscribe request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeDisposition {
    Opened(Box<SubscriptionOpened>),
    /// The subscription was not opened; the outcome's message and diagnostics say why.
    Failed,
}

/// The depth of an opened subscription's schema table in its reply frame: the server message, the
/// reply, the subscribe outcome, the opened subscription, and the schema.
const SUBSCRIPTION_SCHEMA_DEPTH: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeOutcome {
    pub disposition: SubscribeDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl SubscribeOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            SubscribeDisposition::Opened(opened) => {
                let subscription = opened.subscription.encode(encoder)?;
                let domain = encoder.text("SubscriptionOpened.domain", opened.domain.as_str())?;
                let relay = encoder.text("SubscriptionOpened.relay", opened.relay.as_str())?;
                let schema = opened.schema.encode(encoder, SUBSCRIPTION_SCHEMA_DEPTH)?;
                let opened = wire::SubscriptionOpened::create(
                    encoder.fbb(),
                    &wire::SubscriptionOpenedArgs {
                        subscription: Some(subscription),
                        domain: Some(domain),
                        relay: Some(relay),
                        subscription_type: Some(opened.subscription_type.into()),
                        schema: Some(schema),
                    },
                );
                EncodedUnion::new(wire::SubscribeDisposition::SubscriptionOpened, opened)
            }
            SubscribeDisposition::Failed => EncodedUnion::new(
                wire::SubscribeDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
        };
        let message = encoder.text("SubscribeOutcome.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(encoder, "SubscribeOutcome.diagnostics", &self.diagnostics)?;
        let outcome = wire::SubscribeOutcome::create(
            encoder.fbb(),
            &wire::SubscribeOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::SubscribeOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::SubscribeOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(opened) = outcome.disposition_as_subscription_opened() {
            let subscription = SubscriptionHandle::decode(decoder, opened.subscription())?;
            let domain = decoder.name("SubscriptionOpened.domain", opened.domain())?;
            let relay = decoder.name("SubscriptionOpened.relay", opened.relay())?;
            let subscription_type = decoder.required_enumeration(
                "SubscriptionOpened.subscription_type",
                opened.subscription_type(),
            )?;
            let schema = RowSchema::decode(decoder, opened.schema())?;
            SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                subscription,
                domain,
                relay,
                subscription_type,
                schema,
            }))
        } else if let wire::SubscribeDisposition::RequestFailed = outcome.disposition_type() {
            SubscribeDisposition::Failed
        } else {
            return Err(
                decoder.unknown_union("SubscribeOutcome.disposition", outcome.disposition_type().0)
            );
        };
        let message = decoder.text("SubscribeOutcome.message", outcome.message())?;
        let diagnostics = Diagnostic::decode_all(
            decoder,
            "SubscribeOutcome.diagnostics",
            outcome.diagnostics(),
        )?;
        Ok(Self {
            disposition,
            message,
            diagnostics,
        })
    }
}

/// What became of an unsubscribe request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsubscribeDisposition {
    Deleted(SubscriptionHandle),
    /// The subscription was not deleted; the outcome's message and diagnostics say why.
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeOutcome {
    pub disposition: UnsubscribeDisposition,
    pub message: String,
    pub diagnostics: Vec<Diagnostic>,
}

impl UnsubscribeOutcome {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let disposition = match &self.disposition {
            UnsubscribeDisposition::Deleted(subscription) => {
                let subscription = subscription.encode(encoder)?;
                let deleted = wire::SubscriptionDeleted::create(
                    encoder.fbb(),
                    &wire::SubscriptionDeletedArgs {
                        subscription: Some(subscription),
                    },
                );
                EncodedUnion::new(wire::UnsubscribeDisposition::SubscriptionDeleted, deleted)
            }
            UnsubscribeDisposition::Failed => EncodedUnion::new(
                wire::UnsubscribeDisposition::RequestFailed,
                wire::RequestFailed::create(encoder.fbb(), &wire::RequestFailedArgs {}),
            ),
        };
        let message = encoder.text("UnsubscribeOutcome.message", &self.message)?;
        let diagnostics =
            Diagnostic::encode_all(encoder, "UnsubscribeOutcome.diagnostics", &self.diagnostics)?;
        let outcome = wire::UnsubscribeOutcome::create(
            encoder.fbb(),
            &wire::UnsubscribeOutcomeArgs {
                disposition_type: disposition.discriminant,
                disposition: Some(disposition.value),
                message: Some(message),
                diagnostics: Some(diagnostics),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::UnsubscribeOutcome,
            outcome,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::UnsubscribeOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let disposition = if let Some(deleted) = outcome.disposition_as_subscription_deleted() {
            UnsubscribeDisposition::Deleted(SubscriptionHandle::decode(
                decoder,
                deleted.subscription(),
            )?)
        } else if let wire::UnsubscribeDisposition::RequestFailed = outcome.disposition_type() {
            UnsubscribeDisposition::Failed
        } else {
            return Err(decoder.unknown_union(
                "UnsubscribeOutcome.disposition",
                outcome.disposition_type().0,
            ));
        };
        let message = decoder.text("UnsubscribeOutcome.message", outcome.message())?;
        let diagnostics = Diagnostic::decode_all(
            decoder,
            "UnsubscribeOutcome.diagnostics",
            outcome.diagnostics(),
        )?;
        Ok(Self {
            disposition,
            message,
            diagnostics,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelState {
    /// The target is in flight. Its own terminal reply follows.
    Requested,
    /// No request with the target identity is in flight.
    NotInFlight,
}

wire_enum!(ALL_CANCEL_STATES: CancelState => wire::CancelState { Requested, NotInFlight });

/// The reply to a cancel request itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelOutcome {
    pub target: RequestId,
    pub state: CancelState,
}

impl CancelOutcome {
    pub(crate) fn encode_body(&self, encoder: &mut Encoder<'_>) -> EncodedUnion<wire::ReplyBody> {
        let outcome = wire::CancelOutcome::create(
            encoder.fbb(),
            &wire::CancelOutcomeArgs {
                target_request_id: self.target.wire(),
                state: Some(self.state.into()),
            },
        );
        EncodedUnion::new(wire::ReplyBody::CancelOutcome, outcome)
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        outcome: wire::CancelOutcome<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let target = RequestId::decode(
            decoder,
            "CancelOutcome.target_request_id",
            outcome.target_request_id(),
        )?;
        let state = decoder.required_enumeration("CancelOutcome.state", outcome.state())?;
        Ok(Self { target, state })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancellationStage {
    /// The request was not admitted and has no effects.
    BeforeAdmission,
    /// The request was admitted. Its effects may still complete; recover them by identity.
    AfterAdmission,
}

wire_enum!(ALL_CANCELLATION_STAGES: CancellationStage => wire::CancellationStage {
    BeforeAdmission,
    AfterAdmission,
});

/// The terminal reply of a cancelled request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCancelled {
    pub stage: CancellationStage,
}

impl RequestCancelled {
    pub(crate) fn encode_body(&self, encoder: &mut Encoder<'_>) -> EncodedUnion<wire::ReplyBody> {
        let cancelled = wire::RequestCancelled::create(
            encoder.fbb(),
            &wire::RequestCancelledArgs {
                stage: Some(self.stage.into()),
            },
        );
        EncodedUnion::new(wire::ReplyBody::RequestCancelled, cancelled)
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        cancelled: wire::RequestCancelled<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let stage = decoder.required_enumeration("RequestCancelled.stage", cancelled.stage())?;
        Ok(Self { stage })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestRejection {
    /// A field is missing, malformed or out of range.
    InvalidRequest,
    /// The request is not served on this transport.
    UnsupportedRequest,
    /// An enum value the server does not support.
    UnsupportedValue,
    /// The request identity is already in flight in this session.
    DuplicateRequestId,
    /// The complete reply is larger than the session transfer limit.
    ReplyTooLarge,
}

wire_enum!(ALL_REQUEST_REJECTIONS: RequestRejection => wire::RequestRejection {
    InvalidRequest,
    UnsupportedRequest,
    UnsupportedValue,
    DuplicateRequestId,
    ReplyTooLarge,
});

/// The request was not served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRejected {
    pub rejection: RequestRejection,
    /// The offending field, when the rejection concerns one.
    pub field: Option<String>,
    pub message: String,
}

impl RequestRejected {
    pub(crate) fn encode_body(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ReplyBody>, Report<WireEncodeError>> {
        let field = encoder.optional_text("RequestRejected.field", self.field.as_deref())?;
        let message = encoder.text("RequestRejected.message", &self.message)?;
        let rejected = wire::RequestRejected::create(
            encoder.fbb(),
            &wire::RequestRejectedArgs {
                rejection: Some(self.rejection.into()),
                field,
                message: Some(message),
            },
        );
        Ok(EncodedUnion::new(
            wire::ReplyBody::RequestRejected,
            rejected,
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        rejected: wire::RequestRejected<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let rejection =
            decoder.required_enumeration("RequestRejected.rejection", rejected.rejection())?;
        let field = decoder.optional_text("RequestRejected.field", rejected.field())?;
        let message = decoder.text("RequestRejected.message", rejected.message())?;
        Ok(Self {
            rejection,
            field,
            message,
        })
    }
}
