//! Client frames: the requests of a session.

use error_stack::Report;
use flatbuffers::WIPOffset;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    CommandExecutionReference, DomainName, SubscriptionName, TransactionInspectionTarget,
    TransactionOperationNumber, TransactionPosition, TransactionPreviewIdentity,
};

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_size},
    common::{RequestId, WireValueError},
    frame::{ClientFrame, EncodedFrame, VerifiedFrame},
    limits::SessionLimits,
    subscription::SubscriptionType,
    transaction::{
        decode_inspection_target, decode_operation_number, decode_preview_identity,
        encode_inspection_target, encode_operation_number, encode_preview_identity,
    },
    wire,
};

/// Executes NSPL statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRequest {
    pub query: String,
    /// The selected domain, when one is selected.
    pub domain: Option<DomainName>,
    /// The durable identity of the command's effects. A retry after a lost reply, a redirect or a
    /// reconnect repeats it and recovers the recorded outcome.
    pub execution_reference: CommandExecutionReference,
    /// The position an append to the bound transaction expects.
    pub expected_transaction_position: Option<TransactionPosition>,
    /// The whole-transaction preview a COMMIT expects to apply.
    pub expected_preview: Option<TransactionPreviewIdentity>,
}

/// Asks for completions of NSPL source at a cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuggestRequest {
    input: String,
    cursor: usize,
    domain: Option<DomainName>,
}

impl SuggestRequest {
    /// A request for completions at `cursor`, a UTF-8 byte offset on a character boundary of
    /// `input`.
    pub fn new(
        input: String,
        cursor: usize,
        domain: Option<DomainName>,
    ) -> Result<Self, Report<WireValueError>> {
        if !input.is_char_boundary(cursor) {
            return Err(Report::new(WireValueError::CursorOffCharBoundary {
                cursor,
                length: input.len(),
            }));
        }
        Ok(Self {
            input,
            cursor,
            domain,
        })
    }

    pub fn input(&self) -> &str {
        &self.input
    }

    /// A byte offset on a character boundary of the input.
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn domain(&self) -> Option<&DomainName> {
        self.domain.as_ref()
    }
}

/// Selects the domain whose observations the session receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectDomainRequest {
    pub domain: DomainName,
}

/// Binds an existing transaction to the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachTransactionRequest {
    pub transaction_id: String,
}

/// Reads a transaction's impact report without attaching or changing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectTransactionRequest {
    pub target: TransactionInspectionTarget,
    /// The operation to inspect, or `None` for the whole transaction.
    pub operation: Option<TransactionOperationNumber>,
}

/// Opens a subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub domain: DomainName,
    /// Exactly one CREATE SUBSCRIPTION statement.
    pub statement: String,
    pub subscription_type: SubscriptionType,
}

/// Closes a subscription of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeRequest {
    pub subscription: SubscriptionName,
}

/// Stops waiting for an in-flight request. Cancelling a waiter never rolls back admitted work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelRequest {
    pub target: RequestId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequest {
    Command(CommandRequest),
    Suggest(SuggestRequest),
    ListDomains,
    SelectDomain(SelectDomainRequest),
    AttachTransaction(AttachTransactionRequest),
    InspectTransaction(InspectTransactionRequest),
    Subscribe(SubscribeRequest),
    Unsubscribe(UnsubscribeRequest),
    Cancel(CancelRequest),
}

/// One request of a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMessage {
    pub request_id: RequestId,
    pub request: ClientRequest,
}

impl ClientMessage {
    pub fn encode(
        &self,
        limits: &SessionLimits,
    ) -> Result<EncodedFrame<ClientFrame>, Report<WireEncodeError>> {
        let mut encoder = Encoder::new(limits.frame_bytes(), limits);
        let request = self.request.encode(&mut encoder)?;
        let message = wire::ClientMessage::create(
            encoder.fbb(),
            &wire::ClientMessageArgs {
                request_id: self.request_id.wire(),
                request_type: request.discriminant,
                request: Some(request.value),
            },
        );
        encoder.finish::<ClientFrame>(message)
    }

    /// Decodes a verified client frame.
    ///
    /// A failure names the request it concerns whenever the frame carries a valid request
    /// identity, which [`VerifiedFrame::request_id`] reads, so the server can reject that request
    /// and keep the session.
    pub fn decode(frame: &VerifiedFrame<ClientFrame>) -> Result<Self, Report<WireDecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let message = frame.root();
        let request_id =
            RequestId::decode(decoder, "ClientMessage.request_id", message.request_id())?;
        let request = ClientRequest::decode(decoder, message)?;
        Ok(Self {
            request_id,
            request,
        })
    }
}

impl VerifiedFrame<ClientFrame> {
    /// The request identity, read without decoding the request. `None` when it is zero, which
    /// decoding rejects.
    pub fn request_id(&self) -> Option<RequestId> {
        let request_id = std::num::NonZeroU64::new(self.root().request_id())?;
        Some(RequestId::new(request_id))
    }
}

impl ClientRequest {
    fn encode(
        &self,
        encoder: &mut Encoder<'_>,
    ) -> Result<EncodedUnion<wire::ClientRequest>, Report<WireEncodeError>> {
        let union = match self {
            Self::Command(command) => EncodedUnion::new(
                wire::ClientRequest::CommandRequest,
                command.encode(encoder)?,
            ),
            Self::Suggest(suggest) => {
                let input = encoder.text("SuggestRequest.input", &suggest.input)?;
                let domain = suggest.domain.as_ref().map(DomainName::as_str);
                let domain = encoder.optional_text("SuggestRequest.domain", domain)?;
                let cursor = u32::try_from(suggest.cursor).verified(
                    "the cursor lies within the input, which passed a string limit of at most \
                     MAX_FRAME_BYTES above",
                );
                let request = wire::SuggestRequest::create(
                    encoder.fbb(),
                    &wire::SuggestRequestArgs {
                        input: Some(input),
                        cursor,
                        domain,
                    },
                );
                EncodedUnion::new(wire::ClientRequest::SuggestRequest, request)
            }
            Self::ListDomains => EncodedUnion::new(
                wire::ClientRequest::ListDomainsRequest,
                wire::ListDomainsRequest::create(encoder.fbb(), &wire::ListDomainsRequestArgs {}),
            ),
            Self::SelectDomain(select) => {
                let domain = encoder.text("SelectDomainRequest.domain", select.domain.as_str())?;
                let request = wire::SelectDomainRequest::create(
                    encoder.fbb(),
                    &wire::SelectDomainRequestArgs {
                        domain: Some(domain),
                    },
                );
                EncodedUnion::new(wire::ClientRequest::SelectDomainRequest, request)
            }
            Self::AttachTransaction(attach) => {
                let transaction_id = encoder.text(
                    "AttachTransactionRequest.transaction_id",
                    &attach.transaction_id,
                )?;
                let request = wire::AttachTransactionRequest::create(
                    encoder.fbb(),
                    &wire::AttachTransactionRequestArgs {
                        transaction_id: Some(transaction_id),
                    },
                );
                EncodedUnion::new(wire::ClientRequest::AttachTransactionRequest, request)
            }
            Self::InspectTransaction(inspect) => {
                let target = encode_inspection_target(encoder, &inspect.target)?;
                let operation = inspect.operation.map(encode_operation_number);
                let request = wire::InspectTransactionRequest::create(
                    encoder.fbb(),
                    &wire::InspectTransactionRequestArgs {
                        target_type: target.discriminant,
                        target: Some(target.value),
                        operation,
                    },
                );
                EncodedUnion::new(wire::ClientRequest::InspectTransactionRequest, request)
            }
            Self::Subscribe(subscribe) => {
                let domain = encoder.text("SubscribeRequest.domain", subscribe.domain.as_str())?;
                let statement = encoder.text("SubscribeRequest.statement", &subscribe.statement)?;
                let request = wire::SubscribeRequest::create(
                    encoder.fbb(),
                    &wire::SubscribeRequestArgs {
                        domain: Some(domain),
                        statement: Some(statement),
                        subscription_type: Some(subscribe.subscription_type.into()),
                    },
                );
                EncodedUnion::new(wire::ClientRequest::SubscribeRequest, request)
            }
            Self::Unsubscribe(unsubscribe) => {
                let subscription = encoder.text(
                    "UnsubscribeRequest.subscription",
                    unsubscribe.subscription.as_str(),
                )?;
                let request = wire::UnsubscribeRequest::create(
                    encoder.fbb(),
                    &wire::UnsubscribeRequestArgs {
                        subscription: Some(subscription),
                    },
                );
                EncodedUnion::new(wire::ClientRequest::UnsubscribeRequest, request)
            }
            Self::Cancel(cancel) => {
                let request = wire::CancelRequest::create(
                    encoder.fbb(),
                    &wire::CancelRequestArgs {
                        target_request_id: cancel.target.wire(),
                    },
                );
                EncodedUnion::new(wire::ClientRequest::CancelRequest, request)
            }
        };
        Ok(union)
    }

    fn decode(
        decoder: Decoder<'_>,
        message: wire::ClientMessage<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let request = match message.request_type() {
            wire::ClientRequest::CommandRequest => Self::Command(CommandRequest::decode(
                decoder,
                request_member(message.request_as_command_request()),
            )?),
            wire::ClientRequest::SuggestRequest => {
                let suggest = request_member(message.request_as_suggest_request());
                let input = decoder.text("SuggestRequest.input", suggest.input())?;
                let domain = decoder.optional_name("SuggestRequest.domain", suggest.domain())?;
                let cursor = decoder.size("SuggestRequest.cursor", u64::from(suggest.cursor()))?;
                match SuggestRequest::new(input, cursor, domain) {
                    Ok(request) => Self::Suggest(request),
                    Err(error) => {
                        return Err(error.change_context(WireDecodeError::InvalidValue {
                            field: "SuggestRequest.cursor",
                            kind: "character boundary of the input",
                        }));
                    }
                }
            }
            wire::ClientRequest::ListDomainsRequest => Self::ListDomains,
            wire::ClientRequest::SelectDomainRequest => {
                let select = request_member(message.request_as_select_domain_request());
                Self::SelectDomain(SelectDomainRequest {
                    domain: decoder.name("SelectDomainRequest.domain", select.domain())?,
                })
            }
            wire::ClientRequest::AttachTransactionRequest => {
                let attach = request_member(message.request_as_attach_transaction_request());
                Self::AttachTransaction(AttachTransactionRequest {
                    transaction_id: decoder.text(
                        "AttachTransactionRequest.transaction_id",
                        attach.transaction_id(),
                    )?,
                })
            }
            wire::ClientRequest::InspectTransactionRequest => {
                let inspect = request_member(message.request_as_inspect_transaction_request());
                let target = decode_inspection_target(decoder, inspect)?;
                let operation = match inspect.operation() {
                    Some(operation) => Some(decode_operation_number(
                        decoder,
                        "InspectTransactionRequest.operation",
                        operation,
                    )?),
                    None => None,
                };
                Self::InspectTransaction(InspectTransactionRequest { target, operation })
            }
            wire::ClientRequest::SubscribeRequest => {
                let subscribe = request_member(message.request_as_subscribe_request());
                Self::Subscribe(SubscribeRequest {
                    domain: decoder.name("SubscribeRequest.domain", subscribe.domain())?,
                    statement: decoder.text("SubscribeRequest.statement", subscribe.statement())?,
                    subscription_type: decoder.required_enumeration(
                        "SubscribeRequest.subscription_type",
                        subscribe.subscription_type(),
                    )?,
                })
            }
            wire::ClientRequest::UnsubscribeRequest => {
                let unsubscribe = request_member(message.request_as_unsubscribe_request());
                Self::Unsubscribe(UnsubscribeRequest {
                    subscription: decoder.name(
                        "UnsubscribeRequest.subscription",
                        unsubscribe.subscription(),
                    )?,
                })
            }
            wire::ClientRequest::CancelRequest => {
                let cancel = request_member(message.request_as_cancel_request());
                Self::Cancel(CancelRequest {
                    target: RequestId::decode(
                        decoder,
                        "CancelRequest.target_request_id",
                        cancel.target_request_id(),
                    )?,
                })
            }
            undeclared => {
                return Err(decoder.unknown_union("ClientMessage.request", undeclared.0));
            }
        };
        Ok(request)
    }
}

/// Reads the request member a matched discriminant names.
fn request_member<T>(member: Option<T>) -> T {
    member.assured("a client message's discriminant names the member its accessor reads")
}

impl CommandRequest {
    fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
    ) -> Result<WIPOffset<wire::CommandRequest<'fbb>>, Report<WireEncodeError>> {
        let query = encoder.text("CommandRequest.query", &self.query)?;
        let domain = self.domain.as_ref().map(DomainName::as_str);
        let domain = encoder.optional_text("CommandRequest.domain", domain)?;
        let execution_reference = encoder.text(
            "CommandRequest.execution_reference",
            self.execution_reference.as_str(),
        )?;
        let expected_transaction_position = self
            .expected_transaction_position
            .map(|position| wire_size(position.accepted_operations()));
        let expected_preview = match &self.expected_preview {
            Some(preview) => Some(encode_preview_identity(encoder, preview)?),
            None => None,
        };
        Ok(wire::CommandRequest::create(
            encoder.fbb(),
            &wire::CommandRequestArgs {
                query: Some(query),
                domain,
                execution_reference: Some(execution_reference),
                expected_transaction_position,
                expected_preview,
            },
        ))
    }

    fn decode(
        decoder: Decoder<'_>,
        command: wire::CommandRequest<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let query = decoder.text("CommandRequest.query", command.query())?;
        let domain = decoder.optional_name("CommandRequest.domain", command.domain())?;
        let execution_reference = decoder.check_text(
            "CommandRequest.execution_reference",
            command.execution_reference(),
        )?;
        let execution_reference = match CommandExecutionReference::parse(execution_reference) {
            Ok(reference) => reference,
            Err(error) => {
                return Err(error.change_context(WireDecodeError::InvalidValue {
                    field: "CommandRequest.execution_reference",
                    kind: "execution reference",
                }));
            }
        };
        let expected_transaction_position = match command.expected_transaction_position() {
            Some(position) => {
                let position =
                    decoder.size("CommandRequest.expected_transaction_position", position)?;
                Some(TransactionPosition::new(position))
            }
            None => None,
        };
        let expected_preview = match command.expected_preview() {
            Some(preview) => Some(decode_preview_identity(decoder, preview)?),
            None => None,
        };
        Ok(Self {
            query,
            domain,
            execution_reference,
            expected_transaction_position,
            expected_preview,
        })
    }
}
