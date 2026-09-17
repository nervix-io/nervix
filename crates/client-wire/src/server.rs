//! Server frames: replies that answer requests, the parts of replies too large for one frame, and
//! unsolicited messages.
//!
//! A reply always names the request it answers, and an unsolicited message never names one, so a
//! client can route every frame without guessing which waiter it belongs to.

use error_stack::Report;
use meticulous::OptionExt as _;

use crate::{
    codec::{DecodeError, Decoder, EncodeError, EncodedUnion, Encoder},
    command::{AttachOutcome, CommandOutcome},
    common::RequestId,
    domain::{
        ClusterObserved, DomainList, DomainSelection, DomainSnapshotObserved, DomainsObserved,
    },
    event::{LeadershipObserved, ServerNotice, SessionEnding},
    frame::{EncodedFrame, ServerFrame, VerifiedFrame},
    limits::SessionLimits,
    reply::{
        CancelOutcome, InspectionOutcome, RequestCancelled, RequestRejected, SubscribeOutcome,
        SuggestOutcome, UnsubscribeOutcome,
    },
    subscription::{
        SubscriptionDeliveryLost, SubscriptionEnded, SubscriptionRows, SubscriptionRowsSkipped,
    },
    transfer::{TransferPart, TransferParts},
    wire,
};

/// The complete body of a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyBody {
    Command(Box<CommandOutcome>),
    Attach(AttachOutcome),
    Suggest(SuggestOutcome),
    DomainList(DomainList),
    DomainSelection(DomainSelection),
    Inspection(InspectionOutcome),
    Subscribe(SubscribeOutcome),
    Unsubscribe(UnsubscribeOutcome),
    Cancel(CancelOutcome),
    Cancelled(RequestCancelled),
    Rejected(RequestRejected),
}

/// A complete reply to one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub request_id: RequestId,
    pub body: ReplyBody,
}

/// How a reply travels: in one frame, or as transfer parts when it is larger than a frame.
#[derive(Debug)]
pub enum ReplyDelivery {
    Frame(EncodedFrame<ServerFrame>),
    Transfer(TransferParts),
}

impl Reply {
    /// Encodes the reply, splitting it into transfer parts when it does not fit one frame.
    ///
    /// A reply larger than the session transfer limit is refused; the server answers the request
    /// with a rejection instead of truncating it.
    pub fn encode(&self, limits: &SessionLimits) -> Result<ReplyDelivery, Report<EncodeError>> {
        let mut encoder = Encoder::new(limits.transfer_bytes(), limits);
        let body = match &self.body {
            ReplyBody::Command(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::Attach(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::Suggest(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::DomainList(list) => list.encode_body(&mut encoder)?,
            ReplyBody::DomainSelection(selection) => selection.encode_body(&mut encoder)?,
            ReplyBody::Inspection(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::Subscribe(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::Unsubscribe(outcome) => outcome.encode_body(&mut encoder)?,
            ReplyBody::Cancel(outcome) => outcome.encode_body(&mut encoder),
            ReplyBody::Cancelled(cancelled) => cancelled.encode_body(&mut encoder),
            ReplyBody::Rejected(rejected) => rejected.encode_body(&mut encoder)?,
        };
        let reply = wire::Reply::create(
            encoder.fbb(),
            &wire::ReplyArgs {
                request_id: self.request_id.wire(),
                body_type: body.discriminant,
                body: Some(body.value),
            },
        );
        let frame =
            finish_server_message(encoder, EncodedUnion::new(wire::ServerBody::Reply, reply))?;
        if frame.len() <= limits.frame_bytes() {
            return Ok(ReplyDelivery::Frame(frame));
        }
        Ok(ReplyDelivery::Transfer(TransferParts::new(
            self.request_id,
            frame.into_bytes(),
            limits,
        )))
    }

    fn decode(
        decoder: Decoder<'_>,
        request_id: RequestId,
        reply: wire::Reply<'_>,
    ) -> Result<Self, Report<DecodeError>> {
        let body = match reply.body_type() {
            wire::ReplyBody::CommandOutcome => ReplyBody::Command(Box::new(
                CommandOutcome::decode(decoder, reply_member(reply.body_as_command_outcome()))?,
            )),
            wire::ReplyBody::AttachOutcome => ReplyBody::Attach(AttachOutcome::decode(
                decoder,
                reply_member(reply.body_as_attach_outcome()),
            )?),
            wire::ReplyBody::SuggestOutcome => ReplyBody::Suggest(SuggestOutcome::decode(
                decoder,
                reply_member(reply.body_as_suggest_outcome()),
            )?),
            wire::ReplyBody::DomainList => ReplyBody::DomainList(DomainList::decode(
                decoder,
                reply_member(reply.body_as_domain_list()),
            )?),
            wire::ReplyBody::DomainSelectionOutcome => {
                ReplyBody::DomainSelection(DomainSelection::decode(
                    decoder,
                    reply_member(reply.body_as_domain_selection_outcome()),
                )?)
            }
            wire::ReplyBody::InspectionOutcome => ReplyBody::Inspection(InspectionOutcome::decode(
                decoder,
                reply_member(reply.body_as_inspection_outcome()),
            )?),
            wire::ReplyBody::SubscribeOutcome => ReplyBody::Subscribe(SubscribeOutcome::decode(
                decoder,
                reply_member(reply.body_as_subscribe_outcome()),
            )?),
            wire::ReplyBody::UnsubscribeOutcome => {
                ReplyBody::Unsubscribe(UnsubscribeOutcome::decode(
                    decoder,
                    reply_member(reply.body_as_unsubscribe_outcome()),
                )?)
            }
            wire::ReplyBody::CancelOutcome => ReplyBody::Cancel(CancelOutcome::decode(
                decoder,
                reply_member(reply.body_as_cancel_outcome()),
            )?),
            wire::ReplyBody::RequestCancelled => ReplyBody::Cancelled(RequestCancelled::decode(
                decoder,
                reply_member(reply.body_as_request_cancelled()),
            )?),
            wire::ReplyBody::RequestRejected => ReplyBody::Rejected(RequestRejected::decode(
                decoder,
                reply_member(reply.body_as_request_rejected()),
            )?),
            undeclared => return Err(decoder.unknown_union("Reply.body", undeclared.0)),
        };
        Ok(Self { request_id, body })
    }
}

/// Reads the reply member a matched discriminant names.
fn reply_member<T>(member: Option<T>) -> T {
    member.assured("a reply's discriminant names the member its accessor reads")
}

/// A message the server sends without a request.
#[derive(Debug, Clone)]
pub enum ServerEvent {
    Notice(ServerNotice),
    Leadership(LeadershipObserved),
    Domains(DomainsObserved),
    DomainSnapshot(DomainSnapshotObserved),
    Cluster(ClusterObserved),
    SubscriptionRows(SubscriptionRows),
    SubscriptionDeliveryLost(SubscriptionDeliveryLost),
    SubscriptionRowsSkipped(SubscriptionRowsSkipped),
    SubscriptionEnded(SubscriptionEnded),
    SessionEnding(SessionEnding),
}

/// Everything a server frame can hold.
#[derive(Debug, Clone)]
pub enum ServerMessage {
    Reply(Reply),
    /// One part of a reply too large for a single frame; see [`crate::TransferAssembly`].
    TransferPart(TransferPart),
    Event(ServerEvent),
}

impl ServerMessage {
    pub fn decode(frame: &VerifiedFrame<ServerFrame>) -> Result<Self, Report<DecodeError>> {
        let decoder = Decoder::new(frame.limits());
        let message = frame.root();
        let event = match message.body_type() {
            wire::ServerBody::Reply => {
                let reply = server_member(message.body_as_reply());
                let request_id =
                    RequestId::decode(decoder, "Reply.request_id", reply.request_id())?;
                if let Some(part) = reply.body_as_transfer_part() {
                    let part = TransferPart::decode(frame, decoder, request_id, part)?;
                    return Ok(Self::TransferPart(part));
                }
                return Ok(Self::Reply(Reply::decode(decoder, request_id, reply)?));
            }
            wire::ServerBody::ServerNotice => ServerEvent::Notice(ServerNotice::decode(
                decoder,
                server_member(message.body_as_server_notice()),
            )?),
            wire::ServerBody::LeadershipObserved => {
                ServerEvent::Leadership(LeadershipObserved::decode(
                    decoder,
                    server_member(message.body_as_leadership_observed()),
                )?)
            }
            wire::ServerBody::DomainsObserved => ServerEvent::Domains(DomainsObserved::decode(
                decoder,
                server_member(message.body_as_domains_observed()),
            )?),
            wire::ServerBody::DomainSnapshotObserved => {
                ServerEvent::DomainSnapshot(DomainSnapshotObserved::decode(
                    frame,
                    decoder,
                    server_member(message.body_as_domain_snapshot_observed()),
                )?)
            }
            wire::ServerBody::ClusterObserved => ServerEvent::Cluster(ClusterObserved::decode(
                server_member(message.body_as_cluster_observed()),
            )),
            wire::ServerBody::SubscriptionRows => {
                ServerEvent::SubscriptionRows(SubscriptionRows::decode(
                    frame,
                    decoder,
                    server_member(message.body_as_subscription_rows()),
                )?)
            }
            wire::ServerBody::SubscriptionDeliveryLost => {
                ServerEvent::SubscriptionDeliveryLost(SubscriptionDeliveryLost::decode(
                    decoder,
                    server_member(message.body_as_subscription_delivery_lost()),
                )?)
            }
            wire::ServerBody::SubscriptionRowsSkipped => {
                ServerEvent::SubscriptionRowsSkipped(SubscriptionRowsSkipped::decode(
                    decoder,
                    server_member(message.body_as_subscription_rows_skipped()),
                )?)
            }
            wire::ServerBody::SubscriptionEnded => {
                ServerEvent::SubscriptionEnded(SubscriptionEnded::decode(
                    decoder,
                    server_member(message.body_as_subscription_ended()),
                )?)
            }
            wire::ServerBody::SessionEnding => ServerEvent::SessionEnding(SessionEnding::decode(
                decoder,
                server_member(message.body_as_session_ending()),
            )?),
            undeclared => {
                return Err(decoder.unknown_union("ServerMessage.body", undeclared.0));
            }
        };
        Ok(Self::Event(event))
    }
}

/// Reads the server message member a matched discriminant names.
fn server_member<T>(member: Option<T>) -> T {
    member.assured("a server message's discriminant names the member its accessor reads")
}

impl VerifiedFrame<ServerFrame> {
    /// The request a reply or transfer part answers, read without decoding the rest of the frame.
    ///
    /// `None` for an unsolicited message, and for a reply whose request identity is zero, which
    /// decoding then rejects.
    pub fn request_id(&self) -> Option<RequestId> {
        let reply = self.root().body_as_reply()?;
        let request_id = std::num::NonZeroU64::new(reply.request_id())?;
        Some(RequestId::new(request_id))
    }
}

/// Finishes a server frame around its body.
pub(crate) fn finish_server_message(
    mut encoder: Encoder<'_>,
    body: EncodedUnion<wire::ServerBody>,
) -> Result<EncodedFrame<ServerFrame>, Report<EncodeError>> {
    let message = wire::ServerMessage::create(
        encoder.fbb(),
        &wire::ServerMessageArgs {
            body_type: body.discriminant,
            body: Some(body.value),
        },
    );
    encoder.finish::<ServerFrame>(message)
}
