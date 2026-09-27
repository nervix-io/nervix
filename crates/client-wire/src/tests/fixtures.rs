//! Builders shared by the codec tests.

use std::{
    fmt::Debug,
    num::{NonZeroU64, NonZeroUsize},
};

use bytes::Bytes;
use error_stack::Report;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CommandExecutionReference, NameError, TransactionOperationNumber};

use crate::{
    ClientFrame, ClientMessage, EncodedFrame, FrameError, FrameRoot, Reply, ReplyDelivery,
    RequestId, ServerEvent, ServerFrame, ServerMessage, SessionLimitSettings, SessionLimits,
    VerifiedFrame, WireDecodeError, wire,
};

pub(crate) fn limits() -> SessionLimits {
    SessionLimits::DEFAULT
}

pub(crate) fn size(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).assured("the test passes a non-zero size")
}

/// The default limits as settings, for a test to adjust one of them.
pub(crate) fn settings() -> SessionLimitSettings {
    let defaults = SessionLimits::DEFAULT;
    SessionLimitSettings {
        frame_bytes: size(defaults.frame_bytes()),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(defaults.string_bytes()),
    }
}

pub(crate) fn checked(settings: SessionLimitSettings) -> SessionLimits {
    SessionLimits::try_from(settings).assured("the test adjusts limits within their checks")
}

pub(crate) fn name<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str, Error = NameError>,
{
    N::try_from(raw).assured("the test passes a valid name")
}

pub(crate) fn request(id: u64) -> RequestId {
    RequestId::new(NonZeroU64::new(id).assured("the test passes a non-zero request identity"))
}

pub(crate) fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).assured("the test passes a non-zero value")
}

pub(crate) fn operation(number: usize) -> TransactionOperationNumber {
    TransactionOperationNumber::new(size(number))
}

pub(crate) fn reference(raw: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(raw).assured("the test passes a valid execution reference")
}

pub(crate) fn round_trip_client(message: &ClientMessage) -> ClientMessage {
    let frame = message
        .encode(&limits())
        .assured("the test message fits the default limits");
    let frame = frame
        .verify(&limits())
        .assured("an encoded frame verifies under the limits it was encoded for");
    ClientMessage::decode(&frame).assured("an encoded client message decodes")
}

pub(crate) fn verify_server(frame: EncodedFrame<ServerFrame>) -> VerifiedFrame<ServerFrame> {
    frame
        .verify(&limits())
        .assured("an encoded frame verifies under the limits it was encoded for")
}

pub(crate) fn decode_server(frame: EncodedFrame<ServerFrame>) -> ServerMessage {
    ServerMessage::decode(&verify_server(frame)).assured("an encoded server message decodes")
}

pub(crate) fn round_trip_reply(reply: &Reply) -> Reply {
    let delivery = reply
        .encode(&limits())
        .assured("the test reply fits the default limits");
    let ReplyDelivery::Frame(frame) = delivery else {
        panic!("a test reply below the frame limit is delivered in one frame");
    };
    match decode_server(frame) {
        ServerMessage::Reply(decoded) => decoded,
        other => panic!("a reply frame decoded as {other:?}"),
    }
}

pub(crate) fn decode_event(frame: EncodedFrame<ServerFrame>) -> ServerEvent {
    match decode_server(frame) {
        ServerMessage::Event(event) => event,
        other => panic!("an event frame decoded as {other:?}"),
    }
}

/// Finishes a hand-built frame, for frames the typed encoders refuse to produce.
pub(crate) fn finish_raw<T>(
    mut builder: FlatBufferBuilder<'_>,
    root: WIPOffset<T>,
    identifier: &str,
) -> Bytes {
    builder.finish(root, Some(identifier));
    Bytes::copy_from_slice(builder.finished_data())
}

/// Finishes a hand-built reply to request 5 as a server frame.
pub(crate) fn finish_reply(
    mut builder: FlatBufferBuilder<'_>,
    body_type: wire::ReplyBody,
    body: WIPOffset<UnionWIPOffset>,
) -> Bytes {
    let reply = wire::Reply::create(
        &mut builder,
        &wire::ReplyArgs {
            request_id: 5,
            body_type,
            body: Some(body),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::Reply,
            body: Some(reply.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

pub(crate) fn verify_raw<R: FrameRoot>(bytes: Bytes) -> VerifiedFrame<R> {
    VerifiedFrame::verify(bytes, &limits()).assured("the hand-built frame is structurally valid")
}

pub(crate) fn raw_client(bytes: Bytes) -> VerifiedFrame<ClientFrame> {
    verify_raw(bytes)
}

pub(crate) fn raw_server(bytes: Bytes) -> VerifiedFrame<ServerFrame> {
    verify_raw(bytes)
}

/// The context of a decoding failure.
pub(crate) fn decode_error<T: Debug>(
    result: Result<T, Report<WireDecodeError>>,
) -> WireDecodeError {
    match result {
        Ok(value) => panic!("decoding succeeded with {value:?}"),
        Err(error) => error.current_context().clone(),
    }
}

/// The context of a frame verification failure.
pub(crate) fn frame_error<R: FrameRoot>(
    result: Result<VerifiedFrame<R>, Report<FrameError>>,
) -> FrameError {
    match result {
        Ok(frame) => panic!("verification succeeded for {frame:?}"),
        Err(error) => error.current_context().clone(),
    }
}

/// Where `marker` starts inside `bytes`.
pub(crate) fn position_of(bytes: &[u8], marker: &[u8]) -> usize {
    bytes
        .windows(marker.len())
        .position(|window| window == marker)
        .assured("the test frame holds its marker")
}
