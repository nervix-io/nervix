//! The wire contract of the Nervix client session.
//!
//! The FlatBuffers schema in `schema/` is the contract every client implementation, in any
//! language, is written against. This crate is its Rust implementation: frames are verified once
//! when their bytes are taken, read through borrowed views and typed values, and encoded within
//! the same limits a receiver enforces.
//!
//! Layer: edges.
//!
//! - **Owns.** The session schema, frame verification and ownership, the typed requests, replies,
//!   transfers, events and rows the schema describes, the text every client displays a row as,
//!   the session limits, and how frames travel over gRPC and WebSocket messages.
//! - **Depends on.** `flatbuffers`, the vocabulary for names, timestamps, schema fields, the
//!   transaction impact report, the resource description and the status, inspection envelope and
//!   preview identity a session exchanges, `serde_json` to write a row's display text, and tonic's codec traits for the gRPC
//!   transport.
//! - **Must not know.** The server's registry, runtime or consensus, the parser, Arrow, or any
//!   client's dispatch, reconnection or subscription state.

include!(concat!(env!("OUT_DIR"), "/flatbuffers/session_module.rs"));

use generated::nervix::session as wire;

mod codec;
mod command;
mod common;
mod domain;
mod event;
mod frame;
#[cfg(feature = "grpc")]
pub mod grpc;
mod impact;
mod limits;
mod reply;
mod request;
mod resource;
mod row;
mod row_text;
mod server;
mod subscription;
mod transaction;
mod transfer;
mod upload;
pub mod websocket;

pub use codec::{WireDecodeError, WireEncodeError};
pub use command::{
    AttachDisposition, AttachOutcome, CommandDisposition, CommandOutcome,
    ExecutionReferenceConflict, StatementDisposition, StatementOutcome, UnknownOutcomeCause,
};
pub use common::{
    Diagnostic, LeaderEndpoints, LeaderRedirect, OutcomeOrigin, RequestId, SourceSpan,
    WireValueError,
};
pub use domain::{
    ClusterObserved, DomainEntity, DomainInfo, DomainList, DomainSelection, DomainSnapshotObserved,
    DomainsObserved,
};
pub use event::{
    Leadership, LeadershipObserved, NoticeLevel, ServerNotice, SessionEndReason, SessionEnding,
};
pub use frame::{
    ClientFrame, EncodedFrame, FrameError, FrameRoot, FrameViolation, ServerFrame, UploadFrame,
    UploadReplyFrame, VerifiedFrame,
};
pub use limits::{LimitsError, SessionLimitSettings, SessionLimits};
pub use reply::{
    CancelOutcome, CancelState, CancellationStage, InspectionOutcome, RequestCancelled,
    RequestRejected, RequestRejection, SubscribeDisposition, SubscribeOutcome, SubscriptionOpened,
    SuggestOutcome, Suggestion, SuggestionKind, UnsubscribeDisposition, UnsubscribeOutcome,
};
pub use request::{
    AttachTransactionRequest, CancelRequest, ClientMessage, ClientRequest, CommandRequest,
    InspectTransactionRequest, SelectDomainRequest, SubscribeRequest, SuggestRequest,
    UnsubscribeRequest,
};
pub use row::{
    CellView, CellWriter, CellsView, EmptyBranchKey, RowBatchView, RowBranch, RowConformanceError,
    RowLocation, RowSchema,
};
pub use server::{Reply, ReplyBody, ReplyDelivery, ServerEvent, ServerMessage};
pub use subscription::{
    RowsSkippedCause, SubscriptionDeliveryLost, SubscriptionEndReason, SubscriptionEnded,
    SubscriptionHandle, SubscriptionRows, SubscriptionRowsEncoder, SubscriptionRowsSkipped,
    SubscriptionType,
};
pub use transfer::{TransferAssembly, TransferError, TransferPart, TransferParts};
pub use upload::{
    UploadChunk, UploadDisposition, UploadFailure, UploadMessage, UploadReply, UploadStart,
};

#[cfg(test)]
mod tests;
