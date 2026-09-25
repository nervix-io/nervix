//! Why a client call failed.
//!
//! - **Owns.** The failures a caller of the client can act on, and which request each concerns.
//! - **Depends on.** The wire contract's rejection, cancellation and codec errors, and tonic's
//!   transport errors.
//! - **Must not know.** How a caller recovers; the client's own retries happen before an error is
//!   returned.

use nervix_client_wire::{
    CancellationStage, ClientRequest, ReplyBody, RequestRejection, WireDecodeError, WireEncodeError,
};
use nervix_models::{CommandExecutionReference, NameError, ResourceUploadIdentity};
use thiserror::Error;
use tonic::metadata::errors::InvalidMetadataValue;

/// The requests of the session protocol, as errors about their replies name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum RequestKind {
    #[strum(serialize = "command")]
    Command,
    #[strum(serialize = "suggest")]
    Suggest,
    #[strum(serialize = "list domains")]
    ListDomains,
    #[strum(serialize = "select domain")]
    SelectDomain,
    #[strum(serialize = "attach transaction")]
    AttachTransaction,
    #[strum(serialize = "inspect transaction")]
    InspectTransaction,
    #[strum(serialize = "subscribe")]
    Subscribe,
    #[strum(serialize = "unsubscribe")]
    Unsubscribe,
    #[strum(serialize = "cancel")]
    Cancel,
    #[strum(serialize = "upload resource")]
    UploadResource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub enum EventStreamKind {
    #[strum(serialize = "subscription")]
    Subscription,
    #[strum(serialize = "server notice")]
    ServerNotice,
}

impl From<&ClientRequest> for RequestKind {
    fn from(request: &ClientRequest) -> Self {
        match request {
            ClientRequest::Command(_) => Self::Command,
            ClientRequest::Suggest(_) => Self::Suggest,
            ClientRequest::ListDomains => Self::ListDomains,
            ClientRequest::SelectDomain(_) => Self::SelectDomain,
            ClientRequest::AttachTransaction(_) => Self::AttachTransaction,
            ClientRequest::InspectTransaction(_) => Self::InspectTransaction,
            ClientRequest::Subscribe(_) => Self::Subscribe,
            ClientRequest::Unsubscribe(_) => Self::Unsubscribe,
            ClientRequest::Cancel(_) => Self::Cancel,
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid server URI")]
    InvalidServerUri(#[source] tonic::codegen::http::uri::InvalidUri),
    #[error("invalid server URL")]
    InvalidServerUrl(#[source] url::ParseError),
    #[error("server endpoint must be an HTTP or HTTPS origin without credentials or a path")]
    InvalidServerEndpoint,
    #[error("{field} must be between one millisecond and one day")]
    InvalidDeadline { field: &'static str },
    #[error("{count} configured seed servers exceeds the limit of 32")]
    TooManySeedServers { count: usize },
    #[error("TLS is required but the server URI is not https")]
    TlsRequired,
    #[error("failed to configure TLS for server connection")]
    ConfigureTls(#[source] tonic::transport::Error),
    #[error("failed to connect to server")]
    ConnectServer(#[source] tonic::transport::Error),
    #[error("failed to start session exchange: {0}")]
    StartSession(#[source] Box<tonic::Status>),
    #[error("session exchange opening exceeded its deadline")]
    SessionOpenDeadline,
    #[error("failed to build authentication metadata")]
    BuildAuthenticationMetadata(#[source] InvalidMetadataValue),
    #[error("session exchange closed")]
    SessionClosed,
    #[error("session exchange failed: {0}")]
    Transport(#[source] Box<tonic::Status>),
    #[error("the {request} request exceeded its deadline")]
    RequestDeadline { request: RequestKind },
    #[error("the {request} request lost its session after its frame was queued")]
    RequestInterrupted { request: RequestKind },
    #[error("session retry deadline expired")]
    RetryDeadline,
    #[error("command outcome for execution '{reference}' is uncertain")]
    UncertainCommand {
        reference: CommandExecutionReference,
        #[source]
        source: Box<ClientError>,
    },
    #[error("the {stream} event consumer exceeded its bounded queue")]
    EventOverflow { stream: EventStreamKind },
    #[error("failed to attach transaction: {0}")]
    AttachTransaction(String),
    /// The request needs a selected domain, and the session has none.
    #[error("no active domain selected")]
    NoActiveDomain,
    #[error("cursor {cursor} is not a character boundary of a {length}-byte input")]
    InvalidCursor { cursor: usize, length: usize },
    #[error("'{name}' is not a valid resource name")]
    InvalidResourceName {
        name: String,
        #[source]
        source: NameError,
    },
    /// The request does not fit the session limits.
    #[error("failed to encode the {request} request")]
    EncodeRequest {
        request: RequestKind,
        #[source]
        source: WireEncodeError,
    },
    /// The server refused to serve the request.
    #[error("the server rejected the {request} request ({rejection:?}): {message}")]
    RequestRejected {
        request: RequestKind,
        rejection: RequestRejection,
        /// The offending field, when the rejection concerns one.
        field: Option<String>,
        message: String,
    },
    /// The server cancelled the request before answering it.
    #[error("the server cancelled the {request} request ({stage:?})")]
    RequestCancelled {
        request: RequestKind,
        stage: CancellationStage,
    },
    /// The server answered the request with a reply of a kind that does not answer it.
    #[error("the server answered the {request} request with a reply of another kind")]
    UnexpectedReply { request: RequestKind },
    #[error("failed to build upload archive")]
    BuildUploadArchive,
    #[error("upload request failed: {0}")]
    UploadResource(#[source] Box<tonic::Status>),
    #[error("upload outcome for identity '{identity}' is uncertain")]
    UncertainUpload {
        identity: ResourceUploadIdentity,
        #[source]
        source: Box<ClientError>,
    },
    #[error("the upload reply does not decode")]
    InvalidUploadReply(#[source] WireDecodeError),
    #[error("command reply execution '{received}' does not match request execution '{expected}'")]
    ExecutionReferenceMismatch {
        expected: CommandExecutionReference,
        received: CommandExecutionReference,
    },
    #[error("upload response identity '{received}' does not match request identity '{expected}'")]
    UploadIdentityMismatch {
        expected: ResourceUploadIdentity,
        received: ResourceUploadIdentity,
    },
    #[error("failed to load TLS CA certificate")]
    LoadTlsCaCertificate(#[source] std::io::Error),
}

impl ClientError {
    pub(crate) fn can_hide_installed_upload(&self) -> bool {
        match self {
            Self::UploadResource(status) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable
            ),
            _ => self.can_hide_admitted_work(),
        }
    }

    pub(crate) fn can_hide_admitted_work(&self) -> bool {
        match self {
            Self::RequestDeadline { .. }
            | Self::RequestInterrupted { .. }
            | Self::ConnectServer(_)
            | Self::SessionOpenDeadline
            | Self::RetryDeadline => true,
            Self::Transport(_) => self.retryable_session_failure(),
            Self::StartSession(status) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable
            ),
            _ => false,
        }
    }

    pub(crate) fn retryable_session_failure(&self) -> bool {
        match self {
            Self::SessionClosed
            | Self::RequestDeadline { .. }
            | Self::RequestInterrupted { .. } => true,
            Self::Transport(status) => {
                if let tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Unavailable = status.code()
                {
                    return true;
                }
                false
            }
            _ => false,
        }
    }

    /// The error a reply stands for when it is not the outcome `request` waits for.
    pub(crate) fn unexpected_reply(request: RequestKind, body: ReplyBody) -> Self {
        match body {
            ReplyBody::Rejected(rejected) => Self::RequestRejected {
                request,
                rejection: rejected.rejection,
                field: rejected.field,
                message: rejected.message,
            },
            ReplyBody::Cancelled(cancelled) => Self::RequestCancelled {
                request,
                stage: cancelled.stage,
            },
            ReplyBody::Command(_)
            | ReplyBody::Attach(_)
            | ReplyBody::Suggest(_)
            | ReplyBody::DomainList(_)
            | ReplyBody::DomainSelection(_)
            | ReplyBody::Inspection(_)
            | ReplyBody::Subscribe(_)
            | ReplyBody::Unsubscribe(_)
            | ReplyBody::Cancel(_) => Self::UnexpectedReply { request },
        }
    }
}
