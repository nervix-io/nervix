//! Why a client call failed.
//!
//! Layer: edges.
//!
//! - **Owns.** The failures a caller of the client can act on, and which request each concerns.
//!   Each is the context of a report whose frames beneath it keep the cause.
//! - **Depends on.** The wire contract's requests, rejections and cancellations, the vocabulary's
//!   identities and refusals, and tonic's transport errors.
//! - **Must not know.** How a caller recovers; the client's own retries happen before an error is
//!   returned.

use nervix_client_wire::{
    CancellationStage, ClientRequest, EmitterOpenRefusal, ReplyBody, RequestRejection,
};
use nervix_models::{ClientProducerRefusal, CommandExecutionReference, ResourceUploadIdentity};
use thiserror::Error;
use tonic::metadata::errors::InvalidMetadataValue;
use uuid::Uuid;

use crate::consumer::ConsumerReopenReason;

/// The requests of the session protocol, as errors about their replies name them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum RequestKind {
    #[strum(serialize = "command")]
    Command,
    #[strum(serialize = "suggest")]
    Suggest,
    #[strum(serialize = "choice lookup")]
    Choice,
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
    #[strum(serialize = "restore")]
    Restore,
    #[strum(serialize = "attach domain clock")]
    AttachDomainClock,
    #[strum(serialize = "detach domain clock")]
    DetachDomainClock,
    #[strum(serialize = "open ingestor")]
    OpenIngestor,
    #[strum(serialize = "submit batch")]
    SubmitBatch,
    #[strum(serialize = "close ingestor")]
    CloseIngestor,
    #[strum(serialize = "open emitter")]
    OpenEmitter,
    #[strum(serialize = "read emitter batch")]
    ReadEmitterBatch,
    #[strum(serialize = "settle emitter batch")]
    SettleEmitterBatch,
    #[strum(serialize = "close emitter")]
    CloseEmitter,
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
            ClientRequest::Choice(_) => Self::Choice,
            ClientRequest::ListDomains => Self::ListDomains,
            ClientRequest::SelectDomain(_) => Self::SelectDomain,
            ClientRequest::AttachTransaction(_) => Self::AttachTransaction,
            ClientRequest::InspectTransaction(_) => Self::InspectTransaction,
            ClientRequest::Subscribe(_) => Self::Subscribe,
            ClientRequest::Unsubscribe(_) => Self::Unsubscribe,
            ClientRequest::Cancel(_) => Self::Cancel,
            ClientRequest::AttachDomainClock(_) => Self::AttachDomainClock,
            ClientRequest::DetachDomainClock(_) => Self::DetachDomainClock,
            ClientRequest::OpenIngestor(_) => Self::OpenIngestor,
            ClientRequest::SubmitBatch(_) => Self::SubmitBatch,
            ClientRequest::CloseIngestor(_) => Self::CloseIngestor,
            ClientRequest::OpenEmitter(_) => Self::OpenEmitter,
            ClientRequest::ReadEmitterBatch(_) => Self::ReadEmitterBatch,
            ClientRequest::SettleEmitterBatch(_) => Self::SettleEmitterBatch,
            ClientRequest::CloseEmitter(_) => Self::CloseEmitter,
        }
    }
}

/// Why a client call failed. Every call returns it as the current context of an
/// [`error_stack::Report`], whose frames beneath it hold the causes: the transport status, the
/// wire codec's report, the resolver's configuration report, or, for an uncertain command, the
/// failure that left its outcome unknown.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The resolver's configuration report is beneath.
    #[error("failed to load native DNS configuration")]
    LoadDnsConfiguration,
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
    #[error("failed to start session exchange")]
    StartSession(#[source] Box<tonic::Status>),
    #[error("session exchange opening exceeded its deadline")]
    SessionOpenDeadline,
    #[error("failed to build authentication metadata")]
    BuildAuthenticationMetadata(#[source] InvalidMetadataValue),
    #[error("session exchange closed")]
    SessionClosed,
    #[error("a subscription operation task stopped before completing")]
    SubscriptionTask(#[source] nervix_primitives::task::JoinError),
    #[error("session exchange failed")]
    Transport(#[source] Box<tonic::Status>),
    #[error("the {request} request exceeded its deadline")]
    RequestDeadline { request: RequestKind },
    #[error("the {request} request lost its session after its frame was queued")]
    RequestInterrupted { request: RequestKind },
    #[error("session retry deadline expired")]
    RetryDeadline,
    /// The command may have been admitted under `reference`, and its outcome did not arrive. The
    /// failure that left it unknown is beneath; running the same execution handle again recovers
    /// the outcome.
    #[error("command outcome for execution '{reference}' is uncertain")]
    UncertainCommand {
        reference: CommandExecutionReference,
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
    #[error("completion page size {size} must be between 1 and 100")]
    InvalidCompletionPageSize { size: u16 },
    /// The name's report is beneath.
    #[error("'{name}' is not a valid resource name")]
    InvalidResourceName { name: String },
    /// The request does not fit the session limits. The wire codec's report is beneath.
    #[error("failed to encode the {request} request")]
    EncodeRequest { request: RequestKind },
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
    /// The directory could not be archived. The I/O error is beneath when one occurred.
    #[error("failed to build upload archive")]
    BuildUploadArchive,
    #[error("upload request failed")]
    UploadResource(#[source] Box<tonic::Status>),
    /// The upload may have been installed under `identity`, and its outcome did not arrive. The
    /// failure that left it unknown is beneath.
    #[error("upload outcome for identity '{identity}' is uncertain")]
    UncertainUpload { identity: ResourceUploadIdentity },
    /// The wire codec's report is beneath.
    #[error("the upload reply does not decode")]
    InvalidUploadReply,
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
    /// The server refused to open a producer. Nothing was attached.
    #[error("the server refused to open the producer ({refusal:?}): {message}")]
    ProducerRefused {
        refusal: ClientProducerRefusal,
        message: String,
    },
    #[error("the server refused to open the emitter consumer ({refusal:?}): {message}")]
    ConsumerRefused {
        refusal: EmitterOpenRefusal,
        message: String,
    },
    #[error("the emitter consumer was interrupted by a session gap")]
    ConsumerInterrupted,
    #[error("the emitter consumer needs a new open: {0:?}")]
    ConsumerReopenRequired(ConsumerReopenReason),
    #[error("the emitter consumer's session could not be restored before the retry deadline")]
    ConsumerSessionUnavailable,
    #[error("the delivery reference {reference} belongs to an expired attachment")]
    DeliveryReferenceExpired { reference: Uuid },
    #[error("the settlement outcome for delivery reference {reference} is unknown")]
    SettlementUnknown { reference: Uuid },
    /// The archive a restore names could not be read on this machine.
    #[error("failed to read the restore archive '{}' ({kind})", .path.display())]
    ReadRestoreArchive {
        path: std::path::PathBuf,
        kind: std::io::ErrorKind,
    },
    /// The archive a restore names is empty, so it holds no backup.
    #[error("the restore archive '{}' is empty", .path.display())]
    EmptyRestoreArchive { path: std::path::PathBuf },
    #[error("restore request failed")]
    Restore(#[source] Box<tonic::Status>),
    /// The wire codec's report is beneath.
    #[error("the restore reply does not decode")]
    InvalidRestoreReply,
    /// The backup completed, and its archive could not be downloaded. The
    /// [`BackupDownloadError`](crate::BackupDownloadError) that says why is beneath. Running the
    /// same execution handle again recovers the backup's outcome and downloads the archive again
    /// while the server retains it.
    #[error("failed to download the archive of backup '{reference}'")]
    BackupDownload {
        reference: CommandExecutionReference,
    },
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
            Self::StartSession(status) | Self::Restore(status) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable
            ),
            _ => false,
        }
    }

    /// Whether the failure is the loss of a session, which a caller recovers from by opening a
    /// new one and sending its request again.
    pub(crate) fn retryable_session_failure(&self) -> bool {
        match self {
            Self::SessionClosed
            | Self::RequestDeadline { .. }
            | Self::RequestInterrupted { .. } => true,
            Self::Transport(status) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable
            ),
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
            | ReplyBody::Choice(_)
            | ReplyBody::DomainList(_)
            | ReplyBody::DomainSelection(_)
            | ReplyBody::Inspection(_)
            | ReplyBody::Subscribe(_)
            | ReplyBody::Unsubscribe(_)
            | ReplyBody::Cancel(_)
            | ReplyBody::DomainClockAttach(_)
            | ReplyBody::DomainClockDetach(_)
            | ReplyBody::OpenIngestor(_)
            | ReplyBody::Submission(_)
            | ReplyBody::CloseIngestor(_)
            | ReplyBody::OpenEmitter(_)
            | ReplyBody::ReadEmitterBatch(_)
            | ReplyBody::SettleEmitterBatch(_)
            | ReplyBody::CloseEmitter(_) => Self::UnexpectedReply { request },
        }
    }
}
