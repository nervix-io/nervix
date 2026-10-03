//! The failure a host reads: `nx_error`.
//!
//! Layer: edges.
//!
//! - **Owns.** The kinds of failure the header names, how every client error, producer error and
//!   refused read of an attached domain clock is classified into one, and the message, execution
//!   reference and refusal of an open a host reads from it.
//! - **Depends on.** The Rust client's errors, its producer errors and domain clock read errors,
//!   the reports that carry them, and the endpoint vocabulary the header names.
//! - **Must not know.** How a host reacts to a failure.
//!
//! A failure is the reporting boundary of the binding: the client's typed error, with every cause
//! behind it, becomes the message a host displays, and its classification becomes the kind a host
//! branches on.

use error_stack::Report;
use nervix_client_core::{
    BackupDownloadError, ClientError, CommandExecutionReference, DomainClockReadError, ProducerEnd,
    ProducerError,
};

use crate::{abi, endpoint::OpenRefusal};

/// The kinds of failure the header names, with its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum FailureKind {
    InvalidArgument = 1,
    Connect = 2,
    Transport = 3,
    Uncertain = 4,
    Rejected = 5,
    Deadline = 6,
    Cancelled = 7,
    Overflow = 8,
    Protocol = 9,
    Type = 10,
    Closed = 11,
    Interrupted = 12,
    ReopenRequired = 13,
}

/// A failed call, as the host reads it through `nx_error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    kind: FailureKind,
    message: String,
    /// The command the failure concerns, when it concerns one.
    execution_reference: Option<CommandExecutionReference>,
    /// Why the server refused to open a producer or a consumer, when that is the failure.
    open_refusal: Option<OpenRefusal>,
}

impl Failure {
    pub(crate) fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            execution_reference: None,
            open_refusal: None,
        }
    }

    pub(crate) fn invalid_argument(argument: &'static str, problem: &str) -> Self {
        Self::new(
            FailureKind::InvalidArgument,
            format!("`{argument}` {problem}"),
        )
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(FailureKind::Cancelled, "the call was cancelled")
    }

    pub(crate) fn deadline() -> Self {
        Self::new(FailureKind::Deadline, "the call's deadline passed")
    }

    /// Names the command the failure concerns, so a host can recover its outcome.
    pub(crate) fn concerning(mut self, reference: &CommandExecutionReference) -> Self {
        self.execution_reference = Some(reference.clone());
        self
    }

    pub fn kind(&self) -> FailureKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn execution_reference(&self) -> Option<&CommandExecutionReference> {
        self.execution_reference.as_ref()
    }

    pub fn open_refusal(&self) -> Option<OpenRefusal> {
        self.open_refusal
    }

    /// The kind a client error reports to a host. Every variant is named, so a new one has to be
    /// classified before it can be returned.
    fn classify(error: &ClientError) -> FailureKind {
        match error {
            ClientError::InvalidServerUri(_)
            | ClientError::InvalidServerUrl(_)
            | ClientError::InvalidServerEndpoint
            | ClientError::InvalidDeadline { .. }
            | ClientError::TooManySeedServers { .. }
            | ClientError::NoActiveDomain
            | ClientError::InvalidCursor { .. }
            | ClientError::InvalidCompletionPageSize { .. }
            | ClientError::InvalidResourceName { .. }
            | ClientError::EncodeRequest { .. }
            | ClientError::BuildUploadArchive
            | ClientError::ReadRestoreArchive { .. }
            | ClientError::EmptyRestoreArchive { .. } => FailureKind::InvalidArgument,
            ClientError::TlsRequired
            | ClientError::LoadDnsConfiguration(_)
            | ClientError::ConfigureTls(_)
            | ClientError::ConnectServer(_)
            | ClientError::StartSession(_)
            | ClientError::SessionOpenDeadline
            | ClientError::BuildAuthenticationMetadata(_)
            | ClientError::LoadTlsCaCertificate(_) => FailureKind::Connect,
            ClientError::SubscriptionTask(_)
            | ClientError::SubscriptionOperation(_)
            | ClientError::Transport(_)
            | ClientError::RequestInterrupted { .. }
            | ClientError::UploadResource(_)
            | ClientError::Restore(_) => FailureKind::Transport,
            ClientError::ConsumerInterrupted => FailureKind::Interrupted,
            ClientError::ConsumerReopenRequired(_) => FailureKind::ReopenRequired,
            ClientError::RequestDeadline { .. } | ClientError::RetryDeadline => {
                FailureKind::Deadline
            }
            ClientError::UncertainCommand { .. }
            | ClientError::UncertainUpload { .. }
            | ClientError::SettlementUnknown { .. } => FailureKind::Uncertain,
            ClientError::EventOverflow { .. } => FailureKind::Overflow,
            ClientError::AttachTransaction(_)
            | ClientError::RequestRejected { .. }
            | ClientError::ProducerRefused { .. }
            | ClientError::ConsumerRefused { .. }
            | ClientError::DeliveryReferenceExpired { .. } => FailureKind::Rejected,
            ClientError::RequestCancelled { .. } => FailureKind::Cancelled,
            ClientError::UnexpectedReply { .. }
            | ClientError::InvalidUploadReply(_)
            | ClientError::InvalidRestoreReply(_)
            | ClientError::ExecutionReferenceMismatch { .. }
            | ClientError::UploadIdentityMismatch { .. } => FailureKind::Protocol,
            ClientError::SessionClosed => FailureKind::Closed,
            ClientError::ConsumerSessionUnavailable => FailureKind::Connect,
            ClientError::BackupDownload { source, .. } => Self::classify_download(source),
        }
    }

    /// The kind a producer error reports. None of them sent anything: a batch the producer
    /// refuses, or a submission it does not hold, is the host's argument, and a producer that
    /// ended either was closed or has to be opened again.
    fn classify_producer(error: &ProducerError) -> FailureKind {
        match error {
            ProducerError::Ended(ProducerEnd::ReopenRequired(_)) => FailureKind::ReopenRequired,
            ProducerError::Ended(
                ProducerEnd::Closed | ProducerEnd::Ended { .. } | ProducerEnd::SessionLost,
            ) => FailureKind::Closed,
            ProducerError::BatchTooLarge { .. }
            | ProducerError::EmptyBatch
            | ProducerError::UnknownSubmission(_)
            | ProducerError::SchemaMismatch
            | ProducerError::TooManyRows { .. }
            | ProducerError::Encode => FailureKind::InvalidArgument,
            ProducerError::SessionUnavailable => FailureKind::Connect,
        }
    }

    /// The kind a failed backup download reports, by why it failed.
    fn classify_download(error: &BackupDownloadError) -> FailureKind {
        match error {
            BackupDownloadError::Refused { .. } => FailureKind::Rejected,
            BackupDownloadError::Transport { .. }
            | BackupDownloadError::Stalled
            | BackupDownloadError::Interrupted
            | BackupDownloadError::NoLeader
            | BackupDownloadError::RedirectLoop
            | BackupDownloadError::SessionLost => FailureKind::Transport,
            BackupDownloadError::OutOfOrder
            | BackupDownloadError::InvalidFrame(_)
            | BackupDownloadError::Mismatch => FailureKind::Protocol,
            BackupDownloadError::EncodeRequest(_) | BackupDownloadError::Write { .. } => {
                FailureKind::InvalidArgument
            }
        }
    }
}

impl From<ClientError> for Failure {
    fn from(error: ClientError) -> Self {
        Self::from(Report::new(error))
    }
}

impl From<Report<ClientError>> for Failure {
    fn from(report: Report<ClientError>) -> Self {
        let error = report.current_context();
        let execution_reference = match error {
            ClientError::UncertainCommand { reference, .. }
            | ClientError::BackupDownload { reference, .. } => Some(reference.clone()),
            _ => None,
        };
        let open_refusal = match error {
            ClientError::ProducerRefused { refusal, .. } => Some(OpenRefusal::from(*refusal)),
            ClientError::ConsumerRefused { refusal, .. } => Some(OpenRefusal::from(*refusal)),
            _ => None,
        };
        Self {
            kind: Self::classify(error),
            // The alternate form joins every context of the report, and a report holds the causes
            // of the error it was created from as contexts of their own.
            message: format!("{report:#}"),
            execution_reference,
            open_refusal,
        }
    }
}

impl From<Report<ProducerError>> for Failure {
    fn from(report: Report<ProducerError>) -> Self {
        let kind = Self::classify_producer(report.current_context());
        Self::new(kind, format!("{report:#}"))
    }
}

impl From<Report<DomainClockReadError>> for Failure {
    /// A stopped or uninstalled clock has no logical time to read, which is a read of a value the
    /// clock's state does not hold; a projection outside the timestamp range is an argument out of
    /// range for that clock.
    fn from(report: Report<DomainClockReadError>) -> Self {
        let kind = match report.current_context() {
            DomainClockReadError::Stopped { .. } | DomainClockReadError::Uninstalled { .. } => {
                FailureKind::Type
            }
            DomainClockReadError::Arithmetic { .. } => FailureKind::InvalidArgument,
        };
        Self::new(kind, format!("{report:#}"))
    }
}

/// # Safety
///
/// `error` is a live error this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_kind_of(error: *const Failure) -> FailureKind {
    // SAFETY: the header requires a live error.
    unsafe { abi::accessor(error) }.kind
}

/// # Safety
///
/// `error` is a live error this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_message(
    error: *const Failure,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live error and writable out-parameters.
    unsafe {
        let error = abi::accessor(error);
        abi::write_bytes(message, message_len, error.message.as_bytes());
    }
}

/// # Safety
///
/// `error` is a live error this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_execution_reference(
    error: *const Failure,
    reference: *mut *const u8,
    reference_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live error.
    let error = unsafe { abi::accessor(error) };
    let Some(execution_reference) = &error.execution_reference else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write_bytes(
            reference,
            reference_len,
            execution_reference.as_str().as_bytes(),
        )
    };
    true
}

/// # Safety
///
/// `error` is a live error this library returned; a non-null `refusal` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_open_refusal(
    error: *const Failure,
    refusal: *mut OpenRefusal,
) -> bool {
    // SAFETY: the header requires a live error.
    let error = unsafe { abi::accessor(error) };
    let Some(open_refusal) = error.open_refusal else {
        return false;
    };
    // SAFETY: the header requires a writable out-parameter.
    unsafe { abi::write(refusal, open_refusal) };
    true
}

/// # Safety
///
/// A non-null `error` is an error this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_free(error: *mut Failure) {
    // SAFETY: the header requires an unreleased error or null.
    unsafe { abi::release(error) };
}

#[cfg(test)]
mod endpoint_recovery_tests {
    use nervix_client_core::ConsumerReopenReason;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn client_endpoint_recovery_failures_keep_their_actionable_kind() {
        let reference = Uuid::from_bytes([9; 16]);
        let cases = [
            (ClientError::ConsumerInterrupted, FailureKind::Interrupted),
            (
                ClientError::ConsumerSessionUnavailable,
                FailureKind::Connect,
            ),
            (
                ClientError::ConsumerReopenRequired(ConsumerReopenReason::GenerationChanged),
                FailureKind::ReopenRequired,
            ),
            (
                ClientError::DeliveryReferenceExpired { reference },
                FailureKind::Rejected,
            ),
            (
                ClientError::SettlementUnknown { reference },
                FailureKind::Uncertain,
            ),
        ];
        for (error, expected) in cases {
            let failure = Failure::from(error);
            assert_eq!(failure.kind(), expected);
            assert!(!failure.message().is_empty());
        }
    }
}
