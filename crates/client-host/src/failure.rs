//! The failure a host reads.
//!
//! Layer: edges.
//!
//! - **Owns.** The kinds of failure a host branches on, how every client error, producer error and
//!   refused read of an attached domain clock is classified into one, and the message, execution
//!   reference and refusal of an open a host reads from it.
//! - **Depends on.** The Rust client's errors, its producer errors and domain clock read errors,
//!   the reports that carry them, and the endpoint vocabulary.
//! - **Must not know.** How a host reacts to a failure, or how a binding raises it.
//!
//! A failure is the reporting boundary of every binding: the client's typed error, with every
//! cause behind it, becomes the message a host displays, and its classification becomes the kind
//! a host branches on.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_client_core::{
    BackupDownloadError, ClientError, CommandExecutionReference, DomainClockReadError, ProducerEnd,
    ProducerError,
};

use crate::endpoint::OpenRefusal;

/// The kinds of failure a host branches on, with their stable values.
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

/// A failed call, as a host reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "the C ABI projects a failure into its fixed host-visible kind and message; \
                  report conversions render their complete chain at this reporting boundary"
    )
)]
pub struct Failure {
    kind: FailureKind,
    message: String,
    /// The command the failure concerns, when it concerns one.
    execution_reference: Option<CommandExecutionReference>,
    /// Why the server refused to open a producer or a consumer, when that is the failure.
    open_refusal: Option<OpenRefusal>,
}

impl Failure {
    pub fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            execution_reference: None,
            open_refusal: None,
        }
    }

    pub fn invalid_argument(argument: &'static str, problem: &str) -> Self {
        Self::new(
            FailureKind::InvalidArgument,
            format!("`{argument}` {problem}"),
        )
    }

    pub fn cancelled() -> Self {
        Self::new(FailureKind::Cancelled, "the call was cancelled")
    }

    pub fn deadline() -> Self {
        Self::new(FailureKind::Deadline, "the call's deadline passed")
    }

    /// Names the command the failure concerns, so a host can recover its outcome.
    pub fn concerning(mut self, reference: &CommandExecutionReference) -> Self {
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

    /// The kind a client error reports to a host, from the report's current context and, for a
    /// failed backup download, the download failure beneath it. Every variant is named, so a new
    /// one has to be classified before it can be returned.
    fn classify(report: &Report<ClientError>) -> FailureKind {
        match report.current_context() {
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
            | ClientError::LoadDnsConfiguration
            | ClientError::ConfigureTls(_)
            | ClientError::ConnectServer(_)
            | ClientError::StartSession(_)
            | ClientError::SessionOpenDeadline
            | ClientError::BuildAuthenticationMetadata(_) => FailureKind::Connect,
            ClientError::SubscriptionTask(_)
            | ClientError::Transport(_)
            | ClientError::RequestInterrupted { .. }
            | ClientError::UploadResource(_)
            | ClientError::Restore(_) => FailureKind::Transport,
            ClientError::ConsumerInterrupted => FailureKind::Interrupted,
            ClientError::ConsumerReopenRequired(_) => FailureKind::ReopenRequired,
            ClientError::RequestDeadline { .. } | ClientError::RetryDeadline => {
                FailureKind::Deadline
            }
            ClientError::LastUnresolvedAnswer { .. }
            | ClientError::UncertainCommand { .. }
            | ClientError::UncertainUpload { .. }
            | ClientError::SettlementUnknown { .. } => FailureKind::Uncertain,
            ClientError::EventOverflow { .. } => FailureKind::Overflow,
            ClientError::AttachTransaction(_)
            | ClientError::RequestRejected { .. }
            | ClientError::ProducerRefused { .. }
            | ClientError::ConsumerRefused { .. }
            | ClientError::DeliveryReferenceExpired { .. } => FailureKind::Rejected,
            ClientError::RequestCancelled { .. } | ClientError::CallCancelled => {
                FailureKind::Cancelled
            }
            ClientError::UnexpectedReply { .. }
            | ClientError::InvalidUploadReply
            | ClientError::InvalidRestoreReply
            | ClientError::ExecutionReferenceMismatch { .. }
            | ClientError::UploadIdentityMismatch { .. } => FailureKind::Protocol,
            ClientError::SessionClosed => FailureKind::Closed,
            ClientError::ConsumerSessionUnavailable => FailureKind::Connect,
            ClientError::BackupDownload { .. } => {
                let download = report.downcast_ref::<BackupDownloadError>().assured(
                    "the Rust client reports a failed backup download above the download's own \
                     BackupDownloadError",
                );
                Self::classify_download(download)
            }
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
            | BackupDownloadError::HolderUnreachable
            | BackupDownloadError::RedirectLoop
            | BackupDownloadError::SessionLost => FailureKind::Transport,
            BackupDownloadError::OutOfOrder
            | BackupDownloadError::InvalidFrame
            | BackupDownloadError::Mismatch => FailureKind::Protocol,
            BackupDownloadError::EncodeRequest | BackupDownloadError::Write { .. } => {
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
            kind: Self::classify(&report),
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

#[cfg(test)]
mod call_cancellation_tests {
    use meticulous::ResultExt as _;
    use nervix_client_core::{
        CommandDisposition, RequestKind,
        wire::{CancellationStage, UnknownOutcomeCause},
    };

    use super::*;

    #[test]
    fn a_cancelled_call_is_cancelled_unless_it_may_have_taken_effect() {
        let refused = Failure::from(ClientError::RequestCancelled {
            request: RequestKind::Command,
            stage: CancellationStage::BeforeAdmission,
        });
        assert_eq!(refused.kind(), FailureKind::Cancelled);
        assert_eq!(refused.execution_reference(), None);

        let stopped = Failure::from(ClientError::CallCancelled);
        assert_eq!(stopped.kind(), FailureKind::Cancelled);
        assert_eq!(stopped.execution_reference(), None);

        let reference = CommandExecutionReference::parse("command-1")
            .assured("the test execution reference is valid");
        let admitted = Failure::from(Report::new(ClientError::CallCancelled).change_context(
            ClientError::UncertainCommand {
                reference: reference.clone(),
            },
        ));
        assert_eq!(admitted.kind(), FailureKind::Uncertain);
        assert_eq!(admitted.execution_reference(), Some(&reference));
        assert!(
            admitted
                .message()
                .contains("the call was cancelled before its outcome arrived"),
            "the cancellation stays in the reported chain: {}",
            admitted.message()
        );
    }

    #[test]
    fn a_foreign_execution_is_reported_as_uncertain_under_the_own_reference() {
        let expected = CommandExecutionReference::parse("own-command").assured("a valid reference");
        let received =
            CommandExecutionReference::parse("foreign-command").assured("a valid reference");
        let failure = Failure::from(
            Report::new(ClientError::ExecutionReferenceMismatch {
                expected: expected.clone(),
                received,
            })
            .change_context(ClientError::UncertainCommand {
                reference: expected.clone(),
            }),
        );
        assert_eq!(failure.kind(), FailureKind::Uncertain);
        assert_eq!(failure.execution_reference(), Some(&expected));
        assert!(failure.message().contains("foreign-command"));
    }

    #[test]
    fn an_unresolved_answer_stays_beneath_an_uncertain_host_failure() {
        let reference = CommandExecutionReference::parse("command-1")
            .assured("the test execution reference is valid");
        let report = Report::new(ClientError::RetryDeadline)
            .change_context(ClientError::LastUnresolvedAnswer {
                disposition: CommandDisposition::OutcomeUnknown(
                    UnknownOutcomeCause::LeadershipLost,
                ),
                message: "the lease could not be confirmed".to_string(),
            })
            .change_context(ClientError::UncertainCommand {
                reference: reference.clone(),
            });
        let failure = Failure::from(report);

        assert_eq!(failure.kind(), FailureKind::Uncertain);
        assert_eq!(failure.execution_reference(), Some(&reference));
        assert!(failure.message().contains("OutcomeUnknown(LeadershipLost)"));
        assert!(
            failure
                .message()
                .contains("the lease could not be confirmed")
        );
        assert!(failure.message().contains("session retry deadline expired"));
    }
}
