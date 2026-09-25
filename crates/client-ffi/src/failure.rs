//! The failure a host reads: `nx_error`.
//!
//! - **Owns.** The kinds of failure the header names, how every client error is classified into
//!   one, and the message and execution reference a host reads from it.
//! - **Depends on.** The Rust client's errors.
//! - **Must not know.** How a host reacts to a failure.
//!
//! A failure is the reporting boundary of the binding: the client's typed error, with every cause
//! behind it, becomes the message a host displays, and its classification becomes the kind a host
//! branches on.

use std::error::Error as _;

use nervix_client_core::{ClientError, CommandExecutionReference};

use crate::abi;

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
}

/// A failed call, as the host reads it through `nx_error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    kind: FailureKind,
    message: String,
    /// The command the failure concerns, when it concerns one.
    execution_reference: Option<CommandExecutionReference>,
}

impl Failure {
    pub(crate) fn new(kind: FailureKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            execution_reference: None,
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
            | ClientError::InvalidResourceName { .. }
            | ClientError::EncodeRequest { .. }
            | ClientError::BuildUploadArchive => FailureKind::InvalidArgument,
            ClientError::TlsRequired
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
            | ClientError::UploadResource(_) => FailureKind::Transport,
            ClientError::RequestDeadline { .. } | ClientError::RetryDeadline => {
                FailureKind::Deadline
            }
            ClientError::UncertainCommand { .. } | ClientError::UncertainUpload { .. } => {
                FailureKind::Uncertain
            }
            ClientError::EventOverflow { .. } => FailureKind::Overflow,
            ClientError::AttachTransaction(_) | ClientError::RequestRejected { .. } => {
                FailureKind::Rejected
            }
            ClientError::RequestCancelled { .. } => FailureKind::Cancelled,
            ClientError::UnexpectedReply { .. }
            | ClientError::InvalidUploadReply(_)
            | ClientError::ExecutionReferenceMismatch { .. }
            | ClientError::UploadIdentityMismatch { .. } => FailureKind::Protocol,
            ClientError::SessionClosed => FailureKind::Closed,
        }
    }
}

impl From<ClientError> for Failure {
    fn from(error: ClientError) -> Self {
        let kind = Self::classify(&error);
        let mut message = error.to_string();
        let mut cause = error.source();
        while let Some(current) = cause {
            message.push_str(": ");
            message.push_str(&current.to_string());
            cause = current.source();
        }
        let execution_reference = match error {
            ClientError::UncertainCommand { reference, .. } => Some(reference),
            _ => None,
        };
        Self {
            kind,
            message,
            execution_reference,
        }
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
/// A non-null `error` is an error this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_error_free(error: *mut Failure) {
    // SAFETY: the header requires an unreleased error or null.
    unsafe { abi::release(error) };
}
