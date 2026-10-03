//! The outcome of one submitted batch, as a host reads it: `nx_submission_outcome`.
//!
//! - **Owns.** The outcome classes, refusals, batch defects, processing failures and
//!   uncertainties the header names, and reading them and the server's message from an outcome.
//! - **Depends on.** The Rust client's producer outcomes and the vocabulary's submission causes.
//! - **Must not know.** How the outcome was decided, or what a host does about it.

use nervix_client_core::{
    ClientBatchDefect, ClientProcessingFailure, ClientSubmissionRefusal, ProducerOutcome,
    SubmissionUncertainty,
};

use crate::{
    abi,
    failure::{Failure, FailureKind},
};

/// What became of a submitted batch, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SubmissionResult {
    NotAdmitted = 1,
    Completed = 2,
    ProcessingFailed = 3,
    OutcomeUnknown = 4,
}

/// Why a batch was not admitted, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SubmissionRefusal {
    InvalidBatch = 1,
    Suspended = 2,
    Busy = 3,
    Draining = 4,
    ProducerEnded = 5,
    CreditExceeded = 6,
}

impl From<ClientSubmissionRefusal> for SubmissionRefusal {
    fn from(refusal: ClientSubmissionRefusal) -> Self {
        match refusal {
            ClientSubmissionRefusal::InvalidBatch(_) => Self::InvalidBatch,
            ClientSubmissionRefusal::Suspended => Self::Suspended,
            ClientSubmissionRefusal::Busy => Self::Busy,
            ClientSubmissionRefusal::Draining => Self::Draining,
            ClientSubmissionRefusal::ProducerEnded => Self::ProducerEnded,
            ClientSubmissionRefusal::CreditExceeded => Self::CreditExceeded,
        }
    }
}

/// What made a refused batch invalid, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum BatchDefect {
    Malformed = 1,
    UnexpectedMessage = 2,
    Compressed = 3,
    SchemaMismatch = 4,
    NotOneBatch = 5,
    TooManyRows = 6,
    TooLarge = 7,
    InvalidData = 8,
}

impl From<ClientBatchDefect> for BatchDefect {
    fn from(defect: ClientBatchDefect) -> Self {
        match defect {
            ClientBatchDefect::Malformed => Self::Malformed,
            ClientBatchDefect::UnexpectedMessage => Self::UnexpectedMessage,
            ClientBatchDefect::Compressed => Self::Compressed,
            ClientBatchDefect::SchemaMismatch => Self::SchemaMismatch,
            ClientBatchDefect::NotOneBatch => Self::NotOneBatch,
            ClientBatchDefect::TooManyRows => Self::TooManyRows,
            ClientBatchDefect::TooLarge => Self::TooLarge,
            ClientBatchDefect::InvalidData => Self::InvalidData,
        }
    }
}

/// Why an admitted batch's acknowledgement failed, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ProcessingFailure {
    AckTimedOut = 1,
    Rejected = 2,
}

impl From<ClientProcessingFailure> for ProcessingFailure {
    fn from(failure: ClientProcessingFailure) -> Self {
        match failure {
            ClientProcessingFailure::AckTimedOut => Self::AckTimedOut,
            ClientProcessingFailure::Rejected => Self::Rejected,
        }
    }
}

/// Why no terminal result could be established for a batch, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Uncertainty {
    Interrupted = 1,
    OwnerLost = 2,
    SessionLost = 3,
}

impl From<SubmissionUncertainty> for Uncertainty {
    fn from(uncertainty: SubmissionUncertainty) -> Self {
        match uncertainty {
            SubmissionUncertainty::Interrupted => Self::Interrupted,
            SubmissionUncertainty::OwnerLost => Self::OwnerLost,
            SubmissionUncertainty::SessionLost => Self::SessionLost,
        }
    }
}

/// The terminal outcome of one submitted batch, which a host owns once it takes it.
#[derive(Debug)]
pub struct SubmissionOutcome {
    outcome: ProducerOutcome,
}

impl SubmissionOutcome {
    pub(crate) fn new(outcome: ProducerOutcome) -> Self {
        Self { outcome }
    }

    pub fn result(&self) -> SubmissionResult {
        match &self.outcome {
            ProducerOutcome::NotAdmitted { .. } => SubmissionResult::NotAdmitted,
            ProducerOutcome::Completed => SubmissionResult::Completed,
            ProducerOutcome::ProcessingFailed { .. } => SubmissionResult::ProcessingFailed,
            ProducerOutcome::OutcomeUnknown { .. } => SubmissionResult::OutcomeUnknown,
        }
    }

    /// The server's bounded, non-sensitive description of the outcome; empty for a completed
    /// batch.
    pub fn message(&self) -> &str {
        match &self.outcome {
            ProducerOutcome::NotAdmitted { message, .. }
            | ProducerOutcome::ProcessingFailed { message, .. }
            | ProducerOutcome::OutcomeUnknown { message, .. } => message,
            ProducerOutcome::Completed => "",
        }
    }

    fn not_carried(&self, what: &str) -> Failure {
        Failure::new(
            FailureKind::Type,
            format!("a {:?} outcome carries no {what}", self.result()),
        )
    }

    fn refusal_cause(&self) -> Result<ClientSubmissionRefusal, Failure> {
        match &self.outcome {
            ProducerOutcome::NotAdmitted { refusal, .. } => Ok(*refusal),
            ProducerOutcome::Completed
            | ProducerOutcome::ProcessingFailed { .. }
            | ProducerOutcome::OutcomeUnknown { .. } => Err(self.not_carried("refusal")),
        }
    }

    pub fn refusal(&self) -> Result<SubmissionRefusal, Failure> {
        Ok(SubmissionRefusal::from(self.refusal_cause()?))
    }

    pub fn defect(&self) -> Result<BatchDefect, Failure> {
        match self.refusal_cause()? {
            ClientSubmissionRefusal::InvalidBatch(defect) => Ok(BatchDefect::from(defect)),
            ClientSubmissionRefusal::Suspended
            | ClientSubmissionRefusal::Busy
            | ClientSubmissionRefusal::Draining
            | ClientSubmissionRefusal::ProducerEnded
            | ClientSubmissionRefusal::CreditExceeded => Err(Failure::new(
                FailureKind::Type,
                "a refusal other than an invalid batch carries no defect",
            )),
        }
    }

    pub fn failure(&self) -> Result<ProcessingFailure, Failure> {
        match &self.outcome {
            ProducerOutcome::ProcessingFailed { failure, .. } => {
                Ok(ProcessingFailure::from(*failure))
            }
            ProducerOutcome::NotAdmitted { .. }
            | ProducerOutcome::Completed
            | ProducerOutcome::OutcomeUnknown { .. } => Err(self.not_carried("processing failure")),
        }
    }

    pub fn uncertainty(&self) -> Result<Uncertainty, Failure> {
        match &self.outcome {
            ProducerOutcome::OutcomeUnknown { cause, .. } => Ok(Uncertainty::from(*cause)),
            ProducerOutcome::NotAdmitted { .. }
            | ProducerOutcome::Completed
            | ProducerOutcome::ProcessingFailed { .. } => Err(self.not_carried("uncertainty")),
        }
    }
}

/// # Safety
///
/// `outcome` is a live submission outcome this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_result(
    outcome: *const SubmissionOutcome,
) -> SubmissionResult {
    // SAFETY: the header requires a live outcome.
    unsafe { abi::accessor(outcome) }.result()
}

/// # Safety
///
/// `outcome` is a live submission outcome; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_message(
    outcome: *const SubmissionOutcome,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live outcome and writable out-parameters.
    unsafe {
        let outcome = abi::accessor(outcome);
        abi::write_bytes(message, message_len, outcome.message().as_bytes());
    }
}

/// Writes one typed cause of an outcome, when the outcome carries it.
///
/// # Safety
///
/// `outcome` is a live submission outcome, and a non-null `out` is writable.
unsafe fn write_cause<T>(
    outcome: *const SubmissionOutcome,
    out: *mut T,
    read: impl FnOnce(&SubmissionOutcome) -> Result<T, Failure>,
) -> Result<(), Failure> {
    abi::require_out(out, "out")?;
    // SAFETY: the caller guarantees a live outcome.
    let cause = read(unsafe { abi::handle(outcome, "outcome") }?)?;
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, cause) };
    Ok(())
}

/// # Safety
///
/// `outcome` is a live submission outcome; a non-null `refusal` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_refusal(
    outcome: *const SubmissionOutcome,
    refusal: *mut SubmissionRefusal,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cause(outcome, refusal, SubmissionOutcome::refusal) })
}

/// # Safety
///
/// `outcome` is a live submission outcome; a non-null `defect` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_defect(
    outcome: *const SubmissionOutcome,
    defect: *mut BatchDefect,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cause(outcome, defect, SubmissionOutcome::defect) })
}

/// # Safety
///
/// `outcome` is a live submission outcome; a non-null `failure` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_failure(
    outcome: *const SubmissionOutcome,
    failure: *mut ProcessingFailure,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cause(outcome, failure, SubmissionOutcome::failure) })
}

/// # Safety
///
/// `outcome` is a live submission outcome; a non-null `uncertainty` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_uncertainty(
    outcome: *const SubmissionOutcome,
    uncertainty: *mut Uncertainty,
) -> *mut Failure {
    // SAFETY: the header's contract is this function's.
    abi::outcome(unsafe { write_cause(outcome, uncertainty, SubmissionOutcome::uncertainty) })
}

/// # Safety
///
/// A non-null `outcome` is a submission outcome this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_submission_outcome_free(outcome: *mut SubmissionOutcome) {
    // SAFETY: the header requires an unreleased outcome or null.
    unsafe { abi::release(outcome) };
}
