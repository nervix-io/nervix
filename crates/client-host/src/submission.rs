//! The outcome of one submitted batch, as a host reads it.
//!
//! - **Owns.** The outcome classes, refusals, batch defects, processing failures and
//!   uncertainties a host reads, and reading them and the server's message from an outcome.
//! - **Depends on.** The Rust client's producer outcomes and the vocabulary's submission causes.
//! - **Must not know.** How the outcome was decided, or what a host does about it.

use nervix_client_core::{
    ClientBatchDefect, ClientProcessingFailure, ClientSubmissionRefusal, ProducerOutcome,
    SubmissionUncertainty,
};

use crate::failure::{Failure, FailureKind};

/// What became of a submitted batch, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SubmissionResult {
    NotAdmitted = 1,
    Completed = 2,
    ProcessingFailed = 3,
    OutcomeUnknown = 4,
}

/// Why a batch was not admitted, with its stable values.
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

/// What made a refused batch invalid, with its stable values.
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

/// Why an admitted batch's acknowledgement failed, with its stable values.
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

/// Why no terminal result could be established for a batch, with its stable values.
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
    pub fn new(outcome: ProducerOutcome) -> Self {
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
