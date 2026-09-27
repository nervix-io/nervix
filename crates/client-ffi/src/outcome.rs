//! What became of a command: `nx_outcome`.
//!
//! - **Owns.** The dispositions the header names and reading an outcome's message, diagnostics,
//!   execution reference and opened subscription.
//! - **Depends on.** The Rust client's command outcome.
//! - **Must not know.** How the command was sent, retried or redirected.

use nervix_client_core::{CommandDisposition, CommandOutcome};
use triomphe::Arc;

use crate::{
    abi,
    failure::{Failure, FailureKind},
    schema::Schema,
};

/// What became of a command, with the header's values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Disposition {
    Completed = 1,
    Failed = 2,
    NotLeader = 3,
    TransactionDetached = 4,
    TransactionTakenOver = 5,
    OutcomeUnknown = 6,
    ExecutionReferenceConflict = 7,
    ExecutionReferenceExpired = 8,
    PreviewStale = 9,
}

impl From<&CommandDisposition> for Disposition {
    fn from(disposition: &CommandDisposition) -> Self {
        match disposition {
            CommandDisposition::Completed { .. } => Self::Completed,
            CommandDisposition::Failed => Self::Failed,
            CommandDisposition::NotLeader(_) => Self::NotLeader,
            CommandDisposition::TransactionDetached { .. } => Self::TransactionDetached,
            CommandDisposition::TransactionTakenOver { .. } => Self::TransactionTakenOver,
            CommandDisposition::OutcomeUnknown(_) => Self::OutcomeUnknown,
            CommandDisposition::ExecutionReferenceConflict(_) => Self::ExecutionReferenceConflict,
            CommandDisposition::ExecutionReferenceExpired => Self::ExecutionReferenceExpired,
            CommandDisposition::PreviewStale { .. } => Self::PreviewStale,
        }
    }
}

/// A command's outcome, and the schema of the subscription it opened, when it opened one.
#[derive(Debug, Clone)]
pub struct Outcome {
    outcome: CommandOutcome,
    schema: Option<Schema>,
}

impl Outcome {
    pub(crate) fn new(outcome: CommandOutcome) -> Self {
        let schema = outcome
            .subscription
            .as_ref()
            .map(|opened| Schema::new(Arc::new(opened.schema.clone())));
        Self { outcome, schema }
    }

    pub fn disposition(&self) -> Disposition {
        Disposition::from(&self.outcome.disposition)
    }

    pub fn command(&self) -> &CommandOutcome {
        &self.outcome
    }

    fn schema(&self) -> Result<&Schema, Failure> {
        self.schema.as_ref().ok_or_else(|| {
            Failure::new(
                FailureKind::InvalidArgument,
                "the command did not open a subscription",
            )
        })
    }
}

/// # Safety
///
/// `outcome` is a live outcome this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_disposition(outcome: *const Outcome) -> Disposition {
    // SAFETY: the header requires a live outcome.
    unsafe { abi::accessor(outcome) }.disposition()
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_message(
    outcome: *const Outcome,
    message: *mut *const u8,
    message_len: *mut usize,
) {
    // SAFETY: the header requires a live outcome and writable out-parameters.
    unsafe {
        let outcome = abi::accessor(outcome);
        abi::write_bytes(message, message_len, outcome.outcome.message.as_bytes());
    }
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_execution_reference(
    outcome: *const Outcome,
    reference: *mut *const u8,
    reference_len: *mut usize,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(execution_reference) = &outcome.outcome.execution_reference else {
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
/// `outcome` is a live outcome this library returned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_diagnostic_count(outcome: *const Outcome) -> usize {
    // SAFETY: the header requires a live outcome.
    unsafe { abi::accessor(outcome) }.outcome.diagnostics.len()
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_diagnostic(
    outcome: *const Outcome,
    index: usize,
    message: *mut *const u8,
    message_len: *mut usize,
    has_span: *mut bool,
    start: *mut u32,
    end: *mut u32,
) -> *mut Failure {
    // SAFETY: the header requires a live outcome and writable out-parameters.
    abi::outcome(unsafe {
        write_diagnostic(outcome, index, message, message_len, has_span, start, end)
    })
}

/// # Safety
///
/// As [`nx_outcome_diagnostic`].
unsafe fn write_diagnostic(
    outcome: *const Outcome,
    index: usize,
    message: *mut *const u8,
    message_len: *mut usize,
    has_span: *mut bool,
    start: *mut u32,
    end: *mut u32,
) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live outcome.
    let outcome = unsafe { abi::handle(outcome, "outcome") }?;
    let diagnostics = &outcome.outcome.diagnostics;
    let Some(diagnostic) = diagnostics.get(index) else {
        return Err(Failure::new(
            FailureKind::InvalidArgument,
            format!(
                "diagnostic {index} is past the {} diagnostics",
                diagnostics.len()
            ),
        ));
    };
    // SAFETY: the caller guarantees writable out-parameters.
    unsafe {
        abi::write_bytes(message, message_len, diagnostic.message.as_bytes());
        abi::write(has_span, diagnostic.span.is_some());
        if let Some(span) = diagnostic.span {
            abi::write(start, span.start());
            abi::write(end, span.end());
        }
    }
    Ok(())
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; non-null out-parameters are writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_subscription(
    outcome: *const Outcome,
    name: *mut *const u8,
    name_len: *mut usize,
    generation: *mut u64,
) -> bool {
    // SAFETY: the header requires a live outcome.
    let outcome = unsafe { abi::accessor(outcome) };
    let Some(opened) = &outcome.outcome.subscription else {
        return false;
    };
    // SAFETY: the header requires writable out-parameters.
    unsafe {
        abi::write_bytes(name, name_len, opened.subscription.name.as_str().as_bytes());
        abi::write(generation, opened.subscription.generation.get());
    }
    true
}

/// # Safety
///
/// `outcome` is a live outcome this library returned; a non-null `out` is writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_schema(
    outcome: *const Outcome,
    out: *mut *mut Schema,
) -> *mut Failure {
    // SAFETY: the header requires a live outcome and a writable `out`.
    abi::outcome(unsafe { write_schema(outcome, out) })
}

/// # Safety
///
/// As [`nx_outcome_schema`].
unsafe fn write_schema(outcome: *const Outcome, out: *mut *mut Schema) -> Result<(), Failure> {
    // SAFETY: the caller guarantees a live outcome.
    let outcome = unsafe { abi::handle(outcome, "outcome") }?;
    abi::require_out(out, "out")?;
    let schema = outcome.schema()?.clone();
    // SAFETY: `out` is non-null, and the caller guarantees it is writable.
    unsafe { abi::write(out, abi::into_handle(schema)) };
    Ok(())
}

/// # Safety
///
/// A non-null `outcome` is an outcome this library returned that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn nx_outcome_free(outcome: *mut Outcome) {
    // SAFETY: the header requires an unreleased outcome or null.
    unsafe { abi::release(outcome) };
}
