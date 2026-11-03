//! `nx_submission_outcome`: the outcome of one submitted batch, as a host reads it.
//!
//! - **Owns.** Reading an outcome's class, refusal, batch defect, processing failure, uncertainty
//!   and message through the C ABI, and freeing it.
//! - **Depends on.** The shared submission outcome and the C ABI boundary.
//! - **Must not know.** How the outcome was decided, or what a host does about it.

use nervix_client_host::{
    BatchDefect, Failure, ProcessingFailure, SubmissionOutcome, SubmissionRefusal,
    SubmissionResult, Uncertainty,
};

use crate::abi;

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
