//! Upload completion, retry and identity conflict through the shared binding's public C ABI.
//!
//! Layer: test harness.
//! - **Owns.** The upload exercise's C handles and its common cross-language report.
//! - **Depends on.** Exported C functions and a scenario-owned source file.
//! - **Must not know.** The Rust client's outcomes or the server's installation internals.

use std::{io, path::Path, ptr};

use nervix_client_ffi::{
    Disposition, Execution, Outcome, UploadFailure, nx_execution_free, nx_outcome_disposition,
    nx_outcome_free, nx_outcome_upload_failure, nx_session_execute, nx_session_prepare,
};

use super::c_abi_probe::{Emit, OpenSession, check};

struct UploadExecution(*mut Execution);

impl UploadExecution {
    fn prepare(session: &OpenSession, query: &str) -> io::Result<Self> {
        let mut execution = ptr::null_mut();
        // SAFETY: the session is live, the query addresses its readable bytes, and the output
        // pointer is writable. The returned execution belongs to this exercise.
        check(unsafe {
            nx_session_prepare(
                session.0,
                query.as_ptr(),
                query.len(),
                ptr::null(),
                &mut execution,
            )
        })?;
        Ok(Self(execution))
    }

    fn execute(&self, session: &OpenSession) -> io::Result<UploadOutcome> {
        let mut outcome = ptr::null_mut();
        // SAFETY: both input handles are live, and the output pointer is writable. Each returned
        // outcome is owned separately and cannot release the execution used by the next call.
        check(unsafe { nx_session_execute(session.0, self.0, ptr::null(), &mut outcome) })?;
        Ok(UploadOutcome(outcome))
    }
}

impl Drop for UploadExecution {
    fn drop(&mut self) {
        // SAFETY: the execution is live, no call uses it, and it is released exactly once here.
        unsafe { nx_execution_free(self.0) };
    }
}

struct UploadOutcome(*mut Outcome);

impl UploadOutcome {
    fn disposition(&self) -> Disposition {
        // SAFETY: this wrapper retains its live outcome throughout the call.
        unsafe { nx_outcome_disposition(self.0) }
    }

    fn failure(&self) -> Option<UploadFailure> {
        let mut failure = UploadFailure::InvalidStream;
        // SAFETY: this wrapper retains its live outcome, and the out-parameter is writable.
        let present = unsafe { nx_outcome_upload_failure(self.0, &mut failure) };
        // SAFETY: the outcome remains live, and the header permits a null out-parameter.
        let present_without_output = unsafe { nx_outcome_upload_failure(self.0, ptr::null_mut()) };
        assert_eq!(present_without_output, present);
        if present {
            Some(failure)
        } else {
            assert_eq!(
                failure,
                UploadFailure::InvalidStream,
                "absence writes no output"
            );
            None
        }
    }

    fn require_completed(&self) -> io::Result<()> {
        let disposition = self.disposition();
        let failure = self.failure();
        if disposition != Disposition::Completed || failure.is_some() {
            return Err(io::Error::other(format!(
                "the upload must complete without a refusal: {disposition:?}, {failure:?}"
            )));
        }
        Ok(())
    }

    fn require_identity_conflict(&self) -> io::Result<()> {
        let disposition = self.disposition();
        let failure = self.failure();
        if disposition != Disposition::Failed || failure != Some(UploadFailure::IdentityConflict) {
            return Err(io::Error::other(format!(
                "the upload must report its typed identity conflict: {disposition:?}, {failure:?}"
            )));
        }
        Ok(())
    }
}

impl Drop for UploadOutcome {
    fn drop(&mut self) {
        // SAFETY: the outcome is live and is released exactly once here, including on errors.
        unsafe { nx_outcome_free(self.0) };
    }
}

pub(super) fn run(
    session: &OpenSession,
    query: &str,
    payload: &Path,
    emit: &mut Emit<'_>,
) -> io::Result<()> {
    {
        let description = UploadExecution::prepare(session, "DESCRIBE CLUSTER;")?;
        let outcome = description.execute(session)?;
        outcome.require_completed()?;
    }
    let execution = UploadExecution::prepare(session, query)?;
    for line in ["UPLOAD completed", "RETRY completed"] {
        let outcome = execution.execute(session)?;
        outcome.require_completed()?;
        emit(line)?;
    }
    std::fs::write(payload, b"different archive\n")?;
    let outcome = execution.execute(session)?;
    outcome.require_identity_conflict()?;
    emit("CONFLICT IdentityConflict")?;
    emit("PASS")
}
