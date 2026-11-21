use arrow_schema::ArrowError;
use error_stack::Report;
use nervix_wasm_protocol::SavedStateRejection;
use thiserror::Error;

#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM guest ABI's fallible exports return 0 on success"
    )
)]
pub(crate) const SUCCESS: i32 = 0;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers a negative or invalid size -1 among the \
                  SDK's failure codes, which stay apart from the ABI's reserved -7, -8 and -9"
    )
)]
pub(crate) const ERR_INVALID_SIZE: i32 = -1;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers a pointer and size range outside guest \
                  memory -2 among the SDK's failure codes"
    )
)]
pub(crate) const ERR_OUT_OF_BOUNDS: i32 = -2;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers a call before nervix_init -3 among the SDK's \
                  failure codes"
    )
)]
pub(crate) const ERR_NOT_INITIALIZED: i32 = -3;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers an Arrow IPC decode or encode error -4 among \
                  the SDK's failure codes"
    )
)]
pub(crate) const ERR_ARROW_IPC: i32 = -4;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers a batch envelope decode or encode error -5 \
                  among the SDK's failure codes"
    )
)]
pub(crate) const ERR_ENVELOPE: i32 = -5;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Guest SDK chapter numbers a guest latched into error state, whose \
                  reason is on the global-error channel, -6 among the SDK's failure codes"
    )
)]
pub(crate) const ERR_ERROR_STATE: i32 = -6;

/// Semantic guest failure mapped onto the negative Nervix WASM ABI codes.
#[derive(Debug, Error)]
pub enum GuestError {
    #[error("byte size is negative or does not fit the guest address space")]
    InvalidSize,
    #[error("pointer range is outside the guest buffer")]
    OutOfBounds,
    #[error("processor was invoked before initialization")]
    NotInitialized,
    #[error("Arrow IPC processing failed")]
    ArrowIpc(#[from] ArrowError),
    #[error("envelope protocol violation")]
    Protocol,
    #[error("{reason}")]
    Failed { reason: String },
}

impl GuestError {
    /// Wraps a processor-defined fatal failure. The ABI adapter reports the
    /// reason through the global error channel and latches the guest into
    /// error state.
    pub fn failed(reason: impl Into<String>) -> Report<Self> {
        Report::new(Self::Failed {
            reason: reason.into(),
        })
    }

    pub(crate) const fn abi_code(&self) -> i32 {
        match self {
            Self::InvalidSize => ERR_INVALID_SIZE,
            Self::OutOfBounds => ERR_OUT_OF_BOUNDS,
            Self::NotInitialized => ERR_NOT_INITIALIZED,
            Self::ArrowIpc(_) => ERR_ARROW_IPC,
            Self::Protocol => ERR_ENVELOPE,
            Self::Failed { .. } => ERR_ERROR_STATE,
        }
    }
}

/// Why the saved state handed to `nervix_load_state` cannot be restored.
///
/// Each reason is a verdict on the saved bytes, which the SDK reports with the reserved
/// `nervix_load_state` code of its [`SavedStateRejection`] and explains on the global-error
/// channel.
#[derive(Debug, Error)]
pub(crate) enum RejectedSnapshot {
    #[error("saved state is not a guest snapshot envelope")]
    UndecodableEnvelope,
    #[error("saved snapshot carries init metadata this guest cannot decode")]
    UndecodableInitMetadata,
    #[error("saved snapshot was taken under a different branch configuration")]
    OtherBranchConfiguration,
    /// The processor refused the application state; the guest report below this context is the
    /// reason the ABI renders.
    #[error("saved application state was rejected")]
    ApplicationState,
}

impl RejectedSnapshot {
    /// The verdict this rejection reports: whether the snapshot envelope or the application state
    /// it carries is unusable.
    pub(crate) const fn verdict(&self) -> SavedStateRejection {
        match self {
            Self::UndecodableEnvelope
            | Self::UndecodableInitMetadata
            | Self::OtherBranchConfiguration => SavedStateRejection::SnapshotEnvelope,
            Self::ApplicationState => SavedStateRejection::ApplicationState,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_codes_match_the_documented_contract() {
        assert_eq!(GuestError::InvalidSize.abi_code(), -1);
        assert_eq!(GuestError::OutOfBounds.abi_code(), -2);
        assert_eq!(GuestError::NotInitialized.abi_code(), -3);
        assert_eq!(
            GuestError::ArrowIpc(ArrowError::ParseError("x".to_string())).abi_code(),
            -4
        );
        assert_eq!(GuestError::Protocol.abi_code(), -5);
        assert_eq!(GuestError::failed("fatal").current_context().abi_code(), -6);
    }

    #[test]
    fn an_arrow_ipc_failure_names_its_cause_once() {
        let report = Report::new(GuestError::ArrowIpc(ArrowError::ParseError(
            "truncated message".to_string(),
        )));

        assert_eq!(
            format!("{report:#}"),
            "Arrow IPC processing failed: Parser error: truncated message"
        );
    }

    #[test]
    fn a_rejected_envelope_and_a_rejected_application_state_report_their_own_verdicts() {
        let envelope_rejections = [
            RejectedSnapshot::UndecodableEnvelope,
            RejectedSnapshot::UndecodableInitMetadata,
            RejectedSnapshot::OtherBranchConfiguration,
        ];
        for rejected in envelope_rejections {
            assert_eq!(rejected.verdict(), SavedStateRejection::SnapshotEnvelope);
        }
        assert_eq!(
            RejectedSnapshot::ApplicationState.verdict(),
            SavedStateRejection::ApplicationState
        );
    }
}
