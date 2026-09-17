use arrow_schema::ArrowError;
use nervix_wasm_protocol::{ProtocolError, SavedStateRejection};
use thiserror::Error;

pub(crate) const SUCCESS: i32 = 0;
pub(crate) const ERR_INVALID_SIZE: i32 = -1;
pub(crate) const ERR_OUT_OF_BOUNDS: i32 = -2;
pub(crate) const ERR_NOT_INITIALIZED: i32 = -3;
pub(crate) const ERR_ARROW_IPC: i32 = -4;
pub(crate) const ERR_ENVELOPE: i32 = -5;
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
    #[error("Arrow IPC processing failed: {0}")]
    ArrowIpc(#[from] ArrowError),
    #[error("envelope protocol violation: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("{reason}")]
    Failed { reason: String },
}

impl GuestError {
    /// Wraps a processor-defined fatal failure. The ABI adapter reports the
    /// reason through the global error channel and latches the guest into
    /// error state.
    pub fn failed(reason: impl Into<String>) -> Self {
        Self::Failed {
            reason: reason.into(),
        }
    }

    pub(crate) const fn abi_code(&self) -> i32 {
        match self {
            Self::InvalidSize => ERR_INVALID_SIZE,
            Self::OutOfBounds => ERR_OUT_OF_BOUNDS,
            Self::NotInitialized => ERR_NOT_INITIALIZED,
            Self::ArrowIpc(_) => ERR_ARROW_IPC,
            Self::Protocol(_) => ERR_ENVELOPE,
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
    #[error("saved state is not a guest snapshot envelope: {0}")]
    UndecodableEnvelope(ProtocolError),
    #[error("saved snapshot carries init metadata this guest cannot decode: {0}")]
    UndecodableInitMetadata(ProtocolError),
    #[error("saved snapshot was taken under a different branch configuration")]
    OtherBranchConfiguration,
    /// The processor refused the application state; its own error is the whole reason.
    #[error("{0}")]
    ApplicationState(GuestError),
}

impl RejectedSnapshot {
    /// The verdict this rejection reports: whether the snapshot envelope or the application state
    /// it carries is unusable.
    pub(crate) const fn verdict(&self) -> SavedStateRejection {
        match self {
            Self::UndecodableEnvelope(_)
            | Self::UndecodableInitMetadata(_)
            | Self::OtherBranchConfiguration => SavedStateRejection::SnapshotEnvelope,
            Self::ApplicationState(_) => SavedStateRejection::ApplicationState,
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
        assert_eq!(
            GuestError::Protocol(ProtocolError::InvalidIdentifier).abi_code(),
            -5
        );
        assert_eq!(GuestError::failed("fatal").abi_code(), -6);
    }

    #[test]
    fn a_rejected_envelope_and_a_rejected_application_state_report_their_own_verdicts() {
        let envelope_rejections = [
            RejectedSnapshot::UndecodableEnvelope(ProtocolError::InvalidIdentifier),
            RejectedSnapshot::UndecodableInitMetadata(ProtocolError::InvalidIdentifier),
            RejectedSnapshot::OtherBranchConfiguration,
        ];
        for rejected in envelope_rejections {
            assert_eq!(rejected.verdict(), SavedStateRejection::SnapshotEnvelope);
        }
        assert_eq!(
            RejectedSnapshot::ApplicationState(GuestError::failed("counters are truncated"))
                .verdict(),
            SavedStateRejection::ApplicationState
        );
    }
}
