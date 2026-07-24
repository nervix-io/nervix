use arrow_schema::ArrowError;
use nervix_wasm_protocol::ProtocolError;
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
    #[error("guest snapshot carries prototype pending-batch bytes this SDK does not manage")]
    UnsupportedSnapshot,
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
            Self::InvalidSize | Self::UnsupportedSnapshot => ERR_INVALID_SIZE,
            Self::OutOfBounds => ERR_OUT_OF_BOUNDS,
            Self::NotInitialized => ERR_NOT_INITIALIZED,
            Self::ArrowIpc(_) => ERR_ARROW_IPC,
            Self::Protocol(_) => ERR_ENVELOPE,
            Self::Failed { .. } => ERR_ERROR_STATE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abi_codes_match_the_documented_contract() {
        assert_eq!(GuestError::InvalidSize.abi_code(), -1);
        assert_eq!(GuestError::UnsupportedSnapshot.abi_code(), -1);
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
}
