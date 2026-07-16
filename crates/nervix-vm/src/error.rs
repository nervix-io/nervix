use nervix_nspl::vm_program::Span;
use thiserror::Error;

use crate::ir::RegisterRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    DivisionByZero,
    Overflow,
    CastFailed,
    InvalidArgument,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DivisionByZero => "division_by_zero",
            Self::Overflow => "overflow",
            Self::CastFailed => "cast_failed",
            Self::InvalidArgument => "invalid_argument",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SideError {
    pub code: ErrorCode,
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message} at {span}")]
pub struct CompileError {
    pub code: &'static str,
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("batch schema does not match compiled schema")]
    SchemaMismatch,
    #[error("invalid batch: {message}")]
    InvalidBatch { message: String },
    #[error("required output column '{column}' is uninitialized")]
    UninitializedRequiredColumn { column: String },
    #[error("required output column '{column}' contains null values")]
    NullForRequiredColumn { column: String },
    #[error("missing register {reg}")]
    MissingRegister { reg: RegisterRef },
    #[error("register {reg} does not contain {expected}")]
    InvalidRegisterType {
        reg: RegisterRef,
        expected: &'static str,
    },
    #[error("blocking execution task failed: {message}")]
    BlockingExecutionFailed { message: String },
}

#[cfg(test)]
mod tests {
    use super::ErrorCode;

    #[test]
    fn error_code_strings_are_stable() {
        assert_eq!(ErrorCode::DivisionByZero.as_str(), "division_by_zero");
        assert_eq!(ErrorCode::Overflow.as_str(), "overflow");
        assert_eq!(ErrorCode::CastFailed.as_str(), "cast_failed");
        assert_eq!(ErrorCode::InvalidArgument.as_str(), "invalid_argument");
    }
}
