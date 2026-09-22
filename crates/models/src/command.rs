//! Stable identity for one admitted administrative command.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The validated execution reference shared by command protocols and durable state.
//! - **Depends on.** Serialization primitives only.
//! - **Must not know.** Sessions, consensus, parsing, command effects, or runtime application.

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

const MAX_COMMAND_EXECUTION_REFERENCE_BYTES: usize = 128;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct CommandExecutionReference(String);

impl CommandExecutionReference {
    pub fn parse(value: impl Into<String>) -> Result<Self, Report<CommandExecutionReferenceError>> {
        let value = value.into();
        if value.is_empty() {
            return Err(Report::new(CommandExecutionReferenceError::Empty));
        }
        if value.len() > MAX_COMMAND_EXECUTION_REFERENCE_BYTES {
            return Err(Report::new(CommandExecutionReferenceError::TooLong {
                actual: value.len(),
                limit: MAX_COMMAND_EXECUTION_REFERENCE_BYTES,
            }));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(Report::new(
                CommandExecutionReferenceError::InvalidCharacter,
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives the durable identity of one statement in this request.
    ///
    /// The derived reference has a fixed size, so every accepted request reference can identify
    /// its statements even when the caller used the complete request-reference byte budget.
    pub fn derive_step(&self, position: usize) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix.command.step\0");
        hasher.update(self.as_str().as_bytes());
        hasher.update(b"\0");
        hasher.update(position.to_string().as_bytes());
        Self(format!("step-{}", hasher.finalize().to_hex()))
    }
}

impl std::fmt::Display for CommandExecutionReference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandExecutionReferenceError {
    #[error("command execution reference must not be empty")]
    Empty,
    #[error(
        "command execution reference contains {actual} bytes, exceeding the {limit}-byte limit"
    )]
    TooLong { actual: usize, limit: usize },
    #[error("command execution reference may contain only ASCII letters, digits, '.', '_' and '-'")]
    InvalidCharacter,
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    #[test]
    fn every_accepted_reference_can_derive_distinct_step_references() {
        let request =
            CommandExecutionReference::parse("r".repeat(MAX_COMMAND_EXECUTION_REFERENCE_BYTES))
                .assured("the test request exactly matches the accepted byte limit");

        let first = request.derive_step(0);
        let second = request.derive_step(1);

        assert_ne!(first, second);
        assert!(first.as_str().len() <= MAX_COMMAND_EXECUTION_REFERENCE_BYTES);
        assert!(CommandExecutionReference::parse(first.to_string()).is_ok());
    }
}
