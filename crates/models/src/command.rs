//! Stable identity for one admitted administrative command.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The validated execution reference shared by command protocols and durable state.
//! - **Depends on.** Serialization primitives only.
//! - **Must not know.** Sessions, consensus, parsing, command effects, or runtime application.

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
    pub fn parse(value: impl Into<String>) -> Result<Self, CommandExecutionReferenceError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CommandExecutionReferenceError::Empty);
        }
        if value.len() > MAX_COMMAND_EXECUTION_REFERENCE_BYTES {
            return Err(CommandExecutionReferenceError::TooLong {
                actual: value.len(),
                limit: MAX_COMMAND_EXECUTION_REFERENCE_BYTES,
            });
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(CommandExecutionReferenceError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
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
