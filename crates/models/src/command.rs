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

use crate::Timestamp;

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

    /// Returns the caller time embedded in a UUIDv7 retry identity.
    ///
    /// Durable command admission requires this timestamp so a replicated time fence can reject a
    /// reclaimed identity without retaining every caller-selected reference forever. Internal
    /// identities that do not enter that ledger may continue to use the broader reference syntax.
    pub fn retry_issued_at(
        &self,
    ) -> Result<Timestamp, Report<CommandExecutionReferenceTimestampError>> {
        let value = uuid::Uuid::parse_str(self.as_str())
            .map_err(|_| Report::new(CommandExecutionReferenceTimestampError::NotUuidV7))?;
        if value.get_version() != Some(uuid::Version::SortRand) {
            return Err(Report::new(
                CommandExecutionReferenceTimestampError::NotUuidV7,
            ));
        }
        let timestamp = value
            .get_timestamp()
            .ok_or_else(|| Report::new(CommandExecutionReferenceTimestampError::NotUuidV7))?;
        let (seconds, subsec_nanos) = timestamp.to_unix();
        let unix_seconds = u128::from(seconds)
            .checked_mul(1_000_000_000)
            .ok_or_else(|| {
                Report::new(CommandExecutionReferenceTimestampError::OutsideTimestampRange)
            })?;
        let unix_nanos = unix_seconds
            .checked_add(u128::from(subsec_nanos))
            .ok_or_else(|| {
                Report::new(CommandExecutionReferenceTimestampError::OutsideTimestampRange)
            })?;
        let unix_nanos = i64::try_from(unix_nanos).map_err(|_| {
            Report::new(CommandExecutionReferenceTimestampError::OutsideTimestampRange)
        })?;
        Ok(Timestamp::from_unix_nanos(unix_nanos))
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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommandExecutionReferenceTimestampError {
    #[error("command execution retry identity must be a UUID version 7 value")]
    NotUuidV7,
    #[error("command execution retry identity timestamp is outside the supported range")]
    OutsideTimestampRange,
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

    #[test]
    fn uuid_v7_retry_identity_exposes_its_creation_time() {
        let reference =
            CommandExecutionReference::parse("018bcfe5-687b-7000-8000-000000000001".to_string())
                .assured("UUIDv7 text uses only execution-reference characters");

        assert_eq!(
            reference
                .retry_issued_at()
                .assured("the fixture is a UUIDv7 reference")
                .unix_nanos(),
            1_700_000_000_123_000_000
        );
        assert!(
            CommandExecutionReference::parse("caller-selected")
                .assured("the literal uses accepted reference characters")
                .retry_issued_at()
                .is_err()
        );
    }
}
