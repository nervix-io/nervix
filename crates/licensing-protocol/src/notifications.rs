//! Typed invalidation hints carried by the licensing event stream.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The current sync, decision and trust payloads and their JSON mapping.
//! - **Depends on.** Maintained JSON serialization and typed error reports.
//! - **Must not know.** SSE framing, clocks, connections, consensus or policy grants.

use error_stack::Report;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationState {
    pub notification_revision: u64,
    pub decision_revision: u64,
    pub conditions_revision: u64,
    pub trust_serial: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionNotification {
    pub notification_revision: u64,
    pub decision_revision: u64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustNotification {
    pub notification_revision: u64,
    pub trust_serial: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notification {
    Sync(NotificationState),
    Decision(DecisionNotification),
    Trust(TrustNotification),
}

#[derive(Debug, Error)]
pub enum NotificationError {
    #[error("unknown licensing notification kind")]
    Kind,
    #[error("licensing notification payload does not match its kind")]
    Payload,
}

/// The original JSON failure remains inspectable, but diagnostics never render input values.
pub struct NotificationJsonError(serde_json::Error);
impl NotificationJsonError {
    pub fn cause(&self) -> &serde_json::Error {
        &self.0
    }
}
impl std::fmt::Display for NotificationJsonError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "licensing notification JSON {:?} at line {}, column {}",
            self.0.classify(),
            self.0.line(),
            self.0.column()
        )
    }
}
impl std::fmt::Debug for NotificationJsonError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}
impl std::error::Error for NotificationJsonError {}

impl Notification {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Sync(_) => "sync",
            Self::Decision(_) => "decision",
            Self::Trust(_) => "trust",
        }
    }

    pub fn revision(&self) -> u64 {
        match self {
            Self::Sync(value) => value.notification_revision,
            Self::Decision(value) => value.notification_revision,
            Self::Trust(value) => value.notification_revision,
        }
    }

    /// The transport bounds input bytes before calling this mapping.
    pub fn decode(kind: &str, data: &str) -> error_stack::Result<Self, NotificationError> {
        let decoded = match kind {
            "sync" => serde_json::from_str(data).map(Self::Sync),
            "decision" => serde_json::from_str(data).map(Self::Decision),
            "trust" => serde_json::from_str(data).map(Self::Trust),
            _ => return Err(Report::new(NotificationError::Kind)),
        };
        decoded.map_err(|error| {
            Report::new(NotificationJsonError(error)).change_context(NotificationError::Payload)
        })
    }

    pub fn encode(&self) -> error_stack::Result<String, NotificationError> {
        let encoded = match self {
            Self::Sync(value) => serde_json::to_string(value),
            Self::Decision(value) => serde_json::to_string(value),
            Self::Trust(value) => serde_json::to_string(value),
        };
        encoded.map_err(|error| {
            Report::new(NotificationJsonError(error)).change_context(NotificationError::Payload)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_notification_reports_do_not_render_payload_values() {
        let error = match Notification::decode(
            "trust",
            r#"{"notification_revision":1,"trust_serial":"sensitive-value"}"#,
        ) {
            Err(error) => error,
            Ok(_) => panic!("a string is not a trust serial"),
        };
        assert!(!format!("{error:#}").contains("sensitive-value"));
        let Some(cause) = error.downcast_ref::<NotificationJsonError>() else {
            panic!("the concrete JSON failure is retained");
        };
        assert!(cause.cause().is_data());
    }

    #[test]
    fn notification_payloads_require_the_exact_fields_of_their_kind() {
        assert!(Notification::decode("sync", "{}").is_err());
        assert!(
            Notification::decode(
                "decision",
                r#"{"notification_revision":1,"trust_serial":2}"#
            )
            .is_err()
        );
        assert!(
            Notification::decode("trust", r#"{"notification_revision":1,"trust_serial":-1}"#)
                .is_err()
        );
        assert!(
            Notification::decode(
                "trust",
                r#"{"notification_revision":1,"trust_serial":2,"conditions":{}}"#
            )
            .is_err()
        );
        assert!(Notification::decode("message", "{}").is_err());
    }
}
