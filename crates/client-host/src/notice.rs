//! A server notice a host reads: its level, kind and text, and the class and producing node of the
//! error a runtime notice reports.
//!
//! Layer: edges.
//!
//! - **Owns.** The notice levels and kinds a host reads, with their stable values, and reading a
//!   notice's text and the class, node kind and node name of the error it reports.
//! - **Depends on.** The Rust client's server events and the error vocabulary.
//! - **Must not know.** How notices are queued, coalesced or routed; the session decides that.
//!
//! A notice owns the event it reports and nothing else, so every field is read without allocating,
//! and its text and the node's name are borrowed from the notice.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use nervix_client_core::{ServerEvent, wire};

/// How urgently a notice asks for an operator's attention, with its stable values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NoticeLevel {
    Info = 1,
    Warning = 2,
    Error = 3,
}

impl From<wire::NoticeLevel> for NoticeLevel {
    fn from(level: wire::NoticeLevel) -> Self {
        match level {
            wire::NoticeLevel::Info => Self::Info,
            wire::NoticeLevel::Warning => Self::Warning,
            wire::NoticeLevel::Error => Self::Error,
        }
    }
}

/// What a notice reports, with its stable values, so a host routes it without reading its text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum NoticeKind {
    /// A node of the serving node's live set joined, left, or became a new incarnation.
    Membership = 1,
    /// A Raft transition of the serving node, or a membership operation it proposes as leader.
    Leadership = 2,
    /// A committed cluster-parameter revision the serving node installed.
    ClusterParameters = 3,
    /// A failure the runtime of the serving node, or of another node, recovered from.
    Runtime = 4,
    /// A control-plane failure the serving node recovered from.
    ControlPlane = 5,
    /// A licensing lifecycle observation or policy warning.
    Licensing = 6,
}

impl From<wire::NoticeKind> for NoticeKind {
    fn from(kind: wire::NoticeKind) -> Self {
        match kind {
            wire::NoticeKind::Membership => Self::Membership,
            wire::NoticeKind::Leadership => Self::Leadership,
            wire::NoticeKind::ClusterParameters => Self::ClusterParameters,
            wire::NoticeKind::Runtime => Self::Runtime,
            wire::NoticeKind::ControlPlane => Self::ControlPlane,
            wire::NoticeKind::Licensing => Self::Licensing,
        }
    }
}

/// One server notice, as the host holds it.
#[derive(Debug)]
pub struct Notice {
    event: ServerEvent,
}

impl Notice {
    pub fn new(event: ServerEvent) -> Self {
        Self { event }
    }

    /// The notice as the Rust client reported it.
    pub fn server_event(&self) -> &ServerEvent {
        &self.event
    }

    pub fn level(&self) -> NoticeLevel {
        NoticeLevel::from(self.event.level)
    }

    pub fn kind(&self) -> NoticeKind {
        NoticeKind::from(self.event.kind)
    }

    /// The notice's display text, which a host shows and never routes by.
    pub fn message(&self) -> &str {
        &self.event.message
    }

    /// The class of the error a runtime notice reports, as its canonical lower-case name
    /// (`evaluation`, `refused`, ...), or `None` for a notice that reports no classified error.
    pub fn error_class(&self) -> Option<&'static str> {
        let error = self.event.error.as_ref()?;
        Some(error.class.as_str())
    }

    /// The kind of the graph node that produced the reported error, as the model kind's canonical
    /// snake_case name (`junction`, `emitter`, ...), or `None` when the notice reports no
    /// classified error.
    pub fn error_node_kind(&self) -> Option<&'static str> {
        let error = self.event.error.as_ref()?;
        Some(error.node.kind.as_str())
    }

    /// The name of the graph node that produced the reported error, or `None` when the notice
    /// reports no classified error.
    pub fn error_node(&self) -> Option<&str> {
        let error = self.event.error.as_ref()?;
        Some(error.node.identifier.as_str())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_client_core::wire::ServerNotice;
    use nervix_models::{MessageErrorClass, ModelKind, ModelName, NodeRef, ReportedError};

    use super::*;

    fn notice(error: Option<ReportedError>) -> Notice {
        Notice::new(ServerEvent::from(ServerNotice {
            level: wire::NoticeLevel::Error,
            kind: wire::NoticeKind::Runtime,
            message: "window_processor 'hourly' message error in domain 'shop': too wide"
                .to_string(),
            error,
        }))
    }

    #[test]
    fn a_runtime_notice_reads_the_class_and_node_of_the_error_it_reports() {
        let reported = notice(Some(ReportedError {
            class: MessageErrorClass::LIMIT,
            node: NodeRef::new(
                ModelKind::WindowProcessor,
                ModelName::parse("hourly").assured("a valid node name"),
            ),
        }));
        assert_eq!(reported.level(), NoticeLevel::Error);
        assert_eq!(reported.kind(), NoticeKind::Runtime);
        assert_eq!(
            reported.message(),
            "window_processor 'hourly' message error in domain 'shop': too wide"
        );
        assert_eq!(reported.error_class(), Some("limit"));
        assert_eq!(reported.error_node_kind(), Some("window_processor"));
        assert_eq!(reported.error_node(), Some("hourly"));

        let unclassified = notice(None);
        assert_eq!(unclassified.error_class(), None);
        assert_eq!(unclassified.error_node_kind(), None);
        assert_eq!(unclassified.error_node(), None);
    }

    #[test]
    fn every_wire_level_and_kind_reads_as_its_host_value() {
        let levels = [
            (wire::NoticeLevel::Info, NoticeLevel::Info),
            (wire::NoticeLevel::Warning, NoticeLevel::Warning),
            (wire::NoticeLevel::Error, NoticeLevel::Error),
        ];
        for (wire_level, level) in levels {
            assert_eq!(NoticeLevel::from(wire_level), level);
        }
        let kinds = [
            (wire::NoticeKind::Membership, NoticeKind::Membership),
            (wire::NoticeKind::Leadership, NoticeKind::Leadership),
            (
                wire::NoticeKind::ClusterParameters,
                NoticeKind::ClusterParameters,
            ),
            (wire::NoticeKind::Runtime, NoticeKind::Runtime),
            (wire::NoticeKind::ControlPlane, NoticeKind::ControlPlane),
            (wire::NoticeKind::Licensing, NoticeKind::Licensing),
        ];
        for (wire_kind, kind) in kinds {
            assert_eq!(NoticeKind::from(wire_kind), kind);
        }
    }
}
