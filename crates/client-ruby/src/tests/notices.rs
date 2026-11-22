//! The Ruby projection of a server notice, with and without the error a runtime notice reports.
//!
//! Outside the layer order: a test harness.
//! - **Owns.** Fixture notices built from server notices, and the Ruby checks of their readers.
//! - **Depends on.** The production notice class, the host notice and the client's server event.
//! - **Must not know.** How a session queues, drops or routes the notices it receives.

use magnus::{Error, Ruby, function, prelude::*};
use meticulous::ResultExt as _;
use nervix_client_core::{ServerEvent, wire};
use nervix_client_host as host;
use nervix_models::{MessageErrorClass, ModelKind, ModelName, NodeRef, ReportedError};

/// A notice as the session hands it to the host, from the notice the server sent.
fn notice(
    level: wire::NoticeLevel,
    kind: wire::NoticeKind,
    message: &str,
    error: Option<ReportedError>,
) -> crate::notices::Notice {
    let event = ServerEvent::from(wire::ServerNotice {
        level,
        kind,
        message: message.to_string(),
        error,
    });
    crate::notices::Notice::new(host::Notice::new(event))
}

fn reported() -> crate::notices::Notice {
    let error = ReportedError {
        class: MessageErrorClass::EVALUATION,
        node: NodeRef::new(
            ModelKind::Junction,
            ModelName::parse("scorer").assured("a valid node name"),
        ),
    };
    notice(
        wire::NoticeLevel::Error,
        wire::NoticeKind::Runtime,
        "junction 'scorer' message error in domain 'shop': integer division by zero",
        Some(error),
    )
}

fn unclassified() -> crate::notices::Notice {
    notice(
        wire::NoticeLevel::Info,
        wire::NoticeKind::Membership,
        "node 'node-2' joined as incarnation 7",
        None,
    )
}

pub(super) fn check(ruby: &Ruby) -> Result<(), Error> {
    let fixture = ruby.define_module("RubyNoticeFixture")?;
    fixture.define_singleton_method("reported", function!(reported, 0))?;
    fixture.define_singleton_method("unclassified", function!(unclassified, 0))?;
    ruby.eval::<magnus::Value>(
        r#"
nx = Nervix::Client
reported = RubyNoticeFixture.reported
raise 'notice class' unless reported.is_a?(nx::Notice)
raise 'runtime error notice' unless [reported.level, reported.kind] == [nx::NoticeLevel::ERROR, nx::NoticeKind::RUNTIME]
raise 'whole display text' unless reported.message == "junction 'scorer' message error in domain 'shop': integer division by zero"
raise 'reported error' unless [reported.error_class, reported.error_node_kind, reported.error_node] == %w[evaluation junction scorer]
unclassified = RubyNoticeFixture.unclassified
raise 'membership notice' unless [unclassified.level, unclassified.kind] == [nx::NoticeLevel::INFO, nx::NoticeKind::MEMBERSHIP]
raise 'membership text' unless unclassified.message == "node 'node-2' joined as incarnation 7"
raise 'no reported error' unless [unclassified.error_class, unclassified.error_node_kind, unclassified.error_node] == [nil, nil, nil]
"#,
    )?;
    Ok(())
}
