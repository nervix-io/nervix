//! Ruby boundary tests, outside the product layer order.
//!
//! - **Owns.** Typed exception, GVL/cancellation, exact value and GC-lifetime regressions.
//! - **Depends on.** The production registration and one embedded Ruby interpreter.
//! - **Must not know.** Server internals or a copied host state machine.

use magnus::{RClass, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_host::{Failure, FailureKind};

#[test]
fn ruby_boundary_preserves_kinds_values_views_and_interruptible_waits() {
    // SAFETY: This is the only unit test in this executable. Ruby is initialized once on this
    // thread, every Ruby value ends before the cleanup guard, and no library worker calls Ruby.
    let ruby = unsafe { magnus::embed::init() };
    crate::register(&ruby).assured("the native binding registers in a fresh interpreter");
    let cases = [
        (FailureKind::InvalidArgument, "InvalidArgumentError"),
        (FailureKind::Connect, "ConnectError"),
        (FailureKind::Transport, "TransportError"),
        (FailureKind::Uncertain, "UncertainOutcomeError"),
        (FailureKind::Rejected, "RejectedError"),
        (FailureKind::Deadline, "DeadlineError"),
        (FailureKind::Cancelled, "CancelledError"),
        (FailureKind::Overflow, "EventOverflowError"),
        (FailureKind::Protocol, "ProtocolError"),
        (FailureKind::Type, "TypeMismatchError"),
        (FailureKind::Closed, "ClosedError"),
        (FailureKind::Interrupted, "ConsumerInterruptedError"),
        (FailureKind::ReopenRequired, "ReopenRequiredError"),
    ];
    let reference = nervix_client_core::CommandExecutionReference::parse("ruby-boundary-execution")
        .assured("the reference is valid");
    for (kind, class) in cases {
        let failure =
            Failure::new(kind, "outer operation: complete typed cause").concerning(&reference);
        let error = crate::errors::raise(failure);
        let value = error
            .value()
            .assured("projecting a host failure creates a Ruby exception");
        let expected: RClass = ruby
            .eval(&format!("Nervix::Client::{class}"))
            .assured("registration declared every expected exception class");
        let matches: bool = value
            .funcall("is_a?", (expected,))
            .assured("Ruby can inspect an exception's class");
        assert!(matches, "{kind:?} did not raise {class}");
        let actual: i32 = value
            .funcall("kind", ())
            .assured("every exception carries its typed kind");
        assert_eq!(actual, crate::enums::FailureKind::code(kind));
        let message: String = value
            .funcall("message", ())
            .assured("every exception carries its whole message");
        assert_eq!(message, "outer operation: complete typed cause");
        let actual: String = value
            .funcall("execution_reference", ())
            .assured("the exception carries its reference");
        assert_eq!(actual, reference.as_str());
        let refusal: Option<i32> = value
            .funcall("open_refusal", ())
            .assured("unrelated failures have no refusal");
        assert_eq!(refusal, None);
    }
    let refused = Failure::from(nervix_client_core::ClientError::ProducerRefused {
        refusal: nervix_client_core::ClientProducerRefusal::SchemaMismatch,
        message: "the complete schema differs".to_string(),
    });
    let expected_message = refused.message().to_string();
    let error = crate::errors::raise(refused);
    let value = error.value().assured("a refusal projects an exception");
    let refusal: i32 = value
        .funcall("open_refusal", ())
        .assured("the refusal is retained");
    assert_eq!(
        refusal,
        crate::enums::OpenRefusal::code(nervix_client_host::OpenRefusal::SchemaMismatch)
    );
    let message: String = value
        .funcall("message", ())
        .assured("the refusal message is retained");
    assert_eq!(message, expected_message);
    ruby.eval::<magnus::Value>(include_str!("../tests/boundary.rb"))
        .assured("the Ruby boundary's exact-value, GC and GVL regressions pass");
    // A Rust unwind must return through the GVL boundary before it resumes on the Rust side.
    // Crossing the C callback with an unwind would abort the interpreter process instead.
    let panicked = std::panic::catch_unwind(|| {
        let _: Result<(), magnus::Error> = crate::gvl::without_gvl(None, |_| {
            panic!("the fixture's native work panicked");
        });
    });
    assert!(
        panicked.is_err(),
        "the native unwind returns to Rust after GVL reacquisition"
    );
    let answer: u32 = ruby
        .eval("40 + 2")
        .assured("Ruby remains usable after the native unwind");
    assert_eq!(answer, 42);
    let peer = crate::test_server::Peer::start();
    let fixture = ruby
        .define_module("RubyLifetime")
        .assured("the lifetime fixture module registers");
    fixture
        .const_set("URI", peer.uri.as_str())
        .assured("the fixture URI is available in Ruby");
    ruby.eval::<magnus::Value>(include_str!("../tests/lifetime.rb"))
        .assured("a producer submits and closes after its Ruby session has been collected");
}
