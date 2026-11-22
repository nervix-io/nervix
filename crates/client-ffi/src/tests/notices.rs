//! Server notices through the C ABI, as a host calls it.
//!
//! A notice is read field by field, so the error accessor is held to the notices that report a
//! classified error and to those that report none. A session connected to an in-process session
//! server reads notices from real frames: in order, past a gap the client reports when they arrive
//! faster than they are read, and from the next session once the one that delivered them ends, and
//! bounds a wait that nothing ends by cancellation and by deadline.

use std::{ptr, slice, time::Duration};

use meticulous::ResultExt as _;
use nervix_client_core::{
    DomainClockAttachDisposition, DomainClockObservation, DomainClockObservedState, ServerEvent,
    wire::{self, EncodedFrame, ServerFrame, ServerNotice, SessionLimits},
};
use nervix_models::{MessageErrorClass, ModelKind, ModelName, NodeRef, ReportedError};
use nervix_primitives::thread;

use super::{
    clock_events::{DEADLINE_MILLIS, SharedSession, SharedToken, TestServer, domain},
    failure_kind, header_value, succeeded,
};
use crate::{
    ClockEventKind, Disposition, FailureKind, Notice, NoticeKind, NoticeLevel, nx_cancel_free,
    nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline, nx_notice_error, nx_notice_kind_of,
    nx_notice_level_of, nx_notice_message, nx_notice_release, nx_notice_retain,
    nx_session_next_notice,
};

/// The notices the Rust client holds for a caller that has not read them, as the session protocol
/// chapter states; one more than this drops them all.
const NOTICES_HELD: usize = 128;

/// A notice handed out as a host's reference, released when dropped.
struct SharedNotice(*mut Notice);

// SAFETY: a notice is immutable and its references are counted atomically, so a reference may be
// released on any thread, which is what the header promises.
unsafe impl Send for SharedNotice {}

impl SharedNotice {
    fn new(notice: ServerNotice) -> Self {
        Self(crate::abi::into_shared(Notice::new(ServerEvent::from(
            notice,
        ))))
    }

    fn level(&self) -> NoticeLevel {
        // SAFETY: the reference is live.
        unsafe { nx_notice_level_of(self.0) }
    }

    fn kind(&self) -> NoticeKind {
        // SAFETY: the reference is live.
        unsafe { nx_notice_kind_of(self.0) }
    }

    fn message(&self) -> String {
        let mut message = ptr::null();
        let mut message_len = 0;
        // SAFETY: the reference is live, the out-parameters are writable, and the text is copied
        // before the reference can be released.
        unsafe {
            nx_notice_message(self.0, &mut message, &mut message_len);
            copied(message, message_len)
        }
    }

    /// The classified error the notice reports, as the header's out-parameters carry it.
    fn error(&self) -> Option<ErrorFields> {
        let mut class = ptr::null();
        let mut class_len = 0;
        let mut node_kind = ptr::null();
        let mut node_kind_len = 0;
        let mut node = ptr::null();
        let mut node_len = 0;
        // SAFETY: the reference is live and every out-parameter is writable.
        let reported = unsafe {
            nx_notice_error(
                self.0,
                &mut class,
                &mut class_len,
                &mut node_kind,
                &mut node_kind_len,
                &mut node,
                &mut node_len,
            )
        };
        if !reported {
            assert!(
                class.is_null() && node_kind.is_null() && node.is_null(),
                "a notice that reports no classified error writes no name"
            );
            assert!(
                class_len == 0 && node_kind_len == 0 && node_len == 0,
                "a notice that reports no classified error writes no length"
            );
            return None;
        }
        // SAFETY: the binding wrote names borrowed from the live reference, and each is copied
        // before the reference can be released.
        let fields = unsafe {
            ErrorFields {
                class: copied(class, class_len),
                node_kind: copied(node_kind, node_kind_len),
                node: copied(node, node_len),
            }
        };
        Some(fields)
    }
}

impl Drop for SharedNotice {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_notice_release(self.0) };
    }
}

/// The classified error a notice reports, as the header's out-parameters carry it.
#[derive(Debug, PartialEq)]
struct ErrorFields {
    class: String,
    node_kind: String,
    node: String,
}

/// The error every reported notice of these tests carries: an expression of junction `scorer`
/// that could not be computed.
fn scorer_evaluation() -> ErrorFields {
    ErrorFields {
        class: "evaluation".to_string(),
        node_kind: "junction".to_string(),
        node: "scorer".to_string(),
    }
}

/// Copies text the binding borrowed to a host.
///
/// # Safety
///
/// `data` addresses `len` readable bytes.
unsafe fn copied(data: *const u8, len: usize) -> String {
    // SAFETY: the caller upholds this function's contract.
    let bytes = unsafe { slice::from_raw_parts(data, len) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// A runtime notice reporting the message error junction `scorer` raised.
fn runtime_error(message: &str) -> ServerNotice {
    let scorer = ModelName::parse("scorer").assured("the test node name is valid");
    ServerNotice {
        level: wire::NoticeLevel::Error,
        kind: wire::NoticeKind::Runtime,
        message: message.to_string(),
        error: Some(ReportedError {
            class: MessageErrorClass::EVALUATION,
            node: NodeRef::new(ModelKind::Junction, scorer),
        }),
    }
}

/// A notice that reports no classified error.
fn unclassified(level: wire::NoticeLevel, kind: wire::NoticeKind, message: &str) -> ServerNotice {
    ServerNotice {
        level,
        kind,
        message: message.to_string(),
        error: None,
    }
}

/// The frame a server sends for `notice`.
fn frame(notice: &ServerNotice) -> EncodedFrame<ServerFrame> {
    notice
        .encode(&SessionLimits::DEFAULT)
        .assured("a test notice fits the default limits")
}

/// The clock every attach of these tests reports: a domain that has never started.
fn stopped() -> DomainClockObservation {
    DomainClockObservation {
        generation: 0,
        state: DomainClockObservedState::Stopped,
    }
}

impl SharedSession {
    /// The next notice, which must arrive within the test deadline.
    fn next_notice(self) -> SharedNotice {
        let mut deadline = ptr::null_mut();
        let mut notice = ptr::null_mut();
        // SAFETY: the session and the token are live, the out-parameters are writable, and the
        // token is freed once no call waits on it.
        unsafe {
            succeeded(nx_cancel_with_deadline(DEADLINE_MILLIS, &mut deadline));
            let failure = nx_session_next_notice(self.0, deadline, &mut notice);
            nx_cancel_free(deadline);
            succeeded(failure);
        }
        SharedNotice(notice)
    }

    /// Waits for a notice with `token`, which the wait has to fail with, returning the failure's
    /// kind.
    fn notice_wait_with(self, token: SharedToken) -> FailureKind {
        let mut notice = ptr::null_mut();
        // SAFETY: the session and the token outlive the wait, and `notice` is writable.
        let kind = failure_kind(unsafe { nx_session_next_notice(self.0, token.0, &mut notice) });
        assert!(notice.is_null(), "a failed read writes no notice");
        kind
    }

    /// Waits for a notice with a token that ends the wait after `millis`, returning the kind of
    /// the failure the wait has to end with.
    fn notice_wait_expiring_after(self, millis: u64) -> FailureKind {
        let mut deadline = ptr::null_mut();
        // SAFETY: `deadline` is writable.
        succeeded(unsafe { nx_cancel_with_deadline(millis, &mut deadline) });
        let kind = self.notice_wait_with(SharedToken(deadline));
        // SAFETY: the token is live and no call waits on it any more.
        unsafe { nx_cancel_free(deadline) };
        kind
    }
}

#[test]
fn a_runtime_notice_reads_its_level_kind_text_and_the_error_it_reports() {
    let text = "junction 'scorer' message error in domain 'shop': integer division by zero";
    let notice = SharedNotice::new(runtime_error(text));
    assert_eq!(notice.level(), NoticeLevel::Error);
    assert_eq!(notice.kind(), NoticeKind::Runtime);
    assert_eq!(notice.message(), text);
    assert_eq!(notice.error(), Some(scorer_evaluation()));

    let mut node = ptr::null();
    let mut node_len = 0;
    // SAFETY: the reference is live, and a host may pass null for the names it does not read.
    let reported = unsafe {
        nx_notice_error(
            notice.0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut node,
            &mut node_len,
        )
    };
    assert!(reported);
    // SAFETY: the binding wrote a name borrowed from the live reference.
    assert_eq!(unsafe { copied(node, node_len) }, "scorer");
    // SAFETY: the reference is live, and every out-parameter may be null.
    let reported = unsafe {
        nx_notice_error(
            notice.0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    assert!(
        reported,
        "whether a notice reports an error reads without its names"
    );
}

#[test]
fn a_notice_that_reports_no_classified_error_writes_none() {
    let notices = [
        unclassified(
            wire::NoticeLevel::Info,
            wire::NoticeKind::Membership,
            "node 'node-2' joined",
        ),
        unclassified(
            wire::NoticeLevel::Error,
            wire::NoticeKind::Runtime,
            "emitter 'out' recovered from a connector failure",
        ),
        unclassified(
            wire::NoticeLevel::Warning,
            wire::NoticeKind::Licensing,
            "the license expires in seven days",
        ),
    ];
    for unreported in notices {
        let text = unreported.message.clone();
        let notice = SharedNotice::new(unreported);
        assert_eq!(notice.message(), text);
        assert_eq!(
            notice.error(),
            None,
            "a {:?} notice reports no error",
            notice.kind()
        );
    }
}

#[test]
fn every_notice_level_and_kind_has_its_header_value() {
    let levels = [
        (wire::NoticeLevel::Info, NoticeLevel::Info, 1),
        (wire::NoticeLevel::Warning, NoticeLevel::Warning, 2),
        (wire::NoticeLevel::Error, NoticeLevel::Error, 3),
    ];
    for (wire_level, expected, value) in levels {
        let notice = SharedNotice::new(unclassified(wire_level, wire::NoticeKind::Runtime, ""));
        assert_eq!(notice.level(), expected);
        assert_eq!(header_value(notice.level()), value);
    }
    let kinds = [
        (wire::NoticeKind::Membership, NoticeKind::Membership, 1),
        (wire::NoticeKind::Leadership, NoticeKind::Leadership, 2),
        (
            wire::NoticeKind::ClusterParameters,
            NoticeKind::ClusterParameters,
            3,
        ),
        (wire::NoticeKind::Runtime, NoticeKind::Runtime, 4),
        (wire::NoticeKind::ControlPlane, NoticeKind::ControlPlane, 5),
        (wire::NoticeKind::Licensing, NoticeKind::Licensing, 6),
    ];
    for (wire_kind, expected, value) in kinds {
        let notice = SharedNotice::new(unclassified(wire::NoticeLevel::Info, wire_kind, ""));
        assert_eq!(notice.kind(), expected);
        assert_eq!(header_value(notice.kind()), value);
    }
}

#[test]
fn a_retained_notice_outlives_a_reference_released_on_another_thread() {
    let notice = SharedNotice::new(runtime_error("retained"));
    // SAFETY: the reference is live, and the binding returns a new one.
    let second = SharedNotice(unsafe { nx_notice_retain(notice.0) });
    assert_eq!(
        second.0, notice.0,
        "a retained reference addresses the same notice"
    );
    thread::spawn(move || drop(second))
        .join()
        .assured("releasing on another thread does not panic");
    assert_eq!(notice.message(), "retained");
    assert_eq!(notice.error(), Some(scorer_evaluation()));
    // SAFETY: releasing null is allowed and does nothing.
    unsafe { nx_notice_release(ptr::null_mut()) };
}

#[test]
fn a_notice_read_refuses_a_missing_session_or_result() {
    let mut notice = ptr::null_mut();
    // SAFETY: every non-null pointer is writable; the null ones are what is refused.
    unsafe {
        assert_eq!(
            failure_kind(nx_session_next_notice(
                ptr::null(),
                ptr::null(),
                &mut notice
            )),
            FailureKind::InvalidArgument
        );
        assert!(notice.is_null(), "a refused read writes no notice");
        assert_eq!(
            failure_kind(nx_session_next_notice(
                ptr::null(),
                ptr::null(),
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
    }
}

#[test]
fn a_session_reads_notices_in_order_past_a_gap_and_from_the_next_session() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut first = server.next_exchange();

    // SAFETY: the session is live; the missing result is what is refused, before any wait.
    let refused = unsafe { nx_session_next_notice(session.0, ptr::null(), ptr::null_mut()) };
    assert_eq!(failure_kind(refused), FailureKind::InvalidArgument);

    // Nothing but its token ends a wait for a notice the server never sends: here from another
    // thread, and then by its deadline.
    let token = SharedToken(nx_cancel_new());
    let waiter = thread::spawn(move || session.notice_wait_with(token));
    thread::sleep(Duration::from_millis(100));
    // SAFETY: the token is live; it is freed once the waiter has returned.
    unsafe { nx_cancel_trigger(token.0) };
    let cancelled = waiter.join().assured("the waiting thread returns");
    // SAFETY: no call waits on the token any more.
    unsafe { nx_cancel_free(token.0) };
    assert_eq!(cancelled, FailureKind::Cancelled);
    assert_eq!(
        session.notice_wait_expiring_after(50),
        FailureKind::Deadline
    );

    // Each notice is read once, in the order the session received it.
    let text = "junction 'scorer' message error in domain 'sim': integer division by zero";
    let reported = runtime_error(text);
    let joined = unclassified(
        wire::NoticeLevel::Info,
        wire::NoticeKind::Membership,
        "node 'node-2' joined",
    );
    server.runtime.block_on(async {
        first.send(frame(&reported)).await;
        first.send(frame(&joined)).await;
    });
    let runtime = session.next_notice();
    assert_eq!(runtime.level(), NoticeLevel::Error);
    assert_eq!(runtime.kind(), NoticeKind::Runtime);
    assert_eq!(runtime.message(), reported.message);
    assert_eq!(runtime.error(), Some(scorer_evaluation()));
    let membership = session.next_notice();
    assert_eq!(membership.level(), NoticeLevel::Info);
    assert_eq!(membership.kind(), NoticeKind::Membership);
    assert_eq!(membership.message(), joined.message);
    assert_eq!(membership.error(), None);

    // One notice more than the client holds drops every notice it held: the next read reports
    // the gap, and the read after it returns the notice that followed. The attach's reply follows
    // every one of them on the exchange, so once the attach completes the client has taken them.
    let attaching = thread::spawn(move || session.execute("ATTACH DOMAIN CLOCK;"));
    server.runtime.block_on(async {
        let request = first.read_attach(&domain("sim")).await;
        for index in 0..=NOTICES_HELD {
            let dropped = unclassified(
                wire::NoticeLevel::Warning,
                wire::NoticeKind::Runtime,
                &format!("dropped notice {index}"),
            );
            first.send(frame(&dropped)).await;
        }
        let after = unclassified(
            wire::NoticeLevel::Warning,
            wire::NoticeKind::Runtime,
            "after the gap",
        );
        first.send(frame(&after)).await;
        let attached = DomainClockAttachDisposition::Attached {
            domain: domain("sim"),
            clock: stopped(),
        };
        first.reply_to_attach(request, attached).await;
    });
    assert_eq!(
        attaching.join().assured("the attaching thread returns"),
        Disposition::Completed
    );
    assert_eq!(
        session.notice_wait_expiring_after(DEADLINE_MILLIS),
        FailureKind::Overflow
    );
    assert_eq!(session.next_notice().message(), "after the gap");

    // A notice the ended session delivered and nobody read ends with it. The interruption of the
    // clock the session follows witnesses that the client saw the session end, and the clock's
    // restoration opens the next session, whose notices the stream continues with.
    let lost = unclassified(
        wire::NoticeLevel::Info,
        wire::NoticeKind::Leadership,
        "lost with its session",
    );
    server.runtime.block_on(first.send(frame(&lost)));
    drop(first);
    assert_eq!(
        session.next_clock_event().kind(),
        ClockEventKind::Interrupted
    );
    let restoring = thread::spawn(move || session.next_clock_event());
    let mut second = server.next_exchange();
    server.runtime.block_on(second.attach(stopped()));
    let restored = restoring.join().assured("the restoring thread returns");
    assert_eq!(restored.kind(), ClockEventKind::State);
    assert_eq!(
        session.notice_wait_expiring_after(50),
        FailureKind::Deadline
    );
    let kept = unclassified(
        wire::NoticeLevel::Info,
        wire::NoticeKind::ClusterParameters,
        "kept by the next session",
    );
    server.runtime.block_on(second.send(frame(&kept)));
    let next = session.next_notice();
    assert_eq!(next.kind(), NoticeKind::ClusterParameters);
    assert_eq!(next.message(), kept.message);

    // Notices read from a session stay valid after it ends.
    session.free();
    assert_eq!(runtime.error(), Some(scorer_evaluation()));
    assert_eq!(membership.message(), joined.message);
    assert_eq!(next.message(), kept.message);
}
