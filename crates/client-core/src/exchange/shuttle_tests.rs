//! Interleavings of reply validation and the pending-request lifetime.
//!
//! Layer: test harness.
//! - **Owns.** Registration, cancellation and reader ending racing a request-registry close, and
//!   registration and cancellation racing a reply that cannot answer its command: one naming
//!   another execution, or one of another kind.
//! - **Depends on.** The production exchange reader, pending registry and typed wire frames.
//! - **Must not know.** Network scheduling, server execution or reference-count ordering.

use nervix_client_wire::{
    CommandDisposition, CommandRequest, DomainList, LeadershipObserved, OutcomeOrigin,
    ReplyBodyKind, ReplyDelivery,
};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_models::{DomainName, DomainPace, DomainStatus};

use super::*;
use crate::RequestKind;

/// A reply that cannot answer the command it names, and the ending cause it leaves the exchange.
struct Violation {
    reply: fn(RequestId) -> EncodedFrame<ServerFrame>,
    ends_with: fn(&ClientError) -> bool,
}

const FOREIGN_EXECUTION: Violation = Violation {
    reply: foreign_execution_reply,
    ends_with: is_the_foreign_execution,
};

const ANOTHER_KIND: Violation = Violation {
    reply: domain_listing_reply,
    ends_with: is_a_domain_listing,
};

#[test]
fn shuttle_a_foreign_reply_racing_registration_interrupts_every_waiter() {
    check_random_and_pct(|| {
        shuttle::future::block_on(races_registration(&FOREIGN_EXECUTION));
    });
}

#[test]
fn shuttle_a_reply_of_another_kind_racing_registration_interrupts_every_waiter() {
    check_random_and_pct(|| {
        shuttle::future::block_on(races_registration(&ANOTHER_KIND));
    });
}

/// Registration and cancellation race the reader, which reads `violation`'s reply to the command
/// and a later frame. Every waiter ends, the first cause survives, and the later frame is unread.
async fn races_registration(violation: &Violation) {
    let events = SessionEvents::new();
    let generation = events.emitters.begin_generation();
    let pending = Arc::new(SyncMutex::new(PendingReplies::new()));
    let (frames, _outbound) = mpsc::channel(1);
    let requests = Arc::new(ExchangeRequests {
        frames,
        pending: pending.clone(),
    });
    let request = ClientRequest::Command(CommandRequest {
        query: "SHOW CLUSTER STATUS;".to_string(),
        domain: None,
        execution_reference: reference("own-command"),
        expected_transaction_position: None,
        expected_preview: None,
    });
    let mut command = requests.register_for(&request).assured("an open registry");
    let command_id = command.request_id;
    let receiving = nervix_primitives::task::spawn(async move {
        assert!(
            command.receive().await.is_none(),
            "the command was interrupted"
        );
    });
    let cancelled = requests
        .register_for(&ClientRequest::ListDomains)
        .assured("an open registry");
    let cancelling = nervix_primitives::task::spawn(async move { drop(cancelled) });
    let registering = {
        let requests = requests.clone();
        nervix_primitives::task::spawn(async move {
            if let Some(mut other) = requests.register_for(&ClientRequest::ListDomains) {
                assert!(
                    other.receive().await.is_none(),
                    "every registered waiter ends"
                );
            }
        })
    };
    let reply = (violation.reply)(command_id);
    let later = LeadershipObserved {
        leadership: Leadership::Unknown,
    }
    .encode(&SESSION_LIMITS)
    .assured("the leadership frame fits");
    let reader = ExchangeReader::new(pending.clone(), events.emitters.clone(), generation);
    let reading = nervix_primitives::task::spawn(reader.run(nervix_primitives::stream::iter([
        Ok(reply.verify(&SESSION_LIMITS).assured("a valid reply")),
        Ok(later.verify(&SESSION_LIMITS).assured("a valid event")),
    ])));
    reading.await.assured("the reader ends");
    cancelling.await.assured("the cancellation ends");
    registering
        .await
        .assured("registration and interruption end");
    receiving.await.assured("the command receiver ends");
    {
        let mut pending = pending.lock();
        assert!(!pending.is_open());
        pending.close_with(Some(Status::unavailable("a later transport end")));
        assert!(
            (violation.ends_with)(&pending.failure()),
            "the first ending cause survives"
        );
    }
    assert!(
        events.leadership.borrow().is_none(),
        "the later frame was not read"
    );
    let ended = events
        .emitters
        .notices
        .next()
        .await
        .expect_err("the generation ends");
    assert!(matches!(ended.current_context(), EventQueueError::Closed));
}

fn reference(value: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(value).assured("a literal execution reference")
}

fn is_the_foreign_execution(failure: &ClientError) -> bool {
    let ClientError::ExecutionReferenceMismatch { expected, received } = failure else {
        return false;
    };
    *expected == reference("own-command") && *received == reference("foreign-command")
}

fn is_a_domain_listing(failure: &ClientError) -> bool {
    matches!(
        failure,
        ClientError::ReplyKindMismatch {
            request: RequestKind::Command,
            reply: ReplyBodyKind::DomainList,
        }
    )
}

/// A successful outcome under another execution's reference.
fn foreign_execution_reply(request_id: RequestId) -> EncodedFrame<ServerFrame> {
    reply_frame(Reply {
        request_id,
        body: ReplyBody::Command(Box::new(wire::CommandOutcome {
            execution_reference: reference("foreign-command"),
            origin: OutcomeOrigin::Executed,
            disposition: CommandDisposition::Completed {
                already_existed: false,
            },
            message: "foreign success".to_string(),
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            wasm_state: None,
            resource: None,
            backup: None,
            restore: None,
        })),
    })
}

/// A domain listing, a valid reply of another kind than a command is answered with.
fn domain_listing_reply(request_id: RequestId) -> EncodedFrame<ServerFrame> {
    reply_frame(Reply {
        request_id,
        body: ReplyBody::DomainList(DomainList {
            domains: vec![DomainInfo {
                start_version: 1,
                domain: DomainName::parse("tenant").assured("a literal domain name"),
                status: DomainStatus::Running,
                pace: DomainPace::Unpaced,
            }],
        }),
    })
}

fn reply_frame(reply: Reply) -> EncodedFrame<ServerFrame> {
    let ReplyDelivery::Frame(frame) = reply.encode(&SESSION_LIMITS).assured("the reply fits")
    else {
        panic!("a short reply needs no transfer");
    };
    frame
}

#[test]
fn shuttle_a_closed_request_registry_ends_its_silent_reader() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let events = SessionEvents::new();
            let generation = events.emitters.begin_generation();
            let pending = Arc::new(SyncMutex::new(PendingReplies::new()));
            let reader = ExchangeReader::new(pending.clone(), events.emitters.clone(), generation);
            let read =
                nervix_primitives::task::spawn(reader.run(nervix_primitives::stream::pending()));
            let waiting = nervix_primitives::task::spawn(async move {
                assert!(
                    events.emitters.subscriptions.next().await.is_err(),
                    "ending the request registry wakes a reader parked on the silent exchange"
                );
            });
            let closing = nervix_primitives::task::spawn(async move {
                pending.lock().close();
            });
            closing.await.assured("the registry closes");
            read.await
                .assured("the frame reader stops without another transport frame");
            waiting
                .await
                .assured("the subscription reader observes the ended generation");
        });
    });
}
