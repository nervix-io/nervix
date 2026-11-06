//! A reply of a kind that cannot answer the request it names ends its exchange before any of its
//! effects apply and before any later frame is routed.
//!
//! Layer: test harness.
//! - **Owns.** Reply-kind violation fixtures over the production reader and request paths.
//! - **Depends on.** The production client, exchange and typed wire frames.
//! - **Must not know.** How a server serves requests, or network scheduling.

use nervix_client_wire::{
    CancelOutcome, CancelRequest, CancelState, CancellationStage, DomainClockAttachDisposition,
    DomainClockAttachOutcome, ReplyBodyKind, RequestCancelled,
};
use nervix_models::{DomainClockObservation, DomainClockObservedState};

use super::*;

/// A domain clock attachment, which the reader applies to the client's clocks when it answers an
/// attach.
fn attached_clock_reply() -> ReplyBody {
    ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
        disposition: DomainClockAttachDisposition::Attached {
            domain: domain("tenant"),
            clock: DomainClockObservation {
                generation: 1,
                state: DomainClockObservedState::Unpaced,
            },
        },
        message: String::new(),
    })
}

/// A transaction attachment, which binds its transaction when it answers an attach.
fn attached_transaction_reply() -> ReplyBody {
    ReplyBody::Attach(AttachOutcome {
        disposition: AttachDisposition::Attached(open_transaction("adopted", 1)),
        message: "attached".to_string(),
        diagnostics: Vec::new(),
    })
}

/// The outcome of a cancellation, which only a cancel request is answered with.
fn cancel_outcome_reply() -> ReplyBody {
    ReplyBody::Cancel(CancelOutcome {
        target: request_id(1),
        state: CancelState::Requested,
    })
}

fn cancelled_reply() -> ReplyBody {
    ReplyBody::Cancelled(RequestCancelled {
        stage: CancellationStage::BeforeAdmission,
    })
}

fn rejected_reply() -> ReplyBody {
    ReplyBody::Rejected(RequestRejected {
        rejection: RequestRejection::TooManyRequestsInFlight,
        field: None,
        message: "too many requests in flight".to_string(),
    })
}

fn leadership_frame() -> VerifiedFrame<ServerFrame> {
    verified(
        LeadershipObserved {
            leadership: Leadership::Unknown,
        }
        .encode(&SESSION_LIMITS)
        .assured("the event fits"),
    )
}

/// Whether the report holds the typed reply-kind violation of `request` answered by `reply`
/// beneath its current context.
fn holds_mismatch(
    report: &error_stack::Report<ClientError>,
    request: RequestKind,
    reply: ReplyBodyKind,
) -> bool {
    report.frames().any(|frame| {
        matches!(
            frame.downcast_ref::<ClientError>(),
            Some(ClientError::ReplyKindMismatch { request: actual, reply: received })
                if *actual == request && *received == reply
        )
    })
}

#[nervix_primitives::test]
async fn a_command_answered_by_a_reply_of_another_kind_interrupts_every_waiter_and_leaves_its_own_reference_uncertain()
 {
    // Each reply carries state the client applies when the reply answers its own request: a
    // listing, an opened subscription, an attached clock, a bound transaction, a cancellation.
    for body in [
        domain_list_reply(),
        opened_reply(subscription("adopted", 1)),
        attached_clock_reply(),
        attached_transaction_reply(),
        cancel_outcome_reply(),
    ] {
        let kind = ReplyBodyKind::from(&body);
        let mut loopback = Loopback::new(Some(domain("tenant")));
        let client = loopback.client.clone();
        let execution = client.prepare_execution("SHOW CLUSTER STATUS;").await;
        let expected = execution.reference().clone();
        let command =
            nervix_primitives::task::spawn(
                async move { client.execute_prepared(&execution).await },
            );
        let request = loopback.next_request().await;
        assert!(matches!(request.request, ClientRequest::Command(_)));
        let other = register(&loopback.pending, &ClientRequest::ListDomains).await;
        let events = loopback.client.inner.events.emitters.clone();
        let generation = loopback
            .client
            .inner
            .exchange
            .lock()
            .await
            .generation
            .clone();
        let reader = ExchangeReader::new(loopback.pending.clone(), events, generation);
        let frames = nervix_primitives::stream::iter(vec![
            Ok(reply_frame(request.request_id, body)),
            Ok(leadership_frame()),
        ]);
        reader.run(frames).await;

        let error = command
            .await
            .assured("the call ends")
            .expect_err("no known endpoint can recover the command");
        assert!(
            matches!(error.current_context(), ClientError::UncertainCommand { reference } if reference == &expected),
            "a {kind} reply leaves the command's own reference uncertain: {error:?}"
        );
        assert!(
            holds_mismatch(&error, RequestKind::Command, kind),
            "the typed violation stays beneath the uncertainty: {error:?}"
        );
        assert!(
            other.reply.await.is_err(),
            "the other waiter received no reply"
        );
        assert!(!loopback.pending.lock().is_open(), "the session is closed");
        assert!(
            loopback.client.leadership().is_none(),
            "the frame after the violation was never routed"
        );
        assert!(
            loopback.client.active_transaction_status().await.is_none(),
            "no transaction was adopted from a {kind} reply"
        );
        assert!(
            loopback
                .client
                .inner
                .events
                .emitters
                .clocks
                .latest(&domain("tenant"))
                .is_none(),
            "no clock was attached from a {kind} reply"
        );
    }
}

#[nervix_primitives::test]
async fn a_read_answered_by_a_command_outcome_is_interrupted_without_adopting_it() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let client = loopback.client.clone();
    let listing = nervix_primitives::task::spawn(async move { client.list_domains().await });
    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::ListDomains));
    let mut outcome = wire_outcome("command-1", completed(), "another request's outcome");
    outcome.transaction = Some(open_transaction("adopted", 1));
    let events = loopback.client.inner.events.emitters.clone();
    let generation = loopback
        .client
        .inner
        .exchange
        .lock()
        .await
        .generation
        .clone();
    let reader = ExchangeReader::new(loopback.pending.clone(), events, generation);
    reader
        .run(nervix_primitives::stream::iter(vec![Ok(reply_frame(
            request.request_id,
            ReplyBody::Command(Box::new(outcome)),
        ))]))
        .await;

    let error = listing
        .await
        .assured("the listing ends")
        .expect_err("no known endpoint can serve the listing again");
    assert!(
        matches!(
            error.current_context(),
            ClientError::RequestInterrupted {
                request: RequestKind::ListDomains
            }
        ),
        "the listing lost its session: {error:?}"
    );
    assert!(
        holds_mismatch(&error, RequestKind::ListDomains, ReplyBodyKind::Command),
        "the typed violation stays beneath the interruption: {error:?}"
    );
    assert!(
        loopback.client.active_transaction_status().await.is_none(),
        "the command outcome's transaction was not adopted"
    );
}

#[nervix_primitives::test]
async fn a_reply_of_another_kind_ends_the_exchange_before_the_reader_follows_it() {
    let mut fixture = reader_fixture(4);
    let request = register(&fixture.pending, &ClientRequest::ListDomains).await;
    let adopted = subscription("adopted", 1);

    let flow = fixture
        .reader
        .route(reply_frame(
            request.request_id,
            opened_reply(adopted.clone()),
        ))
        .await;

    assert_eq!(flow, ReaderFlow::End, "the violation ends the exchange");
    assert!(
        request.reply.await.is_err(),
        "the waiter receives no reply of another kind"
    );
    assert!(
        matches!(
            fixture.pending.lock().failure(),
            ClientError::ReplyKindMismatch {
                request: RequestKind::ListDomains,
                reply: ReplyBodyKind::Subscribe,
            }
        ),
        "the exchange keeps the typed violation as its end"
    );
    fixture
        .reader
        .route(rows_frame(adopted, &[(1, "never followed")]))
        .await;
    assert!(
        fixture.subscription_events.try_next().is_none(),
        "the reader never followed the subscription the violating reply opened"
    );
}

#[nervix_primitives::test]
async fn a_reply_no_request_waits_for_is_still_followed_whatever_its_kind() {
    let mut fixture = reader_fixture(4);
    let late = subscription("late", 1);
    // A request whose caller stopped waiting has no waiter left, so its late opening is routed as
    // before: the reader follows the subscription and its rows.
    let request = register(&fixture.pending, &command_request("command-1")).await;
    let request_id = request.request_id;
    fixture
        .pending
        .lock()
        .take(request_id)
        .assured("the request still waits");
    drop(request);

    let flow = fixture
        .reader
        .route(reply_frame(request_id, opened_reply(late.clone())))
        .await;

    assert_eq!(flow, ReaderFlow::Continue);
    assert!(
        fixture.pending.lock().is_open(),
        "a late reply breaks nothing"
    );
    fixture.reader.route(rows_frame(late, &[(1, "kept")])).await;
    assert!(
        fixture.subscription_events.try_next().is_some(),
        "the late opening is still followed"
    );
}

#[nervix_primitives::test]
async fn a_rejection_answers_any_request_and_a_cancellation_only_one_it_can_reach() {
    for body in [rejected_reply(), cancelled_reply()] {
        let mut fixture = reader_fixture(4);
        let request = register(&fixture.pending, &command_request("command-1")).await;
        let flow = fixture
            .reader
            .route(reply_frame(request.request_id, body.clone()))
            .await;
        assert_eq!(flow, ReaderFlow::Continue);
        assert_eq!(
            request.reply.await.ok(),
            Some(body),
            "the request receives its rejection or cancellation"
        );
        assert!(fixture.pending.lock().is_open());
    }

    let mut fixture = reader_fixture(4);
    let cancel = register(
        &fixture.pending,
        &ClientRequest::Cancel(CancelRequest {
            target: request_id(1),
        }),
    )
    .await;
    let flow = fixture
        .reader
        .route(reply_frame(cancel.request_id, cancelled_reply()))
        .await;
    assert_eq!(
        flow,
        ReaderFlow::End,
        "nothing cancels a cancellation, so the reply breaks the session"
    );
    assert!(cancel.reply.await.is_err());
    assert!(matches!(
        fixture.pending.lock().failure(),
        ClientError::ReplyKindMismatch {
            request: RequestKind::Cancel,
            reply: ReplyBodyKind::Cancelled,
        }
    ));
}
