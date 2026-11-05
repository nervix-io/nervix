//! Foreign command references terminate the reader before any later frame can be routed.
//!
//! Layer: test harness.
//! - **Owns.** Foreign-outcome and reader-abortion regression fixtures.
//! - **Depends on.** The production client, exchange and typed wire frames.
//! - **Must not know.** Server command execution or network scheduling.

use super::*;

#[nervix_primitives::test]
async fn a_foreign_command_reply_interrupts_every_waiter_and_leaves_its_own_reference_uncertain() {
    for disposition in [
        completed(),
        CommandDisposition::Failed,
        CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
        CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
        CommandDisposition::TransactionDetached {
            transaction_id: "foreign".to_string(),
        },
    ] {
        let mut loopback = Loopback::new(Some(domain("tenant")));
        let client = loopback.client.clone();
        let execution = client.prepare_execution("SHOW CLUSTER STATUS;").await;
        let expected = execution.reference().clone();
        let command =
            nervix_primitives::task::spawn(
                async move { client.execute_prepared(&execution).await },
            );
        let request = loopback.next_request().await;
        let other = register(&loopback.pending).await;
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
        let mut foreign = wire_outcome("command-1", disposition, "foreign outcome");
        foreign.transaction = Some(open_transaction("foreign", 1));
        let reply = Reply {
            request_id: request.request_id,
            body: ReplyBody::Command(Box::new(foreign)),
        };
        let ReplyDelivery::Frame(frame) =
            reply.encode(&SESSION_LIMITS).assured("a short reply fits")
        else {
            panic!("a short reply requires no transfer");
        };
        let later = LeadershipObserved {
            leadership: Leadership::Unknown,
        }
        .encode(&SESSION_LIMITS)
        .assured("the event fits");
        let frames = nervix_primitives::stream::iter(vec![
            Ok(frame.verify(&SESSION_LIMITS).assured("the reply verifies")),
            Ok(later
                .verify(&SESSION_LIMITS)
                .assured("the later frame verifies")),
        ]);
        reader.run(frames).await;
        let error = command
            .await
            .assured("the call ends")
            .expect_err("no known endpoint can recover it");
        assert!(
            matches!(error.current_context(), ClientError::UncertainCommand { reference } if reference == &expected),
            "the own reference is uncertain: {error:?}"
        );
        assert!(
            error.frames().any(|frame| matches!(
                frame.downcast_ref::<ClientError>(),
                Some(ClientError::ExecutionReferenceMismatch { expected: actual, received })
                    if actual == &expected && received.as_str() == "command-1"
            )),
            "the typed mismatch stays beneath the interruption: {error:?}"
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
            "the foreign transaction was not adopted"
        );
    }
}

#[nervix_primitives::test]
async fn an_aborted_reader_interrupts_its_waiters_and_ends_its_generation() {
    use std::{future::Future as _, task::Poll};

    use nervix_primitives::sync::oneshot::error::TryRecvError;

    let fixture = reader_fixture(4);
    let mut waiting = register(&fixture.pending).await;
    let task =
        nervix_primitives::task::spawn(fixture.reader.run(nervix_primitives::stream::pending()));
    task.abort();
    task.await.expect_err("the reader was aborted");
    assert!(
        matches!(waiting.reply.try_recv(), Err(TryRecvError::Closed)),
        "an aborted reader cannot leave a request waiting for a reply"
    );
    let mut next = std::pin::pin!(fixture.notices.next());
    let ended = std::future::poll_fn(|context| Poll::Ready(next.as_mut().poll(context))).await;
    let Poll::Ready(Err(report)) = ended else {
        panic!("the generation must end after the aborted reader stops");
    };
    assert!(matches!(report.current_context(), EventQueueError::Closed));
}

#[nervix_primitives::test(flavor = "current_thread")]
async fn a_cancelled_interruption_keeps_cleanup_owned_until_close_finishes() {
    use std::{future::Future as _, task::Poll};

    let events = SessionEvents::new();
    let (frames, _outbound) = mpsc::channel(1);
    let pending = Arc::new(Mutex::new(PendingReplies::new()));
    let mut exchange = detached_exchange(frames, pending, events.emitters.clone());
    let mut interrupt = Box::pin(exchange.interrupt(crate::exchange::ExchangeEnd::Closed));
    let waiting =
        std::future::poll_fn(|context| Poll::Ready(interrupt.as_mut().poll(context))).await;
    assert!(
        waiting.is_pending(),
        "the reader has not yet run its cleanup"
    );
    drop(interrupt);

    exchange.close().await;
    let mut next = std::pin::pin!(events.emitters.notices.next());
    let ended = std::future::poll_fn(|context| Poll::Ready(next.as_mut().poll(context))).await;
    let Poll::Ready(Err(report)) = ended else {
        panic!("close must finish the interrupted reader's generation cleanup");
    };
    assert!(matches!(report.current_context(), EventQueueError::Closed));
}
