//! Interleavings of command correlation and the pending-request lifetime.
//!
//! Layer: test harness.
//! - **Owns.** Registration, cancellation and reader ending racing a request-registry close.
//! - **Depends on.** The production exchange reader, pending registry and typed wire frames.
//! - **Must not know.** Network scheduling, server execution or reference-count ordering.

use nervix_client_wire::{
    CommandDisposition, CommandRequest, LeadershipObserved, OutcomeOrigin, ReplyDelivery,
};
use nervix_model_harness::shuttle::check_random_and_pct;

use super::*;

#[test]
fn shuttle_a_foreign_reply_racing_registration_interrupts_every_waiter() {
    check_random_and_pct(|| {
        shuttle::future::block_on(async {
            let events = SessionEvents::new();
            let generation = events.emitters.begin_generation();
            let pending = Arc::new(SyncMutex::new(PendingReplies::new()));
            let (frames, _outbound) = mpsc::channel(1);
            let requests = Arc::new(ExchangeRequests {
                frames,
                pending: pending.clone(),
            });
            let expected = reference("own-command");
            let foreign = reference("foreign-command");
            let request = ClientRequest::Command(CommandRequest {
                query: "SHOW CLUSTER STATUS;".to_string(),
                domain: None,
                execution_reference: expected.clone(),
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
            let cancelled = requests.register().assured("an open registry");
            let cancelling = nervix_primitives::task::spawn(async move { drop(cancelled) });
            let registering = {
                let requests = requests.clone();
                nervix_primitives::task::spawn(async move {
                    if let Some(mut other) = requests.register() {
                        assert!(
                            other.receive().await.is_none(),
                            "every registered waiter ends"
                        );
                    }
                })
            };
            let reply = foreign_reply(command_id, foreign.clone());
            let later = LeadershipObserved {
                leadership: Leadership::Unknown,
            }
            .encode(&SESSION_LIMITS)
            .assured("the leadership frame fits");
            let reader = ExchangeReader::new(pending.clone(), events.emitters.clone(), generation);
            let reading =
                nervix_primitives::task::spawn(reader.run(nervix_primitives::stream::iter([
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
                    matches!(pending.failure(), ClientError::ExecutionReferenceMismatch {
                    expected: actual, received
                } if actual == expected && received == foreign),
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
        });
    });
}

fn reference(value: &str) -> CommandExecutionReference {
    CommandExecutionReference::parse(value).assured("a literal execution reference")
}

fn foreign_reply(
    request_id: RequestId,
    execution_reference: CommandExecutionReference,
) -> EncodedFrame<ServerFrame> {
    let reply = Reply {
        request_id,
        body: ReplyBody::Command(Box::new(wire::CommandOutcome {
            execution_reference,
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
    };
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
