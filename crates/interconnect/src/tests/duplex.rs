//! Ordering, half-close, and sender-progress coverage for bidirectional frame streams.
//!
//! Layer: test harness.
//!
//! - **Owns.** The pipelined submission and sender-progress scenarios for one ordered duplex
//!   stream.
//! - **Depends on.** The interconnect test fixture and typed test messages.
//! - **Must not know.** Product runtime graphs or consensus behavior.

use super::*;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct CountingStream {
    start: u64,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct CountingItem {
    value: u64,
}

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct CountingAnswer {
    value: u64,
}

impl InterconnectDuplexRequest for CountingStream {
    type Item = CountingItem;
    type Response = CountingAnswer;

    const NAME: &'static str = "test_counting_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
}

const PIPELINED_ITEMS: u64 = 32;

/// Answer every frame with its value plus the stream's starting offset, in arrival order.
fn register_counting_handler(transport: &Transport) {
    transport
        .register_duplex_handler::<CountingStream, _, _>(|_context, opening, items| async move {
            let stream = futures_util::stream::unfold(
                (opening.start, items),
                |(start, mut items)| async move {
                    let charged = match items.next().await {
                        Ok(Some(charged)) => charged,
                        Ok(None) => return None,
                        Err(error) => return Some((Err(error), (start, items))),
                    };
                    let answered = charged.item.value.checked_add(start)?;
                    Some((Ok(CountingAnswer { value: answered }), (start, items)))
                },
            );
            Ok(DuplexResponses::new(stream))
        })
        .assured("the fresh test transport has no duplex handler with this name");
}

#[tokio::test]
async fn a_duplex_stream_answers_every_frame_in_submission_order() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    register_counting_handler(&transport_b);

    let (mut sender, mut receiver) = transport_a
        .open_duplex_stream(&node_b, CountingStream { start: 100 })
        .await
        .expect("the duplex test stream should open");

    for value in 0..PIPELINED_ITEMS {
        let bytes = sender
            .send(CountingItem { value })
            .await
            .expect("every pipelined frame should be submitted");
        assert!(bytes > 0, "a submitted frame reports the bytes it carried");
    }
    sender
        .finish()
        .expect("the initiator should half-close its direction");

    for value in 0..PIPELINED_ITEMS {
        let answer = timeout(Duration::from_secs(10), receiver.next())
            .await
            .expect("the answer should arrive before the test deadline")
            .expect("the duplex stream should stay healthy")
            .expect("every submitted frame should be answered");
        assert_eq!(
            answer,
            CountingAnswer {
                value: value
                    .checked_add(100)
                    .assured("the test submits a bounded number of frames")
            },
            "answers must arrive in submission order"
        );
    }
    assert!(
        timeout(Duration::from_secs(10), receiver.next())
            .await
            .expect("the half-close should arrive before the test deadline")
            .expect("the duplex stream should stay healthy")
            .is_none(),
        "the responder ends its direction once the initiator half-closes"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn a_duplex_sender_reports_when_its_peer_last_accepted_its_bytes() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    register_counting_handler(&transport_b);

    let (mut sender, mut receiver) = transport_a
        .open_duplex_stream(&node_b, CountingStream { start: 0 })
        .await
        .expect("the duplex test stream should open");
    let progress = sender.progress();

    let before_send = Instant::now();
    sender
        .send(CountingItem { value: 7 })
        .await
        .expect("the frame should be submitted");
    let accepted_at = progress.last_accepted_at();
    assert!(
        accepted_at >= before_send,
        "a frame the peer accepted must move the sender's progress past the moment it was sent"
    );

    let answer = timeout(Duration::from_secs(10), receiver.next())
        .await
        .expect("the answer should arrive before the test deadline")
        .expect("the duplex stream should stay healthy")
        .expect("the submitted frame should be answered");
    assert_eq!(
        answer,
        CountingAnswer { value: 7 },
        "the peer answers the frame it accepted"
    );
    assert_eq!(
        progress.last_accepted_at(),
        accepted_at,
        "answers arriving from the peer must not count as the peer accepting the sender's bytes"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct ReplicationShareRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicationShareResponse;

impl InterconnectRequest for ReplicationShareRequest {
    type Response = ReplicationShareResponse;

    const NAME: &'static str = "test_replication_share";
    const CLASS: PoolClass = PoolClass::Replication;
    const TIMEOUT: Duration = Duration::from_secs(5);
}

#[tokio::test]
async fn an_open_append_stream_cannot_consume_shared_replication_streams() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    // The handler never answers, so the stream this opens stays open for the whole test.
    transport_b
        .register_duplex_handler::<AppendLikeStream, _, _>(|_context, _opening, items| async move {
            let held = futures_util::stream::unfold(items, |mut items| async move {
                match items.next().await {
                    Ok(Some(_)) | Ok(None) => None,
                    Err(error) => Some((Err(error), items)),
                }
            });
            Ok(DuplexResponses::new(held))
        })
        .assured("the fresh test transport has no append handler with this name");
    transport_b
        .register_handler::<ReplicationShareRequest, _, _>(|_context, _request| async move {
            ReplicationShareResponse
        })
        .assured("the fresh test transport has no shared replication handler with this name");

    let held = transport_a
        .open_duplex_stream(&node_b, AppendLikeStream)
        .await
        .expect("the reserved append stream should open");

    assert_eq!(
        timeout(
            Duration::from_secs(10),
            transport_a.request(&node_b, ReplicationShareRequest),
        )
        .await
        .expect("a shared replication request must not wait behind the append stream")
        .expect("the shared replication request should succeed"),
        ReplicationShareResponse
    );

    drop(held);
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct AppendLikeStream;

impl InterconnectDuplexRequest for AppendLikeStream {
    type Item = CountingItem;
    type Response = CountingAnswer;

    const NAME: &'static str = "test_append_like_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Append;
    const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
}

/// How many frames the holding handler is given before the test reads what its class is charged.
const HELD_FRAMES: u64 = 4;

/// A follower whose Raft core is behind holds decoded batches instead of answering them, because
/// it answers a batch only once it has appended it durably. The charge for each decoded frame has
/// to stay with the frame for exactly that long: released at the decode boundary it would leave
/// the node holding batches no budget can see, and no backpressure would ever reach the leader.
#[tokio::test]
async fn frames_a_handler_holds_stay_charged_to_its_class_until_it_drops_them() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    let (held_tx, mut held_rx) = mpsc::unbounded_channel();
    transport_b
        .register_duplex_handler::<AppendLikeStream, _, _>(move |_context, _opening, items| {
            let held = held_tx.clone();
            async move {
                // Decode every frame and hold it, answering none, as a follower does while its
                // core has not caught up with what the leader has already sent.
                tokio::spawn(async move {
                    let mut items = items;
                    while let Ok(Some(charged)) = items.next().await {
                        if held.send(charged).is_err() {
                            break;
                        }
                    }
                });
                Ok(DuplexResponses::new(futures_util::stream::pending()))
            }
        })
        .assured("the fresh test transport has no append handler with this name");

    let (mut sender, _receiver) = transport_a
        .open_duplex_stream(&node_b, AppendLikeStream)
        .await
        .expect("the append-like stream should open");
    let idle = executor_b.snapshot().commands_memory.reserved_bytes;

    for value in 0..HELD_FRAMES {
        sender
            .send(CountingItem { value })
            .await
            .expect("the responder keeps accepting frames while it holds the earlier ones");
    }
    let mut held = Vec::new();
    for _ in 0..HELD_FRAMES {
        let charged = timeout(Duration::from_secs(10), held_rx.recv())
            .await
            .expect("every frame should be decoded before the test deadline")
            .expect("the holding handler should still be running");
        held.push(charged);
    }

    let charged = executor_b.snapshot().commands_memory.reserved_bytes;
    assert!(
        charged > idle,
        "frames the handler still holds must stay charged to its class: it held {idle} bytes \
         before they arrived and {charged} while holding {HELD_FRAMES} of them"
    );

    drop(held);
    let released = executor_b.snapshot().commands_memory.reserved_bytes;
    assert!(
        released < charged,
        "dropping the decoded frames must return their charge: the class held {charged} bytes \
         while they were held and {released} after they were dropped"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn opening_a_duplex_stream_without_a_handler_fails() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;

    let opened = transport_a
        .open_duplex_stream(&node_b, CountingStream { start: 0 })
        .await;
    assert!(
        opened.is_err(),
        "a peer with no registered duplex handler must refuse the stream"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
