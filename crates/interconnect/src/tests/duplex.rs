//! Ordering, half-close, and sender-progress coverage for bidirectional frame streams.
//!
//! Layer: test harness.
//!
//! - **Owns.** The pipelined submission and sender-progress scenarios for one ordered duplex
//!   stream.
//! - **Depends on.** The interconnect test fixture and typed test messages.
//! - **Must not know.** Product runtime graphs or consensus behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;
use crate::answer_admission_tests::{HeldCpuClass, eventually};

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
    const SETUP_DEADLINE: crate::DuplexSetupDeadline = crate::DuplexSetupDeadline::Liveness;
}

const PIPELINED_ITEMS: u64 = 32;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct BulkOpening;

impl InterconnectDuplexRequest for BulkOpening {
    type Item = CountingItem;
    type Response = CountingAnswer;
    const NAME: &'static str = "test_bulk_opening";
    const CLASS: PoolClass = PoolClass::Bulk;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const SETUP_DEADLINE: crate::DuplexSetupDeadline = crate::DuplexSetupDeadline::Liveness;
}

#[nervix_primitives::test]
async fn a_duplex_opening_reports_decode_capacity_and_recovers_after_release() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_duplex_handler::<BulkOpening, _, _>(|_, _, _| async {
            Ok(DuplexResponses::new(futures_util::stream::empty()))
        })
        .assured("the fresh transport registers one bulk opening handler");
    let capacity = executor_b.memory(MemoryClass::Bulk).capacity_bytes;
    let held = executor_b
        .try_reserve(MemoryClass::Bulk, capacity - 4096)
        .assured("the fixture leaves the opening reader's initial allocation");
    let refused = transport_a.open_duplex_stream(&node_b, BulkOpening).await;
    drop(held);
    let recovered = transport_a.open_duplex_stream(&node_b, BulkOpening).await;
    if let Ok((_sender, mut receiver)) = recovered {
        assert!(
            receiver
                .next()
                .await
                .assured("the recovered stream completes")
                .is_none()
        );
    } else {
        panic!("the duplex stream must open once memory is released");
    }
    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
    match refused {
        Err(error) => assert!(
            matches!(
                error.downcast_ref::<TransportError>(),
                Some(TransportError::RemoteRejected { status: 503, .. })
            ),
            "opening decode pressure must be a service-unavailable response: {error:#}"
        ),
        Ok(_) => panic!("the opening reader leaves no room for its decoder"),
    }
    assert_eq!(executor_b.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

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

#[nervix_primitives::test]
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

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[nervix_primitives::test]
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

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct ReplicationShareRequest;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicationShareResponse;

impl InterconnectRequest for ReplicationShareRequest {
    type Response = ReplicationShareResponse;

    const NAME: &'static str = "test_replication_share";
    const CLASS: PoolClass = PoolClass::Replication;
}

#[nervix_primitives::test]
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
    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct AppendLikeStream;

impl InterconnectDuplexRequest for AppendLikeStream {
    type Item = CountingItem;
    type Response = CountingAnswer;

    const NAME: &'static str = "test_append_like_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Append;
    const SETUP_DEADLINE: crate::DuplexSetupDeadline = crate::DuplexSetupDeadline::Liveness;
}

/// How many frames the holding handler is given before the test reads what its class is charged.
const HELD_FRAMES: u64 = 4;

/// A follower whose Raft core is behind holds decoded batches instead of answering them, because
/// it answers a batch only once it has appended it durably. The charge for each decoded frame has
/// to stay with the frame for exactly that long: released at the decode boundary it would leave
/// the node holding batches no budget can see, and no backpressure would ever reach the leader.
#[nervix_primitives::test]
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
                nervix_primitives::task::spawn(async move {
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

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[nervix_primitives::test]
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

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// How many frames a caller abandons its waits for while they are decoded.
const ABANDONED_WAIT_FRAMES: u64 = 16;

/// A caller that selects a receive against a timer or a command abandons the receive whenever the
/// other arm wins. A receive that already took its frame off the stream waits for the frame's
/// decoding on the CPU pool, so the next receive must finish that frame instead of reading past it.
#[nervix_primitives::test]
async fn a_receive_abandoned_while_its_frame_decodes_leaves_the_frame_to_the_next_receive() {
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
    for value in 0..ABANDONED_WAIT_FRAMES {
        sender
            .send(CountingItem { value })
            .await
            .expect("every frame should be submitted");
    }
    sender
        .finish()
        .expect("the initiator should half-close its direction");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut answered = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        // Each receive is polled once and dropped unless it is ready, as a losing select arm is.
        let Some(answer) = receiver.next().now_or_never() else {
            assert!(
                Instant::now() < deadline,
                "every answer should arrive before the test deadline, and {answered:?} did"
            );
            nervix_primitives::time::sleep(Duration::from_millis(1)).await;
            continue;
        };
        let answer = answer.expect("the duplex stream should stay healthy");
        let Some(answer) = answer else {
            break;
        };
        answered.push(answer.value);
    }
    assert_eq!(
        answered,
        (0..ABANDONED_WAIT_FRAMES).collect::<Vec<_>>(),
        "abandoning a receive must lose no answer and reorder none"
    );

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct AbandoningStream;

impl InterconnectDuplexRequest for AbandoningStream {
    type Item = CountingItem;
    type Response = CountingAnswer;

    const NAME: &'static str = "test_abandoning_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SETUP_DEADLINE: crate::DuplexSetupDeadline = crate::DuplexSetupDeadline::Liveness;
}

/// The responding side reads its frames the same way: a handler selecting its next frame against a
/// heartbeat abandons that read whenever the heartbeat wins.
#[nervix_primitives::test]
async fn a_handler_read_abandoned_while_its_frame_decodes_leaves_the_frame_to_the_next_read() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_duplex_handler::<AbandoningStream, _, _>(|_context, _opening, items| async move {
            let answers = futures_util::stream::unfold(items, |mut items| async move {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    // Each read is polled once and dropped unless it is ready.
                    let Some(read) = items.next().now_or_never() else {
                        nervix_primitives::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    };
                    return match read {
                        Ok(Some(charged)) => Some((
                            Ok(CountingAnswer {
                                value: charged.item.value,
                            }),
                            items,
                        )),
                        Ok(None) => None,
                        Err(error) => Some((Err(error), items)),
                    };
                }
            });
            Ok(DuplexResponses::new(answers))
        })
        .assured("the fresh test transport has no duplex handler with this name");

    let (mut sender, mut receiver) = transport_a
        .open_duplex_stream(&node_b, AbandoningStream)
        .await
        .expect("the duplex test stream should open");
    for value in 0..ABANDONED_WAIT_FRAMES {
        sender
            .send(CountingItem { value })
            .await
            .expect("every frame should be submitted");
    }
    sender
        .finish()
        .expect("the initiator should half-close its direction");

    let mut answered = Vec::new();
    while let Some(answer) = timeout(Duration::from_secs(10), receiver.next())
        .await
        .expect("every answer should arrive before the test deadline")
        .expect("the duplex stream should stay healthy")
    {
        nervix_primitives::task::consume_budget().await;
        answered.push(answer.value);
    }
    assert_eq!(
        answered,
        (0..ABANDONED_WAIT_FRAMES).collect::<Vec<_>>(),
        "abandoning a handler's read must lose no frame and reorder none"
    );

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// A stream whose handler answers each frame only once the test opens its gate, so a test can fill
/// a class between a frame's decoding and its answer's encoding.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct GatedStream;

impl InterconnectDuplexRequest for GatedStream {
    type Item = CountingItem;
    type Response = CountingAnswer;

    const NAME: &'static str = "test_gated_stream";
    const CLASS: PoolClass = PoolClass::Replication;
    const SETUP_DEADLINE: crate::DuplexSetupDeadline = crate::DuplexSetupDeadline::Liveness;
}

/// What a test holds of the one gated stream it opens: the value of each frame its handler
/// decoded, and the gate that lets the handler answer the next one.
struct Gate {
    decoded: mpsc::Receiver<u64>,
    answer: mpsc::Sender<()>,
}

/// Answer each frame with its own value once the test opens the gate.
fn register_gated_handler(transport: &Transport) -> Gate {
    let (decoded_sender, decoded) = mpsc::channel::<u64>(1);
    let (answer, gate) = mpsc::channel::<()>(1);
    let gate = StdArc::new(nervix_primitives::sync::blocking::Mutex::new(Some(gate)));
    transport
        .register_duplex_handler::<GatedStream, _, _>(move |_context, _opening, items| {
            let decoded = decoded_sender.clone();
            let gate = gate.lock().take();
            async move {
                let Some(gate) = gate else {
                    return Err(Report::new(StreamHandlerError::new(
                        "the test opens one gated stream".to_string(),
                    )));
                };
                let stream = futures_util::stream::unfold(
                    (items, decoded, gate),
                    |(mut items, decoded, mut gate)| async move {
                        let charged = match items.next().await {
                            Ok(Some(charged)) => charged,
                            Ok(None) => return None,
                            Err(error) => return Some((Err(error), (items, decoded, gate))),
                        };
                        let value = charged.item.value;
                        drop(charged);
                        if decoded.send(value).await.is_err() {
                            return None;
                        }
                        gate.recv().await?;
                        Some((Ok(CountingAnswer { value }), (items, decoded, gate)))
                    },
                );
                Ok(DuplexResponses::new(stream))
            }
        })
        .assured("the fresh test transport has no gated stream handler");
    Gate { decoded, answer }
}

/// Open a gated stream, submit one frame and wait until the responder has decoded it.
async fn decoded_gated_frame(
    transport: &Transport,
    node: &ClusterNodeName,
    gate: &mut Gate,
) -> (DuplexSender<GatedStream>, DuplexReceiver<GatedStream>) {
    let (mut sender, receiver) = transport
        .open_duplex_stream(node, GatedStream)
        .await
        .expect("the gated stream opens");
    sender
        .send(CountingItem { value: 7 })
        .await
        .expect("the frame leaves the initiator");
    let decoded = timeout(Duration::from_secs(10), gate.decoded.recv())
        .await
        .expect("the responder decodes the frame before the test deadline")
        .expect("the gated handler reports the frame it decoded");
    assert_eq!(decoded, 7);
    (sender, receiver)
}

/// An answer carries work its responder already did, so a responder whose class is full holds the
/// answer's encoding back, with its own charge, instead of refusing it and failing the stream. The
/// answer arrives once the class frees.
#[nervix_primitives::test]
async fn an_answer_waits_for_a_place_on_its_responders_full_class_and_arrives_once_one_frees() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    let mut gate = register_gated_handler(&transport_b);
    let (_sender, mut receiver) = decoded_gated_frame(&transport_a, &node_b, &mut gate).await;

    let held = HeldCpuClass::hold(&executor_b, CpuClass::Control).await;
    let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
    let reserved = executor_b.memory(MemoryClass::Commands).reserved_bytes;
    gate.answer
        .send(())
        .await
        .expect("the handler waits for its gate");
    eventually(
        &executor_b,
        "the answer waiting with its charge",
        |executor| {
            executor.memory(MemoryClass::Commands).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
        },
    )
    .await;
    assert_eq!(
        executor_b.snapshot().cpu_class(CpuClass::Control).refused,
        refused,
        "a full class refuses no answer"
    );

    held.release().await;
    let answer = timeout(Duration::from_secs(10), receiver.next())
        .await
        .expect("the answer arrives before the test deadline")
        .expect("the stream stays healthy while its answer waits")
        .expect("the frame is answered");
    assert_eq!(answer, CountingAnswer { value: 7 });

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// Once the initiator resets the stream no answer can reach it, so the reset ends an answer's wait
/// for a place on its responder's full class: the waiting encoding is dropped with its charge, and
/// the class refuses nothing.
#[nervix_primitives::test]
async fn an_initiators_reset_ends_an_answers_wait_for_a_place_on_its_responders_full_class() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    let mut gate = register_gated_handler(&transport_b);
    let (sender, receiver) = decoded_gated_frame(&transport_a, &node_b, &mut gate).await;

    let held = HeldCpuClass::hold(&executor_b, CpuClass::Control).await;
    let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
    let reserved = executor_b.memory(MemoryClass::Commands).reserved_bytes;
    gate.answer
        .send(())
        .await
        .expect("the handler waits for its gate");
    eventually(
        &executor_b,
        "the answer waiting with its charge",
        |executor| {
            executor.memory(MemoryClass::Commands).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
        },
    )
    .await;

    drop(sender);
    drop(receiver);
    eventually(
        &executor_b,
        "the reset ending the answer's wait and returning its charge",
        |executor| executor.memory(MemoryClass::Commands).reserved_bytes < reserved,
    )
    .await;
    assert_eq!(
        executor_b.snapshot().cpu_class(CpuClass::Control).refused,
        refused,
        "a full class refuses no answer"
    );

    held.release().await;
    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// An answer that reaches an initiator whose class is full waits for a place to be decoded,
/// holding its charge, instead of failing the stream, and is received once the class frees.
/// Dropping the receiver instead ends that wait and returns the charge.
#[nervix_primitives::test]
async fn an_answer_waits_for_a_place_on_its_initiators_full_class() {
    for abandon in [false, true] {
        let ConnectedTransports {
            transport_a,
            transport_b,
            node_b,
            ..
        } = connected_transports().await;
        let executor_a = transport_a.inner.executor().clone();
        let mut gate = register_gated_handler(&transport_b);
        let (sender, mut receiver) = decoded_gated_frame(&transport_a, &node_b, &mut gate).await;
        let held = HeldCpuClass::hold(&executor_a, CpuClass::Control).await;
        let refused = executor_a.snapshot().cpu_class(CpuClass::Control).refused;
        let reserved = executor_a.memory(MemoryClass::Commands).reserved_bytes;
        let receiving = nervix_primitives::task::spawn(async move {
            let answer = receiver.next().await;
            (answer, receiver)
        });
        gate.answer
            .send(())
            .await
            .expect("the handler waits for its gate");
        eventually(
            &executor_a,
            "the answer's decoding waiting with its charge",
            |executor| {
                executor.memory(MemoryClass::Commands).reserved_bytes > reserved
                    || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
            },
        )
        .await;
        assert_eq!(
            executor_a.snapshot().cpu_class(CpuClass::Control).refused,
            refused,
            "a full class refuses no answer"
        );
        if abandon {
            receiving.abort();
            drop(sender);
            eventually(
                &executor_a,
                "the dropped receiver returning its decoding's charge",
                |executor| executor.memory(MemoryClass::Commands).reserved_bytes <= reserved,
            )
            .await;
            held.release().await;
        } else {
            held.release().await;
            let (answer, _receiver) = timeout(Duration::from_secs(10), receiving)
                .await
                .expect("the answer is received before the test deadline")
                .expect("the receiving task is joined");
            let answer = answer
                .expect("the stream stays healthy while its answer waits")
                .expect("the frame is answered");
            assert_eq!(answer, CountingAnswer { value: 7 });
            drop(sender);
        }
        transport_a.shutdown(crate::test_teardown_ceiling()).await;
        transport_b.shutdown(crate::test_teardown_ceiling()).await;
    }
}

/// A frame a stream's initiator submits waits for a place on its responder's full class instead
/// of being refused and failing the stream, as a consumer stream's heartbeats and settlements and
/// an append stream's batches do, and it is answered once the class frees.
#[nervix_primitives::test]
async fn a_submitted_frame_waits_for_a_place_on_its_responders_full_class() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    register_counting_handler(&transport_b);
    let (mut sender, mut receiver) = transport_a
        .open_duplex_stream(&node_b, CountingStream { start: 0 })
        .await
        .expect("the counting stream opens");

    let held = HeldCpuClass::hold(&executor_b, CpuClass::Control).await;
    let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
    let reserved = executor_b.memory(MemoryClass::Commands).reserved_bytes;
    sender
        .send(CountingItem { value: 5 })
        .await
        .expect("the frame leaves the initiator");
    eventually(
        &executor_b,
        "the frame's decoding waiting with its charge",
        |executor| {
            executor.memory(MemoryClass::Commands).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
        },
    )
    .await;
    assert_eq!(
        executor_b.snapshot().cpu_class(CpuClass::Control).refused,
        refused,
        "a full class refuses no submitted frame"
    );

    held.release().await;
    let answer = timeout(Duration::from_secs(10), receiver.next())
        .await
        .expect("the answer arrives before the test deadline")
        .expect("the stream stays healthy while its frame waits")
        .expect("the frame is answered");
    assert_eq!(answer, CountingAnswer { value: 5 });

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// The initiator holds a frame's encoding back on its own full class instead of failing the send,
/// and the frame leaves and is answered once the class frees.
#[nervix_primitives::test]
async fn a_submitted_frame_waits_for_a_place_on_its_initiators_full_class() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    let executor_a = transport_a.inner.executor().clone();
    register_counting_handler(&transport_b);
    let (mut sender, mut receiver) = transport_a
        .open_duplex_stream(&node_b, CountingStream { start: 0 })
        .await
        .expect("the counting stream opens");

    let held = HeldCpuClass::hold(&executor_a, CpuClass::Control).await;
    let refused = executor_a.snapshot().cpu_class(CpuClass::Control).refused;
    let reserved = executor_a.memory(MemoryClass::Commands).reserved_bytes;
    let sending = nervix_primitives::task::spawn(async move {
        let sent = sender.send(CountingItem { value: 6 }).await;
        (sent, sender)
    });
    eventually(
        &executor_a,
        "the frame's encoding waiting with its charge",
        |executor| {
            executor.memory(MemoryClass::Commands).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
        },
    )
    .await;
    assert_eq!(
        executor_a.snapshot().cpu_class(CpuClass::Control).refused,
        refused,
        "a full class refuses no submitted frame"
    );

    held.release().await;
    let (sent, _sender) = timeout(Duration::from_secs(10), sending)
        .await
        .expect("the frame leaves before the test deadline")
        .expect("the sending task is joined");
    sent.expect("the frame is encoded once the class frees");
    let answer = timeout(Duration::from_secs(10), receiver.next())
        .await
        .expect("the answer arrives before the test deadline")
        .expect("the stream stays healthy while its frame waits")
        .expect("the frame is answered");
    assert_eq!(answer, CountingAnswer { value: 6 });

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}
