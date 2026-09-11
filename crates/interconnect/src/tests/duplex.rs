//! Ordering and half-close coverage for bidirectional frame streams.
//!
//! Layer: test harness.
//!
//! - **Owns.** The pipelined submission scenario for one ordered duplex stream.
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

#[tokio::test]
async fn a_duplex_stream_answers_every_frame_in_submission_order() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_duplex_handler::<CountingStream, _, _>(|_context, opening, items| async move {
            let stream = futures_util::stream::unfold(
                (opening.start, items),
                |(start, mut items)| async move {
                    let item = match items.next().await {
                        Ok(Some(item)) => item,
                        Ok(None) => return None,
                        Err(error) => return Some((Err(error), (start, items))),
                    };
                    let answered = item.value.checked_add(start)?;
                    Some((Ok(CountingAnswer { value: answered }), (start, items)))
                },
            );
            Ok(DuplexResponses::new(stream))
        })
        .assured("the fresh test transport has no duplex handler with this name");

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
