//! The receive window a bulk stream grants the node that answers it.
//!
//! Layer: test harness.
//!
//! - **Owns.** Assertions that a bulk response puts one whole bulk receive window in flight before
//!   its reader takes a chunk, that the stream backs that window with a `bulk_window` reservation
//!   for exactly as long as it is open, that a stream sends nothing until its window is backed,
//!   and that a transport whose budget cannot back one window refuses to bind.
//! - **Depends on.** The authenticated interconnect test fixture and the executor snapshot.
//! - **Must not know.** What a bulk response carries.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_primitives::sync::atomic::AtomicU64;
use ubyte::ByteUnit;

use super::*;

fn bulk_window_reserved(transport: &Transport) -> u64 {
    transport
        .inner
        .executor()
        .memory(MemoryClass::BulkWindow)
        .reserved_bytes
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct WindowedBulkStream;

impl InterconnectStreamRequest for WindowedBulkStream {
    const NAME: &'static str = "test_windowed_bulk_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_secs(10);
}

/// A replica fetching a guest save, or a node fetching a resource archive, reads a bulk response
/// that a loaded node otherwise moves one receive window per round trip. The sender therefore
/// has to be able to put a whole bulk receive window on the wire while the reader has taken
/// nothing yet.
#[nervix_primitives::test]
async fn a_bulk_response_puts_a_whole_bulk_receive_window_in_flight_before_its_reader_reads() {
    let window = TransportOptions::default()
        .bulk_receive_window
        .value()
        .as_u64();
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    let chunk_bytes = executor_b.limits().bulk_chunk_bytes.as_u64();
    let length = window
        .checked_mul(2)
        .assured("two fixture windows fit in u64");
    let taken = StdArc::new(AtomicU64::new(0));
    let window_taken = StdArc::new(Notify::new());
    transport_b
        .register_stream_handler::<WindowedBulkStream, _, _>({
            let taken = StdArc::clone(&taken);
            let window_taken = StdArc::clone(&window_taken);
            move |_context, _request| {
                let executor = executor_b.clone();
                let taken = StdArc::clone(&taken);
                let window_taken = StdArc::clone(&window_taken);
                async move {
                    // Each chunk is produced only when the transport asks for the next one, which
                    // it does once the previous chunk fitted the reader's window.
                    let chunks = futures_util::stream::unfold(0_u64, move |offset| {
                        let executor = executor.clone();
                        let taken = StdArc::clone(&taken);
                        let window_taken = StdArc::clone(&window_taken);
                        async move {
                            let remaining = length.checked_sub(offset)?;
                            if remaining == 0 {
                                return None;
                            }
                            let size = chunk_bytes.min(remaining);
                            let bytes = usize::try_from(size).assured("a bulk chunk fits usize");
                            let chunk = executor
                                .charge_owned(MemoryClass::Bulk, vec![7_u8; bytes])
                                .await
                                .map_err(StreamHandlerError::with_cause);
                            let previous = taken.fetch_add(size, Ordering::AcqRel);
                            let now = previous
                                .checked_add(size)
                                .assured("the fixture response length fits in u64");
                            if previous < window && now >= window {
                                window_taken.notify_one();
                            }
                            let next = offset
                                .checked_add(size)
                                .assured("the fixture response length fits in u64");
                            Some((chunk, next))
                        }
                    });
                    Ok(StreamingResponse::new(length, chunks))
                }
            }
        })
        .assured("the windowed bulk stream handler registers once");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .assured("the connected test transports become ready");

    assert_eq!(bulk_window_reserved(&transport_a), 0);
    let mut body = transport_a
        .request_stream(&node_b, WindowedBulkStream)
        .await
        .assured("the bulk stream opens");
    // The reader takes nothing until the sender has taken one whole window from its producer.
    // A sender held to a smaller window stops short of it and the stream resets after its
    // progress deadline, so the notification never comes.
    if timeout(Duration::from_secs(60), window_taken.notified())
        .await
        .is_err()
    {
        panic!(
            "the sender stopped after {} bytes, short of one {window}-byte bulk receive window",
            taken.load(Ordering::Acquire)
        );
    }
    assert_eq!(
        bulk_window_reserved(&transport_a),
        window,
        "the open stream backs exactly its one window"
    );
    let mut received = 0_u64;
    while let Some(chunk) = body
        .next_chunk()
        .await
        .assured("the reader receives the whole response")
    {
        nervix_primitives::task::consume_budget().await;
        let size = u64::try_from(chunk.len()).assured("a chunk length fits u64");
        received = received
            .checked_add(size)
            .assured("the fixture response length fits in u64");
    }
    assert_eq!(received, length);
    drop(body);
    assert_eq!(
        bulk_window_reserved(&transport_a),
        0,
        "the ended stream returned its window"
    );

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

#[derive(Debug, Archive, Serialize, Deserialize)]
struct UnbackedBulkStream;

impl InterconnectStreamRequest for UnbackedBulkStream {
    const NAME: &'static str = "test_unbacked_bulk_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Snapshot;
    const TIMEOUT: Duration = Duration::from_millis(500);
}

/// A node whose windows are all held sends no further bulk request: the request waits for a
/// returned window until its own deadline, and opens as soon as one is returned.
#[nervix_primitives::test]
async fn a_bulk_stream_sends_nothing_until_its_receive_window_is_backed() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_b,
        ..
    } = connected_transports().await;
    let answered = StdArc::new(AtomicU64::new(0));
    transport_b
        .register_stream_handler::<UnbackedBulkStream, _, _>({
            let answered = StdArc::clone(&answered);
            move |_context, _request| {
                let executor = executor_b.clone();
                let answered = StdArc::clone(&answered);
                async move {
                    answered.fetch_add(1, Ordering::AcqRel);
                    let chunk = executor
                        .charge_owned(MemoryClass::Bulk, b"window".to_vec())
                        .await
                        .map_err(StreamHandlerError::with_cause)?;
                    Ok(StreamingResponse::new(
                        6,
                        futures_util::stream::iter([Ok(chunk)]),
                    ))
                }
            }
        })
        .assured("the unbacked bulk stream handler registers once");
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .assured("the connected test transports become ready");

    // A first stream binds the bulk connection, so the request below finds a free stream at once
    // and waits for nothing but its receive window.
    read_unbacked_stream(&transport_a, &node_b).await;
    assert_eq!(answered.load(Ordering::Acquire), 1);
    assert_eq!(bulk_window_reserved(&transport_a), 0);

    let executor_a = transport_a.inner.executor().clone();
    let capacity = executor_a.memory(MemoryClass::BulkWindow).capacity_bytes;
    let every_window = executor_a
        .try_reserve(MemoryClass::BulkWindow, capacity)
        .assured("the ended stream returned the only window the transport held");
    let error = match transport_a
        .request_stream(&node_b, UnbackedBulkStream)
        .await
    {
        Ok(_) => panic!("a bulk stream opened without a receive window"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error.downcast_ref::<TransportError>(),
            Some(TransportError::RequestTimeout { .. })
        ),
        "the request waited for a window until its deadline: {error:?}"
    );
    assert_eq!(
        answered.load(Ordering::Acquire),
        1,
        "nothing reached the peer without a backed window"
    );

    drop(every_window);
    read_unbacked_stream(&transport_a, &node_b).await;
    assert_eq!(answered.load(Ordering::Acquire), 2);
    assert_eq!(bulk_window_reserved(&transport_a), 0);

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

/// Open one `UnbackedBulkStream` and read its chunk; dropping the stream returns its window.
async fn read_unbacked_stream(transport: &Transport, node: &ClusterNodeName) {
    let mut body = transport
        .request_stream(node, UnbackedBulkStream)
        .await
        .assured("a backed bulk stream opens");
    let chunk = body
        .next_chunk()
        .await
        .assured("the backed stream is readable")
        .assured("the backed stream carries its chunk");
    assert_eq!(chunk.as_ref(), b"window");
}

/// A transport whose `bulk_window` budget cannot back even one bulk receive window refuses to
/// bind, rather than leaving every bulk stream to wait for a window that never comes.
#[nervix_primitives::test]
async fn a_transport_refuses_to_bind_a_bulk_receive_window_its_budget_cannot_back() {
    let executor = Executor::default();
    let budget = executor.memory(MemoryClass::BulkWindow).capacity_bytes;
    let window = budget
        .checked_mul(2)
        .assured("twice the fixture budget fits in u64");
    let options = TransportOptions {
        bulk_receive_window: BulkReceiveWindow::resolve(Some(ByteUnit::Byte(window)))
            .assured("twice the fixture budget is a valid window"),
        ..TransportOptions::default()
    };
    let node = ClusterNodeName::parse("node-a").assured("the fixture node name is valid");
    let authority = TestCertificateAuthority::new();
    let error = match Transport::bind(
        "127.0.0.1:0"
            .parse()
            .assured("the fixture address is valid"),
        localhost_identity("test-cluster", node.clone()),
        authority.issue("test-cluster", &node),
        options,
        executor,
        test_resolver().await,
    )
    .await
    {
        Ok(_) => panic!("a transport bound a bulk receive window its budget cannot back"),
        Err(error) => error,
    };
    let Some(TransportError::BulkReceiveWindowUnbacked {
        window: refused,
        budget: available,
    }) = error.downcast_ref::<TransportError>()
    else {
        panic!("the bind refusal names the unbacked window: {error:?}");
    };
    assert_eq!(*refused, window);
    assert_eq!(*available, budget);
}
