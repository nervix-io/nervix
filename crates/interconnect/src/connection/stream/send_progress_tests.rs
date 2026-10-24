//! Streamed-response chunk writes against a real HTTP/2 flow-control window.
//!
//! Layer: test harness.
//! - **Owns.** Moving and stalled reader checks for the streamed-response chunk writer.
//! - **Depends on.** The chunk writer, execution reservations, HTTP/2 and permitted in-memory I/O.
//! - **Must not know.** Section, snapshot or resource contents, peer discovery or runtime graph
//!   ownership.

use bytes::Bytes;
use h2::{client, server};
use http::Request;
use nervix_execution::{Executor, MemoryClass};
use nervix_primitives::{
    sync::oneshot,
    task::{JoinHandle, spawn},
    time::{Instant, sleep},
};
use nervix_recovery::Discarded as _;

use super::*;

/// One streamed chunk, larger than many receive windows, as a section fetch sends one.
const CHUNK_BYTES: usize = 64 * 1024;
const RECEIVE_WINDOW: u32 = 512;
const PROGRESS_BOUND: Duration = Duration::from_secs(1);
const READ_CADENCE: Duration = Duration::from_millis(20);

/// Poll the connection while its body is read, releasing a small window at a steady cadence.
async fn moving_reader(io: tokio::io::DuplexStream) -> Result<Vec<u8>, h2::Error> {
    let mut builder = server::Builder::new();
    builder.initial_window_size(RECEIVE_WINDOW);
    let mut connection = builder.handshake::<_, Bytes>(io).await?;
    let (request, _respond) = connection
        .accept()
        .await
        .expect("the test opens one request")?;
    let read = async {
        let mut body = request.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            sleep(READ_CADENCE).await;
            body.flow_control().release_capacity(chunk.len())?;
            bytes.extend_from_slice(&chunk);
        }
        Ok::<_, h2::Error>(bytes)
    };
    tokio::pin!(read);
    nervix_primitives::select! {
        read = &mut read => read,
        closed = futures_util::future::poll_fn(|context| connection.poll_closed(context)) => {
            closed?;
            Err(h2::Reason::CANCEL.into())
        }
    }
}

/// A test client's one open stream toward `io`, and the task that drives its connection.
async fn open_stream(
    io: tokio::io::DuplexStream,
) -> (SendStream<Bytes>, JoinHandle<Result<(), h2::Error>>) {
    let (mut client, connection) = client::handshake(io)
        .await
        .expect("the test HTTP/2 client handshakes");
    let driver = spawn(connection);
    client = client.ready().await.expect("the test client is ready");
    let request = Request::builder()
        .uri("https://localhost/stream")
        .body(())
        .expect("the test request is valid");
    let (_response, stream) = client
        .send_request(request, false)
        .expect("the test opens one stream");
    (stream, driver)
}

/// A chunk of `CHUNK_BYTES` charged to the bulk class.
async fn charged_chunk() -> ChargedBytes {
    let executor = Executor::default();
    let reservation = executor
        .reserve(
            MemoryClass::Bulk,
            u64::try_from(CHUNK_BYTES).expect("the bounded chunk fits in u64"),
        )
        .await
        .expect("the test chunk fits its memory class");
    ChargedBytes::from_owned(vec![0x5a; CHUNK_BYTES], reservation)
}

/// A reader that keeps releasing capacity keeps the stream, however long the chunk takes as a
/// whole: the progress bound measures the time between grants, not the chunk.
#[nervix_primitives::test]
async fn a_moving_streamed_chunk_outlasts_one_progress_bound() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let reader = spawn(moving_reader(server_io));
    let (mut stream, driver) = open_stream(client_io).await;
    let started = Instant::now();
    let result = send_stream_chunk(&mut stream, charged_chunk().await, PROGRESS_BOUND).await;
    assert!(
        result.is_ok(),
        "a chunk whose reader continually releases capacity must outlast one progress bound: \
         {result:?}"
    );
    assert!(
        started.elapsed() >= 2 * PROGRESS_BOUND,
        "the moving chunk must span multiple progress intervals"
    );
    stream
        .send_data(Bytes::new(), true)
        .expect("the completed stream half-closes");
    let observed = reader
        .await
        .expect("the reader task joins")
        .expect("the reader receives the complete chunk");
    assert_eq!(
        observed,
        vec![0x5a; CHUNK_BYTES],
        "the complete chunk is preserved"
    );
    driver.abort();
    driver
        .await
        .discarded("the aborted connection driver ends with the test");
}

/// A reader that releases no capacity resets the stream at the unchanged progress bound.
#[nervix_primitives::test]
async fn a_streamed_chunk_without_released_capacity_reaches_its_progress_bound() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let (release, released) = oneshot::channel();
    let receiver = spawn(async move {
        let mut builder = server::Builder::new();
        builder.initial_window_size(RECEIVE_WINDOW);
        let mut connection = builder
            .handshake::<_, Bytes>(server_io)
            .await
            .expect("the test server handshakes");
        let (_request, _respond) = connection
            .accept()
            .await
            .expect("the test opens one request")
            .expect("the test request is valid");
        nervix_primitives::select! {
            _ = released => {}
            _ = futures_util::future::poll_fn(|context| connection.poll_closed(context)) => {}
        }
    });
    let (mut stream, driver) = open_stream(client_io).await;
    let result = send_stream_chunk(&mut stream, charged_chunk().await, PROGRESS_BOUND).await;
    assert!(
        matches!(result, Err(ref error) if matches!(error.current_context(), TransportError::ProgressTimeout { timeout } if *timeout == PROGRESS_BOUND)),
        "a reader that releases no capacity must reach the unchanged progress bound: {result:?}"
    );
    release
        .send(())
        .discarded("the silent receiver may already have seen its connection close");
    receiver.await.expect("the silent receiver joins");
    driver.abort();
    driver
        .await
        .discarded("the aborted connection driver ends with the test");
}
