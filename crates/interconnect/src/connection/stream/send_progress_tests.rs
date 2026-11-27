//! Streamed-response chunk writes against a real HTTP/2 flow-control window.
//!
//! Layer: test harness.
//! - **Owns.** Moving and held reader checks for the streamed-response chunk writer.
//! - **Depends on.** The chunk writer, execution reservations, HTTP/2 and permitted in-memory I/O.
//! - **Must not know.** Section, snapshot or resource contents, peer discovery or runtime graph
//!   ownership.

use std::time::Duration;

use bytes::Bytes;
use h2::{client, server};
use http::Request;
use nervix_execution::{Executor, MemoryClass};
use nervix_primitives::{
    sync::oneshot,
    task::{JoinHandle, spawn},
    time::sleep,
};
use nervix_recovery::Discarded as _;

use super::*;

/// One streamed chunk, larger than many receive windows, as a section fetch sends one.
const CHUNK_BYTES: usize = 64 * 1024;
const RECEIVE_WINDOW: u32 = 512;
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

/// A reader that keeps releasing capacity receives the whole chunk, however long it takes.
#[nervix_primitives::test]
async fn a_moving_streamed_chunk_is_sent_whole() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let reader = spawn(moving_reader(server_io));
    let (mut stream, driver) = open_stream(client_io).await;
    let result = send_stream_chunk(
        &mut stream,
        charged_chunk().await,
        std::num::NonZeroU64::MAX,
    )
    .await;
    assert!(
        result.is_ok(),
        "a chunk whose reader continually releases capacity is sent whole: {result:?}"
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

/// A reader that releases no capacity is applying backpressure, not failing: the chunk waits for
/// it, with no reset, for as long as the connection lives, and is sent whole once it reads.
#[nervix_primitives::test]
async fn a_streamed_chunk_waits_for_a_reader_that_holds_its_window_and_is_sent_whole() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let (accepted, has_accepted) = oneshot::channel();
    let (read, may_read) = oneshot::channel::<()>();
    let reader = spawn(async move {
        let mut builder = server::Builder::new();
        builder.initial_window_size(RECEIVE_WINDOW);
        let mut connection = builder.handshake::<_, Bytes>(server_io).await?;
        let (request, _respond) = connection
            .accept()
            .await
            .expect("the test opens one request")?;
        accepted
            .send(())
            .discarded("the test waits for the reader to accept before it checks the send");
        let reading = async {
            may_read
                .await
                .discarded("the test lets the reader read once it checked the held send");
            let mut body = request.into_body();
            let mut bytes = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk = chunk?;
                body.flow_control().release_capacity(chunk.len())?;
                bytes.extend_from_slice(&chunk);
            }
            Ok::<_, h2::Error>(bytes)
        };
        tokio::pin!(reading);
        nervix_primitives::select! {
            read = &mut reading => read,
            closed = futures_util::future::poll_fn(|context| connection.poll_closed(context)) => {
                closed?;
                Err(h2::Reason::CANCEL.into())
            }
        }
    });
    let (mut stream, driver) = open_stream(client_io).await;
    let sending = spawn(async move {
        let result = send_stream_chunk(
            &mut stream,
            charged_chunk().await,
            std::num::NonZeroU64::MAX,
        )
        .await;
        (result, stream)
    });
    has_accepted
        .await
        .expect("the reader accepts the stream before the test checks the send");
    // Several poll rounds pass with the window held, and the send neither fails nor completes.
    for _ in 0..64 {
        nervix_primitives::task::yield_now().await;
    }
    sleep(READ_CADENCE).await;
    assert!(
        !sending.is_finished(),
        "a chunk waits for a reader that holds its window"
    );
    read.send(())
        .discarded("the reader waits for the test to let it read");
    let (result, mut stream) = sending.await.expect("the sending task joins");
    assert!(
        result.is_ok(),
        "the held chunk is sent whole once its reader reads: {result:?}"
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
