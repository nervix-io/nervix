//! Frame-write liveness against a real HTTP/2 flow-control window.
//!
//! Layer: test harness.
//! - **Owns.** Moving and stalled receiver checks for the duplex frame writer.
//! - **Depends on.** The writer, execution reservations, HTTP/2 and permitted in-memory I/O.
//! - **Must not know.** Raft commands, peer discovery or runtime graph ownership.

use bytes::Bytes;
use h2::{client, server};
use http::Request;
use nervix_execution::{Executor, MemoryClass};
use nervix_primitives::{
    sync::oneshot,
    task::spawn,
    time::{Instant, sleep},
};

use super::*;

const PAYLOAD_BYTES: usize = 64 * 1024;
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

#[nervix_primitives::test]
async fn a_moving_duplex_frame_outlasts_one_write_progress_bound() {
    let (client_io, server_io) = tokio::io::duplex(1024);
    let reader = spawn(moving_reader(server_io));
    let (mut client, connection) = client::handshake(client_io)
        .await
        .expect("the test HTTP/2 client handshakes");
    let driver = spawn(connection);
    client = client.ready().await.expect("the test client is ready");
    let request = Request::builder()
        .uri("https://localhost/frame")
        .body(())
        .expect("the test request is valid");
    let (_response, stream) = client
        .send_request(request, false)
        .expect("the test opens one frame stream");
    let executor = Executor::default();
    let reservation = executor
        .reserve(
            MemoryClass::Commands,
            u64::try_from(PAYLOAD_BYTES).expect("the bounded payload fits in u64"),
        )
        .await
        .expect("the test payload fits its memory class");
    let mut writer = FrameWriter::new(stream, PROGRESS_BOUND);
    let started = Instant::now();
    let result = writer
        .send_frame(ChargedBytes::from_owned(
            vec![0x5a; PAYLOAD_BYTES],
            reservation,
        ))
        .await;
    assert!(
        result.is_ok(),
        "a frame whose receiver continually releases capacity must outlast one write progress \
         bound: {result:?}"
    );
    assert!(
        started.elapsed() >= 2 * PROGRESS_BOUND,
        "the moving frame must span multiple progress intervals"
    );
    writer.finish().expect("the completed frame half-closes");
    let observed = reader
        .await
        .expect("the reader task joins")
        .expect("the reader receives the complete frame");
    let mut expected = u32::try_from(PAYLOAD_BYTES)
        .expect("the bounded frame fits its length prefix")
        .to_be_bytes()
        .to_vec();
    expected.extend(vec![0x5a; PAYLOAD_BYTES]);
    assert_eq!(
        observed, expected,
        "the complete current frame is preserved"
    );
    driver.abort();
    let _ = driver.await;
}

#[nervix_primitives::test]
async fn a_duplex_frame_without_released_capacity_reaches_its_write_progress_bound() {
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
    let (mut client, connection) = client::handshake(client_io)
        .await
        .expect("the test client handshakes");
    let driver = spawn(connection);
    client = client.ready().await.expect("the test client is ready");
    let (_response, stream) = client
        .send_request(
            Request::builder()
                .uri("https://localhost/frame")
                .body(())
                .expect("the test request is valid"),
            false,
        )
        .expect("the test opens one frame stream");
    let executor = Executor::default();
    let reservation = executor
        .reserve(
            MemoryClass::Commands,
            u64::try_from(PAYLOAD_BYTES).expect("the bounded payload fits in u64"),
        )
        .await
        .expect("the test payload fits its memory class");
    let mut writer = FrameWriter::new(stream, PROGRESS_BOUND);
    let result = writer
        .send_frame(ChargedBytes::from_owned(
            vec![0x5a; PAYLOAD_BYTES],
            reservation,
        ))
        .await;
    assert!(
        matches!(result, Err(ref error) if matches!(error.current_context(), TransportError::ProgressTimeout { timeout } if *timeout == PROGRESS_BOUND)),
        "a receiver that releases no capacity must reach the unchanged progress bound: {result:?}"
    );
    let _ = release.send(());
    receiver.await.expect("the silent receiver joins");
    driver.abort();
    let _ = driver.await;
}
