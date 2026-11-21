//! The request queue of a session the client opens over a real gRPC connection.
//!
//! Layer: test harness.
//! - **Owns.** The witness that a session's request queue takes the configured capacity.
//! - **Depends on.** The production client and the session fixture's real gRPC listener.
//! - **Must not know.** How the server reads the frames.

use super::*;

#[nervix_primitives::test]
async fn a_session_queues_as_many_request_frames_as_its_client_is_configured_to() {
    let mut server = TestServer::start().await;
    let options = ConnectOptions {
        request_frame_capacity: NonZeroUsize::new(3).assured("three is positive"),
        ..ConnectOptions::default()
    };
    let client = server.connect_with(options).await;
    let _exchange = server.next_exchange().await;
    let queued = client
        .inner
        .exchange
        .lock()
        .await
        .requests
        .frames
        .max_capacity();
    assert_eq!(
        queued, 3,
        "the session's request queue holds the configured frames"
    );
}
