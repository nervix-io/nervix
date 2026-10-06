//! Console WebSocket transport tests: the session's broken framing, and the download and restore
//! calls.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that a restore stream over a WebSocket ends with the chunk that
//!   completes its declared size and reads nothing after it, that a frame breaking the stream or
//!   the connection's end ends it, that a console download answers its one request after any
//!   control message and closes normally, that a client's close stops a download before frames
//!   that remain, that a message that is not a frame, a second request and a message above the
//!   frame limit each close a download with their close code, that a vanished client ends a call
//!   quietly, that a restore over a console WebSocket is answered once its declared bytes arrived
//!   and not when the client leaves first, that both end going away when the node stops, and that
//!   a text message closes a console session with its close code.
//! - **Depends on.** The console session and call transports, the session test service, and an
//!   in-memory WebSocket pair.
//! - **Must not know.** How downloads and restores are answered beyond their wire contract.

use std::{num::NonZeroU64, time::Duration};

use futures_util::{SinkExt as _, StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame, BackupDownloadMessage,
    BackupDownloadRequest, EncodedFrame, RequestId, RestoreChunk, RestoreDisposition, RestoreFrame,
    RestoreReply, RestoreStart, RestoreUploadFailure, SessionLimits, VerifiedFrame,
    websocket::{
        ClientBackupDownloadWebSocketCodec, ClientRestoreWebSocketCodec,
        ServerBackupDownloadWebSocketCodec, WebSocketData,
    },
};
use nervix_models::{ArchiveDigest, CommandExecutionReference, RestoreArchive, UserName};
use tokio::io::DuplexStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};

use super::{ConsoleCallEnd, DeclaredProgress, DeclaredRestoreStream};
use crate::application::{
    session::websocket::console_websocket_config,
    test_fixtures::{TestService, build_test_service},
};

/// How long a test waits for the server's next message.
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);

fn limits() -> SessionLimits {
    SessionLimits::DEFAULT
}

fn user() -> UserName {
    UserName::parse("default").assured("the test user is a valid literal name")
}

fn reference() -> CommandExecutionReference {
    CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string())
        .assured("a hyphenated UUID is a valid execution reference")
}

fn declared(total_bytes: u64) -> RestoreArchive {
    RestoreArchive {
        total_bytes: NonZeroU64::new(total_bytes).assured("every test archive has bytes"),
        digest: ArchiveDigest::from_bytes([3; 32]),
    }
}

fn start(statement: &str, archive: RestoreArchive) -> VerifiedFrame<RestoreFrame> {
    RestoreStart {
        request_id: RequestId::new(NonZeroU64::MIN),
        execution_reference: reference(),
        statement: statement.to_string(),
        archive,
    }
    .encode(&limits())
    .assured("a test start fits a frame")
    .verify(&limits())
    .assured("an encoded start verifies")
}

fn chunk(bytes: &[u8]) -> VerifiedFrame<RestoreFrame> {
    RestoreChunk::encode(bytes, &limits())
        .assured("a test chunk fits a frame")
        .verify(&limits())
        .assured("an encoded chunk verifies")
}

const DRY_RUN: &str = "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' DRY RUN;";

#[test]
fn the_declared_size_ends_a_restore_stream_at_the_chunk_that_completes_it() {
    let progress = DeclaredProgress::AwaitingStart.after(&start(DRY_RUN, declared(5)));
    assert_eq!(
        progress,
        DeclaredProgress::Remaining(NonZeroU64::new(5).assured("five is non-zero"))
    );
    let progress = progress.after(&chunk(b"abc"));
    assert_eq!(
        progress,
        DeclaredProgress::Remaining(NonZeroU64::new(2).assured("two is non-zero"))
    );
    assert_eq!(progress.after(&chunk(b"de")), DeclaredProgress::Ended);
    assert_eq!(
        progress.after(&chunk(b"def")),
        DeclaredProgress::Ended,
        "a chunk past the declared size ends the stream, which the restore refuses"
    );
}

#[test]
fn a_frame_that_breaks_a_restore_stream_ends_it_at_that_frame() {
    assert_eq!(
        DeclaredProgress::AwaitingStart.after(&chunk(b"abc")),
        DeclaredProgress::Ended,
        "a stream that does not begin with its start"
    );
    let progress = DeclaredProgress::AwaitingStart.after(&start(DRY_RUN, declared(5)));
    assert_eq!(
        progress.after(&start(DRY_RUN, declared(5))),
        DeclaredProgress::Ended,
        "a second start"
    );
}

#[nervix_primitives::test]
async fn a_declared_restore_stream_reads_nothing_after_its_last_chunk() {
    let frames = vec![
        Ok::<_, ()>(start(DRY_RUN, declared(5))),
        Ok(chunk(b"abc")),
        Ok(chunk(b"de")),
        Ok(chunk(b"never read")),
    ];
    let mut inbound = stream::iter(frames);
    let received = DeclaredRestoreStream::new(&mut inbound)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(received.len(), 3);
    let Some(Ok(unread)) = inbound.next().await else {
        panic!("the frame after the declared size stays unread");
    };
    assert_eq!(unread.bytes(), chunk(b"never read").bytes());
}

/// One end of an in-memory WebSocket a test drives as the console.
type ConsoleEnd = WebSocketStream<DuplexStream>;

/// Opens an in-memory WebSocket whose server end `serve` takes.
async fn console_connection() -> (ConsoleEnd, WebSocketStream<DuplexStream>) {
    let (console, server) = tokio::io::duplex(8 * 1024 * 1024);
    let config = console_websocket_config(&limits());
    let server = WebSocketStream::from_raw_socket(server, Role::Server, Some(config)).await;
    let console = WebSocketStream::from_raw_socket(console, Role::Client, None).await;
    (console, server)
}

/// The next message the server sent, within the test's patience.
async fn next_message(console: &mut ConsoleEnd) -> Message {
    let received = nervix_primitives::time::timeout(MESSAGE_TIMEOUT, console.next()).await;
    let Ok(Some(Ok(message))) = received else {
        panic!("the server sent no message in time: {received:?}");
    };
    message
}

#[nervix_primitives::test]
async fn a_console_download_answers_its_request_and_closes_normally() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    let codec = ClientBackupDownloadWebSocketCodec::new(limits());
    let request = BackupDownloadRequest {
        execution_reference: reference(),
    }
    .encode(&limits())
    .assured("a download request fits a frame");
    console
        .send(Message::Binary(Vec::from(codec.encode(request))))
        .await
        .assured("the in-memory connection takes the request");

    let Message::Binary(payload) = next_message(&mut console).await else {
        panic!("a download is answered with a binary frame");
    };
    let frame = codec
        .decode(WebSocketData::Binary(payload.into()))
        .assured("the answer is one download frame");
    let answer = BackupDownloadMessage::decode(&frame).assured("the download frame decodes");
    let BackupDownloadMessage::Failed(failed) = answer else {
        panic!("a reference without an archive is refused, found {answer:?}");
    };
    assert_eq!(failed.failure, BackupDownloadFailure::NotRetained);
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("the server closes the connection after its answer");
    };
    assert_eq!(close.code, CloseCode::Normal);
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_download_stops_at_its_client_close_while_frames_remain() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let codec = ServerBackupDownloadWebSocketCodec::new(limits());
    let (mut sink, mut messages) = server.split();
    // Every frame of the answer is ready at once, as when a small archive streams.
    let failed = BackupDownloadFailed {
        failure: BackupDownloadFailure::ReadFailed,
        message: "a frame of the answer".to_string(),
    };
    let frames = (0..3)
        .map(|_| {
            BackupDownloadMessage::encode_failed(&failed, &limits())
                .assured("a test frame fits a frame")
        })
        .collect::<Vec<_>>();
    console
        .close(None)
        .await
        .assured("the console closes its side of the connection");

    let ended = service
        .send_download_frames(
            &codec,
            &mut sink,
            &mut messages,
            Box::pin(stream::iter(frames)),
        )
        .await;
    assert!(
        matches!(ended, Some(ConsoleCallEnd::Closed)),
        "a close the client sent stops the download before its remaining frames, found {ended:?}"
    );
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_text_message_closes_a_console_download_with_its_close_code() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    console
        .send(Message::Text("not a frame".into()))
        .await
        .assured("the in-memory connection takes the message");
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("a text message ends the download with a close");
    };
    assert_eq!(close.code, CloseCode::from(1003));
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_restore_is_answered_once_its_declared_bytes_arrived() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_restore(user(), limits(), server)
                .await;
        }
    });
    let codec = ClientRestoreWebSocketCodec::new(limits());
    // The declared digest is not the bytes' digest, so the leader stages the archive and refuses
    // it once every declared byte arrived: the stream ended without a half-close.
    for frame in [start(DRY_RUN, declared(5)), chunk(b"abc"), chunk(b"de")] {
        console
            .send(Message::Binary(frame.bytes().to_vec()))
            .await
            .assured("the in-memory connection takes the frame");
    }

    let Message::Binary(payload) = next_message(&mut console).await else {
        panic!("a restore is answered with a binary frame");
    };
    let frame = codec
        .decode(WebSocketData::Binary(payload.into()))
        .assured("the answer is one restore reply");
    let reply = RestoreReply::decode(&frame).assured("the restore reply decodes");
    let RestoreDisposition::UploadFailed { failure, .. } = reply.disposition else {
        panic!("an archive without its declared digest is refused, found {reply:?}");
    };
    assert_eq!(failure, RestoreUploadFailure::DigestMismatch);
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("the server closes the connection after its reply");
    };
    assert_eq!(close.code, CloseCode::Normal);
    serving.await.assured("the restore task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_restore_whose_client_leaves_early_is_not_answered() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_restore(user(), limits(), server)
                .await;
        }
    });
    for frame in [start(DRY_RUN, declared(5)), chunk(b"abc")] {
        console
            .send(Message::Binary(frame.bytes().to_vec()))
            .await
            .assured("the in-memory connection takes the frame");
    }
    console
        .close(None)
        .await
        .assured("the console closes its side of the connection");
    // The call ended in transport, which changes nothing and answers nothing: the next message
    // is the server's side of the close.
    let message = next_message(&mut console).await;
    assert!(
        matches!(message, Message::Close(_)),
        "a restore whose client left is not answered, found {message:?}"
    );
    serving.await.assured("the restore task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_download_ends_going_away_when_the_node_stops() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    // The client never sends its request; the node stopping ends the call all the same.
    service.inner.admission_shutdown.cancel();
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("the server closes a download when the node stops");
    };
    assert_eq!(close.code, CloseCode::Away);
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_restore_ends_going_away_when_the_node_stops() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_restore(user(), limits(), server)
                .await;
        }
    });
    for frame in [start(DRY_RUN, declared(5)), chunk(b"abc")] {
        console
            .send(Message::Binary(frame.bytes().to_vec()))
            .await
            .assured("the in-memory connection takes the frame");
    }
    service.inner.admission_shutdown.cancel();
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("the server closes a restore whose archive is still arriving when the node stops");
    };
    assert_eq!(close.code, CloseCode::Away);
    serving.await.assured("the restore task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

/// A download request for a reference no node retains an archive under, as a binary message.
fn download_request() -> Message {
    let codec = ClientBackupDownloadWebSocketCodec::new(limits());
    let request = BackupDownloadRequest {
        execution_reference: reference(),
    }
    .encode(&limits())
    .assured("a download request fits a frame");
    Message::Binary(Vec::from(codec.encode(request)))
}

/// The close the server ends the connection with, after any frames and pongs it sent first.
async fn closing_frame(console: &mut ConsoleEnd) -> CloseFrame<'static> {
    loop {
        nervix_primitives::task::consume_budget().await;
        match next_message(console).await {
            Message::Close(Some(close)) => return close,
            Message::Close(None) => panic!("the server closes with a code"),
            Message::Binary(_)
            | Message::Text(_)
            | Message::Ping(_)
            | Message::Pong(_)
            | Message::Frame(_) => {}
        }
    }
}

#[nervix_primitives::test]
async fn a_console_download_takes_its_request_after_a_ping() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    console
        .send(Message::Ping(vec![7]))
        .await
        .assured("the in-memory connection takes the ping");
    console
        .send(download_request())
        .await
        .assured("the in-memory connection takes the request");

    let mut message = next_message(&mut console).await;
    if let Message::Pong(_) = message {
        message = next_message(&mut console).await;
    }
    let Message::Binary(_) = message else {
        panic!("a request after a ping is answered, found {message:?}");
    };
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("the server closes the connection after its answer");
    };
    assert_eq!(close.code, CloseCode::Normal);
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_second_request_on_a_console_download_breaks_the_call() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    for _ in 0..2 {
        console
            .send(download_request())
            .await
            .assured("the in-memory connection takes the request");
    }
    // The second request arrived before the answer was sent, so it is heard first.
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("a second request ends the download with a close");
    };
    assert_eq!(close.code, CloseCode::from(1008));
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_message_above_the_frame_limit_closes_a_console_download_with_its_close_code() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    let oversized = limits()
        .frame_bytes()
        .checked_add(1)
        .assured("the frame limit is far below usize::MAX");
    console
        .send(Message::Binary(vec![0; oversized]))
        .await
        .assured("the in-memory connection takes the message");
    let Message::Close(Some(close)) = next_message(&mut console).await else {
        panic!("a message above the frame limit ends the download with a close");
    };
    assert_eq!(close.code, CloseCode::from(1009));
    serving.await.assured("the download task ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_download_whose_client_vanishes_ends_quietly() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_download(user(), limits(), server)
                .await;
        }
    });
    drop(console);
    serving
        .await
        .assured("a download whose connection failed ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_download_sending_its_frames_stops_when_its_connection_ends_or_the_node_stops() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (_console, server) = console_connection().await;
    let codec = ServerBackupDownloadWebSocketCodec::new(limits());
    let (mut sink, _messages) = server.split();

    // Messages that end without a close come from a connection that failed.
    let mut ended = stream::empty::<Result<Message, tungstenite::Error>>();
    let frames = Box::pin(stream::pending::<EncodedFrame<BackupDownloadFrame>>());
    let end = service
        .send_download_frames(&codec, &mut sink, &mut ended, frames)
        .await;
    assert!(matches!(end, Some(ConsoleCallEnd::Failed)), "found {end:?}");

    // A node that stops ends the download while its frames are still to come.
    service.inner.admission_shutdown.cancel();
    let mut silent = stream::pending::<Result<Message, tungstenite::Error>>();
    let frames = Box::pin(stream::pending::<EncodedFrame<BackupDownloadFrame>>());
    let end = service
        .send_download_frames(&codec, &mut sink, &mut silent, frames)
        .await;
    assert!(
        matches!(end, Some(ConsoleCallEnd::ShuttingDown)),
        "found {end:?}"
    );
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_console_restore_whose_client_leaves_after_its_last_chunk_is_not_answered() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_restore(user(), limits(), server)
                .await;
        }
    });
    for frame in [start(DRY_RUN, declared(5)), chunk(b"abc"), chunk(b"de")] {
        console
            .send(Message::Binary(frame.bytes().to_vec()))
            .await
            .assured("the in-memory connection takes the frame");
    }
    // The client leaves without waiting for the reply its last chunk asked for.
    drop(console);
    serving
        .await
        .assured("a restore whose client left ends after its reply finds no connection");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}

#[nervix_primitives::test]
async fn a_declared_restore_stream_ends_with_its_connection() {
    let mut inbound = stream::iter(Vec::<Result<VerifiedFrame<RestoreFrame>, ()>>::new());
    let received = DeclaredRestoreStream::new(&mut inbound)
        .collect::<Vec<_>>()
        .await;
    assert!(received.is_empty());
}

#[nervix_primitives::test]
async fn a_text_message_closes_a_console_session_with_its_close_code() {
    let TestService {
        service,
        registry: _registry,
        path,
    } = build_test_service(true).await;
    let (mut console, server) = console_connection().await;
    let serving = nervix_primitives::task::spawn({
        let service = service.clone();
        async move {
            service
                .serve_console_session(user(), limits(), server)
                .await;
        }
    });
    // A control message belongs to the connection and keeps the session open.
    console
        .send(Message::Ping(vec![7]))
        .await
        .assured("the in-memory connection takes the ping");
    console
        .send(Message::Text("not a frame".into()))
        .await
        .assured("the in-memory connection takes the message");
    let close = closing_frame(&mut console).await;
    assert_eq!(close.code, CloseCode::from(1003));
    serving.await.assured("the session ends");
    std::fs::remove_dir_all(&path).assured("the test database directory is removable");
}
