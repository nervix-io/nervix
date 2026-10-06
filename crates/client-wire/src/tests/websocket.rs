//! The WebSocket message codec, without a WebSocket.

use bytes::Bytes;
use meticulous::ResultExt as _;
use nervix_models::{ArchiveDigest, RestoreArchive};

use super::{
    fixtures::{checked, limits, non_zero, reference, request, settings, size},
    samples::{client_messages, command_outcome},
};
use crate::{
    BackupArchiveStart, BackupDownloadMessage, BackupDownloadRequest, ClientMessage, ClientRequest,
    CommandDisposition, RestoreDisposition, RestoreMessage, RestoreReply, RestoreStart,
    ServerMessage, SessionLimitSettings,
    restore::RestoreChunk,
    websocket::{
        ClientBackupDownloadWebSocketCodec, ClientRestoreWebSocketCodec, ClientWebSocketCodec,
        ServerBackupDownloadWebSocketCodec, ServerRestoreWebSocketCodec, ServerWebSocketCodec,
        WebSocketData, WebSocketError,
    },
};

#[test]
fn a_binary_message_carries_exactly_one_frame() {
    let client = ClientWebSocketCodec::new(limits());
    let server = ServerWebSocketCodec::new(limits());
    assert_eq!(client.max_message_bytes(), limits().frame_bytes());
    for message in client_messages() {
        let payload = client.encode(message.encode(&limits()).assured("a request fits"));
        let frame = server
            .decode(WebSocketData::Binary(payload))
            .assured("a binary message holding a frame decodes");
        assert_eq!(
            ClientMessage::decode(&frame).assured("the frame decodes"),
            message
        );
    }
    let notice = crate::ServerNotice {
        level: crate::NoticeLevel::Info,
        message: "connected".to_string(),
    };
    let payload = server.encode(notice.encode(&limits()).assured("a notice fits"));
    let frame = client
        .decode(WebSocketData::Binary(payload))
        .assured("a binary message holding a frame decodes");
    assert!(matches!(
        ServerMessage::decode(&frame),
        Ok(ServerMessage::Event(crate::ServerEvent::Notice(_)))
    ));
    assert!(!format!("{client:?}").is_empty());
    assert!(!format!("{:?}", server.clone()).is_empty());
}

#[test]
fn non_frame_messages_end_the_connection_with_a_close_code() {
    let server = ServerWebSocketCodec::new(limits());
    let error = server
        .decode(WebSocketData::Text)
        .expect_err("a text message is not a frame");
    assert_eq!(error.current_context(), &WebSocketError::TextMessage);
    assert_eq!(WebSocketError::close_code(&error), 1003);

    let error = server
        .decode(WebSocketData::Binary(Bytes::from_static(b"not a frame")))
        .expect_err("garbage is not a frame");
    assert_eq!(error.current_context(), &WebSocketError::InvalidFrame);
    assert_eq!(WebSocketError::close_code(&error), 1007);

    let client = ClientWebSocketCodec::new(limits());
    let two_frames = [
        client_messages()[0]
            .encode(&limits())
            .assured("a request fits"),
        client_messages()[1]
            .encode(&limits())
            .assured("a request fits"),
    ]
    .iter()
    .flat_map(|frame| frame.bytes().to_vec())
    .collect::<Vec<_>>();
    let concatenated = server.decode(WebSocketData::Binary(Bytes::from(two_frames)));
    match concatenated {
        Ok(frame) => assert_eq!(
            ClientMessage::decode(&frame).assured("the leading frame decodes"),
            client_messages()[0],
            "trailing bytes are never read as a second request"
        ),
        Err(error) => assert_eq!(error.current_context(), &WebSocketError::InvalidFrame),
    }

    let small = checked(SessionLimitSettings {
        frame_bytes: size(1024),
        ..settings()
    });
    let small_server = ServerWebSocketCodec::new(small);
    let large = ClientMessage {
        request_id: request(1),
        request: ClientRequest::AttachTransaction(crate::AttachTransactionRequest {
            transaction_id: "t".repeat(2048),
        }),
    };
    let payload = client.encode(large.encode(&limits()).assured("a request fits"));
    let error = small_server
        .decode(WebSocketData::Binary(payload))
        .expect_err("a message above the frame limit");
    assert_eq!(WebSocketError::close_code(&error), 1009);
}

#[test]
fn a_console_download_carries_its_request_and_its_answer_one_frame_per_message() {
    let console = ClientBackupDownloadWebSocketCodec::new(limits());
    let server = ServerBackupDownloadWebSocketCodec::new(limits());
    let request = BackupDownloadRequest {
        execution_reference: reference("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"),
    };
    let payload = console.encode(request.encode(&limits()).assured("a request fits"));
    let frame = server
        .decode(WebSocketData::Binary(payload))
        .assured("the request decodes on the server");
    assert_eq!(
        BackupDownloadRequest::decode(&frame).assured("the request frame decodes"),
        request
    );

    let start = BackupArchiveStart {
        total_bytes: non_zero(6),
        digest: ArchiveDigest::from_bytes([7; 32]),
    };
    let answer = [
        BackupDownloadMessage::encode_start(&start, &limits()).assured("a start fits"),
        BackupDownloadMessage::encode_chunk(b"bytes!", &limits()).assured("a chunk fits"),
        BackupDownloadMessage::encode_complete(&limits()).assured("a completion fits"),
    ];
    let mut received = Vec::new();
    for frame in answer {
        let frame = console
            .decode(WebSocketData::Binary(server.encode(frame)))
            .assured("every answer frame decodes in the console");
        received.push(BackupDownloadMessage::decode(&frame).assured("the frame decodes"));
    }
    assert!(matches!(&received[0], BackupDownloadMessage::Start(decoded) if *decoded == start));
    assert!(
        matches!(&received[1], BackupDownloadMessage::Chunk(chunk) if chunk.bytes() == b"bytes!")
    );
    assert!(matches!(&received[2], BackupDownloadMessage::Complete));
}

#[test]
fn a_console_restore_carries_its_stream_and_its_reply_one_frame_per_message() {
    let console = ClientRestoreWebSocketCodec::new(limits());
    let server = ServerRestoreWebSocketCodec::new(limits());
    let start = RestoreStart {
        request_id: request(1),
        execution_reference: reference("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"),
        statement: "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' DRY RUN;".to_string(),
        archive: RestoreArchive {
            total_bytes: non_zero(6),
            digest: ArchiveDigest::from_bytes([5; 32]),
        },
    };
    let stream = [
        start.encode(&limits()).assured("a start fits"),
        RestoreChunk::encode(b"tenant", &limits()).assured("a chunk fits"),
    ];
    let mut received = Vec::new();
    for frame in stream {
        let frame = server
            .decode(WebSocketData::Binary(console.encode(frame)))
            .assured("every restore frame decodes on the server");
        received.push(RestoreMessage::decode(&frame).assured("the frame decodes"));
    }
    assert!(matches!(&received[0], RestoreMessage::Start(decoded) if *decoded == start));
    assert!(matches!(&received[1], RestoreMessage::Chunk(chunk) if chunk.bytes() == b"tenant"));

    let reply = RestoreReply {
        request_id: Some(request(1)),
        disposition: RestoreDisposition::Outcome(Box::new(command_outcome(
            CommandDisposition::Failed,
        ))),
    };
    let payload = server.encode(reply.encode(&limits()).assured("a reply fits"));
    let frame = console
        .decode(WebSocketData::Binary(payload))
        .assured("the reply decodes in the console");
    assert_eq!(
        RestoreReply::decode(&frame).assured("the reply frame decodes"),
        reply
    );
}

#[test]
fn each_console_websocket_refuses_the_frames_of_another_call() {
    let session = ClientWebSocketCodec::new(limits());
    let session_payload = session.encode(
        client_messages()[0]
            .encode(&limits())
            .assured("a request fits"),
    );
    let download = ServerBackupDownloadWebSocketCodec::new(limits());
    let error = download
        .decode(WebSocketData::Binary(session_payload.clone()))
        .expect_err("a session frame is not a download request");
    assert_eq!(WebSocketError::close_code(&error), 1007);
    let restore = ServerRestoreWebSocketCodec::new(limits());
    let error = restore
        .decode(WebSocketData::Binary(session_payload))
        .expect_err("a session frame is not a restore frame");
    assert_eq!(WebSocketError::close_code(&error), 1007);
}
