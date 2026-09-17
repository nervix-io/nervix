//! The WebSocket message codec, without a WebSocket.

use bytes::Bytes;
use meticulous::ResultExt as _;

use super::{
    fixtures::{checked, limits, request, settings, size},
    samples::client_messages,
};
use crate::{
    ClientMessage, ClientRequest, ServerMessage, SessionLimitSettings,
    websocket::{ClientWebSocketCodec, ServerWebSocketCodec, WebSocketData, WebSocketError},
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
