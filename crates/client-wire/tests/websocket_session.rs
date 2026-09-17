//! The session protocol over a WebSocket, as the browser console speaks it.
//!
//! The server and client here are tokio-tungstenite; the browser uses its own WebSocket, but both
//! hand the same codec one binary message per frame, so the exchange is the same.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientMessage, ClientRequest, DomainList, NoticeLevel, Reply, ReplyBody, ReplyDelivery,
    RequestId, ServerEvent, ServerMessage, ServerNotice, SessionLimitSettings, SessionLimits,
    SuggestOutcome, SuggestRequest, Suggestion, SuggestionKind,
    websocket::{ClientWebSocketCodec, ServerWebSocketCodec, WebSocketData, WebSocketError},
};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    WebSocketStream, accept_async_with_config, client_async_with_config,
    tungstenite::{
        Message,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};

const DEADLINE: Duration = Duration::from_secs(30);

fn limits() -> SessionLimits {
    let defaults = SessionLimits::DEFAULT;
    let size = |value: usize| NonZeroUsize::new(value).assured("a non-zero test limit");
    SessionLimits::try_from(SessionLimitSettings {
        frame_bytes: size(4096),
        transfer_bytes: size(defaults.transfer_bytes()),
        nesting_depth: size(defaults.nesting_depth()),
        collection_entries: size(defaults.collection_entries()),
        string_bytes: size(4096),
    })
    .assured("the test limits pass their checks")
}

/// A WebSocket configured to refuse messages the codec would refuse.
fn websocket_config(max_message_bytes: usize) -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(max_message_bytes),
        max_frame_size: Some(max_message_bytes),
        ..WebSocketConfig::default()
    }
}

fn request_id(id: u64) -> RequestId {
    RequestId::new(NonZeroU64::new(id).assured("a non-zero request identity"))
}

/// Serves one connection: a notice, then a reply to every request, and a close for anything that
/// is not a frame.
async fn serve_one(listener: TcpListener, limits: SessionLimits) {
    let codec = ServerWebSocketCodec::new(limits);
    let (stream, _) = listener.accept().await.assured("the client connects");
    let mut websocket =
        accept_async_with_config(stream, Some(websocket_config(codec.max_message_bytes())))
            .await
            .assured("the handshake completes");
    let notice = ServerNotice {
        level: NoticeLevel::Info,
        message: "connected to leader 'node-1'".to_string(),
    }
    .encode(&limits)
    .assured("a notice fits");
    websocket
        .send(Message::Binary(Vec::from(codec.encode(notice))))
        .await
        .assured("the client reads the notice");

    while let Some(message) = websocket.next().await {
        tokio::task::consume_budget().await;
        let data = match message.assured("the client sends well-formed WebSocket messages") {
            Message::Binary(payload) => WebSocketData::Binary(Bytes::from(payload)),
            Message::Text(_) => WebSocketData::Text,
            Message::Close(_) => return,
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
        };
        let frame = match codec.decode(data) {
            Ok(frame) => frame,
            Err(error) => {
                let close = CloseFrame {
                    code: CloseCode::from(WebSocketError::close_code(&error)),
                    reason: error.current_context().to_string().into(),
                };
                websocket
                    .close(Some(close))
                    .await
                    .assured("the client reads the close");
                return;
            }
        };
        let message = ClientMessage::decode(&frame).assured("the test client sends valid requests");
        let body = match message.request {
            ClientRequest::ListDomains => ReplyBody::DomainList(DomainList {
                domains: Vec::new(),
            }),
            ClientRequest::Suggest(suggest) => ReplyBody::Suggest(SuggestOutcome {
                suggestions: vec![Suggestion {
                    value: suggest.input()[..suggest.cursor()].to_string(),
                    kind: SuggestionKind::Text,
                }],
            }),
            other => panic!("the test client does not send {other:?}"),
        };
        let reply = Reply {
            request_id: message.request_id,
            body,
        };
        let ReplyDelivery::Frame(frame) = reply.encode(&limits).assured("a reply fits") else {
            panic!("test replies fit one frame");
        };
        websocket
            .send(Message::Binary(Vec::from(codec.encode(frame))))
            .await
            .assured("the client reads the reply");
    }
}

async fn connect(limits: SessionLimits) -> (WebSocketStream<TcpStream>, ClientWebSocketCodec) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assured("the loopback interface accepts a listener");
    let address = listener
        .local_addr()
        .assured("a bound listener has an address");
    tokio::spawn(serve_one(listener, limits));
    let codec = ClientWebSocketCodec::new(limits);
    let stream = TcpStream::connect(address)
        .await
        .assured("the test server accepts connections");
    let (websocket, _) = client_async_with_config(
        format!("ws://{address}/console/ws"),
        stream,
        Some(websocket_config(codec.max_message_bytes())),
    )
    .await
    .assured("the handshake completes");
    (websocket, codec)
}

async fn next_message(
    websocket: &mut WebSocketStream<TcpStream>,
    codec: &ClientWebSocketCodec,
) -> ServerMessage {
    let message = tokio::time::timeout(DEADLINE, websocket.next())
        .await
        .assured("the server answers within the deadline")
        .assured("the connection stays open")
        .assured("the server sends well-formed WebSocket messages");
    let Message::Binary(payload) = message else {
        panic!("the server sends frames as binary messages, not {message:?}");
    };
    let frame = codec
        .decode(WebSocketData::Binary(Bytes::from(payload)))
        .assured("a binary message holds one frame");
    ServerMessage::decode(&frame).assured("the frame decodes")
}

#[tokio::test]
async fn a_browser_session_exchanges_one_frame_per_binary_message() {
    let limits = limits();
    let (mut websocket, codec) = connect(limits).await;
    let requests = [
        ClientMessage {
            request_id: request_id(1),
            request: ClientRequest::ListDomains,
        },
        ClientMessage {
            request_id: request_id(2),
            request: ClientRequest::Suggest(
                SuggestRequest::new("CREATE ü".to_string(), 7, None)
                    .assured("byte 7 starts a character"),
            ),
        },
    ];
    for request in &requests {
        tokio::task::consume_budget().await;
        let frame = request.encode(&limits).assured("a request fits");
        websocket
            .send(Message::Binary(Vec::from(codec.encode(frame))))
            .await
            .assured("the server reads requests");
    }

    let ServerMessage::Event(ServerEvent::Notice(notice)) =
        next_message(&mut websocket, &codec).await
    else {
        panic!("the unsolicited notice arrives first");
    };
    assert_eq!(notice.message, "connected to leader 'node-1'");

    let ServerMessage::Reply(listed) = next_message(&mut websocket, &codec).await else {
        panic!("the domain request is answered");
    };
    assert_eq!(listed.request_id, request_id(1));
    assert!(matches!(listed.body, ReplyBody::DomainList(_)));

    let ServerMessage::Reply(suggested) = next_message(&mut websocket, &codec).await else {
        panic!("the suggestion request is answered");
    };
    assert_eq!(suggested.request_id, request_id(2));
    assert_eq!(
        suggested.body,
        ReplyBody::Suggest(SuggestOutcome {
            suggestions: vec![Suggestion {
                value: "CREATE ".to_string(),
                kind: SuggestionKind::Text,
            }],
        })
    );
}

async fn close_code_after(message: Message) -> CloseCode {
    let limits = limits();
    let (mut websocket, codec) = connect(limits).await;
    assert!(matches!(
        next_message(&mut websocket, &codec).await,
        ServerMessage::Event(ServerEvent::Notice(_))
    ));
    websocket
        .send(message)
        .await
        .assured("the server reads the message");
    loop {
        tokio::task::consume_budget().await;
        let received = tokio::time::timeout(DEADLINE, websocket.next())
            .await
            .assured("the server answers within the deadline")
            .assured("the connection closes with a close message")
            .assured("the server closes cleanly");
        if let Message::Close(Some(close)) = received {
            return close.code;
        }
    }
}

#[tokio::test]
async fn a_text_message_closes_the_session_as_unsupported_data() {
    let code = close_code_after(Message::Text("SHOW DOMAINS;".to_string())).await;
    assert_eq!(code, CloseCode::Unsupported);
}

#[tokio::test]
async fn a_binary_message_that_is_not_a_frame_closes_the_session_as_invalid() {
    let code = close_code_after(Message::Binary(b"not a frame".to_vec())).await;
    assert_eq!(code, CloseCode::Invalid);
}
