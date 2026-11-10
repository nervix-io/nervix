//! Real-transport checks of how the outbound connection ends when its signaling handshake fails:
//! the Close each failure earns its peer, the resume failure and its typed causes, and the frames
//! the failed attempt accepted.
//!
//! Layer: test harness.
//! - **Owns.** Playing the peer of a failing handshake through a local protocol peer, and reading
//!   again through the WebSocket driver what a peer that stopped reading was sent.
//! - **Depends on.** The ingestor's fixture, the compiled signaling engine, the connector contract
//!   and execution primitives.
//! - **Must not know.** Registry, graph or host synchronization internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test boundary declared above")
)]

use nervix_models::{
    ModelName, SignalingProtocolName, SignalingProtocolOnConnect, SignalingStep, SignalingWaitStep,
    SignalingWireFormat,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError,
        error::CapacityError,
        protocol::{
            CloseFrame, Role,
            frame::{
                Frame,
                coding::{CloseCode, Data, OpCode},
            },
        },
    },
};

use super::{connection::ConnectionEnding, *};
use crate::WebsocketSignalingError;

/// The handshake the peer fails, compiled as the decision layer compiles a protocol.
fn compiled(
    format: SignalingWireFormat,
    on_connect: SignalingProtocolOnConnect,
) -> Arc<CompiledSignalingProtocol> {
    let name = ModelName::parse("handshake").assured("handshake is a Plexus identifier");
    let compiled = CompiledSignalingProtocol::compile_parts(
        &SignalingProtocolName::from(&name),
        &format,
        &on_connect,
        None,
    )
    .assured("the fixture protocol compiles");
    Arc::new(compiled)
}

/// The client sends `subscription`, then waits for `.ready`, which `fails` guard and `capture`
/// reads once it arrives.
fn subscribing(
    subscription: &str,
    fails: &[&str],
    capture: Option<&str>,
    timeout: &str,
) -> SignalingProtocolOnConnect {
    let mut wait = SignalingWaitStep::new(vec![".ready".to_string()]);
    wait.fail_matchers = fails.iter().map(|matcher| matcher.to_string()).collect();
    wait.capture = capture.map(str::to_string);
    SignalingProtocolOnConnect {
        accept_data: false,
        steps: vec![
            SignalingStep::Send(vec![subscription.to_string()]),
            SignalingStep::Wait(wait),
        ],
        fail_matchers: Vec::new(),
        timeout: timeout.to_string(),
    }
}

/// How the client ended a connection whose peer stopped reading it.
enum UnreadEnding {
    /// A Close frame, with the status it carried, came before the end of TCP.
    Closed(Option<CloseFrame<'static>>),
    /// TCP ended with no Close frame before it.
    WithoutClose,
}

/// Reads everything TCP delivered to a peer that stopped reading its connection, once the client
/// has released it, then reads it again through the WebSocket driver: the client's handshake
/// frames, then how the connection ended. Nothing is written back. The calling test bounds the
/// wait for the client's release.
async fn unread_ending(mut peer: WebSocketStream<TcpStream>) -> UnreadEnding {
    let mut delivered = Vec::new();
    peer.get_mut()
        .read_to_end(&mut delivered)
        .await
        .assured("the client read everything the peer sent, so TCP ends in order");
    let mut replay = WebSocketStream::from_raw_socket(
        tokio::io::join(std::io::Cursor::new(delivered), tokio::io::sink()),
        Role::Server,
        None,
    )
    .await;
    loop {
        nervix_primitives::task::consume_budget().await;
        match replay.next().await {
            Some(Ok(Message::Close(frame))) => return UnreadEnding::Closed(frame),
            // The subscription the client sent before its handshake failed.
            Some(Ok(Message::Text(_))) => {}
            Some(Err(WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {
                return UnreadEnding::WithoutClose;
            }
            other => {
                panic!("a failed handshake carries its subscription and a Close, not {other:?}")
            }
        }
    }
}

/// A frame RFC 6455 section 5 forbids a peer to send: a data frame with a reserved bit set.
fn reserved_bit_frame() -> Frame {
    let mut frame = Frame::message(b"payload".to_vec(), OpCode::Data(Data::Binary), true);
    frame.header_mut().rsv1 = true;
    frame
}

/// One way the client's signaling handshake fails, scripted through its peer.
struct FailedHandshake {
    /// What fails the handshake, for assertion messages.
    failure: &'static str,
    /// Client configuration beside the endpoint.
    config: Vec<ClientConfigEntry>,
    format: SignalingWireFormat,
    on_connect: SignalingProtocolOnConnect,
    /// What the peer sends once the upgrade completes. It never reads the connection.
    answers: Vec<Message>,
    /// Whether the resume failure keeps this typed handshake cause.
    cause: fn(&WebsocketSignalingError) -> bool,
    /// The status of the Close the peer reads before TCP ends.
    status: CloseCode,
}

fn failed_handshakes() -> Vec<FailedHandshake> {
    let subscription = "{op: \"subscribe\"}";
    vec![
        FailedHandshake {
            failure: "a frame a FAIL JAQ guard rejects",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[".error"], None, "5s"),
            answers: vec![Message::Text(
                "{\"error\":\"subscription refused\"}".to_string(),
            )],
            cause: |cause| {
                matches!(
                    cause,
                    WebsocketSignalingError::Rejected { matcher, reason }
                        if matcher == ".error" && reason == "subscription refused"
                )
            },
            status: CloseCode::Policy,
        },
        FailedHandshake {
            failure: "the protocol's TIMEOUT",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[], None, "200ms"),
            answers: Vec::new(),
            cause: |cause| {
                matches!(
                    cause,
                    WebsocketSignalingError::Timeout { unsatisfied, .. }
                        if unsatisfied == &[".ready".to_string()]
                )
            },
            status: CloseCode::Away,
        },
        FailedHandshake {
            failure: "a message beyond max_message_size",
            config: vec![ClientConfigEntry {
                key: "max_message_size".to_string(),
                value: "4".to_string(),
            }],
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[], None, "5s"),
            answers: vec![Message::Binary(vec![0; 8])],
            cause: |cause| {
                let WebsocketSignalingError::Receive(failure) = cause else {
                    return false;
                };
                matches!(
                    **failure,
                    WebsocketError::Capacity(CapacityError::MessageTooLong {
                        size: 8,
                        max_size: 4,
                    })
                )
            },
            status: CloseCode::Size,
        },
        FailedHandshake {
            failure: "a framing violation",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[], None, "5s"),
            answers: vec![Message::Frame(reserved_bit_frame())],
            cause: |cause| {
                let WebsocketSignalingError::Receive(failure) = cause else {
                    return false;
                };
                matches!(
                    **failure,
                    WebsocketError::Protocol(ProtocolError::NonZeroReservedBits)
                )
            },
            status: CloseCode::Protocol,
        },
        FailedHandshake {
            failure: "message text that is not UTF-8",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[], None, "5s"),
            answers: vec![Message::Frame(Frame::message(
                vec![0xC3, 0x28],
                OpCode::Data(Data::Text),
                true,
            ))],
            cause: |cause| {
                let WebsocketSignalingError::Receive(failure) = cause else {
                    return false;
                };
                matches!(**failure, WebsocketError::Utf8)
            },
            status: CloseCode::Invalid,
        },
        FailedHandshake {
            failure: "a CAPTURE that produces no object",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing(subscription, &[], Some(".session"), "5s"),
            answers: vec![Message::Text("{\"ready\":true,\"session\":5}".to_string())],
            cause: |cause| {
                matches!(
                    cause,
                    WebsocketSignalingError::Capture { capture, .. } if capture == ".session"
                )
            },
            status: CloseCode::Error,
        },
        FailedHandshake {
            failure: "a SEND JAQ program that produces no frame",
            config: Vec::new(),
            format: SignalingWireFormat::Json,
            on_connect: subscribing("empty", &[], None, "5s"),
            answers: Vec::new(),
            cause: |cause| matches!(cause, WebsocketSignalingError::SendProgram { index: 1, .. }),
            status: CloseCode::Error,
        },
        FailedHandshake {
            failure: "a SEND JAQ output RAW cannot carry",
            config: Vec::new(),
            format: SignalingWireFormat::Raw,
            on_connect: subscribing(subscription, &[], None, "5s"),
            answers: Vec::new(),
            cause: |cause| matches!(cause, WebsocketSignalingError::SendEncode { index: 1, .. }),
            status: CloseCode::Error,
        },
    ]
}

#[nervix_primitives::test]
async fn a_failed_handshake_reads_the_close_its_failure_earns_before_the_connection_ends() {
    for case in failed_handshakes() {
        let (mut ingestor, listener) =
            tests::signaling_ingestor(case.config, compiled(case.format, case.on_connect)).await;
        let answers = case.answers;
        let peer = async {
            let (stream, _) = listener.accept().await.assured("the ingestor connects");
            let mut peer = tokio_tungstenite::accept_async(stream)
                .await
                .assured("the upgrade succeeds");
            for answer in answers {
                peer.send(answer)
                    .await
                    .assured("the peer's transport writes its answer as given");
            }
            unread_ending(peer).await
        };
        let (resumed, ending) = timeout(Duration::from_secs(60), async {
            tokio::join!(ingestor.resume(), peer)
        })
        .await
        .assured("the handshake fails and the client releases its connection within the budget");
        let Err(report) = resumed else {
            panic!("{} fails the handshake", case.failure);
        };
        assert!(
            matches!(report.current_context(), IngestorError::Resume { .. }),
            "{}: {report:#}",
            case.failure
        );
        let Some(cause) = report.downcast_ref::<WebsocketSignalingError>() else {
            panic!(
                "{} keeps the handshake's typed cause: {report:#}",
                case.failure
            );
        };
        assert!((case.cause)(cause), "{}: {report:#}", case.failure);
        assert!(ingestor.needs_resume(), "{}", case.failure);
        let UnreadEnding::Closed(Some(frame)) = ending else {
            panic!(
                "{} ends the connection with a Close frame before TCP ends",
                case.failure
            );
        };
        assert_eq!(frame.code, case.status, "{}", case.failure);
    }
}

#[nervix_primitives::test]
async fn a_peer_close_during_signaling_is_answered_before_the_connection_is_released() {
    for frame in [
        Some(CloseFrame {
            code: CloseCode::Policy,
            reason: "subscription refused".into(),
        }),
        None,
    ] {
        let mut on_connect = subscribing("{op: \"subscribe\"}", &[], None, "5s");
        on_connect.accept_data = true;
        let protocol = compiled(SignalingWireFormat::Json, on_connect);
        let (mut ingestor, listener) = tests::signaling_ingestor(Vec::new(), protocol).await;
        let expected = frame.clone();
        let peer = async {
            let (stream, _) = listener.accept().await.assured("the ingestor connects");
            let mut peer = tokio_tungstenite::accept_async(stream)
                .await
                .assured("the upgrade succeeds");
            let subscription = peer
                .next()
                .await
                .assured("the client sends its subscription")
                .assured("the subscription is a valid frame");
            assert!(matches!(subscription, Message::Text(_)));
            peer.send(Message::Text("{\"user_id\":1}".to_string()))
                .await
                .assured("the failed attempt accepts a payload before the Close");
            peer.send(Message::Close(frame))
                .await
                .assured("the peer writes its Close");
            unread_ending(peer).await
        };
        let (resumed, ending) = timeout(Duration::from_secs(60), async {
            tokio::join!(ingestor.resume(), peer)
        })
        .await
        .assured("the client answers the Close and releases the connection within the budget");
        let Err(report) = resumed else {
            panic!("a peer Close leaves the signaling handshake unfinished");
        };
        let UnreadEnding::Closed(answer) = ending else {
            panic!("the client answers the peer's Close before releasing the connection");
        };
        assert_eq!(
            answer, expected,
            "the reply echoes the exact status and reason"
        );
        match expected {
            Some(frame) => {
                let Some(WebsocketSignalingError::PeerClosed { code, reason }) =
                    report.downcast_ref::<WebsocketSignalingError>()
                else {
                    panic!(
                        "the resume failure retains the typed peer status and reason: {report:#}"
                    );
                };
                assert_eq!(*code, frame.code);
                assert_eq!(reason, frame.reason.as_ref());
            }
            None => assert!(matches!(
                report.downcast_ref::<WebsocketSignalingError>(),
                Some(WebsocketSignalingError::PeerClosedWithoutStatus)
            )),
        }
        assert!(matches!(
            report.current_context(),
            IngestorError::Resume { .. }
        ));
        assert!(ingestor.needs_resume());
        assert!(ingestor.pending.is_empty());
    }
}

#[nervix_primitives::test]
async fn a_handshake_whose_peer_ended_tcp_first_is_released_without_a_close() {
    let protocol = compiled(
        SignalingWireFormat::Json,
        subscribing("{op: \"subscribe\"}", &[], None, "5s"),
    );
    let (mut ingestor, listener) = tests::signaling_ingestor(Vec::new(), protocol).await;
    let peer = async {
        let (stream, _) = listener.accept().await.assured("the ingestor connects");
        let mut peer = tokio_tungstenite::accept_async(stream)
            .await
            .assured("the upgrade succeeds");
        // The peer ends its half of TCP and keeps reading what the client sends after.
        peer.get_mut()
            .shutdown()
            .await
            .assured("a connected socket ends its sending half");
        unread_ending(peer).await
    };
    let (resumed, ending) = timeout(Duration::from_secs(60), async {
        tokio::join!(ingestor.resume(), peer)
    })
    .await
    .assured("the handshake fails and the client releases its connection within the budget");
    let Err(report) = resumed else {
        panic!("TCP ended before the handshake completed");
    };
    let Some(WebsocketSignalingError::Receive(failure)) =
        report.downcast_ref::<WebsocketSignalingError>()
    else {
        panic!("the resume failure keeps the handshake's typed read failure: {report:#}");
    };
    assert!(
        matches!(
            **failure,
            WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
        ),
        "{report:#}"
    );
    assert!(ingestor.needs_resume());
    assert!(
        matches!(ending, UnreadEnding::WithoutClose),
        "a connection whose TCP already ended is released without a Close"
    );
}

#[nervix_primitives::test]
async fn a_failed_handshake_discards_the_frames_it_accepted() {
    // Payload streams from the first frame, so the refused attempt accepts one before it fails.
    let mut on_connect = subscribing("{op: \"subscribe\"}", &[".error"], None, "5s");
    on_connect.accept_data = true;
    let (mut ingestor, listener) =
        tests::signaling_ingestor(Vec::new(), compiled(SignalingWireFormat::Json, on_connect))
            .await;
    let refusing = async {
        let (stream, _) = listener.accept().await.assured("the ingestor connects");
        let mut peer = tokio_tungstenite::accept_async(stream)
            .await
            .assured("the upgrade succeeds");
        for answer in ["{\"user_id\":1}", "{\"error\":\"subscription refused\"}"] {
            peer.send(Message::Text(answer.to_string()))
                .await
                .assured("the peer writes its answer");
        }
        unread_ending(peer).await
    };
    let (refused, ending) = timeout(Duration::from_secs(60), async {
        tokio::join!(ingestor.resume(), refusing)
    })
    .await
    .assured("the handshake fails and the client releases its connection within the budget");
    let Err(report) = refused else {
        panic!("the FAIL JAQ guard rejects the handshake");
    };
    assert!(
        matches!(
            report.downcast_ref::<WebsocketSignalingError>(),
            Some(WebsocketSignalingError::Rejected { .. })
        ),
        "{report:#}"
    );
    let UnreadEnding::Closed(Some(frame)) = ending else {
        panic!("a rejected handshake ends the connection with a Close frame before TCP ends");
    };
    assert_eq!(frame.code, CloseCode::Policy);
    assert!(ingestor.pending.is_empty());
    assert!(ingestor.needs_resume());

    let answering = async {
        let (stream, _) = listener.accept().await.assured("the ingestor reconnects");
        let mut peer = tokio_tungstenite::accept_async(stream)
            .await
            .assured("the upgrade succeeds");
        for answer in ["{\"ready\":true}", "{\"user_id\":2}"] {
            peer.send(Message::Text(answer.to_string()))
                .await
                .assured("the peer writes its answer");
        }
        peer
    };
    let (resumed, _answering_peer) = timeout(Duration::from_secs(60), async {
        tokio::join!(ingestor.resume(), answering)
    })
    .await
    .assured("the next handshake completes within the budget");
    assert_eq!(
        resumed.assured("the next handshake completes"),
        IngestorResume::Ready
    );
    let batch = ingestor
        .next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .await
        .assured("the replacement connection delivers its payload");
    let IngestorBatch::Messages(messages) = batch else {
        panic!("the replacement connection delivers a data frame");
    };
    let payloads: Vec<&[u8]> = messages.iter().map(|message| message.payload()).collect();
    assert_eq!(
        payloads,
        [b"{\"user_id\":2}".as_slice()],
        "the refused attempt's accepted frame never reaches a read"
    );
}

#[nervix_primitives::test]
async fn a_failed_handshake_never_waits_for_a_transport_that_cannot_take_its_close() {
    // A subscription larger than the socket buffers on both sides of a peer that reads nothing
    // leaves its write pending, and the transport full, until the protocol's TIMEOUT.
    let protocol = compiled(
        SignalingWireFormat::Json,
        subscribing("\"x\" * 16777216", &[], None, "200ms"),
    );
    let (mut ingestor, listener) = tests::signaling_ingestor(Vec::new(), protocol).await;
    let holding = async {
        let (stream, _) = listener.accept().await.assured("the ingestor connects");
        tokio_tungstenite::accept_async(stream)
            .await
            .assured("the upgrade succeeds")
    };
    let (resumed, _unread_peer) = timeout(Duration::from_secs(60), async {
        tokio::join!(ingestor.resume(), holding)
    })
    .await
    .assured("the failed handshake returns although its transport cannot take the Close");
    let Err(report) = resumed else {
        panic!("a peer that reads nothing cannot complete the handshake");
    };
    // No step was waiting yet: the TIMEOUT ended the handshake while the subscription's write was
    // still pending, so the Close met a transport that could take nothing more.
    assert!(
        matches!(
            report.downcast_ref::<WebsocketSignalingError>(),
            Some(WebsocketSignalingError::Timeout { unsatisfied, .. }) if unsatisfied.is_empty()
        ),
        "{report:#}"
    );
    assert!(ingestor.needs_resume());
}

#[test]
fn every_handshake_failure_names_the_close_it_earns() {
    let close_bearing = [
        // A read that fails while signaling keeps the read's own classification.
        (
            WebsocketSignalingError::Receive(Box::new(WebsocketError::Capacity(
                CapacityError::MessageTooLong {
                    size: 8,
                    max_size: 4,
                },
            ))),
            CloseCode::Size,
        ),
        (
            WebsocketSignalingError::Receive(Box::new(WebsocketError::Utf8)),
            CloseCode::Invalid,
        ),
        (
            WebsocketSignalingError::Receive(Box::new(WebsocketError::Protocol(
                ProtocolError::NonZeroReservedBits,
            ))),
            CloseCode::Protocol,
        ),
        (
            WebsocketSignalingError::Rejected {
                matcher: ".error".to_string(),
                reason: "subscription refused".to_string(),
            },
            CloseCode::Policy,
        ),
        (
            WebsocketSignalingError::Timeout {
                timeout: Duration::from_secs(1),
                unsatisfied: vec![".ready".to_string()],
            },
            CloseCode::Away,
        ),
        // This client's own programs failed the handshake.
        (
            WebsocketSignalingError::SendProgram {
                index: 1,
                reason: "the program produced no value".to_string(),
            },
            CloseCode::Error,
        ),
        (
            WebsocketSignalingError::SendEncode {
                index: 1,
                format: "RAW",
                reason: "RAW frames carry strings".to_string(),
            },
            CloseCode::Error,
        ),
        (
            WebsocketSignalingError::Capture {
                capture: ".session".to_string(),
                reason: "CAPTURE program must produce an object".to_string(),
            },
            CloseCode::Error,
        ),
    ];
    for (failure, status) in close_bearing {
        assert_eq!(
            ConnectionEnding::from(&failure),
            ConnectionEnding::Close(status),
            "{failure:?}"
        );
    }
    // These endings offer no new Close. Resume flushes a peer Close's queued reply separately.
    let released = [
        WebsocketSignalingError::Receive(Box::new(WebsocketError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake,
        ))),
        WebsocketSignalingError::Receive(Box::new(WebsocketError::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )))),
        WebsocketSignalingError::Send(Box::new(WebsocketError::Io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        )))),
        WebsocketSignalingError::Send(Box::new(WebsocketError::WriteBufferFull(Message::Ping(
            Vec::new(),
        )))),
        WebsocketSignalingError::Send(Box::new(WebsocketError::Protocol(
            ProtocolError::SendAfterClosing,
        ))),
        WebsocketSignalingError::Send(Box::new(WebsocketError::AlreadyClosed)),
        WebsocketSignalingError::PeerClosed {
            code: CloseCode::Policy,
            reason: "subscription refused".to_string(),
        },
        WebsocketSignalingError::PeerClosedWithoutStatus,
        WebsocketSignalingError::Ended,
    ];
    for failure in released {
        assert_eq!(
            ConnectionEnding::from(&failure),
            ConnectionEnding::Release,
            "{failure:?}"
        );
    }
}
