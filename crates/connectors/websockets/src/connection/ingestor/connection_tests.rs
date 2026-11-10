//! Real-transport checks of the connection's frame and header ownership, configured pings and the
//! Close a peer is offered when the idle deadline or a refused frame ends its connection.
//!
//! Layer: test harness.
//! - **Owns.** Observing the production connection owner through a local protocol peer, and
//!   reading again through the WebSocket driver what a peer that never read was sent.
//! - **Depends on.** The ingestor's fixture, connector contract and execution primitives.
//! - **Must not know.** Registry, graph or host synchronization internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test boundary declared above")
)]

use futures_util::FutureExt as _;
use nervix_primitives::task::unconstrained;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError,
        error::{CapacityError, TlsError},
        protocol::{
            CloseFrame, Role,
            frame::{
                Frame, FrameHeader,
                coding::{CloseCode, Control, Data, OpCode},
            },
        },
    },
};

use super::{connection::FailedReadEnding, *};

#[nervix_primitives::test]
async fn a_message_retains_its_upgrade_snapshot_and_exact_data_frame_kind() {
    struct FrameCase {
        frame: Message,
        kind: nervix_models::WebsocketsFrameType,
        payload: &'static [u8],
    }
    for case in [
        FrameCase {
            frame: Message::Text("payload".to_string()),
            kind: nervix_models::WebsocketsFrameType::Text,
            payload: b"payload",
        },
        FrameCase {
            frame: Message::Binary(vec![0, 255, 42]),
            kind: nervix_models::WebsocketsFrameType::Binary,
            payload: &[0, 255, 42],
        },
    ] {
        let headers = WebsocketsUpgradeHeaders::parse(&[nervix_models::WebsocketsUpgradeHeader {
            name: "Authorization".to_string(),
            value: "Bearer fixture".to_string(),
        }])
        .assured("Authorization is an application field with a valid value");
        let (mut ingestor, mut peer) = tests::connected_pair_with(Vec::new(), headers).await;
        let snapshot = ingestor
            .connection
            .as_ref()
            .assured("the pair is connected")
            .headers
            .clone();
        peer.send(case.frame)
            .await
            .assured("the peer sends the data frame");
        let batch = ingestor
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await
            .assured("one data frame is admitted");
        let IngestorBatch::Messages(mut messages) = batch else {
            panic!("the peer delivered a data frame");
        };
        assert_eq!(messages.len(), 1);
        let message = messages
            .pop()
            .verified("the batch contains exactly one message");
        drop(ingestor);
        drop(peer);
        assert_eq!(message.payload(), case.payload);
        let metadata = message.metadata();
        assert_eq!(
            nervix_connector::RetainedIngestMetadata::capture(metadata.fields).values(),
            [nervix_connector::RetainedIngestMetadataValue::String(
                case.kind.as_ref().to_string()
            )]
        );
        assert!(metadata.position.is_none());
        let IngestRowHeaders::Shared(headers) = metadata.headers else {
            panic!("a connection frame shares its connection's header snapshot");
        };
        assert!(Arc::ptr_eq(headers, &snapshot));
        let mut seen = Vec::new();
        message
            .headers()
            .visit(&mut |name, value| seen.push((name.to_string(), value.to_string())));
        assert!(seen.contains(&("x-nervix-upgrade".to_string(), "fixture".to_string())));
    }
}

#[nervix_primitives::test]
async fn a_configured_ping_is_written_while_the_ingestor_waits_for_data() {
    let config = vec![ClientConfigEntry {
        key: "ping_interval_ms".to_string(),
        value: "10".to_string(),
    }];
    let (mut ingestor, mut peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    let cancellation = IngestorReadCancellation::new();
    let read = ingestor.next_batch(
        IngestorBatchRequest {
            max_messages: std::num::NonZeroUsize::MIN,
            batch_timeout: None,
        },
        &cancellation,
    );
    let observe = async {
        let frame = timeout(Duration::from_secs(60), peer.next())
            .await
            .assured("the peer sees a configured ping within the fixture budget")
            .assured("the client keeps the connection open")
            .assured("the frame is valid");
        assert!(matches!(frame, Message::Ping(_)));
        cancellation.cancel();
    };
    let (batch, ()) = tokio::join!(read, observe);
    assert!(matches!(
        batch.assured("a cancellation is an ordinary read outcome"),
        IngestorBatch::Interrupted { .. }
    ));
    assert!(!ingestor.needs_resume());
}

#[nervix_primitives::test]
async fn a_silent_peer_expires_with_the_explicit_or_ping_derived_idle_bound() {
    for explicit in [false, true] {
        let mut config = vec![ClientConfigEntry {
            key: "ping_interval_ms".to_string(),
            value: "10".to_string(),
        }];
        if explicit {
            config.push(ClientConfigEntry {
                key: "idle_timeout_ms".to_string(),
                value: "100".to_string(),
            });
        }
        let (mut ingestor, _silent_peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        let result = timeout(
            Duration::from_secs(60),
            ingestor.next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            ),
        )
        .await
        .assured("the ingestor's idle bound ends its read");
        let Err(report) = result else {
            panic!("a peer that sends no frame must expire");
        };
        let rendered = format!("{report:#}");
        if explicit {
            assert!(report.contains::<nervix_connector::DeadlineElapsed>());
            assert!(rendered.contains("idle_timeout_ms 100ms"), "{rendered}");
        } else {
            assert!(report.contains::<WebsocketsConnectionIngestorFailure>());
            assert!(rendered.contains("30ms idle read deadline"), "{rendered}");
        }
        assert!(ingestor.needs_resume());
    }
}

#[nervix_primitives::test]
async fn a_responsive_peer_survives_a_handoff_past_the_idle_deadline() {
    use nervix_primitives::time::{advance, pause, resume};

    for explicit in [false, true] {
        let mut config = vec![ClientConfigEntry {
            key: "ping_interval_ms".to_string(),
            value: "10000".to_string(),
        }];
        if explicit {
            config.push(ClientConfigEntry {
                key: "idle_timeout_ms".to_string(),
                value: "30000".to_string(),
            });
        }
        let (mut ingestor, mut peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        peer.send(Message::Text("before handoff".to_string()))
            .await
            .assured("the peer supplies the payload the host hands off");
        let request = IngestorBatchRequest {
            max_messages: std::num::NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let cancellation = IngestorReadCancellation::new();
        let first = ingestor
            .next_batch(request, &cancellation)
            .await
            .assured("the first payload reaches the host");
        let IngestorBatch::Messages(first) = first else {
            panic!("the peer supplied the first payload");
        };
        assert_eq!(first[0].payload(), b"before handoff");

        // This is the elapsed-time behavior under test: the host owns its payload and does not
        // poll another read, so no client ping can be written during the handoff.
        pause();
        advance(Duration::from_secs(120)).await;
        resume();
        let connection = ingestor
            .connection
            .as_mut()
            .assured("the connection is retained");
        let resumed = connection
            .next()
            .await
            .assured("a missed client ping is offered before blaming the responsive peer");
        assert!(matches!(resumed, WebsocketRead::Ping));
        // Cancellation before a selected ping finishes writing must not consume its response
        // window. Another handoff can elapse before the caller gets back to that same probe.
        pause();
        advance(Duration::from_secs(120)).await;
        resume();
        let offered_again = ingestor
            .connection
            .as_mut()
            .assured("the unwritten probe retains the connection")
            .next()
            .await
            .assured("an unwritten probe keeps its response opportunity");
        assert!(matches!(offered_again, WebsocketRead::Ping));
        assert!(matches!(
            ingestor
                .send_ping(&cancellation)
                .await
                .assured("the probe is written"),
            WebsocketPingOutcome::Sent
        ));

        let answer = async {
            loop {
                nervix_primitives::task::consume_budget().await;
                let frame = peer
                    .next()
                    .await
                    .assured("the same connection stays open")
                    .assured("the client writes a valid frame");
                if let Message::Ping(_) = frame {
                    peer.flush().await.assured("the peer answers the ping");
                    peer.send(Message::Text("after handoff".to_string()))
                        .await
                        .assured("the responsive peer supplies the next payload");
                    return;
                }
            }
        };
        let (next, ()) = timeout(Duration::from_secs(60), async {
            tokio::join!(ingestor.next_batch(request, &cancellation), answer)
        })
        .await
        .assured("the probe and its payload complete within the fixture budget");
        let IngestorBatch::Messages(next) = next.assured("the resumed read succeeds") else {
            panic!("the peer supplied the second payload");
        };
        assert_eq!(next[0].payload(), b"after handoff");
        assert!(!ingestor.needs_resume());
    }
}

#[nervix_primitives::test]
async fn a_cancelled_read_keeps_the_response_deadline_of_its_written_catch_up_ping() {
    use nervix_primitives::time::{advance, pause, resume};

    let config = vec![ClientConfigEntry {
        key: "ping_interval_ms".to_string(),
        value: "10000".to_string(),
    }];
    let (mut ingestor, _silent_peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    pause();
    advance(Duration::from_secs(120)).await;
    resume();
    let read = ingestor
        .connection
        .as_mut()
        .assured("the pair is connected")
        .next()
        .await
        .assured("the missed ping is offered before the idle verdict");
    assert!(matches!(read, WebsocketRead::Ping));
    assert!(matches!(
        ingestor
            .send_ping(&IngestorReadCancellation::new())
            .await
            .assured("the catch-up ping is written"),
        WebsocketPingOutcome::Sent
    ));

    pause();
    advance(Duration::from_secs(10)).await;
    resume();
    let next_ping = ingestor
        .connection
        .as_mut()
        .assured("the response window retains the connection")
        .next()
        .await
        .assured("normal ping cadence continues inside the response window");
    assert!(matches!(next_ping, WebsocketRead::Ping));
    assert!(matches!(
        ingestor
            .send_ping(&IngestorReadCancellation::new())
            .await
            .assured("the later ping is written"),
        WebsocketPingOutcome::Sent
    ));

    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    let request = IngestorBatchRequest {
        max_messages: std::num::NonZeroUsize::MIN,
        batch_timeout: None,
    };
    let interrupted = ingestor
        .next_batch(request, &cancellation)
        .await
        .assured("read cancellation is an ordinary outcome");
    assert!(matches!(interrupted, IngestorBatch::Interrupted { .. }));
    pause();
    advance(Duration::from_secs(25)).await;
    resume();
    // Thirty-five seconds have passed since the probe, but only twenty-five since the next
    // ping. A renewed window would still wait; the retained deadline must expire immediately.
    let result = futures_util::FutureExt::now_or_never(
        ingestor.next_batch(request, &IngestorReadCancellation::new()),
    )
    .assured("the original response deadline ends a silent peer without another wait");
    let Err(report) = result else {
        panic!("cancellation and further overdue pings cannot renew an unanswered probe");
    };
    assert!(report.contains::<WebsocketsConnectionIngestorFailure>());
    assert!(ingestor.needs_resume());
}

#[nervix_primitives::test]
async fn a_connection_without_client_pings_keeps_its_physical_idle_deadline_after_handoff() {
    use nervix_primitives::time::{advance, pause, resume};

    let config = vec![ClientConfigEntry {
        key: "idle_timeout_ms".to_string(),
        value: "30000".to_string(),
    }];
    let (mut ingestor, _silent_peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    pause();
    advance(Duration::from_secs(120)).await;
    resume();
    let result = ingestor
        .next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .await;
    let Err(report) = result else {
        panic!("without a client ping cadence only inbound activity can extend the deadline");
    };
    assert!(report.contains::<nervix_connector::DeadlineElapsed>());
    assert!(ingestor.needs_resume());
}

#[nervix_primitives::test]
async fn inbound_message_and_frame_limits_end_the_exact_connection_with_the_driver_cause() {
    for key in ["max_message_size", "max_frame_size"] {
        let config = vec![ClientConfigEntry {
            key: key.to_string(),
            value: "4".to_string(),
        }];
        let (mut ingestor, mut peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        peer.send(Message::Binary(vec![0; 8]))
            .await
            .assured("the peer can send its small test frame");
        let result = ingestor
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await;
        let Err(report) = result else {
            panic!("the inbound bytes exceed the declared limit");
        };
        assert!(
            matches!(
                report.downcast_ref::<WebsocketError>(),
                Some(WebsocketError::Capacity(CapacityError::MessageTooLong {
                    size: 8,
                    max_size: 4,
                }))
            ),
            "{key}: {report:#}"
        );
        assert!(matches!(
            report.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(ingestor.needs_resume());
        // The client read the whole frame before refusing it, so TCP ends in order behind the
        // Close it offered.
        let ending = timeout(Duration::from_secs(60), unread_ending(peer))
            .await
            .assured("the client releases the connection within the fixture budget");
        let UnreadEnding::Closed(Some(frame)) = ending else {
            panic!("a {key} overflow ends the connection with a Close frame before TCP ends");
        };
        assert_eq!(frame.code, CloseCode::Size, "{key}");
    }
}

/// A frame RFC 6455 forbids a peer to send, with the violation the client's transport names.
struct ViolatingFrame {
    frame: Frame,
    violation: ProtocolError,
}

/// Frames that break RFC 6455 section 5, each written as is through the peer's own transport.
fn violating_frames() -> [ViolatingFrame; 3] {
    let mut reserved_bit = Frame::message(b"payload".to_vec(), OpCode::Data(Data::Binary), true);
    reserved_bit.header_mut().rsv1 = true;
    let mut fragmented_ping = Frame::ping(Vec::new());
    fragmented_ping.header_mut().is_final = false;
    [
        ViolatingFrame {
            frame: reserved_bit,
            violation: ProtocolError::NonZeroReservedBits,
        },
        ViolatingFrame {
            frame: Frame::message(b"payload".to_vec(), OpCode::Data(Data::Continue), true),
            violation: ProtocolError::UnexpectedContinueFrame,
        },
        ViolatingFrame {
            frame: fragmented_ping,
            violation: ProtocolError::FragmentedControlFrame,
        },
    ]
}

#[nervix_primitives::test]
async fn a_peer_protocol_violation_reads_a_protocol_error_close_and_keeps_the_driver_cause() {
    for case in violating_frames() {
        let (mut ingestor, mut peer) =
            tests::connected_pair_with(Vec::new(), WebsocketsUpgradeHeaders::default()).await;
        peer.send(Message::Frame(case.frame))
            .await
            .assured("the peer's transport writes the frame as given");
        let result = ingestor
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await;
        let Err(report) = result else {
            panic!("{:?} fails the read", case.violation);
        };
        let Some(WebsocketError::Protocol(violation)) = report.downcast_ref::<WebsocketError>()
        else {
            panic!("the read keeps the transport's typed violation: {report:#}");
        };
        assert_eq!(violation, &case.violation);
        assert!(matches!(
            report.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(ingestor.needs_resume());
        let ending = timeout(Duration::from_secs(60), unread_ending(peer))
            .await
            .assured("the client releases the connection within the fixture budget");
        let UnreadEnding::Closed(Some(frame)) = ending else {
            panic!(
                "{:?} ends the connection with a Close frame before TCP ends",
                case.violation
            );
        };
        assert_eq!(frame.code, CloseCode::Protocol, "{:?}", case.violation);
    }
}

#[nervix_primitives::test]
async fn text_that_is_not_utf8_reads_an_invalid_payload_close_and_keeps_the_driver_cause() {
    // An invalid two-byte sequence, as message text and as the reason of a Close the peer sends.
    let not_utf8 = [
        Frame::message(vec![0xC3, 0x28], OpCode::Data(Data::Text), true),
        // Status 1000, then the reason.
        Frame::from_payload(
            FrameHeader {
                opcode: OpCode::Control(Control::Close),
                ..FrameHeader::default()
            },
            vec![0x03, 0xE8, 0xC3, 0x28],
        ),
    ];
    for frame in not_utf8 {
        let (mut ingestor, mut peer) =
            tests::connected_pair_with(Vec::new(), WebsocketsUpgradeHeaders::default()).await;
        peer.send(Message::Frame(frame))
            .await
            .assured("the peer's transport writes the frame as given");
        let result = ingestor
            .next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            )
            .await;
        let Err(report) = result else {
            panic!("text that is not UTF-8 fails the read");
        };
        assert!(
            matches!(
                report.downcast_ref::<WebsocketError>(),
                Some(WebsocketError::Utf8)
            ),
            "{report:#}"
        );
        assert!(matches!(
            report.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(ingestor.needs_resume());
        let ending = timeout(Duration::from_secs(60), unread_ending(peer))
            .await
            .assured("the client releases the connection within the fixture budget");
        let UnreadEnding::Closed(Some(frame)) = ending else {
            panic!("text that is not UTF-8 ends the connection with a Close frame before TCP ends");
        };
        assert_eq!(frame.code, CloseCode::Invalid);
    }
}

#[nervix_primitives::test]
async fn a_peer_that_ends_tcp_first_is_released_without_a_close() {
    let (mut ingestor, mut peer) =
        tests::connected_pair_with(Vec::new(), WebsocketsUpgradeHeaders::default()).await;
    // The peer ends its half of TCP and keeps reading what the client sends after.
    peer.get_mut()
        .shutdown()
        .await
        .assured("a connected socket ends its sending half");
    let result = ingestor
        .next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .await;
    let Err(report) = result else {
        panic!("TCP ended without the closing handshake");
    };
    assert!(
        matches!(
            report.downcast_ref::<WebsocketError>(),
            Some(WebsocketError::Protocol(
                ProtocolError::ResetWithoutClosingHandshake
            ))
        ),
        "{report:#}"
    );
    assert!(ingestor.needs_resume());
    let ending = timeout(Duration::from_secs(60), unread_ending(peer))
        .await
        .assured("the client releases the connection within the fixture budget");
    assert!(
        matches!(ending, UnreadEnding::WithoutClose),
        "a connection whose TCP already ended is released without a Close"
    );
}

/// How the client ended a connection whose peer never read it.
enum UnreadEnding {
    /// A Close frame, with the status it carried, came before the end of TCP.
    Closed(Option<CloseFrame<'static>>),
    /// TCP ended with no Close frame before it.
    WithoutClose,
}

/// Reads everything TCP delivered to a peer that never read its connection, once the client has
/// released it, then reads it again through the WebSocket driver. Nothing is written back, so the
/// pongs the driver queues for unanswered pings cannot fail the reading. The calling test bounds
/// the wait for the client's release.
async fn unread_ending(mut peer: WebSocketStream<TcpStream>) -> UnreadEnding {
    let mut delivered = Vec::new();
    peer.get_mut()
        .read_to_end(&mut delivered)
        .await
        .assured("the peer sent the client nothing to leave unread, so TCP ends in order");
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
            Some(Ok(Message::Ping(_))) => {}
            Some(Err(WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {
                return UnreadEnding::WithoutClose;
            }
            other => panic!("a silent connection carries only pings and a Close, not {other:?}"),
        }
    }
}

#[nervix_primitives::test]
async fn a_silent_peer_reads_a_going_away_close_before_its_connection_ends() {
    for explicit in [false, true] {
        let mut config = vec![ClientConfigEntry {
            key: "ping_interval_ms".to_string(),
            value: "10".to_string(),
        }];
        if explicit {
            config.push(ClientConfigEntry {
                key: "idle_timeout_ms".to_string(),
                value: "100".to_string(),
            });
        }
        let (mut ingestor, silent_peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        let result = timeout(
            Duration::from_secs(60),
            ingestor.next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            ),
        )
        .await
        .assured("the ingestor's idle bound ends its read");
        assert!(result.is_err(), "a peer that sends no frame must expire");
        assert!(ingestor.needs_resume());
        let ending = timeout(Duration::from_secs(60), unread_ending(silent_peer))
            .await
            .assured("the client releases the connection within the fixture budget");
        let UnreadEnding::Closed(Some(frame)) = ending else {
            panic!("the idle deadline ends the connection with a Close frame before TCP ends");
        };
        assert_eq!(frame.code, CloseCode::Away);
    }
}

#[nervix_primitives::test]
async fn the_idle_verdict_never_waits_for_a_transport_that_cannot_take_its_close() {
    const IDLE: Duration = Duration::from_millis(1);
    let config = vec![ClientConfigEntry {
        key: "idle_timeout_ms".to_string(),
        value: IDLE.as_millis().to_string(),
    }];
    let (mut ingestor, _unread_peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    let connection = ingestor
        .connection
        .as_mut()
        .assured("the pair is connected");
    // The peer reads nothing, so the client's transport fills until it takes no more. Each
    // write is polled outside the cooperative budget, so only a full transport refuses one.
    let chunk = vec![0_u8; 64 * 1024];
    loop {
        nervix_primitives::task::consume_budget().await;
        let written =
            unconstrained(connection.relay.send(Message::Binary(chunk.clone()))).now_or_never();
        match written {
            Some(Ok(())) => {}
            Some(Err(error)) => panic!("the unread transport stays open: {error}"),
            None => break,
        }
    }
    nervix_primitives::time::sleep(IDLE).await;
    // A fresh cooperative budget for the one poll below.
    nervix_primitives::task::yield_now().await;
    let verdict = ingestor
        .next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .now_or_never();
    let Some(Err(report)) = verdict else {
        panic!("the idle verdict returns at once although the transport cannot take its Close");
    };
    assert!(report.contains::<nervix_connector::DeadlineElapsed>());
    assert!(ingestor.needs_resume());
}

#[test]
fn a_failed_read_offers_a_close_only_for_what_the_peer_sent() {
    let close_bearing = [
        (
            WebsocketError::Capacity(CapacityError::MessageTooLong {
                size: 8,
                max_size: 4,
            }),
            CloseCode::Size,
        ),
        (WebsocketError::Utf8, CloseCode::Invalid),
        (
            WebsocketError::Protocol(ProtocolError::NonZeroReservedBits),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::MaskedFrameFromServer),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::FragmentedControlFrame),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::ControlFrameTooBig),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::UnknownControlFrameType(11)),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::UnknownDataFrameType(3)),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::UnexpectedContinueFrame),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::ExpectedFragment(Data::Text)),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::InvalidOpcode(15)),
            CloseCode::Protocol,
        ),
        (
            WebsocketError::Protocol(ProtocolError::InvalidCloseSequence),
            CloseCode::Protocol,
        ),
    ];
    for (failure, status) in close_bearing {
        assert_eq!(
            FailedReadEnding::from(&failure),
            FailedReadEnding::Close(status),
            "{failure}"
        );
    }
    // The transport broke, already ended or is already closing: no frame reaches the peer.
    let released = [
        WebsocketError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        WebsocketError::Tls(TlsError::InvalidDnsName),
        WebsocketError::ConnectionClosed,
        WebsocketError::AlreadyClosed,
        WebsocketError::WriteBufferFull(Message::Ping(Vec::new())),
        WebsocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
        WebsocketError::Protocol(ProtocolError::ReceivedAfterClosing),
        WebsocketError::Protocol(ProtocolError::SendAfterClosing),
        // Only the opening upgrade fails with these.
        WebsocketError::Capacity(CapacityError::TooManyHeaders),
        WebsocketError::AttackAttempt,
        WebsocketError::Protocol(ProtocolError::HandshakeIncomplete),
        WebsocketError::Protocol(ProtocolError::SecWebSocketAcceptKeyMismatch),
    ];
    for failure in released {
        assert_eq!(
            FailedReadEnding::from(&failure),
            FailedReadEnding::Release,
            "{failure}"
        );
    }
}

#[nervix_primitives::test]
async fn a_failed_read_never_waits_for_a_transport_that_cannot_take_its_close() {
    let config = vec![ClientConfigEntry {
        key: "max_message_size".to_string(),
        value: "4".to_string(),
    }];
    let (mut ingestor, mut unread_peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    // The oversized message is in the client's socket before its transport fills.
    unread_peer
        .send(Message::Binary(vec![0; 8]))
        .await
        .assured("the peer can send its small test frame");
    let connection = ingestor
        .connection
        .as_mut()
        .assured("the pair is connected");
    // The peer reads nothing, so the client's transport fills until it takes no more. Each
    // write is polled outside the cooperative budget, so only a full transport refuses one.
    let chunk = vec![0_u8; 64 * 1024];
    loop {
        nervix_primitives::task::consume_budget().await;
        let written =
            unconstrained(connection.relay.send(Message::Binary(chunk.clone()))).now_or_never();
        match written {
            Some(Ok(())) => {}
            Some(Err(error)) => panic!("the unread transport stays open: {error}"),
            None => break,
        }
    }
    // A fresh cooperative budget for the one poll below.
    nervix_primitives::task::yield_now().await;
    let verdict = ingestor
        .next_batch(
            IngestorBatchRequest {
                max_messages: std::num::NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &IngestorReadCancellation::new(),
        )
        .now_or_never();
    let Some(Err(report)) = verdict else {
        panic!("the failed read returns at once although the transport cannot take its Close");
    };
    assert!(
        matches!(
            report.downcast_ref::<WebsocketError>(),
            Some(WebsocketError::Capacity(
                CapacityError::MessageTooLong { .. }
            ))
        ),
        "{report:#}"
    );
    assert!(ingestor.needs_resume());
}
