//! Real-transport checks of the connection's frame and header ownership, configured pings and the
//! Close a silent peer is offered when the idle deadline ends its connection.
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
use tokio::io::AsyncReadExt as _;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebsocketError,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};

use super::*;

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
        assert!(report.contains::<tokio_tungstenite::tungstenite::Error>());
        assert!(matches!(
            report.current_context(),
            IngestorError::Read { .. }
        ));
        assert!(ingestor.needs_resume());
    }
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
