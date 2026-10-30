//! Real-transport checks of the connection's frame and header ownership and configured pings.
//!
//! Layer: test harness.
//! - **Owns.** Observing the production connection owner through a local protocol peer.
//! - **Depends on.** The source's fixture, connector contract and execution primitives.
//! - **Must not know.** Registry, graph or host synchronization internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test boundary declared above")
)]

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
        let (mut source, mut peer) = tests::connected_pair_with(Vec::new(), headers).await;
        let snapshot = source
            .connection
            .as_ref()
            .assured("the pair is connected")
            .headers
            .clone();
        peer.send(case.frame)
            .await
            .assured("the peer sends the data frame");
        let batch = source
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
        drop(source);
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
async fn a_configured_ping_is_written_while_the_source_waits_for_data() {
    let config = vec![ClientConfigEntry {
        key: "ping_interval_ms".to_string(),
        value: "10".to_string(),
    }];
    let (mut source, mut peer) =
        tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
    let cancellation = IngestorReadCancellation::new();
    let read = source.next_batch(
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
    assert!(!source.needs_resume());
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
        let (mut source, _silent_peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        let result = timeout(
            Duration::from_secs(60),
            source.next_batch(
                IngestorBatchRequest {
                    max_messages: std::num::NonZeroUsize::MIN,
                    batch_timeout: None,
                },
                &IngestorReadCancellation::new(),
            ),
        )
        .await
        .assured("the source's idle bound ends its read");
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
        assert!(source.needs_resume());
    }
}

#[nervix_primitives::test]
async fn inbound_message_and_frame_limits_end_the_exact_connection_with_the_driver_cause() {
    for key in ["max_message_size", "max_frame_size"] {
        let config = vec![ClientConfigEntry {
            key: key.to_string(),
            value: "4".to_string(),
        }];
        let (mut source, mut peer) =
            tests::connected_pair_with(config, WebsocketsUpgradeHeaders::default()).await;
        peer.send(Message::Binary(vec![0; 8]))
            .await
            .assured("the peer can send its small test frame");
        let result = source
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
        assert!(source.needs_resume());
    }
}
