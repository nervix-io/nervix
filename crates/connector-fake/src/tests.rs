//! Qualification of scripts and complete observations before hosts use the fixtures.
//!
//! Layer: test harness.
//! - **Owns.** Fixture behavior at each connector boundary.
//! - **Depends on.** The fake connector and the contract it implements.
//! - **Must not know.** Runtime internals or real transports.

use std::{num::NonZeroUsize, time::Duration};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::*;
use nervix_models::Timestamp;
use nervix_primitives::sync::{Arc, StdArc, blocking::Mutex, mpsc};

use crate::*;

#[nervix_primitives::test]
async fn opening_consumes_only_the_selected_instance() {
    let observed = Arc::new(Mutex::new(IngestorObservations::default()));
    let plan = FakeIngestorPlan::new(std::collections::BTreeMap::from([
        (0, Ok(FakeIngestor::empty(observed))),
        (
            1,
            Err(Report::new(IngestorError::Open {
                connector: "scripted refusal",
            })),
        ),
    ]));
    let mut opened = FakeIngestor::open(&plan, 0)
        .await
        .assured("instance zero is configured");
    opened
        .close()
        .await
        .assured("the configured instance closes");
    assert!(
        FakeIngestor::open(&plan, 0).await.is_err(),
        "an instance opens once"
    );
    let error = match FakeIngestor::open(&plan, 1).await {
        Err(error) => error,
        Ok(_) => panic!("instance one's refusal must survive opening instance zero"),
    };
    assert_eq!(
        *error.current_context(),
        IngestorError::Open {
            connector: "scripted refusal"
        }
    );
}

fn record() -> EmitterRecord {
    EmitterRecord::new(
        EmitterRecordId::new(7),
        Some("key".to_string()),
        vec![0, 255, 1],
        vec![
            ("same".to_string(), "first".to_string()),
            ("same".to_string(), "second".to_string()),
        ],
        Timestamp::from_unix_nanos(42),
    )
    .with_message_group("group".to_string())
}

#[nervix_primitives::test]
async fn record_observations_keep_the_complete_envelope_on_every_attempt() {
    let mut connector =
        FakeEmitter::<Records>::new([PublishAnswer::Fail, PublishAnswer::ConfirmAll]);
    let observed = connector.observations();
    let failed = connector.publish(vec![record()]).await.into_parts();
    assert!(failed.delivered.is_empty());
    assert!(failed.rejected.is_empty());
    assert!(failed.infrastructure_error.is_some());
    let delivered = connector.publish(vec![record()]).await.into_parts();
    assert_eq!(delivered.delivered, [EmitterRecordId::new(7)]);
    assert!(delivered.rejected.is_empty());
    assert!(delivered.infrastructure_error.is_none());
    let observed = observed.lock();
    assert_eq!(observed.writes.len(), 2);
    for write in &observed.writes {
        let [record] = write.as_slice() else {
            panic!("each attempt carries one record");
        };
        assert_eq!(record.id, EmitterRecordId::new(7));
        assert_eq!(record.key.as_deref(), Some("key"));
        assert_eq!(record.payload, [0, 255, 1]);
        assert_eq!(
            record.headers,
            [
                ("same".to_string(), "first".to_string()),
                ("same".to_string(), "second".to_string())
            ]
        );
        assert_eq!(record.message_group.as_deref(), Some("group"));
        assert_eq!(record.occurred_at, Timestamp::from_unix_nanos(42));
    }
}

#[nervix_primitives::test]
async fn scripts_distinguish_rejection_unanswered_members_and_retry_hints() {
    let mut connector = FakeEmitter::<Records>::new([
        PublishAnswer::Members(vec![
            MemberAnswer::Deliver,
            MemberAnswer::Reject("refused".to_string()),
            MemberAnswer::Unanswered,
        ]),
        PublishAnswer::RetryAfter(Duration::from_secs(3)),
    ]);
    let mut records = vec![record(), record(), record()];
    for (index, record) in records.iter_mut().enumerate() {
        record.id = EmitterRecordId::new(index);
    }
    let answers = connector.publish(records).await.into_parts();
    assert_eq!(answers.delivered, [EmitterRecordId::new(0)]);
    assert_eq!(answers.rejected.len(), 1);
    assert_eq!(answers.rejected[0].id, EmitterRecordId::new(1));
    assert_eq!(answers.rejected[0].error.message, "refused");
    assert!(answers.infrastructure_error.is_none());
    let retry = connector
        .publish(vec![record()])
        .await
        .into_parts()
        .infrastructure_error
        .assured("the script requests a retry");
    assert_eq!(
        retry
            .downcast_ref::<EmitterRetryDelay>()
            .assured("the typed delay survives the boundary")
            .0,
        Duration::from_secs(3)
    );
}

#[nervix_primitives::test]
async fn a_commit_failure_retains_staged_rows_until_the_successful_commit() {
    let mut connector = FakeEmitter::<Rows>::new(PublishScript::Repeat(PublishAnswer::ConfirmAll))
        .staging(Duration::from_secs(5), 2);
    let column: ArrayRef = StdArc::new(Int64Array::from(vec![10, 20]));
    let batch =
        RecordBatch::try_from_iter([("value", column)]).assured("the column supplies its schema");
    let rows = MappedEmitterRows {
        target_columns: &["value".to_string()],
        carriers: vec![MappedEmitterCarrier {
            batch_index: 3,
            batch: &batch,
            selected_rows: &[1, 0],
            occurred_at: Timestamp::from_unix_nanos(42),
            acknowledgements: None,
        }],
    };
    let outcome = connector.publish(rows).await.into_parts();
    assert_eq!(
        outcome.delivered,
        [
            EmitterRecordPosition {
                batch_index: 3,
                row_index: 1
            },
            EmitterRecordPosition {
                batch_index: 3,
                row_index: 0
            }
        ]
    );
    assert_eq!(connector.staged_messages(), 2);
    assert_eq!(
        connector.staged_commit(),
        Some(EmitterStagedCommit::SizeReached)
    );
    connector
        .commit_errors
        .push_back(Report::new(EmitterWriteError::Commit {
            connector: "scripted",
        }));
    assert!(connector.commit().await.is_err());
    assert_eq!(connector.staged_messages(), 2);
    let report = connector
        .commit()
        .await
        .assured("the second commit succeeds")
        .assured("the rows remained staged");
    assert_eq!(report.messages, 2);
    assert_eq!(report.domain_timestamp, Timestamp::from_unix_nanos(42));
    assert_eq!(connector.staged_messages(), 0);
    let observed = connector.observations();
    let observed = observed.lock();
    assert_eq!(observed.writes[0].columns, ["value"]);
    assert_eq!(observed.writes[0].carriers[0].batch, batch);
    assert_eq!(observed.writes[0].carriers[0].rows, [1, 0]);
    assert_eq!(
        observed.writes[0].carriers[0].occurred_at,
        Timestamp::from_unix_nanos(42)
    );
    assert_eq!(observed.commits.len(), 1);
}

struct Headers;
impl IngestMessageHeaders for Headers {
    fn visit(&self, visitor: &mut dyn FnMut(&str, &str)) {
        visitor("same", "first");
        visitor("same", "second");
    }
}

#[nervix_primitives::test]
async fn queued_messages_keep_headers_metadata_positions_and_the_batch_bound() {
    let observed = Arc::new(Mutex::new(IngestorObservations::default()));
    let mut connector = FakeIngestor::empty(observed.clone());
    let mut message = FakeMessage::new(5, vec![0, 255]);
    message.transport = MessageTransport::Kafka {
        topic: "Orders".to_string(),
        partition: 3,
        offset: 9,
        headers: RetainedIngestHeaders::capture(&Headers),
    };
    connector
        .queued()
        .extend([message, FakeMessage::new(6, vec![1])]);
    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    // Input already ready is retained even when the host cancels the read.
    let batch = connector
        .next_batch(
            IngestorBatchRequest {
                max_messages: NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &cancellation,
        )
        .await
        .assured("queued input is ready");
    let IngestorBatch::Messages(messages) = batch else {
        panic!("ready input is taken");
    };
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert_eq!(*message.position(), 5);
    assert_eq!(message.payload(), [0, 255]);
    let metadata = message.metadata();
    assert_eq!(
        RetainedIngestMetadata::capture(metadata.fields).values(),
        [
            RetainedIngestMetadataValue::String("Orders".to_string()),
            RetainedIngestMetadataValue::I32(3),
            RetainedIngestMetadataValue::I64(9),
        ]
    );
    assert_eq!(
        metadata.position,
        Some(IngestTransportPosition::Kafka {
            topic: "Orders",
            partition: 3,
            offset: 9,
        })
    );
    let mut values = Vec::new();
    metadata
        .headers
        .headers()
        .visit(&mut |name, value| values.push((name.to_string(), value.to_string())));
    assert_eq!(
        values,
        [
            ("same".to_string(), "first".to_string()),
            ("same".to_string(), "second".to_string())
        ]
    );
    connector
        .acknowledge(&[5])
        .await
        .assured("the fixture confirms");
    assert_eq!(observed.lock().acknowledged, [vec![5]]);
    assert_eq!(connector.queued().len(), 1);
}

#[nervix_primitives::test]
async fn a_cancelled_channel_read_returns_and_allows_close() {
    let (_sender, receiver) = mpsc::channel(1);
    let mut connector = FakeIngestor::new(receiver);
    let cancellation = IngestorReadCancellation::new();
    cancellation.cancel();
    let batch = connector
        .next_batch(
            IngestorBatchRequest {
                max_messages: NonZeroUsize::MIN,
                batch_timeout: None,
            },
            &cancellation,
        )
        .await
        .assured("cancellation ends the channel read");
    let IngestorBatch::Interrupted { gathered } = batch else {
        panic!("no message was consumed");
    };
    assert!(gathered.is_empty());
    connector
        .close()
        .await
        .assured("the read completed before close");
}

#[nervix_primitives::test]
async fn scripted_payload_and_finish_failures_preserve_their_complete_outcomes() {
    let mut connector = FakeEmitter::<Records>::new([]);
    let refused = record().rejected("scripted refusal".to_string()).error;
    connector.preparation = PayloadPreparation::Refuse(refused.clone());
    let result = connector.prepare_payload(vec![0, 255], Timestamp::from_unix_nanos(42));
    let Err(actual) = result else {
        panic!("the script refuses preparation");
    };
    assert_eq!(actual, refused);
    let deadline = nervix_primitives::time::Instant::now();
    connector
        .finish_results
        .push_back(Err(Report::new(std::io::Error::other(
            "scripted transport failure",
        ))
        .change_context(EmitterFinishError::Flush {
            connector: "scripted",
        })));
    let Err(failed) = connector.finish(deadline).await else {
        panic!("the script refuses finish");
    };
    assert_eq!(
        *failed.current_context(),
        EmitterFinishError::Flush {
            connector: "scripted"
        }
    );
    assert!(failed.contains::<std::io::Error>());
    let observed = connector.observations();
    let observed = observed.lock();
    assert!(
        observed.writes.is_empty(),
        "a refused preparation reaches no publish"
    );
    assert_eq!(observed.finishes, [deadline]);
}

#[test]
fn transport_metadata_preserves_peer_frame_and_ordered_headers() {
    let expected = [
        ("same".to_string(), "first".to_string()),
        ("same".to_string(), "second".to_string()),
    ];
    let mut plain = FakeMessage::new(16, vec![255, 0]);
    plain.transport = MessageTransport::Headers(RetainedIngestHeaders::capture(&Headers));
    let metadata = plain.metadata();
    assert!(
        RetainedIngestMetadata::capture(metadata.fields)
            .values()
            .is_empty()
    );
    assert!(metadata.position.is_none());
    let mut actual = Vec::new();
    metadata
        .headers
        .headers()
        .visit(&mut |name, value| actual.push((name.to_string(), value.to_string())));
    assert_eq!(actual, expected);

    let peer_addr = std::net::SocketAddr::from(([192, 0, 2, 7], 514));
    let mut syslog = FakeMessage::new(17, vec![0, 255]);
    syslog.transport = MessageTransport::Syslog { peer_addr };
    let metadata = syslog.metadata();
    assert_eq!(
        RetainedIngestMetadata::capture(metadata.fields).values(),
        [RetainedIngestMetadataValue::String(peer_addr.to_string())]
    );
    assert_eq!(
        metadata.position,
        Some(IngestTransportPosition::Syslog { peer: peer_addr })
    );
    let mut syslog_headers = Vec::new();
    syslog.headers().visit(&mut |name, value| {
        syslog_headers.push((name.to_string(), value.to_string()));
    });
    assert!(syslog_headers.is_empty());

    let retained = Arc::new(RetainedIngestHeaders::capture(&Headers));
    let mut websocket = FakeMessage::new(18, vec![1, 0]);
    websocket.transport = MessageTransport::Websockets {
        frame_type: nervix_models::WebsocketsFrameType::Binary,
        headers: retained.clone(),
    };
    let metadata = websocket.metadata();
    assert_eq!(
        RetainedIngestMetadata::capture(metadata.fields).values(),
        [RetainedIngestMetadataValue::String(
            nervix_models::WebsocketsFrameType::Binary
                .as_ref()
                .to_string()
        )]
    );
    assert!(metadata.position.is_none());
    let IngestRowHeaders::Shared(headers) = metadata.headers else {
        panic!("the frame shares its connection's header snapshot");
    };
    assert!(Arc::ptr_eq(headers, &retained));
    for boundary in [websocket.headers(), headers.as_ref()] {
        let mut actual = Vec::new();
        boundary.visit(&mut |name, value| actual.push((name.to_string(), value.to_string())));
        assert_eq!(actual, expected);
    }
}
