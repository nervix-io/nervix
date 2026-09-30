//! Consumer credit races under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The node consumer credit invariant while competing sessions attach and release.
//! - **Depends on.** The production consumer budget and the server's Shuttle runner.
//! - **Must not know.** Session frames, scheduling decisions, or an emitter's Arrow contents.

use std::{num::NonZeroU64, time::Duration};

use bytes::Bytes;
use meticulous::OptionExt as _;
use nervix_models::{
    AckWindow, CLIENT_CONSUMER_NODE_BYTES, DomainName, EmitterName, FieldName, ParseAsType,
    RelayName, SchemaField, Timestamp,
};
use nervix_primitives::sync::atomic::Ordering;
use triomphe::Arc;
use uuid::Uuid;

use super::{
    ClientEmitterAnswer, ClientEmitterBudget, ClientEmitterDescription, ClientEmitterEndpoint,
    ClientEmitterPayload, ClientEmitterRefusal, ClientEmitterResult,
};
use crate::{metrics::RuntimeMetrics, shuttle_test::check_interleavings};

fn description(fields: Vec<SchemaField>) -> ClientEmitterDescription {
    ClientEmitterDescription {
        fields,
        maximum_payload_bytes: 1024,
        maximum_payload_rows: 1,
        window: AckWindow::Sequential,
        ack_timeout: Duration::from_secs(30),
        retry_backoff: Duration::ZERO,
        retry_max_backoff: Duration::ZERO,
    }
}

#[test]
fn shuttle_competing_consumer_grants_never_exceed_the_node_budget() {
    check_interleavings(|| {
        shuttle::future::block_on(async {
            let budget = ClientEmitterBudget::default();
            let bytes = NonZeroU64::new(CLIENT_CONSUMER_NODE_BYTES / 2 + 1)
                .assured("more than half the node budget is nonzero");
            let mut workers = Vec::new();
            for _ in 0..3 {
                let budget = budget.clone();
                workers.push(nervix_primitives::task::spawn(async move {
                    if let Some(grant) = budget.try_grant(bytes) {
                        assert!(
                            budget.granted.load(Ordering::Acquire) <= CLIENT_CONSUMER_NODE_BYTES
                        );
                        nervix_primitives::task::yield_now().await;
                        assert!(
                            budget.granted.load(Ordering::Acquire) <= CLIENT_CONSUMER_NODE_BYTES
                        );
                        drop(grant);
                    }
                }));
            }
            for worker in workers {
                worker.await.expect("consumer grant task completes");
            }
            assert_eq!(budget.granted.load(Ordering::Acquire), 0);
        });
    });
}

#[test]
fn shuttle_consumer_loss_and_ack_race_release_one_retained_delivery() {
    check_interleavings(|| {
        shuttle::future::block_on(async {
            let fields = vec![SchemaField {
                name: FieldName::parse("id").expect("literal field name"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }];
            let series = RuntimeMetrics::default().client_emitter_series(
                &DomainName::parse("test").expect("literal domain"),
                &EmitterName::parse("output").expect("literal emitter"),
            );
            let mut next_reference = 1_u128;
            let endpoint = Arc::new(ClientEmitterEndpoint::new_with_references(
                description(fields.clone()),
                ClientEmitterBudget::default(),
                series.clone(),
                move || {
                    next_reference += 1;
                    Uuid::from_u128(next_reference)
                },
                false,
            ));
            let credit = NonZeroU64::new(1024).assured("literal credit");
            let first = endpoint
                .open(&fields, 1, credit, false)
                .await
                .expect("first opens");
            let mut second = endpoint
                .open(&fields, 1, credit, false)
                .await
                .expect("second opens");
            let answerer = first.responder.clone();
            let closer = first.responder.clone();
            let mut first_deliveries = first.deliveries;
            let publishing = endpoint.clone();
            let publisher = nervix_primitives::task::spawn(async move {
                publishing
                    .publish(ClientEmitterPayload {
                        identity: Uuid::from_u128(1),
                        source: RelayName::parse("orders").expect("literal relay"),
                        branch: None,
                        body: Bytes::from_static(b"arrow ipc test bytes"),
                        members: 1,
                        execution_now: Timestamp::from_unix_nanos(1),
                    })
                    .await
            });
            let attempt = first_deliveries
                .recv()
                .await
                .expect("first worker receives attempt");
            let reference = attempt.reference;
            let ack = nervix_primitives::task::spawn(async move {
                answerer.answer(reference, ClientEmitterAnswer::Ack).await
            });
            let close = nervix_primitives::task::spawn(async move {
                closer.close();
            });
            let ack_result = ack.await.expect("ACK task completes");
            close.await.expect("close task completes");
            if ack_result != Ok(()) {
                assert_eq!(ack_result, Err(ClientEmitterRefusal::StaleReference));
                let retry = second
                    .deliveries
                    .recv()
                    .await
                    .expect("second worker gets retry");
                assert_eq!(retry.payload.identity, attempt.payload.identity);
                assert_eq!(retry.payload.body, attempt.payload.body);
                assert_ne!(retry.reference, reference);
                second
                    .answer(retry.reference, ClientEmitterAnswer::Ack)
                    .await
                    .expect("current retry ACK accepted");
            }
            assert_eq!(
                publisher.await.expect("publisher completes"),
                Ok(ClientEmitterResult::Acknowledged)
            );
            assert_eq!(
                series
                    .describe_lines()
                    .iter()
                    .find(|line| line.starts_with("retained batches:"))
                    .expect("retention metric exists"),
                "retained batches: 0"
            );
            second.responder.close();
            endpoint.end();
        });
    });
}

#[test]
fn shuttle_publish_cancellation_and_ack_race_release_one_reservation() {
    check_interleavings(|| {
        shuttle::future::block_on(async {
            let fields = vec![SchemaField {
                name: FieldName::parse("id").expect("literal field name"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }];
            let series = RuntimeMetrics::default().client_emitter_series(
                &DomainName::parse("test").expect("literal domain"),
                &EmitterName::parse("output").expect("literal emitter"),
            );
            let budget = ClientEmitterBudget::default();
            let mut next_reference = 1_u128;
            let endpoint = Arc::new(ClientEmitterEndpoint::new_with_references(
                description(fields.clone()),
                budget.clone(),
                series.clone(),
                move || {
                    next_reference += 1;
                    Uuid::from_u128(next_reference)
                },
                false,
            ));
            let credit = NonZeroU64::new(1024).assured("literal credit");
            let mut consumer = endpoint
                .open(&fields, 1, credit, false)
                .await
                .expect("consumer opens");
            let responder = consumer.responder.clone();
            let publishing = endpoint.clone();
            let publisher = nervix_primitives::task::spawn(async move {
                publishing
                    .publish(ClientEmitterPayload {
                        identity: Uuid::from_u128(1),
                        source: RelayName::parse("orders").expect("literal relay"),
                        branch: None,
                        body: Bytes::from_static(b"arrow ipc test bytes"),
                        members: 1,
                        execution_now: Timestamp::from_unix_nanos(1),
                    })
                    .await
            });
            let attempt = consumer
                .deliveries
                .recv()
                .await
                .expect("consumer receives attempt");
            let ack = nervix_primitives::task::spawn(async move {
                responder
                    .answer(attempt.reference, ClientEmitterAnswer::Ack)
                    .await
            });
            publisher.abort();
            let published = publisher.await;
            let settled = ack.await.expect("ACK task completes");
            assert!(
                matches!(settled, Ok(()) | Err(ClientEmitterRefusal::StaleReference)),
                "ACK either wins or is revoked: {settled:?}"
            );
            if let Ok(result) = published {
                assert_eq!(result, Ok(ClientEmitterResult::Acknowledged));
            }
            let barrier = endpoint
                .open(&fields, 1, credit, false)
                .await
                .expect("owner processes cancellation before the next attach");
            drop(barrier);
            assert_eq!(
                budget.used(),
                0,
                "cancellation releases retained byte credit"
            );
            assert_eq!(
                series
                    .describe_lines()
                    .iter()
                    .find(|line| line.starts_with("retained batches:"))
                    .expect("retention metric exists"),
                "retained batches: 0"
            );
            endpoint.end();
        });
    });
}
