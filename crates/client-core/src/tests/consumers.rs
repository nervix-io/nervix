//! Consumer recovery tests over the hand-served loopback exchange.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions for interruption, contract fencing, stale delivery references, and
//!   closing a handle while its replacement open is in flight.
//! - **Depends on.** The client consumer API and the parent loopback fixture.
//! - **Must not know.** Runtime emitter execution or a real transport.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc as StdArc,
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClientRequest, CloseEmitterOutcome, EmitterBatchReceived, EmitterCloseDisposition,
    EmitterOpenRefusal, EmitterOpened, OpenEmitterDisposition, OpenEmitterOutcome,
    ReadEmitterBatchOutcome, ReadEmitterDisposition, ReplyBody,
};
use nervix_models::{
    AckWindow, ClientConsumerLimits, ClientEndpointContract, EmitterName, ParseAsType, RelayName,
    Timestamp,
};
use uuid::Uuid;

use super::{DEADLINE, Loopback, domain, field};
use crate::{ClientError, ConsumerConnection, ConsumerReopenReason, EmitterConsumer};

fn emitter() -> EmitterName {
    EmitterName::parse("app_output").assured("literal emitter")
}

fn limits() -> ClientConsumerLimits {
    ClientConsumerLimits {
        batches: NonZeroU32::new(2).assured("literal batches are nonzero"),
        bytes: NonZeroU64::new(4096).assured("literal bytes are nonzero"),
    }
}

fn description() -> EmitterOpened {
    EmitterOpened {
        domain: domain("tenant"),
        emitter: emitter(),
        fields: vec![field("id", ParseAsType::String)],
        generation: 4,
        contract: ClientEndpointContract::from_digest([7; 32]),
        window: AckWindow::Sequential,
        ack_timeout: Duration::from_secs(30),
        retry_backoff: Duration::from_millis(100),
        retry_max_backoff: Duration::from_secs(1),
        granted: limits(),
        max_batch_bytes: 4096,
        max_batch_rows: 16,
    }
}

fn opened(description: EmitterOpened) -> ReplyBody {
    ReplyBody::OpenEmitter(OpenEmitterOutcome {
        disposition: OpenEmitterDisposition::Opened(Box::new(description)),
        message: "attached".to_string(),
    })
}

async fn open(loopback: &mut Loopback) -> EmitterConsumer {
    let client = loopback.client.clone();
    let opening = nervix_primitives::task::spawn(async move {
        client
            .subscribe_emitter(domain("tenant"), emitter(), description().fields, limits())
            .await
    });
    let request = loopback.next_request().await;
    assert!(matches!(request.request, ClientRequest::OpenEmitter(_)));
    loopback
        .answer(request.request_id, opened(description()))
        .await;
    tokio::time::timeout(DEADLINE, opening)
        .await
        .assured("open completes within the deadline")
        .assured("open task completes")
        .assured("open reply attaches a consumer")
}

fn received() -> ReplyBody {
    ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
        disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
            identity: Uuid::from_bytes([1; 16]),
            reference: Uuid::from_bytes([2; 16]),
            source_relay: RelayName::parse("orders").assured("literal relay"),
            branch_fingerprint: None,
            batch: Bytes::from_static(b"batch"),
            members: 1,
            execution_now: Timestamp::now(),
        }),
        message: String::new(),
    })
}

#[nervix_primitives::test]
async fn a_consumer_reports_a_gap_then_reads_through_a_fresh_attachment() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = StdArc::new(open(&mut loopback).await);
    loopback.replace_exchange().await;
    assert_eq!(consumer.connection(), ConsumerConnection::Restoring);
    let gap = match consumer.next_batch().await {
        Err(report) => report,
        Ok(_) => panic!("a gap is reported before a restored batch"),
    };
    assert!(matches!(
        gap.current_context(),
        ClientError::ConsumerInterrupted
    ));
    let restore = loopback.next_request().await;
    assert!(matches!(restore.request, ClientRequest::OpenEmitter(_)));
    loopback
        .answer(restore.request_id, opened(description()))
        .await;
    let reading = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let read = loopback.next_request().await;
    let ClientRequest::ReadEmitterBatch(read_request) = read.request else {
        panic!("the restored consumer reads through its new attachment");
    };
    assert_eq!(read_request.consumer.open_request(), restore.request_id);
    loopback.answer(read.request_id, received()).await;
    let delivery = reading
        .await
        .assured("read task completes")
        .assured("restored read succeeds")
        .assured("restored read yields a batch");
    assert_eq!(delivery.reference, Uuid::from_bytes([2; 16]));
    assert_eq!(consumer.connection(), ConsumerConnection::Active);
}

#[nervix_primitives::test]
async fn an_ended_consumer_attachment_reopens_when_its_contract_is_unchanged() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = StdArc::new(open(&mut loopback).await);
    let reading = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let read = loopback.next_request().await;
    assert!(matches!(read.request, ClientRequest::ReadEmitterBatch(_)));
    loopback
        .answer(
            read.request_id,
            ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Ended,
                message: "owner moved".to_string(),
            }),
        )
        .await;
    let interrupted = reading
        .await
        .assured("read task completes")
        .err()
        .assured("an ended attachment reports the gap");
    assert!(matches!(
        interrupted.current_context(),
        ClientError::ConsumerInterrupted
    ));
    let resuming = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let restore = loopback.next_request().await;
    assert!(matches!(restore.request, ClientRequest::OpenEmitter(_)));
    loopback
        .answer(restore.request_id, opened(description()))
        .await;
    let read = loopback.next_request().await;
    let ClientRequest::ReadEmitterBatch(request) = read.request else {
        panic!("the consumer reads through its replacement attachment");
    };
    assert_eq!(request.consumer.open_request(), restore.request_id);
    loopback.answer(read.request_id, received()).await;
    assert!(
        resuming
            .await
            .assured("resumed read completes")
            .assured("resumed read succeeds")
            .is_some()
    );
}

#[nervix_primitives::test]
async fn a_changed_consumer_contract_or_generation_requires_a_new_open() {
    let mut generation = description();
    generation.generation += 1;
    let mut contract = description();
    contract.contract = ClientEndpointContract::from_digest([8; 32]);
    for (changed, expected) in [
        (generation, ConsumerReopenReason::GenerationChanged),
        (contract, ConsumerReopenReason::ContractChanged),
    ] {
        let mut loopback = Loopback::new(Some(domain("tenant")));
        let consumer = open(&mut loopback).await;
        loopback.replace_exchange().await;
        let restore = loopback.next_request().await;
        loopback.answer(restore.request_id, opened(changed)).await;
        let close = loopback.next_request().await;
        let ClientRequest::CloseEmitter(close_request) = close.request else {
            panic!("the mismatching attachment is closed");
        };
        assert_eq!(close_request.consumer.open_request(), restore.request_id);
        loopback
            .answer(
                close.request_id,
                ReplyBody::CloseEmitter(CloseEmitterOutcome {
                    disposition: EmitterCloseDisposition::Closed,
                    message: String::new(),
                }),
            )
            .await;
        let error = match consumer.next_batch().await {
            Err(report) => report,
            Ok(_) => panic!("the gap must be reported first"),
        };
        assert!(matches!(
            error.current_context(),
            ClientError::ConsumerInterrupted
        ));
        let error = match consumer.next_batch().await {
            Err(report) => report,
            Ok(_) => panic!("a changed endpoint must block the old handle"),
        };
        assert!(
            matches!(error.current_context(), ClientError::ConsumerReopenRequired(actual) if *actual == expected)
        );
    }
}

#[nervix_primitives::test]
async fn temporary_consumer_capacity_refusal_retries_the_same_desired_contract() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = open(&mut loopback).await;
    loopback.replace_exchange().await;
    let first = loopback.next_request().await;
    let ClientRequest::OpenEmitter(request) = first.request else {
        panic!("restoration opens the desired consumer");
    };
    loopback
        .answer(
            first.request_id,
            ReplyBody::OpenEmitter(OpenEmitterOutcome {
                disposition: OpenEmitterDisposition::Refused(
                    EmitterOpenRefusal::SessionCapacityExhausted,
                ),
                message: "session capacity full".to_string(),
            }),
        )
        .await;
    let retry = loopback.next_request().await;
    let ClientRequest::OpenEmitter(repeated) = retry.request else {
        panic!("temporary capacity refusal retries the open");
    };
    assert_eq!(repeated, request);
    assert_ne!(retry.request_id, first.request_id);
    loopback
        .answer(retry.request_id, opened(description()))
        .await;
    let gap = match consumer.next_batch().await {
        Err(report) => report,
        Ok(_) => panic!("restoration still reports the session gap first"),
    };
    assert!(matches!(
        gap.current_context(),
        ClientError::ConsumerInterrupted
    ));
    tokio::time::timeout(DEADLINE, async {
        while consumer.connection() != ConsumerConnection::Active {
            nervix_primitives::task::consume_budget().await;
        }
    })
    .await
    .assured("the temporary refusal eventually restores the consumer");
}

#[nervix_primitives::test]
async fn stopped_removed_and_changed_consumer_endpoints_require_a_new_application_open() {
    for (refusal, expected) in [
        (
            EmitterOpenRefusal::DomainStopped,
            ConsumerReopenReason::DomainStopped,
        ),
        (
            EmitterOpenRefusal::EmitterNotFound,
            ConsumerReopenReason::EndpointRemoved,
        ),
        (
            EmitterOpenRefusal::SchemaMismatch,
            ConsumerReopenReason::SchemaChanged,
        ),
    ] {
        let mut loopback = Loopback::new(Some(domain("tenant")));
        let consumer = open(&mut loopback).await;
        loopback.replace_exchange().await;
        let restore = loopback.next_request().await;
        loopback
            .answer(
                restore.request_id,
                ReplyBody::OpenEmitter(OpenEmitterOutcome {
                    disposition: OpenEmitterDisposition::Refused(refusal),
                    message: "endpoint no longer matches".to_string(),
                }),
            )
            .await;
        let gap = match consumer.next_batch().await {
            Err(report) => report,
            Ok(_) => panic!("the gap must be reported first"),
        };
        assert!(matches!(
            gap.current_context(),
            ClientError::ConsumerInterrupted
        ));
        let error = tokio::time::timeout(DEADLINE, consumer.next_batch())
            .await
            .assured("terminal restoration is reported promptly")
            .err()
            .assured("the former consumer needs a new open");
        assert!(
            matches!(error.current_context(), ClientError::ConsumerReopenRequired(reason) if *reason == expected)
        );
    }
}

#[nervix_primitives::test]
async fn a_delivery_from_a_lost_exchange_cannot_ack_a_replacement() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = StdArc::new(open(&mut loopback).await);
    let reading = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let read = loopback.next_request().await;
    loopback.answer(read.request_id, received()).await;
    let delivery = reading
        .await
        .assured("read task completes")
        .assured("read succeeds")
        .assured("read yields a batch");
    loopback.replace_exchange().await;
    let restore = loopback.next_request().await;
    loopback
        .answer(restore.request_id, opened(description()))
        .await;
    let report = delivery
        .ack()
        .await
        .err()
        .assured("the former reference expired");
    assert!(matches!(
        report.current_context(),
        ClientError::DeliveryReferenceExpired { .. }
    ));
    assert!(
        loopback.requests.try_recv().is_err(),
        "no settlement crosses the exchange gap"
    );
}

#[nervix_primitives::test]
async fn a_settlement_sent_before_the_session_lost_its_answer_is_unknown() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = StdArc::new(open(&mut loopback).await);
    let reading = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let read = loopback.next_request().await;
    loopback.answer(read.request_id, received()).await;
    let delivery = reading
        .await
        .assured("read task completes")
        .assured("read succeeds")
        .assured("read yields a batch");
    let settling = nervix_primitives::task::spawn(async move { delivery.ack().await });
    let request = loopback.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::SettleEmitterBatch(_)
    ));
    loopback.replace_exchange().await;
    let report = settling
        .await
        .assured("settlement task completes")
        .err()
        .assured("a sent settlement with no reply is uncertain");
    assert!(matches!(
        report.current_context(),
        ClientError::SettlementUnknown { .. }
    ));
}

#[nervix_primitives::test]
async fn a_read_interrupted_before_its_reply_yields_no_old_batch() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = StdArc::new(open(&mut loopback).await);
    let reading = nervix_primitives::task::spawn({
        let consumer = consumer.clone();
        async move { consumer.next_batch().await }
    });
    let request = loopback.next_request().await;
    assert!(matches!(
        request.request,
        ClientRequest::ReadEmitterBatch(_)
    ));
    loopback.replace_exchange().await;
    let report = match reading.await.assured("read task completes") {
        Err(report) => report,
        Ok(_) => panic!("a read waiting on the lost exchange yields no batch"),
    };
    assert!(matches!(
        report.current_context(),
        ClientError::ConsumerInterrupted
    ));
    let restore = loopback.next_request().await;
    loopback
        .answer(restore.request_id, opened(description()))
        .await;
    tokio::time::timeout(DEADLINE, async {
        while consumer.connection() != ConsumerConnection::Active {
            nervix_primitives::task::consume_budget().await;
        }
    })
    .await
    .assured("the replacement attachment is installed");
}

#[nervix_primitives::test]
async fn an_unexpected_consumer_restoration_reply_requires_a_new_open() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = open(&mut loopback).await;
    loopback.replace_exchange().await;
    let restore = loopback.next_request().await;
    loopback
        .answer(restore.request_id, super::domain_list_reply())
        .await;
    let interrupted = consumer
        .next_batch()
        .await
        .err()
        .assured("the gap is reported");
    assert!(matches!(
        interrupted.current_context(),
        ClientError::ConsumerInterrupted
    ));
    let terminal = tokio::time::timeout(DEADLINE, consumer.next_batch())
        .await
        .assured("invalid restoration is reported promptly")
        .err()
        .assured("the old consumer cannot read");
    assert!(matches!(
        terminal.current_context(),
        ClientError::ConsumerReopenRequired(ConsumerReopenReason::ProtocolViolated)
    ));
    assert!(
        loopback.requests.try_recv().is_err(),
        "invalid open is not retried"
    );
}

#[nervix_primitives::test]
async fn closing_during_restoration_releases_the_late_attachment() {
    let mut loopback = Loopback::new(Some(domain("tenant")));
    let consumer = open(&mut loopback).await;
    loopback.replace_exchange().await;
    let restore = loopback.next_request().await;
    assert_eq!(
        consumer
            .close()
            .await
            .assured("closing the desired handle succeeds"),
        EmitterCloseDisposition::Closed
    );
    loopback
        .answer(restore.request_id, opened(description()))
        .await;
    let close = loopback.next_request().await;
    assert!(matches!(close.request, ClientRequest::CloseEmitter(_)));
    loopback
        .answer(
            close.request_id,
            ReplyBody::CloseEmitter(CloseEmitterOutcome {
                disposition: EmitterCloseDisposition::Closed,
                message: String::new(),
            }),
        )
        .await;
}
