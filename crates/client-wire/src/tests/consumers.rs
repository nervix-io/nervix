//! Current native-emitter request and reply shapes, including Arrow byte delivery.

use std::{num::NonZeroU32, time::Duration};

use bytes::Bytes;
use meticulous::OptionExt as _;
use nervix_models::{AckWindow, ClientConsumerLimits, Timestamp};
use uuid::Uuid;

use super::{
    fixtures::{name, non_zero, request, round_trip_client, round_trip_reply},
    samples::producer_fields,
};
use crate::{
    ClientMessage, ClientRequest, CloseEmitterOutcome, CloseEmitterRequest, ConsumerId,
    EmitterBatchDecision, EmitterBatchReceived, EmitterCloseDisposition, EmitterOpenRefusal,
    EmitterOpened, EmitterSettlement, OpenEmitterDisposition, OpenEmitterOutcome,
    OpenEmitterRequest, ReadEmitterBatchOutcome, ReadEmitterBatchRequest, ReadEmitterDisposition,
    Reply, ReplyBody, SettleEmitterBatchOutcome, SettleEmitterBatchRequest,
};

fn consumer() -> ConsumerId {
    ConsumerId::opened_by(request(31))
}

fn limits() -> ClientConsumerLimits {
    ClientConsumerLimits {
        batches: NonZeroU32::new(4).assured("literal is nonzero"),
        bytes: non_zero(1024 * 1024),
    }
}

#[test]
fn consumer_requests_round_trip_with_exact_fields_and_decisions() {
    let requests = [
        ClientRequest::OpenEmitter(OpenEmitterRequest {
            domain: name("tenant"),
            emitter: name("app_output"),
            expected_fields: producer_fields(),
            limits: limits(),
        }),
        ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest {
            consumer: consumer(),
        }),
        ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
            consumer: consumer(),
            reference: Uuid::from_bytes([0x11; 16]),
            decision: EmitterBatchDecision::Ack,
        }),
        ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
            consumer: consumer(),
            reference: Uuid::from_bytes([0x12; 16]),
            decision: EmitterBatchDecision::Retry,
        }),
        ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
            consumer: consumer(),
            reference: Uuid::from_bytes([0x13; 16]),
            decision: EmitterBatchDecision::Reject("application validation failed".to_string()),
        }),
        ClientRequest::CloseEmitter(CloseEmitterRequest {
            consumer: consumer(),
        }),
    ];
    for (offset, request_body) in requests.into_iter().enumerate() {
        let message = ClientMessage {
            request_id: request(40 + u64::try_from(offset).expect("sample index fits u64")),
            request: request_body,
        };
        assert_eq!(round_trip_client(&message), message);
    }
}

#[test]
fn consumer_replies_round_trip_with_retained_arrow_body() {
    let opened = EmitterOpened {
        domain: name("tenant"),
        emitter: name("app_output"),
        fields: producer_fields(),
        window: AckWindow::Parallel { max: non_zero(8) },
        ack_timeout: Duration::from_secs(30),
        retry_backoff: Duration::from_millis(100),
        retry_max_backoff: Duration::from_secs(1),
        granted: limits(),
        max_batch_bytes: 1024,
        max_batch_rows: 16,
    };
    let bodies = [
        ReplyBody::OpenEmitter(OpenEmitterOutcome {
            disposition: OpenEmitterDisposition::Opened(Box::new(opened)),
            message: "attached".to_string(),
        }),
        ReplyBody::OpenEmitter(OpenEmitterOutcome {
            disposition: OpenEmitterDisposition::Refused(EmitterOpenRefusal::SchemaMismatch),
            message: "fields differ".to_string(),
        }),
        ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
            disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
                identity: Uuid::from_bytes([0x21; 16]),
                reference: Uuid::from_bytes([0x22; 16]),
                source_relay: name("orders"),
                branch_fingerprint: Some([0xA5; 32]),
                batch: Bytes::from_static(b"Arrow IPC batch bytes"),
                members: 3,
                execution_now: Timestamp::now(),
            }),
            message: String::new(),
        }),
        ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
            disposition: ReadEmitterDisposition::Ended,
            message: "endpoint ended".to_string(),
        }),
        ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
            disposition: EmitterSettlement::Confirmed,
            message: String::new(),
        }),
        ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
            disposition: EmitterSettlement::StaleReference,
            message: String::new(),
        }),
        ReplyBody::CloseEmitter(CloseEmitterOutcome {
            disposition: EmitterCloseDisposition::Closed,
            message: String::new(),
        }),
    ];
    for (offset, body) in bodies.into_iter().enumerate() {
        let reply = Reply {
            request_id: request(50 + u64::try_from(offset).expect("sample index fits u64")),
            body,
        };
        assert_eq!(round_trip_reply(&reply), reply);
    }
}
