//! Current native-emitter request and reply shapes, including Arrow byte delivery.

use std::{num::NonZeroU32, time::Duration};

use bytes::Bytes;
use meticulous::OptionExt as _;
use nervix_models::{
    AckWindow, ClientConsumerLimits, DomainName, EmitterName, RelayName, Timestamp,
};
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
        generation: 4,
        contract: nervix_models::ClientEndpointContract::from_digest([7; 32]),
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

#[test]
fn bolero_native_emitter_frames_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|bytes: &[u8]| {
            let byte = |index: usize| match bytes.get(index) {
                Some(value) => *value,
                None => 0,
            };
            let domain: DomainName = name(&format!("tenant_{}", byte(0)));
            let emitter: EmitterName = name(&format!("output_{}", byte(1)));
            let source_relay: RelayName = name(&format!("relay_{}", byte(2)));
            let mut fields = producer_fields();
            fields[0].optional = byte(3) & 1 != 0;
            fields[1].sensitive = byte(3) & 2 != 0;
            let limits = ClientConsumerLimits {
                batches: NonZeroU32::new(1 + u32::from(byte(4)))
                    .assured("one plus a byte is nonzero"),
                bytes: non_zero(4096 + u64::from(byte(5))),
            };
            let opened_by = request(1 + u64::from(byte(6)));
            let consumer = ConsumerId::opened_by(opened_by);
            assert_eq!(consumer.open_request(), opened_by);
            let identity = Uuid::from_bytes(std::array::from_fn(|index| byte(16 + index)));
            let reference = Uuid::from_bytes(std::array::from_fn(|index| byte(32 + index)));
            let reason = format!("application rejected {} 🚀", byte(7));
            let requests = [
                ClientRequest::OpenEmitter(OpenEmitterRequest {
                    domain: domain.clone(),
                    emitter: emitter.clone(),
                    expected_fields: fields.clone(),
                    limits,
                }),
                ClientRequest::ReadEmitterBatch(ReadEmitterBatchRequest { consumer }),
                ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
                    consumer,
                    reference,
                    decision: EmitterBatchDecision::Ack,
                }),
                ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
                    consumer,
                    reference,
                    decision: EmitterBatchDecision::Retry,
                }),
                ClientRequest::SettleEmitterBatch(SettleEmitterBatchRequest {
                    consumer,
                    reference,
                    decision: EmitterBatchDecision::Reject(reason.clone()),
                }),
                ClientRequest::CloseEmitter(CloseEmitterRequest { consumer }),
            ];
            for (index, request_body) in requests.into_iter().enumerate() {
                let message = ClientMessage {
                    request_id: request(300 + u64::try_from(index).expect("sample index fits")),
                    request: request_body,
                };
                assert_eq!(round_trip_client(&message), message);
            }

            let ack_timeout = Duration::from_millis(1 + u64::from(byte(8)));
            let retry_backoff = Duration::from_millis(1 + u64::from(byte(9)));
            let retry_max_backoff = retry_backoff + Duration::from_millis(u64::from(byte(10)));
            let mut replies = Vec::new();
            for window in [
                AckWindow::Sequential,
                AckWindow::Parallel {
                    max: non_zero(1 + u64::from(byte(11))),
                },
            ] {
                replies.push(ReplyBody::OpenEmitter(OpenEmitterOutcome {
                    disposition: OpenEmitterDisposition::Opened(Box::new(EmitterOpened {
                        domain: domain.clone(),
                        emitter: emitter.clone(),
                        fields: fields.clone(),
                        generation: u64::from(byte(14)),
                        contract: nervix_models::ClientEndpointContract::from_digest(
                            [byte(15); 32],
                        ),
                        window,
                        ack_timeout,
                        retry_backoff,
                        retry_max_backoff,
                        granted: limits,
                        max_batch_bytes: 1 + u64::from(byte(12)),
                        max_batch_rows: 1 + u32::from(byte(13)),
                    })),
                    message: reason.clone(),
                }));
            }
            for &refusal in crate::consumer::ALL_EMITTER_OPEN_REFUSALS {
                replies.push(ReplyBody::OpenEmitter(OpenEmitterOutcome {
                    disposition: OpenEmitterDisposition::Refused(refusal),
                    message: reason.clone(),
                }));
            }
            for branch_fingerprint in [None, Some([byte(14); 32])] {
                replies.push(ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
                    disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
                        identity,
                        reference,
                        source_relay: source_relay.clone(),
                        branch_fingerprint,
                        batch: Bytes::from(vec![byte(48), byte(49), byte(50)]),
                        members: 1 + u32::from(byte(51)),
                        execution_now: Timestamp::from_unix_nanos(i64::from(i16::from_le_bytes([
                            byte(52),
                            byte(53),
                        ]))),
                    }),
                    message: reason.clone(),
                }));
            }
            replies.push(ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Ended,
                message: reason.clone(),
            }));
            for &settlement in crate::consumer::ALL_EMITTER_SETTLEMENTS {
                replies.push(ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
                    disposition: settlement,
                    message: reason.clone(),
                }));
            }
            for &disposition in crate::consumer::ALL_EMITTER_CLOSE_DISPOSITIONS {
                replies.push(ReplyBody::CloseEmitter(CloseEmitterOutcome {
                    disposition,
                    message: reason.clone(),
                }));
            }
            for (index, body) in replies.into_iter().enumerate() {
                let reply = Reply {
                    request_id: request(400 + u64::try_from(index).expect("sample index fits")),
                    body,
                };
                assert_eq!(round_trip_reply(&reply), reply);
            }
        });
}
