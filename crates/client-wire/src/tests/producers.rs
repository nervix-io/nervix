//! Client producers: every open, submission and close outcome and every producer event round
//! trips, a submitted batch is carried as the bytes of its frame, and malformed producer frames
//! are refused with the field they break.

use std::num::NonZeroU32;

use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, ClientBatchDefect, ClientOutcomeUncertainty, ClientProcessingFailure,
    ClientProducerAdmission, ClientProducerEndReason, ClientProducerRefusal,
    ClientSubmissionOutcome, ClientSubmissionRefusal,
};
use strum::IntoEnumIterator as _;

use super::{
    fixtures::{
        decode_error, decode_event, finish_raw, finish_reply, limits, name, non_zero, raw_client,
        raw_server, request, round_trip_client, round_trip_reply,
    },
    samples::{producer, producer_description, producer_fields},
};
use crate::{
    ClientMessage, ClientRequest, CloseIngestorDisposition, CloseIngestorOutcome,
    CloseIngestorRequest, OpenIngestorDisposition, OpenIngestorOutcome, OpenIngestorRequest,
    ProducerAdmissionChanged, ProducerEnded, ProducerId, ProducerOpened, Reply, ReplyBody,
    ServerEvent, ServerFrame, ServerMessage, SubmissionOutcome, SubmitBatchRequest, VerifiedFrame,
    WireDecodeError, WireEncodeError, producer::all_submission_refusals, wire,
};

fn reply(body: ReplyBody) -> Reply {
    Reply {
        request_id: request(7),
        body,
    }
}

fn opened(window: AckWindow) -> ProducerOpened {
    ProducerOpened {
        domain: name("tenant"),
        ingestor: name("orders_in"),
        description: producer_description(window),
    }
}

#[test]
fn an_opened_producer_round_trips_under_either_window() {
    for window in [
        AckWindow::Sequential,
        AckWindow::Parallel {
            max: non_zero(u64::MAX),
        },
    ] {
        let original = reply(ReplyBody::OpenIngestor(OpenIngestorOutcome {
            disposition: OpenIngestorDisposition::Opened(Box::new(opened(window))),
            message: "producer attached to ingestor 'orders_in'".to_string(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn every_open_refusal_round_trips() {
    for refusal in ClientProducerRefusal::iter() {
        let original = reply(ReplyBody::OpenIngestor(OpenIngestorOutcome {
            disposition: OpenIngestorDisposition::Refused(refusal),
            message: refusal.as_ref().to_string(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn every_submission_outcome_round_trips() {
    let mut outcomes = vec![ClientSubmissionOutcome::Completed];
    for refusal in all_submission_refusals() {
        outcomes.push(ClientSubmissionOutcome::NotAdmitted(refusal));
    }
    for failure in ClientProcessingFailure::iter() {
        outcomes.push(ClientSubmissionOutcome::ProcessingFailed(failure));
    }
    for cause in ClientOutcomeUncertainty::iter() {
        outcomes.push(ClientSubmissionOutcome::OutcomeUnknown(cause));
    }
    for outcome in outcomes {
        let original = reply(ReplyBody::Submission(SubmissionOutcome {
            outcome,
            message: String::new(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn every_refusal_and_every_defect_has_its_own_schema_value() {
    let refusals = all_submission_refusals();
    assert_eq!(
        refusals.len(),
        wire::SubmissionRefusal::ENUM_VALUES.len(),
        "each schema value names one refusal"
    );
    for defect in ClientBatchDefect::iter() {
        assert!(
            refusals.contains(&ClientSubmissionRefusal::InvalidBatch(defect)),
            "{defect:?} has a schema value"
        );
    }
    for refusal in [
        ClientSubmissionRefusal::Suspended,
        ClientSubmissionRefusal::Busy,
        ClientSubmissionRefusal::Draining,
        ClientSubmissionRefusal::ProducerEnded,
        ClientSubmissionRefusal::CreditExceeded,
    ] {
        assert!(refusals.contains(&refusal), "{refusal:?} has a schema value");
    }
}

#[test]
fn every_close_disposition_round_trips() {
    for disposition in [
        CloseIngestorDisposition::Closed,
        CloseIngestorDisposition::NotOpen,
    ] {
        let original = reply(ReplyBody::CloseIngestor(CloseIngestorOutcome {
            disposition,
            message: "closed".to_string(),
        }));
        assert_eq!(round_trip_reply(&original), original);
    }
}

#[test]
fn producer_events_round_trip_and_name_no_request() {
    for admission in ClientProducerAdmission::iter() {
        let changed = ProducerAdmissionChanged {
            producer: producer(),
            admission,
        };
        let frame = changed
            .encode(&limits())
            .assured("an admission frame fits the default limits");
        let ServerEvent::ProducerAdmissionChanged(decoded) = decode_event(frame) else {
            panic!("an admission frame decodes as an admission change");
        };
        assert_eq!(decoded, changed);
    }
    for reason in ClientProducerEndReason::iter() {
        let ended = ProducerEnded {
            producer: producer(),
            reason,
            message: reason.as_ref().to_string(),
        };
        let frame = ended
            .encode(&limits())
            .assured("an end frame fits the default limits");
        let verified = VerifiedFrame::<ServerFrame>::verify(frame.into_bytes(), &limits())
            .assured("an encoded frame verifies");
        assert_eq!(verified.request_id(), None);
        let ServerMessage::Event(ServerEvent::ProducerEnded(decoded)) =
            ServerMessage::decode(&verified).assured("an end frame decodes")
        else {
            panic!("an end frame decodes as a producer end");
        };
        assert_eq!(decoded, ended);
    }
}

#[test]
fn a_submitted_batch_is_read_as_a_window_onto_its_frame() {
    let batch = Bytes::from(vec![0xA7; 4096]);
    let message = ClientMessage {
        request_id: request(20),
        request: ClientRequest::SubmitBatch(SubmitBatchRequest {
            producer: producer(),
            batch: batch.clone(),
        }),
    };
    let frame = message
        .encode(&limits())
        .assured("a small batch fits a frame");
    let frame = VerifiedFrame::verify(frame.into_bytes(), &limits()).assured("the frame verifies");
    let decoded = ClientMessage::decode(&frame).assured("the frame decodes");
    let ClientRequest::SubmitBatch(submit) = &decoded.request else {
        panic!("a submission decodes as a submission");
    };
    assert_eq!(submit.batch, batch);
    let frame_range = frame.bytes().as_ptr_range();
    let batch_range = submit.batch.as_ptr_range();
    assert!(
        frame_range.start <= batch_range.start && batch_range.end <= frame_range.end,
        "the batch shares the frame's bytes instead of copying them"
    );
    assert_eq!(round_trip_client(&decoded), decoded);
}

#[test]
fn the_largest_submitted_batch_fits_a_frame_and_one_byte_more_does_not() {
    let limits = limits();
    let largest = limits.max_submitted_batch_bytes();
    let submit = |bytes: usize| ClientMessage {
        request_id: request(u64::MAX),
        request: ClientRequest::SubmitBatch(SubmitBatchRequest {
            producer: ProducerId::opened_by(request(u64::MAX)),
            batch: Bytes::from(vec![0x5A; bytes]),
        }),
    };
    let frame = submit(largest)
        .encode(&limits)
        .assured("the largest batch the envelope leaves room for fits a frame");
    assert!(frame.len() <= limits.frame_bytes());
    let error = submit(limits.frame_bytes())
        .encode(&limits)
        .expect_err("a batch as large as the frame cannot fit beside its envelope");
    assert!(matches!(
        error.current_context(),
        WireEncodeError::FrameTooLarge { .. }
    ));
}

#[test]
fn empty_batches_and_empty_expected_schemas_are_refused_both_ways() {
    let empty_batch = ClientMessage {
        request_id: request(1),
        request: ClientRequest::SubmitBatch(SubmitBatchRequest {
            producer: producer(),
            batch: Bytes::new(),
        }),
    };
    assert!(matches!(
        empty_batch
            .encode(&limits())
            .expect_err("an empty batch is refused")
            .current_context(),
        WireEncodeError::EmptyCollection {
            field: "SubmitBatchRequest.batch"
        }
    ));
    let empty_schema = ClientMessage {
        request_id: request(1),
        request: ClientRequest::OpenIngestor(OpenIngestorRequest {
            domain: name("tenant"),
            ingestor: name("orders_in"),
            expected_fields: Vec::new(),
            limits: nervix_models::ClientProducerLimits {
                batches: NonZeroU32::MIN,
                bytes: non_zero(1),
            },
        }),
    };
    assert!(matches!(
        empty_schema
            .encode(&limits())
            .expect_err("an empty expected schema is refused")
            .current_context(),
        WireEncodeError::EmptyCollection {
            field: "OpenIngestorRequest.expected_fields"
        }
    ));

    let mut builder = FlatBufferBuilder::new();
    let batch = builder.create_vector::<u8>(&[]);
    let submit = wire::SubmitBatchRequest::create(
        &mut builder,
        &wire::SubmitBatchRequestArgs {
            producer: 3,
            batch: Some(batch),
        },
    );
    let frame = raw_client(client_frame(
        builder,
        wire::ClientRequest::SubmitBatchRequest,
        submit.as_union_value(),
    ));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        WireDecodeError::EmptyCollection {
            field: "SubmitBatchRequest.batch",
        }
    );
}

/// Finishes a hand-built client frame for request 11.
fn client_frame<'fbb>(
    mut builder: FlatBufferBuilder<'fbb>,
    request_type: wire::ClientRequest,
    request_value: WIPOffset<UnionWIPOffset>,
) -> Bytes {
    let root = wire::ClientMessage::create(
        &mut builder,
        &wire::ClientMessageArgs {
            request_id: 11,
            request_type,
            request: Some(request_value),
        },
    );
    finish_raw(builder, root, "NXCM")
}

/// A hand-built open request with the given limits.
fn open_frame(batches: u32, bytes: u64) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("tenant");
    let ingestor = builder.create_string("orders_in");
    let field_name = builder.create_string("order_id");
    let scalar = wire::ScalarFieldType::create(
        &mut builder,
        &wire::ScalarFieldTypeArgs {
            scalar: Some(wire::ScalarType::U64),
        },
    );
    let field_type = wire::FieldType::create(
        &mut builder,
        &wire::FieldTypeArgs {
            shape_type: wire::FieldTypeShape::ScalarFieldType,
            shape: Some(scalar.as_union_value()),
        },
    );
    let field = wire::RowField::create(
        &mut builder,
        &wire::RowFieldArgs {
            name: Some(field_name),
            field_type: Some(field_type),
            nullable: false,
            sensitive: false,
        },
    );
    let fields = builder.create_vector(&[field]);
    let open = wire::OpenIngestorRequest::create(
        &mut builder,
        &wire::OpenIngestorRequestArgs {
            domain: Some(domain),
            ingestor: Some(ingestor),
            expected_fields: Some(fields),
            max_outstanding_batches: batches,
            max_outstanding_bytes: bytes,
        },
    );
    client_frame(
        builder,
        wire::ClientRequest::OpenIngestorRequest,
        open.as_union_value(),
    )
}

#[test]
fn zero_limits_are_refused_and_positive_ones_are_left_to_the_server() {
    let frame = raw_client(open_frame(0, 1));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "OpenIngestorRequest.max_outstanding_batches",
        }
    );
    let frame = raw_client(open_frame(1, 0));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "OpenIngestorRequest.max_outstanding_bytes",
        }
    );
    // Limits above what one producer may ask for decode: the server answers them with a typed
    // refusal rather than a malformed-request rejection.
    let frame = raw_client(open_frame(u32::MAX, u64::MAX));
    let decoded = ClientMessage::decode(&frame).assured("positive limits decode");
    let ClientRequest::OpenIngestor(open) = decoded.request else {
        panic!("an open request decodes as an open");
    };
    assert!(!open.limits.is_within_bounds());
    assert_eq!(open.expected_fields, producer_fields()[..1].to_vec());
}

#[test]
fn a_producer_identity_must_not_be_zero() {
    let mut builder = FlatBufferBuilder::new();
    let close = wire::CloseIngestorRequest::create(
        &mut builder,
        &wire::CloseIngestorRequestArgs { producer: 0 },
    );
    let frame = raw_client(client_frame(
        builder,
        wire::ClientRequest::CloseIngestorRequest,
        close.as_union_value(),
    ));
    assert_eq!(
        decode_error(ClientMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "CloseIngestorRequest.producer",
        }
    );
    let closed = ClientMessage {
        request_id: request(1),
        request: ClientRequest::CloseIngestor(CloseIngestorRequest {
            producer: producer(),
        }),
    };
    assert_eq!(round_trip_client(&closed), closed);
}

/// What one malformed opened reply sets in place of the valid sample value.
#[derive(Clone, Copy)]
enum OpenedDefect {
    ShortAttachment,
    ZeroTimeout,
    ShorterMaximumBackoff,
    BatchAboveGrant,
    MissingAdmission,
    ZeroParallelWindow,
}

/// A hand-built opened reply that breaks one rule.
fn opened_frame(defect: OpenedDefect) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let domain = builder.create_string("tenant");
    let ingestor = builder.create_string("orders_in");
    let field_name = builder.create_string("order_id");
    let scalar = wire::ScalarFieldType::create(
        &mut builder,
        &wire::ScalarFieldTypeArgs {
            scalar: Some(wire::ScalarType::U64),
        },
    );
    let field_type = wire::FieldType::create(
        &mut builder,
        &wire::FieldTypeArgs {
            shape_type: wire::FieldTypeShape::ScalarFieldType,
            shape: Some(scalar.as_union_value()),
        },
    );
    let field = wire::RowField::create(
        &mut builder,
        &wire::RowFieldArgs {
            name: Some(field_name),
            field_type: Some(field_type),
            nullable: false,
            sensitive: false,
        },
    );
    let fields = builder.create_vector(&[field]);
    let digest = builder.create_vector(&[0x11_u8; 32]);
    let contract = wire::Fingerprint::create(
        &mut builder,
        &wire::FingerprintArgs {
            bytes: Some(digest),
        },
    );
    let attachment_bytes: &[u8] = match defect {
        OpenedDefect::ShortAttachment => &[0x22; 15],
        _ => &[0x22; 16],
    };
    let attachment = builder.create_vector(attachment_bytes);
    let window_max = match defect {
        OpenedDefect::ZeroParallelWindow => 0,
        _ => 4,
    };
    let window = wire::ParallelProducerWindow::create(
        &mut builder,
        &wire::ParallelProducerWindowArgs { max: window_max },
    );
    let ack_timeout_nanos = match defect {
        OpenedDefect::ZeroTimeout => 0,
        _ => 30_000_000_000,
    };
    let retry_max_backoff_nanos = match defect {
        OpenedDefect::ShorterMaximumBackoff => 50_000_000,
        _ => 5_000_000_000,
    };
    let max_batch_bytes = match defect {
        OpenedDefect::BatchAboveGrant => 2048,
        _ => 1024,
    };
    let admission = match defect {
        OpenedDefect::MissingAdmission => None,
        _ => Some(wire::ProducerAdmission::Open),
    };
    let opened = wire::ProducerOpened::create(
        &mut builder,
        &wire::ProducerOpenedArgs {
            domain: Some(domain),
            ingestor: Some(ingestor),
            fields: Some(fields),
            generation: 3,
            contract: Some(contract),
            attachment: Some(attachment),
            window_type: wire::ProducerWindow::ParallelProducerWindow,
            window: Some(window.as_union_value()),
            ack_timeout_nanos,
            retry_backoff_nanos: 100_000_000,
            retry_max_backoff_nanos,
            granted_batches: 4,
            granted_bytes: 1024,
            max_batch_bytes,
            max_batch_rows: 65_536,
            admission,
        },
    );
    let message = builder.create_string("opened");
    let outcome = wire::OpenIngestorOutcome::create(
        &mut builder,
        &wire::OpenIngestorOutcomeArgs {
            disposition_type: wire::OpenIngestorDisposition::ProducerOpened,
            disposition: Some(opened.as_union_value()),
            message: Some(message),
        },
    );
    finish_reply(
        builder,
        wire::ReplyBody::OpenIngestorOutcome,
        outcome.as_union_value(),
    )
}

#[test]
fn a_malformed_opened_reply_is_refused_with_the_field_it_breaks() {
    let cases = [
        (
            OpenedDefect::ShortAttachment,
            WireDecodeError::InvalidValue {
                field: "ProducerOpened.attachment",
                kind: "16-byte attachment identity",
            },
        ),
        (
            OpenedDefect::ZeroTimeout,
            WireDecodeError::ZeroValue {
                field: "ProducerOpened.ack_timeout_nanos",
            },
        ),
        (
            OpenedDefect::ShorterMaximumBackoff,
            WireDecodeError::InvalidValue {
                field: "ProducerOpened.retry_max_backoff_nanos",
                kind: "longest backoff no shorter than the first",
            },
        ),
        (
            OpenedDefect::BatchAboveGrant,
            WireDecodeError::InvalidValue {
                field: "ProducerOpened.max_batch_bytes",
                kind: "batch size within the granted bytes",
            },
        ),
        (
            OpenedDefect::MissingAdmission,
            WireDecodeError::MissingField {
                field: "ProducerOpened.admission",
            },
        ),
        (
            OpenedDefect::ZeroParallelWindow,
            WireDecodeError::ZeroValue {
                field: "ParallelProducerWindow.max",
            },
        ),
    ];
    for (defect, expected) in cases {
        let frame = raw_server(opened_frame(defect));
        assert_eq!(decode_error(ServerMessage::decode(&frame)), expected);
    }
}

#[test]
fn an_undeclared_submission_refusal_is_refused() {
    let mut builder = FlatBufferBuilder::new();
    let not_admitted = wire::SubmissionNotAdmitted::create(
        &mut builder,
        &wire::SubmissionNotAdmittedArgs {
            refusal: Some(wire::SubmissionRefusal(
                wire::SubmissionRefusal::ENUM_MAX
                    .checked_add(1)
                    .assured("the schema leaves bytes undeclared"),
            )),
        },
    );
    let message = builder.create_string("");
    let outcome = wire::SubmissionOutcome::create(
        &mut builder,
        &wire::SubmissionOutcomeArgs {
            disposition_type: wire::SubmissionDisposition::SubmissionNotAdmitted,
            disposition: Some(not_admitted.as_union_value()),
            message: Some(message),
        },
    );
    let frame = raw_server(finish_reply(
        builder,
        wire::ReplyBody::SubmissionOutcome,
        outcome.as_union_value(),
    ));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::UnknownEnumValue {
            field: "SubmissionNotAdmitted.refusal",
            value: wire::SubmissionRefusal::ENUM_MAX + 1,
        }
    );
}
