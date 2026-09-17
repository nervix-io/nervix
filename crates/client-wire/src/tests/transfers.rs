//! Replies larger than one frame: splitting, reassembly, and every way a transfer can go wrong.

use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;
use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    fixtures::{checked, decode_error, finish_raw, raw_server, request, settings, size},
    samples::{command_outcome, impact_report, transaction},
};
use crate::{
    CommandDisposition, InspectionOutcome, Reply, ReplyBody, ReplyDelivery, ServerFrame,
    ServerMessage, SessionLimitSettings, SessionLimits, TransactionInspection, TransactionState,
    TransferAssembly, TransferError, TransferPart, VerifiedFrame, WireDecodeError, WireEncodeError,
    wire,
};

fn small_limits(frame_bytes: usize) -> SessionLimits {
    checked(SessionLimitSettings {
        frame_bytes: size(frame_bytes),
        string_bytes: size(1 << 20),
        ..settings()
    })
}

fn large_reply(request_id: u64) -> Reply {
    let mut outcome = command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    outcome.message = "large reply ".repeat(1500);
    Reply {
        request_id: request(request_id),
        body: ReplyBody::Command(Box::new(outcome)),
    }
}

fn parts(reply: &Reply, limits: &SessionLimits) -> Vec<TransferPart> {
    let ReplyDelivery::Transfer(parts) = reply
        .encode(limits)
        .assured("the reply fits the transfer limit")
    else {
        panic!("a reply larger than the frame limit is transferred");
    };
    parts
        .map(|part| {
            assert!(
                part.len() <= limits.frame_bytes(),
                "a part fits the frame limit"
            );
            let frame = part.verify(limits).assured("a part verifies as a frame");
            match ServerMessage::decode(&frame).assured("a part decodes") {
                ServerMessage::TransferPart(part) => part,
                other => panic!("a part decoded as {other:?}"),
            }
        })
        .collect()
}

#[test]
fn a_large_reply_is_split_into_parts_and_reassembled() {
    let limits = small_limits(1024);
    let reply = large_reply(11);
    let ReplyDelivery::Transfer(planned) = reply.encode(&limits).assured("the reply fits") else {
        panic!("a reply larger than the frame limit is transferred");
    };
    let total = planned.total_bytes();
    assert!(total > limits.frame_bytes());
    assert_eq!(planned.len(), total.div_ceil(1024 - 256));

    let parts = parts(&reply, &limits);
    let mut assembly = TransferAssembly::new(request(11), &limits);
    for (index, part) in parts.iter().enumerate() {
        assert_eq!(part.request_id(), request(11));
        assert_eq!(part.total_bytes(), total);
        assert!(!assembly.is_complete());
        assembly.append(part).assured("parts arrive in order");
        assert_eq!(index + 1 == parts.len(), assembly.is_complete());
    }
    assert_eq!(assembly.received_bytes(), total);
    assert_eq!(assembly.finish().assured("the transfer is complete"), reply);
}

#[test]
fn an_inspection_report_is_transferred_intact() {
    let limits = small_limits(2048);
    let reply = Reply {
        request_id: request(u64::MAX),
        body: ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(
            TransactionInspection {
                transaction: transaction(TransactionState::Open),
                operation: None,
                report: impact_report(),
            },
        ))),
    };
    let mut assembly = TransferAssembly::new(request(u64::MAX), &limits);
    for part in parts(&reply, &limits) {
        assembly.append(&part).assured("parts arrive in order");
    }
    assert_eq!(assembly.finish().assured("the transfer is complete"), reply);
}

#[test]
fn every_part_fits_the_frame_limit_at_awkward_sizes() {
    for frame_bytes in [1024, 1025, 1027, 1029, 1031, 4099] {
        let limits = small_limits(frame_bytes);
        let reply = large_reply(3);
        let parts = parts(&reply, &limits);
        let mut assembly = TransferAssembly::new(request(3), &limits);
        for part in &parts {
            assembly.append(part).assured("parts arrive in order");
        }
        assert_eq!(assembly.finish().assured("the transfer is complete"), reply);
    }
}

#[test]
fn a_reply_above_the_transfer_limit_is_refused() {
    let limits = checked(SessionLimitSettings {
        frame_bytes: size(1024),
        transfer_bytes: size(4096),
        string_bytes: size(4096),
        ..settings()
    });
    let mut outcome = command_outcome(CommandDisposition::Failed);
    outcome.message = "x".repeat(4000);
    let reply = Reply {
        request_id: request(1),
        body: ReplyBody::Command(Box::new(outcome)),
    };
    let error = reply
        .encode(&limits)
        .expect_err("the reply exceeds the transfer limit");
    assert!(matches!(
        error.current_context(),
        WireEncodeError::FrameTooLarge { limit: 4096, .. }
    ));
}

#[test]
fn parts_must_belong_to_the_transfer_and_arrive_in_order() {
    let limits = small_limits(1024);
    let parts = parts(&large_reply(5), &limits);
    assert!(parts.len() >= 3);

    let mut wrong_request = TransferAssembly::new(request(6), &limits);
    let error = wrong_request
        .append(&parts[0])
        .expect_err("another request's part");
    assert_eq!(
        error.current_context(),
        &TransferError::WrongRequest {
            expected: request(6),
            actual: request(5),
        }
    );

    let mut out_of_order = TransferAssembly::new(request(5), &limits);
    let error = out_of_order
        .append(&parts[1])
        .expect_err("a part before its predecessor");
    assert_eq!(
        error.current_context(),
        &TransferError::OutOfOrder {
            expected: 0,
            actual: parts[1].offset(),
        }
    );
    let error = out_of_order
        .append(&raw_part(5, 999_999, 5))
        .expect_err("a first part that does not start the reply");
    assert_eq!(
        error.current_context(),
        &TransferError::OutOfOrder {
            expected: 0,
            actual: 5,
        }
    );
    // Neither refused part's total was adopted, so the transfer still starts normally.
    out_of_order
        .append(&parts[0])
        .assured("a refused part leaves the assembly unchanged");
    assert_eq!(out_of_order.received_bytes(), parts[0].chunk().len());

    let mut repeated = TransferAssembly::new(request(5), &limits);
    repeated.append(&parts[0]).assured("the first part");
    let error = repeated.append(&parts[0]).expect_err("a repeated part");
    assert!(matches!(
        error.current_context(),
        TransferError::OutOfOrder { .. }
    ));

    let mut incomplete = TransferAssembly::new(request(5), &limits);
    incomplete.append(&parts[0]).assured("the first part");
    let error = incomplete.finish().expect_err("a transfer missing parts");
    assert_eq!(
        error.current_context(),
        &TransferError::Incomplete {
            received: parts[0].chunk().len(),
            total_bytes: parts[0].total_bytes(),
        }
    );
    let error = TransferAssembly::new(request(5), &limits)
        .finish()
        .expect_err("a transfer without parts");
    assert_eq!(error.current_context(), &TransferError::NoParts);

    let other = raw_part(5, 999_999, 0);
    let mut changed_total = TransferAssembly::new(request(5), &limits);
    changed_total.append(&parts[0]).assured("the first part");
    let error = changed_total
        .append(&other)
        .expect_err("a part declaring another total");
    assert!(matches!(
        error.current_context(),
        TransferError::TotalChanged { .. }
    ));
}

/// A hand-built part of request `request_id` at `offset` declaring `total_bytes`.
fn raw_part(request_id: u64, total_bytes: u64, offset: u64) -> TransferPart {
    let frame = VerifiedFrame::<ServerFrame>::verify(
        part_frame(request_id, total_bytes, offset, b"chunk"),
        &small_limits(1024),
    )
    .assured("the hand-built part is structurally valid");
    match ServerMessage::decode(&frame).assured("the hand-built part decodes") {
        ServerMessage::TransferPart(part) => part,
        other => panic!("a part decoded as {other:?}"),
    }
}

fn part_frame(request_id: u64, total_bytes: u64, offset: u64, chunk: &[u8]) -> Bytes {
    let mut builder = FlatBufferBuilder::new();
    let chunk = builder.create_vector(chunk);
    let part = wire::TransferPart::create(
        &mut builder,
        &wire::TransferPartArgs {
            total_bytes,
            offset,
            chunk: Some(chunk),
        },
    );
    let reply = wire::Reply::create(
        &mut builder,
        &wire::ReplyArgs {
            request_id,
            body_type: wire::ReplyBody::TransferPart,
            body: Some(part.as_union_value()),
        },
    );
    let root = wire::ServerMessage::create(
        &mut builder,
        &wire::ServerMessageArgs {
            body_type: wire::ServerBody::Reply,
            body: Some(reply.as_union_value()),
        },
    );
    finish_raw(builder, root, "NXSM")
}

#[test]
fn a_transfer_above_the_receivers_limit_is_refused_at_its_first_part() {
    let receiver = checked(SessionLimitSettings {
        frame_bytes: size(1024),
        transfer_bytes: size(2048),
        string_bytes: size(2048),
        ..settings()
    });
    let part = raw_part(1, 2049, 0);
    let mut assembly = TransferAssembly::new(request(1), &receiver);
    let error = assembly
        .append(&part)
        .expect_err("the declared size exceeds the limit");
    assert_eq!(
        error.current_context(),
        &TransferError::TooLarge {
            total_bytes: 2049,
            limit: 2048,
        }
    );
}

#[test]
fn malformed_parts_are_refused() {
    let frame = raw_server(part_frame(1, 0, 0, b"chunk"));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "TransferPart.total_bytes",
        }
    );
    let frame = raw_server(part_frame(1, 10, 0, b""));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::EmptyCollection {
            field: "TransferPart.chunk",
        }
    );
    let frame = raw_server(part_frame(1, 10, 6, b"chunk"));
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "TransferPart.chunk",
            kind: "chunk within the transfer's total bytes",
        }
    );
    let frame = raw_server(part_frame(1, 10, 5, b"chunk"));
    assert!(ServerMessage::decode(&frame).is_ok());
    let frame = raw_server(part_frame(0, 10, 0, b"chunk"));
    assert_eq!(frame.request_id(), None);
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "Reply.request_id",
        }
    );
}

#[test]
fn a_reassembled_frame_must_be_a_complete_reply_to_its_request() {
    let limits = small_limits(1024);
    let nested = part_frame(9, 64, 0, b"nested");
    let error = assemble_bytes(9, nested, &limits);
    assert_eq!(
        error,
        TransferError::NotReply {
            expected: request(9),
        }
    );

    let other_request = match large_reply(10).encode(&limits).assured("the reply fits") {
        ReplyDelivery::Transfer(parts) => parts.fold(Vec::new(), |mut bytes, part| {
            let frame = part.verify(&limits).assured("a part verifies");
            let ServerMessage::TransferPart(part) =
                ServerMessage::decode(&frame).assured("a part decodes")
            else {
                panic!("a part decodes as a part");
            };
            bytes.extend_from_slice(part.chunk());
            bytes
        }),
        ReplyDelivery::Frame(_) => panic!("the reply is transferred"),
    };
    let error = assemble_bytes(9, Bytes::from(other_request), &limits);
    assert_eq!(
        error,
        TransferError::NotReply {
            expected: request(9),
        }
    );

    let error = assemble_bytes(
        9,
        Bytes::from_static(b"this is not a frame at all"),
        &limits,
    );
    assert_eq!(error, TransferError::InvalidReply);
}

/// Sends `bytes` as the whole of a one-part transfer for `request_id` and returns why it failed.
fn assemble_bytes(request_id: u64, bytes: Bytes, limits: &SessionLimits) -> TransferError {
    let total = u64::try_from(bytes.len()).assured("test payloads are small");
    let mut assembly = TransferAssembly::new(request(request_id), limits);
    let mut offset = 0;
    for chunk in bytes.chunks(512) {
        let frame = VerifiedFrame::<ServerFrame>::verify(
            part_frame(request_id, total, offset, chunk),
            limits,
        )
        .assured("the hand-built part is structurally valid");
        let ServerMessage::TransferPart(part) =
            ServerMessage::decode(&frame).assured("the hand-built part decodes")
        else {
            panic!("a part decodes as a part");
        };
        assembly.append(&part).assured("the parts arrive in order");
        offset = offset
            .checked_add(u64::try_from(chunk.len()).assured("test chunks are small"))
            .assured("test offsets are small");
    }
    let chunks = bytes.chunks(512).count();
    assert!(chunks > 0);
    let error = assembly
        .finish()
        .expect_err("the reassembled frame is refused");
    error.current_context().clone()
}

#[test]
fn a_part_reads_its_chunk_in_place() {
    let limits = small_limits(1024);
    let part = parts(&large_reply(2), &limits)
        .into_iter()
        .next()
        .assured("the reply has parts");
    assert_eq!(part.offset(), 0);
    assert!(!part.chunk().is_empty());
    let frame = part_frame(2, 64, 0, b"in place");
    let verified = raw_server(frame.clone());
    let ServerMessage::TransferPart(decoded) =
        ServerMessage::decode(&verified).assured("the part decodes")
    else {
        panic!("a part decodes as a part");
    };
    let start = verified.bytes().as_ptr().addr();
    let chunk = decoded.chunk();
    assert_eq!(chunk, b"in place");
    assert!((start..start + verified.len()).contains(&chunk.as_ptr().addr()));
    assert!(decoded.request_id().get().get() == 2);
}
