//! Resource upload frames: the stream's start and chunks, and every upload outcome.

use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::ResourceUploadIdentity;

use super::{
    fixtures::{decode_error, finish_raw, limits, name, non_zero, request, verify_raw},
    samples::{diagnostics, leader},
};
use crate::{
    LeaderRedirect, OutcomeOrigin, UploadChunk, UploadDisposition, UploadFailure, UploadFrame,
    UploadMessage, UploadReply, UploadReplyFrame, UploadStart, VerifiedFrame, WireDecodeError,
    WireEncodeError, wire,
};

fn identity(raw: &str) -> ResourceUploadIdentity {
    ResourceUploadIdentity::parse(raw).assured("the test passes a valid upload identity")
}

fn decode_upload(frame: crate::EncodedFrame<UploadFrame>) -> UploadMessage {
    let frame = frame.verify(&limits()).assured("an encoded frame verifies");
    UploadMessage::decode(&frame).assured("an encoded upload frame decodes")
}

#[test]
fn an_upload_start_round_trips_at_its_bounds() {
    for (id, total) in [(1, 1), (u64::MAX, u64::MAX)] {
        let start = UploadStart {
            request_id: request(id),
            domain: name("tenant"),
            resource: name("model"),
            upload_identity: identity(&"u".repeat(128)),
            total_bytes: non_zero(total),
        };
        let UploadMessage::Start(decoded) =
            decode_upload(start.encode(&limits()).assured("a start fits the limits"))
        else {
            panic!("a start decodes as a start");
        };
        assert_eq!(decoded, start);
    }
}

#[test]
fn a_chunk_is_read_in_place_or_shared_without_copying() {
    for bytes in [&[0_u8][..], &[0xFF; 4096][..], b"archive"] {
        let frame = UploadChunk::encode(bytes, &limits())
            .assured("a chunk fits the limits")
            .verify(&limits())
            .assured("an encoded frame verifies");
        let UploadMessage::Chunk(chunk) =
            UploadMessage::decode(&frame).assured("a chunk frame decodes")
        else {
            panic!("a chunk decodes as a chunk");
        };
        assert_eq!(chunk.bytes(), bytes);
        let shared = chunk.shared_bytes();
        assert_eq!(&shared[..], bytes);
        let start = frame.bytes().as_ptr().addr();
        assert!((start..start + frame.len()).contains(&shared.as_ptr().addr()));
    }
    let error = UploadChunk::encode(&[], &limits()).expect_err("a chunk holds bytes");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::EmptyCollection {
            field: "UploadChunk.bytes",
        }
    );
}

fn upload_frame(
    part: impl FnOnce(
        &mut FlatBufferBuilder<'static>,
    ) -> (
        wire::UploadPart,
        flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ),
) -> VerifiedFrame<UploadFrame> {
    let mut builder = FlatBufferBuilder::new();
    let (part_type, part) = part(&mut builder);
    let root = wire::UploadMessage::create(
        &mut builder,
        &wire::UploadMessageArgs {
            part_type,
            part: Some(part),
        },
    );
    verify_raw(finish_raw(builder, root, "NXUM"))
}

#[test]
fn malformed_upload_frames_are_refused() {
    let frame = upload_frame(|builder| {
        let bytes = builder.create_vector::<u8>(&[]);
        let chunk =
            wire::UploadChunk::create(builder, &wire::UploadChunkArgs { bytes: Some(bytes) });
        (wire::UploadPart::UploadChunk, chunk.as_union_value())
    });
    assert_eq!(
        decode_error(UploadMessage::decode(&frame)),
        WireDecodeError::EmptyCollection {
            field: "UploadChunk.bytes",
        }
    );

    let start = |total_bytes: u64, upload_identity: &'static str| {
        move |builder: &mut FlatBufferBuilder<'static>| {
            let domain = builder.create_string("tenant");
            let resource = builder.create_string("model");
            let upload_identity = builder.create_string(upload_identity);
            let start = wire::UploadStart::create(
                builder,
                &wire::UploadStartArgs {
                    request_id: 1,
                    domain: Some(domain),
                    resource: Some(resource),
                    upload_identity: Some(upload_identity),
                    total_bytes,
                },
            );
            (wire::UploadPart::UploadStart, start.as_union_value())
        }
    };
    let frame = upload_frame(start(0, "upload-1"));
    assert_eq!(
        decode_error(UploadMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "UploadStart.total_bytes",
        }
    );
    let frame = upload_frame(start(1, "not valid"));
    assert_eq!(
        decode_error(UploadMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "UploadStart.upload_identity",
            kind: "upload identity",
        }
    );
    let frame = upload_frame(|builder| {
        let bytes = builder.create_vector::<u8>(&[1]);
        let chunk =
            wire::UploadChunk::create(builder, &wire::UploadChunkArgs { bytes: Some(bytes) });
        (wire::UploadPart(3), chunk.as_union_value())
    });
    assert_eq!(
        decode_error(UploadMessage::decode(&frame)),
        WireDecodeError::UnknownUnionVariant {
            field: "UploadMessage.part",
            discriminant: 3,
        }
    );
}

#[test]
fn every_upload_outcome_round_trips() {
    let dispositions = [
        UploadDisposition::Installed {
            upload_identity: identity("upload-1"),
            version: non_zero(1),
            origin: OutcomeOrigin::Executed,
        },
        UploadDisposition::Installed {
            upload_identity: identity("upload-1"),
            version: non_zero(u64::MAX),
            origin: OutcomeOrigin::Recovered,
        },
        UploadDisposition::Failed {
            upload_identity: None,
            failure: UploadFailure::InvalidStream,
            assigned_version: None,
        },
        UploadDisposition::Failed {
            upload_identity: Some(identity("upload-2")),
            failure: UploadFailure::ResourceNotDeclared,
            assigned_version: None,
        },
        UploadDisposition::Failed {
            upload_identity: Some(identity("upload-2")),
            failure: UploadFailure::SizeMismatch,
            assigned_version: None,
        },
        UploadDisposition::Failed {
            upload_identity: Some(identity("upload-2")),
            failure: UploadFailure::QuotaExceeded,
            assigned_version: None,
        },
        UploadDisposition::Failed {
            upload_identity: Some(identity("upload-2")),
            failure: UploadFailure::InstallationFailed,
            assigned_version: Some(non_zero(u64::MAX)),
        },
        UploadDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader()),
        }),
        UploadDisposition::NotLeader(LeaderRedirect { leader: None }),
    ];
    for (disposition, request_id) in dispositions
        .into_iter()
        .zip([None, Some(request(u64::MAX))].into_iter().cycle())
    {
        let reply = UploadReply {
            request_id,
            disposition,
            message: "uploaded resource version 1".to_string(),
            diagnostics: diagnostics(),
        };
        let frame = reply
            .encode(&limits())
            .assured("a reply fits the limits")
            .verify(&limits())
            .assured("an encoded frame verifies");
        assert_eq!(
            UploadReply::decode(&frame).assured("an encoded reply decodes"),
            reply
        );
    }
}

fn reply_frame(request_id: Option<u64>, version: u64) -> VerifiedFrame<UploadReplyFrame> {
    let mut builder = FlatBufferBuilder::new();
    let upload_identity = builder.create_string("upload-1");
    let installed = wire::ResourceInstalled::create(
        &mut builder,
        &wire::ResourceInstalledArgs {
            upload_identity: Some(upload_identity),
            version,
            origin: Some(wire::OutcomeOrigin::Executed),
        },
    );
    let message = builder.create_string("");
    let diagnostics = builder.create_vector::<flatbuffers::WIPOffset<wire::Diagnostic>>(&[]);
    let root = wire::UploadReply::create(
        &mut builder,
        &wire::UploadReplyArgs {
            request_id,
            disposition_type: wire::UploadDisposition::ResourceInstalled,
            disposition: Some(installed.as_union_value()),
            message: Some(message),
            diagnostics: Some(diagnostics),
        },
    );
    verify_raw(finish_raw(builder, root, "NXUR"))
}

#[test]
fn malformed_upload_replies_are_refused() {
    assert!(UploadReply::decode(&reply_frame(Some(1), 1)).is_ok());
    assert_eq!(
        decode_error(UploadReply::decode(&reply_frame(Some(0), 1))),
        WireDecodeError::ZeroValue {
            field: "UploadReply.request_id",
        }
    );
    assert_eq!(
        decode_error(UploadReply::decode(&reply_frame(None, 0))),
        WireDecodeError::ZeroValue {
            field: "ResourceInstalled.version",
        }
    );
}
