//! Backup downloads: the request, every frame of the stream that answers it, and the summary a
//! completed backup's outcome carries.

use flatbuffers::FlatBufferBuilder;
use meticulous::ResultExt as _;
use nervix_models::{
    ArchiveDigest, BackupArchiveSummary, BackupDomainSummary, BackupResources,
    CommandExecutionReference, Timestamp,
};

use super::{
    fixtures::{decode_error, finish_raw, limits, name, non_zero, verify_raw},
    samples::{command_outcome, leader},
};
use crate::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame,
    BackupDownloadMessage, BackupDownloadRequest, BackupDownloadRequestFrame, CommandDisposition,
    CommandOutcome, LeaderRedirect, Reply, ReplyBody, ReplyDelivery, ServerFrame, ServerMessage,
    VerifiedFrame, WireDecodeError, WireEncodeError, wire,
};

fn decode(frame: crate::EncodedFrame<BackupDownloadFrame>) -> BackupDownloadMessage {
    let frame = frame.verify(&limits()).assured("an encoded frame verifies");
    BackupDownloadMessage::decode(&frame).assured("an encoded download frame decodes")
}

fn summary(domains: &[&str]) -> BackupArchiveSummary {
    BackupArchiveSummary {
        total_bytes: non_zero(4096),
        digest: ArchiveDigest::from_bytes([7; 32]),
        captured_at: Timestamp::from_unix_nanos(1_790_000_000_000_000_000),
        retained_until: Timestamp::from_unix_nanos(1_790_000_900_000_000_000),
        resources: BackupResources::Included,
        users: None,
        domains: domains
            .iter()
            .map(|domain| BackupDomainSummary {
                domain: name(domain),
                revision: 42,
                sections: 3,
                section_bytes: 1024,
            })
            .collect(),
    }
}

#[test]
fn a_download_request_round_trips() {
    let request = BackupDownloadRequest {
        execution_reference: CommandExecutionReference::parse(
            "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44",
        )
        .assured("the test reference is valid"),
    };
    let frame = request
        .encode(&limits())
        .assured("a request fits the limits")
        .verify(&limits())
        .assured("an encoded request verifies");
    assert_eq!(
        BackupDownloadRequest::decode(&frame).assured("an encoded request decodes"),
        request
    );
}

#[test]
fn a_download_request_with_an_invalid_reference_is_refused() {
    let mut builder = FlatBufferBuilder::new();
    let reference = builder.create_string("not a reference");
    let root = wire::BackupDownloadRequest::create(
        &mut builder,
        &wire::BackupDownloadRequestArgs {
            execution_reference: Some(reference),
        },
    );
    let frame: VerifiedFrame<BackupDownloadRequestFrame> =
        verify_raw(finish_raw(builder, root, "NXBQ"));
    assert_eq!(
        decode_error(BackupDownloadRequest::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "BackupDownloadRequest.execution_reference",
            kind: "execution reference",
        }
    );
}

#[test]
fn every_download_frame_round_trips() {
    let start = BackupArchiveStart {
        total_bytes: non_zero(u64::MAX),
        digest: ArchiveDigest::from_bytes([0xa5; 32]),
    };
    let BackupDownloadMessage::Start(decoded) = decode(
        BackupDownloadMessage::encode_start(&start, &limits()).assured("a start fits the limits"),
    ) else {
        panic!("a start decodes as a start");
    };
    assert_eq!(decoded, start);

    for bytes in [&[0_u8][..], &[0xff; 4096][..]] {
        let BackupDownloadMessage::Chunk(chunk) = decode(
            BackupDownloadMessage::encode_chunk(bytes, &limits())
                .assured("a chunk fits the limits"),
        ) else {
            panic!("a chunk decodes as a chunk");
        };
        assert_eq!(chunk.bytes(), bytes);
        assert_eq!(&chunk.shared_bytes()[..], bytes);
    }

    assert!(matches!(
        decode(BackupDownloadMessage::encode_complete(&limits()).assured("completion fits")),
        BackupDownloadMessage::Complete
    ));

    for failure in [
        BackupDownloadFailure::InvalidRequest,
        BackupDownloadFailure::NotRetained,
        BackupDownloadFailure::Expired,
        BackupDownloadFailure::NotOwner,
        BackupDownloadFailure::ReadFailed,
    ] {
        let failed = BackupDownloadFailed {
            failure,
            message: format!("refused as {failure:?}"),
        };
        let BackupDownloadMessage::Failed(decoded) = decode(
            BackupDownloadMessage::encode_failed(&failed, &limits())
                .assured("a refusal fits the limits"),
        ) else {
            panic!("a refusal decodes as a refusal");
        };
        assert_eq!(decoded, failed);
    }

    for redirect in [
        LeaderRedirect { leader: None },
        LeaderRedirect {
            leader: Some(leader()),
        },
    ] {
        let BackupDownloadMessage::NotLeader(decoded) = decode(
            BackupDownloadMessage::encode_redirect(&redirect, &limits())
                .assured("a redirect fits the limits"),
        ) else {
            panic!("a redirect decodes as a redirect");
        };
        assert_eq!(decoded, redirect);
    }
}

#[test]
fn an_empty_chunk_is_neither_written_nor_read() {
    let error = BackupDownloadMessage::encode_chunk(&[], &limits())
        .expect_err("an empty chunk carries nothing");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::EmptyCollection {
            field: "BackupArchiveChunk.bytes",
        }
    );

    let mut builder = FlatBufferBuilder::new();
    let bytes = builder.create_vector::<u8>(&[]);
    let chunk = wire::BackupArchiveChunk::create(
        &mut builder,
        &wire::BackupArchiveChunkArgs { bytes: Some(bytes) },
    );
    let root = wire::BackupDownloadMessage::create(
        &mut builder,
        &wire::BackupDownloadMessageArgs {
            part_type: wire::BackupDownloadPart::BackupArchiveChunk,
            part: Some(chunk.as_union_value()),
        },
    );
    let frame: VerifiedFrame<BackupDownloadFrame> = verify_raw(finish_raw(builder, root, "NXBD"));
    assert_eq!(
        decode_error(BackupDownloadMessage::decode(&frame)),
        WireDecodeError::EmptyCollection {
            field: "BackupArchiveChunk.bytes",
        }
    );
}

#[test]
fn a_start_of_zero_bytes_and_a_refusal_without_its_reason_are_refused() {
    let mut builder = FlatBufferBuilder::new();
    let digest_bytes = builder.create_vector(&[0_u8; 32]);
    let digest = wire::Fingerprint::create(
        &mut builder,
        &wire::FingerprintArgs {
            bytes: Some(digest_bytes),
        },
    );
    let start = wire::BackupArchiveStart::create(
        &mut builder,
        &wire::BackupArchiveStartArgs {
            total_bytes: 0,
            digest: Some(digest),
        },
    );
    let root = wire::BackupDownloadMessage::create(
        &mut builder,
        &wire::BackupDownloadMessageArgs {
            part_type: wire::BackupDownloadPart::BackupArchiveStart,
            part: Some(start.as_union_value()),
        },
    );
    let frame: VerifiedFrame<BackupDownloadFrame> = verify_raw(finish_raw(builder, root, "NXBD"));
    assert_eq!(
        decode_error(BackupDownloadMessage::decode(&frame)),
        WireDecodeError::ZeroValue {
            field: "BackupArchiveStart.total_bytes",
        }
    );

    let mut builder = FlatBufferBuilder::new();
    let message = builder.create_string("refused");
    let failed = wire::BackupDownloadFailed::create(
        &mut builder,
        &wire::BackupDownloadFailedArgs {
            failure: None,
            message: Some(message),
        },
    );
    let root = wire::BackupDownloadMessage::create(
        &mut builder,
        &wire::BackupDownloadMessageArgs {
            part_type: wire::BackupDownloadPart::BackupDownloadFailed,
            part: Some(failed.as_union_value()),
        },
    );
    let frame: VerifiedFrame<BackupDownloadFrame> = verify_raw(finish_raw(builder, root, "NXBD"));
    assert_eq!(
        decode_error(BackupDownloadMessage::decode(&frame)),
        WireDecodeError::MissingField {
            field: "BackupDownloadFailed.failure",
        }
    );
}

fn command_reply(outcome: CommandOutcome) -> ServerMessage {
    let ReplyDelivery::Frame(frame) = Reply {
        request_id: super::fixtures::request(9),
        body: ReplyBody::Command(Box::new(outcome)),
    }
    .encode(&limits())
    .assured("the reply fits the limits") else {
        panic!("the reply fits one frame");
    };
    let frame: VerifiedFrame<ServerFrame> =
        frame.verify(&limits()).assured("an encoded reply verifies");
    ServerMessage::decode(&frame).assured("an encoded reply decodes")
}

#[test]
fn a_completed_backup_outcome_carries_its_summary() {
    for summary in [summary(&[]), summary(&["analytics", "tenant"])] {
        let outcome = CommandOutcome {
            backup: Some(Box::new(summary.clone())),
            ..command_outcome(CommandDisposition::Completed {
                already_existed: false,
            })
        };
        let ServerMessage::Reply(reply) = command_reply(outcome.clone()) else {
            panic!("a reply decodes as a reply");
        };
        let ReplyBody::Command(decoded) = reply.body else {
            panic!("a command reply decodes as one");
        };
        assert_eq!(*decoded, outcome);
    }
}

#[test]
fn a_summary_with_unordered_domains_is_refused() {
    let outcome = CommandOutcome {
        backup: Some(Box::new(summary(&["tenant", "analytics"]))),
        ..command_outcome(CommandDisposition::Completed {
            already_existed: false,
        })
    };
    let ReplyDelivery::Frame(frame) = Reply {
        request_id: super::fixtures::request(9),
        body: ReplyBody::Command(Box::new(outcome)),
    }
    .encode(&limits())
    .assured("the reply fits the limits") else {
        panic!("the reply fits one frame");
    };
    let frame: VerifiedFrame<ServerFrame> =
        frame.verify(&limits()).assured("an encoded reply verifies");
    assert_eq!(
        decode_error(ServerMessage::decode(&frame)),
        WireDecodeError::InvalidValue {
            field: "BackupArchiveSummary.domains",
            kind: "strictly ascending domain names",
        }
    );
}
