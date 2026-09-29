//! Restores: every frame of the stream that carries an archive, the reply that answers it, and the
//! report a restore's outcome carries.

use flatbuffers::FlatBufferBuilder;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ArchiveDigest, RestoreArchive, RestoreMode, RestoreReport, RestoreStep, RestoreStepOutcome,
    RestoreStepReport, RestoredDomain, RestoredUsers, Timestamp,
};

use super::{
    fixtures::{decode_error, finish_raw, limits, name, non_zero, reference, request, verify_raw},
    samples::{command_outcome, impact_report},
};
use crate::{
    CommandDisposition, CommandOutcome, LeaderRedirect, Reply, ReplyBody, ReplyDelivery,
    RestoreDisposition, RestoreFrame, RestoreMessage, RestoreReply, RestoreReplyFrame,
    RestoreStart, RestoreUploadFailure, ServerFrame, ServerMessage, VerifiedFrame, WireDecodeError,
    WireEncodeError, restore::RestoreChunk, wire,
};

fn decode(frame: crate::EncodedFrame<RestoreFrame>) -> RestoreMessage {
    let frame = frame.verify(&limits()).assured("an encoded frame verifies");
    RestoreMessage::decode(&frame).assured("an encoded restore frame decodes")
}

fn archive() -> RestoreArchive {
    RestoreArchive {
        total_bytes: non_zero(u64::MAX),
        digest: ArchiveDigest::from_bytes([0x5c; 32]),
    }
}

fn start() -> RestoreStart {
    RestoreStart {
        request_id: request(1),
        execution_reference: reference("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"),
        statement: "RESTORE DOMAIN tenant AS tenant_copy FROM './tenant.nvxb' DRY RUN;".to_string(),
        archive: archive(),
    }
}

/// A report of every step outcome, both user counts at their extremes, and a planned model run.
fn report(mode: RestoreMode) -> RestoreReport {
    let planned_models = match mode {
        RestoreMode::Apply => None,
        RestoreMode::DryRun => Some(impact_report()),
    };
    RestoreReport {
        mode,
        archive: archive(),
        captured_at: Timestamp::from_unix_nanos(i64::MIN),
        users: Some(RestoredUsers {
            created: u64::MAX,
            skipped: 0,
            replaced: 7,
        }),
        domains: vec![
            RestoredDomain {
                source: name("tenant"),
                domain: name("tenant_copy"),
                resource_versions: 3,
                models: 12,
                planned_models,
            },
            RestoredDomain {
                source: name("analytics"),
                domain: name("analytics"),
                resource_versions: 0,
                models: 0,
                planned_models: None,
            },
        ],
        steps: vec![
            RestoreStepReport {
                step: RestoreStep::Users,
                outcome: RestoreStepOutcome::Applied,
            },
            RestoreStepReport {
                step: RestoreStep::CreateDomain(name("tenant_copy")),
                outcome: RestoreStepOutcome::Planned,
            },
            RestoreStepReport {
                step: RestoreStep::ImportResources(name("tenant_copy")),
                outcome: RestoreStepOutcome::Failed,
            },
            RestoreStepReport {
                step: RestoreStep::ApplyModels(name("tenant_copy")),
                outcome: RestoreStepOutcome::NotAttempted,
            },
        ],
    }
}

#[test]
fn a_restore_start_round_trips() {
    let RestoreMessage::Start(decoded) =
        decode(start().encode(&limits()).assured("a start fits the limits"))
    else {
        panic!("a start decodes as a start");
    };
    assert_eq!(decoded, start());
}

#[test]
fn every_restore_chunk_round_trips_and_an_empty_one_is_refused() {
    for bytes in [&[0_u8][..], &[0xa5; 4096][..]] {
        let RestoreMessage::Chunk(chunk) =
            decode(RestoreChunk::encode(bytes, &limits()).assured("a chunk fits the limits"))
        else {
            panic!("a chunk decodes as a chunk");
        };
        assert_eq!(chunk.bytes(), bytes);
        assert_eq!(&chunk.shared_bytes()[..], bytes);
    }
    let error = RestoreChunk::encode(&[], &limits()).expect_err("an empty chunk carries nothing");
    assert_eq!(
        error.current_context(),
        &WireEncodeError::EmptyCollection {
            field: "RestoreChunk.bytes",
        }
    );

    let mut builder = FlatBufferBuilder::new();
    let bytes = builder.create_vector::<u8>(&[]);
    let chunk =
        wire::RestoreChunk::create(&mut builder, &wire::RestoreChunkArgs { bytes: Some(bytes) });
    let root = wire::RestoreMessage::create(
        &mut builder,
        &wire::RestoreMessageArgs {
            part_type: wire::RestorePart::RestoreChunk,
            part: Some(chunk.as_union_value()),
        },
    );
    let frame: VerifiedFrame<RestoreFrame> = verify_raw(finish_raw(builder, root, "NXRM"));
    assert_eq!(
        decode_error(RestoreMessage::decode(&frame)),
        WireDecodeError::EmptyCollection {
            field: "RestoreChunk.bytes",
        }
    );
}

/// A raw restore start with `total_bytes` and `reference`.
fn raw_start(total_bytes: u64, reference: &str) -> VerifiedFrame<RestoreFrame> {
    let mut builder = FlatBufferBuilder::new();
    let reference = builder.create_string(reference);
    let statement = builder.create_string("RESTORE CLUSTER FROM 'c.nvxb';");
    let digest_bytes = builder.create_vector(&[0_u8; 32]);
    let digest = wire::Fingerprint::create(
        &mut builder,
        &wire::FingerprintArgs {
            bytes: Some(digest_bytes),
        },
    );
    let start = wire::RestoreStart::create(
        &mut builder,
        &wire::RestoreStartArgs {
            request_id: 1,
            execution_reference: Some(reference),
            statement: Some(statement),
            total_bytes,
            digest: Some(digest),
        },
    );
    let root = wire::RestoreMessage::create(
        &mut builder,
        &wire::RestoreMessageArgs {
            part_type: wire::RestorePart::RestoreStart,
            part: Some(start.as_union_value()),
        },
    );
    verify_raw(finish_raw(builder, root, "NXRM"))
}

#[test]
fn a_start_of_zero_bytes_or_with_an_invalid_reference_is_refused() {
    assert_eq!(
        decode_error(RestoreMessage::decode(&raw_start(
            0,
            "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44"
        ))),
        WireDecodeError::ZeroValue {
            field: "RestoreStart.total_bytes",
        }
    );
    assert_eq!(
        decode_error(RestoreMessage::decode(&raw_start(1, "not a reference"))),
        WireDecodeError::InvalidValue {
            field: "RestoreStart.execution_reference",
            kind: "execution reference",
        }
    );
}

fn round_trip_reply(reply: &RestoreReply) -> RestoreReply {
    let frame: VerifiedFrame<RestoreReplyFrame> = reply
        .encode(&limits())
        .assured("a reply fits the limits")
        .verify(&limits())
        .assured("an encoded reply verifies");
    RestoreReply::decode(&frame).assured("an encoded reply decodes")
}

#[test]
fn every_restore_reply_round_trips() {
    let mut replies = Vec::new();
    for mode in [RestoreMode::Apply, RestoreMode::DryRun] {
        replies.push(RestoreReply {
            request_id: Some(request(1)),
            disposition: RestoreDisposition::Outcome(Box::new(CommandOutcome {
                restore: Some(Box::new(report(mode))),
                ..command_outcome(CommandDisposition::Completed {
                    already_existed: false,
                })
            })),
        });
    }
    replies.push(RestoreReply {
        request_id: Some(request(2)),
        disposition: RestoreDisposition::Outcome(Box::new(command_outcome(
            CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
        ))),
    });
    for failure in [
        RestoreUploadFailure::InvalidStream,
        RestoreUploadFailure::InvalidStatement,
        RestoreUploadFailure::SizeMismatch,
        RestoreUploadFailure::DigestMismatch,
        RestoreUploadFailure::QuotaExceeded,
        RestoreUploadFailure::StagingFailed,
    ] {
        replies.push(RestoreReply {
            request_id: None,
            disposition: RestoreDisposition::UploadFailed {
                failure,
                message: format!("refused as {failure:?}"),
            },
        });
    }
    for reply in replies {
        assert_eq!(round_trip_reply(&reply), reply);
    }
}

#[test]
fn a_restore_outcome_carries_its_report_in_a_session_reply() {
    let outcome = CommandOutcome {
        restore: Some(Box::new(report(RestoreMode::Apply))),
        ..command_outcome(CommandDisposition::Failed)
    };
    let ReplyDelivery::Frame(frame) = Reply {
        request_id: request(9),
        body: ReplyBody::Command(Box::new(outcome.clone())),
    }
    .encode(&limits())
    .assured("the reply fits the limits") else {
        panic!("the reply fits one frame");
    };
    let frame: VerifiedFrame<ServerFrame> =
        frame.verify(&limits()).assured("an encoded reply verifies");
    let ServerMessage::Reply(reply) =
        ServerMessage::decode(&frame).assured("an encoded reply decodes")
    else {
        panic!("a reply decodes as a reply");
    };
    let ReplyBody::Command(decoded) = reply.body else {
        panic!("a command reply decodes as one");
    };
    assert_eq!(*decoded, outcome);
}

/// A reply whose report holds one step of `kind`, naming `domain` when one is given.
fn reply_with_step(
    kind: wire::RestoreStepKind,
    domain: Option<&str>,
) -> VerifiedFrame<RestoreReplyFrame> {
    let mut builder = FlatBufferBuilder::new();
    let domain = domain.map(|domain| builder.create_string(domain));
    let step = wire::RestoreStepReport::create(
        &mut builder,
        &wire::RestoreStepReportArgs {
            kind: Some(kind),
            domain,
            outcome: Some(wire::RestoreStepOutcome::Applied),
        },
    );
    let steps = builder.create_vector(&[step]);
    let domains = builder.create_vector::<flatbuffers::ForwardsUOffset<wire::RestoredDomain>>(&[]);
    let digest_bytes = builder.create_vector(&[1_u8; 32]);
    let digest = wire::Fingerprint::create(
        &mut builder,
        &wire::FingerprintArgs {
            bytes: Some(digest_bytes),
        },
    );
    let report = wire::RestoreReport::create(
        &mut builder,
        &wire::RestoreReportArgs {
            mode: Some(wire::RestoreMode::Apply),
            total_bytes: 1,
            digest: Some(digest),
            captured_at: 0,
            users: None,
            domains: Some(domains),
            steps: Some(steps),
        },
    );
    let reference = builder.create_string("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44");
    let message = builder.create_string("restored");
    let diagnostics = builder.create_vector::<flatbuffers::ForwardsUOffset<wire::Diagnostic>>(&[]);
    let statements =
        builder.create_vector::<flatbuffers::ForwardsUOffset<wire::StatementOutcome>>(&[]);
    let completed = wire::CommandCompleted::create(
        &mut builder,
        &wire::CommandCompletedArgs {
            already_existed: false,
        },
    );
    let outcome = wire::CommandOutcome::create(
        &mut builder,
        &wire::CommandOutcomeArgs {
            execution_reference: Some(reference),
            origin: Some(wire::OutcomeOrigin::Executed),
            disposition_type: wire::CommandDisposition::CommandCompleted,
            disposition: Some(completed.as_union_value()),
            message: Some(message),
            diagnostics: Some(diagnostics),
            statements: Some(statements),
            restore: Some(report),
            ..Default::default()
        },
    );
    let root = wire::RestoreReply::create(
        &mut builder,
        &wire::RestoreReplyArgs {
            request_id: Some(1),
            disposition_type: wire::RestoreDisposition::CommandOutcome,
            disposition: Some(outcome.as_union_value()),
        },
    );
    verify_raw(finish_raw(builder, root, "NXRR"))
}

#[test]
fn a_step_names_a_domain_exactly_when_it_changes_one() {
    for (kind, domain) in [
        (wire::RestoreStepKind::Users, Some("tenant")),
        (wire::RestoreStepKind::CreateDomain, None),
        (wire::RestoreStepKind::ImportResources, None),
        (wire::RestoreStepKind::ApplyModels, None),
    ] {
        assert_eq!(
            decode_error(RestoreReply::decode(&reply_with_step(kind, domain))),
            WireDecodeError::InvalidValue {
                field: "RestoreStepReport.domain",
                kind: "a domain for every step but the users step",
            },
            "{kind:?} with {domain:?}"
        );
    }
    let decoded = RestoreReply::decode(&reply_with_step(wire::RestoreStepKind::Users, None))
        .assured("the users step names no domain");
    let RestoreDisposition::Outcome(outcome) = decoded.disposition else {
        panic!("the reply carries an outcome");
    };
    let report = outcome.restore.assured("the outcome carries its report");
    assert_eq!(report.steps[0].step, RestoreStep::Users);
}
