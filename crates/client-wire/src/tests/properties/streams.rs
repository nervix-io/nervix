//! Every current stream root, retained chunk and complete reply transfer.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current frame cases and their complete representation assertions.
//! - **Depends on.** Production wire codecs and vocabulary test generators.
//! - **Must not know.** Runtime Arrow arrays, service dispatch or live external systems.

use bytes::Bytes;
use meticulous::ResultExt as _;
use nervix_models::{ArchiveDigest, ResourceUploadIdentity, RestoreArchive};

use super::WireValues;
use crate::{
    tests::fixtures::{checked, settings, size},
    *,
};

fn check(bytes: &[u8]) {
    let limits = SessionLimits::DEFAULT;
    let mut values = WireValues::new(bytes);
    let identity = ResourceUploadIdentity::parse(values.arbitrary.name_text())
        .assured("a generated name is an upload identity");
    let original = UploadStart {
        request_id: values.request_id(),
        domain: values.arbitrary.name(),
        resource: values.arbitrary.name(),
        upload_identity: identity.clone(),
        total_bytes: values.arbitrary.positive_u64(),
    };
    let frame = original
        .encode(&limits)
        .assured("bounded upload fits")
        .verify(&limits)
        .assured("upload verifies");
    let UploadMessage::Start(decoded) =
        UploadMessage::decode(&frame).assured("valid upload decodes")
    else {
        panic!("upload start keeps its variant");
    };
    drop(frame);
    assert_eq!(decoded, original);
    let original = RestoreStart {
        request_id: values.request_id(),
        execution_reference: values.reference(),
        statement: values.arbitrary.string(),
        archive: RestoreArchive {
            total_bytes: values.arbitrary.positive_u64(),
            digest: ArchiveDigest::from_bytes(values.digest()),
        },
    };
    let frame = original
        .encode(&limits)
        .assured("bounded restore fits")
        .verify(&limits)
        .assured("restore verifies");
    let RestoreMessage::Start(decoded) =
        RestoreMessage::decode(&frame).assured("valid restore decodes")
    else {
        panic!("restore start keeps its variant");
    };
    drop(frame);
    assert_eq!(decoded, original);
    let original = BackupDownloadRequest {
        execution_reference: values.reference(),
    };
    let frame = original
        .encode(&limits)
        .assured("bounded download request fits")
        .verify(&limits)
        .assured("request verifies");
    assert_eq!(
        BackupDownloadRequest::decode(&frame).assured("valid request decodes"),
        original
    );
    let original = BackupArchiveStart {
        total_bytes: values.arbitrary.positive_u64(),
        digest: ArchiveDigest::from_bytes(values.digest()),
    };
    let frame = BackupDownloadMessage::encode_start(&original, &limits)
        .assured("bounded start fits")
        .verify(&limits)
        .assured("start verifies");
    let BackupDownloadMessage::Start(decoded) =
        BackupDownloadMessage::decode(&frame).assured("valid start decodes")
    else {
        panic!("download start keeps its variant");
    };
    assert_eq!(decoded, original);

    // Shared chunks must survive both the transport frame and their decoded owner's drop.
    let chunk = values.bytes(1);
    let upload = UploadChunk::encode(&chunk, &limits)
        .assured("bounded chunk fits")
        .verify(&limits)
        .assured("chunk verifies");
    let UploadMessage::Chunk(decoded) =
        UploadMessage::decode(&upload).assured("valid chunk decodes")
    else {
        panic!("upload chunk keeps its variant");
    };
    let owned = decoded.shared_bytes();
    assert_eq!(decoded.bytes(), chunk);
    assert_eq!(owned.as_ptr(), decoded.bytes().as_ptr());
    drop(upload);
    drop(decoded);
    assert_eq!(owned, chunk);
    let restore = RestoreChunk::encode(&chunk, &limits)
        .assured("bounded chunk fits")
        .verify(&limits)
        .assured("chunk verifies");
    let RestoreMessage::Chunk(decoded) =
        RestoreMessage::decode(&restore).assured("valid chunk decodes")
    else {
        panic!("restore chunk keeps its variant");
    };
    let owned = decoded.shared_bytes();
    assert_eq!(decoded.bytes(), chunk);
    assert_eq!(owned.as_ptr(), decoded.bytes().as_ptr());
    drop(restore);
    drop(decoded);
    assert_eq!(owned, chunk);
    let backup = BackupDownloadMessage::encode_chunk(&chunk, &limits)
        .assured("bounded chunk fits")
        .verify(&limits)
        .assured("chunk verifies");
    let BackupDownloadMessage::Chunk(decoded) =
        BackupDownloadMessage::decode(&backup).assured("valid chunk decodes")
    else {
        panic!("download chunk keeps its variant");
    };
    let owned = decoded.shared_bytes();
    assert_eq!(decoded.bytes(), chunk);
    assert_eq!(owned.as_ptr(), decoded.bytes().as_ptr());
    drop(backup);
    drop(decoded);
    assert_eq!(owned, chunk);

    let frame = BackupDownloadMessage::encode_complete(&limits)
        .assured("complete fits")
        .verify(&limits)
        .assured("complete verifies");
    assert!(matches!(
        BackupDownloadMessage::decode(&frame).assured("complete decodes"),
        BackupDownloadMessage::Complete
    ));
    for &failure in crate::backup::ALL_BACKUP_DOWNLOAD_FAILURES {
        let original = BackupDownloadFailed {
            failure,
            message: values.arbitrary.string(),
        };
        let frame = BackupDownloadMessage::encode_failed(&original, &limits)
            .assured("failure fits")
            .verify(&limits)
            .assured("failure verifies");
        let BackupDownloadMessage::Failed(decoded) =
            BackupDownloadMessage::decode(&frame).assured("failure decodes")
        else {
            panic!("download failure keeps its variant");
        };
        assert_eq!(decoded, original);
    }
    let redirect = values.redirect();
    let frame = BackupDownloadMessage::encode_redirect(&redirect, &limits)
        .assured("redirect fits")
        .verify(&limits)
        .assured("redirect verifies");
    let BackupDownloadMessage::NotLeader(decoded) =
        BackupDownloadMessage::decode(&frame).assured("redirect decodes")
    else {
        panic!("redirect keeps its variant");
    };
    assert_eq!(decoded, redirect);
    let mut dispositions = vec![
        UploadDisposition::NotLeader(redirect),
        UploadDisposition::Installed {
            upload_identity: identity.clone(),
            version: values.arbitrary.positive_u64(),
            origin: values
                .arbitrary
                .entropy()
                .pick([OutcomeOrigin::Executed, OutcomeOrigin::Recovered]),
        },
    ];
    for &failure in crate::upload::ALL_UPLOAD_FAILURES {
        dispositions.push(UploadDisposition::Failed {
            failure,
            upload_identity: if values.arbitrary.entropy().flag() {
                Some(identity.clone())
            } else {
                None
            },
            assigned_version: if values.arbitrary.entropy().flag() {
                Some(values.arbitrary.positive_u64())
            } else {
                None
            },
        });
    }
    for disposition in dispositions {
        let original = UploadReply {
            request_id: if values.arbitrary.entropy().flag() {
                Some(values.request_id())
            } else {
                None
            },
            disposition,
            message: values.arbitrary.string(),
            diagnostics: values.diagnostics(),
        };
        let frame = original
            .encode(&limits)
            .assured("reply fits")
            .verify(&limits)
            .assured("reply verifies");
        assert_eq!(
            UploadReply::decode(&frame).assured("valid reply decodes"),
            original
        );
    }
    let mut command = crate::tests::samples::command_outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    command.execution_reference = values.reference();
    values.command_metadata(&mut command);
    let mut dispositions = vec![RestoreDisposition::Outcome(Box::new(command))];
    for &failure in crate::restore::ALL_RESTORE_UPLOAD_FAILURES {
        dispositions.push(RestoreDisposition::UploadFailed {
            failure,
            message: values.arbitrary.string(),
        });
    }
    for disposition in dispositions {
        let original = RestoreReply {
            request_id: if values.arbitrary.entropy().flag() {
                Some(values.request_id())
            } else {
                None
            },
            disposition,
        };
        let frame = original
            .encode(&limits)
            .assured("reply fits")
            .verify(&limits)
            .assured("reply verifies");
        assert_eq!(
            RestoreReply::decode(&frame).assured("valid reply decodes"),
            original
        );
    }
}

#[test]
fn bolero_stream_frames_keep_values_and_chunk_ownership() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            check(bytes);
        });
}

fn transfer(bytes: &[u8]) {
    let mut values = WireValues::new(bytes);
    let mut settings = settings();
    settings.frame_bytes = size(1024);
    settings.transfer_bytes = size(32 * 1024);
    settings.string_bytes = size(16 * 1024);
    let limits = checked(settings);
    let message = values.arbitrary.string().repeat(80);
    let original = Reply {
        request_id: values.request_id(),
        body: ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::ServerBusy,
            field: Some(values.arbitrary.string()),
            message,
        }),
    };
    let expected = original
        .encode(&SessionLimits::DEFAULT)
        .assured("bounded complete reply fits");
    let ReplyDelivery::Frame(expected) = expected else {
        panic!("a complete bounded reply fits default limits");
    };
    let mut assembly = TransferAssembly::new(original.request_id, &limits);
    let mut joined = Vec::new();
    match original
        .encode(&limits)
        .assured("the reply fits the transfer budget")
    {
        ReplyDelivery::Frame(frame) => {
            let frame = frame.verify(&limits).assured("small reply verifies");
            let ServerMessage::Reply(decoded) =
                ServerMessage::decode(&frame).assured("small reply decodes")
            else {
                panic!("a reply keeps its variant");
            };
            assert_eq!(decoded, original);
        }
        ReplyDelivery::Transfer(parts) => {
            let total = parts.total_bytes();
            for part in parts {
                let frame = part
                    .verify(&limits)
                    .assured("every generated part verifies");
                let ServerMessage::TransferPart(part) =
                    ServerMessage::decode(&frame).assured("a generated part decodes")
                else {
                    panic!("a part keeps its variant");
                };
                drop(frame);
                assert_eq!(part.request_id(), original.request_id);
                assert_eq!(part.offset(), joined.len());
                assert_eq!(part.total_bytes(), total);
                let before = assembly.received_bytes();
                let mut wrong = TransferAssembly::new(
                    crate::tests::fixtures::request(
                        if original.request_id == crate::tests::fixtures::request(1) {
                            2
                        } else {
                            1
                        },
                    ),
                    &limits,
                );
                assert!(wrong.append(&part).is_err());
                assert_eq!(wrong.received_bytes(), 0);
                assembly.append(&part).assured("ordered parts append");
                assert!(assembly.received_bytes() > before);
                joined.extend_from_slice(part.chunk());
                assert!(assembly.append(&part).is_err());
                assert_eq!(assembly.received_bytes(), joined.len());
            }
            assert_eq!(Bytes::from(joined), *expected.bytes());
            assert!(assembly.is_complete());
            assert_eq!(
                assembly.finish().assured("complete transfer decodes"),
                original
            );
        }
    }
}

#[test]
fn bolero_reply_transfers_keep_exact_bytes_and_reject_without_effects() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(256)
        .for_each(|bytes: &[u8]| {
            transfer(bytes);
        });
}

#[test]
fn stream_boundaries_are_replayable() {
    for seed in [0, 1, 2, 3, 4, 5, 0x80, 0xff] {
        check(&[seed; 2048]);
    }
    transfer(&[1; 256]);
    transfer(&[0; 256]);
}
