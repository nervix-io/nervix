//! The conformance corpus: frames this crate writes, checked in under `conformance/` for
//! independent implementations to decode, and the report every implementation prints for them.
//!
//! Each frame is written from the representative samples of its message family. The checked-in
//! bytes must be exactly what the encoder writes, and decoding them must print exactly the
//! checked-in `corpus.report`. The Go and TypeScript conformance probes read the same files with
//! code generated from the schema and print the same report, which is how an independent reader
//! is held to the frames Rust writes. Setting `NERVIX_UPDATE_CLIENT_WIRE_CORPUS=1` rewrites the
//! corpus instead of checking it; `just update-client-wire-corpus` does that.

use std::{
    fmt::Write as _,
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, ArchiveDigest, BackupArchiveSummary, BackupDomainSummary, BackupResources,
    ClientBatchDefect, ClientConsumerLimits, ClientOutcomeUncertainty, ClientProcessingFailure,
    ClientProducerAdmission, ClientProducerDescription, ClientProducerEndReason,
    ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal,
    CommandExecutionReference, DomainClockObservation, DomainClockObservedState,
    DomainClockTickObservation, ModelKind, ModelName, NodeRef, ParseAsType, PlacementPolicy,
    RequestedResourceVersion, RestoreArchive, RestoreMode, RestoreReport, RestoreStep,
    RestoreStepOutcome, RestoreStepReport, RestoredDomain, RestoredUsers, SchemaField, Timestamp,
};

use super::{
    fixtures::{limits, name, non_zero, request},
    samples::{
        client_messages, command_outcome, domain_clock_observations, leader, producer,
        producer_description, producer_fields, row_schema, rows_frame, subscription,
    },
};
use crate::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame,
    BackupDownloadMessage, BackupDownloadRequest, BackupDownloadRequestFrame, CellView, CellsView,
    Choice, ChoiceOutcome, ChoicePresentation, ChoiceStatus, ChoiceValue, ClientFrame,
    ClientMessage, ClientRequest, CloseEmitterOutcome, CloseIngestorDisposition,
    CloseIngestorOutcome, CommandDisposition, CommandOutcome, DomainClockAttachDisposition,
    DomainClockAttachOutcome, DomainClockAttachmentEndReason, DomainClockAttachmentEnded,
    DomainClockDetachDisposition, DomainClockDetachOutcome, DomainClockObserved, DomainClockTicked,
    DomainPaceChoice, EmitterBatchReceived, EmitterCloseDisposition, EmitterOpened,
    EmitterSettlement, LeaderRedirect, OpenEmitterDisposition, OpenEmitterOutcome,
    OpenIngestorDisposition, OpenIngestorOutcome, ProducerAdmissionChanged, ProducerEnded,
    ProducerOpened, ReadEmitterBatchOutcome, ReadEmitterDisposition, Reply, ReplyBody,
    ReplyDelivery, RequestRejected, RequestRejection, RestoreDisposition, RestoreFrame,
    RestoreMessage, RestoreReply, RestoreReplyFrame, RestoreStart, RestoreUploadFailure,
    ServerEvent, ServerFrame, ServerMessage, SettleEmitterBatchOutcome, SubmissionOutcome,
    SubscribeDisposition, SubscribeOutcome, SubscriptionEndReason, SubscriptionEnded,
    SubscriptionOpened, SubscriptionType, SuggestOutcome, Suggestion, SuggestionKind,
    SuggestionStatus, TextEdit, UnknownOutcomeCause, VerifiedFrame, producer::wire_refusal,
    restore::RestoreChunk, wire,
};

const UPDATE_ENV: &str = "NERVIX_UPDATE_CLIENT_WIRE_CORPUS";
const REPORT: &str = "corpus.report";

fn corpus_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conformance")
}

/// Every frame of the corpus, by file name, as the encoder writes it now. File names sort in the
/// order the report lists them.
fn corpus_frames() -> Vec<(&'static str, Bytes)> {
    let reply = |id: u64, body: ReplyBody| {
        let delivery = Reply {
            request_id: request(id),
            body,
        }
        .encode(&limits())
        .assured("every corpus reply fits the default limits");
        let ReplyDelivery::Frame(frame) = delivery else {
            panic!("a corpus reply fits one frame");
        };
        frame.into_bytes()
    };
    let command = |id: u64, disposition: CommandDisposition| {
        reply(
            id,
            ReplyBody::Command(Box::new(command_outcome(disposition))),
        )
    };
    let client = |index: usize| {
        client_messages()[index]
            .encode(&limits())
            .assured("every corpus request fits the default limits")
            .into_bytes()
    };
    let opened = SubscriptionOpened {
        subscription: subscription(),
        domain: name("tenant"),
        relay: name("orders"),
        subscription_type: SubscriptionType::Row,
        schema: row_schema(),
    };
    let ended = SubscriptionEnded {
        subscription: subscription(),
        reason: SubscriptionEndReason::RelayChanged,
        message: "relay 'orders' was redefined".to_string(),
    }
    .encode(&limits())
    .assured("the corpus event fits the default limits");
    let paced_clock = domain_clock_observations()
        .into_iter()
        .find(|clock| clock.generation == u64::MAX)
        .assured("the samples hold a paced clock at the last generation");
    let clock_attach = |id: u64, disposition: DomainClockAttachDisposition, message: &str| {
        reply(
            id,
            ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                disposition,
                message: message.to_string(),
            }),
        )
    };
    let clock_detach = |id: u64, disposition: DomainClockDetachDisposition, message: &str| {
        reply(
            id,
            ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
                disposition,
                message: message.to_string(),
            }),
        )
    };
    let clock_observed = |generation: u64, state: DomainClockObservedState| {
        DomainClockObserved {
            domain: name("tenant"),
            clock: DomainClockObservation { generation, state },
        }
        .encode(&limits())
        .assured("a corpus clock frame fits the default limits")
        .into_bytes()
    };
    let clock_ended = DomainClockAttachmentEnded {
        domain: name("tenant"),
        reason: DomainClockAttachmentEndReason::DomainRemoved,
    }
    .encode(&limits())
    .assured("a corpus clock frame fits the default limits");
    let submission = |id: u64, outcome: ClientSubmissionOutcome, message: &str| {
        reply(
            id,
            ReplyBody::Submission(SubmissionOutcome {
                outcome,
                message: message.to_string(),
            }),
        )
    };
    let producer_opened = ProducerOpened {
        domain: name("tenant"),
        ingestor: name("orders_in"),
        description: producer_description(AckWindow::Parallel { max: non_zero(8) }),
    };
    let producer_admission = ProducerAdmissionChanged {
        producer: producer(),
        admission: ClientProducerAdmission::Suspended,
    }
    .encode(&limits())
    .assured("a corpus producer frame fits the default limits");
    let producer_ended = ProducerEnded {
        producer: producer(),
        reason: ClientProducerEndReason::Relocated,
        message: "ingestor 'orders_in' moved to node-2".to_string(),
    }
    .encode(&limits())
    .assured("a corpus producer frame fits the default limits");
    let download = |frame: crate::EncodedFrame<BackupDownloadFrame>| frame.into_bytes();
    let backup = CommandOutcome {
        backup: Some(Box::new(backup_archive())),
        ..command_outcome(CommandDisposition::Completed {
            already_existed: false,
        })
    };
    let restore = CommandOutcome {
        restore: Some(Box::new(restore_report())),
        ..command_outcome(CommandDisposition::Failed)
    };
    let clock_ticked = DomainClockTicked {
        domain: name("tenant"),
        tick: DomainClockTickObservation {
            generation: 7,
            tick_id: 42,
            logical_boundary: Timestamp::from_unix_nanos(1_000),
            authority_utc: Timestamp::from_unix_nanos(2_000),
            serving_logical: Timestamp::from_unix_nanos(3_000),
        },
    }
    .encode(&limits())
    .assured("a corpus tick frame fits the default limits");
    vec![
        (
            "backup_download_chunk.nxbd",
            download(
                BackupDownloadMessage::encode_chunk(&[0x00, 0x7f, 0x80, 0xff], &limits())
                    .assured("a corpus chunk fits the default limits"),
            ),
        ),
        (
            "backup_download_complete.nxbd",
            download(
                BackupDownloadMessage::encode_complete(&limits())
                    .assured("a corpus completion fits the default limits"),
            ),
        ),
        (
            "backup_download_failed.nxbd",
            download(
                BackupDownloadMessage::encode_failed(
                    &BackupDownloadFailed {
                        failure: BackupDownloadFailure::NotRetained,
                        message: "no archive is retained under this reference".to_string(),
                    },
                    &limits(),
                )
                .assured("a corpus refusal fits the default limits"),
            ),
        ),
        (
            "backup_download_redirect.nxbd",
            download(
                BackupDownloadMessage::encode_redirect(
                    &LeaderRedirect {
                        leader: Some(leader()),
                    },
                    &limits(),
                )
                .assured("a corpus redirect fits the default limits"),
            ),
        ),
        (
            "backup_download_request.nxbq",
            BackupDownloadRequest {
                execution_reference: CommandExecutionReference::parse(
                    "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44",
                )
                .assured("the corpus reference is a UUID"),
            }
            .encode(&limits())
            .assured("a corpus download request fits the default limits")
            .into_bytes(),
        ),
        (
            "backup_download_start.nxbd",
            download(
                BackupDownloadMessage::encode_start(
                    &BackupArchiveStart {
                        total_bytes: NonZeroU64::MAX,
                        digest: ArchiveDigest::from_bytes([0xa5; 32]),
                    },
                    &limits(),
                )
                .assured("a corpus start fits the default limits"),
            ),
        ),
        ("client_attach_domain_clock.nxcm", client(15)),
        ("client_cancel.nxcm", client(13)),
        ("client_choice.nxcm", client(14)),
        ("client_choice_relay_field.nxcm", client(17)),
        ("client_close_emitter.nxcm", client(24)),
        ("client_close_ingestor.nxcm", client(20)),
        ("client_command.nxcm", client(0)),
        ("client_command_bare.nxcm", client(2)),
        ("client_commit.nxcm", client(1)),
        ("client_detach_domain_clock.nxcm", client(16)),
        ("client_open_emitter.nxcm", client(21)),
        ("client_open_ingestor.nxcm", client(18)),
        ("client_read_emitter_batch.nxcm", client(22)),
        ("client_settle_emitter_batch.nxcm", client(23)),
        ("client_submit_batch.nxcm", client(19)),
        ("client_subscribe.nxcm", client(11)),
        ("client_suggest.nxcm", client(3)),
        (
            "restore_chunk.nxrm",
            RestoreChunk::encode(&[0x00, 0x7f, 0x80, 0xff], &limits())
                .assured("a corpus restore chunk fits the default limits")
                .into_bytes(),
        ),
        (
            "restore_failed.nxrr",
            RestoreReply {
                request_id: None,
                disposition: RestoreDisposition::UploadFailed {
                    failure: RestoreUploadFailure::DigestMismatch,
                    message: "the archive does not have its declared digest".to_string(),
                },
            }
            .encode(&limits())
            .assured("a corpus restore refusal fits the default limits")
            .into_bytes(),
        ),
        (
            "restore_outcome.nxrr",
            RestoreReply {
                request_id: Some(request(1)),
                disposition: RestoreDisposition::Outcome(Box::new(restore.clone())),
            }
            .encode(&limits())
            .assured("a corpus restore outcome fits the default limits")
            .into_bytes(),
        ),
        (
            "restore_start.nxrm",
            RestoreStart {
                request_id: request(1),
                execution_reference: CommandExecutionReference::parse(
                    "0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44",
                )
                .assured("the corpus reference is a UUID"),
                statement: "RESTORE CLUSTER FROM 'cluster.nvxb' ON EXISTING USER SKIP;".to_string(),
                archive: RestoreArchive {
                    total_bytes: NonZeroU64::MAX,
                    digest: ArchiveDigest::from_bytes([0x3c; 32]),
                },
            }
            .encode(&limits())
            .assured("a corpus restore start fits the default limits")
            .into_bytes(),
        ),
        (
            "server_choice.nxsm",
            reply(
                9,
                ReplyBody::Choice(ChoiceOutcome {
                    status: ChoiceStatus::Ready,
                    choices: vec![
                        Choice {
                            value: ChoiceValue::DomainPace(DomainPaceChoice::Paced),
                            presentation: ChoicePresentation {
                                label: "PACED".to_string(),
                                detail: Some("Wall clock".to_string()),
                                group: Some("Domain clock".to_string()),
                            },
                        },
                        Choice {
                            value: ChoiceValue::PlacementPolicy(PlacementPolicy::PreferColocation),
                            presentation: ChoicePresentation {
                                label: "PREFER COLOCATION".to_string(),
                                detail: None,
                                group: Some("Placement".to_string()),
                            },
                        },
                        Choice {
                            value: ChoiceValue::Domain(name("tenant")),
                            presentation: ChoicePresentation {
                                label: "tenant".to_string(),
                                detail: None,
                                group: None,
                            },
                        },
                        Choice {
                            value: ChoiceValue::Resource(name("bundle")),
                            presentation: ChoicePresentation {
                                label: "bundle".to_string(),
                                detail: None,
                                group: None,
                            },
                        },
                        Choice {
                            value: ChoiceValue::ResourceVersion(RequestedResourceVersion::Latest),
                            presentation: ChoicePresentation {
                                label: "LATEST".to_string(),
                                detail: Some("Highest completed version".to_string()),
                                group: Some("Resource version".to_string()),
                            },
                        },
                        Choice {
                            value: ChoiceValue::ResourceVersion(RequestedResourceVersion::Number(
                                3,
                            )),
                            presentation: ChoicePresentation {
                                label: "3".to_string(),
                                detail: Some("Completed version".to_string()),
                                group: Some("Resource version".to_string()),
                            },
                        },
                        Choice {
                            value: ChoiceValue::Model(NodeRef::new(
                                ModelKind::Relay,
                                name::<ModelName>("orders"),
                            )),
                            presentation: ChoicePresentation {
                                label: "orders".to_string(),
                                detail: Some("RELAY".to_string()),
                                group: Some("Models".to_string()),
                            },
                        },
                        Choice {
                            value: ChoiceValue::Field(name("amount")),
                            presentation: ChoicePresentation {
                                label: "amount".to_string(),
                                detail: Some("I64 OPTIONAL".to_string()),
                                group: Some("Relay field".to_string()),
                            },
                        },
                    ],
                    page_cursor: Some("choice-page-two".to_string()),
                }),
            ),
        ),
        (
            "server_command_backup.nxsm",
            reply(6, ReplyBody::Command(Box::new(backup))),
        ),
        (
            "server_command_failed.nxsm",
            command(1, CommandDisposition::Failed),
        ),
        (
            "server_command_leader_unknown.nxsm",
            command(
                3,
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
            ),
        ),
        (
            "server_command_redirect.nxsm",
            command(
                2,
                CommandDisposition::NotLeader(LeaderRedirect {
                    leader: Some(leader()),
                }),
            ),
        ),
        (
            "server_command_restore.nxsm",
            reply(19, ReplyBody::Command(Box::new(restore))),
        ),
        (
            "server_command_unknown.nxsm",
            command(
                4,
                CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::StillApplying),
            ),
        ),
        (
            "server_domain_clock_already_attached.nxsm",
            clock_attach(
                11,
                DomainClockAttachDisposition::AlreadyAttached(name("tenant")),
                "this session already follows the clock of domain 'tenant'",
            ),
        ),
        (
            "server_domain_clock_attach_failed.nxsm",
            clock_attach(
                12,
                DomainClockAttachDisposition::Failed,
                "session-scoped and client-local statements cannot be queued in a transaction",
            ),
        ),
        (
            "server_domain_clock_attached.nxsm",
            clock_attach(
                10,
                DomainClockAttachDisposition::Attached {
                    domain: name("tenant"),
                    clock: paced_clock,
                },
                "attached to the clock of domain 'tenant'",
            ),
        ),
        (
            "server_domain_clock_detached.nxsm",
            clock_detach(
                14,
                DomainClockDetachDisposition::Detached(name("tenant")),
                "detached from the clock of domain 'tenant'",
            ),
        ),
        (
            "server_domain_clock_domain_not_found.nxsm",
            clock_attach(
                13,
                DomainClockAttachDisposition::DomainNotFound(name("missing")),
                "domain 'missing' does not exist",
            ),
        ),
        ("server_domain_clock_ended.nxsm", clock_ended.into_bytes()),
        (
            "server_domain_clock_not_attached.nxsm",
            clock_detach(
                15,
                DomainClockDetachDisposition::NotAttached(name("tenant")),
                "this session does not follow the clock of domain 'tenant'",
            ),
        ),
        (
            "server_domain_clock_stopped.nxsm",
            clock_observed(0, DomainClockObservedState::Stopped),
        ),
        ("server_domain_clock_ticked.nxsm", clock_ticked.into_bytes()),
        (
            "server_domain_clock_uninstalled.nxsm",
            clock_observed(7, DomainClockObservedState::Uninstalled),
        ),
        (
            "server_domain_clock_unpaced.nxsm",
            clock_observed(1, DomainClockObservedState::Unpaced),
        ),
        (
            "server_emitter_batch.nxsm",
            reply(
                23,
                ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
                    disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
                        identity: uuid::Uuid::from_bytes([0xA3; 16]),
                        reference: uuid::Uuid::from_bytes([0xB4; 16]),
                        source_relay: name("orders"),
                        branch_fingerprint: Some([0xC5; 32]),
                        batch: Bytes::from_static(b"ARROW-IPC-OUTPUT"),
                        members: 2,
                        execution_now: Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
                    }),
                    message: String::new(),
                }),
            ),
        ),
        (
            "server_emitter_closed.nxsm",
            reply(
                25,
                ReplyBody::CloseEmitter(CloseEmitterOutcome {
                    disposition: EmitterCloseDisposition::Closed,
                    message: String::new(),
                }),
            ),
        ),
        (
            "server_emitter_opened.nxsm",
            reply(
                22,
                ReplyBody::OpenEmitter(OpenEmitterOutcome {
                    disposition: OpenEmitterDisposition::Opened(Box::new(EmitterOpened {
                        domain: name("tenant"),
                        emitter: name("app_output"),
                        fields: producer_fields(),
                        window: AckWindow::Parallel { max: non_zero(8) },
                        ack_timeout: Duration::from_secs(30),
                        retry_backoff: Duration::from_millis(100),
                        retry_max_backoff: Duration::from_secs(1),
                        granted: ClientConsumerLimits {
                            batches: NonZeroU32::new(8).assured("literal nonzero"),
                            bytes: non_zero(4 * 1024 * 1024),
                        },
                        max_batch_bytes: 1024 * 1024,
                        max_batch_rows: 16,
                    })),
                    message: "consumer attached".to_string(),
                }),
            ),
        ),
        (
            "server_emitter_settled.nxsm",
            reply(
                24,
                ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
                    disposition: EmitterSettlement::Confirmed,
                    message: String::new(),
                }),
            ),
        ),
        (
            "server_ingestor_closed.nxsm",
            reply(
                21,
                ReplyBody::CloseIngestor(CloseIngestorOutcome {
                    disposition: CloseIngestorDisposition::Closed,
                    message: "producer 19 closed".to_string(),
                }),
            ),
        ),
        (
            "server_ingestor_opened.nxsm",
            reply(
                19,
                ReplyBody::OpenIngestor(OpenIngestorOutcome {
                    disposition: OpenIngestorDisposition::Opened(Box::new(producer_opened)),
                    message: "producer attached to ingestor 'orders_in'".to_string(),
                }),
            ),
        ),
        (
            "server_ingestor_refused.nxsm",
            reply(
                19,
                ReplyBody::OpenIngestor(OpenIngestorOutcome {
                    disposition: OpenIngestorDisposition::Refused(
                        ClientProducerRefusal::SchemaMismatch,
                    ),
                    message: "field 'card' differs in sensitivity".to_string(),
                }),
            ),
        ),
        (
            "server_producer_admission.nxsm",
            producer_admission.into_bytes(),
        ),
        ("server_producer_ended.nxsm", producer_ended.into_bytes()),
        (
            "server_rejected.nxsm",
            reply(
                5,
                ReplyBody::Rejected(RequestRejected {
                    rejection: RequestRejection::InvalidRequest,
                    field: Some("SubscribeRequest.subscription_type".to_string()),
                    message: "`SubscribeRequest.subscription_type` is required".to_string(),
                }),
            ),
        ),
        ("server_rows.nxsm", rows_frame(&limits()).into_bytes()),
        (
            "server_submission_completed.nxsm",
            submission(20, ClientSubmissionOutcome::Completed, ""),
        ),
        (
            "server_submission_failed.nxsm",
            submission(
                20,
                ClientSubmissionOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut),
                "no acknowledgement progress within 30s",
            ),
        ),
        (
            "server_submission_not_admitted.nxsm",
            submission(
                20,
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::InvalidBatch(
                    ClientBatchDefect::TooManyRows,
                )),
                "the batch has 70000 rows, more than the 65536 one batch may carry",
            ),
        ),
        (
            "server_submission_unknown.nxsm",
            submission(
                20,
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::OwnerLost),
                "the connection to node-2 was lost",
            ),
        ),
        ("server_subscription_ended.nxsm", ended.into_bytes()),
        (
            "server_subscription_opened.nxsm",
            reply(
                7,
                ReplyBody::Subscribe(SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(opened)),
                    message: "created subscription 'live'".to_string(),
                    diagnostics: Vec::new(),
                }),
            ),
        ),
        (
            "server_suggest.nxsm",
            reply(
                8,
                ReplyBody::Suggest(SuggestOutcome {
                    status: SuggestionStatus::Ready,
                    suggestions: vec![Suggestion {
                        value: "CLUSTER".to_string(),
                        kind: SuggestionKind::Text,
                        edit: TextEdit {
                            start: 5,
                            end: 10,
                            replacement: "CLUSTER".to_string(),
                        },
                    }],
                    continuation: Some("next-page".to_string()),
                }),
            ),
        ),
    ]
}

/// A backup summary at the edges of every field's range: the largest archive, the earliest and
/// latest instants, a present zero user count, and domains in ascending order.
fn backup_archive() -> BackupArchiveSummary {
    BackupArchiveSummary {
        total_bytes: NonZeroU64::MAX,
        digest: ArchiveDigest::from_bytes([0x5a; 32]),
        captured_at: Timestamp::from_unix_nanos(i64::MIN),
        retained_until: Timestamp::from_unix_nanos(i64::MAX),
        resources: BackupResources::Omitted,
        users: Some(0),
        domains: vec![
            BackupDomainSummary {
                domain: name("analytics"),
                revision: u64::MAX,
                sections: 0,
                section_bytes: u64::MAX,
            },
            BackupDomainSummary {
                domain: name("tenant"),
                revision: 1,
                sections: 7,
                section_bytes: 4096,
            },
        ],
    }
}

/// A restore report of every step kind and outcome, user counts at the edges of their range, and
/// a domain restored under another name.
fn restore_report() -> RestoreReport {
    RestoreReport {
        mode: RestoreMode::Apply,
        archive: RestoreArchive {
            total_bytes: NonZeroU64::MAX,
            digest: ArchiveDigest::from_bytes([0x3c; 32]),
        },
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
                models: u64::MAX,
                planned_models: None,
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

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").assured("writing to a String cannot fail");
    }
    text
}

fn text(value: &str) -> String {
    format!("str:{}", hex(value.as_bytes()))
}

fn type_name(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Bytes => "BYTES".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::Array { element, len } => format!("FIXED_LIST<{},{len}>", type_name(element)),
        ParseAsType::Vec { element } => format!("LIST<{}>", type_name(element)),
    }
}

fn field_line(prefix: &str, field: &SchemaField) -> String {
    let nullable = if field.optional {
        "nullable"
    } else {
        "required"
    };
    let sensitive = if field.sensitive {
        "sensitive"
    } else {
        "public"
    };
    format!(
        "{prefix} {} {} {nullable} {sensitive}",
        field.name.as_str(),
        type_name(&field.ty)
    )
}

fn render_cell(cell: CellView<'_>) -> String {
    match cell {
        CellView::Null => "null".to_string(),
        CellView::Redacted => "redacted".to_string(),
        CellView::U8(value) => format!("u8:{value}"),
        CellView::I8(value) => format!("i8:{value}"),
        CellView::U16(value) => format!("u16:{value}"),
        CellView::I16(value) => format!("i16:{value}"),
        CellView::U32(value) => format!("u32:{value}"),
        CellView::I32(value) => format!("i32:{value}"),
        CellView::U64(value) => format!("u64:{value}"),
        CellView::I64(value) => format!("i64:{value}"),
        CellView::F32(value) => format!("f32:{:08x}", value.to_bits()),
        CellView::F64(value) => format!("f64:{:016x}", value.to_bits()),
        CellView::Bool(value) => format!("bool:{value}"),
        CellView::String(value) => text(value),
        CellView::Bytes(value) => format!("bytes:{}", hex(value)),
        CellView::Datetime(value) => format!("datetime:{}", value.unix_nanos()),
        CellView::List(elements) => {
            let elements: Vec<String> = elements.iter().map(render_cell).collect();
            format!("list[{}]", elements.join(","))
        }
    }
}

fn cells(values: CellsView<'_>, fields: &[SchemaField]) -> String {
    let rendered: Vec<String> = values
        .iter()
        .zip(fields)
        .map(|(value, field)| format!("{}={}", field.name.as_str(), render_cell(value)))
        .collect();
    rendered.join(" ")
}

fn disposition(disposition: &CommandDisposition) -> &'static str {
    match disposition {
        CommandDisposition::Completed { .. } => "completed",
        CommandDisposition::Failed => "failed",
        CommandDisposition::NotLeader(_) => "not_leader",
        CommandDisposition::TransactionDetached { .. } => "transaction_detached",
        CommandDisposition::TransactionTakenOver { .. } => "transaction_taken_over",
        CommandDisposition::OutcomeUnknown(_) => "outcome_unknown",
        CommandDisposition::ExecutionReferenceConflict(_) => "execution_reference_conflict",
        CommandDisposition::ExecutionReferenceExpired => "execution_reference_expired",
        CommandDisposition::PreviewStale { .. } => "preview_stale",
    }
}

fn choice_value(value: &ChoiceValue) -> String {
    match value {
        ChoiceValue::DomainPace(value) => format!("pace:{value:?}"),
        ChoiceValue::PlacementPolicy(value) => format!("placement:{value:?}"),
        ChoiceValue::Domain(domain) => format!("domain:{}", domain.as_str()),
        ChoiceValue::Resource(resource) => format!("resource:{}", resource.as_str()),
        ChoiceValue::ResourceVersion(version) => format!("resource-version:{version}"),
        ChoiceValue::Model(node) => {
            format!("model:{}/{}", node.kind.as_str(), node.identifier.as_str())
        }
        ChoiceValue::Field(field) => format!("field:{}", field.as_str()),
    }
}

fn clock_line(clock: &DomainClockObservation) -> String {
    let generation = clock.generation;
    match &clock.state {
        DomainClockObservedState::Stopped => format!("CLOCK generation={generation} state=stopped"),
        DomainClockObservedState::Uninstalled => {
            format!("CLOCK generation={generation} state=uninstalled")
        }
        DomainClockObservedState::Unpaced => format!("CLOCK generation={generation} state=unpaced"),
        DomainClockObservedState::Paced(paced) => format!(
            "CLOCK generation={generation} state=paced period={} skew={} origin={} anchor={} \
             rate=f64:{:016x}",
            paced.period.as_nanos(),
            paced.skew.as_nanos(),
            paced.mapping.logical_start().unix_nanos(),
            paced.mapping.wall_started_at().unix_nanos(),
            paced.mapping.time_rate().get().to_bits()
        ),
    }
}

/// The schema's name for a schema enum value.
fn schema_name(name: Option<&'static str>) -> &'static str {
    name.assured("every value a corpus frame carries is declared by the schema")
}

fn producer_lines(id: u64, opened: &ProducerOpened, message: &str, lines: &mut Vec<String>) {
    let ClientProducerDescription {
        attachment,
        fields,
        generation,
        contract,
        policy,
        grant,
        admission,
    } = &opened.description;
    let window = match policy.window {
        AckWindow::Sequential => "sequential".to_string(),
        AckWindow::Parallel { max } => format!("parallel:{max}"),
    };
    lines.push(format!(
        "REPLY {id} INGESTOR_OPENED domain={} ingestor={} generation={generation} contract={} \
         attachment={} window={window} ack_timeout={} retry={}/{} granted={}/{} max_batch={}/{} \
         admission={} message={}",
        opened.domain.as_str(),
        opened.ingestor.as_str(),
        hex(contract.as_digest()),
        hex(&attachment.as_u128().to_be_bytes()),
        policy.ack_timeout.as_nanos(),
        policy.retry_backoff.as_nanos(),
        policy.retry_max_backoff.as_nanos(),
        grant.batches,
        grant.bytes,
        grant.max_batch_bytes,
        grant.max_batch_rows,
        schema_name(wire::ProducerAdmission::from(*admission).variant_name()),
        text(message)
    ));
    for field in fields {
        lines.push(field_line("FIELD", field));
    }
}

fn submission_line(id: u64, outcome: &SubmissionOutcome) -> String {
    let message = text(&outcome.message);
    match outcome.outcome {
        ClientSubmissionOutcome::Completed => {
            format!("REPLY {id} SUBMISSION completed message={message}")
        }
        ClientSubmissionOutcome::NotAdmitted(refusal) => format!(
            "REPLY {id} SUBMISSION not_admitted refusal={} message={message}",
            schema_name(wire_refusal(refusal).variant_name())
        ),
        ClientSubmissionOutcome::ProcessingFailed(failure) => format!(
            "REPLY {id} SUBMISSION failed failure={} message={message}",
            schema_name(wire::ProcessingFailure::from(failure).variant_name())
        ),
        ClientSubmissionOutcome::OutcomeUnknown(cause) => format!(
            "REPLY {id} SUBMISSION unknown cause={} message={message}",
            schema_name(wire::OutcomeUncertainty::from(cause).variant_name())
        ),
    }
}

fn render_server(message: &ServerMessage, lines: &mut Vec<String>) {
    match message {
        ServerMessage::Reply(reply) => {
            let id = reply.request_id.get();
            match &reply.body {
                ReplyBody::Command(outcome) => {
                    render_command(&format!("REPLY {id} COMMAND"), outcome, lines);
                }
                ReplyBody::Rejected(rejected) => {
                    let field = rejected.field.as_deref().unwrap_or("none");
                    lines.push(format!(
                        "REPLY {id} REJECTED {:?} field={field} message={}",
                        rejected.rejection,
                        text(&rejected.message)
                    ));
                }
                ReplyBody::Suggest(outcome) => {
                    lines.push(format!(
                        "REPLY {id} SUGGEST status={:?} continuation={}",
                        outcome.status,
                        outcome.continuation.as_deref().unwrap_or("none")
                    ));
                    for suggestion in &outcome.suggestions {
                        lines.push(format!(
                            "SUGGESTION kind={:?} value={} edit={}..{} replacement={}",
                            suggestion.kind,
                            text(&suggestion.value),
                            suggestion.edit.start,
                            suggestion.edit.end,
                            text(&suggestion.edit.replacement)
                        ));
                    }
                }
                ReplyBody::Choice(outcome) => {
                    lines.push(format!(
                        "REPLY {id} CHOICE status={:?} cursor={}",
                        outcome.status,
                        outcome.page_cursor.as_deref().unwrap_or("none")
                    ));
                    for choice in &outcome.choices {
                        lines.push(format!(
                            "CHOICE value={} label={} detail={} group={}",
                            choice_value(&choice.value),
                            text(&choice.presentation.label),
                            choice
                                .presentation
                                .detail
                                .as_deref()
                                .map(text)
                                .unwrap_or_else(|| "none".to_string()),
                            choice
                                .presentation
                                .group
                                .as_deref()
                                .map(text)
                                .unwrap_or_else(|| "none".to_string())
                        ));
                    }
                }
                ReplyBody::Subscribe(SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(opened),
                    ..
                }) => {
                    lines.push(format!(
                        "REPLY {id} SUBSCRIBED name={} generation={} domain={} relay={} type={:?}",
                        opened.subscription.name.as_str(),
                        opened.subscription.generation,
                        opened.domain.as_str(),
                        opened.relay.as_str(),
                        opened.subscription_type
                    ));
                    for field in &opened.schema.fields {
                        lines.push(field_line("FIELD", field));
                    }
                    if let Some(branch) = &opened.schema.branch {
                        lines.push(format!("BRANCH {}", branch.branch().as_str()));
                        for field in branch.fields() {
                            lines.push(field_line("KEY", field));
                        }
                    }
                }
                ReplyBody::DomainClockAttach(outcome) => {
                    let message = text(&outcome.message);
                    match &outcome.disposition {
                        DomainClockAttachDisposition::Attached { domain, clock } => {
                            lines.push(format!(
                                "REPLY {id} DOMAIN_CLOCK_ATTACH attached domain={} \
                                 message={message}",
                                domain.as_str()
                            ));
                            lines.push(clock_line(clock));
                        }
                        DomainClockAttachDisposition::AlreadyAttached(domain) => {
                            lines.push(format!(
                                "REPLY {id} DOMAIN_CLOCK_ATTACH already_attached domain={} \
                                 message={message}",
                                domain.as_str()
                            ));
                        }
                        DomainClockAttachDisposition::DomainNotFound(domain) => {
                            lines.push(format!(
                                "REPLY {id} DOMAIN_CLOCK_ATTACH domain_not_found domain={} \
                                 message={message}",
                                domain.as_str()
                            ));
                        }
                        DomainClockAttachDisposition::Failed => lines.push(format!(
                            "REPLY {id} DOMAIN_CLOCK_ATTACH failed message={message}"
                        )),
                    }
                }
                ReplyBody::DomainClockDetach(outcome) => {
                    let message = text(&outcome.message);
                    let (disposition, domain) = match &outcome.disposition {
                        DomainClockDetachDisposition::Detached(domain) => ("detached", domain),
                        DomainClockDetachDisposition::NotAttached(domain) => {
                            ("not_attached", domain)
                        }
                        DomainClockDetachDisposition::Failed => {
                            panic!("the corpus holds no failed detach")
                        }
                    };
                    lines.push(format!(
                        "REPLY {id} DOMAIN_CLOCK_DETACH {disposition} domain={} message={message}",
                        domain.as_str()
                    ));
                }
                ReplyBody::OpenIngestor(outcome) => match &outcome.disposition {
                    OpenIngestorDisposition::Opened(opened) => {
                        producer_lines(id.get(), opened, &outcome.message, lines);
                    }
                    OpenIngestorDisposition::Refused(refusal) => lines.push(format!(
                        "REPLY {id} INGESTOR_REFUSED refusal={} message={}",
                        schema_name(wire::ProducerRefusal::from(*refusal).variant_name()),
                        text(&outcome.message)
                    )),
                },
                ReplyBody::Submission(outcome) => lines.push(submission_line(id.get(), outcome)),
                ReplyBody::CloseIngestor(outcome) => {
                    let disposition = match outcome.disposition {
                        CloseIngestorDisposition::Closed => "closed",
                        CloseIngestorDisposition::NotOpen => "not_open",
                    };
                    lines.push(format!(
                        "REPLY {id} INGESTOR_CLOSE {disposition} message={}",
                        text(&outcome.message)
                    ));
                }
                ReplyBody::OpenEmitter(outcome) => match &outcome.disposition {
                    OpenEmitterDisposition::Opened(opened) => {
                        let window = match opened.window {
                            AckWindow::Sequential => "sequential".to_string(),
                            AckWindow::Parallel { max } => format!("parallel:{}", max.get()),
                        };
                        lines.push(format!(
                            "REPLY {id} EMITTER_OPENED domain={} emitter={} window={window} \
                             ack_timeout={} retry={}/{} granted={}/{} max_batch={}/{} message={}",
                            opened.domain.as_str(),
                            opened.emitter.as_str(),
                            opened.ack_timeout.as_nanos(),
                            opened.retry_backoff.as_nanos(),
                            opened.retry_max_backoff.as_nanos(),
                            opened.granted.batches,
                            opened.granted.bytes,
                            opened.max_batch_bytes,
                            opened.max_batch_rows,
                            text(&outcome.message)
                        ));
                        for field in &opened.fields {
                            lines.push(field_line("FIELD", field));
                        }
                    }
                    OpenEmitterDisposition::Refused(refusal) => lines.push(format!(
                        "REPLY {id} EMITTER_REFUSED refusal={} message={}",
                        schema_name(wire::EmitterOpenRefusal::from(*refusal).variant_name()),
                        text(&outcome.message)
                    )),
                },
                ReplyBody::ReadEmitterBatch(outcome) => match &outcome.disposition {
                    ReadEmitterDisposition::Batch(batch) => lines.push(format!(
                        "REPLY {id} EMITTER_BATCH identity={} reference={} source={} branch={} \
                         body={} members={} now={}",
                        hex(batch.identity.as_bytes()),
                        hex(batch.reference.as_bytes()),
                        batch.source_relay.as_str(),
                        batch
                            .branch_fingerprint
                            .as_ref()
                            .map(|branch| hex(branch))
                            .unwrap_or_else(|| "none".to_string()),
                        hex(&batch.batch),
                        batch.members,
                        batch.execution_now.unix_nanos(),
                    )),
                    ReadEmitterDisposition::Ended => lines.push(format!(
                        "REPLY {id} EMITTER_ENDED message={}",
                        text(&outcome.message)
                    )),
                },
                ReplyBody::SettleEmitterBatch(outcome) => lines.push(format!(
                    "REPLY {id} EMITTER_SETTLED disposition={} message={}",
                    schema_name(wire::EmitterSettlement::from(outcome.disposition).variant_name()),
                    text(&outcome.message)
                )),
                ReplyBody::CloseEmitter(outcome) => lines.push(format!(
                    "REPLY {id} EMITTER_CLOSE disposition={} message={}",
                    schema_name(
                        wire::EmitterCloseDisposition::from(outcome.disposition).variant_name()
                    ),
                    text(&outcome.message)
                )),
                other => panic!("the corpus holds no {other:?} reply"),
            }
        }
        ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) => {
            let handle = rows.subscription();
            lines.push(format!(
                "EVENT ROWS name={} generation={}",
                handle.name.as_str(),
                handle.generation
            ));
            let schema = row_schema();
            let batch = rows.batch();
            batch
                .conform(&schema)
                .assured("the corpus batch conforms to the corpus schema");
            let key = match (batch.branch_key(), &schema.branch) {
                (Some(key), Some(branch)) => cells(key, branch.fields()),
                _ => String::new(),
            };
            for row in batch.rows() {
                lines.push(format!("ROW [{key}] {}", cells(row, &schema.fields)));
            }
        }
        ServerMessage::Event(ServerEvent::SubscriptionEnded(ended)) => {
            lines.push(format!(
                "EVENT ENDED name={} generation={} reason={:?} message={}",
                ended.subscription.name.as_str(),
                ended.subscription.generation,
                ended.reason,
                text(&ended.message)
            ));
        }
        ServerMessage::Event(ServerEvent::DomainClockObserved(observed)) => {
            lines.push(format!(
                "EVENT DOMAIN_CLOCK domain={}",
                observed.domain.as_str()
            ));
            lines.push(clock_line(&observed.clock));
        }
        ServerMessage::Event(ServerEvent::DomainClockTicked(ticked)) => {
            lines.push(format!(
                "EVENT DOMAIN_CLOCK_TICK domain={} generation={} id={} boundary={} \
                 authority_utc={} serving_logical={}",
                ticked.domain.as_str(),
                ticked.tick.generation,
                ticked.tick.tick_id,
                ticked.tick.logical_boundary.unix_nanos(),
                ticked.tick.authority_utc.unix_nanos(),
                ticked.tick.serving_logical.unix_nanos(),
            ));
        }
        ServerMessage::Event(ServerEvent::DomainClockAttachmentEnded(ended)) => {
            lines.push(format!(
                "EVENT DOMAIN_CLOCK_ENDED domain={} reason={:?}",
                ended.domain.as_str(),
                ended.reason
            ));
        }
        ServerMessage::Event(ServerEvent::ProducerAdmissionChanged(changed)) => {
            lines.push(format!(
                "EVENT PRODUCER_ADMISSION producer={} admission={}",
                changed.producer,
                schema_name(wire::ProducerAdmission::from(changed.admission).variant_name())
            ));
        }
        ServerMessage::Event(ServerEvent::ProducerEnded(ended)) => {
            lines.push(format!(
                "EVENT PRODUCER_ENDED producer={} reason={} message={}",
                ended.producer,
                schema_name(wire::ProducerEndReason::from(ended.reason).variant_name()),
                text(&ended.message)
            ));
        }
        other => panic!("the corpus holds no {other:?} message"),
    }
}

fn render_client(message: &ClientMessage, lines: &mut Vec<String>) {
    let id = message.request_id.get();
    match &message.request {
        ClientRequest::Command(command) => {
            let domain = match &command.domain {
                Some(domain) => domain.as_str(),
                None => "none",
            };
            let position = match command.expected_transaction_position {
                Some(position) => position.accepted_operations().to_string(),
                None => "none".to_string(),
            };
            let preview = match &command.expected_preview {
                Some(preview) => format!(
                    "{}/{}/{}",
                    preview.transaction_id,
                    preview.position.accepted_operations(),
                    hex(preview.planning_basis.fingerprint())
                ),
                None => "none".to_string(),
            };
            lines.push(format!(
                "REQUEST {id} COMMAND query={} domain={domain} reference={} \
                 expected_position={position} expected_preview={preview}",
                text(&command.query),
                command.execution_reference.as_str()
            ));
        }
        ClientRequest::Subscribe(subscribe) => lines.push(format!(
            "REQUEST {id} SUBSCRIBE domain={} statement={} type={:?}",
            subscribe.domain.as_str(),
            text(&subscribe.statement),
            subscribe.subscription_type
        )),
        ClientRequest::Suggest(suggest) => lines.push(format!(
            "REQUEST {id} SUGGEST input={} cursor={} domain={} page_size={} continuation={}",
            text(suggest.input()),
            suggest.cursor(),
            suggest
                .domain()
                .map(|domain| domain.as_str())
                .unwrap_or("none"),
            suggest.page_size(),
            suggest.continuation().unwrap_or("none")
        )),
        ClientRequest::Choice(choice) => {
            let dependencies = choice
                .dependencies()
                .iter()
                .map(|selection| choice_value(&selection.value))
                .collect::<Vec<_>>()
                .join(",");
            lines.push(format!(
                "REQUEST {id} CHOICE target={:?} dependencies=[{dependencies}] search={} \
                 page_size={} cursor={}",
                choice.target(),
                text(choice.search()),
                choice.page_size(),
                choice.page_cursor().unwrap_or("none")
            ));
        }
        ClientRequest::Cancel(cancel) => {
            lines.push(format!(
                "REQUEST {id} CANCEL target={}",
                cancel.target.get()
            ));
        }
        ClientRequest::AttachDomainClock(attach) => lines.push(format!(
            "REQUEST {id} ATTACH_DOMAIN_CLOCK domain={}",
            attach.domain.as_str()
        )),
        ClientRequest::DetachDomainClock(detach) => lines.push(format!(
            "REQUEST {id} DETACH_DOMAIN_CLOCK domain={}",
            detach.domain.as_str()
        )),
        ClientRequest::OpenIngestor(open) => {
            lines.push(format!(
                "REQUEST {id} OPEN_INGESTOR domain={} ingestor={} batches={} bytes={}",
                open.domain.as_str(),
                open.ingestor.as_str(),
                open.limits.batches,
                open.limits.bytes
            ));
            for field in &open.expected_fields {
                lines.push(field_line("FIELD", field));
            }
        }
        ClientRequest::SubmitBatch(submit) => lines.push(format!(
            "REQUEST {id} SUBMIT_BATCH producer={} batch={}",
            submit.producer,
            hex(&submit.batch)
        )),
        ClientRequest::CloseIngestor(close) => lines.push(format!(
            "REQUEST {id} CLOSE_INGESTOR producer={}",
            close.producer
        )),
        ClientRequest::OpenEmitter(open) => {
            lines.push(format!(
                "REQUEST {id} OPEN_EMITTER domain={} emitter={} batches={} bytes={}",
                open.domain.as_str(),
                open.emitter.as_str(),
                open.limits.batches,
                open.limits.bytes
            ));
            for field in &open.expected_fields {
                lines.push(field_line("FIELD", field));
            }
        }
        ClientRequest::ReadEmitterBatch(read) => lines.push(format!(
            "REQUEST {id} READ_EMITTER_BATCH consumer={}",
            read.consumer
        )),
        ClientRequest::SettleEmitterBatch(settle) => lines.push(format!(
            "REQUEST {id} SETTLE_EMITTER_BATCH consumer={} reference={} decision={}",
            settle.consumer,
            hex(settle.reference.as_bytes()),
            match &settle.decision {
                crate::EmitterBatchDecision::Ack => "ack".to_string(),
                crate::EmitterBatchDecision::Retry => "retry".to_string(),
                crate::EmitterBatchDecision::Reject(reason) => format!("reject:{}", text(reason)),
            }
        )),
        ClientRequest::CloseEmitter(close) => lines.push(format!(
            "REQUEST {id} CLOSE_EMITTER consumer={}",
            close.consumer
        )),
        other => panic!("the corpus holds no {other:?} request"),
    }
}

/// The lines of one command outcome, the first of them after `head`.
fn render_command(head: &str, outcome: &CommandOutcome, lines: &mut Vec<String>) {
    lines.push(format!(
        "{head} {} reference={} origin={:?} message={}",
        disposition(&outcome.disposition),
        outcome.execution_reference.as_str(),
        outcome.origin,
        text(&outcome.message)
    ));
    for diagnostic in &outcome.diagnostics {
        let span = match diagnostic.span {
            Some(span) => format!("{}..{}", span.start(), span.end()),
            None => "none".to_string(),
        };
        lines.push(format!(
            "DIAGNOSTIC span={span} message={}",
            text(&diagnostic.message)
        ));
    }
    match &outcome.disposition {
        CommandDisposition::NotLeader(LeaderRedirect {
            leader: Some(leader),
        }) => {
            let uri = |uri: &Option<url::Url>| match uri {
                Some(uri) => uri.as_str().to_string(),
                None => "none".to_string(),
            };
            lines.push(format!(
                "LEADER node={} grpc={} console={}",
                leader.node.as_str(),
                uri(&leader.grpc_uri),
                uri(&leader.web_console_uri)
            ));
        }
        CommandDisposition::NotLeader(LeaderRedirect { leader: None }) => {
            lines.push("LEADER none".to_string());
        }
        CommandDisposition::OutcomeUnknown(cause) => {
            lines.push(format!("UNKNOWN cause={cause:?}"));
        }
        _ => {}
    }
    if let Some(archive) = &outcome.backup {
        let users = match archive.users {
            Some(users) => users.to_string(),
            None => "none".to_string(),
        };
        lines.push(format!(
            "BACKUP total_bytes={} digest={} captured_at={} retained_until={} resources={:?} \
             users={users}",
            archive.total_bytes,
            hex(archive.digest.as_bytes()),
            archive.captured_at.unix_nanos(),
            archive.retained_until.unix_nanos(),
            archive.resources
        ));
        for domain in &archive.domains {
            lines.push(format!(
                "BACKUP_DOMAIN domain={} revision={} sections={} section_bytes={}",
                domain.domain.as_str(),
                domain.revision,
                domain.sections,
                domain.section_bytes
            ));
        }
    }
    if let Some(report) = &outcome.restore {
        render_restore_report(report, lines);
    }
}

fn render_restore_report(report: &RestoreReport, lines: &mut Vec<String>) {
    let users = match &report.users {
        Some(users) => format!(
            "created:{},skipped:{},replaced:{}",
            users.created, users.skipped, users.replaced
        ),
        None => "none".to_string(),
    };
    lines.push(format!(
        "RESTORE mode={:?} total_bytes={} digest={} captured_at={} users={users}",
        report.mode,
        report.archive.total_bytes,
        hex(report.archive.digest.as_bytes()),
        report.captured_at.unix_nanos()
    ));
    for domain in &report.domains {
        let planned = match &domain.planned_models {
            Some(_) => "present",
            None => "none",
        };
        lines.push(format!(
            "RESTORE_DOMAIN source={} domain={} resource_versions={} models={} \
             planned_models={planned}",
            domain.source.as_str(),
            domain.domain.as_str(),
            domain.resource_versions,
            domain.models
        ));
    }
    for step in &report.steps {
        let (kind, domain) = match &step.step {
            RestoreStep::Users => ("Users", "none"),
            RestoreStep::CreateDomain(domain) => ("CreateDomain", domain.as_str()),
            RestoreStep::ImportResources(domain) => ("ImportResources", domain.as_str()),
            RestoreStep::ApplyModels(domain) => ("ApplyModels", domain.as_str()),
        };
        lines.push(format!(
            "RESTORE_STEP kind={kind} domain={domain} outcome={:?}",
            step.outcome
        ));
    }
}

fn render_restore(message: &RestoreMessage, lines: &mut Vec<String>) {
    match message {
        RestoreMessage::Start(start) => lines.push(format!(
            "RESTORE_START request={} reference={} statement={} total_bytes={} digest={}",
            start.request_id.get(),
            start.execution_reference.as_str(),
            text(&start.statement),
            start.archive.total_bytes,
            hex(start.archive.digest.as_bytes())
        )),
        RestoreMessage::Chunk(chunk) => {
            lines.push(format!("RESTORE_CHUNK bytes={}", hex(chunk.bytes())));
        }
    }
}

fn render_restore_reply(reply: &RestoreReply, lines: &mut Vec<String>) {
    let request = match reply.request_id {
        Some(request) => request.get().to_string(),
        None => "none".to_string(),
    };
    match &reply.disposition {
        RestoreDisposition::Outcome(outcome) => {
            render_command(&format!("RESTORE_REPLY {request} COMMAND"), outcome, lines);
        }
        RestoreDisposition::UploadFailed { failure, message } => lines.push(format!(
            "RESTORE_REPLY {request} FAILED failure={failure:?} message={}",
            text(message)
        )),
    }
}

fn render_download_request(request: &BackupDownloadRequest, lines: &mut Vec<String>) {
    lines.push(format!(
        "REQUEST DOWNLOAD_BACKUP reference={}",
        request.execution_reference.as_str()
    ));
}

fn render_download(message: &BackupDownloadMessage, lines: &mut Vec<String>) {
    match message {
        BackupDownloadMessage::Start(start) => lines.push(format!(
            "DOWNLOAD START total_bytes={} digest={}",
            start.total_bytes,
            hex(start.digest.as_bytes())
        )),
        BackupDownloadMessage::Chunk(chunk) => {
            lines.push(format!("DOWNLOAD CHUNK bytes={}", hex(chunk.bytes())));
        }
        BackupDownloadMessage::Complete => lines.push("DOWNLOAD COMPLETE".to_string()),
        BackupDownloadMessage::Failed(failed) => lines.push(format!(
            "DOWNLOAD FAILED failure={:?} message={}",
            failed.failure,
            text(&failed.message)
        )),
        BackupDownloadMessage::NotLeader(LeaderRedirect {
            leader: Some(leader),
        }) => {
            let uri = |uri: &Option<url::Url>| match uri {
                Some(uri) => uri.as_str().to_string(),
                None => "none".to_string(),
            };
            lines.push(format!(
                "DOWNLOAD LEADER node={} grpc={} console={}",
                leader.node.as_str(),
                uri(&leader.grpc_uri),
                uri(&leader.web_console_uri)
            ));
        }
        BackupDownloadMessage::NotLeader(LeaderRedirect { leader: None }) => {
            lines.push("DOWNLOAD LEADER none".to_string());
        }
    }
}

/// The report of the frames as they are checked in.
fn report_of(frames: &[(&'static str, Bytes)]) -> String {
    let mut lines = Vec::new();
    for (file, bytes) in frames {
        lines.push(format!("FRAME {file}"));
        if file.ends_with(".nxbq") {
            let frame =
                VerifiedFrame::<BackupDownloadRequestFrame>::verify(bytes.clone(), &limits())
                    .assured("a corpus download request verifies");
            let request =
                BackupDownloadRequest::decode(&frame).assured("a corpus download request decodes");
            render_download_request(&request, &mut lines);
        } else if file.ends_with(".nxbd") {
            let frame = VerifiedFrame::<BackupDownloadFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus download frame verifies");
            let message =
                BackupDownloadMessage::decode(&frame).assured("a corpus download frame decodes");
            render_download(&message, &mut lines);
        } else if file.ends_with(".nxrm") {
            let frame = VerifiedFrame::<RestoreFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus restore frame verifies");
            let message = RestoreMessage::decode(&frame).assured("a corpus restore frame decodes");
            render_restore(&message, &mut lines);
        } else if file.ends_with(".nxrr") {
            let frame = VerifiedFrame::<RestoreReplyFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus restore reply verifies");
            let reply = RestoreReply::decode(&frame).assured("a corpus restore reply decodes");
            render_restore_reply(&reply, &mut lines);
        } else if file.ends_with(".nxcm") {
            let frame = VerifiedFrame::<ClientFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus client frame verifies");
            let message = ClientMessage::decode(&frame).assured("a corpus client frame decodes");
            render_client(&message, &mut lines);
        } else {
            let frame = VerifiedFrame::<ServerFrame>::verify(bytes.clone(), &limits())
                .assured("a corpus server frame verifies");
            let message = ServerMessage::decode(&frame).assured("a corpus server frame decodes");
            render_server(&message, &mut lines);
        }
    }
    let mut report = lines.join("\n");
    report.push('\n');
    report
}

#[test]
fn the_checked_in_corpus_is_what_the_encoder_writes_and_reads() {
    let directory = corpus_directory();
    let written = corpus_frames();
    if std::env::var_os(UPDATE_ENV).is_some() {
        fs::create_dir_all(&directory).assured("the corpus directory can be created");
        for (file, bytes) in &written {
            fs::write(directory.join(file), bytes).assured("a corpus frame can be written");
        }
        fs::write(directory.join(REPORT), report_of(&written))
            .assured("the corpus report can be written");
    }
    let mut checked_in = Vec::new();
    for (file, bytes) in &written {
        let stored = fs::read(directory.join(file)).unwrap_or_else(|error| {
            panic!("corpus frame {file} is missing ({error}); run `just update-client-wire-corpus`")
        });
        assert_eq!(
            stored.as_slice(),
            bytes.as_ref(),
            "corpus frame {file} is not what the encoder writes; run `just \
             update-client-wire-corpus` and review the report"
        );
        checked_in.push((*file, Bytes::from(stored)));
    }
    let mut files: Vec<String> = fs::read_dir(&directory)
        .assured("the corpus directory is readable")
        .map(|entry| {
            entry
                .assured("a corpus directory entry is readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|file| file != REPORT)
        .collect();
    files.sort();
    let expected: Vec<&str> = written.iter().map(|(file, _)| *file).collect();
    assert_eq!(
        files, expected,
        "the corpus holds exactly the frames this test writes"
    );
    let report = fs::read_to_string(directory.join(REPORT)).assured("the corpus report exists");
    assert_eq!(report_of(&checked_in), report);
}
