//! The backup dialog's drafts, its download and restore protocol decisions, and the fencing that
//! keeps an outcome of another backup or restore out of the dialog.

mod downloads;
mod progress;
mod records;
mod restores;

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadMessage,
    CommandDisposition, CommandOutcome, LeaderEndpoints, LeaderRedirect, OutcomeOrigin, RequestId,
    RestoreDisposition, RestoreReply, RestoreUploadFailure, UnknownOutcomeCause,
};
use nervix_models::{
    ArchiveDigest, Backup, BackupArchiveSummary, BackupCapture, BackupCut, BackupDomainSummary,
    BackupResources, BackupScope, ClusterNodeName, CommandExecutionReference, DomainName,
    DomainStatus, ExistingUserPolicy, ImpactPlanningBasis, ImpactReportCompleteness,
    RestoreArchive, RestoreLifecycle, RestoreMode, RestoreReport, RestoreScope, RestoreState,
    RestoreStep, RestoreStepOutcome, RestoreStepReport, RestoredDomain, Statement, Timestamp,
    TransactionImpactReport, TransactionPosition,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

use super::{
    BackupCommand, BackupOutcomeEffect, BackupProgress, RestoreConclusion, RestoreProgress,
    TransferPhase, TransferProgress,
    archive_download::{ArchiveReceipt, AttemptStage, DownloadError, FrameStep},
    backup_draft::{BackupDraft, BackupDraftError, BackupScopeChoice, CaptureChoice},
    download_file_name,
    pending_backup::{PendingBackup, RecordedEntries},
    reports::{restore_report_lines, summary_lines},
    restore_draft::{RestoreDraft, RestoreDraftError, RestoreScopeChoice},
    restore_stream::{ReplyStep, RestoreEnd, reply_step},
};

const ARCHIVE: &[u8] = b"an archive of the console's backup";

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test's domain names are valid")
}

fn reference() -> CommandExecutionReference {
    CommandExecutionReference::parse("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44")
        .assured("the test reference is a valid literal")
}

fn summary(bytes: &[u8]) -> BackupArchiveSummary {
    let length = u64::try_from(bytes.len()).assured("a test archive is short");
    BackupArchiveSummary {
        total_bytes: NonZeroU64::new(length).assured("a test archive has bytes"),
        digest: ArchiveDigest::from_bytes(*blake3::hash(bytes).as_bytes()),
        captured_at: Timestamp::from_unix_nanos(1_790_000_000_000_000_000),
        retained_until: Timestamp::from_unix_nanos(1_790_000_900_000_000_000),
        resources: BackupResources::Included,
        users: Some(2),
        domains: vec![
            BackupDomainSummary {
                domain: domain("tenant"),
                revision: 41,
                cut: BackupCut::Stopped,
                sections: 3,
                section_bytes: 1024,
            },
            BackupDomainSummary {
                domain: domain("tenant_other"),
                revision: 42,
                cut: BackupCut::ConfigurationOnly,
                sections: 1,
                section_bytes: 64,
            },
        ],
    }
}

fn outcome(disposition: CommandDisposition) -> CommandOutcome {
    CommandOutcome {
        execution_reference: reference(),
        origin: OutcomeOrigin::Executed,
        disposition,
        message: "the server's message".to_string(),
        diagnostics: Vec::new(),
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
        wasm_state: None,
        resource: None,
        backup: None,
        restore: None,
    }
}

fn command(attempt: u64) -> BackupCommand {
    BackupCommand {
        attempt,
        reference: reference(),
        file_name: "tenant.nvxb".to_string(),
    }
}

/// The Model `statement` parses into, which a draft's canonical NSPL must reproduce.
fn parsed(statement: &str) -> Statement {
    match parse_client_statement(statement) {
        Ok(ClientStatement::Server(statement)) => statement,
        Ok(other) => panic!("'{statement}' parsed into the client statement {other:?}"),
        Err(error) => panic!("'{statement}' does not parse: {error:?}"),
    }
}

#[test]
fn a_backup_draft_lowers_to_the_backup_model_its_canonical_statement_parses_into() {
    let draft = BackupDraft::for_domain(Some(&domain("tenant")));
    let backup = draft.model().assured("the default domain draft lowers");
    assert_eq!(
        backup,
        Backup {
            scope: BackupScope::Domain(Some(domain("tenant"))),
            destination: "nervix-backup.nvxb".to_string(),
            resources: BackupResources::Included,
            capture: BackupCapture::Quiesced { timeout: None },
        }
    );
    assert_eq!(
        backup.to_canonical_nspl(),
        "BACKUP DOMAIN tenant TO 'nervix-backup.nvxb';"
    );
    assert_eq!(
        parsed(&backup.to_canonical_nspl()),
        Statement::Backup(backup.clone())
    );

    let cluster = BackupDraft {
        scope: BackupScopeChoice::Cluster,
        destination: " cluster.nvxb ".to_string(),
        resources: BackupResources::Omitted,
        capture: CaptureChoice::Quiesced,
        timeout: "30s".to_string(),
        ..draft.clone()
    };
    let backup = cluster.model().assured("a cluster draft lowers");
    assert_eq!(
        backup.to_canonical_nspl(),
        "BACKUP CLUSTER TO 'cluster.nvxb' WITHOUT RESOURCES TIMEOUT 30s;"
    );
    assert_eq!(
        parsed(&backup.to_canonical_nspl()),
        Statement::Backup(backup.clone())
    );
    for (capture, clause) in [
        (CaptureChoice::Live, " WITHOUT PAUSE;"),
        (CaptureChoice::ConfigurationOnly, " WITHOUT STATE;"),
    ] {
        let draft = BackupDraft {
            capture,
            timeout: "not a duration".to_string(),
            ..draft.clone()
        };
        let backup = draft
            .model()
            .assured("a live or configuration-only draft ignores the quiesce timeout");
        assert!(backup.to_canonical_nspl().ends_with(clause));
        assert_eq!(
            parsed(&backup.to_canonical_nspl()),
            Statement::Backup(backup.clone())
        );
    }
}

#[test]
fn a_backup_draft_names_what_keeps_it_from_lowering() {
    let draft = BackupDraft::for_domain(None);
    assert_eq!(draft.scope, BackupScopeChoice::Cluster);
    let domain_draft = BackupDraft {
        scope: BackupScopeChoice::Domain,
        ..draft.clone()
    };
    let error = domain_draft
        .model()
        .expect_err("a domain backup names its domain");
    assert_eq!(error.current_context(), &BackupDraftError::MissingDomain);
    let error = BackupDraft {
        domain: "not a domain".to_string(),
        ..domain_draft
    }
    .model()
    .expect_err("a domain name is an identifier");
    assert_eq!(
        error.current_context(),
        &BackupDraftError::InvalidDomain {
            text: "not a domain".to_string()
        }
    );
    let error = BackupDraft {
        destination: "  ".to_string(),
        ..draft.clone()
    }
    .model()
    .expect_err("an archive file is named");
    assert_eq!(
        error.current_context(),
        &BackupDraftError::MissingDestination
    );
    let error = BackupDraft {
        timeout: "soon".to_string(),
        ..draft
    }
    .model()
    .expect_err("a quiesce timeout is a duration");
    assert_eq!(
        error.current_context(),
        &BackupDraftError::InvalidTimeout {
            text: "soon".to_string()
        }
    );
}

#[test]
fn a_restore_draft_lowers_to_the_restore_model_of_the_archive_it_names() {
    let draft = RestoreDraft {
        domain: "tenant".to_string(),
        target: "tenant_copy".to_string(),
        ..RestoreDraft::default()
    };
    let error = draft
        .model(None, RestoreMode::DryRun)
        .expect_err("a restore reads an archive");
    assert_eq!(error.current_context(), &RestoreDraftError::MissingArchive);

    let dry_run = draft
        .model(Some("tenant.nvxb"), RestoreMode::DryRun)
        .assured("a domain draft with its archive lowers");
    assert_eq!(
        dry_run.to_canonical_nspl(),
        "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' DRY RUN;"
    );
    let apply = draft
        .model(Some("tenant.nvxb"), RestoreMode::Apply)
        .assured("the same draft lowers to the restore itself");
    assert_eq!(
        apply.to_canonical_nspl(),
        "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb';"
    );
    for restore in [dry_run, apply] {
        assert_eq!(
            parsed(&restore.to_canonical_nspl()),
            Statement::Restore(restore.clone())
        );
    }

    let cluster = RestoreDraft {
        scope: RestoreScopeChoice::Cluster,
        existing_users: ExistingUserPolicy::Replace,
        lifecycle: RestoreLifecycle::Resume,
        state: RestoreState::WithoutSourceOffsets,
        ..RestoreDraft::default()
    };
    let restore = cluster
        .model(Some("cluster.nvxb"), RestoreMode::Apply)
        .assured("a cluster draft needs no domain");
    assert_eq!(
        restore.to_canonical_nspl(),
        "RESTORE CLUSTER FROM 'cluster.nvxb' RESUME ON EXISTING USER REPLACE WITHOUT SOURCE \
         OFFSETS;"
    );
    assert_eq!(
        parsed(&restore.to_canonical_nspl()),
        Statement::Restore(restore.clone())
    );
}

#[test]
fn a_restore_draft_names_what_keeps_it_from_lowering() {
    let error = RestoreDraft::default()
        .model(Some("tenant.nvxb"), RestoreMode::Apply)
        .expect_err("a domain restore names its archived domain");
    assert_eq!(error.current_context(), &RestoreDraftError::MissingDomain);
    let error = RestoreDraft {
        domain: "tenant".to_string(),
        target: "not a domain".to_string(),
        ..RestoreDraft::default()
    }
    .model(Some("tenant.nvxb"), RestoreMode::Apply)
    .expect_err("a domain is restored under an identifier");
    assert_eq!(
        error.current_context(),
        &RestoreDraftError::InvalidTarget {
            text: "not a domain".to_string()
        }
    );
}

#[test]
fn a_typed_restore_fills_the_draft_that_lowers_back_to_it() {
    let typed = [
        "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' RESUME WITHOUT STATE;",
        "RESTORE DOMAIN tenant FROM 'tenant.nvxb';",
        "RESTORE CLUSTER FROM 'cluster.nvxb' ON EXISTING USER SKIP;",
    ];
    for statement in typed {
        let Statement::Restore(restore) = parsed(statement) else {
            panic!("'{statement}' is a RESTORE");
        };
        let draft = RestoreDraft::of_model(&restore);
        let lowered = draft
            .model(Some(&restore.source), restore.mode)
            .assured("a typed restore's draft lowers");
        assert_eq!(lowered, restore, "the draft of '{statement}'");
    }
    let Statement::Restore(restore) = parsed("RESTORE CLUSTER FROM 'x.nvxb' DRY RUN;") else {
        panic!("a cluster dry run is a RESTORE");
    };
    assert_eq!(
        RestoreDraft::of_model(&restore).scope,
        RestoreScopeChoice::Cluster
    );
    assert!(matches!(
        restore.scope,
        RestoreScope::Cluster {
            existing_users: ExistingUserPolicy::Fail
        }
    ));
}

#[test]
fn the_browser_saves_an_archive_under_the_last_component_of_its_file() {
    assert_eq!(download_file_name("tenant.nvxb"), "tenant.nvxb");
    assert_eq!(download_file_name("backups/tenant.nvxb"), "tenant.nvxb");
    assert_eq!(
        download_file_name("C:\\backups\\tenant.nvxb"),
        "tenant.nvxb"
    );
    assert_eq!(download_file_name("backups/"), "backups/");
}

fn start(bytes: &[u8]) -> BackupDownloadMessage {
    let summary = summary(bytes);
    BackupDownloadMessage::Start(BackupArchiveStart {
        total_bytes: summary.total_bytes,
        digest: summary.digest,
    })
}

fn chunk(bytes: &[u8]) -> BackupDownloadMessage {
    let limits = nervix_client_wire::SessionLimits::DEFAULT;
    let frame = BackupDownloadMessage::encode_chunk(bytes, &limits)
        .assured("a test chunk fits a frame")
        .verify(&limits)
        .assured("an encoded chunk verifies");
    BackupDownloadMessage::decode(&frame).assured("an encoded chunk decodes")
}

#[test]
fn a_download_accepts_the_archive_only_once_every_byte_matches_its_summary() {
    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let mut stage = AttemptStage::AwaitingStart;
    assert_eq!(
        stage
            .accept(&start(ARCHIVE), &mut receipt)
            .assured("the start matches the summary"),
        FrameStep::Continue
    );
    let (head, tail) = ARCHIVE.split_at(10);
    for part in [head, tail] {
        assert_eq!(
            stage
                .accept(&chunk(part), &mut receipt)
                .assured("every chunk stays within the size"),
            FrameStep::Continue
        );
    }
    assert_eq!(
        receipt.received(),
        u64::try_from(ARCHIVE.len()).assured("a test archive is short")
    );
    assert_eq!(
        stage
            .accept(&BackupDownloadMessage::Complete, &mut receipt)
            .assured("the archive is complete and verified"),
        FrameStep::Complete
    );
}

#[test]
fn a_download_refuses_a_foreign_growing_or_altered_archive() {
    let other = b"another archive of the same length";
    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let error = AttemptStage::AwaitingStart
        .accept(&start(other), &mut receipt)
        .expect_err("a start of another archive is refused");
    assert_eq!(error.current_context(), &DownloadError::Mismatch);

    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let mut stage = AttemptStage::Receiving;
    stage
        .accept(&chunk(ARCHIVE), &mut receipt)
        .assured("the whole archive is within its size");
    let error = stage
        .accept(&chunk(b"!"), &mut receipt)
        .expect_err("an archive growing past its size is refused");
    assert_eq!(error.current_context(), &DownloadError::Mismatch);

    let mut altered = ARCHIVE.to_vec();
    altered[0] ^= 1;
    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let mut stage = AttemptStage::Receiving;
    stage
        .accept(&chunk(&altered), &mut receipt)
        .assured("an altered chunk has the archive's size");
    let error = stage
        .accept(&BackupDownloadMessage::Complete, &mut receipt)
        .expect_err("bytes without the reported digest are never accepted");
    assert_eq!(error.current_context(), &DownloadError::Mismatch);

    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let mut stage = AttemptStage::Receiving;
    stage
        .accept(&chunk(&ARCHIVE[..5]), &mut receipt)
        .assured("a partial archive is within its size");
    let error = stage
        .accept(&BackupDownloadMessage::Complete, &mut receipt)
        .expect_err("a partial archive is never accepted");
    assert_eq!(error.current_context(), &DownloadError::Mismatch);
}

#[test]
fn a_download_answer_out_of_order_or_refused_ends_the_attempt() {
    let mut receipt = ArchiveReceipt::new(&summary(ARCHIVE));
    let error = AttemptStage::AwaitingStart
        .accept(&chunk(ARCHIVE), &mut receipt)
        .expect_err("a chunk before the start breaks the order");
    assert_eq!(error.current_context(), &DownloadError::OutOfOrder);
    let error = AttemptStage::Receiving
        .accept(&start(ARCHIVE), &mut receipt)
        .expect_err("a second start breaks the order");
    assert_eq!(error.current_context(), &DownloadError::OutOfOrder);

    let redirect = LeaderRedirect {
        leader: Some(LeaderEndpoints {
            node: ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            grpc_uri: None,
            web_console_uri: Some(
                url::Url::parse("http://127.0.0.1:47421").assured("the test URL parses"),
            ),
        }),
    };
    assert_eq!(
        AttemptStage::AwaitingStart
            .accept(
                &BackupDownloadMessage::NotLeader(redirect.clone()),
                &mut receipt
            )
            .assured("a redirect answers the request"),
        FrameStep::Redirect(redirect)
    );

    for (failure, retryable) in [
        (BackupDownloadFailure::ReadFailed, true),
        (BackupDownloadFailure::NotRetained, false),
        (BackupDownloadFailure::Expired, false),
        (BackupDownloadFailure::NotOwner, false),
        (BackupDownloadFailure::InvalidRequest, false),
    ] {
        let refused = BackupDownloadMessage::Failed(BackupDownloadFailed {
            failure,
            message: "refused".to_string(),
        });
        let error = AttemptStage::AwaitingStart
            .accept(&refused, &mut receipt)
            .expect_err("a refusal ends the attempt");
        assert_eq!(
            error.current_context().is_retryable(),
            retryable,
            "{failure:?}"
        );
    }
    assert!(DownloadError::Interrupted.is_retryable());
    assert!(!DownloadError::Mismatch.is_retryable());
}

#[test]
fn an_outcome_of_another_backup_never_reaches_the_dialog() {
    let mut completed = outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    completed.backup = Some(Box::new(summary(ARCHIVE)));
    // A backup sent before the console's credentials changed, or before a newer backup, is not
    // the one the dialog shows.
    assert_eq!(
        command(1).outcome_effect(2, &completed),
        BackupOutcomeEffect::Stale
    );
    assert_eq!(
        command(2).outcome_effect(2, &completed),
        BackupOutcomeEffect::Download(Box::new(summary(ARCHIVE)))
    );
    let failed = outcome(CommandDisposition::Failed);
    assert_eq!(
        command(2).outcome_effect(2, &failed),
        BackupOutcomeEffect::Failed("the server's message".to_string())
    );
    let expired = outcome(CommandDisposition::ExecutionReferenceExpired);
    let BackupOutcomeEffect::Failed(reason) = command(2).outcome_effect(2, &expired) else {
        panic!("an expired reference ends the backup the dialog shows");
    };
    assert!(reason.contains("can no longer be recovered"));
    let without_summary = outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    assert_eq!(
        command(2).outcome_effect(2, &without_summary),
        BackupOutcomeEffect::Failed("the backup reported no archive".to_string())
    );
}

#[test]
fn a_cleared_dialog_drops_what_arrives_late_for_the_backup_and_restore_it_showed() {
    leptos::prelude::Owner::new().with(|| {
        use leptos::prelude::*;

        let backups = dialog_signals();
        // The dialog shows its first backup, which runs, and its first restore, which uploads.
        backups.backup_attempt.set(1);
        backups.backup.set(BackupProgress::Running);
        backups.restore_attempt.set(1);
        backups.transfer(1, TransferPhase::Uploading, 4, 34);
        assert_eq!(
            backups.restore_transfer.get_untracked(),
            Some(TransferProgress {
                phase: TransferPhase::Uploading,
                done: 4,
                total: 34,
            })
        );

        // Another identity takes over the console, and what the first one sent ends late.
        backups.clear();
        let mut completed = outcome(CommandDisposition::Completed {
            already_existed: false,
        });
        completed.backup = Some(Box::new(summary(ARCHIVE)));
        backups.backup_outcome(&command(1), &completed);
        backups.backup_refused(&command(1), "the session closed".to_string());
        backups.transfer(1, TransferPhase::Uploading, 34, 34);
        assert_eq!(backups.backup.get_untracked(), BackupProgress::Idle);
        assert_eq!(backups.download.get_untracked(), None);
        assert_eq!(backups.restore_transfer.get_untracked(), None);
    });
}

#[test]
fn a_pending_backup_reads_back_from_the_entries_it_is_recorded_as() {
    for domain in [Some(domain("tenant")), None] {
        let pending = PendingBackup {
            reference: reference(),
            query: "BACKUP DOMAIN tenant TO 'tenant.nvxb';".to_string(),
            domain,
        };
        assert_eq!(PendingBackup::of_entries(pending.entries()), Some(pending));
    }
    let recorded = PendingBackup {
        reference: reference(),
        query: "BACKUP CLUSTER TO 'cluster.nvxb';".to_string(),
        domain: None,
    }
    .entries();
    let foreign_reference = RecordedEntries {
        reference: "not a reference".to_string(),
        ..recorded.clone()
    };
    assert_eq!(PendingBackup::of_entries(foreign_reference), None);
    let foreign_domain = RecordedEntries {
        domain: Some("not a domain".to_string()),
        ..recorded
    };
    assert_eq!(PendingBackup::of_entries(foreign_domain), None);
}

#[test]
fn a_backup_reports_where_it_stands() {
    let summary = Box::new(summary(ARCHIVE));
    let downloading = BackupProgress::Downloading {
        summary: summary.clone(),
        received: 7,
    };
    assert!(downloading.is_active());
    assert_eq!(
        downloading.status(),
        format!("downloading 7 of {} bytes", ARCHIVE.len())
    );
    assert_eq!(downloading.transfer(), Some((7, 34)));
    let downloaded = BackupProgress::Downloaded {
        summary: summary.clone(),
        file_name: "tenant.nvxb".to_string(),
    };
    assert!(!downloaded.is_active());
    assert!(downloaded.status().starts_with(&format!(
        "downloaded 'tenant.nvxb': {} bytes, BLAKE3 ",
        ARCHIVE.len()
    )));
    assert_eq!(downloaded.error(), None);
    let not_downloaded = BackupProgress::NotDownloaded {
        summary,
        reason: "the server refused the download".to_string(),
        retryable: false,
    };
    assert_eq!(
        not_downloaded.status(),
        "the backup completed, but its archive was not downloaded"
    );
    assert_eq!(
        not_downloaded.error().as_deref(),
        Some("the server refused the download")
    );
    assert!(BackupProgress::Running.is_active());
    assert_eq!(
        BackupProgress::Resuming {
            reference: reference()
        }
        .status(),
        format!("resuming backup '{}'", reference())
    );
}

#[test]
fn a_summary_names_every_backed_up_domain_and_nothing_the_archive_holds() {
    let lines = summary_lines(&summary(ARCHIVE));
    assert!(lines[0].starts_with(&format!("archive: {} bytes · BLAKE3 ", ARCHIVE.len())));
    assert!(lines.contains(&"2 users".to_string()));
    assert!(lines.contains(
        &"domain tenant · cut stopped · revision 41 · 3 sections, 1024 bytes".to_string()
    ));
    assert!(lines.contains(
        &"domain tenant_other · cut without state · revision 42 · 1 sections, 64 bytes".to_string()
    ));
}

fn restore_report(mode: RestoreMode, outcomes: [RestoreStepOutcome; 3]) -> RestoreReport {
    let planned_models = match mode {
        RestoreMode::DryRun => Some(
            TransactionImpactReport::new(
                domain("tenant_copy"),
                TransactionPosition::new(0),
                ImpactPlanningBasis::new([1; 32]),
                ImpactReportCompleteness::Complete,
                Vec::new(),
                Vec::new(),
            )
            .assured("an empty report has no operation-step inconsistencies"),
        ),
        RestoreMode::Apply => None,
    };
    let [created, imported, applied] = outcomes;
    RestoreReport {
        mode,
        archive: RestoreArchive {
            total_bytes: NonZeroU64::new(34).assured("thirty-four is non-zero"),
            digest: ArchiveDigest::from_bytes([9; 32]),
        },
        captured_at: Timestamp::from_unix_nanos(1_790_000_000_000_000_000),
        users: None,
        domains: vec![RestoredDomain {
            source: domain("tenant"),
            domain: domain("tenant_copy"),
            resource_versions: 1,
            models: 5,
            status: DomainStatus::Stopped,
            start_version: 3,
            planned_models,
        }],
        steps: vec![
            RestoreStepReport {
                step: RestoreStep::CreateDomain(domain("tenant_copy")),
                outcome: created,
            },
            RestoreStepReport {
                step: RestoreStep::ImportResources(domain("tenant_copy")),
                outcome: imported,
            },
            RestoreStepReport {
                step: RestoreStep::ApplyModels(domain("tenant_copy")),
                outcome: applied,
            },
        ],
    }
}

#[test]
fn a_report_shows_each_restored_domain_and_what_became_of_each_step() {
    let report = restore_report(
        RestoreMode::Apply,
        [
            RestoreStepOutcome::Applied,
            RestoreStepOutcome::Failed,
            RestoreStepOutcome::NotAttempted,
        ],
    );
    let lines = restore_report_lines(&report);
    assert!(lines[0].starts_with("restore of an archive of 34 bytes"));
    assert!(
        lines.contains(
            &"tenant as tenant_copy: STOPPED at start version 3 · 5 models · 1 resource versions"
                .to_string()
        )
    );
    assert!(lines.contains(&"create domain 'tenant_copy': applied".to_string()));
    assert!(
        lines.contains(&"import resource versions of domain 'tenant_copy': failed".to_string())
    );
    assert!(lines.contains(&"apply models of domain 'tenant_copy': not attempted".to_string()));
}

fn restore_outcome(disposition: CommandDisposition, report: Option<RestoreReport>) -> RestoreEnd {
    let mut outcome = outcome(disposition);
    outcome.restore = report.map(Box::new);
    RestoreEnd::Outcome(Box::new(outcome))
}

const DRY_RUN: &str = "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' DRY RUN;";

#[test]
fn a_dry_run_plans_the_draft_revision_it_ran_and_draws_its_first_planned_domain() {
    let planned = restore_report(RestoreMode::DryRun, [RestoreStepOutcome::Planned; 3]);
    let completed = CommandDisposition::Completed {
        already_existed: false,
    };
    let conclusion = RestoreConclusion::of(
        restore_outcome(completed, Some(planned.clone())),
        DRY_RUN,
        RestoreMode::DryRun,
        7,
    );
    assert_eq!(
        conclusion.progress,
        RestoreProgress::Planned {
            revision: 7,
            report: Box::new(planned)
        }
    );
    assert_eq!(conclusion.plan_domain, Some(domain("tenant_copy")));
    assert!(conclusion.progress.planned(7));
    assert!(
        !conclusion.progress.planned(8),
        "an edited draft is planned again before it applies"
    );
    assert_eq!(conclusion.progress.status(), "dry run planned");
    assert_eq!(conclusion.lines.len(), 1);
}

#[test]
fn a_restore_shows_its_applied_report_its_failed_step_or_its_refusal() {
    let applied = restore_report(RestoreMode::Apply, [RestoreStepOutcome::Applied; 3]);
    let conclusion = RestoreConclusion::of(
        restore_outcome(
            CommandDisposition::Completed {
                already_existed: false,
            },
            Some(applied.clone()),
        ),
        DRY_RUN,
        RestoreMode::Apply,
        1,
    );
    assert_eq!(
        conclusion.progress,
        RestoreProgress::Restored {
            report: Box::new(applied.clone())
        }
    );
    assert_eq!(conclusion.progress.result(), Some(&applied));
    assert_eq!(conclusion.progress.status(), "restored");

    let failed_step = restore_report(
        RestoreMode::Apply,
        [
            RestoreStepOutcome::Applied,
            RestoreStepOutcome::Applied,
            RestoreStepOutcome::Failed,
        ],
    );
    let conclusion = RestoreConclusion::of(
        restore_outcome(CommandDisposition::Failed, Some(failed_step.clone())),
        DRY_RUN,
        RestoreMode::Apply,
        1,
    );
    assert_eq!(conclusion.progress.status(), "restore failed");
    assert_eq!(conclusion.progress.result(), Some(&failed_step));
    assert_eq!(
        conclusion.progress.error().as_deref(),
        Some("the server's message")
    );

    let conclusion = RestoreConclusion::of(
        restore_outcome(CommandDisposition::Failed, None),
        DRY_RUN,
        RestoreMode::DryRun,
        1,
    );
    assert_eq!(conclusion.progress.status(), "restore refused");
    assert_eq!(conclusion.progress.result(), None);

    let conclusion = RestoreConclusion::of(
        RestoreEnd::Refused {
            failure: RestoreUploadFailure::DigestMismatch,
            message: "the bytes do not have the declared digest".to_string(),
        },
        DRY_RUN,
        RestoreMode::DryRun,
        1,
    );
    assert_eq!(
        conclusion.progress.error().as_deref(),
        Some("restore refused (DigestMismatch): the bytes do not have the declared digest")
    );
}

fn reply(disposition: RestoreDisposition) -> RestoreReply {
    RestoreReply {
        request_id: Some(RequestId::new(NonZeroU64::MIN)),
        disposition,
    }
}

#[test]
fn a_restore_reply_ends_the_restore_redirects_it_or_repeats_it() {
    let refused = reply(RestoreDisposition::UploadFailed {
        failure: RestoreUploadFailure::QuotaExceeded,
        message: "full".to_string(),
    });
    assert_eq!(
        reply_step(refused, &reference()).assured("a refusal is a reply"),
        ReplyStep::End(RestoreEnd::Refused {
            failure: RestoreUploadFailure::QuotaExceeded,
            message: "full".to_string()
        })
    );
    let completed = outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    assert_eq!(
        reply_step(
            reply(RestoreDisposition::Outcome(Box::new(completed.clone()))),
            &reference()
        )
        .assured("an outcome is a reply"),
        ReplyStep::End(RestoreEnd::Outcome(Box::new(completed)))
    );
    let console = url::Url::parse("http://127.0.0.1:47421/").assured("the test URL parses");
    let redirect = outcome(CommandDisposition::NotLeader(LeaderRedirect {
        leader: Some(LeaderEndpoints {
            node: ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            grpc_uri: None,
            web_console_uri: Some(console.clone()),
        }),
    }));
    assert_eq!(
        reply_step(
            reply(RestoreDisposition::Outcome(Box::new(redirect))),
            &reference()
        )
        .assured("a redirect is a reply"),
        ReplyStep::Redirect(console)
    );
    let unknown = outcome(CommandDisposition::OutcomeUnknown(
        UnknownOutcomeCause::StillApplying,
    ));
    assert_eq!(
        reply_step(
            reply(RestoreDisposition::Outcome(Box::new(unknown))),
            &reference()
        )
        .assured("an unknown outcome is a reply"),
        ReplyStep::Retry
    );
    let no_leader = outcome(CommandDisposition::NotLeader(LeaderRedirect {
        leader: None,
    }));
    assert_eq!(
        reply_step(
            reply(RestoreDisposition::Outcome(Box::new(no_leader))),
            &reference()
        )
        .assured("a redirect naming no leader is a reply"),
        ReplyStep::Retry
    );
}

#[test]
fn a_reply_to_another_request_or_restore_is_refused() {
    let completed = outcome(CommandDisposition::Completed {
        already_existed: false,
    });
    let other_request = RestoreReply {
        request_id: Some(RequestId::new(
            NonZeroU64::new(2).assured("two is non-zero"),
        )),
        disposition: RestoreDisposition::Outcome(Box::new(completed.clone())),
    };
    assert!(reply_step(other_request, &reference()).is_err());
    let other_reference = CommandExecutionReference::parse("0192d4e4-7b36-7c3e-9f00-000000000000")
        .assured("the test reference is a valid literal");
    assert!(
        reply_step(
            reply(RestoreDisposition::Outcome(Box::new(completed))),
            &other_reference
        )
        .is_err()
    );
}

#[test]
fn a_restore_transfer_reports_what_it_read_and_what_it_sent() {
    let reading = TransferProgress {
        phase: TransferPhase::Reading,
        done: 4,
        total: 34,
    };
    assert_eq!(reading.text(), "read 4 of 34 bytes");
    let uploading = TransferProgress {
        phase: TransferPhase::Uploading,
        done: 34,
        total: 34,
    };
    assert_eq!(uploading.text(), "uploaded 34 of 34 bytes");
    assert_eq!(
        RestoreProgress::AwaitingArchive {
            requested: "tenant.nvxb".to_string()
        }
        .status(),
        "choose the archive 'tenant.nvxb' to restore"
    );
}

fn dialog_signals() -> super::BackupSignals {
    super::BackupSignals::new(console_handles(), None)
}

/// The text of every line the dialog wrote to the terminal, oldest first.
fn terminal_texts(backups: super::BackupSignals) -> Vec<String> {
    use leptos::prelude::*;

    let history = backups.console.terminal_lines.get_untracked();
    let mut texts = Vec::new();
    for entry in history.into_lines() {
        texts.push(entry.line.text);
    }
    texts
}

fn console_handles() -> super::ConsoleHandles {
    super::ConsoleHandles {
        base_url: leptos::prelude::RwSignal::new(None),
        auth_token: leptos::prelude::RwSignal::new(None),
        terminal_lines: leptos::prelude::RwSignal::new(crate::TermLineHistory::default()),
        transaction_status: leptos::prelude::RwSignal::new(None),
    }
}

/// The markup the dialog renders for `backups` with the domains `domains` listed.
fn dialog_markup(backups: super::BackupSignals, domains: Vec<DomainName>) -> String {
    use leptos::prelude::*;

    let props = super::BackupDialogProps::builder()
        .backups(backups)
        .request_tx(RwSignal::new(None))
        .domain_names(Signal::derive(move || domains.clone()))
        .build();
    let view = super::BackupDialog(props);
    any_spawner::Executor::poll_local();
    view.to_html()
}

#[test]
fn the_backup_form_renders_its_options_canonical_statement_and_summary() {
    crate::initialize_test_executor();
    leptos::prelude::Owner::new().with(|| {
        use leptos::prelude::*;

        let backups = dialog_signals();
        backups.open_backup(Some(&domain("tenant")));
        let markup = dialog_markup(backups, vec![domain("tenant"), domain("tenant_other")]);
        assert!(markup.contains("Back up and restore"));
        assert!(markup.contains("backup-scope-cluster"));
        assert!(markup.contains("BACKUP DOMAIN tenant TO 'nervix-backup.nvxb';"));
        assert!(
            markup.contains("tenant_other"),
            "every listed domain is offered"
        );
        assert!(
            markup.contains("backup-timeout"),
            "a quiesced capture offers its timeout"
        );

        backups.backup.set(BackupProgress::Downloaded {
            summary: Box::new(summary(ARCHIVE)),
            file_name: "tenant.nvxb".to_string(),
        });
        backups
            .backup_draft
            .update(|draft| draft.destination = String::new());
        let markup = dialog_markup(backups, vec![domain("tenant")]);
        assert!(markup.contains("downloaded 'tenant.nvxb'"));
        assert!(markup.contains("domain tenant_other · cut without state"));
        assert!(
            markup.contains("Name the archive file"),
            "an invalid draft says why"
        );
        assert!(markup.contains("backup-progress"));

        backups.backup.set(BackupProgress::NotDownloaded {
            summary: Box::new(summary(ARCHIVE)),
            reason: "the download's connection failed".to_string(),
            retryable: true,
        });
        let markup = dialog_markup(backups, vec![domain("tenant")]);
        assert!(markup.contains("backup-retry-download"));
        assert!(markup.contains("backup-error"));
    });
}

#[test]
fn the_restore_form_renders_its_previews_plan_and_result() {
    crate::initialize_test_executor();
    leptos::prelude::Owner::new().with(|| {
        use leptos::prelude::*;

        let backups = dialog_signals();
        backups.tab.set(super::BackupTab::Restore);
        backups.open.set(true);
        let markup = dialog_markup(backups, Vec::new());
        assert!(
            markup.contains("Choose the archive to restore"),
            "no archive is chosen yet"
        );
        assert!(markup.contains("restore-dry-run"));

        backups.restore_draft.set(RestoreDraft {
            domain: "tenant".to_string(),
            target: "tenant_copy".to_string(),
            ..RestoreDraft::default()
        });
        backups.archive_name.set(Some("tenant.nvxb".to_string()));
        backups.restore.set(RestoreProgress::Planned {
            revision: backups.restore_revision.get_untracked(),
            report: Box::new(restore_report(
                RestoreMode::DryRun,
                [RestoreStepOutcome::Planned; 3],
            )),
        });
        backups.plan_domain.set(Some(domain("tenant_copy")));
        backups.restore_transfer.set(Some(TransferProgress {
            phase: TransferPhase::Uploading,
            done: 34,
            total: 34,
        }));
        let markup = dialog_markup(backups, Vec::new());
        assert!(markup.contains("FROM 'tenant.nvxb' DRY RUN;"));
        assert!(markup.contains("dry run planned"));
        assert!(markup.contains("uploaded 34 of 34 bytes"));
        assert!(markup.contains("create domain 'tenant_copy': planned"));
        assert!(markup.contains("restore-impact"));
        assert!(markup.contains("Planned model run of domain tenant_copy"));

        backups
            .restore_draft
            .update(|draft| draft.scope = RestoreScopeChoice::Cluster);
        backups.restore.set(RestoreProgress::Failed {
            reason: "restore failed at step".to_string(),
            report: Box::new(restore_report(
                RestoreMode::Apply,
                [
                    RestoreStepOutcome::Applied,
                    RestoreStepOutcome::Applied,
                    RestoreStepOutcome::Failed,
                ],
            )),
        });
        let markup = dialog_markup(backups, Vec::new());
        assert!(
            markup.contains("restore-existing-users"),
            "a cluster restore offers its user policy"
        );
        for policy in ["FAIL", "SKIP", "REPLACE"] {
            assert!(
                markup.contains(&format!("value=\"{policy}\"")),
                "the user policy {policy} is offered under its own name: {markup}"
            );
        }
        assert!(markup.contains("restore-result"));
        assert!(markup.contains("restore failed"));
    });
}

#[test]
fn a_typed_restore_opens_the_form_waiting_for_its_archive() {
    crate::initialize_test_executor();
    leptos::prelude::Owner::new().with(|| {
        use leptos::prelude::*;

        let backups = dialog_signals();
        let Statement::Restore(restore) =
            parsed("RESTORE DOMAIN tenant AS tenant_copy FROM '/backups/tenant.nvxb' RESUME;")
        else {
            panic!("a typed RESTORE parses into one");
        };
        backups.open_restore(&restore);
        assert!(backups.open.get_untracked());
        assert_eq!(backups.tab.get_untracked(), super::BackupTab::Restore);
        assert_eq!(
            backups.restore.get_untracked().status(),
            "choose the archive '/backups/tenant.nvxb' to restore"
        );
        assert_eq!(
            backups.restore_draft.get_untracked().lifecycle,
            RestoreLifecycle::Resume
        );
        let revision = backups.restore_revision.get_untracked();
        backups.clear();
        assert!(!backups.open.get_untracked());
        assert_eq!(backups.restore.get_untracked(), RestoreProgress::Idle);
        assert!(backups.restore_revision.get_untracked() > revision);
    });
}
