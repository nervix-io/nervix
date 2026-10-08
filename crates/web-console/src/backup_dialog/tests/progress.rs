//! Where the dialog's backup and restore stand as the operator acts: what each progress reports,
//! a backup or restore refused while one runs or while the session holds a transaction, a session
//! that refuses a backup, a completed backup the console holds no credentials to download, choosing
//! no archive, and how a streamed restore ended.

use leptos::prelude::*;
use meticulous::ResultExt as _;
use nervix_client_wire::CommandDisposition;
use nervix_models::{
    Backup, RestoreMode, RestoreStepOutcome, Statement, TransactionLifecycle, TransactionPosition,
    TransactionStatus,
};

use super::{
    ARCHIVE, DRY_RUN, dialog_signals, domain, outcome, parsed, reference, restore_outcome,
    restore_report, summary, terminal_texts,
};
use crate::{
    CommandPurpose, ConsoleRequest,
    backup_dialog::{
        BackupProgress, BackupSignals, RestoreProgress, transfer_done, transfer_total,
    },
    request_handoff::request_handoff,
};

const QUERY: &str = "BACKUP CLUSTER TO 'cluster.nvxb';";

fn backup_model() -> Backup {
    let Statement::Backup(backup) = parsed(QUERY) else {
        panic!("{QUERY} is a BACKUP");
    };
    backup
}

/// Starts the cluster backup on a fresh session and returns the requests it sent.
fn start_backup(backups: BackupSignals) -> crate::request_handoff::RequestReceiver {
    let (sender, receiver) = request_handoff();
    backups.start_backup(
        RwSignal::new(Some(sender)),
        QUERY.to_string(),
        None,
        &backup_model(),
    );
    receiver
}

#[test]
fn every_progress_reports_its_status_and_its_transfer() {
    assert_eq!(
        BackupProgress::Running.status(),
        "backing up: waiting for the backup's outcome"
    );
    let failed = BackupProgress::Failed {
        reason: "the leader refused the backup".to_string(),
    };
    assert_eq!(failed.status(), "backup failed");
    assert_eq!(
        failed.error().as_deref(),
        Some("the leader refused the backup")
    );
    let downloading = BackupProgress::Downloading {
        summary: Box::new(summary(ARCHIVE)),
        received: 0,
    };
    assert_eq!(downloading.summary(), Some(&summary(ARCHIVE)));

    assert_eq!(
        RestoreProgress::Streaming {
            mode: RestoreMode::DryRun
        }
        .status(),
        "dry run: streaming the archive to the leader"
    );
    assert_eq!(
        RestoreProgress::Streaming {
            mode: RestoreMode::Apply
        }
        .status(),
        "restoring: streaming the archive to the leader"
    );
    let unknown = RestoreProgress::Unknown {
        reference: reference(),
        reason: "the restore's connection failed".to_string(),
    };
    assert_eq!(
        unknown.status(),
        format!("the outcome of restore '{}' is unknown", reference())
    );
    assert_eq!(
        unknown.error().as_deref(),
        Some("the restore's connection failed")
    );

    assert_eq!(transfer_total(None), "0");
    assert_eq!(transfer_done(None), "0");
    assert_eq!(transfer_total(Some((3, 34))), "34");
    assert_eq!(transfer_done(Some((3, 34))), "3");
}

#[test]
fn a_second_backup_or_restore_is_refused_while_one_runs() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        backups.backup.set(BackupProgress::Running);
        let mut receiver = start_backup(backups);
        assert!(
            receiver.try_take().is_none(),
            "the second backup is not sent"
        );
        assert_eq!(backups.backup.get_untracked(), BackupProgress::Running);

        let streaming = RestoreProgress::Streaming {
            mode: RestoreMode::Apply,
        };
        backups.restore.set(streaming.clone());
        let Statement::Restore(restore) = parsed(DRY_RUN) else {
            panic!("a typed RESTORE parses into one");
        };
        backups.open_restore(&restore);
        backups.run_restore(RestoreMode::Apply);
        assert_eq!(backups.restore.get_untracked(), streaming);
        assert_eq!(
            terminal_texts(backups),
            vec![
                "error: a backup is already running in this console; wait for its download"
                    .to_string(),
                "error: a restore is already streaming in this console; wait for its outcome"
                    .to_string(),
            ]
        );
    });
}

#[test]
fn a_backup_or_restore_is_refused_while_the_session_holds_a_transaction() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        let open = TransactionStatus::new(
            "attached".to_string(),
            domain("tenant"),
            TransactionLifecycle::Open,
            TransactionPosition::new(0),
            0,
        )
        .assured("the test transaction has no applied operations");
        backups.console.transaction_status.set(Some(open));

        let mut receiver = start_backup(backups);
        assert!(receiver.try_take().is_none());
        let BackupProgress::Failed { reason } = backups.backup.get_untracked() else {
            panic!("a backup is refused inside a transaction");
        };
        assert!(reason.starts_with("BACKUP runs outside transactions"));

        backups.archive_name.set(Some("tenant.nvxb".to_string()));
        backups.run_restore(RestoreMode::DryRun);
        let RestoreProgress::Refused { reason } = backups.restore.get_untracked() else {
            panic!("a restore is refused inside a transaction");
        };
        assert!(reason.starts_with("RESTORE runs outside transactions"));
    });
}

#[test]
fn a_backup_the_session_no_longer_takes_fails_at_once() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        let (sender, receiver) = request_handoff();
        drop(receiver);
        backups.start_backup(
            RwSignal::new(Some(sender)),
            QUERY.to_string(),
            None,
            &backup_model(),
        );
        let BackupProgress::Failed { reason } = backups.backup.get_untracked() else {
            panic!("a backup the session refuses fails");
        };
        assert!(terminal_texts(backups).contains(&format!("error: {reason}")));
    });
}

#[test]
fn a_completed_backup_the_console_cannot_download_may_be_downloaded_again() {
    Owner::new().with(|| {
        // The dialog holds no credentials, so no download starts.
        let backups = dialog_signals();
        let mut receiver = start_backup(backups);
        let Some(ConsoleRequest::Command {
            purpose: CommandPurpose::Backup(command),
            ..
        }) = receiver.try_take()
        else {
            panic!("the backup is sent as a command");
        };
        let mut completed = outcome(CommandDisposition::Completed {
            already_existed: false,
        });
        completed.backup = Some(Box::new(summary(ARCHIVE)));
        backups.backup_outcome(&command, &completed);
        let BackupProgress::NotDownloaded {
            retryable, reason, ..
        } = backups.backup.get_untracked()
        else {
            panic!("a completed backup whose archive was not downloaded says so");
        };
        assert!(retryable);
        assert_eq!(
            reason,
            "the console holds no credentials to download the archive with"
        );

        let attempt = backups.backup_attempt.get_untracked();
        backups.download_again();
        assert!(
            backups.backup_attempt.get_untracked() > attempt,
            "a download started again is a new attempt, which fences the earlier one"
        );
        assert!(matches!(
            backups.backup.get_untracked(),
            BackupProgress::NotDownloaded {
                retryable: true,
                ..
            }
        ));

        // Nothing is downloaded again once the archive cannot be.
        backups.backup.set(BackupProgress::Failed {
            reason: "the backup failed".to_string(),
        });
        let attempt = backups.backup_attempt.get_untracked();
        backups.download_again();
        assert_eq!(backups.backup_attempt.get_untracked(), attempt);
    });
}

#[test]
fn choosing_no_archive_leaves_nothing_to_restore() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        let Statement::Restore(restore) = parsed(DRY_RUN) else {
            panic!("a typed RESTORE parses into one");
        };
        backups.open_restore(&restore);
        let revision = backups.restore_revision.get_untracked();
        backups.choose_archive(None);
        assert_eq!(backups.restore.get_untracked(), RestoreProgress::Idle);
        assert_eq!(backups.archive_name.get_untracked(), None);
        assert!(backups.restore_revision.get_untracked() > revision);

        backups.run_restore(RestoreMode::DryRun);
        assert_eq!(
            backups.restore.get_untracked(),
            RestoreProgress::Refused {
                reason: "Choose the archive to restore".to_string()
            }
        );

        // An archive the dialog names but the browser no longer holds starts nothing.
        backups.archive_name.set(Some("tenant.nvxb".to_string()));
        backups.restore.set(RestoreProgress::Idle);
        backups.run_restore(RestoreMode::DryRun);
        assert_eq!(backups.restore.get_untracked(), RestoreProgress::Idle);
    });
}

#[test]
fn a_streamed_restore_shows_how_it_ended_in_the_dialog_and_the_terminal() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        let revision = backups.restore_revision.get_untracked();
        let planned = restore_outcome(
            CommandDisposition::Completed {
                already_existed: false,
            },
            Some(restore_report(
                RestoreMode::DryRun,
                [RestoreStepOutcome::Planned; 3],
            )),
        );
        backups.restore_ended(planned, DRY_RUN, RestoreMode::DryRun, revision);
        assert!(backups.restore.get_untracked().planned(revision));
        assert_eq!(
            backups.plan_domain.get_untracked(),
            Some(domain("tenant_copy"))
        );
        assert!(!terminal_texts(backups).is_empty());

        // A dry run whose domains plan no models draws no impact.
        let unplanned = restore_outcome(
            CommandDisposition::Completed {
                already_existed: false,
            },
            Some(restore_report(
                RestoreMode::Apply,
                [RestoreStepOutcome::Planned; 3],
            )),
        );
        backups.restore_ended(unplanned, DRY_RUN, RestoreMode::DryRun, revision);
        assert_eq!(backups.plan_domain.get_untracked(), None);
    });
}
