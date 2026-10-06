//! The pending backup record in the tab's session storage, and the dialog flows that record,
//! resume and forget it, and that show how a download or a restore stream ended.

use error_stack::Report;
use leptos::prelude::*;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::CommandDisposition;
use nervix_models::{CommandExecutionReference, DomainName, Statement};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};

use super::{
    ARCHIVE, console_handles, dialog_signals, domain, outcome, reference, summary, terminal_texts,
};
use crate::{
    CommandPurpose, ConsoleRequest,
    backup_dialog::{
        BackupCommand, BackupProgress, BackupSignals, RestoreProgress,
        archive_download::DownloadError,
        browser::{RecordStorage, in_memory::MemoryStorage},
        pending_backup::PendingBackup,
        restore_stream::RestoreStreamError,
    },
    request_handoff::{RequestReceiver, request_handoff},
};

const QUERY: &str = "BACKUP DOMAIN tenant TO 'tenant.nvxb';";

fn pending(domain: Option<DomainName>) -> PendingBackup {
    PendingBackup {
        reference: reference(),
        query: QUERY.to_string(),
        domain,
    }
}

/// A dialog whose tab records its pending backup in `storage`.
struct RecordingDialog {
    backups: BackupSignals,
    storage: MemoryStorage,
}

impl RecordingDialog {
    fn new() -> Self {
        let storage = MemoryStorage::default();
        let recorded: Box<dyn RecordStorage> = Box::new(storage.clone());
        Self {
            backups: BackupSignals::new(console_handles(), Some(recorded)),
            storage,
        }
    }

    fn recorded(&self) -> Option<PendingBackup> {
        PendingBackup::recorded(&self.storage)
    }
}

/// A session the dialog sends on, and the requests it received.
fn session() -> (
    RwSignal<Option<crate::request_handoff::RequestSender>>,
    RequestReceiver,
) {
    let (sender, receiver) = request_handoff();
    (RwSignal::new(Some(sender)), receiver)
}

/// The backup command `receiver` took next, with the request it travelled in.
fn sent_backup(
    receiver: &mut RequestReceiver,
) -> (nervix_client_wire::CommandRequest, BackupCommand) {
    let Some(ConsoleRequest::Command {
        request,
        purpose: CommandPurpose::Backup(command),
    }) = receiver.try_take()
    else {
        panic!("the dialog sends its backup as a command on the session");
    };
    (request, command)
}

fn backup_model(query: &str) -> nervix_models::Backup {
    let Ok(ClientStatement::Server(Statement::Backup(backup))) = parse_client_statement(query)
    else {
        panic!("{query} is a BACKUP");
    };
    backup
}

#[test]
fn a_pending_backup_is_recorded_and_read_back_with_or_without_its_domain() {
    let storage = MemoryStorage::default();
    pending(Some(domain("tenant"))).record(&storage);
    assert_eq!(
        PendingBackup::recorded(&storage),
        Some(pending(Some(domain("tenant"))))
    );
    assert_eq!(storage.entries().len(), 3);

    // A later backup without a domain leaves no domain entry behind.
    pending(None).record(&storage);
    assert_eq!(PendingBackup::recorded(&storage), Some(pending(None)));
    assert_eq!(storage.entries().len(), 2);
}

#[test]
fn only_its_own_reference_forgets_a_pending_backup() {
    let storage = MemoryStorage::default();
    pending(None).record(&storage);
    let other = CommandExecutionReference::parse("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e45")
        .assured("the test reference is a valid literal");
    PendingBackup::forget(&storage, &other);
    assert_eq!(PendingBackup::recorded(&storage), Some(pending(None)));

    PendingBackup::forget(&storage, &reference());
    assert_eq!(PendingBackup::recorded(&storage), None);
    assert!(storage.entries().is_empty());
}

#[test]
fn a_record_that_does_not_read_back_is_forgotten() {
    let storage = MemoryStorage::default();
    storage.set("nervix.console.backup.query", QUERY);
    storage.set("nervix.console.backup.reference", "not a reference");
    assert_eq!(PendingBackup::recorded(&storage), None);
    assert!(storage.entries().is_empty());

    storage.set("nervix.console.backup.reference", reference().as_str());
    assert_eq!(
        PendingBackup::recorded(&storage),
        None,
        "the record has no query"
    );
    assert!(storage.entries().is_empty());
}

#[test]
fn a_backup_is_recorded_before_it_is_sent_and_forgotten_once_its_archive_is_saved() {
    Owner::new().with(|| {
        let dialog = RecordingDialog::new();
        let (request_tx, mut receiver) = session();
        dialog.backups.start_backup(
            request_tx,
            QUERY.to_string(),
            Some(domain("tenant")),
            &backup_model(QUERY),
        );
        let (request, command) = sent_backup(&mut receiver);
        assert_eq!(request.query, QUERY);
        assert_eq!(request.domain, Some(domain("tenant")));
        let recorded = dialog
            .recorded()
            .assured("the backup is recorded before it is sent");
        assert_eq!(recorded.reference, request.execution_reference);
        assert_eq!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::Running
        );

        // A download that may succeed again keeps the record, so a reload still resumes it.
        dialog.backups.download_ended(
            command.clone(),
            summary(ARCHIVE),
            Err(Report::new(DownloadError::Transport)),
        );
        assert!(dialog.recorded().is_some());
        assert!(matches!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::NotDownloaded {
                retryable: true,
                ..
            }
        ));

        dialog
            .backups
            .download_ended(command, summary(ARCHIVE), Ok(()));
        assert_eq!(dialog.recorded(), None);
        assert!(matches!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::Downloaded { .. }
        ));
        let texts = terminal_texts(dialog.backups);
        assert!(
            texts.contains(&"archive downloaded as 'tenant.nvxb'".to_string()),
            "{texts:?}"
        );
    });
}

#[test]
fn a_download_that_cannot_succeed_again_forgets_its_record() {
    Owner::new().with(|| {
        let dialog = RecordingDialog::new();
        let (request_tx, mut receiver) = session();
        dialog
            .backups
            .start_backup(request_tx, QUERY.to_string(), None, &backup_model(QUERY));
        let (_, command) = sent_backup(&mut receiver);
        dialog.backups.download_ended(
            command,
            summary(ARCHIVE),
            Err(Report::new(DownloadError::Mismatch)),
        );
        assert_eq!(dialog.recorded(), None);
        let BackupProgress::NotDownloaded {
            retryable, reason, ..
        } = dialog.backups.backup.get_untracked()
        else {
            panic!("a backup whose archive was not downloaded says so");
        };
        assert!(!retryable);
        assert_eq!(reason, DownloadError::Mismatch.to_string());
    });
}

#[test]
fn a_backup_that_fails_or_is_refused_forgets_its_record() {
    Owner::new().with(|| {
        let dialog = RecordingDialog::new();
        let (request_tx, mut receiver) = session();
        dialog
            .backups
            .start_backup(request_tx, QUERY.to_string(), None, &backup_model(QUERY));
        let (_, command) = sent_backup(&mut receiver);
        dialog
            .backups
            .backup_outcome(&command, &outcome(CommandDisposition::Failed));
        assert_eq!(dialog.recorded(), None);
        assert_eq!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::Failed {
                reason: "the server's message".to_string()
            }
        );

        let (request_tx, mut receiver) = session();
        dialog
            .backups
            .start_backup(request_tx, QUERY.to_string(), None, &backup_model(QUERY));
        let (_, command) = sent_backup(&mut receiver);
        dialog
            .backups
            .backup_refused(&command, "the session closed".to_string());
        assert_eq!(dialog.recorded(), None);

        // A backup sent without a session is refused at once, and nothing stays recorded.
        dialog.backups.start_backup(
            RwSignal::new(None),
            QUERY.to_string(),
            None,
            &backup_model(QUERY),
        );
        assert_eq!(dialog.recorded(), None);
        assert!(matches!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::Failed { .. }
        ));
    });
}

#[test]
fn a_reloaded_page_sends_the_recorded_backup_again_under_its_reference() {
    Owner::new().with(|| {
        let dialog = RecordingDialog::new();
        pending(Some(domain("tenant"))).record(&dialog.storage);
        let (request_tx, mut receiver) = session();
        dialog.backups.resume_pending(request_tx);
        let (request, command) = sent_backup(&mut receiver);
        assert_eq!(request.execution_reference, reference());
        assert_eq!(request.query, QUERY);
        assert_eq!(request.domain, Some(domain("tenant")));
        assert_eq!(command.file_name, "tenant.nvxb");
        assert_eq!(
            dialog.backups.backup.get_untracked(),
            BackupProgress::Resuming {
                reference: reference()
            }
        );
        let texts = terminal_texts(dialog.backups);
        assert!(
            texts.contains(&format!("resuming backup '{}': {QUERY}", reference())),
            "{texts:?}"
        );

        // A record whose query is no backup is forgotten, and nothing is sent.
        let dialog = RecordingDialog::new();
        PendingBackup {
            query: "LIST DOMAINS;".to_string(),
            ..pending(None)
        }
        .record(&dialog.storage);
        let (request_tx, mut receiver) = session();
        dialog.backups.resume_pending(request_tx);
        assert!(receiver.try_take().is_none());
        assert!(dialog.storage.entries().is_empty());

        // Signing in as another identity forgets the record.
        let dialog = RecordingDialog::new();
        pending(None).record(&dialog.storage);
        dialog.backups.forget_recorded_backup();
        assert!(dialog.storage.entries().is_empty());
    });
}

#[test]
fn a_dialog_without_session_storage_backs_up_and_resumes_nothing() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        let (request_tx, mut receiver) = session();
        backups.resume_pending(request_tx);
        assert!(receiver.try_take().is_none());

        let (request_tx, mut receiver) = session();
        backups.start_backup(request_tx, QUERY.to_string(), None, &backup_model(QUERY));
        let (request, _) = sent_backup(&mut receiver);
        assert_eq!(request.query, QUERY);
    });
}

#[test]
fn a_restore_stream_that_failed_shows_whether_its_outcome_is_unknown() {
    Owner::new().with(|| {
        let backups = dialog_signals();
        backups.restore_failed(reference(), &Report::new(RestoreStreamError::NoReply));
        let RestoreProgress::Unknown {
            reference: unknown,
            reason,
        } = backups.restore.get_untracked()
        else {
            panic!("a restore that may have been admitted has an unknown outcome");
        };
        assert_eq!(unknown, reference());
        assert!(reason.ends_with(&format!(
            "the outcome of restore '{}' is unknown",
            reference()
        )));

        backups.restore_failed(reference(), &Report::new(RestoreStreamError::ReadArchive));
        assert_eq!(
            backups.restore.get_untracked(),
            RestoreProgress::Refused {
                reason: RestoreStreamError::ReadArchive.to_string()
            }
        );
        assert_eq!(terminal_texts(backups).len(), 2);
    });
}
