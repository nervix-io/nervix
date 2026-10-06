//! Taking a backup as a browser download, and restoring one from an archive the operator chooses.
//!
//! Layer: edges.
//!
//! - **Owns.** The backup and restore drafts and the Models they lower to, where the one backup
//!   and the one restore the dialog shows stand, the browser download and restore streams they
//!   run, the backup a reload resumes, and the summaries and reports the dialog renders.
//! - **Depends on.** The console request dispatcher, which sends `BACKUP` as the command it is and
//!   renders command outcomes; the client wire download and restore calls and their WebSocket
//!   codecs; the impact report view; and the vocabulary's backup and restore Models.
//! - **Must not know.** How the server assembles, retains or restores an archive, or anything an
//!   archive holds beyond the summary a backup or a restore reports.
//!
//! The REPL and the dialog are one console: a `BACKUP` typed at the prompt runs here as one the
//! form submitted, a typed `RESTORE` opens the restore form with its options, and both travel on
//! the session's dispatcher and follow its leader redirect. A backup is sent as a command under an
//! execution reference the tab records before sending, so a reload sends it again under that
//! reference, recovers its outcome and downloads its archive. A restore is streamed beside the
//! session, never as a command and never while the session holds a transaction.

mod archive_download;
mod backup_draft;
mod browser;
mod pending_backup;
mod reports;
mod restore_draft;
mod restore_stream;

#[cfg(test)]
mod tests;

use error_stack::Report;
use leptos::{ev, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{CommandDisposition, CommandOutcome, CommandRequest};
use nervix_models::{
    Backup, BackupArchiveSummary, BackupResources, BackupScope, CommandExecutionReference,
    DomainName, ExistingUserPolicy, Restore, RestoreLifecycle, RestoreMode, RestoreReport,
    RestoreState, Statement, TransactionStatus,
};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statement};
use nervix_primitives::unmodeled::futures::{AbortHandle, Abortable};
use wasm_bindgen::JsCast as _;

pub(crate) use self::browser::boxed_session_storage;
use self::{
    archive_download::{ArchiveDownload, DownloadError},
    backup_draft::{BackupDraft, BackupScopeChoice, CaptureChoice},
    browser::{PageBrowser, RecordStorage},
    pending_backup::PendingBackup,
    reports::{restore_report_lines, summary_lines},
    restore_draft::{RestoreDraft, RestoreDraftError, RestoreScopeChoice},
    restore_stream::{RestoreEnd, RestoreStreamError, RestoreUpload, measure_archive},
};
use crate::{
    CommandPurpose, ConsoleRequest, SESSION_UNAVAILABLE, TermLine, TermLineHistory,
    command_execution_reference, command_outcome_lines, request_handoff::RequestSender,
    transaction_inspector::ImpactReportView, transaction_is_active,
};

/// The two forms of the dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackupTab {
    Backup,
    Restore,
}

impl BackupTab {
    fn data_tab(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Restore => "restore",
        }
    }
}

/// The backup a command carries through the dispatcher: which of the dialog's backups it is, and
/// what its archive downloads as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackupCommand {
    attempt: u64,
    reference: CommandExecutionReference,
    file_name: String,
}

/// What the outcome of a backup command does to the dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackupOutcomeEffect {
    /// The outcome answers another backup than the one the dialog shows, as one sent before the
    /// console's credentials changed; only the terminal shows it.
    Stale,
    /// The backup completed, and its archive downloads.
    Download(Box<BackupArchiveSummary>),
    /// The backup failed, or its outcome can no longer be recovered.
    Failed(String),
}

impl BackupCommand {
    /// What `outcome` does to a dialog showing the backup `current`.
    pub(crate) fn outcome_effect(
        &self,
        current: u64,
        outcome: &CommandOutcome,
    ) -> BackupOutcomeEffect {
        if self.attempt != current {
            return BackupOutcomeEffect::Stale;
        }
        match &outcome.disposition {
            CommandDisposition::Completed { .. } => match &outcome.backup {
                Some(summary) => BackupOutcomeEffect::Download(summary.clone()),
                None => BackupOutcomeEffect::Failed("the backup reported no archive".to_string()),
            },
            CommandDisposition::ExecutionReferenceExpired => BackupOutcomeEffect::Failed(format!(
                "the outcome of backup '{}' can no longer be recovered: its execution reference \
                 expired",
                self.reference
            )),
            CommandDisposition::Failed
            | CommandDisposition::NotLeader(_)
            | CommandDisposition::TransactionDetached { .. }
            | CommandDisposition::TransactionTakenOver { .. }
            | CommandDisposition::OutcomeUnknown(_)
            | CommandDisposition::ExecutionReferenceConflict(_)
            | CommandDisposition::PreviewStale { .. } => {
                BackupOutcomeEffect::Failed(outcome.message.clone())
            }
        }
    }
}

/// Where the dialog's backup stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackupProgress {
    Idle,
    /// The backup was sent; its outcome has not arrived.
    Running,
    /// A reload sent the backup again under its reference to recover its outcome.
    Resuming {
        reference: CommandExecutionReference,
    },
    /// The backup completed, and its archive downloads.
    Downloading {
        summary: Box<BackupArchiveSummary>,
        received: u64,
    },
    /// The verified archive was handed to the browser.
    Downloaded {
        summary: Box<BackupArchiveSummary>,
        file_name: String,
    },
    /// The backup itself failed or was refused.
    Failed {
        reason: String,
    },
    /// The backup completed, but its archive was not downloaded. `retryable` says the server
    /// still retains it, so it may be downloaded again.
    NotDownloaded {
        summary: Box<BackupArchiveSummary>,
        reason: String,
        retryable: bool,
    },
}

impl BackupProgress {
    fn is_active(&self) -> bool {
        match self {
            Self::Running | Self::Resuming { .. } | Self::Downloading { .. } => true,
            Self::Idle
            | Self::Downloaded { .. }
            | Self::Failed { .. }
            | Self::NotDownloaded { .. } => false,
        }
    }

    fn status(&self) -> String {
        match self {
            Self::Idle => String::new(),
            Self::Running => "backing up: waiting for the backup's outcome".to_string(),
            Self::Resuming { reference } => format!("resuming backup '{reference}'"),
            Self::Downloading { summary, received } => {
                format!("downloading {received} of {} bytes", summary.total_bytes)
            }
            Self::Downloaded { summary, file_name } => format!(
                "downloaded '{file_name}': {} bytes, BLAKE3 {}",
                summary.total_bytes, summary.digest
            ),
            Self::Failed { .. } => "backup failed".to_string(),
            Self::NotDownloaded { .. } => {
                "the backup completed, but its archive was not downloaded".to_string()
            }
        }
    }

    fn error(&self) -> Option<String> {
        match self {
            Self::Failed { reason } | Self::NotDownloaded { reason, .. } => Some(reason.clone()),
            Self::Idle
            | Self::Running
            | Self::Resuming { .. }
            | Self::Downloading { .. }
            | Self::Downloaded { .. } => None,
        }
    }

    fn summary(&self) -> Option<&BackupArchiveSummary> {
        match self {
            Self::Downloading { summary, .. }
            | Self::Downloaded { summary, .. }
            | Self::NotDownloaded { summary, .. } => Some(summary),
            Self::Idle | Self::Running | Self::Resuming { .. } | Self::Failed { .. } => None,
        }
    }

    /// The bytes received of the archive, and its size, while it downloads or once it did.
    fn transfer(&self) -> Option<(u64, u64)> {
        match self {
            Self::Downloading { summary, received } => Some((*received, summary.total_bytes.get())),
            Self::Downloaded { summary, .. } => {
                Some((summary.total_bytes.get(), summary.total_bytes.get()))
            }
            Self::Idle
            | Self::Running
            | Self::Resuming { .. }
            | Self::Failed { .. }
            | Self::NotDownloaded { .. } => None,
        }
    }
}

/// What a restore is reading or sending of its archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferPhase {
    /// The archive is read once to measure and digest it.
    Reading,
    /// The archive streams to the leader.
    Uploading,
}

/// How far a restore's transfer of its archive has come.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransferProgress {
    pub(crate) phase: TransferPhase,
    pub(crate) done: u64,
    pub(crate) total: u64,
}

impl TransferProgress {
    fn text(self) -> String {
        match self.phase {
            TransferPhase::Reading => format!("read {} of {} bytes", self.done, self.total),
            TransferPhase::Uploading => format!("uploaded {} of {} bytes", self.done, self.total),
        }
    }
}

/// Where the dialog's restore stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestoreProgress {
    Idle,
    /// A typed `RESTORE` waits for the operator to choose the archive it names.
    AwaitingArchive {
        requested: String,
    },
    /// The archive is read and streamed in `mode`, or its outcome is awaited.
    Streaming {
        mode: RestoreMode,
    },
    /// A dry run planned the restore of the draft revision it names.
    Planned {
        revision: u64,
        report: Box<RestoreReport>,
    },
    /// The restore applied.
    Restored {
        report: Box<RestoreReport>,
    },
    /// The restore was refused before it changed anything.
    Refused {
        reason: String,
    },
    /// The restore failed at a step; the steps before it stay applied.
    Failed {
        reason: String,
        report: Box<RestoreReport>,
    },
    /// The console could not learn the outcome of the restore its reference names.
    Unknown {
        reference: CommandExecutionReference,
        reason: String,
    },
}

impl RestoreProgress {
    fn is_active(&self) -> bool {
        matches!(self, Self::Streaming { .. })
    }

    fn status(&self) -> String {
        match self {
            Self::Idle => String::new(),
            Self::AwaitingArchive { requested } => {
                format!("choose the archive '{requested}' to restore")
            }
            Self::Streaming {
                mode: RestoreMode::DryRun,
            } => "dry run: streaming the archive to the leader".to_string(),
            Self::Streaming {
                mode: RestoreMode::Apply,
            } => "restoring: streaming the archive to the leader".to_string(),
            Self::Planned { .. } => "dry run planned".to_string(),
            Self::Restored { .. } => "restored".to_string(),
            Self::Refused { .. } => "restore refused".to_string(),
            Self::Failed { .. } => "restore failed".to_string(),
            Self::Unknown { reference, .. } => {
                format!("the outcome of restore '{reference}' is unknown")
            }
        }
    }

    fn error(&self) -> Option<String> {
        match self {
            Self::Refused { reason }
            | Self::Failed { reason, .. }
            | Self::Unknown { reason, .. } => Some(reason.clone()),
            Self::Idle
            | Self::AwaitingArchive { .. }
            | Self::Streaming { .. }
            | Self::Planned { .. }
            | Self::Restored { .. } => None,
        }
    }

    /// The report of a restore that applied or failed at a step, which the result pane renders.
    fn result(&self) -> Option<&RestoreReport> {
        match self {
            Self::Restored { report } | Self::Failed { report, .. } => Some(report),
            Self::Idle
            | Self::AwaitingArchive { .. }
            | Self::Streaming { .. }
            | Self::Planned { .. }
            | Self::Refused { .. }
            | Self::Unknown { .. } => None,
        }
    }

    fn plan(&self) -> Option<&RestoreReport> {
        match self {
            Self::Planned { report, .. } => Some(report),
            Self::Idle
            | Self::AwaitingArchive { .. }
            | Self::Streaming { .. }
            | Self::Restored { .. }
            | Self::Refused { .. }
            | Self::Failed { .. }
            | Self::Unknown { .. } => None,
        }
    }

    /// Whether a dry run of draft revision `revision` planned the restore, which it may now apply.
    fn planned(&self, revision: u64) -> bool {
        matches!(self, Self::Planned { revision: planned, .. } if *planned == revision)
    }
}

/// The parts of the console the dialog drives.
#[derive(Clone, Copy)]
pub(crate) struct ConsoleHandles {
    /// The console the session currently talks to: the leader's, once the session followed it.
    pub(crate) base_url: RwSignal<Option<String>>,
    pub(crate) auth_token: RwSignal<Option<String>>,
    pub(crate) terminal_lines: RwSignal<TermLineHistory>,
    pub(crate) transaction_status: RwSignal<Option<TransactionStatus>>,
}

/// The dialog's state, shared by its forms, the REPL and the dispatcher.
#[derive(Clone, Copy)]
pub(crate) struct BackupSignals {
    pub(crate) open: RwSignal<bool>,
    pub(crate) tab: RwSignal<BackupTab>,
    pub(crate) backup_draft: RwSignal<BackupDraft>,
    pub(crate) backup: RwSignal<BackupProgress>,
    /// The backup the dialog shows; an outcome or a download of an earlier one is dropped.
    backup_attempt: RwSignal<u64>,
    pub(crate) restore_draft: RwSignal<RestoreDraft>,
    pub(crate) restore: RwSignal<RestoreProgress>,
    restore_transfer: RwSignal<Option<TransferProgress>>,
    /// The restore the dialog shows; the outcome of an earlier one is dropped.
    restore_attempt: RwSignal<u64>,
    /// Counts edits of the restore draft and its archive. A dry run plans one revision, and only
    /// that revision may be applied.
    restore_revision: RwSignal<u64>,
    /// The archive file the restore reads, which only the browser can hold.
    archive: StoredValue<Option<web_sys::File>, LocalStorage>,
    archive_name: RwSignal<Option<String>>,
    /// The restored domain whose planned model run the impact view draws.
    plan_domain: RwSignal<Option<DomainName>>,
    /// The download of a completed backup's archive the dialog last ran, which it may run again.
    download: RwSignal<Option<BackupCommand>>,
    download_abort: RwSignal<Option<AbortHandle>>,
    restore_abort: RwSignal<Option<AbortHandle>>,
    /// Where the tab records the backup a reload resumes, when the browser offers a place.
    record_storage: StoredValue<Option<Box<dyn RecordStorage>>, LocalStorage>,
    console: ConsoleHandles,
}

/// The name the browser saves an archive under: the last component of the file the statement
/// names, since a browser chooses the directory itself.
fn download_file_name(destination: &str) -> String {
    match destination.rsplit(['/', '\\']).next() {
        Some(name) if !name.is_empty() => name.to_string(),
        Some(_) | None => destination.to_string(),
    }
}

/// Advances one of the dialog's counters and returns its new value.
fn advance(counter: RwSignal<u64>) -> u64 {
    let next = counter
        .get_untracked()
        .checked_add(1)
        .assured("a console page cannot start or edit 2^64 backups or restores");
    counter.set(next);
    next
}

impl BackupSignals {
    pub(crate) fn new(
        console: ConsoleHandles,
        record_storage: Option<Box<dyn RecordStorage>>,
    ) -> Self {
        Self {
            open: RwSignal::new(false),
            tab: RwSignal::new(BackupTab::Backup),
            backup_draft: RwSignal::new(BackupDraft::for_domain(None)),
            backup: RwSignal::new(BackupProgress::Idle),
            backup_attempt: RwSignal::new(0),
            restore_draft: RwSignal::new(RestoreDraft::default()),
            restore: RwSignal::new(RestoreProgress::Idle),
            restore_transfer: RwSignal::new(None),
            restore_attempt: RwSignal::new(0),
            restore_revision: RwSignal::new(0),
            archive: StoredValue::new_local(None),
            archive_name: RwSignal::new(None),
            plan_domain: RwSignal::new(None),
            download: RwSignal::new(None),
            download_abort: RwSignal::new(None),
            restore_abort: RwSignal::new(None),
            record_storage: StoredValue::new_local(record_storage),
            console,
        }
    }

    /// Opens the dialog on the backup form, backing up the active domain unless a backup was
    /// already drafted.
    pub(crate) fn open_backup(self, active_domain: Option<&DomainName>) {
        if !self.backup.with_untracked(BackupProgress::is_active)
            && self
                .backup_draft
                .with_untracked(|draft| draft.domain.is_empty())
        {
            self.backup_draft.update(|draft| {
                if let Some(domain) = active_domain {
                    draft.scope = BackupScopeChoice::Domain;
                    draft.domain = domain.to_string();
                }
            });
        }
        self.tab.set(BackupTab::Backup);
        self.open.set(true);
    }

    /// Forgets everything of a previous identity the dialog shows: its drafts, and its backup and
    /// restore, whose late outcomes and downloads no longer reach the dialog.
    pub(crate) fn clear(self) {
        if let Some(abort) = self.download_abort.get_untracked() {
            abort.abort();
        }
        if let Some(abort) = self.restore_abort.get_untracked() {
            abort.abort();
        }
        self.open.set(false);
        self.tab.set(BackupTab::Backup);
        self.backup_draft.set(BackupDraft::for_domain(None));
        self.backup.set(BackupProgress::Idle);
        advance(self.backup_attempt);
        self.restore_draft.set(RestoreDraft::default());
        self.restore.set(RestoreProgress::Idle);
        self.restore_transfer.set(None);
        advance(self.restore_attempt);
        self.archive.set_value(None);
        self.archive_name.set(None);
        self.plan_domain.set(None);
        self.download.set(None);
        self.download_abort.set(None);
        self.restore_abort.set(None);
        self.edit_restore();
    }

    /// Forgets the backup this tab recorded for a reload to resume: it belongs to an identity that
    /// no longer uses the console.
    pub(crate) fn forget_recorded_backup(self) {
        self.with_record_storage(PendingBackup::forget_any);
    }

    /// Applies `apply` to the storage the tab records its pending backup in, when it has one.
    fn with_record_storage(self, apply: impl FnOnce(&dyn RecordStorage)) {
        self.record_storage.with_value(|storage| {
            if let Some(storage) = storage {
                apply(storage.as_ref());
            }
        });
    }

    /// Forgets the pending backup `reference` names, when the tab still records it.
    fn forget_pending(self, reference: &CommandExecutionReference) {
        self.with_record_storage(|storage| PendingBackup::forget(storage, reference));
    }

    /// The backup a previous page of this tab recorded and did not download.
    fn recorded_backup(self) -> Option<PendingBackup> {
        self.record_storage.with_value(|storage| {
            let storage = storage.as_deref()?;
            PendingBackup::recorded(storage)
        })
    }

    fn line(self, line: TermLine) {
        self.console.terminal_lines.update(|lines| lines.push(line));
    }

    fn holds_transaction(self) -> bool {
        self.console
            .transaction_status
            .with_untracked(|status| transaction_is_active(status.as_ref()))
    }

    /// Sends `backup`, written as `query` and sent for `domain`, under a new execution reference,
    /// and downloads its archive once it completes. The tab records the backup before it is sent,
    /// so a reload recovers its outcome instead of running it again.
    pub(crate) fn start_backup(
        self,
        request_tx: RwSignal<Option<RequestSender>>,
        query: String,
        domain: Option<DomainName>,
        backup: &Backup,
    ) {
        self.tab.set(BackupTab::Backup);
        self.open.set(true);
        if self.backup.with_untracked(BackupProgress::is_active) {
            self.line(TermLine::error(
                "a backup is already running in this console; wait for its download",
            ));
            return;
        }
        if self.holds_transaction() {
            let reason = "BACKUP runs outside transactions; commit or revert the session's \
                          transaction first"
                .to_string();
            self.line(TermLine::error(reason.clone()));
            self.backup.set(BackupProgress::Failed { reason });
            return;
        }
        let pending = PendingBackup {
            reference: command_execution_reference(),
            query,
            domain,
        };
        self.with_record_storage(|storage| pending.record(storage));
        self.backup.set(BackupProgress::Running);
        self.send_backup(request_tx, pending, &backup.destination);
    }

    /// Sends the backup this tab recorded before a reload again under its reference, which
    /// recovers its outcome, and downloads its archive.
    pub(crate) fn resume_pending(self, request_tx: RwSignal<Option<RequestSender>>) {
        let Some(pending) = self.recorded_backup() else {
            return;
        };
        let Ok(ClientStatement::Server(Statement::Backup(backup))) =
            parse_client_statement(&pending.query)
        else {
            self.forget_pending(&pending.reference);
            return;
        };
        self.line(TermLine::info(format!(
            "resuming backup '{}': {}",
            pending.reference, pending.query
        )));
        self.tab.set(BackupTab::Backup);
        self.open.set(true);
        self.backup.set(BackupProgress::Resuming {
            reference: pending.reference.clone(),
        });
        self.send_backup(request_tx, pending, &backup.destination);
    }

    fn send_backup(
        self,
        request_tx: RwSignal<Option<RequestSender>>,
        pending: PendingBackup,
        destination: &str,
    ) {
        let command = BackupCommand {
            attempt: advance(self.backup_attempt),
            reference: pending.reference.clone(),
            file_name: download_file_name(destination),
        };
        let request = ConsoleRequest::Command {
            request: CommandRequest {
                query: pending.query,
                domain: pending.domain,
                execution_reference: pending.reference.clone(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::Backup(command),
        };
        let reason = match request_tx.get_untracked() {
            Some(request_tx) => match request_tx.send(request) {
                Ok(()) => return,
                Err(refusal) => refusal.current_context().to_string(),
            },
            None => SESSION_UNAVAILABLE.to_string(),
        };
        self.forget_pending(&pending.reference);
        self.line(TermLine::error(reason.clone()));
        self.backup.set(BackupProgress::Failed { reason });
    }

    /// Takes the outcome of the backup `command`. A completed backup downloads its archive.
    pub(crate) fn backup_outcome(self, command: &BackupCommand, outcome: &CommandOutcome) {
        match command.outcome_effect(self.backup_attempt.get_untracked(), outcome) {
            BackupOutcomeEffect::Stale => {}
            BackupOutcomeEffect::Download(summary) => self.download(command.clone(), *summary),
            BackupOutcomeEffect::Failed(reason) => {
                self.forget_pending(&command.reference);
                self.backup.set(BackupProgress::Failed { reason });
            }
        }
    }

    /// Takes the reason the backup `command` got no usable reply.
    pub(crate) fn backup_refused(self, command: &BackupCommand, reason: String) {
        if self.backup_attempt.get_untracked() != command.attempt {
            return;
        }
        self.forget_pending(&command.reference);
        self.backup.set(BackupProgress::Failed { reason });
    }

    /// Downloads the archive the backup `command` assembled, from the console the session talks
    /// to, which reported the backup.
    fn download(self, command: BackupCommand, summary: BackupArchiveSummary) {
        let attempt = command.attempt;
        self.download.set(Some(command.clone()));
        self.backup.set(BackupProgress::Downloading {
            summary: Box::new(summary.clone()),
            received: 0,
        });
        let Some(auth_token) = self.console.auth_token.get_untracked() else {
            self.backup.set(BackupProgress::NotDownloaded {
                summary: Box::new(summary),
                reason: "the console holds no credentials to download the archive with".to_string(),
                retryable: true,
            });
            return;
        };
        let download = ArchiveDownload {
            reference: command.reference.clone(),
            summary: summary.clone(),
            base_url: self.console.base_url.get_untracked(),
            auth_token,
            file_name: command.file_name.clone(),
        };
        let backup = self.backup;
        let backup_attempt = self.backup_attempt;
        let progress = move |received: u64| {
            if backup_attempt.get_untracked() != attempt {
                return;
            }
            backup.update(|progress| {
                if let BackupProgress::Downloading {
                    received: current, ..
                } = progress
                {
                    *current = received;
                }
            });
        };
        let (abort, registration) = AbortHandle::new_pair();
        if let Some(previous) = self.download_abort.get_untracked() {
            previous.abort();
        }
        self.download_abort.set(Some(abort));
        wasm_bindgen_futures::spawn_local(async move {
            let running = download.run(&PageBrowser, progress);
            let Ok(downloaded) = Abortable::new(running, registration).await else {
                return;
            };
            self.download_ended(command, summary, downloaded);
        });
    }

    /// Shows how the download of the backup `command` ended, unless the dialog shows another
    /// backup by now.
    fn download_ended(
        self,
        command: BackupCommand,
        summary: BackupArchiveSummary,
        downloaded: Result<(), Report<DownloadError>>,
    ) {
        if self.backup_attempt.get_untracked() != command.attempt {
            return;
        }
        self.download_abort.set(None);
        let failure = match downloaded {
            Ok(()) => {
                self.forget_pending(&command.reference);
                self.line(TermLine::info(format!(
                    "archive downloaded as '{}'",
                    command.file_name
                )));
                self.backup.set(BackupProgress::Downloaded {
                    summary: Box::new(summary),
                    file_name: command.file_name,
                });
                return;
            }
            Err(failure) => failure,
        };
        // A download that may succeed again keeps the record, so a reload still resumes it.
        let retryable = failure.current_context().is_retryable();
        if !retryable {
            self.forget_pending(&command.reference);
        }
        let reason = failure.current_context().to_string();
        self.line(TermLine::error(format!(
            "backup '{}' completed, but its archive was not downloaded: {reason}",
            command.reference
        )));
        self.backup.set(BackupProgress::NotDownloaded {
            summary: Box::new(summary),
            reason,
            retryable,
        });
    }

    /// Downloads the archive of a completed backup again, from its first byte, while the server
    /// still retains it.
    fn download_again(self) {
        let progress = self.backup.get_untracked();
        let BackupProgress::NotDownloaded {
            summary,
            retryable: true,
            ..
        } = progress
        else {
            return;
        };
        let Some(previous) = self.download.get_untracked() else {
            return;
        };
        let command = BackupCommand {
            attempt: advance(self.backup_attempt),
            ..previous
        };
        self.download(command, *summary);
    }

    /// Opens the restore form with the options of a typed `RESTORE`, whose archive the operator
    /// chooses: the browser cannot read the path the statement names.
    pub(crate) fn open_restore(self, restore: &Restore) {
        self.tab.set(BackupTab::Restore);
        self.open.set(true);
        if self.restore.with_untracked(RestoreProgress::is_active) {
            self.line(TermLine::error(
                "a restore is already streaming in this console; wait for its outcome",
            ));
            return;
        }
        self.restore_draft.set(RestoreDraft::of_model(restore));
        self.archive.set_value(None);
        self.archive_name.set(None);
        self.restore_transfer.set(None);
        self.restore.set(RestoreProgress::AwaitingArchive {
            requested: restore.source.clone(),
        });
        self.edit_restore();
    }

    fn edit_restore(self) {
        advance(self.restore_revision);
    }

    fn choose_archive(self, file: Option<web_sys::File>) {
        let name = file.as_ref().map(web_sys::File::name);
        self.archive.set_value(file);
        self.archive_name.set(name);
        if let RestoreProgress::AwaitingArchive { .. } = self.restore.get_untracked() {
            self.restore.set(RestoreProgress::Idle);
        }
        self.edit_restore();
    }

    /// The restore the draft and the chosen archive lower to in `mode`.
    fn restore_model(self, mode: RestoreMode) -> Result<Restore, Report<RestoreDraftError>> {
        let name = self.archive_name.get_untracked();
        self.restore_draft
            .with_untracked(|draft| draft.model(name.as_deref(), mode))
    }

    /// Streams the restore the draft lowers to in `mode`, under a new execution reference.
    fn run_restore(self, mode: RestoreMode) {
        if self.restore.with_untracked(RestoreProgress::is_active) {
            return;
        }
        if self.holds_transaction() {
            let reason = "RESTORE runs outside transactions; commit or revert the session's \
                          transaction first"
                .to_string();
            self.line(TermLine::error(reason.clone()));
            self.restore.set(RestoreProgress::Refused { reason });
            return;
        }
        let restore = match self.restore_model(mode) {
            Ok(restore) => restore,
            Err(error) => {
                self.restore.set(RestoreProgress::Refused {
                    reason: error.current_context().to_string(),
                });
                return;
            }
        };
        let Some(file) = self.archive.get_value() else {
            return;
        };
        let Some(auth_token) = self.console.auth_token.get_untracked() else {
            self.restore.set(RestoreProgress::Refused {
                reason: "the console holds no credentials to restore with".to_string(),
            });
            return;
        };
        let statement = restore.to_canonical_nspl();
        let reference = command_execution_reference();
        let attempt = advance(self.restore_attempt);
        let revision = self.restore_revision.get_untracked();
        self.line(TermLine::prompt(statement.clone(), None));
        self.restore.set(RestoreProgress::Streaming { mode });
        self.plan_domain.set(None);
        self.restore_transfer.set(None);
        let base_url = self.console.base_url.get_untracked();
        let (abort, registration) = AbortHandle::new_pair();
        if let Some(previous) = self.restore_abort.get_untracked() {
            previous.abort();
        }
        self.restore_abort.set(Some(abort));
        let upload_reference = reference.clone();
        let upload_statement = statement.clone();
        let streamed = async move {
            let reading = move |done: u64, total: u64| {
                self.transfer(attempt, TransferPhase::Reading, done, total)
            };
            let archive = measure_archive(&file, reading).await?;
            let upload = RestoreUpload {
                reference: upload_reference,
                statement: upload_statement,
                archive,
                file,
                base_url,
                auth_token,
            };
            let uploading = move |done: u64, total: u64| {
                self.transfer(attempt, TransferPhase::Uploading, done, total);
            };
            upload.run(&PageBrowser, uploading).await
        };
        wasm_bindgen_futures::spawn_local(async move {
            let Ok(ended) = Abortable::new(streamed, registration).await else {
                return;
            };
            if self.restore_attempt.get_untracked() != attempt {
                return;
            }
            self.restore_abort.set(None);
            match ended {
                Ok(end) => self.restore_ended(end, &statement, mode, revision),
                Err(failure) => self.restore_failed(reference, &failure),
            }
        });
    }

    /// Shows a restore under `reference` whose stream ended in `failure`: its outcome is unknown
    /// when the leader may have admitted it, and otherwise nothing changed.
    fn restore_failed(
        self,
        reference: CommandExecutionReference,
        failure: &Report<RestoreStreamError>,
    ) {
        let error = failure.current_context();
        if error.leaves_outcome_unknown() {
            let reason = format!("{error}; the outcome of restore '{reference}' is unknown");
            self.line(TermLine::error(reason.clone()));
            self.restore
                .set(RestoreProgress::Unknown { reference, reason });
            return;
        }
        let reason = error.to_string();
        self.line(TermLine::error(reason.clone()));
        self.restore.set(RestoreProgress::Refused { reason });
    }

    fn transfer(self, attempt: u64, phase: TransferPhase, done: u64, total: u64) {
        if self.restore_attempt.get_untracked() != attempt {
            return;
        }
        self.restore_transfer
            .set(Some(TransferProgress { phase, done, total }));
    }

    /// Shows how a streamed restore of `statement` ended, in the dialog and the terminal.
    fn restore_ended(self, end: RestoreEnd, statement: &str, mode: RestoreMode, revision: u64) {
        let RestoreConclusion {
            lines,
            progress,
            plan_domain,
        } = RestoreConclusion::of(end, statement, mode, revision);
        self.console
            .terminal_lines
            .update(|terminal| terminal.extend(lines));
        self.plan_domain.set(plan_domain);
        self.restore.set(progress);
    }
}

/// How a streamed restore ended: the terminal lines that show it, where the dialog's restore
/// stands, and the restored domain whose planned model run a dry run draws first.
pub(crate) struct RestoreConclusion {
    pub(crate) lines: Vec<TermLine>,
    pub(crate) progress: RestoreProgress,
    pub(crate) plan_domain: Option<DomainName>,
}

impl RestoreConclusion {
    /// The conclusion of a restore of `statement` in `mode`, whose dry run planned draft revision
    /// `revision`. The outcome renders as the dispatcher renders every command's.
    pub(crate) fn of(end: RestoreEnd, statement: &str, mode: RestoreMode, revision: u64) -> Self {
        let outcome = match end {
            RestoreEnd::Outcome(outcome) => *outcome,
            RestoreEnd::Refused { failure, message } => {
                let reason = format!("restore refused ({failure:?}): {message}");
                return Self {
                    lines: vec![TermLine::error(reason.clone())],
                    progress: RestoreProgress::Refused { reason },
                    plan_domain: None,
                };
            }
        };
        let completed = matches!(outcome.disposition, CommandDisposition::Completed { .. });
        let report = outcome.restore.clone();
        let message = outcome.message.clone();
        let lines = command_outcome_lines(outcome, statement);
        let Some(report) = report else {
            // A restore refused before it wrote anything reports no steps.
            return Self {
                lines,
                progress: RestoreProgress::Refused { reason: message },
                plan_domain: None,
            };
        };
        if !completed {
            return Self {
                lines,
                progress: RestoreProgress::Failed {
                    reason: message,
                    report,
                },
                plan_domain: None,
            };
        }
        match mode {
            RestoreMode::DryRun => {
                // Bounded by the domains one archive holds.
                let planned = report
                    .domains
                    .iter()
                    .find(|domain| domain.planned_models.is_some());
                let plan_domain = planned.map(|domain| domain.domain.clone());
                Self {
                    lines,
                    progress: RestoreProgress::Planned { revision, report },
                    plan_domain,
                }
            }
            RestoreMode::Apply => Self {
                lines,
                progress: RestoreProgress::Restored { report },
                plan_domain: None,
            },
        }
    }
}

#[component]
pub(crate) fn BackupMenuButton(
    backups: BackupSignals,
    active_domain: RwSignal<Option<DomainName>>,
) -> impl IntoView {
    view! {
        <button
            class="backup-menu-button"
            type="button"
            aria-haspopup="dialog"
            on:click=move |_| backups.open_backup(active_domain.get_untracked().as_ref())
        >
            <span aria-hidden="true">"⇅"</span>
            <span>"Backups"</span>
        </button>
    }
}

#[component]
pub(crate) fn BackupDialog(
    backups: BackupSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    domain_names: Signal<Vec<DomainName>>,
) -> impl IntoView {
    let close = move || backups.open.set(false);
    view! {
        <Show when=move || backups.open.get() fallback=|| ()>
            <div
                class="modal-scrim backup-scrim"
                on:click=move |_| close()
                on:keydown=move |event: ev::KeyboardEvent| {
                    if event.key() == "Escape" {
                        event.prevent_default();
                        close();
                    }
                }
            >
                <section
                    class="backup-dialog"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="backup-dialog-title"
                    on:click=move |event| event.stop_propagation()
                >
                    <header class="create-head backup-head">
                        <div>
                            <span>"Backups"</span>
                            <h2 id="backup-dialog-title">"Back up and restore"</h2>
                        </div>
                        <button class="dialog-close backup-close" type="button" title="Close" aria-label="Close backup dialog" on:click=move |_| close()>"×"</button>
                    </header>
                    <nav class="backup-tabs" role="tablist">
                        <TabButton backups=backups tab=BackupTab::Backup label="Back up" />
                        <TabButton backups=backups tab=BackupTab::Restore label="Restore" />
                    </nav>
                    <Show when=move || backups.tab.get() == BackupTab::Backup fallback=|| ()>
                        <BackupForm backups=backups request_tx=request_tx domain_names=domain_names />
                    </Show>
                    <Show when=move || backups.tab.get() == BackupTab::Restore fallback=|| ()>
                        <RestoreForm backups=backups />
                    </Show>
                </section>
            </div>
        </Show>
    }
}

#[component]
fn TabButton(backups: BackupSignals, tab: BackupTab, label: &'static str) -> impl IntoView {
    view! {
        <button
            class="backup-tab"
            class:active=move || backups.tab.get() == tab
            type="button"
            role="tab"
            data-tab=tab.data_tab()
            aria-selected=move || (backups.tab.get() == tab).to_string()
            on:click=move |_| backups.tab.set(tab)
        >
            {label}
        </button>
    }
}

#[component]
fn BackupForm(
    backups: BackupSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    domain_names: Signal<Vec<DomainName>>,
) -> impl IntoView {
    let draft = backups.backup_draft;
    let preview = move || match draft.with(BackupDraft::model) {
        Ok(backup) => backup.to_canonical_nspl(),
        Err(_) => String::new(),
    };
    let validation = move || match draft.with(BackupDraft::model) {
        Ok(_) => None,
        Err(error) => Some(error.current_context().to_string()),
    };
    let submit = move |event: ev::SubmitEvent| {
        event.prevent_default();
        let backup = match draft.with_untracked(BackupDraft::model) {
            Ok(backup) => backup,
            Err(error) => {
                backups.backup.set(BackupProgress::Failed {
                    reason: error.current_context().to_string(),
                });
                return;
            }
        };
        let query = backup.to_canonical_nspl();
        let domain = match &backup.scope {
            BackupScope::Domain(Some(domain)) => Some(domain.clone()),
            BackupScope::Domain(None) | BackupScope::Cluster => None,
        };
        backups.line(TermLine::prompt(query.clone(), None));
        backups.start_backup(request_tx, query, domain, &backup);
    };
    let progress = backups.backup;
    view! {
        <form class="backup-form" novalidate on:submit=submit>
            <fieldset class="backup-choices">
                <legend>"Scope"</legend>
                <label class="create-check">
                    <input class="backup-scope-cluster" type="radio" name="backup-scope"
                        prop:checked=move || draft.get().scope == BackupScopeChoice::Cluster
                        on:change=move |_| draft.update(|draft| draft.scope = BackupScopeChoice::Cluster) />
                    <span>"Cluster: every user and domain"</span>
                </label>
                <label class="create-check">
                    <input class="backup-scope-domain" type="radio" name="backup-scope"
                        prop:checked=move || draft.get().scope == BackupScopeChoice::Domain
                        on:change=move |_| draft.update(|draft| draft.scope = BackupScopeChoice::Domain) />
                    <span>"One domain"</span>
                </label>
            </fieldset>
            <Show when=move || draft.get().scope == BackupScopeChoice::Domain fallback=|| ()>
                <label class="create-field">
                    <span>"Domain"</span>
                    <select class="backup-domain"
                        prop:value=move || draft.get().domain
                        on:change=move |event| {
                            let value = event_target_select(&event).value();
                            draft.update(|draft| draft.domain = value);
                        }
                    >
                        <For each=move || domain_names.get() key=|domain| domain.clone() children=move |domain| {
                            let value = domain.to_string();
                            let selected = value.clone();
                            view! { <option value=value.clone() selected=move || draft.get().domain == selected>{value.clone()}</option> }
                        } />
                    </select>
                </label>
            </Show>
            <label class="create-field">
                <span>"Archive file"</span>
                <input class="backup-destination" type="text" autocomplete="off"
                    prop:value=move || draft.get().destination
                    on:input=move |event| {
                        let value = crate::event_target_input(&event).value();
                        draft.update(|draft| draft.destination = value);
                    } />
            </label>
            <fieldset class="backup-choices">
                <legend>"Runtime state"</legend>
                <label class="create-check">
                    <input class="backup-capture-quiesced" type="radio" name="backup-capture"
                        prop:checked=move || draft.get().capture == CaptureChoice::Quiesced
                        on:change=move |_| draft.update(|draft| draft.capture = CaptureChoice::Quiesced) />
                    <span>"Quiesced: pause and drain each running domain"</span>
                </label>
                <label class="create-check">
                    <input class="backup-capture-live" type="radio" name="backup-capture"
                        prop:checked=move || draft.get().capture == CaptureChoice::Live
                        on:change=move |_| draft.update(|draft| draft.capture = CaptureChoice::Live) />
                    <span>"Live: latest checkpoints, without a pause"</span>
                </label>
                <label class="create-check">
                    <input class="backup-capture-configuration" type="radio" name="backup-capture"
                        prop:checked=move || draft.get().capture == CaptureChoice::ConfigurationOnly
                        on:change=move |_| draft.update(|draft| draft.capture = CaptureChoice::ConfigurationOnly) />
                    <span>"Configuration only, without state"</span>
                </label>
            </fieldset>
            <Show when=move || draft.get().capture == CaptureChoice::Quiesced fallback=|| ()>
                <label class="create-field">
                    <span>"Quiesce timeout (empty keeps the domain's own)"</span>
                    <input class="backup-timeout" type="text" autocomplete="off" placeholder="30s"
                        prop:value=move || draft.get().timeout
                        on:input=move |event| {
                            let value = crate::event_target_input(&event).value();
                            draft.update(|draft| draft.timeout = value);
                        } />
                </label>
            </Show>
            <label class="create-check">
                <input class="backup-without-resources" type="checkbox"
                    prop:checked=move || draft.get().resources == BackupResources::Omitted
                    on:change=move |event| {
                        let omitted = crate::event_target_input(&event).checked();
                        draft.update(|draft| {
                            draft.resources = if omitted {
                                BackupResources::Omitted
                            } else {
                                BackupResources::Included
                            };
                        });
                    } />
                <span>"Without resource bytes"</span>
            </label>
            <div class="create-preview-block">
                <span>"Canonical NSPL preview"</span>
                <code class="create-preview backup-preview">{preview}</code>
            </div>
            <Show when=move || validation().is_some() fallback=|| ()>
                <p class="create-validation backup-validation" role="alert">{move || validation().unwrap_or_default()}</p>
            </Show>
            <p class="create-status backup-status" aria-live="polite">{move || progress.get().status()}</p>
            <Show when=move || progress.get().transfer().is_some() fallback=|| ()>
                <progress class="backup-progress"
                    max=move || transfer_total(progress.get().transfer())
                    value=move || transfer_done(progress.get().transfer())
                ></progress>
            </Show>
            <Show when=move || progress.get().error().is_some() fallback=|| ()>
                <p class="create-error backup-error" role="alert">{move || progress.get().error().unwrap_or_default()}</p>
            </Show>
            <Show when=move || progress.get().summary().is_some() fallback=|| ()>
                <ul class="backup-summary">
                    <For
                        each=move || match progress.get().summary() {
                            Some(summary) => summary_lines(summary),
                            None => Vec::new(),
                        }
                        key=|line| line.clone()
                        children=|line| view! { <li>{line}</li> }
                    />
                </ul>
            </Show>
            <footer class="create-actions">
                <Show when=move || matches!(progress.get(), BackupProgress::NotDownloaded { retryable: true, .. }) fallback=|| ()>
                    <button class="backup-retry-download" type="button" on:click=move |_| backups.download_again()>"Download again"</button>
                </Show>
                <button class="create-submit backup-submit" type="submit"
                    disabled=move || progress.get().is_active() || validation().is_some()
                >"Back up and download"</button>
            </footer>
        </form>
    }
}

#[component]
fn RestoreForm(backups: BackupSignals) -> impl IntoView {
    let draft = backups.restore_draft;
    let progress = backups.restore;
    let edit = move |change: &dyn Fn(&mut RestoreDraft)| {
        draft.update(|draft| change(draft));
        backups.edit_restore();
    };
    let preview = move |mode: RestoreMode| {
        backups.archive_name.track();
        draft.track();
        match backups.restore_model(mode) {
            Ok(restore) => restore.to_canonical_nspl(),
            Err(_) => String::new(),
        }
    };
    let validation = move || {
        backups.archive_name.track();
        draft.track();
        match backups.restore_model(RestoreMode::Apply) {
            Ok(_) => None,
            Err(error) => Some(error.current_context().to_string()),
        }
    };
    let can_apply = move || {
        let revision = backups.restore_revision.get();
        progress.with(|progress| progress.planned(revision))
    };
    let plan_report = Signal::derive(move || {
        let domain = backups.plan_domain.get()?;
        progress.with(|progress| {
            let plan = progress.plan()?;
            let restored = plan
                .domains
                .iter()
                .find(|restored| restored.domain == domain)?;
            restored.planned_models.clone()
        })
    });
    let plan_summary = Signal::derive(move || match backups.plan_domain.get() {
        Some(domain) => format!("Planned model run of domain {domain}"),
        None => String::new(),
    });
    view! {
        <form class="restore-form" novalidate on:submit=move |event: ev::SubmitEvent| event.prevent_default()>
            <label class="create-field">
                <span>"Archive"</span>
                <input class="restore-file" type="file" accept=".nvxb,.tar,application/x-tar"
                    on:change=move |event| {
                        let input = crate::event_target_input(&event);
                        let file = match input.files() {
                            Some(files) => files.item(0),
                            None => None,
                        };
                        backups.choose_archive(file);
                    } />
            </label>
            <fieldset class="backup-choices">
                <legend>"Scope"</legend>
                <label class="create-check">
                    <input class="restore-scope-cluster" type="radio" name="restore-scope"
                        prop:checked=move || draft.get().scope == RestoreScopeChoice::Cluster
                        on:change=move |_| edit(&|draft| draft.scope = RestoreScopeChoice::Cluster) />
                    <span>"Cluster: every user and domain of a cluster archive"</span>
                </label>
                <label class="create-check">
                    <input class="restore-scope-domain" type="radio" name="restore-scope"
                        prop:checked=move || draft.get().scope == RestoreScopeChoice::Domain
                        on:change=move |_| edit(&|draft| draft.scope = RestoreScopeChoice::Domain) />
                    <span>"One domain"</span>
                </label>
            </fieldset>
            <Show when=move || draft.get().scope == RestoreScopeChoice::Cluster fallback=|| ()>
                <label class="create-field">
                    <span>"Users the cluster already has"</span>
                    <select class="restore-existing-users"
                        on:change=move |event| {
                            let value = event_target_select(&event).value();
                            // Every option carries a policy's own name; only a page edited by
                            // hand offers another, which selects nothing.
                            let Ok(policy) = value.parse::<ExistingUserPolicy>() else {
                                return;
                            };
                            edit(&|draft| draft.existing_users = policy);
                        }
                    >
                        <ExistingUserOption draft=draft policy=ExistingUserPolicy::Fail label="Fail the restore" />
                        <ExistingUserOption draft=draft policy=ExistingUserPolicy::Skip label="Keep the existing user" />
                        <ExistingUserOption draft=draft policy=ExistingUserPolicy::Replace label="Replace its password hash" />
                    </select>
                </label>
            </Show>
            <Show when=move || draft.get().scope == RestoreScopeChoice::Domain fallback=|| ()>
                <div class="create-field-row">
                    <label class="create-field">
                        <span>"Archived domain"</span>
                        <input class="restore-domain" type="text" autocomplete="off"
                            prop:value=move || draft.get().domain
                            on:input=move |event| {
                                let value = crate::event_target_input(&event).value();
                                edit(&|draft| draft.domain = value.clone());
                            } />
                    </label>
                    <label class="create-field">
                        <span>"Restore as (empty keeps its name)"</span>
                        <input class="restore-target" type="text" autocomplete="off"
                            prop:value=move || draft.get().target
                            on:input=move |event| {
                                let value = crate::event_target_input(&event).value();
                                edit(&|draft| draft.target = value.clone());
                            } />
                    </label>
                </div>
            </Show>
            <fieldset class="backup-choices">
                <legend>"Runtime state"</legend>
                <label class="create-check">
                    <input class="restore-state-all" type="radio" name="restore-state"
                        prop:checked=move || draft.get().state == RestoreState::All
                        on:change=move |_| edit(&|draft| draft.state = RestoreState::All) />
                    <span>"All archived state"</span>
                </label>
                <label class="create-check">
                    <input class="restore-state-without-offsets" type="radio" name="restore-state"
                        prop:checked=move || draft.get().state == RestoreState::WithoutSourceOffsets
                        on:change=move |_| edit(&|draft| draft.state = RestoreState::WithoutSourceOffsets) />
                    <span>"Without source offsets"</span>
                </label>
                <label class="create-check">
                    <input class="restore-state-configuration" type="radio" name="restore-state"
                        prop:checked=move || draft.get().state == RestoreState::ConfigurationOnly
                        on:change=move |_| edit(&|draft| draft.state = RestoreState::ConfigurationOnly) />
                    <span>"Configuration only, without state"</span>
                </label>
            </fieldset>
            <label class="create-check">
                <input class="restore-resume" type="checkbox"
                    prop:checked=move || draft.get().lifecycle == RestoreLifecycle::Resume
                    on:change=move |event| {
                        let resume = crate::event_target_input(&event).checked();
                        let lifecycle = if resume {
                            RestoreLifecycle::Resume
                        } else {
                            RestoreLifecycle::Stopped
                        };
                        edit(&|draft| draft.lifecycle = lifecycle);
                    } />
                <span>"Resume the restored domains at their archived lifecycle"</span>
            </label>
            <div class="create-preview-block">
                <span>"Dry run"</span>
                <code class="create-preview restore-dry-run-preview">{move || preview(RestoreMode::DryRun)}</code>
                <span>"Restore"</span>
                <code class="create-preview restore-preview">{move || preview(RestoreMode::Apply)}</code>
            </div>
            <Show when=move || validation().is_some() fallback=|| ()>
                <p class="create-validation restore-validation" role="alert">{move || validation().unwrap_or_default()}</p>
            </Show>
            <p class="create-status restore-status" aria-live="polite">{move || progress.get().status()}</p>
            <Show when=move || backups.restore_transfer.get().is_some() fallback=|| ()>
                <progress class="restore-progress"
                    max=move || transfer_total(backups.restore_transfer.get().map(|transfer| (transfer.done, transfer.total)))
                    value=move || transfer_done(backups.restore_transfer.get().map(|transfer| (transfer.done, transfer.total)))
                ></progress>
                <p class="restore-progress-text">{move || match backups.restore_transfer.get() {
                    Some(transfer) => transfer.text(),
                    None => String::new(),
                }}</p>
            </Show>
            <Show when=move || progress.get().error().is_some() fallback=|| ()>
                <p class="create-error restore-error" role="alert">{move || progress.get().error().unwrap_or_default()}</p>
            </Show>
            <Show when=move || progress.with(|progress| progress.plan().is_some()) fallback=|| ()>
                <ul class="restore-plan">
                    <For
                        each=move || match progress.get().plan() {
                            Some(report) => restore_report_lines(report),
                            None => Vec::new(),
                        }
                        key=|line| line.clone()
                        children=|line| view! { <li>{line}</li> }
                    />
                </ul>
                <div class="restore-impact-domains">
                    <For
                        each=move || match progress.get().plan() {
                            Some(report) => report
                                .domains
                                .iter()
                                .filter(|domain| domain.planned_models.is_some())
                                .map(|domain| domain.domain.clone())
                                .collect::<Vec<_>>(),
                            None => Vec::new(),
                        }
                        key=|domain| domain.clone()
                        children=move |domain| {
                            let chosen = domain.clone();
                            let label = domain.to_string();
                            view! {
                                <button class="restore-impact-domain" type="button"
                                    class:active=move || backups.plan_domain.get().as_ref() == Some(&domain)
                                    on:click=move |_| backups.plan_domain.set(Some(chosen.clone()))
                                >{label}</button>
                            }
                        }
                    />
                </div>
                <div class="restore-impact">
                    <ImpactReportView report=plan_report summary=plan_summary waiting="This domain holds no models to plan." />
                </div>
            </Show>
            <Show when=move || progress.with(|progress| progress.result().is_some()) fallback=|| ()>
                <ul class="restore-result">
                    <For
                        each=move || match progress.get().result() {
                            Some(report) => restore_report_lines(report),
                            None => Vec::new(),
                        }
                        key=|line| line.clone()
                        children=|line| view! { <li>{line}</li> }
                    />
                </ul>
            </Show>
            <footer class="create-actions">
                <button class="create-cancel restore-dry-run" type="button"
                    disabled=move || progress.get().is_active() || validation().is_some()
                    on:click=move |_| backups.run_restore(RestoreMode::DryRun)
                >"Dry run"</button>
                <button class="create-submit restore-submit" type="button"
                    disabled=move || progress.get().is_active() || !can_apply()
                    on:click=move |_| backups.run_restore(RestoreMode::Apply)
                >"Restore"</button>
            </footer>
        </form>
    }
}

#[component]
fn ExistingUserOption(
    draft: RwSignal<RestoreDraft>,
    policy: ExistingUserPolicy,
    label: &'static str,
) -> impl IntoView {
    view! {
        <option value=policy.as_ref().to_string() selected=move || draft.get().existing_users == policy>
            {label}
        </option>
    }
}

/// The size of a transfer, as a progress bar's maximum.
fn transfer_total(transfer: Option<(u64, u64)>) -> String {
    match transfer {
        Some((_, total)) => total.to_string(),
        None => "0".to_string(),
    }
}

/// The bytes a transfer moved, as a progress bar's value.
fn transfer_done(transfer: Option<(u64, u64)>) -> String {
    match transfer {
        Some((done, _)) => done.to_string(),
        None => "0".to_string(),
    }
}

fn event_target_select(event: &ev::Event) -> web_sys::HtmlSelectElement {
    let target = event
        .target()
        .verified("a change event names the element it changed");
    target
        .dyn_into::<web_sys::HtmlSelectElement>()
        .verified("this handler is only bound to a select element")
}
