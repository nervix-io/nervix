//! One statement the CLI sends, typed at the prompt or given to `--command`, how it ends, and the
//! lines that print its outcome.
//!
//! Layer: edges.
//!
//! - **Owns.** Serving the statements the CLI runs itself, streaming an upload with its progress
//!   line, sending every other statement through the client, interrupting a statement on Ctrl-C,
//!   printing each outcome and its diagnostics, telling the operator how to recover an outcome that
//!   did not arrive, and repeating such a statement under its identity when it runs again at the
//!   prompt.
//! - **Depends on.** `nervix-client-core`, the language layer's client statement parser, and the
//!   backup, restore, interrupt and status modules of this crate.
//! - **Must not know.** The server. It speaks the session API through the client core and nothing
//!   else.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{io, io::Write, ops::Range, path::PathBuf};

use arch_into::ArchInto as _;
use ariadne::{Color, Config, IndexType, Label, Report, ReportKind, Source};
use byte_unit::{Byte, UnitType};
use error_stack::Report as StackReport;
use nervix_client_core::{
    Client, ClientError as CoreClientError, CommandDisposition, CommandExecutionReference,
    CommandOutcome, Diagnostic, ExecutionHandle, LeaderRedirect, ResourceUploadIdentity, Restore,
    SourceSpan, StatementDisposition, StatementOutcome, wire::CancellationStage,
};
use nervix_models::UploadResource;
use nervix_plexus::client_statement::parse_upload_resource_query;
use nervix_primitives::sync::{
    Arc, CancellationToken,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use nervix_recovery::{Discarded as _, Reported as _};

use super::{
    ClientError, backup,
    interrupt::{interrupted, unless_interrupted},
    restore,
    status::RunEnd,
};

/// Where the statements a runner sends come from, which decides how the CLI tells its operator to
/// recover an outcome that did not arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementSource {
    /// Typed at the REPL's prompt, where running the same statement again repeats it.
    Prompt,
    /// Given to `--command` or sent by a node administration subcommand: the run ends with the
    /// statement, and the operator passes its reference or identity to the next run.
    Arguments,
}

/// The execution reference and upload identity an operator passed back to repeat a statement
/// whose earlier run did not learn its outcome.
#[derive(Debug, Clone, Default)]
pub(super) struct Repetition {
    pub(super) reference: Option<CommandExecutionReference>,
    pub(super) upload_identity: Option<ResourceUploadIdentity>,
}

/// A statement whose outcome did not arrive when the REPL last ran it, kept so that running it
/// again repeats it rather than starting another command.
struct UncertainStatement {
    /// The statement as it was submitted, without the whitespace around it.
    text: String,
    execution: ExecutionHandle,
}

/// Runs the statements of one CLI session, and keeps the last one whose outcome did not arrive.
pub(super) struct StatementRunner {
    source: StatementSource,
    repetition: Repetition,
    uncertain: Option<UncertainStatement>,
}

impl StatementRunner {
    /// A runner for the statements typed at the REPL's prompt.
    pub(super) fn at_prompt() -> Self {
        Self {
            source: StatementSource::Prompt,
            repetition: Repetition::default(),
            uncertain: None,
        }
    }

    /// A runner for the one statement `--command` or a subcommand sends, repeated under
    /// `repetition` when the operator passed one back.
    pub(super) fn for_arguments(repetition: Repetition) -> Self {
        Self {
            source: StatementSource::Arguments,
            repetition,
            uncertain: None,
        }
    }

    /// Runs `query`, prints its outcome, and returns how it ended. At the prompt a failed request
    /// is printed too; for arguments it is returned, so the run ends with its whole report.
    pub(super) async fn run(
        &mut self,
        client: &Client,
        query: String,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        if let Some(describe) = backup::describe_backup_statement(&query) {
            let described = backup::run_describe_backup(&describe);
            let end = match &described {
                Ok(()) => RunEnd::Completed,
                Err(_) => RunEnd::Failed,
            };
            described.discarded("describing an archive already printed why it failed");
            return Ok(end);
        }
        let execution = self.execution_for(client, &query).await;
        let end = if let Some(restore) = restore::restore_statement(&query) {
            self.run_restore(client, &execution, &restore).await?
        } else if let Ok(upload) = parse_upload_resource_query(&query) {
            self.run_upload(client, &execution, upload).await?
        } else {
            self.run_command(client, &execution, &query).await?
        };
        if let RunEnd::Uncertain = end {
            self.uncertain = Some(UncertainStatement {
                text: query.trim().to_string(),
                execution,
            });
        }
        Ok(end)
    }

    /// The execution `query` runs under: the uncertain statement's own when `query` repeats it in
    /// the same domain, which recovers its outcome, and otherwise a new one under the identity the
    /// operator passed back, if any.
    async fn execution_for(&mut self, client: &Client, query: &str) -> ExecutionHandle {
        if let Some(uncertain) = self.uncertain.take() {
            let domain = client.domain().await;
            if uncertain.text == query.trim() && uncertain.execution.domain() == domain.as_ref() {
                let repeated = uncertain.execution;
                if parse_upload_resource_query(query).is_ok() {
                    emit_terminal_line(format!(
                        "repeating the uncertain upload under upload identity {}",
                        repeated.upload_identity()
                    ));
                } else {
                    emit_terminal_line(format!(
                        "repeating the uncertain statement under execution reference {}",
                        repeated.reference()
                    ));
                }
                return repeated;
            }
            self.uncertain = Some(uncertain);
        }
        let mut execution = client.prepare_execution(query.to_string()).await;
        if let Some(reference) = self.repetition.reference.clone() {
            execution = execution.with_reference(reference);
        }
        if let Some(identity) = self.repetition.upload_identity.clone() {
            execution = execution.with_upload_identity(identity);
        }
        execution
    }

    /// Sends a statement the server runs, or the client serves itself, and prints its outcome.
    /// The first Ctrl-C asks the server to cancel the command and waits for what the server
    /// decided; a second one stops waiting.
    async fn run_command(
        &self,
        client: &Client,
        execution: &ExecutionHandle,
        query: &str,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        let cancellation = CancellationToken::new();
        let mut running =
            std::pin::pin!(client.execute_prepared_with_cancellation(execution, &cancellation));
        let result = nervix_primitives::select! {
            result = &mut running => result,
            interrupt = interrupted() => {
                interrupt?;
                cancellation.cancel();
                nervix_primitives::select! {
                    result = &mut running => result,
                    interrupt = interrupted() => {
                        interrupt?;
                        emit_terminal_line("interrupted: stopped waiting for the server's answer");
                        emit_terminal_line(self.command_recovery(
                            "the command may have been admitted",
                            execution.reference(),
                        ));
                        return Ok(RunEnd::Uncertain);
                    }
                }
            }
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(report) => return self.command_failed(report),
        };
        Ok(self.print_command_outcome(&outcome, query))
    }

    /// Prints a command's outcome and returns the worst ending of its statements.
    fn print_command_outcome(&self, outcome: &CommandOutcome, query: &str) -> RunEnd {
        let mut end = RunEnd::of_command(&outcome.disposition);
        if outcome.statements.is_empty() {
            print_outcome(&PrintedOutcome::of_command(outcome), query);
        } else {
            for statement in &outcome.statements {
                print_outcome(&PrintedOutcome::of_statement(statement), query);
                end = end.max(RunEnd::of_statement(&statement.disposition));
            }
        }
        if let (CommandDisposition::OutcomeUnknown(_), Some(reference)) =
            (&outcome.disposition, &outcome.execution_reference)
        {
            emit_terminal_line(self.command_recovery("the command was admitted", reference));
        }
        end
    }

    /// Prints why a statement's call failed and returns how it ended: cancelled before the server
    /// admitted it, not known yet, or failed.
    fn command_failed(
        &self,
        report: StackReport<CoreClientError>,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        match report.current_context() {
            CoreClientError::RequestCancelled {
                stage: CancellationStage::BeforeAdmission,
                ..
            } => {
                emit_terminal_line(
                    "cancelled: the command was not admitted, so it changed nothing",
                );
                Ok(RunEnd::Cancelled)
            }
            CoreClientError::UncertainCommand { reference } => {
                emit_terminal_line(format!("error: {report:#}"));
                emit_terminal_line(
                    self.command_recovery("the command may have been admitted", reference),
                );
                Ok(RunEnd::Uncertain)
            }
            CoreClientError::BackupDownload { reference } => {
                emit_terminal_line(format!("error: {report:#}"));
                emit_terminal_line(self.archive_recovery(reference));
                Ok(RunEnd::Uncertain)
            }
            CoreClientError::UncertainUpload { identity } => {
                emit_terminal_line(format!("error: {report:#}"));
                emit_terminal_line(self.upload_recovery(identity));
                Ok(RunEnd::Uncertain)
            }
            CoreClientError::CallCancelled | CoreClientError::RequestCancelled { .. } => {
                emit_terminal_line(
                    "interrupted: stopped waiting for the statement, which may still take effect",
                );
                Ok(RunEnd::Uncertain)
            }
            _ => self.request_failed(report),
        }
    }

    /// A request that failed and changed nothing: printed at the prompt, and returned for
    /// arguments, which end the run with its whole report.
    fn request_failed(
        &self,
        report: StackReport<CoreClientError>,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        match self.source {
            StatementSource::Prompt => {
                emit_terminal_line(format!("error: {report:#}"));
                Ok(RunEnd::Failed)
            }
            StatementSource::Arguments => Err(report.change_context(ClientError::Request)),
        }
    }

    /// Streams a directory as a version of a resource, with a progress line while it sends. Ctrl-C
    /// stops sending: no request can cancel an upload, and the server may already hold the whole
    /// archive.
    async fn run_upload(
        &self,
        client: &Client,
        execution: &ExecutionHandle,
        upload: UploadResource,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        let identifier = upload.identifier.to_string();
        let directory = PathBuf::from(upload.source_path);
        let Some(domain) = execution.domain().cloned() else {
            return self.request_failed(StackReport::new(CoreClientError::NoActiveDomain));
        };
        let identity = execution.upload_identity();
        let progress = UploadProgress::start(&identifier);
        let uploading = client.upload_resource_from_directory_with_identity(
            &identifier,
            &directory,
            domain,
            identity.clone(),
            progress.counter(),
        );
        let uploaded = unless_interrupted(uploading).await;
        let sent = progress.finish().await;
        let Some(result) = uploaded? else {
            emit_terminal_line(
                "interrupted: stopped sending the upload, which the server may have received whole",
            );
            emit_terminal_line(self.upload_recovery(identity));
            return Ok(RunEnd::Uncertain);
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(report) => return self.command_failed(report),
        };
        if outcome.succeeded() {
            emit_terminal_line(format!(
                "upload resource '{identifier}' finished: {} sent, installed on every live node",
                human_bytes(sent),
            ));
            emit_terminal_line(outcome.message);
            return Ok(RunEnd::Completed);
        }
        emit_terminal_line(format!(
            "upload resource '{identifier}' failed after sending {}",
            human_bytes(sent),
        ));
        emit_terminal_line(format!("error: {}", outcome.message));
        if outcome.diagnostics.is_empty() {
            emit_terminal_line("- no diagnostics provided");
        } else {
            let query_source = format!(
                "UPLOAD RESOURCE {identifier} VERSION '{}';",
                directory.display()
            );
            print_diagnostics("upload", &query_source, &outcome.diagnostics);
        }
        Ok(RunEnd::Failed)
    }

    /// Streams the archive a `RESTORE` names under the execution's reference and prints its
    /// report. Ctrl-C stops streaming: no request can cancel a restore stream, and the server may
    /// already hold the whole archive.
    async fn run_restore(
        &self,
        client: &Client,
        execution: &ExecutionHandle,
        restore: &Restore,
    ) -> Result<RunEnd, StackReport<ClientError>> {
        let Some(result) = restore::stream_restore(client, restore, execution.reference()).await?
        else {
            emit_terminal_line(
                "interrupted: stopped streaming the restore archive, which the server may have \
                 received whole",
            );
            emit_terminal_line(
                self.command_recovery("the restore may have been admitted", execution.reference()),
            );
            return Ok(RunEnd::Uncertain);
        };
        match result {
            Ok(outcome) => Ok(restore::print_restore_outcome(&outcome)),
            Err(report) => self.command_failed(report),
        }
    }

    /// How the operator recovers the outcome of a command that `admission` describes.
    fn command_recovery(&self, admission: &str, reference: &CommandExecutionReference) -> String {
        match self.source {
            StatementSource::Prompt => format!(
                "outcome: not known yet; {admission}. Run the same statement again to recover its \
                 outcome under execution reference {reference}."
            ),
            StatementSource::Arguments => format!(
                "outcome: not known yet; {admission}. Run it again with --execution-reference \
                 {reference} to recover its outcome."
            ),
        }
    }

    /// How the operator recovers an upload whose outcome did not arrive.
    fn upload_recovery(&self, identity: &ResourceUploadIdentity) -> String {
        match self.source {
            StatementSource::Prompt => format!(
                "upload outcome: not known yet; the server may have installed it. Run the same \
                 statement again to recover its outcome under upload identity {identity}."
            ),
            StatementSource::Arguments => format!(
                "upload outcome: not known yet; the server may have installed it. Run it again \
                 with --upload-identity {identity} to recover its outcome."
            ),
        }
    }

    /// How the operator downloads the archive of a backup that completed without delivering it.
    fn archive_recovery(&self, reference: &CommandExecutionReference) -> String {
        match self.source {
            StatementSource::Prompt => format!(
                "archive: not downloaded; the backup completed. Run the same statement again to \
                 download its archive under execution reference {reference}."
            ),
            StatementSource::Arguments => format!(
                "archive: not downloaded; the backup completed. Run it again with \
                 --execution-reference {reference} to download its archive."
            ),
        }
    }
}

/// How much of its archive an upload has sent, and whether it is done sending.
struct UploadProgressState {
    sent: AtomicU64,
    finished: AtomicBool,
}

/// The progress line an upload draws while it streams its archive.
struct UploadProgress {
    state: Arc<UploadProgressState>,
    drawing: nervix_primitives::task::JoinHandle<()>,
}

impl UploadProgress {
    /// Starts drawing the progress of uploading a version of `identifier`.
    fn start(identifier: &str) -> Self {
        let state = Arc::new(UploadProgressState {
            sent: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        });
        let drawn = Arc::clone(&state);
        let identifier = identifier.to_string();
        let drawing = nervix_primitives::task::spawn(async move {
            let mut interval =
                nervix_primitives::time::interval(std::time::Duration::from_millis(120));
            let frames = ["|", "/", "-", "\\"];
            let mut frame_index = 0_usize;
            loop {
                interval.tick().await;
                let bytes = drawn.sent.load(Ordering::Relaxed);
                if drawn.finished.load(Ordering::Relaxed) {
                    break;
                }
                render_progress_line(format!(
                    "{} upload resource '{}' {} (streaming archive)",
                    frames[frame_index % frames.len()],
                    identifier,
                    human_bytes(bytes),
                ));
                frame_index += 1;
            }
        });
        Self { state, drawing }
    }

    /// What the upload tells each number of archive bytes it hands to the transport.
    fn counter(&self) -> impl Fn(u64) + Send + Sync + Clone + 'static {
        let state = Arc::clone(&self.state);
        move |bytes| {
            state.sent.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Stops drawing, clears the line, and returns how many bytes the upload sent.
    async fn finish(self) -> u64 {
        self.state.finished.store(true, Ordering::Relaxed);
        // The task only renders the progress line, and `finished` has already told it to stop.
        // Losing its join tells the operator nothing the upload outcome does not already say.
        self.drawing
            .await
            .reported("rendering the upload progress line");
        clear_progress_line();
        self.state.sent.load(Ordering::Relaxed)
    }
}

/// One statement's outcome as the terminal prints it.
struct PrintedOutcome<'a> {
    disposition: CommandDisposition,
    message: &'a str,
    diagnostics: &'a [Diagnostic],
}

impl<'a> PrintedOutcome<'a> {
    fn of_command(outcome: &'a CommandOutcome) -> Self {
        Self {
            disposition: outcome.disposition.clone(),
            message: &outcome.message,
            diagnostics: &outcome.diagnostics,
        }
    }

    fn of_statement(outcome: &'a StatementOutcome) -> Self {
        let disposition = match &outcome.disposition {
            StatementDisposition::Completed { already_existed } => CommandDisposition::Completed {
                already_existed: *already_existed,
            },
            StatementDisposition::Failed => CommandDisposition::Failed,
            StatementDisposition::NotLeader(redirect) => {
                CommandDisposition::NotLeader(redirect.clone())
            }
        };
        Self {
            disposition,
            message: &outcome.message,
            diagnostics: &outcome.diagnostics,
        }
    }

    /// The lines printed for the outcome ahead of its diagnostics.
    fn summary(&self) -> Vec<String> {
        match &self.disposition {
            CommandDisposition::Completed { .. } => {
                if self.message.is_empty() {
                    return Vec::new();
                }
                vec![self.message.to_string()]
            }
            CommandDisposition::NotLeader(redirect) => vec![not_leader_line(redirect)],
            // The runner follows the message with how to recover the outcome.
            CommandDisposition::OutcomeUnknown(_) => {
                if self.message.is_empty() {
                    return Vec::new();
                }
                vec![self.message.to_string()]
            }
            CommandDisposition::Failed
            | CommandDisposition::TransactionDetached { .. }
            | CommandDisposition::TransactionTakenOver { .. }
            | CommandDisposition::ExecutionReferenceConflict(_)
            | CommandDisposition::ExecutionReferenceExpired
            | CommandDisposition::PreviewStale { .. } => vec![format!("error: {}", self.message)],
        }
    }
}

/// The topology line of a statement the serving node could not run because it is not the leader.
fn not_leader_line(redirect: &LeaderRedirect) -> String {
    let Some(leader) = &redirect.leader else {
        return "topology: not-a-leader".to_string();
    };
    match &leader.grpc_uri {
        Some(uri) => format!(
            "topology: not-a-leader, retry on leader '{}' at {uri}",
            leader.node
        ),
        None => format!("topology: not-a-leader, retry on leader '{}'", leader.node),
    }
}

fn print_outcome(outcome: &PrintedOutcome<'_>, query_source: &str) {
    for line in outcome.summary() {
        emit_terminal_line(line);
    }
    match outcome.disposition {
        CommandDisposition::Completed { .. } => {}
        CommandDisposition::OutcomeUnknown(_) => {
            if !outcome.diagnostics.is_empty() {
                print_diagnostics("remote", query_source, outcome.diagnostics);
            }
        }
        CommandDisposition::Failed
        | CommandDisposition::NotLeader(_)
        | CommandDisposition::TransactionDetached { .. }
        | CommandDisposition::TransactionTakenOver { .. }
        | CommandDisposition::ExecutionReferenceConflict(_)
        | CommandDisposition::ExecutionReferenceExpired
        | CommandDisposition::PreviewStale { .. } => {
            if outcome.diagnostics.is_empty() {
                emit_terminal_line("- no diagnostics provided");
            } else {
                print_diagnostics("remote", query_source, outcome.diagnostics);
            }
        }
    }
}

fn emit_terminal_line(line: impl Into<String>) {
    println!("{}", line.into());
}

fn render_progress_line(line: impl AsRef<str>) {
    print!("\r\x1b[2K{}", line.as_ref());
    io::stdout()
        .flush()
        .discarded("the next update redraws the progress line a failed flush left behind");
}

fn clear_progress_line() {
    print!("\r\x1b[2K");
    io::stdout()
        .flush()
        .discarded("the next update redraws the progress line a failed flush left behind");
}

pub(super) fn human_bytes(bytes: u64) -> String {
    let adjusted = Byte::from_u64(bytes).get_appropriate_unit(UnitType::Binary);
    format!("{adjusted:.1}")
}

pub(super) fn print_diagnostics(source_id: &str, source: &str, diagnostics: &[Diagnostic]) {
    let config = Config::default().with_index_type(IndexType::Byte);
    for diagnostic in diagnostics {
        let report = match diagnostic_range(source, diagnostic.span) {
            Some(range) => Report::build(ReportKind::Error, (source_id, range.clone()))
                .with_config(config)
                .with_message("server parse error")
                .with_label(
                    Label::new((source_id, range))
                        .with_message(diagnostic.message.clone())
                        .with_color(Color::Red),
                )
                .finish(),
            // A diagnostic without a location in this source renders without an underline. A
            // report without a label prints no source section, so its message carries the
            // diagnostic.
            None => Report::build(ReportKind::Error, (source_id, 0..0))
                .with_config(config)
                .with_message(format!("server parse error: {}", diagnostic.message))
                .finish(),
        };

        if let Err(err) = report.eprint((source_id, Source::from(source))) {
            eprintln!("failed to render diagnostic: {err}");
        }
    }
}

/// The byte range of `source` a diagnostic underlines: its span, when the span lies within the
/// source and both of its ends fall on character boundaries.
fn diagnostic_range(source: &str, span: Option<SourceSpan>) -> Option<Range<usize>> {
    let span = span?;
    let start: usize = span.start().arch_into();
    let end: usize = span.end().arch_into();
    // A span never ends before it starts, and an offset past the end of the source is never a
    // character boundary, so the two checks keep the whole range inside the source.
    if !source.is_char_boundary(start) || !source.is_char_boundary(end) {
        return None;
    }
    Some(start..end)
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::ClusterNodeName;

    use super::*;

    #[test]
    fn upload_resource_query_is_parsed_locally() {
        let parsed = parse_upload_resource_query("UPLOAD RESOURCE proto VERSION '/tmp/proto';")
            .expect("parse should succeed");
        assert_eq!(parsed.identifier.as_str(), "proto");
        assert_eq!(
            PathBuf::from(parsed.source_path),
            PathBuf::from("/tmp/proto")
        );
    }

    #[test]
    fn human_bytes_uses_human_units() {
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    fn span(start: u32, end: u32) -> Option<SourceSpan> {
        Some(SourceSpan::new(start, end).assured("the test span starts before it ends"))
    }

    #[test]
    fn diagnostic_ranges_stay_within_the_source_on_character_boundaries() {
        // `é` occupies bytes 7 and 8 of the ten-byte source.
        let source = "CREATE é;";
        assert_eq!(diagnostic_range(source, span(0, 6)), Some(0..6));
        assert_eq!(diagnostic_range(source, span(7, 9)), Some(7..9));
        assert_eq!(diagnostic_range(source, span(10, 10)), Some(10..10));
        assert_eq!(
            diagnostic_range(source, span(8, 9)),
            None,
            "a span that starts inside a character underlines nothing"
        );
        assert_eq!(
            diagnostic_range(source, span(7, 8)),
            None,
            "a span that ends inside a character underlines nothing"
        );
        assert_eq!(
            diagnostic_range(source, span(0, 11)),
            None,
            "a span past the end of the source underlines nothing"
        );
        assert_eq!(diagnostic_range(source, None), None);
    }

    #[test]
    fn rendering_a_diagnostic_never_panics_whatever_its_span() {
        let diagnostics = [
            None,
            span(8, 9),
            span(0, 11),
            span(u32::MAX, u32::MAX),
            span(0, 10),
        ]
        .map(|span| Diagnostic {
            message: "unexpected token".to_string(),
            span,
        });
        print_diagnostics("remote", "CREATE é;", &diagnostics);
        print_diagnostics("remote", "", &diagnostics);
    }

    fn leader(grpc_uri: Option<&str>) -> LeaderRedirect {
        LeaderRedirect {
            leader: Some(nervix_client_core::NodeEndpoints {
                node: ClusterNodeName::parse("node-2").assured("a valid node name"),
                grpc_uri: grpc_uri
                    .map(|uri| url::Url::parse(uri).assured("the test URI is a valid URL")),
                web_console_uri: None,
            }),
        }
    }

    fn summary(disposition: CommandDisposition, message: &str) -> Vec<String> {
        PrintedOutcome {
            disposition,
            message,
            diagnostics: &[],
        }
        .summary()
    }

    #[test]
    fn outcome_summaries_follow_their_disposition() {
        let completed = CommandDisposition::Completed {
            already_existed: false,
        };
        assert_eq!(summary(completed.clone(), "created"), ["created"]);
        assert!(summary(completed, "").is_empty());
        assert_eq!(
            summary(CommandDisposition::Failed, "unknown relay"),
            ["error: unknown relay"]
        );
        assert_eq!(
            summary(CommandDisposition::ExecutionReferenceExpired, "expired",),
            ["error: expired"]
        );
        assert_eq!(
            summary(
                CommandDisposition::NotLeader(leader(Some("http://127.0.0.1:47393"))),
                "",
            ),
            ["topology: not-a-leader, retry on leader 'node-2' at http://127.0.0.1:47393/"]
        );
        assert_eq!(
            summary(CommandDisposition::NotLeader(leader(None)), ""),
            ["topology: not-a-leader, retry on leader 'node-2'"]
        );
        assert_eq!(
            summary(
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
                "",
            ),
            ["topology: not-a-leader"]
        );

        assert_eq!(
            summary(
                CommandDisposition::OutcomeUnknown(
                    nervix_client_core::UnknownOutcomeCause::StillApplying
                ),
                "the command is still applying",
            ),
            ["the command is still applying"]
        );
    }

    #[test]
    fn statement_outcomes_print_like_the_command_outcomes_they_are_part_of() {
        let statements = [
            StatementOutcome {
                disposition: StatementDisposition::Completed {
                    already_existed: true,
                },
                message: "domain 'orders' already exists".to_string(),
                diagnostics: Vec::new(),
            },
            StatementOutcome {
                disposition: StatementDisposition::Failed,
                message: "unknown schema 'order'".to_string(),
                diagnostics: Vec::new(),
            },
            StatementOutcome {
                disposition: StatementDisposition::NotLeader(leader(None)),
                message: String::new(),
                diagnostics: Vec::new(),
            },
        ];

        let lines = statements
            .iter()
            .map(|statement| PrintedOutcome::of_statement(statement).summary())
            .collect::<Vec<_>>();

        assert_eq!(
            lines,
            [
                vec!["domain 'orders' already exists".to_string()],
                vec!["error: unknown schema 'order'".to_string()],
                vec!["topology: not-a-leader, retry on leader 'node-2'".to_string()],
            ]
        );
    }

    const REFERENCE: &str = "01a1f3c2-7e4b-7a10-9c3d-5b2e8f6a1d00";

    fn reference() -> CommandExecutionReference {
        CommandExecutionReference::parse(REFERENCE).assured("the test reference is a UUID")
    }

    fn cancelled(stage: CancellationStage) -> StackReport<CoreClientError> {
        StackReport::new(CoreClientError::RequestCancelled {
            request: nervix_client_core::RequestKind::Command,
            stage,
        })
    }

    #[test]
    fn a_failed_call_ends_as_what_its_typed_cause_says_about_the_command() {
        let prompt = StatementRunner::at_prompt();
        let arguments = StatementRunner::for_arguments(Repetition::default());

        let not_admitted = arguments.command_failed(cancelled(CancellationStage::BeforeAdmission));
        assert_eq!(not_admitted.ok(), Some(RunEnd::Cancelled));

        let admitted = cancelled(CancellationStage::AfterAdmission).change_context(
            CoreClientError::UncertainCommand {
                reference: reference(),
            },
        );
        assert_eq!(
            arguments.command_failed(admitted).ok(),
            Some(RunEnd::Uncertain)
        );

        let stopped = StackReport::new(CoreClientError::CallCancelled);
        assert_eq!(prompt.command_failed(stopped).ok(), Some(RunEnd::Uncertain));

        let undelivered = StackReport::new(CoreClientError::RetryDeadline).change_context(
            CoreClientError::BackupDownload {
                reference: reference(),
            },
        );
        assert_eq!(
            prompt.command_failed(undelivered).ok(),
            Some(RunEnd::Uncertain)
        );

        let refused = StackReport::new(CoreClientError::RetryDeadline);
        assert_eq!(prompt.command_failed(refused).ok(), Some(RunEnd::Failed));
        let refused = StackReport::new(CoreClientError::RetryDeadline);
        let Err(report) = arguments.command_failed(refused) else {
            panic!("a failed request ends a run of arguments with its whole report");
        };
        assert!(matches!(report.current_context(), ClientError::Request));
    }

    #[test]
    fn a_recovery_line_names_the_identity_and_how_its_source_repeats_it() {
        let prompt = StatementRunner::at_prompt();
        let arguments = StatementRunner::for_arguments(Repetition::default());
        let reference = reference();
        assert_eq!(
            arguments.command_recovery("the command may have been admitted", &reference),
            format!(
                "outcome: not known yet; the command may have been admitted. Run it again with \
                 --execution-reference {REFERENCE} to recover its outcome."
            )
        );
        assert_eq!(
            prompt.command_recovery("the command was admitted", &reference),
            format!(
                "outcome: not known yet; the command was admitted. Run the same statement again \
                 to recover its outcome under execution reference {REFERENCE}."
            )
        );
        let identity =
            ResourceUploadIdentity::parse("upload-1").assured("the test identity is valid");
        assert_eq!(
            arguments.upload_recovery(&identity),
            "upload outcome: not known yet; the server may have installed it. Run it again with \
             --upload-identity upload-1 to recover its outcome."
        );
        assert_eq!(
            prompt.upload_recovery(&identity),
            "upload outcome: not known yet; the server may have installed it. Run the same \
             statement again to recover its outcome under upload identity upload-1."
        );
        assert!(arguments.archive_recovery(&reference).contains(&format!(
            "--execution-reference {REFERENCE} to download its archive"
        )));
    }

    #[test]
    fn a_command_of_several_statements_ends_as_its_worst_statement() {
        let statement = |disposition| StatementOutcome {
            disposition,
            message: String::new(),
            diagnostics: Vec::new(),
        };
        let outcome = CommandOutcome {
            execution_reference: Some(reference()),
            origin: None,
            disposition: CommandDisposition::Completed {
                already_existed: false,
            },
            message: String::new(),
            diagnostics: Vec::new(),
            statements: vec![
                statement(StatementDisposition::Completed {
                    already_existed: false,
                }),
                statement(StatementDisposition::Failed),
            ],
            transaction: None,
            transaction_admission: None,
            inspection: None,
            entity_labels: None,
            state: None,
            resource: None,
            subscription: None,
            resource_upload: None,
            backup: None,
            restore: None,
        };
        let runner = StatementRunner::for_arguments(Repetition::default());
        assert_eq!(
            runner.print_command_outcome(&outcome, "CREATE DOMAIN a; CREATE DOMAIN b;"),
            RunEnd::Failed
        );
    }
}
