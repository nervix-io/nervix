//! One statement the CLI sends, typed at the prompt or given to `--command`, and the lines that
//! print its outcome.
//!
//! Layer: edges.
//!
//! - **Owns.** Serving the statements the CLI runs itself, streaming an upload with its progress
//!   line, sending every other statement through the client, and printing each outcome and its
//!   diagnostics.
//! - **Depends on.** `nervix-client-core`, the language layer's client statement parser, and the
//!   backup and restore statements of this crate.
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
use error_stack::{Report as StackReport, ResultExt as _};
use nervix_client_core::{
    Client, CommandDisposition, CommandExecutionReference, CommandOutcome, Diagnostic,
    LeaderRedirect, SourceSpan, StatementDisposition, StatementOutcome,
};
use nervix_plexus::client_statement::parse_upload_resource_query;
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use nervix_recovery::{Discarded as _, Reported as _};

use super::{ClientError, backup, restore};

pub(super) async fn execute_and_print(
    client: &Client,
    query: String,
) -> Result<(), StackReport<ClientError>> {
    if let Some(describe) = backup::describe_backup_statement(&query) {
        backup::run_describe_backup(&describe)
            .discarded("describing an archive already printed why it failed");
        return Ok(());
    }
    if let Some(restore) = restore::restore_statement(&query) {
        return restore::execute_restore_and_print(client, &restore).await;
    }
    if let Ok(upload) = parse_upload_resource_query(&query) {
        return execute_upload_and_print(
            client,
            upload.identifier.to_string(),
            PathBuf::from(upload.source_path),
        )
        .await;
    }
    let query_source = query.clone();
    let result = client
        .execute(query)
        .await
        .change_context(ClientError::Request)?;
    if !result.statements.is_empty() {
        for statement in &result.statements {
            print_outcome(&PrintedOutcome::of_statement(statement), &query_source);
        }
        return Ok(());
    }
    print_outcome(&PrintedOutcome::of_command(&result), &query_source);

    Ok(())
}

/// One statement's outcome as the terminal prints it.
struct PrintedOutcome<'a> {
    disposition: CommandDisposition,
    message: &'a str,
    diagnostics: &'a [Diagnostic],
    execution_reference: Option<&'a CommandExecutionReference>,
}

impl<'a> PrintedOutcome<'a> {
    fn of_command(outcome: &'a CommandOutcome) -> Self {
        Self {
            disposition: outcome.disposition.clone(),
            message: &outcome.message,
            diagnostics: &outcome.diagnostics,
            execution_reference: outcome.execution_reference.as_ref(),
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
            execution_reference: None,
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
            CommandDisposition::OutcomeUnknown(_) => {
                let mut lines = Vec::with_capacity(2);
                if !self.message.is_empty() {
                    lines.push(self.message.to_string());
                }
                let hint = match self.execution_reference {
                    Some(reference) => format!(
                        "outcome: not known yet; the command was admitted, and retrying the same \
                         request (execution reference {reference}) recovers it"
                    ),
                    None => "outcome: not known yet; the command was admitted, and retrying the \
                             same request recovers it"
                        .to_string(),
                };
                lines.push(hint);
                lines
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

async fn execute_upload_and_print(
    client: &Client,
    identifier: String,
    directory: PathBuf,
) -> Result<(), StackReport<ClientError>> {
    let uploaded = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicBool::new(false));
    let progress_uploaded = Arc::clone(&uploaded);
    let progress_finished = Arc::clone(&finished);
    let progress_identifier = identifier.clone();
    let progress_task = nervix_primitives::task::spawn(async move {
        let mut interval = nervix_primitives::time::interval(std::time::Duration::from_millis(120));
        let frames = ["|", "/", "-", "\\"];
        let mut frame_index = 0_usize;
        loop {
            interval.tick().await;
            let bytes = progress_uploaded.load(Ordering::Relaxed);
            if progress_finished.load(Ordering::Relaxed) {
                break;
            }
            render_progress_line(format!(
                "{} upload resource '{}' {} (streaming archive)",
                frames[frame_index % frames.len()],
                progress_identifier,
                human_bytes(bytes),
            ));
            frame_index += 1;
        }
    });

    let outcome = client
        .upload_resource_from_directory(&identifier, &directory, {
            let uploaded = Arc::clone(&uploaded);
            move |bytes| {
                uploaded.fetch_add(bytes, Ordering::Relaxed);
            }
        })
        .await;

    finished.store(true, Ordering::Relaxed);
    // The task only renders the progress line, and `finished` has already told it to stop. Losing
    // its join tells the operator nothing the upload outcome below does not already say.
    progress_task
        .await
        .reported("rendering the upload progress line");
    let total_uploaded = uploaded.load(Ordering::Relaxed);
    clear_progress_line();
    let result = outcome.change_context(ClientError::Request)?;
    if result.succeeded() {
        emit_terminal_line(format!(
            "upload resource '{}' finished: {} sent, installed on every live node",
            identifier,
            human_bytes(total_uploaded),
        ));
        emit_terminal_line(result.message);
    } else {
        emit_terminal_line(format!(
            "upload resource '{}' failed after sending {}",
            identifier,
            human_bytes(total_uploaded),
        ));
        emit_terminal_line(format!("error: {}", result.message));
        if result.diagnostics.is_empty() {
            emit_terminal_line("- no diagnostics provided");
        } else {
            let query_source = format!(
                "UPLOAD RESOURCE {} VERSION '{}';",
                identifier,
                directory.display()
            );
            print_diagnostics("upload", &query_source, &result.diagnostics);
        }
    }

    Ok(())
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

    fn summary(
        disposition: CommandDisposition,
        message: &str,
        execution_reference: Option<&CommandExecutionReference>,
    ) -> Vec<String> {
        PrintedOutcome {
            disposition,
            message,
            diagnostics: &[],
            execution_reference,
        }
        .summary()
    }

    #[test]
    fn outcome_summaries_follow_their_disposition() {
        let completed = CommandDisposition::Completed {
            already_existed: false,
        };
        assert_eq!(summary(completed.clone(), "created", None), ["created"]);
        assert!(summary(completed, "", None).is_empty());
        assert_eq!(
            summary(CommandDisposition::Failed, "unknown relay", None),
            ["error: unknown relay"]
        );
        assert_eq!(
            summary(
                CommandDisposition::ExecutionReferenceExpired,
                "expired",
                None
            ),
            ["error: expired"]
        );
        assert_eq!(
            summary(
                CommandDisposition::NotLeader(leader(Some("http://127.0.0.1:47393"))),
                "",
                None
            ),
            ["topology: not-a-leader, retry on leader 'node-2' at http://127.0.0.1:47393/"]
        );
        assert_eq!(
            summary(CommandDisposition::NotLeader(leader(None)), "", None),
            ["topology: not-a-leader, retry on leader 'node-2'"]
        );
        assert_eq!(
            summary(
                CommandDisposition::NotLeader(LeaderRedirect { leader: None }),
                "",
                None
            ),
            ["topology: not-a-leader"]
        );

        let reference = CommandExecutionReference::parse("command-1").assured("a valid reference");
        assert_eq!(
            summary(
                CommandDisposition::OutcomeUnknown(
                    nervix_client_core::UnknownOutcomeCause::StillApplying
                ),
                "the command is still applying",
                Some(&reference),
            ),
            [
                "the command is still applying",
                "outcome: not known yet; the command was admitted, and retrying the same request \
                 (execution reference command-1) recovers it",
            ]
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
}
