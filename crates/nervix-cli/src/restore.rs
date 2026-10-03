//! Restores from the terminal: the `restore` subcommand, and `RESTORE` typed at the prompt.
//!
//! Layer: edges.
//!
//! - **Owns.** Turning the subcommand's arguments into a `RESTORE` statement, showing the archive's
//!   progress while it streams, and reporting the restore as text or JSON with the exit status its
//!   outcome calls for.
//! - **Depends on.** The client core, which reads the archive and runs the restore, and the
//!   vocabulary.
//! - **Must not know.** How the server stages, verifies, plans or applies a restore.
//!
//! The report goes to standard output and progress to standard error, only when standard error is
//! a terminal. Every restore that did not complete ends the process with a nonzero status.

use std::io::{IsTerminal as _, Write as _};

use error_stack::Report as StackReport;
use meticulous::ResultExt as _;
use nervix_client_core::{
    Client, CommandOutcome, ConnectOptions, ExistingUserPolicy, Restore, RestoreMode,
    RestoreReport, RestoreScope, RestoreStepOutcome,
};
use nervix_models::{DomainName, RestoreState, Statement};
use nervix_nspl::client_statement::{ClientStatement, parse_client_statements};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use nervix_recovery::{Discarded as _, Reported as _};
use serde_json::{Value, json};

use super::{ClientError, backup::CliReportFormat, human_bytes};

/// What a restore recreates, as the subcommand names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
pub(super) enum CliRestoreScope {
    /// Every domain and user of a cluster archive.
    Cluster,
    /// One domain of an archive.
    Domain,
}

/// What a cluster restore does with an archived user the cluster already has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
pub(super) enum CliExistingUsers {
    /// Refuse the restore before it changes anything.
    Fail,
    /// Keep the existing user and its password.
    Skip,
    /// Give the existing user the archived password hash.
    Replace,
}

impl From<CliExistingUsers> for ExistingUserPolicy {
    fn from(policy: CliExistingUsers) -> Self {
        match policy {
            CliExistingUsers::Fail => Self::Fail,
            CliExistingUsers::Skip => Self::Skip,
            CliExistingUsers::Replace => Self::Replace,
        }
    }
}

/// The arguments of one `restore` subcommand.
pub(super) struct RestoreRequest {
    pub(super) server: String,
    pub(super) connect_options: ConnectOptions,
    pub(super) session_domain: DomainName,
    pub(super) scope: CliRestoreScope,
    pub(super) domain: Option<DomainName>,
    pub(super) target: Option<DomainName>,
    pub(super) input: String,
    pub(super) existing_users: Option<CliExistingUsers>,
    pub(super) dry_run: bool,
    pub(super) without_state: bool,
    pub(super) without_source_offsets: bool,
    pub(super) format: CliReportFormat,
}

impl RestoreRequest {
    /// The statement the arguments ask for, or why they ask for none.
    fn restore(&self) -> Result<Restore, &'static str> {
        let scope = match (self.scope, &self.domain) {
            (CliRestoreScope::Cluster, Some(_)) => {
                return Err("a cluster restore recreates every archived domain and names none");
            }
            (CliRestoreScope::Cluster, None) if self.target.is_some() => {
                return Err("--as renames the one domain of a domain restore");
            }
            (CliRestoreScope::Cluster, None) => RestoreScope::Cluster {
                existing_users: match self.existing_users {
                    Some(policy) => policy.into(),
                    None => ExistingUserPolicy::default(),
                },
            },
            (CliRestoreScope::Domain, None) => {
                return Err("a domain restore names the archived domain it recreates");
            }
            (CliRestoreScope::Domain, Some(_)) if self.existing_users.is_some() => {
                return Err("a domain restore imports no users, so it takes no user policy");
            }
            (CliRestoreScope::Domain, Some(domain)) => RestoreScope::Domain {
                domain: domain.clone(),
                target: self.target.clone(),
            },
        };
        let mode = if self.dry_run {
            RestoreMode::DryRun
        } else {
            RestoreMode::Apply
        };
        let state = if self.without_state {
            RestoreState::ConfigurationOnly
        } else if self.without_source_offsets {
            RestoreState::WithoutSourceOffsets
        } else {
            RestoreState::All
        };
        Ok(Restore {
            scope,
            source: self.input.clone(),
            mode,
            state,
        })
    }
}

/// Runs one restore and reports it.
pub(super) async fn run_restore(request: RestoreRequest) -> Result<(), StackReport<ClientError>> {
    let format = request.format;
    let restore = match request.restore() {
        Ok(restore) => restore,
        Err(reason) => {
            let error = ClientError::RestoreArguments { reason };
            report_failure(format, "INVALID_ARGUMENTS", &error.to_string(), None);
            return Err(StackReport::new(error));
        }
    };
    let client = match Client::connect_with_options(
        &request.server,
        Some(request.session_domain),
        request.connect_options,
    )
    .await
    {
        Ok(client) => client,
        Err(error) => {
            report_failure(format, "CONNECTION_FAILED", &error.to_string(), None);
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    let progress = ProgressLine::start(&restore.source);
    let restored = client.restore(&restore, progress.counter()).await;
    progress.finish().await;
    let outcome = match restored {
        Ok(outcome) => outcome,
        Err(error) => {
            report_failure(format, "RESTORE_FAILED", &error_chain(&error), None);
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    if !outcome.succeeded() {
        let (code, report) = match outcome.restore.as_deref() {
            Some(report) => (
                "RESTORE_INCOMPLETE",
                Some(report_json(&restore, &outcome, report)),
            ),
            None => ("RESTORE_REFUSED", None),
        };
        report_failure(format, code, &outcome.message, report);
        return Err(StackReport::new(ClientError::RestoreFailed {
            message: outcome.message,
        }));
    }
    match format {
        CliReportFormat::Text => println!("{}", report_text(&outcome)),
        CliReportFormat::Json => {
            let document = match outcome.restore.as_deref() {
                Some(report) => report_json(&restore, &outcome, report),
                None => json!({ "message": outcome.message }),
            };
            println!("{document}");
        }
    }
    Ok(())
}

/// The `RESTORE` statement `query` consists of, when it is one.
pub(super) fn restore_statement(query: &str) -> Option<Restore> {
    let Ok(statements) = parse_client_statements(query) else {
        return None;
    };
    let [ClientStatement::Server(Statement::Restore(restore))] = statements.as_slice() else {
        return None;
    };
    Some(restore.clone())
}

/// Runs a `RESTORE` typed at the prompt, showing its progress, and prints its report.
pub(super) async fn execute_restore_and_print(
    client: &Client,
    restore: &Restore,
) -> Result<(), StackReport<ClientError>> {
    let progress = ProgressLine::start(&restore.source);
    let restored = client.restore(restore, progress.counter()).await;
    progress.finish().await;
    let outcome = restored.map_err(|error| StackReport::new(ClientError::from(error)))?;
    if outcome.succeeded() {
        println!("{}", report_text(&outcome));
    } else {
        println!("error: {}", report_text(&outcome));
    }
    Ok(())
}

/// How much of an archive a restore has sent, and whether it is done sending.
struct ProgressState {
    sent: AtomicU64,
    finished: AtomicBool,
}

/// The progress of the archive a restore streams, drawn on standard error while it streams when
/// standard error is a terminal.
struct ProgressLine {
    state: Arc<ProgressState>,
    drawing: Option<nervix_primitives::task::JoinHandle<()>>,
}

impl ProgressLine {
    /// Starts drawing the progress of streaming the archive `source` names.
    fn start(source: &str) -> Self {
        let state = Arc::new(ProgressState {
            sent: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        });
        let mut drawing = None;
        if std::io::stderr().is_terminal() {
            let state = state.clone();
            let source = source.to_string();
            drawing = Some(nervix_primitives::task::spawn(async move {
                let mut interval =
                    nervix_primitives::time::interval(std::time::Duration::from_millis(200));
                loop {
                    interval.tick().await;
                    if state.finished.load(Ordering::Relaxed) {
                        break;
                    }
                    let bytes = state.sent.load(Ordering::Relaxed);
                    eprint!(
                        "\r\x1b[2Krestore from '{source}': {} sent",
                        human_bytes(bytes)
                    );
                    std::io::stderr()
                        .flush()
                        .discarded("the next tick redraws the progress line a failed flush left");
                }
            }));
        }
        Self { state, drawing }
    }

    /// What the restore tells each number of archive bytes it hands to the transport.
    fn counter(&self) -> impl Fn(u64) + Send + Sync + Clone + 'static {
        let state = self.state.clone();
        move |bytes| {
            state.sent.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Stops drawing and clears the line.
    async fn finish(self) {
        self.state.finished.store(true, Ordering::Relaxed);
        let Some(drawing) = self.drawing else {
            return;
        };
        // The task only draws the progress line and has been told to stop; losing its join tells
        // the operator nothing the restore's outcome does not.
        drawing.await.reported("drawing the restore progress line");
        eprint!("\r\x1b[2K");
    }
}

/// The outcome's message, followed by what became of each step.
fn report_text(outcome: &CommandOutcome) -> String {
    let mut text = outcome.message.clone();
    let Some(report) = outcome.restore.as_deref() else {
        return text;
    };
    for step in &report.steps {
        text.push_str(&format!(
            "\n- {}: {}",
            step_outcome_label(step.outcome),
            step.step
        ));
    }
    for diagnostic in &outcome.diagnostics {
        if diagnostic.message.starts_with("warning:") {
            text.push_str(&format!("\n- {}", diagnostic.message));
        }
    }
    text
}

fn step_outcome_label(outcome: RestoreStepOutcome) -> &'static str {
    match outcome {
        RestoreStepOutcome::Applied => "applied",
        RestoreStepOutcome::Planned => "planned",
        RestoreStepOutcome::Failed => "failed",
        RestoreStepOutcome::NotAttempted => "not attempted",
    }
}

/// The error and every cause behind it, as one line.
fn error_chain(error: &nervix_client_core::ClientError) -> String {
    let mut message = error.to_string();
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        message.push_str(": ");
        message.push_str(&current.to_string());
        cause = current.source();
    }
    message
}

/// Prints a failure: an error line, or a JSON error document that carries the report of a
/// restore that failed at a step.
fn report_failure(format: CliReportFormat, code: &str, message: &str, report: Option<Value>) {
    match format {
        CliReportFormat::Text => eprintln!("error: {message}"),
        CliReportFormat::Json => {
            let mut error = json!({ "code": code, "message": message });
            if let Some(report) = report {
                error["report"] = report;
            }
            println!("{}", json!({ "error": error }));
        }
    }
}

/// The report of a restore as one JSON document.
fn report_json(restore: &Restore, outcome: &CommandOutcome, report: &RestoreReport) -> Value {
    let execution_reference = match &outcome.execution_reference {
        Some(reference) => Value::String(reference.to_string()),
        None => Value::Null,
    };
    let users = match &report.users {
        Some(users) => json!({
            "created": users.created,
            "skipped": users.skipped,
            "replaced": users.replaced,
        }),
        None => Value::Null,
    };
    let mut domains = Vec::with_capacity(report.domains.len());
    for domain in &report.domains {
        let planned_models = match &domain.planned_models {
            Some(planned) => serde_json::to_value(planned)
                .assured("a transaction impact report serializes to JSON with string keys"),
            None => Value::Null,
        };
        domains.push(json!({
            "source": domain.source.as_str(),
            "domain": domain.domain.as_str(),
            "resource_versions": domain.resource_versions,
            "models": domain.models,
            "planned_models": planned_models,
        }));
    }
    let mut steps = Vec::with_capacity(report.steps.len());
    for step in &report.steps {
        steps.push(json!({
            "step": step.step.to_string(),
            "outcome": step.outcome.as_ref(),
        }));
    }
    json!({
        "execution_reference": execution_reference,
        "input": restore.source,
        "mode": report.mode.as_ref(),
        "message": outcome.message,
        "total_bytes": report.archive.total_bytes.get(),
        "blake3": report.archive.digest.to_string(),
        "captured_at": report.captured_at.to_string(),
        "users": users,
        "domains": domains,
        "steps": steps,
        "warnings": outcome.diagnostics.iter()
            .filter(|diagnostic| diagnostic.message.starts_with("warning:"))
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    fn domain(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test domain is a valid literal name")
    }

    fn request(scope: CliRestoreScope) -> RestoreRequest {
        RestoreRequest {
            server: "http://127.0.0.1:47391".to_string(),
            connect_options: ConnectOptions::default(),
            session_domain: domain("session"),
            scope,
            domain: None,
            target: None,
            input: "cluster.nvxb".to_string(),
            existing_users: None,
            dry_run: false,
            without_state: false,
            without_source_offsets: false,
            format: CliReportFormat::Json,
        }
    }

    #[test]
    fn a_cluster_restore_refuses_existing_users_unless_told_otherwise() {
        let restore = request(CliRestoreScope::Cluster)
            .restore()
            .assured("a cluster restore needs no other argument");
        assert_eq!(
            restore.scope,
            RestoreScope::Cluster {
                existing_users: ExistingUserPolicy::Fail
            }
        );
        assert_eq!(restore.mode, RestoreMode::Apply);
        assert_eq!(restore.source, "cluster.nvxb");

        let mut replacing = request(CliRestoreScope::Cluster);
        replacing.existing_users = Some(CliExistingUsers::Replace);
        replacing.dry_run = true;
        let restore = replacing
            .restore()
            .assured("a cluster restore takes a user policy");
        assert_eq!(
            restore.scope,
            RestoreScope::Cluster {
                existing_users: ExistingUserPolicy::Replace
            }
        );
        assert_eq!(restore.mode, RestoreMode::DryRun);
    }

    #[test]
    fn a_domain_restore_names_its_domain_and_may_rename_it() {
        let mut renamed = request(CliRestoreScope::Domain);
        renamed.domain = Some(domain("payments"));
        renamed.target = Some(domain("payments_copy"));
        let restore = renamed
            .restore()
            .assured("a domain restore names its domain");
        assert_eq!(
            restore.scope,
            RestoreScope::Domain {
                domain: domain("payments"),
                target: Some(domain("payments_copy")),
            }
        );
        assert_eq!(
            restore.to_canonical_nspl(),
            "RESTORE DOMAIN payments AS payments_copy FROM 'cluster.nvxb';"
        );
    }

    #[test]
    fn arguments_that_contradict_their_scope_are_refused() {
        let mut named_cluster = request(CliRestoreScope::Cluster);
        named_cluster.domain = Some(domain("payments"));
        assert!(named_cluster.restore().is_err());

        let mut renamed_cluster = request(CliRestoreScope::Cluster);
        renamed_cluster.target = Some(domain("copy"));
        assert!(renamed_cluster.restore().is_err());

        assert!(request(CliRestoreScope::Domain).restore().is_err());

        let mut domain_with_users = request(CliRestoreScope::Domain);
        domain_with_users.domain = Some(domain("payments"));
        domain_with_users.existing_users = Some(CliExistingUsers::Skip);
        assert!(domain_with_users.restore().is_err());
    }

    #[test]
    fn a_restore_typed_at_the_prompt_is_recognized_alone() {
        let restore = restore_statement("RESTORE CLUSTER FROM 'c.nvxb' ON EXISTING USER SKIP;")
            .assured("the prompt holds one RESTORE");
        assert_eq!(
            restore.scope,
            RestoreScope::Cluster {
                existing_users: ExistingUserPolicy::Skip
            }
        );
        assert!(restore_statement("LIST DOMAINS;").is_none());
        assert!(restore_statement("RESTORE CLUSTER FROM").is_none());
    }

    #[test]
    fn every_step_outcome_has_a_label() {
        assert_eq!(step_outcome_label(RestoreStepOutcome::Applied), "applied");
        assert_eq!(step_outcome_label(RestoreStepOutcome::Planned), "planned");
        assert_eq!(step_outcome_label(RestoreStepOutcome::Failed), "failed");
        assert_eq!(
            step_outcome_label(RestoreStepOutcome::NotAttempted),
            "not attempted"
        );
    }
}
