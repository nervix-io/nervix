//! The interactive terminal client for Nervix.
//!
//! Layer: edges.
//!
//! - **Owns.** The REPL: key bindings, the completion menu, rendered diagnostics, output formatting
//!   and the shell-facing command surface.
//! - **Depends on.** `nervix-client-core`, the language layer for completion and local statement
//!   parsing, and the vocabulary.
//! - **Must not know.** The server. It speaks the session API through the client core and nothing
//!   else.

use std::{
    io::{self, Write},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arch_into::ArchInto as _;
use ariadne::{Color, Config, IndexType, Label, Report, ReportKind, Source};
use byte_unit::{Byte, UnitType};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use error_stack::Report as StackReport;
use nervix_client_core::{
    AutocompleteSuggestion, Client, ClientError as CoreClientError, CommandDisposition,
    CommandExecutionReference, CommandOutcome, ConnectOptions, Diagnostic, DomainName,
    LeaderRedirect, NoticeLevel, ServerEvent, SourceSpan, StatementDisposition, StatementOutcome,
    SubscriptionDeliveryBehavior, SubscriptionEvent, SubscriptionRequest,
    SuggestionKind as ClientSuggestionKind, TlsRequirement, TransactionLifecycle,
    TransactionStatus,
};
use nervix_models::{ClusterNodeName, Statement, TransactionReportFormat};
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statements, parse_upload_resource_query,
    upload_resource_path_fragment,
};
use nervix_recovery::{Discarded as _, NoReceiver as _, Reported as _};
use reedline::{
    Completer, DefaultHinter, DefaultPrompt, DefaultPromptSegment, Emacs, FileBackedHistory,
    KeyCode, KeyModifiers, ListMenu, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
    Suggestion,
};
use thiserror::Error;
use tokio::{runtime::Handle, signal, task::block_in_place};
use triomphe::Arc;

const HISTORY_FILE: &str = ".nervix_client_history";

#[derive(Parser, Debug, Clone)]
#[command(name = "nervix-cli")]
#[command(about = "Interactive Nervix client")]
struct Args {
    /// Session gRPC endpoint; an https:// URL connects over TLS
    #[arg(long, default_value = "http://127.0.0.1:47391")]
    server: String,
    /// Whether TLS is required; `required` refuses a server URL that is not https
    #[arg(long, value_enum, default_value_t = CliTlsRequirement::Preferred)]
    tls: CliTlsRequirement,
    /// PEM certificate authority used to verify the server certificate
    #[arg(long)]
    tls_ca_cert: Option<PathBuf>,
    /// Domain the session starts in
    #[arg(long, default_value = "default")]
    domain: DomainName,
    /// Registry user to authenticate as
    #[arg(long, env = "NERVIX_USERNAME", default_value = "default")]
    username: String,
    /// Password for the registry user; prompted for interactively when unset
    #[arg(long, env = "NERVIX_PASSWORD")]
    password: Option<String>,
    /// Run NSPL statements once and exit instead of starting the interactive REPL
    #[arg(long)]
    command: Option<String>,
    #[command(subcommand)]
    subcommand: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Generate shell completion scripts
    Completions {
        /// Target shell
        shell: Shell,
    },
    /// Subscribe to one relay and print events until interrupted
    Subscribe {
        /// Session-local subscription name
        name: String,
        /// Relay name to subscribe to
        relay: String,
        /// Drop delivered events when the session transport queue is full
        #[arg(long, conflicts_with = "blocking")]
        dropping: bool,
        /// Block delivered events when the session transport queue is full
        #[arg(long, conflicts_with = "dropping")]
        blocking: bool,
        /// Optional per-arrival batch sample rate from 0.0 through 1.0
        #[arg(long)]
        batch_sample_rate: Option<String>,
        /// Optional NSPL predicate applied to delivered records
        #[arg(long = "where")]
        where_clause: Option<String>,
    },
    /// Remove a node from the cluster membership
    RemoveNode {
        /// Node id to remove
        node_id: ClusterNodeName,
    },
    /// Prevent the scheduler from placing new tasks on a node
    CordonNode {
        /// Node id to cordon
        node_id: ClusterNodeName,
    },
    /// Allow the scheduler to place new tasks on a node
    UncordonNode {
        /// Node id to uncordon
        node_id: ClusterNodeName,
    },
    /// Move scheduled graph nodes away from a node and keep it cordoned
    DrainNode {
        /// Node id to drain
        node_id: ClusterNodeName,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "lower")]
enum CliTlsRequirement {
    Preferred,
    Required,
}

#[derive(Clone)]
struct GrpcCompleter {
    runtime: Handle,
    client: Client,
    buffer_prefix: Arc<StdMutex<String>>,
}

#[derive(Debug, Error)]
enum ClientError {
    #[error(transparent)]
    Core(#[from] CoreClientError),
    #[error("failed to initialize history")]
    InitHistory,
    #[error("failed to read user input")]
    ReadLine,
    #[error("failed to read password")]
    ReadPassword,
    #[error("invalid subscription WHERE expression: {reason}")]
    InvalidSubscriptionWhere { reason: String },
    #[error("transaction inspection failed: {message}")]
    InspectionFailed { message: String },
}

impl Completer for GrpcCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = match self.buffer_prefix.lock() {
            Ok(prefix) => prefix.clone(),
            Err(_) => String::new(),
        };
        let combined = format!("{}{}", prefix, &line[..pos.min(line.len())]);
        let cursor = combined.len();
        let client = self.client.clone();
        let runtime = self.runtime.clone();

        let suggestions = block_in_place(|| {
            runtime.block_on(async move { client.suggest(combined, cursor).await.ok() })
        })
        .unwrap_or_default();

        let start = word_start(line, pos);
        if suggestions
            .iter()
            .any(|suggestion| suggestion.kind == ClientSuggestionKind::LocalDirectoryLookup)
        {
            let lookup_hint = suggestions
                .iter()
                .find(|suggestion| suggestion.kind == ClientSuggestionKind::LocalDirectoryLookup);
            if let Some(local) = complete_local_upload_paths(line, pos, lookup_hint) {
                return local;
            }
        }

        suggestions
            .into_iter()
            .filter(|suggestion| suggestion.kind == ClientSuggestionKind::Text)
            .map(|suggestion| Suggestion {
                value: suggestion.value,
                description: None,
                style: None,
                extra: None,
                span: reedline::Span::new(start, pos),
                append_whitespace: true,
            })
            .collect()
    }
}

fn word_start(line: &str, pos: usize) -> usize {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let boundary = line[..pos.min(line.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c));
    match boundary {
        Some((index, character)) => index + character.len_utf8(),
        None => 0,
    }
}

#[tokio::main]
async fn main() -> Result<(), StackReport<ClientError>> {
    let args = Args::parse();
    match args.subcommand.clone() {
        Some(Command::Completions { shell }) => {
            print_completions(shell);
            return Ok(());
        }
        Some(Command::Subscribe {
            name,
            relay,
            dropping,
            blocking: _,
            batch_sample_rate,
            where_clause,
        }) => {
            let connect_options = connect_options_from_args(&args)?;
            return run_subscribe_mode(SubscribeModeOptions {
                server: args.server,
                connect_options,
                domain: args.domain,
                name,
                relay,
                delivery_behavior: if dropping {
                    SubscriptionDeliveryBehavior::Dropping
                } else {
                    SubscriptionDeliveryBehavior::Blocking
                },
                batch_sample_rate,
                where_clause,
            })
            .await;
        }
        Some(Command::RemoveNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client = Client::connect_with_options(
                &args.server,
                Some(args.domain.clone()),
                connect_options,
            )
            .await
            .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("DROP NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::CordonNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client = Client::connect_with_options(
                &args.server,
                Some(args.domain.clone()),
                connect_options,
            )
            .await
            .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("CORDON NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::UncordonNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client = Client::connect_with_options(
                &args.server,
                Some(args.domain.clone()),
                connect_options,
            )
            .await
            .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("UNCORDON NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::DrainNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client = Client::connect_with_options(
                &args.server,
                Some(args.domain.clone()),
                connect_options,
            )
            .await
            .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("DRAIN NODE {node_id};")).await?;
            return Ok(());
        }
        None => {}
    }

    if let Some(command) = args.command.as_deref()
        && is_json_inspection_command(command)
    {
        return run_json_inspection_mode(&args, command).await;
    }

    let connect_options = connect_options_from_args(&args)?;
    let client =
        Client::connect_with_options(&args.server, Some(args.domain.clone()), connect_options)
            .await
            .map_err(|err| StackReport::new(ClientError::from(err)))?;
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(128);
    spawn_event_collectors(client.clone(), event_sender);
    if let Some(command) = args.command {
        execute_and_print(&client, command).await?;
        return Ok(());
    }

    let buffer_prefix = Arc::new(StdMutex::new(String::new()));

    let completer = GrpcCompleter {
        runtime: Handle::current(),
        client: client.clone(),
        buffer_prefix: buffer_prefix.clone(),
    };

    let mut buffer = String::new();
    println!("nervix-cli connected to {}", args.server);
    println!("Type 'exit' to quit. Trailing ';' is optional.");
    println!("[events] notifications are printed above the prompt");

    loop {
        let prompt_domain = prompt_domain(
            client.domain().await.as_ref(),
            client.transaction_status().await.as_ref(),
        );
        drain_event_queue(&mut event_receiver);
        let prompt = if buffer.is_empty() {
            DefaultPrompt::new(
                DefaultPromptSegment::Basic(format!("nervix[{prompt_domain}]")),
                DefaultPromptSegment::Empty,
            )
        } else {
            DefaultPrompt::new(
                DefaultPromptSegment::Basic(format!("....[{prompt_domain}]")),
                DefaultPromptSegment::Empty,
            )
        };

        if let Ok(mut guard) = buffer_prefix.lock() {
            *guard = buffer.clone();
        }

        let mut line_editor = create_line_editor(completer.clone())?;

        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let trimmed = line.trim();
                if buffer.is_empty()
                    && (trimmed.eq_ignore_ascii_case("exit")
                        || trimmed.eq_ignore_ascii_case("quit"))
                {
                    break;
                }

                if trimmed.is_empty() {
                    continue;
                }

                buffer.push_str(&line);
                buffer.push('\n');

                if command_buffer_is_complete(&buffer) {
                    let payload = std::mem::take(&mut buffer);
                    execute_and_print(&client, payload).await?;
                    drain_event_queue(&mut event_receiver);
                }
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => break,
            Err(err) => {
                return Err(StackReport::new(ClientError::ReadLine)
                    .attach_printable(format!("readline failed: {err}")));
            }
        }
    }

    Ok(())
}

/// A one-shot JSON inspection reserves stdout for exactly one machine-readable document.
fn is_json_inspection_command(query: &str) -> bool {
    let Ok(statements) = parse_client_statements(query) else {
        return false;
    };
    let [ClientStatement::Server(Statement::DescribeTransaction(describe))] = statements.as_slice()
    else {
        return false;
    };
    describe.format == TransactionReportFormat::Json
}

async fn run_json_inspection_mode(
    args: &Args,
    query: &str,
) -> Result<(), StackReport<ClientError>> {
    let options = match connect_options_from_args(args) {
        Ok(options) => options,
        Err(error) => {
            print_json_inspection_error("CLIENT_CONFIGURATION", &error.to_string());
            return Err(error);
        }
    };
    let client = match Client::connect_with_options(
        &args.server,
        Some(args.domain.clone()),
        options,
    )
    .await
    {
        Ok(client) => client,
        Err(error) => {
            print_json_inspection_error("CONNECTION_FAILED", &error.to_string());
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    spawn_event_loggers(client.clone(), EventOutput::Stderr);
    let outcome = match client.execute(query).await {
        Ok(outcome) => outcome,
        Err(error) => {
            print_json_inspection_error("REQUEST_FAILED", &error.to_string());
            return Err(StackReport::new(ClientError::from(error)));
        }
    };
    if !outcome.succeeded() {
        print_json_inspection_error("INSPECTION_REFUSED", &outcome.message);
        return Err(StackReport::new(ClientError::InspectionFailed {
            message: outcome.message,
        }));
    }
    if outcome.inspection.is_none() {
        let message = "the inspection response contained no typed report";
        print_json_inspection_error("REPORT_MISSING", message);
        return Err(StackReport::new(ClientError::InspectionFailed {
            message: message.to_string(),
        }));
    }
    println!("{}", outcome.message);
    Ok(())
}

fn print_json_inspection_error(code: &str, message: &str) {
    println!(
        "{}",
        serde_json::json!({ "error": { "code": code, "message": message } })
    );
}

fn create_line_editor(completer: GrpcCompleter) -> Result<Reedline, StackReport<ClientError>> {
    let history = Box::new(
        FileBackedHistory::with_file(200, HISTORY_FILE.into())
            .map_err(|_| StackReport::new(ClientError::InitHistory))?,
    );
    let completion_menu = ListMenu::default()
        .with_name("completion_menu")
        .with_only_buffer_difference(false);
    let mut keybindings = reedline::default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );
    let edit_mode = Box::new(Emacs::new(keybindings));
    let hinter = Box::new(DefaultHinter::default());
    Ok(Reedline::create()
        .with_history(history)
        .with_hinter(hinter)
        .with_completer(Box::new(completer))
        .with_menu(ReedlineMenu::EngineCompleter(Box::new(completion_menu)))
        .with_edit_mode(edit_mode))
}

fn complete_local_upload_paths(
    line: &str,
    pos: usize,
    lookup_hint: Option<&AutocompleteSuggestion>,
) -> Option<Vec<Suggestion>> {
    let hinted = match lookup_hint {
        Some(hint)
            if !hint.value.is_empty() || line[..pos.min(line.len())].contains(" VERSION '") =>
        {
            Some(hint.value.as_str())
        }
        _ => None,
    };
    let path_fragment = match hinted {
        Some(path_fragment) => path_fragment,
        None => upload_resource_path_fragment(line, pos)?,
    };
    // The suggested fragment may be longer than the text typed so far, in which case the
    // replacement span starts at the beginning of the line.
    let span_start = pos.saturating_sub(path_fragment.len());
    let path = Path::new(path_fragment);
    let (base_dir, partial_name) = if path_fragment.is_empty() {
        (PathBuf::from("."), String::new())
    } else if path_fragment.ends_with(std::path::MAIN_SEPARATOR) || path_fragment.ends_with('/') {
        (expand_user_path(path), String::new())
    } else {
        let parent = match path.parent() {
            Some(parent) => parent.to_path_buf(),
            None => PathBuf::from("."),
        };
        let file_name = match path.file_name() {
            Some(name) => name.to_string_lossy().to_string(),
            None => String::new(),
        };
        (expand_user_path(parent), file_name)
    };
    let Ok(entries) = std::fs::read_dir(&base_dir) else {
        return None;
    };
    let mut suggestions = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().to_string();
        if !partial_name.is_empty() && !name.starts_with(&partial_name) {
            continue;
        }
        let value = if uses_home_prefix(path_fragment) {
            let Some(relative_base) = strip_home_prefix(&base_dir) else {
                continue;
            };
            if relative_base.as_os_str().is_empty() {
                format!("~/{name}")
            } else {
                format!("~/{}/{}", relative_base.display(), name)
            }
        } else if base_dir == Path::new(".") {
            name.clone()
        } else {
            base_dir.join(&name).display().to_string()
        };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        suggestions.push(Suggestion {
            value: if file_type.is_dir() {
                format!("{value}/")
            } else {
                value
            },
            description: None,
            style: None,
            extra: None,
            span: reedline::Span::new(span_start, pos),
            append_whitespace: false,
        });
    }
    suggestions.sort_by(|left, right| left.value.cmp(&right.value));
    Some(suggestions)
}

fn expand_user_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    if raw == "~" {
        return match std::env::var_os("HOME") {
            Some(home) => PathBuf::from(home),
            None => path.to_path_buf(),
        };
    }
    if let Some(stripped) = raw.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(stripped);
    }
    path.to_path_buf()
}

fn uses_home_prefix(path_fragment: &str) -> bool {
    path_fragment == "~" || path_fragment.starts_with("~/")
}

fn strip_home_prefix(path: &Path) -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    path.strip_prefix(home).ok().map(Path::to_path_buf)
}

struct SubscribeModeOptions {
    server: String,
    connect_options: ConnectOptions,
    domain: DomainName,
    name: String,
    relay: String,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<String>,
    where_clause: Option<String>,
}

async fn run_subscribe_mode(options: SubscribeModeOptions) -> Result<(), StackReport<ClientError>> {
    let client = Client::connect_with_options(
        &options.server,
        Some(options.domain),
        options.connect_options,
    )
    .await
    .map_err(|err| StackReport::new(ClientError::from(err)))?;
    spawn_event_loggers(client.clone(), EventOutput::Stdout);
    let request = subscribe_request(
        &options.name,
        &options.relay,
        options.delivery_behavior,
        options.batch_sample_rate.as_deref(),
        options.where_clause.as_deref(),
    )
    .map_err(|reason| StackReport::new(ClientError::InvalidSubscriptionWhere { reason }))?;
    let query = request.to_query();
    let result = client
        .subscribe(&request)
        .await
        .map_err(|err| StackReport::new(ClientError::from(err)))?;
    if !result.succeeded() {
        println!("error: {}", result.message);
        if result.diagnostics.is_empty() {
            println!("- no diagnostics provided");
        } else {
            print_diagnostics("subscribe", &query, &result.diagnostics);
        }
        return Ok(());
    }

    println!("{}", result.message);
    println!(
        "listening for events from relay '{}'. Press Ctrl-C to stop.",
        options.relay
    );
    signal::ctrl_c()
        .await
        .map_err(|_| StackReport::new(ClientError::from(CoreClientError::SessionClosed)))?;
    Ok(())
}

fn command_buffer_is_complete(buffer: &str) -> bool {
    if parse_client_statements(buffer).is_ok() {
        return true;
    }
    buffer.trim_end().ends_with(';')
}

fn print_completions(shell: Shell) {
    let mut command = Args::command();
    let bin_name = command.get_name().to_string();
    generate(shell, &mut command, bin_name, &mut std::io::stdout());
}

fn connect_options_from_args(args: &Args) -> Result<ConnectOptions, StackReport<ClientError>> {
    let ca_certificate_pem = match args.tls_ca_cert.as_ref() {
        Some(path) => Some(
            std::fs::read(path)
                .map_err(CoreClientError::LoadTlsCaCertificate)
                .map_err(ClientError::from)
                .map_err(StackReport::new)?,
        ),
        None => None,
    };
    let password = match args.password.clone() {
        Some(password) => password,
        None => rpassword::prompt_password(format!("Password for {}: ", args.username))
            .map_err(|_| StackReport::new(ClientError::ReadPassword))?,
    };
    Ok(ConnectOptions {
        tls_requirement: Some(match args.tls {
            CliTlsRequirement::Preferred => TlsRequirement::Preferred,
            CliTlsRequirement::Required => TlsRequirement::Required,
        }),
        ca_certificate_pem,
        username: Some(args.username.clone()),
        password: Some(password),
        ..ConnectOptions::default()
    })
}

async fn execute_and_print(client: &Client, query: String) -> Result<(), StackReport<ClientError>> {
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
        .map_err(|err| StackReport::new(ClientError::from(err)))?;
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
    let progress_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(120));
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
    let result = outcome.map_err(|err| StackReport::new(ClientError::from(err)))?;
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

fn human_bytes(bytes: u64) -> String {
    let adjusted = Byte::from_u64(bytes).get_appropriate_unit(UnitType::Binary);
    format!("{adjusted:.1}")
}

fn spawn_event_collectors(client: Client, sender: tokio::sync::mpsc::Sender<String>) {
    let subscription_client = client.clone();
    let subscription_sender = sender.clone();
    tokio::spawn(async move {
        while let Ok(event) = subscription_client.next_subscription().await {
            tokio::task::consume_budget().await;
            for line in format_subscription_event(&event) {
                subscription_sender
                    .send(line)
                    .await
                    .means_shutdown("terminal event printer");
            }
        }
    });

    tokio::spawn(async move {
        while let Ok(event) = client.next_server_event().await {
            tokio::task::consume_budget().await;
            sender
                .send(format_server_event(&event))
                .await
                .means_shutdown("terminal event printer");
        }
    });
}

#[derive(Clone, Copy)]
enum EventOutput {
    Stdout,
    Stderr,
}

impl EventOutput {
    fn print(self, line: &str) {
        match self {
            Self::Stdout => println!("{line}"),
            Self::Stderr => eprintln!("{line}"),
        }
    }
}

fn spawn_event_loggers(client: Client, output: EventOutput) {
    let subscription_client = client.clone();
    tokio::spawn(async move {
        while let Ok(event) = subscription_client.next_subscription().await {
            tokio::task::consume_budget().await;
            for line in format_subscription_event(&event) {
                output.print(&line);
            }
        }
    });

    tokio::spawn(async move {
        while let Ok(event) = client.next_server_event().await {
            tokio::task::consume_budget().await;
            output.print(&format_server_event(&event));
        }
    });
}

fn drain_event_queue(receiver: &mut tokio::sync::mpsc::Receiver<String>) {
    while let Ok(line) = receiver.try_recv() {
        println!("{line}");
    }
}

/// The terminal lines of one subscription event: one line per row of a batch, or one notice.
fn format_subscription_event(event: &SubscriptionEvent) -> Vec<String> {
    let subscription = &event.subscription().name;
    match event {
        SubscriptionEvent::Rows(rows) => {
            let prefix = format!(
                "[events] subscription [{subscription}] from [{}]",
                rows.relay
            );
            match rows.display_lines() {
                Ok(lines) => lines
                    .into_iter()
                    .map(|line| format!("{prefix}: {line}"))
                    .collect(),
                Err(error) => vec![format!(
                    "{prefix}: rows do not match the subscription schema: {}",
                    error.current_context()
                )],
            }
        }
        SubscriptionEvent::DeliveryLost(lost) => vec![format!(
            "[events] subscription [{subscription}] notice: {} rows were dropped because the \
             session could not take them in time",
            lost.dropped_rows
        )],
        SubscriptionEvent::RowsSkipped(skipped) => vec![format!(
            "[events] subscription [{subscription}] notice: {} rows were skipped ({:?}): {}",
            skipped.skipped_rows, skipped.cause, skipped.message
        )],
        SubscriptionEvent::Ended(ended) => vec![format!(
            "[events] subscription [{subscription}] notice: the subscription ended: {}",
            ended.message
        )],
        SubscriptionEvent::Interrupted(_) => vec![format!(
            "[events] subscription [{subscription}] notice: delivery was interrupted; rows may be \
             missing before restoration"
        )],
        SubscriptionEvent::ConsumerOverflow(_) => vec![format!(
            "[events] subscription [{subscription}] notice: the client event buffer filled; \
             delivery ended with a gap"
        )],
    }
}

fn format_server_event(event: &ServerEvent) -> String {
    let label = if event.message.starts_with("raft transition:") {
        "topology"
    } else {
        "server"
    };
    format!(
        "[events] {} {}: {}",
        label,
        notice_level_label(event.level),
        event.message
    )
}

/// The label a server notice's level prints with.
fn notice_level_label(level: NoticeLevel) -> &'static str {
    match level {
        NoticeLevel::Info => "INFO",
        NoticeLevel::Warning => "WARN",
        NoticeLevel::Error => "ERROR",
    }
}

/// The prompt's domain segment: the session's domain, and the state of its transaction while one
/// is active.
fn prompt_domain(domain: Option<&DomainName>, transaction: Option<&TransactionStatus>) -> String {
    let domain = match domain {
        Some(domain) => domain.to_string(),
        None => "no domain".to_string(),
    };
    let lifecycle = transaction.map(TransactionStatus::lifecycle);
    match lifecycle {
        Some(TransactionLifecycle::Open) => format!("{domain} tx"),
        Some(TransactionLifecycle::Committing) => format!("{domain} committing"),
        _ => domain,
    }
}

fn subscribe_request(
    name: &str,
    relay: &str,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<&str>,
    where_clause: Option<&str>,
) -> Result<SubscriptionRequest, String> {
    let request = match delivery_behavior {
        SubscriptionDeliveryBehavior::Blocking => SubscriptionRequest::new(name, relay).blocking(),
        SubscriptionDeliveryBehavior::Dropping => SubscriptionRequest::new(name, relay).dropping(),
    };
    let request = match batch_sample_rate {
        Some(batch_sample_rate) => request.with_batch_sample_rate(batch_sample_rate),
        None => request,
    };
    where_clause
        .map(nervix_nspl::parse_expression)
        .transpose()
        .map_err(|error| format!("invalid subscription WHERE expression: {error:?}"))
        .map(|where_clause| match where_clause {
            Some(where_clause) => request.with_where_clause(where_clause),
            None => request,
        })
}

fn print_diagnostics(source_id: &str, source: &str, diagnostics: &[Diagnostic]) {
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
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn one_shot_json_mode_requires_one_complete_inspection_statement() {
        assert!(is_json_inspection_command(
            "DESCRIBE TRANSACTION 'tx-1' OPERATION 1 FORMAT JSON;"
        ));
        assert!(!is_json_inspection_command("DESCRIBE TRANSACTION;"));
        assert!(!is_json_inspection_command(
            "DESCRIBE TRANSACTION FORMAT JSON; SHOW TRANSACTIONS;"
        ));
        assert!(!is_json_inspection_command(
            "DESCRIBE TRANSACTION FORMAT JSON; ???"
        ));
    }

    #[test]
    fn args_defaults_are_applied_without_subcommand() {
        let args = Args::parse_from(["nervix-cli"]);
        assert_eq!(args.server, "http://127.0.0.1:47391");
        assert_eq!(args.tls, CliTlsRequirement::Preferred);
        assert_eq!(args.tls_ca_cert, None);
        assert_eq!(args.domain.as_str(), "default");
        assert_eq!(args.command, None);
        assert!(args.subcommand.is_none());
    }

    #[test]
    fn args_parse_custom_server_domain_and_command() {
        let args = Args::parse_from([
            "nervix-cli",
            "--server",
            "http://localhost:9999",
            "--tls",
            "required",
            "--tls-ca-cert",
            "/tmp/ca.pem",
            "--domain",
            "tenant_a",
            "--command",
            "SHOW CLUSTER STATUS;",
        ]);
        assert_eq!(args.server, "http://localhost:9999");
        assert_eq!(args.tls, CliTlsRequirement::Required);
        assert_eq!(args.tls_ca_cert, Some(PathBuf::from("/tmp/ca.pem")));
        assert_eq!(args.domain.as_str(), "tenant_a");
        assert_eq!(args.command.as_deref(), Some("SHOW CLUSTER STATUS;"));
    }

    #[test]
    fn completions_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "completions", "bash"]);
        match args.subcommand {
            Some(Command::Completions { shell }) => assert!(matches!(shell, Shell::Bash)),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn subscribe_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "subscribe", "live_events", "events"]);
        match args.subcommand {
            Some(Command::Subscribe {
                name,
                relay,
                dropping,
                blocking,
                batch_sample_rate,
                where_clause,
            }) => {
                assert_eq!(name, "live_events");
                assert_eq!(relay, "events");
                assert!(!dropping);
                assert!(!blocking);
                assert_eq!(batch_sample_rate, None);
                assert_eq!(where_clause, None);
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn remove_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "remove-node", "node-2"]);
        match args.subcommand {
            Some(Command::RemoveNode { node_id }) => assert_eq!(
                node_id,
                ClusterNodeName::parse("node-2").expect("valid name")
            ),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn cordon_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "cordon-node", "node-2"]);
        match args.subcommand {
            Some(Command::CordonNode { node_id }) => assert_eq!(
                node_id,
                ClusterNodeName::parse("node-2").expect("valid name")
            ),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn uncordon_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "uncordon-node", "node-2"]);
        match args.subcommand {
            Some(Command::UncordonNode { node_id }) => assert_eq!(
                node_id,
                ClusterNodeName::parse("node-2").expect("valid name")
            ),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn drain_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "drain-node", "node-2"]);
        match args.subcommand {
            Some(Command::DrainNode { node_id }) => assert_eq!(
                node_id,
                ClusterNodeName::parse("node-2").expect("valid name")
            ),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn subscribe_query_uses_named_form() {
        assert_eq!(
            subscribe_request(
                "live_myss",
                "myss",
                SubscriptionDeliveryBehavior::Blocking,
                None,
                None
            )
            .expect("subscription request should build")
            .to_query(),
            "CREATE SUBSCRIPTION live_myss TO myss;"
        );
        assert_eq!(
            subscribe_request(
                "live_myss",
                "myss",
                SubscriptionDeliveryBehavior::Blocking,
                None,
                Some("input.tenant = \"acme\"")
            )
            .expect("subscription request should build")
            .to_query(),
            "CREATE SUBSCRIPTION live_myss TO myss WHERE input.tenant = 'acme';"
        );
        assert_eq!(
            subscribe_request(
                "sampled_myss",
                "myss",
                SubscriptionDeliveryBehavior::Dropping,
                Some("0.1"),
                Some("input.tenant = \"acme\"")
            )
            .expect("subscription request should build")
            .to_query(),
            "CREATE SUBSCRIPTION sampled_myss TO myss DROPPING BATCH SAMPLE RATE 0.1 WHERE \
             input.tenant = 'acme';"
        );
    }

    #[test]
    fn word_start_tracks_identifier_boundaries() {
        assert_eq!(word_start("CREATE SCHE", "CREATE SCHE".len()), 7);
        assert_eq!(word_start("tenant_id", "tenant_id".len()), 0);
        assert_eq!(word_start("WHERE (tenant", "WHERE (tenant".len()), 7);
        assert_eq!(word_start("tenant", usize::MAX), 0);
    }

    #[test]
    fn use_domain_parser_accepts_repl_command() {
        use nervix_nspl::client_statement::parse_use_domain;

        assert_eq!(
            parse_use_domain("USE prod;")
                .expect("parse should succeed")
                .as_str(),
            "prod"
        );
        assert_eq!(
            parse_use_domain(" use tenant_a ; ")
                .expect("parse should succeed")
                .as_str(),
            "tenant_a"
        );
        assert!(parse_use_domain("SHOW CLUSTER STATUS;").is_err());
        assert!(parse_use_domain("USE two words;").is_err());
    }

    #[test]
    fn command_buffer_is_complete_without_trailing_semicolon() {
        assert!(command_buffer_is_complete("CREATE DOMAIN default\n"));
        assert!(command_buffer_is_complete(
            "CREATE DOMAIN default; CREATE SCHEMA notification ( user_id U32 )\n"
        ));
        assert!(command_buffer_is_complete(
            "CREATE CLIENT http_main TYPE HTTP CONFIG { 'url' = 'http://example.com/a;b' }\n"
        ));
    }

    #[test]
    fn command_buffer_waits_for_incomplete_multiline_statement() {
        assert!(!command_buffer_is_complete(
            "CREATE SCHEMA notification (\n"
        ));
        assert!(command_buffer_is_complete(
            "CREATE SCHEMA notification (\nuser_id U32\n)\n"
        ));
    }

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
    fn local_upload_path_completion_lists_matching_directories() {
        let temp =
            std::env::temp_dir().join(format!("nervix-cli-upload-complete-{}", std::process::id()));
        if temp.exists() {
            std::fs::remove_dir_all(&temp).expect("old temp dir should be removed");
        }
        std::fs::create_dir_all(temp.join("proto-dir")).expect("fixture dir");
        std::fs::create_dir_all(temp.join("other-dir")).expect("fixture dir");
        let line = format!("UPLOAD RESOURCE proto VERSION '{}/pro", temp.display());
        let suggestions = complete_local_upload_paths(
            &line,
            line.len(),
            Some(&AutocompleteSuggestion {
                value: format!("{}/pro", temp.display()),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
            }),
        )
        .expect("local path completion should be available");
        assert!(suggestions.iter().any(|suggestion| {
            suggestion.value.contains("proto-dir") && suggestion.value.ends_with('/')
        }));
        assert!(
            !suggestions
                .iter()
                .any(|suggestion| suggestion.value.contains("other-dir"))
        );
        std::fs::remove_dir_all(&temp).expect("temp dir should be removed");
    }

    #[test]
    fn local_upload_path_completion_does_not_introduce_double_slashes() {
        let temp = std::env::temp_dir().join(format!(
            "nervix-cli-upload-double-slash-{}",
            std::process::id()
        ));
        if temp.exists() {
            std::fs::remove_dir_all(&temp).expect("old temp dir should be removed");
        }
        std::fs::create_dir_all(temp.join("proto-dir")).expect("fixture dir");
        let suggestions = complete_local_upload_paths(
            "",
            0,
            Some(&AutocompleteSuggestion {
                value: format!("{}/", temp.display()),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
            }),
        )
        .expect("local path completion should be available");
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.value == format!("{}/proto-dir/", temp.display()))
        );
        std::fs::remove_dir_all(&temp).expect("temp dir should be removed");
    }

    #[test]
    fn local_upload_path_completion_expands_tilde_and_preserves_user_facing_prefix() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            return;
        };
        let temp = home.join(format!("nervix-cli-upload-home-{}", std::process::id()));
        if temp.exists() {
            std::fs::remove_dir_all(&temp).expect("old temp dir should be removed");
        }
        std::fs::create_dir_all(temp.join("proto-dir")).expect("fixture dir");
        let basename = temp
            .file_name()
            .expect("basename should exist")
            .to_string_lossy()
            .to_string();
        let suggestions = complete_local_upload_paths(
            "",
            0,
            Some(&AutocompleteSuggestion {
                value: format!("~/{basename}"),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
            }),
        )
        .expect("local path completion should be available");
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.value == format!("~/{basename}/"))
        );
        let nested_suggestions = complete_local_upload_paths(
            "",
            0,
            Some(&AutocompleteSuggestion {
                value: format!("~/{basename}/"),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
            }),
        )
        .expect("nested local path completion should be available");
        assert!(
            nested_suggestions
                .iter()
                .any(|suggestion| suggestion.value == format!("~/{basename}/proto-dir/"))
        );
        std::fs::remove_dir_all(&temp).expect("temp dir should be removed");
    }

    #[test]
    fn human_bytes_uses_human_units() {
        assert_eq!(human_bytes(999), "999 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
    }

    #[test]
    fn subscribe_request_targets_requested_stream() {
        let request = subscribe_request(
            "live_events",
            "events",
            SubscriptionDeliveryBehavior::Blocking,
            None,
            None,
        )
        .expect("subscription request should build");
        assert_eq!(request.name, "live_events");
        assert_eq!(request.relay, "events");
        assert_eq!(request.where_clause, None);
    }

    #[test]
    fn subscribe_command_parses_where_clause() {
        let args = Args::parse_from([
            "nervix-cli",
            "subscribe",
            "live_events",
            "events",
            "--where",
            "input.tenant = \"acme\"",
        ]);
        match args.subcommand {
            Some(Command::Subscribe {
                name,
                relay,
                where_clause,
                ..
            }) => {
                assert_eq!(name, "live_events");
                assert_eq!(relay, "events");
                assert_eq!(where_clause.as_deref(), Some("input.tenant = \"acme\""));
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn raft_transition_server_events_are_labeled_as_topology() {
        let rendered = format_server_event(&ServerEvent {
            level: NoticeLevel::Info,
            message: "raft transition: state=Leader leader=node-1 term=2".to_string(),
        });
        assert_eq!(
            rendered,
            "[events] topology INFO: raft transition: state=Leader leader=node-1 term=2"
        );
    }

    #[test]
    fn non_raft_server_events_keep_server_label() {
        let rendered = format_server_event(&ServerEvent {
            level: NoticeLevel::Warning,
            message: "runtime warning".to_string(),
        });
        assert_eq!(rendered, "[events] server WARN: runtime warning");
    }

    #[test]
    fn server_error_notices_print_with_the_error_label() {
        let rendered = format_server_event(&ServerEvent {
            level: NoticeLevel::Error,
            message: "relay 'orders' failed".to_string(),
        });
        assert_eq!(rendered, "[events] server ERROR: relay 'orders' failed");
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
            leader: Some(nervix_client_core::LeaderEndpoints {
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

    fn live_subscription() -> nervix_client_core::SubscriptionHandle {
        nervix_client_core::SubscriptionHandle {
            name: nervix_models::SubscriptionName::parse("live").assured("a valid name"),
            generation: std::num::NonZeroU64::MIN,
        }
    }

    fn rows_event(declared: nervix_models::ParseAsType) -> SubscriptionEvent {
        use nervix_client_core::wire::{
            ServerEvent as WireEvent, ServerMessage, SessionLimits, SubscriptionRowsEncoder,
        };

        let mut batch =
            SubscriptionRowsEncoder::unbranched(live_subscription(), &SessionLimits::DEFAULT)
                .assured("an unbranched batch starts within the limits");
        for id in [1, 2] {
            batch
                .push_row(|cells| cells.push_u64(id))
                .assured("a one-cell row fits the limits");
        }
        let frame = batch
            .finish()
            .assured("a two-row batch fits a frame")
            .verify(&SessionLimits::DEFAULT)
            .assured("an encoded frame verifies");
        let ServerMessage::Event(WireEvent::SubscriptionRows(rows)) =
            ServerMessage::decode(&frame).assured("an encoded frame decodes")
        else {
            panic!("a rows frame decodes as subscription rows");
        };
        let schema = nervix_client_core::RowSchema {
            fields: vec![nervix_models::SchemaField {
                name: nervix_models::FieldName::parse("id").assured("a valid field name"),
                ty: declared,
                optional: false,
                sensitive: false,
            }],
            branch: None,
        };
        SubscriptionEvent::Rows(nervix_client_core::SubscriptionRowsEvent {
            relay: nervix_models::RelayName::parse("orders").assured("a valid relay name"),
            schema: Arc::new(schema),
            rows,
        })
    }

    #[test]
    fn subscription_rows_print_one_line_per_row() {
        assert_eq!(
            format_subscription_event(&rows_event(nervix_models::ParseAsType::U64)),
            [
                "[events] subscription [live] from [orders]: {\"id\":1}",
                "[events] subscription [live] from [orders]: {\"id\":2}",
            ]
        );
    }

    #[test]
    fn rows_that_break_their_schema_print_one_notice() {
        let lines = format_subscription_event(&rows_event(nervix_models::ParseAsType::String));
        assert_eq!(lines.len(), 1);
        assert!(
            lines[0].starts_with(
                "[events] subscription [live] from [orders]: rows do not match the subscription \
                 schema: "
            ),
            "{lines:?}"
        );
    }

    #[test]
    fn subscription_notices_print_one_line_each() {
        let ended = SubscriptionEvent::Ended(nervix_client_core::SubscriptionEnded {
            subscription: live_subscription(),
            reason: nervix_client_core::wire::SubscriptionEndReason::RelayChanged,
            message: "relay 'orders' was redefined".to_string(),
        });
        assert_eq!(
            format_subscription_event(&ended),
            [
                "[events] subscription [live] notice: the subscription ended: relay 'orders' was \
                 redefined"
            ]
        );

        let lost = SubscriptionEvent::DeliveryLost(nervix_client_core::SubscriptionDeliveryLost {
            subscription: live_subscription(),
            dropped_rows: std::num::NonZeroU64::new(3).assured("a non-zero count"),
        });
        assert_eq!(
            format_subscription_event(&lost),
            [
                "[events] subscription [live] notice: 3 rows were dropped because the session \
                 could not take them in time"
            ]
        );
    }

    #[test]
    fn the_prompt_shows_the_domain_and_its_active_transaction() {
        let domain = DomainName::parse("tenant").assured("a valid domain name");
        let status = |lifecycle| {
            TransactionStatus::new(
                "tx-1".to_string(),
                domain.clone(),
                lifecycle,
                nervix_client_core::TransactionPosition::new(0),
                0,
            )
            .assured("an empty transaction is consistent")
        };
        assert_eq!(prompt_domain(Some(&domain), None), "tenant");
        assert_eq!(
            prompt_domain(Some(&domain), Some(&status(TransactionLifecycle::Open))),
            "tenant tx"
        );
        assert_eq!(
            prompt_domain(
                Some(&domain),
                Some(&status(TransactionLifecycle::Committing))
            ),
            "tenant committing"
        );
        assert_eq!(
            prompt_domain(
                Some(&domain),
                Some(&status(TransactionLifecycle::Committed))
            ),
            "tenant"
        );
        assert_eq!(prompt_domain(None, None), "no domain");
    }
}
