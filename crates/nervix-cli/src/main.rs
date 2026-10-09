//! The interactive terminal client for Nervix.
//!
//! Layer: edges.
//!
//! - **Owns.** The REPL: key bindings, the completion menu, rendered diagnostics, output formatting
//!   and the shell-facing command surface.
//! - **Depends on.** `nervix-client-core`, the language layer for completion and local statement
//!   parsing, the archive format's reader for describing a local backup, and the vocabulary.
//! - **Must not know.** The server. It speaks the session API through the client core and nothing
//!   else.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "CLI session and terminal coordination belong to the client edge"
    )
)]

use std::{
    collections::BTreeSet,
    io,
    io::Write,
    net::SocketAddr,
    ops::Range,
    path::{Path, PathBuf},
};

use arch_into::ArchInto as _;
use ariadne::{Color, Config, IndexType, Label, Report, ReportKind, Source};
use byte_unit::{Byte, UnitType};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use error_stack::{Report as StackReport, ResultExt as _};
use nervix_client_core::{
    AutocompleteOutcome, AutocompleteSuggestion, Client, ClientError as CoreClientError,
    CommandDisposition, CommandExecutionReference, CommandOutcome, ConnectDns, ConnectOptions,
    Diagnostic, DomainClockAttachDisposition, DomainClockAttachOutcome,
    DomainClockDetachDisposition, DomainClockDetachOutcome, DomainClockEvent, DomainName,
    LeaderRedirect, NoticeLevel, ServerEvent, SourceSpan, StatementDisposition, StatementOutcome,
    SubscriptionDeliveryBehavior, SubscriptionEvent, SubscriptionRequest,
    SuggestionKind as ClientSuggestionKind, TlsRequirement, TransactionLifecycle,
    TransactionStatus,
};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_models::{ClusterNodeName, InspectionFormat, Statement};
use nervix_nspl::client_statement::{
    ClientStatement, local_path_fragment, parse_client_statements, parse_upload_resource_query,
};
use nervix_primitives::{
    runtime::Handle,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        blocking::Mutex,
    },
    task::block_in_place,
};
use nervix_recovery::{Discarded as _, Reported as _};
use reedline::{
    Completer, DefaultHinter, DefaultPrompt, DefaultPromptSegment, Emacs, FileBackedHistory,
    KeyCode, KeyModifiers, ListMenu, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
    Suggestion,
};
use thiserror::Error;
use tokio::signal;

mod backup;
mod restore;

use self::{
    backup::{BackupRequest, CliBackupScope, CliReportFormat},
    restore::{CliExistingUsers, CliRestoreScope, RestoreRequest},
};

nervix_primitives::product_binary!("nervix-cli");

const HISTORY_FILE: &str = ".nervix_client_history";
const EVENT_BUFFER_RECORDS: usize = 128;
const EVENT_LINE_BYTES: usize = 8 * 1024;
const EVENT_BUFFER_BYTES: usize = EVENT_BUFFER_RECORDS * EVENT_LINE_BYTES;
const EVENT_LINE_SUFFIX: &str = "… [line truncated]";
const EVENT_LINE_PREFIX_BYTES: usize = EVENT_LINE_BYTES - EVENT_LINE_SUFFIX.len();
const _: () = assert!(EVENT_LINE_BYTES > EVENT_LINE_SUFFIX.len());
const _: () = assert!(EVENT_BUFFER_BYTES == 1024 * 1024);

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
    /// Resolv.conf-format file used by native session hostname resolution
    #[arg(long, env = "NERVIX_DNS_RESOLVER_CONFIG", default_value = nervix_dns::SYSTEM_RESOLVER_CONFIGURATION)]
    dns_resolver_config: PathBuf,
    /// Hosts-format file consulted before DNS
    #[arg(long, env = "NERVIX_DNS_HOSTS_FILE", default_value = nervix_dns::SYSTEM_HOSTS_FILE)]
    dns_hosts_file: PathBuf,
    /// Name server addresses with ports, replacing those in the resolver configuration
    #[arg(
        long = "dns-name-server",
        env = "NERVIX_DNS_NAME_SERVERS",
        value_delimiter = ','
    )]
    dns_name_servers: Vec<SocketAddr>,
    /// Domain the session starts in
    #[arg(long, default_value = "default")]
    domain: DomainName,
    /// Registry user to authenticate as
    #[arg(long, env = "NERVIX_USERNAME", default_value = "default")]
    username: String,
    /// Password for the registry user; prompted for interactively when unset
    #[arg(long, env = "NERVIX_PASSWORD")]
    password: Option<String>,
    /// Overall BACKUP command wait across all domain cuts, redirects and reconnects (default 10m)
    #[arg(long, global = true, value_parser = nervix_models::parse_duration_text)]
    backup_wait_timeout: Option<std::time::Duration>,
    /// Run NSPL statements once and exit instead of starting the interactive REPL
    #[arg(long, conflicts_with = "suggest")]
    command: Option<String>,
    /// Print completion candidates for NSPL input as JSON and exit
    #[arg(long, conflicts_with = "command")]
    suggest: Option<String>,
    /// UTF-8 byte cursor in --suggest input; defaults to the end
    #[arg(long, requires = "suggest")]
    cursor: Option<usize>,
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
    /// Follow the selected domain's clock until interrupted
    DomainClock,
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
    /// Back up configuration, users and resources into an archive file
    Backup {
        /// What the archive covers: `cluster` for every domain and user, `domain` for one domain
        #[arg(value_enum)]
        scope: CliBackupScope,
        /// The domain a `domain` backup covers; the session's `--domain` when omitted
        name: Option<DomainName>,
        /// Where the archive is written; `-` writes it to standard output
        #[arg(long, short = 'o')]
        output: String,
        /// Record every resource version and its digests without the version's bytes
        #[arg(long)]
        without_resources: bool,
        /// Capture configuration only, without pausing domains
        #[arg(long, conflicts_with = "without_pause")]
        without_state: bool,
        /// Capture the latest published state without pausing domains
        #[arg(long)]
        without_pause: bool,
        /// Maximum time for each running domain's quiesced capture
        #[arg(long, value_parser = nervix_models::parse_duration_text, conflicts_with_all = ["without_state", "without_pause"])]
        timeout: Option<std::time::Duration>,
        /// Recover the same backup with its original domain and capture options
        #[arg(long, value_parser = |value: &str| CommandExecutionReference::parse(value))]
        execution_reference: Option<CommandExecutionReference>,
        /// How the backup's report is printed
        #[arg(long, value_enum, default_value_t = CliReportFormat::Text)]
        format: CliReportFormat,
    },
    /// Restore configuration, users and resources from an archive file
    Restore {
        /// What to restore: `cluster` for every domain and user of a cluster archive, `domain`
        /// for one domain of an archive
        #[arg(value_enum)]
        scope: CliRestoreScope,
        /// The archived domain a `domain` restore recreates
        name: Option<DomainName>,
        /// The archive file to restore from
        #[arg(long, short = 'i')]
        input: String,
        /// Restore the domain under this name instead of its archived one
        #[arg(long = "as")]
        target: Option<DomainName>,
        /// What a cluster restore does with an archived user the cluster already has; `fail`
        /// when omitted
        #[arg(long, value_enum)]
        on_existing_user: Option<CliExistingUsers>,
        /// Resume the archived start and clock mapping after installing all state
        #[arg(long)]
        resume: bool,
        /// Verify the archive and plan the restore, changing nothing
        #[arg(long)]
        dry_run: bool,
        /// Restore configuration without runtime state
        #[arg(long, conflicts_with = "without_source_offsets")]
        without_state: bool,
        /// Restore runtime state but leave source offsets empty
        #[arg(long)]
        without_source_offsets: bool,
        /// How the restore's report is printed
        #[arg(long, value_enum, default_value_t = CliReportFormat::Text)]
        format: CliReportFormat,
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
    buffer_prefix: Arc<Mutex<String>>,
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
    #[error("invalid subscription WHERE expression")]
    InvalidSubscriptionWhere,
    #[error("transaction inspection failed: {message}")]
    InspectionFailed { message: String },
    #[error("completion pagination repeated a continuation")]
    RepeatedSuggestionPage,
    #[error("domain '{domain}' does not exist")]
    ClockDomainNotFound { domain: DomainName },
    #[error("the session already follows the clock of domain '{domain}'")]
    ClockAlreadyAttached { domain: DomainName },
    #[error("domain clock attachment was refused")]
    ClockAttachRefused,
    #[error("domain clock detachment was refused")]
    ClockDetachRefused,
    #[error("failed to attach to the domain clock")]
    ClockAttachRequest,
    #[error("failed to read the domain clock")]
    ClockEventRead,
    #[error("failed to detach from the domain clock")]
    ClockDetachRequest,
    #[error("invalid backup arguments: {reason}")]
    BackupArguments { reason: &'static str },
    #[error("the backup failed: {message}")]
    BackupFailed { message: String },
    #[error("standard output could not be inspected for the backup archive")]
    InspectStandardOutput,
    #[error("the null device could not be identified")]
    IdentifyNullDevice,
    #[error(
        "standard output was closed when the CLI started or is the null device, either of which \
         would discard the backup archive"
    )]
    DiscardingStandardOutput,
    #[error("the backup archive could not be staged for standard output")]
    StageArchive,
    #[error("the backup archive could not be read from its private staging file")]
    ReadStagedArchive,
    #[error("the backup archive could not be written to standard output")]
    WriteArchive,
    #[error(
        "the backup archive reached standard output, but its staging directory could not be \
         removed"
    )]
    RemoveStaging,
    #[error("the backup archive could not be described")]
    DescribeBackup,
    #[error("invalid restore arguments: {reason}")]
    RestoreArguments { reason: &'static str },
    #[error("the restore did not complete: {message}")]
    RestoreFailed { message: String },
}

async fn collect_suggestions(
    client: &Client,
    input: String,
    cursor: usize,
) -> Result<AutocompleteOutcome, StackReport<ClientError>> {
    let mut continuation = None;
    let mut seen = BTreeSet::new();
    let mut suggestions = Vec::new();
    loop {
        nervix_primitives::task::consume_budget().await;
        let page = client
            .suggest(input.clone(), cursor, 100, continuation.take())
            .await
            .map_err(|error| StackReport::new(ClientError::from(error)))?;
        if page.status != nervix_client_core::SuggestionStatus::Ready {
            return Ok(page);
        }
        suggestions.extend(page.suggestions);
        let Some(next) = page.continuation else {
            return Ok(AutocompleteOutcome {
                status: nervix_client_core::SuggestionStatus::Ready,
                suggestions,
                continuation: None,
            });
        };
        if !seen.insert(next.clone()) {
            return Err(StackReport::new(ClientError::RepeatedSuggestionPage));
        }
        continuation = Some(next);
    }
}

impl Completer for GrpcCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = self.buffer_prefix.lock().clone();
        let pos = line.floor_char_boundary(pos.min(line.len()));
        let Some(cursor) = prefix.len().checked_add(pos) else {
            return Vec::new();
        };
        let combined = format!("{prefix}{line}");
        let client = self.client.clone();
        let runtime = self.runtime.clone();

        let outcome =
            block_in_place(|| runtime.block_on(collect_suggestions(&client, combined, cursor)));
        let Ok(outcome) = outcome else {
            return Vec::new();
        };
        if outcome.status != nervix_client_core::SuggestionStatus::Ready {
            return Vec::new();
        }
        let suggestions = outcome.suggestions;

        if suggestions
            .iter()
            .any(|suggestion| suggestion.kind == ClientSuggestionKind::LocalDirectoryLookup)
        {
            let lookup_hint = suggestions
                .iter()
                .find(|suggestion| suggestion.kind == ClientSuggestionKind::LocalDirectoryLookup);
            if let Some(local) = complete_local_paths(line, pos, prefix.len(), lookup_hint) {
                return local;
            }
        }

        Self::text_suggestions(line, prefix.len(), suggestions)
    }
}

impl GrpcCompleter {
    fn text_suggestions(
        line: &str,
        buffer_prefix_len: usize,
        suggestions: Vec<AutocompleteSuggestion>,
    ) -> Vec<Suggestion> {
        suggestions
            .into_iter()
            .filter(|suggestion| suggestion.kind == ClientSuggestionKind::Text)
            .filter_map(|suggestion| {
                let start = usize::try_from(suggestion.edit.start).ok()?;
                let end = usize::try_from(suggestion.edit.end).ok()?;
                let start = start.checked_sub(buffer_prefix_len)?;
                let end = end.checked_sub(buffer_prefix_len)?;
                if start > end || line.get(start..end).is_none() {
                    return None;
                }
                Some(Suggestion {
                    value: suggestion.edit.replacement,
                    description: Some(suggestion.value),
                    style: None,
                    extra: None,
                    span: reedline::Span::new(start, end),
                    append_whitespace: end == line.len(),
                })
            })
            .collect()
    }

    fn local_path_range(
        line: &str,
        pos: usize,
        buffer_prefix_len: usize,
        hint: &AutocompleteSuggestion,
    ) -> Option<std::ops::Range<usize>> {
        let start = usize::try_from(hint.edit.start)
            .ok()?
            .checked_sub(buffer_prefix_len)?;
        let end = usize::try_from(hint.edit.end)
            .ok()?
            .checked_sub(buffer_prefix_len)?;
        line.get(start..end)?;
        if end < pos || line.get(start..pos)? != hint.value {
            return None;
        }
        Some(start..end)
    }
}

#[nervix_primitives::main]
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
        Some(Command::DomainClock) => return run_domain_clock_mode(&args).await,
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
        Some(Command::Backup {
            scope,
            name,
            output,
            without_resources,
            without_state,
            without_pause,
            timeout,
            execution_reference,
            format,
        }) => {
            let connect_options = connect_options_from_args(&args)?;
            return backup::run_backup(BackupRequest {
                server: args.server,
                connect_options,
                session_domain: args.domain,
                scope,
                domain: name,
                output,
                without_resources,
                without_state,
                without_pause,
                timeout,
                execution_reference,
                format,
            })
            .await;
        }
        Some(Command::Restore {
            scope,
            name,
            input,
            target,
            on_existing_user,
            dry_run,
            resume,
            without_state,
            without_source_offsets,
            format,
        }) => {
            let connect_options = connect_options_from_args(&args)?;
            return restore::run_restore(RestoreRequest {
                server: args.server,
                connect_options,
                session_domain: args.domain,
                scope,
                domain: name,
                target,
                input,
                existing_users: on_existing_user,
                dry_run,
                resume,
                without_state,
                without_source_offsets,
                format,
            })
            .await;
        }
        None => {}
    }

    if let Some(command) = args.command.as_deref()
        && let Some(describe) = backup::describe_backup_statement(command)
    {
        return backup::run_describe_backup(&describe);
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
    if let Some(input) = args.suggest.as_deref() {
        let cursor = args.cursor.unwrap_or(input.len());
        let outcome = collect_suggestions(&client, input.to_string(), cursor).await?;
        let local_hint = outcome
            .suggestions
            .iter()
            .find(|suggestion| suggestion.kind == ClientSuggestionKind::LocalDirectoryLookup);
        let suggestions = if let Some(hint) = local_hint {
            complete_local_paths(input, cursor, 0, Some(hint))
                .unwrap_or_default()
                .into_iter()
                .map(|suggestion| {
                    serde_json::json!({
                        "value": suggestion.value,
                        "kind": "LocalDirectoryLookup",
                        "edit": {
                            "start": suggestion.span.start,
                            "end": suggestion.span.end,
                            "replacement": suggestion.value,
                        },
                    })
                })
                .collect::<Vec<_>>()
        } else {
            outcome
                .suggestions
                .iter()
                .map(|suggestion| {
                    serde_json::json!({
                        "value": suggestion.value,
                        "kind": format!("{:?}", suggestion.kind),
                        "edit": {
                            "start": suggestion.edit.start,
                            "end": suggestion.edit.end,
                            "replacement": suggestion.edit.replacement,
                        },
                    })
                })
                .collect::<Vec<_>>()
        };
        println!(
            "{}",
            serde_json::json!({
                "status": format!("{:?}", outcome.status),
                "suggestions": suggestions,
            })
        );
        return Ok(());
    }
    let (event_sender, mut event_receiver) =
        nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
    let event_sender = EventLineSender::new(event_sender);
    spawn_event_collectors(client.clone(), event_sender.clone());
    if let Some(command) = args.command {
        execute_and_print(&client, command).await?;
        return Ok(());
    }

    let buffer_prefix = Arc::new(Mutex::new(String::new()));

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
        drain_event_queue(&mut event_receiver, &event_sender);
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

        *buffer_prefix.lock() = buffer.clone();

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
                    drain_event_queue(&mut event_receiver, &event_sender);
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
    describe.format == InspectionFormat::Json
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

/// The local path being completed at `pos` and the range of `line` a completion replaces. The
/// server's lookup hint names the path when it names one; the line itself places it otherwise, and
/// supplies the range when the hint's range does not fall within this line.
fn local_path_at(
    line: &str,
    pos: usize,
    buffer_prefix_len: usize,
    lookup_hint: Option<&AutocompleteSuggestion>,
) -> Option<(String, Range<usize>)> {
    let local = local_path_fragment(line, pos);
    let Some(hint) = lookup_hint else {
        let local = local?;
        return Some((local.fragment.to_string(), local.range));
    };
    // An empty hint names no path of its own, so the line has to place one.
    if hint.value.is_empty() && local.is_none() {
        return None;
    }
    let range = match GrpcCompleter::local_path_range(line, pos, buffer_prefix_len, hint) {
        Some(range) => range,
        None => match &local {
            Some(local) => local.range.clone(),
            None => 0..pos,
        },
    };
    Some((hint.value.clone(), range))
}

/// Completes the local file or directory path a statement expects at `pos`: an upload's resource
/// directory, a backup's archive destination, or the archive `DESCRIBE BACKUP` reads.
fn complete_local_paths(
    line: &str,
    pos: usize,
    buffer_prefix_len: usize,
    lookup_hint: Option<&AutocompleteSuggestion>,
) -> Option<Vec<Suggestion>> {
    let (path_fragment, replacement_range) =
        local_path_at(line, pos, buffer_prefix_len, lookup_hint)?;
    let path_fragment = path_fragment.as_str();
    let path = Path::new(path_fragment);
    let (base_dir, partial_name) = if path_fragment.is_empty() {
        (PathBuf::from("."), String::new())
    } else if path_fragment.ends_with(std::path::MAIN_SEPARATOR) || path_fragment.ends_with('/') {
        (expand_user_path(path), String::new())
    } else {
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            Some(_) | None => PathBuf::from("."),
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
            span: reedline::Span::new(replacement_range.start, replacement_range.end),
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
    )?;
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

async fn run_domain_clock_mode(args: &Args) -> Result<(), StackReport<ClientError>> {
    let connect_options = connect_options_from_args(args)?;
    let client =
        Client::connect_with_options(&args.server, Some(args.domain.clone()), connect_options)
            .await
            .map_err(|error| StackReport::new(ClientError::from(error)))?;
    let outcome = client
        .attach_domain_clock(args.domain.clone())
        .await
        .change_context(ClientError::ClockAttachRequest)?;
    println!("{}", clock_attach_message(outcome)?);

    let interrupt = signal::ctrl_c();
    tokio::pin!(interrupt);
    loop {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            event = client.next_domain_clock_event() => {
                let event = event.change_context(ClientError::ClockEventRead)?;
                println!("{}", format_domain_clock_event(&event));
                if let DomainClockEvent::Ended(_) = event {
                    return Ok(());
                }
            }
            interrupted = &mut interrupt => {
                interrupted.map_err(|_| StackReport::new(ClientError::from(CoreClientError::SessionClosed)))?;
                let outcome = client
                    .detach_domain_clock(args.domain.clone())
                    .await
                    .change_context(ClientError::ClockDetachRequest)?;
                clock_detach_completed(outcome)?;
                return Ok(());
            }
        }
    }
}

fn clock_attach_message(
    outcome: DomainClockAttachOutcome,
) -> Result<String, StackReport<ClientError>> {
    match outcome.disposition {
        DomainClockAttachDisposition::Attached { .. } => Ok(outcome.message),
        DomainClockAttachDisposition::DomainNotFound(domain) => {
            Err(StackReport::new(ClientError::ClockDomainNotFound {
                domain,
            }))
        }
        DomainClockAttachDisposition::AlreadyAttached(domain) => {
            Err(StackReport::new(ClientError::ClockAlreadyAttached {
                domain,
            }))
        }
        DomainClockAttachDisposition::Failed => {
            Err(StackReport::new(ClientError::ClockAttachRefused).attach_printable(outcome.message))
        }
    }
}

fn clock_detach_completed(
    outcome: DomainClockDetachOutcome,
) -> Result<(), StackReport<ClientError>> {
    match outcome.disposition {
        DomainClockDetachDisposition::Detached(_)
        | DomainClockDetachDisposition::NotAttached(_) => Ok(()),
        DomainClockDetachDisposition::Failed => {
            Err(StackReport::new(ClientError::ClockDetachRefused).attach_printable(outcome.message))
        }
    }
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
        dns: ConnectDns::Configuration(DnsConfiguration {
            resolver_configuration: args.dns_resolver_config.clone(),
            hosts_file: args.dns_hosts_file.clone(),
            name_servers: if args.dns_name_servers.is_empty() {
                NameServers::ResolverConfiguration
            } else {
                NameServers::Explicit(args.dns_name_servers.clone())
            },
        }),
        tls_requirement: Some(match args.tls {
            CliTlsRequirement::Preferred => TlsRequirement::Preferred,
            CliTlsRequirement::Required => TlsRequirement::Required,
        }),
        ca_certificate_pem,
        username: Some(args.username.clone()),
        password: Some(password),
        backup_wait_timeout: args
            .backup_wait_timeout
            .unwrap_or(ConnectOptions::default().backup_wait_timeout),
        ..ConnectOptions::default()
    })
}

async fn execute_and_print(client: &Client, query: String) -> Result<(), StackReport<ClientError>> {
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

#[derive(Clone)]
struct EventLineSender {
    sender: nervix_primitives::sync::mpsc::Sender<String>,
    dropped: Arc<AtomicU64>,
}

impl EventLineSender {
    fn new(sender: nervix_primitives::sync::mpsc::Sender<String>) -> Self {
        Self {
            sender,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Event readers never wait for a terminal. A full queue drops the new line and records the
    /// gap for the printer; every retained line has a fixed maximum byte length.
    #[allow(deprecated)] // until try_update is stabilized
    fn push(&self, mut line: String) {
        if line.len() > EVENT_LINE_BYTES {
            let boundary = line.floor_char_boundary(EVENT_LINE_PREFIX_BYTES);
            line.truncate(boundary);
            line.push_str(EVENT_LINE_SUFFIX);
        }
        #[allow(deprecated)] // until try_update is stabilized
        match self.sender.try_send(line) {
            Ok(()) => {}
            Err(nervix_primitives::sync::mpsc::error::TrySendError::Full(_)) => {
                self.dropped
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                        // The displayed gap count clamps once it reaches its representable limit.
                        Some(match count.checked_add(1) {
                            Some(next) => next,
                            None => count,
                        })
                    })
                    .discarded("the updated drop count is read when the terminal next drains");
            }
            Err(nervix_primitives::sync::mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }
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

fn print_event_gap(dropped: u64, output: EventOutput) {
    if dropped > 0 {
        output.print(&format!(
            "[events] notice: {dropped} event lines were omitted because terminal output could \
             not keep up"
        ));
    }
}

fn spawn_event_collectors(client: Client, sender: EventLineSender) {
    for stream in EventStream::ALL {
        nervix_primitives::task::spawn(stream.collect(client.clone(), sender.clone()));
    }
}

/// An asynchronous stream of session output the terminal prints, named the way its notices name
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::AsRefStr)]
enum EventStream {
    #[strum(serialize = "subscription events")]
    Subscriptions,
    #[strum(serialize = "domain clock events")]
    DomainClocks,
    #[strum(serialize = "server notices")]
    ServerNotices,
}

/// What the terminal prints about a failed read of an event stream, and whether the stream is
/// over.
#[derive(Debug, PartialEq, Eq)]
struct StreamFailure {
    line: String,
    ended: bool,
}

impl EventStream {
    const ALL: [Self; 3] = [Self::Subscriptions, Self::DomainClocks, Self::ServerNotices];

    /// Prints the stream's events for as long as the client can open a session. The client
    /// restores subscriptions and clocks on its next session and notices continue there, so every
    /// other failure is printed and reading goes on.
    async fn collect(self, client: Client, sender: EventLineSender) {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_lines(&client).await {
                Ok(lines) => {
                    for line in lines {
                        sender.push(line);
                    }
                }
                Err(error) => {
                    let failure = self.failure(error.current_context());
                    sender.push(failure.line);
                    if failure.ended {
                        return;
                    }
                }
            }
        }
    }

    /// The terminal lines of the stream's next event.
    async fn next_lines(
        self,
        client: &Client,
    ) -> Result<Vec<String>, StackReport<CoreClientError>> {
        match self {
            Self::Subscriptions => {
                let event = client.next_subscription().await.map_err(StackReport::new)?;
                Ok(format_subscription_event(&event))
            }
            Self::DomainClocks => {
                let event = client.next_domain_clock_event().await?;
                Ok(vec![format_domain_clock_event(&event)])
            }
            Self::ServerNotices => {
                let event = client.next_server_event().await.map_err(StackReport::new)?;
                Ok(vec![format_server_event(&event)])
            }
        }
    }

    /// What the terminal prints when a read of the stream fails. Only a session that no known
    /// server can reopen ends the stream.
    fn failure(self, error: &CoreClientError) -> StreamFailure {
        let stream = self.as_ref();
        match error {
            CoreClientError::SessionClosed => StreamFailure {
                line: format!(
                    "[events] notice: {stream} stopped because the session closed and no known \
                     server can reopen it"
                ),
                ended: true,
            },
            CoreClientError::EventOverflow { .. } => StreamFailure {
                line: format!(
                    "[events] notice: {stream} were dropped because they arrived faster than they \
                     were read"
                ),
                ended: false,
            },
            other => StreamFailure {
                line: format!(
                    "[events] notice: {stream} could not resume yet: {other}; the client keeps \
                     trying"
                ),
                ended: false,
            },
        }
    }
}

fn spawn_event_loggers(client: Client, output: EventOutput) {
    let (sender, mut receiver) = nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
    let sender = EventLineSender::new(sender);
    let dropped = sender.dropped.clone();
    spawn_event_collectors(client, sender);
    nervix_primitives::task::spawn_blocking(move || {
        while let Some(line) = receiver.blocking_recv() {
            print_event_gap(dropped.swap(0, Ordering::Relaxed), output);
            output.print(&line);
        }
        print_event_gap(dropped.swap(0, Ordering::Relaxed), output);
    });
}

fn drain_event_queue(
    receiver: &mut nervix_primitives::sync::mpsc::Receiver<String>,
    sender: &EventLineSender,
) {
    for _ in 0..EVENT_BUFFER_RECORDS {
        let Ok(line) = receiver.try_recv() else {
            break;
        };
        EventOutput::Stdout.print(&line);
    }
    print_event_gap(sender.take_dropped(), EventOutput::Stdout);
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
        SubscriptionEvent::RestorationFailed(failure) => vec![format!(
            "[events] subscription [{subscription}] notice: opening the subscription again \
             failed: {}; the next attempt follows in {:?}",
            failure.message, failure.retry_after
        )],
        SubscriptionEvent::ConsumerOverflow(_) => vec![format!(
            "[events] subscription [{subscription}] notice: the client event buffer filled; \
             delivery ended with a gap"
        )],
    }
}

/// The terminal line of one event about a domain clock the session follows.
fn format_domain_clock_event(event: &DomainClockEvent) -> String {
    match event {
        DomainClockEvent::Observed(observed) => format!(
            "[events] domain clock [{}]: {}",
            observed.domain, observed.clock
        ),
        DomainClockEvent::Ticked(ticked) => format!(
            "[events] domain clock [{}] tick: generation {}, id {}, boundary {}, authority UTC \
             {}, node logical {}",
            ticked.domain,
            ticked.tick.generation,
            ticked.tick.tick_id,
            ticked.tick.logical_boundary.to_rfc3339(),
            ticked.tick.authority_utc.to_rfc3339(),
            ticked.tick.serving_logical.to_rfc3339(),
        ),
        DomainClockEvent::Ended(ended) => format!(
            "[events] domain clock [{}] notice: the attachment ended because {}",
            ended.domain, ended.reason
        ),
        DomainClockEvent::Interrupted(interrupted) => format!(
            "[events] domain clock [{}] notice: the session was interrupted; the clock is \
             attached again on the next session",
            interrupted.domain
        ),
        DomainClockEvent::RestorationFailed(failure) => format!(
            "[events] domain clock [{}] notice: attaching the clock again failed: {}; the next \
             attempt follows in {:?}",
            failure.domain, failure.message, failure.retry_after
        ),
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
) -> Result<SubscriptionRequest, StackReport<ClientError>> {
    let request = match delivery_behavior {
        SubscriptionDeliveryBehavior::Blocking => SubscriptionRequest::new(name, relay).blocking(),
        SubscriptionDeliveryBehavior::Dropping => SubscriptionRequest::new(name, relay).dropping(),
    };
    let request = match batch_sample_rate {
        Some(batch_sample_rate) => request.with_batch_sample_rate(batch_sample_rate),
        None => request,
    };
    let Some(where_clause) = where_clause else {
        return Ok(request);
    };
    let where_clause = nervix_nspl::parse_expression(where_clause)
        .change_context(ClientError::InvalidSubscriptionWhere)?;
    Ok(request.with_where_clause(where_clause))
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
    fn backup_wait_and_recovery_arguments_use_shared_connect_options() {
        let args = Args::try_parse_from([
            "nervix-cli",
            "--password",
            "secret",
            "--backup-wait-timeout",
            "7m",
            "--command",
            "BACKUP CLUSTER TO 'cluster.nvxb';",
        ])
        .assured("NSPL accepts the global backup wait option");
        let options = connect_options_from_args(&args).assured("connect options are valid");
        assert_eq!(
            options.backup_wait_timeout,
            std::time::Duration::from_secs(7 * 60)
        );
        assert_eq!(
            options.request_timeout,
            ConnectOptions::default().request_timeout
        );
        assert_eq!(
            options.retry_timeout,
            ConnectOptions::default().retry_timeout
        );

        let args = Args::try_parse_from([
            "nervix-cli",
            "--password",
            "secret",
            "backup",
            "cluster",
            "--output",
            "-",
            "--execution-reference",
            "recovery.backup",
            "--backup-wait-timeout",
            "8m",
        ])
        .assured("the backup subcommand accepts recovery and global wait options");
        let options = connect_options_from_args(&args).assured("connect options are valid");
        assert_eq!(
            options.backup_wait_timeout,
            std::time::Duration::from_secs(8 * 60)
        );
        let Some(Command::Backup {
            execution_reference: Some(reference),
            ..
        }) = args.subcommand
        else {
            panic!("the backup retains its explicit execution reference");
        };
        assert_eq!(reference.as_str(), "recovery.backup");
        let args = Args::try_parse_from(["nervix-cli", "--password", "secret"])
            .assured("the CLI's defaults parse");
        let options = connect_options_from_args(&args).assured("default options are valid");
        assert_eq!(
            options.backup_wait_timeout,
            std::time::Duration::from_secs(10 * 60)
        );
    }

    #[test]
    fn event_line_queue_bounds_records_and_bytes_without_waiting_for_the_printer() {
        let (sender, mut receiver) = nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
        let sink = EventLineSender::new(sender);
        for _ in 0..EVENT_BUFFER_RECORDS {
            sink.push("é".repeat(EVENT_LINE_BYTES));
        }
        sink.push("one more line".to_string());
        assert_eq!(sink.take_dropped(), 1);
        let mut retained_bytes = 0;
        for _ in 0..EVENT_BUFFER_RECORDS {
            let line = receiver
                .try_recv()
                .verified("the test filled the bounded queue with this many lines");
            assert!(line.len() <= EVENT_LINE_BYTES);
            assert!(line.ends_with(EVENT_LINE_SUFFIX));
            retained_bytes += line.len();
        }
        assert!(retained_bytes <= EVENT_BUFFER_BYTES);
    }

    #[test]
    fn draining_events_resets_the_visible_gap_after_a_full_queue() {
        let (sender, mut receiver) = nervix_primitives::sync::mpsc::channel(2);
        let sink = EventLineSender::new(sender);
        sink.push("first".to_string());
        sink.push("second".to_string());
        sink.push("dropped".to_string());

        drain_event_queue(&mut receiver, &sink);

        assert!(receiver.try_recv().is_err());
        assert_eq!(
            sink.take_dropped(),
            0,
            "the gap was reported during the drain"
        );
    }

    #[test]
    fn an_event_gap_clamps_and_a_closed_printer_discards_new_lines() {
        let (sender, receiver) = nervix_primitives::sync::mpsc::channel(1);
        let sink = EventLineSender::new(sender);
        sink.push("retained".to_string());
        sink.dropped.store(u64::MAX, Ordering::Relaxed);
        sink.push("overflow".to_string());
        assert_eq!(sink.take_dropped(), u64::MAX);

        drop(receiver);
        sink.push("printer stopped".to_string());
        assert_eq!(sink.take_dropped(), 0);
    }

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
    fn domain_clock_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "--domain", "sim", "domain-clock"]);
        assert_eq!(args.domain.as_str(), "sim");
        assert!(matches!(args.subcommand, Some(Command::DomainClock)));
    }

    #[test]
    fn domain_clock_attach_refusals_keep_typed_errors_and_server_detail() {
        let domain = DomainName::parse("sim").assured("a valid domain name");
        let already_attached = DomainClockAttachOutcome {
            disposition: DomainClockAttachDisposition::AlreadyAttached(domain.clone()),
            message: "already attached".to_string(),
        };
        let Err(error) = clock_attach_message(already_attached) else {
            panic!("an already attached clock must be refused");
        };
        assert!(matches!(
            error.current_context(),
            ClientError::ClockAlreadyAttached { domain: attached } if attached == &domain
        ));

        let refused = DomainClockAttachOutcome {
            disposition: DomainClockAttachDisposition::Failed,
            message: "the server refused the attachment".to_string(),
        };
        let Err(error) = clock_attach_message(refused) else {
            panic!("a refused clock attachment must fail");
        };
        assert!(matches!(
            error.current_context(),
            ClientError::ClockAttachRefused
        ));
        assert!(format!("{error:?}").contains("the server refused the attachment"));
    }

    #[test]
    fn domain_clock_detach_accepts_absence_and_reports_refusal() {
        let domain = DomainName::parse("sim").assured("a valid domain name");
        let not_attached = DomainClockDetachOutcome {
            disposition: DomainClockDetachDisposition::NotAttached(domain),
            message: "not attached".to_string(),
        };
        assert!(clock_detach_completed(not_attached).is_ok());

        let refused = DomainClockDetachOutcome {
            disposition: DomainClockDetachDisposition::Failed,
            message: "the server refused the detachment".to_string(),
        };
        let Err(error) = clock_detach_completed(refused) else {
            panic!("a refused clock detachment must fail");
        };
        assert!(matches!(
            error.current_context(),
            ClientError::ClockDetachRefused
        ));
        assert!(format!("{error:?}").contains("the server refused the detachment"));
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
    fn subscribe_query_reads_a_backslash_in_a_string_literal_as_a_typed_statement_does() {
        assert_eq!(
            subscribe_request(
                "live_myss",
                "myss",
                SubscriptionDeliveryBehavior::Blocking,
                None,
                Some(r#"input.tenant = 'a\nb' OR input.tenant = "c\'d""#)
            )
            .expect("subscription request should build")
            .to_query(),
            r#"CREATE SUBSCRIPTION live_myss TO myss WHERE input.tenant = 'a\nb' OR input.tenant = "c\'d";"#
        );
    }

    #[test]
    fn subscribe_request_rejects_an_unparsable_where_clause() {
        let error = subscribe_request(
            "live_myss",
            "myss",
            SubscriptionDeliveryBehavior::Blocking,
            None,
            Some("input.tenant ="),
        )
        .expect_err("an incomplete WHERE expression must be rejected");

        assert!(matches!(
            error.current_context(),
            ClientError::InvalidSubscriptionWhere
        ));
        assert!(
            format!("{error:#}").starts_with("invalid subscription WHERE expression: parse error"),
            "unexpected error: {error:#}"
        );
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
    fn typed_completion_edit_replaces_a_word_before_a_unicode_suffix() {
        let line = "SHOW CLUSTR;😊";
        let suggestions = GrpcCompleter::text_suggestions(
            line,
            0,
            vec![AutocompleteSuggestion {
                value: "CLUSTER".to_string(),
                kind: ClientSuggestionKind::Text,
                edit: nervix_client_core::TextEdit {
                    start: 5,
                    end: 11,
                    replacement: "CLUSTER".to_string(),
                },
            }],
        );
        assert_eq!(suggestions.len(), 1);
        assert_eq!(suggestions[0].span, reedline::Span::new(5, 11));
        assert_eq!(
            format!(
                "{}{}{}",
                &line[..suggestions[0].span.start],
                suggestions[0].value,
                &line[suggestions[0].span.end..]
            ),
            "SHOW CLUSTER;😊"
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
        let suggestions = complete_local_paths(
            &line,
            line.len(),
            0,
            Some(&AutocompleteSuggestion {
                value: format!("{}/pro", temp.display()),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
                edit: nervix_client_core::TextEdit {
                    start: 0,
                    end: 0,
                    replacement: String::new(),
                },
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
        let path_prefix = format!("{}/pro", temp.display());
        let source_start = "UPLOAD RESOURCE proto VERSION '".len();
        let cursor = source_start + path_prefix.len();
        let mid_line = format!("UPLOAD RESOURCE proto VERSION '{path_prefix}to';");
        let mid_suggestions = complete_local_paths(
            &mid_line,
            cursor,
            0,
            Some(&AutocompleteSuggestion {
                value: path_prefix,
                kind: ClientSuggestionKind::LocalDirectoryLookup,
                edit: nervix_client_core::TextEdit {
                    start: u32::try_from(source_start).assured("the test source fits u32"),
                    end: u32::try_from(cursor + 2).assured("the test source fits u32"),
                    replacement: String::new(),
                },
            }),
        )
        .assured("local path completion is available in the middle of a path");
        assert!(mid_suggestions.iter().any(|suggestion| {
            suggestion.span == reedline::Span::new(source_start, cursor + 2)
                && suggestion.value.ends_with("/proto-dir/")
        }));
        std::fs::remove_dir_all(&temp).expect("temp dir should be removed");
    }

    #[test]
    fn local_path_completion_serves_backup_destinations_and_described_archives() {
        for line in [
            "BACKUP CLUSTER TO 'Cargo.tml'",
            "BACKUP DOMAIN tenant TO 'Cargo.tml' WITHOUT RESOURCES;",
            "DESCRIBE BACKUP 'Cargo.tml' FORMAT JSON;",
        ] {
            let cursor = line.find("ml'").assured("the test input marks its cursor");
            let suggestions = complete_local_paths(line, cursor, 0, None)
                .assured("the package directory can be read for local completion");
            let start = line.find('\'').assured("the path is quoted") + 1;
            assert!(
                suggestions
                    .iter()
                    .any(|suggestion| suggestion.value == "Cargo.toml"
                        && suggestion.span
                            == reedline::Span::new(start, start + "Cargo.tml".len())),
                "{line} completes its path"
            );
        }
        assert!(complete_local_paths("BACKUP CLUSTER ", 15, 0, None).is_none());
    }

    #[test]
    fn local_upload_path_completion_reads_a_bare_relative_filename() {
        let line = "UPLOAD RESOURCE bundle VERSION 'Cargo.tml'";
        let cursor = line.find("ml'").assured("the test input marks its cursor");
        let suggestions = complete_local_paths(line, cursor, 0, None)
            .assured("the package directory can be read for local completion");
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.value == "Cargo.toml")
        );
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
        let suggestions = complete_local_paths(
            "",
            0,
            0,
            Some(&AutocompleteSuggestion {
                value: format!("{}/", temp.display()),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
                edit: nervix_client_core::TextEdit {
                    start: 0,
                    end: 0,
                    replacement: String::new(),
                },
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
        let suggestions = complete_local_paths(
            "",
            0,
            0,
            Some(&AutocompleteSuggestion {
                value: format!("~/{basename}"),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
                edit: nervix_client_core::TextEdit {
                    start: 0,
                    end: 0,
                    replacement: String::new(),
                },
            }),
        )
        .expect("local path completion should be available");
        assert!(
            suggestions
                .iter()
                .any(|suggestion| suggestion.value == format!("~/{basename}/"))
        );
        let nested_suggestions = complete_local_paths(
            "",
            0,
            0,
            Some(&AutocompleteSuggestion {
                value: format!("~/{basename}/"),
                kind: ClientSuggestionKind::LocalDirectoryLookup,
                edit: nervix_client_core::TextEdit {
                    start: 0,
                    end: 0,
                    replacement: String::new(),
                },
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
    fn subscription_rows_display_branch_null_and_redaction_from_typed_cells() {
        use nervix_client_core::wire::{
            RowBranch, ServerEvent as WireEvent, ServerMessage, SessionLimits,
            SubscriptionRowsEncoder,
        };
        use nervix_models::{FieldName, ParseAsType, SchemaField};

        let field = |name: &str, ty: ParseAsType, optional: bool, sensitive: bool| SchemaField {
            name: FieldName::parse(name).assured("the test field name is valid"),
            ty,
            optional,
            sensitive,
        };
        let mut batch = SubscriptionRowsEncoder::branched(
            live_subscription(),
            &SessionLimits::DEFAULT,
            |key| key.push_string("north"),
        )
        .assured("the branch key fits the frame");
        batch
            .push_row(|cells| {
                cells.push_u64(7)?;
                cells.push_null()?;
                cells.push_redacted()
            })
            .assured("the typed row fits the frame");
        let frame = batch
            .finish()
            .assured("the batch fits the frame")
            .verify(&SessionLimits::DEFAULT)
            .assured("the encoded frame verifies");
        let ServerMessage::Event(WireEvent::SubscriptionRows(rows)) =
            ServerMessage::decode(&frame).assured("the encoded frame decodes")
        else {
            panic!("the frame contains typed subscription rows");
        };
        let branch = RowBranch::new(
            nervix_models::BranchName::parse("by_tenant").assured("the test branch name is valid"),
            vec![field("tenant", ParseAsType::String, false, false)],
        )
        .assured("the branch has a key field");
        let schema = nervix_client_core::RowSchema {
            fields: vec![
                field("id", ParseAsType::U64, false, false),
                field("note", ParseAsType::String, true, false),
                field("secret", ParseAsType::String, false, true),
            ],
            branch: Some(branch),
        };
        let event = SubscriptionEvent::Rows(nervix_client_core::SubscriptionRowsEvent {
            relay: nervix_models::RelayName::parse("orders").assured("the relay name is valid"),
            schema: Arc::new(schema),
            rows,
        });
        assert_eq!(
            format_subscription_event(&event),
            [
                "[events] subscription [live] from [orders]: key={\"tenant\":\"north\"} \
                 payload={\"id\":7,\"secret\":\"<masked>\"}"
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

        let refused = SubscriptionEvent::RestorationFailed(
            nervix_client_core::SubscriptionRestorationFailure {
                subscription: live_subscription(),
                message: "stream 'orders' does not exist in domain 'tenant'".to_string(),
                retry_after: std::time::Duration::from_secs(2),
            },
        );
        assert_eq!(
            format_subscription_event(&refused),
            [
                "[events] subscription [live] notice: opening the subscription again failed: \
                 stream 'orders' does not exist in domain 'tenant'; the next attempt follows in 2s"
            ]
        );
    }

    #[test]
    fn a_failed_event_read_is_printed_and_only_an_unrecoverable_session_ends_the_stream() {
        assert_eq!(
            EventStream::DomainClocks.failure(&CoreClientError::RetryDeadline),
            StreamFailure {
                line: "[events] notice: domain clock events could not resume yet: session retry \
                       deadline expired; the client keeps trying"
                    .to_string(),
                ended: false,
            }
        );
        assert_eq!(
            EventStream::ServerNotices.failure(&CoreClientError::EventOverflow {
                stream: nervix_client_core::EventStreamKind::ServerNotice,
            }),
            StreamFailure {
                line: "[events] notice: server notices were dropped because they arrived faster \
                       than they were read"
                    .to_string(),
                ended: false,
            }
        );
        assert_eq!(
            EventStream::Subscriptions.failure(&CoreClientError::SessionClosed),
            StreamFailure {
                line: "[events] notice: subscription events stopped because the session closed \
                       and no known server can reopen it"
                    .to_string(),
                ended: true,
            }
        );
    }

    #[test]
    fn domain_clock_events_print_one_line_each() {
        let domain = DomainName::parse("sim").assured("a valid domain name");
        let observed = DomainClockEvent::Observed(nervix_client_core::DomainClockObserved {
            domain: domain.clone(),
            clock: nervix_client_core::DomainClockObservation {
                generation: 4,
                state: nervix_client_core::DomainClockObservedState::Unpaced,
            },
        });
        assert_eq!(
            format_domain_clock_event(&observed),
            "[events] domain clock [sim]: generation 4, unpaced"
        );
        let ticked = DomainClockEvent::Ticked(nervix_client_core::DomainClockTicked {
            domain: domain.clone(),
            tick: nervix_client_core::DomainClockTickObservation {
                generation: 5,
                tick_id: 12,
                logical_boundary: nervix_client_core::Timestamp::from_unix_nanos(1_000),
                authority_utc: nervix_client_core::Timestamp::from_unix_nanos(2_000),
                serving_logical: nervix_client_core::Timestamp::from_unix_nanos(3_000),
            },
        });
        assert_eq!(
            format_domain_clock_event(&ticked),
            "[events] domain clock [sim] tick: generation 5, id 12, boundary \
             1970-01-01T00:00:00.000001Z, authority UTC 1970-01-01T00:00:00.000002Z, node logical \
             1970-01-01T00:00:00.000003Z"
        );
        let ended = DomainClockEvent::Ended(nervix_client_core::DomainClockAttachmentEnded {
            domain: domain.clone(),
            reason: nervix_client_core::DomainClockAttachmentEndReason::DomainRemoved,
        });
        assert_eq!(
            format_domain_clock_event(&ended),
            "[events] domain clock [sim] notice: the attachment ended because the domain no \
             longer exists on the serving node"
        );
        let refused = DomainClockEvent::RestorationFailed(
            nervix_client_core::DomainClockRestorationFailure {
                domain: domain.clone(),
                message: "the session holds a transaction".to_string(),
                retry_after: std::time::Duration::from_secs(4),
            },
        );
        assert_eq!(
            format_domain_clock_event(&refused),
            "[events] domain clock [sim] notice: attaching the clock again failed: the session \
             holds a transaction; the next attempt follows in 4s"
        );
        let interrupted =
            DomainClockEvent::Interrupted(nervix_client_core::DomainClockInterruption { domain });
        assert_eq!(
            format_domain_clock_event(&interrupted),
            "[events] domain clock [sim] notice: the session was interrupted; the clock is \
             attached again on the next session"
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
