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
    nervix::layer(edge, reason = "interactive terminal language adapter")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "CLI session and terminal coordination belong to the client edge"
    )
)]

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    ops::Range,
    path::{Path, PathBuf},
};

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use error_stack::{Report as StackReport, ResultExt as _};
use nervix_client_core::{
    AutocompleteOutcome, AutocompleteSuggestion, Client, ClientError as CoreClientError,
    CommandExecutionReference, ConnectDns, ConnectOptions, DomainClockAttachDisposition,
    DomainClockAttachOutcome, DomainClockAttachRefusal, DomainClockDetachDisposition,
    DomainClockDetachOutcome, DomainClockEvent, DomainName, NoticeKind, NoticeLevel,
    ResourceUploadIdentity, ServerEvent, SubscriptionDeliveryBehavior, SubscriptionEvent,
    SubscriptionRequest, SuggestionKind as ClientSuggestionKind, TlsRequirement,
    TransactionLifecycle, TransactionStatus,
};
use nervix_dns::{DnsConfiguration, NameServers};
use nervix_models::{ClusterNodeName, InspectionFormat, Statement};
use nervix_plexus::client_statement::{
    ClientStatement, local_path_fragment, parse_client_statements, parse_upload_resource_query,
};
use nervix_primitives::{
    runtime::Handle,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        blocking::Mutex,
    },
    task::{JoinHandle, block_in_place},
};
use nervix_recovery::Discarded as _;
use reedline::{
    Completer, CompletionResult, DefaultHinter, DefaultPrompt, DefaultPromptSegment, Emacs,
    ExternalOutput, FileBackedHistory, KeyCode, KeyModifiers, ListMenu, MenuBuilder, Reedline,
    ReedlineEvent, ReedlineMenu, Signal, Suggestion,
};
use thiserror::Error;

mod backup;
mod interrupt;
mod restore;
mod statement;
mod status;

use self::{
    backup::{BackupRequest, CliBackupScope, CliReportFormat},
    interrupt::interrupted,
    restore::{CliExistingUsers, CliRestoreScope, RestoreRequest},
    statement::{Repetition, StatementRunner, print_diagnostics},
    status::RunEnd,
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
    /// Run Plexus statements once and exit instead of starting the interactive REPL
    #[arg(long, conflicts_with = "suggest")]
    command: Option<String>,
    /// With `--command` or a node administration subcommand, send the command under the execution
    /// reference the CLI printed for an earlier run of it whose outcome did not arrive
    #[arg(long, value_parser = |value: &str| CommandExecutionReference::parse(value), conflicts_with = "suggest")]
    execution_reference: Option<CommandExecutionReference>,
    /// With a `--command` that is one UPLOAD RESOURCE, send it under the upload identity the CLI
    /// printed for an earlier run of it whose outcome did not arrive
    #[arg(long, value_parser = |value: &str| ResourceUploadIdentity::parse(value), requires = "command", conflicts_with = "execution_reference")]
    upload_identity: Option<ResourceUploadIdentity>,
    /// Print completion candidates for Plexus input as JSON and exit
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
    /// Subscribe to one relay and print events until it ends or the observer is interrupted
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
        /// Optional Plexus predicate applied to delivered records
        #[arg(long = "where")]
        where_clause: Option<String>,
    },
    /// Follow the selected domain's clock until its attachment ends or the observer is interrupted
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
    /// The client's report of why no session opened is beneath.
    #[error("failed to connect to the server")]
    Connect,
    /// The client's report of why the request failed is beneath.
    #[error("the request to the server failed")]
    Request,
    #[error("failed to read the TLS CA certificate")]
    LoadTlsCaCertificate,
    #[error("failed to wait for the interrupt signal")]
    InterruptSignal,
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
    #[error("domain clock attachment was refused because {refusal}")]
    ClockAttachRefused { refusal: DomainClockAttachRefusal },
    #[error("domain clock detachment was refused")]
    ClockDetachRefused,
    #[error("failed to attach to the domain clock")]
    ClockAttachRequest,
    #[error("failed to read the domain clock")]
    ClockEventRead,
    #[error("the subscription event reader stopped unexpectedly")]
    SubscriptionEventReader,
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
            .change_context(ClientError::Request)?;
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
    fn complete(&mut self, line: &str, pos: usize) -> CompletionResult {
        let prefix = self.buffer_prefix.lock().clone();
        let pos = line.floor_char_boundary(pos.min(line.len()));
        let Some(cursor) = prefix.len().checked_add(pos) else {
            return CompletionResult::fresh(Vec::<Suggestion>::new());
        };
        let combined = format!("{prefix}{line}");
        let client = self.client.clone();
        let runtime = self.runtime.clone();

        let outcome =
            block_in_place(|| runtime.block_on(collect_suggestions(&client, combined, cursor)));
        let Ok(outcome) = outcome else {
            return CompletionResult::fresh(Vec::<Suggestion>::new());
        };
        if outcome.status != nervix_client_core::SuggestionStatus::Ready {
            return CompletionResult::fresh(Vec::<Suggestion>::new());
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
                return CompletionResult::fresh(local);
            }
        }

        CompletionResult::fresh(Self::text_suggestions(line, prefix.len(), suggestions))
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
                    display_override: None,
                    description: Some(suggestion.value),
                    style: None,
                    extra: None,
                    span: reedline::Span::new(start, end),
                    append_whitespace: end == line.len(),
                    match_indices: None,
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
async fn main() -> Result<RunEnd, StackReport<ClientError>> {
    let args = Args::parse();
    let repetition = repetition_of(&args);
    match args.subcommand.clone() {
        Some(Command::Completions { shell }) => {
            print_completions(shell);
            return Ok(RunEnd::Completed);
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
            let statement = format!("DROP NODE {node_id};");
            return run_administration(&args, repetition, statement).await;
        }
        Some(Command::CordonNode { node_id }) => {
            let statement = format!("CORDON NODE {node_id};");
            return run_administration(&args, repetition, statement).await;
        }
        Some(Command::UncordonNode { node_id }) => {
            let statement = format!("UNCORDON NODE {node_id};");
            return run_administration(&args, repetition, statement).await;
        }
        Some(Command::DrainNode { node_id }) => {
            let statement = format!("DRAIN NODE {node_id};");
            return run_administration(&args, repetition, statement).await;
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
            backup::run_backup(BackupRequest {
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
            .await?;
            return Ok(RunEnd::Completed);
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
            restore::run_restore(RestoreRequest {
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
            .await?;
            return Ok(RunEnd::Completed);
        }
        None => {}
    }

    if let Some(command) = args.command.as_deref()
        && let Some(describe) = backup::describe_backup_statement(command)
    {
        backup::run_describe_backup(&describe)?;
        return Ok(RunEnd::Completed);
    }
    if let Some(command) = args.command.as_deref()
        && is_json_inspection_command(command)
    {
        run_json_inspection_mode(&args, command).await?;
        return Ok(RunEnd::Completed);
    }

    let connect_options = connect_options_from_args(&args)?;
    let client =
        Client::connect_with_options(&args.server, Some(args.domain.clone()), connect_options)
            .await
            .change_context(ClientError::Connect)?;
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
        return Ok(RunEnd::Completed);
    }
    let (event_sender, event_receiver) =
        nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
    let event_sender = EventLineSender::new(event_sender);
    drop(spawn_event_collectors(
        client.clone(),
        event_sender.clone(),
        SubscriptionEndPolicy::Continue,
    ));
    if let Some(command) = args.command {
        return StatementRunner::for_arguments(repetition)
            .run(&client, command)
            .await;
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

    let mut line_editor = create_line_editor(completer)?.with_external_printer(ReplEventOutput {
        receiver: event_receiver,
        sender: event_sender,
    });

    // A statement's outcome never ends the REPL: the runner prints it, whatever it is.
    let mut statements = StatementRunner::at_prompt();
    loop {
        // Keep changes made by another CLI's history visible at each prompt, as reopening the
        // history for each line did, while one editor owns asynchronous output throughout the REPL.
        line_editor
            .sync_history()
            .change_context(ClientError::InitHistory)?;
        let prompt_domain = prompt_domain(
            client.domain().await.as_ref(),
            client.transaction_status().await.as_ref(),
        );
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
                    statements.run(&client, payload).await?;
                }
            }
            Ok(_) => break,
            Err(err) => {
                return Err(StackReport::new(ClientError::ReadLine)
                    .attach_printable(format!("readline failed: {err}")));
            }
        }
    }

    Ok(RunEnd::Completed)
}

/// Runs the statement a node administration subcommand sends, as `--command` runs it.
async fn run_administration(
    args: &Args,
    repetition: Repetition,
    statement: String,
) -> Result<RunEnd, StackReport<ClientError>> {
    let connect_options = connect_options_from_args(args)?;
    let client =
        Client::connect_with_options(&args.server, Some(args.domain.clone()), connect_options)
            .await
            .change_context(ClientError::Connect)?;
    StatementRunner::for_arguments(repetition)
        .run(&client, statement)
        .await
}

/// What `--execution-reference` and `--upload-identity` ask the run to repeat. Either one given
/// where it would repeat nothing ends the run as invalid arguments.
fn repetition_of(args: &Args) -> Repetition {
    let repetition = Repetition {
        reference: args.execution_reference.clone(),
        upload_identity: args.upload_identity.clone(),
    };
    let uploads = match args.command.as_deref() {
        Some(command) => parse_upload_resource_query(command).is_ok(),
        None => false,
    };
    if repetition.upload_identity.is_some() && !uploads {
        refuse_arguments(
            "--upload-identity repeats an upload, so --command must be one UPLOAD RESOURCE              statement",
        );
    }
    if repetition.reference.is_some() {
        let repeats_command = match &args.subcommand {
            None => args.command.is_some() && !uploads,
            Some(
                Command::RemoveNode { .. }
                | Command::CordonNode { .. }
                | Command::UncordonNode { .. }
                | Command::DrainNode { .. },
            ) => true,
            Some(_) => false,
        };
        if !repeats_command {
            refuse_arguments(
                "--execution-reference repeats the command --command or a node administration                  subcommand sends; an upload is repeated with --upload-identity, and a backup with                  the backup subcommand's own --execution-reference",
            );
        }
    }
    repetition
}

/// Ends the run as invalid arguments, the way the argument parser ends it.
fn refuse_arguments(message: &str) -> ! {
    Args::command()
        .error(clap::error::ErrorKind::ArgumentConflict, message)
        .exit()
}

/// A one-shot JSON inspection reserves stdout for exactly one machine-readable document.
fn is_json_inspection_command(query: &str) -> bool {
    let Ok(statements) = parse_client_statements(query) else {
        return false;
    };
    let [ClientStatement::Server(statement)] = statements.as_slice() else {
        return false;
    };
    match statement {
        Statement::DescribeTransaction(describe) => describe.format == InspectionFormat::Json,
        Statement::DescribeWasmProcessor(describe) => describe.format == InspectionFormat::Json,
        Statement::ShowRelayMaterializedState(show) => show.format == InspectionFormat::Json,
        _ => false,
    }
}

async fn run_json_inspection_mode(
    args: &Args,
    query: &str,
) -> Result<(), StackReport<ClientError>> {
    let options = match connect_options_from_args(args) {
        Ok(options) => options,
        Err(error) => {
            print_json_inspection_error("CLIENT_CONFIGURATION", &format!("{error:#}"));
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
            print_json_inspection_error("CONNECTION_FAILED", &format!("{error:#}"));
            return Err(error.change_context(ClientError::Connect));
        }
    };
    drop(spawn_event_loggers(
        client.clone(),
        EventOutput::Stderr,
        SubscriptionEndPolicy::Continue,
    ));
    let outcome = match client.execute(query).await {
        Ok(outcome) => outcome,
        Err(error) => {
            print_json_inspection_error("REQUEST_FAILED", &format!("{error:#}"));
            return Err(error.change_context(ClientError::Request));
        }
    };
    if outcome.state.is_some() {
        println!("{}", outcome.message);
        return if outcome.succeeded() {
            Ok(())
        } else {
            Err(StackReport::new(ClientError::InspectionFailed {
                message: outcome.message,
            }))
        };
    }
    if !outcome.succeeded() {
        print_json_inspection_error("INSPECTION_REFUSED", &outcome.message);
        return Err(StackReport::new(ClientError::InspectionFailed {
            message: outcome.message,
        }));
    }
    if outcome.inspection.is_none() && outcome.state.is_none() && outcome.wasm_state.is_none() {
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
            .map_err(|cause| StackReport::new(cause).change_context(ClientError::InitHistory))?,
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
            display_override: None,
            description: None,
            style: None,
            extra: None,
            span: reedline::Span::new(replacement_range.start, replacement_range.end),
            append_whitespace: false,
            match_indices: None,
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

async fn run_subscribe_mode(
    options: SubscribeModeOptions,
) -> Result<RunEnd, StackReport<ClientError>> {
    let client = Client::connect_with_options(
        &options.server,
        Some(options.domain),
        options.connect_options,
    )
    .await
    .change_context(ClientError::Connect)?;
    let subscription = spawn_event_loggers(
        client.clone(),
        EventOutput::Stdout,
        SubscriptionEndPolicy::Finish,
    );
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
        .change_context(ClientError::Request)?;
    if !result.succeeded() {
        println!("error: {}", result.message);
        if result.diagnostics.is_empty() {
            println!("- no diagnostics provided");
        } else {
            print_diagnostics("subscribe", &query, &result.diagnostics);
        }
        return Ok(RunEnd::Failed);
    }

    println!("{}", result.message);
    println!(
        "listening for events from relay '{}'. Press Ctrl-C to stop.",
        options.relay
    );
    match interrupt::unless_interrupted(subscription).await? {
        Some(ending) => ending.change_context(ClientError::SubscriptionEventReader),
        None => Ok(RunEnd::Completed),
    }
}

async fn run_domain_clock_mode(args: &Args) -> Result<RunEnd, StackReport<ClientError>> {
    let connect_options = connect_options_from_args(args)?;
    let client =
        Client::connect_with_options(&args.server, Some(args.domain.clone()), connect_options)
            .await
            .change_context(ClientError::Connect)?;
    let outcome = client
        .attach_domain_clock(args.domain.clone())
        .await
        .change_context(ClientError::ClockAttachRequest)?;
    println!("{}", clock_attach_message(outcome)?);

    let mut attachment = ClockStreamState::Attached;
    let interrupt = interrupted();
    tokio::pin!(interrupt);
    loop {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            event = client.next_domain_clock_event() => {
                match event {
                    Ok(event) => {
                        attachment.observe(&event);
                        println!("{}", format_domain_clock_event(&event));
                        if attachment.is_ended() {
                            return Ok(RunEnd::Completed);
                        }
                    }
                    Err(error) => {
                        let failure = EventStream::DomainClocks.failure(&error);
                        println!("{}", failure.line);
                        if failure.ended {
                            return Err(error.change_context(ClientError::ClockEventRead));
                        }
                        attachment = ClockStreamState::Interrupted;
                    }
                }
            }
            interrupted = &mut interrupt => {
                interrupted?;
                attachment.detach(&client, args.domain.clone()).await?;
                return Ok(RunEnd::Completed);
            }
        }
    }
}

/// The attachment's last reported state. An interrupted observer can stop without opening a
/// replacement session solely to detach from it.
enum ClockStreamState {
    Attached,
    Interrupted,
    Ended,
}

impl ClockStreamState {
    fn observe(&mut self, event: &DomainClockEvent) {
        match event {
            DomainClockEvent::Observed(_) | DomainClockEvent::Ticked(_) => {
                *self = Self::Attached;
            }
            DomainClockEvent::Interrupted(_) | DomainClockEvent::RestorationFailed(_) => {
                *self = Self::Interrupted;
            }
            DomainClockEvent::Ended(_) => {
                *self = Self::Ended;
            }
        }
    }

    fn is_ended(&self) -> bool {
        matches!(self, Self::Ended)
    }

    async fn detach(
        self,
        client: &Client,
        domain: DomainName,
    ) -> Result<(), StackReport<ClientError>> {
        match self {
            Self::Attached => {
                let outcome = client
                    .detach_domain_clock(domain)
                    .await
                    .change_context(ClientError::ClockDetachRequest)?;
                clock_detach_completed(outcome)
            }
            Self::Interrupted | Self::Ended => Ok(()),
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
        DomainClockAttachDisposition::Refused(refusal) => Err(StackReport::new(
            ClientError::ClockAttachRefused { refusal },
        )
        .attach_printable(outcome.message)),
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
        Some(path) => Some(std::fs::read(path).change_context(ClientError::LoadTlsCaCertificate)?),
        None => None,
    };
    let password = match args.password.clone() {
        Some(password) => password,
        None => rpassword::prompt_password(format!("Password for {}: ", args.username))
            .map_err(|cause| StackReport::new(cause).change_context(ClientError::ReadPassword))?,
    };
    Ok(ConnectOptions {
        dns: ConnectDns::Configuration(DnsConfiguration {
            settings: nervix_dns::DnsSettings::default(),
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

    fn gap_line(dropped: u64) -> Option<String> {
        if dropped == 0 {
            return None;
        }
        Some(format!(
            "[events] notice: {dropped} event lines were omitted because terminal output could \
             not keep up"
        ))
    }
}

/// The editor owns the existing queue's receiver. Each poll takes at most one queue's capacity,
/// so a producer that keeps refilling it cannot prevent input and repainting from making progress.
/// Between read_line calls the same queue retains statement-time events under its original bound.
struct ReplEventOutput {
    receiver: nervix_primitives::sync::mpsc::Receiver<String>,
    sender: EventLineSender,
}

impl ExternalOutput for ReplEventOutput {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            library,
            reason = "reedline requires its ExternalOutput callback to return io::Result; this \
                      bounded queue drain performs no I/O"
        )
    )]
    fn drain(&mut self) -> std::io::Result<Vec<String>> {
        let mut lines = Vec::new();
        for _ in 0..EVENT_BUFFER_RECORDS {
            let Ok(line) = self.receiver.try_recv() else {
                break;
            };
            lines.push(line);
        }
        if let Some(gap) = EventLineSender::gap_line(self.sender.take_dropped()) {
            lines.push(gap);
        }
        Ok(lines)
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
    if let Some(gap) = EventLineSender::gap_line(dropped) {
        output.print(&gap);
    }
}

fn spawn_event_collectors(
    client: Client,
    sender: EventLineSender,
    subscription_end: SubscriptionEndPolicy,
) -> JoinHandle<RunEnd> {
    let subscription = nervix_primitives::task::spawn(EventStream::Subscriptions.collect(
        client.clone(),
        sender.clone(),
        subscription_end,
    ));
    for stream in [EventStream::DomainClocks, EventStream::ServerNotices] {
        nervix_primitives::task::spawn(stream.collect(
            client.clone(),
            sender.clone(),
            SubscriptionEndPolicy::Continue,
        ));
    }
    subscription
}

/// Whether the caller observes all subscriptions of a session, or follows the one subscription
/// its dedicated subcommand opened and finishes when that subscription ends.
#[derive(Debug, Clone, Copy)]
enum SubscriptionEndPolicy {
    Continue,
    Finish,
}

/// Rendered event lines and the terminal subscription event that a dedicated follower awaits.
enum EventLines {
    Observed(Vec<String>),
    SubscriptionEnded(Vec<String>),
}

impl EventLines {
    fn ends_subscription(&self, policy: SubscriptionEndPolicy) -> bool {
        matches!(
            (self, policy),
            (Self::SubscriptionEnded(_), SubscriptionEndPolicy::Finish)
        )
    }

    fn into_lines(self) -> Vec<String> {
        match self {
            Self::Observed(lines) | Self::SubscriptionEnded(lines) => lines,
        }
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
    /// Prints the stream's events for as long as the client can open a session. The client
    /// restores subscriptions and clocks on its next session and notices continue there, so every
    /// other failure is printed and reading goes on.
    async fn collect(
        self,
        client: Client,
        sender: EventLineSender,
        subscription_end: SubscriptionEndPolicy,
    ) -> RunEnd {
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_lines(&client).await {
                Ok(event) => {
                    let ended = event.ends_subscription(subscription_end);
                    for line in event.into_lines() {
                        sender.push(line);
                    }
                    if ended {
                        return RunEnd::Completed;
                    }
                }
                Err(error) => {
                    let failure = self.failure(&error);
                    sender.push(failure.line);
                    if failure.ended {
                        return RunEnd::Failed;
                    }
                }
            }
        }
    }

    /// The terminal lines of the stream's next event.
    async fn next_lines(self, client: &Client) -> Result<EventLines, StackReport<CoreClientError>> {
        match self {
            Self::Subscriptions => {
                let event = client.next_subscription().await?;
                let lines = format_subscription_event(&event);
                if let SubscriptionEvent::Ended(_) = event {
                    Ok(EventLines::SubscriptionEnded(lines))
                } else {
                    Ok(EventLines::Observed(lines))
                }
            }
            Self::DomainClocks => {
                let event = client.next_domain_clock_event().await?;
                Ok(EventLines::Observed(vec![format_domain_clock_event(
                    &event,
                )]))
            }
            Self::ServerNotices => {
                let event = client.next_server_event().await?;
                Ok(EventLines::Observed(vec![format_server_event(&event)]))
            }
        }
    }

    /// What the terminal prints when a read of the stream fails. Only a session that no known
    /// server can reopen ends the stream.
    fn failure(self, error: &StackReport<CoreClientError>) -> StreamFailure {
        let stream = self.as_ref();
        match error.current_context() {
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
            _ => StreamFailure {
                line: format!(
                    "[events] notice: {stream} could not resume yet: {error:#}; the client keeps \
                     trying"
                ),
                ended: false,
            },
        }
    }
}

fn spawn_event_loggers(
    client: Client,
    output: EventOutput,
    subscription_end: SubscriptionEndPolicy,
) -> JoinHandle<RunEnd> {
    let (sender, mut receiver) = nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
    let sender = EventLineSender::new(sender);
    let dropped = sender.dropped.clone();
    let subscription = spawn_event_collectors(client, sender, subscription_end);
    nervix_primitives::task::spawn_blocking(move || {
        while let Some(line) = receiver.blocking_recv() {
            print_event_gap(dropped.swap(0, Ordering::Relaxed), output);
            output.print(&line);
        }
        print_event_gap(dropped.swap(0, Ordering::Relaxed), output);
    });
    subscription
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
            "[events] subscription [{subscription}] notice: the subscription ended because {}: {}",
            ended.reason, ended.message
        )],
        SubscriptionEvent::Interrupted(_) => vec![format!(
            "[events] subscription [{subscription}] notice: delivery was interrupted; rows may be \
             missing before restoration"
        )],
        SubscriptionEvent::RestorationFailed(failure) => vec![format!(
            "[events] subscription [{subscription}] notice: opening the subscription again failed \
             because {}: {}; the next attempt follows in {:?}",
            failure.cause, failure.message, failure.retry_after
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
            "[events] domain clock [{}] notice: attaching the clock again failed because {}: {}; \
             the next attempt follows in {:?}",
            failure.domain, failure.cause, failure.message, failure.retry_after
        ),
    }
}

fn format_server_event(event: &ServerEvent) -> String {
    format!(
        "[events] {} {}: {}",
        notice_kind_label(event.kind),
        notice_level_label(event.level),
        event.message
    )
}

/// The label a server notice prints with, naming the kind of event the server sent it as.
fn notice_kind_label(kind: NoticeKind) -> &'static str {
    match kind {
        NoticeKind::Membership => "membership",
        NoticeKind::Leadership => "leadership",
        NoticeKind::ClusterParameters => "cluster parameters",
        NoticeKind::Runtime => "runtime",
        NoticeKind::ControlPlane => "control plane",
    }
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
    let where_clause = nervix_plexus::parse_expression(where_clause)
        .change_context(ClientError::InvalidSubscriptionWhere)?;
    Ok(request.with_where_clause(where_clause))
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
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
        .assured("Plexus accepts the global backup wait option");
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
        let connector = EventLineSender::new(sender);
        for _ in 0..EVENT_BUFFER_RECORDS {
            connector.push("é".repeat(EVENT_LINE_BYTES));
        }
        connector.push("one more line".to_string());
        assert_eq!(connector.take_dropped(), 1);
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
        let (sender, receiver) = nervix_primitives::sync::mpsc::channel(2);
        let connector = EventLineSender::new(sender);
        connector.push("first".to_string());
        connector.push("second".to_string());
        connector.push("dropped".to_string());

        let mut output = ReplEventOutput {
            receiver,
            sender: connector,
        };
        let lines = output.drain().assured("the queue drain performs no I/O");

        assert_eq!(
            lines,
            [
                "first",
                "second",
                "[events] notice: 1 event lines were omitted because terminal output could not \
                 keep up",
            ]
        );
        assert!(
            output
                .drain()
                .assured("the queue drain performs no I/O")
                .is_empty()
        );
    }

    #[test]
    fn an_idle_editor_reports_a_gap_without_waiting_for_another_event() {
        let (sender, receiver) = nervix_primitives::sync::mpsc::channel(EVENT_BUFFER_RECORDS);
        let sender = EventLineSender::new(sender);
        sender.dropped.store(3, Ordering::Relaxed);
        let mut output = ReplEventOutput { receiver, sender };

        assert_eq!(
            output.drain().assured("the queue drain performs no I/O"),
            [
                "[events] notice: 3 event lines were omitted because terminal output could not \
                 keep up"
            ]
        );
        assert!(
            output
                .drain()
                .assured("the queue drain performs no I/O")
                .is_empty()
        );
    }

    #[test]
    fn an_event_gap_clamps_and_a_closed_printer_discards_new_lines() {
        let (sender, receiver) = nervix_primitives::sync::mpsc::channel(1);
        let connector = EventLineSender::new(sender);
        connector.push("retained".to_string());
        connector.dropped.store(u64::MAX, Ordering::Relaxed);
        connector.push("overflow".to_string());
        assert_eq!(connector.take_dropped(), u64::MAX);

        drop(receiver);
        connector.push("printer stopped".to_string());
        assert_eq!(connector.take_dropped(), 0);
    }

    #[test]
    fn one_shot_json_mode_requires_one_complete_inspection_statement() {
        assert!(is_json_inspection_command(
            "DESCRIBE TRANSACTION 'tx-1' OPERATION 1 FORMAT JSON;"
        ));
        assert!(is_json_inspection_command(
            "SHOW RELAY events MATERIALIZED STATE FOR UNBRANCHED LIMIT 1 FORMAT JSON;"
        ));
        assert!(is_json_inspection_command(
            "DESCRIBE WASM PROCESSOR worker FORMAT JSON;"
        ));
        assert!(!is_json_inspection_command(
            "SHOW RELAY events MATERIALIZED STATE FORMAT TEXT;"
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
            disposition: DomainClockAttachDisposition::Refused(
                DomainClockAttachRefusal::InTransaction,
            ),
            message: "session-scoped and client-local statements cannot be queued in a transaction"
                .to_string(),
        };
        let Err(error) = clock_attach_message(refused) else {
            panic!("a refused clock attachment must fail");
        };
        assert!(matches!(
            error.current_context(),
            ClientError::ClockAttachRefused {
                refusal: DomainClockAttachRefusal::InTransaction
            }
        ));
        assert_eq!(
            error.current_context().to_string(),
            "domain clock attachment was refused because the session holds a transaction"
        );
        assert!(format!("{error:?}").contains("cannot be queued in a transaction"));
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
        use nervix_plexus::client_statement::parse_use_domain;

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
            "CREATE HTTP CLIENT http_main CONFIG { 'endpoint' = 'http://example.com/a;b', \
             'timeout_ms' = 5000 }\n"
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
    fn every_server_notice_is_labelled_by_its_kind() {
        /// One notice and the line the REPL prints for it.
        struct Printed {
            notice: ServerEvent,
            line: &'static str,
        }
        let printed = [
            Printed {
                notice: ServerEvent {
                    level: NoticeLevel::Info,
                    kind: NoticeKind::Membership,
                    message: "node 'node-3' left".to_string(),
                },
                line: "[events] membership INFO: node 'node-3' left",
            },
            Printed {
                notice: ServerEvent {
                    level: NoticeLevel::Info,
                    kind: NoticeKind::Leadership,
                    message: "raft transition: state=Leader leader=node-1 term=2".to_string(),
                },
                line: "[events] leadership INFO: raft transition: state=Leader leader=node-1 \
                       term=2",
            },
            Printed {
                notice: ServerEvent {
                    level: NoticeLevel::Info,
                    kind: NoticeKind::ClusterParameters,
                    message: "cluster parameters committed at revision 3".to_string(),
                },
                line: "[events] cluster parameters INFO: cluster parameters committed at revision \
                       3",
            },
            Printed {
                notice: ServerEvent {
                    level: NoticeLevel::Error,
                    kind: NoticeKind::Runtime,
                    message: "relay 'orders' failed".to_string(),
                },
                line: "[events] runtime ERROR: relay 'orders' failed",
            },
            Printed {
                notice: ServerEvent {
                    level: NoticeLevel::Warning,
                    kind: NoticeKind::ControlPlane,
                    message: "resource refresh was delayed".to_string(),
                },
                line: "[events] control plane WARN: resource refresh was delayed",
            },
        ];
        for Printed { notice, line } in printed {
            assert_eq!(format_server_event(&notice), line);
        }
    }

    #[test]
    fn a_notice_is_labelled_by_its_kind_whatever_its_text_says() {
        let rendered = format_server_event(&ServerEvent {
            level: NoticeLevel::Error,
            kind: NoticeKind::Runtime,
            message: "raft transition: state=Leader".to_string(),
        });
        assert_eq!(
            rendered,
            "[events] runtime ERROR: raft transition: state=Leader"
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
                "[events] subscription [live] notice: the subscription ended because its relay \
                 was redefined: relay 'orders' was redefined"
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
                cause: nervix_client_core::SubscriptionRestorationCause::Unavailable,
                message: "the serving node is catching up with committed domain state".to_string(),
                retry_after: std::time::Duration::from_secs(2),
            },
        );
        assert_eq!(
            format_subscription_event(&refused),
            [
                "[events] subscription [live] notice: opening the subscription again failed \
                 because the serving node could not open it now: the serving node is catching up \
                 with committed domain state; the next attempt follows in 2s"
            ]
        );
    }

    #[test]
    fn a_failed_event_read_is_printed_and_only_an_unrecoverable_session_ends_the_stream() {
        assert_eq!(
            EventStream::DomainClocks.failure(&StackReport::new(CoreClientError::RetryDeadline)),
            StreamFailure {
                line: "[events] notice: domain clock events could not resume yet: session retry \
                       deadline expired; the client keeps trying"
                    .to_string(),
                ended: false,
            }
        );
        assert_eq!(
            EventStream::ServerNotices.failure(&StackReport::new(CoreClientError::EventOverflow {
                stream: nervix_client_core::EventStreamKind::ServerNotice,
            })),
            StreamFailure {
                line: "[events] notice: server notices were dropped because they arrived faster \
                       than they were read"
                    .to_string(),
                ended: false,
            }
        );
        assert_eq!(
            EventStream::Subscriptions.failure(&StackReport::new(CoreClientError::SessionClosed)),
            StreamFailure {
                line: "[events] notice: subscription events stopped because the session closed \
                       and no known server can reopen it"
                    .to_string(),
                ended: true,
            }
        );
        let unreadable = StackReport::new(std::io::Error::from(std::io::ErrorKind::NotFound))
            .change_context(CoreClientError::LoadDnsConfiguration);
        assert_eq!(
            EventStream::ServerNotices.failure(&unreadable),
            StreamFailure {
                line: "[events] notice: server notices could not resume yet: failed to load \
                       native DNS configuration: entity not found; the client keeps trying"
                    .to_string(),
                ended: false,
            },
            "a failed read prints every context of its report"
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
                cause: nervix_client_core::DomainClockRestorationCause::InTransaction,
                message: "client-local statements cannot be queued in a transaction".to_string(),
                retry_after: std::time::Duration::from_secs(4),
            },
        );
        assert_eq!(
            format_domain_clock_event(&refused),
            "[events] domain clock [sim] notice: attaching the clock again failed because the \
             session holds a transaction: client-local statements cannot be queued in a \
             transaction; the next attempt follows in 4s"
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
