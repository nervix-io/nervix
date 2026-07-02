use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use ariadne::{Color, Label, Report, ReportKind, Source};
use byte_unit::{Byte, UnitType};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use error_stack::Report as StackReport;
use nervix_client_core::{
    AutocompleteSuggestion, Client, ClientError as CoreClientError, CommandOutcomeKind,
    ConnectOptions, Diagnostic, SubscriptionDeliveryBehavior, SubscriptionRequest,
    SuggestionKind as ClientSuggestionKind, TlsRequirement,
};
use nervix_nspl::client_statement::{
    parse_client_statements, parse_upload_resource_query, upload_resource_path_fragment,
};
use reedline::{
    Completer, DefaultHinter, DefaultPrompt, DefaultPromptSegment, Emacs, FileBackedHistory,
    KeyCode, KeyModifiers, ListMenu, MenuBuilder, Reedline, ReedlineEvent, ReedlineMenu, Signal,
    Suggestion,
};
use thiserror::Error;
use tokio::{runtime::Handle, signal, task::block_in_place};

const HISTORY_FILE: &str = ".nervix_client_history";

#[derive(Parser, Debug, Clone)]
#[command(name = "nervix-cli")]
#[command(about = "Interactive Nervix client")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:47391")]
    server: String,
    #[arg(long, value_enum, default_value_t = CliTlsRequirement::Preferred)]
    tls: CliTlsRequirement,
    #[arg(long)]
    tls_ca_cert: Option<PathBuf>,
    #[arg(long, default_value = "default")]
    domain: String,
    #[arg(long, env = "NERVIX_USERNAME", default_value = "default")]
    username: String,
    #[arg(long, env = "NERVIX_PASSWORD")]
    password: Option<String>,
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
        /// Optional session-level FILTER-MAP program applied to delivered records
        #[arg(long)]
        filter_map: Option<String>,
    },
    /// Remove a node from the cluster membership
    RemoveNode {
        /// Node id to remove
        node_id: String,
    },
    /// Prevent the scheduler from placing new tasks on a node
    CordonNode {
        /// Node id to cordon
        node_id: String,
    },
    /// Allow the scheduler to place new tasks on a node
    UncordonNode {
        /// Node id to uncordon
        node_id: String,
    },
    /// Move scheduled graph nodes away from a node and keep it cordoned
    DrainNode {
        /// Node id to drain
        node_id: String,
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
}

impl Completer for GrpcCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let prefix = self
            .buffer_prefix
            .lock()
            .map(|value| value.clone())
            .unwrap_or_default();
        let combined = format!("{}{}", prefix, &line[..pos.min(line.len())]);
        let cursor = u32::try_from(combined.len()).unwrap_or(u32::MAX);
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
    line[..pos.min(line.len())]
        .char_indices()
        .rev()
        .find(|(_, c)| !is_word(*c))
        .map(|(idx, c)| idx + c.len_utf8())
        .unwrap_or(0)
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
            relay,
            dropping,
            blocking,
            batch_sample_rate,
            filter_map,
        }) => {
            let connect_options = connect_options_from_args(&args)?;
            return run_subscribe_mode(
                &args.server,
                connect_options,
                args.domain,
                relay,
                if dropping {
                    SubscriptionDeliveryBehavior::Dropping
                } else if blocking {
                    SubscriptionDeliveryBehavior::Blocking
                } else {
                    SubscriptionDeliveryBehavior::Blocking
                },
                batch_sample_rate,
                filter_map,
            )
            .await;
        }
        Some(Command::RemoveNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client =
                Client::connect_with_options(&args.server, args.domain.clone(), connect_options)
                    .await
                    .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("DROP NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::CordonNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client =
                Client::connect_with_options(&args.server, args.domain.clone(), connect_options)
                    .await
                    .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("CORDON NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::UncordonNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client =
                Client::connect_with_options(&args.server, args.domain.clone(), connect_options)
                    .await
                    .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("UNCORDON NODE {node_id};")).await?;
            return Ok(());
        }
        Some(Command::DrainNode { node_id }) => {
            let connect_options = connect_options_from_args(&args)?;
            let client =
                Client::connect_with_options(&args.server, args.domain.clone(), connect_options)
                    .await
                    .map_err(|err| StackReport::new(ClientError::from(err)))?;
            execute_and_print(&client, format!("DRAIN NODE {node_id};")).await?;
            return Ok(());
        }
        None => {}
    }

    let connect_options = connect_options_from_args(&args)?;
    let client = Client::connect_with_options(&args.server, args.domain.clone(), connect_options)
        .await
        .map_err(|err| StackReport::new(ClientError::from(err)))?;
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
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
        let active_domain = client.domain().await;
        let prompt_domain = if client.transaction_active().await {
            format!("{active_domain} tx")
        } else {
            active_domain
        };
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
    let path_fragment = lookup_hint
        .map(|hint| hint.value.as_str())
        .filter(|hint| !hint.is_empty() || line[..pos.min(line.len())].contains(" VERSION '"))
        .or_else(|| upload_resource_path_fragment(line, pos))?;
    let span_start = pos.saturating_sub(path_fragment.len());
    let path = Path::new(path_fragment);
    let (base_dir, partial_name) = if path_fragment.is_empty() {
        (PathBuf::from("."), String::new())
    } else if path_fragment.ends_with(std::path::MAIN_SEPARATOR) || path_fragment.ends_with('/') {
        (expand_user_path(path), String::new())
    } else {
        (
            expand_user_path(
                path.parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| PathBuf::from(".")),
            ),
            path.file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default(),
        )
    };
    let mut suggestions = std::fs::read_dir(&base_dir)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            if !partial_name.is_empty() && !name.starts_with(&partial_name) {
                return None;
            }
            let value = if uses_home_prefix(path_fragment) {
                let relative_base = strip_home_prefix(&base_dir)?;
                let relative = if relative_base.as_os_str().is_empty() {
                    format!("~/{name}")
                } else {
                    format!("~/{}/{}", relative_base.display(), name)
                };
                relative
            } else if base_dir == PathBuf::from(".") {
                name.clone()
            } else {
                base_dir.join(&name).display().to_string()
            };
            let is_dir = entry.file_type().ok()?.is_dir();
            Some(Suggestion {
                value: if is_dir { format!("{value}/") } else { value },
                description: None,
                style: None,
                extra: None,
                span: reedline::Span::new(span_start, pos),
                append_whitespace: false,
            })
        })
        .collect::<Vec<_>>();
    suggestions.sort_by(|left, right| left.value.cmp(&right.value));
    Some(suggestions)
}

fn expand_user_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    if raw == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf());
    }
    if let Some(stripped) = raw.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
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

async fn run_subscribe_mode(
    server: &str,
    connect_options: ConnectOptions,
    domain: String,
    relay: String,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<String>,
    filter_map: Option<String>,
) -> Result<(), StackReport<ClientError>> {
    let client = Client::connect_with_options(server, domain, connect_options)
        .await
        .map_err(|err| StackReport::new(ClientError::from(err)))?;
    spawn_event_loggers(client.clone());
    let request = subscribe_request(
        &relay,
        delivery_behavior,
        batch_sample_rate.as_deref(),
        filter_map.as_deref(),
    );
    let query = request.to_query();
    let result = client
        .subscribe(&request)
        .await
        .map_err(|err| StackReport::new(ClientError::from(err)))?;
    if !result.success {
        println!("error: {}", result.message);
        if result.diagnostics.is_empty() {
            println!("- no diagnostics provided");
        } else {
            print_diagnostics("subscribe", &query, &result.diagnostics);
        }
        return Ok(());
    }

    println!("{}", result.message);
    println!("listening for events from relay '{relay}'. Press Ctrl-C to stop.");
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
    if !result.results.is_empty() {
        for item in &result.results {
            print_command_outcome(item, &query_source);
        }
        return Ok(());
    }
    print_command_outcome(&result, &query_source);

    Ok(())
}

fn print_command_outcome(result: &nervix_client_core::CommandOutcome, query_source: &str) {
    if result.success {
        emit_terminal_line(result.message.clone());
    } else {
        match result.kind {
            CommandOutcomeKind::NotLeader => {
                if let (Some(leader), Some(leader_grpc_uri)) =
                    (result.leader.as_deref(), result.leader_grpc_uri.as_deref())
                {
                    emit_terminal_line(
                        "topology: not-a-leader, retry on leader '{leader}' at {leader_grpc_uri}"
                            .replace("{leader}", leader)
                            .replace("{leader_grpc_uri}", leader_grpc_uri),
                    );
                } else if let Some(leader) = result.leader.as_deref() {
                    emit_terminal_line(format!(
                        "topology: not-a-leader, retry on leader '{leader}'"
                    ));
                } else {
                    emit_terminal_line("topology: not-a-leader");
                }
            }
            _ => emit_terminal_line(format!("error: {}", result.message)),
        }
        if result.diagnostics.is_empty() {
            emit_terminal_line("- no diagnostics provided");
        } else {
            print_diagnostics("remote", query_source, &result.diagnostics);
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
    let waiting_for_replication = Arc::new(AtomicBool::new(false));
    let progress_uploaded = Arc::clone(&uploaded);
    let progress_finished = Arc::clone(&finished);
    let progress_waiting = Arc::clone(&waiting_for_replication);
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
            let phase = if progress_waiting.load(Ordering::Relaxed) {
                "waiting for cluster replication"
            } else {
                "streaming archive"
            };
            render_progress_line(format!(
                "{} upload resource '{}' {} ({})",
                frames[frame_index % frames.len()],
                progress_identifier,
                human_bytes(bytes),
                phase,
            ));
            frame_index += 1;
        }
    });

    let outcome = client
        .upload_resource_from_directory(&identifier, &directory, {
            let uploaded = Arc::clone(&uploaded);
            let waiting_for_replication = Arc::clone(&waiting_for_replication);
            move |bytes| {
                uploaded.fetch_add(bytes, Ordering::Relaxed);
                waiting_for_replication.store(false, Ordering::Relaxed);
            }
        })
        .await;

    waiting_for_replication.store(true, Ordering::Relaxed);
    finished.store(true, Ordering::Relaxed);
    let _ = progress_task.await;
    let total_uploaded = uploaded.load(Ordering::Relaxed);
    clear_progress_line();
    let result = outcome.map_err(|err| StackReport::new(ClientError::from(err)))?;
    if result.success {
        emit_terminal_line(format!(
            "upload resource '{}' finished: {} sent, replication complete",
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
    let _ = io::stdout().flush();
}

fn clear_progress_line() {
    print!("\r\x1b[2K");
    let _ = io::stdout().flush();
}

fn human_bytes(bytes: u64) -> String {
    let adjusted = Byte::from_u64(bytes).get_appropriate_unit(UnitType::Binary);
    format!("{adjusted:.1}")
}

fn spawn_event_collectors(client: Client, sender: tokio::sync::mpsc::UnboundedSender<String>) {
    let subscription_client = client.clone();
    let subscription_sender = sender.clone();
    tokio::spawn(async move {
        while let Ok(event) = subscription_client.next_subscription().await {
            tokio::task::consume_budget().await;
            let _ = subscription_sender.send(format!(
                "[events] subscription [{}] from [{}]: {}",
                event.subscription, event.relay, event.payload
            ));
        }
    });

    tokio::spawn(async move {
        while let Ok(event) = client.next_server_event().await {
            tokio::task::consume_budget().await;
            let _ = sender.send(format_server_event(&event));
        }
    });
}

fn spawn_event_loggers(client: Client) {
    let subscription_client = client.clone();
    tokio::spawn(async move {
        while let Ok(event) = subscription_client.next_subscription().await {
            tokio::task::consume_budget().await;
            println!(
                "[events] subscription [{}] from [{}]: {}",
                event.subscription, event.relay, event.payload
            );
        }
    });

    tokio::spawn(async move {
        while let Ok(event) = client.next_server_event().await {
            tokio::task::consume_budget().await;
            println!("{}", format_server_event(&event));
        }
    });
}

fn drain_event_queue(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<String>) {
    while let Ok(line) = receiver.try_recv() {
        println!("{line}");
    }
}

fn format_server_event(event: &nervix_client_core::ServerEvent) -> String {
    let label = if event.message.starts_with("raft transition:") {
        "topology"
    } else {
        "server"
    };
    format!("[events] {} {}: {}", label, event.level, event.message)
}

fn subscribe_request(
    relay: &str,
    delivery_behavior: SubscriptionDeliveryBehavior,
    batch_sample_rate: Option<&str>,
    filter_map: Option<&str>,
) -> SubscriptionRequest {
    let request = match delivery_behavior {
        SubscriptionDeliveryBehavior::Blocking => SubscriptionRequest::new(relay).blocking(),
        SubscriptionDeliveryBehavior::Dropping => SubscriptionRequest::new(relay).dropping(),
    };
    let request = match batch_sample_rate {
        Some(batch_sample_rate) => request.with_batch_sample_rate(batch_sample_rate),
        None => request,
    };
    match filter_map {
        Some(filter_map) => request.with_filter_map(filter_map),
        None => request,
    }
}

fn print_diagnostics(source_id: &str, source: &str, diagnostics: &[Diagnostic]) {
    for diagnostic in diagnostics {
        let start = usize::try_from(diagnostic.span_start).unwrap_or(0);
        let mut end = usize::try_from(diagnostic.span_end).unwrap_or(start);
        if end < start {
            end = start;
        }
        let end = end.min(source.len());
        let start = start.min(end);

        let report = Report::build(ReportKind::Error, (source_id, start..end))
            .with_message("server parse error")
            .with_label(
                Label::new((source_id, start..end))
                    .with_message(diagnostic.message.clone())
                    .with_color(Color::Red),
            )
            .finish();

        if let Err(err) = report.eprint((source_id, Source::from(source))) {
            eprintln!("failed to render diagnostic: {err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_client_core::{ServerEvent, ServerEventLevel};

    use super::*;

    #[test]
    fn args_defaults_are_applied_without_subcommand() {
        let args = Args::parse_from(["nervix-cli"]);
        assert_eq!(args.server, "http://127.0.0.1:47391");
        assert_eq!(args.tls, CliTlsRequirement::Preferred);
        assert_eq!(args.tls_ca_cert, None);
        assert_eq!(args.domain, "default");
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
        assert_eq!(args.domain, "tenant_a");
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
        let args = Args::parse_from(["nervix-cli", "subscribe", "myss"]);
        match args.subcommand {
            Some(Command::Subscribe {
                relay,
                dropping,
                blocking,
                batch_sample_rate,
                filter_map,
            }) => {
                assert_eq!(relay, "myss");
                assert!(!dropping);
                assert!(!blocking);
                assert_eq!(batch_sample_rate, None);
                assert_eq!(filter_map, None);
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn remove_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "remove-node", "node-2"]);
        match args.subcommand {
            Some(Command::RemoveNode { node_id }) => assert_eq!(node_id, "node-2"),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn cordon_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "cordon-node", "node-2"]);
        match args.subcommand {
            Some(Command::CordonNode { node_id }) => assert_eq!(node_id, "node-2"),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn uncordon_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "uncordon-node", "node-2"]);
        match args.subcommand {
            Some(Command::UncordonNode { node_id }) => assert_eq!(node_id, "node-2"),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn drain_node_command_is_parsed() {
        let args = Args::parse_from(["nervix-cli", "drain-node", "node-2"]);
        match args.subcommand {
            Some(Command::DrainNode { node_id }) => assert_eq!(node_id, "node-2"),
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn subscribe_query_uses_collect_form() {
        assert_eq!(
            subscribe_request("myss", SubscriptionDeliveryBehavior::Blocking, None, None)
                .to_query(),
            "SUBSCRIBE SESSION TO myss;"
        );
        assert_eq!(
            subscribe_request(
                "myss",
                SubscriptionDeliveryBehavior::Blocking,
                None,
                Some("SET seen = true WHERE tenant = \"acme\"")
            )
            .to_query(),
            "SUBSCRIBE SESSION TO myss SET seen = true WHERE tenant = \"acme\";"
        );
        assert_eq!(
            subscribe_request(
                "myss",
                SubscriptionDeliveryBehavior::Dropping,
                Some("0.1"),
                Some("WHERE tenant = \"acme\"")
            )
            .to_query(),
            "SUBSCRIBE SESSION TO myss DROPPING BATCH SAMPLE RATE 0.1 WHERE tenant = \"acme\";"
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
        let request =
            subscribe_request("events", SubscriptionDeliveryBehavior::Blocking, None, None);
        assert_eq!(request.relay, "events");
        assert_eq!(request.filter_map, None);
    }

    #[test]
    fn subscribe_command_parses_filter_map() {
        let args = Args::parse_from([
            "nervix-cli",
            "subscribe",
            "events",
            "--filter-map",
            "SET seen = true WHERE tenant = \"acme\"",
        ]);
        match args.subcommand {
            Some(Command::Subscribe {
                relay, filter_map, ..
            }) => {
                assert_eq!(relay, "events");
                assert_eq!(
                    filter_map.as_deref(),
                    Some("SET seen = true WHERE tenant = \"acme\"")
                );
            }
            other => panic!("unexpected subcommand: {other:?}"),
        }
    }

    #[test]
    fn raft_transition_server_events_are_labeled_as_topology() {
        let rendered = format_server_event(&ServerEvent {
            level: ServerEventLevel::Info,
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
            level: ServerEventLevel::Warn,
            message: "runtime warning".to_string(),
        });
        assert_eq!(rendered, "[events] server WARN: runtime warning");
    }
}
