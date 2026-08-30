use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{FutureExt, SinkExt, StreamExt};
use gloo_net::websocket::{
    Message as WebSocketMessage, State as WebSocketState, futures::WebSocket,
};
use leptos::{ev, mount::mount_to_body, prelude::*};
use nervix_dataflow_graph::{
    DataflowBranch, DataflowEdgeKind, DataflowGraph, DataflowInputSide, DataflowNodeKind,
    DataflowNodeRole, DataflowNodeStatus, DataflowProcessorKind, DataflowSchemaField,
    DataflowStatistics,
};
use nervix_models::Statement;
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statement, parse_client_statements, parse_use_domain,
};
use nervix_web_console::graph::{
    graph_layout_edge, graph_layout_item,
    layout::{GroupRegion, Layout, Rect},
};
use prost::Message as ProstMessage;
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

const RUNTIME_VERSION_LABEL: &str = concat!("nervix runtime v", env!("CARGO_PKG_VERSION"));
const WEBSOCKET_INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const WEBSOCKET_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsoleConnectionState {
    Connecting,
    Connected,
    Waiting,
}

#[derive(Clone)]
struct WebConsoleSession {
    state: RwSignal<ConsoleConnectionState>,
    request_tx: RwSignal<Option<UnboundedSender<QueuedRequest>>>,
    upload_base_url: RwSignal<Option<String>>,
    auth_token: RwSignal<Option<String>>,
}

#[derive(Clone, Copy)]
struct WebConsoleSignals {
    terminal_lines: RwSignal<Vec<TermLine>>,
    suggestions: RwSignal<Vec<String>>,
    domain_snapshots: RwSignal<Vec<DomainSnapshotView>>,
    cluster_counters: RwSignal<ClusterCounters>,
    active_domain: RwSignal<Option<String>>,
    transaction_status: RwSignal<Option<nervix_proto::TransactionStatus>>,
    domains: RwSignal<Vec<DomainView>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    domains_loaded: RwSignal<bool>,
    user_selected_domain: RwSignal<bool>,
    auth_token: RwSignal<Option<String>>,
    auth_error: RwSignal<Option<String>>,
}

#[derive(Clone)]
enum QueuedRequest {
    Command {
        query: String,
        request: nervix_proto::SessionRequest,
    },
    SubscriptionStart {
        tab_id: u64,
        request: nervix_proto::SessionRequest,
    },
    SubscriptionStop {
        request: nervix_proto::SessionRequest,
    },
    ResourceDescribe {
        resource: String,
        request: nervix_proto::SessionRequest,
    },
    SetActiveDomain {
        request: nervix_proto::SessionRequest,
    },
    Suggest {
        request: nervix_proto::SessionRequest,
    },
}

#[derive(Clone)]
struct QueuedCommand {
    query: String,
    request: nervix_proto::SessionRequest,
}

#[derive(Clone)]
enum PendingRequest {
    AttachTransaction {
        request: nervix_proto::SessionRequest,
    },
    Command(QueuedCommand),
    SubscriptionStart {
        tab_id: u64,
        request: nervix_proto::SessionRequest,
    },
    SubscriptionStop {
        request: nervix_proto::SessionRequest,
    },
    ResourceDescribe {
        resource: String,
        request: nervix_proto::SessionRequest,
    },
}

impl QueuedRequest {
    fn request(&self) -> &nervix_proto::SessionRequest {
        match self {
            Self::Command { request, .. }
            | Self::SubscriptionStart { request, .. }
            | Self::SubscriptionStop { request, .. }
            | Self::ResourceDescribe { request, .. }
            | Self::SetActiveDomain { request }
            | Self::Suggest { request } => request,
        }
    }
}

impl PendingRequest {
    fn request(&self) -> &nervix_proto::SessionRequest {
        match self {
            Self::AttachTransaction { request } => request,
            Self::Command(command) => &command.request,
            Self::SubscriptionStart { request, .. } | Self::SubscriptionStop { request, .. } => {
                request
            }
            Self::ResourceDescribe { request, .. } => request,
        }
    }
}

#[derive(Clone)]
struct SubscriptionTabView {
    id: u64,
    state: SubscriptionTabState,
    name: String,
    domain: String,
    relay: String,
    filter: String,
    sample_rate_index: usize,
    title: String,
    subscribe_command: String,
    unsubscribe_command: String,
    lines: Vec<TermLine>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubscriptionTabState {
    Pending,
    Open,
}

#[derive(Clone, Default)]
struct ResourceDetailView {
    versions: Vec<ResourceVersionView>,
    status: String,
}

#[derive(Clone, Default)]
struct ResourceVersionView {
    version: String,
    root_checksum: Option<String>,
    manifest_checksum: Option<String>,
    file_count: Option<String>,
    total_bytes: Option<String>,
    created_by_node: Option<String>,
    created_at: Option<String>,
    files: Vec<ResourceFileView>,
}

#[derive(Clone, Default)]
struct ResourceFileView {
    path: String,
    entry_type: String,
    size: Option<String>,
    checksum: Option<String>,
}

impl ConsoleConnectionState {
    fn label(self) -> &'static str {
        match self {
            Self::Connecting => "CONNECTING",
            Self::Connected => "CONNECTED",
            Self::Waiting => "WAITING",
        }
    }

    fn pill_class(self) -> &'static str {
        match self {
            Self::Connecting => "pill connecting",
            Self::Connected => "pill ok",
            Self::Waiting => "pill waiting",
        }
    }
}

const THEMES: [ThemeView; 4] = [
    ThemeView {
        id: "nebula",
        label: "Dark navy",
        swatches: ["#070b18", "#06b6d4", "#885cf6"],
    },
    ThemeView {
        id: "obsidian",
        label: "Pure dark",
        swatches: ["#09090e", "#06b6d4", "#a78bfa"],
    },
    ThemeView {
        id: "d0znpp",
        label: "D0ZNPP",
        swatches: ["#ffffff", "#f05500", "#1a1a1a"],
    },
    ThemeView {
        id: "aurora",
        label: "Light",
        swatches: ["#f0f4ff", "#0891b2", "#7c3aed"],
    },
];

/// The zoom range the stage allows, shared by the buttons, the wheel and the fit control.
const GRAPH_MIN_ZOOM: f64 = 0.25;
const GRAPH_MAX_ZOOM: f64 = 3.0;
/// One press of a zoom button.
const GRAPH_ZOOM_STEP: f64 = 0.1;
/// Fitting never enlarges: a small graph is shown at its natural size, centred.
const GRAPH_FIT_MAX_ZOOM: f64 = 1.0;
/// Clearance kept around the graph when framing it.
const GRAPH_FIT_PADDING: f64 = 48.0;
/// How long a snapshot stays fresh before the freshness pill reports a stall.
const GRAPH_FRESHNESS_TIMEOUT: Duration = Duration::from_millis(2_500);
/// How often the freshness pill re-evaluates the age of the last snapshot.
const GRAPH_FRESHNESS_TICK: Duration = Duration::from_millis(500);

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(App);
}

#[component]
fn App() -> impl IntoView {
    let active_domain = RwSignal::new(None::<String>);
    let domains = RwSignal::new(Vec::<DomainView>::new());
    let active_theme = RwSignal::new(0_usize);
    let input = RwSignal::new(String::new());
    let terminal_lines = RwSignal::new(Vec::<TermLine>::new());
    let transaction_status = RwSignal::new(None::<nervix_proto::TransactionStatus>);
    let subscription_tabs = RwSignal::new(Vec::<SubscriptionTabView>::new());
    let active_subscription_tab = RwSignal::new(None::<u64>);
    let next_subscription_tab_id = RwSignal::new(1_u64);
    let suggestions = RwSignal::new(Vec::<String>::new());
    let domain_snapshots = RwSignal::new(Vec::<DomainSnapshotView>::new());
    let cluster_counters = RwSignal::new(ClusterCounters::default());
    let resource_details = RwSignal::new(BTreeMap::<String, ResourceDetailView>::new());
    let domains_loaded = RwSignal::new(false);
    let user_selected_domain = RwSignal::new(false);
    let auth_token = RwSignal::new(web_console_auth_token_from_location());
    let auth_error = RwSignal::new(None::<String>);
    let web_console_session = use_websocket_session(WebConsoleSignals {
        terminal_lines,
        suggestions,
        domain_snapshots,
        cluster_counters,
        active_domain,
        transaction_status,
        domains,
        resource_details,
        subscription_tabs,
        active_subscription_tab,
        domains_loaded,
        user_selected_domain,
        auth_token,
        auth_error,
    });

    let active_domain_name = move || active_domain.get().unwrap_or_default();
    let active_graph = move || {
        let active_id = active_domain_name();
        let snapshots = domain_snapshots.get();
        snapshots
            .iter()
            .find(|snapshot| snapshot.domain == active_id)
            .cloned()
            .filter(|snapshot| !snapshot.dataflow_graph.nodes.is_empty())
            .map(|snapshot| GraphView::from_dataflow_graph(snapshot.dataflow_graph))
    };
    let active_entities = move || {
        let active_id = active_domain_name();
        domain_snapshots
            .get()
            .into_iter()
            .find(|snapshot| snapshot.domain == active_id)
            .map(|snapshot| snapshot.entities)
            .unwrap_or_default()
    };
    let active_domain_session = web_console_session.clone();
    Effect::new(move |_| {
        let Some(domain) = active_domain.get() else {
            return;
        };
        let request = nervix_proto::SessionRequest {
            request: Some(nervix_proto::session_request::Request::SetActiveDomain(
                nervix_proto::SetActiveDomainRequest { domain },
            )),
        };
        let queued = QueuedRequest::SetActiveDomain { request };
        if let Some(request_tx) = active_domain_session.request_tx.get_untracked() {
            let _ = request_tx.unbounded_send(queued);
        }
    });
    let suggestion_session = web_console_session.clone();
    let request_suggestions = move |value: String| {
        if !domains_loaded.get_untracked() {
            suggestions.set(Vec::new());
            return;
        }
        let cursor = value.len() as u32;
        let request = nervix_proto::SessionRequest {
            request: Some(nervix_proto::session_request::Request::Suggest(
                nervix_proto::SuggestRequest {
                    input: value,
                    cursor,
                    domain: active_domain_name(),
                },
            )),
        };
        let queued = QueuedRequest::Suggest { request };
        if let Some(request_tx) = suggestion_session.request_tx.get_untracked()
            && request_tx.unbounded_send(queued).is_err()
        {
            suggestions.set(Vec::new());
        }
    };

    let run_command = move |next_command: Option<String>| {
        let command = next_command
            .unwrap_or_else(|| input.get())
            .trim()
            .to_string();
        if command.is_empty() {
            return;
        }
        terminal_lines.update(|lines| {
            lines.push(TermLine::prompt(
                command.clone(),
                current_transaction_state(transaction_status.get_untracked()),
            ));
        });
        if command.eq_ignore_ascii_case("clear") {
            terminal_lines.set(Vec::new());
            input.set(String::new());
            return;
        }
        if let Ok(ClientStatement::ListDomains) = parse_client_statement(&command) {
            if transaction_is_active(transaction_status.get_untracked()) {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            let request = nervix_proto::SessionRequest {
                request: Some(nervix_proto::session_request::Request::ListDomains(
                    nervix_proto::ListDomainsRequest {},
                )),
            };
            let queued = QueuedRequest::Command {
                query: command.clone(),
                request,
            };
            if let Some(request_tx) = web_console_session.request_tx.get_untracked()
                && request_tx.unbounded_send(queued).is_err()
            {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error("websocket command channel is closed"));
                });
            }
        } else if let Ok(domain) = parse_use_domain(&command) {
            if transaction_is_active(transaction_status.get_untracked()) {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            let domain_name = domain.to_string();
            if domains
                .get_untracked()
                .iter()
                .any(|domain| domain.id == domain_name)
            {
                user_selected_domain.set(true);
                active_domain.set(Some(domain_name.clone()));
                terminal_lines.update(|lines| {
                    lines.push(TermLine::info(format!("using domain '{domain_name}'")));
                });
            } else {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(format!(
                        "domain '{domain_name}' is not present in this console view"
                    )));
                });
            }
        } else {
            if active_domain.get_untracked().is_none() && !is_domainless_server_command(&command) {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error("no active domain selected"));
                });
                suggestions.set(Vec::new());
                input.set(String::new());
                return;
            }
            let request_domain = active_domain.get_untracked().unwrap_or_default();
            let request = nervix_proto::SessionRequest {
                request: Some(nervix_proto::session_request::Request::Command(
                    nervix_proto::CommandRequest {
                        query: command.clone(),
                        domain: request_domain,
                    },
                )),
            };
            let queued = QueuedRequest::Command {
                query: command.clone(),
                request,
            };
            if let Some(request_tx) = web_console_session.request_tx.get_untracked() {
                if request_tx.unbounded_send(queued).is_err() {
                    terminal_lines.update(|lines| {
                        lines.push(TermLine::error("websocket command channel is closed"));
                    });
                } else if web_console_session.state.get_untracked()
                    != ConsoleConnectionState::Connected
                {
                    terminal_lines.update(|lines| {
                        lines.push(TermLine::info("queued until websocket reconnects"));
                    });
                }
            } else {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error("websocket session is not available"));
                });
            }
        }
        suggestions.set(Vec::new());
        input.set(String::new());
    };
    let subscription_session = web_console_session.clone();
    let start_subscription = move |relay: String, filter: String, sample_rate_index: usize| {
        let Some(domain) = active_domain.get_untracked() else {
            active_subscription_tab.set(None);
            terminal_lines.update(|lines| lines.push(TermLine::error("no active domain selected")));
            return;
        };
        let title = subscription_tab_title(&relay, &filter);
        if let Some(existing) = subscription_tabs.get_untracked().into_iter().find(|tab| {
            tab.domain == domain
                && tab.relay == relay
                && tab.filter == filter
                && tab.sample_rate_index == sample_rate_index
        }) {
            if existing.state == SubscriptionTabState::Open {
                active_subscription_tab.set(Some(existing.id));
            }
            return;
        }
        let tab_id = next_subscription_tab_id.get_untracked();
        next_subscription_tab_id.set(tab_id + 1);
        let name = format!("web_console_subscription_{tab_id}");
        let subscribe_command =
            subscribe_session_command(&name, &relay, &filter, sample_rate_index);
        let unsubscribe_command = unsubscribe_session_command(&name);
        subscription_tabs.update(|tabs| {
            tabs.push(SubscriptionTabView {
                id: tab_id,
                state: SubscriptionTabState::Pending,
                name,
                domain: domain.clone(),
                relay,
                filter,
                sample_rate_index,
                title,
                subscribe_command: subscribe_command.clone(),
                unsubscribe_command,
                lines: Vec::new(),
            });
        });
        let request = nervix_proto::SessionRequest {
            request: Some(nervix_proto::session_request::Request::Command(
                nervix_proto::CommandRequest {
                    query: subscribe_command,
                    domain,
                },
            )),
        };
        if let Some(request_tx) = subscription_session.request_tx.get_untracked() {
            if request_tx
                .unbounded_send(QueuedRequest::SubscriptionStart { tab_id, request })
                .is_err()
            {
                append_subscription_tab_line(
                    subscription_tabs,
                    tab_id,
                    TermLine::error("websocket command channel is closed"),
                );
            }
        } else {
            append_subscription_tab_line(
                subscription_tabs,
                tab_id,
                TermLine::error("websocket session is not available"),
            );
        }
    };
    let stop_subscription_session = web_console_session.clone();
    let stop_subscription = move |tab_id: u64| {
        let Some(tab) = subscription_tabs
            .get_untracked()
            .into_iter()
            .find(|tab| tab.id == tab_id)
        else {
            return;
        };
        subscription_tabs.update(|tabs| tabs.retain(|tab| tab.id != tab_id));
        active_subscription_tab.update(|active| {
            if *active == Some(tab_id) {
                *active = None;
            }
        });
        let request = nervix_proto::SessionRequest {
            request: Some(nervix_proto::session_request::Request::Command(
                nervix_proto::CommandRequest {
                    query: tab.unsubscribe_command,
                    domain: tab.domain,
                },
            )),
        };
        if let Some(request_tx) = stop_subscription_session.request_tx.get_untracked() {
            let _ = request_tx.unbounded_send(QueuedRequest::SubscriptionStop { request });
        }
    };

    view! {
        <Show
            when=move || auth_token.get().is_some()
            fallback=move || {
                view! {
                    <AuthPanel auth_token=auth_token auth_error=auth_error />
                }
            }
        >
            <main class=move || format!("console-shell theme-{}", THEMES[active_theme.get()].id)>
                <Header
                    active_theme=active_theme
                    websocket_state=web_console_session.state
                    active_domain=active_domain
                    domains=domains
                    run_command=run_command
                />
                <div class="console-body">
                    <Sidebar active_domain=active_domain user_selected_domain=user_selected_domain domains=domains domains_loaded=domains_loaded active_graph=active_graph active_entities=active_entities cluster_counters=cluster_counters resource_details=resource_details web_console_session=web_console_session.clone() run_command=run_command />
                    <section class="main-pane">
                        <GraphPanel
                            active_domain=active_domain
                            domains=domains
                            websocket_state=web_console_session.state
                            domain=active_graph
                            run_command=run_command
                            start_subscription=start_subscription
                        />
                        <ReplPanel
                            domain=active_domain_name
                            input=input
                            terminal_lines=terminal_lines
                            transaction_state=move || current_transaction_state(transaction_status.get())
                            subscription_tabs=subscription_tabs
                            active_subscription_tab=active_subscription_tab
                            stop_subscription=stop_subscription
                            suggestions=move || suggestions.get()
                            request_suggestions=request_suggestions
                            input_enabled=move || domains_loaded.get()
                            run_command=run_command
                        />
                    </section>
                </div>
            </main>
        </Show>
    }
}

#[component]
fn AuthPanel(
    auth_token: RwSignal<Option<String>>,
    auth_error: RwSignal<Option<String>>,
) -> impl IntoView {
    let username = RwSignal::new("default".to_string());
    let password = RwSignal::new(String::new());
    let submit = move |event: ev::SubmitEvent| {
        event.prevent_default();
        let username_value = username.get_untracked().trim().to_string();
        if username_value.is_empty() {
            auth_error.set(Some("Username is required".to_string()));
            return;
        }
        let password_value = password.get_untracked();
        let token = BASE64_STANDARD.encode(format!("{username_value}:{password_value}"));
        auth_error.set(None);
        auth_token.set(Some(token));
    };

    view! {
        <main class="auth-shell">
            <form class="auth-panel" on:submit=submit>
                <img class="auth-mark" src="/console/nervix-icon.svg" alt="" />
                <h1>"nervix"</h1>
                <label>
                    <span>"User"</span>
                    <input
                        class="auth-username"
                        type="text"
                        autocomplete="username"
                        prop:value=move || username.get()
                        on:input=move |event| username.set(event_target_input(&event).value())
                    />
                </label>
                <label>
                    <span>"Password"</span>
                    <input
                        class="auth-password"
                        type="password"
                        autocomplete="current-password"
                        prop:value=move || password.get()
                        on:input=move |event| password.set(event_target_input(&event).value())
                    />
                </label>
                <Show when=move || auth_error.get().is_some() fallback=|| ()>
                    <p class="auth-error">{move || auth_error.get().unwrap_or_default()}</p>
                </Show>
                <button class="auth-submit" type="submit">"Connect"</button>
            </form>
        </main>
    }
}

fn use_websocket_session(signals: WebConsoleSignals) -> WebConsoleSession {
    let WebConsoleSignals {
        terminal_lines,
        active_domain,
        transaction_status,
        domains_loaded,
        auth_token,
        auth_error,
        ..
    } = signals;
    let state = RwSignal::new(ConsoleConnectionState::Connecting);
    let request_tx = RwSignal::new(None);
    let upload_base_url = RwSignal::new(web_console_http_base_url());
    let (tx, mut rx) = unbounded::<QueuedRequest>();
    request_tx.set(Some(tx));

    spawn_local(async move {
        let mut reconnect_delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
        let mut pending_requests = VecDeque::new();
        let mut redirected_url = None::<String>;
        loop {
            let Some(current_auth_token) = auth_token.get_untracked() else {
                state.set(ConsoleConnectionState::Waiting);
                domains_loaded.set(false);
                pending_requests.clear();
                redirected_url = None;
                wait_for_websocket_reconnect(WEBSOCKET_INITIAL_RECONNECT_DELAY).await;
                continue;
            };
            let Some(url) = redirected_url
                .clone()
                .or_else(|| web_console_websocket_url(&current_auth_token))
            else {
                state.set(ConsoleConnectionState::Waiting);
                wait_for_websocket_reconnect(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(WEBSOCKET_MAX_RECONNECT_DELAY);
                continue;
            };
            state.set(ConsoleConnectionState::Connecting);
            domains_loaded.set(false);
            let mut opened_this_attempt = false;
            match WebSocket::open(&url) {
                Ok(mut socket) => {
                    wait_for_websocket_open(&socket).await;
                    if let WebSocketState::Open = socket.state() {
                        opened_this_attempt = true;
                        reconnect_delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
                        auth_error.set(None);
                        if let Some(domain) = active_domain.get_untracked() {
                            let request = nervix_proto::SessionRequest {
                                request: Some(
                                    nervix_proto::session_request::Request::SetActiveDomain(
                                        nervix_proto::SetActiveDomainRequest { domain },
                                    ),
                                ),
                            };
                            if socket
                                .send(WebSocketMessage::Bytes(request.encode_to_vec()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        let request = nervix_proto::SessionRequest {
                            request: Some(nervix_proto::session_request::Request::ListDomains(
                                nervix_proto::ListDomainsRequest {},
                            )),
                        };
                        if socket
                            .send(WebSocketMessage::Bytes(request.encode_to_vec()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        let mut resend_pending_after_connect = !pending_requests.is_empty();
                        let mut waiting_for_transaction_attach = false;
                        if transaction_is_active(transaction_status.get_untracked()) {
                            let existing_attach = pending_requests.front().and_then(|pending| {
                                let PendingRequest::AttachTransaction { request } = pending else {
                                    return None;
                                };
                                Some(request.clone())
                            });
                            let had_existing_attach = existing_attach.is_some();
                            let request = existing_attach.unwrap_or_else(|| {
                                let id = transaction_status
                                    .get_untracked()
                                    .map(|status| status.id)
                                    .unwrap_or_default();
                                nervix_proto::SessionRequest {
                                    request: Some(
                                        nervix_proto::session_request::Request::AttachTransaction(
                                            nervix_proto::AttachTransactionRequest { id },
                                        ),
                                    ),
                                }
                            });
                            if socket
                                .send(WebSocketMessage::Bytes(request.encode_to_vec()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            if !had_existing_attach {
                                pending_requests
                                    .push_front(PendingRequest::AttachTransaction { request });
                            }
                            waiting_for_transaction_attach = true;
                        }
                        loop {
                            futures_util::select! {
                                queued = rx.next().fuse() => {
                                    let Some(queued) = queued else {
                                        state.set(ConsoleConnectionState::Waiting);
                                        return;
                                    };
                                    match socket
                                        .send(WebSocketMessage::Bytes(queued.request().encode_to_vec()))
                                        .await
                                    {
                                        Ok(()) => {
                                            match queued {
                                                QueuedRequest::Command { query, request } => {
                                                    pending_requests.push_back(PendingRequest::Command(QueuedCommand { query, request }));
                                                }
                                                QueuedRequest::SubscriptionStart { tab_id, request } => {
                                                    pending_requests.push_back(PendingRequest::SubscriptionStart { tab_id, request });
                                                }
                                                QueuedRequest::SubscriptionStop { request } => {
                                                    pending_requests.push_back(PendingRequest::SubscriptionStop { request });
                                                }
                                                QueuedRequest::ResourceDescribe { resource, request } => {
                                                    pending_requests.push_back(PendingRequest::ResourceDescribe { resource, request });
                                                }
                                                QueuedRequest::SetActiveDomain { .. } | QueuedRequest::Suggest { .. } => {}
                                            }
                                        }
                                        Err(error) => {
                                            leptos::logging::error!(
                                                "failed to send web console websocket command: {error:?}"
                                            );
                                            match queued {
                                                QueuedRequest::Command { query, request } => {
                                                    pending_requests.push_front(PendingRequest::Command(QueuedCommand { query, request }));
                                                }
                                                QueuedRequest::SubscriptionStart { tab_id, request } => {
                                                    pending_requests.push_front(PendingRequest::SubscriptionStart { tab_id, request });
                                                }
                                                QueuedRequest::SubscriptionStop { request } => {
                                                    pending_requests.push_front(PendingRequest::SubscriptionStop { request });
                                                }
                                                QueuedRequest::ResourceDescribe { resource, request } => {
                                                    pending_requests.push_front(PendingRequest::ResourceDescribe { resource, request });
                                                }
                                                QueuedRequest::SetActiveDomain { .. } | QueuedRequest::Suggest { .. } => {}
                                            }
                                            break;
                                        }
                                    }
                                }
                                message = socket.next().fuse() => {
                                    let Some(message) = message else {
                                        break;
                                    };
                                    match message {
                                        Ok(WebSocketMessage::Bytes(payload)) => {
                                            match nervix_proto::SessionResponse::decode(
                                                prost::bytes::Bytes::from(payload),
                                            ) {
                                                Ok(response) => {
                                                    match handle_session_response(
                                                        signals,
                                                        response,
                                                        &mut pending_requests,
                                                    ) {
                                                        SessionResponseAction::Continue => {
                                                            state.set(ConsoleConnectionState::Connected);
                                                            if resend_pending_after_connect
                                                                && !waiting_for_transaction_attach
                                                            {
                                                                resend_pending_after_connect = false;
                                                                if !send_pending_websocket_commands(
                                                                    &mut socket,
                                                                    &mut pending_requests,
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        SessionResponseAction::ReattachTransaction { id } => {
                                                            let request = nervix_proto::SessionRequest {
                                                                request: Some(
                                                                    nervix_proto::session_request::Request::AttachTransaction(
                                                                        nervix_proto::AttachTransactionRequest { id },
                                                                    ),
                                                                ),
                                                            };
                                                            if socket
                                                                .send(WebSocketMessage::Bytes(request.encode_to_vec()))
                                                                .await
                                                                .is_err()
                                                            {
                                                                break;
                                                            }
                                                            pending_requests.push_front(
                                                                PendingRequest::AttachTransaction { request },
                                                            );
                                                            waiting_for_transaction_attach = true;
                                                            resend_pending_after_connect = true;
                                                        }
                                                        SessionResponseAction::TransactionAttached {
                                                            replay_pending,
                                                        } => {
                                                            state.set(ConsoleConnectionState::Connected);
                                                            waiting_for_transaction_attach = false;
                                                            if resend_pending_after_connect
                                                                && replay_pending
                                                            {
                                                                resend_pending_after_connect = false;
                                                                if !send_pending_websocket_commands(
                                                                    &mut socket,
                                                                    &mut pending_requests,
                                                                )
                                                                .await
                                                                {
                                                                    break;
                                                                }
                                                            } else {
                                                                resend_pending_after_connect = false;
                                                            }
                                                        }
                                                        SessionResponseAction::TransactionAttachFailed => {
                                                            state.set(ConsoleConnectionState::Connected);
                                                            waiting_for_transaction_attach = false;
                                                            resend_pending_after_connect = false;
                                                        }
                                                        SessionResponseAction::Reconnect(next_url) => {
                                                            upload_base_url.set(Some(next_url.clone()));
                                                            redirected_url = web_console_websocket_url_from_base(
                                                                &next_url,
                                                                &current_auth_token,
                                                            );
                                                            break;
                                                        }
                                                    }
                                                }
                                                Err(error) => {
                                                    terminal_lines.update(|lines| {
                                                        lines.push(TermLine::error(format!(
                                                            "failed to decode protobuf response: {error}"
                                                        )));
                                                    });
                                                }
                                            }
                                        }
                                        Ok(WebSocketMessage::Text(text)) => {
                                            terminal_lines.update(|lines| {
                                                lines.push(TermLine::output(text));
                                            });
                                        }
                                        Err(error) => {
                                            leptos::logging::error!(
                                                "web console websocket failed: {error:?}"
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    leptos::logging::error!("failed to open web console websocket: {error:?}");
                }
            }
            if !opened_this_attempt
                && auth_token.get_untracked().as_deref() == Some(current_auth_token.as_str())
            {
                auth_error.set(Some("Authentication failed".to_string()));
                auth_token.set(None);
                pending_requests.clear();
                redirected_url = None;
                continue;
            }
            state.set(ConsoleConnectionState::Waiting);
            wait_for_websocket_reconnect(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(WEBSOCKET_MAX_RECONNECT_DELAY);
        }
    });

    WebConsoleSession {
        state,
        request_tx,
        upload_base_url,
        auth_token,
    }
}

async fn wait_for_websocket_open(socket: &WebSocket) {
    while matches!(socket.state(), WebSocketState::Connecting) {
        wait_for_websocket_reconnect(Duration::from_millis(50)).await;
    }
}

async fn wait_for_websocket_reconnect(delay: Duration) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                &resolve,
                delay.as_millis().min(i32::MAX as u128) as i32,
            );
        } else {
            let _ = resolve.call0(&wasm_bindgen::JsValue::UNDEFINED);
        }
    });
    let _ = wasm_bindgen_futures::JsFuture::from(promise).await;
}

async fn send_pending_websocket_commands(
    socket: &mut WebSocket,
    pending_requests: &mut VecDeque<PendingRequest>,
) -> bool {
    let requests = pending_requests.drain(..).collect::<Vec<_>>();
    for request in requests {
        match socket
            .send(WebSocketMessage::Bytes(request.request().encode_to_vec()))
            .await
        {
            Ok(()) => pending_requests.push_back(request),
            Err(error) => {
                leptos::logging::error!("failed to resend web console command: {error:?}");
                pending_requests.push_front(request);
                return false;
            }
        }
    }
    true
}

fn web_console_auth_token_from_location() -> Option<String> {
    let href = web_sys::window()?.location().href().ok()?;
    let url = Url::parse(&href).ok()?;
    url.query_pairs()
        .find_map(|(key, value)| (key == "auth").then(|| value.into_owned()))
}

fn web_console_websocket_url(auth_token: &str) -> Option<String> {
    let location = web_sys::window()?.location();
    let protocol = match location.protocol().ok()?.as_str() {
        "https:" => "wss:",
        _ => "ws:",
    };
    let host = location.host().ok()?;
    Some(format!(
        "{protocol}//{host}/console/ws?auth={}",
        encode_query_component(auth_token)
    ))
}

fn web_console_http_base_url() -> Option<String> {
    let location = web_sys::window()?.location();
    let protocol = location.protocol().ok()?;
    let host = location.host().ok()?;
    Some(format!("{protocol}//{host}"))
}

fn web_console_websocket_url_from_base(base_url: &str, auth_token: &str) -> Option<String> {
    let mut url = Url::parse(base_url).ok()?;
    let websocket_scheme = match url.scheme() {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        _ => return None,
    };
    url.set_scheme(websocket_scheme).ok()?;
    url.set_path("/console/ws");
    url.set_query(Some(&format!(
        "auth={}",
        encode_query_component(auth_token)
    )));
    url.set_fragment(None);
    Some(url.to_string())
}

enum SessionResponseAction {
    Continue,
    /// The leader has no binding for this session's transaction. Attach it again, then replay the
    /// command that was rejected.
    ReattachTransaction {
        id: String,
    },
    TransactionAttached {
        replay_pending: bool,
    },
    TransactionAttachFailed,
    Reconnect(String),
}

fn current_transaction_state(
    status: Option<nervix_proto::TransactionStatus>,
) -> Option<nervix_proto::TransactionState> {
    status.and_then(|status| nervix_proto::TransactionState::try_from(status.state).ok())
}

fn transaction_is_active(status: Option<nervix_proto::TransactionStatus>) -> bool {
    matches!(
        current_transaction_state(status),
        Some(nervix_proto::TransactionState::Open | nervix_proto::TransactionState::Committing)
    )
}

fn transaction_operation_was_observed(
    previous: Option<&nervix_proto::TransactionStatus>,
    current: Option<&nervix_proto::TransactionStatus>,
) -> bool {
    let (Some(previous), Some(current)) = (previous, current) else {
        return false;
    };
    current_transaction_state(Some(current.clone())) != Some(nervix_proto::TransactionState::Open)
        || current.pending_count != previous.pending_count
        || current.completed_count != previous.completed_count
        || current.total_count != previous.total_count
}

fn active_domain_graph_missing(
    active_domain: Option<String>,
    domain_snapshots: &RwSignal<Vec<DomainSnapshotView>>,
) -> bool {
    let Some(active_domain) = active_domain else {
        return true;
    };
    !domain_snapshots
        .get_untracked()
        .iter()
        .any(|snapshot| snapshot.domain == active_domain)
}

fn handle_session_response(
    signals: WebConsoleSignals,
    response: nervix_proto::SessionResponse,
    pending_requests: &mut VecDeque<PendingRequest>,
) -> SessionResponseAction {
    let WebConsoleSignals {
        terminal_lines,
        suggestions,
        domain_snapshots,
        cluster_counters,
        active_domain,
        transaction_status,
        domains,
        resource_details,
        subscription_tabs,
        active_subscription_tab,
        domains_loaded,
        user_selected_domain,
        ..
    } = signals;
    match response.event {
        Some(nervix_proto::session_response::Event::Result(result)) => {
            if let Some(leader_url) = leader_web_console_redirect_url(&result) {
                return SessionResponseAction::Reconnect(leader_url);
            }
            let previous_transaction = transaction_status.get_untracked();
            if let Some(status) = result.transaction.clone() {
                if !status.domain.is_empty()
                    && active_domain.get_untracked().as_deref() != Some(status.domain.as_str())
                {
                    user_selected_domain.set(true);
                    active_domain.set(Some(status.domain.clone()));
                }
                transaction_status.set(Some(status));
            }
            if result_is_set_active_domain_ack(&result) {
                terminal_lines.update(|lines| lines.extend(command_result_lines(result, "")));
                return SessionResponseAction::Continue;
            }
            let mut pending = pending_requests.pop_front();
            if command_result_is_transaction_detached(&result)
                && pending.is_some()
                && let Some(id) = transaction_status
                    .get_untracked()
                    .map(|status| status.id)
                    .filter(|id| !id.is_empty())
            {
                pending_requests.push_front(pending.take().expect("pending was checked above"));
                return SessionResponseAction::ReattachTransaction { id };
            }
            let pending = pending;
            if let Some(PendingRequest::AttachTransaction { .. }) = pending {
                if !result.success {
                    if result.transaction.is_none() {
                        transaction_status.set(None);
                    }
                    pending_requests.clear();
                    let message = result.message.clone();
                    let has_recorded_results = !result.results.is_empty();
                    terminal_lines.update(|lines| {
                        if has_recorded_results {
                            lines.push(TermLine::error(message));
                        }
                        lines.extend(command_result_lines(result, "ATTACH TRANSACTION"));
                    });
                    return SessionResponseAction::TransactionAttachFailed;
                }
                let operation_was_observed = transaction_operation_was_observed(
                    previous_transaction.as_ref(),
                    result.transaction.as_ref(),
                );
                if operation_was_observed {
                    pending_requests.clear();
                    terminal_lines.update(|lines| {
                        lines.extend(command_result_lines(result, "ATTACH TRANSACTION"));
                    });
                }
                return SessionResponseAction::TransactionAttached {
                    replay_pending: !operation_was_observed,
                };
            }
            if let Some(PendingRequest::ResourceDescribe { resource, .. }) = pending {
                resource_details.update(|details| {
                    details.insert(resource, resource_detail_from_result(result));
                });
                return SessionResponseAction::Continue;
            }
            if let Some(PendingRequest::SubscriptionStart { tab_id, .. }) = pending {
                let lines = command_result_lines(result, "");
                if lines.iter().any(|line| line.kind == TermLineKind::Error) {
                    append_subscription_tab_lines(subscription_tabs, tab_id, lines);
                }
                subscription_tabs.update(|tabs| {
                    if let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) {
                        tab.state = SubscriptionTabState::Open;
                    }
                });
                active_subscription_tab.set(Some(tab_id));
                return SessionResponseAction::Continue;
            }
            if let Some(PendingRequest::SubscriptionStop { .. }) = pending {
                return SessionResponseAction::Continue;
            }
            let query = match pending {
                Some(PendingRequest::Command(command)) => command.query,
                _ => String::new(),
            };
            if result.success
                && let Some(domain) = first_created_domain_from_query(&query)
            {
                user_selected_domain.set(true);
                active_domain.set(Some(domain));
            }
            terminal_lines.update(|lines| {
                lines.extend(command_result_lines(result, &query));
            });
        }
        Some(nervix_proto::session_response::Event::Subscription(event)) => {
            append_subscription_event(subscription_tabs, event);
        }
        Some(nervix_proto::session_response::Event::Server(event)) => {
            terminal_lines.update(|lines| lines.push(server_event_line(event)));
        }
        Some(nervix_proto::session_response::Event::Suggest(response)) => {
            suggestions.set(
                response
                    .suggestions
                    .into_iter()
                    .map(|suggestion| suggestion.value)
                    .collect(),
            );
        }
        Some(nervix_proto::session_response::Event::Domains(response)) => {
            let next_domains = response
                .domains
                .into_iter()
                .map(DomainView::from)
                .collect::<Vec<_>>();
            domains_loaded.set(true);
            domains.set(next_domains.clone());
            let current = active_domain.get_untracked();
            if current
                .as_ref()
                .is_none_or(|id| !next_domains.iter().any(|domain| domain.id == *id))
            {
                active_domain.set(next_domains.first().map(|domain| domain.id.clone()));
            }
            if response.response_to_request
                && let Some(PendingRequest::Command(_)) = pending_requests.pop_front()
            {
                terminal_lines.update(|lines| {
                    lines.extend(domain_list_lines(&next_domains));
                });
            }
        }
        Some(nervix_proto::session_response::Event::Snapshot(snapshot)) => {
            match DataflowGraph::deserialize(&snapshot.dataflow_graph) {
                Ok(graph) => {
                    let graph_domain = snapshot.domain.clone();
                    let should_select_graph_domain = !user_selected_domain.get_untracked()
                        && active_domain_graph_missing(
                            active_domain.get_untracked(),
                            &domain_snapshots,
                        );
                    domain_snapshots.update(|snapshots| {
                        snapshots.retain(|existing| existing.domain != snapshot.domain);
                        snapshots.push(DomainSnapshotView::from_snapshot(snapshot, graph));
                    });
                    if should_select_graph_domain {
                        active_domain.set(Some(graph_domain));
                    }
                }
                Err(error) => {
                    terminal_lines.update(|lines| {
                        lines.push(TermLine::error(format!(
                            "failed to decode graph snapshot for domain '{}': {error}",
                            snapshot.domain
                        )));
                    });
                }
            }
        }
        Some(nervix_proto::session_response::Event::Cluster(summary)) => {
            cluster_counters.set(ClusterCounters::from(summary));
        }
        None => {}
    }
    SessionResponseAction::Continue
}

fn result_is_set_active_domain_ack(result: &nervix_proto::CommandResult) -> bool {
    result.success && result.message.starts_with("using domain '")
}

fn command_result_is_transaction_detached(result: &nervix_proto::CommandResult) -> bool {
    nervix_proto::CommandResultKind::try_from(result.kind).ok()
        == Some(nervix_proto::CommandResultKind::TransactionDetached)
}

fn leader_web_console_redirect_url(result: &nervix_proto::CommandResult) -> Option<String> {
    if nervix_proto::CommandResultKind::try_from(result.kind).ok()
        != Some(nervix_proto::CommandResultKind::NotLeader)
    {
        return None;
    }
    (!result.leader_web_console_uri.is_empty()).then(|| result.leader_web_console_uri.clone())
}

fn command_result_lines(result: nervix_proto::CommandResult, query: &str) -> Vec<TermLine> {
    if !result.results.is_empty() {
        return result
            .results
            .into_iter()
            .flat_map(|result| command_result_lines(result, query))
            .collect();
    }

    let mut lines = Vec::new();
    if result.success {
        if !result.message.is_empty() {
            lines.push(TermLine::output(result.message));
        }
        return lines;
    }

    match nervix_proto::CommandResultKind::try_from(result.kind).ok() {
        Some(nervix_proto::CommandResultKind::NotLeader) => {
            if !result.leader.is_empty() && !result.leader_grpc_uri.is_empty() {
                lines.push(TermLine::info(format!(
                    "topology: not-a-leader, retry on leader '{}' at {}",
                    result.leader, result.leader_grpc_uri
                )));
            } else if !result.leader.is_empty() {
                lines.push(TermLine::info(format!(
                    "topology: not-a-leader, retry on leader '{}'",
                    result.leader
                )));
            } else {
                lines.push(TermLine::info("topology: not-a-leader"));
            }
        }
        _ => lines.push(TermLine::error(result.message)),
    }

    if result.diagnostics.is_empty() {
        lines.push(TermLine::output("- no diagnostics provided"));
    } else {
        lines.extend(
            result
                .diagnostics
                .into_iter()
                .map(|diagnostic| diagnostic_line(query, diagnostic)),
        );
    }
    lines
}

fn append_subscription_tab_line(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    tab_id: u64,
    line: TermLine,
) {
    append_subscription_tab_lines(subscription_tabs, tab_id, vec![line]);
}

fn append_subscription_tab_lines(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    tab_id: u64,
    lines: Vec<TermLine>,
) {
    subscription_tabs.update(|tabs| {
        if let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) {
            tab.lines.extend(lines);
        }
    });
}

fn append_subscription_event(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    event: nervix_proto::SubscriptionEvent,
) {
    let line = TermLine::output(event.payload);
    let relay = event.relay;
    let subscription = event.subscription;
    subscription_tabs.update(|tabs| {
        let matching_tabs = tabs
            .iter()
            .enumerate()
            .filter_map(|(index, tab)| {
                (tab.relay == relay && tab.name == subscription).then_some(index)
            })
            .collect::<Vec<_>>();
        for index in matching_tabs {
            let Some(tab) = tabs.get_mut(index) else {
                continue;
            };
            tab.lines.push(line.clone());
        }
    });
}

fn subscribe_session_command(
    name: &str,
    relay: &str,
    filter: &str,
    sample_rate_index: usize,
) -> String {
    let mut command = format!("CREATE SUBSCRIPTION {name} TO {relay}");
    if let Some(sample_rate) = subscription_sample_rate(sample_rate_index) {
        command.push_str(" BATCH SAMPLE RATE ");
        command.push_str(sample_rate);
    }
    let filter = filter.trim();
    if !filter.is_empty() {
        command.push(' ');
        command.push_str(&subscription_where_clause(filter));
    }
    command.push(';');
    command
}

fn unsubscribe_session_command(name: &str) -> String {
    format!("DELETE SUBSCRIPTION {name};")
}

fn subscription_tab_title(relay: &str, filter: &str) -> String {
    let filter = filter.trim();
    if filter.is_empty() {
        relay.to_string()
    } else {
        format!("{relay} {filter}")
    }
}

fn subscription_where_clause(filter: &str) -> String {
    let trimmed = filter.trim();
    let Some(first_word) = trimmed.split_ascii_whitespace().next() else {
        return String::new();
    };
    if first_word.eq_ignore_ascii_case("WHERE") {
        trimmed.to_string()
    } else {
        format!("WHERE {trimmed}")
    }
}

fn subscription_sample_rate(index: usize) -> Option<&'static str> {
    match index {
        0 => None,
        1 => Some("0.1"),
        2 => Some("0.01"),
        3 => Some("0.001"),
        _ => None,
    }
}

fn domain_list_lines(domains: &[DomainView]) -> Vec<TermLine> {
    if domains.is_empty() {
        return vec![TermLine::output("no domains registered")];
    }
    std::iter::once(TermLine::output("domains:"))
        .chain(domains.iter().map(|domain| {
            TermLine::output(format!(
                "{} pace={} status={}",
                domain.id, domain.mode, domain.status
            ))
        }))
        .collect()
}

fn resource_detail_from_result(result: nervix_proto::CommandResult) -> ResourceDetailView {
    if !result.success {
        return ResourceDetailView {
            versions: Vec::new(),
            status: result.message,
        };
    }
    let versions = parse_resource_versions_from_describe(&result.message);
    let versions = if versions.is_empty() {
        result
            .message
            .lines()
            .find_map(|line| line.strip_prefix("versions: "))
            .map(|versions| {
                if versions == "(none)" {
                    Vec::new()
                } else {
                    versions
                        .split(',')
                        .map(str::trim)
                        .filter(|version| !version.is_empty())
                        .map(|version| ResourceVersionView {
                            version: version.to_string(),
                            ..Default::default()
                        })
                        .collect()
                }
            })
            .unwrap_or_default()
    } else {
        versions
    };
    ResourceDetailView {
        versions,
        status: "ready".to_string(),
    }
}

fn parse_resource_versions_from_describe(message: &str) -> Vec<ResourceVersionView> {
    let mut versions = Vec::new();
    let mut current = None::<ResourceVersionView>;
    for line in message.lines() {
        if let Some(version) = parse_resource_version_detail(line) {
            if let Some(current) = current.replace(version) {
                versions.push(current);
            }
        } else if let Some(file) = parse_resource_file_detail(line)
            && let Some(version) = &mut current
        {
            version.files.push(file);
        }
    }
    if let Some(current) = current {
        versions.push(current);
    }
    versions
}

fn parse_resource_version_detail(line: &str) -> Option<ResourceVersionView> {
    let line = line.strip_prefix("- ")?;
    let mut version = None;
    let mut root_checksum = None;
    let mut manifest_checksum = None;
    let mut file_count = None;
    let mut total_bytes = None;
    let mut created_by_node = None;
    let mut created_at = None;
    for part in line.split_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "version" => version = Some(value.to_string()),
            "root_checksum" => root_checksum = Some(value.to_string()),
            "manifest_checksum" => manifest_checksum = Some(value.to_string()),
            "file_count" => file_count = Some(value.to_string()),
            "total_bytes" => total_bytes = Some(value.to_string()),
            "created_by_node" => created_by_node = Some(value.to_string()),
            "created_at" => created_at = Some(value.to_string()),
            _ => {}
        }
    }
    version.map(|version| ResourceVersionView {
        version,
        root_checksum,
        manifest_checksum,
        file_count,
        total_bytes,
        created_by_node,
        created_at,
        files: Vec::new(),
    })
}

fn parse_resource_file_detail(line: &str) -> Option<ResourceFileView> {
    let line = line.strip_prefix("  - ")?;
    if line.starts_with("none") || line.starts_with("unavailable") {
        return None;
    }
    let mut path = None;
    let mut entry_type = None;
    let mut size = None;
    let mut checksum = None;
    for part in line.split_whitespace() {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "type" => entry_type = Some(value.to_string()),
            "path" => path = Some(value.to_string()),
            "size" => size = Some(value.to_string()),
            "checksum" => checksum = Some(value.to_string()),
            _ => {}
        }
    }
    Some(ResourceFileView {
        path: path?,
        entry_type: entry_type.unwrap_or_else(|| "file".to_string()),
        size,
        checksum,
    })
}

fn resource_version_summary(version: &ResourceVersionView) -> String {
    let mut parts = Vec::new();
    if let Some(file_count) = &version.file_count {
        parts.push(format!("{file_count} files"));
    }
    if let Some(total_bytes) = &version.total_bytes {
        parts.push(format!("{total_bytes} bytes"));
    }
    if let Some(created_by_node) = &version.created_by_node {
        parts.push(format!("from {created_by_node}"));
    }
    if let Some(created_at) = &version.created_at {
        parts.push(created_at.clone());
    }
    parts.join(" | ")
}

fn resource_version_checksums(version: &ResourceVersionView) -> String {
    let mut parts = Vec::new();
    if let Some(root_checksum) = &version.root_checksum {
        parts.push(format!("root {root_checksum}"));
    }
    if let Some(manifest_checksum) = &version.manifest_checksum {
        parts.push(format!("manifest {manifest_checksum}"));
    }
    parts.join(" | ")
}

fn resource_file_summary(file: &ResourceFileView) -> String {
    let mut parts = Vec::new();
    parts.push(file.entry_type.clone());
    if let Some(size) = &file.size
        && file.entry_type != "directory"
    {
        parts.push(format!("{size} bytes"));
    }
    if let Some(checksum) = &file.checksum
        && checksum != "-"
    {
        parts.push(format!("checksum {checksum}"));
    }
    parts.join(" | ")
}

fn first_created_domain_from_query(query: &str) -> Option<String> {
    parse_client_statements(query)
        .ok()?
        .into_iter()
        .find_map(|statement| match statement {
            ClientStatement::Server(Statement::CreateDomain(create)) => {
                Some(create.id.as_str().to_string())
            }
            _ => None,
        })
}

fn is_domainless_server_command(command: &str) -> bool {
    let normalized = command.trim_start().to_ascii_uppercase();
    normalized.starts_with("COMMIT")
        || normalized.starts_with("REVERT")
        || normalized.starts_with("CREATE DOMAIN ")
        || normalized.starts_with("CREATE UNPACED DOMAIN ")
        || normalized.starts_with("CREATE PACED DOMAIN ")
        || normalized.starts_with("CREATE USER ")
        || normalized.starts_with("CREATE IF NOT EXISTS USER ")
}

fn diagnostic_line(query: &str, diagnostic: nervix_proto::Diagnostic) -> TermLine {
    let span_start = diagnostic.span_start as usize;
    let span_end = diagnostic.span_end as usize;
    if span_start < span_end && span_end <= query.len() {
        TermLine::output(format!(
            "- {} at {}..{}: {}",
            &query[span_start..span_end],
            diagnostic.span_start,
            diagnostic.span_end,
            diagnostic.message
        ))
    } else {
        TermLine::output(format!("- {}", diagnostic.message))
    }
}

fn server_event_line(event: nervix_proto::ServerEvent) -> TermLine {
    match nervix_proto::ServerEventLevel::try_from(event.level).ok() {
        Some(nervix_proto::ServerEventLevel::Error) => TermLine::error(event.message),
        Some(nervix_proto::ServerEventLevel::Warn) => {
            TermLine::info(format!("warn: {}", event.message))
        }
        _ => TermLine::info(event.message),
    }
}

#[component]
fn Header(
    active_theme: RwSignal<usize>,
    websocket_state: RwSignal<ConsoleConnectionState>,
    active_domain: RwSignal<Option<String>>,
    domains: RwSignal<Vec<DomainView>>,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let theme_open = RwSignal::new(false);
    let selected_domain = move || {
        let active = active_domain.get();
        domains
            .get()
            .into_iter()
            .find(|domain| Some(domain.id.clone()) == active)
    };
    view! {
        <header class="topbar">
            <a class="brand" href="/console" aria-label="Nervix console">
                <img class="brand-mark" src="/console/nervix-icon.svg" alt="" />
                <span class="brand-logotype">"nervix"</span>
            </a>
            <span class="crumb-separator">"/"</span>
            <span class="crumb">"console"</span>
            <div class="topbar-status">
                <span class=move || websocket_state.get().pill_class()>
                    {move || websocket_state.get().label()}
                </span>
                <Show
                    when=move || selected_domain()
                        .is_some_and(|domain| domain_can_toggle_state(&domain.status))
                    fallback=|| ()
                >
                    <button
                        class="domain-state-button topbar-domain-state-button"
                        class:domain-state-start=move || selected_domain()
                            .is_some_and(|domain| domain.status.eq_ignore_ascii_case("STOPPED"))
                        class:domain-state-stop=move || selected_domain()
                            .is_some_and(|domain| domain.status.eq_ignore_ascii_case("RUNNING"))
                        type="button"
                        disabled=move || websocket_state.get() != ConsoleConnectionState::Connected
                        title=move || selected_domain()
                            .map(|domain| {
                                domain_state_hint(
                                    &domain.status,
                                    websocket_state.get() == ConsoleConnectionState::Connected,
                                )
                                .to_string()
                            })
                            .unwrap_or_else(|| "Domain lifecycle".to_string())
                        aria-label=move || selected_domain()
                            .map(|domain| {
                                domain_state_hint(
                                    &domain.status,
                                    websocket_state.get() == ConsoleConnectionState::Connected,
                                )
                                .to_string()
                            })
                            .unwrap_or_else(|| "Domain lifecycle".to_string())
                        on:click=move |_| {
                            if websocket_state.get_untracked() != ConsoleConnectionState::Connected {
                                return;
                            }
                            if let Some(domain) = selected_domain()
                                && let Some(command) = domain_state_command(&domain.status)
                            {
                                run_command(Some(command.to_string()));
                            }
                        }
                    >
                        <Show
                            when=move || selected_domain()
                                .is_some_and(|domain| domain.status.eq_ignore_ascii_case("RUNNING"))
                            fallback=|| view! { <SidebarIcon kind="play" /> }
                        >
                            <SidebarIcon kind="stop" />
                        </Show>
                        <span class="domain-state-hint" aria-hidden="true">
                            {move || selected_domain()
                                .map(|domain| {
                                    domain_state_hint(
                                        &domain.status,
                                        websocket_state.get() == ConsoleConnectionState::Connected,
                                    )
                                    .to_string()
                                })
                                .unwrap_or_else(|| "Domain lifecycle".to_string())
                            }
                        </span>
                    </button>
                </Show>
                <span>{RUNTIME_VERSION_LABEL}</span>
                <div class="menu-wrap">
                    <button
                        class="theme-button"
                        type="button"
                        title="Theme"
                        aria-expanded=move || theme_open.get().to_string()
                        on:click=move |_| theme_open.update(|open| *open = !*open)
                    >
                        <SidebarIcon kind="palette" />
                        <span>{move || THEMES[active_theme.get()].label}</span>
                    </button>
                    <div class="popup-menu theme-menu" class:open=move || theme_open.get()>
                        <For
                            each={|| THEMES.iter().enumerate().collect::<Vec<_>>()}
                            key=|(_, theme)| theme.id
                            children={move |(index, theme)| {
                                view! {
                                    <button
                                        type="button"
                                        class=move || {
                                            if active_theme.get() == index {
                                                "popup-item theme-option active"
                                            } else {
                                                "popup-item theme-option"
                                            }
                                        }
                                        on:click=move |_| {
                                            active_theme.set(index);
                                            theme_open.set(false);
                                        }
                                    >
                                        <span class="swatches">
                                            <i style=format!("background: {}", theme.swatches[0])></i>
                                            <i style=format!("background: {}", theme.swatches[1])></i>
                                            <i style=format!("background: {}", theme.swatches[2])></i>
                                        </span>
                                        <span>{theme.label}</span>
                                        <Show when=move || active_theme.get() == index fallback=|| ()>
                                            <strong class="theme-check">"✓"</strong>
                                        </Show>
                                    </button>
                                }
                            }}
                        />
                    </div>
                </div>
            </div>
        </header>
    }
}

#[component]
fn Sidebar(
    active_domain: RwSignal<Option<String>>,
    user_selected_domain: RwSignal<bool>,
    domains: RwSignal<Vec<DomainView>>,
    domains_loaded: RwSignal<bool>,
    active_graph: impl Fn() -> Option<GraphView> + Copy + Send + Sync + 'static,
    active_entities: impl Fn() -> Vec<EntityView> + Copy + Send + Sync + 'static,
    cluster_counters: RwSignal<ClusterCounters>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    web_console_session: WebConsoleSession,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let domain_open = RwSignal::new(false);
    let schemas_open = RwSignal::new(true);
    let wire_open = RwSignal::new(true);
    let codecs_open = RwSignal::new(true);
    let resources_open = RwSignal::new(true);
    let clients_open = RwSignal::new(true);
    let vhosts_open = RwSignal::new(true);
    let endpoints_open = RwSignal::new(true);
    let selected_resource = RwSignal::new(None::<String>);
    let upload_status = RwSignal::new(String::new());
    let entities_for = move |kind: &'static str| {
        active_entities()
            .into_iter()
            .filter(move |entity| entity.kind == kind)
            .collect::<Vec<_>>()
    };
    let wire_schema_entities = move || {
        active_entities()
            .into_iter()
            .filter(|entity| {
                matches!(
                    entity.kind.as_str(),
                    "wire_json_schema" | "wire_cbor_schema" | "wire_avro_schema"
                )
            })
            .collect::<Vec<_>>()
    };
    let selected_domain = move || {
        let active = active_domain.get();
        let found = domains
            .get()
            .into_iter()
            .find(|domain| Some(domain.id.clone()) == active);
        found.or_else(|| {
            active.map(|id| DomainView {
                id,
                mode: "UNKNOWN".to_string(),
                status: "UNKNOWN".to_string(),
            })
        })
    };
    view! {
        <aside class="sidebar">
            <div class="domain-menu-wrap">
                <button
                    class="domain-select"
                    type="button"
                    aria-expanded=move || domain_open.get().to_string()
                    on:click=move |_| domain_open.update(|open| *open = !*open)
                >
                    <span class="status-dot"></span>
                    <span>{move || {
                        selected_domain()
                            .map(|domain| domain.id)
                            .unwrap_or_else(|| {
                                if domains_loaded.get() {
                                    "no domain".to_string()
                                } else {
                                    "loading domains".to_string()
                                }
                            })
                    }}</span>
                    <span class="domain-mode">{move || {
                        selected_domain()
                            .map(|domain| domain.mode)
                            .unwrap_or_else(|| {
                                if domains_loaded.get() {
                                    "NONE".to_string()
                                } else {
                                    "WAIT".to_string()
                                }
                            })
                    }}</span>
                    <span class="chevron">{move || if domain_open.get() { "⌃" } else { "⌄" }}</span>
                </button>
                <div class="popup-menu domain-menu" class:open=move || domain_open.get()>
                    <For
                        each=move || domains.get()
                        key=|domain| domain.id.clone()
                        children={move |domain| {
                            let domain_id = domain.id.clone();
                            let active_domain_id = domain.id.clone();
                            let domain_label = domain.id.clone();
                            let domain_mode = domain.mode.clone();
                            let command_domain = domain.id.clone();
                            view! {
                                <button
                                    type="button"
                                    data-domain=domain_id.clone()
                                    class=move || {
                                        if active_domain.get().as_ref() == Some(&active_domain_id) {
                                            "popup-item active"
                                        } else {
                                            "popup-item"
                                        }
                                    }
                                    on:click=move |_| {
                                        user_selected_domain.set(true);
                                        active_domain.set(Some(command_domain.clone()));
                                        domain_open.set(false);
                                        run_command(Some(format!("USE {};", command_domain)));
                                    }
                                >
                                    <span class="status-dot"></span>
                                    <span>{domain_label}</span>
                                    <em>{domain_mode}</em>
                                </button>
                            }
                        }}
                    />
                </div>
            </div>
            <div class="summary-block">
                <div class="summary-row">
                    <span>
                        <SidebarIcon kind="box" />
                        "graph from leader"
                    </span>
                    <span>
                        <SidebarIcon kind="branch" />
                        "live snapshot"
                    </span>
                    <strong>{move || selected_domain().map(|domain| domain.status).unwrap_or_else(|| "WAITING".to_string())}</strong>
                </div>
                <div class="summary-metrics">
                    <MetricMini value=move || active_graph().map(|graph| graph.statistics.messages_rate()).unwrap_or_else(|| "0".to_string()) label="msgs/s" />
                    <MetricMini value=move || active_graph().map(|graph| graph.statistics.bytes_rate()).unwrap_or_else(|| "0B".to_string()) label="bytes/s" />
                    <MetricMini value=move || active_graph().map(|graph| graph.statistics.batches_rate()).unwrap_or_else(|| "0".to_string()) label="batches" />
                </div>
            </div>
            <nav class="nav-list" aria-label="Console entities">
                <NavHeader title="Schemas" count=move || entities_for("schema").len().to_string() kind="schemas" open=schemas_open />
                <Show when=move || schemas_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("schema")
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="schemas" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Wire Schemas" count=move || wire_schema_entities().len().to_string() kind="wire" open=wire_open />
                <Show when=move || wire_open.get() fallback=|| ()>
                    <For
                        each=wire_schema_entities
                        key=|entity| format!("{}:{}", entity.kind, entity.name)
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="wire" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Codecs" count=move || entities_for("codec").len().to_string() kind="codecs" open=codecs_open />
                <Show when=move || codecs_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("codec")
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="codecs" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Resources" count=move || entities_for("resource").len().to_string() kind="resources" open=resources_open />
                <Show when=move || resources_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("resource")
                        key=|entity| entity.name.clone()
                        children={move |entity| {
                            let name = entity.name.clone();
                            let describe_name = entity.name.clone();
                            let request_tx = web_console_session.request_tx;
                            let describe_command = entity_describe_command("resource", &entity.name);
                            view! {
                                <NavItem
                                    name=entity.name
                                    meta=entity.detail
                                    kind="resources"
                                    on_click=move || {
                                        if let Some(command) = describe_command.clone() {
                                            run_command(Some(command));
                                        }
                                        selected_resource.set(Some(name.clone()));
                                        upload_status.set(String::new());
                                        request_resource_describe(
                                            request_tx,
                                            describe_name.clone(),
                                            active_domain.get_untracked().unwrap_or_default(),
                                        );
                                    }
                                />
                            }
                        }}
                    />
                </Show>
                <NavHeader title="Clients" count=move || entities_for("client").len().to_string() kind="resources" open=clients_open />
                <Show when=move || clients_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("client")
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="resources" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Vhosts" count=move || entities_for("vhost").len().to_string() kind="resources" open=vhosts_open />
                <Show when=move || vhosts_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("vhost")
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="resources" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Endpoints" count=move || entities_for("endpoint").len().to_string() kind="resources" open=endpoints_open />
                <Show when=move || endpoints_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("endpoint")
                        key=|entity| entity.name.clone()
                        children={move |entity| {
                            let describe_command = entity_describe_command("endpoint", &entity.name);
                            view! {
                                <NavItem
                                    name=entity.name
                                    meta=entity.detail
                                    kind="branch"
                                    on_click=move || {
                                        if let Some(command) = describe_command.clone() {
                                            run_command(Some(command));
                                        }
                                    }
                                />
                            }
                        }}
                    />
                </Show>
            </nav>
            <div class="cluster-block">
                <p>"Cluster"</p>
                <ClusterRow label="running" value=move || cluster_counters.get().running.to_string() />
                <ClusterRow label="nodes" value=move || cluster_counters.get().nodes.to_string() />
                <ClusterRow label="relays" value=move || cluster_counters.get().relays.to_string() />
            </div>
            <Show when=move || selected_resource.get().is_some() fallback=|| ()>
                <ResourceDialog
                    resource=move || selected_resource.get().unwrap_or_default()
                    details=resource_details
                    upload_status=upload_status
                    upload_base_url=web_console_session.upload_base_url
                    auth_token=web_console_session.auth_token
                    request_tx=web_console_session.request_tx
                    active_domain=active_domain
                    close=move || selected_resource.set(None)
                />
            </Show>
        </aside>
    }
}

fn domain_can_toggle_state(status: &str) -> bool {
    domain_state_command(status).is_some()
}

fn domain_state_command(status: &str) -> Option<&'static str> {
    if status.eq_ignore_ascii_case("RUNNING") {
        Some("STOP;")
    } else if status.eq_ignore_ascii_case("STOPPED") {
        Some("START;")
    } else {
        None
    }
}

fn domain_state_title(status: &str) -> &'static str {
    if status.eq_ignore_ascii_case("RUNNING") {
        "Stop domain"
    } else if status.eq_ignore_ascii_case("STOPPED") {
        "Start domain"
    } else {
        "Domain lifecycle"
    }
}

fn domain_state_hint(status: &str, connected: bool) -> &'static str {
    if connected {
        domain_state_title(status)
    } else {
        "Waiting for connection"
    }
}

#[component]
fn MetricMini(
    value: impl Fn() -> String + Copy + Send + 'static,
    label: &'static str,
) -> impl IntoView {
    view! {
        <div>
            <strong>{move || value()}</strong>
            <span>{label}</span>
        </div>
    }
}

#[component]
fn NavHeader(
    title: &'static str,
    count: impl Fn() -> String + Copy + Send + 'static,
    kind: &'static str,
    open: RwSignal<bool>,
) -> impl IntoView {
    view! {
        <button
            class=format!("nav-header {kind}")
            type="button"
            aria-expanded=move || open.get().to_string()
            on:click=move |_| open.update(|value| *value = !*value)
        >
            <span class="section-chevron">{move || if open.get() { "⌄" } else { "›" }}</span>
            <span>{title}</span>
            <strong>{move || count()}</strong>
        </button>
    }
}

#[component]
fn NavItem(
    name: String,
    meta: String,
    kind: &'static str,
    on_click: impl Fn() + Send + 'static,
) -> impl IntoView {
    view! {
        <button class=format!("nav-item {kind}") type="button" on:click=move |_| on_click()>
            <SidebarIcon kind=kind />
            <span>{name}</span>
            <em>{meta}</em>
        </button>
    }
}

fn request_resource_describe(
    request_tx: RwSignal<Option<UnboundedSender<QueuedRequest>>>,
    resource: String,
    domain: String,
) {
    let query = format!("DESCRIBE RESOURCE {resource};");
    let request = nervix_proto::SessionRequest {
        request: Some(nervix_proto::session_request::Request::Command(
            nervix_proto::CommandRequest { query, domain },
        )),
    };
    if let Some(tx) = request_tx.get_untracked() {
        let _ = tx.unbounded_send(QueuedRequest::ResourceDescribe { resource, request });
    }
}

fn entity_describe_command(kind: &str, name: &str) -> Option<String> {
    match kind {
        "endpoint" => Some(format!("DESCRIBE ENDPOINT {name};")),
        "resource" => Some(format!("DESCRIBE RESOURCE {name};")),
        _ => None,
    }
}

#[component]
fn ResourceDialog(
    resource: impl Fn() -> String + Copy + Send + Sync + 'static,
    details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    upload_status: RwSignal<String>,
    upload_base_url: RwSignal<Option<String>>,
    auth_token: RwSignal<Option<String>>,
    request_tx: RwSignal<Option<UnboundedSender<QueuedRequest>>>,
    active_domain: RwSignal<Option<String>>,
    close: impl Fn() + Copy + Send + 'static,
) -> impl IntoView {
    let file_input = NodeRef::<leptos::html::Input>::new();
    let directory_input = NodeRef::<leptos::html::Input>::new();
    let uploading = RwSignal::new(false);
    let trigger_upload = move |input: web_sys::HtmlInputElement| {
        let resource_name = resource();
        let Some(upload_domain) = active_domain.get_untracked() else {
            upload_status.set("no active domain selected".to_string());
            return;
        };
        upload_status.set("uploading".to_string());
        uploading.set(true);
        spawn_local(async move {
            let message = upload_resource_files(
                resource_name.clone(),
                upload_domain,
                input,
                upload_base_url.get_untracked(),
                auth_token.get_untracked(),
            )
            .await;
            upload_status.set(message);
            uploading.set(false);
            request_resource_describe(
                request_tx,
                resource_name,
                active_domain.get_untracked().unwrap_or_default(),
            );
        });
    };
    view! {
        <div class="modal-scrim" on:click=move |_| close()>
            <section class="resource-dialog" on:click=move |event| event.stop_propagation()>
                <header class="subscribe-head">
                    <span class="live-dot"></span>
                    <span>"resource"</span>
                    <strong>{move || resource()}</strong>
                    <button class="dialog-close" type="button" title="Close" on:click=move |_| close()>"×"</button>
                </header>
                <div class="resource-upload-actions">
                    <input
                        node_ref=file_input
                        class="hidden-upload-input file-upload-input"
                        type="file"
                        multiple=true
                        on:change=move |event| {
                            let input = event_target_input(&event);
                            trigger_upload(input);
                        }
                    />
                    <input
                        node_ref=directory_input
                        class="hidden-upload-input directory-upload-input"
                        type="file"
                        multiple=true
                        on:change=move |event| {
                            let input = event_target_input(&event);
                            trigger_upload(input);
                        }
                    />
                    <button
                        type="button"
                        disabled=move || uploading.get()
                        on:click=move |_| {
                            if let Some(input) = file_input.get() {
                                input.click();
                            }
                        }
                    >
                        <SidebarIcon kind="resources" />
                        <span>"Upload files"</span>
                    </button>
                    <button
                        type="button"
                        disabled=move || uploading.get()
                        on:click=move |_| {
                            if let Some(input) = directory_input.get() {
                                let _ = input.set_attribute("webkitdirectory", "");
                                input.click();
                            }
                        }
                    >
                        <SidebarIcon kind="box" />
                        <span>"Upload directory"</span>
                    </button>
                </div>
                <Show when=move || !upload_status.get().is_empty() fallback=|| ()>
                    <p class="resource-upload-status">{move || upload_status.get()}</p>
                </Show>
                <div class="resource-version-list">
                    <div class="resource-version-title">
                        <span>"Versions"</span>
                        <strong>{move || {
                            details
                                .get()
                                .get(&resource())
                                .map(|detail| detail.versions.len())
                                .unwrap_or(0)
                                .to_string()
                        }}</strong>
                    </div>
                    <Show
                        when=move || {
                            details
                                .get()
                                .get(&resource())
                                .is_some_and(|detail| !detail.versions.is_empty())
                        }
                        fallback=move || {
                            view! {
                                <div class="resource-empty">
                                    {move || {
                                        details
                                            .get()
                                            .get(&resource())
                                            .map(|detail| detail.status.clone())
                                            .unwrap_or_else(|| "loading".to_string())
                                    }}
                                </div>
                            }
                        }
                    >
                        <For
                            each=move || {
                                details
                                    .get()
                                    .get(&resource())
                                    .map(|detail| detail.versions.clone())
                                    .unwrap_or_default()
                            }
                            key=|version| version.version.clone()
                            children=|version| {
                                let summary = resource_version_summary(&version);
                                let checksums = resource_version_checksums(&version);
                                let files = version.files.clone();
                                view! {
                                    <div class="resource-version-row">
                                        <strong>{format!("version {}", version.version)}</strong>
                                        <span>{summary.clone()}</span>
                                        <em>{checksums.clone()}</em>
                                        <div class="resource-file-list">
                                            <For
                                                each=move || files.clone()
                                                key=|file| format!("{}:{}", file.entry_type, file.path)
                                                children=|file| {
                                                    let file_summary = resource_file_summary(&file);
                                                    view! {
                                                        <div class="resource-file-row">
                                                            <strong>{file.path}</strong>
                                                            <span>{file_summary}</span>
                                                        </div>
                                                    }
                                                }
                                            />
                                        </div>
                                    </div>
                                }
                            }
                        />
                    </Show>
                </div>
            </section>
        </div>
    }
}

fn event_target_input(event: &ev::Event) -> web_sys::HtmlInputElement {
    event
        .target()
        .and_then(|target| target.dyn_into::<web_sys::HtmlInputElement>().ok())
        .expect("upload input event target must be an input")
}

async fn upload_resource_files(
    resource: String,
    domain: String,
    input: web_sys::HtmlInputElement,
    upload_base_url: Option<String>,
    auth_token: Option<String>,
) -> String {
    let Some(files) = input.files() else {
        return "no files selected".to_string();
    };
    if files.length() == 0 {
        return "no files selected".to_string();
    }
    let form = match web_sys::FormData::new() {
        Ok(form) => form,
        Err(_) => return "failed to create upload form".to_string(),
    };
    for index in 0..files.length() {
        let Some(file) = files.item(index) else {
            continue;
        };
        let relative_path = file_relative_path(&file);
        let file_name = if relative_path.is_empty() {
            file.name()
        } else {
            relative_path
        };
        if form
            .append_with_blob_and_filename("file", &file, &file_name)
            .is_err()
        {
            return "failed to attach selected file".to_string();
        }
    }
    input.set_value("");
    let url = web_console_resource_upload_url(
        upload_base_url.as_deref(),
        &resource,
        &domain,
        auth_token.as_deref(),
    );
    match gloo_net::http::Request::post(&url).body(form) {
        Ok(request) => match request.send().await {
            Ok(response) => {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                if (200..300).contains(&status) {
                    text
                } else if text.is_empty() {
                    format!("upload failed with HTTP {status}")
                } else {
                    text
                }
            }
            Err(error) => format!("upload request failed: {error}"),
        },
        Err(error) => format!("failed to build upload request: {error}"),
    }
}

fn web_console_resource_upload_url(
    base_url: Option<&str>,
    resource: &str,
    domain: &str,
    auth_token: Option<&str>,
) -> String {
    let auth_query = auth_token
        .map(|token| format!("&auth={}", encode_query_component(token)))
        .unwrap_or_default();
    let query = format!(
        "resource={}&domain={}{}",
        encode_query_component(resource),
        encode_query_component(domain),
        auth_query
    );
    let path = format!("/console/resources/upload?{query}");
    let Some(base_url) = base_url else {
        return path;
    };
    let Ok(mut url) = Url::parse(base_url) else {
        return path;
    };
    url.set_path("/console/resources/upload");
    url.set_query(Some(&query));
    url.set_fragment(None);
    url.to_string()
}

fn file_relative_path(file: &web_sys::File) -> String {
    js_sys::Reflect::get(file, &wasm_bindgen::JsValue::from_str("webkitRelativePath"))
        .ok()
        .and_then(|value| value.as_string())
        .unwrap_or_default()
}

fn encode_query_component(value: &str) -> String {
    js_sys::encode_uri_component(value)
        .as_string()
        .unwrap_or_else(|| value.to_string())
}

#[component]
fn ClusterRow(
    label: &'static str,
    value: impl Fn() -> String + Copy + Send + 'static,
) -> impl IntoView {
    view! {
        <div class="cluster-row">
            <span>
                <SidebarIcon kind=match label {
                    "running" => "activity",
                    "nodes" => "box",
                    _ => "branch",
                } />
                {label}
            </span>
            <strong>{move || value()}</strong>
        </div>
    }
}

#[component]
fn SidebarIcon(kind: &'static str) -> impl IntoView {
    let path = match kind {
        "schemas" => "M12 2 2 7l10 5 10-5-10-5zM2 12l10 5 10-5M2 17l10 5 10-5",
        "wire" => {
            "M12 3c4.4 0 8 1.34 8 3s-3.6 3-8 3-8-1.34-8-3 3.6-3 8-3zM4 6v6c0 1.66 3.6 3 8 3s8-1.34 \
             8-3V6M4 12v6c0 1.66 3.6 3 8 3s8-1.34 8-3v-6"
        }
        "codecs" => "M13 2 3 14h8l-1 8 10-12h-8l1-8z",
        "resources" | "box" => {
            "M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 \
             4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16zM3.3 7 12 12l8.7-5M12 22V12"
        }
        "branch" => {
            "M6 3v12M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM18 9c0 \
             6-12 0-12 6"
        }
        "activity" => "M22 12h-4l-3 8L9 4l-3 8H2",
        "play" => "M8 5v14l11-7-11-7z",
        "stop" => "M6 6h12v12H6z",
        "search" => "M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16zM21 21l-4.35-4.35",
        "x" => "M18 6 6 18M6 6l12 12",
        "zoom-out" => "M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16zM21 21l-4.35-4.35M8 11h6",
        "zoom-in" => "M11 19a8 8 0 1 1 0-16 8 8 0 0 1 0 16zM21 21l-4.35-4.35M11 8v6M8 11h6",
        "maximize" => {
            "M8 3H5a2 2 0 0 0-2 2v3M21 8V5a2 2 0 0 0-2-2h-3M16 21h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 \
             0 0 2 2h3"
        }
        "minimize" => {
            "M8 3v3a2 2 0 0 1-2 2H3M21 8h-3a2 2 0 0 1-2-2V3M16 21v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 \
             0 1 2 2v3"
        }
        "terminal" => "M4 17l6-6-6-6M12 19h8",
        "palette" => {
            "M12 22a10 10 0 1 1 10-10c0 2.2-1.8 4-4 4h-1.5c-.9 0-1.5.7-1.5 1.5 0 .4.2.8.4 \
             1.1.3.4.4.8.2 1.3-.3.8-1.5 2.1-3.6 2.1zM6.5 11.5h.01M9.5 7.5h.01M14.5 7.5h.01M17.5 \
             11.5h.01"
        }
        "chevron-up" => "M18 15l-6-6-6 6",
        "chevron-down" => "M6 9l6 6 6-6",
        _ => "M12 12m-4 0a4 4 0 1 0 8 0 4 4 0 1 0-8 0",
    };

    view! {
        <svg class="sidebar-icon" viewBox="0 0 24 24" aria-hidden="true">
            <path d=path></path>
        </svg>
    }
}

fn graph_edge_focus_request(event: &ev::MouseEvent) -> Option<(String, String, DataflowEdgeKind)> {
    let hit = web_sys::window()
        .and_then(|window| window.document())
        .and_then(|document| {
            document.element_from_point(event.client_x() as f32, event.client_y() as f32)
        })
        .and_then(graph_edge_hit_from_element)
        .or_else(|| {
            event
                .target()
                .and_then(|target| target.dyn_into::<web_sys::Element>().ok())
                .and_then(graph_edge_hit_from_element)
        })?;
    let source = hit.get_attribute("data-source")?;
    let target = hit.get_attribute("data-target")?;
    let kind = graph_edge_kind_from_label(hit.get_attribute("data-kind")?.as_str())?;
    Some((source, target, kind))
}

fn graph_edge_hit_from_element(element: web_sys::Element) -> Option<web_sys::Element> {
    element
        .closest(".graph-edge-hit")
        .ok()
        .flatten()
        .or_else(|| {
            element
                .closest(".graph-edge-group")
                .ok()
                .flatten()?
                .query_selector(".graph-edge-hit")
                .ok()
                .flatten()
        })
}

fn graph_edge_kind_from_label(label: &str) -> Option<DataflowEdgeKind> {
    match label {
        "DATA" => Some(DataflowEdgeKind::Data),
        "CORRELATION_TIMEOUT" => Some(DataflowEdgeKind::CorrelationTimeout),
        "MESSAGE_ERROR" => Some(DataflowEdgeKind::MessageError),
        "STATE_LINK" => Some(DataflowEdgeKind::StateLink),
        _ => None,
    }
}

#[component]
fn GraphPanel(
    active_domain: RwSignal<Option<String>>,
    domains: RwSignal<Vec<DomainView>>,
    websocket_state: RwSignal<ConsoleConnectionState>,
    domain: impl Fn() -> Option<GraphView> + Copy + Send + Sync + 'static,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
    start_subscription: impl Fn(String, String, usize) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let selected_relay = RwSignal::new(None::<GraphViewRelay>);
    let selected_action_target = RwSignal::new(None::<GraphActionTarget>);
    let selected_branch_group = RwSignal::new(None::<String>);
    let subscribe_filter = RwSignal::new(String::new());
    let sample_rate = RwSignal::new(0_usize);
    let graph_zoom = RwSignal::new(1.0_f64);
    let graph_pan_x = RwSignal::new(0.0_f64);
    let graph_pan_y = RwSignal::new(0.0_f64);
    let graph_drag = RwSignal::new(None::<GraphDrag>);
    let graph_moved = RwSignal::new(false);
    let graph_hover = RwSignal::new(None::<GraphHover>);
    let fullscreen = RwSignal::new(false);
    let graph_search = RwSignal::new(String::new());
    let graph_search_focus_key = RwSignal::new(None::<(GraphTopologyKey, String)>);
    let graph_stage_ref = NodeRef::<leptos::html::Div>::new();
    let current_graph_state = RwSignal::new(None::<GraphView>);
    let topology_graph_state = RwSignal::new(None::<GraphView>);
    let topology_key_state = RwSignal::new(None::<GraphTopologyKey>);
    let topology_render_count = RwSignal::new(0_u64);
    let fitted_topology_key = RwSignal::new(None::<GraphTopologyKey>);
    let snapshot_observed_at = RwSignal::new(js_sys::Date::now());
    let freshness_now = RwSignal::new(js_sys::Date::now());
    Effect::new(move |_| {
        let selected_domain = active_domain.get().unwrap_or_default();
        let next_graph = domain().filter(|graph| graph.id == selected_domain);
        if let Some(graph) = &next_graph {
            let next_key = graph.topology_key();
            if topology_key_state.get_untracked().as_ref() != Some(&next_key) {
                topology_key_state.set(Some(next_key));
                topology_graph_state.set(Some(graph.clone()));
                topology_render_count.update(|count| *count = count.saturating_add(1));
            }
        } else {
            topology_key_state.set(None);
            topology_graph_state.set(None);
        }
        snapshot_observed_at.set(js_sys::Date::now());
        current_graph_state.set(next_graph);
    });
    let freshness_interval = set_interval_with_handle(
        move || freshness_now.set(js_sys::Date::now()),
        GRAPH_FRESHNESS_TICK,
    )
    .ok();
    on_cleanup(move || {
        if let Some(interval) = freshness_interval {
            interval.clear();
        }
    });
    let visible_graph = move || {
        let selected_domain = active_domain.get().unwrap_or_default();
        current_graph_state
            .get()
            .filter(|graph| graph.id == selected_domain)
    };
    let visible_topology_graph = move || {
        let selected_domain = active_domain.get().unwrap_or_default();
        topology_graph_state
            .get()
            .filter(|graph| graph.id == selected_domain)
            .or_else(&visible_graph)
    };
    let current_graph =
        move || visible_graph().expect("graph view must exist when graph is visible");
    let current_topology_graph =
        move || visible_topology_graph().expect("graph topology must exist when graph is visible");
    let active_graph_search = move || {
        let query = graph_search.get().trim().to_ascii_lowercase();
        (query.chars().count() >= 2).then_some(query)
    };
    let domain_lifecycle = move || {
        let selected_domain = active_domain.get().unwrap_or_default();
        domains
            .get()
            .into_iter()
            .find(|domain| domain.id == selected_domain)
            .map(|domain| domain.lifecycle_label())
            .unwrap_or("STOPPED")
    };
    let graph_freshness = move || {
        if websocket_state.get() != ConsoleConnectionState::Connected {
            return "OFFLINE";
        }
        let age = freshness_now.get() - snapshot_observed_at.get();
        if age <= GRAPH_FRESHNESS_TIMEOUT.as_millis() as f64 {
            "LIVE"
        } else {
            "STALE"
        }
    };
    let focus_graph_bounds = move |graph: &GraphView, bounds: GraphBounds, max_zoom: f64| -> bool {
        let Some(stage) = graph_stage_ref.get() else {
            return false;
        };
        let stage_width = f64::from(stage.client_width());
        let stage_height = f64::from(stage.client_height());
        if stage_width <= 1.0 || stage_height <= 1.0 {
            return false;
        }
        let available_width = (stage_width - GRAPH_FIT_PADDING * 2.0).max(stage_width * 0.4);
        let available_height = (stage_height - GRAPH_FIT_PADDING * 2.0).max(stage_height * 0.4);
        let zoom = (available_width / bounds.width())
            .min(available_height / bounds.height())
            .clamp(GRAPH_MIN_ZOOM, max_zoom);
        let (center_x, center_y) = bounds.center();
        let canvas_width = f64::from(graph.canvas_width());
        let canvas_height = f64::from(graph.canvas_height());
        let base_x = (stage_width - canvas_width) / 2.0;
        let base_y = (stage_height - canvas_height) / 2.0;
        let origin_x = canvas_width / 2.0;
        let origin_y = canvas_height / 2.0;
        graph_zoom.set(zoom);
        graph_pan_x.set(stage_width / 2.0 - base_x - zoom * center_x - (1.0 - zoom) * origin_x);
        graph_pan_y.set(stage_height / 2.0 - base_y - zoom * center_y - (1.0 - zoom) * origin_y);
        true
    };
    let fit_graph = move || {
        if let Some(graph) = visible_topology_graph() {
            focus_graph_bounds(&graph, graph.canvas_bounds(), GRAPH_FIT_MAX_ZOOM);
        }
    };
    let focus_graph_edge = move |source: String, target: String, kind: DataflowEdgeKind| {
        let graph = current_topology_graph();
        let Some(bounds) = graph.edge_focus_bounds(&source, &target, kind) else {
            return;
        };
        focus_graph_bounds(&graph, bounds, GRAPH_MAX_ZOOM);
    };
    // A newly loaded graph, and every switch to a different domain, opens framed rather than at
    // an arbitrary zoom and pan. The stage is read reactively, so a graph that arrives before the
    // stage is measurable is framed as soon as it is.
    Effect::new(move |_| {
        let Some(graph) = visible_topology_graph() else {
            fitted_topology_key.set(None);
            return;
        };
        let key = graph.topology_key();
        if fitted_topology_key.get_untracked().as_ref() == Some(&key) {
            return;
        }
        if focus_graph_bounds(&graph, graph.canvas_bounds(), GRAPH_FIT_MAX_ZOOM) {
            fitted_topology_key.set(Some(key));
        }
    });
    Effect::new(move |_| {
        let Some(query) = active_graph_search() else {
            graph_search_focus_key.set(None);
            return;
        };
        let Some(graph) = visible_topology_graph() else {
            graph_search_focus_key.set(None);
            return;
        };
        let Some(bounds) = graph.search_result_bounds(&query) else {
            graph_search_focus_key.set(None);
            return;
        };
        let key = (graph.topology_key(), query);
        if graph_search_focus_key.get_untracked().as_ref() == Some(&key) {
            return;
        }
        if focus_graph_bounds(&graph, bounds, GRAPH_MAX_ZOOM) {
            graph_search_focus_key.set(Some(key));
        }
    });
    view! {
        <section class="graph-panel" class:fullscreen=move || fullscreen.get()>
            <div class="graph-toolbar">
                <div class="graph-title">
                    <SidebarIcon kind="branch" />
                    <strong>"Execution Graph"</strong>
                    <span class="graph-chevron">"›"</span>
                    <span>{move || visible_graph().map(|graph| graph.id).unwrap_or_else(|| "unavailable".to_string())}</span>
                    <span class="pill warn" data-lifecycle=domain_lifecycle>{domain_lifecycle}</span>
                    <span class="pill waiting" data-freshness=graph_freshness><i></i>{graph_freshness}</span>
                </div>
                <div class="graph-actions">
                    <div class="graph-search">
                        <SidebarIcon kind="search" />
                        <input
                            type="search"
                            aria-label="Search graph nodes"
                            placeholder="Search graph"
                            prop:value=move || graph_search.get()
                            on:input=move |event| graph_search.set(event_target_value(&event))
                        />
                        <span class="graph-search-count">
                            {move || {
                                let graph = visible_topology_graph();
                                active_graph_search()
                                    .zip(graph)
                                    .map(|(query, graph)| graph.search_result_count(&query).to_string())
                                    .unwrap_or_default()
                            }}
                        </span>
                        <button
                            type="button"
                            class="graph-search-clear"
                            title="Clear search"
                            aria-label="Clear graph search"
                            prop:disabled=move || graph_search.get().is_empty()
                            on:click=move |_| graph_search.set(String::new())
                        >
                            <SidebarIcon kind="x" />
                        </button>
                    </div>
                    <div class="zoom-group">
                        <button
                            type="button"
                            title="Zoom out"
                            on:click=move |_| graph_zoom.update(|zoom| {
                                *zoom = (*zoom - GRAPH_ZOOM_STEP).max(GRAPH_MIN_ZOOM);
                            })
                        >
                            <SidebarIcon kind="zoom-out" />
                        </button>
                        <button
                            type="button"
                            title="Reset zoom"
                            on:click=move |_| {
                                graph_zoom.set(1.0);
                                graph_pan_x.set(0.0);
                                graph_pan_y.set(0.0);
                            }
                        >
                            {move || format!("{}%", (graph_zoom.get() * 100.0).round() as i32)}
                        </button>
                        <button
                            type="button"
                            title="Zoom in"
                            on:click=move |_| graph_zoom.update(|zoom| {
                                *zoom = (*zoom + GRAPH_ZOOM_STEP).min(GRAPH_MAX_ZOOM);
                            })
                        >
                            <SidebarIcon kind="zoom-in" />
                        </button>
                        <button
                            type="button"
                            class="graph-fit"
                            title="Fit to view"
                            on:click=move |_| fit_graph()
                        >
                            "FIT"
                        </button>
                    </div>
                    <button
                        class="fullscreen-button"
                        type="button"
                        title=move || if fullscreen.get() { "Exit fullscreen" } else { "Fullscreen" }
                        on:click=move |_| fullscreen.update(|open| *open = !*open)
                    >
                        {move || {
                            if fullscreen.get() {
                                view! { <SidebarIcon kind="minimize" /> }
                            } else {
                                view! { <SidebarIcon kind="maximize" /> }
                            }
                        }}
                    </button>
                </div>
            </div>
            <Show
                when=move || visible_graph().is_some()
                fallback=|| view! {
                    <div class="graph-stage graph-error" role="alert">
                        <div class="graph-error-message">
                            <strong>"No active dataflow graph"</strong>
                            <span>"No graph snapshot was received from the leader for this console session."</span>
                        </div>
                    </div>
                }
            >
                <div
                    class="graph-stage"
                    node_ref=graph_stage_ref
                    class:dragging=move || graph_drag.get().is_some()
                    on:wheel=move |event: ev::WheelEvent| {
                        if event.ctrl_key() || event.meta_key() {
                            event.prevent_default();
                            graph_zoom.update(|zoom| {
                                *zoom = (*zoom - event.delta_y() * 0.001)
                                    .clamp(GRAPH_MIN_ZOOM, GRAPH_MAX_ZOOM);
                            });
                        }
                    }
                    on:mousedown=move |event: ev::MouseEvent| {
                        if event.button() != 0 {
                            return;
                        }
                        event.prevent_default();
                        graph_drag.set(Some(GraphDrag {
                            client_x: event.client_x(),
                            client_y: event.client_y(),
                            pan_x: graph_pan_x.get(),
                            pan_y: graph_pan_y.get(),
                        }));
                        graph_moved.set(false);
                    }
                    on:mousemove=move |event: ev::MouseEvent| {
                        if let Some(drag) = graph_drag.get() {
                            let delta_x = event.client_x() - drag.client_x;
                            let delta_y = event.client_y() - drag.client_y;
                            if delta_x.abs() > 3 || delta_y.abs() > 3 {
                                graph_moved.set(true);
                            }
                            graph_pan_x.set(drag.pan_x + f64::from(delta_x));
                            graph_pan_y.set(drag.pan_y + f64::from(delta_y));
                        }
                    }
                    on:mouseup=move |_| graph_drag.set(None)
                    on:mouseleave=move |_| {
                        graph_drag.set(None);
                        graph_hover.set(None);
                    }
                    on:click=move |event: ev::MouseEvent| {
                        if let Some((source, target, kind)) = graph_edge_focus_request(&event) {
                            event.prevent_default();
                            event.stop_propagation();
                            focus_graph_edge(source, target, kind);
                        }
                    }
                >
                    <div
                        class="graph-zoom-layer"
                        data-render-count=move || topology_render_count.get().to_string()
                        style=move || {
                            let graph = current_topology_graph();
                            format!(
                                "width: {}px; height: {}px; transform: translate({:.1}px, {:.1}px) scale({:.2});",
                                graph.canvas_width(),
                                graph.canvas_height(),
                                graph_pan_x.get(),
                                graph_pan_y.get(),
                                graph_zoom.get(),
                            )
                        }
                    >
                        <svg
                            class="graph-branch-layer"
                            viewBox=move || {
                                let graph = current_topology_graph();
                                format!("0 0 {} {}", graph.canvas_width(), graph.canvas_height())
                            }
                            aria-hidden="true"
                            focusable="false"
                        >
                            <For each={move || current_graph().groups.clone()} key=|group| {
                                (group.branch.clone(), group.active_branches)
                            } children={move |group| {
                                view! {
                                    <g class="graph-branch-group">
                                        <path
                                            class="graph-branch-body"
                                            d=group.outline.clone()
                                            stroke-width=group.outline_stroke_width()
                                            data-branch=group.branch.clone()
                                            data-key-schema=group.key_schema.clone()
                                            data-key-fields=group.key_fields_data()
                                            data-active-branches=group.active_branches.to_string()
                                        />
                                    </g>
                                }
                            }} />
                        </svg>
                        <svg
                            class="graph-pulse-layer"
                            viewBox=move || {
                                let graph = current_topology_graph();
                                format!("0 0 {} {}", graph.canvas_width(), graph.canvas_height())
                            }
                            aria-hidden="true"
                            focusable="false"
                            on:click:capture=move |event: ev::MouseEvent| {
                                if let Some((source, target, kind)) = graph_edge_focus_request(&event) {
                                    event.prevent_default();
                                    event.stop_propagation();
                                    focus_graph_edge(source, target, kind);
                                }
                            }
                        >
                            <defs>
                                <marker
                                    id="graph-arrow"
                                    markerWidth="4"
                                    markerHeight="4"
                                    refX="3.4"
                                    refY="2"
                                    orient="auto"
                                    markerUnits="strokeWidth"
                                >
                                    <path d="M0,0 L4,2 L0,4 z" class="graph-arrow-head"></path>
                                </marker>
                                <marker
                                    id="graph-arrow-hollow"
                                    markerWidth="5"
                                    markerHeight="5"
                                    refX="4.2"
                                    refY="2.5"
                                    orient="auto"
                                    markerUnits="strokeWidth"
                                >
                                    <path d="M0.5,0.5 L4.2,2.5 L0.5,4.5 z" class="graph-arrow-head hollow"></path>
                                </marker>
                            </defs>
                            <For each={move || current_topology_graph().edges.clone()} key=move |edge| {
                                (
                                    edge.source.clone(),
                                    edge.target.clone(),
                                    edge.kind,
                                    edge.path(),
                                )
                            } children={move |edge| {
                                let path = edge.path();
                                let source = edge.source.clone();
                                let target = edge.target.clone();
                                let kind = edge.kind;
                                let kind_label = kind.as_ref().to_string();
                                let class = format!("graph-edge {}", kind.css_class());
                                let emphasis_edge = edge.clone();
                                let hover_source = source.clone();
                                let hover_target = target.clone();
                                let messages_source = source.clone();
                                let messages_target = target.clone();
                                let bytes_source = source.clone();
                                let bytes_target = target.clone();
                                let batches_source = source.clone();
                                let batches_target = target.clone();
                                let messages_total_source = source.clone();
                                let messages_total_target = target.clone();
                                let bytes_total_source = source.clone();
                                let bytes_total_target = target.clone();
                                let batches_total_source = source.clone();
                                let batches_total_target = target.clone();
                                let flowing_source = source.clone();
                                let flowing_target = target.clone();
                                let route_summary = edge.route_summary();
                                view! {
                                    <g
                                        class="graph-edge-group"
                                        // A pulse only travels an edge that is actually carrying
                                        // records, so a stopped domain draws a still graph.
                                        class:flowing=move || {
                                            visible_graph().is_some_and(|graph| {
                                                graph
                                                    .edge_statistics(&flowing_source, &flowing_target, kind)
                                                    .has_edge_activity()
                                            })
                                        }
                                        class:emphasis=move || {
                                            graph_hover
                                                .get()
                                                .is_some_and(|hover| hover.emphasises_edge(&emphasis_edge))
                                        }
                                        on:mouseenter=move |_| {
                                            graph_hover.set(Some(GraphHover::Edge(
                                                hover_source.clone(),
                                                hover_target.clone(),
                                                kind,
                                            )));
                                        }
                                        on:mouseleave=move |_| graph_hover.set(None)
                                    >
                                        <title>{route_summary}</title>
                                        <path
                                            class="graph-edge-hit"
                                            data-kind=kind_label.clone()
                                            data-source=source.clone()
                                            data-target=target.clone()
                                            d=path.clone()
                                        />
                                        <path class=format!("graph-edge-shadow {}", kind.css_class()) d=path.clone() />
                                        <path
                                            class=class
                                            data-kind=kind_label
                                            data-source=source
                                            data-target=target
                                            data-feedback=edge.feedback_data()
                                            data-input-side=edge.input_side_data()
                                            data-routes=edge.routes.to_string()
                                            data-messages-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&messages_source, &messages_target, kind)
                                                    .messages_per_second
                                                    .to_string()
                                            }
                                            data-bytes-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&bytes_source, &bytes_target, kind)
                                                    .bytes_per_second
                                                    .to_string()
                                            }
                                            data-batches-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&batches_source, &batches_target, kind)
                                                    .batches_per_second
                                                    .to_string()
                                            }
                                            data-messages-total=move || {
                                                current_graph()
                                                    .edge_statistics(&messages_total_source, &messages_total_target, kind)
                                                    .messages_total
                                                    .to_string()
                                            }
                                            data-bytes-total=move || {
                                                current_graph()
                                                    .edge_statistics(&bytes_total_source, &bytes_total_target, kind)
                                                    .bytes_total
                                                    .to_string()
                                            }
                                            data-batches-total=move || {
                                                current_graph()
                                                    .edge_statistics(&batches_total_source, &batches_total_target, kind)
                                                    .batches_total
                                                    .to_string()
                                            }
                                            d=path.clone()
                                            marker-end=edge.marker()
                                        />
                                        <circle class="graph-pulse" r="3.2">
                                            <animateMotion
                                                dur="2.7s"
                                                repeatCount="indefinite"
                                                path=path
                                            />
                                        </circle>
                                    </g>
                                }
                            }} />
                        </svg>
                        <div class="graph-branch-label-layer">
                            <For each={move || current_graph().groups.clone()} key=|group| (group.branch.clone(), group.active_branches) children={move |group| {
                                view! {
                                    <BranchHeader group=group selected_branch_group=selected_branch_group />
                                }
                            }} />
                        </div>
                        <div class="graph-hit-layer" aria-label="Execution graph interactions">
                            <For each={move || current_graph().relays.clone()} key=|relay| {
                                (
                                    relay.id.clone(),
                                    relay.rect_key(),
                                    relay.label.clone(),
                                    relay.statistics.relay_buffer_capacity,
                                    relay.statistics.relay_buffer_len_p50.map(f64::to_bits),
                                    relay.statistics.relay_buffer_len_p90.map(f64::to_bits),
                                    relay.statistics.relay_buffer_len_p99.map(f64::to_bits),
                                )
                            } children={move |relay| {
                            let click_relay = relay.clone();
                            let relay_label = relay.label.clone();
                            let relay_title = relay.buffer_summary();
                            let buffer_capacity = relay.buffer_capacity_data();
                            let buffer_p50 = relay.buffer_p50_data();
                            let buffer_p90 = relay.buffer_p90_data();
                            let buffer_p99 = relay.buffer_p99_data();
                            let relay_search_class = relay.clone();
                            let relay_search_data = relay.clone();
                            let relay_dimmed = relay.clone();
                            let relay_emphasis = relay.id.clone();
                            let hover_id = relay.id.clone();
                            view! {
                                <button
                                    type="button"
                                    class="relay-hit"
                                    class:search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| relay_search_class.matches_search(&query))
                                    }
                                    class:emphasis=move || {
                                        graph_hover
                                            .get()
                                            .is_some_and(|hover| hover.emphasises_item(&relay_emphasis))
                                    }
                                    style=relay.hit_style()
                                    title=relay_title
                                    data-item-id=relay.id.clone()
                                    data-label=relay.label.clone()
                                    data-kind="RELAY"
                                    data-role="RELAY"
                                    data-status="OK"
                                    data-relay="true"
                                    data-search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| relay_search_data.matches_search(&query))
                                            .to_string()
                                    }
                                    data-search-dimmed=move || {
                                        active_graph_search()
                                            .is_some_and(|query| !relay_dimmed.matches_search(&query))
                                            .to_string()
                                    }
                                    data-buffer-capacity=buffer_capacity
                                    data-buffer-p50=buffer_p50
                                    data-buffer-p90=buffer_p90
                                    data-buffer-p99=buffer_p99
                                    on:mouseenter=move |_| graph_hover.set(Some(GraphHover::Item(hover_id.clone())))
                                    on:mouseleave=move |_| graph_hover.set(None)
                                    on:click=move |_| {
                                        if !graph_moved.get() {
                                            selected_action_target.set(Some(GraphActionTarget::relay(click_relay.clone())));
                                        }
                                    }
                                >
                                    <i class="relay-port left"></i>
                                    <span class="relay-label">{relay_label}</span>
                                    <span class="relay-buffer-distribution" aria-hidden="true">
                                        <span class="relay-buffer-quantile p50"></span>
                                        <span class="relay-buffer-quantile p90"></span>
                                        <span class="relay-buffer-quantile p99"></span>
                                    </span>
                                    <i class="relay-port right"></i>
                                </button>
                            }
                            }} />
                            <For each={move || current_graph().edges.clone()} key=move |edge| {
                                (
                                    edge.source.clone(),
                                    edge.target.clone(),
                                    edge.kind,
                                    edge.statistics.messages_per_second.to_bits(),
                                    edge.statistics.bytes_per_second.to_bits(),
                                    edge.statistics.batches_per_second.to_bits(),
                                    edge.statistics.messages_total,
                                    edge.statistics.bytes_total,
                                    edge.statistics.batches_total,
                                )
                            } children={move |edge| {
                                let title = edge.metric_summary();
                                let source = edge.source.clone();
                                let target = edge.target.clone();
                                let kind_label = edge.kind.as_ref().to_string();
                                let style = edge.metric_style();
                                let messages_rate = edge.statistics.messages_rate();
                                let has_activity = edge.statistics.has_edge_activity() && style.is_some();
                                let style = style.unwrap_or_default();
                                let messages_per_second = edge.statistics.messages_per_second.to_string();
                                let bytes_per_second = edge.statistics.bytes_per_second.to_string();
                                let batches_per_second = edge.statistics.batches_per_second.to_string();
                                let messages_total = edge.statistics.messages_total.to_string();
                                let bytes_total = edge.statistics.bytes_total.to_string();
                                let batches_total = edge.statistics.batches_total.to_string();
                                view! {
                                    <Show when=move || has_activity fallback=|| ()>
                                        <div
                                            class="graph-edge-metric"
                                            style=style.clone()
                                            title=title.clone()
                                            data-source=source.clone()
                                            data-target=target.clone()
                                            data-kind=kind_label.clone()
                                            data-messages-per-second=messages_per_second.clone()
                                            data-bytes-per-second=bytes_per_second.clone()
                                            data-batches-per-second=batches_per_second.clone()
                                            data-messages-total=messages_total.clone()
                                            data-bytes-total=bytes_total.clone()
                                            data-batches-total=batches_total.clone()
                                        >
                                            <strong class="metric-msgs"><i></i>{messages_rate.clone()}<em>"msg/s"</em></strong>
                                        </div>
                                    </Show>
                                }
                            }} />
                            <For each={move || current_graph().nodes.clone()} key=|node| {
                                (
                                    node.id.clone(),
                                    node.rect_key(),
                                    node.label.clone(),
                                    node.detail_label().to_string(),
                                    node.status,
                                    node.status_detail.clone(),
                                    node.reconnect_wait_millis,
                                )
                            } children={move |node| {
                            let class_node = node.clone();
                            let click_node = node.clone();
                            let detail = node.detail_label().to_string();
                            let detail_caption = detail.clone();
                            let label = node.label.clone();
                            let branch_summary = node.branch_summary();
                            let node_search_class = node.clone();
                            let node_search_data = node.clone();
                            let node_dimmed = node.clone();
                            let node_emphasis = node.id.clone();
                            let hover_id = node.id.clone();
                            view! {
                                <button
                                    type="button"
                                    class=move || class_node.hit_class()
                                    class:search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| node_search_class.matches_search(&query))
                                    }
                                    class:emphasis=move || {
                                        graph_hover
                                            .get()
                                            .is_some_and(|hover| hover.emphasises_item(&node_emphasis))
                                    }
                                    style=node.hit_style()
                                    title=branch_summary
                                    data-item-id=node.id.clone()
                                    data-status=node.status_label()
                                    data-label=node.label.clone()
                                    data-kind=node.kind_label()
                                    data-role=detail
                                    data-search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| node_search_data.matches_search(&query))
                                            .to_string()
                                    }
                                    data-search-dimmed=move || {
                                        active_graph_search()
                                            .is_some_and(|query| !node_dimmed.matches_search(&query))
                                            .to_string()
                                    }
                                    data-status-detail=node.status_detail.clone().unwrap_or_default()
                                    data-reconnect-wait-ms=node.reconnect_wait_millis.map(|value| value.to_string()).unwrap_or_default()
                                    on:mouseenter=move |_| graph_hover.set(Some(GraphHover::Item(hover_id.clone())))
                                    on:mouseleave=move |_| graph_hover.set(None)
                                    on:click=move |_| {
                                        if !graph_moved.get() {
                                            selected_action_target.set(Some(GraphActionTarget::node(&click_node)));
                                        }
                                    }
                                >
                                    <span class="node-accent"></span>
                                    <span class="node-hit-type">{detail_caption}</span>
                                    <span class="node-status"></span>
                                    <ReconnectTimer wait_millis=node.reconnect_wait_millis />
                                    <span class="node-hit-name">{label}</span>
                                </button>
                            }
                            }} />
                        </div>
                    </div>
                </div>
            </Show>
            <Show when=move || selected_branch_group.get().is_some() fallback=|| ()>
                <BranchDetailsDialog domain=current_graph selected_branch_group=selected_branch_group />
            </Show>
            <Show when=move || selected_action_target.get().is_some() fallback=|| ()>
                <div
                    class="modal-scrim graph-action-scrim"
                    on:click=move |_| selected_action_target.set(None)
                >
                    <section
                        class="graph-action-menu"
                        on:click=|event| event.stop_propagation()
                    >
                        <header>
                            <span>{move || selected_action_target.get().map(|target| target.kind).unwrap_or_default()}</span>
                            <strong>{move || selected_action_target.get().map(|target| target.name).unwrap_or_default()}</strong>
                        </header>
                        <div class="graph-action-list">
                            <Show when=move || selected_action_target.get().and_then(|target| target.describe_command).is_some() fallback=|| ()>
                                <button
                                    type="button"
                                    on:click=move |_| {
                                        if let Some(command) = selected_action_target.get().and_then(|target| target.describe_command) {
                                            run_command(Some(command));
                                            selected_action_target.set(None);
                                        }
                                    }
                                >
                                    "DESCRIBE"
                                </button>
                            </Show>
                            <button
                                type="button"
                                on:click=move |_| {
                                    if let Some(target) = selected_action_target.get() {
                                        run_command(Some(target.show_create_command));
                                        selected_action_target.set(None);
                                    }
                                }
                            >
                                "SHOW CREATE"
                            </button>
                            <Show when=move || selected_action_target.get().and_then(|target| target.relay).is_some() fallback=|| ()>
                                <button
                                    type="button"
                                    on:click=move |_| {
                                        if let Some(relay) = selected_action_target.get().and_then(|target| target.relay) {
                                            selected_relay.set(Some(relay));
                                            subscribe_filter.set(String::new());
                                            sample_rate.set(0);
                                            selected_action_target.set(None);
                                        }
                                    }
                                >
                                    "SUBSCRIBE"
                                </button>
                            </Show>
                        </div>
                    </section>
                </div>
            </Show>
            <Show when=move || selected_relay.get().is_some() fallback=|| ()>
                <div
                    class="modal-scrim"
                    on:click=move |_| selected_relay.set(None)
                >
                    <section
                        class="subscribe-dialog"
                        on:click=|event| event.stop_propagation()
                    >
                        <header class="subscribe-head">
                            <span class="live-dot"></span>
                            <span>"SUBSCRIBE"</span>
                            <strong>{move || selected_relay.get().map(|relay| relay.label).unwrap_or_default()}</strong>
                        </header>
                        <div class="subscribe-block">
                            <p>
                                "SCHEMA"
                                <em>{move || selected_relay.get().and_then(|relay| relay.schema).unwrap_or_default()}</em>
                            </p>
                            <For
                                each=move || selected_relay.get().map(|relay| relay.schema_fields).unwrap_or_default()
                                key=|field| field.name.clone()
                                children={move |field| {
                                    let subscribe_filter = subscribe_filter;
                                    let field_name = field.name.clone();
                                    let ty = schema_field_type_label(&field);
                                    view! {
                                        <button
                                            type="button"
                                            class="schema-row schema-field-button"
                                            on:click=move |_| {
                                                let reference = format!("input.{field_name}");
                                                append_filter_reference(subscribe_filter, &reference);
                                            }
                                        >
                                            <span>{field.name}</span>
                                            <em>{ty}</em>
                                        </button>
                                    }
                                }}
                            />
                        </div>
                        <label class="subscribe-block">
                            <p>"WHERE " <em>"(optional)"</em></p>
                            <input
                                type="text"
                                placeholder="e.g. tier = \"premium\""
                                prop:value=move || subscribe_filter.get()
                                on:input=move |event| subscribe_filter.set(event_target_value(&event))
                            />
                        </label>
                        <div class="subscribe-block">
                            <p>"SAMPLE RATE"</p>
                            <div class="sample-options">
                                <For
                                    each={|| ["100%", "10%", "1%", "0.1%"].into_iter().enumerate().collect::<Vec<_>>()}
                                    key=|(index, _)| *index
                                    children={move |(index, label)| {
                                        view! {
                                            <button
                                                type="button"
                                                class=move || if sample_rate.get() == index { "active" } else { "" }
                                                on:click=move |_| sample_rate.set(index)
                                            >
                                                {label}
                                            </button>
                                        }
                                    }}
                                />
                            </div>
                        </div>
                        <footer class="subscribe-actions">
                            <button type="button" on:click=move |_| selected_relay.set(None)>"CANCEL"</button>
                            <button
                                type="button"
                                on:click=move |_| {
                                    if let Some(relay) = selected_relay.get() {
                                        let filter = subscribe_filter.get().trim().to_string();
                                        start_subscription(relay.label, filter, sample_rate.get());
                                        selected_relay.set(None);
                                    }
                                }
                            >
                                "SUBSCRIBE"
                            </button>
                        </footer>
                    </section>
                </div>
            </Show>
            <div class="legend-row">
                <span><i class="ingestor"></i>"Ingestor"</span>
                <span><i class="processor"></i>"Processor"</span>
                <span><i class="emitter"></i>"Emitter"</span>
                <span><i class="relay"></i>"Relay"</span>
                <span><i class="client"></i>"Client"</span>
                <em>"click graph item → actions"</em>
            </div>
        </section>
    }
}

#[component]
fn ReconnectTimer(wait_millis: Option<u64>) -> impl IntoView {
    let Some(wait_millis) = wait_millis.filter(|value| *value > 0) else {
        return view! { <span class="node-reconnect-timer empty"></span> }.into_any();
    };
    let started_at = js_sys::Date::now();
    let deadline = started_at + wait_millis as f64;
    let remaining = RwSignal::new(wait_millis);
    let interval = set_interval_with_handle(
        move || {
            let millis = (deadline - js_sys::Date::now()).max(0.0).round() as u64;
            remaining.set(millis);
        },
        Duration::from_millis(100),
    )
    .ok();
    on_cleanup(move || {
        if let Some(interval) = interval {
            interval.clear();
        }
    });
    let label = move || format_timer_millis(remaining.get());
    let progress_style = move || {
        let remaining = remaining.get() as f64;
        let total = wait_millis.max(1) as f64;
        let progress = (1.0 - remaining / total).clamp(0.0, 1.0);
        format!("--timer-progress: {:.3};", progress)
    };
    view! {
        <span class="node-reconnect-timer" title="waiting before reconnect" style=progress_style>
            <i></i>
            <span>{label}</span>
        </span>
    }
    .into_any()
}

fn format_timer_millis(millis: u64) -> String {
    if millis >= 1_000 {
        format!("{:.1}s", millis as f64 / 1_000.0)
    } else {
        format!("{millis}ms")
    }
}

#[component]
fn BranchHeader(
    group: GraphBranchGroup,
    selected_branch_group: RwSignal<Option<String>>,
) -> impl IntoView {
    if group.header.is_none() {
        return ().into_any();
    }
    let branch = group.branch.clone();
    let title = group.branch.clone();
    let subtitle = group.subtitle();
    view! {
        <button
            type="button"
            class="graph-branch-label"
            style=group.header_style()
            data-branch=group.branch.clone()
            data-active-branches=group.active_branches.to_string()
            on:mousedown=move |event: ev::MouseEvent| event.stop_propagation()
            on:click=move |_| selected_branch_group.set(Some(branch.clone()))
        >
            <strong>{title}</strong>
            <span>{subtitle}</span>
        </button>
    }
    .into_any()
}

#[component]
fn BranchDetailsDialog(
    domain: impl Fn() -> GraphView + Copy + Send + 'static,
    selected_branch_group: RwSignal<Option<String>>,
) -> impl IntoView {
    let selected_group = move || {
        let selected = selected_branch_group.get()?;
        domain()
            .groups
            .into_iter()
            .find(|group| group.branch == selected)
    };
    view! {
        <div
            class="modal-scrim"
            on:click=move |_| selected_branch_group.set(None)
        >
            <section
                class="branch-dialog"
                on:click=|event| event.stop_propagation()
            >
                <header class="subscribe-head">
                    <span class="live-dot"></span>
                    <span>"BRANCH"</span>
                    <strong>{move || selected_group().map(|group| group.branch).unwrap_or_default()}</strong>
                </header>
                <div class="subscribe-block">
                    <p>"BRANCH KEY"</p>
                    <div class="schema-row">
                        <span>"schema"</span>
                        <em>{move || selected_group().map(|group| group.key_schema).unwrap_or_default()}</em>
                    </div>
                    <For
                        each=move || selected_group()
                            .map(|group| group.key_fields)
                            .unwrap_or_default()
                        key=|field| field.clone()
                        children=|field| {
                            view! {
                                <div class="schema-row">
                                    <span>{field}</span>
                                    <em>"branch key"</em>
                                </div>
                            }
                        }
                    />
                </div>
                <div class="subscribe-block">
                    <p>"BRANCH STATISTICS"</p>
                    <div class="schema-row">
                        <span>"active branches"</span>
                        <em>{move || selected_group()
                            .map(|group| group.active_branches.to_string())
                            .unwrap_or_else(|| "0".to_string())}</em>
                    </div>
                </div>
                <footer class="subscribe-actions">
                    <button type="button" on:click=move |_| selected_branch_group.set(None)>"CLOSE"</button>
                </footer>
            </section>
        </div>
    }
}

#[component]
fn ReplPanel(
    domain: impl Fn() -> String + Copy + Send + 'static,
    input: RwSignal<String>,
    terminal_lines: RwSignal<Vec<TermLine>>,
    transaction_state: impl Fn() -> Option<nervix_proto::TransactionState> + Copy + Send + 'static,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    stop_subscription: impl Fn(u64) + Copy + Send + 'static,
    suggestions: impl Fn() -> Vec<String> + Copy + Send + 'static,
    request_suggestions: impl Fn(String) + Copy + Send + 'static,
    input_enabled: impl Fn() -> bool + Copy + Send + 'static,
    run_command: impl Fn(Option<String>) + Copy + Send + 'static,
) -> impl IntoView {
    let collapsed = RwSignal::new(false);
    let completion_cycle = RwSignal::new(None::<CompletionCycle>);
    let command_history = RwSignal::new(CommandHistory::default());
    let terminal_ref = NodeRef::<leptos::html::Div>::new();
    let input_ref = NodeRef::<leptos::html::Input>::new();
    Effect::new(move |_| {
        terminal_lines.track();
        subscription_tabs.track();
        active_subscription_tab.track();
        if let Some(terminal) = terminal_ref.get_untracked() {
            terminal.set_scroll_top(terminal.scroll_height());
        }
    });
    let visible_lines = move || {
        let Some(tab_id) = active_subscription_tab.get() else {
            return (None, terminal_lines.get());
        };
        let lines = subscription_tabs
            .get()
            .into_iter()
            .find(|tab| tab.id == tab_id)
            .map(|tab| tab.lines)
            .unwrap_or_default();
        (Some(tab_id), lines)
    };
    let repl_active = move || active_subscription_tab.get().is_none();
    view! {
        <section class="repl-panel" class:collapsed=move || collapsed.get()>
            <div class="repl-toolbar">
                <button
                    type="button"
                    class=move || if repl_active() { "tab active" } else { "tab" }
                    on:click=move |_| {
                        active_subscription_tab.set(None);
                        if collapsed.get() {
                            collapsed.set(false);
                        }
                    }
                >
                    <SidebarIcon kind="terminal" />
                    <span>"NSPL REPL"</span>
                </button>
                <For
                    each=move || {
                        subscription_tabs
                            .get()
                            .into_iter()
                            .filter(|tab| tab.state == SubscriptionTabState::Open)
                            .collect::<Vec<_>>()
                    }
                    key=|tab| tab.id
                    children={move |tab| {
                        let tab_id = tab.id;
                        let title = tab.title.clone();
                        view! {
                            <div class=move || if active_subscription_tab.get() == Some(tab_id) { "tab active subscription-tab" } else { "tab subscription-tab" }>
                                <button
                                    type="button"
                                    class="tab-main"
                                    title=tab.subscribe_command.clone()
                                    data-subscription-title=title.clone()
                                    on:click=move |_| {
                                        active_subscription_tab.set(Some(tab_id));
                                        if collapsed.get() {
                                            collapsed.set(false);
                                        }
                                    }
                                >
                                    <span class="live-dot"></span>
                                    <span>{title.clone()}</span>
                                </button>
                                <button
                                    type="button"
                                    class="tab-close"
                                    title="Close stream"
                                    on:click=move |event| {
                                        event.stop_propagation();
                                        stop_subscription(tab_id);
                                    }
                                >
                                    "×"
                                </button>
                            </div>
                        }
                    }}
                />
                <button
                    class="repl-collapse"
                    type="button"
                    title=move || if collapsed.get() { "Expand panel" } else { "Minimize panel" }
                    on:click=move |_| collapsed.update(|value| *value = !*value)
                >
                    {move || {
                        if collapsed.get() {
                            view! { <SidebarIcon kind="chevron-up" /> }
                        } else {
                            view! { <SidebarIcon kind="chevron-down" /> }
                        }
                    }}
                </button>
            </div>
            <div class="terminal" node_ref=terminal_ref>
                <For
                    each={move || {
                        let (tab_id, lines) = visible_lines();
                        lines
                            .into_iter()
                            .enumerate()
                            .map(|(index, line)| ((tab_id, index), line))
                            .collect::<Vec<_>>()
                    }}
                    key=|(line_key, _)| *line_key
                    children=|(_, line)| view! { <TermLineView line=line /> }
                />
            </div>
            <div class="suggestions" class:hidden=move || !repl_active() || suggestions().is_empty()>
                <For
                    each=suggestions
                    key=|suggestion| suggestion.clone()
                    children={move |suggestion| {
                        let value = suggestion.clone();
                        view! {
                            <button
                                type="button"
                                on:click=move |_| {
                                    completion_cycle.set(None);
                                    input.set(apply_completion(&input.get_untracked(), &value));
                                }
                            >
                                {suggestion}
                            </button>
                        }
                    }}
                />
            </div>
            <form class="prompt-row" class:hidden=move || !repl_active() on:submit=move |event| {
                event.prevent_default();
                let command = input_ref
                    .get_untracked()
                    .map(|input| input.value())
                    .unwrap_or_else(|| input.get_untracked());
                completion_cycle.set(None);
                command_history.update(|history| history.push(command.as_str()));
                input.set(command.clone());
                run_command(Some(command));
            }>
                <span>{move || {
                    match transaction_state() {
                        Some(nervix_proto::TransactionState::Open) => {
                            format!("nervix[{} tx]>", domain())
                        }
                        Some(nervix_proto::TransactionState::Committing) => {
                            format!("nervix[{} committing]>", domain())
                        }
                        _ => format!("nervix[{}]>", domain()),
                    }
                }}</span>
                <input
                    node_ref=input_ref
                    type="text"
                    placeholder="type a command..."
                    disabled=move || !input_enabled()
                    prop:value=move || input.get()
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        completion_cycle.set(None);
                        command_history.update(CommandHistory::reset_navigation);
                        input.set(value.clone());
                        request_suggestions(value);
                    }
                    on:keydown=move |event: ev::KeyboardEvent| {
                        if event.key() == "Tab" {
                            event.prevent_default();
                            let suggestion_items = suggestions();
                            if !suggestion_items.is_empty() {
                                let source = completion_cycle
                                    .get_untracked()
                                    .map(|cycle| cycle.source)
                                    .unwrap_or_else(|| input.get_untracked());
                                let index = completion_cycle
                                    .get_untracked()
                                    .map(|cycle| cycle.next_index % suggestion_items.len())
                                    .unwrap_or(0);
                                input.set(apply_completion(&source, &suggestion_items[index]));
                                completion_cycle.set(Some(CompletionCycle {
                                    source,
                                    next_index: (index + 1) % suggestion_items.len(),
                                }));
                            } else {
                                request_suggestions(input.get_untracked());
                            }
                        } else if event.key() == "ArrowUp" {
                            event.prevent_default();
                            let current = input_ref
                                .get_untracked()
                                .map(|input| input.value())
                                .unwrap_or_else(|| input.get_untracked());
                            completion_cycle.set(None);
                            let mut command = None;
                            command_history.update(|history| {
                                command = history.previous(current);
                            });
                            if let Some(command) = command {
                                input.set(command.clone());
                                request_suggestions(command);
                            }
                        } else if event.key() == "ArrowDown" {
                            event.prevent_default();
                            completion_cycle.set(None);
                            let mut command = None;
                            command_history.update(|history| {
                                command = history.next();
                            });
                            if let Some(command) = command {
                                input.set(command.clone());
                                request_suggestions(command);
                            }
                        } else if event.key() == "Enter" && (event.meta_key() || event.ctrl_key()) {
                            event.prevent_default();
                            let command = input_ref
                                .get_untracked()
                                .map(|input| input.value())
                                .unwrap_or_else(|| input.get_untracked());
                            completion_cycle.set(None);
                            command_history.update(|history| history.push(command.as_str()));
                            input.set(command.clone());
                            run_command(Some(command));
                        }
                    }
                />
                <button type="submit" disabled=move || !input_enabled()>"RUN"</button>
            </form>
        </section>
    }
}

#[derive(Clone)]
struct CompletionCycle {
    source: String,
    next_index: usize,
}

#[derive(Default)]
struct CommandHistory {
    entries: Vec<String>,
    position: Option<usize>,
    draft: String,
}

impl CommandHistory {
    fn push(&mut self, command: &str) {
        let command = command.trim();
        if command.is_empty() {
            return;
        }
        if self.entries.last().is_none_or(|entry| entry != command) {
            self.entries.push(command.to_string());
        }
        self.reset_navigation();
    }

    fn previous(&mut self, current: String) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        let next_position = if let Some(position) = self.position {
            position.saturating_sub(1)
        } else {
            self.draft = current;
            self.entries.len() - 1
        };
        self.position = Some(next_position);
        self.entries.get(next_position).cloned()
    }

    fn next(&mut self) -> Option<String> {
        let position = self.position?;
        if position + 1 < self.entries.len() {
            let next_position = position + 1;
            self.position = Some(next_position);
            self.entries.get(next_position).cloned()
        } else {
            self.position = None;
            Some(self.draft.clone())
        }
    }

    fn reset_navigation(&mut self) {
        self.position = None;
        self.draft.clear();
    }
}

fn apply_completion(input: &str, suggestion: &str) -> String {
    let prefix_start = input
        .char_indices()
        .rev()
        .find_map(|(index, character)| {
            character
                .is_whitespace()
                .then_some(index + character.len_utf8())
        })
        .unwrap_or(0);
    let mut completed = String::with_capacity(prefix_start + suggestion.len());
    completed.push_str(&input[..prefix_start]);
    completed.push_str(suggestion);
    completed
}

#[component]
fn TermLineView(line: TermLine) -> impl IntoView {
    let class_name = line.kind.class_name();
    if let TermLineKind::Prompt = line.kind {
        let (prompt, command) = line.text.split_once(' ').unwrap_or((&line.text, ""));
        view! {
            <div class=class_name>
                <span>{prompt.to_string()}</span>
                <em>{command.to_string()}</em>
            </div>
        }
        .into_any()
    } else {
        view! { <div class=class_name>{line.text}</div> }.into_any()
    }
}

#[derive(Clone, Copy)]
struct ThemeView {
    id: &'static str,
    label: &'static str,
    swatches: [&'static str; 3],
}

#[derive(Clone)]
struct GraphView {
    id: String,
    statistics: GraphStatistics,
    nodes: Vec<GraphViewNode>,
    relays: Vec<GraphViewRelay>,
    edges: Vec<GraphViewEdge>,
    groups: Vec<GraphBranchGroup>,
    width: i32,
    height: i32,
}

impl GraphView {
    fn from_dataflow_graph(graph: DataflowGraph) -> Self {
        let layout = Layout::build(
            &graph
                .nodes
                .iter()
                .map(graph_layout_item)
                .collect::<Vec<_>>(),
            &graph
                .edges
                .iter()
                .map(graph_layout_edge)
                .collect::<Vec<_>>(),
        );

        let mut nodes = Vec::new();
        let mut relays = Vec::new();
        for node in graph.nodes {
            let rect = layout.items.get(&node.id).copied().unwrap_or_default();
            let branches = node
                .branches
                .into_iter()
                .map(|branch| GraphBranchStatistics {
                    branch: branch.branch,
                    statistics: GraphStatistics::from(branch.statistics),
                })
                .collect();
            if node.role.is_relay() {
                relays.push(GraphViewRelay {
                    id: node.id,
                    label: node.label,
                    rect,
                    schema: node.schema,
                    schema_fields: node
                        .schema_fields
                        .into_iter()
                        .map(GraphSchemaField::from)
                        .collect(),
                    branch: node.branch,
                    statistics: GraphStatistics::from(node.statistics),
                    branches,
                });
            } else {
                nodes.push(GraphViewNode {
                    id: node.id,
                    label: node.label,
                    kind: NodeKind::from_dataflow_kind(node.role.kind()),
                    role: node.role,
                    status: node.status,
                    status_detail: node.status_detail,
                    reconnect_wait_millis: node.reconnect_wait_millis,
                    rect,
                    branch: node.branch,
                    branches,
                });
            }
        }

        let routes = layout
            .edges
            .iter()
            .map(|edge| ((edge.source.as_str(), edge.target.as_str()), edge))
            .collect::<BTreeMap<_, _>>();
        let edges = graph
            .edges
            .into_iter()
            .map(|edge| {
                let route = routes.get(&(edge.source.as_str(), edge.target.as_str()));
                GraphViewEdge {
                    points: route.map(|route| route.points.clone()).unwrap_or_default(),
                    badge: route.and_then(|route| route.badge),
                    feedback: route.is_some_and(|route| route.feedback),
                    source: edge.source,
                    target: edge.target,
                    kind: edge.kind,
                    input_side: edge.input_side,
                    routes: edge.routes,
                    statistics: GraphStatistics::from(edge.statistics),
                    branches: edge
                        .branches
                        .into_iter()
                        .map(|branch| GraphBranchStatistics {
                            branch: branch.branch,
                            statistics: GraphStatistics::from(branch.statistics),
                        })
                        .collect(),
                }
            })
            .collect::<Vec<_>>();

        let groups = layout
            .groups
            .iter()
            .map(|region| GraphBranchGroup::new(region, &nodes, &relays, &edges))
            .collect();

        Self {
            id: graph.domain,
            statistics: GraphStatistics::from(graph.statistics),
            nodes,
            relays,
            edges,
            groups,
            width: layout.width,
            height: layout.height,
        }
    }

    fn topology_key(&self) -> GraphTopologyKey {
        GraphTopologyKey {
            id: self.id.clone(),
            nodes: self.nodes.iter().map(GraphNodeTopologyKey::from).collect(),
            relays: self
                .relays
                .iter()
                .map(GraphRelayTopologyKey::from)
                .collect(),
            edges: self.edges.iter().map(GraphEdgeTopologyKey::from).collect(),
        }
    }

    fn edge_statistics(
        &self,
        source: &str,
        target: &str,
        kind: DataflowEdgeKind,
    ) -> GraphStatistics {
        self.edges
            .iter()
            .find(|edge| edge.source == source && edge.target == target && edge.kind == kind)
            .map(|edge| edge.statistics)
            .unwrap_or_default()
    }

    fn edge_focus_bounds(
        &self,
        source: &str,
        target: &str,
        kind: DataflowEdgeKind,
    ) -> Option<GraphBounds> {
        let edge = self
            .edges
            .iter()
            .find(|edge| edge.source == source && edge.target == target && edge.kind == kind)?;
        let mut bounds = None::<GraphBounds>;
        for id in [edge.source.as_str(), edge.target.as_str()] {
            if let Some(item) = self.item_bounds(id) {
                include(&mut bounds, item);
            }
        }
        for point in &edge.points {
            include(&mut bounds, GraphBounds::from_point(point.0, point.1));
        }
        bounds
    }

    fn search_result_bounds(&self, query: &str) -> Option<GraphBounds> {
        let mut bounds = None::<GraphBounds>;
        for node in self.nodes.iter().filter(|node| node.matches_search(query)) {
            include(&mut bounds, GraphBounds::from_rect(node.rect));
        }
        for relay in self
            .relays
            .iter()
            .filter(|relay| relay.matches_search(query))
        {
            include(&mut bounds, GraphBounds::from_rect(relay.rect));
        }
        bounds
    }

    fn search_result_count(&self, query: &str) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.matches_search(query))
            .count()
            + self
                .relays
                .iter()
                .filter(|relay| relay.matches_search(query))
                .count()
    }

    /// The whole drawing, used to frame the graph on load and when the fit control is pressed.
    fn canvas_bounds(&self) -> GraphBounds {
        GraphBounds::from_rect(Rect {
            x: 0,
            y: 0,
            width: self.width,
            height: self.height,
        })
    }

    fn item_bounds(&self, id: &str) -> Option<GraphBounds> {
        if let Some(node) = self
            .nodes
            .iter()
            .find(|node| Self::item_matches(&node.id, &node.label, id))
        {
            return Some(GraphBounds::from_rect(node.rect));
        }
        self.relays
            .iter()
            .find(|relay| Self::item_matches(&relay.id, &relay.label, id))
            .map(|relay| GraphBounds::from_rect(relay.rect))
    }

    fn item_matches(candidate_id: &str, candidate_label: &str, requested: &str) -> bool {
        if candidate_id == requested || candidate_label == requested {
            return true;
        }
        if let Some((_, suffix)) = requested.rsplit_once(':')
            && (candidate_id == suffix || candidate_label == suffix)
        {
            return true;
        }
        false
    }

    const fn canvas_width(&self) -> i32 {
        self.width
    }

    const fn canvas_height(&self) -> i32 {
        self.height
    }
}

fn include(bounds: &mut Option<GraphBounds>, next: GraphBounds) {
    match bounds {
        Some(bounds) => bounds.include_bounds(next),
        None => *bounds = Some(next),
    }
}

/// Everything the drawing is derived from. Geometry is a pure function of topology, so it is
/// deliberately absent here: a graph that moves without changing shape is the same topology.
#[derive(Clone, PartialEq, Eq)]
struct GraphTopologyKey {
    id: String,
    nodes: BTreeSet<GraphNodeTopologyKey>,
    relays: BTreeSet<GraphRelayTopologyKey>,
    edges: BTreeSet<GraphEdgeTopologyKey>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphNodeTopologyKey {
    id: String,
    label: String,
    role: DataflowNodeRole,
    branch: Option<GraphBranchTopologyKey>,
}

impl From<&GraphViewNode> for GraphNodeTopologyKey {
    fn from(node: &GraphViewNode) -> Self {
        Self {
            id: node.id.clone(),
            label: node.label.clone(),
            role: node.role.clone(),
            branch: node.branch.as_ref().map(GraphBranchTopologyKey::from),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphRelayTopologyKey {
    id: String,
    label: String,
    schema: Option<String>,
    schema_fields: Vec<GraphSchemaFieldTopologyKey>,
    branch: Option<GraphBranchTopologyKey>,
}

impl From<&GraphViewRelay> for GraphRelayTopologyKey {
    fn from(relay: &GraphViewRelay) -> Self {
        Self {
            id: relay.id.clone(),
            label: relay.label.clone(),
            schema: relay.schema.clone(),
            schema_fields: relay
                .schema_fields
                .iter()
                .map(GraphSchemaFieldTopologyKey::from)
                .collect(),
            branch: relay.branch.as_ref().map(GraphBranchTopologyKey::from),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphBranchTopologyKey {
    name: String,
    key_schema: String,
    key_fields: Vec<String>,
}

impl From<&DataflowBranch> for GraphBranchTopologyKey {
    fn from(branch: &DataflowBranch) -> Self {
        Self {
            name: branch.name.clone(),
            key_schema: branch.key_schema.clone(),
            key_fields: branch.key_fields.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphSchemaFieldTopologyKey {
    name: String,
    ty: String,
    optional: bool,
    sensitive: bool,
}

impl From<&GraphSchemaField> for GraphSchemaFieldTopologyKey {
    fn from(field: &GraphSchemaField) -> Self {
        Self {
            name: field.name.clone(),
            ty: field.ty.clone(),
            optional: field.optional,
            sensitive: field.sensitive,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphEdgeTopologyKey {
    source: String,
    target: String,
    kind: DataflowEdgeKind,
    input_side: Option<DataflowInputSide>,
    routes: u32,
}

impl From<&GraphViewEdge> for GraphEdgeTopologyKey {
    fn from(edge: &GraphViewEdge) -> Self {
        Self {
            source: edge.source.clone(),
            target: edge.target.clone(),
            kind: edge.kind,
            input_side: edge.input_side,
            routes: edge.routes,
        }
    }
}

#[derive(Clone)]
struct GraphActionTarget {
    kind: &'static str,
    name: String,
    describe_command: Option<String>,
    show_create_command: String,
    relay: Option<GraphViewRelay>,
}

impl GraphActionTarget {
    fn node(node: &GraphViewNode) -> Self {
        let kind = node.command_kind();
        let name = node.label.clone();
        Self {
            kind,
            name: name.clone(),
            describe_command: describe_command(kind, &name),
            show_create_command: format!("SHOW CREATE {kind} {name};"),
            relay: None,
        }
    }

    fn relay(relay: GraphViewRelay) -> Self {
        let name = relay.label.clone();
        Self {
            kind: "RELAY",
            name: name.clone(),
            describe_command: Some(format!("DESCRIBE RELAY {name};")),
            show_create_command: format!("SHOW CREATE RELAY {name};"),
            relay: Some(relay),
        }
    }
}

fn describe_command(kind: &str, name: &str) -> Option<String> {
    match kind {
        "INGESTOR" | "DEDUPLICATOR" | "REINGESTOR" | "REORDERER" | "WASM PROCESSOR"
        | "CORRELATOR" | "EMITTER" => Some(format!("DESCRIBE {kind} {name};")),
        "WINDOW PROCESSOR" => Some(format!("DESCRIBE WINDOW PROCESSOR {name};")),
        _ => None,
    }
}

#[derive(Clone)]
struct GraphViewNode {
    id: String,
    label: String,
    kind: NodeKind,
    role: DataflowNodeRole,
    status: DataflowNodeStatus,
    status_detail: Option<String>,
    reconnect_wait_millis: Option<u64>,
    rect: Rect,
    branch: Option<DataflowBranch>,
    branches: Vec<GraphBranchStatistics>,
}

impl GraphViewNode {
    fn hit_class(&self) -> &'static str {
        match (self.kind, self.status) {
            (NodeKind::Client, DataflowNodeStatus::Ok) => "node-hit client status-ok",
            (NodeKind::Ingestor, DataflowNodeStatus::Ok) => "node-hit ingestor status-ok",
            (NodeKind::Processor, DataflowNodeStatus::Ok) => "node-hit processor status-ok",
            (NodeKind::Emitter, DataflowNodeStatus::Ok) => "node-hit emitter status-ok",
            (NodeKind::Client, DataflowNodeStatus::Error) => "node-hit client status-error",
            (NodeKind::Ingestor, DataflowNodeStatus::Error) => "node-hit ingestor status-error",
            (NodeKind::Processor, DataflowNodeStatus::Error) => "node-hit processor status-error",
            (NodeKind::Emitter, DataflowNodeStatus::Error) => "node-hit emitter status-error",
        }
    }

    fn hit_style(&self) -> String {
        graph_position_style(self.rect)
    }

    fn matches_search(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        query.chars().count() >= 2
            && (self.id.to_ascii_lowercase().contains(&query)
                || self.label.to_ascii_lowercase().contains(&query))
    }

    const fn status_label(&self) -> &'static str {
        match self.status {
            DataflowNodeStatus::Ok => "OK",
            DataflowNodeStatus::Error => "ERROR",
        }
    }

    fn kind_label(&self) -> String {
        self.role.kind().as_ref().to_string()
    }

    /// The drawn rectangle as a keyable value, so a card is re-rendered exactly when it moves.
    const fn rect_key(&self) -> (i32, i32, i32, i32) {
        rect_key(self.rect)
    }

    /// The caption drawn on the card: the transport for a connector, the processor for a
    /// processor.
    fn detail_label(&self) -> &str {
        self.role.detail_label()
    }

    /// The branch group this node is drawn inside. A node that constructs or collapses branches
    /// stands on the group's border rather than within it, which is the same rule the layout
    /// applies when it decides which items a group's bands contain.
    fn group_branch(&self) -> Option<&str> {
        self.branch
            .as_ref()
            .filter(|_| !self.role.constructs_branches() && !self.role.collapses_branches())
            .map(|branch| branch.name.as_str())
    }

    fn command_kind(&self) -> &'static str {
        match self.role.processor() {
            Some(DataflowProcessorKind::Junction) => "JUNCTION",
            Some(DataflowProcessorKind::Deduplicator) => "DEDUPLICATOR",
            Some(DataflowProcessorKind::Correlator) => "CORRELATOR",
            Some(DataflowProcessorKind::Reorderer) => "REORDERER",
            Some(DataflowProcessorKind::WindowProcessor) => "WINDOW PROCESSOR",
            Some(DataflowProcessorKind::WasmProcessor) => "WASM PROCESSOR",
            Some(DataflowProcessorKind::Inferencer) => "INFERENCER",
            Some(DataflowProcessorKind::Generator) => "GENERATOR",
            Some(DataflowProcessorKind::Reingestor) => "REINGESTOR",
            None => match self.kind {
                NodeKind::Client => "CLIENT",
                NodeKind::Ingestor => "INGESTOR",
                NodeKind::Emitter => "EMITTER",
                NodeKind::Processor => "PROCESSOR",
            },
        }
    }

    fn branch_summary(&self) -> String {
        let status = self
            .status_detail
            .as_ref()
            .map(|detail| format!("status: {}\n{detail}", self.status_label()))
            .unwrap_or_else(|| format!("status: {}", self.status_label()));
        if self.branches.is_empty() {
            return format!("{status}\nno branch statistics");
        }
        let branches = self
            .branches
            .iter()
            .map(|branch| {
                format!(
                    "{}: {}/s, {}/s, {}/s",
                    branch.branch,
                    branch.statistics.messages_rate(),
                    branch.statistics.bytes_rate(),
                    branch.statistics.batches_rate()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!("{status}\n{branches}")
    }
}

#[derive(Clone)]
struct GraphBranchStatistics {
    branch: String,
    statistics: GraphStatistics,
}

#[derive(Clone, Copy, Default)]
struct GraphStatistics {
    messages_per_second: f64,
    bytes_per_second: f64,
    batches_per_second: f64,
    messages_total: u64,
    bytes_total: u64,
    batches_total: u64,
    relay_buffer_capacity: Option<u64>,
    relay_buffer_len_p50: Option<f64>,
    relay_buffer_len_p90: Option<f64>,
    relay_buffer_len_p99: Option<f64>,
}

impl GraphStatistics {
    fn messages_rate(self) -> String {
        format_scaled_metric(self.messages_per_second)
    }

    fn bytes_rate(self) -> String {
        format_bytes_metric(self.bytes_per_second)
    }

    fn batches_rate(self) -> String {
        format_scaled_metric(self.batches_per_second)
    }

    fn has_batches(self) -> bool {
        self.batches_total > 0 || self.batches_per_second > 0.0
    }

    fn has_edge_activity(self) -> bool {
        self.messages_per_second > 0.0
            || self.bytes_per_second > 0.0
            || self.batches_per_second > 0.0
    }
}

impl From<DataflowStatistics> for GraphStatistics {
    fn from(value: DataflowStatistics) -> Self {
        Self {
            messages_per_second: value.messages_per_second,
            bytes_per_second: value.bytes_per_second,
            batches_per_second: value.batches_per_second,
            messages_total: value.messages_total,
            bytes_total: value.bytes_total,
            batches_total: value.batches_total,
            relay_buffer_capacity: value.relay_buffer_capacity,
            relay_buffer_len_p50: value.relay_buffer_len_p50,
            relay_buffer_len_p90: value.relay_buffer_len_p90,
            relay_buffer_len_p99: value.relay_buffer_len_p99,
        }
    }
}

#[derive(Clone)]
struct GraphViewRelay {
    id: String,
    label: String,
    rect: Rect,
    schema: Option<String>,
    schema_fields: Vec<GraphSchemaField>,
    branch: Option<DataflowBranch>,
    statistics: GraphStatistics,
    branches: Vec<GraphBranchStatistics>,
}

#[derive(Clone)]
struct GraphSchemaField {
    name: String,
    ty: String,
    optional: bool,
    sensitive: bool,
}

impl From<DataflowSchemaField> for GraphSchemaField {
    fn from(value: DataflowSchemaField) -> Self {
        Self {
            name: value.name,
            ty: value.ty,
            optional: value.optional,
            sensitive: value.sensitive,
        }
    }
}

impl GraphViewRelay {
    const fn rect_key(&self) -> (i32, i32, i32, i32) {
        rect_key(self.rect)
    }

    fn hit_style(&self) -> String {
        format!(
            "{} --relay-buffer-p50: {:.2}%; --relay-buffer-p90: {:.2}%; --relay-buffer-p99: \
             {:.2}%;",
            graph_position_style(self.rect),
            self.buffer_percent(self.statistics.relay_buffer_len_p50),
            self.buffer_percent(self.statistics.relay_buffer_len_p90),
            self.buffer_percent(self.statistics.relay_buffer_len_p99),
        )
    }

    fn matches_search(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        query.chars().count() >= 2
            && (self.id.to_ascii_lowercase().contains(&query)
                || self.label.to_ascii_lowercase().contains(&query))
    }

    fn group_branch(&self) -> Option<&str> {
        self.branch.as_ref().map(|branch| branch.name.as_str())
    }

    fn buffer_summary(&self) -> String {
        let Some(capacity) = self.statistics.relay_buffer_capacity else {
            return String::new();
        };
        format!(
            "buffer p50 {}/{}; p90 {}/{}; p99 {}/{}",
            graph_optional_number(self.statistics.relay_buffer_len_p50),
            capacity,
            graph_optional_number(self.statistics.relay_buffer_len_p90),
            capacity,
            graph_optional_number(self.statistics.relay_buffer_len_p99),
            capacity
        )
    }

    fn buffer_percent(&self, value: Option<f64>) -> f64 {
        let Some(capacity) = self.statistics.relay_buffer_capacity else {
            return 0.0;
        };
        if capacity == 0 {
            return 0.0;
        }
        let value = value.unwrap_or(0.0);
        (value / capacity as f64 * 100.0).clamp(0.0, 100.0)
    }

    fn buffer_capacity_data(&self) -> String {
        self.statistics
            .relay_buffer_capacity
            .map(|value| value.to_string())
            .unwrap_or_default()
    }

    fn buffer_p50_data(&self) -> String {
        graph_optional_number(self.statistics.relay_buffer_len_p50)
    }

    fn buffer_p90_data(&self) -> String {
        graph_optional_number(self.statistics.relay_buffer_len_p90)
    }

    fn buffer_p99_data(&self) -> String {
        graph_optional_number(self.statistics.relay_buffer_len_p99)
    }
}

/// A drawn branch group: the region the layout reserved for one branch, plus the branch identity
/// and live branch count of the items inside it.
#[derive(Clone)]
struct GraphBranchGroup {
    branch: String,
    key_schema: String,
    key_fields: Vec<String>,
    outline: String,
    header: Option<Rect>,
    active_branches: usize,
}

impl GraphBranchGroup {
    fn new(
        region: &GroupRegion,
        nodes: &[GraphViewNode],
        relays: &[GraphViewRelay],
        edges: &[GraphViewEdge],
    ) -> Self {
        let members = nodes
            .iter()
            .filter(|node| node.group_branch() == Some(region.branch.as_str()))
            .map(|node| node.id.as_str())
            .chain(
                relays
                    .iter()
                    .filter(|relay| relay.group_branch() == Some(region.branch.as_str()))
                    .map(|relay| relay.id.as_str()),
            )
            .collect::<BTreeSet<_>>();

        // The branch key is declared identically by every member, so any member states it.
        let identity = nodes
            .iter()
            .filter_map(|node| node.branch.as_ref())
            .chain(relays.iter().filter_map(|relay| relay.branch.as_ref()))
            .find(|branch| branch.name == region.branch);

        let mut active = BTreeSet::<&str>::new();
        for node in nodes
            .iter()
            .filter(|node| members.contains(node.id.as_str()))
        {
            active.extend(node.branches.iter().map(|branch| branch.branch.as_str()));
        }
        for relay in relays
            .iter()
            .filter(|relay| members.contains(relay.id.as_str()))
        {
            active.extend(relay.branches.iter().map(|branch| branch.branch.as_str()));
        }
        for edge in edges.iter().filter(|edge| {
            members.contains(edge.source.as_str()) || members.contains(edge.target.as_str())
        }) {
            active.extend(edge.branches.iter().map(|branch| branch.branch.as_str()));
        }

        Self {
            branch: region.branch.clone(),
            key_schema: identity
                .map(|branch| branch.key_schema.clone())
                .unwrap_or_default(),
            key_fields: identity
                .map(|branch| branch.key_fields.clone())
                .unwrap_or_default(),
            outline: region.outline(),
            header: region.header_anchor(),
            active_branches: active.len(),
        }
    }

    fn key_fields_data(&self) -> String {
        self.key_fields.join(",")
    }

    /// The outline weight, which grows with the number of live branches so a busy group reads as
    /// heavier than a quiet one.
    fn outline_stroke_width(&self) -> String {
        let count = self.active_branches.min(8) as f64;
        format!("{:.2}", 1.0 + count * 0.35)
    }

    fn header_style(&self) -> String {
        self.header.map(graph_position_style).unwrap_or_default()
    }

    /// The line under the branch name: its key fields, then how many branches are live.
    fn subtitle(&self) -> String {
        let key = if self.key_fields.is_empty() {
            "(singleton key)".to_string()
        } else {
            format!("({})", self.key_fields.join(", "))
        };
        format!("{key} · {} br", self.active_branches)
    }
}

#[derive(Clone, Copy)]
struct GraphBounds {
    left: f64,
    top: f64,
    right: f64,
    bottom: f64,
}

impl GraphBounds {
    fn from_point(x: i32, y: i32) -> Self {
        let x = f64::from(x);
        let y = f64::from(y);
        Self {
            left: x,
            top: y,
            right: x,
            bottom: y,
        }
    }

    fn from_rect(rect: Rect) -> Self {
        Self {
            left: f64::from(rect.x),
            top: f64::from(rect.y),
            right: f64::from(rect.right()),
            bottom: f64::from(rect.bottom()),
        }
    }

    fn include_point(&mut self, x: f64, y: f64) {
        self.left = self.left.min(x);
        self.top = self.top.min(y);
        self.right = self.right.max(x);
        self.bottom = self.bottom.max(y);
    }

    fn include_bounds(&mut self, bounds: Self) {
        self.include_point(bounds.left, bounds.top);
        self.include_point(bounds.right, bounds.bottom);
    }

    fn width(self) -> f64 {
        (self.right - self.left).max(1.0)
    }

    fn height(self) -> f64 {
        (self.bottom - self.top).max(1.0)
    }

    fn center(self) -> (f64, f64) {
        (
            (self.left + self.right) / 2.0,
            (self.top + self.bottom) / 2.0,
        )
    }
}

#[derive(Clone)]
struct GraphViewEdge {
    source: String,
    target: String,
    kind: DataflowEdgeKind,
    input_side: Option<DataflowInputSide>,
    routes: u32,
    statistics: GraphStatistics,
    branches: Vec<GraphBranchStatistics>,
    /// The turns the drawn line makes, left to right, as the layout placed them.
    points: Vec<(i32, i32)>,
    /// Where the rate badge sits, when this edge carries traffic worth reporting.
    badge: Option<Rect>,
    /// A return path: it travels right to left against the flow.
    feedback: bool,
}

impl GraphViewEdge {
    /// The radius of a drawn corner. A corner between two short segments uses half of it so the
    /// curve can never eat the segment it turns out of.
    const CORNER_RADIUS: i32 = 10;

    fn path(&self) -> String {
        let Some(start) = self.points.first() else {
            return String::new();
        };
        let mut path = format!("M{} {}", start.0, start.1);
        if self.points.len() == 1 {
            return path;
        }
        for index in 1..self.points.len() - 1 {
            let previous = self.points[index - 1];
            let current = self.points[index];
            let next = self.points[index + 1];
            let incoming = (current.0 - previous.0, current.1 - previous.1);
            let outgoing = (next.0 - current.0, next.1 - current.1);
            let incoming_length = incoming.0.abs() + incoming.1.abs();
            let outgoing_length = outgoing.0.abs() + outgoing.1.abs();
            let uniform = Self::CORNER_RADIUS;
            let radius = if incoming_length < uniform * 2 || outgoing_length < uniform * 2 {
                uniform / 2
            } else {
                uniform
            }
            .min(incoming_length / 2)
            .min(outgoing_length / 2);
            if radius == 0 {
                path.push_str(&format!(" L{} {}", current.0, current.1));
                continue;
            }
            let entry = (
                current.0 - incoming.0.signum() * radius,
                current.1 - incoming.1.signum() * radius,
            );
            let exit = (
                current.0 + outgoing.0.signum() * radius,
                current.1 + outgoing.1.signum() * radius,
            );
            path.push_str(&format!(" L{} {}", entry.0, entry.1));
            path.push_str(&format!(
                " Q{} {}, {} {}",
                current.0, current.1, exit.0, exit.1
            ));
        }
        let end = self.points.last().expect("non-empty points checked above");
        path.push_str(&format!(" L{} {}", end.0, end.1));
        path
    }

    fn metric_style(&self) -> Option<String> {
        self.badge.map(graph_position_style)
    }

    /// A state dependency is looked up rather than delivered, so it ends in a hollow head.
    const fn marker(&self) -> &'static str {
        if self.kind.carries_records() {
            "url(#graph-arrow)"
        } else {
            "url(#graph-arrow-hollow)"
        }
    }

    /// What this line stands for, reported on hover whether or not it is carrying traffic.
    fn route_summary(&self) -> String {
        let side = match self.input_side {
            Some(DataflowInputSide::Left) => " into LEFT",
            Some(DataflowInputSide::Right) => " into RIGHT",
            None => "",
        };
        let subject = match self.kind {
            DataflowEdgeKind::Data => "records",
            DataflowEdgeKind::CorrelationTimeout => "correlation timeouts",
            DataflowEdgeKind::MessageError => "message errors",
            DataflowEdgeKind::StateLink => "materialized state",
        };
        let routes = if self.routes > 1 {
            format!(" · {} routes", self.routes)
        } else {
            String::new()
        };
        let feedback = if self.feedback { " · return path" } else { "" };
        format!(
            "{} → {}{side}: {subject}{routes}{feedback}",
            self.source, self.target
        )
    }

    fn feedback_data(&self) -> String {
        self.feedback.to_string()
    }

    fn input_side_data(&self) -> String {
        self.input_side
            .map(|side| side.as_ref().to_string())
            .unwrap_or_default()
    }

    fn metric_summary(&self) -> String {
        let mut parts = vec![
            format!(
                "messages: {}/s total {}",
                self.statistics.messages_rate(),
                self.statistics.messages_total
            ),
            format!(
                "bytes: {}/s total {}",
                self.statistics.bytes_rate(),
                self.statistics.bytes_total
            ),
        ];
        if self.statistics.has_batches() {
            parts.push(format!(
                "batches: {}/s total {}",
                self.statistics.batches_rate(),
                self.statistics.batches_total
            ));
        }
        if self.routes > 1 {
            parts.push(format!("routes: {}", self.routes));
        }
        parts.join("; ")
    }
}

trait DataflowEdgeKindView {
    fn css_class(self) -> &'static str;
}

impl DataflowEdgeKindView for DataflowEdgeKind {
    fn css_class(self) -> &'static str {
        match self {
            Self::Data => "graph-edge--data",
            Self::CorrelationTimeout => "graph-edge--correlation-timeout",
            Self::MessageError => "graph-edge--message-error",
            Self::StateLink => "graph-edge--state-link",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DomainView {
    id: String,
    mode: String,
    status: String,
}

impl DomainView {
    /// The lifecycle the console reports for this domain, normalised to the three states a
    /// domain can be in.
    fn lifecycle_label(&self) -> &'static str {
        if self.status.eq_ignore_ascii_case("RUNNING") {
            "RUNNING"
        } else if self.status.eq_ignore_ascii_case("PAUSED") {
            "PAUSED"
        } else {
            "STOPPED"
        }
    }
}

impl From<nervix_proto::DomainInfo> for DomainView {
    fn from(value: nervix_proto::DomainInfo) -> Self {
        Self {
            id: value.id,
            mode: value.pace,
            status: value.status,
        }
    }
}

#[derive(Clone, PartialEq)]
struct DomainSnapshotView {
    domain: String,
    dataflow_graph: DataflowGraph,
    entities: Vec<EntityView>,
}

impl DomainSnapshotView {
    fn from_snapshot(
        snapshot: nervix_proto::DomainSnapshot,
        dataflow_graph: DataflowGraph,
    ) -> Self {
        let mut entities = snapshot
            .entities
            .into_iter()
            .map(EntityView::from)
            .collect::<Vec<_>>();
        entities.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.name.cmp(&right.name))
        });
        Self {
            domain: snapshot.domain,
            dataflow_graph,
            entities,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClusterCounters {
    running: u64,
    nodes: u64,
    relays: u64,
}

impl From<nervix_proto::ClusterSummary> for ClusterCounters {
    fn from(summary: nervix_proto::ClusterSummary) -> Self {
        Self {
            running: summary.running_domains,
            nodes: summary.nodes,
            relays: summary.relays,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct EntityView {
    kind: String,
    name: String,
    detail: String,
}

impl From<nervix_proto::DomainEntitySnapshot> for EntityView {
    fn from(value: nervix_proto::DomainEntitySnapshot) -> Self {
        Self {
            kind: value.kind,
            name: value.identifier,
            detail: value.detail,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum NodeKind {
    Client,
    Ingestor,
    Processor,
    Emitter,
}

impl NodeKind {
    const fn from_dataflow_kind(kind: DataflowNodeKind) -> Self {
        match kind {
            DataflowNodeKind::Client => Self::Client,
            DataflowNodeKind::Ingestor => Self::Ingestor,
            DataflowNodeKind::Emitter => Self::Emitter,
            DataflowNodeKind::Processor | DataflowNodeKind::Relay => Self::Processor,
        }
    }
}

fn format_scaled_metric(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else {
        format!("{value:.0}")
    }
}

fn graph_optional_number(value: Option<f64>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    let rendered = format!("{value:.6}");
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn format_bytes_metric(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}MB", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}kB", value / 1_000.0)
    } else {
        format!("{value:.0}B")
    }
}

fn schema_field_type_label(field: &GraphSchemaField) -> String {
    let mut parts = vec![field.ty.clone()];
    if field.optional {
        parts.push("OPTIONAL".to_string());
    }
    if field.sensitive {
        parts.push("SENSITIVE".to_string());
    }
    parts.join(" ")
}

fn append_filter_reference(filter: RwSignal<String>, reference: &str) {
    filter.update(|value| {
        if !value.trim().is_empty() && !value.ends_with(char::is_whitespace) {
            value.push(' ');
        }
        value.push_str(reference);
    });
}

#[derive(Clone, Copy)]
struct GraphDrag {
    client_x: i32,
    client_y: i32,
    pan_x: f64,
    pan_y: f64,
}

/// What the console is currently pointing at, so an item can light up the edges it touches and an
/// edge can light up the items it joins.
#[derive(Clone, PartialEq, Eq)]
enum GraphHover {
    Item(String),
    Edge(String, String, DataflowEdgeKind),
}

impl GraphHover {
    fn emphasises_item(&self, id: &str) -> bool {
        match self {
            Self::Item(hovered) => hovered == id,
            Self::Edge(source, target, _) => source == id || target == id,
        }
    }

    fn emphasises_edge(&self, edge: &GraphViewEdge) -> bool {
        match self {
            Self::Item(hovered) => *hovered == edge.source || *hovered == edge.target,
            Self::Edge(source, target, kind) => {
                *source == edge.source && *target == edge.target && *kind == edge.kind
            }
        }
    }
}

fn graph_position_style(rect: Rect) -> String {
    format!(
        "left: {}px; top: {}px; width: {}px; height: {}px;",
        rect.x, rect.y, rect.width, rect.height
    )
}

/// A rectangle reduced to the hashable tuple keyed views compare.
const fn rect_key(rect: Rect) -> (i32, i32, i32, i32) {
    (rect.x, rect.y, rect.width, rect.height)
}

#[derive(Clone)]
struct TermLine {
    kind: TermLineKind,
    text: String,
}

impl TermLine {
    fn prompt(
        text: impl Into<String>,
        transaction_state: Option<nervix_proto::TransactionState>,
    ) -> Self {
        let prompt = match transaction_state {
            Some(nervix_proto::TransactionState::Open) => "nervix[tx]>",
            Some(nervix_proto::TransactionState::Committing) => "nervix[committing]>",
            _ => "nervix>",
        };
        Self {
            kind: TermLineKind::Prompt,
            text: format!("{prompt} {}", text.into()),
        }
    }

    fn output(text: impl Into<String>) -> Self {
        Self {
            kind: TermLineKind::Output,
            text: text.into(),
        }
    }

    fn info(text: impl Into<String>) -> Self {
        Self {
            kind: TermLineKind::Info,
            text: text.into(),
        }
    }

    fn error(text: impl Into<String>) -> Self {
        Self {
            kind: TermLineKind::Error,
            text: format!("error: {}", text.into()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TermLineKind {
    Prompt,
    Output,
    Info,
    Error,
}

impl TermLineKind {
    const fn class_name(self) -> &'static str {
        match self {
            Self::Prompt => "term-line prompt",
            Self::Output => "term-line output",
            Self::Info => "term-line info",
            Self::Error => "term-line error",
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_dataflow_graph::{
        DataflowBranchStatistics, DataflowEdge, DataflowNode, DataflowProcessorKind,
    };

    use super::*;

    #[test]
    fn successful_commands_without_output_add_no_terminal_line() {
        let lines = command_result_lines(
            nervix_proto::CommandResult {
                success: true,
                kind: nervix_proto::CommandResultKind::Ok as i32,
                ..Default::default()
            },
            "CREATE DOMAIN quiet",
        );

        assert!(lines.is_empty());
    }

    #[test]
    fn transaction_reconnect_replays_only_when_replicated_progress_is_unchanged() {
        let previous = nervix_proto::TransactionStatus {
            id: "tx-1".to_string(),
            domain: "tenant".to_string(),
            state: nervix_proto::TransactionState::Open as i32,
            pending_count: 1,
            completed_count: 0,
            total_count: 1,
            error: String::new(),
            failing_step: None,
        };
        assert!(!transaction_operation_was_observed(
            Some(&previous),
            Some(&previous)
        ));

        let mut queued = previous.clone();
        queued.pending_count = 2;
        queued.total_count = 2;
        assert!(transaction_operation_was_observed(
            Some(&previous),
            Some(&queued)
        ));

        let mut committing = previous.clone();
        committing.state = nervix_proto::TransactionState::Committing as i32;
        assert!(transaction_operation_was_observed(
            Some(&previous),
            Some(&committing)
        ));
    }

    #[test]
    fn branch_group_states_the_declared_branch_key_without_parsing_names() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor("ingestor:mqtt", "mqtt", Some(site_branch())),
                relay(
                    "relay:telemetry_by_site",
                    "telemetry_by_site",
                    Some(site_branch()),
                ),
                junction("junction:route_site", "route_site", Some(site_branch())),
                emitter("emitter:redis_site", "redis_site", Some(site_branch())),
            ],
            edges: vec![
                edge("ingestor:mqtt", "relay:telemetry_by_site"),
                edge("relay:telemetry_by_site", "junction:route_site"),
                edge("junction:route_site", "emitter:redis_site"),
            ],
        });

        assert_eq!(graph.groups.len(), 1);
        let group = &graph.groups[0];
        assert_eq!(group.branch, "by_site");
        assert_eq!(group.key_schema, "site_key");
        assert_eq!(group.key_fields, vec!["site".to_string()]);
        assert_eq!(group.key_fields_data(), "site");
        assert!(!group.outline.is_empty());
        assert!(group.header.is_some());
    }

    #[test]
    fn branch_group_counts_unique_active_branches_from_group_items() {
        let region = GroupRegion {
            branch: "by_site".to_string(),
            bands: vec![Rect {
                x: 0,
                y: 0,
                width: 200,
                height: 120,
            }],
        };
        let nodes = vec![
            view_node(
                "ingestor:mqtt",
                DataflowNodeRole::Ingestor {
                    transport: "MQTT".to_string(),
                },
                Some(site_branch()),
                &["site=iad-1", "site=lhr-1"],
            ),
            view_node(
                "junction:route_site",
                DataflowNodeRole::Processor {
                    processor: DataflowProcessorKind::Junction,
                },
                Some(site_branch()),
                &["site=iad-1", "site=sfo-1"],
            ),
        ];
        let relays = vec![view_relay(
            "relay:telemetry_by_site",
            Some(site_branch()),
            &["site=ams-1"],
        )];
        let edges = vec![view_edge(
            "relay:telemetry_by_site",
            "junction:route_site",
            &["site=cdg-1"],
        )];

        let group = GraphBranchGroup::new(&region, &nodes, &relays, &edges);

        // The members and the edge between them contribute iad-1, sfo-1, ams-1 and cdg-1. The
        // ingestor constructs the branch, so it stands on the group's border and lhr-1, which
        // only it reports, is not counted.
        assert_eq!(group.active_branches, 4);
        assert_eq!(group.subtitle(), "(site) · 4 br");
    }

    #[test]
    fn branch_group_without_key_fields_reports_a_singleton_key() {
        let region = GroupRegion {
            branch: "by_tenant".to_string(),
            bands: vec![Rect {
                x: 0,
                y: 0,
                width: 100,
                height: 40,
            }],
        };
        let branch = DataflowBranch {
            name: "by_tenant".to_string(),
            key_schema: "tenant_key".to_string(),
            key_fields: Vec::new(),
        };
        let relays = vec![view_relay("relay:tenants", Some(branch), &[])];

        let group = GraphBranchGroup::new(&region, &[], &relays, &[]);

        assert_eq!(group.key_fields_data(), "");
        assert_eq!(group.subtitle(), "(singleton key) · 0 br");
    }

    #[test]
    fn node_command_kind_comes_from_the_typed_processor() {
        let reingestor = view_node(
            "reingestor:replay",
            DataflowNodeRole::Processor {
                processor: DataflowProcessorKind::Reingestor,
            },
            None,
            &[],
        );
        assert_eq!(reingestor.command_kind(), "REINGESTOR");
        assert_eq!(reingestor.kind_label(), "PROCESSOR");
        assert_eq!(reingestor.detail_label(), "REINGESTOR");

        let window = view_node(
            "window_processor:rolling",
            DataflowNodeRole::Processor {
                processor: DataflowProcessorKind::WindowProcessor,
            },
            None,
            &[],
        );
        assert_eq!(window.command_kind(), "WINDOW PROCESSOR");
        assert_eq!(
            describe_command(window.command_kind(), "rolling").as_deref(),
            Some("DESCRIBE WINDOW PROCESSOR rolling;")
        );

        let emitter = view_node(
            "emitter:redis",
            DataflowNodeRole::Emitter {
                transport: "REDIS".to_string(),
            },
            None,
            &[],
        );
        assert_eq!(emitter.command_kind(), "EMITTER");
        assert_eq!(emitter.kind_label(), "EMITTER");
        assert_eq!(emitter.detail_label(), "REDIS");
    }

    #[test]
    fn branch_group_membership_excludes_the_nodes_that_bound_the_branch() {
        let ingestor = view_node(
            "ingestor:mqtt",
            DataflowNodeRole::Ingestor {
                transport: "MQTT".to_string(),
            },
            Some(site_branch()),
            &[],
        );
        assert_eq!(ingestor.group_branch(), None);

        let junction = view_node(
            "junction:route",
            DataflowNodeRole::Processor {
                processor: DataflowProcessorKind::Junction,
            },
            Some(site_branch()),
            &[],
        );
        assert_eq!(junction.group_branch(), Some("by_site"));

        let relay = view_relay("relay:telemetry", Some(site_branch()), &[]);
        assert_eq!(relay.group_branch(), Some("by_site"));
    }

    #[test]
    fn node_geometry_comes_from_the_layout_rectangle() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "datalake_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![emitter(
                "emitter:iceberg_connected_sessions",
                "iceberg_connected_sessions",
                None,
            )],
            edges: Vec::new(),
        });

        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == "emitter:iceberg_connected_sessions")
            .expect("datalake node should be present");
        assert_eq!(node.hit_style(), graph_position_style(node.rect));
        assert!(node.rect.width > 0 && node.rect.height > 0);
        assert!(graph.canvas_width() >= node.rect.right());
        assert!(graph.canvas_height() >= node.rect.bottom());
    }

    #[test]
    fn edge_activity_badges_require_current_rate_not_only_historical_totals() {
        let historical = GraphStatistics {
            messages_per_second: 0.0,
            bytes_per_second: 0.0,
            batches_per_second: 0.0,
            messages_total: 42,
            bytes_total: 2048,
            batches_total: 3,
            relay_buffer_capacity: None,
            relay_buffer_len_p50: None,
            relay_buffer_len_p90: None,
            relay_buffer_len_p99: None,
        };

        assert!(
            !historical.has_edge_activity(),
            "stale totals should not render route metric badges"
        );
        assert!(
            GraphStatistics {
                messages_per_second: 1.0,
                ..historical
            }
            .has_edge_activity()
        );
    }

    #[test]
    fn state_links_carry_no_rate_badge() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "state_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                relay("relay:reference", "reference", None),
                DataflowNode::new(
                    "generator:ticks",
                    "ticks",
                    DataflowNodeRole::Processor {
                        processor: DataflowProcessorKind::Generator,
                    },
                ),
            ],
            edges: vec![DataflowEdge::data(
                "relay:reference",
                "generator:ticks",
                DataflowEdgeKind::StateLink,
            )],
        });

        let link = &graph.edges[0];
        assert_eq!(link.metric_style(), None);
        assert_eq!(link.kind.css_class(), "graph-edge--state-link");
        assert_eq!(link.marker(), "url(#graph-arrow-hollow)");
        assert_eq!(
            link.route_summary(),
            "relay:reference → generator:ticks: materialized state"
        );
    }

    #[test]
    fn an_edge_reports_the_routes_and_correlator_side_it_stands_for() {
        let edge = GraphViewEdge {
            input_side: Some(DataflowInputSide::Right),
            routes: 3,
            ..view_edge("relay:orders", "correlator:match", &[])
        };

        assert_eq!(edge.input_side_data(), "RIGHT");
        assert_eq!(edge.feedback_data(), "false");
        assert_eq!(edge.marker(), "url(#graph-arrow)");
        assert_eq!(
            edge.route_summary(),
            "relay:orders → correlator:match into RIGHT: records · 3 routes"
        );
    }

    #[test]
    fn a_branch_group_outline_thickens_with_its_live_branches() {
        let region = GroupRegion {
            branch: "by_site".to_string(),
            bands: vec![Rect {
                x: 0,
                y: 0,
                width: 80,
                height: 40,
            }],
        };
        let quiet = GraphBranchGroup::new(
            &region,
            &[],
            &[view_relay("relay:a", Some(site_branch()), &[])],
            &[],
        );
        let busy = GraphBranchGroup::new(
            &region,
            &[],
            &[view_relay(
                "relay:a",
                Some(site_branch()),
                &["site=iad-1", "site=lhr-1", "site=sfo-1"],
            )],
            &[],
        );

        assert!(
            busy.outline_stroke_width() > quiet.outline_stroke_width(),
            "a busier group must be drawn heavier"
        );
    }

    #[test]
    fn edge_path_rounds_its_turns_and_reports_them() {
        let edge = view_edge_with_points(
            "relay:a",
            "junction:b",
            vec![(0, 0), (100, 0), (100, 100), (200, 100)],
        );

        let path = edge.path();
        assert!(path.starts_with("M0 0"), "{path}");
        assert!(path.ends_with("L200 100"), "{path}");
        assert_eq!(
            path.matches(" Q").count(),
            2,
            "both turns must be drawn as corners: {path}"
        );
        assert!(
            path.contains("L90 0 Q100 0, 100 10"),
            "a corner between long segments uses the full radius: {path}"
        );
    }

    #[test]
    fn edge_path_halves_the_corner_radius_between_short_segments() {
        let edge = view_edge_with_points("relay:a", "junction:b", vec![(0, 0), (12, 0), (12, 40)]);

        let path = edge.path();
        assert!(
            path.contains("L7 0 Q12 0, 12 5"),
            "a short segment must not be eaten by its corner: {path}"
        );
    }

    #[test]
    fn edge_path_of_a_straight_run_has_no_corners() {
        let edge = view_edge_with_points("relay:a", "junction:b", vec![(0, 40), (180, 40)]);

        assert_eq!(edge.path(), "M0 40 L180 40");
    }

    #[test]
    fn graph_topology_key_ignores_runtime_statistics() {
        let base = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "metrics_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor(
                    "ingestor:http_notifications",
                    "http_notifications",
                    Some(user_branch()),
                ),
                relay("relay:notifications", "notifications", Some(user_branch())),
            ],
            edges: vec![edge("ingestor:http_notifications", "relay:notifications")],
        });
        let changed = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "metrics_demo".to_string(),
            statistics: DataflowStatistics {
                messages_per_second: 100.0,
                bytes_per_second: 1024.0,
                batches_per_second: 5.0,
                messages_total: 1000,
                bytes_total: 4096,
                batches_total: 12,
                relay_buffer_capacity: None,
                relay_buffer_len_p50: None,
                relay_buffer_len_p90: None,
                relay_buffer_len_p99: None,
            },
            nodes: vec![
                ingestor(
                    "ingestor:http_notifications",
                    "http_notifications",
                    Some(user_branch()),
                )
                .with_statistics(DataflowStatistics {
                    messages_per_second: 10.0,
                    messages_total: 20,
                    ..DataflowStatistics::default()
                })
                .with_branches(vec![DataflowBranchStatistics {
                    branch: r#"{"user_id":42}"#.to_string(),
                    statistics: DataflowStatistics {
                        messages_per_second: 10.0,
                        messages_total: 20,
                        ..DataflowStatistics::default()
                    },
                }]),
                relay("relay:notifications", "notifications", Some(user_branch()))
                    .with_statistics(DataflowStatistics {
                        messages_per_second: 10.0,
                        messages_total: 20,
                        relay_buffer_capacity: Some(3),
                        relay_buffer_len_p50: Some(1.0),
                        relay_buffer_len_p90: Some(2.0),
                        relay_buffer_len_p99: Some(3.0),
                        ..DataflowStatistics::default()
                    })
                    .with_branches(vec![DataflowBranchStatistics {
                        branch: r#"{"user_id":42}"#.to_string(),
                        statistics: DataflowStatistics {
                            messages_per_second: 10.0,
                            messages_total: 20,
                            ..DataflowStatistics::default()
                        },
                    }]),
            ],
            edges: vec![
                edge("ingestor:http_notifications", "relay:notifications")
                    .with_statistics(DataflowStatistics {
                        messages_per_second: 10.0,
                        bytes_per_second: 2048.0,
                        batches_per_second: 5.0,
                        messages_total: 20,
                        bytes_total: 4096,
                        batches_total: 5,
                        ..DataflowStatistics::default()
                    })
                    .with_branches(vec![DataflowBranchStatistics {
                        branch: r#"{"user_id":42}"#.to_string(),
                        statistics: DataflowStatistics {
                            messages_per_second: 10.0,
                            messages_total: 20,
                            ..DataflowStatistics::default()
                        },
                    }]),
            ],
        });

        assert!(
            base.topology_key() == changed.topology_key(),
            "runtime statistics and active branches must not force topology rerendering"
        );
    }

    #[test]
    fn graph_topology_key_changes_with_the_shape_of_the_graph() {
        let base = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "layout_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor("ingestor:http_notifications", "http_notifications", None),
                relay("relay:notifications", "notifications", None),
            ],
            edges: vec![edge("ingestor:http_notifications", "relay:notifications")],
        });
        let extended = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "layout_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor("ingestor:http_notifications", "http_notifications", None),
                relay("relay:notifications", "notifications", None),
                emitter("emitter:redis", "redis", None),
            ],
            edges: vec![
                edge("ingestor:http_notifications", "relay:notifications"),
                edge("relay:notifications", "emitter:redis"),
            ],
        });

        assert!(
            base.topology_key() != extended.topology_key(),
            "a different graph shape must rerender topology"
        );
    }

    #[test]
    fn graph_topology_key_distinguishes_correlator_input_sides_and_route_counts() {
        let left = GraphView::from_dataflow_graph(correlator_graph(DataflowInputSide::Left, 1));
        let right = GraphView::from_dataflow_graph(correlator_graph(DataflowInputSide::Right, 1));
        let collapsed =
            GraphView::from_dataflow_graph(correlator_graph(DataflowInputSide::Left, 3));

        assert!(left.topology_key() != right.topology_key());
        assert!(left.topology_key() != collapsed.topology_key());
    }

    #[test]
    fn hovering_an_item_emphasises_its_incident_edges() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "hover_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor("ingestor:mqtt", "mqtt", None),
                relay("relay:telemetry", "telemetry", None),
                emitter("emitter:redis", "redis", None),
            ],
            edges: vec![
                edge("ingestor:mqtt", "relay:telemetry"),
                edge("relay:telemetry", "emitter:redis"),
            ],
        });
        let first = graph
            .edges
            .iter()
            .find(|edge| edge.source == "ingestor:mqtt")
            .expect("the ingest edge must exist");
        let second = graph
            .edges
            .iter()
            .find(|edge| edge.target == "emitter:redis")
            .expect("the emit edge must exist");

        let hover = GraphHover::Item("ingestor:mqtt".to_string());
        assert!(hover.emphasises_item("ingestor:mqtt"));
        assert!(!hover.emphasises_item("emitter:redis"));
        assert!(hover.emphasises_edge(first));
        assert!(!hover.emphasises_edge(second));

        let hover = GraphHover::Edge(
            "relay:telemetry".to_string(),
            "emitter:redis".to_string(),
            DataflowEdgeKind::Data,
        );
        assert!(hover.emphasises_item("relay:telemetry"));
        assert!(hover.emphasises_item("emitter:redis"));
        assert!(!hover.emphasises_item("ingestor:mqtt"));
        assert!(hover.emphasises_edge(second));
        assert!(!hover.emphasises_edge(first));
    }

    #[test]
    fn search_matches_items_by_name_and_frames_them() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "search_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                ingestor("ingestor:mqtt_telemetry", "mqtt_telemetry", None),
                relay("relay:telemetry", "telemetry", None),
                emitter("emitter:redis_alerts", "redis_alerts", None),
            ],
            edges: vec![
                edge("ingestor:mqtt_telemetry", "relay:telemetry"),
                edge("relay:telemetry", "emitter:redis_alerts"),
            ],
        });

        assert_eq!(graph.search_result_count("telemetry"), 2);
        assert_eq!(graph.search_result_count("t"), 0, "one letter is too broad");
        assert!(graph.search_result_bounds("telemetry").is_some());
        assert!(graph.search_result_bounds("nothing").is_none());
    }

    #[test]
    fn domain_lifecycle_reports_the_three_states() {
        let domain = |status: &str| DomainView {
            id: "demo".to_string(),
            mode: "LIVE".to_string(),
            status: status.to_string(),
        };
        assert_eq!(domain("RUNNING").lifecycle_label(), "RUNNING");
        assert_eq!(domain("running").lifecycle_label(), "RUNNING");
        assert_eq!(domain("PAUSED").lifecycle_label(), "PAUSED");
        assert_eq!(domain("STOPPED").lifecycle_label(), "STOPPED");
        assert_eq!(domain("").lifecycle_label(), "STOPPED");
    }

    #[test]
    fn subscription_command_accepts_full_where_clause() {
        assert_eq!(
            subscribe_session_command(
                "live_notifications",
                "notifications",
                "WHERE input.user_id = 42",
                0,
            ),
            "CREATE SUBSCRIPTION live_notifications TO notifications WHERE input.user_id = 42;"
        );
    }

    #[test]
    fn subscription_command_wraps_bare_filter_as_where_clause() {
        assert_eq!(
            subscribe_session_command(
                "live_notifications",
                "notifications",
                "input.user_id = 42",
                0,
            ),
            "CREATE SUBSCRIPTION live_notifications TO notifications WHERE input.user_id = 42;"
        );
    }

    #[test]
    fn subscription_command_keeps_non_filter_syntax_inside_where_scope() {
        assert_eq!(
            subscribe_session_command(
                "live_notifications",
                "notifications",
                "SET normalized = input.user_id",
                0,
            ),
            "CREATE SUBSCRIPTION live_notifications TO notifications WHERE SET normalized = \
             input.user_id;"
        );
    }

    #[test]
    fn unsubscribe_command_uses_only_the_session_subscription_name() {
        assert_eq!(
            unsubscribe_session_command("live_notifications"),
            "DELETE SUBSCRIPTION live_notifications;"
        );
    }

    fn site_branch() -> DataflowBranch {
        DataflowBranch {
            name: "by_site".to_string(),
            key_schema: "site_key".to_string(),
            key_fields: vec!["site".to_string()],
        }
    }

    fn user_branch() -> DataflowBranch {
        DataflowBranch {
            name: "by_user".to_string(),
            key_schema: "user_key".to_string(),
            key_fields: vec!["user_id".to_string()],
        }
    }

    fn ingestor(id: &str, label: &str, branch: Option<DataflowBranch>) -> DataflowNode {
        DataflowNode::new(
            id,
            label,
            DataflowNodeRole::Ingestor {
                transport: "MQTT".to_string(),
            },
        )
        .with_branch(branch)
    }

    fn emitter(id: &str, label: &str, branch: Option<DataflowBranch>) -> DataflowNode {
        DataflowNode::new(
            id,
            label,
            DataflowNodeRole::Emitter {
                transport: "REDIS".to_string(),
            },
        )
        .with_branch(branch)
    }

    fn junction(id: &str, label: &str, branch: Option<DataflowBranch>) -> DataflowNode {
        DataflowNode::new(
            id,
            label,
            DataflowNodeRole::Processor {
                processor: DataflowProcessorKind::Junction,
            },
        )
        .with_branch(branch)
    }

    fn relay(id: &str, label: &str, branch: Option<DataflowBranch>) -> DataflowNode {
        DataflowNode::new(id, label, DataflowNodeRole::Relay).with_branch(branch)
    }

    fn edge(source: &str, target: &str) -> DataflowEdge {
        DataflowEdge::data(source, target, DataflowEdgeKind::Data)
    }

    fn correlator_graph(side: DataflowInputSide, routes: u32) -> DataflowGraph {
        DataflowGraph {
            domain: "correlation_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                relay("relay:orders", "orders", None),
                DataflowNode::new(
                    "correlator:match",
                    "match",
                    DataflowNodeRole::Processor {
                        processor: DataflowProcessorKind::Correlator,
                    },
                ),
            ],
            edges: vec![
                edge("relay:orders", "correlator:match")
                    .with_input_side(Some(side))
                    .with_routes(routes),
            ],
        }
    }

    fn view_node(
        id: &str,
        role: DataflowNodeRole,
        branch: Option<DataflowBranch>,
        branches: &[&str],
    ) -> GraphViewNode {
        GraphViewNode {
            id: id.to_string(),
            label: id.rsplit(':').next().unwrap_or(id).to_string(),
            kind: NodeKind::from_dataflow_kind(role.kind()),
            role,
            status: DataflowNodeStatus::Ok,
            status_detail: None,
            reconnect_wait_millis: None,
            rect: Rect::default(),
            branch,
            branches: branch_statistics(branches),
        }
    }

    fn view_relay(id: &str, branch: Option<DataflowBranch>, branches: &[&str]) -> GraphViewRelay {
        GraphViewRelay {
            id: id.to_string(),
            label: id.rsplit(':').next().unwrap_or(id).to_string(),
            rect: Rect::default(),
            schema: None,
            schema_fields: Vec::new(),
            branch,
            statistics: GraphStatistics::default(),
            branches: branch_statistics(branches),
        }
    }

    fn view_edge(source: &str, target: &str, branches: &[&str]) -> GraphViewEdge {
        GraphViewEdge {
            source: source.to_string(),
            target: target.to_string(),
            kind: DataflowEdgeKind::Data,
            input_side: None,
            routes: 1,
            statistics: GraphStatistics::default(),
            branches: branch_statistics(branches),
            points: Vec::new(),
            badge: None,
            feedback: false,
        }
    }

    fn view_edge_with_points(source: &str, target: &str, points: Vec<(i32, i32)>) -> GraphViewEdge {
        GraphViewEdge {
            points,
            ..view_edge(source, target, &[])
        }
    }

    fn branch_statistics(branches: &[&str]) -> Vec<GraphBranchStatistics> {
        branches
            .iter()
            .map(|branch| GraphBranchStatistics {
                branch: (*branch).to_string(),
                statistics: GraphStatistics::default(),
            })
            .collect()
    }
}
