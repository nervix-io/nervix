use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use charming::{
    Chart, WasmRenderer,
    element::{Color, Easing, Label, LabelPosition, LineStyle},
    series::{
        Graph as CharmingGraphSeries, GraphCategory, GraphData, GraphLayout,
        GraphLink as CharmingGraphLink, GraphNode as CharmingGraphNode, GraphNodeLabel,
    },
};
use futures_channel::mpsc::{UnboundedSender, unbounded};
use futures_util::{FutureExt, SinkExt, StreamExt};
use gloo_net::websocket::{
    Message as WebSocketMessage, State as WebSocketState, futures::WebSocket,
};
use leptos::{ev, mount::mount_to_body, prelude::*};
use nervix_dataflow_graph::{
    DataflowEdgeKind, DataflowGraph, DataflowNodeKind, DataflowNodeStatus, DataflowSchemaField,
    DataflowStatistics,
};
use nervix_models::Statement;
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statement, parse_client_statements, parse_use_domain,
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

const GRAPH_MIN_WIDTH: i32 = 1164;
const GRAPH_MIN_HEIGHT: i32 = 360;
const GRAPH_NODE_WIDTH: i32 = 190;
const GRAPH_NODE_HEIGHT: i32 = 82;
const GRAPH_NODE_CENTER_X: i32 = GRAPH_NODE_WIDTH / 2;
const GRAPH_NODE_CENTER_Y: i32 = GRAPH_NODE_HEIGHT / 2;
const GRAPH_EDGE_LANE_MIN_SPAN: i32 = 96;
const GRAPH_EDGE_LANE_MIN_OVERLAP: i32 = 140;
const GRAPH_EDGE_SHARED_ENDPOINT_LANE_MIN_OVERLAP: i32 = 48;
const GRAPH_EDGE_LANE_GROUP_Y: i32 = 30;
const GRAPH_EDGE_LANE_SPACING: i32 = 18;
const GRAPH_EDGE_TURN_X: i32 = 48;
const GRAPH_EDGE_TERMINAL_STRAIGHT: i32 = 72;

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
    let transaction_active = RwSignal::new(false);
    let subscription_tabs = RwSignal::new(Vec::<SubscriptionTabView>::new());
    let active_subscription_tab = RwSignal::new(None::<u64>);
    let next_subscription_tab_id = RwSignal::new(1_u64);
    let suggestions = RwSignal::new(Vec::<String>::new());
    let domain_snapshots = RwSignal::new(Vec::<DomainSnapshotView>::new());
    let resource_details = RwSignal::new(BTreeMap::<String, ResourceDetailView>::new());
    let domains_loaded = RwSignal::new(false);
    let user_selected_domain = RwSignal::new(false);
    let auth_token = RwSignal::new(web_console_auth_token_from_location());
    let auth_error = RwSignal::new(None::<String>);
    let web_console_session = use_websocket_session(
        terminal_lines,
        suggestions,
        domain_snapshots,
        active_domain,
        transaction_active,
        domains,
        resource_details,
        subscription_tabs,
        active_subscription_tab,
        domains_loaded,
        user_selected_domain,
        auth_token,
        auth_error,
    );

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
        if let Some(request_tx) = suggestion_session.request_tx.get_untracked() {
            if request_tx.unbounded_send(queued).is_err() {
                suggestions.set(Vec::new());
            }
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
                transaction_active.get_untracked(),
            ));
        });
        if command.eq_ignore_ascii_case("clear") {
            terminal_lines.set(Vec::new());
            input.set(String::new());
            return;
        }
        if let Ok(ClientStatement::ListDomains) = parse_client_statement(&command) {
            if transaction_active.get_untracked() {
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
            if let Some(request_tx) = web_console_session.request_tx.get_untracked() {
                if request_tx.unbounded_send(queued).is_err() {
                    terminal_lines.update(|lines| {
                        lines.push(TermLine::error("websocket command channel is closed"));
                    });
                }
            }
        } else if let Ok(domain) = parse_use_domain(&command) {
            if transaction_active.get_untracked() {
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
            let request_domain = command_request_domain(&command, active_domain.get_untracked());
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
                    <Sidebar active_domain=active_domain user_selected_domain=user_selected_domain domains=domains domains_loaded=domains_loaded active_graph=active_graph active_entities=active_entities resource_details=resource_details web_console_session=web_console_session.clone() run_command=run_command />
                    <section class="main-pane">
                        <GraphPanel active_domain=active_domain domain=active_graph run_command=run_command start_subscription=start_subscription />
                        <ReplPanel
                            domain=active_domain_name
                            input=input
                            terminal_lines=terminal_lines
                            transaction_active=move || transaction_active.get()
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

fn use_websocket_session(
    terminal_lines: RwSignal<Vec<TermLine>>,
    suggestions: RwSignal<Vec<String>>,
    domain_snapshots: RwSignal<Vec<DomainSnapshotView>>,
    active_domain: RwSignal<Option<String>>,
    transaction_active: RwSignal<bool>,
    domains: RwSignal<Vec<DomainView>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    domains_loaded: RwSignal<bool>,
    user_selected_domain: RwSignal<bool>,
    auth_token: RwSignal<Option<String>>,
    auth_error: RwSignal<Option<String>>,
) -> WebConsoleSession {
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
                                                        terminal_lines,
                                                        suggestions,
                                                        domain_snapshots,
                                                        active_domain,
                                                        transaction_active,
                                                        domains,
                                                        resource_details,
                                                        subscription_tabs,
                                                        active_subscription_tab,
                                                        domains_loaded,
                                                        user_selected_domain,
                                                        response,
                                                        &mut pending_requests,
                                                    ) {
                                                        SessionResponseAction::Continue => {
                                                            state.set(ConsoleConnectionState::Connected);
                                                            if resend_pending_after_connect {
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
    Reconnect(String),
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
    terminal_lines: RwSignal<Vec<TermLine>>,
    suggestions: RwSignal<Vec<String>>,
    domain_snapshots: RwSignal<Vec<DomainSnapshotView>>,
    active_domain: RwSignal<Option<String>>,
    transaction_active: RwSignal<bool>,
    domains: RwSignal<Vec<DomainView>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    domains_loaded: RwSignal<bool>,
    user_selected_domain: RwSignal<bool>,
    response: nervix_proto::SessionResponse,
    pending_requests: &mut VecDeque<PendingRequest>,
) -> SessionResponseAction {
    match response.event {
        Some(nervix_proto::session_response::Event::Result(result)) => {
            if let Some(leader_url) = leader_web_console_redirect_url(&result) {
                return SessionResponseAction::Reconnect(leader_url);
            }
            if let Some(active) = result.transaction_active {
                transaction_active.set(active);
            }
            if result_is_set_active_domain_ack(&result) {
                terminal_lines.update(|lines| lines.extend(command_result_lines(result, "")));
                return SessionResponseAction::Continue;
            }
            let pending = pending_requests.pop_front();
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
            if response.response_to_request {
                if let Some(PendingRequest::Command(_)) = pending_requests.pop_front() {
                    terminal_lines.update(|lines| {
                        lines.extend(domain_list_lines(&next_domains));
                    });
                }
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
        None => {}
    }
    SessionResponseAction::Continue
}

fn result_is_set_active_domain_ack(result: &nervix_proto::CommandResult) -> bool {
    result.success && result.message.starts_with("using domain '")
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
        lines.push(TermLine::output(result.message));
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

fn command_request_domain(command: &str, active_domain: Option<String>) -> String {
    active_domain
        .or_else(|| first_created_domain_from_query(command))
        .unwrap_or_default()
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
    normalized.starts_with("BEGIN")
        || normalized.starts_with("COMMIT")
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
                <NavHeader title="Wire Schemas" count=move || entities_for("wire_schema").len().to_string() kind="wire" open=wire_open />
                <Show when=move || wire_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for("wire_schema")
                        key=|entity| entity.name.clone()
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
                <ClusterRow label="running" value="5" />
                <ClusterRow label="nodes" value="57" />
                <ClusterRow label="relays" value="49" />
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
        upload_status.set("uploading".to_string());
        uploading.set(true);
        spawn_local(async move {
            let message = upload_resource_files(
                resource_name.clone(),
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
    auth_token: Option<&str>,
) -> String {
    let auth_query = auth_token
        .map(|token| format!("&auth={}", encode_query_component(token)))
        .unwrap_or_default();
    let path = format!(
        "/console/resources/upload?resource={}{}",
        encode_query_component(resource),
        auth_query
    );
    let Some(base_url) = base_url else {
        return path;
    };
    let Ok(mut url) = Url::parse(base_url) else {
        return path;
    };
    url.set_path("/console/resources/upload");
    url.set_query(Some(&format!(
        "resource={}{}",
        encode_query_component(resource),
        auth_query
    )));
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
fn ClusterRow(label: &'static str, value: &'static str) -> impl IntoView {
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
            <strong>{value}</strong>
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
        _ => None,
    }
}

#[component]
fn GraphPanel(
    active_domain: RwSignal<Option<String>>,
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
    let fullscreen = RwSignal::new(false);
    let graph_search = RwSignal::new(String::new());
    let graph_search_focus_key = RwSignal::new(None::<(GraphTopologyKey, String)>);
    let graph_stage_ref = NodeRef::<leptos::html::Div>::new();
    let current_graph_state = RwSignal::new(None::<GraphView>);
    let topology_graph_state = RwSignal::new(None::<GraphView>);
    let topology_key_state = RwSignal::new(None::<GraphTopologyKey>);
    Effect::new(move |_| {
        let selected_domain = active_domain.get().unwrap_or_default();
        let next_graph = domain().filter(|graph| graph.id == selected_domain);
        if let Some(graph) = &next_graph {
            let next_key = graph.topology_key();
            if topology_key_state.get_untracked().as_ref() != Some(&next_key) {
                topology_key_state.set(Some(next_key));
                topology_graph_state.set(Some(graph.clone()));
            }
        } else {
            topology_key_state.set(None);
            topology_graph_state.set(None);
        }
        current_graph_state.set(next_graph);
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
            .or_else(|| visible_graph())
    };
    let current_graph =
        move || visible_graph().expect("graph view must exist when graph is visible");
    let current_topology_graph =
        move || visible_topology_graph().expect("graph topology must exist when graph is visible");
    let active_graph_search = move || {
        let query = graph_search.get().trim().to_ascii_lowercase();
        (query.chars().count() >= 2).then_some(query)
    };
    let focus_graph_bounds = move |graph: &GraphView, bounds: GraphBounds| {
        let Some(stage) = graph_stage_ref.get() else {
            return;
        };
        let stage_width = f64::from(stage.client_width());
        let stage_height = f64::from(stage.client_height());
        if stage_width <= 1.0 || stage_height <= 1.0 {
            return;
        }
        let padding = 72.0_f64;
        let available_width = (stage_width - padding * 2.0).max(stage_width * 0.4);
        let available_height = (stage_height - padding * 2.0).max(stage_height * 0.4);
        let zoom = (available_width / bounds.width())
            .min(available_height / bounds.height())
            .clamp(0.25, 1.6);
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
    };
    let focus_graph_edge = move |source: String, target: String, kind: DataflowEdgeKind| {
        let graph = current_topology_graph();
        let Some(bounds) = graph.edge_focus_bounds(&source, &target, kind) else {
            return;
        };
        focus_graph_bounds(&graph, bounds);
    };
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
        graph_search_focus_key.set(Some(key));
        focus_graph_bounds(&graph, bounds);
    });
    view! {
        <section class="graph-panel" class:fullscreen=move || fullscreen.get()>
            <div class="graph-toolbar">
                <div class="graph-title">
                    <SidebarIcon kind="branch" />
                    <strong>"Execution Graph"</strong>
                    <span class="graph-chevron">"›"</span>
                    <span>{move || visible_graph().map(|graph| graph.id).unwrap_or_else(|| "unavailable".to_string())}</span>
                    <span class="pill warn">{move || visible_graph().map(|graph| graph.mode).unwrap_or_else(|| "NO GRAPH".to_string())}</span>
                    <span class="pill waiting"><i></i>{move || visible_graph().map(|graph| graph.status).unwrap_or_else(|| "ERROR".to_string())}</span>
                </div>
                <div class="graph-actions">
                    <span>{move || visible_graph().map(|graph| graph.uptime).unwrap_or_default()}</span>
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
                            on:click=move |_| graph_zoom.update(|zoom| *zoom = (*zoom - 0.1).max(0.7))
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
                            on:click=move |_| graph_zoom.update(|zoom| *zoom = (*zoom + 0.1).min(1.6))
                        >
                            <SidebarIcon kind="zoom-in" />
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
                                *zoom = (*zoom - event.delta_y() * 0.001).clamp(0.25, 3.0);
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
                    on:mouseleave=move |_| graph_drag.set(None)
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
                        <CharmingGraph domain=current_topology_graph />
                        <svg
                            class="graph-branch-layer"
                            viewBox=move || {
                                let graph = current_topology_graph();
                                format!("0 0 {} {}", graph.canvas_width(), graph.canvas_height())
                            }
                            aria-hidden="true"
                            focusable="false"
                        >
                            <For each={move || current_graph().branching_groups()} key=|group| group.id.clone() children={move |group| {
                                let callouts = group.callout_paths();
                                view! {
                                    <g class="graph-branch-group">
                                        <rect
                                            class="graph-branch-stack graph-branch-stack-back"
                                            x=group.stack_x(2)
                                            y=group.stack_y(2)
                                            width=group.width
                                            height=group.height
                                            rx="8"
                                            ry="8"
                                        />
                                        <rect
                                            class="graph-branch-stack graph-branch-stack-mid"
                                            x=group.stack_x(1)
                                            y=group.stack_y(1)
                                            width=group.width
                                            height=group.height
                                            rx="8"
                                            ry="8"
                                        />
                                        <rect
                                            class="graph-branch-body"
                                            x=group.x
                                            y=group.y
                                            width=group.width
                                            height=group.height
                                            rx="8"
                                            ry="8"
                                            data-schema=group.schema.clone()
                                            data-x=group.x.to_string()
                                            data-y=group.y.to_string()
                                            data-width=group.width.to_string()
                                            data-height=group.height.to_string()
                                            data-left-callouts=group.initiators.len().to_string()
                                            data-right-callouts=group.finalizers.len().to_string()
                                        />
                                        <For each=move || callouts.clone() key=|path| path.clone() children=|path| {
                                            view! { <path class="graph-branch-callout" d=path /> }
                                        } />
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
                            </defs>
                            <For each={move || current_topology_graph().edges.clone()} key=move |edge| {
                                let graph = current_topology_graph();
                                (
                                    edge.source.clone(),
                                    edge.target.clone(),
                                    edge.kind,
                                    edge.path(&graph),
                                )
                            } children={move |edge| {
                                let path = edge.path(&current_topology_graph());
                                let source = edge.source.clone();
                                let target = edge.target.clone();
                                let kind = edge.kind;
                                let kind_label = kind.as_ref().to_string();
                                let class = format!("graph-edge {}", kind.css_class());
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
                                view! {
                                    <g class="graph-edge-group">
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
                                            marker-end="url(#graph-arrow)"
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
                            <For each={move || current_graph().branching_groups()} key=|group| (group.id.clone(), group.active_branches) children={move |group| {
                                view! {
                                    <BranchHeader group=group selected_branch_group=selected_branch_group />
                                }
                            }} />
                        </div>
                        <div class="graph-hit-layer" aria-label="Execution graph interactions">
                            <For each={move || current_graph().relays.clone()} key=|relay| {
                                (
                                    relay.id.clone(),
                                    relay.x,
                                    relay.y,
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
                            view! {
                                <button
                                    type="button"
                                    class="relay-hit"
                                    class:search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| relay_search_class.matches_search(&query))
                                    }
                                    style=relay.hit_style()
                                    title=relay_title
                                    data-label=relay.label.clone()
                                    data-search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| relay_search_data.matches_search(&query))
                                            .to_string()
                                    }
                                    data-buffer-capacity=buffer_capacity
                                    data-buffer-p50=buffer_p50
                                    data-buffer-p90=buffer_p90
                                    data-buffer-p99=buffer_p99
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
                                let style = current_topology_graph()
                                    .edge_metric_style(&source, &target, edge.kind)
                                    .unwrap_or_else(|| {
                                        graph_position_style(edge.x1, edge.y1, 68, 16)
                                    });
                                let messages_rate = edge.statistics.messages_rate();
                                let has_activity = edge.statistics.has_edge_activity();
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
                                    node.x,
                                    node.y,
                                    node.label.clone(),
                                    node.subtype.clone(),
                                    node.status,
                                    node.status_detail.clone(),
                                    node.reconnect_wait_millis,
                                )
                            } children={move |node| {
                            let class_node = node.clone();
                            let click_node = node.clone();
                            let subtype = node.subtype.clone();
                            let label = node.label.clone();
                            let branch_summary = node.branch_summary();
                            let node_search_class = node.clone();
                            let node_search_data = node.clone();
                            view! {
                                <button
                                    type="button"
                                    class=move || class_node.hit_class()
                                    class:search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| node_search_class.matches_search(&query))
                                    }
                                    style=node.hit_style()
                                    title=branch_summary
                                    data-status=node.status_label()
                                    data-label=node.label.clone()
                                    data-search-highlight=move || {
                                        active_graph_search()
                                            .is_some_and(|query| node_search_data.matches_search(&query))
                                            .to_string()
                                    }
                                    data-status-detail=node.status_detail.clone().unwrap_or_default()
                                    data-reconnect-wait-ms=node.reconnect_wait_millis.map(|value| value.to_string()).unwrap_or_default()
                                    on:click=move |_| {
                                        if !graph_moved.get() {
                                            selected_action_target.set(Some(GraphActionTarget::node(&click_node)));
                                        }
                                    }
                                >
                                    <span class="node-accent"></span>
                                    <span class="node-hit-type">{subtype}</span>
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
    let group_id = group.id.clone();
    let schema = group.schema.clone();
    let subtitle = group.subtitle();
    view! {
        <button
            type="button"
            class="graph-branch-header"
            style=group.label_style()
            on:mousedown=move |event: ev::MouseEvent| event.stop_propagation()
            on:click=move |_| selected_branch_group.set(Some(group_id.clone()))
        >
            <strong>{schema}</strong>
            <span>{subtitle}</span>
        </button>
    }
}

#[component]
fn BranchDetailsDialog(
    domain: impl Fn() -> GraphView + Copy + Send + 'static,
    selected_branch_group: RwSignal<Option<String>>,
) -> impl IntoView {
    let selected_group = move || {
        let selected_id = selected_branch_group.get()?;
        domain()
            .branching_groups()
            .into_iter()
            .find(|group| group.id == selected_id)
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
                    <strong>{move || selected_group().map(|group| group.schema).unwrap_or_default()}</strong>
                </header>
                <div class="subscribe-block">
                    <p>"BRANCH KEY"</p>
                    <div class="schema-row">
                        <span>"schema"</span>
                        <em>{move || selected_group().map(|group| group.schema).unwrap_or_default()}</em>
                    </div>
                    <For
                        each=move || selected_group()
                            .map(|group| group.key_fields())
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
fn CharmingGraph(domain: impl Fn() -> GraphView + Copy + Send + 'static) -> impl IntoView {
    Effect::new(move || {
        let chart = build_graph_chart(domain());
        increment_charming_graph_render_count();
        if let Err(error) =
            WasmRenderer::new_opt(None, None).render("execution-graph-chart", &chart)
        {
            leptos::logging::error!("failed to render execution graph: {error}");
        }
    });

    view! {
        <div
            id="execution-graph-chart"
            class="charming-graph"
            role="img"
            aria-label="Execution graph preview"
            data-render-count="0"
        ></div>
    }
}

fn increment_charming_graph_render_count() {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        return;
    };
    let Some(element) = document.get_element_by_id("execution-graph-chart") else {
        return;
    };
    let next_count = element
        .get_attribute("data-render-count")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
        .saturating_add(1);
    if let Err(error) = element.set_attribute("data-render-count", &next_count.to_string()) {
        leptos::logging::error!("failed to update execution graph render count: {error:?}");
    }
}

fn build_graph_chart(domain: GraphView) -> Chart {
    let width = f64::from(domain.canvas_width());
    let height = f64::from(domain.canvas_height());
    let mut nodes = vec![
        chart_anchor("__viewport_min", 0.0, 0.0),
        chart_anchor("__viewport_max", width, height),
    ];
    nodes.extend(domain.nodes.iter().map(GraphViewNode::chart_node));
    nodes.extend(domain.relays.iter().map(GraphViewRelay::chart_node));

    let links = domain
        .edges
        .iter()
        .map(|edge| CharmingGraphLink {
            source: edge.source.to_string(),
            target: edge.target.to_string(),
            value: Some(1.0),
        })
        .collect::<Vec<_>>();

    Chart::new()
        .animation(true)
        .animation_duration(650)
        .animation_easing(Easing::CubicOut)
        .animation_duration_update(450)
        .color(vec![
            Color::Value("#06b6d4".to_string()),
            Color::Value("#885cf6".to_string()),
            Color::Value("#f97316".to_string()),
            Color::Value("#38bdf8".to_string()),
        ])
        .background_color(Color::Value("transparent".to_string()))
        .series(
            CharmingGraphSeries::new()
                .name(domain.id)
                .layout(GraphLayout::None)
                .roam(false)
                .edge_symbol(Some(("none".to_string(), "arrow".to_string())))
                .label(
                    Label::new()
                        .show(false)
                        .color(Color::Value("#c5cef0".to_string()))
                        .font_size(10.0)
                        .position(LabelPosition::Bottom),
                )
                .line_style(
                    LineStyle::new()
                        .color(Color::Value("rgba(6, 182, 212, 0)".to_string()))
                        .width(0.0)
                        .opacity(0.0)
                        .curveness(0.18),
                )
                .data(GraphData {
                    nodes,
                    links,
                    categories: vec![
                        GraphCategory {
                            name: "Client".to_string(),
                        },
                        GraphCategory {
                            name: "Ingestor".to_string(),
                        },
                        GraphCategory {
                            name: "Processor".to_string(),
                        },
                        GraphCategory {
                            name: "Emitter".to_string(),
                        },
                        GraphCategory {
                            name: "Relay".to_string(),
                        },
                        GraphCategory {
                            name: "Viewport".to_string(),
                        },
                    ],
                }),
        )
}

fn chart_anchor(id: &str, x: f64, y: f64) -> CharmingGraphNode {
    CharmingGraphNode {
        id: id.to_string(),
        name: String::new(),
        x,
        y,
        value: 0.0,
        category: 5,
        symbol_size: 0.0,
        label: Some(GraphNodeLabel::new().show(false)),
    }
}

#[component]
fn ReplPanel(
    domain: impl Fn() -> String + Copy + Send + 'static,
    input: RwSignal<String>,
    terminal_lines: RwSignal<Vec<TermLine>>,
    transaction_active: impl Fn() -> bool + Copy + Send + 'static,
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
                    if transaction_active() {
                        format!("nervix[{} tx]>", domain())
                    } else {
                        format!("nervix[{}]>", domain())
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
    mode: String,
    status: String,
    uptime: String,
    statistics: GraphStatistics,
    nodes: Vec<GraphViewNode>,
    relays: Vec<GraphViewRelay>,
    edges: Vec<GraphViewEdge>,
}

impl GraphView {
    fn from_dataflow_graph(graph: DataflowGraph) -> Self {
        let mut nodes = Vec::new();
        let mut relays = Vec::new();
        for node in graph.nodes {
            match node.kind {
                DataflowNodeKind::Relay => relays.push(GraphViewRelay {
                    id: node.id,
                    label: node.label,
                    x: node.x + GRAPH_NODE_CENTER_X,
                    y: node.y + GRAPH_NODE_CENTER_Y,
                    schema: node.schema,
                    schema_fields: node
                        .schema_fields
                        .into_iter()
                        .map(GraphSchemaField::from)
                        .collect(),
                    branching_schema: node.branching_schema,
                    statistics: GraphStatistics::from(node.statistics),
                    branches: node
                        .branches
                        .into_iter()
                        .map(|branch| GraphBranchStatistics {
                            branch: branch.branch,
                            statistics: GraphStatistics::from(branch.statistics),
                        })
                        .collect(),
                }),
                kind => nodes.push(GraphViewNode {
                    id: node.id,
                    label: node.label,
                    kind: NodeKind::from_dataflow_kind(kind),
                    subtype: node.subtype,
                    status: node.status,
                    status_detail: node.status_detail,
                    reconnect_wait_millis: node.reconnect_wait_millis,
                    x: node.x,
                    y: node.y,
                    branching_schema: node.branching_schema,
                    branches: node
                        .branches
                        .into_iter()
                        .map(|branch| GraphBranchStatistics {
                            branch: branch.branch,
                            statistics: GraphStatistics::from(branch.statistics),
                        })
                        .collect(),
                }),
            }
        }

        Self {
            id: graph.domain,
            mode: "LIVE".to_string(),
            status: "RUNNING".to_string(),
            uptime: String::new(),
            statistics: GraphStatistics::from(graph.statistics),
            nodes,
            relays,
            edges: graph
                .edges
                .into_iter()
                .map(|edge| GraphViewEdge {
                    source: edge.source,
                    target: edge.target,
                    kind: edge.kind,
                    statistics: GraphStatistics::from(edge.statistics),
                    branches: edge
                        .branches
                        .into_iter()
                        .map(|branch| GraphBranchStatistics {
                            branch: branch.branch,
                            statistics: GraphStatistics::from(branch.statistics),
                        })
                        .collect(),
                    x1: 0,
                    y1: 0,
                    x2: 0,
                    y2: 0,
                })
                .collect(),
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

    fn edge_metric_style(
        &self,
        source: &str,
        target: &str,
        kind: DataflowEdgeKind,
    ) -> Option<String> {
        self.edges
            .iter()
            .find(|edge| edge.source == source && edge.target == target && edge.kind == kind)
            .map(|edge| edge.metric_style(self))
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
        let ((x1, y1), (x2, y2)) = edge.endpoints(self);
        let mut bounds = self
            .graph_item_bounds(&edge.source)
            .unwrap_or_else(|| GraphBounds::from_point(x1, y1));
        bounds.include_bounds(
            self.graph_item_bounds(&edge.target)
                .unwrap_or_else(|| GraphBounds::from_point(x2, y2)),
        );
        let start = GraphRoutePoint::new(x1, y1);
        let end = GraphRoutePoint::new(x2, y2);
        let obstacles = self.edge_obstacles(&edge.source, &edge.target);
        let preferred_lane = self.edge_preferred_lane(edge);
        if should_route_with_direct_curve(preferred_lane, start, end)
            && let Some(curve) = direct_curve(start, end, &obstacles)
        {
            for index in 0..=12 {
                let (x, y) = curve_point(curve, f64::from(index) / 12.0);
                bounds.include_point(x, y);
            }
        } else {
            for point in edge.route_points_with_lane(self, preferred_lane) {
                bounds.include_point(f64::from(point.x), f64::from(point.y));
            }
        }
        Some(bounds)
    }

    fn search_result_bounds(&self, query: &str) -> Option<GraphBounds> {
        let mut bounds = None::<GraphBounds>;
        for node in self.nodes.iter().filter(|node| node.matches_search(query)) {
            let node_bounds = GraphBounds::from_rect(
                node.x,
                node.y,
                node.x + GRAPH_NODE_WIDTH,
                node.y + GRAPH_NODE_HEIGHT,
            );
            if let Some(bounds) = &mut bounds {
                bounds.include_bounds(node_bounds);
            } else {
                bounds = Some(node_bounds);
            }
        }
        for relay in self
            .relays
            .iter()
            .filter(|relay| relay.matches_search(query))
        {
            let width = relay.width();
            let relay_bounds = GraphBounds::from_rect(
                relay.x - width / 2,
                relay.y - 10,
                relay.x + width / 2,
                relay.y + 10,
            );
            if let Some(bounds) = &mut bounds {
                bounds.include_bounds(relay_bounds);
            } else {
                bounds = Some(relay_bounds);
            }
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

    fn graph_item_bounds(&self, id: &str) -> Option<GraphBounds> {
        if let Some(node) = self
            .nodes
            .iter()
            .find(|node| Self::graph_item_matches(&node.id, &node.label, id))
        {
            return Some(GraphBounds::from_rect(
                node.x,
                node.y,
                node.x + GRAPH_NODE_WIDTH,
                node.y + GRAPH_NODE_HEIGHT,
            ));
        }
        if let Some(relay) = self
            .relays
            .iter()
            .find(|relay| Self::graph_item_matches(&relay.id, &relay.label, id))
        {
            let width = relay.width();
            return Some(GraphBounds::from_rect(
                relay.x - width / 2,
                relay.y - 10,
                relay.x + width / 2,
                relay.y + 10,
            ));
        }
        None
    }

    fn graph_item_matches(candidate_id: &str, candidate_label: &str, requested: &str) -> bool {
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

    fn graph_endpoint(&self, id: &str, side: EndpointSide) -> Option<(i32, i32)> {
        if let Some(node) = self.nodes.iter().find(|node| node.id == id) {
            let y = node.y + GRAPH_NODE_CENTER_Y;
            return Some(match side {
                EndpointSide::Outgoing => (node.x + GRAPH_NODE_WIDTH, y),
                EndpointSide::Incoming => (node.x, y),
            });
        }
        if let Some(relay) = self.relays.iter().find(|relay| relay.id == id) {
            let half_width = relay.width() / 2;
            return Some(match side {
                EndpointSide::Outgoing => (relay.x + half_width, relay.y),
                EndpointSide::Incoming => (relay.x - half_width, relay.y),
            });
        }
        None
    }

    fn edge_obstacles(&self, source: &str, target: &str) -> Vec<GraphRouteRect> {
        let mut obstacles = self
            .nodes
            .iter()
            .filter(|node| node.id != source && node.id != target)
            .map(GraphRouteRect::from_node)
            .chain(
                self.relays
                    .iter()
                    .filter(|relay| relay.id != source && relay.id != target)
                    .map(GraphRouteRect::from_relay),
            )
            .collect::<Vec<_>>();
        obstacles.extend(
            self.branching_groups()
                .into_iter()
                .filter(|group| group.is_obstacle_for_edge(source, target))
                .map(|group| GraphRouteRect::from_branch_group(&group)),
        );
        obstacles
    }

    fn edge_preferred_lane(&self, edge: &GraphViewEdge) -> Option<i32> {
        let candidate = GraphEdgeLaneCandidate::from_edge(self, edge)?;
        let mut peers = self
            .edges
            .iter()
            .filter_map(|peer| GraphEdgeLaneCandidate::from_edge(self, peer))
            .filter(|peer| peer.overlaps_lane_group(candidate))
            .collect::<Vec<_>>();
        if peers.len() <= 1 {
            return None;
        }
        peers.sort_by(|left, right| {
            left.base_y
                .cmp(&right.base_y)
                .then_with(|| left.source.cmp(&right.source))
                .then_with(|| left.target.cmp(&right.target))
                .then_with(|| left.kind.cmp(&right.kind))
        });
        let index = peers
            .iter()
            .position(|peer| peer.same_edge(candidate))
            .expect("candidate edge should be present in its peer group");
        let center_offset = (peers.len().saturating_sub(1) as i32 * GRAPH_EDGE_LANE_SPACING) / 2;
        let lane = candidate.base_y + index as i32 * GRAPH_EDGE_LANE_SPACING - center_offset;
        Some(lane.clamp(12, self.canvas_height().saturating_sub(12)))
    }

    fn canvas_width(&self) -> i32 {
        let node_right = self
            .nodes
            .iter()
            .map(|node| node.x + GRAPH_NODE_WIDTH)
            .max();
        let relay_right = self
            .relays
            .iter()
            .map(|relay| relay.x + relay.width() / 2)
            .max();
        node_right
            .into_iter()
            .chain(relay_right)
            .max()
            .unwrap_or(GRAPH_MIN_WIDTH - 48)
            .saturating_add(48)
            .max(GRAPH_MIN_WIDTH)
    }

    fn canvas_height(&self) -> i32 {
        let node_bottom = self
            .nodes
            .iter()
            .map(|node| node.y + GRAPH_NODE_HEIGHT)
            .max();
        let relay_bottom = self.relays.iter().map(|relay| relay.y + 11).max();
        node_bottom
            .into_iter()
            .chain(relay_bottom)
            .max()
            .unwrap_or(GRAPH_MIN_HEIGHT - 32)
            .saturating_add(32)
            .max(GRAPH_MIN_HEIGHT)
    }

    fn branching_groups(&self) -> Vec<GraphBranchGroup> {
        let mut adjacency = BTreeMap::<&str, Vec<&str>>::new();
        for edge in &self.edges {
            adjacency
                .entry(edge.source.as_str())
                .or_default()
                .push(edge.target.as_str());
        }

        let node_by_id = self
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node))
            .collect::<BTreeMap<_, _>>();
        let relay_by_id = self
            .relays
            .iter()
            .map(|relay| (relay.id.as_str(), relay))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = Vec::<GraphBranchGroupCandidate>::new();
        for start in self.nodes.iter().filter(|node| node.starts_branch_group()) {
            let start_relays = adjacency
                .get(start.id.as_str())
                .into_iter()
                .flatten()
                .filter_map(|id| relay_by_id.get(id).copied())
                .filter_map(|relay| {
                    relay
                        .branching_schema
                        .as_ref()
                        .map(|schema| (relay, schema))
                });
            for (start_relay, schema) in start_relays {
                let mut members = BTreeSet::<String>::new();
                let mut finalizers = Vec::<GraphAnchor>::new();
                let mut metric_node_ids = BTreeSet::<String>::from([start.id.clone()]);
                let mut boundary_nodes = BTreeSet::<String>::from([start.id.clone()]);
                let mut visited = BTreeSet::<String>::new();
                let mut pending = VecDeque::from([start_relay.id.as_str()]);
                while let Some(id) = pending.pop_front() {
                    if !visited.insert(id.to_string()) {
                        continue;
                    }
                    if let Some(relay) = relay_by_id.get(id) {
                        if relay.branching_schema.as_ref() != Some(schema) {
                            continue;
                        }
                        metric_node_ids.insert(relay.id.clone());
                        members.insert(relay.id.clone());
                        pending.extend(adjacency.get(id).into_iter().flatten().copied());
                        continue;
                    }
                    let Some(node) = node_by_id.get(id) else {
                        continue;
                    };
                    if node.ends_branch_group() {
                        boundary_nodes.insert(node.id.clone());
                        finalizers.push(GraphAnchor::incoming_node(node));
                        continue;
                    }
                    metric_node_ids.insert(node.id.clone());
                    members.insert(node.id.clone());
                    pending.extend(adjacency.get(id).into_iter().flatten().copied());
                }
                candidates.push(GraphBranchGroupCandidate {
                    schema: schema.clone(),
                    start_id: start.id.clone(),
                    members,
                    metric_node_ids,
                    boundary_nodes,
                    initiator: GraphAnchor::outgoing_node(start),
                    finalizers,
                });
            }
        }
        GraphBranchGroupCandidate::merge(candidates)
            .into_iter()
            .filter_map(|candidate| {
                GraphBranchGroup::from_members(
                    &candidate.schema,
                    &candidate.start_id,
                    &candidate.members,
                    &candidate.metric_node_ids,
                    &candidate.boundary_nodes,
                    &candidate.initiators,
                    &candidate.finalizers,
                    self,
                )
            })
            .collect()
    }

    fn active_branch_count(&self, node_ids: &BTreeSet<String>) -> usize {
        self.nodes
            .iter()
            .filter(|node| node_ids.contains(&node.id))
            .flat_map(|node| node.branches.iter().map(|branch| branch.branch.clone()))
            .chain(
                self.relays
                    .iter()
                    .filter(|relay| node_ids.contains(&relay.id))
                    .flat_map(|relay| relay.branches.iter().map(|branch| branch.branch.clone())),
            )
            .chain(
                self.edges
                    .iter()
                    .filter(|edge| {
                        node_ids.contains(&edge.source) || node_ids.contains(&edge.target)
                    })
                    .flat_map(|edge| edge.branches.iter().map(|branch| branch.branch.clone())),
            )
            .collect::<BTreeSet<_>>()
            .len()
    }
}

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
    kind: NodeKind,
    subtype: String,
    x: i32,
    y: i32,
    branching_schema: Option<String>,
}

impl From<&GraphViewNode> for GraphNodeTopologyKey {
    fn from(node: &GraphViewNode) -> Self {
        Self {
            id: node.id.clone(),
            label: node.label.clone(),
            kind: node.kind,
            subtype: node.subtype.clone(),
            x: node.x,
            y: node.y,
            branching_schema: node.branching_schema.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphRelayTopologyKey {
    id: String,
    label: String,
    x: i32,
    y: i32,
    schema: Option<String>,
    schema_fields: Vec<GraphSchemaFieldTopologyKey>,
    branching_schema: Option<String>,
}

impl From<&GraphViewRelay> for GraphRelayTopologyKey {
    fn from(relay: &GraphViewRelay) -> Self {
        Self {
            id: relay.id.clone(),
            label: relay.label.clone(),
            x: relay.x,
            y: relay.y,
            schema: relay.schema.clone(),
            schema_fields: relay
                .schema_fields
                .iter()
                .map(GraphSchemaFieldTopologyKey::from)
                .collect(),
            branching_schema: relay.branching_schema.clone(),
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
}

impl From<&GraphViewEdge> for GraphEdgeTopologyKey {
    fn from(edge: &GraphViewEdge) -> Self {
        Self {
            source: edge.source.clone(),
            target: edge.target.clone(),
            kind: edge.kind,
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
    subtype: String,
    status: DataflowNodeStatus,
    status_detail: Option<String>,
    reconnect_wait_millis: Option<u64>,
    x: i32,
    y: i32,
    branching_schema: Option<String>,
    branches: Vec<GraphBranchStatistics>,
}

impl GraphViewNode {
    fn chart_node(&self) -> CharmingGraphNode {
        CharmingGraphNode {
            id: self.id.clone(),
            name: self.label.clone(),
            x: f64::from(self.x + GRAPH_NODE_CENTER_X),
            y: f64::from(self.y + GRAPH_NODE_CENTER_Y),
            value: 1.0,
            category: self.kind.category_index(),
            symbol_size: 0.0,
            label: Some(GraphNodeLabel::new().show(false)),
        }
    }

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
        graph_position_style(self.x, self.y, GRAPH_NODE_WIDTH, GRAPH_NODE_HEIGHT)
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

    fn starts_branch_group(&self) -> bool {
        self.kind == NodeKind::Ingestor || self.is_reingestor()
    }

    fn ends_branch_group(&self) -> bool {
        self.kind == NodeKind::Emitter || self.is_reingestor()
    }

    fn is_reingestor(&self) -> bool {
        self.subtype.eq_ignore_ascii_case("reingestor")
    }

    fn command_kind(&self) -> &'static str {
        match self.kind {
            NodeKind::Client => "CLIENT",
            NodeKind::Ingestor => "INGESTOR",
            NodeKind::Emitter => "EMITTER",
            NodeKind::Processor => match self.subtype.to_ascii_lowercase().as_str() {
                "deduplicator" => "DEDUPLICATOR",
                "correlator" => "CORRELATOR",
                "generator" => "GENERATOR",
                "inferencer" => "INFERENCER",
                "reingestor" => "REINGESTOR",
                "reorderer" => "REORDERER",
                "junction" => "JUNCTION",
                "wasm_processor" | "wasm processor" => "WASM PROCESSOR",
                "window_processor" | "window processor" => "WINDOW PROCESSOR",
                _ => "PROCESSOR",
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
    x: i32,
    y: i32,
    schema: Option<String>,
    schema_fields: Vec<GraphSchemaField>,
    branching_schema: Option<String>,
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
    fn chart_node(&self) -> CharmingGraphNode {
        CharmingGraphNode {
            id: self.id.clone(),
            name: self.label.clone(),
            x: f64::from(self.x),
            y: f64::from(self.y),
            value: 1.0,
            category: 3,
            symbol_size: 0.0,
            label: Some(GraphNodeLabel::new().show(false)),
        }
    }

    fn hit_style(&self) -> String {
        let width = self.width();
        format!(
            "{} --relay-buffer-p50: {:.2}%; --relay-buffer-p90: {:.2}%; --relay-buffer-p99: \
             {:.2}%;",
            graph_position_style(self.x - width / 2, self.y - 10, width, 20),
            self.buffer_percent(self.statistics.relay_buffer_len_p50),
            self.buffer_percent(self.statistics.relay_buffer_len_p90),
            self.buffer_percent(self.statistics.relay_buffer_len_p99),
        )
    }

    fn width(&self) -> i32 {
        (self.label.len() as i32 * 7 + 28).max(62)
    }

    fn matches_search(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        query.chars().count() >= 2
            && (self.id.to_ascii_lowercase().contains(&query)
                || self.label.to_ascii_lowercase().contains(&query))
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

#[derive(Clone)]
struct GraphBranchGroup {
    id: String,
    schema: String,
    members: BTreeSet<String>,
    boundary_nodes: BTreeSet<String>,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
    active_branches: usize,
    initiators: Vec<GraphAnchor>,
    finalizers: Vec<GraphAnchor>,
}

struct GraphBranchGroupCandidate {
    schema: String,
    start_id: String,
    members: BTreeSet<String>,
    metric_node_ids: BTreeSet<String>,
    boundary_nodes: BTreeSet<String>,
    initiator: GraphAnchor,
    finalizers: Vec<GraphAnchor>,
}

struct MergedGraphBranchGroupCandidate {
    schema: String,
    start_id: String,
    members: BTreeSet<String>,
    metric_node_ids: BTreeSet<String>,
    boundary_nodes: BTreeSet<String>,
    initiators: Vec<GraphAnchor>,
    finalizers: Vec<GraphAnchor>,
}

impl GraphBranchGroupCandidate {
    fn merge(candidates: Vec<GraphBranchGroupCandidate>) -> Vec<MergedGraphBranchGroupCandidate> {
        let mut merged = Vec::<MergedGraphBranchGroupCandidate>::new();
        for candidate in candidates {
            let Some(entry) = merged.iter_mut().find(|entry| {
                entry.schema == candidate.schema
                    && entry
                        .members
                        .iter()
                        .any(|member| candidate.members.contains(member))
            }) else {
                merged.push(MergedGraphBranchGroupCandidate {
                    schema: candidate.schema,
                    start_id: candidate.start_id,
                    members: candidate.members,
                    metric_node_ids: candidate.metric_node_ids,
                    boundary_nodes: candidate.boundary_nodes,
                    initiators: vec![candidate.initiator],
                    finalizers: candidate.finalizers,
                });
                continue;
            };
            entry.members.extend(candidate.members);
            entry.metric_node_ids.extend(candidate.metric_node_ids);
            entry.boundary_nodes.extend(candidate.boundary_nodes);
            entry.initiators.push(candidate.initiator);
            entry.finalizers.extend(candidate.finalizers);
        }
        let mut index = 0;
        while index < merged.len() {
            let mut other = index + 1;
            while other < merged.len() {
                if merged[index].schema == merged[other].schema
                    && merged[index]
                        .members
                        .iter()
                        .any(|member| merged[other].members.contains(member))
                {
                    let removed = merged.remove(other);
                    merged[index].members.extend(removed.members);
                    merged[index]
                        .metric_node_ids
                        .extend(removed.metric_node_ids);
                    merged[index].boundary_nodes.extend(removed.boundary_nodes);
                    merged[index].initiators.extend(removed.initiators);
                    merged[index].finalizers.extend(removed.finalizers);
                } else {
                    other += 1;
                }
            }
            index += 1;
        }
        merged
    }
}

impl GraphBranchGroup {
    fn is_obstacle_for_edge(&self, source: &str, target: &str) -> bool {
        let source_member = self.members.contains(source);
        let target_member = self.members.contains(target);
        let source_boundary = self.boundary_nodes.contains(source);
        let target_boundary = self.boundary_nodes.contains(target);

        !(source_member || target_member || (source_boundary && target_boundary))
    }

    fn from_members(
        schema: &str,
        start_id: &str,
        members: &BTreeSet<String>,
        metric_node_ids: &BTreeSet<String>,
        boundary_nodes: &BTreeSet<String>,
        initiators: &[GraphAnchor],
        finalizers: &[GraphAnchor],
        graph: &GraphView,
    ) -> Option<Self> {
        let mut left = i32::MAX;
        let mut top = i32::MAX;
        let mut right = i32::MIN;
        let mut bottom = i32::MIN;

        for node in graph.nodes.iter().filter(|node| members.contains(&node.id)) {
            left = left.min(node.x);
            top = top.min(node.y);
            right = right.max(node.x + GRAPH_NODE_WIDTH);
            bottom = bottom.max(node.y + GRAPH_NODE_HEIGHT);
        }
        for relay in graph
            .relays
            .iter()
            .filter(|relay| members.contains(&relay.id))
        {
            let width = relay.width();
            left = left.min(relay.x - width / 2);
            top = top.min(relay.y - 10);
            right = right.max(relay.x + width / 2);
            bottom = bottom.max(relay.y + 10);
        }

        if left == i32::MAX {
            for anchor in initiators.iter().chain(finalizers) {
                left = left.min(anchor.x);
                top = top.min(anchor.y);
                right = right.max(anchor.x);
                bottom = bottom.max(anchor.y);
            }
            if left == i32::MAX {
                return None;
            }
        }

        let padding_x = 18;
        let padding_top = 54;
        let padding_bottom = 18;
        let anchor_gap = Self::CALLOUT_CLEARANCE;
        let anchor_padding_y = Self::CALLOUT_HALF_HEIGHT + 6;
        let mut box_left = left - padding_x;
        let mut box_top = top - padding_top;
        let mut box_right = right + padding_x;
        let mut box_bottom = bottom + padding_bottom;

        for anchor in initiators {
            box_left = box_left.min(anchor.x + anchor_gap);
            box_top = box_top.min(anchor.y - anchor_padding_y);
            box_bottom = box_bottom.max(anchor.y + anchor_padding_y);
        }
        for anchor in finalizers {
            box_right = box_right.max(anchor.x - anchor_gap);
            box_top = box_top.min(anchor.y - anchor_padding_y);
            box_bottom = box_bottom.max(anchor.y + anchor_padding_y);
        }

        box_left = box_left.max(2);
        box_top = box_top.max(0);
        Some(Self {
            id: format!("{schema}:{start_id}"),
            schema: schema.to_string(),
            members: members.clone(),
            boundary_nodes: boundary_nodes.clone(),
            x: box_left,
            y: box_top,
            width: box_right - box_left,
            height: box_bottom - box_top,
            active_branches: graph.active_branch_count(metric_node_ids),
            initiators: initiators.to_vec(),
            finalizers: finalizers.to_vec(),
        })
    }

    const CORNER_RADIUS: i32 = 8;
    const CALLOUT_HALF_HEIGHT: i32 = 12;
    const CALLOUT_GAP: i32 = 18;
    const CALLOUT_CLEARANCE: i32 = 48;

    fn callout_paths(&self) -> Vec<String> {
        let right = self.x + self.width;
        let mut paths = self
            .callout_anchors(&self.initiators)
            .into_iter()
            .map(|anchor| {
                format!(
                    "M{} {} L{} {} L{} {} Z",
                    self.x,
                    anchor.y - Self::CALLOUT_HALF_HEIGHT,
                    anchor.x,
                    anchor.y,
                    self.x,
                    anchor.y + Self::CALLOUT_HALF_HEIGHT
                )
            })
            .collect::<Vec<_>>();
        paths.extend(
            self.callout_anchors(&self.finalizers)
                .into_iter()
                .map(|anchor| {
                    format!(
                        "M{} {} L{} {} L{} {} Z",
                        right,
                        anchor.y - Self::CALLOUT_HALF_HEIGHT,
                        anchor.x,
                        anchor.y,
                        right,
                        anchor.y + Self::CALLOUT_HALF_HEIGHT
                    )
                }),
        );
        paths
    }

    fn label_style(&self) -> String {
        let width = self.width.saturating_sub(28).clamp(96, 220);
        graph_position_style(self.x + 14, self.y + 8, width, 30)
    }

    fn stack_x(&self, index: i32) -> i32 {
        self.x + index * 5
    }

    fn stack_y(&self, index: i32) -> i32 {
        self.y - index * 5
    }

    fn key_fields(&self) -> Vec<String> {
        branch_key_fields(&self.schema)
    }

    fn subtitle(&self) -> String {
        let fields = self.key_fields();
        let branches = self.active_branch_header_label();
        if fields.is_empty() {
            format!("{branches} · singleton key")
        } else {
            format!("{branches} · keys {}", fields.join(", "))
        }
    }

    fn active_branch_header_label(&self) -> String {
        format!("{} br", self.active_branches)
    }

    fn callout_anchors(&self, anchors: &[GraphAnchor]) -> Vec<GraphAnchor> {
        let min_y = self.y + Self::CORNER_RADIUS + Self::CALLOUT_HALF_HEIGHT;
        let max_y = self.y + self.height - Self::CORNER_RADIUS - Self::CALLOUT_HALF_HEIGHT;
        let mut anchors = anchors
            .iter()
            .map(|anchor| GraphAnchor {
                x: anchor.x,
                y: anchor.y.clamp(min_y, max_y),
            })
            .collect::<Vec<_>>();
        anchors.sort_unstable_by_key(|anchor| (anchor.y, anchor.x));
        anchors.dedup_by(|right, left| {
            if (right.y - left.y).abs() < Self::CALLOUT_GAP {
                right.y = (right.y + left.y) / 2;
                right.x = (right.x + left.x) / 2;
                true
            } else {
                false
            }
        });
        anchors
    }
}

#[derive(Clone, Copy)]
struct GraphAnchor {
    x: i32,
    y: i32,
}

impl GraphAnchor {
    fn outgoing_node(node: &GraphViewNode) -> Self {
        Self {
            x: node.x + GRAPH_NODE_WIDTH,
            y: node.y + GRAPH_NODE_CENTER_Y,
        }
    }

    fn incoming_node(node: &GraphViewNode) -> Self {
        Self {
            x: node.x,
            y: node.y + GRAPH_NODE_CENTER_Y,
        }
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

    fn from_rect(left: i32, top: i32, right: i32, bottom: i32) -> Self {
        Self {
            left: f64::from(left),
            top: f64::from(top),
            right: f64::from(right),
            bottom: f64::from(bottom),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct GraphRoutePoint {
    x: i32,
    y: i32,
}

impl GraphRoutePoint {
    const fn new(x: i32, y: i32) -> Self {
        Self { x, y }
    }
}

#[derive(Clone, Copy)]
struct GraphRouteRect {
    left: i32,
    top: i32,
    right: i32,
    bottom: i32,
    core_left: i32,
    core_top: i32,
    core_right: i32,
    core_bottom: i32,
}

impl GraphRouteRect {
    const MARGIN: i32 = 14;

    fn from_node(node: &GraphViewNode) -> Self {
        Self::new(
            node.x,
            node.y,
            node.x + GRAPH_NODE_WIDTH,
            node.y + GRAPH_NODE_HEIGHT,
        )
    }

    fn from_relay(relay: &GraphViewRelay) -> Self {
        let width = relay.width();
        Self::new(
            relay.x - width / 2,
            relay.y - 10,
            relay.x + width / 2,
            relay.y + 10,
        )
    }

    fn from_branch_group(group: &GraphBranchGroup) -> Self {
        Self::new(
            group.x,
            group.y,
            group.x + group.width,
            group.y + group.height,
        )
    }

    fn new(left: i32, top: i32, right: i32, bottom: i32) -> Self {
        Self {
            left: left - Self::MARGIN,
            top: top - Self::MARGIN,
            right: right + Self::MARGIN,
            bottom: bottom + Self::MARGIN,
            core_left: left,
            core_top: top,
            core_right: right,
            core_bottom: bottom,
        }
    }

    fn intersects_segment(self, start: GraphRoutePoint, end: GraphRoutePoint) -> bool {
        self.intersects_bounds(start, end, self.left, self.top, self.right, self.bottom)
    }

    fn contains_core_point(self, x: f64, y: f64) -> bool {
        x > f64::from(self.core_left)
            && x < f64::from(self.core_right)
            && y > f64::from(self.core_top)
            && y < f64::from(self.core_bottom)
    }

    fn intersects_core_segment(self, start: GraphRoutePoint, end: GraphRoutePoint) -> bool {
        self.intersects_bounds(
            start,
            end,
            self.core_left,
            self.core_top,
            self.core_right,
            self.core_bottom,
        )
    }

    fn blocks_segment(
        self,
        start: GraphRoutePoint,
        end: GraphRoutePoint,
        route_start: GraphRoutePoint,
        route_end: GraphRoutePoint,
    ) -> bool {
        if start == route_start || end == route_start || start == route_end || end == route_end {
            return self.intersects_core_segment(start, end);
        }
        self.intersects_segment(start, end)
    }

    fn intersects_bounds(
        self,
        start: GraphRoutePoint,
        end: GraphRoutePoint,
        left: i32,
        top: i32,
        right: i32,
        bottom: i32,
    ) -> bool {
        if start.x == end.x {
            let y_min = start.y.min(end.y);
            let y_max = start.y.max(end.y);
            return start.x > left && start.x < right && y_max > top && y_min < bottom;
        }
        if start.y == end.y {
            let x_min = start.x.min(end.x);
            let x_max = start.x.max(end.x);
            return start.y > top && start.y < bottom && x_max > left && x_min < right;
        }
        false
    }
}

#[derive(Clone)]
struct GraphViewEdge {
    source: String,
    target: String,
    kind: DataflowEdgeKind,
    statistics: GraphStatistics,
    branches: Vec<GraphBranchStatistics>,
    x1: i32,
    y1: i32,
    x2: i32,
    y2: i32,
}

#[derive(Clone, Copy)]
struct GraphEdgeLaneCandidate<'a> {
    source: &'a str,
    target: &'a str,
    kind: DataflowEdgeKind,
    base_y: i32,
    left: i32,
    right: i32,
}

impl<'a> GraphEdgeLaneCandidate<'a> {
    fn from_edge(graph: &GraphView, edge: &'a GraphViewEdge) -> Option<Self> {
        let ((x1, y1), (x2, y2)) = edge.endpoints(graph);
        let left = x1.min(x2);
        let right = x1.max(x2);
        if right - left < GRAPH_EDGE_LANE_MIN_SPAN {
            return None;
        }
        Some(Self {
            source: edge.source.as_str(),
            target: edge.target.as_str(),
            kind: edge.kind,
            base_y: (y1 + y2) / 2,
            left,
            right,
        })
    }

    fn overlaps_lane_group(self, other: Self) -> bool {
        let overlap = self.horizontal_overlap(other);
        if self.source == other.source || self.target == other.target {
            return (self.base_y - other.base_y).abs() < GRAPH_EDGE_LANE_SPACING
                && overlap >= GRAPH_EDGE_SHARED_ENDPOINT_LANE_MIN_OVERLAP;
        }
        (self.base_y - other.base_y).abs() <= GRAPH_EDGE_LANE_GROUP_Y
            && overlap >= GRAPH_EDGE_LANE_MIN_OVERLAP
    }

    fn horizontal_overlap(self, other: Self) -> i32 {
        self.right.min(other.right) - self.left.max(other.left)
    }

    fn same_edge(self, other: Self) -> bool {
        self.source == other.source && self.target == other.target && self.kind == other.kind
    }
}

impl GraphViewEdge {
    fn endpoints(&self, domain: &GraphView) -> ((i32, i32), (i32, i32)) {
        (
            domain
                .graph_endpoint(&self.source, EndpointSide::Outgoing)
                .unwrap_or((self.x1, self.y1)),
            domain
                .graph_endpoint(&self.target, EndpointSide::Incoming)
                .unwrap_or((self.x2, self.y2)),
        )
    }

    fn path(&self, domain: &GraphView) -> String {
        let ((x1, y1), (x2, y2)) = self.endpoints(domain);
        let start = GraphRoutePoint::new(x1, y1);
        let end = GraphRoutePoint::new(x2, y2);
        let obstacles = domain.edge_obstacles(&self.source, &self.target);
        let preferred_lane = domain.edge_preferred_lane(self);
        if should_route_with_direct_curve(preferred_lane, start, end)
            && let Some(path) = direct_curve_path(start, end, &obstacles)
        {
            return path;
        }
        let points = self.route_points_with_lane(domain, preferred_lane);
        Self::rounded_path(&points)
    }

    fn rounded_path(points: &[GraphRoutePoint]) -> String {
        let Some(start) = points.first() else {
            return String::new();
        };
        let mut path = format!("M{} {}", start.x, start.y);
        if points.len() == 1 {
            return path;
        }
        const CORNER_RADIUS: i32 = 18;
        const ENDPOINT_CORNER_RADIUS: i32 = 8;
        for index in 1..points.len() - 1 {
            let previous = points[index - 1];
            let current = points[index];
            let next = points[index + 1];
            let incoming = (current.x - previous.x, current.y - previous.y);
            let outgoing = (next.x - current.x, next.y - current.y);
            let incoming_length = incoming.0.abs() + incoming.1.abs();
            let outgoing_length = outgoing.0.abs() + outgoing.1.abs();
            let mut radius = CORNER_RADIUS
                .min(incoming_length / 2)
                .min(outgoing_length / 2);
            if index == 1 || index == points.len() - 2 {
                radius = radius.min(ENDPOINT_CORNER_RADIUS);
            }
            if radius == 0
                || incoming.0.signum() == outgoing.0.signum()
                    && incoming.1.signum() == outgoing.1.signum()
            {
                path.push_str(&format!(" L{} {}", current.x, current.y));
                continue;
            }
            let entry = GraphRoutePoint::new(
                current.x - incoming.0.signum() * radius,
                current.y - incoming.1.signum() * radius,
            );
            let exit = GraphRoutePoint::new(
                current.x + outgoing.0.signum() * radius,
                current.y + outgoing.1.signum() * radius,
            );
            path.push_str(&format!(" L{} {}", entry.x, entry.y));
            path.push_str(&format!(
                " Q{} {}, {} {}",
                current.x, current.y, exit.x, exit.y
            ));
        }
        let end = points.last().expect("non-empty points checked above");
        path.push_str(&format!(" L{} {}", end.x, end.y));
        path
    }

    fn metric_style(&self, domain: &GraphView) -> String {
        let ((x1, y1), (x2, y2)) = self.endpoints(domain);
        let start = GraphRoutePoint::new(x1, y1);
        let end = GraphRoutePoint::new(x2, y2);
        let obstacles = domain.edge_obstacles(&self.source, &self.target);
        let preferred_lane = domain.edge_preferred_lane(self);
        let ((mid_x, mid_y), (tangent_x, tangent_y)) =
            if should_route_with_direct_curve(preferred_lane, start, end)
                && let Some(curve) = direct_curve(start, end, &obstacles)
            {
                curve_midpoint(curve)
            } else {
                let points = self.route_points_with_lane(domain, preferred_lane);
                Self::polyline_midpoint(&points)
            };
        let length = (tangent_x.powi(2) + tangent_y.powi(2)).sqrt().max(1.0);
        let normal_x = tangent_y / length;
        let normal_y = -tangent_x / length;
        let width = 68;
        let height = 16;
        let x = (mid_x + normal_x * 12.0).round() as i32 - width / 2;
        let y = (mid_y + normal_y * 12.0).round() as i32 - height / 2;
        graph_position_style(x, y, width, height)
    }

    #[cfg(test)]
    fn route_points(&self, domain: &GraphView) -> Vec<GraphRoutePoint> {
        self.route_points_with_lane(domain, domain.edge_preferred_lane(self))
    }

    fn route_points_with_lane(
        &self,
        domain: &GraphView,
        preferred_lane: Option<i32>,
    ) -> Vec<GraphRoutePoint> {
        let ((x1, y1), (x2, y2)) = self.endpoints(domain);
        let start = GraphRoutePoint::new(x1, y1);
        let end = GraphRoutePoint::new(x2, y2);
        let obstacles = domain.edge_obstacles(&self.source, &self.target);
        route_graph_edge(
            start,
            end,
            &obstacles,
            domain.canvas_width(),
            domain.canvas_height(),
            preferred_lane,
        )
        .unwrap_or_else(|| {
            let mid_x = (x1 + x2) / 2;
            if let Some(lane) = preferred_lane {
                vec![
                    start,
                    GraphRoutePoint::new(mid_x, y1),
                    GraphRoutePoint::new(mid_x, lane),
                    GraphRoutePoint::new(mid_x, y2),
                    end,
                ]
            } else {
                vec![
                    start,
                    GraphRoutePoint::new(mid_x, y1),
                    GraphRoutePoint::new(mid_x, y2),
                    end,
                ]
            }
        })
    }

    fn polyline_midpoint(points: &[GraphRoutePoint]) -> ((f64, f64), (f64, f64)) {
        let Some(first) = points.first() else {
            return ((0.0, 0.0), (1.0, 0.0));
        };
        if points.len() == 1 {
            return ((f64::from(first.x), f64::from(first.y)), (1.0, 0.0));
        }
        let total = points
            .windows(2)
            .map(|window| {
                let start = window[0];
                let end = window[1];
                (end.x - start.x).abs() + (end.y - start.y).abs()
            })
            .sum::<i32>()
            .max(1);
        let target = total as f64 / 2.0;
        let mut traversed = 0.0;
        for window in points.windows(2) {
            let start = window[0];
            let end = window[1];
            let dx = f64::from(end.x - start.x);
            let dy = f64::from(end.y - start.y);
            let segment = dx.abs() + dy.abs();
            if segment <= 0.0 {
                continue;
            }
            if traversed + segment >= target {
                let ratio = ((target - traversed) / segment).clamp(0.0, 1.0);
                return (
                    (
                        f64::from(start.x) + dx * ratio,
                        f64::from(start.y) + dy * ratio,
                    ),
                    (dx, dy),
                );
            }
            traversed += segment;
        }
        let last = points.last().copied().unwrap_or(*first);
        let previous = points.iter().rev().nth(1).copied().unwrap_or(last);
        (
            (f64::from(last.x), f64::from(last.y)),
            (
                f64::from(last.x - previous.x),
                f64::from(last.y - previous.y),
            ),
        )
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
        parts.join("; ")
    }
}

#[derive(Clone, Copy)]
struct GraphRouteCurve {
    start: GraphRoutePoint,
    control_1: GraphRoutePoint,
    control_2: GraphRoutePoint,
    end: GraphRoutePoint,
}

fn direct_curve_path(
    start: GraphRoutePoint,
    end: GraphRoutePoint,
    obstacles: &[GraphRouteRect],
) -> Option<String> {
    let curve = direct_curve(start, end, obstacles)?;
    Some(format!(
        "M{} {} C{} {}, {} {}, {} {}",
        curve.start.x,
        curve.start.y,
        curve.control_1.x,
        curve.control_1.y,
        curve.control_2.x,
        curve.control_2.y,
        curve.end.x,
        curve.end.y
    ))
}

fn should_route_with_direct_curve(
    preferred_lane: Option<i32>,
    start: GraphRoutePoint,
    end: GraphRoutePoint,
) -> bool {
    preferred_lane.is_none()
        || (end.x - start.x).abs()
            < GRAPH_EDGE_TURN_X + GRAPH_EDGE_TERMINAL_STRAIGHT + GRAPH_EDGE_LANE_SPACING
}

fn direct_curve(
    start: GraphRoutePoint,
    end: GraphRoutePoint,
    obstacles: &[GraphRouteRect],
) -> Option<GraphRouteCurve> {
    let flow_direction = (end.x - start.x).signum();
    if flow_direction == 0 {
        return None;
    }
    let horizontal_distance = (end.x - start.x).abs();
    if horizontal_distance < 48 {
        return None;
    }
    let control_offset = ((horizontal_distance as f64 * 0.45).round() as i32).max(36);
    let curve = GraphRouteCurve {
        start,
        control_1: GraphRoutePoint::new(start.x + flow_direction * control_offset, start.y),
        control_2: GraphRoutePoint::new(end.x - flow_direction * control_offset, end.y),
        end,
    };
    let distance = (end.x - start.x).abs() + (end.y - start.y).abs();
    let samples = (distance / 8).clamp(12, 80);
    for index in 1..samples {
        let t = f64::from(index) / f64::from(samples);
        let (x, y) = curve_point(curve, t);
        if obstacles
            .iter()
            .any(|obstacle| obstacle.contains_core_point(x, y))
        {
            return None;
        }
    }
    Some(curve)
}

fn curve_midpoint(curve: GraphRouteCurve) -> ((f64, f64), (f64, f64)) {
    (curve_point(curve, 0.5), curve_tangent(curve, 0.5))
}

fn curve_point(curve: GraphRouteCurve, t: f64) -> (f64, f64) {
    let inv = 1.0 - t;
    let start = (f64::from(curve.start.x), f64::from(curve.start.y));
    let control_1 = (f64::from(curve.control_1.x), f64::from(curve.control_1.y));
    let control_2 = (f64::from(curve.control_2.x), f64::from(curve.control_2.y));
    let end = (f64::from(curve.end.x), f64::from(curve.end.y));
    (
        inv.powi(3) * start.0
            + 3.0 * inv.powi(2) * t * control_1.0
            + 3.0 * inv * t.powi(2) * control_2.0
            + t.powi(3) * end.0,
        inv.powi(3) * start.1
            + 3.0 * inv.powi(2) * t * control_1.1
            + 3.0 * inv * t.powi(2) * control_2.1
            + t.powi(3) * end.1,
    )
}

fn curve_tangent(curve: GraphRouteCurve, t: f64) -> (f64, f64) {
    let inv = 1.0 - t;
    let start = (f64::from(curve.start.x), f64::from(curve.start.y));
    let control_1 = (f64::from(curve.control_1.x), f64::from(curve.control_1.y));
    let control_2 = (f64::from(curve.control_2.x), f64::from(curve.control_2.y));
    let end = (f64::from(curve.end.x), f64::from(curve.end.y));
    (
        3.0 * inv.powi(2) * (control_1.0 - start.0)
            + 6.0 * inv * t * (control_2.0 - control_1.0)
            + 3.0 * t.powi(2) * (end.0 - control_2.0),
        3.0 * inv.powi(2) * (control_1.1 - start.1)
            + 6.0 * inv * t * (control_2.1 - control_1.1)
            + 3.0 * t.powi(2) * (end.1 - control_2.1),
    )
}

fn route_graph_edge(
    start: GraphRoutePoint,
    end: GraphRoutePoint,
    obstacles: &[GraphRouteRect],
    canvas_width: i32,
    canvas_height: i32,
    preferred_lane: Option<i32>,
) -> Option<Vec<GraphRoutePoint>> {
    let mut xs = vec![0, canvas_width, start.x, end.x];
    let mut ys = vec![0, canvas_height, start.y, end.y];
    let flow_direction = (end.x - start.x).signum();
    let source_straight_end_x = start.x + flow_direction * GRAPH_EDGE_TURN_X;
    let requires_source_straight =
        if flow_direction != 0 && (end.x - start.x).abs() >= GRAPH_EDGE_TURN_X {
            let plug_end = GraphRoutePoint::new(source_straight_end_x, start.y);
            plug_end.x > 0
                && plug_end.x < canvas_width
                && !obstacles
                    .iter()
                    .any(|obstacle| obstacle.intersects_segment(start, plug_end))
        } else {
            false
        };
    let requires_terminal_straight =
        if flow_direction != 0 && (end.x - start.x).abs() >= GRAPH_EDGE_TERMINAL_STRAIGHT {
            let plug_start =
                GraphRoutePoint::new(end.x - flow_direction * GRAPH_EDGE_TERMINAL_STRAIGHT, end.y);
            plug_start.x > 0
                && plug_start.x < canvas_width
                && !obstacles
                    .iter()
                    .any(|obstacle| obstacle.intersects_segment(plug_start, end))
        } else {
            false
        };
    if flow_direction != 0 {
        let start_turn_x = start.x + flow_direction * GRAPH_EDGE_TURN_X;
        let end_turn_x = end.x - flow_direction * GRAPH_EDGE_TERMINAL_STRAIGHT;
        let mid_x = (start.x + end.x) / 2;
        for x in [start_turn_x, end_turn_x, mid_x] {
            if x > 0 && x < canvas_width {
                xs.push(x);
            }
        }
    }
    if let Some(lane) = preferred_lane
        && lane > 0
        && lane < canvas_height
    {
        ys.push(lane);
    }
    for obstacle in obstacles {
        xs.extend([obstacle.left.max(0), obstacle.right.min(canvas_width)]);
        ys.extend([obstacle.top.max(0), obstacle.bottom.min(canvas_height)]);
    }
    xs.sort_unstable();
    xs.dedup();
    ys.sort_unstable();
    ys.dedup();
    let start_x = xs.iter().position(|x| *x == start.x)?;
    let start_y = ys.iter().position(|y| *y == start.y)?;
    let end_x = xs.iter().position(|x| *x == end.x)?;
    let end_y = ys.iter().position(|y| *y == end.y)?;
    let width = xs.len();
    let point_count = width * ys.len();
    let state_count = point_count * 3;
    let point_index = |x_index: usize, y_index: usize| y_index * width + x_index;
    let state_index = |point: usize, direction: usize| point * 3 + direction;
    let state_point = |state: usize| state / 3;
    let state_direction = |state: usize| state % 3;
    let start_point = point_index(start_x, start_y);
    let end_point = point_index(end_x, end_y);
    let start_state = state_index(start_point, 0);
    let mut distances = vec![i32::MAX; state_count];
    let mut previous = vec![None::<usize>; state_count];
    let mut pending = BinaryHeap::<(Reverse<i32>, usize)>::new();
    distances[start_state] = 0;
    pending.push((Reverse(0), start_state));

    while let Some((Reverse(cost), state)) = pending.pop() {
        if cost != distances[state] {
            continue;
        }
        let point = state_point(state);
        let direction = state_direction(state);
        let x_index = point % width;
        let y_index = point / width;
        for (next_x, next_y, next_direction) in [
            (x_index.checked_sub(1), Some(y_index), 1_usize),
            (
                (x_index + 1 < width).then_some(x_index + 1),
                Some(y_index),
                1,
            ),
            (Some(x_index), y_index.checked_sub(1), 2),
            (
                Some(x_index),
                (y_index + 1 < ys.len()).then_some(y_index + 1),
                2,
            ),
        ] {
            let (Some(next_x), Some(next_y)) = (next_x, next_y) else {
                continue;
            };
            let current = GraphRoutePoint::new(xs[x_index], ys[y_index]);
            let next = GraphRoutePoint::new(xs[next_x], ys[next_y]);
            if flow_direction != 0 {
                if requires_source_straight {
                    if current == start {
                        let step_direction = (next.x - current.x).signum();
                        if next_direction != 1 || step_direction != flow_direction {
                            continue;
                        }
                    }
                    if next_direction == 2
                        && source_plug_zone_contains_x(
                            current.x,
                            start.x,
                            source_straight_end_x,
                            flow_direction,
                        )
                    {
                        continue;
                    }
                }
                if next == end {
                    let step_direction = (next.x - current.x).signum();
                    if next_direction != 1 || step_direction != flow_direction {
                        continue;
                    }
                }
                if requires_terminal_straight && next_direction == 2 {
                    let terminal_start_x = end.x - flow_direction * GRAPH_EDGE_TERMINAL_STRAIGHT;
                    if current.x > terminal_start_x.min(end.x)
                        && current.x < terminal_start_x.max(end.x)
                    {
                        continue;
                    }
                }
            }
            if obstacles
                .iter()
                .any(|obstacle| obstacle.blocks_segment(current, next, start, end))
            {
                continue;
            }
            let distance = (next.x - current.x).abs() + (next.y - current.y).abs();
            let turn_penalty = if direction != 0 && direction != next_direction {
                24
            } else {
                0
            };
            let flow_penalty = route_flow_penalty(
                current,
                next,
                start,
                end,
                next_direction,
                canvas_width,
                canvas_height,
                preferred_lane,
            );
            let next_point = point_index(next_x, next_y);
            let next_state = state_index(next_point, next_direction);
            let next_cost = cost
                .saturating_add(distance)
                .saturating_add(turn_penalty)
                .saturating_add(flow_penalty);
            if next_cost < distances[next_state] {
                distances[next_state] = next_cost;
                previous[next_state] = Some(state);
                pending.push((Reverse(next_cost), next_state));
            }
        }
    }

    let end_state = (0..3)
        .map(|direction| state_index(end_point, direction))
        .min_by_key(|state| {
            distances[*state].saturating_add(route_end_direction_penalty(
                state_direction(*state),
                start,
                end,
            ))
        })?;
    if distances[end_state] == i32::MAX {
        return None;
    }

    let mut states = Vec::new();
    let mut state = end_state;
    states.push(state);
    while state != start_state {
        state = previous[state]?;
        states.push(state);
    }
    states.reverse();
    let points = states
        .into_iter()
        .map(|state| {
            let point = state_point(state);
            GraphRoutePoint::new(xs[point % width], ys[point / width])
        })
        .collect::<Vec<_>>();
    Some(simplify_route_points(points))
}

fn source_plug_zone_contains_x(x: i32, start_x: i32, end_x: i32, flow_direction: i32) -> bool {
    if flow_direction > 0 {
        x >= start_x && x < end_x
    } else {
        x > end_x && x <= start_x
    }
}

fn route_flow_penalty(
    current: GraphRoutePoint,
    next: GraphRoutePoint,
    start: GraphRoutePoint,
    end: GraphRoutePoint,
    next_direction: usize,
    canvas_width: i32,
    canvas_height: i32,
    preferred_lane: Option<i32>,
) -> i32 {
    let mut penalty = 0_i32;
    let flow_direction = (end.x - start.x).signum();
    if flow_direction != 0 {
        let segment_direction = (next.x - current.x).signum();
        if segment_direction == -flow_direction {
            penalty = penalty.saturating_add(600);
        }
        if current == start && next_direction == 2 {
            penalty = penalty.saturating_add(180);
        }
        if next == end && next_direction == 2 {
            penalty = penalty.saturating_add(900);
        }
        if next == end && next_direction == 1 {
            let segment_direction = (next.x - current.x).signum();
            if segment_direction != flow_direction {
                penalty = penalty.saturating_add(5_000);
            }
        }
    }
    if current.x == 0 || next.x == 0 || current.x == canvas_width || next.x == canvas_width {
        penalty = penalty.saturating_add(480);
    }
    if current.y == 0 || next.y == 0 || current.y == canvas_height || next.y == canvas_height {
        penalty = penalty.saturating_add(480);
    }
    if let Some(lane) = preferred_lane
        && next_direction == 1
        && current.y != lane
    {
        let segment_length = (next.x - current.x).abs();
        let short_terminal_segment =
            segment_length <= GRAPH_EDGE_TERMINAL_STRAIGHT && (current == start || next == end);
        if !short_terminal_segment {
            penalty = penalty.saturating_add(1_400 + segment_length.saturating_mul(2));
        }
    }
    if preferred_lane.is_none() && next_direction == 1 && start.y != end.y && current.y != end.y {
        let segment_length = (next.x - current.x).abs();
        let source_plug_segment = current == start && segment_length <= GRAPH_EDGE_TURN_X;
        if !source_plug_segment {
            penalty = penalty.saturating_add(900 + segment_length.saturating_mul(2));
        }
    }
    penalty
}

fn route_end_direction_penalty(
    direction: usize,
    start: GraphRoutePoint,
    end: GraphRoutePoint,
) -> i32 {
    if (end.x - start.x).signum() != 0 && direction == 2 {
        return 900;
    }
    0
}

fn simplify_route_points(points: Vec<GraphRoutePoint>) -> Vec<GraphRoutePoint> {
    let mut simplified = Vec::<GraphRoutePoint>::new();
    for point in points {
        if simplified.last().is_some_and(|last| *last == point) {
            continue;
        }
        simplified.push(point);
        while simplified.len() >= 3 {
            let len = simplified.len();
            let a = simplified[len - 3];
            let b = simplified[len - 2];
            let c = simplified[len - 1];
            if (a.x == b.x && b.x == c.x) || (a.y == b.y && b.y == c.y) {
                simplified.remove(len - 2);
            } else {
                break;
            }
        }
    }
    simplified
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
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct DomainView {
    id: String,
    mode: String,
    status: String,
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
    const fn category_index(self) -> u64 {
        match self {
            Self::Client => 0,
            Self::Ingestor => 1,
            Self::Processor => 2,
            Self::Emitter => 3,
        }
    }

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
enum EndpointSide {
    Outgoing,
    Incoming,
}

#[derive(Clone, Copy)]
struct GraphDrag {
    client_x: i32,
    client_y: i32,
    pan_x: f64,
    pan_y: f64,
}

fn graph_position_style(x: i32, y: i32, width: i32, height: i32) -> String {
    format!("left: {x}px; top: {y}px; width: {width}px; height: {height}px;")
}

fn branch_key_fields(schema: &str) -> Vec<String> {
    let stem = schema.strip_suffix("_branch").unwrap_or(schema);
    stem.split('_')
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

#[derive(Clone)]
struct TermLine {
    kind: TermLineKind,
    text: String,
}

impl TermLine {
    fn prompt(text: impl Into<String>, transaction_active: bool) -> Self {
        let prompt = if transaction_active {
            "nervix[tx]>"
        } else {
            "nervix>"
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
    use nervix_dataflow_graph::{DataflowBranchStatistics, DataflowEdge, DataflowNode};

    use super::*;

    #[test]
    fn branch_group_uses_callouts_for_initiator_and_finalizers() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node(
                    "processor:device_repartition",
                    "device_repartition",
                    DataflowNodeKind::Processor,
                    "reingestor",
                    "device_branch",
                    0,
                    100,
                ),
                node(
                    "relay:telemetry_ordered",
                    "telemetry_ordered",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    260,
                    100,
                ),
                unbranched_node(
                    "processor:anomaly_splitter",
                    "anomaly_splitter",
                    DataflowNodeKind::Processor,
                    "deduplicator",
                    520,
                    100,
                ),
                node(
                    "relay:thermal_alerts",
                    "thermal_alerts",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    780,
                    40,
                ),
                node(
                    "relay:maintenance_alerts",
                    "maintenance_alerts",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    780,
                    160,
                ),
                node(
                    "emitter:redis_thermal_alerts",
                    "redis_thermal_alerts",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    1040,
                    40,
                ),
                node(
                    "emitter:redis_maintenance_alerts",
                    "redis_maintenance_alerts",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    1040,
                    160,
                ),
            ],
            edges: vec![
                edge("processor:device_repartition", "relay:telemetry_ordered"),
                edge("relay:telemetry_ordered", "processor:anomaly_splitter"),
                edge("processor:anomaly_splitter", "relay:thermal_alerts"),
                edge("processor:anomaly_splitter", "relay:maintenance_alerts"),
                edge("relay:thermal_alerts", "emitter:redis_thermal_alerts"),
                edge(
                    "relay:maintenance_alerts",
                    "emitter:redis_maintenance_alerts",
                ),
            ],
        });

        let groups = graph.branching_groups();
        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.schema, "device_branch");
        assert_eq!(group.initiators.len(), 1);
        assert_eq!(group.finalizers.len(), 2);
        assert!(
            group.x > GRAPH_NODE_WIDTH,
            "branch body should not cover the reingestor"
        );
        assert!(
            group.x + group.width < 1040,
            "branch body should not cover finalizing emitters"
        );
        assert_eq!(
            group.x - GRAPH_NODE_WIDTH,
            GraphBranchGroup::CALLOUT_CLEARANCE,
            "branch body should start close to the initiating node border"
        );
        assert_eq!(
            1040 - (group.x + group.width),
            GraphBranchGroup::CALLOUT_CLEARANCE,
            "branch body should end close to the finalizing node borders"
        );
        let callouts = group.callout_paths();
        assert_eq!(callouts.len(), 3);
        assert!(
            callouts
                .iter()
                .any(|path| path.contains(&format!("L{} ", GRAPH_NODE_WIDTH))),
            "branch callout should point left to its initiator"
        );
        assert!(
            callouts
                .iter()
                .any(|path| path.contains(&format!("L{} ", 1040))),
            "branch callout should point right to finalizing consumers"
        );
    }

    #[test]
    fn branch_group_is_not_an_obstacle_for_attached_member_edges() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node(
                    "processor:device_repartition",
                    "device_repartition",
                    DataflowNodeKind::Processor,
                    "reingestor",
                    "device_branch",
                    0,
                    100,
                ),
                node(
                    "relay:telemetry_ordered",
                    "telemetry_ordered",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    260,
                    100,
                ),
                node(
                    "emitter:redis_maintenance_alerts",
                    "redis_maintenance_alerts",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    520,
                    100,
                ),
                unbranched_node(
                    "client:redis_alerts",
                    "redis_alerts",
                    DataflowNodeKind::Client,
                    "REDIS",
                    780,
                    100,
                ),
            ],
            edges: vec![
                edge("processor:device_repartition", "relay:telemetry_ordered"),
                edge(
                    "relay:telemetry_ordered",
                    "emitter:redis_maintenance_alerts",
                ),
                edge("emitter:redis_maintenance_alerts", "client:redis_alerts"),
            ],
        });

        let groups = graph.branching_groups();
        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert!(
            group
                .boundary_nodes
                .contains("emitter:redis_maintenance_alerts")
        );
        let entry_obstacles =
            graph.edge_obstacles("processor:device_repartition", "relay:telemetry_ordered");
        let exit_obstacles = graph.edge_obstacles(
            "relay:telemetry_ordered",
            "emitter:redis_maintenance_alerts",
        );

        assert!(
            entry_obstacles.iter().all(|obstacle| {
                obstacle.core_left != group.x
                    || obstacle.core_top != group.y
                    || obstacle.core_right != group.x + group.width
                    || obstacle.core_bottom != group.y + group.height
            }),
            "edges entering a branch group should not route around that same group"
        );
        assert!(
            exit_obstacles.iter().all(|obstacle| {
                obstacle.core_left != group.x
                    || obstacle.core_top != group.y
                    || obstacle.core_right != group.x + group.width
                    || obstacle.core_bottom != group.y + group.height
            }),
            "edges exiting a branch group member should not route around that same group"
        );
    }

    #[test]
    fn finalizer_edge_routes_around_unrelated_downstream_branch_group() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node(
                    "ingestor:site_ingest",
                    "site_ingest",
                    DataflowNodeKind::Ingestor,
                    "http",
                    "site_branch",
                    0,
                    300,
                ),
                node(
                    "relay:battery_alerts",
                    "battery_alerts",
                    DataflowNodeKind::Relay,
                    "relay",
                    "site_branch",
                    300,
                    300,
                ),
                node(
                    "emitter:redis_battery_alerts",
                    "redis_battery_alerts",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "site_branch",
                    560,
                    300,
                ),
                node(
                    "processor:device_repartition",
                    "device_repartition",
                    DataflowNodeKind::Processor,
                    "reingestor",
                    "device_branch",
                    700,
                    120,
                ),
                node(
                    "relay:maintenance_alerts",
                    "maintenance_alerts",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    960,
                    120,
                ),
                node(
                    "emitter:redis_maintenance_alerts",
                    "redis_maintenance_alerts",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    1220,
                    120,
                ),
                unbranched_node(
                    "client:redis_alerts",
                    "redis_alerts",
                    DataflowNodeKind::Client,
                    "REDIS",
                    1560,
                    300,
                ),
            ],
            edges: vec![
                edge("ingestor:site_ingest", "relay:battery_alerts"),
                edge("relay:battery_alerts", "emitter:redis_battery_alerts"),
                edge("emitter:redis_battery_alerts", "client:redis_alerts"),
                edge("processor:device_repartition", "relay:maintenance_alerts"),
                edge(
                    "relay:maintenance_alerts",
                    "emitter:redis_maintenance_alerts",
                ),
                edge("emitter:redis_maintenance_alerts", "client:redis_alerts"),
            ],
        });

        let groups = graph.branching_groups();
        let device_group = groups
            .iter()
            .find(|group| group.schema == "device_branch")
            .expect("device branch group should be present");
        let route = graph
            .edges
            .iter()
            .find(|edge| {
                edge.source == "emitter:redis_battery_alerts"
                    && edge.target == "client:redis_alerts"
            })
            .map(|edge| edge.route_points(&graph))
            .expect("shared sink edge should route");
        let obstacle = GraphRouteRect::from_branch_group(device_group);

        assert!(
            route
                .windows(2)
                .all(|segment| !obstacle.intersects_core_segment(segment[0], segment[1])),
            "shared sink edge should avoid unrelated branch group body: {route:?}"
        );
    }

    #[test]
    fn route_graph_edge_avoids_branch_body_after_source_plug() {
        let start = GraphRoutePoint::new(1814, 310);
        let end = GraphRoutePoint::new(3224, 251);
        let obstacle = GraphRouteRect::new(1862, 128, 2856, 338);
        let route = route_graph_edge(start, end, &[obstacle], 3400, 420, Some(310))
            .expect("edge should route around branch body");

        assert!(
            route
                .windows(2)
                .all(|segment| !obstacle.intersects_core_segment(segment[0], segment[1])),
            "route should avoid branch body: {route:?}"
        );
    }

    #[test]
    fn branch_groups_are_not_merged_only_because_schema_matches() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node(
                    "processor:left_repartition",
                    "left_repartition",
                    DataflowNodeKind::Processor,
                    "reingestor",
                    "device_branch",
                    0,
                    40,
                ),
                node(
                    "relay:left_stream",
                    "left_stream",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    260,
                    40,
                ),
                node(
                    "emitter:left_redis",
                    "left_redis",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    520,
                    40,
                ),
                node(
                    "processor:right_repartition",
                    "right_repartition",
                    DataflowNodeKind::Processor,
                    "reingestor",
                    "device_branch",
                    0,
                    180,
                ),
                node(
                    "relay:right_stream",
                    "right_stream",
                    DataflowNodeKind::Relay,
                    "relay",
                    "device_branch",
                    260,
                    180,
                ),
                node(
                    "emitter:right_redis",
                    "right_redis",
                    DataflowNodeKind::Emitter,
                    "redis",
                    "device_branch",
                    520,
                    180,
                ),
            ],
            edges: vec![
                edge("processor:left_repartition", "relay:left_stream"),
                edge("relay:left_stream", "emitter:left_redis"),
                edge("processor:right_repartition", "relay:right_stream"),
                edge("relay:right_stream", "emitter:right_redis"),
            ],
        });

        let groups = graph.branching_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.schema == "device_branch"));
        assert_ne!(groups[0].id, groups[1].id);
    }

    #[test]
    fn branch_group_merges_multiple_ingestors_for_same_branch_members() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                unbranched_node(
                    "ingestor:mqtt_primary",
                    "mqtt_primary",
                    DataflowNodeKind::Ingestor,
                    "MQTT",
                    0,
                    40,
                ),
                unbranched_node(
                    "ingestor:mqtt_backup",
                    "mqtt_backup",
                    DataflowNodeKind::Ingestor,
                    "MQTT",
                    0,
                    180,
                ),
                node(
                    "relay:telemetry_by_site",
                    "telemetry_by_site",
                    DataflowNodeKind::Relay,
                    "relay",
                    "site_branch",
                    320,
                    110,
                ),
            ],
            edges: vec![
                edge("ingestor:mqtt_primary", "relay:telemetry_by_site"),
                edge("ingestor:mqtt_backup", "relay:telemetry_by_site"),
            ],
        });

        let groups = graph.branching_groups();
        assert_eq!(groups.len(), 1);
        let group = &groups[0];
        assert_eq!(group.schema, "site_branch");
        assert_eq!(group.initiators.len(), 2);
        assert_eq!(group.finalizers.len(), 0);
        assert_eq!(group.callout_paths().len(), 2);
        assert_eq!(group.key_fields(), vec!["site".to_string()]);
    }

    #[test]
    fn branch_group_counts_unique_active_branches_from_group_items() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "iot_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node_with_branches(
                    "ingestor:mqtt_primary",
                    "mqtt_primary",
                    DataflowNodeKind::Ingestor,
                    "MQTT",
                    "site_branch",
                    0,
                    40,
                    &["site=iad-1", "site=sfo-1"],
                ),
                node_with_branches(
                    "ingestor:mqtt_backup",
                    "mqtt_backup",
                    DataflowNodeKind::Ingestor,
                    "MQTT",
                    "site_branch",
                    0,
                    180,
                    &["site=sfo-1"],
                ),
                node_with_branches(
                    "relay:telemetry_by_site",
                    "telemetry_by_site",
                    DataflowNodeKind::Relay,
                    "relay",
                    "site_branch",
                    320,
                    110,
                    &["site=lhr-1"],
                ),
            ],
            edges: vec![
                edge("ingestor:mqtt_primary", "relay:telemetry_by_site"),
                edge("ingestor:mqtt_backup", "relay:telemetry_by_site"),
            ],
        });

        let groups = graph.branching_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].active_branches, 3);
        assert_eq!(groups[0].subtitle(), "3 br · keys site".to_string());
    }

    #[test]
    fn datalake_node_geometry_aligns_hit_box_route_endpoint_and_obstacle() {
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "datalake_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![unbranched_node(
                "emitter:iceberg_connected_sessions",
                "iceberg_connected_sessions",
                DataflowNodeKind::Emitter,
                "ICEBERG",
                0,
                0,
            )],
            edges: Vec::new(),
        });

        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == "emitter:iceberg_connected_sessions")
            .expect("datalake node should be present");
        assert_eq!(
            node.hit_style(),
            graph_position_style(0, 0, GRAPH_NODE_WIDTH, GRAPH_NODE_HEIGHT)
        );
        assert_eq!(
            graph.graph_endpoint(&node.id, EndpointSide::Outgoing),
            Some((GRAPH_NODE_WIDTH, GRAPH_NODE_CENTER_Y))
        );
        let obstacle = GraphRouteRect::from_node(node);
        assert_eq!(obstacle.core_left, 0);
        assert_eq!(obstacle.core_top, 0);
        assert_eq!(obstacle.core_right, GRAPH_NODE_WIDTH);
        assert_eq!(obstacle.core_bottom, GRAPH_NODE_HEIGHT);
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
    fn graph_topology_key_ignores_runtime_statistics() {
        let base = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "metrics_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                node(
                    "ingestor:http_notifications",
                    "http_notifications",
                    DataflowNodeKind::Ingestor,
                    "HTTP",
                    "user_branch",
                    0,
                    100,
                ),
                node(
                    "relay:notifications",
                    "notifications",
                    DataflowNodeKind::Relay,
                    "relay",
                    "user_branch",
                    320,
                    100,
                ),
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
                node(
                    "ingestor:http_notifications",
                    "http_notifications",
                    DataflowNodeKind::Ingestor,
                    "HTTP",
                    "user_branch",
                    0,
                    100,
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
                node(
                    "relay:notifications",
                    "notifications",
                    DataflowNodeKind::Relay,
                    "relay",
                    "user_branch",
                    320,
                    100,
                )
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
    fn graph_topology_key_changes_for_layout_updates() {
        let base = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "layout_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                unbranched_node(
                    "ingestor:http_notifications",
                    "http_notifications",
                    DataflowNodeKind::Ingestor,
                    "HTTP",
                    0,
                    100,
                ),
                unbranched_node(
                    "relay:notifications",
                    "notifications",
                    DataflowNodeKind::Relay,
                    "relay",
                    320,
                    100,
                ),
            ],
            edges: vec![edge("ingestor:http_notifications", "relay:notifications")],
        });
        let moved = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "layout_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                unbranched_node(
                    "ingestor:http_notifications",
                    "http_notifications",
                    DataflowNodeKind::Ingestor,
                    "HTTP",
                    0,
                    100,
                ),
                unbranched_node(
                    "relay:notifications",
                    "notifications",
                    DataflowNodeKind::Relay,
                    "relay",
                    420,
                    100,
                ),
            ],
            edges: vec![edge("ingestor:http_notifications", "relay:notifications")],
        });

        assert!(
            base.topology_key() != moved.topology_key(),
            "layout changes must still rerender topology"
        );
    }

    #[test]
    fn overlapping_long_edges_use_distinct_horizontal_lanes() {
        let graph = GraphView {
            id: "datalake_demo".to_string(),
            mode: "LIVE".to_string(),
            status: "RUNNING".to_string(),
            uptime: String::new(),
            statistics: GraphStatistics::default(),
            nodes: Vec::new(),
            relays: Vec::new(),
            edges: vec![
                graph_view_edge(
                    "relay:first_source",
                    "processor:first_target",
                    0,
                    100,
                    620,
                    100,
                ),
                graph_view_edge(
                    "relay:second_source",
                    "processor:second_target",
                    40,
                    100,
                    660,
                    100,
                ),
            ],
        };

        let first_lane = graph
            .edge_preferred_lane(&graph.edges[0])
            .expect("first long edge should have a preferred lane");
        let second_lane = graph
            .edge_preferred_lane(&graph.edges[1])
            .expect("second long edge should have a preferred lane");
        assert_ne!(first_lane, second_lane);

        let first_route = graph.edges[0].route_points(&graph);
        let second_route = graph.edges[1].route_points(&graph);
        assert_eq!(longest_horizontal_lane(&first_route), Some(first_lane));
        assert_eq!(longest_horizontal_lane(&second_route), Some(second_lane));
    }

    #[test]
    fn shared_source_fanout_edges_use_distinct_horizontal_lanes() {
        let graph = GraphView {
            id: "datalake_demo".to_string(),
            mode: "LIVE".to_string(),
            status: "RUNNING".to_string(),
            uptime: String::new(),
            statistics: GraphStatistics::default(),
            nodes: Vec::new(),
            relays: Vec::new(),
            edges: vec![
                graph_view_edge(
                    "relay:source_events",
                    "processor:top_target",
                    0,
                    100,
                    620,
                    80,
                ),
                graph_view_edge(
                    "relay:source_events",
                    "processor:bottom_target",
                    0,
                    100,
                    620,
                    160,
                ),
            ],
        };

        assert!(
            graph.edge_preferred_lane(&graph.edges[0]).is_none(),
            "fan-out edges whose target rows are already distinct should not get synthetic lanes"
        );
        assert!(
            graph.edge_preferred_lane(&graph.edges[1]).is_none(),
            "fan-out edges whose target rows are already distinct should not get synthetic lanes"
        );
        let first_route = graph.edges[0].route_points(&graph);
        let second_route = graph.edges[1].route_points(&graph);
        assert_ne!(
            longest_horizontal_lane(&first_route),
            longest_horizontal_lane(&second_route)
        );
        assert!(
            route_turn_count(&first_route) <= 2,
            "fan-out route should not dogleg through a synthetic lane: {first_route:?}"
        );
        assert!(
            route_turn_count(&second_route) <= 2,
            "fan-out route should not dogleg through a synthetic lane: {second_route:?}"
        );
    }

    #[test]
    fn route_graph_edge_prefers_assigned_horizontal_lane() {
        let start = GraphRoutePoint::new(40, 100);
        let end = GraphRoutePoint::new(620, 100);
        let route = route_graph_edge(start, end, &[], 700, 240, Some(118))
            .expect("edge should route through the assigned lane");

        assert_eq!(longest_horizontal_lane(&route), Some(118));
    }

    #[test]
    fn graph_edge_route_escapes_soft_margin_without_crossing_obstacle_core() {
        let obstacle = GraphRouteRect::new(100, 0, 200, 100);
        let start = GraphRoutePoint::new(95, 50);
        let end = GraphRoutePoint::new(250, 50);

        let route = route_graph_edge(start, end, &[obstacle], 300, 160, None)
            .expect("edge should route around an obstacle when the source is in its soft margin");

        assert_eq!(route.first(), Some(&start));
        assert_eq!(route.last(), Some(&end));
        assert!(
            route.len() > 2,
            "route should not use the direct segment through the obstacle"
        );
        for segment in route.windows(2) {
            assert!(
                !obstacle.intersects_core_segment(segment[0], segment[1]),
                "route segment should not cross the obstacle core: {segment:?}"
            );
        }
    }

    #[test]
    fn graph_edge_route_prefers_horizontal_first_forward_flow() {
        let start = GraphRoutePoint::new(100, 40);
        let end = GraphRoutePoint::new(280, 160);

        let route = route_graph_edge(start, end, &[], 360, 220, None)
            .expect("edge should route through an empty canvas");

        assert_eq!(route.first(), Some(&start));
        assert_eq!(route.last(), Some(&end));
        let first_segment = route
            .windows(2)
            .next()
            .expect("route must contain a first segment");
        assert_eq!(
            first_segment[0].y, first_segment[1].y,
            "forward fan-out should avoid a vertical trunk at the source"
        );
        let last_segment = route
            .windows(2)
            .last()
            .expect("route must contain a last segment");
        assert_eq!(
            last_segment[0].y, last_segment[1].y,
            "forward fan-out should enter the target from the side"
        );
        assert!(
            last_segment[1].x > last_segment[0].x,
            "forward fan-out should enter the target from the left"
        );
        assert!(
            last_segment[1].x - last_segment[0].x >= GRAPH_EDGE_TERMINAL_STRAIGHT,
            "forward fan-out should keep terminal bends away from the target"
        );
    }

    #[test]
    fn graph_edge_uses_direct_curve_when_clear() {
        let start = GraphRoutePoint::new(100, 80);
        let end = GraphRoutePoint::new(300, 180);

        let path = direct_curve_path(start, end, &[])
            .expect("clear left-to-right edges should use a direct curve");

        assert!(path.starts_with("M100 80 C"));
        assert!(path.ends_with("300 180"));
    }

    #[test]
    fn graph_edge_direct_curve_rejects_obstacle_crossing() {
        let start = GraphRoutePoint::new(100, 80);
        let end = GraphRoutePoint::new(300, 180);
        let obstacle = GraphRouteRect::new(180, 90, 240, 160);

        assert!(direct_curve_path(start, end, &[obstacle]).is_none());
    }

    #[test]
    fn graph_edge_path_rounds_middle_orthogonal_corners() {
        let path = GraphViewEdge::rounded_path(&[
            GraphRoutePoint::new(0, 20),
            GraphRoutePoint::new(100, 20),
            GraphRoutePoint::new(100, 80),
            GraphRoutePoint::new(160, 80),
            GraphRoutePoint::new(160, 130),
        ]);

        assert_eq!(
            path,
            "M0 20 L92 20 Q100 20, 100 28 L100 62 Q100 80, 118 80 L152 80 Q160 80, 160 88 L160 130"
        );
    }

    #[test]
    fn graph_edge_path_rounds_endpoint_plugs_without_eating_them() {
        let path = GraphViewEdge::rounded_path(&[
            GraphRoutePoint::new(0, 20),
            GraphRoutePoint::new(100, 20),
            GraphRoutePoint::new(100, 80),
            GraphRoutePoint::new(160, 80),
        ]);

        assert_eq!(
            path,
            "M0 20 L92 20 Q100 20, 100 28 L100 72 Q100 80, 108 80 L160 80"
        );
    }

    #[test]
    fn route_graph_edge_rejects_vertical_target_entry() {
        let start = GraphRoutePoint::new(853, 251);
        let end = GraphRoutePoint::new(984, 251);
        let route = route_graph_edge(start, end, &[], 1120, 360, Some(242))
            .expect("edge should route without a vertical target entry");
        let first_segment = route
            .windows(2)
            .next()
            .expect("route must contain a first segment");
        let last_segment = route
            .windows(2)
            .last()
            .expect("route must contain a last segment");

        assert_eq!(
            first_segment[0].y, first_segment[1].y,
            "route should leave the source horizontally: {route:?}"
        );
        assert!(
            first_segment[1].x - first_segment[0].x >= GRAPH_EDGE_TURN_X,
            "route should reserve a clear source plug: {route:?}"
        );
        assert_eq!(
            last_segment[0].y, last_segment[1].y,
            "route should enter the target horizontally: {route:?}"
        );
        assert!(
            last_segment[1].x > last_segment[0].x,
            "route should enter the target in the flow direction: {route:?}"
        );
        assert!(
            last_segment[1].x - last_segment[0].x >= GRAPH_EDGE_TERMINAL_STRAIGHT,
            "route should reserve a clear target plug: {route:?}"
        );
    }

    #[test]
    fn datalake_splitter_input_edge_stays_near_its_endpoints() {
        let graph = GraphView::from_dataflow_graph(
            DataflowGraph {
                domain: "datalake_demo".to_string(),
                statistics: DataflowStatistics::default(),
                nodes: vec![
                    node(
                        "client:mqtt_devices",
                        "mqtt_devices",
                        DataflowNodeKind::Client,
                        "MQTT",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "client:nats_edge",
                        "nats_edge",
                        DataflowNodeKind::Client,
                        "NATS",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "client:kafka_auth",
                        "kafka_auth",
                        DataflowNodeKind::Client,
                        "KAFKA",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "ingestor:iot_device_activity",
                        "iot_device_activity",
                        DataflowNodeKind::Ingestor,
                        "MQTT",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "ingestor:edge_server_activity",
                        "edge_server_activity",
                        DataflowNodeKind::Ingestor,
                        "NATS",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "ingestor:auth_server_activity",
                        "auth_server_activity",
                        DataflowNodeKind::Ingestor,
                        "KAFKA",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:device_activity_landing",
                        "device_activity_landing",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:edge_activity_landing",
                        "edge_activity_landing",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:auth_activity_landing",
                        "auth_activity_landing",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "processor:device_activity_splitter",
                        "device_activity_splitter",
                        DataflowNodeKind::Processor,
                        "deduplicator",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "processor:edge_location_lookup",
                        "edge_location_lookup",
                        DataflowNodeKind::Processor,
                        "deduplicator",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "processor:auth_activity_splitter",
                        "auth_activity_splitter",
                        DataflowNodeKind::Processor,
                        "deduplicator",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:device_connect_events",
                        "device_connect_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:device_location_events",
                        "device_location_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:device_disconnect_events",
                        "device_disconnect_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:edge_activity_enriched_landing",
                        "edge_activity_enriched_landing",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:auth_authorized_events",
                        "auth_authorized_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:auth_denied_events",
                        "auth_denied_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "processor:edge_activity_splitter",
                        "edge_activity_splitter",
                        DataflowNodeKind::Processor,
                        "deduplicator",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:edge_connect_events",
                        "edge_connect_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                    node(
                        "relay:edge_disconnect_events",
                        "edge_disconnect_events",
                        DataflowNodeKind::Relay,
                        "relay",
                        "device_branch",
                        0,
                        0,
                    ),
                ],
                edges: vec![
                    edge("client:mqtt_devices", "ingestor:iot_device_activity"),
                    edge("client:nats_edge", "ingestor:edge_server_activity"),
                    edge("client:kafka_auth", "ingestor:auth_server_activity"),
                    edge(
                        "ingestor:iot_device_activity",
                        "relay:device_activity_landing",
                    ),
                    edge(
                        "ingestor:edge_server_activity",
                        "relay:edge_activity_landing",
                    ),
                    edge(
                        "ingestor:auth_server_activity",
                        "relay:auth_activity_landing",
                    ),
                    edge(
                        "relay:device_activity_landing",
                        "processor:device_activity_splitter",
                    ),
                    edge(
                        "relay:edge_activity_landing",
                        "processor:edge_location_lookup",
                    ),
                    edge(
                        "relay:auth_activity_landing",
                        "processor:auth_activity_splitter",
                    ),
                    edge(
                        "processor:device_activity_splitter",
                        "relay:device_connect_events",
                    ),
                    edge(
                        "processor:device_activity_splitter",
                        "relay:device_location_events",
                    ),
                    edge(
                        "processor:device_activity_splitter",
                        "relay:device_disconnect_events",
                    ),
                    edge(
                        "processor:edge_location_lookup",
                        "relay:edge_activity_enriched_landing",
                    ),
                    edge(
                        "relay:edge_activity_enriched_landing",
                        "processor:edge_activity_splitter",
                    ),
                    edge(
                        "processor:auth_activity_splitter",
                        "relay:auth_authorized_events",
                    ),
                    edge(
                        "processor:auth_activity_splitter",
                        "relay:auth_denied_events",
                    ),
                    edge(
                        "processor:edge_activity_splitter",
                        "relay:edge_connect_events",
                    ),
                    edge(
                        "processor:edge_activity_splitter",
                        "relay:edge_disconnect_events",
                    ),
                ],
            }
            .laid_out(),
        );
        let edge = graph
            .edges
            .iter()
            .find(|edge| {
                edge.source == "relay:edge_activity_enriched_landing"
                    && edge.target == "processor:edge_activity_splitter"
            })
            .expect("edge_activity_splitter input edge must exist");

        let route = edge.route_points(&graph);
        let path = edge.path(&graph);
        let min_y = route.iter().map(|point| point.y).min().unwrap_or_default();
        let max_y = route.iter().map(|point| point.y).max().unwrap_or_default();
        let endpoint_min_y = route
            .first()
            .zip(route.last())
            .map(|(start, end)| start.y.min(end.y))
            .unwrap_or_default();
        let endpoint_max_y = route
            .first()
            .zip(route.last())
            .map(|(start, end)| start.y.max(end.y))
            .unwrap_or_default();

        assert!(
            min_y >= endpoint_min_y - GRAPH_EDGE_LANE_SPACING
                && max_y <= endpoint_max_y + GRAPH_EDGE_LANE_SPACING,
            "deduplicator input edge should not make a tall vertical detour: {route:?}"
        );
        assert!(
            path.contains(" C"),
            "short clear deduplicator input edge should use a direct curve instead of an \
             orthogonal dogleg: {path}"
        );
    }

    #[test]
    fn short_clear_edges_use_direct_curve_even_with_synthetic_lane() {
        let graph = GraphView {
            id: "datalake_demo".to_string(),
            mode: "LIVE".to_string(),
            status: "RUNNING".to_string(),
            uptime: String::new(),
            statistics: GraphStatistics::default(),
            nodes: Vec::new(),
            relays: Vec::new(),
            edges: vec![
                graph_view_edge(
                    "relay:edge_activity_enriched_landing",
                    "processor:edge_activity_splitter",
                    1518,
                    428,
                    1624,
                    369,
                ),
                graph_view_edge(
                    "relay:auth_activity_landing",
                    "processor:edge_activity_splitter",
                    1518,
                    428,
                    1624,
                    369,
                ),
            ],
        };
        let edge = &graph.edges[0];

        assert!(
            graph.edge_preferred_lane(edge).is_some(),
            "test setup must assign a synthetic lane"
        );
        assert!(
            edge.path(&graph).contains(" C"),
            "short clear edge should not dogleg through its synthetic lane"
        );
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

    fn node(
        id: &str,
        label: &str,
        kind: DataflowNodeKind,
        subtype: &str,
        schema: &str,
        x: i32,
        y: i32,
    ) -> DataflowNode {
        let mut node =
            DataflowNode::new(id, label, kind, subtype).with_branching_schema(schema.to_string());
        node.x = x;
        node.y = y;
        node
    }

    fn unbranched_node(
        id: &str,
        label: &str,
        kind: DataflowNodeKind,
        subtype: &str,
        x: i32,
        y: i32,
    ) -> DataflowNode {
        let mut node = DataflowNode::new(id, label, kind, subtype);
        node.x = x;
        node.y = y;
        node
    }

    fn node_with_branches(
        id: &str,
        label: &str,
        kind: DataflowNodeKind,
        subtype: &str,
        schema: &str,
        x: i32,
        y: i32,
        branches: &[&str],
    ) -> DataflowNode {
        node(id, label, kind, subtype, schema, x, y).with_branches(
            branches
                .iter()
                .map(|branch| DataflowBranchStatistics {
                    branch: (*branch).to_string(),
                    statistics: DataflowStatistics::default(),
                })
                .collect(),
        )
    }

    fn edge(source: &str, target: &str) -> DataflowEdge {
        DataflowEdge {
            source: source.to_string(),
            target: target.to_string(),
            kind: DataflowEdgeKind::Data,
            metric: None,
            statistics: DataflowStatistics::default(),
            branches: Vec::new(),
        }
    }

    fn graph_view_edge(
        source: &str,
        target: &str,
        x1: i32,
        y1: i32,
        x2: i32,
        y2: i32,
    ) -> GraphViewEdge {
        GraphViewEdge {
            source: source.to_string(),
            target: target.to_string(),
            kind: DataflowEdgeKind::Data,
            statistics: GraphStatistics::default(),
            branches: Vec::new(),
            x1,
            y1,
            x2,
            y2,
        }
    }

    fn longest_horizontal_lane(points: &[GraphRoutePoint]) -> Option<i32> {
        points
            .windows(2)
            .filter(|segment| segment[0].y == segment[1].y)
            .max_by_key(|segment| (segment[1].x - segment[0].x).abs())
            .map(|segment| segment[0].y)
    }

    fn route_turn_count(points: &[GraphRoutePoint]) -> usize {
        points
            .windows(3)
            .filter(|window| {
                let incoming = (window[1].x - window[0].x, window[1].y - window[0].y);
                let outgoing = (window[2].x - window[1].x, window[2].y - window[1].y);
                incoming.0.signum() != outgoing.0.signum()
                    || incoming.1.signum() != outgoing.1.signum()
            })
            .count()
    }
}
