use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque, btree_map::Entry},
    num::NonZeroU64,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_util::{
    FutureExt, SinkExt, StreamExt,
    future::{AbortHandle, Abortable},
};
use gloo_net::websocket::{
    Message as WebSocketMessage, State as WebSocketState, futures::WebSocket,
};
use leptos::{ev, mount::mount_to_body, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::{ApproxInto as _, CheckedApproxInto as _};
use nervix_client_wire::{
    AttachDisposition, AttachOutcome, AttachTransactionRequest, CancellationStage, ClientMessage,
    ClientRequest, ClusterObserved, CommandDisposition, CommandOutcome, CommandRequest, Diagnostic,
    DomainEntity, DomainInfo, DomainSelection, DomainSnapshotObserved, LeaderRedirect, Leadership,
    NoticeLevel, ReplyBody, RequestCancelled, RequestId, RowBatchView, RowSchema,
    SelectDomainRequest, ServerEvent, ServerFrame, ServerMessage, ServerNotice, SessionEndReason,
    SessionLimits, StatementDisposition, StatementOutcome, SubscribeDisposition, SubscribeOutcome,
    SubscribeRequest, SubscriptionHandle, SubscriptionOpened, SubscriptionRows, SubscriptionType,
    SuggestRequest, TransferAssembly, TransferPart, UnsubscribeDisposition, UnsubscribeOutcome,
    UnsubscribeRequest, VerifiedFrame,
    websocket::{ClientWebSocketCodec, WebSocketData},
};
use nervix_dataflow_graph::{
    DataflowBranch, DataflowEdgeKind, DataflowGraph, DataflowInputSide, DataflowNodeKind,
    DataflowNodeRole, DataflowNodeStatus, DataflowProcessorKind, DataflowSchemaField,
    DataflowStatistics,
};
use nervix_models::{
    ClusterNodeName, CommandExecutionReference, DomainName, DomainPace, DomainStatus, ModelKind,
    Statement, SubscriptionName, TransactionLifecycle, TransactionStatus,
};
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statement, parse_client_statements, parse_use_domain,
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use nervix_web_console::graph::{
    GraphEdgeId, GraphSearch, LiveGraphLayout, graph_layout_edge, graph_layout_item,
    layout::{EdgeTravel, GroupRegion, Rect},
    viewport::{Extent, GraphBounds, Viewport},
};
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

const RUNTIME_VERSION_LABEL: &str = concat!("nervix runtime v", env!("CARGO_PKG_VERSION"));
const SUGGESTION_REQUEST_DEBOUNCE_DELAY: Duration = Duration::from_millis(50);
const WEBSOCKET_INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const WEBSOCKET_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const SUBSCRIPTION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The limits every frame of the console's session is held to.
const SESSION_LIMITS: SessionLimits = SessionLimits::DEFAULT;

/// What a request shows when the server answered it with a reply meant for another kind of
/// request.
const UNEXPECTED_REPLY: &str = "the server answered with a reply of another kind";

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsoleConnectionState {
    Connecting,
    Connected,
    Waiting,
}

#[derive(Clone)]
struct WebConsoleSession {
    state: RwSignal<ConsoleConnectionState>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    upload_base_url: RwSignal<Option<String>>,
    auth_token: RwSignal<Option<String>>,
}

#[derive(Clone, Copy)]
struct WebConsoleSignals {
    terminal_lines: RwSignal<TermLineHistory>,
    suggestions: RwSignal<Vec<String>>,
    domain_snapshots: RwSignal<BTreeMap<DomainName, DomainSnapshotView>>,
    cluster_counters: RwSignal<ClusterCounters>,
    active_domain: RwSignal<Option<DomainName>>,
    transaction_status: RwSignal<Option<TransactionStatus>>,
    domains: RwSignal<Vec<DomainView>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    domains_loaded: RwSignal<bool>,
    auth_token: RwSignal<Option<String>>,
    auth_error: RwSignal<Option<String>>,
}

impl WebConsoleSignals {
    /// A replacement credential starts a separate view of the cluster. No tab, suggestion, or
    /// observed graph from the previous identity may remain visible to the new session.
    fn clear_authenticated_view(self) {
        self.active_domain.set(None);
        self.transaction_status.set(None);
        self.domains.set(Vec::new());
        self.domain_snapshots.set(BTreeMap::new());
        self.resource_details.set(BTreeMap::new());
        self.cluster_counters.set(ClusterCounters::default());
        self.domains_loaded.set(false);
        self.subscription_tabs.set(Vec::new());
        self.active_subscription_tab.set(None);
        self.suggestions.set(Vec::new());
        self.terminal_lines.set(TermLineHistory::default());
    }

    /// Closing a pending start waits for its reply before deleting that subscription. An opened
    /// stream stays visible as closing until its unsubscribe reply names the same generation.
    fn begin_subscription_close(self, tab_id: u64) -> Option<UnsubscribeRequest> {
        let tab = self
            .subscription_tabs
            .get_untracked()
            .into_iter()
            .find(|tab| tab.id == tab_id)?;
        let stream = match tab.state {
            SubscriptionTabState::Pending | SubscriptionTabState::Restoring => {
                self.subscription_tabs.update(|tabs| {
                    if let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) {
                        tab.state = SubscriptionTabState::Closing(None);
                    }
                });
                return None;
            }
            SubscriptionTabState::Interrupted => {
                remove_subscription_tab(
                    self.subscription_tabs,
                    self.active_subscription_tab,
                    tab_id,
                );
                return None;
            }
            SubscriptionTabState::Open(stream) => stream,
            SubscriptionTabState::Closing(_) => return None,
        };
        self.subscription_tabs.update(|tabs| {
            if let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) {
                tab.state = SubscriptionTabState::Closing(Some(stream));
            }
        });
        Some(UnsubscribeRequest {
            subscription: tab.name,
        })
    }
}

/// A request the console sends over its session, together with what its reply is for.
#[derive(Clone)]
enum ConsoleRequest {
    /// Executes NSPL statements.
    Command {
        request: CommandRequest,
        purpose: CommandPurpose,
    },
    /// `LIST DOMAINS` typed in the REPL. The reply updates the domain list and is printed.
    ListDomains,
    /// Opens the subscription the tab `tab_id` shows.
    SubscriptionStart {
        tab_id: u64,
        request: SubscribeRequest,
    },
    /// Closes the subscription of a tab the operator closed.
    SubscriptionStop {
        tab_id: u64,
        request: UnsubscribeRequest,
    },
    /// Selects the domain whose observations the session receives.
    SelectDomain(SelectDomainRequest),
    /// Asks for completions of the REPL input.
    Suggest(SuggestRequest),
    /// Binds the session's transaction to the connection.
    AttachTransaction(AttachTransactionRequest),
}

/// Who reads the outcome of a command.
#[derive(Clone)]
enum CommandPurpose {
    /// The REPL prints it.
    Repl,
    /// The resource dialog reads the versions of `resource` from its `DESCRIBE RESOURCE` text.
    ResourceDescription { resource: String },
}

impl ConsoleRequest {
    /// Whether the request keeps its place in the order the console issued requests: it waits
    /// until the session can serve it and outlives a connection that ended before answering it.
    ///
    /// Every connection selects the active domain and attaches the session's transaction again
    /// itself, and a completion request only matters while the operator is typing, so those are
    /// sent at once and forgotten with the connection.
    fn is_ordered(&self) -> bool {
        match self {
            Self::Command { .. }
            | Self::ListDomains
            | Self::SubscriptionStart { .. }
            | Self::SubscriptionStop { .. } => true,
            Self::SelectDomain(_) | Self::Suggest(_) | Self::AttachTransaction(_) => false,
        }
    }

    /// The wire request that carries this request.
    fn client_request(&self) -> ClientRequest {
        match self {
            Self::Command { request, .. } => ClientRequest::Command(request.clone()),
            Self::ListDomains => ClientRequest::ListDomains,
            Self::SubscriptionStart { request, .. } => ClientRequest::Subscribe(request.clone()),
            Self::SubscriptionStop { request, .. } => ClientRequest::Unsubscribe(request.clone()),
            Self::SelectDomain(request) => ClientRequest::SelectDomain(request.clone()),
            Self::Suggest(request) => ClientRequest::Suggest(request.clone()),
            Self::AttachTransaction(request) => ClientRequest::AttachTransaction(request.clone()),
        }
    }
}

/// The place of a request in the order the console issued its requests. A request keeps it when it
/// is sent again, so requests sent again on a new connection keep their original order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct IssueOrder(u64);

/// A request and its place in the issue order.
struct IssuedRequest {
    order: IssueOrder,
    request: ConsoleRequest,
}

/// Where a server message goes.
enum Routed {
    /// An unsolicited message. It never answers a request.
    Event(ServerEvent),
    /// The terminal reply to a request that awaited it.
    Reply(Box<AnsweredRequest>),
    /// A reply that could not be read. The request it answers is over.
    Unreadable(Box<UnreadableReply>),
    /// A reply to a request the console no longer awaits.
    Untracked,
    /// A part of a reply that is still arriving.
    Pending,
}

/// A request and the terminal reply that answers it.
struct AnsweredRequest {
    request: IssuedRequest,
    body: ReplyBody,
}

/// A request whose reply could not be read, and why.
struct UnreadableReply {
    request: IssuedRequest,
    reason: String,
}

/// Where every request of the console's session stands: waiting until the session can serve it,
/// or sent on the current connection and awaiting its terminal reply.
///
/// A reply names the request it answers, so replies are paired with their requests by identity
/// and never by arrival order. Unsolicited server messages answer no request, so they never
/// complete or discard one.
struct SessionRequests {
    /// The place the next issued request takes.
    next_issue: u64,
    /// Ordered requests waiting until the session can serve them, in the order they were issued.
    held: BTreeMap<IssueOrder, ConsoleRequest>,
    /// The identity the next request sent on the current connection carries.
    next_request_id: NonZeroU64,
    /// The requests sent on the current connection that await their terminal reply, by the
    /// identity the reply carries. Identities only increase, so iteration is send order.
    in_flight: BTreeMap<RequestId, IssuedRequest>,
    /// The replies too large for one frame whose parts are still arriving.
    transfers: BTreeMap<RequestId, TransferAssembly>,
    /// The latest completion request. An earlier one is no longer awaited, so its stale
    /// suggestions are never shown.
    latest_suggestion: Option<RequestId>,
    /// Whether the server confirmed that the node serving the connection leads the cluster.
    leader_confirmed: bool,
    /// The request attaching the session's transaction to the connection, while it is in flight.
    attaching: Option<RequestId>,
}

impl SessionRequests {
    fn new() -> Self {
        Self {
            next_issue: 0,
            held: BTreeMap::new(),
            next_request_id: NonZeroU64::MIN,
            in_flight: BTreeMap::new(),
            transfers: BTreeMap::new(),
            latest_suggestion: None,
            leader_confirmed: false,
            attaching: None,
        }
    }

    /// Gives a request the next place in the issue order.
    fn issue(&mut self, request: ConsoleRequest) -> IssuedRequest {
        let order = IssueOrder(self.next_issue);
        self.next_issue = self
            .next_issue
            .checked_add(1)
            .assured("a console session cannot issue 2^64 requests");
        IssuedRequest { order, request }
    }

    /// Whether ordered requests can be sent: the connection is served by the leader, and the
    /// session's transaction is not being attached.
    fn is_ready(&self) -> bool {
        self.leader_confirmed && self.attaching.is_none()
    }

    /// Whether an attach of the session's transaction is in flight.
    fn is_attaching(&self) -> bool {
        self.attaching.is_some()
    }

    /// Records that the node serving the connection leads the cluster.
    fn confirm_leader(&mut self) {
        self.leader_confirmed = true;
    }

    /// Takes a newly issued request. An ordered request waits while the session is not ready;
    /// anything else is sent at once, and the returned message carries it.
    fn accept(&mut self, issued: IssuedRequest) -> Option<ClientMessage> {
        if issued.request.is_ordered() && !self.is_ready() {
            self.held.insert(issued.order, issued.request);
            return None;
        }
        Some(self.dispatch(issued))
    }

    /// Registers a request as sent on the current connection and returns the message that
    /// carries it, under a fresh identity.
    fn dispatch(&mut self, issued: IssuedRequest) -> ClientMessage {
        let request_id = RequestId::new(self.next_request_id);
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .assured("a console connection cannot send 2^64 requests");
        match &issued.request {
            ConsoleRequest::Suggest(_) => {
                if let Some(previous) = self.latest_suggestion.replace(request_id) {
                    self.in_flight.remove(&previous);
                }
            }
            ConsoleRequest::AttachTransaction(_) => {
                self.attaching = Some(request_id);
            }
            ConsoleRequest::Command { .. }
            | ConsoleRequest::ListDomains
            | ConsoleRequest::SubscriptionStart { .. }
            | ConsoleRequest::SubscriptionStop { .. }
            | ConsoleRequest::SelectDomain(_) => {}
        }
        let message = ClientMessage {
            request_id,
            request: issued.request.client_request(),
        };
        self.in_flight.insert(request_id, issued);
        message
    }

    /// Dispatches the held requests in the order they were issued, once the session is ready.
    fn release_held(&mut self) -> Vec<ClientMessage> {
        if !self.is_ready() {
            return Vec::new();
        }
        let held = std::mem::take(&mut self.held);
        let mut messages = Vec::with_capacity(held.len());
        for (order, request) in held {
            messages.push(self.dispatch(IssuedRequest { order, request }));
        }
        messages
    }

    /// Holds a request that has to be sent again, in its original place in the issue order.
    fn hold_again(&mut self, issued: IssuedRequest) {
        self.held.insert(issued.order, issued.request);
    }

    /// Drops the held requests, which can no longer be served as they were issued.
    fn clear_held(&mut self) {
        self.held.clear();
    }

    /// Pairs a server message with the request it answers. An event answers no request.
    fn route(&mut self, message: ServerMessage) -> Routed {
        match message {
            ServerMessage::Event(event) => Routed::Event(event),
            ServerMessage::Reply(reply) => match self.answer(reply.request_id) {
                Some(request) => Routed::Reply(Box::new(AnsweredRequest {
                    request,
                    body: reply.body,
                })),
                None => Routed::Untracked,
            },
            ServerMessage::TransferPart(part) => self.assemble(&part),
        }
    }

    /// Ends the request `request_id` names, when it awaits its reply.
    fn answer(&mut self, request_id: RequestId) -> Option<IssuedRequest> {
        let request = self.in_flight.remove(&request_id)?;
        self.transfers.remove(&request_id);
        if self.attaching == Some(request_id) {
            self.attaching = None;
        }
        if self.latest_suggestion == Some(request_id) {
            self.latest_suggestion = None;
        }
        Some(request)
    }

    /// Adds one part to the reply it belongs to, and routes the reply once it is complete.
    fn assemble(&mut self, part: &TransferPart) -> Routed {
        let request_id = part.request_id();
        if !self.in_flight.contains_key(&request_id) {
            return Routed::Untracked;
        }
        let assembly = match self.transfers.entry(request_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                entry.insert(TransferAssembly::new(request_id, &SESSION_LIMITS))
            }
        };
        let appended = assembly.append(part);
        let complete = assembly.is_complete();
        if let Err(error) = appended {
            let request = self
                .answer(request_id)
                .verified("the request was found in flight above");
            return Routed::Unreadable(Box::new(UnreadableReply {
                request,
                reason: format!("failed to reassemble a reply: {}", error.current_context()),
            }));
        }
        if !complete {
            return Routed::Pending;
        }
        let assembly = self
            .transfers
            .remove(&request_id)
            .verified("the assembly was completed above");
        let request = self
            .answer(request_id)
            .verified("the request was found in flight above");
        match assembly.finish() {
            Ok(reply) => Routed::Reply(Box::new(AnsweredRequest {
                request,
                body: reply.body,
            })),
            Err(error) => Routed::Unreadable(Box::new(UnreadableReply {
                request,
                reason: format!("failed to reassemble a reply: {}", error.current_context()),
            })),
        }
    }

    /// Forgets the connection that ended. Every ordered request it left unanswered waits for the
    /// next connection in its place in the issue order; a command keeps its execution reference,
    /// so the server recovers its recorded outcome instead of executing it twice.
    fn end_connection(&mut self) {
        let in_flight = std::mem::take(&mut self.in_flight);
        for issued in in_flight.into_values() {
            if issued.request.is_ordered()
                && !matches!(&issued.request, ConsoleRequest::SubscriptionStop { .. })
            {
                self.hold_again(issued);
            }
        }
        self.held
            .retain(|_, request| !matches!(request, ConsoleRequest::SubscriptionStop { .. }));
        self.transfers.clear();
        self.next_request_id = NonZeroU64::MIN;
        self.latest_suggestion = None;
        self.leader_confirmed = false;
        self.attaching = None;
    }
}

/// What the session loop does after a server message was applied.
enum SessionStep {
    /// Keep serving the connection.
    Continue,
    /// Attach the session's transaction again. The requests held for it follow once it is
    /// attached.
    Reattach { transaction_id: String },
    /// End the connection and continue the session at the leader's web console.
    Redirect(Url),
    /// End the connection and connect again at the same address after the reconnect delay.
    Reconnect,
}

/// How a connection ended.
enum ConnectionEnd {
    /// The connection closed or failed. The next one opens at the same address.
    Dropped,
    /// The session continues at the leader's web console.
    Redirected(Url),
    /// The console stopped issuing requests, so the session is over.
    ConsoleClosed,
}

#[derive(Clone)]
struct SubscriptionTabView {
    id: u64,
    state: SubscriptionTabState,
    name: SubscriptionName,
    domain: DomainName,
    relay: String,
    filter: String,
    sample_rate_index: usize,
    title: String,
    subscribe_command: String,
    lines: TermLineHistory,
}

/// An opened subscription and the schema its rows follow.
#[derive(Clone)]
struct TabStream {
    subscription: SubscriptionHandle,
    schema: RowSchema,
}

impl SubscriptionTabView {
    /// Whether the tab shows `subscription`. A name reused after deletion has a new generation, so
    /// messages about an earlier subscription never reach a later tab.
    fn streams(&self, subscription: &SubscriptionHandle) -> bool {
        matches!(&self.state, SubscriptionTabState::Open(stream) if stream.subscription == *subscription)
    }

    /// The schema of `subscription`'s rows, when the tab shows that subscription.
    fn stream_schema(&self, subscription: &SubscriptionHandle) -> Option<&RowSchema> {
        let SubscriptionTabState::Open(stream) = &self.state else {
            return None;
        };
        if stream.subscription != *subscription {
            return None;
        }
        Some(&stream.schema)
    }
}

#[derive(Clone)]
enum SubscriptionTabState {
    Pending,
    Open(TabStream),
    Interrupted,
    Restoring,
    Closing(Option<TabStream>),
}

impl SubscriptionTabState {
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Open(_) => "active",
            Self::Interrupted => "interrupted",
            Self::Restoring => "restoring",
            Self::Closing(_) => "closing",
        }
    }

    fn can_activate(&self) -> bool {
        match self {
            Self::Open(_) | Self::Interrupted | Self::Restoring | Self::Closing(Some(_)) => true,
            Self::Pending | Self::Closing(None) => false,
        }
    }
}

#[derive(Clone, Default)]
struct ResourceDetailView {
    versions: Vec<ResourceVersionView>,
    status: String,
}

/// Everything one version row of the resource dialog shows. A keyed list re-renders a row only
/// when its key changes, so the dialog keys each row by this whole value: a later description
/// that changes the row, such as the usages a rebinding moved, replaces it.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceVersionView {
    version: u64,
    root_checksum: Option<String>,
    manifest_checksum: Option<String>,
    file_count: Option<String>,
    total_bytes: Option<String>,
    created_by_node: Option<ClusterNodeName>,
    created_at: Option<String>,
    files: Vec<ResourceFileView>,
    usages: Vec<ResourceUsageView>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceFileView {
    path: String,
    entry_type: String,
    size: Option<String>,
    checksum: Option<String>,
}

/// One model bound to a resource version.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceUsageView {
    /// The model kind as a statement names it, such as `HASH MAP`.
    kind: String,
    name: String,
}

/// One line of the `usages` section of `DESCRIBE RESOURCE`: a bound model and the version it
/// pins.
struct ResourceUsageDetail {
    version: u64,
    usage: ResourceUsageView,
}

/// The part of a `DESCRIBE RESOURCE` description a line belongs to. The version and usage lists
/// share the `- key=value` line shape, so a line is read by the section that holds it.
#[derive(Clone, Copy)]
enum ResourceDescribeSection {
    /// The `resource`, `latest` and `versions` summary lines.
    Summary,
    /// `version_details`: one line per version, each followed by its entries.
    VersionDetails,
    /// `usages`: one line per model bound to the resource.
    Usages,
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
    let active_domain = RwSignal::new(None::<DomainName>);
    let domains = RwSignal::new(Vec::<DomainView>::new());
    let active_theme = RwSignal::new(0_usize);
    let input = RwSignal::new(String::new());
    let terminal_lines = RwSignal::new(TermLineHistory::default());
    let transaction_status = RwSignal::new(None::<TransactionStatus>);
    let subscription_tabs = RwSignal::new(Vec::<SubscriptionTabView>::new());
    let active_subscription_tab = RwSignal::new(None::<u64>);
    let next_subscription_tab_id = RwSignal::new(1_u64);
    let suggestions = RwSignal::new(Vec::<String>::new());
    let domain_snapshots = RwSignal::new(BTreeMap::<DomainName, DomainSnapshotView>::new());
    let cluster_counters = RwSignal::new(ClusterCounters::default());
    let resource_details = RwSignal::new(BTreeMap::<String, ResourceDetailView>::new());
    let domains_loaded = RwSignal::new(false);
    let auth_token = RwSignal::new(web_console_auth_token_from_location());
    let auth_error = RwSignal::new(None::<String>);
    let signals = WebConsoleSignals {
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
        auth_token,
        auth_error,
    };
    let web_console_session = use_websocket_session(signals);

    let active_domain_name = move || match active_domain.get() {
        Some(domain) => domain.to_string(),
        None => String::new(),
    };
    let active_graph = move || {
        let active = active_domain.get()?;
        let graph = {
            let snapshots = domain_snapshots.read();
            let snapshot = snapshots.get(&active)?;
            snapshot.dataflow_graph.clone()
        };
        if graph.nodes.is_empty() {
            return None;
        }
        Some(GraphView::from_dataflow_graph(graph))
    };
    let active_entities = move || {
        let Some(active) = active_domain.get() else {
            return Vec::new();
        };
        let snapshots = domain_snapshots.read();
        match snapshots.get(&active) {
            Some(snapshot) => snapshot.entities.clone(),
            None => Vec::new(),
        }
    };
    let active_domain_session = web_console_session.clone();
    Effect::new(move |_| {
        let Some(domain) = active_domain.get() else {
            return;
        };
        let queued = ConsoleRequest::SelectDomain(SelectDomainRequest { domain });
        if let Some(request_tx) = active_domain_session.request_tx.get_untracked() {
            request_tx
                .unbounded_send(queued)
                .means_shutdown("web console session");
        }
    });
    let suggestion_request_sequence = RwSignal::new(0_u64);
    let suggestion_session = web_console_session.clone();
    let request_suggestions = move |value: String| {
        suggestion_request_sequence.update(|sequence| {
            *sequence = sequence
                .checked_add(1)
                .assured("a console session cannot request 2^64 suggestions");
        });
        let request_sequence = suggestion_request_sequence.get_untracked();
        if !domains_loaded.get_untracked() {
            suggestions.set(Vec::new());
            return;
        }
        let domain = active_domain.get_untracked();
        let auth_at_schedule = suggestion_session.auth_token.get_untracked();
        spawn_local(async move {
            wait_for_browser_delay(SUGGESTION_REQUEST_DEBOUNCE_DELAY).await;
            if suggestion_request_sequence.get_untracked() != request_sequence
                || suggestion_session.auth_token.get_untracked() != auth_at_schedule
            {
                return;
            }
            let cursor = value.len();
            let request = SuggestRequest::new(value, cursor, domain)
                .assured("the end of the input is always a character boundary");
            let queued = ConsoleRequest::Suggest(request);
            if let Some(request_tx) = suggestion_session.request_tx.get_untracked()
                && request_tx.unbounded_send(queued).is_err()
            {
                suggestions.set(Vec::new());
            }
        });
    };

    let run_command = move |next_command: Option<String>| {
        suggestion_request_sequence.update(|sequence| {
            *sequence = sequence
                .checked_add(1)
                .assured("a console session cannot request 2^64 suggestions");
        });
        let command = next_command
            .unwrap_or_else(|| input.get())
            .trim()
            .to_string();
        if command.is_empty() {
            return;
        }
        let prompt_transaction =
            transaction_status.with_untracked(|status| ActiveTransaction::of(status.as_ref()));
        terminal_lines.update(|lines| {
            lines.push(TermLine::prompt(command.clone(), prompt_transaction));
        });
        if command.eq_ignore_ascii_case("clear") {
            terminal_lines.set(TermLineHistory::default());
            input.set(String::new());
            return;
        }
        let transaction_active =
            transaction_status.with_untracked(|status| transaction_is_active(status.as_ref()));
        if let Ok(ClientStatement::ListDomains) = parse_client_statement(&command) {
            if transaction_active {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            if let Some(request_tx) = web_console_session.request_tx.get_untracked()
                && request_tx
                    .unbounded_send(ConsoleRequest::ListDomains)
                    .is_err()
            {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error("websocket command channel is closed"));
                });
            }
        } else if let Ok(domain) = parse_use_domain(&command) {
            if transaction_active {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            let listed = {
                let listed_domains = domains.read_untracked();
                // Bounded by the domains of the cluster, which the domain menu lists in this
                // order.
                listed_domains
                    .iter()
                    .any(|candidate| candidate.domain == domain)
            };
            if listed {
                active_domain.set(Some(domain.clone()));
                terminal_lines.update(|lines| {
                    lines.push(TermLine::info(format!("using domain '{domain}'")));
                });
            } else {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(format!(
                        "domain '{domain}' is not present in this console view"
                    )));
                });
            }
        } else {
            let request_domain = active_domain.get_untracked();
            if request_domain.is_none() && !is_domainless_server_command(&command) {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error("no active domain selected"));
                });
                suggestions.set(Vec::new());
                input.set(String::new());
                return;
            }
            let transaction = transaction_status.get_untracked();
            let expected_transaction_position = match &transaction {
                Some(status) if status.lifecycle().is_active() => {
                    Some(status.accepted_operations())
                }
                Some(_) | None => None,
            };
            let request = CommandRequest {
                query: command.clone(),
                domain: request_domain,
                execution_reference: command_execution_reference(),
                expected_transaction_position,
                expected_preview: None,
            };
            let queued = ConsoleRequest::Command {
                request,
                purpose: CommandPurpose::Repl,
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
        // Bounded by the subscription tabs the operator has open in this console, and the tab
        // strip renders them in this order.
        if let Some(existing) = subscription_tabs.get_untracked().into_iter().find(|tab| {
            tab.domain == domain
                && tab.relay == relay
                && tab.filter == filter
                && tab.sample_rate_index == sample_rate_index
        }) {
            if matches!(
                existing.state,
                SubscriptionTabState::Open(_)
                    | SubscriptionTabState::Interrupted
                    | SubscriptionTabState::Restoring
                    | SubscriptionTabState::Closing(Some(_))
            ) {
                active_subscription_tab.set(Some(existing.id));
            }
            return;
        }
        let tab_id = next_subscription_tab_id.get_untracked();
        let next_tab_id = tab_id
            .checked_add(1)
            .assured("a console session cannot open 2^64 subscription tabs");
        next_subscription_tab_id.set(next_tab_id);
        let name = SubscriptionName::parse(&format!("web_console_subscription_{tab_id}"))
            .assured("lower-case letters, underscores and at most 20 digits form a valid name");
        let subscribe_command =
            subscribe_session_command(name.as_str(), &relay, &filter, sample_rate_index);
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
                lines: TermLineHistory::default(),
            });
        });
        let request = SubscribeRequest {
            domain,
            statement: subscribe_command,
            subscription_type: SubscriptionType::Row,
        };
        if let Some(request_tx) = subscription_session.request_tx.get_untracked() {
            if request_tx
                .unbounded_send(ConsoleRequest::SubscriptionStart { tab_id, request })
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
        let Some(request) = signals.begin_subscription_close(tab_id) else {
            return;
        };
        if let Some(request_tx) = stop_subscription_session.request_tx.get_untracked()
            && request_tx
                .unbounded_send(ConsoleRequest::SubscriptionStop { tab_id, request })
                .is_ok()
        {
            return;
        }
        restore_failed_unsubscribe(
            subscription_tabs,
            tab_id,
            "websocket session is not available".to_string(),
        );
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
                    <Sidebar active_domain=active_domain domains=domains domains_loaded=domains_loaded active_graph=active_graph active_entities=active_entities cluster_counters=cluster_counters resource_details=resource_details web_console_session=web_console_session.clone() run_command=run_command />
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
                            transaction_state=move || transaction_status.with(|status| ActiveTransaction::of(status.as_ref()))
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
    let state = RwSignal::new(ConsoleConnectionState::Connecting);
    let upload_base_url = RwSignal::new(web_console_http_base_url());
    let (sender, receiver) = unbounded::<ConsoleRequest>();
    let request_tx = RwSignal::new(Some(sender));
    let (abort, registration) = AbortHandle::new_pair();
    spawn_local(async move {
        Abortable::new(
            run_websocket_session(signals, state, upload_base_url, receiver),
            registration,
        )
        .await
        .discarded("the console owner aborts its session when its browser view is removed");
    });
    on_cleanup(move || {
        abort.abort();
        request_tx.set(None);
    });
    WebConsoleSession {
        state,
        request_tx,
        upload_base_url,
        auth_token: signals.auth_token,
    }
}

/// Keeps the console's session open: connects, serves each connection until it ends, and connects
/// again, at the leader's web console when the server names it.
async fn run_websocket_session(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    upload_base_url: RwSignal<Option<String>>,
    mut queued: UnboundedReceiver<ConsoleRequest>,
) {
    let WebConsoleSignals {
        domains_loaded,
        auth_token,
        auth_error,
        ..
    } = signals;
    let mut reconnect_delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
    let mut requests = SessionRequests::new();
    let mut redirected_url = None::<String>;
    let mut session_auth_token = auth_token.get_untracked();
    loop {
        let next_auth_token = auth_token.get_untracked();
        if next_auth_token != session_auth_token {
            let previous_was_authenticated = session_auth_token.is_some();
            session_auth_token = next_auth_token.clone();
            redirected_url = None;
            if previous_was_authenticated {
                requests = SessionRequests::new();
                while queued.try_recv().is_ok() {}
                upload_base_url.set(web_console_http_base_url());
                signals.clear_authenticated_view();
            }
        }
        let Some(current_auth_token) = auth_token.get_untracked() else {
            state.set(ConsoleConnectionState::Waiting);
            domains_loaded.set(false);
            requests.clear_held();
            redirected_url = None;
            wait_for_browser_delay(WEBSOCKET_INITIAL_RECONNECT_DELAY).await;
            continue;
        };
        let url = match &redirected_url {
            Some(url) => Some(url.clone()),
            None => web_console_websocket_url(&current_auth_token),
        };
        let Some(url) = url else {
            state.set(ConsoleConnectionState::Waiting);
            wait_for_browser_delay(reconnect_delay).await;
            reconnect_delay = (reconnect_delay * 2).min(WEBSOCKET_MAX_RECONNECT_DELAY);
            continue;
        };
        state.set(ConsoleConnectionState::Connecting);
        domains_loaded.set(false);
        let mut opened_this_attempt = false;
        match WebSocket::open(&url) {
            Ok(socket) => {
                wait_for_websocket_open(&socket).await;
                if auth_token.get_untracked().as_deref() != Some(current_auth_token.as_str()) {
                    requests.end_connection();
                    interrupt_subscription_tabs(signals);
                    continue;
                }
                let ended = if let WebSocketState::Open = socket.state() {
                    Some(
                        serve_connection(
                            signals,
                            state,
                            socket,
                            &mut requests,
                            &mut queued,
                            &current_auth_token,
                        )
                        .await,
                    )
                } else {
                    // A server that ends the session at once, such as a follower redirecting to the
                    // leader, can close the connection before this loop sees it open. Its frames
                    // are still buffered, and only a connection that never opened delivers none.
                    drain_closed_connection(
                        signals,
                        state,
                        socket,
                        &mut requests,
                        &current_auth_token,
                    )
                    .await
                };
                if let Some(ended) = ended {
                    if auth_token.get_untracked().as_deref() != Some(current_auth_token.as_str()) {
                        requests.end_connection();
                        interrupt_subscription_tabs(signals);
                        continue;
                    }
                    opened_this_attempt = true;
                    reconnect_delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
                    auth_error.set(None);
                    requests.end_connection();
                    if !matches!(&ended, ConnectionEnd::ConsoleClosed) {
                        interrupt_subscription_tabs(signals);
                    }
                    match ended {
                        ConnectionEnd::Dropped => {}
                        ConnectionEnd::Redirected(leader) => {
                            upload_base_url.set(Some(leader.to_string()));
                            redirected_url =
                                web_console_websocket_url_from_base(&leader, &current_auth_token);
                        }
                        ConnectionEnd::ConsoleClosed => {
                            state.set(ConsoleConnectionState::Waiting);
                            return;
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
            && credentials_invalid(&current_auth_token).await
            && auth_token.get_untracked().as_deref() == Some(current_auth_token.as_str())
        {
            auth_error.set(Some("Authentication failed".to_string()));
            auth_token.set(None);
            requests.clear_held();
            redirected_url = None;
            continue;
        }
        state.set(ConsoleConnectionState::Waiting);
        wait_for_browser_delay(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(WEBSOCKET_MAX_RECONNECT_DELAY);
    }
}

/// A failed WebSocket handshake does not tell browser JavaScript whether the server rejected the
/// credentials or was unreachable. The same-origin authentication probe does: a valid
/// credential gets `204 No Content`, and a rejected credential gets `401 Unauthorized` without a
/// browser-managed Basic authentication challenge.
/// Network failures and a slow probe leave the token in place for the next reconnect attempt.
async fn credentials_invalid(auth_token: &str) -> bool {
    let Some(base) = web_console_http_base_url() else {
        return false;
    };
    let Ok(mut url) = Url::parse(&base) else {
        return false;
    };
    url.set_path("/console/auth");
    url.query_pairs_mut().append_pair("auth", auth_token);
    let response = gloo_net::http::Request::get(url.as_str()).send().fuse();
    let timeout = wait_for_browser_delay(Duration::from_secs(3)).fuse();
    futures_util::pin_mut!(response, timeout);
    match futures_util::select! {
        result = response => Some(result),
        () = timeout => None,
    } {
        Some(Ok(response)) => response.status() == 401,
        Some(Err(_)) | None => false,
    }
}

/// Reads the frames a connection delivered before it closed, without sending anything on it.
///
/// `None` says no frame arrived, so the connection never opened, as when the server refused its
/// credentials; otherwise the connection ends the way its frames say.
async fn drain_closed_connection(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    mut socket: WebSocket,
    requests: &mut SessionRequests,
    current_auth_token: &str,
) -> Option<ConnectionEnd> {
    let codec = ClientWebSocketCodec::new(SESSION_LIMITS);
    let mut received = false;
    while let Some(Ok(message)) = socket.next().await {
        if signals.auth_token.get_untracked().as_deref() != Some(current_auth_token) {
            return Some(ConnectionEnd::Dropped);
        }
        let WebSocketMessage::Bytes(payload) = message else {
            continue;
        };
        let Ok(frame) = codec.decode(WebSocketData::Binary(Bytes::from(payload))) else {
            continue;
        };
        received = true;
        match receive_frame(signals, state, requests, &frame) {
            SessionStep::Redirect(leader) => return Some(ConnectionEnd::Redirected(leader)),
            SessionStep::Continue | SessionStep::Reattach { .. } | SessionStep::Reconnect => {}
        }
    }
    if received {
        return Some(ConnectionEnd::Dropped);
    }
    None
}

/// Serves one connection until it ends.
///
/// The connection first selects the active domain again, so the domain's observations resume, and
/// attaches the session's transaction again. Ordered requests wait until the server confirms that
/// the serving node leads and the transaction is attached, and then go out in the order the
/// console issued them.
async fn serve_connection(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    mut socket: WebSocket,
    requests: &mut SessionRequests,
    queued: &mut UnboundedReceiver<ConsoleRequest>,
    current_auth_token: &str,
) -> ConnectionEnd {
    let codec = ClientWebSocketCodec::new(SESSION_LIMITS);
    let mut opening = Vec::new();
    if let Some(domain) = signals.active_domain.get_untracked() {
        opening.push(ConsoleRequest::SelectDomain(SelectDomainRequest { domain }));
    }
    let transaction_id = signals
        .transaction_status
        .with_untracked(|status| transaction_to_attach(status.as_ref()));
    if let Some(transaction_id) = transaction_id {
        opening.push(ConsoleRequest::AttachTransaction(
            AttachTransactionRequest { transaction_id },
        ));
    }
    for request in opening {
        if signals.auth_token.get_untracked().as_deref() != Some(current_auth_token) {
            return ConnectionEnd::Dropped;
        }
        let issued = requests.issue(request);
        let message = requests.dispatch(issued);
        if !send_message(&mut socket, &codec, signals, requests, message).await {
            return ConnectionEnd::Dropped;
        }
    }
    queue_subscription_restorations(signals, requests);
    let mut retry_delay = Box::pin(wait_for_browser_delay(SUBSCRIPTION_RETRY_DELAY).fuse());
    loop {
        if signals.auth_token.get_untracked().as_deref() != Some(current_auth_token) {
            return ConnectionEnd::Dropped;
        }
        let step = futures_util::select! {
            request = queued.next().fuse() => {
                if signals.auth_token.get_untracked().as_deref() != Some(current_auth_token) {
                    return ConnectionEnd::Dropped;
                }
                let Some(request) = request else {
                    return ConnectionEnd::ConsoleClosed;
                };
                let issued = requests.issue(request);
                if let Some(message) = requests.accept(issued)
                    && !send_message(&mut socket, &codec, signals, requests, message).await
                {
                    return ConnectionEnd::Dropped;
                }
                SessionStep::Continue
            }
            message = socket.next().fuse() => {
                if signals.auth_token.get_untracked().as_deref() != Some(current_auth_token) {
                    return ConnectionEnd::Dropped;
                }
                let Some(message) = message else {
                    return ConnectionEnd::Dropped;
                };
                let data = match message {
                    Ok(WebSocketMessage::Bytes(payload)) => {
                        WebSocketData::Binary(Bytes::from(payload))
                    }
                    Ok(WebSocketMessage::Text(_)) => WebSocketData::Text,
                    Err(error) => {
                        leptos::logging::error!("web console websocket failed: {error:?}");
                        return ConnectionEnd::Dropped;
                    }
                };
                match codec.decode(data) {
                    Ok(frame) => receive_frame(signals, state, requests, &frame),
                    Err(error) => {
                        let reason = format!(
                            "the server sent a message that is not a session frame: {}",
                            error.current_context()
                        );
                        signals
                            .terminal_lines
                            .update(|lines| lines.push(TermLine::error(reason)));
                        return ConnectionEnd::Dropped;
                    }
                }
            }
            () = retry_delay.as_mut() => {
                retry_delay = Box::pin(wait_for_browser_delay(SUBSCRIPTION_RETRY_DELAY).fuse());
                queue_subscription_restorations(signals, requests);
                SessionStep::Continue
            }
        };
        match step {
            SessionStep::Continue => {}
            SessionStep::Reattach { transaction_id } => {
                if !requests.is_attaching() {
                    let attach = ConsoleRequest::AttachTransaction(AttachTransactionRequest {
                        transaction_id,
                    });
                    let issued = requests.issue(attach);
                    let message = requests.dispatch(issued);
                    if !send_message(&mut socket, &codec, signals, requests, message).await {
                        return ConnectionEnd::Dropped;
                    }
                }
            }
            SessionStep::Redirect(leader) => return ConnectionEnd::Redirected(leader),
            SessionStep::Reconnect => return ConnectionEnd::Dropped,
        }
        for message in requests.release_held() {
            if !send_message(&mut socket, &codec, signals, requests, message).await {
                return ConnectionEnd::Dropped;
            }
        }
    }
}

/// An acknowledged subscription belongs to the connection that ended. Its tab remains desired,
/// but its previous generation must never accept rows from the replacement connection.
fn interrupt_subscription_tabs(signals: WebConsoleSignals) {
    signals.subscription_tabs.update(|tabs| {
        for tab in tabs.iter_mut() {
            if let SubscriptionTabState::Open(_) = &tab.state {
                tab.state = SubscriptionTabState::Interrupted;
                tab.lines.push(TermLine::info(
                    "delivery interrupted; restoring on the next connection",
                ));
            }
        }
        tabs.retain(|tab| !matches!(&tab.state, SubscriptionTabState::Closing(Some(_))));
    });
    let active = signals.active_subscription_tab.get_untracked();
    if let Some(active) = active {
        let still_present = signals.subscription_tabs.with_untracked(|tabs| {
            // The operator explicitly controls the number of open tabs in this console.
            tabs.iter().any(|tab| tab.id == active)
        });
        if !still_present {
            signals.active_subscription_tab.set(None);
        }
    }
}

/// A tab acknowledged on an earlier connection is reissued once on the new connection. Starts
/// that were already in flight remain in the request ledger and are replayed there instead.
fn queue_subscription_restorations(signals: WebConsoleSignals, requests: &mut SessionRequests) {
    let mut restore = Vec::new();
    signals.subscription_tabs.update(|tabs| {
        for tab in tabs.iter_mut() {
            if let SubscriptionTabState::Interrupted = &tab.state {
                tab.state = SubscriptionTabState::Restoring;
                restore.push((
                    tab.id,
                    SubscribeRequest {
                        domain: tab.domain.clone(),
                        statement: tab.subscribe_command.clone(),
                        subscription_type: SubscriptionType::Row,
                    },
                ));
            }
        }
    });
    for (tab_id, request) in restore {
        let issued = requests.issue(ConsoleRequest::SubscriptionStart { tab_id, request });
        requests.hold_again(issued);
    }
}

/// Sends one request message as one binary WebSocket message.
///
/// `false` says the connection is gone. The request stays registered, so the next connection sends
/// it again when it is ordered. A request that cannot become a frame is answered here, because no
/// reply will ever answer it.
async fn send_message(
    socket: &mut WebSocket,
    codec: &ClientWebSocketCodec,
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    message: ClientMessage,
) -> bool {
    let frame = match message.encode(&SESSION_LIMITS) {
        Ok(frame) => frame,
        Err(error) => {
            if let Some(unsent) = requests.answer(message.request_id) {
                let reason = format!("the request cannot be sent: {}", error.current_context());
                fail_request(signals, requests, unsent.request, reason);
            }
            return true;
        }
    };
    let payload = codec.encode(frame);
    match socket
        .send(WebSocketMessage::Bytes(Vec::from(payload)))
        .await
    {
        Ok(()) => true,
        Err(error) => {
            leptos::logging::error!("failed to send web console request: {error:?}");
            false
        }
    }
}

/// Applies one verified server frame.
fn receive_frame(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    requests: &mut SessionRequests,
    frame: &VerifiedFrame<ServerFrame>,
) -> SessionStep {
    let message = match ServerMessage::decode(frame) {
        Ok(message) => message,
        Err(error) => {
            let reason = format!(
                "failed to decode a server message: {}",
                error.current_context()
            );
            // A reply that cannot be read still ends the request it answers.
            let answered = match frame.request_id() {
                Some(request_id) => requests.answer(request_id),
                None => None,
            };
            match answered {
                Some(unread) => fail_request(signals, requests, unread.request, reason),
                None => {
                    signals
                        .terminal_lines
                        .update(|lines| lines.push(TermLine::error(reason)));
                }
            }
            return SessionStep::Continue;
        }
    };
    match requests.route(message) {
        Routed::Event(event) => apply_event(signals, state, requests, event),
        Routed::Reply(answered) => apply_reply(signals, requests, *answered),
        Routed::Unreadable(unreadable) => {
            let UnreadableReply { request, reason } = *unreadable;
            fail_request(signals, requests, request.request, reason);
            SessionStep::Continue
        }
        Routed::Untracked | Routed::Pending => SessionStep::Continue,
    }
}

async fn wait_for_websocket_open(socket: &WebSocket) {
    while matches!(socket.state(), WebSocketState::Connecting) {
        wait_for_browser_delay(Duration::from_millis(50)).await;
    }
}

async fn wait_for_browser_delay(delay: Duration) {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            window
                .set_timeout_with_callback_and_timeout_and_arguments_0(
                    &resolve,
                    i32::try_from(delay.as_millis()).unwrap_or(i32::MAX),
                )
                .discarded(
                    "a timer the browser refuses to schedule leaves the promise pending until the \
                     caller is dropped",
                );
        } else {
            resolve.call0(&wasm_bindgen::JsValue::UNDEFINED).discarded(
                "a resolve callback that throws leaves the promise pending until the caller is \
                 dropped",
            );
        }
    });
    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .discarded(
            "the wait is over either way: this promise carries no value and rejects only if the \
             timer threw",
        );
}

/// The session token the console was opened with, or `None` when the page carries none.
///
/// The console runs in a browser it does not control, so every step here can legitimately come up
/// empty: a document rendered without a window, a location the URL grammar does not accept, and an
/// address with no `auth` query all mean the same thing to the caller, which is that the console
/// has to ask the operator to sign in.
fn web_console_auth_token_from_location() -> Option<String> {
    let href = web_sys::window()?.location().href().ok()?;
    let url = Url::parse(&href).ok()?;
    url.query_pairs()
        .find_map(|(key, value)| (key == "auth").then(|| value.into_owned()))
}

/// The session websocket address derived from the page's own location.
///
/// A browser that refuses to hand over its protocol or host leaves the console with no address to
/// connect to, which is why absence is the answer rather than a failure: the caller retries once
/// the document is ready.
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

/// The origin the console makes its HTTP requests against, or `None` before the document has a
/// readable location.
fn web_console_http_base_url() -> Option<String> {
    let location = web_sys::window()?.location();
    let protocol = location.protocol().ok()?;
    let host = location.host().ok()?;
    Some(format!("{protocol}//{host}"))
}

/// The session websocket address of the web console at `base_url`.
///
/// `None` says the base URL is not one a session can be opened on: its scheme has no websocket
/// counterpart. The caller falls back to the page's own location.
fn web_console_websocket_url_from_base(base_url: &Url, auth_token: &str) -> Option<String> {
    let mut url = base_url.clone();
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

/// The state of the session's transaction the REPL prompt names, while the transaction can still
/// change.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveTransaction {
    Open,
    Committing,
}

impl ActiveTransaction {
    /// The prompt state of the session's transaction, or `None` while it has none that can still
    /// change.
    fn of(status: Option<&TransactionStatus>) -> Option<Self> {
        let status = status?;
        match status.lifecycle() {
            TransactionLifecycle::Open => Some(Self::Open),
            TransactionLifecycle::Committing => Some(Self::Committing),
            TransactionLifecycle::Committed
            | TransactionLifecycle::Failed { .. }
            | TransactionLifecycle::Reverted
            | TransactionLifecycle::Expired => None,
        }
    }
}

/// Whether the session's transaction can still change.
fn transaction_is_active(status: Option<&TransactionStatus>) -> bool {
    match status {
        Some(status) => status.lifecycle().is_active(),
        None => false,
    }
}

/// The transaction a new connection attaches again: the session's transaction, while it can still
/// change.
fn transaction_to_attach(status: Option<&TransactionStatus>) -> Option<String> {
    let status = status?;
    if !status.lifecycle().is_active() {
        return None;
    }
    Some(status.transaction_id().to_string())
}

/// Applies a message the server sent without a request.
fn apply_event(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    requests: &mut SessionRequests,
    event: ServerEvent,
) -> SessionStep {
    match event {
        ServerEvent::Leadership(observed) => match observed.leadership {
            Leadership::ServingNode(node) => {
                signals.terminal_lines.update(|lines| {
                    lines.push(TermLine::info(format!("connected to leader '{node}'")));
                });
                state.set(ConsoleConnectionState::Connected);
                requests.confirm_leader();
                SessionStep::Continue
            }
            Leadership::Remote(leader) => redirect_step(
                signals,
                &LeaderRedirect {
                    leader: Some(leader),
                },
            ),
            Leadership::Unknown => redirect_step(signals, &LeaderRedirect { leader: None }),
        },
        ServerEvent::Notice(notice) => {
            let line = notice_line(notice);
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        ServerEvent::Domains(observed) => {
            let listed = observed.domains.into_iter().map(DomainView::from).collect();
            apply_domain_list(signals, listed);
            SessionStep::Continue
        }
        ServerEvent::DomainSnapshot(snapshot) => {
            apply_snapshot(signals, &snapshot);
            SessionStep::Continue
        }
        ServerEvent::Cluster(cluster) => {
            signals.cluster_counters.set(ClusterCounters::from(cluster));
            SessionStep::Continue
        }
        ServerEvent::SubscriptionRows(rows) => {
            append_subscription_rows(signals.subscription_tabs, &rows);
            SessionStep::Continue
        }
        ServerEvent::SubscriptionDeliveryLost(lost) => {
            let line = TermLine::info(format!(
                "dropped {} rows the session could not take in time",
                lost.dropped_rows
            ));
            append_subscription_line(signals.subscription_tabs, &lost.subscription, line);
            SessionStep::Continue
        }
        ServerEvent::SubscriptionRowsSkipped(skipped) => {
            let line = TermLine::error(skipped.message);
            append_subscription_line(signals.subscription_tabs, &skipped.subscription, line);
            SessionStep::Continue
        }
        ServerEvent::SubscriptionEnded(ended) => {
            let line = TermLine::error(ended.message);
            append_subscription_line(signals.subscription_tabs, &ended.subscription, line);
            SessionStep::Continue
        }
        ServerEvent::SessionEnding(ending) => match ending.reason {
            SessionEndReason::ServerShuttingDown => {
                signals.terminal_lines.update(|lines| {
                    lines.push(TermLine::info("the server is shutting down"));
                });
                SessionStep::Reconnect
            }
            SessionEndReason::ProtocolViolated { message } => {
                signals.terminal_lines.update(|lines| {
                    lines.push(TermLine::error(format!(
                        "the server ended the session: {message}"
                    )));
                });
                SessionStep::Reconnect
            }
            SessionEndReason::LeaderRedirect(redirect) => redirect_step(signals, &redirect),
        },
    }
}

/// Applies the terminal reply to a request.
fn apply_reply(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    answered: AnsweredRequest,
) -> SessionStep {
    let AnsweredRequest { request, body } = answered;
    let IssuedRequest { order, request } = request;
    match (request, body) {
        (request, ReplyBody::Rejected(rejected)) => {
            fail_request(signals, requests, request, rejected.message);
            SessionStep::Continue
        }
        (request, ReplyBody::Cancelled(cancelled)) => {
            fail_request(signals, requests, request, cancellation_reason(cancelled));
            SessionStep::Continue
        }
        (ConsoleRequest::Command { request, purpose }, ReplyBody::Command(outcome)) => {
            apply_command_outcome(signals, requests, order, request, purpose, *outcome)
        }
        (ConsoleRequest::ListDomains, ReplyBody::DomainList(list)) => {
            let listed = list
                .domains
                .into_iter()
                .map(DomainView::from)
                .collect::<Vec<_>>();
            let lines = domain_list_lines(&listed);
            apply_domain_list(signals, listed);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
            SessionStep::Continue
        }
        (ConsoleRequest::SelectDomain(_), ReplyBody::DomainSelection(selection)) => {
            let line = domain_selection_line(selection);
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        (ConsoleRequest::Suggest(_), ReplyBody::Suggest(outcome)) => {
            let values = outcome
                .suggestions
                .into_iter()
                .map(|suggestion| suggestion.value)
                .collect();
            signals.suggestions.set(values);
            SessionStep::Continue
        }
        (ConsoleRequest::AttachTransaction(_), ReplyBody::Attach(outcome)) => {
            apply_attach_outcome(signals, requests, outcome)
        }
        (ConsoleRequest::SubscriptionStart { tab_id, request }, ReplyBody::Subscribe(outcome)) => {
            apply_subscribe_outcome(signals, requests, tab_id, &request.statement, outcome);
            SessionStep::Continue
        }
        (ConsoleRequest::SubscriptionStop { tab_id, request }, ReplyBody::Unsubscribe(outcome)) => {
            apply_unsubscribe_outcome(signals, tab_id, &request, outcome);
            SessionStep::Continue
        }
        (request, _) => {
            fail_request(signals, requests, request, UNEXPECTED_REPLY.to_string());
            SessionStep::Continue
        }
    }
}

/// Applies the outcome of a command.
///
/// A command the serving node could not run because it does not lead is sent again at the leader's
/// console, and a command the leader could not bind to the session's transaction is sent again
/// once the transaction is attached. An unknown outcome reconnects before another attempt. Each
/// attempt keeps the command's execution reference, so the server recovers its recorded outcome
/// instead of running it twice. A definitive outcome goes to whoever reads the command.
fn apply_command_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    order: IssueOrder,
    request: CommandRequest,
    purpose: CommandPurpose,
    mut outcome: CommandOutcome,
) -> SessionStep {
    if let Some(redirect) = command_redirect(&outcome.disposition) {
        let step = redirect_step(signals, redirect);
        requests.hold_again(IssuedRequest {
            order,
            request: ConsoleRequest::Command { request, purpose },
        });
        return step;
    }
    if matches!(outcome.disposition, CommandDisposition::OutcomeUnknown(_)) {
        requests.hold_again(IssuedRequest {
            order,
            request: ConsoleRequest::Command { request, purpose },
        });
        return SessionStep::Reconnect;
    }
    if let Some(status) = outcome.transaction.take() {
        adopt_transaction(signals, status);
    }
    if let CommandDisposition::TransactionDetached { transaction_id } = &outcome.disposition {
        let transaction_id = transaction_id.clone();
        requests.hold_again(IssuedRequest {
            order,
            request: ConsoleRequest::Command { request, purpose },
        });
        return SessionStep::Reattach { transaction_id };
    }
    match purpose {
        CommandPurpose::Repl => show_command_outcome(signals, &request.query, outcome),
        CommandPurpose::ResourceDescription { resource } => {
            let detail = ResourceDetailView::from_description(outcome);
            signals.resource_details.update(|details| {
                details.insert(resource, detail);
            });
        }
    }
    SessionStep::Continue
}

/// Prints the outcome of a REPL command, and makes the domain a completed `CREATE DOMAIN` created
/// the active one.
fn show_command_outcome(signals: WebConsoleSignals, query: &str, outcome: CommandOutcome) {
    if let CommandDisposition::Completed { .. } = outcome.disposition
        && let Some(domain) = first_created_domain_from_query(query)
    {
        signals.active_domain.set(Some(domain));
    }
    let lines = command_outcome_lines(outcome, query);
    signals
        .terminal_lines
        .update(|terminal| terminal.extend(lines));
}

/// Applies the outcome of attaching the session's transaction.
///
/// Once the transaction is attached, the requests held for it are released. A transaction that
/// already finished, or that could not be attached, ends the requests held for it.
fn apply_attach_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    outcome: AttachOutcome,
) -> SessionStep {
    let AttachOutcome {
        disposition,
        message,
        diagnostics,
    } = outcome;
    // An attach carries no source text, so no diagnostic of it points into one.
    let query = "";
    match disposition {
        AttachDisposition::Attached(status) => {
            let active = status.lifecycle().is_active();
            adopt_transaction(signals, status);
            if !active {
                requests.clear_held();
                let lines = completed_lines(message);
                signals
                    .terminal_lines
                    .update(|terminal| terminal.extend(lines));
            }
            SessionStep::Continue
        }
        AttachDisposition::AlreadyFinished(status) => {
            adopt_transaction(signals, status);
            requests.clear_held();
            let lines = failed_lines(message, diagnostics, query);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
            SessionStep::Continue
        }
        AttachDisposition::Failed => {
            signals.transaction_status.set(None);
            requests.clear_held();
            let lines = failed_lines(message, diagnostics, query);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
            SessionStep::Continue
        }
        AttachDisposition::NotLeader(redirect) => redirect_step(signals, &redirect),
    }
}

/// Applies the outcome of opening a tab's subscription. The tab opens either way: an opened
/// subscription streams its rows into it, and a failure is shown in it.
fn apply_subscribe_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    tab_id: u64,
    statement: &str,
    outcome: SubscribeOutcome,
) {
    let SubscribeOutcome {
        disposition,
        message,
        diagnostics,
    } = outcome;
    match disposition {
        SubscribeDisposition::Opened(opened) => {
            let SubscriptionOpened {
                subscription,
                schema,
                ..
            } = *opened;
            let stream = TabStream {
                subscription,
                schema,
            };
            let mut close_after_open = false;
            let mut opened = false;
            signals.subscription_tabs.update(|tabs| {
                let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) else {
                    return;
                };
                match tab.state.clone() {
                    SubscriptionTabState::Pending | SubscriptionTabState::Restoring => {
                        tab.state = SubscriptionTabState::Open(stream.clone());
                        opened = true;
                    }
                    SubscriptionTabState::Closing(None) => {
                        tab.state = SubscriptionTabState::Closing(Some(stream.clone()));
                        close_after_open = true;
                    }
                    SubscriptionTabState::Open(_)
                    | SubscriptionTabState::Interrupted
                    | SubscriptionTabState::Closing(Some(_)) => {}
                }
            });
            if close_after_open {
                let issued = requests.issue(ConsoleRequest::SubscriptionStop {
                    tab_id,
                    request: UnsubscribeRequest {
                        subscription: stream.subscription.name,
                    },
                });
                requests.hold_again(issued);
            } else if opened {
                signals.active_subscription_tab.set(Some(tab_id));
            }
        }
        SubscribeDisposition::Failed => {
            let lines = failed_lines(message, diagnostics, statement);
            fail_subscription_start(signals, tab_id, lines);
        }
    }
}

fn apply_unsubscribe_outcome(
    signals: WebConsoleSignals,
    tab_id: u64,
    request: &UnsubscribeRequest,
    outcome: UnsubscribeOutcome,
) {
    match outcome.disposition {
        UnsubscribeDisposition::Deleted(deleted) => {
            let matches_closing = signals.subscription_tabs.with_untracked(|tabs| {
                tabs.iter().any(|tab| {
                    tab.id == tab_id
                        && matches!(
                            &tab.state,
                            SubscriptionTabState::Closing(Some(stream))
                                if stream.subscription == deleted
                        )
                })
            });
            if matches_closing && deleted.name == request.subscription {
                remove_subscription_tab(
                    signals.subscription_tabs,
                    signals.active_subscription_tab,
                    tab_id,
                );
            } else {
                restore_failed_unsubscribe(
                    signals.subscription_tabs,
                    tab_id,
                    "the server confirmed another subscription deletion".to_string(),
                );
            }
        }
        UnsubscribeDisposition::Failed => {
            let reason = outcome.message.clone();
            let lines = failed_lines(outcome.message, outcome.diagnostics, "");
            restore_failed_unsubscribe(signals.subscription_tabs, tab_id, reason);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
        }
    }
}

/// Ends a request that gets no usable reply, showing `reason` where its reply would have been
/// shown.
fn fail_request(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    request: ConsoleRequest,
    reason: String,
) {
    match request {
        ConsoleRequest::Command {
            purpose: CommandPurpose::Repl,
            ..
        }
        | ConsoleRequest::ListDomains
        | ConsoleRequest::SelectDomain(_) => {
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
        ConsoleRequest::Command {
            purpose: CommandPurpose::ResourceDescription { resource },
            ..
        } => {
            let detail = ResourceDetailView {
                versions: Vec::new(),
                status: reason,
            };
            signals.resource_details.update(|details| {
                details.insert(resource, detail);
            });
        }
        ConsoleRequest::SubscriptionStart { tab_id, .. } => {
            fail_subscription_start(signals, tab_id, vec![TermLine::error(reason)]);
        }
        ConsoleRequest::SubscriptionStop { tab_id, .. } => {
            restore_failed_unsubscribe(signals.subscription_tabs, tab_id, reason.clone());
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
        ConsoleRequest::Suggest(_) => signals.suggestions.set(Vec::new()),
        ConsoleRequest::AttachTransaction(_) => {
            // Without its transaction attached, the session cannot serve what was held for it.
            signals.transaction_status.set(None);
            requests.clear_held();
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
    }
}

/// Takes the session's transaction as the server reports it, and makes its domain the active one.
fn adopt_transaction(signals: WebConsoleSignals, status: TransactionStatus) {
    let domain = status.domain().clone();
    let already_active = signals
        .active_domain
        .with_untracked(|active| active.as_ref() == Some(&domain));
    if !already_active {
        signals.active_domain.set(Some(domain));
    }
    signals.transaction_status.set(Some(status));
}

/// Takes the complete domain list. The active domain stays while it is listed; otherwise the first
/// listed domain becomes the active one.
fn apply_domain_list(signals: WebConsoleSignals, listed: Vec<DomainView>) {
    let active = signals.active_domain.get_untracked();
    // Bounded by the domains of the cluster, which the domain menu lists in this order.
    let active_is_listed = match &active {
        Some(active) => listed.iter().any(|domain| domain.domain == *active),
        None => false,
    };
    let first = listed.first().map(|domain| domain.domain.clone());
    signals.domains_loaded.set(true);
    signals.domains.set(listed);
    if !active_is_listed {
        signals.active_domain.set(first);
    }
}

/// Keeps the latest snapshot of a domain's graph and entities.
fn apply_snapshot(signals: WebConsoleSignals, snapshot: &DomainSnapshotObserved) {
    match DataflowGraph::deserialize(snapshot.graph_json().as_bytes()) {
        Ok(graph) => {
            let view = DomainSnapshotView::new(snapshot.entities(), graph);
            let domain = snapshot.domain().clone();
            signals.domain_snapshots.update(|snapshots| {
                snapshots.insert(domain, view);
            });
        }
        Err(error) => {
            let reason = format!(
                "failed to decode graph snapshot for domain '{}': {error}",
                snapshot.domain()
            );
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
    }
}

/// Continues the session at the leader's web console, or, while the leader or its console is not
/// known, says so and connects again at the same address.
fn redirect_step(signals: WebConsoleSignals, redirect: &LeaderRedirect) -> SessionStep {
    if let Some(leader) = redirect_console(redirect) {
        return SessionStep::Redirect(leader.clone());
    }
    let line = leader_redirect_line(redirect);
    signals.terminal_lines.update(|lines| lines.push(line));
    SessionStep::Reconnect
}

/// The leader's web console a command has to be sent to, when the serving node does not lead and
/// knows where the leader's console is.
fn command_redirect(disposition: &CommandDisposition) -> Option<&LeaderRedirect> {
    let CommandDisposition::NotLeader(redirect) = disposition else {
        return None;
    };
    Some(redirect)
}

/// The web console a redirect names, when the leader is known and advertises one.
fn redirect_console(redirect: &LeaderRedirect) -> Option<&Url> {
    let leader = redirect.leader.as_ref()?;
    leader.web_console_uri.as_ref()
}

/// The terminal lines of a command outcome, whose diagnostics point into `query`. A command of
/// several statements shows the outcome of each.
fn command_outcome_lines(outcome: CommandOutcome, query: &str) -> Vec<TermLine> {
    let CommandOutcome {
        disposition,
        message,
        diagnostics,
        statements,
        ..
    } = outcome;
    if !statements.is_empty() {
        let mut lines = Vec::new();
        for statement in statements {
            lines.extend(statement_outcome_lines(statement, query));
        }
        return lines;
    }
    match disposition {
        CommandDisposition::Completed { .. } => completed_lines(message),
        CommandDisposition::NotLeader(redirect) => not_leader_lines(&redirect, diagnostics, query),
        CommandDisposition::Failed
        | CommandDisposition::TransactionDetached { .. }
        | CommandDisposition::TransactionTakenOver { .. }
        | CommandDisposition::OutcomeUnknown(_)
        | CommandDisposition::ExecutionReferenceConflict(_)
        | CommandDisposition::ExecutionReferenceExpired
        | CommandDisposition::PreviewStale { .. } => failed_lines(message, diagnostics, query),
    }
}

/// The terminal lines of one statement of a command.
fn statement_outcome_lines(statement: StatementOutcome, query: &str) -> Vec<TermLine> {
    let StatementOutcome {
        disposition,
        message,
        diagnostics,
    } = statement;
    match disposition {
        StatementDisposition::Completed { .. } => completed_lines(message),
        StatementDisposition::Failed => failed_lines(message, diagnostics, query),
        StatementDisposition::NotLeader(redirect) => {
            not_leader_lines(&redirect, diagnostics, query)
        }
    }
}

/// A completed outcome shows its message, when it has one.
fn completed_lines(message: String) -> Vec<TermLine> {
    if message.is_empty() {
        return Vec::new();
    }
    vec![TermLine::output(message)]
}

/// A failed outcome shows its message as an error, followed by its diagnostics.
fn failed_lines(message: String, diagnostics: Vec<Diagnostic>, query: &str) -> Vec<TermLine> {
    let mut lines = vec![TermLine::error(message)];
    lines.extend(diagnostic_lines(diagnostics, query));
    lines
}

/// An outcome that needed the leader says where the leader is, followed by its diagnostics.
fn not_leader_lines(
    redirect: &LeaderRedirect,
    diagnostics: Vec<Diagnostic>,
    query: &str,
) -> Vec<TermLine> {
    let mut lines = vec![leader_redirect_line(redirect)];
    lines.extend(diagnostic_lines(diagnostics, query));
    lines
}

/// The diagnostics of an outcome that did not complete, or a line saying it carried none.
fn diagnostic_lines(diagnostics: Vec<Diagnostic>, query: &str) -> Vec<TermLine> {
    if diagnostics.is_empty() {
        return vec![TermLine::output("- no diagnostics provided")];
    }
    let mut lines = Vec::with_capacity(diagnostics.len());
    for diagnostic in diagnostics {
        lines.push(diagnostic_line(query, diagnostic));
    }
    lines
}

/// Where the leader is, as far as the serving node knows.
fn leader_redirect_line(redirect: &LeaderRedirect) -> TermLine {
    let Some(leader) = &redirect.leader else {
        return TermLine::info("topology: not-a-leader");
    };
    match &leader.grpc_uri {
        Some(uri) => TermLine::info(format!(
            "topology: not-a-leader, retry on leader '{}' at {uri}",
            leader.node
        )),
        None => TermLine::info(format!(
            "topology: not-a-leader, retry on leader '{}'",
            leader.node
        )),
    }
}

/// What selecting a domain did.
fn domain_selection_line(selection: DomainSelection) -> TermLine {
    match selection {
        DomainSelection::Selected(domain) => TermLine::info(format!("using domain '{domain}'")),
        DomainSelection::NotFound(domain) => {
            TermLine::error(format!("domain '{domain}' does not exist"))
        }
    }
}

/// Why a cancelled request ended, and what may still come of it.
fn cancellation_reason(cancelled: RequestCancelled) -> String {
    match cancelled.stage {
        CancellationStage::BeforeAdmission => {
            "the request was cancelled before it was admitted".to_string()
        }
        CancellationStage::AfterAdmission => "the request was cancelled after it was admitted, \
                                              and its effects may still complete"
            .to_string(),
    }
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

/// A creation failure leaves no live tab. A failed restoration keeps the acknowledged tab visible
/// and interrupted, so it can be restored on the next connection.
fn fail_subscription_start(signals: WebConsoleSignals, tab_id: u64, lines: Vec<TermLine>) {
    let mut remove = false;
    signals.subscription_tabs.update(|tabs| {
        let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) else {
            return;
        };
        match tab.state.clone() {
            SubscriptionTabState::Pending | SubscriptionTabState::Closing(None) => remove = true,
            SubscriptionTabState::Restoring => {
                tab.state = SubscriptionTabState::Interrupted;
                tab.lines.extend(lines.clone());
            }
            SubscriptionTabState::Open(_)
            | SubscriptionTabState::Interrupted
            | SubscriptionTabState::Closing(Some(_)) => {}
        }
    });
    if remove {
        remove_subscription_tab(
            signals.subscription_tabs,
            signals.active_subscription_tab,
            tab_id,
        );
    }
    signals
        .terminal_lines
        .update(|terminal| terminal.extend(lines));
}

fn restore_failed_unsubscribe(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    tab_id: u64,
    reason: String,
) {
    subscription_tabs.update(|tabs| {
        let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) else {
            return;
        };
        if let SubscriptionTabState::Closing(Some(stream)) = &tab.state {
            tab.state = SubscriptionTabState::Open(stream.clone());
            tab.lines.push(TermLine::error(reason));
        }
    });
}

fn remove_subscription_tab(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    tab_id: u64,
) {
    subscription_tabs.update(|tabs| tabs.retain(|tab| tab.id != tab_id));
    active_subscription_tab.update(|active| {
        if *active == Some(tab_id) {
            *active = None;
        }
    });
}

/// Shows a batch of a subscription's rows, one line per row, in the tabs that show the
/// subscription.
fn append_subscription_rows(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    rows: &SubscriptionRows,
) {
    let batch = rows.batch();
    subscription_tabs.update(|tabs| {
        // Bounded by the subscription tabs the operator has open in this console.
        for tab in tabs.iter_mut() {
            let Some(schema) = tab.stream_schema(rows.subscription()) else {
                continue;
            };
            let lines = subscription_row_lines(&batch, schema);
            tab.lines.extend(lines);
        }
    });
}

/// The display line of every row of a batch, or the reason the batch cannot be shown against the
/// schema its subscription announced.
fn subscription_row_lines(batch: &RowBatchView<'_>, schema: &RowSchema) -> Vec<TermLine> {
    match batch.display_lines(schema) {
        Ok(lines) => lines.into_iter().map(TermLine::output).collect(),
        Err(error) => vec![TermLine::error(format!(
            "rows do not match the subscription schema: {}",
            error.current_context()
        ))],
    }
}

/// Shows a line about a subscription in the tabs that show it.
fn append_subscription_line(
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    subscription: &SubscriptionHandle,
    line: TermLine,
) {
    subscription_tabs.update(|tabs| {
        // Bounded by the subscription tabs the operator has open in this console.
        for tab in tabs.iter_mut() {
            if tab.streams(subscription) {
                tab.lines.push(line.clone());
            }
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
        .chain(
            domains
                .iter()
                .map(|domain| TermLine::output(domain.listing_line())),
        )
        .collect()
}

impl ResourceDetailView {
    /// Reads the dialog's versions from the same `DESCRIBE RESOURCE` description the REPL prints,
    /// attaching to each version the usages that pin it. A description that did not complete
    /// shows its message instead.
    fn from_description(outcome: CommandOutcome) -> Self {
        let CommandDisposition::Completed { .. } = outcome.disposition else {
            return Self {
                versions: Vec::new(),
                status: outcome.message,
            };
        };
        let mut versions = Vec::<ResourceVersionView>::new();
        let mut usages_by_version = BTreeMap::<u64, Vec<ResourceUsageView>>::new();
        let mut section = ResourceDescribeSection::Summary;
        for line in outcome.message.lines() {
            if line == "version_details:" {
                section = ResourceDescribeSection::VersionDetails;
                continue;
            }
            if line == "usages:" {
                section = ResourceDescribeSection::Usages;
                continue;
            }
            match section {
                ResourceDescribeSection::Summary => {}
                ResourceDescribeSection::VersionDetails => {
                    if let Some(version) = parse_resource_version_detail(line) {
                        versions.push(version);
                    } else if let Some(file) = parse_resource_file_detail(line)
                        && let Some(version) = versions.last_mut()
                    {
                        version.files.push(file);
                    }
                }
                ResourceDescribeSection::Usages => {
                    if let Some(detail) = ResourceUsageDetail::parse(line) {
                        usages_by_version
                            .entry(detail.version)
                            .or_default()
                            .push(detail.usage);
                    }
                }
            }
        }
        for version in &mut versions {
            if let Some(usages) = usages_by_version.remove(&version.version) {
                version.usages = usages;
            }
        }
        Self {
            versions,
            status: "ready".to_string(),
        }
    }
}

impl ResourceUsageDetail {
    /// Reads `- kind=<kind> name=<name> version=<n>`. The description spells the kind in
    /// snake_case, such as `hash_map`; the dialog shows it as a statement names it, `HASH MAP`.
    fn parse(line: &str) -> Option<Self> {
        let line = line.strip_prefix("- ")?;
        let mut kind = None;
        let mut name = None;
        let mut version = None;
        for part in line.split_whitespace() {
            let Some((key, value)) = part.split_once('=') else {
                continue;
            };
            match key {
                "kind" => kind = Some(value.replace('_', " ").to_ascii_uppercase()),
                "name" => name = Some(value.to_string()),
                "version" => version = value.parse::<u64>().ok(),
                _ => {}
            }
        }
        Some(Self {
            version: version?,
            usage: ResourceUsageView {
                kind: kind?,
                name: name?,
            },
        })
    }
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
            "version" => version = value.parse::<u64>().ok(),
            "root_checksum" => root_checksum = Some(value.to_string()),
            "manifest_checksum" => manifest_checksum = Some(value.to_string()),
            "file_count" => file_count = Some(value.to_string()),
            "total_bytes" => total_bytes = Some(value.to_string()),
            "created_by_node" => created_by_node = ClusterNodeName::parse(value).ok(),
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
        usages: Vec::new(),
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

/// The domain the first `CREATE DOMAIN` in `query` declares, or `None` when there is none.
///
/// The query is whatever the operator has typed so far, so text that does not parse is the
/// ordinary case rather than a failure; it simply names no domain yet.
fn first_created_domain_from_query(query: &str) -> Option<DomainName> {
    let statements = parse_client_statements(query).ok()?;
    for statement in statements {
        if let ClientStatement::Server(Statement::CreateDomain(create)) = statement {
            return Some(create.id.clone());
        }
    }
    None
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

/// One diagnostic as the terminal shows it: the text its span covers in `query` and its message.
///
/// The span arrives from the server, so it is shown only when it is a non-empty range whose ends
/// both lie within `query` on character boundaries; any other span shows the message alone.
fn diagnostic_line(query: &str, diagnostic: Diagnostic) -> TermLine {
    let Diagnostic { message, span } = diagnostic;
    if let Some(span) = span {
        let start = usize::try_from(span.start())
            .assured("supported browser and test targets have at least 32-bit pointers");
        let end = usize::try_from(span.end())
            .assured("supported browser and test targets have at least 32-bit pointers");
        // `str::get` answers `None` for a range outside the text or off a character boundary.
        if start < end
            && let Some(covered) = query.get(start..end)
        {
            return TermLine::output(format!(
                "- {covered} at {}..{}: {message}",
                span.start(),
                span.end()
            ));
        }
    }
    TermLine::output(format!("- {message}"))
}

/// A server notice as the terminal shows it.
fn notice_line(notice: ServerNotice) -> TermLine {
    match notice.level {
        NoticeLevel::Error => TermLine::error(notice.message),
        NoticeLevel::Warning => TermLine::info(format!("warn: {}", notice.message)),
        NoticeLevel::Info => TermLine::info(notice.message),
    }
}

#[component]
fn Header(
    active_theme: RwSignal<usize>,
    websocket_state: RwSignal<ConsoleConnectionState>,
    active_domain: RwSignal<Option<DomainName>>,
    domains: RwSignal<Vec<DomainView>>,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let theme_open = RwSignal::new(false);
    let selected_domain = move || {
        let active = active_domain.get()?;
        let listed_domains = domains.read();
        // Bounded by the domains of the cluster, which the domain menu lists.
        listed_domains
            .iter()
            .find(|candidate| candidate.domain == active)
            .cloned()
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
                        .is_some_and(|domain| domain.state_command().is_some())
                    fallback=|| ()
                >
                    <button
                        class="domain-state-button topbar-domain-state-button"
                        class:domain-state-start=move || selected_domain()
                            .is_some_and(|domain| domain.status == DomainStatus::Stopped)
                        class:domain-state-stop=move || selected_domain()
                            .is_some_and(|domain| domain.status == DomainStatus::Running)
                        type="button"
                        disabled=move || websocket_state.get() != ConsoleConnectionState::Connected
                        title=move || match selected_domain() {
                            Some(domain) => domain.state_hint(
                                websocket_state.get() == ConsoleConnectionState::Connected,
                            )
                            .to_string(),
                            None => "Domain lifecycle".to_string(),
                        }
                        aria-label=move || match selected_domain() {
                            Some(domain) => domain.state_hint(
                                websocket_state.get() == ConsoleConnectionState::Connected,
                            )
                            .to_string(),
                            None => "Domain lifecycle".to_string(),
                        }
                        on:click=move |_| {
                            if websocket_state.get_untracked() != ConsoleConnectionState::Connected {
                                return;
                            }
                            if let Some(domain) = selected_domain()
                                && let Some(command) = domain.state_command()
                            {
                                run_command(Some(command.to_string()));
                            }
                        }
                    >
                        <Show
                            when=move || selected_domain()
                                .is_some_and(|domain| domain.status == DomainStatus::Running)
                            fallback=|| view! { <SidebarIcon kind="play" /> }
                        >
                            <SidebarIcon kind="stop" />
                        </Show>
                        <span class="domain-state-hint" aria-hidden="true">
                            {move || match selected_domain() {
                                Some(domain) => domain.state_hint(
                                    websocket_state.get() == ConsoleConnectionState::Connected,
                                )
                                .to_string(),
                                None => "Domain lifecycle".to_string(),
                            }}
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
    active_domain: RwSignal<Option<DomainName>>,
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
    let entities_for = move |kind: EntityKind| {
        active_entities()
            .into_iter()
            .filter(move |entity| entity.kind == kind)
            .collect::<Vec<_>>()
    };
    let wire_schema_entities = move || {
        active_entities()
            .into_iter()
            .filter(|entity| entity.kind.is_wire_schema())
            .collect::<Vec<_>>()
    };
    let selected_domain = move || {
        let active = active_domain.get()?;
        let listed = {
            let listed_domains = domains.read();
            // Bounded by the domains of the cluster, which the domain menu lists.
            listed_domains
                .iter()
                .find(|candidate| candidate.domain == active)
                .cloned()
        };
        let domain = match listed {
            Some(domain) => SidebarDomain::Listed(domain),
            None => SidebarDomain::Unlisted(active),
        };
        Some(domain)
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
                        if let Some(domain) = selected_domain() {
                            domain.name().to_string()
                        } else if domains_loaded.get() {
                            "no domain".to_string()
                        } else {
                            "loading domains".to_string()
                        }
                    }}</span>
                    <span class="domain-mode">{move || {
                        if let Some(domain) = selected_domain() {
                            domain.pace_label().to_string()
                        } else if domains_loaded.get() {
                            "NONE".to_string()
                        } else {
                            "WAIT".to_string()
                        }
                    }}</span>
                    <span class="chevron">{move || if domain_open.get() { "⌃" } else { "⌄" }}</span>
                </button>
                <div class="popup-menu domain-menu" class:open=move || domain_open.get()>
                    <For
                        each=move || domains.get()
                        key=|domain| domain.domain.clone()
                        children={move |domain| {
                            let domain_id = domain.domain.to_string();
                            let active_domain_id = domain.domain.clone();
                            let domain_label = domain.domain.to_string();
                            let domain_mode = domain.pace_label().to_string();
                            let command_domain = domain.domain.clone();
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
                    <strong>{move || match selected_domain() {
                        Some(domain) => domain.status_label().to_string(),
                        None => "WAITING".to_string(),
                    }}</strong>
                </div>
                <div class="summary-metrics">
                    <MetricMini value=move || match active_graph() {
                        Some(graph) => graph.statistics.messages_rate(),
                        None => "0".to_string(),
                    } label="msgs/s" />
                    <MetricMini value=move || match active_graph() {
                        Some(graph) => graph.statistics.bytes_rate(),
                        None => "0B".to_string(),
                    } label="bytes/s" />
                    <MetricMini value=move || match active_graph() {
                        Some(graph) => graph.statistics.batches_rate(),
                        None => "0".to_string(),
                    } label="batches" />
                </div>
            </div>
            <nav class="nav-list" aria-label="Console entities">
                <NavHeader title="Schemas" count=move || entities_for(EntityKind::Model(ModelKind::Schema)).len().to_string() kind="schemas" open=schemas_open />
                <Show when=move || schemas_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for(EntityKind::Model(ModelKind::Schema))
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="schemas" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Wire Schemas" count=move || wire_schema_entities().len().to_string() kind="wire" open=wire_open />
                <Show when=move || wire_open.get() fallback=|| ()>
                    <For
                        each=wire_schema_entities
                        key=|entity| (entity.kind, entity.name.clone())
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="wire" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Codecs" count=move || entities_for(EntityKind::Model(ModelKind::Codec)).len().to_string() kind="codecs" open=codecs_open />
                <Show when=move || codecs_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for(EntityKind::Model(ModelKind::Codec))
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="codecs" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Resources" count=move || entities_for(EntityKind::Resource).len().to_string() kind="resources" open=resources_open />
                <Show when=move || resources_open.get() fallback=|| ()>
                    // A resource's detail is its latest completed version, which changes while the
                    // row is shown. A keyed list re-renders a row only when its key changes, so the
                    // row is keyed by everything it shows.
                    <For
                        each=move || entities_for(EntityKind::Resource)
                        key=|entity| entity.clone()
                        children={move |entity| {
                            let name = entity.name.clone();
                            let describe_name = entity.name.clone();
                            let request_tx = web_console_session.request_tx;
                            let describe_command = entity.describe_command();
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
                                            active_domain.get_untracked(),
                                        );
                                    }
                                />
                            }
                        }}
                    />
                </Show>
                <NavHeader title="Clients" count=move || entities_for(EntityKind::Model(ModelKind::Client)).len().to_string() kind="resources" open=clients_open />
                <Show when=move || clients_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for(EntityKind::Model(ModelKind::Client))
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="resources" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Vhosts" count=move || entities_for(EntityKind::Model(ModelKind::Vhost)).len().to_string() kind="resources" open=vhosts_open />
                <Show when=move || vhosts_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for(EntityKind::Model(ModelKind::Vhost))
                        key=|entity| entity.name.clone()
                        children={|entity| view! { <NavItem name=entity.name meta=entity.detail kind="resources" on_click=|| () /> }}
                    />
                </Show>
                <NavHeader title="Endpoints" count=move || entities_for(EntityKind::Model(ModelKind::Endpoint)).len().to_string() kind="resources" open=endpoints_open />
                <Show when=move || endpoints_open.get() fallback=|| ()>
                    <For
                        each=move || entities_for(EntityKind::Model(ModelKind::Endpoint))
                        key=|entity| entity.name.clone()
                        children={move |entity| {
                            let describe_command = entity.describe_command();
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
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    resource: String,
    domain: Option<DomainName>,
) {
    let request = CommandRequest {
        query: format!("DESCRIBE RESOURCE {resource};"),
        domain,
        execution_reference: command_execution_reference(),
        expected_transaction_position: None,
        expected_preview: None,
    };
    let queued = ConsoleRequest::Command {
        request,
        purpose: CommandPurpose::ResourceDescription { resource },
    };
    if let Some(tx) = request_tx.get_untracked() {
        tx.unbounded_send(queued)
            .means_shutdown("web console session");
    }
}

/// Durable command admission reads the creation time embedded in a UUIDv7 retry identity, so a
/// persistent command sent from the console must carry one.
fn command_execution_reference() -> CommandExecutionReference {
    CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string()).assured(
        "a hyphenated UUID is 36 ASCII hex digits and hyphens, which an execution reference admits",
    )
}

#[component]
fn ResourceDialog(
    resource: impl Fn() -> String + Copy + Send + Sync + 'static,
    details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    upload_status: RwSignal<String>,
    upload_base_url: RwSignal<Option<String>>,
    auth_token: RwSignal<Option<String>>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    active_domain: RwSignal<Option<DomainName>>,
    close: impl Fn() + Copy + Send + 'static,
) -> impl IntoView {
    let file_input = NodeRef::<leptos::html::Input>::new();
    let directory_input = NodeRef::<leptos::html::Input>::new();
    let uploading = RwSignal::new(false);
    let upload_abort = RwSignal::new(None::<AbortHandle>);
    on_cleanup(move || {
        if let Some(abort) = upload_abort.get_untracked() {
            abort.abort();
        }
    });
    let trigger_upload = move |input: web_sys::HtmlInputElement| {
        let resource_name = resource();
        let Some(upload_domain) = active_domain.get_untracked() else {
            upload_status.set("no active domain selected".to_string());
            return;
        };
        upload_status.set("uploading".to_string());
        uploading.set(true);
        let attempt_auth = auth_token.get_untracked();
        let (abort, registration) = AbortHandle::new_pair();
        upload_abort.update(|active| {
            if let Some(previous) = active.replace(abort) {
                previous.abort();
            }
        });
        spawn_local(async move {
            let outcome = Abortable::new(
                upload_resource_files(
                    resource_name.clone(),
                    upload_domain,
                    input,
                    upload_base_url.get_untracked(),
                    attempt_auth.clone(),
                ),
                registration,
            )
            .await;
            let Ok(message) = outcome else {
                return;
            };
            if auth_token.get_untracked() != attempt_auth {
                return;
            }
            upload_abort.set(None);
            upload_status.set(message);
            uploading.set(false);
            request_resource_describe(request_tx, resource_name, active_domain.get_untracked());
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
                                input.set_attribute("webkitdirectory", "")
                                .discarded(
                                    "a browser that rejects the attribute opens a file picker \
                                     instead of a directory picker",
                                );
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
                            match details.get().get(&resource()) {
                                Some(detail) => detail.versions.len().to_string(),
                                None => "0".to_string(),
                            }
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
                                        match details.get().get(&resource()) {
                                            Some(detail) => detail.status.clone(),
                                            None => "loading".to_string(),
                                        }
                                    }}
                                </div>
                            }
                        }
                    >
                        <For
                            each=move || {
                                match details.get().get(&resource()) {
                                    Some(detail) => detail.versions.clone(),
                                    None => Vec::new(),
                                }
                            }
                            key=|version| version.clone()
                            children=|version| {
                                let summary = resource_version_summary(&version);
                                let checksums = resource_version_checksums(&version);
                                let files = version.files.clone();
                                let usages = version.usages.clone();
                                let unbound = version.usages.is_empty();
                                view! {
                                    <div class="resource-version-row" data-version=version.version.to_string()>
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
                                        <div class="resource-usage-list">
                                            <p>"usages"</p>
                                            <For
                                                each=move || usages.clone()
                                                key=|usage| usage.clone()
                                                children=|usage| {
                                                    view! {
                                                        <div class="resource-usage-row">
                                                            <em>{usage.kind}</em>
                                                            " "
                                                            <strong>{usage.name}</strong>
                                                        </div>
                                                    }
                                                }
                                            />
                                            <Show when=move || unbound fallback=|| ()>
                                                <div class="resource-usage-row resource-usage-none">"none"</div>
                                            </Show>
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
        .verified("this handler is only bound to the upload input element")
}

async fn upload_resource_files(
    resource: String,
    domain: DomainName,
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
    let Some(window) = web_sys::window() else {
        return "failed to access browser upload identity source".to_string();
    };
    let Ok(crypto) = window.crypto() else {
        return "failed to access browser upload identity source".to_string();
    };
    let upload_identity = crypto.random_uuid();
    let url = web_console_resource_upload_url(
        upload_base_url.as_deref(),
        &resource,
        domain.as_str(),
        &upload_identity,
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
    upload_identity: &str,
    auth_token: Option<&str>,
) -> String {
    let auth_query = match auth_token {
        Some(token) => format!("&auth={}", encode_query_component(token)),
        None => String::new(),
    };
    let query = format!(
        "resource={}&domain={}&upload_identity={}{}",
        encode_query_component(resource),
        encode_query_component(domain),
        encode_query_component(upload_identity),
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
    let Ok(value) =
        js_sys::Reflect::get(file, &wasm_bindgen::JsValue::from_str("webkitRelativePath"))
    else {
        return String::new();
    };
    value.as_string().unwrap_or_default()
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

/// The edge a click landed on, named the way the graph identifies it.
fn graph_edge_focus_request(event: &ev::MouseEvent) -> Option<GraphEdgeId> {
    let pointer_hit = if let Some(window) = web_sys::window()
        && let Some(document) = window.document()
        && let Some(element) = document.element_from_point(
            event.client_x().approx_into(),
            event.client_y().approx_into(),
        ) {
        graph_edge_hit_from_element(element)
    } else {
        None
    };
    let hit = if let Some(hit) = pointer_hit {
        hit
    } else {
        let target = event.target()?;
        let Ok(element) = target.dyn_into::<web_sys::Element>() else {
            return None;
        };
        graph_edge_hit_from_element(element)?
    };
    let source = hit.get_attribute("data-source")?;
    let target = hit.get_attribute("data-target")?;
    let kind = graph_edge_kind_from_label(hit.get_attribute("data-kind")?.as_str())?;
    Some(GraphEdgeId {
        source,
        target,
        kind,
    })
}

fn graph_edge_hit_from_element(element: web_sys::Element) -> Option<web_sys::Element> {
    if let Ok(Some(hit)) = element.closest(".graph-edge-hit") {
        return Some(hit);
    }
    let Ok(Some(group)) = element.closest(".graph-edge-group") else {
        return None;
    };
    group.query_selector(".graph-edge-hit").unwrap_or_default()
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
    active_domain: RwSignal<Option<DomainName>>,
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
    let graph_search_focus_key = RwSignal::new(None::<(GraphTopologyKey, GraphSearch)>);
    let graph_stage_ref = NodeRef::<leptos::html::Div>::new();
    let current_graph_state = RwSignal::new(None::<GraphView>);
    let topology_graph_state = RwSignal::new(None::<GraphView>);
    let topology_key_state = RwSignal::new(None::<GraphTopologyKey>);
    let topology_render_count = RwSignal::new(0_u64);
    let fitted_topology_key = RwSignal::new(None::<GraphTopologyKey>);
    let snapshot_observed_at = RwSignal::new(js_sys::Date::now());
    let freshness_now = RwSignal::new(js_sys::Date::now());
    Effect::new(move |_| {
        let selected_domain = active_domain.get();
        let next_graph = domain().filter(|graph| graph.belongs_to(selected_domain.as_ref()));
        if let Some(graph) = &next_graph {
            let next_key = graph.topology_key();
            if topology_key_state.get_untracked().as_ref() != Some(&next_key) {
                topology_key_state.set(Some(next_key));
                topology_graph_state.set(Some(graph.clone()));
                topology_render_count.update(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("a console session cannot render 2^64 topologies");
                });
            }
            current_graph_state.set(next_graph);
        } else {
            // `Show` removes the graph branch reactively. Keep its last values alive until that
            // unmount finishes so outgoing child computations cannot observe an impossible gap.
            topology_key_state.set(None);
        }
        snapshot_observed_at.set(js_sys::Date::now());
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
        let selected_domain = active_domain.get();
        current_graph_state
            .get()
            .filter(|graph| graph.belongs_to(selected_domain.as_ref()))
    };
    let visible_topology_graph = move || {
        let selected_domain = active_domain.get();
        match topology_graph_state.get() {
            Some(graph) if graph.belongs_to(selected_domain.as_ref()) => Some(graph),
            _ => visible_graph(),
        }
    };
    let current_graph = move || {
        current_graph_state.get().verified(
            "the mounted graph branch retains its last graph until reactive unmount completes",
        )
    };
    let current_topology_graph = move || {
        topology_graph_state.get().verified(
            "the mounted graph branch retains its last topology until reactive unmount completes",
        )
    };
    let active_graph_search = move || GraphSearch::parse(&graph_search.get());
    let domain_lifecycle = move || {
        let Some(selected_domain) = active_domain.get() else {
            return "STOPPED";
        };
        let listed_domains = domains.read();
        // Bounded by the domains of the cluster, which the domain menu lists.
        match listed_domains
            .iter()
            .find(|candidate| candidate.domain == selected_domain)
        {
            Some(domain) => domain.lifecycle_label(),
            None => "STOPPED",
        }
    };
    let graph_freshness = move || {
        if websocket_state.get() != ConsoleConnectionState::Connected {
            return "OFFLINE";
        }
        let age = freshness_now.get() - snapshot_observed_at.get();
        if age <= GRAPH_FRESHNESS_TIMEOUT.as_millis().approx_into::<f64>() {
            "LIVE"
        } else {
            "STALE"
        }
    };
    let focus_graph_bounds = move |graph: &GraphView, bounds: GraphBounds, max_zoom: f64| -> bool {
        let Some(stage) = graph_stage_ref.get() else {
            return false;
        };
        let stage = Extent {
            width: f64::from(stage.client_width()),
            height: f64::from(stage.client_height()),
        };
        let canvas = Extent {
            width: f64::from(graph.canvas_width()),
            height: f64::from(graph.canvas_height()),
        };
        let Some(viewport) = Viewport::framing(stage, canvas, bounds, max_zoom) else {
            return false;
        };
        graph_zoom.set(viewport.zoom);
        graph_pan_x.set(viewport.pan_x);
        graph_pan_y.set(viewport.pan_y);
        true
    };
    let fit_graph = move || {
        if let Some(graph) = visible_topology_graph() {
            focus_graph_bounds(&graph, graph.canvas_bounds(), Viewport::FIT_MAX_ZOOM);
        }
    };
    let focus_graph_edge = move |request: GraphEdgeId| {
        let graph = current_topology_graph();
        let Some(bounds) = graph.edge_focus_bounds(&request) else {
            return;
        };
        focus_graph_bounds(&graph, bounds, Viewport::MAX_ZOOM);
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
        if focus_graph_bounds(&graph, graph.canvas_bounds(), Viewport::FIT_MAX_ZOOM) {
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
        if focus_graph_bounds(&graph, bounds, Viewport::MAX_ZOOM) {
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
                    <span>{move || match visible_graph() {
                        Some(graph) => graph.id,
                        None => "unavailable".to_string(),
                    }}</span>
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
                                match (active_graph_search(), visible_topology_graph()) {
                                    (Some(query), Some(graph)) => {
                                        graph.search_result_count(&query).to_string()
                                    }
                                    _ => String::new(),
                                }
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
                                *zoom = (*zoom - Viewport::ZOOM_STEP).max(Viewport::MIN_ZOOM);
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
                            {move || {
                                let percent: i32 = (graph_zoom.get() * 100.0)
                                    .round()
                                    .checked_approx_into()
                                    .unwrap_or(i32::MAX);
                                format!("{percent}%")
                            }}
                        </button>
                        <button
                            type="button"
                            title="Zoom in"
                            on:click=move |_| graph_zoom.update(|zoom| {
                                *zoom = (*zoom + Viewport::ZOOM_STEP).min(Viewport::MAX_ZOOM);
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
                                    .clamp(Viewport::MIN_ZOOM, Viewport::MAX_ZOOM);
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
                        if let Some(request) = graph_edge_focus_request(&event) {
                            event.prevent_default();
                            event.stop_propagation();
                            focus_graph_edge(request);
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
                                if let Some(request) = graph_edge_focus_request(&event) {
                                    event.prevent_default();
                                    event.stop_propagation();
                                    focus_graph_edge(request);
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
                            <For each={move || current_topology_graph().drawn_edges()} key=move |edge| {
                                (edge.id.clone(), edge.path())
                            } children={move |edge| {
                                let path = edge.path();
                                let id = edge.id.clone();
                                let kind = id.kind;
                                let kind_label = kind.as_ref().to_string();
                                let class = format!("graph-edge {}", kind.css_class());
                                let emphasis_edge = edge.clone();
                                let hover_id = id.clone();
                                let messages_id = id.clone();
                                let bytes_id = id.clone();
                                let batches_id = id.clone();
                                let messages_total_id = id.clone();
                                let bytes_total_id = id.clone();
                                let batches_total_id = id.clone();
                                let flowing_id = id.clone();
                                let route_summary = edge.route_summary();
                                view! {
                                    <g
                                        class="graph-edge-group"
                                        // A pulse only travels an edge that is actually carrying
                                        // records, so a stopped domain draws a still graph.
                                        class:flowing=move || {
                                            visible_graph().is_some_and(|graph| {
                                                graph.edge_statistics(&flowing_id).has_edge_activity()
                                            })
                                        }
                                        class:emphasis=move || {
                                            graph_hover
                                                .get()
                                                .is_some_and(|hover| hover.emphasises_edge(&emphasis_edge))
                                        }
                                        on:mouseenter=move |_| {
                                            graph_hover.set(Some(GraphHover::Edge(hover_id.clone())));
                                        }
                                        on:mouseleave=move |_| graph_hover.set(None)
                                    >
                                        <title>{route_summary}</title>
                                        <path
                                            class="graph-edge-hit"
                                            data-kind=kind_label.clone()
                                            data-source=id.source.clone()
                                            data-target=id.target.clone()
                                            d=path.clone()
                                        />
                                        <path class=format!("graph-edge-shadow {}", kind.css_class()) d=path.clone() />
                                        <path
                                            class=class
                                            data-kind=kind_label
                                            data-source=id.source.clone()
                                            data-target=id.target.clone()
                                            data-feedback=edge.feedback_data()
                                            data-input-side=edge.input_side_data()
                                            data-routes=edge.routes.to_string()
                                            data-messages-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&messages_id)
                                                    .messages_per_second
                                                    .to_string()
                                            }
                                            data-bytes-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&bytes_id)
                                                    .bytes_per_second
                                                    .to_string()
                                            }
                                            data-batches-per-second=move || {
                                                current_graph()
                                                    .edge_statistics(&batches_id)
                                                    .batches_per_second
                                                    .to_string()
                                            }
                                            data-messages-total=move || {
                                                current_graph()
                                                    .edge_statistics(&messages_total_id)
                                                    .messages_total
                                                    .to_string()
                                            }
                                            data-bytes-total=move || {
                                                current_graph()
                                                    .edge_statistics(&bytes_total_id)
                                                    .bytes_total
                                                    .to_string()
                                            }
                                            data-batches-total=move || {
                                                current_graph()
                                                    .edge_statistics(&batches_total_id)
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
                            <For each={move || current_graph().relays.clone()} key=GraphViewRelayKey::of children={move |relay| {
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
                            <For each={move || current_graph().drawn_edges()} key=move |edge| {
                                (
                                    edge.id.clone(),
                                    edge.statistics.messages_per_second.to_bits(),
                                    edge.statistics.bytes_per_second.to_bits(),
                                    edge.statistics.batches_per_second.to_bits(),
                                    edge.statistics.messages_total,
                                    edge.statistics.bytes_total,
                                    edge.statistics.batches_total,
                                )
                            } children={move |edge| {
                                let title = edge.metric_summary();
                                let source = edge.id.source.clone();
                                let target = edge.id.target.clone();
                                let kind_label = edge.id.kind.as_ref().to_string();
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
                            <For each={move || current_graph().nodes.clone()} key=GraphViewNodeKey::of children={move |node| {
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
                                    data-reconnect-wait-ms=match node.reconnect_wait_millis {
                                        Some(value) => value.to_string(),
                                        None => String::new(),
                                    }
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
                            <span>{move || match selected_action_target.get() {
                                Some(target) => target.kind,
                                None => "",
                            }}</span>
                            <strong>{move || match selected_action_target.get() {
                                Some(target) => target.name,
                                None => String::new(),
                            }}</strong>
                        </header>
                        <div class="graph-action-list">
                            <Show when=move || selected_action_target.get().and_then(|target| target.describe_command).is_some() fallback=|| ()>
                                <button
                                    type="button"
                                    on:click=move |_| {
                                        if let Some(target) = selected_action_target.get()
                                            && let Some(command) = target.describe_command
                                        {
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
                                        if let Some(target) = selected_action_target.get()
                                            && let Some(relay) = target.relay
                                        {
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
                            <strong>{move || match selected_relay.get() {
                                Some(relay) => relay.label,
                                None => String::new(),
                            }}</strong>
                        </header>
                        <div class="subscribe-block">
                            <p>
                                "SCHEMA"
                                <em>{move || match selected_relay.get() {
                                    Some(relay) => relay.schema.unwrap_or_default(),
                                    None => String::new(),
                                }}</em>
                            </p>
                            <For
                                each=move || match selected_relay.get() {
                                    Some(relay) => relay.schema_fields,
                                    None => Vec::new(),
                                }
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
    let deadline = started_at + wait_millis.approx_into::<f64>();
    let remaining = RwSignal::new(wait_millis);
    let interval = set_interval_with_handle(
        move || {
            let millis = (deadline - js_sys::Date::now())
                .max(0.0)
                .round()
                .checked_approx_into()
                .unwrap_or(u64::MAX);
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
        let remaining = remaining.get().approx_into::<f64>();
        let total = wait_millis.max(1).approx_into::<f64>();
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
        format!("{:.1}s", millis.approx_into::<f64>() / 1_000.0)
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
                    <strong>{move || match selected_group() {
                        Some(group) => group.branch,
                        None => String::new(),
                    }}</strong>
                </header>
                <div class="subscribe-block">
                    <p>"BRANCH KEY"</p>
                    <div class="schema-row">
                        <span>"schema"</span>
                        <em>{move || match selected_group() {
                            Some(group) => group.key_schema,
                            None => String::new(),
                        }}</em>
                    </div>
                    <For
                        each=move || match selected_group() {
                            Some(group) => group.key_fields,
                            None => Vec::new(),
                        }
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
                        <em>{move || match selected_group() {
                            Some(group) => group.active_branches.to_string(),
                            None => "0".to_string(),
                        }}</em>
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
    terminal_lines: RwSignal<TermLineHistory>,
    transaction_state: impl Fn() -> Option<ActiveTransaction> + Copy + Send + 'static,
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
            return (None, terminal_lines.get().into_lines());
        };
        let tab = subscription_tabs
            .get()
            .into_iter()
            .find(|tab| tab.id == tab_id);
        let lines = match tab {
            Some(tab) => tab.lines.into_lines(),
            None => Vec::new(),
        };
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
                    each=move || subscription_tabs.get()
                    key=|tab| tab.id
                    children={move |tab| {
                        let tab_id = tab.id;
                        let title = tab.title.clone();
                        let state = move || subscription_tabs.with(|tabs| {
                            // The operator controls the number of visible subscription tabs.
                            match tabs.iter().find(|tab| tab.id == tab_id) {
                                Some(tab) => tab.state.clone(),
                                None => SubscriptionTabState::Pending,
                            }
                        });
                        view! {
                            <div
                                class=move || if active_subscription_tab.get() == Some(tab_id) { "tab active subscription-tab" } else { "tab subscription-tab" }
                                data-subscription-state=move || state().label()
                            >
                                <button
                                    type="button"
                                    class="tab-main"
                                    title=tab.subscribe_command.clone()
                                    data-subscription-title=title.clone()
                                    on:click=move |_| {
                                        if !state().can_activate() {
                                            return;
                                        }
                                        active_subscription_tab.set(Some(tab_id));
                                        if collapsed.get() {
                                            collapsed.set(false);
                                        }
                                    }
                                >
                                    <span class=move || if matches!(state(), SubscriptionTabState::Open(_)) { "live-dot" } else { "live-dot paused" }></span>
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
                            .map(|entry| ((tab_id, entry.id), entry.line))
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
                let command = match input_ref.get_untracked() {
                    Some(element) => element.value(),
                    None => input.get_untracked(),
                };
                completion_cycle.set(None);
                command_history.update(|history| history.push(command.as_str()));
                input.set(command.clone());
                run_command(Some(command));
            }>
                <span>{move || {
                    match transaction_state() {
                        Some(ActiveTransaction::Open) => format!("nervix[{} tx]>", domain()),
                        Some(ActiveTransaction::Committing) => {
                            format!("nervix[{} committing]>", domain())
                        }
                        None => format!("nervix[{}]>", domain()),
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
                                let source = match completion_cycle.get_untracked() {
                                    Some(cycle) => cycle.source,
                                    None => input.get_untracked(),
                                };
                                let index = match completion_cycle.get_untracked() {
                                    Some(cycle) => cycle.next_index % suggestion_items.len(),
                                    None => 0,
                                };
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
                            let current = match input_ref.get_untracked() {
                                Some(element) => element.value(),
                                None => input.get_untracked(),
                            };
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
                            let command = match input_ref.get_untracked() {
                                Some(element) => element.value(),
                                None => input.get_untracked(),
                            };
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
            // Stepping back from the oldest entry stays on it.
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
    /// Every edge by its identity, so parallel edges between one pair of items stay apart.
    edges: BTreeMap<GraphEdgeId, GraphViewEdge>,
    groups: Vec<GraphBranchGroup>,
    width: i32,
    height: i32,
}

impl GraphView {
    fn from_dataflow_graph(graph: DataflowGraph) -> Self {
        let layout = LiveGraphLayout::build(
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

        let mut edges = BTreeMap::new();
        for edge in graph.edges {
            let id = GraphEdgeId::from(&edge);
            let route = layout.edges.get(&id);
            let points = match route {
                Some(route) => route.points.clone(),
                None => Vec::new(),
            };
            let drawn = GraphViewEdge {
                points,
                badge: route.and_then(|route| route.badge),
                feedback: route.is_some_and(|route| route.travel == EdgeTravel::Return),
                id: id.clone(),
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
            };
            edges.insert(id, drawn);
        }

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

    /// Whether this is the graph of `domain`. No graph belongs to the absence of a domain.
    fn belongs_to(&self, domain: Option<&DomainName>) -> bool {
        match domain {
            Some(domain) => self.id == domain.as_str(),
            None => false,
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
            edges: self
                .edges
                .values()
                .map(GraphEdgeTopologyKey::from)
                .collect(),
        }
    }

    /// The edges in the order they are drawn.
    fn drawn_edges(&self) -> Vec<GraphViewEdge> {
        self.edges.values().cloned().collect()
    }

    fn edge_statistics(&self, id: &GraphEdgeId) -> GraphStatistics {
        match self.edges.get(id) {
            Some(edge) => edge.statistics,
            None => GraphStatistics::default(),
        }
    }

    fn edge_focus_bounds(&self, id: &GraphEdgeId) -> Option<GraphBounds> {
        let edge = self.edges.get(id)?;
        let mut bounds = None::<GraphBounds>;
        for endpoint in [edge.id.source.as_str(), edge.id.target.as_str()] {
            if let Some(item) = self.item_bounds(endpoint) {
                GraphBounds::include(&mut bounds, item);
            }
        }
        for point in &edge.points {
            GraphBounds::include(&mut bounds, GraphBounds::from_point(point.0, point.1));
        }
        bounds
    }

    fn search_result_bounds(&self, query: &GraphSearch) -> Option<GraphBounds> {
        let mut bounds = None::<GraphBounds>;
        for node in self.nodes.iter().filter(|node| node.matches_search(query)) {
            GraphBounds::include(&mut bounds, GraphBounds::from_rect(node.rect));
        }
        for relay in self
            .relays
            .iter()
            .filter(|relay| relay.matches_search(query))
        {
            GraphBounds::include(&mut bounds, GraphBounds::from_rect(relay.rect));
        }
        bounds
    }

    fn search_result_count(&self, query: &GraphSearch) -> usize {
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
        GraphBounds::canvas(self.width, self.height)
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
    id: GraphEdgeId,
    input_side: Option<DataflowInputSide>,
    routes: u32,
}

impl From<&GraphViewEdge> for GraphEdgeTopologyKey {
    fn from(edge: &GraphViewEdge) -> Self {
        Self {
            id: edge.id.clone(),
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

/// Everything a drawn node card shows. The hit layer re-renders a card exactly when one of these
/// changes, so a node that only gains statistics keeps its element.
#[derive(Clone, PartialEq, Eq, Hash)]
struct GraphViewNodeKey {
    id: String,
    rect: Rect,
    label: String,
    detail: String,
    status: DataflowNodeStatus,
    status_detail: Option<String>,
    reconnect_wait_millis: Option<u64>,
}

impl GraphViewNodeKey {
    fn of(node: &GraphViewNode) -> Self {
        Self {
            id: node.id.clone(),
            rect: node.rect,
            label: node.label.clone(),
            detail: node.detail_label().to_string(),
            status: node.status,
            status_detail: node.status_detail.clone(),
            reconnect_wait_millis: node.reconnect_wait_millis,
        }
    }
}

impl GraphViewNode {
    fn hit_class(&self) -> &'static str {
        match (self.kind, self.status) {
            (NodeKind::Client, DataflowNodeStatus::Ok) => "node-hit client status-ok",
            (NodeKind::Ingestor, DataflowNodeStatus::Ok) => "node-hit ingestor status-ok",
            (NodeKind::Processor, DataflowNodeStatus::Ok) => "node-hit processor status-ok",
            (NodeKind::Emitter, DataflowNodeStatus::Ok) => "node-hit emitter status-ok",
            (NodeKind::Client, DataflowNodeStatus::Waiting) => "node-hit client status-waiting",
            (NodeKind::Ingestor, DataflowNodeStatus::Waiting) => "node-hit ingestor status-waiting",
            (NodeKind::Processor, DataflowNodeStatus::Waiting) => {
                "node-hit processor status-waiting"
            }
            (NodeKind::Emitter, DataflowNodeStatus::Waiting) => "node-hit emitter status-waiting",
            (NodeKind::Client, DataflowNodeStatus::Error) => "node-hit client status-error",
            (NodeKind::Ingestor, DataflowNodeStatus::Error) => "node-hit ingestor status-error",
            (NodeKind::Processor, DataflowNodeStatus::Error) => "node-hit processor status-error",
            (NodeKind::Emitter, DataflowNodeStatus::Error) => "node-hit emitter status-error",
        }
    }

    fn hit_style(&self) -> String {
        graph_position_style(self.rect)
    }

    fn matches_search(&self, search: &GraphSearch) -> bool {
        search.matches(&self.id) || search.matches(&self.label)
    }

    const fn status_label(&self) -> &'static str {
        match self.status {
            DataflowNodeStatus::Ok => "OK",
            DataflowNodeStatus::Waiting => "WAITING",
            DataflowNodeStatus::Error => "ERROR",
        }
    }

    fn kind_label(&self) -> String {
        self.role.kind().as_ref().to_string()
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
        let status = match &self.status_detail {
            Some(detail) => format!("status: {}\n{detail}", self.status_label()),
            None => format!("status: {}", self.status_label()),
        };
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

/// Everything a drawn relay card shows, including the buffer percentages its meter renders. The
/// float percentiles are keyed by their bit pattern because they are compared, never ordered.
#[derive(Clone, PartialEq, Eq, Hash)]
struct GraphViewRelayKey {
    id: String,
    rect: Rect,
    label: String,
    buffer_capacity: Option<u64>,
    buffer_len_p50: Option<u64>,
    buffer_len_p90: Option<u64>,
    buffer_len_p99: Option<u64>,
}

impl GraphViewRelayKey {
    fn of(relay: &GraphViewRelay) -> Self {
        Self {
            id: relay.id.clone(),
            rect: relay.rect,
            label: relay.label.clone(),
            buffer_capacity: relay.statistics.relay_buffer_capacity,
            buffer_len_p50: relay.statistics.relay_buffer_len_p50.map(f64::to_bits),
            buffer_len_p90: relay.statistics.relay_buffer_len_p90.map(f64::to_bits),
            buffer_len_p99: relay.statistics.relay_buffer_len_p99.map(f64::to_bits),
        }
    }
}

impl GraphViewRelay {
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

    fn matches_search(&self, search: &GraphSearch) -> bool {
        search.matches(&self.id) || search.matches(&self.label)
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
        (value / capacity.approx_into::<f64>() * 100.0).clamp(0.0, 100.0)
    }

    fn buffer_capacity_data(&self) -> String {
        match self.statistics.relay_buffer_capacity {
            Some(value) => value.to_string(),
            None => String::new(),
        }
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
        region: &GroupRegion<String>,
        nodes: &[GraphViewNode],
        relays: &[GraphViewRelay],
        edges: &BTreeMap<GraphEdgeId, GraphViewEdge>,
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
        for edge in edges.values().filter(|edge| {
            members.contains(edge.id.source.as_str()) || members.contains(edge.id.target.as_str())
        }) {
            active.extend(edge.branches.iter().map(|branch| branch.branch.as_str()));
        }

        Self {
            branch: region.branch.clone(),
            key_schema: match identity {
                Some(branch) => branch.key_schema.clone(),
                None => String::new(),
            },
            key_fields: match identity {
                Some(branch) => branch.key_fields.clone(),
                None => Vec::new(),
            },
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
        let count = self.active_branches.min(8).approx_into::<f64>();
        format!("{:.2}", 1.0 + count * 0.35)
    }

    fn header_style(&self) -> String {
        match self.header {
            Some(header) => graph_position_style(header),
            None => String::new(),
        }
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

#[derive(Clone)]
struct GraphViewEdge {
    /// The items this edge joins and what travels along it.
    id: GraphEdgeId,
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
        let end = self
            .points
            .last()
            .verified("the empty-points branch above already returned");
        path.push_str(&format!(" L{} {}", end.0, end.1));
        path
    }

    fn metric_style(&self) -> Option<String> {
        self.badge.map(graph_position_style)
    }

    /// A state dependency is looked up rather than delivered, so it ends in a hollow head.
    const fn marker(&self) -> &'static str {
        if self.id.kind.carries_records() {
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
        let subject = match self.id.kind {
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
            self.id.source, self.id.target
        )
    }

    fn feedback_data(&self) -> String {
        self.feedback.to_string()
    }

    fn input_side_data(&self) -> String {
        match self.input_side {
            Some(side) => side.as_ref().to_string(),
            None => String::new(),
        }
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

/// One domain as the domain list describes it.
#[derive(Clone, PartialEq, Eq)]
struct DomainView {
    domain: DomainName,
    pace: DomainPace,
    status: DomainStatus,
}

impl DomainView {
    /// The lifecycle the console reports for this domain.
    fn lifecycle_label(&self) -> &'static str {
        (&self.status).into()
    }

    /// How the domain paces its clock, as the domain menu and `LIST DOMAINS` name it.
    fn pace_label(&self) -> &str {
        self.pace.as_ref()
    }

    /// The line `LIST DOMAINS` prints for this domain.
    fn listing_line(&self) -> String {
        format!(
            "{} pace={} status={}",
            self.domain,
            self.pace.as_ref(),
            self.status.as_ref()
        )
    }

    /// The statement that starts a stopped domain or stops a running one. A paused domain has
    /// none.
    fn state_command(&self) -> Option<&'static str> {
        match self.status {
            DomainStatus::Running => Some("STOP;"),
            DomainStatus::Stopped => Some("START;"),
            DomainStatus::Paused => None,
        }
    }

    /// What the lifecycle button does for this domain.
    fn state_title(&self) -> &'static str {
        match self.status {
            DomainStatus::Running => "Stop domain",
            DomainStatus::Stopped => "Start domain",
            DomainStatus::Paused => "Domain lifecycle",
        }
    }

    /// The lifecycle button's hint, which says the button waits while the session is not
    /// connected.
    fn state_hint(&self, connected: bool) -> &'static str {
        if connected {
            self.state_title()
        } else {
            "Waiting for connection"
        }
    }
}

impl From<DomainInfo> for DomainView {
    fn from(info: DomainInfo) -> Self {
        Self {
            domain: info.domain,
            pace: info.pace,
            status: info.status,
        }
    }
}

/// The active domain as the sidebar shows it.
#[derive(Clone)]
enum SidebarDomain {
    /// The domain list describes the active domain.
    Listed(DomainView),
    /// The domain list does not describe the active domain yet, so only its name is known.
    Unlisted(DomainName),
}

impl SidebarDomain {
    fn name(&self) -> &DomainName {
        match self {
            Self::Listed(domain) => &domain.domain,
            Self::Unlisted(domain) => domain,
        }
    }

    fn pace_label(&self) -> &str {
        match self {
            Self::Listed(domain) => domain.pace_label(),
            Self::Unlisted(_) => "UNKNOWN",
        }
    }

    fn status_label(&self) -> &str {
        match self {
            Self::Listed(domain) => domain.status.as_ref(),
            Self::Unlisted(_) => "UNKNOWN",
        }
    }
}

/// The latest snapshot of one domain's graph and entities.
#[derive(Clone, PartialEq)]
struct DomainSnapshotView {
    dataflow_graph: DataflowGraph,
    entities: Vec<EntityView>,
}

impl DomainSnapshotView {
    fn new(entities: &[DomainEntity], dataflow_graph: DataflowGraph) -> Self {
        let mut entities = entities.iter().map(EntityView::from).collect::<Vec<_>>();
        entities.sort_by(EntityView::sidebar_order);
        Self {
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

impl From<ClusterObserved> for ClusterCounters {
    fn from(cluster: ClusterObserved) -> Self {
        Self {
            running: cluster.running_domains,
            nodes: cluster.graph_nodes,
            relays: cluster.relays,
        }
    }
}

/// What a sidebar entity is.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum EntityKind {
    Model(ModelKind),
    Resource,
}

impl EntityKind {
    /// The kind as the vocabulary spells it.
    fn name(self) -> &'static str {
        match self {
            Self::Model(kind) => kind.as_str(),
            Self::Resource => "resource",
        }
    }

    /// Whether the entity is a wire schema of any encoding.
    fn is_wire_schema(self) -> bool {
        matches!(
            self,
            Self::Model(
                ModelKind::WireJsonSchema | ModelKind::WireCborSchema | ModelKind::WireAvroSchema
            )
        )
    }
}

/// One entity of the active domain, as the sidebar lists it.
#[derive(Clone, PartialEq, Eq, Hash)]
struct EntityView {
    kind: EntityKind,
    name: String,
    /// What the sidebar shows beside the name: a model's kind, or the latest completed version of
    /// a resource.
    detail: String,
}

impl EntityView {
    /// The order the sidebar lists entities in: by kind name, then by name.
    fn sidebar_order(&self, other: &Self) -> Ordering {
        self.kind
            .name()
            .cmp(other.kind.name())
            .then_with(|| self.name.cmp(&other.name))
    }

    /// The statement the sidebar runs when the entity is clicked, for the kinds that describe
    /// themselves.
    fn describe_command(&self) -> Option<String> {
        match self.kind {
            EntityKind::Model(ModelKind::Endpoint) => {
                Some(format!("DESCRIBE ENDPOINT {};", self.name))
            }
            EntityKind::Resource => Some(format!("DESCRIBE RESOURCE {};", self.name)),
            EntityKind::Model(_) => None,
        }
    }
}

impl From<&DomainEntity> for EntityView {
    fn from(entity: &DomainEntity) -> Self {
        match entity {
            DomainEntity::Model(node) => Self {
                kind: EntityKind::Model(node.kind),
                name: node.identifier.as_str().to_string(),
                detail: node.kind.as_str().replace('_', " ").to_ascii_uppercase(),
            },
            DomainEntity::Resource {
                name,
                latest_version,
            } => {
                let detail = match latest_version {
                    Some(version) => format!("v{version}"),
                    None => "catalog".to_string(),
                };
                Self {
                    kind: EntityKind::Resource,
                    name: name.as_str().to_string(),
                    detail,
                }
            }
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
    Edge(GraphEdgeId),
}

impl GraphHover {
    fn emphasises_item(&self, id: &str) -> bool {
        match self {
            Self::Item(hovered) => hovered == id,
            Self::Edge(edge) => edge.source == id || edge.target == id,
        }
    }

    fn emphasises_edge(&self, edge: &GraphViewEdge) -> bool {
        match self {
            Self::Item(hovered) => *hovered == edge.id.source || *hovered == edge.id.target,
            Self::Edge(hovered) => *hovered == edge.id,
        }
    }
}

fn graph_position_style(rect: Rect) -> String {
    format!(
        "left: {}px; top: {}px; width: {}px; height: {}px;",
        rect.x, rect.y, rect.width, rect.height
    )
}

#[derive(Clone)]
struct TermLine {
    kind: TermLineKind,
    text: String,
}

const MAX_HISTORY_RECORDS: usize = 256;
const MAX_HISTORY_BYTES: usize = 256 * 1024;
const HISTORY_MARKER: &str = "history limit reached; earlier lines were omitted";
const HISTORY_SUFFIX: &str = "… [line truncated]";
const HISTORY_PAYLOAD_RECORDS: usize = MAX_HISTORY_RECORDS - 1;
const HISTORY_PAYLOAD_BYTES: usize = MAX_HISTORY_BYTES - HISTORY_MARKER.len();
const _: () = assert!(HISTORY_PAYLOAD_RECORDS > 0);
const _: () = assert!(HISTORY_PAYLOAD_BYTES > HISTORY_SUFFIX.len());

/// A rendered line and its stable identity. Trimming the front of a history never reuses an ID,
/// so the keyed browser view cannot show stale content after an eviction.
#[derive(Clone)]
struct HistoryEntry {
    id: u64,
    line: TermLine,
}

/// The console's displayed history, bounded independently for the REPL and every subscription.
/// One record and its bytes are reserved for the visible overflow notice.
#[derive(Clone)]
struct TermLineHistory {
    lines: VecDeque<HistoryEntry>,
    bytes: usize,
    next_id: u64,
    dropped: bool,
}

impl Default for TermLineHistory {
    fn default() -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            next_id: 1,
            dropped: false,
        }
    }
}

impl TermLineHistory {
    fn push(&mut self, mut line: TermLine) {
        if line.text.len() > HISTORY_PAYLOAD_BYTES {
            let prefix_limit = HISTORY_PAYLOAD_BYTES
                .checked_sub(HISTORY_SUFFIX.len())
                .assured("the history payload capacity exceeds its truncation suffix");
            let boundary = line.text.floor_char_boundary(prefix_limit);
            line.text.truncate(boundary);
            line.text.push_str(HISTORY_SUFFIX);
            self.dropped = true;
        }
        let line_bytes = line.text.len();
        while self.lines.len() >= HISTORY_PAYLOAD_RECORDS
            || self
                .bytes
                .checked_add(line_bytes)
                .assured("both byte counts are bounded by the 256 KiB history capacity")
                > HISTORY_PAYLOAD_BYTES
        {
            let evicted = self
                .lines
                .pop_front()
                .verified("the capacity condition requires an existing line to evict");
            self.bytes = self
                .bytes
                .checked_sub(evicted.line.text.len())
                .assured("the retained byte count includes the evicted line");
            self.dropped = true;
        }
        self.bytes = self
            .bytes
            .checked_add(line_bytes)
            .assured("the capacity loop left room for the new line");
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .assured("one browser session cannot display 2^64 lines");
        self.lines.push_back(HistoryEntry { id, line });
    }

    fn extend(&mut self, lines: impl IntoIterator<Item = TermLine>) {
        for line in lines {
            self.push(line);
        }
    }

    fn into_lines(self) -> Vec<HistoryEntry> {
        let mut lines = Vec::with_capacity(
            self.lines
                .len()
                .checked_add(usize::from(self.dropped))
                .assured("the history stores fewer than 256 lines"),
        );
        if self.dropped {
            lines.push(HistoryEntry {
                id: 0,
                line: TermLine::info(HISTORY_MARKER),
            });
        }
        lines.extend(self.lines);
        lines
    }
}

impl TermLine {
    fn prompt(text: impl Into<String>, transaction: Option<ActiveTransaction>) -> Self {
        let prompt = match transaction {
            Some(ActiveTransaction::Open) => "nervix[tx]>",
            Some(ActiveTransaction::Committing) => "nervix[committing]>",
            None => "nervix>",
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
    use leptos::prelude::Owner;
    use nervix_client_wire::{DomainList, DomainsObserved, OutcomeOrigin, Reply, SourceSpan};
    use nervix_dataflow_graph::{
        DataflowBranchStatistics, DataflowEdge, DataflowNode, DataflowProcessorKind,
    };
    use nervix_models::{
        DomainClockPeriod, DomainClockSkew, ModelName, NodeRef, ResourceName, TransactionPosition,
    };

    use super::*;

    fn subscription_signals(state: SubscriptionTabState) -> WebConsoleSignals {
        let name = SubscriptionName::parse("live").assured("the test subscription name is valid");
        let domain = DomainName::parse("tenant").assured("the test domain name is valid");
        WebConsoleSignals {
            terminal_lines: RwSignal::new(TermLineHistory::default()),
            suggestions: RwSignal::new(Vec::new()),
            domain_snapshots: RwSignal::new(BTreeMap::new()),
            cluster_counters: RwSignal::new(ClusterCounters::default()),
            active_domain: RwSignal::new(Some(domain.clone())),
            transaction_status: RwSignal::new(None),
            domains: RwSignal::new(Vec::new()),
            resource_details: RwSignal::new(BTreeMap::new()),
            subscription_tabs: RwSignal::new(vec![SubscriptionTabView {
                id: 1,
                state,
                name,
                domain,
                relay: "orders".to_string(),
                filter: String::new(),
                sample_rate_index: 0,
                title: "orders".to_string(),
                subscribe_command: "SUBSCRIBE live TO orders;".to_string(),
                lines: TermLineHistory::default(),
            }]),
            active_subscription_tab: RwSignal::new(Some(1)),
            domains_loaded: RwSignal::new(true),
            auth_token: RwSignal::new(None),
            auth_error: RwSignal::new(None),
        }
    }

    fn test_stream() -> TabStream {
        TabStream {
            subscription: SubscriptionHandle {
                name: SubscriptionName::parse("live").assured("the test name is valid"),
                generation: NonZeroU64::MIN,
            },
            schema: RowSchema {
                fields: Vec::new(),
                branch: None,
            },
        }
    }

    #[test]
    fn changing_credentials_clears_the_previous_users_private_view() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
            signals
                .suggestions
                .set(vec!["private completion".to_string()]);
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::output("private output")));

            signals.clear_authenticated_view();

            assert_eq!(signals.active_domain.get_untracked(), None);
            assert_eq!(signals.transaction_status.get_untracked(), None);
            assert!(signals.domains.get_untracked().is_empty());
            assert!(signals.domain_snapshots.get_untracked().is_empty());
            assert!(signals.resource_details.get_untracked().is_empty());
            assert!(!signals.domains_loaded.get_untracked());
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
            assert!(signals.suggestions.get_untracked().is_empty());
            assert!(
                signals
                    .terminal_lines
                    .get_untracked()
                    .into_lines()
                    .is_empty()
            );
        });
    }

    #[test]
    fn subscription_tab_states_expose_only_usable_tabs() {
        let stream = test_stream();
        let cases = [
            (SubscriptionTabState::Pending, "pending", false),
            (SubscriptionTabState::Open(stream.clone()), "active", true),
            (SubscriptionTabState::Interrupted, "interrupted", true),
            (SubscriptionTabState::Restoring, "restoring", true),
            (SubscriptionTabState::Closing(None), "closing", false),
            (SubscriptionTabState::Closing(Some(stream)), "closing", true),
        ];
        for (state, label, can_activate) in cases {
            assert_eq!(state.label(), label);
            assert_eq!(state.can_activate(), can_activate, "state {label}");
        }
    }

    #[test]
    fn a_tab_accepts_rows_only_from_its_open_generation() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let current = stream.subscription.clone();
            let next = SubscriptionHandle {
                name: current.name.clone(),
                generation: NonZeroU64::new(2).assured("two is nonzero"),
            };
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(tabs[0].streams(&current));
                assert!(tabs[0].stream_schema(&current).is_some());
                assert!(!tabs[0].streams(&next));
                assert!(tabs[0].stream_schema(&next).is_none());
            });
            signals.subscription_tabs.update(|tabs| {
                tabs[0].state = SubscriptionTabState::Closing(Some(stream));
            });
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(!tabs[0].streams(&current));
                assert!(tabs[0].stream_schema(&current).is_none());
            });
        });
    }

    #[test]
    fn interrupted_subscription_restores_once_and_rejects_its_previous_stream() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let mut requests = SessionRequests::new();

            interrupt_subscription_tabs(signals);
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Interrupted));
                assert!(!tabs[0].streams(&stream.subscription));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains("delivery interrupted")
                );
            });

            queue_subscription_restorations(signals, &mut requests);
            queue_subscription_restorations(signals, &mut requests);
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Restoring)
            }));
            requests.confirm_leader();
            let messages = requests.release_held();
            assert_eq!(messages.len(), 1, "one restore is held per interrupted tab");
            let ClientRequest::Subscribe(request) = &messages[0].request else {
                panic!("the held request restores the subscription");
            };
            assert_eq!(request.statement, "SUBSCRIBE live TO orders;");
        });
    }

    #[test]
    fn disconnect_forgets_a_closing_stream_and_its_selected_tab() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(test_stream())));
            interrupt_subscription_tabs(signals);
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
        });
    }

    #[test]
    fn closing_an_open_tab_waits_for_the_matching_delete_reply() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let request = signals
                .begin_subscription_close(1)
                .assured("an open tab needs an unsubscribe request");
            assert_eq!(request.subscription, stream.subscription.name);
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Closing(Some(_)))
            }));
            assert!(signals.begin_subscription_close(1).is_none());

            apply_unsubscribe_outcome(
                signals,
                1,
                &request,
                UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Deleted(stream.subscription),
                    message: "deleted".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            assert!(signals.subscription_tabs.get_untracked().is_empty());
        });
    }

    #[test]
    fn closing_a_pending_or_interrupted_tab_never_sends_a_stale_delete() {
        for state in [
            SubscriptionTabState::Pending,
            SubscriptionTabState::Restoring,
        ] {
            Owner::new().with(|| {
                let signals = subscription_signals(state);
                assert!(signals.begin_subscription_close(1).is_none());
                assert!(signals.subscription_tabs.with_untracked(|tabs| {
                    matches!(&tabs[0].state, SubscriptionTabState::Closing(None))
                }));
                assert!(signals.begin_subscription_close(1).is_none());
            });
        }
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Interrupted);
            assert!(signals.begin_subscription_close(1).is_none());
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
            assert!(signals.begin_subscription_close(1).is_none());
        });
    }

    #[test]
    fn failed_unsubscribe_keeps_the_tab_active_and_reports_one_error_prefix() {
        Owner::new().with(|| {
            let name = SubscriptionName::parse("live").assured("the test name is valid");
            let handle = SubscriptionHandle {
                name: name.clone(),
                generation: NonZeroU64::MIN,
            };
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(TabStream {
                subscription: handle,
                schema: RowSchema {
                    fields: Vec::new(),
                    branch: None,
                },
            })));
            let request = UnsubscribeRequest { subscription: name };
            apply_unsubscribe_outcome(
                signals,
                1,
                &request,
                UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Failed,
                    message: "server refused deletion".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            let tab = signals.subscription_tabs.get_untracked().remove(0);
            assert!(matches!(tab.state, SubscriptionTabState::Open(_)));
            assert_eq!(
                tab.lines.into_lines()[0].line.text,
                "error: server refused deletion"
            );
        });
    }

    #[test]
    fn acknowledged_unsubscribe_removes_only_the_matching_closing_tab() {
        Owner::new().with(|| {
            let name = SubscriptionName::parse("live").assured("the test name is valid");
            let handle = SubscriptionHandle {
                name: name.clone(),
                generation: NonZeroU64::MIN,
            };
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(TabStream {
                subscription: handle.clone(),
                schema: RowSchema {
                    fields: Vec::new(),
                    branch: None,
                },
            })));
            let request = UnsubscribeRequest { subscription: name };

            apply_unsubscribe_outcome(
                signals,
                1,
                &request,
                UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Deleted(handle),
                    message: "subscription deleted".to_string(),
                    diagnostics: Vec::new(),
                },
            );

            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
        });
    }

    #[test]
    fn a_delete_reply_for_another_generation_cannot_close_the_current_tab() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(stream.clone())));
            let request = UnsubscribeRequest {
                subscription: stream.subscription.name.clone(),
            };
            let other_generation = SubscriptionHandle {
                name: stream.subscription.name,
                generation: NonZeroU64::new(2).assured("two is nonzero"),
            };

            apply_unsubscribe_outcome(
                signals,
                1,
                &request,
                UnsubscribeOutcome {
                    disposition: UnsubscribeDisposition::Deleted(other_generation),
                    message: "deleted".to_string(),
                    diagnostics: Vec::new(),
                },
            );

            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Open(_)));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains("another subscription deletion")
                );
            });
        });
    }

    #[test]
    fn rejected_unsubscribe_restores_the_tab_and_reports_the_failure() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(stream.clone())));
            let mut requests = SessionRequests::new();
            let issued = requests.issue(ConsoleRequest::SubscriptionStop {
                tab_id: 1,
                request: UnsubscribeRequest {
                    subscription: stream.subscription.name,
                },
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: issued,
                    body: ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                        rejection: nervix_client_wire::RequestRejection::InvalidRequest,
                        field: None,
                        message: "delete was rejected".to_string(),
                    }),
                },
            );

            assert!(matches!(step, SessionStep::Continue));
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Open(_))
            }));
            assert!(
                signals.terminal_lines.get_untracked().into_lines()[0]
                    .line
                    .text
                    .contains("delete was rejected")
            );
        });
    }

    #[test]
    fn a_new_subscription_activates_only_after_the_open_reply() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            signals.active_subscription_tab.set(None);
            let mut requests = SessionRequests::new();
            let stream = test_stream();
            apply_subscribe_outcome(
                signals,
                &mut requests,
                1,
                "SUBSCRIBE live TO orders;",
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                        subscription: stream.subscription.clone(),
                        domain: DomainName::parse("tenant").assured("the test domain is valid"),
                        relay: nervix_models::RelayName::parse("orders")
                            .assured("the test relay is valid"),
                        subscription_type: SubscriptionType::Row,
                        schema: stream.schema,
                    })),
                    message: String::new(),
                    diagnostics: Vec::new(),
                },
            );
            assert_eq!(signals.active_subscription_tab.get_untracked(), Some(1));
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Open(_))
            }));
        });
    }

    #[test]
    fn a_failed_new_subscription_leaves_no_live_tab() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests = SessionRequests::new();
            apply_subscribe_outcome(
                signals,
                &mut requests,
                1,
                "SUBSCRIBE live TO orders;",
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Failed,
                    message: "relay is unavailable".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
            assert!(
                signals.terminal_lines.get_untracked().into_lines()[0]
                    .line
                    .text
                    .contains("relay is unavailable")
            );
        });
    }

    #[test]
    fn a_failed_restore_remains_desired_and_reports_its_error() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Restoring);
            let mut requests = SessionRequests::new();
            apply_subscribe_outcome(
                signals,
                &mut requests,
                1,
                "SUBSCRIBE live TO orders;",
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Failed,
                    message: "leader is changing".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Interrupted));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains("leader is changing")
                );
            });
        });
    }

    #[test]
    fn closing_an_in_flight_restore_deletes_its_late_success() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Closing(None));
            let mut requests = SessionRequests::new();
            let name = SubscriptionName::parse("live").assured("the test name is valid");
            let handle = SubscriptionHandle {
                name,
                generation: NonZeroU64::MIN,
            };
            apply_subscribe_outcome(
                signals,
                &mut requests,
                1,
                "SUBSCRIBE live TO orders;",
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                        subscription: handle.clone(),
                        domain: DomainName::parse("tenant").assured("the test domain is valid"),
                        relay: nervix_models::RelayName::parse("orders")
                            .assured("the test relay is valid"),
                        subscription_type: SubscriptionType::Row,
                        schema: RowSchema {
                            fields: Vec::new(),
                            branch: None,
                        },
                    })),
                    message: String::new(),
                    diagnostics: Vec::new(),
                },
            );
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(
                    &tabs[0].state,
                    SubscriptionTabState::Closing(Some(stream))
                        if stream.subscription == handle
                )
            }));
            requests.confirm_leader();
            let messages = requests.release_held();
            assert_eq!(messages.len(), 1);
            assert!(matches!(
                &messages[0].request,
                ClientRequest::Unsubscribe(request) if request.subscription == handle.name
            ));
        });
    }

    #[test]
    fn terminal_history_bounds_records_and_keeps_render_keys_stable() {
        let mut history = TermLineHistory::default();
        for index in 0..300 {
            history.push(TermLine::output(format!("line {index}")));
        }
        let lines = history.into_lines();
        assert_eq!(lines.len(), MAX_HISTORY_RECORDS);
        assert_eq!(lines[0].line.text, HISTORY_MARKER);
        assert_eq!(lines[1].line.text, "line 45");
        assert_eq!(lines[1].id, 46);
        assert_eq!(lines[255].line.text, "line 299");
        assert_eq!(lines[255].id, 300);
    }

    #[test]
    fn terminal_history_bounds_utf8_bytes_and_exposes_truncation() {
        let mut history = TermLineHistory::default();
        history.push(TermLine::output("é".repeat(MAX_HISTORY_BYTES)));
        let lines = history.into_lines();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].line.text, HISTORY_MARKER);
        assert!(lines[1].line.text.ends_with(HISTORY_SUFFIX));
        let retained_bytes = lines
            .iter()
            .map(|entry| entry.line.text.len())
            .sum::<usize>();
        assert!(retained_bytes <= MAX_HISTORY_BYTES);
        assert!(
            lines[1]
                .line
                .text
                .is_char_boundary(lines[1].line.text.len())
        );
    }

    #[test]
    fn untracked_domain_push_cannot_discard_a_pending_websocket_request() {
        let mut requests = SessionRequests::new();
        let attach = requests.issue(ConsoleRequest::AttachTransaction(
            AttachTransactionRequest {
                transaction_id: "transaction".to_string(),
            },
        ));
        let attach = requests.dispatch(attach);

        let pushed = requests.route(ServerMessage::Event(ServerEvent::Domains(
            DomainsObserved {
                domains: Vec::new(),
            },
        )));
        assert!(matches!(pushed, Routed::Event(_)));
        let untracked = requests.route(reply(
            request_id(99),
            ReplyBody::DomainList(DomainList {
                domains: Vec::new(),
            }),
        ));
        assert!(matches!(untracked, Routed::Untracked));

        let Routed::Reply(answered) = requests.route(reply(attach.request_id, attached())) else {
            panic!("an untracked websocket domain response discarded the pending attach");
        };
        assert!(matches!(
            answered.request.request,
            ConsoleRequest::AttachTransaction(_)
        ));
    }

    #[test]
    fn command_with_temporarily_unknown_leader_keeps_its_redirect() {
        let disposition = CommandDisposition::NotLeader(LeaderRedirect { leader: None });
        assert!(
            command_redirect(&disposition).is_some(),
            "a leaderless redirect must keep the command pending for another connection"
        );
    }

    #[test]
    fn ordered_requests_wait_for_the_leader_and_keep_their_order_across_a_reconnect() {
        let mut requests = SessionRequests::new();
        let first = requests.issue(repl_command("CREATE SCHEMA first ( value I64 );"));
        let second = requests.issue(repl_command("CREATE SCHEMA second ( value I64 );"));
        assert!(
            requests.accept(first).is_none(),
            "an ordered request waits until the server confirms that it leads"
        );
        assert!(requests.accept(second).is_none());
        let completion = requests.issue(suggest("CREATE "));
        assert!(
            requests.accept(completion).is_some(),
            "a completion request is sent at once"
        );

        requests.confirm_leader();
        let sent = requests.release_held();
        assert_eq!(
            sent_queries(&sent),
            vec![
                "CREATE SCHEMA first ( value I64 );",
                "CREATE SCHEMA second ( value I64 );",
            ]
        );
        assert_eq!(request_numbers(&sent), vec![2, 3]);

        requests.end_connection();
        assert!(
            requests.release_held().is_empty(),
            "the next connection waits for its own leader confirmation"
        );
        requests.confirm_leader();
        let resent = requests.release_held();
        assert_eq!(sent_queries(&resent), sent_queries(&sent));
        assert_eq!(
            request_numbers(&resent),
            vec![1, 2],
            "a new connection numbers its requests from one, and the completion request is not \
             sent again"
        );
        assert_eq!(
            execution_references(&resent),
            execution_references(&sent),
            "a command sent again keeps its execution reference"
        );
    }

    #[test]
    fn repeated_replay_keeps_every_unanswered_request_and_drops_a_stale_close() {
        let mut requests = SessionRequests::new();
        requests.confirm_leader();
        let first = requests.issue(repl_command("first"));
        let second = requests.issue(repl_command("second"));
        let third = requests.issue(repl_command("third"));
        let first = requests
            .accept(first)
            .assured("the leader can send the first request");
        let second = requests
            .accept(second)
            .assured("the leader can send the second request");
        let third = requests
            .accept(third)
            .assured("the leader can send the third request");
        let sent = [first, second, third];
        assert!(requests.answer(sent[0].request_id).is_some());
        let name = SubscriptionName::parse("closing").assured("the test name is valid");
        let closing = requests.issue(ConsoleRequest::SubscriptionStop {
            tab_id: 9,
            request: UnsubscribeRequest { subscription: name },
        });
        assert!(requests.accept(closing).is_some());

        requests.end_connection();
        requests.confirm_leader();
        let replayed = requests.release_held();
        assert_eq!(
            replayed.len(),
            2,
            "the close belongs to the ended connection"
        );
        assert_eq!(sent_queries(&replayed), vec!["second", "third"]);
        assert_eq!(
            execution_references(&replayed),
            execution_references(&sent[1..])
        );

        requests.end_connection();
        requests.confirm_leader();
        let replayed_again = requests.release_held();
        assert_eq!(sent_queries(&replayed_again), vec!["second", "third"]);
        assert_eq!(
            execution_references(&replayed_again),
            execution_references(&sent[1..])
        );
    }

    #[test]
    fn unknown_command_outcome_retries_with_its_execution_reference() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests = SessionRequests::new();
            let issued = requests.issue(repl_command("CREATE SCHEMA pending ( value I64 );"));
            let order = issued.order;
            let ConsoleRequest::Command { request, purpose } = issued.request else {
                panic!("the test issued a command");
            };
            let reference = request.execution_reference.clone();
            let mut outcome = completed_outcome("leadership moved during admission");
            outcome.disposition = CommandDisposition::OutcomeUnknown(
                nervix_client_wire::UnknownOutcomeCause::LeadershipLost,
            );

            let step =
                apply_command_outcome(signals, &mut requests, order, request, purpose, outcome);
            assert!(matches!(step, SessionStep::Reconnect));
            assert!(
                signals
                    .terminal_lines
                    .get_untracked()
                    .into_lines()
                    .is_empty()
            );

            requests.end_connection();
            requests.confirm_leader();
            let resent = requests.release_held();
            assert_eq!(sent_commands(&resent)[0].execution_reference, reference);
        });
    }

    #[test]
    fn commands_held_for_an_attach_keep_their_issue_order_whatever_order_their_replies_take() {
        let mut requests = SessionRequests::new();
        requests.confirm_leader();
        let first = requests.issue(repl_command("first"));
        let second = requests.issue(repl_command("second"));
        let first = requests
            .accept(first)
            .assured("a ready session sends an ordered request at once");
        let second = requests
            .accept(second)
            .assured("a ready session sends an ordered request at once");
        for detached in [second.request_id, first.request_id] {
            let Routed::Reply(answered) = requests.route(reply(detached, detached_command()))
            else {
                panic!("the reply answers a request in flight");
            };
            requests.hold_again(answered.request);
        }

        let attach = requests.issue(ConsoleRequest::AttachTransaction(
            AttachTransactionRequest {
                transaction_id: "transaction".to_string(),
            },
        ));
        let attach = requests.dispatch(attach);
        assert!(
            requests.release_held().is_empty(),
            "held commands wait while the transaction is being attached"
        );
        let third = requests.issue(repl_command("third"));
        assert!(
            requests.accept(third).is_none(),
            "a command issued during the attach waits behind the held ones"
        );
        let attach_reply = requests.route(reply(attach.request_id, attached()));
        assert!(matches!(attach_reply, Routed::Reply(_)));

        let resent = requests.release_held();
        assert_eq!(sent_queries(&resent), vec!["first", "second", "third"]);
    }

    #[test]
    fn only_the_latest_completion_request_is_awaited() {
        let mut requests = SessionRequests::new();
        let earlier = requests.issue(suggest("SH"));
        let earlier = requests
            .accept(earlier)
            .assured("a completion request is sent at once");
        let later = requests.issue(suggest("SHOW"));
        let later = requests
            .accept(later)
            .assured("a completion request is sent at once");
        let suggestions = || {
            ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                suggestions: Vec::new(),
            })
        };

        let stale = requests.route(reply(earlier.request_id, suggestions()));
        assert!(matches!(stale, Routed::Untracked));
        let latest = requests.route(reply(later.request_id, suggestions()));
        assert!(matches!(latest, Routed::Reply(_)));
    }

    #[test]
    fn successful_commands_without_output_add_no_terminal_line() {
        let lines = command_outcome_lines(completed_outcome(""), "CREATE DOMAIN quiet");

        assert!(lines.is_empty());
    }

    #[test]
    fn a_command_of_several_statements_shows_the_outcome_of_each() {
        let query = "CREATE SCHEMA a ( value I64 ); CREATE SCHEMA a ( value I64 );";
        let mut outcome = completed_outcome("ignored for a command of several statements");
        outcome.statements = vec![
            StatementOutcome {
                disposition: StatementDisposition::Completed {
                    already_existed: false,
                },
                message: "created schema 'a'".to_string(),
                diagnostics: Vec::new(),
            },
            StatementOutcome {
                disposition: StatementDisposition::Failed,
                message: "schema 'a' already exists".to_string(),
                diagnostics: vec![diagnostic("duplicate schema", 45, 46)],
            },
            StatementOutcome {
                disposition: StatementDisposition::NotLeader(LeaderRedirect { leader: None }),
                message: String::new(),
                diagnostics: Vec::new(),
            },
        ];

        let lines = command_outcome_lines(outcome, query)
            .into_iter()
            .map(|line| line.text)
            .collect::<Vec<_>>();

        assert_eq!(
            lines,
            vec![
                "created schema 'a'",
                "error: schema 'a' already exists",
                "- a at 45..46: duplicate schema",
                "topology: not-a-leader",
                "- no diagnostics provided",
            ]
        );
    }

    #[test]
    fn a_diagnostic_shows_the_text_of_a_span_only_on_character_boundaries_within_the_query() {
        let query = "CREATE ü";

        assert_eq!(
            diagnostic_line(query, diagnostic("bad name", 7, 9)).text,
            "- ü at 7..9: bad name"
        );
        for (start, end) in [(7, 8), (7, 42), (3, 3)] {
            assert_eq!(
                diagnostic_line(query, diagnostic("bad name", start, end)).text,
                "- bad name",
                "span {start}..{end}"
            );
        }
        let unplaced = Diagnostic {
            message: "bad name".to_string(),
            span: None,
        };
        assert_eq!(diagnostic_line(query, unplaced).text, "- bad name");
    }

    #[test]
    fn resource_description_lists_each_usage_under_the_version_it_pins() {
        let message = [
            "resource: lookup_bundle",
            "latest: 2",
            "versions: 1,2",
            "version_details:",
            "- version=1 file_count=1 total_bytes=24",
            "  entries:",
            "  - type=file path=lookup.jsonl size=24 checksum=first",
            "- version=2 file_count=1 total_bytes=24",
            "  entries:",
            "  - type=file path=lookup.jsonl size=24 checksum=second",
            "usages:",
            "- kind=client name=lookup_store version=2",
            "- kind=hash_map name=lookup_by_id version=2",
        ]
        .join("\n");

        let detail = ResourceDetailView::from_description(completed_outcome(&message));

        let versions = detail
            .versions
            .iter()
            .map(|version| version.version)
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![1, 2]);
        let first = &detail.versions[0];
        assert_eq!(first.files.len(), 1);
        assert!(first.usages.is_empty());
        let second = &detail.versions[1];
        assert_eq!(second.files.len(), 1);
        let usages = second
            .usages
            .iter()
            .map(|usage| format!("{} {}", usage.kind, usage.name))
            .collect::<Vec<_>>();
        assert_eq!(usages, vec!["CLIENT lookup_store", "HASH MAP lookup_by_id"]);
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
        let edges = edge_map([view_edge(
            "relay:telemetry_by_site",
            "junction:route_site",
            &["site=cdg-1"],
        )]);

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

        let group = GraphBranchGroup::new(&region, &[], &relays, &BTreeMap::new());

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

        let link = graph
            .edges
            .values()
            .next()
            .expect("the state link must be drawn");
        assert_eq!(link.metric_style(), None);
        assert_eq!(link.id.kind.css_class(), "graph-edge--state-link");
        assert_eq!(link.marker(), "url(#graph-arrow-hollow)");
        assert_eq!(
            link.route_summary(),
            "relay:reference → generator:ticks: materialized state"
        );
    }

    #[test]
    fn a_relay_read_as_input_and_as_state_draws_two_routes() {
        let records = edge("relay:events", "junction:enrich");
        let state = DataflowEdge::data(
            "relay:events",
            "junction:enrich",
            DataflowEdgeKind::StateLink,
        );
        let graph = GraphView::from_dataflow_graph(DataflowGraph {
            domain: "parallel_demo".to_string(),
            statistics: DataflowStatistics::default(),
            nodes: vec![
                relay("relay:events", "events", None),
                junction("junction:enrich", "enrich", None),
            ],
            edges: vec![records.clone(), state.clone()],
        });

        let records = &graph.edges[&GraphEdgeId::from(&records)];
        let state = &graph.edges[&GraphEdgeId::from(&state)];
        assert!(!records.points.is_empty() && !state.points.is_empty());
        assert_ne!(
            records.path(),
            state.path(),
            "the record input and the state read must not be drawn on one line"
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
            &BTreeMap::new(),
        );
        let busy = GraphBranchGroup::new(
            &region,
            &[],
            &[view_relay(
                "relay:a",
                Some(site_branch()),
                &["site=iad-1", "site=lhr-1", "site=sfo-1"],
            )],
            &BTreeMap::new(),
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
            .values()
            .find(|edge| edge.id.source == "ingestor:mqtt")
            .expect("the ingest edge must exist");
        let second = graph
            .edges
            .values()
            .find(|edge| edge.id.target == "emitter:redis")
            .expect("the emit edge must exist");

        let hover = GraphHover::Item("ingestor:mqtt".to_string());
        assert!(hover.emphasises_item("ingestor:mqtt"));
        assert!(!hover.emphasises_item("emitter:redis"));
        assert!(hover.emphasises_edge(first));
        assert!(!hover.emphasises_edge(second));

        let hover = GraphHover::Edge(GraphEdgeId {
            source: "relay:telemetry".to_string(),
            target: "emitter:redis".to_string(),
            kind: DataflowEdgeKind::Data,
        });
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

        let search = |query: &str| GraphSearch::parse(query).expect("the query is long enough");
        assert_eq!(graph.search_result_count(&search("telemetry")), 2);
        assert_eq!(GraphSearch::parse("t"), None, "one letter is too broad");
        assert!(graph.search_result_bounds(&search("telemetry")).is_some());
        assert!(graph.search_result_bounds(&search("nothing")).is_none());
    }

    #[test]
    fn domain_lifecycle_reports_the_three_states() {
        let domain = |status: DomainStatus| DomainView {
            domain: domain_name("demo"),
            pace: DomainPace::Unpaced,
            status,
        };
        assert_eq!(domain(DomainStatus::Running).lifecycle_label(), "RUNNING");
        assert_eq!(domain(DomainStatus::Paused).lifecycle_label(), "PAUSED");
        assert_eq!(domain(DomainStatus::Stopped).lifecycle_label(), "STOPPED");
    }

    #[test]
    fn domain_lifecycle_button_toggles_only_a_running_or_stopped_domain() {
        let domain = |status: DomainStatus| DomainView {
            domain: domain_name("demo"),
            pace: DomainPace::Unpaced,
            status,
        };
        assert_eq!(domain(DomainStatus::Running).state_command(), Some("STOP;"));
        assert_eq!(
            domain(DomainStatus::Stopped).state_command(),
            Some("START;")
        );
        assert_eq!(domain(DomainStatus::Paused).state_command(), None);
        assert_eq!(
            domain(DomainStatus::Stopped).state_hint(false),
            "Waiting for connection"
        );
    }

    #[test]
    fn domain_listing_names_the_pace_and_status_of_each_domain() {
        let unpaced = DomainView {
            domain: domain_name("demo"),
            pace: DomainPace::Unpaced,
            status: DomainStatus::Stopped,
        };
        let paced = DomainView {
            domain: domain_name("demo_paced"),
            pace: DomainPace::Paced {
                period: DomainClockPeriod::try_from(Duration::from_secs(30))
                    .assured("thirty seconds is a valid domain clock period"),
                skew: DomainClockSkew::try_from(Duration::from_secs(1))
                    .assured("one second is a valid domain clock skew"),
            },
            status: DomainStatus::Running,
        };

        let lines = domain_list_lines(&[unpaced, paced])
            .into_iter()
            .map(|line| line.text)
            .collect::<Vec<_>>();

        assert_eq!(
            lines,
            vec![
                "domains:",
                "demo pace=UNPACED status=STOPPED",
                "demo_paced pace=PACED status=RUNNING",
            ]
        );
    }

    #[test]
    fn sidebar_entities_show_the_model_kind_or_the_latest_completed_resource_version() {
        let model = |kind: ModelKind, name: &str| {
            let name = ModelName::parse(name).assured("the test names a valid model");
            DomainEntity::Model(NodeRef::new(kind, name))
        };
        let resource = |name: &str, latest_version: Option<u64>| DomainEntity::Resource {
            name: ResourceName::parse(name).assured("the test names a valid resource"),
            latest_version: latest_version.and_then(NonZeroU64::new),
        };
        let snapshot = DomainSnapshotView::new(
            &[
                model(ModelKind::WireJsonSchema, "orders_json"),
                resource("bundle", Some(2)),
                model(ModelKind::WireAvroSchema, "orders_avro"),
                resource("catalog_only", None),
                model(ModelKind::Endpoint, "ingress"),
            ],
            DataflowGraph::new("demo"),
        );

        let entities = snapshot
            .entities
            .iter()
            .map(|entity| format!("{} {}", entity.name, entity.detail))
            .collect::<Vec<_>>();
        assert_eq!(
            entities,
            vec![
                "ingress ENDPOINT",
                "bundle v2",
                "catalog_only catalog",
                "orders_avro WIRE AVRO SCHEMA",
                "orders_json WIRE JSON SCHEMA",
            ]
        );
        let describe = snapshot
            .entities
            .iter()
            .map(EntityView::describe_command)
            .collect::<Vec<_>>();
        assert_eq!(
            describe,
            vec![
                Some("DESCRIBE ENDPOINT ingress;".to_string()),
                Some("DESCRIBE RESOURCE bundle;".to_string()),
                Some("DESCRIBE RESOURCE catalog_only;".to_string()),
                None,
                None,
            ]
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

    fn request_id(id: u64) -> RequestId {
        RequestId::new(NonZeroU64::new(id).assured("the test names a non-zero request identity"))
    }

    fn reply(request_id: RequestId, body: ReplyBody) -> ServerMessage {
        ServerMessage::Reply(Reply { request_id, body })
    }

    fn attached() -> ReplyBody {
        let status = TransactionStatus::new(
            "transaction".to_string(),
            domain_name("demo"),
            TransactionLifecycle::Open,
            TransactionPosition::new(2),
            0,
        )
        .assured("no operation of the test transaction has applied");
        ReplyBody::Attach(AttachOutcome {
            disposition: AttachDisposition::Attached(status),
            message: String::new(),
            diagnostics: Vec::new(),
        })
    }

    fn detached_command() -> ReplyBody {
        let mut outcome = completed_outcome("");
        outcome.disposition = CommandDisposition::TransactionDetached {
            transaction_id: "transaction".to_string(),
        };
        ReplyBody::Command(Box::new(outcome))
    }

    fn domain_name(name: &str) -> DomainName {
        DomainName::parse(name).assured("the test names a valid domain")
    }

    fn repl_command(query: &str) -> ConsoleRequest {
        ConsoleRequest::Command {
            request: CommandRequest {
                query: query.to_string(),
                domain: Some(domain_name("demo")),
                execution_reference: command_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::Repl,
        }
    }

    fn suggest(input: &str) -> ConsoleRequest {
        let request = SuggestRequest::new(input.to_string(), input.len(), None)
            .assured("the end of the input is a character boundary");
        ConsoleRequest::Suggest(request)
    }

    fn completed_outcome(message: &str) -> CommandOutcome {
        CommandOutcome {
            execution_reference: command_execution_reference(),
            origin: OutcomeOrigin::Executed,
            disposition: CommandDisposition::Completed {
                already_existed: false,
            },
            message: message.to_string(),
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
        }
    }

    fn diagnostic(message: &str, start: u32, end: u32) -> Diagnostic {
        Diagnostic {
            message: message.to_string(),
            span: Some(SourceSpan::new(start, end).assured("the test span does not end first")),
        }
    }

    fn sent_commands(messages: &[ClientMessage]) -> Vec<&CommandRequest> {
        let mut commands = Vec::new();
        for message in messages {
            let ClientRequest::Command(command) = &message.request else {
                panic!("the test sends only commands");
            };
            commands.push(command);
        }
        commands
    }

    fn sent_queries(messages: &[ClientMessage]) -> Vec<&str> {
        sent_commands(messages)
            .into_iter()
            .map(|command| command.query.as_str())
            .collect()
    }

    fn execution_references(messages: &[ClientMessage]) -> Vec<&CommandExecutionReference> {
        sent_commands(messages)
            .into_iter()
            .map(|command| &command.execution_reference)
            .collect()
    }

    fn request_numbers(messages: &[ClientMessage]) -> Vec<u64> {
        messages
            .iter()
            .map(|message| message.request_id.get().get())
            .collect()
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
            id: GraphEdgeId {
                source: source.to_string(),
                target: target.to_string(),
                kind: DataflowEdgeKind::Data,
            },
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

    fn edge_map(
        edges: impl IntoIterator<Item = GraphViewEdge>,
    ) -> BTreeMap<GraphEdgeId, GraphViewEdge> {
        edges
            .into_iter()
            .map(|edge| (edge.id.clone(), edge))
            .collect()
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
