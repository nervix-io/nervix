use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque, btree_map::Entry},
    num::NonZeroU64,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use error_stack::Report;
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
    AttachDisposition, AttachDomainClockRequest, AttachOutcome, AttachTransactionRequest,
    CancellationStage, ChoiceLookupRequest, ClientMessage, ClientRequest, ClusterObserved,
    CommandDisposition, CommandOutcome, CommandRequest, DetachDomainClockRequest, Diagnostic,
    DomainClockAttachDisposition, DomainClockAttachOutcome, DomainClockDetachDisposition,
    DomainClockDetachOutcome, DomainEntity, DomainInfo, DomainSelection, DomainSnapshotObserved,
    InspectTransactionRequest, InspectionOutcome, LeaderRedirect, Leadership,
    MAX_IN_FLIGHT_REQUESTS, NoticeLevel, ReplyBody, RequestCancelled, RequestId, RowBatchView,
    RowSchema, SelectDomainRequest, ServerEvent, ServerFrame, ServerMessage, ServerNotice,
    SessionEndReason, SessionLimits, StatementDisposition, StatementOutcome, SubscribeDisposition,
    SubscribeOutcome, SubscribeRequest, SubscriptionEnded, SubscriptionHandle, SubscriptionOpened,
    SubscriptionRows, SubscriptionType, SuggestRequest, Suggestion as WireSuggestion,
    SuggestionKind, SuggestionStatus, TextEdit, TransferAssembly, TransferPart,
    UnsubscribeDisposition, UnsubscribeOutcome, UnsubscribeRequest, VerifiedFrame,
    websocket::{ClientWebSocketCodec, WebSocketData},
};
use nervix_dataflow_graph::{
    DataflowBranch, DataflowEdgeKind, DataflowGraph, DataflowInputSide, DataflowNodeKind,
    DataflowNodeRole, DataflowNodeStatus, DataflowProcessorKind, DataflowSchemaField,
    DataflowStatistics,
};
use nervix_models::{
    CommandExecutionReference, CreateSubscription, DomainName, DomainPace, DomainStatus, ModelKind,
    RelayName, ResourceDescription, ResourceEntryContent, ResourceManifestEntry, ResourceUsage,
    ResourceVersionDescription, ResourceVersionEntries, Statement, SubscriptionName, Timestamp,
    TransactionLifecycle, TransactionStatus, expression_to_nspl,
};
use nervix_nspl::client_statement::{
    ClientStatement, parse_client_statement, parse_client_statements, parse_use_domain,
};
use nervix_recovery::Discarded as _;
use nervix_web_console::graph::{
    GraphEdgeId, GraphSearch, LiveGraphLayout, graph_layout_edge, graph_layout_item,
    layout::{EdgeTravel, GroupRegion, Rect},
    viewport::{Extent, GraphBounds, Viewport},
};
use thiserror::Error;
use url::Url;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

mod clock_display;
mod create_dialog;
mod request_handoff;
mod transaction_inspector;

use clock_display::{ClockDisplay, ClockPanel, ClockSelection, ClockSelectionChange, ClockStatus};
use create_dialog::{
    ChoiceControl, ChoiceRequestContext, CommandDispatch, CreateCommandContext, CreateDialog,
    CreateDispatch, CreateKind, CreateMenu, CreateSignals, CreateSubmission, SubscriptionDispatch,
};
use request_handoff::{RequestReceiver, RequestSender, request_handoff};
use transaction_inspector::{InspectorSignals, TransactionInspector};

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

/// What a request shows when the console has no session to hand it to.
const SESSION_UNAVAILABLE: &str = "websocket session is not available";

/// The most requests of the console's controls its session keeps outstanding: held until the
/// session can serve them, or sent and awaiting their reply.
const MAX_OUTSTANDING_REQUESTS: usize = 256;
/// The most text those requests carry together.
const MAX_OUTSTANDING_REQUEST_BYTES: usize = 4 * request_handoff::MAX_WAITING_REQUEST_BYTES;
// Everything the hand-off holds fits a session that has nothing else outstanding.
const _: () = assert!(request_handoff::MAX_WAITING_REQUESTS <= MAX_OUTSTANDING_REQUESTS);
const _: () = assert!(request_handoff::MAX_WAITING_REQUEST_BYTES <= MAX_OUTSTANDING_REQUEST_BYTES);

/// Why the console did not send a request one of its controls issued. The control shows the
/// reason where the request's outcome would have been shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
enum RequestRefusal {
    #[error(
        "{limit} requests are already waiting for the console's session; this one was not sent"
    )]
    TooManyWaiting { limit: usize },
    #[error(
        "the requests waiting for the console's session would carry more than {limit} bytes of \
         text; this one was not sent"
    )]
    TooMuchWaitingText { limit: usize },
    #[error(
        "{limit} requests are already outstanding in the console's session; this one was not sent"
    )]
    TooManyOutstanding { limit: usize },
    #[error(
        "the requests outstanding in the console's session would carry more than {limit} bytes of \
         text; this one was not sent"
    )]
    TooMuchOutstandingText { limit: usize },
    /// The session loop stopped taking requests.
    #[error("websocket command channel is closed")]
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsoleConnectionState {
    Connecting,
    Connected,
    Waiting,
}

#[derive(Clone, Copy)]
struct WebConsoleSession {
    state: RwSignal<ConsoleConnectionState>,
    request_tx: RwSignal<Option<RequestSender>>,
    upload_base_url: RwSignal<Option<String>>,
    auth_token: RwSignal<Option<String>>,
}

impl WebConsoleSession {
    /// Sends on the active connection. A disconnected session returns `Ok(false)`; a full
    /// hand-off returns its refusal so the control can explain why the request was not sent.
    fn send_when_connected(&self, request: ConsoleRequest) -> Result<bool, Report<RequestRefusal>> {
        if self.state.get_untracked() != ConsoleConnectionState::Connected {
            return Ok(false);
        }
        let Some(request_tx) = self.request_tx.get_untracked() else {
            return Ok(false);
        };
        request_tx.send(request)?;
        Ok(true)
    }
}

#[derive(Clone, Copy)]
struct WebConsoleSignals {
    terminal_lines: RwSignal<TermLineHistory>,
    suggestions: RwSignal<Vec<WireSuggestion>>,
    suggestion_status: RwSignal<Option<SuggestionStatus>>,
    suggestion_query: RwSignal<Option<SuggestionQuery>>,
    suggestion_continuation: RwSignal<Option<String>>,
    /// The latest snapshot of the domain the session observes. The session observes only its
    /// active domain, so the console keeps one snapshot; another domain's snapshot is sent again at
    /// once when that domain is selected.
    domain_snapshot: RwSignal<Option<DomainSnapshotView>>,
    cluster_counters: RwSignal<ClusterCounters>,
    active_domain: RwSignal<Option<DomainName>>,
    clock_display: RwSignal<ClockDisplay>,
    transaction_status: RwSignal<Option<TransactionStatus>>,
    inspector: InspectorSignals,
    domains: RwSignal<Vec<DomainView>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    subscription_tabs: RwSignal<Vec<SubscriptionTabView>>,
    active_subscription_tab: RwSignal<Option<u64>>,
    domains_loaded: RwSignal<bool>,
    auth_token: RwSignal<Option<String>>,
    auth_error: RwSignal<Option<String>>,
    session_generation: RwSignal<u64>,
    create: CreateSignals,
    selected_resource: RwSignal<Option<String>>,
    upload_status: RwSignal<String>,
}

impl WebConsoleSignals {
    /// Keep detach ahead of attach on the session's ordered request lane. The selected clock is
    /// marked before this call, so a refused request stays refused until selection or connection
    /// changes instead of being queued again by a later render.
    fn queue_clock_transition(
        self,
        change: ClockSelectionChange,
        connection_generation: u64,
        request_tx: &RequestSender,
    ) {
        if let Some(previous) = change.detach {
            let request = ConsoleRequest::DomainClockDetach {
                request: DetachDomainClockRequest {
                    domain: previous.clone(),
                },
                connection_generation,
                origin: ClockRequestOrigin::Automatic,
            };
            if let Err(error) = request_tx.send(request) {
                self.terminal_lines.update(|lines| {
                    lines.push(TermLine::error(format!(
                        "domain clock [{previous}]: detach could not be queued: {}",
                        error.current_context()
                    )));
                });
            }
        }
        if let Some(selected) = change.attach {
            let request = ConsoleRequest::DomainClockAttach {
                request: AttachDomainClockRequest {
                    domain: selected.clone(),
                },
                connection_generation,
                origin: ClockRequestOrigin::Automatic,
            };
            if let Err(error) = request_tx.send(request) {
                self.clock_display.update(|display| {
                    display.refuse(
                        &selected,
                        format!("attach could not be queued: {}", error.current_context()),
                    );
                });
                self.terminal_lines.update(|lines| {
                    lines.push(TermLine::error(format!(
                        "domain clock [{selected}]: attach could not be queued: {}",
                        error.current_context()
                    )));
                });
            }
        }
    }

    /// A replacement credential starts a separate view of the cluster. No tab, suggestion, or
    /// observed graph from the previous identity may remain visible to the new session.
    fn clear_authenticated_view(self) {
        self.active_domain.set(None);
        self.clock_display.set(ClockDisplay::NoDomain);
        self.transaction_status.set(None);
        self.inspector.clear();
        self.domains.set(Vec::new());
        self.domain_snapshot.set(None);
        self.resource_details.set(BTreeMap::new());
        self.cluster_counters.set(ClusterCounters::default());
        self.domains_loaded.set(false);
        self.subscription_tabs.set(Vec::new());
        self.active_subscription_tab.set(None);
        self.suggestions.set(Vec::new());
        self.suggestion_status.set(None);
        self.suggestion_query.set(None);
        self.suggestion_continuation.set(None);
        self.terminal_lines.set(TermLineHistory::default());
        self.create.connection_lost();
        self.selected_resource.set(None);
        self.upload_status.set(String::new());
    }

    fn apply_clock_attach_outcome(
        self,
        requested: &DomainName,
        origin: ClockRequestOrigin,
        outcome: DomainClockAttachOutcome,
    ) {
        let attached = matches!(
            &outcome.disposition,
            DomainClockAttachDisposition::Attached { domain, .. } if domain == requested
        );
        self.clock_display.update(|display| {
            display.attach_outcome(requested, &outcome, origin == ClockRequestOrigin::Automatic);
        });
        let line = if attached {
            TermLine::info(format!(
                "domain clock [{requested}] attached: {}",
                outcome.message
            ))
        } else {
            TermLine::error(format!(
                "domain clock [{requested}] attach refused: {}",
                outcome.message
            ))
        };
        self.terminal_lines.update(|lines| lines.push(line));
    }

    fn apply_clock_detach_outcome(
        self,
        requested: &DomainName,
        origin: ClockRequestOrigin,
        outcome: DomainClockDetachOutcome,
    ) {
        let detached = matches!(
            &outcome.disposition,
            DomainClockDetachDisposition::Detached(domain) if domain == requested
        );
        let already_detached = matches!(
            &outcome.disposition,
            DomainClockDetachDisposition::NotAttached(domain) if domain == requested
        );
        if detached && origin == ClockRequestOrigin::Repl {
            self.clock_display
                .update(|display| display.detached(requested));
        }
        let line = if detached {
            TermLine::info(format!(
                "domain clock [{requested}] detached: {}",
                outcome.message
            ))
        } else if already_detached && origin == ClockRequestOrigin::Automatic {
            TermLine::info(format!("domain clock [{requested}] was already detached"))
        } else {
            TermLine::error(format!(
                "domain clock [{requested}] detach refused: {}",
                outcome.message
            ))
        };
        self.terminal_lines.update(|lines| lines.push(line));
    }

    /// Closing a pending start waits for its reply before deleting that subscription. An opened
    /// stream stays visible as closing until its unsubscribe reply names the same generation.
    ///
    /// An interrupted or ended tab holds no subscription that still delivers, so it closes at once.
    /// The server keeps the name of a generation it ended until the name is reused or the session
    /// ends, and nothing of that generation remains to release.
    fn begin_subscription_close(self, tab_id: u64) -> Option<UnsubscribeRequest> {
        let tab = self
            .subscription_tabs
            .get_untracked()
            .into_iter()
            .find(|tab| tab.id == tab_id)?;
        let stream = match tab.state {
            SubscriptionTabState::Pending
            | SubscriptionTabState::Restoring
            | SubscriptionTabState::Resubscribing => {
                self.subscription_tabs.update(|tabs| {
                    if let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) {
                        tab.state = SubscriptionTabState::Closing(None);
                    }
                });
                return None;
            }
            SubscriptionTabState::Interrupted | SubscriptionTabState::Ended => {
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

    /// Marks every interrupted tab as restoring and returns the requests that open their
    /// subscriptions again, each under its name as a new generation of the current session.
    fn begin_restorations(self) -> Vec<ConsoleRequest> {
        let mut restorations = Vec::new();
        self.subscription_tabs.update(|tabs| {
            // Bounded by the subscription tabs the operator has open in this console.
            for tab in tabs.iter_mut() {
                if let SubscriptionTabState::Interrupted = &tab.state {
                    tab.state = SubscriptionTabState::Restoring;
                    restorations.push(ConsoleRequest::SubscriptionStart {
                        tab_id: tab.id,
                        request: SubscribeRequest {
                            domain: tab.domain.clone(),
                            statement: tab.subscribe_command.clone(),
                            subscription_type: SubscriptionType::Row,
                        },
                        origin: SubscriptionOrigin::Restoration,
                    });
                }
            }
        });
        restorations
    }

    /// Opens an ended tab's subscription again, under its name and with the statement that first
    /// opened it. The server takes the reused name as a new generation, whose opening reply
    /// announces the relay's current schema.
    fn begin_resubscribe(self, tab_id: u64) -> Option<SubscribeRequest> {
        let mut request = None;
        self.subscription_tabs.update(|tabs| {
            // Bounded by the subscription tabs the operator has open in this console.
            let Some(tab) = tabs.iter_mut().find(|tab| tab.id == tab_id) else {
                return;
            };
            if !tab.state.can_resubscribe() {
                return;
            }
            tab.state = SubscriptionTabState::Resubscribing;
            request = Some(SubscribeRequest {
                domain: tab.domain.clone(),
                statement: tab.subscribe_command.clone(),
                subscription_type: SubscriptionType::Row,
            });
        });
        request
    }

    /// Ends the tab that shows a generation the server ended. The tab keeps its rows and shows
    /// why, and it is not restored on a later connection, which would not change why the server
    /// ended it. A tab the operator is closing is removed at once, because nothing is left to
    /// show.
    fn end_subscription(self, ended: &SubscriptionEnded) {
        let mut closed = None;
        self.subscription_tabs.update(|tabs| {
            // Bounded by the subscription tabs the operator has open in this console.
            for tab in tabs.iter_mut() {
                match &tab.state {
                    SubscriptionTabState::Open(stream)
                        if stream.subscription == ended.subscription =>
                    {
                        tab.state = SubscriptionTabState::Ended;
                        tab.lines.push(TermLine::error(ended.message.clone()));
                    }
                    SubscriptionTabState::Closing(Some(stream))
                        if stream.subscription == ended.subscription =>
                    {
                        closed = Some(tab.id);
                    }
                    SubscriptionTabState::Pending
                    | SubscriptionTabState::Open(_)
                    | SubscriptionTabState::Interrupted
                    | SubscriptionTabState::Restoring
                    | SubscriptionTabState::Ended
                    | SubscriptionTabState::Resubscribing
                    | SubscriptionTabState::Closing(_) => {}
                }
            }
        });
        if let Some(tab_id) = closed {
            remove_subscription_tab(self.subscription_tabs, self.active_subscription_tab, tab_id);
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SuggestionQuery {
    input: String,
    cursor: usize,
    domain: Option<DomainName>,
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
        origin: SubscriptionOrigin,
    },
    /// Closes the subscription of a tab the operator closed.
    SubscriptionStop {
        tab_id: u64,
        request: UnsubscribeRequest,
    },
    /// Selects the domain whose observations the session receives.
    SelectDomain(SelectDomainRequest),
    /// Follows the selected domain clock on the current session.
    DomainClockAttach {
        request: AttachDomainClockRequest,
        connection_generation: u64,
        origin: ClockRequestOrigin,
    },
    /// Releases a clock when its domain is deselected or the operator requests it.
    DomainClockDetach {
        request: DetachDomainClockRequest,
        connection_generation: u64,
        origin: ClockRequestOrigin,
    },
    /// Asks for completions of the REPL input.
    Suggest(SuggestRequest),
    /// Resolves a structured form control into typed choices.
    Choice {
        request: ChoiceLookupRequest,
        context: ChoiceRequestContext,
    },
    /// Binds the session's transaction to the connection.
    AttachTransaction(AttachTransactionRequest),
    /// Reads an impact report without binding the inspected transaction.
    InspectTransaction(InspectTransactionRequest),
}

/// Who asked for a subscription, and so who reads its outcome besides its tab.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SubscriptionOrigin {
    /// Typed in the REPL, or resubscribed from an ended tab: the tab and the terminal show the
    /// outcome.
    Console,
    /// The restoration of an interrupted tab on a new connection: the tab and the terminal show
    /// the outcome. It belongs to that connection, which opens it before it attaches the
    /// session's transaction again; a later connection restores the tab itself.
    Restoration,
    /// Submitted by the Create form, whose attempt completes or fails with the outcome.
    Create { attempt: u64, draft_revision: u64 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClockRequestOrigin {
    Automatic,
    Repl,
}

/// Who reads the outcome of a command.
#[derive(Clone)]
enum CommandPurpose {
    /// The REPL prints it.
    Repl,
    /// A popup form reads the terminal outcome and its own submission state.
    Create(CreateCommandContext),
    /// The resource dialog reads the versions of `resource` from its typed `DESCRIBE RESOURCE`
    /// description.
    ResourceDescription { resource: String },
}

impl ConsoleRequest {
    fn inspects_transaction(&self) -> bool {
        match self {
            Self::InspectTransaction(_) => true,
            Self::Command { request, .. } => {
                matches!(parse_client_statement(&request.query), Ok(statement) if statement.inspects_transaction())
            }
            Self::ListDomains
            | Self::SubscriptionStart { .. }
            | Self::SubscriptionStop { .. }
            | Self::SelectDomain(_)
            | Self::DomainClockAttach { .. }
            | Self::DomainClockDetach { .. }
            | Self::Suggest(_)
            | Self::Choice { .. }
            | Self::AttachTransaction(_) => false,
        }
    }

    /// Whether the request keeps its place in the order the console issued requests: it waits
    /// until the session can serve it and outlives a connection that ended before answering it.
    ///
    /// Every connection selects the active domain and attaches the session's transaction again
    /// itself. Completion and structured-choice requests only matter for the current input or
    /// draft, so those are sent at once and forgotten with the connection.
    fn is_ordered(&self) -> bool {
        match self {
            Self::Command { .. }
            | Self::ListDomains
            | Self::SubscriptionStart { .. }
            | Self::SubscriptionStop { .. }
            | Self::DomainClockAttach { .. }
            | Self::DomainClockDetach { .. } => true,
            Self::InspectTransaction(_) => true,
            Self::SelectDomain(_)
            | Self::Suggest(_)
            | Self::Choice { .. }
            | Self::AttachTransaction(_) => false,
        }
    }

    /// Whether the request belongs to the connection it was sent on: a session-local deletion,
    /// clock attachment, or restoration. The next connection issues its own.
    fn belongs_to_connection(&self) -> bool {
        match self {
            Self::SubscriptionStop { .. }
            | Self::DomainClockAttach { .. }
            | Self::DomainClockDetach { .. }
            | Self::SubscriptionStart {
                origin: SubscriptionOrigin::Restoration,
                ..
            } => true,
            Self::SubscriptionStart {
                origin: SubscriptionOrigin::Console | SubscriptionOrigin::Create { .. },
                ..
            }
            | Self::Command { .. }
            | Self::ListDomains
            | Self::SelectDomain(_)
            | Self::Suggest(_)
            | Self::Choice { .. }
            | Self::AttachTransaction(_)
            | Self::InspectTransaction(_) => false,
        }
    }

    /// The bytes of free text the request carries: a command's source, a subscription's
    /// statement, the input a completion is asked for, or the search of a choice. Names and
    /// identities are short by their own validation, so the bounds on outstanding requests count
    /// only text an operator typed or pasted.
    fn text_bytes(&self) -> usize {
        match self {
            Self::Command { request, .. } => request.query.len(),
            Self::SubscriptionStart { request, .. } => request.statement.len(),
            Self::Suggest(request) => request.input().len(),
            Self::Choice { request, .. } => request.search().len(),
            Self::ListDomains
            | Self::SubscriptionStop { .. }
            | Self::DomainClockAttach { .. }
            | Self::DomainClockDetach { .. }
            | Self::SelectDomain(_)
            | Self::AttachTransaction(_)
            | Self::InspectTransaction(_) => 0,
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
            Self::DomainClockAttach { request, .. } => {
                ClientRequest::AttachDomainClock(request.clone())
            }
            Self::DomainClockDetach { request, .. } => {
                ClientRequest::DetachDomainClock(request.clone())
            }
            Self::Suggest(request) => ClientRequest::Suggest(request.clone()),
            Self::Choice { request, .. } => ClientRequest::Choice(request.clone()),
            Self::AttachTransaction(request) => ClientRequest::AttachTransaction(request.clone()),
            Self::InspectTransaction(request) => ClientRequest::InspectTransaction(request.clone()),
        }
    }

    fn belongs_to_connection_generation(&self, connection_generation: u64) -> bool {
        match self {
            Self::DomainClockAttach {
                connection_generation: issued,
                ..
            }
            | Self::DomainClockDetach {
                connection_generation: issued,
                ..
            } => *issued == connection_generation,
            _ => true,
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

/// What became of a request one of the console's controls issued.
enum Admission {
    /// It was sent at once, in this message.
    Sent(ClientMessage),
    /// It waits until the session can serve it.
    Held,
    /// It was never sent, because the session already keeps as many requests outstanding as it
    /// holds.
    Refused(Box<RefusedRequest>),
}

/// A request the session refused, and why.
struct RefusedRequest {
    request: ConsoleRequest,
    refusal: RequestRefusal,
}

/// The requests the session keeps outstanding, and the text they carry.
#[derive(Default)]
struct OutstandingLoad {
    requests: usize,
    bytes: usize,
}

impl OutstandingLoad {
    fn add(&mut self, request: &ConsoleRequest) {
        self.requests = self.requests.checked_add(1).assured(
            "the outstanding requests are held in memory, so they cannot number usize::MAX",
        );
        self.bytes = self.bytes.checked_add(request.text_bytes()).assured(
            "every counted text is held in memory at once, so the texts cannot add up past \
             usize::MAX",
        );
    }

    /// Whether one more request carrying `bytes` of text stays within the bounds, or which bound
    /// it would cross.
    fn admits(&self, bytes: usize) -> Result<(), Report<RequestRefusal>> {
        if self.requests >= MAX_OUTSTANDING_REQUESTS {
            return Err(Report::new(RequestRefusal::TooManyOutstanding {
                limit: MAX_OUTSTANDING_REQUESTS,
            }));
        }
        match self.bytes.checked_add(bytes) {
            Some(total) if total <= MAX_OUTSTANDING_REQUEST_BYTES => Ok(()),
            // A sum that overflows is past the bound as well.
            Some(_) | None => Err(Report::new(RequestRefusal::TooMuchOutstandingText {
                limit: MAX_OUTSTANDING_REQUEST_BYTES,
            })),
        }
    }
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
    /// The latest request for each structured control. Older replies cannot overwrite a newer
    /// draft or search.
    latest_choices: BTreeMap<ChoiceControl, RequestId>,
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
            latest_choices: BTreeMap::new(),
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

    /// Takes a request one of the console's controls issued. It is refused when the session
    /// already keeps as many requests outstanding, or as much of their text, as it holds. An
    /// ordered request then waits until it can go out in its place; anything else is sent at once.
    fn accept(&mut self, issued: IssuedRequest) -> Admission {
        if let Err(report) = self.outstanding().admits(issued.request.text_bytes()) {
            return Admission::Refused(Box::new(RefusedRequest {
                request: issued.request,
                refusal: *report.current_context(),
            }));
        }
        if issued.request.is_ordered() && !self.can_send_ordered() {
            self.held.insert(issued.order, issued.request);
            return Admission::Held;
        }
        Admission::Sent(self.dispatch(issued))
    }

    /// Whether an ordered request can go out at once: the session is ready, nothing issued before
    /// it still waits, and the server admits another request in flight. Beyond
    /// `MAX_IN_FLIGHT_REQUESTS` the server would refuse the request rather than queue it, so the
    /// console holds it until an earlier reply frees a place.
    fn can_send_ordered(&self) -> bool {
        self.is_ready() && self.held.is_empty() && self.has_room_in_flight()
    }

    /// Whether the server admits another request in flight on this connection. The console
    /// counts a request until its reply arrives, and the server stops counting it once the reply
    /// is queued, so the console never counts fewer than the server does.
    fn has_room_in_flight(&self) -> bool {
        self.in_flight.len() < MAX_IN_FLIGHT_REQUESTS
    }

    /// The requests the session keeps outstanding: held, or sent and awaiting their reply.
    fn outstanding(&self) -> OutstandingLoad {
        let mut load = OutstandingLoad::default();
        // Bounded by `MAX_OUTSTANDING_REQUESTS`, which `accept` enforces, together with the
        // requests the session issues itself: the requests that open a connection, one
        // restoration or deletion per subscription tab, and one description per completed
        // resource creation.
        for request in self.held.values() {
            load.add(request);
        }
        for issued in self.in_flight.values() {
            load.add(&issued.request);
        }
        load
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
                    self.transfers.remove(&previous);
                }
            }
            ConsoleRequest::Choice { context, .. } => {
                if let Some(previous) = self.latest_choices.insert(context.control, request_id) {
                    self.in_flight.remove(&previous);
                    self.transfers.remove(&previous);
                }
            }
            ConsoleRequest::AttachTransaction(_) => {
                self.attaching = Some(request_id);
            }
            ConsoleRequest::Command { .. }
            | ConsoleRequest::ListDomains
            | ConsoleRequest::InspectTransaction(_)
            | ConsoleRequest::SubscriptionStart { .. }
            | ConsoleRequest::SubscriptionStop { .. }
            | ConsoleRequest::SelectDomain(_)
            | ConsoleRequest::DomainClockAttach { .. }
            | ConsoleRequest::DomainClockDetach { .. } => {}
        }
        let message = ClientMessage {
            request_id,
            request: issued.request.client_request(),
        };
        self.in_flight.insert(request_id, issued);
        message
    }

    /// Dispatches the held requests in the order they were issued, once the session is ready and
    /// while the server admits them in flight. The rest keep waiting for earlier replies.
    fn release_held(&mut self) -> Vec<ClientMessage> {
        let mut messages = Vec::new();
        if !self.is_ready() {
            return messages;
        }
        while self.has_room_in_flight() {
            let Some((order, request)) = self.held.pop_first() else {
                break;
            };
            messages.push(self.dispatch(IssuedRequest { order, request }));
        }
        messages
    }

    /// Holds a request that has to be sent again, in its original place in the issue order.
    fn hold_again(&mut self, issued: IssuedRequest) {
        self.held.insert(issued.order, issued.request);
    }

    /// Drops every held request, because the session they were issued for is over.
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
        self.latest_choices
            .retain(|_, latest| *latest != request_id);
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
    /// so the server recovers its recorded outcome instead of executing it twice. A request that
    /// belongs to the ended connection ends with it.
    fn end_connection(&mut self) {
        let in_flight = std::mem::take(&mut self.in_flight);
        for issued in in_flight.into_values() {
            if issued.request.is_ordered() && !issued.request.belongs_to_connection() {
                self.hold_again(issued);
            }
        }
        self.held
            .retain(|_, request| !request.belongs_to_connection());
        self.transfers.clear();
        self.next_request_id = NonZeroU64::MIN;
        self.latest_suggestion = None;
        self.latest_choices.clear();
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

/// A subscription the console shows as a tab. Its name is unique among the console's tabs, which
/// are the session's subscriptions, so a name also finds the tab of a typed `DELETE SUBSCRIPTION`.
#[derive(Clone)]
struct SubscriptionTabView {
    id: u64,
    state: SubscriptionTabState,
    name: SubscriptionName,
    domain: DomainName,
    title: String,
    /// The canonical statement that opens the subscription, sent again to restore the tab.
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

/// Where a subscription tab stands.
#[derive(Clone)]
enum SubscriptionTabState {
    /// A new tab whose subscription has not opened yet. A refusal removes the tab.
    Pending,
    /// The subscription streams its rows into the tab.
    Open(TabStream),
    /// The connection that carried the subscription ended, or the tab's restoration was refused.
    /// The next connection restores the tab, and so does a retry every second while the session
    /// holds no transaction.
    Interrupted,
    /// The subscription of an interrupted tab is being opened again. A refusal leaves the tab
    /// interrupted, so it is tried again.
    Restoring,
    /// The server ended the subscription's generation, because its relay was redefined or
    /// removed. The tab keeps its rows and why it ended, and is never restored on its own.
    Ended,
    /// The operator resubscribed an ended tab. A refusal leaves the tab ended and shows why.
    Resubscribing,
    /// The operator closed the tab, which waits for its opening reply or for the deletion of its
    /// subscription.
    Closing(Option<TabStream>),
}

impl SubscriptionTabState {
    fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Open(_) => "active",
            Self::Interrupted => "interrupted",
            Self::Restoring => "restoring",
            Self::Ended => "ended",
            Self::Resubscribing => "resubscribing",
            Self::Closing(_) => "closing",
        }
    }

    fn can_activate(&self) -> bool {
        match self {
            Self::Open(_)
            | Self::Interrupted
            | Self::Restoring
            | Self::Ended
            | Self::Resubscribing
            | Self::Closing(Some(_)) => true,
            Self::Pending | Self::Closing(None) => false,
        }
    }

    /// Whether the operator can open the tab's subscription again from the tab.
    fn can_resubscribe(&self) -> bool {
        match self {
            Self::Ended => true,
            Self::Pending
            | Self::Open(_)
            | Self::Interrupted
            | Self::Restoring
            | Self::Resubscribing
            | Self::Closing(_) => false,
        }
    }
}

#[derive(Clone, Default)]
struct ResourceDetailView {
    versions: Vec<ResourceVersionView>,
    status: String,
}

/// Everything one version row of the resource dialog shows: the version as the typed description
/// reports it, and the models bound to it. A keyed list re-renders a row only when its key
/// changes, so the dialog keys each row by this whole value: a later description that changes the
/// row, such as the usages a rebinding moved, replaces it.
#[derive(Clone, PartialEq, Eq, Hash)]
struct ResourceVersionView {
    version: ResourceVersionDescription,
    usages: Vec<ResourceUsage>,
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

#[cfg(test)]
fn initialize_test_executor() {
    static EXECUTOR: std::sync::Once = std::sync::Once::new();
    EXECUTOR.call_once(|| {
        any_spawner::Executor::init_futures_executor()
            .assured("the test process initializes the Leptos executor once");
    });
}

#[component]
fn App() -> impl IntoView {
    let active_domain = RwSignal::new(None::<DomainName>);
    let clock_display = RwSignal::new(ClockDisplay::NoDomain);
    let clock_now = RwSignal::new(clock_display::browser_utc_now());
    let clock_interval = set_interval_with_handle(
        move || clock_now.set(clock_display::browser_utc_now()),
        clock_display::CLOCK_REFRESH,
    )
    .ok();
    on_cleanup(move || {
        if let Some(interval) = clock_interval {
            interval.clear();
        }
    });
    let domains = RwSignal::new(Vec::<DomainView>::new());
    let active_theme = RwSignal::new(0_usize);
    let input = RwSignal::new(String::new());
    let terminal_lines = RwSignal::new(TermLineHistory::default());
    let transaction_status = RwSignal::new(None::<TransactionStatus>);
    let inspector = InspectorSignals::new();
    let subscription_tabs = RwSignal::new(Vec::<SubscriptionTabView>::new());
    let active_subscription_tab = RwSignal::new(None::<u64>);
    let next_subscription_tab_id = RwSignal::new(1_u64);
    let suggestions = RwSignal::new(Vec::<WireSuggestion>::new());
    let suggestion_status = RwSignal::new(None::<SuggestionStatus>);
    let suggestion_query = RwSignal::new(None::<SuggestionQuery>);
    let suggestion_continuation = RwSignal::new(None::<String>);
    let domain_snapshot = RwSignal::new(None::<DomainSnapshotView>);
    let cluster_counters = RwSignal::new(ClusterCounters::default());
    let resource_details = RwSignal::new(BTreeMap::<String, ResourceDetailView>::new());
    let domains_loaded = RwSignal::new(false);
    let auth_token = RwSignal::new(web_console_auth_token_from_location());
    let auth_error = RwSignal::new(None::<String>);
    let session_generation = RwSignal::new(0_u64);
    let create = CreateSignals::new();
    let selected_resource = RwSignal::new(None::<String>);
    let upload_status = RwSignal::new(String::new());
    let signals = WebConsoleSignals {
        terminal_lines,
        suggestions,
        suggestion_status,
        suggestion_query,
        suggestion_continuation,
        domain_snapshot,
        cluster_counters,
        active_domain,
        clock_display,
        transaction_status,
        inspector,
        domains,
        resource_details,
        subscription_tabs,
        active_subscription_tab,
        domains_loaded,
        auth_token,
        auth_error,
        session_generation,
        create,
        selected_resource,
        upload_status,
    };
    let web_console_session = use_websocket_session(signals);

    let active_domain_name = move || match active_domain.get() {
        Some(domain) => domain.to_string(),
        None => String::new(),
    };
    let active_graph = move || {
        let active = active_domain.get()?;
        let graph = {
            let snapshot = domain_snapshot.read();
            let snapshot = snapshot.as_ref()?;
            if snapshot.domain != active {
                return None;
            }
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
        let snapshot = domain_snapshot.read();
        match snapshot.as_ref() {
            Some(snapshot) if snapshot.domain == active => snapshot.entities.clone(),
            Some(_) | None => Vec::new(),
        }
    };
    let active_domain_session = web_console_session;
    Effect::new(move |_| {
        let Some(domain) = active_domain.get() else {
            return;
        };
        let queued = ConsoleRequest::SelectDomain(SelectDomainRequest { domain });
        if let Some(request_tx) = active_domain_session.request_tx.get_untracked()
            && let Err(refusal) = request_tx.send(queued)
        {
            let reason = refusal.current_context().to_string();
            terminal_lines.update(|lines| lines.push(TermLine::error(reason)));
        }
    });
    let clock_session = web_console_session;
    let clock_selection = RwSignal::new(ClockSelection::default());
    Effect::new(move |_| {
        let domain = active_domain.get();
        let connected = clock_session.state.get() == ConsoleConnectionState::Connected;
        let generation = session_generation.get_untracked();
        let mut selection = clock_selection.get_untracked();
        let change = selection.change(connected, generation, domain.clone());
        clock_selection.set(selection);
        if !change.refresh_display {
            return;
        }
        clock_display.set(ClockDisplay::selected(domain, connected));
        if !connected {
            return;
        }
        let Some(request_tx) = clock_session.request_tx.get_untracked() else {
            if let Some(selected) = change.attach {
                clock_display.update(|display| {
                    display.refuse(&selected, "attach could not be queued".to_string());
                });
            }
            return;
        };
        signals.queue_clock_transition(change, generation, &request_tx);
    });
    let suggestion_request_sequence = RwSignal::new(0_u64);
    let suggestion_session = web_console_session;
    let request_suggestions = move |value: String, cursor: usize, continuation: Option<String>| {
        suggestion_request_sequence.update(|sequence| {
            *sequence = sequence
                .checked_add(1)
                .assured("a console session cannot request 2^64 suggestions");
        });
        let request_sequence = suggestion_request_sequence.get_untracked();
        if continuation.is_none() {
            suggestions.set(Vec::new());
            suggestion_status.set(None);
            suggestion_query.set(None);
            suggestion_continuation.set(None);
        }
        if !domains_loaded.get_untracked() {
            suggestions.set(Vec::new());
            return;
        }
        let domain = active_domain.get_untracked();
        suggestion_query.set(Some(SuggestionQuery {
            input: value.clone(),
            cursor,
            domain: domain.clone(),
        }));
        let auth_at_schedule = suggestion_session.auth_token.get_untracked();
        spawn_local(async move {
            wait_for_browser_delay(SUGGESTION_REQUEST_DEBOUNCE_DELAY).await;
            if suggestion_request_sequence.get_untracked() != request_sequence
                || suggestion_session.auth_token.get_untracked() != auth_at_schedule
                || (continuation.is_some()
                    && suggestion_continuation.get_untracked() != continuation)
            {
                return;
            }
            let request = SuggestRequest::new(value, cursor, domain)
                .assured("the browser cursor is converted to a UTF-8 character boundary");
            let request = request
                .with_page(64, continuation)
                .assured("the console page size is within the protocol bound");
            let queued = ConsoleRequest::Suggest(request);
            if let Some(request_tx) = suggestion_session.request_tx.get_untracked()
                && request_tx.send(queued).is_err()
            {
                suggestions.set(Vec::new());
                suggestion_status.set(Some(SuggestionStatus::LookupFailed));
            }
        });
    };

    let subscription_request_tx = web_console_session.request_tx;
    // The one dispatcher of subscription statements: a Create form submission and a statement
    // typed in the REPL both open a tab here, which the subscription lifecycle then owns.
    let open_subscription = move |dispatch: SubscriptionDispatch, origin: SubscriptionOrigin| {
        let SubscriptionDispatch {
            domain,
            subscription,
            statement,
        } = dispatch;
        // Bounded by the subscription tabs the operator has open in this console.
        let name_taken = subscription_tabs
            .with_untracked(|tabs| tabs.iter().any(|tab| tab.name == subscription.name));
        if name_taken {
            let reason = format!(
                "a subscription tab named '{}' is already open",
                subscription.name
            );
            terminal_lines.update(|lines| lines.push(TermLine::error(reason.clone())));
            fail_subscription_origin(signals, origin, reason);
            return;
        }
        let tab_id = next_subscription_tab_id.get_untracked();
        let next_tab_id = tab_id
            .checked_add(1)
            .assured("a console session cannot open 2^64 subscription tabs");
        next_subscription_tab_id.set(next_tab_id);
        subscription_tabs.update(|tabs| {
            tabs.push(SubscriptionTabView {
                id: tab_id,
                state: SubscriptionTabState::Pending,
                name: subscription.name.clone(),
                domain: domain.clone(),
                title: subscription_tab_title(&subscription),
                subscribe_command: statement.clone(),
                lines: TermLineHistory::default(),
            });
        });
        let request = SubscribeRequest {
            domain,
            statement,
            subscription_type: SubscriptionType::Row,
        };
        send_subscription_start(signals, subscription_request_tx, tab_id, request, origin);
    };
    let tab_request_tx = web_console_session.request_tx;
    let stop_subscription =
        move |tab_id: u64| close_subscription_tab(signals, tab_request_tx, tab_id);
    let resubscribe = move |tab_id: u64| resubscribe_tab(signals, tab_request_tx, tab_id);

    let run_command = move |next_command: Option<String>| {
        suggestion_request_sequence.update(|sequence| {
            *sequence = sequence
                .checked_add(1)
                .assured("a console session cannot request 2^64 suggestions");
        });
        suggestion_query.set(None);
        suggestion_status.set(None);
        suggestion_continuation.set(None);
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
                && let Err(refusal) = request_tx.send(ConsoleRequest::ListDomains)
            {
                let reason = refusal.current_context().to_string();
                terminal_lines.update(|lines| lines.push(TermLine::error(reason)));
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
        } else if let Ok(ClientStatement::AttachDomainClock) = parse_client_statement(&command) {
            if transaction_active {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            if let Some(domain) = active_domain.get_untracked() {
                let request = ConsoleRequest::DomainClockAttach {
                    request: AttachDomainClockRequest { domain },
                    connection_generation: session_generation.get_untracked(),
                    origin: ClockRequestOrigin::Repl,
                };
                match web_console_session.send_when_connected(request) {
                    Ok(true) => {}
                    Ok(false) => terminal_lines.update(|lines| {
                        lines.push(TermLine::error("websocket session is not connected"));
                    }),
                    Err(reason) => terminal_lines.update(|lines| {
                        lines.push(TermLine::error(reason.current_context().to_string()));
                    }),
                }
            } else {
                terminal_lines
                    .update(|lines| lines.push(TermLine::error("no active domain selected")));
            }
        } else if let Ok(ClientStatement::DetachDomainClock) = parse_client_statement(&command) {
            if transaction_active {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "client-local commands are not allowed while a transaction is active",
                    ));
                });
                return;
            }
            if let Some(domain) = active_domain.get_untracked() {
                let request = ConsoleRequest::DomainClockDetach {
                    request: DetachDomainClockRequest { domain },
                    connection_generation: session_generation.get_untracked(),
                    origin: ClockRequestOrigin::Repl,
                };
                match web_console_session.send_when_connected(request) {
                    Ok(true) => {}
                    Ok(false) => terminal_lines.update(|lines| {
                        lines.push(TermLine::error("websocket session is not connected"));
                    }),
                    Err(reason) => terminal_lines.update(|lines| {
                        lines.push(TermLine::error(reason.current_context().to_string()));
                    }),
                }
            } else {
                terminal_lines
                    .update(|lines| lines.push(TermLine::error("no active domain selected")));
            }
        } else if let Ok(ClientStatement::CreateSubscription(subscription)) =
            parse_client_statement(&command)
        {
            match active_domain.get_untracked() {
                Some(domain) => match SubscriptionDispatch::new(domain, subscription) {
                    Ok(dispatch) => open_subscription(dispatch, SubscriptionOrigin::Console),
                    Err(error) => {
                        let reason = error.current_context().to_string();
                        terminal_lines.update(|lines| lines.push(TermLine::error(reason)));
                    }
                },
                None => {
                    terminal_lines
                        .update(|lines| lines.push(TermLine::error("no active domain selected")));
                }
            }
        } else if let Ok(
            ClientStatement::DescribeBackup(_) | ClientStatement::Server(Statement::Backup(_)),
        ) = parse_client_statement(&command)
        {
            terminal_lines.update(|lines| {
                lines.push(TermLine::error(
                    "BACKUP and DESCRIBE BACKUP write and read archive files on the client's \
                     machine; run them with nervix-cli",
                ));
            });
        } else if let Ok(ClientStatement::DeleteSubscription(delete)) =
            parse_client_statement(&command)
        {
            // Bounded by the subscription tabs the operator has open in this console.
            let tab_id = subscription_tabs.with_untracked(|tabs| {
                tabs.iter()
                    .find(|tab| tab.name == delete.name)
                    .map(|tab| tab.id)
            });
            match tab_id {
                Some(tab_id) => stop_subscription(tab_id),
                None => {
                    let reason = format!("no subscription tab named '{}'", delete.name);
                    terminal_lines.update(|lines| lines.push(TermLine::error(reason)));
                }
            }
        } else {
            // The server decides which statements need a selected domain, and answers one sent
            // without it with a failed outcome.
            let request_domain = active_domain.get_untracked();
            let transaction = transaction_status.get_untracked();
            let parsed = parse_client_statement(&command).ok();
            if let Some(ClientStatement::Server(Statement::DescribeTransaction(describe))) = &parsed
            {
                inspector.prepare_describe(describe.request.target.clone());
            }
            let is_commit = matches!(parsed, Some(ClientStatement::CommitTransaction));
            let preview = if is_commit {
                transaction
                    .as_ref()
                    .and_then(|status| inspector.commit_basis(status))
            } else {
                None
            };
            if is_commit && preview.is_none() {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(
                        "Inspect the attached transaction at its current position before COMMIT",
                    ))
                });
                return;
            }
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
                expected_preview: preview,
            };
            let queued = ConsoleRequest::Command {
                request,
                purpose: CommandPurpose::Repl,
            };
            if let Some(request_tx) = web_console_session.request_tx.get_untracked() {
                if let Err(refusal) = request_tx.send(queued) {
                    let reason = refusal.current_context().to_string();
                    terminal_lines.update(|lines| lines.push(TermLine::error(reason)));
                } else if web_console_session.state.get_untracked()
                    != ConsoleConnectionState::Connected
                {
                    terminal_lines.update(|lines| {
                        lines.push(TermLine::info("queued until websocket reconnects"));
                    });
                }
            } else {
                terminal_lines.update(|lines| {
                    lines.push(TermLine::error(SESSION_UNAVAILABLE));
                });
            }
        }
        suggestions.set(Vec::new());
        input.set(String::new());
    };
    let create_request_tx = web_console_session.request_tx;
    let submit_create = move |submission: CreateSubmission, attempt: u64, draft_revision: u64| {
        let CreateSubmission {
            kind,
            presentation,
            dispatch,
        } = submission;
        match dispatch {
            CreateDispatch::Command(command) => submit_create_command(
                signals,
                create_request_tx,
                kind,
                presentation,
                command,
                attempt,
                draft_revision,
            ),
            CreateDispatch::Subscription(subscription) => {
                let prompt_transaction = transaction_status
                    .with_untracked(|status| ActiveTransaction::of(status.as_ref()));
                terminal_lines.update(|lines| {
                    lines.push(TermLine::prompt(presentation, prompt_transaction));
                });
                let origin = SubscriptionOrigin::Create {
                    attempt,
                    draft_revision,
                };
                open_subscription(subscription, origin);
            }
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
                    clock_display=clock_display
                    domains=domains
                    run_command=run_command
                    transaction_status=transaction_status
                    inspector=inspector
                    create=create
                />
                <div class="console-body">
                    <Sidebar active_domain=active_domain clock_display=clock_display clock_now=clock_now domains=domains domains_loaded=domains_loaded active_graph=active_graph active_entities=active_entities cluster_counters=cluster_counters resource_details=resource_details selected_resource=selected_resource upload_status=upload_status create=create web_console_session=web_console_session run_command=run_command />
                    <section class="main-pane">
                        <TransactionInspector
                            inspector=inspector
                            transaction_status=transaction_status
                            request_tx=web_console_session.request_tx
                            run_command=run_command
                        />
                        <GraphPanel
                            active_domain=active_domain
                            domains=domains
                            websocket_state=web_console_session.state
                            domain=active_graph
                            run_command=run_command
                            create=create
                        />
                        <ReplPanel
                            domain=active_domain_name
                            input=input
                            terminal_lines=terminal_lines
                            transaction_state=move || transaction_status.with(|status| ActiveTransaction::of(status.as_ref()))
                            subscription_tabs=subscription_tabs
                            active_subscription_tab=active_subscription_tab
                            stop_subscription=stop_subscription
                            resubscribe=resubscribe
                            suggestions=move || suggestions.get()
                            suggestion_status=move || suggestion_status.get()
                            suggestion_continuation=move || suggestion_continuation.get()
                            request_suggestions=request_suggestions
                            input_enabled=move || domains_loaded.get()
                            run_command=run_command
                        />
                    </section>
                </div>
                <CreateDialog
                    signals=create
                    active_domain=active_domain
                    connection_state=web_console_session.state
                    session_generation=session_generation
                    request_tx=web_console_session.request_tx
                    submit=submit_create
                />
            </main>
        </Show>
    }
}

/// Closes the tab `tab_id`, deleting its subscription when it still delivers. A deletion the
/// console cannot hand over leaves the tab showing its stream, with the reason.
fn close_subscription_tab(
    signals: WebConsoleSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    tab_id: u64,
) {
    let Some(request) = signals.begin_subscription_close(tab_id) else {
        return;
    };
    let stop = ConsoleRequest::SubscriptionStop { tab_id, request };
    let reason = match request_tx.get_untracked() {
        Some(request_tx) => match request_tx.send(stop) {
            Ok(()) => return,
            Err(refusal) => refusal.current_context().to_string(),
        },
        None => SESSION_UNAVAILABLE.to_string(),
    };
    restore_failed_unsubscribe(signals.subscription_tabs, tab_id, reason);
}

/// Opens the subscription of the ended tab `tab_id` again, under its name.
fn resubscribe_tab(
    signals: WebConsoleSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    tab_id: u64,
) {
    let Some(request) = signals.begin_resubscribe(tab_id) else {
        return;
    };
    send_subscription_start(
        signals,
        request_tx,
        tab_id,
        request,
        SubscriptionOrigin::Console,
    );
}

/// Hands the start of the tab `tab_id`'s subscription to the session. A start the console cannot
/// hand over fails as a refused start does: the tab and the terminal show why, and a Create form
/// that submitted it fails.
fn send_subscription_start(
    signals: WebConsoleSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    tab_id: u64,
    request: SubscribeRequest,
    origin: SubscriptionOrigin,
) {
    let start = ConsoleRequest::SubscriptionStart {
        tab_id,
        request,
        origin,
    };
    let reason = match request_tx.get_untracked() {
        Some(request_tx) => match request_tx.send(start) {
            Ok(()) => return,
            Err(refusal) => refusal.current_context().to_string(),
        },
        None => SESSION_UNAVAILABLE.to_string(),
    };
    fail_subscription_start(signals, tab_id, vec![TermLine::error(reason.clone())]);
    fail_subscription_origin(signals, origin, reason);
}

/// Sends a Create form's persistent statement on the durable command path the REPL uses, echoing
/// its masked presentation in the terminal.
fn submit_create_command(
    signals: WebConsoleSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    kind: CreateKind,
    presentation: String,
    command: CommandDispatch,
    attempt: u64,
    draft_revision: u64,
) {
    let CommandDispatch {
        query,
        domain,
        resource,
        created_domain,
    } = command;
    let create = signals.create;
    let transaction_status = signals.transaction_status;
    let prompt_transaction =
        transaction_status.with_untracked(|status| ActiveTransaction::of(status.as_ref()));
    signals.terminal_lines.update(|lines| {
        lines.push(TermLine::prompt(presentation.clone(), prompt_transaction));
    });
    let transaction = transaction_status.get_untracked();
    let expected_transaction_position = match &transaction {
        Some(status) if status.lifecycle().is_active() => Some(status.accepted_operations()),
        Some(_) | None => None,
    };
    let context = CreateCommandContext {
        attempt,
        draft_revision,
        kind,
        presentation,
        domain: domain.clone(),
        resource,
        created_domain,
    };
    let queued = ConsoleRequest::Command {
        request: CommandRequest {
            query,
            domain,
            execution_reference: command_execution_reference(),
            expected_transaction_position,
            expected_preview: None,
        },
        purpose: CommandPurpose::Create(context),
    };
    let Some(request_tx) = request_tx.get_untracked() else {
        create.failed(attempt, draft_revision, SESSION_UNAVAILABLE.to_string());
        return;
    };
    if let Err(refusal) = request_tx.send(queued) {
        create.failed(
            attempt,
            draft_revision,
            refusal.current_context().to_string(),
        );
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
    let (sender, receiver) = request_handoff();
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
    mut queued: RequestReceiver,
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
                queued.discard_waiting();
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
                    signals.create.connection_lost();
                    interrupt_subscription_tabs(signals);
                    continue;
                }
                let ended = if let WebSocketState::Open = socket.state() {
                    signals.session_generation.update(|generation| {
                        *generation = generation
                            .checked_add(1)
                            .assured("a console cannot open 2^64 websocket connections");
                    });
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
                        signals.create.connection_lost();
                        interrupt_subscription_tabs(signals);
                        continue;
                    }
                    opened_this_attempt = true;
                    reconnect_delay = WEBSOCKET_INITIAL_RECONNECT_DELAY;
                    auth_error.set(None);
                    requests.end_connection();
                    signals.create.connection_lost();
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
/// The connection first selects the active domain again, so the domain's observations resume,
/// restores the interrupted subscription tabs, and attaches the session's transaction again. The
/// tabs come before the transaction, because a session that holds a transaction refuses
/// subscriptions. Ordered requests wait until the server confirms that the serving node leads and
/// the transaction is attached, and then go out in the order the console issued them.
async fn serve_connection(
    signals: WebConsoleSignals,
    state: RwSignal<ConsoleConnectionState>,
    mut socket: WebSocket,
    requests: &mut SessionRequests,
    queued: &mut RequestReceiver,
    current_auth_token: &str,
) -> ConnectionEnd {
    let codec = ClientWebSocketCodec::new(SESSION_LIMITS);
    let mut opening = Vec::new();
    if let Some(domain) = signals.active_domain.get_untracked() {
        opening.push(ConsoleRequest::SelectDomain(SelectDomainRequest { domain }));
    }
    opening.extend(signals.begin_restorations());
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
                if !request.belongs_to_connection_generation(signals.session_generation.get_untracked()) {
                    continue;
                }
                let issued = requests.issue(request);
                if issued.request.inspects_transaction() {
                    signals.inspector.requested(issued.order.0);
                }
                match requests.accept(issued) {
                    Admission::Sent(message) => {
                        if !send_message(&mut socket, &codec, signals, requests, message).await {
                            return ConnectionEnd::Dropped;
                        }
                    }
                    Admission::Held => {}
                    Admission::Refused(refused) => {
                        let RefusedRequest { request, refusal } = *refused;
                        fail_request(signals, request, refusal.to_string());
                    }
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
                // A tab whose restoration was refused is tried again while the session is ready,
                // has room for it in flight, and holds no transaction, which would refuse it again.
                let transaction_active = signals
                    .transaction_status
                    .with_untracked(|status| transaction_is_active(status.as_ref()));
                if requests.can_send_ordered() && !transaction_active {
                    for request in signals.begin_restorations() {
                        let issued = requests.issue(request);
                        let message = requests.dispatch(issued);
                        if !send_message(&mut socket, &codec, signals, requests, message).await {
                            return ConnectionEnd::Dropped;
                        }
                    }
                }
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
/// but its previous generation must never accept rows from the replacement connection. A
/// restoration that connection left unanswered ended with it, so its tab is interrupted again and
/// the next connection restores it.
fn interrupt_subscription_tabs(signals: WebConsoleSignals) {
    signals.subscription_tabs.update(|tabs| {
        for tab in tabs.iter_mut() {
            match &tab.state {
                SubscriptionTabState::Open(_) => {
                    tab.state = SubscriptionTabState::Interrupted;
                    tab.lines.push(TermLine::info(
                        "delivery interrupted; restoring on the next connection",
                    ));
                }
                SubscriptionTabState::Restoring => tab.state = SubscriptionTabState::Interrupted,
                SubscriptionTabState::Pending
                | SubscriptionTabState::Interrupted
                | SubscriptionTabState::Ended
                | SubscriptionTabState::Resubscribing
                | SubscriptionTabState::Closing(_) => {}
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
                fail_request(signals, unsent.request, reason);
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
                Some(unread) => fail_request(signals, unread.request, reason),
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
            fail_request(signals, request.request, reason);
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
            signals.end_subscription(&ended);
            SessionStep::Continue
        }
        ServerEvent::DomainClockObserved(observed) => {
            signals.clock_display.update(|display| {
                display.observed(&observed.domain, observed.clock.clone());
            });
            let line = TermLine::info(format!(
                "domain clock [{}]: {}",
                observed.domain, observed.clock
            ));
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        ServerEvent::DomainClockTicked(ticked) => {
            signals.clock_display.update(|display| {
                display.ticked(&ticked.domain, ticked.tick.clone());
            });
            let line = TermLine::info(format!(
                "domain clock [{}] tick: generation {}, id {}, boundary {}, authority UTC {}, \
                 node logical {}",
                ticked.domain,
                ticked.tick.generation,
                ticked.tick.tick_id,
                ticked.tick.logical_boundary.to_rfc3339(),
                ticked.tick.authority_utc.to_rfc3339(),
                ticked.tick.serving_logical.to_rfc3339(),
            ));
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        ServerEvent::DomainClockAttachmentEnded(ended) => {
            signals.clock_display.update(|display| {
                display.ended(&ended.domain, ended.reason.to_string());
            });
            let line = TermLine::error(format!(
                "domain clock [{}]: the attachment ended because {}",
                ended.domain, ended.reason
            ));
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        // The console opens no producers, so these name a producer of no view; they are shown
        // rather than dropped.
        ServerEvent::ProducerAdmissionChanged(changed) => {
            let line = TermLine::info(format!(
                "producer {}: admission {}",
                changed.producer,
                changed.admission.as_ref()
            ));
            signals.terminal_lines.update(|lines| lines.push(line));
            SessionStep::Continue
        }
        ServerEvent::ProducerEnded(ended) => {
            let line = TermLine::error(ended.message);
            signals.terminal_lines.update(|lines| lines.push(line));
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
            fail_request(signals, request, rejected.message);
            SessionStep::Continue
        }
        (request, ReplyBody::Cancelled(cancelled)) => {
            fail_request(signals, request, cancellation_reason(cancelled));
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
        (
            ConsoleRequest::DomainClockAttach {
                request, origin, ..
            },
            ReplyBody::DomainClockAttach(outcome),
        ) => {
            signals.apply_clock_attach_outcome(&request.domain, origin, outcome);
            SessionStep::Continue
        }
        (
            ConsoleRequest::DomainClockDetach {
                request, origin, ..
            },
            ReplyBody::DomainClockDetach(outcome),
        ) => {
            signals.apply_clock_detach_outcome(&request.domain, origin, outcome);
            SessionStep::Continue
        }
        (ConsoleRequest::Suggest(request), ReplyBody::Suggest(outcome)) => {
            let Some(query) = signals.suggestion_query.get_untracked() else {
                return SessionStep::Continue;
            };
            if query.input != request.input()
                || query.cursor != request.cursor()
                || query.domain.as_ref() != request.domain()
                || query.domain != signals.active_domain.get_untracked()
            {
                return SessionStep::Continue;
            }
            signals.suggestion_status.set(Some(outcome.status));
            signals.suggestion_continuation.set(outcome.continuation);
            let text_suggestions = outcome
                .suggestions
                .into_iter()
                .filter(|suggestion| suggestion.kind == SuggestionKind::Text)
                .collect::<Vec<_>>();
            if request.continuation().is_some() && outcome.status == SuggestionStatus::Ready {
                signals
                    .suggestions
                    .update(|suggestions| suggestions.extend(text_suggestions));
            } else {
                signals.suggestions.set(text_suggestions);
            }
            SessionStep::Continue
        }
        (ConsoleRequest::Choice { context, .. }, ReplyBody::Choice(outcome)) => {
            signals.create.apply_choice(
                context,
                signals.session_generation.get_untracked(),
                outcome,
            );
            SessionStep::Continue
        }
        (ConsoleRequest::AttachTransaction(_), ReplyBody::Attach(outcome)) => {
            apply_attach_outcome(signals, outcome)
        }
        (ConsoleRequest::InspectTransaction(request), ReplyBody::Inspection(outcome)) => {
            match outcome {
                InspectionOutcome::Inspected(inspection) => {
                    signals.inspector.accept(
                        *inspection,
                        order.0,
                        Some(&request.target),
                        signals.transaction_status.get_untracked().as_ref(),
                    );
                    SessionStep::Continue
                }
                InspectionOutcome::Rejected { message, .. } => {
                    signals.inspector.error.set(Some(message));
                    SessionStep::Continue
                }
                InspectionOutcome::NotLeader(redirect) => {
                    requests.hold_again(IssuedRequest {
                        order,
                        request: ConsoleRequest::InspectTransaction(request),
                    });
                    redirect_step(signals, &redirect)
                }
            }
        }
        (
            ConsoleRequest::SubscriptionStart {
                tab_id,
                request,
                origin,
            },
            ReplyBody::Subscribe(outcome),
        ) => {
            let started = SubscriptionStarted {
                tab_id,
                statement: &request.statement,
                origin,
            };
            apply_subscribe_outcome(signals, requests, started, outcome);
            SessionStep::Continue
        }
        (ConsoleRequest::SubscriptionStop { tab_id, request }, ReplyBody::Unsubscribe(outcome)) => {
            apply_unsubscribe_outcome(signals, tab_id, &request, outcome);
            SessionStep::Continue
        }
        (request, _) => {
            fail_request(signals, request, UNEXPECTED_REPLY.to_string());
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
        if let CommandPurpose::Create(context) = &purpose {
            signals
                .create
                .queued_reconnect(context.attempt, context.draft_revision);
        }
        let step = redirect_step(signals, redirect);
        requests.hold_again(IssuedRequest {
            order,
            request: ConsoleRequest::Command { request, purpose },
        });
        return step;
    }
    if matches!(outcome.disposition, CommandDisposition::OutcomeUnknown(_)) {
        if let CommandPurpose::Create(context) = &purpose {
            signals
                .create
                .queued_reconnect(context.attempt, context.draft_revision);
        }
        requests.hold_again(IssuedRequest {
            order,
            request: ConsoleRequest::Command { request, purpose },
        });
        return SessionStep::Reconnect;
    }
    let queued_transaction_position = queued_transaction_position(&outcome);
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
        CommandPurpose::Repl => show_command_outcome(signals, order, &request.query, outcome),
        CommandPurpose::Create(context) => {
            show_create_outcome(
                signals,
                requests,
                context,
                outcome,
                queued_transaction_position,
            );
        }
        CommandPurpose::ResourceDescription { resource } => {
            let detail = ResourceDetailView::from_description(outcome);
            signals.resource_details.update(|details| {
                details.insert(resource, detail);
            });
        }
    }
    SessionStep::Continue
}

fn show_create_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    context: CreateCommandContext,
    outcome: CommandOutcome,
    queued_transaction_position: Option<usize>,
) {
    let completed = matches!(outcome.disposition, CommandDisposition::Completed { .. });
    let failure = if outcome.message.is_empty() {
        "Create failed".to_string()
    } else {
        outcome.message.clone()
    };
    let lines = command_outcome_lines(outcome, &context.presentation);
    signals
        .terminal_lines
        .update(|terminal| terminal.extend(lines));

    if let Some(position) = queued_transaction_position {
        signals
            .create
            .queued_transaction(context.attempt, context.draft_revision, position);
        return;
    }
    if !completed {
        signals
            .create
            .failed(context.attempt, context.draft_revision, failure);
        return;
    }
    if !signals
        .create
        .completed(context.attempt, context.draft_revision)
    {
        return;
    }
    if context.kind == CreateKind::Domain
        && let Some(domain) = context.created_domain
    {
        signals.active_domain.set(Some(domain));
    }
    if context.kind == CreateKind::Resource
        && let (Some(resource), Some(domain)) = (context.resource, context.domain)
    {
        signals.selected_resource.set(Some(resource.clone()));
        signals.upload_status.set(String::new());
        let issued = requests.issue(ConsoleRequest::Command {
            request: CommandRequest {
                query: format!("DESCRIBE RESOURCE {resource};"),
                domain: Some(domain),
                execution_reference: command_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::ResourceDescription { resource },
        });
        requests.hold_again(issued);
    }
}

/// The accepted position of a popup command that remains queued in an attached transaction.
///
/// Standalone model commands also use a short durable transaction internally. Their completed
/// outcomes retain an admission as retry evidence but carry no active transaction, so that
/// admission describes completed work rather than a queued operation.
fn queued_transaction_position(outcome: &CommandOutcome) -> Option<usize> {
    outcome
        .transaction
        .as_ref()
        .filter(|status| status.lifecycle().is_active())?;
    outcome
        .transaction_admission
        .as_ref()
        .map(|admission| admission.operation.get())
}

/// Prints the outcome of a REPL command, and makes the domain a completed `CREATE DOMAIN` created
/// the active one.
fn show_command_outcome(
    signals: WebConsoleSignals,
    order: IssueOrder,
    query: &str,
    mut outcome: CommandOutcome,
) {
    if let Some(inspection) = outcome.inspection.take() {
        signals.inspector.accept(
            *inspection,
            order.0,
            None,
            signals.transaction_status.get_untracked().as_ref(),
        );
    }
    if let CommandDisposition::PreviewStale { .. } = &outcome.disposition {
        signals.inspector.requested(order.0);
        signals.inspector.error.set(Some(
            "Preview is stale. Refresh the inspection before committing.".to_string(),
        ));
        signals.inspector.stale_preview.set(true);
    }
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
/// Once the attach ends, every held command keeps its issue order and execution reference. The
/// server recovers a recorded outcome even when the transaction finished, or returns a failure for
/// a command it never admitted. The other held requests also go out once the session is ready.
fn apply_attach_outcome(signals: WebConsoleSignals, outcome: AttachOutcome) -> SessionStep {
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
                let lines = completed_lines(message);
                signals
                    .terminal_lines
                    .update(|terminal| terminal.extend(lines));
            }
            SessionStep::Continue
        }
        AttachDisposition::AlreadyFinished(status) => {
            adopt_transaction(signals, status);
            let lines = failed_lines(message, diagnostics, query);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
            SessionStep::Continue
        }
        AttachDisposition::Failed => {
            signals.transaction_status.set(None);
            let lines = failed_lines(message, diagnostics, query);
            signals
                .terminal_lines
                .update(|terminal| terminal.extend(lines));
            SessionStep::Continue
        }
        AttachDisposition::NotLeader(redirect) => redirect_step(signals, &redirect),
    }
}

/// A subscription start whose outcome arrived: the tab it opens, the statement it sent, and who
/// asked for it.
struct SubscriptionStarted<'a> {
    tab_id: u64,
    statement: &'a str,
    origin: SubscriptionOrigin,
}

/// Applies the outcome of opening a tab's subscription. An opened subscription streams its rows
/// into its tab, and a failure removes a tab that never opened and shows why. A Create form that
/// submitted the subscription completes or fails with the same outcome.
fn apply_subscribe_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    started: SubscriptionStarted<'_>,
    outcome: SubscribeOutcome,
) {
    let SubscriptionStarted {
        tab_id,
        statement,
        origin,
    } = started;
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
                    SubscriptionTabState::Pending
                    | SubscriptionTabState::Restoring
                    | SubscriptionTabState::Resubscribing => {
                        tab.state = SubscriptionTabState::Open(stream.clone());
                        opened = true;
                    }
                    SubscriptionTabState::Closing(None) => {
                        tab.state = SubscriptionTabState::Closing(Some(stream.clone()));
                        close_after_open = true;
                    }
                    SubscriptionTabState::Open(_)
                    | SubscriptionTabState::Interrupted
                    | SubscriptionTabState::Ended
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
            if let SubscriptionOrigin::Create {
                attempt,
                draft_revision,
            } = origin
            {
                signals.create.completed(attempt, draft_revision);
            }
        }
        SubscribeDisposition::Failed => {
            let reason = message.clone();
            let lines = failed_lines(message, diagnostics, statement);
            fail_subscription_start(signals, tab_id, lines);
            fail_subscription_origin(signals, origin, reason);
        }
    }
}

/// Fails the Create form attempt that submitted a subscription which could not open. A statement
/// typed in the REPL or a restored tab reports only through its tab and the terminal.
fn fail_subscription_origin(
    signals: WebConsoleSignals,
    origin: SubscriptionOrigin,
    reason: String,
) {
    if let SubscriptionOrigin::Create {
        attempt,
        draft_revision,
    } = origin
    {
        signals.create.failed(attempt, draft_revision, reason);
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
fn fail_request(signals: WebConsoleSignals, request: ConsoleRequest, reason: String) {
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
            purpose: CommandPurpose::Create(context),
            ..
        } => {
            signals
                .create
                .failed(context.attempt, context.draft_revision, reason.clone());
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
        ConsoleRequest::SubscriptionStart { tab_id, origin, .. } => {
            fail_subscription_start(signals, tab_id, vec![TermLine::error(reason.clone())]);
            fail_subscription_origin(signals, origin, reason);
        }
        ConsoleRequest::SubscriptionStop { tab_id, .. } => {
            restore_failed_unsubscribe(signals.subscription_tabs, tab_id, reason.clone());
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
        ConsoleRequest::DomainClockAttach {
            request, origin, ..
        } => {
            if origin == ClockRequestOrigin::Automatic {
                signals.clock_display.update(|display| {
                    display.refuse(&request.domain, reason.clone());
                });
            }
            signals.terminal_lines.update(|lines| {
                lines.push(TermLine::error(format!(
                    "domain clock [{}] attach failed: {reason}",
                    request.domain
                )));
            });
        }
        ConsoleRequest::DomainClockDetach { request, .. } => {
            signals.terminal_lines.update(|lines| {
                lines.push(TermLine::error(format!(
                    "domain clock [{}] detach failed: {reason}",
                    request.domain
                )));
            });
        }
        ConsoleRequest::Suggest(_) => {
            // An empty list alone would read as no matches.
            signals.suggestions.set(Vec::new());
            signals
                .suggestion_status
                .set(Some(SuggestionStatus::LookupFailed));
        }
        ConsoleRequest::Choice { context, .. } => {
            signals
                .create
                .fail_choice(context, signals.session_generation.get_untracked(), reason)
        }
        ConsoleRequest::InspectTransaction(_) => signals.inspector.error.set(Some(reason)),
        ConsoleRequest::AttachTransaction(_) => {
            // Held commands keep their references so the server can recover a recorded outcome
            // or report that a command was never admitted.
            signals.transaction_status.set(None);
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::error(reason)));
        }
    }
}

/// Takes the session's transaction as the server reports it, and makes its domain the active one.
fn adopt_transaction(signals: WebConsoleSignals, status: TransactionStatus) {
    signals.inspector.retain_finished(&status);
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

/// Keeps the latest snapshot of the active domain's graph and entities. A snapshot of a domain the
/// console no longer shows, sent before the session learned of the newly selected domain, is
/// dropped.
fn apply_snapshot(signals: WebConsoleSignals, snapshot: &DomainSnapshotObserved) {
    let observed = signals
        .active_domain
        .with_untracked(|active| active.as_ref() == Some(snapshot.domain()));
    if !observed {
        return;
    }
    match DataflowGraph::deserialize(snapshot.graph_json().as_bytes()) {
        Ok(graph) => {
            let view =
                DomainSnapshotView::new(snapshot.domain().clone(), snapshot.entities(), graph);
            signals.domain_snapshot.set(Some(view));
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

/// A creation failure leaves no live tab. A failed restoration keeps the acknowledged tab visible
/// and interrupted, so it can be restored on the next connection. A failed resubscription leaves
/// the tab ended, showing why, until the operator resubscribes or closes it.
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
            SubscriptionTabState::Resubscribing => {
                tab.state = SubscriptionTabState::Ended;
                tab.lines.extend(lines.clone());
            }
            SubscriptionTabState::Open(_)
            | SubscriptionTabState::Interrupted
            | SubscriptionTabState::Ended
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

/// A tab names the relay it reads, followed by its filter when it has one.
fn subscription_tab_title(subscription: &CreateSubscription) -> String {
    let relay = subscription.relay.to_string();
    let Some(filter) = &subscription.where_clause else {
        return relay;
    };
    let filter = expression_to_nspl(filter)
        .assured("an expression the NSPL parser produced renders back to NSPL");
    format!("{relay} {filter}")
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

/// What the resource dialog shows for a completed description that carries no typed description.
const MISSING_RESOURCE_DESCRIPTION: &str = "the server returned no resource description";

impl ResourceDetailView {
    /// Reads the dialog's versions from the typed description `DESCRIBE RESOURCE` returns beside
    /// the text the REPL prints, attaching to each version the usages that pin it. A description
    /// that did not complete shows its message instead.
    fn from_description(outcome: CommandOutcome) -> Self {
        let CommandDisposition::Completed { .. } = outcome.disposition else {
            return Self {
                versions: Vec::new(),
                status: outcome.message,
            };
        };
        let Some(description) = outcome.resource else {
            return Self {
                versions: Vec::new(),
                status: MISSING_RESOURCE_DESCRIPTION.to_string(),
            };
        };
        let ResourceDescription {
            versions, usages, ..
        } = *description;
        let mut usages_by_version = BTreeMap::<NonZeroU64, Vec<ResourceUsage>>::new();
        for usage in usages {
            usages_by_version
                .entry(usage.version)
                .or_default()
                .push(usage);
        }
        let mut version_views = Vec::with_capacity(versions.len());
        for version in versions {
            let usages = usages_by_version
                .remove(&version.version)
                .unwrap_or_default();
            version_views.push(ResourceVersionView { version, usages });
        }
        Self {
            versions: version_views,
            status: "ready".to_string(),
        }
    }
}

fn resource_version_summary(version: &ResourceVersionDescription) -> String {
    format!(
        "{} files | {} bytes | from {} | {}",
        version.file_count, version.total_bytes, version.created_by_node, version.created_at
    )
}

fn resource_version_checksums(version: &ResourceVersionDescription) -> String {
    format!(
        "root {} | manifest {}",
        version.root_checksum, version.manifest_checksum
    )
}

fn resource_entry_summary(entry: &ResourceManifestEntry) -> String {
    match &entry.content {
        ResourceEntryContent::Directory => "directory".to_string(),
        ResourceEntryContent::File { size, checksum } => {
            format!("file | {size} bytes | checksum {checksum}")
        }
    }
}

/// The entries of one version row: each entry by its path, or why the serving node could not
/// list them.
fn resource_entries_view(entries: ResourceVersionEntries) -> AnyView {
    match entries {
        ResourceVersionEntries::Listed(entries) => view! {
            <For
                each=move || entries.clone()
                key=|entry| entry.clone()
                children=|entry| {
                    let summary = resource_entry_summary(&entry);
                    view! {
                        <div class="resource-file-row">
                            <strong>{entry.path}</strong>
                            <span>{summary}</span>
                        </div>
                    }
                }
            />
        }
        .into_any(),
        ResourceVersionEntries::Unavailable { reason } => view! {
            <div class="resource-file-row resource-file-unavailable">
                <strong>"entries unavailable"</strong>
                <span>{reason}</span>
            </div>
        }
        .into_any(),
    }
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
    clock_display: RwSignal<ClockDisplay>,
    domains: RwSignal<Vec<DomainView>>,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
    transaction_status: RwSignal<Option<TransactionStatus>>,
    inspector: InspectorSignals,
    create: CreateSignals,
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
                <CreateMenu signals=create active_domain=active_domain />
                <Show when=move || transaction_status.get().is_some_and(|status| status.lifecycle().is_active()) fallback=|| ()>
                    <button class="transaction-indicator" type="button" on:click=move |_| inspector.open_attached()>
                        "Transaction · Inspect"
                    </button>
                </Show>
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
                <ClockStatus display=clock_display />
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
    clock_display: RwSignal<ClockDisplay>,
    clock_now: RwSignal<Timestamp>,
    domains: RwSignal<Vec<DomainView>>,
    domains_loaded: RwSignal<bool>,
    active_graph: impl Fn() -> Option<GraphView> + Copy + Send + Sync + 'static,
    active_entities: impl Fn() -> Vec<EntityView> + Copy + Send + Sync + 'static,
    cluster_counters: RwSignal<ClusterCounters>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
    selected_resource: RwSignal<Option<String>>,
    upload_status: RwSignal<String>,
    create: CreateSignals,
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
                <ClockPanel display=clock_display now=clock_now />
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
                <button
                    id="sidebar-create-resource"
                    class="sidebar-create-button"
                    type="button"
                    on:click=move |_| create.open(
                        CreateKind::Resource,
                        active_domain.get_untracked(),
                        "sidebar-create-resource",
                    )
                >
                    <span aria-hidden="true">"＋"</span>
                    <span>"Create resource in this domain"</span>
                </button>
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
                                            resource_details,
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

/// Asks for the typed description of `resource`, which the resource dialog shows. A request the
/// console cannot hand over leaves the dialog showing why.
fn request_resource_describe(
    request_tx: RwSignal<Option<RequestSender>>,
    resource_details: RwSignal<BTreeMap<String, ResourceDetailView>>,
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
        purpose: CommandPurpose::ResourceDescription {
            resource: resource.clone(),
        },
    };
    let reason = match request_tx.get_untracked() {
        Some(request_tx) => match request_tx.send(queued) {
            Ok(()) => return,
            Err(refusal) => refusal.current_context().to_string(),
        },
        None => SESSION_UNAVAILABLE.to_string(),
    };
    let detail = ResourceDetailView {
        versions: Vec::new(),
        status: reason,
    };
    resource_details.update(|details| {
        details.insert(resource, detail);
    });
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
    request_tx: RwSignal<Option<RequestSender>>,
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
            request_resource_describe(
                request_tx,
                details,
                resource_name,
                active_domain.get_untracked(),
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
                                let ResourceVersionView { version, usages } = version;
                                let summary = resource_version_summary(&version);
                                let checksums = resource_version_checksums(&version);
                                let number = version.version;
                                let unbound = usages.is_empty();
                                view! {
                                    <div class="resource-version-row" data-version=number.to_string()>
                                        <strong>{format!("version {number}")}</strong>
                                        <span>{summary.clone()}</span>
                                        <em>{checksums.clone()}</em>
                                        <div class="resource-file-list">
                                            {resource_entries_view(version.entries)}
                                        </div>
                                        <div class="resource-usage-list">
                                            <p>"usages"</p>
                                            <For
                                                each=move || usages.clone()
                                                key=|usage| usage.clone()
                                                children=|usage| {
                                                    view! {
                                                        <div class="resource-usage-row">
                                                            <em>{usage.node.kind.keyword_phrase()}</em>
                                                            " "
                                                            <strong>{usage.node.identifier.as_str().to_string()}</strong>
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
    create: CreateSignals,
) -> impl IntoView {
    let selected_action_target = RwSignal::new(None::<GraphActionTarget>);
    let selected_branch_group = RwSignal::new(None::<String>);
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
                            id="graph-search"
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
                                            selected_action_target.set(Some(GraphActionTarget::relay(&click_relay)));
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
                                            && let Some(domain) = active_domain.get_untracked()
                                        {
                                            selected_action_target.set(None);
                                            create.open_subscription(domain, relay, "graph-search");
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
    resubscribe: impl Fn(u64) + Copy + Send + Sync + 'static,
    suggestions: impl Fn() -> Vec<WireSuggestion> + Copy + Send + 'static,
    suggestion_status: impl Fn() -> Option<SuggestionStatus> + Copy + Send + 'static,
    suggestion_continuation: impl Fn() -> Option<String> + Copy + Send + 'static,
    request_suggestions: impl Fn(String, usize, Option<String>) + Copy + Send + 'static,
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
    // Submits the input as the operator sees it, and keeps it for `ArrowUp` when the history can.
    let submit_input = move || {
        let command = match input_ref.get_untracked() {
            Some(element) => element.value(),
            None => input.get_untracked(),
        };
        completion_cycle.set(None);
        let mut pushed = HistoryPush::Kept;
        command_history.update(|history| pushed = history.push(command.as_str()));
        input.set(command.clone());
        run_command(Some(command));
        if pushed == HistoryPush::TooLarge {
            terminal_lines
                .update(|lines| lines.push(TermLine::info(COMMAND_TOO_LARGE_FOR_HISTORY)));
        }
    };
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
                        let name = tab.name.to_string();
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
                                data-subscription-name=name
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
                                <Show when=move || state().can_resubscribe() fallback=|| ()>
                                    <button
                                        type="button"
                                        class="tab-resubscribe"
                                        title="Resubscribe"
                                        aria-label="Resubscribe"
                                        on:click=move |event| {
                                            event.stop_propagation();
                                            resubscribe(tab_id);
                                        }
                                    >
                                        "↻"
                                    </button>
                                </Show>
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
                    key=|suggestion| suggestion.value.clone()
                    children={move |suggestion| {
                        let edit = suggestion.edit.clone();
                        view! {
                            <button
                                type="button"
                                on:click=move |_| {
                                    completion_cycle.set(None);
                                    input.set(apply_completion(&input.get_untracked(), &edit));
                                }
                            >
                                {suggestion.value}
                            </button>
                        }
                    }}
                />
            </div>
            <button
                type="button"
                class="completion-more"
                class:hidden=move || !repl_active() || suggestion_continuation().is_none()
                on:click=move |_| {
                    let Some(next) = suggestion_continuation() else {
                        return;
                    };
                    let value = input.get_untracked();
                    let cursor = match input_ref.get_untracked() {
                        Some(element) => browser_cursor_byte_offset(&value, &element),
                        None => value.len(),
                    };
                    request_suggestions(value, cursor, Some(next));
                }
            >"MORE SUGGESTIONS"</button>
            <div class="completion-status" class:hidden=move || !repl_active() || matches!(suggestion_status(), None | Some(SuggestionStatus::Ready))>
                {move || match suggestion_status() {
                    Some(SuggestionStatus::MissingContext) => "Select an existing domain for this reference.",
                    Some(SuggestionStatus::StaleContext) => "Transaction context changed; reconnect or reattach it.",
                    Some(SuggestionStatus::LookupFailed) => "Completion lookup failed; try again.",
                    Some(SuggestionStatus::Ready) | None => "",
                }}
            </div>
            <form class="prompt-row" class:hidden=move || !repl_active() on:submit=move |event| {
                event.prevent_default();
                submit_input();
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
                        let cursor = match input_ref.get_untracked() {
                            Some(element) => browser_cursor_byte_offset(&value, &element),
                            None => value.len(),
                        };
                        request_suggestions(value, cursor, None);
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
                                input.set(apply_completion(&source, &suggestion_items[index].edit));
                                completion_cycle.set(Some(CompletionCycle {
                                    source,
                                    next_index: (index + 1) % suggestion_items.len(),
                                }));
                            } else {
                                let value = input.get_untracked();
                                let cursor = match input_ref.get_untracked() {
                                    Some(element) => browser_cursor_byte_offset(&value, &element),
                                    None => value.len(),
                                };
                                request_suggestions(value, cursor, None);
                            }
                        } else if event.key() == "ArrowUp" {
                            event.prevent_default();
                            let current = match input_ref.get_untracked() {
                                Some(element) => element.value(),
                                None => input.get_untracked(),
                            };
                            completion_cycle.set(None);
                            let mut recalled = None;
                            command_history.update(|history| {
                                recalled = history.previous(current);
                            });
                            if let Some(RecalledCommand { command, reached_omission }) = recalled {
                                if reached_omission {
                                    terminal_lines.update(|lines| {
                                        lines.push(TermLine::info(COMMAND_HISTORY_MARKER));
                                    });
                                }
                                input.set(command.clone());
                                request_suggestions(command.clone(), command.len(), None);
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
                                request_suggestions(command.clone(), command.len(), None);
                            }
                        } else if event.key() == "Enter" && (event.meta_key() || event.ctrl_key()) {
                            event.prevent_default();
                            submit_input();
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

/// The most commands the REPL keeps for `ArrowUp` and `ArrowDown`.
const MAX_COMMAND_HISTORY_RECORDS: usize = 256;
/// The most bytes of command text the REPL keeps for them.
const MAX_COMMAND_HISTORY_BYTES: usize = 256 * 1024;
const COMMAND_HISTORY_MARKER: &str = "command history limit reached; earlier commands were omitted";
const COMMAND_TOO_LARGE_FOR_HISTORY: &str =
    "the command is larger than the command history keeps, so ArrowUp cannot recall it";
const _: () = assert!(MAX_COMMAND_HISTORY_RECORDS > 0);

/// The commands the REPL submitted, oldest first, bounded by count and by bytes. The oldest
/// commands give way to newer ones, and a walk back through the history reports that it reached
/// the point where earlier commands were omitted.
#[derive(Default)]
struct CommandHistory {
    entries: VecDeque<String>,
    bytes: usize,
    /// Whether commands older than the oldest one kept were omitted.
    omitted: bool,
    position: Option<usize>,
    draft: String,
}

/// What the history did with a submitted command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryPush {
    /// Nothing of the command is lost: it can be recalled, or it was empty.
    Kept,
    /// The command alone is larger than the whole history, so it cannot be recalled.
    TooLarge,
}

/// A command a walk back through the history recalled.
struct RecalledCommand {
    command: String,
    /// The walk arrived at the oldest command the history keeps, and earlier ones were omitted.
    reached_omission: bool,
}

impl CommandHistory {
    fn push(&mut self, command: &str) -> HistoryPush {
        self.reset_navigation();
        let command = command.trim();
        let repeats_newest = self.entries.back().is_some_and(|entry| entry == command);
        if command.is_empty() || repeats_newest {
            return HistoryPush::Kept;
        }
        if command.len() > MAX_COMMAND_HISTORY_BYTES {
            return HistoryPush::TooLarge;
        }
        while self.entries.len() >= MAX_COMMAND_HISTORY_RECORDS
            || self.bytes.checked_add(command.len()).assured(
                "the kept commands stay within the byte capacity, and the command was held to it \
                 above, so their sum is at most twice the capacity",
            ) > MAX_COMMAND_HISTORY_BYTES
        {
            let evicted = self
                .entries
                .pop_front()
                .verified("the capacity condition requires an existing command to evict");
            self.bytes = self
                .bytes
                .checked_sub(evicted.len())
                .assured("the retained byte count includes the evicted command");
            self.omitted = true;
        }
        self.bytes = self
            .bytes
            .checked_add(command.len())
            .assured("the capacity loop left room for the new command");
        self.entries.push_back(command.to_string());
        HistoryPush::Kept
    }

    fn previous(&mut self, current: String) -> Option<RecalledCommand> {
        let newest = self.entries.len().checked_sub(1)?;
        let next_position = match self.position {
            // Stepping back from the oldest entry stays on it.
            Some(0) => 0,
            Some(position) => position
                .checked_sub(1)
                .verified("the arm above takes the oldest position"),
            None => {
                self.draft = current;
                newest
            }
        };
        let arrived_at_oldest = next_position == 0 && self.position != Some(0);
        self.position = Some(next_position);
        let command = self.entries.get(next_position).cloned().assured(
            "a walk starts at the newest command and only moves back, and every push that changes \
             the commands ends the walk",
        );
        Some(RecalledCommand {
            command,
            reached_omission: arrived_at_oldest && self.omitted,
        })
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

fn browser_cursor_byte_offset(value: &str, element: &web_sys::HtmlInputElement) -> usize {
    let Some(cursor) = element.selection_start().ok().flatten() else {
        return value.len();
    };
    let target =
        usize::try_from(cursor).assured("browser UTF-16 offsets fit the target pointer width");
    let mut utf16_offset = 0;
    for (byte_offset, character) in value.char_indices() {
        if utf16_offset >= target {
            return byte_offset;
        }
        utf16_offset = utf16_offset
            .checked_add(character.len_utf16())
            .assured("the browser cursor cannot exceed a string already held in memory");
        if utf16_offset > target {
            return byte_offset;
        }
    }
    value.len()
}

fn apply_completion(input: &str, edit: &TextEdit) -> String {
    let start = usize::try_from(edit.start).assured("wire offsets fit the target pointer width");
    let end = usize::try_from(edit.end).assured("wire offsets fit the target pointer width");
    if start > end || input.get(start..end).is_none() {
        return input.to_string();
    }
    let mut completed = String::with_capacity(input.len());
    completed.push_str(&input[..start]);
    completed.push_str(&edit.replacement);
    completed.push_str(&input[end..]);
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
    /// The relay a SUBSCRIBE action opens the subscription form for.
    relay: Option<RelayName>,
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

    /// A relay's actions. SUBSCRIBE is offered for a relay whose drawn label names it, which is
    /// every relay the server draws.
    fn relay(relay: &GraphViewRelay) -> Self {
        let name = relay.label.clone();
        Self {
            kind: "RELAY",
            name: name.clone(),
            describe_command: Some(format!("DESCRIBE RELAY {name};")),
            show_create_command: format!("SHOW CREATE RELAY {name};"),
            relay: RelayName::parse(&name).ok(),
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
    domain: DomainName,
    dataflow_graph: DataflowGraph,
    entities: Vec<EntityView>,
}

impl DomainSnapshotView {
    fn new(domain: DomainName, entities: &[DomainEntity], dataflow_graph: DataflowGraph) -> Self {
        let mut entities = entities.iter().map(EntityView::from).collect::<Vec<_>>();
        entities.sort_by(EntityView::sidebar_order);
        Self {
            domain,
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
    use nervix_client_wire::{
        DomainClockAttachmentEndReason, DomainClockAttachmentEnded, DomainClockObserved,
        DomainClockTicked, DomainList, DomainsObserved, OutcomeOrigin, Reply, ReplyDelivery,
        SessionLimitSettings, SourceSpan, SubscriptionEndReason,
    };
    use nervix_dataflow_graph::{
        DataflowBranchStatistics, DataflowEdge, DataflowNode, DataflowProcessorKind,
    };
    use nervix_models::{
        ClusterNodeName, DomainClockObservation, DomainClockObservedState, DomainClockPeriod,
        DomainClockSkew, DomainClockTickObservation, ImpactPlanningBasis, ImpactReportCompleteness,
        ModelName, NodeRef, ResourceName, Timestamp, TransactionImpactReport,
        TransactionInspection, TransactionInspectionRejection, TransactionInspectionTarget,
        TransactionLifecycle, TransactionOperationAdmission, TransactionOperationNumber,
        TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
    };

    use super::*;

    #[test]
    fn completion_edits_preserve_unicode_suffixes_and_ignore_invalid_byte_ranges() {
        let input = "SHOW CLUST;😊";
        assert_eq!(
            apply_completion(
                input,
                &TextEdit {
                    start: 5,
                    end: 10,
                    replacement: "CLUSTER".to_string(),
                },
            ),
            "SHOW CLUSTER;😊"
        );
        let unicode_input = "éclair";
        for (start, end) in [(1, 2), (3, 2), (0, 99)] {
            assert_eq!(
                apply_completion(
                    unicode_input,
                    &TextEdit {
                        start,
                        end,
                        replacement: "changed".to_string(),
                    },
                ),
                unicode_input
            );
        }
    }

    #[test]
    fn completion_replies_filter_local_paths_append_pages_and_ignore_stale_queries() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let domain = domain_name("tenant");
            let query = SuggestionQuery {
                input: "SH".to_string(),
                cursor: 2,
                domain: Some(domain.clone()),
            };
            signals.suggestion_query.set(Some(query));
            let mut requests = SessionRequests::new();
            let suggestion = |value: &str, kind| WireSuggestion {
                value: value.to_string(),
                kind,
                edit: TextEdit {
                    start: 0,
                    end: 2,
                    replacement: value.to_string(),
                },
            };
            let first = SuggestRequest::new("SH".to_string(), 2, Some(domain.clone()))
                .assured("the test cursor ends at a UTF-8 boundary");
            let first = requests.issue(ConsoleRequest::Suggest(first));
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: first,
                    body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                        status: SuggestionStatus::Ready,
                        continuation: Some("page-two".to_string()),
                        suggestions: vec![
                            suggestion("SHOW", SuggestionKind::Text),
                            suggestion("./local", SuggestionKind::LocalDirectoryLookup),
                        ],
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(signals.suggestions.get_untracked().len(), 1);
            assert_eq!(signals.suggestions.get_untracked()[0].value, "SHOW");
            assert_eq!(
                signals.suggestion_status.get_untracked(),
                Some(SuggestionStatus::Ready)
            );
            assert_eq!(
                signals.suggestion_continuation.get_untracked().as_deref(),
                Some("page-two")
            );

            let next = SuggestRequest::new("SH".to_string(), 2, Some(domain.clone()))
                .assured("the test cursor ends at a UTF-8 boundary")
                .with_page(64, Some("page-two".to_string()))
                .assured("the test page size is valid");
            let next = requests.issue(ConsoleRequest::Suggest(next));
            apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: next,
                    body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                        status: SuggestionStatus::Ready,
                        continuation: None,
                        suggestions: vec![suggestion("SHUTDOWN", SuggestionKind::Text)],
                    }),
                },
            );
            let values = signals
                .suggestions
                .get_untracked()
                .into_iter()
                .map(|item| item.value)
                .collect::<Vec<_>>();
            assert_eq!(values, ["SHOW", "SHUTDOWN"]);
            assert_eq!(signals.suggestion_continuation.get_untracked(), None);

            let stale = SuggestRequest::new("S".to_string(), 1, Some(domain))
                .assured("the test cursor ends at a UTF-8 boundary");
            let stale = requests.issue(ConsoleRequest::Suggest(stale));
            apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: stale,
                    body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                        status: SuggestionStatus::LookupFailed,
                        continuation: None,
                        suggestions: Vec::new(),
                    }),
                },
            );
            assert_eq!(
                signals.suggestion_status.get_untracked(),
                Some(SuggestionStatus::Ready)
            );
            assert_eq!(signals.suggestions.get_untracked().len(), 2);

            let failed = SuggestRequest::new("SH".to_string(), 2, Some(domain_name("tenant")))
                .assured("the test cursor ends at a UTF-8 boundary");
            let failed = requests.issue(ConsoleRequest::Suggest(failed));
            apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: failed,
                    body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                        status: SuggestionStatus::LookupFailed,
                        continuation: None,
                        suggestions: Vec::new(),
                    }),
                },
            );
            assert_eq!(
                signals.suggestion_status.get_untracked(),
                Some(SuggestionStatus::LookupFailed)
            );
            assert!(signals.suggestions.get_untracked().is_empty());
        });
    }

    /// A subscription start typed in the REPL for the tab `tab_id`.
    fn console_start(tab_id: u64) -> SubscriptionStarted<'static> {
        SubscriptionStarted {
            tab_id,
            statement: "CREATE SUBSCRIPTION live TO orders;",
            origin: SubscriptionOrigin::Console,
        }
    }

    fn subscription_signals(state: SubscriptionTabState) -> WebConsoleSignals {
        let name = SubscriptionName::parse("live").assured("the test subscription name is valid");
        let domain = DomainName::parse("tenant").assured("the test domain name is valid");
        WebConsoleSignals {
            terminal_lines: RwSignal::new(TermLineHistory::default()),
            suggestions: RwSignal::new(Vec::new()),
            suggestion_status: RwSignal::new(None),
            suggestion_query: RwSignal::new(None),
            suggestion_continuation: RwSignal::new(None),
            domain_snapshot: RwSignal::new(None),
            cluster_counters: RwSignal::new(ClusterCounters::default()),
            active_domain: RwSignal::new(Some(domain.clone())),
            clock_display: RwSignal::new(ClockDisplay::selected(Some(domain.clone()), true)),
            transaction_status: RwSignal::new(None),
            inspector: InspectorSignals::new(),
            domains: RwSignal::new(Vec::new()),
            resource_details: RwSignal::new(BTreeMap::new()),
            subscription_tabs: RwSignal::new(vec![SubscriptionTabView {
                id: 1,
                state,
                name,
                domain,
                title: "orders".to_string(),
                subscribe_command: "CREATE SUBSCRIPTION live TO orders;".to_string(),
                lines: TermLineHistory::default(),
            }]),
            active_subscription_tab: RwSignal::new(Some(1)),
            domains_loaded: RwSignal::new(true),
            auth_token: RwSignal::new(None),
            auth_error: RwSignal::new(None),
            session_generation: RwSignal::new(0),
            create: CreateSignals::new(),
            selected_resource: RwSignal::new(None),
            upload_status: RwSignal::new(String::new()),
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

    fn test_inspection() -> TransactionInspection {
        let domain = DomainName::parse("tenant").assured("the test domain name is valid");
        let report = TransactionImpactReport::new(
            domain.clone(),
            TransactionPosition::new(0),
            ImpactPlanningBasis::new([4; 32]),
            ImpactReportCompleteness::Complete,
            Vec::new(),
            Vec::new(),
        )
        .assured("an empty test report has no operation-step inconsistencies");
        let transaction = TransactionStatus::new(
            "attached".to_string(),
            domain,
            TransactionLifecycle::Open,
            TransactionPosition::new(0),
            0,
        )
        .assured("the test transaction has no applied operations");
        TransactionInspection {
            transaction,
            operation: None,
            report,
        }
    }

    #[test]
    fn inspection_reply_sets_the_commit_basis_and_rejection_reports_an_error() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let inspection = test_inspection();
            signals
                .transaction_status
                .set(Some(inspection.transaction.clone()));
            signals.inspector.open_attached();
            let request = ConsoleRequest::InspectTransaction(InspectTransactionRequest {
                target: TransactionInspectionTarget::Attached,
                operation: None,
            });
            assert!(request.inspects_transaction());
            assert!(repl_command("DESCRIBE TRANSACTION;").inspects_transaction());
            assert!(!repl_command("SHOW DOMAINS;").inspects_transaction());
            assert!(request.is_ordered());
            assert!(matches!(
                request.client_request(),
                ClientRequest::InspectTransaction(_)
            ));
            let mut requests = SessionRequests::new();
            let issued = requests.issue(request);
            signals.inspector.requested(issued.order.0);
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: issued,
                    body: ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(
                        inspection.clone(),
                    ))),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(
                signals
                    .inspector
                    .commit_basis(&inspection.transaction)
                    .is_some()
            );

            let rejected = requests.issue(ConsoleRequest::InspectTransaction(
                InspectTransactionRequest {
                    target: TransactionInspectionTarget::Attached,
                    operation: None,
                },
            ));
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: rejected,
                    body: ReplyBody::Inspection(InspectionOutcome::Rejected {
                        rejection: TransactionInspectionRejection::ReportUnavailable,
                        message: "report is unavailable".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(
                signals.inspector.error.get_untracked().as_deref(),
                Some("report is unavailable")
            );

            let invalid = requests.issue(ConsoleRequest::InspectTransaction(
                InspectTransactionRequest {
                    target: TransactionInspectionTarget::Attached,
                    operation: None,
                },
            ));
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: invalid,
                    body: ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                        rejection: nervix_client_wire::RequestRejection::InvalidRequest,
                        field: None,
                        message: "invalid inspection request".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(
                signals.inspector.error.get_untracked().as_deref(),
                Some("invalid inspection request")
            );
        });
    }

    #[test]
    fn describe_outcome_opens_the_inspector_without_rebinding_the_session() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let inspection = test_inspection();
            signals
                .transaction_status
                .set(Some(inspection.transaction.clone()));
            signals
                .inspector
                .prepare_describe(TransactionInspectionTarget::Attached);
            signals.inspector.requested(1);
            let mut outcome = completed_outcome("transaction described");
            outcome.inspection = Some(Box::new(inspection.clone()));
            show_command_outcome(signals, IssueOrder(1), "DESCRIBE TRANSACTION;", outcome);
            assert!(signals.inspector.open.get_untracked());
            assert_eq!(
                signals
                    .inspector
                    .commit_basis(&inspection.transaction)
                    .map(|basis| basis.position),
                Some(inspection.report.position())
            );
        });
    }

    #[test]
    fn stale_preview_outcome_requests_a_fresh_inspection() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let inspection = test_inspection();
            signals
                .transaction_status
                .set(Some(inspection.transaction.clone()));
            signals.inspector.open_attached();
            let preview = TransactionPreviewIdentity {
                transaction_id: inspection.transaction.transaction_id().to_string(),
                position: inspection.report.position(),
                planning_basis: inspection.report.planning_basis(),
            };
            let mut outcome = completed_outcome("preview changed");
            outcome.disposition = CommandDisposition::PreviewStale {
                expected: preview.clone(),
                current: preview,
            };
            show_command_outcome(signals, IssueOrder(1), "COMMIT;", outcome);
            assert!(signals.inspector.stale_preview.get_untracked());
            assert!(
                signals
                    .inspector
                    .error
                    .get_untracked()
                    .is_some_and(|message| message.contains("Refresh"))
            );
            assert_eq!(
                signals.transaction_status.get_untracked(),
                Some(inspection.transaction)
            );
        });
    }

    #[test]
    fn changing_credentials_clears_the_previous_users_private_view() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
            signals.suggestions.set(vec![WireSuggestion {
                value: "private completion".to_string(),
                kind: nervix_client_wire::SuggestionKind::Text,
                edit: TextEdit {
                    start: 0,
                    end: 0,
                    replacement: "private completion".to_string(),
                },
            }]);
            signals
                .terminal_lines
                .update(|lines| lines.push(TermLine::output("private output")));

            signals.clear_authenticated_view();

            assert_eq!(signals.active_domain.get_untracked(), None);
            assert_eq!(signals.transaction_status.get_untracked(), None);
            assert!(signals.domains.get_untracked().is_empty());
            assert!(signals.domain_snapshot.get_untracked().is_none());
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
            (SubscriptionTabState::Ended, "ended", true),
            (SubscriptionTabState::Resubscribing, "resubscribing", true),
            (SubscriptionTabState::Closing(None), "closing", false),
            (SubscriptionTabState::Closing(Some(stream)), "closing", true),
        ];
        for (state, label, can_activate) in cases {
            assert_eq!(state.label(), label);
            assert_eq!(state.can_activate(), can_activate, "state {label}");
            assert_eq!(
                state.can_resubscribe(),
                label == "ended",
                "only an ended tab offers to resubscribe, not a {label} one"
            );
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

            let mut restorations = signals.begin_restorations();
            assert!(
                signals.begin_restorations().is_empty(),
                "one restoration is issued per interrupted tab"
            );
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Restoring)
            }));
            assert_eq!(restorations.len(), 1);
            let ConsoleRequest::SubscriptionStart {
                tab_id,
                request,
                origin,
            } = restorations.remove(0)
            else {
                panic!("the restoration opens the tab's subscription");
            };
            assert_eq!(tab_id, 1);
            assert_eq!(request.statement, "CREATE SUBSCRIPTION live TO orders;");
            assert_eq!(origin, SubscriptionOrigin::Restoration);
            assert!(
                requests.release_held().is_empty(),
                "a restoration is never held"
            );
        });
    }

    #[test]
    fn a_restoration_ends_with_its_connection_and_the_next_connection_restores_the_tab() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Interrupted);
            let mut requests = SessionRequests::new();
            for restoration in signals.begin_restorations() {
                let issued = requests.issue(restoration);
                requests.dispatch(issued);
            }
            let pending_tab = requests.issue(ConsoleRequest::SubscriptionStart {
                tab_id: 2,
                request: SubscribeRequest {
                    domain: domain_name("tenant"),
                    statement: "CREATE SUBSCRIPTION other TO orders;".to_string(),
                    subscription_type: SubscriptionType::Row,
                },
                origin: SubscriptionOrigin::Console,
            });
            requests.dispatch(pending_tab);

            requests.end_connection();
            interrupt_subscription_tabs(signals);
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Interrupted)
            }));
            requests.confirm_leader();
            let replayed = requests.release_held();
            assert_eq!(
                replayed.len(),
                1,
                "a new tab's start is replayed, but the restoration ended with its connection"
            );
            let ClientRequest::Subscribe(replayed) = &replayed[0].request else {
                panic!("the new tab's start is replayed");
            };
            assert_eq!(replayed.statement, "CREATE SUBSCRIPTION other TO orders;");
            assert_eq!(
                signals.begin_restorations().len(),
                1,
                "the next connection restores the tab itself"
            );
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
    fn closing_a_pending_interrupted_or_ended_tab_never_sends_a_stale_delete() {
        for state in [
            SubscriptionTabState::Pending,
            SubscriptionTabState::Restoring,
            SubscriptionTabState::Resubscribing,
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
        for state in [
            SubscriptionTabState::Interrupted,
            SubscriptionTabState::Ended,
        ] {
            Owner::new().with(|| {
                let signals = subscription_signals(state);
                assert!(signals.begin_subscription_close(1).is_none());
                assert!(signals.subscription_tabs.get_untracked().is_empty());
                assert_eq!(signals.active_subscription_tab.get_untracked(), None);
                assert!(signals.begin_subscription_close(1).is_none());
            });
        }
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
    fn domain_clock_frames_are_written_to_the_event_log() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let state = RwSignal::new(ConsoleConnectionState::Connected);
            let mut requests = SessionRequests::new();
            let domain = DomainName::parse("tenant").assured("the test domain name is valid");
            let observed = ServerEvent::DomainClockObserved(DomainClockObserved {
                domain: domain.clone(),
                clock: DomainClockObservation {
                    generation: 2,
                    state: DomainClockObservedState::Stopped,
                },
            });
            let step = apply_event(signals, state, &mut requests, observed);
            assert!(matches!(step, SessionStep::Continue));
            let ended = ServerEvent::DomainClockAttachmentEnded(DomainClockAttachmentEnded {
                domain,
                reason: DomainClockAttachmentEndReason::DomainRemoved,
            });
            let step = apply_event(signals, state, &mut requests, ended);
            assert!(matches!(step, SessionStep::Continue));

            let lines = signals.terminal_lines.get_untracked().into_lines();
            let texts = lines
                .iter()
                .map(|entry| entry.line.text.as_str())
                .collect::<Vec<_>>();
            assert_eq!(
                texts,
                [
                    "domain clock [tenant]: generation 2, stopped",
                    "error: domain clock [tenant]: the attachment ended because the domain no \
                     longer exists on the serving node",
                ]
            );
        });
    }

    #[test]
    fn domain_clock_tick_is_written_to_the_event_log() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let state = RwSignal::new(ConsoleConnectionState::Connected);
            let mut requests = SessionRequests::new();
            let ticked = ServerEvent::DomainClockTicked(DomainClockTicked {
                domain: DomainName::parse("tenant").assured("the test domain name is valid"),
                tick: DomainClockTickObservation {
                    generation: 2,
                    tick_id: 12,
                    logical_boundary: Timestamp::from_unix_nanos(1_000),
                    authority_utc: Timestamp::from_unix_nanos(2_000),
                    serving_logical: Timestamp::from_unix_nanos(3_000),
                },
            });
            let step = apply_event(signals, state, &mut requests, ticked);
            assert!(matches!(step, SessionStep::Continue));
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert_eq!(
                lines[0].line.text,
                "domain clock [tenant] tick: generation 2, id 12, boundary \
                 1970-01-01T00:00:00.000001Z, authority UTC 1970-01-01T00:00:00.000002Z, node \
                 logical 1970-01-01T00:00:00.000003Z"
            );
        });
    }

    #[test]
    fn clock_replies_follow_the_selected_domain_and_manual_detach() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let domain = domain_name("tenant");
            signals.apply_clock_attach_outcome(
                &domain,
                ClockRequestOrigin::Automatic,
                DomainClockAttachOutcome {
                    disposition: DomainClockAttachDisposition::Attached {
                        domain: domain.clone(),
                        clock: DomainClockObservation {
                            generation: 0,
                            state: DomainClockObservedState::Stopped,
                        },
                    },
                    message: "following the clock".to_string(),
                },
            );
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Following { .. }
            ));
            signals.apply_clock_attach_outcome(
                &domain,
                ClockRequestOrigin::Repl,
                DomainClockAttachOutcome {
                    disposition: DomainClockAttachDisposition::AlreadyAttached(domain.clone()),
                    message: "already attached".to_string(),
                },
            );
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Following { .. }
            ));
            signals.apply_clock_detach_outcome(
                &domain,
                ClockRequestOrigin::Repl,
                DomainClockDetachOutcome {
                    disposition: DomainClockDetachDisposition::Detached(domain.clone()),
                    message: "detached".to_string(),
                },
            );
            assert_eq!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Detached(domain.clone())
            );
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert!(
                lines[0]
                    .line
                    .text
                    .contains("domain clock [tenant] attached")
            );
            assert!(lines[1].line.text.contains("already attached"));
            assert!(
                lines[2]
                    .line
                    .text
                    .contains("domain clock [tenant] detached")
            );
        });
    }

    #[test]
    fn typed_clock_replies_update_the_selected_clock_and_event_log() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let domain = domain_name("tenant");
            let mut requests = SessionRequests::new();
            let attach = requests.issue(ConsoleRequest::DomainClockAttach {
                request: AttachDomainClockRequest {
                    domain: domain.clone(),
                },
                connection_generation: 1,
                origin: ClockRequestOrigin::Automatic,
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: attach,
                    body: ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                        disposition: DomainClockAttachDisposition::Attached {
                            domain: domain.clone(),
                            clock: DomainClockObservation {
                                generation: 3,
                                state: DomainClockObservedState::Stopped,
                            },
                        },
                        message: "following".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Following { clock, .. } if clock.generation == 3
            ));

            let detach = requests.issue(ConsoleRequest::DomainClockDetach {
                request: DetachDomainClockRequest {
                    domain: domain.clone(),
                },
                connection_generation: 1,
                origin: ClockRequestOrigin::Repl,
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: detach,
                    body: ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
                        disposition: DomainClockDetachDisposition::Detached(domain.clone()),
                        message: "detached".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Detached(domain)
            );
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert!(lines[0].line.text.contains("attached: following"));
            assert!(lines[1].line.text.contains("detached: detached"));
        });
    }

    #[test]
    fn rejected_clock_requests_report_failure_and_refuse_automatic_following() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let domain = domain_name("tenant");
            let mut requests = SessionRequests::new();
            let attach = requests.issue(ConsoleRequest::DomainClockAttach {
                request: AttachDomainClockRequest {
                    domain: domain.clone(),
                },
                connection_generation: 1,
                origin: ClockRequestOrigin::Automatic,
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: attach,
                    body: ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                        rejection: nervix_client_wire::RequestRejection::InvalidRequest,
                        field: None,
                        message: "attachment refused".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Refused { domain: selected, reason }
                    if selected == domain && reason == "attachment refused"
            ));

            let detach = requests.issue(ConsoleRequest::DomainClockDetach {
                request: DetachDomainClockRequest {
                    domain: domain.clone(),
                },
                connection_generation: 1,
                origin: ClockRequestOrigin::Automatic,
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: detach,
                    body: ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                        rejection: nervix_client_wire::RequestRejection::InvalidRequest,
                        field: None,
                        message: "detachment refused".to_string(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Refused { .. }
            ));
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert!(
                lines[0]
                    .line
                    .text
                    .contains("attach failed: attachment refused")
            );
            assert!(
                lines[1]
                    .line
                    .text
                    .contains("detach failed: detachment refused")
            );
        });
    }

    #[test]
    fn automatic_clock_transition_orders_requests_and_reports_a_closed_queue() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let previous = domain_name("tenant");
            let selected = domain_name("other");
            signals
                .clock_display
                .set(ClockDisplay::selected(Some(selected.clone()), true));
            let (sender, mut receiver) = request_handoff();
            signals.queue_clock_transition(
                ClockSelectionChange {
                    detach: Some(previous.clone()),
                    attach: Some(selected.clone()),
                    refresh_display: true,
                },
                7,
                &sender,
            );
            let ConsoleRequest::DomainClockDetach {
                request,
                connection_generation,
                origin,
            } = receiver
                .try_take()
                .assured("the transition queues a detach before its attach")
            else {
                panic!("the first clock request detaches the previous domain");
            };
            assert_eq!(request.domain, previous);
            assert_eq!(connection_generation, 7);
            assert!(origin == ClockRequestOrigin::Automatic);
            let ConsoleRequest::DomainClockAttach {
                request,
                connection_generation,
                origin,
            } = receiver
                .try_take()
                .assured("the transition queues the selected clock after detaching")
            else {
                panic!("the second clock request attaches the selected domain");
            };
            assert_eq!(request.domain, selected);
            assert_eq!(connection_generation, 7);
            assert!(origin == ClockRequestOrigin::Automatic);
            assert!(receiver.try_take().is_none());

            drop(receiver);
            signals.queue_clock_transition(
                ClockSelectionChange {
                    detach: Some(previous),
                    attach: Some(selected.clone()),
                    refresh_display: true,
                },
                7,
                &sender,
            );
            assert!(matches!(
                signals.clock_display.get_untracked(),
                ClockDisplay::Refused { domain, .. } if domain == selected
            ));
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert!(lines[0].line.text.contains("detach could not be queued"));
            assert!(lines[1].line.text.contains("attach could not be queued"));
        });
    }

    #[test]
    fn clock_requests_only_enter_an_open_connected_session() {
        Owner::new().with(|| {
            let (sender, mut receiver) = request_handoff();
            let state = RwSignal::new(ConsoleConnectionState::Connected);
            let request_tx = RwSignal::new(Some(sender));
            let session = WebConsoleSession {
                state,
                request_tx,
                upload_base_url: RwSignal::new(None),
                auth_token: RwSignal::new(None),
            };
            assert!(matches!(
                session.send_when_connected(ConsoleRequest::ListDomains),
                Ok(true)
            ));
            assert!(matches!(
                receiver.try_take(),
                Some(ConsoleRequest::ListDomains)
            ));
            for _ in 0..request_handoff::MAX_WAITING_REQUESTS {
                assert!(matches!(
                    session.send_when_connected(ConsoleRequest::ListDomains),
                    Ok(true)
                ));
            }
            let Err(refusal) = session.send_when_connected(ConsoleRequest::ListDomains) else {
                panic!("a full hand-off refuses another request");
            };
            assert_eq!(
                refusal.current_context(),
                &RequestRefusal::TooManyWaiting {
                    limit: request_handoff::MAX_WAITING_REQUESTS
                }
            );
            receiver.discard_waiting();
            state.set(ConsoleConnectionState::Waiting);
            assert!(matches!(
                session.send_when_connected(ConsoleRequest::ListDomains),
                Ok(false)
            ));
            state.set(ConsoleConnectionState::Connected);
            request_tx.set(None);
            assert!(matches!(
                session.send_when_connected(ConsoleRequest::ListDomains),
                Ok(false)
            ));
        });
    }

    #[test]
    fn automatic_detach_already_released_by_server_is_informational() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let domain = domain_name("tenant");
            signals.apply_clock_detach_outcome(
                &domain,
                ClockRequestOrigin::Automatic,
                DomainClockDetachOutcome {
                    disposition: DomainClockDetachDisposition::NotAttached(domain.clone()),
                    message: "not attached".to_string(),
                },
            );
            assert_eq!(
                signals.clock_display.get_untracked(),
                ClockDisplay::selected(Some(domain), true)
            );
            let lines = signals.terminal_lines.get_untracked().into_lines();
            assert!(lines[0].line.text.contains("was already detached"));
        });
    }

    #[test]
    fn clock_requests_are_connection_scoped_even_while_waiting_for_the_leader() {
        let domain = domain_name("tenant");
        let attach = ConsoleRequest::DomainClockAttach {
            request: AttachDomainClockRequest {
                domain: domain.clone(),
            },
            connection_generation: 5,
            origin: ClockRequestOrigin::Automatic,
        };
        assert!(attach.is_ordered());
        assert!(attach.belongs_to_connection_generation(5));
        assert!(!attach.belongs_to_connection_generation(6));
        assert!(matches!(
            attach.client_request(),
            ClientRequest::AttachDomainClock(_)
        ));
        let mut requests = SessionRequests::new();
        let issued = requests.issue(attach);
        assert!(matches!(requests.accept(issued), Admission::Held));
        assert_eq!(requests.held.len(), 1);
        requests.end_connection();
        assert!(requests.held.is_empty());

        let detach = ConsoleRequest::DomainClockDetach {
            request: DetachDomainClockRequest { domain },
            connection_generation: 5,
            origin: ClockRequestOrigin::Automatic,
        };
        assert!(matches!(
            detach.client_request(),
            ClientRequest::DetachDomainClock(_)
        ));
        let issued = requests.issue(detach);
        requests.dispatch(issued);
        requests.end_connection();
        assert!(requests.held.is_empty());
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
                console_start(1),
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
                console_start(1),
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
                console_start(1),
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
                console_start(1),
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
    fn subscription_replies_follow_the_request_that_opened_and_closed_the_tab() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            signals.active_subscription_tab.set(None);
            let mut requests = SessionRequests::new();
            let stream = test_stream();
            let domain = DomainName::parse("tenant").assured("the test domain is valid");
            let start = requests.issue(ConsoleRequest::SubscriptionStart {
                tab_id: 1,
                request: SubscribeRequest {
                    domain: domain.clone(),
                    statement: "CREATE SUBSCRIPTION live TO orders;".to_string(),
                    subscription_type: SubscriptionType::Row,
                },
                origin: SubscriptionOrigin::Console,
            });
            let opened = SubscribeOutcome {
                disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                    subscription: stream.subscription.clone(),
                    domain,
                    relay: nervix_models::RelayName::parse("orders")
                        .assured("the test relay is valid"),
                    subscription_type: SubscriptionType::Row,
                    schema: stream.schema,
                })),
                message: String::new(),
                diagnostics: Vec::new(),
            };
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: start,
                    body: ReplyBody::Subscribe(opened),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(signals.active_subscription_tab.get_untracked(), Some(1));

            let stop_request = signals
                .begin_subscription_close(1)
                .assured("the acknowledged tab has a stream to close");
            let stop = requests.issue(ConsoleRequest::SubscriptionStop {
                tab_id: 1,
                request: stop_request,
            });
            let deleted = UnsubscribeOutcome {
                disposition: UnsubscribeDisposition::Deleted(stream.subscription),
                message: String::new(),
                diagnostics: Vec::new(),
            };
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: stop,
                    body: ReplyBody::Unsubscribe(deleted),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);
        });
    }

    #[test]
    fn a_late_open_reply_cannot_replace_an_already_open_stream() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let mut requests = SessionRequests::new();
            let late = SubscriptionHandle {
                name: stream.subscription.name.clone(),
                generation: NonZeroU64::new(2).assured("two is nonzero"),
            };
            apply_subscribe_outcome(
                signals,
                &mut requests,
                console_start(1),
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                        subscription: late.clone(),
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
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(tabs[0].streams(&stream.subscription));
                assert!(!tabs[0].streams(&late));
            });
            assert!(requests.release_held().is_empty());
        });
    }

    #[test]
    fn rejected_subscribe_reply_removes_a_new_tab_but_keeps_a_desired_restore() {
        for restoring in [false, true] {
            Owner::new().with(|| {
                let state = if restoring {
                    SubscriptionTabState::Restoring
                } else {
                    SubscriptionTabState::Pending
                };
                let signals = subscription_signals(state);
                let mut requests = SessionRequests::new();
                let issued = requests.issue(ConsoleRequest::SubscriptionStart {
                    tab_id: 1,
                    request: SubscribeRequest {
                        domain: DomainName::parse("tenant").assured("the test domain is valid"),
                        statement: "CREATE SUBSCRIPTION live TO orders;".to_string(),
                        subscription_type: SubscriptionType::Row,
                    },
                    origin: SubscriptionOrigin::Console,
                });
                let step = apply_reply(
                    signals,
                    &mut requests,
                    AnsweredRequest {
                        request: issued,
                        body: ReplyBody::Rejected(nervix_client_wire::RequestRejected {
                            rejection: nervix_client_wire::RequestRejection::InvalidRequest,
                            field: None,
                            message: "subscription rejected".to_string(),
                        }),
                    },
                );
                assert!(matches!(step, SessionStep::Continue));
                if restoring {
                    signals.subscription_tabs.with_untracked(|tabs| {
                        assert!(matches!(&tabs[0].state, SubscriptionTabState::Interrupted));
                        assert!(
                            tabs[0].lines.clone().into_lines()[0]
                                .line
                                .text
                                .contains("subscription rejected")
                        );
                    });
                } else {
                    assert!(signals.subscription_tabs.get_untracked().is_empty());
                    assert_eq!(signals.active_subscription_tab.get_untracked(), None);
                }
            });
        }
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

    /// The notice with which the server ends the generation `subscription`.
    fn subscription_ended(subscription: SubscriptionHandle, message: &str) -> ServerEvent {
        ServerEvent::SubscriptionEnded(SubscriptionEnded {
            subscription,
            reason: SubscriptionEndReason::RelayChanged,
            message: message.to_string(),
        })
    }

    #[test]
    fn a_generation_the_server_ended_ends_its_tab_which_keeps_its_rows_and_is_not_restored() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let state = RwSignal::new(ConsoleConnectionState::Connected);
            let mut requests = SessionRequests::new();
            signals.subscription_tabs.update(|tabs| {
                tabs[0].lines.push(TermLine::output("{\"id\":1}"));
            });

            let earlier = SubscriptionHandle {
                name: stream.subscription.name.clone(),
                generation: NonZeroU64::new(2).assured("two is nonzero"),
            };
            let step = apply_event(
                signals,
                state,
                &mut requests,
                subscription_ended(earlier, "another generation ended"),
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(
                signals
                    .subscription_tabs
                    .with_untracked(|tabs| { tabs[0].streams(&stream.subscription) })
            );

            apply_event(
                signals,
                state,
                &mut requests,
                subscription_ended(
                    stream.subscription.clone(),
                    "session subscription 'live' ended because relay 'orders' was redefined",
                ),
            );
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Ended));
                assert!(!tabs[0].streams(&stream.subscription));
                let texts = tabs[0]
                    .lines
                    .clone()
                    .into_lines()
                    .into_iter()
                    .map(|entry| entry.line.text)
                    .collect::<Vec<_>>();
                assert_eq!(
                    texts,
                    [
                        "{\"id\":1}",
                        "error: session subscription 'live' ended because relay 'orders' was \
                         redefined",
                    ]
                );
            });
            assert_eq!(signals.active_subscription_tab.get_untracked(), Some(1));

            interrupt_subscription_tabs(signals);
            assert!(
                signals.begin_restorations().is_empty(),
                "a new connection does not change why the server ended the generation"
            );
            assert!(
                signals.subscription_tabs.with_untracked(|tabs| {
                    matches!(&tabs[0].state, SubscriptionTabState::Ended)
                })
            );
        });
    }

    #[test]
    fn an_ending_that_reaches_a_closing_tab_removes_it() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Closing(Some(stream.clone())));
            let request = UnsubscribeRequest {
                subscription: stream.subscription.name.clone(),
            };
            signals.end_subscription(&SubscriptionEnded {
                subscription: stream.subscription.clone(),
                reason: SubscriptionEndReason::RelayRemoved,
                message: "relay 'orders' no longer exists".to_string(),
            });
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert_eq!(signals.active_subscription_tab.get_untracked(), None);

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
            assert!(
                signals.subscription_tabs.get_untracked().is_empty(),
                "the late deletion reply finds no tab to change"
            );
        });
    }

    #[test]
    fn resubscribing_an_ended_tab_reopens_it_or_leaves_it_ended_with_the_refusal() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Ended);
            signals.active_subscription_tab.set(None);
            let mut requests = SessionRequests::new();

            let request = signals
                .begin_resubscribe(1)
                .assured("an ended tab can be resubscribed");
            assert_eq!(request.domain, domain_name("tenant"));
            assert_eq!(request.statement, "CREATE SUBSCRIPTION live TO orders;");
            assert_eq!(request.subscription_type, SubscriptionType::Row);
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Resubscribing)
            }));
            assert!(
                signals.begin_resubscribe(1).is_none(),
                "a resubscription in flight is not sent twice"
            );

            apply_subscribe_outcome(
                signals,
                &mut requests,
                console_start(1),
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Failed,
                    message: "stream 'orders' does not exist in domain 'tenant'".to_string(),
                    diagnostics: Vec::new(),
                },
            );
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Ended));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains("does not exist in domain 'tenant'")
                );
            });

            signals
                .begin_resubscribe(1)
                .assured("a refused resubscription leaves the tab ended and resubscribable");
            let reopened = SubscriptionHandle {
                name: SubscriptionName::parse("live").assured("the test name is valid"),
                generation: NonZeroU64::new(3).assured("three is nonzero"),
            };
            apply_subscribe_outcome(
                signals,
                &mut requests,
                console_start(1),
                SubscribeOutcome {
                    disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                        subscription: reopened.clone(),
                        domain: domain_name("tenant"),
                        relay: RelayName::parse("orders").assured("the test relay is valid"),
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
            assert!(
                signals
                    .subscription_tabs
                    .with_untracked(|tabs| tabs[0].streams(&reopened))
            );
            assert_eq!(signals.active_subscription_tab.get_untracked(), Some(1));
        });
    }

    #[test]
    fn resubscribing_from_the_tab_sends_its_statement_or_leaves_it_ended_with_the_refusal() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Ended);
            let (sender, mut receiver) = request_handoff();
            let request_tx = RwSignal::new(Some(sender));
            resubscribe_tab(signals, request_tx, 1);
            let Some(ConsoleRequest::SubscriptionStart {
                tab_id,
                request,
                origin,
            }) = receiver.try_take()
            else {
                panic!("resubscribing hands the tab's start to the session");
            };
            assert_eq!(tab_id, 1);
            assert_eq!(request.statement, "CREATE SUBSCRIPTION live TO orders;");
            assert_eq!(origin, SubscriptionOrigin::Console);
            resubscribe_tab(signals, request_tx, 1);
            resubscribe_tab(signals, request_tx, 7);
            assert!(
                receiver.try_take().is_none(),
                "only an ended tab that exists is resubscribed"
            );
        });
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Ended);
            resubscribe_tab(signals, RwSignal::new(None), 1);
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Ended));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains(SESSION_UNAVAILABLE)
                );
            });
        });
    }

    #[test]
    fn closing_an_open_tab_deletes_its_subscription_or_keeps_it_open_with_the_refusal() {
        Owner::new().with(|| {
            let stream = test_stream();
            let signals = subscription_signals(SubscriptionTabState::Open(stream.clone()));
            let (sender, mut receiver) = request_handoff();
            close_subscription_tab(signals, RwSignal::new(Some(sender)), 1);
            assert!(matches!(
                receiver.try_take(),
                Some(ConsoleRequest::SubscriptionStop { tab_id: 1, .. })
            ));
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Closing(Some(_)))
            }));
        });
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Open(test_stream()));
            close_subscription_tab(signals, RwSignal::new(None), 1);
            signals.subscription_tabs.with_untracked(|tabs| {
                assert!(matches!(&tabs[0].state, SubscriptionTabState::Open(_)));
                assert!(
                    tabs[0].lines.clone().into_lines()[0]
                        .line
                        .text
                        .contains(SESSION_UNAVAILABLE)
                );
            });
        });
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Ended);
            let (sender, mut receiver) = request_handoff();
            close_subscription_tab(signals, RwSignal::new(Some(sender)), 1);
            assert!(signals.subscription_tabs.get_untracked().is_empty());
            assert!(
                receiver.try_take().is_none(),
                "an ended tab closes without a deletion"
            );
        });
    }

    #[test]
    fn a_subscription_start_reaches_the_session_or_fails_its_tab_with_the_refusal() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let request = SubscribeRequest {
                domain: domain_name("tenant"),
                statement: "CREATE SUBSCRIPTION live TO orders;".to_string(),
                subscription_type: SubscriptionType::Row,
            };
            let (sender, mut receiver) = request_handoff();
            send_subscription_start(
                signals,
                RwSignal::new(Some(sender.clone())),
                1,
                request.clone(),
                SubscriptionOrigin::Console,
            );
            assert!(matches!(
                receiver.try_take(),
                Some(ConsoleRequest::SubscriptionStart { tab_id: 1, .. })
            ));
            assert!(signals.subscription_tabs.with_untracked(|tabs| {
                matches!(&tabs[0].state, SubscriptionTabState::Pending)
            }));

            drop(receiver);
            send_subscription_start(
                signals,
                RwSignal::new(Some(sender)),
                1,
                request,
                SubscriptionOrigin::Console,
            );
            assert!(
                signals.subscription_tabs.get_untracked().is_empty(),
                "a new tab whose start was refused is removed"
            );
            assert_eq!(
                signals.terminal_lines.get_untracked().into_lines()[0]
                    .line
                    .text,
                "error: websocket command channel is closed"
            );
        });
    }

    #[test]
    fn a_resource_description_the_console_cannot_hand_over_shows_why_in_its_dialog() {
        Owner::new().with(|| {
            let details = RwSignal::new(BTreeMap::<String, ResourceDetailView>::new());
            let (sender, mut receiver) = request_handoff();
            request_resource_describe(
                RwSignal::new(Some(sender.clone())),
                details,
                "live".to_string(),
                Some(domain_name("tenant")),
            );
            assert!(matches!(
                receiver.try_take(),
                Some(ConsoleRequest::Command {
                    purpose: CommandPurpose::ResourceDescription { .. },
                    ..
                })
            ));
            assert!(details.get_untracked().is_empty());

            drop(receiver);
            request_resource_describe(
                RwSignal::new(Some(sender)),
                details,
                "bundle".to_string(),
                Some(domain_name("tenant")),
            );
            request_resource_describe(RwSignal::new(None), details, "other".to_string(), None);
            let details = details.get_untracked();
            assert_eq!(
                details["bundle"].status,
                "websocket command channel is closed"
            );
            assert_eq!(details["other"].status, SESSION_UNAVAILABLE);
        });
    }

    /// A REPL command issued in the session's transaction, at its first accepted position.
    fn transaction_command(query: &str) -> ConsoleRequest {
        let ConsoleRequest::Command {
            mut request,
            purpose,
        } = repl_command(query)
        else {
            panic!("the test builds a command");
        };
        request.expected_transaction_position = Some(TransactionPosition::new(1));
        ConsoleRequest::Command { request, purpose }
    }

    /// The resource description the console requests on its own, outside any transaction.
    fn resource_description(resource: &str) -> ConsoleRequest {
        ConsoleRequest::Command {
            request: CommandRequest {
                query: format!("DESCRIBE RESOURCE {resource};"),
                domain: Some(domain_name("tenant")),
                execution_reference: command_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::ResourceDescription {
                resource: resource.to_string(),
            },
        }
    }

    fn reverted_transaction() -> TransactionStatus {
        TransactionStatus::new(
            "transaction".to_string(),
            domain_name("tenant"),
            TransactionLifecycle::Reverted,
            TransactionPosition::new(1),
            0,
        )
        .assured("no operation of the reverted test transaction applied")
    }

    #[test]
    fn a_finished_or_failed_attach_releases_commands_under_their_original_references() {
        let attach_endings: [fn(WebConsoleSignals, &mut SessionRequests); 4] = [
            |signals, requests| {
                let attach = requests
                    .answer(request_id(1))
                    .verified("the attach was dispatched above");
                let step = apply_reply(
                    signals,
                    requests,
                    AnsweredRequest {
                        request: attach,
                        body: ReplyBody::Attach(AttachOutcome {
                            disposition: AttachDisposition::AlreadyFinished(reverted_transaction()),
                            message: "transaction already finished".to_string(),
                            diagnostics: Vec::new(),
                        }),
                    },
                );
                assert!(matches!(step, SessionStep::Continue));
            },
            |signals, _requests| {
                apply_attach_outcome(
                    signals,
                    AttachOutcome {
                        disposition: AttachDisposition::Attached(reverted_transaction()),
                        message: "transaction reverted".to_string(),
                        diagnostics: Vec::new(),
                    },
                );
            },
            |signals, _requests| {
                apply_attach_outcome(
                    signals,
                    AttachOutcome {
                        disposition: AttachDisposition::Failed,
                        message: "transaction is not retained".to_string(),
                        diagnostics: Vec::new(),
                    },
                );
            },
            |signals, _requests| {
                let attach = ConsoleRequest::AttachTransaction(AttachTransactionRequest {
                    transaction_id: "transaction".to_string(),
                });
                fail_request(signals, attach, "the attach was rejected".to_string());
            },
        ];
        for end_attach in attach_endings {
            Owner::new().with(|| {
                let signals = subscription_signals(SubscriptionTabState::Pending);
                signals.transaction_status.set(Some(
                    TransactionStatus::new(
                        "transaction".to_string(),
                        domain_name("tenant"),
                        TransactionLifecycle::Open,
                        TransactionPosition::new(1),
                        0,
                    )
                    .assured("no operation of the open test transaction applied"),
                ));
                let mut requests = SessionRequests::new();
                let attach = requests.issue(ConsoleRequest::AttachTransaction(
                    AttachTransactionRequest {
                        transaction_id: "transaction".to_string(),
                    },
                ));
                requests.dispatch(attach);
                requests.confirm_leader();
                let new_tab = requests.issue(ConsoleRequest::SubscriptionStart {
                    tab_id: 1,
                    request: SubscribeRequest {
                        domain: domain_name("tenant"),
                        statement: "CREATE SUBSCRIPTION live TO orders;".to_string(),
                        subscription_type: SubscriptionType::Row,
                    },
                    origin: SubscriptionOrigin::Console,
                });
                assert!(matches!(requests.accept(new_tab), Admission::Held));
                let in_transaction = requests.issue(transaction_command("REVERT;"));
                let ConsoleRequest::Command { request, .. } = &in_transaction.request else {
                    panic!("the test issued a transaction command");
                };
                let reference = request.execution_reference.clone();
                assert!(matches!(requests.accept(in_transaction), Admission::Held));
                let outside = requests.issue(resource_description("bundle"));
                assert!(matches!(requests.accept(outside), Admission::Held));

                end_attach(signals, &mut requests);
                requests.answer(request_id(1));
                let released = requests.release_held();
                assert_eq!(released.len(), 3);
                let ClientRequest::Subscribe(start) = &released[0].request else {
                    panic!("the tab's start goes out first, in its place");
                };
                assert_eq!(start.statement, "CREATE SUBSCRIPTION live TO orders;");
                assert_eq!(sent_queries(&released[1..2]), vec!["REVERT;"]);
                assert_eq!(
                    sent_commands(&released[1..2])[0].execution_reference,
                    reference
                );
                assert_eq!(
                    sent_queries(&released[2..]),
                    vec!["DESCRIBE RESOURCE bundle;"]
                );
            });
        }
    }

    #[test]
    fn the_session_refuses_requests_past_its_outstanding_bounds_and_reports_them() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests = SessionRequests::new();
            requests.confirm_leader();
            for _ in 0..MAX_OUTSTANDING_REQUESTS {
                let issued = requests.issue(repl_command("SHOW CLUSTER STATUS;"));
                assert!(!matches!(requests.accept(issued), Admission::Refused(_)));
            }
            assert_eq!(requests.in_flight.len(), MAX_IN_FLIGHT_REQUESTS);

            let issued = requests.issue(repl_command("LIST DOMAINS;"));
            let Admission::Refused(refused) = requests.accept(issued) else {
                panic!("a request past the outstanding bound is refused");
            };
            assert_eq!(
                refused.refusal,
                RequestRefusal::TooManyOutstanding {
                    limit: MAX_OUTSTANDING_REQUESTS
                }
            );
            let RefusedRequest { request, refusal } = *refused;
            fail_request(signals, request, refusal.to_string());
            assert_eq!(
                signals.terminal_lines.get_untracked().into_lines()[0]
                    .line
                    .text,
                "error: 256 requests are already outstanding in the console's session; this one \
                 was not sent"
            );

            let completion = requests.issue(suggest("SH"));
            let Admission::Refused(refused) = requests.accept(completion) else {
                panic!("a completion request past the outstanding bound is refused");
            };
            let RefusedRequest { request, refusal } = *refused;
            fail_request(signals, request, refusal.to_string());
            assert!(signals.suggestions.get_untracked().is_empty());
            assert_eq!(
                signals.suggestion_status.get_untracked(),
                Some(SuggestionStatus::LookupFailed),
                "a refused completion request is not shown as having no matches"
            );

            let first = *requests
                .in_flight
                .keys()
                .next()
                .assured("the session has requests in flight");
            assert!(requests.answer(first).is_some());
            assert_eq!(
                requests.release_held().len(),
                1,
                "a reply frees a place for the next held request"
            );
            let issued = requests.issue(repl_command("LIST DOMAINS;"));
            assert!(
                matches!(requests.accept(issued), Admission::Held),
                "a request issued later waits behind the held ones"
            );
        });
    }

    #[test]
    fn ordered_requests_beyond_the_in_flight_limit_wait_in_order_for_earlier_replies() {
        let mut requests = SessionRequests::new();
        requests.confirm_leader();
        let mut sent_ids = Vec::new();
        for index in 0..MAX_IN_FLIGHT_REQUESTS {
            let issued = requests.issue(repl_command(&format!("SHOW {index};")));
            sent_ids.push(sent(requests.accept(issued)).request_id);
        }
        for query in ["first to wait", "second to wait"] {
            let issued = requests.issue(repl_command(query));
            assert!(
                matches!(requests.accept(issued), Admission::Held),
                "the server admits no more requests in flight"
            );
        }
        assert!(requests.release_held().is_empty());

        assert!(requests.answer(sent_ids[0]).is_some());
        assert_eq!(
            sent_queries(&requests.release_held()),
            vec!["first to wait"]
        );
        assert!(requests.answer(sent_ids[1]).is_some());
        assert_eq!(
            sent_queries(&requests.release_held()),
            vec!["second to wait"]
        );
        assert_eq!(requests.in_flight.len(), MAX_IN_FLIGHT_REQUESTS);
    }

    #[test]
    fn the_session_bounds_the_text_its_outstanding_requests_carry() {
        let mut requests = SessionRequests::new();
        let largest = requests.issue(repl_command(&"a".repeat(MAX_OUTSTANDING_REQUEST_BYTES)));
        assert!(matches!(requests.accept(largest), Admission::Held));
        let issued = requests.issue(repl_command("b"));
        let Admission::Refused(refused) = requests.accept(issued) else {
            panic!("text past the outstanding bound is refused");
        };
        assert_eq!(
            refused.refusal,
            RequestRefusal::TooMuchOutstandingText {
                limit: MAX_OUTSTANDING_REQUEST_BYTES
            }
        );
        let issued = requests.issue(ConsoleRequest::ListDomains);
        assert!(
            matches!(requests.accept(issued), Admission::Held),
            "a request that carries no text still fits"
        );
    }

    /// The first part of a completion reply to `request_id` too large for one frame.
    fn first_suggestion_part(request_id: RequestId) -> ServerMessage {
        let small_frames = SessionLimits::try_from(SessionLimitSettings {
            frame_bytes: std::num::NonZeroUsize::new(1024).assured("a literal nonzero size"),
            transfer_bytes: std::num::NonZeroUsize::new(SESSION_LIMITS.transfer_bytes())
                .assured("the default transfer limit is nonzero"),
            nesting_depth: std::num::NonZeroUsize::new(SESSION_LIMITS.nesting_depth())
                .assured("the default nesting limit is nonzero"),
            collection_entries: std::num::NonZeroUsize::new(SESSION_LIMITS.collection_entries())
                .assured("the default collection limit is nonzero"),
            string_bytes: std::num::NonZeroUsize::new(SESSION_LIMITS.string_bytes())
                .assured("the default string limit is nonzero"),
        })
        .assured("a 1 KiB frame limit is the smallest a session admits");
        let long = "SHOW ".repeat(1024);
        let reply = Reply {
            request_id,
            body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                status: SuggestionStatus::Ready,
                continuation: None,
                suggestions: vec![WireSuggestion {
                    value: long.clone(),
                    kind: SuggestionKind::Text,
                    edit: TextEdit {
                        start: 0,
                        end: 0,
                        replacement: long,
                    },
                }],
            }),
        };
        let ReplyDelivery::Transfer(mut parts) = reply
            .encode(&small_frames)
            .assured("the reply stays within the transfer limit")
        else {
            panic!("a reply larger than a frame travels as parts");
        };
        let first = parts
            .next()
            .assured("a transfer has a first part")
            .verify(&small_frames)
            .assured("an encoded part verifies under the limits it was encoded for");
        ServerMessage::decode(&first).assured("a verified part decodes")
    }

    #[test]
    fn a_superseded_completion_request_drops_its_partial_reply() {
        let mut requests = SessionRequests::new();
        let earlier = requests.issue(suggest("SH"));
        let earlier = sent(requests.accept(earlier));
        let part = first_suggestion_part(earlier.request_id);
        assert!(matches!(requests.route(part), Routed::Pending));
        assert!(requests.transfers.contains_key(&earlier.request_id));

        let later = requests.issue(suggest("SHOW"));
        sent(requests.accept(later));
        assert!(
            requests.transfers.is_empty(),
            "the parts of a reply nobody awaits are not kept"
        );
    }

    #[test]
    fn command_history_keeps_the_latest_commands_and_reports_where_it_omitted_earlier_ones() {
        let mut history = CommandHistory::default();
        let issued = MAX_COMMAND_HISTORY_RECORDS + 10;
        for index in 0..issued {
            assert_eq!(history.push(&format!("SHOW {index};")), HistoryPush::Kept);
        }
        let mut recalled = Vec::new();
        let mut omission_reports = 0;
        for _ in 0..MAX_COMMAND_HISTORY_RECORDS {
            let step = history
                .previous("draft".to_string())
                .assured("the history keeps commands to walk back through");
            if step.reached_omission {
                omission_reports += 1;
            }
            recalled.push(step.command);
        }
        assert_eq!(recalled[0], format!("SHOW {};", issued - 1));
        assert_eq!(recalled[MAX_COMMAND_HISTORY_RECORDS - 1], "SHOW 10;");
        assert_eq!(
            omission_reports, 1,
            "the walk reports the omission once, where it reaches the oldest command kept"
        );

        let stay = history
            .previous("draft".to_string())
            .assured("stepping back from the oldest command stays on it");
        assert_eq!(stay.command, "SHOW 10;");
        assert!(!stay.reached_omission);
        assert_eq!(history.next().as_deref(), Some("SHOW 11;"));
        for _ in 0..(MAX_COMMAND_HISTORY_RECORDS - 2) {
            history.next();
        }
        assert_eq!(
            history.next().as_deref(),
            Some("draft"),
            "walking forward past the newest command restores the draft"
        );
    }

    #[test]
    fn command_history_bounds_its_bytes_and_cannot_keep_a_command_larger_than_itself() {
        let mut history = CommandHistory::default();
        let half = MAX_COMMAND_HISTORY_BYTES / 2;
        let first = "a".repeat(half);
        let second = "b".repeat(half);
        assert_eq!(history.push(&first), HistoryPush::Kept);
        assert_eq!(history.push(&second), HistoryPush::Kept);
        assert_eq!(
            history.push(&second),
            HistoryPush::Kept,
            "a repeat adds nothing"
        );
        assert_eq!(
            history.push("   "),
            HistoryPush::Kept,
            "an empty command adds nothing"
        );
        assert_eq!(history.entries.len(), 2);
        assert!(!history.omitted);

        assert_eq!(history.push("c"), HistoryPush::Kept);
        assert_eq!(
            history.entries.len(),
            2,
            "the oldest command gave way to the new one"
        );
        assert_eq!(history.bytes, half + 1);
        assert_eq!(
            history.push(&"x".repeat(MAX_COMMAND_HISTORY_BYTES + 1)),
            HistoryPush::TooLarge
        );
        assert_eq!(
            history.entries.len(),
            2,
            "a command too large to keep evicts nothing"
        );

        let newest = history
            .previous(String::new())
            .assured("the history keeps two commands");
        assert_eq!(newest.command, "c");
        assert!(!newest.reached_omission);
        let oldest = history
            .previous(String::new())
            .assured("the history keeps two commands");
        assert_eq!(oldest.command, second);
        assert!(oldest.reached_omission);
        assert!(CommandHistory::default().previous(String::new()).is_none());
    }

    /// A snapshot of `domain` as the server pushes it, whose graph is named after the domain.
    fn observed_snapshot(domain: &str) -> ServerEvent {
        let graph = DataflowGraph::new(domain)
            .serialize()
            .assured("an empty graph serializes");
        let graph_json = String::from_utf8(graph).assured("a serialized graph is JSON text");
        let frame = DomainSnapshotObserved::encode(
            &domain_name(domain),
            &graph_json,
            &[DomainEntity::Resource {
                name: ResourceName::parse(&format!("{domain}_bundle"))
                    .assured("the test names a valid resource"),
                latest_version: None,
            }],
            &SESSION_LIMITS,
        )
        .assured("a small snapshot fits a frame")
        .verify(&SESSION_LIMITS)
        .assured("an encoded snapshot verifies");
        let ServerMessage::Event(event) =
            ServerMessage::decode(&frame).assured("a verified snapshot decodes")
        else {
            panic!("a snapshot is an event");
        };
        event
    }

    #[test]
    fn the_console_keeps_only_the_snapshot_of_the_domain_it_observes() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let state = RwSignal::new(ConsoleConnectionState::Connected);
            let mut requests = SessionRequests::new();
            let retained_resource = || match signals.domain_snapshot.get_untracked() {
                Some(snapshot) => (
                    snapshot.domain.to_string(),
                    snapshot.entities[0].name.clone(),
                ),
                None => panic!("the console retains a snapshot"),
            };

            apply_event(signals, state, &mut requests, observed_snapshot("tenant"));
            assert_eq!(
                retained_resource(),
                ("tenant".to_string(), "tenant_bundle".to_string())
            );

            apply_event(signals, state, &mut requests, observed_snapshot("other"));
            assert_eq!(
                retained_resource(),
                ("tenant".to_string(), "tenant_bundle".to_string()),
                "a snapshot of a domain the console does not show is dropped"
            );

            signals.active_domain.set(Some(domain_name("other")));
            apply_event(signals, state, &mut requests, observed_snapshot("other"));
            assert_eq!(
                retained_resource(),
                ("other".to_string(), "other_bundle".to_string()),
                "the newly observed domain's snapshot replaces the previous one"
            );
        });
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
    fn a_redirect_moves_to_the_leaders_console_only_when_the_leader_advertises_one() {
        let unknown = LeaderRedirect { leader: None };
        assert_eq!(redirect_console(&unknown), None);
        assert_eq!(
            leader_redirect_line(&unknown).text,
            "topology: not-a-leader"
        );

        let node = ClusterNodeName::parse("node-2").assured("a literal node name");
        let unadvertised = LeaderRedirect {
            leader: Some(nervix_client_wire::LeaderEndpoints {
                node: node.clone(),
                grpc_uri: None,
                web_console_uri: None,
            }),
        };
        assert_eq!(redirect_console(&unadvertised), None);
        assert_eq!(
            leader_redirect_line(&unadvertised).text,
            "topology: not-a-leader, retry on leader 'node-2'"
        );

        let console = Url::parse("http://node-2:17420/console/").assured("a literal URL");
        let advertised = LeaderRedirect {
            leader: Some(nervix_client_wire::LeaderEndpoints {
                node,
                grpc_uri: None,
                web_console_uri: Some(console.clone()),
            }),
        };
        assert_eq!(redirect_console(&advertised), Some(&console));
    }

    #[test]
    fn ordered_requests_wait_for_the_leader_and_keep_their_order_across_a_reconnect() {
        let mut requests = SessionRequests::new();
        let first = requests.issue(repl_command("CREATE SCHEMA first ( value I64 );"));
        let second = requests.issue(repl_command("CREATE SCHEMA second ( value I64 );"));
        assert!(
            matches!(requests.accept(first), Admission::Held),
            "an ordered request waits until the server confirms that it leads"
        );
        assert!(matches!(requests.accept(second), Admission::Held));
        let completion = requests.issue(suggest("CREATE "));
        assert!(
            matches!(requests.accept(completion), Admission::Sent(_)),
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
        let first = sent(requests.accept(first));
        let second = sent(requests.accept(second));
        let third = sent(requests.accept(third));
        let sent = [first, second, third];
        assert!(requests.answer(sent[0].request_id).is_some());
        let name = SubscriptionName::parse("closing").assured("the test name is valid");
        let closing = requests.issue(ConsoleRequest::SubscriptionStop {
            tab_id: 9,
            request: UnsubscribeRequest { subscription: name },
        });
        assert!(matches!(requests.accept(closing), Admission::Sent(_)));

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
        let first = sent(requests.accept(first));
        let second = sent(requests.accept(second));
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
            matches!(requests.accept(third), Admission::Held),
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
        let earlier = sent(requests.accept(earlier));
        let later = requests.issue(suggest("SHOW"));
        let later = sent(requests.accept(later));
        let suggestions = || {
            ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                status: nervix_client_wire::SuggestionStatus::Ready,
                continuation: None,
                suggestions: Vec::new(),
            })
        };

        let stale = requests.route(reply(earlier.request_id, suggestions()));
        assert!(matches!(stale, Routed::Untracked));
        let latest = requests.route(reply(later.request_id, suggestions()));
        assert!(matches!(latest, Routed::Reply(_)));
    }

    #[test]
    fn only_the_latest_request_for_each_typed_choice_control_is_awaited() {
        let mut requests = SessionRequests::new();
        let earlier = requests.issue(choice_request(ChoiceControl::DomainPace, 1));
        let earlier = sent(requests.accept(earlier));
        assert!(matches!(earlier.request, ClientRequest::Choice(_)));

        let placement = requests.issue(choice_request(ChoiceControl::PlacementPolicy, 1));
        let placement = sent(requests.accept(placement));
        let schema = requests.issue(choice_request(ChoiceControl::BranchSchema, 1));
        let schema = sent(requests.accept(schema));
        let later = requests.issue(choice_request(ChoiceControl::DomainPace, 1));
        let later = sent(requests.accept(later));

        assert!(matches!(
            requests.route(reply(earlier.request_id, ready_choice_reply())),
            Routed::Untracked
        ));
        assert!(matches!(
            requests.route(reply(placement.request_id, ready_choice_reply())),
            Routed::Reply(_)
        ));
        assert!(matches!(
            requests.route(reply(schema.request_id, ready_choice_reply())),
            Routed::Reply(_)
        ));

        let Routed::Reply(answered) = requests.route(reply(later.request_id, ready_choice_reply()))
        else {
            panic!("the latest choice request remains tracked");
        };
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            signals.create.open(CreateKind::Domain, None, "trigger");
            assert!(matches!(
                apply_reply(signals, &mut requests, *answered),
                SessionStep::Continue
            ));
        });
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
        assert_eq!(
            diagnostic_line(query, diagnostic("unknown statement", 0, 6)).text,
            "- CREATE at 0..6: unknown statement",
            "a span starting at zero is a location in the query"
        );
    }

    fn described_version(number: u64, path: &str) -> ResourceVersionDescription {
        ResourceVersionDescription {
            version: NonZeroU64::new(number).assured("the test version is non-zero"),
            root_checksum: format!("root-{number}"),
            manifest_checksum: format!("manifest-{number}"),
            file_count: 1,
            total_bytes: 24,
            created_at: Timestamp::from_unix_nanos(1_789_000_000_000_000_000),
            created_by_node: ClusterNodeName::parse("node-1").assured("a literal node name"),
            entries: ResourceVersionEntries::Listed(vec![ResourceManifestEntry {
                path: path.to_string(),
                content: ResourceEntryContent::File {
                    size: 24,
                    checksum: format!("checksum-{number}"),
                },
            }]),
        }
    }

    fn usage(kind: ModelKind, name: &str, version: u64) -> ResourceUsage {
        ResourceUsage {
            node: NodeRef::new(kind, ModelName::parse(name).assured("a literal model name")),
            version: NonZeroU64::new(version).assured("the test version is non-zero"),
        }
    }

    #[test]
    fn resource_description_lists_each_usage_under_the_version_it_pins() {
        let mut outcome = completed_outcome("resource: lookup_bundle");
        outcome.resource = Some(Box::new(ResourceDescription {
            resource: ResourceName::parse("lookup_bundle").assured("a literal resource name"),
            latest_version: NonZeroU64::new(2),
            versions: vec![
                described_version(1, "lookup table.jsonl"),
                described_version(2, "lookup table.jsonl"),
            ],
            usages: vec![
                usage(ModelKind::Client, "lookup_store", 2),
                usage(ModelKind::Lookup, "lookup_by_id", 2),
            ],
        }));

        let detail = ResourceDetailView::from_description(outcome);

        assert_eq!(detail.status, "ready");
        let versions = detail
            .versions
            .iter()
            .map(|version| version.version.version.get())
            .collect::<Vec<_>>();
        assert_eq!(versions, vec![1, 2]);
        let first = &detail.versions[0];
        assert!(first.usages.is_empty());
        let ResourceVersionEntries::Listed(entries) = &first.version.entries else {
            panic!("the first version lists its entries");
        };
        let summaries = entries
            .iter()
            .map(|entry| format!("{} {}", entry.path, resource_entry_summary(entry)))
            .collect::<Vec<_>>();
        assert_eq!(
            summaries,
            vec!["lookup table.jsonl file | 24 bytes | checksum checksum-1"]
        );
        let second = &detail.versions[1];
        let usages = second
            .usages
            .iter()
            .map(|usage| {
                format!(
                    "{} {}",
                    usage.node.kind.keyword_phrase(),
                    usage.node.identifier.as_str()
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(usages, vec!["CLIENT lookup_store", "HASH MAP lookup_by_id"]);
        assert_eq!(
            resource_version_summary(&second.version),
            "1 files | 24 bytes | from node-1 | 2026-09-10 00:26:40 UTC"
        );
    }

    #[test]
    fn a_resource_description_shows_why_it_has_no_versions() {
        let mut failed = completed_outcome("resource 'lookup_bundle' does not exist");
        failed.disposition = CommandDisposition::Failed;
        let detail = ResourceDetailView::from_description(failed);
        assert!(detail.versions.is_empty());
        assert_eq!(detail.status, "resource 'lookup_bundle' does not exist");

        let untyped = completed_outcome("resource: lookup_bundle");
        let detail = ResourceDetailView::from_description(untyped);
        assert!(detail.versions.is_empty());
        assert_eq!(detail.status, MISSING_RESOURCE_DESCRIPTION);

        let entry = ResourceManifestEntry {
            path: "nested dir".to_string(),
            content: ResourceEntryContent::Directory,
        };
        assert_eq!(resource_entry_summary(&entry), "directory");
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
            DomainName::parse("demo").assured("the test names a valid domain"),
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
    fn a_tab_title_names_the_relay_and_its_canonical_filter() {
        let parse = |statement: &str| match parse_client_statement(statement) {
            Ok(ClientStatement::CreateSubscription(subscription)) => subscription,
            _ => panic!("the test statement is a subscription"),
        };
        assert_eq!(
            subscription_tab_title(&parse("CREATE SUBSCRIPTION live TO orders;")),
            "orders"
        );
        assert_eq!(
            subscription_tab_title(&parse(
                "CREATE SUBSCRIPTION live TO orders DROPPING WHERE input.user_id=42;"
            )),
            "orders input.user_id = 42"
        );
    }

    /// The message that carries a request the session sent at once.
    fn sent(admission: Admission) -> ClientMessage {
        let Admission::Sent(message) = admission else {
            panic!("the session sends the request at once");
        };
        message
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

    fn choice_request(control: ChoiceControl, draft_revision: u64) -> ConsoleRequest {
        let target = match control {
            ChoiceControl::DomainPace => nervix_client_wire::ChoiceTarget::DomainPace,
            ChoiceControl::PlacementPolicy => nervix_client_wire::ChoiceTarget::PlacementPolicy,
            ChoiceControl::BranchSchema
            | ChoiceControl::RelaySchema
            | ChoiceControl::CodecSchema => nervix_client_wire::ChoiceTarget::Schema,
            ChoiceControl::RelayBranch => nervix_client_wire::ChoiceTarget::Branch,
            ChoiceControl::SubscriptionRelay => nervix_client_wire::ChoiceTarget::Relay,
            ChoiceControl::SubscriptionField => nervix_client_wire::ChoiceTarget::RelayField,
            ChoiceControl::CodecWireSchema => nervix_client_wire::ChoiceTarget::WireJsonSchema,
            ChoiceControl::CodecResource
            | ChoiceControl::SignalingResource
            | ChoiceControl::ClientResource
            | ChoiceControl::VhostResource => nervix_client_wire::ChoiceTarget::Resource,
            ChoiceControl::CodecVersion
            | ChoiceControl::SignalingVersion
            | ChoiceControl::ClientVersion
            | ChoiceControl::VhostVersion
            | ChoiceControl::HashVersion => {
                nervix_client_wire::ChoiceTarget::CompletedResourceVersion
            }
            ChoiceControl::ClientSignaling | ChoiceControl::EndpointSignaling => {
                nervix_client_wire::ChoiceTarget::SignalingProtocol
            }
            ChoiceControl::EndpointVhost => nervix_client_wire::ChoiceTarget::Vhost,
            ChoiceControl::HashResource => nervix_client_wire::ChoiceTarget::Resource,
            ChoiceControl::HashCodec => nervix_client_wire::ChoiceTarget::Codec,
            ChoiceControl::HashKey => nervix_client_wire::ChoiceTarget::CodecField,
            ChoiceControl::IngestSourceRef => {
                nervix_client_wire::ChoiceTarget::IngestEndpointSource
            }
            ChoiceControl::IngestCodec => nervix_client_wire::ChoiceTarget::IngestCodec,
            ChoiceControl::IngestTimestampField | ChoiceControl::IngestInputField => {
                nervix_client_wire::ChoiceTarget::CodecField
            }
            ChoiceControl::IngestRouteBranch => nervix_client_wire::ChoiceTarget::Branch,
            ChoiceControl::IngestRouteRelay | ChoiceControl::IngestErrorRelay => {
                nervix_client_wire::ChoiceTarget::IngestUnbranchedRelay
            }
            ChoiceControl::IngestOutputField | ChoiceControl::IngestErrorField => {
                nervix_client_wire::ChoiceTarget::RelayField
            }
            ChoiceControl::IngestBranchField => nervix_client_wire::ChoiceTarget::BranchField,
        };
        ConsoleRequest::Choice {
            request: ChoiceLookupRequest::new(target, Vec::new(), String::new()),
            context: ChoiceRequestContext {
                control,
                draft_revision,
                session_generation: 0,
                append: false,
            },
        }
    }

    fn ready_choice_reply() -> ReplyBody {
        ReplyBody::Choice(nervix_client_wire::ChoiceOutcome {
            status: nervix_client_wire::ChoiceStatus::Ready,
            choices: Vec::new(),
            page_cursor: None,
        })
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
            wasm_state: None,
            resource: None,
            backup: None,
        }
    }

    #[test]
    fn create_admission_is_queued_only_while_its_transaction_remains_active() {
        let mut outcome = completed_outcome("created resource");
        outcome.transaction_admission = Some(TransactionOperationAdmission {
            operation: TransactionOperationNumber::from_index(0)
                .assured("the first transaction operation is addressable"),
            preview: TransactionPreviewIdentity {
                transaction_id: "standalone-or-attached".to_string(),
                position: TransactionPosition::new(1),
                planning_basis: ImpactPlanningBasis::new([7; 32]),
            },
        });

        assert_eq!(queued_transaction_position(&outcome), None);

        outcome.transaction = Some(
            TransactionStatus::new(
                "attached".to_string(),
                domain_name("demo"),
                TransactionLifecycle::Open,
                TransactionPosition::new(1),
                0,
            )
            .assured("the open transaction has not applied its accepted operation"),
        );
        assert_eq!(queued_transaction_position(&outcome), Some(1));

        outcome.transaction = Some(
            TransactionStatus::new(
                "finished".to_string(),
                domain_name("demo"),
                TransactionLifecycle::Committed,
                TransactionPosition::new(1),
                1,
            )
            .assured("the committed transaction applied its accepted operation"),
        );
        assert_eq!(queued_transaction_position(&outcome), None);
    }

    #[test]
    fn popup_submission_uses_the_shared_command_queue_and_masks_its_prompt() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            let transaction = TransactionStatus::new(
                "attached".to_string(),
                domain_name("demo"),
                TransactionLifecycle::Open,
                TransactionPosition::new(2),
                0,
            )
            .assured("the test transaction has two accepted, unapplied operations");
            signals.transaction_status.set(Some(transaction));
            let (sender, mut receiver) = request_handoff();
            submit_create_command(
                signals,
                RwSignal::new(Some(sender)),
                CreateKind::User,
                "CREATE USER operator WITH PASSWORD '********';".to_string(),
                CommandDispatch {
                    query: "CREATE USER operator WITH PASSWORD 'actual-secret';".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: None,
                },
                attempt,
                revision,
            );
            let ConsoleRequest::Command { request, purpose } = receiver
                .try_take()
                .assured("the shared command queue receives the popup command")
            else {
                panic!("popup submission sends a command request");
            };
            assert!(request.query.contains("actual-secret"));
            assert_eq!(
                request.expected_transaction_position,
                Some(TransactionPosition::new(2))
            );
            assert!(matches!(purpose, CommandPurpose::Create(_)));
            let prompt = &signals.terminal_lines.get_untracked().into_lines()[0]
                .line
                .text;
            assert!(prompt.contains("********"));
            assert!(!prompt.contains("actual-secret"));

            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(false);
            signals.transaction_status.set(None);
            submit_create_command(
                signals,
                RwSignal::new(None),
                CreateKind::User,
                "CREATE USER unavailable WITH PASSWORD '********';".to_string(),
                CommandDispatch {
                    query: "CREATE USER unavailable WITH PASSWORD 'secret';".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: None,
                },
                attempt,
                revision,
            );

            let (closed_sender, closed_receiver) = request_handoff();
            drop(closed_receiver);
            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            submit_create_command(
                signals,
                RwSignal::new(Some(closed_sender)),
                CreateKind::User,
                "CREATE USER closed WITH PASSWORD '********';".to_string(),
                CommandDispatch {
                    query: "CREATE USER closed WITH PASSWORD 'secret';".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: None,
                },
                attempt,
                revision,
            );
        });
    }

    #[test]
    fn create_outcomes_update_domain_resource_and_retry_state_once() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests = SessionRequests::new();

            signals.create.open(CreateKind::Domain, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            let created_domain = domain_name("created");
            show_create_outcome(
                signals,
                &mut requests,
                CreateCommandContext {
                    attempt,
                    draft_revision: revision,
                    kind: CreateKind::Domain,
                    presentation: "CREATE UNPACED DOMAIN created PLACEMENT NEUTRAL;".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: Some(created_domain.clone()),
                },
                completed_outcome("created domain"),
                None,
            );
            assert_eq!(signals.active_domain.get_untracked(), Some(created_domain));

            let scope = domain_name("created");
            signals
                .create
                .open(CreateKind::Resource, Some(scope.clone()), "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            show_create_outcome(
                signals,
                &mut requests,
                CreateCommandContext {
                    attempt,
                    draft_revision: revision,
                    kind: CreateKind::Resource,
                    presentation: "CREATE RESOURCE bundle;".to_string(),
                    domain: Some(scope.clone()),
                    resource: Some("bundle".to_string()),
                    created_domain: None,
                },
                completed_outcome("created resource"),
                None,
            );
            assert_eq!(
                signals.selected_resource.get_untracked().as_deref(),
                Some("bundle")
            );
            assert!(requests.held.values().any(|request| matches!(
                request,
                ConsoleRequest::Command { request, purpose: CommandPurpose::ResourceDescription { resource } }
                    if request.query == "DESCRIBE RESOURCE bundle;"
                        && request.domain.as_ref() == Some(&scope)
                        && resource == "bundle"
            )));

            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            let mut failed = completed_outcome("");
            failed.disposition = CommandDisposition::Failed;
            show_create_outcome(
                signals,
                &mut requests,
                CreateCommandContext {
                    attempt,
                    draft_revision: revision,
                    kind: CreateKind::User,
                    presentation: "CREATE USER duplicate WITH PASSWORD '********';".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: None,
                },
                failed,
                None,
            );

            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            let context = CreateCommandContext {
                attempt,
                draft_revision: revision,
                kind: CreateKind::User,
                presentation: "CREATE USER queued WITH PASSWORD '********';".to_string(),
                domain: None,
                resource: None,
                created_domain: None,
            };
            show_create_outcome(
                signals,
                &mut requests,
                context.clone(),
                completed_outcome("queued user"),
                Some(3),
            );
            show_create_outcome(
                signals,
                &mut requests,
                CreateCommandContext {
                    attempt: 0,
                    ..context.clone()
                },
                completed_outcome("stale completion"),
                None,
            );

            let issued = requests.issue(ConsoleRequest::Command {
                request: CommandRequest {
                    query: "CREATE USER queued WITH PASSWORD 'secret';".to_string(),
                    domain: None,
                    execution_reference: command_execution_reference(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                purpose: CommandPurpose::Create(context),
            });
            let IssuedRequest { order, request } = issued;
            let ConsoleRequest::Command { request, purpose } = request else {
                panic!("the test issued a create command");
            };
            let mut unknown = completed_outcome("leadership moved during admission");
            unknown.disposition = CommandDisposition::OutcomeUnknown(
                nervix_client_wire::UnknownOutcomeCause::LeadershipLost,
            );
            assert!(matches!(
                apply_command_outcome(
                    signals,
                    &mut requests,
                    order,
                    request,
                    purpose,
                    unknown,
                ),
                SessionStep::Reconnect
            ));
        });
    }

    #[test]
    fn cancelled_create_and_wrong_command_replies_report_terminal_failures() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests = SessionRequests::new();
            signals.create.open(CreateKind::User, None, "trigger");
            let (attempt, revision) = signals.create.begin_submission(true);
            let create = requests.issue(ConsoleRequest::Command {
                request: CommandRequest {
                    query: "CREATE USER operator WITH PASSWORD 'secret';".to_string(),
                    domain: None,
                    execution_reference: command_execution_reference(),
                    expected_transaction_position: None,
                    expected_preview: None,
                },
                purpose: CommandPurpose::Create(CreateCommandContext {
                    attempt,
                    draft_revision: revision,
                    kind: CreateKind::User,
                    presentation: "CREATE USER operator WITH PASSWORD '********';".to_string(),
                    domain: None,
                    resource: None,
                    created_domain: None,
                }),
            });
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: create,
                    body: ReplyBody::Cancelled(RequestCancelled {
                        stage: CancellationStage::BeforeAdmission,
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(
                signals
                    .terminal_lines
                    .get_untracked()
                    .into_lines()
                    .iter()
                    .any(|entry| entry.line.text.contains("cancelled before it was admitted"))
            );

            let command = requests.issue(repl_command("SHOW CLUSTER STATUS;"));
            let step = apply_reply(
                signals,
                &mut requests,
                AnsweredRequest {
                    request: command,
                    body: ReplyBody::Suggest(nervix_client_wire::SuggestOutcome {
                        status: SuggestionStatus::Ready,
                        continuation: None,
                        suggestions: Vec::new(),
                    }),
                },
            );
            assert!(matches!(step, SessionStep::Continue));
            assert!(
                signals
                    .terminal_lines
                    .get_untracked()
                    .into_lines()
                    .iter()
                    .any(|entry| entry.line.text.contains(UNEXPECTED_REPLY))
            );
        });
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
