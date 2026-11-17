//! Which replies answer a console request (C-11). A reply answers its request only when its kind is
//! the request's own outcome, a rejection or a cancellation that can reach it, as the session
//! contract's table in `nervix-client-wire` says. Any other kind under a waiting request's identity
//! is a protocol violation: the console checks the kind before it applies anything the reply
//! carries, takes nothing from it, stops reading the connection, and keeps every ordered request it
//! issued whole for the next connection. A reply whose request no longer waits is discarded,
//! whatever its kind.

use nervix_client_wire::{
    CancelOutcome, CancelState, ClientRequestKind, CloseEmitterOutcome, CloseIngestorDisposition,
    CloseIngestorOutcome, EmitterCloseDisposition, EmitterOpenRefusal, EmitterSettlement,
    OpenEmitterDisposition, OpenEmitterOutcome, OpenIngestorDisposition, OpenIngestorOutcome,
    ReadEmitterBatchOutcome, ReadEmitterDisposition, ReplyBodyKind, RequestRejected,
    RequestRejection, SettleEmitterBatchOutcome, SubmissionOutcome, SuggestOutcome,
};
use nervix_models::{ClientProducerRefusal, ClientSubmissionOutcome};
use strum::IntoEnumIterator as _;

use super::*;

/// One request of each kind the console sends, issued from the tab state it needs.
#[derive(Clone, Copy, Debug)]
enum Sample {
    Command,
    ListDomains,
    /// A subscription the operator opened, here by resubscribing an ended tab.
    Subscribe,
    /// The restoration of an interrupted tab, which belongs to its connection.
    Restore,
    Unsubscribe,
    SelectDomain,
    AttachClock,
    DetachClock,
    Suggest,
    Choice,
    AttachTransaction,
    InspectTransaction,
}

const SAMPLES: [Sample; 12] = [
    Sample::Command,
    Sample::ListDomains,
    Sample::Subscribe,
    Sample::Restore,
    Sample::Unsubscribe,
    Sample::SelectDomain,
    Sample::AttachClock,
    Sample::DetachClock,
    Sample::Suggest,
    Sample::Choice,
    Sample::AttachTransaction,
    Sample::InspectTransaction,
];

impl Sample {
    /// The state of the console's one tab when the request is issued.
    fn tab(self) -> SubscriptionTabState {
        match self {
            Self::Subscribe => SubscriptionTabState::Ended,
            Self::Restore => SubscriptionTabState::Interrupted(test_restoration()),
            Self::Unsubscribe => SubscriptionTabState::Open(test_stream()),
            Self::Command
            | Self::ListDomains
            | Self::SelectDomain
            | Self::AttachClock
            | Self::DetachClock
            | Self::Suggest
            | Self::Choice
            | Self::AttachTransaction
            | Self::InspectTransaction => SubscriptionTabState::Pending,
        }
    }

    /// The request, built by the console's own builder where it has one.
    fn request(self, signals: WebConsoleSignals) -> ConsoleRequest {
        let tenant = domain_name("tenant");
        match self {
            Self::Command => repl_command("CREATE SCHEMA first ( value I64 );"),
            Self::ListDomains => ConsoleRequest::ListDomains,
            Self::Subscribe => ConsoleRequest::SubscriptionStart {
                tab_id: 1,
                request: signals
                    .begin_resubscribe(1)
                    .assured("the sample's tab is ended, so it can be resubscribed"),
                origin: SubscriptionOrigin::Console,
            },
            Self::Restore => {
                let mut restorations = signals.begin_restorations();
                assert_eq!(restorations.len(), 1, "the sample has one interrupted tab");
                restorations.remove(0)
            }
            Self::Unsubscribe => ConsoleRequest::SubscriptionStop {
                tab_id: 1,
                request: signals
                    .begin_subscription_close(1)
                    .assured("the sample's tab is open, so closing it deletes its stream"),
            },
            Self::SelectDomain => {
                ConsoleRequest::SelectDomain(SelectDomainRequest { domain: tenant })
            }
            Self::AttachClock => ConsoleRequest::DomainClockAttach {
                request: AttachDomainClockRequest { domain: tenant },
                connection_generation: 0,
                origin: ClockRequestOrigin::Automatic,
            },
            Self::DetachClock => ConsoleRequest::DomainClockDetach {
                request: DetachDomainClockRequest { domain: tenant },
                connection_generation: 0,
                origin: ClockRequestOrigin::Repl,
            },
            Self::Suggest => suggest("SH"),
            Self::Choice => choice_request(ChoiceControl::DomainPace, 0),
            Self::AttachTransaction => {
                ConsoleRequest::AttachTransaction(AttachTransactionRequest {
                    transaction_id: "attached".to_string(),
                })
            }
            Self::InspectTransaction => {
                ConsoleRequest::InspectTransaction(InspectTransactionRequest {
                    target: TransactionInspectionTarget::Attached,
                    operation: None,
                })
            }
        }
    }

    /// Whether the request waits for the next connection when its own ends unanswered. The
    /// ordered requests the console issued itself do; a connection's own setup, a session-local
    /// deletion or clock request, and every lookup end with their connection, which the next
    /// connection issues again where they still matter.
    fn outlives_its_connection(self) -> bool {
        match self {
            Self::Command | Self::ListDomains | Self::Subscribe | Self::InspectTransaction => true,
            Self::Restore
            | Self::Unsubscribe
            | Self::SelectDomain
            | Self::AttachClock
            | Self::DetachClock
            | Self::Suggest
            | Self::Choice
            | Self::AttachTransaction => false,
        }
    }
}

/// A reply of `kind` carrying what the console would act on if it took the reply as an answer: a
/// listing, a selection, an opened subscription, an attached transaction or clock, an inspection.
/// A command outcome names the execution of `answering` when that is a command, so only its kind
/// can make it a violation.
fn reply_of(kind: ReplyBodyKind, answering: &ClientRequest) -> ReplyBody {
    let tenant = domain_name("tenant");
    match kind {
        ReplyBodyKind::Command => {
            let mut outcome = completed_outcome("created first");
            if let ClientRequest::Command(command) = answering {
                outcome.execution_reference = command.execution_reference.clone();
            }
            ReplyBody::Command(Box::new(outcome))
        }
        ReplyBodyKind::Attach => attached(),
        ReplyBodyKind::Suggest => ReplyBody::Suggest(SuggestOutcome {
            status: SuggestionStatus::Ready,
            suggestions: vec![WireSuggestion {
                value: "SHOW ".to_string(),
                kind: SuggestionKind::Text,
                edit: TextEdit {
                    start: 0,
                    end: 2,
                    replacement: "SHOW ".to_string(),
                },
            }],
            continuation: None,
        }),
        ReplyBodyKind::Choice => ready_choice_reply(),
        ReplyBodyKind::DomainList => ReplyBody::DomainList(DomainList {
            domains: vec![DomainInfo {
                start_version: 1,
                domain: domain_name("demo"),
                status: DomainStatus::Running,
                pace: DomainPace::Unpaced,
            }],
        }),
        ReplyBodyKind::DomainSelection => {
            ReplyBody::DomainSelection(DomainSelection::Selected(domain_name("demo")))
        }
        ReplyBodyKind::Inspection => {
            ReplyBody::Inspection(InspectionOutcome::Inspected(Box::new(test_inspection())))
        }
        ReplyBodyKind::Subscribe => {
            let stream = test_stream();
            ReplyBody::Subscribe(SubscribeOutcome {
                disposition: SubscribeDisposition::Opened(Box::new(SubscriptionOpened {
                    subscription: stream.subscription,
                    domain: tenant,
                    relay: RelayName::parse("orders").assured("the test relay name is valid"),
                    subscription_type: SubscriptionType::Row,
                    schema: stream.schema,
                })),
                message: String::new(),
                diagnostics: Vec::new(),
            })
        }
        ReplyBodyKind::Unsubscribe => ReplyBody::Unsubscribe(UnsubscribeOutcome {
            disposition: UnsubscribeDisposition::Deleted(test_stream().subscription),
            message: String::new(),
            diagnostics: Vec::new(),
        }),
        ReplyBodyKind::Cancel => ReplyBody::Cancel(CancelOutcome {
            target: request_id(1),
            state: CancelState::Requested,
        }),
        ReplyBodyKind::Cancelled => ReplyBody::Cancelled(RequestCancelled {
            stage: CancellationStage::BeforeAdmission,
        }),
        ReplyBodyKind::Rejected => ReplyBody::Rejected(RequestRejected {
            rejection: RequestRejection::ServerBusy,
            field: None,
            message: "the server is busy".to_string(),
        }),
        ReplyBodyKind::DomainClockAttach => {
            ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Attached {
                    domain: tenant,
                    clock: DomainClockObservation {
                        generation: 3,
                        state: DomainClockObservedState::Stopped,
                    },
                },
                message: "following".to_string(),
            })
        }
        ReplyBodyKind::DomainClockDetach => {
            ReplyBody::DomainClockDetach(DomainClockDetachOutcome {
                disposition: DomainClockDetachDisposition::Detached(tenant),
                message: String::new(),
            })
        }
        ReplyBodyKind::OpenIngestor => ReplyBody::OpenIngestor(OpenIngestorOutcome {
            disposition: OpenIngestorDisposition::Refused(ClientProducerRefusal::IngestorNotFound),
            message: "ingestor not found".to_string(),
        }),
        ReplyBodyKind::Submission => ReplyBody::Submission(SubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
        ReplyBodyKind::CloseIngestor => ReplyBody::CloseIngestor(CloseIngestorOutcome {
            disposition: CloseIngestorDisposition::Closed,
            message: String::new(),
        }),
        ReplyBodyKind::OpenEmitter => ReplyBody::OpenEmitter(OpenEmitterOutcome {
            disposition: OpenEmitterDisposition::Refused(EmitterOpenRefusal::EmitterNotFound),
            message: "emitter not found".to_string(),
        }),
        ReplyBodyKind::ReadEmitterBatch => ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
            disposition: ReadEmitterDisposition::Ended,
            message: String::new(),
        }),
        ReplyBodyKind::SettleEmitterBatch => {
            ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
                disposition: EmitterSettlement::Confirmed,
                message: String::new(),
            })
        }
        ReplyBodyKind::CloseEmitter => ReplyBody::CloseEmitter(CloseEmitterOutcome {
            disposition: EmitterCloseDisposition::Closed,
            message: String::new(),
        }),
    }
}

/// What the console shows outside its terminal, which a reply of another kind leaves as it was.
#[derive(Debug, PartialEq)]
struct Presented {
    transaction: Option<TransactionStatus>,
    active_domain: Option<DomainName>,
    domains: usize,
    tabs: Vec<(u64, &'static str)>,
    active_tab: Option<u64>,
    clock: ClockDisplay,
    suggestions: usize,
    suggestion_status: Option<SuggestionStatus>,
    inspection: Option<TransactionInspection>,
    inspector_error: Option<String>,
    selected_resource: Option<String>,
    resource_details: usize,
}

impl Presented {
    fn of(signals: WebConsoleSignals) -> Self {
        let tabs = signals
            .subscription_tabs
            .get_untracked()
            .iter()
            .map(|tab| (tab.id, tab.state.label()))
            .collect();
        Self {
            transaction: signals.transaction_status.get_untracked(),
            active_domain: signals.active_domain.get_untracked(),
            domains: signals.domains.get_untracked().len(),
            tabs,
            active_tab: signals.active_subscription_tab.get_untracked(),
            clock: signals.clock_display.get_untracked(),
            suggestions: signals.suggestions.get_untracked().len(),
            suggestion_status: signals.suggestion_status.get_untracked(),
            inspection: signals.inspector.inspection.get_untracked(),
            inspector_error: signals.inspector.error.get_untracked(),
            selected_resource: signals.selected_resource.get_untracked(),
            resource_details: signals.resource_details.get_untracked().len(),
        }
    }
}

/// The text of every terminal line, in order.
fn terminal_text(signals: WebConsoleSignals) -> Vec<String> {
    let lines = signals.terminal_lines.get_untracked().into_lines();
    lines.into_iter().map(|entry| entry.line.text).collect()
}

/// Answers `sample` with a reply of `reply`'s kind through the session's frame path, checks that
/// the console accepts it exactly when the contract says that kind answers the request, and
/// returns the kind of the request.
fn answer_sample(sample: Sample, reply: ReplyBodyKind) -> ClientRequestKind {
    Owner::new().with(|| {
        let signals = subscription_signals(sample.tab());
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        let issued = requests.issue(sample.request(signals));
        let message = sent(requests.accept(issued));
        let kind = ClientRequestKind::from(&message.request);
        let frame = reply_frame(&Reply {
            request_id: message.request_id,
            body: reply_of(reply, &message.request),
        });
        let before = Presented::of(signals);
        let lines_before = terminal_text(signals).len();

        let step = receive_frame(signals, state, &mut requests, &frame);
        if kind.is_answered_by(reply) {
            assert!(
                !matches!(step, SessionStep::Abandon),
                "a {reply} reply answers the {sample:?} request, so the session goes on"
            );
            return kind;
        }
        assert!(
            matches!(step, SessionStep::Abandon),
            "a {reply} reply cannot answer the {sample:?} request, so nothing later on its \
             connection is read"
        );
        let lines = terminal_text(signals);
        let added = &lines[lines_before..];
        assert_eq!(
            added.len(),
            1,
            "the console reports the violation and nothing else for a {reply} reply to the \
             {sample:?} request: {added:?}"
        );
        assert!(
            added[0].starts_with(&format!("error: protocol violation: {UNEXPECTED_REPLY};")),
            "{added:?}"
        );
        assert_eq!(
            Presented::of(signals),
            before,
            "the console takes nothing from a {reply} reply to the {sample:?} request"
        );

        // The session loop ends the connection the step abandons, and connects again.
        requests.end_connection();
        requests.confirm_leader();
        let resent = requests.release_held();
        if sample.outlives_its_connection() {
            assert_eq!(
                resent.len(),
                1,
                "the {sample:?} request waits for the next connection"
            );
            assert_eq!(
                resent[0].request, message.request,
                "the {sample:?} request goes out again whole"
            );
        } else {
            assert!(
                resent.is_empty(),
                "the {sample:?} request ends with its connection"
            );
        }
        kind
    })
}

#[test]
fn a_reply_answers_a_console_request_only_when_the_contract_says_its_kind_does() {
    let mut sent = BTreeSet::new();
    for sample in SAMPLES {
        for reply in ReplyBodyKind::iter() {
            let kind = answer_sample(sample, reply);
            sent.insert(<&'static str>::from(kind));
        }
    }
    let mut unsent = Vec::new();
    for kind in ClientRequestKind::iter() {
        if !sent.contains(<&'static str>::from(kind)) {
            unsent.push(kind);
        }
    }
    assert_eq!(
        unsent,
        vec![
            ClientRequestKind::Cancel,
            ClientRequestKind::OpenIngestor,
            ClientRequestKind::SubmitBatch,
            ClientRequestKind::CloseIngestor,
            ClientRequestKind::OpenEmitter,
            ClientRequestKind::ReadEmitterBatch,
            ClientRequestKind::SettleEmitterBatch,
            ClientRequestKind::CloseEmitter,
        ],
        "the samples cover every request the console sends, and the console sends neither a \
         cancellation nor a producer or consumer request"
    );
}

#[test]
fn a_reply_of_another_kind_to_a_superseded_lookup_is_discarded_without_ending_the_session() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        let earlier = requests.issue(suggest("SH"));
        let earlier = sent(requests.accept(earlier));
        let later = requests.issue(suggest("SHO"));
        let later = sent(requests.accept(later));
        let before = Presented::of(signals);

        // The earlier completion is no longer awaited, so its late reply completes nothing,
        // whatever its kind.
        let late = reply_frame(&Reply {
            request_id: earlier.request_id,
            body: attached(),
        });
        let step = receive_frame(signals, state, &mut requests, &late);
        assert!(matches!(step, SessionStep::Continue));
        assert!(terminal_text(signals).is_empty());
        assert_eq!(Presented::of(signals), before);
        assert!(
            !requests.in_flight.contains_key(&earlier.request_id),
            "the late reply frees the request place the superseded lookup kept"
        );

        // The later completion still waits, and a reply of another kind under its identity ends
        // the session.
        let violating = reply_frame(&Reply {
            request_id: later.request_id,
            body: attached(),
        });
        let step = receive_frame(signals, state, &mut requests, &violating);
        assert!(matches!(step, SessionStep::Abandon));
        assert_eq!(Presented::of(signals), before);
    });
}
