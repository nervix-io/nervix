//! Replies that break the session contract interrupt their session and preserve commands in issue
//! order: a reply that cannot be decoded or reassembled, and a readable reply of another kind than
//! its request is answered with (C-11).

use bytes::Bytes;

use super::*;

fn undecodable(request: &ClientMessage) -> VerifiedFrame<ServerFrame> {
    let ClientRequest::Command(command) = &request.request else {
        panic!("the fixture answers a command");
    };
    let mut outcome = completed_outcome("created first");
    outcome.execution_reference = command.execution_reference.clone();
    let ReplyDelivery::Frame(frame) = (Reply {
        request_id: request.request_id,
        body: ReplyBody::Command(Box::new(outcome)),
    })
    .encode(&SESSION_LIMITS)
    .assured("the fixture fits one frame") else {
        panic!("the fixture fits one frame");
    };
    let mut bytes = frame.into_bytes().to_vec();
    let reference = command.execution_reference.as_str().as_bytes();
    let offset = bytes
        .windows(reference.len())
        .position(|window| window == reference)
        .assured("the encoded outcome carries the reference");
    bytes[offset] = b'~';
    let frame = VerifiedFrame::<ServerFrame>::verify(Bytes::from(bytes), &SESSION_LIMITS)
        .assured("changing a string's content preserves FlatBuffers structure");
    assert_eq!(frame.request_id(), Some(request.request_id));
    assert!(ServerMessage::decode(&frame).is_err());
    frame
}

/// A listing of the command's domain under the command's request identity: a valid reply of
/// another kind than a command is answered with, as the testing seam answers one.
fn another_kind(request: &ClientMessage) -> VerifiedFrame<ServerFrame> {
    let ClientRequest::Command(command) = &request.request else {
        panic!("the fixture answers a command");
    };
    let domain = command
        .domain
        .clone()
        .assured("the fixture's commands name their domain");
    reply_frame(&Reply {
        request_id: request.request_id,
        body: ReplyBody::DomainList(DomainList {
            domains: vec![DomainInfo {
                start_version: 1,
                domain,
                status: DomainStatus::Running,
                pace: DomainPace::Unpaced,
            }],
        }),
    })
}

/// How the reply to a command breaks the session contract.
#[derive(Clone, Copy, Debug)]
enum Violation {
    /// The reply's frame verifies but does not decode.
    Undecodable,
    /// A part of a split reply arrives where its first part should.
    Split,
    /// The reply decodes as a reply of another kind under the command's request identity.
    AnotherKind,
}

impl Violation {
    /// The frame that answers `request` this way.
    fn frame(self, request: &ClientMessage) -> VerifiedFrame<ServerFrame> {
        match self {
            Self::Undecodable => undecodable(request),
            Self::Split => suggestion_frames(request.request_id)[1].clone(),
            Self::AnotherKind => another_kind(request),
        }
    }

    /// The cause the console reports for the violation.
    fn diagnostic(self) -> &'static str {
        match self {
            Self::Undecodable => "failed to decode a server message",
            Self::Split => "failed to reassemble a reply",
            Self::AnotherKind => UNEXPECTED_REPLY,
        }
    }
}

/// Checks that the terminal shows the violation as its only line: the command is not reported as
/// failed, and nothing the reply carries is shown.
fn assert_reported_only(signals: WebConsoleSignals, violation: Violation) {
    let lines = signals.terminal_lines.get_untracked().into_lines();
    assert_eq!(lines.len(), 1, "{violation:?} adds one terminal line");
    let text = &lines[0].line.text;
    assert!(text.starts_with("error: protocol violation: "), "{text}");
    assert!(text.contains(violation.diagnostic()), "{text}");
    assert!(
        text.contains("pending command outcomes are unknown"),
        "{text}"
    );
}

fn preserves_the_whole_command_and_issue_order(violation: Violation) {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        let mut first = repl_command("CREATE SCHEMA first ( value I64 );");
        let ConsoleRequest::Command { request, .. } = &mut first else {
            panic!("the fixture prepares a command");
        };
        request.expected_transaction_position = Some(TransactionPosition::new(2));
        request.expected_preview = Some(TransactionPreviewIdentity {
            transaction_id: "attached".to_string(),
            position: TransactionPosition::new(2),
            planning_basis: ImpactPlanningBasis::new([7; 32]),
        });
        let first = requests.issue(first);
        let first_order = first.order;
        let previous = requests.command_retries.wait(first_order).delay;
        requests.command_retries.ready(first_order);
        let second = requests.issue(repl_command("CREATE SCHEMA second ( value I64 );"));
        let first = sent(requests.accept(first));
        let second = sent(requests.accept(second));
        let frame = violation.frame(&first);
        let step = receive_frame(signals, state, &mut requests, &frame);
        assert!(
            matches!(step, SessionStep::Abandon),
            "{violation:?} ends the session"
        );
        assert_reported_only(signals, violation);
        assert!(
            signals.domains.get_untracked().is_empty(),
            "nothing the reply carries is adopted"
        );
        requests.end_connection();
        assert!(requests.in_flight.is_empty());
        assert!(requests.transfers.is_empty());
        requests.confirm_leader();
        let resent = requests.release_held();
        assert_eq!(sent_commands(&resent), sent_commands(&[first, second]));
        assert_eq!(
            requests.command_retries.wait(first_order).delay,
            previous
                .checked_mul(2)
                .assured("two bounded delays fit Duration")
        );
    });
}

#[test]
fn an_unreadable_reply_preserves_the_whole_command_and_issue_order() {
    preserves_the_whole_command_and_issue_order(Violation::Undecodable);
}

#[test]
fn an_unreadable_split_reply_preserves_the_whole_command_and_issue_order() {
    preserves_the_whole_command_and_issue_order(Violation::Split);
}

#[test]
fn a_reply_of_another_kind_preserves_the_whole_command_and_issue_order() {
    preserves_the_whole_command_and_issue_order(Violation::AnotherKind);
}

#[test]
fn an_unreadable_joined_reply_keeps_its_command() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        let issued = requests.issue(repl_command("CREATE SCHEMA first ( value I64 );"));
        let command = sent(requests.accept(issued));
        let mut frames = suggestion_frames(command.request_id);
        // The first fixture part is bounded by its 1 KiB frame limit. Only the joined reply
        // interprets its chunk's string bytes, so invalid UTF-8 keeps every part valid.
        let mut bytes = frames[0].bytes().to_vec();
        let offset = bytes
            .windows(b"SHOW SHOW SHOW ".len())
            .position(|window| window == b"SHOW SHOW SHOW ")
            .assured("the first chunk holds the fixture's long suggestion string");
        bytes[offset] = 0xff;
        frames[0] = VerifiedFrame::<ServerFrame>::verify(Bytes::from(bytes), &SESSION_LIMITS)
            .assured("a transfer chunk is a byte vector");
        let (last, preceding) = frames.split_last().assured("the fixture spans frames");
        for frame in preceding {
            assert!(matches!(
                receive_frame(signals, state, &mut requests, frame),
                SessionStep::Continue
            ));
        }
        assert!(matches!(
            receive_frame(signals, state, &mut requests, last),
            SessionStep::Abandon
        ));
        assert!(
            signals
                .terminal_lines
                .get_untracked()
                .into_lines()
                .iter()
                .any(|entry| entry.line.text.contains("failed to reassemble a reply"))
        );
        requests.end_connection();
        requests.confirm_leader();
        assert_eq!(
            sent_commands(&requests.release_held()),
            sent_commands(&[command])
        );
    });
}

fn visual_create_keeps_waiting_for_its_own_outcome(violation: Violation) {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        signals
            .create
            .open(CreateKind::Resource, Some(domain_name("demo")), "trigger");
        let (attempt, revision) = signals.create.begin_submission(true);
        let create = requests.issue(ConsoleRequest::Command {
            request: CommandRequest {
                query: "CREATE RESOURCE answered_bundle;".to_string(),
                domain: Some(domain_name("demo")),
                execution_reference: command_execution_reference(),
                expected_transaction_position: None,
                expected_preview: None,
            },
            purpose: CommandPurpose::Create(CreateCommandContext {
                attempt,
                draft_revision: revision,
                kind: CreateKind::Resource,
                presentation: "CREATE RESOURCE answered_bundle;".to_string(),
                domain: Some(domain_name("demo")),
                resource: Some("answered_bundle".to_string()),
                created_domain: None,
            }),
        });
        let create = sent(requests.accept(create));
        let frame = violation.frame(&create);
        let step = receive_frame(signals, state, &mut requests, &frame);
        assert!(matches!(step, SessionStep::Abandon));
        assert_reported_only(signals, violation);
        assert!(signals.selected_resource.get_untracked().is_none());
        requests.end_connection();
        signals.create.connection_lost();
        requests.confirm_leader();
        let resent = requests.release_held();
        assert_eq!(sent_commands(&resent), sent_commands(&[create]));
        let own = &resent[0];
        let ClientRequest::Command(command) = &own.request else {
            panic!("a create is a command");
        };
        let mut outcome = completed_outcome("created resource 'answered_bundle'");
        outcome.execution_reference = command.execution_reference.clone();
        let Routed::Reply(answered) =
            requests.route(reply(own.request_id, ReplyBody::Command(Box::new(outcome))))
        else {
            panic!("the create awaits its reply");
        };
        assert!(matches!(
            apply_reply(signals, &mut requests, *answered),
            SessionStep::Continue
        ));
        assert_eq!(
            signals.selected_resource.get_untracked().as_deref(),
            Some("answered_bundle")
        );
    });
}

#[test]
fn an_unreadable_visual_create_keeps_waiting_for_its_own_outcome() {
    visual_create_keeps_waiting_for_its_own_outcome(Violation::Undecodable);
}

#[test]
fn an_unreadable_split_visual_create_keeps_waiting_for_its_own_outcome() {
    visual_create_keeps_waiting_for_its_own_outcome(Violation::Split);
}

#[test]
fn a_visual_create_answered_with_a_reply_of_another_kind_keeps_waiting_for_its_own_outcome() {
    visual_create_keeps_waiting_for_its_own_outcome(Violation::AnotherKind);
}

#[test]
fn a_readable_split_reply_updates_its_waiting_request_once() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        signals.active_domain.set(None);
        signals.suggestion_query.set(Some(SuggestionQuery {
            input: "SH".to_string(),
            cursor: 2,
            domain: None,
        }));
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
        requests.confirm_leader();
        let issued = requests.issue(suggest("SH"));
        let sent = sent(requests.accept(issued));
        let frames = suggestion_frames(sent.request_id);
        let (last, preceding) = frames.split_last().assured("the reply spans frames");
        for frame in preceding {
            assert!(matches!(
                receive_frame(signals, state, &mut requests, frame),
                SessionStep::Continue
            ));
            assert!(signals.suggestions.get_untracked().is_empty());
        }
        assert!(matches!(
            receive_frame(signals, state, &mut requests, last),
            SessionStep::Continue
        ));
        assert_eq!(signals.suggestions.get_untracked().len(), 1);
        assert_eq!(
            signals.suggestions.get_untracked()[0].value,
            "SHOW ".repeat(1024)
        );
        assert_eq!(
            signals.suggestion_status.get_untracked(),
            Some(SuggestionStatus::Ready)
        );
        assert!(requests.in_flight.is_empty());
        assert!(requests.transfers.is_empty());
        assert!(matches!(
            receive_frame(signals, state, &mut requests, last),
            SessionStep::Continue
        ));
        assert_eq!(signals.suggestions.get_untracked().len(), 1);
    });
}
