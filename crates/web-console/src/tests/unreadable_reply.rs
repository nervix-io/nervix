//! Unreadable replies interrupt their session and preserve commands in issue order.

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

fn preserves_the_whole_command_and_issue_order(split: bool) {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new();
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
        let step = if split {
            let parts = suggestion_frames(first.request_id);
            receive_frame(signals, state, &mut requests, &parts[1])
        } else {
            receive_frame(signals, state, &mut requests, &undecodable(&first))
        };
        assert!(
            matches!(step, SessionStep::Abandon),
            "unreadable replies end the session"
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
    preserves_the_whole_command_and_issue_order(false);
}

#[test]
fn an_unreadable_split_reply_preserves_the_whole_command_and_issue_order() {
    preserves_the_whole_command_and_issue_order(true);
}

#[test]
fn an_unreadable_joined_reply_keeps_its_command() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new();
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

fn visual_create_keeps_waiting_for_its_own_outcome(split: bool) {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let state = RwSignal::new(ConsoleConnectionState::Connected);
        let mut requests = SessionRequests::new();
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
        let frame = if split {
            suggestion_frames(create.request_id)[1].clone()
        } else {
            undecodable(&create)
        };
        let step = receive_frame(signals, state, &mut requests, &frame);
        assert!(matches!(step, SessionStep::Abandon));
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
    visual_create_keeps_waiting_for_its_own_outcome(false);
}

#[test]
fn an_unreadable_split_visual_create_keeps_waiting_for_its_own_outcome() {
    visual_create_keeps_waiting_for_its_own_outcome(true);
}

#[test]
fn an_unexpected_reply_body_interrupts_instead_of_failing_a_command() {
    Owner::new().with(|| {
        let signals = subscription_signals(SubscriptionTabState::Pending);
        let mut requests = SessionRequests::new();
        requests.confirm_leader();
        let issued = requests.issue(repl_command("CREATE SCHEMA first ( value I64 );"));
        let sent = sent(requests.accept(issued));
        let Routed::Reply(answered) = requests.route(reply(
            sent.request_id,
            ReplyBody::DomainList(DomainList {
                domains: Vec::new(),
            }),
        )) else {
            panic!("the reply answers the issued request");
        };
        assert!(matches!(
            apply_reply(signals, &mut requests, *answered),
            SessionStep::Abandon
        ));
        requests.end_connection();
        requests.confirm_leader();
        assert_eq!(
            sent_commands(&requests.release_held()),
            sent_commands(&[sent])
        );
    });
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
        let mut requests = SessionRequests::new();
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
