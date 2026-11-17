//! A command reply that names another execution than the command's own (E-5): the console takes
//! nothing from it, reports a protocol violation, stops reading its connection, and repeats the
//! command under its own execution reference, in its place in the issue order, once the session
//! connects again.

use leptos::prelude::Owner;
use nervix_client_wire::{NodeEndpoints, UnknownOutcomeCause};
use nervix_models::{
    ClusterNodeName, TransactionLifecycle, TransactionPosition, TransactionStatus,
};

use super::*;

/// Outcomes of another execution that the console would act on if it took one for the command's
/// own: a success that opens a transaction, a failure, a redirect to another console, an unknown
/// outcome, and a detached transaction.
fn foreign_outcomes() -> Vec<CommandOutcome> {
    let transaction = TransactionStatus::new(
        "foreign".to_string(),
        domain_name("demo"),
        TransactionLifecycle::Open,
        TransactionPosition::new(1),
        0,
    )
    .assured("no operation of the test transaction has applied");
    let mut completed = completed_outcome("foreign success");
    completed.transaction = Some(transaction);

    let mut failed = completed_outcome("foreign failure");
    failed.disposition = CommandDisposition::Failed;

    let console = Url::parse("http://node-2:17420/console/").assured("a literal URL");
    let mut redirected = completed_outcome("foreign redirect");
    redirected.disposition = CommandDisposition::NotLeader(LeaderRedirect {
        leader: Some(NodeEndpoints {
            node: ClusterNodeName::parse("node-2").assured("a literal node name"),
            grpc_uri: None,
            web_console_uri: Some(console),
        }),
    });

    let mut unknown = completed_outcome("foreign unknown outcome");
    unknown.disposition = CommandDisposition::OutcomeUnknown(UnknownOutcomeCause::LeadershipLost);

    let mut detached = completed_outcome("foreign detached transaction");
    detached.disposition = CommandDisposition::TransactionDetached {
        transaction_id: "foreign".to_string(),
    };

    vec![completed, failed, redirected, unknown, detached]
}

/// The terminal lines that contain `text`.
fn lines_containing(signals: WebConsoleSignals, text: &str) -> usize {
    let lines = signals.terminal_lines.get_untracked().into_lines();
    lines
        .iter()
        .filter(|entry| entry.line.text.contains(text))
        .count()
}

/// Answers `request` with its own completed outcome and returns the step the console takes.
fn answer_with_own_outcome(
    signals: WebConsoleSignals,
    requests: &mut SessionRequests,
    request: &ClientMessage,
    message: &str,
) -> SessionStep {
    let ClientRequest::Command(command) = &request.request else {
        panic!("the test answers a command");
    };
    let mut own = completed_outcome(message);
    own.execution_reference = command.execution_reference.clone();
    let terminal = reply(request.request_id, ReplyBody::Command(Box::new(own)));
    let Routed::Reply(answered) = requests.route(terminal) else {
        panic!("the repeated command awaits its own outcome");
    };
    apply_reply(signals, requests, *answered)
}

#[test]
fn a_repl_command_answered_with_another_execution_is_repeated_under_its_own_reference() {
    for foreign in foreign_outcomes() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests =
                SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
            requests.confirm_leader();
            let first = requests.issue(repl_command("CREATE SCHEMA first ( value I64 );"));
            let second = requests.issue(repl_command("CREATE SCHEMA second ( value I64 );"));
            let first = sent(requests.accept(first));
            sent(requests.accept(second));
            let ClientRequest::Command(original) = &first.request else {
                panic!("the test sent a command");
            };
            let original = original.clone();
            let received = foreign.execution_reference.clone();
            let foreign_message = foreign.message.clone();
            assert_ne!(received, original.execution_reference);

            let answered = requests.route(reply(
                first.request_id,
                ReplyBody::Command(Box::new(foreign)),
            ));
            let Routed::Reply(answered) = answered else {
                panic!("the reply's identity names the command in flight");
            };
            let step = apply_reply(signals, &mut requests, *answered);
            assert!(
                matches!(step, SessionStep::Abandon),
                "'{foreign_message}' breaks the session contract, so the connection is not read \
                 any further"
            );
            assert_eq!(
                lines_containing(signals, &foreign_message),
                0,
                "nothing of another execution's outcome is shown as the command's"
            );
            assert_eq!(lines_containing(signals, "protocol violation"), 1);
            assert_eq!(
                lines_containing(
                    signals,
                    &format!(
                        "the reply to command execution '{}' names execution '{received}'",
                        original.execution_reference
                    )
                ),
                1,
                "the violation names both executions"
            );
            assert!(
                signals.transaction_status.get_untracked().is_none(),
                "the foreign reply's transaction is not adopted"
            );
            assert!(
                !requests.is_attaching(),
                "no transaction is attached for the foreign reply"
            );

            // The session loop ends the connection the step abandons, and connects again.
            requests.end_connection();
            assert!(
                requests.release_held().is_empty(),
                "nothing is sent before the new connection's leader is confirmed"
            );
            requests.confirm_leader();
            let resent = requests.release_held();
            assert_eq!(
                sent_queries(&resent),
                vec![
                    "CREATE SCHEMA first ( value I64 );",
                    "CREATE SCHEMA second ( value I64 );"
                ],
                "the command keeps its place ahead of later work"
            );
            assert_eq!(
                sent_commands(&resent)[0],
                &original,
                "the command is repeated whole, under its own execution reference"
            );

            let step = answer_with_own_outcome(signals, &mut requests, &resent[0], "created first");
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(
                lines_containing(signals, "created first"),
                1,
                "the command has one result, its own"
            );
        });
    }
}

#[test]
fn a_visual_create_answered_with_another_execution_keeps_waiting_for_its_own_outcome() {
    for foreign in foreign_outcomes() {
        Owner::new().with(|| {
            let signals = subscription_signals(SubscriptionTabState::Pending);
            let mut requests =
                SessionRequests::new(Some(nervix_client_wire::SessionLimits::DEFAULT));
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
            let ClientRequest::Command(original) = &create.request else {
                panic!("the test sent a command");
            };
            let original = original.clone();
            let foreign_message = foreign.message.clone();

            let answered = requests.route(reply(
                create.request_id,
                ReplyBody::Command(Box::new(foreign)),
            ));
            let Routed::Reply(answered) = answered else {
                panic!("the reply's identity names the create in flight");
            };
            let step = apply_reply(signals, &mut requests, *answered);
            assert!(
                matches!(step, SessionStep::Abandon),
                "'{foreign_message}' breaks the session contract"
            );
            assert_eq!(
                lines_containing(signals, &foreign_message),
                0,
                "the create shows nothing of another execution's outcome"
            );
            assert_eq!(lines_containing(signals, "protocol violation"), 1);
            assert!(signals.transaction_status.get_untracked().is_none());
            assert!(
                signals.selected_resource.get_untracked().is_none(),
                "the create does not complete from another execution's outcome"
            );

            requests.end_connection();
            requests.confirm_leader();
            let resent = requests.release_held();
            assert_eq!(sent_commands(&resent), vec![&original]);

            let step = answer_with_own_outcome(
                signals,
                &mut requests,
                &resent[0],
                "created resource 'answered_bundle'",
            );
            assert!(matches!(step, SessionStep::Continue));
            assert_eq!(
                lines_containing(signals, "created resource 'answered_bundle'"),
                1
            );
            assert_eq!(
                signals.selected_resource.get_untracked().as_deref(),
                Some("answered_bundle"),
                "the create completes from its own outcome"
            );
        });
    }
}
