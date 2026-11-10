//! Ordinary-mode evidence for what the node's log filter lets a subscriber record.
//!
//! Outside the layer order: tests may name any layer and product code may not name them.
//!
//! - **Owns.** Filters written as an operator writes `RUST_LOG`, the node's default, and
//!   assertions over what a subscriber deciding with them records and answers for a callsite.
//! - **Depends on.** The tracing owner and the captured-logs test support.
//! - **Must not know.** What a connector does with a message its broker returned.
//!
//! Every event these tests write goes to a capturing subscriber, so no callsite is first reached
//! while nothing is interested in it. Each case also records through the configured filter alone,
//! which proves the filter would select the returned message the rule keeps out.

use meticulous::{OptionExt as _, ResultExt as _};
use tracing::{
    Metadata, Span, Subscriber, debug, debug_span, info, info_span, level_filters::LevelFilter,
    subscriber::Interest, trace, trace_span, warn, warn_span,
};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt as _, registry::LookupSpan};

use super::{DEFAULT_TRACE_FILTER, LogFilter, MqttWarningPrefix};
use crate::captured_logs::CapturedLogs;

/// What Lapin's returned-message event carries in place of a returned record.
const RETURNED_BODY: &str = "returned-record-body";

const RETURNED_MESSAGE: &str = "Server returned us a message";

/// A filter written as an operator writes `RUST_LOG`.
fn operator_filter(directives: &str) -> LogFilter {
    // `EnvFilter::new` skips a directive it cannot parse, and a skipped one proves nothing.
    let configured = EnvFilter::try_new(directives)
        .assured("the cases write only directives in tracing-subscriber's grammar");
    LogFilter { configured }
}

/// The configured filter alone, on the registry, which is what each case's control records through.
fn filter_alone(
    directives: &str,
) -> impl Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static {
    tracing_subscriber::registry().with(operator_filter(directives).configured)
}

/// What `subscriber` records of what `write` logs, through a writer layered on it as the node
/// layers its own.
fn recorded<S>(subscriber: S, write: impl FnOnce()) -> String
where
    S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static,
{
    let logs = CapturedLogs::default();
    let subscriber = subscriber.with(fmt::layer().with_writer(logs.clone()).with_ansi(false));
    tracing::subscriber::with_default(subscriber, write);
    logs.contents()
}

/// Lapin's event for a message its broker returned, with the target and level Lapin gives it.
fn lapin_logs_a_returned_message() {
    let message = RETURNED_BODY;
    warn!(target: "lapin::returned_messages", ?message, "Server returned us a message");
}

/// One of Lapin's `trace` events about returned messages, which carries no message.
fn lapin_traces_a_dropped_confirm() {
    trace!(target: "lapin::returned_messages", "Registering new dropped PublisherConfirm");
}

fn node_logs_its_startup() {
    info!(target: "nervix_server::application", "node startup finished");
}

fn node_debugs_a_batch() {
    debug!(target: "nervix_server::runtime", "batch detail");
}

/// The pinned MQTT driver's warning carries the full refused request inside its message.
fn mqtt_logs_a_refused_retained_publication() {
    let request = "confidential-retained-payload-and-user-properties";
    warn!(target: "rumqttc::eventloop",
        "rejecting retained publish because the broker does not support retained messages: {request:?}");
}

fn mqtt_logs_a_connection_warning() {
    warn!(target: "rumqttc::eventloop", "MQTT connection closed before CONNACK");
}

#[test]
fn mqtt_connection_warnings_remain_visible_without_refused_retained_payloads() {
    fn warnings() {
        let span = info_span!(target: "rumqttc::eventloop", "mqtt_publish", channel = 7_u64);
        let _entered = span.enter();
        mqtt_logs_a_refused_retained_publication();
        mqtt_logs_a_connection_warning();
    }
    for directives in [
        "info",
        "trace",
        "error,rumqttc::eventloop=warn",
        "error,[mqtt_publish]=trace",
        "error,rumqttc::eventloop[mqtt_publish{channel=7}]=trace",
    ] {
        let unruled = recorded(filter_alone(directives), warnings);
        assert!(unruled.contains("confidential-retained-payload-and-user-properties"));
        let ruled = recorded(operator_filter(directives).into_subscriber(), warnings);
        assert!(!ruled.contains("confidential-retained-payload-and-user-properties"));
        assert!(ruled.contains("MQTT connection closed before CONNACK"));
    }
}

#[test]
fn the_mqtt_prefix_comparison_stops_at_a_match_or_the_first_difference() {
    use std::fmt::Write as _;

    let mut prefix = MqttWarningPrefix::Matching(b"fixed prefix");
    assert!(prefix.write_str("fixed ").is_ok());
    assert!(!prefix.matched());
    assert!(prefix.write_str("prefix").is_err());
    assert!(prefix.matched());
    assert!(prefix.write_str("request").is_err());

    let mut prefix = MqttWarningPrefix::Matching(b"fixed prefix");
    assert!(prefix.write_str("fixed ").is_ok());
    assert!(prefix.write_str("other warning").is_err());
    assert!(matches!(prefix, MqttWarningPrefix::Different));
}

#[test]
fn the_refused_mqtt_request_is_never_formatted_by_the_payload_filter() {
    struct ConfidentialRequest;
    impl std::fmt::Debug for ConfidentialRequest {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("the payload filter must decide from the fixed prefix before the request");
        }
    }
    let logs = recorded(operator_filter("trace").into_subscriber(), || {
        let request = ConfidentialRequest;
        warn!(target: "rumqttc::eventloop",
            "rejecting retained publish because the broker does not support retained messages: {request:?}");
    });
    assert!(logs.is_empty());
}

/// Everything these tests look for, so every filter sees every callsite.
fn every_event() {
    lapin_logs_a_returned_message();
    lapin_traces_a_dropped_confirm();
    node_logs_its_startup();
    node_debugs_a_batch();
}

fn holds_a_returned_message(log: &str) -> bool {
    log.contains(RETURNED_MESSAGE) || log.contains(RETURNED_BODY)
}

/// Records `write` through the filter `directives` configure, alone and with the payload rule, and
/// returns what the rule let through after checking that the filter alone selects the returned
/// message and the rule keeps it out.
fn recorded_with_and_without_the_rule(directives: &str, write: fn()) -> String {
    let unruled = recorded(filter_alone(directives), write);
    assert!(
        unruled.contains(RETURNED_MESSAGE),
        "the filter '{directives}' alone does not select Lapin's returned message:\n{unruled}"
    );
    let ruled = recorded(operator_filter(directives).into_subscriber(), write);
    assert!(
        !holds_a_returned_message(&ruled),
        "the filter '{directives}' let Lapin's returned message into the log:\n{ruled}"
    );
    ruled
}

#[test]
fn the_default_filter_records_startup_without_a_returned_message() {
    let log = recorded_with_and_without_the_rule(DEFAULT_TRACE_FILTER, every_event);

    assert!(log.contains("node startup finished"), "{log}");
    assert!(
        !log.contains("Registering new dropped PublisherConfirm"),
        "{log}"
    );
    assert!(!log.contains("batch detail"), "{log}");
}

#[test]
fn no_operator_filter_records_a_returned_message() {
    let operator_filters = [
        "info",
        "warn",
        "trace",
        "lapin=trace",
        "info,lapin::returned_messages=warn",
        "info,lapin::returned_messages=trace",
        "error,lapin::returned_messages[{message}]=warn",
    ];
    for directives in operator_filters {
        recorded_with_and_without_the_rule(directives, every_event);
    }
}

/// Lapin writes its returned-message event inside its `io_loop` span, so a directive scoped to that
/// span selects it.
fn every_event_in_an_io_loop() {
    let io_loop = info_span!("io_loop");
    let _entered = io_loop.enter();
    every_event();
}

#[test]
fn a_span_scoped_directive_records_its_span_but_not_a_returned_message() {
    let log =
        recorded_with_and_without_the_rule("error,[io_loop]=trace", every_event_in_an_io_loop);

    assert!(log.contains("batch detail"), "{log}");
    assert!(
        log.contains("Registering new dropped PublisherConfirm"),
        "{log}"
    );
}

#[test]
fn an_operator_filter_keeps_its_choice_for_every_other_event() {
    let log = recorded_with_and_without_the_rule(
        "warn,nervix_server::runtime=debug,lapin::returned_messages=trace",
        every_event,
    );

    assert!(log.contains("batch detail"), "{log}");
    assert!(
        log.contains("Registering new dropped PublisherConfirm"),
        "{log}"
    );
    assert!(!log.contains("node startup finished"), "{log}");
}

/// What a subscriber answers when a callsite registers, in a form the assertions compare.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    Never,
    Sometimes,
    Always,
}

impl From<Interest> for Answer {
    fn from(interest: Interest) -> Self {
        if interest.is_never() {
            return Self::Never;
        }
        if interest.is_always() {
            return Self::Always;
        }
        Self::Sometimes
    }
}

/// The callsite of the span `open` opens. A subscriber that records everything is in scope while
/// it opens, so the span is enabled and carries its callsite's metadata.
fn callsite_of(open: fn() -> Span) -> &'static Metadata<'static> {
    let records_everything = tracing_subscriber::registry();
    tracing::subscriber::with_default(records_everything, || open().metadata())
        .assured("an enabled span carries its callsite's metadata")
}

#[test]
fn the_payload_rule_leaves_the_filter_its_level_and_its_answer_for_every_other_callsite() {
    let other_callsites = [
        callsite_of(|| trace_span!(target: "nervix_server::runtime", "batch_trace")),
        callsite_of(|| debug_span!(target: "nervix_server::runtime", "batch_debug")),
        callsite_of(|| info_span!(target: "nervix_server::application", "startup")),
        callsite_of(|| trace_span!(target: "lapin::returned_messages", "dropped_confirm")),
    ];
    let operator_filters = [
        DEFAULT_TRACE_FILTER,
        "info,nervix_server::runtime=debug",
        "error,[io_loop]=trace",
        "error,lapin::returned_messages[{message}]=warn",
    ];
    for directives in operator_filters {
        let alone = filter_alone(directives);
        let ruled = operator_filter(directives).into_subscriber();

        assert_eq!(
            ruled.max_level_hint(),
            alone.max_level_hint(),
            "the rule changed the level hint of the filter '{directives}'"
        );
        for callsite in other_callsites {
            let expected = Answer::from(alone.register_callsite(callsite));
            let answer = Answer::from(ruled.register_callsite(callsite));
            assert_eq!(
                answer, expected,
                "the rule changed the answer of the filter '{directives}' for {callsite:?}"
            );
        }
    }
}

#[test]
fn a_returned_message_the_filter_would_always_record_is_never_recorded() {
    let returned = callsite_of(|| warn_span!(target: "lapin::returned_messages", "returned"));
    let alone = filter_alone("info");
    let ruled = operator_filter("info").into_subscriber();

    assert_eq!(
        Answer::from(alone.register_callsite(returned)),
        Answer::Always
    );
    assert_eq!(
        Answer::from(ruled.register_callsite(returned)),
        Answer::Never
    );
}

/// A scenario run records through one process subscriber, and the `log` bridge hands that
/// subscriber every record a dependency writes through the `log` crate up to the most verbose
/// level the filter selects for any target, to be judged one record at a time. Cranelift writes
/// `trace` records throughout every module it compiles, so a single `trace` directive in the
/// scenario default made each compile of the scenario WASM guest about six times as long, which on
/// a loaded runner outlasted the budget a runtime revision has to become prepared and ready.
#[test]
fn the_scenario_default_selects_nothing_more_verbose_than_debug() {
    let directives = LogFilter::scenario_default();
    let most_verbose = operator_filter(&directives)
        .into_subscriber()
        .max_level_hint()
        .assured("a filter of static directives states its most verbose level");
    assert!(
        most_verbose <= LevelFilter::DEBUG,
        "the scenario default selects {most_verbose}, so the log bridge hands the filter every \
         dependency record up to that level: '{directives}'"
    );
}

/// The vote and pre-vote boundaries and the receiver contexts a failed election is read for stay
/// in the scenario log, at the level the scenario default selects.
#[test]
fn the_scenario_default_records_election_requests_and_their_receivers() {
    let directives = LogFilter::scenario_default();
    let log = recorded(operator_filter(&directives).into_subscriber(), || {
        debug!(target: "nervix_consensus", peer = %"node-2", "sending Raft vote request");
        debug!(target: "nervix_consensus", peer = %"node-2", "Raft vote request completed");
        let receiver =
            debug_span!(target: "nervix_consensus", "receive_raft_vote", node = %"node-1");
        let _entered = receiver.enter();
        info!(target: "openraft::raft::api::protocol", "Raft::vote(): rpc");
    });

    assert!(log.contains("sending Raft vote request"), "{log}");
    assert!(log.contains("Raft vote request completed"), "{log}");
    assert!(
        log.contains("receive_raft_vote{node=node-1}") && log.contains("Raft::vote(): rpc"),
        "{log}"
    );
}
