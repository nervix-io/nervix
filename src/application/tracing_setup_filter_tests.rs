//! Ordinary-mode evidence for what the node's log filter lets a subscriber record.
//!
//! Outside the layer order: tests may name any layer and product code may not name them.
//!
//! - **Owns.** Filters written as an operator writes `RUST_LOG`, the node's default, and
//!   assertions over what a subscriber deciding with them records.
//! - **Depends on.** The tracing owner and the captured-logs test support.
//! - **Must not know.** What a connector does with a message its broker returned.
//!
//! Every event these tests write goes to a capturing subscriber, so no callsite is first reached
//! while nothing is interested in it. Each case also records through the configured filter alone,
//! which proves the filter would select the returned message the rule keeps out.

use meticulous::ResultExt as _;
use tracing::{debug, info, info_span, trace, warn};
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, layer::SubscriberExt as _};

use super::{DEFAULT_TRACE_FILTER, LogFilter};
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

/// What a subscriber deciding with `filter` records of what `write` logs.
fn recorded(filter: impl Layer<Registry> + Send + Sync + 'static, write: impl FnOnce()) -> String {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(logs.clone()).with_ansi(false));
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
    let unruled = recorded(operator_filter(directives).configured, write);
    assert!(
        unruled.contains(RETURNED_MESSAGE),
        "the filter '{directives}' alone does not select Lapin's returned message:\n{unruled}"
    );
    let ruled = recorded(operator_filter(directives).into_layer(), write);
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
