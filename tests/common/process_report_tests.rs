//! Regressions for reading why a server process's startup ended from the report it wrote.
//!
//! Layer: test harness.
//! - **Owns.** Logs a launch can leave behind, rendered by the same `error-stack` reports and the
//!   same `Error: ` prefix a node's `main` writes, and the checks that the reader finds the
//!   outermost context and that the node-startup classification names the listener it lost.
//! - **Depends on.** The process-report reader, the node-startup classification and the
//!   application error a node returns.
//! - **Must not know.** How a launch spawns a process or waits for its readiness.

use std::io;

use error_stack::Report;
use nervix_server::application::AppError;

use crate::{node_startup::StartupPorts, process_report::reported_context};

/// The line a tracing subscriber writes before a node's report, which the reader passes over.
const TRACING_LINE: &str = "2026-10-19T21:00:00.000000Z  INFO nervix_server: starting node";

/// What a node's `main` writes when it returns `report`, the way a Rust program reports the error
/// its `main` returned.
fn written_report(report: &Report<AppError>) -> String {
    format!("Error: {report:?}\n")
}

/// The report a node writes when binding a listener failed because another socket holds its port.
fn lost_bind_report(error: AppError) -> Report<AppError> {
    Report::new(io::Error::from(io::ErrorKind::AddrInUse)).change_context(error)
}

#[test]
fn a_report_names_its_outermost_context_without_its_emphasis() {
    let log = format!(
        "{TRACING_LINE}\n{}",
        written_report(&lost_bind_report(AppError::BindWebConsoleListenAddress))
    );

    assert_eq!(
        reported_context(&log),
        Some(AppError::BindWebConsoleListenAddress.to_string())
    );
}

#[test]
fn the_last_report_a_launch_wrote_is_the_one_read() {
    let log = format!(
        "{}{TRACING_LINE}\n{}",
        written_report(&Report::new(AppError::OpenRegistry)),
        written_report(&lost_bind_report(AppError::BindGrpcListenAddress))
    );

    assert_eq!(
        reported_context(&log),
        Some(AppError::BindGrpcListenAddress.to_string())
    );
}

#[test]
fn a_log_without_a_report_names_no_context() {
    let panicked = "thread 'main' panicked at src/main.rs:1:1:\nexplicit panic\n";

    assert_eq!(reported_context(""), None);
    assert_eq!(reported_context(&format!("{TRACING_LINE}\n")), None);
    assert_eq!(reported_context(panicked), None);
}

#[test]
fn control_sequences_are_removed_and_the_text_between_them_is_kept() {
    let emphasised = "Error: \u{1b}[1mfailed to bind\u{1b}[22m the \u{1b}[3;1maddress\u{1b}[0m\n";
    let lone_escape = "Error: lone \u{1b}escape\n";
    let plain = "Error: failed to open registry\n";

    assert_eq!(
        reported_context(emphasised),
        Some("failed to bind the address".to_string())
    );
    assert_eq!(
        reported_context(lone_escape),
        Some("lone escape".to_string())
    );
    assert_eq!(
        reported_context(plain),
        Some("failed to open registry".to_string())
    );
}

#[test]
fn a_report_of_a_lost_bind_names_the_listener_the_next_launch_moves() {
    for (error, ports) in [
        (AppError::BindGrpcListenAddress, StartupPorts::Grpc),
        (AppError::BindHttpListenAddress, StartupPorts::Http),
        (AppError::BindHttpsListenAddress, StartupPorts::Https),
        (
            AppError::BindObservabilityListenAddress,
            StartupPorts::Observability,
        ),
        (
            AppError::BindWebConsoleListenAddress,
            StartupPorts::WebConsole,
        ),
        (
            AppError::BindWebConsoleHttpsListenAddress,
            StartupPorts::WebConsole,
        ),
        (AppError::StartInterconnect, StartupPorts::Interconnect),
    ] {
        let rendered = error.to_string();
        let context = reported_context(&written_report(&lost_bind_report(error)));

        assert_eq!(context.as_deref(), Some(rendered.as_str()));
        assert_eq!(
            StartupPorts::lost_in_report(&rendered),
            Some(ports),
            "the report '{rendered}' names a listener another process bound first"
        );
    }
}

#[test]
fn a_report_of_any_other_failure_names_no_listener() {
    for error in [
        AppError::OpenRegistry,
        AppError::LoadInterconnectTls,
        AppError::ParseWebConsoleListenAddress,
    ] {
        let rendered = error.to_string();
        let context = reported_context(&written_report(&Report::new(error)));

        assert_eq!(context.as_deref(), Some(rendered.as_str()));
        assert_eq!(
            StartupPorts::lost_in_report(&rendered),
            None,
            "the report '{rendered}' is the node's decision, which a fresh launch repeats"
        );
    }
}
