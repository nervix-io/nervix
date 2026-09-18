//! Focused regression tests for the in-process test harness's node-liveness owner.
//!
//! Outside the layer order: a harness test crate.
//!
//! - **Owns.** Registration of the focused node-liveness regressions with Rust's test runner.
//! - **Depends on.** The node-liveness harness module and its private test fixtures.
//! - **Must not know.** Scenario state or production node lifecycle policy.

#[path = "common/node_liveness.rs"]
mod node_liveness;

mod tests {
    use std::{
        future, io,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use error_stack::Report;
    use meticulous::ResultExt as _;
    use nervix_client_core::{ClientError, CommandOutcomeKind, Diagnostic};
    use nervix_models::ClusterNodeName;
    use nervix_server::application::AppError;
    use tokio::{sync::oneshot, time::timeout};
    use triomphe::Arc;

    use crate::node_liveness::{
        LastReadinessOutcome, NodeStartupFailure, NodeTaskState, NodeTaskTerminalOutcome,
        NodeTaskWaitOutcome, OwnedNodeTask, ReadinessConnectionFailure, ReadinessProbeOutcome,
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_POLL_INTERVAL: Duration = Duration::from_millis(1);

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn non_ready_response(message: &str) -> ReadinessProbeOutcome {
        ReadinessProbeOutcome::UnsuccessfulResponse {
            response_kind: CommandOutcomeKind::Error,
            message: message.to_string(),
            diagnostics: Vec::new(),
        }
    }

    fn test_node(raw: &str) -> ClusterNodeName {
        ClusterNodeName::parse(raw).assured("test node names satisfy the cluster node grammar")
    }

    async fn inspect_terminal(task: &mut OwnedNodeTask) -> NodeTaskState {
        timeout(TEST_TIMEOUT, async {
            loop {
                let state = task.inspect().await;
                if let NodeTaskState::Terminal(_) = state {
                    return state;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .assured("the bounded test task reaches a terminal outcome")
    }

    #[tokio::test]
    async fn running_task_reports_the_last_failed_probe_at_the_deadline() {
        let mut task = OwnedNodeTask::spawn(async {
            future::pending::<()>().await;
            Ok(())
        });
        let attempts = Arc::new(AtomicUsize::new(0));
        let probe_attempts = attempts.clone();

        let node = test_node("node-probe");
        let error = task
            .wait_until_ready(
                &node,
                3,
                Duration::from_millis(15),
                TEST_POLL_INTERVAL,
                move || {
                    probe_attempts.fetch_add(1, Ordering::SeqCst);
                    future::ready(ReadinessProbeOutcome::ConnectionFailed(
                        ReadinessConnectionFailure::Session(ClientError::TlsRequired),
                    ))
                },
            )
            .await
            .expect_err("a node whose probes all fail must reach the startup deadline");

        let diagnostic = error.current_context();
        let rendered = error.to_string();
        assert_eq!(diagnostic.node, node);
        assert_eq!(diagnostic.attempt, 3);
        assert_eq!(diagnostic.failure, NodeStartupFailure::DeadlineExpired);
        assert!(matches!(&diagnostic.task_state, NodeTaskState::Running));
        assert!(matches!(
            &diagnostic.last_readiness,
            LastReadinessOutcome::Observed(ReadinessProbeOutcome::ConnectionFailed(_))
        ));
        assert!(attempts.load(Ordering::SeqCst) >= 1);
        assert!(rendered.contains("node 'node-probe' startup attempt 3 failed after"));
        assert!(rendered.contains("task state: running"));
        assert!(rendered.contains("last readiness outcome: connection/session creation failed"));
        task.abort();
    }

    #[tokio::test]
    async fn clean_application_exit_before_readiness_is_terminal() {
        let mut task = OwnedNodeTask::spawn(async { Ok(()) });
        tokio::task::yield_now().await;
        let node = test_node("node-clean");

        let error = task
            .wait_until_ready(&node, 1, TEST_TIMEOUT, TEST_POLL_INTERVAL, || {
                future::pending::<ReadinessProbeOutcome>()
            })
            .await
            .expect_err("a clean exit before readiness must fail startup");

        let diagnostic = error.current_context();
        assert_eq!(diagnostic.failure, NodeStartupFailure::TaskTerminated);
        assert!(matches!(
            &diagnostic.task_state,
            NodeTaskState::Terminal(outcome)
                if matches!(outcome.as_ref(), NodeTaskTerminalOutcome::CleanApplicationExit)
        ));
    }

    #[tokio::test]
    async fn application_error_before_readiness_is_retained() {
        let mut task = OwnedNodeTask::spawn(async {
            Err(Report::new(AppError::MissingGrpcHttpsListenAddress))
        });
        tokio::task::yield_now().await;
        let node = test_node("node-error");

        let error = task
            .wait_until_ready(&node, 2, TEST_TIMEOUT, TEST_POLL_INTERVAL, || {
                future::ready(non_ready_response("starting"))
            })
            .await
            .expect_err("an application error before readiness must fail startup");

        assert!(matches!(
            &error.current_context().task_state,
            NodeTaskState::Terminal(outcome)
                if matches!(
                    outcome.as_ref(),
                    NodeTaskTerminalOutcome::ApplicationError(report)
                        if matches!(report.current_context(), AppError::MissingGrpcHttpsListenAddress)
                )
        ));
        let Some(application_error) = task.application_error() else {
            panic!("the terminal task must retain its application error");
        };
        assert!(matches!(
            application_error.current_context(),
            AppError::MissingGrpcHttpsListenAddress
        ));
    }

    #[tokio::test]
    async fn panic_and_cancellation_have_distinct_terminal_outcomes() {
        let mut panicked = OwnedNodeTask::spawn(async {
            panic!("intentional node task panic");
        });
        assert!(matches!(
            inspect_terminal(&mut panicked).await,
            NodeTaskState::Terminal(outcome)
                if matches!(outcome.as_ref(), NodeTaskTerminalOutcome::Panic(_))
        ));

        let mut cancelled =
            OwnedNodeTask::spawn(async { future::pending::<Result<(), Report<AppError>>>().await });
        let OwnedNodeTask::Running(cancelled_handle) = &cancelled else {
            panic!("the cancellation fixture must own a running task");
        };
        cancelled_handle.abort();
        assert!(matches!(
            inspect_terminal(&mut cancelled).await,
            NodeTaskState::Terminal(outcome)
                if matches!(outcome.as_ref(), NodeTaskTerminalOutcome::Cancellation(_))
        ));
    }

    #[test]
    fn readiness_outcomes_retain_typed_causes_and_non_ready_response() {
        let options = ReadinessProbeOutcome::ConnectionFailed(ReadinessConnectionFailure::Options(
            io::Error::other("missing test CA"),
        ));
        assert!(matches!(
            options,
            ReadinessProbeOutcome::ConnectionFailed(ReadinessConnectionFailure::Options(_))
        ));

        let connection = ReadinessProbeOutcome::ConnectionFailed(
            ReadinessConnectionFailure::Session(ClientError::TlsRequired),
        );
        assert!(matches!(
            connection,
            ReadinessProbeOutcome::ConnectionFailed(ReadinessConnectionFailure::Session(
                ClientError::TlsRequired
            ))
        ));

        let command = ReadinessProbeOutcome::CommandFailed(ClientError::SessionClosed);
        assert!(matches!(
            command,
            ReadinessProbeOutcome::CommandFailed(ClientError::SessionClosed)
        ));

        let response = ReadinessProbeOutcome::UnsuccessfulResponse {
            response_kind: CommandOutcomeKind::Error,
            message: "cluster status unavailable".to_string(),
            diagnostics: vec![Diagnostic {
                message: "consensus is starting".to_string(),
                span_start: 0,
                span_end: 0,
            }],
        };
        assert!(matches!(
            response,
            ReadinessProbeOutcome::UnsuccessfulResponse {
                response_kind: CommandOutcomeKind::Error,
                ref message,
                ref diagnostics,
            } if message == "cluster status unavailable"
                && diagnostics[0].message == "consensus is starting"
        ));

        let ready = ReadinessProbeOutcome::Ready {
            response_kind: CommandOutcomeKind::NotLeader,
        };
        assert!(ready.is_ready());
    }

    #[tokio::test]
    async fn stop_and_drop_paths_use_the_task_inspected_for_diagnostics() {
        let completed_drops = Arc::new(AtomicUsize::new(0));
        let completed_guard = completed_drops.clone();
        let mut completed = OwnedNodeTask::spawn(async move {
            let _guard = DropCount(completed_guard);
            Ok(())
        });
        let NodeTaskState::Terminal(inspected_outcome) = inspect_terminal(&mut completed).await
        else {
            panic!("the completed task must be terminal when inspected");
        };
        let NodeTaskWaitOutcome::AlreadyObserved(stopped_outcome) =
            completed.wait(TEST_TIMEOUT).await
        else {
            panic!("stopping after inspection must reuse the observed terminal outcome");
        };
        let NodeTaskState::Terminal(retained_outcome) = completed.state() else {
            panic!("stopping an inspected task must retain its terminal outcome");
        };
        assert!(std::ptr::eq(
            inspected_outcome.as_ref(),
            stopped_outcome.as_ref()
        ));
        assert!(std::ptr::eq(
            inspected_outcome.as_ref(),
            retained_outcome.as_ref()
        ));
        assert_eq!(completed_drops.load(Ordering::SeqCst), 1);

        let aborted_drops = Arc::new(AtomicUsize::new(0));
        let aborted_guard = aborted_drops.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let mut running = OwnedNodeTask::spawn(async move {
            let _guard = DropCount(aborted_guard);
            started_tx
                .send(())
                .assured("the test retains the startup receiver until the task begins");
            future::pending::<()>().await;
            Ok(())
        });
        started_rx
            .await
            .assured("the running task sends after installing its drop guard");
        assert!(matches!(running.inspect().await, NodeTaskState::Running));
        running.abort();
        timeout(TEST_TIMEOUT, async {
            while aborted_drops.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .assured("aborting the owned task drops its future before the test deadline");
        assert_eq!(aborted_drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn forced_cleanup_aborts_and_joins_the_owned_task_once() {
        let mut not_started = OwnedNodeTask::not_started();
        assert!(matches!(
            not_started.wait(TEST_TIMEOUT).await,
            NodeTaskWaitOutcome::NotStarted
        ));

        let mut completed = OwnedNodeTask::spawn(async { Ok(()) });
        let NodeTaskWaitOutcome::Joined(completed_outcome) = completed.wait(TEST_TIMEOUT).await
        else {
            panic!("waiting for a clean task must join its terminal outcome");
        };
        assert!(matches!(
            completed_outcome.as_ref(),
            NodeTaskTerminalOutcome::CleanApplicationExit
        ));

        let drops = Arc::new(AtomicUsize::new(0));
        let task_guard = drops.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let mut task = OwnedNodeTask::spawn(async move {
            let _guard = DropCount(task_guard);
            started_tx
                .send(())
                .assured("the forced-cleanup receiver remains alive until the task begins");
            future::pending::<()>().await;
            Ok(())
        });
        assert!(task.is_running());
        started_rx
            .await
            .assured("the forced-cleanup task sends after installing its drop guard");

        let NodeTaskWaitOutcome::AbortedAtDeadline(first_outcome) = task.wait(Duration::ZERO).await
        else {
            panic!("a pending task with an expired deadline must be aborted and joined");
        };
        assert!(matches!(
            first_outcome.as_ref(),
            NodeTaskTerminalOutcome::Cancellation(_)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let NodeTaskWaitOutcome::AlreadyObserved(observed_outcome) = task.wait(TEST_TIMEOUT).await
        else {
            panic!("a second cleanup must reuse the retained terminal outcome");
        };
        let NodeTaskState::Terminal(second_outcome) = task.state() else {
            panic!("a second cleanup must leave the retained terminal outcome available");
        };
        assert!(std::ptr::eq(
            first_outcome.as_ref(),
            observed_outcome.as_ref()
        ));
        assert!(std::ptr::eq(
            first_outcome.as_ref(),
            second_outcome.as_ref()
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
