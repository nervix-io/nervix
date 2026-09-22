//! Focused regression tests for the liveness owners of the in-process test harness.
//!
//! Outside the layer order: a harness test crate.
//!
//! - **Owns.** Registration of the focused node-liveness, phase-deadline, status-request,
//!   cluster-teardown and scenario-phase regressions with Rust's test runner, and the stand-in
//!   nodes those regressions talk to.
//! - **Depends on.** The node-liveness, phase-deadline, status-request, cluster-teardown and
//!   scenario-phase harness modules, and the generated session service they send status requests
//!   to.
//! - **Must not know.** Scenario state or production node lifecycle policy.

#[path = "common/cluster_teardown.rs"]
mod cluster_teardown;
#[path = "common/node_liveness.rs"]
mod node_liveness;
#[path = "common/phase_deadline.rs"]
mod phase_deadline;
#[path = "common/scenario_phase.rs"]
mod scenario_phase;
#[path = "common/status_request.rs"]
mod status_request;

mod tests {
    use std::{
        collections::BTreeMap,
        future,
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use error_stack::Report;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_core::{
        CommandOutcomeKind,
        proto::{
            CommandResult, CommandResultKind, Diagnostic, SessionRequest, SessionResponse,
            UploadResourceRequest, UploadResourceResponse,
            session_response::Event,
            session_service_server::{SessionService, SessionServiceServer},
        },
    };
    use nervix_models::ClusterNodeName;
    use nervix_recovery::NoReceiver as _;
    use nervix_server::application::AppError;
    use parking_lot::Mutex;
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    use tempfile::TempDir;
    use tokio::{
        net::TcpListener,
        sync::{Notify, mpsc, oneshot},
        task::JoinHandle,
        time::{Instant, timeout},
    };
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::{Request, Response, Status, Streaming, transport::Server};
    use triomphe::Arc;

    use crate::{
        cluster_teardown::{ClusterTeardown, TeardownNode},
        node_liveness::{
            LastReadinessOutcome, NodeStartupFailure, NodeTaskState, NodeTaskTerminalOutcome,
            NodeTaskWaitOutcome, OwnedNodeTask, ReadinessProbeOutcome,
        },
        phase_deadline::{BeforeDeadline, PhaseDeadline},
        scenario_phase::{ActiveScenario, ActiveScenarioRegistration, ScenarioPhase},
        status_request::{
            STATUS_REQUEST_TIMEOUT, STATUS_WAIT_BUDGET, StatusEndpoint, StatusOperation,
            StatusRequestError, StatusTransport,
        },
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const TEST_POLL_INTERVAL: Duration = Duration::from_millis(1);
    /// The cleanup budget the teardown regressions give a cluster. It is long enough that one
    /// budget and one budget per node are unmistakably different, and every regression that uses
    /// it runs on a paused clock, so no wall-clock time is spent reaching it.
    const TEST_TEARDOWN_BUDGET: Duration = Duration::from_secs(60);
    /// The bound a cleanup that shares one budget stays under and a cleanup that spends one budget
    /// per node cannot: the first ends one budget after it started, the second three.
    const TEST_SHARED_TEARDOWN_BOUND: Duration = match TEST_TEARDOWN_BUDGET.checked_mul(2) {
        Some(bound) => bound,
        None => panic!("the shared cleanup bound must fit in Duration"),
    };
    /// A poll interval like the one the harness status waits use.
    const STATUS_POLL_INTERVAL: Duration = Duration::from_millis(200);
    /// How long a request to a stalled stand-in may take. It leaves a loaded machine ample time to
    /// connect and open a loopback session first, so the stall is reached in the operation each
    /// regression names.
    const STALLED_REQUEST_BUDGET: Duration = Duration::from_secs(2);
    /// How long a regression waits for a stalled request to end. Only a request that ignored its
    /// phase deadline and ran to the status request timeout reaches it.
    const STALLED_REQUEST_GUARD: Duration = Duration::from_secs(8);
    const _: () = assert!(
        STALLED_REQUEST_BUDGET.as_nanos() < STALLED_REQUEST_GUARD.as_nanos()
            && STALLED_REQUEST_GUARD.as_nanos() < STATUS_REQUEST_TIMEOUT.as_nanos(),
        "a stalled-request regression must tell its phase deadline from the request timeout"
    );
    const TEST_AUTHORIZATION: &str = "Basic c3RhbmQtaW46c3RhbmQtaW4=";
    const HEALTHY_STATUS: &str = "raft.state: Leader\nraft.current_leader: node-1";

    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// How a stand-in node answers the status sessions these regressions open against it.
    #[derive(Clone)]
    enum StandInBehavior {
        /// Answers the session's command with this result.
        Answer(CommandResult),
        /// Answers the session's command once `gate` opens.
        AnswerAfter {
            result: CommandResult,
            gate: Arc<Notify>,
        },
        /// Never returns from session establishment.
        WithholdSession,
        /// Receives the session's command, signals `received`, and never answers it.
        WithholdResponse { received: Arc<Notify> },
        /// Keeps the response stream ready with events that are unrelated to the command result.
        FloodResponses,
        /// Refuses every session.
        RejectSession,
        /// Fails the response stream instead of answering.
        FailResponse,
        /// Ends the session without answering.
        EndSession,
    }

    impl StandInBehavior {
        async fn answer(
            self,
            mut commands: Streaming<SessionRequest>,
            responses: mpsc::Sender<Result<SessionResponse, Status>>,
        ) {
            let Ok(Some(_command)) = commands.message().await else {
                return;
            };
            match self {
                Self::Answer(result) => Self::send_result(&responses, result).await,
                Self::AnswerAfter { result, gate } => {
                    gate.notified().await;
                    Self::send_result(&responses, result).await;
                }
                Self::WithholdResponse { received } => {
                    received.notify_one();
                    future::pending::<()>().await;
                }
                Self::FloodResponses => loop {
                    tokio::task::consume_budget().await;
                    let response = SessionResponse { event: None };
                    if responses.send(Ok(response)).await.is_err() {
                        return;
                    }
                },
                Self::FailResponse => responses
                    .send(Err(Status::internal("the stand-in fails every response")))
                    .await
                    .means_peer_left("the status request under test"),
                Self::EndSession | Self::WithholdSession | Self::RejectSession => {}
            }
        }

        async fn send_result(
            responses: &mpsc::Sender<Result<SessionResponse, Status>>,
            result: CommandResult,
        ) {
            let response = SessionResponse {
                event: Some(Event::Result(Box::new(result))),
            };
            responses
                .send(Ok(response))
                .await
                .means_peer_left("the status request under test");
        }
    }

    #[derive(Clone)]
    struct StandInService {
        behavior: StandInBehavior,
    }

    #[tonic::async_trait]
    impl SessionService for StandInService {
        type SessionStream = ReceiverStream<Result<SessionResponse, Status>>;

        async fn session(
            &self,
            request: Request<Streaming<SessionRequest>>,
        ) -> Result<Response<Self::SessionStream>, Status> {
            match &self.behavior {
                StandInBehavior::WithholdSession => future::pending::<()>().await,
                StandInBehavior::RejectSession => {
                    return Err(Status::unauthenticated(
                        "the stand-in refuses every session",
                    ));
                }
                StandInBehavior::Answer(_)
                | StandInBehavior::AnswerAfter { .. }
                | StandInBehavior::WithholdResponse { .. }
                | StandInBehavior::FloodResponses
                | StandInBehavior::FailResponse
                | StandInBehavior::EndSession => {}
            }
            let (response_tx, response_rx) = mpsc::channel(4);
            tokio::spawn(
                self.behavior
                    .clone()
                    .answer(request.into_inner(), response_tx),
            );
            Ok(Response::new(ReceiverStream::new(response_rx)))
        }

        async fn upload_resource(
            &self,
            _request: Request<Streaming<UploadResourceRequest>>,
        ) -> Result<Response<UploadResourceResponse>, Status> {
            Err(Status::unimplemented("stand-in nodes accept no uploads"))
        }
    }

    /// A session service standing in for a node on a loopback port, served until it is dropped.
    struct StandInNode {
        address: SocketAddr,
        server: JoinHandle<()>,
    }

    impl StandInNode {
        async fn serve(behavior: StandInBehavior) -> Self {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the loopback interface accepts a test listener");
            let address = listener
                .local_addr()
                .assured("a bound listener has an address");
            let service = SessionServiceServer::new(StandInService { behavior });
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .assured("the stand-in serves until the test drops it");
            });
            Self { address, server }
        }

        fn endpoint(&self) -> StatusEndpoint {
            plaintext_endpoint(self.address)
        }
    }

    impl Drop for StandInNode {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    fn plaintext_endpoint(address: SocketAddr) -> StatusEndpoint {
        StatusEndpoint::new(
            address,
            StatusTransport::Plaintext,
            TEST_AUTHORIZATION.to_string(),
        )
    }

    fn command_result(kind: CommandResultKind, message: &str) -> CommandResult {
        CommandResult {
            success: kind == CommandResultKind::Ok,
            kind: i32::from(kind),
            message: message.to_string(),
            ..CommandResult::default()
        }
    }

    /// A loopback address nothing listens on, so connecting to it is refused.
    async fn refused_address() -> SocketAddr {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the loopback interface accepts a test listener");
        listener
            .local_addr()
            .assured("a bound listener has an address")
    }

    /// A self-signed certificate authority for a TLS status endpoint.
    fn test_authority(directory: &TempDir) -> PathBuf {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().assured("the test key algorithm is supported");
        let certificate = params
            .self_signed(&key)
            .assured("a default certificate with a fresh key signs itself");
        let path = directory.path().join("authority.pem");
        std::fs::write(&path, certificate.pem()).assured("the test directory is writable");
        path
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

    fn deadline_passed_during(
        error: &Report<StatusRequestError>,
        expected: StatusOperation,
    ) -> bool {
        match error.current_context() {
            StatusRequestError::DeadlinePassed { operation, budget } => {
                *operation == expected && *budget <= STALLED_REQUEST_BUDGET
            }
            _ => false,
        }
    }

    /// Sends one status request to a stand-in that stalls, and checks that the request ends at
    /// its deadline while `expected` is the operation still in flight.
    async fn assert_request_ends_at_its_deadline(
        endpoint: &StatusEndpoint,
        expected: StatusOperation,
    ) {
        let started = Instant::now();
        let error = timeout(
            STALLED_REQUEST_GUARD,
            endpoint.request(PhaseDeadline::after(STALLED_REQUEST_BUDGET)),
        )
        .await
        .assured("a stalled status request ends at its phase deadline")
        .expect_err("a stalled status request must not produce a result");
        assert!(started.elapsed() >= STALLED_REQUEST_BUDGET);
        assert!(
            deadline_passed_during(&error, expected),
            "expected the deadline to pass during {expected}, got {error:#}"
        );
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
                PhaseDeadline::after(Duration::from_millis(15)),
                TEST_POLL_INTERVAL,
                move |_phase| {
                    probe_attempts.fetch_add(1, Ordering::SeqCst);
                    future::ready(ReadinessProbeOutcome::RequestFailed(Report::new(
                        StatusRequestError::SessionEnded,
                    )))
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
            LastReadinessOutcome::Observed(ReadinessProbeOutcome::RequestFailed(_))
        ));
        assert!(attempts.load(Ordering::SeqCst) >= 1);
        assert!(rendered.contains("node 'node-probe' startup attempt 3 failed after"));
        assert!(rendered.contains("task state: running"));
        assert!(rendered.contains(
            "last readiness outcome: status request failed: the status session ended before the \
             command result arrived"
        ));
        task.abort();
    }

    #[tokio::test]
    async fn clean_application_exit_before_readiness_is_terminal() {
        let mut task = OwnedNodeTask::spawn(async { Ok(()) });
        tokio::task::yield_now().await;
        let node = test_node("node-clean");

        let error = task
            .wait_until_ready(
                &node,
                1,
                PhaseDeadline::after(TEST_TIMEOUT),
                TEST_POLL_INTERVAL,
                |_phase| future::pending::<ReadinessProbeOutcome>(),
            )
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
            .wait_until_ready(
                &node,
                2,
                PhaseDeadline::after(TEST_TIMEOUT),
                TEST_POLL_INTERVAL,
                |_phase| future::ready(non_ready_response("starting")),
            )
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

    #[tokio::test]
    async fn readiness_and_status_outcomes_retain_their_typed_cause() {
        let ready = StandInNode::serve(StandInBehavior::Answer(command_result(
            CommandResultKind::Ok,
            HEALTHY_STATUS,
        )))
        .await;
        let not_leader = StandInNode::serve(StandInBehavior::Answer(command_result(
            CommandResultKind::NotLeader,
            "not the leader",
        )))
        .await;
        let mut unavailable_result =
            command_result(CommandResultKind::Error, "cluster status unavailable");
        unavailable_result.diagnostics = vec![Diagnostic {
            message: "consensus is starting".to_string(),
            span_start: 0,
            span_end: 0,
        }];
        let unavailable = StandInNode::serve(StandInBehavior::Answer(unavailable_result)).await;
        let rejecting = StandInNode::serve(StandInBehavior::RejectSession).await;
        let failing = StandInNode::serve(StandInBehavior::FailResponse).await;
        let ending = StandInNode::serve(StandInBehavior::EndSession).await;
        let refused = plaintext_endpoint(refused_address().await);
        let phase = PhaseDeadline::after(STALLED_REQUEST_GUARD);

        assert!(matches!(
            ReadinessProbeOutcome::probe(&ready.endpoint(), phase).await,
            ReadinessProbeOutcome::Ready {
                response_kind: CommandOutcomeKind::Ok
            }
        ));
        assert!(matches!(
            ReadinessProbeOutcome::probe(&not_leader.endpoint(), phase).await,
            ReadinessProbeOutcome::Ready {
                response_kind: CommandOutcomeKind::NotLeader
            }
        ));
        let unsuccessful = ReadinessProbeOutcome::probe(&unavailable.endpoint(), phase).await;
        assert!(matches!(
            unsuccessful,
            ReadinessProbeOutcome::UnsuccessfulResponse {
                response_kind: CommandOutcomeKind::Error,
                ref message,
                ref diagnostics,
            } if message == "cluster status unavailable"
                && diagnostics[0].message == "consensus is starting"
        ));

        let ReadinessProbeOutcome::RequestFailed(rejected) =
            ReadinessProbeOutcome::probe(&rejecting.endpoint(), phase).await
        else {
            panic!("a refused session must fail the readiness request");
        };
        assert!(matches!(
            rejected.current_context(),
            StatusRequestError::OpenSession(status) if status.code() == tonic::Code::Unauthenticated
        ));
        let ReadinessProbeOutcome::RequestFailed(failed) =
            ReadinessProbeOutcome::probe(&failing.endpoint(), phase).await
        else {
            panic!("a failed response stream must fail the readiness request");
        };
        assert!(matches!(
            failed.current_context(),
            StatusRequestError::ReceiveResponse(status) if status.code() == tonic::Code::Internal
        ));
        let ReadinessProbeOutcome::RequestFailed(ended) =
            ReadinessProbeOutcome::probe(&ending.endpoint(), phase).await
        else {
            panic!("a session that ends unanswered must fail the readiness request");
        };
        assert!(matches!(
            ended.current_context(),
            StatusRequestError::SessionEnded
        ));
        let refused_outcome = ReadinessProbeOutcome::probe(&refused, phase).await;
        let ReadinessProbeOutcome::RequestFailed(connection) = &refused_outcome else {
            panic!("a refused connection must fail the readiness request");
        };
        assert!(matches!(
            connection.current_context(),
            StatusRequestError::Connect(_)
        ));
        assert!(
            refused_outcome
                .to_string()
                .starts_with("status request failed: the status endpoint connection failed: ")
        );

        let status_error = unavailable
            .endpoint()
            .cluster_status(phase)
            .await
            .expect_err("an unsuccessful status result is not a status");
        assert!(matches!(
            status_error.current_context(),
            StatusRequestError::Unsuccessful {
                kind: CommandOutcomeKind::Error,
                message,
                diagnostics,
            } if message == "cluster status unavailable" && diagnostics.len() == 1
        ));
        assert_eq!(
            ready
                .endpoint()
                .cluster_status(phase)
                .await
                .assured("a healthy stand-in reports its status"),
            HEALTHY_STATUS
        );
    }

    #[tokio::test]
    async fn status_request_ends_at_its_deadline_while_the_connection_is_pending() {
        // A listener that never accepts still completes the TCP handshake, so a TLS connection to
        // it waits for a server hello that never comes.
        let silent = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the loopback interface accepts a test listener");
        let directory = tempfile::tempdir().assured("the test can create a temporary directory");
        let endpoint = StatusEndpoint::new(
            silent
                .local_addr()
                .assured("a bound listener has an address"),
            StatusTransport::Tls {
                authority: test_authority(&directory),
            },
            TEST_AUTHORIZATION.to_string(),
        );

        assert_request_ends_at_its_deadline(&endpoint, StatusOperation::Connect).await;
    }

    #[tokio::test]
    async fn status_request_ends_at_its_deadline_while_session_establishment_is_pending() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;

        assert_request_ends_at_its_deadline(&stalled.endpoint(), StatusOperation::OpenSession)
            .await;
    }

    #[tokio::test]
    async fn status_request_ends_at_its_deadline_while_the_response_is_pending() {
        let received = Arc::new(Notify::new());
        let stalled = StandInNode::serve(StandInBehavior::WithholdResponse {
            received: received.clone(),
        })
        .await;

        assert_request_ends_at_its_deadline(&stalled.endpoint(), StatusOperation::ReceiveResponse)
            .await;
        timeout(TEST_TIMEOUT, received.notified())
            .await
            .assured("the stalled stand-in received the status command");
    }

    #[tokio::test]
    async fn status_request_ends_at_its_deadline_while_unrelated_responses_remain_ready() {
        let flooding = StandInNode::serve(StandInBehavior::FloodResponses).await;

        assert_request_ends_at_its_deadline(&flooding.endpoint(), StatusOperation::ReceiveResponse)
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn nested_deadline_never_outlives_its_phase() {
        let phase = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        assert_eq!(
            phase.nested(STATUS_REQUEST_TIMEOUT).budget(),
            STATUS_REQUEST_TIMEOUT
        );

        let late = STATUS_WAIT_BUDGET
            .checked_sub(Duration::from_secs(1))
            .assured("the status wait budget is longer than one second");
        tokio::time::advance(late).await;
        assert_eq!(
            phase.nested(STATUS_REQUEST_TIMEOUT).budget(),
            Duration::from_secs(1)
        );
        let BeforeDeadline::Passed = phase.bound(future::pending::<()>()).await else {
            panic!("an operation that never finishes must stop at the phase deadline");
        };
        assert!(phase.has_passed());
        assert!(phase.elapsed() >= STATUS_WAIT_BUDGET);
        assert_eq!(phase.remaining(), Duration::ZERO);
        assert_eq!(
            phase.nested(STATUS_REQUEST_TIMEOUT).budget(),
            Duration::ZERO
        );
    }

    #[tokio::test(start_paused = true)]
    async fn status_wait_ends_at_its_original_deadline_when_the_final_request_never_replies() {
        // The first requests reply at once with a status the wait does not accept; every later
        // request never replies and ends only at its own deadline, the way a status request to a
        // node that stopped answering does.
        const REPLYING_REQUESTS: usize = 3;
        let requests = AtomicUsize::new(0);
        let wait = PhaseDeadline::after(STATUS_WAIT_BUDGET);

        let waited = wait
            .poll_until(
                STATUS_POLL_INTERVAL,
                |phase| {
                    let request = requests.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if request < REPLYING_REQUESTS {
                            return Ok("raft.current_leader: (none)");
                        }
                        let request_deadline = phase.nested(STATUS_REQUEST_TIMEOUT);
                        StatusOperation::ReceiveResponse
                            .within(request_deadline, future::pending::<&'static str>())
                            .await
                    }
                },
                |status| status.contains("node-1"),
            )
            .await;

        let Err(expired) = waited else {
            panic!("a node that never reports a leader must not satisfy the wait");
        };
        assert!(wait.elapsed() >= STATUS_WAIT_BUDGET);
        assert!(
            wait.elapsed() < STATUS_WAIT_BUDGET + STATUS_POLL_INTERVAL,
            "the wait must end at its original deadline, not one request timeout later: {:?}",
            wait.elapsed()
        );
        assert_eq!(expired.last_output, Some("raft.current_leader: (none)"));
        let Some(last_failure) = expired.last_failure else {
            panic!("the final request must be the wait's last failure");
        };
        let StatusRequestError::DeadlinePassed { operation, budget } =
            last_failure.current_context()
        else {
            panic!("the final request must end at its deadline, got {last_failure:#}");
        };
        assert_eq!(*operation, StatusOperation::ReceiveResponse);
        assert!(
            *budget < STATUS_REQUEST_TIMEOUT,
            "the final request must receive only what was left of the wait"
        );
        // Four stalled requests fit in the wait, the last one cut short by its deadline.
        assert_eq!(requests.load(Ordering::SeqCst), REPLYING_REQUESTS + 4);
    }

    #[tokio::test]
    async fn startup_readiness_failure_reports_the_timed_out_status_operation() {
        let received = Arc::new(Notify::new());
        let stalled = StandInNode::serve(StandInBehavior::WithholdResponse {
            received: received.clone(),
        })
        .await;
        let endpoint = stalled.endpoint();
        let mut task = OwnedNodeTask::spawn(async {
            future::pending::<()>().await;
            Ok(())
        });
        let node = test_node("node-stalled");

        let started = Instant::now();
        let error = timeout(
            STALLED_REQUEST_GUARD,
            task.wait_until_ready(
                &node,
                1,
                PhaseDeadline::after(STALLED_REQUEST_BUDGET),
                TEST_POLL_INTERVAL,
                |phase| ReadinessProbeOutcome::probe(&endpoint, phase),
            ),
        )
        .await
        .assured("readiness polling ends at its startup deadline")
        .expect_err("a node that never answers its readiness probe must fail startup");

        assert!(started.elapsed() >= STALLED_REQUEST_BUDGET);
        let diagnostic = error.current_context();
        assert_eq!(diagnostic.failure, NodeStartupFailure::DeadlineExpired);
        assert!(matches!(&diagnostic.task_state, NodeTaskState::Running));
        let LastReadinessOutcome::Observed(ReadinessProbeOutcome::RequestFailed(probe_error)) =
            &diagnostic.last_readiness
        else {
            panic!(
                "the readiness context must hold the timed-out probe, got {}",
                diagnostic.last_readiness
            );
        };
        assert!(deadline_passed_during(
            probe_error,
            StatusOperation::ReceiveResponse
        ));
        assert!(error.to_string().contains(
            "last readiness outcome: status request failed: response receive was still pending \
             when the"
        ));
        task.abort();
    }

    #[tokio::test]
    async fn status_snapshots_keep_a_healthy_node_while_another_node_stalls() {
        // The healthy node answers only after the stalled node has received its command, so it
        // can report only if both requests are in flight at once.
        let stalled_received = Arc::new(Notify::new());
        let stalled = StandInNode::serve(StandInBehavior::WithholdResponse {
            received: stalled_received.clone(),
        })
        .await;
        let healthy = StandInNode::serve(StandInBehavior::AnswerAfter {
            result: command_result(CommandResultKind::Ok, HEALTHY_STATUS),
            gate: stalled_received,
        })
        .await;
        let endpoints = BTreeMap::from([
            ("node-1".to_string(), healthy.endpoint()),
            ("node-2".to_string(), stalled.endpoint()),
        ]);

        let snapshots = timeout(
            STALLED_REQUEST_GUARD,
            StatusEndpoint::cluster_statuses(
                &endpoints,
                PhaseDeadline::after(STALLED_REQUEST_BUDGET),
            ),
        )
        .await
        .assured("status snapshots end at their diagnostic deadline");

        let Some(Ok(healthy_status)) = snapshots.get("node-1") else {
            panic!("the healthy node's snapshot must be retained: {snapshots:?}");
        };
        assert_eq!(healthy_status, HEALTHY_STATUS);
        let Some(Err(stalled_error)) = snapshots.get("node-2") else {
            panic!("the stalled node's snapshot must be its timeout: {snapshots:?}");
        };
        assert!(deadline_passed_during(
            stalled_error,
            StatusOperation::ReceiveResponse
        ));
    }

    #[tokio::test]
    async fn failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;
        let healthy = StandInNode::serve(StandInBehavior::Answer(command_result(
            CommandResultKind::Ok,
            HEALTHY_STATUS,
        )))
        .await;
        let endpoints = BTreeMap::from([
            ("node-1".to_string(), healthy.endpoint()),
            ("node-2".to_string(), stalled.endpoint()),
            (
                "node-3".to_string(),
                plaintext_endpoint(refused_address().await),
            ),
        ]);
        let (cleanup_tx, cleanup_rx) = oneshot::channel();

        let teardown = async {
            let snapshots = StatusEndpoint::cluster_statuses(
                &endpoints,
                PhaseDeadline::after(STALLED_REQUEST_BUDGET),
            )
            .await;
            cleanup_tx
                .send(Instant::now())
                .assured("the test holds the cleanup receiver until teardown ends");
            snapshots
        };
        let started = Instant::now();
        let snapshots = timeout(STALLED_REQUEST_GUARD, teardown)
            .await
            .assured("diagnostics end by their deadline, so cleanup starts");
        let cleanup_started = cleanup_rx
            .await
            .assured("teardown starts cleanup after its diagnostics");

        assert!(cleanup_started.duration_since(started) >= STALLED_REQUEST_BUDGET);
        assert!(matches!(snapshots.get("node-1"), Some(Ok(status)) if status == HEALTHY_STATUS));
        let Some(Err(stalled_error)) = snapshots.get("node-2") else {
            panic!("the stalled node's snapshot must be its timeout: {snapshots:?}");
        };
        assert!(deadline_passed_during(
            stalled_error,
            StatusOperation::OpenSession
        ));
        let Some(Err(refused_error)) = snapshots.get("node-3") else {
            panic!("the refused node's snapshot must be its failure: {snapshots:?}");
        };
        assert!(matches!(
            refused_error.current_context(),
            StatusRequestError::Connect(_)
        ));
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
            completed.wait(PhaseDeadline::after(TEST_TIMEOUT)).await
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

    /// How a stand-in node's task ends once cleanup has asked it to stop.
    #[derive(Clone, Copy, Debug)]
    enum StandInEnding {
        StopsWhenAsked,
        FailsWhenAsked,
        PanicsWhenAsked,
        /// Ignores the stop request, so only the cleanup deadline can end it.
        NeverStops,
    }

    impl StandInEnding {
        /// Whether `outcome` is how a task that ends this way ends.
        fn ended_as(self, outcome: &NodeTaskTerminalOutcome) -> bool {
            match self {
                Self::StopsWhenAsked => {
                    matches!(outcome, NodeTaskTerminalOutcome::CleanApplicationExit)
                }
                Self::FailsWhenAsked => matches!(
                    outcome,
                    NodeTaskTerminalOutcome::ApplicationError(error)
                        if matches!(
                            error.current_context(),
                            AppError::MissingGrpcHttpsListenAddress
                        )
                ),
                Self::PanicsWhenAsked => matches!(outcome, NodeTaskTerminalOutcome::Panic(_)),
                // A task the cleanup deadline aborted is joined for the cancellation it left.
                Self::NeverStops => matches!(outcome, NodeTaskTerminalOutcome::Cancellation(_)),
            }
        }
    }

    /// What the stand-in nodes of one cleanup did, in the order they did it.
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum CleanupEvent {
        StopRequested {
            node: String,
        },
        TaskEnded {
            node: String,
        },
        Released {
            node: String,
            /// The phase the scenario published while this node was releasing, when the node was
            /// given a registration to read.
            phase: Option<ScenarioPhase>,
        },
    }

    #[derive(Debug, Default)]
    struct CleanupLog {
        events: Mutex<Vec<CleanupEvent>>,
    }

    impl CleanupLog {
        fn record(&self, event: CleanupEvent) {
            self.events.lock().push(event);
        }

        fn events(&self) -> Vec<CleanupEvent> {
            self.events.lock().clone()
        }

        fn stop_was_requested(&self, node: &str) -> bool {
            self.events().iter().any(|event| {
                matches!(event, CleanupEvent::StopRequested { node: requested } if requested == node)
            })
        }

        fn phase_while_releasing(&self, node: &str) -> Option<ScenarioPhase> {
            self.events().into_iter().find_map(|event| match event {
                CleanupEvent::Released {
                    node: released,
                    phase,
                } if released == node => phase,
                _ => None,
            })
        }

        /// Whether every node gave its harness state back only once every task had ended.
        fn released_only_after_every_task_ended(&self) -> bool {
            let events = self.events();
            let last_end = events
                .iter()
                .rposition(|event| matches!(event, CleanupEvent::TaskEnded { .. }));
            let first_release = events
                .iter()
                .position(|event| matches!(event, CleanupEvent::Released { .. }));
            let Some(last_end) = last_end else {
                return false;
            };
            let Some(first_release) = first_release else {
                return false;
            };
            last_end < first_release
        }

        fn released(&self) -> Vec<String> {
            self.events()
                .into_iter()
                .filter_map(|event| match event {
                    CleanupEvent::Released { node, .. } => Some(node),
                    _ => None,
                })
                .collect()
        }
    }

    /// Records that a node's task ended, whether it returned, failed, panicked or was aborted.
    struct TaskEnd {
        node: String,
        log: Arc<CleanupLog>,
    }

    impl Drop for TaskEnd {
        fn drop(&mut self) {
            self.log.record(CleanupEvent::TaskEnded {
                node: self.node.clone(),
            });
        }
    }

    /// A node standing in for a cluster node during cleanup: it records the stop request it was
    /// given and the harness state it gave back, and its task ends the way the regression asked.
    struct StandInTeardownNode {
        name: String,
        stop: Arc<Notify>,
        task: OwnedNodeTask,
        log: Arc<CleanupLog>,
        scenario: Option<Arc<ActiveScenarioRegistration>>,
    }

    impl StandInTeardownNode {
        fn new(name: &str, ending: StandInEnding, log: &Arc<CleanupLog>) -> Self {
            let stop = Arc::new(Notify::new());
            let task_stop = stop.clone();
            let task_end = TaskEnd {
                node: name.to_string(),
                log: log.clone(),
            };
            let task = OwnedNodeTask::spawn(async move {
                let _ended = task_end;
                match ending {
                    StandInEnding::NeverStops => {
                        future::pending::<()>().await;
                        Ok(())
                    }
                    StandInEnding::StopsWhenAsked => {
                        task_stop.notified().await;
                        Ok(())
                    }
                    StandInEnding::FailsWhenAsked => {
                        task_stop.notified().await;
                        Err(Report::new(AppError::MissingGrpcHttpsListenAddress))
                    }
                    StandInEnding::PanicsWhenAsked => {
                        task_stop.notified().await;
                        panic!("intentional stand-in node panic");
                    }
                }
            });
            Self {
                name: name.to_string(),
                stop,
                task,
                log: log.clone(),
                scenario: None,
            }
        }

        /// Reads the phase `scenario` publishes while this node releases, so a regression can tell
        /// what a reader of the registry would have seen during cleanup.
        fn reading(mut self, scenario: &Arc<ActiveScenarioRegistration>) -> Self {
            self.scenario = Some(scenario.clone());
            self
        }
    }

    impl TeardownNode for StandInTeardownNode {
        fn node_name(&self) -> String {
            self.name.clone()
        }

        fn request_stop(&mut self) {
            self.log.record(CleanupEvent::StopRequested {
                node: self.name.clone(),
            });
            self.stop.notify_one();
        }

        fn owned_task(&mut self) -> &mut OwnedNodeTask {
            &mut self.task
        }

        fn release(&mut self) {
            let phase = self
                .scenario
                .as_ref()
                .map(|scenario| published(scenario).phase);
            self.log.record(CleanupEvent::Released {
                node: self.name.clone(),
                phase,
            });
        }
    }

    /// What the registry publishes for one scenario, read the way a suite watchdog reads it.
    fn published(scenario: &ActiveScenarioRegistration) -> ActiveScenario {
        ActiveScenario::active()
            .into_iter()
            .find(|active| &active.identity == scenario.identity())
            .verified("a registered scenario is published until its registration is dropped")
    }

    fn stand_in_cluster(
        nodes: &[(&str, StandInEnding)],
        log: &Arc<CleanupLog>,
    ) -> Vec<StandInTeardownNode> {
        nodes
            .iter()
            .map(|(name, ending)| StandInTeardownNode::new(name, *ending, log))
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_single_node_cleanup_keeps_how_its_task_ended() {
        for ending in [
            StandInEnding::StopsWhenAsked,
            StandInEnding::FailsWhenAsked,
            StandInEnding::PanicsWhenAsked,
        ] {
            let log = Arc::new(CleanupLog::default());
            let mut nodes = stand_in_cluster(&[("node-1", ending)], &log);

            let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;

            assert!(log.stop_was_requested("node-1"));
            assert!(
                teardown.elapsed < TEST_TEARDOWN_BUDGET,
                "a node that stops when asked must not reach the cleanup deadline: {teardown}"
            );
            assert!(!teardown.was_forced(), "{teardown}");
            let [node] = teardown.nodes.as_slice() else {
                panic!("a cluster of one reports one node: {teardown}");
            };
            let NodeTaskWaitOutcome::Joined(outcome) = &node.stop else {
                panic!("a node that ends itself must be joined, not forced: {node}");
            };
            assert!(
                ending.ended_as(outcome.as_ref()),
                "cleanup must keep how the task of a node that {ending:?} ended, got {outcome}"
            );
            assert_eq!(log.released(), vec!["node-1".to_string()]);
            assert!(
                log.released_only_after_every_task_ended(),
                "{:?}",
                log.events()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_node_is_the_only_cleanup_failure_a_three_node_cluster_reports() {
        let log = Arc::new(CleanupLog::default());
        let mut nodes = stand_in_cluster(
            &[
                ("node-1", StandInEnding::StopsWhenAsked),
                ("node-2", StandInEnding::FailsWhenAsked),
                ("node-3", StandInEnding::PanicsWhenAsked),
            ],
            &log,
        );

        let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;

        assert!(!teardown.was_forced(), "{teardown}");
        let panicked = teardown
            .panics()
            .map(|node| node.node.clone())
            .collect::<Vec<_>>();
        assert_eq!(panicked, vec!["node-3".to_string()]);
        assert_eq!(
            log.released(),
            vec![
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string()
            ]
        );
        assert!(
            log.released_only_after_every_task_ended(),
            "{:?}",
            log.events()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn three_stuck_nodes_spend_one_cleanup_budget_rather_than_three() {
        let log = Arc::new(CleanupLog::default());
        let mut nodes = stand_in_cluster(
            &[
                ("node-1", StandInEnding::NeverStops),
                ("node-2", StandInEnding::NeverStops),
                ("node-3", StandInEnding::NeverStops),
            ],
            &log,
        );

        let started = Instant::now();
        let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;
        let spent = started.elapsed();

        assert!(spent >= TEST_TEARDOWN_BUDGET, "{teardown}");
        assert!(
            spent < TEST_SHARED_TEARDOWN_BOUND,
            "three stuck nodes must share one cleanup budget, but cleanup took {spent:?}: \
             {teardown}"
        );
        for node in &teardown.nodes {
            assert!(log.stop_was_requested(&node.node));
            let NodeTaskWaitOutcome::AbortedAtDeadline(outcome) = &node.stop else {
                panic!("a node that never stops must be aborted at the deadline: {node}");
            };
            assert!(
                StandInEnding::NeverStops.ended_as(outcome.as_ref()),
                "an aborted node task must be joined for its outcome: {outcome}"
            );
        }
        assert_eq!(teardown.forced().count(), 3, "{teardown}");
        assert_eq!(log.released().len(), 3);
        assert!(
            log.released_only_after_every_task_ended(),
            "harness state must be given back only once every task has ended: {:?}",
            log.events()
        );
    }

    #[tokio::test]
    async fn a_stalled_diagnostic_still_reaches_every_node_stop() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;
        let healthy = StandInNode::serve(StandInBehavior::Answer(command_result(
            CommandResultKind::Ok,
            HEALTHY_STATUS,
        )))
        .await;
        let endpoints = BTreeMap::from([
            ("node-1".to_string(), healthy.endpoint()),
            ("node-2".to_string(), stalled.endpoint()),
        ]);
        let log = Arc::new(CleanupLog::default());
        let mut nodes = stand_in_cluster(
            &[
                ("node-1", StandInEnding::StopsWhenAsked),
                ("node-2", StandInEnding::StopsWhenAsked),
            ],
            &log,
        );

        let started = Instant::now();
        let snapshots = StatusEndpoint::cluster_statuses(
            &endpoints,
            PhaseDeadline::after(STALLED_REQUEST_BUDGET),
        )
        .await;
        let diagnostics_ended = started.elapsed();
        let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;

        assert!(
            diagnostics_ended >= STALLED_REQUEST_BUDGET,
            "the stalled diagnostic must run out its own budget before cleanup continues"
        );
        assert!(matches!(snapshots.get("node-1"), Some(Ok(status)) if status == HEALTHY_STATUS));
        let Some(Err(stalled_error)) = snapshots.get("node-2") else {
            panic!("the stalled node's snapshot must be its timeout: {snapshots:?}");
        };
        assert!(deadline_passed_during(
            stalled_error,
            StatusOperation::OpenSession
        ));
        for node in &teardown.nodes {
            assert!(
                log.stop_was_requested(&node.node),
                "a stalled diagnostic must not keep a node from being asked to stop: {node}"
            );
        }
        assert!(!teardown.was_forced(), "{teardown}");
        assert_eq!(log.released().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_finished_phase_is_published_only_once_cleanup_has_completed() {
        let scenario = Arc::new(ActiveScenarioRegistration::start(
            "Harness liveness",
            "cleanup publishes truthful phases",
        ));
        let log = Arc::new(CleanupLog::default());
        let mut nodes = [
            StandInTeardownNode::new("node-1", StandInEnding::NeverStops, &log).reading(&scenario),
        ];

        scenario.enter(ScenarioPhase::BodyComplete);
        scenario.enter(ScenarioPhase::TeardownStarted);
        scenario.enter(ScenarioPhase::Diagnostics);
        scenario.enter(ScenarioPhase::Stopping);
        let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;
        assert_eq!(published(&scenario).phase, ScenarioPhase::Stopping);
        scenario.enter(ScenarioPhase::Finished);

        assert!(teardown.was_forced(), "{teardown}");
        assert_eq!(
            log.phase_while_releasing("node-1"),
            Some(ScenarioPhase::Stopping),
            "cleanup that is still giving state back must not publish that it has finished"
        );
        assert_eq!(published(&scenario).phase, ScenarioPhase::Finished);
    }

    #[tokio::test(start_paused = true)]
    async fn an_active_scenario_publishes_its_phase_and_the_age_of_that_phase() {
        let scenario = ActiveScenarioRegistration::start("Harness liveness", "phase ages");
        let queued = published(&scenario);
        assert_eq!(&queued.identity, scenario.identity());
        assert_eq!(queued.phase, ScenarioPhase::Queued);
        assert_eq!(queued.identity.feature, "Harness liveness");
        assert_eq!(queued.identity.scenario, "phase ages");

        // A scenario that has not reached its first step ages in the phase it is waiting in.
        tokio::time::advance(Duration::from_secs(30)).await;
        let waiting = published(&scenario);
        assert_eq!(waiting.phase, ScenarioPhase::Queued);
        assert_eq!(waiting.phase_age(), Duration::from_secs(30));
        assert_eq!(waiting.age(), Duration::from_secs(30));

        let started = scenario.enter(ScenarioPhase::Body);
        assert_eq!(started.phase, ScenarioPhase::Body);
        assert_eq!(started.phase_age(), Duration::ZERO);
        assert_eq!(started.age(), Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(5)).await;
        let running = published(&scenario);
        assert_eq!(running.phase, ScenarioPhase::Body);
        assert_eq!(running.phase_age(), Duration::from_secs(5));
        assert_eq!(running.age(), Duration::from_secs(35));

        let identity = queued.identity.clone();
        drop(scenario);
        assert!(
            !ActiveScenario::active()
                .iter()
                .any(|active| active.identity == identity),
            "a scenario whose world is dropped must leave the registry"
        );
    }

    #[tokio::test]
    async fn forced_cleanup_aborts_and_joins_the_owned_task_once() {
        let mut not_started = OwnedNodeTask::not_started();
        assert!(matches!(
            not_started.wait(PhaseDeadline::after(TEST_TIMEOUT)).await,
            NodeTaskWaitOutcome::NotStarted
        ));

        let mut completed = OwnedNodeTask::spawn(async { Ok(()) });
        let NodeTaskWaitOutcome::Joined(completed_outcome) =
            completed.wait(PhaseDeadline::after(TEST_TIMEOUT)).await
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

        let NodeTaskWaitOutcome::AbortedAtDeadline(first_outcome) =
            task.wait(PhaseDeadline::after(Duration::ZERO)).await
        else {
            panic!("a pending task with an expired deadline must be aborted and joined");
        };
        assert!(matches!(
            first_outcome.as_ref(),
            NodeTaskTerminalOutcome::Cancellation(_)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let NodeTaskWaitOutcome::AlreadyObserved(observed_outcome) =
            task.wait(PhaseDeadline::after(TEST_TIMEOUT)).await
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
