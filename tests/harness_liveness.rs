//! Focused regression tests for the liveness owners of the in-process test harness.
//!
//! Outside the layer order: a harness test crate.
//!
//! - **Owns.** Registration of the focused node-liveness, node-startup, phase-deadline,
//!   status-request, port-pool, cluster-teardown, scenario-phase and suite-watchdog regressions
//!   with Rust's test runner, and the stand-in nodes those regressions talk to.
//! - **Depends on.** The node-liveness, node-startup, phase-deadline, status-request, port-pool,
//!   cluster-teardown, scenario-phase and suite-watchdog harness modules, and the generated
//!   session service they send status requests to.
//! - **Must not know.** Scenario state or production node lifecycle policy.

#[path = "common/cluster_teardown.rs"]
mod cluster_teardown;
#[path = "common/node_liveness.rs"]
mod node_liveness;
#[path = "common/node_startup.rs"]
mod node_startup;
#[path = "common/phase_deadline.rs"]
mod phase_deadline;
#[path = "common/port_pool.rs"]
mod port_pool;
#[path = "common/scenario_phase.rs"]
mod scenario_phase;
#[path = "common/status_request.rs"]
mod status_request;
#[path = "common/suite_watchdog.rs"]
mod suite_watchdog;

mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        future, io,
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::{
            Arc as StdArc, LazyLock,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use clap::Parser as _;
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
    use tokio_util::sync::CancellationToken;
    use tonic::{Request, Response, Status, Streaming, transport::Server};
    use triomphe::Arc;

    use crate::{
        cluster_teardown::{ClusterTeardown, TeardownNode},
        node_liveness::{
            LastReadinessOutcome, NodeStartupError, NodeStartupFailure, NodeTaskState,
            NodeTaskTerminalOutcome, NodeTaskWaitOutcome, OwnedNodeTask, ReadinessProbeOutcome,
        },
        node_startup::{
            ATTEMPT_READINESS_BUDGET, AttemptCleanup, AttemptFailure, FULL_LENGTH_ATTEMPTS,
            NODE_START_ATTEMPTS, NODE_STARTUP_BUDGET, NodeStartup, NodeStartupExhausted,
            StartableNode, StartupEnd, StartupRetry, cluster_startup_budget,
        },
        phase_deadline::{BeforeDeadline, PhaseDeadline},
        port_pool::{
            PORT_DRAW_LIMIT, PortPoolError, next_port, next_ports, release_test_ports, reserve,
        },
        scenario_phase::{
            ActiveScenario, ActiveScenarioRegistration, ScenarioIdentity, ScenarioPhase,
        },
        status_request::{
            STATUS_REQUEST_TIMEOUT, STATUS_WAIT_BUDGET, StatusEndpoint, StatusOperation,
            StatusRequestError, StatusTransport,
        },
        suite_watchdog::{
            DEPENDENCY_SHUTDOWN_BUDGET, LiveCluster, LiveClusterHandle, LiveClusterRegistration,
            NodeStop, SUITE_BUDGET, StalledScenario, SuiteOutcome, SuiteRun, SuiteTeardown,
            SuiteTimeout, SuiteWatchdog, SuiteWatchdogArgs,
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
    /// Where a stand-in scenario claims to begin in its feature file. The registry keys attempts
    /// by identity, and a scenario's line is part of that identity, so every regression that must
    /// be told apart from another gives its own line rather than sharing this one.
    const STAND_IN_SCENARIO_LINE: usize = 1;
    const TEST_AUTHORIZATION: &str = "Basic c3RhbmQtaW46c3RhbmQtaW4=";
    const HEALTHY_STATUS: &str = "raft.state: Leader\nraft.current_leader: node-1";
    /// How often a startup regression probes its stand-in. The startup regressions run on a
    /// paused clock, so this only decides how many probes one attempt performs.
    const STARTUP_POLL_INTERVAL: Duration = Duration::from_secs(1);
    /// What a startup regression allows beyond the budget it asserts. A paused clock advances
    /// only for a timer, so the slack covers the work between them.
    const STARTUP_BUDGET_SLACK: Duration = Duration::from_secs(1);
    /// The longest one node's startup may take before a regression calls its budget broken.
    const STARTUP_BUDGET_CEILING: Duration =
        match NODE_STARTUP_BUDGET.checked_add(STARTUP_BUDGET_SLACK) {
            Some(ceiling) => ceiling,
            None => panic!("the startup budget and its regression slack must fit in Duration"),
        };

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

    /// How the node a startup regression drives behaves on one launch.
    #[derive(Debug)]
    enum LaunchBehavior {
        /// The launch itself fails, the way the harness's own configuration does.
        LaunchFails,
        /// The node runs, never answers a readiness probe, and stops when asked.
        NeverReady,
        /// The node runs, never answers a readiness probe, and ignores the stop request.
        NeverReadyAndNeverStops,
        /// The node's task ends at once with this application error.
        ApplicationError(AppError),
        /// The node's task panics at once.
        Panics,
        /// The node answers its first readiness probe.
        Ready,
    }

    /// A node the startup owner can drive, behaving on each launch the way its regression scripts.
    struct StandInStartupNode {
        node: ClusterNodeName,
        behaviors: VecDeque<LaunchBehavior>,
        task: OwnedNodeTask,
        stop: CancellationToken,
        answers_readiness: bool,
        ports_available: bool,
        launches: u32,
        ports_moved: u32,
    }

    impl StandInStartupNode {
        fn new(node: &str, behaviors: impl IntoIterator<Item = LaunchBehavior>) -> Self {
            Self {
                node: test_node(node),
                behaviors: behaviors.into_iter().collect(),
                task: OwnedNodeTask::not_started(),
                stop: CancellationToken::new(),
                answers_readiness: false,
                ports_available: true,
                launches: 0,
                ports_moved: 0,
            }
        }

        fn without_spare_ports(mut self) -> Self {
            self.ports_available = false;
            self
        }

        /// Starts this node the way the harness starts one node of its own, and returns the
        /// exhaustion diagnostic when it never becomes ready.
        async fn start_within(
            &mut self,
            budget: PhaseDeadline,
        ) -> Result<(), Report<NodeStartupExhausted>> {
            let node = self.node.clone();
            NodeStartup::start(&node, self, budget).await
        }
    }

    impl StartableNode for StandInStartupNode {
        fn launch(&mut self) -> io::Result<()> {
            self.launches += 1;
            let behavior = self
                .behaviors
                .pop_front()
                .assured("a startup regression scripts one behavior for every launch it allows");
            self.answers_readiness = matches!(behavior, LaunchBehavior::Ready);
            self.stop = CancellationToken::new();
            let stop = self.stop.clone();
            self.task = match behavior {
                LaunchBehavior::LaunchFails => {
                    return Err(io::Error::other("the stand-in node cannot be launched"));
                }
                LaunchBehavior::ApplicationError(error) => {
                    OwnedNodeTask::spawn(async move { Err(Report::new(error)) })
                }
                LaunchBehavior::Panics => {
                    OwnedNodeTask::spawn(async { panic!("intentional stand-in node panic") })
                }
                LaunchBehavior::NeverReadyAndNeverStops => OwnedNodeTask::spawn(async {
                    future::pending::<()>().await;
                    Ok(())
                }),
                LaunchBehavior::NeverReady | LaunchBehavior::Ready => {
                    OwnedNodeTask::spawn(async move {
                        stop.cancelled().await;
                        Ok(())
                    })
                }
            };
            Ok(())
        }

        async fn readiness(
            &mut self,
            attempt: u32,
            readiness: PhaseDeadline,
        ) -> error_stack::Result<(), NodeStartupError> {
            let node = self.node.clone();
            let answers = self.answers_readiness;
            self.task
                .wait_until_ready(
                    &node,
                    attempt,
                    readiness,
                    STARTUP_POLL_INTERVAL,
                    move |_| {
                        future::ready(if answers {
                            ReadinessProbeOutcome::Ready {
                                response_kind: CommandOutcomeKind::Ok,
                            }
                        } else {
                            ReadinessProbeOutcome::RequestFailed(Report::new(
                                StatusRequestError::SessionEnded,
                            ))
                        })
                    },
                )
                .await
        }

        async fn clean_up(&mut self, cleanup: PhaseDeadline) -> AttemptCleanup {
            self.stop.cancel();
            AttemptCleanup::from(self.task.wait(cleanup).await)
        }

        fn move_to_fresh_ports(&mut self) -> io::Result<()> {
            if !self.ports_available {
                return Err(io::Error::other("the port pool is exhausted"));
            }
            self.ports_moved += 1;
            Ok(())
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

    /// The attempt limit as the number of attempts an exhausted startup records.
    fn attempt_limit() -> usize {
        usize::try_from(NODE_START_ATTEMPTS).assured("the attempt limit is a small count")
    }

    /// The attempts the budget pays for at full length, as a number of recorded attempts.
    fn full_length_attempts() -> usize {
        usize::try_from(FULL_LENGTH_ATTEMPTS).assured("the full-length attempts are a small count")
    }

    /// How the startup owner classified each attempt it spent, in the order they ran.
    fn attempt_retries(exhausted: &NodeStartupExhausted) -> Vec<StartupRetry> {
        exhausted
            .attempts
            .spent
            .iter()
            .map(|attempt| attempt.retry)
            .collect()
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
    async fn stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three() {
        for node_count in [1_usize, 3] {
            let log = Arc::new(CleanupLog::default());
            let names = (1..=node_count)
                .map(|index| format!("node-{index}"))
                .collect::<Vec<_>>();
            let stuck = names
                .iter()
                .map(|name| (name.as_str(), StandInEnding::NeverStops))
                .collect::<Vec<_>>();
            let mut nodes = stand_in_cluster(&stuck, &log);

            let started = Instant::now();
            let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), TEST_TEARDOWN_BUDGET).await;
            let spent = started.elapsed();

            assert!(spent >= TEST_TEARDOWN_BUDGET, "{teardown}");
            assert!(
                spent < TEST_SHARED_TEARDOWN_BOUND,
                "{node_count} stuck node(s) must share one cleanup budget, but cleanup took \
                 {spent:?}: {teardown}"
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
            assert_eq!(teardown.forced().count(), node_count, "{teardown}");
            assert_eq!(log.released().len(), node_count);
            assert!(
                log.released_only_after_every_task_ended(),
                "harness state must be given back only once every task has ended: {:?}",
                log.events()
            );
        }
    }

    /// How a node of a diagnostics regression answers the status request its cleanup sends.
    #[derive(Clone, Copy, Debug)]
    enum StandInDiagnostic {
        Answers,
        Stalls,
    }

    #[tokio::test]
    async fn a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;
        let healthy = StandInNode::serve(StandInBehavior::Answer(command_result(
            CommandResultKind::Ok,
            HEALTHY_STATUS,
        )))
        .await;
        let clusters: [&[(&str, StandInDiagnostic)]; 2] = [
            &[("node-1", StandInDiagnostic::Stalls)],
            &[
                ("node-1", StandInDiagnostic::Answers),
                ("node-2", StandInDiagnostic::Stalls),
                ("node-3", StandInDiagnostic::Answers),
            ],
        ];

        for cluster in clusters {
            let mut endpoints = BTreeMap::new();
            for (name, diagnostic) in cluster {
                let endpoint = match diagnostic {
                    StandInDiagnostic::Answers => healthy.endpoint(),
                    StandInDiagnostic::Stalls => stalled.endpoint(),
                };
                endpoints.insert((*name).to_string(), endpoint);
            }
            let log = Arc::new(CleanupLog::default());
            let endings = cluster
                .iter()
                .map(|(name, _)| (*name, StandInEnding::StopsWhenAsked))
                .collect::<Vec<_>>();
            let mut nodes = stand_in_cluster(&endings, &log);

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
            for (name, diagnostic) in cluster {
                match diagnostic {
                    StandInDiagnostic::Answers => assert!(
                        matches!(snapshots.get(*name), Some(Ok(status)) if status == HEALTHY_STATUS),
                        "a healthy node's snapshot must be kept beside a stalled one: \
                         {snapshots:?}"
                    ),
                    StandInDiagnostic::Stalls => {
                        let Some(Err(stalled_error)) = snapshots.get(*name) else {
                            panic!(
                                "the stalled node's snapshot must be its timeout: {snapshots:?}"
                            );
                        };
                        assert!(deadline_passed_during(
                            stalled_error,
                            StatusOperation::OpenSession
                        ));
                    }
                }
            }
            for node in &teardown.nodes {
                assert!(
                    log.stop_was_requested(&node.node),
                    "a stalled diagnostic must not keep a node from being asked to stop: {node}"
                );
            }
            assert!(!teardown.was_forced(), "{teardown}");
            assert_eq!(log.released().len(), cluster.len());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_finished_phase_is_published_only_once_cleanup_has_completed() {
        let scenario = Arc::new(ActiveScenarioRegistration::start(
            "Harness liveness",
            "cleanup publishes truthful phases",
            STAND_IN_SCENARIO_LINE,
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
        let scenario = ActiveScenarioRegistration::start(
            "Harness liveness",
            "phase ages",
            STAND_IN_SCENARIO_LINE,
        );
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

    #[tokio::test(start_paused = true)]
    async fn repeated_readiness_failure_spends_one_budget_across_every_attempt() {
        let mut node = StandInStartupNode::new(
            "node-unready",
            [
                LaunchBehavior::NeverReady,
                LaunchBehavior::NeverReady,
                LaunchBehavior::NeverReady,
            ],
        );

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a node that never answers a probe must exhaust its startup");

        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::AttemptsSpent));
        assert_eq!(exhausted.budget, NODE_STARTUP_BUDGET);
        assert_eq!(node.launches, NODE_START_ATTEMPTS);
        assert_eq!(node.ports_moved, NODE_START_ATTEMPTS - 1);
        assert_eq!(exhausted.attempts.spent.len(), attempt_limit());
        assert_eq!(
            attempt_retries(exhausted),
            vec![StartupRetry::Transient; attempt_limit()]
        );
        for attempt in &exhausted.attempts.spent {
            let AttemptFailure::NotReady(readiness) = &attempt.failure else {
                panic!(
                    "every attempt must end in its readiness: {}",
                    attempt.failure
                );
            };
            assert_eq!(
                readiness.current_context().failure,
                NodeStartupFailure::DeadlineExpired
            );
        }
        let (last, earlier) = exhausted
            .attempts
            .spent
            .split_last()
            .assured("an exhausted startup records the attempts it spent");
        for attempt in earlier {
            assert!(
                matches!(attempt.cleanup, AttemptCleanup::Stopped(_)),
                "a node asked to stop while the budget remains must stop: {}",
                attempt.cleanup
            );
        }
        // The last attempt polls readiness to the end of the budget, so its cleanup has nothing
        // left to wait with and aborts the node task at once.
        assert!(
            matches!(last.cleanup, AttemptCleanup::AbortedAtDeadline(_)),
            "cleanup after the budget is spent must abort rather than wait: {}",
            last.cleanup
        );
        assert!(
            exhausted.elapsed >= NODE_STARTUP_BUDGET,
            "every attempt the budget admits must be spent: {:?}",
            exhausted.elapsed
        );
        assert!(
            exhausted.elapsed <= STARTUP_BUDGET_CEILING,
            "retries must share one budget rather than restart it: {:?}",
            exhausted.elapsed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_bound_address_is_retried_until_a_launch_becomes_ready() {
        let mut node = StandInStartupNode::new(
            "node-bound",
            [
                LaunchBehavior::ApplicationError(AppError::BindGrpcListenAddress),
                LaunchBehavior::Ready,
            ],
        );
        let budget = PhaseDeadline::after(NODE_STARTUP_BUDGET);

        node.start_within(budget)
            .await
            .assured("a bound address must be retried on fresh ports");

        assert_eq!(node.launches, 2);
        assert_eq!(node.ports_moved, 1);
        assert!(
            budget.elapsed() < ATTEMPT_READINESS_BUDGET,
            "a launch that fails at once must not wait for readiness: {:?}",
            budget.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_last_attempt_still_becomes_ready_with_what_the_budget_left() {
        let mut node = StandInStartupNode::new(
            "node-slow",
            [
                LaunchBehavior::NeverReady,
                LaunchBehavior::NeverReady,
                LaunchBehavior::Ready,
            ],
        );
        let budget = PhaseDeadline::after(NODE_STARTUP_BUDGET);

        node.start_within(budget)
            .await
            .assured("the budget must still admit the launch that becomes ready");

        assert_eq!(node.launches, NODE_START_ATTEMPTS);
        assert_eq!(node.ports_moved, NODE_START_ATTEMPTS - 1);
        assert!(
            budget.elapsed() < NODE_STARTUP_BUDGET,
            "a startup that becomes ready must not spend its whole budget: {:?}",
            budget.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_application_error_ends_the_startup_without_another_launch() {
        let mut node = StandInStartupNode::new(
            "node-registry",
            [LaunchBehavior::ApplicationError(AppError::OpenRegistry)],
        );

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("an application error must end the startup");

        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::TerminalFailure));
        assert_eq!(attempt_retries(exhausted), vec![StartupRetry::Terminal]);
        assert_eq!(node.launches, 1);
        assert_eq!(node.ports_moved, 0);
        assert!(error.to_string().contains("failed to open registry"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_panicking_node_ends_the_startup_without_another_launch() {
        let mut node = StandInStartupNode::new("node-panic", [LaunchBehavior::Panics]);

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a panicking node must end the startup");

        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::TerminalFailure));
        assert_eq!(attempt_retries(exhausted), vec![StartupRetry::Terminal]);
        assert_eq!(node.launches, 1);
        assert!(
            error
                .to_string()
                .contains("intentional stand-in node panic")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_launch_failure_ends_the_startup_before_anything_is_cleaned_up() {
        let mut node = StandInStartupNode::new("node-unlaunchable", [LaunchBehavior::LaunchFails]);

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a node that cannot be launched must end the startup");

        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::TerminalFailure));
        assert_eq!(attempt_retries(exhausted), vec![StartupRetry::Terminal]);
        assert_eq!(node.launches, 1);
        assert_eq!(node.ports_moved, 0);
        let [attempt] = exhausted.attempts.spent.as_slice() else {
            panic!("a launch failure must record exactly one attempt");
        };
        assert!(matches!(attempt.failure, AttemptFailure::Launch(_)));
        assert!(matches!(attempt.cleanup, AttemptCleanup::NothingLaunched));
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_that_never_completes_is_aborted_inside_the_same_budget() {
        let mut node = StandInStartupNode::new(
            "node-unstoppable",
            [
                LaunchBehavior::NeverReadyAndNeverStops,
                LaunchBehavior::NeverReadyAndNeverStops,
                LaunchBehavior::NeverReadyAndNeverStops,
            ],
        );

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a node that never stops must still exhaust one startup budget");

        // Cleanup spends the same budget as readiness, so a node that has to be aborted every time
        // spends the whole budget on the attempts it pays for at full length, and the launch a
        // fast failure would have left room for never starts.
        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::BudgetSpent));
        assert_eq!(node.launches, FULL_LENGTH_ATTEMPTS);
        assert_eq!(exhausted.attempts.spent.len(), full_length_attempts());
        for attempt in &exhausted.attempts.spent {
            assert!(
                matches!(attempt.cleanup, AttemptCleanup::AbortedAtDeadline(_)),
                "a node that ignores its stop must be aborted: {}",
                attempt.cleanup
            );
        }
        assert!(
            exhausted.elapsed <= STARTUP_BUDGET_CEILING,
            "cleanup must spend the startup budget rather than a shutdown watchdog: {:?}",
            exhausted.elapsed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn exhaustion_reports_every_attempt_with_its_typed_cause() {
        let mut node = StandInStartupNode::new(
            "node-history",
            [
                LaunchBehavior::ApplicationError(AppError::BindHttpListenAddress),
                LaunchBehavior::NeverReadyAndNeverStops,
                LaunchBehavior::NeverReady,
            ],
        );

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a node that never answers a probe must exhaust its startup");

        let rendered = error.to_string();
        assert!(
            rendered.contains(&format!(
                "node 'node-history' did not become ready within its {NODE_STARTUP_BUDGET:?} \
                 startup budget: every launch the attempt limit allows was spent"
            )),
            "{rendered}"
        );
        assert!(rendered.contains("attempt 1/3 [transient]"), "{rendered}");
        assert!(
            rendered.contains("failed to bind HTTP listen address"),
            "{rendered}"
        );
        assert!(rendered.contains("attempt 2/3 [transient]"), "{rendered}");
        assert!(
            rendered.contains("cleanup: aborted at the cleanup deadline (cancellation:"),
            "{rendered}"
        );
        assert!(rendered.contains("attempt 3/3 [transient]"), "{rendered}");
        assert!(
            rendered.contains("startup deadline expired; task state: running"),
            "{rendered}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn ports_that_cannot_be_reallocated_end_the_startup() {
        let mut node = StandInStartupNode::new("node-portless", [LaunchBehavior::NeverReady])
            .without_spare_ports();

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a startup without ports for its next launch must end");

        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::PortsUnavailable(_)));
        assert_eq!(node.launches, 1);
        assert_eq!(exhausted.attempts.spent.len(), 1);
        assert!(
            error.to_string().contains("the port pool is exhausted"),
            "{error}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sequential_cluster_construction_stays_inside_its_derived_budget() {
        const CLUSTER_NODES: u32 = 2;
        let construction = PhaseDeadline::after(cluster_startup_budget(CLUSTER_NODES));
        let ceiling = cluster_startup_budget(CLUSTER_NODES)
            .checked_add(STARTUP_BUDGET_SLACK)
            .assured("a two-node construction budget and its slack fit in Duration");

        for index in 1..=CLUSTER_NODES {
            let mut node = StandInStartupNode::new(
                &format!("node-{index}"),
                [
                    LaunchBehavior::NeverReady,
                    LaunchBehavior::NeverReady,
                    LaunchBehavior::NeverReady,
                ],
            );
            let error = node
                .start_within(construction.nested(NODE_STARTUP_BUDGET))
                .await
                .expect_err("a node that never answers a probe must exhaust its startup");
            assert!(
                error.current_context().budget <= NODE_STARTUP_BUDGET,
                "no node may receive more than the per-node budget"
            );
        }

        assert!(
            construction.elapsed() <= ceiling,
            "building a cluster one node at a time must stay inside the derived budget: {:?}",
            construction.elapsed()
        );
    }

    /// Ports the port-pool regressions draw. Nothing binds them, and they sit below the ephemeral
    /// range, so they cannot collide with the loopback ports the stand-in nodes above take from
    /// the operating system. Each regression draws its own so they can run side by side.
    const POOL_REGRESSION_PORTS: [u16; 6] = [1_024, 1_025, 1_026, 1_027, 1_028, 1_029];

    /// The draw limit as a number of draws a regression counts.
    fn draw_limit() -> usize {
        usize::try_from(PORT_DRAW_LIMIT).assured("the draw limit is a small count")
    }

    #[test]
    fn a_draw_that_keeps_landing_on_reserved_ports_ends_at_the_draw_limit() {
        let [taken, fresh, ..] = POOL_REGRESSION_PORTS;
        assert_eq!(
            reserve(1, || Ok(taken)).assured("a port nothing holds is reserved at once"),
            vec![taken]
        );

        // Every draw inside the limit lands on the port already taken, and the draw after the
        // limit would find a fresh one: a pool that keeps drawing past its limit succeeds here,
        // and one that gives up reports exhaustion without ever reaching the fresh port.
        let draws = AtomicUsize::new(0);
        let outcome = reserve(1, || {
            let draw = draws.fetch_add(1, Ordering::Relaxed);
            if draw < draw_limit() {
                Ok(taken)
            } else {
                Ok(fresh)
            }
        });

        let Err(PortPoolError::Exhausted { reserved, misses }) = outcome else {
            panic!("a draw that only finds reserved ports must report exhaustion: {outcome:?}");
        };
        assert_eq!(misses, PORT_DRAW_LIMIT);
        assert!(
            reserved >= 1,
            "the diagnostic names how many ports were held: {reserved}"
        );
        assert_eq!(
            draws.load(Ordering::Relaxed),
            draw_limit(),
            "the draw must stop at the limit rather than spin until a port frees up"
        );
        release_test_ports(&[taken]);
    }

    #[test]
    fn an_exhausted_draw_gives_back_the_ports_it_had_reserved() {
        let [_, _, taken, partial, ..] = POOL_REGRESSION_PORTS;
        reserve(1, || Ok(taken)).assured("a port nothing holds is reserved at once");

        // The first draw of two lands on a fresh port and the rest on the taken one, so the draw
        // ends exhausted holding a port it must not keep.
        let draws = AtomicUsize::new(0);
        let outcome = reserve(2, || {
            let draw = draws.fetch_add(1, Ordering::Relaxed);
            if draw == 0 { Ok(partial) } else { Ok(taken) }
        });
        assert!(
            matches!(outcome, Err(PortPoolError::Exhausted { .. })),
            "{outcome:?}"
        );

        assert_eq!(
            reserve(1, || Ok(partial))
                .assured("the port an exhausted draw gave back is free again"),
            vec![partial]
        );
        release_test_ports(&[taken, partial]);
    }

    #[test]
    fn a_draw_the_operating_system_refuses_is_reported_as_its_own_failure() {
        let [_, _, _, _, reserved_first, ..] = POOL_REGRESSION_PORTS;
        let draws = AtomicUsize::new(0);
        let outcome = reserve(2, || {
            let draw = draws.fetch_add(1, Ordering::Relaxed);
            if draw == 0 {
                Ok(reserved_first)
            } else {
                Err(io::Error::other("no sockets left"))
            }
        });

        let Err(PortPoolError::Draw(error)) = outcome else {
            panic!(
                "a refused draw must be reported as the operating system's failure: {outcome:?}"
            );
        };
        assert_eq!(error.to_string(), "no sockets left");
        assert_eq!(
            reserve(1, || Ok(reserved_first)).assured("a refused draw gives back what it reserved"),
            vec![reserved_first]
        );
        release_test_ports(&[reserved_first]);
    }

    #[test]
    fn ports_drawn_from_the_operating_system_are_distinct_and_reserved() {
        let drawn = next_ports(3).assured("the loopback interface hands out ephemeral ports");
        let extra = next_port().assured("the loopback interface hands out one more port");
        let mut all = drawn.clone();
        all.push(extra);
        let distinct = all.iter().copied().collect::<BTreeSet<u16>>();
        assert_eq!(
            distinct.len(),
            all.len(),
            "every drawn port is distinct: {all:?}"
        );

        // Every port the pool holds is refused to a later draw, however that draw finds it.
        assert!(
            matches!(
                reserve(1, || Ok(extra)),
                Err(PortPoolError::Exhausted { .. })
            ),
            "a port the operating system handed out is held by the pool"
        );

        release_test_ports(&all);
        assert_eq!(
            reserve(1, || Ok(extra)).assured("a released port is drawn again"),
            vec![extra]
        );
        release_test_ports(&[extra]);
    }

    #[test]
    fn a_released_port_can_be_drawn_again() {
        let [.., port] = POOL_REGRESSION_PORTS;
        reserve(1, || Ok(port)).assured("a port nothing holds is reserved at once");
        assert!(
            matches!(
                reserve(1, || Ok(port)),
                Err(PortPoolError::Exhausted { .. })
            ),
            "a port that is still held cannot be drawn again"
        );

        release_test_ports(&[port]);

        assert_eq!(
            reserve(1, || Ok(port)).assured("a released port is drawn again"),
            vec![port]
        );
        release_test_ports(&[port]);
    }

    /// The suite watchdog reads registries the whole process shares and stops every node in them,
    /// so the regressions that drive it run one at a time. Two of them running together would see
    /// each other's scenarios and stop each other's nodes. It is an async mutex because a
    /// regression holds it across the budget it waits out.
    static WATCHDOG_REGRESSIONS: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// A suite budget short enough that a regression reaches its expiry at once. Every regression
    /// that spends it runs on a paused clock, so no wall-clock time is spent reaching it.
    const TEST_SUITE_BUDGET: Duration = Duration::from_secs(30);
    /// The cleanup window a watchdog regression gives every live node to end within. It is
    /// unmistakably shorter than the suite budget, so a regression can tell which of the two a
    /// measured elapsed time belongs to.
    const TEST_CLEANUP_WINDOW: Duration = Duration::from_secs(5);
    const _: () = assert!(
        TEST_CLEANUP_WINDOW.as_nanos() < TEST_SUITE_BUDGET.as_nanos(),
        "a watchdog regression must tell its cleanup window from its suite budget"
    );

    /// Whether a stand-in node ends when the watchdog asks it to.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum StandInStopBehavior {
        /// The node ends as soon as it is asked, the way a healthy node does.
        StopsWhenAsked,
        /// The node ignores the request, the way a wedged node does.
        IgnoresTheRequest,
    }

    /// The stop a stand-in node publishes to the live-cluster registry.
    struct StandInNodeStop {
        stop: CancellationToken,
        requests: Arc<AtomicUsize>,
    }

    impl NodeStop for StandInNodeStop {
        fn request_stop(&self) {
            self.requests.fetch_add(1, Ordering::Relaxed);
            self.stop.cancel();
        }
    }

    /// A node the watchdog can reach: it publishes itself to the live-cluster registry for as long
    /// as its task runs, and it ends only the way the regression asked.
    struct StandInLiveNode {
        task: OwnedNodeTask,
        requests: Arc<AtomicUsize>,
    }

    impl StandInLiveNode {
        fn start(cluster: &LiveClusterHandle, name: &str, behavior: StandInStopBehavior) -> Self {
            let requests = Arc::new(AtomicUsize::new(0));
            let stop = CancellationToken::new();
            let published = StdArc::new(StandInNodeStop {
                stop: stop.clone(),
                requests: requests.clone(),
            });
            let live = cluster.node_started(name, published);
            let task = OwnedNodeTask::spawn(async move {
                let _live = live;
                match behavior {
                    StandInStopBehavior::StopsWhenAsked => stop.cancelled().await,
                    StandInStopBehavior::IgnoresTheRequest => future::pending::<()>().await,
                }
                Ok(())
            });
            Self { task, requests }
        }

        fn stop_requests(&self) -> usize {
            self.requests.load(Ordering::Relaxed)
        }
    }

    impl Drop for StandInLiveNode {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// A scenario run that never finishes, and that records whether the watchdog dropped it.
    struct StalledRun {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for StalledRun {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// What one watchdog regression set up: the scenario it registered, the cluster that scenario
    /// holds, and the nodes of that cluster.
    struct WatchdogRegression {
        identity: ScenarioIdentity,
        scenario: Option<ActiveScenarioRegistration>,
        _cluster: LiveClusterRegistration,
        nodes: Vec<StandInLiveNode>,
    }

    impl WatchdogRegression {
        /// Registers a scenario in `phase` holding a cluster whose nodes behave as `nodes` says.
        fn start(
            scenario_name: &str,
            line: usize,
            phase: ScenarioPhase,
            nodes: &[(&str, StandInStopBehavior)],
        ) -> Self {
            let scenario =
                ActiveScenarioRegistration::start("Harness liveness", scenario_name, line);
            scenario.enter(phase);
            let identity = scenario.identity().clone();
            let cluster = LiveClusterRegistration::start(identity.clone());
            let handle = cluster.handle();
            let mut started = Vec::new();
            for (name, behavior) in nodes {
                started.push(StandInLiveNode::start(&handle, name, *behavior));
            }
            Self {
                identity,
                scenario: Some(scenario),
                _cluster: cluster,
                nodes: started,
            }
        }

        fn identity(&self) -> &ScenarioIdentity {
            &self.identity
        }

        /// Drops the scenario registration while this regression keeps holding its cluster.
        ///
        /// A world drops its scenario registration before the cluster it also holds, so a cluster
        /// whose scenario has already left the registry is a state the suite really reaches.
        fn scenario_leaves_the_registry(&mut self) {
            self.scenario = None;
        }

        /// Whether every node of this cluster was asked to stop exactly once.
        fn every_node_was_asked_to_stop(&self) -> bool {
            self.nodes.iter().all(|node| node.stop_requests() == 1)
        }
    }

    /// The entry a suite timeout published for one regression's scenario.
    fn stalled(timeout: &SuiteTimeout, identity: &ScenarioIdentity) -> StalledScenario {
        timeout
            .stall
            .scenarios
            .iter()
            .find(|stalled| &stalled.active.identity == identity)
            .verified("a registered scenario is published until its registration is dropped")
            .clone()
    }

    /// Runs `watchdog` against a run that never finishes, and returns what the timeout reported.
    async fn time_out(watchdog: SuiteWatchdog) -> SuiteTimeout {
        let dropped = Arc::new(AtomicUsize::new(0));
        let guard = StalledRun {
            dropped: dropped.clone(),
        };
        let bounded = watchdog
            .bound(async move {
                let _run = guard;
                future::pending::<()>().await;
            })
            .await;
        let SuiteRun::TimedOut(timeout) = bounded else {
            panic!("a run that never finishes must end at the suite budget");
        };
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            1,
            "a timed-out suite must drop the run it was holding, so the scenarios it still owns \
             are aborted"
        );
        timeout
    }

    #[tokio::test(start_paused = true)]
    async fn a_run_that_finishes_inside_its_budget_keeps_what_it_produced() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let watchdog = SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW);

        let bounded = watchdog
            .bound(async { "the writer the run produced" })
            .await;

        let SuiteRun::Completed(output) = bounded else {
            panic!("a run that finishes inside its budget must not be timed out");
        };
        assert_eq!(output, "the writer the run produced");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_scenario_body_is_named_with_its_attempt_phase_and_nodes() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let regression = WatchdogRegression::start(
            "a body that never returns",
            10,
            ScenarioPhase::Body,
            &[
                ("node-1", StandInStopBehavior::StopsWhenAsked),
                ("node-2", StandInStopBehavior::StopsWhenAsked),
            ],
        );
        let stalled_before_the_budget = Duration::from_secs(7);
        tokio::time::advance(stalled_before_the_budget).await;

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;

        let stalled = stalled(&timeout, regression.identity());
        assert_eq!(stalled.active.phase, ScenarioPhase::Body);
        assert_eq!(stalled.active.attempt, 1);
        let stalled_for = stalled_before_the_budget
            .checked_add(TEST_SUITE_BUDGET)
            .assured("a stall of seconds and a test budget of seconds fit in Duration");
        assert!(
            stalled.active.phase_age() >= stalled_for,
            "a scenario already stalled when the budget started must age through the whole of it: \
             {:?}",
            stalled.active.phase_age()
        );
        assert_eq!(
            stalled.nodes,
            vec!["node-1".to_string(), "node-2".to_string()]
        );
        assert_eq!(timeout.stall.budget, TEST_SUITE_BUDGET);
        assert!(
            regression.every_node_was_asked_to_stop(),
            "every node of a live cluster must be asked to stop"
        );
        assert!(
            !timeout.cleanup.was_forced(),
            "nodes that stop when asked must end inside the cleanup window: {}",
            timeout.cleanup
        );
        assert!(timeout.cleanup.asked >= 2, "{}", timeout.cleanup);
        assert!(
            timeout.cleanup.elapsed <= TEST_CLEANUP_WINDOW,
            "the cleanup must end within the window it was given: {}",
            timeout.cleanup
        );

        let diagnostic = timeout.to_string();
        assert!(
            diagnostic.contains("a body that never returns"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains("attempt=1"), "{diagnostic}");
        assert!(diagnostic.contains("phase=started"), "{diagnostic}");
        assert!(
            diagnostic.contains("nodes=[node-1, node-2]"),
            "{diagnostic}"
        );
        drop(regression);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_teardown_diagnostic_is_named_by_the_phase_it_is_in() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let regression = WatchdogRegression::start(
            "a teardown diagnostic that never returns",
            20,
            ScenarioPhase::Diagnostics,
            &[("node-1", StandInStopBehavior::StopsWhenAsked)],
        );

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;

        let stalled = stalled(&timeout, regression.identity());
        assert_eq!(stalled.active.phase, ScenarioPhase::Diagnostics);
        assert_eq!(stalled.nodes, vec!["node-1".to_string()]);
        assert!(
            regression.every_node_was_asked_to_stop(),
            "a scenario stalled in its diagnostics must still have its nodes asked to stop"
        );
        assert!(!timeout.cleanup.was_forced(), "{}", timeout.cleanup);
        assert!(
            timeout.to_string().contains("phase=teardown diagnostics"),
            "{timeout}"
        );
        drop(regression);
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_that_never_stops_is_named_at_the_end_of_the_cleanup_window() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let regression = WatchdogRegression::start(
            "a node stop that never returns",
            30,
            ScenarioPhase::Stopping,
            &[("node-1", StandInStopBehavior::IgnoresTheRequest)],
        );

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;

        assert!(
            regression.every_node_was_asked_to_stop(),
            "a node that ignores the request must still have been asked"
        );
        assert!(
            timeout.cleanup.was_forced(),
            "a node that never stops must still be live at the end of the window: {}",
            timeout.cleanup
        );
        assert!(
            timeout.cleanup.still_live.contains(&LiveCluster {
                scenario: regression.identity().clone(),
                nodes: vec!["node-1".to_string()],
            }),
            "the cleanup must name the cluster it could not stop: {}",
            timeout.cleanup
        );
        assert!(
            timeout.cleanup.elapsed >= TEST_CLEANUP_WINDOW,
            "a cleanup that could not stop a node spends its whole window: {}",
            timeout.cleanup
        );
        assert!(
            timeout.cleanup.elapsed < TEST_SUITE_BUDGET,
            "the cleanup must not run past the window into a second budget: {}",
            timeout.cleanup
        );
        drop(regression);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cluster_that_outlives_its_scenario_is_named_as_unclaimed() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let mut regression = WatchdogRegression::start(
            "a cluster left behind",
            40,
            ScenarioPhase::Stopping,
            &[("node-1", StandInStopBehavior::StopsWhenAsked)],
        );
        let identity = regression.identity().clone();
        regression.scenario_leaves_the_registry();

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;

        assert!(
            timeout.stall.unclaimed.contains(&LiveCluster {
                scenario: identity.clone(),
                nodes: vec!["node-1".to_string()],
            }),
            "a cluster no active scenario claims must be named on its own: {timeout}"
        );
        assert!(
            !timeout
                .stall
                .scenarios
                .iter()
                .any(|stalled| stalled.active.identity == identity),
            "a scenario that has left the registry must not be reported as active: {timeout}"
        );
        assert!(
            timeout.to_string().contains("unclaimed cluster"),
            "{timeout}"
        );
        drop(regression);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retried_scenario_publishes_which_attempt_is_running() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let first = ActiveScenarioRegistration::start("Harness liveness", "a retried scenario", 50);
        assert_eq!(published(&first).attempt, 1);
        drop(first);

        let retry = ActiveScenarioRegistration::start("Harness liveness", "a retried scenario", 50);
        assert_eq!(
            published(&retry).attempt,
            2,
            "a scenario the suite takes up again is on its next attempt"
        );

        // An outline expands into one scenario per example row, and those rows share the outline's
        // name. They are separate scenarios, so neither counts as a retry of the other.
        let example =
            ActiveScenarioRegistration::start("Harness liveness", "a retried scenario", 51);
        assert_eq!(published(&example).attempt, 1);
        assert_ne!(published(&retry).identity, published(&example).identity);
    }

    /// A command line that carries only the suite watchdog's own options, so the injection point
    /// a run configures the budget through can be parsed on its own.
    #[derive(clap::Parser)]
    struct StandInSuiteCli {
        #[command(flatten)]
        watchdog: SuiteWatchdogArgs,
    }

    #[test]
    fn the_suite_budget_is_injectable_and_defaults_to_the_suite_policy() {
        let configured = StandInSuiteCli::try_parse_from(["scenarios", "--suite-budget", "90s"])
            .expect("the suite budget option must parse a duration");
        assert_eq!(
            configured.watchdog.watchdog().budget(),
            Duration::from_secs(90),
            "a run that gives its own budget must be bounded by it"
        );

        // The option also reads `NERVIX_TEST_SUITE_BUDGET`, so a run that sets it in the
        // environment sees that value here instead of the policy default.
        let default = StandInSuiteCli::try_parse_from(["scenarios"])
            .expect("the suite budget option must have a default");
        assert_eq!(default.watchdog.watchdog().budget(), SUITE_BUDGET);
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_suite_is_reported_apart_from_a_passing_and_a_failing_one() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let regression = WatchdogRegression::start(
            "a suite that has to be ended",
            60,
            ScenarioPhase::Body,
            &[("node-1", StandInStopBehavior::StopsWhenAsked)],
        );

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;
        let reported = SuiteOutcome::TimedOut(timeout);

        let SuiteOutcome::TimedOut(timeout) = &reported else {
            panic!("a timed-out suite must be reported as one: {reported:?}");
        };
        assert!(
            timeout.to_string().contains("a suite that has to be ended"),
            "{timeout}"
        );
        // A passing suite ends the process by returning, which is what running this is.
        SuiteOutcome::Passed.end_process();
        drop(regression);
    }

    #[test]
    #[should_panic(expected = "3 step(s) failed")]
    fn a_failing_suite_ends_the_process_by_unwinding() {
        SuiteOutcome::Failed("3 step(s) failed".to_string()).end_process();
    }

    #[tokio::test(start_paused = true)]
    async fn a_dependency_stop_that_never_returns_is_abandoned_at_its_budget() {
        let started = Instant::now();

        let teardown = SuiteTeardown::bounded(async {
            future::pending::<()>().await;
            Vec::new()
        })
        .await;

        assert!(
            matches!(teardown, SuiteTeardown::Abandoned(budget) if budget == DEPENDENCY_SHUTDOWN_BUDGET),
            "a stop that never returns must be abandoned at its budget: {teardown}"
        );
        assert!(
            started.elapsed() >= DEPENDENCY_SHUTDOWN_BUDGET,
            "the stop must be given its whole budget before it is abandoned: {:?}",
            started.elapsed()
        );
        assert!(
            !teardown.is_clean(),
            "a teardown that never finished has not stopped anything: {teardown}"
        );
        assert!(
            teardown.to_string().contains("left to the runner"),
            "{teardown}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dependency_stop_that_finishes_keeps_what_it_reported() {
        let clean = SuiteTeardown::bounded(async { Vec::new() }).await;
        assert!(clean.is_clean(), "{clean}");

        let failed =
            SuiteTeardown::bounded(async { vec!["redis container did not stop".to_string()] })
                .await;
        assert!(
            !failed.is_clean(),
            "a dependency that reported a failure is not a clean teardown: {failed}"
        );
        assert!(
            failed.to_string().contains("redis container did not stop"),
            "{failed}"
        );
    }
}
