//! Focused regression tests for the liveness owners of the in-process test harness.
//!
//! Outside the layer order: a harness test crate.
//!
//! - **Owns.** Registration of the focused node-liveness, readiness-history, node-startup,
//!   node-stop, phase-deadline, status-request, port-pool, node-bound address, process-cluster
//!   convergence, cluster-teardown, scenario-phase, suite-watchdog, Redis-client, HTTP-receiver,
//!   Iceberg-catalog-gate, backup phase, materialized-state diagnostic, gRPC-receiver, NATS-observer, runner-lag,
//!   runner-offload, Syslog TLS sender, server-executable, stallable-TCP-proxy, unaccepted-address,
//!   process-observation, process-program, process-report, WebSocket close-reply, console-connection and WebSocket peer regressions
//!   with Rust's test runner, and the stand-in nodes, clients, images and processes those regressions
//!   talk to.
//!   It also owns registration of generator cadence, paced-interconnect cadence, bounded
//!   subscription payload observations and the per-node execution readings of timed probes.
//! - **Depends on.** The node-liveness, node-startup, node-stop, phase-deadline, status-request,
//!   port-pool, node-bound address, process-cluster convergence, cluster-teardown, scenario-phase,
//!   suite-watchdog, Redis-client, HTTP-receiver, Iceberg-catalog-gate, backup phase, gRPC-receiver,
//!   NATS-observer, runner-lag, runner-offload, Syslog TLS sender, server-executable, stallable-TCP-proxy,
//!   unaccepted-address, process-observation, process-program, process-report, WebSocket close-reply,
//!   console-connection, WebSocket peer and materialized-state diagnostic harness modules, the product's shutdown
//!   coordinator a stopping
//!   node is read through, and the client wire session protocol the stand-in nodes answer status
//!   requests with.
//!   It also depends on the focused generator cadence, paced-interconnect cadence, subscription
//!   payload observation and node execution reading owners.
//! - **Must not know.** Scenario state or production node lifecycle policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "harness regressions drive every layer from outside the order"
    )
)]

use websocket_close::reply_to_close;

#[path = "common/cluster/byte_pace.rs"]
mod byte_pace;
#[path = "common/cluster/byte_pace_tests.rs"]
mod byte_pace_tests;

#[path = "common/backup_phase.rs"]
mod backup_phase;
#[path = "common/backup_phase_tests.rs"]
mod backup_phase_tests;
#[path = "common/broker_observer.rs"]
mod broker_observer;
#[path = "common/broker_observer_tests.rs"]
mod broker_observer_tests;
#[path = "common/cluster_teardown.rs"]
mod cluster_teardown;
#[path = "common/console_connections.rs"]
mod console_connections;
#[path = "common/console_connections_tests.rs"]
mod console_connections_tests;
#[path = "common/generator_cadence.rs"]
mod generator_cadence;
#[path = "common/generator_cadence_tests.rs"]
mod generator_cadence_tests;
#[path = "common/grpc_receiver.rs"]
mod grpc_receiver;
#[path = "common/http_receiver.rs"]
mod http_receiver;
#[path = "common/iceberg_catalog_gate.rs"]
mod iceberg_catalog_gate;
#[path = "common/iceberg_catalog_gate_tests.rs"]
mod iceberg_catalog_gate_tests;
#[path = "common/materialized_state_diagnostics.rs"]
mod materialized_state_diagnostics;
#[path = "common/materialized_state_diagnostics_tests.rs"]
mod materialized_state_diagnostics_tests;
#[path = "common/nats_observer.rs"]
mod nats_observer;
#[path = "common/nats_observer_tests.rs"]
mod nats_observer_tests;
#[path = "common/node_bound_address.rs"]
mod node_bound_address;
#[path = "common/node_bound_address_tests.rs"]
mod node_bound_address_tests;
#[path = "common/node_execution.rs"]
mod node_execution;
#[path = "common/node_execution_tests.rs"]
mod node_execution_tests;
#[path = "common/node_liveness.rs"]
mod node_liveness;
#[path = "common/node_startup.rs"]
mod node_startup;
#[path = "common/node_stop.rs"]
mod node_stop;
#[path = "common/phase_deadline.rs"]
mod phase_deadline;
#[path = "common/port_pool.rs"]
mod port_pool;
#[path = "common/process_cluster_convergence.rs"]
mod process_cluster_convergence;
#[path = "common/process_observation.rs"]
mod process_observation;
#[path = "common/process_observation_tests.rs"]
mod process_observation_tests;
#[path = "common/process_program.rs"]
mod process_program;
#[path = "common/process_program_tests.rs"]
mod process_program_tests;
#[path = "common/process_report.rs"]
mod process_report;
#[path = "common/process_report_tests.rs"]
mod process_report_tests;
#[path = "common/readiness_history_tests.rs"]
mod readiness_history_tests;
#[path = "common/redis_client.rs"]
mod redis_client;
#[path = "common/runner_lag.rs"]
mod runner_lag;
#[path = "common/runner_lag_tests.rs"]
mod runner_lag_tests;
#[path = "common/runner_observation.rs"]
mod runner_observation;
#[path = "common/runner_observation_tests.rs"]
mod runner_observation_tests;
#[path = "common/runner_offload.rs"]
mod runner_offload;
#[path = "common/runner_offload_tests.rs"]
mod runner_offload_tests;
#[path = "common/scenario_phase.rs"]
#[allow(
    dead_code,
    reason = "the scenario binary uses the measurement APIs shared by this harness test"
)]
mod scenario_phase;
#[path = "common/scenario_schedule.rs"]
#[allow(
    dead_code,
    reason = "the scenario binary uses the admission APIs shared by this harness test"
)]
mod scenario_schedule;
#[path = "common/scenario_shard.rs"]
mod scenario_shard;
#[path = "common/scenario_shard_tests.rs"]
mod scenario_shard_tests;
#[path = "common/server_executable.rs"]
mod server_executable;
#[path = "common/server_executable_tests.rs"]
mod server_executable_tests;
#[path = "common/stallable_tcp_proxy.rs"]
mod stallable_tcp_proxy;
#[path = "common/stallable_tcp_proxy_tests.rs"]
mod stallable_tcp_proxy_tests;
#[path = "common/status_request.rs"]
mod status_request;
#[path = "common/subscription_payloads.rs"]
mod subscription_payloads;
#[path = "common/subscription_payloads_tests.rs"]
mod subscription_payloads_tests;
#[path = "common/suite_watchdog.rs"]
mod suite_watchdog;
#[path = "common/syslog_tls.rs"]
mod syslog_tls;
#[path = "common/syslog_tls_tests.rs"]
mod syslog_tls_tests;
#[path = "common/unaccepted_address.rs"]
mod unaccepted_address;
#[path = "common/unaccepted_address_tests.rs"]
mod unaccepted_address_tests;
#[path = "common/websocket_close.rs"]
mod websocket_close;
#[path = "common/websocket_close_tests.rs"]
mod websocket_close_tests;

#[path = "websocket_lifecycle/peer.rs"]
#[allow(
    dead_code,
    reason = "the scenario binary uses the lifecycle APIs shared by this fixture qualification"
)]
mod websocket_peer;
#[path = "websocket_lifecycle/peer_tests.rs"]
mod websocket_peer_tests;

mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        future, io,
        net::{Ipv4Addr, SocketAddr},
        path::PathBuf,
        time::Duration,
    };

    use clap::Parser as _;
    use error_stack::Report;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{
        ClientFrame, ClientMessage, CommandDisposition, CommandOutcome, Diagnostic, EncodedFrame,
        LeaderRedirect, NoticeLevel, OutcomeOrigin, Reply, ReplyBody, ReplyDelivery, ServerFrame,
        ServerNotice, SessionLimits, VerifiedFrame,
        grpc::{EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec},
    };
    use nervix_models::{ClusterNodeName, CommandExecutionReference};
    use nervix_primitives::{
        net::{TcpListener, TcpSocket},
        stream::wrappers::{ReceiverStream, TcpListenerStream},
        sync::{
            Arc, CancellationToken, Notify, StdArc,
            atomic::{AtomicUsize, Ordering},
            blocking::{LazyLock, Mutex},
            mpsc, oneshot, watch,
        },
        task::JoinHandle,
        time::{Instant, timeout},
    };
    use nervix_recovery::NoReceiver as _;
    use nervix_server::application::{
        AppError, ShutdownCoordinator, ShutdownOutcome, ShutdownPhase, ShutdownPhaseOutcome,
    };
    use nervix_test_environment::{ContainerTeardownError, TeardownFailures};
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
    use tempfile::TempDir;
    use tonic::{
        Request, Response, Status, Streaming,
        body::Body,
        codegen::{BoxFuture, Service, http},
        server::{Grpc, NamedService, StreamingService},
        transport::Server,
    };

    use crate::{
        cluster_teardown::{ClusterTeardown, TeardownNode},
        node_liveness::{
            LastReadinessOutcome, NodeStartupError, NodeStartupFailure, NodeTaskState,
            NodeTaskTerminalOutcome, NodeTaskWaitOutcome, OwnedNodeTask, ReadinessProbeOutcome,
            ResponseKind,
        },
        node_startup::{
            ATTEMPT_READINESS_BUDGET, AttemptCleanup, AttemptFailure, NODE_START_ATTEMPTS,
            NODE_STARTUP_BUDGET, NodeStartup, NodeStartupExhausted, StartableNode, StartupEnd,
            StartupPorts, StartupRetry, StartupRetryPolicy, cluster_startup_budget,
        },
        node_stop::{
            NodeStopClock, NodeStopEnding, NodeStopProgress, NodeStopRecord,
            ProductShutdownDeadline,
        },
        phase_deadline::{BeforeDeadline, PhaseDeadline},
        port_pool::{
            PORT_DRAW_LIMIT, PortPoolError, next_port, next_ports, release_test_ports, reserve,
        },
        process_cluster_convergence::{
            ClusterConvergence, ConvergenceError, Divergence, PeerReachability,
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
            NodeStop, SUITE_BUDGET, SUITE_BUDGET_ENV, StalledScenario, SuiteOutcome, SuiteRun,
            SuiteTeardown, SuiteTimeout, SuiteWatchdog, SuiteWatchdogArgs, WORKFLOW_JOB_LIMIT,
        },
    };

    #[nervix_primitives::test]
    async fn a_retry_runs_before_the_remaining_first_attempts() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(1);
        let current = slots
            .admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "current",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let mut first = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "queued first attempt",
            ScenarioRun::FIRST,
            |_| {},
        ));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        let mut retry = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "retry",
            first_retry(),
            |_| {},
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());

        drop(current);
        let std::task::Poll::Ready(retried) = futures_util::poll!(retry.as_mut()) else {
            panic!("the next released run slot must go to the retry");
        };
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        drop(retried);
        assert!(futures_util::poll!(first.as_mut()).is_ready());
    }

    #[nervix_primitives::test]
    async fn a_ready_retry_runs_before_first_attempts_of_limited_features() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        for limit in [FeatureLimit::WebConsole, FeatureLimit::WasmStateReset] {
            nervix_primitives::task::consume_budget().await;
            let slots = ScenarioRunSlots::new(1);
            let current = slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "current",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            let mut first = Box::pin(slots.admit_with(
                limit,
                ScenarioLength::Ordinary,
                "limited first attempt",
                ScenarioRun::FIRST,
                |_| {},
            ));
            assert!(futures_util::poll!(first.as_mut()).is_pending());
            let mut retry = Box::pin(slots.admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "retry",
                first_retry(),
                |_| {},
            ));
            assert!(futures_util::poll!(retry.as_mut()).is_pending());
            drop(current);
            let std::task::Poll::Ready(retried) = futures_util::poll!(retry.as_mut()) else {
                panic!("an eligible retry must precede a limited first attempt");
            };
            assert!(futures_util::poll!(first.as_mut()).is_pending());
            drop(retried);
            assert!(futures_util::poll!(first.as_mut()).is_ready());
        }
    }

    #[nervix_primitives::test]
    async fn a_retry_waiting_for_its_feature_does_not_hold_a_free_run_slot() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(3);
        let first = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "console",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let second = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "console",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let mut retry = Box::pin(slots.admit_with(
            FeatureLimit::WebConsole,
            ScenarioLength::Ordinary,
            "console retry",
            first_retry(),
            |_| {},
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        let mut ordinary = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "ordinary first attempt",
            ScenarioRun::FIRST,
            |_| {},
        ));
        let std::task::Poll::Ready(ordinary) = futures_util::poll!(ordinary.as_mut()) else {
            panic!("a retry blocked by its feature must leave the free run slot usable");
        };
        drop(first);
        assert!(futures_util::poll!(retry.as_mut()).is_ready());
        drop(second);
        drop(ordinary);
    }

    #[nervix_primitives::test]
    async fn a_cancelled_retry_returns_its_feature_and_run_capacity() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        for limit in [
            FeatureLimit::WebConsole,
            FeatureLimit::WasmStateReset,
            FeatureLimit::Unlimited,
        ] {
            nervix_primitives::task::consume_budget().await;
            let slots = ScenarioRunSlots::new(1);
            let current = slots
                .admit_with(
                    limit,
                    ScenarioLength::Ordinary,
                    "current",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            let mut retry = Box::pin(slots.admit_with(
                limit,
                ScenarioLength::Ordinary,
                "cancelled retry",
                first_retry(),
                |_| {},
            ));
            assert!(futures_util::poll!(retry.as_mut()).is_pending());
            drop(retry);
            let mut successor = Box::pin(slots.admit_with(
                limit,
                ScenarioLength::Ordinary,
                "successor",
                ScenarioRun::FIRST,
                |_| {},
            ));
            assert!(futures_util::poll!(successor.as_mut()).is_pending());
            drop(current);
            assert!(futures_util::poll!(successor.as_mut()).is_ready());
        }
    }

    #[test]
    fn each_retry_holds_twice_the_share_of_the_run_before_it() {
        use std::num::NonZeroU32;

        use crate::scenario_schedule::{SCENARIO_RETRIES, ScenarioRun};

        let run = |number: u32| {
            ScenarioRun::new(NonZeroU32::new(number).verified("the runs below count from one"))
        };
        // In CI's 48 slots a first run holds one, the first retry a sixteenth and the second retry
        // an eighth of the pool.
        assert_eq!(SCENARIO_RETRIES, 2);
        assert_eq!(run(1).run_slots(48), 1);
        assert_eq!(run(2).run_slots(48), 3);
        assert_eq!(run(3).run_slots(48), 6);
        // A run past the configured retries, as a `--retry` override allows, keeps the last share.
        assert_eq!(run(4).run_slots(48), 6);
        assert_eq!(run(9).run_slots(48), 6);
        // A pool too small for a sixteenth still gives every run one slot.
        assert_eq!(run(2).run_slots(4), 1);
        assert_eq!(run(3).run_slots(4), 1);
        assert_eq!(run(2).run_slots(16), 1);
        assert_eq!(run(3).run_slots(16), 2);
        assert_eq!(run(3).run_slots(1), 1);

        for slots in 1..=512 {
            let mut previous = run(1).run_slots(slots);
            assert_eq!(
                previous, 1,
                "a first run of a {slots}-slot pool holds one slot"
            );
            for number in 2..=8 {
                let share = run(number).run_slots(slots);
                assert!(
                    share >= previous,
                    "run {number} of a {slots}-slot pool holds {share}, less than the run before \
                     it"
                );
                assert!(
                    share <= (slots / 4).max(1),
                    "run {number} of a {slots}-slot pool holds {share}, more than a quarter of it"
                );
                previous = share;
            }
        }
    }

    #[nervix_primitives::test]
    async fn a_retry_holds_its_share_of_the_run_slots_until_it_ends() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(48);
        let retry = slots
            .admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "first retry",
                first_retry(),
                |_| {},
            )
            .await;
        assert_eq!(retry.slots(), 3);
        let mut beside = Vec::new();
        for _ in 0..45 {
            nervix_primitives::task::consume_budget().await;
            let admission = slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "first attempt beside the retry",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            assert_eq!(admission.slots(), 1);
            beside.push(admission);
        }
        let mut queued = Vec::new();
        for _ in 0..3 {
            nervix_primitives::task::consume_budget().await;
            let mut waiting = Box::pin(slots.admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "first attempt waiting for a slot",
                ScenarioRun::FIRST,
                |_| {},
            ));
            assert!(
                futures_util::poll!(waiting.as_mut()).is_pending(),
                "the retry's three slots and 45 single slots fill the 48-slot pool"
            );
            queued.push(waiting);
        }

        drop(retry);
        for waiting in &mut queued {
            nervix_primitives::task::consume_budget().await;
            assert!(
                futures_util::poll!(waiting.as_mut()).is_ready(),
                "the retry gives all three of its slots back when it ends"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_retry_waiting_for_its_share_keeps_the_slots_that_free_up() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(16);
        let mut running = Vec::new();
        for _ in 0..16 {
            nervix_primitives::task::consume_budget().await;
            let admission = slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "running first attempt",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            running.push(admission);
        }
        // Run three of a 16-slot pool holds two slots.
        let second_retry =
            ScenarioRun::new(std::num::NonZeroU32::new(3).verified("three is not zero"));
        let mut retry = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "second retry",
            second_retry,
            |_| {},
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        let mut behind = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "first attempt behind the retry",
            ScenarioRun::FIRST,
            |_| {},
        ));
        assert!(futures_util::poll!(behind.as_mut()).is_pending());

        running.pop();
        assert!(
            futures_util::poll!(retry.as_mut()).is_pending(),
            "one free slot does not cover the second retry's two"
        );
        assert!(
            futures_util::poll!(behind.as_mut()).is_pending(),
            "the free slot is kept for the retry ahead, not given to the attempt behind it"
        );
        running.pop();
        let std::task::Poll::Ready(retried) = futures_util::poll!(retry.as_mut()) else {
            panic!("two free slots must admit the second retry");
        };
        assert_eq!(retried.slots(), 2);
        assert!(futures_util::poll!(behind.as_mut()).is_pending());
        drop(retried);
        assert!(futures_util::poll!(behind.as_mut()).is_ready());
    }

    #[nervix_primitives::test]
    async fn a_cancelled_retry_returns_its_whole_share() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(32);
        let mut running = Vec::new();
        for _ in 0..32 {
            nervix_primitives::task::consume_budget().await;
            let admission = slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "running first attempt",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            running.push(admission);
        }
        // The first retry of a 32-slot pool holds two slots.
        let mut retry = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "cancelled first retry",
            first_retry(),
            |_| {},
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        drop(retry);
        let mut successors = Vec::new();
        for _ in 0..2 {
            nervix_primitives::task::consume_budget().await;
            let mut successor = Box::pin(slots.admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "successor",
                ScenarioRun::FIRST,
                |_| {},
            ));
            assert!(futures_util::poll!(successor.as_mut()).is_pending());
            successors.push(successor);
        }

        running.pop();
        running.pop();
        for successor in &mut successors {
            nervix_primitives::task::consume_budget().await;
            assert!(
                futures_util::poll!(successor.as_mut()).is_ready(),
                "the cancelled retry's grant returns both of its slots to the waiters behind it"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_queued_web_console_scenario_does_not_hold_a_run_slot() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = StdArc::new(ScenarioRunSlots::new(3));
        let first = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let second = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let waiting = slots.clone();
        let queued = nervix_primitives::task::spawn(async move {
            waiting
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console execution graph",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await
        });
        nervix_primitives::task::yield_now().await;
        let unrelated = timeout(
            Duration::from_millis(100),
            slots.admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "ordinary",
                ScenarioRun::FIRST,
                |_| {},
            ),
        )
        .await;
        assert!(
            unrelated.is_ok(),
            "a feature wait must leave the free run slot available"
        );
        drop(first);
        drop(second);
        queued
            .await
            .expect("queued scenario completes after the feature releases");
    }

    #[nervix_primitives::test]
    async fn the_next_limited_scenario_gets_a_slot_beside_bulk_work() {
        use crate::scenario_schedule::{
            AdmissionWait, FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = StdArc::new(ScenarioRunSlots::new(1));
        let current = slots
            .admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "ordinary",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let (ordinary_waiting, ordinary_started) = oneshot::channel();
        let ordinary_slots = slots.clone();
        let ordinary = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(ordinary_waiting);
            ordinary_slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "ordinary",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::RunSlot {
                            waiting
                                .take()
                                .expect("ordinary waiter reports once")
                                .send(())
                                .expect("the test still observes admission");
                        }
                    },
                )
                .await
        });
        ordinary_started.await.expect("ordinary waiter starts");
        nervix_primitives::task::yield_now().await;

        let (limited_waiting, limited_started) = oneshot::channel();
        let limited_slots = slots.clone();
        let limited = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(limited_waiting);
            limited_slots
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console execution graph",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::RunSlot {
                            waiting
                                .take()
                                .expect("limited waiter reports once")
                                .send(())
                                .expect("the test still observes admission");
                        }
                    },
                )
                .await
        });
        limited_started
            .await
            .expect("limited waiter reaches the slot queue");
        nervix_primitives::task::yield_now().await;
        drop(current);
        let limited_permit = timeout(Duration::from_millis(100), limited)
            .await
            .expect("limited work takes the next slot")
            .expect("limited task completes");
        assert!(
            !ordinary.is_finished(),
            "bulk work must wait for the limited chain"
        );
        drop(limited_permit);
        ordinary.await.expect("bulk work takes the released slot");
    }

    #[nervix_primitives::test]
    async fn releasing_a_limited_scenario_hands_its_slot_to_the_next_in_its_chain() {
        use crate::scenario_schedule::{
            AdmissionWait, FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = StdArc::new(ScenarioRunSlots::new(2));
        let first = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let second = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;

        let (limited_waiting, limited_started) = oneshot::channel();
        let next_slots = slots.clone();
        let next = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(limited_waiting);
            next_slots
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console execution graph",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::WebConsole {
                            waiting
                                .take()
                                .expect("feature waiter reports once")
                                .send(())
                                .expect("the test still observes admission");
                        }
                    },
                )
                .await
        });
        limited_started.await.expect("limited successor starts");

        let (ordinary_waiting, ordinary_started) = oneshot::channel();
        let ordinary_slots = slots.clone();
        let ordinary = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(ordinary_waiting);
            ordinary_slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    "ordinary",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::RunSlot {
                            waiting
                                .take()
                                .expect("ordinary waiter reports once")
                                .send(())
                                .expect("the test still observes admission");
                        }
                    },
                )
                .await
        });
        ordinary_started.await.expect("ordinary waiter starts");
        nervix_primitives::task::yield_now().await;

        drop(first);
        let next_permit = timeout(Duration::from_millis(100), next)
            .await
            .expect("the limited successor takes the released slot")
            .expect("limited successor task completes");
        assert!(
            !ordinary.is_finished(),
            "bulk work waits while a limited chain has an admitted successor"
        );
        drop(next_permit);
        ordinary.await.expect("bulk work takes the later slot");
        drop(second);
    }

    #[nervix_primitives::test]
    async fn each_web_console_feature_starts_before_one_feature_consumes_the_group() {
        use crate::scenario_schedule::{
            AdmissionWait, FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = StdArc::new(ScenarioRunSlots::new(2));
        let first = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let second = slots
            .admit_with(
                FeatureLimit::WebConsole,
                ScenarioLength::Ordinary,
                "Web console execution graph",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;

        let (graph_waiting, graph_queued) = oneshot::channel();
        let graph_slots = slots.clone();
        let graph = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(graph_waiting);
            graph_slots
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console execution graph",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::WebConsole {
                            waiting
                                .take()
                                .expect("reports once")
                                .send(())
                                .expect("observed");
                        }
                    },
                )
                .await
        });
        graph_queued.await.expect("graph successor is queued");

        let (repl_waiting, repl_queued) = oneshot::channel();
        let repl_slots = slots.clone();
        let repl = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(repl_waiting);
            repl_slots
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console Plexus REPL",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::WebConsole {
                            waiting
                                .take()
                                .expect("reports once")
                                .send(())
                                .expect("observed");
                        }
                    },
                )
                .await
        });
        repl_queued.await.expect("REPL successor is queued");

        let (inspector_waiting, inspector_queued) = oneshot::channel();
        let inspector_slots = slots.clone();
        let inspector = nervix_primitives::task::spawn(async move {
            let mut waiting = Some(inspector_waiting);
            inspector_slots
                .admit_with(
                    FeatureLimit::WebConsole,
                    ScenarioLength::Ordinary,
                    "Web console transaction inspector",
                    ScenarioRun::FIRST,
                    |reason| {
                        if reason == AdmissionWait::WebConsole {
                            waiting
                                .take()
                                .expect("reports once")
                                .send(())
                                .expect("observed");
                        }
                    },
                )
                .await
        });
        inspector_queued
            .await
            .expect("inspector successor is queued");

        drop(first);
        let repl_admission = timeout(Duration::from_millis(100), repl)
            .await
            .expect("the REPL takes the first released slot")
            .expect("REPL admission task completes");
        drop(second);
        let inspector_admission = timeout(Duration::from_millis(100), inspector)
            .await
            .expect("the inspector takes the second released slot")
            .expect("inspector admission task completes");
        assert!(
            !graph.is_finished(),
            "a third graph case waits for its turn"
        );
        drop(repl_admission);
        drop(inspector_admission);
        graph.await.expect("graph successor eventually runs");
    }

    #[nervix_primitives::test]
    async fn a_declared_long_scenario_is_admitted_before_queued_ordinary_first_attempts() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(1);
        let current = slots
            .admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "current",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let mut ordinary = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "ordinary first attempt queued earlier",
            ScenarioRun::FIRST,
            |_| {},
        ));
        assert!(futures_util::poll!(ordinary.as_mut()).is_pending());
        let mut long = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::LongRunning,
            "long-running first attempt queued later",
            ScenarioRun::FIRST,
            |_| {},
        ));
        assert!(futures_util::poll!(long.as_mut()).is_pending());

        drop(current);
        let std::task::Poll::Ready(long_admission) = futures_util::poll!(long.as_mut()) else {
            panic!("the released run slot must go to the long-running scenario queued later");
        };
        assert!(futures_util::poll!(ordinary.as_mut()).is_pending());
        drop(long_admission);
        assert!(futures_util::poll!(ordinary.as_mut()).is_ready());
    }

    #[nervix_primitives::test]
    async fn a_retry_runs_before_a_declared_long_first_attempt() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        let slots = ScenarioRunSlots::new(1);
        let current = slots
            .admit_with(
                FeatureLimit::Unlimited,
                ScenarioLength::Ordinary,
                "current",
                ScenarioRun::FIRST,
                |_| {},
            )
            .await;
        let mut long = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::LongRunning,
            "long-running first attempt",
            ScenarioRun::FIRST,
            |_| {},
        ));
        assert!(futures_util::poll!(long.as_mut()).is_pending());
        let mut retry = Box::pin(slots.admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            "ordinary retry",
            first_retry(),
            |_| {},
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());

        drop(current);
        let std::task::Poll::Ready(retried) = futures_util::poll!(retry.as_mut()) else {
            panic!("an eligible retry must precede a long-running first attempt");
        };
        assert!(futures_util::poll!(long.as_mut()).is_pending());
        drop(retried);
        assert!(futures_util::poll!(long.as_mut()).is_ready());
    }

    /// Two run slots take up four ordinary attempts of ten seconds, queued in feature order before
    /// a declared long-running attempt of thirty. In queue order the long attempt starts at twenty
    /// seconds, once the four have ended, and runs on alone until fifty: the shape of the suite
    /// tail in which one transaction-report attempt, admitted in the run's last quarter, outlasted
    /// every other scenario. Admitted ahead of them, it runs beside them and the suite ends with
    /// its bulk, at forty.
    #[nervix_primitives::test(start_paused = true)]
    async fn a_declared_long_scenario_queued_last_runs_beside_the_bulk_instead_of_after_it() {
        use crate::scenario_schedule::{
            FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots,
        };

        const ORDINARY_ATTEMPT: Duration = Duration::from_secs(10);
        const LONG_ATTEMPT: Duration = Duration::from_secs(30);
        const BULK_END: Duration = Duration::from_secs(40);

        async fn attempt(
            slots: &ScenarioRunSlots,
            length: ScenarioLength,
            time: Duration,
            began: Instant,
        ) -> Duration {
            let admission = slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    length,
                    "Suite tail",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await;
            nervix_primitives::time::sleep(time).await;
            drop(admission);
            began.elapsed()
        }

        let slots = ScenarioRunSlots::new(2);
        let began = Instant::now();
        let mut attempts = Vec::new();
        for _ in 0..4 {
            attempts.push(Box::pin(attempt(
                &slots,
                ScenarioLength::Ordinary,
                ORDINARY_ATTEMPT,
                began,
            )));
        }
        attempts.push(Box::pin(attempt(
            &slots,
            ScenarioLength::LongRunning,
            LONG_ATTEMPT,
            began,
        )));
        // Queue every attempt in feature order, the order in which Cucumber takes them up.
        for queued in &mut attempts {
            assert!(futures_util::poll!(queued.as_mut()).is_pending());
        }

        let ends = futures_util::future::join_all(attempts).await;
        let suite_end = ends
            .into_iter()
            .max()
            .verified("the suite above ran five attempts");
        assert_eq!(
            suite_end, BULK_END,
            "the long-running attempt must run beside the bulk instead of after it"
        );
    }

    #[test]
    fn long_running_is_declared_by_a_tag_of_the_scenario_its_rule_or_its_feature() {
        use crate::scenario_schedule::ScenarioLength;

        assert_eq!(
            ScenarioLength::declared_by(["transaction_quiesce_qualification", "long_running"]),
            ScenarioLength::LongRunning
        );
        assert_eq!(
            ScenarioLength::declared_by(["transaction_quiesce_qualification"]),
            ScenarioLength::Ordinary
        );
        assert_eq!(
            ScenarioLength::declared_by(std::iter::empty::<&str>()),
            ScenarioLength::Ordinary
        );
    }

    #[test]
    fn an_undeclared_attempt_of_two_minutes_names_its_scenario_long_running() {
        use crate::scenario_schedule::{LONG_RUNNING_ATTEMPT, ScenarioLength};

        let shorter = LONG_RUNNING_ATTEMPT
            .checked_sub(Duration::from_millis(1))
            .verified("the long-running bound is two minutes");
        assert!(ScenarioLength::Ordinary.lacks_declaration_for(LONG_RUNNING_ATTEMPT));
        assert!(!ScenarioLength::Ordinary.lacks_declaration_for(shorter));
        assert!(!ScenarioLength::LongRunning.lacks_declaration_for(LONG_RUNNING_ATTEMPT));
    }

    #[test]
    fn limited_features_are_taken_up_before_the_bulk() {
        use crate::scenario_schedule::prioritize_features;

        let mut names = [
            "Ordinary feature one",
            "Web console Plexus REPL",
            "Ordinary feature two",
            "Web console domain clock",
            "Coordinated WASM processor state reset",
        ];
        prioritize_features(&mut names, |name| Some(name));
        assert_eq!(
            &names[..3],
            &[
                "Coordinated WASM processor state reset",
                "Web console Plexus REPL",
                "Web console domain clock",
            ]
        );
    }

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

    /// The run that follows a failed first run.
    fn first_retry() -> crate::scenario_schedule::ScenarioRun {
        crate::scenario_schedule::ScenarioRun::new(
            std::num::NonZeroU32::new(2).verified("two is not zero"),
        )
    }

    const TEST_AUTHORIZATION: &str = "Basic c3RhbmQtaW46c3RhbmQtaW4=";
    /// How long a convergence regression gives its stand-in members to converge. Only a wait that
    /// never ends while its members converge reaches it.
    const CONVERGENCE_TEST_BUDGET: Duration = Duration::from_secs(30);
    /// The budget a convergence regression lets pass on purpose. Its stand-in members answer at
    /// once, so the wait asks them many times within it.
    const CONVERGENCE_EXPIRY_BUDGET: Duration = Duration::from_secs(1);
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
        /// Answers the session's command with this outcome.
        Answer(CommandOutcome),
        /// Answers the session's command with the answer published when the command arrives.
        AnswerPublished(watch::Receiver<PublishedAnswer>),
        /// Answers the session's command once `gate` opens.
        AnswerAfter {
            result: CommandOutcome,
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

    type StandInResponses = mpsc::Sender<Result<EncodedFrame<ServerFrame>, Status>>;

    /// One answer a stand-in gives every status session that arrives while it is published, and how
    /// many sessions it has answered.
    #[derive(Clone)]
    struct PublishedAnswer {
        result: CommandOutcome,
        answered: Arc<watch::Sender<usize>>,
    }

    impl PublishedAnswer {
        fn status(text: &str) -> Self {
            Self {
                result: command_result(completed(), text),
                answered: Arc::new(watch::Sender::new(0)),
            }
        }

        /// Waits until the stand-in has answered `count` sessions with this answer.
        async fn answered(&self, count: usize) {
            let mut answered = self.answered.subscribe();
            answered
                .wait_for(|answered| *answered >= count)
                .await
                .assured("the answer keeps its own counter");
        }
    }

    /// What a stand-in member answers, replaced as a regression moves the member's view along.
    struct PublishedMember {
        answer: watch::Sender<PublishedAnswer>,
    }

    impl PublishedMember {
        fn new(answer: &PublishedAnswer) -> Self {
            Self {
                answer: watch::Sender::new(answer.clone()),
            }
        }

        fn behavior(&self) -> StandInBehavior {
            StandInBehavior::AnswerPublished(self.answer.subscribe())
        }

        fn publish(&self, answer: &PublishedAnswer) {
            self.answer.send_replace(answer.clone());
        }
    }

    /// The application-health target a stand-in member reports for one peer.
    struct PeerTargetLine {
        peer: &'static str,
        generation: u64,
        status: &'static str,
    }

    /// The cluster status one member of a stand-in three-member cluster reports, in the form
    /// `SHOW CLUSTER STATUS` renders it: its view of the Raft leader and voters, and the
    /// application-health target it observes each peer through.
    fn member_status(member: &str, leader: &str, targets: &[PeerTargetLine]) -> String {
        let role = if member == leader {
            "Leader"
        } else {
            "Follower"
        };
        let mut lines = vec![
            "[chitchat]".to_string(),
            "cluster_id: cucumber".to_string(),
            "seed_nodes: (none)".to_string(),
            "self:".to_string(),
            format!("- node_id: {member}"),
            "live_nodes:".to_string(),
            String::new(),
            "[raft]".to_string(),
            format!("raft.id: {member}"),
            format!("raft.current_leader: {leader}"),
            "raft.current_term: 3".to_string(),
            format!("raft.state: {role}"),
            "raft.cordoned_nodes: (none)".to_string(),
            "raft.last_log_index: 40".to_string(),
            "raft.last_applied: 40".to_string(),
            "raft.membership:".to_string(),
            "- node-1 [voter] 127.0.0.1:41001".to_string(),
            "- node-2 [voter] 127.0.0.1:41002".to_string(),
            "- node-3 [voter] 127.0.0.1:41003".to_string(),
            String::new(),
            "[interconnect]".to_string(),
        ];
        let mut warnings = Vec::new();
        for target in targets {
            let observation = match target.status {
                "connected" => "healthy",
                "unknown" => "none",
                _ => "failure",
            };
            lines.push(format!(
                "- {}: addr=127.0.0.1:4100{} generation={} observation={observation} \
                 observation_age=200ms status={}",
                target.peer,
                target.peer.trim_start_matches("node-"),
                target.generation,
                target.status
            ));
            if target.status == "unavailable" {
                warnings.push(format!(
                    "- raft member '{}' is marked unavailable by application health",
                    target.peer
                ));
            }
        }
        if warnings.is_empty() {
            warnings.push("- none".to_string());
        }
        lines.extend([
            String::new(),
            "[domains]".to_string(),
            "- none".to_string(),
            String::new(),
            "[schedule]".to_string(),
            "- none".to_string(),
            String::new(),
            "[warnings]".to_string(),
        ]);
        lines.extend(warnings);
        lines.join("\n")
    }

    fn target(peer: &'static str, generation: u64, status: &'static str) -> PeerTargetLine {
        PeerTargetLine {
            peer,
            generation,
            status,
        }
    }

    impl StandInBehavior {
        async fn answer(
            self,
            mut commands: Streaming<VerifiedFrame<ClientFrame>>,
            responses: StandInResponses,
        ) {
            let Ok(Some(command)) = commands.message().await else {
                return;
            };
            let request_id = ClientMessage::decode(&command)
                .assured("the status request under test sends a valid request")
                .request_id;
            match self {
                Self::Answer(result) => Self::send_result(&responses, request_id, result).await,
                Self::AnswerPublished(published) => {
                    let answer = published.borrow().clone();
                    Self::send_result(&responses, request_id, answer.result).await;
                    answer.answered.send_modify(|answered| *answered += 1);
                }
                Self::AnswerAfter { result, gate } => {
                    gate.notified().await;
                    Self::send_result(&responses, request_id, result).await;
                }
                Self::WithholdResponse { received } => {
                    received.notify_one();
                    future::pending::<()>().await;
                }
                Self::FloodResponses => loop {
                    nervix_primitives::task::consume_budget().await;
                    let notice = ServerNotice {
                        level: NoticeLevel::Info,
                        message: "an event unrelated to the status request".to_string(),
                    }
                    .encode(&SessionLimits::DEFAULT)
                    .assured("a short notice fits a frame");
                    if responses.send(Ok(notice)).await.is_err() {
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
            responses: &StandInResponses,
            request_id: nervix_client_wire::RequestId,
            result: CommandOutcome,
        ) {
            let reply = Reply {
                request_id,
                body: ReplyBody::Command(Box::new(result)),
            };
            let ReplyDelivery::Frame(frame) = reply
                .encode(&SessionLimits::DEFAULT)
                .assured("a stand-in reply fits the session limits")
            else {
                panic!("a stand-in reply fits one frame");
            };
            responses
                .send(Ok(frame))
                .await
                .means_peer_left("the status request under test");
        }
    }

    /// A session service that serves the exchange method the way the stand-in behaves.
    #[derive(Clone)]
    struct StandInService {
        behavior: StandInBehavior,
    }

    impl NamedService for StandInService {
        const NAME: &'static str = SERVICE_NAME;
    }

    impl Service<http::Request<Body>> for StandInService {
        type Response = http::Response<Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;

        fn poll_ready(
            &mut self,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<Body>) -> Self::Future {
            let behavior = self.behavior.clone();
            if request.uri().path() != EXCHANGE_PATH {
                return Box::pin(async move {
                    Ok(Status::unimplemented("stand-in nodes serve only the exchange").into_http())
                });
            }
            Box::pin(async move {
                let mut grpc = Grpc::new(ServerExchangeCodec::new(SessionLimits::DEFAULT));
                Ok(grpc.streaming(StandInExchange { behavior }, request).await)
            })
        }
    }

    struct StandInExchange {
        behavior: StandInBehavior,
    }

    impl StreamingService<VerifiedFrame<ClientFrame>> for StandInExchange {
        type Response = EncodedFrame<ServerFrame>;
        type ResponseStream = ReceiverStream<Result<EncodedFrame<ServerFrame>, Status>>;
        type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

        fn call(
            &mut self,
            request: Request<Streaming<VerifiedFrame<ClientFrame>>>,
        ) -> Self::Future {
            let behavior = self.behavior.clone();
            Box::pin(async move {
                match &behavior {
                    StandInBehavior::WithholdSession => future::pending::<()>().await,
                    StandInBehavior::RejectSession => {
                        return Err(Status::unauthenticated(
                            "the stand-in refuses every session",
                        ));
                    }
                    StandInBehavior::Answer(_)
                    | StandInBehavior::AnswerPublished(_)
                    | StandInBehavior::AnswerAfter { .. }
                    | StandInBehavior::WithholdResponse { .. }
                    | StandInBehavior::FloodResponses
                    | StandInBehavior::FailResponse
                    | StandInBehavior::EndSession => {}
                }
                let (response_tx, response_rx) = mpsc::channel(4);
                nervix_primitives::task::spawn(behavior.answer(request.into_inner(), response_tx));
                Ok(Response::new(ReceiverStream::new(response_rx)))
            })
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
            let service = StandInService { behavior };
            let server = nervix_primitives::task::spawn(async move {
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
        /// The node's owned teardown finishes after the stop request.
        NeverReadyThenStops { delay: Duration },
        /// Initialization returns its application error after readiness has expired.
        NeverReadyThenFails { error: AppError },
        /// The node's task ends at once with this application error.
        ApplicationError(AppError),
        /// A listener fails with its typed I/O cause before initialization starts.
        ListenError {
            application: AppError,
            kind: io::ErrorKind,
        },
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
        port_moves: Vec<StartupPorts>,
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
                port_moves: Vec::new(),
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
            self.start_with_policy(budget, StartupRetryPolicy::Transient)
                .await
        }

        async fn start_with_policy(
            &mut self,
            budget: PhaseDeadline,
            policy: StartupRetryPolicy,
        ) -> Result<(), Report<NodeStartupExhausted>> {
            let node = self.node.clone();
            NodeStartup::start(&node, self, budget, policy).await
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
                LaunchBehavior::ListenError { application, kind } => {
                    OwnedNodeTask::spawn(async move {
                        Err(Report::new(io::Error::from(kind)).change_context(application))
                    })
                }
                LaunchBehavior::Panics => {
                    OwnedNodeTask::spawn(async { panic!("intentional stand-in node panic") })
                }
                LaunchBehavior::NeverReadyAndNeverStops => OwnedNodeTask::spawn(async {
                    future::pending::<()>().await;
                    Ok(())
                }),
                LaunchBehavior::NeverReadyThenStops { delay } => OwnedNodeTask::spawn(async move {
                    stop.cancelled().await;
                    nervix_primitives::time::sleep(delay).await;
                    Ok(())
                }),
                LaunchBehavior::NeverReadyThenFails { error } => OwnedNodeTask::spawn(async move {
                    stop.cancelled().await;
                    Err(Report::new(error))
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
                                response_kind: ResponseKind::Completed,
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

        fn move_to_fresh_ports(&mut self, ports: StartupPorts) -> io::Result<()> {
            if !self.ports_available {
                return Err(io::Error::other("the port pool is exhausted"));
            }
            self.ports_moved += 1;
            self.port_moves.push(ports);
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

    /// A status command outcome with `disposition` and `message`.
    fn command_result(disposition: CommandDisposition, message: &str) -> CommandOutcome {
        CommandOutcome {
            execution_reference: CommandExecutionReference::parse("stand-in-status")
                .assured("a literal execution reference"),
            origin: OutcomeOrigin::Executed,
            disposition,
            message: message.to_string(),
            diagnostics: Vec::new(),
            statements: Vec::new(),
            transaction: None,
            transaction_admission: None,
            inspection: None,
            wasm_state: None,
            resource: None,
            backup: None,
            restore: None,
        }
    }

    fn completed() -> CommandDisposition {
        CommandDisposition::Completed {
            already_existed: false,
        }
    }

    fn not_leader() -> CommandDisposition {
        CommandDisposition::NotLeader(LeaderRedirect { leader: None })
    }

    /// A loopback address that refuses connections for as long as it is held.
    ///
    /// Its socket stays bound, so no other test or process can take the port while a probe
    /// expects the refusal, and it never listens, so the kernel refuses every connection to it.
    /// A port released before the probe could be taken by anything else on a busy runner.
    struct RefusedAddress {
        _socket: TcpSocket,
        address: SocketAddr,
    }

    impl RefusedAddress {
        fn bind() -> Self {
            let socket = TcpSocket::new_v4().assured("the test can open a loopback TCP socket");
            socket
                .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .assured("the loopback interface accepts a test socket");
            let address = socket.local_addr().assured("a bound socket has an address");
            Self {
                _socket: socket,
                address,
            }
        }
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
            response_kind: ResponseKind::Failed,
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
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .assured("the bounded test task reaches a terminal outcome")
    }

    #[nervix_primitives::test]
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
    }

    #[nervix_primitives::test]
    async fn clean_application_exit_before_readiness_is_terminal() {
        let mut task = OwnedNodeTask::spawn(async { Ok(()) });
        nervix_primitives::task::yield_now().await;
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

    #[nervix_primitives::test]
    async fn application_error_before_readiness_is_retained() {
        let mut task = OwnedNodeTask::spawn(async {
            Err(Report::new(AppError::MissingGrpcHttpsListenAddress))
        });
        nervix_primitives::task::yield_now().await;
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn readiness_and_status_outcomes_retain_their_typed_cause() {
        let ready = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            HEALTHY_STATUS,
        )))
        .await;
        let not_leader = StandInNode::serve(StandInBehavior::Answer(command_result(
            not_leader(),
            "not the leader",
        )))
        .await;
        let mut unavailable_result =
            command_result(CommandDisposition::Failed, "cluster status unavailable");
        unavailable_result.diagnostics = vec![Diagnostic {
            message: "consensus is starting".to_string(),
            span: None,
        }];
        let unavailable = StandInNode::serve(StandInBehavior::Answer(unavailable_result)).await;
        let rejecting = StandInNode::serve(StandInBehavior::RejectSession).await;
        let failing = StandInNode::serve(StandInBehavior::FailResponse).await;
        let ending = StandInNode::serve(StandInBehavior::EndSession).await;
        let refused_address = RefusedAddress::bind();
        let refused = plaintext_endpoint(refused_address.address);
        let phase = PhaseDeadline::after(STALLED_REQUEST_GUARD);

        assert!(matches!(
            ReadinessProbeOutcome::probe(&ready.endpoint(), phase).await,
            ReadinessProbeOutcome::Ready {
                response_kind: ResponseKind::Completed
            }
        ));
        assert!(matches!(
            ReadinessProbeOutcome::probe(&not_leader.endpoint(), phase).await,
            ReadinessProbeOutcome::Ready {
                response_kind: ResponseKind::NotLeader
            }
        ));
        let unsuccessful = ReadinessProbeOutcome::probe(&unavailable.endpoint(), phase).await;
        assert!(matches!(
            unsuccessful,
            ReadinessProbeOutcome::UnsuccessfulResponse {
                response_kind: ResponseKind::Failed,
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
                disposition: CommandDisposition::Failed,
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

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn status_request_ends_at_its_deadline_while_session_establishment_is_pending() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;

        assert_request_ends_at_its_deadline(&stalled.endpoint(), StatusOperation::OpenSession)
            .await;
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn status_request_ends_at_its_deadline_while_unrelated_responses_remain_ready() {
        let flooding = StandInNode::serve(StandInBehavior::FloodResponses).await;

        assert_request_ends_at_its_deadline(&flooding.endpoint(), StatusOperation::ReceiveResponse)
            .await;
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn nested_deadline_never_outlives_its_phase() {
        let phase = PhaseDeadline::after(STATUS_WAIT_BUDGET);
        assert_eq!(
            phase.nested(STATUS_REQUEST_TIMEOUT).budget(),
            STATUS_REQUEST_TIMEOUT
        );

        let late = STATUS_WAIT_BUDGET
            .checked_sub(Duration::from_secs(1))
            .assured("the status wait budget is longer than one second");
        nervix_primitives::time::advance(late).await;
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test]
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
    }

    #[nervix_primitives::test]
    async fn status_snapshots_keep_a_healthy_node_while_another_node_stalls() {
        // The healthy node answers only after the stalled node has received its command, so it
        // can report only if both requests are in flight at once.
        let stalled_received = Arc::new(Notify::new());
        let stalled = StandInNode::serve(StandInBehavior::WithholdResponse {
            received: stalled_received.clone(),
        })
        .await;
        let healthy = StandInNode::serve(StandInBehavior::AnswerAfter {
            result: command_result(completed(), HEALTHY_STATUS),
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

    /// A relaunched member's killed process never left the voter configuration, so the members agree
    /// on their leader and voters as soon as the new process follows the leader. The leader still
    /// judges the member by the target it observed the killed process through until it discovers
    /// the new process, and refuses to place work on it until then.
    #[nervix_primitives::test]
    async fn a_relaunched_member_converges_only_once_every_peer_reaches_its_new_process() {
        for stale in ["unavailable", "connected"] {
            nervix_primitives::task::consume_budget().await;
            // node-2's process has exited. node-1 leads and observes it through generation 4,
            // node-3 through generation 6.
            let leader = PublishedMember::new(&PublishedAnswer::status(&member_status(
                "node-1",
                "node-1",
                &[target("node-2", 4, stale), target("node-3", 2, "connected")],
            )));
            let relaunched = PublishedMember::new(&PublishedAnswer {
                result: command_result(CommandDisposition::Failed, "the process has exited"),
                answered: Arc::new(watch::Sender::new(0)),
            });
            let other = PublishedMember::new(&PublishedAnswer::status(&member_status(
                "node-3",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-2", 6, "unavailable"),
                ],
            )));
            let node_1 = StandInNode::serve(leader.behavior()).await;
            let node_2 = StandInNode::serve(relaunched.behavior()).await;
            let node_3 = StandInNode::serve(other.behavior()).await;
            let convergence = ClusterConvergence::new(
                BTreeMap::from([
                    ("node-1".to_string(), node_1.endpoint()),
                    ("node-2".to_string(), node_2.endpoint()),
                    ("node-3".to_string(), node_3.endpoint()),
                ]),
                CONVERGENCE_TEST_BUDGET,
            );
            // The new process follows the leader and reaches both peers, and node-3 has discovered
            // it through a new target, while node-1 still observes the killed process.
            let unaware = PublishedAnswer::status(&member_status(
                "node-1",
                "node-1",
                &[target("node-2", 4, stale), target("node-3", 2, "connected")],
            ));
            let relaunch = async {
                relaunched.publish(&PublishedAnswer::status(&member_status(
                    "node-2",
                    "node-1",
                    &[
                        target("node-1", 1, "connected"),
                        target("node-3", 2, "connected"),
                    ],
                )));
                other.publish(&PublishedAnswer::status(&member_status(
                    "node-3",
                    "node-1",
                    &[
                        target("node-1", 1, "connected"),
                        target("node-2", 7, "connected"),
                    ],
                )));
                leader.publish(&unaware);
                Ok(node_2.endpoint())
            };
            let mut converging = std::pin::pin!(convergence.relaunch("node-2", relaunch));

            let premature = nervix_primitives::select! {
                converged = &mut converging => Some(converged),
                () = unaware.answered(2) => None,
            };
            if let Some(converged) = premature {
                panic!(
                    "the relaunch converged while node-1 still observed the killed process \
                     through a {stale} target: {converged:?}"
                );
            }
            leader.publish(&PublishedAnswer::status(&member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 5, "connected"),
                    target("node-3", 2, "connected"),
                ],
            )));
            timeout(CONVERGENCE_TEST_BUDGET, converging)
                .await
                .assured("the relaunch ends within its own budget")
                .assured("every member reaches the new process once node-1 discovers it");
        }
    }

    /// A start ends only once every member's latest application-health probe of every other member
    /// reached it, as an in-process cluster's construction does.
    #[nervix_primitives::test]
    async fn a_started_cluster_converges_once_every_member_reaches_every_peer() {
        let first = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-1",
            "node-1",
            &[
                target("node-2", 1, "connected"),
                target("node-3", 2, "connected"),
            ],
        )));
        let second = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-2",
            "node-1",
            &[
                target("node-1", 1, "connected"),
                target("node-3", 2, "connected"),
            ],
        )));
        let unprobed = PublishedAnswer::status(&member_status(
            "node-3",
            "node-1",
            &[
                target("node-1", 1, "unknown"),
                target("node-2", 2, "connected"),
            ],
        ));
        let third = PublishedMember::new(&unprobed);
        let node_1 = StandInNode::serve(first.behavior()).await;
        let node_2 = StandInNode::serve(second.behavior()).await;
        let node_3 = StandInNode::serve(third.behavior()).await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), node_1.endpoint()),
                ("node-2".to_string(), node_2.endpoint()),
                ("node-3".to_string(), node_3.endpoint()),
            ]),
            CONVERGENCE_TEST_BUDGET,
        );
        let mut converging = std::pin::pin!(convergence.wait());

        let premature = nervix_primitives::select! {
            converged = &mut converging => Some(converged),
            () = unprobed.answered(2) => None,
        };
        if let Some(converged) = premature {
            panic!("the cluster converged while node-3 had not reached node-1: {converged:?}");
        }
        third.publish(&PublishedAnswer::status(&member_status(
            "node-3",
            "node-1",
            &[
                target("node-1", 1, "connected"),
                target("node-2", 2, "connected"),
            ],
        )));
        timeout(CONVERGENCE_TEST_BUDGET, converging)
            .await
            .assured("the start ends within its own budget")
            .assured("every member reaches every peer once node-3 reaches node-1");
    }

    #[nervix_primitives::test]
    async fn a_one_member_cluster_converges_without_peers() {
        let alone = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            "raft.current_leader: node-1\nraft.membership:\n- node-1 [voter] \
             127.0.0.1:41001\n\n[interconnect]\n- (none)",
        )))
        .await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([("node-1".to_string(), alone.endpoint())]),
            CONVERGENCE_TEST_BUDGET,
        );

        timeout(CONVERGENCE_TEST_BUDGET, convergence.wait())
            .await
            .assured("the start ends within its own budget")
            .assured("a member with no peers converges on its own leadership");
    }

    /// The status format is the server's, so a line the wait cannot read would be read the same way
    /// on every later attempt: the wait ends at once, naming the member and the line.
    #[nervix_primitives::test]
    async fn a_status_line_convergence_cannot_read_ends_the_wait_with_its_member_and_line() {
        const VOTER: &str = "- node-1 [voter] 127.0.0.1:41001";
        const TARGET: &str = "- node-2: addr=127.0.0.1:41002 generation=2 status=connected";
        // Each garbled membership item replaces the voter, and each garbled target the target.
        let garbled_membership = [
            "- node-1 voter 127.0.0.1:41001",
            "- node-1 [voter 127.0.0.1:41001",
        ];
        let garbled_targets = [
            "- node-2 addr=127.0.0.1:41002 generation=2 status=connected",
            "- node-2: addr 127.0.0.1:41002 generation=2 status=connected",
            "- node-2: addr=127.0.0.1:41002 generation=two status=connected",
            "- node-2: addr=127.0.0.1:41002 generation=2 status=reachable",
            "- node-2: addr=127.0.0.1:41002 status=connected",
            "- node-2: addr=127.0.0.1:41002 generation=2",
        ];
        /// One status with a single line the wait cannot read.
        struct GarbledStatus {
            garbled: &'static str,
            voter: &'static str,
            target: &'static str,
        }

        let mut statuses = Vec::new();
        for garbled in garbled_membership {
            statuses.push(GarbledStatus {
                garbled,
                voter: garbled,
                target: TARGET,
            });
        }
        for garbled in garbled_targets {
            statuses.push(GarbledStatus {
                garbled,
                voter: VOTER,
                target: garbled,
            });
        }
        for status in statuses {
            nervix_primitives::task::consume_budget().await;
            let garbled = status.garbled;
            let text = [
                "raft.current_leader: node-1",
                "raft.membership:",
                status.voter,
                "",
                "[interconnect]",
                status.target,
            ]
            .join("\n");
            let garbling =
                StandInNode::serve(StandInBehavior::Answer(command_result(completed(), &text)))
                    .await;
            let convergence = ClusterConvergence::new(
                BTreeMap::from([("node-1".to_string(), garbling.endpoint())]),
                CONVERGENCE_TEST_BUDGET,
            );

            let failure = timeout(STALLED_REQUEST_GUARD, convergence.wait())
                .await
                .assured("an unreadable status ends the wait before any budget does")
                .expect_err("a status the wait cannot read must fail it");

            let ConvergenceError::UnreadableStatus { member, line } = failure.current_context()
            else {
                panic!("the wait must name the unreadable status {garbled:?}, got {failure:#}");
            };
            assert_eq!(member, "node-1");
            assert_eq!(line, garbled);
        }
    }

    /// The wait names, by its budget, each condition of convergence a member's latest answer did
    /// not meet.
    #[nervix_primitives::test]
    async fn an_expired_wait_names_the_condition_a_member_did_not_meet() {
        struct UnmetCondition {
            first: String,
            second: String,
            expected: Divergence,
        }

        let connected_to = |peer| [target(peer, 1, "connected")];
        let cases = [
            UnmetCondition {
                first: member_status("node-1", "(none)", &connected_to("node-2")),
                second: member_status("node-2", "node-1", &connected_to("node-1")),
                expected: Divergence::NoLeader {
                    member: "node-1".to_string(),
                },
            },
            UnmetCondition {
                first: member_status("node-1", "node-3", &connected_to("node-2")),
                second: member_status("node-2", "node-3", &connected_to("node-1")),
                expected: Divergence::LeaderOutsideCluster {
                    member: "node-1".to_string(),
                    leader: "node-3".to_string(),
                },
            },
            UnmetCondition {
                first: member_status("node-1", "node-1", &connected_to("node-2")),
                second: member_status("node-2", "node-2", &connected_to("node-1")),
                expected: Divergence::LeadersDiffer {
                    member: "node-2".to_string(),
                    leader: "node-2".to_string(),
                    other: "node-1".to_string(),
                    other_leader: "node-1".to_string(),
                },
            },
            UnmetCondition {
                first: member_status("node-1", "node-1", &connected_to("node-2"))
                    .replace("- node-2 [voter]", "- node-2 [learner]"),
                second: member_status("node-2", "node-1", &connected_to("node-1")),
                expected: Divergence::VoterMissing {
                    member: "node-1".to_string(),
                    voter: "node-2".to_string(),
                },
            },
            UnmetCondition {
                first: member_status("node-1", "node-1", &connected_to("node-3")),
                second: member_status("node-2", "node-1", &connected_to("node-1")),
                expected: Divergence::PeerUnobserved {
                    member: "node-1".to_string(),
                    peer: "node-2".to_string(),
                },
            },
        ];
        /// How one case's wait ended, beside the condition it should name.
        struct ExpiredWait {
            expected: Divergence,
            waited: Result<(), Report<ConvergenceError>>,
        }

        let mut stand_ins = Vec::new();
        let mut waits = Vec::new();
        for case in cases {
            let first = StandInNode::serve(StandInBehavior::Answer(command_result(
                completed(),
                &case.first,
            )))
            .await;
            let second = StandInNode::serve(StandInBehavior::Answer(command_result(
                completed(),
                &case.second,
            )))
            .await;
            let convergence = ClusterConvergence::new(
                BTreeMap::from([
                    ("node-1".to_string(), first.endpoint()),
                    ("node-2".to_string(), second.endpoint()),
                ]),
                CONVERGENCE_EXPIRY_BUDGET,
            );
            stand_ins.push(first);
            stand_ins.push(second);
            waits.push(async move {
                ExpiredWait {
                    expected: case.expected,
                    waited: convergence.wait().await,
                }
            });
        }

        let outcomes = timeout(
            CONVERGENCE_TEST_BUDGET,
            futures_util::future::join_all(waits),
        )
        .await
        .assured("every wait ends at its budget");
        for outcome in outcomes {
            let failure = outcome
                .waited
                .expect_err("members that never converge must fail the wait");
            let ConvergenceError::NotConverged { divergence, .. } = failure.current_context()
            else {
                panic!("the wait must end by its budget, got {failure:#}");
            };
            assert_eq!(divergence, &outcome.expected);
        }
    }

    /// Every member must answer the round that converges, so a member that stops answering keeps
    /// the cluster from converging even when its last answer had converged.
    #[nervix_primitives::test]
    async fn a_member_that_stops_answering_keeps_the_cluster_from_converging() {
        let unprobed = PublishedAnswer::status(&member_status(
            "node-1",
            "node-1",
            &[target("node-2", 1, "unknown")],
        ));
        let first = PublishedMember::new(&unprobed);
        let answering = PublishedAnswer::status(&member_status(
            "node-2",
            "node-1",
            &[target("node-1", 1, "connected")],
        ));
        let second = PublishedMember::new(&answering);
        let node_1 = StandInNode::serve(first.behavior()).await;
        let node_2 = StandInNode::serve(second.behavior()).await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), node_1.endpoint()),
                ("node-2".to_string(), node_2.endpoint()),
            ]),
            CONVERGENCE_EXPIRY_BUDGET,
        );
        let mut converging = std::pin::pin!(convergence.wait());

        // node-2 answers with a converged status, then fails every later request, while node-1
        // reaches node-2 only after node-2 has stopped answering.
        let premature = nervix_primitives::select! {
            converged = &mut converging => Some(converged),
            () = answering.answered(1) => None,
        };
        if let Some(converged) = premature {
            panic!("the cluster converged while node-1 had not reached node-2: {converged:?}");
        }
        second.publish(&PublishedAnswer {
            result: command_result(CommandDisposition::Failed, "the node is stopping"),
            answered: Arc::new(watch::Sender::new(0)),
        });
        first.publish(&PublishedAnswer::status(&member_status(
            "node-1",
            "node-1",
            &[target("node-2", 1, "connected")],
        )));
        let failure = timeout(CONVERGENCE_TEST_BUDGET, converging)
            .await
            .assured("a wait that never converges ends at its budget")
            .expect_err("a member that stopped answering must keep the cluster from converging");

        let ConvergenceError::NotConverged { divergence, .. } = failure.current_context() else {
            panic!("the wait must end by its budget, got {failure:#}");
        };
        assert_eq!(
            divergence,
            &Divergence::StatusUnanswered {
                member: "node-2".to_string(),
            }
        );
        let rendered = format!("{failure:#}");
        assert!(rendered.contains("- node-2 last reported "), "{rendered}");
        assert!(
            rendered.contains("- node-2 did not answer its latest request: "),
            "{rendered}"
        );
    }

    #[nervix_primitives::test]
    async fn convergence_ends_at_its_budget_with_what_had_not_converged_and_what_each_member_said()
    {
        let first = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 1, "probe-failed"),
                    target("node-3", 2, "connected"),
                ],
            ),
        )))
        .await;
        let second = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-2",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-3", 2, "connected"),
                ],
            ),
        )))
        .await;
        let refused_address = RefusedAddress::bind();
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), first.endpoint()),
                ("node-2".to_string(), second.endpoint()),
                (
                    "node-3".to_string(),
                    plaintext_endpoint(refused_address.address),
                ),
            ]),
            CONVERGENCE_EXPIRY_BUDGET,
        );
        let started = Instant::now();

        let failure = timeout(CONVERGENCE_TEST_BUDGET, convergence.wait())
            .await
            .assured("a wait that never converges ends at its budget")
            .expect_err("members that never converge must fail the wait");

        assert!(started.elapsed() >= CONVERGENCE_EXPIRY_BUDGET);
        let ConvergenceError::NotConverged {
            budget, divergence, ..
        } = failure.current_context()
        else {
            panic!("the wait must end by its budget, got {failure:#}");
        };
        assert_eq!(*budget, CONVERGENCE_EXPIRY_BUDGET);
        assert_eq!(
            divergence,
            &Divergence::PeerUnreached {
                member: "node-1".to_string(),
                peer: "node-2".to_string(),
                reachability: PeerReachability::ProbeFailed,
            }
        );
        let rendered = format!("{failure:#}");
        assert!(rendered.contains("- node-1 last reported "), "{rendered}");
        assert!(rendered.contains("- node-2 last reported "), "{rendered}");
        assert!(
            rendered.contains("- node-3 did not answer its latest request: "),
            "{rendered}"
        );
        assert!(!rendered.contains("- node-3 last reported "), "{rendered}");
    }

    /// A running member that had already stopped observing the exited process holds no target for
    /// it, so the first target it reports after the relaunch observes the new process.
    #[nervix_primitives::test]
    async fn a_member_that_had_stopped_observing_the_exited_process_converges_on_its_first_target()
    {
        let leader = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-1",
            "node-1",
            &[target("node-3", 2, "connected")],
        )));
        let relaunched = PublishedMember::new(&PublishedAnswer {
            result: command_result(CommandDisposition::Failed, "the process has exited"),
            answered: Arc::new(watch::Sender::new(0)),
        });
        let other = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-3",
            "node-1",
            &[target("node-1", 1, "connected")],
        )));
        let node_1 = StandInNode::serve(leader.behavior()).await;
        let node_2 = StandInNode::serve(relaunched.behavior()).await;
        let node_3 = StandInNode::serve(other.behavior()).await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), node_1.endpoint()),
                ("node-2".to_string(), node_2.endpoint()),
                ("node-3".to_string(), node_3.endpoint()),
            ]),
            CONVERGENCE_TEST_BUDGET,
        );
        let relaunch = async {
            relaunched.publish(&PublishedAnswer::status(&member_status(
                "node-2",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-3", 1, "connected"),
                ],
            )));
            leader.publish(&PublishedAnswer::status(&member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 1, "connected"),
                    target("node-3", 2, "connected"),
                ],
            )));
            other.publish(&PublishedAnswer::status(&member_status(
                "node-3",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-2", 1, "connected"),
                ],
            )));
            Ok(node_2.endpoint())
        };

        timeout(
            CONVERGENCE_TEST_BUDGET,
            convergence.relaunch("node-2", relaunch),
        )
        .await
        .assured("the relaunch ends within its own budget")
        .assured("a member with no target for the exited process converges on its first one");
    }

    /// The targets the running members observed the exited process through decide what counts as
    /// the new process, so a relaunch does not start before every running member has reported
    /// them.
    #[nervix_primitives::test]
    async fn a_relaunch_relaunches_nothing_while_a_running_member_has_not_reported_its_targets() {
        let leader = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 4, "unavailable"),
                    target("node-3", 2, "connected"),
                ],
            ),
        )))
        .await;
        let exited = StandInNode::serve(StandInBehavior::EndSession).await;
        let refusing = StandInNode::serve(StandInBehavior::RejectSession).await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), leader.endpoint()),
                ("node-2".to_string(), exited.endpoint()),
                ("node-3".to_string(), refusing.endpoint()),
            ]),
            CONVERGENCE_EXPIRY_BUDGET,
        );
        let relaunches = AtomicUsize::new(0);
        let relaunch = async {
            relaunches.fetch_add(1, Ordering::SeqCst);
            Ok(exited.endpoint())
        };

        let failure = timeout(
            CONVERGENCE_TEST_BUDGET,
            convergence.relaunch("node-2", relaunch),
        )
        .await
        .assured("reading the targets ends at its budget")
        .expect_err("a member that never reports its targets must fail the relaunch");

        assert_eq!(relaunches.load(Ordering::SeqCst), 0);
        let ConvergenceError::ExitedTargetsUnread {
            relaunched, budget, ..
        } = failure.current_context()
        else {
            panic!("the relaunch must name the unread targets, got {failure:#}");
        };
        assert_eq!(relaunched, "node-2");
        assert_eq!(*budget, CONVERGENCE_EXPIRY_BUDGET);
        let rendered = format!("{failure:#}");
        assert!(
            rendered.contains("- node-3 did not answer its latest request: "),
            "{rendered}"
        );
        assert!(!rendered.contains("- node-2 last reported"), "{rendered}");
        assert!(!rendered.contains("- node-2 did not answer"), "{rendered}");
    }

    #[nervix_primitives::test]
    async fn a_failed_relaunch_keeps_its_cause_beneath_the_member_it_relaunched() {
        let leader = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 4, "unavailable"),
                    target("node-3", 2, "connected"),
                ],
            ),
        )))
        .await;
        let exited = StandInNode::serve(StandInBehavior::EndSession).await;
        let other = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-3",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-2", 6, "unavailable"),
                ],
            ),
        )))
        .await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), leader.endpoint()),
                ("node-2".to_string(), exited.endpoint()),
                ("node-3".to_string(), other.endpoint()),
            ]),
            CONVERGENCE_TEST_BUDGET,
        );

        let failure = timeout(
            CONVERGENCE_TEST_BUDGET,
            convergence.relaunch("node-2", async {
                Err(io::Error::other(
                    "the relaunched process exited during startup",
                ))
            }),
        )
        .await
        .assured("a failed relaunch ends the wait at once")
        .expect_err("a relaunch that fails must fail the restart");

        assert!(matches!(
            failure.current_context(),
            ConvergenceError::Relaunch { member } if member == "node-2"
        ));
        let Some(cause) = failure.downcast_ref::<io::Error>() else {
            panic!("the relaunch's own failure must stay beneath it, got {failure:#}");
        };
        assert_eq!(
            cause.to_string(),
            "the relaunched process exited during startup"
        );
    }

    /// A relaunch can move the relaunched member's listeners to fresh ports, so the members
    /// converge on the new process only where the relaunch says it answers: the exited process's
    /// endpoint never answers again.
    #[nervix_primitives::test]
    async fn a_relaunched_member_is_asked_where_its_new_process_answers() {
        // node-2's process has exited. node-1 leads and observes it through generation 4, node-3
        // through generation 6.
        let leader = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-1",
            "node-1",
            &[
                target("node-2", 4, "unavailable"),
                target("node-3", 2, "connected"),
            ],
        )));
        let other = PublishedMember::new(&PublishedAnswer::status(&member_status(
            "node-3",
            "node-1",
            &[
                target("node-1", 1, "connected"),
                target("node-2", 6, "unavailable"),
            ],
        )));
        let node_1 = StandInNode::serve(leader.behavior()).await;
        let exited = StandInNode::serve(StandInBehavior::EndSession).await;
        let node_3 = StandInNode::serve(other.behavior()).await;
        let convergence = ClusterConvergence::new(
            BTreeMap::from([
                ("node-1".to_string(), node_1.endpoint()),
                ("node-2".to_string(), exited.endpoint()),
                ("node-3".to_string(), node_3.endpoint()),
            ]),
            CONVERGENCE_TEST_BUDGET,
        );
        // The new process answers on another port. It follows the leader and reaches both peers,
        // and both peers reach it through a target newer than the exited process's.
        let moved = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            &member_status(
                "node-2",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-3", 2, "connected"),
                ],
            ),
        )))
        .await;
        let relaunch = async {
            leader.publish(&PublishedAnswer::status(&member_status(
                "node-1",
                "node-1",
                &[
                    target("node-2", 5, "connected"),
                    target("node-3", 2, "connected"),
                ],
            )));
            other.publish(&PublishedAnswer::status(&member_status(
                "node-3",
                "node-1",
                &[
                    target("node-1", 1, "connected"),
                    target("node-2", 7, "connected"),
                ],
            )));
            Ok(moved.endpoint())
        };

        timeout(
            CONVERGENCE_TEST_BUDGET,
            convergence.relaunch("node-2", relaunch),
        )
        .await
        .assured("the relaunch ends within its own budget")
        .assured("the members converge where the new process answers");
    }

    #[nervix_primitives::test]
    async fn failed_and_stalled_diagnostics_end_by_their_deadline_so_cleanup_starts() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;
        let healthy = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
            HEALTHY_STATUS,
        )))
        .await;
        let refused_address = RefusedAddress::bind();
        let endpoints = BTreeMap::from([
            ("node-1".to_string(), healthy.endpoint()),
            ("node-2".to_string(), stalled.endpoint()),
            (
                "node-3".to_string(),
                plaintext_endpoint(refused_address.address),
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

    #[nervix_primitives::test]
    async fn stop_and_keeper_paths_use_the_task_inspected_for_diagnostics() {
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

        let kept_drops = Arc::new(AtomicUsize::new(0));
        let kept_guard = kept_drops.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let mut running = OwnedNodeTask::spawn(async move {
            let _guard = DropCount(kept_guard);
            started_tx
                .send(())
                .assured("the test retains the startup receiver until the task begins");
            task_stop.cancelled().await;
            Ok(())
        });
        started_rx
            .await
            .assured("the running task sends after installing its drop guard");
        assert!(matches!(running.inspect().await, NodeTaskState::Running));

        let (ended_tx, ended_rx) = oneshot::channel();
        assert!(
            running.keep_until_ended(move |outcome| {
                let clean = matches!(outcome, NodeTaskTerminalOutcome::CleanApplicationExit);
                ended_tx
                    .send(clean)
                    .assured("the test retains the keeper's receiver until the task ends");
            }),
            "a running task must be handed to its keeper"
        );
        assert!(matches!(running.state(), NodeTaskState::LeftStopping));
        assert!(
            matches!(
                running.wait(PhaseDeadline::after(TEST_TIMEOUT)).await,
                NodeTaskWaitOutcome::LeftStopping
            ),
            "an owner that handed its task over has nothing left to wait for"
        );
        assert!(
            !running.keep_until_ended(|_| panic!("a task is handed to one keeper only")),
            "a task that was handed over cannot be handed over again"
        );
        // Handing the task over ends nothing: the node still runs until it ends itself.
        for _ in 0..8 {
            nervix_primitives::task::yield_now().await;
        }
        assert_eq!(kept_drops.load(Ordering::SeqCst), 0);

        stop.cancel();
        let ended_cleanly = timeout(TEST_TIMEOUT, ended_rx)
            .await
            .assured("the keeper reports the task's end before the test deadline")
            .assured("the keeper sends how the task ended");
        assert!(ended_cleanly, "the keeper must report how the task ended");
        assert_eq!(kept_drops.load(Ordering::SeqCst), 1);
    }

    /// How a stand-in node's task ends once cleanup has asked it to stop.
    #[derive(Clone, Copy, Debug)]
    enum StandInEnding {
        StopsWhenAsked,
        FailsWhenAsked,
        PanicsWhenAsked,
        /// Ignores the stop request and ends only once the regression lets it, after cleanup has
        /// stopped waiting for it.
        OutlastsCleanup,
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
                // Nothing cancels a node that outlasts its cleanup: it ends itself, cleanly.
                Self::OutlastsCleanup => {
                    matches!(outcome, NodeTaskTerminalOutcome::CleanApplicationExit)
                }
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
        /// Cleanup stopped waiting for the node and left it to end itself.
        LeftStopping {
            node: String,
        },
        /// The keeper of a node that was left stopping gave back what the node held, having seen
        /// the node end the way the regression asked.
        KeptUntilEnded {
            node: String,
            ended_as_asked: bool,
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

        fn left_stopping(&self) -> Vec<String> {
            self.events()
                .into_iter()
                .filter_map(|event| match event {
                    CleanupEvent::LeftStopping { node } => Some(node),
                    _ => None,
                })
                .collect()
        }

        /// The nodes whose keeper gave back what they held.
        fn kept_until_ended(&self) -> Vec<String> {
            self.events()
                .into_iter()
                .filter_map(|event| match event {
                    CleanupEvent::KeptUntilEnded { node, .. } => Some(node),
                    _ => None,
                })
                .collect()
        }

        /// Whether the keeper of `node` gave back what the node held only once the node's own task
        /// had ended, and saw it end the way the regression asked.
        fn kept_until_its_task_ended(&self, node: &str) -> bool {
            let events = self.events();
            let ended = events.iter().position(
                |event| matches!(event, CleanupEvent::TaskEnded { node: ended } if ended == node),
            );
            let given_back = events.iter().position(|event| {
                matches!(
                    event,
                    CleanupEvent::KeptUntilEnded { node: kept, ended_as_asked: true } if kept == node
                )
            });
            match (ended, given_back) {
                (Some(ended), Some(given_back)) => ended < given_back,
                (None, _) | (_, None) => false,
            }
        }
    }

    /// Records that a node's task ended, whether it returned, failed or panicked.
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
        ending: StandInEnding,
        stop: Arc<Notify>,
        /// Lets a node that outlasts its cleanup end, which nothing but the regression does.
        let_end: CancellationToken,
        task: OwnedNodeTask,
        log: Arc<CleanupLog>,
        scenario: Option<Arc<ActiveScenarioRegistration>>,
        /// What the node's shutdown reads as while it stops, when the regression gives it one.
        progress: Option<NodeStopProgress>,
    }

    impl StandInTeardownNode {
        fn new(name: &str, ending: StandInEnding, log: &Arc<CleanupLog>) -> Self {
            let stop = Arc::new(Notify::new());
            let task_stop = stop.clone();
            let let_end = CancellationToken::new();
            let task_let_end = let_end.clone();
            let task_end = TaskEnd {
                node: name.to_string(),
                log: log.clone(),
            };
            let task = OwnedNodeTask::spawn(async move {
                let _ended = task_end;
                match ending {
                    StandInEnding::OutlastsCleanup => {
                        task_let_end.cancelled().await;
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
                ending,
                stop,
                let_end,
                task,
                log: log.clone(),
                scenario: None,
                progress: None,
            }
        }

        /// Gives the node a shutdown that reads as `progress` for as long as it stops.
        fn stopping_in(mut self, progress: NodeStopProgress) -> Self {
            self.progress = Some(progress);
            self
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

        fn stop_progress(&self) -> Option<NodeStopProgress> {
            self.progress
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

        fn leave_stopping(&mut self) {
            self.log.record(CleanupEvent::LeftStopping {
                node: self.name.clone(),
            });
            let log = self.log.clone();
            let node = self.name.clone();
            let ending = self.ending;
            self.task.keep_until_ended(move |outcome| {
                log.record(CleanupEvent::KeptUntilEnded {
                    node,
                    ended_as_asked: ending.ended_as(outcome),
                });
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_single_node_cleanup_keeps_how_its_task_ended() {
        for ending in [
            StandInEnding::StopsWhenAsked,
            StandInEnding::FailsWhenAsked,
            StandInEnding::PanicsWhenAsked,
        ] {
            let log = Arc::new(CleanupLog::default());
            let mut nodes = stand_in_cluster(&[("node-1", ending)], &log);

            let teardown = ClusterTeardown::stop_all(
                nodes.iter_mut(),
                PhaseDeadline::after(TEST_TEARDOWN_BUDGET),
            )
            .await;

            assert!(log.stop_was_requested("node-1"));
            assert!(
                teardown.elapsed < TEST_TEARDOWN_BUDGET,
                "a node that stops when asked must not reach the cleanup deadline: {teardown}"
            );
            assert!(!teardown.left_nodes_stopping(), "{teardown}");
            let [node] = teardown.nodes.as_slice() else {
                panic!("a cluster of one reports one node: {teardown}");
            };
            let NodeTaskWaitOutcome::Joined(outcome) = &node.stop else {
                panic!("a node that ends itself must be joined, not left stopping: {node}");
            };
            assert!(
                ending.ended_as(outcome.as_ref()),
                "cleanup must keep how the task of a node that {ending:?} ended, got {outcome}"
            );
            // A caller that failed before this cleanup reports only what the cleanup left
            // unclean: an application error is how a node ended, and a panic is a record.
            match (ending, teardown.unclean()) {
                (StandInEnding::PanicsWhenAsked, Some(unclean)) => {
                    assert!(unclean.contains("node-1"), "{unclean}");
                }
                (StandInEnding::PanicsWhenAsked, None) => {
                    panic!("a node that panicked while stopping must be named: {teardown}")
                }
                (_, Some(unclean)) => {
                    panic!("a node that ended without a panic leaves no record: {unclean}")
                }
                (_, None) => {}
            }
            assert_eq!(log.released(), vec!["node-1".to_string()]);
            assert!(
                log.released_only_after_every_task_ended(),
                "{:?}",
                log.events()
            );
        }
    }

    #[nervix_primitives::test(start_paused = true)]
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

        let teardown =
            ClusterTeardown::stop_all(nodes.iter_mut(), PhaseDeadline::after(TEST_TEARDOWN_BUDGET))
                .await;

        assert!(!teardown.left_nodes_stopping(), "{teardown}");
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

    #[nervix_primitives::test(start_paused = true)]
    async fn stuck_nodes_spend_one_cleanup_budget_in_a_cluster_of_one_and_of_three() {
        for node_count in [1_usize, 3] {
            let log = Arc::new(CleanupLog::default());
            let names = (1..=node_count)
                .map(|index| format!("node-{index}"))
                .collect::<Vec<_>>();
            let stuck = names
                .iter()
                .map(|name| (name.as_str(), StandInEnding::OutlastsCleanup))
                .collect::<Vec<_>>();
            let mut nodes = stand_in_cluster(&stuck, &log);

            let started = Instant::now();
            let teardown = ClusterTeardown::stop_all(
                nodes.iter_mut(),
                PhaseDeadline::after(TEST_TEARDOWN_BUDGET),
            )
            .await;
            let spent = started.elapsed();

            assert!(spent >= TEST_TEARDOWN_BUDGET, "{teardown}");
            assert!(
                spent < TEST_SHARED_TEARDOWN_BOUND,
                "{node_count} stuck node(s) must share one cleanup budget, but cleanup took \
                 {spent:?}: {teardown}"
            );
            for node in &teardown.nodes {
                assert!(log.stop_was_requested(&node.node));
                assert!(
                    matches!(node.stop, NodeTaskWaitOutcome::StillRunning),
                    "a node that outlasts the deadline must be left stopping, not ended: {node}"
                );
            }
            assert_eq!(teardown.still_stopping().count(), node_count, "{teardown}");
            assert_eq!(log.left_stopping(), names);
            assert!(
                log.released().is_empty() && log.kept_until_ended().is_empty(),
                "nothing a node still uses may be given back while its task runs: {:?}",
                log.events()
            );
            for node in &nodes {
                assert!(
                    matches!(node.task.state(), NodeTaskState::LeftStopping),
                    "a node left stopping is owned by its keeper"
                );
            }
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn finishing_background_work_does_not_restart_the_cluster_cleanup_budget() {
        let log = Arc::new(CleanupLog::default());
        let mut nodes = stand_in_cluster(&[("node-1", StandInEnding::OutlastsCleanup)], &log);
        let deadline = PhaseDeadline::after(TEST_TEARDOWN_BUDGET);
        let prestop = TEST_TEARDOWN_BUDGET
            .checked_div(2)
            .assured("the divisor is nonzero");
        nervix_primitives::time::advance(prestop).await;

        let teardown = ClusterTeardown::stop_all(nodes.iter_mut(), deadline).await;

        assert_eq!(deadline.elapsed(), TEST_TEARDOWN_BUDGET);
        assert_eq!(teardown.elapsed, TEST_TEARDOWN_BUDGET);
        assert_eq!(teardown.budget, TEST_TEARDOWN_BUDGET);
        assert!(log.stop_was_requested("node-1"));
        assert_eq!(teardown.still_stopping().count(), 1);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_node_left_stopping_keeps_what_it_holds_until_its_own_task_ends() {
        let log = Arc::new(CleanupLog::default());
        let mut nodes = stand_in_cluster(
            &[
                ("node-1", StandInEnding::StopsWhenAsked),
                ("node-2", StandInEnding::OutlastsCleanup),
                ("node-3", StandInEnding::OutlastsCleanup),
            ],
            &log,
        );

        let teardown =
            ClusterTeardown::stop_all(nodes.iter_mut(), PhaseDeadline::after(TEST_TEARDOWN_BUDGET))
                .await;

        let still_stopping = teardown
            .still_stopping()
            .map(|node| node.node.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            still_stopping,
            vec!["node-2".to_string(), "node-3".to_string()],
            "{teardown}"
        );
        assert_eq!(
            teardown.to_string(),
            format!(
                "stopped 1 of 3 node(s) in {:?} of a {TEST_TEARDOWN_BUDGET:?} budget, 2 still \
                 stopping",
                teardown.elapsed
            )
        );
        let unclean = teardown
            .unclean()
            .assured("nodes left stopping are a record for the caller");
        assert!(
            unclean.contains("node \"node-2\" was still stopping at the cleanup deadline")
                && unclean.contains("node \"node-3\" was still stopping at the cleanup deadline")
                && !unclean.contains("node-1"),
            "{unclean}"
        );
        // The node that ended gives back what it held; the two still stopping keep theirs.
        assert_eq!(log.released(), vec!["node-1".to_string()]);
        assert!(log.kept_until_ended().is_empty(), "{:?}", log.events());

        // One of them ends: its keeper gives back what it held, and the other keeps running.
        nodes[1].let_end.cancel();
        timeout(TEST_TIMEOUT, async {
            while log.kept_until_ended().is_empty() {
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .assured("the keeper sees its node end before the test deadline");
        assert_eq!(log.kept_until_ended(), vec!["node-2".to_string()]);
        assert!(
            log.kept_until_its_task_ended("node-2"),
            "{:?}",
            log.events()
        );

        nodes[2].let_end.cancel();
        timeout(TEST_TIMEOUT, async {
            while log.kept_until_ended().len() < 2 {
                nervix_primitives::task::yield_now().await;
            }
        })
        .await
        .assured("the second keeper sees its node end before the test deadline");
        assert!(
            log.kept_until_its_task_ended("node-3"),
            "{:?}",
            log.events()
        );
        assert_eq!(
            log.released(),
            vec!["node-1".to_string()],
            "a node left stopping is given back by its keeper, never by the cleanup that left it"
        );
    }

    /// How a node of a diagnostics regression answers the status request its cleanup sends.
    #[derive(Clone, Copy, Debug)]
    enum StandInDiagnostic {
        Answers,
        Stalls,
    }

    #[nervix_primitives::test]
    async fn a_stalled_diagnostic_still_reaches_every_node_stop_in_a_cluster_of_one_and_of_three() {
        let stalled = StandInNode::serve(StandInBehavior::WithholdSession).await;
        let healthy = StandInNode::serve(StandInBehavior::Answer(command_result(
            completed(),
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
            let teardown = ClusterTeardown::stop_all(
                nodes.iter_mut(),
                PhaseDeadline::after(TEST_TEARDOWN_BUDGET),
            )
            .await;

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
            assert!(!teardown.left_nodes_stopping(), "{teardown}");
            assert_eq!(log.released().len(), cluster.len());
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_node_left_stopping_is_recorded_with_where_its_own_shutdown_stood() {
        let log = Arc::new(CleanupLog::default());
        let mut nodes =
            [
                StandInTeardownNode::new("node-1", StandInEnding::StopsWhenAsked, &log)
                    .stopping_in(NodeStopProgress {
                        phase: ShutdownPhase::DrainSupport,
                        deadline: ProductShutdownDeadline::Ahead(Duration::from_secs(200)),
                    }),
                StandInTeardownNode::new("node-2", StandInEnding::OutlastsCleanup, &log)
                    .stopping_in(NodeStopProgress {
                        phase: ShutdownPhase::TerminalTeardown,
                        deadline: ProductShutdownDeadline::Passed,
                    }),
                StandInTeardownNode::new("node-3", StandInEnding::OutlastsCleanup, &log),
            ];

        let teardown =
            ClusterTeardown::stop_all(nodes.iter_mut(), PhaseDeadline::after(TEST_TEARDOWN_BUDGET))
                .await;

        let [ended, read, unread] = teardown.nodes.as_slice() else {
            panic!("a cluster of three reports three nodes: {teardown}");
        };
        // A node that ended inside the budget has no shutdown left to describe.
        assert_eq!(ended.progress, None, "{ended}");
        assert_eq!(
            read.to_string(),
            "node \"node-2\" was still stopping at the cleanup deadline, in shutdown phase \
             TerminalTeardown, past its own shutdown deadline, and keeps its storage and ports \
             until it ends"
        );
        // A node whose shutdown cannot be read is still named as stopping.
        assert_eq!(
            unread.to_string(),
            "node \"node-3\" was still stopping at the cleanup deadline and keeps its storage and \
             ports until it ends"
        );
        assert_eq!(
            log.left_stopping(),
            vec!["node-2".to_string(), "node-3".to_string()]
        );
    }

    /// The test nodes' shutdown timeout, which a stopping node's deadline is read against.
    const STAND_IN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(240);

    #[nervix_primitives::test(start_paused = true)]
    async fn a_stopping_node_is_read_with_its_shutdown_phase_and_its_own_deadline() {
        let shutdown = ShutdownCoordinator::new(STAND_IN_SHUTDOWN_TIMEOUT);

        let serving = NodeStopProgress::observe(&shutdown);
        assert_eq!(
            serving,
            NodeStopProgress {
                phase: ShutdownPhase::Serving,
                deadline: ProductShutdownDeadline::NotRequested,
            }
        );
        assert_eq!(
            serving.to_string(),
            "in shutdown phase Serving, with no stop request accepted"
        );

        shutdown.request_stop();
        nervix_primitives::time::advance(Duration::from_secs(90)).await;
        let stopping = NodeStopProgress::observe(&shutdown);
        assert_eq!(
            stopping,
            NodeStopProgress {
                phase: ShutdownPhase::StopRequested,
                deadline: ProductShutdownDeadline::Ahead(Duration::from_secs(150)),
            }
        );
        assert_eq!(
            stopping.to_string(),
            "in shutdown phase StopRequested, 150s before its own shutdown deadline"
        );

        // An in-process node has no deadline supervision, so it is read past its deadline for as
        // long as it goes on stopping.
        nervix_primitives::time::advance(Duration::from_secs(150)).await;
        let overdue = NodeStopProgress::observe(&shutdown);
        assert_eq!(overdue.phase, ShutdownPhase::StopRequested);
        assert_eq!(overdue.deadline, ProductShutdownDeadline::Passed);
        nervix_primitives::time::advance(Duration::from_secs(60)).await;
        assert_eq!(NodeStopProgress::observe(&shutdown), overdue);
        assert_eq!(
            overdue.to_string(),
            "in shutdown phase StopRequested, past its own shutdown deadline"
        );
    }

    #[test]
    fn a_stop_record_reads_as_the_transition_the_harness_prints() {
        let ended = NodeStopRecord {
            node: "node-2".to_string(),
            stopping_for: Some(Duration::from_millis(480)),
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::Ended {
                task: Arc::new(NodeTaskTerminalOutcome::CleanApplicationExit),
                shutdown: Some(ShutdownOutcome {
                    stop_admission: ShutdownPhaseOutcome::Completed,
                    drain_support: ShutdownPhaseOutcome::Abandoned,
                    terminal_teardown: ShutdownPhaseOutcome::Completed,
                }),
            },
        };
        assert_eq!(
            ended.to_string(),
            "node 'node-2' stop: ended 480ms after its stop request, inside its 300s watchdog: \
             clean application exit; shutdown phases: stop admission Completed, drain support \
             Abandoned, terminal teardown Completed"
        );

        let unreported = NodeStopRecord {
            node: "node-3".to_string(),
            stopping_for: Some(Duration::from_secs(2)),
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::Ended {
                task: Arc::new(NodeTaskTerminalOutcome::CleanApplicationExit),
                shutdown: None,
            },
        };
        assert_eq!(
            unreported.to_string(),
            "node 'node-3' stop: ended 2s after its stop request, inside its 300s watchdog: clean \
             application exit; no shutdown outcome was reported"
        );

        let still_stopping = NodeStopRecord {
            node: "restored-1".to_string(),
            stopping_for: Some(Duration::from_secs(300)),
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::StillStopping(NodeStopProgress {
                phase: ShutdownPhase::TerminalTeardown,
                deadline: ProductShutdownDeadline::Passed,
            }),
        };
        assert_eq!(
            still_stopping.to_string(),
            "node 'restored-1' stop: still stopping 300s after its stop request, at the end of \
             its 300s watchdog, in shutdown phase TerminalTeardown, past its own shutdown deadline"
        );

        let kept = NodeStopRecord {
            node: "restored-2".to_string(),
            stopping_for: Some(Duration::from_secs(75)),
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::LeftToKeeper,
        };
        assert_eq!(
            kept.to_string(),
            "node 'restored-2' stop: still stopping 75s after its stop request; it had already \
             been left to a keeper, which waits for it to end"
        );

        let unasked = NodeStopRecord {
            node: "node-1".to_string(),
            stopping_for: None,
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::Ended {
                task: Arc::new(NodeTaskTerminalOutcome::CleanApplicationExit),
                shutdown: None,
            },
        };
        assert_eq!(
            unasked.to_string(),
            "node 'node-1' stop: ended without a stop request from the harness, inside its 300s \
             watchdog: clean application exit; no shutdown outcome was reported"
        );

        // A node that was to end by itself and did not is read the same way as one the harness
        // stopped.
        let unasked_and_running = NodeStopRecord {
            node: "node-1".to_string(),
            stopping_for: None,
            watchdog: Duration::from_secs(300),
            ending: NodeStopEnding::StillStopping(NodeStopProgress {
                phase: ShutdownPhase::Serving,
                deadline: ProductShutdownDeadline::NotRequested,
            }),
        };
        assert_eq!(
            unasked_and_running.to_string(),
            "node 'node-1' stop: still running without a stop request from the harness, at the \
             end of its 300s watchdog, in shutdown phase Serving, with no stop request accepted"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_stop_is_timed_from_its_first_request_to_the_ending_of_the_node_task() {
        let (mut clock, ended) = NodeStopClock::start();
        assert_eq!(
            clock.stopping_for(),
            None,
            "a node the harness never asked to stop has no stop to time"
        );

        nervix_primitives::time::advance(Duration::from_secs(7)).await;
        clock.stop_requested();
        nervix_primitives::time::advance(Duration::from_secs(3)).await;
        assert_eq!(clock.stopping_for(), Some(Duration::from_secs(3)));

        // A later request changes nothing, as it changes nothing in the node.
        clock.stop_requested();
        nervix_primitives::time::advance(Duration::from_secs(2)).await;
        assert_eq!(clock.stopping_for(), Some(Duration::from_secs(5)));

        // The task records its own ending, so a harness that gets round to the node later still
        // reads how long the node itself took.
        drop(ended);
        nervix_primitives::time::advance(Duration::from_secs(60)).await;
        assert_eq!(clock.stopping_for(), Some(Duration::from_secs(5)));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_node_that_had_ended_before_it_was_asked_to_stop_took_no_time_to_stop() {
        let (mut clock, ended) = NodeStopClock::start();
        drop(ended);
        nervix_primitives::time::advance(Duration::from_secs(4)).await;

        clock.stop_requested();

        assert_eq!(clock.stopping_for(), Some(Duration::ZERO));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_finished_phase_is_published_only_once_cleanup_has_completed() {
        use crate::scenario_schedule::ScenarioLength;

        let scenario = Arc::new(ActiveScenarioRegistration::start(
            "Harness liveness",
            "cleanup publishes truthful phases",
            STAND_IN_SCENARIO_LINE,
            ScenarioLength::Ordinary,
        ));
        let log = Arc::new(CleanupLog::default());
        let mut nodes = [
            StandInTeardownNode::new("node-1", StandInEnding::StopsWhenAsked, &log)
                .reading(&scenario),
        ];

        scenario.enter(ScenarioPhase::BodyComplete);
        scenario.enter(ScenarioPhase::TeardownStarted);
        scenario.enter(ScenarioPhase::Diagnostics);
        scenario.enter(ScenarioPhase::Stopping);
        let teardown =
            ClusterTeardown::stop_all(nodes.iter_mut(), PhaseDeadline::after(TEST_TEARDOWN_BUDGET))
                .await;
        assert_eq!(published(&scenario).phase, ScenarioPhase::Stopping);
        scenario.enter(ScenarioPhase::Finished);

        assert!(!teardown.left_nodes_stopping(), "{teardown}");
        assert_eq!(
            log.phase_while_releasing("node-1"),
            Some(ScenarioPhase::Stopping),
            "cleanup that is still giving state back must not publish that it has finished"
        );
        assert_eq!(published(&scenario).phase, ScenarioPhase::Finished);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn an_active_scenario_publishes_its_phase_and_the_age_of_that_phase() {
        use crate::scenario_schedule::ScenarioLength;

        let scenario = ActiveScenarioRegistration::start(
            "Harness liveness",
            "phase ages",
            STAND_IN_SCENARIO_LINE,
            ScenarioLength::Ordinary,
        );
        let queued = published(&scenario);
        assert_eq!(&queued.identity, scenario.identity());
        assert_eq!(queued.phase, ScenarioPhase::Queued);
        assert_eq!(queued.identity.feature, "Harness liveness");
        assert_eq!(queued.identity.scenario, "phase ages");

        // A scenario that has not reached its first step ages in the phase it is waiting in.
        nervix_primitives::time::advance(Duration::from_secs(30)).await;
        let waiting = published(&scenario);
        assert_eq!(waiting.phase, ScenarioPhase::Queued);
        assert_eq!(waiting.phase_age(), Duration::from_secs(30));
        assert_eq!(waiting.age(), Duration::from_secs(30));

        let started = scenario.enter(ScenarioPhase::Body);
        assert_eq!(started.phase, ScenarioPhase::Body);
        assert_eq!(started.phase_age(), Duration::ZERO);
        assert_eq!(started.age(), Duration::from_secs(30));
        nervix_primitives::time::advance(Duration::from_secs(5)).await;
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

    #[nervix_primitives::test]
    async fn the_running_scenarios_are_those_that_hold_their_run_slots() {
        use crate::scenario_schedule::ScenarioLength;

        let start = |scenario: &str| {
            ActiveScenarioRegistration::start(
                "Harness liveness",
                scenario,
                STAND_IN_SCENARIO_LINE,
                ScenarioLength::Ordinary,
            )
        };
        let queued = start("running set: queued for its permits");
        let running = start("running set: running its steps");
        running.start_running(1);
        let stopping = start("running set: stopping its cluster");
        stopping.start_running(1);
        stopping.enter(ScenarioPhase::Stopping);
        // Cucumber keeps the world of a failed attempt for its report, and with it the attempt's
        // registration, after the attempt has finished.
        let finished = start("running set: finished and retained");
        finished.start_running(1);
        finished.enter(ScenarioPhase::Finished);

        let mut holding = BTreeSet::new();
        for active in ActiveScenario::running() {
            holding.insert(active.identity);
        }
        assert!(holding.contains(running.identity()));
        assert!(holding.contains(stopping.identity()));
        assert!(
            !holding.contains(queued.identity()),
            "a scenario queued for its permits runs nothing that can hold the runner task"
        );
        assert!(!holding.contains(finished.identity()));

        let mut unfinished = BTreeSet::new();
        for active in ActiveScenario::unfinished() {
            unfinished.insert(active.identity);
        }
        assert!(unfinished.contains(queued.identity()));
        assert!(unfinished.contains(running.identity()));
        assert!(unfinished.contains(stopping.identity()));
        assert!(
            !unfinished.contains(finished.identity()),
            "a finished attempt is not one the suite is still waiting for"
        );
    }

    #[nervix_primitives::test]
    async fn a_wait_past_its_deadline_leaves_the_task_running_with_its_owner() {
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
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let mut task = OwnedNodeTask::spawn(async move {
            let _guard = DropCount(task_guard);
            started_tx
                .send(())
                .assured("the waiting test's receiver remains alive until the task begins");
            task_stop.cancelled().await;
            Ok(())
        });
        assert!(task.is_running());
        started_rx
            .await
            .assured("the task sends after installing its drop guard");

        // The deadline ends the wait and nothing else: the task is neither cancelled nor dropped,
        // and its owner still holds it.
        assert!(
            matches!(
                task.wait(PhaseDeadline::after(Duration::ZERO)).await,
                NodeTaskWaitOutcome::StillRunning
            ),
            "a pending task with an expired deadline must be left running"
        );
        assert!(task.is_running());
        assert!(matches!(task.inspect().await, NodeTaskState::Running));
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        // The node ends itself, and the same owner joins it for how it ended.
        stop.cancel();
        let NodeTaskWaitOutcome::Joined(first_outcome) =
            task.wait(PhaseDeadline::after(TEST_TIMEOUT)).await
        else {
            panic!("a task that ends inside the deadline must be joined");
        };
        assert!(matches!(
            first_outcome.as_ref(),
            NodeTaskTerminalOutcome::CleanApplicationExit
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let NodeTaskWaitOutcome::AlreadyObserved(observed_outcome) =
            task.wait(PhaseDeadline::after(TEST_TIMEOUT)).await
        else {
            panic!("a second wait must reuse the retained terminal outcome");
        };
        let NodeTaskState::Terminal(second_outcome) = task.state() else {
            panic!("a second wait must leave the retained terminal outcome available");
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

    #[nervix_primitives::test(start_paused = true)]
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
        assert!(matches!(exhausted.end, StartupEnd::BudgetSpent));
        assert_eq!(exhausted.budget, NODE_STARTUP_BUDGET);
        assert_eq!(node.launches, NODE_START_ATTEMPTS);
        assert_eq!(node.ports_moved, NODE_START_ATTEMPTS - 1);
        assert_eq!(exhausted.attempts.spent.len(), attempt_limit());
        assert_eq!(
            attempt_retries(exhausted),
            vec![StartupRetry::Transient(StartupPorts::All); attempt_limit()]
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
        // left to wait with: the node has been asked to stop and is still stopping when the
        // startup ends.
        assert!(
            matches!(last.cleanup, AttemptCleanup::StillStopping),
            "cleanup after the budget is spent must not wait past it: {}",
            last.cleanup
        );
        assert!(
            node.task.is_running(),
            "a node still stopping when its startup ends stays owned"
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

    #[nervix_primitives::test(start_paused = true)]
    async fn armed_startup_retries_only_the_colliding_listener_before_initialization() {
        for (application, ports) in [
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
        ] {
            nervix_primitives::task::consume_budget().await;
            let mut node = StandInStartupNode::new(
                "node-armed",
                [
                    LaunchBehavior::ListenError {
                        application,
                        kind: io::ErrorKind::AddrInUse,
                    },
                    LaunchBehavior::Ready,
                ],
            );
            let budget = PhaseDeadline::after(ATTEMPT_READINESS_BUDGET);
            node.start_with_policy(budget, StartupRetryPolicy::BindBeforeRuntime)
                .await
                .assured("an occupied early listener permits a fresh-port launch");
            assert_eq!(node.launches, 2);
            assert_eq!(node.port_moves, [ports]);
            assert!(budget.elapsed() < ATTEMPT_READINESS_BUDGET);
            assert!(matches!(
                node.clean_up(PhaseDeadline::after(TEST_TIMEOUT)).await,
                AttemptCleanup::Stopped(_)
            ));
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn armed_startup_retries_no_other_failure() {
        for behavior in [
            LaunchBehavior::ListenError {
                application: AppError::BindHttpListenAddress,
                kind: io::ErrorKind::PermissionDenied,
            },
            LaunchBehavior::ApplicationError(AppError::BindHttpListenAddress),
            LaunchBehavior::ListenError {
                application: AppError::OpenRegistry,
                kind: io::ErrorKind::AddrInUse,
            },
            LaunchBehavior::ListenError {
                application: AppError::StartInterconnect,
                kind: io::ErrorKind::AddrInUse,
            },
            LaunchBehavior::LaunchFails,
            LaunchBehavior::Panics,
        ] {
            nervix_primitives::task::consume_budget().await;
            let mut node = StandInStartupNode::new("node-armed-terminal", [behavior]);
            let error = node
                .start_with_policy(
                    PhaseDeadline::after(ATTEMPT_READINESS_BUDGET),
                    StartupRetryPolicy::BindBeforeRuntime,
                )
                .await
                .expect_err("a failure that cannot prove an untouched fault must be terminal");
            assert!(matches!(
                error.current_context().end,
                StartupEnd::TerminalFailure
            ));
            assert_eq!(node.launches, 1);
            assert!(node.port_moves.is_empty());
            assert_eq!(
                attempt_retries(error.current_context()),
                [StartupRetry::Terminal]
            );
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn armed_startup_keeps_one_readiness_budget() {
        let mut node = StandInStartupNode::new("node-armed-unready", [LaunchBehavior::NeverReady]);
        let budget = PhaseDeadline::after(ATTEMPT_READINESS_BUDGET);
        let error = node
            .start_with_policy(budget, StartupRetryPolicy::BindBeforeRuntime)
            .await
            .expect_err("a running node may have consumed the fault and cannot be relaunched");
        assert_eq!(error.current_context().budget, ATTEMPT_READINESS_BUDGET);
        assert_eq!(node.launches, 1);
        assert!(node.port_moves.is_empty());
        assert_eq!(
            attempt_retries(error.current_context()),
            [StartupRetry::Terminal]
        );
        assert!(budget.elapsed() >= ATTEMPT_READINESS_BUDGET);
        assert!(matches!(
            node.clean_up(PhaseDeadline::after(TEST_TIMEOUT)).await,
            AttemptCleanup::Stopped(_)
        ));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn armed_startup_retains_every_collision_when_the_launch_limit_is_spent() {
        let behaviors = (0..NODE_START_ATTEMPTS).map(|_| LaunchBehavior::ListenError {
            application: AppError::BindObservabilityListenAddress,
            kind: io::ErrorKind::AddrInUse,
        });
        let mut node = StandInStartupNode::new("node-armed-collisions", behaviors);
        let budget = PhaseDeadline::after(ATTEMPT_READINESS_BUDGET);
        let error = node
            .start_with_policy(budget, StartupRetryPolicy::BindBeforeRuntime)
            .await
            .expect_err("the launch limit also bounds fast listener collisions");
        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::AttemptsSpent));
        assert_eq!(exhausted.budget, ATTEMPT_READINESS_BUDGET);
        assert_eq!(node.launches, NODE_START_ATTEMPTS);
        assert_eq!(exhausted.attempts.spent.len(), attempt_limit());
        assert_eq!(
            node.port_moves,
            vec![StartupPorts::Observability; attempt_limit() - 1]
        );
        assert!(budget.elapsed() < ATTEMPT_READINESS_BUDGET);
        for attempt in &exhausted.attempts.spent {
            let AttemptFailure::NotReady(failure) = &attempt.failure else {
                panic!("each collision ended a launched node");
            };
            assert_eq!(
                failure.current_context().failure,
                NodeStartupFailure::TaskTerminated
            );
            let AttemptCleanup::Stopped(outcome) = &attempt.cleanup else {
                panic!("each failed task must have ended before moving its port");
            };
            let NodeTaskTerminalOutcome::ApplicationError(cause) = outcome.as_ref() else {
                panic!("the collision retains its typed application error");
            };
            assert!(matches!(
                cause.current_context(),
                AppError::BindObservabilityListenAddress
            ));
            assert_eq!(
                cause
                    .downcast_ref::<io::Error>()
                    .assured("the bind report keeps its I/O cause")
                    .kind(),
                io::ErrorKind::AddrInUse
            );
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_bound_listener_retries_only_its_own_addresses() {
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
            let mut node = StandInStartupNode::new(
                "node-bound",
                [
                    LaunchBehavior::ApplicationError(error),
                    LaunchBehavior::Ready,
                ],
            );
            node.start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
                .await
                .assured("a bound listener must be retried within the same startup budget");
            assert_eq!(node.launches, 2);
            assert_eq!(node.port_moves, [ports]);
        }
    }

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_failed_attempt_finishes_its_owned_cleanup_before_retrying_with_the_remaining_budget()
    {
        let mut node = StandInStartupNode::new(
            "node-delayed-cleanup",
            [
                LaunchBehavior::NeverReadyThenStops {
                    delay: Duration::from_secs(7),
                },
                LaunchBehavior::Ready,
            ],
        );
        let budget = PhaseDeadline::after(NODE_STARTUP_BUDGET);

        let result = node.start_within(budget).await;

        assert!(
            result.is_ok(),
            "the same budget has room to finish cleanup and retry: {result:?}"
        );
        assert_eq!(node.launches, 2);
        assert_eq!(node.ports_moved, 1);
        assert!(budget.elapsed() < NODE_STARTUP_BUDGET);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn an_initialization_failure_observed_during_cleanup_keeps_its_terminal_cause() {
        let mut node = StandInStartupNode::new(
            "node-initialization-failure",
            [
                LaunchBehavior::NeverReadyThenFails {
                    error: AppError::OpenRegistry,
                },
                LaunchBehavior::Ready,
            ],
        );

        let result = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await;
        let Err(error) = result else {
            panic!("a terminal initialization failure must end startup");
        };
        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::TerminalFailure));
        assert_eq!(node.launches, 1);
        assert_eq!(node.ports_moved, 0);
        let [attempt] = exhausted.attempts.spent.as_slice() else {
            panic!("one launched node has one retained attempt");
        };
        assert_eq!(attempt.retry, StartupRetry::Terminal);
        let AttemptCleanup::Stopped(outcome) = &attempt.cleanup else {
            panic!("the initialization failure was joined during cleanup");
        };
        let NodeTaskTerminalOutcome::ApplicationError(cause) = outcome.as_ref() else {
            panic!("the joined task retains its application error");
        };
        assert!(matches!(cause.current_context(), AppError::OpenRegistry));
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_bind_failure_observed_during_cleanup_moves_only_its_own_listeners() {
        let mut node = StandInStartupNode::new(
            "node-late-bind-failure",
            [
                LaunchBehavior::NeverReadyThenFails {
                    error: AppError::BindHttpListenAddress,
                },
                LaunchBehavior::Ready,
            ],
        );

        let result = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await;

        assert!(
            result.is_ok(),
            "a fresh HTTP listener resolves the bind failure: {result:?}"
        );
        assert_eq!(node.port_moves, vec![StartupPorts::Http]);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_node_that_does_not_stop_ends_the_startup_instead_of_being_launched_over() {
        let mut node = StandInStartupNode::new(
            "node-unstoppable",
            [
                LaunchBehavior::NeverReadyAndNeverStops,
                LaunchBehavior::Ready,
            ],
        );

        let error = node
            .start_within(PhaseDeadline::after(NODE_STARTUP_BUDGET))
            .await
            .expect_err("a node that never stops must end its startup");

        // The node the first attempt launched still holds whatever a node holds, so the launch
        // that would have become ready never starts and the node keeps its ports.
        let exhausted = error.current_context();
        assert!(matches!(exhausted.end, StartupEnd::BudgetSpent));
        assert_eq!(node.launches, 1);
        assert_eq!(node.ports_moved, 0);
        let [attempt] = exhausted.attempts.spent.as_slice() else {
            panic!("a startup that ends at its first cleanup records one attempt");
        };
        assert_eq!(attempt.retry, StartupRetry::Transient(StartupPorts::All));
        assert!(
            matches!(attempt.cleanup, AttemptCleanup::StillStopping),
            "a node that ignores its stop is still stopping at the cleanup deadline: {}",
            attempt.cleanup
        );
        assert!(
            node.task.is_running(),
            "the node that did not stop stays owned by the harness"
        );
        assert!(
            exhausted.elapsed >= NODE_STARTUP_BUDGET,
            "cleanup spends the remaining startup budget before leaving its task owned: {:?}",
            exhausted.elapsed
        );
        let rendered = error.to_string();
        assert!(
            rendered.contains("cleanup: still stopping at the cleanup deadline"),
            "{rendered}"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn exhaustion_reports_every_attempt_with_its_typed_cause() {
        let mut node = StandInStartupNode::new(
            "node-history",
            [
                LaunchBehavior::ApplicationError(AppError::BindHttpListenAddress),
                LaunchBehavior::NeverReady,
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
            rendered.contains("cleanup: stopped (clean application exit)"),
            "{rendered}"
        );
        assert!(rendered.contains("attempt 3/3 [transient]"), "{rendered}");
        assert!(
            rendered.contains("startup deadline expired; task state: running"),
            "{rendered}"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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
    static WATCHDOG_REGRESSIONS: LazyLock<nervix_primitives::sync::Mutex<()>> =
        LazyLock::new(|| nervix_primitives::sync::Mutex::new(()));

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
        /// Kept so the node's task stays owned for as long as the regression holds the node.
        _task: OwnedNodeTask,
        requests: Arc<AtomicUsize>,
        /// Ends the node when the regression drops it, whatever it does with a stop request.
        dropped: CancellationToken,
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
            let dropped = CancellationToken::new();
            let task_dropped = dropped.clone();
            let task = OwnedNodeTask::spawn(async move {
                let _live = live;
                match behavior {
                    StandInStopBehavior::StopsWhenAsked => {
                        nervix_primitives::select! {
                            () = stop.cancelled() => {}
                            () = task_dropped.cancelled() => {}
                        }
                    }
                    StandInStopBehavior::IgnoresTheRequest => task_dropped.cancelled().await,
                }
                Ok(())
            });
            Self {
                _task: task,
                requests,
                dropped,
            }
        }

        fn stop_requests(&self) -> usize {
            self.requests.load(Ordering::Relaxed)
        }
    }

    impl Drop for StandInLiveNode {
        fn drop(&mut self) {
            self.dropped.cancel();
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
            use crate::scenario_schedule::ScenarioLength;

            let scenario = ActiveScenarioRegistration::start(
                "Harness liveness",
                scenario_name,
                line,
                ScenarioLength::Ordinary,
            );
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
             end"
        );
        timeout
    }

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_suite_timeout_reports_before_it_drops_the_run() {
        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let dropped = Arc::new(AtomicUsize::new(0));
        let reported = Arc::new(AtomicUsize::new(0));
        let run_guard = StalledRun {
            dropped: dropped.clone(),
        };
        let reported_in_callback = reported.clone();
        let dropped_in_callback = dropped.clone();
        let result = SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)
            .bound_with_timeout_report(
                async move {
                    let _run_guard = run_guard;
                    future::pending::<()>().await;
                },
                move || {
                    assert_eq!(dropped_in_callback.load(Ordering::Relaxed), 0);
                    reported_in_callback.store(1, Ordering::Relaxed);
                },
            )
            .await;
        assert!(matches!(result, SuiteRun::TimedOut(_)));
        assert_eq!(reported.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_suite_timeout_does_not_count_a_finished_attempt_as_active() {
        use crate::scenario_schedule::ScenarioLength;

        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        // Cucumber keeps the world of a failed attempt for its report, so the attempt's
        // registration outlives the attempt, and the retry that ran after it, until the run ends.
        let retained = ActiveScenarioRegistration::start(
            "Harness liveness",
            "a failed attempt whose world Cucumber keeps",
            20,
            ScenarioLength::Ordinary,
        );
        retained.start_running(1);
        retained.enter(ScenarioPhase::Finished);
        let regression = WatchdogRegression::start(
            "a body that never returns beside a retained attempt",
            21,
            ScenarioPhase::Body,
            &[("node-1", StandInStopBehavior::StopsWhenAsked)],
        );

        let timeout = time_out(SuiteWatchdog::new(TEST_SUITE_BUDGET, TEST_CLEANUP_WINDOW)).await;

        assert!(
            timeout
                .stall
                .scenarios
                .iter()
                .all(|stalled| &stalled.active.identity != retained.identity()),
            "a finished attempt holds no permit and no cluster, so it is not active: {timeout}"
        );
        let stalled = stalled(&timeout, regression.identity());
        assert_eq!(stalled.active.phase, ScenarioPhase::Body);
        let diagnostic = timeout.to_string();
        assert!(
            !diagnostic.contains("a failed attempt whose world Cucumber keeps"),
            "{diagnostic}"
        );
        drop(regression);
        drop(retained);
    }

    #[nervix_primitives::test(start_paused = true)]
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
        nervix_primitives::time::advance(stalled_before_the_budget).await;

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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_retried_scenario_publishes_which_attempt_is_running() {
        use crate::scenario_schedule::{ScenarioAttempt, ScenarioLength, ScenarioRun};

        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let first = ActiveScenarioRegistration::start(
            "Harness liveness",
            "a retried scenario",
            50,
            ScenarioLength::Ordinary,
        );
        assert_eq!(published(&first).attempt, 1);
        assert_eq!(first.run(), ScenarioRun::FIRST);
        assert_eq!(first.run().attempt(), ScenarioAttempt::First);
        drop(first);

        let retry = ActiveScenarioRegistration::start(
            "Harness liveness",
            "a retried scenario",
            50,
            ScenarioLength::Ordinary,
        );
        assert_eq!(retry.run(), first_retry());
        assert_eq!(retry.run().attempt(), ScenarioAttempt::Retry);
        assert_eq!(
            published(&retry).attempt,
            2,
            "a scenario the suite takes up again is on its next attempt"
        );

        // An outline expands into one scenario per example row, and those rows share the outline's
        // name. They are separate scenarios, so neither counts as a retry of the other.
        let example = ActiveScenarioRegistration::start(
            "Harness liveness",
            "a retried scenario",
            51,
            ScenarioLength::Ordinary,
        );
        assert_eq!(published(&example).attempt, 1);
        assert_eq!(example.run(), ScenarioRun::FIRST);
        assert_ne!(published(&retry).identity, published(&example).identity);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_suite_summary_names_every_undeclared_attempt_of_two_minutes_finished_or_running() {
        use crate::{
            scenario_phase::suite_summary,
            scenario_schedule::{LONG_RUNNING_ATTEMPT, ScenarioLength},
        };

        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let start_body = |scenario, declared| {
            let registration = ActiveScenarioRegistration::start(
                "Harness liveness",
                scenario,
                STAND_IN_SCENARIO_LINE,
                declared,
            );
            registration.enter(ScenarioPhase::Body);
            registration
        };
        let undeclared = start_body(
            "an undeclared attempt that ran for two minutes",
            ScenarioLength::Ordinary,
        );
        let _running = start_body(
            "an undeclared attempt still running after two minutes",
            ScenarioLength::Ordinary,
        );
        let declared = start_body(
            "a declared attempt that ran for two minutes",
            ScenarioLength::LongRunning,
        );
        let shorter = start_body(
            "an undeclared attempt that ended just short of two minutes",
            ScenarioLength::Ordinary,
        );

        let short_of_the_bound = Duration::from_millis(100);
        let just_short = LONG_RUNNING_ATTEMPT
            .checked_sub(short_of_the_bound)
            .verified("the long-running bound is two minutes, far above 100 milliseconds");
        nervix_primitives::time::advance(just_short).await;
        shorter.enter(ScenarioPhase::Finished);
        nervix_primitives::time::advance(short_of_the_bound).await;
        undeclared.enter(ScenarioPhase::Finished);
        declared.enter(ScenarioPhase::Finished);

        // The section is the summary's last, and rows of other tests' scenarios may share it.
        let summary = suite_summary();
        let Some((_, section)) =
            summary.split_once("### Attempts of at least 120s without @long_running")
        else {
            panic!("the summary must have a section for undeclared long attempts:\n{summary}");
        };
        let row = |scenario: &str| {
            format!(
                "| 120.0s | 1 | Harness liveness / {scenario} (line {STAND_IN_SCENARIO_LINE}) |"
            )
        };
        assert!(
            section.contains(&row("an undeclared attempt that ran for two minutes")),
            "{summary}"
        );
        assert!(
            section.contains(&row("an undeclared attempt still running after two minutes")),
            "an attempt still in its body counts as long as it has run so far:\n{summary}"
        );
        assert!(
            !section.contains("a declared attempt that ran for two minutes"),
            "{summary}"
        );
        assert!(
            !section.contains("an undeclared attempt that ended just short of two minutes"),
            "{summary}"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn the_suite_summary_counts_the_slot_time_a_retry_holds_beyond_one_slot() {
        use crate::{scenario_phase::suite_summary, scenario_schedule::ScenarioLength};

        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let extra_slot_seconds = || {
            let summary = suite_summary();
            let Some((_, held)) = summary.split_once("holding ") else {
                panic!("the summary must say how long retries held extra slots:\n{summary}");
            };
            let Some((seconds, _)) = held.split_once(" slot-seconds beyond one slot each") else {
                panic!("the summary must say how long retries held extra slots:\n{summary}");
            };
            seconds
                .parse::<f64>()
                .unwrap_or_else(|error| panic!("{error}: the held slot time in\n{summary}"))
        };
        let before = extra_slot_seconds();

        let retry = ActiveScenarioRegistration::start(
            "Harness liveness",
            "a retry that holds three run slots",
            STAND_IN_SCENARIO_LINE,
            ScenarioLength::Ordinary,
        );
        retry.start_running(3);
        nervix_primitives::time::advance(Duration::from_secs(10)).await;
        let running = extra_slot_seconds();
        retry.enter(ScenarioPhase::Finished);
        let after = extra_slot_seconds();

        // Ten seconds on three slots hold twenty slot-seconds beyond the one every attempt holds,
        // counted while the retry runs and kept once it has finished.
        assert!(
            (running - before - 20.0).abs() < 0.05,
            "{before} then {running}"
        );
        assert!(
            (after - before - 20.0).abs() < 0.05,
            "{before} then {after}"
        );
    }

    #[nervix_primitives::test]
    async fn the_suite_summary_adds_up_every_runner_tick_beside_its_stalls() {
        use crate::{
            runner_lag::RunnerTick,
            scenario_phase::{record_runner_tick, suite_summary},
        };

        let _serialized = WATCHDOG_REGRESSIONS.lock().await;
        let counted = || {
            let summary = suite_summary();
            let Some((_, runner)) = summary.split_once("Runner task: ") else {
                panic!("the summary must describe the runner task:\n{summary}");
            };
            let Some((ticks, rest)) = runner.split_once(" ticks polled ") else {
                panic!("the runner task line must count its ticks:\n{summary}");
            };
            let Some((_, rest)) = rest.split_once("; ") else {
                panic!("the runner task line must count its stalls:\n{summary}");
            };
            let Some((stalls, _)) = rest.split_once(" stalls took ") else {
                panic!("the runner task line must count its stalls:\n{summary}");
            };
            let ticks = ticks
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{error}: the tick count in\n{summary}"));
            let stalls = stalls
                .parse::<u64>()
                .unwrap_or_else(|error| panic!("{error}: the stall count in\n{summary}"));
            (ticks, stalls)
        };
        let (ticks_before, stalls_before) = counted();

        let bound = Duration::from_secs(1);
        record_runner_tick(RunnerTick {
            late: Duration::from_millis(300),
            reported: bound,
        });
        record_runner_tick(RunnerTick {
            late: Duration::from_millis(1_500),
            reported: bound,
        });
        let (ticks_after, stalls_after) = counted();

        assert_eq!(
            ticks_after.checked_sub(ticks_before),
            Some(2),
            "a tick below the stall bound still counts as a tick"
        );
        assert_eq!(
            stalls_after.checked_sub(stalls_before),
            Some(1),
            "only the tick at or above the bound is a stall"
        );
        let summary = suite_summary();
        assert!(summary.contains("\nRuntime workers: "), "{summary}");
    }

    /// A command line that carries only the suite watchdog's own options, so the injection point
    /// a run configures the budget through can be parsed on its own.
    #[derive(clap::Parser)]
    struct StandInSuiteCli {
        #[command(flatten)]
        watchdog: SuiteWatchdogArgs,
    }

    #[test]
    fn the_suite_budget_and_its_workflow_share_the_same_outer_limit() {
        let workflow = include_str!("../.github/workflows/check.yaml");
        let (_, scenario_job) = workflow
            .split_once("\n  scenarios:\n")
            .expect("the workflow must declare its scenarios job");
        let minutes = scenario_job
            .lines()
            .find_map(|line| line.strip_prefix("    timeout-minutes: "))
            .expect("the scenarios job must declare an emergency limit")
            .parse::<u64>()
            .expect("the job limit must be a number of minutes");
        assert_eq!(
            WORKFLOW_JOB_LIMIT,
            Duration::from_secs(minutes * 60),
            "the harness must leave its setup and cleanup reserves inside the actual CI job"
        );
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
        // environment is bounded by that value, and only a run that does not receives the policy
        // default. This process may be either kind of run.
        let expected_default = match std::env::var(SUITE_BUDGET_ENV) {
            Ok(given) => nervix_models::parse_duration_text(&given)
                .assured("the option parsed the same environment value as a duration above"),
            Err(_) => SUITE_BUDGET,
        };
        let default = StandInSuiteCli::try_parse_from(["scenarios"])
            .expect("the suite budget option must have a default");
        assert_eq!(
            default.watchdog.watchdog().budget(),
            expected_default,
            "a run that gives no budget of its own is bounded by the environment's budget, or by \
             the policy default when the environment gives none"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_dependency_stop_that_never_returns_is_abandoned_at_its_budget() {
        let started = Instant::now();

        let teardown = SuiteTeardown::bounded(async {
            future::pending::<()>().await;
            Ok(())
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

    #[nervix_primitives::test(start_paused = true)]
    async fn a_dependency_stop_that_finishes_keeps_what_it_reported() {
        let clean = SuiteTeardown::bounded(async { Ok(()) }).await;
        assert!(clean.is_clean(), "{clean}");

        let failed = SuiteTeardown::bounded(async {
            let stop = Report::new(io::Error::other("the container did not answer"))
                .change_context(ContainerTeardownError::Stop {
                    container: "redis".to_string(),
                });
            Err(TeardownFailures::from(stop))
        })
        .await;
        assert!(
            !failed.is_clean(),
            "a dependency that reported a failure is not a clean teardown: {failed}"
        );
        let SuiteTeardown::Failed(failures) = &failed else {
            panic!("a stop that finished with a failure keeps it: {failed}");
        };
        assert!(
            failures.reports()[0].contains::<io::Error>(),
            "the failure keeps the cause it was given: {failed}"
        );
        assert_eq!(
            failed.to_string(),
            "suite dependency teardown failed: failed to stop test container redis: the container \
             did not answer"
        );
    }
}

mod http_receiver_tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_primitives::{net::TcpStream, time::Instant};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use crate::http_receiver::{
        ClientCertificatePolicy, HttpReceiver, LengthPolicy, RECEIVER_CONNECTION_STOP_BUDGET,
        ReceiverFault, ReceiverProtocols, ReceiverResponse, ReceiverScriptError,
        ReceiverTlsOptions, ReceiverTransport,
    };

    /// How long a regression waits for something the receiver does promptly on an idle machine.
    const WITHIN: Duration = Duration::from_secs(10);

    fn loopback() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
    }

    fn script(lines: &str) -> Vec<ReceiverResponse> {
        lines
            .lines()
            .map(|line| {
                line.parse()
                    .assured("every regression script line is valid")
            })
            .collect()
    }

    async fn start(transport: ReceiverTransport) -> HttpReceiver {
        HttpReceiver::start(loopback(), transport, LengthPolicy::Lenient)
            .await
            .assured("a loopback receiver binds an ephemeral port")
    }

    /// Writes `request` on a fresh connection and reads until the receiver closes it.
    async fn exchange(receiver: &HttpReceiver, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port()))
            .await
            .assured("the receiver is listening");
        stream
            .write_all(request)
            .await
            .assured("the receiver reads what a client writes");
        let mut response = Vec::new();
        nervix_primitives::time::timeout(WITHIN, stream.read_to_end(&mut response))
            .await
            .assured("the receiver closes a connection its script ends")
            .assured("reading from a loopback connection succeeds");
        response
    }

    #[nervix_primitives::test]
    async fn a_strict_receiver_refuses_a_content_method_that_declares_no_length() {
        let receiver = HttpReceiver::start(
            loopback(),
            ReceiverTransport::Plain,
            LengthPolicy::RequiredForContent,
        )
        .await
        .assured("a loopback receiver binds an ephemeral port");

        let undeclared = exchange(
            &receiver,
            b"POST /events/1 HTTP/1.1\r\nhost: receiver\r\nconnection: close\r\n\r\n",
        )
        .await;
        let declared = exchange(
            &receiver,
            b"PUT /events/2 HTTP/1.1\r\nhost: receiver\r\nconnection: close\r\n\
              content-length: 0\r\n\r\n",
        )
        .await;
        let chunked = exchange(
            &receiver,
            b"PATCH /events/3 HTTP/1.1\r\nhost: receiver\r\nconnection: close\r\n\
              transfer-encoding: chunked\r\n\r\n0\r\n\r\n",
        )
        .await;
        let without_content = exchange(
            &receiver,
            b"DELETE /events/4 HTTP/1.1\r\nhost: receiver\r\nconnection: close\r\n\r\n",
        )
        .await;

        assert!(
            undeclared.starts_with(b"HTTP/1.1 411 Length Required\r\n"),
            "{}",
            String::from_utf8_lossy(&undeclared)
        );
        for answer in [&declared, &chunked, &without_content] {
            assert!(
                answer.starts_with(b"HTTP/1.1 200 "),
                "{}",
                String::from_utf8_lossy(answer)
            );
        }
        receiver
            .wait_for_fault(
                "refused length",
                |fault| {
                    matches!(
                        fault,
                        ReceiverFault::LengthRequired { method, .. } if method == "POST"
                    )
                },
                WITHIN,
            )
            .await
            .assured("the POST above declared no length");
        let captured = receiver
            .wait_for_requests(3, WITHIN)
            .await
            .assured("every request that a strict receiver accepts was answered");
        let mut lines = Vec::new();
        for request in &captured {
            lines.push(request.request_line());
        }
        assert_eq!(
            lines,
            ["PUT /events/2", "PATCH /events/3", "DELETE /events/4"]
        );
    }

    #[nervix_primitives::test]
    async fn the_receiver_captures_requests_and_answers_its_script_in_order() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script(
            "respond 503; header Retry-After: 1\nrespond 202; body accepted",
        ));
        let client = reqwest::Client::new();

        let first = client
            .patch(format!("{}/v1/events/42?notify=true", receiver.origin()))
            .header("X-Tenant", "north")
            .header("Idempotency-Key", "42")
            .body(r#"{"event_id":"42","payload":"p"}"#)
            .send()
            .await
            .assured("the receiver answers a scripted request");
        assert_eq!(first.status(), 503);
        assert_eq!(first.headers()["retry-after"], "1");
        let second = client
            .delete(format!("{}/v1/events/7", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers a scripted request");
        assert_eq!(second.status(), 202);
        assert_eq!(
            second.text().await.assured("the scripted body is complete"),
            "accepted"
        );
        let unscripted = client
            .get(format!("{}/after", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers an unscripted request");
        assert_eq!(unscripted.status(), 200);
        receiver.answer_unscripted_requests_with(
            "respond 404"
                .parse()
                .assured("the standing response is valid"),
        );
        let standing = client
            .get(format!("{}/missing", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers with its replaced standing response");
        assert_eq!(standing.status(), 404);

        let captured = receiver
            .wait_for_requests(4, WITHIN)
            .await
            .assured("every request above was answered, so it was captured first");
        assert_eq!(captured.len(), 4);
        assert_eq!(captured[0].method, "PATCH");
        assert_eq!(captured[0].target, "/v1/events/42?notify=true");
        assert_eq!(
            captured[0].header_values("x-tenant"),
            vec![b"north".as_slice()]
        );
        assert_eq!(
            captured[0].header_values("IDEMPOTENCY-KEY"),
            vec![b"42".as_slice()]
        );
        assert_eq!(captured[0].body, br#"{"event_id":"42","payload":"p"}"#);
        assert_eq!(captured[1].method, "DELETE");
        assert!(captured[1].body.is_empty());
        assert_eq!(captured[3].target, "/missing");

        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert_eq!(stop.captured, 4, "{stop}");
    }

    #[nervix_primitives::test]
    async fn a_lost_response_is_captured_and_the_connection_closes_without_an_answer() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script("lose response"));

        let response = exchange(
            &receiver,
            b"POST /applied HTTP/1.1\r\nHost: receiver\r\nContent-Length: 7\r\n\r\napplied",
        )
        .await;

        assert!(response.is_empty(), "a lost response writes nothing");
        let captured = receiver.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].body, b"applied");
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn a_scripted_retry_after_date_is_measured_from_when_its_response_is_written() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script("respond 503; after 1s; retry after date in 2s"));
        let sent_at = chrono::Utc::now();

        let response = reqwest::Client::new()
            .get(format!("{}/limited", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers a scripted request");
        let received_at = chrono::Utc::now();

        assert_eq!(response.status(), 503);
        let retry_after = response.headers()["retry-after"]
            .to_str()
            .assured("an HTTP date is ASCII");
        let date = chrono::DateTime::parse_from_rfc2822(retry_after)
            .assured("the receiver writes an IMF-fixdate");
        let at_least = sent_at + chrono::TimeDelta::seconds(3);
        let at_most = received_at + chrono::TimeDelta::seconds(3);
        // The date is taken once the one-second response delay has passed and is rounded up to a
        // whole second, so it asks for at least two seconds from then and at most three from when
        // the response arrived.
        assert!(
            date >= at_least,
            "{retry_after} is before {at_least}, so it asks for less than the scripted delay"
        );
        assert!(
            date <= at_most,
            "{retry_after} is after {at_most}, so it was not measured from the response"
        );
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn a_request_held_until_released_answers_with_the_released_response() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script(
            "hold response until released\nhold response until released",
        ));
        let client = reqwest::Client::new();
        let held = nervix_primitives::task::spawn({
            let client = client.clone();
            let target = format!("{}/held", receiver.origin());
            async move { client.get(target).send().await }
        });
        receiver
            .wait_for_requests(1, WITHIN)
            .await
            .assured("the held request is captured before it is held");
        assert!(
            !held.is_finished(),
            "nothing answers a held request before the release"
        );

        receiver.release_held_responses(
            "respond 202; body released"
                .parse()
                .assured("the released response is valid"),
        );
        let released = nervix_primitives::time::timeout(WITHIN, held)
            .await
            .assured("the release answers the held request")
            .assured("the held client does not panic")
            .assured("the released response is complete");
        assert_eq!(released.status(), 202);
        assert_eq!(
            released
                .text()
                .await
                .assured("the released body is complete"),
            "released"
        );
        let later = client
            .get(format!("{}/later", receiver.origin()))
            .send()
            .await
            .assured("a request held after the release takes the released response at once");
        assert_eq!(later.status(), 202);
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn a_target_with_its_own_answer_leaves_the_script_to_other_requests() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script("respond 503"));
        receiver.answer_requests_for(
            "/tenants/north?attempt=1".to_string(),
            "respond 404"
                .parse()
                .assured("the targeted response is valid"),
        );
        let client = reqwest::Client::new();

        let targeted = client
            .get(format!("{}/tenants/north?attempt=1", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers a targeted request");
        let scripted = client
            .get(format!("{}/tenants/south", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers a scripted request");
        let repeated = client
            .get(format!("{}/tenants/north?attempt=1", receiver.origin()))
            .send()
            .await
            .assured("a targeted answer holds for every request for its target");
        let standing = client
            .get(format!("{}/tenants/north", receiver.origin()))
            .send()
            .await
            .assured("the receiver answers with its standing response");

        assert_eq!(targeted.status(), 404);
        assert_eq!(scripted.status(), 503);
        assert_eq!(repeated.status(), 404);
        assert_eq!(standing.status(), 200);
        assert_eq!(receiver.captured().len(), 4);
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn chunked_bodies_interim_responses_and_raw_bytes_are_served_as_scripted() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script(
            "respond 200; interim 103; header Link: </a>\nraw HTTP/1.1 200 OK\\r\\nbroken \
             header\\r\\n\\r\\n",
        ));

        let interim = exchange(
            &receiver,
            b"POST /chunked HTTP/1.1\r\nHost: receiver\r\nConnection: close\r\n\
              Transfer-Encoding: chunked\r\n\r\n4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n",
        )
        .await;
        let raw = exchange(&receiver, b"GET /raw HTTP/1.1\r\nHost: receiver\r\n\r\n").await;

        let interim = String::from_utf8(interim).assured("the scripted response is ASCII");
        let (interim_head, final_response) = interim
            .split_once("\r\n\r\n")
            .assured("the interim response ends with an empty line");
        assert!(interim_head.starts_with("HTTP/1.1 103 "), "{interim}");
        assert!(
            final_response.starts_with("HTTP/1.1 200 OK\r\n"),
            "{interim}"
        );
        assert!(final_response.contains("Link: </a>\r\n"), "{interim}");
        assert_eq!(raw, b"HTTP/1.1 200 OK\r\nbroken header\r\n\r\n");
        let captured = receiver.captured();
        assert_eq!(captured[0].body, b"Wikipedia");
        assert_eq!(captured[1].target, "/raw");
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn held_responses_and_stalled_bodies_end_within_the_stop_budget() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script(
            "hold response\nrespond 200; body partial; stall body",
        ));
        let held_port = receiver.port();
        let held = nervix_primitives::task::spawn(async move {
            let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, held_port))
                .await
                .assured("the receiver is listening");
            stream
                .write_all(b"GET /held HTTP/1.1\r\nHost: receiver\r\n\r\n")
                .await
                .assured("the receiver reads what a client writes");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .assured("a held connection ends cleanly when the receiver stops");
            response
        });
        receiver
            .wait_for_requests(1, WITHIN)
            .await
            .assured("the held request is captured before it is held");

        let stalled = reqwest::Client::new()
            .get(format!("{}/stalled", receiver.origin()))
            .send()
            .await
            .assured("complete headers arrive although the body stalls");
        assert_eq!(stalled.status(), 200);
        let body = nervix_primitives::task::spawn(stalled.bytes());

        let started = Instant::now();
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert!(
            started.elapsed() < RECEIVER_CONNECTION_STOP_BUDGET,
            "held connections observe the stop: {stop}"
        );
        let held = nervix_primitives::time::timeout(WITHIN, held)
            .await
            .assured("the held client sees the connection close")
            .assured("the held client does not panic");
        assert!(held.is_empty(), "a held response writes nothing");
        let body = nervix_primitives::time::timeout(WITHIN, body)
            .await
            .assured("the stalled body ends when the receiver stops")
            .assured("the stalled body reader does not panic");
        assert!(body.is_err(), "a stalled body never completes");
    }

    #[nervix_primitives::test]
    async fn requests_awaiting_a_response_are_counted_until_their_final_head_begins() {
        let concurrent = start(ReceiverTransport::Plain).await;
        concurrent.answer_unscripted_requests_with(
            "respond 204; after 300ms"
                .parse()
                .assured("the standing response is valid"),
        );
        let client = reqwest::Client::new();
        let (first, second) = tokio::join!(
            client.get(format!("{}/first", concurrent.origin())).send(),
            client.get(format!("{}/second", concurrent.origin())).send(),
        );
        assert_eq!(first.assured("the first request is answered").status(), 204);
        assert_eq!(
            second.assured("the second request is answered").status(),
            204
        );
        assert_eq!(
            concurrent.most_awaiting_responses(),
            2,
            "both delayed requests awaited their responses at once"
        );
        assert!(!concurrent.stop().await.was_forced());

        let sequential = start(ReceiverTransport::Plain).await;
        sequential.answer_unscripted_requests_with(
            "respond 204; after 100ms"
                .parse()
                .assured("the standing response is valid"),
        );
        for target in ["/first", "/second", "/third"] {
            let answered = client
                .get(format!("{}{target}", sequential.origin()))
                .send()
                .await
                .assured("each request is answered before the next is sent");
            assert_eq!(answered.status(), 204);
        }
        assert_eq!(
            sequential.most_awaiting_responses(),
            1,
            "a client that waits for each final head never has two requests awaiting"
        );
        assert!(!sequential.stop().await.was_forced());
    }

    /// Writes a request on a fresh connection, reads until the end of the response head, and
    /// closes the connection without reading anything more.
    async fn read_head_and_leave(receiver: &HttpReceiver, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port()))
            .await
            .assured("the receiver is listening");
        stream
            .write_all(request)
            .await
            .assured("the receiver reads what a client writes");
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let read = nervix_primitives::time::timeout(WITHIN, stream.read(&mut byte))
                .await
                .assured("the receiver writes a response head")
                .assured("reading from a loopback connection succeeds");
            assert_eq!(read, 1, "the connection stays open until the head ends");
            head.push(byte[0]);
        }
        head
    }

    #[nervix_primitives::test]
    async fn a_client_that_leaves_an_unfinished_response_abandons_it() {
        let receiver = start(ReceiverTransport::Plain).await;
        receiver.script(script(
            "hold response\nrespond 200; body partial; stall body\nrespond 200; body bytes \
             33554432\nrespond 204",
        ));

        let mut held = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port()))
            .await
            .assured("the receiver is listening");
        held.write_all(b"GET /held HTTP/1.1\r\nHost: receiver\r\n\r\n")
            .await
            .assured("the receiver reads what a client writes");
        receiver
            .wait_for_requests(1, WITHIN)
            .await
            .assured("the held request is captured before it is held");
        drop(held);
        receiver
            .wait_for_abandoned_responses(1, WITHIN)
            .await
            .assured("a client that leaves a held response abandons it");

        let stalled = read_head_and_leave(
            &receiver,
            b"GET /stalled HTTP/1.1\r\nHost: receiver\r\n\r\n",
        )
        .await;
        assert!(stalled.starts_with(b"HTTP/1.1 200 "));
        receiver
            .wait_for_abandoned_responses(2, WITHIN)
            .await
            .assured("a client that leaves at the head of a stalled body abandons it");

        // The body is many times what a loopback connection buffers, so the receiver can finish
        // writing it only if the client reads it.
        let oversized =
            read_head_and_leave(&receiver, b"GET /large HTTP/1.1\r\nHost: receiver\r\n\r\n").await;
        let oversized = String::from_utf8(oversized).assured("the response head is ASCII");
        assert!(
            oversized.contains("content-length: 33554432\r\n"),
            "{oversized}"
        );
        receiver
            .wait_for_abandoned_responses(3, WITHIN)
            .await
            .assured("a client that leaves at the head of a large body abandons it");

        let complete = exchange(
            &receiver,
            b"GET /complete HTTP/1.1\r\nHost: receiver\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(complete.starts_with(b"HTTP/1.1 204 "));
        let unmet = receiver
            .wait_for_abandoned_responses(4, Duration::ZERO)
            .await
            .expect_err("a response the client read to its end is not abandoned");
        assert!(
            unmet.to_string().contains("abandoned 3 of the 4"),
            "{unmet}"
        );
        assert_eq!(
            receiver.most_awaiting_responses(),
            1,
            "each request was captured after the previous one ended"
        );
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert_eq!(
            stop.faults, 0,
            "abandoning a response is not a fault: {stop}"
        );
    }

    #[nervix_primitives::test]
    async fn a_wait_for_a_request_line_ends_once_that_request_is_captured() {
        let receiver = start(ReceiverTransport::Plain).await;
        let client = reqwest::Client::new();
        for target in ["/first", "/second"] {
            client
                .get(format!("{}{target}", receiver.origin()))
                .send()
                .await
                .assured("the receiver answers every request");
        }

        let captured = receiver
            .wait_for_request_line("GET /second", WITHIN)
            .await
            .assured("the second request was captured");
        assert_eq!(captured.len(), 2);
        let missing = receiver
            .wait_for_request_line("GET /never", Duration::from_millis(100))
            .await
            .expect_err("no request for /never was sent");
        assert!(
            missing.to_string().contains("request line 'GET /never'"),
            "{missing}"
        );
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn requests_beyond_the_receiver_bounds_are_faults_not_captures() {
        let receiver = start(ReceiverTransport::Plain).await;

        exchange(
            &receiver,
            b"POST / HTTP/1.1\r\nHost: receiver\r\nContent-Length: 999999999999\r\n\r\n",
        )
        .await;
        receiver
            .wait_for_fault(
                "oversized body",
                |fault| matches!(fault, ReceiverFault::RequestBodyTooLarge { .. }),
                WITHIN,
            )
            .await
            .assured("the declared body exceeds the receiver's limit");
        exchange(&receiver, b"NOT A REQUEST\r\n\r\n").await;
        receiver
            .wait_for_fault(
                "malformed request",
                |fault| matches!(fault, ReceiverFault::MalformedRequest { .. }),
                WITHIN,
            )
            .await
            .assured("the request line has no HTTP version");

        assert!(receiver.captured().is_empty());
        let missing = receiver
            .wait_for_fault(
                "TLS handshake failure",
                ReceiverFault::is_tls_handshake,
                Duration::ZERO,
            )
            .await
            .expect_err("a plain receiver never records a TLS handshake");
        assert!(
            missing.to_string().contains("TLS handshake failure"),
            "{missing}"
        );
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert_eq!(stop.faults, 2, "{stop}");
    }

    #[nervix_primitives::test]
    async fn a_tls_receiver_offering_http2_captures_and_answers_requests_over_http2() {
        let receiver = start(ReceiverTransport::Tls(ReceiverTlsOptions {
            certificate_hosts: vec!["127.0.0.1".to_string()],
            client_certificate: ClientCertificatePolicy::NotRequested,
            protocols: ReceiverProtocols::Http2AndHttp1,
        }))
        .await;
        receiver.script(script(
            "respond 201; header X-Answer: scripted; body created\nrespond 200; interim 103",
        ));
        let files = receiver
            .tls_files()
            .assured("a TLS receiver writes its client files");
        let ca = std::fs::read(files.join(HttpReceiver::tls_file_names()[0]))
            .assured("the receiver wrote its files");
        let ca = reqwest::Certificate::from_pem(&ca).assured("the receiver's CA is valid PEM");
        let client = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .assured("the client configuration is valid");

        let answered = client
            .post(format!("{}/events?tenant=north", receiver.origin()))
            .body("payload")
            .send()
            .await
            .assured("the receiver answers a request over HTTP/2");
        assert_eq!(answered.version(), reqwest::Version::HTTP_2);
        assert_eq!(answered.status(), 201);
        assert_eq!(answered.headers()["x-answer"], "scripted");
        let body = answered
            .text()
            .await
            .assured("the receiver sends the scripted body");
        assert_eq!(body, "created");
        let unsendable = client
            .get(format!("{}/interim", receiver.origin()))
            .send()
            .await;
        // The receiver records the fault before hyper resets the stream, so the client's failure
        // already means it was recorded.
        assert!(
            unsendable.is_err(),
            "a response HTTP/2 cannot send resets its request's stream"
        );

        let captured = receiver.captured();
        assert_eq!(captured.len(), 2, "{captured:?}");
        assert_eq!(captured[0].request_line(), "POST /events?tenant=north");
        assert_eq!(captured[0].version, http::Version::HTTP_2);
        assert_eq!(captured[0].body, b"payload");
        assert_eq!(captured[1].request_line(), "GET /interim");
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert_eq!(stop.faults, 1, "{stop}");
    }

    #[nervix_primitives::test]
    async fn a_tls_receiver_accepts_the_client_certificate_it_issued_and_refuses_others() {
        let receiver = start(ReceiverTransport::Tls(ReceiverTlsOptions {
            certificate_hosts: vec!["127.0.0.1".to_string()],
            client_certificate: ClientCertificatePolicy::Required,
            protocols: ReceiverProtocols::Http1,
        }))
        .await;
        let files = receiver
            .tls_files()
            .assured("a TLS receiver writes its client files");
        let [ca, certificate, key] = HttpReceiver::tls_file_names()
            .map(|name| std::fs::read(files.join(name)).assured("the receiver wrote its files"));
        let ca = reqwest::Certificate::from_pem(&ca).assured("the receiver's CA is valid PEM");
        let identity = reqwest::Identity::from_pem(&[certificate, key].concat())
            .assured("the receiver's client identity is valid PEM");

        let trusted = reqwest::Client::builder()
            .add_root_certificate(ca.clone())
            .identity(identity)
            .build()
            .assured("the client configuration is valid")
            .get(format!("{}/mutual", receiver.origin()))
            .send()
            .await
            .assured("a client with the issued certificate completes the handshake");
        assert_eq!(trusted.status(), 200);
        let anonymous = reqwest::Client::builder()
            .add_root_certificate(ca)
            .build()
            .assured("the client configuration is valid")
            .get(format!("{}/anonymous", receiver.origin()))
            .send()
            .await;
        assert!(
            anonymous.is_err(),
            "a client without a certificate is refused"
        );
        receiver
            .wait_for_fault(
                "TLS handshake failure",
                ReceiverFault::is_tls_handshake,
                WITHIN,
            )
            .await
            .assured("the refused client fails its handshake");

        assert_eq!(receiver.captured().len(), 1);
        assert!(!receiver.stop().await.was_forced());
    }

    #[nervix_primitives::test]
    async fn a_tls_receiver_is_refused_by_a_client_that_dials_a_name_its_certificate_lacks() {
        let receiver = start(ReceiverTransport::Tls(ReceiverTlsOptions {
            certificate_hosts: vec!["localhost".to_string()],
            client_certificate: ClientCertificatePolicy::NotRequested,
            protocols: ReceiverProtocols::Http1,
        }))
        .await;
        let files = receiver
            .tls_files()
            .assured("a TLS receiver writes its client files");
        let ca = std::fs::read(files.join(HttpReceiver::tls_file_names()[0]))
            .assured("the receiver wrote its CA certificate");
        let client = reqwest::Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(&ca).assured("the receiver's CA is valid PEM"),
            )
            .build()
            .assured("the client configuration is valid");

        let by_name = client
            .get(format!("https://localhost:{}/named", receiver.port()))
            .send()
            .await
            .assured("the certificate names localhost");
        assert_eq!(by_name.status(), 200);
        let by_address = client
            .get(format!("{}/address", receiver.origin()))
            .send()
            .await;
        assert!(
            by_address.is_err(),
            "the certificate does not name 127.0.0.1"
        );
        receiver
            .wait_for_fault(
                "TLS handshake failure",
                ReceiverFault::is_tls_handshake,
                WITHIN,
            )
            .await
            .assured("the client abandons the handshake on the hostname mismatch");

        assert_eq!(receiver.captured().len(), 1);
        assert!(!receiver.stop().await.was_forced());
    }

    #[test]
    fn every_documented_script_form_parses_and_unknown_forms_are_refused() {
        for line in [
            "respond 200",
            "respond 204; header X-A: b; body text; interim 100; after 250ms; extra headers 129",
            "respond 200; stall body",
            "respond 200; body bytes 67108864",
            "respond 429; retry after date in 3s",
            "lose response",
            "hold response",
            "hold response until released",
            "raw HTTP/1.1 999 Odd\\r\\n\\r\\n",
        ] {
            assert!(line.parse::<ReceiverResponse>().is_ok(), "{line}");
        }
        let refused = [
            ("answer 200", "UnknownForm"),
            ("hold response until later", "UnknownForm"),
            ("respond 20", "Status"),
            ("respond 099", "Status"),
            ("respond 200; header nocolon", "Header"),
            ("respond 200; after soon", "Duration"),
            ("respond 503; retry after date in soon", "Duration"),
            ("respond 503; retry after date in 2days", "RetryAfterDelay"),
            ("respond 200; extra headers many", "Count"),
            ("respond 200; body bytes 67108865", "BodySize"),
            ("respond 200; teapot", "UnknownClause"),
            ("raw trailing\\", "Escape"),
        ];
        for (line, variant) in refused {
            let error: ReceiverScriptError = line
                .parse::<ReceiverResponse>()
                .expect_err("the line is not a response");
            assert!(
                format!("{error:?}").starts_with(variant),
                "{line}: {error:?}"
            );
        }
    }
}

mod grpc_receiver_tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use bytes::Bytes;
    use meticulous::ResultExt as _;
    use nervix_primitives::net::TcpStream;
    use nervix_recovery::Discarded as _;

    use crate::{
        grpc_receiver::{GrpcAnswer, GrpcReceiver, MAX_REQUEST_MESSAGE_BYTES},
        http_receiver::RECEIVER_CONNECTION_STOP_BUDGET,
    };

    /// How long a regression waits for something the receiver does promptly on an idle machine.
    const WITHIN: Duration = Duration::from_secs(10);
    const METHOD: &str = "/nervix.test.Receiver/Call";

    async fn start() -> GrpcReceiver {
        GrpcReceiver::start(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .assured("a loopback receiver binds an ephemeral port")
    }

    /// An HTTP/2 connection to `receiver`, driven by a task of its own until either side ends it.
    async fn connect(receiver: &GrpcReceiver) -> h2::client::SendRequest<Bytes> {
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port()))
            .await
            .assured("the receiver is listening");
        let (send_request, connection) = h2::client::handshake(stream)
            .await
            .assured("the receiver completes an HTTP/2 handshake");
        nervix_primitives::task::spawn(async move {
            connection
                .await
                .discarded("a regression reads how the connection ended from its calls");
        });
        send_request
    }

    /// One gRPC message with `flag` as its compressed flag.
    fn message(flag: u8, bytes: &[u8]) -> Bytes {
        let length = u32::try_from(bytes.len()).assured("a regression message is small");
        let mut framed = vec![flag];
        framed.extend_from_slice(&length.to_be_bytes());
        framed.extend_from_slice(bytes);
        Bytes::from(framed)
    }

    /// How one call ended.
    #[derive(Debug, PartialEq, Eq)]
    enum CallEnd {
        /// The receiver answered with this `grpc-status`.
        Status(String),
        /// The receiver closed the connection or reset the call without answering.
        NoAnswer,
    }

    /// Makes one call whose whole request is `body` and reads how the receiver ended it.
    async fn call(send_request: &mut h2::client::SendRequest<Bytes>, body: Bytes) -> CallEnd {
        let request = http::Request::builder()
            .method("POST")
            .uri(format!("http://127.0.0.1{METHOD}"))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())
            .assured("the regression request is well formed");
        let mut ready = send_request
            .clone()
            .ready()
            .await
            .assured("the connection accepts another call");
        let (response, mut stream) = ready
            .send_request(request, false)
            .assured("the connection accepts another call");
        stream
            .send_data(body, true)
            .assured("an open call accepts its request");
        let response = nervix_primitives::time::timeout(WITHIN, response)
            .await
            .assured("the receiver ends every call it does not hold");
        let Ok(response) = response else {
            return CallEnd::NoAnswer;
        };
        if let Some(status) = response.headers().get("grpc-status") {
            return CallEnd::Status(status.to_str().assured("a status is ASCII").to_string());
        }
        let mut body = response.into_body();
        while let Some(chunk) = body.data().await {
            if chunk.is_err() {
                return CallEnd::NoAnswer;
            }
        }
        match body.trailers().await {
            Ok(Some(trailers)) => match trailers.get("grpc-status") {
                Some(status) => {
                    CallEnd::Status(status.to_str().assured("a status is ASCII").to_string())
                }
                None => CallEnd::NoAnswer,
            },
            Ok(None) | Err(_) => CallEnd::NoAnswer,
        }
    }

    #[nervix_primitives::test]
    async fn the_grpc_receiver_captures_calls_and_answers_its_script_in_order() {
        let receiver = start().await;
        assert_eq!(
            receiver.origin(),
            format!("http://127.0.0.1:{}", receiver.port()),
            "a client dials the receiver's loopback port without TLS"
        );
        receiver.script([
            GrpcAnswer::Status(tonic::Code::Unavailable),
            GrpcAnswer::Accept,
        ]);
        let mut connection = connect(&receiver).await;

        let first = call(&mut connection, message(0, b"first")).await;
        let second = call(&mut connection, message(1, b"second")).await;
        let unscripted = call(&mut connection, message(0, b"third")).await;

        assert_eq!(first, CallEnd::Status("14".to_string()));
        assert_eq!(second, CallEnd::Status("0".to_string()));
        assert_eq!(
            unscripted,
            CallEnd::Status("0".to_string()),
            "a call beyond the script is accepted"
        );
        let captured = receiver.captured();
        assert_eq!(captured.len(), 3);
        for call in &captured {
            assert_eq!(call.path, METHOD);
            assert_eq!(
                call.header_values("content-type"),
                vec![b"application/grpc".as_slice()]
            );
        }
        assert_eq!(
            captured
                .iter()
                .map(|call| (call.compressed, call.message.clone()))
                .collect::<Vec<_>>(),
            vec![
                (false, b"first".to_vec()),
                (true, b"second".to_vec()),
                (false, b"third".to_vec()),
            ],
            "every message is captured exactly as it arrived, compressed flag included"
        );
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
        assert_eq!((stop.captured, stop.faults), (3, 0));
    }

    #[nervix_primitives::test]
    async fn a_lost_grpc_answer_is_captured_and_its_connection_closes_without_one() {
        let receiver = start().await;
        receiver.script([GrpcAnswer::LoseResponse]);
        let mut connection = connect(&receiver).await;

        let lost = call(&mut connection, message(0, b"lost")).await;
        let mut reconnected = connect(&receiver).await;
        let resent = call(&mut reconnected, message(0, b"lost")).await;

        assert_eq!(lost, CallEnd::NoAnswer);
        assert_eq!(resent, CallEnd::Status("0".to_string()));
        let captured = receiver.captured();
        assert_eq!(
            captured.len(),
            2,
            "the call whose answer was lost is still captured"
        );
        assert_eq!(
            captured[0], captured[1],
            "a resent call captures as the same call"
        );
        let stop = receiver.stop().await;
        assert!(!stop.was_forced(), "{stop}");
    }

    #[nervix_primitives::test]
    async fn held_grpc_calls_end_when_the_client_resets_them_or_the_receiver_stops() {
        let receiver = start().await;
        receiver.script([GrpcAnswer::HoldResponse, GrpcAnswer::HoldResponse]);
        let connection = connect(&receiver).await;
        let request = || {
            http::Request::builder()
                .method("POST")
                .uri(format!("http://127.0.0.1{METHOD}"))
                .header("content-type", "application/grpc")
                .body(())
                .assured("the regression request is well formed")
        };

        // The client gives up on the first held call, as a request deadline does.
        let mut ready = connection
            .clone()
            .ready()
            .await
            .assured("the connection accepts a call");
        let (_held, mut held_stream) = ready
            .send_request(request(), false)
            .assured("the connection accepts a call");
        held_stream
            .send_data(message(0, b"held"), true)
            .assured("an open call accepts its request");
        receiver
            .wait_for_calls(1, WITHIN)
            .await
            .assured("the receiver captures a held call before holding it");
        held_stream.send_reset(h2::Reason::CANCEL);
        let second = nervix_primitives::task::spawn({
            let mut connection = connection.clone();
            async move { call(&mut connection, message(0, b"held until stop")).await }
        });
        receiver
            .wait_for_calls(2, WITHIN)
            .await
            .assured("a reset call leaves its connection serving the next one");

        let stop = receiver.stop().await;

        assert!(
            !stop.was_forced() && stop.elapsed < RECEIVER_CONNECTION_STOP_BUDGET,
            "a held call observes the stop rather than waiting to be aborted: {stop}"
        );
        assert_eq!(
            second.await.assured("the held call's task ends"),
            CallEnd::NoAnswer
        );
        drop(connection);
    }

    #[nervix_primitives::test]
    async fn a_request_that_is_not_one_bounded_message_is_a_fault_not_a_capture() {
        let receiver = start().await;
        let mut connection = connect(&receiver).await;
        let above_limit = u32::try_from(MAX_REQUEST_MESSAGE_BYTES + 1)
            .assured("the message limit fits a gRPC length prefix");
        let mut declared_above_limit = vec![0];
        declared_above_limit.extend_from_slice(&above_limit.to_be_bytes());
        let mut trailing = message(0, b"one").to_vec();
        trailing.push(0);
        let malformed = [
            Bytes::from_static(&[0, 0, 0]),
            Bytes::from(declared_above_limit),
            message(0, b"four").slice(..7),
            Bytes::from(trailing),
            message(7, b"flag"),
        ];

        for body in malformed {
            let ended = call(&mut connection, body).await;
            assert_eq!(
                ended,
                CallEnd::Status("13".to_string()),
                "a malformed request is answered with INTERNAL"
            );
        }

        let error = receiver
            .wait_for_calls(1, Duration::from_millis(50))
            .await
            .expect_err("no malformed request is captured");
        assert!(
            error.to_string().contains("unknown compressed flag 7"),
            "the wait reports the latest fault: {error}"
        );
        let stop = receiver.stop().await;
        assert_eq!((stop.captured, stop.faults), (0, 5));
    }
}

mod redis_client_tests {
    use std::{net::Ipv4Addr, time::Duration};

    use meticulous::ResultExt as _;
    use nervix_primitives::{
        net::TcpListener,
        sync::oneshot,
        time::{sleep, timeout},
    };
    use redis::{AsyncCommands as _, Value};
    use tokio::io::AsyncWriteExt as _;

    use crate::redis_client::{REDIS_REQUEST_BUDGET, TestRedisClient, publish_burst};

    #[nervix_primitives::test]
    async fn redis_bursts_send_every_publication_before_waiting_for_replies() {
        for subscriber_disappears in [false, true] {
            nervix_primitives::task::consume_budget().await;
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the regression can bind an ephemeral loopback port");
            let address = listener.local_addr().assured("the listener has an address");
            let server = nervix_primitives::task::spawn(async move {
                let (mut stream, _) = listener.accept().await.assured("the client connects");
                let mut decoder = Default::default();
                for _ in 0..2 {
                    nervix_primitives::task::consume_budget().await;
                    redis::parse_redis_value_async(&mut decoder, &mut stream)
                        .await
                        .assured("the client sends its setup command");
                    stream
                        .write_all(b"+OK\r\n")
                        .await
                        .assured("the client is waiting for setup");
                }
                timeout(REDIS_REQUEST_BUDGET, async {
                    for _ in 0..256 {
                        nervix_primitives::task::consume_budget().await;
                        let command = redis::parse_redis_value_async(&mut decoder, &mut stream)
                            .await
                            .assured("every publish arrives before the first reply");
                        assert_eq!(
                            command,
                            Value::Array(vec![
                                Value::BulkString(b"PUBLISH".to_vec()),
                                Value::BulkString(b"notifications".to_vec()),
                                Value::BulkString(b"payload".to_vec()),
                            ])
                        );
                    }
                })
                .await
                .assured("the entire burst arrives without needing individual replies");
                let replies = (0..256).fold(Vec::new(), |mut replies, index| {
                    replies.extend_from_slice(if subscriber_disappears && index == 1 {
                        b":0\r\n"
                    } else {
                        b":1\r\n"
                    });
                    replies
                });
                stream
                    .write_all(&replies)
                    .await
                    .assured("the publisher reads every reply");
            });
            let client = TestRedisClient::open(&format!("redis://{address}/"))
                .assured("the loopback address is a valid Redis URL");
            let mut connection = client.connect().await.assured("the peer answers setup");
            let result = publish_burst(&mut connection, "notifications", "payload", 256).await;
            if subscriber_disappears {
                let error = result.expect_err("a publication without a subscriber fails the burst");
                assert!(error.to_string().contains("every publication"), "{error}");
            } else {
                result.assured("every publication reached a subscriber");
            }
            server
                .await
                .assured("the scripted Redis endpoint completed");
        }
    }

    #[nervix_primitives::test]
    async fn redis_harness_connections_fail_when_the_broker_never_answers() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the regression can bind an ephemeral loopback port");
        let address = listener.local_addr().assured("the listener has an address");
        let (stop, stopped) = oneshot::channel();
        let server = nervix_primitives::task::spawn(async move {
            let (stream, _) = listener.accept().await.assured("the client connects");
            stopped.await.assured("the test stops its silent endpoint");
            drop(stream);
        });
        let client = TestRedisClient::open(&format!("redis://{address}/"))
            .assured("the loopback address is a valid Redis URL");
        let result = timeout(REDIS_REQUEST_BUDGET * 2, client.connect()).await;
        stop.send(())
            .assured("the endpoint waits for the test to stop it");
        server.await.assured("the silent endpoint stopped");
        let result =
            result.assured("the driver's finite connection budget ends before the test bound");
        let Err(error) = result else {
            panic!("a silent endpoint cannot finish the Redis handshake");
        };
        assert!(error.to_string().contains("timed out"), "{error}");
    }

    #[nervix_primitives::test]
    async fn redis_harness_requests_allow_delayed_handshake_and_publish_replies() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("the regression can bind an ephemeral loopback port");
        let address = listener
            .local_addr()
            .assured("the bound listener has an address");
        let server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener.accept().await.assured("the client connects");
            let mut decoder = Default::default();
            for _ in 0..2 {
                nervix_primitives::task::consume_budget().await;
                let command = redis::parse_redis_value_async(&mut decoder, &mut stream)
                    .await
                    .assured("the Redis client sends a complete setup command");
                let Value::Array(parts) = command else {
                    panic!("Redis commands are arrays");
                };
                assert_eq!(parts[0], Value::BulkString(b"CLIENT".to_vec()));
                // Inject scheduling/broker latency at both setup replies, rather than waiting
                // for a condition that an idle machine would satisfy immediately.
                sleep(Duration::from_secs(2)).await;
                stream
                    .write_all(b"+OK\r\n")
                    .await
                    .assured("the client is waiting for setup");
            }
            let command = redis::parse_redis_value_async(&mut decoder, &mut stream)
                .await
                .assured("the client sends its publish command");
            assert_eq!(
                command,
                Value::Array(vec![
                    Value::BulkString(b"PUBLISH".to_vec()),
                    Value::BulkString(b"notifications".to_vec()),
                    Value::BulkString(b"payload".to_vec()),
                ])
            );
            sleep(Duration::from_secs(2)).await;
            stream
                .write_all(b":1\r\n")
                .await
                .assured("the client is waiting for the publish result");
        });
        let client = TestRedisClient::open(&format!("redis://{address}/"))
            .assured("the loopback address is a valid Redis URL");
        let mut connection = client
            .connect()
            .await
            .assured("the handshake replies arrive within the harness budget");
        let subscribers: usize = connection
            .publish("notifications", "payload")
            .await
            .assured("the publish reply arrives within the harness budget");
        assert_eq!(subscribers, 1);
        server
            .await
            .assured("the scripted Redis endpoint completed");
    }
}
