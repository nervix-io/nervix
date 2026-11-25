//! Answers one node sends another wait for a place on a full worker class instead of being
//! refused: relay status and grant answers, connection answers, accepted or refused, and acks, each
//! bounded by the exchange it answers.
//!
//! Layer: test harness.
//!
//! - **Owns.** Holding a worker class full around one exchange, and the owner checks that each
//!   answer waits with its own charge, refuses nothing, arrives once the class frees and returns
//!   its charge once its exchange ends.
//! - **Depends on.** The interconnect test fixture, its two bound transports and their executors.
//! - **Must not know.** Product runtime graphs or what the answers carry for the runtime.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeSet, time::Duration};

use error_stack::Report;
use nervix_execution::{CpuClass, Executor, MemoryClass, QueueAdmission};
use nervix_models::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, DomainName, NodeEndpoint,
    PrerequisiteMismatch, PrerequisiteTimings, RelayName, RemoteAckOutcome, RemoteAckRegistration,
    TimingPrerequisite,
};
use nervix_primitives::time::{Instant, timeout};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    Envelope, InterconnectRequest, JoinProgress, JoinRefusal, JoinTerms, PoolClass,
    RelayAdmissionStatus, RelayDelivery, RelayPayload, RelayPayloadKind, Transport, TransportError,
    TransportIdentity, TransportOptions,
    observation::ConnectionDirection,
    tests::{
        ConnectedTransports, TestCertificateAuthority, bound_transports_with_options,
        resolver::{localhost_identity, test_join_terms, test_resolver},
    },
};

/// A bulk request whose class is not preconnected, so the first one dials its own connection.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct AnswerProbe;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct AnswerProbeResponse;

impl InterconnectRequest for AnswerProbe {
    type Response = AnswerProbeResponse;

    const NAME: &'static str = "test_answer_probe";
    const CLASS: PoolClass = PoolClass::Bulk;
}

/// Two bound transports with the first dialing the second.
async fn connected_with_options(options: TransportOptions) -> ConnectedTransports {
    let transports = bound_transports_with_options(options).await;
    transports
        .transport_a
        .register_outbound_target(
            transports.node_b.clone(),
            NodeEndpoint::new("localhost", transports.transport_b.local_addr().port()),
        )
        .expect("authenticated test target should register");
    transports
}

/// The ack registration `ack_id` that `registrar` made in its first run.
fn registration(ack_id: u64, registrar: &ClusterNodeName) -> RemoteAckRegistration {
    RemoteAckRegistration {
        ack_id,
        registrar: ClusterNodeIdentity::new(registrar.clone(), ClusterNodeIncarnation::new(1)),
    }
}

/// Every worker of one CPU class busy with a job the test releases, and every place in its wait
/// queue taken by a job waiting behind them. A job that refuses on a full class is refused, and
/// one that waits for a place waits, until the hold is released.
pub(crate) struct HeldCpuClass {
    releases: Vec<nervix_primitives::sync::oneshot::Sender<()>>,
    jobs: Vec<nervix_primitives::task::JoinHandle<()>>,
}

impl HeldCpuClass {
    pub(crate) async fn hold(executor: &Executor, class: CpuClass) -> Self {
        let capacity = executor.snapshot().cpu_class(class);
        let mut releases = Vec::new();
        let mut jobs = Vec::new();
        for _ in 0..capacity.workers {
            let (started, has_started) = nervix_primitives::sync::oneshot::channel();
            let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
            releases.push(release);
            let charge = executor
                .try_reserve(MemoryClass::Bulk, 1)
                .expect("the test's bulk class has room for one byte per held job");
            let occupier = executor.clone();
            jobs.push(nervix_primitives::task::spawn(async move {
                occupier
                    .run_cpu(class, charge, move |_charge, _cancellation| {
                        started
                            .send(())
                            .expect("the test awaits each occupying job");
                        released
                            .blocking_recv()
                            .expect("the test releases each occupying job before it exits");
                    })
                    .await
                    .expect("an occupying job takes a worker");
            }));
            has_started
                .await
                .expect("an occupying job reports once it holds its worker");
        }
        for _ in 0..capacity.queue_capacity {
            let charge = executor
                .try_reserve(MemoryClass::Bulk, 1)
                .expect("the test's bulk class has room for one byte per held job");
            let queuer = executor.clone();
            jobs.push(nervix_primitives::task::spawn(async move {
                queuer
                    .run_cpu(class, charge, |_charge, _cancellation| ())
                    .await
                    .expect("a queued job takes a place behind the occupied workers");
            }));
        }
        while executor.snapshot().cpu_class(class).pending < capacity.queue_capacity {
            nervix_primitives::task::yield_now().await;
        }
        Self { releases, jobs }
    }

    /// Let every occupying job exit, so the queued jobs run and free their places, and wait for
    /// all of them.
    pub(crate) async fn release(self) {
        for release in self.releases {
            release
                .send(())
                .expect("each occupying job waits for its release");
        }
        for job in self.jobs {
            job.await.expect("each holding job's task is joined");
        }
    }
}

/// Poll `future` once, which must leave it pending, announce that on `polled`, then finish it.
/// A job that waits for a place has joined the line for one once it reports.
async fn announce_after_first_poll<F: Future>(
    future: F,
    polled: nervix_primitives::sync::oneshot::Sender<()>,
) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|context| match future.as_mut().poll(context) {
        std::task::Poll::Ready(_) => {
            panic!("a job behind every busy worker of its class cannot finish on its first poll")
        }
        std::task::Poll::Pending => std::task::Poll::Ready(()),
    })
    .await;
    polled
        .send(())
        .expect("the test waits until each job has joined its line");
    future.await
}

/// Every worker of one CPU class busy with a job the test releases, with its wait queue left to
/// whatever arrives next.
pub(crate) struct OccupiedWorkers {
    executor: Executor,
    class: CpuClass,
    releases: Vec<nervix_primitives::sync::oneshot::Sender<()>>,
    jobs: Vec<nervix_primitives::task::JoinHandle<()>>,
}

impl OccupiedWorkers {
    pub(crate) async fn occupy(executor: &Executor, class: CpuClass) -> Self {
        let workers = executor.snapshot().cpu_class(class).workers;
        let mut occupied = Self {
            executor: executor.clone(),
            class,
            releases: Vec::new(),
            jobs: Vec::new(),
        };
        for _ in 0..workers {
            let started = occupied.hold_worker(QueueAdmission::RefuseWhenFull);
            started
                .await
                .expect("an occupying job reports once it holds its worker");
        }
        occupied
    }

    /// Submit a job that holds a worker until the test releases it, and return the signal it sends
    /// once it holds one.
    fn hold_worker(
        &mut self,
        admission: QueueAdmission,
    ) -> nervix_primitives::sync::oneshot::Receiver<()> {
        let (started, has_started) = nervix_primitives::sync::oneshot::channel();
        let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
        self.releases.push(release);
        let charge = self
            .executor
            .try_reserve(MemoryClass::Bulk, 1)
            .expect("the test's bulk class has room for one byte per held job");
        let occupier = self.executor.clone();
        let class = self.class;
        self.jobs.push(nervix_primitives::task::spawn(async move {
            occupier
                .run_cpu_with(class, admission, charge, move |_charge, _cancellation| {
                    started.send(()).expect("the test awaits each holding job");
                    released
                        .blocking_recv()
                        .expect("the test releases each holding job before it exits");
                })
                .await
                .expect("a holding job takes a worker");
        }));
        has_started
    }

    /// Keep the class full past the job now at the head of its queue. A job that will hold a worker
    /// and one waiting job per place join the line behind the head job, then one worker frees: the
    /// head job runs, the holding job takes the worker after it, and the waiting jobs take every
    /// place that frees. The next job the class is handed finds every worker busy and every place
    /// taken, and it stays so until the test releases the class.
    pub(crate) async fn keep_full_after_head(mut self) -> Self {
        let capacity = self
            .executor
            .snapshot()
            .cpu_class(self.class)
            .queue_capacity;
        let (started, has_started) = nervix_primitives::sync::oneshot::channel();
        let (release, released) = nervix_primitives::sync::oneshot::channel::<()>();
        let charge = self
            .executor
            .try_reserve(MemoryClass::Bulk, 1)
            .expect("the test's bulk class has room for one byte per held job");
        let holder = self.executor.clone();
        let class = self.class;
        let (joined, has_joined) = nervix_primitives::sync::oneshot::channel();
        self.jobs.push(nervix_primitives::task::spawn(async move {
            let job = holder.run_cpu_with(
                class,
                QueueAdmission::WaitForPlace,
                charge,
                move |_charge, _cancellation| {
                    started
                        .send(())
                        .expect("the test awaits the job that keeps the class full");
                    released
                        .blocking_recv()
                        .expect("the test releases the job that keeps the class full");
                },
            );
            announce_after_first_poll(job, joined)
                .await
                .expect("the job that keeps the class full takes a worker");
        }));
        has_joined
            .await
            .expect("the job that keeps the class full joins its line");
        for _ in 0..capacity {
            let charge = self
                .executor
                .try_reserve(MemoryClass::Bulk, 1)
                .expect("the test's bulk class has room for one byte per held job");
            let waiter = self.executor.clone();
            let (joined, has_joined) = nervix_primitives::sync::oneshot::channel();
            self.jobs.push(nervix_primitives::task::spawn(async move {
                let job = waiter.run_cpu_with(
                    class,
                    QueueAdmission::WaitForPlace,
                    charge,
                    |_charge, _cancellation| (),
                );
                announce_after_first_poll(job, joined)
                    .await
                    .expect("a job that keeps a place runs once the class is released");
            }));
            has_joined
                .await
                .expect("a job that keeps a place joins its line");
        }
        let freed = self.releases.remove(0);
        freed
            .send(())
            .expect("an occupying job waits for its release");
        has_started
            .await
            .expect("the job that keeps the class full takes the freed worker");
        self.releases.push(release);
        self
    }

    pub(crate) async fn release(self) {
        for release in self.releases {
            release
                .send(())
                .expect("each holding job waits for its release");
        }
        for job in self.jobs {
            job.await.expect("each holding job's task is joined");
        }
    }
}

/// Wait until `condition` holds of `executor`, failing with `what` if it does not within a
/// generous bound.
pub(crate) async fn eventually(
    executor: &Executor,
    what: &str,
    condition: impl Fn(&Executor) -> bool,
) {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(30))
        .expect("a thirty-second bound fits the monotonic clock");
    while !condition(executor) {
        nervix_primitives::task::consume_budget().await;
        assert!(
            Instant::now() < deadline,
            "{what} never happened: {:?}",
            executor.snapshot()
        );
        nervix_primitives::time::sleep(Duration::from_millis(1)).await;
    }
}

/// One routed delivery of a single-byte batch on channel `channel`, registered by `registrar`.
fn routed_delivery(channel: u8, registrar: &ClusterNodeName) -> RelayPayload {
    RelayPayload {
        ack_timing: nervix_models::RemoteAckTiming::default(),
        decoded_bytes: 0,
        delivery: RelayDelivery {
            channel_incarnation: [channel; 16],
            sequence: 0,
        },
        kind: RelayPayloadKind::Routed,
        domain: DomainName::parse("test").expect("test domain should be valid"),
        relay: RelayName::parse("relay").expect("test relay should be valid"),
        unit: nervix_models::Unit::SINGLE,
        key: nervix_models::RemoteBranchKey::Unbranched,
        batch_ipc: Executor::default()
            .try_charge_owned(MemoryClass::Relay, vec![1])
            .expect("the test relay body should fit its budget"),
        metadata: Vec::new(),
        acks: Vec::new(),
        admission: Some(registration(u64::from(channel), registrar)),
    }
}

/// Send one delivery and wait for the receiver to take it in, so the connections a relay needs
/// are bound before a test fills a class their binding uses.
async fn relay_one_delivery(
    transports: &mut ConnectedTransports,
    channel: u8,
) -> Result<(), Report<TransportError>> {
    let payload = routed_delivery(channel, transports.transport_a.node_id());
    transports
        .transport_a
        .send(&transports.node_b, Envelope::RelayPayload(payload))
        .await?;
    let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
        .await
        .expect("the granted relay should enter the application queue")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
    Ok(())
}

/// The receiver has already decided a grant, and the grant already holds its relay memory, so a
/// receiver whose control class is full holds the grant's answer back instead of refusing it and
/// leaving the grant to be reconciled. The delivery is granted and taken in once the class frees,
/// and a sender that drops the delivery resets the exchange, which ends the answer's wait and
/// returns its charge.
#[nervix_primitives::test]
async fn a_relay_grant_answer_waits_for_a_place_on_its_receivers_full_control_class() {
    for sender_gives_up in [false, true] {
        let mut transports = connected_with_options(TransportOptions::default()).await;
        relay_one_delivery(&mut transports, 1)
            .await
            .expect("an idle receiver grants the first delivery");

        let executor_b = transports.transport_b.inner.executor().clone();
        let held = HeldCpuClass::hold(&executor_b, CpuClass::Control).await;
        let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
        let reserved = executor_b.memory(MemoryClass::Management).reserved_bytes;
        let transport_a = transports.transport_a.clone();
        let node_b = transports.node_b.clone();
        let payload = routed_delivery(2, transports.transport_a.node_id());
        let sending = nervix_primitives::task::spawn(async move {
            transport_a
                .send(&node_b, Envelope::RelayPayload(payload))
                .await
        });
        eventually(
            &executor_b,
            "the grant's answer waiting with its charge",
            |executor| {
                executor.memory(MemoryClass::Management).reserved_bytes > reserved
                    || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
            },
        )
        .await;
        assert_eq!(
            executor_b.snapshot().cpu_class(CpuClass::Control).refused,
            refused,
            "a full class refuses no grant answer"
        );

        if sender_gives_up {
            // Dropping the delivery resets its grant exchange, which ends the answer's wait.
            sending.abort();
            eventually(
                &executor_b,
                "the sender's reset ending the grant answer's wait",
                |executor| executor.memory(MemoryClass::Management).reserved_bytes <= reserved,
            )
            .await;
            assert_eq!(
                executor_b.snapshot().cpu_class(CpuClass::Control).refused,
                refused,
                "a full class refuses no grant answer"
            );
            held.release().await;
        } else {
            held.release().await;
            timeout(Duration::from_secs(10), sending)
                .await
                .expect("the delivery is sent before the test deadline")
                .expect("the sending task is joined")
                .expect("the grant's answer arrives once the receiver's class frees");
            let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
                .await
                .expect("the granted relay should enter the application queue")
                .expect("the application queue should remain open");
            assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
        }
        transports
            .transport_a
            .shutdown(crate::test_teardown_ceiling())
            .await;
        transports
            .transport_b
            .shutdown(crate::test_teardown_ceiling())
            .await;
    }
}

/// A grant's answer that reaches a sender whose control class is full waits for a place to be
/// decoded rather than turning the delivery indeterminate, refusing nothing, and the delivery is
/// taken in once the class frees.
#[nervix_primitives::test]
async fn a_relay_grant_answer_waits_for_a_place_on_its_senders_full_control_class() {
    let mut transports = connected_with_options(TransportOptions::default()).await;
    relay_one_delivery(&mut transports, 1)
        .await
        .expect("an idle receiver grants the first delivery");

    let executor_a = transports.transport_a.inner.executor().clone();
    let held = HeldCpuClass::hold(&executor_a, CpuClass::Control).await;
    let refused = executor_a.snapshot().cpu_class(CpuClass::Control).refused;
    let reserved = executor_a.memory(MemoryClass::Management).reserved_bytes;
    let transport_a = transports.transport_a.clone();
    let node_b = transports.node_b.clone();
    let payload = routed_delivery(2, transports.transport_a.node_id());
    let sending = nervix_primitives::task::spawn(async move {
        transport_a
            .send(&node_b, Envelope::RelayPayload(payload))
            .await
    });

    eventually(
        &executor_a,
        "the grant's answer waiting to be decoded with its charge",
        |executor| {
            executor.memory(MemoryClass::Management).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
        },
    )
    .await;
    assert_eq!(
        executor_a.snapshot().cpu_class(CpuClass::Control).refused,
        refused,
        "a full class refuses no grant answer"
    );
    held.release().await;
    timeout(Duration::from_secs(10), sending)
        .await
        .expect("the delivery is sent before the test deadline")
        .expect("the sending task is joined")
        .expect("the grant's answer is decoded once the sender's class frees");
    let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
        .await
        .expect("the granted relay should enter the application queue")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::RelayPayload(_)));
    transports
        .transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    transports
        .transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

/// Wait until the second transport holds every preconnected connection the first one dials to it,
/// so the only handshake a test still sees is the one it starts.
async fn every_preconnected_pool_bound(transports: &ConnectedTransports) {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(30))
        .expect("a thirty-second bound fits the monotonic clock");
    loop {
        nervix_primitives::task::consume_budget().await;
        let connections =
            transports.transport_b.snapshot().connections[ConnectionDirection::Inbound.index()];
        let mut bound = 0usize;
        for class in PoolClass::PRECONNECTED {
            bound = bound
                .checked_add(connections[class.index()])
                .expect("a test holds few connections");
        }
        if bound == PoolClass::preconnected_connections_per_peer() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the preconnected pools never bound: {connections:?}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The accepting node's answer to a connection hello carries the binding it has already validated
/// and registered, so an acceptor whose control class fills between the hello and the answer holds
/// the answer back, with its own charge, instead of failing the binding: the connection binds and
/// its first request is answered once the class frees. A dialing node that stops dialing, at its
/// setup deadline or because it shuts down, closes the connection, and that ends the acceptor's
/// wait and returns the answer's charge.
#[nervix_primitives::test]
async fn a_connection_answer_waits_for_a_place_on_its_acceptors_full_control_class() {
    // Half of the answer's 4 KiB first granule: a queued hello's own charges are far smaller.
    let answer_charge = 2 * 1024;
    for dialer_gives_up in [false, true] {
        // The setup deadline outlasts the test, so only the test decides when the dialer stops.
        let transports = connected_with_options(TransportOptions {
            connection_setup_timeout: Duration::from_secs(600),
            ..TransportOptions::default()
        })
        .await;
        transports
            .transport_b
            .register_handler::<AnswerProbe, _, _>(|_context, _request| async move {
                AnswerProbeResponse
            })
            .expect("the bulk handler registers");
        every_preconnected_pool_bound(&transports).await;

        let executor_b = transports.transport_b.inner.executor().clone();
        let occupied = OccupiedWorkers::occupy(&executor_b, CpuClass::Control).await;
        let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
        let transport_a = transports.transport_a.clone();
        let node_b = transports.node_b.clone();
        // A bulk connection is dialed on first use, so this request starts exactly one handshake.
        let requesting = nervix_primitives::task::spawn(async move {
            transport_a.request(&node_b, AnswerProbe).await
        });
        eventually(
            &executor_b,
            "the hello waiting at the head of the control queue",
            |executor| executor.snapshot().cpu_class(CpuClass::Control).pending == 1,
        )
        .await;
        // The queued hello holds its own small charges, which it gives back once decoded; the
        // answer's encoding then holds its whole first granule.
        let baseline = executor_b.memory(MemoryClass::Management).reserved_bytes;
        let held = occupied.keep_full_after_head().await;
        eventually(
            &executor_b,
            "the answer waiting with its charge",
            |executor| {
                executor.memory(MemoryClass::Management).reserved_bytes
                    >= baseline
                        .checked_add(answer_charge)
                        .expect("a test's charges are small")
                    || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
            },
        )
        .await;
        assert_eq!(
            executor_b.snapshot().cpu_class(CpuClass::Control).refused,
            refused,
            "a full class refuses no connection answer"
        );

        if dialer_gives_up {
            // The dial runs in its pool's connection worker, so the dialing node stops it by
            // cancelling that worker, as its setup deadline does.
            requesting.abort();
            transports
                .transport_a
                .shutdown(crate::test_teardown_ceiling())
                .await;
            eventually(
                &executor_b,
                "the dialing node's departure ending the answer's wait",
                |executor| {
                    executor.memory(MemoryClass::Management).reserved_bytes
                        < baseline
                            .checked_add(answer_charge)
                            .expect("a test's charges are small")
                },
            )
            .await;
            assert_eq!(
                executor_b.snapshot().cpu_class(CpuClass::Control).refused,
                refused,
                "a full class refuses no connection answer"
            );
            held.release().await;
        } else {
            held.release().await;
            let response = timeout(Duration::from_secs(10), requesting)
                .await
                .expect("the request is answered before the test deadline")
                .expect("the requesting task is joined")
                .expect("the connection binds once the acceptor's class frees");
            assert_eq!(response, AnswerProbeResponse);
            transports
                .transport_a
                .shutdown(crate::test_teardown_ceiling())
                .await;
        }
        transports
            .transport_b
            .shutdown(crate::test_teardown_ceiling())
            .await;
    }
}

/// Two transports of one cluster whose Raft heartbeats lie beyond each other's compatibility bound,
/// so the second refuses every hello the first sends it.
struct MismatchedTransports {
    _authority: TestCertificateAuthority,
    dialing: Transport,
    accepting: Transport,
    accepting_node: ClusterNodeName,
    accepting_executor: Executor,
}

impl MismatchedTransports {
    /// The heartbeat every test transport brings.
    const DIALING_HEARTBEAT: Duration = Duration::from_millis(250);
    /// At least twice the dialing node's heartbeat, which no node may run beside.
    const ACCEPTING_HEARTBEAT: Duration = Duration::from_secs(1);

    async fn bind(options: TransportOptions) -> Self {
        let authority = TestCertificateAuthority::new();
        let dialing_node =
            ClusterNodeName::parse("node-a").expect("test node name should be valid");
        let accepting_node =
            ClusterNodeName::parse("node-b").expect("test node name should be valid");
        let dialing_identity = localhost_identity("test-cluster", dialing_node.clone());
        assert_eq!(
            dialing_identity.join_terms.timings.raft_heartbeat_interval,
            Self::DIALING_HEARTBEAT,
            "the dialing node brings the heartbeat every test transport brings"
        );
        let shared = test_join_terms();
        let accepting_identity = TransportIdentity {
            join_terms: JoinTerms {
                durability: shared.durability,
                timings: PrerequisiteTimings {
                    raft_heartbeat_interval: Self::ACCEPTING_HEARTBEAT,
                    ..shared.timings
                },
            },
            ..localhost_identity("test-cluster", accepting_node.clone())
        };
        let (dialing, _incoming) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            dialing_identity,
            authority.issue("test-cluster", &dialing_node),
            options.clone(),
            Executor::default(),
            test_resolver().await,
        )
        .await
        .expect("the dialing test transport should bind");
        let accepting_executor = Executor::default();
        let (accepting, _incoming) = Transport::bind(
            "127.0.0.1:0".parse().expect("test address should be valid"),
            accepting_identity,
            authority.issue("test-cluster", &accepting_node),
            options,
            accepting_executor.clone(),
            test_resolver().await,
        )
        .await
        .expect("the accepting test transport should bind");
        let live_nodes = BTreeSet::from([dialing_node, accepting_node.clone()]);
        dialing.replace_live_nodes(&live_nodes);
        accepting.replace_live_nodes(&live_nodes);
        Self {
            _authority: authority,
            dialing,
            accepting,
            accepting_node,
            accepting_executor,
        }
    }

    /// Start every preconnected connection the dialing node keeps to the accepting node.
    fn dial(&self) {
        self.dialing
            .register_outbound_target(
                self.accepting_node.clone(),
                NodeEndpoint::new("localhost", self.accepting.local_addr().port()),
            )
            .expect("authenticated test target should register");
    }

    /// The refusal the dialing node reads once the accepting node has refused its hello.
    fn refusal(&self) -> JoinRefusal {
        JoinRefusal {
            peer: self.accepting_node.clone(),
            mismatch: PrerequisiteMismatch::Timing {
                prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
                local: Self::DIALING_HEARTBEAT,
                peer: Self::ACCEPTING_HEARTBEAT,
            },
        }
    }
}

/// A refusal is the accepting node's verdict on the prerequisites the dialing node sent, which the
/// dialing node reports, and which ends its join, so an acceptor whose control class fills between
/// the hellos and their refusals holds each refusal back, with its own charge, instead of closing
/// the binding without one: the dialing node reads the mismatch once the class frees. A dialing
/// node that stops dialing closes its connections, and that ends the acceptor's waits and returns
/// the refusals' charges.
#[nervix_primitives::test]
async fn a_connection_refusal_waits_for_a_place_on_its_acceptors_full_control_class() {
    // Half of one refusal's 4 KiB first granule: a queued hello's own charges are far smaller.
    let refusal_charge = 2 * 1024;
    let hellos = PoolClass::preconnected_connections_per_peer();
    for dialer_gives_up in [false, true] {
        // The setup deadline outlasts the test, so only the test decides when the dialer stops.
        let transports = MismatchedTransports::bind(TransportOptions {
            connection_setup_timeout: Duration::from_secs(600),
            ..TransportOptions::default()
        })
        .await;
        let executor_b = transports.accepting_executor.clone();
        let occupied = OccupiedWorkers::occupy(&executor_b, CpuClass::Control).await;
        let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
        let mut progress = transports.dialing.join_progress();
        // Nothing else dials the accepting node, so these are the only handshakes it sees.
        transports.dial();
        eventually(
            &executor_b,
            "every preconnected hello waiting in the control queue",
            |executor| executor.snapshot().cpu_class(CpuClass::Control).pending == hellos,
        )
        .await;
        // The queued hellos hold their own small charges, which each gives back once decoded; a
        // refusal's encoding then holds its whole first granule.
        let baseline = executor_b.memory(MemoryClass::Management).reserved_bytes;
        let held = occupied.keep_full_after_head().await;
        eventually(
            &executor_b,
            "a refusal waiting with its charge",
            |executor| {
                executor.memory(MemoryClass::Management).reserved_bytes
                    >= baseline
                        .checked_add(refusal_charge)
                        .expect("a test's charges are small")
                    || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
            },
        )
        .await;
        assert_eq!(
            executor_b.snapshot().cpu_class(CpuClass::Control).refused,
            refused,
            "a full class refuses no connection refusal"
        );
        assert_eq!(
            *progress.borrow(),
            JoinProgress::Joining,
            "no refusal reaches the dialing node while the acceptor's class is full"
        );

        if dialer_gives_up {
            transports
                .dialing
                .shutdown(crate::test_teardown_ceiling())
                .await;
            eventually(
                &executor_b,
                "the dialing node's departure ending the refusals' waits",
                |executor| {
                    executor.memory(MemoryClass::Management).reserved_bytes
                        < baseline
                            .checked_add(refusal_charge)
                            .expect("a test's charges are small")
                },
            )
            .await;
            assert_eq!(
                executor_b.snapshot().cpu_class(CpuClass::Control).refused,
                refused,
                "a full class refuses no connection refusal"
            );
            held.release().await;
        } else {
            held.release().await;
            let ended = timeout(
                Duration::from_secs(10),
                progress.wait_for(|progress| *progress != JoinProgress::Joining),
            )
            .await
            .expect("a refusal reaches the dialing node before the test deadline")
            .expect("the dialing transport keeps its join progress")
            .clone();
            assert_eq!(
                ended,
                JoinProgress::Refused(transports.refusal()),
                "the refusal the dialing node read names the accepting node's mismatch and ends \
                 its join"
            );
            transports
                .dialing
                .shutdown(crate::test_teardown_ceiling())
                .await;
        }
        transports
            .accepting
            .shutdown(crate::test_teardown_ceiling())
            .await;
    }
}

/// An ack carries the outcome of a delivery its sender already resolved, which nothing presents
/// again, so a sender whose data class is full holds the ack's encoding back, with its own charge,
/// instead of refusing it, and the ack reaches its registrar once the class frees.
#[nervix_primitives::test]
async fn an_ack_waits_for_a_place_on_its_senders_full_data_class() {
    let mut transports = connected_with_options(TransportOptions::default()).await;
    let node_a = transports.transport_a.node_id().clone();
    let ack =
        |ack_id| Envelope::Ack(registration(ack_id, &node_a).resolution(RemoteAckOutcome::Ack));
    transports
        .transport_a
        .send(&transports.node_b, ack(1))
        .await
        .expect("an idle sender sends the first ack");
    let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
        .await
        .expect("the first ack reaches its registrar before the test deadline")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::Ack(_)));

    let executor_a = transports.transport_a.inner.executor().clone();
    let held = HeldCpuClass::hold(&executor_a, CpuClass::Data).await;
    let refused = executor_a.snapshot().cpu_class(CpuClass::Data).refused;
    let reserved = executor_a.memory(MemoryClass::Management).reserved_bytes;
    let transport_a = transports.transport_a.clone();
    let node_b = transports.node_b.clone();
    let second = ack(2);
    let sending =
        nervix_primitives::task::spawn(async move { transport_a.send(&node_b, second).await });

    eventually(
        &executor_a,
        "the ack's encoding waiting with its charge",
        |executor| {
            executor.memory(MemoryClass::Management).reserved_bytes > reserved
                || executor.snapshot().cpu_class(CpuClass::Data).refused > refused
        },
    )
    .await;
    assert_eq!(
        executor_a.snapshot().cpu_class(CpuClass::Data).refused,
        refused,
        "a full class refuses no ack"
    );
    held.release().await;
    timeout(Duration::from_secs(10), sending)
        .await
        .expect("the ack is sent before the test deadline")
        .expect("the sending task is joined")
        .expect("the ack is encoded once the sender's class frees");
    let received = timeout(Duration::from_secs(10), transports.incoming_b.recv())
        .await
        .expect("the second ack reaches its registrar before the test deadline")
        .expect("the application queue should remain open");
    assert!(matches!(received.envelope, Envelope::Ack(_)));
    transports
        .transport_a
        .shutdown(crate::test_teardown_ceiling())
        .await;
    transports
        .transport_b
        .shutdown(crate::test_teardown_ceiling())
        .await;
}

/// A relay admission status is the receiver's verdict on an attempt, and a cancellation it answers
/// is already applied, so a receiver whose control class fills between the query and its answer
/// holds the answer back, with its own charge, instead of refusing it: the sender receives the
/// status once the class frees. A sender that stops waiting, at its deadline or otherwise, resets
/// the exchange, and that ends the receiver's wait and returns the answer's charge.
#[nervix_primitives::test]
async fn a_relay_status_answer_waits_for_a_place_on_its_receivers_full_control_class() {
    // Half of the answer's 4 KiB first granule: a queued query's own charges are far smaller.
    let answer_charge = 2 * 1024;
    for sender_gives_up in [false, true] {
        // The deadline outlasts the test, so only the test decides when the sender stops waiting.
        let transports = connected_with_options(TransportOptions {
            ..TransportOptions::default()
        })
        .await;
        every_preconnected_pool_bound(&transports).await;
        // A delivery the receiver never saw has no retained attempt, so nothing but this query
        // reaches the receiver's control class.
        let delivery = RelayDelivery {
            channel_incarnation: [9; 16],
            sequence: 0,
        };

        let executor_b = transports.transport_b.inner.executor().clone();
        let occupied = OccupiedWorkers::occupy(&executor_b, CpuClass::Control).await;
        let refused = executor_b.snapshot().cpu_class(CpuClass::Control).refused;
        let transport_a = transports.transport_a.clone();
        let node_b = transports.node_b.clone();
        let querying = nervix_primitives::task::spawn(async move {
            transport_a
                .relay_admission_status(
                    &node_b,
                    delivery,
                    nervix_models::RemoteAckTiming::default(),
                )
                .await
        });
        eventually(
            &executor_b,
            "the query waiting at the head of the control queue",
            |executor| executor.snapshot().cpu_class(CpuClass::Control).pending == 1,
        )
        .await;
        // The queued query holds its own small charges, which it gives back once decoded; the
        // status's encoding then holds its whole first granule.
        let baseline = executor_b.memory(MemoryClass::Management).reserved_bytes;
        let held = occupied.keep_full_after_head().await;
        eventually(
            &executor_b,
            "the status waiting with its charge",
            |executor| {
                executor.memory(MemoryClass::Management).reserved_bytes
                    >= baseline
                        .checked_add(answer_charge)
                        .expect("a test's charges are small")
                    || executor.snapshot().cpu_class(CpuClass::Control).refused > refused
            },
        )
        .await;
        assert_eq!(
            executor_b.snapshot().cpu_class(CpuClass::Control).refused,
            refused,
            "a full class refuses no status answer"
        );

        if sender_gives_up {
            querying.abort();
            eventually(
                &executor_b,
                "the sender's reset ending the status's wait",
                |executor| {
                    executor.memory(MemoryClass::Management).reserved_bytes
                        < baseline
                            .checked_add(answer_charge)
                            .expect("a test's charges are small")
                },
            )
            .await;
            assert_eq!(
                executor_b.snapshot().cpu_class(CpuClass::Control).refused,
                refused,
                "a full class refuses no status answer"
            );
            held.release().await;
        } else {
            held.release().await;
            let status = timeout(Duration::from_secs(10), querying)
                .await
                .expect("the status arrives before the test deadline")
                .expect("the querying task is joined")
                .expect("the status is answered once the receiver's class frees");
            assert_eq!(status, RelayAdmissionStatus::Unknown);
        }
        transports
            .transport_a
            .shutdown(crate::test_teardown_ceiling())
            .await;
        transports
            .transport_b
            .shutdown(crate::test_teardown_ceiling())
            .await;
    }
}
