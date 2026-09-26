//! Pool isolation, bounded admission and resource release around a stalled peer.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** The three-host stalled-peer plan, its bounded request fixtures, and the capacity
//!   bounds every owner observation of that plan is checked against.
//! - **Depends on.** The transport fixture, the production request, stream and relay APIs, the
//!   transport and executor snapshots, and Turmoil link holds.
//! - **Must not know.** Runtime graphs, persistent stores, or connector behavior.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use error_stack::Report;
use futures_util::{StreamExt as _, future::join_all, stream};
use nervix_execution::{ExecutionConfig, ExecutorSnapshot, WorkerCounts};
use nervix_interconnect::{
    ConnectionDirection, IncomingByteStream, InterconnectStreamRequest,
    MAX_CONCURRENT_HEALTH_PROBES, RelayAdmissionDecision, RelayAdmissionStatus, StreamHandlerError,
    StreamingResponse, TransferDirection, TransportSnapshot,
};
use nervix_models::{RemoteAckOutcome, RemoteAckResolution};
use parking_lot::Mutex;
use strum::{EnumCount as _, IntoEnumIterator as _};
use tokio::{sync::oneshot, task::JoinHandle};

use super::*;

const HUB: &str = "hub";
const STALLED: &str = "stalled";
const HEALTHY: &str = "healthy";

/// Shared management streams one connection leases, from the documented management partition.
const MANAGEMENT_SHARED_STREAMS: usize = 32;
/// Jobs one hub worker class may hold waiting, small enough for one burst to fill it.
const HUB_PENDING_JOBS: usize = 8;
/// Burst requests beyond what one worker and its full wait queue admit.
const REFUSED_JOBS: usize = 2;
const STREAM_CHUNK_BYTES: usize = 16 * 1024;
const STREAM_BYTES: u64 = 256 * 1024;
/// How long one more shared management request waits for a stream on the saturated connection.
const SLOT_WAIT: Duration = Duration::from_millis(250);
/// The interval between reads inside a condition wait: one simulation tick.
const POLL: Duration = Duration::from_millis(1);
/// How far a release may fall from its documented deadline: the observation interval on each side.
const DEADLINE_SLACK: Duration = Duration::from_millis(2);

/// A shared management operation the stalled peer accepts and never answers.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct StalledOperation;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct StalledOperationResponse;

impl InterconnectRequest for StalledOperation {
    type Response = StalledOperationResponse;

    const NAME: &'static str = "simulation_stalled_operation";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(60);
}

/// A shared management operation answered at once.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct ManagementProbe;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct ManagementProbeResponse {
    peer: ClusterNodeName,
}

impl InterconnectRequest for ManagementProbe {
    type Response = ManagementProbeResponse;

    const NAME: &'static str = "simulation_management_probe";
    const CLASS: PoolClass = PoolClass::Management;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

/// A management operation on the reserved relay-cancellation streams.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct CancellationProbe;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct CancellationProbeResponse;

impl InterconnectRequest for CancellationProbe {
    type Response = CancellationProbeResponse;

    const NAME: &'static str = "simulation_cancellation_probe";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Cancellation;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

/// A management operation on the reserved discovery streams gossip uses.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct DiscoveryProbe;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct DiscoveryProbeResponse;

impl InterconnectRequest for DiscoveryProbe {
    type Response = DiscoveryProbeResponse;

    const NAME: &'static str = "simulation_discovery_probe";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

/// A typed request on the bulk pool, encoded and decoded by the bulk CPU class.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct BulkProbe;

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct BulkProbeResponse;

impl InterconnectRequest for BulkProbe {
    type Response = BulkProbeResponse;

    const NAME: &'static str = "simulation_bulk_probe";
    const CLASS: PoolClass = PoolClass::Bulk;
    const TIMEOUT: Duration = Duration::from_secs(10);
}

/// A flow-controlled resource stream the hub produces in charged chunks.
#[derive(Debug, Archive, Serialize, Deserialize)]
struct ResourceStream;

impl InterconnectStreamRequest for ResourceStream {
    const NAME: &'static str = "simulation_resource_stream";
    const SUBQUOTA: RequestSubquota = RequestSubquota::Resource;
    const TIMEOUT: Duration = Duration::from_secs(2);
}

/// Worker counts fixed independently of the machine, so every replay admits the same jobs.
fn executor(pending_jobs: usize) -> Executor {
    Executor::new(ExecutionConfig {
        workers: WorkerCounts {
            control_cpu: NonZeroUsize::MIN,
            data_cpu: NonZeroUsize::MIN,
            bulk_cpu: NonZeroUsize::MIN,
            consensus_storage: NonZeroUsize::MIN,
            filesystem_storage: NonZeroUsize::MIN,
            pending_jobs: NonZeroUsize::new(pending_jobs).assured("fixture queues are nonzero"),
        },
        ..ExecutionConfig::default()
    })
    .assured("the fixture keeps the default budgets and operation limits")
}

async fn bind_host(
    name: &'static str,
    credentials: Credentials,
    seed: u64,
    executor: Executor,
) -> (Transport, mpsc::Receiver<ReceivedEnvelope>) {
    let mut options = TransportOptions::default();
    let entropy = SimulatedEntropy::new(seed, name);
    options.entropy = TransportEntropy::from_source(move || entropy.next_u64());
    Transport::bind(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT)),
        TransportIdentity {
            cluster_id: CLUSTER.to_string(),
            node_id: node(name),
            advertised_host: name.to_string(),
        },
        credentials.bundle(),
        options,
        executor,
        PeerResolver::simulated(),
    )
    .await
    .assured("fixture transport binds on its simulated host")
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn live(names: &[&str]) -> BTreeSet<ClusterNodeName> {
    names.iter().map(|name| node(name)).collect()
}

/// Poll `future` exactly once, so the caller can observe what that first poll admitted.
async fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut future = Some(future);
    poll_fn(|context| {
        let future = future.take().assured("the poll function runs once");
        Poll::Ready(future.poll(context))
    })
    .await
}

/// The ceilings every observation of one host is checked against.
#[derive(Debug, Clone, Copy)]
struct HostBounds {
    peers: usize,
    pending_jobs: usize,
}

impl HostBounds {
    /// The documented node-wide admissions of one direction at the standard node capacity.
    fn admissions(subquota: RequestSubquota) -> usize {
        match subquota {
            RequestSubquota::Shared => TransportOptions::default().incoming_queue_capacity,
            RequestSubquota::Liveness => MAX_CONCURRENT_HEALTH_PROBES,
            RequestSubquota::Cancellation | RequestSubquota::Terminal => 4,
            RequestSubquota::Append
            | RequestSubquota::Resource
            | RequestSubquota::Snapshot
            | RequestSubquota::Discovery
            | RequestSubquota::Progress
            | RequestSubquota::Admission => 8,
        }
    }

    /// No pool, stream partition, admission quota, worker queue or memory budget exceeds its
    /// configured bound.
    fn check(self, host: &str, observation: &Observation) {
        for class in PoolClass::ALL {
            let connection_limit = self
                .peers
                .checked_mul(class.connections_per_peer())
                .assured("fixture peer counts are small");
            for direction in [ConnectionDirection::Outbound, ConnectionDirection::Inbound] {
                let connections = observation.connections(direction, class);
                assert!(
                    connections <= connection_limit,
                    "{host} holds {connections} {direction:?} {class:?} connections for {} peers: \
                     {observation:?}",
                    self.peers
                );
            }
            let slot_limit = observation
                .connections(ConnectionDirection::Outbound, class)
                .checked_mul(class.stream_slots_per_connection())
                .assured("fixture connection counts are small");
            assert!(
                observation.leased(class) <= slot_limit,
                "{host} leased more {class:?} streams than its connections carry: {observation:?}"
            );
        }
        for direction in [ConnectionDirection::Outbound, ConnectionDirection::Inbound] {
            for subquota in RequestSubquota::iter() {
                let pending = observation.pending(direction, subquota);
                assert!(
                    pending <= Self::admissions(subquota),
                    "{host} admitted {pending} {direction:?} {subquota:?} requests: \
                     {observation:?}"
                );
            }
        }
        let executor = &observation.executor;
        for workers in [
            executor.control_cpu,
            executor.data_cpu,
            executor.bulk_cpu,
            executor.consensus_storage,
            executor.filesystem_storage,
        ] {
            assert!(
                workers.running <= workers.workers,
                "{host}: {observation:?}"
            );
            assert!(
                workers.pending <= self.pending_jobs,
                "{host}: {observation:?}"
            );
        }
        for budget in [
            executor.management_memory,
            executor.commands_memory,
            executor.relay_memory,
            executor.bulk_memory,
        ] {
            assert!(
                budget.reserved_bytes <= budget.capacity_bytes,
                "{host}: {observation:?}"
            );
        }
    }
}

/// One read of everything a host's transport and executor hold.
#[derive(Debug)]
struct Observation {
    transport: TransportSnapshot,
    executor: ExecutorSnapshot,
}

impl Observation {
    fn read(transport: &Transport, executor: &Executor) -> Self {
        Self {
            transport: transport.snapshot(),
            executor: executor.snapshot(),
        }
    }

    fn connections(&self, direction: ConnectionDirection, class: PoolClass) -> usize {
        self.transport.connections[direction.index()][class.index()]
    }

    fn connections_by_class(&self, direction: ConnectionDirection) -> [usize; PoolClass::COUNT] {
        self.transport.connections[direction.index()]
    }

    fn leased(&self, class: PoolClass) -> usize {
        self.transport.leased_streams[class.index()]
    }

    fn pending(&self, direction: ConnectionDirection, subquota: RequestSubquota) -> usize {
        self.transport.pending_operations[direction.index()][subquota.index()]
    }

    fn bulk_sent(&self) -> u64 {
        self.transport.counters.bulk_bytes[PoolClass::Bulk.index()][TransferDirection::Sent.index()]
    }

    /// Nothing is leased, admitted, queued, running or charged.
    fn assert_released(&self, host: &str) {
        assert!(
            self.transport
                .leased_streams
                .iter()
                .all(|leased| *leased == 0),
            "{host}: {self:?}"
        );
        assert!(
            self.transport
                .pending_operations
                .iter()
                .flatten()
                .all(|pending| *pending == 0),
            "{host}: {self:?}"
        );
        assert_eq!(self.transport.relay_attempts, 0, "{host}: {self:?}");
        assert_eq!(self.transport.relay_grants, 0, "{host}: {self:?}");
        let executor = &self.executor;
        for workers in [executor.control_cpu, executor.data_cpu, executor.bulk_cpu] {
            assert_eq!(workers.running, 0, "{host}: {self:?}");
            assert_eq!(workers.pending, 0, "{host}: {self:?}");
        }
        for budget in [
            executor.management_memory,
            executor.commands_memory,
            executor.relay_memory,
            executor.bulk_memory,
        ] {
            assert_eq!(budget.reserved_bytes, 0, "{host}: {self:?}");
        }
    }
}

/// The pools one peer holds to another once every preconnected class is ready: management,
/// commands, replication and relay open with the peer, bulk only once it is leased.
fn peer_pools(with_bulk: bool) -> [usize; PoolClass::COUNT] {
    let mut pools = [0; PoolClass::COUNT];
    for class in PoolClass::ALL {
        if class == PoolClass::Bulk && !with_bulk {
            continue;
        }
        pools[class.index()] = class.connections_per_peer();
    }
    pools
}

fn add_pools(
    left: [usize; PoolClass::COUNT],
    right: [usize; PoolClass::COUNT],
) -> [usize; PoolClass::COUNT] {
    std::array::from_fn(|index| {
        left[index]
            .checked_add(right[index])
            .assured("fixture pool counts are small")
    })
}

/// What the hub asks a peer host to do next, and where the answer goes.
#[derive(Debug)]
enum PeerCommand {
    /// Register the hub and wait until every preconnected pool to it is ready.
    Connect(oneshot::Sender<()>),
    /// Exchange shared management work, liveness, a typed Arrow batch and an admitted relay batch.
    Exchange(oneshot::Sender<()>),
    /// Read one complete resource stream.
    ReadStream(oneshot::Sender<u64>),
    /// Open one resource stream and leave it unread.
    StallStream(oneshot::Sender<()>),
    /// Open one resource stream beyond the connection's reserved streams, and report its wait.
    OverflowResourceSlots(oneshot::Sender<Duration>),
    /// Read what the unread streams let through before their reset, then drop them.
    ReleaseStreams(oneshot::Sender<u64>),
    /// Send one relay batch the hub holds without admitting.
    SendHeldRelay(oneshot::Sender<RelayDelivery>),
    CancelRelay {
        delivery: RelayDelivery,
        reply: oneshot::Sender<RelayAdmissionStatus>,
    },
    /// Shut the transport down and report how long that took.
    Shutdown(oneshot::Sender<Duration>),
}

/// The hub's end of one peer host's command channel.
struct PeerLink {
    node: ClusterNodeName,
    commands: mpsc::Sender<PeerCommand>,
}

impl PeerLink {
    async fn call<T>(&self, command: impl FnOnce(oneshot::Sender<T>) -> PeerCommand) -> T {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .assured("peer hosts serve commands until the hub drops its link");
        answer
            .await
            .assured("peer hosts answer every command they accept")
    }
}

/// One peer host's transport and the work it holds between commands.
struct PeerHost {
    name: &'static str,
    transport: Transport,
    incoming: mpsc::Receiver<ReceivedEnvelope>,
    executor: Executor,
    hub: ClusterNodeName,
    trace: SemanticTrace,
    streams: Vec<IncomingByteStream>,
    next_delivery: RelayDelivery,
    next_ack_id: u64,
}

impl PeerHost {
    const BOUNDS: HostBounds = HostBounds {
        peers: 1,
        pending_jobs: 1024,
    };

    async fn serve(mut self, mut commands: mpsc::Receiver<PeerCommand>) -> Result<(), io::Error> {
        while let Some(command) = commands.recv().await {
            tokio::task::consume_budget().await;
            match command {
                PeerCommand::Connect(reply) => {
                    self.connect().await;
                    Self::answer(reply, ());
                }
                PeerCommand::Exchange(reply) => {
                    self.exchange().await;
                    Self::answer(reply, ());
                }
                PeerCommand::ReadStream(reply) => {
                    let bytes = self.read_stream().await;
                    Self::answer(reply, bytes);
                }
                PeerCommand::StallStream(reply) => {
                    self.stall_stream().await;
                    Self::answer(reply, ());
                }
                PeerCommand::OverflowResourceSlots(reply) => {
                    let waited = self.overflow_resource_slots().await;
                    Self::answer(reply, waited);
                }
                PeerCommand::ReleaseStreams(reply) => {
                    let received = self.release_streams().await;
                    Self::answer(reply, received);
                }
                PeerCommand::SendHeldRelay(reply) => {
                    let delivery = self.send_held_relay().await;
                    Self::answer(reply, delivery);
                }
                PeerCommand::CancelRelay { delivery, reply } => {
                    let status = self
                        .transport
                        .cancel_relay(&self.hub, delivery)
                        .await
                        .assured("the hub answers relay cancellation");
                    self.trace.record(
                        self.name,
                        format!("cancelled relay {delivery:?}: {status:?}"),
                    );
                    Self::answer(reply, status);
                }
                PeerCommand::Shutdown(reply) => {
                    let took = self.shut_down().await;
                    Self::answer(reply, took);
                }
            }
        }
        Ok(())
    }

    fn answer<T: std::fmt::Debug>(reply: oneshot::Sender<T>, value: T) {
        reply
            .send(value)
            .assured("the hub awaits every command answer");
    }

    fn observe(&self) -> Observation {
        let observation = Observation::read(&self.transport, &self.executor);
        Self::BOUNDS.check(self.name, &observation);
        observation
    }

    async fn connect(&mut self) {
        register_peer(&self.transport, HUB).await;
        wait_for_connection(&self.transport, &self.hub).await;
        self.observe();
        self.trace.record(self.name, "pools to the hub ready");
    }

    async fn exchange(&mut self) {
        let probe = self
            .transport
            .request(&self.hub, ManagementProbe)
            .await
            .assured("the hub answers shared management work");
        assert_eq!(probe.peer, *self.transport.node_id());
        assert_liveness(&self.transport, &self.hub).await;
        let batch = self
            .transport
            .request(&self.hub, BatchRequest { ipc: arrow_batch() })
            .await
            .assured("the hub answers a typed Arrow batch on the commands pool");
        assert_eq!(batch.rows, 3);
        let payload = self.next_relay();
        let delivery = payload.delivery;
        let ack_id = payload
            .admission
            .as_ref()
            .assured("fixture relays register an admission")
            .ack_id;
        self.transport
            .send(&self.hub, Envelope::RelayPayload(payload))
            .await
            .assured("the hub grants and receives the relay body");
        let resolved = tokio::time::timeout(HOST_DEADLINE, async {
            loop {
                let received = self
                    .incoming
                    .recv()
                    .await
                    .assured("the peer's incoming queue stays open");
                let Envelope::Ack(RemoteAckResolution {
                    ack_id: resolved,
                    outcome,
                }) = received.envelope
                else {
                    panic!("only relay outcomes reach a peer's application queue");
                };
                assert_eq!(resolved, ack_id);
                // The hub reports progress while admission is unresolved; it changes no outcome.
                if let RemoteAckOutcome::Alive = outcome {
                    continue;
                }
                return outcome;
            }
        })
        .await
        .assured("the hub admits the relay within the simulated deadline");
        assert!(matches!(resolved, RemoteAckOutcome::Ack), "{resolved:?}");
        self.observe();
        self.trace.record(
            self.name,
            format!("management, liveness, commands and relay {delivery:?} completed"),
        );
    }

    async fn read_stream(&mut self) -> u64 {
        let mut stream = self
            .transport
            .request_stream(&self.hub, ResourceStream)
            .await
            .assured("the hub opens a resource stream for the healthy peer");
        let mut received = 0_u64;
        while let Some(chunk) = stream
            .next_chunk()
            .await
            .assured("the healthy peer reads the whole stream")
        {
            tokio::task::consume_budget().await;
            let length = u64::try_from(chunk.len()).assured("chunk lengths fit in u64");
            received = received
                .checked_add(length)
                .assured("the stream is bounded by its declared length");
        }
        drop(stream);
        let observation = self.observe();
        assert_eq!(observation.leased(PoolClass::Bulk), 0);
        self.trace
            .record(self.name, format!("read {received} streamed bytes"));
        received
    }

    async fn stall_stream(&mut self) {
        let stream = self
            .transport
            .request_stream(&self.hub, ResourceStream)
            .await
            .assured("the hub opens a resource stream");
        assert_eq!(stream.content_length(), STREAM_BYTES);
        self.streams.push(stream);
        let observation = self.observe();
        assert_eq!(observation.leased(PoolClass::Bulk), self.streams.len());
        assert_eq!(
            observation.pending(ConnectionDirection::Outbound, RequestSubquota::Resource),
            self.streams.len()
        );
        self.trace.record(
            self.name,
            format!(
                "resource stream {} opened and left unread",
                self.streams.len()
            ),
        );
    }

    async fn overflow_resource_slots(&mut self) -> Duration {
        let started = turmoil::elapsed();
        let error = match self
            .transport
            .request_stream(&self.hub, ResourceStream)
            .await
        {
            Ok(_) => panic!("both reserved resource streams are leased"),
            Err(error) => error,
        };
        assert!(
            matches!(error.current_context(), RequestError::Stream { .. }),
            "{error:?}"
        );
        let waited = turmoil::elapsed()
            .checked_sub(started)
            .assured("simulated time does not run backwards");
        let observation = self.observe();
        assert_eq!(observation.leased(PoolClass::Bulk), self.streams.len());
        assert_eq!(
            observation.pending(ConnectionDirection::Outbound, RequestSubquota::Resource),
            self.streams.len()
        );
        self.trace.record(
            self.name,
            format!("a third resource stream waited {waited:?} for a reserved stream"),
        );
        waited
    }

    async fn release_streams(&mut self) -> u64 {
        let mut received = 0_u64;
        for mut stream in self.streams.drain(..) {
            loop {
                tokio::task::consume_budget().await;
                match stream.next_chunk().await {
                    Ok(Some(chunk)) => {
                        let length = u64::try_from(chunk.len()).assured("chunk lengths fit in u64");
                        received = received
                            .checked_add(length)
                            .assured("streams are bounded by their declared length");
                    }
                    Ok(None) => panic!("a stream the hub reset cannot complete"),
                    Err(_) => break,
                }
            }
        }
        let observation = self.observe();
        assert_eq!(observation.leased(PoolClass::Bulk), 0);
        assert_eq!(
            observation.pending(ConnectionDirection::Outbound, RequestSubquota::Resource),
            0
        );
        assert_eq!(observation.executor.bulk_memory.reserved_bytes, 0);
        self.trace.record(
            self.name,
            format!("unread streams ended after {received} bytes; leases returned"),
        );
        received
    }

    async fn send_held_relay(&mut self) -> RelayDelivery {
        let payload = self.next_relay();
        let delivery = payload.delivery;
        self.transport
            .send(&self.hub, Envelope::RelayPayload(payload))
            .await
            .assured("the hub grants and receives the relay body");
        self.trace.record(
            self.name,
            format!("relay {delivery:?} body received by the hub"),
        );
        delivery
    }

    fn next_relay(&mut self) -> RelayPayload {
        let delivery = self.next_delivery;
        self.next_delivery.sequence = delivery
            .sequence
            .checked_add(1)
            .assured("the fixture sends a handful of batches per channel");
        let ack_id = self.next_ack_id;
        self.next_ack_id = ack_id
            .checked_add(1)
            .assured("the fixture registers a handful of acknowledgements");
        let batch_ipc = self
            .executor
            .try_charge_owned(MemoryClass::Relay, arrow_batch())
            .assured("the Arrow fixture fits the relay memory budget");
        RelayPayload {
            delivery,
            kind: RelayPayloadKind::Routed,
            domain: DomainName::parse("simulated").assured("fixture domain is valid"),
            relay: RelayName::parse("records").assured("fixture relay is valid"),
            key: None,
            batch_ipc,
            metadata: Vec::new(),
            acks: Vec::new(),
            admission: Some(RemoteAckRegistration {
                ack_id,
                reply_node_id: self.transport.node_id().clone(),
            }),
        }
    }

    async fn shut_down(&mut self) -> Duration {
        let started = turmoil::elapsed();
        self.transport.shutdown().await;
        let took = turmoil::elapsed()
            .checked_sub(started)
            .assured("simulated time does not run backwards");
        while self.incoming.try_recv().is_ok() {
            tokio::task::consume_budget().await;
        }
        let observation = self.observe();
        observation.assert_released(self.name);
        assert!(
            observation
                .transport
                .connections
                .iter()
                .flatten()
                .all(|connections| *connections == 0),
            "{}: {observation:?}",
            self.name
        );
        self.trace
            .record(self.name, format!("transport shut down after {took:?}"));
        took
    }
}

/// The hub's application side of relay delivery: it admits a batch and sends its terminal outcome,
/// or holds it unadmitted.
struct RelayIngress {
    incoming: mpsc::Receiver<ReceivedEnvelope>,
    transport: Transport,
}

impl RelayIngress {
    async fn next_from(&mut self, peer: &ClusterNodeName) -> ReceivedEnvelope {
        let received = tokio::time::timeout(HOST_DEADLINE, self.incoming.recv())
            .await
            .assured("the relay reaches the hub within the simulated deadline")
            .assured("the hub's incoming queue stays open");
        assert_eq!(&received.peer_node_id, peer);
        let Envelope::RelayPayload(ref body) = received.envelope else {
            panic!("only relay batches reach the hub's application queue");
        };
        assert_eq!(decode_arrow(&body.batch_ipc), 3);
        received
    }

    async fn admit_next(&mut self, peer: &ClusterNodeName) -> RelayDelivery {
        let received = self.next_from(peer).await;
        let Envelope::RelayPayload(ref body) = received.envelope else {
            panic!("next_from returns relay batches only");
        };
        let delivery = body.delivery;
        let ack_id = body
            .admission
            .as_ref()
            .assured("fixture relays register an admission")
            .ack_id;
        assert_eq!(
            received
                .relay_admission
                .as_ref()
                .assured("a received relay carries its admission")
                .admit(),
            RelayAdmissionDecision::Admitted
        );
        drop(received);
        self.transport
            .send(
                peer,
                Envelope::Ack(RemoteAckResolution {
                    ack_id,
                    outcome: RemoteAckOutcome::Ack,
                }),
            )
            .await
            .assured("the terminal outcome reaches the sender over reserved capacity");
        delivery
    }
}

/// How many handlers of the never-answered operation the stalled peer started and dropped.
struct StalledHandlers {
    started: watch::Receiver<usize>,
    dropped: watch::Receiver<usize>,
}

/// Counts one dropped handler of the never-answered operation.
struct HandlerDropProbe {
    dropped: watch::Sender<usize>,
}

impl Drop for HandlerDropProbe {
    fn drop(&mut self) {
        self.dropped.send_modify(|count| {
            *count = count
                .checked_add(1)
                .assured("the hub sends a bounded number of operations");
        });
    }
}

/// The hub: the node whose capacity the plan observes, and the host that drives both peers.
struct HubHost {
    transport: Transport,
    ingress: RelayIngress,
    executor: Executor,
    trace: SemanticTrace,
    stalled: PeerLink,
    healthy: PeerLink,
    handlers: StalledHandlers,
    held: Vec<JoinHandle<Result<StalledOperationResponse, Report<RequestError>>>>,
}

impl HubHost {
    const BOUNDS: HostBounds = HostBounds {
        peers: 2,
        pending_jobs: HUB_PENDING_JOBS,
    };

    async fn run(mut self) -> Result<(), io::Error> {
        self.connect().await;
        self.saturate_stalled_management().await;
        self.saturate_bulk_execution().await;
        self.stall_bulk_flow_control().await;
        self.isolate_relay_channels().await;
        self.hold_stalled_link().await;
        self.retire_stalled_peer().await;
        self.tear_down_stalled_host().await;
        self.release_stalled_link().await;
        self.finish().await;
        Ok(())
    }

    fn observe(&self) -> Observation {
        let observation = Observation::read(&self.transport, &self.executor);
        Self::BOUNDS.check(HUB, &observation);
        observation
    }

    /// Read the hub every tick until `condition` holds; every read is checked against the bounds.
    async fn wait_until(
        &self,
        what: &str,
        condition: impl Fn(&Observation) -> bool,
    ) -> Observation {
        let waited = tokio::time::timeout(HOST_DEADLINE, async {
            loop {
                tokio::task::consume_budget().await;
                let observation = self.observe();
                if condition(&observation) {
                    return observation;
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await;
        match waited {
            Ok(observation) => observation,
            Err(_) => panic!("the hub never observed {what}: {:?}", self.observe()),
        }
    }

    /// The same wait, also returning the simulated time the condition was first observed.
    async fn wait_until_at(
        &self,
        what: &str,
        condition: impl Fn(&Observation) -> bool,
    ) -> (Observation, Duration) {
        let observation = self.wait_until(what, condition).await;
        (observation, turmoil::elapsed())
    }

    fn since(started: Duration) -> Duration {
        turmoil::elapsed()
            .checked_sub(started)
            .assured("simulated time does not run backwards")
    }

    async fn exchange_with_healthy(&mut self) {
        let Self {
            ingress,
            healthy,
            trace,
            ..
        } = self;
        let ((), delivery) = tokio::join!(
            healthy.call(PeerCommand::Exchange),
            ingress.admit_next(&healthy.node)
        );
        trace.record(
            HUB,
            format!("admitted the healthy peer's relay {delivery:?}"),
        );
    }

    async fn connect(&mut self) {
        for peer in [STALLED, HEALTHY] {
            register_peer(&self.transport, peer).await;
        }
        tokio::join!(
            self.stalled.call(PeerCommand::Connect),
            self.healthy.call(PeerCommand::Connect)
        );
        wait_for_connection(&self.transport, &self.stalled.node).await;
        wait_for_connection(&self.transport, &self.healthy.node).await;
        let ready = self.observe();
        ready.assert_released(HUB);
        let expected = add_pools(peer_pools(false), peer_pools(false));
        assert_eq!(
            ready.connections_by_class(ConnectionDirection::Outbound),
            expected
        );
        self.trace.record(HUB, "pools ready to both peers");
    }

    /// A slow peer holds every shared management stream the hub's connection to it has. The
    /// reserved streams on that same connection and every pool to the other peer stay usable.
    async fn saturate_stalled_management(&mut self) {
        for sequence in 1..=MANAGEMENT_SHARED_STREAMS {
            tokio::task::consume_budget().await;
            let transport = self.transport.clone();
            let peer = self.stalled.node.clone();
            self.held.push(tokio::spawn(async move {
                transport.request(&peer, StalledOperation).await
            }));
            wait_for_count(&mut self.handlers.started, sequence).await;
        }
        let saturated = self.observe();
        assert_eq!(
            saturated.leased(PoolClass::Management),
            MANAGEMENT_SHARED_STREAMS
        );
        assert_eq!(
            saturated.pending(ConnectionDirection::Outbound, RequestSubquota::Shared),
            MANAGEMENT_SHARED_STREAMS
        );
        self.trace.record(
            HUB,
            format!(
                "{MANAGEMENT_SHARED_STREAMS} shared management streams held by the stalled peer"
            ),
        );

        let started = turmoil::elapsed();
        let error = match self
            .transport
            .request_with_timeout(&self.stalled.node, StalledOperation, SLOT_WAIT)
            .await
        {
            Ok(_) => panic!("the stalled peer never answers"),
            Err(error) => error,
        };
        assert!(
            matches!(error.current_context(), RequestError::Timeout { timeout, .. } if *timeout == SLOT_WAIT),
            "{error:?}"
        );
        let waited = Self::since(started);
        assert!(waited >= SLOT_WAIT, "{waited:?}");
        assert_eq!(
            *self.handlers.started.borrow(),
            MANAGEMENT_SHARED_STREAMS,
            "an operation waiting for a stream slot never reaches the peer"
        );
        let after_wait = self.observe();
        assert_eq!(
            after_wait.leased(PoolClass::Management),
            MANAGEMENT_SHARED_STREAMS
        );
        assert_eq!(
            after_wait.pending(ConnectionDirection::Outbound, RequestSubquota::Shared),
            MANAGEMENT_SHARED_STREAMS
        );
        self.trace.record(
            HUB,
            format!("one more shared operation waited {waited:?} for a stream and expired"),
        );

        assert_liveness(&self.transport, &self.stalled.node).await;
        assert_eq!(
            self.transport
                .request(&self.stalled.node, CancellationProbe)
                .await
                .assured("reserved cancellation streams remain free"),
            CancellationProbeResponse
        );
        assert_eq!(
            self.transport
                .request(&self.stalled.node, DiscoveryProbe)
                .await
                .assured("reserved discovery streams remain free"),
            DiscoveryProbeResponse
        );
        self.trace.record(
            HUB,
            "liveness, cancellation and discovery reached the stalled peer on reserved streams",
        );

        let probe = self
            .transport
            .request(&self.healthy.node, ManagementProbe)
            .await
            .assured("the healthy peer's management pool is separate");
        assert_eq!(probe.peer, *self.transport.node_id());
        self.exchange_with_healthy().await;
        let isolated = self.observe();
        assert_eq!(
            isolated.leased(PoolClass::Management),
            MANAGEMENT_SHARED_STREAMS
        );
        self.trace
            .record(HUB, "the healthy peer's pools served the hub and the peer");
    }

    /// One burst fills the bulk worker's wait queue. The burst beyond it is refused at once and
    /// its reservations are never taken, while a control job in the same pass starts at once.
    async fn saturate_bulk_execution(&mut self) {
        let before = self.observe();
        let mut single = Box::pin(self.transport.request(&self.healthy.node, BulkProbe));
        assert!(poll_once(single.as_mut()).await.is_pending());
        let one = self.observe();
        assert_eq!(one.executor.bulk_cpu.running, 1);
        assert_eq!(one.executor.bulk_cpu.pending, 0);
        let charge = one
            .executor
            .bulk_memory
            .reserved_bytes
            .checked_sub(before.executor.bulk_memory.reserved_bytes)
            .assured("an admitted encode adds its charge");
        assert!(charge > 0, "{one:?}");
        single
            .await
            .assured("an uncontended bulk request completes");

        let admitted = HUB_PENDING_JOBS
            .checked_add(1)
            .assured("one worker beside the queue");
        let burst_size = admitted
            .checked_add(REFUSED_JOBS)
            .assured("the burst is small");
        let requests =
            (0..burst_size).map(|_| self.transport.request(&self.healthy.node, BulkProbe));
        let mut burst = Box::pin(join_all(requests));
        let mut control = Box::pin(self.transport.request(&self.healthy.node, LivenessRequest));
        assert!(poll_once(burst.as_mut()).await.is_pending());
        assert!(poll_once(control.as_mut()).await.is_pending());
        let saturated = self.observe();
        let bulk = saturated.executor.bulk_cpu;
        assert_eq!(bulk.running, 1, "{saturated:?}");
        assert_eq!(bulk.pending, HUB_PENDING_JOBS, "{saturated:?}");
        let refused = bulk
            .refused
            .checked_sub(before.executor.bulk_cpu.refused)
            .assured("the refusal counter only grows");
        assert_eq!(
            refused,
            u64::try_from(REFUSED_JOBS).assured("the burst is small")
        );
        let admitted_charge = charge
            .checked_mul(u64::try_from(admitted).assured("the burst is small"))
            .assured("the burst's charges are small");
        assert_eq!(
            saturated.executor.bulk_memory.reserved_bytes, admitted_charge,
            "a refused job holds no charge: {saturated:?}"
        );
        assert_eq!(saturated.executor.control_cpu.running, 1, "{saturated:?}");
        assert_eq!(saturated.executor.control_cpu.pending, 0, "{saturated:?}");
        self.trace.record(
            HUB,
            format!(
                "bulk worker busy with {HUB_PENDING_JOBS} queued and {REFUSED_JOBS} refused; \
                 control job started"
            ),
        );

        let (results, liveness) = tokio::join!(burst, control);
        let liveness = liveness.assured("control work runs beside a saturated bulk class");
        assert_eq!(liveness.peer, *self.transport.node_id());
        // An admitted request runs further bulk jobs after its first encode, and each of them is
        // admitted against the same queue the rest of the burst still fills. Every failure is
        // therefore one refusal by that queue: the requests beyond it fail their first encode,
        // and an admitted request fails at the later stage the full queue refused.
        let mut answered = 0_usize;
        let mut failed = 0_usize;
        for (index, result) in results.iter().enumerate() {
            let Err(error) = result else {
                answered = answered.checked_add(1).assured("the burst is small");
                continue;
            };
            failed = failed.checked_add(1).assured("the burst is small");
            let context = error.current_context();
            if index < admitted {
                assert!(
                    matches!(context, RequestError::Transport { .. }),
                    "admitted bulk request {index}: {error:?}"
                );
            } else {
                assert!(
                    matches!(context, RequestError::Encode { .. }),
                    "bulk request {index} beyond the full queue: {error:?}"
                );
            }
        }
        let bulk_before = before.executor.bulk_memory.reserved_bytes;
        let drained = self
            .wait_until("the bulk class drained", |observation| {
                let bulk = observation.executor.bulk_cpu;
                bulk.running == 0
                    && bulk.pending == 0
                    && observation.executor.bulk_memory.reserved_bytes == bulk_before
            })
            .await;
        let refused_total = drained
            .executor
            .bulk_cpu
            .refused
            .checked_sub(before.executor.bulk_cpu.refused)
            .assured("the refusal counter only grows");
        assert_eq!(
            refused_total,
            u64::try_from(failed).assured("the burst is small"),
            "every failed bulk request was refused by the full queue exactly once"
        );
        assert!(answered > 0, "the admitted head of the burst is answered");
        self.trace.record(
            HUB,
            format!(
                "{answered} bulk requests answered, {failed} refused by the full queue; drained"
            ),
        );
    }

    /// The stalled peer opens resource streams and never reads them. HTTP/2 flow control stops
    /// the hub after one stream window each, the progress deadline returns their admissions and
    /// memory, and the healthy peer's stream is unaffected throughout.
    async fn stall_bulk_flow_control(&mut self) {
        let options = TransportOptions::default();
        let window = u64::from(options.initial_stream_window_bytes);
        let before = self.observe();
        let sent_before = before.bulk_sent();
        let bulk_before = before.executor.bulk_memory.reserved_bytes;
        let sent_after = |streams: u64| {
            let windows = window
                .checked_mul(streams)
                .assured("fixture stream windows are small");
            windows
                .checked_add(sent_before)
                .assured("fixture byte counters are small")
        };

        // The hub is read every tick from before each stream opens, so the moment its window runs
        // out, and with it the start of the progress deadline, is observed within one tick.
        let ((), (first, first_stalled_at)) = tokio::join!(
            self.stalled.call(PeerCommand::StallStream),
            self.wait_until_at(
                "the first unread stream to fill its window",
                |observation| {
                    observation.bulk_sent() == sent_after(1)
                        && observation
                            .pending(ConnectionDirection::Inbound, RequestSubquota::Resource)
                            == 1
                }
            )
        );
        let per_stream = first
            .executor
            .bulk_memory
            .reserved_bytes
            .checked_sub(bulk_before)
            .assured("the stalled stream adds its charge");
        let chunk = u64::try_from(STREAM_CHUNK_BYTES).assured("the chunk size fits in u64");
        assert!(
            per_stream >= chunk,
            "the hub holds the chunk the window refused: {first:?}"
        );

        let ((), (second, second_stalled_at)) = tokio::join!(
            self.stalled.call(PeerCommand::StallStream),
            self.wait_until_at(
                "the second unread stream to fill its window",
                |observation| {
                    observation.bulk_sent() == sent_after(2)
                        && observation
                            .pending(ConnectionDirection::Inbound, RequestSubquota::Resource)
                            == 2
                }
            )
        );
        let both_streams = per_stream
            .checked_mul(2)
            .assured("two stream charges are small");
        assert_eq!(
            second.executor.bulk_memory.reserved_bytes,
            both_streams
                .checked_add(bulk_before)
                .assured("two stream charges are small"),
            "{second:?}"
        );
        self.trace.record(
            HUB,
            format!("two unread resource streams stopped at {window} bytes each"),
        );

        let waited = self.stalled.call(PeerCommand::OverflowResourceSlots).await;
        assert!(waited >= ResourceStream::TIMEOUT, "{waited:?}");
        let read = self.healthy.call(PeerCommand::ReadStream).await;
        assert_eq!(read, STREAM_BYTES);
        let during = self.observe();
        assert_eq!(
            during.bulk_sent(),
            sent_after(2)
                .checked_add(STREAM_BYTES)
                .assured("fixture streams are small"),
            "the stalled streams sent nothing beyond their windows: {during:?}"
        );
        assert_eq!(
            during.pending(ConnectionDirection::Inbound, RequestSubquota::Resource),
            2
        );
        self.trace.record(
            HUB,
            "the healthy peer read a whole stream while both stalled streams were held",
        );

        self.wait_until(
            "the first stalled stream's progress deadline",
            |observation| {
                observation.pending(ConnectionDirection::Inbound, RequestSubquota::Resource) < 2
            },
        )
        .await;
        let first_held = Self::since(first_stalled_at);
        self.wait_until(
            "the second stalled stream's progress deadline",
            |observation| {
                observation.pending(ConnectionDirection::Inbound, RequestSubquota::Resource) == 0
                    && observation.executor.bulk_memory.reserved_bytes == bulk_before
            },
        )
        .await;
        let second_held = Self::since(second_stalled_at);
        for held in [first_held, second_held] {
            assert!(
                held.abs_diff(options.progress_timeout) <= DEADLINE_SLACK,
                "a stalled stream returned its admission after {held:?}"
            );
        }
        self.trace.record(
            HUB,
            format!(
                "stalled streams released their admission and memory at the {:?} progress deadline",
                options.progress_timeout
            ),
        );

        let received = self.stalled.call(PeerCommand::ReleaseStreams).await;
        assert_eq!(
            received,
            window
                .checked_mul(2)
                .assured("two stream windows are small"),
            "the stalled peer receives exactly what its windows let through before the resets"
        );
    }

    /// A relay batch the hub holds unadmitted keeps its reservation. Another peer's channel still
    /// receives grants and admission, and cancelling the held batch returns every byte.
    async fn isolate_relay_channels(&mut self) {
        let limits = *self.executor.limits();
        let overlap = limits
            .relay_decoded_bytes
            .as_u64()
            .checked_add(limits.relay_scratch_bytes.as_u64())
            .assured("default relay limits are addressable");
        let (delivery, held) = {
            let Self {
                ingress, stalled, ..
            } = self;
            tokio::join!(
                stalled.call(PeerCommand::SendHeldRelay),
                ingress.next_from(&stalled.node)
            )
        };
        let Envelope::RelayPayload(ref body) = held.envelope else {
            panic!("next_from returns relay batches only");
        };
        assert_eq!(body.delivery, delivery);
        let reservation = u64::try_from(body.batch_ipc.len())
            .assured("the fixture batch length fits in u64")
            .checked_add(overlap)
            .assured("one relay operation is addressable");
        let reserved = self.observe();
        assert_eq!(
            reserved.executor.relay_memory.reserved_bytes, reservation,
            "the grant reserved the exact body, the largest decoded batch and scratch: \
             {reserved:?}"
        );
        assert_eq!(reserved.transport.relay_attempts, 1);
        self.trace.record(
            HUB,
            format!(
                "holding the stalled peer's relay {delivery:?} with {reservation} bytes reserved"
            ),
        );

        self.exchange_with_healthy().await;
        self.wait_until("the healthy relay retired", |observation| {
            observation.transport.relay_attempts == 1
                && observation.executor.relay_memory.reserved_bytes == reservation
        })
        .await;

        let status = self
            .stalled
            .call(|reply| PeerCommand::CancelRelay { delivery, reply })
            .await;
        assert_eq!(status, RelayAdmissionStatus::Cancelled);
        assert_eq!(
            held.relay_admission
                .as_ref()
                .assured("a received relay carries its admission")
                .admit(),
            RelayAdmissionDecision::Cancelled
        );
        drop(held);
        self.wait_until(
            "the cancelled relay returned its reservation",
            |observation| {
                observation.transport.relay_attempts == 0
                    && observation.transport.relay_grants == 0
                    && observation.executor.relay_memory.reserved_bytes == 0
            },
        )
        .await;
        self.trace.record(
            HUB,
            "cancellation fenced the held relay and returned its reservation",
        );
    }

    /// With the stalled link held, the hub's own deadline ends its probe, and the healthy peer
    /// keeps exchanging every traffic class.
    async fn hold_stalled_link(&mut self) {
        turmoil::hold(HUB, STALLED);
        self.trace.record(HUB, "link to the stalled peer held");
        assert_liveness_timeout(&self.transport, &self.stalled.node).await;
        let expired = self.observe();
        assert_eq!(
            expired.pending(ConnectionDirection::Outbound, RequestSubquota::Liveness),
            0
        );
        assert_eq!(
            expired.leased(PoolClass::Management),
            MANAGEMENT_SHARED_STREAMS
        );
        self.exchange_with_healthy().await;
        self.trace.record(
            HUB,
            "the healthy peer stayed live while the stalled link was held",
        );
    }

    /// Membership removal ends every operation to the departed peer and closes its pools without
    /// that peer's cooperation.
    async fn retire_stalled_peer(&mut self) {
        let started = turmoil::elapsed();
        self.transport.replace_live_nodes(&live(&[HUB, HEALTHY]));
        for request in self.held.drain(..) {
            tokio::task::consume_budget().await;
            let result = request.await.assured("held request tasks join");
            let Err(error) = result else {
                panic!("the stalled peer never answers");
            };
            assert!(
                matches!(error.current_context(), RequestError::TargetLeft { .. }),
                "{error:?}"
            );
        }
        let ended = Self::since(started);
        assert!(
            ended.is_zero(),
            "membership removal waited {ended:?} for the departed peer"
        );
        let released = self.observe();
        assert_eq!(released.leased(PoolClass::Management), 0, "{released:?}");
        assert_eq!(
            released.pending(ConnectionDirection::Outbound, RequestSubquota::Shared),
            0
        );
        self.trace.record(
            HUB,
            format!(
                "membership removal ended {MANAGEMENT_SHARED_STREAMS} operations after {ended:?}"
            ),
        );
        let healthy_only = peer_pools(true);
        self.wait_until(
            "the departed peer's outbound pools to close",
            |observation| {
                observation.connections_by_class(ConnectionDirection::Outbound) == healthy_only
            },
        )
        .await;
        let closed = Self::since(started);
        assert!(
            closed.is_zero(),
            "the departed peer's pools closed after {closed:?}"
        );
        assert_eq!(
            *self.handlers.dropped.borrow(),
            0,
            "the held link keeps the hub's cancellations from the stalled peer"
        );
        self.trace.record(
            HUB,
            "the departed peer's outbound pools closed; its handlers are still running",
        );
    }

    /// The stalled host tears its transport down while its link is still held. Teardown closes the
    /// connections the hub opened to it at once, with the handlers the hub's operations left behind.
    async fn tear_down_stalled_host(&mut self) {
        let took = self.stalled.call(PeerCommand::Shutdown).await;
        assert!(
            took.is_zero(),
            "the stalled host's teardown waited {took:?}"
        );
        assert_eq!(
            *self.handlers.dropped.borrow(),
            MANAGEMENT_SHARED_STREAMS,
            "teardown drops every handler the stalled peer was running"
        );
        self.trace.record(
            HUB,
            format!(
                "the stalled host's teardown dropped {MANAGEMENT_SHARED_STREAMS} handlers at once"
            ),
        );
    }

    /// Once the held segments are delivered, the torn-down peer's inbound connections close.
    async fn release_stalled_link(&mut self) {
        let started = turmoil::elapsed();
        turmoil::release(HUB, STALLED);
        let healthy_only = peer_pools(true);
        self.wait_until(
            "the torn-down peer's inbound connections to close",
            |observation| {
                observation.connections_by_class(ConnectionDirection::Inbound) == healthy_only
            },
        )
        .await;
        let closed = Self::since(started);
        self.trace.record(
            HUB,
            format!("the torn-down peer's inbound connections closed {closed:?} after release"),
        );
    }

    async fn finish(&mut self) {
        self.exchange_with_healthy().await;
        let took = self.healthy.call(PeerCommand::Shutdown).await;
        assert!(took < TransportOptions::default().shutdown_drain_timeout);
        self.transport.shutdown().await;
        while self.ingress.incoming.try_recv().is_ok() {
            tokio::task::consume_budget().await;
        }
        let stopped = self.observe();
        stopped.assert_released(HUB);
        assert_stopped(&self.transport).await;
        self.trace.record(
            HUB,
            "transport shut down; every permit and reservation returned",
        );
    }
}

/// The stalled host's ends of its handler counters.
#[derive(Clone)]
struct StalledHandlerSignals {
    started: watch::Sender<usize>,
    dropped: watch::Sender<usize>,
}

/// One peer host's part of the plan, taken once when Turmoil first starts the host.
struct PeerPlan {
    name: &'static str,
    credentials: Credentials,
    seed: u64,
    commands: mpsc::Receiver<PeerCommand>,
    handlers: Option<StalledHandlerSignals>,
    delivery: RelayDelivery,
    first_ack_id: u64,
    trace: SemanticTrace,
}

impl PeerPlan {
    async fn start(self) -> Result<(), io::Error> {
        let executor = executor(PeerHost::BOUNDS.pending_jobs);
        let (transport, incoming) =
            bind_host(self.name, self.credentials, self.seed, executor.clone()).await;
        transport
            .register_handler::<LivenessRequest, _, _>(|context, _| async move {
                LivenessResponse {
                    peer: context.peer_node_id().clone(),
                }
            })
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<ManagementProbe, _, _>(|context, _| async move {
                ManagementProbeResponse {
                    peer: context.peer_node_id().clone(),
                }
            })
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<BulkProbe, _, _>(|_, _| async move { BulkProbeResponse })
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<CancellationProbe, _, _>(
                |_, _| async move { CancellationProbeResponse },
            )
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<DiscoveryProbe, _, _>(|_, _| async move { DiscoveryProbeResponse })
            .assured("each handler is registered once per transport");
        if let Some(signals) = self.handlers {
            transport
                .register_handler::<StalledOperation, _, _>(move |_, _| {
                    let started = signals.started.clone();
                    let probe = HandlerDropProbe {
                        dropped: signals.dropped.clone(),
                    };
                    async move {
                        let _probe = probe;
                        started.send_modify(|count| {
                            *count = count
                                .checked_add(1)
                                .assured("the hub sends a bounded number of operations");
                        });
                        std::future::pending::<StalledOperationResponse>().await
                    }
                })
                .assured("each handler is registered once per transport");
        }
        transport.replace_live_nodes(&live(&[HUB, STALLED, HEALTHY]));
        let host = PeerHost {
            name: self.name,
            transport,
            incoming,
            executor,
            hub: node(HUB),
            trace: self.trace,
            streams: Vec::new(),
            next_delivery: self.delivery,
            next_ack_id: self.first_ack_id,
        };
        host.serve(self.commands).await
    }
}

/// The hub host's part of the plan, taken once when Turmoil first starts the host.
struct HubPlan {
    credentials: Credentials,
    seed: u64,
    stalled: mpsc::Sender<PeerCommand>,
    healthy: mpsc::Sender<PeerCommand>,
    handlers: StalledHandlers,
    trace: SemanticTrace,
}

impl HubPlan {
    async fn start(self) -> Result<(), io::Error> {
        let executor = executor(HUB_PENDING_JOBS);
        let (transport, incoming) =
            bind_host(HUB, self.credentials, self.seed, executor.clone()).await;
        transport
            .register_handler::<LivenessRequest, _, _>(|context, _| async move {
                LivenessResponse {
                    peer: context.peer_node_id().clone(),
                }
            })
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<ManagementProbe, _, _>(|context, _| async move {
                ManagementProbeResponse {
                    peer: context.peer_node_id().clone(),
                }
            })
            .assured("each handler is registered once per transport");
        transport
            .register_handler::<BatchRequest, _, _>(|context, request| async move {
                BatchResponse {
                    rows: decode_arrow(&request.ipc),
                    peer: context.peer_node_id().clone(),
                }
            })
            .assured("each handler is registered once per transport");
        let chunks = executor.clone();
        transport
            .register_stream_handler::<ResourceStream, _, _>(move |_, _| {
                let executor = chunks.clone();
                async move {
                    let chunk_bytes =
                        u64::try_from(STREAM_CHUNK_BYTES).assured("the chunk size fits in u64");
                    let count = STREAM_BYTES / chunk_bytes;
                    // Each chunk is charged only when the producer is polled for it, so the hub's
                    // bulk memory shows exactly what the stream holds in flight.
                    let produced = stream::iter(0..count).map(move |_| {
                        executor
                            .try_charge_owned(MemoryClass::Bulk, vec![7_u8; STREAM_CHUNK_BYTES])
                            .map_err(|error| StreamHandlerError::new(error.to_string()))
                    });
                    Ok(StreamingResponse::new(STREAM_BYTES, produced))
                }
            })
            .assured("each handler is registered once per transport");
        transport.replace_live_nodes(&live(&[HUB, STALLED, HEALTHY]));
        let ingress = RelayIngress {
            incoming,
            transport: transport.clone(),
        };
        let hub = HubHost {
            transport,
            ingress,
            executor,
            trace: self.trace,
            stalled: PeerLink {
                node: node(STALLED),
                commands: self.stalled,
            },
            healthy: PeerLink {
                node: node(HEALTHY),
                commands: self.healthy,
            },
            handlers: self.handlers,
            held: Vec::new(),
        };
        hub.run().await
    }
}

/// Start a host whose plan is taken on its first start, and count it finished whatever it returns.
fn start_once<P, F, S>(
    plan: &StdArc<Mutex<Option<P>>>,
    finished: &watch::Sender<usize>,
    start: S,
) -> impl Future<Output = turmoil::Result> + use<P, F, S>
where
    F: Future<Output = Result<(), io::Error>> + Send + 'static,
    S: FnOnce(P) -> F,
{
    let plan = plan
        .lock()
        .take()
        .assured("each host starts once; the plan never bounces a host");
    let running = start(plan);
    let finished = finished.clone();
    async move {
        let result = HostSupervisor::run(running).await;
        finished.send_modify(|count| {
            *count = count.checked_add(1).assured("three fixture hosts finish");
        });
        result
    }
}

fn isolation_config(seed: u64) -> SimulationConfig {
    let mut configuration = config(seed);
    configuration.bounds = SimulationBounds {
        simulated_duration: Duration::from_secs(120),
        tick: POLL,
        max_steps: NonZeroUsize::new(120_000).assured("the fixture step limit is nonzero"),
        wall_duration: Duration::from_secs(120),
    };
    configuration
}

fn stalled_peer_plan(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let trace = run.trace();
    let authority = Authority::new();
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let (stalled_commands, stalled_receiver) = mpsc::channel(1);
    let (healthy_commands, healthy_receiver) = mpsc::channel(1);
    let (started_tx, started_rx) = watch::channel(0_usize);
    let (dropped_tx, dropped_rx) = watch::channel(0_usize);
    let hub = StdArc::new(Mutex::new(Some(HubPlan {
        credentials: authority.issue(HUB),
        seed,
        stalled: stalled_commands,
        healthy: healthy_commands,
        handlers: StalledHandlers {
            started: started_rx,
            dropped: dropped_rx,
        },
        trace: trace.clone(),
    })));
    let peers = [
        PeerPlan {
            name: STALLED,
            credentials: authority.issue(STALLED),
            seed: seed.checked_add(1).assured("fixture seeds are small"),
            commands: stalled_receiver,
            handlers: Some(StalledHandlerSignals {
                started: started_tx,
                dropped: dropped_tx,
            }),
            delivery: RelayDelivery {
                channel_incarnation: [81; 16],
                sequence: 0,
            },
            first_ack_id: 81_000,
            trace: trace.clone(),
        },
        PeerPlan {
            name: HEALTHY,
            credentials: authority.issue(HEALTHY),
            seed: seed.checked_add(2).assured("fixture seeds are small"),
            commands: healthy_receiver,
            handlers: None,
            delivery: RelayDelivery {
                channel_incarnation: [82; 16],
                sequence: 0,
            },
            first_ack_id: 82_000,
            trace: trace.clone(),
        },
    ];
    run.simulate(move |simulation| {
        for peer in peers {
            let name = peer.name;
            let plan = StdArc::new(Mutex::new(Some(peer)));
            let finished = finished_tx.clone();
            simulation.host(name, move || start_once(&plan, &finished, PeerPlan::start));
        }
        let finished = finished_tx.clone();
        simulation.host(HUB, move || start_once(&hub, &finished, HubPlan::start));
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            tokio::time::timeout(Duration::from_secs(110), async {
                while *finished.borrow() < 3 {
                    tokio::task::consume_budget().await;
                    finished
                        .changed()
                        .await
                        .assured("the fixture hosts remain alive");
                }
            })
            .await
            .assured("the stalled-peer plan finishes within its simulated budget");
            Ok(())
        });
    })
}

#[test]
fn stalled_peer_cannot_consume_unrelated_capacity_or_leak_reservations() {
    let scenario = Scenario {
        name: "stalled peer isolation",
        fault_plan: "one peer accepts shared management work and never answers while another \
                     keeps exchanging every traffic class with the hub; the hub then holds the \
                     stalled link, lets that peer depart and tears it down",
        seeds: &[91, 92, 93],
    };
    scenario.check(isolation_config, stalled_peer_plan);
}
