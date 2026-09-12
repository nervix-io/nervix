//! Who is in the cluster, and how to reach them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Gossip membership, current application-health observations, and the adapter that
//!   carries Chitchat exchanges on the interconnect management pool.
//! - **Depends on.** `chitchat`, authenticated HTTP/2 interconnect requests, the vocabulary's node
//!   names, and the gossip view types consensus reconciles membership from.
//! - **Must not know.** Domains, graphs, schedules or the runtime. Cluster topology and current
//!   application health are the whole answers it gives.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use chitchat::{
    Chitchat, ChitchatHandle, ChitchatId, ChitchatMessage, Deserializable as _, NodeState,
    Serializable as _, spawn_chitchat,
    transport::{Socket as GossipSocket, Transport as GossipTransport},
};
use dashmap::DashMap;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{GossipNode, GossipState};
use nervix_interconnect::{
    InterconnectRequest, PeerTarget, PoolClass, RequestContext, RequestSubquota,
    Transport as InterconnectTransport,
};
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::{
    net::lookup_host,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::info;

const KEY_CLUSTER_ID: &str = "cluster_id";
const KEY_NODE_ID: &str = "node_id";
const KEY_GRPC_LISTEN_ADDR: &str = "grpc_listen_addr";
const KEY_GRPC_ADVERTISE_ADDR: &str = "grpc_advertise_addr";
const KEY_WEB_CONSOLE_ADVERTISE_ADDR: &str = "web_console_advertise_addr";
const KEY_INTERCONNECT_LISTEN_ADDR: &str = "interconnect_listen_addr";
const KEY_INTERCONNECT_ADVERTISE_ADDR: &str = "interconnect_advertise_addr";
const KEY_BOOTSTRAP_HOST: &str = "bootstrap_host";
const KEY_SUBSCRIPTION_INTEREST_PREFIX: &str = "subscription_interest:";
const KEY_RUNTIME_REVISION_READY: &str = "runtime_revision_ready";
/// How many cluster changes a session can fall behind before the bus drops the oldest.
const CLUSTER_EVENT_CAPACITY: usize = 256;
const GOSSIP_QUEUE_CAPACITY: usize = 1024;
/// Leaves room for the typed request fields inside the 64-KiB management-event limit.
const MAX_GOSSIP_MESSAGE_BYTES: usize = 60 * 1024;

pub struct ClusterHandle {
    local_incarnation: nervix_models::ClusterNodeIncarnation,
    chitchat: Arc<tokio::sync::Mutex<Chitchat>>,
    chitchat_server: Mutex<Option<ChitchatHandle>>,
    events: ClusterEvents,
    peer_health_state: watch::Sender<PeerHealthStateSnapshot>,
    node_unavailability_timeout: Duration,
    membership_task: Mutex<Option<JoinHandle<()>>>,
}

/// The gossip event bus, and the one way a membership or peer-connectivity change reaches a
/// session attached to this node.
///
/// Offering goes through [`Self::offer`] rather than through the sender directly. Each caller
/// records the change in its own `info` line first, with the node and address as fields, and that
/// line is the record: it is written whether or not anyone is attached. Subscribers here are live
/// sessions only, so a node serving none is the ordinary case rather than a failure, and an
/// undelivered offer costs the operator nothing.
#[derive(Clone)]
struct ClusterEvents {
    sender: broadcast::Sender<String>,
}

impl ClusterEvents {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(CLUSTER_EVENT_CAPACITY);
        Self { sender }
    }

    /// Offer an already-recorded change in this node's view of the cluster to attached sessions.
    fn offer(&self, message: String) {
        self.sender
            .send(message)
            .discarded("the caller logged this change before offering it to attached sessions");
    }

    fn subscribe(&self) -> broadcast::Receiver<String> {
        self.sender.subscribe()
    }
}

/// One exact advertised endpoint whose application health this node should observe.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PeerHealthEndpoint {
    identity: ClusterNodeIdentity,
    address: String,
}

impl PeerHealthEndpoint {
    pub(crate) fn new(identity: ClusterNodeIdentity, address: String) -> Self {
        Self { identity, address }
    }
}

/// The retained identity of one probe attempt.
///
/// The generated endpoint generation prevents a late result from an endpoint that disappeared and
/// later returned with the same advertised address from becoming current again.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PeerHealthProbeTarget {
    identity: ClusterNodeIdentity,
    endpoint_generation: u64,
    address: String,
}

impl PeerHealthProbeTarget {
    pub(crate) fn identity(&self) -> &ClusterNodeIdentity {
        &self.identity
    }

    pub(crate) fn node_id(&self) -> &ClusterNodeName {
        self.identity.node_id()
    }

    pub(crate) const fn endpoint_generation(&self) -> u64 {
        self.endpoint_generation
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    fn matches_endpoint(&self, endpoint: &PeerHealthEndpoint) -> bool {
        self.identity == endpoint.identity && self.address == endpoint.address
    }

    fn matches_gossip_node(&self, node: &GossipNode) -> bool {
        self.identity == node.identity() && self.address == node.interconnect_advertise_addr
    }
}

/// The semantic outcome of one attempted application-health observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PeerHealthProbeOutcome {
    Healthy(ClusterNodeIdentity),
    Failure,
    CapacityExhausted,
    Unscheduled,
}

/// One completed or deliberately unscheduled probe, timestamped on this observing node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PeerHealthProbeResult {
    target: PeerHealthProbeTarget,
    outcome: PeerHealthProbeOutcome,
    observed_at: Instant,
}

impl PeerHealthProbeResult {
    pub(crate) fn new(
        target: PeerHealthProbeTarget,
        outcome: PeerHealthProbeOutcome,
        observed_at: Instant,
    ) -> Self {
        Self {
            target,
            outcome,
            observed_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerHealthResultDisposition {
    Published { revision: u64 },
    Superseded,
}

/// The effective state of one current peer-health target at a particular monotonic instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerHealthStatus {
    Unknown,
    Healthy,
    Failure,
    Unavailable,
}

#[derive(Debug, Clone)]
pub(crate) struct PeerHealthSnapshot {
    #[cfg(test)]
    revision: u64,
    scheduling_revision: u64,
    statuses: BTreeMap<ClusterNodeName, PeerHealthStatus>,
    latest_outcomes: BTreeMap<ClusterNodeName, PeerHealthObservationKind>,
    observation_times: BTreeMap<ClusterNodeName, Instant>,
}

impl PeerHealthSnapshot {
    #[cfg(test)]
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) const fn scheduling_revision(&self) -> u64 {
        self.scheduling_revision
    }

    pub(crate) fn status(&self, node_id: &ClusterNodeName) -> Option<PeerHealthStatus> {
        self.statuses.get(node_id).copied()
    }

    pub(crate) fn latest_outcome(
        &self,
        node_id: &ClusterNodeName,
    ) -> Option<PeerHealthObservationKind> {
        self.latest_outcomes.get(node_id).copied()
    }

    pub(crate) fn observed_at(&self, node_id: &ClusterNodeName) -> Option<Instant> {
        self.observation_times.get(node_id).copied()
    }

    pub(crate) fn unavailable_nodes(&self) -> BTreeSet<ClusterNodeName> {
        self.statuses
            .iter()
            .filter_map(|(node_id, status)| {
                if *status == PeerHealthStatus::Unavailable {
                    Some(node_id.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

/// The latest diagnostic outcome, retained even when its effective availability is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerHealthObservationKind {
    Healthy,
    Failure,
    CapacityExhausted,
    Unscheduled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerHealthObservation {
    outcome: PeerHealthObservationKind,
    observed_at: Instant,
    failure_since: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetainedPeerHealth {
    target: PeerHealthProbeTarget,
    observation: Option<PeerHealthObservation>,
}

#[derive(Debug, Clone, Default)]
struct PeerHealthStateSnapshot {
    revision: u64,
    scheduling_revision: u64,
    next_endpoint_generation: u64,
    peers: BTreeMap<ClusterNodeName, RetainedPeerHealth>,
    effective_statuses: BTreeMap<ClusterNodeName, PeerHealthStatus>,
}

impl PeerHealthStateSnapshot {
    const fn revision(&self) -> u64 {
        self.revision
    }

    const fn scheduling_revision(&self) -> u64 {
        self.scheduling_revision
    }

    fn replace_endpoints(
        &mut self,
        endpoints: impl IntoIterator<Item = PeerHealthEndpoint>,
        now: Instant,
        observation_freshness: Duration,
    ) -> Vec<PeerHealthProbeTarget> {
        let mut desired = BTreeMap::new();
        for endpoint in endpoints {
            let node_id = endpoint.identity.node_id().clone();
            match desired.entry(node_id) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    if endpoint > *entry.get() {
                        entry.insert(endpoint);
                    }
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(endpoint);
                }
            }
        }

        let previous = std::mem::take(&mut self.peers);
        let mut next = BTreeMap::new();
        for (node_id, endpoint) in desired {
            let retained = match previous.get(&node_id) {
                Some(current) if current.target.matches_endpoint(&endpoint) => current.clone(),
                Some(_) | None => RetainedPeerHealth {
                    target: PeerHealthProbeTarget {
                        identity: endpoint.identity,
                        endpoint_generation: self.issue_endpoint_generation(),
                        address: endpoint.address,
                    },
                    observation: None,
                },
            };
            next.insert(node_id, retained);
        }
        let changed = previous != next;
        self.peers = next;
        if changed {
            self.effective_statuses = self.derive_effective_statuses(now, observation_freshness);
            self.advance_revision();
            self.advance_scheduling_revision();
        } else {
            self.refresh_effective_statuses(now, observation_freshness);
        }
        self.peers
            .values()
            .map(|peer| peer.target.clone())
            .collect()
    }

    fn record_result(
        &mut self,
        result: PeerHealthProbeResult,
        observation_freshness: Duration,
    ) -> PeerHealthResultDisposition {
        let node_id = result.target.node_id().clone();
        let Some(peer) = self.peers.get_mut(&node_id) else {
            return PeerHealthResultDisposition::Superseded;
        };
        if peer.target != result.target {
            return PeerHealthResultDisposition::Superseded;
        }
        let outcome = match result.outcome {
            PeerHealthProbeOutcome::Healthy(identity) => {
                if identity != peer.target.identity {
                    return PeerHealthResultDisposition::Superseded;
                }
                PeerHealthObservationKind::Healthy
            }
            PeerHealthProbeOutcome::Failure => PeerHealthObservationKind::Failure,
            PeerHealthProbeOutcome::CapacityExhausted => {
                PeerHealthObservationKind::CapacityExhausted
            }
            PeerHealthProbeOutcome::Unscheduled => PeerHealthObservationKind::Unscheduled,
        };
        if let Some(current) = peer.observation.as_ref()
            && current.observed_at >= result.observed_at
        {
            return PeerHealthResultDisposition::Superseded;
        }
        let failure_since = if outcome == PeerHealthObservationKind::Failure {
            match peer.observation.as_ref() {
                Some(current)
                    if current.outcome == PeerHealthObservationKind::Failure
                        && result
                            .observed_at
                            .checked_duration_since(current.observed_at)
                            .is_some_and(|elapsed| elapsed < observation_freshness) =>
                {
                    current.failure_since
                }
                Some(_) | None => Some(result.observed_at),
            }
        } else {
            None
        };
        peer.observation = Some(PeerHealthObservation {
            outcome,
            observed_at: result.observed_at,
            failure_since,
        });
        let effective_statuses =
            self.derive_effective_statuses(result.observed_at, observation_freshness);
        let scheduling_changed = effective_statuses != self.effective_statuses;
        self.effective_statuses = effective_statuses;
        self.advance_revision();
        if scheduling_changed {
            self.advance_scheduling_revision();
        }
        PeerHealthResultDisposition::Published {
            revision: self.revision,
        }
    }

    fn effective_snapshot(
        &mut self,
        now: Instant,
        observation_freshness: Duration,
    ) -> PeerHealthSnapshot {
        self.refresh_effective_statuses(now, observation_freshness);
        PeerHealthSnapshot {
            #[cfg(test)]
            revision: self.revision,
            scheduling_revision: self.scheduling_revision,
            statuses: self.effective_statuses.clone(),
            latest_outcomes: self
                .peers
                .iter()
                .filter_map(|(node_id, peer)| {
                    let observation = peer.observation.as_ref()?;
                    Some((node_id.clone(), observation.outcome))
                })
                .collect(),
            observation_times: self
                .peers
                .iter()
                .filter_map(|(node_id, peer)| {
                    let observation = peer.observation.as_ref()?;
                    Some((node_id.clone(), observation.observed_at))
                })
                .collect(),
        }
    }

    fn refresh_effective_statuses(
        &mut self,
        now: Instant,
        observation_freshness: Duration,
    ) -> bool {
        let next = self.derive_effective_statuses(now, observation_freshness);
        if next == self.effective_statuses {
            return false;
        }
        self.effective_statuses = next;
        self.advance_revision();
        self.advance_scheduling_revision();
        true
    }

    fn derive_effective_statuses(
        &self,
        now: Instant,
        observation_freshness: Duration,
    ) -> BTreeMap<ClusterNodeName, PeerHealthStatus> {
        self.peers
            .iter()
            .map(|(node_id, peer)| {
                (
                    node_id.clone(),
                    peer.effective_status(now, observation_freshness),
                )
            })
            .collect()
    }

    fn next_effective_transition(
        &self,
        now: Instant,
        observation_freshness: Duration,
    ) -> Option<Instant> {
        self.peers
            .values()
            .filter_map(|peer| peer.next_effective_transition(now, observation_freshness))
            .min()
    }

    fn issue_endpoint_generation(&mut self) -> u64 {
        self.next_endpoint_generation = self
            .next_endpoint_generation
            .checked_add(1)
            .assured("a process cannot publish u64::MAX endpoint generations during its lifetime");
        self.next_endpoint_generation
    }

    fn advance_revision(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .assured("a process cannot publish u64::MAX peer-health revisions during its lifetime");
    }

    fn advance_scheduling_revision(&mut self) {
        self.scheduling_revision = self.scheduling_revision.checked_add(1).assured(
            "a process cannot publish u64::MAX peer-health scheduling revisions during its \
             lifetime",
        );
    }
}

impl RetainedPeerHealth {
    fn effective_status(&self, now: Instant, observation_freshness: Duration) -> PeerHealthStatus {
        let Some(observation) = self.observation.as_ref() else {
            return PeerHealthStatus::Unknown;
        };
        let Some(age) = now.checked_duration_since(observation.observed_at) else {
            return PeerHealthStatus::Unknown;
        };
        if age >= observation_freshness {
            return PeerHealthStatus::Unknown;
        }
        match observation.outcome {
            PeerHealthObservationKind::Healthy => PeerHealthStatus::Healthy,
            PeerHealthObservationKind::CapacityExhausted
            | PeerHealthObservationKind::Unscheduled => PeerHealthStatus::Unknown,
            PeerHealthObservationKind::Failure => {
                let Some(failure_since) = observation.failure_since else {
                    return PeerHealthStatus::Unknown;
                };
                let Some(failure_age) = now.checked_duration_since(failure_since) else {
                    return PeerHealthStatus::Unknown;
                };
                if failure_age >= observation_freshness {
                    PeerHealthStatus::Unavailable
                } else {
                    PeerHealthStatus::Failure
                }
            }
        }
    }

    fn next_effective_transition(
        &self,
        now: Instant,
        observation_freshness: Duration,
    ) -> Option<Instant> {
        let observation = self.observation.as_ref()?;
        let stale_at = observation.observed_at.checked_add(observation_freshness)?;
        if stale_at <= now {
            return None;
        }
        if observation.outcome != PeerHealthObservationKind::Failure {
            return Some(stale_at);
        }
        let unavailable_at = observation
            .failure_since?
            .checked_add(observation_freshness)?;
        if unavailable_at > now && unavailable_at < stale_at {
            Some(unavailable_at)
        } else {
            Some(stale_at)
        }
    }
}

struct PeerHealthStateWatcher {
    state: watch::Receiver<PeerHealthStateSnapshot>,
    observation_freshness: Duration,
}

impl PeerHealthStateWatcher {
    fn wait_for_change_or_next_transition(&mut self) -> impl std::future::Future<Output = ()> + '_ {
        let scheduling_revision = self.state.borrow_and_update().scheduling_revision();
        async move {
            loop {
                tokio::task::consume_budget().await;
                let next_transition = self
                    .state
                    .borrow()
                    .next_effective_transition(Instant::now(), self.observation_freshness);
                tokio::select! {
                    changed = self.state.changed() => {
                        changed.assured(
                            "the cluster handle retains its peer-health state sender for its \
                             lifetime",
                        );
                        if self.state.borrow_and_update().scheduling_revision()
                            != scheduling_revision
                        {
                            return;
                        }
                    }
                    _ = async {
                        match next_transition {
                            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => return,
                }
            }
        }
    }
}

/// Retained inputs whose changes can alter the node's effective cluster view.
///
/// Application-health failure becomes effective after an elapsed monotonic deadline without a
/// retained-input change. The watcher owns that deadline so consumers can re-evaluate cluster state
/// without sampling it on an interval.
pub(crate) struct ClusterStateWatcher {
    live_node_states: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    peer_health_state: PeerHealthStateWatcher,
}

impl ClusterStateWatcher {
    pub(crate) fn wait_for_change_or_next_unavailability(
        &mut self,
    ) -> impl std::future::Future<Output = ()> + '_ {
        let Self {
            live_node_states,
            peer_health_state,
        } = self;
        let peer_health_change = peer_health_state.wait_for_change_or_next_transition();
        async move {
            tokio::select! {
                changed = live_node_states.changed() => changed.assured(
                    "the cluster handle retains its Chitchat state sender for its lifetime",
                ),
                _ = peer_health_change => {}
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostPort {
    host: String,
    port: u16,
}

impl HostPort {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self::new(addr.ip().to_string(), addr.port())
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn with_port(&self, port: u16) -> Self {
        Self::new(self.host.clone(), port)
    }

    pub fn url_authority(&self) -> String {
        self.authority()
    }

    fn authority(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    pub async fn resolve_all(&self) -> io::Result<Vec<SocketAddr>> {
        let resolved = lookup_host((self.host.as_str(), self.port))
            .await?
            .collect::<Vec<_>>();
        if resolved.is_empty() {
            Err(io::Error::other(format!(
                "host '{}' resolved to no addresses",
                self.host
            )))
        } else {
            Ok(resolved)
        }
    }

    pub async fn resolve_one(&self) -> io::Result<SocketAddr> {
        self.resolve_all().await.map(|mut addrs| addrs.remove(0))
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.authority())
    }
}

impl From<SocketAddr> for HostPort {
    fn from(addr: SocketAddr) -> Self {
        Self::from_socket_addr(addr)
    }
}

impl FromStr for HostPort {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = raw.parse::<SocketAddr>() {
            return Ok(Self::from_socket_addr(addr));
        }

        let (host, port) = raw
            .rsplit_once(':')
            .ok_or_else(|| "missing ':port' suffix".to_string())?;
        if host.is_empty() {
            return Err("missing host".to_string());
        }
        if host.contains(':') {
            return Err("IPv6 addresses must use '[addr]:port' form".to_string());
        }
        let port = port
            .parse::<u16>()
            .map_err(|_| format!("invalid port '{port}'"))?;
        Ok(Self::new(host.to_string(), port))
    }
}

#[derive(Clone)]
pub struct ClusterSettings {
    pub cluster_id: String,
    pub node_id: ClusterNodeName,
    pub grpc_listen_addr: SocketAddr,
    pub grpc_advertise_addr: String,
    pub web_console_advertise_addr: String,
    pub interconnect_advertise_addr: HostPort,
    pub bootstrap_host: Option<String>,
    pub interconnect: InterconnectTransport,
    pub node_unavailability_timeout: Duration,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct GossipExchange {
    from: SocketAddr,
    payload: Vec<u8>,
}

impl InterconnectRequest for GossipExchange {
    type Response = Result<(), String>;

    const NAME: &'static str = "gossip_exchange";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
    const TIMEOUT: Duration = Duration::from_secs(1);
    const REQUIRES_LIVE_TARGET: bool = false;
}

#[derive(Clone)]
struct InterconnectGossipTransport {
    inner: Arc<InterconnectGossipTransportInner>,
}

struct InterconnectGossipTransportInner {
    listen_addr: SocketAddr,
    advertise_addr: SocketAddr,
    interconnect: InterconnectTransport,
    routes: DashMap<SocketAddr, GossipRoute>,
    incoming_tx: mpsc::Sender<GossipDatagram>,
    incoming_rx: Mutex<Option<mpsc::Receiver<GossipDatagram>>>,
}

struct GossipDatagram {
    from: SocketAddr,
    payload: Vec<u8>,
}

#[derive(Clone)]
struct GossipRoute {
    node_id: Option<ClusterNodeName>,
    target: PeerTarget,
}

impl InterconnectGossipTransport {
    fn build(
        interconnect: InterconnectTransport,
        advertise_addr: SocketAddr,
        seed_targets: Vec<(SocketAddr, PeerTarget)>,
    ) -> io::Result<Self> {
        let (incoming_tx, incoming_rx) = mpsc::channel(GOSSIP_QUEUE_CAPACITY);
        let routes = DashMap::new();
        for (addr, target) in seed_targets {
            routes.insert(
                addr,
                GossipRoute {
                    node_id: None,
                    target,
                },
            );
        }
        let transport = Self {
            inner: Arc::new(InterconnectGossipTransportInner {
                listen_addr: interconnect.local_addr(),
                advertise_addr,
                interconnect: interconnect.clone(),
                routes,
                incoming_tx,
                incoming_rx: Mutex::new(Some(incoming_rx)),
            }),
        };
        let handler_transport = transport.clone();
        interconnect
            .register_handler::<GossipExchange, _, _>(move |context, request| {
                let transport = handler_transport.clone();
                async move { transport.receive(context, request).await }
            })
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(transport)
    }

    async fn receive(
        &self,
        context: RequestContext,
        request: GossipExchange,
    ) -> Result<(), String> {
        if request.payload.len() > MAX_GOSSIP_MESSAGE_BYTES {
            return Err(format!(
                "gossip message exceeds {MAX_GOSSIP_MESSAGE_BYTES} bytes"
            ));
        }
        let target = PeerTarget::new(request.from, context.peer_advertised_host().to_string());
        self.inner
            .interconnect
            .register_outbound_target(context.peer_node_id().clone(), target.clone())
            .map_err(|error| error.to_string())?;
        self.inner.routes.insert(
            request.from,
            GossipRoute {
                node_id: Some(context.peer_node_id().clone()),
                target,
            },
        );
        self.inner
            .incoming_tx
            .send(GossipDatagram {
                from: request.from,
                payload: request.payload,
            })
            .await
            .map_err(|_| "gossip receiver has shut down".to_string())
    }

    fn refresh_routes(&self, nodes: &BTreeMap<ChitchatId, NodeState>) {
        for (id, state) in nodes {
            let Ok(node_id) = ClusterNodeName::parse(&id.node_id) else {
                continue;
            };
            if &node_id == self.inner.interconnect.node_id() {
                continue;
            }
            let Some(endpoint) = state.get(KEY_INTERCONNECT_ADVERTISE_ADDR) else {
                continue;
            };
            let Ok(endpoint) = endpoint.parse::<HostPort>() else {
                continue;
            };
            if endpoint.port() != id.gossip_advertise_addr.port() {
                continue;
            }
            let target = PeerTarget::new(id.gossip_advertise_addr, endpoint.host().to_string());
            if self
                .inner
                .interconnect
                .register_outbound_target(node_id.clone(), target.clone())
                .is_ok()
            {
                self.inner.routes.insert(
                    id.gossip_advertise_addr,
                    GossipRoute {
                        node_id: Some(node_id),
                        target,
                    },
                );
            }
        }
    }
}

struct InterconnectGossipSocket {
    transport: InterconnectGossipTransport,
    incoming: mpsc::Receiver<GossipDatagram>,
}

#[async_trait]
impl GossipTransport for InterconnectGossipTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn GossipSocket>> {
        if listen_addr != self.inner.listen_addr {
            anyhow::bail!(
                "interconnect gossip transport was opened for {listen_addr}, expected {}",
                self.inner.listen_addr
            );
        }
        let incoming =
            self.inner.incoming_rx.lock().take().ok_or_else(|| {
                anyhow::anyhow!("interconnect gossip transport was already opened")
            })?;
        Ok(Box::new(InterconnectGossipSocket {
            transport: self.clone(),
            incoming,
        }))
    }
}

#[async_trait]
impl GossipSocket for InterconnectGossipSocket {
    async fn send(&mut self, to: SocketAddr, message: ChitchatMessage) -> anyhow::Result<()> {
        let payload = message.serialize_to_vec();
        if payload.len() > MAX_GOSSIP_MESSAGE_BYTES {
            anyhow::bail!("gossip message exceeds {MAX_GOSSIP_MESSAGE_BYTES} bytes");
        }
        let route = match self.transport.inner.routes.get(&to) {
            Some(route) => route.clone(),
            None => GossipRoute {
                node_id: None,
                target: PeerTarget::new(to, to.ip().to_string()),
            },
        };
        let node_id = match route.node_id {
            Some(node_id) => node_id,
            None => {
                let node_id = self
                    .transport
                    .inner
                    .interconnect
                    .bootstrap_target(route.target.clone())
                    .await?;
                self.transport.inner.routes.insert(
                    to,
                    GossipRoute {
                        node_id: Some(node_id.clone()),
                        target: route.target,
                    },
                );
                node_id
            }
        };
        let response = self
            .transport
            .inner
            .interconnect
            .request(
                &node_id,
                GossipExchange {
                    from: self.transport.inner.advertise_addr,
                    payload,
                },
            )
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        response.map_err(anyhow::Error::msg)?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, ChitchatMessage)> {
        let GossipDatagram { from, payload } = self
            .incoming
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("interconnect gossip receiver shut down"))?;
        let mut remaining = payload.as_slice();
        let message = ChitchatMessage::deserialize(&mut remaining)?;
        if !remaining.is_empty() {
            anyhow::bail!("gossip message has trailing bytes");
        }
        Ok((from, message))
    }
}

pub async fn start_cluster(settings: ClusterSettings) -> io::Result<ClusterHandle> {
    let node_id = settings.node_id.clone();
    // Gossip tells one run of a node from the next by this generation, and a peer ignores any
    // key-value whose version is below what it already holds for the same generation. A restart
    // publishes its address from version one again, so two restarts sharing a generation leave
    // peers on the address the earlier of them published, dialling a port nothing listens on.
    // Only the interconnect ports move across a restart, so the rest of the identity is equal and
    // the generation is the sole thing keeping the runs apart: it is taken in nanoseconds.
    let generation_id = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .assured("nanoseconds since the epoch stay within a u64 until the year 2554");
    let gossip_advertise_addr = settings.interconnect_advertise_addr.resolve_one().await?;
    let mut seed_targets = Vec::new();
    let seed_nodes = match settings.bootstrap_host.as_deref() {
        Some(seed) => {
            let seed = seed
                .parse::<HostPort>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let addresses = seed.resolve_all().await?;
            for addr in &addresses {
                seed_targets.push((*addr, PeerTarget::new(*addr, seed.host().to_string())));
            }
            addresses.into_iter().map(|addr| addr.to_string()).collect()
        }
        None => Vec::new(),
    };
    let transport = InterconnectGossipTransport::build(
        settings.interconnect.clone(),
        gossip_advertise_addr,
        seed_targets,
    )?;
    let chitchat_id = ChitchatId {
        node_id: node_id.to_string(),
        generation_id,
        gossip_advertise_addr,
    };

    let config = chitchat::ChitchatConfig {
        chitchat_id,
        cluster_id: settings.cluster_id.clone(),
        gossip_interval: Duration::from_millis(500),
        listen_addr: settings.interconnect.local_addr(),
        seed_nodes,
        failure_detector_config: chitchat::FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(60),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };

    let initial_key_values = vec![
        (KEY_CLUSTER_ID.to_string(), settings.cluster_id.clone()),
        (KEY_NODE_ID.to_string(), node_id.to_string()),
        (
            KEY_GRPC_LISTEN_ADDR.to_string(),
            settings.grpc_listen_addr.to_string(),
        ),
        (
            KEY_GRPC_ADVERTISE_ADDR.to_string(),
            settings.grpc_advertise_addr.clone(),
        ),
        (
            KEY_WEB_CONSOLE_ADVERTISE_ADDR.to_string(),
            settings.web_console_advertise_addr.clone(),
        ),
        (
            KEY_INTERCONNECT_LISTEN_ADDR.to_string(),
            settings.interconnect.local_addr().to_string(),
        ),
        (
            KEY_INTERCONNECT_ADVERTISE_ADDR.to_string(),
            settings.interconnect_advertise_addr.to_string(),
        ),
        (
            KEY_BOOTSTRAP_HOST.to_string(),
            settings.bootstrap_host.clone().unwrap_or_default(),
        ),
    ];

    let chitchat = spawn_chitchat(config, initial_key_values, &transport)
        .await
        .map_err(|err| io::Error::other(format!("failed to start chitchat: {err}")))?;

    let events = ClusterEvents::new();
    let chitchat_state = chitchat.chitchat();
    let mut live_nodes = chitchat_state.lock().await.live_nodes_watch_stream();
    let event_tx = events.clone();
    let route_transport = transport.clone();
    let membership_task = tokio::spawn(async move {
        while let Some(nodes) = live_nodes.next().await {
            tokio::task::consume_budget().await;
            route_transport.refresh_routes(&nodes);
            let report = membership_report(&nodes);
            info!(members = ?report, "cluster membership updated");
            event_tx.offer(format!("cluster membership updated: {}", report.join(", ")));
        }
    });

    Ok(ClusterHandle {
        local_incarnation: nervix_models::ClusterNodeIncarnation::new(generation_id),
        chitchat: chitchat_state,
        chitchat_server: Mutex::new(Some(chitchat)),
        events,
        peer_health_state: watch::channel(PeerHealthStateSnapshot::default()).0,
        node_unavailability_timeout: settings.node_unavailability_timeout,
        membership_task: Mutex::new(Some(membership_task)),
    })
}

impl ClusterHandle {
    pub fn local_incarnation(&self) -> nervix_models::ClusterNodeIncarnation {
        self.local_incarnation
    }

    pub async fn shutdown(&self) -> io::Result<()> {
        let chitchat_server = self.chitchat_server.lock().take();
        let result = match chitchat_server {
            Some(chitchat_server) => chitchat_server.shutdown().await.map_err(io::Error::other),
            None => Ok(()),
        };

        let membership_task = self.membership_task.lock().take();
        if let Some(membership_task) = membership_task {
            membership_task.abort();
            if let Err(error) = membership_task.await
                && !error.is_cancelled()
            {
                return Err(io::Error::other(error));
            }
        }

        result
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
    }

    pub async fn local_node_identity(&self) -> ClusterNodeIdentity {
        let chitchat = self.chitchat.lock().await;
        let identity = chitchat.self_chitchat_id();
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(&identity.node_id)
                .assured("the local Chitchat identity was built from a validated node name"),
            ClusterNodeIncarnation::new(identity.generation_id),
        )
    }

    pub(crate) async fn subscribe_live_node_states(
        &self,
    ) -> watch::Receiver<BTreeMap<ChitchatId, NodeState>> {
        self.chitchat.lock().await.live_nodes_watcher()
    }

    pub(crate) async fn subscribe_state_changes(&self) -> ClusterStateWatcher {
        ClusterStateWatcher {
            live_node_states: self.subscribe_live_node_states().await,
            peer_health_state: self.subscribe_peer_health_state(),
        }
    }

    pub async fn status_lines(&self) -> Vec<String> {
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();
        let cluster_id = chitchat.cluster_id().to_string();
        let self_state = chitchat.node_state(&self_id).cloned();
        let mut live_nodes = BTreeMap::new();
        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
                continue;
            }
            let Some(state) = chitchat.node_state(node_id) else {
                continue;
            };
            live_nodes.insert(node_id.clone(), state.clone());
        }
        let seed_nodes = chitchat
            .seed_nodes()
            .into_iter()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();
        drop(chitchat);
        let mut out = Vec::new();
        out.push(format!("cluster_id: {cluster_id}"));
        out.push(format!("seed_nodes: {}", join_or_none(&seed_nodes)));
        out.push("self:".to_string());
        if let Some(self_state) = self_state {
            out.extend(render_node_state_lines(&self_id, &self_state));
        }
        out.push("live_nodes:".to_string());
        for (node_id, state) in live_nodes {
            out.extend(render_node_state_lines(&node_id, &state));
        }
        out
    }

    pub fn interconnect_status_section(&self) -> Vec<String> {
        self.peer_health_status_lines()
    }

    pub async fn gossip_state(&self) -> GossipState {
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let live_nodes = current_live_nodes(&chitchat);
        let live_node_ids = live_nodes.keys().cloned().collect::<BTreeSet<_>>();

        let dead_node_ids = chitchat
            .dead_nodes()
            .filter_map(|node_id| ClusterNodeName::parse(&node_id.node_id).ok())
            .filter(|node_id| !live_node_ids.contains(node_id))
            .collect::<BTreeSet<_>>();

        GossipState {
            live_nodes: live_nodes.into_values().collect(),
            dead_node_ids,
        }
    }

    /// Current topology with only effective application-health unavailability added.
    ///
    /// Membership reconciliation consumes [`Self::gossip_state`] directly. Runtime coordination
    /// and scheduling use this availability view so an unknown observation never removes a peer.
    pub(crate) async fn availability_state(&self) -> GossipState {
        let mut state = self.gossip_state().await;
        state
            .dead_node_ids
            .extend(self.peer_health_snapshot().unavailable_nodes());
        state
    }

    pub async fn live_node_ids(&self) -> Vec<ClusterNodeName> {
        let gossip = self.availability_state().await;
        let dead_node_ids = gossip.dead_node_ids;
        gossip
            .live_nodes
            .into_iter()
            .filter(|node| !dead_node_ids.contains(&node.node_id))
            .map(|node| node.node_id)
            .collect()
    }

    pub async fn set_local_runtime_revision_ready(&self, revision: u64) {
        let chitchat_handle = self.chitchat.clone();
        let mut chitchat = chitchat_handle.lock().await;
        advance_runtime_revision_readiness(chitchat.self_node_state(), revision);
    }

    pub async fn nodes_ready_for_runtime_revision(
        &self,
        revision: u64,
    ) -> BTreeSet<ClusterNodeIdentity> {
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();
        let mut ready = BTreeSet::new();

        if let Some(state) = chitchat.node_state(&self_id)
            && runtime_revision_is_ready(state, revision)
            && let Some(identity) = cluster_node_identity(&self_id)
        {
            ready.insert(identity);
        }
        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
                continue;
            }
            if let Some(state) = chitchat.node_state(node_id)
                && runtime_revision_is_ready(state, revision)
                && let Some(identity) = cluster_node_identity(node_id)
            {
                ready.insert(identity);
            }
        }
        ready
    }

    pub async fn set_local_subscription_interest(
        &self,
        domain: &str,
        relay: &str,
        interested: bool,
    ) {
        let key = subscription_interest_key(domain, relay);
        let chitchat_handle = self.chitchat.clone();
        let mut chitchat = chitchat_handle.lock().await;
        let state = chitchat.self_node_state();
        if interested {
            state.set(key, "1");
        } else {
            state.delete(&key);
        }
    }

    pub async fn nodes_with_subscription_interest(
        &self,
        domain: &str,
        relay: &str,
    ) -> BTreeSet<ClusterNodeName> {
        let key = subscription_interest_key(domain, relay);
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();
        let mut interested = BTreeSet::new();

        if let Some(state) = chitchat.node_state(&self_id)
            && state.get(&key).is_some()
        {
            interested.extend(ClusterNodeName::parse(&self_id.node_id).ok());
        }

        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
                continue;
            }
            if let Some(state) = chitchat.node_state(node_id)
                && state.get(&key).is_some()
            {
                interested.extend(ClusterNodeName::parse(&node_id.node_id).ok());
            }
        }

        interested
    }

    /// Wait until this node's live Chitchat view contains the interest advertised by the exact
    /// subscriber incarnation. The interconnect request that calls this method owns the deadline.
    pub(crate) async fn wait_for_subscription_interest(
        &self,
        subscriber: &ClusterNodeIdentity,
        domain: &str,
        relay: &str,
    ) {
        let key = subscription_interest_key(domain, relay);
        let mut live_node_states = self.subscribe_live_node_states().await;
        loop {
            tokio::task::consume_budget().await;
            let visible = {
                let states = live_node_states.borrow_and_update();
                states.iter().any(|(chitchat_id, state)| {
                    cluster_node_identity(chitchat_id).as_ref() == Some(subscriber)
                        && state.get(&key).is_some()
                })
            };
            if visible {
                return;
            }
            live_node_states.changed().await.assured(
                "the cluster handle retains its Chitchat state sender for the server lifetime",
            );
        }
    }

    pub(crate) fn replace_peer_health_endpoints(
        &self,
        endpoints: impl IntoIterator<Item = PeerHealthEndpoint>,
    ) -> Vec<PeerHealthProbeTarget> {
        let now = Instant::now();
        let endpoints = endpoints.into_iter().collect::<Vec<_>>();
        let mut targets = Vec::new();
        self.peer_health_state.send_if_modified(|snapshot| {
            let previous_revision = snapshot.revision();
            targets = snapshot.replace_endpoints(
                endpoints.clone(),
                now,
                self.node_unavailability_timeout,
            );
            snapshot.revision() != previous_revision
        });
        targets
    }

    pub(crate) async fn record_peer_health_result(
        &self,
        result: PeerHealthProbeResult,
    ) -> PeerHealthResultDisposition {
        let chitchat = self.chitchat.lock().await;
        let current_live_nodes = current_live_nodes(&chitchat);
        let Some(current_node) = current_live_nodes.get(result.target.node_id()) else {
            return PeerHealthResultDisposition::Superseded;
        };
        if !result.target.matches_gossip_node(current_node) {
            return PeerHealthResultDisposition::Superseded;
        }
        let mut disposition = PeerHealthResultDisposition::Superseded;
        self.peer_health_state.send_if_modified(|snapshot| {
            disposition = snapshot.record_result(result.clone(), self.node_unavailability_timeout);
            matches!(disposition, PeerHealthResultDisposition::Published { .. })
        });
        disposition
    }

    pub(crate) fn peer_health_snapshot(&self) -> PeerHealthSnapshot {
        let now = Instant::now();
        let mut effective = None;
        self.peer_health_state.send_if_modified(|snapshot| {
            let previous_revision = snapshot.revision();
            effective = Some(snapshot.effective_snapshot(now, self.node_unavailability_timeout));
            snapshot.revision() != previous_revision
        });
        effective.assured("the peer-health watch update closure always publishes its snapshot")
    }

    pub(crate) fn peer_health_scheduling_revision_is_current(&self, expected: u64) -> bool {
        self.peer_health_snapshot().scheduling_revision() == expected
    }

    fn peer_health_status_lines(&self) -> Vec<String> {
        let effective = self.peer_health_snapshot();
        let now = Instant::now();
        let retained = self.peer_health_state.borrow();
        if retained.peers.is_empty() {
            return vec!["- (none)".to_string()];
        }
        retained
            .peers
            .iter()
            .map(|(node_id, peer)| {
                let status = match effective.status(node_id) {
                    Some(PeerHealthStatus::Healthy) => "connected",
                    Some(PeerHealthStatus::Failure) => "probe-failed",
                    Some(PeerHealthStatus::Unavailable) => "unavailable",
                    Some(PeerHealthStatus::Unknown) | None => "unknown",
                };
                let outcome = match effective.latest_outcome(node_id) {
                    Some(PeerHealthObservationKind::Healthy) => "healthy",
                    Some(PeerHealthObservationKind::Failure) => "failure",
                    Some(PeerHealthObservationKind::CapacityExhausted) => "capacity-exhausted",
                    Some(PeerHealthObservationKind::Unscheduled) => "unscheduled",
                    None => "none",
                };
                let observation_age = match effective.observed_at(node_id) {
                    Some(observed_at) => match now.checked_duration_since(observed_at) {
                        Some(age) => format!("{age:?}"),
                        None => "unknown".to_string(),
                    },
                    None => "none".to_string(),
                };
                format!(
                    "- {node_id}: addr={} generation={} observation={outcome} \
                     observation_age={observation_age} status={status}",
                    peer.target.address(),
                    peer.target.endpoint_generation(),
                )
            })
            .collect()
    }

    fn subscribe_peer_health_state(&self) -> PeerHealthStateWatcher {
        PeerHealthStateWatcher {
            state: self.peer_health_state.subscribe(),
            observation_freshness: self.node_unavailability_timeout,
        }
    }

    pub(crate) fn node_unavailability_timeout(&self) -> Duration {
        self.node_unavailability_timeout
    }
}

fn subscription_interest_key(domain: &str, relay: &str) -> String {
    format!("{KEY_SUBSCRIPTION_INTEREST_PREFIX}{domain}:{relay}")
}

fn runtime_revision_is_ready(state: &NodeState, revision: u64) -> bool {
    let Some(value) = state.get(KEY_RUNTIME_REVISION_READY) else {
        return false;
    };
    let Ok(ready_revision) = value.parse::<u64>() else {
        return false;
    };
    ready_revision >= revision
}

fn advance_runtime_revision_readiness(state: &mut NodeState, revision: u64) {
    if runtime_revision_is_ready(state, revision) {
        return;
    }
    state.set(KEY_RUNTIME_REVISION_READY, revision.to_string());
}

fn cluster_node_identity(node_id: &ChitchatId) -> Option<ClusterNodeIdentity> {
    Some(ClusterNodeIdentity::new(
        ClusterNodeName::parse(&node_id.node_id).ok()?,
        ClusterNodeIncarnation::new(node_id.generation_id),
    ))
}

fn join_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}

fn render_node_state_lines(node_id: &ChitchatId, state: &NodeState) -> Vec<String> {
    vec![
        format!("- node_id: {}", node_id.node_id),
        format!("  gossip_addr: {}", node_id.gossip_advertise_addr),
        format!(
            "  grpc_listen_addr: {}",
            state.get(KEY_GRPC_LISTEN_ADDR).unwrap_or("<unknown>")
        ),
        format!(
            "  grpc_advertise_addr: {}",
            state.get(KEY_GRPC_ADVERTISE_ADDR).unwrap_or("<unknown>")
        ),
        format!(
            "  web_console_advertise_addr: {}",
            state
                .get(KEY_WEB_CONSOLE_ADVERTISE_ADDR)
                .unwrap_or("<unknown>")
        ),
        format!(
            "  interconnect_listen_addr: {}",
            state
                .get(KEY_INTERCONNECT_LISTEN_ADDR)
                .unwrap_or("<unknown>")
        ),
        format!(
            "  interconnect_advertise_addr: {}",
            state
                .get(KEY_INTERCONNECT_ADVERTISE_ADDR)
                .unwrap_or("<unknown>")
        ),
        format!(
            "  bootstrap_host: {}",
            match state.get(KEY_BOOTSTRAP_HOST) {
                Some(value) if !value.is_empty() => value,
                _ => "(none)",
            }
        ),
    ]
}

fn membership_report(nodes: &BTreeMap<ChitchatId, NodeState>) -> Vec<String> {
    nodes
        .iter()
        .map(|(id, state)| {
            let grpc_addr = state.get(KEY_GRPC_ADVERTISE_ADDR).unwrap_or("<unknown>");
            format!("{}@{}", id.node_id, grpc_addr)
        })
        .collect()
}

/// The peer `state` describes, or `None` while it is still incomplete.
///
/// Gossip converges field by field, so a peer that has not yet published every address, or has
/// published a node identity this build does not accept, is one to skip and read again on the next
/// round rather than one to report.
fn to_gossip_node(node_id: &ChitchatId, state: &NodeState) -> Option<GossipNode> {
    let identity = cluster_node_identity(node_id)?;
    let grpc_advertise_addr = state.get(KEY_GRPC_ADVERTISE_ADDR).unwrap_or("").to_string();
    let web_console_advertise_addr = state
        .get(KEY_WEB_CONSOLE_ADVERTISE_ADDR)
        .unwrap_or("")
        .to_string();
    let interconnect_advertise_addr = state
        .get(KEY_INTERCONNECT_ADVERTISE_ADDR)
        .unwrap_or("")
        .to_string();
    Some(GossipNode {
        node_id: identity.node_id().clone(),
        incarnation: identity.incarnation(),
        grpc_advertise_addr,
        web_console_advertise_addr,
        interconnect_advertise_addr,
    })
}

fn current_live_nodes(chitchat: &Chitchat) -> BTreeMap<ClusterNodeName, GossipNode> {
    let self_id = chitchat.self_chitchat_id();
    let mut live_nodes = BTreeMap::new();
    if let Some(state) = chitchat.node_state(self_id)
        && let Some(node) = to_gossip_node(self_id, state)
    {
        live_nodes.insert(node.node_id.clone(), node);
    }

    for node_id in chitchat.live_nodes() {
        if node_id == self_id {
            continue;
        }
        if let Some(state) = chitchat.node_state(node_id)
            && let Some(node) = to_gossip_node(node_id, state)
        {
            match live_nodes.entry(node.node_id.clone()) {
                std::collections::btree_map::Entry::Occupied(mut current) => {
                    if node.incarnation > current.get().incarnation {
                        current.insert(node);
                    }
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(node);
                }
            }
        }
    }
    live_nodes
}

pub fn derive_peer_addr(grpc_addr: SocketAddr) -> Option<SocketAddr> {
    let port = grpc_addr.port().checked_add(1)?;
    Some(SocketAddr::new(grpc_addr.ip(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health_identity(node: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(node).assured("the test node name is valid"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    fn health_endpoint(node: &str, incarnation: u64, address: &str) -> PeerHealthEndpoint {
        PeerHealthEndpoint::new(health_identity(node, incarnation), address.to_string())
    }

    #[test]
    fn derive_peer_addr_increments_port() {
        let grpc_addr: SocketAddr = "127.0.0.1:47391".parse().expect("valid socket addr");
        let gossip_addr = derive_peer_addr(grpc_addr).expect("must derive");
        let expected: SocketAddr = "127.0.0.1:47392".parse().unwrap();
        assert_eq!(gossip_addr, expected);
    }

    #[test]
    fn derive_peer_addr_returns_none_on_port_overflow() {
        let grpc_addr: SocketAddr = "127.0.0.1:65535".parse().expect("valid socket addr");
        assert!(derive_peer_addr(grpc_addr).is_none());
    }

    #[test]
    fn host_port_parses_hostname() {
        let parsed = "nervix-0.nervix-headless:47392"
            .parse::<HostPort>()
            .expect("host:port should parse");
        assert_eq!(parsed.to_string(), "nervix-0.nervix-headless:47392");
    }

    #[test]
    fn host_port_round_trips_ipv6_socket_addr() {
        let parsed = "[::1]:47392"
            .parse::<HostPort>()
            .expect("IPv6 socket address should parse");
        assert_eq!(parsed.to_string(), "[::1]:47392");
        assert_eq!(parsed.url_authority(), "[::1]:47392");
    }

    #[test]
    fn runtime_revision_readiness_advances_monotonically() {
        let mut state = NodeState::for_test();

        advance_runtime_revision_readiness(&mut state, 12);
        advance_runtime_revision_readiness(&mut state, 11);
        assert_eq!(state.get(KEY_RUNTIME_REVISION_READY), Some("12"));

        advance_runtime_revision_readiness(&mut state, 13);
        assert_eq!(state.get(KEY_RUNTIME_REVISION_READY), Some("13"));
    }

    #[test]
    fn peer_health_ignores_results_superseded_by_an_endpoint_change() {
        let timeout = Duration::from_secs(10);
        let started_at = Instant::now();
        let mut state = PeerHealthStateSnapshot::default();
        let superseded_target = state
            .replace_endpoints(
                vec![health_endpoint("node-2", 7, "node-2.example:7001")],
                started_at,
                timeout,
            )
            .into_iter()
            .next()
            .assured("one endpoint produces one probe target");
        let current_target = state
            .replace_endpoints(
                vec![health_endpoint("node-2", 8, "node-2.example:7002")],
                started_at,
                timeout,
            )
            .into_iter()
            .next()
            .assured("the replacement endpoint produces one probe target");
        assert!(current_target.endpoint_generation() > superseded_target.endpoint_generation());
        let revision_after_replacement = state.revision();

        assert_eq!(
            state.record_result(
                PeerHealthProbeResult::new(
                    superseded_target,
                    PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
                    started_at,
                ),
                timeout,
            ),
            PeerHealthResultDisposition::Superseded
        );
        assert_eq!(state.revision(), revision_after_replacement);
        assert_eq!(
            state
                .effective_snapshot(started_at, timeout)
                .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Unknown)
        );

        assert!(matches!(
            state.record_result(
                PeerHealthProbeResult::new(
                    current_target,
                    PeerHealthProbeOutcome::Healthy(health_identity("node-2", 8)),
                    started_at,
                ),
                timeout,
            ),
            PeerHealthResultDisposition::Published { .. }
        ));
        let healthy = state.effective_snapshot(started_at, timeout);
        let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        assert_eq!(healthy.status(&node), Some(PeerHealthStatus::Healthy));
        assert_eq!(healthy.observed_at(&node), Some(started_at));
    }

    #[test]
    fn repeated_equivalent_observations_do_not_invalidate_scheduling() {
        let freshness = Duration::from_secs(10);
        let started_at = Instant::now();
        let repeated_at = started_at
            .checked_add(Duration::from_secs(1))
            .assured("the test observation interval fits in the monotonic clock range");
        let mut state = PeerHealthStateSnapshot::default();
        let target = state
            .replace_endpoints(
                [health_endpoint("node-2", 7, "node-2.example:7001")],
                started_at,
                freshness,
            )
            .into_iter()
            .next()
            .assured("one endpoint produces one probe target");
        state.record_result(
            PeerHealthProbeResult::new(
                target.clone(),
                PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
                started_at,
            ),
            freshness,
        );
        let first = state.effective_snapshot(started_at, freshness);
        state.record_result(
            PeerHealthProbeResult::new(
                target,
                PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
                repeated_at,
            ),
            freshness,
        );
        let repeated = state.effective_snapshot(repeated_at, freshness);

        assert!(repeated.revision() > first.revision());
        assert_eq!(
            repeated.scheduling_revision(),
            first.scheduling_revision(),
            "a fresher observation with the same effective availability cannot stale a schedule"
        );
    }

    #[test]
    fn only_fresh_continuous_failures_become_unavailable() {
        let timeout = Duration::from_secs(10);
        let started_at = Instant::now();
        let second_observation_at = started_at
            .checked_add(Duration::from_secs(9))
            .assured("the test observation time fits in the monotonic clock range");
        let unavailable_at = started_at
            .checked_add(timeout)
            .assured("the test unavailability time fits in the monotonic clock range");
        let stale_at = second_observation_at
            .checked_add(timeout)
            .assured("the test stale time fits in the monotonic clock range");
        let mut state = PeerHealthStateSnapshot::default();
        let target = state
            .replace_endpoints(
                vec![health_endpoint("node-2", 7, "node-2.example:7001")],
                started_at,
                timeout,
            )
            .into_iter()
            .next()
            .assured("one endpoint produces one probe target");
        state.record_result(
            PeerHealthProbeResult::new(target.clone(), PeerHealthProbeOutcome::Failure, started_at),
            timeout,
        );
        state.record_result(
            PeerHealthProbeResult::new(
                target,
                PeerHealthProbeOutcome::Failure,
                second_observation_at,
            ),
            timeout,
        );

        let failed = state.effective_snapshot(second_observation_at, timeout);
        assert_eq!(
            failed.status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Failure)
        );
        let revision_before_deadline = failed.revision();
        let unavailable = state.effective_snapshot(unavailable_at, timeout);
        assert_eq!(
            unavailable
                .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Unavailable)
        );
        assert!(unavailable.revision() > revision_before_deadline);
        assert!(
            unavailable
                .unavailable_nodes()
                .contains(&ClusterNodeName::parse("node-2").assured("the test node name is valid"))
        );

        let stale = state.effective_snapshot(stale_at, timeout);
        assert_eq!(
            stale.status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Unknown)
        );
        assert!(stale.unavailable_nodes().is_empty());
    }

    #[test]
    fn capacity_and_unscheduled_results_break_a_failure_run_without_marking_the_peer_dead() {
        let timeout = Duration::from_secs(10);
        let started_at = Instant::now();
        let mut state = PeerHealthStateSnapshot::default();
        let target = state
            .replace_endpoints(
                vec![health_endpoint("node-2", 7, "node-2.example:7001")],
                started_at,
                timeout,
            )
            .into_iter()
            .next()
            .assured("one endpoint produces one probe target");
        state.record_result(
            PeerHealthProbeResult::new(target.clone(), PeerHealthProbeOutcome::Failure, started_at),
            timeout,
        );
        let capacity_at = started_at
            .checked_add(Duration::from_secs(9))
            .assured("the test capacity observation fits in the monotonic clock range");
        state.record_result(
            PeerHealthProbeResult::new(
                target.clone(),
                PeerHealthProbeOutcome::CapacityExhausted,
                capacity_at,
            ),
            timeout,
        );
        let original_failure_deadline = started_at
            .checked_add(timeout)
            .assured("the test original failure deadline fits in the monotonic clock range");
        let capacity = state.effective_snapshot(original_failure_deadline, timeout);
        assert_eq!(
            capacity
                .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Unknown)
        );
        assert_eq!(
            capacity.latest_outcome(
                &ClusterNodeName::parse("node-2").assured("the test node name is valid")
            ),
            Some(PeerHealthObservationKind::CapacityExhausted)
        );
        assert!(capacity.unavailable_nodes().is_empty());

        let unscheduled_at = capacity_at
            .checked_add(Duration::from_secs(1))
            .assured("the test unscheduled observation fits in the monotonic clock range");
        state.record_result(
            PeerHealthProbeResult::new(target, PeerHealthProbeOutcome::Unscheduled, unscheduled_at),
            timeout,
        );
        let unscheduled = state.effective_snapshot(unscheduled_at, timeout);
        assert_eq!(
            unscheduled
                .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
            Some(PeerHealthStatus::Unknown)
        );
        assert_eq!(
            unscheduled.latest_outcome(
                &ClusterNodeName::parse("node-2").assured("the test node name is valid")
            ),
            Some(PeerHealthObservationKind::Unscheduled)
        );
        assert!(unscheduled.unavailable_nodes().is_empty());
    }

    #[tokio::test]
    async fn cluster_state_watcher_observes_a_health_target_change_after_wait_preparation() {
        let observation_freshness = Duration::from_secs(10);
        let (_live_state, live_state_receiver) = watch::channel(BTreeMap::new());
        let (peer_health_state, peer_health_state_receiver) =
            watch::channel(PeerHealthStateSnapshot::default());
        let mut watcher = ClusterStateWatcher {
            live_node_states: live_state_receiver,
            peer_health_state: PeerHealthStateWatcher {
                state: peer_health_state_receiver,
                observation_freshness,
            },
        };
        let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());
        peer_health_state.send_modify(|snapshot| {
            snapshot.replace_endpoints(
                [health_endpoint("node-2", 7, "node-2.example:7001")],
                Instant::now(),
                observation_freshness,
            );
        });

        tokio::select! {
            biased;
            _ = &mut waiting => {}
            () = tokio::task::yield_now() => {
                panic!("a health target update after wait preparation must wake the waiter")
            }
        }
    }
}
