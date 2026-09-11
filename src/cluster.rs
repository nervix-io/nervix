//! Who is in the cluster, and how to reach them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Gossip membership: the node's advertised addresses, peer set, liveness, and the
//!   adapter that carries Chitchat exchanges on the interconnect management pool.
//! - **Depends on.** `chitchat`, authenticated HTTP/2 interconnect requests, the vocabulary's node
//!   names, and the gossip view types consensus reconciles membership from.
//! - **Must not know.** Domains, graphs, schedules or the runtime. Membership is the whole answer
//!   it gives.

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
use tracing::{error, info};

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
    interconnect_state: watch::Sender<InterconnectStateSnapshot>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum InterconnectPeerState {
    Connected {
        target_addr: String,
    },
    Disconnected {
        target_addr: Option<String>,
        since: Instant,
    },
}

impl InterconnectPeerState {
    fn target_addr(&self) -> Option<&str> {
        match self {
            Self::Connected { target_addr } => Some(target_addr),
            Self::Disconnected { target_addr, .. } => target_addr.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct InterconnectStateSnapshot {
    peers: BTreeMap<ClusterNodeName, InterconnectPeerState>,
}

impl InterconnectStateSnapshot {
    fn next_unavailability_deadline(&self, timeout: Duration, now: Instant) -> Option<Instant> {
        let mut earliest = None;
        for peer in self.peers.values() {
            let InterconnectPeerState::Disconnected { since, .. } = peer else {
                continue;
            };
            let Some(deadline) = since.checked_add(timeout) else {
                // This configured threshold has no reachable deadline on the platform's
                // monotonic clock, so it cannot cause a future eligibility transition.
                continue;
            };
            if deadline <= now {
                continue;
            }
            match earliest {
                Some(current) if current <= deadline => {}
                _ => earliest = Some(deadline),
            }
        }
        earliest
    }
}

struct InterconnectStateWatcher {
    state: watch::Receiver<InterconnectStateSnapshot>,
    unavailability_timeout: Duration,
}

impl InterconnectStateWatcher {
    fn wait_for_change_or_next_unavailability(
        &mut self,
    ) -> impl std::future::Future<Output = ()> + '_ {
        let next_unavailability = {
            self.state
                .borrow()
                .next_unavailability_deadline(self.unavailability_timeout, Instant::now())
        };
        async move {
            tokio::select! {
                changed = self.state.changed() => changed.assured(
                    "the cluster handle retains its interconnect state sender for its lifetime",
                ),
                _ = async {
                    match next_unavailability {
                        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
            }
        }
    }
}

/// Retained inputs whose changes can alter the node's effective cluster view.
///
/// Interconnect failure becomes effective after an elapsed monotonic deadline without changing
/// either retained input. The watcher owns that deadline so consumers can re-evaluate cluster
/// state without sampling it on an interval.
pub(crate) struct ClusterStateWatcher {
    live_node_states: watch::Receiver<BTreeMap<ChitchatId, NodeState>>,
    interconnect_state: InterconnectStateWatcher,
}

impl ClusterStateWatcher {
    pub(crate) fn wait_for_change_or_next_unavailability(
        &mut self,
    ) -> impl std::future::Future<Output = ()> + '_ {
        let Self {
            live_node_states,
            interconnect_state,
        } = self;
        let interconnect_change = interconnect_state.wait_for_change_or_next_unavailability();
        async move {
            tokio::select! {
                changed = live_node_states.changed() => changed.assured(
                    "the cluster handle retains its Chitchat state sender for its lifetime",
                ),
                _ = interconnect_change => {}
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
        interconnect_state: watch::channel(InterconnectStateSnapshot::default()).0,
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
            interconnect_state: self.subscribe_interconnect_state(),
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
        self.interconnect_status_lines()
    }

    pub async fn gossip_state(&self) -> GossipState {
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();

        let mut live_nodes = BTreeMap::new();
        if let Some(state) = chitchat.node_state(&self_id)
            && let Some(node) = to_gossip_node(&self_id, state)
        {
            live_nodes.insert(node.node_id.clone(), node);
        }

        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
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
        let live_node_ids = live_nodes.keys().cloned().collect::<BTreeSet<_>>();

        let mut dead_node_ids = chitchat
            .dead_nodes()
            .filter_map(|node_id| ClusterNodeName::parse(&node_id.node_id).ok())
            .filter(|node_id| !live_node_ids.contains(node_id))
            .collect::<BTreeSet<_>>();
        dead_node_ids.extend(self.unavailable_interconnect_nodes());

        GossipState {
            live_nodes: live_nodes.into_values().collect(),
            dead_node_ids,
        }
    }

    pub async fn live_node_ids(&self) -> Vec<ClusterNodeName> {
        self.gossip_state()
            .await
            .live_nodes
            .into_iter()
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

    pub fn record_interconnect_connected(&self, node_id: &ClusterNodeName, target_addr: String) {
        let mut previous_state = None;
        self.interconnect_state.send_if_modified(|snapshot| {
            let next = InterconnectPeerState::Connected {
                target_addr: target_addr.clone(),
            };
            previous_state = snapshot.peers.get(node_id).cloned();
            let modified = previous_state.as_ref() != Some(&next);
            snapshot.peers.insert(node_id.clone(), next);
            modified
        });

        match previous_state {
            Some(InterconnectPeerState::Disconnected { .. }) => {
                info!(%node_id, target_addr, "interconnect connection restored");
                self.events.offer(format!(
                    "interconnect connection restored: {node_id}@{target_addr}"
                ));
            }
            None => {
                info!(%node_id, target_addr, "interconnect connection established");
                self.events.offer(format!(
                    "interconnect connection established: {node_id}@{target_addr}"
                ));
            }
            Some(InterconnectPeerState::Connected { .. }) => {}
        }
    }

    pub fn record_interconnect_failure(
        &self,
        node_id: &ClusterNodeName,
        target_addr: Option<String>,
    ) {
        let now = Instant::now();
        let mut previous_state = None;
        let mut recorded_target_addr = None;
        self.interconnect_state.send_if_modified(|snapshot| {
            previous_state = snapshot.peers.get(node_id).cloned();
            recorded_target_addr = match target_addr.as_ref() {
                Some(target_addr) => Some(target_addr.clone()),
                None => match previous_state.as_ref() {
                    Some(previous) => previous.target_addr().map(str::to_owned),
                    None => None,
                },
            };
            let since = match previous_state.as_ref() {
                Some(InterconnectPeerState::Disconnected { since, .. }) => *since,
                Some(InterconnectPeerState::Connected { .. }) | None => now,
            };
            let next = InterconnectPeerState::Disconnected {
                target_addr: recorded_target_addr.clone(),
                since,
            };
            let modified = previous_state.as_ref() != Some(&next);
            snapshot.peers.insert(node_id.clone(), next);
            modified
        });
        match previous_state {
            Some(InterconnectPeerState::Disconnected { .. }) => {}
            Some(InterconnectPeerState::Connected { .. }) | None => {
                error!(
                    %node_id,
                    target_addr = recorded_target_addr.as_deref().unwrap_or("<unknown>"),
                    "interconnect connection establishment failed"
                );
            }
        }
    }

    pub fn retain_interconnect_live_set(&self, live_node_ids: &BTreeSet<ClusterNodeName>) {
        self.interconnect_state.send_if_modified(|snapshot| {
            let previous_len = snapshot.peers.len();
            snapshot
                .peers
                .retain(|node_id, _| live_node_ids.contains(node_id));
            snapshot.peers.len() != previous_len
        });
    }

    fn interconnect_status_lines(&self) -> Vec<String> {
        let snapshot = self.interconnect_state.borrow();
        if snapshot.peers.is_empty() {
            return vec!["- (none)".to_string()];
        }

        let now = Instant::now();
        snapshot
            .peers
            .iter()
            .map(|(node_id, state)| {
                let status = match state {
                    InterconnectPeerState::Connected { .. } => "connected".to_string(),
                    InterconnectPeerState::Disconnected { since, .. } => {
                        let elapsed = now.checked_duration_since(*since).assured(
                            "an interconnect failure is recorded before its status is observed",
                        );
                        if elapsed >= self.node_unavailability_timeout {
                            format!("unavailable for {}", humantime::format_duration(elapsed))
                        } else {
                            format!("connecting for {}", humantime::format_duration(elapsed))
                        }
                    }
                };
                format!(
                    "- {node_id}: addr={} status={status}",
                    state.target_addr().unwrap_or("<unknown>")
                )
            })
            .collect()
    }

    fn unavailable_interconnect_nodes(&self) -> BTreeSet<ClusterNodeName> {
        let snapshot = self.interconnect_state.borrow();
        let now = Instant::now();
        let mut unavailable = BTreeSet::new();
        for (node_id, state) in &snapshot.peers {
            let InterconnectPeerState::Disconnected { since, .. } = state else {
                continue;
            };
            let elapsed = now
                .checked_duration_since(*since)
                .assured("an interconnect failure is recorded before its availability is observed");
            if elapsed >= self.node_unavailability_timeout {
                unavailable.insert(node_id.clone());
            }
        }
        unavailable
    }

    pub fn is_interconnect_unavailable(&self, node_id: &ClusterNodeName) -> bool {
        self.unavailable_interconnect_nodes().contains(node_id)
    }

    fn subscribe_interconnect_state(&self) -> InterconnectStateWatcher {
        InterconnectStateWatcher {
            state: self.interconnect_state.subscribe(),
            unavailability_timeout: self.node_unavailability_timeout,
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

pub fn derive_peer_addr(grpc_addr: SocketAddr) -> Option<SocketAddr> {
    let port = grpc_addr.port().checked_add(1)?;
    Some(SocketAddr::new(grpc_addr.ip(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn interconnect_snapshot_reports_earliest_pending_unavailability() {
        let earlier_failure = Instant::now();
        let failure_spacing = Duration::from_secs(2);
        let observation_delay = Duration::from_secs(4);
        let unavailability_timeout = Duration::from_secs(10);
        let later_failure = earlier_failure
            .checked_add(failure_spacing)
            .assured("the test failure spacing fits in the monotonic clock range");
        let now = earlier_failure
            .checked_add(observation_delay)
            .assured("the test observation delay fits in the monotonic clock range");
        let expected = earlier_failure
            .checked_add(unavailability_timeout)
            .assured("the test timeout fits in the monotonic clock range");
        let snapshot = InterconnectStateSnapshot {
            peers: BTreeMap::from([
                (
                    ClusterNodeName::parse("node-2").assured("the test node name is valid"),
                    InterconnectPeerState::Disconnected {
                        target_addr: None,
                        since: later_failure,
                    },
                ),
                (
                    ClusterNodeName::parse("node-3").assured("the test node name is valid"),
                    InterconnectPeerState::Disconnected {
                        target_addr: None,
                        since: earlier_failure,
                    },
                ),
            ]),
        };

        assert_eq!(
            snapshot.next_unavailability_deadline(unavailability_timeout, now),
            Some(expected)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn interconnect_watcher_reports_an_existing_change_once_before_waiting_for_deadline() {
        let unavailability_timeout = Duration::from_secs(10);
        let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let (state, receiver) = watch::channel(InterconnectStateSnapshot::default());
        state.send_modify(|snapshot| {
            snapshot.peers.insert(
                node,
                InterconnectPeerState::Disconnected {
                    target_addr: None,
                    since: Instant::now(),
                },
            );
        });
        let mut watcher = InterconnectStateWatcher {
            state: receiver,
            unavailability_timeout,
        };
        let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());

        tokio::select! {
            biased;
            _ = &mut waiting => {}
            () = tokio::task::yield_now() => {
                panic!("an unseen watch update must wake the waiter once")
            }
        }
        drop(waiting);
        let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());

        tokio::select! {
            biased;
            _ = &mut waiting => panic!("a reported watch update must not cause a busy wake"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(unavailability_timeout).await;
        waiting.await;
        drop(state);
    }

    #[tokio::test(start_paused = true)]
    async fn interconnect_watcher_observes_a_change_after_wait_preparation() {
        let unavailability_timeout = Duration::from_secs(10);
        let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let (state, receiver) = watch::channel(InterconnectStateSnapshot::default());
        let mut watcher = InterconnectStateWatcher {
            state: receiver,
            unavailability_timeout,
        };
        let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());
        state.send_modify(|snapshot| {
            snapshot.peers.insert(
                node,
                InterconnectPeerState::Disconnected {
                    target_addr: None,
                    since: Instant::now(),
                },
            );
        });

        tokio::select! {
            biased;
            _ = &mut waiting => {}
            () = tokio::task::yield_now() => {
                panic!("a watch update after wait preparation must wake the waiter")
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_state_watcher_wakes_when_interconnect_unavailability_becomes_effective() {
        let unavailability_timeout = Duration::from_secs(10);
        let before_deadline = unavailability_timeout / 2;
        let remaining = unavailability_timeout
            .checked_sub(before_deadline)
            .assured("half of a positive timeout leaves a representable remainder");
        let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
        let (_live_state, live_state_receiver) = watch::channel(BTreeMap::new());
        let (interconnect_state, interconnect_state_receiver) =
            watch::channel(InterconnectStateSnapshot::default());
        interconnect_state.send_modify(|snapshot| {
            snapshot.peers.insert(
                node,
                InterconnectPeerState::Disconnected {
                    target_addr: None,
                    since: Instant::now(),
                },
            );
        });
        let mut watcher = ClusterStateWatcher {
            live_node_states: live_state_receiver,
            interconnect_state: InterconnectStateWatcher {
                state: interconnect_state_receiver,
                unavailability_timeout,
            },
        };
        watcher.wait_for_change_or_next_unavailability().await;
        let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());

        tokio::time::advance(before_deadline).await;
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("the effective unavailability deadline has not elapsed"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(remaining).await;
        waiting.await;
        drop(interconnect_state);
    }
}
