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
    Chitchat, ChitchatHandle, ChitchatId, NodeState, spawn_chitchat,
    transport::{Socket, Transport, UdpSocket},
};
use nervix_consensus::{GossipNode, GossipState};
use parking_lot::{Mutex, RwLock};
use tokio::{net::lookup_host, sync::broadcast, task::JoinHandle};
use tokio_stream::StreamExt;
use tracing::{error, info};

const KEY_CLUSTER_ID: &str = "cluster_id";
const KEY_NODE_ID: &str = "node_id";
const KEY_CLUSTER_LISTEN_ADDR: &str = "cluster_listen_addr";
const KEY_CLUSTER_ADVERTISE_ADDR: &str = "cluster_advertise_addr";
const KEY_GRPC_LISTEN_ADDR: &str = "grpc_listen_addr";
const KEY_GRPC_ADVERTISE_ADDR: &str = "grpc_advertise_addr";
const KEY_WEB_CONSOLE_ADVERTISE_ADDR: &str = "web_console_advertise_addr";
const KEY_CLUSTER_API_LISTEN_ADDR: &str = "cluster_api_listen_addr";
const KEY_CLUSTER_API_ADVERTISE_ADDR: &str = "cluster_api_advertise_addr";
const KEY_INTERCONNECT_LISTEN_ADDR: &str = "interconnect_listen_addr";
const KEY_INTERCONNECT_ADVERTISE_ADDR: &str = "interconnect_advertise_addr";
const KEY_INTERCONNECT_MODE: &str = "interconnect_mode";
const KEY_INTERCONNECT_PUBLIC_KEY: &str = "interconnect_public_key";
const KEY_BOOTSTRAP_HOST: &str = "bootstrap_host";
const KEY_SUBSCRIPTION_INTEREST_PREFIX: &str = "subscription_interest:";

pub struct ClusterHandle {
    chitchat: Arc<tokio::sync::Mutex<Chitchat>>,
    chitchat_server: Mutex<Option<ChitchatHandle>>,
    events: broadcast::Sender<String>,
    interconnect_state: RwLock<BTreeMap<String, InterconnectPeerState>>,
    node_unavailability_timeout: Duration,
    membership_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
struct InterconnectPeerState {
    target_addr: Option<String>,
    connected: bool,
    unavailable_since: Option<Instant>,
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

#[derive(Debug, Clone)]
pub struct ClusterSettings {
    pub cluster_id: String,
    pub node_id: String,
    pub cluster_listen_addr: SocketAddr,
    pub cluster_advertise_addr: HostPort,
    pub grpc_listen_addr: SocketAddr,
    pub grpc_advertise_addr: String,
    pub web_console_advertise_addr: String,
    pub cluster_api_listen_addr: SocketAddr,
    pub cluster_api_advertise_addr: String,
    pub interconnect_listen_addr: SocketAddr,
    pub interconnect_advertise_addr: HostPort,
    pub interconnect_mode: String,
    pub interconnect_public_key: String,
    pub bootstrap_host: Option<String>,
    pub node_unavailability_timeout: Duration,
}

pub async fn bind_gossip_transport(listen_addr: SocketAddr) -> io::Result<PreboundUdpTransport> {
    let socket = UdpSocket::open(listen_addr)
        .await
        .map_err(io::Error::other)?;
    Ok(PreboundUdpTransport {
        listen_addr,
        socket: Mutex::new(Some(socket)),
    })
}

pub struct PreboundUdpTransport {
    listen_addr: SocketAddr,
    socket: Mutex<Option<UdpSocket>>,
}

#[async_trait]
impl Transport for PreboundUdpTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        if listen_addr != self.listen_addr {
            anyhow::bail!(
                "prebound UDP transport was opened for {listen_addr}, expected {}",
                self.listen_addr
            );
        }
        let socket = match self.socket.lock().take() {
            Some(socket) => socket,
            None => anyhow::bail!("prebound UDP transport for {listen_addr} was already opened"),
        };
        Ok(Box::new(socket))
    }
}

pub async fn start_cluster(settings: ClusterSettings) -> io::Result<ClusterHandle> {
    let transport = bind_gossip_transport(settings.cluster_listen_addr).await?;
    start_cluster_with_transport(settings, &transport).await
}

pub async fn start_cluster_with_transport(
    settings: ClusterSettings,
    transport: &dyn Transport,
) -> io::Result<ClusterHandle> {
    let node_id = settings.node_id.clone();
    let generation_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let gossip_advertise_addr = settings.cluster_advertise_addr.resolve_one().await?;
    let seed_nodes = match settings.bootstrap_host.as_deref() {
        Some(seed) => match seed.parse::<HostPort>() {
            Ok(seed) => seed
                .resolve_all()
                .await?
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            Err(_) => vec![seed.to_string()],
        },
        None => Vec::new(),
    };
    let chitchat_id = ChitchatId {
        node_id: node_id.clone(),
        generation_id,
        gossip_advertise_addr,
    };

    let config = chitchat::ChitchatConfig {
        chitchat_id,
        cluster_id: settings.cluster_id.clone(),
        gossip_interval: Duration::from_millis(500),
        listen_addr: settings.cluster_listen_addr,
        seed_nodes,
        failure_detector_config: chitchat::FailureDetectorConfig::default(),
        marked_for_deletion_grace_period: Duration::from_secs(60),
        catchup_callback: None,
        extra_liveness_predicate: None,
    };

    let initial_key_values = vec![
        (KEY_CLUSTER_ID.to_string(), settings.cluster_id.clone()),
        (KEY_NODE_ID.to_string(), node_id),
        (
            KEY_CLUSTER_LISTEN_ADDR.to_string(),
            settings.cluster_listen_addr.to_string(),
        ),
        (
            KEY_CLUSTER_ADVERTISE_ADDR.to_string(),
            settings.cluster_advertise_addr.to_string(),
        ),
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
            KEY_CLUSTER_API_LISTEN_ADDR.to_string(),
            settings.cluster_api_listen_addr.to_string(),
        ),
        (
            KEY_CLUSTER_API_ADVERTISE_ADDR.to_string(),
            settings.cluster_api_advertise_addr.clone(),
        ),
        (
            KEY_INTERCONNECT_LISTEN_ADDR.to_string(),
            settings.interconnect_listen_addr.to_string(),
        ),
        (
            KEY_INTERCONNECT_ADVERTISE_ADDR.to_string(),
            settings.interconnect_advertise_addr.to_string(),
        ),
        (
            KEY_INTERCONNECT_MODE.to_string(),
            settings.interconnect_mode.clone(),
        ),
        (
            KEY_INTERCONNECT_PUBLIC_KEY.to_string(),
            settings.interconnect_public_key.clone(),
        ),
        (
            KEY_BOOTSTRAP_HOST.to_string(),
            settings.bootstrap_host.clone().unwrap_or_default(),
        ),
    ];

    let chitchat = spawn_chitchat(config, initial_key_values, transport)
        .await
        .map_err(|err| io::Error::other(format!("failed to start chitchat: {err}")))?;

    let (events, _) = broadcast::channel(256);
    let chitchat_state = chitchat.chitchat();
    let mut live_nodes = chitchat_state.lock().await.live_nodes_watch_stream();
    let event_tx = events.clone();
    let membership_task = tokio::spawn(async move {
        while let Some(nodes) = live_nodes.next().await {
            tokio::task::consume_budget().await;
            let report = membership_report(&nodes);
            info!(members = ?report, "cluster membership updated");
            let _ = event_tx.send(format!("cluster membership updated: {}", report.join(", ")));
        }
    });

    Ok(ClusterHandle {
        chitchat: chitchat_state,
        chitchat_server: Mutex::new(Some(chitchat)),
        events,
        interconnect_state: RwLock::new(BTreeMap::new()),
        node_unavailability_timeout: settings.node_unavailability_timeout,
        membership_task: Mutex::new(Some(membership_task)),
    })
}

impl ClusterHandle {
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

    pub async fn status_lines(&self) -> Vec<String> {
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();
        let cluster_id = chitchat.cluster_id().to_string();
        let self_state = chitchat.node_state(&self_id).cloned();
        let live_nodes = chitchat
            .live_nodes()
            .filter(|node_id| **node_id != self_id)
            .filter_map(|node_id| {
                chitchat
                    .node_state(node_id)
                    .cloned()
                    .map(|state| (node_id.clone(), state))
            })
            .collect::<BTreeMap<_, _>>();
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

        let mut live_nodes = Vec::new();
        if let Some(state) = chitchat.node_state(&self_id)
            && let Some(node) = to_gossip_node(&self_id, state)
        {
            live_nodes.push(node);
        }

        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
                continue;
            }
            if let Some(state) = chitchat.node_state(node_id)
                && let Some(node) = to_gossip_node(node_id, state)
            {
                live_nodes.push(node);
            }
        }
        let live_node_ids = live_nodes
            .iter()
            .map(|node| node.node_id.clone())
            .collect::<BTreeSet<_>>();

        let mut dead_node_ids = chitchat
            .dead_nodes()
            .map(|node_id| node_id.node_id.clone())
            .filter(|node_id| !live_node_ids.contains(node_id))
            .collect::<BTreeSet<_>>();
        dead_node_ids.extend(self.unavailable_interconnect_nodes());

        live_nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        GossipState {
            live_nodes,
            dead_node_ids,
        }
    }

    pub async fn live_node_ids(&self) -> Vec<String> {
        self.gossip_state()
            .await
            .live_nodes
            .into_iter()
            .map(|node| node.node_id)
            .collect()
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
    ) -> BTreeSet<String> {
        let key = subscription_interest_key(domain, relay);
        let chitchat_handle = self.chitchat.clone();
        let chitchat = chitchat_handle.lock().await;
        let self_id = chitchat.self_chitchat_id().clone();
        let mut interested = BTreeSet::new();

        if let Some(state) = chitchat.node_state(&self_id)
            && state.get(&key).is_some()
        {
            interested.insert(self_id.node_id.clone());
        }

        for node_id in chitchat.live_nodes() {
            if *node_id == self_id {
                continue;
            }
            if let Some(state) = chitchat.node_state(node_id)
                && state.get(&key).is_some()
            {
                interested.insert(node_id.node_id.clone());
            }
        }

        interested
    }

    pub fn record_interconnect_connected(&self, node_id: &str, target_addr: String) {
        let mut peers = self.interconnect_state.write();
        let entry = peers
            .entry(node_id.to_string())
            .or_insert_with(|| InterconnectPeerState {
                target_addr: Some(target_addr.clone()),
                connected: false,
                unavailable_since: None,
            });
        entry.target_addr = Some(target_addr.clone());
        let was_connected = entry.connected;
        let was_unavailable = entry.unavailable_since.is_some();
        entry.connected = true;
        entry.unavailable_since = None;

        if !was_connected {
            info!(node_id, target_addr, "interconnect connection established");
            let _ = self.events.send(format!(
                "interconnect connection established: {node_id}@{target_addr}"
            ));
        } else if was_unavailable {
            info!(node_id, target_addr, "interconnect connection restored");
            let _ = self.events.send(format!(
                "interconnect connection restored: {node_id}@{target_addr}"
            ));
        }
    }

    pub fn record_interconnect_failure(&self, node_id: &str, target_addr: Option<String>) {
        let mut peers = self.interconnect_state.write();
        let entry = peers
            .entry(node_id.to_string())
            .or_insert_with(|| InterconnectPeerState {
                target_addr: target_addr.clone(),
                connected: false,
                unavailable_since: None,
            });
        if let Some(target_addr) = target_addr {
            entry.target_addr = Some(target_addr);
        }
        let was_connected = entry.connected;
        let was_unavailable = entry.unavailable_since.is_some();
        entry.connected = false;
        if entry.unavailable_since.is_none() {
            entry.unavailable_since = Some(Instant::now());
        }
        if was_connected || !was_unavailable {
            error!(
                node_id,
                target_addr = entry.target_addr.as_deref().unwrap_or("<unknown>"),
                "interconnect connection establishment failed"
            );
        }
    }

    pub fn retain_interconnect_live_set(&self, live_node_ids: &BTreeSet<String>) {
        self.interconnect_state
            .write()
            .retain(|node_id, _| live_node_ids.contains(node_id));
    }

    fn interconnect_status_lines(&self) -> Vec<String> {
        let peers = self.interconnect_state.read();
        if peers.is_empty() {
            return vec!["- (none)".to_string()];
        }

        let now = Instant::now();
        peers
            .iter()
            .map(|(node_id, state)| {
                let status = if state.connected {
                    "connected".to_string()
                } else if let Some(since) = state.unavailable_since {
                    let elapsed = now.saturating_duration_since(since);
                    if elapsed >= self.node_unavailability_timeout {
                        format!("unavailable for {}", humantime::format_duration(elapsed))
                    } else {
                        format!("connecting for {}", humantime::format_duration(elapsed))
                    }
                } else {
                    "connecting".to_string()
                };
                format!(
                    "- {node_id}: addr={} status={status}",
                    state.target_addr.as_deref().unwrap_or("<unknown>")
                )
            })
            .collect()
    }

    fn unavailable_interconnect_nodes(&self) -> BTreeSet<String> {
        let peers = self.interconnect_state.read();
        let now = Instant::now();
        peers
            .iter()
            .filter_map(|(node_id, state)| {
                let since = state.unavailable_since?;
                let elapsed = now.saturating_duration_since(since);
                (elapsed >= self.node_unavailability_timeout).then(|| node_id.clone())
            })
            .collect()
    }

    pub fn is_interconnect_unavailable(&self, node_id: &str) -> bool {
        self.unavailable_interconnect_nodes().contains(node_id)
    }
}

fn subscription_interest_key(domain: &str, relay: &str) -> String {
    format!("{KEY_SUBSCRIPTION_INTEREST_PREFIX}{domain}:{relay}")
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
        format!("  cluster_addr: {}", node_id.gossip_advertise_addr),
        format!(
            "  cluster_listen_addr: {}",
            state.get(KEY_CLUSTER_LISTEN_ADDR).unwrap_or("<unknown>")
        ),
        format!(
            "  cluster_advertise_addr: {}",
            state.get(KEY_CLUSTER_ADVERTISE_ADDR).unwrap_or("<unknown>")
        ),
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
            "  cluster_api_listen_addr: {}",
            state
                .get(KEY_CLUSTER_API_LISTEN_ADDR)
                .unwrap_or("<unknown>")
        ),
        format!(
            "  cluster_api_advertise_addr: {}",
            state
                .get(KEY_CLUSTER_API_ADVERTISE_ADDR)
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
            "  interconnect_mode: {}",
            state.get(KEY_INTERCONNECT_MODE).unwrap_or("<unknown>")
        ),
        format!(
            "  interconnect_public_key: {}",
            state
                .get(KEY_INTERCONNECT_PUBLIC_KEY)
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

fn to_gossip_node(node_id: &ChitchatId, state: &NodeState) -> Option<GossipNode> {
    let cluster_api_advertise_addr = state.get(KEY_CLUSTER_API_ADVERTISE_ADDR)?.to_string();
    let grpc_advertise_addr = state.get(KEY_GRPC_ADVERTISE_ADDR).unwrap_or("").to_string();
    let web_console_advertise_addr = state
        .get(KEY_WEB_CONSOLE_ADVERTISE_ADDR)
        .unwrap_or("")
        .to_string();
    let interconnect_advertise_addr = state
        .get(KEY_INTERCONNECT_ADVERTISE_ADDR)
        .unwrap_or("")
        .to_string();
    let interconnect_mode = state
        .get(KEY_INTERCONNECT_MODE)
        .unwrap_or("http")
        .to_string();
    let interconnect_public_key = state
        .get(KEY_INTERCONNECT_PUBLIC_KEY)
        .unwrap_or("")
        .to_string();
    Some(GossipNode {
        node_id: node_id.node_id.clone(),
        cluster_api_advertise_addr,
        grpc_advertise_addr,
        web_console_advertise_addr,
        interconnect_advertise_addr,
        interconnect_mode,
        interconnect_public_key,
    })
}

pub fn derive_peer_addr(grpc_addr: SocketAddr) -> Option<SocketAddr> {
    let port = grpc_addr.port().checked_add(1)?;
    Some(SocketAddr::new(grpc_addr.ip(), port))
}

pub fn derive_interconnect_host_port(cluster_api_addr: &HostPort) -> Option<HostPort> {
    let port = cluster_api_addr.port().checked_add(1)?;
    Some(cluster_api_addr.with_port(port))
}

pub fn derive_interconnect_addr(cluster_api_addr: SocketAddr) -> Option<SocketAddr> {
    let port = cluster_api_addr.port().checked_add(1)?;
    Some(SocketAddr::new(cluster_api_addr.ip(), port))
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
    fn derive_interconnect_addr_increments_raft_port() {
        let raft_addr: SocketAddr = "127.0.0.1:47393".parse().expect("valid socket addr");
        let interconnect_addr =
            derive_interconnect_addr(raft_addr).expect("must derive interconnect addr");
        let expected: SocketAddr = "127.0.0.1:47394".parse().unwrap();
        assert_eq!(interconnect_addr, expected);
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
    fn derive_interconnect_host_port_preserves_host() {
        let host_port = "nervix-0.nervix-headless:47393"
            .parse::<HostPort>()
            .expect("host:port should parse");
        let derived = derive_interconnect_host_port(&host_port).expect("must derive");
        assert_eq!(derived.to_string(), "nervix-0.nervix-headless:47394");
    }
}
