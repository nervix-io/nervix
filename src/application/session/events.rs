//! What a session is told without asking.
//!
//! Layer: edges.
//!
//! - **Owns.** The unsolicited events of one session and when they are sent: server notices and
//!   runtime errors, leadership as the serving node observes it, the domain list, and the cluster
//!   summary and live snapshot of the domain the session selected. It also ends a console session
//!   whose node does not lead.
//! - **Depends on.** Consensus observation, the registry, resource catalog and runtime statistics
//!   a snapshot reads, the bulk workers a snapshot is serialized on, and the client wire contract.
//! - **Must not know.** How a transport carries frames, or anything about requests.
//!
//! Every session receives leadership first, then the domain list, then both again whenever they
//! change. Observations of one domain are opt-in: a session that selected a domain receives its
//! cluster summary and snapshot at once and then periodically, and a session that selected none
//! receives neither.

use std::num::NonZeroU64;

use arch_into::ArchInto as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    ClusterObserved, DomainEntity, DomainInfo, DomainSnapshotObserved, DomainsObserved,
    EncodedFrame, LeaderRedirect as WireLeaderRedirect, Leadership, LeadershipObserved,
    NoticeLevel, ServerFrame, ServerNotice, SessionEndReason, SessionLimits,
};
use nervix_dataflow_graph::DataflowGraph;
use nervix_execution::{CpuClass, MemoryClass};
use nervix_models::{DomainName, DomainStatus, RelayName, RequestedResourceVersion};
use tokio::{
    sync::broadcast::error::RecvError,
    time::{Duration, MissedTickBehavior, interval},
};
use tracing::{debug, warn};
use triomphe::Arc;

use super::{SessionShared, SessionTransport, outcome::leader_endpoints};
use crate::{
    application::{observation::dataflow_metric_target, session_service::SessionServiceImpl},
    runtime::RuntimeEvent,
};

/// How often a session checks which node leads.
const LEADERSHIP_CHECK_INTERVAL: Duration = Duration::from_millis(250);

/// How often a session that selected a domain receives its snapshot.
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);

/// Sends a session its unsolicited events until the session ends.
pub(super) async fn run_session_events(shared: Arc<SessionShared>) {
    let service = shared.service.clone();
    let mut notices = service.inner.events.subscribe();
    let mut runtime_events = service.inner.runtime.subscribe_events();
    let mut domains = service.inner.consensus.subscribe_domains();
    let mut selection = shared.selection.subscribe();
    let mut leadership_check = interval(LEADERSHIP_CHECK_INTERVAL);
    leadership_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut snapshots = interval(SNAPSHOT_INTERVAL);
    snapshots.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut leadership = service.observed_leadership().await;
    if !shared.announce_leadership(&leadership).await {
        return;
    }
    if !shared.send_domains().await {
        return;
    }
    leadership_check.reset();

    loop {
        tokio::task::consume_budget().await;
        tokio::select! {
            biased;
            _ = shared.ended() => return,
            notice = notices.recv() => match notice {
                Ok(notice) => {
                    if !shared.send_notice(notice).await {
                        return;
                    }
                }
                // The session resumes from the newest notice. Saying how many it skipped is what
                // stops the gap from looking like quiet.
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "a session fell behind the server notice bus");
                }
                Err(RecvError::Closed) => return,
            },
            event = runtime_events.recv() => match event {
                Ok(RuntimeEvent::Error(message)) => {
                    let notice = ServerNotice {
                        level: NoticeLevel::Error,
                        message,
                    };
                    if !shared.send_notice(notice).await {
                        return;
                    }
                }
                // As above: the runtime errors the session missed are gone, so the count is the
                // only record that they happened.
                Err(RecvError::Lagged(skipped)) => {
                    warn!(skipped, "a session fell behind the runtime event bus");
                }
                Err(RecvError::Closed) => return,
            },
            _ = leadership_check.tick() => {
                let current = service.observed_leadership().await;
                if current == leadership {
                    continue;
                }
                leadership = current;
                if !shared.announce_leadership(&leadership).await {
                    return;
                }
            }
            changed = domains.changed() => {
                if changed.is_err() {
                    return;
                }
                if !shared.send_domains().await {
                    return;
                }
                shared.forget_vanished_selection().await;
            }
            changed = selection.changed() => {
                if changed.is_err() {
                    return;
                }
                snapshots.reset();
                if !shared.send_selected_domain().await {
                    return;
                }
            }
            _ = snapshots.tick() => {
                if !shared.send_selected_domain().await {
                    return;
                }
            }
        }
    }
}

impl SessionShared {
    /// Sends one event. `false` means the transport stopped taking frames.
    async fn send_event(&self, frame: EncodedFrame<ServerFrame>) -> bool {
        let sent = self.send_frame(frame).await;
        if sent.is_err() {
            debug!("the session's client left before an event reached it");
            return false;
        }
        true
    }

    async fn send_notice(&self, notice: ServerNotice) -> bool {
        match notice.encode(self.limits()) {
            Ok(frame) => self.send_event(frame).await,
            Err(error) => {
                warn!(error = %error, "a server notice does not fit a session frame");
                true
            }
        }
    }

    /// Tells the session which node leads. A console session is served only by the leader, so a
    /// console session on any other node is ended with a redirect instead. `false` means the
    /// session ended.
    async fn announce_leadership(&self, leadership: &Leadership) -> bool {
        if let SessionTransport::Console = self.transport {
            let redirect = match leadership {
                Leadership::ServingNode(_) => None,
                Leadership::Remote(leader) => Some(WireLeaderRedirect {
                    leader: Some(leader.clone()),
                }),
                Leadership::Unknown => Some(WireLeaderRedirect { leader: None }),
            };
            if let Some(redirect) = redirect {
                self.end(SessionEndReason::LeaderRedirect(redirect));
                return false;
            }
        }
        let observed = LeadershipObserved {
            leadership: leadership.clone(),
        };
        match observed.encode(self.limits()) {
            Ok(frame) => self.send_event(frame).await,
            Err(error) => {
                warn!(error = %error, "a leadership observation does not fit a session frame");
                true
            }
        }
    }

    async fn send_domains(&self) -> bool {
        let observed = DomainsObserved {
            domains: self.service.domain_infos().await,
        };
        match observed.encode(self.limits()) {
            Ok(frame) => self.send_event(frame).await,
            Err(error) => {
                warn!(error = %error, "the domain list does not fit a session frame");
                true
            }
        }
    }

    /// Stops observing a selected domain that no longer exists.
    async fn forget_vanished_selection(&self) {
        let selected = self.selection.borrow().clone();
        let Some(domain) = selected else {
            return;
        };
        let existing = self.service.inner.consensus.current_domain(&domain).await;
        if existing.is_none() {
            self.selection.send_replace(None);
        }
    }

    /// Sends the cluster summary and the snapshot of the selected domain, when one is selected.
    async fn send_selected_domain(&self) -> bool {
        let selected = self.selection.borrow().clone();
        let Some(domain) = selected else {
            return true;
        };
        let summary = self.service.cluster_observation().await;
        match summary.encode(self.limits()) {
            Ok(frame) => {
                if !self.send_event(frame).await {
                    return false;
                }
            }
            Err(error) => warn!(error = %error, "a cluster summary does not fit a session frame"),
        }
        let snapshot = self.service.domain_snapshot(&domain, *self.limits()).await;
        match snapshot {
            Some(frame) => self.send_event(frame).await,
            None => true,
        }
    }
}

impl SessionServiceImpl {
    /// Every domain as this node's replica of the cluster state describes it.
    pub(in crate::application) async fn domain_infos(&self) -> Vec<DomainInfo> {
        let domains = self.inner.consensus.current_domains().await;
        let mut infos = Vec::with_capacity(domains.len());
        for (domain, state) in domains {
            infos.push(DomainInfo {
                domain,
                status: state.status,
                pace: state.config.pace,
            });
        }
        infos
    }

    /// Leadership as this node currently observes it, with the endpoints discovery established
    /// for a remote leader.
    async fn observed_leadership(&self) -> Leadership {
        let local = self.inner.consensus.local_node_id();
        let leader = self.inner.consensus.current_leader().await;
        let Some(leader) = leader else {
            return Leadership::Unknown;
        };
        if &leader == local {
            return Leadership::ServingNode(leader);
        }
        let location = self.leader_location(leader).await;
        Leadership::Remote(leader_endpoints(location))
    }

    async fn cluster_observation(&self) -> ClusterObserved {
        let domains = self.inner.consensus.current_domains().await;
        let mut running_domains = 0_u64;
        for state in domains.values() {
            if state.status == DomainStatus::Running {
                running_domains = running_domains
                    .checked_add(1)
                    .assured("the count never exceeds the domains the map holds");
            }
        }
        let mut graph_nodes = 0_u64;
        let mut relays = 0_u64;
        for (_, graph) in self.inner.registry.active_graphs() {
            let counts = graph.dataflow_graph_counts();
            let nodes: u64 = counts.nodes.arch_into();
            let graph_relays: u64 = counts.relays.arch_into();
            graph_nodes = graph_nodes
                .checked_add(nodes)
                .assured("every counted graph node is held in memory, so the total fits in u64");
            relays = relays
                .checked_add(graph_relays)
                .assured("every counted relay is held in memory, so the total fits in u64");
        }
        ClusterObserved {
            running_domains,
            graph_nodes,
            relays,
        }
    }

    /// The encoded snapshot of one domain: its live graph with runtime statistics, and the
    /// entities it declares. The graph is serialized and encoded on the bulk workers. `None` when
    /// the domain has gone, or the snapshot cannot be prepared or does not fit a frame.
    async fn domain_snapshot(
        &self,
        domain: &DomainName,
        limits: SessionLimits,
    ) -> Option<EncodedFrame<ServerFrame>> {
        self.inner.consensus.current_domain(domain).await?;
        let graph = self.domain_graph(domain).await;
        let entities = self.domain_entities(domain).await;
        let executor = self.inner.runtime.executor();
        let chunk_bytes = executor.limits().bulk_chunk_bytes.as_u64();
        let reservation = match executor.reserve(MemoryClass::Bulk, chunk_bytes).await {
            Ok(reservation) => reservation,
            Err(error) => {
                debug!(domain = domain.as_str(), error = %error, "a domain snapshot was skipped");
                return None;
            }
        };
        let snapshot_domain = domain.clone();
        let encoded = executor
            .run_cpu(
                CpuClass::Bulk,
                reservation,
                move |_charge, _cancellation| {
                    encode_snapshot(&snapshot_domain, &graph, &entities, &limits)
                },
            )
            .await;
        match encoded {
            Ok(Ok(frame)) => Some(frame),
            Ok(Err(error)) => {
                warn!(domain = domain.as_str(), error = %error, "a domain snapshot was not sent");
                None
            }
            Err(error) => {
                debug!(domain = domain.as_str(), error = %error, "a domain snapshot was skipped");
                None
            }
        }
    }

    /// The live graph of one domain, with the runtime statistics and node health a snapshot
    /// shows. A domain without an active graph is an empty graph.
    async fn domain_graph(&self, domain: &DomainName) -> DataflowGraph {
        let mut graph = match self.inner.registry.active_graph(domain) {
            Some(active) => active.to_dataflow_graph(domain.as_str()),
            None => DataflowGraph::new(domain.as_str()),
        };
        let runtime = &self.inner.runtime;
        graph.statistics = runtime.dataflow_domain_statistics(domain);
        for node in &mut graph.nodes {
            tokio::task::consume_budget().await;
            let Some((kind, identifier)) = dataflow_metric_target(&node.id) else {
                continue;
            };
            let health = self
                .dataflow_node_status_for_graph(domain, &kind, &identifier)
                .await;
            node.status = health.status;
            node.status_detail = health.detail;
            node.reconnect_wait_millis = health.reconnect_wait_millis;
            if kind != "RELAY" {
                continue;
            }
            let relay = RelayName::from(&identifier);
            node.statistics = runtime.dataflow_relay_buffer_statistics(domain, &relay);
            let mut listed = std::collections::BTreeSet::new();
            for branch in &node.branches {
                listed.insert(branch.branch.clone());
            }
            for branch in runtime.dataflow_relay_branch_statistics(domain, &relay) {
                if !listed.contains(&branch.branch) {
                    node.branches.push(branch);
                }
            }
        }
        for edge in &mut graph.edges {
            let Some(metric) = edge.metric.as_ref() else {
                continue;
            };
            edge.statistics = runtime.dataflow_edge_statistics(domain, metric);
            edge.branches = runtime.dataflow_edge_branch_statistics(domain, metric);
        }
        graph
    }

    /// The models and resources one domain declares, models first.
    async fn domain_entities(&self, domain: &DomainName) -> Vec<DomainEntity> {
        let mut entities = Vec::new();
        for node in self.inner.registry.active_domain_entities(domain) {
            entities.push(DomainEntity::Model(node));
        }
        let resources = self.inner.consensus.current_resources().await;
        for name in resources.resources_in(domain) {
            let latest = resources.uploads.resolve_completed_version(
                domain,
                name,
                RequestedResourceVersion::Latest,
            );
            let latest_version = match latest {
                Ok(id) => Some(
                    NonZeroU64::new(id.version)
                        .assured("a resource catalog assigns versions counting from 1"),
                ),
                Err(_) => None,
            };
            entities.push(DomainEntity::Resource {
                name: name.clone(),
                latest_version,
            });
        }
        entities
    }
}

/// Serializes a domain graph and encodes the snapshot frame that carries it.
fn encode_snapshot(
    domain: &DomainName,
    graph: &DataflowGraph,
    entities: &[DomainEntity],
    limits: &SessionLimits,
) -> Result<EncodedFrame<ServerFrame>, SnapshotEncodingError> {
    let graph_bytes = graph.serialize().map_err(SnapshotEncodingError::Graph)?;
    let graph_json = String::from_utf8(graph_bytes).assured("a JSON serializer writes UTF-8 text");
    DomainSnapshotObserved::encode(domain, &graph_json, entities, limits)
        .map_err(SnapshotEncodingError::Frame)
}

/// Why a domain snapshot could not be encoded.
#[derive(Debug, thiserror::Error)]
enum SnapshotEncodingError {
    #[error("the domain graph could not be serialized: {0}")]
    Graph(nervix_dataflow_graph::DataflowGraphError),
    #[error("the snapshot does not fit a session frame: {0}")]
    Frame(error_stack::Report<nervix_client_wire::WireEncodeError>),
}
