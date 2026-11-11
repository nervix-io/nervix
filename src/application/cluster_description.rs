//! The description of the cluster a client asks for with `DESCRIBE CLUSTER`.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering the public identity, prerequisites, leader/timing drift, committed
//!   live/departed capacity claims and effective receive bounds as printable lines.
//! - **Depends on.** The cluster gossip view and coherent public consensus observations.
//! - **Must not know.** Private keys, record publication or how two records are judged.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use nervix_consensus::{ClusterIdentity, GossipNode, Observer};
use nervix_models::{
    ClusterNodeName, NodeCapabilityRecord, NodePrerequisites, PeerReceiveBounds, Prerequisite,
    SettingDefinition as _, timing_text,
};

use crate::cluster;

/// The public identity, leader, timing drift and each live node's published prerequisites.
pub(in crate::application) async fn describe_cluster(
    cluster: &cluster::ClusterHandle,
    consensus: &Observer,
    interconnect: &nervix_interconnect::Transport,
    runtime: &crate::runtime::Runtime,
) -> String {
    let leader = consensus.current_leader().await;
    let gossip = cluster.gossip_state().await;
    let nodes = gossip.latest_nodes_by_id();
    let leader_record = match &leader {
        Some(leader) => match nodes.get(leader) {
            Some(node) => node.prerequisites.clone(),
            None => None,
        },
        None => None,
    };
    let description = ClusterDescription {
        identity: consensus.cluster_identity(),
        leader,
        leader_record,
        nodes,
        capabilities: consensus.node_capabilities(),
    };
    let mut lines = description.lines();
    for (peer, record) in interconnect.peer_capability_records() {
        lines.push(format!(
            "adopted peer receive bounds: node={peer} incarnation={} {}",
            record.identity.incarnation(),
            bounds_line(record.capabilities.receive)
        ));
    }
    lines.extend(super::cluster_parameters::describe_parameters(cluster, consensus).await);
    lines.extend(LocalExecutionDescription { runtime }.lines());
    lines.join("\n")
}

struct LocalExecutionDescription<'runtime> {
    runtime: &'runtime crate::runtime::Runtime,
}

impl LocalExecutionDescription<'_> {
    fn lines(&self) -> Vec<String> {
        let runtime = self.runtime;
        let mut lines = Vec::new();
        lines.push("local execution capacity:".to_string());
        let snapshot = runtime.executor().snapshot();
        for (class, workers) in snapshot.worker_classes() {
            lines.push(format!(
                "  execution workers: class={} workers={} pending_jobs={}",
                class.as_str(),
                workers.workers,
                workers.queue_capacity
            ));
        }
        for (class, memory) in snapshot.memory_classes() {
            lines.push(format!(
                "  execution memory: class={} bytes={}",
                class.as_str(),
                memory.capacity_bytes
            ));
        }
        lines.push(format!(
            "  client byte credit: producer={} consumer={}",
            runtime.client_endpoint_capacities().0,
            runtime.client_endpoint_capacities().1
        ));
        let limits = runtime.executor().limits();
        lines.push(format!(
            "  execution operation limits: relay_encoded={} relay_decoded={} relay_scratch={} \
             resident_replication_batches={} bulk_chunk={}",
            limits.relay_encoded_bytes.as_u64(),
            limits.relay_decoded_bytes.as_u64(),
            limits.relay_scratch_bytes.as_u64(),
            limits.resident_replication_batches.get(),
            limits.bulk_chunk_bytes.as_u64()
        ));
        lines
    }
}

/// One view of the cluster, as the lines of `DESCRIBE CLUSTER` render it.
struct ClusterDescription {
    identity: Option<ClusterIdentity>,
    leader: Option<ClusterNodeName>,
    leader_record: Option<NodePrerequisites>,
    nodes: BTreeMap<ClusterNodeName, GossipNode>,
    capabilities: BTreeMap<ClusterNodeName, NodeCapabilityRecord>,
}

impl ClusterDescription {
    fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        match &self.leader {
            Some(leader) => lines.push(format!("leader: {leader}")),
            None => lines.push("leader: none".to_string()),
        }
        if let Some(identity) = &self.identity {
            lines.push(format!("cluster uid: {}", identity.cluster_uid));
            lines.push(format!("cluster key: {}", identity.kid));
            lines.push(format!(
                "cluster created: {}",
                identity.created_at.to_rfc3339()
            ));
        }
        lines.push(format!("timing drift: {}", self.drift_summary()));
        for (node_id, node) in &self.nodes {
            lines.extend(self.node_lines(node_id, node));
        }
        for (node_id, record) in &self.capabilities {
            if !self.nodes.contains_key(node_id) {
                lines.push(format!("node: {node_id} (departed)"));
                lines.push(format!("  incarnation: {}", record.identity.incarnation()));
                lines.push(capability_line(record));
                lines.push(interconnect_line(record));
                lines.extend(infrastructure_lines(record));
            }
        }
        let effective = self
            .capabilities
            .iter()
            .filter_map(|(name, record)| {
                let live = self.nodes.get(name)?;
                if live.incarnation == record.identity.incarnation() {
                    Some(record.capabilities.peer_receive_bounds())
                } else {
                    None
                }
            })
            .reduce(PeerReceiveBounds::intersection);
        match effective {
            Some(bounds) => lines.push(format!(
                "cluster-effective receive bounds: {}",
                bounds_line(bounds)
            )),
            None => lines.push(
                "cluster-effective receive bounds: awaiting committed live claims".to_string(),
            ),
        }
        lines
    }

    /// The nodes whose timings differ from the leader's, `none`, or why nothing was compared.
    fn drift_summary(&self) -> String {
        let Some(leader_record) = &self.leader_record else {
            return "unknown, no leader record is published".to_string();
        };
        let mut drifting = Vec::new();
        for (node_id, node) in &self.nodes {
            let Some(record) = &node.prerequisites else {
                continue;
            };
            if !record.timings.differing(&leader_record.timings).is_empty() {
                drifting.push(node_id.to_string());
            }
        }
        if drifting.is_empty() {
            return "none".to_string();
        }
        drifting.join(", ")
    }

    fn node_lines(&self, node_id: &ClusterNodeName, node: &GossipNode) -> Vec<String> {
        let is_leader = self.leader.as_ref() == Some(node_id);
        let mut lines = Vec::new();
        if is_leader {
            lines.push(format!("node: {node_id} (leader)"));
        } else {
            lines.push(format!("node: {node_id}"));
        }
        lines.push(format!("  incarnation: {}", node.incarnation));
        match self.capabilities.get(node_id) {
            Some(record) if record.identity.incarnation() == node.incarnation => {
                lines.push(capability_line(record));
                lines.push(interconnect_line(record));
                lines.extend(infrastructure_lines(record));
            }
            Some(record) => lines.push(format!(
                "  capabilities: awaiting this incarnation; retained {}",
                record.identity
            )),
            None => lines.push("  capabilities: awaiting committed claim".to_string()),
        }
        if let Some(parameters) = node
            .parameters
            .and_then(|publication| publication.running())
        {
            let liveness =
                nervix_interconnect::InterconnectLiveness::from_parameters(&parameters.values);
            lines.push(format!(
                "  interconnect liveness: ping-interval={} ping-timeout={} probe-timeout={}",
                timing_text(liveness.ping_interval()),
                timing_text(liveness.ping_timeout()),
                timing_text(liveness.probe_timeout())
            ));
        }
        let Some(record) = &node.prerequisites else {
            lines.push("  prerequisites: not published".to_string());
            return lines;
        };
        for described in record.described() {
            let drift = self.timing_drift(is_leader, record, described.prerequisite);
            match drift {
                Some(leader_value) => lines.push(format!(
                    "  {}: {}, the leader's {leader_value}",
                    described.prerequisite, described.value
                )),
                None => lines.push(format!("  {}: {}", described.prerequisite, described.value)),
            }
        }
        lines
    }

    /// The leader's value of `prerequisite` when it is a timing this non-leader record holds
    /// differently.
    fn timing_drift(
        &self,
        is_leader: bool,
        record: &NodePrerequisites,
        prerequisite: Prerequisite,
    ) -> Option<String> {
        if is_leader {
            return None;
        }
        let leader_record = self.leader_record.as_ref()?;
        for timing in record.timings.differing(&leader_record.timings) {
            if Prerequisite::from(timing) == prerequisite {
                return Some(timing_text(leader_record.timings.get(timing)));
            }
        }
        None
    }
}

fn bounds_line(bounds: PeerReceiveBounds) -> String {
    format!(
        "relay-batch-bytes={} relay-decoded-bytes={} bulk-chunk-bytes={} bulk-window-bytes={}",
        bounds.relay_batch_bytes,
        bounds.relay_decoded_bytes,
        bounds.bulk_chunk_bytes,
        bounds.bulk_window_bytes
    )
}

fn infrastructure_lines(record: &NodeCapabilityRecord) -> Vec<String> {
    let claim = record.capabilities;
    let dns = claim.dns;
    let mut lines = vec![format!(
        "  dns: cache-entries={} positive-max-ttl={} negative-max-ttl={} hook-budget={} \
         families={} trust-negative={} concurrent-lookups={}",
        dns.cache_entries,
        timing_text(dns.positive_max_ttl),
        timing_text(dns.negative_max_ttl),
        timing_text(dns.hook_lookup_budget),
        dns.address_families,
        dns.trust_negative_answers,
        dns.concurrent_lookups
    )];
    for (name, database) in [
        ("database", claim.node_database),
        ("raft database", claim.consensus_database),
    ] {
        lines.push(format!(
            "  {name}: cache-bytes={} journal-bytes={} workers={}",
            database.cache_bytes, database.journal_bytes, database.workers
        ));
    }
    let consensus = claim.consensus;
    lines.push(format!(
        "  consensus events: capacity={}",
        consensus.event_capacity
    ));
    lines.push(format!(
        "  consensus: {}",
        [
            format!("append-batch-entries={}", consensus.append_batch_entries),
            format!(
                "outstanding-append-batches={}",
                consensus.outstanding_append_batches
            ),
            format!(
                "outstanding-append-bytes={}",
                consensus.outstanding_append_bytes
            ),
            format!(
                "append-idle-bound={}",
                timing_text(consensus.append_idle_bound)
            ),
            format!(
                "snapshot-transfer-timeout={}",
                timing_text(consensus.snapshot_transfer_timeout)
            ),
            format!(
                "membership-mutation-timeout={}",
                timing_text(consensus.membership_mutation_timeout)
            ),
            format!(
                "retention-interval={}",
                timing_text(consensus.retention_interval)
            ),
            format!(
                "retention-admission-timeout={}",
                timing_text(consensus.retention_admission_timeout)
            ),
            format!(
                "retention-admission-poll={}",
                timing_text(consensus.retention_admission_poll)
            ),
            format!("purge-batch-entries={}", consensus.purge_batch_entries),
            format!("api-channel-capacity={}", consensus.api_channel_capacity),
            format!(
                "notification-channel-capacity={}",
                consensus.notification_channel_capacity
            ),
            format!(
                "state-machine-channel-capacity={}",
                consensus.state_machine_channel_capacity
            ),
            format!("api-batch-capacity={}", consensus.api_batch_capacity),
            format!("snapshot-chunk-bytes={}", consensus.snapshot_chunk_bytes),
            format!(
                "replication-backoff={}",
                timing_text(consensus.replication_backoff)
            ),
            format!(
                "replication-backoff-maximum={}",
                timing_text(consensus.replication_backoff_maximum)
            ),
            format!(
                "membership-step-down-delay={}",
                timing_text(consensus.membership_step_down_delay)
            ),
            format!(
                "replication-lag-threshold={}",
                consensus.replication_lag_threshold
            ),
        ]
        .join(" ")
    ));
    lines
}

fn interconnect_line(record: &NodeCapabilityRecord) -> String {
    let capacity = record.capabilities.interconnect;
    format!(
        "  interconnect: max-peers={} request-quota={} health-probes={} max-connections={} \
         handshakes={} queue={} completion={} watermarks={} stream-window={} connection-window={} \
         setup={} request={} progress={} backoff={} max-backoff={} jitter-permille={}",
        capacity.max_peers,
        capacity.request_quota,
        capacity.health_probes,
        capacity.max_connections,
        capacity.concurrent_handshakes,
        capacity.incoming_queue_capacity,
        capacity.completion_quota,
        capacity.relay_watermarks,
        capacity.stream_window_bytes,
        capacity.connection_window_bytes,
        timing_text(capacity.connection_setup_timeout),
        timing_text(capacity.request_timeout),
        timing_text(capacity.progress_timeout),
        timing_text(capacity.reconnect_backoff),
        timing_text(capacity.max_reconnect_backoff),
        capacity.reconnect_jitter_permille
    )
}

fn capability_line(record: &NodeCapabilityRecord) -> String {
    let capabilities = record.capabilities;
    let cpu = match capabilities.cpu_count {
        Some(count) => count.to_string(),
        None => "unknown".to_string(),
    };
    let memory = match capabilities.memory_bytes {
        Some(bytes) => bytes.to_string(),
        None => "unknown".to_string(),
    };
    format!(
        "  capabilities: committed {} cpu-count={cpu} memory-bytes={memory} {} {} {} \
         listeners={:?}",
        record.identity,
        bounds_line(capabilities.receive),
        engine_capability_line(capabilities.engines),
        connector_host_capability_line(capabilities.connector_host),
        capabilities.listeners,
    )
}

fn engine_capability_line(values: nervix_models::EngineCapabilities) -> String {
    format!(
        "{}={} {}={} {}={} {}={} {}={} {}={} wasm-parallel-compilation={}",
        nervix_vm::VmInlineRowLimit::NAME,
        values.vm_inline_row_limit,
        nervix_vm::VmRegexSearchCacheBytes::NAME,
        values.vm_regex_search_cache_bytes,
        nervix_vm::VmDynamicPatternCacheCapacity::NAME,
        values.vm_dynamic_pattern_cache_capacity,
        nervix_vm::VmPatternSetsPerBatch::NAME,
        values.vm_pattern_sets_per_batch,
        nervix_wasm::WasmStackBytes::NAME,
        values.wasm_stack_bytes,
        nervix_wasm::WasmMemoryReservationBytes::NAME,
        values.wasm_memory_reservation_bytes,
        values.wasm_parallel_compilation,
    )
}

fn connector_host_capability_line(values: nervix_models::ConnectorHostCapabilities) -> String {
    format!(
        "{}={} {}={} {}={} {}={} {}={} {}={}",
        nervix_connector::ConnectorRetryMultiplier::NAME,
        values.retry_multiplier,
        nervix_connector::ConnectorRetryJitterPercent::NAME,
        values.retry_jitter_percent,
        nervix_connector_kafka::KafkaDomainAssignmentRetry::NAME,
        humantime::format_duration(values.kafka_domain_assignment_retry),
        crate::runtime::KafkaPartitionWatchInterval::NAME,
        humantime::format_duration(values.kafka_partition_watch_interval),
        nervix_connector_syslog::SyslogIntakeQueueCapacity::NAME,
        values.syslog_intake_queue_capacity,
        nervix_connector_syslog::SyslogReadChunkBytes::NAME,
        values.syslog_read_chunk_bytes,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nervix_models::{
        ClusterNodeIncarnation, ConsensusFloors, PrerequisiteTimings, RaftLogDurability,
        TrustRootFingerprint, WireContractFingerprint,
    };

    use super::*;

    fn name(node: &str) -> ClusterNodeName {
        ClusterNodeName::parse(node).expect("a valid node name")
    }

    fn record(heartbeat_millis: u64) -> NodePrerequisites {
        NodePrerequisites {
            cluster_identity: "cluster".to_string(),
            trust_root: TrustRootFingerprint::from_digest([1; 32]),
            wire_contract: WireContractFingerprint::from_digest([2; 32]),
            consensus_floors: ConsensusFloors {
                largest_command_bytes: 1 << 20,
                largest_management_event_bytes: 64 << 10,
            },
            durability: RaftLogDurability::FsyncEveryAppend,
            timings: PrerequisiteTimings {
                raft_heartbeat_interval: Duration::from_millis(heartbeat_millis),
                gossip_interval: Duration::from_millis(500),
                clock_skew_tolerance: Duration::from_millis(500),
            },
        }
    }

    fn node(node: &str, incarnation: u64, record: Option<NodePrerequisites>) -> GossipNode {
        GossipNode {
            licensing: None,
            parameters: None,
            node_id: name(node),
            incarnation: ClusterNodeIncarnation::new(incarnation),
            terminating: false,
            client_url: None,
            console_url: None,
            interconnect_endpoint: None,
            prerequisites: record,
        }
    }

    #[test]
    fn a_node_whose_timing_differs_from_the_leader_is_marked_with_both_values() {
        let description = ClusterDescription {
            capabilities: BTreeMap::new(),
            identity: None,
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 7, Some(record(250)))),
                (name("node-2"), node("node-2", 8, Some(record(400)))),
                (name("node-3"), node("node-3", 9, None)),
            ]),
        };
        let lines = description.lines();
        assert_eq!(lines[0], "leader: node-1");
        assert_eq!(lines[1], "timing drift: node-2");
        assert!(lines.contains(&"node: node-1 (leader)".to_string()));
        assert!(lines.contains(&"  raft-heartbeat-interval: 250ms".to_string()));
        assert!(
            lines.contains(&"  raft-heartbeat-interval: 400ms, the leader's 250ms".to_string())
        );
        assert!(lines.contains(&"  cluster identity: cluster".to_string()));
        assert!(lines.contains(&"  durability promise: fsync every append".to_string()));
        assert!(lines.contains(&"  prerequisites: not published".to_string()));
    }

    #[test]
    fn without_a_leader_record_no_drift_is_claimed() {
        let description = ClusterDescription {
            capabilities: BTreeMap::new(),
            identity: None,
            leader: None,
            leader_record: None,
            nodes: BTreeMap::from([(name("node-1"), node("node-1", 1, Some(record(250))))]),
        };
        let lines = description.lines();
        assert_eq!(lines[0], "leader: none");
        assert_eq!(
            lines[1],
            "timing drift: unknown, no leader record is published"
        );
        assert!(lines.contains(&"  raft-heartbeat-interval: 250ms".to_string()));
    }

    #[test]
    fn matching_timings_report_no_drift() {
        let description = ClusterDescription {
            capabilities: BTreeMap::new(),
            identity: None,
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, Some(record(250)))),
                (name("node-2"), node("node-2", 2, Some(record(250)))),
            ]),
        };
        assert_eq!(description.lines()[1], "timing drift: none");
    }

    #[test]
    fn node_capabilities_keep_departed_claims_and_only_current_live_claims_set_effective_bounds() {
        use std::num::NonZeroU64;

        use nervix_models::{ClusterNodeIdentity, NodeCapabilities};

        let claim = |node: &str, incarnation, relay, chunk| {
            let mut receive = nervix_interconnect::PeerReceiveSettings::default().bounds();
            receive.relay_batch_bytes = NonZeroU64::new(relay).expect("positive relay bound");
            receive.bulk_chunk_bytes = NonZeroU64::new(chunk).expect("positive chunk bound");
            NodeCapabilityRecord {
                identity: ClusterNodeIdentity::new(
                    name(node),
                    ClusterNodeIncarnation::new(incarnation),
                ),
                capabilities: NodeCapabilities {
                    interconnect: nervix_interconnect::TransportSettings::default().capabilities(),
                    dns: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .dns,
                    node_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .node_database,
                    consensus_database: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus_database,
                    consensus: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_capability_record()
                    .capabilities
                    .consensus,
                    engines: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_engine_capabilities(),
                    connector_host: nervix_arbitrary::Arbitrary::new(
                        &[],
                        nervix_arbitrary::Domain::Vocabulary,
                    )
                    .node_connector_host_capabilities(),
                    listeners: crate::application::listener_settings::ListenerSettings::default()
                        .bounds(),
                    cpu_count: None,
                    memory_bytes: NonZeroU64::new(8 << 30),
                    receive,
                },
            }
        };
        let first = claim("node-1", 1, 2048, 16384);
        let second = claim("node-2", 2, 4096, 8192);
        let stale = claim("node-3", 2, 1, 1);
        let departed = claim("node-4", 4, 1, 1);
        let description = ClusterDescription {
            identity: None,
            leader: None,
            leader_record: None,
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, None)),
                (name("node-2"), node("node-2", 2, None)),
                (name("node-3"), node("node-3", 3, None)),
            ]),
            capabilities: [first.clone(), second.clone(), stale, departed.clone()]
                .into_iter()
                .map(|record| (record.identity.node_id().clone(), record))
                .collect(),
        };
        let lines = description.lines();
        assert!(lines.contains(&capability_line(&first)));
        assert!(capability_line(&first).contains("cpu-count=unknown memory-bytes=8589934592"));
        assert!(lines.contains(&"node: node-4 (departed)".to_string()));
        assert!(lines.contains(&capability_line(&departed)));
        let effective = first
            .capabilities
            .receive
            .intersection(second.capabilities.receive);
        assert_eq!(effective.relay_batch_bytes.get(), 2048);
        assert_eq!(effective.bulk_chunk_bytes.get(), 8192);
        assert!(lines.contains(&format!(
            "cluster-effective receive bounds: {}",
            bounds_line(effective)
        )));
        assert!(
            lines.iter().any(
                |line| line.starts_with("  capabilities: awaiting this incarnation; retained ")
            )
        );
    }
}
