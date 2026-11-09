//! The description of the cluster a client asks for with `DESCRIBE CLUSTER`.
//!
//! Layer: control plane.
//!
//! - **Owns.** Rendering the public identity, prerequisites, leader/timing drift, committed
//!   live/departed node records (capacity claims, detected properties and tags) and effective
//!   receive bounds as printable lines.
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
    ClusterNodeName, CommittedNodeRecord, CudaProviderRegistration, Detected, GpuInventory,
    NodeCapabilityRecord, NodePrerequisites, NodeProperties, NodeTags, OnnxRuntimeDetection,
    PeerReceiveBounds, Prerequisite, PropertyText, timing_text,
};

use crate::cluster;

/// The public identity, leader, timing drift and each live node's published prerequisites.
pub(in crate::application) async fn describe_cluster(
    cluster: &cluster::ClusterHandle,
    consensus: &Observer,
    interconnect: &nervix_interconnect::Transport,
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
        records: consensus.node_records(),
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
    lines.join("\n")
}

/// One view of the cluster, as the lines of `DESCRIBE CLUSTER` render it.
struct ClusterDescription {
    identity: Option<ClusterIdentity>,
    leader: Option<ClusterNodeName>,
    leader_record: Option<NodePrerequisites>,
    nodes: BTreeMap<ClusterNodeName, GossipNode>,
    records: BTreeMap<ClusterNodeName, CommittedNodeRecord>,
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
        for (node_id, committed) in &self.records {
            if !self.nodes.contains_key(node_id) {
                lines.push(format!("node: {node_id} (departed)"));
                lines.push(format!(
                    "  incarnation: {}",
                    committed.record.identity().incarnation()
                ));
                lines.push(capability_line(&committed.record.claim));
                lines.extend(record_lines(Some(committed)));
            }
        }
        let effective = self
            .records
            .iter()
            .filter_map(|(name, committed)| {
                let live = self.nodes.get(name)?;
                if live.incarnation == committed.record.identity().incarnation() {
                    Some(committed.record.capabilities().peer_receive_bounds())
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
        let committed = self.records.get(node_id);
        match committed {
            Some(committed) if committed.record.identity().incarnation() == node.incarnation => {
                lines.push(capability_line(&committed.record.claim))
            }
            Some(committed) => lines.push(format!(
                "  capabilities: awaiting this incarnation; retained {}",
                committed.record.identity()
            )),
            None => lines.push("  capabilities: awaiting committed claim".to_string()),
        }
        lines.extend(self.prerequisite_lines(is_leader, node));
        lines.extend(record_lines(committed));
        lines
    }

    /// The prerequisites the node publishes, each marked when it is a timing the leader holds
    /// differently.
    fn prerequisite_lines(&self, is_leader: bool, node: &GossipNode) -> Vec<String> {
        let Some(record) = &node.prerequisites else {
            return vec!["  prerequisites: not published".to_string()];
        };
        let mut lines = Vec::new();
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

/// The committed record's revision and run, its detected properties and its tags, or that the node
/// has not announced one.
fn record_lines(committed: Option<&CommittedNodeRecord>) -> Vec<String> {
    let Some(committed) = committed else {
        return vec!["  record: not announced".to_string()];
    };
    let mut lines = vec![
        format!(
            "  record: revision {} (incarnation {})",
            committed.revision,
            committed.record.identity().incarnation()
        ),
        "  properties:".to_string(),
    ];
    lines.extend(property_lines(&committed.record.properties));
    lines.extend(tag_lines(&committed.record.tags));
    lines
}

/// One line per detected property, named as a node selector reads it, an undetected value with its
/// reason.
fn property_lines(properties: &NodeProperties) -> Vec<String> {
    let mut lines = vec![
        format!("    architecture: {}", properties.architecture),
        format!(
            "    cpu.model: {}",
            detected_line(properties.cpu_model.as_ref())
        ),
    ];
    let gpus = &properties.gpus;
    match gpus {
        GpuInventory::Undetected(reason) => {
            lines.push(format!("    gpu.count: 0 (undetected: {reason})"));
        }
        GpuInventory::Devices { .. } | GpuInventory::NoDevice => {
            lines.push(format!("    gpu.count: {}", gpus.count()));
        }
    }
    let mut models = Vec::new();
    for device in gpus.devices() {
        models.push(detected_line(device.model.as_ref()));
    }
    if models.is_empty() {
        lines.push("    gpu.models: none".to_string());
    } else {
        lines.push(format!("    gpu.models: {}", models.join(", ")));
    }
    match gpus.smallest_memory_bytes() {
        Some(bytes) => lines.push(format!("    gpu.memory: {bytes}")),
        None => lines.push("    gpu.memory: none".to_string()),
    }
    match gpus.driver() {
        Some(driver) => lines.push(format!("    gpu.driver: {}", detected_line(driver))),
        None => lines.push("    gpu.driver: none".to_string()),
    }
    match &properties.onnx {
        OnnxRuntimeDetection::Loaded(runtime) => {
            lines.push(format!(
                "    onnx.version: {}",
                detected_line(runtime.version.as_ref())
            ));
            let providers = match &runtime.providers {
                Detected::Value(providers) if providers.as_slice().is_empty() => "none".to_string(),
                Detected::Value(providers) => {
                    let mut names = Vec::new();
                    for provider in providers.as_slice() {
                        names.push(provider.as_str());
                    }
                    names.join(", ")
                }
                Detected::Undetected(reason) => format!("undetected ({reason})"),
            };
            lines.push(format!("    onnx.providers: {providers}"));
            match runtime.cuda {
                CudaProviderRegistration::Registered => {
                    lines.push("    onnx.cuda: true".to_string());
                }
                refusal @ (CudaProviderRegistration::NotInLibrary
                | CudaProviderRegistration::RegistrationFailed) => {
                    lines.push(format!("    onnx.cuda: false ({refusal})"));
                }
            }
        }
        OnnxRuntimeDetection::Undetected(reason) => {
            lines.push(format!("    onnx.version: undetected ({reason})"));
            lines.push(format!("    onnx.providers: undetected ({reason})"));
            lines.push(format!("    onnx.cuda: false (undetected: {reason})"));
        }
    }
    lines.push(format!(
        "    cuda.available: {}",
        properties.cuda_available()
    ));
    lines
}

/// One line per tag, `key=value` in key order, or that the node has none.
fn tag_lines(tags: &NodeTags) -> Vec<String> {
    if tags.is_empty() {
        return vec!["  tags: none".to_string()];
    }
    let mut lines = vec!["  tags:".to_string()];
    for (key, value) in tags.iter() {
        lines.push(format!("    {key}={value}"));
    }
    lines
}

fn detected_line(detected: Detected<&PropertyText>) -> String {
    match detected {
        Detected::Value(text) => text.to_string(),
        Detected::Undetected(reason) => format!("undetected ({reason})"),
    }
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
        "  capabilities: committed {} cpu-count={cpu} memory-bytes={memory} {}",
        record.identity,
        bounds_line(capabilities.receive)
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
            records: BTreeMap::new(),
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
            records: BTreeMap::new(),
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
            records: BTreeMap::new(),
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

    fn committed(claim: NodeCapabilityRecord, revision: u64, tags: &str) -> CommittedNodeRecord {
        use nervix_models::{NodeRecord, UndetectedReason};

        CommittedNodeRecord {
            revision,
            record: NodeRecord {
                claim,
                properties: NodeProperties {
                    architecture: PropertyText::new("x86_64").expect("short printable text"),
                    cpu_model: Detected::Undetected(UndetectedReason::NotReported),
                    gpus: GpuInventory::Undetected(UndetectedReason::LibraryUnavailable),
                    onnx: OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable),
                },
                tags: if tags.is_empty() {
                    NodeTags::default()
                } else {
                    tags.parse().expect("valid tags")
                },
            },
        }
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
            records: [first.clone(), second.clone(), stale, departed.clone()]
                .into_iter()
                .map(|claim| (claim.identity.node_id().clone(), committed(claim, 7, "")))
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

    #[test]
    fn each_node_prints_its_record_properties_and_tags_after_its_prerequisites() {
        use std::num::NonZeroU64;

        use nervix_models::{
            ClusterNodeIdentity, ExecutionProviders, GpuDevice, GpuDevices, NodeCapabilities,
            OnnxRuntime, UndetectedReason,
        };

        let identity = |node: &str, incarnation| {
            ClusterNodeIdentity::new(name(node), ClusterNodeIncarnation::new(incarnation))
        };
        let claim = |node: &str, incarnation| NodeCapabilityRecord {
            identity: identity(node, incarnation),
            capabilities: NodeCapabilities {
                cpu_count: NonZeroU64::new(4),
                memory_bytes: NonZeroU64::new(8 << 30),
                receive: nervix_interconnect::PeerReceiveSettings::default().bounds(),
            },
        };
        let text = |text: &str| PropertyText::new(text).expect("short printable text");
        let mut gpu_node = committed(claim("node-2", 5), 42, "zone=eu,pool=gpu");
        gpu_node.record.properties = NodeProperties {
            architecture: text("x86_64"),
            cpu_model: Detected::Value(text("Test CPU")),
            gpus: GpuInventory::Devices {
                devices: GpuDevices::try_from(vec![
                    GpuDevice {
                        model: Detected::Value(text("Test GPU")),
                        memory_bytes: Detected::Value(NonZeroU64::new(16 << 30).expect("positive")),
                    },
                    GpuDevice {
                        model: Detected::Undetected(UndetectedReason::QueryFailed),
                        memory_bytes: Detected::Value(NonZeroU64::new(8 << 30).expect("positive")),
                    },
                ])
                .expect("two devices"),
                driver: Detected::Value(text("550.54")),
            },
            onnx: OnnxRuntimeDetection::Loaded(OnnxRuntime {
                version: Detected::Value(text("1.30.0")),
                providers: Detected::Value(
                    ExecutionProviders::try_from(vec![
                        text("CUDAExecutionProvider"),
                        text("CPUExecutionProvider"),
                    ])
                    .expect("two providers"),
                ),
                cuda: CudaProviderRegistration::Registered,
            }),
        };
        let description = ClusterDescription {
            identity: None,
            leader: Some(name("node-1")),
            leader_record: Some(record(250)),
            nodes: BTreeMap::from([
                (name("node-1"), node("node-1", 1, Some(record(250)))),
                (name("node-2"), node("node-2", 5, Some(record(250)))),
                (name("node-3"), node("node-3", 6, None)),
            ]),
            records: BTreeMap::from([
                (name("node-1"), committed(claim("node-1", 1), 3, "")),
                (name("node-2"), gpu_node),
            ]),
        };
        let lines = description.lines();
        let block = |node: &str| -> Vec<String> {
            let header = format!("node: {node}");
            let start = lines
                .iter()
                .position(|line| line.starts_with(&header))
                .expect("the node has a block");
            lines[start..]
                .iter()
                .skip(1)
                .take_while(|line| line.starts_with("  "))
                .cloned()
                .collect()
        };
        let gpu_block = block("node-2");
        let record_at = gpu_block
            .iter()
            .position(|line| line == "  record: revision 42 (incarnation 5)")
            .expect("the record names its revision and run");
        let prerequisites_at = gpu_block
            .iter()
            .position(|line| line.starts_with("  raft-heartbeat-interval: "))
            .expect("the prerequisites are printed");
        assert!(
            prerequisites_at < record_at,
            "the record follows the prerequisites"
        );
        assert_eq!(
            gpu_block[record_at..].to_vec(),
            vec![
                "  record: revision 42 (incarnation 5)",
                "  properties:",
                "    architecture: x86_64",
                "    cpu.model: Test CPU",
                "    gpu.count: 2",
                "    gpu.models: Test GPU, undetected (the library refused the query)",
                "    gpu.memory: 8589934592",
                "    gpu.driver: 550.54",
                "    onnx.version: 1.30.0",
                "    onnx.providers: CUDAExecutionProvider, CPUExecutionProvider",
                "    onnx.cuda: true",
                "    cuda.available: true",
                "  tags:",
                "    pool=gpu",
                "    zone=eu",
            ]
        );
        let cpu_block = block("node-1");
        assert!(cpu_block.contains(
            &"    gpu.count: 0 (undetected: the library that reports it did not load)".to_string()
        ));
        assert!(
            cpu_block.contains(
                &"    onnx.cuda: false (undetected: the library that reports it did not load)"
                    .to_string()
            )
        );
        assert!(cpu_block.contains(&"    cuda.available: false".to_string()));
        assert!(cpu_block.contains(&"  tags: none".to_string()));
        let unannounced = block("node-3");
        assert!(unannounced.contains(&"  prerequisites: not published".to_string()));
        assert!(unannounced.contains(&"  record: not announced".to_string()));
    }

    #[test]
    fn a_node_whose_nvml_reports_no_device_has_no_gpu_and_names_no_driver() {
        use nervix_models::UndetectedReason;

        let properties = NodeProperties {
            architecture: PropertyText::new("x86_64").expect("short printable text"),
            cpu_model: Detected::Undetected(UndetectedReason::NotReported),
            gpus: GpuInventory::NoDevice,
            onnx: OnnxRuntimeDetection::Undetected(UndetectedReason::LibraryUnavailable),
        };
        let lines = property_lines(&properties);
        for expected in [
            "    gpu.count: 0",
            "    gpu.models: none",
            "    gpu.memory: none",
            "    gpu.driver: none",
            "    cuda.available: false",
        ] {
            assert!(
                lines.contains(&expected.to_string()),
                "{expected} in {lines:?}"
            );
        }
    }
}
