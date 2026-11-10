//! The runtime activation implied by two domain schedules.
//!
//! Layer: decisions.
//!
//! - **Owns.** Pure classification of schedule changes into unchanged, dynamic, entity-swap and
//!   domain-rebuild activation decisions.
//! - **Depends on.** Scheduled vocabulary and model-change classification.
//! - **Must not know.** Runtime tasks, control-plane coordination, persistence or presentation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::collections::BTreeSet;

use nervix_models::{
    BranchName, DomainSchedule, DynamicModelUpdate, ModelKind, NodeRef, NodeUnitRef, QuiesceLevel,
    RelayName, ResolvedBranching, ScheduledNode, WasmStateResetPhase,
};
use sorted_vec::SortedSet;

use super::{entity_gate::entity_input_relays, graph::is_schedulable_model};

/// How the partition epochs of two schedules differ, branch by branch.
struct EpochChanges {
    /// Branches whose count changed: a committed change began the next epoch.
    recounted: BTreeSet<BranchName>,
    /// Branches whose epoch and count are the same and whose transition ended.
    retired: BTreeSet<BranchName>,
}

impl EpochChanges {
    fn between(existing: &DomainSchedule, desired: &DomainSchedule) -> Self {
        let mut recounted = BTreeSet::new();
        let mut retired = BTreeSet::new();
        for (branch, epoch) in &desired.partition_epochs {
            let Some(current) = existing.partition_epochs.get(branch) else {
                recounted.insert(branch.clone());
                continue;
            };
            if current == epoch {
                continue;
            }
            if current.number == epoch.number && current.partitions == epoch.partitions {
                retired.insert(branch.clone());
            } else {
                recounted.insert(branch.clone());
            }
        }
        for branch in existing.partition_epochs.keys() {
            if !desired.partition_epochs.contains_key(branch) {
                recounted.insert(branch.clone());
            }
        }
        Self { recounted, retired }
    }
}

/// The schedule residue a node may change without a rebuild, decided by how its model changed and
/// whether its branch's partition epoch changed.
#[derive(Debug, Clone, Copy)]
struct ScheduleResidueChanges {
    /// An emitter's schema fingerprint, which an entity-level model change may move.
    schema_fingerprint: bool,
    /// A reingestor's or generator's model-derived residue.
    model_derived: bool,
    /// An ingestor's unit schedule.
    ingestor_schedule: bool,
    /// The partition count the node's branch places keys by.
    partitions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScheduleDelta {
    Unchanged,
    Dynamic(Vec<DynamicModelUpdate>),
    EntitySwap {
        entities: Vec<NodeRef>,
        reassignments: Vec<NodeUnitRef>,
        dynamic_updates: Vec<DynamicModelUpdate>,
    },
    Rebuild,
}

impl ScheduleDelta {
    pub(crate) fn between(
        existing: Option<&DomainSchedule>,
        desired: Option<&DomainSchedule>,
    ) -> Self {
        match (existing, desired) {
            (Some(existing), Some(desired)) => Self::classify(existing, desired),
            (None, None) => Self::Unchanged,
            (Some(existing), None) => {
                if Self::has_executable_nodes(existing) {
                    Self::Rebuild
                } else {
                    Self::Dynamic(Vec::new())
                }
            }
            (None, Some(desired)) => {
                if Self::has_executable_nodes(desired) {
                    Self::Rebuild
                } else {
                    Self::Dynamic(Vec::new())
                }
            }
        }
    }

    /// Whether `desired` changes the partition count of a branch `existing` places keys by,
    /// beginning the branch's next partition epoch.
    pub(crate) fn changes_a_partition_count(
        existing: &DomainSchedule,
        desired: &DomainSchedule,
    ) -> bool {
        !EpochChanges::between(existing, desired)
            .recounted
            .is_empty()
    }

    fn has_executable_nodes(schedule: &DomainSchedule) -> bool {
        schedule
            .nodes
            .values()
            .any(|node| is_schedulable_model(node.config.as_ref()))
    }

    pub(crate) fn classify(existing: &DomainSchedule, desired: &DomainSchedule) -> Self {
        if existing == desired {
            return Self::Unchanged;
        }
        if existing.domain != desired.domain {
            return Self::Rebuild;
        }

        let mut updates = Vec::new();
        let mut entities = Vec::new();
        let mut reassignments = Vec::new();
        // A branch whose partition count changes moves its keys between units of the same
        // entities, so every entity it reaches is replaced in place rather than the domain rebuilt.
        // A retired transition changes only the entities that keep state per key, and in place:
        // their units stop claiming keys and their holding units end, while every task that keeps
        // its unit keeps running with its input.
        let epochs = EpochChanges::between(existing, desired);
        let mut moving = Self::repartitioned_entities(existing, &epochs.recounted);
        moving.extend(Self::repartitioned_entities(desired, &epochs.recounted));
        let mut retiring = Self::keyed_entities(existing, &epochs.retired);
        retiring.extend(Self::keyed_entities(desired, &epochs.retired));
        for (identity, desired_node) in &desired.nodes {
            let Some(existing_node) = existing.nodes.get(identity) else {
                if is_schedulable_model(desired_node.config.as_ref()) {
                    return Self::Rebuild;
                }
                continue;
            };
            let repartitions = moving.contains(identity);
            let retires = retiring.contains(identity);
            for (unit, placement) in &desired_node.units {
                if existing_node.units.get(unit).is_none_or(|existing| {
                    existing.primary_node != placement.primary_node
                        || existing.assigned_nodes != placement.assigned_nodes
                }) {
                    reassignments.push(NodeUnitRef::new(desired_node.identity(), *unit));
                }
            }
            if !repartitions
                && !retires
                && existing_node
                    .units
                    .keys()
                    .any(|unit| !desired_node.units.contains_key(unit))
            {
                return Self::Rebuild;
            }
            if repartitions {
                entities.push(desired_node.identity());
            }
            // A swap of the same entity for another change starts its units outside the
            // transition, which leaves the retirement nothing to do there.
            if retires {
                updates.push(DynamicModelUpdate::PartitionTransitionRetired {
                    node: desired_node.identity(),
                });
            }
            let aspects = existing_node
                .config
                .change_aspects_against(&desired_node.config);
            let level = aspects.quiesce_level();
            let mut resets = Vec::new();
            for (unit, desired) in &desired_node.units {
                if existing_node
                    .units
                    .get(unit)
                    .is_some_and(|existing| existing.wasm_state_reset != desired.wasm_state_reset)
                    && let Some(reset) = &desired.wasm_state_reset
                    && !resets.contains(reset)
                {
                    resets.push(reset.clone());
                }
            }
            for reset in resets {
                updates.push(DynamicModelUpdate::WasmStateReset {
                    processor: desired_node.identifier.clone(),
                    reset,
                });
            }
            // A coordinated reset replaces its selected branch tasks inside their unit supervisor.
            // Binding changes and forced recovery still replace the complete processor.
            let generations_changed = desired_node.units.iter().any(|(unit, desired)| {
                existing_node.units.get(unit).is_some_and(|existing| {
                    existing.wasm_state_generations != desired.wasm_state_generations
                        && !Self::is_coordinated_wasm_reset(existing, desired)
                })
            });
            if generations_changed {
                entities.push(desired_node.identity());
            }
            let emitter_schema_fingerprint_may_change =
                desired_node.kind() == ModelKind::Emitter && level == QuiesceLevel::EntityPause;
            let model_derived_residue_may_change = matches!(
                desired_node.kind(),
                ModelKind::Reingestor | ModelKind::Generator
            ) && level == QuiesceLevel::EntityPause;
            let ingestor_schedule_residue_may_change = desired_node.kind() == ModelKind::Ingestor
                && (level == QuiesceLevel::EntityPause || repartitions);
            if !Self::same_schedule_residue(
                existing_node,
                desired_node,
                ScheduleResidueChanges {
                    schema_fingerprint: emitter_schema_fingerprint_may_change,
                    model_derived: model_derived_residue_may_change,
                    ingestor_schedule: ingestor_schedule_residue_may_change,
                    partitions: repartitions,
                },
            ) {
                return Self::Rebuild;
            }
            if aspects.is_empty() {
                continue;
            }
            match level {
                QuiesceLevel::Dynamic if aspects.is_control_plane_only() => {}
                QuiesceLevel::Dynamic => {
                    if aspects.dynamic_updates().is_empty() {
                        return Self::Rebuild;
                    }
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::EntityPause => {
                    // A declaration without a runtime of its own, such as a branch, reaches the
                    // runtime through the entities that resolve it.
                    if is_schedulable_model(desired_node.config.as_ref()) {
                        entities.push(desired_node.identity());
                    }
                    updates.extend(aspects.dynamic_updates().iter().cloned());
                }
                QuiesceLevel::DomainPause => return Self::Rebuild,
            }
        }
        if existing.nodes.iter().any(|(identity, existing_node)| {
            !desired.nodes.contains_key(identity)
                && is_schedulable_model(existing_node.config.as_ref())
        }) {
            return Self::Rebuild;
        }

        if entities.is_empty() && reassignments.is_empty() {
            // The schedules still differ somewhere the runtime does not execute, such as a
            // placement definition. Publishing it keeps the stored schedule truthful.
            return Self::Dynamic(updates);
        }
        Self::EntitySwap {
            entities: SortedSet::from_unsorted(entities).into_vec(),
            reassignments: SortedSet::from_unsorted(reassignments).into_vec(),
            dynamic_updates: updates,
        }
    }

    fn is_coordinated_wasm_reset(
        existing: &nervix_models::ScheduledUnit,
        desired: &nervix_models::ScheduledUnit,
    ) -> bool {
        let Some(reset) = desired.wasm_state_reset.as_ref() else {
            return false;
        };
        if reset.phase() != WasmStateResetPhase::Publishing
            || existing
                .wasm_state_reset
                .as_ref()
                .is_some_and(|current| current.request() == reset.request())
        {
            return false;
        }
        let (Some(existing_generations), Some(desired_generations)) = (
            existing.wasm_state_generations.as_ref(),
            desired.wasm_state_generations.as_ref(),
        ) else {
            return false;
        };
        let mut expected = existing_generations.clone();
        expected.begin_reset(reset.scope());
        &expected == desired_generations
    }

    pub(crate) const fn quiesce_level(&self) -> QuiesceLevel {
        match self {
            Self::Unchanged | Self::Dynamic(_) => QuiesceLevel::Dynamic,
            Self::EntitySwap { .. } => QuiesceLevel::EntityPause,
            Self::Rebuild => QuiesceLevel::DomainPause,
        }
    }

    pub(crate) fn entity_gate_entities(&self) -> Vec<NodeRef> {
        let Self::EntitySwap {
            entities,
            reassignments,
            ..
        } = self
        else {
            return Vec::new();
        };
        SortedSet::from_unsorted(
            entities
                .iter()
                .cloned()
                .chain(reassignments.iter().map(|unit| unit.node.clone()))
                .collect(),
        )
        .into_vec()
    }

    /// Every node of `schedule` that keeps state per key in a unit of one of `branches`: each
    /// deduplicator, window and WASM processor the branches divide, and each materialized relay
    /// they branch.
    fn keyed_entities(
        schedule: &DomainSchedule,
        branches: &BTreeSet<BranchName>,
    ) -> BTreeSet<NodeRef> {
        let mut entities = BTreeSet::new();
        if branches.is_empty() {
            return entities;
        }
        for node in schedule.nodes.values() {
            let Some(branch) = node.partition_branch() else {
                continue;
            };
            if branches.contains(branch) && node.keeps_state_per_key() {
                entities.insert(node.identity());
            }
        }
        entities
    }

    /// Every entity of `schedule` a change of the partition epochs of `branches` replaces: each
    /// relay they branch, each node they divide into units, and each node producing into, reading
    /// from or resolving materialized state in one of those relays.
    fn repartitioned_entities(
        schedule: &DomainSchedule,
        branches: &BTreeSet<BranchName>,
    ) -> BTreeSet<NodeRef> {
        let mut entities = BTreeSet::new();
        if branches.is_empty() {
            return entities;
        }
        let mut relays = BTreeSet::<RelayName>::new();
        for node in schedule.nodes.values() {
            let branch = match node.resolved_branching.as_ref() {
                Some(branching) => branching.branch(),
                None => None,
            };
            let Some(branch) = branch else {
                continue;
            };
            if !branches.contains(branch) {
                continue;
            }
            if node.kind() == ModelKind::Relay {
                relays.insert(RelayName::from(&node.identifier));
                entities.insert(node.identity());
            } else if node.partition_branch().is_some() {
                entities.insert(node.identity());
            }
        }
        for node in schedule.nodes.values() {
            let config = node.config.as_ref();
            let mut reaches = false;
            for relay in entity_input_relays(config) {
                if relays.contains(&relay) {
                    reaches = true;
                }
            }
            for relay in config.materialized_state_relays() {
                if relays.contains(relay) {
                    reaches = true;
                }
            }
            if let Some(outputs) = config.output_routes() {
                for relay in outputs.relays() {
                    if relays.contains(relay) {
                        reaches = true;
                    }
                }
            }
            if reaches {
                entities.insert(node.identity());
            }
        }
        entities
    }

    /// Whether two resolutions name the same branch with the same key schema, whatever partition
    /// count each places keys by.
    fn same_branching_apart_from_partitions(
        existing: Option<&ResolvedBranching>,
        desired: Option<&ResolvedBranching>,
    ) -> bool {
        match (existing, desired) {
            (Some(existing), Some(desired)) => existing.same_key_as(desired),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        }
    }

    fn same_schedule_residue(
        existing: &ScheduledNode,
        desired: &ScheduledNode,
        allowed: ScheduleResidueChanges,
    ) -> bool {
        let ScheduleResidueChanges {
            schema_fingerprint: allow_schema_fingerprint_change,
            model_derived: allow_model_derived_residue_change,
            ingestor_schedule: allow_ingestor_schedule_residue_change,
            partitions: allow_partition_change,
        } = allowed;
        let ScheduledNode {
            identifier: existing_identifier,
            resolved_branching: existing_resolved_branching,
            schema_fingerprint: existing_schema_fingerprint,
            kafka_partition_schedule: existing_kafka_partition_schedule,
            ..
        } = existing;
        let ScheduledNode {
            identifier: desired_identifier,
            resolved_branching: desired_resolved_branching,
            schema_fingerprint: desired_schema_fingerprint,
            kafka_partition_schedule: desired_kafka_partition_schedule,
            ..
        } = desired;

        if existing_identifier != desired_identifier || existing.kind() != desired.kind() {
            return false;
        }
        if allow_ingestor_schedule_residue_change {
            return true;
        }

        if !allow_model_derived_residue_change {
            let resolved_branching_matches = if allow_partition_change {
                Self::same_branching_apart_from_partitions(
                    existing_resolved_branching.as_ref(),
                    desired_resolved_branching.as_ref(),
                )
            } else {
                existing_resolved_branching == desired_resolved_branching
            };
            if !resolved_branching_matches {
                return false;
            }

            if !allow_schema_fingerprint_change {
                let schema_fingerprint_matches =
                    existing_schema_fingerprint == desired_schema_fingerprint;
                if !schema_fingerprint_matches {
                    return false;
                }
            }
        }

        let kafka_partition_schedule_matches =
            existing_kafka_partition_schedule == desired_kafka_partition_schedule;
        if !kafka_partition_schedule_matches {
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use nervix_models::{
        AckMode, BranchKeyFingerprint, BranchSelection, ClusterNodeName, CommandExecutionReference,
        CreateEmitter, CreateIngestor, CreateJunction, CreatePlacement, CreateRelay, CreateVhost,
        CreateWasmProcessor, DomainName, DomainSchedule, DynamicModelUpdate, EmitterEntity,
        EndpointIngestMode, ErrorPolicies, Expression, FlushPolicy, GeneralErrorPolicy,
        HttpEndpointIngestor, HttpIngestor, IngestorEntity, Literal, Model, ModelKind, NodeRef,
        NodeUnitRef, OutputBranch, PlacementPolicy, ProcessorInputs, ProcessorOutput,
        ProcessorOutputs, QuiesceLevel, RelayBranching, ResolvedBranching, RetryPolicy,
        RouteConstruction, ScheduledNode, SchemaFingerprint, VhostTlsResource, WasmProcessorLimits,
        WasmStateResetScope, ZeroMqEmitter, ZeroMqPushEmitter,
    };
    use nonzero_ext::nonzero;

    use super::ScheduleDelta;

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    fn push_scheduled(schedule: &mut DomainSchedule, node: ScheduledNode) {
        schedule.nodes.insert(node.identity(), node);
    }

    fn first_scheduled(schedule: DomainSchedule) -> ScheduledNode {
        schedule
            .nodes
            .into_values()
            .next()
            .expect("fixture schedule must contain a node")
    }

    fn placement_node(policy: PlacementPolicy) -> ScheduledNode {
        ScheduledNode::new(
            Model::Placement(
                CreatePlacement::new(
                    named("keep_local"),
                    vec![named("event_source")],
                    vec![named("events")],
                    policy,
                    Some(nonzero!(1u64)),
                )
                .expect("valid placement"),
            ),
            SchemaFingerprint::from_digest([1; 32]),
        )
    }

    fn schedule(capacity: NonZeroUsize) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(
                    Model::Relay(CreateRelay {
                        labels: Default::default(),
                        name: named("events"),
                        schema: named("event"),
                        buffer: capacity,
                        branching: RelayBranching::unbranched(),
                        materialized_state: None,
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )
                .with_resolved_branching(Some(ResolvedBranching::unbranched()))
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        )
    }

    fn ingestor_schedule(endpoint: &str) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(
                    Model::Ingestor(CreateIngestor {
                        labels: Default::default(),
                        name: named("event_source"),
                        output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                            relay: named("events"),
                            construction: RouteConstruction::default(),
                            flush_policy: Some(FlushPolicy::Immediate),
                            message_error_policy: nervix_models::MessageErrorPolicy::Log,
                            branch: Some(OutputBranch::Unbranched),
                        }]),
                        input: nervix_models::IngestorInput::Transport(
                            nervix_models::TransportIngestorInput {
                                source: IngestorEntity::Http(HttpIngestor::Endpoint(
                                    HttpEndpointIngestor {
                                        endpoint: named(endpoint),
                                        mode: EndpointIngestMode::NoAck,
                                        quiesce: nervix_models::IngestQuiesceMode::EndpointBuffer {
                                            max_size: "1MiB".to_string(),
                                        },
                                    },
                                )),
                                codec: named("event_codec"),
                            },
                        ),
                        timestamp_source: None,
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        )
    }

    fn wasm_processor_schedule(owner: &str) -> DomainSchedule {
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(
                    Model::WasmProcessor(CreateWasmProcessor {
                        labels: Default::default(),
                        name: named("counting_guest"),
                        from: ProcessorInputs::single(named("events")),
                        output_routes: ProcessorOutputs::single(named("counted_events")),
                        branched_by: BranchSelection::unbranched(),
                        resource: named("counting_bundle"),
                        resource_version: 1,
                        file: "processors/counting.wasm".to_string(),
                        limits: WasmProcessorLimits {
                            max_fuel: nonzero!(1_000_000u64),
                            max_memory_bytes: nonzero!(67_108_864u64),
                        },
                        global_error_policy: GeneralErrorPolicy::Log,
                        rejected_state_policy: Default::default(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )
                .placed_on(
                    Some(ClusterNodeName::parse(owner).expect("valid name")),
                    vec![
                        ClusterNodeName::parse("node-1").expect("valid name"),
                        ClusterNodeName::parse("node-2").expect("valid name"),
                    ],
                ),
            ],
            Vec::new(),
        )
    }

    fn wasm_processor_ref() -> NodeRef {
        NodeRef {
            kind: ModelKind::WasmProcessor,
            identifier: named("counting_guest"),
        }
    }

    #[test]
    fn unchanged_schedule_has_no_apply_work() {
        let existing = schedule(nonzero!(1usize));
        assert_eq!(
            ScheduleDelta::classify(&existing, &existing),
            ScheduleDelta::Unchanged
        );
    }

    #[test]
    fn control_plane_only_schedule_appearance_and_disappearance_are_dynamic() {
        let control_plane_only = DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![placement_node(PlacementPolicy::PreferColocation)],
            Vec::new(),
        );

        assert_eq!(
            ScheduleDelta::between(None, Some(&control_plane_only)),
            ScheduleDelta::Dynamic(Vec::new())
        );
        assert_eq!(
            ScheduleDelta::between(Some(&control_plane_only), None),
            ScheduleDelta::Dynamic(Vec::new())
        );
    }

    #[test]
    fn executable_schedule_appearance_and_disappearance_rebuild() {
        let executable = schedule(nonzero!(1usize));

        assert_eq!(
            ScheduleDelta::between(None, Some(&executable)),
            ScheduleDelta::Rebuild
        );
        assert_eq!(
            ScheduleDelta::between(Some(&executable), None),
            ScheduleDelta::Rebuild
        );
    }

    #[test]
    fn relay_capacity_is_a_dynamic_delta() {
        let existing = schedule(nonzero!(1usize));
        let desired = schedule(nonzero!(5usize));
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::RelayCapacity {
                relay: named("events"),
                capacity: nonzero!(5usize),
            }])
        );
    }

    #[test]
    fn a_tls_version_change_is_a_dynamic_listener_refresh_while_another_bundle_rebuilds() {
        fn vhost_schedule(resource: &str, version: u64) -> DomainSchedule {
            DomainSchedule::new(
                DomainName::parse("testing").expect("valid domain"),
                vec![ScheduledNode::new(
                    Model::Vhost(CreateVhost {
                        labels: Default::default(),
                        name: named("edge"),
                        hostnames: vec!["edge.example.com".to_string()],
                        tls: Some(VhostTlsResource {
                            resource: named(resource),
                            version,
                        }),
                    }),
                    SchemaFingerprint::from_digest([1; 32]),
                )],
                Vec::new(),
            )
        }

        let existing = vhost_schedule("edge_tls", 1);
        let rotated = vhost_schedule("edge_tls", 2);
        let delta = ScheduleDelta::classify(&existing, &rotated);
        assert_eq!(
            delta,
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::VhostTlsVersion {
                vhost: named("edge"),
            }])
        );
        assert_eq!(delta.quiesce_level(), QuiesceLevel::Dynamic);
        assert_eq!(
            ScheduleDelta::classify(&existing, &vhost_schedule("other_tls", 1)),
            ScheduleDelta::Rebuild
        );
    }

    #[test]
    fn schema_and_schedule_residue_changes_rebuild() {
        let existing = schedule(nonzero!(1usize));
        let mut schema_change = schedule(nonzero!(1usize));
        let Model::Relay(relay) = schema_change.nodes[0].config.as_mut() else {
            panic!("test node should contain a relay");
        };
        relay.schema = named("event_v2");
        assert_eq!(
            ScheduleDelta::classify(&existing, &schema_change),
            ScheduleDelta::Rebuild
        );

        let mut fingerprint_change = schedule(nonzero!(1usize));
        fingerprint_change.nodes[0].schema_fingerprint = SchemaFingerprint::from_digest([2; 32]);
        assert_eq!(
            ScheduleDelta::classify(&existing, &fingerprint_change),
            ScheduleDelta::Rebuild
        );
    }

    #[test]
    fn junction_filter_is_dynamic_and_attachment_changes_swap_the_entity() {
        let junction = CreateJunction {
            labels: Default::default(),
            name: named("route_events"),
            from: ProcessorInputs::single(named("incoming")),
            output_routes: ProcessorOutputs::new(vec![ProcessorOutput::with_flush_policy(
                named("outgoing"),
                FlushPolicy::Each {
                    interval: "100ms".to_string(),
                    max_batch_size: "1MiB".to_string(),
                },
            )]),
            branched_by: BranchSelection::unbranched(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(
                    Model::Junction(junction.clone()),
                    SchemaFingerprint::from_digest([1; 32]),
                )
                .with_resolved_branching(Some(ResolvedBranching::unbranched()))
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        );
        let mut dynamic_config = junction;
        dynamic_config.filter_where = Some(Expression::Literal(Literal::Bool(true)));
        let mut desired = existing.clone();
        *desired.nodes[0].config = Model::Junction(dynamic_config);
        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Processor {
                kind: ModelKind::Junction,
                processor: named("route_events"),
            }])
        );

        let mut structural = existing.clone();
        let Model::Junction(config) = structural.nodes[0].config.as_mut() else {
            panic!("test node should contain a junction");
        };
        config.mode = AckMode::Detached;
        assert_eq!(
            ScheduleDelta::classify(&existing, &structural),
            ScheduleDelta::EntitySwap {
                entities: vec![NodeRef {
                    kind: ModelKind::Junction,
                    identifier: named("route_events"),
                }],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn ingestor_changes_swap_even_when_schedule_placement_residue_changes() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_b");
        desired.nodes[0].schema_fingerprint = SchemaFingerprint::from_digest([2; 32]);
        desired.nodes[0].single_unit_mut().primary_node =
            Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].single_unit_mut().assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];

        let ScheduleDelta::EntitySwap {
            entities,
            reassignments,
            dynamic_updates,
        } = ScheduleDelta::classify(&existing, &desired)
        else {
            panic!("ingestor changes should use entity swap");
        };
        assert_eq!(entities.len(), 1);
        assert_eq!(entities[0].kind, ModelKind::Ingestor);
        assert_eq!(entities[0].identifier, named("event_source"));
        assert_eq!(
            reassignments,
            entities
                .into_iter()
                .map(|node| NodeUnitRef::new(node, nervix_models::Unit::SINGLE))
                .collect::<Vec<_>>()
        );
        assert!(dynamic_updates.is_empty());
    }

    #[test]
    fn emitter_flush_is_dynamic_and_client_changes_swap_despite_fingerprint_changes() {
        let emitter = CreateEmitter {
            labels: Default::default(),
            instances: nervix_models::EmitterInstances::ONE,
            name: named("event_emitter"),
            from: nervix_models::ProcessorInputs::single(named("events")),
            body: nervix_models::EmitterBody::Codec {
                codec: named("event_codec"),
            },
            envelope: Box::new(nervix_models::EmitterEnvelope::default()),
            connector: Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
                ZeroMqPushEmitter {
                    mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                        retry_policy: RetryPolicy {
                            backoff: "250ms".to_string(),
                            max_backoff: "30s".to_string(),
                        },
                    },
                    client: named("emitter_a"),
                },
            ))),
            batch: None,
            flush_policy: FlushPolicy::Each {
                interval: "30s".to_string(),
                max_batch_size: "1MiB".to_string(),
            },
            error_policies: ErrorPolicies::handled_by_log(),
            mode: AckMode::Attached,
            construction: RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let existing = DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![
                ScheduledNode::new(
                    Model::Emitter(emitter.clone()),
                    SchemaFingerprint::from_digest([1; 32]),
                )
                .with_resolved_branching(Some(ResolvedBranching::unbranched()))
                .placed_on(
                    Some(ClusterNodeName::parse("node-1").expect("valid name")),
                    vec![ClusterNodeName::parse("node-1").expect("valid name")],
                ),
            ],
            Vec::new(),
        );

        let mut dynamic_emitter = emitter.clone();
        dynamic_emitter.flush_policy = FlushPolicy::Immediate;
        let mut dynamic = existing.clone();
        *dynamic.nodes[0].config = Model::Emitter(dynamic_emitter.clone());
        assert_eq!(
            ScheduleDelta::classify(&existing, &dynamic),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::Emitter {
                emitter: named("event_emitter"),
                config: Box::new(dynamic_emitter),
            }])
        );

        let mut swapped_emitter = emitter;
        swapped_emitter.connector = Box::new(EmitterEntity::ZeroMq(ZeroMqEmitter::Push(
            ZeroMqPushEmitter {
                mode: nervix_models::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: named("emitter_b"),
            },
        )));
        let mut swapped = existing.clone();
        *swapped.nodes[0].config = Model::Emitter(swapped_emitter);
        swapped.nodes[0].schema_fingerprint = SchemaFingerprint::from_digest([2; 32]);
        assert_eq!(
            ScheduleDelta::classify(&existing, &swapped),
            ScheduleDelta::EntitySwap {
                entities: vec![NodeRef {
                    kind: ModelKind::Emitter,
                    identifier: named("event_emitter"),
                }],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn owner_change_alone_reassigns_without_a_rebuild() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].single_unit_mut().primary_node =
            Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].single_unit_mut().assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeUnitRef::new(
                    NodeRef {
                        kind: ModelKind::Ingestor,
                        identifier: named("event_source"),
                    },
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn replica_set_change_alone_reassigns_without_a_rebuild() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].single_unit_mut().assigned_nodes = vec![
            ClusterNodeName::parse("node-1").expect("valid name"),
            ClusterNodeName::parse("node-3").expect("valid name"),
        ];

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeUnitRef::new(
                    NodeRef {
                        kind: ModelKind::Ingestor,
                        identifier: named("event_source"),
                    },
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn adding_a_placement_only_applies_the_assignments_it_changes() {
        let existing = ingestor_schedule("ingress_a");
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].single_unit_mut().primary_node =
            Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].single_unit_mut().assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];
        push_scheduled(
            &mut desired,
            placement_node(PlacementPolicy::RequireColocation),
        );

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeUnitRef::new(
                    NodeRef {
                        kind: ModelKind::Ingestor,
                        identifier: named("event_source"),
                    },
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn a_placement_policy_change_only_applies_its_reassignments() {
        let mut existing = ingestor_schedule("ingress_a");
        push_scheduled(
            &mut existing,
            placement_node(PlacementPolicy::PreferColocation),
        );
        let mut desired = ingestor_schedule("ingress_a");
        desired.nodes[0].single_unit_mut().primary_node =
            Some(ClusterNodeName::parse("node-2").expect("valid name"));
        desired.nodes[0].single_unit_mut().assigned_nodes =
            vec![ClusterNodeName::parse("node-2").expect("valid name")];
        push_scheduled(
            &mut desired,
            placement_node(PlacementPolicy::RequireColocation),
        );

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeUnitRef::new(
                    NodeRef {
                        kind: ModelKind::Ingestor,
                        identifier: named("event_source"),
                    },
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn a_model_change_and_an_unrelated_move_apply_together() {
        let mut existing = schedule(nonzero!(1usize));
        push_scheduled(
            &mut existing,
            first_scheduled(ingestor_schedule("ingress_a")),
        );
        let mut desired = schedule(nonzero!(5usize));
        let mut moved_ingestor = first_scheduled(ingestor_schedule("ingress_a"));
        moved_ingestor.single_unit_mut().primary_node =
            Some(ClusterNodeName::parse("node-3").expect("valid name"));
        moved_ingestor.single_unit_mut().assigned_nodes =
            vec![ClusterNodeName::parse("node-3").expect("valid name")];
        push_scheduled(&mut desired, moved_ingestor);

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: Vec::new(),
                reassignments: vec![NodeUnitRef::new(
                    NodeRef {
                        kind: ModelKind::Ingestor,
                        identifier: named("event_source"),
                    },
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: vec![DynamicModelUpdate::RelayCapacity {
                    relay: named("events"),
                    capacity: nonzero!(5usize),
                }],
            }
        );
    }

    #[test]
    fn a_guest_state_lifetime_change_swaps_the_processor_where_it_keeps_executing() {
        let existing = wasm_processor_schedule("node-1");
        let mut every_branch = wasm_processor_schedule("node-1");
        every_branch.nodes[0].begin_wasm_state_generation();
        let mut one_branch = wasm_processor_schedule("node-1");
        one_branch.nodes[0].begin_wasm_branch_state_generation(BranchKeyFingerprint::new([3; 32]));
        let swap = ScheduleDelta::EntitySwap {
            entities: vec![wasm_processor_ref()],
            reassignments: Vec::new(),
            dynamic_updates: Vec::new(),
        };

        assert_eq!(ScheduleDelta::classify(&existing, &every_branch), swap);
        assert_eq!(ScheduleDelta::classify(&existing, &one_branch), swap);
        assert_eq!(
            ScheduleDelta::classify(&every_branch, &every_branch),
            ScheduleDelta::Unchanged
        );
    }

    #[test]
    fn a_partition_guest_state_lifetime_change_swaps_without_a_single_unit() {
        let mut existing = wasm_processor_schedule("node-1");
        let single = existing.nodes[0]
            .units
            .shift_remove(&nervix_models::Unit::SINGLE)
            .expect("the fixture starts with one unit");
        existing.nodes[0]
            .units
            .insert(nervix_models::Unit::partition(0), single.clone());
        existing.nodes[0]
            .units
            .insert(nervix_models::Unit::partition(1), single);
        let mut desired = existing.clone();
        desired.nodes[0].units[&nervix_models::Unit::partition(1)]
            .wasm_state_generations
            .as_mut()
            .expect("the processor has guest-state generations")
            .begin_every_branch();

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: vec![wasm_processor_ref()],
                reassignments: Vec::new(),
                dynamic_updates: Vec::new(),
            }
        );
    }

    #[test]
    fn coordinated_reset_publication_and_readiness_are_dynamic_processor_updates() {
        let existing = wasm_processor_schedule("node-1");
        let mut publishing = existing.clone();
        let request = CommandExecutionReference::parse("reset-counting-guest")
            .expect("the reset reference must be valid");
        assert!(publishing.nodes[0].begin_wasm_state_reset(
            request.clone(),
            WasmStateResetScope::Unbranched,
            nervix_models::WasmStateResetReason::Operator,
        ));
        let publishing_reset = publishing.nodes[0]
            .wasm_state_reset()
            .expect("the reset was published")
            .clone();

        assert_eq!(
            ScheduleDelta::classify(&existing, &publishing),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::WasmStateReset {
                processor: named("counting_guest"),
                reset: publishing_reset,
            }])
        );

        let mut ready = publishing.clone();
        assert!(ready.nodes[0].complete_wasm_state_reset(&request));
        let ready_reset = ready.nodes[0]
            .wasm_state_reset()
            .expect("the reset became ready")
            .clone();
        assert_eq!(
            ScheduleDelta::classify(&publishing, &ready),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::WasmStateReset {
                processor: named("counting_guest"),
                reset: ready_reset,
            }])
        );
    }

    #[test]
    fn an_owner_replacement_with_a_new_guest_state_lifetime_swaps_and_reassigns() {
        let existing = wasm_processor_schedule("node-1");
        let mut desired = wasm_processor_schedule("node-2");
        desired.nodes[0].begin_wasm_state_generation();

        assert_eq!(
            ScheduleDelta::classify(&existing, &desired),
            ScheduleDelta::EntitySwap {
                entities: vec![wasm_processor_ref()],
                reassignments: vec![NodeUnitRef::new(
                    wasm_processor_ref(),
                    nervix_models::Unit::SINGLE
                )],
                dynamic_updates: Vec::new(),
            }
        );
    }

    /// One branch of eight partitions with a materialized relay, which keeps state per key, and a
    /// plain relay beside it, both placed on one node, in partition epoch two.
    fn repartitioned_schedule(transition: bool) -> DomainSchedule {
        let branch = || named::<nervix_models::BranchName>("by_tenant");
        let partitions = |count: u16| {
            nervix_models::BranchPartitions::Direct(
                std::num::NonZeroU16::new(count).expect("test counts are positive"),
            )
        };
        let key_schema = nervix_models::CreateSchema {
            labels: Default::default(),
            name: named("tenant_key"),
            fields: Vec::new(),
        };
        let node = || ClusterNodeName::parse("node-1").expect("valid name");
        let relay = |name: &str, materialized: bool| {
            ScheduledNode::new(
                Model::Relay(CreateRelay {
                    labels: Default::default(),
                    name: named(name),
                    schema: named("event"),
                    buffer: nonzero!(8usize),
                    branching: RelayBranching::branched_by(branch()),
                    materialized_state: materialized
                        .then_some(nervix_models::MaterializedRelayState::LastByTimestamp),
                }),
                SchemaFingerprint::from_digest([1; 32]),
            )
            .with_resolved_branching(Some(ResolvedBranching::branched(
                branch(),
                key_schema.clone(),
                partitions(8),
            )))
            .placed_on(Some(node()), vec![node()])
        };
        let epoch = nervix_models::BranchPartitionEpoch {
            number: nervix_models::PartitionEpochNumber::FIRST.next(),
            partitions: partitions(8),
            transition: transition.then_some(nervix_models::PartitionTransition {
                previous: partitions(4),
            }),
        };
        DomainSchedule::new(
            DomainName::parse("testing").expect("valid domain"),
            vec![relay("profiles", true), relay("events", false)],
            Vec::new(),
        )
        .with_partition_epochs(std::collections::BTreeMap::from([(branch(), epoch)]))
    }

    #[test]
    fn a_retired_transition_retires_the_entities_that_keep_state_per_key_in_place() {
        let moving = repartitioned_schedule(true);
        let retired = repartitioned_schedule(false);
        assert_eq!(
            ScheduleDelta::classify(&moving, &retired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::PartitionTransitionRetired {
                node: NodeRef::new(
                    ModelKind::Relay,
                    named::<nervix_models::ModelName>("profiles")
                ),
            }])
        );
        assert!(!ScheduleDelta::changes_a_partition_count(&moving, &retired));
    }

    #[test]
    fn a_retired_shrinking_transition_ends_its_holding_units_without_a_rebuild() {
        let units = |count: u16| {
            (0..u32::from(count))
                .map(nervix_models::Unit::partition)
                .collect::<Vec<_>>()
        };
        let with_profile_units = |schedule: DomainSchedule, count: u16| {
            let mut schedule = schedule;
            let node = schedule
                .nodes
                .get_mut(&NodeRef::new(
                    ModelKind::Relay,
                    named::<nervix_models::ModelName>("profiles"),
                ))
                .expect("the fixture schedules the materialized relay");
            let local = ClusterNodeName::parse("node-1").expect("valid name");
            *node = node
                .clone()
                .with_units(&units(count))
                .placed_on(Some(local.clone()), vec![local]);
            schedule
        };
        // Under the previous count of eight, units 4 to 7 hold the keys the count of four places
        // elsewhere until the transition retires.
        let shrinking = |transition: bool| {
            let mut schedule = repartitioned_schedule(transition);
            let mut epochs = schedule.partition_epochs.clone();
            for epoch in epochs.values_mut() {
                epoch.partitions = nervix_models::BranchPartitions::Direct(nonzero!(4u16));
                epoch.transition = transition.then_some(nervix_models::PartitionTransition {
                    previous: nervix_models::BranchPartitions::Direct(nonzero!(8u16)),
                });
            }
            schedule = schedule.with_partition_epochs(epochs);
            schedule
        };
        let moving = with_profile_units(shrinking(true), 8);
        let retired = with_profile_units(shrinking(false), 4);
        assert_eq!(
            ScheduleDelta::classify(&moving, &retired),
            ScheduleDelta::Dynamic(vec![DynamicModelUpdate::PartitionTransitionRetired {
                node: NodeRef::new(
                    ModelKind::Relay,
                    named::<nervix_models::ModelName>("profiles")
                ),
            }])
        );
    }

    #[test]
    fn a_new_epoch_swaps_every_entity_its_branch_reaches() {
        let settled = repartitioned_schedule(false);
        let mut changed = repartitioned_schedule(true);
        let next = nervix_models::BranchPartitionEpoch {
            number: nervix_models::PartitionEpochNumber::FIRST.next().next(),
            partitions: nervix_models::BranchPartitions::Direct(nonzero!(8u16)),
            transition: Some(nervix_models::PartitionTransition {
                previous: nervix_models::BranchPartitions::Direct(nonzero!(8u16)),
            }),
        };
        changed = changed.with_partition_epochs(std::collections::BTreeMap::from([(
            named::<nervix_models::BranchName>("by_tenant"),
            next,
        )]));
        assert!(ScheduleDelta::changes_a_partition_count(&settled, &changed));
        let ScheduleDelta::EntitySwap { entities, .. } =
            ScheduleDelta::classify(&settled, &changed)
        else {
            panic!("a new partition epoch swaps the branch's entities");
        };
        assert_eq!(
            entities,
            vec![
                NodeRef::new(
                    ModelKind::Relay,
                    named::<nervix_models::ModelName>("events")
                ),
                NodeRef::new(
                    ModelKind::Relay,
                    named::<nervix_models::ModelName>("profiles")
                ),
            ]
        );
    }
}
