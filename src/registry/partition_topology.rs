//! External partition observations resolved against one validated domain graph.
//!
//! Layer: decisions.
//! - **Owns.** Source observation plans, producer eligibility, count agreement and shared grouping.
//! - **Depends on.** Semantic Models, declared connector capabilities and committed vocabulary.
//! - **Must not know.** Broker drivers, asynchronous tasks, locks or runtime records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU16,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    BranchName, BranchPartitions, ClientConfigEntry, ClientName, ClientResourceMount, DomainName,
    DomainSchedule, GeneralErrorPolicy, IngestorEntity, IngestorExternalPartitions, IngestorName,
    InheritedPartitionLayout, KafkaIngestor, KafkaOffsetMode, KafkaTopicName, Model, ModelIndex,
    ModelKind, NodeRef, PartitionSourceFingerprint, PartitionTopology, PulsarIngestor,
    PulsarTopicName, SourcePartitionObservation,
};
use serde::Serialize;
use thiserror::Error;

use super::RegistryError;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum PartitionTopologyError {
    #[error(
        "branch '{branch}' requires an enumerable external partition set; '{producer:?}' cannot \
         construct it"
    )]
    Producer {
        branch: BranchName,
        producer: NodeRef,
    },
    #[error("inherited branch '{branch}' observes different external partition counts: {counts:?}")]
    CountMismatch {
        branch: BranchName,
        counts: BTreeMap<IngestorName, u32>,
    },
    #[error(
        "inherited branch '{branch}' observes {count} partitions, above the maximum {}",
        u16::MAX
    )]
    CountBound { branch: BranchName, count: u32 },
    #[error(
        "inherited branch '{branch}' observes {count} partitions beyond its signed metadata \
         coordinates"
    )]
    MetadataBound { branch: BranchName, count: u32 },
    #[error("partition observation source '{ingestor}' has no matching configured client")]
    Client { ingestor: IngestorName },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PartitionSourceClient {
    pub(crate) name: ClientName,
    pub(crate) mount: Option<ClientResourceMount>,
    pub(crate) config: Vec<ClientConfigEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) enum KafkaPartitionSourcePlan {
    Topic {
        topic: KafkaTopicName,
        client: PartitionSourceClient,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) enum PulsarPartitionSourcePlan {
    Topic {
        topic: PulsarTopicName,
        client: PartitionSourceClient,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) enum PartitionSourcePlan {
    Kafka(KafkaPartitionSourcePlan),
    Pulsar(PulsarPartitionSourcePlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionSourceSpec {
    pub(crate) ingestor: IngestorName,
    pub(crate) source: PartitionSourcePlan,
    pub(crate) fingerprint: PartitionSourceFingerprint,
    pub(crate) branches: BTreeSet<BranchName>,
    pub(crate) general_error_policy: GeneralErrorPolicy,
}

/// Borrowed semantic inputs shared by validation snapshots and the already validated graph.
pub(crate) trait PartitionModels {
    fn models(&self) -> impl Iterator<Item = &Model>;
    fn model(&self, identity: &NodeRef) -> Option<&Model>;
}
impl PartitionModels for ModelIndex {
    fn models(&self) -> impl Iterator<Item = &Model> {
        self.models()
    }
    fn model(&self, identity: &NodeRef) -> Option<&Model> {
        self.get(identity)
    }
}
impl PartitionModels for nervix_models::DomainSchedule {
    fn models(&self) -> impl Iterator<Item = &Model> {
        self.nodes.values().map(|node| node.config.as_ref())
    }
    fn model(&self, identity: &NodeRef) -> Option<&Model> {
        self.nodes.get(identity).map(|node| node.config.as_ref())
    }
}
impl PartitionModels for super::ActiveGraph {
    fn models(&self) -> impl Iterator<Item = &Model> {
        self.graph.node_weights().map(|node| node.config.as_ref())
    }
    fn model(&self, identity: &NodeRef) -> Option<&Model> {
        let index = self.indices.get(identity)?;
        self.graph
            .node_weight(*index)
            .map(|node| node.config.as_ref())
    }
}

/// Source configurations that require observations: every inherited producer and Kafka readers
/// whose offsets Nervix owns. Client and entity projection happens here, before observation runs.
/// Non-security BLAKE3: pins observations to their typed client and locator configuration; it
/// authenticates no peer and grants no source access.
pub(crate) fn source_specs(
    domain: &DomainName,
    models: &impl PartitionModels,
) -> Result<BTreeMap<IngestorName, PartitionSourceSpec>, Report<RegistryError>> {
    let mut sources = BTreeMap::new();
    for model in models.models() {
        let Model::Ingestor(ingestor) = model else {
            continue;
        };
        let mut branches = BTreeSet::new();
        for route in ingestor.output_routes.outputs() {
            let Some(branching) = &route.branch else {
                continue;
            };
            let Some(branch) = branching.constructed_branch() else {
                continue;
            };
            if let Some(Model::Branch(declaration)) =
                models.model(&NodeRef::new(ModelKind::Branch, branch))
                && declaration.partitions.is_inherited()
            {
                branches.insert(branch.clone());
            }
        }
        let entity = ingestor.input.transport_entity();
        let domain_offsets = owns_domain_offsets(ingestor);
        if branches.is_empty() && !domain_offsets {
            continue;
        }
        let missing = || {
            Report::new(PartitionTopologyError::Client {
                ingestor: ingestor.name.clone(),
            })
            .change_context(RegistryError::PartitionTopology {
                domain: domain.clone(),
            })
        };
        let source = match entity {
            Some(IngestorEntity::Kafka(KafkaIngestor::Topic(topic))) => {
                let Some(Model::ClientKafka(client)) =
                    models.model(&NodeRef::new(ModelKind::Client, &topic.client))
                else {
                    return Err(missing());
                };
                PartitionSourcePlan::Kafka(KafkaPartitionSourcePlan::Topic {
                    topic: topic.topic.clone(),
                    client: PartitionSourceClient {
                        name: client.name.clone(),
                        mount: client.mount.clone(),
                        config: client.config.clone(),
                    },
                })
            }
            Some(IngestorEntity::Pulsar(PulsarIngestor::Topic(topic))) => {
                let Some(Model::ClientPulsar(client)) =
                    models.model(&NodeRef::new(ModelKind::Client, &topic.client))
                else {
                    return Err(missing());
                };
                PartitionSourcePlan::Pulsar(PulsarPartitionSourcePlan::Topic {
                    topic: topic.topic.clone(),
                    client: PartitionSourceClient {
                        name: client.name.clone(),
                        mount: client.mount.clone(),
                        config: client.config.clone(),
                    },
                })
            }
            _ => continue,
        };
        let bytes = serde_json::to_vec(&source)
            .verified("source plans contain serializable names, versions and strings");
        let fingerprint = PartitionSourceFingerprint::from_digest(*blake3::hash(&bytes).as_bytes());
        sources.insert(
            ingestor.name.clone(),
            PartitionSourceSpec {
                ingestor: ingestor.name.clone(),
                source,
                fingerprint,
                branches,
                general_error_policy: ingestor.general_error_policy.clone(),
            },
        );
    }
    Ok(sources)
}

/// Rejects construction without position provenance while preserving ordinary branch-preserving
/// processor and reingestor routes.
pub(in crate::registry) fn validate_producers(
    domain: &DomainName,
    models: &ModelIndex,
) -> Result<(), Report<RegistryError>> {
    for (node, model) in models {
        let (routes, enumerable) = match model {
            Model::Ingestor(ingestor) => (
                &ingestor.output_routes,
                matches!(
                    ingestor
                        .input
                        .entity_kind()
                        .capabilities()
                        .external_partitions,
                    IngestorExternalPartitions::Enumerated(_)
                ),
            ),
            Model::Reingestor(reingestor) => (&reingestor.output_routes, false),
            _ => continue,
        };
        if enumerable {
            continue;
        }
        for route in routes.outputs() {
            let Some(branch) = route
                .branch
                .as_ref()
                .and_then(|branch| branch.constructed_branch())
            else {
                continue;
            };
            if matches!(models.get(&NodeRef::new(ModelKind::Branch, branch)), Some(Model::Branch(declaration))
                if declaration.partitions.is_inherited())
            {
                return Err(Report::new(PartitionTopologyError::Producer {
                    branch: branch.clone(),
                    producer: node.clone(),
                })
                .change_context(RegistryError::PartitionTopology {
                    domain: domain.clone(),
                }));
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct PartitionTopologyDecision {
    pub(crate) topology: PartitionTopology,
    pub(crate) errors: BTreeMap<IngestorName, PartitionTopologyError>,
}

/// Resolves all producers together. Unavailable observations preserve the last committed layout
/// and suspend its producers; they never manufacture a count or discard retained key state.
pub(crate) fn decide(
    models: &impl PartitionModels,
    specs: &BTreeMap<IngestorName, PartitionSourceSpec>,
    existing: &PartitionTopology,
    observations: &BTreeMap<IngestorName, SourcePartitionObservation>,
) -> PartitionTopologyDecision {
    let mut topology = PartitionTopology::default();
    for name in specs.keys() {
        if let Some(observation) = observations.get(name) {
            topology.sources.insert(name.clone(), observation.clone());
        }
    }
    let mut waiting = specs
        .values()
        // Ordinary domain-offset readers retain their native metadata diagnostics while their
        // committed assignment is unknown. Inherited producers must wait for branch coherence.
        .filter(|spec| !spec.branches.is_empty())
        .filter(|spec| {
            topology
                .sources
                .get(&spec.ingestor)
                .is_none_or(|observation| {
                    observation.configuration != spec.fingerprint
                        || observation.partitions.is_none()
                })
        })
        .map(|spec| spec.ingestor.clone())
        .collect::<BTreeSet<_>>();
    let mut errors = BTreeMap::new();
    for model in models.models() {
        let Model::Branch(branch) = model else {
            continue;
        };
        if !branch.partitions.is_inherited() {
            continue;
        }
        let producers = specs
            .values()
            .filter(|spec| spec.branches.contains(&branch.name))
            .collect::<Vec<_>>();
        let counts = producers
            .iter()
            .filter_map(|spec| {
                let observation = topology.sources.get(&spec.ingestor)?;
                if observation.configuration != spec.fingerprint {
                    return None;
                }
                let set = observation.partitions?;
                Some((spec.ingestor.clone(), set.count().get()))
            })
            .collect::<BTreeMap<_, _>>();
        let distinct = counts.values().copied().collect::<BTreeSet<_>>();
        let issue = if distinct.len() > 1 {
            Some(PartitionTopologyError::CountMismatch {
                branch: branch.name.clone(),
                counts: counts.clone(),
            })
        } else if distinct.first().is_some_and(|count| {
            count
                .checked_sub(1)
                .verified("observed source counts are nonzero")
                > i32::MAX.cast_unsigned()
        }) {
            Some(PartitionTopologyError::MetadataBound {
                branch: branch.name.clone(),
                count: *distinct
                    .first()
                    .verified("a bound failure observed a count"),
            })
        } else if branch.partitions == BranchPartitions::Inherited
            && distinct
                .first()
                .is_some_and(|count| *count > u32::from(u16::MAX))
        {
            Some(PartitionTopologyError::CountBound {
                branch: branch.name.clone(),
                count: *distinct
                    .first()
                    .verified("a bound failure observed a count"),
            })
        } else {
            None
        };
        let observed =
            if issue.is_none() && !producers.is_empty() && counts.len() == producers.len() {
                distinct.first().copied()
            } else {
                None
            };
        let source_count = observed.and_then(std::num::NonZeroU32::new);
        let layout = match (branch.partitions, source_count) {
            (BranchPartitions::Inherited, Some(count)) => {
                let count = u16::try_from(count.get()).verified("the bound was checked above");
                Some(InheritedPartitionLayout::Identity(
                    NonZeroU16::new(count).verified("a source count is positive"),
                ))
            }
            (BranchPartitions::InheritedGrouped(groups), Some(source_count)) => {
                Some(InheritedPartitionLayout::Grouped {
                    source_count,
                    groups,
                })
            }
            _ => existing.branches.get(&branch.name).copied(),
        };
        if let Some(layout) = layout {
            topology.branches.insert(branch.name.clone(), layout);
        }
        if observed.is_none() {
            for producer in producers {
                waiting.insert(producer.ingestor.clone());
                if let Some(issue) = &issue {
                    errors.insert(producer.ingestor.clone(), issue.clone());
                }
            }
        }
    }
    topology.suspended = waiting;
    PartitionTopologyDecision { topology, errors }
}

/// A source replacement can move keys without changing the count. Every coherent change to the
/// producer set, locator or external set therefore uses the same state-claim epoch as regrouping.
pub(in crate::registry) fn changed_source_branches(
    domain: &DomainName,
    specs: &BTreeMap<IngestorName, PartitionSourceSpec>,
    topology: &PartitionTopology,
    existing: Option<&DomainSchedule>,
) -> Result<BTreeSet<BranchName>, Report<RegistryError>> {
    let Some(existing) = existing else {
        return Ok(BTreeSet::new());
    };
    let previous = source_specs(domain, existing)?;
    let mut changed = BTreeSet::new();
    for branch in topology.branches.keys() {
        let current = specs.values().filter(|spec| spec.branches.contains(branch));
        let mut basis = BTreeMap::new();
        let mut coherent = true;
        for spec in current {
            if topology.suspended.contains(&spec.ingestor) {
                coherent = false;
                break;
            }
            if let Some(observation) = topology.sources.get(&spec.ingestor) {
                basis.insert(
                    &spec.ingestor,
                    (observation.configuration, observation.partitions),
                );
            }
        }
        if !coherent {
            continue;
        }
        let mut prior = BTreeMap::new();
        for spec in previous
            .values()
            .filter(|spec| spec.branches.contains(branch))
        {
            if let Some(observation) = existing.partition_topology.sources.get(&spec.ingestor) {
                prior.insert(
                    &spec.ingestor,
                    (observation.configuration, observation.partitions),
                );
            }
        }
        if basis != prior {
            changed.insert(branch.clone());
        }
    }
    Ok(changed)
}

/// Domain-offset intake remains observed even when it constructs no inherited branch.
pub(in crate::registry) fn owns_domain_offsets(ingestor: &nervix_models::CreateIngestor) -> bool {
    matches!(ingestor.input.transport_entity(),
        Some(IngestorEntity::Kafka(KafkaIngestor::Topic(topic)))
        if matches!(topic.offset_mode, KafkaOffsetMode::Domain))
}

/// Source epochs fence an exact observation and reader layout. The layout is already present in
/// ScheduledNode, so only the counter is recorded with the source; no assignment table repeats it.
pub(in crate::registry) fn continue_source_epochs(
    desired: &mut nervix_models::DomainSchedule,
    existing: Option<&nervix_models::DomainSchedule>,
) {
    for (name, observation) in &mut desired.partition_topology.sources {
        let Some(previous) =
            existing.and_then(|schedule| schedule.partition_topology.sources.get(name))
        else {
            observation.epoch = 0;
            continue;
        };
        let identity = NodeRef::new(ModelKind::Ingestor, name);
        let before = match existing {
            Some(schedule) => schedule.nodes.get(&identity).map(|node| node.units.len()),
            None => None,
        };
        let after = desired.nodes.get(&identity).map(|node| node.units.len());
        observation.epoch = if observation.configuration == previous.configuration
            && observation.partitions == previous.partitions
            && before == after
        {
            previous.epoch
        } else {
            previous
                .epoch
                .checked_add(1)
                .assured("a source commits fewer than 2^64 topology changes")
        };
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests exercise typed topology and placement decisions"
    )
)]
mod tests {
    use nervix_models::{EmitterInstances, IngestorInput, IngestorInstances, ModelIndex, Unit};
    use nonzero_ext::nonzero;

    use super::*;
    #[cfg(feature = "testing")]
    use crate::registry::SchedulerMode;
    use crate::registry::{domain_state::DomainState, test_fixtures::*};

    fn models(partitions: BranchPartitions, producers: &[&str]) -> ModelIndex {
        let mut branch = branch_for_relay("events", "row");
        let Model::Branch(declaration) = &mut branch else {
            unreachable!("the fixture declares a branch")
        };
        declaration.partitions = partitions;
        let mut values = vec![
            schema("row"),
            wire_schema("event_wire"),
            codec("json", "row"),
            client_model("broker"),
            branch,
            materialized_relay("events", "row"),
        ];
        for name in producers {
            let mut source = ingestor_statement(name, "events", "json", "broker", &["value"]);
            let IngestorInput::Transport(input) = &mut source.input else {
                unreachable!("the fixture reads Kafka")
            };
            let IngestorEntity::Kafka(KafkaIngestor::Topic(topic)) = &mut input.source else {
                unreachable!("the fixture reads Kafka")
            };
            topic.offset_mode = KafkaOffsetMode::Domain;
            topic.instances = IngestorInstances::PerPartition;
            values.push(Model::Ingestor(source));
        }
        let Model::Emitter(mut output) = emitter("output", "events", "json", "broker") else {
            unreachable!("the fixture emits Kafka")
        };
        output.instances = EmitterInstances::PerPartition;
        values.push(Model::Emitter(output));
        values.into_iter().collect()
    }

    fn observations(
        specs: &BTreeMap<IngestorName, PartitionSourceSpec>,
        counts: &[Option<u32>],
    ) -> BTreeMap<IngestorName, SourcePartitionObservation> {
        specs
            .values()
            .zip(counts)
            .map(|(spec, count)| {
                (
                    spec.ingestor.clone(),
                    SourcePartitionObservation {
                        configuration: spec.fingerprint,
                        partitions: count
                            .and_then(std::num::NonZeroU32::new)
                            .map(nervix_models::ExternalPartitionSet::Partitioned),
                        epoch: 0,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn an_unobserved_domain_offset_topic_keeps_its_native_diagnostic_reader() {
        let models = models(BranchPartitions::Direct(nonzero!(4u16)), &["reader"]);
        let specs = source_specs(&named("orders"), &models).assured("the fixture client resolves");
        let decision = decide(
            &models,
            &specs,
            &PartitionTopology::default(),
            &BTreeMap::new(),
        );
        assert!(decision.topology.suspended.is_empty());
        assert!(decision.topology.sources.is_empty());
        assert!(decision.topology.branches.is_empty());
    }

    #[test]
    fn unknown_topology_has_no_fabricated_count_and_incoherent_producers_keep_their_last_layout() {
        let models = models(BranchPartitions::Inherited, &["first", "second"]);
        let specs = source_specs(&named("orders"), &models).assured("the fixture clients resolve");
        let empty = PartitionTopology::default();
        let unknown = decide(&models, &specs, &empty, &BTreeMap::new());
        assert!(unknown.topology.branches.is_empty());
        assert_eq!(unknown.topology.suspended.len(), 2);
        let coherent = decide(
            &models,
            &specs,
            &empty,
            &observations(&specs, &[Some(12), Some(12)]),
        );
        let layout = coherent.topology.branches[&branch_name_for_relay("events")];
        assert_eq!(layout.partitions().count().get(), 12);
        assert!(coherent.topology.suspended.is_empty());
        let mismatch = decide(
            &models,
            &specs,
            &coherent.topology,
            &observations(&specs, &[Some(16), Some(12)]),
        );
        assert_eq!(
            mismatch.topology.branches[&branch_name_for_relay("events")],
            layout
        );
        assert_eq!(mismatch.topology.suspended.len(), 2);
        assert_eq!(mismatch.errors.len(), 2);
        assert!(
            mismatch
                .errors
                .values()
                .all(|error| matches!(error, PartitionTopologyError::CountMismatch { .. }))
        );
    }

    #[test]
    fn grouped_observations_publish_epoch_and_inverse_instance_placement_together() {
        let models = models(
            BranchPartitions::InheritedGrouped(
                std::num::NonZeroU16::new(4).assured("four is positive"),
            ),
            &["input"],
        );
        let domain = named("orders");
        let graph = DomainState::build(&domain, &models)
            .assured("the inherited fixture graph validates")
            .graph;
        let specs = source_specs(&domain, &models).assured("the fixture clients resolve");
        let topology = decide(
            &models,
            &specs,
            &PartitionTopology::default(),
            &observations(&specs, &[Some(12)]),
        )
        .topology;
        let nodes = [named("node-1"), named("node-2"), named("node-3")];
        let capabilities = crate::registry::uniform_capabilities(&nodes);
        let schedule = graph
            .schedule_for_domain_with_topology(
                crate::registry::DomainSchedulingInputs {
                    domain: &domain,
                    cluster_nodes: &nodes,
                    replica_count: 0,
                    existing: None,
                    #[cfg(feature = "testing")]
                    scheduler_mode: SchedulerMode::Sticky,
                    capabilities: &capabilities,
                },
                &topology,
                None,
            )
            .assured("the topology resolves before placement");
        let relay = &schedule.nodes[&NodeRef::new(
            ModelKind::Relay,
            named::<nervix_models::ModelName>("events"),
        )];
        let intake = &schedule.nodes[&NodeRef::new(
            ModelKind::Ingestor,
            named::<nervix_models::ModelName>("input"),
        )];
        let output = &schedule.nodes[&NodeRef::new(
            ModelKind::Emitter,
            named::<nervix_models::ModelName>("output"),
        )];
        assert_eq!(
            (relay.units.len(), intake.units.len(), output.units.len()),
            (4, 4, 4)
        );
        for partition in 0..4 {
            let unit = Unit::partition(partition);
            let reader =
                Unit::try_instance(u64::from(partition)).assured("the fixture instance fits");
            assert_eq!(
                relay.units[&unit].primary_node,
                intake.units[&reader].primary_node
            );
            assert_eq!(
                relay.units[&unit].primary_node,
                output.units[&reader].primary_node
            );
        }
        let grown = decide(
            &models,
            &specs,
            &schedule.partition_topology,
            &observations(&specs, &[Some(16)]),
        )
        .topology;
        let next = graph
            .schedule_for_domain_with_topology(
                crate::registry::DomainSchedulingInputs {
                    domain: &domain,
                    cluster_nodes: &nodes,
                    replica_count: 0,
                    existing: Some(&schedule),
                    #[cfg(feature = "testing")]
                    scheduler_mode: SchedulerMode::Sticky,
                    capabilities: &capabilities,
                },
                &grown,
                None,
            )
            .assured("growth keeps the group count");
        let branch = branch_name_for_relay("events");
        assert_eq!(next.partition_epochs[&branch].number.get(), 2);
        assert_eq!(next.partition_epochs[&branch].partitions.count().get(), 4);
        assert_eq!(
            next.partition_topology.branches[&branch]
                .source_count()
                .get(),
            16
        );
        assert_eq!(
            next.partition_topology.sources[&named::<IngestorName>("input")].epoch,
            1
        );
        assert!(!matches!(
            crate::registry::ScheduleDelta::classify(&schedule, &next),
            crate::registry::ScheduleDelta::Rebuild
        ));
    }

    #[test]
    fn groups_above_the_source_count_are_idle() {
        let models = models(
            BranchPartitions::InheritedGrouped(
                std::num::NonZeroU16::new(16).assured("sixteen is positive"),
            ),
            &["input"],
        );
        let specs = source_specs(&named("orders"), &models).assured("the fixture source resolves");
        let decision = decide(
            &models,
            &specs,
            &PartitionTopology::default(),
            &observations(&specs, &[Some(12)]),
        );
        let layout = decision.topology.branches[&branch_name_for_relay("events")];
        assert_eq!(
            layout.idle_partitions().collect::<Vec<_>>(),
            vec![12, 13, 14, 15]
        );
        for external in 0..12 {
            assert_eq!(
                layout.unit_of(nervix_models::ExternalPartition::new(external)),
                Some(Unit::partition(external))
            );
        }
    }
}
