//! One change to a domain's Models, and what applying it costs.
//!
//! Layer: decisions.
//!
//! - **Owns.** The create, alter and drop a statement becomes, the plan a batch of them produces,
//!   and the quiesce level each plan demands before it can be applied.
//! - **Depends on.** The Models a mutation carries and the graph it is planned against.
//! - **Must not know.** How the store persists a plan or how the runtime applies it.

use ahash::{HashMap, HashSet};
use nervix_models::{
    AlterDeduplicator, AlterEmitter, AlterGenerator, AlterIngestor, AlterJunction, AlterPlacement,
    AlterPlacementOperation, AlterReingestor, AlterRelay, AlterReorderer, AlterSchema,
    AlterWireSchema, AvroType, CborType, DomainName, DropModel, JsonType, Model, ModelChangeAspect,
    ModelIndex, ModelKind, NodeRef, QuiesceLevel,
};
use nervix_recovery::Discarded;

use crate::registry::{
    domain_state::DomainState,
    graph::ActiveGraph,
    storage::{ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS, RuntimeChanges},
};
#[derive(Debug, Clone)]
pub(crate) enum RegistryMutation {
    Create(Box<Model>),
    AlterSchema(AlterSchema),
    AlterWireJsonSchema(AlterWireSchema<JsonType>),
    AlterWireCborSchema(AlterWireSchema<CborType>),
    AlterWireAvroSchema(AlterWireSchema<AvroType>),
    AlterRelay(AlterRelay),
    AlterJunction(AlterJunction),
    AlterDeduplicator(AlterDeduplicator),
    AlterReorderer(AlterReorderer),
    AlterEmitter(AlterEmitter),
    AlterIngestor(AlterIngestor),
    AlterReingestor(AlterReingestor),
    AlterGenerator(AlterGenerator),
    AlterPlacement(AlterPlacement),
    Drop(DropModel),
}

impl RegistryMutation {
    pub(in crate::registry) fn target_key(&self) -> NodeRef {
        match self {
            Self::Create(model) => model.node_ref(),
            Self::AlterSchema(alter) => NodeRef::new(ModelKind::Schema, alter.schema.clone()),
            Self::AlterWireJsonSchema(alter) => {
                NodeRef::new(ModelKind::WireJsonSchema, alter.schema.clone())
            }
            Self::AlterWireCborSchema(alter) => {
                NodeRef::new(ModelKind::WireCborSchema, alter.schema.clone())
            }
            Self::AlterWireAvroSchema(alter) => {
                NodeRef::new(ModelKind::WireAvroSchema, alter.schema.clone())
            }
            Self::AlterRelay(alter) => NodeRef::new(ModelKind::Relay, alter.relay.clone()),
            Self::AlterJunction(alter) => NodeRef::new(ModelKind::Junction, alter.junction.clone()),
            Self::AlterDeduplicator(alter) => {
                NodeRef::new(ModelKind::Deduplicator, alter.deduplicator.clone())
            }
            Self::AlterReorderer(alter) => {
                NodeRef::new(ModelKind::Reorderer, alter.reorderer.clone())
            }
            Self::AlterEmitter(alter) => NodeRef::new(ModelKind::Emitter, alter.emitter.clone()),
            Self::AlterIngestor(alter) => NodeRef::new(ModelKind::Ingestor, alter.ingestor.clone()),
            Self::AlterReingestor(alter) => {
                NodeRef::new(ModelKind::Reingestor, alter.reingestor.clone())
            }
            Self::AlterGenerator(alter) => {
                NodeRef::new(ModelKind::Generator, alter.generator.clone())
            }
            Self::AlterPlacement(alter) => {
                NodeRef::new(ModelKind::Placement, alter.placement.clone())
            }
            Self::Drop(drop) => NodeRef::new(drop.kind, drop.name.clone()),
        }
    }

    pub(in crate::registry) fn resulting_key(&self) -> Option<NodeRef> {
        match self {
            Self::Drop(_) => None,
            Self::AlterPlacement(alter) => {
                let identifier = alter
                    .operations
                    .iter()
                    .filter_map(|operation| match operation {
                        AlterPlacementOperation::RenameTo { name } => Some(name),
                        AlterPlacementOperation::SetPolicy { .. }
                        | AlterPlacementOperation::SetRank { .. }
                        | AlterPlacementOperation::DropRank
                        | AlterPlacementOperation::SetMembers { .. } => None,
                    })
                    .next_back()
                    .unwrap_or(&alter.placement);
                Some(NodeRef::new(ModelKind::Placement, identifier.clone()))
            }
            _ => Some(self.target_key()),
        }
    }

    /// Fold this mutation into `models` without validating the outcome. An alteration that no
    /// longer applies leaves the stored model as it was, so a description of queued configuration
    /// never fails on an intermediate state its later statements repair.
    pub(in crate::registry) fn fold_into_models(&self, models: &mut ModelIndex) {
        match self {
            Self::Create(model) => {
                models.insert(model.as_ref().clone());
            }
            Self::Drop(_) => {
                models.remove(&self.target_key());
            }
            _ => {
                let Some(mut model) = models.remove(&self.target_key()) else {
                    return;
                };
                self.apply_alteration(&mut model);
                models.insert(model);
            }
        }
    }

    /// Fold one alteration into the model it targets, keeping the model as it was when the
    /// alteration no longer applies.
    ///
    /// Every arm below discards its outcome for the reason [`Self::fold_into_models`] gives: this
    /// walk describes queued configuration rather than committing it, and a statement that reads
    /// as invalid against an intermediate state is routinely repaired by a later statement in the
    /// same batch. Validation happens once, on the batch's final models, where a rejection can
    /// name the statement that caused it.
    fn apply_alteration(&self, model: &mut Model) {
        match (self, model) {
            (Self::AlterSchema(alter), Model::Schema(schema)) => {
                schema
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterWireJsonSchema(alter), Model::WireJsonSchema(schema)) => {
                schema
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterWireCborSchema(alter), Model::WireCborSchema(schema)) => {
                schema
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterWireAvroSchema(alter), Model::WireAvroSchema(schema)) => {
                schema
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterRelay(alter), Model::Relay(relay)) => {
                relay
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterJunction(alter), Model::Junction(junction)) => {
                junction
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterDeduplicator(alter), Model::Deduplicator(deduplicator)) => {
                deduplicator
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterReorderer(alter), Model::Reorderer(reorderer)) => {
                reorderer
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterEmitter(alter), Model::Emitter(emitter)) => {
                emitter
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterIngestor(alter), Model::Ingestor(ingestor)) => {
                ingestor
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterReingestor(alter), Model::Reingestor(reingestor)) => {
                reingestor
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterGenerator(alter), Model::Generator(generator)) => {
                generator
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            (Self::AlterPlacement(alter), Model::Placement(placement)) => {
                placement
                    .apply_alter(alter)
                    .discarded(ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS);
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::registry) enum RegistryPersistMutation {
    Create(Model),
    Replace(Model),
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedMutations {
    pub(in crate::registry) domain: DomainName,
    pub(in crate::registry) batch_size: usize,
    pub(in crate::registry) operation_name: String,
    pub(in crate::registry) base_models: ModelIndex,
    pub(in crate::registry) domain_state: DomainState,
    pub(in crate::registry) models_to_persist: HashMap<NodeRef, RegistryPersistMutation>,
    pub(in crate::registry) drops_in_batch: HashSet<NodeRef>,
    pub(in crate::registry) runtime_changes: RuntimeChanges,
    pub(in crate::registry) quiesce: QuiescePlan,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionMutationPreflight {
    pub(in crate::registry) planned: Option<PlannedMutations>,
    pub(in crate::registry) mutation_quiesce_levels: Vec<QuiesceLevel>,
}

impl TransactionMutationPreflight {
    pub(crate) fn planned(&self) -> Option<&PlannedMutations> {
        self.planned.as_ref()
    }

    pub(crate) fn mutation_quiesce_levels(&self) -> &[QuiesceLevel] {
        &self.mutation_quiesce_levels
    }
}

impl PlannedMutations {
    pub(crate) fn quiesce(&self) -> &QuiescePlan {
        &self.quiesce
    }

    pub(crate) fn is_noop(&self) -> bool {
        self.models_to_persist.is_empty() && self.drops_in_batch.is_empty()
    }

    pub(crate) fn candidate_graph(&self) -> Option<ActiveGraph> {
        self.runtime_changes.graph.clone()
    }

    /// Every model the batch would create or replace, ordered by kind and identifier so boundary
    /// validation reports the same model first for the same batch.
    pub(crate) fn changed_models(&self) -> Vec<&Model> {
        let mut changed = self.models_to_persist.iter().collect::<Vec<_>>();
        changed.sort_by(|(left, _), (right, _)| {
            left.kind
                .as_str()
                .cmp(right.kind.as_str())
                .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
        });
        changed
            .into_iter()
            .map(|(_, mutation)| match mutation {
                RegistryPersistMutation::Create(model)
                | RegistryPersistMutation::Replace(model) => model,
            })
            .collect()
    }

    /// Every model of one kind the batch would leave active, including the models it does not
    /// change. Callers that must compile or bind a whole family at once need the candidate set,
    /// not just the mutated members.
    pub(crate) fn candidate_models_of_kind(&self, kind: ModelKind) -> Vec<&Model> {
        let mut candidates = self
            .domain_state
            .models
            .iter()
            .filter(|(key, _)| key.kind == kind)
            .collect::<Vec<_>>();
        candidates.sort_by(|(left, _), (right, _)| {
            left.identifier.as_str().cmp(right.identifier.as_str())
        });
        candidates.into_iter().map(|(_, model)| model).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuiescePlan {
    level: QuiesceLevel,
    affected_entities: Vec<NodeRef>,
}

impl QuiescePlan {
    pub(crate) fn level(&self) -> QuiesceLevel {
        self.level
    }

    pub(crate) fn affected_entities(&self) -> &[NodeRef] {
        &self.affected_entities
    }
}

pub(in crate::registry) fn classify_quiesce(
    base: &ModelIndex,
    candidate: &ModelIndex,
    candidate_graph: &ActiveGraph,
) -> QuiescePlan {
    let mut level = QuiesceLevel::Dynamic;
    let mut affected_entities = Vec::new();
    let mut gated_seeds = HashSet::<NodeRef>::default();

    for (key, base_model) in base {
        let change_level = match candidate.get(key) {
            Some(candidate_model) => {
                let aspects = base_model.change_aspects_against(candidate_model);
                if aspects.is_empty() {
                    continue;
                }
                aspects.quiesce_level()
            }
            None => ModelChangeAspect::EntityDropped.quiesce_level(),
        };
        level = level.max(change_level);
        if change_level.requires_entity_pause() {
            gated_seeds.insert(key.clone());
        }
        affected_entities.push(NodeRef {
            kind: key.kind,
            identifier: key.identifier.clone(),
        });
    }

    for key in candidate.nodes().filter(|key| !base.contains(key)) {
        level = level.max(ModelChangeAspect::EntityCreated.quiesce_level());
        affected_entities.push(NodeRef {
            kind: key.kind,
            identifier: key.identifier.clone(),
        });
    }

    // An entity-paused change also disturbs everything downstream of it, so the gate has to cover
    // the dependent dataflow nodes and not just the models the batch names.
    affected_entities.extend(candidate_graph.dependent_dataflow_entities(&gated_seeds));

    affected_entities.sort_by(|left, right| {
        left.kind
            .as_str()
            .cmp(right.kind.as_str())
            .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
    });
    affected_entities.dedup();
    QuiescePlan {
        level,
        affected_entities,
    }
}

pub(in crate::registry) fn classify_quiesce_level(
    base: Option<&Model>,
    candidate: Option<&Model>,
) -> QuiesceLevel {
    match (base, candidate) {
        (Some(base), Some(candidate)) => base.change_aspects_against(candidate).quiesce_level(),
        (None, Some(_)) => ModelChangeAspect::EntityCreated.quiesce_level(),
        (Some(_), None) => ModelChangeAspect::EntityDropped.quiesce_level(),
        (None, None) => QuiesceLevel::Dynamic,
    }
}
