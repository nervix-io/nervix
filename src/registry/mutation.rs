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
    ModelIndex, ModelKind, NodeRef, QuiesceLevel, StatePurge, Statement,
};
use nervix_recovery::Discarded;
use thiserror::Error;

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

#[derive(Debug, Error)]
#[error("the statement is not a model mutation")]
pub(crate) struct RegistryMutationConversionError;

impl TryFrom<&Statement> for RegistryMutation {
    type Error = RegistryMutationConversionError;

    fn try_from(statement: &Statement) -> Result<Self, Self::Error> {
        let mutation = match statement {
            Statement::Create(create) => Self::Create(create.body.clone()),
            Statement::AlterSchema(alter) => Self::AlterSchema(alter.clone()),
            Statement::AlterWireJsonSchema(alter) => Self::AlterWireJsonSchema(alter.clone()),
            Statement::AlterWireCborSchema(alter) => Self::AlterWireCborSchema(alter.clone()),
            Statement::AlterWireAvroSchema(alter) => Self::AlterWireAvroSchema(alter.clone()),
            Statement::AlterRelay(alter) => Self::AlterRelay(alter.clone()),
            Statement::AlterJunction(alter) => Self::AlterJunction(alter.clone()),
            Statement::AlterDeduplicator(alter) => Self::AlterDeduplicator(alter.clone()),
            Statement::AlterReorderer(alter) => Self::AlterReorderer(alter.clone()),
            Statement::AlterEmitter(alter) => Self::AlterEmitter(alter.clone()),
            Statement::AlterIngestor(alter) => Self::AlterIngestor(alter.clone()),
            Statement::AlterReingestor(alter) => Self::AlterReingestor(alter.clone()),
            Statement::AlterGenerator(alter) => Self::AlterGenerator(alter.clone()),
            Statement::AlterPlacement(alter) => Self::AlterPlacement(alter.clone()),
            Statement::Drop(drop) => Self::Drop(drop.clone()),
            _ => return Err(RegistryMutationConversionError),
        };
        Ok(mutation)
    }
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
    pub(in crate::registry) base_graph: ActiveGraph,
    pub(in crate::registry) domain_state: DomainState,
    pub(in crate::registry) models_to_persist: HashMap<NodeRef, RegistryPersistMutation>,
    pub(in crate::registry) drops_in_batch: HashSet<NodeRef>,
    pub(in crate::registry) runtime_changes: RuntimeChanges,
    pub(in crate::registry) quiesce: QuiescePlan,
}

#[derive(Debug, Clone)]
pub(crate) struct TransactionMutationPreflight {
    pub(in crate::registry) planned: Option<PlannedMutations>,
    pub(in crate::registry) candidate_models: ModelIndex,
    pub(in crate::registry) incomplete_reason: Option<String>,
}

impl TransactionMutationPreflight {
    pub(crate) fn candidate_models(&self) -> &ModelIndex {
        &self.candidate_models
    }

    pub(crate) fn incomplete_reason(&self) -> Option<&str> {
        self.incomplete_reason.as_deref()
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

    pub(crate) fn base_graph(&self) -> &ActiveGraph {
        &self.base_graph
    }

    pub(crate) fn resulting_graph(&self) -> &ActiveGraph {
        &self.domain_state.graph
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
pub(crate) struct ChangedModelImpact {
    pub(crate) node: NodeRef,
    pub(crate) level: QuiesceLevel,
    pub(crate) state_resets: Vec<StatePurge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuiescePlan {
    level: QuiesceLevel,
    affected_entities: Vec<NodeRef>,
    changed: Vec<ChangedModelImpact>,
}

impl QuiescePlan {
    pub(crate) fn level(&self) -> QuiesceLevel {
        self.level
    }

    pub(crate) fn affected_entities(&self) -> &[NodeRef] {
        &self.affected_entities
    }

    pub(crate) fn changed(&self) -> &[ChangedModelImpact] {
        &self.changed
    }
}

pub(in crate::registry) fn classify_quiesce(
    base: &ModelIndex,
    candidate: &ModelIndex,
    base_graph: &ActiveGraph,
    candidate_graph: &ActiveGraph,
) -> QuiescePlan {
    let mut level = QuiesceLevel::Dynamic;
    let mut affected_entities = Vec::new();
    let mut changed = Vec::new();

    for (key, base_model) in base {
        let (change_level, state_resets) = match candidate.get(key) {
            Some(candidate_model) => {
                let aspects = base_model.change_aspects_against(candidate_model);
                if aspects.is_empty() {
                    continue;
                }
                (aspects.quiesce_level(), aspects.state_purges())
            }
            None => (ModelChangeAspect::EntityDropped.quiesce_level(), Vec::new()),
        };
        level = level.max(change_level);
        changed.push(ChangedModelImpact {
            node: key.clone(),
            level: change_level,
            state_resets,
        });
    }

    for key in candidate.nodes().filter(|key| !base.contains(key)) {
        let change_level = ModelChangeAspect::EntityCreated.quiesce_level();
        level = level.max(change_level);
        changed.push(ChangedModelImpact {
            node: key.clone(),
            level: change_level,
            state_resets: Vec::new(),
        });
    }

    if level.requires_domain_pause() {
        for graph in [base_graph, candidate_graph] {
            affected_entities.extend(
                graph
                    .whole_impact()
                    .nodes
                    .into_iter()
                    .filter(|coverage| coverage.branches.is_some())
                    .map(|coverage| coverage.node),
            );
        }
    } else {
        for change in &changed {
            if !change.level.requires_entity_pause() {
                continue;
            }
            for graph in [base_graph, candidate_graph] {
                affected_entities.extend(
                    graph
                        .downstream_impact(&change.node)
                        .nodes
                        .into_iter()
                        .filter(|coverage| coverage.branches.is_some())
                        .map(|coverage| coverage.node),
                );
            }
        }
    }

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
        changed,
    }
}
