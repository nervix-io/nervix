//! The durable model store and the mutations that change it.
//!
//! Layer: decisions.
//!
//! - **Owns.** Reading and writing Models in the keyspace, planning a mutation batch against the
//!   candidate graph, committing or rolling it back, and the runtime changes a commit implies.
//! - **Depends on.** `fjall` for storage and the domain state for validation.
//! - **Must not know.** How a runtime change is applied.

use std::{collections::BTreeSet, path::Path, str::FromStr};

use ahash::{HashMap, HashSet};
use error_stack::{Report, ResultExt};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use meticulous::OptionExt;
#[cfg(test)]
use nervix_models::{AlterRelay, DropModel};
use nervix_models::{
    ClusterSchedule, CreateAvroWireSchema, CreateDeduplicator, CreateEmitter, CreateGenerator,
    CreateIngestor, CreateJunction, CreatePlacement, CreateReingestor, CreateRelay,
    CreateReorderer, CreateSchema, DomainName, IngestSource, IngestorName, Model, ModelIndex,
    ModelKind, ModelName, NodeRef, PlacementPolicy, UniquelyKindedModel,
};
use nervix_recovery::Discarded;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
use tracing::{info, warn};
use triomphe::Arc;

use crate::registry::{
    domain_state::{DomainState, RegistryState},
    error::RegistryError,
    graph::{ActiveGraph, ensure_drop_targets_are_not_in_use},
    mutation::{
        PlannedMutations, RegistryMutation, RegistryPersistMutation, TransactionMutationPreflight,
        classify_quiesce, classify_quiesce_level,
    },
    placement::{PlacementPlan, ensure_placement_member_shape_change_allowed},
};
/// Why applying one alteration of a batch discards its outcome.
/// See [`RegistryMutation::apply_alteration`].
pub(in crate::registry) const ALTERATIONS_ARE_VALIDATED_ON_THE_FINAL_MODELS: &str =
    "an alteration the batch later repairs is applied against the final models, where a rejection \
     can name the statement that caused it";

#[derive(Debug, Clone, PartialEq, Eq)]
/// One model as storage holds it, with the domain that owns it.
///
/// The node the model configures is read from the model rather than carried beside it, because the
/// key it is stored under is decoded from the model's own kind and name when the record is read.
pub(in crate::registry) struct StoredModelRecord {
    pub(in crate::registry) domain: DomainName,
    pub(in crate::registry) model: Model,
}

impl StoredModelRecord {
    fn node(&self) -> NodeRef {
        self.model.node_ref()
    }
}

pub(crate) struct Registry {
    storage: ModelStorage,
    state: RwLock<Arc<RegistryState>>,
    commit_lock: Mutex<()>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeChanges {
    pub(crate) domain: DomainName,
    pub(crate) graph: Option<ActiveGraph>,
    pub(crate) changes: Vec<RuntimeChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeChange {
    StartIngestor {
        source_model: Box<Model>,
        ingestor: Box<CreateIngestor>,
    },
    StopIngestor {
        ingestor: IngestorName,
    },
}

impl Registry {
    #[cfg(test)]
    pub(in crate::registry) fn open(path: impl AsRef<Path>) -> Result<Self, Report<RegistryError>> {
        let path = path.as_ref();
        let db = Database::builder(path)
            .open()
            .change_context(RegistryError::OpenDatabase)?;
        Self::from_database(db, Some(path))
    }

    pub(crate) fn from_database(
        db: Database,
        path: Option<&Path>,
    ) -> Result<Self, Report<RegistryError>> {
        let storage = ModelStorage::from_database(db).change_context(RegistryError::OpenStorage)?;
        let stored = storage
            .list_all_models()
            .change_context(RegistryError::LoadStoredModels)?;

        if let Some(path) = path {
            info!(
                path = %path.display(),
                model_count = stored.len(),
                "loaded persisted models from storage"
            );
        } else {
            info!(
                model_count = stored.len(),
                "loaded persisted models from storage"
            );
        }

        for record in &stored {
            let node = record.node();
            info!(
                domain = record.domain.as_str(),
                model = node.identifier.as_str(),
                kind = node.kind.as_str(),
                "loaded persisted model"
            );
        }

        let state = match RegistryState::from_records(stored) {
            Ok(state) => state,
            Err(err) => {
                if let Some(path) = path {
                    warn!(
                        path = %path.display(),
                        result = "err",
                        error = %err,
                        "persistent state load failed"
                    );
                } else {
                    warn!(result = "err", error = %err, "persistent state load failed");
                }
                return Err(err);
            }
        };

        if let Some(path) = path {
            info!(
                path = %path.display(),
                result = "ok",
                domain_count = state.domains.len(),
                "registry opened"
            );
        } else {
            info!(
                result = "ok",
                domain_count = state.domains.len(),
                "registry opened"
            );
        }
        log_registry_state("persistent state load result", &state);

        Ok(Self {
            storage,
            state: RwLock::new(Arc::new(state)),
            commit_lock: Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(in crate::registry) fn apply_batch(
        &self,
        domain: &DomainName,
        models: Vec<Model>,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutation_batch(
            domain,
            models
                .into_iter()
                .map(|model| RegistryMutation::Create(Box::new(model)))
                .collect(),
        )
    }

    #[cfg(test)]
    pub(in crate::registry) fn drop_batch(
        &self,
        domain: &DomainName,
        drops: Vec<DropModel>,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(
            domain,
            drops.into_iter().map(RegistryMutation::Drop).collect(),
            "drop batch",
        )
    }

    #[cfg(test)]
    pub(in crate::registry) fn alter_relay(
        &self,
        domain: &DomainName,
        alter: AlterRelay,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(
            domain,
            vec![RegistryMutation::AlterRelay(alter)],
            "relay alter",
        )
    }

    #[cfg(test)]
    pub(in crate::registry) fn apply_mutation_batch(
        &self,
        domain: &DomainName,
        mutations: Vec<RegistryMutation>,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(domain, mutations, "mixed mutation batch")
    }

    pub(crate) fn startup_runtime_changes(
        &self,
    ) -> Result<Vec<RuntimeChanges>, Report<RegistryError>> {
        let state = self.state.read();
        let domains = SortedSet::from_unsorted(state.domains.keys().cloned().collect()).into_vec();
        let mut startup_changes = Vec::new();
        for domain in domains {
            let domain_state = &state.domains[&domain];
            let changes = runtime_changes_for_domain(
                &domain,
                Some(domain_state.graph.clone()),
                &ModelIndex::new(),
                &domain_state.models,
            );
            if changes.graph.is_some() || !changes.changes.is_empty() {
                startup_changes.push(changes);
            }
        }
        Ok(startup_changes)
    }

    pub(crate) fn synchronize_cluster_schedule(
        &self,
        schedule: &ClusterSchedule,
    ) -> Result<(), Report<RegistryError>> {
        let desired_domains = schedule.domains.keys().cloned().collect::<HashSet<_>>();
        for domain_schedule in schedule.domains.values() {
            let models = domain_schedule
                .nodes
                .values()
                .map(|node| node.config.as_ref().clone())
                .collect::<ModelIndex>();
            self.synchronize_domain_models(&domain_schedule.domain, models)?;
        }
        let stale_domains = self
            .state
            .read()
            .domains
            .keys()
            .filter(|domain| !desired_domains.contains(*domain))
            .cloned()
            .collect::<Vec<_>>();
        for domain in stale_domains {
            self.synchronize_domain_models(&domain, ModelIndex::new())?;
        }
        Ok(())
    }

    fn synchronize_domain_models(
        &self,
        domain: &DomainName,
        models: ModelIndex,
    ) -> Result<(), Report<RegistryError>> {
        let _commit_guard = self.commit_lock.lock();
        let current_models = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| record.model)
            .collect::<ModelIndex>();
        if current_models == models {
            return Ok(());
        }

        let domain_state = self.build_domain_state(domain, &models)?;
        let models_to_persist = models
            .iter()
            .filter_map(|(key, model)| match current_models.get(key) {
                None => Some((key.clone(), RegistryPersistMutation::Create(model.clone()))),
                Some(current) if current != model => {
                    Some((key.clone(), RegistryPersistMutation::Replace(model.clone())))
                }
                Some(_) => None,
            })
            .collect::<HashMap<_, _>>();
        let drops = current_models
            .nodes()
            .filter(|key| !models.contains(key))
            .cloned()
            .collect::<HashSet<_>>();
        self.storage
            .commit_batch(domain, &models_to_persist, &drops)
            .change_context(RegistryError::PersistBatch)?;

        let current = self.state.read();
        let mut domains = current.domains.clone();
        if domain_state.graph.node_count() == 0 {
            domains.remove(domain);
        } else {
            domains.insert(domain.clone(), domain_state);
        }
        drop(current);
        *self.state.write() = Arc::new(RegistryState { domains });

        info!(
            domain = domain.as_str(),
            model_count = models.len(),
            "synchronized registry models from consensus schedule"
        );
        Ok(())
    }

    #[cfg(test)]
    fn apply_mutations(
        &self,
        domain: &DomainName,
        mutations: Vec<RegistryMutation>,
        operation_name: &str,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        let planned = self.plan_mutations_named(domain, &mutations, operation_name)?;
        self.commit_planned(planned)
    }

    pub(crate) fn plan_mutations(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
    ) -> Result<PlannedMutations, Report<RegistryError>> {
        self.plan_mutations_named(domain, mutations, "mixed mutation batch")
    }

    /// Applies every mutation's statement-local checks against the accumulated candidate. If the
    /// candidate already forms a complete domain graph, it returns the normal plan so callers can
    /// run boundary validation too. Missing or incompatible cross-model relationships are treated
    /// as provisionally incomplete because a later statement in the same atomic transaction run
    /// may repair them.
    pub(crate) fn preflight_transaction_mutations(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
    ) -> Result<TransactionMutationPreflight, Report<RegistryError>> {
        self.plan_mutations_named_with_incomplete_candidate(
            domain,
            mutations,
            "transaction queue preflight",
            true,
        )
    }

    fn plan_mutations_named(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
        operation_name: &str,
    ) -> Result<PlannedMutations, Report<RegistryError>> {
        Ok(self
            .plan_mutations_named_with_incomplete_candidate(
                domain,
                mutations,
                operation_name,
                false,
            )?
            .planned
            .verified(
                "this call disallows an incomplete candidate, and only that path returns no plan",
            ))
    }

    fn plan_mutations_named_with_incomplete_candidate(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
        operation_name: &str,
        allow_incomplete_candidate: bool,
    ) -> Result<TransactionMutationPreflight, Report<RegistryError>> {
        let batch_size = mutations.len();
        info!(
            domain = domain.as_str(),
            batch_size,
            operation = operation_name,
            "planning mutation batch"
        );

        let existing = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?;

        let current_models = existing
            .iter()
            .map(|record| record.model.clone())
            .collect::<ModelIndex>();
        let current_state = self.build_domain_state(domain, &current_models)?;
        let mut candidate = current_models.clone();
        let mut mutation_quiesce_levels = Vec::with_capacity(mutations.len());

        for (mutation_index, mutation) in mutations.iter().enumerate() {
            let mutation_base = candidate.get(&mutation.target_key()).cloned();
            match mutation {
                RegistryMutation::Create(model) => {
                    let identifier = model.name();
                    let key = model.node_ref();

                    info!(
                        domain = domain.as_str(),
                        model = identifier.as_str(),
                        kind = model.kind().as_str(),
                        "staging model create from batch"
                    );

                    if candidate.contains(&key) {
                        warn!(
                            domain = domain.as_str(),
                            model = identifier.as_str(),
                            kind = model.kind().as_str(),
                            "rejecting batch because model already exists"
                        );
                        return Err(Report::new(RegistryError::AlreadyExists {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                        }));
                    }

                    candidate.insert(model.as_ref().clone());
                }
                RegistryMutation::AlterSchema(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.schema.as_str(),
                        kind = ModelKind::Schema.as_str(),
                        "staging schema alter from batch"
                    );

                    let Some(schema) =
                        candidate.configured_mut::<CreateSchema>(alter.schema.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                        }));
                    };
                    schema.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterWireJsonSchema(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.schema.as_str(),
                        kind = ModelKind::WireJsonSchema.as_str(),
                        "staging JSON wire schema alter from batch"
                    );

                    let Some(schema) = candidate.narrowed_mut(
                        ModelKind::WireJsonSchema,
                        alter.schema.clone(),
                        |model| {
                            if let Model::WireJsonSchema(schema) = model {
                                Some(schema)
                            } else {
                                None
                            }
                        },
                    ) else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                        }));
                    };
                    schema.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterWireCborSchema(alter) => {
                    let Some(schema) = candidate.narrowed_mut(
                        ModelKind::WireCborSchema,
                        alter.schema.clone(),
                        |model| {
                            if let Model::WireCborSchema(schema) = model {
                                Some(schema)
                            } else {
                                None
                            }
                        },
                    ) else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                        }));
                    };
                    schema.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterWireAvroSchema(alter) => {
                    let Some(schema) =
                        candidate.configured_mut::<CreateAvroWireSchema>(alter.schema.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                        }));
                    };
                    schema.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.schema.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterRelay(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.relay.as_str(),
                        kind = ModelKind::Relay.as_str(),
                        "staging relay alter from batch"
                    );

                    let Some(relay) = candidate.configured_mut::<CreateRelay>(alter.relay.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.relay.as_str().to_string(),
                        }));
                    };
                    let before = Model::Relay(relay.clone());
                    relay.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.relay.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                    let after = Model::Relay(relay.clone());
                    ensure_placement_member_shape_change_allowed(
                        domain, &before, &after, &candidate,
                    )?;
                }
                RegistryMutation::AlterJunction(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.junction.as_str(),
                        kind = ModelKind::Junction.as_str(),
                        "staging junction alter from batch"
                    );

                    let Some(junction) =
                        candidate.configured_mut::<CreateJunction>(alter.junction.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.junction.as_str().to_string(),
                        }));
                    };
                    junction.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.junction.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterDeduplicator(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.deduplicator.as_str(),
                        kind = ModelKind::Deduplicator.as_str(),
                        "staging deduplicator alter from batch"
                    );

                    let Some(deduplicator) =
                        candidate.configured_mut::<CreateDeduplicator>(alter.deduplicator.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.deduplicator.as_str().to_string(),
                        }));
                    };
                    deduplicator.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.deduplicator.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterReorderer(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.reorderer.as_str(),
                        kind = ModelKind::Reorderer.as_str(),
                        "staging reorderer alter from batch"
                    );

                    let Some(reorderer) =
                        candidate.configured_mut::<CreateReorderer>(alter.reorderer.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.reorderer.as_str().to_string(),
                        }));
                    };
                    reorderer.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.reorderer.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterEmitter(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.emitter.as_str(),
                        kind = ModelKind::Emitter.as_str(),
                        "staging emitter alter from batch"
                    );

                    let Some(emitter) =
                        candidate.configured_mut::<CreateEmitter>(alter.emitter.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.emitter.as_str().to_string(),
                        }));
                    };
                    emitter.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.emitter.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterIngestor(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.ingestor.as_str(),
                        kind = ModelKind::Ingestor.as_str(),
                        "staging ingestor alter from batch"
                    );

                    let Some(ingestor) =
                        candidate.configured_mut::<CreateIngestor>(alter.ingestor.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.ingestor.as_str().to_string(),
                        }));
                    };
                    let before = Model::Ingestor(ingestor.clone());
                    ingestor.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.ingestor.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                    let after = Model::Ingestor(ingestor.clone());
                    ensure_placement_member_shape_change_allowed(
                        domain, &before, &after, &candidate,
                    )?;
                }
                RegistryMutation::AlterReingestor(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.reingestor.as_str(),
                        kind = ModelKind::Reingestor.as_str(),
                        "staging reingestor alter from batch"
                    );

                    let Some(reingestor) =
                        candidate.configured_mut::<CreateReingestor>(alter.reingestor.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.reingestor.as_str().to_string(),
                        }));
                    };
                    reingestor.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.reingestor.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterGenerator(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.generator.as_str(),
                        kind = ModelKind::Generator.as_str(),
                        "staging generator alter from batch"
                    );

                    let Some(generator) =
                        candidate.configured_mut::<CreateGenerator>(alter.generator.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.generator.as_str().to_string(),
                        }));
                    };
                    generator.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.generator.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                }
                RegistryMutation::AlterPlacement(alter) => {
                    let key = NodeRef::new(ModelKind::Placement, alter.placement.clone());
                    info!(
                        domain = domain.as_str(),
                        model = alter.placement.as_str(),
                        kind = ModelKind::Placement.as_str(),
                        "staging placement alter from batch"
                    );
                    // A placement alteration may rename it, and the index keys a model by its own
                    // name, so the entry leaves the index and re-enters under the resulting name.
                    let Some(placement) =
                        candidate.configured_mut::<CreatePlacement>(key.identifier.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.placement.as_str().to_string(),
                        }));
                    };
                    let mut altered = placement.clone();
                    altered.apply_alter(alter).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: alter.placement.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                    let model = Model::Placement(altered);
                    let next_key = model.node_ref();
                    if next_key != key && candidate.contains(&next_key) {
                        return Err(Report::new(RegistryError::AlreadyExists {
                            domain: domain.as_str().to_string(),
                            identifier: model.name().as_str().to_string(),
                        }));
                    }
                    candidate.remove(&key).verified(
                        "the placement being altered was resolved from this same index by this \
                         key above",
                    );
                    candidate.insert(model);
                }
                RegistryMutation::Drop(drop) => {
                    let key = NodeRef::new(drop.kind, drop.name.clone());
                    info!(
                        domain = domain.as_str(),
                        model = drop.name.as_str(),
                        kind = drop.kind.as_str(),
                        "staging model drop from batch"
                    );

                    if !candidate.contains(&key) {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: drop.name.as_str().to_string(),
                        }));
                    }
                    let recreated_later = mutations[mutation_index + 1..].iter().any(|mutation| {
                        let RegistryMutation::Create(model) = mutation else {
                            return false;
                        };
                        model.node_ref() == key
                    });
                    if !allow_incomplete_candidate && !recreated_later {
                        let candidate_state = self.build_domain_state(domain, &candidate)?;
                        ensure_drop_targets_are_not_in_use(
                            domain,
                            &candidate_state.graph,
                            &HashSet::from_iter([key.clone()]),
                        )?;
                    }
                    candidate.remove(&key).discarded(
                        "a candidate that never held the dropped entity is already in the state \
                         this removal wants",
                    );
                }
            }
            let mutation_candidate = mutation
                .resulting_key()
                .as_ref()
                .and_then(|key| candidate.get(key));
            mutation_quiesce_levels.push(classify_quiesce_level(
                mutation_base.as_ref(),
                mutation_candidate,
            ));
        }

        let drops_in_batch = current_models
            .nodes()
            .filter(|key| !candidate.contains(key))
            .cloned()
            .collect::<HashSet<_>>();

        let domain_state = match self.build_domain_state(domain, &candidate) {
            Ok(state) => state,
            // A caller that allows an incomplete candidate is queuing a step of a transaction that
            // later steps complete, so a candidate that does not yet build has no plan to offer
            // and no failure to report. The batch is validated once, on its final models, where a
            // rejection can name the statement that caused it.
            Err(_) if allow_incomplete_candidate => {
                return Ok(TransactionMutationPreflight {
                    planned: None,
                    mutation_quiesce_levels,
                });
            }
            Err(err) => {
                let active_graph = self.active_graph_snapshot(domain);
                warn!(
                    domain = domain.as_str(),
                    batch_size,
                    operation = operation_name,
                    result = "err",
                    error = %err,
                    "failed to apply mutation batch\n{}",
                    active_graph
                );
                return Err(err);
            }
        };
        let models_to_persist = candidate
            .iter()
            .filter_map(|(key, model)| match current_models.get(key) {
                None => Some((key.clone(), RegistryPersistMutation::Create(model.clone()))),
                Some(current) if current != model => {
                    Some((key.clone(), RegistryPersistMutation::Replace(model.clone())))
                }
                Some(_) => None,
            })
            .collect::<HashMap<_, _>>();
        let quiesce = classify_quiesce(&current_models, &candidate, &domain_state.graph);
        let is_noop = models_to_persist.is_empty() && drops_in_batch.is_empty();
        let runtime_changes = if is_noop {
            RuntimeChanges {
                domain: domain.clone(),
                graph: None,
                changes: Vec::new(),
            }
        } else {
            runtime_changes_for_domain(
                domain,
                (domain_state.graph.node_count() > 0).then_some(domain_state.graph.clone()),
                &current_state.models,
                &domain_state.models,
            )
        };

        Ok(TransactionMutationPreflight {
            planned: Some(PlannedMutations {
                domain: domain.clone(),
                batch_size,
                operation_name: operation_name.to_string(),
                base_models: current_models,
                domain_state,
                models_to_persist,
                drops_in_batch,
                runtime_changes,
                quiesce,
            }),
            mutation_quiesce_levels,
        })
    }

    pub(crate) fn commit_planned(
        &self,
        planned: PlannedMutations,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        let _commit_guard = self.commit_lock.lock();
        let current_models = self
            .storage
            .list_models(&planned.domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| record.model)
            .collect::<ModelIndex>();
        if current_models != planned.base_models {
            return Err(Report::new(RegistryError::ConcurrentMutation {
                domain: planned.domain.as_str().to_string(),
            }));
        }

        self.storage
            .commit_batch(
                &planned.domain,
                &planned.models_to_persist,
                &planned.drops_in_batch,
            )
            .change_context(RegistryError::PersistBatch)?;

        let current = self.state.read();
        let mut domains = current.domains.clone();
        if planned.domain_state.graph.node_count() == 0 {
            domains.remove(&planned.domain);
        } else {
            domains.insert(planned.domain.clone(), planned.domain_state);
        }
        drop(current);

        let mut writer = self.state.write();
        *writer = Arc::new(RegistryState { domains });

        let graph_snapshot = match writer.domains.get(&planned.domain) {
            Some(state) => state.graph.describe(),
            None => String::new(),
        };
        let node_count = match writer.domains.get(&planned.domain) {
            Some(state) => state.graph.node_count(),
            None => 0,
        };
        let edge_count = match writer.domains.get(&planned.domain) {
            Some(state) => state.graph.edge_count(),
            None => 0,
        };

        info!(
            domain = planned.domain.as_str(),
            batch_size = planned.batch_size,
            operation = planned.operation_name,
            result = "ok",
            node_count,
            edge_count,
            "applied mutation batch\n{}",
            graph_snapshot
        );

        Ok(planned.runtime_changes)
    }

    pub(crate) fn rollback_committed(
        &self,
        planned: PlannedMutations,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        let _commit_guard = self.commit_lock.lock();
        let current_models = self
            .storage
            .list_models(&planned.domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| record.model)
            .collect::<ModelIndex>();
        if current_models != planned.domain_state.models {
            return Err(Report::new(RegistryError::ConcurrentMutation {
                domain: planned.domain.as_str().to_string(),
            }));
        }

        let models_to_persist = planned
            .base_models
            .iter()
            .filter_map(|(key, model)| match current_models.get(key) {
                None => Some((key.clone(), RegistryPersistMutation::Create(model.clone()))),
                Some(current) if current != model => {
                    Some((key.clone(), RegistryPersistMutation::Replace(model.clone())))
                }
                Some(_) => None,
            })
            .collect::<HashMap<_, _>>();
        let drops = current_models
            .nodes()
            .filter(|key| !planned.base_models.contains(key))
            .cloned()
            .collect::<HashSet<_>>();
        self.storage
            .commit_batch(&planned.domain, &models_to_persist, &drops)
            .change_context(RegistryError::PersistBatch)?;

        let base_state = self.build_domain_state(&planned.domain, &planned.base_models)?;
        let runtime_changes = runtime_changes_for_domain(
            &planned.domain,
            (base_state.graph.node_count() > 0).then_some(base_state.graph.clone()),
            &planned.domain_state.models,
            &base_state.models,
        );
        let current = self.state.read();
        let mut domains = current.domains.clone();
        if base_state.graph.node_count() == 0 {
            domains.remove(&planned.domain);
        } else {
            domains.insert(planned.domain.clone(), base_state);
        }
        drop(current);
        *self.state.write() = Arc::new(RegistryState { domains });

        Ok(runtime_changes)
    }

    /// The `M` named `identifier` in `domain`, when the domain configures one.
    ///
    /// `M` names the kind the read is keyed by, so a hit is an `M` and the caller has no second
    /// outcome to answer for. Stored bytes that decode to another kind are corrupt storage rather
    /// than configuration a domain can hold, and they surface here once as
    /// [`RegistryError::StoredModelKindMismatch`].
    pub(crate) fn get<M: UniquelyKindedModel>(
        &self,
        domain: &DomainName,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<M>, Report<RegistryError>> {
        let identifier = identifier.into();
        let Some(model) = self.get_of_kind(domain, M::KIND, identifier.clone())? else {
            return Ok(None);
        };
        let stored_kind = model.kind();
        let Some(model) = M::from_model(model) else {
            return Err(Report::new(RegistryError::StoredModelKindMismatch {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: M::KIND.as_str(),
                stored_kind: stored_kind.as_str(),
            }));
        };
        Ok(Some(model))
    }

    /// The model named `identifier` in `domain` under a kind the caller learns at runtime.
    ///
    /// `SHOW CREATE` takes the kind from the statement it is rendering, so it reads the union and
    /// renders whatever the domain stores. Every read that names its kind in its own source uses
    /// [`Self::get`] instead.
    pub(crate) fn get_of_kind(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<Model>, Report<RegistryError>> {
        let identifier = identifier.into();
        self.storage
            .get(domain, kind, identifier)
            .change_context(RegistryError::LoadStoredModels)
    }

    /// Whether `domain` already configures a model of `kind` named `identifier`.
    ///
    /// `CREATE ... IF NOT EXISTS` only asks whether the name is taken, so it never decodes the
    /// stored model to find out.
    pub(crate) fn contains(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<bool, Report<RegistryError>> {
        self.storage
            .contains(domain, kind, identifier)
            .change_context(RegistryError::LoadStoredModels)
    }

    pub(crate) fn list_identifiers(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        prefix: &str,
    ) -> Result<Vec<ModelName>, Report<RegistryError>> {
        self.storage
            .list_identifiers(domain, kind, prefix)
            .change_context(RegistryError::LoadStoredModels)
    }

    /// Identifiers of `kind` in the configuration `queued` produces when applied to `domain` in
    /// written order. Only the create and drop sequence decides a name, so an intermediate
    /// configuration that does not yet resolve still reports the names it defines.
    pub(crate) fn resulting_identifiers(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        prefix: &str,
        queued: &[RegistryMutation],
    ) -> Result<Vec<ModelName>, Report<RegistryError>> {
        let committed = self.list_identifiers(domain, kind, prefix)?;
        if queued.is_empty() {
            return Ok(committed);
        }

        let prefix = prefix.to_ascii_lowercase();
        let mut identifiers = committed.into_iter().collect::<BTreeSet<_>>();
        for mutation in queued {
            let target = mutation.target_key();
            if target.kind == kind {
                identifiers.remove(&target.identifier);
            }
            if let Some(resulting) = mutation.resulting_key()
                && resulting.kind == kind
                && resulting.identifier.as_str().starts_with(&prefix)
            {
                identifiers.insert(resulting.identifier);
            }
        }
        Ok(identifiers.into_iter().collect())
    }

    /// The models of `domain` as `queued` leaves them, applied in written order and without
    /// validating the result. An alteration whose target is gone is skipped, because this describes
    /// configuration a client is still writing rather than a plan that will be persisted.
    pub(crate) fn resulting_models(
        &self,
        domain: &DomainName,
        queued: &[RegistryMutation],
    ) -> Result<Vec<Model>, Report<RegistryError>> {
        let mut models = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| record.model)
            .collect::<ModelIndex>();
        for mutation in queued {
            mutation.fold_into_models(&mut models);
        }
        Ok(models.into_iter().map(|(_, model)| model).collect())
    }

    pub(crate) fn active_graph(&self, domain: &DomainName) -> Option<ActiveGraph> {
        let state = self.state.read();
        state.domains.get(domain).map(|ns| ns.graph.clone())
    }

    pub(crate) fn placement_plan(
        &self,
        domain: &DomainName,
        default_policy: PlacementPolicy,
    ) -> Option<PlacementPlan> {
        let state = self.state.read();
        state
            .domains
            .get(domain)
            .map(|domain_state| domain_state.graph.placement_plan(default_policy))
    }

    pub(crate) fn active_graphs(&self) -> Vec<(DomainName, ActiveGraph)> {
        let state = self.state.read();
        let mut graphs = state
            .domains
            .iter()
            .map(|(domain, domain_state)| (domain.clone(), domain_state.graph.clone()))
            .collect::<Vec<_>>();
        graphs.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        graphs
    }

    pub(crate) fn active_domain_entities(&self, domain: &DomainName) -> Vec<NodeRef> {
        let state = self.state.read();
        let Some(domain_state) = state.domains.get(domain) else {
            return Vec::new();
        };
        let mut entities = domain_state
            .graph
            .nodes()
            .into_iter()
            .filter(|node| !node.is_dataflow_node())
            .map(|node| node.node_ref())
            .collect::<Vec<_>>();
        entities.sort();
        entities
    }

    fn build_domain_state(
        &self,
        domain: &DomainName,
        models: &ModelIndex,
    ) -> Result<DomainState, Report<RegistryError>> {
        DomainState::build(domain, models)
    }

    fn active_graph_snapshot(&self, domain: &DomainName) -> String {
        match self.active_graph(domain) {
            Some(graph) => graph.describe(),
            None => String::new(),
        }
    }
}

fn log_registry_state(message: &str, state: &RegistryState) {
    if state.domains.is_empty() {
        info!(result = "ok", "{message}");
        return;
    }

    for (domain, domain_state) in &state.domains {
        let active_graph = domain_state.graph.describe();
        info!(
            domain = domain.as_str(),
            result = "ok",
            node_count = domain_state.graph.node_count(),
            edge_count = domain_state.graph.edge_count(),
            "{message}\n{}",
            active_graph
        );
    }
}

struct ModelStorage {
    db: Database,
    index: Keyspace,
}

impl ModelStorage {
    fn from_database(db: Database) -> Result<Self, Report<RegistryError>> {
        let index = db
            .keyspace("models", KeyspaceCreateOptions::default)
            .change_context(RegistryError::OpenKeyspace)?;

        Ok(Self { db, index })
    }

    #[cfg(test)]
    fn put(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: &ModelName,
        model: &Model,
    ) -> Result<(), Report<RegistryError>> {
        let key = encode_key(domain, kind, identifier)?;

        if self
            .index
            .get(key.clone())
            .change_context(RegistryError::ReadValue)?
            .is_some()
        {
            return Err(Report::new(RegistryError::AlreadyExists {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
            }));
        }

        let value = serialize_value(model)?;

        self.index
            .insert(key, value)
            .change_context(RegistryError::WriteValue)
    }

    fn commit_batch(
        &self,
        domain: &DomainName,
        models_to_persist: &HashMap<NodeRef, RegistryPersistMutation>,
        drops_in_batch: &HashSet<NodeRef>,
    ) -> Result<(), Report<RegistryError>> {
        let encoded_models = models_to_persist
            .iter()
            .map(|(key, mutation)| {
                let model = match mutation {
                    RegistryPersistMutation::Create(model)
                    | RegistryPersistMutation::Replace(model) => model,
                };
                Ok((
                    encode_key(domain, key.kind, &key.identifier)?,
                    serialize_value(model)?,
                ))
            })
            .collect::<Result<Vec<_>, Report<RegistryError>>>()?;
        let encoded_drops = drops_in_batch
            .iter()
            .map(|key| encode_key(domain, key.kind, &key.identifier))
            .collect::<Result<Vec<_>, Report<RegistryError>>>()?;

        let mut batch = self.db.batch();
        for (key, value) in encoded_models {
            batch.insert(&self.index, key, value);
        }
        for key in encoded_drops {
            batch.remove(&self.index, key);
        }
        batch.commit().change_context(RegistryError::WriteValue)
    }

    fn get(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<Model>, Report<RegistryError>> {
        let identifier = identifier.into();
        let key = encode_key(domain, kind, identifier)?;
        let Some(raw) = self
            .index
            .get(key)
            .change_context(RegistryError::ReadValue)?
        else {
            return Ok(None);
        };

        deserialize_value(raw.as_ref()).map(Some)
    }

    fn contains(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<bool, Report<RegistryError>> {
        let key = encode_key(domain, kind, identifier.into())?;
        self.index
            .contains_key(key)
            .change_context(RegistryError::ReadValue)
    }

    fn list_identifiers(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        prefix: &str,
    ) -> Result<Vec<ModelName>, Report<RegistryError>> {
        let mut out = Vec::new();
        let prefix = prefix.to_ascii_lowercase();

        for record in self.list_records()? {
            if &record.domain != domain {
                continue;
            }

            if record.model.kind() != kind {
                continue;
            }

            let identifier = record.model.name();
            if !identifier.as_str().starts_with(&prefix) {
                continue;
            }

            out.push(identifier);
        }

        out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        out.dedup_by(|a, b| a.as_str() == b.as_str());
        Ok(out)
    }

    fn list_models(
        &self,
        domain: &DomainName,
    ) -> Result<Vec<StoredModelRecord>, Report<RegistryError>> {
        self.list_records().map(|records| {
            records
                .into_iter()
                .filter(|record| &record.domain == domain)
                .collect()
        })
    }

    fn list_all_models(&self) -> Result<Vec<StoredModelRecord>, Report<RegistryError>> {
        self.list_records()
    }

    fn list_records(&self) -> Result<Vec<StoredModelRecord>, Report<RegistryError>> {
        let mut records = Vec::new();

        for guard in self.index.iter() {
            let (raw_key, raw_value) = guard
                .into_inner()
                .change_context(RegistryError::ReadValue)?;

            let key: ModelKeyOwned =
                storekey::deserialize(&raw_key).change_context(RegistryError::DecodeKey)?;

            let model = deserialize_value(raw_value.as_ref())?;

            let domain = DomainName::parse(&key.domain).change_context(RegistryError::DecodeKey)?;
            let kind = ModelKind::from_str(&key.kind)
                .map_err(|_| Report::new(RegistryError::DecodeKey))?;
            let identifier =
                ModelName::parse(&key.identifier).change_context(RegistryError::DecodeKey)?;

            // Every model is written under the key it reports, so a stored pair that disagrees is
            // corrupt storage. Checking it once here is what lets every reader below take the node
            // from the model and never reconcile the two again.
            let stored = NodeRef::new(kind, identifier);
            if stored != model.node_ref() {
                return Err(Report::new(RegistryError::StoredModelKindMismatch {
                    domain: domain.as_str().to_string(),
                    identifier: stored.identifier.as_str().to_string(),
                    expected_kind: stored.kind.as_str(),
                    stored_kind: model.kind().as_str(),
                }));
            }

            records.push(StoredModelRecord { domain, model });
        }

        Ok(records)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct ModelKey<'a> {
    domain: &'a str,
    kind: &'a str,
    identifier: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ModelKeyOwned {
    domain: String,
    kind: String,
    identifier: String,
}

fn encode_key(
    domain: &DomainName,
    kind: ModelKind,
    identifier: impl Into<ModelName>,
) -> Result<Vec<u8>, Report<RegistryError>> {
    let identifier = identifier.into();
    storekey::serialize(&ModelKey {
        domain: domain.as_str(),
        kind: kind.as_str(),
        identifier: identifier.as_str(),
    })
    .change_context(RegistryError::EncodeKey)
}

fn serialize_value(model: &Model) -> Result<Vec<u8>, Report<RegistryError>> {
    rkyv::to_bytes::<rkyv::rancor::Error>(model)
        .map(|bytes| bytes.to_vec())
        .change_context(RegistryError::SerializeValue)
}

fn deserialize_value(bytes: &[u8]) -> Result<Model, Report<RegistryError>> {
    rkyv::from_bytes::<Model, rkyv::rancor::Error>(bytes)
        .change_context(RegistryError::DeserializeValue)
}

fn runtime_changes_for_domain(
    domain: &DomainName,
    graph: Option<ActiveGraph>,
    current_models: &ModelIndex,
    candidate_models: &ModelIndex,
) -> RuntimeChanges {
    let current_ingestor_ids = SortedSet::from_unsorted(
        current_models
            .iter()
            .filter_map(|(key, model)| {
                matches!(model, Model::Ingestor(_)).then_some(key.identifier.clone())
            })
            .collect::<Vec<_>>(),
    )
    .into_vec();
    let candidate_ingestor_ids = SortedSet::from_unsorted(
        candidate_models
            .iter()
            .filter_map(|(key, model)| {
                matches!(model, Model::Ingestor(_)).then_some(key.identifier.clone())
            })
            .collect::<Vec<_>>(),
    )
    .into_vec();

    let mut changes = Vec::new();

    for ingestor in &current_ingestor_ids {
        changes.push(RuntimeChange::StopIngestor {
            ingestor: IngestorName::from(ingestor),
        });
    }

    for ingestor in &candidate_ingestor_ids {
        let Some(Model::Ingestor(ingestor_model)) =
            candidate_models.get(&NodeRef::new(ModelKind::Ingestor, ingestor.clone()))
        else {
            continue;
        };
        let source_ref = ingestor_model.source.source_ref();
        let source_kind = match &ingestor_model.source {
            IngestSource::Http { .. }
            | IngestSource::Kafka { .. }
            | IngestSource::Pulsar { .. }
            | IngestSource::Prometheus { .. }
            | IngestSource::RabbitMq { .. }
            | IngestSource::RedisPubSub { .. }
            | IngestSource::Mqtt { .. }
            | IngestSource::Nats { .. }
            | IngestSource::ZeroMq { .. }
            | IngestSource::Sqs { .. }
            | IngestSource::Websockets { .. }
            | IngestSource::Syslog { .. } => ModelKind::Client,
            IngestSource::Endpoint { .. } => ModelKind::Endpoint,
        };
        let Some(source_model) =
            candidate_models.get(&NodeRef::new(source_kind, source_ref.clone()))
        else {
            continue;
        };
        changes.push(RuntimeChange::StartIngestor {
            source_model: Box::new(source_model.clone()),
            ingestor: Box::new(ingestor_model.clone()),
        });
    }

    RuntimeChanges {
        domain: domain.clone(),
        graph,
        changes,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nervix_models::{
        AckMode, AlterEmitter, AlterJunction, AlterProcessorOperation, AlterRelay,
        AlterRelayOperation, AlterSchema, AlterSchemaOperation, AlterWireSchema,
        AlterWireSchemaOperation, BranchSelection, ClientName, ClusterNodeName, CodecWireFormat,
        CreateWireSchema, DropModel, EmitterName, FlushPolicy, JsonType, MaterializedRelayState,
        ParseAsType, ProcessorInputs, ProcessorOutputs, QuiesceLevel, RelayName, SchemaField,
        SchemaName, WireSchemaField,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::registry::test_fixtures::{
        assert_example_graph_validates, branch_for_relay, branch_schema, client_model, codec,
        emitter, full_graph_batch, ingestor, ingestor_with_params, named, processor, relay,
        relay_branched_by_relay_branch, relay_branched_like, sample_transport_model, schema,
        temp_db_path, wire_schema, with_inherit_all,
    };

    #[test]
    fn runnable_example_graphs_validate() {
        assert_example_graph_validates("iot", include_str!("../../examples/iot/iot.nspl"));
        assert_example_graph_validates(
            "nats_factory_windows",
            include_str!("../../examples/nats-factory-windows/nats_factory_windows.nspl"),
        );
        assert_example_graph_validates(
            "datalake",
            include_str!("../../examples/datalake/datalake.nspl"),
        );
        assert_example_graph_validates(
            "wasm_dual",
            include_str!("../../examples/wasm-processors/wasm-dual.nspl"),
        );
        assert_example_graph_validates(
            "binance_websocket",
            include_str!("../../examples/binance-websocket/binance_websocket.nspl"),
        );
        assert_example_graph_validates(
            "onnx_batched",
            include_str!("../../examples/onnx-inference/batched.nspl"),
        );
        assert_example_graph_validates(
            "onnx_per_message",
            include_str!("../../examples/onnx-inference/per-message.nspl"),
        );
    }

    #[test]
    fn create_fails_when_model_already_exists() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&ns, vec![sample_transport_model("kafka_main")])
            .expect("partial graph should succeed");
        let err = registry
            .apply_batch(&ns, vec![sample_transport_model("kafka_main")])
            .expect_err("duplicate create must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::AlreadyExists { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn create_allows_same_identifier_for_different_kinds() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &ns,
                vec![schema("shared_name"), client_model("shared_name")],
            )
            .expect("different kinds should be allowed to share an identifier");

        assert!(
            registry
                .get::<CreateSchema>(
                    &ns,
                    ModelName::parse("shared_name").expect("valid model name"),
                )
                .expect("schema read should succeed")
                .is_some()
        );
        assert!(
            registry
                .get_of_kind(
                    &ns,
                    ModelKind::Client,
                    ModelName::parse("shared_name").expect("valid model name"),
                )
                .expect("client read should succeed")
                .is_some()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn open_fails_when_persisted_state_is_invalid() {
        let path = temp_db_path();
        let db = Database::builder(&path)
            .open()
            .expect("database should open");
        let storage = ModelStorage::from_database(db).expect("storage should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let schema = schema("event_schema");
        let wire_schema = wire_schema("event_wire");
        let relay = relay("raw_events", "event_schema");
        let model = ingestor("kafka_ingestor", "raw_events", "event_codec", "kafka_main");

        storage
            .put(&domain, schema.kind(), &schema.name(), &schema)
            .expect("write should succeed");
        storage
            .put(
                &domain,
                wire_schema.kind(),
                &wire_schema.name(),
                &wire_schema,
            )
            .expect("write should succeed");
        storage
            .put(&domain, relay.kind(), &relay.name(), &relay)
            .expect("write should succeed");
        storage
            .put(&domain, model.kind(), &model.name(), &model)
            .expect("write should succeed");
        drop(storage);

        let err = Registry::open(&path)
            .err()
            .expect("invalid persisted state must fail startup");
        assert!(
            format!("{err}").contains("requires missing codec 'event_codec'"),
            "unexpected startup error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn list_identifiers_filters_by_kind_and_prefix() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = DomainName::parse("default").expect("valid domain");

        registry
            .storage
            .put(
                &ns,
                ModelKind::Client,
                &ModelName::from(&ClientName::parse("kafka_main").expect("valid client name")),
                &sample_transport_model("kafka_main"),
            )
            .expect("write should succeed");

        let transports = registry
            .list_identifiers(&ns, ModelKind::Client, "kafka_")
            .expect("list should succeed");
        assert_eq!(
            transports
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            vec!["kafka_main"]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn get_roundtrip_returns_stored_model() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = DomainName::parse("default").expect("valid domain");
        let id = ClientName::parse("kafka_main").expect("valid client name");
        let model = sample_transport_model("kafka_main");

        registry
            .storage
            .put(&ns, ModelKind::Client, &ModelName::from(&id), &model)
            .expect("create should succeed");
        let loaded = registry
            .get_of_kind(&ns, ModelKind::Client, &id)
            .expect("read should succeed")
            .expect("model should exist");

        assert_eq!(loaded, model);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn synchronized_domain_schedule_persists_models_for_restart() {
        let source_path = temp_db_path();
        let source = Registry::open(&source_path).expect("source registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let mut models = full_graph_batch();
        let Model::Relay(notifications) = models
            .iter_mut()
            .find(|model| {
                model.kind() == ModelKind::Relay && model.name().as_str() == "notifications"
            })
            .expect("notifications relay should exist")
        else {
            panic!("notifications model should be a relay");
        };
        notifications.materialized_state = Some(MaterializedRelayState::LastByTimestamp);
        source
            .apply_batch(&domain, models)
            .expect("source graph should be valid");
        let schedule = source
            .active_graph(&domain)
            .expect("source graph should exist")
            .schedule_for_domain(
                &domain,
                &[ClusterNodeName::parse("node-1").expect("valid name")],
                0,
                PlacementPolicy::Neutral,
            );
        let scheduled_relay = schedule
            .nodes
            .get(&NodeRef::new(
                ModelKind::Relay,
                named::<ModelName>("notifications"),
            ))
            .expect("fixture schedule must include its materialized relay");
        assert_eq!(
            scheduled_relay.assigned_nodes,
            [named::<ClusterNodeName>("node-1")]
        );

        let replica_path = temp_db_path();
        {
            let replica = Registry::open(&replica_path).expect("replica registry should open");
            replica
                .synchronize_cluster_schedule(&ClusterSchedule::from_iter([schedule]))
                .expect("schedule models should synchronize");
        }

        let reopened = Registry::open(&replica_path).expect("replica registry should reopen");
        assert_eq!(
            reopened
                .get::<CreateIngestor>(&domain, named::<ModelName>("ing"))
                .expect("replica model read should succeed"),
            source
                .get::<CreateIngestor>(&domain, named::<ModelName>("ing"))
                .expect("source model read should succeed")
        );

        let _ = fs::remove_dir_all(source_path);
        let _ = fs::remove_dir_all(replica_path);
    }

    #[test]
    fn apply_batch_accepts_partial_graphs() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![schema("event_schema"), client_model("kafka_main")],
            )
            .expect("partial graph should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        assert_eq!(graph.node_count(), 2);
        assert_eq!(graph.edge_count(), 0);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn alter_relay_set_capacity_updates_stored_model_and_active_graph() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay("notifications", "event_schema"),
                ],
            )
            .expect("create should succeed");

        let changes = registry
            .alter_relay(
                &domain,
                AlterRelay {
                    relay: named("notifications"),
                    operations: vec![AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(5usize),
                    }],
                },
            )
            .expect("alter should succeed");
        assert!(
            changes.changes.is_empty(),
            "capacity updates are applied from the published schedule delta"
        );
        assert!(changes.graph.is_some());

        let stored_relay = registry
            .get::<CreateRelay>(&domain, named::<ModelName>("notifications"))
            .expect("read should succeed")
            .expect("relay should exist");
        assert_eq!(stored_relay.buffer, nonzero!(5usize));

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let node = graph
            .node(ModelKind::Relay, &named("notifications"))
            .expect("relay node should exist");
        let Model::Relay(graph_relay) = node.config.as_ref() else {
            panic!("graph node should contain relay config");
        };
        assert_eq!(graph_relay.buffer, nonzero!(5usize));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn mutation_plan_classifies_no_op_and_relay_capacity_from_the_model_diff() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay("notifications", "event_schema"),
                ],
            )
            .expect("initial graph should succeed");

        let noop = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterRelay(AlterRelay {
                    relay: named("notifications"),
                    operations: vec![AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(1usize),
                    }],
                })],
            )
            .expect("no-op alter should plan");
        assert!(noop.is_noop());
        assert_eq!(noop.quiesce().level(), QuiesceLevel::Dynamic);
        assert!(noop.quiesce().affected_entities().is_empty());

        let capacity = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterRelay(AlterRelay {
                    relay: named("notifications"),
                    operations: vec![AlterRelayOperation::SetCapacity {
                        capacity: nonzero!(5usize),
                    }],
                })],
            )
            .expect("capacity alter should plan");
        assert!(!capacity.is_noop());
        assert_eq!(capacity.quiesce().level(), QuiesceLevel::Dynamic);
        assert_eq!(
            capacity.quiesce().affected_entities(),
            &[super::NodeRef {
                kind: ModelKind::Relay,
                identifier: named("notifications"),
            }]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn transaction_preflight_classifies_each_mutation_against_its_prefix() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay("notifications", "event_schema"),
                ],
            )
            .expect("initial graph should succeed");

        let preflight = registry
            .preflight_transaction_mutations(
                &domain,
                &[
                    RegistryMutation::AlterSchema(AlterSchema {
                        schema: named("event_schema"),
                        operations: vec![AlterSchemaOperation::AddField {
                            field: SchemaField {
                                name: named("note"),
                                ty: ParseAsType::String,
                                optional: true,
                                sensitive: false,
                            },
                        }],
                    }),
                    RegistryMutation::AlterRelay(AlterRelay {
                        relay: named("notifications"),
                        operations: vec![AlterRelayOperation::SetCapacity {
                            capacity: nonzero!(5usize),
                        }],
                    }),
                ],
            )
            .expect("transaction preflight should succeed");

        assert_eq!(
            preflight.mutation_quiesce_levels(),
            &[QuiesceLevel::DomainPause, QuiesceLevel::Dynamic]
        );
        assert_eq!(
            preflight
                .planned()
                .expect("the candidate graph is complete")
                .quiesce()
                .level(),
            QuiesceLevel::DomainPause
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn junction_alter_is_applied_before_diff_based_quiesce_classification() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay("incoming", "event_schema"),
                    relay("outgoing", "event_schema"),
                    Model::Junction(CreateJunction {
                        name: named("route_events"),
                        from: ProcessorInputs::single(named("incoming")),
                        output_routes: with_inherit_all(ProcessorOutputs::single(named(
                            "outgoing",
                        )))
                        .with_flush_policy(FlushPolicy::Each {
                            interval: "100ms".to_string(),
                            max_batch_size: "1MiB".to_string(),
                        }),
                        branched_by: BranchSelection::unbranched(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect("initial graph should succeed");

        let dynamic = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterJunction(AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::SetFilterWhere {
                        where_clause: nervix_nspl::parse_expression("input.value != ''")
                            .expect("valid expression"),
                    }],
                })],
            )
            .expect("filter alter should plan");
        assert_eq!(dynamic.quiesce().level(), QuiesceLevel::Dynamic);

        let entity_pause = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterJunction(AlterJunction {
                    junction: named("route_events"),
                    operations: vec![AlterProcessorOperation::SetMode {
                        mode: AckMode::Detached,
                    }],
                })],
            )
            .expect("mode alter should plan");
        assert_eq!(entity_pause.quiesce().level(), QuiesceLevel::EntityPause);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_alter_is_applied_before_diff_based_quiesce_classification() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("sink_a"),
                    client_model("sink_b"),
                    relay("outgoing", "event_schema"),
                    emitter("event_sink", "outgoing", "event_codec", "sink_a"),
                ],
            )
            .expect("initial graph should succeed");

        let dynamic = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterEmitter(AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![nervix_models::AlterEmitterOperation::SetFlush {
                        flush_policy: FlushPolicy::Immediate,
                    }],
                })],
            )
            .expect("flush alter should plan");
        assert_eq!(dynamic.quiesce().level(), QuiesceLevel::Dynamic);

        let entity_pause = registry
            .plan_mutations(
                &domain,
                &[RegistryMutation::AlterEmitter(AlterEmitter {
                    emitter: named("event_sink"),
                    operations: vec![nervix_models::AlterEmitterOperation::SetClient {
                        client: named("sink_b"),
                    }],
                })],
            )
            .expect("client alter should plan");
        assert_eq!(entity_pause.quiesce().level(), QuiesceLevel::EntityPause);
        assert_eq!(
            entity_pause.quiesce().affected_entities(),
            &[super::NodeRef {
                kind: ModelKind::Emitter,
                identifier: named("event_sink"),
            }]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn relay_drop_create_same_key_is_classified_as_a_model_change() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    schema("event_schema_v2"),
                    relay("notifications", "event_schema"),
                ],
            )
            .expect("initial graph should succeed");

        let planned = registry
            .plan_mutations(
                &domain,
                &[
                    RegistryMutation::Drop(DropModel {
                        kind: ModelKind::Relay,
                        name: named("notifications"),
                    }),
                    RegistryMutation::Create(Box::new(relay("notifications", "event_schema_v2"))),
                ],
            )
            .expect("relay recreation should plan");

        assert_eq!(planned.quiesce().level(), QuiesceLevel::DomainPause);
        assert_eq!(
            planned.quiesce().affected_entities(),
            &[super::NodeRef {
                kind: ModelKind::Relay,
                identifier: named("notifications"),
            }]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn referenced_codec_drop_create_same_key_is_classified_as_domain_pause() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        let mut models = full_graph_batch();
        models.push(Model::WireJsonSchema(CreateWireSchema {
            name: named("event_wire_v2"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("value"),
                ty: JsonType::String,
                optional: false,
            }],
        }));
        registry
            .apply_batch(&domain, models)
            .expect("initial graph should succeed");

        let Model::Codec(mut replacement) = codec("event_codec", "event_schema") else {
            unreachable!("codec helper must build a codec model");
        };
        replacement.wire_format = CodecWireFormat::Json {
            wire_schema: named("event_wire_v2"),
        };
        let planned = registry
            .plan_mutations(
                &domain,
                &[
                    RegistryMutation::Drop(DropModel {
                        kind: ModelKind::Codec,
                        name: named("event_codec"),
                    }),
                    RegistryMutation::Create(Box::new(Model::Codec(replacement))),
                ],
            )
            .expect("referenced codec recreation should plan");

        assert_eq!(planned.quiesce().level(), QuiesceLevel::DomainPause);
        assert_eq!(
            planned.quiesce().affected_entities(),
            &[super::NodeRef {
                kind: ModelKind::Codec,
                identifier: named("event_codec"),
            }]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn alter_relay_rejects_missing_relay_without_persisting() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let result = registry.alter_relay(
            &domain,
            AlterRelay {
                relay: named("notifications"),
                operations: vec![AlterRelayOperation::SetCapacity {
                    capacity: nonzero!(5usize),
                }],
            },
        );
        assert!(matches!(
            result
                .expect_err("missing relay should be rejected")
                .current_context(),
            RegistryError::NotFound { .. }
        ));
        assert!(
            registry
                .get::<CreateRelay>(&domain, named::<ModelName>("notifications"),)
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn startup_runtime_changes_include_graph_only_domains() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_out"),
                    relay("notifications", "event_schema"),
                    emitter("emit", "notifications", "event_codec", "broker_out"),
                ],
            )
            .expect("graph-only batch should succeed");

        let startup_changes = registry
            .startup_runtime_changes()
            .expect("startup runtime changes should load");
        let change = startup_changes
            .iter()
            .find(|change| change.domain == domain)
            .expect("domain runtime changes should exist");

        assert!(change.graph.is_some(), "graph snapshot must be included");
        assert!(
            change.changes.is_empty(),
            "graph-only domain should not synthesize ingestor lifecycle changes"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn adding_second_ingestor_restarts_existing_ingestor_and_starts_new_one() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    ingestor("ing1", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect("initial graph should succeed");

        let changes = registry
            .apply_batch(
                &domain,
                vec![ingestor(
                    "ing2",
                    "notifications",
                    "event_codec",
                    "kafka_main",
                )],
            )
            .expect("adding second ingestor should succeed");

        let stop_names = changes
            .changes
            .iter()
            .filter_map(|change| match change {
                RuntimeChange::StopIngestor { ingestor } => Some(ingestor.as_str().to_string()),
                RuntimeChange::StartIngestor { .. } => None,
            })
            .collect::<Vec<_>>();
        let start_names = changes
            .changes
            .iter()
            .filter_map(|change| match change {
                RuntimeChange::StartIngestor { ingestor, .. } => {
                    Some(ingestor.name.as_str().to_string())
                }
                RuntimeChange::StopIngestor { .. } => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(stop_names, vec!["ing1"]);
        assert_eq!(start_names, vec!["ing1", "ing2"]);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_missing_references_without_persisting() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![ingestor(
                    "kafka_ingestor",
                    "raw_events",
                    "event_codec",
                    "kafka_main",
                )],
            )
            .expect_err("missing dependencies must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::MissingReference { .. }
        ));
        assert!(
            registry
                .get::<CreateIngestor>(
                    &domain,
                    IngestorName::parse("kafka_ingestor").expect("valid ingestor name"),
                )
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_is_order_independent() {
        let domain = DomainName::parse("default").expect("valid domain");

        let path_a = temp_db_path();
        let registry_a = Registry::open(&path_a).expect("registry should open");
        registry_a
            .apply_batch(&domain, full_graph_batch())
            .expect("ordered batch should succeed");
        let graph_a = registry_a
            .active_graph(&domain)
            .expect("graph should be installed");

        let path_b = temp_db_path();
        let registry_b = Registry::open(&path_b).expect("registry should open");
        let batch_b = vec![
            schema("event_schema"),
            wire_schema("event_wire"),
            codec("event_codec", "event_schema"),
            client_model("broker_out"),
            relay_branched_like("p99", "event_schema", "notifications"),
            relay_branched_by_relay_branch("notifications", "event_schema"),
            emitter("emit", "p99", "event_codec", "broker_out"),
            branch_schema("value_branch", &["value"]),
            branch_for_relay("notifications", "value_branch"),
            ingestor_with_params(
                "ing",
                "notifications",
                "event_codec",
                "broker_in",
                &["value"],
            ),
            processor("p99_proc", "notifications", "p99"),
            client_model("broker_in"),
        ];

        registry_b
            .apply_batch(&domain, batch_b)
            .expect("reordered batch should also succeed");
        let graph_b = registry_b
            .active_graph(&domain)
            .expect("graph should be installed");

        assert_eq!(graph_a.node_count(), 12);
        assert_eq!(graph_a.edge_count(), 21);
        assert_eq!(graph_a.node_count(), graph_b.node_count());
        assert_eq!(graph_a.edge_count(), graph_b.edge_count());

        let _ = fs::remove_dir_all(path_a);
        let _ = fs::remove_dir_all(path_b);
    }

    #[test]
    fn failed_batch_does_not_mutate_registry_state() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_out"),
                    emitter("emit", "missing_stream", "event_codec", "broker_out"),
                ],
            )
            .expect_err("invalid batch must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::MissingReference { .. }
        ));
        assert!(
            registry.active_graph(&domain).is_none(),
            "failed batch must not install a graph"
        );
        assert!(
            registry
                .get::<CreateSchema>(
                    &domain,
                    SchemaName::parse("event_schema").expect("valid schema name"),
                )
                .expect("read should succeed")
                .is_none()
        );
        assert!(
            registry
                .get_of_kind(
                    &domain,
                    ModelKind::Client,
                    RelayName::parse("broker_out").expect("valid relay name")
                )
                .expect("read should succeed")
                .is_none()
        );
        assert!(
            registry
                .get::<CreateEmitter>(
                    &domain,
                    EmitterName::parse("emit").expect("valid emitter name"),
                )
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn planned_schema_alters_do_not_mutate_until_committed() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect("create should succeed");
        let planned = registry
            .plan_mutations(
                &domain,
                &[
                    RegistryMutation::AlterSchema(AlterSchema {
                        schema: named("event_schema"),
                        operations: vec![AlterSchemaOperation::AddField {
                            field: SchemaField {
                                name: named("note"),
                                ty: ParseAsType::String,
                                optional: true,
                                sensitive: false,
                            },
                        }],
                    }),
                    RegistryMutation::AlterWireJsonSchema(AlterWireSchema {
                        schema: named("event_wire"),
                        operations: vec![AlterWireSchemaOperation::AddField {
                            field: WireSchemaField {
                                name: named("note"),
                                ty: JsonType::String,
                                optional: true,
                            },
                        }],
                    }),
                ],
            )
            .expect("planning should succeed");

        let before = registry
            .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
            .expect("read should succeed")
            .expect("schema should exist");
        assert_eq!(before.fields.len(), 1, "planning must not persist");

        registry
            .commit_planned(planned)
            .expect("commit should succeed");

        let after = registry
            .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
            .expect("read should succeed")
            .expect("schema should exist");
        assert_eq!(after.fields.len(), 2);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn failed_mixed_schema_alter_batch_applies_nothing() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");
        registry
            .apply_batch(&domain, vec![schema("event_schema")])
            .expect("create should succeed");

        let error = registry
            .apply_mutation_batch(
                &domain,
                vec![
                    RegistryMutation::Create(Box::new(schema("new_schema"))),
                    RegistryMutation::AlterSchema(AlterSchema {
                        schema: named("event_schema"),
                        operations: vec![
                            AlterSchemaOperation::AddField {
                                field: SchemaField {
                                    name: named("note"),
                                    ty: ParseAsType::String,
                                    optional: true,
                                    sensitive: false,
                                },
                            },
                            AlterSchemaOperation::DropField {
                                field: named("missing"),
                            },
                        ],
                    }),
                ],
            )
            .expect_err("invalid ALTER should reject the whole batch");

        assert!(matches!(
            error.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            registry
                .get::<CreateSchema>(&domain, named::<ModelName>("new_schema"))
                .expect("read should succeed")
                .is_none()
        );
        let schema = registry
            .get::<CreateSchema>(&domain, named::<ModelName>("event_schema"))
            .expect("read should succeed")
            .expect("schema should exist");
        assert_eq!(schema.fields.len(), 1);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn deduplicator_dependencies_participate_in_candidate_graph_validation() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("my_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "my_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("input", "my_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("input", "value_branch"),
                    ingestor_with_params("ing", "input", "event_codec", "broker_in", &["value"]),
                    processor("p99_proc", "input", "missing_output"),
                ],
            )
            .expect_err("missing deduplicator output relay must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::MissingReference { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn drop_batch_removes_unused_model() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![schema("event_schema"), client_model("broker_in")],
            )
            .expect("partial graph should succeed");
        registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Client,
                    name: ModelName::parse("broker_in").expect("valid identifier"),
                }],
            )
            .expect("drop should succeed");

        assert!(
            registry
                .get_of_kind(
                    &domain,
                    ModelKind::Client,
                    RelayName::parse("broker_in").expect("valid relay name")
                )
                .expect("read should succeed")
                .is_none()
        );
        let graph = registry
            .active_graph(&domain)
            .expect("graph should still exist");
        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 0);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn drop_batch_rejects_delete_when_model_is_in_use() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        let err = registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Schema,
                    name: ModelName::parse("event_schema").expect("valid identifier"),
                }],
            )
            .expect_err("drop should be rejected while schema is in use");

        assert!(matches!(
            err.current_context(),
            RegistryError::DeleteInUse { .. }
        ));
        assert!(
            registry
                .get::<CreateSchema>(
                    &domain,
                    SchemaName::parse("event_schema").expect("valid schema name"),
                )
                .expect("read should succeed")
                .is_some()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn drop_batch_allows_delete_of_emitter() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Emitter,
                    name: ModelName::parse("emit").expect("valid identifier"),
                }],
            )
            .expect("emitter should be droppable");

        assert!(
            registry
                .get::<CreateEmitter>(
                    &domain,
                    EmitterName::parse("emit").expect("valid emitter name"),
                )
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn drop_batch_rejects_delete_of_deduplicator_output_stream() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = DomainName::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("input", "event_schema"),
                    relay_branched_like("output", "event_schema", "input"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("input", "value_branch"),
                    ingestor_with_params("ing", "input", "event_codec", "broker_in", &["value"]),
                    processor("p99_proc", "input", "output"),
                ],
            )
            .expect("deduplicator graph should succeed");

        let err = registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Relay,
                    name: ModelName::parse("output").expect("valid identifier"),
                }],
            )
            .expect_err("deduplicator output relay should be blocked");

        assert!(matches!(
            err.current_context(),
            RegistryError::DeleteInUse { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }
}
