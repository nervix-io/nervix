//! The durable model store and the mutations that change it.
//!
//! Layer: decisions.
//!
//! - **Owns.** Reading and writing Models in the keyspace, planning a mutation batch against the
//!   candidate graph, committing or rolling it back, and the runtime changes a commit implies.
//! - **Depends on.** `fjall` for storage and the domain state for validation.
//! - **Must not know.** How a runtime change is applied.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "durable Model mutation belongs to the control-plane decision boundary"
    )
)]

use std::{path::Path, str::FromStr};

use ahash::{HashMap, HashSet};
use error_stack::{Report, ResultExt};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, Readable as _, Snapshot};
use meticulous::OptionExt;
#[cfg(test)]
use nervix_models::{AlterRelay, DropModel};
use nervix_models::{
    ClusterSchedule, CreateAvroWireSchema, CreateBranch, CreateCodec, CreateDeduplicator,
    CreateEmitter, CreateGenerator, CreateIngestor, CreateJunction, CreatePlacement,
    CreateReingestor, CreateRelay, CreateReorderer, CreateSchema, DomainName,
    EmitterPayloadPresentation, Model, ModelIndex, ModelKind, ModelName, NodeRef,
    RequestedResourceVersion, TransactionModelTransition, UniquelyKindedModel,
};
use nervix_primitives::sync::{
    Arc,
    blocking::{Mutex, RwLock},
};
use nervix_recovery::Discarded;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::registry::{
    domain_state::{DomainState, RegistryState},
    error::RegistryError,
    graph::{ActiveGraph, ensure_drop_targets_are_not_in_use},
    mutation::{
        PlannedMutations, RegistryMutation, RegistryPersistMutation, TransactionMutationPreflight,
        classify_quiesce,
    },
    placement::PlacementPlan,
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
    parameters: nervix_primitives::publication::ArcSwap<nervix_models::ClusterParameterValues>,
    storage: ModelStorage,
    state: RwLock<Arc<RegistryState>>,
    commit_lock: Mutex<()>,
}

/// The execution graph one registry state gives a domain. It is `None` when the domain has nothing
/// left to run, and for a planned batch that changes no Model.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeChanges {
    pub(crate) graph: Option<ActiveGraph>,
}

impl Registry {
    pub(crate) fn install_cluster_parameters(&self, values: nervix_models::ClusterParameterValues) {
        self.parameters
            .store(nervix_primitives::sync::StdArc::new(values));
    }

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
                        error = %format_args!("{err:#}"),
                        "persistent state load failed"
                    );
                } else {
                    warn!(result = "err", error = %format_args!("{err:#}"), "persistent state load failed");
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
            parameters: nervix_primitives::publication::ArcSwap::from_pointee(
                nervix_models::ClusterParameterValues::default(),
            ),
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
            .commit_batch(
                domain,
                &models_to_persist,
                &drops,
                &nervix_models::EntityLabelLimits::from(self.parameters.load().as_ref()),
            )
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

    #[cfg(test)]
    pub(crate) fn plan_mutations(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
    ) -> Result<PlannedMutations, Report<RegistryError>> {
        self.plan_mutations_named(domain, mutations, "mixed mutation batch")
    }

    /// Plans one direct model batch from its caller's captured Models and parameters.
    ///
    /// The snapshot owns both inputs, so runtime policy installation and registry storage cannot
    /// select a different model set or policy between validation and schedule publication.
    pub(crate) fn plan_snapshot_mutations_with_parameters(
        domain: &DomainName,
        base_models: ModelIndex,
        parameters: &nervix_models::ClusterParameterValues,
        mutations: &[RegistryMutation],
    ) -> Result<PlannedMutations, Report<RegistryError>> {
        Ok(Self::plan_mutations_against(
            domain,
            mutations,
            "mixed mutation batch",
            parameters,
            false,
            base_models,
        )?
        .planned
        .verified(
            "this call disallows an incomplete candidate, and only that path returns no plan",
        ))
    }

    /// Plans one transaction model run against the exact Models captured by its caller.
    ///
    /// The owned base keeps the decision independent of registry storage and locks. Transaction
    /// planning can therefore advance several commit steps in written order without rereading a
    /// newer registry state between them.
    pub(crate) fn preflight_transaction_mutations_against(
        domain: &DomainName,
        base_models: ModelIndex,
        parameters: &nervix_models::ClusterParameterValues,
        mutations: &[RegistryMutation],
        allow_incomplete_candidate: bool,
    ) -> Result<TransactionMutationPreflight, Report<RegistryError>> {
        Self::plan_mutations_against(
            domain,
            mutations,
            "ordered transaction planning",
            parameters,
            allow_incomplete_candidate,
            base_models,
        )
    }

    #[cfg(test)]
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

    #[cfg(test)]
    fn plan_mutations_named_with_incomplete_candidate(
        &self,
        domain: &DomainName,
        mutations: &[RegistryMutation],
        operation_name: &str,
        allow_incomplete_candidate: bool,
    ) -> Result<TransactionMutationPreflight, Report<RegistryError>> {
        let current_models = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| record.model)
            .collect::<ModelIndex>();
        Self::plan_mutations_against(
            domain,
            mutations,
            operation_name,
            &self.parameters.load(),
            allow_incomplete_candidate,
            current_models,
        )
    }

    fn plan_mutations_against(
        domain: &DomainName,
        mutations: &[RegistryMutation],
        operation_name: &str,
        parameters: &nervix_models::ClusterParameterValues,
        allow_incomplete_candidate: bool,
        current_models: ModelIndex,
    ) -> Result<TransactionMutationPreflight, Report<RegistryError>> {
        let batch_size = mutations.len();
        info!(
            domain = domain.as_str(),
            batch_size,
            operation = operation_name,
            "planning mutation batch"
        );

        let current_state = DomainState::build(domain, &current_models)?;
        let mut candidate = current_models.clone();

        for (mutation_index, mutation) in mutations.iter().enumerate() {
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
                RegistryMutation::Labels(edit) => {
                    let Some(mut model) = candidate.remove(&edit.entity) else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_owned(),
                            identifier: edit.entity.identifier.as_str().to_owned(),
                        }));
                    };
                    model.labels_mut().apply(&edit.mutation);
                    candidate.insert(model);
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
                    schema.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.schema.as_str(), rejection)
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
                    schema.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.schema.as_str(), rejection)
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
                    schema.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.schema.as_str(), rejection)
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
                    schema.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.schema.as_str(), rejection)
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
                    relay.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.relay.as_str(), rejection)
                    })?;
                }
                RegistryMutation::AlterBranch(alter) => {
                    info!(
                        domain = domain.as_str(),
                        model = alter.branch.as_str(),
                        kind = ModelKind::Branch.as_str(),
                        "staging branch alter from batch"
                    );

                    let Some(branch) =
                        candidate.configured_mut::<CreateBranch>(alter.branch.clone())
                    else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.branch.as_str().to_string(),
                        }));
                    };
                    branch.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.branch.as_str(), rejection)
                    })?;
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
                    junction.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.junction.as_str(), rejection)
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
                    deduplicator.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.deduplicator.as_str(), rejection)
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
                    reorderer.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.reorderer.as_str(), rejection)
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
                    emitter.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.emitter.as_str(), rejection)
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
                    ingestor.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.ingestor.as_str(), rejection)
                    })?;
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
                    reingestor.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.reingestor.as_str(), rejection)
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
                    generator.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.generator.as_str(), rejection)
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
                    altered.apply_alter(alter).map_err(|rejection| {
                        RegistryError::invalid_model(domain, alter.placement.as_str(), rejection)
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
                        let candidate_state = DomainState::build(domain, &candidate)?;
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
        }

        let drops_in_batch = current_models
            .nodes()
            .filter(|key| !candidate.contains(key))
            .cloned()
            .collect::<HashSet<_>>();

        super::validate_model_parameters(domain, candidate.models(), parameters)?;
        let domain_state = match DomainState::build(domain, &candidate) {
            Ok(state) => state,
            // A caller that allows an incomplete candidate is queuing a step of a transaction that
            // later steps complete, so a candidate that does not yet build has no plan to offer
            // and no failure to report. The batch is validated once, on its final models, where a
            // rejection can name the statement that caused it.
            Err(error) if allow_incomplete_candidate => {
                return Ok(TransactionMutationPreflight {
                    planned: None,
                    candidate_models: candidate,
                    incomplete_reason: Some(error.to_string()),
                });
            }
            Err(err) => {
                warn!(
                    domain = domain.as_str(),
                    batch_size,
                    operation = operation_name,
                    result = "err",
                    error = %format_args!("{err:#}"),
                    "failed to apply mutation batch\n{}",
                    current_state.graph.describe()
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
        let quiesce = classify_quiesce(
            &current_models,
            &candidate,
            &current_state.graph,
            &domain_state.graph,
        );
        let is_noop = models_to_persist.is_empty() && drops_in_batch.is_empty();
        let graph = if is_noop || domain_state.graph.node_count() == 0 {
            None
        } else {
            Some(domain_state.graph.clone())
        };
        let runtime_changes = RuntimeChanges { graph };

        let candidate_models = domain_state.models.clone();
        Ok(TransactionMutationPreflight {
            planned: Some(PlannedMutations {
                domain: domain.clone(),
                batch_size,
                operation_name: operation_name.to_string(),
                base_models: current_models,
                base_graph: current_state.graph.clone(),
                domain_state,
                models_to_persist,
                drops_in_batch,
                runtime_changes,
                quiesce,
            }),
            candidate_models,
            incomplete_reason: None,
        })
    }

    /// Restores the exact Model decision stored at commit admission against its captured base.
    pub(crate) fn restore_transaction_model_plan(
        domain: &DomainName,
        current_models: ModelIndex,
        transitions: &[TransactionModelTransition],
        operation_count: usize,
    ) -> Result<PlannedMutations, Report<RegistryError>> {
        let current_state = DomainState::build(domain, &current_models)?;
        let mut candidate = current_models.clone();
        for transition in transitions {
            match transition {
                TransactionModelTransition::Create { model } => {
                    let key = model.node_ref();
                    if candidate.contains(&key) {
                        return Err(Report::new(RegistryError::ConcurrentMutation {
                            domain: domain.as_str().to_string(),
                        }));
                    }
                    candidate.insert(model.as_ref().clone());
                }
                TransactionModelTransition::Replace { before, after } => {
                    let key = before.node_ref();
                    if after.node_ref() != key {
                        return Err(Report::new(RegistryError::InvalidTransactionPlan {
                            domain: domain.as_str().to_string(),
                        }));
                    }
                    if candidate.get(&key) != Some(before.as_ref()) {
                        return Err(Report::new(RegistryError::ConcurrentMutation {
                            domain: domain.as_str().to_string(),
                        }));
                    }
                    candidate.insert(after.as_ref().clone());
                }
                TransactionModelTransition::Drop { model } => {
                    let key = model.node_ref();
                    if candidate.get(&key) != Some(model.as_ref()) {
                        return Err(Report::new(RegistryError::ConcurrentMutation {
                            domain: domain.as_str().to_string(),
                        }));
                    }
                    candidate.remove(&key).verified(
                        "the admitted drop matched the Model in this same candidate index",
                    );
                }
            }
        }

        let domain_state = DomainState::build(domain, &candidate)?;
        let drops_in_batch = current_models
            .nodes()
            .filter(|key| !candidate.contains(key))
            .cloned()
            .collect::<HashSet<_>>();
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
        let quiesce = classify_quiesce(
            &current_models,
            &candidate,
            &current_state.graph,
            &domain_state.graph,
        );
        let is_noop = models_to_persist.is_empty() && drops_in_batch.is_empty();
        let graph = if is_noop || domain_state.graph.node_count() == 0 {
            None
        } else {
            Some(domain_state.graph.clone())
        };
        let runtime_changes = RuntimeChanges { graph };
        Ok(PlannedMutations {
            domain: domain.clone(),
            batch_size: operation_count,
            operation_name: "admitted transaction model step".to_string(),
            base_models: current_models,
            base_graph: current_state.graph.clone(),
            domain_state,
            models_to_persist,
            drops_in_batch,
            runtime_changes,
            quiesce,
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
                &nervix_models::EntityLabelLimits::from(self.parameters.load().as_ref()),
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
            .commit_batch(
                &planned.domain,
                &models_to_persist,
                &drops,
                &nervix_models::EntityLabelLimits::from(self.parameters.load().as_ref()),
            )
            .change_context(RegistryError::PersistBatch)?;

        let base_state = self.build_domain_state(&planned.domain, &planned.base_models)?;
        let graph = if base_state.graph.node_count() == 0 {
            None
        } else {
            Some(base_state.graph.clone())
        };
        let runtime_changes = RuntimeChanges { graph };
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

    /// The models of `domain` as `queued` leaves them, applied in written order and without
    /// validating the result. An alteration whose target is gone is skipped, because this describes
    /// configuration a client is still writing rather than a plan that will be persisted. Queued
    /// statements keep the resource versions they wrote, so stored models are written the same way.
    pub(crate) fn resulting_models(
        &self,
        domain: &DomainName,
        queued: &[nervix_models::Statement],
    ) -> Result<Vec<Model<RequestedResourceVersion>>, Report<RegistryError>> {
        let mut models = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?
            .into_iter()
            .map(|record| Model::<RequestedResourceVersion>::from(record.model))
            .collect::<ModelIndex<RequestedResourceVersion>>();
        for statement in queued {
            RegistryMutation::fold_statement(statement, &mut models);
        }
        Ok(models.into_iter().map(|(_, model)| model).collect())
    }

    pub(crate) fn active_graph(&self, domain: &DomainName) -> Option<ActiveGraph> {
        let state = self.state.read();
        state.domains.get(domain).map(|ns| ns.graph.clone())
    }

    /// Captures the complete model input for a pure planning decision while holding the registry
    /// read lock only for the clone. Empty domains have no registry entry and therefore produce an
    /// empty Model index.
    pub(crate) fn transaction_planning_models(&self, domain: &DomainName) -> ModelIndex {
        let state = self.state.read();
        match state.domains.get(domain) {
            Some(domain_state) => domain_state.models.clone(),
            None => ModelIndex::default(),
        }
    }

    /// Reads an emitter and its effective payload from one short-lived storage snapshot.
    /// Both reads retain storage/decode failures, without cloning the domain's model index.
    pub(crate) fn emitter_configuration(
        &self,
        domain: &DomainName,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<(CreateEmitter, EmitterPayloadPresentation)>, Report<RegistryError>> {
        let identifier = identifier.into();
        let snapshot = self.storage.db.snapshot();
        let Some(emitter) = self
            .storage
            .get_at::<CreateEmitter>(&snapshot, domain, &identifier)
            .change_context(RegistryError::LoadStoredModels)?
        else {
            return Ok(None);
        };
        let codec = match emitter.body.codec() {
            Some(name) => Some(
                self.storage
                    .get_at::<CreateCodec>(&snapshot, domain, &ModelName::from(name))
                    .change_context(RegistryError::LoadStoredModels)?
                    .ok_or_else(|| {
                        Report::new(RegistryError::MissingReference {
                            domain: domain.as_str().to_owned(),
                            identifier: identifier.as_str().to_owned(),
                            expected_kind: ModelKind::Codec.as_str(),
                            reference: name.as_str().to_owned(),
                        })
                    })?,
            ),
            None => None,
        };
        let payload = emitter
            .payload_presentation(|_| codec.as_ref())
            .assured("the snapshot read resolved the codec the emitter names");
        Ok(Some((emitter, payload)))
    }

    pub(crate) fn placement_plan(&self, domain: &DomainName) -> Option<PlacementPlan> {
        let state = self.state.read();
        state
            .domains
            .get(domain)
            .map(|domain_state| domain_state.graph.placement_plan())
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
        super::validate_model_parameters(domain, models.models(), &self.parameters.load())?;
        DomainState::build(domain, models)
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
    /// Typed reads sharing one MVCC view, for inspection that resolves another Model.
    fn get_at<M: UniquelyKindedModel>(
        &self,
        snapshot: &Snapshot,
        domain: &DomainName,
        identifier: &ModelName,
    ) -> Result<Option<M>, Report<RegistryError>> {
        let key = encode_key(domain, M::KIND, identifier)?;
        let Some(raw) = snapshot
            .get(&self.index, key)
            .change_context(RegistryError::ReadValue)?
        else {
            return Ok(None);
        };
        let model = deserialize_value(raw.as_ref())?;
        let stored_kind = model.kind();
        let model = M::from_model(model).ok_or_else(|| {
            Report::new(RegistryError::StoredModelKindMismatch {
                domain: domain.as_str().to_owned(),
                identifier: identifier.as_str().to_owned(),
                expected_kind: M::KIND.as_str(),
                stored_kind: stored_kind.as_str(),
            })
        })?;
        Ok(Some(model))
    }

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

        let value = serialize_value(
            model,
            &nervix_models::EntityLabelLimits::from(
                &nervix_models::ClusterParameterValues::default(),
            ),
        )?;

        self.index
            .insert(key, value)
            .change_context(RegistryError::WriteValue)
    }

    fn commit_batch(
        &self,
        domain: &DomainName,
        models_to_persist: &HashMap<NodeRef, RegistryPersistMutation>,
        drops_in_batch: &HashSet<NodeRef>,
        label_limits: &nervix_models::EntityLabelLimits,
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
                    serialize_value(model, label_limits)?,
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

            let StoredModelKey {
                domain,
                kind,
                identifier,
            } = decode_key(&raw_key)?;

            let model = deserialize_value(raw_value.as_ref())?;

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

/// The domain, kind and name one stored key holds.
struct StoredModelKey {
    domain: DomainName,
    kind: ModelKind,
    identifier: ModelName,
}

/// The domain, kind and name a stored key holds. Only the exact encoding a commit writes holds
/// them: a name spelled other than canonically, or bytes after the key, would let a second stored
/// record claim the same Model.
fn decode_key(raw: &[u8]) -> Result<StoredModelKey, Report<RegistryError>> {
    let key: ModelKeyOwned = storekey::deserialize(raw).change_context(RegistryError::DecodeKey)?;
    let domain = DomainName::decode(&key.domain).change_context(RegistryError::DecodeKey)?;
    let kind = ModelKind::from_str(&key.kind).map_err(|_| Report::new(RegistryError::DecodeKey))?;
    let identifier = ModelName::decode(&key.identifier).change_context(RegistryError::DecodeKey)?;
    if encode_key(&domain, kind, identifier.clone())? != raw {
        return Err(Report::new(RegistryError::DecodeKey));
    }
    Ok(StoredModelKey {
        domain,
        kind,
        identifier,
    })
}

fn serialize_value(
    model: &Model,
    limits: &nervix_models::EntityLabelLimits,
) -> Result<Vec<u8>, Report<RegistryError>> {
    model
        .labels()
        .validate(limits)
        .change_context(RegistryError::SerializeValue)?;
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(model)
        .change_context(RegistryError::SerializeValue)?;
    let capacity = MODEL_ARCHIVE_PREFIX_BYTES
        .checked_add(archive.len())
        .assured("a Vec archive is bounded by isize::MAX, and its 16-byte header fits in usize");
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(MODEL_ARCHIVE_HEADER);
    for value in [
        limits.count,
        limits.key_bytes,
        limits.value_bytes,
        limits.bytes,
    ] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    payload.extend_from_slice(&archive);
    Ok(payload)
}

fn deserialize_value(bytes: &[u8]) -> Result<Model, Report<RegistryError>> {
    let archive = bytes
        .strip_prefix(MODEL_ARCHIVE_HEADER)
        .ok_or_else(|| Report::new(RegistryError::InvalidModelArchive))?;
    let (encoded_limits, archive) = archive
        .split_at_checked(MODEL_LABEL_LIMIT_BYTES)
        .ok_or_else(|| Report::new(RegistryError::InvalidModelArchive))?;
    let mut values = encoded_limits.as_chunks::<{ size_of::<u64>() }>().0.iter();
    let mut next = || {
        u64::from_le_bytes(
            *values
                .next()
                .assured("the fixed policy prefix holds one word per declared limit"),
        )
    };
    let limits = nervix_models::EntityLabelLimits {
        count: next(),
        key_bytes: next(),
        value_bytes: next(),
        bytes: next(),
    };
    // A value read back from an on-disk table is a slice of its block at whatever offset the block
    // placed it, so the archive is copied to the alignment its root requires before validation.
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(archive.len());
    aligned.extend_from_slice(archive);
    let model = rkyv::from_bytes::<Model, rkyv::rancor::Error>(&aligned)
        .change_context(RegistryError::DeserializeValue)?;
    model
        .labels()
        .validate(&limits)
        .change_context(RegistryError::DeserializeValue)?;
    Ok(model)
}

// The header identifies the current persisted Model shape before archive decoding.
const MODEL_ARCHIVE_HEADER: &[u8; 16] = b"NERVIX LABELS   ";
const MODEL_LABEL_LIMIT_BYTES: usize = 4 * size_of::<u64>();
const MODEL_ARCHIVE_PREFIX_BYTES: usize = MODEL_ARCHIVE_HEADER.len() + MODEL_LABEL_LIMIT_BYTES;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod properties;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
