mod stored;

use std::{
    cmp::Reverse,
    num::NonZeroUsize,
    path::Path,
    str::FromStr,
    sync::{Arc as StdArc, RwLock},
    time::Duration,
};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit,
};
use error_stack::{Report, ResultExt};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use nervix_dataflow_graph::{
    DataflowEdge, DataflowEdgeKind, DataflowGraph, DataflowMetricRef, DataflowNode,
    DataflowNodeKind, DataflowSchemaField,
};
use nervix_models::{
    AlterRelay, AlterRelayOperation, Assignment, AssignmentTarget, AvroType, BranchSelection,
    CodecEncoding, CodecEncodingRule, CodecWireFormat, CorrelationTimeoutAction, CreateBranch,
    CreateCodec, CreateCorrelator, CreateDeduplicator, CreateGenerator, CreateInferencer,
    CreateIngestor, CreateLookup, CreateMaterializer, CreateSchema, CreateSignalingProtocol,
    CreateWindowProcessor, CreateWireSchemaStmt, Domain, DomainSchedule, DropModel, EmitSink,
    EndpointType, Expression, Identifier, IngestSource, IngestTimestampSource, JsonType,
    MaterializedStateDependency, MaterializedStatePolicy, MessageErrorPolicy, Model, ModelKind,
    OutputBranch, ParseAsType, ProcessorOutput, ProcessorOutputs, RouteConstruction, ScheduledNode,
    SchemaField,
};
use nervix_nspl::{
    vm_program::{
        Expr, FunctionName, InternalFieldNamespace, InternalFieldRef, Literal, Program,
        SemanticNamespaces, SpannedExpr, lower_branch_construction, lower_finalized_output_filter,
        lower_generated_route, lower_route_construction, lower_set_only_route,
        lower_transforming_route,
    },
    window_processor::aggregate::{lower_window_assignments, referenced_field_refs},
};
use nervix_vm::{
    CompileBinding, CompileOptions, OutputMode, SchemaSensitivity,
    compile_program_for_bindings_with_sensitivity,
    compile_program_with_options_for_bindings_with_sensitivity, infer_set_expr_types_for_bindings,
};
use petgraph::{
    Direction, algo::is_cyclic_directed, graph::DiGraph, prelude::NodeIndex, visit::EdgeRef,
};
use serde::{Deserialize, Serialize};
use sorted_vec::SortedSet;
pub use stored::StoredModelVersioned;
use thiserror::Error;
use tracing::{info, warn};
use triomphe::Arc;

const BRANCH_NAMESPACE: &str = "branch";
const INGEST_MESSAGE_NAMESPACE: &str = "message";
const INNER_OUTPUT_NAMESPACE: &str = "inner_output";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RegistryError {
    #[error("failed to open registry storage")]
    OpenStorage,
    #[error("failed to open database")]
    OpenDatabase,
    #[error("failed to open keyspace")]
    OpenKeyspace,
    #[error("failed to load stored models")]
    LoadStoredModels,
    #[error("failed to encode key")]
    EncodeKey,
    #[error("failed to serialize model")]
    SerializeValue,
    #[error("failed to write model")]
    WriteValue,
    #[error("failed to read model")]
    ReadValue,
    #[error("failed to deserialize model")]
    DeserializeValue,
    #[error("failed to convert stored model")]
    ModelConversion,
    #[error("failed to iterate values")]
    IterateValues,
    #[error("failed to decode key")]
    DecodeKey,
    #[error("failed to persist model batch")]
    PersistBatch,
    #[error("model '{identifier}' already exists in domain '{domain}'")]
    AlreadyExists { domain: String, identifier: String },
    #[error("batch contains duplicate model '{identifier}' in domain '{domain}'")]
    DuplicateInBatch { domain: String, identifier: String },
    #[error("model '{identifier}' does not exist in domain '{domain}'")]
    NotFound { domain: String, identifier: String },
    #[error(
        "model '{identifier}' in domain '{domain}' expected kind {expected_kind}, found \
         {actual_kind}"
    )]
    InvalidModelKind {
        domain: String,
        identifier: String,
        expected_kind: &'static str,
        actual_kind: &'static str,
    },
    #[error(
        "model '{identifier}' in domain '{domain}' requires missing {expected_kind} '{reference}'"
    )]
    MissingReference {
        domain: String,
        identifier: String,
        expected_kind: &'static str,
        reference: String,
    },
    #[error(
        "model '{identifier}' in domain '{domain}' expected {expected_kind} '{reference}', found \
         {actual_kind}"
    )]
    InvalidReferenceKind {
        domain: String,
        identifier: String,
        expected_kind: &'static str,
        reference: String,
        actual_kind: &'static str,
    },
    #[error("active configuration graph for domain '{domain}' contains a cycle")]
    ConfigurationCycle { domain: String },
    #[error(
        "model '{identifier}' in domain '{domain}' has incompatible schema relationship: {reason}"
    )]
    IncompatibleSchema {
        domain: String,
        identifier: String,
        reason: String,
    },
    #[error("model '{identifier}' in domain '{domain}' is invalid: {reason}")]
    InvalidModel {
        domain: String,
        identifier: String,
        reason: String,
    },
    #[error(
        "cannot delete model '{identifier}' in domain '{domain}' because it is used by {blockers}"
    )]
    DeleteInUse {
        domain: String,
        identifier: String,
        blockers: String,
    },
    #[error("failed to update in-memory registry state")]
    UpdateState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredModelRecord {
    domain: Domain,
    key: RegistryKey,
    model: Model,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RegistryKey {
    kind: ModelKind,
    identifier: Identifier,
}

impl RegistryKey {
    fn new(kind: ModelKind, identifier: Identifier) -> Self {
        Self { kind, identifier }
    }

    fn from_model(model: &Model) -> Self {
        Self::new(model.kind(), model.identifier().clone())
    }
}

pub struct Registry {
    storage: ModelStorage,
    state: RwLock<Arc<RegistryState>>,
}

#[derive(Debug, Clone)]
pub struct RuntimeChanges {
    pub domain: Domain,
    pub graph: Option<ActiveGraph>,
    pub changes: Vec<RuntimeChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryEntity {
    pub kind: ModelKind,
    pub identifier: Identifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeChange {
    StartIngestor {
        source_model: Box<Model>,
        ingestor: Box<CreateIngestor>,
    },
    StopIngestor {
        ingestor: Identifier,
    },
    SetRelayCapacity {
        relay: Identifier,
        capacity: NonZeroUsize,
    },
}

impl Registry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Report<RegistryError>> {
        let path = path.as_ref();
        let db = Database::builder(path)
            .open()
            .change_context(RegistryError::OpenDatabase)?;
        Self::from_database(db, Some(path))
    }

    pub fn from_database(db: Database, path: Option<&Path>) -> Result<Self, Report<RegistryError>> {
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
            info!(
                domain = record.domain.as_str(),
                model = record.key.identifier.as_str(),
                kind = record.key.kind.as_str(),
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
        })
    }

    pub fn apply_batch(
        &self,
        domain: &Domain,
        models: Vec<Model>,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(
            domain,
            models
                .into_iter()
                .map(|model| RegistryMutation::Create(Box::new(model)))
                .collect(),
            "model batch",
        )
    }

    pub fn drop_batch(
        &self,
        domain: &Domain,
        drops: Vec<DropModel>,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(
            domain,
            drops.into_iter().map(RegistryMutation::Drop).collect(),
            "drop batch",
        )
    }

    pub fn alter_relay(
        &self,
        domain: &Domain,
        alter: AlterRelay,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        self.apply_mutations(
            domain,
            vec![RegistryMutation::AlterRelay(alter)],
            "relay alter",
        )
    }

    pub fn startup_runtime_changes(&self) -> Result<Vec<RuntimeChanges>, Report<RegistryError>> {
        let state = self
            .state
            .read()
            .map_err(|_| Report::new(RegistryError::UpdateState))?;
        let domains = SortedSet::from_unsorted(state.domains.keys().cloned().collect()).into_vec();

        Ok(domains
            .into_iter()
            .filter_map(|domain| {
                let domain_state = state.domains.get(&domain)?;
                let changes = runtime_changes_for_domain(
                    &domain,
                    Some(domain_state.graph.clone()),
                    &HashMap::new(),
                    &domain_state.models,
                );
                (changes.graph.is_some() || !changes.changes.is_empty()).then_some(changes)
            })
            .collect())
    }

    fn apply_mutations(
        &self,
        domain: &Domain,
        mutations: Vec<RegistryMutation>,
        operation_name: &str,
    ) -> Result<RuntimeChanges, Report<RegistryError>> {
        let batch_size = mutations.len();
        info!(
            domain = domain.as_str(),
            batch_size,
            operation = operation_name,
            "applying mutation batch"
        );

        let existing = self
            .storage
            .list_models(domain)
            .change_context(RegistryError::LoadStoredModels)?;

        let current_models = existing
            .iter()
            .map(|record| (record.key.clone(), record.model.clone()))
            .collect::<HashMap<_, _>>();
        let current_state = self.build_domain_state(domain, &current_models)?;
        let mut candidate = existing
            .iter()
            .map(|record| (record.key.clone(), record.model.clone()))
            .collect::<HashMap<_, _>>();

        let mut models_to_persist = HashMap::<RegistryKey, RegistryPersistMutation>::new();
        let mut drops_in_batch = HashSet::<RegistryKey>::new();
        let mut targeted_runtime_changes = Vec::new();
        let mut targeted_runtime_changes_only = true;
        for mutation in mutations {
            match mutation {
                RegistryMutation::Create(model) => {
                    targeted_runtime_changes_only = false;
                    let identifier = model.identifier().clone();
                    let key = RegistryKey::from_model(&model);

                    info!(
                        domain = domain.as_str(),
                        model = identifier.as_str(),
                        kind = model.kind().as_str(),
                        "staging model create from batch"
                    );

                    if candidate.contains_key(&key) {
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

                    if models_to_persist
                        .insert(
                            key.clone(),
                            RegistryPersistMutation::Create((*model).clone()),
                        )
                        .is_some()
                    {
                        warn!(
                            domain = domain.as_str(),
                            model = identifier.as_str(),
                            kind = model.kind().as_str(),
                            "rejecting batch because model is duplicated in batch"
                        );
                        return Err(Report::new(RegistryError::DuplicateInBatch {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                        }));
                    }

                    candidate.insert(key, *model);
                }
                RegistryMutation::AlterRelay(alter) => {
                    let key = RegistryKey::new(ModelKind::Relay, alter.relay.clone());
                    info!(
                        domain = domain.as_str(),
                        model = alter.relay.as_str(),
                        kind = ModelKind::Relay.as_str(),
                        "staging relay alter from batch"
                    );

                    let Some(model) = candidate.get_mut(&key) else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: alter.relay.as_str().to_string(),
                        }));
                    };

                    let Model::Relay(relay) = model else {
                        return Err(Report::new(RegistryError::InvalidModelKind {
                            domain: domain.as_str().to_string(),
                            identifier: alter.relay.as_str().to_string(),
                            expected_kind: ModelKind::Relay.as_str(),
                            actual_kind: model.kind().as_str(),
                        }));
                    };
                    match &alter.operation {
                        AlterRelayOperation::SetCapacity { capacity } => {
                            let Some(nonzero_capacity) = NonZeroUsize::new(*capacity) else {
                                return Err(Report::new(RegistryError::InvalidModel {
                                    domain: domain.as_str().to_string(),
                                    identifier: alter.relay.as_str().to_string(),
                                    reason: "relay capacity must be greater than 0".to_string(),
                                }));
                            };
                            targeted_runtime_changes.push(RuntimeChange::SetRelayCapacity {
                                relay: alter.relay.clone(),
                                capacity: nonzero_capacity,
                            });
                        }
                    }
                    relay.apply_alter(&alter.operation);

                    if models_to_persist
                        .insert(key.clone(), RegistryPersistMutation::Replace(model.clone()))
                        .is_some()
                    {
                        return Err(Report::new(RegistryError::DuplicateInBatch {
                            domain: domain.as_str().to_string(),
                            identifier: alter.relay.as_str().to_string(),
                        }));
                    }
                }
                RegistryMutation::Drop(drop) => {
                    targeted_runtime_changes_only = false;
                    let key = RegistryKey::new(drop.kind, drop.name.clone());
                    info!(
                        domain = domain.as_str(),
                        model = drop.name.as_str(),
                        kind = drop.kind.as_str(),
                        "staging model drop from batch"
                    );

                    let Some(_existing_model) = candidate.get(&key) else {
                        return Err(Report::new(RegistryError::NotFound {
                            domain: domain.as_str().to_string(),
                            identifier: drop.name.as_str().to_string(),
                        }));
                    };

                    if !drops_in_batch.insert(key) {
                        return Err(Report::new(RegistryError::DuplicateInBatch {
                            domain: domain.as_str().to_string(),
                            identifier: drop.name.as_str().to_string(),
                        }));
                    }
                }
            }
        }

        ensure_drop_targets_are_not_in_use(domain, &current_state.graph, &drops_in_batch)?;

        for key in &drops_in_batch {
            candidate.remove(key);
        }

        let domain_state = match self.build_domain_state(domain, &candidate) {
            Ok(state) => state,
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
        let runtime_changes = if targeted_runtime_changes_only {
            RuntimeChanges {
                domain: domain.clone(),
                graph: (domain_state.graph.node_count() > 0).then_some(domain_state.graph.clone()),
                changes: targeted_runtime_changes,
            }
        } else {
            runtime_changes_for_domain(
                domain,
                (domain_state.graph.node_count() > 0).then_some(domain_state.graph.clone()),
                &current_state.models,
                &domain_state.models,
            )
        };

        for (key, mutation) in &models_to_persist {
            match mutation {
                RegistryPersistMutation::Create(model) => {
                    self.storage
                        .put(domain, key.kind, &key.identifier, model)
                        .change_context(RegistryError::PersistBatch)?;
                }
                RegistryPersistMutation::Replace(model) => {
                    self.storage
                        .replace(domain, key.kind, &key.identifier, model)
                        .change_context(RegistryError::PersistBatch)?;
                }
            }
        }
        for key in &drops_in_batch {
            self.storage
                .delete(domain, key.kind, &key.identifier)
                .change_context(RegistryError::PersistBatch)?;
        }

        let current = self
            .state
            .read()
            .map_err(|_| Report::new(RegistryError::UpdateState))?;
        let mut domains = current.domains.clone();
        if domain_state.graph.node_count() == 0 {
            domains.remove(domain);
        } else {
            domains.insert(domain.clone(), domain_state);
        }
        drop(current);

        let mut writer = self
            .state
            .write()
            .map_err(|_| Report::new(RegistryError::UpdateState))?;
        *writer = Arc::new(RegistryState { domains });

        let graph_snapshot = writer
            .domains
            .get(domain)
            .map(|state| state.graph.describe())
            .unwrap_or_default();

        info!(
            domain = domain.as_str(),
            batch_size,
            operation = operation_name,
            result = "ok",
            node_count = writer
                .domains
                .get(domain)
                .map(|state| state.graph.node_count())
                .unwrap_or(0),
            edge_count = writer
                .domains
                .get(domain)
                .map(|state| state.graph.edge_count())
                .unwrap_or(0),
            "applied mutation batch\n{}",
            graph_snapshot
        );

        Ok(runtime_changes)
    }

    pub fn get(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<Option<Model>, Report<RegistryError>> {
        self.storage
            .get(domain, kind, identifier)
            .change_context(RegistryError::LoadStoredModels)
    }

    pub fn list_identifiers(
        &self,
        domain: &Domain,
        kind: ModelKind,
        prefix: &str,
    ) -> Result<Vec<Identifier>, Report<RegistryError>> {
        self.storage
            .list_identifiers(domain, kind, prefix)
            .change_context(RegistryError::LoadStoredModels)
    }

    pub fn active_graph(&self, domain: &Domain) -> Option<ActiveGraph> {
        let state = self.state.read().ok()?;
        state.domains.get(domain).map(|ns| ns.graph.clone())
    }

    pub fn active_graphs(&self) -> Vec<(Domain, ActiveGraph)> {
        let Some(state) = self.state.read().ok() else {
            return Vec::new();
        };
        let mut graphs = state
            .domains
            .iter()
            .map(|(domain, domain_state)| (domain.clone(), domain_state.graph.clone()))
            .collect::<Vec<_>>();
        graphs.sort_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));
        graphs
    }

    pub fn active_domain_entities(&self, domain: &Domain) -> Vec<RegistryEntity> {
        let Some(state) = self.state.read().ok() else {
            return Vec::new();
        };
        let Some(domain_state) = state.domains.get(domain) else {
            return Vec::new();
        };
        let mut entities = domain_state
            .graph
            .nodes()
            .into_iter()
            .filter(|node| !node.is_dataflow_node())
            .map(|node| RegistryEntity {
                kind: node.kind,
                identifier: node.identifier,
            })
            .collect::<Vec<_>>();
        entities.sort_by(|left, right| {
            left.kind
                .as_str()
                .cmp(right.kind.as_str())
                .then_with(|| left.identifier.as_str().cmp(right.identifier.as_str()))
        });
        entities
    }

    fn build_domain_state(
        &self,
        domain: &Domain,
        models: &HashMap<RegistryKey, Model>,
    ) -> Result<DomainState, Report<RegistryError>> {
        DomainState::build(domain, models)
    }

    fn active_graph_snapshot(&self, domain: &Domain) -> String {
        self.active_graph(domain)
            .map(|graph| graph.describe())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
enum RegistryMutation {
    Create(Box<Model>),
    AlterRelay(AlterRelay),
    Drop(DropModel),
}

#[derive(Debug, Clone)]
enum RegistryPersistMutation {
    Create(Model),
    Replace(Model),
}

#[derive(Debug, Clone)]
struct RegistryState {
    domains: HashMap<Domain, DomainState>,
}

impl RegistryState {
    fn from_records(records: Vec<StoredModelRecord>) -> Result<Self, Report<RegistryError>> {
        let mut grouped = HashMap::<Domain, HashMap<RegistryKey, Model>>::new();

        for record in records {
            grouped
                .entry(record.domain)
                .or_default()
                .insert(record.key, record.model);
        }

        let mut domains = HashMap::new();
        for (domain, models) in grouped {
            let state = DomainState::build(&domain, &models)?;
            domains.insert(domain, state);
        }

        Ok(Self { domains })
    }
}

#[derive(Debug, Clone)]
struct DomainState {
    models: HashMap<RegistryKey, Model>,
    graph: ActiveGraph,
}

impl DomainState {
    fn build(
        domain: &Domain,
        models: &HashMap<RegistryKey, Model>,
    ) -> Result<Self, Report<RegistryError>> {
        let mut graph = DiGraph::<ActiveNode, EdgeKind>::new();
        let mut indices = HashMap::new();

        for (key, model) in models {
            let (effective_branching, effective_branching_schema) = match model {
                Model::Relay(relay) => {
                    if let Some(branch_ref) = relay.branching.branch() {
                        let branch = branch_model(domain, &key.identifier, models, branch_ref)?;
                        (
                            Some(branching_schema_fields(
                                domain,
                                &key.identifier,
                                models,
                                &branch.schema,
                            )?),
                            Some(branch.schema.clone()),
                        )
                    } else {
                        (Some(Vec::new()), None)
                    }
                }
                _ => {
                    if let Some(branched_by) = model_branch_selection(model) {
                        let branching = resolved_branch_selection(
                            domain,
                            &key.identifier,
                            models,
                            branched_by,
                        )?;
                        (Some(branching.fields), branching.schema)
                    } else {
                        (None, None)
                    }
                }
            };
            let node = ActiveNode {
                identifier: key.identifier.clone(),
                kind: key.kind,
                config: Arc::new(model.clone()),
                effective_branching,
                effective_branching_schema,
            };
            let index = graph.add_node(node);
            indices.insert(key.clone(), index);
        }

        for (key, model) in models {
            let identifier = &key.identifier;
            let source = *indices
                .get(key)
                .expect("graph node must exist for every model");

            if let Some(branched_by) = model_branch_selection(model) {
                if let Some(branch_ref) = branched_by.branch_ref() {
                    let branch = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        branch_ref,
                        ModelKind::Branch,
                    )?;
                    graph.add_edge(branch, source, EdgeKind::RequiredBy);
                }
            }
            match model {
                Model::Ingestor(ingestor) => add_output_branch_dependency_edges(
                    domain,
                    identifier,
                    models,
                    &indices,
                    &mut graph,
                    source,
                    &ingestor.output_routes,
                )?,
                Model::Reingestor(reingestor) => add_output_branch_dependency_edges(
                    domain,
                    identifier,
                    models,
                    &indices,
                    &mut graph,
                    source,
                    &reingestor.output_routes,
                )?,
                _ => {}
            }

            let materialized_state = model_materialized_state_dependencies(model);
            add_materialized_state_dependency_edges(
                domain,
                identifier,
                models,
                &indices,
                &mut graph,
                source,
                materialized_state,
            )?;
            validate_declared_materialized_state_references(
                domain,
                identifier,
                model,
                materialized_state,
            )?;

            match model {
                Model::Schema(schema) => {
                    ensure_schema_has_fields(domain, identifier, &schema.fields, "schema")?;
                }
                Model::Branch(branch) => {
                    let branch_schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &branch.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(branch_schema, source, EdgeKind::RequiredBy);
                    validate_branch_model(domain, identifier, models, branch)?;
                }
                Model::WireSchema(schema) => {
                    ensure_wire_schema_has_fields(domain, identifier, schema)?;
                }
                Model::ClientKafka(_)
                | Model::ClientPulsar(_)
                | Model::ClientKinesis(_)
                | Model::ClientHttp(_)
                | Model::ClientPrometheus(_)
                | Model::ClientRabbitMq(_)
                | Model::ClientRedis(_)
                | Model::ClientMqtt(_)
                | Model::ClientNats(_)
                | Model::ClientZeroMq(_)
                | Model::ClientSqs(_)
                | Model::ClientClickHouse(_)
                | Model::ClientPostgres(_)
                | Model::ClientMySql(_)
                | Model::ClientMongoDb(_)
                | Model::ClientS3(_)
                | Model::ClientGcs(_)
                | Model::ClientAzureBlob(_)
                | Model::ClientIcebergRest(_)
                | Model::Vhost(_) => {}
                Model::ClientWebsockets(client) => {
                    if let Some(signaling_protocol) = client.signaling_protocol.as_ref() {
                        let signaling_protocol = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            signaling_protocol,
                            ModelKind::SignalingProtocol,
                        )?;
                        graph.add_edge(signaling_protocol, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Materializer(_) => {}
                Model::SignalingProtocol(protocol) => {
                    ensure_signaling_protocol_is_valid(domain, identifier, protocol)?;
                }
                Model::Generator(generator) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &generator.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &generator.output_routes,
                    )?;
                    let input = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &generator.materialized_relay,
                        ModelKind::Relay,
                    )?;
                    graph.add_edge(input, source, EdgeKind::RequiredBy);
                    graph.add_edge(input, source, EdgeKind::SendsTo);
                    ensure_stream_is_materialized(
                        domain,
                        identifier,
                        models,
                        &generator.materialized_relay,
                    )?;
                    for output in generator.output_routes.outputs() {
                        validate_generator_output(domain, identifier, models, generator, output)?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &generator.output_routes,
                    )?;
                }
                Model::Inferencer(processor) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &processor.output_routes,
                    )?;
                    processor.execution_mode().map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: error.to_string(),
                        })
                    })?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.from,
                        "inferencer input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &processor.from,
                        "inferencer input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        processor.filter_where.as_ref(),
                    )?;
                    ensure_inferencer_input_mappings(
                        domain,
                        identifier,
                        processor,
                        &input_schemas,
                    )?;
                    for output in processor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        validate_inferencer_output_filter_map(
                            domain,
                            identifier,
                            models,
                            output,
                            consumer_schema,
                            branch_schema,
                            processor,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                }
                Model::WasmProcessor(processor) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.from,
                        "wasm processor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &processor.from,
                        "wasm processor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        processor.filter_where.as_ref(),
                    )?;
                    ensure_wasm_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        processor,
                        &input_schemas,
                        branch_schema,
                    )?;

                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &processor.output_routes,
                    )?;
                }
                Model::Codec(codec) => {
                    if let Some(wire_schema_identifier) = codec.wire_schema.as_ref() {
                        let wire_schema = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            wire_schema_identifier,
                            ModelKind::WireSchema,
                        )?;
                        graph.add_edge(wire_schema, source, EdgeKind::RequiredBy);
                    }
                    let schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &codec.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(schema, source, EdgeKind::RequiredBy);

                    let schema_model =
                        expect_schema_model(domain, identifier, models, &codec.schema)?;
                    let wire_schema_model = codec
                        .wire_schema
                        .as_ref()
                        .map(|wire_schema| {
                            expect_wire_schema_model(domain, identifier, models, wire_schema)
                        })
                        .transpose()?;
                    ensure_codec_schema_compatibility(
                        domain,
                        identifier,
                        &codec.wire_format,
                        wire_schema_model,
                        schema_model,
                        &codec.encoding_rules,
                    )?;
                }
                Model::Ingestor(ingestor) => {
                    validate_ingestor_source(domain, identifier, ingestor)?;
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &ingestor.output_routes,
                    )?;

                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &ingestor.output_routes,
                    )?;

                    let codec = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &ingestor.decode_using_codec,
                        ModelKind::Codec,
                    )?;
                    graph.add_edge(codec, source, EdgeKind::RequiredBy);
                    let codec_model = expect_codec_model(
                        domain,
                        identifier,
                        models,
                        &ingestor.decode_using_codec,
                    )?;
                    ensure_codec_supports_decoding(domain, identifier, codec_model)?;

                    match &ingestor.source {
                        IngestSource::Http { client, .. }
                        | IngestSource::Kinesis { client, .. }
                        | IngestSource::Kafka { client, .. }
                        | IngestSource::Pulsar { client, .. }
                        | IngestSource::Prometheus { client, .. }
                        | IngestSource::RabbitMq { client, .. }
                        | IngestSource::RedisPubSub { client, .. }
                        | IngestSource::Mqtt { client, .. }
                        | IngestSource::Nats { client, .. }
                        | IngestSource::ZeroMq { client, .. }
                        | IngestSource::Sqs { client, .. }
                        | IngestSource::Websockets { client, .. } => {
                            let client = expect_kind(
                                domain,
                                identifier,
                                models,
                                &indices,
                                client,
                                ModelKind::Client,
                            )?;
                            graph.add_edge(client, source, EdgeKind::RequiredBy);
                        }
                        IngestSource::Endpoint { endpoint, .. } => {
                            let endpoint = expect_kind(
                                domain,
                                identifier,
                                models,
                                &indices,
                                endpoint,
                                ModelKind::Endpoint,
                            )?;
                            graph.add_edge(endpoint, source, EdgeKind::RequiredBy);
                        }
                    }

                    let producer_schema = schema_for_codec_model(
                        domain,
                        identifier,
                        models,
                        &ingestor.decode_using_codec,
                    )?;
                    let message_namespace = Identifier::parse(INGEST_MESSAGE_NAMESPACE)
                        .expect("static namespace must be a valid identifier");
                    validate_ingestor_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &[(&message_namespace, producer_schema)],
                        None,
                        ingestor.filter_where.as_ref(),
                        &ingestor.source,
                    )?;
                    for output in ingestor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        let effective_schema = effective_ingestor_output_filter_map_schema(
                            domain,
                            identifier,
                            models,
                            ingestor,
                            producer_schema,
                            output,
                            consumer_schema,
                        )?;
                        ensure_internal_schema_compatibility(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "ingestor output",
                        )?;
                        ensure_output_branch(
                            domain,
                            identifier,
                            models,
                            output,
                            producer_schema,
                            &effective_schema,
                            None,
                        )?;
                    }
                    ensure_ingestor_timestamp_source(
                        domain,
                        identifier,
                        ingestor,
                        producer_schema,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &ingestor.output_routes,
                    )?;
                }
                Model::Relay(stream) => {
                    if identifier.as_str().eq_ignore_ascii_case(BRANCH_NAMESPACE)
                        || stream.name.as_str().eq_ignore_ascii_case(BRANCH_NAMESPACE)
                    {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "'branch' is a reserved namespace and cannot be used as a \
                                     relay name"
                                .to_string(),
                        }));
                    }
                    let schema = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &stream.schema,
                        ModelKind::Schema,
                    )?;
                    graph.add_edge(schema, source, EdgeKind::RequiredBy);
                    if let Some(branch_ref) = stream.branching.branch() {
                        let branch = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            branch_ref,
                            ModelKind::Branch,
                        )?;
                        graph.add_edge(branch, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Reingestor(reingestor) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &reingestor.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.from,
                        "reingestor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &reingestor.from,
                        "reingestor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &reingestor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        reingestor.filter_where.as_ref(),
                    )?;
                    for output in reingestor.output_routes.outputs() {
                        let consumer_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        let effective_schema = effective_processor_output_filter_map_schema(
                            domain,
                            identifier,
                            models,
                            &input_schemas,
                            output,
                            consumer_schema,
                            branch_schema,
                        )?;
                        ensure_internal_schema_compatibility(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "reingestor flow",
                        )?;
                        let incoming_branch =
                            relay_declared_branch(domain, identifier, models, first_input_relay)?;
                        ensure_output_branch(
                            domain,
                            identifier,
                            models,
                            output,
                            input_schemas[0].1,
                            &effective_schema,
                            incoming_branch,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reingestor.output_routes,
                    )?;
                }
                Model::Endpoint(endpoint) => {
                    let vhost = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &endpoint.on_vhost,
                        ModelKind::Vhost,
                    )?;
                    graph.add_edge(vhost, source, EdgeKind::RequiredBy);
                    if let Some(signaling_protocol) = endpoint.signaling_protocol.as_ref() {
                        if endpoint.endpoint_type != EndpointType::Websockets {
                            return Err(Report::new(RegistryError::InvalidModel {
                                domain: domain.as_str().to_string(),
                                identifier: identifier.as_str().to_string(),
                                reason: "SIGNALING PROTOCOL is only valid for WEBSOCKETS endpoints"
                                    .to_string(),
                            }));
                        }
                        let signaling_protocol = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            signaling_protocol,
                            ModelKind::SignalingProtocol,
                        )?;
                        graph.add_edge(signaling_protocol, source, EdgeKind::RequiredBy);
                    }
                }
                Model::Lookup(lookup) => {
                    let codec = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &lookup.decode_using_codec,
                        ModelKind::Codec,
                    )?;
                    graph.add_edge(codec, source, EdgeKind::RequiredBy);
                    let codec_model =
                        expect_codec_model(domain, identifier, models, &lookup.decode_using_codec)?;
                    ensure_codec_supports_decoding(domain, identifier, codec_model)?;

                    let schema = schema_for_codec_model(
                        domain,
                        identifier,
                        models,
                        &lookup.decode_using_codec,
                    )?;
                    ensure_lookup_key_field_exists(domain, identifier, lookup, schema)?;
                }
                Model::Deduplicator(deduplicator) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &deduplicator.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.from,
                        "deduplicator input",
                    )?;
                    ensure_deduplicator_key_compiles(
                        domain,
                        identifier,
                        deduplicator,
                        &input_schemas,
                    )?;
                    humantime::parse_duration(&deduplicator.max_time).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!(
                                "invalid deduplicator MAX TIME '{}': {error}",
                                deduplicator.max_time
                            ),
                        })
                    })?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &deduplicator.from,
                        "deduplicator input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &deduplicator.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        deduplicator.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        &deduplicator.output_routes,
                        &input_schemas,
                        branch_schema,
                        "deduplicator flow",
                        ProcessorOutputSchemaCompatibility::Compatible,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &deduplicator.output_routes,
                    )?;
                }
                Model::Correlator(correlator) => {
                    let left_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.left,
                        "correlator left input",
                    )?;
                    let right_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.right,
                        "correlator right input",
                    )?;
                    validate_correlator_input_sides_do_not_overlap(domain, identifier, correlator)?;

                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.output_routes,
                    )?;

                    add_correlation_timeout_action_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.timeout_policy.left,
                    )?;
                    add_correlation_timeout_action_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.timeout_policy.right,
                    )?;

                    let Some((left_relay, _left_schema)) = left_schemas.first().copied() else {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "correlator left input requires at least one input relay"
                                .to_string(),
                        }));
                    };
                    let Some((_right_relay, _right_schema)) = right_schemas.first().copied() else {
                        return Err(Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: "correlator right input requires at least one input relay"
                                .to_string(),
                        }));
                    };
                    let branch_schema =
                        relay_declared_branch_schema(domain, identifier, models, left_relay)?;
                    validate_scoped_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &left_schemas,
                        branch_schema,
                        correlator.left.where_clauses(),
                        "left",
                    )?;
                    validate_scoped_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &right_schemas,
                        branch_schema,
                        correlator.right.where_clauses(),
                        "right",
                    )?;
                    let mut input_schemas =
                        Vec::with_capacity(left_schemas.len() + right_schemas.len());
                    input_schemas.extend(left_schemas.iter().copied());
                    input_schemas.extend(right_schemas.iter().copied());
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        correlator.filter_where.as_ref(),
                    )?;
                    validate_correlator(
                        domain,
                        identifier,
                        models,
                        correlator,
                        &left_schemas,
                        &right_schemas,
                    )?;
                    for output in correlator.output_routes.outputs() {
                        let output_schema =
                            schema_for_ack_model(domain, identifier, models, &output.relay)?;
                        validate_correlator_output(
                            domain,
                            identifier,
                            models,
                            &left_schemas,
                            &right_schemas,
                            output,
                            output_schema,
                            branch_schema,
                        )?;
                    }
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &correlator.output_routes,
                    )?;
                }
                Model::Reorderer(reorderer) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.output_routes,
                    )?;

                    humantime::parse_duration(&reorderer.max_time).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!(
                                "invalid reorderer MAX TIME '{}': {error}",
                                reorderer.max_time
                            ),
                        })
                    })?;
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &reorderer.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.from,
                        "reorderer input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &reorderer.from,
                        "reorderer input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &reorderer.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        reorderer.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        &reorderer.output_routes,
                        &input_schemas,
                        branch_schema,
                        "reorderer flow",
                        ProcessorOutputSchemaCompatibility::Compatible,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &reorderer.output_routes,
                    )?;
                }
                Model::Junction(junction) => {
                    ensure_processor_output_flush_policies(
                        domain,
                        identifier,
                        &junction.output_routes,
                    )?;
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &junction.output_routes,
                    )?;

                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &junction.from,
                        "junction input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &junction.from,
                        "junction input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &junction.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        junction.filter_where.as_ref(),
                    )?;
                    ensure_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        &junction.output_routes,
                        &input_schemas,
                        branch_schema,
                        "junction flow",
                        ProcessorOutputSchemaCompatibility::Equal,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &junction.output_routes,
                    )?;
                }
                Model::WindowProcessor(window_processor) => {
                    add_processor_output_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.output_routes,
                    )?;

                    parse_window_bound_duration(
                        domain,
                        identifier,
                        "WIDTH",
                        window_processor.width.duration.as_deref(),
                    )?;
                    parse_window_bound_duration(
                        domain,
                        identifier,
                        "STEP",
                        window_processor.step.duration.as_deref(),
                    )?;
                    let input_schemas = processor_input_schemas(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.from,
                        "window processor input",
                    )?;
                    let first_input_relay = processor_first_input_relay(
                        domain,
                        identifier,
                        &window_processor.from,
                        "window processor input",
                    )?;
                    let branch_schema = relay_declared_branch_schema(
                        domain,
                        identifier,
                        models,
                        first_input_relay,
                    )?;
                    validate_from_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        &window_processor.from.r#where,
                    )?;
                    validate_filter_where_for_internal_schemas(
                        domain,
                        identifier,
                        models,
                        &input_schemas,
                        branch_schema,
                        window_processor.filter_where.as_ref(),
                    )?;
                    ensure_window_processor_output_schemas(
                        domain,
                        identifier,
                        models,
                        window_processor,
                        &input_schemas,
                        branch_schema,
                    )?;
                    add_output_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &window_processor.output_routes,
                    )?;
                }
                Model::Emitter(emitter) => {
                    let input = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &emitter.from_relay,
                        ModelKind::Relay,
                    )?;
                    graph.add_edge(input, source, EdgeKind::RequiredBy);
                    graph.add_edge(input, source, EdgeKind::SendsTo);

                    if let Some(codec_name) = &emitter.encode_using_codec {
                        let codec = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            codec_name,
                            ModelKind::Codec,
                        )?;
                        graph.add_edge(codec, source, EdgeKind::RequiredBy);
                        let codec_model =
                            expect_codec_model(domain, identifier, models, codec_name)?;
                        ensure_codec_supports_encoding(domain, identifier, codec_model)?;
                    }

                    let client = emitter.sink.client();
                    let client = expect_kind(
                        domain,
                        identifier,
                        models,
                        &indices,
                        client,
                        ModelKind::Client,
                    )?;
                    graph.add_edge(client, source, EdgeKind::RequiredBy);

                    if let Some(catalog_client) = emitter.sink.iceberg_catalog_client() {
                        let catalog_client = expect_kind(
                            domain,
                            identifier,
                            models,
                            &indices,
                            catalog_client,
                            ModelKind::Client,
                        )?;
                        graph.add_edge(catalog_client, source, EdgeKind::RequiredBy);
                    }

                    let producer_schema =
                        schema_for_ack_model(domain, identifier, models, &emitter.from_relay)?;
                    let output_schema = if let Some(codec_name) = &emitter.encode_using_codec {
                        schema_for_codec_model(domain, identifier, models, codec_name)?
                    } else {
                        producer_schema
                    };
                    let effective_schema = effective_emitter_filter_map_schema(
                        domain,
                        identifier,
                        models,
                        emitter,
                        producer_schema,
                        output_schema,
                    )?;
                    if let Some(codec_name) = &emitter.encode_using_codec {
                        let consumer_schema =
                            schema_for_codec_model(domain, identifier, models, codec_name)?;
                        ensure_internal_schema_compatibility_with_policy(
                            domain,
                            identifier,
                            &effective_schema,
                            consumer_schema,
                            "emitter input",
                            SensitivityCompatibility::AllowSensitiveProducer,
                        )?;
                    }
                    add_message_error_policy_edges(
                        domain,
                        identifier,
                        models,
                        &indices,
                        &mut graph,
                        source,
                        &emitter.error_policies.message,
                    )?;
                }
            }
            validate_model_message_error_policies(domain, identifier, models, model)?;
        }

        if has_required_by_cycle(&graph) {
            return Err(Report::new(RegistryError::ConfigurationCycle {
                domain: domain.as_str().to_string(),
            }));
        }

        validate_vhost_hostnames(domain, models)?;
        validate_endpoint_paths(domain, models)?;
        infer_stream_branchings(domain, models, &indices, &mut graph)?;
        validate_processing_branch_selections(domain, models, &indices, &graph)?;
        attach_materializer_nodes(models, &indices, &mut graph);

        Ok(Self {
            models: models.clone(),
            graph: ActiveGraph { graph, indices },
        })
    }
}

#[derive(Debug, Clone)]
pub struct ActiveGraph {
    graph: DiGraph<ActiveNode, EdgeKind>,
    indices: HashMap<RegistryKey, NodeIndex>,
}

impl ActiveGraph {
    pub fn node(&self, kind: ModelKind, identifier: &Identifier) -> Option<&ActiveNode> {
        self.indices
            .get(&RegistryKey::new(kind, identifier.clone()))
            .and_then(|index| self.graph.node_weight(*index))
    }

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub fn edges(&self) -> Vec<(Identifier, Identifier, EdgeKind)> {
        self.graph
            .edge_references()
            .map(|edge| {
                let from = self
                    .graph
                    .node_weight(edge.source())
                    .expect("source node must exist")
                    .identifier
                    .clone();
                let to = self
                    .graph
                    .node_weight(edge.target())
                    .expect("target node must exist")
                    .identifier
                    .clone();
                (from, to, *edge.weight())
            })
            .collect()
    }

    pub fn nodes(&self) -> Vec<ActiveNode> {
        self.graph.node_weights().cloned().collect()
    }

    pub fn schedule_for_domain(
        &self,
        domain: &Domain,
        cluster_nodes: &[String],
        replica_count: usize,
    ) -> DomainSchedule {
        let cluster_nodes = SortedSet::from_unsorted(cluster_nodes.to_vec()).into_vec();
        let mut next_assignment = 0usize;
        let mut node_load = HashMap::<String, usize>::new();
        let mut assigned_by_key = HashMap::<RegistryKey, Vec<String>>::new();
        let mut depth_cache = HashMap::<NodeIndex, usize>::new();
        let mut nodes = self
            .graph
            .node_indices()
            .map(|index| {
                let node = self
                    .graph
                    .node_weight(index)
                    .expect("graph node must exist for every index")
                    .clone();
                let depth = schedulable_depth(&self.graph, index, &mut depth_cache);
                (index, node, depth)
            })
            .collect::<Vec<_>>();
        nodes.sort_by(
            |(left_index, left_node, left_depth), (right_index, right_node, right_depth)| {
                left_depth
                    .cmp(right_depth)
                    .then_with(|| left_node.kind.as_str().cmp(right_node.kind.as_str()))
                    .then_with(|| {
                        left_node
                            .identifier
                            .as_str()
                            .cmp(right_node.identifier.as_str())
                    })
                    .then_with(|| left_index.index().cmp(&right_index.index()))
            },
        );

        DomainSchedule {
            domain: domain.clone(),
            nodes: nodes
                .into_iter()
                .map(|(index, node, _)| {
                    let mut assignment_planner = AssignmentPlanner {
                        graph: &self.graph,
                        cluster_nodes: &cluster_nodes,
                        assigned_by_key: &assigned_by_key,
                        node_load: &node_load,
                        next_assignment: &mut next_assignment,
                        replica_count,
                    };
                    let assigned_nodes =
                        assignment_for_model(&mut assignment_planner, index, node.config.as_ref());
                    let primary_node = assigned_nodes.first().cloned();
                    if !assigned_nodes.is_empty() {
                        assigned_by_key.insert(node.key(), assigned_nodes.clone());
                        for assigned_node in &assigned_nodes {
                            *node_load.entry(assigned_node.clone()).or_insert(0) += 1;
                        }
                    }
                    ScheduledNode {
                        identifier: node.identifier,
                        kind: node.kind,
                        config: Box::new((*node.config).clone()),
                        effective_branching: node.effective_branching,
                        effective_branching_schema: node.effective_branching_schema,
                        kafka_partition_schedule: None,
                        primary_node,
                        assigned_nodes,
                    }
                })
                .collect(),
        }
    }

    pub fn describe(&self) -> String {
        self.to_dataflow_graph("").render_ascii()
    }

    pub fn to_dataflow_graph(&self, domain: impl Into<String>) -> DataflowGraph {
        let mut included_nodes = HashSet::new();
        let mut edges = self
            .graph
            .node_indices()
            .filter(|index| {
                self.graph
                    .node_weight(*index)
                    .expect("dataflow graph node must exist")
                    .is_dataflow_node()
            })
            .flat_map(|source_index| {
                let source = self
                    .graph
                    .node_weight(source_index)
                    .expect("dataflow source node must exist");
                included_nodes.insert(source_index);
                visible_dataflow_targets(&self.graph, source_index)
                    .into_iter()
                    .map(|(target_index, edge_kind)| {
                        let target = self
                            .graph
                            .node_weight(target_index)
                            .expect("dataflow target node must exist");
                        included_nodes.insert(target_index);
                        DataflowEdge::data(
                            source.dataflow_id(),
                            target.dataflow_id(),
                            dataflow_edge_kind(edge_kind),
                        )
                        .with_metric(source.dataflow_metric_for_target(target))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let schemas = self
            .graph
            .node_indices()
            .filter_map(|index| {
                let node = self
                    .graph
                    .node_weight(index)
                    .expect("dataflow graph node must exist");
                let Model::Schema(schema) = node.config.as_ref() else {
                    return None;
                };
                Some((node.identifier.clone(), schema.clone()))
            })
            .collect::<HashMap<_, _>>();

        let mut nodes = included_nodes
            .iter()
            .map(|index| {
                self.graph
                    .node_weight(*index)
                    .expect("dataflow graph node must exist")
                    .to_dataflow_node(&schemas)
            })
            .collect::<Vec<_>>();
        for index in &included_nodes {
            let node = self
                .graph
                .node_weight(*index)
                .expect("dataflow graph node must exist");
            if let Some(client_node) = node.dataflow_source_client_node() {
                edges.push(
                    DataflowEdge::data(
                        client_node.id.clone(),
                        node.dataflow_id(),
                        DataflowEdgeKind::Data,
                    )
                    .with_metric(node.dataflow_source_client_metric()),
                );
                nodes.push(client_node);
            }
            if let Some(client_node) = node.dataflow_sink_client_node() {
                if let Some(metric) = node.dataflow_sink_client_metric() {
                    edges.push(
                        DataflowEdge::data(
                            node.dataflow_id(),
                            client_node.id.clone(),
                            DataflowEdgeKind::Data,
                        )
                        .with_metric(metric),
                    );
                }
                nodes.push(client_node);
            }
        }

        nodes.sort_by(|left, right| left.id.cmp(&right.id));
        nodes.dedup_by(|left, right| left.id == right.id);
        edges.sort_by(|left, right| {
            left.source
                .cmp(&right.source)
                .then_with(|| left.target.cmp(&right.target))
                .then_with(|| left.kind.cmp(&right.kind))
        });
        edges.dedup_by(|left, right| {
            left.source == right.source && left.target == right.target && left.kind == right.kind
        });

        DataflowGraph {
            domain: domain.into(),
            statistics: Default::default(),
            nodes,
            edges,
        }
        .laid_out()
    }
}

fn visible_dataflow_targets(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    source_index: NodeIndex,
) -> Vec<(NodeIndex, EdgeKind)> {
    let mut targets = Vec::new();
    let mut visited = HashSet::new();
    let mut pending = graph
        .edges_directed(source_index, Direction::Outgoing)
        .filter(|edge| edge.weight().is_visible_dataflow_edge())
        .map(|edge| (edge.target(), *edge.weight()))
        .collect::<Vec<_>>();

    while let Some((index, edge_kind)) = pending.pop() {
        if !visited.insert((index, edge_kind)) {
            continue;
        }
        let node = graph
            .node_weight(index)
            .expect("dataflow traversal node must exist");
        if node.is_dataflow_node() {
            targets.push((index, edge_kind));
            continue;
        }
        pending.extend(
            graph
                .edges_directed(index, Direction::Outgoing)
                .filter(|edge| edge.weight().is_visible_dataflow_edge())
                .map(|edge| (edge.target(), edge_kind)),
        );
    }

    targets
}

const fn dataflow_edge_kind(kind: EdgeKind) -> DataflowEdgeKind {
    match kind {
        EdgeKind::RequiredBy => DataflowEdgeKind::Data,
        EdgeKind::SendsTo => DataflowEdgeKind::Data,
        EdgeKind::CorrelationTimeout => DataflowEdgeKind::CorrelationTimeout,
        EdgeKind::MessageError => DataflowEdgeKind::MessageError,
    }
}

#[derive(Debug, Clone)]
pub struct ActiveNode {
    pub identifier: Identifier,
    pub kind: ModelKind,
    pub config: Arc<Model>,
    pub effective_branching: Option<Vec<Identifier>>,
    pub effective_branching_schema: Option<Identifier>,
}

impl ActiveNode {
    fn key(&self) -> RegistryKey {
        RegistryKey::new(self.kind, self.identifier.clone())
    }

    fn dataflow_id(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.identifier.as_str())
    }

    fn dataflow_source_client_node(&self) -> Option<DataflowNode> {
        let Model::Ingestor(ingestor) = self.config.as_ref() else {
            return None;
        };
        let source = ingestor.source.source_ref();
        let source_kind = ingestor.source.source_kind().as_str();
        Some(DataflowNode::new(
            format!("{}_source:{}", source_kind, source.as_str()),
            source.as_str(),
            DataflowNodeKind::Client,
            ingestor.source.transport_label(),
        ))
    }

    fn dataflow_sink_client_node(&self) -> Option<DataflowNode> {
        let Model::Emitter(emitter) = self.config.as_ref() else {
            return None;
        };
        let client = emitter.sink.client();
        Some(DataflowNode::new(
            format!("client_sink:{}", client.as_str()),
            client.as_str(),
            DataflowNodeKind::Client,
            emitter.sink.transport_label(),
        ))
    }

    fn dataflow_source_client_metric(&self) -> DataflowMetricRef {
        DataflowMetricRef::new(
            self.kind.as_str().to_ascii_uppercase(),
            self.identifier.as_str(),
            "received",
            None::<String>,
        )
    }

    fn dataflow_sink_client_metric(&self) -> Option<DataflowMetricRef> {
        let Model::Emitter(emitter) = self.config.as_ref() else {
            return None;
        };
        Some(DataflowMetricRef::new(
            self.kind.as_str().to_ascii_uppercase(),
            self.identifier.as_str(),
            "sent",
            Some(emitter.from_relay.as_str().to_string()),
        ))
    }

    fn dataflow_metric_for_target(&self, target: &ActiveNode) -> DataflowMetricRef {
        if let ModelKind::Relay = target.kind {
            return DataflowMetricRef::new(
                self.kind.as_str().to_ascii_uppercase(),
                self.identifier.as_str(),
                "sent",
                Some(target.identifier.as_str().to_string()),
            );
        }
        DataflowMetricRef::new(
            target.kind.as_str().to_ascii_uppercase(),
            target.identifier.as_str(),
            "received",
            Some(self.identifier.as_str().to_string()),
        )
    }

    fn to_dataflow_node(&self, schemas: &HashMap<Identifier, CreateSchema>) -> DataflowNode {
        let node = DataflowNode::new(
            self.dataflow_id(),
            self.identifier.as_str(),
            self.dataflow_kind(),
            self.dataflow_subtype(),
        )
        .with_optional_branching_schema(
            self.dataflow_branching_schema()
                .map(|schema| schema.as_str().to_string()),
        );
        match self.config.as_ref() {
            Model::Relay(relay) => {
                let Some(schema) = schemas.get(&relay.schema) else {
                    return node;
                };
                node.with_schema(
                    schema.name.as_str(),
                    schema
                        .fields
                        .iter()
                        .map(dataflow_schema_field)
                        .collect::<Vec<_>>(),
                )
            }
            _ => node,
        }
    }

    fn dataflow_kind(&self) -> DataflowNodeKind {
        match self.kind {
            ModelKind::Ingestor => DataflowNodeKind::Ingestor,
            ModelKind::Emitter => DataflowNodeKind::Emitter,
            ModelKind::Relay => DataflowNodeKind::Relay,
            _ => DataflowNodeKind::Processor,
        }
    }

    fn dataflow_subtype(&self) -> &str {
        match self.kind {
            ModelKind::Ingestor => ingestor_subtype(self.config.as_ref()),
            ModelKind::Emitter => emitter_subtype(self.config.as_ref()),
            ModelKind::Relay => "RELAY",
            _ => self.kind.as_str(),
        }
    }

    fn dataflow_branching_schema(&self) -> Option<Identifier> {
        self.effective_branching_schema.clone()
    }

    fn is_dataflow_node(&self) -> bool {
        matches!(
            self.kind,
            ModelKind::Ingestor
                | ModelKind::Relay
                | ModelKind::Generator
                | ModelKind::Inferencer
                | ModelKind::WasmProcessor
                | ModelKind::Reingestor
                | ModelKind::Correlator
                | ModelKind::Junction
                | ModelKind::Deduplicator
                | ModelKind::Reorderer
                | ModelKind::WindowProcessor
                | ModelKind::Emitter
        )
    }
}

fn dataflow_schema_field(field: &SchemaField) -> DataflowSchemaField {
    DataflowSchemaField {
        name: field.name.as_str().to_string(),
        ty: parse_as_to_dataflow_label(&field.ty),
        optional: field.optional,
        sensitive: field.sensitive,
    }
}

fn parse_as_to_dataflow_label(ty: &ParseAsType) -> String {
    match ty {
        ParseAsType::U8 => "U8".to_string(),
        ParseAsType::I8 => "I8".to_string(),
        ParseAsType::U16 => "U16".to_string(),
        ParseAsType::I16 => "I16".to_string(),
        ParseAsType::U32 => "U32".to_string(),
        ParseAsType::I32 => "I32".to_string(),
        ParseAsType::U64 => "U64".to_string(),
        ParseAsType::I64 => "I64".to_string(),
        ParseAsType::Bool => "BOOL".to_string(),
        ParseAsType::String => "STRING".to_string(),
        ParseAsType::Datetime => "DATETIME".to_string(),
        ParseAsType::F32 => "F32".to_string(),
        ParseAsType::F64 => "F64".to_string(),
        ParseAsType::Array { element, len } => {
            format!("ARRAY<{}, {}>", parse_as_to_dataflow_label(element), len)
        }
        ParseAsType::Vec { element } => format!("VEC<{}>", parse_as_to_dataflow_label(element)),
    }
}

fn validate_ingestor_source(
    domain: &Domain,
    identifier: &Identifier,
    ingestor: &CreateIngestor,
) -> Result<(), Report<RegistryError>> {
    if let IngestSource::Mqtt {
        topic,
        instances,
        mode,
        ..
    } = &ingestor.source
    {
        if topic.is_empty() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "MQTT topic filter must not be empty".to_string(),
            }));
        }
        if *instances == 0 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "MQTT instances must be greater than 0".to_string(),
            }));
        }
        if mode.max_inflight() == 0 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "MQTT mode MAX must be greater than 0".to_string(),
            }));
        }
    }
    Ok(())
}

fn ingestor_subtype(model: &Model) -> &str {
    let Model::Ingestor(ingestor) = model else {
        return "INGESTOR";
    };
    if let IngestSource::Endpoint { .. } = ingestor.source {
        return "INGESTOR";
    }
    ingestor.source.transport_label()
}

fn emitter_subtype(model: &Model) -> &str {
    let Model::Emitter(emitter) = model else {
        return "EMITTER";
    };
    emitter.sink.transport_label()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeKind {
    RequiredBy,
    SendsTo,
    CorrelationTimeout,
    MessageError,
}

impl EdgeKind {
    const fn is_visible_dataflow_edge(self) -> bool {
        self.is_runtime_flow_edge()
    }

    const fn is_runtime_flow_edge(self) -> bool {
        match self {
            Self::RequiredBy => false,
            Self::SendsTo | Self::CorrelationTimeout | Self::MessageError => true,
        }
    }
}

fn schedulable_depth(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    cache: &mut HashMap<NodeIndex, usize>,
) -> usize {
    if let Some(depth) = cache.get(&index) {
        return *depth;
    }

    let mut max_depth = 0usize;
    for edge in graph.edges_directed(index, Direction::Incoming) {
        if !edge.weight().is_runtime_flow_edge() {
            continue;
        }
        let source = edge.source();
        let source_node = graph
            .node_weight(source)
            .expect("incoming source node must exist");
        let candidate_depth = if is_schedulable_model(source_node.config.as_ref()) {
            schedulable_depth(graph, source, cache) + 1
        } else {
            schedulable_depth(graph, source, cache)
        };
        max_depth = max_depth.max(candidate_depth);
    }

    cache.insert(index, max_depth);
    max_depth
}

fn is_schedulable_model(model: &Model) -> bool {
    matches!(
        model,
        Model::Generator(_)
            | Model::Inferencer(_)
            | Model::Ingestor(_)
            | Model::Reingestor(_)
            | Model::Materializer(_)
            | Model::Lookup(_)
            | Model::Deduplicator(_)
            | Model::Correlator(_)
            | Model::Reorderer(_)
            | Model::Junction(_)
            | Model::WindowProcessor(_)
            | Model::Emitter(_)
    )
}

fn attach_materializer_nodes(
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
) {
    let materialized_streams = models
        .iter()
        .filter_map(|(key, model)| {
            let Model::Relay(relay) = model else {
                return None;
            };
            relay
                .materialized_state
                .clone()
                .map(|state| (key.identifier.clone(), relay.clone(), state))
        })
        .collect::<Vec<_>>();

    for (identifier, relay, state) in materialized_streams {
        let Some(stream_index) = indices
            .get(&RegistryKey::new(ModelKind::Relay, identifier.clone()))
            .copied()
        else {
            continue;
        };
        let effective_branching = graph
            .node_weight(stream_index)
            .and_then(|node| node.effective_branching.clone());
        let effective_branching_schema = graph
            .node_weight(stream_index)
            .and_then(|node| node.effective_branching_schema.clone());
        let materializer = CreateMaterializer {
            relay: relay.name,
            state,
        };
        let materializer_index = graph.add_node(ActiveNode {
            identifier: identifier.clone(),
            kind: ModelKind::Materializer,
            config: Arc::new(Model::Materializer(materializer)),
            effective_branching,
            effective_branching_schema,
        });
        graph.add_edge(stream_index, materializer_index, EdgeKind::RequiredBy);
        graph.add_edge(stream_index, materializer_index, EdgeKind::SendsTo);
    }
}

fn validate_branch_model(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    branch: &CreateBranch,
) -> Result<(), Report<RegistryError>> {
    parse_branch_ttl(domain, identifier, &branch.ttl)?;
    if let Some(eviction) = &branch.eviction {
        if eviction.max_instances() == 0 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "branch MAX INSTANCES must be greater than zero".to_string(),
            }));
        }
    }
    ensure_branch_schema_exists(domain, identifier, models, branch)
}

fn parse_branch_ttl(
    domain: &Domain,
    identifier: &Identifier,
    ttl: &str,
) -> Result<Duration, Report<RegistryError>> {
    humantime::parse_duration(ttl).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("invalid branch ttl '{ttl}': {error}"),
        })
    })
}

fn ensure_branch_schema_exists(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    branch: &CreateBranch,
) -> Result<(), Report<RegistryError>> {
    let Some(Model::Schema(_)) =
        models.get(&RegistryKey::new(ModelKind::Schema, branch.schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch.schema.as_str().to_string(),
        }));
    };

    Ok(())
}

fn ensure_schema_has_fields<T>(
    domain: &Domain,
    identifier: &Identifier,
    fields: &[T],
    schema_kind: &str,
) -> Result<(), Report<RegistryError>> {
    if fields.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{schema_kind} must declare at least one field"),
        }));
    }
    Ok(())
}

fn ensure_wire_schema_has_fields(
    domain: &Domain,
    identifier: &Identifier,
    schema: &CreateWireSchemaStmt,
) -> Result<(), Report<RegistryError>> {
    match schema {
        CreateWireSchemaStmt::Json(schema) => {
            ensure_schema_has_fields(domain, identifier, &schema.fields, "wire schema")
        }
        CreateWireSchemaStmt::Cbor(schema) => {
            ensure_schema_has_fields(domain, identifier, &schema.fields, "wire schema")
        }
        CreateWireSchemaStmt::Avro(schema) => {
            ensure_schema_has_fields(domain, identifier, &schema.fields, "wire schema")
        }
    }
}

fn ensure_signaling_protocol_is_valid(
    domain: &Domain,
    identifier: &Identifier,
    protocol: &CreateSignalingProtocol,
) -> Result<(), Report<RegistryError>> {
    if protocol.on_connect.send_bodies.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "signaling protocol must send at least one body".to_string(),
        }));
    }
    if protocol.on_connect.wait_bodies.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "signaling protocol must wait for at least one body".to_string(),
        }));
    }
    humantime::parse_duration(&protocol.on_connect.timeout).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "invalid signaling protocol timeout '{}': {error}",
                protocol.on_connect.timeout
            ),
        })
    })?;
    Ok(())
}

fn parse_window_bound_duration(
    domain: &Domain,
    identifier: &Identifier,
    bound_name: &str,
    duration: Option<&str>,
) -> Result<(), Report<RegistryError>> {
    let Some(duration) = duration else {
        return Ok(());
    };
    humantime::parse_duration(duration)
        .map(|_| ())
        .map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid window {bound_name} duration '{duration}': {error}"),
            })
        })
}

fn processor_input_schemas<'inputs, 'models>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'models HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    inputs: &'inputs nervix_models::ProcessorInputs,
    relation: &str,
) -> Result<Vec<(&'inputs Identifier, &'models CreateSchema)>, Report<RegistryError>> {
    if inputs.from.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{relation} requires at least one input relay"),
        }));
    }

    let mut seen = HashSet::new();
    let mut input_schemas = Vec::new();
    let mut reference_schema = None;
    for from_relay in inputs.relays() {
        if !seen.insert(from_relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} input relay '{}' is declared more than once",
                    from_relay.as_str()
                ),
            }));
        }
        let input = expect_kind(
            domain,
            identifier,
            models,
            indices,
            from_relay,
            ModelKind::Relay,
        )?;
        graph.add_edge(input, source, EdgeKind::RequiredBy);
        graph.add_edge(input, source, EdgeKind::SendsTo);

        let input_schema = schema_for_ack_model(domain, identifier, models, from_relay)?;
        if let Some(reference_schema) = reference_schema {
            ensure_equal_internal_schema(
                domain,
                identifier,
                input_schema,
                reference_schema,
                relation,
            )?;
        } else {
            reference_schema = Some(input_schema);
        }
        input_schemas.push((from_relay, input_schema));
    }
    Ok(input_schemas)
}

fn validate_correlator_input_sides_do_not_overlap(
    domain: &Domain,
    identifier: &Identifier,
    correlator: &CreateCorrelator,
) -> Result<(), Report<RegistryError>> {
    let mut left = HashSet::new();
    for relay in correlator.left.relays() {
        left.insert(relay.clone());
    }
    for relay in correlator.right.relays() {
        if left.contains(relay) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator input relay '{}' is declared on both LEFT and RIGHT",
                    relay.as_str()
                ),
            }));
        }
    }
    Ok(())
}

fn processor_first_input_relay<'a>(
    domain: &Domain,
    identifier: &Identifier,
    inputs: &'a nervix_models::ProcessorInputs,
    relation: &str,
) -> Result<&'a Identifier, Report<RegistryError>> {
    inputs.from.first().ok_or_else(|| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{relation} requires at least one input relay"),
        })
    })
}

fn ensure_window_processor_output_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    window_processor: &CreateWindowProcessor,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, &window_processor.output_routes)?;
    for output in window_processor.output_routes.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        validate_window_processor_output(
            domain,
            identifier,
            models,
            output,
            output_schema,
            input_schemas,
            branch_schema,
        )?;
    }
    Ok(())
}

fn ensure_wasm_processor_output_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    processor: &nervix_models::CreateWasmProcessor,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, &processor.output_routes)?;
    let mut output_relays = HashSet::new();
    for output in processor.output_routes.outputs() {
        if !output_relays.insert(output.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "WASM processor output relay '{}' is declared more than once",
                    output.relay.as_str()
                ),
            }));
        }
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let effective_schema = effective_wasm_output_filter_map_schema(
            domain,
            identifier,
            models,
            input_schemas,
            output,
            output_schema,
            branch_schema,
        )?;
        ProcessorOutputSchemaCompatibility::Compatible.ensure(
            domain,
            identifier,
            &effective_schema,
            output_schema,
            "wasm processor flow",
        )?;
    }

    Ok(())
}

fn effective_wasm_output_filter_map_schema(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<CreateSchema, Report<RegistryError>> {
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_generated_route(
        &output.construction,
        output_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("WASM output route is invalid: {reason}"),
        })
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "FILTER-MAP may contain at most one WHERE clause".to_string(),
        }));
    }
    if !parsed.inner.invoke.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "WASM processor TO clauses may use SET and WHERE, but not INVOKE".to_string(),
        }));
    }

    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let Some((_first_input_relay, _first_input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "wasm processor input requires at least one input relay".to_string(),
        }));
    };
    let mut bindings = vec![
        readonly_binding_for_internal_schema("generated", output_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut local_namespaces = HashSet::new();
    local_namespaces.insert("generated".to_string());
    local_namespaces.insert("output".to_string());
    local_namespaces.insert(BRANCH_NAMESPACE.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

fn validate_window_processor_output(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    let aggregate = lower_window_assignments(&output.construction).map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("window output '{}' is invalid: {reason}", output.relay),
        })
    })?;
    if aggregate.inner.demands().is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window output '{}' must contain at least one aggregate function",
                output.relay
            ),
        }));
    }
    let Some((_input_relay, input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "window processor requires at least one input relay".to_string(),
        }));
    };
    for assignment in &aggregate.inner.assignments {
        for field_ref in referenced_field_refs(&assignment.value.inner) {
            if field_ref.relay == "input"
                && !input_schema
                    .fields
                    .iter()
                    .any(|field| field.name.as_str() == field_ref.field)
            {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "window aggregate references unknown input field '{}.{}'",
                        field_ref.relay, field_ref.field
                    ),
                }));
            }
        }
    }
    let assigned_fields = aggregate
        .inner
        .assignments
        .iter()
        .map(|assignment| assignment.target.field.as_str())
        .collect::<HashSet<_>>();
    for assignment in &aggregate.inner.assignments {
        if output_schema
            .fields
            .iter()
            .any(|field| field.name.as_str() == assignment.target.field)
        {
            continue;
        }
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window aggregate target field '{}.{}' is not declared in output schema '{}'",
                output.relay, assignment.target.field, output_schema.name
            ),
        }));
    }
    for field in &output_schema.fields {
        if field.optional || assigned_fields.contains(field.name.as_str()) {
            continue;
        }
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window aggregate must assign required output field '{}.{}'",
                output.relay, field.name
            ),
        }));
    }
    validate_window_route_where(
        domain,
        identifier,
        models,
        output,
        output_schema,
        branch_schema,
    )?;
    Ok(())
}

fn validate_window_route_where(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    let Some(where_clause) = output.construction.where_clause.as_ref() else {
        return Ok(());
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_finalized_output_filter(where_clause, output_arrow_schema.as_ref())
        .map_err(|reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "window output '{}' WHERE is invalid: {reason}",
                    output.relay
                ),
            })
        })?;
    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![writable_binding_for_internal_schema(
        "output",
        output_schema,
    )];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let local_namespaces = HashSet::from_iter(["output".to_string(), BRANCH_NAMESPACE.to_string()]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "window route WHERE",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions::default(),
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "window output '{}' WHERE compile failed: {}",
                output.relay, error.message
            ),
        })
    })?;
    Ok(())
}

fn locality_affinity_scores(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    assigned_by_key: &HashMap<RegistryKey, Vec<String>>,
) -> HashMap<String, usize> {
    let mut scores = HashMap::<String, usize>::new();
    collect_locality_affinity(
        graph,
        index,
        assigned_by_key,
        &mut HashSet::new(),
        &mut scores,
    );
    scores
}

fn collect_locality_affinity(
    graph: &DiGraph<ActiveNode, EdgeKind>,
    index: NodeIndex,
    assigned_by_key: &HashMap<RegistryKey, Vec<String>>,
    visited: &mut HashSet<NodeIndex>,
    scores: &mut HashMap<String, usize>,
) {
    if !visited.insert(index) {
        return;
    }

    for edge in graph.edges_directed(index, Direction::Incoming) {
        if !edge.weight().is_runtime_flow_edge() {
            continue;
        }
        let source = edge.source();
        let source_node = graph
            .node_weight(source)
            .expect("incoming source node must exist");
        if is_schedulable_model(source_node.config.as_ref()) {
            if let Some(node_ids) = assigned_by_key.get(&source_node.key()) {
                for node_id in node_ids {
                    *scores.entry(node_id.clone()).or_insert(0) += 1;
                }
            }
        } else {
            collect_locality_affinity(graph, source, assigned_by_key, visited, scores);
        }
    }
}

struct AssignmentPlanner<'a> {
    graph: &'a DiGraph<ActiveNode, EdgeKind>,
    cluster_nodes: &'a [String],
    assigned_by_key: &'a HashMap<RegistryKey, Vec<String>>,
    node_load: &'a HashMap<String, usize>,
    next_assignment: &'a mut usize,
    replica_count: usize,
}

impl AssignmentPlanner<'_> {
    fn for_model(&mut self, index: NodeIndex, model: &Model) -> Vec<String> {
        if self.cluster_nodes.is_empty() {
            return Vec::new();
        }

        match model {
            Model::Ingestor(CreateIngestor {
                source: IngestSource::Endpoint { .. },
                ..
            }) => self.cluster_nodes.to_vec(),
            Model::Generator(_)
            | Model::Inferencer(_)
            | Model::Ingestor(_)
            | Model::Reingestor(_)
            | Model::Materializer(_)
            | Model::Lookup(_)
            | Model::Deduplicator(_)
            | Model::Correlator(_)
            | Model::Junction(_)
            | Model::WindowProcessor(_)
            | Model::Emitter(_) => {
                let preferred_order =
                    locality_affinity_scores(self.graph, index, self.assigned_by_key);
                let mut ordered_nodes = self
                    .cluster_nodes
                    .iter()
                    .enumerate()
                    .map(|(position, node_id)| {
                        (
                            preferred_order.get(node_id).copied().unwrap_or(0),
                            Reverse(self.node_load.get(node_id).copied().unwrap_or(0)),
                            Reverse(
                                (position + self.cluster_nodes.len()
                                    - (*self.next_assignment % self.cluster_nodes.len()))
                                    % self.cluster_nodes.len(),
                            ),
                            node_id.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                ordered_nodes.sort_unstable();
                ordered_nodes.reverse();
                *self.next_assignment += 1;
                ordered_nodes
                    .into_iter()
                    .take(self.replica_count.saturating_add(1))
                    .map(|(_, _, _, node_id)| node_id)
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}

fn assignment_for_model(
    planner: &mut AssignmentPlanner<'_>,
    index: NodeIndex,
    model: &Model,
) -> Vec<String> {
    if planner.cluster_nodes.is_empty() {
        return Vec::new();
    }
    planner.for_model(index, model)
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
    _db: Database,
    index: Keyspace,
}

impl ModelStorage {
    fn from_database(db: Database) -> Result<Self, Report<RegistryError>> {
        let index = db
            .keyspace("models", KeyspaceCreateOptions::default)
            .change_context(RegistryError::OpenKeyspace)?;

        Ok(Self { _db: db, index })
    }

    fn put(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
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

    fn replace(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
        model: &Model,
    ) -> Result<(), Report<RegistryError>> {
        let key = encode_key(domain, kind, identifier)?;

        if self
            .index
            .get(key.clone())
            .change_context(RegistryError::ReadValue)?
            .is_none()
        {
            return Err(Report::new(RegistryError::NotFound {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
            }));
        }

        let value = serialize_value(model)?;

        self.index
            .insert(key, value)
            .change_context(RegistryError::WriteValue)
    }

    fn delete(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<(), Report<RegistryError>> {
        let key = encode_key(domain, kind, identifier)?;
        self.index
            .remove(key)
            .change_context(RegistryError::WriteValue)?;
        Ok(())
    }

    fn get(
        &self,
        domain: &Domain,
        kind: ModelKind,
        identifier: &Identifier,
    ) -> Result<Option<Model>, Report<RegistryError>> {
        let key = encode_key(domain, kind, identifier)?;
        let Some(raw) = self
            .index
            .get(key)
            .change_context(RegistryError::ReadValue)?
        else {
            return Ok(None);
        };

        let envelope = deserialize_value(raw.as_ref())?;

        let model = Model::try_from(envelope).change_context(RegistryError::ModelConversion)?;
        Ok(Some(model))
    }

    fn list_identifiers(
        &self,
        domain: &Domain,
        kind: ModelKind,
        prefix: &str,
    ) -> Result<Vec<Identifier>, Report<RegistryError>> {
        let mut out = Vec::new();
        let prefix = prefix.to_ascii_lowercase();

        for record in self.list_records()? {
            if &record.domain != domain {
                continue;
            }

            if record.model.kind() != kind {
                continue;
            }

            if !record.key.identifier.as_str().starts_with(&prefix) {
                continue;
            }

            out.push(record.key.identifier);
        }

        out.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        out.dedup_by(|a, b| a.as_str() == b.as_str());
        Ok(out)
    }

    fn list_models(
        &self,
        domain: &Domain,
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

            let envelope = deserialize_value(raw_value.as_ref())?;
            let model = Model::try_from(envelope).change_context(RegistryError::ModelConversion)?;

            let domain =
                Domain::parse(&key.domain).change_context(RegistryError::ModelConversion)?;
            let kind = ModelKind::from_str(&key.kind)
                .map_err(|_| Report::new(RegistryError::ModelConversion))?;
            let identifier = Identifier::parse(&key.identifier)
                .change_context(RegistryError::ModelConversion)?;

            records.push(StoredModelRecord {
                domain,
                key: RegistryKey::new(kind, identifier),
                model,
            });
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
    domain: &Domain,
    kind: ModelKind,
    identifier: &Identifier,
) -> Result<Vec<u8>, Report<RegistryError>> {
    storekey::serialize(&ModelKey {
        domain: domain.as_str(),
        kind: kind.as_str(),
        identifier: identifier.as_str(),
    })
    .change_context(RegistryError::EncodeKey)
}

fn serialize_value(model: &Model) -> Result<Vec<u8>, Report<RegistryError>> {
    let stored = StoredModelVersioned::from(model.clone());
    rkyv::to_bytes::<rkyv::rancor::Error>(&stored)
        .map(|bytes| bytes.to_vec())
        .change_context(RegistryError::SerializeValue)
}

fn deserialize_value(bytes: &[u8]) -> Result<StoredModelVersioned, Report<RegistryError>> {
    rkyv::from_bytes::<StoredModelVersioned, rkyv::rancor::Error>(bytes)
        .change_context(RegistryError::DeserializeValue)
}

fn expect_kind(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    referenced: &Identifier,
    expected_kind: ModelKind,
) -> Result<NodeIndex, Report<RegistryError>> {
    let referenced_key = RegistryKey::new(expected_kind, referenced.clone());
    models.get(&referenced_key).ok_or_else(|| {
        Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: expected_kind.as_str(),
            reference: referenced.as_str().to_string(),
        })
    })?;

    Ok(*indices
        .get(&referenced_key)
        .expect("referenced model must have a graph node"))
}

fn add_message_error_policy_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    policy: &MessageErrorPolicy,
) -> Result<(), Report<RegistryError>> {
    let MessageErrorPolicy::Dlq { relay, .. } = policy else {
        return Ok(());
    };
    let dlq = expect_kind(domain, identifier, models, indices, relay, ModelKind::Relay)?;
    graph.add_edge(dlq, source, EdgeKind::RequiredBy);
    graph.add_edge(source, dlq, EdgeKind::MessageError);
    Ok(())
}

#[derive(Clone, Copy, Default)]
struct MessageErrorSchemas<'a> {
    input: Option<&'a CreateSchema>,
    left: Option<&'a CreateSchema>,
    right: Option<&'a CreateSchema>,
    partial_output: Option<&'a CreateSchema>,
    allow_header_reads: bool,
}

fn validate_model_message_error_policies(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    model: &Model,
) -> Result<(), Report<RegistryError>> {
    let validate_outputs = |outputs: &ProcessorOutputs,
                            schemas: MessageErrorSchemas<'_>,
                            expected_branch: Option<&Identifier>| {
        for output in outputs.outputs() {
            let partial_output = schema_for_ack_model(domain, identifier, models, &output.relay)?;
            validate_message_error_policy(
                domain,
                identifier,
                models,
                &output.message_error_policy,
                MessageErrorSchemas {
                    partial_output: Some(partial_output),
                    ..schemas
                },
                expected_branch,
            )?;
        }
        Ok::<(), Report<RegistryError>>(())
    };

    match model {
        Model::Ingestor(node) => {
            let input =
                schema_for_codec_model(domain, identifier, models, &node.decode_using_codec)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    allow_header_reads: ingest_source_supports_headers(&node.source),
                    ..MessageErrorSchemas::default()
                },
                None,
            )
        }
        Model::Reingestor(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "reingestor error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            let branch = relay_declared_branch(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                branch,
            )
        }
        Model::Generator(node) => validate_outputs(
            &node.output_routes,
            MessageErrorSchemas::default(),
            node.branched_by.branch(),
        ),
        Model::Inferencer(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "inferencer error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::WasmProcessor(node) => {
            let relay = processor_first_input_relay(
                domain,
                identifier,
                &node.from,
                "WASM processor error input",
            )?;
            let input = schema_for_ack_model(domain, identifier, models, relay)?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    input: Some(input),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::Junction(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "junction error input",
        ),
        Model::Deduplicator(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "deduplicator error input",
        ),
        Model::Reorderer(node) => validate_transforming_processor_message_errors(
            domain,
            identifier,
            models,
            &node.from,
            &node.output_routes,
            &node.branched_by,
            "reorderer error input",
        ),
        Model::WindowProcessor(node) => validate_outputs(
            &node.output_routes,
            MessageErrorSchemas::default(),
            node.branched_by.branch(),
        ),
        Model::Correlator(node) => {
            let left_relay = processor_first_input_relay(
                domain,
                identifier,
                &node.left,
                "correlator left error input",
            )?;
            let right_relay = processor_first_input_relay(
                domain,
                identifier,
                &node.right,
                "correlator right error input",
            )?;
            validate_outputs(
                &node.output_routes,
                MessageErrorSchemas {
                    left: Some(schema_for_ack_model(
                        domain, identifier, models, left_relay,
                    )?),
                    right: Some(schema_for_ack_model(
                        domain,
                        identifier,
                        models,
                        right_relay,
                    )?),
                    ..MessageErrorSchemas::default()
                },
                node.branched_by.branch(),
            )
        }
        Model::Emitter(node) => {
            let input = schema_for_ack_model(domain, identifier, models, &node.from_relay)?;
            let partial_output = node
                .encode_using_codec
                .as_ref()
                .map(|codec| schema_for_codec_model(domain, identifier, models, codec))
                .transpose()?;
            let branch = relay_declared_branch(domain, identifier, models, &node.from_relay)?;
            validate_message_error_policy(
                domain,
                identifier,
                models,
                &node.error_policies.message,
                MessageErrorSchemas {
                    input: Some(input),
                    partial_output,
                    ..MessageErrorSchemas::default()
                },
                branch,
            )
        }
        _ => Ok(()),
    }
}

fn validate_transforming_processor_message_errors(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    inputs: &nervix_models::ProcessorInputs,
    outputs: &ProcessorOutputs,
    branch: &BranchSelection,
    input_label: &str,
) -> Result<(), Report<RegistryError>> {
    let relay = processor_first_input_relay(domain, identifier, inputs, input_label)?;
    let input = schema_for_ack_model(domain, identifier, models, relay)?;
    for output in outputs.outputs() {
        validate_message_error_policy(
            domain,
            identifier,
            models,
            &output.message_error_policy,
            MessageErrorSchemas {
                input: Some(input),
                partial_output: Some(schema_for_ack_model(
                    domain,
                    identifier,
                    models,
                    &output.relay,
                )?),
                ..MessageErrorSchemas::default()
            },
            branch.branch(),
        )?;
    }
    Ok(())
}

fn validate_message_error_policy(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    policy: &MessageErrorPolicy,
    schemas: MessageErrorSchemas<'_>,
    expected_branch: Option<&Identifier>,
) -> Result<(), Report<RegistryError>> {
    let MessageErrorPolicy::Dlq { relay, assignments } = policy else {
        return Ok(());
    };
    let actual_branch = relay_declared_branch(domain, identifier, models, relay)?;
    if actual_branch != expected_branch {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "message-error relay '{}' uses branch {}, expected {}",
                relay,
                actual_branch.map_or("UNBRANCHED", Identifier::as_str),
                expected_branch.map_or("UNBRANCHED", Identifier::as_str),
            ),
        }));
    }

    let error_output = schema_for_ack_model(domain, identifier, models, relay)?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: assignments.clone(),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("error_output", "error_output"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("message-error SET is invalid: {reason}"),
        })
    })?;
    let mut bindings = vec![compile_binding_with_internal_schema(
        CompileBinding::writable(
            "error_output",
            arrow_schema_for_internal_schema(error_output),
        ),
        error_output,
    )];
    if let Some(input) = schemas.input {
        bindings.push(readonly_binding_for_internal_schema("input", input));
    }
    if let Some(left) = schemas.left {
        bindings.push(readonly_binding_for_internal_schema("left", left));
    }
    if let Some(right) = schemas.right {
        bindings.push(readonly_binding_for_internal_schema("right", right));
    }
    if let Some(partial_output) = schemas.partial_output {
        bindings.push(all_optional_binding_for_internal_schema(
            "partial_output",
            partial_output,
        ));
    }
    bindings.push(CompileBinding::readonly(
        "error",
        structured_message_error_arrow_schema(),
    ));
    let local_namespaces = HashSet::from_iter([
        "error_output".to_string(),
        "input".to_string(),
        "left".to_string(),
        "right".to_string(),
        "partial_output".to_string(),
        "error".to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &parsed,
        &local_namespaces,
        "message-error SET",
    )?);
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(error_output),
        schema_sensitivity_for_internal_schema(error_output),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            allow_header_reads: schemas.allow_header_reads,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("message-error SET compile failed: {}", error.message),
        })
    })?;
    Ok(())
}

fn all_optional_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    CompileBinding::readonly(
        namespace,
        StdArc::new(ArrowSchema::new(
            schema
                .fields
                .iter()
                .map(|field| {
                    ArrowField::new(
                        field.name.as_str(),
                        arrow_data_type_for_parse_as(&field.ty),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )),
    )
    .with_sensitivity(schema_sensitivity_for_internal_schema(schema))
}

fn structured_message_error_arrow_schema() -> StdArc<ArrowSchema> {
    StdArc::new(ArrowSchema::new(vec![
        ArrowField::new("reference", ArrowDataType::Utf8, false),
        ArrowField::new("code", ArrowDataType::Utf8, false),
        ArrowField::new("message", ArrowDataType::Utf8, false),
        ArrowField::new("operation", ArrowDataType::Utf8, false),
        ArrowField::new("operation_index", ArrowDataType::UInt32, true),
        ArrowField::new(
            "fields",
            ArrowDataType::List(StdArc::new(ArrowField::new(
                "item",
                ArrowDataType::Utf8,
                false,
            ))),
            false,
        ),
        ArrowField::new(
            "occurred_at",
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
    ]))
}

fn model_materialized_state_dependencies(model: &Model) -> &[MaterializedStateDependency] {
    match model {
        Model::Reingestor(model) => &model.materialized_state,
        Model::Inferencer(model) => &model.materialized_state,
        Model::WasmProcessor(model) => &model.materialized_state,
        Model::Junction(model) => &model.materialized_state,
        Model::Deduplicator(model) => &model.materialized_state,
        Model::Correlator(model) => &model.materialized_state,
        Model::Reorderer(model) => &model.materialized_state,
        Model::WindowProcessor(model) => &model.materialized_state,
        Model::Emitter(model) => &model.materialized_state,
        _ => &[],
    }
}

fn add_materialized_state_dependency_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    dependencies: &[MaterializedStateDependency],
) -> Result<(), Report<RegistryError>> {
    let mut declared = HashSet::default();
    for dependency in dependencies {
        if !declared.insert(dependency.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state relay '{}' is declared more than once",
                    dependency.relay
                ),
            }));
        }
        let relay = expect_kind(
            domain,
            identifier,
            models,
            indices,
            &dependency.relay,
            ModelKind::Relay,
        )?;
        ensure_stream_is_materialized(domain, identifier, models, &dependency.relay)?;
        validate_materialized_state_default(domain, identifier, models, dependency)?;
        graph.add_edge(relay, source, EdgeKind::RequiredBy);
    }
    Ok(())
}

fn validate_materialized_state_default(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    dependency: &MaterializedStateDependency,
) -> Result<(), Report<RegistryError>> {
    let MaterializedStatePolicy::Default(assignments) = &dependency.policy else {
        return Ok(());
    };
    let mut targets = HashSet::default();
    for assignment in assignments {
        if !targets.insert(assignment.target.field.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' assigns field '{}' more than once",
                    dependency.relay, assignment.target.field
                ),
            }));
        }
        let mut field_reference = None;
        assignment
            .value
            .visit_fields(&mut |field| field_reference = Some(field.clone()));
        if let Some(field) = field_reference {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' must be constant; field reference \
                     '{field:?}' is not allowed",
                    dependency.relay
                ),
            }));
        }
        if expression_contains_nondeterministic_or_side_effect_call(&assignment.value) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state DEFAULT for '{}' must use deterministic side-effect-free \
                     expressions",
                    dependency.relay
                ),
            }));
        }
    }

    let schema = schema_for_ack_model(domain, identifier, models, &dependency.relay)?;
    let output_schema = arrow_schema_for_internal_schema(schema);
    let construction = RouteConstruction {
        assignments: assignments.clone(),
        ..RouteConstruction::default()
    };
    let parsed = lower_set_only_route(&construction, output_schema.as_ref()).map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "materialized-state DEFAULT for '{}' is invalid: {reason}",
                dependency.relay
            ),
        })
    })?;
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        schema_sensitivity_for_internal_schema(schema),
        vec![writable_binding_for_internal_schema("output", schema)],
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "materialized-state DEFAULT for '{}' is invalid: {}",
                dependency.relay, error.message
            ),
        })
    })?;
    Ok(())
}

fn expression_contains_nondeterministic_or_side_effect_call(expression: &Expression) -> bool {
    match expression {
        Expression::Literal(_) | Expression::Field(_) => false,
        Expression::Unary { expression, .. } | Expression::Cast { expression, .. } => {
            expression_contains_nondeterministic_or_side_effect_call(expression)
        }
        Expression::Binary { left, right, .. } => {
            expression_contains_nondeterministic_or_side_effect_call(left)
                || expression_contains_nondeterministic_or_side_effect_call(right)
        }
        Expression::Call {
            function,
            arguments,
        } => {
            matches!(
                function.as_str().to_ascii_lowercase().as_str(),
                "now" | "uuid_v4" | "uuid_v7" | "write_header"
            ) || arguments
                .iter()
                .any(expression_contains_nondeterministic_or_side_effect_call)
        }
        Expression::Array(items) => items
            .iter()
            .any(expression_contains_nondeterministic_or_side_effect_call),
    }
}

fn validate_declared_materialized_state_references(
    domain: &Domain,
    identifier: &Identifier,
    model: &Model,
    dependencies: &[MaterializedStateDependency],
) -> Result<(), Report<RegistryError>> {
    let declared = if let Model::Generator(generator) = model {
        HashSet::from_iter([generator.materialized_relay.clone()])
    } else {
        dependencies
            .iter()
            .map(|dependency| dependency.relay.clone())
            .collect()
    };
    let mut referenced = HashSet::default();
    visit_model_expressions(model, &mut |expression| {
        expression.visit_fields(&mut |field| {
            if let nervix_models::FieldScope::RelayState { relay } = &field.scope {
                referenced.insert(relay.clone());
            }
        });
    });
    for relay in referenced {
        if !declared.contains(&relay) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "materialized-state reference 'relay_state.{relay}' has no matching USING \
                     MATERIALIZED STATE declaration"
                ),
            }));
        }
    }
    Ok(())
}

fn visit_model_expressions(model: &Model, visitor: &mut impl FnMut(&Expression)) {
    fn visit_inputs(
        inputs: &nervix_models::ProcessorInputs,
        visitor: &mut impl FnMut(&Expression),
    ) {
        for source_filter in inputs.where_clauses() {
            visitor(&source_filter.where_clause);
        }
    }
    fn visit_error_policy(policy: &MessageErrorPolicy, visitor: &mut impl FnMut(&Expression)) {
        if let MessageErrorPolicy::Dlq { assignments, .. } = policy {
            for assignment in assignments {
                visitor(&assignment.value);
            }
        }
    }
    fn visit_outputs(outputs: &ProcessorOutputs, visitor: &mut impl FnMut(&Expression)) {
        for output in outputs.outputs() {
            for assignment in &output.construction.assignments {
                visitor(&assignment.value);
            }
            if let Some(where_clause) = &output.construction.where_clause {
                visitor(where_clause);
            }
            for invocation in &output.construction.invocations {
                for argument in &invocation.arguments {
                    visitor(argument);
                }
            }
            visit_error_policy(&output.message_error_policy, visitor);
        }
    }
    fn visit_filter(filter: &Option<Expression>, visitor: &mut impl FnMut(&Expression)) {
        if let Some(filter) = filter {
            visitor(filter);
        }
    }

    match model {
        Model::Ingestor(model) => {
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Reingestor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Generator(model) => visit_outputs(&model.output_routes, visitor),
        Model::Inferencer(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for mapping in &model.inputs {
                visitor(&mapping.expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::WasmProcessor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Junction(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Deduplicator(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for expression in &model.deduplicate_on {
                visitor(expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Correlator(model) => {
            visit_inputs(&model.left, visitor);
            visit_inputs(&model.right, visitor);
            visitor(&model.correlate_where);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Reorderer(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            for expression in &model.order_by {
                visitor(expression);
            }
            visit_outputs(&model.output_routes, visitor);
        }
        Model::WindowProcessor(model) => {
            visit_inputs(&model.from, visitor);
            visit_filter(&model.filter_where, visitor);
            visit_outputs(&model.output_routes, visitor);
        }
        Model::Emitter(model) => {
            for assignment in &model.construction.assignments {
                visitor(&assignment.value);
            }
            if let Some(where_clause) = &model.construction.where_clause {
                visitor(where_clause);
            }
            for invocation in &model.construction.invocations {
                for argument in &invocation.arguments {
                    visitor(argument);
                }
            }
            let values = match &model.sink {
                EmitSink::ClickHouse { values, .. }
                | EmitSink::Postgres { values, .. }
                | EmitSink::MySql { values, .. }
                | EmitSink::MongoDb { values, .. }
                | EmitSink::Iceberg { values, .. } => Some(values.as_slice()),
                _ => None,
            };
            if let Some(values) = values {
                for value in values {
                    visitor(&value.expression);
                }
            }
            visit_error_policy(&model.error_policies.message, visitor);
        }
        _ => {}
    }
}

fn add_output_message_error_policy_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        add_message_error_policy_edges(
            domain,
            identifier,
            models,
            indices,
            graph,
            source,
            &output.message_error_policy,
        )?;
    }
    Ok(())
}

fn add_correlation_timeout_action_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    action: &CorrelationTimeoutAction,
) -> Result<(), Report<RegistryError>> {
    let CorrelationTimeoutAction::SendTo { relay } = action else {
        return Ok(());
    };
    let relay = expect_kind(domain, identifier, models, indices, relay, ModelKind::Relay)?;
    graph.add_edge(relay, source, EdgeKind::RequiredBy);
    graph.add_edge(source, relay, EdgeKind::CorrelationTimeout);
    Ok(())
}

fn expect_schema_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    referenced: &Identifier,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    match models.get(&RegistryKey::new(ModelKind::Schema, referenced.clone())) {
        Some(Model::Schema(schema)) => Ok(schema),
        Some(model) => Err(Report::new(RegistryError::InvalidReferenceKind {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: referenced.as_str().to_string(),
            actual_kind: model.kind().as_str(),
        })),
        None => Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: referenced.as_str().to_string(),
        })),
    }
}

fn expect_wire_schema_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    referenced: &Identifier,
) -> Result<&'a CreateWireSchemaStmt, Report<RegistryError>> {
    match models.get(&RegistryKey::new(ModelKind::WireSchema, referenced.clone())) {
        Some(Model::WireSchema(schema)) => Ok(schema),
        Some(model) => Err(Report::new(RegistryError::InvalidReferenceKind {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::WireSchema.as_str(),
            reference: referenced.as_str().to_string(),
            actual_kind: model.kind().as_str(),
        })),
        None => Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::WireSchema.as_str(),
            reference: referenced.as_str().to_string(),
        })),
    }
}

fn expect_codec_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    referenced: &Identifier,
) -> Result<&'a CreateCodec, Report<RegistryError>> {
    match models.get(&RegistryKey::new(ModelKind::Codec, referenced.clone())) {
        Some(Model::Codec(codec)) => Ok(codec),
        Some(model) => Err(Report::new(RegistryError::InvalidReferenceKind {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Codec.as_str(),
            reference: referenced.as_str().to_string(),
            actual_kind: model.kind().as_str(),
        })),
        None => Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Codec.as_str(),
            reference: referenced.as_str().to_string(),
        })),
    }
}

fn ensure_codec_supports_decoding(
    domain: &Domain,
    identifier: &Identifier,
    codec: &CreateCodec,
) -> Result<(), Report<RegistryError>> {
    if codec.wire_format.supports_decoding() {
        return Ok(());
    }

    Err(Report::new(RegistryError::InvalidModel {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "codec '{}' cannot be used for decoding because it does not declare an ON INGESTION \
             transformation",
            codec.name.as_str()
        ),
    }))
}

fn ensure_codec_supports_encoding(
    domain: &Domain,
    identifier: &Identifier,
    codec: &CreateCodec,
) -> Result<(), Report<RegistryError>> {
    if codec.wire_format.supports_encoding() {
        return Ok(());
    }

    Err(Report::new(RegistryError::InvalidModel {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "codec '{}' cannot be used for encoding because it does not declare an ON EMITTING \
             transformation",
            codec.name.as_str()
        ),
    }))
}

fn schema_for_codec_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    codec_id: &Identifier,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let codec = expect_codec_model(domain, identifier, models, codec_id)?;
    expect_schema_model(domain, identifier, models, &codec.schema)
}

fn schema_for_ack_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    relay_id: &Identifier,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let relay = match models.get(&RegistryKey::new(ModelKind::Relay, relay_id.clone())) {
        Some(Model::Relay(relay)) => relay,
        Some(model) => {
            return Err(Report::new(RegistryError::InvalidReferenceKind {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Relay.as_str(),
                reference: relay_id.as_str().to_string(),
                actual_kind: model.kind().as_str(),
            }));
        }
        None => {
            return Err(Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Relay.as_str(),
                reference: relay_id.as_str().to_string(),
            }));
        }
    };

    expect_schema_model(domain, identifier, models, &relay.schema)
}

fn schema_for_lookup_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    lookup_id: &Identifier,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let lookup = match models.get(&RegistryKey::new(ModelKind::Lookup, lookup_id.clone())) {
        Some(Model::Lookup(lookup)) => lookup,
        Some(model) => {
            return Err(Report::new(RegistryError::InvalidReferenceKind {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Lookup.as_str(),
                reference: lookup_id.as_str().to_string(),
                actual_kind: model.kind().as_str(),
            }));
        }
        None => {
            return Err(Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Lookup.as_str(),
                reference: lookup_id.as_str().to_string(),
            }));
        }
    };

    schema_for_codec_model(domain, identifier, models, &lookup.decode_using_codec)
}

#[derive(Debug, Clone, Copy)]
enum ProcessorOutputSchemaCompatibility {
    Compatible,
    Equal,
}

impl ProcessorOutputSchemaCompatibility {
    fn ensure(
        self,
        domain: &Domain,
        identifier: &Identifier,
        effective_schema: &CreateSchema,
        output_schema: &CreateSchema,
        relation: &str,
    ) -> Result<(), Report<RegistryError>> {
        match self {
            Self::Compatible => ensure_internal_schema_compatibility(
                domain,
                identifier,
                effective_schema,
                output_schema,
                relation,
            ),
            Self::Equal => ensure_equal_internal_schema(
                domain,
                identifier,
                effective_schema,
                output_schema,
                relation,
            ),
        }
    }
}

fn ensure_processor_outputs_declared(
    domain: &Domain,
    identifier: &Identifier,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    if outputs.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "processor must declare at least one TO destination".to_string(),
        }));
    }

    Ok(())
}

fn ensure_processor_output_flush_policies(
    domain: &Domain,
    identifier: &Identifier,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        let Some(policy) = output.flush_policy.as_ref() else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "TO output '{}' must declare FLUSH EACH or FLUSH IMMEDIATE",
                    output.relay.as_str()
                ),
            }));
        };
        if policy.flush_each.eq_ignore_ascii_case("IMMEDIATE") {
            if policy.max_batch_size.is_some() {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TO output '{}' FLUSH IMMEDIATE cannot declare MAX BATCH SIZE",
                        output.relay.as_str()
                    ),
                }));
            }
            continue;
        }
        humantime::parse_duration(&policy.flush_each).map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "invalid TO output '{}' FLUSH EACH duration '{}': {error}",
                    output.relay.as_str(),
                    policy.flush_each
                ),
            })
        })?;
        let Some(max_batch_size) = policy.max_batch_size.as_deref() else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "TO output '{}' FLUSH EACH requires MAX BATCH SIZE",
                    output.relay.as_str()
                ),
            }));
        };
        max_batch_size.parse::<ubyte::ByteUnit>().map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "invalid TO output '{}' MAX BATCH SIZE '{}': {error}",
                    output.relay.as_str(),
                    max_batch_size
                ),
            })
        })?;
    }
    Ok(())
}

fn add_processor_output_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, outputs)?;
    for output in outputs.outputs() {
        let output_node = expect_kind(
            domain,
            identifier,
            models,
            indices,
            &output.relay,
            ModelKind::Relay,
        )?;
        graph.add_edge(output_node, source, EdgeKind::RequiredBy);
        graph.add_edge(source, output_node, EdgeKind::SendsTo);
    }
    Ok(())
}

fn add_output_branch_dependency_edges(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
    source: NodeIndex,
    outputs: &ProcessorOutputs,
) -> Result<(), Report<RegistryError>> {
    for output in outputs.outputs() {
        let Some(branch_ref) = output.branch.as_ref().and_then(OutputBranch::branch) else {
            continue;
        };
        let branch = expect_kind(
            domain,
            identifier,
            models,
            indices,
            branch_ref,
            ModelKind::Branch,
        )?;
        graph.add_edge(branch, source, EdgeKind::RequiredBy);
    }
    Ok(())
}

fn validate_filter_where_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    filter_where: Option<&Expression>,
) -> Result<(), Report<RegistryError>> {
    let Some(filter_where) = filter_where else {
        return Ok(());
    };
    validate_where_program_for_internal_schemas(
        domain,
        identifier,
        models,
        input_schemas,
        branch_schema,
        filter_where,
        "FILTER WHERE",
        CompileOptions::default(),
    )
}

fn validate_ingestor_filter_where_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    filter_where: Option<&Expression>,
    source: &IngestSource,
) -> Result<(), Report<RegistryError>> {
    let Some(filter_where) = filter_where else {
        return Ok(());
    };
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(filter_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "__invalid_filter_target"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER WHERE is invalid: {reason}"),
        })
    })?;
    if program_uses_header_reads(&parsed.inner) && !ingest_source_supports_headers(source) {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                source.transport_label()
            ),
        }));
    }
    validate_where_program_for_internal_schemas(
        domain,
        identifier,
        models,
        input_schemas,
        branch_schema,
        filter_where,
        "FILTER WHERE",
        CompileOptions {
            allow_header_reads: true,
            ..CompileOptions::default()
        },
    )
}

fn validate_from_where_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    from_where: &[nervix_models::ProcessorInputWhere],
) -> Result<(), Report<RegistryError>> {
    validate_scoped_from_where_for_internal_schemas(
        domain,
        identifier,
        models,
        input_schemas,
        branch_schema,
        from_where,
        "input",
    )
}

fn validate_scoped_from_where_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    from_where: &[nervix_models::ProcessorInputWhere],
    input_namespace: &'static str,
) -> Result<(), Report<RegistryError>> {
    let mut seen_relays = HashSet::new();
    for source_filter in from_where {
        if !seen_relays.insert(source_filter.relay.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "FROM WHERE declared more than once for input relay '{}'",
                    source_filter.relay.as_str()
                ),
            }));
        }
        let Some((relay, schema)) = input_schemas
            .iter()
            .find(|(relay, _schema)| **relay == source_filter.relay)
            .copied()
        else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "FROM WHERE references unknown input relay '{}'",
                    source_filter.relay.as_str()
                ),
            }));
        };
        validate_where_program_for_scoped_internal_schemas(
            domain,
            identifier,
            models,
            &[(relay, schema)],
            branch_schema,
            &source_filter.where_clause,
            "FROM WHERE",
            CompileOptions::default(),
            input_namespace,
        )?;
    }
    Ok(())
}

fn validate_where_program_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    where_program: &Expression,
    clause_name: &str,
    compile_options: CompileOptions,
) -> Result<(), Report<RegistryError>> {
    validate_where_program_for_scoped_internal_schemas(
        domain,
        identifier,
        models,
        input_schemas,
        branch_schema,
        where_program,
        clause_name,
        compile_options,
        "input",
    )
}

fn validate_where_program_for_scoped_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    where_program: &Expression,
    clause_name: &str,
    compile_options: CompileOptions,
    input_namespace: &'static str,
) -> Result<(), Report<RegistryError>> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(where_program.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new(input_namespace, "__invalid_filter_target"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} is invalid: {reason}"),
        })
    })?;

    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let Some((_first_relay, first_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} requires at least one input relay"),
        }));
    };
    let mut bindings = vec![
        CompileBinding::writable(
            input_namespace,
            arrow_schema_for_internal_schema(first_schema),
        )
        .with_sensitivity(schema_sensitivity_for_internal_schema(first_schema)),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut input_relay_names = input_schemas
        .iter()
        .map(|(relay, _schema)| relay.as_str().to_string())
        .collect::<HashSet<_>>();
    input_relay_names.insert(input_namespace.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &input_relay_names,
        clause_name,
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(first_schema),
        schema_sensitivity_for_internal_schema(first_schema),
        bindings,
        compile_options,
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("{clause_name} compile failed: {}", error.message),
        })
    })?;

    Ok(())
}

fn effective_processor_output_filter_map_schema(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schemas: &[(&Identifier, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<CreateSchema, Report<RegistryError>> {
    let Some((_first_relay, first_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "processor output requires at least one input relay".to_string(),
        }));
    };
    let input_arrow_schema = arrow_schema_for_internal_schema(first_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_transforming_route(
        &output.construction,
        input_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("output route is invalid: {reason}"),
        })
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "FILTER-MAP may contain at most one WHERE clause".to_string(),
        }));
    }
    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;

    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", first_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let input_relay_names = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &input_relay_names,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(output_schema),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

fn ensure_processor_output_schemas(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    outputs: &ProcessorOutputs,
    input_schemas: &[(&Identifier, &CreateSchema)],
    branch_schema: Option<&CreateSchema>,
    relation: &str,
    compatibility: ProcessorOutputSchemaCompatibility,
) -> Result<(), Report<RegistryError>> {
    ensure_processor_outputs_declared(domain, identifier, outputs)?;
    for output in outputs.outputs() {
        let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
        let effective_schema = effective_processor_output_filter_map_schema(
            domain,
            identifier,
            models,
            input_schemas,
            output,
            output_schema,
            branch_schema,
        )?;
        compatibility.ensure(
            domain,
            identifier,
            &effective_schema,
            output_schema,
            relation,
        )?;
    }
    Ok(())
}

fn effective_emitter_filter_map_schema(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    emitter: &nervix_models::CreateEmitter,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
) -> Result<CreateSchema, Report<RegistryError>> {
    let codec_route = emitter.encode_using_codec.is_some();
    if !codec_route
        && (emitter.construction.inherit.is_some()
            || !emitter.construction.assignments.is_empty()
            || !emitter.construction.invocations.is_empty())
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "direct emitter routes support VALUES and WHERE only".to_string(),
        }));
    }
    if emitter.construction.is_empty() && !codec_route {
        return Ok(input_schema.clone());
    }
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = if codec_route {
        lower_transforming_route(
            &emitter.construction,
            input_arrow_schema.as_ref(),
            output_arrow_schema.as_ref(),
        )
    } else {
        lower_route_construction(
            &emitter.construction,
            SemanticNamespaces::new("input", "__invalid_direct_emitter_output"),
        )
    }
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("emitter route is invalid: {reason}"),
        })
    })?;
    let invokes_write_header = parsed
        .inner
        .invoke
        .iter()
        .any(|invocation| invocation.inner.function == FunctionName::WriteHeader);
    if invokes_write_header && !emit_sink_supports_headers(&emitter.sink) {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} emitters do not support write_header",
                emitter.sink.transport_label()
            ),
        }));
    }

    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut body_bindings = if codec_route {
        vec![
            readonly_binding_for_internal_schema("input", input_schema),
            writable_binding_for_internal_schema("output", output_schema),
        ]
    } else {
        vec![
            writable_binding_for_internal_schema("input", input_schema),
            readonly_binding_for_internal_schema("message", input_schema),
        ]
    };
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
    ]);
    body_bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "emitter route",
    )?);
    body_bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        body_bindings,
        CompileOptions {
            output_mode: if codec_route {
                OutputMode::ExplicitOnly
            } else {
                OutputMode::PassthroughByName
            },
            allow_sensitive_output: false,
            allow_header_writes: true,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

fn lookup_hash_map_literal_arg(args: &[SpannedExpr], index: usize) -> Result<&str, String> {
    let Some(arg) = args.get(index) else {
        return Err(format!(
            "LOOKUP_HASH_MAP expects 3 arguments, found {}",
            args.len()
        ));
    };
    match &arg.inner {
        Expr::Literal(Literal::String(value)) => Ok(value.as_str()),
        _ => Err(format!(
            "LOOKUP_HASH_MAP argument {} must be a string literal",
            index + 1
        )),
    }
}

fn lookup_hash_map_bindings(mut fields: Vec<(String, ArrowDataType)>) -> Vec<CompileBinding> {
    if fields.is_empty() {
        return Vec::new();
    }
    fields.sort_by(|left, right| left.0.cmp(&right.0));
    fields.dedup_by(|left, right| left.0 == right.0);
    vec![CompileBinding::internal_readonly(
        InternalFieldNamespace::LookupHashMap,
        StdArc::new(ArrowSchema::new(
            fields
                .into_iter()
                .map(|(name, data_type)| ArrowField::new(name, data_type, true))
                .collect::<Vec<_>>(),
        )),
    )]
}

type LookupHashMapRewriteResult = (
    nervix_nspl::vm_program::SpannedNode<Program>,
    Vec<(String, ArrowDataType)>,
);

fn rewrite_lookup_hash_map_program(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    parsed: &nervix_nspl::vm_program::SpannedNode<Program>,
) -> Result<LookupHashMapRewriteResult, Report<RegistryError>> {
    let mut next_field = 0usize;
    let mut calls = Vec::<(Identifier, String, String, String, ArrowDataType)>::new();
    let mut rewrite = |expr: &SpannedExpr| {
        rewrite_lookup_hash_map_expr(
            domain,
            identifier,
            models,
            expr,
            &mut calls,
            &mut next_field,
        )
    };
    let program = nervix_nspl::vm_program::SpannedNode {
        inner: Program {
            filter: parsed.inner.filter.as_ref().map(&mut rewrite).transpose()?,
            branch_filters: parsed
                .inner
                .branch_filters
                .iter()
                .map(&mut rewrite)
                .collect::<Result<Vec<_>, _>>()?,
            set: parsed
                .inner
                .set
                .iter()
                .map(|(field, expr)| rewrite(expr).map(|expr| (field.clone(), expr)))
                .collect::<Result<Vec<_>, _>>()?,
            invoke: parsed
                .inner
                .invoke
                .iter()
                .map(|invocation| {
                    Ok(nervix_nspl::vm_program::SpannedNode {
                        inner: nervix_nspl::vm_program::Invocation {
                            function: invocation.inner.function.clone(),
                            args: invocation
                                .inner
                                .args
                                .iter()
                                .map(&mut rewrite)
                                .collect::<Result<Vec<_>, Report<RegistryError>>>()?,
                        },
                        span: invocation.span,
                    })
                })
                .collect::<Result<Vec<_>, Report<RegistryError>>>()?,
        },
        span: parsed.span,
    };
    let fields = calls
        .into_iter()
        .map(|(_, _, _, generated_field, data_type)| (generated_field, data_type))
        .collect();
    Ok((program, fields))
}

fn rewrite_lookup_hash_map_expr(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    expr: &SpannedExpr,
    calls: &mut Vec<(Identifier, String, String, String, ArrowDataType)>,
    next_field: &mut usize,
) -> Result<SpannedExpr, Report<RegistryError>> {
    let inner = match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => expr.inner.clone(),
        Expr::Unary { op, expr: inner } => Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, inner, calls, next_field,
            )?),
        },
        Expr::Binary { op, left, right } => Expr::Binary {
            op: *op,
            left: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, left, calls, next_field,
            )?),
            right: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, right, calls, next_field,
            )?),
        },
        Expr::Cast {
            expr: inner,
            data_type,
        } => Expr::Cast {
            expr: Box::new(rewrite_lookup_hash_map_expr(
                domain, identifier, models, inner, calls, next_field,
            )?),
            data_type: data_type.clone(),
        },
        Expr::Call { function, args } => {
            if let FunctionName::LookupHashMap = function {
                if args.len() != 3 {
                    return Err(Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP expects 3 arguments, found {}",
                            args.len()
                        ),
                    }));
                }
                let lookup_name = lookup_hash_map_literal_arg(args, 0).map_err(|reason| {
                    Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason,
                    })
                })?;
                let lookup = Identifier::parse(lookup_name).map_err(|error| {
                    Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP hash map name '{lookup_name}' is invalid: {error}"
                        ),
                    })
                })?;
                let lookup_field = lookup_hash_map_literal_arg(args, 2)
                    .map_err(|reason| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason,
                        })
                    })?
                    .to_string();
                let lookup_schema = schema_for_lookup_model(domain, identifier, models, &lookup)?;
                let Some(schema_field) = lookup_schema
                    .fields
                    .iter()
                    .find(|field| field.name.as_str() == lookup_field)
                else {
                    return Err(Report::new(RegistryError::IncompatibleSchema {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "LOOKUP_HASH_MAP field '{}' is missing from hash map '{}' schema",
                            lookup_field,
                            lookup.as_str()
                        ),
                    }));
                };
                let key = format!("{:?}", args[1].inner);
                let data_type = arrow_data_type_for_parse_as(&schema_field.ty);
                let existing = calls
                    .iter()
                    .find(|(call_lookup, call_field, call_key, _, _)| {
                        call_lookup == &lookup && call_field == &lookup_field && call_key == &key
                    });
                let generated_field = if let Some((_, _, _, generated_field, _)) = existing {
                    generated_field.clone()
                } else {
                    let generated_field = format!("value_{}", *next_field);
                    *next_field += 1;
                    calls.push((
                        lookup,
                        lookup_field,
                        key,
                        generated_field.clone(),
                        data_type,
                    ));
                    generated_field
                };
                Expr::InternalFieldRef(InternalFieldRef {
                    namespace: InternalFieldNamespace::LookupHashMap,
                    field: generated_field,
                })
            } else {
                Expr::Call {
                    function: function.clone(),
                    args: args
                        .iter()
                        .map(|arg| {
                            rewrite_lookup_hash_map_expr(
                                domain, identifier, models, arg, calls, next_field,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                }
            }
        }
    };
    Ok(nervix_nspl::vm_program::SpannedNode {
        inner,
        span: expr.span,
    })
}

fn collect_expr_field_refs(expr: &SpannedExpr, refs: &mut Vec<(String, String)>) {
    match &expr.inner {
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
        Expr::FieldRef(field_ref) => {
            refs.push((field_ref.relay.clone(), field_ref.field.clone()));
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_field_refs(expr, refs);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_field_refs(left, refs);
            collect_expr_field_refs(right, refs);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_field_refs(arg, refs);
            }
        }
    }
}

fn expr_uses_header_read(expr: &SpannedExpr) -> bool {
    match &expr.inner {
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => expr_uses_header_read(expr),
        Expr::Binary { left, right, .. } => {
            expr_uses_header_read(left) || expr_uses_header_read(right)
        }
        Expr::Call { function, args } => {
            if let FunctionName::ReadHeader | FunctionName::ReadHeaders = function {
                true
            } else {
                args.iter().any(expr_uses_header_read)
            }
        }
    }
}

fn program_uses_header_reads(program: &Program) -> bool {
    program.filter.as_ref().is_some_and(expr_uses_header_read)
        || program.branch_filters.iter().any(expr_uses_header_read)
        || program
            .set
            .iter()
            .any(|(_field, expr)| expr_uses_header_read(expr))
        || program
            .invoke
            .iter()
            .flat_map(|invocation| &invocation.inner.args)
            .any(expr_uses_header_read)
}

fn collect_program_field_refs(program: &nervix_nspl::vm_program::Program) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    if let Some(filter) = &program.filter {
        collect_expr_field_refs(filter, &mut refs);
    }
    for branch_filter in &program.branch_filters {
        collect_expr_field_refs(branch_filter, &mut refs);
    }
    for (_field_ref, expr) in &program.set {
        collect_expr_field_refs(expr, &mut refs);
    }
    for invocation in &program.invoke {
        for arg in &invocation.inner.args {
            collect_expr_field_refs(arg, &mut refs);
        }
    }
    refs
}

fn referenced_materialized_stream_bindings(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    parsed: &nervix_nspl::vm_program::SpannedNode<nervix_nspl::vm_program::Program>,
    excluded_namespaces: &HashSet<String>,
    program_label: &str,
) -> Result<Vec<CompileBinding>, Report<RegistryError>> {
    let mut fields_by_stream = HashMap::<Identifier, HashSet<String>>::default();
    for (relay, field) in collect_program_field_refs(&parsed.inner) {
        if excluded_namespaces.contains(&relay) || relay == "metadata" || relay == BRANCH_NAMESPACE
        {
            continue;
        }
        let Some(relay_name) = relay.strip_prefix("relay_state.") else {
            continue;
        };
        let relay = Identifier::parse(relay_name).map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid materialized-state relay '{relay_name}': {error}"),
            })
        })?;
        let Some(Model::Relay(ack_model)) =
            models.get(&RegistryKey::new(ModelKind::Relay, relay.clone()))
        else {
            return Err(Report::new(RegistryError::MissingReference {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                expected_kind: ModelKind::Relay.as_str(),
                reference: relay.as_str().to_string(),
            }));
        };
        if ack_model.materialized_state.is_none() {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{} source relay '{}' must declare materialized state",
                    program_label,
                    relay.as_str()
                ),
            }));
        }
        fields_by_stream.entry(relay).or_default().insert(field);
    }

    let mut bindings = Vec::with_capacity(fields_by_stream.len());
    for (relay, fields) in fields_by_stream {
        let schema = schema_for_ack_model(domain, identifier, models, &relay)?;
        let projected_fields = schema
            .fields
            .iter()
            .filter(|field| fields.contains(field.name.as_str()))
            .map(arrow_field_for_schema_field)
            .collect::<Vec<_>>();
        let projected_sensitivity = SchemaSensitivity::from_sensitive_fields(
            schema
                .fields
                .iter()
                .filter(|field| field.sensitive && fields.contains(field.name.as_str()))
                .map(|field| field.name.as_str().to_string()),
        );
        bindings.push(
            CompileBinding::readonly(
                format!("relay_state.{}", relay.as_str()),
                StdArc::new(ArrowSchema::new(projected_fields)),
            )
            .with_sensitivity(projected_sensitivity),
        );
    }

    Ok(bindings)
}

fn emit_sink_supports_headers(sink: &EmitSink) -> bool {
    if let EmitSink::Kafka { .. }
    | EmitSink::Pulsar { .. }
    | EmitSink::RabbitMq { .. }
    | EmitSink::Nats { .. }
    | EmitSink::Sqs { .. } = sink
    {
        true
    } else {
        false
    }
}

fn ensure_stream_is_materialized(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    relay: &Identifier,
) -> Result<(), Report<RegistryError>> {
    let Some(Model::Relay(ack_model)) =
        models.get(&RegistryKey::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("missing relay '{}'", relay.as_str()),
        }));
    };
    if ack_model.materialized_state.is_none() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "generator source relay '{}' must declare materialized state",
                relay.as_str()
            ),
        }));
    }
    Ok(())
}

fn validate_generator_output(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    generator: &CreateGenerator,
    output: &ProcessorOutput,
) -> Result<(), Report<RegistryError>> {
    let output_schema = schema_for_ack_model(domain, identifier, models, &output.relay)?;
    let source_schema =
        schema_for_ack_model(domain, identifier, models, &generator.materialized_relay)?;
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_set_only_route(&output.construction, output_arrow_schema.as_ref()).map_err(
        |reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("generator output '{}' is invalid: {reason}", output.relay),
            })
        },
    )?;
    let allowed_state_namespace = format!("relay_state.{}", generator.materialized_relay);
    for (namespace, _field) in collect_program_field_refs(&parsed.inner) {
        if namespace.starts_with("relay_state.") && namespace != allowed_state_namespace {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "generator output '{}' references materialized state namespace '{namespace}', \
                     but only '{}' is declared",
                    output.relay, allowed_state_namespace
                ),
            }));
        }
    }

    let mut bindings = vec![
        writable_binding_for_internal_schema("output", output_schema),
        readonly_binding_for_internal_schema(&allowed_state_namespace, source_schema),
    ];
    if let Some(branch_schema) =
        relay_declared_branch_schema(domain, identifier, models, &generator.materialized_relay)?
    {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "generator output '{}' compile failed: {}",
                output.relay, error.message
            ),
        })
    })?;
    Ok(())
}

fn effective_ingestor_output_filter_map_schema(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    ingestor: &CreateIngestor,
    input_schema: &CreateSchema,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
) -> Result<CreateSchema, Report<RegistryError>> {
    let input_arrow_schema = arrow_schema_for_internal_schema(input_schema);
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let parsed = lower_transforming_route(
        &output.construction,
        input_arrow_schema.as_ref(),
        output_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("ingestor output route is invalid: {reason}"),
        })
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "FILTER-MAP may contain at most one WHERE clause".to_string(),
        }));
    }
    if program_uses_header_reads(&parsed.inner) && !ingest_source_supports_headers(&ingestor.source)
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{} ingestors do not support read_header or read_headers",
                ingestor.source.transport_label()
            ),
        }));
    }
    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;

    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(metadata_schema) = ingestor_filter_map_metadata_schema(&ingestor.source) {
        bindings.push(CompileBinding::readonly(
            "metadata",
            arrow_schema_for_internal_schema(&metadata_schema),
        ));
    }
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "metadata".to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(output_schema),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            allow_header_reads: true,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(output_schema.clone())
}

fn ingest_source_supports_headers(source: &IngestSource) -> bool {
    if let IngestSource::Endpoint { .. }
    | IngestSource::Http { .. }
    | IngestSource::Kafka { .. }
    | IngestSource::Nats { .. }
    | IngestSource::Pulsar { .. }
    | IngestSource::RabbitMq { .. }
    | IngestSource::Sqs { .. } = source
    {
        true
    } else {
        false
    }
}

fn ingestor_filter_map_metadata_schema(source: &IngestSource) -> Option<CreateSchema> {
    match source {
        IngestSource::Kafka { .. } => Some(CreateSchema {
            name: Identifier::parse("ingestor_metadata").expect("valid metadata schema name"),
            fields: vec![
                SchemaField {
                    name: Identifier::parse("topic").expect("valid metadata field"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: Identifier::parse("partition").expect("valid metadata field"),
                    ty: ParseAsType::I32,
                    optional: true,
                    sensitive: false,
                },
                SchemaField {
                    name: Identifier::parse("offset").expect("valid metadata field"),
                    ty: ParseAsType::I64,
                    optional: true,
                    sensitive: false,
                },
            ],
        }),
        _ => None,
    }
}

fn arrow_schema_for_internal_schema(schema: &CreateSchema) -> StdArc<ArrowSchema> {
    StdArc::new(ArrowSchema::new(
        schema
            .fields
            .iter()
            .map(arrow_field_for_schema_field)
            .collect::<Vec<_>>(),
    ))
}

fn arrow_field_for_schema_field(field: &SchemaField) -> ArrowField {
    ArrowField::new(
        field.name.as_str(),
        arrow_data_type_for_parse_as(&field.ty),
        field.optional,
    )
}

fn schema_sensitivity_for_internal_schema(schema: &CreateSchema) -> SchemaSensitivity {
    SchemaSensitivity::from_sensitive_fields(
        schema
            .fields
            .iter()
            .filter(|field| field.sensitive)
            .map(|field| field.name.as_str().to_string()),
    )
}

fn compile_binding_with_internal_schema(
    binding: CompileBinding,
    schema: &CreateSchema,
) -> CompileBinding {
    binding.with_sensitivity(schema_sensitivity_for_internal_schema(schema))
}

fn writable_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    compile_binding_with_internal_schema(
        CompileBinding::writable(namespace, arrow_schema_for_internal_schema(schema)),
        schema,
    )
}

fn readonly_binding_for_internal_schema(
    namespace: impl Into<String>,
    schema: &CreateSchema,
) -> CompileBinding {
    compile_binding_with_internal_schema(
        CompileBinding::readonly(namespace, arrow_schema_for_internal_schema(schema)),
        schema,
    )
}

fn arrow_data_type_for_parse_as(ty: &ParseAsType) -> ArrowDataType {
    match ty {
        ParseAsType::U8 => ArrowDataType::UInt8,
        ParseAsType::I8 => ArrowDataType::Int8,
        ParseAsType::U16 => ArrowDataType::UInt16,
        ParseAsType::I16 => ArrowDataType::Int16,
        ParseAsType::U32 => ArrowDataType::UInt32,
        ParseAsType::I32 => ArrowDataType::Int32,
        ParseAsType::U64 => ArrowDataType::UInt64,
        ParseAsType::I64 => ArrowDataType::Int64,
        ParseAsType::Bool => ArrowDataType::Boolean,
        ParseAsType::String => ArrowDataType::Utf8,
        ParseAsType::Datetime => {
            ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some("+00:00".into()))
        }
        ParseAsType::F32 => ArrowDataType::Float32,
        ParseAsType::F64 => ArrowDataType::Float64,
        ParseAsType::Array { element, len } => ArrowDataType::FixedSizeList(
            ArrowFieldRef::new(ArrowField::new(
                "item",
                arrow_data_type_for_parse_as(element),
                false,
            )),
            i32::try_from(*len).expect("array length must fit Arrow fixed-size list"),
        ),
        ParseAsType::Vec { element } => ArrowDataType::List(ArrowFieldRef::new(ArrowField::new(
            "item",
            arrow_data_type_for_parse_as(element),
            false,
        ))),
    }
}

fn ensure_internal_schema_compatibility(
    domain: &Domain,
    identifier: &Identifier,
    producer: &CreateSchema,
    consumer: &CreateSchema,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    ensure_internal_schema_compatibility_with_policy(
        domain,
        identifier,
        producer,
        consumer,
        relation,
        SensitivityCompatibility::Enforce,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SensitivityCompatibility {
    Enforce,
    AllowSensitiveProducer,
}

fn ensure_internal_schema_compatibility_with_policy(
    domain: &Domain,
    identifier: &Identifier,
    producer: &CreateSchema,
    consumer: &CreateSchema,
    relation: &str,
    sensitivity: SensitivityCompatibility,
) -> Result<(), Report<RegistryError>> {
    for consumer_field in &consumer.fields {
        let Some(producer_field) = producer
            .fields
            .iter()
            .find(|field| field.name == consumer_field.name)
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} requires producer schema '{}' to provide field '{}'",
                    producer.name.as_str(),
                    consumer_field.name.as_str()
                ),
            }));
        };

        if producer_field.ty != consumer_field.ty {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' type mismatch: producer {:?}, consumer {:?}",
                    consumer_field.name.as_str(),
                    producer_field.ty,
                    consumer_field.ty
                ),
            }));
        }
        if producer_field.optional != consumer_field.optional {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' optionality mismatch: producer {}, consumer {}",
                    consumer_field.name.as_str(),
                    producer_field.optional,
                    consumer_field.optional
                ),
            }));
        }
        if producer_field.sensitive
            && !consumer_field.sensitive
            && sensitivity == SensitivityCompatibility::Enforce
        {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{relation} field '{}' would store sensitive data in a non-sensitive output \
                     field; use leak_sensitive(...) to explicitly remove sensitivity",
                    consumer_field.name.as_str()
                ),
            }));
        }
    }

    for producer_field in &producer.fields {
        if consumer
            .fields
            .iter()
            .any(|field| field.name == producer_field.name)
        {
            continue;
        }

        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{relation} produces field '{}' that is not declared in consumer schema '{}'",
                producer_field.name.as_str(),
                consumer.name.as_str()
            ),
        }));
    }

    Ok(())
}

fn ensure_equal_internal_schema(
    domain: &Domain,
    identifier: &Identifier,
    left: &CreateSchema,
    right: &CreateSchema,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    if left.fields == right.fields {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{relation} requires equal internal schemas, but '{}' and '{}' differ",
            left.name.as_str(),
            right.name.as_str()
        ),
    }))
}

fn ensure_deduplicator_key_compiles(
    domain: &Domain,
    identifier: &Identifier,
    deduplicator: &CreateDeduplicator,
    input_schemas: &[(&Identifier, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let Some((_primary_relay, primary_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "deduplicator input requires at least one input relay".to_string(),
        }));
    };
    if deduplicator.deduplicate_on.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "DEDUPLICATE ON requires at least one expression".to_string(),
        }));
    }
    let assignments = deduplicator
        .deduplicate_on
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(Assignment {
                target: AssignmentTarget::bare(
                    Identifier::parse(&format!("deduplicate_key_{index}")).map_err(|error| {
                        Report::new(RegistryError::InvalidModel {
                            domain: domain.as_str().to_string(),
                            identifier: identifier.as_str().to_string(),
                            reason: format!("invalid deduplicate key target: {error}"),
                        })
                    })?,
                ),
                value: expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, Report<RegistryError>>>()?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "input"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON is invalid: {reason}"),
        })
    })?;
    let bindings = vec![writable_binding_for_internal_schema(
        "input",
        primary_schema,
    )];
    let key_types = infer_set_expr_types_for_bindings(&parsed, bindings).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("DEDUPLICATE ON compile failed: {}", error.message),
        })
    })?;
    if key_types.len() != deduplicator.deduplicate_on.len() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "DEDUPLICATE ON inferred a different number of key fields".to_string(),
        }));
    }
    Ok(())
}

fn validate_correlator(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    correlator: &CreateCorrelator,
    left_schemas: &[(&Identifier, &CreateSchema)],
    right_schemas: &[(&Identifier, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    humantime::parse_duration(&correlator.max_time).map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "invalid correlator MAX TIME '{}': {error}",
                correlator.max_time
            ),
        })
    })?;
    ensure_processor_output_flush_policies(domain, identifier, &correlator.output_routes)?;

    validate_correlate_where_for_internal_schemas(
        domain,
        identifier,
        correlator,
        left_schemas,
        right_schemas,
    )?;

    let Some((_left_relay, left_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left timeout requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right timeout requires at least one input relay".to_string(),
        }));
    };
    validate_correlator_timeout_action(
        domain,
        identifier,
        models,
        left_schema,
        &correlator.timeout_policy.left,
        "correlator left timeout",
    )?;
    validate_correlator_timeout_action(
        domain,
        identifier,
        models,
        right_schema,
        &correlator.timeout_policy.right,
        "correlator right timeout",
    )
}

fn validate_correlate_where_for_internal_schemas(
    domain: &Domain,
    identifier: &Identifier,
    correlator: &CreateCorrelator,
    left_schemas: &[(&Identifier, &CreateSchema)],
    right_schemas: &[(&Identifier, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            where_clause: Some(correlator.correlate_where.clone()),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new(
            "__invalid_correlator_bare_read",
            "__invalid_correlator_target",
        ),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("CORRELATE WHERE is invalid: {reason}"),
        })
    })?;
    let Some((_first_relay, first_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left input requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right input requires at least one input relay".to_string(),
        }));
    };
    let bindings = vec![
        writable_binding_for_internal_schema("left", first_schema),
        readonly_binding_for_internal_schema("right", right_schema),
    ];

    compile_program_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(first_schema),
        schema_sensitivity_for_internal_schema(first_schema),
        bindings,
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("CORRELATE WHERE compile failed: {}", error.message),
        })
    })?;

    Ok(())
}

fn validate_correlator_output(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    left_schemas: &[(&Identifier, &CreateSchema)],
    right_schemas: &[(&Identifier, &CreateSchema)],
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
) -> Result<(), Report<RegistryError>> {
    if output.construction.assignments.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' must declare SET assignments",
                output.relay.as_str()
            ),
        }));
    }
    let parsed = lower_route_construction(
        &output.construction,
        SemanticNamespaces::new("__invalid_correlator_bare_read", "output"),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' is invalid: {}",
                output.relay.as_str(),
                reason
            ),
        })
    })?;
    if !parsed.inner.branch_filters.is_empty()
        || !parsed.inner.invoke.is_empty()
        || parsed.inner.set.is_empty()
    {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' must contain SET assignments and may contain WHERE",
                output.relay.as_str()
            ),
        }));
    }

    let Some((_left_relay, left_schema)) = left_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator left input requires at least one input relay".to_string(),
        }));
    };
    let Some((_right_relay, right_schema)) = right_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "correlator right input requires at least one input relay".to_string(),
        }));
    };
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let mut bindings = vec![
        readonly_binding_for_internal_schema("left", left_schema),
        readonly_binding_for_internal_schema("right", right_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let local_namespaces = HashSet::from_iter([
        "left".to_string(),
        "right".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &parsed,
        &local_namespaces,
        "correlator output",
    )?);
    let compiled = compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema.clone(),
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "correlator TO output '{}' compile failed: {}",
                output.relay.as_str(),
                error.message
            ),
        })
    })?;

    for field in compiled.output_schema.fields() {
        let Some(target) = output_arrow_schema
            .fields()
            .iter()
            .find(|target| target.name() == field.name())
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' assigns unknown field '{}.{}'",
                    output.relay.as_str(),
                    output.relay.as_str(),
                    field.name()
                ),
            }));
        };
        if target.data_type() != field.data_type() {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' field '{}' type mismatch: expression {:?}, schema \
                     {:?}",
                    output.relay.as_str(),
                    field.name(),
                    field.data_type(),
                    target.data_type()
                ),
            }));
        }
    }

    for target in output_arrow_schema.fields() {
        if !target.is_nullable()
            && !compiled
                .output_schema
                .fields()
                .iter()
                .any(|field| field.name() == target.name())
        {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "correlator TO output '{}' does not assign required field '{}.{}'",
                    output.relay.as_str(),
                    output.relay.as_str(),
                    target.name()
                ),
            }));
        }
    }

    Ok(())
}

fn validate_correlator_timeout_action(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    input_schema: &CreateSchema,
    action: &CorrelationTimeoutAction,
    relation: &str,
) -> Result<(), Report<RegistryError>> {
    let CorrelationTimeoutAction::SendTo { relay } = action else {
        return Ok(());
    };
    let target_schema = schema_for_ack_model(domain, identifier, models, relay)?;
    ensure_internal_schema_compatibility(domain, identifier, input_schema, target_schema, relation)
}

fn ensure_inferencer_input_mappings(
    domain: &Domain,
    identifier: &Identifier,
    processor: &CreateInferencer,
    input_schemas: &[(&Identifier, &CreateSchema)],
) -> Result<(), Report<RegistryError>> {
    let Some((_relay, input_schema)) = input_schemas.first() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "inferencer requires at least one input relay".to_string(),
        }));
    };
    for mapping in &processor.inputs {
        let target = Identifier::parse("mapped_tensor").map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("invalid inferencer mapping target: {error}"),
            })
        })?;
        let parsed = lower_route_construction(
            &RouteConstruction {
                assignments: vec![Assignment {
                    target: AssignmentTarget::bare(target),
                    value: mapping.expression.clone(),
                }],
                ..RouteConstruction::default()
            },
            SemanticNamespaces::new("input", "input"),
        )
        .map_err(|reason| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("inference input '{}' is invalid: {reason}", mapping.tensor),
            })
        })?;
        let inferred = infer_set_expr_types_for_bindings(
            &parsed,
            [writable_binding_for_internal_schema("input", input_schema)],
        )
        .map_err(|error| {
            Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "inference input '{}' compile failed: {}",
                    mapping.tensor, error.message
                ),
            })
        })?;
        let Some((_field, actual_type, actual_nullable)) = inferred.first() else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("inference input '{}' produced no value", mapping.tensor),
            }));
        };
        let expected_type = arrow_data_type_for_parse_as(&mapping.schema.message_type());
        if actual_type != &expected_type || *actual_nullable {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "inference input '{}' requires {:?} non-null, found {:?}{}",
                    mapping.tensor,
                    expected_type,
                    actual_type,
                    if *actual_nullable {
                        " nullable"
                    } else {
                        " non-null"
                    }
                ),
            }));
        }
    }

    Ok(())
}

fn validate_inferencer_output_filter_map(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    output: &ProcessorOutput,
    output_schema: &CreateSchema,
    branch_schema: Option<&CreateSchema>,
    processor: &CreateInferencer,
) -> Result<(), Report<RegistryError>> {
    let inner_output_schema = processor.inner_output_schema(domain, identifier)?;
    let output_arrow_schema = arrow_schema_for_internal_schema(output_schema);
    let generated_arrow_schema = arrow_schema_for_internal_schema(&inner_output_schema);
    let parsed = lower_generated_route(
        &output.construction,
        output_arrow_schema.as_ref(),
        generated_arrow_schema.as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("inferencer output route is invalid: {reason}"),
        })
    })?;
    if !parsed.inner.branch_filters.is_empty() {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "FILTER-MAP may contain at most one WHERE clause".to_string(),
        }));
    }
    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("generated", &inner_output_schema),
        writable_binding_for_internal_schema("output", output_schema),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(readonly_binding_for_internal_schema(
            BRANCH_NAMESPACE,
            branch_schema,
        ));
    }
    let mut local_namespaces = HashSet::new();
    local_namespaces.insert("generated".to_string());
    local_namespaces.insert("output".to_string());
    local_namespaces.insert(BRANCH_NAMESPACE.to_string());
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "FILTER-MAP",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));

    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_arrow_schema,
        schema_sensitivity_for_internal_schema(output_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("FILTER-MAP compile failed: {}", error.message),
        })
    })?;

    Ok(())
}

trait InferencerRegistrySchema {
    fn inner_output_schema(
        &self,
        domain: &Domain,
        identifier: &Identifier,
    ) -> Result<CreateSchema, Report<RegistryError>>;
}

impl InferencerRegistrySchema for CreateInferencer {
    fn inner_output_schema(
        &self,
        domain: &Domain,
        identifier: &Identifier,
    ) -> Result<CreateSchema, Report<RegistryError>> {
        let fields = self
            .output_schema
            .iter()
            .map(|declaration| {
                let name = Identifier::parse(&declaration.tensor).map_err(|error| {
                    Report::new(RegistryError::InvalidModel {
                        domain: domain.as_str().to_string(),
                        identifier: identifier.as_str().to_string(),
                        reason: format!(
                            "ONNX output tensor '{}' cannot be referenced as '{}.{}': {}",
                            declaration.tensor, INNER_OUTPUT_NAMESPACE, declaration.tensor, error
                        ),
                    })
                })?;
                Ok(SchemaField {
                    name,
                    ty: declaration.schema.message_type(),
                    optional: false,
                    sensitive: false,
                })
            })
            .collect::<Result<Vec<_>, Report<RegistryError>>>()?;
        Ok(CreateSchema {
            name: Identifier::parse(INNER_OUTPUT_NAMESPACE)
                .expect("public inferencer namespace must be a valid identifier"),
            fields,
        })
    }
}

fn ensure_lookup_key_field_exists(
    domain: &Domain,
    identifier: &Identifier,
    lookup: &CreateLookup,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    if schema
        .fields
        .iter()
        .any(|field| field.name == lookup.key_field)
    {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "LOOKUP KEY field '{}' is missing from schema '{}'",
            lookup.key_field.as_str(),
            schema.name.as_str()
        ),
    }))
}

fn ensure_ingestor_timestamp_source(
    domain: &Domain,
    identifier: &Identifier,
    ingestor: &CreateIngestor,
    schema: &CreateSchema,
) -> Result<(), Report<RegistryError>> {
    match &ingestor.timestamp_source {
        None | Some(IngestTimestampSource::Now) => Ok(()),
        Some(IngestTimestampSource::At(timestamp_field)) => {
            let Some(field) = schema
                .fields
                .iter()
                .find(|field| field.name == *timestamp_field)
            else {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TIMESTAMP field '{}' is missing from schema '{}'",
                        timestamp_field.as_str(),
                        schema.name.as_str()
                    ),
                }));
            };

            if let ParseAsType::Datetime = field.ty {
                return Ok(());
            }

            Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "TIMESTAMP field '{}' must use DATETIME in schema '{}'",
                    timestamp_field.as_str(),
                    schema.name.as_str()
                ),
            }))
        }
    }
}

fn relay_declared_branch<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    relay: &Identifier,
) -> Result<Option<&'a Identifier>, Report<RegistryError>> {
    let Some(Model::Relay(relay_model)) =
        models.get(&RegistryKey::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Relay.as_str(),
            reference: relay.as_str().to_string(),
        }));
    };
    Ok(relay_model.branching.branch())
}

fn relay_declared_branch_schema<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    relay: &Identifier,
) -> Result<Option<&'a CreateSchema>, Report<RegistryError>> {
    let Some(Model::Relay(relay_model)) =
        models.get(&RegistryKey::new(ModelKind::Relay, relay.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Relay.as_str(),
            reference: relay.as_str().to_string(),
        }));
    };
    let Some(branch_ref) = relay_model.branching.branch() else {
        return Ok(None);
    };
    let branch = branch_model(domain, identifier, models, branch_ref)?;
    let Some(Model::Schema(schema)) =
        models.get(&RegistryKey::new(ModelKind::Schema, branch.schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch.schema.as_str().to_string(),
        }));
    };
    Ok(Some(schema))
}

fn ensure_output_branch(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    output: &ProcessorOutput,
    input_schema: &CreateSchema,
    output_schema: &CreateSchema,
    incoming_branch: Option<&Identifier>,
) -> Result<(), Report<RegistryError>> {
    let target_branch = relay_declared_branch(domain, identifier, models, &output.relay)?;
    let Some(branch_action) = output.branch.as_ref() else {
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' must declare BRANCHED BY or UNBRANCHED",
                output.relay.as_str()
            ),
        }));
    };

    let (branch_ref, assignments) = match branch_action {
        OutputBranch::Unbranched => {
            if let Some(target_branch) = target_branch {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TO output '{}' is BRANCHED BY '{}', but the route declares UNBRANCHED",
                        output.relay.as_str(),
                        target_branch.as_str()
                    ),
                }));
            }
            return Ok(());
        }
        OutputBranch::BranchedBy {
            branch,
            assignments,
        } => (branch, assignments),
    };

    if target_branch != Some(branch_ref) {
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' must use its exact declared branch '{}'",
                output.relay.as_str(),
                target_branch.map_or("UNBRANCHED", Identifier::as_str)
            ),
        }));
    }

    if incoming_branch == Some(branch_ref) {
        if assignments.is_empty() {
            return Ok(());
        }
        return Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "TO output '{}' preserves branch '{}' and cannot construct a new key",
                output.relay.as_str(),
                branch_ref.as_str()
            ),
        }));
    }

    let branch = branch_model(domain, identifier, models, branch_ref)?;
    let branch_schema = schema_model(domain, identifier, models, &branch.schema)?;
    let parsed = lower_branch_construction(
        assignments,
        arrow_schema_for_internal_schema(branch_schema).as_ref(),
        arrow_schema_for_internal_schema(output_schema).as_ref(),
        arrow_schema_for_internal_schema(input_schema).as_ref(),
    )
    .map_err(|reason| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("branch construction is invalid: {reason}"),
        })
    })?;
    let original_parsed = parsed.clone();
    let (parsed, lookup_fields) =
        rewrite_lookup_hash_map_program(domain, identifier, models, &parsed)?;
    let mut bindings = vec![
        readonly_binding_for_internal_schema("input", input_schema),
        readonly_binding_for_internal_schema("output", output_schema),
        readonly_binding_for_internal_schema("message", output_schema),
        writable_binding_for_internal_schema(BRANCH_NAMESPACE, branch_schema),
    ];
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "message".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    bindings.extend(referenced_materialized_stream_bindings(
        domain,
        identifier,
        models,
        &original_parsed,
        &local_namespaces,
        "branch SET",
    )?);
    bindings.extend(lookup_hash_map_bindings(lookup_fields));
    compile_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        arrow_schema_for_internal_schema(branch_schema),
        schema_sensitivity_for_internal_schema(branch_schema),
        bindings,
        CompileOptions {
            output_mode: OutputMode::ExplicitOnly,
            ..CompileOptions::default()
        },
    )
    .map_err(|error| {
        Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!("branch SET compile failed: {}", error.message),
        })
    })?;
    Ok(())
}

fn validate_vhost_hostnames(
    domain: &Domain,
    models: &HashMap<RegistryKey, Model>,
) -> Result<(), Report<RegistryError>> {
    let mut owners = HashMap::<String, Identifier>::new();

    for (key, model) in models {
        let Model::Vhost(vhost) = model else {
            continue;
        };
        let identifier = &key.identifier;

        let mut seen_in_vhost = HashSet::new();
        for hostname in &vhost.hostnames {
            let normalized = hostname.to_ascii_lowercase();
            if !seen_in_vhost.insert(normalized.clone()) {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!("hostname '{hostname}' is listed more than once"),
                }));
            }

            if let Some(existing) = owners.insert(normalized, identifier.clone()) {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "hostname '{hostname}' is already assigned to vhost '{}'",
                        existing.as_str()
                    ),
                }));
            }
        }
    }

    Ok(())
}

fn validate_endpoint_paths(
    domain: &Domain,
    models: &HashMap<RegistryKey, Model>,
) -> Result<(), Report<RegistryError>> {
    let mut routes = HashMap::<(Identifier, String), Identifier>::new();

    for (key, model) in models {
        let Model::Endpoint(endpoint) = model else {
            continue;
        };
        let identifier = &key.identifier;

        let key = (endpoint.on_vhost.clone(), endpoint.path.clone());
        if let Some(existing) = routes.insert(key, identifier.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "path '{}' is already assigned to endpoint '{}' on vhost '{}'",
                    endpoint.path,
                    existing.as_str(),
                    endpoint.on_vhost.as_str()
                ),
            }));
        }
    }

    Ok(())
}

fn infer_stream_branchings(
    domain: &Domain,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let producer_ids = SortedSet::from_unsorted(
        models
            .iter()
            .filter_map(|(key, model)| {
                matches!(
                    model,
                    Model::Generator(_)
                        | Model::Inferencer(_)
                        | Model::Ingestor(_)
                        | Model::Reingestor(_)
                        | Model::Deduplicator(_)
                        | Model::Correlator(_)
                        | Model::Junction(_)
                        | Model::WindowProcessor(_)
                )
                .then_some(key.identifier.clone())
            })
            .collect::<Vec<_>>(),
    )
    .into_vec();

    let mut changed = true;
    while changed {
        changed = false;

        for producer_id in &producer_ids {
            let Some(model) = models
                .get(&RegistryKey::new(ModelKind::Generator, producer_id.clone()))
                .or_else(|| {
                    models.get(&RegistryKey::new(
                        ModelKind::Inferencer,
                        producer_id.clone(),
                    ))
                })
                .or_else(|| {
                    models.get(&RegistryKey::new(
                        ModelKind::WasmProcessor,
                        producer_id.clone(),
                    ))
                })
                .or_else(|| models.get(&RegistryKey::new(ModelKind::Ingestor, producer_id.clone())))
                .or_else(|| {
                    models.get(&RegistryKey::new(
                        ModelKind::Reingestor,
                        producer_id.clone(),
                    ))
                })
                .or_else(|| {
                    models.get(&RegistryKey::new(
                        ModelKind::Deduplicator,
                        producer_id.clone(),
                    ))
                })
                .or_else(|| models.get(&RegistryKey::new(ModelKind::Junction, producer_id.clone())))
                .or_else(|| {
                    models.get(&RegistryKey::new(
                        ModelKind::WindowProcessor,
                        producer_id.clone(),
                    ))
                })
            else {
                continue;
            };

            let proposed = match model {
                Model::Generator(generator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &generator.branched_by,
                    )?;
                    Some(
                        generator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect::<Vec<_>>(),
                    )
                }
                Model::Inferencer(processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &processor.branched_by,
                    )?;
                    Some(
                        processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::WasmProcessor(processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &processor.branched_by,
                    )?;
                    Some(
                        processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Ingestor(ingestor) => Some(resolved_output_branches(
                    domain,
                    producer_id,
                    models,
                    &ingestor.output_routes,
                )?),
                Model::Reingestor(reingestor) => Some(resolved_output_branches(
                    domain,
                    producer_id,
                    models,
                    &reingestor.output_routes,
                )?),
                Model::Deduplicator(deduplicator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &deduplicator.branched_by,
                    )?;
                    Some(
                        deduplicator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Correlator(correlator) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &correlator.branched_by,
                    )?;
                    Some(
                        correlator
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::Junction(junction) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &junction.branched_by,
                    )?;
                    Some(
                        junction
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                Model::WindowProcessor(window_processor) => {
                    let branching = resolved_branch_selection(
                        domain,
                        producer_id,
                        models,
                        &window_processor.branched_by,
                    )?;
                    Some(
                        window_processor
                            .output_routes
                            .relays()
                            .cloned()
                            .map(|target| (target, branching.clone()))
                            .collect(),
                    )
                }
                _ => None,
            };

            let Some(proposed_targets) = proposed else {
                continue;
            };

            for (target_relay, branching) in proposed_targets {
                changed |= assign_stream_branching(
                    domain,
                    producer_id,
                    &target_relay,
                    branching,
                    indices,
                    graph,
                )?;
            }
        }
    }

    Ok(())
}

fn validate_processing_branch_selections(
    domain: &Domain,
    models: &HashMap<RegistryKey, Model>,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    // Normal processors are branch-preserving: they must run under an explicit
    // concrete relay branch. Only REINGESTOR may change branching and
    // only EMITTER may fan in across branches, so every processor source checked
    // here must already have an inferred branch shape.
    for (key, model) in models {
        match model {
            Model::Generator(generator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "generator",
                    models,
                    indices,
                    graph,
                };
                check.matches_relay(&generator.branched_by, &generator.materialized_relay)?;
                check.matches_outputs(&generator.branched_by, &generator.output_routes)?;
            }
            Model::Inferencer(processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "inferencer",
                    models,
                    indices,
                    graph,
                };
                for from_relay in processor.from.relays() {
                    check.matches_relay(&processor.branched_by, from_relay)?;
                }
                for dependency in &processor.materialized_state {
                    check.matches_relay(&processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&processor.branched_by, &processor.output_routes)?;
            }
            Model::WasmProcessor(processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "wasm processor",
                    models,
                    indices,
                    graph,
                };
                for from_relay in processor.from.relays() {
                    check.matches_relay(&processor.branched_by, from_relay)?;
                }
                for dependency in &processor.materialized_state {
                    check.matches_relay(&processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&processor.branched_by, &processor.output_routes)?;
            }
            Model::Deduplicator(deduplicator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "deduplicator",
                    models,
                    indices,
                    graph,
                };
                for from_relay in deduplicator.from.relays() {
                    check.matches_relay(&deduplicator.branched_by, from_relay)?;
                }
                for dependency in &deduplicator.materialized_state {
                    check.matches_relay(&deduplicator.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&deduplicator.branched_by, &deduplicator.output_routes)?;
            }
            Model::Correlator(correlator) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "correlator",
                    models,
                    indices,
                    graph,
                };
                for relay in correlator.left.relays() {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                for relay in correlator.right.relays() {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &correlator.timeout_policy.left
                {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                if let CorrelationTimeoutAction::SendTo { relay } = &correlator.timeout_policy.right
                {
                    check.matches_relay(&correlator.branched_by, relay)?;
                }
                for dependency in &correlator.materialized_state {
                    check.matches_relay(&correlator.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&correlator.branched_by, &correlator.output_routes)?;
            }
            Model::Reorderer(reorderer) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "reorderer",
                    models,
                    indices,
                    graph,
                };
                for from_relay in reorderer.from.relays() {
                    check.matches_relay(&reorderer.branched_by, from_relay)?;
                }
                for dependency in &reorderer.materialized_state {
                    check.matches_relay(&reorderer.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&reorderer.branched_by, &reorderer.output_routes)?;
            }
            Model::Reingestor(reingestor) => {
                for from_relay in reingestor.from.relays() {
                    ensure_processing_source_branching(
                        domain,
                        &key.identifier,
                        "reingestor",
                        from_relay,
                        indices,
                        graph,
                    )?;
                }
                if let Some(from_relay) = reingestor.from.first() {
                    for dependency in &reingestor.materialized_state {
                        ensure_relays_have_same_branch(
                            domain,
                            &key.identifier,
                            "reingestor materialized state",
                            from_relay,
                            &dependency.relay,
                            &indices,
                            &graph,
                        )?;
                    }
                }
            }
            Model::WindowProcessor(window_processor) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "window processor",
                    models,
                    indices,
                    graph,
                };
                for from_relay in window_processor.from.relays() {
                    check.matches_relay(&window_processor.branched_by, from_relay)?;
                }
                for dependency in &window_processor.materialized_state {
                    check.matches_relay(&window_processor.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(
                    &window_processor.branched_by,
                    &window_processor.output_routes,
                )?;
            }
            Model::Junction(junction) => {
                let check = ProcessorBranchingCheck {
                    domain,
                    identifier: &key.identifier,
                    model_kind: "junction",
                    models,
                    indices,
                    graph,
                };
                for from_relay in junction.from.relays() {
                    check.matches_relay(&junction.branched_by, from_relay)?;
                }
                for dependency in &junction.materialized_state {
                    check.matches_relay(&junction.branched_by, &dependency.relay)?;
                }
                check.matches_outputs(&junction.branched_by, &junction.output_routes)?;
            }
            Model::Emitter(emitter) => {
                for dependency in &emitter.materialized_state {
                    ensure_relays_have_same_branch(
                        domain,
                        &key.identifier,
                        "emitter materialized state",
                        &emitter.from_relay,
                        &dependency.relay,
                        &indices,
                        &graph,
                    )?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

struct ProcessorBranchingCheck<'a> {
    domain: &'a Domain,
    identifier: &'a Identifier,
    model_kind: &'a str,
    models: &'a HashMap<RegistryKey, Model>,
    indices: &'a HashMap<RegistryKey, NodeIndex>,
    graph: &'a DiGraph<ActiveNode, EdgeKind>,
}

impl ProcessorBranchingCheck<'_> {
    fn matches_outputs(
        &self,
        branched_by: &BranchSelection,
        outputs: &ProcessorOutputs,
    ) -> Result<(), Report<RegistryError>> {
        for output in outputs.outputs() {
            self.matches_relay(branched_by, &output.relay)?;
        }
        Ok(())
    }

    fn matches_relay(
        &self,
        branched_by: &BranchSelection,
        relay: &Identifier,
    ) -> Result<(), Report<RegistryError>> {
        let declared =
            resolved_branch_selection(self.domain, self.identifier, self.models, branched_by)?;
        let relay_branching =
            if let Some(relay_branching) = relay_branching(self.indices, self.graph, relay) {
                relay_branching
            } else if declared.is_empty() {
                return Ok(());
            } else {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: self.domain.as_str().to_string(),
                    identifier: self.identifier.as_str().to_string(),
                    reason: format!(
                        "{} '{}' requires relay '{}' to have branch fields ({})",
                        self.model_kind,
                        self.identifier.as_str(),
                        relay.as_str(),
                        format_branched_by(&declared.fields),
                    ),
                }));
            };

        if relay_branching.fields.is_empty() && !declared.fields.is_empty() {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: self.domain.as_str().to_string(),
                identifier: self.identifier.as_str().to_string(),
                reason: format!(
                    "{} '{}' requires relay '{}' to have branch fields ({})",
                    self.model_kind,
                    self.identifier.as_str(),
                    relay.as_str(),
                    format_branched_by(&declared.fields),
                ),
            }));
        }

        if relay_branching.fields != declared.fields {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: self.domain.as_str().to_string(),
                identifier: self.identifier.as_str().to_string(),
                reason: format!(
                    "{} '{}' branch fields ({}) do not match relay '{}' branch fields ({})",
                    self.model_kind,
                    self.identifier.as_str(),
                    format_branched_by(&declared.fields),
                    relay.as_str(),
                    format_branched_by(&relay_branching.fields),
                ),
            }));
        }

        if relay_branching.branch == declared.branch {
            return Ok(());
        }

        Err(Report::new(RegistryError::IncompatibleSchema {
            domain: self.domain.as_str().to_string(),
            identifier: self.identifier.as_str().to_string(),
            reason: format!(
                "{} '{}' branch name '{}' does not match relay '{}' branch name '{}'",
                self.model_kind,
                self.identifier.as_str(),
                format_branch_name(declared.branch.as_ref()),
                relay.as_str(),
                format_branch_name(relay_branching.branch.as_ref()),
            ),
        }))
    }
}

fn ensure_processing_source_branching(
    domain: &Domain,
    identifier: &Identifier,
    model_kind: &str,
    relay: &Identifier,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let Some(index) = indices.get(&RegistryKey::new(ModelKind::Relay, relay.clone())) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: "relay",
            reference: relay.as_str().to_string(),
        }));
    };
    let Some(node) = graph.node_weight(*index) else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: "relay",
            reference: relay.as_str().to_string(),
        }));
    };
    if node.effective_branching.is_some() {
        return Ok(());
    }

    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{} '{}' requires relay '{}' to declare BRANCHED BY or UNBRANCHED",
            model_kind,
            identifier.as_str(),
            relay.as_str(),
        ),
    }))
}

fn ensure_relays_have_same_branch(
    domain: &Domain,
    identifier: &Identifier,
    context: &str,
    left: &Identifier,
    right: &Identifier,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
) -> Result<(), Report<RegistryError>> {
    let left_branching = relay_branching(indices, graph, left);
    let right_branching = relay_branching(indices, graph, right);
    let compatible = match (&left_branching, &right_branching) {
        (None, None) => true,
        (Some(left), Some(right)) => left.branch == right.branch && left.fields == right.fields,
        _ => false,
    };
    if compatible {
        return Ok(());
    }
    Err(Report::new(RegistryError::IncompatibleSchema {
        domain: domain.as_str().to_string(),
        identifier: identifier.as_str().to_string(),
        reason: format!(
            "{context} requires relay '{}' and materialized relay '{}' to use the same exact \
             branch",
            left, right
        ),
    }))
}

fn relay_branching(
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &DiGraph<ActiveNode, EdgeKind>,
    relay: &Identifier,
) -> Option<ResolvedBranching> {
    let index = indices.get(&RegistryKey::new(ModelKind::Relay, relay.clone()))?;
    let node = graph.node_weight(*index)?;
    let Model::Relay(relay) = node.config.as_ref() else {
        return None;
    };
    Some(ResolvedBranching {
        branch: relay.branching.branch().cloned(),
        schema: node.effective_branching_schema.clone(),
        fields: node.effective_branching.clone()?,
    })
}

#[derive(Clone)]
struct ResolvedBranching {
    branch: Option<Identifier>,
    schema: Option<Identifier>,
    fields: Vec<Identifier>,
}

impl ResolvedBranching {
    fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

trait BranchReference {
    fn branch_ref(&self) -> Option<&Identifier>;
}

impl BranchReference for BranchSelection {
    fn branch_ref(&self) -> Option<&Identifier> {
        self.branch()
    }
}

impl BranchReference for OutputBranch {
    fn branch_ref(&self) -> Option<&Identifier> {
        self.branch()
    }
}

fn resolved_output_branches(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    outputs: &ProcessorOutputs,
) -> Result<Vec<(Identifier, ResolvedBranching)>, Report<RegistryError>> {
    outputs
        .outputs()
        .map(|output| {
            let Some(branch) = output.branch.as_ref() else {
                return Err(Report::new(RegistryError::InvalidModel {
                    domain: domain.as_str().to_string(),
                    identifier: identifier.as_str().to_string(),
                    reason: format!(
                        "TO output '{}' must declare BRANCHED BY or UNBRANCHED",
                        output.relay.as_str()
                    ),
                }));
            };
            Ok((
                output.relay.clone(),
                resolved_branch_selection(domain, identifier, models, branch)?,
            ))
        })
        .collect()
}

fn resolved_branch_selection(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    branched_by: &dyn BranchReference,
) -> Result<ResolvedBranching, Report<RegistryError>> {
    let Some(branch_ref) = branched_by.branch_ref() else {
        return Ok(ResolvedBranching {
            branch: None,
            schema: None,
            fields: Vec::new(),
        });
    };
    let branch = branch_model(domain, identifier, models, branch_ref)?;
    Ok(ResolvedBranching {
        branch: Some(branch_ref.clone()),
        schema: Some(branch.schema.clone()),
        fields: branching_schema_fields(domain, identifier, models, &branch.schema)?,
    })
}

fn branch_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    branch_ref: &Identifier,
) -> Result<&'a CreateBranch, Report<RegistryError>> {
    let Some(Model::Branch(branch)) =
        models.get(&RegistryKey::new(ModelKind::Branch, branch_ref.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Branch.as_str(),
            reference: branch_ref.as_str().to_string(),
        }));
    };
    Ok(branch)
}

fn schema_model<'a>(
    domain: &Domain,
    identifier: &Identifier,
    models: &'a HashMap<RegistryKey, Model>,
    schema_ref: &Identifier,
) -> Result<&'a CreateSchema, Report<RegistryError>> {
    let Some(Model::Schema(schema)) =
        models.get(&RegistryKey::new(ModelKind::Schema, schema_ref.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: schema_ref.as_str().to_string(),
        }));
    };
    Ok(schema)
}

fn model_branch_selection(model: &Model) -> Option<&dyn BranchReference> {
    match model {
        Model::Generator(generator) => Some(&generator.branched_by),
        Model::Inferencer(processor) => Some(&processor.branched_by),
        Model::WasmProcessor(processor) => Some(&processor.branched_by),
        Model::Deduplicator(deduplicator) => Some(&deduplicator.branched_by),
        Model::Correlator(correlator) => Some(&correlator.branched_by),
        Model::Junction(junction) => Some(&junction.branched_by),
        Model::Reorderer(reorderer) => Some(&reorderer.branched_by),
        Model::WindowProcessor(window_processor) => Some(&window_processor.branched_by),
        _ => None,
    }
}

fn branching_schema_fields(
    domain: &Domain,
    identifier: &Identifier,
    models: &HashMap<RegistryKey, Model>,
    branch_schema: &Identifier,
) -> Result<Vec<Identifier>, Report<RegistryError>> {
    let Some(Model::Schema(schema)) =
        models.get(&RegistryKey::new(ModelKind::Schema, branch_schema.clone()))
    else {
        return Err(Report::new(RegistryError::MissingReference {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            expected_kind: ModelKind::Schema.as_str(),
            reference: branch_schema.as_str().to_string(),
        }));
    };
    Ok(schema
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect())
}

fn assign_stream_branching(
    domain: &Domain,
    producer: &Identifier,
    relay: &Identifier,
    branching: ResolvedBranching,
    indices: &HashMap<RegistryKey, NodeIndex>,
    graph: &mut DiGraph<ActiveNode, EdgeKind>,
) -> Result<bool, Report<RegistryError>> {
    let index = *indices
        .get(&RegistryKey::new(ModelKind::Relay, relay.clone()))
        .expect("stream node must exist in graph");
    let node = graph
        .node_weight_mut(index)
        .expect("stream node must exist in graph");

    match &node.effective_branching {
        None => {
            node.effective_branching = Some(branching.fields);
            node.effective_branching_schema = branching.schema;
            Ok(true)
        }
        Some(existing) if *existing == branching.fields => {
            let Model::Relay(relay_model) = node.config.as_ref() else {
                unreachable!("stream branching may only be assigned to a relay")
            };
            if relay_model.branching.branch() != branching.branch.as_ref() {
                return Err(Report::new(RegistryError::IncompatibleSchema {
                    domain: domain.as_str().to_string(),
                    identifier: producer.as_str().to_string(),
                    reason: format!(
                        "stream '{}' receives conflicting branch names: existing '{}' vs producer \
                         '{}' with '{}'",
                        relay.as_str(),
                        format_branch_name(relay_model.branching.branch()),
                        producer.as_str(),
                        format_branch_name(branching.branch.as_ref()),
                    ),
                }));
            }
            if node.effective_branching_schema.is_none() && branching.schema.is_some() {
                node.effective_branching_schema = branching.schema;
                return Ok(true);
            }
            Ok(false)
        }
        Some(existing) => Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: producer.as_str().to_string(),
            reason: format!(
                "stream '{}' receives conflicting branch fields: existing ({}) vs producer '{}' \
                 with ({})",
                relay.as_str(),
                format_branched_by(existing),
                producer.as_str(),
                format_branched_by(&branching.fields),
            ),
        })),
    }
}

fn format_branch_name(branch: Option<&Identifier>) -> &str {
    branch.map(Identifier::as_str).unwrap_or("UNBRANCHED")
}

fn format_branched_by(branched_by: &[Identifier]) -> String {
    if branched_by.is_empty() {
        "(none)".to_string()
    } else {
        branched_by
            .iter()
            .map(Identifier::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn ensure_codec_schema_compatibility(
    domain: &Domain,
    identifier: &Identifier,
    wire_format: &CodecWireFormat,
    wire_schema: Option<&CreateWireSchemaStmt>,
    schema: &CreateSchema,
    encoding_rules: &[CodecEncodingRule],
) -> Result<(), Report<RegistryError>> {
    let rfc3339_fields =
        ensure_supported_codec_encoding_rules(domain, identifier, schema, encoding_rules)?;
    match (wire_format, wire_schema) {
        (CodecWireFormat::Json, Some(CreateWireSchemaStmt::Json(json))) => {
            ensure_wire_field_set_matches(
                domain,
                identifier,
                &json
                    .fields
                    .iter()
                    .map(|field| WireFieldCompatibility {
                        name: field.name.as_str(),
                        optional: field.optional,
                        wire_type: field.ty.as_ref().to_string(),
                        compatibility: WireTypeCompatibility::Json(field.ty),
                    })
                    .collect::<Vec<_>>(),
                schema,
                "json",
                &rfc3339_fields,
            )
        }
        (CodecWireFormat::Cbor, Some(CreateWireSchemaStmt::Cbor(cbor))) => {
            ensure_wire_field_set_matches(
                domain,
                identifier,
                &cbor
                    .fields
                    .iter()
                    .map(|field| WireFieldCompatibility {
                        name: field.name.as_str(),
                        optional: field.optional,
                        wire_type: field.ty.as_ref().to_string(),
                        compatibility: WireTypeCompatibility::Json(field.ty),
                    })
                    .collect::<Vec<_>>(),
                schema,
                "cbor",
                &rfc3339_fields,
            )
        }
        (CodecWireFormat::Avro, Some(CreateWireSchemaStmt::Avro(avro))) => {
            ensure_wire_field_set_matches(
                domain,
                identifier,
                &avro
                    .fields
                    .iter()
                    .map(|field| WireFieldCompatibility {
                        name: field.name.as_str(),
                        optional: field.optional,
                        wire_type: field.ty.as_ref().to_string(),
                        compatibility: WireTypeCompatibility::Avro(field.ty),
                    })
                    .collect::<Vec<_>>(),
                schema,
                "avro",
                &rfc3339_fields,
            )
        }
        (
            CodecWireFormat::JaqNative {
                transformations, ..
            },
            None,
        ) if transformations.has_any() => Ok(()),
        (CodecWireFormat::Protobuf(config), None) if config.transformations.has_any() => Ok(()),
        (
            CodecWireFormat::JaqNative {
                transformations, ..
            },
            None,
        ) => Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: if transformations.has_any() {
                "JAQ-native codec is invalid".to_string()
            } else {
                "JAQ-native codec must declare a JAQ transformation".to_string()
            },
        })),
        (CodecWireFormat::Json, Some(CreateWireSchemaStmt::Avro(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares JSON wire format but references an avro wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Json, Some(CreateWireSchemaStmt::Cbor(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares JSON wire format but references a cbor wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Cbor, Some(CreateWireSchemaStmt::Json(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares CBOR wire format but references a json wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Cbor, Some(CreateWireSchemaStmt::Avro(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares CBOR wire format but references an avro wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Avro, Some(CreateWireSchemaStmt::Json(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares AVRO wire format but references a json wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Avro, Some(CreateWireSchemaStmt::Cbor(_))) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "codec declares AVRO wire format but references a cbor wire schema"
                    .to_string(),
            }))
        }
        (CodecWireFormat::Json, None) => Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "codec declares JSON wire format but does not reference a json wire schema"
                .to_string(),
        })),
        (CodecWireFormat::Cbor, None) => Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "codec declares CBOR wire format but does not reference a cbor wire schema"
                .to_string(),
        })),
        (CodecWireFormat::Avro, None) => Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "codec declares AVRO wire format but does not reference an avro wire schema"
                .to_string(),
        })),
        (CodecWireFormat::JaqNative { .. }, Some(_)) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: "JAQ-native codec must not reference a wire schema".to_string(),
            }))
        }
        (CodecWireFormat::Protobuf(config), None) => {
            Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: if config.transformations.has_any() {
                    "protobuf codec is invalid".to_string()
                } else {
                    "protobuf codec must declare a JAQ transformation".to_string()
                },
            }))
        }
        (CodecWireFormat::Protobuf(_), Some(_)) => Err(Report::new(RegistryError::InvalidModel {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: "protobuf codec must not reference a wire schema".to_string(),
        })),
    }
}

fn ensure_supported_codec_encoding_rules(
    domain: &Domain,
    identifier: &Identifier,
    schema: &CreateSchema,
    encoding_rules: &[CodecEncodingRule],
) -> Result<HashSet<Identifier>, Report<RegistryError>> {
    let mut rfc3339_fields = HashSet::new();
    for rule in encoding_rules {
        if rule.encoding != CodecEncoding::Rfc3339 {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!("unsupported codec encoding rule {rule:?}"),
            }));
        }

        let Some(schema_field) = schema
            .fields
            .iter()
            .find(|schema_field| schema_field.name == rule.field)
        else {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "codec encoding rule references unknown schema field '{}'",
                    rule.field.as_str()
                ),
            }));
        };

        if schema_field.ty != ParseAsType::Datetime {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "codec encoding rule field '{}' must be DATETIME, found {:?}",
                    rule.field.as_str(),
                    schema_field.ty
                ),
            }));
        }

        if !rfc3339_fields.insert(rule.field.clone()) {
            return Err(Report::new(RegistryError::InvalidModel {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "duplicate codec encoding rule for field '{}'",
                    rule.field.as_str()
                ),
            }));
        }
    }
    Ok(rfc3339_fields)
}

struct WireFieldCompatibility<'a> {
    name: &'a str,
    optional: bool,
    wire_type: String,
    compatibility: WireTypeCompatibility,
}

#[derive(Clone, Copy)]
enum WireTypeCompatibility {
    Json(JsonType),
    Avro(AvroType),
}

fn ensure_wire_field_set_matches(
    domain: &Domain,
    identifier: &Identifier,
    wire_fields: &[WireFieldCompatibility<'_>],
    schema: &CreateSchema,
    wire_kind: &str,
    rfc3339_fields: &HashSet<Identifier>,
) -> Result<(), Report<RegistryError>> {
    for schema_field in &schema.fields {
        let Some(wire_field) = wire_fields
            .iter()
            .find(|wire_field| wire_field.name == schema_field.name.as_str())
        else {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} wire schema is missing field '{}'",
                    schema_field.name.as_str()
                ),
            }));
        };

        if !wire_field.compatibility.supports(
            &schema_field.ty,
            rfc3339_fields.contains(&schema_field.name),
        ) {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} field '{}' type mismatch: wire {}, internal {:?}",
                    schema_field.name.as_str(),
                    wire_field.wire_type,
                    schema_field.ty
                ),
            }));
        }
        if wire_field.optional != schema_field.optional {
            return Err(Report::new(RegistryError::IncompatibleSchema {
                domain: domain.as_str().to_string(),
                identifier: identifier.as_str().to_string(),
                reason: format!(
                    "{wire_kind} field '{}' optionality mismatch: wire {}, internal {}",
                    schema_field.name.as_str(),
                    wire_field.optional,
                    schema_field.optional
                ),
            }));
        }
    }

    if wire_fields.len() != schema.fields.len() {
        return Err(Report::new(RegistryError::IncompatibleSchema {
            domain: domain.as_str().to_string(),
            identifier: identifier.as_str().to_string(),
            reason: format!(
                "{wire_kind} wire schema field set must exactly match internal schema '{}'",
                schema.name.as_str()
            ),
        }));
    }

    Ok(())
}

impl WireTypeCompatibility {
    fn supports(self, ty: &ParseAsType, encodes_datetime_as_rfc3339: bool) -> bool {
        match self {
            Self::Json(wire) => json_type_matches_parse_as(wire, ty, encodes_datetime_as_rfc3339),
            Self::Avro(wire) => avro_type_matches_parse_as(wire, ty, encodes_datetime_as_rfc3339),
        }
    }
}

fn json_type_matches_parse_as(
    wire: JsonType,
    ty: &ParseAsType,
    encodes_datetime_as_rfc3339: bool,
) -> bool {
    match wire {
        JsonType::String => {
            *ty == ParseAsType::String
                || encodes_datetime_as_rfc3339 && *ty == ParseAsType::Datetime
        }
        JsonType::Number => *ty == ParseAsType::F32 || *ty == ParseAsType::F64,
        JsonType::Integer => parse_as_is_integer(ty),
        JsonType::Boolean => *ty == ParseAsType::Bool,
        JsonType::Array => parse_as_is_list(ty),
        JsonType::Object
        | JsonType::Null
        | JsonType::U8
        | JsonType::I8
        | JsonType::U16
        | JsonType::I16
        | JsonType::U32
        | JsonType::I32
        | JsonType::U64
        | JsonType::I64
        | JsonType::Datetime
        | JsonType::F32
        | JsonType::F64 => false,
    }
}

fn avro_type_matches_parse_as(
    wire: AvroType,
    ty: &ParseAsType,
    encodes_datetime_as_rfc3339: bool,
) -> bool {
    match wire {
        AvroType::Boolean => *ty == ParseAsType::Bool,
        AvroType::Int => *ty == ParseAsType::I32,
        AvroType::Long => *ty == ParseAsType::I64,
        AvroType::Float => *ty == ParseAsType::F32,
        AvroType::Double => *ty == ParseAsType::F64,
        AvroType::String => {
            *ty == ParseAsType::String
                || encodes_datetime_as_rfc3339 && *ty == ParseAsType::Datetime
        }
        AvroType::Array => parse_as_is_list(ty),
        AvroType::Null
        | AvroType::Bytes
        | AvroType::Record
        | AvroType::Enum
        | AvroType::Map
        | AvroType::Fixed => false,
    }
}

fn parse_as_is_list(ty: &ParseAsType) -> bool {
    if let ParseAsType::Array { .. } = ty {
        return true;
    }
    if let ParseAsType::Vec { .. } = ty {
        return true;
    }
    false
}

fn parse_as_is_integer(ty: &ParseAsType) -> bool {
    matches!(
        ty,
        ParseAsType::U8
            | ParseAsType::I8
            | ParseAsType::U16
            | ParseAsType::I16
            | ParseAsType::U32
            | ParseAsType::I32
            | ParseAsType::U64
            | ParseAsType::I64
    )
}

fn runtime_changes_for_domain(
    domain: &Domain,
    graph: Option<ActiveGraph>,
    current_models: &HashMap<RegistryKey, Model>,
    candidate_models: &HashMap<RegistryKey, Model>,
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
            ingestor: ingestor.clone(),
        });
    }

    for ingestor in &candidate_ingestor_ids {
        let Some(Model::Ingestor(ingestor_model)) =
            candidate_models.get(&RegistryKey::new(ModelKind::Ingestor, ingestor.clone()))
        else {
            continue;
        };
        let source_ref = match &ingestor_model.source {
            IngestSource::Http { client, .. } => client,
            IngestSource::Kinesis { client, .. } => client,
            IngestSource::Kafka { client, .. } => client,
            IngestSource::Pulsar { client, .. } => client,
            IngestSource::Prometheus { client, .. } => client,
            IngestSource::RabbitMq { client, .. } => client,
            IngestSource::RedisPubSub { client, .. } => client,
            IngestSource::Mqtt { client, .. } => client,
            IngestSource::Nats { client, .. } => client,
            IngestSource::ZeroMq { client, .. } => client,
            IngestSource::Sqs { client, .. } => client,
            IngestSource::Websockets { client, .. } => client,
            IngestSource::Endpoint { endpoint, .. } => endpoint,
        };
        let source_kind = match &ingestor_model.source {
            IngestSource::Http { .. }
            | IngestSource::Kinesis { .. }
            | IngestSource::Kafka { .. }
            | IngestSource::Pulsar { .. }
            | IngestSource::Prometheus { .. }
            | IngestSource::RabbitMq { .. }
            | IngestSource::RedisPubSub { .. }
            | IngestSource::Mqtt { .. }
            | IngestSource::Nats { .. }
            | IngestSource::ZeroMq { .. }
            | IngestSource::Sqs { .. }
            | IngestSource::Websockets { .. } => ModelKind::Client,
            IngestSource::Endpoint { .. } => ModelKind::Endpoint,
        };
        let Some(source_model) =
            candidate_models.get(&RegistryKey::new(source_kind, source_ref.clone()))
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

fn has_required_by_cycle(graph: &DiGraph<ActiveNode, EdgeKind>) -> bool {
    let mut required_by_graph = DiGraph::<(), ()>::new();
    let mut node_map = HashMap::new();

    for index in graph.node_indices() {
        node_map.insert(index, required_by_graph.add_node(()));
    }

    for edge in graph.edge_references() {
        if *edge.weight() != EdgeKind::RequiredBy {
            continue;
        }
        let source = *node_map
            .get(&edge.source())
            .expect("required-by source node must exist");
        let target = *node_map
            .get(&edge.target())
            .expect("required-by target node must exist");
        required_by_graph.add_edge(source, target, ());
    }

    is_cyclic_directed(&required_by_graph)
}

fn ensure_drop_targets_are_not_in_use(
    domain: &Domain,
    graph: &ActiveGraph,
    drops_in_batch: &HashSet<RegistryKey>,
) -> Result<(), Report<RegistryError>> {
    for key in drops_in_batch {
        let Some(index) = graph.indices.get(key).copied() else {
            continue;
        };

        let mut blockers = graph
            .graph
            .edges_directed(index, Direction::Outgoing)
            .filter_map(|blocker_index| {
                if *blocker_index.weight() != EdgeKind::RequiredBy {
                    return None;
                }
                let blocker = graph
                    .graph
                    .node_weight(blocker_index.target())
                    .expect("outgoing blocker node must exist")
                    .clone();
                (!drops_in_batch.contains(&blocker.key())).then_some(blocker.identifier)
            })
            .collect::<Vec<_>>();
        blockers.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        blockers.dedup_by(|a, b| a.as_str() == b.as_str());

        if let Some(blocker) = blockers.first() {
            return Err(Report::new(RegistryError::DeleteInUse {
                domain: domain.as_str().to_string(),
                identifier: key.identifier.as_str().to_string(),
                blockers: blocker.as_str().to_string(),
            }));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use ahash::HashMap;
    use fjall::Database;
    use nervix_dataflow_graph::DataflowEdgeKind;
    use nervix_models::{
        AckMode, AlterRelay, AlterRelayOperation, Assignment, AssignmentTarget,
        AssignmentTargetScope, BranchSelection, ClientConfigEntry, CodecEncoding,
        CodecEncodingRule, CodecJaqFormat, CodecJaqTransformations, CodecProtobufConfig,
        CodecWireFormat, CorrelationTimeoutAction, CorrelationTimeoutPolicy, CorrelatorMatchPolicy,
        CreateBranch, CreateClientKafka, CreateCodec, CreateCorrelator, CreateDeduplicator,
        CreateEmitter, CreateGenerator, CreateIngestor, CreateJunction, CreateReingestor,
        CreateRelay, CreateSchema, CreateVhost, CreateWasmProcessor, CreateWindowProcessor,
        CreateWireSchema, CreateWireSchemaStmt, Domain, DomainSchedule, DropModel, EmitSink,
        ErrorPolicies, Expression, FieldReference, FieldScope, GeneralErrorPolicy, Identifier,
        IngestSource, IngestTimestampSource, Inheritance, JsonType, KafkaConfigEntry,
        KafkaIngestMode, KafkaOffsetMode, MaterializedRelayState, MessageErrorPolicy, Model,
        ModelKind, MqttIngestMode, MqttQos, MqttSession, OutputBranch, ParseAsType,
        ProcessorInputs, ProcessorOutput, ProcessorOutputs, RelayBranching, ScheduledNode,
        SchemaField, WindowBound, WireSchemaField,
    };

    use super::{ModelStorage, Registry, RegistryError, RuntimeChange};

    fn temp_db_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("nervix-registry-test-{nanos}"))
    }

    fn sample_transport_model(name: &str) -> Model {
        Model::ClientKafka(CreateClientKafka {
            name: Identifier::parse(name).expect("valid identifier"),
            mount: None,
            config: vec![KafkaConfigEntry {
                key: "bootstrap.servers".to_string(),
                value: "localhost:9092".to_string(),
            }],
        })
    }

    fn identifier(raw: &str) -> Identifier {
        Identifier::parse(raw).expect("valid identifier")
    }

    fn branch_name_for_relay(relay: &str) -> Identifier {
        identifier(&format!("by_{relay}"))
    }

    fn branched_by(relay: &str, fields: &[&str]) -> OutputBranch {
        OutputBranch::BranchedBy {
            branch: branch_name_for_relay(relay),
            assignments: fields
                .iter()
                .map(|field| Assignment {
                    target: AssignmentTarget {
                        scope: AssignmentTargetScope::Bare,
                        field: identifier(field),
                    },
                    value: Expression::Field(FieldReference::scoped(
                        FieldScope::Message,
                        identifier(field),
                    )),
                })
                .collect(),
        }
    }

    fn with_output_branch(mut outputs: ProcessorOutputs, branch: OutputBranch) -> ProcessorOutputs {
        for output in &mut outputs.routes {
            output.branch = Some(branch.clone());
        }
        outputs
    }

    fn with_processor_branching(mut model: Model) -> Model {
        match &mut model {
            Model::Deduplicator(processor) => {
                let branch = processor
                    .from
                    .relays()
                    .first()
                    .expect("processor helper requires at least one input");
                processor.branched_by =
                    BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
            }
            Model::Correlator(processor) => {
                let branch = processor
                    .left
                    .relays()
                    .first()
                    .expect("processor helper requires at least one input");
                processor.branched_by =
                    BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
            }
            Model::Junction(processor) => {
                let branch = processor
                    .from
                    .relays()
                    .first()
                    .expect("processor helper requires at least one input");
                processor.branched_by =
                    BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
            }
            Model::WindowProcessor(processor) => {
                let branch = processor
                    .from
                    .relays()
                    .first()
                    .expect("processor helper requires at least one input");
                processor.branched_by =
                    BranchSelection::branched_by(branch_name_for_relay(branch.as_str()));
            }
            _ => panic!("model is not a branch-preserving processor"),
        }
        model
    }

    fn with_inherit_all(mut outputs: ProcessorOutputs) -> ProcessorOutputs {
        for output in &mut outputs.routes {
            output.construction.inherit = Some(Inheritance::All);
        }
        outputs
    }

    fn unbranched_transforming_outputs(relay: &str) -> ProcessorOutputs {
        with_output_branch(
            with_inherit_all(ProcessorOutputs::single(identifier(relay)))
                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
            OutputBranch::Unbranched,
        )
    }

    fn branch_schema(name: &str, fields: &[&str]) -> Model {
        Model::Schema(CreateSchema {
            name: identifier(name),
            fields: fields
                .iter()
                .map(|field| SchemaField {
                    name: identifier(field),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                })
                .collect(),
        })
    }

    fn branch(name: &str, schema: &str) -> Model {
        Model::Branch(CreateBranch {
            name: identifier(name),
            schema: identifier(schema),
            ttl: "5m".to_string(),
            eviction: None,
        })
    }

    fn branch_for_relay(relay: &str, schema: &str) -> Model {
        Model::Branch(CreateBranch {
            name: branch_name_for_relay(relay),
            schema: identifier(schema),
            ttl: "5m".to_string(),
            eviction: None,
        })
    }

    fn branch_schema_with_types(name: &str, fields: &[(&str, ParseAsType)]) -> Model {
        Model::Schema(CreateSchema {
            name: identifier(name),
            fields: fields
                .iter()
                .map(|(field, ty)| SchemaField {
                    name: identifier(field),
                    ty: ty.clone(),
                    optional: false,
                    sensitive: false,
                })
                .collect(),
        })
    }

    fn schema(name: &str) -> Model {
        Model::Schema(CreateSchema {
            name: Identifier::parse(name).expect("valid identifier"),
            fields: vec![SchemaField {
                name: Identifier::parse("value").expect("valid identifier"),
                ty: nervix_models::ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        })
    }

    fn wire_schema(name: &str) -> Model {
        Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
            name: Identifier::parse(name).expect("valid identifier"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: Identifier::parse("value").expect("valid identifier"),
                ty: JsonType::String,
                optional: false,
            }],
        }))
    }

    fn json_wire_schema_with_type(name: &str, field_type: JsonType) -> Model {
        Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
            name: identifier(name),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: identifier("value"),
                ty: field_type,
                optional: false,
            }],
        }))
    }

    fn avro_wire_schema_with_type(name: &str, field_type: nervix_models::AvroType) -> Model {
        Model::WireSchema(CreateWireSchemaStmt::Avro(CreateWireSchema {
            name: identifier(name),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: identifier("value"),
                ty: field_type,
                optional: false,
            }],
        }))
    }

    fn client_model(name: &str) -> Model {
        sample_transport_model(name)
    }

    fn vhost(name: &str, hostnames: &[&str]) -> Model {
        Model::Vhost(CreateVhost {
            name: Identifier::parse(name).expect("valid identifier"),
            hostnames: hostnames
                .iter()
                .map(|hostname| (*hostname).to_string())
                .collect(),
            tls: None,
        })
    }

    fn endpoint(
        name: &str,
        vhost_name: &str,
        path: &str,
        endpoint_type: nervix_models::EndpointType,
    ) -> Model {
        Model::Endpoint(nervix_models::CreateEndpoint {
            name: Identifier::parse(name).expect("valid identifier"),
            on_vhost: Identifier::parse(vhost_name).expect("valid identifier"),
            path: path.to_string(),
            endpoint_type,
            signaling_protocol: None,
        })
    }

    fn codec(name: &str, schema: &str) -> Model {
        Model::Codec(CreateCodec {
            name: Identifier::parse(name).expect("valid identifier"),
            wire_format: CodecWireFormat::Json,
            wire_schema: Some(Identifier::parse("event_wire").expect("valid identifier")),
            schema: Identifier::parse(schema).expect("valid identifier"),
            encoding_rules: Vec::new(),
        })
    }

    fn avro_codec(name: &str, wire_schema: &str, schema: &str) -> Model {
        Model::Codec(CreateCodec {
            name: identifier(name),
            wire_format: CodecWireFormat::Avro,
            wire_schema: Some(identifier(wire_schema)),
            schema: identifier(schema),
            encoding_rules: Vec::new(),
        })
    }

    fn jaq_native_codec(
        name: &str,
        schema: &str,
        on_ingestion: Option<&str>,
        on_emitting: Option<&str>,
    ) -> Model {
        Model::Codec(CreateCodec {
            name: identifier(name),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                },
            },
            wire_schema: None,
            schema: identifier(schema),
            encoding_rules: Vec::new(),
        })
    }

    fn protobuf_codec(
        name: &str,
        schema: &str,
        on_ingestion: Option<&str>,
        on_emitting: Option<&str>,
    ) -> Model {
        Model::Codec(CreateCodec {
            name: identifier(name),
            wire_format: CodecWireFormat::Protobuf(CodecProtobufConfig {
                resource: identifier("proto_bundle"),
                resource_version: Some(1),
                config: vec![ClientConfigEntry {
                    key: "file".to_string(),
                    value: "notification.proto".to_string(),
                }],
                message: "nervix.test.Notification".to_string(),
                transformations: CodecJaqTransformations {
                    on_ingestion: on_ingestion.map(str::to_string),
                    on_emitting: on_emitting.map(str::to_string),
                },
            }),
            wire_schema: None,
            schema: identifier(schema),
            encoding_rules: Vec::new(),
        })
    }

    fn rfc3339_json_codec(name: &str, wire_schema: &str, schema: &str) -> Model {
        rfc3339_json_codec_for_field(name, wire_schema, schema, "value")
    }

    fn rfc3339_json_codec_for_field(
        name: &str,
        wire_schema: &str,
        schema: &str,
        field: &str,
    ) -> Model {
        Model::Codec(CreateCodec {
            name: identifier(name),
            wire_format: CodecWireFormat::Json,
            wire_schema: Some(identifier(wire_schema)),
            schema: identifier(schema),
            encoding_rules: vec![CodecEncodingRule {
                field: identifier(field),
                encoding: CodecEncoding::Rfc3339,
            }],
        })
    }

    fn ingestor(name: &str, into: &str, codec: &str, client: &str) -> Model {
        let Model::Ingestor(mut ingestor) = ingestor_with_params(name, into, codec, client, &[])
        else {
            unreachable!("ingestor helper must build an ingestor model")
        };
        for output in &mut ingestor.output_routes.routes {
            output.branch = Some(OutputBranch::Unbranched);
        }
        Model::Ingestor(ingestor)
    }

    fn unbranched_ingestor(name: &str, into: &str, codec: &str, client: &str) -> Model {
        ingestor(name, into, codec, client)
    }

    fn ingestor_with_params(
        name: &str,
        into: &str,
        codec: &str,
        client: &str,
        branch_fields: &[&str],
    ) -> Model {
        let branch = if branch_fields.is_empty() {
            OutputBranch::Unbranched
        } else {
            branched_by(into, branch_fields)
        };
        Model::Ingestor(CreateIngestor {
            name: identifier(name),
            output_routes: with_output_branch(
                with_inherit_all(ProcessorOutputs::single(identifier(into)))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                branch,
            ),
            decode_using_codec: identifier(codec),
            timestamp_source: None,
            source: IngestSource::Kafka {
                client: Identifier::parse(client).expect("valid identifier"),
                topic: Identifier::parse("notifications").expect("valid identifier"),
                offset_mode: KafkaOffsetMode::ConsumerGroup(
                    Identifier::parse("cg").expect("valid identifier"),
                ),
                instances: 1,
                mode: KafkaIngestMode::AckSequential {
                    timeout: "30s".to_string(),
                    retry_policy: nervix_models::RetryPolicy {
                        backoff: "200ms".to_string(),
                        max_backoff: "5s".to_string(),
                    },
                },
            },
            general_error_policy: GeneralErrorPolicy::Log,

            filter_where: None,
        })
    }

    fn relay(name: &str, schema: &str) -> Model {
        Model::Relay(CreateRelay {
            name: Identifier::parse(name).expect("valid identifier"),
            schema: Identifier::parse(schema).expect("valid identifier"),
            buffer: 1,
            branching: RelayBranching::unbranched(),
            materialized_state: None,
        })
    }

    fn relay_branched_by(name: &str, schema: &str, branch: &str) -> Model {
        let Model::Relay(mut relay) = relay(name, schema) else {
            unreachable!("relay helper must build a relay model")
        };
        relay.branching = RelayBranching::branched_by(identifier(branch));
        Model::Relay(relay)
    }

    fn relay_branched_by_relay_branch(name: &str, schema: &str) -> Model {
        let Model::Relay(mut relay) = relay(name, schema) else {
            unreachable!("relay helper must build a relay model")
        };
        relay.branching = RelayBranching::branched_by(branch_name_for_relay(name));
        Model::Relay(relay)
    }

    fn relay_branched_like(name: &str, schema: &str, source_relay: &str) -> Model {
        let Model::Relay(mut relay) = relay(name, schema) else {
            unreachable!("relay helper must build a relay model")
        };
        relay.branching = RelayBranching::branched_by(branch_name_for_relay(source_relay));
        Model::Relay(relay)
    }

    fn materialized_relay(name: &str, schema: &str) -> Model {
        Model::Relay(CreateRelay {
            name: Identifier::parse(name).expect("valid identifier"),
            schema: Identifier::parse(schema).expect("valid identifier"),
            buffer: 1,
            branching: RelayBranching::branched_by(branch_name_for_relay(name)),
            materialized_state: Some(MaterializedRelayState::LastByTimestamp),
        })
    }

    fn explicitly_unbranched_relay(name: &str, schema: &str) -> Model {
        let Model::Relay(mut relay) = relay(name, schema) else {
            unreachable!("relay helper must build a relay model")
        };
        relay.branching = RelayBranching::unbranched();
        Model::Relay(relay)
    }

    fn processor(name: &str, from_relay: &str, into_relay: &str) -> Model {
        deduplicator(
            name,
            from_relay,
            into_relay,
            &format!("{from_relay}.value"),
            "10m",
        )
    }

    fn wasm_processor(name: &str, from_relay: &str, into_relay: &str) -> Model {
        Model::WasmProcessor(CreateWasmProcessor {
            name: identifier(name),
            from: ProcessorInputs::single(identifier(from_relay)),
            output_routes: {
                let mut outputs = ProcessorOutputs::single(identifier(into_relay));
                outputs.routes[0].construction =
                    nervix_nspl::parse_route_construction("SET value = value")
                        .expect("generated route construction must parse");
                outputs
            },
            branched_by: BranchSelection::unbranched(),
            resource: identifier("wasm_filter"),
            resource_version: Some(1),
            file: "processors/filter_even.wasm".to_string(),
            global_error_policy: GeneralErrorPolicy::Log,
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn unbranched_correlator(
        name: &str,
        left_relay: &str,
        right_relay: &str,
        into_relay: &str,
    ) -> Model {
        let mut output_routes = (ProcessorOutputs::single(identifier(into_relay)))
            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string()));
        output_routes.routes[0].construction =
            nervix_nspl::parse_route_construction("SET value = left.value")
                .expect("route construction must parse");
        Model::Correlator(CreateCorrelator {
            name: identifier(name),
            left: ProcessorInputs::single(identifier(left_relay)),
            right: ProcessorInputs::single(identifier(right_relay)),
            output_routes,
            branched_by: BranchSelection::unbranched(),
            correlate_where: nervix_nspl::parse_expression("left.value = right.value")
                .expect("correlator expression must parse"),
            match_policy: CorrelatorMatchPolicy::Earliest,
            max_time: "5s".to_string(),
            timeout_policy: CorrelationTimeoutPolicy {
                left: CorrelationTimeoutAction::Drop,
                right: CorrelationTimeoutAction::Drop,
            },
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn window_processor(
        name: &str,
        from_relay: &str,
        into_relay: &str,
        construction: &str,
    ) -> Model {
        let mut output_routes =
            ProcessorOutputs::single(Identifier::parse(into_relay).expect("valid identifier"));
        output_routes.routes[0].construction = nervix_nspl::parse_route_construction(construction)
            .expect("window route construction must parse");
        Model::WindowProcessor(CreateWindowProcessor {
            name: Identifier::parse(name).expect("valid identifier"),
            from: ProcessorInputs::single(Identifier::parse(from_relay).expect("valid identifier")),
            output_routes,
            branched_by: BranchSelection::branched_by(branch_name_for_relay(from_relay)),
            width: WindowBound {
                messages: Some(10),
                duration: None,
            },
            step: WindowBound {
                messages: Some(5),
                duration: None,
            },
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn junction(name: &str, from_relays: &[&str], into_relay: &str) -> Model {
        Model::Junction(CreateJunction {
            name: Identifier::parse(name).expect("valid identifier"),
            from: ProcessorInputs::new(
                from_relays
                    .iter()
                    .map(|stream| Identifier::parse(stream).expect("valid identifier"))
                    .collect(),
                Vec::new(),
            ),
            output_routes: with_inherit_all(ProcessorOutputs::single(
                Identifier::parse(into_relay).expect("valid identifier"),
            ))
            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
            branched_by: BranchSelection::branched_by(branch_name_for_relay(
                from_relays
                    .first()
                    .expect("junction helper requires at least one input"),
            )),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn deduplicator(
        name: &str,
        from_relay: &str,
        into_relay: &str,
        field: &str,
        max_time: &str,
    ) -> Model {
        Model::Deduplicator(CreateDeduplicator {
            name: Identifier::parse(name).expect("valid identifier"),
            from: ProcessorInputs::single(Identifier::parse(from_relay).expect("valid identifier")),
            output_routes: with_inherit_all(ProcessorOutputs::single(
                Identifier::parse(into_relay).expect("valid identifier"),
            ))
            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
            branched_by: BranchSelection::branched_by(branch_name_for_relay(from_relay)),
            deduplicate_on: vec![
                nervix_nspl::parse_expression(&field.replace(&format!("{from_relay}."), "input."))
                    .expect("deduplicate expression must parse"),
            ],
            max_time: max_time.to_string(),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn reingestor(name: &str, from_relay: &str, into_relay: &str, params: &[&str]) -> Model {
        let branch = if params.is_empty() {
            OutputBranch::Unbranched
        } else {
            branched_by(into_relay, params)
        };
        Model::Reingestor(CreateReingestor {
            name: Identifier::parse(name).expect("valid identifier"),
            from: ProcessorInputs::single(Identifier::parse(from_relay).expect("valid identifier")),
            output_routes: with_output_branch(
                with_inherit_all(ProcessorOutputs::single(
                    Identifier::parse(into_relay).expect("valid identifier"),
                ))
                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                branch,
            ),
            mode: AckMode::Attached,
            filter_where: None,
            materialized_state: Vec::new(),
        })
    }

    fn emitter(name: &str, from_relay: &str, codec: &str, client: &str) -> Model {
        Model::Emitter(CreateEmitter {
            name: Identifier::parse(name).expect("valid identifier"),
            from_relay: Identifier::parse(from_relay).expect("valid identifier"),
            encode_using_codec: Some(Identifier::parse(codec).expect("valid identifier")),
            sink: EmitSink::Kafka {
                client: Identifier::parse(client).expect("valid identifier"),
                topic: Identifier::parse("topic").expect("valid topic identifier"),
            },
            flush_each: "100ms".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),

            construction: nervix_models::RouteConstruction {
                inherit: Some(Inheritance::All),
                ..nervix_models::RouteConstruction::default()
            },
            materialized_state: Vec::new(),
        })
    }

    #[test]
    fn emitter_header_invocations_are_rejected_for_unsupported_sinks() {
        let domain = Domain::parse("default").expect("valid domain");
        let schema = CreateSchema {
            name: identifier("event_schema"),
            fields: vec![SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        };
        let mut emitter = CreateEmitter {
            name: identifier("emit"),
            from_relay: identifier("events"),
            encode_using_codec: Some(identifier("events_codec")),
            sink: EmitSink::ZeroMq {
                client: identifier("zeromq_main"),
            },
            flush_each: "100ms".to_string(),
            max_batch_size: Some("1MiB".to_string()),
            mode: AckMode::Attached,
            error_policies: ErrorPolicies::handled_by_log(),
            construction: nervix_nspl::parse_route_construction(
                "INHERIT ALL INVOKE write_header(\"tenant\", input.tenant)",
            )
            .expect("valid construction"),
            materialized_state: Vec::new(),
        };

        let error = super::effective_emitter_filter_map_schema(
            &domain,
            &emitter.name,
            &HashMap::default(),
            &emitter,
            &schema,
            &schema,
        )
        .expect_err("ZeroMQ emitters must reject write_header");
        assert!(format!("{error:#}").contains("ZEROMQ emitters do not support write_header"));

        emitter.sink = EmitSink::Kafka {
            client: identifier("kafka_main"),
            topic: identifier("events_out"),
        };
        super::effective_emitter_filter_map_schema(
            &domain,
            &emitter.name,
            &HashMap::default(),
            &emitter,
            &schema,
            &schema,
        )
        .expect("Kafka emitters must accept write_header");
    }

    fn scheduled_node<'a>(
        schedule: &'a DomainSchedule,
        kind: ModelKind,
        identifier: &str,
    ) -> &'a ScheduledNode {
        schedule
            .nodes
            .iter()
            .find(|node| node.kind == kind && node.identifier.as_str() == identifier)
            .unwrap_or_else(|| panic!("missing scheduled node {kind:?}:{identifier}"))
    }

    fn full_graph_batch() -> Vec<Model> {
        vec![
            schema("event_schema"),
            branch_schema("value_branch", &["value"]),
            branch_for_relay("notifications", "value_branch"),
            wire_schema("event_wire"),
            codec("event_codec", "event_schema"),
            client_model("broker_in"),
            client_model("broker_out"),
            relay_branched_by_relay_branch("notifications", "event_schema"),
            relay_branched_like("p99", "event_schema", "notifications"),
            ingestor_with_params(
                "ing",
                "notifications",
                "event_codec",
                "broker_in",
                &["value"],
            ),
            processor("p99_proc", "notifications", "p99"),
            emitter("emit", "p99", "event_codec", "broker_out"),
        ]
    }

    fn example_graph_models(name: &str, source: &str) -> (Domain, Vec<nervix_models::Model>) {
        let statements = nervix_nspl::client_statement::parse_client_statement_sources(source)
            .unwrap_or_else(|error| panic!("{name} example should parse: {error:?}"));
        let mut domain = Domain::parse("default").expect("valid domain");
        let mut models = Vec::new();

        for parsed in statements {
            match parsed.statement {
                nervix_nspl::client_statement::ClientStatement::UseDomain(next) => {
                    domain = next;
                }
                nervix_nspl::client_statement::ClientStatement::UploadResource(_)
                | nervix_nspl::client_statement::ClientStatement::BeginTransaction
                | nervix_nspl::client_statement::ClientStatement::CommitTransaction
                | nervix_nspl::client_statement::ClientStatement::RevertTransaction
                | nervix_nspl::client_statement::ClientStatement::CreateSubscription(_)
                | nervix_nspl::client_statement::ClientStatement::DeleteSubscription(_) => {}
                nervix_nspl::client_statement::ClientStatement::Server(statement) => {
                    match statement {
                        nervix_models::Statement::CreateDomain(create) => {
                            domain = create.body.id;
                        }
                        nervix_models::Statement::Create(create) => {
                            models.push(*create.body);
                        }
                        nervix_models::Statement::CreateResource(_)
                        | nervix_models::Statement::UploadResource(_)
                        | nervix_models::Statement::StartDomain(_) => {}
                        other => panic!("unexpected {name} example statement: {other:?}"),
                    }
                }
                other => panic!("unexpected {name} example client statement: {other:?}"),
            }
        }

        (domain, models)
    }

    fn assert_example_graph_validates(name: &str, source: &str) {
        let (domain, models) = example_graph_models(name, source);
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        registry
            .apply_batch(&domain, models)
            .unwrap_or_else(|error| panic!("{name} example graph should validate: {error:?}"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn runnable_example_graphs_validate() {
        assert_example_graph_validates("iot", include_str!("../../../examples/iot/iot.nspl"));
        assert_example_graph_validates(
            "nats_factory_windows",
            include_str!("../../../examples/nats-factory-windows/nats_factory_windows.nspl"),
        );
        assert_example_graph_validates(
            "datalake",
            include_str!("../../../examples/datalake/datalake.nspl"),
        );
        assert_example_graph_validates(
            "wasm_dual",
            include_str!("../../../examples/wasm-processors/wasm-dual.nspl"),
        );
    }

    #[test]
    fn create_fails_when_model_already_exists() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = Domain::parse("default").expect("valid domain");

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
        let ns = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &ns,
                vec![schema("shared_name"), client_model("shared_name")],
            )
            .expect("different kinds should be allowed to share an identifier");

        assert!(
            registry
                .get(
                    &ns,
                    ModelKind::Schema,
                    &Identifier::parse("shared_name").expect("valid identifier"),
                )
                .expect("schema read should succeed")
                .is_some()
        );
        assert!(
            registry
                .get(
                    &ns,
                    ModelKind::Client,
                    &Identifier::parse("shared_name").expect("valid identifier"),
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
        let domain = Domain::parse("default").expect("valid domain");
        let schema = schema("event_schema");
        let wire_schema = wire_schema("event_wire");
        let relay = relay("raw_events", "event_schema");
        let model = ingestor("kafka_ingestor", "raw_events", "event_codec", "kafka_main");

        storage
            .put(&domain, schema.kind(), schema.identifier(), &schema)
            .expect("write should succeed");
        storage
            .put(
                &domain,
                wire_schema.kind(),
                wire_schema.identifier(),
                &wire_schema,
            )
            .expect("write should succeed");
        storage
            .put(&domain, relay.kind(), relay.identifier(), &relay)
            .expect("write should succeed");
        storage
            .put(&domain, model.kind(), model.identifier(), &model)
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
        let ns = Domain::parse("default").expect("valid domain");

        registry
            .storage
            .put(
                &ns,
                ModelKind::Client,
                &Identifier::parse("kafka_main").expect("valid identifier"),
                &sample_transport_model("kafka_main"),
            )
            .expect("write should succeed");

        let transports = registry
            .list_identifiers(&ns, ModelKind::Client, "kafka_")
            .expect("list should succeed");
        assert_eq!(
            transports
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["kafka_main"]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn get_roundtrip_returns_stored_model() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let ns = Domain::parse("default").expect("valid domain");
        let id = Identifier::parse("kafka_main").expect("valid identifier");
        let model = sample_transport_model("kafka_main");

        registry
            .storage
            .put(&ns, ModelKind::Client, &id, &model)
            .expect("create should succeed");
        let loaded = registry
            .get(&ns, ModelKind::Client, &id)
            .expect("read should succeed")
            .expect("model should exist");

        assert_eq!(loaded, model);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_partial_graphs() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

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
        let domain = Domain::parse("default").expect("valid domain");

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
                    relay: identifier("notifications"),
                    operation: AlterRelayOperation::SetCapacity { capacity: 5 },
                },
            )
            .expect("alter should succeed");
        assert_eq!(changes.changes.len(), 1);
        let RuntimeChange::SetRelayCapacity { relay, capacity } = &changes.changes[0] else {
            panic!("alter relay capacity should produce a targeted capacity change");
        };
        assert_eq!(relay, &identifier("notifications"));
        assert_eq!(capacity.get(), 5);

        let stored = registry
            .get(&domain, ModelKind::Relay, &identifier("notifications"))
            .expect("read should succeed")
            .expect("relay should exist");
        let Model::Relay(stored_relay) = stored else {
            panic!("stored model should be a relay");
        };
        assert_eq!(stored_relay.buffer, 5);

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let node = graph
            .node(ModelKind::Relay, &identifier("notifications"))
            .expect("relay node should exist");
        let Model::Relay(graph_relay) = node.config.as_ref() else {
            panic!("graph node should contain relay config");
        };
        assert_eq!(graph_relay.buffer, 5);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn alter_relay_rejects_missing_relay_without_persisting() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let result = registry.alter_relay(
            &domain,
            AlterRelay {
                relay: identifier("notifications"),
                operation: AlterRelayOperation::SetCapacity { capacity: 5 },
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
                .get(&domain, ModelKind::Relay, &identifier("notifications"))
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_unbranched_ingestor_without_branch_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    unbranched_ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect("unbranched ingestor should not require a branch schema");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let relay = graph
            .node(ModelKind::Relay, &identifier("notifications"))
            .expect("relay should exist");
        assert_eq!(relay.effective_branching, Some(Vec::new()));
        assert_eq!(relay.effective_branching_schema, None);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_inferencer_generated_output() {
        let (domain, models) = example_graph_models(
            "inferencer generated output schema",
            r#"
            CREATE SCHEMA features (
              tenant STRING,
              vector ARRAY<F32, 2>
            );

            CREATE SCHEMA scored (
              score ARRAY<F32, 1>
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY features SCHEMA features BRANCHED BY by_tenant_branch;
            CREATE RELAY scored SCHEMA scored BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model VERSION 1
              FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
              BRANCHED BY by_tenant_branch
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("inferencer should construct output from immutable generated state");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_mixed_inferencer_execution_modes() {
        let (domain, models) = example_graph_models(
            "mixed inferencer execution modes",
            r#"
            CREATE SCHEMA features ( vector ARRAY<F32, 2> );
            CREATE SCHEMA scored ( score ARRAY<F32, 1> );
            CREATE RELAY features SCHEMA features UNBRANCHED;
            CREATE RELAY scored SCHEMA scored UNBRANCHED;
            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[BATCH, 2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] }
              UNBRANCHED
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("mixed inferencer execution modes must fail");
        assert!(error.to_string().contains("mixes batched and per-message"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_multiple_batch_axes_in_one_tensor() {
        let (domain, models) = example_graph_models(
            "multiple inferencer batch axes",
            r#"
            CREATE SCHEMA features ( vector ARRAY<F32, 2> );
            CREATE SCHEMA scored ( score ARRAY<F32, 1> );
            CREATE RELAY features SCHEMA features UNBRANCHED;
            CREATE RELAY scored SCHEMA scored UNBRANCHED;
            CREATE INFERENCER score_model
              FROM features
              USING RESOURCE fraud_model FILE 'models/simple_score.onnx'
              INPUTS { "features" DENSE TENSOR<F32>[BATCH, BATCH, 2] = input.vector }
              OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[BATCH, 1] }
              UNBRANCHED
              TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("multiple BATCH axes must fail");
        assert!(
            error
                .to_string()
                .contains("contains more than one BATCH axis")
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_window_processor_generated_output_schema() {
        let (domain, models) = example_graph_models(
            "window processor generated output schema",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              sample_count I64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              BRANCHED BY by_tenant_branch
              TO metric_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("window aggregate outputs should define non-input output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_processor_unassigned_output_field() {
        let (domain, models) = example_graph_models(
            "window processor unassigned output field",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency U64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              total_latency U64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY metric_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 10s DURATION
              STEP 5s DURATION
              BRANCHED BY by_tenant_branch
              TO metric_summaries
                SET total_latency = SUM(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("window aggregate should reject unassigned output fields");
        assert!(
            format!("{err}").contains(
                "window aggregate must assign required output field 'metric_summaries.tenant'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_window_output_route_filter_on_generated_output() {
        let (domain, models) = example_graph_models(
            "window processor output route filter",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              sample_count I64,
              total_latency I64
            );

            CREATE IF NOT EXISTS SCHEMA tenant_branch ( tenant STRING );
            CREATE RELAY metrics SCHEMA metric BRANCHED BY by_tenant_branch;
            CREATE RELAY high_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE RELAY low_summaries SCHEMA metric_summary BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE WINDOW PROCESSOR first_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              BRANCHED BY by_tenant_branch
              TO high_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency),
                    total_latency = SUM(input.latency)
                WHERE total_latency >= 100
                ON MESSAGE ERROR LOG
              TO low_summaries
                SET tenant = FIRST(input.tenant),
                    sample_count = COUNT(input.latency),
                    total_latency = SUM(input.latency)
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("window output route predicates should read generated output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_output_route_filter_on_live_input() {
        let (domain, models) = example_graph_models(
            "window processor output input filter",
            r#"
            CREATE SCHEMA metric (
              tenant STRING,
              latency I64
            );

            CREATE SCHEMA metric_summary (
              tenant STRING,
              total_latency I64
            );

            CREATE RELAY metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY metric_summaries SCHEMA metric_summary UNBRANCHED;

            CREATE WINDOW PROCESSOR latency_window
              FROM metrics
              WIDTH 2 MESSAGES
              STEP 2 MESSAGES
              UNBRANCHED
              TO metric_summaries
                SET tenant = FIRST(input.tenant),
                    total_latency = SUM(input.latency)
                WHERE input.latency > 0
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let error = registry
            .apply_batch(&domain, models)
            .expect_err("window route WHERE must not expose live input");
        assert!(
            format!("{error}").contains("input is unavailable after set-only output finalization"),
            "unexpected error: {error}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_wasm_output_routes_on_generated_output() {
        let (domain, models) = example_graph_models(
            "wasm processor output routes",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE SCHEMA projected_metric (
              value I64,
              source STRING OPTIONAL,
              bucket STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY even_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY projected_metrics SCHEMA projected_metric UNBRANCHED;

            CREATE WASM PROCESSOR route_guest_output
              FROM raw_metrics
              FILTER WHERE input.value >= 0
              USING RESOURCE wasm_filter VERSION 1
              FILE 'processors/filter_even.wasm'
              UNBRANCHED
              TO even_metrics
                SET value = value, source = source
                WHERE value >= 10
                ON MESSAGE ERROR LOG
              TO projected_metrics
                SET value = value,
                    source = source,
                    bucket = lower(bucket)
                ON MESSAGE ERROR LOG
              ON GLOBAL ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("wasm output routes should read guest output fields");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_processor_from_where() {
        let (domain, models) = example_graph_models(
            "processor source where",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE input.value >= 0
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("source WHERE should validate against the input relay");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_non_boolean_processor_from_where() {
        let (domain, models) = example_graph_models(
            "processor non-boolean source where",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE input.value
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-boolean source WHERE must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err:#}").contains("FROM WHERE compile failed"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_processor_from_where_unavailable_scope() {
        let (domain, models) = example_graph_models(
            "processor source where other relay",
            r#"
            CREATE SCHEMA metric (
              value I64,
              source STRING
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY deduped_metrics SCHEMA metric UNBRANCHED;

            CREATE DEDUPLICATOR dedup_metrics
              FROM raw_metrics WHERE branch.value >= 0
              DEDUPLICATE ON input.source
              MAX TIME 10m
              UNBRANCHED
              TO deduped_metrics INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("source WHERE cannot reference a branch during unbranched execution");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("FROM WHERE") && rendered.contains("branch"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_duplicate_wasm_output_route() {
        let (domain, models) = example_graph_models(
            "wasm processor duplicate output route",
            r#"
            CREATE SCHEMA metric (
              value I64
            );

            CREATE RELAY raw_metrics SCHEMA metric UNBRANCHED;
            CREATE RELAY projected_metrics SCHEMA metric UNBRANCHED;

            CREATE WASM PROCESSOR route_guest_output
              FROM raw_metrics
              USING RESOURCE wasm_filter VERSION 1
              FILE 'processors/filter_even.wasm'
              UNBRANCHED
              TO projected_metrics SET value = value ON MESSAGE ERROR LOG
              TO projected_metrics SET value = value WHERE value >= 0 ON MESSAGE ERROR LOG
              ON GLOBAL ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("duplicate WASM output routes must be rejected");
        assert!(
            format!("{err}").contains(
                "WASM processor output relay 'projected_metrics' is declared more than once"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_unconditional_processor_output_route() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain =
            Domain::parse("unconditional_processor_output_route").expect("domain should parse");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    explicitly_unbranched_relay("projected_events", "event_schema"),
                    Model::Deduplicator(CreateDeduplicator {
                        name: identifier("dedup_events"),
                        from: ProcessorInputs::single(identifier("raw_events")),
                        output_routes: with_inherit_all(ProcessorOutputs::single(identifier(
                            "projected_events",
                        )))
                        .with_flush_policy("IMMEDIATE".to_string(), None),
                        branched_by: BranchSelection::unbranched(),
                        deduplicate_on: vec![
                            nervix_nspl::parse_expression("input.value")
                                .expect("deduplicate expression must parse"),
                        ],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect("unconditional output route should be accepted");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_empty_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let schema_domain = Domain::parse("empty_schema").expect("valid domain");
        let wire_schema_domain = Domain::parse("empty_wire_schema").expect("valid domain");

        let result = registry.apply_batch(
            &schema_domain,
            vec![Model::Schema(CreateSchema {
                name: identifier("root_branch"),
                fields: Vec::new(),
            })],
        );
        assert!(matches!(
            result
                .expect_err("empty schema should be rejected")
                .current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let result = registry.apply_batch(
            &wire_schema_domain,
            vec![Model::WireSchema(CreateWireSchemaStmt::Json(
                CreateWireSchema {
                    name: identifier("empty_wire"),
                    strictness: Default::default(),
                    fields: Vec::<WireSchemaField<JsonType>>::new(),
                },
            ))],
        );
        assert!(matches!(
            result
                .expect_err("empty wire schema should be rejected")
                .current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_spreads_independent_ingestors_before_locality_applies() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_a"),
                    client_model("broker_b"),
                    relay("notifications_a", "event_schema"),
                    relay("notifications_b", "event_schema"),
                    ingestor("ing_a", "notifications_a", "event_codec", "broker_a"),
                    ingestor("ing_b", "notifications_b", "event_codec", "broker_b"),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule =
            graph.schedule_for_domain(&domain, &["node-1".to_string(), "node-2".to_string()], 0);

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_a").assigned_nodes,
            vec!["node-1".to_string()]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_b").assigned_nodes,
            vec!["node-2".to_string()]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_prefers_upstream_locality_for_dedicated_chain() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
            0,
        );

        let ingestor_node = scheduled_node(&schedule, ModelKind::Ingestor, "ing")
            .assigned_single_node()
            .map(str::to_string)
            .clone();
        let processor_node = scheduled_node(&schedule, ModelKind::Deduplicator, "p99_proc")
            .assigned_single_node()
            .map(str::to_string)
            .clone();
        let emitter_node = scheduled_node(&schedule, ModelKind::Emitter, "emit")
            .assigned_single_node()
            .map(str::to_string)
            .clone();

        assert_eq!(processor_node, ingestor_node);
        assert_eq!(emitter_node, processor_node);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_prefers_majority_upstream_locality_for_shared_downstream() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_a"),
                    client_model("broker_b"),
                    client_model("broker_c"),
                    client_model("broker_out"),
                    relay_branched_by_relay_branch("root_a", "event_schema"),
                    relay_branched_by_relay_branch("root_b", "event_schema"),
                    relay_branched_by_relay_branch("root_c", "event_schema"),
                    relay_branched_like("branch_a", "event_schema", "root_a"),
                    relay_branched_like("branch_b", "event_schema", "root_b"),
                    relay_branched_like("branch_c", "event_schema", "root_c"),
                    relay_branched_by_relay_branch("shared", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("root_a", "value_branch"),
                    branch_for_relay("root_b", "value_branch"),
                    branch_for_relay("root_c", "value_branch"),
                    branch_for_relay("shared", "value_branch"),
                    ingestor_with_params("ing_a", "root_a", "event_codec", "broker_a", &["value"]),
                    ingestor_with_params("ing_b", "root_b", "event_codec", "broker_b", &["value"]),
                    ingestor_with_params("ing_c", "root_c", "event_codec", "broker_c", &["value"]),
                    processor("proc_a", "root_a", "branch_a"),
                    processor("proc_b", "root_b", "branch_b"),
                    processor("proc_c", "root_c", "branch_c"),
                    reingestor("shared_a", "branch_a", "shared", &["value"]),
                    reingestor("shared_b", "branch_b", "shared", &["value"]),
                    reingestor("shared_c", "branch_c", "shared", &["value"]),
                    emitter("emit_shared", "shared", "event_codec", "broker_out"),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule =
            graph.schedule_for_domain(&domain, &["node-1".to_string(), "node-2".to_string()], 0);

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_a").assigned_nodes,
            vec!["node-1".to_string()]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_b").assigned_nodes,
            vec!["node-2".to_string()]
        );
        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "ing_c").assigned_nodes,
            vec!["node-1".to_string()]
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Emitter, "emit_shared").assigned_nodes,
            vec!["node-1".to_string()]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_places_server_side_ingestors_on_all_live_nodes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    vhost("public", &["events.example.com"]),
                    endpoint(
                        "ingest_http",
                        "public",
                        "/ingest",
                        nervix_models::EndpointType::Http,
                    ),
                    relay("notifications", "event_schema"),
                    Model::Ingestor(CreateIngestor {
                        name: Identifier::parse("http_ing").expect("valid identifier"),
                        output_routes: unbranched_transforming_outputs("notifications"),
                        decode_using_codec: Identifier::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Endpoint {
                            endpoint: Identifier::parse("ingest_http").expect("valid identifier"),
                            mode: nervix_models::EndpointIngestMode::NoAckSequential,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let schedule = graph.schedule_for_domain(
            &domain,
            &[
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
            0,
        );

        assert_eq!(
            scheduled_node(&schedule, ModelKind::Ingestor, "http_ing").assigned_nodes,
            vec![
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string()
            ]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn mqtt_instances_greater_than_one_are_valid() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                schema("event_schema"),
                wire_schema("event_wire"),
                codec("event_codec", "event_schema"),
                client_model("mqtt_main"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: Identifier::parse("mqtt_ing").expect("valid identifier"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    decode_using_codec: Identifier::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Mqtt {
                        client: Identifier::parse("mqtt_main").expect("valid identifier"),
                        topic: "notifications".to_string(),
                        instances: 2,
                        mode: MqttIngestMode::NoAckSequential {
                            session: MqttSession::Clean,
                            qos: MqttQos::AtMostOnce,
                        },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,
                    filter_where: None,
                }),
            ],
        );

        result.expect("MQTT multi-instance ingestors should not expose subscription mode");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_timestamp_field_must_use_rfc3339_schema_type() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: Identifier::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: Identifier::parse("occurred_at").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                    name: Identifier::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![
                        WireSchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                        WireSchemaField {
                            name: Identifier::parse("occurred_at").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                })),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: Identifier::parse("ing").expect("valid identifier"),
                    output_routes: unbranched_transforming_outputs("notifications"),
                    decode_using_codec: Identifier::parse("event_codec").expect("valid identifier"),
                    timestamp_source: Some(IngestTimestampSource::At(
                        Identifier::parse("occurred_at").expect("valid identifier"),
                    )),
                    source: IngestSource::Kafka {
                        client: Identifier::parse("broker").expect("valid identifier"),
                        topic: Identifier::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            Identifier::parse("cg").expect("valid identifier"),
                        ),
                        instances: 1,
                        mode: KafkaIngestMode::NoAckParallel { max: 1 },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("timestamp field with non-DATETIME type must fail");
        assert!(
            format!("{error:#}").contains("TIMESTAMP field 'occurred_at' must use DATETIME"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_route_validation_accepts_explicit_projection() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("raw").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("transformed_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("total").expect("valid identifier"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("raw").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker"),
                    relay_branched_by_relay_branch("notifications", "transformed_schema"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    Model::Ingestor(CreateIngestor {
                        name: Identifier::parse("ing").expect("valid identifier"),
                        output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                            relay: Identifier::parse("notifications").expect("valid identifier"),
                            construction: nervix_nspl::parse_route_construction(
                                "SET total = input.value, tenant = input.tenant",
                            )
                            .expect("route construction must parse"),
                            flush_policy: None,
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: Some(branched_by("notifications", &["tenant"])),
                        }]))
                        .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                        decode_using_codec: Identifier::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Kafka {
                            client: Identifier::parse("broker").expect("valid identifier"),
                            topic: Identifier::parse("notifications").expect("valid identifier"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(
                                Identifier::parse("cg").expect("valid identifier"),
                            ),
                            instances: 1,
                            mode: KafkaIngestMode::NoAckParallel { max: 1 },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                ],
            )
            .expect("batch with valid FILTER-MAP should succeed");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_filter_map_compile_errors_are_reported_on_leader() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: Identifier::parse("event_schema").expect("valid identifier"),
                    fields: vec![SchemaField {
                        name: Identifier::parse("value").expect("valid identifier"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
                Model::Schema(CreateSchema {
                    name: Identifier::parse("transformed_schema").expect("valid identifier"),
                    fields: vec![SchemaField {
                        name: Identifier::parse("total").expect("valid identifier"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }),
                Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                    name: Identifier::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![WireSchemaField {
                        name: Identifier::parse("value").expect("valid identifier"),
                        ty: JsonType::Integer,
                        optional: false,
                    }],
                })),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "transformed_schema"),
                Model::Ingestor(CreateIngestor {
                    name: Identifier::parse("ing").expect("valid identifier"),
                    output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: Identifier::parse("notifications").expect("valid identifier"),
                        construction: nervix_nspl::parse_route_construction(
                            "SET total = input.missing + 1",
                        )
                        .expect("route construction must parse"),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        branch: Some(OutputBranch::Unbranched),
                    }]))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    decode_using_codec: Identifier::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Kafka {
                        client: Identifier::parse("broker").expect("valid identifier"),
                        topic: Identifier::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            Identifier::parse("cg").expect("valid identifier"),
                        ),
                        instances: 1,
                        mode: KafkaIngestMode::NoAckParallel { max: 1 },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("invalid FILTER-MAP must fail");
        assert!(
            format!("{error:#}").contains("unknown input field 'missing'"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_inherit_all_except_rejects_required_uninitialized_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let result = registry.apply_batch(
            &domain,
            vec![
                Model::Schema(CreateSchema {
                    name: Identifier::parse("event_schema").expect("valid identifier"),
                    fields: vec![
                        SchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: ParseAsType::I64,
                            optional: false,
                            sensitive: false,
                        },
                        SchemaField {
                            name: Identifier::parse("tenant").expect("valid identifier"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        },
                    ],
                }),
                Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                    name: Identifier::parse("event_wire").expect("valid identifier"),
                    strictness: Default::default(),
                    fields: vec![
                        WireSchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: JsonType::Integer,
                            optional: false,
                        },
                        WireSchemaField {
                            name: Identifier::parse("tenant").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: false,
                        },
                    ],
                })),
                codec("event_codec", "event_schema"),
                client_model("broker"),
                relay("notifications", "event_schema"),
                Model::Ingestor(CreateIngestor {
                    name: Identifier::parse("ing").expect("valid identifier"),
                    output_routes: (ProcessorOutputs::new(vec![ProcessorOutput {
                        relay: Identifier::parse("notifications").expect("valid identifier"),
                        construction: nervix_nspl::parse_route_construction(
                            "INHERIT ALL EXCEPT value",
                        )
                        .expect("route construction must parse"),
                        flush_policy: None,
                        message_error_policy: MessageErrorPolicy::Log,
                        branch: Some(OutputBranch::Unbranched),
                    }]))
                    .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                    decode_using_codec: Identifier::parse("event_codec").expect("valid identifier"),
                    timestamp_source: None,
                    source: IngestSource::Kafka {
                        client: Identifier::parse("broker").expect("valid identifier"),
                        topic: Identifier::parse("notifications").expect("valid identifier"),
                        offset_mode: KafkaOffsetMode::ConsumerGroup(
                            Identifier::parse("cg").expect("valid identifier"),
                        ),
                        instances: 1,
                        mode: KafkaIngestMode::NoAckParallel { max: 1 },
                    },
                    general_error_policy: GeneralErrorPolicy::Log,

                    filter_where: None,
                }),
            ],
        );

        let error = result.expect_err("excluded required output must remain uninitialized");
        assert!(
            format!("{error:#}").contains("required output field 'value' remains uninitialized"),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn schedule_removes_server_side_ingestor_placements_for_missing_nodes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    vhost("public", &["events.example.com"]),
                    endpoint(
                        "ingest_ws",
                        "public",
                        "/ws",
                        nervix_models::EndpointType::Websockets,
                    ),
                    relay("notifications", "event_schema"),
                    Model::Ingestor(CreateIngestor {
                        name: Identifier::parse("ws_ing").expect("valid identifier"),
                        output_routes: unbranched_transforming_outputs("notifications"),
                        decode_using_codec: Identifier::parse("event_codec")
                            .expect("valid identifier"),
                        timestamp_source: None,
                        source: IngestSource::Endpoint {
                            endpoint: Identifier::parse("ingest_ws").expect("valid identifier"),
                            mode: nervix_models::EndpointIngestMode::NoAckSequential,
                        },
                        general_error_policy: GeneralErrorPolicy::Log,

                        filter_where: None,
                    }),
                ],
            )
            .expect("batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let initial_schedule = graph.schedule_for_domain(
            &domain,
            &[
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string(),
            ],
            0,
        );
        let reduced_schedule =
            graph.schedule_for_domain(&domain, &["node-1".to_string(), "node-3".to_string()], 0);

        assert_eq!(
            scheduled_node(&initial_schedule, ModelKind::Ingestor, "ws_ing").assigned_nodes,
            vec![
                "node-1".to_string(),
                "node-2".to_string(),
                "node-3".to_string()
            ]
        );
        assert_eq!(
            scheduled_node(&reduced_schedule, ModelKind::Ingestor, "ws_ing").assigned_nodes,
            vec!["node-1".to_string(), "node-3".to_string()]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn startup_runtime_changes_include_graph_only_domains() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

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
        let domain = Domain::parse("default").expect("valid domain");

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
                RuntimeChange::SetRelayCapacity { .. } => None,
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
                RuntimeChange::SetRelayCapacity { .. } => None,
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
        let domain = Domain::parse("default").expect("valid domain");

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
                .get(
                    &domain,
                    ModelKind::Ingestor,
                    &Identifier::parse("kafka_ingestor").expect("valid identifier")
                )
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_rejects_codec_without_decode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    jaq_native_codec("event_codec", "event_schema", None, Some("{payload: .}")),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect_err("ingestor must reject encode-only codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for decoding because it does not declare an \
                 ON INGESTION transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_rejects_codec_without_encode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    jaq_native_codec("event_codec", "event_schema", Some("."), None),
                    client_model("broker_out"),
                    relay("notifications", "event_schema"),
                    emitter("emit", "notifications", "event_codec", "broker_out"),
                ],
            )
            .expect_err("emitter must reject decode-only codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for encoding because it does not declare an \
                 ON EMITTING transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_codec_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::U32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect_err("incompatible codec schema should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_requires_explicit_rfc3339_encoding_for_json_string_datetime() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::Datetime,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::String),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect_err("implicit string datetime parsing must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_explicit_rfc3339_encoding_for_json_string_datetime() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::Datetime,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::String),
                    rfc3339_json_codec("event_codec", "event_wire", "event_schema"),
                ],
            )
            .expect("explicit RFC3339 encoding should allow string datetime wire field");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_rfc3339_encoding_for_unknown_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::Datetime,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::String),
                    rfc3339_json_codec_for_field(
                        "event_codec",
                        "event_wire",
                        "event_schema",
                        "missing",
                    ),
                ],
            )
            .expect_err("RFC3339 encoding must reference an internal schema field");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_rfc3339_encoding_for_non_datetime_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::String),
                    rfc3339_json_codec("event_codec", "event_wire", "event_schema"),
                ],
            )
            .expect_err("RFC3339 encoding must target a DATETIME internal schema field");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_rfc3339_encoding_without_json_string_wire_datetime() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::Datetime,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::Number),
                    rfc3339_json_codec("event_codec", "event_wire", "event_schema"),
                ],
            )
            .expect_err("RFC3339 encoding must require string wire field");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_json_integer_shape_for_internal_integer_widths() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::U32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::Integer),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect("json integer shape should support internal U32");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_json_number_shape_for_internal_f32() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::F32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    json_wire_schema_with_type("event_wire", JsonType::Number),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect("json number shape should support internal F32");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_avro_long_internal_width_coercion() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::I32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    avro_wire_schema_with_type("event_wire", nervix_models::AvroType::Long),
                    avro_codec("event_codec", "event_wire", "event_schema"),
                ],
            )
            .expect_err("avro long must not implicitly match I32");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_branching_value_type_mismatch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("event_schema"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    relay_branched_by_relay_branch("events", "event_schema"),
                    Model::Schema(CreateSchema {
                        name: identifier("value_branch"),
                        fields: vec![SchemaField {
                            name: identifier("value"),
                            ty: ParseAsType::U32,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    branch_for_relay("events", "value_branch"),
                    client_model("kafka_main"),
                    ingestor_with_params(
                        "events_in",
                        "events",
                        "event_codec",
                        "kafka_main",
                        &["value"],
                    ),
                ],
            )
            .expect_err("branch value type mismatch must fail");

        let message = format!("{err}");
        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            message.contains(
                "branch SET compile failed: SET field 'value' has expression type Utf8, expected \
                 declared output type UInt32"
            ),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_wire_and_internal_optionality_mismatch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::String,
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![WireSchemaField {
                            name: Identifier::parse("value").expect("valid identifier"),
                            ty: JsonType::String,
                            optional: true,
                        }],
                    })),
                    codec("event_codec", "event_schema"),
                ],
            )
            .expect_err("wire/internal optionality mismatch should fail");

        assert!(
            format!("{err:#}").contains("optionality mismatch"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_deduplicator_stream_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("wide_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("extra").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("wide", "wide_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    processor("project", "notifications", "wide"),
                ],
            )
            .expect_err("deduplicator schema mismatch should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_multiple_deduplicator_inputs_with_different_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: identifier("wide_schema"),
                        fields: vec![
                            SchemaField {
                                name: identifier("value"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: identifier("extra"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    explicitly_unbranched_relay("notifications_a", "event_schema"),
                    explicitly_unbranched_relay("notifications_b", "wide_schema"),
                    explicitly_unbranched_relay("deduped", "event_schema"),
                    Model::Deduplicator(CreateDeduplicator {
                        name: identifier("dedup_notifications"),
                        from: ProcessorInputs::new(
                            vec![identifier("notifications_a"), identifier("notifications_b")],
                            Vec::new(),
                        ),
                        output_routes: (ProcessorOutputs::single(identifier("deduped")))
                            .with_flush_policy("IMMEDIATE".to_string(), None),
                        branched_by: BranchSelection::unbranched(),
                        deduplicate_on: vec![
                            nervix_nspl::parse_expression("input.value")
                                .expect("deduplicate expression must parse"),
                        ],
                        max_time: "10m".to_string(),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("deduplicator input schema mismatch should fail");

        let message = format!("{err:#}");
        assert!(
            message.contains("deduplicator input"),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_sensitive_passthrough_to_non_sensitive_field() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: identifier("sensitive_event"),
                        fields: vec![
                            SchemaField {
                                name: identifier("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: identifier("secret"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: true,
                            },
                        ],
                    }),
                    Model::Schema(CreateSchema {
                        name: identifier("public_event"),
                        fields: vec![
                            SchemaField {
                                name: identifier("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: identifier("secret"),
                                ty: ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    explicitly_unbranched_relay("sensitive_events", "sensitive_event"),
                    explicitly_unbranched_relay("public_events", "public_event"),
                    Model::Reingestor(CreateReingestor {
                        name: identifier("leak_events"),
                        from: ProcessorInputs::single(identifier("sensitive_events")),
                        output_routes: unbranched_transforming_outputs("public_events"),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("sensitive passthrough into public schema should fail");

        let message = format!("{err:#}");
        assert!(
            message.contains("would store sensitive data in a non-sensitive output field"),
            "unexpected error: {message}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_junction_stream_schemas() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("wide_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("extra").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    relay("notifications_a", "event_schema"),
                    relay("notifications_b", "wide_schema"),
                    relay("merged", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications_a", "value_branch"),
                    junction(
                        "join_streams",
                        &["notifications_a", "notifications_b"],
                        "merged",
                    ),
                ],
            )
            .expect_err("junction schema mismatch should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_array_lengths() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("short_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: Identifier::parse("window").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::Array {
                                element: Box::new(nervix_models::ParseAsType::F32),
                                len: 2,
                            },
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("long_schema").expect("valid identifier"),
                        fields: vec![SchemaField {
                            name: Identifier::parse("window").expect("valid identifier"),
                            ty: nervix_models::ParseAsType::Array {
                                element: Box::new(nervix_models::ParseAsType::F32),
                                len: 3,
                            },
                            optional: false,
                            sensitive: false,
                        }],
                    }),
                    relay("short_stream", "short_schema"),
                    relay("long_stream", "long_schema"),
                    relay("merged", "short_schema"),
                    branch_schema("window_branch", &["window"]),
                    branch_for_relay("short_stream", "window_branch"),
                    junction("merge_windows", &["short_stream", "long_stream"], "merged"),
                ],
            )
            .expect_err("array length mismatch should fail");

        assert!(
            format!("{err:#}").contains("differ"),
            "unexpected error: {err:#}"
        );
        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_field_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay("deduped", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.transaction_id",
                        "10m",
                    ),
                ],
            )
            .expect_err("missing dedup field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(format!("{err}").contains("DEDUPLICATE ON compile failed"));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlate_where_non_boolean_predicate() {
        let (domain, models) = example_graph_models(
            "correlator non-boolean predicate",
            r#"
            CREATE SCHEMA event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA event UNBRANCHED;
            CREATE RELAY right_events SCHEMA event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE lower(left.value)
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-boolean CORRELATE WHERE must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err:#}").contains("CORRELATE WHERE compile failed"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_correlator_side_scoped_from_where() {
        let (domain, models) = example_graph_models(
            "correlator side source predicates",
            r#"
            CREATE SCHEMA left_event (
              value STRING,
              marker I64
            );

            CREATE SCHEMA right_event (
              value STRING,
              active BOOL
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
            CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events WHERE left.marker > 0
              RIGHT FROM right_events WHERE right.active
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        registry
            .apply_batch(&domain, models)
            .expect("side source predicates should use their correlator side scope");

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlate_where_non_input_namespace() {
        let (domain, models) = example_graph_models(
            "correlator non-input namespace",
            r#"
            CREATE SCHEMA tenant_branch (
              tenant STRING
            );

            CREATE SCHEMA event (
              tenant STRING,
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA event BRANCHED BY by_tenant_branch;
            CREATE RELAY right_events SCHEMA event BRANCHED BY by_tenant_branch;
            CREATE RELAY correlated_events SCHEMA correlated_event BRANCHED BY by_tenant_branch;
            CREATE BRANCH by_tenant_branch
              SCHEMA tenant_branch TTL 5m;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE input.tenant = left.tenant
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              BRANCHED BY by_tenant_branch
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("non-input CORRELATE WHERE namespace must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("CORRELATE WHERE compile failed") && rendered.contains("input"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_validates_each_correlator_output_against_its_destination_schema() {
        let (domain, models) = example_graph_models(
            "correlator destination schemas",
            r#"
            CREATE SCHEMA event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE SCHEMA correlation_count (
              count I64
            );

            CREATE RELAY left_events SCHEMA event UNBRANCHED;
            CREATE RELAY right_events SCHEMA event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;
            CREATE RELAY correlation_counts SCHEMA correlation_count UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events
              RIGHT FROM right_events
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG
              TO correlation_counts
                SET count = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("each correlator route must use its own destination schema");

        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("correlator TO output 'correlation_counts' compile failed"),
            "unexpected error: {rendered}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_correlator_left_side_schema_mismatch() {
        let (domain, models) = example_graph_models(
            "correlator left schema mismatch",
            r#"
            CREATE SCHEMA left_event (
              value STRING
            );

            CREATE SCHEMA other_left_event (
              value I64
            );

            CREATE SCHEMA right_event (
              value STRING
            );

            CREATE SCHEMA correlated_event (
              value STRING
            );

            CREATE RELAY left_events SCHEMA left_event UNBRANCHED;
            CREATE RELAY other_left_events SCHEMA other_left_event UNBRANCHED;
            CREATE RELAY right_events SCHEMA right_event UNBRANCHED;
            CREATE RELAY correlated_events SCHEMA correlated_event UNBRANCHED;

            CREATE CORRELATOR correlate_events
              LEFT FROM left_events, other_left_events
              RIGHT FROM right_events
              CORRELATE WHERE left.value = right.value
              MATCH EARLIEST
              MAX TIME 5s
              ON CORRELATION TIMEOUT DROP, DROP
              UNBRANCHED
              TO correlated_events
                SET value = left.value
                FLUSH IMMEDIATE
                ON MESSAGE ERROR LOG;
            "#,
        );
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");

        let err = registry
            .apply_batch(&domain, models)
            .expect_err("same-side correlator schema mismatch must fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err:#}").contains("correlator left input requires equal internal schemas"),
            "unexpected error: {err:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_message_target() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("summaries", "event_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    window_processor(
                        "window",
                        "notifications",
                        "summaries",
                        "SET message.value = COUNT(input.value)",
                    ),
                ],
            )
            .expect_err("message is not a window output target");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("window SET targets must be bare or output.<field>"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_window_aggregate_argument_outside_input() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("summaries", "event_schema", "notifications"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    window_processor(
                        "window",
                        "notifications",
                        "summaries",
                        "SET value = COUNT(output.value)",
                    ),
                ],
            )
            .expect_err("aggregate arguments must read the original input");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("window aggregate arguments may read only input fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_branched_by_fields_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    branch_schema("missing_key_branch", &["missing_key"]),
                    branch_for_relay("notifications", "missing_key_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["missing_key"],
                    ),
                ],
            )
            .expect_err("missing branch field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("unknown finalized output field 'missing_key'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incomplete_ingestor_branch_construction() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    client_model("broker_in_2"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing_a",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    Model::Ingestor(CreateIngestor {
                        name: identifier("ing_b"),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::single(identifier("notifications")))
                                .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                            OutputBranch::BranchedBy {
                                branch: branch_name_for_relay("notifications"),
                                assignments: vec![Assignment {
                                    target: AssignmentTarget {
                                        scope: AssignmentTargetScope::Bare,
                                        field: identifier("user_id"),
                                    },
                                    value: Expression::Field(FieldReference::scoped(
                                        FieldScope::Message,
                                        identifier("user_id"),
                                    )),
                                }],
                            },
                        ),
                        decode_using_codec: identifier("event_codec"),
                        timestamp_source: None,
                        source: IngestSource::Kafka {
                            client: identifier("broker_in_2"),
                            topic: identifier("notifications"),
                            offset_mode: KafkaOffsetMode::ConsumerGroup(identifier("cg")),
                            instances: 1,
                            mode: KafkaIngestMode::AckSequential {
                                timeout: "30s".to_string(),
                                retry_policy: nervix_models::RetryPolicy {
                                    backoff: "200ms".to_string(),
                                    max_backoff: "5s".to_string(),
                                },
                            },
                        },
                        general_error_policy: GeneralErrorPolicy::Log,
                        filter_where: None,
                    }),
                ],
            )
            .expect_err("every required branch field must be initialized");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("required branch field 'tenant' remains uninitialized"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_ingestor_branch_name_mismatch_with_same_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");
        let Model::Ingestor(mut ingestor) = ingestor_with_params(
            "ing",
            "notifications",
            "event_codec",
            "broker_in",
            &["value"],
        ) else {
            unreachable!("ingestor helper must build an ingestor model")
        };
        let Some(OutputBranch::BranchedBy {
            branch: ingestor_branch,
            ..
        }) = &mut ingestor.output_routes.routes[0].branch
        else {
            unreachable!("ingestor helper must build a branched ingestor")
        };
        *ingestor_branch = identifier("branch_b");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by("notifications", "event_schema", "branch_a"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Ingestor(ingestor),
                ],
            )
            .expect_err("differently named ingestor and relay branches must be incompatible");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains("must use its exact declared branch 'branch_a'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_processor_crossing_same_schema_branch_names() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");
        let Model::Deduplicator(mut processor) = processor("project", "input", "output") else {
            unreachable!("processor helper must build a deduplicator model")
        };
        processor.branched_by = BranchSelection::branched_by(identifier("branch_b"));

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    relay_branched_by("input", "event_schema", "branch_a"),
                    relay_branched_by("output", "event_schema", "branch_b"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Deduplicator(processor),
                ],
            )
            .expect_err("normal processors must not cross differently named branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "branch name 'branch_b' does not match relay 'input' branch name 'branch_a'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_generator_crossing_same_schema_branch_names() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");
        let Model::Relay(mut input) = relay_branched_by("input", "event_schema", "branch_a") else {
            unreachable!("relay helper must build a relay model")
        };
        input.materialized_state = Some(MaterializedRelayState::LastByTimestamp);

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    Model::Relay(input),
                    relay_branched_by("output", "event_schema", "branch_b"),
                    branch_schema("value_branch", &["value"]),
                    branch("branch_a", "value_branch"),
                    branch("branch_b", "value_branch"),
                    Model::Generator(CreateGenerator {
                        name: identifier("generate"),
                        materialized_relay: identifier("input"),
                        branched_by: BranchSelection::branched_by(identifier("branch_b")),
                        each: "100ms".to_string(),
                        output_routes: ProcessorOutputs::new(vec![ProcessorOutput {
                            relay: identifier("output"),
                            construction: nervix_nspl::parse_route_construction(
                                "SET value = relay_state.input.value",
                            )
                            .expect("generator route must parse"),
                            flush_policy: Some(nervix_models::OutputFlushPolicy {
                                flush_each: "IMMEDIATE".to_string(),
                                max_batch_size: None,
                            }),
                            message_error_policy: MessageErrorPolicy::Log,
                            branch: None,
                        }]),
                    }),
                ],
            )
            .expect_err("generators must not cross differently named branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "generator 'generate' branch name 'branch_b' does not match relay 'input' branch \
                 name 'branch_a'"
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_duplicate_vhost_hostnames() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    vhost("edge", &["api.example.com"]),
                    vhost("edge_internal", &["api.example.com"]),
                ],
            )
            .expect_err("duplicate hostname should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("hostname 'api.example.com' is already assigned"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_deduplicator_chain() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_like("projected", "event_schema", "notifications"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    with_processor_branching(processor("project", "notifications", "projected")),
                ],
            )
            .expect("graph with inherited branch fields should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let projected = graph
            .node(
                ModelKind::Relay,
                &Identifier::parse("projected").expect("valid identifier"),
            )
            .expect("projected relay should exist");

        assert_eq!(
            projected
                .effective_branching
                .as_ref()
                .expect("projected relay should be branched")
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["tenant", "user_id"]
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_reingestor_outputs() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by(
                        "notifications",
                        "event_schema",
                        branch_name_for_relay("notifications").as_str(),
                    ),
                    relay_branched_by("errors", "event_schema", "by_route_logs"),
                    relay_branched_by("warnings", "event_schema", "by_route_logs"),
                    relay_branched_by("info", "event_schema", "by_route_logs"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    branch("by_route_logs", "tenant_user_id_branch"),
                    Model::Reingestor(CreateReingestor {
                        name: identifier("route_logs"),
                        from: ProcessorInputs::single(identifier("notifications")),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::new(vec![
                                ProcessorOutput {
                                    relay: identifier("errors"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.value = "error""#,
                                    )
                                    .expect("route construction must parse"),
                                    flush_policy: None,
                                    message_error_policy: MessageErrorPolicy::Log,
                                    branch: None,
                                },
                                ProcessorOutput {
                                    relay: identifier("warnings"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.value = "warn""#,
                                    )
                                    .expect("route construction must parse"),
                                    flush_policy: None,
                                    message_error_policy: MessageErrorPolicy::Log,
                                    branch: None,
                                },
                                ProcessorOutput::new(identifier("info")),
                            ]))
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                            branched_by("route_logs", &["tenant", "user_id"]),
                        ),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect("reingestor graph should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");

        for relay_name in ["errors", "warnings", "info"] {
            let relay = graph
                .node(
                    ModelKind::Relay,
                    &Identifier::parse(relay_name).expect("valid identifier"),
                )
                .expect("routed relay should exist");

            assert_eq!(
                relay
                    .effective_branching
                    .as_ref()
                    .expect("routed relay should be branched")
                    .iter()
                    .map(Identifier::as_str)
                    .collect::<Vec<_>>(),
                vec!["tenant", "user_id"]
            );
        }

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_output_predicate_missing_from_schema() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_by_relay_branch("errors", "event_schema"),
                    relay_branched_like("info", "event_schema", "errors"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    branch_for_relay("errors", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    Model::Reingestor(CreateReingestor {
                        name: identifier("route_logs"),
                        from: ProcessorInputs::single(identifier("notifications")),
                        output_routes: with_output_branch(
                            with_inherit_all(ProcessorOutputs::new(vec![
                                ProcessorOutput {
                                    relay: identifier("errors"),
                                    construction: nervix_nspl::parse_route_construction(
                                        r#"WHERE input.missing = "error""#,
                                    )
                                    .expect("route construction must parse"),
                                    flush_policy: None,
                                    message_error_policy: MessageErrorPolicy::Log,
                                    branch: None,
                                },
                                ProcessorOutput::new(identifier("info")),
                            ]))
                            .with_flush_policy("100ms".to_string(), Some("1MiB".to_string())),
                            OutputBranch::BranchedBy {
                                branch: branch_name_for_relay("notifications"),
                                assignments: Vec::new(),
                            },
                        ),
                        mode: AckMode::Attached,
                        filter_where: None,
                        materialized_state: Vec::new(),
                    }),
                ],
            )
            .expect_err("reingestor output predicate on missing field should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::InvalidModel { .. }
        ));
        assert!(
            format!("{err}").contains("unknown input field 'missing'"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_without_explicit_upstream_branching_alias() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_like("projected", "event_schema", "notifications"),
                    processor("project", "notifications", "projected"),
                ],
            )
            .expect_err("deduplicator without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "deduplicator 'project' requires relay 'notifications' to have branch fields",
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_infers_stream_branching_through_deduplicators() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let changes = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("notification").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("transaction_id")
                                    .expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("transaction_id")
                                    .expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "notification"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "notification"),
                    relay_branched_like("deduped", "notification", "notifications"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    with_processor_branching(deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.transaction_id",
                        "10m",
                    )),
                ],
            )
            .expect("graph with deduplicator branch fields should succeed");

        let schedule = changes
            .graph
            .expect("graph should be present")
            .schedule_for_domain(&domain, &["node-1".to_string()], 0);
        let deduped = scheduled_node(&schedule, ModelKind::Relay, "deduped");
        assert_eq!(
            deduped.effective_branching,
            Some(vec![Identifier::parse("tenant").expect("valid identifier")])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_deduplicator_without_explicit_upstream_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("notifications", "value_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_like("deduped", "event_schema", "notifications"),
                    deduplicator(
                        "dedup",
                        "notifications",
                        "deduped",
                        "notifications.value",
                        "10m",
                    ),
                ],
            )
            .expect_err("deduplicator without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains(
                "deduplicator 'dedup' requires relay 'notifications' to have branch fields",
            ),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_constructs_reingestor_target_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("notifications", "event_schema"),
                    relay_branched_by_relay_branch("tenant_notifications", "event_schema"),
                    branch_schema_with_types(
                        "tenant_user_id_branch",
                        &[
                            ("tenant", ParseAsType::String),
                            ("user_id", ParseAsType::I64),
                        ],
                    ),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("notifications", "tenant_user_id_branch"),
                    branch_for_relay("tenant_notifications", "tenant_branch"),
                    ingestor_with_params(
                        "ing",
                        "notifications",
                        "event_codec",
                        "broker_in",
                        &["tenant", "user_id"],
                    ),
                    reingestor(
                        "tenant_partition",
                        "notifications",
                        "tenant_notifications",
                        &["tenant"],
                    ),
                ],
            )
            .expect("graph with reingestor branch fields should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let target = graph
            .node(
                ModelKind::Relay,
                &Identifier::parse("tenant_notifications").expect("valid identifier"),
            )
            .expect("target relay should exist");

        assert_eq!(
            target
                .effective_branching
                .as_ref()
                .expect("target relay should be branched")
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["tenant"]
        );
        assert_eq!(
            target
                .effective_branching_schema
                .as_ref()
                .map(Identifier::as_str),
            Some("tenant_branch")
        );

        let dataflow_graph = graph.to_dataflow_graph(domain.as_str());
        let branching_schemas = dataflow_graph
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node.branching_schema.as_deref()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(branching_schemas.get("ingestor:ing"), Some(&None));
        assert_eq!(
            branching_schemas.get("reingestor:tenant_partition"),
            Some(&None)
        );
        assert_eq!(
            branching_schemas.get("relay:tenant_notifications"),
            Some(&Some("tenant_branch"))
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_accepts_reingestor_from_unbranched_source_to_branched_target() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::U32,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("tenant_notifications", "tenant_branch"),
                    relay("notifications", "event_schema"),
                    relay_branched_by_relay_branch("tenant_notifications", "event_schema"),
                    reingestor(
                        "tenant_partition",
                        "notifications",
                        "tenant_notifications",
                        &["tenant"],
                    ),
                ],
            )
            .expect("reingestor may repartition an explicitly unbranched source");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        let source = graph
            .node(ModelKind::Relay, &identifier("notifications"))
            .expect("source relay should exist");
        assert_eq!(source.effective_branching, Some(Vec::new()));
        assert_eq!(source.effective_branching_schema, None);

        let target = graph
            .node(ModelKind::Relay, &identifier("tenant_notifications"))
            .expect("target relay should exist");
        assert_eq!(
            target
                .effective_branching
                .as_ref()
                .expect("target relay should be branched")
                .iter()
                .map(Identifier::as_str)
                .collect::<Vec<_>>(),
            vec!["tenant"]
        );
        assert_eq!(
            target
                .effective_branching_schema
                .as_ref()
                .map(Identifier::as_str),
            Some("tenant_branch")
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_junction_without_explicit_upstream_branching() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("left", "value_branch"),
                    relay("left", "event_schema"),
                    relay("right", "event_schema"),
                    relay_branched_like("merged", "event_schema", "left"),
                    junction("join_streams", &["left", "right"], "merged"),
                ],
            )
            .expect_err("junction without upstream branch fields should fail");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}")
                .contains("junction 'join_streams' requires relay 'left' to have branch fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_rejects_incompatible_branches_for_one_relay() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let err = registry
            .apply_batch(
                &domain,
                vec![
                    Model::Schema(CreateSchema {
                        name: Identifier::parse("event_schema").expect("valid identifier"),
                        fields: vec![
                            SchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            },
                            SchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: nervix_models::ParseAsType::String,
                                optional: false,
                                sensitive: false,
                            },
                        ],
                    }),
                    Model::WireSchema(CreateWireSchemaStmt::Json(CreateWireSchema {
                        name: Identifier::parse("event_wire").expect("valid identifier"),
                        strictness: Default::default(),
                        fields: vec![
                            WireSchemaField {
                                name: Identifier::parse("tenant").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("user_id").expect("valid identifier"),
                                ty: JsonType::Integer,
                                optional: false,
                            },
                            WireSchemaField {
                                name: Identifier::parse("value").expect("valid identifier"),
                                ty: JsonType::String,
                                optional: false,
                            },
                        ],
                    })),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    client_model("broker_in_2"),
                    relay_branched_by_relay_branch("left", "event_schema"),
                    relay_branched_by_relay_branch("right", "event_schema"),
                    relay_branched_like("merged", "event_schema", "left"),
                    branch_schema("tenant_branch", &["tenant"]),
                    branch_for_relay("left", "tenant_branch"),
                    ingestor_with_params(
                        "ing_left",
                        "left",
                        "event_codec",
                        "broker_in",
                        &["tenant"],
                    ),
                    branch_schema_with_types("user_id_branch", &[("user_id", ParseAsType::I64)]),
                    branch_for_relay("right", "user_id_branch"),
                    ingestor_with_params(
                        "ing_right",
                        "right",
                        "event_codec",
                        "broker_in_2",
                        &["user_id"],
                    ),
                    with_processor_branching(processor("left_proc", "left", "merged")),
                    with_processor_branching(processor("right_proc", "right", "merged")),
                ],
            )
            .expect_err("one relay cannot receive incompatible branches");

        assert!(matches!(
            err.current_context(),
            RegistryError::IncompatibleSchema { .. }
        ));
        assert!(
            format!("{err}").contains("conflicting branch fields"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn apply_batch_is_order_independent() {
        let domain = Domain::parse("default").expect("valid domain");

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
        let domain = Domain::parse("default").expect("valid domain");

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
                .get(
                    &domain,
                    ModelKind::Schema,
                    &Identifier::parse("event_schema").expect("valid identifier")
                )
                .expect("read should succeed")
                .is_none()
        );
        assert!(
            registry
                .get(
                    &domain,
                    ModelKind::Client,
                    &Identifier::parse("broker_out").expect("valid identifier")
                )
                .expect("read should succeed")
                .is_none()
        );
        assert!(
            registry
                .get(
                    &domain,
                    ModelKind::Emitter,
                    &Identifier::parse("emit").expect("valid identifier")
                )
                .expect("read should succeed")
                .is_none()
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn deduplicator_dependencies_participate_in_candidate_graph_validation() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

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
    fn apply_batch_builds_full_graph_in_single_batch() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        let graph = registry
            .active_graph(&domain)
            .expect("graph should be installed");
        assert_eq!(graph.node_count(), 12);
        assert_eq!(graph.edge_count(), 21);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_deduplicator_between_two_relays() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    relay_branched_by_relay_branch("raw_events", "event_schema"),
                    relay_branched_like("deduped_events", "event_schema", "raw_events"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("raw_events", "value_branch"),
                    ingestor_with_params(
                        "ingest_events",
                        "raw_events",
                        "event_codec",
                        "broker_in",
                        &["value"],
                    ),
                    processor("dedup_events", "raw_events", "deduped_events"),
                ],
            )
            .expect("deduplicator graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"relay:raw_events"),
            "raw relay missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"deduplicator:dedup_events"),
            "deduplicator missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:deduped_events"),
            "deduped relay missing from {node_ids:?}"
        );
        let branching_schemas = dataflow_graph
            .nodes
            .iter()
            .map(|node| (node.id.as_str(), node.branching_schema.as_deref()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(branching_schemas.get("ingestor:ingest_events"), Some(&None));
        assert_eq!(
            branching_schemas.get("relay:raw_events"),
            Some(&Some("value_branch"))
        );
        assert_eq!(
            branching_schemas.get("relay:deduped_events"),
            Some(&Some("value_branch"))
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "deduplicator:dedup_events"),
                ("deduplicator:dedup_events", "relay:deduped_events"),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_wasm_processor_between_two_relays() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    explicitly_unbranched_relay("filtered_events", "event_schema"),
                    unbranched_ingestor("ingest_events", "raw_events", "event_codec", "broker_in"),
                    wasm_processor("filter_events", "raw_events", "filtered_events"),
                ],
            )
            .expect("wasm processor graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"relay:raw_events"),
            "raw relay missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"wasm_processor:filter_events"),
            "wasm processor missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:filtered_events"),
            "filtered relay missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "wasm_processor:filter_events"),
                ("wasm_processor:filter_events", "relay:filtered_events"),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_keeps_reused_ingest_and_emit_client_nodes_separate() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker"),
                    explicitly_unbranched_relay("raw_events", "event_schema"),
                    unbranched_ingestor("ingest_events", "raw_events", "event_codec", "broker"),
                    emitter("emit_events", "raw_events", "event_codec", "broker"),
                ],
            )
            .expect("client reuse graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            node_ids.contains("client_source:broker"),
            "source client missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains("client_sink:broker"),
            "sink client missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker", "ingestor:ingest_events"),
                ("ingestor:ingest_events", "relay:raw_events"),
                ("relay:raw_events", "emitter:emit_events"),
                ("emitter:emit_events", "client_sink:broker"),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_includes_correlator_between_input_and_output_relays() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    explicitly_unbranched_relay("left_events", "event_schema"),
                    explicitly_unbranched_relay("right_events", "event_schema"),
                    explicitly_unbranched_relay("matched_events", "event_schema"),
                    explicitly_unbranched_relay("uncorrelated_left_events", "event_schema"),
                    explicitly_unbranched_relay("uncorrelated_right_events", "event_schema"),
                    explicitly_unbranched_relay("correlator_errors", "event_schema"),
                    {
                        let Model::Correlator(mut correlator) = unbranched_correlator(
                            "match_events",
                            "left_events",
                            "right_events",
                            "matched_events",
                        ) else {
                            unreachable!("helper must return correlator")
                        };
                        correlator.timeout_policy = CorrelationTimeoutPolicy {
                            left: CorrelationTimeoutAction::SendTo {
                                relay: identifier("uncorrelated_left_events"),
                            },
                            right: CorrelationTimeoutAction::SendTo {
                                relay: identifier("uncorrelated_right_events"),
                            },
                        };
                        correlator.output_routes.routes[0].message_error_policy =
                            MessageErrorPolicy::Dlq {
                                relay: identifier("correlator_errors"),
                                assignments: vec![Assignment {
                                    target: AssignmentTarget::bare(identifier("value")),
                                    value: nervix_nspl::parse_expression("left.value")
                                        .expect("error assignment must parse"),
                                }],
                            };
                        Model::Correlator(correlator)
                    },
                ],
            )
            .expect("correlator graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"correlator:match_events"),
            "correlator missing from {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str(), edge.kind))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                (
                    "relay:left_events",
                    "correlator:match_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "relay:right_events",
                    "correlator:match_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "correlator:match_events",
                    "relay:matched_events",
                    DataflowEdgeKind::Data,
                ),
                (
                    "correlator:match_events",
                    "relay:uncorrelated_left_events",
                    DataflowEdgeKind::CorrelationTimeout,
                ),
                (
                    "correlator:match_events",
                    "relay:uncorrelated_right_events",
                    DataflowEdgeKind::CorrelationTimeout,
                ),
                (
                    "correlator:match_events",
                    "relay:correlator_errors",
                    DataflowEdgeKind::MessageError,
                ),
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn correlator_message_error_set_uses_structured_scopes_and_all_optional_partial_output() {
        fn error_schema() -> Model {
            Model::Schema(CreateSchema {
                name: identifier("error_schema"),
                fields: vec![
                    SchemaField {
                        name: identifier("reference"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: identifier("fields"),
                        ty: ParseAsType::Vec {
                            element: Box::new(ParseAsType::String),
                        },
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: identifier("attempted"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                ],
            })
        }

        let models_with_policy = |assignment_source: &str| {
            let Model::Correlator(mut correlator) = unbranched_correlator(
                "match_events",
                "left_events",
                "right_events",
                "matched_events",
            ) else {
                unreachable!("helper must return correlator")
            };
            correlator.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Dlq {
                relay: identifier("correlator_errors"),
                assignments: vec![
                    Assignment {
                        target: AssignmentTarget::bare(identifier("reference")),
                        value: nervix_nspl::parse_expression("error.reference")
                            .expect("error reference must parse"),
                    },
                    Assignment {
                        target: AssignmentTarget::bare(identifier("fields")),
                        value: nervix_nspl::parse_expression("error.fields")
                            .expect("error fields must parse"),
                    },
                    Assignment {
                        target: AssignmentTarget::bare(identifier("attempted")),
                        value: nervix_nspl::parse_expression(assignment_source)
                            .expect("attempted value must parse"),
                    },
                ],
            };
            vec![
                schema("event_schema"),
                error_schema(),
                explicitly_unbranched_relay("left_events", "event_schema"),
                explicitly_unbranched_relay("right_events", "event_schema"),
                explicitly_unbranched_relay("matched_events", "event_schema"),
                explicitly_unbranched_relay("correlator_errors", "error_schema"),
                Model::Correlator(correlator),
            ]
        };

        let valid_path = temp_db_path();
        Registry::open(&valid_path)
            .expect("registry should open")
            .apply_batch(
                &Domain::parse("default").expect("valid domain"),
                models_with_policy("coalesce(partial_output.value, 'missing')"),
            )
            .expect("structured correlator error construction should validate");
        let _ = fs::remove_dir_all(valid_path);

        let invalid_path = temp_db_path();
        let error = Registry::open(&invalid_path)
            .expect("registry should open")
            .apply_batch(
                &Domain::parse("default").expect("valid domain"),
                models_with_policy("input.value"),
            )
            .expect_err("correlator error construction must not expose input");
        assert!(format!("{error:?}").contains("input"));
        let _ = fs::remove_dir_all(invalid_path);
    }

    #[test]
    fn message_error_relay_requires_the_exact_named_branch() {
        let Model::Correlator(mut correlator) = unbranched_correlator(
            "match_events",
            "left_events",
            "right_events",
            "matched_events",
        ) else {
            unreachable!("helper must return correlator")
        };
        correlator.branched_by = BranchSelection::branched_by(identifier("event_branch"));
        correlator.output_routes.routes[0].message_error_policy = MessageErrorPolicy::Dlq {
            relay: identifier("correlator_errors"),
            assignments: vec![Assignment {
                target: AssignmentTarget::bare(identifier("value")),
                value: nervix_nspl::parse_expression("left.value")
                    .expect("correlator error input must parse"),
            }],
        };
        let path = temp_db_path();
        let error = Registry::open(&path)
            .expect("registry should open")
            .apply_batch(
                &Domain::parse("default").expect("valid domain"),
                vec![
                    schema("event_schema"),
                    schema("error_schema"),
                    branch_schema("branch_key", &["tenant"]),
                    branch("event_branch", "branch_key"),
                    branch("error_branch", "branch_key"),
                    relay_branched_by("left_events", "event_schema", "event_branch"),
                    relay_branched_by("right_events", "event_schema", "event_branch"),
                    relay_branched_by("matched_events", "event_schema", "event_branch"),
                    relay_branched_by("correlator_errors", "error_schema", "error_branch"),
                    Model::Correlator(correlator),
                ],
            )
            .expect_err("structurally equal but differently named branches must be rejected");

        let rendered = format!("{error:?}");
        assert!(rendered.contains("message-error relay 'correlator_errors'"));
        assert!(rendered.contains("error_branch"));
        assert!(rendered.contains("event_branch"));
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn dataflow_graph_excludes_synthetic_materializer_nodes() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    wire_schema("event_wire"),
                    codec("event_codec", "event_schema"),
                    client_model("broker_in"),
                    materialized_relay("state_txns", "event_schema"),
                    branch_schema("value_branch", &["value"]),
                    branch_for_relay("state_txns", "value_branch"),
                    ingestor_with_params(
                        "state_txns_ingestor",
                        "state_txns",
                        "event_codec",
                        "broker_in",
                        &["value"],
                    ),
                ],
            )
            .expect("materialized relay graph should succeed");

        let dataflow_graph = registry
            .active_graph(&domain)
            .expect("graph should be installed")
            .to_dataflow_graph(domain.as_str());

        let node_ids = dataflow_graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        assert!(
            node_ids.contains(&"ingestor:state_txns_ingestor"),
            "ingestor missing from {node_ids:?}"
        );
        assert!(
            node_ids.contains(&"relay:state_txns"),
            "relay missing from {node_ids:?}"
        );
        assert!(
            !node_ids.contains(&"materializer:state_txns"),
            "materializer must not be part of dataflow graph: {node_ids:?}"
        );
        let edges = dataflow_graph
            .edges
            .iter()
            .map(|edge| (edge.source.as_str(), edge.target.as_str()))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            edges,
            std::collections::BTreeSet::from([
                ("client_source:broker_in", "ingestor:state_txns_ingestor"),
                ("ingestor:state_txns_ingestor", "relay:state_txns")
            ])
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn drop_batch_removes_unused_model() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

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
                    name: Identifier::parse("broker_in").expect("valid identifier"),
                }],
            )
            .expect("drop should succeed");

        assert!(
            registry
                .get(
                    &domain,
                    ModelKind::Client,
                    &Identifier::parse("broker_in").expect("valid identifier")
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
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        let err = registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Schema,
                    name: Identifier::parse("event_schema").expect("valid identifier"),
                }],
            )
            .expect_err("drop should be rejected while schema is in use");

        assert!(matches!(
            err.current_context(),
            RegistryError::DeleteInUse { .. }
        ));
        assert!(
            registry
                .get(
                    &domain,
                    ModelKind::Schema,
                    &Identifier::parse("event_schema").expect("valid identifier")
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
        let domain = Domain::parse("default").expect("valid domain");

        registry
            .apply_batch(&domain, full_graph_batch())
            .expect("full graph batch should succeed");

        registry
            .drop_batch(
                &domain,
                vec![DropModel {
                    kind: ModelKind::Emitter,
                    name: Identifier::parse("emit").expect("valid identifier"),
                }],
            )
            .expect("emitter should be droppable");

        assert!(
            registry
                .get(
                    &domain,
                    ModelKind::Emitter,
                    &Identifier::parse("emit").expect("valid identifier")
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
        let domain = Domain::parse("default").expect("valid domain");

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
                    name: Identifier::parse("output").expect("valid identifier"),
                }],
            )
            .expect_err("deduplicator output relay should be blocked");

        assert!(matches!(
            err.current_context(),
            RegistryError::DeleteInUse { .. }
        ));

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn ingestor_rejects_protobuf_codec_without_decode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    protobuf_codec("event_codec", "event_schema", None, Some("{payload: .}")),
                    client_model("kafka_main"),
                    relay("notifications", "event_schema"),
                    ingestor("ing", "notifications", "event_codec", "kafka_main"),
                ],
            )
            .expect_err("ingestor must reject encode-only protobuf codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for decoding because it does not declare an \
                 ON INGESTION transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn emitter_rejects_protobuf_codec_without_encode_capability() {
        let path = temp_db_path();
        let registry = Registry::open(&path).expect("registry should open");
        let domain = Domain::parse("default").expect("valid domain");

        let error = registry
            .apply_batch(
                &domain,
                vec![
                    schema("event_schema"),
                    protobuf_codec("event_codec", "event_schema", Some("."), None),
                    client_model("broker_out"),
                    relay("notifications", "event_schema"),
                    emitter("emit", "notifications", "event_codec", "broker_out"),
                ],
            )
            .expect_err("emitter must reject decode-only protobuf codec");

        assert!(
            format!("{error:#}").contains(
                "codec 'event_codec' cannot be used for encoding because it does not declare an \
                 ON EMITTING transformation"
            ),
            "unexpected error: {error:#}"
        );

        let _ = fs::remove_dir_all(path);
    }
}
