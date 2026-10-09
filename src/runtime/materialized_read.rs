//! Reading an installed materialized dependency from its selected state owner.
//!
//! Layer: data plane.
//! - **Owns.** Resolving local or remote materialized rows and reporting dependency-read failures.
//! - **Depends on.** Prepared materialized plans, runtime row carriers, state placement and exchange.
//! - **Must not know.** Models, NSPL parsing or archive restore policy.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "materialized dependencies are read repeatedly by the concrete branch processing \
                  path"
    )
)]

use std::pin::Pin;

use error_stack::ResultExt as _;

use super::{state_snapshot_exchange::MaterializedSnapshotExchangeError, *};

#[derive(Debug, Error)]
pub(crate) enum MaterializedReadError {
    #[error("failed to open stored {placement}")]
    StoredSnapshot { placement: RuntimeStatePlacement },
    #[error("failed to fetch {placement} from node '{target}'")]
    RemoteSnapshot {
        target: ClusterNodeName,
        placement: RuntimeStatePlacement,
    },
    #[error(
        "failed to read field '{field}' from materialized relay '{relay}' in domain '{domain}'"
    )]
    Field {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
        field: String,
    },
    #[error("materialized relay '{relay}' in domain '{domain}' requires a concrete branch")]
    CurrentBranchRequired {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
    },
    #[error("materialized relay '{relay}' is not instantiated in domain '{domain}'")]
    RelayUnavailable {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
    },
    #[error("materialized relay '{relay}' in domain '{domain}' has no published state identity")]
    StateIdentity {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
    },
    #[error("materialized relay '{relay}' is declared more than once in domain '{domain}'")]
    DuplicateDependency {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
    },
    #[error(
        "failed to evaluate default field '{field}' for materialized relay '{relay}' in domain \
         '{domain}'"
    )]
    DefaultExpression {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
        field: FieldName,
    },
    #[error(
        "default materialized relay '{relay}' in domain '{domain}' did not initialize required \
         field '{field}'"
    )]
    DefaultRequiredField {
        domain: DomainName,
        relay: RelayName,
        branch: Option<BranchKey>,
        field: String,
    },
    #[error("failed to read the execution clock for domain '{domain}'")]
    DomainClock {
        domain: DomainName,
        branch: Option<BranchKey>,
    },
    #[error("failed to render a materialized record")]
    RecordReport { branch: Option<BranchKey> },
}

impl MaterializedReadError {
    fn from_remote_snapshot_result(
        result: error_stack::Result<
            Option<RestoredMaterializedSnapshot>,
            MaterializedSnapshotExchangeError,
        >,
        target_node_id: &ClusterNodeName,
        placement: RuntimeStatePlacement,
    ) -> error_stack::Result<Option<RestoredMaterializedSnapshot>, Self> {
        match result {
            Ok(restored) => Ok(restored),
            Err(error) if error.current_context().is_between_assignments() => Ok(None),
            Err(error) => Err(error.change_context(Self::RemoteSnapshot {
                target: target_node_id.clone(),
                placement,
            })),
        }
    }
}

pub(super) struct MaterializedRelayRead<'a> {
    pub(super) relay: &'a RelayName,
    pub(super) key_mode: MaterializedLookupKeyMode,
    pub(super) schema: &'a StdArc<arrow_schema::Schema>,
    pub(super) fields: &'a [MaterializedFieldInterest],
}

impl Runtime {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "domain routing binds each materialized relay's installed-state publication \
                      once"
        )
    )]
    pub(super) fn materialized_relay_publications(
        &self,
        domain: &DomainName,
        specs: &HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    ) -> HashMap<RelayName, state_replication::routing::MaterializedRelayPublication> {
        let mut publications = HashMap::default();
        for relay in specs.keys() {
            let entity = DomainNodeRef::node_in(domain.clone(), ModelKind::Relay, relay);
            let published = self
                .inner
                .state_replication_routing
                .materialized(&entity)
                .assured(
                    "each materialized relay assignment is registered before routing is staged",
                );
            publications.insert(relay.clone(), published);
        }
        publications
    }

    /// Every materialized record one relay holds on this node, each reported with the concrete
    /// branch it belongs to.
    ///
    /// This is the relay-scoped report, and it says so. Reading one branch is a different
    /// operation that names that branch.
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            observer,
            reason = "the public report enumerates installed states on observer request"
        )
    )]
    pub(crate) async fn local_materialized_stream_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<Vec<MaterializedRecordReport>, MaterializedReadError> {
        let routing = self
            .domain_routing(domain)
            .map(|routing| routing.load_full());
        let states = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|state| {
                let placement = state.key();
                placement.domain == *domain
                    && placement.kind == ModelKind::Relay
                    && placement.identifier == ModelName::from(relay)
            })
            .map(|state| {
                (
                    state.key().clone(),
                    ReplicatedMaterializedRelayState::read(state.value()),
                )
            })
            .collect::<Vec<_>>();
        let mut reports = Vec::new();
        let mut found = false;
        for (placement, state) in states {
            found = true;
            for record in state.records() {
                if !self.materialized_stream_key_is_visible(
                    routing.as_deref(),
                    &placement,
                    &record.branch,
                ) {
                    continue;
                }
                reports.push(materialized_record_report(&record)?);
            }
        }
        if !found {
            let placement = match self.state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            ) {
                Ok(placement) => placement,
                // This node has applied no schedule that carries the relay, so it holds no state
                // for it: the local report is empty.
                Err(error)
                    if matches!(
                        error.current_context(),
                        StateIdentityError::SchemaFingerprintUnpublished { .. }
                    ) =>
                {
                    return Ok(reports);
                }
                Err(error) => {
                    return Err(error.change_context(MaterializedReadError::StateIdentity {
                        domain: domain.clone(),
                        relay: relay.clone(),
                        branch: None,
                    }));
                }
            };
            if let Some(restored) = self
                .open_stored_materialized_snapshot(None, &placement)
                .await?
            {
                for record in restored.records {
                    reports.push(materialized_record_report(&MaterializedGenerationRecord {
                        branch: record.branch,
                        row: record.row,
                    })?);
                }
            }
        }
        reports.sort_by(|left, right| BranchKey::canonical_order(&left.branch, &right.branch));
        Ok(reports)
    }

    /// Find the branch's record wherever this node keeps it.
    ///
    /// A relay scheduled on the cluster keeps every branch's record in one relay-owned state; a
    /// branch-local relay keeps each branch's record in its own. Both are addressed by naming the
    /// branch, and both answer with that branch's record alone.
    async fn local_materialized_record(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
    ) -> error_stack::Result<Option<MaterializedGenerationRecord>, MaterializedReadError> {
        let placements = self.materialized_record_placements(domain, relay, branch_key)?;
        let root = placements
            .last()
            .assured("a materialized read always has its relay placement");
        if !self.materialized_stream_key_is_visible(Some(routing), root, branch_key) {
            return Ok(None);
        }
        if let Some(published) = routing.materialized_stream_reads.get(relay) {
            if let Some(record) = published.record(branch_key) {
                return Ok(Some(record));
            }
            // Live state owns absence too. Storage may still hold its pre-eviction checkpoint.
            if published.has_state_for(branch_key) {
                return Ok(None);
            }
        }
        for placement in &placements {
            let Some(restored) = self
                .open_stored_materialized_snapshot(Some(routing), placement)
                .await?
            else {
                continue;
            };
            if let Some(record) = restored
                .records
                .into_iter()
                .find(|record| record.branch == *branch_key)
            {
                return Ok(Some(MaterializedGenerationRecord {
                    branch: record.branch,
                    row: record.row,
                }));
            }
        }
        Ok(None)
    }

    /// The placements that may hold one branch's record, most specific first.
    fn materialized_record_placements(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
    ) -> error_stack::Result<Vec<RuntimeStatePlacement>, MaterializedReadError> {
        let relay_scoped = self
            .state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            )
            .change_context_lazy(|| MaterializedReadError::StateIdentity {
                domain: domain.clone(),
                relay: relay.clone(),
                branch: branch_key.clone(),
            })?;
        let Some(branch_key) = branch_key.clone() else {
            return Ok(vec![relay_scoped]);
        };
        Ok(vec![
            RuntimeStatePlacement {
                branch_key: Some(branch_key),
                ..relay_scoped.clone()
            },
            relay_scoped,
        ])
    }

    /// Open the sealed snapshot this node persisted for a placement, if it has one.
    async fn open_stored_materialized_snapshot(
        &self,
        routing: Option<&DomainRoutingSnapshot>,
        placement: &RuntimeStatePlacement,
    ) -> error_stack::Result<Option<RestoredMaterializedSnapshot>, MaterializedReadError> {
        let Some(store) = &self.inner.state_store else {
            return Ok(None);
        };
        let Some(schema) = self.materialized_relay_schema(routing, placement) else {
            return Ok(None);
        };
        let store = store.clone();
        let selected = placement.clone();
        let charge = self
            .inner
            .executor
            .reserve(
                nervix_execution::MemoryClass::Bulk,
                RESTORE_STATE_WORKING_BYTES,
            )
            .await
            .change_context(MaterializedReadError::StoredSnapshot {
                placement: placement.clone(),
            })?;
        let reader = self
            .inner
            .executor
            .run_storage(
                nervix_execution::StorageClass::Filesystem,
                charge,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(RuntimePersistenceError::Cancelled)?;
                    store.checkpoint_reader(&selected)
                },
            )
            .await
            .change_context(MaterializedReadError::StoredSnapshot {
                placement: placement.clone(),
            })?
            .change_context(MaterializedReadError::StoredSnapshot {
                placement: placement.clone(),
            })?;
        let Some(reader) = reader else {
            return Ok(None);
        };
        RestoredMaterializedSnapshot::open_relay(
            &self.inner.executor,
            &schema,
            SealedSource::stored(self.inner.executor.clone(), reader),
        )
        .await
        .map(Some)
        .change_context(MaterializedReadError::StoredSnapshot {
            placement: placement.clone(),
        })
    }

    fn materialized_relay_schema(
        &self,
        routing: Option<&DomainRoutingSnapshot>,
        placement: &RuntimeStatePlacement,
    ) -> Option<StdArc<arrow_schema::Schema>> {
        if let Some(routing) = routing {
            return routing
                .materialized_stream_specs
                .get(&RelayName::from(&placement.identifier))
                .map(|spec| spec.schema.clone());
        }
        let published = self.domain_routing(&placement.domain)?;
        published
            .load()
            .materialized_stream_specs
            .get(&RelayName::from(&placement.identifier))
            .map(|spec| spec.schema.clone())
    }

    pub(super) async fn local_materialized_stream_values_for_branch(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
        fields: &[MaterializedFieldInterest],
    ) -> error_stack::Result<Option<Vec<Option<RuntimeValue>>>, MaterializedReadError> {
        let Some(record) = self
            .local_materialized_record(routing, domain, relay, branch_key)
            .await?
        else {
            return Ok(None);
        };
        fields
            .iter()
            .map(|field| {
                record.row.value_at(field.column_index).change_context(
                    MaterializedReadError::Field {
                        domain: domain.clone(),
                        relay: relay.clone(),
                        branch: branch_key.clone(),
                        field: field.name.clone(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(super) async fn remote_materialized_stream_values_for_branch(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
        schema: &StdArc<arrow_schema::Schema>,
        fields: &[MaterializedFieldInterest],
    ) -> error_stack::Result<Option<Vec<Option<RuntimeValue>>>, MaterializedReadError> {
        let Some(record) = self
            .remote_materialized_record(target_node_id, domain, relay, branch_key, schema)
            .await?
        else {
            return Ok(None);
        };
        fields
            .iter()
            .map(|field| {
                record.row.value_at(field.column_index).change_context(
                    MaterializedReadError::Field {
                        domain: domain.clone(),
                        relay: relay.clone(),
                        branch: branch_key.clone(),
                        field: field.name.clone(),
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(super) async fn load_materialized_relay_values(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        read: MaterializedRelayRead<'_>,
    ) -> error_stack::Result<Option<Vec<Option<RuntimeValue>>>, MaterializedReadError> {
        let MaterializedRelayRead {
            relay,
            key_mode,
            schema,
            fields,
        } = read;
        let placement_branch_key = match key_mode {
            MaterializedLookupKeyMode::CurrentBranch => {
                let Some(key) = branch_key.as_ref() else {
                    return Err(Report::new(MaterializedReadError::CurrentBranchRequired {
                        domain: domain.clone(),
                        relay: relay.clone(),
                        branch: branch_key.clone(),
                    }));
                };
                Some(key.clone())
            }
            MaterializedLookupKeyMode::Root => None,
        };
        // The schedule can publish a materialized relay's destination before that destination
        // activates its prepared state. The ownership gate already fences records headed to the
        // relay across that interval; dependency reads observe the same fence so a REQUIRED WAIT
        // batch remains parked instead of racing a snapshot transfer against activation.
        if routing
            .relay_services
            .get(relay)
            .is_some_and(|services| services.dispatch_is_fenced())
        {
            return Ok(None);
        }
        let owner = routing
            .materialized_stream_owner_nodes
            .get(relay)
            .and_then(|node| node.as_ref())
            .cloned();
        if let Some(owner) = owner
            && !self.is_local_node(&owner)
        {
            return self
                .remote_materialized_stream_values_for_branch(
                    &owner,
                    domain,
                    relay,
                    &placement_branch_key,
                    schema,
                    fields,
                )
                .await;
        }
        self.local_materialized_stream_values_for_branch(
            routing,
            domain,
            relay,
            &placement_branch_key,
            fields,
        )
        .await
    }

    pub(super) fn materialized_stream_key_is_visible(
        &self,
        routing: Option<&DomainRoutingSnapshot>,
        placement: &RuntimeStatePlacement,
        key: &Option<BranchKey>,
    ) -> bool {
        let scheduled = if let Some(routing) = routing
            && let Some(owner) = routing
                .materialized_stream_owner_nodes
                .get(&RelayName::from(&placement.identifier))
        {
            owner.is_some()
        } else {
            false
        };
        if scheduled {
            return true;
        }
        if let Some(routing) = routing
            && let Some(services) = routing
                .relay_services
                .get(&RelayName::from(&placement.identifier))
        {
            return services.branch_presence.contains(key.as_ref());
        }
        true
    }

    /// Every materialized record one relay holds on another node, reported the same way as the
    /// local relay-scoped view.
    pub(crate) async fn remote_materialized_stream_state(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<Vec<MaterializedRecordReport>, MaterializedReadError> {
        let placement = self
            .state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            )
            .change_context_lazy(|| MaterializedReadError::StateIdentity {
                domain: domain.clone(),
                relay: relay.clone(),
                branch: None,
            })?;
        let Some(schema) = self.materialized_relay_schema(None, &placement) else {
            return Ok(Vec::new());
        };
        let Some(restored) = self
            .fetch_sealed_materialized_snapshot(target_node_id, &placement, &schema, None)
            .await
            .change_context(MaterializedReadError::RemoteSnapshot {
                target: target_node_id.clone(),
                placement: placement.clone(),
            })?
        else {
            return Ok(Vec::new());
        };
        let mut reports = restored
            .records
            .into_iter()
            .map(|record| {
                materialized_record_report(&MaterializedGenerationRecord {
                    branch: record.branch,
                    row: record.row,
                })
            })
            .collect::<error_stack::Result<Vec<_>, MaterializedReadError>>()?;
        reports.sort_by(|left, right| BranchKey::canonical_order(&left.branch, &right.branch));
        Ok(reports)
    }

    /// Every materialized record one relay holds, taken from the node that owns them.
    ///
    /// Records keep their columns and their typed concrete branch identity: nothing is turned into
    /// named scalar fields on the way, and nothing is rebuilt into a row on arrival.
    pub(in crate::runtime) async fn materialized_records_from_owner(
        &self,
        routing: &mut DomainRoutingCache,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<Vec<MaterializedGenerationRecord>, MaterializedReadError> {
        let routing = routing.load().clone();
        let placement = self
            .state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            )
            .change_context_lazy(|| MaterializedReadError::StateIdentity {
                domain: domain.clone(),
                relay: relay.clone(),
                branch: None,
            })?;
        let owner = routing
            .materialized_stream_owner_nodes
            .get(relay)
            .cloned()
            .flatten();
        if let Some(owner) = owner
            && !self.is_local_node(&owner)
        {
            let Some(schema) = self.materialized_relay_schema(Some(&routing), &placement) else {
                return Ok(Vec::new());
            };
            let Some(restored) = self
                .fetch_sealed_materialized_snapshot(&owner, &placement, &schema, None)
                .await
                .change_context(MaterializedReadError::RemoteSnapshot {
                    target: owner.clone(),
                    placement: placement.clone(),
                })?
            else {
                return Ok(Vec::new());
            };
            return Ok(restored
                .records
                .into_iter()
                .map(|record| MaterializedGenerationRecord {
                    branch: record.branch,
                    row: record.row,
                })
                .collect());
        }
        let states = match routing.materialized_stream_reads.get(relay) {
            Some(published) => published.states(),
            None => Vec::new(),
        };
        if states.is_empty() {
            let Some(restored) = self
                .open_stored_materialized_snapshot(Some(&routing), &placement)
                .await?
            else {
                return Ok(Vec::new());
            };
            return Ok(restored
                .records
                .into_iter()
                .map(|record| MaterializedGenerationRecord {
                    branch: record.branch,
                    row: record.row,
                })
                .collect());
        }
        let mut records = Vec::new();
        for state in states {
            for record in state.records() {
                if self.materialized_stream_key_is_visible(
                    Some(&routing),
                    state.placement(),
                    &record.branch,
                ) {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }

    /// The record of exactly one branch of one relay, read from the node that owns it.
    async fn remote_materialized_record(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
        schema: &StdArc<arrow_schema::Schema>,
    ) -> error_stack::Result<Option<MaterializedGenerationRecord>, MaterializedReadError> {
        let placement = self
            .state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            )
            .change_context_lazy(|| MaterializedReadError::StateIdentity {
                domain: domain.clone(),
                relay: relay.clone(),
                branch: branch_key.clone(),
            })?;
        let restored = MaterializedReadError::from_remote_snapshot_result(
            self.fetch_sealed_materialized_snapshot(target_node_id, &placement, schema, None)
                .await,
            target_node_id,
            placement,
        )?;
        let Some(restored) = restored else {
            return Ok(None);
        };
        Ok(restored
            .records
            .into_iter()
            .find(|record| record.branch == *branch_key)
            .map(|record| MaterializedGenerationRecord {
                branch: record.branch,
                row: record.row,
            }))
    }

    pub(crate) async fn load_materialized_side_inputs(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        interest: &MaterializedProgramInterest,
    ) -> error_stack::Result<HashMap<String, RuntimeValue>, MaterializedReadError> {
        let mut values = HashMap::default();
        if interest.relays.is_empty() {
            return Ok(values);
        }

        for relay_interest in &interest.relays {
            nervix_primitives::task::consume_budget().await;
            let Some(relay_values) = self
                .load_materialized_relay_values(
                    routing,
                    domain,
                    branch_key,
                    MaterializedRelayRead {
                        relay: &relay_interest.relay,
                        key_mode: relay_interest.key_mode,
                        schema: &relay_interest.schema,
                        fields: &relay_interest.fields,
                    },
                )
                .await?
            else {
                continue;
            };
            for (field, value) in relay_interest.fields.iter().zip(relay_values) {
                let Some(value) = value else {
                    continue;
                };
                values.insert(
                    format!(
                        "relay_state.{}.{}",
                        relay_interest.relay.as_str(),
                        field.name
                    ),
                    value,
                );
            }
        }

        Ok(values)
    }

    pub(in crate::runtime) async fn load_materialized_dependency_values(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        relay: &RelayName,
    ) -> error_stack::Result<Option<HashMap<String, RuntimeValue>>, MaterializedReadError> {
        let Some(spec) = routing.materialized_stream_specs.get(relay) else {
            return Err(Report::new(MaterializedReadError::RelayUnavailable {
                domain: domain.clone(),
                relay: relay.clone(),
                branch: branch_key.clone(),
            }));
        };

        let key_mode = if spec.branching.is_unbranched() {
            MaterializedLookupKeyMode::Root
        } else {
            MaterializedLookupKeyMode::CurrentBranch
        };
        let Some(field_values) = self
            .load_materialized_relay_values(
                routing,
                domain,
                branch_key,
                MaterializedRelayRead {
                    relay,
                    key_mode,
                    schema: &spec.schema,
                    fields: &spec.fields,
                },
            )
            .await?
        else {
            return Ok(None);
        };
        let values = spec
            .fields
            .iter()
            .zip(field_values)
            .filter_map(|(field, value)| {
                value.map(|value| {
                    (
                        format!("relay_state.{}.{}", relay.as_str(), field.name),
                        value,
                    )
                })
            })
            .collect();
        Ok(Some(values))
    }

    pub(in crate::runtime) async fn resolve_materialized_dependencies(
        &self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        dependencies: &[nervix_models::MaterializedStateDependency],
        execution_now: Timestamp,
    ) -> error_stack::Result<MaterializedDependencyResolution, MaterializedReadError> {
        if dependencies.is_empty() {
            return Ok(MaterializedDependencyResolution::Ready(HashMap::default()));
        }
        let mut resolved = HashMap::default();
        let mut declared = HashSet::default();
        for dependency in dependencies {
            nervix_primitives::task::consume_budget().await;
            if !declared.insert(dependency.relay.clone()) {
                return Err(Report::new(MaterializedReadError::DuplicateDependency {
                    domain: domain.clone(),
                    relay: dependency.relay.clone(),
                    branch: branch_key.clone(),
                }));
            }
            if let Some(values) = self
                .load_materialized_dependency_values(routing, domain, branch_key, &dependency.relay)
                .await?
            {
                resolved.extend(values);
                continue;
            }
            match &dependency.policy {
                MaterializedStatePolicy::RequiredSkip => {
                    return Ok(MaterializedDependencyResolution::Skip);
                }
                MaterializedStatePolicy::RequiredWait => {
                    return Ok(MaterializedDependencyResolution::Wait);
                }
                MaterializedStatePolicy::Default(assignments) => {
                    let Some(spec) = routing.materialized_stream_specs.get(&dependency.relay)
                    else {
                        return Err(Report::new(MaterializedReadError::RelayUnavailable {
                            domain: domain.clone(),
                            relay: dependency.relay.clone(),
                            branch: branch_key.clone(),
                        }));
                    };
                    for assignment in assignments {
                        if matches!(
                            assignment.value,
                            nervix_models::Expression::Literal(ModelLiteral::Null)
                        ) {
                            continue;
                        }
                        let value = evaluate_constant_expression_vm(
                            self.executor(),
                            &assignment.value,
                            Some(&routing.udfs),
                            execution_now,
                        )
                        .await
                        .map_err(|reason| {
                            Report::new(MaterializedReadError::DefaultExpression {
                                domain: domain.clone(),
                                relay: dependency.relay.clone(),
                                branch: branch_key.clone(),
                                field: assignment.target.field.clone(),
                            })
                            .attach_printable(reason)
                        })?;
                        resolved.insert(
                            format!(
                                "relay_state.{}.{}",
                                dependency.relay, assignment.target.field
                            ),
                            value,
                        );
                    }
                    for field in spec
                        .schema
                        .fields()
                        .iter()
                        .filter(|field| !field.is_nullable())
                    {
                        let qualified =
                            format!("relay_state.{}.{}", dependency.relay.as_str(), field.name());
                        if !resolved.contains_key(&qualified) {
                            return Err(Report::new(MaterializedReadError::DefaultRequiredField {
                                domain: domain.clone(),
                                relay: dependency.relay.clone(),
                                branch: branch_key.clone(),
                                field: field.name().clone(),
                            }));
                        }
                    }
                }
            }
        }
        Ok(MaterializedDependencyResolution::Ready(resolved))
    }

    pub(in crate::runtime) async fn resolve_materialized_dependencies_for_batch(
        &self,
        handles: MaterializedDomainHandles<'_>,
        input_relay: &RelayName,
        dependencies: &[nervix_models::MaterializedStateDependency],
        batch: RelayRecordBatch,
        wait: MaterializedBatchWaitContext<'_>,
    ) -> error_stack::Result<
        Option<(RelayRecordBatch, HashMap<String, RuntimeValue>, Timestamp)>,
        MaterializedReadError,
    > {
        let MaterializedDomainHandles {
            routing,
            domain_clock,
            domain,
        } = handles;
        let MaterializedBatchWaitContext {
            shutdown_rx,
            wait_for_required_state,
            mut quiesce_work,
        } = wait;
        let mut required_wait = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let execution_now = domain_clock
                .snapshot()
                .change_context(MaterializedReadError::DomainClock {
                    domain: domain.clone(),
                    branch: batch.key.clone(),
                })?
                .now();
            let (resolution, changed) = self
                .observe_materialized_dependencies(
                    routing.load(),
                    domain,
                    &batch.key,
                    dependencies,
                    execution_now,
                )
                .await?;
            match resolution {
                MaterializedDependencyResolution::Ready(values) => {
                    if let Some(work) = quiesce_work.as_deref_mut() {
                        work.resume_from_required_materialized_state();
                    }
                    drop(required_wait.take());
                    return Ok(Some((batch, values, execution_now)));
                }
                MaterializedDependencyResolution::Skip => {
                    if let Some(work) = quiesce_work.as_deref_mut() {
                        work.resume_from_required_materialized_state();
                    }
                    drop(required_wait.take());
                    for ack in batch.acks.iter() {
                        ack.ack_success();
                    }
                    return Ok(None);
                }
                MaterializedDependencyResolution::Wait => {
                    if required_wait.is_none() {
                        required_wait = Some(AckParkGuard::new(batch.acks.iter()));
                        if let Some(work) = quiesce_work.as_deref_mut() {
                            work.park_for_required_materialized_state();
                        }
                    }
                    if !wait_for_required_state {
                        for ack in batch.acks.iter() {
                            ack.no_ack(format!(
                                "node stopped while waiting for required materialized state at \
                                 relay '{}'",
                                input_relay
                            ));
                        }
                        return Ok(None);
                    }
                    nervix_primitives::select! {
                        _ = changed => {}
                        _ = sleep(self.inner.state_replication_poll_interval) => {}
                        result = shutdown_rx.changed() => {
                            if result.is_err() || *shutdown_rx.borrow() {
                                for ack in batch.acks.iter() {
                                    ack.no_ack(format!(
                                        "node stopped while waiting for required materialized state \
                                         at relay '{}'",
                                        input_relay
                                    ));
                                }
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Registration and observation are one protocol boundary. The returned future already
    /// observes notify_waiters, including a publication after the read and before its first poll.
    pub(in crate::runtime) async fn observe_materialized_dependencies<'a>(
        &'a self,
        routing: &DomainRoutingSnapshot,
        domain: &DomainName,
        branch: &Option<BranchKey>,
        dependencies: &[nervix_models::MaterializedStateDependency],
        execution_now: Timestamp,
    ) -> error_stack::Result<
        (
            MaterializedDependencyResolution,
            nervix_primitives::sync::futures::Notified<'a>,
        ),
        MaterializedReadError,
    > {
        let changed = self.inner.materialized_state_changed.notified();
        let resolution = self
            .resolve_materialized_dependencies(routing, domain, branch, dependencies, execution_now)
            .await?;
        Ok((resolution, changed))
    }
}

/// Render one materialized record for the public relay-state report.
fn materialized_record_report(
    record: &MaterializedGenerationRecord,
) -> error_stack::Result<MaterializedRecordReport, MaterializedReadError> {
    Ok(MaterializedRecordReport {
        branch: record.branch.clone(),
        payload: record.row.to_json_string().change_context(
            MaterializedReadError::RecordReport {
                branch: record.branch.clone(),
            },
        )?,
        ingested_at_low_watermark: record.row.metadata().ingested_at_low_watermark(),
        ingested_at_high_watermark: record.row.metadata().ingested_at_high_watermark(),
    })
}

/// The wait of one branch task for the materialized state its parked messages need.
///
/// A message parks when the state it needs is missing, and the owner of that state notifies every
/// waiter once it changes. A notification that lands between the read that parked a message and a
/// registration made afterwards wakes nothing, so the wait registers when it is made, before the
/// task processes anything that may park, stays registered until a change wakes it, and registers
/// again before the task retries its parked messages, which reads the state again.
pub(in crate::runtime) struct MaterializedStateWait<'runtime> {
    notify: &'runtime Notify,
    changed: Pin<Box<nervix_primitives::sync::futures::Notified<'runtime>>>,
}

impl<'runtime> MaterializedStateWait<'runtime> {
    pub(in crate::runtime) fn new(notify: &'runtime Notify) -> Self {
        let mut changed = Box::pin(notify.notified());
        changed.as_mut().enable();
        Self { notify, changed }
    }

    /// Waits for a change of materialized state since the wait was made or last registered again.
    pub(in crate::runtime) async fn changed(&mut self) {
        self.changed.as_mut().await;
    }

    /// Registers for the next change before a retry reads the state again.
    pub(in crate::runtime) fn register_again(&mut self) {
        self.changed.set(self.notify.notified());
        self.changed.as_mut().enable();
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use nervix_model_harness::shuttle::check_random_and_pct;
    use nervix_primitives::sync::atomic::AtomicBool;

    use super::*;

    const MODEL_TASK_JOINS: &str =
        "Shuttle fails the whole execution when a model task panics, so no join observes one";

    /// Waits the way a branch task waits for a parked message's state: it reads the state that
    /// decides whether the message stays parked, and waits for the next change while it does.
    async fn wait_until_present(notify: Arc<Notify>, present: Arc<AtomicBool>) {
        let mut wait = MaterializedStateWait::new(&notify);
        loop {
            nervix_primitives::task::consume_budget().await;
            if present.load(Ordering::SeqCst) {
                return;
            }
            wait.changed().await;
            wait.register_again();
        }
    }

    /// The owner of a materialized state publishes the state a branch task's parked message needs
    /// while the task decides to park it: the task's wait observes that publication.
    fn a_parked_message_is_woken_by_the_state_it_waits_for() {
        shuttle::future::block_on(async {
            let notify = Arc::new(Notify::new());
            let present = Arc::new(AtomicBool::new(false));
            let waiting =
                nervix_primitives::task::spawn(wait_until_present(notify.clone(), present.clone()));
            let publishing = nervix_primitives::task::spawn(async move {
                present.store(true, Ordering::SeqCst);
                notify.notify_waiters();
            });
            publishing.await.assured(MODEL_TASK_JOINS);
            waiting.await.assured(MODEL_TASK_JOINS);
        });
    }

    #[test]
    fn shuttle_a_parked_message_is_woken_by_the_materialized_state_it_waits_for() {
        check_random_and_pct(a_parked_message_is_woken_by_the_state_it_waits_for);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ahash::HashMap;
    use nervix_interconnect::{RemoteOperationFailure, RemoteOperationSubject};
    use nervix_models::{Assignment, AssignmentTarget, Expression, ParseAsType};
    use nervix_primitives::{sync::watch, time::timeout};

    use super::*;
    use crate::{
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{RuntimeRecordMetadata, RuntimeValue},
    };

    #[nervix_primitives::test]
    async fn stored_materialized_reads_open_containers_larger_than_the_bulk_budget() {
        use meticulous::ResultExt as _;

        use super::super::state_store::generation::CheckpointMetadata;
        use crate::runtime_schema::test_runtime_row;

        let root = tempfile::tempdir().expect("database directory opens");
        let db = fjall::Database::builder(root.path())
            .open()
            .expect("database opens");
        let runtime = Runtime::with_persistence(Some(db), DEFAULT_STATE_SNAPSHOT_INTERVAL)
            .expect("persistent runtime opens");
        let rows = (0..80)
            .map(|index| MaterializedGenerationRecord {
                branch: string_branch_key("tenant", &format!("tenant-{index:02}")),
                row: test_runtime_row([(
                    "payload".to_string(),
                    RuntimeValue::String("p".repeat(512 * 1024)),
                )]),
            })
            .collect::<Vec<_>>();
        let schema = rows[0].row.arrow_schema();
        let generation = MaterializedGeneration::new(80, 0, 80, schema.clone(), rows);
        let sealed = generation
            .seal(runtime.executor(), &runtime.inner.snapshot_staging)
            .await
            .assured("bounded groups seal");
        assert!(sealed.descriptor.length > 32 * 1024 * 1024);
        let placement = RuntimeStatePlacement {
            domain: domain("default"),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]).materialized_at(37),
            },
            kind: ModelKind::Relay,
            identifier: named("state"),
            branch_key: None,
        };
        runtime
            .inner
            .state_store
            .as_ref()
            .expect("store exists")
            .checkpoint_stream_writer()
            .publish_checkpoint_stream(
                &placement,
                CheckpointMetadata {
                    lsm: sealed.descriptor.revision,
                    length: sealed.descriptor.length,
                    digest: sealed.descriptor.digest,
                },
                std::fs::File::open(sealed.artifact.path()).expect("sealed file opens"),
                || Ok(()),
            )
            .assured("segmented checkpoint publishes");
        let routing = DomainRoutingSnapshot {
            materialized_stream_specs: HashMap::from_iter([(
                named("state"),
                RuntimeMaterializedRelaySpec::new(
                    schema,
                    VmSchemaSensitivity::default(),
                    test_branching(&[("tenant", ParseAsType::String)]),
                ),
            )]),
            ..DomainRoutingSnapshot::default()
        };
        let restored = runtime
            .open_stored_materialized_snapshot(Some(&routing), &placement)
            .await
            .assured("stored read fits the default bulk budget")
            .expect("the persisted generation exists");
        assert_eq!(restored.revision, 80);
        assert_eq!(restored.branch_generation, 80);
        assert_eq!(restored.records.len(), 80);
        for (index, record) in restored.records.iter().enumerate() {
            assert_eq!(
                record.branch,
                string_branch_key("tenant", &format!("tenant-{index:02}"))
            );
            assert_eq!(
                record.row.value_at(0).expect("payload reads"),
                Some(RuntimeValue::String("p".repeat(512 * 1024)))
            );
        }
    }

    #[test]
    fn remote_materialized_read_distinguishes_assignment_gaps_from_failures() {
        let target = ClusterNodeName::parse("node-1").expect("the test node name is valid");
        let domain = domain("default");
        let relay = named::<RelayName>("profiles");
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeState::MaterializedRelay {
                schema: SchemaFingerprint::from_digest([7; 32]),
            },
            kind: ModelKind::Relay,
            identifier: ModelName::from(&relay),
            branch_key: None,
        };

        assert!(
            MaterializedReadError::from_remote_snapshot_result(
                Ok(None),
                &target,
                placement.clone(),
            )
            .expect("successful materialized-state absence remains absence")
            .is_none()
        );

        let failure = MaterializedReadError::from_remote_snapshot_result(
            Err(Report::new(
                MaterializedSnapshotExchangeError::DispatcherUnavailable {
                    target: target.clone(),
                    placement: placement.clone(),
                },
            )),
            &target,
            placement.clone(),
        )
        .expect_err("a runtime outside a cluster cannot fetch remote materialized state");
        assert!(matches!(
            failure.current_context(),
            MaterializedReadError::RemoteSnapshot {
                target: failed_target,
                placement,
            } if failed_target == &target
                && placement.domain == domain
                && placement.identifier == ModelName::from(&relay)
        ));

        assert!(
            MaterializedReadError::from_remote_snapshot_result(
                Err(Report::new(
                    MaterializedSnapshotExchangeError::RemoteFailure {
                        target: target.clone(),
                        placement: placement.clone(),
                        failure: RemoteOperationFailure::not_ready(RemoteOperationSubject::state(
                            &placement.to_remote(),
                        )),
                    }
                )),
                &target,
                placement,
            )
            .expect("an assignment gap is ordinary materialized-state absence")
            .is_none()
        );
    }

    #[nervix_primitives::test]
    async fn materialized_dependencies_resolve_defaults_and_stop_in_declaration_order() {
        let runtime = Runtime::default();
        let domain = domain("default");
        for relay in ["profiles", "rules"] {
            publish_state_identity(
                &runtime,
                &domain,
                ModelKind::Relay,
                named::<ModelName>(relay),
            );
        }
        let state_schema = test_optional_schema(&[
            OptionalTestField {
                name: "status",
                ty: ParseAsType::String,
                optional: false,
            },
            OptionalTestField {
                name: "note",
                ty: ParseAsType::String,
                optional: true,
            },
            OptionalTestField {
                name: "evaluated_at",
                ty: ParseAsType::Datetime,
                optional: true,
            },
        ]);
        let (shutdown, _) = watch::channel(false);
        let materialized_stream_specs = ["profiles", "rules"]
            .into_iter()
            .map(|relay| {
                (
                    named(relay),
                    RuntimeMaterializedRelaySpec::new(
                        state_schema.arrow_schema(),
                        VmSchemaSensitivity::default(),
                        ResolvedBranching::unbranched(),
                    ),
                )
            })
            .collect();
        let relay_services = [(named("input"), test_relay_boundary_services())]
            .into_iter()
            .collect();
        runtime.install_domain_execution(
            &domain,
            DomainExecution {
                revision: test_execution_revision(&domain, Vec::new()),
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown,
                routing: runtime.stage_domain_routing(
                    &domain,
                    DomainRoutingSnapshot {
                        relay_services,
                        materialized_stream_specs,
                        ..DomainRoutingSnapshot::default()
                    },
                ),

                branched_entrypoints: HashMap::default(),
                endpoint_routes: HashMap::default(),
                node_tasks: HashMap::default(),
                emitter_tasks: HashMap::default(),
                generator_tasks: HashMap::default(),
                reingestor_tasks: HashMap::default(),
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks: HashMap::default(),
                tasks: Vec::new(),
            },
        );
        let mut routing = runtime
            .domain_routing_cache(&domain)
            .expect("the test domain execution publishes routing");
        let routing_snapshot = routing.load().clone();
        let domain_clock = test_domain_clock(&domain);

        let default = nervix_models::MaterializedStateDependency {
            relay: named("profiles"),
            policy: nervix_models::MaterializedStatePolicy::Default(vec![
                Assignment {
                    target: AssignmentTarget::bare(named("status")),
                    value: Expression::Literal(nervix_models::Literal::String(
                        "unknown".to_string(),
                    )),
                },
                Assignment {
                    target: AssignmentTarget::bare(named("evaluated_at")),
                    value: nervix_nspl::parse_expression("now()")
                        .expect("NOW is a valid materialized default expression"),
                },
            ]),
        };
        let execution_now = Timestamp::from_unix_nanos(946_684_800_000_000_000);
        let resolved = runtime
            .resolve_materialized_dependencies(
                &routing_snapshot,
                &domain,
                &None,
                std::slice::from_ref(&default),
                execution_now,
            )
            .await
            .expect("default dependency should resolve");
        let MaterializedDependencyResolution::Ready(values) = resolved else {
            panic!("default dependency should be ready");
        };
        assert_eq!(
            values.get("relay_state.profiles.status"),
            Some(&RuntimeValue::String("unknown".to_string()))
        );
        assert!(!values.contains_key("relay_state.profiles.note"));
        assert_eq!(
            values.get("relay_state.profiles.evaluated_at"),
            Some(&RuntimeValue::Datetime(
                execution_now.as_datetime().fixed_offset()
            ))
        );

        let concrete_branch = string_branch_key("tenant", "acme");
        let duplicate_error = runtime
            .resolve_materialized_dependencies(
                &routing_snapshot,
                &domain,
                &concrete_branch,
                &[default.clone(), default.clone()],
                execution_now,
            )
            .await
            .err()
            .expect("a repeated dependency must be a typed failure");
        assert!(matches!(
            duplicate_error.current_context(),
            MaterializedReadError::DuplicateDependency {
                domain: error_domain,
                relay,
                branch,
            } if error_domain == &domain
                && relay == &named("profiles")
                && branch == &concrete_branch
        ));

        let incomplete_default = nervix_models::MaterializedStateDependency {
            relay: named("profiles"),
            policy: nervix_models::MaterializedStatePolicy::Default(vec![Assignment {
                target: AssignmentTarget::bare(named("note")),
                value: Expression::Literal(nervix_models::Literal::String(
                    "optional only".to_string(),
                )),
            }]),
        };
        let incomplete_error = runtime
            .resolve_materialized_dependencies(
                &routing_snapshot,
                &domain,
                &concrete_branch,
                &[incomplete_default],
                execution_now,
            )
            .await
            .err()
            .expect("a default that omits a required field must be a typed failure");
        assert!(matches!(
            incomplete_error.current_context(),
            MaterializedReadError::DefaultRequiredField {
                domain: error_domain,
                relay,
                branch,
                field,
            } if error_domain == &domain
                && relay == &named("profiles")
                && branch == &concrete_branch
                && field == "status"
        ));

        let wait = nervix_models::MaterializedStateDependency {
            relay: named("profiles"),
            policy: nervix_models::MaterializedStatePolicy::RequiredWait,
        };
        let skip = nervix_models::MaterializedStateDependency {
            relay: named("rules"),
            policy: nervix_models::MaterializedStatePolicy::RequiredSkip,
        };
        assert!(matches!(
            runtime
                .resolve_materialized_dependencies(
                    &routing_snapshot,
                    &domain,
                    &None,
                    &[wait.clone(), skip.clone()],
                    Timestamp::from_unix_nanos(1),
                )
                .await
                .expect("missing dependencies should produce a policy outcome"),
            MaterializedDependencyResolution::Wait
        ));
        assert!(matches!(
            runtime
                .resolve_materialized_dependencies(
                    &routing_snapshot,
                    &domain,
                    &None,
                    &[skip, wait],
                    Timestamp::from_unix_nanos(1),
                )
                .await
                .expect("missing dependencies should produce a policy outcome"),
            MaterializedDependencyResolution::Skip
        ));

        let (acks, completion) = AckSet::root();
        let retained_row = state_schema
            .batch_from_test_rows([[(
                "status".to_string(),
                RuntimeValue::String("pending".to_string()),
            )]])
            .expect("required-wait branch Arrow batch must build")
            .runtime_row(0, RuntimeRecordMetadata::test())
            .expect("required-wait branch Arrow row must build");
        let retained = RelayRecordBatch::single(
            state_schema.clone(),
            string_branch_key("tenant", "acme"),
            retained_row,
            acks,
        )
        .expect("required-wait branch batch must build");
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let input_relay = named("input");
        let dependencies = [nervix_models::MaterializedStateDependency {
            relay: named("profiles"),
            policy: nervix_models::MaterializedStatePolicy::RequiredWait,
        }];
        {
            let resolution = runtime.resolve_materialized_dependencies_for_batch(
                MaterializedDomainHandles {
                    routing: &mut routing,
                    domain_clock: &domain_clock,
                    domain: &domain,
                },
                &input_relay,
                &dependencies,
                retained,
                MaterializedBatchWaitContext {
                    shutdown_rx: &mut shutdown_rx,
                    wait_for_required_state: true,
                    quiesce_work: None,
                },
            );
            tokio::pin!(resolution);
            assert!(
                timeout(Duration::from_millis(50), &mut resolution)
                    .await
                    .is_err(),
                "an empty non-owner relay presence must not evict retained branch work"
            );
            shutdown_tx.send_replace(true);
            assert!(
                timeout(Duration::from_secs(1), &mut resolution)
                    .await
                    .expect("shutdown should release retained branch work")
                    .expect("retained branch resolution should not fail")
                    .is_none()
            );
        }
        assert!(matches!(
            completion.wait().await,
            AckOutcome::NoAck(reason)
                if reason.contains("node stopped while waiting for required materialized state")
        ));

        let (acks, completion) = AckSet::root();
        let retained_row = state_schema
            .batch_from_test_rows([[(
                "status".to_string(),
                RuntimeValue::String("pending".to_string()),
            )]])
            .expect("required-wait test Arrow batch must build")
            .runtime_row(0, RuntimeRecordMetadata::test())
            .expect("required-wait test Arrow row must build");
        let retained = RelayRecordBatch::single(state_schema, None, retained_row, acks)
            .expect("required-wait test batch must build");
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        assert!(
            runtime
                .resolve_materialized_dependencies_for_batch(
                    MaterializedDomainHandles {
                        routing: &mut routing,
                        domain_clock: &domain_clock,
                        domain: &domain,
                    },
                    &named("input"),
                    &[nervix_models::MaterializedStateDependency {
                        relay: named("profiles"),
                        policy: nervix_models::MaterializedStatePolicy::RequiredWait,
                    }],
                    retained,
                    MaterializedBatchWaitContext {
                        shutdown_rx: &mut shutdown_rx,
                        wait_for_required_state: false,
                        quiesce_work: None,
                    },
                )
                .await
                .expect("terminal drain must resolve retained materialized work")
                .is_none()
        );
        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck(
                "node stopped while waiting for required materialized state at relay 'input'"
                    .to_string()
            )
        );
    }
}
