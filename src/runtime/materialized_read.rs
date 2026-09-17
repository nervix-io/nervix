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
    /// Every materialized record one relay holds on this node, each reported with the concrete
    /// branch it belongs to.
    ///
    /// This is the relay-scoped report, and it says so. Reading one branch is a different
    /// operation that names that branch.
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
            let placement = self.state_placement(
                domain,
                RuntimeState::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            );
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
        reports.sort_by(|left, right| {
            branch_key_display(&left.branch).cmp(branch_key_display(&right.branch))
        });
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
        for placement in self.materialized_record_placements(domain, relay, branch_key) {
            let state = self
                .inner
                .replicated_materialized_stream_states
                .get(&placement)
                .map(|state| ReplicatedMaterializedRelayState::read(state.value()));
            let Some(state) = state else {
                continue;
            };
            if !self.materialized_stream_key_is_visible(Some(routing), &placement, branch_key) {
                continue;
            }
            if let Some(record) = state.record(branch_key) {
                return Ok(Some(record));
            }
        }
        for placement in self.materialized_record_placements(domain, relay, branch_key) {
            let Some(restored) = self
                .open_stored_materialized_snapshot(Some(routing), &placement)
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
    ) -> Vec<RuntimeStatePlacement> {
        let relay_scoped = self.state_placement(
            domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        let Some(branch_key) = branch_key.clone() else {
            return vec![relay_scoped];
        };
        vec![
            RuntimeStatePlacement {
                branch_key: Some(branch_key),
                ..relay_scoped.clone()
            },
            relay_scoped,
        ]
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
        let Some(snapshot) = store.latest_snapshot(placement).change_context(
            MaterializedReadError::StoredSnapshot {
                placement: placement.clone(),
            },
        )?
        else {
            return Ok(None);
        };
        let Some(schema) = self.materialized_relay_schema(routing, placement) else {
            return Ok(None);
        };
        let sealed = self
            .inner
            .executor
            .charge_owned(nervix_execution::MemoryClass::Bulk, snapshot.payload)
            .await
            .change_context(MaterializedReadError::StoredSnapshot {
                placement: placement.clone(),
            })?;
        RestoredMaterializedSnapshot::open(
            &self.inner.executor,
            &schema,
            placement.schema_fingerprint,
            SealedSource::memory(sealed),
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
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
        {
            return Some(
                ReplicatedMaterializedRelayState::read(state.value())
                    .schema()
                    .clone(),
            );
        }
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
        let expiring_placement = self.state_placement(
            &placement.domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            &placement.identifier,
            None,
        );
        self.inner
            .expiring_stream_states
            .get(&expiring_placement)
            .is_none_or(|state| state.registry.contains_key(key))
    }

    /// Every materialized record one relay holds on another node, reported the same way as the
    /// local relay-scoped view.
    pub(crate) async fn remote_materialized_stream_state(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
    ) -> error_stack::Result<Vec<MaterializedRecordReport>, MaterializedReadError> {
        let placement = self.state_placement(
            domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
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
        reports.sort_by(|left, right| {
            branch_key_display(&left.branch).cmp(branch_key_display(&right.branch))
        });
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
        let placement = self.state_placement(
            domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
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
        let states = self
            .inner
            .replicated_materialized_stream_states
            .iter()
            .filter(|state| {
                let key = state.key();
                key.domain == *domain
                    && key.kind == ModelKind::Relay
                    && key.identifier == ModelName::from(relay)
            })
            .map(|state| {
                (
                    state.key().clone(),
                    ReplicatedMaterializedRelayState::read(state.value()),
                )
            })
            .collect::<Vec<_>>();
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
        for (placement, state) in states {
            for record in state.records() {
                if self.materialized_stream_key_is_visible(
                    Some(&routing),
                    &placement,
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
        let placement = self.state_placement(
            domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
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
            tokio::task::consume_budget().await;
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
            tokio::task::consume_budget().await;
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
            tokio::task::consume_budget().await;
            let execution_now = domain_clock
                .snapshot()
                .change_context(MaterializedReadError::DomainClock {
                    domain: domain.clone(),
                    branch: batch.key.clone(),
                })?
                .now();
            let changed = self.inner.materialized_state_changed.notified();
            match self
                .resolve_materialized_dependencies(
                    routing.load(),
                    domain,
                    &batch.key,
                    dependencies,
                    execution_now,
                )
                .await?
            {
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
                        required_wait = Some(AckRequiredWaitGuard::new(batch.acks.iter()));
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
                    tokio::select! {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use ahash::HashMap;
    use nervix_execution::sync::ArcSwapOption;
    use nervix_interconnect::{RemoteOperationFailure, RemoteOperationSubject};
    use nervix_models::{Assignment, AssignmentTarget, DomainSchedule, Expression, ParseAsType};
    use tokio::{
        sync::watch,
        time::{Duration, timeout},
    };

    use super::*;
    use crate::{
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{RuntimeRecordMetadata, RuntimeValue},
    };

    #[test]
    fn remote_materialized_read_distinguishes_assignment_gaps_from_failures() {
        let target = ClusterNodeName::parse("node-1").expect("the test node name is valid");
        let domain = domain("default");
        let relay = named::<RelayName>("profiles");
        let runtime = Runtime::default();
        let placement = runtime.state_placement(
            &domain,
            RuntimeState::MaterializedRelay,
            ModelKind::Relay,
            &relay,
            None,
        );

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

    #[tokio::test]
    async fn materialized_dependencies_resolve_defaults_and_stop_in_declaration_order() {
        let runtime = Runtime::default();
        let domain = domain("default");
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
        let relay_registries = [(named("input"), RelayRegistry::new())]
            .into_iter()
            .collect();
        runtime.install_domain_execution(
            &domain,
            DomainExecution {
                schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                routing: runtime.stage_domain_routing(
                    &domain,
                    DomainRoutingSnapshot {
                        relay_registries,
                        materialized_stream_specs,
                        ..DomainRoutingSnapshot::default()
                    },
                ),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                endpoint_routes: HashMap::default(),
                node_tasks: HashMap::default(),
                emitter_tasks: HashMap::default(),
                generator_tasks: HashMap::default(),
                reingestor_tasks: HashMap::default(),
                placement_tasks: HashMap::default(),
                relay_state_tasks: HashMap::default(),
                relay_owner_tasks: HashMap::default(),
                clients: HashMap::default(),
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
                "an empty non-owner relay registry must not evict retained branch work"
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
