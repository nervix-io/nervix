use super::*;

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
    pub async fn local_materialized_stream_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<MaterializedRecordReport>, String> {
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
                if !self.materialized_stream_key_is_visible(&placement, &record.branch) {
                    continue;
                }
                reports.push(materialized_record_report(&record)?);
            }
        }
        if !found {
            let placement = self.state_placement(
                domain,
                RuntimeStateKind::MaterializedRelay,
                ModelKind::Relay,
                relay,
                None,
            );
            if let Some(restored) = self.open_stored_materialized_snapshot(&placement).await? {
                for record in restored.records {
                    reports.push(materialized_record_report(&MaterializedGenerationRecord {
                        branch: record.branch,
                        row: record.row,
                    })?);
                }
            }
        }
        reports.sort_by(|left, right| left.branch.cmp(&right.branch));
        Ok(reports)
    }

    /// Find the branch's record wherever this node keeps it.
    ///
    /// A relay scheduled on the cluster keeps every branch's record in one relay-owned state; a
    /// branch-local relay keeps each branch's record in its own. Both are addressed by naming the
    /// branch, and both answer with that branch's record alone.
    async fn local_materialized_record(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
    ) -> Result<Option<MaterializedGenerationRecord>, String> {
        for placement in self.materialized_record_placements(domain, relay, branch_key) {
            let state = self
                .inner
                .replicated_materialized_stream_states
                .get(&placement)
                .map(|state| ReplicatedMaterializedRelayState::read(state.value()));
            let Some(state) = state else {
                continue;
            };
            if !self.materialized_stream_key_is_visible(&placement, branch_key) {
                continue;
            }
            if let Some(record) = state.record(branch_key) {
                return Ok(Some(record));
            }
        }
        for placement in self.materialized_record_placements(domain, relay, branch_key) {
            let Some(restored) = self.open_stored_materialized_snapshot(&placement).await? else {
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
            RuntimeStateKind::MaterializedRelay,
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
        placement: &RuntimeStatePlacement,
    ) -> Result<Option<RestoredMaterializedSnapshot>, String> {
        let Some(store) = &self.inner.state_store else {
            return Ok(None);
        };
        let Some(snapshot) = store
            .latest_snapshot(placement)
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let Some(schema) = self.materialized_relay_schema(placement) else {
            return Ok(None);
        };
        let sealed = self
            .inner
            .executor
            .charge_owned(nervix_execution::MemoryClass::Bulk, snapshot.payload)
            .await
            .map_err(|error| error.to_string())?;
        RestoredMaterializedSnapshot::open(
            &self.inner.executor,
            &schema,
            placement.schema_fingerprint,
            SealedSource::memory(sealed),
        )
        .await
        .map(Some)
        .map_err(|error| error.to_string())
    }

    fn materialized_relay_schema(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> Option<StdArc<arrow_schema::Schema>> {
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(placement)
        {
            return Some(ReplicatedMaterializedRelayState::read(state.value()).schema().clone());
        }
        let execution = self.inner.executions.get(&placement.domain)?;
        execution
            .materialized_stream_specs
            .get(&RelayName::from(&placement.identifier))
            .map(|spec| spec.schema.clone())
    }

    pub(super) async fn local_materialized_stream_values_for_branch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
        fields: &[MaterializedFieldInterest],
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let Some(record) = self
            .local_materialized_record(domain, relay, branch_key)
            .await?
        else {
            return Ok(None);
        };
        fields
            .iter()
            .map(|field| record.row.value_at(field.column_index))
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
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let Some(record) = self
            .remote_materialized_record(target_node_id, domain, relay, branch_key, schema)
            .await?
        else {
            return Ok(None);
        };
        fields
            .iter()
            .map(|field| record.row.value_at(field.column_index))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(super) async fn load_materialized_relay_values(
        &self,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        read: MaterializedRelayRead<'_>,
        owner_nodes: &HashMap<RelayName, Option<ClusterNodeName>>,
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let MaterializedRelayRead {
            relay,
            key_mode,
            schema,
            fields,
        } = read;
        let placement_branch_key = match key_mode {
            MaterializedLookupKeyMode::CurrentBranch => {
                let Some(key) = branch_key.as_ref() else {
                    return Err(format!(
                        "materialized relay '{}' requires a current branch key",
                        relay.as_str()
                    ));
                };
                Some(key.clone())
            }
            MaterializedLookupKeyMode::Root => None,
        };
        let owner = owner_nodes
            .get(relay)
            .and_then(|node| node.as_ref())
            .cloned();
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        if let Some(owner) = owner
            && local_node_id.as_ref() != Some(&owner)
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
            domain,
            relay,
            &placement_branch_key,
            fields,
        )
        .await
    }

    pub(super) fn materialized_stream_key_is_visible(
        &self,
        placement: &RuntimeStatePlacement,
        key: &Option<BranchKey>,
    ) -> bool {
        let scheduled = if let Some(execution) = self.inner.executions.get(&placement.domain)
            && let Some(owner) = execution
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
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            &placement.identifier,
            None,
        );
        self.inner
            .expiring_stream_states
            .get(&expiring_placement)
            .is_none_or(|state| state.contains_key(key))
    }

    /// Every materialized record one relay holds on another node, reported the same way as the
    /// local relay-scoped view.
    pub async fn remote_materialized_stream_state(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<MaterializedRecordReport>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        let Some(schema) = self.materialized_relay_schema(&placement) else {
            return Ok(Vec::new());
        };
        let Some(restored) = self
            .fetch_sealed_materialized_snapshot(target_node_id, &placement, &schema, None)
            .await?
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
            .collect::<Result<Vec<_>, String>>()?;
        reports.sort_by(|left, right| left.branch.cmp(&right.branch));
        Ok(reports)
    }

    /// Every materialized record one relay holds, taken from the node that owns them.
    ///
    /// Records keep their columns and their typed concrete branch identity: nothing is turned into
    /// named scalar fields on the way, and nothing is rebuilt into a row on arrival.
    pub(in crate::runtime) async fn materialized_records_from_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<MaterializedGenerationRecord>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        let owner = if let Some(execution) = self.inner.executions.get(domain)
            && let Some(owner) = execution.materialized_stream_owner_nodes.get(relay)
        {
            owner.clone()
        } else {
            None
        };
        let local_node_id = self.inner.remote_dispatch.local_node_id.read().clone();
        if let Some(owner) = owner
            && local_node_id.as_ref() != Some(&owner)
        {
            let Some(schema) = self.materialized_relay_schema(&placement) else {
                return Ok(Vec::new());
            };
            let Some(restored) = self
                .fetch_sealed_materialized_snapshot(&owner, &placement, &schema, None)
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
            let Some(restored) = self.open_stored_materialized_snapshot(&placement).await? else {
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
                if self.materialized_stream_key_is_visible(&placement, &record.branch) {
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
    ) -> Result<Option<MaterializedGenerationRecord>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            None,
        );
        let Some(restored) = self
            .fetch_sealed_materialized_snapshot(target_node_id, &placement, schema, None)
            .await?
        else {
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
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        interest: &MaterializedProgramInterest,
        owner_nodes: &HashMap<RelayName, Option<ClusterNodeName>>,
    ) -> Result<HashMap<String, RuntimeValue>, String> {
        let mut values = HashMap::default();
        if interest.relays.is_empty() {
            return Ok(values);
        }

        for relay_interest in &interest.relays {
            tokio::task::consume_budget().await;
            let Some(relay_values) = self
                .load_materialized_relay_values(
                    domain,
                    branch_key,
                    MaterializedRelayRead {
                        relay: &relay_interest.relay,
                        key_mode: relay_interest.key_mode,
                        schema: &relay_interest.schema,
                        fields: &relay_interest.fields,
                    },
                    owner_nodes,
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

    pub(crate) async fn load_materialized_dependency_values(
        &self,
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        relay: &RelayName,
        owner_nodes: &HashMap<RelayName, Option<ClusterNodeName>>,
    ) -> Result<Option<HashMap<String, RuntimeValue>>, String> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(format!("domain '{}' is not instantiated", domain));
        };
        let Some(spec) = execution.materialized_stream_specs.get(relay).cloned() else {
            return Err(format!(
                "materialized relay '{}' is not instantiated in domain '{}'",
                relay, domain
            ));
        };
        drop(execution);

        let key_mode = if spec.branching.is_empty() {
            MaterializedLookupKeyMode::Root
        } else {
            MaterializedLookupKeyMode::CurrentBranch
        };
        let Some(field_values) = self
            .load_materialized_relay_values(
                domain,
                branch_key,
                MaterializedRelayRead {
                    relay,
                    key_mode,
                    schema: &spec.schema,
                    fields: &spec.fields,
                },
                owner_nodes,
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
        domain: &DomainName,
        branch_key: &Option<BranchKey>,
        dependencies: &[nervix_models::MaterializedStateDependency],
        execution_now: Timestamp,
    ) -> Result<MaterializedDependencyResolution, String> {
        if dependencies.is_empty() {
            return Ok(MaterializedDependencyResolution::Ready(HashMap::default()));
        }
        let owner_nodes = match self.inner.executions.get(domain) {
            Some(execution) => execution.materialized_stream_owner_nodes.clone(),
            None => HashMap::default(),
        };
        let mut resolved = HashMap::default();
        let udfs = self.udf_executor(domain);
        for dependency in dependencies {
            tokio::task::consume_budget().await;
            if let Some(values) = self
                .load_materialized_dependency_values(
                    domain,
                    branch_key,
                    &dependency.relay,
                    &owner_nodes,
                )
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
                    for assignment in assignments {
                        if matches!(
                            assignment.value,
                            nervix_models::Expression::Literal(ModelLiteral::Null)
                        ) {
                            continue;
                        }
                        let value = evaluate_constant_expression_vm(
                            &assignment.value,
                            udfs.as_ref(),
                            execution_now,
                        )
                        .await?;
                        resolved.insert(
                            format!(
                                "relay_state.{}.{}",
                                dependency.relay, assignment.target.field
                            ),
                            value,
                        );
                    }
                }
            }
        }
        Ok(MaterializedDependencyResolution::Ready(resolved))
    }

    pub(in crate::runtime) async fn resolve_materialized_dependencies_for_batch(
        &self,
        domain: &DomainName,
        input_relay: &RelayName,
        dependencies: &[nervix_models::MaterializedStateDependency],
        batch: RelayRecordBatch,
        wait: MaterializedBatchWaitContext<'_>,
    ) -> Result<Option<(RelayRecordBatch, HashMap<String, RuntimeValue>, Timestamp)>, String> {
        let MaterializedBatchWaitContext {
            shutdown_rx,
            wait_for_required_state,
            mut quiesce_work,
        } = wait;
        let domain_clock = self
            .bind_domain_clock(domain)
            .map_err(|error| error.to_string())?;
        let mut required_wait = None;
        loop {
            tokio::task::consume_budget().await;
            let execution_now = domain_clock
                .snapshot()
                .map_err(|error| error.to_string())?
                .now();
            let changed = self.inner.materialized_state_changed.notified();
            match self
                .resolve_materialized_dependencies(domain, &batch.key, dependencies, execution_now)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use ahash::HashMap;
    use arc_swap::ArcSwapOption;
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
                        Vec::new(),
                    ),
                )
            })
            .collect();
        let relay_registries = [(named("input"), RelayRegistry::new())]
            .into_iter()
            .collect();
        runtime.inner.executions.insert(
            domain.clone(),
            DomainExecution {
                schedule: DomainSchedule::new(domain.clone(), Vec::new(), Vec::new()),
                passive_only: false,
                start_version: 0,
                domain_clock: test_domain_clock(&domain),
                shutdown,
                graph: StdArc::new(ArcSwapOption::empty()),
                relay_registries,
                relay_schemas: HashMap::default(),
                relay_services: HashMap::default(),
                relay_branchings: HashMap::default(),
                relay_branching_schemas: HashMap::default(),
                materialized_stream_specs,
                materialized_stream_owner_nodes: HashMap::default(),
                branched_ingestors: HashMap::default(),
                branched_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                udfs: nervix_roto::UdfExecutor::default(),
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
            .resolve_materialized_dependencies(&domain, &None, &[default], execution_now)
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
        let resolution = runtime.resolve_materialized_dependencies_for_batch(
            &domain,
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
                    &domain,
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

/// Render one materialized record for the public relay-state report.
fn materialized_record_report(
    record: &MaterializedGenerationRecord,
) -> Result<MaterializedRecordReport, String> {
    Ok(MaterializedRecordReport {
        branch: branch_key_display(&record.branch).to_string(),
        payload: record.row.to_json_string()?,
        ingested_at_low_watermark: record.row.metadata().ingested_at_low_watermark(),
        ingested_at_high_watermark: record.row.metadata().ingested_at_high_watermark(),
    })
}
