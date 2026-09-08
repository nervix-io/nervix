use super::*;

pub(super) struct MaterializedRelayRead<'a> {
    pub(super) relay: &'a RelayName,
    pub(super) key_mode: MaterializedLookupKeyMode,
    pub(super) schema: &'a StdArc<arrow_schema::Schema>,
    pub(super) fields: &'a [MaterializedFieldInterest],
}

impl Runtime {
    pub fn local_materialized_stream_state(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<(String, nervix_models::RemoteRuntimeRecord)>, String> {
        let mut entries = Vec::new();
        for state in self.inner.replicated_materialized_stream_states.iter() {
            let placement = state.key();
            if placement.domain == *domain
                && placement.kind == ModelKind::Relay
                && placement.identifier == ModelName::from(relay)
            {
                entries.extend(
                    self.visible_materialized_stream_remote_entries(placement, state.value())?
                        .into_iter()
                        .map(|(key, record)| (branch_key_display(&key).to_string(), record)),
                );
            }
        }
        if !entries.is_empty() {
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            return Ok(entries);
        }
        self.local_materialized_stream_state_for_branch(domain, relay, &None)
    }

    pub(in crate::runtime) fn local_materialized_stream_state_for_branch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, nervix_models::RemoteRuntimeRecord)>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            branch_key.clone(),
        );
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(&placement)
        {
            let entries = self
                .visible_materialized_stream_remote_entries(&placement, &state)?
                .into_iter()
                .map(|(key, record)| (branch_key_display(&key).to_string(), record))
                .collect::<Vec<_>>();
            if !entries.is_empty() || branch_key.is_none() {
                return Ok(entries);
            }
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement.clone()
            };
            if let Some(state) = self
                .inner
                .replicated_materialized_stream_states
                .get(&aggregate_placement)
            {
                return Ok(self
                    .visible_materialized_stream_remote_entries(&aggregate_placement, &state)?
                    .into_iter()
                    .filter(|(key, _)| key == branch_key)
                    .map(|(key, record)| (branch_key_display(&key).to_string(), record))
                    .collect());
            }
        }
        if let Some(store) = &self.inner.state_store
            && let Some(snapshot) = store
                .latest_snapshot(&placement)
                .map_err(|error| error.to_string())?
        {
            return decode_materialized_stream_snapshot(&snapshot.payload)
                .map(|entries| {
                    let mut visible = entries
                        .into_iter()
                        .map(|(key, record)| (branch_key_display(&key).to_string(), record))
                        .collect::<Vec<_>>();
                    visible.sort_by(|left, right| left.0.cmp(&right.0));
                    visible
                })
                .map_err(|error| error.to_string());
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement
            };
            if let Some(store) = &self.inner.state_store
                && let Some(snapshot) = store
                    .latest_snapshot(&aggregate_placement)
                    .map_err(|error| error.to_string())?
            {
                return decode_materialized_stream_snapshot(&snapshot.payload)
                    .map(|entries| {
                        let mut visible = entries
                            .into_iter()
                            .filter(|(key, _)| key == branch_key)
                            .map(|(key, record)| (branch_key_display(&key).to_string(), record))
                            .collect::<Vec<_>>();
                        visible.sort_by(|left, right| left.0.cmp(&right.0));
                        visible
                    })
                    .map_err(|error| error.to_string());
            }
        }
        Ok(Vec::new())
    }

    pub(super) fn materialized_stream_values_from_snapshot(
        &self,
        payload: &[u8],
        branch_key: &Option<BranchKey>,
        schema: &StdArc<arrow_schema::Schema>,
        fields: &[MaterializedFieldInterest],
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let record = decode_materialized_stream_snapshot(payload)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find_map(|(key, record)| (key == *branch_key).then_some(record));
        let Some(record) = record else {
            return Ok(None);
        };
        let record = RuntimeRow::from_remote(schema.clone(), record)?;
        fields
            .iter()
            .map(|field| record.value_at(field.column_index))
            .collect::<Result<Vec<_>, _>>()
            .map(Some)
    }

    pub(super) fn local_materialized_stream_values_for_branch(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
        schema: &StdArc<arrow_schema::Schema>,
        fields: &[MaterializedFieldInterest],
    ) -> Result<Option<Vec<Option<RuntimeValue>>>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            branch_key.clone(),
        );
        if let Some(state) = self
            .inner
            .replicated_materialized_stream_states
            .get(&placement)
        {
            let values = if self.materialized_stream_key_is_visible(&placement, branch_key) {
                state.values_at(branch_key, fields.iter().map(|field| field.column_index))?
            } else {
                None
            };
            if values.is_some() || branch_key.is_none() {
                return Ok(values);
            }
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement.clone()
            };
            if let Some(state) = self
                .inner
                .replicated_materialized_stream_states
                .get(&aggregate_placement)
                && self.materialized_stream_key_is_visible(&aggregate_placement, branch_key)
                && let Some(values) =
                    state.values_at(branch_key, fields.iter().map(|field| field.column_index))?
            {
                return Ok(Some(values));
            }
        }
        if let Some(store) = &self.inner.state_store
            && let Some(snapshot) = store
                .latest_snapshot(&placement)
                .map_err(|error| error.to_string())?
        {
            return self.materialized_stream_values_from_snapshot(
                &snapshot.payload,
                branch_key,
                schema,
                fields,
            );
        }
        if branch_key.is_some() {
            let aggregate_placement = RuntimeStatePlacement {
                branch_key: None,
                ..placement
            };
            if let Some(store) = &self.inner.state_store
                && let Some(snapshot) = store
                    .latest_snapshot(&aggregate_placement)
                    .map_err(|error| error.to_string())?
            {
                return self.materialized_stream_values_from_snapshot(
                    &snapshot.payload,
                    branch_key,
                    schema,
                    fields,
                );
            }
        }
        Ok(None)
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
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            branch_key.clone(),
        );
        let Some(snapshot) = self
            .request_state_sync(target_node_id, &placement, 0)
            .await?
        else {
            return Ok(None);
        };
        self.materialized_stream_values_from_snapshot(&snapshot.payload, branch_key, schema, fields)
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
            schema,
            fields,
        )
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

    pub(in crate::runtime) fn visible_materialized_stream_remote_entries(
        &self,
        placement: &RuntimeStatePlacement,
        state: &ReplicatedMaterializedRelayState,
    ) -> Result<Vec<(Option<BranchKey>, nervix_models::RemoteRuntimeRecord)>, String> {
        let mut entries = state
            .entries
            .iter()
            .filter(|entry| self.materialized_stream_key_is_visible(placement, entry.key()))
            .map(|entry| {
                entry
                    .value()
                    .to_remote()
                    .map(|record| (entry.key().clone(), record))
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries
            .sort_by(|left, right| branch_key_display(&left.0).cmp(branch_key_display(&right.0)));
        Ok(entries)
    }

    pub(super) fn visible_materialized_stream_remote_entry(
        &self,
        placement: &RuntimeStatePlacement,
        state: &ReplicatedMaterializedRelayState,
        key: &Option<BranchKey>,
    ) -> Result<Option<(Option<BranchKey>, nervix_models::RemoteRuntimeRecord)>, String> {
        if !self.materialized_stream_key_is_visible(placement, key) {
            return Ok(None);
        }
        state
            .entries
            .get(key)
            .map(|record| record.to_remote().map(|record| (key.clone(), record)))
            .transpose()
    }

    pub async fn remote_materialized_stream_state(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<(String, nervix_models::RemoteRuntimeRecord)>, String> {
        self.remote_materialized_stream_state_for_branch(target_node_id, domain, relay, &None)
            .await
    }

    pub(super) async fn materialized_stream_state_from_owner(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<Vec<(String, nervix_models::RemoteRuntimeRecord)>, String> {
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
            return self
                .remote_materialized_stream_state(&owner, domain, relay)
                .await;
        }
        self.local_materialized_stream_state(domain, relay)
    }

    pub(in crate::runtime) async fn remote_materialized_stream_state_for_branch(
        &self,
        target_node_id: &ClusterNodeName,
        domain: &DomainName,
        relay: &RelayName,
        branch_key: &Option<BranchKey>,
    ) -> Result<Vec<(String, nervix_models::RemoteRuntimeRecord)>, String> {
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::MaterializedRelay,
            ModelKind::Relay,
            relay,
            branch_key.clone(),
        );
        let Some(snapshot) = self
            .request_state_sync(target_node_id, &placement, 0)
            .await?
        else {
            return Ok(Vec::new());
        };
        decode_materialized_stream_snapshot(&snapshot.payload)
            .map(|entries| {
                let mut visible = entries
                    .into_iter()
                    .map(|(key, record)| (branch_key_display(&key).to_string(), record))
                    .collect::<Vec<_>>();
                visible.sort_by(|left, right| left.0.cmp(&right.0));
                visible
            })
            .map_err(|error| error.to_string())
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
                        let value =
                            evaluate_constant_expression_vm(&assignment.value, udfs.as_ref())
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
    ) -> Result<Option<(RelayRecordBatch, HashMap<String, RuntimeValue>)>, String> {
        let MaterializedBatchWaitContext {
            shutdown_rx,
            wait_for_required_state,
            mut quiesce_work,
        } = wait;
        let mut required_wait = None;
        loop {
            tokio::task::consume_budget().await;
            let changed = self.inner.materialized_state_changed.notified();
            match self
                .resolve_materialized_dependencies(domain, &batch.key, dependencies)
                .await?
            {
                MaterializedDependencyResolution::Ready(values) => {
                    if let Some(work) = quiesce_work.as_deref_mut() {
                        work.resume_from_required_materialized_state();
                    }
                    drop(required_wait.take());
                    return Ok(Some((batch, values)));
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
            policy: nervix_models::MaterializedStatePolicy::Default(vec![Assignment {
                target: AssignmentTarget::bare(named("status")),
                value: Expression::Literal(nervix_models::Literal::String("unknown".to_string())),
            }]),
        };
        let resolved = runtime
            .resolve_materialized_dependencies(&domain, &None, &[default])
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
                .resolve_materialized_dependencies(&domain, &None, &[wait.clone(), skip.clone()])
                .await
                .expect("missing dependencies should produce a policy outcome"),
            MaterializedDependencyResolution::Wait
        ));
        assert!(matches!(
            runtime
                .resolve_materialized_dependencies(&domain, &None, &[skip, wait])
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
