use super::*;

pub(super) struct GeneratorTaskSpec {
    pub(super) generator: CreateGenerator,
    pub(super) source_relay: RelayName,
    pub(super) source_schema: Arc<CompiledSchema>,
    pub(super) source_branching: Vec<FieldName>,
    pub(super) context_projection: GeneratorContextProjection,
    pub(super) routes: Vec<GeneratorTaskRouteSpec>,
}

impl GeneratorTaskSpec {
    pub(super) fn new(
        generator: CreateGenerator,
        source_schema: Arc<CompiledSchema>,
        source_branching: Vec<FieldName>,
        source_branch_schema: Option<StdArc<arrow_schema::Schema>>,
        routes: Vec<GeneratorTaskRouteSpec>,
    ) -> Self {
        let source_relay = generator.materialized_relay.clone();
        let context_projection = GeneratorContextProjection::new(
            &source_relay,
            source_schema.arrow_schema().as_ref(),
            source_branch_schema.as_deref(),
        );
        Self {
            generator,
            source_relay,
            source_schema,
            source_branching,
            context_projection,
            routes,
        }
    }
}

pub(super) struct GeneratorContextProjection {
    pub(super) source_namespace: String,
    pub(super) schema: StdArc<arrow_schema::Schema>,
}

impl GeneratorContextProjection {
    pub(super) fn new(
        source_relay: &RelayName,
        source_schema: &arrow_schema::Schema,
        branch_schema: Option<&arrow_schema::Schema>,
    ) -> Self {
        let source_namespace = format!("relay_state.{}", source_relay.as_str());
        let mut fields = source_schema
            .fields()
            .iter()
            .map(|field| {
                StdArc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(format!("{source_namespace}.{}", field.name())),
                )
            })
            .collect::<Vec<_>>();
        if let Some(branch_schema) = branch_schema {
            fields.extend(branch_schema.fields().iter().map(|field| {
                StdArc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_name(format!("branch.{}", field.name())),
                )
            }));
        }
        Self {
            source_namespace,
            schema: StdArc::new(arrow_schema::Schema::new(fields)),
        }
    }

    pub(super) fn project(
        &self,
        source: &RuntimeRecordBatch,
        branch_key: &Option<BranchKey>,
    ) -> Result<RuntimeRecordBatch, String> {
        let namespace_batches = [(self.source_namespace.as_str(), source)];
        let strict_namespaces = [self.source_namespace.as_str()];
        let side_inputs = HashMap::default();
        let lookup_columns = HashMap::default();
        let input = project_vm_input_batch(
            &self.schema,
            &VmInputProjectionSources {
                carrier: source,
                namespace_batches: &namespace_batches,
                strict_namespaces: &strict_namespaces,
                keys: std::slice::from_ref(branch_key),
                side_inputs: &side_inputs,
                ingest_metadata: None,
                lookup_columns: &lookup_columns,
                uninitialized: None,
            },
            None,
        )?;
        vm_typed_batch_to_runtime_batch(&input)
    }

    pub(super) fn materialized_state_snapshot(
        &self,
        source: &RuntimeRecordBatch,
    ) -> Result<HashMap<String, RuntimeValue>, String> {
        let schema = source.schema();
        let mut snapshot = HashMap::with_capacity(schema.fields().len());
        for field in schema.fields() {
            if let Some(value) = source.value(0, field.name())? {
                snapshot.insert(format!("{}.{}", self.source_namespace, field.name()), value);
            }
        }
        Ok(snapshot)
    }
}

pub(super) struct GeneratorTaskRouteSpec {
    pub(super) output: ProcessorOutput,
    pub(super) program: CompiledProgramWithMaterializedInterest,
    pub(super) input_projection: GeneratorRouteInputProjection,
    pub(super) output_schema: Arc<CompiledSchema>,
    pub(super) output_registry: RelayRegistry,
    pub(super) output_services: Arc<RelayBoundaryServices>,
}

impl GeneratorTaskRouteSpec {
    pub(super) fn new(
        output: ProcessorOutput,
        program: CompiledProgramWithMaterializedInterest,
        output_schema: Arc<CompiledSchema>,
        output_registry: RelayRegistry,
        output_services: Arc<RelayBoundaryServices>,
    ) -> Self {
        let input_projection = GeneratorRouteInputProjection::new(&program.compiled.input_schema);
        Self {
            output,
            program,
            input_projection,
            output_schema,
            output_registry,
            output_services,
        }
    }

    pub(super) fn project_input(
        &self,
        context: &RuntimeRecordBatch,
        branch_key: &Option<BranchKey>,
    ) -> Result<VmTypedBatch, String> {
        self.input_projection
            .project(&self.program.compiled.input_schema, context, branch_key)
    }
}

pub(super) struct GeneratorRouteInputProjection {
    pub(super) uninitialized: VmUninitializedInput,
}

impl GeneratorRouteInputProjection {
    pub(super) fn new(schema: &arrow_schema::Schema) -> Self {
        Self {
            uninitialized: VmUninitializedInput {
                fields: schema
                    .fields()
                    .iter()
                    .filter(|field| field.name().starts_with("output."))
                    .map(|field| field.name().clone())
                    .collect(),
            },
        }
    }

    pub(super) fn project(
        &self,
        schema: &StdArc<arrow_schema::Schema>,
        context: &RuntimeRecordBatch,
        branch_key: &Option<BranchKey>,
    ) -> Result<VmTypedBatch, String> {
        let side_inputs = HashMap::default();
        let lookup_columns = HashMap::default();
        project_vm_input_batch(
            schema,
            &VmInputProjectionSources {
                carrier: context,
                namespace_batches: &[],
                strict_namespaces: &[],
                keys: std::slice::from_ref(branch_key),
                side_inputs: &side_inputs,
                ingest_metadata: None,
                lookup_columns: &lookup_columns,
                uninitialized: Some(&self.uninitialized),
            },
            None,
        )
    }
}

#[derive(Default)]
pub(super) struct GeneratorBranchTaskState {
    pub(super) next_generation: Option<Timestamp>,
    pub(super) routes: Vec<GeneratorRouteBranchTaskState>,
}

#[derive(Default)]
pub(super) struct GeneratorRouteBranchTaskState {
    pub(super) next_flush: Option<Timestamp>,
    pub(super) pending: Vec<RelayMessage>,
}

pub(super) enum GeneratorProgramOutcome {
    Filtered,
    Output(RuntimeRow),
    MessageError {
        error: StructuredMessageError,
        partial_output: Option<RuntimeRecordBatch>,
    },
}

pub(super) async fn execute_generator_program_on_context(
    program: &CompiledProgramWithMaterializedInterest,
    input: &VmTypedBatch,
    execution_now: Timestamp,
) -> Result<GeneratorProgramOutcome, String> {
    let result = execute_program_with_selection_in_context(
        &program.compiled,
        input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| format!("GENERATOR execution failed: {error}"))?;
    if result.batch.row_count() == 0 {
        return Ok(GeneratorProgramOutcome::Filtered);
    }
    if result.batch.row_count() != 1 {
        return Err(format!(
            "GENERATOR produced {} rows for a single input key",
            result.batch.row_count()
        ));
    }
    if let Some(side_error) = result.batch.errors().first() {
        return Ok(GeneratorProgramOutcome::MessageError {
            error: program.structured_side_error(
                execution_now,
                format!(
                    "GENERATOR side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                ),
                side_error.span,
                MessageErrorOperation::Set,
            ),
            partial_output: captured_partial_output(&result.batch, 0),
        });
    }
    let batch = vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &[0])?;
    RuntimeRow::new(
        Arc::new(batch),
        0,
        RuntimeRecordMetadata::from_ingested_at_watermarks(execution_now, execution_now),
    )
    .map(GeneratorProgramOutcome::Output)
}

pub(super) struct GeneratorFlushContext<'a> {
    pub(super) runtime: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) generator: &'a GeneratorName,
    pub(super) output_relay: &'a RelayName,
    pub(super) output_schema: &'a Arc<CompiledSchema>,
    pub(super) output_registry: &'a RelayRegistry,
    pub(super) output_services: &'a Arc<RelayBoundaryServices>,
    pub(super) task_events: &'a RuntimeEvents,
}

pub(super) async fn flush_generator_groups(
    context: GeneratorFlushContext<'_>,
    pending_groups: &mut Vec<(Option<BranchKey>, Vec<RelayMessage>)>,
) {
    let GeneratorFlushContext {
        runtime,
        domain,
        generator,
        output_relay,
        output_schema,
        output_registry,
        output_services,
        task_events,
    } = context;
    for (_key, messages) in std::mem::take(pending_groups) {
        let batch = match RelayRecordBatch::from_messages(output_schema.clone(), messages) {
            Ok(batch) => batch,
            Err(error) => {
                task_events.report_error(format!(
                    "failed to build generator batch for '{}' in domain '{}': {}",
                    generator.as_str(),
                    domain.as_str(),
                    error
                ));
                continue;
            }
        };
        if let Err(error) = runtime
            .ingest_stream_boundary_message(
                domain,
                output_relay,
                output_registry,
                output_services,
                &batch,
            )
            .await
        {
            task_events.report_error(format!(
                "failed to flush generator '{}' into relay '{}' in domain '{}'",
                generator.as_str(),
                output_relay.as_str(),
                domain.as_str(),
            ));
            drop(error);
        }
    }
}

impl Runtime {
    pub(super) fn generator_activity_tracker(&self, domain: &DomainName) -> Arc<AtomicUsize> {
        self.inner
            .generator_activity_by_domain
            .entry(domain.clone())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    pub(in crate::runtime) fn spawn_generator_task(
        &self,
        domain: &DomainName,
        shutdown_tx: &watch::Sender<bool>,
        spec: GeneratorTaskSpec,
    ) -> Result<JoinHandle<()>, RuntimeError> {
        let GeneratorTaskSpec {
            generator,
            source_relay,
            source_schema,
            source_branching,
            context_projection,
            routes,
        } = spec;
        let interval = Self::parse_runtime_node_duration_setting(
            domain,
            "generator",
            &generator.name,
            "each",
            &generator.each,
        )?;
        let routes = routes
            .into_iter()
            .map(|route| {
                let policy = route.output.flush_policy.as_ref().ok_or_else(|| {
                    RuntimeError::BuildDomainExecution {
                        domain: domain.as_str().to_string(),
                        reason: format!(
                            "generator '{}' output '{}' has no flush policy",
                            generator.name, route.output.relay
                        ),
                    }
                })?;
                let flush_policy = Self::parse_runtime_node_flush_policy(
                    domain,
                    "generator",
                    &generator.name,
                    policy,
                )?;
                Ok((route, flush_policy))
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        let task_domain = domain.clone();
        let task_generator = generator.name.clone();
        let source_gate = self
            .inner
            .relay_boundary_fanouts
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Relay,
                source_relay.clone(),
            ))
            .map(|fanout| fanout.dispatch_gate());
        let Some(source_gate) = source_gate else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "missing generator source relay gate '{}'",
                    source_relay.as_str()
                ),
            });
        };
        let quiesce_counters =
            self.node_quiesce_counters(domain, NodeRef::new(ModelKind::Generator, &generator.name));
        let mut shutdown_rx = shutdown_tx.subscribe();
        let mut domain_status_rx = self.inner.domain_status_changed.subscribe();
        let generator_activity = self.generator_activity_tracker(domain);
        let domain_clock =
            self.bind_domain_clock(domain)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "generator '{}' could not bind its domain clock: {error}",
                        generator.name.as_str(),
                    ),
                })?;
        let runtime = self.clone();
        let task_events = self.inner.events.clone();

        Ok(tokio::spawn(async move {
            let mut activity = DomainActivityGuard::new(generator_activity);
            let mut quiesce_activity = Some(NodeQuiesceWorkGuard::begin(quiesce_counters.clone()));
            let mut next_state_refresh = None::<Timestamp>;
            let mut branch_states =
                HashMap::<Option<BranchKey>, GeneratorBranchTaskState>::default();

            loop {
                tokio::task::consume_budget().await;
                if source_gate.is_closed() {
                    for (route_index, (route, _)) in routes.iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let mut pending_groups = Vec::new();
                        for (branch_key, state) in &mut branch_states {
                            let route_state = &mut state.routes[route_index];
                            route_state.next_flush = None;
                            if !route_state.pending.is_empty() {
                                pending_groups.push((
                                    branch_key.clone(),
                                    std::mem::take(&mut route_state.pending),
                                ));
                            }
                        }
                        if !pending_groups.is_empty() {
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_groups,
                            )
                            .await;
                        }
                    }
                    quiesce_activity.take();
                    activity.set_active(false);
                    tokio::select! {
                        _ = source_gate.wait_open() => {}
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                    if quiesce_activity.is_none() {
                        quiesce_activity =
                            Some(NodeQuiesceWorkGuard::begin(quiesce_counters.clone()));
                    }
                    continue;
                }
                if runtime
                    .inner
                    .domains
                    .get(&task_domain)
                    .is_some_and(|state| {
                        matches!(state.status, nervix_models::DomainStatus::Paused)
                    })
                {
                    for (route_index, (route, _)) in routes.iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let mut pending_groups = Vec::new();
                        for (branch_key, state) in &mut branch_states {
                            let route_state = &mut state.routes[route_index];
                            route_state.next_flush = None;
                            if !route_state.pending.is_empty() {
                                pending_groups.push((
                                    branch_key.clone(),
                                    std::mem::take(&mut route_state.pending),
                                ));
                            }
                        }
                        if !pending_groups.is_empty() {
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_groups,
                            )
                            .await;
                        }
                    }
                    activity.set_active(false);
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        changed = domain_status_rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                        }
                        _ = source_gate.wait_closed() => {}
                    }
                    continue;
                }
                activity.set_active(true);
                let execution_now = match domain_clock.snapshot() {
                    Ok(snapshot) => snapshot.now(),
                    Err(error) => {
                        task_events.report_error(format!(
                            "generator '{}' in domain '{}' lost its clock: {error}",
                            task_generator.as_str(),
                            task_domain.as_str(),
                        ));
                        break;
                    }
                };

                if next_state_refresh.is_none() {
                    next_state_refresh = Some(execution_now);
                }
                let should_refresh_state =
                    next_state_refresh.is_some_and(|next| execution_now >= next);
                let mut did_scheduled_work = false;

                if should_refresh_state {
                    advance_scheduled_timestamp(&mut next_state_refresh, interval, execution_now);
                    did_scheduled_work = true;

                    let mut state_load_failed = false;
                    let state = match runtime
                        .materialized_stream_state_from_owner(&task_domain, &source_relay)
                        .await
                    {
                        Ok(state) => state,
                        Err(error) => {
                            state_load_failed = true;
                            task_events.report_error(format!(
                                "failed to read materialized state for generator '{}' from relay \
                                 '{}' in domain '{}': {}",
                                task_generator.as_str(),
                                source_relay.as_str(),
                                task_domain.as_str(),
                                error
                            ));
                            Vec::new()
                        }
                    };

                    let mut source_state_by_branch = HashMap::<
                        Option<BranchKey>,
                        Vec<nervix_models::RemoteRuntimeRecord>,
                    >::default();
                    if !state_load_failed {
                        let mut latest_state =
                            HashMap::<String, nervix_models::RemoteRuntimeRecord>::default();
                        for (key, record) in state {
                            let replace = latest_state.get(&key).is_none_or(|existing| {
                                let existing = &existing.metadata;
                                let candidate = &record.metadata;
                                candidate.ingested_at_high_watermark
                                    > existing.ingested_at_high_watermark
                                    || (candidate.ingested_at_high_watermark
                                        == existing.ingested_at_high_watermark
                                        && candidate.ingested_at_low_watermark
                                            > existing.ingested_at_low_watermark)
                            });
                            if replace {
                                latest_state.insert(key, record);
                            }
                        }
                        for record in latest_state.into_values() {
                            let branch_key = if source_branching.is_empty() {
                                None
                            } else {
                                match BranchKey::from_remote_record(
                                    &record,
                                    source_branching.iter(),
                                ) {
                                    Ok(Some(key)) => Some(key),
                                    Ok(None) => {
                                        task_events.report_error(format!(
                                            "generator '{}' source relay '{}' record is missing \
                                             concrete branch fields",
                                            task_generator.as_str(),
                                            source_relay.as_str(),
                                        ));
                                        continue;
                                    }
                                    Err(error) => {
                                        task_events.report_error(format!(
                                            "generator '{}' source relay '{}' has invalid \
                                             concrete branch fields: {}",
                                            task_generator.as_str(),
                                            source_relay.as_str(),
                                            error,
                                        ));
                                        continue;
                                    }
                                }
                            };
                            source_state_by_branch
                                .entry(branch_key)
                                .or_default()
                                .push(record);
                        }
                    }

                    if !state_load_failed {
                        let active_branch_keys = source_state_by_branch
                            .keys()
                            .cloned()
                            .collect::<HashSet<_>>();
                        branch_states
                            .retain(|branch_key, _| active_branch_keys.contains(branch_key));
                        for (branch_key, records) in source_state_by_branch {
                            tokio::task::consume_budget().await;
                            let branch_state = branch_states
                                .entry(branch_key.clone())
                                .or_insert_with(|| GeneratorBranchTaskState {
                                    next_generation: None,
                                    routes: routes
                                        .iter()
                                        .map(|_| GeneratorRouteBranchTaskState::default())
                                        .collect(),
                                });
                            if branch_state.next_generation.is_none() {
                                branch_state.next_generation = Some(execution_now);
                            }
                            for (route_state, (_, flush_policy)) in
                                branch_state.routes.iter_mut().zip(&routes)
                            {
                                if route_state.next_flush.is_none()
                                    && let RuntimeFlushPolicy::Each {
                                        interval: flush_each,
                                        ..
                                    } = flush_policy
                                {
                                    route_state.next_flush =
                                        Some(checked_add_duration_to_timestamp(
                                            execution_now,
                                            *flush_each,
                                        ));
                                }
                            }
                            if !branch_state
                                .next_generation
                                .is_some_and(|next| execution_now >= next)
                            {
                                continue;
                            }
                            advance_scheduled_timestamp(
                                &mut branch_state.next_generation,
                                interval,
                                execution_now,
                            );

                            for source_record in records {
                                tokio::task::consume_budget().await;
                                let (source_batch, source_metadata) =
                                    match source_schema.runtime_batch_from_remote(source_record) {
                                        Ok(decoded) => decoded,
                                        Err(error) => {
                                            task_events.report_error(format!(
                                                "failed to decode generator '{}' source relay \
                                                 '{}' state in domain '{}': {}",
                                                task_generator.as_str(),
                                                source_relay.as_str(),
                                                task_domain.as_str(),
                                                error
                                            ));
                                            continue;
                                        }
                                    };
                                let source_batch = Arc::new(source_batch);
                                let context = match context_projection
                                    .project(source_batch.as_ref(), &branch_key)
                                {
                                    Ok(context) => context,
                                    Err(error) => {
                                        task_events.report_error(format!(
                                            "failed to prepare generator '{}' context in domain \
                                             '{}' branch '{}': {}",
                                            task_generator.as_str(),
                                            task_domain.as_str(),
                                            branch_key_display(&branch_key),
                                            error
                                        ));
                                        continue;
                                    }
                                };
                                let mut materialized_state_snapshot = None;

                                for (route_index, (route, flush_policy)) in
                                    routes.iter().enumerate()
                                {
                                    tokio::task::consume_budget().await;
                                    let input = match route.project_input(&context, &branch_key) {
                                        Ok(input) => input,
                                        Err(error) => {
                                            task_events.report_error(format!(
                                                "failed to prepare generator '{}' route '{}' \
                                                 input in domain '{}' branch '{}': {}",
                                                task_generator.as_str(),
                                                route.output.relay.as_str(),
                                                task_domain.as_str(),
                                                branch_key_display(&branch_key),
                                                error
                                            ));
                                            continue;
                                        }
                                    };
                                    match execute_generator_program_on_context(
                                        &route.program,
                                        &input,
                                        execution_now,
                                    )
                                    .await
                                    {
                                        Ok(GeneratorProgramOutcome::Filtered) => {}
                                        Ok(GeneratorProgramOutcome::Output(record)) => {
                                            let (acks, _completion) =
                                                runtime.tracked_ack_root(&task_domain);
                                            let route_state = &mut branch_state.routes[route_index];
                                            route_state.pending.push(RelayMessage {
                                                key: branch_key.clone(),
                                                record,
                                                acks,
                                            });
                                            if route_state.next_flush.is_none() {
                                                route_state.next_flush =
                                                    Some(checked_add_duration_to_timestamp(
                                                        execution_now,
                                                        flush_policy.interval(),
                                                    ));
                                            }
                                        }
                                        Ok(GeneratorProgramOutcome::MessageError {
                                            error,
                                            partial_output,
                                        }) => {
                                            if materialized_state_snapshot.is_none() {
                                                materialized_state_snapshot =
                                                    match context_projection
                                                        .materialized_state_snapshot(
                                                            source_batch.as_ref(),
                                                        ) {
                                                        Ok(snapshot) => Some(snapshot),
                                                        Err(error) => {
                                                            task_events.report_error(format!(
                                                                "failed to capture generator '{}' \
                                                                 materialized state in domain \
                                                                 '{}' branch '{}': {}",
                                                                task_generator.as_str(),
                                                                task_domain.as_str(),
                                                                branch_key_display(&branch_key),
                                                                error
                                                            ));
                                                            continue;
                                                        }
                                                    };
                                            }
                                            let materialized_state = materialized_state_snapshot
                                                .as_ref()
                                                .verified(
                                                    "the branch above takes the snapshot and \
                                                     continues when it cannot",
                                                )
                                                .clone();
                                            let (acks, _completion) =
                                                runtime.tracked_ack_root(&task_domain);
                                            runtime
                                                .handle_structured_message_error(
                                                    MessageErrorHandling {
                                                        domain: &task_domain,
                                                        node_kind: ModelKind::Generator,
                                                        node: &ModelName::from(&task_generator),
                                                        source_route: Some(&route.output.relay),
                                                        policy: &route.output.message_error_policy,
                                                        message: RelayMessage {
                                                            key: branch_key.clone(),
                                                            record: RuntimeRow::new(
                                                                source_batch.clone(),
                                                                0,
                                                                source_metadata.clone(),
                                                            )
                                                            .verified(
                                                                "the generator decodes one source \
                                                                 row per tick before reaching here",
                                                            ),
                                                            acks,
                                                        },
                                                        error,
                                                        partial_output,
                                                        materialized_state,
                                                        ingest_metadata: None,
                                                        execution_now,
                                                    },
                                                )
                                                .await;
                                        }
                                        Err(error) => {
                                            task_events.report_error(format!(
                                                "failed to execute generator '{}' route '{}' in \
                                                 domain '{}' branch '{}': {}",
                                                task_generator.as_str(),
                                                route.output.relay.as_str(),
                                                task_domain.as_str(),
                                                branch_key_display(&branch_key),
                                                error
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let mut flushed_any_branch = false;
                for (branch_key, branch_state) in &mut branch_states {
                    tokio::task::consume_budget().await;
                    for ((route, flush_policy), route_state) in
                        routes.iter().zip(&mut branch_state.routes)
                    {
                        if !route_state
                            .next_flush
                            .is_some_and(|next| execution_now >= next)
                        {
                            continue;
                        }
                        match flush_policy {
                            RuntimeFlushPolicy::Each { interval, .. } => {
                                advance_scheduled_timestamp(
                                    &mut route_state.next_flush,
                                    *interval,
                                    execution_now,
                                );
                            }
                            RuntimeFlushPolicy::Immediate => {
                                route_state.next_flush = None;
                            }
                        }
                        if !route_state.pending.is_empty() {
                            let mut pending_group = vec![(
                                branch_key.clone(),
                                std::mem::take(&mut route_state.pending),
                            )];
                            flush_generator_groups(
                                GeneratorFlushContext {
                                    runtime: &runtime,
                                    domain: &task_domain,
                                    generator: &task_generator,
                                    output_relay: &route.output.relay,
                                    output_schema: &route.output_schema,
                                    output_registry: &route.output_registry,
                                    output_services: &route.output_services,
                                    task_events: &task_events,
                                },
                                &mut pending_group,
                            )
                            .await;
                        }
                        flushed_any_branch = true;
                    }
                }
                did_scheduled_work |= flushed_any_branch;

                if did_scheduled_work {
                    continue;
                }

                let next_deadline =
                    next_state_refresh
                        .into_iter()
                        .chain(
                            branch_states
                                .values()
                                .filter_map(|state| state.next_generation),
                        )
                        .chain(branch_states.values().flat_map(|state| {
                            state.routes.iter().filter_map(|route| route.next_flush)
                        }))
                        .min();
                let sleep_duration = if let Some(next) = next_deadline {
                    match domain_clock.physical_duration_until(execution_now, next) {
                        Ok(duration) => duration,
                        Err(error) => {
                            task_events.report_error(format!(
                                "generator '{}' in domain '{}' could not schedule its logical \
                                 deadline: {error}",
                                task_generator.as_str(),
                                task_domain.as_str(),
                            ));
                            break;
                        }
                    }
                } else {
                    interval
                };

                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = sleep(sleep_duration) => {}
                    _ = source_gate.wait_closed() => {}
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use nervix_models::{
        CreateGenerator, MessageErrorPolicy, ParseAsType, ProcessorOutput, ProcessorOutputs,
        Timestamp,
    };
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::runtime_schema::RuntimeValue;
    #[tokio::test]
    async fn generator_set_program_projects_columnar_state_and_branch_context() {
        let source_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("amount", ParseAsType::I64),
            (
                "samples",
                ParseAsType::Array {
                    element: Box::new(ParseAsType::F32),
                    len: nonzero!(2u32),
                },
            ),
            (
                "labels",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::String),
                },
            ),
        ]);
        let output_schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("amount", ParseAsType::I64),
            (
                "samples",
                ParseAsType::Array {
                    element: Box::new(ParseAsType::F32),
                    len: nonzero!(2u32),
                },
            ),
            (
                "labels",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::String),
                },
            ),
        ]);
        let branch_schema = test_schema(&[("tenant", ParseAsType::String)]);
        let output = ProcessorOutput {
            relay: named("generated_notifications"),
            construction: construction(
                "SET tenant = branch.tenant, amount = relay_state.notifications.amount + 1, \
                 samples = relay_state.notifications.samples, labels = \
                 relay_state.notifications.labels",
            ),
            flush_policy: Some(FlushPolicy::Each {
                interval: "100ms".to_string(),
                max_batch_size: "1MiB".to_string(),
            }),
            message_error_policy: MessageErrorPolicy::Log,
            branch: None,
        };
        let generator = CreateGenerator {
            name: named("synth_notifications"),
            materialized_relay: named("notifications"),
            branched_by: processor_branched_by("generated_notifications", &["tenant"]),
            each: "100ms".to_string(),
            output_routes: ProcessorOutputs::new(vec![output.clone()]),
        };

        let program = compile_generator_set_program(
            &domain("default"),
            &generator,
            &output,
            GeneratorSetProgramSchemas {
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
                source: source_schema.arrow_schema(),
                branch: Some(branch_schema.arrow_schema()),
            },
            None,
        )
        .expect("generator set program must compile");

        let samples = RuntimeValue::Array(vec![
            RuntimeValue::F32(OrderedFloat(1.0)),
            RuntimeValue::F32(OrderedFloat(2.5)),
        ]);
        let labels = RuntimeValue::Vec(vec![
            RuntimeValue::String("api".to_string()),
            RuntimeValue::String("prod".to_string()),
        ]);
        let source = source_schema
            .batch_from_test_rows([[
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("amount".to_string(), RuntimeValue::I64(7)),
                ("samples".to_string(), samples.clone()),
                ("labels".to_string(), labels.clone()),
            ]])
            .expect("generator source batch must build");
        let branch_key = string_branch_key("tenant", "acme");
        let context_projection = GeneratorContextProjection::new(
            &named("notifications"),
            source_schema.arrow_schema().as_ref(),
            Some(branch_schema.arrow_schema().as_ref()),
        );
        let context = context_projection
            .project(&source, &branch_key)
            .expect("generator context must project");
        let source_samples = source
            .schema()
            .index_of("samples")
            .expect("source samples column must exist");
        let context_samples = context
            .schema()
            .index_of("relay_state.notifications.samples")
            .expect("context samples column must exist");
        assert!(StdArc::ptr_eq(
            source.batch().column(source_samples),
            context.batch().column(context_samples),
        ));
        let input = GeneratorRouteInputProjection::new(&program.compiled.input_schema)
            .project(&program.compiled.input_schema, &context, &branch_key)
            .expect("generator route input must project");
        let input_samples = input
            .schema()
            .index_of("relay_state.notifications.samples")
            .expect("route input samples column must exist");
        let input_samples = input.column(input_samples).to_array_ref();
        assert!(StdArc::ptr_eq(
            context.batch().column(context_samples),
            &input_samples,
        ));

        let output =
            execute_generator_program_on_context(&program, &input, Timestamp::from_unix_nanos(1))
                .await
                .expect("generator program must execute");
        let GeneratorProgramOutcome::Output(output) = output else {
            panic!("generator program must emit one row");
        };

        assert_eq!(
            row_value(&output, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert_eq!(row_value(&output, "amount"), Some(RuntimeValue::I64(8)));
        assert_eq!(row_value(&output, "samples"), Some(samples));
        assert_eq!(row_value(&output, "labels"), Some(labels));
    }
}
