use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaterializedLookupKeyMode {
    CurrentBranch,
    Root,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedFieldInterest {
    pub(super) name: String,
    pub(super) column_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedRelayInterest {
    pub(super) relay: RelayName,
    pub(super) schema: StdArc<arrow_schema::Schema>,
    pub(super) fields: Vec<MaterializedFieldInterest>,
    pub(super) key_mode: MaterializedLookupKeyMode,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MaterializedProgramInterest {
    pub(super) relays: Vec<MaterializedRelayInterest>,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeMaterializedRelaySpec {
    pub(crate) schema: StdArc<arrow_schema::Schema>,
    pub(crate) sensitivity: VmSchemaSensitivity,
    pub(crate) branching: Vec<FieldName>,
    pub(super) fields: Arc<Vec<MaterializedFieldInterest>>,
}

impl RuntimeMaterializedRelaySpec {
    pub(crate) fn new(
        schema: StdArc<arrow_schema::Schema>,
        sensitivity: VmSchemaSensitivity,
        branching: Vec<FieldName>,
    ) -> Self {
        let fields = Arc::new(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(column_index, field)| MaterializedFieldInterest {
                    name: field.name().clone(),
                    column_index,
                })
                .collect(),
        );
        Self {
            schema,
            sensitivity,
            branching,
            fields,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledProgramWithMaterializedInterest {
    pub(crate) compiled: Arc<VmCompiledProgram>,
    pub(crate) output_sensitivity: VmSchemaSensitivity,
    pub(crate) materialized_interest: MaterializedProgramInterest,
    pub(super) output_namespace_input: OutputNamespaceInput,
    pub(super) lookup_hash_maps: Vec<LookupHashMapCall>,
    pub(super) error_sites: CompiledMessageErrorSites,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledBranchProgram {
    pub(super) program: CompiledProgramWithMaterializedInterest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutputNamespaceInput {
    Uninitialized,
    Finalized,
}

/// Message-error metadata for one lowered operation, keyed by the span the VM reports when that
/// operation fails.
///
/// Spans are two `usize`s, so ordered comparison is cheaper than hashing them.
pub(super) type CompiledMessageErrorSites = BTreeMap<VmSpan, CompiledMessageErrorSite>;

#[derive(Debug, Clone)]
pub(super) struct CompiledMessageErrorSite {
    pub(super) operation: MessageErrorOperation,
    pub(super) operation_index: Option<u32>,
    pub(super) fields: SortedSet<FieldPath>,
}

impl CompiledProgramWithMaterializedInterest {
    pub(super) fn captures_partial_output(&self) -> bool {
        self.error_sites.values().any(|site| {
            matches!(
                site.operation,
                MessageErrorOperation::Inherit | MessageErrorOperation::Set
            )
        })
    }

    pub(super) fn structured_side_error(
        &self,
        execution_now: Timestamp,
        reason: String,
        span: VmSpan,
        fallback_operation: MessageErrorOperation,
    ) -> StructuredMessageError {
        let site = self.error_sites.get(&span);
        let operation = match site {
            Some(site) => site.operation,
            None => fallback_operation,
        };
        structured_message_error(
            execution_now,
            MessageErrorCode::Evaluation,
            reason,
            operation,
            site.and_then(|site| site.operation_index),
            site.map(|site| site.fields.iter().cloned())
                .into_iter()
                .flatten(),
        )
    }
}

pub(crate) type EmitterHeaders = Vec<(String, String)>;

#[derive(Debug, Clone)]
pub(crate) struct CompiledEmitterFilterMapProgram {
    pub(crate) body: CompiledProgramWithMaterializedInterest,
    pub(crate) codec_route: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeVmCompileContext<'a> {
    pub(crate) available_materialized_streams: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(crate) available_lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
    pub(crate) current_branching: &'a [FieldName],
    pub(crate) current_branch_schema: Option<&'a StdArc<arrow_schema::Schema>>,
    pub(crate) current_branch_sensitivity: Option<&'a VmSchemaSensitivity>,
    pub(crate) udfs: Option<&'a UdfExecutor>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RuntimeCompileTarget<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) identifier: &'a ModelName,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeVmSchema {
    pub(super) schema: StdArc<arrow_schema::Schema>,
    pub(super) sensitivity: VmSchemaSensitivity,
}

impl RuntimeVmCompileContext<'_> {
    pub(super) fn branch_binding(&self) -> Option<VmCompileBinding> {
        self.current_branch_schema.map(|schema| {
            let sensitivity = self.current_branch_sensitivity.cloned().unwrap_or_default();
            VmCompileBinding::readonly(BRANCH_NAMESPACE, schema.clone())
                .with_sensitivity(sensitivity)
        })
    }

    pub(super) fn compile_options(&self, options: VmCompileOptions) -> VmCompileOptions {
        runtime_udf_compile_options(self.udfs, options)
    }
}

/// The signatures a compile resolves calls against: the domain's when it has a UDF executor, and
/// none when it has not.
pub(super) fn runtime_udf_signatures(udfs: Option<&UdfExecutor>) -> VmUdfSignatures {
    match udfs {
        Some(udfs) => udfs.signatures().clone(),
        None => VmUdfSignatures::default(),
    }
}

pub(super) fn runtime_udf_compile_options(
    udfs: Option<&UdfExecutor>,
    mut options: VmCompileOptions,
) -> VmCompileOptions {
    if let Some(udfs) = udfs {
        options.udf_signatures = udfs.signatures().clone();
        options.injector = Some(Arc::new(Box::new(udfs.clone())));
    }
    options
}

#[derive(Debug, Clone)]
pub(super) struct RuntimeVmSchemaPair {
    pub(super) input: StdArc<arrow_schema::Schema>,
    pub(super) input_sensitivity: VmSchemaSensitivity,
    pub(super) output: StdArc<arrow_schema::Schema>,
    pub(super) output_sensitivity: VmSchemaSensitivity,
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledDomainUdfs {
    pub(super) models: Vec<CreateUdf>,
    pub(super) executor: UdfExecutor,
}

pub(super) fn referenced_materialized_stream_bindings(
    parsed: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    writable_namespaces: &HashSet<String>,
    available_materialized_streams: &HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    current_branching: &[FieldName],
) -> Result<(Vec<VmCompileBinding>, MaterializedProgramInterest), String> {
    let mut fields_by_relay = HashMap::<RelayName, BTreeSet<String>>::default();
    for (relay, field) in collect_program_field_refs(&parsed.inner) {
        if writable_namespaces.contains(&relay)
            || relay == INGEST_METADATA_NAMESPACE
            || relay == BRANCH_NAMESPACE
        {
            continue;
        }
        let Some(relay_name) = relay.strip_prefix("relay_state.") else {
            continue;
        };
        let Ok(relay) = RelayName::parse(relay_name) else {
            continue;
        };
        let Some(spec) = available_materialized_streams.get(&relay) else {
            continue;
        };
        if !spec.branching.is_empty() && spec.branching != current_branching {
            return Err(format!(
                "materialized relay 'relay_state.{}' uses branch fields ({}) but current input \
                 uses ({})",
                relay.as_str(),
                format_branched_by(&spec.branching),
                format_branched_by(current_branching),
            ));
        }
        fields_by_relay.entry(relay).or_default().insert(field);
    }

    let mut bindings = Vec::with_capacity(fields_by_relay.len());
    let mut interest = Vec::with_capacity(fields_by_relay.len());
    for (relay, fields) in fields_by_relay {
        let Some(spec) = available_materialized_streams.get(&relay) else {
            continue;
        };
        // The referenced fields are a set, so schema projection tests membership by key while
        // still walking the set in the sorted order the binding and interest lists require.
        let ordered_fields = fields;
        let projected_fields = spec
            .schema
            .fields()
            .iter()
            .filter(|field| ordered_fields.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>();
        let projected_sensitivity = VmSchemaSensitivity::from_sensitive_fields(
            ordered_fields
                .iter()
                .filter(|field| spec.sensitivity.is_sensitive(field))
                .cloned(),
        );
        let field_interests = ordered_fields
            .iter()
            .map(|name| {
                spec.schema
                    .index_of(name)
                    .map(|column_index| MaterializedFieldInterest {
                        name: name.clone(),
                        column_index,
                    })
                    .map_err(|_| {
                        format!(
                            "materialized relay '{}' has no field '{}'",
                            relay.as_str(),
                            name
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        bindings.push(
            VmCompileBinding::readonly(
                format!("relay_state.{}", relay.as_str()),
                StdArc::new(arrow_schema::Schema::new(projected_fields)),
            )
            .with_sensitivity(projected_sensitivity),
        );
        interest.push(MaterializedRelayInterest {
            relay,
            schema: spec.schema.clone(),
            fields: field_interests,
            key_mode: if spec.branching.is_empty() {
                MaterializedLookupKeyMode::Root
            } else {
                MaterializedLookupKeyMode::CurrentBranch
            },
        });
    }
    interest.sort_by(|left, right| left.relay.as_str().cmp(right.relay.as_str()));

    Ok((bindings, MaterializedProgramInterest { relays: interest }))
}

pub(super) fn collect_expression_field_paths(
    expression: &SpannedExpr,
    fields: &mut Vec<FieldPath>,
) {
    match &expression.inner {
        Expr::FieldRef(field) => {
            fields.push(FieldPath::new(format!("{}.{}", field.relay, field.field)));
        }
        Expr::InternalFieldRef(_) => {}
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expression_field_paths(expr, fields);
        }
        Expr::Binary { left, right, .. } => {
            collect_expression_field_paths(left, fields);
            collect_expression_field_paths(right, fields);
        }
        Expr::Call { args, .. } => {
            for argument in args {
                collect_expression_field_paths(argument, fields);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expression_field_paths(operand, fields);
            }
            for branch in branches {
                collect_expression_field_paths(&branch.when, fields);
                collect_expression_field_paths(&branch.result, fields);
            }
            if let Some(else_result) = else_result {
                collect_expression_field_paths(else_result, fields);
            }
        }
        Expr::Literal(_) => {}
    }
}

pub(super) fn compiled_message_error_sites(
    program: &nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    set_operations: &[MessageErrorOperation],
    filter_operation: Option<MessageErrorOperation>,
) -> Result<CompiledMessageErrorSites, String> {
    if set_operations.len() != program.inner.set.len() {
        return Err(format!(
            "message-error metadata has {} SET operations for {} lowered assignments",
            set_operations.len(),
            program.inner.set.len()
        ));
    }
    let mut sites = CompiledMessageErrorSites::new();
    for (index, (assignment, operation)) in program.inner.set.iter().zip(set_operations).enumerate()
    {
        let (target, expression) = assignment;
        let mut fields = vec![FieldPath::new(format!("{}.{}", target.relay, target.field))];
        collect_expression_field_paths(expression, &mut fields);
        sites.insert(
            expression.span,
            CompiledMessageErrorSite {
                operation: *operation,
                operation_index: Some(
                    u32::try_from(index)
                        .map_err(|_| "too many ordered SET operations".to_string())?,
                ),
                fields: SortedSet::from_unsorted(fields),
            },
        );
    }
    if let Some(expression) = &program.inner.filter {
        let mut fields = Vec::new();
        collect_expression_field_paths(expression, &mut fields);
        sites.insert(
            expression.span,
            CompiledMessageErrorSite {
                operation: filter_operation.ok_or_else(|| {
                    "message-error metadata is missing the filter operation".to_string()
                })?,
                operation_index: None,
                fields: SortedSet::from_unsorted(fields),
            },
        );
    }
    for (index, invocation) in program.inner.invoke.iter().enumerate() {
        let mut fields = Vec::new();
        for argument in &invocation.inner.args {
            collect_expression_field_paths(argument, &mut fields);
        }
        sites.insert(
            invocation.span,
            CompiledMessageErrorSite {
                operation: MessageErrorOperation::Invoke,
                operation_index: Some(
                    u32::try_from(index)
                        .map_err(|_| "too many ordered INVOKE operations".to_string())?,
                ),
                fields: SortedSet::from_unsorted(fields),
            },
        );
    }
    Ok(sites)
}

pub(super) fn message_error_arrow_schema() -> StdArc<arrow_schema::Schema> {
    StdArc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("reference", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("code", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("message", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("operation", ArrowDataType::Utf8, false),
        arrow_schema::Field::new("operation_index", ArrowDataType::UInt32, true),
        arrow_schema::Field::new(
            "fields",
            ArrowDataType::List(StdArc::new(arrow_schema::Field::new(
                "item",
                ArrowDataType::Utf8,
                false,
            ))),
            false,
        ),
        arrow_schema::Field::new(
            "occurred_at",
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
    ]))
}

pub(super) fn all_optional_arrow_schema(schema: &CompiledSchema) -> StdArc<arrow_schema::Schema> {
    StdArc::new(arrow_schema::Schema::new(
        schema
            .arrow_schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn compile_message_error_set_program(
    domain: &DomainName,
    node: &ModelName,
    assignments: &[Assignment],
    output_schema: Arc<CompiledSchema>,
    schemas: MessageErrorCompileSchemas,
    context: RuntimeVmCompileContext<'_>,
) -> Result<CompiledProgramWithMaterializedInterest, String> {
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: assignments.to_vec(),
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("error_output", "error_output"),
    )
    .map_err(|reason| {
        format!(
            "message-error SET for '{}' in domain '{}' is invalid: {reason}",
            node.as_str(),
            domain.as_str()
        )
    })?;
    let set_operations = vec![MessageErrorOperation::Set; parsed.inner.set.len()];
    let error_sites = compiled_message_error_sites(&parsed, &set_operations, None)?;
    let mut bindings = vec![
        VmCompileBinding::writable("error_output", output_schema.arrow_schema())
            .with_sensitivity(output_schema.vm_sensitivity()),
    ];
    if let Some(input) = schemas.input {
        bindings.push(
            VmCompileBinding::readonly("input", input.arrow_schema())
                .with_sensitivity(input.vm_sensitivity()),
        );
    }
    if let Some(left) = schemas.left {
        bindings.push(
            VmCompileBinding::readonly("left", left.arrow_schema())
                .with_sensitivity(left.vm_sensitivity()),
        );
    }
    if let Some(right) = schemas.right {
        bindings.push(
            VmCompileBinding::readonly("right", right.arrow_schema())
                .with_sensitivity(right.vm_sensitivity()),
        );
    }
    if let Some(partial_output) = schemas.partial_output {
        bindings.push(
            VmCompileBinding::readonly(
                "partial_output",
                all_optional_arrow_schema(partial_output.as_ref()),
            )
            .with_sensitivity(partial_output.vm_sensitivity()),
        );
    }
    bindings.push(VmCompileBinding::readonly(
        "error",
        message_error_arrow_schema(),
    ));

    let local_namespaces = HashSet::from_iter([
        "error_output".to_string(),
        "input".to_string(),
        "left".to_string(),
        "right".to_string(),
        "partial_output".to_string(),
        "error".to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        &schemas.current_branching,
    )?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups)?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        "error_output",
        &bindings,
        context.udfs,
    )?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let output_sensitivity = output_schema.vm_sensitivity();
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema.arrow_schema(),
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            allow_header_reads: schemas.allow_header_reads,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| {
        format!(
            "message-error SET compile failed for '{}': {}",
            node.as_str(),
            error.message
        )
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    })
}

pub(super) fn compile_expression_filter_program(
    target: RuntimeCompileTarget<'_>,
    filter: Option<&nervix_models::Expression>,
    input: RuntimeVmSchema,
    allow_header_reads: bool,
    filter_operation: MessageErrorOperation,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    compile_scoped_filter_program(
        target,
        filter,
        input,
        filter_operation,
        context,
        RuntimeFilterScope::Source {
            namespace: "input",
            allow_header_reads,
            allow_metadata: allow_header_reads,
        },
    )
}

pub(super) fn compile_finalized_output_filter_program(
    domain: &DomainName,
    identifier: &ModelName,
    filter: Option<&nervix_models::Expression>,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    compile_scoped_filter_program(
        RuntimeCompileTarget { domain, identifier },
        filter,
        RuntimeVmSchema {
            schema: output_schema,
            sensitivity: output_sensitivity,
        },
        MessageErrorOperation::RouteWhere,
        context,
        RuntimeFilterScope::FinalizedOutput,
    )
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RuntimeFilterScope {
    Source {
        namespace: &'static str,
        allow_header_reads: bool,
        allow_metadata: bool,
    },
    FinalizedOutput,
}

impl RuntimeFilterScope {
    pub(super) const fn namespace(self) -> &'static str {
        match self {
            Self::Source { namespace, .. } => namespace,
            Self::FinalizedOutput => "output",
        }
    }

    pub(super) const fn allow_header_reads(self) -> bool {
        match self {
            Self::Source {
                allow_header_reads, ..
            } => allow_header_reads,
            Self::FinalizedOutput => false,
        }
    }

    pub(super) const fn allow_metadata(self) -> bool {
        match self {
            Self::Source { allow_metadata, .. } => allow_metadata,
            Self::FinalizedOutput => false,
        }
    }
}

pub(super) fn compile_scoped_filter_program(
    target: RuntimeCompileTarget<'_>,
    filter: Option<&nervix_models::Expression>,
    input: RuntimeVmSchema,
    filter_operation: MessageErrorOperation,
    context: RuntimeVmCompileContext<'_>,
    scope: RuntimeFilterScope,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchema {
        schema,
        sensitivity,
    } = input;
    let Some(filter) = filter else {
        return Ok(None);
    };
    let parsed = match scope {
        RuntimeFilterScope::Source { .. } => lower_route_construction(
            &RouteConstruction {
                where_clause: Some(filter.clone()),
                ..RouteConstruction::default()
            },
            SemanticNamespaces::new("input", "__invalid_filter_target"),
        ),
        RuntimeFilterScope::FinalizedOutput => lower_finalized_output_filter(filter, &schema),
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!("filter for '{}' is invalid: {reason}", identifier.as_str()),
    })?;
    let error_sites =
        compiled_message_error_sites(&parsed, &[], Some(filter_operation)).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason,
            }
        })?;
    let mut local_namespaces =
        HashSet::from_iter([scope.namespace().to_string(), BRANCH_NAMESPACE.to_string()]);
    if scope.allow_metadata() {
        local_namespaces.insert(INGEST_METADATA_NAMESPACE.to_string());
    }
    let mut bindings = vec![
        VmCompileBinding::writable(scope.namespace(), schema.clone())
            .with_sensitivity(sensitivity.clone()),
    ];
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "filter compile failed for '{}': {reason}",
                    identifier.as_str()
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        scope.namespace(),
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "filter compile failed for '{}': {reason}",
            identifier.as_str()
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        schema,
        sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            allow_header_reads: scope.allow_header_reads(),
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "filter compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity: sensitivity,
        materialized_interest,
        output_namespace_input: match scope {
            RuntimeFilterScope::Source { .. } => OutputNamespaceInput::Uninitialized,
            RuntimeFilterScope::FinalizedOutput => OutputNamespaceInput::Finalized,
        },
        lookup_hash_maps,
        error_sites,
    }))
}

pub(super) fn compile_processor_output_filter_map_program(
    target: RuntimeCompileTarget<'_>,
    input_relays: &[RelayName],
    output_relay: &RelayName,
    construction: &RouteConstruction,
    schemas: RuntimeVmSchemaPair,
    inferencer_tensors: Option<InferencerFilterMapTensors<'_>>,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchemaPair {
        input: input_schema,
        input_sensitivity,
        output: output_schema,
        output_sensitivity,
    } = schemas;
    let parsed = if let Some(tensors) = inferencer_tensors {
        lower_generated_route(
            construction,
            output_schema.as_ref(),
            tensors.output_arrow_schema().as_ref(),
        )
    } else {
        lower_transforming_route(construction, &input_schema, &output_schema)
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output construction for '{}' is invalid: {reason}",
            identifier
        ),
    })?;
    let inherited_count = if inferencer_tensors.is_some() {
        0
    } else {
        parsed
            .inner
            .set
            .len()
            .checked_sub(construction.assignments.len())
            .verified(
                "a compiled construction lists one set operation per inherited field before its \
                 assignments",
            )
    };
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
    ];
    if let Some(tensors) = inferencer_tensors {
        bindings.push(VmCompileBinding::readonly(
            "generated",
            tensors.output_arrow_schema(),
        ));
    } else {
        bindings.insert(
            0,
            VmCompileBinding::readonly("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
        );
        for relay in input_relays {
            bindings.push(
                VmCompileBinding::readonly(relay.as_str(), input_schema.clone())
                    .with_sensitivity(input_sensitivity.clone()),
            );
        }
    }
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let mut local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "generated".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    if inferencer_tensors.is_none() {
        local_namespaces.extend(input_relays.iter().map(|relay| relay.as_str().to_string()));
        local_namespaces.insert(output_relay.as_str().to_string());
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

pub(super) fn compile_output_branch_program(
    target: RuntimeCompileTarget<'_>,
    branch: Option<&OutputBranch>,
    input: RuntimeVmSchema,
    output: RuntimeVmSchema,
    branch_schema: Option<StdArc<arrow_schema::Schema>>,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledBranchProgram>, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let Some(OutputBranch::BranchedBy { assignments, .. }) = branch else {
        return Ok(None);
    };
    if assignments.is_empty() {
        return Ok(None);
    }
    let branch_schema = branch_schema.ok_or_else(|| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch construction for '{}' has no branch schema",
            identifier.as_str()
        ),
    })?;
    let parsed = lower_branch_construction(
        assignments,
        branch_schema.as_ref(),
        output.schema.as_ref(),
        input.schema.as_ref(),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch construction for '{}' is invalid: {}",
            identifier.as_str(),
            reason
        ),
    })?;
    let error_sites = compiled_message_error_sites(
        &parsed,
        &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
        None,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::readonly("input", input.schema.clone())
            .with_sensitivity(input.sensitivity),
        VmCompileBinding::readonly("output", output.schema.clone())
            .with_sensitivity(output.sensitivity.clone()),
        VmCompileBinding::readonly("message", output.schema).with_sensitivity(output.sensitivity),
        VmCompileBinding::writable(BRANCH_NAMESPACE, branch_schema.clone()),
    ];
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "output".to_string(),
        "message".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "output branch compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        BRANCH_NAMESPACE,
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch compile failed for '{}': {}",
            identifier.as_str(),
            reason
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let sensitivity = VmSchemaSensitivity::default();
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        branch_schema,
        sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "output branch compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledBranchProgram {
        program: CompiledProgramWithMaterializedInterest {
            compiled: Arc::new(compiled),
            output_sensitivity: sensitivity,
            materialized_interest,
            output_namespace_input: OutputNamespaceInput::Uninitialized,
            lookup_hash_maps,
            error_sites,
        },
    }))
}

pub(super) fn compile_wasm_output_filter_map_program(
    domain: &DomainName,
    identifier: &ModelName,
    construction: &RouteConstruction,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let parsed =
        lower_generated_route(construction, output_schema.as_ref(), output_schema.as_ref())
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "WASM output construction for '{}' is invalid: {reason}",
                    identifier
                ),
            })?;
    if !parsed.inner.invoke.is_empty() {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "WASM processor '{}' TO clauses may use SET and WHERE, but not INVOKE",
                identifier.as_str()
            ),
        });
    }
    let set_operations = vec![MessageErrorOperation::Set; parsed.inner.set.len()];
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let original_parsed = parsed.clone();
    let mut bindings = vec![
        VmCompileBinding::readonly("generated", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
    ];
    if let Some(binding) = context.branch_binding() {
        bindings.push(binding);
    }
    let local_namespaces = HashSet::from_iter([
        "generated".to_string(),
        "output".to_string(),
        BRANCH_NAMESPACE.to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &original_parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

pub(crate) fn compile_emitter_filter_map_program(
    domain: &DomainName,
    emitter: &CreateEmitter,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    output_schema: StdArc<arrow_schema::Schema>,
    output_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledEmitterFilterMapProgram>, RuntimeError> {
    if emitter.construction.is_empty() {
        return Ok(None);
    }
    let codec_route = emitter.encode_using_codec.is_some();
    if !codec_route
        && (emitter.construction.inherit.is_some()
            || !emitter.construction.assignments.is_empty()
            || !emitter.construction.invocations.is_empty())
    {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "direct emitter '{}' supports VALUES and WHERE only",
                emitter.name.as_str()
            ),
        });
    }
    let parsed = if codec_route {
        lower_transforming_route(
            &emitter.construction,
            input_schema.as_ref(),
            output_schema.as_ref(),
        )
    } else {
        lower_route_construction(
            &emitter.construction,
            SemanticNamespaces::new("input", "__invalid_direct_emitter_output"),
        )
    }
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "emitter route '{}' is invalid: {reason}",
            emitter.name.as_str()
        ),
    })?;
    if parsed
        .inner
        .invoke
        .iter()
        .any(|invocation| invocation.inner.function == FunctionName::WriteHeader)
        && !emit_sink_supports_headers(&emitter.sink)
    {
        return Err(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "{} emitters do not support FILTER-MAP headers",
                emitter.sink.transport_label()
            ),
        });
    }
    let inherited_count = if codec_route {
        parsed
            .inner
            .set
            .len()
            .checked_sub(emitter.construction.assignments.len())
            .verified(
                "a compiled construction lists one set operation per inherited field before its \
                 assignments",
            )
    } else {
        0
    };
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let body = compile_emitter_filter_map_part(
        RuntimeCompileTarget {
            domain,
            identifier: &ModelName::from(&emitter.name),
        },
        parsed,
        RuntimeVmSchemaPair {
            input: input_schema,
            input_sensitivity,
            output: output_schema,
            output_sensitivity,
        },
        codec_route,
        error_sites,
        context,
    )?;
    Ok(Some(CompiledEmitterFilterMapProgram { body, codec_route }))
}

pub(crate) fn compile_sqs_fifo_group_program(
    domain: &DomainName,
    emitter: &CreateEmitter,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let EmitSink::Sqs {
        fifo_group: Some(SqsFifoGroup::Expression(expression)),
        ..
    } = emitter.sink.as_ref()
    else {
        return Ok(None);
    };
    let field = FieldName::parse("fifo_group")
        .assured("this is a constant literal that satisfies the identifier grammar");
    let output_schema = StdArc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        field.as_str(),
        ArrowDataType::Utf8,
        false,
    )]));
    let parsed = lower_transforming_route(
        &RouteConstruction {
            assignments: vec![Assignment {
                target: nervix_models::AssignmentTarget::bare(field),
                value: expression.clone(),
            }],
            ..RouteConstruction::default()
        },
        input_schema.as_ref(),
        output_schema.as_ref(),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "SQS FIFO GROUP expression for emitter '{}' is invalid: {reason}",
            emitter.name.as_str()
        ),
    })?;
    let error_sites = compiled_message_error_sites(&parsed, &[MessageErrorOperation::Set], None)
        .map_err(|reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason,
        })?;
    compile_emitter_filter_map_part(
        RuntimeCompileTarget {
            domain,
            identifier: &ModelName::from(&emitter.name),
        },
        parsed,
        RuntimeVmSchemaPair {
            input: input_schema,
            input_sensitivity,
            output: output_schema,
            output_sensitivity: VmSchemaSensitivity::default(),
        },
        true,
        error_sites,
        context,
    )
    .map(Some)
}

pub(super) fn compile_emitter_filter_map_part(
    target: RuntimeCompileTarget<'_>,
    parsed: nervix_vm::program::SpannedNode<nervix_vm::program::Program>,
    schemas: RuntimeVmSchemaPair,
    codec_route: bool,
    error_sites: CompiledMessageErrorSites,
    context: RuntimeVmCompileContext<'_>,
) -> Result<CompiledProgramWithMaterializedInterest, RuntimeError> {
    let RuntimeCompileTarget { domain, identifier } = target;
    let RuntimeVmSchemaPair {
        input: input_schema,
        input_sensitivity,
        output: output_schema,
        output_sensitivity,
    } = schemas;
    let mut bindings = if codec_route {
        vec![
            VmCompileBinding::readonly("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
            VmCompileBinding::writable("output", output_schema.clone())
                .with_sensitivity(output_sensitivity.clone()),
        ]
    } else {
        vec![
            VmCompileBinding::writable("input", input_schema.clone())
                .with_sensitivity(input_sensitivity.clone()),
            VmCompileBinding::readonly("message", input_schema).with_sensitivity(input_sensitivity),
        ]
    };
    let local_namespaces = HashSet::from_iter([
        "input".to_string(),
        "message".to_string(),
        "output".to_string(),
    ]);
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &local_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let lookup_output_namespace = if codec_route { "output" } else { "input" };
    let (lookup_hash_maps, lookup_binding) = compile_lookup_hash_map_calls(
        pending_lookup_calls,
        lookup_output_namespace,
        &bindings,
        context.udfs,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            reason
        ),
    })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: if codec_route {
                VmOutputMode::ExplicitOnly
            } else {
                VmOutputMode::PassthroughByName
            },
            allow_sensitive_output: false,
            allow_header_writes: true,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    })
}

pub(crate) fn compile_session_filter_map_program(
    domain: &DomainName,
    identifier: impl Into<ModelName>,
    where_clause: Option<&nervix_models::Expression>,
    input_schema: StdArc<arrow_schema::Schema>,
    input_sensitivity: VmSchemaSensitivity,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let identifier = identifier.into();
    compile_expression_filter_program(
        RuntimeCompileTarget {
            domain,
            identifier: &identifier,
        },
        where_clause,
        RuntimeVmSchema {
            schema: input_schema,
            sensitivity: input_sensitivity,
        },
        false,
        MessageErrorOperation::SourceWhere,
        context,
    )
}

pub(crate) fn compile_key_projection_program(
    processor_kind: &str,
    processor: &ModelName,
    clause: &str,
    input_relays: &[RelayName],
    expressions: &[nervix_models::Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<VmCompiledProgram, String> {
    if input_relays.is_empty() {
        return Err(format!(
            "{} '{}' {} requires at least one input relay",
            processor_kind,
            processor.as_str(),
            clause
        ));
    }
    let assignments = expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            Ok(nervix_models::Assignment {
                target: nervix_models::AssignmentTarget::bare(
                    FieldName::parse(&format!("key_{index}")).map_err(|error| {
                        format!(
                            "{processor_kind} '{}' has invalid key target: {error}",
                            processor
                        )
                    })?,
                ),
                value: expression.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments,
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", "input"),
    )
    .map_err(|reason| {
        format!(
            "{} '{}' {} is invalid: {}",
            processor_kind,
            processor.as_str(),
            clause,
            reason
        )
    })?;
    let bindings = vec![VmCompileBinding::writable("input", input_schema.clone())];
    let signatures = runtime_udf_signatures(udfs);
    let key_types =
        infer_vm_set_expr_types_for_bindings_with_udfs(&parsed, bindings.clone(), signatures)
            .map_err(|error| {
                format!(
                    "{} '{}' {} compile failed: {}",
                    processor_kind,
                    processor.as_str(),
                    clause,
                    error.message
                )
            })?;
    if key_types.len() != expressions.len() {
        return Err(format!(
            "{} '{}' {} inferred a different number of key fields",
            processor_kind,
            processor.as_str(),
            clause
        ));
    }
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        key_types
            .into_iter()
            .map(|inferred| {
                arrow_schema::Field::new(inferred.field, inferred.data_type, inferred.nullable)
            })
            .collect::<Vec<_>>(),
    ));
    compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        VmSchemaSensitivity::default(),
        bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| {
        format!(
            "{} '{}' {} compile failed: {}",
            processor_kind,
            processor.as_str(),
            clause,
            error.message
        )
    })
}

pub(super) async fn evaluate_constant_expression_vm(
    expression: &nervix_models::Expression,
    udfs: Option<&UdfExecutor>,
    execution_now: Timestamp,
) -> Result<RuntimeValue, String> {
    const OUTPUT_NAMESPACE: &str = "constant";
    const OUTPUT_FIELD: &str = "value";
    let assignment = nervix_models::Assignment {
        target: nervix_models::AssignmentTarget::bare(
            FieldName::parse(OUTPUT_FIELD).map_err(|error| error.to_string())?,
        ),
        value: expression.clone(),
    };
    let parsed = lower_route_construction(
        &RouteConstruction {
            assignments: vec![assignment],
            ..RouteConstruction::default()
        },
        SemanticNamespaces::new("input", OUTPUT_NAMESPACE),
    )
    .map_err(|reason| format!("constant expression is invalid: {reason}"))?;
    let empty_schema = StdArc::new(arrow_schema::Schema::empty());
    let infer_bindings = vec![
        VmCompileBinding::readonly("input", empty_schema.clone()),
        VmCompileBinding::writeonly(OUTPUT_NAMESPACE, empty_schema),
    ];
    let inferred = infer_vm_set_expr_types_for_bindings_with_udfs(
        &parsed,
        infer_bindings,
        runtime_udf_signatures(udfs),
    )
    .map_err(|error| {
        format!(
            "constant expression type inference failed: {}",
            error.message
        )
    })?;
    let output_schema = StdArc::new(arrow_schema::Schema::new(
        inferred
            .into_iter()
            .map(|inferred| {
                arrow_schema::Field::new(inferred.field, inferred.data_type, inferred.nullable)
            })
            .collect::<Vec<_>>(),
    ));
    let bindings = vec![
        VmCompileBinding::readonly("input", StdArc::new(arrow_schema::Schema::empty())),
        VmCompileBinding::writeonly(OUTPUT_NAMESPACE, output_schema.clone()),
    ];
    let compiled = Arc::new(
        compile_vm_program_with_options_for_bindings_with_sensitivity(
            &parsed,
            output_schema,
            VmSchemaSensitivity::default(),
            bindings,
            runtime_udf_compile_options(
                udfs,
                VmCompileOptions {
                    output_mode: VmOutputMode::ExplicitOnly,
                    ..VmCompileOptions::default()
                },
            ),
        )
        .map_err(|error| format!("constant expression compile failed: {}", error.message))?,
    );
    let input = VmTypedBatch::try_new_with_row_count(
        compiled.input_schema.clone(),
        compiled
            .input_schema
            .fields()
            .iter()
            .map(|field| VmTypedArray::uninitialized(field.data_type().clone(), 1))
            .collect(),
        1,
    )
    .map_err(|error| error.to_string())?;
    let result = execute_program_with_selection_in_context(
        &compiled,
        &input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    .map_err(|error| format!("constant expression execution failed: {error}"))?;
    if !result.selected_rows.is_single(0) {
        return Err("constant expression did not produce exactly one row".to_string());
    }
    vm_output_value(&result.batch, 0, OUTPUT_FIELD)?
        .ok_or_else(|| "constant expression produced NULL".to_string())
}

pub(super) fn compile_reorderer_program(
    processor: &ModelName,
    input_relays: &[RelayName],
    order_by: &[nervix_models::Expression],
    input_schema: StdArc<arrow_schema::Schema>,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledReordererProgram, String> {
    if order_by.is_empty() {
        return Err(format!(
            "reorderer '{}' requires at least one BY expression",
            processor.as_str()
        ));
    }
    let compiled = compile_key_projection_program(
        "reorderer",
        processor,
        "BY",
        input_relays,
        order_by,
        input_schema,
        udfs,
    )?;
    Ok(CompiledReordererProgram {
        key_column_offset: 0,
        key_count: order_by.len(),
        program: Arc::new(compiled),
    })
}

pub(super) fn compile_ingestor_filter_map_program(
    domain: &DomainName,
    identifier: impl Into<ModelName>,
    metadata_kind: IngestMetadataKind,
    allow_header_reads: bool,
    construction: &RouteConstruction,
    schemas: RuntimeVmSchemaPair,
    context: RuntimeVmCompileContext<'_>,
) -> Result<Option<CompiledProgramWithMaterializedInterest>, RuntimeError> {
    let identifier = identifier.into();
    let parsed = lower_transforming_route(construction, &schemas.input, &schemas.output).map_err(
        |reason| RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: format!(
                "ingestor output construction for '{}' is invalid: {reason}",
                identifier
            ),
        },
    )?;
    let inherited_count = parsed
        .inner
        .set
        .len()
        .checked_sub(construction.assignments.len())
        .verified(
            "a compiled construction lists one set operation per inherited field before its \
             assignments",
        );
    let set_operations = (0..parsed.inner.set.len())
        .map(|index| {
            if index < inherited_count {
                MessageErrorOperation::Inherit
            } else {
                MessageErrorOperation::Set
            }
        })
        .collect::<Vec<_>>();
    let error_sites = compiled_message_error_sites(
        &parsed,
        &set_operations,
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;

    let mut bindings = vec![
        VmCompileBinding::readonly("input", schemas.input.clone())
            .with_sensitivity(schemas.input_sensitivity),
        VmCompileBinding::writable("output", schemas.output.clone())
            .with_sensitivity(schemas.output_sensitivity.clone()),
    ];
    let writable_namespaces = HashSet::from_iter(["input".to_string(), "output".to_string()]);
    if let Some(metadata_schema) = metadata_kind.integration_arrow_schema() {
        bindings.push(VmCompileBinding::readonly(
            INGEST_METADATA_NAMESPACE,
            metadata_schema,
        ));
    }
    let (materialized_bindings, materialized_interest) = referenced_materialized_stream_bindings(
        &parsed,
        &writable_namespaces,
        context.available_materialized_streams,
        context.current_branching,
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    bindings.extend(materialized_bindings);
    let (parsed, pending_lookup_calls) =
        rewrite_lookup_hash_map_program(&parsed, context.available_lookups).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            }
        })?;
    let (lookup_hash_maps, lookup_binding) =
        compile_lookup_hash_map_calls(pending_lookup_calls, "output", &bindings, context.udfs)
            .map_err(|reason| RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "FILTER-MAP compile failed for '{}': {}",
                    identifier.as_str(),
                    reason
                ),
            })?;
    if let Some(lookup_binding) = lookup_binding {
        bindings.push(lookup_binding);
    }

    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        schemas.output,
        schemas.output_sensitivity.clone(),
        bindings,
        context.compile_options(VmCompileOptions {
            output_mode: VmOutputMode::ExplicitOnly,
            allow_header_reads,
            ..VmCompileOptions::default()
        }),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "FILTER-MAP compile failed for '{}': {}",
            identifier.as_str(),
            error.message
        ),
    })?;
    Ok(Some(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity: schemas.output_sensitivity,
        materialized_interest,
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps,
        error_sites,
    }))
}

/// The schema surface a generator's set-only route compiles against: the output it constructs, that
/// output's sensitivity, the materialized source it reads, and the branch it preserves.
pub(super) struct GeneratorSetProgramSchemas {
    pub(super) output: StdArc<arrow_schema::Schema>,
    pub(super) output_sensitivity: VmSchemaSensitivity,
    pub(super) source: StdArc<arrow_schema::Schema>,
    pub(super) branch: Option<StdArc<arrow_schema::Schema>>,
}

pub(super) fn compile_generator_set_program(
    domain: &DomainName,
    generator: &CreateGenerator,
    output: &ProcessorOutput,
    schemas: GeneratorSetProgramSchemas,
    udfs: Option<&UdfExecutor>,
) -> Result<CompiledProgramWithMaterializedInterest, RuntimeError> {
    let GeneratorSetProgramSchemas {
        output: output_schema,
        output_sensitivity,
        source: source_schema,
        branch: branch_schema,
    } = schemas;
    let parsed =
        lower_set_only_route(&output.construction, output_schema.as_ref()).map_err(|reason| {
            RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "generator '{}' output '{}' is invalid: {reason}",
                    generator.name, output.relay
                ),
            }
        })?;
    let error_sites = compiled_message_error_sites(
        &parsed,
        &vec![MessageErrorOperation::Set; parsed.inner.set.len()],
        Some(MessageErrorOperation::RouteWhere),
    )
    .map_err(|reason| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason,
    })?;
    let mut bindings = vec![
        VmCompileBinding::writable("output", output_schema.clone())
            .with_sensitivity(output_sensitivity.clone()),
        VmCompileBinding::readonly(
            format!("relay_state.{}", generator.materialized_relay),
            source_schema,
        ),
    ];
    if let Some(branch_schema) = branch_schema {
        bindings.push(VmCompileBinding::readonly("branch", branch_schema));
    }
    let compiled = compile_vm_program_with_options_for_bindings_with_sensitivity(
        &parsed,
        output_schema,
        output_sensitivity.clone(),
        bindings,
        runtime_udf_compile_options(
            udfs,
            VmCompileOptions {
                output_mode: VmOutputMode::ExplicitOnly,
                ..VmCompileOptions::default()
            },
        ),
    )
    .map_err(|error| RuntimeError::BuildDomainExecution {
        domain: domain.as_str().to_string(),
        reason: format!(
            "generator '{}' output '{}' compile failed: {}",
            generator.name, output.relay, error.message
        ),
    })?;
    Ok(CompiledProgramWithMaterializedInterest {
        compiled: Arc::new(compiled),
        output_sensitivity,
        materialized_interest: MaterializedProgramInterest::default(),
        output_namespace_input: OutputNamespaceInput::Uninitialized,
        lookup_hash_maps: Vec::new(),
        error_sites,
    })
}

/// Compiles an output route's FILTER-MAP program once and caches it on the route.
///
/// Kept separate from evaluation so every selected route is compiled before the batch scope is
/// built: the scope's materialized snapshot has to cover all of their programs.
pub(super) fn compile_processor_output_program(
    context: &mut ProcessorOutputDispatchContext<'_>,
    output: &mut RelayProcessorOutputNode,
    batch: &RelayRecordBatch,
    output_schema: &Arc<CompiledSchema>,
) -> Result<(), PlannedGeneralError> {
    if output.compiled_program.is_some() {
        return Ok(());
    }
    let input_relays = context.filter_source.relays(context.input_relays);
    let materialized_stream_specs = materialized_stream_specs_for_graph(
        &context.branch.runtime,
        &context.branch.domain,
        context.graph,
    );
    let current_branching = if let Some(relay) = input_relays.first()
        && let Some(execution) = context
            .branch
            .runtime
            .inner
            .executions
            .get(&context.branch.domain)
        && let Some(branching) = execution.relay_branchings.get(relay)
    {
        branching.clone()
    } else {
        Default::default()
    };
    let current_branch_schema = if let Some(relay) = input_relays.first() {
        relay_branch_schema_for_runtime(&context.branch.runtime, &context.branch.domain, relay)
    } else {
        None
    };
    let available_lookups = match context
        .branch
        .runtime
        .inner
        .executions
        .get(&context.branch.domain)
    {
        Some(execution) => execution.lookups.clone(),
        None => HashMap::default(),
    };
    let udfs = context
        .branch
        .runtime
        .inner
        .executions
        .get(&context.branch.domain)
        .map(|execution| execution.udfs.clone());
    let input_sensitivity = processor_output_input_sensitivity(context.branch, &input_relays);
    let compile_context = RuntimeVmCompileContext {
        available_materialized_streams: &materialized_stream_specs,
        available_lookups: &available_lookups,
        current_branching: &current_branching,
        current_branch_schema: current_branch_schema.as_ref(),
        current_branch_sensitivity: None,
        udfs: udfs.as_ref(),
    };
    let compiled = match context.filter_source {
        ProcessorOutputFilterSource::OutputRelay => compile_finalized_output_filter_program(
            &context.branch.domain,
            context.processor,
            output.construction.where_clause.as_ref(),
            output_schema.arrow_schema(),
            output_schema.vm_sensitivity(),
            compile_context,
        ),
        ProcessorOutputFilterSource::InputRelays | ProcessorOutputFilterSource::Inferencer(_) => {
            compile_processor_output_filter_map_program(
                RuntimeCompileTarget {
                    domain: &context.branch.domain,
                    identifier: context.processor,
                },
                &input_relays,
                &output.relay,
                &output.construction,
                RuntimeVmSchemaPair {
                    input: batch.arrow_schema(),
                    input_sensitivity,
                    output: output_schema.arrow_schema(),
                    output_sensitivity: output_schema.vm_sensitivity(),
                },
                context.filter_source.inferencer_tensors(),
                compile_context,
            )
        }
    };
    match compiled {
        Ok(program) => {
            output.compiled_program = program;
            Ok(())
        }
        Err(error) => Err(PlannedGeneralError {
            acks: batch.acks.clone(),
            reason: error.to_string(),
        }),
    }
}

pub(super) fn relay_schema_for_runtime(
    runtime: &Runtime,
    domain: &DomainName,
    relay: &RelayName,
) -> Result<Arc<CompiledSchema>, String> {
    let Some(execution) = runtime.inner.executions.get(domain) else {
        return Err(format!("domain '{}' is not instantiated", domain.as_str()));
    };
    execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
        format!(
            "stream '{}' schema is not instantiated in domain '{}'",
            relay.as_str(),
            domain.as_str()
        )
    })
}

pub(super) fn relay_branch_schema_for_runtime(
    runtime: &Runtime,
    domain: &DomainName,
    relay: &RelayName,
) -> Option<StdArc<arrow_schema::Schema>> {
    runtime
        .inner
        .executions
        .get(domain)
        .and_then(|execution| execution.relay_branching_schemas.get(relay).cloned())
        .flatten()
}

pub(super) fn materialized_stream_specs_for_graph(
    runtime: &Runtime,
    domain: &DomainName,
    _graph: &SharedActiveGraph,
) -> HashMap<RelayName, RuntimeMaterializedRelaySpec> {
    let Some(execution) = runtime.inner.executions.get(domain) else {
        return HashMap::default();
    };
    execution.materialized_stream_specs.clone()
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{CreateSchema, ModelName, ParseAsType, Timestamp};
    use triomphe::Arc;

    use super::*;
    use crate::runtime_schema::{RuntimeValue, compile_schema, test_runtime_row};
    #[test]
    fn filter_map_rejects_branch_namespace_without_branch_schema() {
        let schema = test_schema(&[("tenant", ParseAsType::String)]);
        let error = compile_processor_output_filter_map_program(
            RuntimeCompileTarget {
                domain: &domain("default"),
                identifier: &named("project_notifications"),
            },
            &[named("notifications")],
            &named("projected_notifications"),
            &construction("INHERIT ALL WHERE branch.tenant = output.tenant"),
            RuntimeVmSchemaPair {
                input: schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            None,
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect_err("branch namespace must require a branch schema");
        let error = error.to_string();

        assert!(
            error.contains("branch.tenant") || error.contains("namespace 'branch'"),
            "expected branch namespace error, got {error}"
        );
    }

    #[test]
    fn processor_key_expressions_reject_relay_qualified_fields() {
        assert!(nervix_nspl::parse_expression("incoming_notifications.sequence").is_err());
    }

    #[tokio::test]
    async fn ingestor_filter_map_accepts_missing_optional_input_fields() {
        let input_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("optional_logic"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("raw"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }));
        let output_schema = Arc::new(compile_schema(&CreateSchema {
            name: named("optional_logic_output"),
            fields: vec![
                nervix_models::SchemaField {
                    name: named("tenant"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: named("normalized"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }));
        let program = compile_ingestor_filter_map_program(
            &domain("default"),
            named::<ModelName>("logic_ingestor"),
            IngestMetadataKind::Headers,
            true,
            &construction("INHERIT tenant SET normalized = lower(input.raw)"),
            RuntimeVmSchemaPair {
                input: input_schema.arrow_schema(),
                input_sensitivity: VmSchemaSensitivity::default(),
                output: output_schema.arrow_schema(),
                output_sensitivity: VmSchemaSensitivity::default(),
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("filter-map must compile")
        .expect("program must exist");

        let output = execute_filter_map_for_test(
            &program,
            test_runtime_row([(
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            )]),
            None,
            None,
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("filter-map must execute")
        .expect("record must not be filtered out");

        assert_eq!(
            row_value(&output, "tenant"),
            Some(RuntimeValue::String("acme".to_string()))
        );
        assert!(row_value(&output, "raw").is_none());
        assert!(row_value(&output, "normalized").is_none());
    }

    #[tokio::test]
    async fn finalized_output_filter_reads_constructed_output_values() {
        let output_schema =
            test_schema(&[("tenant", ParseAsType::String), ("total", ParseAsType::I64)]);
        let program = compile_finalized_output_filter_program(
            &domain("default"),
            &named("aggregate_route"),
            Some(&expression("output.total >= 100 AND tenant = \"acme\"")),
            output_schema.arrow_schema(),
            VmSchemaSensitivity::default(),
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("finalized output filter must compile")
        .expect("program must exist");

        let selected = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("total".to_string(), RuntimeValue::I64(130)),
        ]);
        assert!(
            execute_filter_map_for_test(
                &program,
                selected,
                None,
                None,
                Timestamp::from_unix_nanos(1),
            )
            .await
            .expect("finalized output filter must execute")
            .is_some()
        );

        let rejected = test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("total".to_string(), RuntimeValue::I64(99)),
        ]);
        assert!(
            execute_filter_map_for_test(
                &program,
                rejected,
                None,
                None,
                Timestamp::from_unix_nanos(1),
            )
            .await
            .expect("finalized output filter must execute")
            .is_none()
        );
    }
}
