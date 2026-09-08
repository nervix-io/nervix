use super::*;

pub(super) struct MessageErrorContext<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) node_kind: ModelKind,
    pub(super) node: &'a ModelName,
    pub(super) source_route: Option<&'a RelayName>,
    pub(super) message: &'a RelayMessage,
    pub(super) error: &'a StructuredMessageError,
    pub(super) partial_output: Option<&'a RuntimeRecordBatch>,
    pub(super) materialized_state: &'a HashMap<String, RuntimeValue>,
    pub(super) ingest_metadata: Option<&'a IngestFilterMapMetadata>,
}

pub(super) struct MessageErrorHandling<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) node_kind: ModelKind,
    pub(super) node: &'a ModelName,
    pub(super) source_route: Option<&'a RelayName>,
    pub(super) policy: &'a MessageErrorPolicy,
    pub(super) message: RelayMessage,
    pub(super) error: StructuredMessageError,
    pub(super) partial_output: Option<RuntimeRecordBatch>,
    pub(super) materialized_state: HashMap<String, RuntimeValue>,
    pub(super) ingest_metadata: Option<IngestFilterMapMetadata>,
}

pub(super) struct MessageErrorFailure {
    pub(super) source_route: Option<RelayName>,
    pub(super) reason: String,
    pub(super) operation: MessageErrorOperation,
}

impl MessageErrorFailure {
    pub(super) fn publish(source_route: Option<&RelayName>, reason: String) -> Self {
        Self::new(source_route, reason, MessageErrorOperation::Publish)
    }

    pub(super) fn new(
        source_route: Option<&RelayName>,
        reason: String,
        operation: MessageErrorOperation,
    ) -> Self {
        Self {
            source_route: source_route.cloned(),
            reason,
            operation,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct MessageErrorCompileSchemas {
    pub(super) input: Option<Arc<CompiledSchema>>,
    pub(super) left: Option<Arc<CompiledSchema>>,
    pub(super) right: Option<Arc<CompiledSchema>>,
    pub(super) partial_output: Option<Arc<CompiledSchema>>,
    pub(super) current_branching: Vec<FieldName>,
    pub(super) allow_header_reads: bool,
}

#[derive(Debug)]
pub(super) enum SingleRecordFilterMapOutcome {
    Filtered,
    Output(RuntimeRow),
    MessageError {
        error: StructuredMessageError,
        partial_output: Option<RuntimeRecordBatch>,
        materialized_state: HashMap<String, RuntimeValue>,
    },
}

pub(super) fn structured_message_error(
    code: MessageErrorCode,
    message: String,
    operation: MessageErrorOperation,
    operation_index: Option<u32>,
    fields: impl IntoIterator<Item = FieldPath>,
) -> StructuredMessageError {
    StructuredMessageError {
        reference: uuid::Uuid::now_v7(),
        code,
        message,
        operation,
        operation_index,
        fields: SortedSet::from_unsorted(fields.into_iter().collect()),
        occurred_at: current_timestamp(),
    }
}

pub(super) fn planned_structured_message_error(
    message: RelayMessage,
    error: StructuredMessageError,
    partial_output: Option<RuntimeRecordBatch>,
    materialized_state: HashMap<String, RuntimeValue>,
) -> PlannedMessageError {
    PlannedMessageError {
        message,
        error,
        partial_output,
        materialized_state,
    }
}

pub(super) fn operation_for_filter_label(label: &str) -> MessageErrorOperation {
    match label {
        "FROM WHERE" => MessageErrorOperation::SourceWhere,
        "FILTER WHERE" => MessageErrorOperation::FilterWhere,
        _ => MessageErrorOperation::Set,
    }
}

pub(super) fn preserved_message_error_branch(
    target_branching: &[FieldName],
    incoming: &Option<BranchKey>,
    relay: &RelayName,
    reference: uuid::Uuid,
) -> Result<Option<BranchKey>, String> {
    match (target_branching.is_empty(), incoming.as_ref()) {
        (true, None) | (false, Some(_)) => Ok(incoming.clone()),
        (true, Some(_)) => Err(format!(
            "unbranched DLQ relay '{}' cannot receive branched message error {}",
            relay, reference
        )),
        (false, None) => Err(format!(
            "branched DLQ relay '{}' cannot receive unbranched message error {}",
            relay, reference
        )),
    }
}

/// The `partial_output` view an error handler sees, for the callers that do not surface a failed
/// capture in the error itself.
///
/// A capture that fails leaves the view absent, which the error record already models as an
/// `Option`, so the recovery is defined rather than improvised. It is logged instead of dropped so
/// that an absent view is never mistaken for a route that had nothing to capture. Callers that put
/// the failure into the error reason call [`vm_partial_output_row_to_runtime_batch`] directly and
/// keep the error.
pub(super) fn captured_partial_output(
    batch: &VmTypedBatch,
    row: usize,
) -> Option<RuntimeRecordBatch> {
    match vm_partial_output_row_to_runtime_batch(batch, row) {
        Ok(partial_output) => Some(partial_output),
        Err(error) => {
            debug!(
                error,
                row, "failed to capture the partial output view for a message error"
            );
            None
        }
    }
}

pub(super) fn vm_partial_output_row_to_runtime_batch(
    batch: &VmTypedBatch,
    row: usize,
) -> Result<RuntimeRecordBatch, String> {
    if row >= batch.row_count() {
        return Err(format!(
            "partial output row {row} is outside batch with {} rows",
            batch.row_count()
        ));
    }
    let mut fields_and_columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let array = match column {
            VmTypedArray::Uninitialized { .. } => continue,
            column => column.to_array_ref().slice(row, 1),
        };
        let field_name = field
            .name()
            .strip_prefix("output.")
            .unwrap_or(field.name())
            .to_string();
        fields_and_columns.push((
            StdArc::new(arrow_schema::Field::new(
                field_name,
                field.data_type().clone(),
                true,
            )),
            array,
        ));
    }
    let (fields, columns): (Vec<_>, Vec<_>) = fields_and_columns.into_iter().unzip();
    let schema = StdArc::new(arrow_schema::Schema::new(fields));
    let record_batch = if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|error| error.to_string())?;
    RuntimeRecordBatch::from_record_batch(schema, record_batch)
}

pub(super) fn invalid_output_fields(batch: &VmTypedBatch, row: usize) -> Vec<FieldPath> {
    let mut invalid_fields = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let invalid = match column {
            VmTypedArray::Uninitialized { .. } => true,
            VmTypedArray::UInt8(array) => array.is_null(row),
            VmTypedArray::Int8(array) => array.is_null(row),
            VmTypedArray::UInt16(array) => array.is_null(row),
            VmTypedArray::Int16(array) => array.is_null(row),
            VmTypedArray::UInt32(array) => array.is_null(row),
            VmTypedArray::Int32(array) => array.is_null(row),
            VmTypedArray::UInt64(array) => array.is_null(row),
            VmTypedArray::Int64(array) => array.is_null(row),
            VmTypedArray::Float32(array) => array.is_null(row),
            VmTypedArray::Float64(array) => array.is_null(row),
            VmTypedArray::Boolean(array) => array.is_null(row),
            VmTypedArray::Utf8(array) => array.is_null(row),
            VmTypedArray::Datetime(array) => array.is_null(row),
            VmTypedArray::Generic(array) => array.is_null(row),
        };
        if invalid && !field.is_nullable() {
            invalid_fields.push(FieldPath::new(format!(
                "output.{}",
                field.name().strip_prefix("output.").unwrap_or(field.name())
            )));
        }
    }
    invalid_fields
}

impl Runtime {
    pub(in crate::runtime) async fn handle_message_error(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: &ModelName,
        policies: &ErrorPolicies,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorFailure {
            source_route,
            reason,
            operation,
        } = failure;
        self.handle_structured_message_error(MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route: source_route.as_ref(),
            policy: &policies.message,
            message,
            error: structured_message_error(
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
        })
        .await;
    }

    pub(in crate::runtime) async fn handle_message_error_with_policy(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: &ModelName,
        policy: &MessageErrorPolicy,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorFailure {
            source_route,
            reason,
            operation,
        } = failure;
        self.handle_structured_message_error(MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route: source_route.as_ref(),
            policy,
            message,
            error: structured_message_error(
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
        })
        .await;
    }

    pub(in crate::runtime) async fn handle_structured_message_error(
        &self,
        handling: MessageErrorHandling<'_>,
    ) {
        let MessageErrorHandling {
            domain,
            node_kind,
            node,
            source_route,
            policy,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
        } = handling;
        match policy {
            MessageErrorPolicy::Ignore => {
                message.acks.ack_success();
            }
            MessageErrorPolicy::Log => {
                self.inner.events.report_error(format!(
                    "{} '{}' message error in domain '{}': {}",
                    node_kind.as_str(),
                    node.as_str(),
                    domain.as_str(),
                    error.message
                ));
                warn!(
                    domain = domain.as_str(),
                    node_kind = node_kind.as_str(),
                    node = node.as_str(),
                    error_reference = %error.reference,
                    error_code = error.code.as_ref(),
                    error_operation = error.operation.as_ref(),
                    reason = %error.message,
                    "runtime node handled message error"
                );
                message.acks.no_ack(error.message);
            }
            MessageErrorPolicy::Dlq { relay, assignments } => {
                let context = MessageErrorContext {
                    domain,
                    node_kind,
                    node,
                    source_route,
                    message: &message,
                    error: &error,
                    partial_output: partial_output.as_ref(),
                    materialized_state: &materialized_state,
                    ingest_metadata: ingest_metadata.as_ref(),
                };
                if let Err(dispatch_error) = self
                    .dispatch_message_error_to_dlq(context, relay, assignments)
                    .await
                {
                    self.inner.events.report_error(format!(
                        "{} '{}' failed to dispatch message error {} to DLQ '{}' in domain '{}': \
                         {}",
                        node_kind.as_str(),
                        node.as_str(),
                        error.reference,
                        relay.as_str(),
                        domain.as_str(),
                        dispatch_error
                    ));
                    message.acks.no_ack(format!(
                        "{} '{}' failed to dispatch message error {} to DLQ '{}': {}",
                        node_kind.as_str(),
                        node.as_str(),
                        error.reference,
                        relay.as_str(),
                        dispatch_error
                    ));
                }
            }
        }
    }

    pub(in crate::runtime) fn handle_general_error_for_acks<'a>(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: impl Into<ModelName>,
        policies: &ErrorPolicies,
        acks: impl IntoIterator<Item = &'a AckSet>,
        reason: String,
    ) {
        let node = node.into();
        match policies.general {
            GeneralErrorPolicy::Ignore => {
                for ack in acks {
                    ack.ack_success();
                }
            }
            GeneralErrorPolicy::Log => {
                self.inner.events.report_error(format!(
                    "{} '{}' general error in domain '{}': {}",
                    node_kind.as_str(),
                    node.as_str(),
                    domain.as_str(),
                    reason
                ));
                warn!(
                    domain = domain.as_str(),
                    node_kind = node_kind.as_str(),
                    node = node.as_str(),
                    reason = %reason,
                    "runtime node handled general error"
                );
                for ack in acks {
                    ack.no_ack(reason.clone());
                }
            }
        }
    }

    pub(in crate::runtime) fn handle_internal_processor_error_for_acks<'a>(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: impl Into<ModelName>,
        _policies: &ErrorPolicies,
        acks: impl IntoIterator<Item = &'a AckSet>,
        reason: String,
    ) {
        let node = node.into();
        self.inner.events.report_error(format!(
            "{} '{}' internal error in domain '{}': {}",
            node_kind.as_str(),
            node.as_str(),
            domain.as_str(),
            reason
        ));
        warn!(
            domain = domain.as_str(),
            node_kind = node_kind.as_str(),
            node = node.as_str(),
            reason = %reason,
            "runtime processor handled internal error"
        );
        for ack in acks {
            ack.no_ack(reason.clone());
        }
    }

    pub(in crate::runtime) async fn handle_planned_message_errors(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: impl Into<ModelName>,
        policies: &ErrorPolicies,
        errors: Vec<PlannedMessageError>,
    ) {
        let node = node.into();
        for error in errors {
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind,
                node: &node,
                source_route: None,
                policy: &policies.message,
                message: error.message,
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn handle_planned_message_errors_with_policy(
        &self,
        domain: &DomainName,
        node_kind: ModelKind,
        node: &ModelName,
        source_route: Option<&RelayName>,
        policy: &MessageErrorPolicy,
        errors: Vec<PlannedMessageError>,
    ) {
        for error in errors {
            self.handle_structured_message_error(MessageErrorHandling {
                domain,
                node_kind,
                node,
                source_route,
                policy,
                message: error.message,
                error: error.error,
                partial_output: error.partial_output,
                materialized_state: error.materialized_state,
                ingest_metadata: None,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn dispatch_message_error_to_dlq(
        &self,
        context: MessageErrorContext<'_>,
        relay: &RelayName,
        assignments: &[Assignment],
    ) -> Result<(), String> {
        let MessageErrorContext {
            domain,
            node_kind,
            node,
            source_route,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
        } = context;
        /// Everything the error route needs from the domain execution, resolved while the
        /// execution is borrowed so the delivery below runs without holding that borrow.
        struct MessageErrorRoutePlan {
            schema: Arc<CompiledSchema>,
            target: MessageErrorRouteTarget,
            branching: Vec<FieldName>,
            program: CompiledProgramWithMaterializedInterest,
            flush_policy: Option<RuntimeFlushPolicy>,
        }

        let MessageErrorRoutePlan {
            schema,
            target,
            branching,
            program,
            flush_policy,
        } = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(format!("domain '{}' is not instantiated", domain.as_str()));
            };
            let schema = execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                format!(
                    "DLQ relay '{}' schema is not instantiated in domain '{}'",
                    relay.as_str(),
                    domain.as_str()
                )
            })?;
            let registry = execution
                .relay_registries
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "DLQ relay '{}' is not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    )
                })?;
            let services = execution
                .relay_services
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "DLQ relay '{}' services are not instantiated in domain '{}'",
                        relay.as_str(),
                        domain.as_str()
                    )
                })?;
            let branching = execution
                .relay_branchings
                .get(relay)
                .cloned()
                .unwrap_or_default();
            let flush_policy = Self::message_error_flush_policy(
                &execution,
                domain,
                node_kind,
                node,
                source_route,
                relay,
                assignments,
            )?;
            let schemas = Self::message_error_compile_schemas(
                &execution,
                node_kind,
                node,
                source_route,
                relay,
                assignments,
            )?;
            let program = compile_message_error_set_program(
                domain,
                node,
                assignments,
                schema.clone(),
                schemas,
                RuntimeVmCompileContext {
                    available_materialized_streams: &execution.materialized_stream_specs,
                    available_lookups: &execution.lookups,
                    current_branching: &branching,
                    current_branch_schema: None,
                    current_branch_sensitivity: None,
                    udfs: Some(&execution.udfs),
                },
            )?;
            MessageErrorRoutePlan {
                schema,
                target: MessageErrorRouteTarget { registry, services },
                branching,
                program,
                flush_policy,
            }
        };
        let dlq_record = Self::execute_message_error_set_program(
            &program,
            message,
            error,
            partial_output,
            materialized_state,
            ingest_metadata,
            self.current_stream_expiration_time(domain)
                .ok()
                .flatten()
                .unwrap_or_else(current_timestamp),
        )
        .await?;
        let key = preserved_message_error_branch(&branching, &message.key, relay, error.reference)?;
        let batch = RelayRecordBatch::single(schema, key, dlq_record, AckSet::empty())?;
        if let Some(flush_policy) = flush_policy {
            self.enqueue_message_error_delivery(
                MessageErrorRouteKey {
                    domain: domain.clone(),
                    node: NodeRef::new(node_kind, node.clone()),
                    source_route: source_route.cloned(),
                    error_relay: relay.clone(),
                },
                target,
                flush_policy,
                MessageErrorDelivery {
                    batch,
                    source_acks: vec![message.acks.clone()],
                },
            )
            .await?;
        } else {
            self.ingest_stream_boundary_message(
                domain,
                relay,
                &target.registry,
                &target.services,
                &batch,
            )
            .await
            .map_err(|_| {
                format!(
                    "DLQ relay '{}' rejected message error from {} '{}'",
                    relay.as_str(),
                    node_kind.as_str(),
                    node.as_str()
                )
            })?;
            message.acks.ack_success();
        }
        Ok(())
    }

    pub(super) fn message_error_flush_policy(
        execution: &DomainExecution,
        domain: &DomainName,
        node_kind: ModelKind,
        node: &ModelName,
        source_route: Option<&RelayName>,
        error_relay: &RelayName,
        assignments: &[Assignment],
    ) -> Result<Option<RuntimeFlushPolicy>, String> {
        let scheduled = ModelKind::from_str(node_kind.as_str())
            .ok()
            .and_then(|kind| {
                execution
                    .schedule
                    .nodes
                    .get(&NodeRef::new(kind, node.clone()))
            })
            .ok_or_else(|| {
                format!(
                    "runtime model for {} '{}' is unavailable",
                    node_kind.as_str(),
                    node.as_str()
                )
            })?;
        let outputs = match scheduled.config.as_ref() {
            Model::Ingestor(model) => &model.output_routes,
            Model::Reingestor(model) => &model.output_routes,
            Model::Junction(model) => &model.output_routes,
            Model::Deduplicator(model) => &model.output_routes,
            Model::Reorderer(model) => &model.output_routes,
            Model::WindowProcessor(model) => &model.output_routes,
            Model::Generator(model) => &model.output_routes,
            Model::Inferencer(model) => &model.output_routes,
            Model::WasmProcessor(model) => &model.output_routes,
            Model::Correlator(model) => &model.output_routes,
            Model::Emitter(model) => {
                return Self::parse_runtime_node_flush_policy(
                    domain,
                    node_kind.as_str(),
                    node,
                    &model.flush_policy,
                )
                .map(Some)
                .map_err(|error| error.to_string());
            }
            other => {
                return Err(format!(
                    "{} '{}' cannot own a message-error route",
                    other.kind().as_str(),
                    node.as_str()
                ));
            }
        };
        let output = matching_message_error_output(outputs, source_route, error_relay, assignments)
            .ok_or_else(|| {
                format!(
                    "{} '{}' message-error output route is unavailable",
                    node_kind.as_str(),
                    node.as_str()
                )
            })?;
        let Some(policy) = output.flush_policy.as_ref() else {
            return Ok(None);
        };
        Self::parse_runtime_node_flush_policy(domain, node_kind.as_str(), node, policy)
            .map(Some)
            .map_err(|error| error.to_string())
    }

    pub(super) fn message_error_compile_schemas(
        execution: &DomainExecution,
        node_kind: ModelKind,
        node: &ModelName,
        source_route: Option<&RelayName>,
        error_relay: &RelayName,
        assignments: &[Assignment],
    ) -> Result<MessageErrorCompileSchemas, String> {
        let scheduled = ModelKind::from_str(node_kind.as_str())
            .ok()
            .and_then(|kind| {
                execution
                    .schedule
                    .nodes
                    .get(&NodeRef::new(kind, node.clone()))
            })
            .ok_or_else(|| {
                format!(
                    "runtime model for {} '{}' is unavailable",
                    node_kind.as_str(),
                    node.as_str()
                )
            })?;
        let relay_schema = |relay: &RelayName| {
            execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                format!(
                    "runtime schema for relay '{}' is unavailable",
                    relay.as_str()
                )
            })
        };
        let partial_output_schema = |outputs: &nervix_models::ProcessorOutputs| {
            matching_message_error_output(outputs, source_route, error_relay, assignments)
                .map(|output| relay_schema(&output.relay))
                .transpose()
        };
        let mut schemas = MessageErrorCompileSchemas {
            input: None,
            left: None,
            right: None,
            partial_output: None,
            current_branching: Vec::new(),
            allow_header_reads: false,
        };
        let mut current_branch_relay = None;
        match scheduled.config.as_ref() {
            Model::Ingestor(model) => {
                schemas.input = execution
                    .codecs
                    .get(&model.decode_using_codec)
                    .map(|codec| codec.schema())
                    .ok_or_else(|| {
                        format!(
                            "runtime codec '{}' is unavailable",
                            model.decode_using_codec.as_str()
                        )
                    })?
                    .into();
                schemas.allow_header_reads = ingest_source_supports_headers(&model.source);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reingestor(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("reingestor '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Junction(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("junction '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Deduplicator(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("deduplicator '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reorderer(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("reorderer '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::WindowProcessor(model) => {
                current_branch_relay = model.from.first().cloned();
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Generator(model) => {
                current_branch_relay = Some(model.materialized_relay.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Inferencer(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("inferencer '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::WasmProcessor(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!(
                        "WASM processor '{}' has no input relay",
                        model.name.as_str()
                    )
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Correlator(model) => {
                let left = model.left.first().ok_or_else(|| {
                    format!("correlator '{}' has no left relay", model.name.as_str())
                })?;
                let right = model.right.first().ok_or_else(|| {
                    format!("correlator '{}' has no right relay", model.name.as_str())
                })?;
                schemas.left = Some(relay_schema(left)?);
                schemas.right = Some(relay_schema(right)?);
                current_branch_relay = Some(left.clone());
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Emitter(model) => {
                let input = model.from.first().ok_or_else(|| {
                    format!("emitter '{}' has no input relay", model.name.as_str())
                })?;
                schemas.input = Some(relay_schema(input)?);
                current_branch_relay = Some(input.clone());
                schemas.partial_output = model
                    .encode_using_codec
                    .as_ref()
                    .map(|codec| {
                        execution
                            .codecs
                            .get(codec)
                            .map(|compiled| compiled.schema())
                            .ok_or_else(|| {
                                format!("runtime codec '{}' is unavailable", codec.as_str())
                            })
                    })
                    .transpose()?;
            }
            other => {
                return Err(format!(
                    "{} '{}' cannot own a message-error route",
                    other.kind().as_str(),
                    node.as_str()
                ));
            }
        }
        if let Some(relay) = current_branch_relay {
            schemas.current_branching = execution
                .relay_branchings
                .get(&relay)
                .cloned()
                .unwrap_or_default();
        }
        Ok(schemas)
    }

    pub(in crate::runtime) async fn execute_message_error_set_program(
        program: &CompiledProgramWithMaterializedInterest,
        message: &RelayMessage,
        error: &StructuredMessageError,
        partial_output: Option<&RuntimeRecordBatch>,
        materialized_state: &HashMap<String, RuntimeValue>,
        ingest_metadata: Option<&IngestFilterMapMetadata>,
        execution_now: Timestamp,
    ) -> Result<RuntimeRow, String> {
        let carrier = message.record.one_row_batch();
        let keys = vec![message.key.clone()];
        let namespace_batches = partial_output
            .map(|batch| vec![("partial_output", batch)])
            .unwrap_or_default();
        let mut side_inputs = materialized_state.clone();
        side_inputs.insert(
            "error.reference".to_string(),
            RuntimeValue::String(error.reference.to_string()),
        );
        side_inputs.insert(
            "error.code".to_string(),
            RuntimeValue::String(error.code.as_ref().to_string()),
        );
        side_inputs.insert(
            "error.message".to_string(),
            RuntimeValue::String(error.message.clone()),
        );
        side_inputs.insert(
            "error.operation".to_string(),
            RuntimeValue::String(error.operation.as_ref().to_string()),
        );
        if let Some(operation_index) = error.operation_index {
            side_inputs.insert(
                "error.operation_index".to_string(),
                RuntimeValue::U32(operation_index),
            );
        }
        side_inputs.insert(
            "error.fields".to_string(),
            RuntimeValue::Vec(
                error
                    .fields
                    .iter()
                    .map(|field| RuntimeValue::String(field.as_str().to_string()))
                    .collect(),
            ),
        );
        side_inputs.insert(
            "error.occurred_at".to_string(),
            RuntimeValue::Datetime(error.occurred_at.as_datetime().fixed_offset()),
        );
        let lookup_columns = compute_lookup_hash_map_columns(
            program,
            &FilterMapBatchInputs {
                carrier: &carrier,
                namespace_batches: &namespace_batches,
                keys: &keys,
                side_inputs: &side_inputs,
                ingest_metadata,
            },
            execution_now,
            None,
        )
        .await?;
        let uninitialized = VmUninitializedInput {
            fields: program
                .compiled
                .input_schema
                .fields()
                .iter()
                .filter(|field| field.name().starts_with("error_output."))
                .map(|field| field.name().clone())
                .collect(),
        };
        let batch = project_vm_input_batch(
            &program.compiled.input_schema,
            &VmInputProjectionSources {
                carrier: &carrier,
                namespace_batches: &namespace_batches,
                strict_namespaces: &["partial_output"],
                keys: &keys,
                side_inputs: &side_inputs,
                ingest_metadata,
                lookup_columns: &lookup_columns,
                uninitialized: Some(&uninitialized),
            },
            None,
        )?;
        let result = execute_program_with_selection_in_context(
            &program.compiled,
            &batch,
            &VmExecutionContext {
                now: execution_now,
                injector: Some(IngestHeaderFunctionInjector::from_metadata(
                    ingest_metadata,
                    batch.row_count(),
                )),
            },
        )
        .await
        .map_err(|error| format!("message-error SET execution failed: {error}"))?;
        if result.batch.row_count() != 1 {
            return Err(format!(
                "message-error SET produced {} rows for one error",
                result.batch.row_count()
            ));
        }
        if let Some(side_error) = result.batch.errors().row(0).first() {
            return Err(format!(
                "message-error SET failed with {}: {} at {}",
                side_error.code.as_str(),
                side_error.message,
                side_error.span
            ));
        }
        let output = vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &[0])?;
        RuntimeRow::new(Arc::new(output), 0, message.record.metadata().clone())
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{
        FieldPath, MessageErrorCode, MessageErrorOperation, ParseAsType, StructuredMessageError,
        Timestamp,
    };
    use sorted_vec::SortedSet;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{RuntimeValue, test_runtime_row},
    };
    #[tokio::test]
    async fn message_error_set_uses_vm_functions_and_captured_snapshots() {
        let source = test_runtime_row([("input_id".to_string(), RuntimeValue::U32(7))]);
        let message = RelayMessage {
            key: None,
            record: source,
            acks: AckSet::empty(),
        };
        let partial_output =
            test_runtime_row([("total".to_string(), RuntimeValue::I64(41))]).one_row_batch();
        let materialized_state = HashMap::from_iter([(
            "relay_state.profiles.plan".to_string(),
            RuntimeValue::String("pro".to_string()),
        )]);
        let reference = uuid::Uuid::now_v7();
        let occurred_at = Timestamp::now();
        let error = StructuredMessageError {
            reference,
            code: MessageErrorCode::Evaluation,
            message: "division failed".to_string(),
            operation: MessageErrorOperation::Set,
            operation_index: Some(2),
            fields: SortedSet::from_unsorted(vec![
                FieldPath::new("input.denominator"),
                FieldPath::new("output.total"),
            ]),
            occurred_at,
        };
        let input_schema = test_schema(&[("input_id", ParseAsType::U32)]);
        let partial_schema = test_schema(&[("total", ParseAsType::I64)]);
        let state_schema = test_schema(&[("plan", ParseAsType::String)]);
        let output_schema = test_optional_schema(&[
            OptionalTestField {
                name: "input_id",
                ty: ParseAsType::U32,
                optional: false,
            },
            OptionalTestField {
                name: "message_digest",
                ty: ParseAsType::String,
                optional: false,
            },
            OptionalTestField {
                name: "attempted",
                ty: ParseAsType::I64,
                optional: true,
            },
            OptionalTestField {
                name: "plan",
                ty: ParseAsType::String,
                optional: false,
            },
            OptionalTestField {
                name: "operation",
                ty: ParseAsType::String,
                optional: false,
            },
            OptionalTestField {
                name: "operation_index",
                ty: ParseAsType::U32,
                optional: true,
            },
        ]);
        let materialized_specs = HashMap::from_iter([(
            named("profiles"),
            RuntimeMaterializedRelaySpec::new(
                state_schema.arrow_schema(),
                VmSchemaSensitivity::default(),
                Vec::new(),
            ),
        )]);
        let assignments = construction(
            "SET input_id = input.input_id, message_digest = md5(error.message), attempted = \
             partial_output.total, plan = relay_state.profiles.plan, operation = error.operation, \
             operation_index = error.operation_index",
        )
        .assignments;
        let program = compile_message_error_set_program(
            &domain("default"),
            &named("calculate"),
            &assignments,
            output_schema,
            MessageErrorCompileSchemas {
                input: Some(input_schema),
                left: None,
                right: None,
                partial_output: Some(partial_schema),
                current_branching: Vec::new(),
                allow_header_reads: false,
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_specs,
                available_lookups: &HashMap::default(),
                current_branching: &[],
                current_branch_schema: None,
                current_branch_sensitivity: None,
                udfs: None,
            },
        )
        .expect("message-error SET should compile through the VM");
        let output = Runtime::execute_message_error_set_program(
            &program,
            &message,
            &error,
            Some(&partial_output),
            &materialized_state,
            None,
            occurred_at,
        )
        .await
        .expect("message-error SET should execute through the VM");

        assert_eq!(row_value(&output, "input_id"), Some(RuntimeValue::U32(7)));
        assert_eq!(row_value(&output, "attempted"), Some(RuntimeValue::I64(41)));
        assert_eq!(
            row_value(&output, "plan"),
            Some(RuntimeValue::String("pro".to_string()))
        );
        assert_eq!(
            row_value(&output, "operation"),
            Some(RuntimeValue::String("set".to_string()))
        );
        assert_eq!(
            row_value(&output, "operation_index"),
            Some(RuntimeValue::U32(2))
        );
        let Some(RuntimeValue::String(digest)) = row_value(&output, "message_digest") else {
            panic!("message digest should be a string");
        };
        assert_eq!(digest.len(), 32);
    }

    #[test]
    fn message_error_routes_preserve_branch_identity_without_reconstruction() {
        let incoming = string_branch_key("tenant", "acme");
        let relay = named("processing_errors");
        let reference = uuid::Uuid::now_v7();

        assert_eq!(
            preserved_message_error_branch(&[named("tenant")], &incoming, &relay, reference,)
                .expect("matching branched error route should preserve its key"),
            incoming
        );
        assert!(
            preserved_message_error_branch(&[], &incoming, &relay, reference)
                .expect_err("unbranched error relay must reject a branch")
                .contains("cannot receive branched message error")
        );
        assert!(
            preserved_message_error_branch(&[named("tenant")], &None, &relay, reference,)
                .expect_err("branched error relay must reject unbranched execution")
                .contains("cannot receive unbranched message error")
        );
    }
}
