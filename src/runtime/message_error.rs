use super::{vm_compile::RuntimeVmCompileError, *};

#[derive(Debug, thiserror::Error)]
pub(super) enum MessageErrorRecordConstructionError {
    #[error("failed to compute message-error lookup columns: {error}")]
    LookupColumns {
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
    #[error("failed to project the message-error VM input: {error}")]
    InputProjection {
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
    #[error("message-error SET execution failed: {source}")]
    Execution {
        #[source]
        source: nervix_vm::RuntimeError,
    },
    #[error("message-error SET produced {rows} rows for one error")]
    RowCount { rows: usize },
    #[error("message-error SET recorded {} at {span}", .code.as_str())]
    SideError {
        code: nervix_vm::ErrorCode,
        span: VmSpan,
    },
    #[error("failed to project the message-error output: {error}")]
    OutputProjection {
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
    #[error("failed to materialize the message-error output row: {error}")]
    OutputRow {
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
}

#[derive(Debug, Clone, Copy, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub(super) enum MessageErrorInput {
    Input,
    Left,
    Right,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum MessageErrorHandlingError {
    #[error(
        "unbranched DLQ relay '{}' cannot receive branched message error {reference}",
        .relay.as_str()
    )]
    BranchedErrorForUnbranchedRelay {
        relay: RelayName,
        reference: uuid::Uuid,
    },
    #[error(
        "branched DLQ relay '{}' cannot receive unbranched message error {reference}",
        .relay.as_str()
    )]
    UnbranchedErrorForBranchedRelay {
        relay: RelayName,
        reference: uuid::Uuid,
    },
    #[error("partial output row {row} is outside batch with {rows} rows")]
    PartialOutputRowOutOfBounds { row: usize, rows: usize },
    #[error("failed to construct a partial-output Arrow batch: {source}")]
    PartialOutputArrow {
        #[source]
        source: arrow_schema::ArrowError,
    },
    #[error("failed to construct a partial-output runtime batch: {error}")]
    PartialOutputRecord {
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
    #[error("domain '{}' is not instantiated", .domain.as_str())]
    DomainNotInstantiated { domain: DomainName },
    #[error(
        "DLQ relay '{}' schema is not instantiated in domain '{}'",
        .relay.as_str(),
        .domain.as_str()
    )]
    DlqSchemaNotInstantiated {
        domain: DomainName,
        relay: RelayName,
    },
    #[error(
        "DLQ relay '{}' is not instantiated in domain '{}'",
        .relay.as_str(),
        .domain.as_str()
    )]
    DlqRelayNotInstantiated {
        domain: DomainName,
        relay: RelayName,
    },
    #[error(
        "DLQ relay '{}' services are not instantiated in domain '{}'",
        .relay.as_str(),
        .domain.as_str()
    )]
    DlqServicesNotInstantiated {
        domain: DomainName,
        relay: RelayName,
    },
    #[error(
        "runtime model for {} '{}' is unavailable",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    RuntimeModelUnavailable { node: NodeRef },
    #[error(
        "{} '{}' cannot own a message-error route",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    InvalidRouteOwner { node: NodeRef },
    #[error(
        "{} '{}' message-error output route is unavailable",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    OutputRouteUnavailable {
        node: NodeRef,
        source_route: Option<RelayName>,
        error_relay: RelayName,
    },
    #[error("{source}")]
    FlushPolicy {
        node: NodeRef,
        #[source]
        source: RuntimeError,
    },
    #[error("runtime schema for relay '{}' is unavailable", .relay.as_str())]
    RelaySchemaUnavailable { relay: RelayName },
    #[error("runtime codec '{}' is unavailable", .codec.as_str())]
    CodecUnavailable { codec: CodecName },
    #[error(
        "{} '{}' has no {input} relay",
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    InputRelayUnavailable {
        node: NodeRef,
        input: MessageErrorInput,
    },
    #[error(
        "failed to compile the message-error route for {} '{}' in domain '{}' to relay '{}': {error}",
        .node.kind.as_str(),
        .node.identifier.as_str(),
        .domain.as_str(),
        .error_relay.as_str()
    )]
    ProgramCompilation {
        domain: DomainName,
        node: NodeRef,
        error_relay: RelayName,
        error: error_stack::Report<RuntimeVmCompileError>,
    },
    #[error(
        "failed to construct message-error record {reference} for {}: {source}",
        .operation.as_ref()
    )]
    RecordConstruction {
        reference: uuid::Uuid,
        code: MessageErrorCode,
        operation: MessageErrorOperation,
        fields: SortedSet<FieldPath>,
        #[source]
        source: MessageErrorRecordConstructionError,
    },
    #[error(
        "failed to build the message-error batch for {} '{}' to DLQ relay '{}': {reason}",
        .node.kind.as_str(),
        .node.identifier.as_str(),
        .relay.as_str()
    )]
    BatchConstruction {
        domain: DomainName,
        node: NodeRef,
        relay: RelayName,
        reason: String,
    },
    #[error(
        "DLQ relay '{}' rejected message error from {} '{}'",
        .relay.as_str(),
        .node.kind.as_str(),
        .node.identifier.as_str()
    )]
    RelayRejected {
        domain: DomainName,
        node: NodeRef,
        relay: RelayName,
    },
    #[error(
        "message-error route for {} '{}' to relay '{}' is stopped",
        .route.node.kind.as_str(),
        .route.node.identifier.as_str(),
        .route.error_relay.as_str()
    )]
    DeliveryStopped { route: MessageErrorRouteKey },
}

impl MessageErrorHandlingError {
    fn record_construction(
        error: &StructuredMessageError,
        source: MessageErrorRecordConstructionError,
    ) -> error_stack::Report<Self> {
        error_stack::Report::new(Self::RecordConstruction {
            reference: error.reference,
            code: error.code,
            operation: error.operation,
            fields: error.fields.clone(),
            source,
        })
    }
}

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
    pub(super) execution_now: Timestamp,
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
    pub(super) execution_now: Timestamp,
}

pub(super) struct MessageErrorFailure {
    pub(super) source_route: Option<RelayName>,
    pub(super) reason: String,
    pub(super) operation: MessageErrorOperation,
}

pub(super) struct MessageErrorSourceContext<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) node_kind: ModelKind,
    pub(super) node: &'a ModelName,
    pub(super) execution_now: Timestamp,
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
    pub(super) current_branching: ResolvedBranching,
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
    execution_now: Timestamp,
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
        occurred_at: execution_now,
    }
}

pub(super) fn planned_structured_message_error(
    message: RelayMessage,
    error: StructuredMessageError,
    partial_output: Option<RuntimeRecordBatch>,
    materialized_state: HashMap<String, RuntimeValue>,
    execution_now: Timestamp,
) -> PlannedMessageError {
    PlannedMessageError {
        message,
        error,
        partial_output,
        materialized_state,
        execution_now,
    }
}

pub(super) fn preserved_message_error_branch(
    target_branching: &ResolvedBranching,
    incoming: &Option<BranchKey>,
    relay: &RelayName,
    reference: uuid::Uuid,
) -> error_stack::Result<Option<BranchKey>, MessageErrorHandlingError> {
    match (target_branching.is_unbranched(), incoming.as_ref()) {
        (true, None) | (false, Some(_)) => Ok(incoming.clone()),
        (true, Some(_)) => Err(error_stack::Report::new(
            MessageErrorHandlingError::BranchedErrorForUnbranchedRelay {
                relay: relay.clone(),
                reference,
            },
        )),
        (false, None) => Err(error_stack::Report::new(
            MessageErrorHandlingError::UnbranchedErrorForBranchedRelay {
                relay: relay.clone(),
                reference,
            },
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
                error = %error,
                row, "failed to capture the partial output view for a message error"
            );
            None
        }
    }
}

pub(super) fn vm_partial_output_row_to_runtime_batch(
    batch: &VmTypedBatch,
    row: usize,
) -> error_stack::Result<RuntimeRecordBatch, MessageErrorHandlingError> {
    if row >= batch.row_count() {
        return Err(error_stack::Report::new(
            MessageErrorHandlingError::PartialOutputRowOutOfBounds {
                row,
                rows: batch.row_count(),
            },
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
    .map_err(|source| {
        error_stack::Report::new(MessageErrorHandlingError::PartialOutputArrow { source })
    })?;
    RuntimeRecordBatch::from_record_batch(schema, record_batch).map_err(|error| {
        error_stack::Report::new(MessageErrorHandlingError::PartialOutputRecord { error })
    })
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
            VmTypedArray::Binary(array) => array.is_null(row),
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
        context: MessageErrorSourceContext<'_>,
        policies: &ErrorPolicies,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorSourceContext {
            domain,
            node_kind,
            node,
            execution_now,
        } = context;
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
                execution_now,
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
            execution_now,
        })
        .await;
    }

    pub(in crate::runtime) async fn handle_message_error_with_policy(
        &self,
        context: MessageErrorSourceContext<'_>,
        policy: &MessageErrorPolicy,
        message: RelayMessage,
        failure: MessageErrorFailure,
    ) {
        let MessageErrorSourceContext {
            domain,
            node_kind,
            node,
            execution_now,
        } = context;
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
                execution_now,
                MessageErrorCode::External,
                reason,
                operation,
                None,
                std::iter::empty(),
            ),
            partial_output: None,
            materialized_state: HashMap::default(),
            ingest_metadata: None,
            execution_now,
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
            execution_now,
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
                    execution_now,
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
                execution_now: error.execution_now,
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
                execution_now: error.execution_now,
            })
            .await;
        }
    }

    pub(in crate::runtime) async fn dispatch_message_error_to_dlq(
        &self,
        context: MessageErrorContext<'_>,
        relay: &RelayName,
        assignments: &[Assignment],
    ) -> error_stack::Result<(), MessageErrorHandlingError> {
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
            execution_now,
        } = context;
        let owner = NodeRef::new(node_kind, node.clone());
        /// Everything the error route needs from the domain execution, resolved while the
        /// execution is borrowed so the delivery below runs without holding that borrow.
        struct MessageErrorRoutePlan {
            schema: Arc<CompiledSchema>,
            target: MessageErrorRouteTarget,
            branching: ResolvedBranching,
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
                return Err(error_stack::Report::new(
                    MessageErrorHandlingError::DomainNotInstantiated {
                        domain: domain.clone(),
                    },
                ));
            };
            let schema = execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                error_stack::Report::new(MessageErrorHandlingError::DlqSchemaNotInstantiated {
                    domain: domain.clone(),
                    relay: relay.clone(),
                })
            })?;
            let registry = execution
                .relay_registries
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    error_stack::Report::new(MessageErrorHandlingError::DlqRelayNotInstantiated {
                        domain: domain.clone(),
                        relay: relay.clone(),
                    })
                })?;
            let services = execution
                .relay_services
                .get(relay)
                .cloned()
                .ok_or_else(|| {
                    error_stack::Report::new(
                        MessageErrorHandlingError::DlqServicesNotInstantiated {
                            domain: domain.clone(),
                            relay: relay.clone(),
                        },
                    )
                })?;
            let branching = execution
                .relay_branchings
                .get(relay)
                .cloned()
                .assured("the validated message-error relay has branch routing");
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
                    udfs: Some(&execution.udfs),
                },
            )
            .map_err(|error| {
                error_stack::Report::new(MessageErrorHandlingError::ProgramCompilation {
                    domain: domain.clone(),
                    node: owner.clone(),
                    error_relay: relay.clone(),
                    error,
                })
            })?;
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
            execution_now,
        )
        .await?;
        let key = preserved_message_error_branch(&branching, &message.key, relay, error.reference)?;
        let batch = RelayRecordBatch::single(schema, key, dlq_record, AckSet::empty()).map_err(
            |reason| {
                error_stack::Report::new(MessageErrorHandlingError::BatchConstruction {
                    domain: domain.clone(),
                    node: owner.clone(),
                    relay: relay.clone(),
                    reason: reason.to_string(),
                })
            },
        )?;
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
                error_stack::Report::new(MessageErrorHandlingError::RelayRejected {
                    domain: domain.clone(),
                    node: owner,
                    relay: relay.clone(),
                })
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
    ) -> error_stack::Result<Option<RuntimeFlushPolicy>, MessageErrorHandlingError> {
        let owner = NodeRef::new(node_kind, node.clone());
        let scheduled = execution.schedule.nodes.get(&owner).ok_or_else(|| {
            error_stack::Report::new(MessageErrorHandlingError::RuntimeModelUnavailable {
                node: owner.clone(),
            })
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
                .map_err(|source| {
                    error_stack::Report::new(MessageErrorHandlingError::FlushPolicy {
                        node: owner,
                        source,
                    })
                });
            }
            other => {
                return Err(error_stack::Report::new(
                    MessageErrorHandlingError::InvalidRouteOwner {
                        node: NodeRef::new(other.kind(), node.clone()),
                    },
                ));
            }
        };
        let output = matching_message_error_output(outputs, source_route, error_relay, assignments)
            .ok_or_else(|| {
                error_stack::Report::new(MessageErrorHandlingError::OutputRouteUnavailable {
                    node: owner.clone(),
                    source_route: source_route.cloned(),
                    error_relay: error_relay.clone(),
                })
            })?;
        let Some(policy) = output.flush_policy.as_ref() else {
            return Ok(None);
        };
        Self::parse_runtime_node_flush_policy(domain, node_kind.as_str(), node, policy)
            .map(Some)
            .map_err(|source| {
                error_stack::Report::new(MessageErrorHandlingError::FlushPolicy {
                    node: owner,
                    source,
                })
            })
    }

    pub(super) fn message_error_compile_schemas(
        execution: &DomainExecution,
        node_kind: ModelKind,
        node: &ModelName,
        source_route: Option<&RelayName>,
        error_relay: &RelayName,
        assignments: &[Assignment],
    ) -> error_stack::Result<MessageErrorCompileSchemas, MessageErrorHandlingError> {
        let owner = NodeRef::new(node_kind, node.clone());
        let scheduled = execution.schedule.nodes.get(&owner).ok_or_else(|| {
            error_stack::Report::new(MessageErrorHandlingError::RuntimeModelUnavailable {
                node: owner.clone(),
            })
        })?;
        let relay_schema = |relay: &RelayName| -> error_stack::Result<
            Arc<CompiledSchema>,
            MessageErrorHandlingError,
        > {
            execution.relay_schemas.get(relay).cloned().ok_or_else(|| {
                error_stack::Report::new(MessageErrorHandlingError::RelaySchemaUnavailable {
                    relay: relay.clone(),
                })
            })
        };
        let partial_output_schema = |outputs: &nervix_models::ProcessorOutputs| {
            matching_message_error_output(outputs, source_route, error_relay, assignments)
                .map(|output| relay_schema(&output.relay))
                .transpose()
        };
        let required_input = |inputs: &nervix_models::ProcessorInputs,
                              input: MessageErrorInput|
         -> error_stack::Result<RelayName, MessageErrorHandlingError> {
            inputs.first().cloned().ok_or_else(|| {
                error_stack::Report::new(MessageErrorHandlingError::InputRelayUnavailable {
                    node: owner.clone(),
                    input,
                })
            })
        };
        let mut schemas = MessageErrorCompileSchemas {
            input: None,
            left: None,
            right: None,
            partial_output: None,
            current_branching: ResolvedBranching::unbranched(),
            allow_header_reads: false,
        };
        let mut current_branch_relay = None;
        match scheduled.config.as_ref() {
            Model::Ingestor(model) => {
                let Some(codec) = execution.codecs.get(&model.decode_using_codec) else {
                    return Err(error_stack::Report::new(
                        MessageErrorHandlingError::CodecUnavailable {
                            codec: model.decode_using_codec.clone(),
                        },
                    ));
                };
                schemas.input = codec.schema().into();
                schemas.allow_header_reads = model.source.reads_headers();
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reingestor(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Junction(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Deduplicator(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Reorderer(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
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
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::WasmProcessor(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Correlator(model) => {
                let left = required_input(&model.left, MessageErrorInput::Left)?;
                let right = required_input(&model.right, MessageErrorInput::Right)?;
                schemas.left = Some(relay_schema(&left)?);
                schemas.right = Some(relay_schema(&right)?);
                current_branch_relay = Some(left);
                schemas.partial_output = partial_output_schema(&model.output_routes)?;
            }
            Model::Emitter(model) => {
                let input = required_input(&model.from, MessageErrorInput::Input)?;
                schemas.input = Some(relay_schema(&input)?);
                current_branch_relay = Some(input);
                schemas.partial_output = model
                    .body
                    .codec()
                    .map(|codec| match execution.codecs.get(codec) {
                        Some(compiled) => Ok(compiled.schema()),
                        None => Err(error_stack::Report::new(
                            MessageErrorHandlingError::CodecUnavailable {
                                codec: codec.clone(),
                            },
                        )),
                    })
                    .transpose()?;
            }
            other => {
                return Err(error_stack::Report::new(
                    MessageErrorHandlingError::InvalidRouteOwner {
                        node: NodeRef::new(other.kind(), node.clone()),
                    },
                ));
            }
        }
        if let Some(relay) = current_branch_relay {
            schemas.current_branching = execution
                .relay_branchings
                .get(&relay)
                .cloned()
                .assured("a validated route's source relay has installed branch routing");
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
    ) -> error_stack::Result<RuntimeRow, MessageErrorHandlingError> {
        let carrier = message.record.one_row_batch();
        let keys = vec![message.key.clone()];
        let namespace_batches = match partial_output {
            Some(batch) => vec![("partial_output", batch)],
            None => Vec::new(),
        };
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
        .await
        .map_err(|lookup_error| {
            MessageErrorHandlingError::record_construction(
                error,
                MessageErrorRecordConstructionError::LookupColumns {
                    error: lookup_error,
                },
            )
        })?;
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
        )
        .map_err(|projection_error| {
            MessageErrorHandlingError::record_construction(
                error,
                MessageErrorRecordConstructionError::InputProjection {
                    error: projection_error,
                },
            )
        })?;
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
        .map_err(|source| {
            MessageErrorHandlingError::record_construction(
                error,
                MessageErrorRecordConstructionError::Execution { source },
            )
        })?;
        if result.batch.row_count() != 1 {
            return Err(MessageErrorHandlingError::record_construction(
                error,
                MessageErrorRecordConstructionError::RowCount {
                    rows: result.batch.row_count(),
                },
            ));
        }
        if let Some(side_error) = result.batch.errors().row(0).first() {
            return Err(MessageErrorHandlingError::record_construction(
                error,
                MessageErrorRecordConstructionError::SideError {
                    code: side_error.code(),
                    span: side_error.span,
                },
            ));
        }
        let output = vm_typed_batch_selected_rows_to_runtime_batch(&result.batch, &[0]).map_err(
            |projection_error| {
                MessageErrorHandlingError::record_construction(
                    error,
                    MessageErrorRecordConstructionError::OutputProjection {
                        error: projection_error,
                    },
                )
            },
        )?;
        RuntimeRow::new(Arc::new(output), 0, message.record.metadata().clone()).map_err(
            |output_error| {
                MessageErrorHandlingError::record_construction(
                    error,
                    MessageErrorRecordConstructionError::OutputRow {
                        error: output_error,
                    },
                )
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use nervix_models::{
        FieldPath, MessageErrorCode, MessageErrorOperation, ParseAsType, Statement,
        StructuredMessageError, Timestamp,
    };
    use sorted_vec::SortedSet;

    use super::*;
    use crate::{
        runtime_ack::AckSet,
        runtime_schema::{RuntimeValue, test_runtime_row},
    };

    fn parsed_model(source: &str) -> Model {
        let statement = nervix_nspl::server_statement::parse_server_statement(source)
            .expect("test model should parse");
        let Statement::Create(create) = statement else {
            panic!("test source should create a runtime model");
        };
        create
            .body
            .try_map_resource_versions(|resource, requested| match requested {
                nervix_models::RequestedResourceVersion::Number(version) => Ok(version),
                nervix_models::RequestedResourceVersion::Latest => Err(resource.clone()),
            })
            .expect("test sources name explicit resource versions")
    }

    fn install_message_error_test_execution(
        runtime: &Runtime,
        domain: &DomainName,
        model: Option<Model>,
        routing: DomainRoutingSnapshot,
    ) {
        let nodes = model.into_iter().map(scheduled_model).collect::<Vec<_>>();
        install_test_domain_execution(runtime, domain, nodes, routing);
    }

    #[test]
    fn partial_output_capture_rejects_an_out_of_bounds_row() {
        let schema = test_schema(&[("result", ParseAsType::I64)]);
        let batch = vm_input_from_test_rows(
            &[test_runtime_row([(
                "result".to_string(),
                RuntimeValue::I64(7),
            )])],
            &schema.arrow_schema(),
        )
        .expect("test VM input should build");

        let error = vm_partial_output_row_to_runtime_batch(&batch, 1)
            .expect_err("a row outside the VM batch must fail");

        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::PartialOutputRowOutOfBounds { row: 1, rows: 1 }
        ));
    }

    #[test]
    fn message_error_route_planning_classifies_configuration_failures() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let node = named::<ModelName>("calculate");
        let output = named::<RelayName>("results");
        let error_relay = named::<RelayName>("route_errors");
        let assignments = Vec::new();

        install_message_error_test_execution(
            &runtime,
            &domain,
            None,
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_flush_policy(
            &execution,
            &domain,
            ModelKind::Junction,
            &node,
            Some(&output),
            &error_relay,
            &assignments,
        )
        .expect_err("an absent scheduled model must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::RuntimeModelUnavailable { .. }
        ));
        drop(execution);

        let relay = parsed_model("CREATE RELAY calculate SCHEMA error_schema UNBRANCHED;");
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(relay),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_flush_policy(
            &execution,
            &domain,
            ModelKind::Relay,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a relay cannot own an error route");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::InvalidRouteOwner { .. }
        ));
        drop(execution);

        let junction = parsed_model(
            "CREATE JUNCTION calculate FROM calculations UNBRANCHED TO results SET value = \
             input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        );
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(junction.clone()),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_flush_policy(
            &execution,
            &domain,
            ModelKind::Junction,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("an unmatched output route must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::OutputRouteUnavailable { .. }
        ));
        drop(execution);

        let mut invalid_flush_junction = junction;
        let Model::Junction(model) = &mut invalid_flush_junction else {
            panic!("test model should be a junction");
        };
        model.output_routes.routes[0].flush_policy = Some(nervix_models::FlushPolicy::Each {
            interval: "not-a-duration".to_string(),
            max_batch_size: "1MiB".to_string(),
        });
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(invalid_flush_junction),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_flush_policy(
            &execution,
            &domain,
            ModelKind::Junction,
            &node,
            Some(&output),
            &error_relay,
            &assignments,
        )
        .expect_err("an invalid route flush policy must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::FlushPolicy { .. }
        ));
    }

    #[test]
    fn message_error_schema_planning_classifies_missing_inputs() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let node = named::<ModelName>("calculate");
        let error_relay = named::<RelayName>("route_errors");
        let assignments = Vec::new();

        install_message_error_test_execution(
            &runtime,
            &domain,
            None,
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Junction,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("an absent scheduled model must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::RuntimeModelUnavailable { .. }
        ));
        drop(execution);

        let relay = parsed_model("CREATE RELAY calculate SCHEMA error_schema UNBRANCHED;");
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(relay),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Relay,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a relay cannot own an error route");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::InvalidRouteOwner { .. }
        ));
        drop(execution);

        let junction_source = "CREATE JUNCTION calculate FROM calculations UNBRANCHED TO results \
                               SET value = input.value FLUSH IMMEDIATE ON MESSAGE ERROR LOG;";
        let mut junction_without_input = parsed_model(junction_source);
        let Model::Junction(model) = &mut junction_without_input else {
            panic!("test model should be a junction");
        };
        model.from.from.clear();
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(junction_without_input),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Junction,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a junction without input must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::InputRelayUnavailable {
                input: MessageErrorInput::Input,
                ..
            }
        ));
        drop(execution);

        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(parsed_model(junction_source)),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Junction,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a missing input relay schema must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::RelaySchemaUnavailable { .. }
        ));
        drop(execution);

        let ingestor = parsed_model(
            "CREATE INGESTOR calculate FROM ENDPOINT ingress MODE NO_ACK SEQUENTIAL ON QUIESCE \
             BUFFER MAX SIZE 1MiB DECODE USING input_codec TO results INHERIT ALL UNBRANCHED \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        );
        install_message_error_test_execution(
            &runtime,
            &domain,
            Some(ingestor),
            DomainRoutingSnapshot::default(),
        );
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Ingestor,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a missing decoder must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::CodecUnavailable { .. }
        ));
        drop(execution);

        let emitter = parsed_model(
            "CREATE EMITTER calculate FROM calculations TO SYSLOG output_client MODE NO_ACK RETRY \
             POLICY BACKOFF 100ms MAX 1s ENCODE USING output_codec INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        );
        let mut routing = DomainRoutingSnapshot::default();
        routing.relay_schemas.insert(
            named("calculations"),
            test_schema(&[("value", ParseAsType::I64)]),
        );
        install_message_error_test_execution(&runtime, &domain, Some(emitter), routing);
        let execution = runtime
            .inner
            .executions
            .get(&domain)
            .expect("test execution should be installed");
        let error = Runtime::message_error_compile_schemas(
            &execution,
            ModelKind::Emitter,
            &node,
            None,
            &error_relay,
            &assignments,
        )
        .expect_err("a missing encoder must fail");
        assert!(matches!(
            error.current_context(),
            MessageErrorHandlingError::CodecUnavailable { .. }
        ));
    }

    #[tokio::test]
    async fn dlq_dispatch_classifies_missing_runtime_dependencies() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let node = named::<ModelName>("calculate");
        let error_relay = named::<RelayName>("route_errors");
        let message = RelayMessage {
            key: None,
            record: test_runtime_row([]),
            acks: AckSet::empty(),
        };
        let error = structured_message_error(
            Timestamp::now(),
            MessageErrorCode::Evaluation,
            "non-sensitive evaluation failure".to_string(),
            MessageErrorOperation::Set,
            None,
            [],
        );
        let materialized_state = HashMap::default();
        let assignments = Vec::new();
        let context = || MessageErrorContext {
            domain: &domain,
            node_kind: ModelKind::Junction,
            node: &node,
            source_route: None,
            message: &message,
            error: &error,
            partial_output: None,
            materialized_state: &materialized_state,
            ingest_metadata: None,
            execution_now: Timestamp::now(),
        };

        let failure = runtime
            .dispatch_message_error_to_dlq(context(), &error_relay, &assignments)
            .await
            .expect_err("a missing domain execution must fail");
        assert!(matches!(
            failure.current_context(),
            MessageErrorHandlingError::DomainNotInstantiated { .. }
        ));

        install_message_error_test_execution(
            &runtime,
            &domain,
            None,
            DomainRoutingSnapshot::default(),
        );
        let failure = runtime
            .dispatch_message_error_to_dlq(context(), &error_relay, &assignments)
            .await
            .expect_err("a missing DLQ schema must fail");
        assert!(matches!(
            failure.current_context(),
            MessageErrorHandlingError::DlqSchemaNotInstantiated { .. }
        ));

        let mut routing = DomainRoutingSnapshot::default();
        routing
            .relay_schemas
            .insert(error_relay.clone(), test_schema(&[]));
        install_message_error_test_execution(&runtime, &domain, None, routing);
        let failure = runtime
            .dispatch_message_error_to_dlq(context(), &error_relay, &assignments)
            .await
            .expect_err("a missing DLQ relay registry must fail");
        assert!(matches!(
            failure.current_context(),
            MessageErrorHandlingError::DlqRelayNotInstantiated { .. }
        ));

        let mut routing = DomainRoutingSnapshot::default();
        routing
            .relay_schemas
            .insert(error_relay.clone(), test_schema(&[]));
        routing
            .relay_registries
            .insert(error_relay.clone(), RelayRegistry::new());
        install_message_error_test_execution(&runtime, &domain, None, routing);
        let failure = runtime
            .dispatch_message_error_to_dlq(context(), &error_relay, &assignments)
            .await
            .expect_err("missing DLQ relay services must fail");
        assert!(matches!(
            failure.current_context(),
            MessageErrorHandlingError::DlqServicesNotInstantiated { .. }
        ));
    }

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
                ResolvedBranching::unbranched(),
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
                current_branching: ResolvedBranching::unbranched(),
                allow_header_reads: false,
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &materialized_specs,
                available_lookups: &HashMap::default(),
                current_branching: &ResolvedBranching::unbranched(),
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

    #[tokio::test]
    async fn message_error_record_construction_failure_is_distinct_from_the_original_error() {
        let output_schema = test_optional_schema(&[OptionalTestField {
            name: "result",
            ty: ParseAsType::I64,
            optional: false,
        }]);
        let assignments = construction("SET result = 1 / 0").assignments;
        let program = compile_message_error_set_program(
            &domain("default"),
            &named("calculate"),
            &assignments,
            output_schema,
            MessageErrorCompileSchemas {
                input: None,
                left: None,
                right: None,
                partial_output: None,
                current_branching: ResolvedBranching::unbranched(),
                allow_header_reads: false,
            },
            RuntimeVmCompileContext {
                available_materialized_streams: &HashMap::default(),
                available_lookups: &HashMap::default(),
                current_branching: &ResolvedBranching::unbranched(),
                udfs: None,
            },
        )
        .expect("message-error SET should compile");
        let reference = uuid::Uuid::now_v7();
        let fields = SortedSet::from_unsorted(vec![FieldPath::new("input.secret")]);
        let original_error = StructuredMessageError {
            reference,
            code: MessageErrorCode::External,
            message: "sensitive-payload-value".to_string(),
            operation: MessageErrorOperation::Publish,
            operation_index: None,
            fields: fields.clone(),
            occurred_at: Timestamp::now(),
        };
        let message = RelayMessage {
            key: None,
            record: test_runtime_row([]),
            acks: AckSet::empty(),
        };

        let failure = Runtime::execute_message_error_set_program(
            &program,
            &message,
            &original_error,
            None,
            &HashMap::default(),
            None,
            Timestamp::now(),
        )
        .await
        .expect_err("a failing error-route SET must stop record construction");

        let MessageErrorHandlingError::RecordConstruction {
            reference: failed_reference,
            code,
            operation,
            fields: failed_fields,
            source:
                MessageErrorRecordConstructionError::SideError {
                    code: side_code, ..
                },
        } = failure.current_context()
        else {
            panic!("record construction must have its own typed failure: {failure}");
        };
        assert_eq!(*failed_reference, reference);
        assert_eq!(*code, MessageErrorCode::External);
        assert_eq!(*operation, MessageErrorOperation::Publish);
        assert_eq!(failed_fields, &fields);
        assert_eq!(*side_code, nervix_vm::ErrorCode::DivisionByZero);
        assert!(!failure.to_string().contains("sensitive-payload-value"));
    }

    #[test]
    fn message_error_routes_preserve_branch_identity_without_reconstruction() {
        let incoming = string_branch_key("tenant", "acme");
        let relay = named("processing_errors");
        let reference = uuid::Uuid::now_v7();
        let branching = test_branching(&[("tenant", ParseAsType::String)]);
        let unbranched = ResolvedBranching::unbranched();

        assert_eq!(
            preserved_message_error_branch(&branching, &incoming, &relay, reference,)
                .expect("matching branched error route should preserve its key"),
            incoming
        );
        assert!(
            preserved_message_error_branch(&unbranched, &incoming, &relay, reference)
                .expect_err("unbranched error relay must reject a branch")
                .to_string()
                .contains("cannot receive branched message error")
        );
        assert!(
            preserved_message_error_branch(&branching, &None, &relay, reference,)
                .expect_err("branched error relay must reject unbranched execution")
                .to_string()
                .contains("cannot receive unbranched message error")
        );
    }
}
