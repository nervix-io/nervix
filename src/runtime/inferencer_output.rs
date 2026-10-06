use error_stack::ResultExt as _;

use super::*;

/// Every way an inferencer fails to run the input one output route buffered through its model.
/// The inferencer names itself when it reports the failure, and the acknowledgements of the input
/// fail with it.
#[derive(Debug, thiserror::Error)]
pub(super) enum InferencerOutputError {
    #[error("failed to concatenate the buffered input batches")]
    ConcatenateInput,
    #[error("the resource store is not attached")]
    ResourceStoreDetached,
    #[error("failed to resolve resource '{resource}@{version}' file '{file}'")]
    ResolveFile {
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error("failed to load resource '{resource}@{version}' file '{file}'")]
    LoadModel {
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error("failed to decode the input batch into messages")]
    DecodeInput,
    #[error("the ONNX session was not loaded")]
    SessionUnavailable,
    #[error("failed to build the INPUTS batch")]
    InputsBatch,
    #[error("INPUTS execution failed")]
    InputsExecution,
    #[error("INPUTS produced {rows} rows for {messages} messages")]
    InputsRowCount { rows: usize, messages: usize },
    #[error("failed to read the INPUTS output")]
    InputsOutput,
    #[error("ONNX execution failed for resource '{resource}@{version}' file '{file}'")]
    Execute {
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error(
        "returned {columns} output columns for {declarations} declarations or a column with an \
         invalid row count for {messages} input messages"
    )]
    OutputColumns {
        columns: usize,
        declarations: usize,
        messages: usize,
    },
    #[error("failed to build output tensor column '{field}'")]
    OutputTensorColumn { field: String },
    #[error("failed to build the output tensor batch")]
    OutputTensorBatch,
    #[error("failed to build the output batch")]
    OutputBatch,
}

pub(super) async fn flush_branch_inferencer_output(
    context: InferencerFlushContext<'_>,
    output_buffer: &mut InferencerOutputBuffer,
    output_index: usize,
) {
    let InferencerFlushContext {
        branch,
        node_kind,
        processor,
        error_policies,
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        output_schema,
        compiled_input_program,
        session,
        materialized_state,
        execution_now,
    } = context;
    output_routes.routes[output_index].clear_flush_timer();
    let pending = output_buffer.take_pending();
    if pending.is_empty() {
        return;
    }
    let pending_acks = pending
        .iter()
        .flat_map(|batch| batch.acks.iter().cloned())
        .collect::<Vec<_>>();
    let forwarded = match RelayRecordBatch::concat(pending) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                pending_acks.iter(),
                &error.change_context(InferencerOutputError::ConcatenateInput),
            );
            return;
        }
    };

    // The session belongs to this branch instance and is loaded once, from the resource version
    // the inferencer pins.
    if session.is_none() {
        let Some(resource_store) = branch.runtime.inner.resource_store.load_full() else {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                &Report::new(InferencerOutputError::ResourceStoreDetached),
            );
            return;
        };
        let resource_id =
            ResourceId::new(branch.domain.clone(), resource.clone(), resource_version);
        let path = match resource_store.resolve_content_path(&resource_id, file) {
            Ok(path) => path,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    &error.change_context(InferencerOutputError::ResolveFile {
                        resource: resource.clone(),
                        version: resource_version,
                        file: file.to_owned(),
                    }),
                );
                return;
            }
        };
        match inferencer::OnnxInferencerSession::load(branch.runtime.executor(), &path).await {
            Ok(loaded) => *session = Some(loaded),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    &error.change_context(InferencerOutputError::LoadModel {
                        resource: resource.clone(),
                        version: resource_version,
                        file: file.to_owned(),
                    }),
                );
                return;
            }
        }
    }

    let input_key = forwarded.key.clone();
    let input_keys = forwarded.keys.clone();
    let input_batch = forwarded.batch.clone();
    let messages = match forwarded.try_into_messages() {
        Ok(messages) => messages,
        Err(error_and_batch) => {
            let failure = *error_and_batch;
            let batch = failure.preserved;
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                batch.acks.iter(),
                &failure
                    .error
                    .change_context(InferencerOutputError::DecodeInput),
            );
            return;
        }
    };

    let declared_schema = match inputs.first() {
        Some(mapping) => Some(&mapping.schema),
        None => output_schema.first().map(|declaration| &declaration.schema),
    };
    let execution_mode = match declared_schema {
        Some(schema) if schema.batch_axis().is_some() => InferencerExecutionMode::Batched,
        _ => InferencerExecutionMode::PerMessage,
    };
    let Some(session) = session.as_ref() else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            messages.iter().map(|message| &message.acks),
            &Report::new(InferencerOutputError::SessionUnavailable),
        );
        return;
    };
    let side_inputs = HashMap::default();
    let lookup_columns = HashMap::default();
    let mapped_vm_input = match project_vm_input_batch(
        &compiled_input_program.program.input_schema,
        &VmInputProjectionSources {
            carrier: &input_batch,
            namespace_batches: &[],
            strict_namespaces: &[],
            keys: &input_keys,
            side_inputs: &side_inputs,
            ingest_metadata: None,
            lookup_columns: &lookup_columns,
            uninitialized: None,
        },
        None,
    ) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error.change_context(InferencerOutputError::InputsBatch),
            );
            return;
        }
    };
    let mapped_result = match execute_program_with_selection_in_context(
        branch.runtime.executor(),
        &compiled_input_program.program,
        &mapped_vm_input,
        &VmExecutionContext {
            now: execution_now,
            injector: None,
        },
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error.change_context(InferencerOutputError::InputsExecution),
            );
            return;
        }
    };
    let mapped_batch = match vm_typed_batch_to_runtime_batch(&mapped_result.batch) {
        Ok(batch) if batch.batch().num_rows() == messages.len() => batch,
        Ok(batch) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &Report::new(InferencerOutputError::InputsRowCount {
                    rows: batch.batch().num_rows(),
                    messages: messages.len(),
                }),
            );
            return;
        }
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error.change_context(InferencerOutputError::InputsOutput),
            );
            return;
        }
    };
    let output_columns = match session
        .execute(
            branch.runtime.executor(),
            &mapped_batch,
            inputs,
            output_schema,
            execution_mode,
        )
        .await
    {
        Ok(output_fields) => output_fields,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error.change_context(InferencerOutputError::Execute {
                    resource: resource.clone(),
                    version: resource_version,
                    file: file.to_owned(),
                }),
            );
            return;
        }
    };
    if output_columns.len() != output_schema.len()
        || output_columns
            .iter()
            .any(|column| column.len() != messages.len())
    {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            messages.iter().map(|message| &message.acks),
            &Report::new(InferencerOutputError::OutputColumns {
                columns: output_columns.len(),
                declarations: output_schema.len(),
                messages: messages.len(),
            }),
        );
        return;
    }
    let inferencer_tensors = InferencerFilterMapTensors { output_schema };
    let tensor_batch_result = inferencer_tensors.output_batch(&output_columns, messages.len());
    let tensor_batch = match tensor_batch_result {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error,
            );
            return;
        }
    };
    let output_metadata = RecordMetadataColumns::from_rows(
        messages
            .iter()
            .map(|message| message.record.metadata().clone()),
    );
    let output_acks = messages
        .iter()
        .map(|message| message.acks.clone())
        .collect::<Vec<_>>();
    let output_batch = match RelayRecordBatch::from_filtered_parts(
        input_key,
        tensor_batch,
        output_metadata,
        output_acks,
    ) {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                &error.change_context(InferencerOutputError::OutputBatch),
            );
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            branch,
            node_kind,
            source_kind: ModelKind::Inferencer,
            processor,
            error_policies,
            materialized_state: ProcessorMaterializedState::ResolvedAtDispatch(materialized_state),
            execution_now,
        },
        output_routes,
        output_batch,
        output_index,
    )
    .await
    {
        for ack in acks {
            ack.ack_success();
        }
    }
}

impl InferencerFilterMapTensors<'_> {
    /// The columns the model returned, as one batch of the output tensor schema with one row for
    /// every input message.
    fn output_batch(
        &self,
        output_columns: &[Vec<RuntimeValue>],
        rows: usize,
    ) -> error_stack::Result<RuntimeRecordBatch, InferencerOutputError> {
        let tensor_schema = self.output_arrow_schema();
        let mut columns = Vec::with_capacity(tensor_schema.fields().len());
        for field in tensor_schema.fields() {
            let column_failure = || InferencerOutputError::OutputTensorColumn {
                field: field.name().clone(),
            };
            let column_index = tensor_schema
                .index_of(field.name())
                .change_context_lazy(column_failure)?;
            let values = output_columns[column_index].iter().map(Some);
            let column = runtime_values_input_column(values, rows, field)
                .change_context_lazy(column_failure)?;
            columns.push(column.to_array_ref());
        }
        let batch = RecordBatch::try_new(tensor_schema.clone(), columns)
            .change_context(InferencerOutputError::OutputTensorBatch)?;
        RuntimeRecordBatch::from_record_batch(tensor_schema, batch)
            .change_context(InferencerOutputError::OutputTensorBatch)
    }
}
