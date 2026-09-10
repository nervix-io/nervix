use super::*;

pub(super) async fn flush_branch_inferencer_output(
    context: InferencerFlushContext<'_>,
    output_buffer: &mut InferencerOutputBuffer,
    output_index: usize,
) {
    let InferencerFlushContext {
        graph,
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
        input_relays,
        session,
        materialized_state,
        execution_now,
    } = context;
    output_routes.routes[output_index].clear_flush_deadline();
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
                format!(
                    "inferencer '{}' failed to concatenate buffered input batches for output: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };

    let version = match branch.runtime.resolve_resource_id(
        &branch.domain,
        resource,
        resource_version,
        resource.as_str(),
    ) {
        Ok(id) => id.version,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                error,
            );
            return;
        }
    };
    if session
        .as_ref()
        .is_none_or(|loaded| loaded.version() != version)
    {
        let Some(resource_store) = branch.runtime.inner.resource_store.read().clone() else {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                forwarded.acks.iter(),
                "resource store is not attached".to_string(),
            );
            return;
        };
        let resource_id = ResourceId::new(branch.domain.clone(), resource.clone(), version);
        let path = match resource_store.resolve_content_path(&resource_id, file) {
            Ok(path) => path,
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    error.to_string(),
                );
                return;
            }
        };
        match inferencer::OnnxInferencerSession::load(version, &path).await {
            Ok(loaded) => *session = Some(loaded),
            Err(error) => {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    forwarded.acks.iter(),
                    format!(
                        "inferencer '{}' failed to load resource '{}@{}' file '{}': {}",
                        processor.as_str(),
                        resource.as_str(),
                        version,
                        file,
                        error
                    ),
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
            let (error, batch) = *error_and_batch;
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                batch.acks.iter(),
                format!(
                    "inferencer '{}' failed to decode arrow batch: {}",
                    processor.as_str(),
                    error
                ),
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
            format!(
                "inferencer '{}' ONNX session was not loaded",
                processor.as_str()
            ),
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
                format!("inferencer '{}' INPUTS batch failed: {error}", processor),
            );
            return;
        }
    };
    let mapped_result = match execute_program_with_selection_in_context(
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
                format!(
                    "inferencer '{}' INPUTS execution failed: {error}",
                    processor
                ),
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
                format!(
                    "inferencer '{}' INPUTS produced {} rows for {} messages",
                    processor,
                    batch.batch().num_rows(),
                    messages.len()
                ),
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
                format!("inferencer '{}' INPUTS output failed: {error}", processor),
            );
            return;
        }
    };
    let output_columns = match session
        .execute(&mapped_batch, inputs, output_schema, execution_mode)
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
                format!(
                    "inferencer '{}' failed ONNX execution for resource '{}@{}' file '{}': {}",
                    processor.as_str(),
                    resource.as_str(),
                    version,
                    file,
                    error
                ),
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
            format!(
                "inferencer '{}' returned {} output columns for {} declarations or a column with \
                 an invalid row count for {} input messages",
                processor.as_str(),
                output_columns.len(),
                output_schema.len(),
                messages.len()
            ),
        );
        return;
    }
    let inferencer_tensors = InferencerFilterMapTensors { output_schema };
    let tensor_schema = inferencer_tensors.output_arrow_schema();
    let tensor_batch_result = (|| {
        let columns = tensor_schema
            .fields()
            .iter()
            .map(|field| {
                let column_index = tensor_schema
                    .index_of(field.name())
                    .map_err(|error| error.to_string())?;
                runtime_values_input_column(
                    output_columns[column_index].iter().map(Some),
                    messages.len(),
                    field,
                )
                .map(|column| column.to_array_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let batch = RecordBatch::try_new(tensor_schema.clone(), columns)
            .map_err(|error| error.to_string())?;
        RuntimeRecordBatch::from_record_batch(tensor_schema, batch)
    })();
    let tensor_batch = match tensor_batch_result {
        Ok(batch) => batch,
        Err(error) => {
            branch.runtime.handle_internal_processor_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                messages.iter().map(|message| &message.acks),
                format!(
                    "inferencer '{}' failed to build output tensor columns: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    let output_metadata = messages
        .iter()
        .map(|message| message.record.metadata().clone())
        .collect::<Vec<_>>();
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
                format!(
                    "inferencer '{}' failed to build output batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            return;
        }
    };
    if let Some(acks) = dispatch_processor_output(
        ProcessorOutputDispatchContext {
            graph,
            branch,
            node_kind,
            source_kind: ModelKind::Inferencer,
            processor,
            error_policies,
            input_relays,
            filter_source: ProcessorOutputFilterSource::Inferencer(inferencer_tensors),
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
