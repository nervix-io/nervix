use super::*;

pub(super) async fn flush_branch_wasm_processor(
    context: WasmFlushContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
    ack_map: &mut WasmAckMap,
    next_ack_token: &mut u64,
    pending: &mut Vec<RelayRecordBatch>,
) {
    let WasmFlushContext {
        graph,
        branch,
        node_kind,
        processor,
        error_policies,
        input_relays,
        output_routes,
        resource,
        resource_version,
        file,
        limits,
        replicated_state,
    } = context;
    if pending.is_empty() {
        return;
    }
    let grouped_batches = std::mem::take(pending);
    let forwarded = match RelayRecordBatch::concat(grouped_batches.clone()) {
        Ok(forwarded) => forwarded,
        Err(error) => {
            for batch in grouped_batches {
                branch.runtime.handle_internal_processor_error_for_acks(
                    &branch.domain,
                    node_kind,
                    processor,
                    error_policies,
                    batch.acks.iter(),
                    format!(
                        "wasm processor '{}' failed to concat arrow batches: {}",
                        processor.as_str(),
                        error
                    ),
                );
            }
            return;
        }
    };

    if output_routes.routes.is_empty() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' has no output destinations",
                processor.as_str()
            ),
        );
        return;
    }
    let Some(primary_input_relay) = input_relays.first() else {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' has no input relays",
                processor.as_str()
            ),
        );
        return;
    };
    let input_schema =
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, primary_input_relay) {
            Ok(schema) => schema,
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
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match relay_schema_for_runtime(&branch.runtime, &branch.domain, &output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
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
        }
    }

    if let Err(error) = ensure_wasm_processor_instance(
        WasmInstanceContext {
            branch,
            processor,
            resource,
            resource_version,
            file,
            limits,
            guest_input_relay: primary_input_relay,
            input_schema: &input_schema,
            output_schemas: &output_schemas,
            replicated_state,
        },
        compiled,
        instance,
    )
    .await
    {
        branch.runtime.handle_general_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            error,
        );
        return;
    }

    if instance.is_none() {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!(
                "wasm processor '{}' instance is unavailable",
                processor.as_str()
            ),
        );
        return;
    }

    let (envelope, input_ack_map) =
        match wasm_envelope_from_relay_batch(branch.runtime.executor(), &forwarded, next_ack_token)
            .await
        {
            Ok(envelope) => envelope,
            Err(error) => {
                branch.runtime.handle_general_error_for_acks(
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
    ack_map.extend(input_ack_map);
    let process_result = instance
        .as_mut()
        .verified("the let-else above returned unless this branch holds an instance")
        .process_envelope(&envelope)
        .await;
    let outputs = match process_result {
        Ok(outputs) => outputs,
        Err(error) => {
            let resource_limit_exceeded = error.is_resource_limit_exceeded();
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                format!(
                    "wasm processor '{}' failed to process batch: {}",
                    processor.as_str(),
                    error
                ),
            );
            ack_map.clear();
            if resource_limit_exceeded {
                *instance = None;
            }
            return;
        }
    };

    let output_branch_key = branch.key.clone();
    if let Err(error) = dispatch_wasm_output_envelopes(
        WasmOutputContext {
            graph,
            branch,
            node_kind,
            processor,
            error_policies,
            output_routes,
            input_relays,
            input_schema: &input_schema,
            output_schemas: &output_schemas,
            key: &output_branch_key,
            dispatch_error: "failed to forward message",
        },
        outputs,
        ack_map,
    )
    .await
    {
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
    let persist_result =
        persist_wasm_guest_state(&branch.runtime, processor, replicated_state, instance).await;
    if let Err(error) = persist_result {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            std::iter::empty::<&AckSet>(),
            error,
        );
    }
}

pub(super) struct WasmInstanceContext<'a> {
    pub(super) branch: &'a BranchRuntime,
    pub(super) processor: &'a ModelName,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: Option<u64>,
    pub(super) file: &'a str,
    pub(super) limits: nervix_models::WasmProcessorLimits,
    pub(super) guest_input_relay: &'a RelayName,
    pub(super) input_schema: &'a Arc<CompiledSchema>,
    pub(super) output_schemas: &'a [(RelayName, Arc<CompiledSchema>)],
    pub(super) replicated_state: &'a ReplicatedWasmProcessorState,
}

impl Runtime {
    pub(super) async fn compile_wasm_processor_module(
        &self,
        domain: &DomainName,
        processor: impl Into<ModelName>,
        resource: &ResourceName,
        resource_version: Option<u64>,
        file: &str,
    ) -> Result<WasmCompiledBranchProcessor, String> {
        let processor = processor.into();
        let id = self.resolve_resource_id(domain, resource, resource_version, resource.as_str())?;
        let version = id.version;
        let Some(resource_store) = self.inner.resource_store.read().clone() else {
            return Err("resource store is not attached".to_string());
        };
        let path = resource_store
            .resolve_content_path(&id, file)
            .map_err(|error| error.to_string())?;
        let wasm = tokio::fs::read(&path).await.map_err(|error| {
            format!(
                "failed to read wasm processor '{}' resource '{}@{}' file '{}': {}",
                processor.as_str(),
                resource.as_str(),
                version,
                path.display(),
                error
            )
        })?;
        let compiled = self
            .inner
            .wasm_runtime
            .compile_processor(&wasm)
            .await
            .map_err(|error| {
                format!(
                    "failed to compile wasm processor '{}' resource '{}@{}' file '{}': {}",
                    processor.as_str(),
                    resource.as_str(),
                    version,
                    file,
                    error
                )
            })?;
        Ok(WasmCompiledBranchProcessor {
            version,
            compiled: Arc::new(compiled),
        })
    }
}

pub(super) async fn ensure_wasm_processor_instance(
    context: WasmInstanceContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<nervix_wasm::WasmBranchInstance>>,
) -> Result<(), String> {
    let WasmInstanceContext {
        branch,
        processor,
        resource,
        resource_version,
        file,
        limits,
        guest_input_relay,
        input_schema,
        output_schemas,
        replicated_state,
    } = context;
    let version = branch
        .runtime
        .resolve_resource_id(
            &branch.domain,
            resource,
            resource_version,
            resource.as_str(),
        )?
        .version;
    let needs_compile = compiled
        .as_ref()
        .is_none_or(|compiled| compiled.version != version);
    if needs_compile {
        *compiled = Some(
            branch
                .runtime
                .compile_wasm_processor_module(
                    &branch.domain,
                    processor,
                    resource,
                    Some(version),
                    file,
                )
                .await?,
        );
        *instance = None;
    }

    if instance.is_none() {
        let Some(compiled) = compiled.as_ref() else {
            return Err(format!(
                "wasm processor '{}' was not compiled",
                processor.as_str()
            ));
        };
        let init = WasmBranchInit {
            domain_name: branch.domain.as_str().to_string(),
            domain_type: "runtime".to_string(),
            branch_key: branch
                .key
                .as_ref()
                .map(|key| key.as_str().as_bytes().to_vec()),
            input_schema: input_schema
                .wasm_processor_schema(guest_input_relay.as_str().to_string()),
            output_schemas: output_schemas
                .iter()
                .map(|(relay, schema)| schema.wasm_processor_schema(relay.as_str().to_string()))
                .collect(),
        };
        let clock = RuntimeWasmDomainClock {
            runtime: branch.runtime.clone(),
            domain: branch.domain.clone(),
        };
        let restored_guest_state = replicated_state.restore_guest_state();
        *instance = Some(Box::new(
            compiled
                .compiled
                .instantiate_branch(
                    limits,
                    init,
                    Box::new(clock),
                    restored_guest_state.as_deref(),
                )
                .await
                .map_err(|error| {
                    format!(
                        "failed to instantiate wasm processor '{}' branch '{}': {}",
                        processor.as_str(),
                        branch_key_display(&branch.key),
                        error
                    )
                })?,
        ));
    }
    Ok(())
}

pub(super) async fn wasm_envelope_from_relay_batch(
    executor: &Executor,
    batch: &RelayRecordBatch,
    next_ack_token: &mut u64,
) -> Result<(WasmEnvelope, WasmAckMap), String> {
    let arrow_ipc_batch = batch
        .batch
        .encode_arrow_ipc(executor)
        .await
        .map_err(|error| error.to_string())?
        .to_vec();
    let row_count = batch.batch.batch().num_rows();
    if row_count != batch.acks.len() || row_count != batch.metadata.len() {
        return Err(format!(
            "wasm input row count {} does not match ack count {} and metadata count {}",
            row_count,
            batch.acks.len(),
            batch.metadata.len()
        ));
    }
    let mut rows = Vec::with_capacity(batch.acks.len());
    let mut ack_map = HashMap::with_capacity(batch.acks.len());
    let input_batch = Arc::new(batch.batch.clone());
    for (input_row, (metadata, acks)) in batch.metadata.iter().zip(batch.acks.iter()).enumerate() {
        let token = *next_ack_token;
        *next_ack_token = next_ack_token
            .checked_add(1)
            .assured("a branch instance cannot issue 2^64 ACK tokens");
        rows.push(WasmOutputRow {
            tokens: vec![WasmAckToken(token)],
            source_token: Some(WasmAckToken(token)),
        });
        ack_map.insert(
            token,
            WasmAckContext {
                acks: acks.clone(),
                metadata: metadata.clone(),
                input_batch: Arc::clone(&input_batch),
                input_row,
            },
        );
    }
    Ok((
        WasmEnvelope::input(
            arrow_ipc_batch,
            WasmAckSidecar {
                rows,
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        ),
        ack_map,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::{Array, Int32Array};
    use nervix_models::ParseAsType;
    use nervix_wasm::{WasmAckToken, WasmEnvelope, WasmOutputColumnRef};
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;
    use triomphe::Arc;

    use super::*;
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    #[tokio::test]
    async fn wasm_input_envelope_retains_one_shared_source_batch_and_source_tokens() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (envelope, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30]).await;
        let WasmEnvelope::Input {
            arrow_ipc_batch,
            acks,
        } = envelope
        else {
            panic!("host must construct an input envelope");
        };

        assert!(!arrow_ipc_batch.is_empty());
        assert_eq!(acks.rows.len(), 3);
        for (row, expected_token) in acks.rows.iter().zip(1_u64..) {
            assert_eq!(row.tokens, vec![WasmAckToken(expected_token)]);
            assert_eq!(row.source_token, Some(WasmAckToken(expected_token)));
        }
        let first = ack_map.get(&1).expect("first token must exist");
        for (input_row, token) in (1_u64..=3).enumerate() {
            let context = ack_map.get(&token).expect("token context must exist");
            assert!(Arc::ptr_eq(&first.input_batch, &context.input_batch));
            assert_eq!(context.input_row, input_row);
        }
    }

    #[tokio::test]
    async fn wasm_identity_input_reference_reuses_exact_source_array() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30]).await;
        let source = ack_map[&1].input_batch.batch().column(0).clone();
        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                wasm_input_acks(&input).rows.clone(),
            )],
        )
        .expect("identity reference must materialize");

        assert!(StdArc::ptr_eq(&source, outputs[0].batch.batch().column(0)));
    }

    #[tokio::test]
    async fn wasm_contiguous_input_reference_shares_source_buffers() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30, 40]).await;
        let rows = wasm_input_acks(&input).rows[1..3].to_vec();
        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows,
            )],
        )
        .expect("contiguous reference must materialize");
        let source_data = ack_map[&1].input_batch.batch().column(0).to_data();
        let output_data = outputs[0].batch.batch().column(0).to_data();

        // The offset pointer is only compared, never read, so `wrapping_add` is the defined way to
        // compute it without claiming the provenance that `add` requires.
        assert_eq!(
            output_data.buffers()[0].as_ptr(),
            source_data.buffers()[0]
                .as_ptr()
                .wrapping_add(std::mem::size_of::<i32>())
        );
        let values = outputs[0]
            .batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output must be I32");
        assert_eq!(values.values().as_ref(), &[20, 30]);
    }

    #[tokio::test]
    async fn wasm_general_input_selection_filters_reorders_and_duplicates_rows() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (input, ack_map) = wasm_input_for_values(&schema, &[10, 20, 30, 40]).await;
        let input_rows = wasm_input_acks(&input).rows.clone();
        let rows = vec![
            input_rows[3].clone(),
            input_rows[1].clone(),
            input_rows[1].clone(),
        ];
        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows,
            )],
        )
        .expect("general selection must materialize");
        let values = outputs[0]
            .batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output must be I32");

        assert_eq!(values.values().as_ref(), &[40, 20, 20]);
    }

    #[tokio::test]
    async fn wasm_input_references_materialize_rows_from_multiple_retained_batches() {
        let schema = test_schema(&[("value", ParseAsType::I32)]);
        let (first_input, mut ack_map) = wasm_input_for_values(&schema, &[10]).await;
        let (second_input, mut second_ack_map) = wasm_input_for_values(&schema, &[20]).await;
        let second_context = second_ack_map.remove(&1).expect("second token must exist");
        ack_map.insert(2, second_context);
        let mut rows = wasm_input_acks(&first_input).rows.clone();
        let mut second_row = wasm_input_acks(&second_input).rows.clone().remove(0);
        second_row.tokens = vec![WasmAckToken(2)];
        second_row.source_token = Some(WasmAckToken(2));
        rows.push(second_row);

        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                vec![WasmOutputColumnRef::Input { column_index: 0 }],
                rows,
            )],
        )
        .expect("live sources retained across batches must materialize");
        let values = outputs[0]
            .batch
            .batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("output must be I32");

        assert_eq!(values.values().as_ref(), &[10, 20]);
    }

    #[tokio::test]
    async fn wasm_identity_references_support_every_internal_arrow_field_kind() {
        let schema = test_schema(&[
            ("u8", ParseAsType::U8),
            ("i8", ParseAsType::I8),
            ("u16", ParseAsType::U16),
            ("i16", ParseAsType::I16),
            ("u32", ParseAsType::U32),
            ("i32", ParseAsType::I32),
            ("u64", ParseAsType::U64),
            ("i64", ParseAsType::I64),
            ("bool", ParseAsType::Bool),
            ("string", ParseAsType::String),
            ("datetime", ParseAsType::Datetime),
            ("f32", ParseAsType::F32),
            ("f64", ParseAsType::F64),
            (
                "array",
                ParseAsType::Array {
                    element: Box::new(ParseAsType::I32),
                    len: nonzero!(2u32),
                },
            ),
            (
                "vec",
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::String),
                },
            ),
        ]);
        let record = test_runtime_row([
            ("u8".to_string(), RuntimeValue::U8(1)),
            ("i8".to_string(), RuntimeValue::I8(-2)),
            ("u16".to_string(), RuntimeValue::U16(3)),
            ("i16".to_string(), RuntimeValue::I16(-4)),
            ("u32".to_string(), RuntimeValue::U32(5)),
            ("i32".to_string(), RuntimeValue::I32(-6)),
            ("u64".to_string(), RuntimeValue::U64(7)),
            ("i64".to_string(), RuntimeValue::I64(-8)),
            ("bool".to_string(), RuntimeValue::Bool(true)),
            (
                "string".to_string(),
                RuntimeValue::String("value".to_string()),
            ),
            (
                "datetime".to_string(),
                RuntimeValue::Datetime(
                    chrono::DateTime::parse_from_rfc3339("2026-07-13T12:34:56Z")
                        .expect("timestamp must parse"),
                ),
            ),
            ("f32".to_string(), RuntimeValue::F32(OrderedFloat(1.5))),
            ("f64".to_string(), RuntimeValue::F64(OrderedFloat(2.5))),
            (
                "array".to_string(),
                RuntimeValue::Array(vec![RuntimeValue::I32(9), RuntimeValue::I32(10)]),
            ),
            (
                "vec".to_string(),
                RuntimeValue::Vec(vec![
                    RuntimeValue::String("a".to_string()),
                    RuntimeValue::String("b".to_string()),
                ]),
            ),
        ]);
        let (input, ack_map) = wasm_input_for_records(&schema, vec![record]).await;
        let source_columns = ack_map[&1].input_batch.batch().columns().to_vec();
        let outputs = validate_wasm_test_outputs(
            &schema,
            &schema,
            &ack_map,
            vec![wasm_test_output(
                (0..schema.arrow_schema().fields().len())
                    .map(|column_index| WasmOutputColumnRef::Input {
                        column_index: u32::try_from(column_index)
                            .assured("the test schema has fewer than u32::MAX fields"),
                    })
                    .collect(),
                wasm_input_acks(&input).rows.clone(),
            )],
        )
        .expect("all internal field kinds must materialize");

        for (source, output) in source_columns
            .iter()
            .zip(outputs[0].batch.batch().columns())
        {
            assert!(StdArc::ptr_eq(source, output));
        }
    }
}
