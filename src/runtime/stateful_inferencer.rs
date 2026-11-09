//! Admission-time execution of one branch's explicitly stateful inferencer.
//!
//! Layer: data plane.
//! Owns: ordered one-row mapping, model-error disposition and one fan-out of each model result.
//! Depends on: prepared native state, compiled VM programs and processor output dispatch.
//! Must not know: Models, parsing, placement or durable state formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "executes a branch-owned inference plan")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "admits rows against compiled programs and prepared native tensor plans"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "processes admitted rows in their branch task's arrival order"
    )
)]

use error_stack::Report;

use super::*;

#[derive(Debug, thiserror::Error)]
enum StatefulAdmissionError {
    #[error("the inference resource store is detached")]
    ResourceStore,
    #[error("failed to resolve the pinned inference model")]
    ModelFile,
    #[error("failed to address the admitted input row")]
    InputRow,
    #[error("failed to project the INPUTS VM batch")]
    InputProjection,
    #[error("INPUTS execution was not admitted")]
    InputExecution,
    #[error("failed to materialize the mapped Arrow tensor batch")]
    MappedInput,
}

pub(super) struct StatefulAdmissionContext<'a> {
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: u64,
    pub(super) file: &'a str,
    pub(super) program: &'a CompiledProgramWithMaterializedInterest,
    pub(super) prepared: &'a Arc<inferencer::PreparedStatefulInference>,
    pub(super) session: &'a mut inferencer::StatefulInferenceSession,
    pub(super) replicated_state: &'a Arc<ReplicatedInferencerState>,
    pub(super) incarnation: u64,
    pub(super) snapshot_bytes: u64,
    pub(super) materialized_values: &'a HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
}
impl StatefulAdmissionContext<'_> {
    fn general<C: error_stack::Context>(&self, acks: &AckSet, error: &Report<C>) {
        self.branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &self.branch.domain,
                ModelKind::Inferencer,
                self.processor,
                self.error_policies,
                std::iter::once(acks),
                error,
            );
    }
    async fn rejected(&mut self, message: RelayMessage, error: StructuredMessageError) {
        let count = self.output_routes.routes.len();
        if count == 0 {
            message.acks.ack_success();
            return;
        }
        let mut messages = Vec::with_capacity(count);
        for _ in 1..count {
            messages.push(RelayMessage {
                key: message.key.clone(),
                record: message.record.clone(),
                acks: message.acks.attached(),
            });
        }
        messages.push(message);
        for (output, message) in self.output_routes.routes.iter().zip(messages) {
            nervix_primitives::task::consume_budget().await;
            self.branch
                .runtime
                .handle_structured_message_error(MessageErrorHandling {
                    routing: self.branch.routing_snapshot.as_deref(),
                    domain: &self.branch.domain,
                    node_kind: ModelKind::Inferencer,
                    node: self.processor,
                    source_route: Some(&output.relay),
                    policy: &output.message_error_policy,
                    message,
                    error: error.clone(),
                    partial_output: None,
                    materialized_state: self.materialized_values.clone(),
                    ingest_metadata: None,
                    execution_now: self.execution_now,
                })
                .await;
        }
    }
}

/// The branch task awaits each row, so neither another row nor a route timer can invoke its model.
/// Only this operation owns the allocation for its admission future.
pub(super) fn admit_stateful_inferencer<'a>(
    context: StatefulAdmissionContext<'a>,
    batch: RelayRecordBatch,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(admit_stateful_inferencer_rows(context, batch))
}

async fn admit_stateful_inferencer_rows(
    mut context: StatefulAdmissionContext<'_>,
    mut batch: RelayRecordBatch,
) {
    let Some(store) = context.branch.runtime.inner.resource_store.load_full() else {
        context
            .branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &context.branch.domain,
                ModelKind::Inferencer,
                context.processor,
                context.error_policies,
                batch.acks.iter(),
                &Report::new(StatefulAdmissionError::ResourceStore),
            );
        return;
    };
    let id = ResourceId::new(
        context.branch.domain.clone(),
        context.resource.clone(),
        context.resource_version,
    );
    let path = match store.resolve_content_path(&id, context.file) {
        Ok(path) => path,
        Err(error) => {
            context
                .branch
                .runtime
                .handle_internal_processor_error_for_acks(
                    &context.branch.domain,
                    ModelKind::Inferencer,
                    context.processor,
                    context.error_policies,
                    batch.acks.iter(),
                    &error.change_context(StatefulAdmissionError::ModelFile),
                );
            return;
        }
    };
    let mut changed = false;
    let acknowledgements = std::mem::take(&mut batch.acks);
    for (row, acks) in acknowledgements.into_iter().enumerate() {
        nervix_primitives::task::consume_budget().await;
        let record = match batch.runtime_row(row) {
            Ok(record) => record,
            Err(error) => {
                context.general(
                    &acks,
                    &error.change_context(StatefulAdmissionError::InputRow),
                );
                continue;
            }
        };
        let message = RelayMessage {
            key: batch.keys[row].clone(),
            record,
            acks,
        };
        let carrier = message.record.one_row_batch();
        let side_inputs = HashMap::default();
        let lookup_columns = HashMap::default();
        let keys = [message.key.clone()];
        let projected = match project_vm_input_batch(
            &context.program.compiled.input_schema,
            &VmInputProjectionSources {
                carrier: &carrier,
                namespace_batches: &[],
                strict_namespaces: &[],
                keys: &keys,
                side_inputs: &side_inputs,
                ingest_metadata: None,
                lookup_columns: &lookup_columns,
                uninitialized: None,
            },
            None,
        ) {
            Ok(projected) => projected,
            Err(error) => {
                context.general(
                    &message.acks,
                    &error.change_context(StatefulAdmissionError::InputProjection),
                );
                continue;
            }
        };
        let executed = match execute_program_with_selection_in_context(
            context.branch.runtime.executor(),
            &context.program.compiled,
            &projected,
            &VmExecutionContext {
                now: context.execution_now,
                injector: None,
            },
        )
        .await
        {
            Ok(executed) => executed,
            Err(error) => {
                context.general(
                    &message.acks,
                    &error.change_context(StatefulAdmissionError::InputExecution),
                );
                continue;
            }
        };
        if let Some(error) = executed.batch.errors().row(0).first() {
            let error = context.program.structured_side_error(
                context.execution_now,
                "inferencer INPUTS evaluation refused this row".to_owned(),
                error.span,
                MessageErrorOperation::Inferencer,
            );
            context.rejected(message, error).await;
            continue;
        }
        let mapped = match vm_typed_batch_to_runtime_batch(&executed.batch) {
            Ok(mapped) => mapped,
            Err(error) => {
                context.general(
                    &message.acks,
                    &error.change_context(StatefulAdmissionError::MappedInput),
                );
                continue;
            }
        };
        let generated = match context
            .session
            .execute(
                context.branch.runtime.executor(),
                &path,
                context.prepared,
                mapped,
            )
            .await
        {
            Ok(Ok(generated)) => {
                changed = true;
                generated
            }
            Ok(Err(_error)) => {
                // Native diagnostics may contain input values. The message-error boundary projects
                // the typed row refusal without copying driver text into the error relay.
                let error = structured_message_error(
                    context.execution_now,
                    MessageErrorCode::External,
                    "the ONNX model refused this row or returned an invalid output tensor"
                        .to_owned(),
                    MessageErrorOperation::Inferencer,
                    None,
                    [],
                );
                context.rejected(message, error).await;
                continue;
            }
            Err(error) => {
                context.general(&message.acks, &error);
                continue;
            }
        };
        let generated = RelayRecordBatch {
            key: message.key.clone(),
            keys: vec![message.key],
            batch: Arc::new(generated),
            metadata: batch
                .metadata
                .take(&[row])
                .verified("an addressed input row has metadata"),
            acks: vec![message.acks],
        };
        processor_output::dispatch_inferencer_result(
            ProcessorOutputDispatchContext {
                branch: context.branch,
                node_kind: ModelKind::Inferencer,
                entity_kind: ModelKind::Inferencer,
                processor: context.processor,
                error_policies: context.error_policies,
                materialized_state: ProcessorMaterializedState::Admitted(
                    context.materialized_values,
                ),
                execution_now: context.execution_now,
            },
            context.output_routes,
            generated,
            &message.record,
        )
        .await;
    }
    if changed {
        context.replicated_state.publish_session(
            context.session,
            context.incarnation,
            context.snapshot_bytes,
        );
    }
}
