//! Admission-time inference: one model run for each batch a branch's inferencer admits, before
//! any route, with every route receiving that one result.
//!
//! Layer: data plane.
//! Owns: the `INPUTS` evaluation of an admitted batch, the disposition of every message the model
//! run accepted, refused or failed, one dispatch of the accepted results to every route, and the
//! publication of carried state after the batch.
//! Depends on: compiled VM programs, the branch's inference session and processor output dispatch.
//! Must not know: Models, parsing, placement or durable state formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "executes a branch-owned inference plan")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "admits batches against compiled programs and prepared native tensor plans"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "processes admitted batches in their branch task's arrival order"
    )
)]

use error_stack::Report;

use super::*;

/// Every way an inferencer fails to take an admitted batch through its model. The inferencer
/// names itself when it reports the failure, and the acknowledgements of the messages it names
/// fail with it.
#[derive(Debug, thiserror::Error)]
pub(super) enum InferencerAdmissionError {
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
    #[error("failed to build the INPUTS batch")]
    InputsBatch,
    #[error("INPUTS execution failed")]
    InputsExecution,
    #[error("failed to read the INPUTS output")]
    InputsOutput,
    #[error("ONNX execution failed for resource '{resource}@{version}' file '{file}'")]
    Execute {
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error("failed to address admitted input row {row}")]
    InputRow { row: usize },
    #[error("the model run returned {outputs} output rows for {accepted} accepted messages")]
    OutputRows { outputs: usize, accepted: usize },
}

/// One inferencer branch taking one admitted batch through its model.
pub(super) struct InferencerAdmission<'a> {
    pub(super) branch: &'a mut BranchRuntime,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
    pub(super) output_routes: &'a mut RelayProcessorOutputsNode,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: u64,
    pub(super) file: &'a str,
    pub(super) program: &'a CompiledProgramWithMaterializedInterest,
    pub(super) prepared: &'a Arc<inferencer::PreparedInference>,
    pub(super) session: &'a mut inferencer::InferenceSession,
    pub(super) materialized_values: &'a HashMap<String, RuntimeValue>,
    pub(super) execution_now: Timestamp,
}

/// Messages of the admitted batch that a route-independent step refused, each with the error its
/// routes' message-error policies receive.
struct RejectedRow {
    row: usize,
    error: RaisedMessageError,
}

/// Inference reports a route failure with the admitted message its generated row came from:
/// output row `i` of the dispatched batch was generated from admitted row `rows[i]`.
#[derive(Clone, Copy)]
pub(super) struct AdmittedInputRows<'a> {
    batch: &'a RelayRecordBatch,
    rows: &'a [usize],
}

impl<'a> AdmittedInputRows<'a> {
    pub(super) fn new(batch: &'a RelayRecordBatch, rows: &'a [usize]) -> Self {
        Self { batch, rows }
    }
}

impl processor_output::ProcessorOutputErrorSource for AdmittedInputRows<'_> {
    fn error_input(
        &self,
        _dispatched: &RelayRecordBatch,
        row: usize,
    ) -> error_stack::Result<RuntimeRow, relay_batch::RelayRecordBatchError> {
        let Some(admitted) = self.rows.get(row) else {
            return Err(Report::new(
                relay_batch::RelayRecordBatchError::SelectedRowOutOfBounds {
                    row,
                    batch_rows: self.rows.len(),
                },
            ));
        };
        self.batch.runtime_row(*admitted)
    }
}

impl inferencer::MessageRefusal {
    /// The value-free error a message the model run refused alone carries to its routes'
    /// message-error policies. No native diagnostic and no tensor value is copied into it.
    fn message_error(self, execution_now: Timestamp) -> RaisedMessageError {
        let (class, message) = match self {
            Self::Input => (
                MessageErrorClass::VALIDATION,
                "the mapped INPUTS of this row do not form the declared input tensors".to_owned(),
            ),
            Self::Model { status } => (
                MessageErrorClass::REFUSED,
                format!("the ONNX model refused this row with status {status:?}"),
            ),
            // An output of the wrong shape is the model breaking its declared contract.
            Self::Output => (
                MessageErrorClass::INTERNAL,
                "the ONNX model returned an invalid output tensor for this row".to_owned(),
            ),
            Self::WorkingSet => (
                MessageErrorClass::LIMIT,
                "the model outputs of this row exceed the configured relay operation limit"
                    .to_owned(),
            ),
        };
        raised_message_error(
            execution_now,
            class,
            message,
            MessageErrorOperation::Inferencer,
            None,
            [],
        )
    }
}

impl<'a> InferencerAdmission<'a> {
    /// The branch task awaits each admitted batch, so neither another batch nor a route timer runs
    /// the model while this one does. Only this operation owns the allocation of its future.
    pub(super) fn admit(
        self,
        batch: RelayRecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.admit_batch(batch))
    }

    async fn admit_batch(mut self, mut batch: RelayRecordBatch) {
        if self.session.needs_load() && !self.load_session(&batch).await {
            return;
        }
        let Some(executed) = self.evaluate_inputs(&batch).await else {
            return;
        };
        let mut rejected = Vec::new();
        let mut mapped_rows = Vec::with_capacity(batch.acks.len());
        for row in 0..batch.acks.len() {
            if let Some(error) = executed.batch.errors().row(row).first() {
                rejected.push(RejectedRow {
                    row,
                    error: self.program.structured_side_error(
                        self.execution_now,
                        "inferencer INPUTS evaluation refused this row".to_owned(),
                        error.span,
                        MessageErrorOperation::Inferencer,
                    ),
                });
                continue;
            }
            mapped_rows.push(row);
        }
        let mut run = inferencer::InferenceRun::default();
        if !mapped_rows.is_empty() {
            match vm_typed_batch_selected_rows_to_runtime_batch(&executed.batch, &mapped_rows) {
                Ok(mapped) => {
                    run = self
                        .branch
                        .remote_ack_timing
                        .keep_alive_while_pending(
                            batch.acks.as_slice(),
                            self.session.run(
                                self.branch.runtime.executor(),
                                self.prepared,
                                &mapped,
                            ),
                        )
                        .await;
                }
                Err(error) => {
                    let acks = mapped_rows
                        .iter()
                        .map(|row| &batch.acks[*row])
                        .collect::<Vec<_>>();
                    self.fail(
                        acks,
                        &error.change_context(InferencerAdmissionError::InputsOutput),
                    );
                    mapped_rows.clear();
                }
            }
        }
        drop(executed);
        let mut acks = std::mem::take(&mut batch.acks)
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>();
        let accepted = run
            .accepted
            .iter()
            .map(|row| mapped_rows[*row])
            .collect::<Vec<_>>();
        if let Some(outputs) = run.outputs.take() {
            self.dispatch(&batch, &mut acks, outputs, &accepted).await;
        }
        for refused in run.refused {
            rejected.push(RejectedRow {
                row: mapped_rows[refused.row],
                error: refused.refusal.message_error(self.execution_now),
            });
        }
        rejected.sort_by_key(|rejected| rejected.row);
        for rejected in rejected {
            nervix_primitives::task::consume_budget().await;
            let Some(row_acks) = acks[rejected.row].take() else {
                continue;
            };
            self.reject(&batch, rejected.row, row_acks, rejected.error)
                .await;
        }
        for failed in run.failed {
            let mut failed_acks = Vec::with_capacity(failed.rows.len());
            for row in &failed.rows {
                if let Some(row_acks) = acks[mapped_rows[*row]].take() {
                    failed_acks.push(row_acks);
                }
            }
            self.fail(
                failed_acks.iter(),
                &failed.error.change_context(self.execute_failure()),
            );
        }
        if run.advanced
            && let Some(carried) = self.session.carried()
        {
            carried.publish();
        }
    }

    /// Load the branch's session from the resource version the inferencer pins. A failure fails
    /// the whole batch, and the next batch tries again.
    async fn load_session(&mut self, batch: &RelayRecordBatch) -> bool {
        let Some(store) = self.branch.runtime.inner.resource_store.load_full() else {
            self.fail(
                batch.acks.iter(),
                &Report::new(InferencerAdmissionError::ResourceStoreDetached),
            );
            return false;
        };
        let id = ResourceId::new(
            self.branch.domain.clone(),
            self.resource.clone(),
            self.resource_version,
        );
        let path = match store.resolve_content_path(&id, self.file) {
            Ok(path) => path,
            Err(error) => {
                self.fail(
                    batch.acks.iter(),
                    &error.change_context(InferencerAdmissionError::ResolveFile {
                        resource: self.resource.clone(),
                        version: self.resource_version,
                        file: self.file.to_owned(),
                    }),
                );
                return false;
            }
        };
        let loaded = self
            .branch
            .remote_ack_timing
            .keep_alive_while_pending(
                batch.acks.as_slice(),
                self.session
                    .load(self.branch.runtime.executor(), &path, self.prepared),
            )
            .await;
        if let Err(error) = loaded {
            self.fail(
                batch.acks.iter(),
                &error.change_context(InferencerAdmissionError::LoadModel {
                    resource: self.resource.clone(),
                    version: self.resource_version,
                    file: self.file.to_owned(),
                }),
            );
            return false;
        }
        true
    }

    /// Evaluate `INPUTS` once over the whole admitted batch. A failure of the whole evaluation
    /// fails the batch; a row the evaluation refuses is left to its routes' policies.
    async fn evaluate_inputs(
        &self,
        batch: &RelayRecordBatch,
    ) -> Option<nervix_vm::ExecutionResult> {
        let side_inputs = HashMap::default();
        let lookup_columns = HashMap::default();
        let projected = match project_vm_input_batch(
            &self.program.compiled.input_schema,
            &VmInputProjectionSources {
                carrier: &batch.batch,
                namespace_batches: &[],
                strict_namespaces: &[],
                keys: &batch.keys,
                side_inputs: &side_inputs,
                ingest_metadata: None,
                lookup_columns: &lookup_columns,
                uninitialized: None,
            },
            None,
        ) {
            Ok(projected) => projected,
            Err(error) => {
                self.fail(
                    batch.acks.iter(),
                    &error.change_context(InferencerAdmissionError::InputsBatch),
                );
                return None;
            }
        };
        let executed = self
            .branch
            .remote_ack_timing
            .keep_alive_while_pending(
                batch.acks.as_slice(),
                execute_program_with_selection_in_context(
                    self.branch.runtime.executor(),
                    &self.program.compiled,
                    &projected,
                    &VmExecutionContext {
                        now: self.execution_now,
                        injector: None,
                    },
                ),
            )
            .await;
        match executed {
            Ok(executed) => Some(executed),
            Err(error) => {
                self.fail(
                    batch.acks.iter(),
                    &error.change_context(InferencerAdmissionError::InputsExecution),
                );
                None
            }
        }
    }

    /// Hand the accepted messages' outputs to every route at once, and release the inferencer's
    /// own share of each message once its routes have taken theirs.
    async fn dispatch(
        &mut self,
        batch: &RelayRecordBatch,
        acks: &mut [Option<AckSet>],
        outputs: RuntimeRecordBatch,
        accepted: &[usize],
    ) {
        let mut accepted_acks = Vec::with_capacity(accepted.len());
        for row in accepted {
            accepted_acks.push(
                acks[*row]
                    .take()
                    .verified("each admitted message is accepted, refused or failed exactly once"),
            );
        }
        let output_rows = outputs.batch().num_rows();
        if output_rows != accepted.len() {
            self.fail(
                accepted_acks.iter(),
                &Report::new(InferencerAdmissionError::OutputRows {
                    outputs: output_rows,
                    accepted: accepted.len(),
                }),
            );
            return;
        }
        let metadata = batch
            .metadata
            .take(accepted)
            .verified("accepted rows are rows of the admitted batch");
        let generated = RelayRecordBatch::from_filtered_parts(
            batch.key.clone(),
            outputs,
            metadata,
            accepted_acks,
        )
        .verified("the check above matched the outputs, metadata and acknowledgements row for row");
        let dispatched = processor_output::dispatch_selected_processor_outputs(
            ProcessorOutputDispatchContext {
                branch: &mut *self.branch,
                node_kind: ModelKind::Inferencer,
                entity_kind: ModelKind::Inferencer,
                processor: self.processor,
                error_policies: self.error_policies,
                materialized_state: ProcessorMaterializedState::Admitted(self.materialized_values),
                execution_now: self.execution_now,
            },
            &mut *self.output_routes,
            generated,
            None,
            false,
            AdmittedInputRows::new(batch, accepted),
        )
        .await;
        // Each route took its own share of every message it forwarded or buffered, so the
        // inferencer releases the share it was handed. A dispatch that failed has already settled
        // its messages through the error policy and returns nothing.
        if let Some(dispatched) = dispatched {
            for acks in dispatched {
                acks.ack_success();
            }
        }
    }

    /// Hand a message one step refused to every route's message-error policy, each with its own
    /// share of the message's acknowledgement. Without routes the message is complete.
    async fn reject(
        &self,
        batch: &RelayRecordBatch,
        row: usize,
        acks: AckSet,
        error: RaisedMessageError,
    ) {
        let record = match batch.runtime_row(row) {
            Ok(record) => record,
            Err(failure) => {
                self.fail(
                    std::iter::once(&acks),
                    &failure.change_context(InferencerAdmissionError::InputRow { row }),
                );
                return;
            }
        };
        let count = self.output_routes.routes.len();
        if count == 0 {
            acks.ack_success();
            return;
        }
        let mut shares = Vec::with_capacity(count);
        for _ in 1..count {
            shares.push(acks.attached());
        }
        shares.push(acks);
        for (output, share) in self.output_routes.routes.iter().zip(shares) {
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
                    message: RelayMessage {
                        key: batch.key.clone(),
                        record: record.clone(),
                        acks: share,
                    },
                    error: error.clone(),
                    partial_output: None,
                    materialized_state: self.materialized_values.clone(),
                    ingest_metadata: None,
                    execution_now: self.execution_now,
                })
                .await;
        }
    }

    /// Fail `acks` through the inferencer's general error policy.
    fn fail<'b, C: error_stack::Context>(
        &self,
        acks: impl IntoIterator<Item = &'b AckSet>,
        error: &Report<C>,
    ) {
        self.branch
            .runtime
            .handle_internal_processor_error_for_acks(
                &self.branch.domain,
                ModelKind::Inferencer,
                self.processor,
                self.error_policies,
                acks,
                error,
            );
    }

    fn execute_failure(&self) -> InferencerAdmissionError {
        InferencerAdmissionError::Execute {
            resource: self.resource.clone(),
            version: self.resource_version,
            file: self.file.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests;
