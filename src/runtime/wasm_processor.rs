//! Branch-local WASM processor execution.
//!
//! Layer: data plane.
//! - **Owns.** WASM invocation, guest output routing, branch-local timeout handling, and the
//!   lifecycle stage and diagnostic identity of every WASM processor failure.
//! - **Depends on.** Compiled WASM processors, Arrow batches and explicit execution contexts.
//! - **Must not know.** NSPL parsing, placement policy or external connector clients.

use error_stack::{Report, ResultExt as _};

use super::{state_replication::StateReplicationError, *};

/// Every way a WASM processor's module, or one of its branch instances, fails.
///
/// A module failure belongs to the processor. Every later failure belongs to one branch instance
/// and names the lifecycle stage it happened at, because the stage decides what the failure says
/// about the saved state: only a guest's rejection of the state it was asked to restore classifies
/// those bytes, and every other failure leaves them as usable as they were.
#[derive(Debug, thiserror::Error)]
pub(super) enum WasmInstanceError {
    #[error("resource store is not attached")]
    ResourceStoreDetached,
    #[error(
        "failed to resolve wasm processor '{}' resource '{}@{version}' file '{file}'",
        .processor.as_str(),
        .resource.as_str()
    )]
    ResolveFile {
        processor: ModelName,
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error(
        "failed to read wasm processor '{}' resource '{}@{version}' file '{}'",
        .processor.as_str(),
        .resource.as_str(),
        .path.display()
    )]
    ReadModule {
        processor: ModelName,
        resource: ResourceName,
        version: u64,
        path: std::path::PathBuf,
    },
    #[error(
        "wasm processor '{}' {} failed (resource '{}' version {version} file '{file}')",
        .processor.as_str(),
        WasmLifecycleStage::ModuleCompilation,
        .resource.as_str()
    )]
    CompileModule {
        processor: ModelName,
        resource: ResourceName,
        version: u64,
        file: String,
    },
    #[error(
        "wasm processor '{}' instance is unavailable while saving guest state",
        .processor.as_str()
    )]
    InstanceUnavailable { processor: ModelName },
    #[error(
        "wasm processor '{}' {stage} failed ({module}{}{})",
        .module.processor.as_str(),
        WasmGuestExportDetail(*.export),
        WasmSavedStateRevisionDetail(*.revision)
    )]
    Lifecycle {
        module: WasmBranchModule,
        stage: WasmLifecycleStage,
        export: Option<&'static str>,
        revision: Option<u64>,
    },
    #[error("failed to encode the wasm input batch")]
    EncodeInput,
    #[error(
        "wasm input row count {rows} does not match ack count {acks} and metadata count {metadata}"
    )]
    InputRowCount {
        rows: usize,
        acks: usize,
        metadata: usize,
    },
}

/// The stage of a WASM processor branch instance's lifecycle at which a failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(super) enum WasmLifecycleStage {
    /// Resolving, reading, or compiling the pinned module file.
    #[strum(to_string = "module compilation")]
    ModuleCompilation,
    /// A guest operation that failed without a verdict on saved state.
    #[strum(to_string = "{0}")]
    Guest(nervix_wasm::WasmGuestOperation),
    /// The guest could not decode the snapshot envelope of the saved state and rejected it.
    #[strum(to_string = "snapshot envelope decoding")]
    SnapshotEnvelopeDecoding,
    /// The guest decoded the saved snapshot and rejected the application state it carries.
    #[strum(to_string = "application state restoration")]
    ApplicationStateRestoration,
    /// The host could not decode or validate what the guest emitted.
    #[strum(to_string = "output emission")]
    OutputEmission,
    /// Writing the saved state to the node's state store.
    #[strum(to_string = "local state persistence")]
    LocalPersistence,
    /// Confirming the saved state with its replicas.
    #[strum(to_string = "state replication")]
    Replication,
    /// The state's authority refused the state: a peer that is not the state's authority, or this
    /// node once the state's lifetime or ownership moved on.
    #[strum(to_string = "state authority check")]
    AuthorityRejection,
}

impl WasmLifecycleStage {
    /// The stage a failed guest operation belongs to. A verdict on saved state and output the host
    /// cannot decode are stages of their own; every other failure belongs to its operation.
    pub(super) fn of_guest_failure(failure: &nervix_wasm::WasmGuestError) -> Self {
        match failure.saved_state_rejection() {
            Some(nervix_wasm::SavedStateRejection::SnapshotEnvelope) => {
                return Self::SnapshotEnvelopeDecoding;
            }
            Some(nervix_wasm::SavedStateRejection::ApplicationState) => {
                return Self::ApplicationStateRestoration;
            }
            None => {}
        }
        if failure.is_invalid_emission() {
            return Self::OutputEmission;
        }
        Self::Guest(failure.operation())
    }

    /// The stage a failed save of guest state belongs to once the guest produced the state.
    pub(super) fn of_state_persistence(failure: &StateReplicationError) -> Self {
        if failure.is_local_persistence() {
            return Self::LocalPersistence;
        }
        if failure.is_authority_rejection() {
            return Self::AuthorityRejection;
        }
        Self::Replication
    }
}

/// One WASM processor branch instance and the pinned module file it runs, as a diagnostic names
/// them. The pinned resource version carries the owning domain, which the node reporting the
/// failure already renders beside it.
#[derive(Debug, Clone)]
pub(super) struct WasmBranchModule {
    pub(super) processor: ModelName,
    pub(super) branch: Option<BranchKey>,
    pub(super) resource: ResourceId,
    pub(super) file: String,
}

impl WasmBranchModule {
    /// Reports a failed guest operation on this instance under the stage it belongs to. `revision`
    /// is the saved state revision the operation was restoring, when it was restoring one.
    pub(super) fn guest_failure(
        &self,
        failure: Report<nervix_wasm::WasmGuestError>,
        revision: Option<u64>,
    ) -> Report<WasmInstanceError> {
        let stage = WasmLifecycleStage::of_guest_failure(failure.current_context());
        let export = failure.current_context().export();
        failure.change_context(WasmInstanceError::Lifecycle {
            module: self.clone(),
            stage,
            export,
            revision,
        })
    }

    /// Reports that the guest state saved at `revision` could not be persisted or replicated.
    pub(super) fn persistence_failure(
        &self,
        failure: Report<StateReplicationError>,
        revision: u64,
    ) -> Report<WasmInstanceError> {
        let stage = WasmLifecycleStage::of_state_persistence(failure.current_context());
        failure.change_context(WasmInstanceError::Lifecycle {
            module: self.clone(),
            stage,
            export: None,
            revision: Some(revision),
        })
    }

    /// Reports a save of guest state this node no longer holds the lifetime or ownership of.
    pub(super) fn authority_failure(
        &self,
        failure: Report<StateReplicationError>,
    ) -> Report<WasmInstanceError> {
        failure.change_context(WasmInstanceError::Lifecycle {
            module: self.clone(),
            stage: WasmLifecycleStage::AuthorityRejection,
            export: None,
            revision: None,
        })
    }

    /// Reports output the guest emitted that failed validation.
    pub(super) fn emission_failure<C: error_stack::Context>(
        &self,
        failure: Report<C>,
    ) -> Report<WasmInstanceError> {
        failure.change_context(WasmInstanceError::Lifecycle {
            module: self.clone(),
            stage: WasmLifecycleStage::OutputEmission,
            export: Some("nervix_read_emit"),
            revision: None,
        })
    }
}

impl std::fmt::Display for WasmBranchModule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.branch {
            Some(branch) => write!(formatter, "branch {branch}")?,
            None => formatter.write_str("unbranched")?,
        }
        write!(
            formatter,
            ", resource '{}' version {} file '{}'",
            self.resource.identifier.as_str(),
            self.resource.version,
            self.file
        )
    }
}

/// A branch's live guest instance, together with the pinned module it was instantiated from.
#[derive(Debug)]
pub(super) struct WasmLiveInstance {
    pub(super) module: WasmBranchModule,
    pub(super) guest: nervix_wasm::WasmBranchInstance,
}

impl WasmCompiledBranchProcessor {
    /// Instantiates and initializes one branch guest of this module, hands it `saved` when the
    /// branch has saved state, and reports a failure under the lifecycle stage it happened at.
    pub(super) async fn instantiate_branch(
        &self,
        module: WasmBranchModule,
        limits: nervix_models::WasmProcessorLimits,
        init: WasmBranchInit,
        execution_now: Timestamp,
        saved: Option<RestorableGuestState<'_>>,
    ) -> error_stack::Result<WasmLiveInstance, WasmInstanceError> {
        let mut restored_bytes = None;
        let mut restored_revision = None;
        if let Some(saved) = saved {
            restored_bytes = Some(saved.bytes);
            restored_revision = Some(saved.revision);
        }
        let instantiation = self
            .compiled
            .instantiate_branch(
                limits,
                init,
                nervix_wasm::WasmExecutionContext::new(execution_now),
                restored_bytes,
            )
            .await;
        match instantiation {
            Ok(guest) => Ok(WasmLiveInstance { module, guest }),
            Err(error) => Err(module.guest_failure(error, restored_revision)),
        }
    }
}

/// The guest export a lifecycle failure names, rendered as a trailing diagnostic detail.
struct WasmGuestExportDetail(Option<&'static str>);

impl std::fmt::Display for WasmGuestExportDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(export) => write!(formatter, ", export '{export}'"),
            None => Ok(()),
        }
    }
}

/// The saved state revision a lifecycle failure involves, rendered as a trailing diagnostic
/// detail.
struct WasmSavedStateRevisionDetail(Option<u64>);

impl std::fmt::Display for WasmSavedStateRevisionDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(revision) => write!(formatter, ", saved state revision {revision}"),
            None => Ok(()),
        }
    }
}

pub(super) async fn flush_branch_wasm_processor(
    context: WasmFlushContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<WasmLiveInstance>>,
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
        execution_now,
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
    let input_schema = match branch.relay_schema(primary_input_relay) {
        Ok(schema) => schema,
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
    let mut output_schemas = Vec::with_capacity(output_routes.routes.len());
    for output in &output_routes.routes {
        match branch.relay_schema(&output.relay) {
            Ok(schema) => output_schemas.push((output.relay.clone(), schema)),
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
        }
    }

    let ensured = ensure_wasm_processor_instance(
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
            execution_now,
        },
        compiled,
        instance,
    )
    .await;
    if let Err(error) = ensured {
        branch.runtime.handle_general_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            forwarded.acks.iter(),
            format!("{error:#}"),
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
                    format!("{error:#}"),
                );
                return;
            }
        };
    ack_map.extend(input_ack_map);
    let process_result = instance
        .as_mut()
        .verified("the is_none check above returned unless this branch holds an instance")
        .guest
        .process_envelope_in_context(
            &envelope,
            nervix_wasm::WasmExecutionContext::new(execution_now),
        )
        .await;
    let outputs = match process_result {
        Ok(outputs) => outputs,
        Err(error) => {
            let resource_limit_exceeded = error.current_context().is_resource_limit_exceeded();
            let failure = instance
                .as_ref()
                .verified("the is_none check above returned unless this branch holds an instance")
                .module
                .guest_failure(error, None);
            branch.runtime.handle_general_error_for_acks(
                &branch.domain,
                node_kind,
                processor,
                error_policies,
                ack_map.values().map(|context| &context.acks),
                format!("{failure:#}"),
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
            module: &instance
                .as_ref()
                .verified("the is_none check above returned unless this branch holds an instance")
                .module,
            dispatch_error: "failed to forward message",
            execution_now,
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
            format!("{error:#}"),
        );
        return;
    }
    let persist_result = persist_wasm_guest_state(
        &branch.runtime,
        processor,
        replicated_state,
        instance,
        execution_now,
    )
    .await;
    if let Err(error) = persist_result {
        branch.runtime.handle_internal_processor_error_for_acks(
            &branch.domain,
            node_kind,
            processor,
            error_policies,
            std::iter::empty::<&AckSet>(),
            format!("{error:#}"),
        );
    }
}

pub(super) struct WasmInstanceContext<'a> {
    pub(super) branch: &'a BranchRuntime,
    pub(super) processor: &'a ModelName,
    pub(super) resource: &'a ResourceName,
    pub(super) resource_version: u64,
    pub(super) file: &'a str,
    pub(super) limits: nervix_models::WasmProcessorLimits,
    pub(super) guest_input_relay: &'a RelayName,
    pub(super) input_schema: &'a Arc<CompiledSchema>,
    pub(super) output_schemas: &'a [(RelayName, Arc<CompiledSchema>)],
    pub(super) replicated_state: &'a ReplicatedWasmProcessorState,
    pub(super) execution_now: Timestamp,
}

impl Runtime {
    /// Compiles the guest module of the resource version a WASM processor pins. The version is
    /// part of the processor's model, so compiling never consults the resource catalog.
    pub(super) async fn compile_wasm_processor_module(
        &self,
        domain: &DomainName,
        processor: impl Into<ModelName>,
        resource: &ResourceName,
        resource_version: u64,
        file: &str,
    ) -> error_stack::Result<WasmCompiledBranchProcessor, WasmInstanceError> {
        let processor = processor.into();
        let id = ResourceId::new(domain.clone(), resource.clone(), resource_version);
        let Some(resource_store) = self.inner.resource_store.load_full() else {
            return Err(Report::new(WasmInstanceError::ResourceStoreDetached));
        };
        let path = resource_store
            .resolve_content_path(&id, file)
            .change_context_lazy(|| WasmInstanceError::ResolveFile {
                processor: processor.clone(),
                resource: resource.clone(),
                version: resource_version,
                file: file.to_string(),
            })?;
        let wasm =
            tokio::fs::read(&path)
                .await
                .change_context_lazy(|| WasmInstanceError::ReadModule {
                    processor: processor.clone(),
                    resource: resource.clone(),
                    version: resource_version,
                    path: path.clone(),
                })?;
        let compiled = self
            .inner
            .wasm_runtime
            .compile_processor(&wasm)
            .await
            .change_context_lazy(|| WasmInstanceError::CompileModule {
                processor: processor.clone(),
                resource: resource.clone(),
                version: resource_version,
                file: file.to_string(),
            })?;
        Ok(WasmCompiledBranchProcessor {
            compiled: Arc::new(compiled),
        })
    }
}

/// Makes sure the branch has a guest instance. The module is compiled at most once per branch
/// instance, from the resource version the processor pins.
pub(super) async fn ensure_wasm_processor_instance(
    context: WasmInstanceContext<'_>,
    compiled: &mut Option<WasmCompiledBranchProcessor>,
    instance: &mut Option<Box<WasmLiveInstance>>,
) -> error_stack::Result<(), WasmInstanceError> {
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
        execution_now,
    } = context;
    let compiled_module = match compiled.as_ref() {
        Some(compiled_module) => compiled_module.clone(),
        None => {
            let prepared = branch
                .runtime
                .compile_wasm_processor_module(
                    &branch.domain,
                    processor,
                    resource,
                    resource_version,
                    file,
                )
                .await?;
            *compiled = Some(prepared.clone());
            *instance = None;
            prepared
        }
    };

    if instance.is_none() {
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
        let module = WasmBranchModule {
            processor: processor.clone(),
            branch: branch.key.clone(),
            resource: ResourceId::new(branch.domain.clone(), resource.clone(), resource_version),
            file: file.to_string(),
        };
        let saved = replicated_state.restore_guest_state();
        let live = compiled_module
            .instantiate_branch(module, limits, init, execution_now, saved.restorable())
            .await?;
        *instance = Some(Box::new(live));
    }
    Ok(())
}

pub(super) async fn wasm_envelope_from_relay_batch(
    executor: &Executor,
    batch: &RelayRecordBatch,
    next_ack_token: &mut u64,
) -> error_stack::Result<(WasmEnvelope, WasmAckMap), WasmInstanceError> {
    let arrow_ipc_batch = batch
        .batch
        .encode_arrow_ipc(executor)
        .await
        .change_context(WasmInstanceError::EncodeInput)?
        .to_vec();
    let row_count = batch.batch.batch().num_rows();
    if row_count != batch.acks.len() || row_count != batch.metadata.len() {
        return Err(Report::new(WasmInstanceError::InputRowCount {
            rows: row_count,
            acks: batch.acks.len(),
            metadata: batch.metadata.len(),
        }));
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
    use nervix_models::{ParseAsType, WasmStateGeneration};
    use nervix_wasm::{WasmAckToken, WasmEnvelope, WasmOutputColumnRef};
    use nonzero_ext::nonzero;
    use ordered_float::OrderedFloat;
    use triomphe::Arc;

    use super::*;
    use crate::runtime_schema::{RuntimeValue, test_runtime_row};

    fn tenant_branch(tenant: &str) -> Option<BranchKey> {
        let key = BranchKey::from_fields([(
            FieldName::parse("tenant").expect("valid identifier"),
            RuntimeValue::String(tenant.to_string()),
        )])
        .expect("test branch key must be non-empty");
        Some(key)
    }

    fn sessionizer_module(branch: Option<BranchKey>) -> WasmBranchModule {
        WasmBranchModule {
            processor: ModelName::parse("sessionizer").expect("valid identifier"),
            branch,
            resource: ResourceId::new(
                DomainName::parse("events").expect("valid domain"),
                ResourceName::parse("sessionizer").expect("valid identifier"),
                3,
            ),
            file: "sessionizer.wasm".to_string(),
        }
    }

    fn placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("events").expect("valid domain"),
            state: RuntimeState::WasmProcessor {
                schema: SchemaFingerprint::from_digest([7; 32]),
                generation: WasmStateGeneration::FIRST,
            },
            kind: ModelKind::WasmProcessor,
            identifier: ModelName::parse("sessionizer").expect("valid identifier"),
            branch_key: tenant_branch("alpha"),
        }
    }

    #[test]
    fn a_rejected_saved_state_is_a_stage_of_its_own() {
        let envelope = nervix_wasm::WasmGuestError::SnapshotEnvelopeRejected { reason: None };
        let application = nervix_wasm::WasmGuestError::ApplicationStateRejected { reason: None };

        assert_eq!(
            WasmLifecycleStage::of_guest_failure(&envelope),
            WasmLifecycleStage::SnapshotEnvelopeDecoding
        );
        assert_eq!(
            WasmLifecycleStage::of_guest_failure(&application),
            WasmLifecycleStage::ApplicationStateRestoration
        );
    }

    #[test]
    fn a_failed_restore_without_a_verdict_stays_with_its_operation() {
        let exhausted = nervix_wasm::WasmGuestError::Failed {
            operation: nervix_wasm::WasmGuestOperation::StateRestore,
            cause: nervix_wasm::WasmGuestCallError::FuelExhausted {
                limit: nonzero!(1_000u64),
                export: Some("nervix_load_state"),
            },
        };
        let refused_init = nervix_wasm::WasmGuestError::Failed {
            operation: nervix_wasm::WasmGuestOperation::Initialization,
            cause: nervix_wasm::WasmGuestCallError::GlobalError {
                reason: "unsupported schema".to_string(),
            },
        };

        assert_eq!(
            WasmLifecycleStage::of_guest_failure(&exhausted),
            WasmLifecycleStage::Guest(nervix_wasm::WasmGuestOperation::StateRestore)
        );
        assert_eq!(
            WasmLifecycleStage::of_guest_failure(&refused_init),
            WasmLifecycleStage::Guest(nervix_wasm::WasmGuestOperation::Initialization)
        );
    }

    #[test]
    fn output_the_host_cannot_decode_is_an_emission_failure() {
        let decode_failure =
            WasmEnvelope::decode(&[0xa0]).expect_err("a single byte is not an envelope");
        let emission = nervix_wasm::WasmGuestError::Failed {
            operation: nervix_wasm::WasmGuestOperation::BatchProcessing,
            cause: nervix_wasm::WasmGuestCallError::InvalidEmission(decode_failure),
        };

        assert_eq!(
            WasmLifecycleStage::of_guest_failure(&emission),
            WasmLifecycleStage::OutputEmission
        );
        assert_eq!(emission.export(), Some("nervix_read_emit"));
    }

    #[test]
    fn a_state_persistence_failure_is_classified_by_what_refused_the_state() {
        let local = StateReplicationError::Persist {
            placement: placement(),
            lsm: 4,
        };
        let quorum = StateReplicationError::ReplicaQuorum {
            placement: placement(),
            lsm: 4,
            required_acks: 1,
        };
        let refused = StateReplicationError::RemoteFailure {
            target: ClusterNodeName::parse("node-2").expect("valid name"),
            placement: placement(),
            failure: nervix_interconnect::RemoteOperationFailure::rejected(
                nervix_interconnect::RemoteOperationSubject::domain(&placement().domain),
            ),
        };

        assert_eq!(
            WasmLifecycleStage::of_state_persistence(&local),
            WasmLifecycleStage::LocalPersistence
        );
        assert_eq!(
            WasmLifecycleStage::of_state_persistence(&quorum),
            WasmLifecycleStage::Replication
        );
        assert_eq!(
            WasmLifecycleStage::of_state_persistence(&refused),
            WasmLifecycleStage::AuthorityRejection
        );
    }

    #[test]
    fn a_rejected_saved_state_diagnostic_names_the_branch_module_export_and_revision() {
        let rejection = Report::new(nervix_wasm::WasmGuestError::ApplicationStateRejected {
            reason: Some("counters header is truncated".to_string()),
        });

        let failure = sessionizer_module(tenant_branch("alpha")).guest_failure(rejection, Some(12));

        assert_eq!(
            format!("{failure:#}"),
            "wasm processor 'sessionizer' application state restoration failed (branch \
             {\"tenant\":\"alpha\"}, resource 'sessionizer' version 3 file 'sessionizer.wasm', \
             export 'nervix_load_state', saved state revision 12): wasm guest rejected the \
             application state in its saved snapshot: counters header is truncated"
        );
    }

    #[test]
    fn an_unbranched_guest_failure_diagnostic_renders_every_cause_once() {
        let exhausted = Report::new(nervix_wasm::WasmGuestError::Failed {
            operation: nervix_wasm::WasmGuestOperation::BatchProcessing,
            cause: nervix_wasm::WasmGuestCallError::FuelExhausted {
                limit: nonzero!(1_000u64),
                export: Some("nervix_process_batch"),
            },
        });

        let failure = sessionizer_module(None).guest_failure(exhausted, None);

        assert_eq!(
            format!("{failure:#}"),
            "wasm processor 'sessionizer' batch processing failed (unbranched, resource \
             'sessionizer' version 3 file 'sessionizer.wasm', export 'nervix_process_batch'): \
             wasm guest batch processing failed: wasm guest exhausted MAX FUEL 1000"
        );
    }

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
