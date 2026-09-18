//! Opaque drivers for measuring WASM guest-state checkpoints against a real state store.
//!
//! This module only exists with the `benchmarks` feature. Its public surface deliberately exposes
//! benchmark operations instead of Nervix runtime carriers, placements or stores.

use std::path::Path;

use fjall::Database;
use meticulous::ResultExt as _;
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_models::{
    DomainName, FieldName, ModelKind, ModelName, SchemaFingerprint, WasmStateGeneration,
};
use tempfile::TempDir;
use triomphe::Arc;

use super::{
    BranchKey, ReplicatedWasmProcessorState, RuntimeState, RuntimeStatePlacement,
    RuntimeStateStore, WasmCheckpointBoundary,
};
use crate::runtime_schema::RuntimeValue;

/// Branch guest states of one WASM processor, checkpointed into a state store of their own.
pub struct WasmCheckpointBenchmark {
    store: Arc<RuntimeStateStore>,
    executor: Executor,
    branches: Vec<ReplicatedWasmProcessorState>,
    state_bytes: usize,
    _directory: TempDir,
}

impl WasmCheckpointBenchmark {
    /// A state store in a fresh directory under `parent`, holding `branches` branch states that each
    /// save `state_bytes` bytes per checkpoint. `parent` decides what a synchronization costs, so a
    /// measurement of durable checkpoints needs a directory on the storage being measured.
    pub fn new(parent: &Path, branches: usize, state_bytes: usize) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("wasm-checkpoint-benchmark")
            .tempdir_in(parent)
            .assured("the benchmark parent directory is writable");
        let database = Database::builder(directory.path())
            .open()
            .assured("a fresh benchmark directory opens as a database");
        let executor = Executor::default();
        let store = Arc::new(
            RuntimeStateStore::from_database(database, executor.clone())
                .assured("a fresh database opens as a runtime state store"),
        );
        let domain = DomainName::parse("benchmark").assured("the benchmark domain name is valid");
        let processor = ModelName::parse("guest").assured("the benchmark processor name is valid");
        let tenant = FieldName::parse("tenant").assured("the benchmark branch field is valid");
        let branches = (0..branches)
            .map(|branch| {
                let key = BranchKey::from_fields([(
                    tenant.clone(),
                    RuntimeValue::String(format!("tenant-{branch}")),
                )])
                .assured("a benchmark branch key names one field");
                let placement = RuntimeStatePlacement {
                    domain: domain.clone(),
                    state: RuntimeState::WasmProcessor {
                        schema: SchemaFingerprint::from_digest([0; 32]),
                        generation: WasmStateGeneration::FIRST,
                    },
                    kind: ModelKind::WasmProcessor,
                    identifier: processor.clone(),
                    branch_key: Some(key),
                };
                ReplicatedWasmProcessorState::new(placement, None)
            })
            .collect();
        Self {
            store,
            executor,
            branches,
            state_bytes,
            _directory: directory,
        }
    }

    /// Checkpoint every branch once, all of them at the same time, and return once every
    /// checkpoint is on stable storage.
    pub async fn checkpoint_every_branch(&self) {
        let checkpoints = self.branches.iter().map(|branch| async move {
            let captured = branch.capture(
                vec![7; self.state_bytes],
                WasmCheckpointBoundary::LocalStorage,
            );
            self.store
                .persist_wasm_checkpoint(&branch.placement, captured.saved())
                .await
                .assured("a benchmark checkpoint reaches its fresh store");
        });
        futures_util::future::join_all(checkpoints).await;
    }

    /// Write every branch's state once, all of them at the same time, on the storage workers and
    /// without synchronizing the storage, the way periodic runtime-state snapshots are written.
    pub async fn write_every_branch_without_synchronization(&self) {
        let writes = self.branches.iter().map(|branch| async move {
            let captured = branch.capture(
                vec![7; self.state_bytes],
                WasmCheckpointBoundary::LocalStorage,
            );
            let saved = captured.saved();
            let placement = branch.placement.clone();
            let store = self.store.clone();
            let reservation = self
                .executor
                .reserve(MemoryClass::Bulk, 1)
                .await
                .assured("a benchmark write is admitted by an idle executor");
            self.executor
                .run_storage(
                    StorageClass::Filesystem,
                    reservation,
                    move |_charge, _cancellation| {
                        store.persist_latest_snapshot(&placement, saved.revision(), saved.bytes())
                    },
                )
                .await
                .assured("a benchmark storage job runs to completion")
                .assured("a benchmark write reaches its fresh store");
        });
        futures_util::future::join_all(writes).await;
    }
}
