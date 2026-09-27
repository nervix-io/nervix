//! The node-to-node messages of WASM guest-state lifetime operations.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The owner-side reset stages, the typed reset target, and the leader-side reset and
//!   recovery requests, together with the pool, quota and deadline each of them carries.
//! - **Depends on.** The vocabulary a reset names and the typed request contract.
//! - **Must not know.** How a reset is coordinated, which guest state it replaces, or how a refused
//!   lifetime's recovery budget is decided.

use std::time::Duration;

use nervix_models::{
    CommandExecutionReference, CoordinationIdentity, DomainName, ModelName, RemoteRuntimeField,
    WasmSavedStateRejection, WasmStateResetReason, WasmStateResetScope,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, PoolClass, RemoteOperationFailure, RequestSubquota};

/// The owner-side stage of one coordinated WASM guest-state reset.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum WasmStateResetRuntimeAction {
    /// Quiesce the selected branch tasks and construct their fresh initial guest snapshots. A
    /// `published` request resumes a generation that is already durable and can never roll back.
    Prepare {
        branch_key: Option<Vec<RemoteRuntimeField>>,
        published: bool,
        reason: WasmStateResetReason,
    },
    /// Apply the currently committed schedule while the reset gate remains held. Replicas install
    /// the new generation before the execution owner writes its initial checkpoint.
    ActivateCommittedSchedule,
    /// Restore the preceding branch tasks after a failure before generation publication.
    Abort,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct WasmStateResetRuntimeRequest {
    pub coordination: CoordinationIdentity,
    pub domain: DomainName,
    pub processor: ModelName,
    pub request: CommandExecutionReference,
    pub scope: WasmStateResetScope,
    pub action: WasmStateResetRuntimeAction,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct WasmStateResetRuntimeResponse {
    pub result: Result<(), RemoteOperationFailure>,
}

/// The typed target of the shared leader-side WASM guest-state reset coordinator.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum WasmStateResetTarget {
    Unbranched,
    Branch(Vec<RemoteRuntimeField>),
    AllBranches,
}

/// Invoke the durable reset coordinator on the current control-plane leader.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct CoordinateWasmStateResetRequest {
    pub domain: DomainName,
    pub processor: ModelName,
    pub request: CommandExecutionReference,
    pub target: WasmStateResetTarget,
    pub reason: WasmStateResetReason,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoordinateWasmStateResetResponse {
    pub result: Result<(), RemoteOperationFailure>,
}

/// Ask the current control-plane leader to spend the one recovery attempt a refused guest-state
/// lifetime is worth.
///
/// The owner reports the generation it was refused rather than choosing a reset request of its own,
/// so the leader decides from the committed schedule whether that lifetime still has an attempt
/// left and which coordinated reset an admitted attempt drives.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub struct RecoverWasmProcessorStateRequest {
    pub domain: DomainName,
    pub processor: ModelName,
    pub target: WasmStateResetTarget,
    pub generation: u64,
    pub rejection: WasmSavedStateRejection,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoverWasmProcessorStateResponse {
    pub result: Result<(), RemoteOperationFailure>,
}

impl InterconnectRequest for WasmStateResetRuntimeRequest {
    type Response = WasmStateResetRuntimeResponse;

    const NAME: &'static str = "wasm_state_reset_runtime";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const TIMEOUT: Duration = Duration::from_secs(60);

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for CoordinateWasmStateResetRequest {
    type Response = CoordinateWasmStateResetResponse;

    const NAME: &'static str = "coordinate_wasm_state_reset";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const TIMEOUT: Duration = Duration::from_secs(120);
}

impl InterconnectRequest for RecoverWasmProcessorStateRequest {
    type Response = RecoverWasmProcessorStateResponse;

    const NAME: &'static str = "recover_wasm_processor_state";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const TIMEOUT: Duration = Duration::from_secs(120);
}
