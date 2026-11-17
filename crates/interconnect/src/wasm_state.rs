//! The node-to-node messages of WASM guest-state lifetime operations.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The owner-side reset stages, the typed reset target, and the leader-side reset and
//!   recovery requests, together with the pool, quota and deadline each of them carries.
//! - **Depends on.** The vocabulary a reset names and the typed request contract.
//! - **Must not know.** How a reset is coordinated, which guest state it replaces, or how a refused
//!   lifetime's recovery budget is decided.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use nervix_models::{
    CommandExecutionReference, CoordinationIdentity, DomainName, ModelName,
    WasmSavedStateRejection, WasmStateResetReason, WasmStateResetScope,
};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, PoolClass, RemoteOperationFailure, RequestSubquota};

/// Inherited checkpoint diagnostics keep the actual unit; fields alone cannot reconstruct an
/// external partitioner's decision. The aggregate metrics exchange stays field-only.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeInheritedWasmStateRequest {
    pub domain: DomainName,
    pub processor: ModelName,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescribeInheritedWasmStateResponse {
    pub result: Result<
        Vec<(
            nervix_models::Unit,
            Vec<nervix_models::WasmCheckpointInspection>,
        )>,
        RemoteOperationFailure,
    >,
}

impl InterconnectRequest for DescribeInheritedWasmStateRequest {
    type Response = DescribeInheritedWasmStateResponse;
    const NAME: &'static str = "describe_inherited_wasm_state";
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Setup;
}

/// The owner-side stage of one coordinated WASM guest-state reset.
#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq)]
pub enum WasmStateResetRuntimeAction {
    /// Quiesce the selected branch tasks and construct their fresh initial guest snapshots. A
    /// `published` request resumes a generation that is already durable and can never roll back.
    Prepare {
        branch_key: nervix_models::RemoteBranchKey,
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
    Branch(nervix_models::RemoteBranchKey),
    AllBranches,
    /// An inherited or retained branch's actual execution, including keys with no external
    /// witness yet. Boxing keeps field-based targets at their existing footprint.
    BranchInUnit {
        unit: nervix_models::Unit,
        key: Box<nervix_models::RemoteBranchKey>,
    },
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
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Transfer;

    fn coordination_identity(&self) -> Option<&CoordinationIdentity> {
        Some(&self.coordination)
    }
}

impl InterconnectRequest for CoordinateWasmStateResetRequest {
    type Response = CoordinateWasmStateResetResponse;

    const NAME: &'static str = "coordinate_wasm_state_reset";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Recovery;
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "turmoil"))
))]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Exact unit and checkpoint witnesses through the production wire codec.
    //! - **Depends on.** Current WASM messages and bounded wire admission.
    //! - **Must not know.** Guest execution, scheduling or a broker.

    use meticulous::ResultExt as _;
    use nervix_execution::Executor;
    use nervix_models::{
        BranchKeyFingerprint, Unit, WasmCheckpointInspection, WasmCheckpointStage,
        WasmCheckpointState, WasmStateGeneration,
    };

    use super::*;
    use crate::RkyvMessage;

    async fn round_trip<M: RkyvMessage + Clone + PartialEq + std::fmt::Debug>(message: M) {
        let executor = Executor::default();
        let class = PoolClass::Management;
        let (bytes, _charge) = message
            .clone()
            .encode_rkyv(executor.clone(), class, class.payload_limit(&executor))
            .await
            .assured("the bounded message encodes");
        let (decoded, _charge) = M::decode_rkyv(executor, class, bytes)
            .await
            .assured("the current message decodes");
        assert_eq!(decoded, message);
    }

    #[nervix_primitives::test]
    async fn inherited_checkpoint_diagnostics_keep_the_actual_units_and_lifetimes() {
        let request = DescribeInheritedWasmStateRequest {
            domain: "events".try_into().assured("valid domain"),
            processor: "guest".try_into().assured("valid processor"),
        };
        round_trip(request).await;
        let checkpoint = |byte| WasmCheckpointInspection {
            branch: Some(BranchKeyFingerprint::new([byte; 32])),
            state: WasmCheckpointState {
                generation: WasmStateGeneration::FIRST,
                committed_revision: Some(std::num::NonZeroU64::MIN),
                latest_revision: std::num::NonZeroU64::new(2),
                stage: WasmCheckpointStage::Captured,
                required_replicas: Some(1),
                confirmed_replicas: Some(0),
            },
        };
        round_trip(DescribeInheritedWasmStateResponse {
            result: Ok(vec![
                (Unit::partition(3), vec![checkpoint(7)]),
                (Unit::partition(1), vec![checkpoint(13)]),
            ]),
        })
        .await;
        round_trip(WasmStateResetTarget::BranchInUnit {
            unit: Unit::partition(3),
            key: Box::new(nervix_models::RemoteBranchKey::Direct(
                vec![nervix_models::RemoteRuntimeField {
                    name: "tenant".to_string(),
                    value: nervix_models::RemoteRuntimeValue::String("alpha".to_string()),
                }]
                .into(),
            )),
        })
        .await;
    }
}

impl InterconnectRequest for RecoverWasmProcessorStateRequest {
    type Response = RecoverWasmProcessorStateResponse;

    const NAME: &'static str = "recover_wasm_processor_state";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Admission;
    const DEADLINE: crate::RequestDeadline = crate::RequestDeadline::Recovery;
}
