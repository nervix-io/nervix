//! The durable decision admitted for one identified transaction preview.
//!
//! Layer: vocabulary.
//! - **Owns.** Serializable commit steps, Model transitions and entity-gate selections.
//! - **Depends on.** Transaction impact identities and control-plane Models.
//! - **Must not know.** Planning, persistence, consensus or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use super::{ExecutionStepImpactReport, TransactionOperationNumber, TransactionPreviewIdentity};
use crate::{DomainSchedule, Model, RelayName, ResourceName};

/// One exact Model transition captured by commit admission.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionModelTransition {
    Create {
        model: Box<Model>,
    },
    Replace {
        before: Box<Model>,
        after: Box<Model>,
    },
    Drop {
        model: Box<Model>,
    },
}

/// The exact runtime gate boundaries selected while the commit plan was admitted.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionEntityGatePlan {
    pub targets: crate::EntityGateTargets,
    pub relays: Vec<RelayName>,
}

/// The durable decision for one atomic transaction execution step.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum TransactionCommitStepKind {
    Models {
        transitions: Vec<TransactionModelTransition>,
        schedule: Option<Box<DomainSchedule>>,
        no_op_operations: Vec<TransactionOperationNumber>,
        model_gate: TransactionEntityGatePlan,
        ownership_gate: TransactionEntityGatePlan,
    },
    CreateResource {
        resource: ResourceName,
        labels: crate::EntityLabels,
        already_existed: bool,
    },
    EditResourceLabels {
        resource: ResourceName,
        labels: crate::EntityLabels,
    },
    ResetWasmState {
        reset: Box<crate::ResetWasmState>,
        request: crate::CommandExecutionReference,
        schedule: Box<DomainSchedule>,
    },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitPlanStep {
    pub impact: ExecutionStepImpactReport,
    pub kind: TransactionCommitStepKind,
}

/// The complete plan admitted for one identified transaction preview.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitPlan {
    pub preview: TransactionPreviewIdentity,
    pub steps: Vec<TransactionCommitPlanStep>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct TransactionCommitPlanHeader {
    pub preview: TransactionPreviewIdentity,
    #[rkyv(with = crate::CountAsU64)]
    pub step_count: usize,
}
