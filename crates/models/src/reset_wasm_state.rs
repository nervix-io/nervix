//! A requested replacement of a WASM processor's durable guest state.
//!
//! Layer: vocabulary.
//! - **Owns.** The processor and explicit branch selection of one reset command; the command acts
//!   on the domain its transaction is bound to.
//! - **Depends on.** Processor names, schema field names, and scalar literals.
//! - **Must not know.** Parsing, sessions, branch instances, coordination, or checkpoint storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    ResolvedStateBranchSelection, StateBranchSelection, WasmProcessorName, WasmStateResetScope,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResetWasmState {
    pub processor: WasmProcessorName,
    pub scope: StateBranchSelection,
}

impl ResolvedStateBranchSelection {
    pub fn reset_scope(&self) -> WasmStateResetScope {
        match self {
            Self::Unbranched => WasmStateResetScope::Unbranched,
            Self::AllBranches => WasmStateResetScope::AllBranches,
            Self::Branch { fingerprint, .. } => WasmStateResetScope::Branch(*fingerprint),
        }
    }
}
