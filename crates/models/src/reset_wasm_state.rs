//! A requested replacement of a WASM processor's durable guest state.
//!
//! Layer: vocabulary.
//! - **Owns.** The explicit domain, processor, and branch selection of one reset command.
//! - **Depends on.** Domain and processor names, schema field names, and scalar literals.
//! - **Must not know.** Parsing, branch instances, coordination, or checkpoint storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    DomainName, ResolvedStateBranchSelection, StateBranchSelection, WasmProcessorName,
    WasmStateResetScope,
};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ResetWasmState {
    pub domain: DomainName,
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
