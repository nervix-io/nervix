//! A requested bounded read of a WASM processor's guest-state facts.
//!
//! Layer: vocabulary.
//! - **Owns.** The processor, optional branch selection, page request and rendering format of one
//!   read command; the command reads the processor in its request's domain.
//! - **Depends on.** Processor names, branch selections, inspection page requests and formats.
//! - **Must not know.** Parsing, sessions, owner routing, checkpoint storage, or the facts it reads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{
    InspectionFormat, StateBranchSelection, StateInspectionPageRequest, WasmProcessorName,
};

/// `SHOW WASM PROCESSOR ... STATE`: one bounded read of a WASM processor's committed guest-state
/// facts, with the inspection clauses every state read shares.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct ShowWasmProcessorState {
    pub processor: WasmProcessorName,
    pub selection: Option<StateBranchSelection>,
    pub page: Option<StateInspectionPageRequest>,
    pub format: InspectionFormat,
}
