//! Layer: vocabulary.
//! Owns: typed inference state counts and observed revision summaries.
//! Depends on: node identities and serialization.
//! Must not know: tensor values, state owners, collection or transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "names observed inference state facts")
)]

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::ClusterNodeName;

/// Progress observed by an execution owner for one currently assigned replica.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerReplicaProgress {
    pub node: ClusterNodeName,
    /// A maximum over branch-local revisions, not a common checkpoint. Zero is a known baseline.
    pub replicated_revision: Option<u64>,
}

/// Counts from current immutable tensor publications, without inspecting tensor or key values.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct InferencerStateSummary {
    pub branches: u64,
    pub state_bytes: u64,
    /// Maximum branch-local revision observed at this stage; absent when there was no state.
    pub published_revision: Option<u64>,
    pub persisted_revision: Option<u64>,
    /// In the schedule's replica order, including a replica that has not reported progress.
    pub replicas: Vec<InferencerReplicaProgress>,
}
