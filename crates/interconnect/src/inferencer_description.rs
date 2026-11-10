//! Layer: engines and infrastructure.
//! Owns: the bounded request for one inferencer unit's typed state summary.
//! Depends on: vocabulary identities and the common request transport.
//! Must not know: schedules, tensor values, state collection or description rendering.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "carries typed inference observations")
)]

use nervix_models::{DomainName, InferencerName, InferencerStateSummary, Unit};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{InterconnectRequest, RemoteOperationFailure, RuntimeState};

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct DescribeInferencerRequest {
    pub domain: DomainName,
    pub name: InferencerName,
    pub unit: Unit,
    /// The exact current schema/model identity, or none for a stateless inferencer.
    pub state: Option<RuntimeState>,
}

impl InterconnectRequest for DescribeInferencerRequest {
    type Response = Result<InferencerStateSummary, RemoteOperationFailure>;

    const NAME: &'static str = "describe_inferencer";
    const TIMEOUT: std::time::Duration = crate::DEFAULT_REQUEST_TIMEOUT;
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "turmoil"))))]
#[path = "inferencer_description_tests.rs"]
mod tests;
