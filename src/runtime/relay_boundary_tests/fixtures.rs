//! Single-unit relay boundary observations for owner tests.
//!
//! Layer: test harness.
//! - **Owns.** Presence, ingress-cancellation and relay-metric fixture accessors.
//! - **Depends on.** The relay boundary harness and its typed runtime observations.
//! - **Must not know.** Production control-plane orchestration or external connectors.

use super::{
    BranchKey, DomainName, RELAY_BUFFER_DIRECTION_CONCRETE, RelayBoundaryServices,
    RelayBranchPresence, RelayMetricsHandle, RelayName, Runtime, Unit,
};

/// The branch presence of a relay that is not divided into partitions: its single unit's.
pub(super) fn single_presence(services: &RelayBoundaryServices) -> RelayBranchPresence {
    services
        .branch_presences
        .of(Unit::SINGLE)
        .cloned()
        .expect("a whole relay has its single unit's presence")
}

/// Whether the ingress channel a producer of `branch` binds now, on a relay that is not divided
/// into partitions, is cancelled from the start.
pub(super) fn single_ingress_cancelled(
    services: &RelayBoundaryServices,
    branch: &Option<BranchKey>,
) -> bool {
    services
        .ingress_slot(branch, Unit::SINGLE)
        .cancellation()
        .is_cancelled()
}

pub(super) fn relay_metrics(
    runtime: &Runtime,
    domain: &DomainName,
    relay: &RelayName,
) -> RelayMetricsHandle {
    runtime.inner.metrics.resolve_relay_metrics(
        domain,
        relay,
        Unit::SINGLE,
        None,
        RELAY_BUFFER_DIRECTION_CONCRETE,
        None,
    )
}
