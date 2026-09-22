//! What this node does when a WASM guest refuses the snapshot it was handed.
//!
//! Layer: data plane.
//!
//! - **Owns.** The request an owner raises for a refused guest-state lifetime, and the record of
//!   which refused lifetimes it has already raised.
//! - **Depends on.** The branch's guest-state placement and the channel the control plane drains.
//! - **Must not know.** Consensus, schedules, entity gates, or how a reset is coordinated. Whether
//!   a refused lifetime still has an attempt left is decided from committed state, not here.

use super::*;

/// How many refused lifetimes may be waiting for the control plane to decide at once.
///
/// A refused lifetime is raised once and held until the control plane answers it, so this bounds
/// distinct lifetimes rather than repeated reports of one. Beyond this many, a branch reports its
/// refusal as usual and leaves the raise to a later record.
const WASM_STATE_RECOVERY_QUEUE: usize = 32;

/// One refused guest-state lifetime, as its owner reports it to the control plane.
///
/// The placement and the refused generation are read from one another at construction and are never
/// set apart, so a request cannot name a placement of one lifetime and a generation of another.
#[derive(Debug, Clone)]
pub(crate) struct WasmStateRecoveryRequest {
    placement: RuntimeStatePlacement,
    generation: WasmStateGeneration,
    rejection: WasmSavedStateRejection,
}

impl WasmStateRecoveryRequest {
    /// The branch-local guest state whose saved snapshot was refused. It is what releases the raise
    /// once the control plane has answered.
    pub(crate) fn placement(&self) -> &RuntimeStatePlacement {
        &self.placement
    }

    /// The domain the refused branch executes in.
    pub(crate) fn domain(&self) -> &DomainName {
        &self.placement.domain
    }

    /// The WASM processor whose branch was refused.
    pub(crate) fn processor(&self) -> &ModelName {
        &self.placement.identifier
    }

    /// The guest-state lifetime the owner was refused.
    pub(crate) const fn generation(&self) -> WasmStateGeneration {
        self.generation
    }

    /// The guest's verdict on the refused snapshot.
    pub(crate) const fn rejection(&self) -> WasmSavedStateRejection {
        self.rejection
    }

    /// The reset target this refused lifetime selects: the branch that was refused, or the
    /// processor's explicit unbranched execution.
    pub(crate) fn target(&self) -> WasmStateResetTarget {
        match BranchKey::to_remote_key(&self.placement.branch_key) {
            Some(fields) => WasmStateResetTarget::Branch(fields),
            None => WasmStateResetTarget::Unbranched,
        }
    }
}

/// The refused lifetimes this node has raised and not yet seen answered.
///
/// The key is the branch's guest-state placement, which already carries the refused generation, so
/// a branch that reaches a new lifetime and has that one refused too is a different entry. Without
/// this, every record arriving for a refused branch would ask the leader the same question again.
/// The answer would not change, because the budget is decided from committed state, but the asking
/// would cost one cluster round trip per record.
#[derive(Debug, Default)]
pub(in crate::runtime) struct RaisedWasmStateRecoveries {
    raised: DashMap<RuntimeStatePlacement, (), RandomState>,
}

impl RaisedWasmStateRecoveries {
    /// Claim the right to raise `placement`, or report that it is already raised.
    fn claim(&self, placement: RuntimeStatePlacement) -> bool {
        self.raised.insert(placement, ()).is_none()
    }

    /// Release a raised lifetime once the control plane has answered it.
    fn release(&self, placement: &RuntimeStatePlacement) {
        self.raised.remove(placement);
    }
}

impl Runtime {
    /// Start draining the refused guest-state lifetimes this node raises.
    ///
    /// Called once while the node starts. Until it is, a branch that is refused reports the refusal
    /// and raises nothing, which is also what a node with no reachable leader would achieve.
    pub(crate) fn attach_wasm_state_recovery_coordinator(
        &self,
    ) -> mpsc::Receiver<WasmStateRecoveryRequest> {
        let (sender, receiver) = mpsc::channel(WASM_STATE_RECOVERY_QUEUE);
        self.inner
            .wasm_state_recovery_requests
            .store(Some(StdArc::new(sender)));
        receiver
    }

    /// Report that the control plane has answered the refused lifetime at `placement`.
    ///
    /// A spent attempt is durable, so a branch that is refused again after this raises one more
    /// request, which the leader answers from committed state rather than with a second reset.
    pub(crate) fn release_raised_wasm_state_recovery(&self, placement: &RuntimeStatePlacement) {
        self.inner.raised_wasm_state_recoveries.release(placement);
    }

    /// Raise the refused guest-state lifetime of one branch, unless it is already raised.
    pub(in crate::runtime) fn raise_wasm_state_recovery(
        &self,
        placement: &RuntimeStatePlacement,
        rejection: WasmSavedStateRejection,
    ) {
        let RuntimeState::WasmProcessor { generation, .. } = placement.state else {
            return;
        };
        let Some(sender) = self.inner.wasm_state_recovery_requests.load_full() else {
            return;
        };
        if !self
            .inner
            .raised_wasm_state_recoveries
            .claim(placement.clone())
        {
            return;
        }
        let request = WasmStateRecoveryRequest {
            placement: placement.clone(),
            generation,
            rejection,
        };
        if sender.try_send(request).is_err() {
            // The control plane has either stopped draining, which happens as the node stops, or
            // is already deciding this many distinct lifetimes. Neither is a reason to keep the
            // claim: the branch's next record raises it again.
            self.inner.raised_wasm_state_recoveries.release(placement);
            return;
        }
        info!(
            domain = %placement.domain.as_str(),
            processor = %placement.identifier.as_str(),
            generation = %generation,
            %rejection,
            "raised a refused WASM guest-state lifetime for recovery"
        );
    }

    /// Report a recovery the control plane refused or could not complete.
    ///
    /// The report is the only witness that an opted-in processor is not going to recover on its
    /// own, so it reaches the observers a runtime error reaches rather than this node's log alone.
    pub(crate) fn report_wasm_state_recovery_failure(
        &self,
        placement: &RuntimeStatePlacement,
        failure: &str,
    ) {
        self.inner.events.report_error(format!(
            "wasm processor '{}' rejected-state recovery failed in domain '{}': {failure}",
            placement.identifier.as_str(),
            placement.domain.as_str()
        ));
    }
}
