//! What a WASM guest asks for when it wants the state lifetime it runs in replaced.
//!
//! Layer: data plane.
//!
//! - **Owns.** The branch-local fence a requesting guest leaves behind, the request that branch
//!   hands over, and the requests this node holds until they are coordinated.
//! - **Depends on.** Branch keys, WASM state generations, and the runtime handle.
//! - **Must not know.** Consensus, entity gates, the interconnect, or how a reset is coordinated.
//!   Coordinating one is the control plane's, and a branch task only asks.

use nervix_interconnect::WasmStateResetTarget;
use nervix_models::WasmStateGeneration;

use super::*;

/// Whether a branch's guest may still run, or has asked for the state lifetime it ran in to be
/// replaced.
///
/// A guest that asked leaves nothing behind that a later callback could continue from: its
/// instance is dropped with its timers, and the state the callback would have checkpointed is
/// exactly what the reset discards. The fence is what keeps the branch from quietly instantiating
/// the guest again from the committed checkpoint of the lifetime being replaced, which is the one
/// state the guest asked never to see again. It ends with the branch task, which the coordinated
/// reset stops before it starts a fresh one.
#[derive(Debug)]
pub(super) enum WasmGuestStateResetFence {
    /// No reset was requested; the guest runs normally.
    Open,
    /// The guest asked for a new state lifetime, from the generation named here. Every input the
    /// branch accepts until the coordinated reset stops it is negatively acknowledged, so its
    /// source redelivers it into the fresh lifetime.
    Closed { generation: WasmStateGeneration },
}

impl WasmGuestStateResetFence {
    /// Why this branch refuses the work it accepts, or `None` while its guest still runs in the
    /// state lifetime it has.
    pub(super) fn refusal(&self, processor: &ModelName) -> Option<String> {
        let Self::Closed { generation } = self else {
            return None;
        };
        Some(format!(
            "wasm processor '{}' guest requested a new state lifetime, replacing generation \
             {generation}",
            processor.as_str()
        ))
    }
}

/// The branch whose guest asked for a new state lifetime.
///
/// One branch has at most one outstanding request: a guest that asks again before this node hands
/// the request over replaces it, and a fenced branch runs no further callback that could ask.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct GuestWasmStateResetBranch {
    domain: DomainName,
    processor: ModelName,
    branch: Option<BranchKeyFingerprint>,
}

/// One branch's guest asking for the state lifetime it ran in to be replaced.
#[derive(Debug, Clone)]
pub(crate) struct GuestWasmStateResetRequest {
    branch: GuestWasmStateResetBranch,
    key: Option<BranchKey>,
    generation: WasmStateGeneration,
}

impl GuestWasmStateResetRequest {
    pub(super) fn new(
        domain: DomainName,
        processor: ModelName,
        key: Option<BranchKey>,
        generation: WasmStateGeneration,
    ) -> Self {
        let branch = GuestWasmStateResetBranch {
            domain,
            processor,
            branch: key.as_ref().map(BranchKey::fingerprint),
        };
        Self {
            branch,
            key,
            generation,
        }
    }

    pub(crate) fn domain(&self) -> &DomainName {
        &self.branch.domain
    }

    pub(crate) fn processor(&self) -> &ModelName {
        &self.branch.processor
    }

    pub(crate) fn branch_fingerprint(&self) -> Option<&BranchKeyFingerprint> {
        self.branch.branch.as_ref()
    }

    /// The generation the guest asked from. A reset that already replaced it has answered this
    /// request, so a coordinator that observes a later generation has nothing left to do.
    pub(crate) fn generation(&self) -> WasmStateGeneration {
        self.generation
    }

    /// The exact scope this request selects: the one branch whose guest asked, and never another
    /// processor, domain, or branch.
    pub(crate) fn target(&self) -> WasmStateResetTarget {
        match BranchKey::to_remote_key(&self.key) {
            Some(fields) => WasmStateResetTarget::Branch(fields),
            None => WasmStateResetTarget::Unbranched,
        }
    }

    /// The stable reference this request is coordinated under.
    ///
    /// It is derived from the branch and the generation the guest asked from, so every request the
    /// guests of one state lifetime make resolves to the same coordinated reset and replaces that
    /// lifetime once, while the first request made from the lifetime that replaced it resolves to
    /// the next reset.
    pub(crate) fn reference(&self) -> CommandExecutionReference {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nervix/wasm-guest-state-reset");
        hasher.update(self.branch.domain.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(self.branch.processor.as_str().as_bytes());
        hasher.update(&[0]);
        match self.branch.branch.as_ref() {
            Some(fingerprint) => hasher.update(fingerprint.fingerprint()),
            None => hasher.update(&[0]),
        };
        hasher.update(&u64::from(self.generation).to_be_bytes());
        let digest = hasher.finalize().to_hex();
        CommandExecutionReference::parse(format!("wasm-guest-reset.{digest}"))
            .assured("a fixed prefix and a hex digest are a valid command execution reference")
    }
}

/// The branch whose guest asked, and what the branch needs to hand the request over.
pub(super) struct WasmGuestStateResetContext<'a> {
    pub(super) branch: &'a BranchRuntime,
    pub(super) processor: &'a ModelName,
    pub(super) replicated_state: &'a ReplicatedWasmProcessorState,
}

impl WasmGuestStateResetContext<'_> {
    /// Hand this branch's request for a new state lifetime to the node, and return the generation
    /// it asked from.
    fn hand_over(&self) -> WasmStateGeneration {
        let generation = self.replicated_state.generation();
        self.branch
            .runtime
            .request_guest_wasm_state_reset(GuestWasmStateResetRequest::new(
                self.branch.domain.clone(),
                self.processor.clone(),
                self.branch.key.clone(),
                generation,
            ));
        generation
    }
}

/// Refuse the input a fenced branch accepted, and ask again for the state lifetime it is waiting
/// for.
///
/// Coordinating a reset can fail for reasons that leave this branch running and fenced, such as
/// another alteration holding the domain. The branch cannot run a callback that would ask again,
/// so the input it refuses is what re-states the request. A reset that already completed stopped
/// this task before the new lifetime started, so nothing a later task hands over belongs to a
/// generation that is gone.
pub(super) fn refuse_fenced_wasm_branch_input(
    context: WasmGuestStateResetContext<'_>,
    refusal: &str,
    batches: Vec<RelayRecordBatch>,
) {
    for batch in batches {
        for ack in batch.acks.iter() {
            ack.no_ack(refusal);
        }
    }
    context.hand_over();
}

/// Settle the callback whose guest asked for a new state lifetime, and hand the request over.
///
/// `outcome` is what that callback returned. The request is terminal for everything the callback
/// left uncommitted, so the output it emitted is dropped here rather than dispatched, and every
/// input the branch holds — the ones the callback decided and the ones the guest still buffers —
/// is negatively acknowledged for its source to redeliver into the new lifetime. A callback that
/// failed while asking is still asking, so its failure is reported and the request stands.
///
/// Nothing is checkpointed: the state this callback would save is the state the reset discards,
/// and the committed checkpoint it would replace is the one the guest asked never to be restored
/// from. The live instance is dropped with its pending timeouts, and the fence closes so the
/// branch cannot instantiate the guest again from that same committed checkpoint while it waits.
pub(super) fn request_wasm_guest_state_reset(
    context: WasmGuestStateResetContext<'_>,
    outcome: Result<Vec<nervix_wasm::WasmEnvelope>, Report<nervix_wasm::WasmGuestError>>,
    instance: &mut Option<Box<WasmLiveInstance>>,
    ack_map: &mut WasmAckMap,
    fence: &mut WasmGuestStateResetFence,
) {
    if let Err(failure) = outcome {
        let module = &instance
            .as_ref()
            .verified("a guest callback only runs on a branch that holds an instance")
            .module;
        let reported = module.guest_failure(failure, None);
        context
            .branch
            .runtime
            .events()
            .report_error(format!("{reported:#}"));
    }
    let generation = context.hand_over();
    *fence = WasmGuestStateResetFence::Closed { generation };
    *instance = None;
    let refusal = fence
        .refusal(context.processor)
        .verified("the fence closed immediately above");
    for discarded in std::mem::take(ack_map).values() {
        discarded.acks.no_ack(refusal.clone());
    }
}

/// The guest-requested state resets this node holds until the control plane coordinates them.
///
/// They live in memory only. A node that stops before a request is coordinated loses it, and the
/// guest that recreated from the same committed checkpoint asks again from its next callback,
/// because asking is never proof that a new lifetime became durable.
#[derive(Default)]
pub(in crate::runtime) struct PendingGuestWasmStateResets {
    requests: DashMap<GuestWasmStateResetBranch, GuestWasmStateResetRequest, RandomState>,
    /// Wakes the coordinator that drains `requests`.
    arrived: Notify,
}

impl Runtime {
    /// Hand over a branch's request for a new guest-state lifetime.
    ///
    /// The branch fences itself before it asks, so the request it leaves here is the whole of what
    /// it still expects from this node.
    pub(super) fn request_guest_wasm_state_reset(&self, request: GuestWasmStateResetRequest) {
        let pending = &self.inner.guest_wasm_state_resets;
        pending
            .requests
            .insert(request.branch.clone(), request.clone());
        info!(
            domain = request.domain().as_str(),
            processor = request.processor().as_str(),
            key = branch_key_display(&request.key),
            generation = %request.generation,
            "wasm guest requested a new branch state lifetime"
        );
        pending.arrived.notify_one();
    }

    /// Take every guest-requested state reset this node is holding.
    pub(crate) fn take_guest_wasm_state_resets(&self) -> Vec<GuestWasmStateResetRequest> {
        let pending = &self.inner.guest_wasm_state_resets;
        let branches = pending
            .requests
            .iter()
            .map(|request| request.key().clone())
            .collect::<Vec<_>>();
        let mut taken = Vec::with_capacity(branches.len());
        for branch in branches {
            if let Some((_, request)) = pending.requests.remove(&branch) {
                taken.push(request);
            }
        }
        taken
    }

    /// Wait until a branch hands over a request for a new guest-state lifetime.
    pub(crate) async fn guest_wasm_state_reset_requested(&self) {
        self.inner.guest_wasm_state_resets.arrived.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn tenant(tenant: &str) -> Option<BranchKey> {
        Some(
            BranchKey::from_fields([(
                FieldName::try_from("tenant").expect("the field name must be valid"),
                RuntimeValue::String(tenant.to_string()),
            )])
            .expect("a one-field branch key must build"),
        )
    }

    fn request(
        tenant_key: Option<BranchKey>,
        generation: WasmStateGeneration,
    ) -> GuestWasmStateResetRequest {
        GuestWasmStateResetRequest::new(
            DomainName::try_from("events").expect("the domain name must be valid"),
            ModelName::try_from("counting_guest").expect("the processor name must be valid"),
            tenant_key,
            generation,
        )
    }

    /// Every request a branch's guests make from one state lifetime coordinates as one reset.
    #[test]
    fn requests_from_one_branch_and_generation_share_one_reference() {
        let first = request(tenant("alpha"), WasmStateGeneration::FIRST);
        let second = request(tenant("alpha"), WasmStateGeneration::FIRST);

        assert_eq!(first.reference(), second.reference());
    }

    /// The first request made from the lifetime a reset produced coordinates as the next reset,
    /// rather than resolving to the one that already completed.
    #[test]
    fn a_request_from_the_next_generation_has_its_own_reference() {
        let replaced = request(tenant("alpha"), WasmStateGeneration::FIRST);
        let replacement = request(
            tenant("alpha"),
            WasmStateGeneration::try_from(2).expect("generation 2 is valid"),
        );

        assert_ne!(replaced.reference(), replacement.reference());
    }

    #[test]
    fn sibling_branches_of_one_processor_have_their_own_references() {
        let alpha = request(tenant("alpha"), WasmStateGeneration::FIRST);
        let beta = request(tenant("beta"), WasmStateGeneration::FIRST);
        let unbranched = request(None, WasmStateGeneration::FIRST);

        assert_ne!(alpha.reference(), beta.reference());
        assert_ne!(alpha.reference(), unbranched.reference());
    }

    /// A request selects the branch that asked and nothing else, so a guest can never name a
    /// sibling branch or the processor as a whole.
    #[test]
    fn a_request_targets_only_the_branch_whose_guest_asked() {
        let alpha = request(tenant("alpha"), WasmStateGeneration::FIRST);

        let WasmStateResetTarget::Branch(fields) = alpha.target() else {
            panic!("a branched request must select its own concrete branch");
        };
        assert_eq!(
            BranchKey::from_remote_key(Some(fields)).expect("the selected key must decode"),
            tenant("alpha")
        );
        assert_eq!(
            request(None, WasmStateGeneration::FIRST).target(),
            WasmStateResetTarget::Unbranched
        );
    }

    /// A guest that asks again before its branch is coordinated leaves one request behind, so the
    /// coordinator never runs the same reset twice.
    #[test]
    fn repeated_requests_for_one_branch_are_held_as_one() {
        let runtime = Runtime::new();

        runtime
            .request_guest_wasm_state_reset(request(tenant("alpha"), WasmStateGeneration::FIRST));
        runtime
            .request_guest_wasm_state_reset(request(tenant("alpha"), WasmStateGeneration::FIRST));
        runtime.request_guest_wasm_state_reset(request(tenant("beta"), WasmStateGeneration::FIRST));

        let taken = runtime.take_guest_wasm_state_resets();
        let references = taken
            .iter()
            .map(GuestWasmStateResetRequest::reference)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            references,
            BTreeSet::from([
                request(tenant("alpha"), WasmStateGeneration::FIRST).reference(),
                request(tenant("beta"), WasmStateGeneration::FIRST).reference(),
            ])
        );
        assert!(runtime.take_guest_wasm_state_resets().is_empty());
    }
}
