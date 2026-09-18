//! The checkpoint that completes every WASM guest callback, and the acknowledgements it holds back.
//!
//! Layer: data plane.
//! - **Owns.** The success acknowledgements one guest callback decides, the checkpoint of the guest
//!   instance the callback leaves behind, releasing those acknowledgements once that checkpoint
//!   reached its boundary, and the branch's transition when it does not.
//! - **Depends on.** Replicated WASM guest state, the runtime's checkpoint stages, the processor's
//!   error policies and reporting, and acknowledgement sets.
//! - **Must not know.** NSPL parsing, placement policy, connector clients, or how stable storage
//!   and replicas are reached.

use error_stack::ResultExt as _;

use super::*;

/// How long one checkpoint may take to reach its boundary, from the guest's save to the
/// confirmation of its last replica. A checkpoint still short of its boundary then fails.
pub(super) const WASM_CHECKPOINT_DEADLINE: Duration = Duration::from_secs(10);

/// The success acknowledgements guest callbacks decided, held back until the checkpoint that covers
/// those callbacks reaches its boundary.
///
/// Each hold is one more share of an input's acknowledgement: the input resolves once every
/// delivery the callback made for it resolves and its hold is released. A negative decision
/// resolves the input at once, whatever holds it. Holds live in memory only, for one callback and
/// the checkpoint after it, which the checkpoint deadline bounds.
#[derive(Debug, Default)]
pub(super) struct WasmCheckpointHolds {
    held: Vec<AckSet>,
}

impl WasmCheckpointHolds {
    /// Hold back the success of the input `acks` acknowledges.
    pub(super) fn hold(&mut self, acks: &AckSet) {
        self.held.push(acks.attached());
    }

    /// The acknowledgements these holds keep open.
    pub(super) fn acks(&self) -> impl Iterator<Item = &AckSet> {
        self.held.iter()
    }

    /// Release the holds, once the checkpoint that covers them reached its boundary.
    fn release(self) {
        for held in self.held {
            held.ack_success();
        }
    }
}

/// What a WASM guest callback reports its failures through.
#[derive(Clone, Copy)]
pub(super) struct WasmCallbackReporting<'a> {
    pub(super) runtime: &'a Runtime,
    pub(super) domain: &'a DomainName,
    pub(super) node_kind: ModelKind,
    pub(super) processor: &'a ModelName,
    pub(super) error_policies: &'a ErrorPolicies,
}

impl WasmCallbackReporting<'_> {
    /// Apply the processor's general error policy to every input the branch holds, because a
    /// callback failed without reporting lineage for any of them. A success the policy grants is
    /// held back until the checkpoint that completes the callback.
    pub(super) fn fail_held_inputs(
        self,
        ack_map: &mut WasmAckMap,
        holds: &mut WasmCheckpointHolds,
        reason: String,
    ) {
        let held = std::mem::take(ack_map);
        for context in held.values() {
            holds.hold(&context.acks);
        }
        self.runtime.handle_general_error_for_acks(
            self.domain,
            self.node_kind,
            self.processor,
            self.error_policies,
            held.values().map(|context| &context.acks),
            reason,
        );
    }

    /// Report a guest callback that failed and decide every input the branch holds through the
    /// processor's general error policy.
    ///
    /// A callback that exhausted an execution limit leaves the guest's store unusable, so the
    /// instance is discarded and the next work recreates it from the committed checkpoint.
    pub(super) fn fail_callback(
        self,
        failure: Report<nervix_wasm::WasmGuestError>,
        instance: &mut Option<Box<WasmLiveInstance>>,
        ack_map: &mut WasmAckMap,
        holds: &mut WasmCheckpointHolds,
    ) {
        let resource_limit_exceeded = failure.current_context().is_resource_limit_exceeded();
        let module = &instance
            .as_ref()
            .verified("a guest callback only runs on a branch that holds an instance")
            .module;
        let reported = module.guest_failure(failure, None);
        self.fail_held_inputs(ack_map, holds, format!("{reported:#}"));
        if resource_limit_exceeded {
            *instance = None;
        }
    }

    /// Complete a guest callback: checkpoint the guest instance it leaves behind, and release the
    /// acknowledgements the callback decided once that checkpoint reached its boundary.
    ///
    /// A callback that leaves no instance behind added nothing to guest state that the committed
    /// checkpoint lacks, so its decisions are released at once. A checkpoint that fails leaves the
    /// previous committed checkpoint in place and ends the instance, whose state nothing committed:
    /// every acknowledgement the callback decided, and every input the instance still buffers, is
    /// negatively acknowledged, and the next work for the branch recreates the guest from the
    /// committed checkpoint.
    pub(super) async fn complete_callback(
        self,
        replicated_state: &ReplicatedWasmProcessorState,
        instance: &mut Option<Box<WasmLiveInstance>>,
        ack_map: &mut WasmAckMap,
        holds: WasmCheckpointHolds,
        execution_now: Timestamp,
    ) {
        let Some(live) = instance.as_mut() else {
            holds.release();
            return;
        };
        let checkpoint =
            checkpoint_wasm_instance(self.runtime, replicated_state, live, execution_now).await;
        match checkpoint {
            Ok(()) => holds.release(),
            Err(failure) => {
                let buffered = std::mem::take(ack_map);
                let withheld = holds
                    .acks()
                    .chain(buffered.values().map(|context| &context.acks));
                self.runtime.handle_internal_processor_error_for_acks(
                    self.domain,
                    self.node_kind,
                    self.processor,
                    self.error_policies,
                    withheld,
                    format!("{failure:#}"),
                );
                *instance = None;
            }
        }
    }
}

/// Checkpoint the state of `live` and wait until the checkpoint reached its boundary.
///
/// The checkpoint is refused before the guest is asked to save when this node no longer holds the
/// state's lifetime or ownership. Every failure leaves the committed checkpoint where it was and
/// records the failure in the branch's checkpoint progress.
pub(super) async fn checkpoint_wasm_instance(
    runtime: &Runtime,
    state: &ReplicatedWasmProcessorState,
    live: &mut WasmLiveInstance,
    execution_now: Timestamp,
) -> error_stack::Result<(), WasmInstanceError> {
    let deadline = Instant::now()
        .checked_add(WASM_CHECKPOINT_DEADLINE)
        .assured("a deadline seconds away stays within Instant");
    let boundary = match runtime.wasm_checkpoint_boundary(state) {
        Ok(boundary) => boundary,
        Err(error) => {
            state.record_failed();
            return Err(live.module.authority_failure(error));
        }
    };
    let saved = live
        .guest
        .save_state_in_context(nervix_wasm::WasmExecutionContext::new(execution_now))
        .await;
    let bytes = match saved {
        Ok(bytes) => bytes,
        Err(error) => {
            state.record_failed();
            return Err(live.module.guest_failure(error, None));
        }
    };
    let captured = state.capture(bytes, boundary);
    let revision = captured.revision();
    let durable = match runtime
        .persist_wasm_checkpoint(state, captured, deadline)
        .await
    {
        Ok(durable) => durable,
        Err(error) => {
            state.record_failed();
            return Err(live.module.persistence_failure(error, revision));
        }
    };
    let completed = match runtime
        .confirm_wasm_checkpoint(state, durable, deadline)
        .await
    {
        Ok(completed) => completed,
        Err(error) => {
            state.record_failed();
            return Err(live.module.persistence_failure(error, revision));
        }
    };
    state.commit(completed);
    Ok(())
}

/// Checkpoint the guest state an ownership handoff transfers, and wait until it reached its
/// boundary.
///
/// The instance is kept whatever happens. No callback runs between the branch's last committed
/// checkpoint and this one, so the instance holds exactly the committed state, and the handoff
/// that asked for the checkpoint decides what becomes of the branch.
pub(super) async fn checkpoint_wasm_guest_state(
    runtime: &Runtime,
    processor: &ModelName,
    replicated_state: &ReplicatedWasmProcessorState,
    live: &mut WasmLiveInstance,
    execution_now: Timestamp,
) -> OwnershipHandoffResult<()> {
    checkpoint_wasm_instance(runtime, replicated_state, live, execution_now)
        .await
        .change_context_lazy(|| OwnershipHandoffError::WasmCheckpoint {
            processor: processor.clone(),
        })
}

/// Every input token the validated output of one callback decides: carried into an output row, or
/// given a terminal decision.
pub(super) fn wasm_callback_decided_tokens(outputs: &[WasmMaterializedOutput]) -> HashSet<u64> {
    let mut decided = HashSet::default();
    for output in outputs {
        for row in &output.acks.rows {
            for token in &row.tokens {
                decided.insert(token.0);
            }
        }
        for decision in &output.acks.acked {
            for token in &decision.tokens {
                decided.insert(token.0);
            }
        }
        for decision in &output.acks.nacked {
            for token in &decision.tokens {
                decided.insert(token.0);
            }
        }
        for decision in &output.acks.message_errors {
            for token in &decision.tokens {
                decided.insert(token.0);
            }
        }
    }
    decided
}

#[cfg(test)]
mod tests {
    use tokio::time::timeout;

    use super::*;
    use crate::runtime_ack::AckOutcome;

    /// A held input stays unresolved after every other share of it succeeded, and resolves only
    /// when the hold is released.
    #[tokio::test]
    async fn a_held_input_resolves_only_once_its_hold_is_released() {
        let (acks, completion) = AckSet::root();
        let mut holds = WasmCheckpointHolds::default();
        holds.hold(&acks);
        acks.ack_success();
        let completion = tokio::spawn(completion.wait());
        tokio::task::yield_now().await;
        assert!(
            !completion.is_finished(),
            "an input must not be acknowledged before the checkpoint that covers it completes"
        );

        holds.release();
        let outcome = timeout(Duration::from_secs(1), completion)
            .await
            .expect("releasing the hold resolves the input")
            .expect("the completion task must not panic");
        assert_eq!(outcome, AckOutcome::Ack);
    }

    /// Withholding a hold negatively acknowledges the input even after its deliveries succeeded.
    #[tokio::test]
    async fn a_withheld_input_is_negatively_acknowledged() {
        let (acks, completion) = AckSet::root();
        let mut holds = WasmCheckpointHolds::default();
        holds.hold(&acks);
        acks.ack_success();
        for held in holds.acks() {
            held.no_ack("the checkpoint did not reach its boundary");
        }

        let outcome = timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("a withheld input resolves at once");
        assert_eq!(
            outcome,
            AckOutcome::NoAck("the checkpoint did not reach its boundary".to_string())
        );
    }
}
