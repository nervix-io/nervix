//! The guest binding, limits and node-wide policies of a visual WASM processor until they become
//! its Model.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of a WASM processor's guest binding, the text of its `MAX FUEL`
//!   and `MAX MEMORY` limits, its `ON REJECTED STATE` and `ON GLOBAL ERROR` choices, the
//!   consequence each rejected-state policy has under the guest-state contract, and their
//!   conversion to the WASM processor Model.
//! - **Depends on.** Typed WASM vocabulary, the shared processor binding draft, and the number
//!   and byte-size readings the language applies to the limits.
//! - **Must not know.** Guest compilation, fuel or memory accounting, or state recovery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{GeneralErrorPolicy, WasmProcessorLimits, WasmRejectedStatePolicy};

use super::{
    processor_binding_draft::{ProcessorBinding, ProcessorBindingDraft},
    processor_draft::{ProcessorDraftError, positive_byte_size},
};

/// What only a WASM processor declares, kept as typed and chosen until the draft builds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct WasmDraft {
    pub(super) binding: ProcessorBindingDraft,
    /// The `MAX FUEL` text, a positive whole number of fuel units.
    pub(super) max_fuel: String,
    /// The `MAX MEMORY` text, a positive byte size.
    pub(super) max_memory: String,
    /// `PRESERVE` unless the operator opts in to a reset, as the language assumes when the clause
    /// is left out.
    pub(super) rejected_state: WasmRejectedStatePolicy,
    pub(super) global_error: Option<GeneralErrorPolicy>,
}

/// The checked guest binding and limits of a WASM processor.
pub(super) struct WasmParts {
    pub(super) binding: ProcessorBinding,
    pub(super) limits: WasmProcessorLimits,
}

impl WasmDraft {
    /// The guest binding and its limits, or the first missing or unreadable one in form order.
    pub(super) fn build(&self) -> error_stack::Result<WasmParts, ProcessorDraftError> {
        let binding = self.binding.build()?;
        let max_fuel = match self.max_fuel.trim().parse::<NonZeroU64>() {
            Ok(fuel) => fuel,
            Err(cause) => {
                return Err(Report::new(cause).change_context(ProcessorDraftError::MaxFuel));
            }
        };
        let max_memory_bytes = positive_byte_size(
            &self.max_memory,
            ProcessorDraftError::MaxMemory,
            ProcessorDraftError::MaxMemoryZero,
        )?;
        Ok(WasmParts {
            binding,
            limits: WasmProcessorLimits {
                max_fuel,
                max_memory_bytes,
            },
        })
    }

    /// The `ON GLOBAL ERROR` policy, the processor's last clause, which the form checks after its
    /// routes.
    pub(super) fn global_error_policy(
        &self,
    ) -> error_stack::Result<GeneralErrorPolicy, ProcessorDraftError> {
        match &self.global_error {
            Some(policy) => Ok(policy.clone()),
            None => Err(Report::new(ProcessorDraftError::GlobalError)),
        }
    }
}

/// What `policy` does when a recreated guest refuses the snapshot it is handed, stated in the
/// guest-state contract's own terms.
pub(super) fn rejected_state_consequence(policy: WasmRejectedStatePolicy) -> &'static str {
    match policy {
        WasmRejectedStatePolicy::Preserve => {
            "PRESERVE keeps the snapshot a recreated guest refused and reports the refusal. Every \
             later instance of that branch is handed the same bytes and reports the same failure \
             until an operator acts, for example with RESET WASM PROCESSOR … STATE FOR …; no \
             computation state is discarded."
        }
        WasmRejectedStatePolicy::Reset => {
            "RESET replaces the refused branch's state lifetime once, through the coordinated \
             reset: that branch starts again from the guest's initial state and its refused \
             computation state is discarded, while sibling branches keep theirs. Only the guest's \
             own refusal of the saved bytes reaches this policy; a module, limit, storage or \
             replication failure never discards state through it."
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{RequestedResourceVersion, ResourceName};

    use super::*;

    fn bound() -> WasmDraft {
        let mut draft = WasmDraft::default();
        draft
            .binding
            .pin
            .select_resource(ResourceName::parse("guests").assured("valid resource"));
        draft
            .binding
            .pin
            .select_version(RequestedResourceVersion::Number(3));
        draft.binding.file = "processors/filter_even.wasm".into();
        draft
    }

    fn refusal(draft: &WasmDraft) -> ProcessorDraftError {
        draft
            .build()
            .err()
            .assured("the WASM draft is incomplete")
            .current_context()
            .clone()
    }

    #[test]
    fn limits_are_positive_and_read_as_the_language_reads_them() {
        let mut draft = bound();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxFuel);
        draft.max_fuel = "0".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxFuel);
        draft.max_fuel = "1_000".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxFuel);
        draft.max_fuel = " 1000000000 ".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxMemory);
        draft.max_memory = "lots".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxMemory);
        draft.max_memory = "0B".into();
        assert_eq!(refusal(&draft), ProcessorDraftError::MaxMemoryZero);
        draft.max_memory = "64MiB".into();
        let parts = draft.build().assured("the binding and limits are complete");
        assert_eq!(parts.limits.max_fuel.get(), 1_000_000_000);
        assert_eq!(parts.limits.max_memory_bytes.get(), 64 * 1024 * 1024);
        assert_eq!(parts.binding.version, RequestedResourceVersion::Number(3));
        assert_eq!(parts.binding.file, "processors/filter_even.wasm");
    }

    #[test]
    fn the_global_error_policy_is_chosen_and_the_rejected_state_defaults_to_preserve() {
        let mut draft = bound();
        assert_eq!(draft.rejected_state, WasmRejectedStatePolicy::Preserve);
        assert_eq!(
            draft
                .global_error_policy()
                .err()
                .assured("no global error policy is chosen")
                .current_context(),
            &ProcessorDraftError::GlobalError
        );
        draft.global_error = Some(GeneralErrorPolicy::Ignore);
        assert_eq!(
            draft
                .global_error_policy()
                .verified("a global error policy is chosen"),
            GeneralErrorPolicy::Ignore
        );
        assert!(
            rejected_state_consequence(WasmRejectedStatePolicy::Preserve)
                .contains("keeps the snapshot")
        );
        assert!(
            rejected_state_consequence(WasmRejectedStatePolicy::Reset).contains("initial state")
        );
    }
}
