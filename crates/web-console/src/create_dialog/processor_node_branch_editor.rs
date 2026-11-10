//! The node-wide branch a branch-preserving visual processor declares.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser choice between `UNBRANCHED` and one named branch for the processor families
//!   that declare a node-wide branch, wherever the family's form shows it.
//! - **Depends on.** Processor drafts, the typed branch choice control and Leptos signals.
//! - **Must not know.** Branch scheduling, branch-key construction or registry validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    processor_draft::{NodeBranchDraft, ProcessorFamily},
};

#[component]
pub(super) fn ProcessorNodeBranch(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let Some(label) = family.node_branch_label() else {
        return ().into_any();
    };
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-processor-branching">
            <legend>{label}</legend>
            <button type="button" data-branch="unbranched"
                class:active=move || draft.get().branching == NodeBranchDraft::Unbranched
                disabled=pending
                on:click=move |_| { draft.update(|state| state.choose_unbranched()); signals.edit(); }>"UNBRANCHED"</button>
            <button type="button" data-branch="named"
                class:active=move || matches!(draft.get().branching, NodeBranchDraft::Branched(_))
                disabled=pending
                on:click=move |_| { draft.update(|state| state.choose_branched()); signals.edit(); }>"Named branch"</button>
        </fieldset>
        <Show when=move || matches!(draft.get().branching, NodeBranchDraft::Branched(_)) fallback=|| ()>
            <ChoiceGroup class_name="create-processor-branch" label=label
                control=ChoiceControl::ProcessorBranch signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
    }
    .into_any()
}
