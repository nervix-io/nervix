//! Ordered materialized dependencies of a visual emitter.
//!
//! Layer: edges.
//! - **Owns.** State ordering, explicit absence policies and default-field editing.
//! - **Depends on.** Shared dependency drafts and all-input-compatible relay questions.
//! - **Must not know.** Materialized storage or branch execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter state controls")
)]

use leptos::prelude::*;
use meticulous::OptionExt as _;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender,
    emitter_rows::{AssignmentArea, AssignmentRows, OrderButtons, move_item},
    processor_draft::{StateDependencyDraft, StatePolicyDraft},
};

#[component]
pub(super) fn EmitterStateEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-emitter-state"><h3>"Ordered materialized dependencies"</h3>
            <For each={move || (0..signals.emitter.get().state.len()).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                <button type="button" data-state=index.to_string() class:active=move || signals.emitter.get().active_state == index disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.active_state = index); signals.edit(); }>{format!("Dependency {}", index.checked_add(1).assured("an allocated dependency index is below usize::MAX"))}</button>
            } />
            <button type="button" class="create-emitter-add-state" disabled=pending on:click=move |_| { signals.emitter.update(|draft| { draft.state.push(StateDependencyDraft::default()); draft.active_state = draft.state.len().checked_sub(1).assured("a dependency was just appended"); }); signals.edit(); }>"Add materialized state"</button>
            <Show when=move || !signals.emitter.get().state.is_empty() fallback=|| ()>
                <OrderButtons signals=signals index=Signal::derive(move || signals.emitter.get().active_state) len=Signal::derive(move || signals.emitter.get().state.len())
                    move_row=Callback::new(move |(index, up): (usize, bool)| signals.emitter.update(|draft| { let other = if up { index.checked_sub(1) } else { index.checked_add(1) }; if let Some(other) = other && other < draft.state.len() && index < draft.state.len() { move_item(&mut draft.state, index, up); draft.active_state = other; } }))
                    remove_row=Callback::new(move |index| signals.emitter.update(|draft| { if index < draft.state.len() { draft.state.remove(index); draft.active_state = match draft.state.len().checked_sub(1) { Some(last) => index.min(last), None => 0 }; } })) />
                <ChoiceGroup class_name="create-emitter-state-relay" label="Materialized relay" control=ChoiceControl::EmitterStateRelay signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                <fieldset class="create-field create-emitter-state-policy"><legend>"When state is absent"</legend>
                    <button type="button" data-state-policy="skip" disabled=pending on:click=move |_| { signals.emitter.update(|draft| { if let Some(state) = draft.state.get_mut(draft.active_state) { state.policy = StatePolicyDraft::RequiredSkip; } }); signals.edit(); }>"REQUIRED SKIP"</button>
                    <button type="button" data-state-policy="wait" disabled=pending on:click=move |_| { signals.emitter.update(|draft| { if let Some(state) = draft.state.get_mut(draft.active_state) { state.policy = StatePolicyDraft::RequiredWait; } }); signals.edit(); }>"REQUIRED WAIT"</button>
                    <button type="button" data-state-policy="default" disabled=pending on:click=move |_| { signals.emitter.update(|draft| { if let Some(state) = draft.state.get_mut(draft.active_state) { state.policy = StatePolicyDraft::Default(Vec::new()); } }); signals.edit(); }>"DEFAULT"</button>
                </fieldset>
                <Show when=move || signals.emitter.with(|draft| draft.state.get(draft.active_state).is_some_and(|state| matches!(state.policy, StatePolicyDraft::Default(_)))) fallback=|| ()>
                    <ChoiceGroup class_name="create-emitter-state-field" label="Add default field" control=ChoiceControl::EmitterStateField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                    <AssignmentRows signals=signals area=AssignmentArea::State />
                </Show>
            </Show>
        </section>
    }
}
