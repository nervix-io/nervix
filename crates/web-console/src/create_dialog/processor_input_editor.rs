//! Ordered processor inputs and their node-wide collection and branch contract.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of relay input order, source predicates, collection, and junction
//!   branch selection.
//! - **Depends on.** Shared processor drafts and typed choice controls.
//! - **Must not know.** Runtime input collection or registry implementation.

use leptos::prelude::*;
use nervix_models::InputCollectPolicy;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_checked, event_target_textarea_value, event_target_value,
    processor_draft::{JunctionBranchDraft, ProcessorFamily},
    processor_editor::indices,
};

#[component]
pub(super) fn ProcessorInputEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-processor-inputs">
            <h3>"Ordered input relays"</h3>
            <Show when=move || family == ProcessorFamily::Junction fallback=|| ()>
                <fieldset class="create-field create-processor-branching">
                    <legend>"Junction branch"</legend>
                    <button type="button" data-branch="unbranched"
                        class:active=move || draft.get().branching == JunctionBranchDraft::Unbranched
                        disabled=pending
                        on:click=move |_| { draft.update(|state| state.choose_unbranched()); signals.edit(); }>"UNBRANCHED"</button>
                    <button type="button" data-branch="named"
                        class:active=move || matches!(draft.get().branching, JunctionBranchDraft::Branched(_))
                        disabled=pending
                        on:click=move |_| { draft.update(|state| state.choose_branched()); signals.edit(); }>"Named branch"</button>
                </fieldset>
                <Show when=move || matches!(draft.get().branching, JunctionBranchDraft::Branched(_)) fallback=|| ()>
                    <ChoiceGroup class_name="create-processor-branch" label="Junction branch"
                        control=ChoiceControl::ProcessorBranch signals=signals request_tx=request_tx
                        session_generation=session_generation show_detail=true />
                </Show>
            </Show>
            <div class="create-processor-input-list">
                <For each=move || indices(draft.get().inputs.len()) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-input=index.to_string()
                            class:active=move || draft.get().active_input == index
                            disabled=pending
                            on:click=move |_| { draft.update(|state| state.active_input = index); signals.edit(); }>
                            {format!("Input {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-processor-add-input" disabled=pending
                    on:click=move |_| { draft.update(|state| state.add_input()); signals.edit(); }>"Add input"</button>
            </div>
            <div class="create-processor-input-actions">
                <button type="button" class="create-processor-input-up"
                    disabled=move || { pending() || draft.get().active_input == 0 }
                    on:click=move |_| { draft.update(|state| state.move_input(true)); signals.edit(); }>"Move up"</button>
                <button type="button" class="create-processor-input-down"
                    disabled=move || { pending() || draft.get().active_input + 1 >= draft.get().inputs.len() }
                    on:click=move |_| { draft.update(|state| state.move_input(false)); signals.edit(); }>"Move down"</button>
                <button type="button" class="create-processor-remove-input"
                    disabled=move || { pending() || draft.get().inputs.len() == 1 }
                    on:click=move |_| { draft.update(|state| state.remove_input()); signals.edit(); }>"Remove input"</button>
            </div>
            <ChoiceGroup class_name="create-processor-input" label="Input relay"
                control=ChoiceControl::ProcessorInputRelay signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <label class="create-field"><span>"Input WHERE expression (optional)"</span>
                <textarea class="create-processor-input-where"
                    prop:value=move || {
                        let state = draft.get();
                        match state.active_input() {
                            Some(input) => input.where_clause.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_textarea_value(&event);
                        draft.update(|state| {
                            if let Some(input) = state.active_input_mut() { input.where_clause = value; }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-check"><input type="checkbox" class="create-processor-collect"
                prop:checked=move || draft.get().collect.is_some()
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    draft.update(|state| {
                        state.collect = if checked {
                            Some(InputCollectPolicy { collect_for: String::new(), max_batch_size: None })
                        } else { None };
                    });
                    signals.edit();
                } />"COLLECT FOR input batches"</label>
            <Show when=move || draft.get().collect.is_some() fallback=|| ()>
                <label class="create-field"><span>"Collect duration"</span>
                    <input class="create-processor-collect-for" type="text" autocomplete="off"
                        prop:value=move || match draft.get().collect {
                            Some(policy) => policy.collect_for,
                            None => String::new(),
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            draft.update(|state| {
                                if let Some(collect) = &mut state.collect { collect.collect_for = value; }
                            });
                            signals.edit();
                        } />
                </label>
                <label class="create-field"><span>"Maximum collected batch size (optional)"</span>
                    <input class="create-processor-collect-size" type="text" autocomplete="off"
                        prop:value=move || match draft.get().collect {
                            Some(policy) => policy.max_batch_size.unwrap_or_default(),
                            None => String::new(),
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            draft.update(|state| {
                                if let Some(collect) = &mut state.collect { collect.max_batch_size = Some(value); }
                            });
                            signals.edit();
                        } />
                </label>
            </Show>
        </section>
    }
}
