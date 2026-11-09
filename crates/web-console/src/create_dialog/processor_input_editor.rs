//! Ordered processor inputs and their node-wide collection and branch contract.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of relay input order, source predicates, collection, and the
//!   node-wide branch selection of junctions, deduplicators and reorderers.
//! - **Depends on.** Shared processor drafts and typed choice controls.
//! - **Must not know.** Runtime input collection or registry implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::InputCollectPolicy;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_checked, event_target_textarea_value, event_target_value,
    processor_draft::{NodeBranchDraft, ProcessorFamily},
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
    let node_branch = family.node_branch_label().map(|label| {
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
    });
    let section = view! {
        <section class="create-processor-inputs">
            <h3>"Ordered input relays"</h3>
            {node_branch}
            <div class="create-processor-input-list">
                <For each=move || indices(draft.get().from.relays.len()) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-input=index.to_string()
                            class:active=move || draft.get().from.active == index
                            disabled=pending
                            on:click=move |_| { draft.update(|state| state.from.active = index); signals.edit(); }>
                            {format!("Input {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-processor-add-input" disabled=pending
                    on:click=move |_| { draft.update(|state| state.from.add()); signals.edit(); }>"Add input"</button>
            </div>
            <div class="create-processor-input-actions">
                <button type="button" class="create-processor-input-up"
                    disabled=move || { pending() || draft.get().from.active == 0 }
                    on:click=move |_| { draft.update(|state| state.from.move_active(true)); signals.edit(); }>"Move up"</button>
                <button type="button" class="create-processor-input-down"
                    disabled=move || { pending() || draft.get().from.active + 1 >= draft.get().from.relays.len() }
                    on:click=move |_| { draft.update(|state| state.from.move_active(false)); signals.edit(); }>"Move down"</button>
                <button type="button" class="create-processor-remove-input"
                    disabled=move || { pending() || draft.get().from.relays.len() == 1 }
                    on:click=move |_| { draft.update(|state| state.from.remove_active()); signals.edit(); }>"Remove input"</button>
            </div>
            <ChoiceGroup class_name="create-processor-input" label="Input relay"
                control=ChoiceControl::ProcessorInputRelay signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <label class="create-field"><span>"Input WHERE expression (optional)"</span>
                <textarea class="create-processor-input-where"
                    prop:value=move || {
                        let state = draft.get();
                        match state.from.active() {
                            Some(input) => input.where_clause.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_textarea_value(&event);
                        draft.update(|state| {
                            if let Some(input) = state.from.active_mut() { input.where_clause = value; }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-check"><input type="checkbox" class="create-processor-collect"
                prop:checked=move || draft.get().from.collect.is_some()
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    draft.update(|state| {
                        state.from.collect = if checked {
                            Some(InputCollectPolicy { collect_for: String::new(), max_batch_size: None })
                        } else { None };
                    });
                    signals.edit();
                } />"COLLECT FOR input batches"</label>
            <Show when=move || draft.get().from.collect.is_some() fallback=|| ()>
                <label class="create-field"><span>"Collect duration"</span>
                    <input class="create-processor-collect-for" type="text" autocomplete="off"
                        prop:value=move || match draft.get().from.collect {
                            Some(policy) => policy.collect_for,
                            None => String::new(),
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            draft.update(|state| {
                                if let Some(collect) = &mut state.from.collect { collect.collect_for = value; }
                            });
                            signals.edit();
                        } />
                </label>
                <label class="create-field"><span>"Maximum collected batch size (optional)"</span>
                    <input class="create-processor-collect-size" type="text" autocomplete="off"
                        prop:value=move || match draft.get().from.collect {
                            Some(policy) => policy.max_batch_size.unwrap_or_default(),
                            None => String::new(),
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            draft.update(|state| {
                                if let Some(collect) = &mut state.from.collect { collect.max_batch_size = Some(value); }
                            });
                            signals.edit();
                        } />
                </label>
            </Show>
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}
