//! One ordered processor input list, its collection, and the node-wide branch contract.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of one input list's relay order, source predicates and collection,
//!   a correlator's `RIGHT FROM` list included, and the node-wide branch selection the primary
//!   list of a branch-preserving processor shows.
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
    processor_draft::{NodeBranchDraft, ProcessorDraft, ProcessorFamily},
    processor_editor::indices,
    processor_input_draft::{InputList, InputListDraft},
};

/// The static presentation of one input list's controls.
struct InputListControls {
    section: &'static str,
    heading: &'static str,
    list: &'static str,
    add: &'static str,
    actions: &'static str,
    up: &'static str,
    down: &'static str,
    remove: &'static str,
    relay: &'static str,
    relay_label: &'static str,
    relay_control: ChoiceControl,
    predicate: &'static str,
    collect: &'static str,
    collect_label: &'static str,
    collect_for: &'static str,
    collect_size: &'static str,
}

fn input_list_controls(list: InputList) -> InputListControls {
    match list {
        InputList::From => primary_list_controls("Ordered input relays"),
        InputList::Left => primary_list_controls("Ordered LEFT FROM relays"),
        InputList::Right => InputListControls {
            section: "create-processor-inputs create-processor-right-inputs",
            heading: "Ordered RIGHT FROM relays",
            list: "create-processor-right-input-list",
            add: "create-processor-right-add-input",
            actions: "create-processor-input-actions create-processor-right-input-actions",
            up: "create-processor-right-input-up",
            down: "create-processor-right-input-down",
            remove: "create-processor-right-remove-input",
            relay: "create-processor-right-input",
            relay_label: "Right relay",
            relay_control: ChoiceControl::ProcessorRightInputRelay,
            predicate: "create-processor-right-input-where",
            collect: "create-processor-right-collect",
            collect_label: "COLLECT FOR right batches",
            collect_for: "create-processor-right-collect-for",
            collect_size: "create-processor-right-collect-size",
        },
    }
}

/// The controls of a processor's primary list: `FROM`, or a correlator's `LEFT FROM`.
fn primary_list_controls(heading: &'static str) -> InputListControls {
    InputListControls {
        section: "create-processor-inputs",
        heading,
        list: "create-processor-input-list",
        add: "create-processor-add-input",
        actions: "create-processor-input-actions",
        up: "create-processor-input-up",
        down: "create-processor-input-down",
        remove: "create-processor-remove-input",
        relay: "create-processor-input",
        relay_label: "Input relay",
        relay_control: ChoiceControl::ProcessorInputRelay,
        predicate: "create-processor-input-where",
        collect: "create-processor-collect",
        collect_label: "COLLECT FOR input batches",
        collect_for: "create-processor-collect-for",
        collect_size: "create-processor-collect-size",
    }
}

/// What `read` reads from the open draft's `list`, or nothing while the draft holds no such list.
fn read_list<T: Default>(
    draft: RwSignal<ProcessorDraft>,
    list: InputList,
    read: impl FnOnce(&InputListDraft) -> T,
) -> T {
    draft.with(|state| match state.input_list(list) {
        Some(inputs) => read(inputs),
        None => T::default(),
    })
}

/// Applies `edit` to the open draft's `list` as one edit of the form.
fn edit_list(
    signals: CreateSignals,
    draft: RwSignal<ProcessorDraft>,
    list: InputList,
    edit: impl FnOnce(&mut InputListDraft),
) {
    draft.update(|state| {
        if let Some(inputs) = state.input_list_mut(list) {
            edit(inputs);
        }
    });
    signals.edit();
}

#[component]
pub(super) fn ProcessorInputEditor(
    family: ProcessorFamily,
    list: InputList,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let controls = input_list_controls(list);
    // The node-wide branch is chosen once, beside the draft's primary list.
    let node_branch_label = if list == family.primary_input_list() {
        family.node_branch_label()
    } else {
        None
    };
    let node_branch = node_branch_label.map(|label| {
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
    let collects = move || read_list(draft, list, |inputs| inputs.collect.is_some());
    let section = view! {
        <section class=controls.section>
            <h3>{controls.heading}</h3>
            {node_branch}
            <div class=controls.list>
                <For each=move || indices(read_list(draft, list, |inputs| inputs.relays.len())) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-input=index.to_string()
                            class:active=move || read_list(draft, list, |inputs| inputs.active == index)
                            disabled=pending
                            on:click=move |_| edit_list(signals, draft, list, |inputs| inputs.active = index)>
                            {format!("Input {}", index + 1)}
                        </button>
                    } />
                <button type="button" class=controls.add disabled=pending
                    on:click=move |_| edit_list(signals, draft, list, InputListDraft::add)>"Add input"</button>
            </div>
            <div class=controls.actions>
                <button type="button" class=controls.up
                    disabled=move || { pending() || read_list(draft, list, |inputs| inputs.active == 0) }
                    on:click=move |_| edit_list(signals, draft, list, |inputs| inputs.move_active(true))>"Move up"</button>
                <button type="button" class=controls.down
                    disabled=move || { pending() || read_list(draft, list, |inputs| inputs.active + 1 >= inputs.relays.len()) }
                    on:click=move |_| edit_list(signals, draft, list, |inputs| inputs.move_active(false))>"Move down"</button>
                <button type="button" class=controls.remove
                    disabled=move || { pending() || read_list(draft, list, |inputs| inputs.relays.len() <= 1) }
                    on:click=move |_| edit_list(signals, draft, list, InputListDraft::remove_active)>"Remove input"</button>
            </div>
            <ChoiceGroup class_name=controls.relay label=controls.relay_label
                control=controls.relay_control signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <label class="create-field"><span>"Input WHERE expression (optional)"</span>
                <textarea class=controls.predicate
                    prop:value=move || read_list(draft, list, |inputs| match inputs.active() {
                        Some(input) => input.where_clause.clone(),
                        None => String::new(),
                    })
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_textarea_value(&event);
                        edit_list(signals, draft, list, |inputs| {
                            if let Some(input) = inputs.active_mut() { input.where_clause = value; }
                        });
                    } />
            </label>
            <label class="create-check"><input type="checkbox" class=controls.collect
                prop:checked=collects
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    edit_list(signals, draft, list, |inputs| {
                        inputs.collect = if checked {
                            Some(InputCollectPolicy { collect_for: String::new(), max_batch_size: None })
                        } else { None };
                    });
                } />{controls.collect_label}</label>
            <Show when=collects fallback=|| ()>
                <label class="create-field"><span>"Collect duration"</span>
                    <input class=controls.collect_for type="text" autocomplete="off"
                        prop:value=move || read_list(draft, list, |inputs| match &inputs.collect {
                            Some(policy) => policy.collect_for.clone(),
                            None => String::new(),
                        })
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            edit_list(signals, draft, list, |inputs| {
                                if let Some(collect) = &mut inputs.collect { collect.collect_for = value; }
                            });
                        } />
                </label>
                <label class="create-field"><span>"Maximum collected batch size (optional)"</span>
                    <input class=controls.collect_size type="text" autocomplete="off"
                        prop:value=move || read_list(draft, list, |inputs| match &inputs.collect {
                            Some(policy) => match &policy.max_batch_size {
                                Some(size) => size.clone(),
                                None => String::new(),
                            },
                            None => String::new(),
                        })
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            edit_list(signals, draft, list, |inputs| {
                                if let Some(collect) = &mut inputs.collect { collect.max_batch_size = Some(value); }
                            });
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
