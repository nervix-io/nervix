//! The model tensors of a visual inferencer.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of an inferencer's ordered `INPUTS`, `STATE` and `OUTPUT SCHEMA`
//!   tensors, each tensor's dense shape axis by axis, the input-field picker of its mappings, and
//!   the execution mode its shapes imply.
//! - **Depends on.** The inferencer draft, typed choice controls and Leptos signals.
//! - **Must not know.** ONNX inspection, tensor conversion or model execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::InferencerExecutionMode;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_textarea_value, event_target_value,
    processor_draft::ProcessorFamily,
    processor_editor::indices,
    processor_inferencer_draft::{
        DimensionKind, InferencerDraft, StateInitialDraft, TensorList, TensorListEdit,
    },
};

/// Applies `edit` to the open inferencer draft as one edit of the form.
fn edit_inferencer(signals: CreateSignals, edit: impl FnOnce(&mut InferencerDraft)) {
    signals
        .processor(ProcessorFamily::Inferencer)
        .update(|draft| {
            if let Some(inferencer) = draft.inferencer_mut() {
                edit(inferencer);
            }
        });
    signals.edit();
}

/// What `read` reads from the open inferencer draft, tracked by the view that reads it.
fn read_inferencer<T: Default>(
    signals: CreateSignals,
    read: impl FnOnce(&InferencerDraft) -> T,
) -> T {
    signals
        .processor(ProcessorFamily::Inferencer)
        .with(|draft| match draft.inferencer() {
            Some(inferencer) => read(inferencer),
            None => T::default(),
        })
}

/// The static presentation of one tensor list's controls.
struct TensorListText {
    section: &'static str,
    heading: &'static str,
    explanation: &'static str,
    list: &'static str,
    entry: &'static str,
    add: &'static str,
    add_label: &'static str,
    up: &'static str,
    down: &'static str,
    remove: &'static str,
    shape: &'static str,
}

fn tensor_list_text(list: TensorList) -> TensorListText {
    match list {
        TensorList::Inputs => TensorListText {
            section: "create-inferencer-inputs",
            heading: "INPUTS",
            explanation: "Each model input is filled by an expression over the live input record, \
                          the one place an inferencer reads it.",
            list: "create-inferencer-input-list",
            entry: "Input tensor",
            add: "create-inferencer-add-input",
            add_label: "Add input tensor",
            up: "create-inferencer-input-up",
            down: "create-inferencer-input-down",
            remove: "create-inferencer-remove-input",
            shape: "create-tensor-shape create-inferencer-input-shape",
        },
        TensorList::State => TensorListText {
            section: "create-inferencer-state",
            heading: "STATE",
            explanation: "A stateful model carries each state tensor from its output to its input \
                          on the next row of the same concrete branch, starting from its INITIAL \
                          value. State tensors have fixed axes, and a stateful inferencer \
                          declares no BATCH axis in any tensor.",
            list: "create-inferencer-state-list",
            entry: "State tensor",
            add: "create-inferencer-add-state",
            add_label: "Add state tensor",
            up: "create-inferencer-state-up",
            down: "create-inferencer-state-down",
            remove: "create-inferencer-remove-state",
            shape: "create-tensor-shape create-inferencer-state-shape",
        },
        TensorList::Outputs => TensorListText {
            section: "create-inferencer-outputs",
            heading: "OUTPUT SCHEMA",
            explanation: "Each model output becomes a generated field of the same name, which the \
                          routes read.",
            list: "create-inferencer-output-list",
            entry: "Output tensor",
            add: "create-inferencer-add-output",
            add_label: "Add output tensor",
            up: "create-inferencer-output-up",
            down: "create-inferencer-output-down",
            remove: "create-inferencer-remove-output",
            shape: "create-tensor-shape create-inferencer-output-shape",
        },
    }
}

/// The ordered tensors of one list: which one is edited, and adding, moving and removing them.
#[component]
fn TensorListControls(list: TensorList, signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let text = tensor_list_text(list);
    let count = move || read_inferencer(signals, |inferencer| inferencer.tensor_count(list));
    let active = move || read_inferencer(signals, |inferencer| inferencer.active_tensor(list));
    view! {
        <div class=text.list>
            <For each=move || indices(count()) key=|index| *index
                children=move |index| view! {
                    <button type="button" data-tensor-index=index.to_string()
                        class:active=move || active() == index
                        disabled=pending
                        on:click=move |_| edit_inferencer(signals, |inferencer| inferencer.edit_tensors(list, TensorListEdit::Select(index)))>
                        {format!("{} {}", text.entry, index + 1)}
                    </button>
                } />
            <button type="button" class=text.add disabled=pending
                on:click=move |_| edit_inferencer(signals, |inferencer| inferencer.edit_tensors(list, TensorListEdit::Add))>{text.add_label}</button>
        </div>
        <Show when=move || { count() > 0 } fallback=|| ()>
            <div class="create-inferencer-tensor-actions">
                <button type="button" class=text.up
                    disabled=move || { pending() || active() == 0 }
                    on:click=move |_| edit_inferencer(signals, |inferencer| inferencer.edit_tensors(list, TensorListEdit::MoveActiveUp))>"Move up"</button>
                <button type="button" class=text.down
                    disabled=move || { pending() || active() + 1 >= count() }
                    on:click=move |_| edit_inferencer(signals, |inferencer| inferencer.edit_tensors(list, TensorListEdit::MoveActiveDown))>"Move down"</button>
                <button type="button" class=text.remove
                    disabled=move || { pending() || (list.required() && count() <= 1) }
                    on:click=move |_| edit_inferencer(signals, |inferencer| inferencer.edit_tensors(list, TensorListEdit::RemoveActive))>"Remove tensor"</button>
            </div>
        </Show>
    }
    .into_any()
}

/// The dense shape of the tensor of `list` being edited, axis by axis.
#[component]
fn TensorShapeEditor(list: TensorList, signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let text = tensor_list_text(list);
    let kind = move |axis: usize| {
        read_inferencer(signals, |inferencer| match inferencer.active_shape(list) {
            Some(shape) => shape.dimension_kind(axis),
            None => None,
        })
    };
    let choose = move |axis: usize, chosen: DimensionKind| {
        edit_inferencer(signals, |inferencer| {
            if let Some(shape) = inferencer.active_shape_mut(list) {
                shape.set_kind(axis, chosen);
            }
        });
    };
    let section = view! {
        <fieldset class=text.shape>
            <legend>"Dense tensor shape"</legend>
            <p class="create-tensor-type">{move || read_inferencer(signals, |inferencer| match inferencer.active_shape(list) {
                Some(shape) => format!(
                    "{} TENSOR<{}>: an inferencer tensor is dense, of {} elements. Each axis has a fixed size, is DYNAMIC, or is the BATCH axis of a batched model.",
                    shape.representation.as_ref(),
                    shape.element_type.as_ref(),
                    shape.element_type.as_ref(),
                ),
                None => String::new(),
            })}</p>
            <div class="create-tensor-dimensions">
                <For each=move || indices(read_inferencer(signals, |inferencer| inferencer.active_axis_count(list))) key=|axis| *axis
                    children=move |axis| view! {
                        <div class="create-tensor-dimension" data-dimension=axis.to_string()>
                            <span>{format!("Axis {}", axis + 1)}</span>
                            <button type="button" data-dimension-kind="size"
                                class:active=move || kind(axis) == Some(DimensionKind::Size)
                                disabled=pending
                                on:click=move |_| choose(axis, DimensionKind::Size)>"Size"</button>
                            <button type="button" data-dimension-kind="dynamic"
                                class:active=move || kind(axis) == Some(DimensionKind::Dynamic)
                                disabled=pending
                                on:click=move |_| choose(axis, DimensionKind::Dynamic)>"DYNAMIC"</button>
                            <button type="button" data-dimension-kind="batch"
                                class:active=move || kind(axis) == Some(DimensionKind::Batch)
                                disabled=pending
                                on:click=move |_| choose(axis, DimensionKind::Batch)>"BATCH"</button>
                            <Show when=move || kind(axis) == Some(DimensionKind::Size) fallback=|| ()>
                                <input class="create-tensor-dimension-size" type="text" inputmode="numeric" autocomplete="off"
                                    aria-label=format!("Axis {} size", axis + 1)
                                    prop:value=move || read_inferencer(signals, |inferencer| match inferencer.active_shape(list) {
                                        Some(shape) => shape.size_text(axis),
                                        None => String::new(),
                                    })
                                    disabled=pending
                                    on:input=move |event| {
                                        let value = event_target_value(&event);
                                        edit_inferencer(signals, |inferencer| {
                                            if let Some(shape) = inferencer.active_shape_mut(list) { shape.set_size(axis, value); }
                                        });
                                    } />
                            </Show>
                            <button type="button" class="create-tensor-remove-dimension" disabled=pending
                                on:click=move |_| edit_inferencer(signals, |inferencer| {
                                    if let Some(shape) = inferencer.active_shape_mut(list) { shape.remove_dimension(axis); }
                                })>"Remove axis"</button>
                        </div>
                    } />
            </div>
            <button type="button" class="create-tensor-add-dimension" disabled=pending
                on:click=move |_| edit_inferencer(signals, |inferencer| {
                    if let Some(shape) = inferencer.active_shape_mut(list) { shape.add_dimension(); }
                })>"Add axis"</button>
        </fieldset>
    };
    section.into_any()
}

/// A text field of the tensor of `list` being edited.
#[component]
fn TensorTextField(
    class_name: &'static str,
    label: &'static str,
    read: fn(&InferencerDraft) -> String,
    write: fn(&mut InferencerDraft, String),
    signals: CreateSignals,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field"><span>{label}</span>
            <input class=class_name type="text" autocomplete="off" spellcheck="false"
                prop:value=move || read_inferencer(signals, read)
                disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    edit_inferencer(signals, |inferencer| write(inferencer, value));
                } />
        </label>
    }
    .into_any()
}

fn input_tensor(inferencer: &InferencerDraft) -> String {
    match inferencer.inputs.active() {
        Some(mapping) => mapping.tensor.clone(),
        None => String::new(),
    }
}

fn set_input_tensor(inferencer: &mut InferencerDraft, value: String) {
    if let Some(mapping) = inferencer.inputs.active_mut() {
        mapping.tensor = value;
    }
}

fn state_input(inferencer: &InferencerDraft) -> String {
    match inferencer.state.active() {
        Some(binding) => binding.input.clone(),
        None => String::new(),
    }
}

fn set_state_input(inferencer: &mut InferencerDraft, value: String) {
    if let Some(binding) = inferencer.state.active_mut() {
        binding.input = value;
    }
}

fn state_output(inferencer: &InferencerDraft) -> String {
    match inferencer.state.active() {
        Some(binding) => binding.output.clone(),
        None => String::new(),
    }
}

fn set_state_output(inferencer: &mut InferencerDraft, value: String) {
    if let Some(binding) = inferencer.state.active_mut() {
        binding.output = value;
    }
}

fn output_tensor(inferencer: &InferencerDraft) -> String {
    match inferencer.outputs.active() {
        Some(declaration) => declaration.tensor.clone(),
        None => String::new(),
    }
}

fn set_output_tensor(inferencer: &mut InferencerDraft, value: String) {
    if let Some(declaration) = inferencer.outputs.active_mut() {
        declaration.tensor = value;
    }
}

#[component]
fn InputTensors(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let text = tensor_list_text(TensorList::Inputs);
    let section = view! {
        <fieldset class=text.section>
            <legend>{text.heading}</legend>
            <p>{text.explanation}</p>
            <TensorListControls list=TensorList::Inputs signals=signals />
            <Show when=move || { read_inferencer(signals, |inferencer| inferencer.tensor_count(TensorList::Inputs)) > 0 } fallback=|| ()>
                <TensorTextField class_name="create-inferencer-input-name" label="Model input name"
                    read=input_tensor write=set_input_tensor signals=signals />
                <TensorShapeEditor list=TensorList::Inputs signals=signals />
                <label class="create-field"><span>"Expression over the live input"</span>
                    <textarea class="create-inferencer-input-expression"
                        prop:value=move || read_inferencer(signals, |inferencer| match inferencer.inputs.active() {
                            Some(mapping) => mapping.expression.clone(),
                            None => String::new(),
                        })
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_textarea_value(&event);
                            edit_inferencer(signals, |inferencer| {
                                if let Some(mapping) = inferencer.inputs.active_mut() { mapping.expression = value; }
                            });
                        } />
                </label>
                <ChoiceGroup class_name="create-inferencer-input-fields" label="Insert a live input field"
                    control=ChoiceControl::InferencerInputField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
            </Show>
        </fieldset>
    };
    section.into_any()
}

#[component]
fn StateTensors(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let text = tensor_list_text(TensorList::State);
    let initial = move || {
        read_inferencer(signals, |inferencer| match inferencer.state.active() {
            Some(binding) => binding.initial.clone(),
            None => StateInitialDraft::Unselected,
        })
    };
    let section = view! {
        <fieldset class=text.section>
            <legend>{text.heading}</legend>
            <p>{text.explanation}</p>
            <TensorListControls list=TensorList::State signals=signals />
            <Show when=move || { read_inferencer(signals, |inferencer| inferencer.tensor_count(TensorList::State)) > 0 } fallback=|| ()>
                <TensorTextField class_name="create-inferencer-state-input" label="Model state input"
                    read=state_input write=set_state_input signals=signals />
                <TensorTextField class_name="create-inferencer-state-output" label="Model state output it comes FROM"
                    read=state_output write=set_state_output signals=signals />
                <TensorShapeEditor list=TensorList::State signals=signals />
                <fieldset class="create-field create-inferencer-state-initial">
                    <legend>"INITIAL"</legend>
                    <button type="button" data-initial="zeros"
                        class:active=move || initial() == StateInitialDraft::Zeros
                        disabled=pending
                        on:click=move |_| edit_inferencer(signals, |inferencer| {
                            if let Some(binding) = inferencer.state.active_mut() { binding.initial = StateInitialDraft::Zeros; }
                        })>"ZEROS"</button>
                    <button type="button" data-initial="constant"
                        class:active=move || matches!(initial(), StateInitialDraft::Constant(_))
                        disabled=pending
                        on:click=move |_| edit_inferencer(signals, |inferencer| {
                            if let Some(binding) = inferencer.state.active_mut()
                                && !matches!(binding.initial, StateInitialDraft::Constant(_)) {
                                binding.initial = StateInitialDraft::Constant(String::new());
                            }
                        })>"Constant"</button>
                </fieldset>
                <Show when=move || matches!(initial(), StateInitialDraft::Constant(_)) fallback=|| ()>
                    <label class="create-field"><span>"Constant of exactly the declared type and shape, such as [0 AS F32, 1 AS F32]"</span>
                        <textarea class="create-inferencer-state-constant"
                            prop:value=move || match initial() {
                                StateInitialDraft::Constant(text) => text,
                                StateInitialDraft::Unselected | StateInitialDraft::Zeros => String::new(),
                            }
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_textarea_value(&event);
                                edit_inferencer(signals, |inferencer| {
                                    if let Some(binding) = inferencer.state.active_mut() {
                                        binding.initial = StateInitialDraft::Constant(value);
                                    }
                                });
                            } />
                    </label>
                </Show>
            </Show>
        </fieldset>
    };
    section.into_any()
}

#[component]
fn OutputTensors(signals: CreateSignals) -> impl IntoView {
    let text = tensor_list_text(TensorList::Outputs);
    let section = view! {
        <fieldset class=text.section>
            <legend>{text.heading}</legend>
            <p>{text.explanation}</p>
            <TensorListControls list=TensorList::Outputs signals=signals />
            <Show when=move || { read_inferencer(signals, |inferencer| inferencer.tensor_count(TensorList::Outputs)) > 0 } fallback=|| ()>
                <TensorTextField class_name="create-inferencer-output-name" label="Model output name"
                    read=output_tensor write=set_output_tensor signals=signals />
                <TensorShapeEditor list=TensorList::Outputs signals=signals />
            </Show>
        </fieldset>
    };
    section.into_any()
}

/// The execution mode the tensor shapes imply, as the inferencer Model's rule derives it.
fn execution_mode_text(inferencer: &InferencerDraft) -> String {
    match inferencer.execution_mode() {
        None => "Execution mode: size every axis of every tensor to see it".to_string(),
        Some(Ok(InferencerExecutionMode::PerMessage)) => "Execution mode: per message, invoking \
                                                          the model once for each admitted message"
            .to_string(),
        Some(Ok(InferencerExecutionMode::Batched)) => {
            "Execution mode: batched, each tensor's BATCH axis carrying the messages of one batch"
                .to_string()
        }
        Some(Err(refusal)) => format!("Execution mode refused: {refusal:#}"),
    }
}

#[component]
pub(super) fn ProcessorTensorEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let section = view! {
        <section class="create-inferencer-tensors">
            <h3>"Model tensors"</h3>
            <p class="create-inferencer-execution-mode" role="status">{move || read_inferencer(signals, execution_mode_text)}</p>
            <InputTensors signals=signals request_tx=request_tx session_generation=session_generation />
            <StateTensors signals=signals />
            <OutputTensors signals=signals />
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}
