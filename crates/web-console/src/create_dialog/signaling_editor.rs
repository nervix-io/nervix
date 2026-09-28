//! Ordered signaling handshake controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser controls for signaling wire format and connect/send/wait/fail/capture.
//! - **Depends on.** The signaling browser draft and shared resource choices.
//! - **Must not know.** WebSocket frames, handshake execution, or registry internals.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;

use super::{
    ConsoleRequest, CreateSignals, event_target_checked, event_target_textarea_value,
    event_target_value,
    resource_binding_editor::{ResourceBindingEditor, ResourceBindingForm},
    signaling_draft::{
        SignalingDraft, SignalingFormatDraft, SignalingFormatKind, SignalingStepDraft,
    },
};

#[derive(Clone, Copy)]
enum ProgramsAt {
    ProtocolFail,
    Send(usize),
    Wait(usize),
    WaitFail(usize),
}

impl ProgramsAt {
    fn in_draft(self, draft: &SignalingDraft) -> Option<&Vec<String>> {
        match self {
            Self::ProtocolFail => Some(&draft.fail_matchers),
            Self::Send(index) => match draft.steps.get(index) {
                Some(SignalingStepDraft::Send { programs }) => Some(programs),
                _ => None,
            },
            Self::Wait(index) => match draft.steps.get(index) {
                Some(SignalingStepDraft::Wait { matchers, .. }) => Some(matchers),
                _ => None,
            },
            Self::WaitFail(index) => match draft.steps.get(index) {
                Some(SignalingStepDraft::Wait { fail_matchers, .. }) => Some(fail_matchers),
                _ => None,
            },
        }
    }

    fn in_draft_mut(self, draft: &mut SignalingDraft) -> Option<&mut Vec<String>> {
        match self {
            Self::ProtocolFail => Some(&mut draft.fail_matchers),
            Self::Send(index) => match draft.steps.get_mut(index) {
                Some(SignalingStepDraft::Send { programs }) => Some(programs),
                _ => None,
            },
            Self::Wait(index) => match draft.steps.get_mut(index) {
                Some(SignalingStepDraft::Wait { matchers, .. }) => Some(matchers),
                _ => None,
            },
            Self::WaitFail(index) => match draft.steps.get_mut(index) {
                Some(SignalingStepDraft::Wait { fail_matchers, .. }) => Some(fail_matchers),
                _ => None,
            },
        }
    }
}

#[component]
fn ProgramListEditor(
    signals: CreateSignals,
    at: ProgramsAt,
    label: &'static str,
    input_class: &'static str,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <div class=format!("create-signaling-program-list {input_class}-list")>
            <p>{label}</p>
            <For each=move || {
                    let draft = signals.signaling.get();
                    let len = at.in_draft(&draft).map(Vec::len).unwrap_or(0);
                    (0..len).collect::<Vec<_>>()
                }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-signaling-program-row">
                        <label class="create-field"><span>{format!("{label} {}", index + 1)}</span>
                            <textarea class=input_class spellcheck="false"
                                prop:value=move || {
                                    let draft = signals.signaling.get();
                                    at.in_draft(&draft).and_then(|programs| programs.get(index)).cloned().unwrap_or_default()
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_textarea_value(&event);
                                    signals.signaling.update(|draft| {
                                        if let Some(programs) = at.in_draft_mut(draft)
                                            && let Some(program) = programs.get_mut(index)
                                        { *program = value; }
                                    });
                                    signals.edit();
                                }></textarea>
                        </label>
                        <button class="create-signaling-program-remove" type="button" disabled=pending
                            on:click=move |_| {
                                signals.signaling.update(|draft| {
                                    if let Some(programs) = at.in_draft_mut(draft)
                                        && index < programs.len()
                                    { programs.remove(index); }
                                });
                                signals.edit();
                            }>"Remove"</button>
                    </div>
                } />
            <button class="create-signaling-program-add" type="button" disabled=pending
                on:click=move |_| {
                    signals.signaling.update(|draft| {
                        if let Some(programs) = at.in_draft_mut(draft) { programs.push(String::new()); }
                    });
                    signals.edit();
                }>{format!("Add {label}")}</button>
        </div>
    }
}

#[component]
fn SignalingStepEditor(signals: CreateSignals, index: usize) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <div class="create-signaling-step">
            <div class="create-signaling-step-head">
                <strong>{move || match signals.signaling.get().steps.get(index) {
                    Some(SignalingStepDraft::Send { .. }) => format!("{}. SEND", index + 1),
                    Some(SignalingStepDraft::Wait { .. }) => format!("{}. WAIT", index + 1),
                    None => String::new(),
                }}</strong>
                <button type="button" class="create-signaling-step-up"
                    aria-label=format!("Move step {} up", index + 1)
                    disabled=move || { pending() || index == 0 }
                    on:click=move |_| { signals.signaling.update(|draft| draft.move_step_up(index)); signals.edit(); }>"↑"</button>
                <button type="button" class="create-signaling-step-down"
                    aria-label=format!("Move step {} down", index + 1)
                    disabled=move || { pending() || index + 1 >= signals.signaling.get().steps.len() }
                    on:click=move |_| { signals.signaling.update(|draft| draft.move_step_down(index)); signals.edit(); }>"↓"</button>
                <button type="button" class="create-signaling-step-remove" disabled=pending
                    on:click=move |_| {
                        signals.signaling.update(|draft| { if index < draft.steps.len() { draft.steps.remove(index); } });
                        signals.edit();
                    }>"Remove step"</button>
            </div>
            <Show when=move || matches!(signals.signaling.get().steps.get(index), Some(SignalingStepDraft::Send { .. })) fallback=|| ()>
                <ProgramListEditor signals=signals at=ProgramsAt::Send(index)
                    label="SEND jaq" input_class="create-signaling-send-program" />
            </Show>
            <Show when=move || matches!(signals.signaling.get().steps.get(index), Some(SignalingStepDraft::Wait { .. })) fallback=|| ()>
                <ProgramListEditor signals=signals at=ProgramsAt::Wait(index)
                    label="WAIT matcher" input_class="create-signaling-wait-matcher" />
                <ProgramListEditor signals=signals at=ProgramsAt::WaitFail(index)
                    label="WAIT failure matcher" input_class="create-signaling-wait-fail" />
                <label class="create-check"><input class="create-signaling-capture-enabled" type="checkbox"
                    prop:checked=move || matches!(signals.signaling.get().steps.get(index), Some(SignalingStepDraft::Wait { capture: Some(_), .. }))
                    disabled=pending
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.signaling.update(|draft| {
                            if let Some(SignalingStepDraft::Wait { capture, .. }) = draft.steps.get_mut(index) {
                                *capture = if checked { Some(String::new()) } else { None };
                            }
                        });
                        signals.edit();
                    } /><span>"Capture state from this wait"</span></label>
                <Show when=move || matches!(signals.signaling.get().steps.get(index), Some(SignalingStepDraft::Wait { capture: Some(_), .. })) fallback=|| ()>
                    <label class="create-field"><span>"CAPTURE jaq program"</span>
                        <textarea class="create-signaling-capture" spellcheck="false"
                            prop:value=move || match signals.signaling.get().steps.get(index) {
                                Some(SignalingStepDraft::Wait { capture: Some(value), .. }) => value.clone(),
                                _ => String::new(),
                            }
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_textarea_value(&event);
                                signals.signaling.update(|draft| {
                                    if let Some(SignalingStepDraft::Wait { capture, .. }) = draft.steps.get_mut(index) { *capture = Some(value); }
                                });
                                signals.edit();
                            }></textarea>
                    </label>
                </Show>
                <label class="create-check"><input class="create-signaling-accept-data" type="checkbox"
                    prop:checked=move || matches!(signals.signaling.get().steps.get(index), Some(SignalingStepDraft::Wait { accept_data: true, .. }))
                    disabled=pending
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.signaling.update(|draft| {
                            if let Some(SignalingStepDraft::Wait { accept_data, .. }) = draft.steps.get_mut(index) { *accept_data = checked; }
                        });
                        signals.edit();
                    } /><span>"ACCEPT DATA after this wait"</span></label>
            </Show>
        </div>
    }
}

#[component]
pub(super) fn SignalingEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field"><span>"Signaling protocol name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.signaling.get().name disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    signals.signaling.update(|draft| draft.name = value);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-signaling-format">
            <legend>"Wire format"</legend>
            <div class="create-choice-buttons">
                <For each=move || SignalingFormatKind::ALL.to_vec()
                    key=|kind| kind.key()
                    children=move |kind| view! {
                        <button type="button" data-format=kind.key()
                            class:active=move || signals.signaling.get().format.as_ref().is_some_and(|format| format.kind() == kind)
                            disabled=pending
                            on:click=move |_| { signals.signaling.update(|draft| draft.set_format(kind)); signals.edit(); }>
                            {kind.label()}
                        </button>
                    } />
            </div>
        </fieldset>
        <Show when=move || matches!(signals.signaling.get().format, Some(SignalingFormatDraft::Protobuf { .. })) fallback=|| ()>
            <ResourceBindingEditor signals=signals kind=ResourceBindingForm::Signaling
                request_tx=request_tx session_generation=session_generation />
            <label class="create-field"><span>"Protobuf send message type"</span>
                <input class="create-signaling-send-message" type="text"
                    prop:value=move || match signals.signaling.get().format { Some(SignalingFormatDraft::Protobuf { send_message, .. }) => send_message, _ => String::new() }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        signals.signaling.update(|draft| {
                            if let Some(SignalingFormatDraft::Protobuf { send_message, .. }) = &mut draft.format { *send_message = value; }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-field"><span>"Protobuf wait message type"</span>
                <input class="create-signaling-wait-message" type="text"
                    prop:value=move || match signals.signaling.get().format { Some(SignalingFormatDraft::Protobuf { wait_message, .. }) => wait_message, _ => String::new() }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        signals.signaling.update(|draft| {
                            if let Some(SignalingFormatDraft::Protobuf { wait_message, .. }) = &mut draft.format { *wait_message = value; }
                        });
                        signals.edit();
                    } />
            </label>
        </Show>
        <label class="create-check"><input class="create-signaling-connect-accept" type="checkbox"
            prop:checked=move || signals.signaling.get().accept_data disabled=pending
            on:change=move |event| {
                let checked = event_target_checked(&event);
                signals.signaling.update(|draft| draft.accept_data = checked);
                signals.edit();
            } /><span>"ACCEPT DATA when the connection opens"</span></label>
        <div class="create-signaling-steps">
            <h3>"On connect · ordered steps"</h3>
            <For each=move || { (0..signals.signaling.get().steps.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! { <SignalingStepEditor signals=signals index=index /> } />
            <div class="create-field-row">
                <button class="create-signaling-add-send" type="button" disabled=pending
                    on:click=move |_| { signals.signaling.update(|draft| draft.steps.push(SignalingStepDraft::send())); signals.edit(); }>
                    "Add SEND step"
                </button>
                <button class="create-signaling-add-wait" type="button" disabled=pending
                    on:click=move |_| { signals.signaling.update(|draft| draft.steps.push(SignalingStepDraft::wait())); signals.edit(); }>
                    "Add WAIT step"
                </button>
            </div>
        </div>
        <ProgramListEditor signals=signals at=ProgramsAt::ProtocolFail
            label="Protocol failure matcher" input_class="create-signaling-fail" />
        <label class="create-field"><span>"Handshake timeout"</span>
            <input class="create-signaling-timeout" type="text"
                prop:value=move || signals.signaling.get().timeout disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    signals.signaling.update(|draft| draft.timeout = value);
                    signals.edit();
                } />
        </label>
    }
}
