//! Structured controls for every current codec wire format.
//!
//! Layer: edges.
//!
//! - **Owns.** Codec format, schema, direction, jaq, Protobuf, and field encoding controls.
//! - **Depends on.** Browser drafts and the shared typed choice presentation.
//! - **Must not know.** Codec compilation, runtime encode/decode, or registry internals.

use futures_channel::mpsc::UnboundedSender;
use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, ConsoleRequest, CreateSignals,
    codec_draft::{CodecFormatDraft, CodecFormatKind, EncodingRuleDraft},
    event_target_checked, event_target_textarea_value, event_target_value,
    resource_binding_editor::{ResourceBindingEditor, ResourceBindingForm},
};

#[component]
pub(super) fn CodecEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <label class="create-field">
            <span>"Codec name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.codec.get().name disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    signals.codec.update(|draft| draft.name = value);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-codec-format">
            <legend>"Wire format"</legend>
            <div class="create-choice-buttons">
                <For each=move || CodecFormatKind::ALL.to_vec()
                    key=|kind| kind.key()
                    children=move |kind| view! {
                        <button type="button" data-format=kind.key()
                            class:active=move || signals.codec.get().format.as_ref().is_some_and(|format| format.kind() == kind)
                            disabled=pending
                            on:click=move |_| {
                                signals.codec.update(|draft| draft.set_format(kind));
                                signals.edit();
                            }>{kind.label()}</button>
                    } />
            </div>
        </fieldset>
        <ChoiceGroup class_name="create-codec-schema" label="Internal schema"
            control=ChoiceControl::CodecSchema signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || matches!(signals.codec.get().format, Some(CodecFormatDraft::Wire { .. })) fallback=|| ()>
            <ChoiceGroup class_name="create-codec-wire-schema" label="Wire schema"
                control=ChoiceControl::CodecWireSchema signals=signals request_tx=request_tx
                session_generation=session_generation />
        </Show>
        <Show when=move || matches!(signals.codec.get().format, Some(CodecFormatDraft::Protobuf { .. })) fallback=|| ()>
            <ResourceBindingEditor signals=signals kind=ResourceBindingForm::Codec
                request_tx=request_tx session_generation=session_generation />
            <label class="create-field"><span>"Protobuf message type"</span>
                <input class="create-codec-message" type="text"
                    prop:value=move || match signals.codec.get().format {
                        Some(CodecFormatDraft::Protobuf { message, .. }) => message,
                        _ => String::new(),
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        signals.codec.update(|draft| {
                            if let Some(CodecFormatDraft::Protobuf { message, .. }) = &mut draft.format { *message = value; }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-field"><span>"Batch message type · optional"</span>
                <input class="create-codec-batch-message" type="text"
                    prop:value=move || match signals.codec.get().format {
                        Some(CodecFormatDraft::Protobuf { batch_message, .. }) => batch_message,
                        _ => String::new(),
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        signals.codec.update(|draft| {
                            if let Some(CodecFormatDraft::Protobuf { batch_message, .. }) = &mut draft.format { *batch_message = value; }
                        });
                        signals.edit();
                    } />
            </label>
        </Show>
        <Show when=move || signals.codec.get().transformations().is_some() fallback=|| ()>
            <fieldset class="create-choice-group create-codec-directions">
                <legend>"Jaq transformations and direction"</legend>
                <label class="create-check"><input class="create-codec-ingestion-enabled" type="checkbox"
                    prop:checked=move || signals.codec.get().transformations().is_some_and(|value| value.ingestion.is_some())
                    disabled=pending
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.codec.update(|draft| {
                            if let Some(value) = draft.transformations_mut() {
                                value.ingestion = if checked { Some(String::new()) } else { None };
                            }
                        });
                        signals.edit();
                    } /><span>"On ingestion · decode"</span></label>
                <Show when=move || signals.codec.get().transformations().is_some_and(|value| value.ingestion.is_some()) fallback=|| ()>
                    <label class="create-field"><span>"Ingestion jaq program"</span>
                        <textarea class="create-codec-ingestion-program" spellcheck="false"
                            prop:value=move || signals.codec.get().transformations().and_then(|value| value.ingestion.clone()).unwrap_or_default()
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_textarea_value(&event);
                                signals.codec.update(|draft| {
                                    if let Some(transforms) = draft.transformations_mut() { transforms.ingestion = Some(value); }
                                });
                                signals.edit();
                            }></textarea>
                    </label>
                </Show>
                <label class="create-check"><input class="create-codec-emitting-enabled" type="checkbox"
                    prop:checked=move || signals.codec.get().transformations().is_some_and(|value| value.emitting.is_some())
                    disabled=pending
                    on:change=move |event| {
                        let checked = event_target_checked(&event);
                        signals.codec.update(|draft| {
                            if let Some(value) = draft.transformations_mut() {
                                value.emitting = if checked { Some(String::new()) } else { None };
                                if !checked { value.emitting_batch = None; }
                            }
                        });
                        signals.edit();
                    } /><span>"On emitting · encode"</span></label>
                <Show when=move || signals.codec.get().transformations().is_some_and(|value| value.emitting.is_some()) fallback=|| ()>
                    <label class="create-field"><span>"Emitting jaq program"</span>
                        <textarea class="create-codec-emitting-program" spellcheck="false"
                            prop:value=move || signals.codec.get().transformations().and_then(|value| value.emitting.clone()).unwrap_or_default()
                            disabled=pending
                            on:input=move |event| {
                                let value = event_target_textarea_value(&event);
                                signals.codec.update(|draft| {
                                    if let Some(transforms) = draft.transformations_mut() { transforms.emitting = Some(value); }
                                });
                                signals.edit();
                            }></textarea>
                    </label>
                    <label class="create-check"><input class="create-codec-batch-enabled" type="checkbox"
                        prop:checked=move || signals.codec.get().transformations().is_some_and(|value| value.emitting_batch.is_some())
                        disabled=pending
                        on:change=move |event| {
                            let checked = event_target_checked(&event);
                            signals.codec.update(|draft| {
                                if let Some(value) = draft.transformations_mut() {
                                    value.emitting_batch = if checked { Some(String::new()) } else { None };
                                }
                            });
                            signals.edit();
                        } /><span>"On emitting batch · replace the format container"</span></label>
                    <Show when=move || signals.codec.get().transformations().is_some_and(|value| value.emitting_batch.is_some()) fallback=|| ()>
                        <label class="create-field"><span>"Batch jaq program"</span>
                            <textarea class="create-codec-batch-program" spellcheck="false"
                                prop:value=move || signals.codec.get().transformations().and_then(|value| value.emitting_batch.clone()).unwrap_or_default()
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_textarea_value(&event);
                                    signals.codec.update(|draft| {
                                        if let Some(transforms) = draft.transformations_mut() { transforms.emitting_batch = Some(value); }
                                    });
                                    signals.edit();
                                }></textarea>
                        </label>
                    </Show>
                </Show>
            </fieldset>
        </Show>
        <Show when=move || signals.codec.get().format.as_ref().is_some_and(|format| format.kind() != CodecFormatKind::Syslog) fallback=|| ()>
            <div class="create-codec-encoding">
                <p>"Field encoding rules"</p>
                <For each=move || { (0..signals.codec.get().encoding_rules.len()).collect::<Vec<_>>() }
                    key=|index| *index
                    children=move |index| view! {
                        <div class="create-codec-encoding-entry create-field-row">
                            <label class="create-field"><span>"Field"</span>
                                <input class="create-codec-encoding-field" type="text"
                                    prop:value=move || signals.codec.get().encoding_rules.get(index).map(|rule| rule.field.clone()).unwrap_or_default()
                                    disabled=pending
                                    on:input=move |event| {
                                        let value = event_target_value(&event);
                                        signals.codec.update(|draft| {
                                            if let Some(rule) = draft.encoding_rules.get_mut(index) { rule.field = value; }
                                        });
                                        signals.edit();
                                    } />
                            </label>
                            <span>"AS RFC3339"</span>
                            <button type="button" class="create-codec-encoding-remove" disabled=pending
                                on:click=move |_| {
                                    signals.codec.update(|draft| {
                                        if index < draft.encoding_rules.len() { draft.encoding_rules.remove(index); }
                                    });
                                    signals.edit();
                                }>"Remove"</button>
                        </div>
                    } />
                <button type="button" class="create-codec-encoding-add" disabled=pending
                    on:click=move |_| {
                        signals.codec.update(|draft| draft.encoding_rules.push(EncodingRuleDraft::default()));
                        signals.edit();
                    }>"Add RFC3339 field"</button>
            </div>
        </Show>
    }
}
