//! Resource, completed-version, and Protobuf compiler controls for create forms.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser presentation of one selected resource binding and its compiler inputs.
//! - **Depends on.** Shared typed choices and codec or signaling browser drafts.
//! - **Must not know.** Upload execution, resource storage, or version resolution.

use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_value,
    resource_binding_draft::{ConfigEntryDraft, ResourceBindingDraft},
};

#[derive(Clone, Copy)]
pub(super) enum ResourceBindingForm {
    Codec,
    Signaling,
}

fn binding(signals: CreateSignals, kind: ResourceBindingForm) -> Option<ResourceBindingDraft> {
    match kind {
        ResourceBindingForm::Codec => signals.codec.get().binding().cloned(),
        ResourceBindingForm::Signaling => signals.signaling.get().binding().cloned(),
    }
}

fn update_binding(
    signals: CreateSignals,
    kind: ResourceBindingForm,
    change: impl FnOnce(&mut ResourceBindingDraft),
) {
    match kind {
        ResourceBindingForm::Codec => signals.codec.update(|draft| {
            if let Some(binding) = draft.binding_mut() {
                change(binding);
            }
        }),
        ResourceBindingForm::Signaling => signals.signaling.update(|draft| {
            if let Some(binding) = draft.binding_mut() {
                change(binding);
            }
        }),
    }
    signals.edit();
}

#[component]
pub(super) fn ResourceBindingEditor(
    signals: CreateSignals,
    kind: ResourceBindingForm,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let resource_control = match kind {
        ResourceBindingForm::Codec => ChoiceControl::CodecResource,
        ResourceBindingForm::Signaling => ChoiceControl::SignalingResource,
    };
    let version_control = match kind {
        ResourceBindingForm::Codec => ChoiceControl::CodecVersion,
        ResourceBindingForm::Signaling => ChoiceControl::SignalingVersion,
    };
    let pending = move || signals.progress.get().is_pending();
    view! {
        <ChoiceGroup class_name="create-protobuf-resource" label="Existing resource"
            control=resource_control signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || binding(signals, kind).and_then(|binding| binding.resource).is_some() fallback=|| ()>
            <p class="create-selected-resource">{move || {
                let draft = binding(signals, kind).unwrap_or_default();
                match draft.resource {
                    Some(selected) => format!("Selected resource: {}", selected.name()),
                    None => String::new(),
                }
            }}</p>
        </Show>
        <ChoiceGroup class_name="create-protobuf-version" label="Completed version"
            control=version_control signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || binding(signals, kind).and_then(|binding| binding.version).is_some() fallback=|| ()>
            <p class="create-selected-version">{move || {
                let draft = binding(signals, kind).unwrap_or_default();
                match draft.version {
                    Some(selected) => format!("Requested version: {}", selected.name()),
                    None => String::new(),
                }
            }}</p>
        </Show>
        <label class="create-field">
            <span>"Proto file · optional; all .proto files when empty"</span>
            <input class="create-protobuf-file" type="text" autocomplete="off"
                prop:value=move || binding(signals, kind).unwrap_or_default().file
                disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    update_binding(signals, kind, |binding| binding.file = value);
                } />
        </label>
        <label class="create-field">
            <span>"Proto include root · optional"</span>
            <input class="create-protobuf-include" type="text" autocomplete="off"
                prop:value=move || binding(signals, kind).unwrap_or_default().include
                disabled=pending
                on:input=move |event| {
                    let value = event_target_value(&event);
                    update_binding(signals, kind, |binding| binding.include = value);
                } />
        </label>
        <div class="create-config-list">
            <p>"Additional Protobuf compiler configuration"</p>
            <For each=move || { (0..binding(signals, kind).unwrap_or_default().config.len()).collect::<Vec<_>>() }
                key=|index| *index
                children=move |index| view! {
                    <div class="create-config-entry create-field-row">
                        <label class="create-field"><span>"Key"</span>
                            <input class="create-config-key" type="text"
                                prop:value=move || {
                                    let draft = binding(signals, kind).unwrap_or_default();
                                    match draft.config.get(index) {
                                        Some(entry) => entry.key.clone(),
                                        None => String::new(),
                                    }
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    update_binding(signals, kind, |binding| {
                                        if let Some(entry) = binding.config.get_mut(index) { entry.key = value; }
                                    });
                                } />
                        </label>
                        <label class="create-field"><span>"Value"</span>
                            <input class="create-config-value" type="text"
                                prop:value=move || {
                                    let draft = binding(signals, kind).unwrap_or_default();
                                    match draft.config.get(index) {
                                        Some(entry) => entry.value.clone(),
                                        None => String::new(),
                                    }
                                }
                                disabled=pending
                                on:input=move |event| {
                                    let value = event_target_value(&event);
                                    update_binding(signals, kind, |binding| {
                                        if let Some(entry) = binding.config.get_mut(index) { entry.value = value; }
                                    });
                                } />
                        </label>
                        <button type="button" class="create-config-remove" disabled=pending
                            on:click=move |_| update_binding(signals, kind, |binding| {
                                if index < binding.config.len() { binding.config.remove(index); }
                            })>"Remove"</button>
                    </div>
                } />
            <button type="button" class="create-config-add" disabled=pending
                on:click=move |_| update_binding(signals, kind, |binding| binding.config.push(ConfigEntryDraft::default()))>
                "Add configuration"
            </button>
        </div>
    }
}

#[cfg(test)]
mod tests {
    use leptos::prelude::*;

    use super::{ResourceBindingForm, update_binding};
    use crate::create_dialog::{
        CreateSignals, codec_draft::CodecFormatKind, signaling_draft::SignalingFormatKind,
    };

    #[test]
    fn binding_updates_are_sent_to_the_selected_form_only() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals
                .codec
                .update(|draft| draft.set_format(CodecFormatKind::Protobuf));
            signals
                .signaling
                .update(|draft| draft.set_format(SignalingFormatKind::Protobuf));

            update_binding(signals, ResourceBindingForm::Codec, |binding| {
                binding.file = "codec.proto".to_string()
            });
            let codec = signals.codec.get_untracked();
            let signaling = signals.signaling.get_untracked();
            assert_eq!(
                codec.binding().map(|binding| binding.file.as_str()),
                Some("codec.proto")
            );
            assert_eq!(
                signaling.binding().map(|binding| binding.file.as_str()),
                Some("")
            );

            update_binding(signals, ResourceBindingForm::Signaling, |binding| {
                binding.include = "protocol".to_string()
            });
            let codec = signals.codec.get_untracked();
            let signaling = signals.signaling.get_untracked();
            assert_eq!(
                signaling.binding().map(|binding| binding.include.as_str()),
                Some("protocol")
            );
            assert_eq!(
                codec.binding().map(|binding| binding.include.as_str()),
                Some("")
            );
        });
    }
}
