//! Shared typed choices in the visual create dialog.
//!
//! Layer: edges.
//!
//! - **Owns.** Typed choice rendering, selection display, and applying selections to drafts.
//! - **Depends on.** Create dialog signals and the client choice contract.
//! - **Must not know.** Server choice resolution, browser transport, or registry internals.

use leptos::prelude::*;
use nervix_client_wire::ChoiceValue;
use nervix_models::ModelKind;

use super::{
    ChoiceControl, ChoiceControlSignals, ChoiceLoad, CodecFormatDraft, CreateSignals,
    RequestSender, event_target_value, request_choices,
};

#[component]
pub(super) fn ChoiceGroup(
    class_name: &'static str,
    label: &'static str,
    control: ChoiceControl,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
    /// Shows each choice's detail beside its label, as a typed field shows its type.
    #[prop(optional)]
    show_detail: bool,
) -> impl IntoView {
    let ChoiceControlSignals { search, load } = signals.choices.of(control);
    view! {
        <fieldset class=format!("create-choice-group {class_name}")>
            <legend>{label}</legend>
            <label class="create-choice-search-label">
                <span class="sr-only">{format!("Search {label}")}</span>
                <input
                    class="create-choice-search"
                    type="search"
                    placeholder=format!("Search {}", label.to_ascii_lowercase())
                    prop:value=move || search.get()
                    on:input=move |event| {
                        search.set(event_target_value(&event));
                        signals.edit();
                    }
                />
            </label>
            <Show when=move || matches!(load.get(), ChoiceLoad::Loading | ChoiceLoad::Waiting) fallback=|| ()>
                <p class="create-choice-state">{move || if load.get() == ChoiceLoad::Waiting { "Waiting for connection" } else { "Loading choices" }}</p>
            </Show>
            <Show when=move || load.get() == ChoiceLoad::Empty fallback=|| ()>
                <p class="create-choice-state">"No choices"</p>
            </Show>
            <Show when=move || matches!(load.get(), ChoiceLoad::MissingPrerequisite(_)) fallback=|| ()>
                <p class="create-choice-state create-choice-missing">{move || match load.get() {
                    ChoiceLoad::MissingPrerequisite(reason) => reason,
                    _ => "",
                }}</p>
            </Show>
            <Show when=move || load.get() == ChoiceLoad::StaleContext fallback=|| ()>
                <div class="create-choice-recovery">
                    <p class="create-choice-state create-choice-stale">"The form context changed. Refresh choices."</p>
                    <button class="create-choice-retry" type="button" on:click=move |_| request_choices(signals, control, request_tx, session_generation.get_untracked(), false)>"Retry"</button>
                </div>
            </Show>
            <Show when=move || matches!(load.get(), ChoiceLoad::Failed(_)) fallback=|| ()>
                <p class="create-choice-state choice-failed" role="alert">{move || match load.get() {
                    ChoiceLoad::Failed(reason) => reason,
                    _ => String::new(),
                }}</p>
            </Show>
            <div class="create-choice-buttons">
                <For
                    each=move || match load.get() {
                        ChoiceLoad::Ready { choices, .. } => choices,
                        ChoiceLoad::Waiting
                        | ChoiceLoad::Loading
                        | ChoiceLoad::Empty
                        | ChoiceLoad::MissingPrerequisite(_)
                        | ChoiceLoad::StaleContext
                        | ChoiceLoad::Failed(_) => Vec::new(),
                    }
                    key=|choice| choice.presentation.label.clone()
                    children=move |choice| {
                        let label = choice.presentation.label.clone();
                        let detail = choice.presentation.detail.clone().unwrap_or_default();
                        let shown_detail = if show_detail { Some(detail.clone()) } else { None };
                        let selected_value = choice.value.clone();
                        let selected_for_class = selected_value.clone();
                        view! {
                            <button
                                type="button"
                                data-value=label.clone()
                                class:active=move || selected_choice(signals, control, &selected_for_class)
                                title=detail
                                on:click=move |_| {
                                    select_choice(signals, control, selected_value.clone());
                                    signals.edit();
                                }
                            >
                                <span>{label.clone()}</span>
                                {shown_detail.map(|detail| view! { <em>{detail}</em> })}
                            </button>
                        }
                    }
                />
            </div>
            <Show when=move || matches!(load.get(), ChoiceLoad::Ready { page_cursor: Some(_), .. }) fallback=|| ()>
                <button class="create-choice-more" type="button" on:click=move |_| request_choices(
                    signals,
                    control,
                    request_tx,
                    session_generation.get_untracked(),
                    true,
                )>"Load more"</button>
            </Show>
        </fieldset>
    }
}

/// Whether `value` is the selection `control` currently holds. A value of another kind than the
/// control asks for selects nothing.
pub(super) fn selected_choice(
    signals: CreateSignals,
    control: ChoiceControl,
    value: &ChoiceValue,
) -> bool {
    match (control, value) {
        (ChoiceControl::DomainPace, ChoiceValue::DomainPace(value)) => {
            signals.domain.get().pace == *value
        }
        (ChoiceControl::PlacementPolicy, ChoiceValue::PlacementPolicy(value)) => {
            signals.domain.get().placement == *value
        }
        (ChoiceControl::BranchSchema, ChoiceValue::Model(node)) => {
            signals.structured.get().branch.selects_schema(node)
        }
        (ChoiceControl::RelaySchema, ChoiceValue::Model(node)) => {
            signals.relay.get().selects_schema(node)
        }
        (ChoiceControl::RelayBranch, ChoiceValue::Model(node)) => {
            signals.relay.get().selects_branch(node)
        }
        (ChoiceControl::SubscriptionRelay, ChoiceValue::Model(node)) => {
            signals.subscription.get().selects_relay(node)
        }
        (ChoiceControl::CodecSchema, ChoiceValue::Model(node)) => signals
            .codec
            .get()
            .schema
            .as_ref()
            .is_some_and(|selected| selected.selects(ModelKind::Schema, node)),
        (ChoiceControl::CodecWireSchema, ChoiceValue::Model(node)) => {
            match signals.codec.get().format {
                Some(CodecFormatDraft::Wire {
                    kind,
                    schema: Some(schema),
                }) => kind
                    .wire_schema_kind()
                    .is_some_and(|kind| schema.selects(kind, node)),
                _ => false,
            }
        }
        (ChoiceControl::CodecResource, ChoiceValue::Resource(resource)) => signals
            .codec
            .get()
            .binding()
            .and_then(|binding| binding.resource.as_ref())
            .is_some_and(|selected| selected.is_current() && selected.name() == resource),
        (ChoiceControl::CodecVersion, ChoiceValue::ResourceVersion(version)) => signals
            .codec
            .get()
            .binding()
            .and_then(|binding| binding.version.as_ref())
            .is_some_and(|selected| selected.is_current() && selected.name() == version),
        (ChoiceControl::SignalingResource, ChoiceValue::Resource(resource)) => signals
            .signaling
            .get()
            .binding()
            .and_then(|binding| binding.resource.as_ref())
            .is_some_and(|selected| selected.is_current() && selected.name() == resource),
        (ChoiceControl::SignalingVersion, ChoiceValue::ResourceVersion(version)) => signals
            .signaling
            .get()
            .binding()
            .and_then(|binding| binding.version.as_ref())
            .is_some_and(|selected| selected.is_current() && selected.name() == version),
        (ChoiceControl::ClientResource, ChoiceValue::Resource(resource)) => signals
            .client
            .get()
            .mount
            .resource
            .as_ref()
            .is_some_and(|selected| selected.is_current() && selected.name() == resource),
        (ChoiceControl::ClientVersion, ChoiceValue::ResourceVersion(version)) => signals
            .client
            .get()
            .mount
            .version
            .as_ref()
            .is_some_and(|selected| selected.is_current() && selected.name() == version),
        (ChoiceControl::ClientSignaling, ChoiceValue::Model(node)) => {
            signals.client.get().selects_signaling(node)
        }
        (ChoiceControl::VhostResource, ChoiceValue::Resource(resource)) => signals
            .vhost
            .get()
            .tls
            .resource
            .as_ref()
            .is_some_and(|selected| selected.is_current() && selected.name() == resource),
        (ChoiceControl::VhostVersion, ChoiceValue::ResourceVersion(version)) => signals
            .vhost
            .get()
            .tls
            .version
            .as_ref()
            .is_some_and(|selected| selected.is_current() && selected.name() == version),
        (ChoiceControl::EndpointVhost, ChoiceValue::Model(node)) => {
            signals.endpoint.get().selects_vhost(node)
        }
        (ChoiceControl::EndpointSignaling, ChoiceValue::Model(node)) => {
            signals.endpoint.get().selects_signaling(node)
        }
        // A field reference is inserted into the filter rather than held as a selection.
        _ => false,
    }
}

/// Applies `value` to the draft `control` edits. A value of another kind than the control asks
/// for changes nothing.
pub(super) fn select_choice(signals: CreateSignals, control: ChoiceControl, value: ChoiceValue) {
    match (control, value) {
        (ChoiceControl::DomainPace, ChoiceValue::DomainPace(value)) => {
            signals.domain.update(|draft| draft.pace = value);
        }
        (ChoiceControl::PlacementPolicy, ChoiceValue::PlacementPolicy(value)) => {
            signals.domain.update(|draft| draft.placement = value);
        }
        (ChoiceControl::BranchSchema, ChoiceValue::Model(node)) => {
            signals
                .structured
                .update(|drafts| drafts.branch.select_schema(&node));
        }
        (ChoiceControl::RelaySchema, ChoiceValue::Model(node)) => {
            signals.relay.update(|draft| draft.select_schema(&node));
        }
        (ChoiceControl::RelayBranch, ChoiceValue::Model(node)) => {
            signals.relay.update(|draft| draft.select_branch(&node));
        }
        (ChoiceControl::SubscriptionRelay, ChoiceValue::Model(node)) => {
            signals
                .subscription
                .update(|draft| draft.select_relay(&node));
        }
        (ChoiceControl::SubscriptionField, ChoiceValue::Field(field)) => {
            signals
                .subscription
                .update(|draft| draft.insert_field_reference(field));
        }
        (ChoiceControl::CodecSchema, ChoiceValue::Model(node)) => {
            signals.codec.update(|draft| draft.select_schema(&node));
        }
        (ChoiceControl::CodecWireSchema, ChoiceValue::Model(node)) => {
            signals
                .codec
                .update(|draft| draft.select_wire_schema(&node));
        }
        (ChoiceControl::CodecResource, ChoiceValue::Resource(resource)) => {
            signals.codec.update(|draft| {
                if let Some(binding) = draft.binding_mut() {
                    binding.select_resource(resource);
                }
            });
        }
        (ChoiceControl::CodecVersion, ChoiceValue::ResourceVersion(version)) => {
            signals.codec.update(|draft| {
                if let Some(binding) = draft.binding_mut() {
                    binding.select_version(version);
                }
            });
        }
        (ChoiceControl::SignalingResource, ChoiceValue::Resource(resource)) => {
            signals.signaling.update(|draft| {
                if let Some(binding) = draft.binding_mut() {
                    binding.select_resource(resource);
                }
            });
        }
        (ChoiceControl::SignalingVersion, ChoiceValue::ResourceVersion(version)) => {
            signals.signaling.update(|draft| {
                if let Some(binding) = draft.binding_mut() {
                    binding.select_version(version);
                }
            });
        }
        (ChoiceControl::ClientResource, ChoiceValue::Resource(resource)) => {
            signals
                .client
                .update(|draft| draft.mount.select_resource(resource));
        }
        (ChoiceControl::ClientVersion, ChoiceValue::ResourceVersion(version)) => {
            signals
                .client
                .update(|draft| draft.mount.select_version(version));
        }
        (ChoiceControl::ClientSignaling, ChoiceValue::Model(node)) => {
            signals.client.update(|draft| draft.select_signaling(&node));
        }
        (ChoiceControl::VhostResource, ChoiceValue::Resource(resource)) => {
            signals
                .vhost
                .update(|draft| draft.tls.select_resource(resource));
        }
        (ChoiceControl::VhostVersion, ChoiceValue::ResourceVersion(version)) => {
            signals
                .vhost
                .update(|draft| draft.tls.select_version(version));
        }
        (ChoiceControl::EndpointVhost, ChoiceValue::Model(node)) => {
            signals.endpoint.update(|draft| draft.select_vhost(&node));
        }
        (ChoiceControl::EndpointSignaling, ChoiceValue::Model(node)) => {
            signals
                .endpoint
                .update(|draft| draft.select_signaling(&node));
        }
        _ => {}
    }
}
