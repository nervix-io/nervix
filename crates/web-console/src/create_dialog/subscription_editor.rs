//! Structured session-subscription controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** The subscription form's name and relay reference controls, the relay's typed field
//!   references, the filter editor and its live reading, and the delivery and sampling options.
//! - **Depends on.** Browser draft signals, the model-owned delivery variants, and the shared
//!   choice UI.
//! - **Must not know.** Subscription tabs, statement dispatch, or how the server compiles the
//!   filter.

use leptos::prelude::*;
use nervix_models::SubscriptionDeliveryBehavior;
use strum::IntoEnumIterator as _;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_checked,
    event_target_value, subscription_draft::FilterReading,
};

#[component]
pub(super) fn SubscriptionEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let reading = move || signals.subscription.get().filter_reading();
    view! {
        <label class="create-field">
            <span>"Subscription name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.subscription.get().name
                disabled=pending
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.subscription.update(|draft| draft.name = name);
                    signals.edit();
                } />
        </label>
        <ChoiceGroup class_name="create-relay-ref" label="Relay"
            control=ChoiceControl::SubscriptionRelay signals=signals request_tx=request_tx
            session_generation=session_generation />
        <Show when=move || signals.subscription.get().relay.is_some() fallback=|| ()>
            <p class="create-selected-relay">{move || match signals.subscription.get().relay {
                Some(relay) => format!("Selected relay: {}", relay.name()),
                None => String::new(),
            }}</p>
        </Show>
        <Show when=move || signals.subscription.get().relay.is_some_and(|relay| !relay.is_current()) fallback=|| ()>
            <p class="create-reference-invalid" role="alert">"The selected relay's domain changed. Select a relay again."</p>
        </Show>
        <ChoiceGroup class_name="create-field-refs" label="Relay fields"
            control=ChoiceControl::SubscriptionField signals=signals request_tx=request_tx
            session_generation=session_generation show_detail=true />
        <label class="create-field">
            <span>"Filter · WHERE over the relay record; a relay field inserts input.<field>"</span>
            <input class="create-filter" type="text" autocomplete="off" spellcheck="false"
                placeholder="e.g. input.tier = 'premium'"
                prop:value=move || signals.subscription.get().filter
                disabled=pending
                on:input=move |event| {
                    let filter = event_target_value(&event);
                    signals.subscription.update(|draft| draft.filter = filter);
                    signals.edit();
                } />
        </label>
        <p class="create-filter-reading" aria-live="polite"
            class:invalid=move || matches!(reading(), FilterReading::Invalid(_))>
            {move || match reading() {
                FilterReading::Everything => "No filter: every record is selected".to_string(),
                FilterReading::Expression(expression) => format!("Reads as WHERE {expression}"),
                FilterReading::Invalid(reason) => reason,
            }}
        </p>
        <fieldset class="create-choice-group create-delivery-options">
            <legend>"Delivery"</legend>
            <div class="create-choice-buttons">
                <For each=move || { SubscriptionDeliveryBehavior::iter().collect::<Vec<_>>() }
                    key=|delivery| delivery.as_ref().to_string()
                    children=move |delivery| {
                        let label = delivery.as_ref().to_string();
                        view! {
                            <button type="button" data-value=label.clone()
                                class:active=move || signals.subscription.get().delivery == delivery
                                disabled=pending
                                on:click=move |_| {
                                    signals.subscription.update(|draft| draft.delivery = delivery);
                                    signals.edit();
                                }>{label.clone()}</button>
                        }
                    } />
            </div>
        </fieldset>
        <label class="create-check">
            <input class="create-sample" type="checkbox"
                prop:checked=move || signals.subscription.get().sampled
                disabled=pending
                on:change=move |event| {
                    let checked = event_target_checked(&event);
                    signals.subscription.update(|draft| draft.sampled = checked);
                    signals.edit();
                } />
            <span>"Sample batches"</span>
        </label>
        <Show when=move || signals.subscription.get().sampled fallback=|| ()>
            <label class="create-field">
                <span>"Batch sample rate · from 0 to 1"</span>
                <input class="create-sample-rate" type="text" inputmode="decimal" autocomplete="off"
                    prop:value=move || signals.subscription.get().sample_rate
                    disabled=pending
                    on:input=move |event| {
                        let rate = event_target_value(&event);
                        signals.subscription.update(|draft| draft.sample_rate = rate);
                        signals.edit();
                    } />
            </label>
        </Show>
    }
}
