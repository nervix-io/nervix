//! Declaration-driven entity and payload controls for visual emitters.
//!
//! Layer: edges.
//! - **Owns.** Entity selection, literal locators, typed reference pickers and entity option widgets.
//! - **Depends on.** Vocabulary declarations, destination drafts and reusable browser controls.
//! - **Must not know.** Driver configuration interpretation, external schemas or network clients.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns visual emitter destination controls")
)]

use leptos::prelude::*;
use nervix_models::{
    EmitterBodyForm, EmitterEntityKind, OptionDeclaration, OptionForm, OptionPresence, OptionValue,
    OtelAggregationTemporality,
};

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender,
    emitter_destination_draft::{ConflictDraft, MetricChoice, ScopeDraft},
    emitter_draft::BodyChoice,
    emitter_rows::{
        EmitterOptionalText, EmitterText, OrderButtons, ValueArea, ValueRows, move_item,
    },
    event_target_checked, event_target_value,
};

#[component]
pub(super) fn EmitterDestinationEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-emitter-entity"><legend>"Connector and entity"</legend>
            <For each={|| EmitterEntityKind::all().collect::<Vec<_>>()} key=|kind| *kind children=move |kind| view! {
                <button type="button" data-entity=kind.phrase().to_ascii_lowercase().replace(' ', "-")
                    class:active=move || signals.emitter.get().destination.kind == Some(kind) disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.choose_kind(kind)); signals.edit(); }>{kind.phrase()}</button>
            } />
        </fieldset>
        <Show when=move || signals.emitter.get().destination.kind.is_some() fallback=|| ()>
            <ChoiceGroup class_name="create-emitter-reference" label="Configured client or output schema" control=ChoiceControl::EmitterReference signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
            <p>{move || signals.emitter.with(|draft| match &draft.destination.reference { Some(reference) => format!("Selected reference: {}", reference.name()), None => "Choose a reference".to_string() })}</p>
            <Show when=move || signals.emitter.with(|draft| draft.destination.reference.as_ref().is_some_and(|reference| !reference.is_current())) fallback=|| ()>
                <p class="create-reference-invalid">"This reference belongs to a changed context. Select it again."</p>
            </Show>
            {move || match signals.emitter.get().destination.kind.and_then(|kind| kind.capabilities().locator) {
                Some(locator) => view! { <EmitterText label=locator.keywords class_name="create-emitter-locator" signals=signals read=|draft| draft.destination.locator.clone() write=|draft, value| draft.destination.locator = value /> }.into_any(),
                None => ().into_any(),
            }}
            <For each={move || match signals.emitter.get().destination.kind { Some(kind) => kind.capabilities().options.to_vec(), None => Vec::new() }} key=|option| option.keywords children=move |option| view! {
                <section class="create-emitter-option" data-option=option.keywords>
                    <EmitterOption option=option signals=signals request_tx=request_tx session_generation=session_generation />
                </section>
            } />
            <EmitterPayloadEditor signals=signals request_tx=request_tx session_generation=session_generation />
        </Show>
    }
}

#[component]
fn EmitterOption(
    option: OptionDeclaration,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let keyword = option.keywords;
    let choices = match option.form {
        OptionForm::Choice(choices) => choices,
        _ => &[],
    };
    view! {
        <Show when=move || !choices.is_empty() fallback=|| ()>
            <fieldset class="create-field create-emitter-option-choices"><legend>{keyword}</legend>
                <Show when=move || matches!(option.presence, OptionPresence::Optional { .. }) fallback=|| ()>
                    <button type="button" data-choice="omit" disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| { signals.emitter.update(|draft| draft.destination.choose_option(keyword, "")); signals.edit(); }>"Omit"</button>
                </Show>
                <For each={move || choices.to_vec()} key=|choice| choice.name children=move |choice| view! {
                    <button type="button" data-choice=choice.name title=choice.example
                        class:active=move || signals.emitter.get().destination.chosen_option(keyword) == Some(choice.name)
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| { signals.emitter.update(|draft| draft.destination.choose_option(keyword, choice.name)); signals.edit(); }>{choice.name}</button>
                } />
            </fieldset>
        </Show>
        {match keyword {
            "METHOD" => view! { <EmitterText label=keyword class_name="create-emitter-method" signals=signals multiline=true read=|draft| draft.destination.method.clone() write=|draft, value| draft.destination.method = value /> }.into_any(),
            "PATH" => view! { <EmitterText label=keyword class_name="create-emitter-path" signals=signals multiline=true read=|draft| draft.destination.path.clone() write=|draft, value| draft.destination.path = value /> }.into_any(),
            "SUBJECT" => view! { <EmitterText label=keyword class_name="create-emitter-subject" signals=signals read=|draft| draft.destination.subject.clone() write=|draft, value| draft.destination.subject = value /> }.into_any(),
            "ROUTING KEY" => view! { <EmitterText label=keyword class_name="create-emitter-routing-key" signals=signals multiline=true read=|draft| draft.destination.routing_key.clone() write=|draft, value| draft.destination.routing_key = value /> }.into_any(),
            "FIFO GROUP" => view! { <EmitterOptionalText label=keyword class_name="create-emitter-fifo-group" signals=signals read=|draft| draft.destination.fifo_group.clone() write=|draft, value| draft.destination.fifo_group = value /> }.into_any(),
            "PLATFORM" => view! { <EmitterOptionalText label=keyword class_name="create-emitter-platform" signals=signals read=|draft| draft.destination.platform.clone() write=|draft, value| draft.destination.platform = value /> }.into_any(),
            "ATTRIBUTES" => view! { <h4>{keyword}</h4><ValueRows signals=signals area=ValueArea::Attributes class_name="create-emitter-attributes" /> }.into_any(),
            "RESOURCE" => view! { <h4>{keyword}</h4><ValueRows signals=signals area=ValueArea::Resource class_name="create-emitter-resource" /> }.into_any(),
            "METRIC" => view! { <MetricEditor signals=signals /> }.into_any(),
            "SCOPE" => view! { <ScopeEditor signals=signals /> }.into_any(),
            "ON CONFLICT" => view! { <ConflictEditor option=option signals=signals /> }.into_any(),
            "NAMESPACE" => view! { <EmitterText label=keyword class_name="create-emitter-namespace" signals=signals read=|draft| draft.destination.namespace.clone() write=|draft, value| draft.destination.namespace = value /> }.into_any(),
            "LOCATION" => view! { <EmitterText label=keyword class_name="create-emitter-location" signals=signals read=|draft| draft.destination.location.clone() write=|draft, value| draft.destination.location = value /> }.into_any(),
            "CATALOG" => view! { <ChoiceGroup class_name="create-emitter-catalog" label="Catalog client" control=ChoiceControl::EmitterCatalog signals=signals request_tx=request_tx session_generation=session_generation show_detail=true /> }.into_any(),
            "COMMIT EACH" => view! {
                <EmitterText label=keyword class_name="create-emitter-commit-each" signals=signals read=|draft| draft.destination.commit_each.clone() write=|draft, value| draft.destination.commit_each = value />
                <EmitterText label="Maximum commit size" class_name="create-emitter-commit-size" signals=signals read=|draft| draft.destination.max_commit_size.clone() write=|draft, value| draft.destination.max_commit_size = value />
            }.into_any(),
            "QOS" | "SEND" => ().into_any(),
            _ => view! { <p role="alert" class="create-validation">"This declared entity option has no visual editor."</p> }.into_any(),
        }}
    }
}

#[component]
fn EmitterPayloadEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    view! {
        <fieldset class="create-field create-emitter-body"><legend>"Payload"</legend>
            <Show when=move || signals.emitter.get().destination.kind.is_some_and(|kind| matches!(kind.capabilities().body, EmitterBodyForm::Codec | EmitterBodyForm::CodecOrWithoutBody)) fallback=|| ()>
                <button type="button" data-body="codec" class:active=move || signals.emitter.get().body == Some(BodyChoice::Codec) disabled=move || signals.progress.get().is_pending()
                    on:click=move |_| { signals.emitter.update(|draft| draft.choose_body(BodyChoice::Codec)); signals.edit(); }>"ENCODE USING"</button>
            </Show>
            <Show when=move || signals.emitter.get().destination.kind.is_some_and(|kind| kind.capabilities().body == EmitterBodyForm::CodecOrWithoutBody) fallback=|| ()>
                <button type="button" data-body="without-body" class:active=move || signals.emitter.get().body == Some(BodyChoice::WithoutBody) disabled=move || signals.progress.get().is_pending()
                    on:click=move |_| { signals.emitter.update(|draft| draft.choose_body(BodyChoice::WithoutBody)); signals.edit(); }>"WITHOUT BODY"</button>
            </Show>
            <Show when=move || signals.emitter.get().body == Some(BodyChoice::Client) fallback=|| ()><p>"Native Arrow output constructed against the selected schema"</p></Show>
        </fieldset>
        <Show when=move || signals.emitter.get().body == Some(BodyChoice::Codec) fallback=|| ()>
            <ChoiceGroup class_name="create-emitter-codec" label="Encoding codec" control=ChoiceControl::EmitterCodec signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
            <Show when=move || signals.emitter.get().codec.as_ref().is_some_and(|codec| !codec.is_current()) fallback=|| ()><p class="create-reference-invalid">"This codec belongs to a changed context. Select it again."</p></Show>
        </Show>
        <Show when=move || signals.emitter.get().body == Some(BodyChoice::Values) fallback=|| ()><h3>"VALUES"</h3><ValueRows signals=signals area=ValueArea::Values class_name="create-emitter-values" /></Show>
    }
}

#[component]
fn ScopeEditor(signals: CreateSignals) -> impl IntoView {
    view! {
        <label class="create-check"><input type="checkbox" class="create-emitter-scope-enabled" prop:checked=move || signals.emitter.get().destination.scope.is_some() disabled=move || signals.progress.get().is_pending()
            on:change=move |event| { let enabled = event_target_checked(&event); signals.emitter.update(|draft| draft.destination.scope = if enabled { Some(ScopeDraft::default()) } else { None }); signals.edit(); } />"SCOPE"</label>
        <Show when=move || signals.emitter.get().destination.scope.is_some() fallback=|| ()>
            <EmitterText label="Scope name" class_name="create-emitter-scope-name" signals=signals read=|draft| match &draft.destination.scope { Some(scope) => scope.name.clone(), None => String::new() } write=|draft, value| { if let Some(scope) = &mut draft.destination.scope { scope.name = value; } } />
            <EmitterOptionalText label="Scope version" class_name="create-emitter-scope-version" signals=signals read=|draft| match &draft.destination.scope { Some(scope) => scope.version.clone(), None => None } write=|draft, value| { if let Some(scope) = &mut draft.destination.scope { scope.version = value; } } />
        </Show>
    }
}

#[component]
fn MetricEditor(signals: CreateSignals) -> impl IntoView {
    view! {
        <EmitterText label="Metric name" class_name="create-emitter-metric-name" signals=signals read=|draft| draft.destination.metric_name.clone() write=|draft, value| draft.destination.metric_name = value />
        <EmitterText label="Metric unit" class_name="create-emitter-metric-unit" signals=signals read=|draft| draft.destination.metric_unit.clone() write=|draft, value| draft.destination.metric_unit = value />
        <EmitterOptionalText label="Metric description" class_name="create-emitter-metric-description" signals=signals read=|draft| draft.destination.metric_description.clone() write=|draft, value| draft.destination.metric_description = value />
        <Show when=move || signals.emitter.get().destination.metric_kind == Some(MetricChoice::Sum) fallback=|| ()>
            <fieldset class="create-field create-emitter-metric-monotonic"><legend>"Sum monotonicity"</legend>
                <For each={|| [true, false]} key=|value| *value children=move |value| view! {
                    <button type="button" data-monotonic=value.to_string() class:active=move || signals.emitter.get().destination.metric_monotonic == Some(value) disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| { signals.emitter.update(|draft| draft.destination.metric_monotonic = Some(value)); signals.edit(); }>{if value { "MONOTONIC" } else { "NON MONOTONIC" }}</button>
                } />
            </fieldset>
        </Show>
        <Show when=move || matches!(signals.emitter.get().destination.metric_kind, Some(MetricChoice::Sum | MetricChoice::Histogram)) fallback=|| ()>
            <fieldset class="create-field create-emitter-metric-temporality"><legend>"Temporality"</legend>
                <For each={|| [(OtelAggregationTemporality::Delta, "DELTA"), (OtelAggregationTemporality::Cumulative, "CUMULATIVE")]} key=|(_, label)| *label children=move |(value, label)| view! {
                    <button type="button" data-temporality=label class:active=move || signals.emitter.get().destination.metric_temporality == Some(value) disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| { signals.emitter.update(|draft| draft.destination.metric_temporality = Some(value)); signals.edit(); }>{label}</button>
                } />
            </fieldset>
        </Show>
    }
}

#[component]
fn ConflictEditor(option: OptionDeclaration, signals: CreateSignals) -> impl IntoView {
    let has_target = match option.form {
        OptionForm::Choice(choices) => choices
            .iter()
            .any(|choice| choice.value == Some(OptionValue::Clause)),
        _ => false,
    };
    view! {
        <Show when=move || has_target && signals.emitter.get().destination.conflict != ConflictDraft::None fallback=|| ()>
            <h4>"Ordered conflict target columns"</h4>
            <For each={move || (0..signals.emitter.get().destination.conflict_targets.len()).collect::<Vec<_>>()} key=|index| *index children=move |index| view! {
                <label class="create-field"><span>"Column"</span><input type="text" class="create-emitter-conflict-target" prop:value=move || signals.emitter.with(|draft| draft.destination.conflict_targets.get(index).cloned().unwrap_or_default()) disabled=move || signals.progress.get().is_pending()
                    on:input=move |event| { let value = event_target_value(&event); signals.emitter.update(|draft| { if let Some(target) = draft.destination.conflict_targets.get_mut(index) { *target = value; } }); signals.edit(); } /></label>
                <OrderButtons signals=signals index=Signal::derive(move || index) len=Signal::derive(move || signals.emitter.get().destination.conflict_targets.len())
                    move_row=Callback::new(move |(index, up)| signals.emitter.update(|draft| move_item(&mut draft.destination.conflict_targets, index, up)))
                    remove_row=Callback::new(move |index| signals.emitter.update(|draft| { if index < draft.destination.conflict_targets.len() { draft.destination.conflict_targets.remove(index); } })) />
            } />
            <button type="button" class="create-emitter-add-conflict-target" disabled=move || signals.progress.get().is_pending() on:click=move |_| { signals.emitter.update(|draft| draft.destination.conflict_targets.push(String::new())); signals.edit(); }>"Add conflict target"</button>
        </Show>
    }
}
