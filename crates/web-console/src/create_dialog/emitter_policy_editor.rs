//! Publication, envelope, batch, flush and error controls for emitter creation.
//!
//! Layer: edges.
//! - **Owns.** Explicit policy choices and presentation of each entity's declared envelope.
//! - **Depends on.** Vocabulary declarations, policy drafts and typed relay choices.
//! - **Must not know.** Confirmation windows, runtime retries or connector error handling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter policy controls")
)]

use leptos::prelude::*;
use nervix_models::{EmitterBatchLegality, EmitterPublishingModeKind, GeneralErrorPolicy};

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender,
    emitter_publication_draft::WindowDraft,
    emitter_rows::{AssignmentArea, AssignmentRows, EmitterText, ValueArea, ValueRows},
    event_target_checked, event_target_textarea_value,
    ingestor_route_draft::{FlushDraft, MessageErrorDraft},
};

#[component]
pub(super) fn EmitterPublicationEditor(signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-emitter-publishing"><legend>"Publishing mode"</legend>
            <For each={move || match signals.emitter.get().destination.kind { Some(kind) => kind.capabilities().publishing_modes.to_vec(), None => Vec::new() }} key=|mode| mode.kind children=move |mode| view! {
                <button type="button" data-mode=mode.kind.label() class:active=move || signals.emitter.get().destination.publication.kind == Some(mode.kind)
                    disabled=move || pending() || !signals.emitter.get().destination.publication_allowed(mode.kind)
                    on:click=move |_| { signals.emitter.update(|draft| draft.destination.publication.kind = Some(mode.kind)); signals.edit(); }>{mode.kind.label()}</button>
            } />
        </fieldset>
        <Show when=move || matches!(signals.emitter.get().destination.publication.kind, Some(EmitterPublishingModeKind::ClientAck | EmitterPublishingModeKind::BrokerAck)) fallback=|| ()>
            <fieldset class="create-field create-emitter-window"><legend>"Acknowledgement window"</legend>
                <button type="button" data-window="sequential" class:active=move || signals.emitter.get().destination.publication.window == WindowDraft::Sequential disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.destination.publication.window = WindowDraft::Sequential); signals.edit(); }>"ACK SEQUENTIAL"</button>
                <button type="button" data-window="parallel" class:active=move || matches!(signals.emitter.get().destination.publication.window, WindowDraft::Parallel(_)) disabled=pending
                    on:click=move |_| { signals.emitter.update(|draft| draft.destination.publication.window = WindowDraft::Parallel(String::new())); signals.edit(); }>"ACK PARALLEL"</button>
            </fieldset>
            <Show when=move || matches!(signals.emitter.get().destination.publication.window, WindowDraft::Parallel(_)) fallback=|| ()>
                <EmitterText label="ACK maximum" class_name="create-emitter-ack-max" signals=signals read=|draft| match &draft.destination.publication.window { WindowDraft::Parallel(max) => max.clone(), _ => String::new() } write=|draft, value| { if let WindowDraft::Parallel(max) = &mut draft.destination.publication.window { *max = value; } } />
            </Show>
            <EmitterText label="Acknowledgement timeout" class_name="create-emitter-ack-timeout" signals=signals read=|draft| draft.destination.publication.ack_timeout.clone() write=|draft, value| draft.destination.publication.ack_timeout = value />
        </Show>
        <EmitterText label="Retry backoff" class_name="create-emitter-retry-backoff" signals=signals read=|draft| draft.destination.publication.retry_backoff.clone() write=|draft, value| draft.destination.publication.retry_backoff = value />
        <EmitterText label="Maximum retry backoff" class_name="create-emitter-retry-max" signals=signals read=|draft| draft.destination.publication.retry_max.clone() write=|draft, value| draft.destination.publication.retry_max = value />
        <EmitterEnvelopeEditor signals=signals />
    }
    .into_any()
}

#[component]
fn EmitterEnvelopeEditor(signals: CreateSignals) -> impl IntoView {
    view! {
        <section class="create-emitter-envelope">
            <For each={move || match signals.emitter.get().destination.kind { Some(kind) => kind.capabilities().envelope.to_vec(), None => Vec::new() }} key=|kind| *kind children=move |kind| view! {
                <div data-clause=kind.keywords()>
                    <label class="create-check"><input type="checkbox" prop:checked=move || signals.emitter.get().envelope.clauses.contains_key(&kind) disabled=move || signals.progress.get().is_pending()
                        on:change=move |event| { let enabled = event_target_checked(&event); signals.emitter.update(|draft| draft.envelope.toggle(kind, enabled)); signals.edit(); } />{kind.keywords()}</label>
                    <Show when=move || signals.emitter.with(|draft| draft.envelope.clauses.get(&kind).is_some_and(|value| value.text().is_some())) fallback=|| ()>
                        <label class="create-field"><span>{kind.keywords()}</span><textarea class="create-emitter-envelope-value" disabled=move || signals.progress.get().is_pending()
                            prop:value=move || signals.emitter.with(|draft| match draft.envelope.clauses.get(&kind) { Some(value) => value.text().unwrap_or_default().to_string(), None => String::new() })
                            on:input=move |event| { let value = event_target_textarea_value(&event); signals.emitter.update(|draft| { if let Some(clause) = draft.envelope.clauses.get_mut(&kind) { clause.set_text(value); } }); signals.edit(); } /></label>
                    </Show>
                </div>
            } />
            <Show when=move || signals.emitter.get().destination.kind.is_some_and(|kind| !kind.capabilities().properties.is_empty()) fallback=|| ()>
                <h3>"PROPERTIES"</h3>
                <p>{move || signals.emitter.with(|draft| match draft.destination.kind { Some(kind) => kind.capabilities().properties.iter().map(|property| format!("{}: {}", property.name, property.ty)).collect::<Vec<_>>().join(", "), None => String::new() })}</p>
                <ValueRows signals=signals area=ValueArea::Properties class_name="create-emitter-properties" />
            </Show>
        </section>
    }
    .into_any()
}

#[component]
pub(super) fn EmitterPolicyEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let batch_required = move || {
        signals.emitter.get().destination.kind.is_some_and(|kind| {
            matches!(
                kind.capabilities().batch,
                EmitterBatchLegality::Required { .. }
            )
        })
    };
    view! {
        <Show when=move || signals.emitter.get().destination.kind.is_some_and(|kind| kind.capabilities().batch.is_available()) fallback=|| ()>
            <label class="create-check"><input class="create-emitter-batch-enabled" type="checkbox" prop:checked=move || signals.emitter.get().batch.enabled disabled=move || pending() || batch_required()
                on:change=move |event| { let enabled = event_target_checked(&event); signals.emitter.update(|draft| draft.batch.enabled = enabled); signals.edit(); } />"BATCH"</label>
            <Show when=move || signals.emitter.get().batch.enabled fallback=|| ()>
                <EmitterText label="Maximum messages per payload" class_name="create-emitter-batch-messages" signals=signals read=|draft| draft.batch.messages.clone() write=|draft, value| draft.batch.messages = value />
                <EmitterText label="Maximum encoded payload size" class_name="create-emitter-batch-size" signals=signals read=|draft| draft.batch.size.clone() write=|draft, value| draft.batch.size = value />
            </Show>
        </Show>
        <fieldset class="create-field create-emitter-flush"><legend>"Flush policy"</legend>
            <button type="button" data-flush="immediate" class:active=move || signals.emitter.get().flush == FlushDraft::Immediate disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.flush = FlushDraft::Immediate); signals.edit(); }>"FLUSH IMMEDIATE"</button>
            <button type="button" data-flush="each" class:active=move || matches!(signals.emitter.get().flush, FlushDraft::Each { .. }) disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.flush = FlushDraft::Each { interval: String::new(), max_batch_size: String::new() }); signals.edit(); }>"FLUSH EACH"</button>
        </fieldset>
        <Show when=move || matches!(signals.emitter.get().flush, FlushDraft::Each { .. }) fallback=|| ()>
            <EmitterText label="Flush interval" class_name="create-emitter-flush-interval" signals=signals read=|draft| match &draft.flush { FlushDraft::Each { interval, .. } => interval.clone(), _ => String::new() } write=|draft, value| { if let FlushDraft::Each { interval, .. } = &mut draft.flush { *interval = value; } } />
            <EmitterText label="Maximum flushed batch size" class_name="create-emitter-flush-size" signals=signals read=|draft| match &draft.flush { FlushDraft::Each { max_batch_size, .. } => max_batch_size.clone(), _ => String::new() } write=|draft, value| { if let FlushDraft::Each { max_batch_size, .. } = &mut draft.flush { *max_batch_size = value; } } />
        </Show>
        <fieldset class="create-field create-emitter-message-error"><legend>"ON MESSAGE ERROR"</legend>
            <button type="button" data-error="ignore" class:active=move || signals.emitter.get().message_error == MessageErrorDraft::Ignore disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.message_error = MessageErrorDraft::Ignore); signals.edit(); }>"IGNORE"</button>
            <button type="button" data-error="log" class:active=move || signals.emitter.get().message_error == MessageErrorDraft::Log disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.message_error = MessageErrorDraft::Log); signals.edit(); }>"LOG"</button>
            <button type="button" data-error="send-to" class:active=move || matches!(signals.emitter.get().message_error, MessageErrorDraft::SendTo { .. }) disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.message_error = MessageErrorDraft::SendTo { relay: None, assignments: Vec::new() }); signals.edit(); }>"SEND TO"</button>
        </fieldset>
        <Show when=move || matches!(signals.emitter.get().message_error, MessageErrorDraft::SendTo { .. }) fallback=|| ()>
            <ChoiceGroup class_name="create-emitter-error-relay" label="Error relay" control=ChoiceControl::EmitterErrorRelay signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
            <ChoiceGroup class_name="create-emitter-error-field" label="Add error field" control=ChoiceControl::EmitterErrorField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
            <AssignmentRows signals=signals area=AssignmentArea::Error />
        </Show>
        <fieldset class="create-field create-emitter-general-error"><legend>"ON GENERAL ERROR"</legend>
            <button type="button" data-error="ignore" class:active=move || signals.emitter.get().general_error == Some(GeneralErrorPolicy::Ignore) disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.general_error = Some(GeneralErrorPolicy::Ignore)); signals.edit(); }>"IGNORE"</button>
            <button type="button" data-error="log" class:active=move || signals.emitter.get().general_error == Some(GeneralErrorPolicy::Log) disabled=pending on:click=move |_| { signals.emitter.update(|draft| draft.general_error = Some(GeneralErrorPolicy::Log)); signals.edit(); }>"LOG"</button>
        </fieldset>
    }
    .into_any()
}
