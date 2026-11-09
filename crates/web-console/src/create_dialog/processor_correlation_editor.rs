//! The correlation, match, time and timeout policies of a visual correlator.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of `CORRELATE WHERE` with its left and right field references, the
//!   explicit match policy, the `MAX TIME` text, and each side's timeout action and relay.
//! - **Depends on.** The correlator draft, typed choice controls and Leptos signals.
//! - **Must not know.** Correlation buffers, expression compilation or timeout deadlines.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::CorrelatorMatchPolicy;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_textarea_value, event_target_value,
    processor_correlator_draft::{CorrelationSide, CorrelatorDraft, TimeoutDraft},
    processor_draft::ProcessorFamily,
};

/// Applies `edit` to the correlator draft as one edit of the form.
fn edit_correlator(signals: CreateSignals, edit: impl FnOnce(&mut CorrelatorDraft)) {
    signals
        .processor(ProcessorFamily::Correlator)
        .update(|draft| {
            if let Some(correlator) = draft.correlator_mut() {
                edit(correlator);
            }
        });
    signals.edit();
}

/// Whether the open correlator draft satisfies `check`, or not while the draft is no correlator.
fn correlator_holds(signals: CreateSignals, check: impl FnOnce(&CorrelatorDraft) -> bool) -> bool {
    let draft = signals.processor(ProcessorFamily::Correlator).get();
    match draft.correlator() {
        Some(correlator) => check(correlator),
        None => false,
    }
}

/// The static presentation of one side's timeout controls.
struct TimeoutControls {
    class_name: &'static str,
    legend: &'static str,
    relay_class: &'static str,
    relay_label: &'static str,
    relay_control: ChoiceControl,
}

fn timeout_controls(side: CorrelationSide) -> TimeoutControls {
    match side {
        CorrelationSide::Left => TimeoutControls {
            class_name: "create-field create-correlator-left-timeout",
            legend: "When a left record times out unmatched",
            relay_class: "create-correlator-left-timeout-relay",
            relay_label: "Left timeout relay",
            relay_control: ChoiceControl::ProcessorLeftTimeoutRelay,
        },
        CorrelationSide::Right => TimeoutControls {
            class_name: "create-field create-correlator-right-timeout",
            legend: "When a right record times out unmatched",
            relay_class: "create-correlator-right-timeout-relay",
            relay_label: "Right timeout relay",
            relay_control: ChoiceControl::ProcessorRightTimeoutRelay,
        },
    }
}

#[component]
fn CorrelationTimeoutEditor(
    side: CorrelationSide,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let controls = timeout_controls(side);
    let sends = move || {
        correlator_holds(signals, |correlator| {
            matches!(correlator.timeout(side), TimeoutDraft::SendTo(_))
        })
    };
    view! {
        <fieldset class=controls.class_name>
            <legend>{controls.legend}</legend>
            <button type="button" data-timeout="drop"
                class:active=move || correlator_holds(signals, |correlator| *correlator.timeout(side) == TimeoutDraft::Drop)
                disabled=pending
                on:click=move |_| edit_correlator(signals, |correlator| correlator.timeout_mut(side).choose_drop())>"DROP"</button>
            <button type="button" data-timeout="send"
                class:active=sends
                disabled=pending
                on:click=move |_| edit_correlator(signals, |correlator| correlator.timeout_mut(side).choose_send())>"SEND TO"</button>
        </fieldset>
        <Show when=sends fallback=|| ()>
            <ChoiceGroup class_name=controls.relay_class label=controls.relay_label
                control=controls.relay_control signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
        </Show>
    }
}

#[component]
pub(super) fn ProcessorCorrelationEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(ProcessorFamily::Correlator);
    let pending = move || signals.progress.get().is_pending();
    let matches_by = move |policy: CorrelatorMatchPolicy| {
        correlator_holds(signals, |correlator| {
            correlator.match_policy == Some(policy)
        })
    };
    let section = view! {
        <section class="create-processor-correlation">
            <h3>"Correlation"</h3>
            <p>"A correlator reads each side's record in its explicit left or right scope and has no default input. A left field inserts left.<field>, a right field right.<field>."</p>
            <ChoiceGroup class_name="create-correlate-left-fields" label="Left fields"
                control=ChoiceControl::ProcessorCorrelateLeftField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <ChoiceGroup class_name="create-correlate-right-fields" label="Right fields"
                control=ChoiceControl::ProcessorCorrelateRightField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <label class="create-field"><span>"CORRELATE WHERE expression"</span>
                <textarea class="create-correlate-where"
                    prop:value=move || {
                        let state = draft.get();
                        match state.correlator() {
                            Some(correlator) => correlator.correlate_where.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_textarea_value(&event);
                        edit_correlator(signals, |correlator| correlator.correlate_where = value);
                    } />
            </label>
            <fieldset class="create-field create-correlator-match">
                <legend>"Match policy"</legend>
                <button type="button" data-match="earliest"
                    class:active=move || matches_by(CorrelatorMatchPolicy::Earliest)
                    disabled=pending
                    on:click=move |_| edit_correlator(signals, |correlator| correlator.match_policy = Some(CorrelatorMatchPolicy::Earliest))>"MATCH EARLIEST"</button>
                <button type="button" data-match="latest"
                    class:active=move || matches_by(CorrelatorMatchPolicy::Latest)
                    disabled=pending
                    on:click=move |_| edit_correlator(signals, |correlator| correlator.match_policy = Some(CorrelatorMatchPolicy::Latest))>"MATCH LATEST"</button>
            </fieldset>
            <label class="create-field"><span>"MAX TIME duration"</span>
                <input class="create-correlator-max-time" type="text" autocomplete="off"
                    prop:value=move || {
                        let state = draft.get();
                        match state.correlator() {
                            Some(correlator) => correlator.max_time.clone(),
                            None => String::new(),
                        }
                    }
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        edit_correlator(signals, |correlator| correlator.max_time = value);
                    } />
            </label>
            <CorrelationTimeoutEditor side=CorrelationSide::Left signals=signals request_tx=request_tx session_generation=session_generation />
            <CorrelationTimeoutEditor side=CorrelationSide::Right signals=signals request_tx=request_tx session_generation=session_generation />
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}
