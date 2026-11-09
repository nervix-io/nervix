//! Ordered output routes shared by every visual processor family.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser route order, reingestor branch choices, construction, flush and message
//!   error controls until each route is complete, showing only the construction and flush
//!   controls the family's route contract admits.
//! - **Depends on.** Shared route drafts, typed choices, and the ordered construction editors.
//! - **Must not know.** Data-plane routing, buffering, or error execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;

use super::{
    ChoiceControl, CreateSignals, RequestSender,
    choice_group::ChoiceGroup,
    event_target_textarea_value, event_target_value,
    ingestor_route_draft::{FlushDraft, MessageErrorDraft, RouteBranchDraft},
    processor_assignment_editor::{AssignmentArea, ProcessorAssignmentRows},
    processor_draft::ProcessorFamily,
    processor_editor::indices,
    processor_inheritance_editor::ProcessorInheritanceEditor,
    processor_invocation_editor::ProcessorInvocationEditor,
    processor_route_references::ProcessorRouteReferences,
};

fn route_flush_interval(signals: CreateSignals, family: ProcessorFamily) -> String {
    match signals.processor(family).get().active_route() {
        Some(route) => match &route.flush {
            FlushDraft::Each { interval, .. } => interval.clone(),
            _ => String::new(),
        },
        None => String::new(),
    }
}

fn route_flush_size(signals: CreateSignals, family: ProcessorFamily) -> String {
    match signals.processor(family).get().active_route() {
        Some(route) => match &route.flush {
            FlushDraft::Each { max_batch_size, .. } => max_batch_size.clone(),
            _ => String::new(),
        },
        None => String::new(),
    }
}

#[component]
pub(super) fn ProcessorRouteEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let inheritance = family.route_contract().transforms().then(|| {
        view! { <ProcessorInheritanceEditor family=family signals=signals request_tx=request_tx session_generation=session_generation /> }
    });
    let section = view! {
        <section class="create-processor-routes">
            <h3>"Ordered output routes"</h3>
            <div class="create-processor-route-list">
                <For each=move || indices(draft.get().routes.len()) key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-route=index.to_string()
                            class:active=move || draft.get().active_route == index
                            disabled=pending
                            on:click=move |_| { draft.update(|state| state.active_route = index); signals.edit(); }>
                            {format!("Route {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-processor-add-route" disabled=pending
                    on:click=move |_| { draft.update(|state| state.add_route()); signals.edit(); }>"Add route"</button>
            </div>
            <div class="create-processor-route">
                <div class="create-processor-route-actions">
                    <button type="button" class="create-processor-route-up"
                        disabled=move || { pending() || draft.get().active_route == 0 }
                        on:click=move |_| { draft.update(|state| state.move_route(true)); signals.edit(); }>"Move up"</button>
                    <button type="button" class="create-processor-route-down"
                        disabled=move || { pending() || draft.get().active_route + 1 >= draft.get().routes.len() }
                        on:click=move |_| { draft.update(|state| state.move_route(false)); signals.edit(); }>"Move down"</button>
                    <button type="button" class="create-processor-remove-route"
                        disabled=move || { pending() || draft.get().routes.len() == 1 }
                        on:click=move |_| { draft.update(|state| state.remove_route()); signals.edit(); }>"Remove route"</button>
                </div>
                <ProcessorRouteBranchEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
                <ChoiceGroup class_name="create-processor-route-relay" label="Output relay"
                    control=ChoiceControl::ProcessorRouteRelay signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                {inheritance}
                <ProcessorRouteConstructionEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
                <ProcessorRouteFlushEditor family=family signals=signals />
                <ProcessorRouteMessageErrorEditor family=family signals=signals request_tx=request_tx session_generation=session_generation />
            </div>
        </section>
    };
    // Erased at the section boundary: the processor editor composing this section then holds and
    // names a boxed view, so neither its rendering frames nor the debug names of every form that
    // embeds it repeat this section's complete nested view.
    section.into_any()
}

/// The active route's branch and its branch key assignments, for the families whose routes choose one.
#[component]
fn ProcessorRouteBranchEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <Show when=move || !family.declares_node_branch() fallback=|| ()>
            <fieldset class="create-field create-processor-route-branching">
                <legend>"Route branch"</legend>
                <button type="button" data-branch="preserve"
                    class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Preserve))
                    disabled=pending
                    on:click=move |_| { draft.update(|state| {
                        if let Some(route) = state.active_route_mut() {
                            route.branch.choose_preserve();
                            if let Some(relay) = &mut route.relay { relay.invalidate(); }
                        }
                    }); signals.edit(); }>"Preserve incoming branch"</button>
                <button type="button" data-branch="unbranched"
                    class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Unbranched))
                    disabled=pending
                    on:click=move |_| { draft.update(|state| {
                        if let Some(route) = state.active_route_mut() {
                            route.branch.choose_unbranched();
                            if let Some(relay) = &mut route.relay { relay.invalidate(); }
                        }
                    }); signals.edit(); }>"UNBRANCHED"</button>
                <button type="button" data-branch="named"
                    class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Branched { .. }))
                    disabled=pending
                    on:click=move |_| { draft.update(|state| {
                        if let Some(route) = state.active_route_mut() {
                            route.branch.choose_branched();
                            if let Some(relay) = &mut route.relay { relay.invalidate(); }
                        }
                    }); signals.edit(); }>"Named branch"</button>
            </fieldset>
            <Show when=move || draft.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Branched { .. })) fallback=|| ()>
                <ChoiceGroup class_name="create-processor-route-branch" label="Named route branch"
                    control=ChoiceControl::ProcessorRouteBranch signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <div class="create-processor-branch-assignments">
                    <ChoiceGroup class_name="create-processor-branch-fields" label="Add branch key assignment"
                        control=ChoiceControl::ProcessorBranchField signals=signals request_tx=request_tx
                        session_generation=session_generation show_detail=true />
                    <ProcessorAssignmentRows family=family area=AssignmentArea::Branch signals=signals />
                </div>
            </Show>
        </Show>
    }
    .into_any()
}

/// The active route's output assignments with the family's scoped field references, its WHERE
/// expression and, on a transforming route, its ordered invocations.
#[component]
fn ProcessorRouteConstructionEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    let invocations = family
        .route_contract()
        .transforms()
        .then(|| view! { <ProcessorInvocationEditor family=family signals=signals /> });
    view! {
        <div class="create-processor-output-assignments">
            <ChoiceGroup class_name="create-processor-output-fields" label="Add output assignment"
                control=ChoiceControl::ProcessorOutputField signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <ProcessorRouteReferences family=family signals=signals request_tx=request_tx session_generation=session_generation />
            <ProcessorAssignmentRows family=family area=AssignmentArea::Output signals=signals />
        </div>
        <label class="create-field"><span>"Route WHERE expression (optional)"</span>
            <textarea class="create-processor-route-where"
                prop:value=move || {
                    let state = draft.get();
                    match state.active_route() {
                        Some(route) => route.where_clause.clone(),
                        None => String::new(),
                    }
                }
                disabled=pending
                on:input=move |event| {
                    let value = event_target_textarea_value(&event);
                    draft.update(|state| {
                        if let Some(route) = state.active_route_mut() { route.where_clause = value; }
                    });
                    signals.edit();
                } />
        </label>
        {invocations}
    }
    .into_any()
}

/// The active route's flush policy, for the families whose routes flush.
#[component]
fn ProcessorRouteFlushEditor(family: ProcessorFamily, signals: CreateSignals) -> impl IntoView {
    if !family.route_contract().flushes() {
        return ().into_any();
    }
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-processor-flush">
            <legend>"Flush policy"</legend>
            <button type="button" data-flush="immediate"
                class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Immediate))
                disabled=pending
                on:click=move |_| { draft.update(|state| {
                    if let Some(route) = state.active_route_mut() { route.flush = FlushDraft::Immediate; }
                }); signals.edit(); }>"IMMEDIATE"</button>
            <button type="button" data-flush="each"
                class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Each { .. }))
                disabled=pending
                on:click=move |_| { draft.update(|state| {
                    if let Some(route) = state.active_route_mut() {
                        route.flush = FlushDraft::Each { interval: String::new(), max_batch_size: String::new() };
                    }
                }); signals.edit(); }>"EACH"</button>
        </fieldset>
        <Show when=move || draft.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Each { .. })) fallback=|| ()>
            <label class="create-field"><span>"Flush interval"</span>
                <input class="create-processor-flush-interval" type="text" autocomplete="off"
                    prop:value=move || route_flush_interval(signals, family)
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        draft.update(|state| {
                            if let Some(route) = state.active_route_mut()
                                && let FlushDraft::Each { interval, .. } = &mut route.flush { *interval = value; }
                        });
                        signals.edit();
                    } />
            </label>
            <label class="create-field"><span>"Maximum batch size"</span>
                <input class="create-processor-flush-size" type="text" autocomplete="off"
                    prop:value=move || route_flush_size(signals, family)
                    disabled=pending
                    on:input=move |event| {
                        let value = event_target_value(&event);
                        draft.update(|state| {
                            if let Some(route) = state.active_route_mut()
                                && let FlushDraft::Each { max_batch_size, .. } = &mut route.flush { *max_batch_size = value; }
                        });
                        signals.edit();
                    } />
            </label>
        </Show>
    }
    .into_any()
}

/// The active route's message error policy and its error relay assignments.
#[component]
fn ProcessorRouteMessageErrorEditor(
    family: ProcessorFamily,
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let draft = signals.processor(family);
    let pending = move || signals.progress.get().is_pending();
    view! {
        <fieldset class="create-field create-processor-message-error">
            <legend>"On message error"</legend>
            <button type="button" data-error="ignore"
                class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::Ignore))
                disabled=pending
                on:click=move |_| { draft.update(|state| {
                    if let Some(route) = state.active_route_mut() { route.message_error = MessageErrorDraft::Ignore; }
                }); signals.edit(); }>"IGNORE"</button>
            <button type="button" data-error="log"
                class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::Log))
                disabled=pending
                on:click=move |_| { draft.update(|state| {
                    if let Some(route) = state.active_route_mut() { route.message_error = MessageErrorDraft::Log; }
                }); signals.edit(); }>"LOG"</button>
            <button type="button" data-error="send"
                class:active=move || draft.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::SendTo { .. }))
                disabled=pending
                on:click=move |_| { draft.update(|state| {
                    if let Some(route) = state.active_route_mut() {
                        route.message_error = MessageErrorDraft::SendTo { relay: None, assignments: Vec::new() };
                    }
                }); signals.edit(); }>"SEND TO"</button>
        </fieldset>
        <Show when=move || draft.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::SendTo { .. })) fallback=|| ()>
            <div class="create-processor-error-assignments">
                <ChoiceGroup class_name="create-processor-error-relay" label="Error relay"
                    control=ChoiceControl::ProcessorErrorRelay signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <ChoiceGroup class_name="create-processor-error-fields" label="Add error assignment"
                    control=ChoiceControl::ProcessorErrorField signals=signals request_tx=request_tx
                    session_generation=session_generation show_detail=true />
                <ProcessorAssignmentRows family=family area=AssignmentArea::Error signals=signals />
            </div>
        </Show>
    }
    .into_any()
}
