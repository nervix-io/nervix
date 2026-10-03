//! Ordered route controls for visual ingestor creation.
//!
//! Layer: edges.
//!
//! - **Owns.** Browser editing of route order, construction, branch keys, flush, and errors.
//! - **Depends on.** Browser drafts and typed choice presentation.
//! - **Must not know.** Registry validation or data-plane execution.

use leptos::prelude::*;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_checked,
    event_target_textarea_value, event_target_value,
    ingestor_route_draft::{
        AssignmentDraft, FlushDraft, InheritDraft, InvocationDraft, MessageErrorDraft,
        RouteBranchDraft,
    },
};

#[derive(Clone, Copy)]
enum AssignmentArea {
    Output,
    Branch,
    Error,
}

fn indices(len: usize) -> Vec<usize> {
    (0..len).collect()
}

fn inherited_field_name(signals: CreateSignals, index: usize) -> String {
    let draft = signals.ingestor.get();
    match draft.active_route().map(|route| &route.inherit) {
        Some(InheritDraft::AllExcept(fields)) => {
            fields.get(index).map(|field| field.name().to_string())
        }
        Some(InheritDraft::Fields(fields)) => fields
            .get(index)
            .map(|field| field.field.name().to_string()),
        _ => None,
    }
    .unwrap_or_default()
}

impl AssignmentArea {
    fn rows(self, signals: CreateSignals) -> Vec<AssignmentDraft> {
        let draft = signals.ingestor.get();
        let Some(route) = draft.active_route() else {
            return Vec::new();
        };
        match self {
            Self::Output => route.assignments.clone(),
            Self::Branch => match &route.branch {
                RouteBranchDraft::Branched { assignments, .. } => assignments.clone(),
                _ => Vec::new(),
            },
            Self::Error => match &route.message_error {
                MessageErrorDraft::SendTo { assignments, .. } => assignments.clone(),
                _ => Vec::new(),
            },
        }
    }

    fn with_rows(self, signals: CreateSignals, action: impl FnOnce(&mut Vec<AssignmentDraft>)) {
        signals.ingestor.update(|draft| {
            let Some(route) = draft.active_route_mut() else {
                return;
            };
            match self {
                Self::Output => action(&mut route.assignments),
                Self::Branch => {
                    if let RouteBranchDraft::Branched { assignments, .. } = &mut route.branch {
                        action(assignments);
                    }
                }
                Self::Error => {
                    if let MessageErrorDraft::SendTo { assignments, .. } = &mut route.message_error
                    {
                        action(assignments);
                    }
                }
            }
        });
        signals.edit();
    }
}

fn assignment_field_name(area: AssignmentArea, signals: CreateSignals, index: usize) -> String {
    let rows = area.rows(signals);
    let Some(assignment) = rows.get(index) else {
        return "Choose field".to_string();
    };
    match &assignment.field {
        Some(field) => field.name().to_string(),
        None => "Choose field".to_string(),
    }
}

fn assignment_expression(area: AssignmentArea, signals: CreateSignals, index: usize) -> String {
    let rows = area.rows(signals);
    match rows.get(index) {
        Some(assignment) => assignment.expression.clone(),
        None => String::new(),
    }
}

fn invocation_count(signals: CreateSignals) -> usize {
    let draft = signals.ingestor.get();
    match draft.active_route() {
        Some(route) => route.invocations.len(),
        None => 0,
    }
}

fn invocation_function(signals: CreateSignals, index: usize) -> String {
    let draft = signals.ingestor.get();
    if let Some(route) = draft.active_route()
        && let Some(invocation) = route.invocations.get(index)
    {
        return invocation.function.clone();
    }
    String::new()
}

fn invocation_argument_count(signals: CreateSignals, index: usize) -> usize {
    let draft = signals.ingestor.get();
    if let Some(route) = draft.active_route()
        && let Some(invocation) = route.invocations.get(index)
    {
        return invocation.arguments.len();
    }
    0
}

fn invocation_argument(signals: CreateSignals, index: usize, argument_index: usize) -> String {
    let draft = signals.ingestor.get();
    if let Some(route) = draft.active_route()
        && let Some(invocation) = route.invocations.get(index)
        && let Some(argument) = invocation.arguments.get(argument_index)
    {
        return argument.clone();
    }
    String::new()
}

fn route_where_clause(signals: CreateSignals) -> String {
    let draft = signals.ingestor.get();
    match draft.active_route() {
        Some(route) => route.where_clause.clone(),
        None => String::new(),
    }
}

fn flush_interval(signals: CreateSignals) -> String {
    let draft = signals.ingestor.get();
    match draft.active_route() {
        Some(route) => match &route.flush {
            FlushDraft::Each { interval, .. } => interval.clone(),
            FlushDraft::Immediate | FlushDraft::Unselected => String::new(),
        },
        None => String::new(),
    }
}

fn flush_max_batch_size(signals: CreateSignals) -> String {
    let draft = signals.ingestor.get();
    match draft.active_route() {
        Some(route) => match &route.flush {
            FlushDraft::Each { max_batch_size, .. } => max_batch_size.clone(),
            FlushDraft::Immediate | FlushDraft::Unselected => String::new(),
        },
        None => String::new(),
    }
}

#[component]
fn AssignmentRows(signals: CreateSignals, area: AssignmentArea) -> impl IntoView {
    view! {
        <For each=move || indices(area.rows(signals).len())
            key=|index| *index
            children=move |index| view! {
                <div class="create-ingestor-assignment">
                    <span class="create-ingestor-assignment-field">{move || assignment_field_name(area, signals, index)}</span>
                    <input type="text" class="create-ingestor-assignment-expression" autocomplete="off"
                        aria-label="Assignment expression"
                        prop:value=move || assignment_expression(area, signals, index)
                        disabled=move || signals.progress.get().is_pending()
                        on:input=move |event| {
                            let expression = event_target_value(&event);
                            area.with_rows(signals, |rows| {
                                if let Some(row) = rows.get_mut(index) { row.expression = expression; }
                            });
                        } />
                    <button type="button" class="create-ingestor-remove-assignment"
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| area.with_rows(signals, |rows| { if index < rows.len() { rows.remove(index); } })>
                        "Remove assignment"
                    </button>
                </div>
            } />
    }
}

#[component]
fn InvocationRows(signals: CreateSignals) -> impl IntoView {
    view! {
        <For each=move || indices(invocation_count(signals))
            key=|index| *index
            children=move |index| view! {
                <div class="create-ingestor-invocation">
                    <input type="text" class="create-ingestor-invocation-function" autocomplete="off"
                        aria-label="Function name"
                        prop:value=move || invocation_function(signals, index)
                        disabled=move || signals.progress.get().is_pending()
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            signals.ingestor.update(|draft| {
                                if let Some(route) = draft.active_route_mut()
                                    && let Some(invocation) = route.invocations.get_mut(index) { invocation.function = value; }
                            });
                            signals.edit();
                        } />
                    <For each=move || indices(invocation_argument_count(signals, index))
                        key=|argument_index| *argument_index
                        children=move |argument_index| view! {
                            <div class="create-ingestor-invocation-argument-row">
                                <input type="text" class="create-ingestor-invocation-argument" autocomplete="off"
                                    aria-label="Function argument expression"
                                    prop:value=move || invocation_argument(signals, index, argument_index)
                                    disabled=move || signals.progress.get().is_pending()
                                    on:input=move |event| {
                                        let value = event_target_value(&event);
                                        signals.ingestor.update(|draft| {
                                            if let Some(route) = draft.active_route_mut()
                                                && let Some(invocation) = route.invocations.get_mut(index)
                                                && let Some(argument) = invocation.arguments.get_mut(argument_index) { *argument = value; }
                                        });
                                        signals.edit();
                                    } />
                                <button type="button" class="create-ingestor-remove-argument"
                                    disabled=move || signals.progress.get().is_pending()
                                    on:click=move |_| {
                                        signals.ingestor.update(|draft| {
                                            if let Some(route) = draft.active_route_mut()
                                                && let Some(invocation) = route.invocations.get_mut(index)
                                                && argument_index < invocation.arguments.len() {
                                                invocation.arguments.remove(argument_index);
                                            }
                                        });
                                        signals.edit();
                                    }>"Remove argument"</button>
                            </div>
                        } />
                    <button type="button" class="create-ingestor-add-argument"
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.ingestor.update(|draft| {
                                if let Some(route) = draft.active_route_mut()
                                    && let Some(invocation) = route.invocations.get_mut(index) { invocation.arguments.push(String::new()); }
                            });
                            signals.edit();
                        }>"Add argument"</button>
                    <button type="button" class="create-ingestor-remove-invocation"
                        disabled=move || signals.progress.get().is_pending()
                        on:click=move |_| {
                            signals.ingestor.update(|draft| {
                                if let Some(route) = draft.active_route_mut()
                                    && index < route.invocations.len() { route.invocations.remove(index); }
                            });
                            signals.edit();
                        }>"Remove invocation"</button>
                </div>
            } />
    }
}

#[component]
pub(super) fn IngestorRouteEditor(
    signals: CreateSignals,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    view! {
        <section class="create-ingestor-routes">
            <h3>"Output routes"</h3>
            <div class="create-ingestor-route-list">
                <For each=move || indices(signals.ingestor.get().routes.len())
                    key=|index| *index
                    children=move |index| view! {
                        <button type="button" data-route=index.to_string()
                            class:active=move || signals.ingestor.get().active_route == index
                            disabled=pending
                            on:click=move |_| { signals.ingestor.update(|draft| draft.select_route(index)); signals.edit(); }>
                            {format!("Route {}", index + 1)}
                        </button>
                    } />
                <button type="button" class="create-ingestor-add-route" disabled=pending
                    on:click=move |_| { signals.ingestor.update(|draft| draft.add_route()); signals.edit(); }>"Add route"</button>
            </div>
            <div class="create-ingestor-route">
                <div class="create-ingestor-route-actions">
                    <button type="button" class="create-ingestor-move-up" disabled=move || pending() || signals.ingestor.get().active_route == 0
                        on:click=move |_| { signals.ingestor.update(|draft| draft.move_route_up()); signals.edit(); }>"Move up"</button>
                    <button type="button" class="create-ingestor-move-down" disabled=move || { pending() || (signals.ingestor.get().active_route + 1 >= signals.ingestor.get().routes.len()) }
                        on:click=move |_| { signals.ingestor.update(|draft| draft.move_route_down()); signals.edit(); }>"Move down"</button>
                    <button type="button" class="create-ingestor-remove-route" disabled=move || pending() || signals.ingestor.get().routes.len() == 1
                        on:click=move |_| { signals.ingestor.update(|draft| draft.remove_route()); signals.edit(); }>"Remove route"</button>
                </div>
                <fieldset class="create-field create-ingestor-branching">
                    <legend>"Output branch"</legend>
                    <button type="button" data-branch="unbranched"
                        class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Unbranched))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.choose_route_unbranched()); signals.edit(); }>"UNBRANCHED"</button>
                    <button type="button" data-branch="named"
                        class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Branched { .. }))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| draft.choose_route_branched()); signals.edit(); }>"Named branch"</button>
                </fieldset>
                <Show when=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.branch, RouteBranchDraft::Branched { .. })) fallback=|| ()>
                    <ChoiceGroup class_name="create-ingestor-branch" label="Named branch"
                        control=ChoiceControl::IngestRouteBranch signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                    <div class="create-ingestor-branch-assignments">
                        <ChoiceGroup class_name="create-ingestor-branch-fields" label="Add branch key assignment"
                            control=ChoiceControl::IngestBranchField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                        <AssignmentRows signals=signals area=AssignmentArea::Branch />
                    </div>
                </Show>
                <ChoiceGroup class_name="create-ingestor-relay" label="Output relay"
                    control=ChoiceControl::IngestRouteRelay signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                <fieldset class="create-field create-ingestor-inherit">
                    <legend>"Inherit decoded fields"</legend>
                    <For each=|| ["none", "all", "all-except", "fields"].into_iter()
                        key=|mode| *mode
                        children=move |mode| view! {
                            <button type="button" data-inherit=mode
                                class:active=move || signals.ingestor.get().active_route().is_some_and(|route| route.inherit.key() == mode)
                                disabled=pending on:click=move |_| {
                                    signals.ingestor.update(|draft| {
                                        if let Some(route) = draft.active_route_mut() { route.inherit.choose(mode); }
                                    });
                                    signals.edit();
                                }>{mode.to_ascii_uppercase()}</button>
                        } />
                </fieldset>
                <Show when=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.inherit, InheritDraft::AllExcept(_) | InheritDraft::Fields(_))) fallback=|| ()>
                    <ChoiceGroup class_name="create-ingestor-input-fields" label="Add decoded field"
                        control=ChoiceControl::IngestInputField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                    <For each=move || {
                        let draft = signals.ingestor.get();
                        let len = match draft.active_route().map(|route| &route.inherit) {
                            Some(InheritDraft::AllExcept(fields)) => fields.len(),
                            Some(InheritDraft::Fields(fields)) => fields.len(),
                            _ => 0,
                        };
                        (0..len).collect::<Vec<_>>()
                    } key=|index| *index children=move |index| view! {
                        <div class="create-ingestor-inherited-field" data-field=move || inherited_field_name(signals, index)>
                            <span>{move || inherited_field_name(signals, index)}</span>
                            <Show when=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.inherit, InheritDraft::Fields(_))) fallback=|| ()>
                                <label><input type="checkbox" class="create-ingestor-leak"
                                    prop:checked=move || signals.ingestor.get().active_route().is_some_and(|route| match &route.inherit {
                                        InheritDraft::Fields(fields) => fields.get(index).is_some_and(|field| field.leak_sensitive),
                                        _ => false,
                                    })
                                    disabled=pending on:change=move |event| {
                                        let checked = event_target_checked(&event);
                                        signals.ingestor.update(|draft| {
                                            if let Some(route) = draft.active_route_mut()
                                                && let InheritDraft::Fields(fields) = &mut route.inherit
                                                && let Some(field) = fields.get_mut(index) { field.leak_sensitive = checked; }
                                        });
                                        signals.edit();
                                    } />"LEAK SENSITIVE"</label>
                            </Show>
                            <button type="button" class="create-ingestor-remove-inherited-field" disabled=pending
                                on:click=move |_| {
                                    signals.ingestor.update(|draft| {
                                        if let Some(route) = draft.active_route_mut() {
                                            match &mut route.inherit {
                                                InheritDraft::AllExcept(fields) if index < fields.len() => { fields.remove(index); },
                                                InheritDraft::Fields(fields) if index < fields.len() => { fields.remove(index); },
                                                _ => {},
                                            }
                                        }
                                    });
                                    signals.edit();
                                }>"Remove field"</button>
                        </div>
                    } />
                </Show>
                <div class="create-ingestor-output-assignments">
                    <ChoiceGroup class_name="create-ingestor-output-fields" label="Add output assignment"
                        control=ChoiceControl::IngestOutputField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                    <AssignmentRows signals=signals area=AssignmentArea::Output />
                </div>
                <label class="create-field"><span>"Route WHERE expression (optional)"</span>
                    <textarea class="create-ingestor-where" prop:value=move || route_where_clause(signals)
                        disabled=pending on:input=move |event| {
                            let value = event_target_textarea_value(&event);
                            signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.where_clause = value; } });
                            signals.edit();
                        } />
                </label>
                <section class="create-ingestor-invocations"><h4>"Ordered invocations"</h4>
                    <InvocationRows signals=signals />
                    <button type="button" class="create-ingestor-add-invocation" disabled=pending
                        on:click=move |_| {
                            signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.invocations.push(InvocationDraft::default()); } });
                            signals.edit();
                        }>"Add invocation"</button>
                </section>
                <fieldset class="create-field create-ingestor-flush"><legend>"Flush policy"</legend>
                    <button type="button" data-flush="immediate" class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Immediate))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.flush = FlushDraft::Immediate; } }); signals.edit(); }>"IMMEDIATE"</button>
                    <button type="button" data-flush="each" class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Each { .. }))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.flush = FlushDraft::Each { interval: String::new(), max_batch_size: String::new() }; } }); signals.edit(); }>"EACH"</button>
                </fieldset>
                <Show when=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.flush, FlushDraft::Each { .. })) fallback=|| ()>
                    <label class="create-field"><span>"Flush interval"</span><input class="create-ingestor-flush-interval" type="text" autocomplete="off"
                        prop:value=move || flush_interval(signals)
                        disabled=pending on:input=move |event| {
                            let value = event_target_value(&event);
                            signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() && let FlushDraft::Each { interval, .. } = &mut route.flush { *interval = value; } });
                            signals.edit();
                        } /></label>
                    <label class="create-field"><span>"Maximum batch size"</span><input class="create-ingestor-flush-size" type="text" autocomplete="off"
                        prop:value=move || flush_max_batch_size(signals)
                        disabled=pending on:input=move |event| {
                            let value = event_target_value(&event);
                            signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() && let FlushDraft::Each { max_batch_size, .. } = &mut route.flush { *max_batch_size = value; } });
                            signals.edit();
                        } /></label>
                </Show>
                <fieldset class="create-field create-ingestor-message-error"><legend>"On message error"</legend>
                    <button type="button" data-error="ignore" class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::Ignore))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.message_error = MessageErrorDraft::Ignore; } }); signals.edit(); }>"IGNORE"</button>
                    <button type="button" data-error="log" class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::Log))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.message_error = MessageErrorDraft::Log; } }); signals.edit(); }>"LOG"</button>
                    <button type="button" data-error="send" class:active=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::SendTo { .. }))
                        disabled=pending on:click=move |_| { signals.ingestor.update(|draft| { if let Some(route) = draft.active_route_mut() { route.message_error = MessageErrorDraft::SendTo { relay: None, assignments: Vec::new() }; } }); signals.edit(); }>"SEND TO"</button>
                </fieldset>
                <Show when=move || signals.ingestor.get().active_route().is_some_and(|route| matches!(route.message_error, MessageErrorDraft::SendTo { .. })) fallback=|| ()>
                    <div class="create-ingestor-error-assignments">
                        <ChoiceGroup class_name="create-ingestor-error-relay" label="Error relay"
                            control=ChoiceControl::IngestErrorRelay signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                        <ChoiceGroup class_name="create-ingestor-error-fields" label="Add error assignment"
                            control=ChoiceControl::IngestErrorField signals=signals request_tx=request_tx session_generation=session_generation show_detail=true />
                        <AssignmentRows signals=signals area=AssignmentArea::Error />
                    </div>
                </Show>
            </div>
        </section>
    }
}

#[cfg(test)]
mod tests {
    use leptos::prelude::*;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::{DomainName, FieldName};

    use super::{
        AssignmentArea, AssignmentDraft, CreateSignals, FlushDraft, InheritDraft, InvocationDraft,
        MessageErrorDraft, assignment_expression, assignment_field_name, flush_interval,
        flush_max_batch_size, inherited_field_name, invocation_argument, invocation_argument_count,
        invocation_count, invocation_function, route_where_clause,
    };
    use crate::create_dialog::CreateKind;

    fn field(name: &str) -> FieldName {
        FieldName::parse(name).assured("test field name is valid")
    }

    #[test]
    fn branch_output_and_error_assignments_edit_independently() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(
                CreateKind::Ingestor,
                Some(DomainName::parse("orders").assured("test domain is valid")),
                "global-create-button",
            );
            signals.ingestor.update(|draft| {
                let route = draft.active_route_mut().assured("first route exists");
                route.branch.choose_branched();
                route.branch.add_assignment(field("tenant"));
                route.add_assignment(field("message"));
                route.message_error = MessageErrorDraft::SendTo {
                    relay: None,
                    assignments: vec![AssignmentDraft::selected(field("reason"))],
                };
                route.inherit = InheritDraft::Fields(Vec::new());
                route.inherit.add_field(field("message"));
            });
            assert_eq!(inherited_field_name(signals, 0), "message");
            for area in [
                AssignmentArea::Branch,
                AssignmentArea::Output,
                AssignmentArea::Error,
            ] {
                assert_eq!(area.rows(signals).len(), 1);
            }

            AssignmentArea::Branch
                .with_rows(signals, |rows| rows[0].expression = "message.tenant".into());
            AssignmentArea::Output.with_rows(signals, |rows| {
                rows[0].expression = "message.message".into()
            });
            AssignmentArea::Error
                .with_rows(signals, |rows| rows[0].expression = "error.message".into());
            assert_eq!(
                AssignmentArea::Branch.rows(signals)[0].expression,
                "message.tenant"
            );
            assert_eq!(
                AssignmentArea::Output.rows(signals)[0].expression,
                "message.message"
            );
            assert_eq!(
                AssignmentArea::Error.rows(signals)[0].expression,
                "error.message"
            );

            AssignmentArea::Branch.with_rows(signals, |rows| {
                rows.remove(0);
            });
            assert!(AssignmentArea::Branch.rows(signals).is_empty());
            assert_eq!(AssignmentArea::Output.rows(signals).len(), 1);
            assert_eq!(AssignmentArea::Error.rows(signals).len(), 1);
            signals.ingestor.update(|draft| {
                let route = draft.active_route_mut().assured("first route exists");
                route.inherit = InheritDraft::AllExcept(Vec::new());
                route.inherit.add_field(field("secret"));
            });
            assert_eq!(inherited_field_name(signals, 0), "secret");
        });
    }

    #[test]
    fn visible_route_values_follow_the_selected_route() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(
                CreateKind::Ingestor,
                Some(DomainName::parse("orders").assured("test domain is valid")),
                "global-create-button",
            );
            assert_eq!(
                assignment_field_name(AssignmentArea::Output, signals, 0),
                "Choose field"
            );
            assert_eq!(
                assignment_expression(AssignmentArea::Output, signals, 0),
                ""
            );
            assert_eq!(invocation_count(signals), 0);
            assert_eq!(invocation_function(signals, 0), "");
            assert_eq!(invocation_argument_count(signals, 0), 0);
            assert_eq!(invocation_argument(signals, 0, 0), "");
            assert_eq!(route_where_clause(signals), "");
            assert_eq!(flush_interval(signals), "");
            assert_eq!(flush_max_batch_size(signals), "");

            signals.ingestor.update(|draft| {
                let route = draft.active_route_mut().assured("first route exists");
                route.add_assignment(field("message"));
                route.assignments[0].expression = "message.message".into();
                route.where_clause = "message.message != ''".into();
                route.invocations.push(InvocationDraft {
                    function: "coalesce".into(),
                    arguments: vec!["message.message".into(), "'fallback'".into()],
                });
                route.flush = FlushDraft::Each {
                    interval: "2s".into(),
                    max_batch_size: "1MiB".into(),
                };
            });
            assert_eq!(
                assignment_field_name(AssignmentArea::Output, signals, 0),
                "message"
            );
            assert_eq!(
                assignment_expression(AssignmentArea::Output, signals, 0),
                "message.message"
            );
            assert_eq!(invocation_count(signals), 1);
            assert_eq!(invocation_function(signals, 0), "coalesce");
            assert_eq!(invocation_argument_count(signals, 0), 2);
            assert_eq!(invocation_argument(signals, 0, 1), "'fallback'");
            assert_eq!(route_where_clause(signals), "message.message != ''");
            assert_eq!(flush_interval(signals), "2s");
            assert_eq!(flush_max_batch_size(signals), "1MiB");

            signals.ingestor.update(|draft| draft.add_route());
            assert_eq!(
                assignment_field_name(AssignmentArea::Output, signals, 0),
                "Choose field"
            );
            assert_eq!(invocation_count(signals), 0);
            assert_eq!(route_where_clause(signals), "");
            assert_eq!(flush_interval(signals), "");
            signals.ingestor.update(|draft| draft.select_route(0));
            assert_eq!(invocation_function(signals, 0), "coalesce");
        });
    }
}
