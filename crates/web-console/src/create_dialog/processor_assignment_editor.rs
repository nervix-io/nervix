//! Ordered SET rows shared by processor routes, branch keys, error routes, and state defaults.
//!
//! Layer: edges.
//!
//! - **Owns.** One reusable browser editor for assignment order, expressions, and removal.
//! - **Depends on.** Processor drafts and the shared typed assignment draft.
//! - **Must not know.** Expression execution or output routing.

use leptos::prelude::*;

use super::{
    CreateSignals, event_target_value,
    ingestor_route_draft::{AssignmentDraft, MessageErrorDraft, RouteBranchDraft},
    processor_draft::{ProcessorFamily, StatePolicyDraft},
    processor_editor::indices,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AssignmentArea {
    State,
    Output,
    Branch,
    Error,
}

impl AssignmentArea {
    fn rows(self, signals: CreateSignals, family: ProcessorFamily) -> Vec<AssignmentDraft> {
        let draft = signals.processor(family).get();
        match self {
            Self::State => match draft.active_state() {
                Some(state) => match &state.policy {
                    StatePolicyDraft::Default(rows) => rows.clone(),
                    _ => Vec::new(),
                },
                None => Vec::new(),
            },
            Self::Output => match draft.active_route() {
                Some(route) => route.assignments.clone(),
                None => Vec::new(),
            },
            Self::Branch => match draft.active_route() {
                Some(route) => match &route.branch {
                    RouteBranchDraft::Branched { assignments, .. } => assignments.clone(),
                    _ => Vec::new(),
                },
                None => Vec::new(),
            },
            Self::Error => match draft.active_route() {
                Some(route) => match &route.message_error {
                    MessageErrorDraft::SendTo { assignments, .. } => assignments.clone(),
                    _ => Vec::new(),
                },
                None => Vec::new(),
            },
        }
    }

    fn with_rows(
        self,
        signals: CreateSignals,
        family: ProcessorFamily,
        action: impl FnOnce(&mut Vec<AssignmentDraft>),
    ) {
        signals.processor(family).update(|draft| match self {
            Self::State => {
                if let Some(state) = draft.active_state_mut()
                    && let StatePolicyDraft::Default(rows) = &mut state.policy
                {
                    action(rows);
                }
            }
            Self::Output => {
                if let Some(route) = draft.active_route_mut() {
                    action(&mut route.assignments);
                }
            }
            Self::Branch => {
                if let Some(route) = draft.active_route_mut()
                    && let RouteBranchDraft::Branched { assignments, .. } = &mut route.branch
                {
                    action(assignments);
                }
            }
            Self::Error => {
                if let Some(route) = draft.active_route_mut()
                    && let MessageErrorDraft::SendTo { assignments, .. } = &mut route.message_error
                {
                    action(assignments);
                }
            }
        });
        signals.edit();
    }

    fn expression_class(self) -> &'static str {
        match self {
            Self::State => "create-processor-state-assignment-expression",
            Self::Output => "create-processor-assignment-expression",
            Self::Branch => "create-processor-branch-assignment-expression",
            Self::Error => "create-processor-error-assignment-expression",
        }
    }
}

#[component]
pub(super) fn ProcessorAssignmentRows(
    family: ProcessorFamily,
    area: AssignmentArea,
    signals: CreateSignals,
) -> impl IntoView {
    let expression_class = area.expression_class();
    let pending = move || signals.progress.get().is_pending();
    view! {
        <For each=move || indices(area.rows(signals, family).len())
            key=|index| *index
            children=move |index| view! {
                <div class="create-processor-assignment-row">
                    <span class="create-processor-assignment-field">{move || {
                        let rows = area.rows(signals, family);
                        match rows.get(index).and_then(|row| row.field.as_ref()) {
                            Some(field) => field.name().to_string(),
                            None => "Choose field".to_string(),
                        }
                    }}</span>
                    <input type="text" class=expression_class autocomplete="off" aria-label="Assignment expression"
                        prop:value=move || {
                            let rows = area.rows(signals, family);
                            match rows.get(index) {
                                Some(row) => row.expression.clone(),
                                None => String::new(),
                            }
                        }
                        disabled=pending
                        on:input=move |event| {
                            let value = event_target_value(&event);
                            area.with_rows(signals, family, |rows| {
                                if let Some(row) = rows.get_mut(index) { row.expression = value; }
                            });
                        } />
                    <button type="button" class="create-processor-assignment-up"
                        disabled=move || { pending() || index == 0 }
                        on:click=move |_| area.with_rows(signals, family, |rows| {
                            if index > 0 && index < rows.len() { rows.swap(index, index - 1); }
                        })>"Move up"</button>
                    <button type="button" class="create-processor-assignment-down"
                        disabled=move || { pending() || index + 1 >= area.rows(signals, family).len() }
                        on:click=move |_| area.with_rows(signals, family, |rows| {
                            if index + 1 < rows.len() { rows.swap(index, index + 1); }
                        })>"Move down"</button>
                    <button type="button" class="create-processor-assignment-remove" disabled=pending
                        on:click=move |_| area.with_rows(signals, family, |rows| {
                            if index < rows.len() { rows.remove(index); }
                        })>"Remove assignment"</button>
                </div>
            } />
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::FieldName;

    use super::*;
    use crate::create_dialog::{CreateKind, ingestor_route_draft::RouteBranchDraft};

    #[test]
    fn assignment_areas_keep_state_output_branch_and_error_rows_independent() {
        Owner::new().with(|| {
            let signals = CreateSignals::new();
            signals.open(CreateKind::Reingestor, None, "global-create-button");
            signals.reingestor.update(|draft| {
                draft.add_state();
                draft.active_state_mut().assured("state exists").policy =
                    StatePolicyDraft::Default(Vec::new());
                let route = draft.active_route_mut().assured("route exists");
                route.branch = RouteBranchDraft::Branched {
                    branch: None,
                    assignments: Vec::new(),
                };
                route.message_error = MessageErrorDraft::SendTo {
                    relay: None,
                    assignments: Vec::new(),
                };
            });

            for (area, class_name) in [
                (
                    AssignmentArea::State,
                    "create-processor-state-assignment-expression",
                ),
                (
                    AssignmentArea::Output,
                    "create-processor-assignment-expression",
                ),
                (
                    AssignmentArea::Branch,
                    "create-processor-branch-assignment-expression",
                ),
                (
                    AssignmentArea::Error,
                    "create-processor-error-assignment-expression",
                ),
            ] {
                assert_eq!(area.expression_class(), class_name);
                area.with_rows(signals, ProcessorFamily::Reingestor, |rows| {
                    rows.push(AssignmentDraft::selected(
                        FieldName::parse("value").assured("valid field"),
                    ));
                });
                assert_eq!(area.rows(signals, ProcessorFamily::Reingestor).len(), 1);
            }
        });
    }
}
