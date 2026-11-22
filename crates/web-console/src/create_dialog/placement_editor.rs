//! Structured placement controls for the create popup.
//!
//! Layer: edges.
//!
//! - **Owns.** The placement form's name, whether it relates hops or selects nodes, its
//!   domain-wide, corridor or member coverage, the ordered `FROM`, `TO` and `FOR` member lists with
//!   their move and remove actions, the colocation or node policy choice, the node selector and
//!   the rank.
//! - **Depends on.** Browser draft signals, the shared choice UI, and the session's typed
//!   placement-member and policy choices.
//! - **Must not know.** Which runtime nodes are eligible, graph validation, or how a command is
//!   dispatched.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use leptos::prelude::*;
use nervix_models::ModelName;

use super::{
    ChoiceControl, ChoiceGroup, CreateSignals, RequestSender, event_target_value,
    placement_draft::{
        CorridorSide, MemberChange, MemberContext, MemberMove, NodePolicyChoice, PlacementCoverage,
        PlacementDraft, PlacementForm,
    },
};

#[component]
pub(super) fn PlacementEditor(
    signals: CreateSignals,
    name_input: NodeRef<leptos::html::Input>,
    request_tx: RwSignal<Option<RequestSender>>,
    session_generation: RwSignal<u64>,
) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let node_rule = move || signals.placement.get().form == PlacementForm::Nodes;
    let corridor = move || {
        let draft = signals.placement.get();
        draft.form == PlacementForm::Colocation && draft.coverage != PlacementCoverage::DomainWide
    };
    let listed = move || {
        let draft = signals.placement.get();
        draft.form == PlacementForm::Nodes && draft.coverage != PlacementCoverage::DomainWide
    };
    let domain_wide = move || signals.placement.get().coverage == PlacementCoverage::DomainWide;
    let selects = move || signals.placement.get().node_policy.selects();
    let node_policy = move |choice: NodePolicyChoice| signals.placement.get().node_policy == choice;
    view! {
        <label class="create-field">
            <span>"Placement name"</span>
            <input node_ref=name_input class="create-name" type="text" autocomplete="off"
                prop:value=move || signals.placement.get().name
                disabled=pending
                on:input=move |event| {
                    let name = event_target_value(&event);
                    signals.placement.update(|draft| draft.name = name);
                    signals.edit();
                } />
        </label>
        <fieldset class="create-choice-group create-placement-form">
            <legend>"Rule"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-form="colocation"
                    class:active=move || !node_rule()
                    disabled=pending
                    on:click=move |_| choose_form(signals, PlacementForm::Colocation)
                >"Colocation"</button>
                <button type="button" data-form="nodes"
                    class:active=node_rule
                    disabled=pending
                    on:click=move |_| choose_form(signals, PlacementForm::Nodes)
                >"Cluster nodes"</button>
            </div>
        </fieldset>
        <fieldset class="create-choice-group create-placement-coverage">
            <legend>"Coverage"</legend>
            <div class="create-choice-buttons">
                <button type="button" data-scope="domain"
                    class:active=domain_wide
                    disabled=pending
                    on:click=move |_| choose_coverage(signals, PlacementCoverage::DomainWide)
                >"Entire domain"</button>
                <Show when=move || !node_rule() fallback=move || view! {
                    <button type="button" data-scope="members"
                        class:active=listed
                        disabled=pending
                        on:click=move |_| choose_coverage(signals, PlacementCoverage::Members)
                    >"Members"</button>
                }>
                    <button type="button" data-scope="corridor"
                        class:active=corridor
                        disabled=pending
                        on:click=move |_| choose_coverage(signals, PlacementCoverage::Corridor)
                    >"Corridor"</button>
                </Show>
            </div>
        </fieldset>
        <Show when=corridor fallback=|| ()>
            <ChoiceGroup class_name="create-placement-from" label="FROM members"
                control=ChoiceControl::PlacementFromMember signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <CorridorMembers side=CorridorSide::From signals=signals />
            <ChoiceGroup class_name="create-placement-to" label="TO members"
                control=ChoiceControl::PlacementToMember signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <CorridorMembers side=CorridorSide::To signals=signals />
            <Show when=move || signals.placement.with(PlacementDraft::has_changed_members) fallback=|| ()>
                <p class="create-reference-invalid" role="alert">"A member was selected in another domain. Remove it or select it again."</p>
            </Show>
        </Show>
        <Show when=listed fallback=|| ()>
            <ChoiceGroup class_name="create-placement-for" label="FOR members"
                control=ChoiceControl::PlacementForMember signals=signals request_tx=request_tx
                session_generation=session_generation show_detail=true />
            <CorridorMembers side=CorridorSide::For signals=signals />
            <Show when=move || signals.placement.with(PlacementDraft::has_changed_members) fallback=|| ()>
                <p class="create-reference-invalid" role="alert">"A member was selected in another domain. Remove it or select it again."</p>
            </Show>
        </Show>
        <Show when=node_rule fallback=move || view! {
            <ChoiceGroup class_name="create-placement-options" label="Placement"
                control=ChoiceControl::PlacementPolicy signals=signals request_tx=request_tx
                session_generation=session_generation />
        }>
            <fieldset class="create-choice-group create-placement-node-policy">
                <legend>"Nodes"</legend>
                <div class="create-choice-buttons">
                    <button type="button" data-node-policy="require"
                        class:active=move || node_policy(NodePolicyChoice::Require)
                        disabled=pending
                        on:click=move |_| choose_node_policy(signals, NodePolicyChoice::Require)
                    >"REQUIRE NODES WHERE"</button>
                    <button type="button" data-node-policy="prefer"
                        class:active=move || node_policy(NodePolicyChoice::Prefer)
                        disabled=pending
                        on:click=move |_| choose_node_policy(signals, NodePolicyChoice::Prefer)
                    >"PREFER NODES WHERE"</button>
                    <button type="button" data-node-policy="any"
                        class:active=move || node_policy(NodePolicyChoice::Any)
                        disabled=pending
                        on:click=move |_| choose_node_policy(signals, NodePolicyChoice::Any)
                    >"ANY NODE"</button>
                </div>
            </fieldset>
            <Show when=selects fallback=|| ()>
                <label class="create-field">
                    <span>"Node selector"</span>
                    <input class="create-placement-selector" type="text" autocomplete="off"
                        spellcheck="false"
                        placeholder="tag('zone') = 'eu'"
                        prop:value=move || signals.placement.get().selector
                        disabled=pending
                        on:input=move |event| {
                            let selector = event_target_value(&event);
                            signals.placement.update(|draft| draft.selector = selector);
                            signals.edit();
                        } />
                </label>
            </Show>
        </Show>
        <label class="create-field">
            <span>"Rank (optional)"</span>
            <input class="create-placement-rank" type="text" inputmode="numeric"
                prop:value=move || signals.placement.get().rank
                disabled=pending
                on:input=move |event| {
                    let rank = event_target_value(&event);
                    signals.placement.update(|draft| draft.rank = rank);
                    signals.edit();
                } />
        </label>
    }
    .into_any()
}

fn choose_coverage(signals: CreateSignals, coverage: PlacementCoverage) {
    signals.placement.update(|draft| draft.coverage = coverage);
    signals.edit();
}

fn choose_form(signals: CreateSignals, form: PlacementForm) {
    signals.placement.update(|draft| draft.choose_form(form));
    signals.edit();
}

fn choose_node_policy(signals: CreateSignals, policy: NodePolicyChoice) {
    signals.placement.update(|draft| draft.node_policy = policy);
    signals.edit();
}

/// Where a member sits within its side, which decides the ways it can move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RowEnds {
    Only,
    First,
    Middle,
    Last,
}

impl RowEnds {
    fn of(index: usize, count: usize) -> Self {
        let last = count.checked_sub(1);
        match (index, last) {
            (0, Some(0)) => Self::Only,
            (0, _) => Self::First,
            (index, Some(last)) if index == last => Self::Last,
            _ => Self::Middle,
        }
    }

    fn can_move_earlier(self) -> bool {
        match self {
            Self::Middle | Self::Last => true,
            Self::Only | Self::First => false,
        }
    }

    fn can_move_later(self) -> bool {
        match self {
            Self::First | Self::Middle => true,
            Self::Only | Self::Last => false,
        }
    }
}

/// One member row: keyed by everything it shows and every action it takes, so a moved or
/// invalidated member re-renders with its new place.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MemberRow {
    index: usize,
    name: ModelName,
    context: MemberContext,
    ends: RowEnds,
}

fn member_rows(draft: &PlacementDraft, side: CorridorSide) -> Vec<MemberRow> {
    let members = draft.members(side);
    let mut rows = Vec::with_capacity(members.len());
    for (index, (name, context)) in members.iter().enumerate() {
        rows.push(MemberRow {
            index,
            name: name.clone(),
            context: *context,
            ends: RowEnds::of(index, members.len()),
        });
    }
    rows
}

fn members_class(side: CorridorSide) -> &'static str {
    match side {
        CorridorSide::From => "create-placement-members create-placement-from-members",
        CorridorSide::To => "create-placement-members create-placement-to-members",
        CorridorSide::For => "create-placement-members create-placement-for-members",
    }
}

/// One side's members in the order the rule writes them.
#[component]
fn CorridorMembers(side: CorridorSide, signals: CreateSignals) -> impl IntoView {
    let rows = move || signals.placement.with(|draft| member_rows(draft, side));
    view! {
        <ol class=members_class(side) aria-label=format!("{} members", side.keyword())>
            <For
                each=rows
                key=|row| row.clone()
                children=move |row| view! { <CorridorMember row=row side=side signals=signals /> }
            />
        </ol>
    }
    .into_any()
}

#[component]
fn CorridorMember(row: MemberRow, side: CorridorSide, signals: CreateSignals) -> impl IntoView {
    let pending = move || signals.progress.get().is_pending();
    let name = row.name.to_string();
    let index = row.index;
    let ends = row.ends;
    let edit = move |change: MemberChange| {
        signals
            .placement
            .update(|draft| draft.change_member(side, index, change));
        signals.edit();
    };
    view! {
        <li class="create-placement-member" class:invalid=row.context == MemberContext::Changed data-member=name.clone()>
            <span class="create-placement-member-name">{name.clone()}</span>
            <button type="button" class="create-placement-member-up"
                aria-label=format!("Move {name} up")
                disabled=move || pending() || !ends.can_move_earlier()
                on:click=move |_| edit(MemberChange::Move(MemberMove::Earlier))
            >"↑"</button>
            <button type="button" class="create-placement-member-down"
                aria-label=format!("Move {name} down")
                disabled=move || pending() || !ends.can_move_later()
                on:click=move |_| edit(MemberChange::Move(MemberMove::Later))
            >"↓"</button>
            <button type="button" class="create-placement-member-remove"
                aria-label=format!("Remove {name}")
                disabled=pending
                on:click=move |_| edit(MemberChange::Remove)
            >"×"</button>
        </li>
    }
}
