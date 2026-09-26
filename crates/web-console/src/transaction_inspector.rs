//! Browser presentation of a typed transaction inspection.
//!
//! Layer: edges.
//!
//! - **Owns.** The inspected report, its viewport and selection, and the preview identity the
//!   console may send with a commit of its attached transaction.
//! - **Depends on.** The client wire request channel, report vocabulary, and impact projection.
//! - **Must not know.** How inspection is planned or persisted, or the live graph's geometry.

use std::collections::BTreeMap;

use futures_channel::mpsc::UnboundedSender;
use leptos::{ev, prelude::*};
use meticulous::OptionExt as _;
use nervix_client_wire::InspectTransactionRequest;
use nervix_models::{
    OperationImpactReport, TransactionInspection, TransactionInspectionTarget,
    TransactionOperation, TransactionOperationNumber, TransactionOperationRange,
    TransactionPosition, TransactionPreviewIdentity, TransactionStatus,
};
use nervix_web_console::graph::{
    GraphSearch,
    impact::{
        ImpactEdge, ImpactEdgeId, ImpactGraph, ImpactItem, ImpactItemId, ImpactOutcome, ImpactRole,
        ImpactView, TopologyPresence,
    },
    viewport::{Extent, GraphBounds, Viewport},
};

use crate::{ConsoleRequest, event_target_input};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PreviewKey {
    transaction_id: String,
    position: TransactionPosition,
}

/// The inspector never adopts a transaction into the session. The selected report and the
/// attached transaction's commit bases therefore have separate owners.
#[derive(Clone, Copy)]
pub(super) struct InspectorSignals {
    pub inspection: RwSignal<Option<TransactionInspection>>,
    pub target: RwSignal<Option<TransactionInspectionTarget>>,
    pub open: RwSignal<bool>,
    pub error: RwSignal<Option<String>>,
    pub refresh: RwSignal<u64>,
    latest_order: RwSignal<Option<u64>>,
    requested_order: RwSignal<Option<u64>>,
    pub stale_preview: RwSignal<bool>,
    preview_bases: RwSignal<BTreeMap<PreviewKey, TransactionPreviewIdentity>>,
}

impl InspectorSignals {
    pub fn new() -> Self {
        Self {
            inspection: RwSignal::new(None),
            target: RwSignal::new(None),
            open: RwSignal::new(false),
            error: RwSignal::new(None),
            refresh: RwSignal::new(0),
            latest_order: RwSignal::new(None),
            requested_order: RwSignal::new(None),
            stale_preview: RwSignal::new(false),
            preview_bases: RwSignal::new(BTreeMap::new()),
        }
    }

    pub fn clear(self) {
        self.inspection.set(None);
        self.target.set(None);
        self.open.set(false);
        self.error.set(None);
        self.latest_order.set(None);
        self.requested_order.set(None);
        self.stale_preview.set(false);
        self.preview_bases.set(BTreeMap::new());
    }

    pub fn open_attached(self) {
        self.inspection.set(None);
        self.open.set(true);
        self.target.set(Some(TransactionInspectionTarget::Attached));
        self.error.set(None);
    }

    pub fn open_transaction(self, transaction_id: String) {
        self.inspection.set(None);
        self.open.set(true);
        self.target
            .set(Some(TransactionInspectionTarget::Transaction {
                transaction_id,
            }));
        self.error.set(None);
    }

    /// Once the attached transaction reaches a terminal state, inspect its retained report by
    /// identity. The session has already detached it, so `Attached` can no longer read it.
    pub fn retain_finished(self, status: &TransactionStatus) {
        if status.lifecycle().is_active()
            || !self.open.get_untracked()
            || self.target.get_untracked() != Some(TransactionInspectionTarget::Attached)
            || self.inspection.get_untracked().is_none_or(|inspection| {
                inspection.transaction.transaction_id() != status.transaction_id()
            })
        {
            return;
        }
        self.target
            .set(Some(TransactionInspectionTarget::Transaction {
                transaction_id: status.transaction_id().to_string(),
            }));
        self.error.set(None);
    }

    pub fn refresh(self) {
        self.refresh.update(|generation| {
            *generation = generation
                .checked_add(1)
                .assured("a browser session cannot request 2^64 inspection refreshes");
        });
    }

    /// A DESCRIBE command carries its inspection in the typed command outcome. Its target is
    /// recorded before dispatch so a later selection can reject its delayed answer.
    pub fn prepare_describe(self, target: TransactionInspectionTarget) {
        self.target.set(Some(target));
        self.inspection.set(None);
        self.open.set(false);
        self.error.set(None);
    }

    pub fn requested(self, order: u64) {
        self.requested_order.set(Some(order));
    }

    pub fn accept(
        self,
        inspection: TransactionInspection,
        order: u64,
        request_target: Option<&TransactionInspectionTarget>,
        attached: Option<&TransactionStatus>,
    ) {
        if let Some(target) = request_target
            && self.target.get_untracked().as_ref() != Some(target)
        {
            return;
        }
        if self
            .latest_order
            .get_untracked()
            .is_some_and(|latest| order < latest)
        {
            return;
        }
        if self
            .requested_order
            .get_untracked()
            .is_some_and(|latest| order < latest)
        {
            return;
        }
        if request_target.is_none() {
            match self.target.get_untracked() {
                Some(TransactionInspectionTarget::Transaction { transaction_id })
                    if transaction_id != inspection.transaction.transaction_id() =>
                {
                    return;
                }
                Some(TransactionInspectionTarget::Attached)
                    if attached.is_none_or(|status| {
                        status.transaction_id() != inspection.transaction.transaction_id()
                    }) =>
                {
                    return;
                }
                Some(_) | None => {}
            }
        }
        if let Some(current) = self.inspection.get_untracked()
            && current.transaction.transaction_id() == inspection.transaction.transaction_id()
            && inspection.report.position() < current.report.position()
        {
            return;
        }

        if request_target.is_none() {
            self.target
                .set(Some(TransactionInspectionTarget::Transaction {
                    transaction_id: inspection.transaction.transaction_id().to_string(),
                }));
        }
        self.latest_order.set(Some(order));
        self.error.set(None);
        self.stale_preview.set(false);
        self.open.set(true);

        if let Some(status) = attached
            && status.lifecycle().is_active()
            && status.transaction_id() == inspection.transaction.transaction_id()
            && status.accepted_operations() == inspection.report.position()
        {
            let key = PreviewKey {
                transaction_id: status.transaction_id().to_string(),
                position: inspection.report.position(),
            };
            if !inspection.report.completeness().is_complete() {
                self.preview_bases.update(|bases| {
                    bases.remove(&key);
                });
                self.inspection.set(Some(inspection));
                return;
            }
            let preview = TransactionPreviewIdentity {
                transaction_id: status.transaction_id().to_string(),
                position: inspection.report.position(),
                planning_basis: inspection.report.planning_basis(),
            };
            self.preview_bases.update(|bases| {
                bases.insert(key, preview);
                // A console session retains only its most recent 256 preview positions. A
                // refresh of one position replaces its basis in place.
                while bases.len() > 256 {
                    bases.pop_first();
                }
            });
        }
        self.inspection.set(Some(inspection));
    }

    pub fn commit_basis(self, status: &TransactionStatus) -> Option<TransactionPreviewIdentity> {
        let key = PreviewKey {
            transaction_id: status.transaction_id().to_string(),
            position: status.accepted_operations(),
        };
        self.preview_bases.get_untracked().get(&key).cloned()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScopeSelection {
    Transaction,
    Step(TransactionOperationRange),
    Operation(TransactionOperationNumber),
}

#[derive(Clone, PartialEq, Eq)]
enum DetailSelection {
    Item(ImpactItemId),
    Edge(ImpactEdgeId),
}

/// The CSS canvas transform is published as one value so framing cannot render new zoom with
/// earlier pan coordinates.
#[derive(Clone, Copy)]
struct CanvasTransform {
    zoom: f64,
    x: f64,
    y: f64,
}

#[component]
pub(super) fn TransactionInspector(
    inspector: InspectorSignals,
    transaction_status: RwSignal<Option<TransactionStatus>>,
    request_tx: RwSignal<Option<UnboundedSender<ConsoleRequest>>>,
    run_command: impl Fn(Option<String>) + Copy + Send + Sync + 'static,
) -> impl IntoView {
    let lookup_id = RwSignal::new(String::new());
    let selection = RwSignal::new(ScopeSelection::Transaction);
    let view = RwSignal::new(ImpactView::Changes);
    let outcome = RwSignal::new(ImpactOutcome::Planned);
    let search = RwSignal::new(String::new());
    let drawing = RwSignal::new(None::<ImpactGraph>);
    let detail = RwSignal::new(None::<DetailSelection>);
    let transform = RwSignal::new(CanvasTransform {
        zoom: 1.0,
        x: 0.0,
        y: 0.0,
    });
    let drag = RwSignal::new(None::<(i32, i32, f64, f64)>);
    let stage = NodeRef::<leptos::html::Div>::new();
    // Command replies can repeat an unchanged status. Only a changed position, lifecycle, or
    // applied count warrants another attached inspection; a stale-preview refusal leaves these
    // unchanged and must keep its explicit stale marker until the operator refreshes.
    let attached_state = Memo::new(move |_| transaction_status.get());

    // An attached inspection is refreshed when its accepted position changes. A named
    // transaction is read without consulting or changing the session's selected domain.
    Effect::new(move |_| {
        if !inspector.open.get() {
            return;
        }
        let Some(target) = inspector.target.get() else {
            return;
        };
        let _refresh = inspector.refresh.get();
        if let TransactionInspectionTarget::Attached = target {
            let _status = attached_state.get();
        }
        let request = ConsoleRequest::InspectTransaction(InspectTransactionRequest {
            target,
            operation: None,
        });
        if let Some(tx) = request_tx.get() {
            if tx.unbounded_send(request).is_err() {
                inspector
                    .error
                    .set(Some("websocket command channel is closed".to_string()));
            }
        } else {
            inspector
                .error
                .set(Some("websocket session is not available".to_string()));
        }
    });

    Effect::new(move |_| {
        let Some(inspection) = inspector.inspection.get() else {
            drawing.set(None);
            detail.set(None);
            return;
        };
        let graph = project_impact(&inspection, selection.get(), outcome.get());
        let selection_still_exists = match detail.get_untracked() {
            Some(DetailSelection::Item(id)) => graph.items.contains_key(&id),
            Some(DetailSelection::Edge(id)) => graph.edges.contains_key(&id),
            None => true,
        };
        if !selection_still_exists {
            detail.set(None);
        }
        drawing.set(Some(graph));
    });

    let frame = move |bounds: GraphBounds, max_zoom: f64| {
        let Some(graph) = drawing.get_untracked() else {
            return;
        };
        let Some(stage) = stage.get() else {
            return;
        };
        let stage_extent = Extent {
            width: f64::from(stage.client_width()),
            height: f64::from(stage.client_height()),
        };
        let Some(framed) = Viewport::framing(
            stage_extent,
            Extent {
                width: f64::from(graph.width),
                height: f64::from(graph.height),
            },
            bounds,
            max_zoom,
        ) else {
            return;
        };
        let (center_x, center_y) = bounds.center();
        // This canvas has a fixed top-left origin. The translation therefore depends only on
        // the region being framed, even when the canvas is larger than its clipped stage.
        transform.set(CanvasTransform {
            zoom: framed.zoom,
            x: stage_extent.width / 2.0 - framed.zoom * center_x,
            y: stage_extent.height / 2.0 - framed.zoom * center_y,
        });
    };
    let fit = move || {
        let Some(graph) = drawing.get_untracked() else {
            return;
        };
        frame(graph.canvas_bounds(), Viewport::FIT_MAX_ZOOM);
    };
    let focus_edge = move |id: &ImpactEdgeId| {
        let Some(graph) = drawing.get_untracked() else {
            return;
        };
        let Some(bounds) = graph.edge_bounds(id) else {
            return;
        };
        frame(bounds, Viewport::MAX_ZOOM);
    };
    Effect::new(move |_| {
        let _graph = drawing.get();
        let _stage = stage.get();
        fit();
    });
    Effect::new(move |_| {
        let Some(query) = GraphSearch::parse(&search.get()) else {
            return;
        };
        let Some(graph) = drawing.get() else {
            return;
        };
        let Some(bounds) = graph.search_bounds(&query) else {
            return;
        };
        frame(bounds, Viewport::MAX_ZOOM);
    });

    view! {
        <Show when=move || inspector.open.get() fallback=|| ()>
            <section class="transaction-inspector" aria-label="Transaction inspector">
                <header class="inspector-toolbar">
                    <strong>"Transaction inspector"</strong>
                    <button type="button" on:click=move |_| inspector.refresh()>"Refresh"</button>
                    <button type="button" aria-label="Close transaction inspector" on:click=move |_| inspector.open.set(false)>"Close"</button>
                </header>
                <div class="inspector-lookup">
                    <button type="button" on:click=move |_| run_command(Some("SHOW TRANSACTIONS;".to_string()))>"Discover transactions"</button>
                    <Show when=move || transaction_status.get().is_some_and(|status| status.lifecycle().is_active()) fallback=|| ()>
                        <button type="button" on:click=move |_| inspector.open_attached()>"Attached transaction"</button>
                    </Show>
                    <label>"Transaction ID" <input type="text" prop:value=move || lookup_id.get() on:input=move |event| lookup_id.set(event_target_input(&event).value()) /></label>
                    <button type="button" on:click=move |_| {
                        let id = lookup_id.get_untracked().trim().to_string();
                        if !id.is_empty() { inspector.open_transaction(id); }
                    }>"Inspect ID"</button>
                </div>
                <Show when=move || inspector.error.get().is_some() fallback=|| ()>
                    <p class="inspector-error" role="alert">{move || inspector.error.get().unwrap_or_default()}</p>
                </Show>
                <Show when=move || inspector.inspection.get().is_some() fallback=|| view! { <p class="inspector-loading">"Waiting for inspection report…"</p> }>
                    <div class="inspector-body">
                        <aside class="inspector-outline">
                            <div class="inspector-summary">
                                {move || inspection_summary(inspector.inspection.get().as_ref(), transaction_status.get().as_ref(), inspector.stale_preview.get())}
                            </div>
                            <button type="button" class="scope-select" on:click=move |_| selection.set(ScopeSelection::Transaction)>"Whole transaction · scoped union"</button>
                            <For each=move || match inspector.inspection.get() {
                                Some(value) => value.report.execution_steps().to_vec(),
                                None => Vec::new(),
                            }
                                key=|step| step.operations()
                                children=move |step| {
                                    let range = step.operations();
                                    view! {
                                        <button type="button" class="step-select" on:click=move |_| selection.set(ScopeSelection::Step(range))>
                                            {move || match inspector.inspection.get() {
                                                Some(value) => match value.report.execution_steps().iter().find(|current| current.operations() == range) {
                                                    Some(current) => format!("Execution step {}–{} · {}", range.first(), range.last(), current.actual().outcome.as_ref()),
                                                    None => String::new(),
                                                },
                                                None => String::new(),
                                            }}
                                        </button>
                                        <For each=move || match inspector.inspection.get() {
                                            Some(value) => range.operations().filter_map(|number| value.report.operations().get(number.index()).cloned()).collect::<Vec<_>>(),
                                            None => Vec::new(),
                                        }
                                            key=|operation| operation.number
                                            children=move |operation| {
                                                let number = operation.number;
                                                view! { <button type="button" class="operation-select" on:click=move |_| selection.set(ScopeSelection::Operation(number))>
                                                    {move || match inspector.inspection.get() {
                                                        Some(value) => match value.report.operations().get(number.index()) {
                                                            Some(current) => format!("Operation {} · {}", number, operation_label(current)),
                                                            None => String::new(),
                                                        },
                                                        None => String::new(),
                                                    }}
                                                </button> }
                                            }
                                        />
                                    }
                                }
                            />
                            <p class="inspector-scope">{move || scope_summary(inspector.inspection.get().as_ref(), selection.get(), outcome.get())}</p>
                            <details class="inspector-relation-list">
                                <summary>"Relations"</summary>
                                <For each=move || match drawing.get() {
                                    Some(graph) => graph.edges.into_values().collect::<Vec<_>>(),
                                    None => Vec::new(),
                                }
                                    key=|edge| (edge.id.clone(), edge.presence)
                                    children=move |edge| {
                                        let id = edge.id;
                                        let selected = id.clone();
                                        let relation = format!("{:?}", id.relation);
                                        let source = id.source.name().to_string();
                                        let target = id.target.name().to_string();
                                        let label = format!("{relation}: {source} → {target} ({:?})", edge.presence);
                                        view! {
                                            <button type="button" class="inspector-relation-select"
                                                data-relation=relation data-source=source data-target=target
                                                on:click=move |_| {
                                                    detail.set(Some(DetailSelection::Edge(selected.clone())));
                                                    focus_edge(&selected);
                                                }
                                            >{label}</button>
                                        }
                                    }
                                />
                            </details>
                        </aside>
                        <div class="inspector-visual">
                            <div class="inspector-controls">
                                <button type="button" class:active=move || view.get() == ImpactView::Before on:click=move |_| view.set(ImpactView::Before)>"Before"</button>
                                <button type="button" class:active=move || view.get() == ImpactView::Changes on:click=move |_| view.set(ImpactView::Changes)>"Changes"</button>
                                <button type="button" class:active=move || view.get() == ImpactView::After on:click=move |_| view.set(ImpactView::After)>"After"</button>
                                <button type="button" class:active=move || outcome.get() == ImpactOutcome::Planned on:click=move |_| outcome.set(ImpactOutcome::Planned)>"Planned"</button>
                                <button type="button" class:active=move || outcome.get() == ImpactOutcome::Actual on:click=move |_| outcome.set(ImpactOutcome::Actual)>"Actual"</button>
                                <label>"Search" <input type="search" prop:value=move || search.get() on:input=move |event| search.set(event_target_input(&event).value()) /></label>
                                <button type="button" aria-label="Zoom out" on:click=move |_| transform.update(|value| value.zoom = (value.zoom - Viewport::ZOOM_STEP).max(Viewport::MIN_ZOOM))>"−"</button>
                                <button type="button" aria-label="Zoom in" on:click=move |_| transform.update(|value| value.zoom = (value.zoom + Viewport::ZOOM_STEP).min(Viewport::MAX_ZOOM))>"+"</button>
                                <button type="button" class="inspector-fit" on:click=move |_| fit()>"FIT"</button>
                            </div>
                            <div class="inspector-stage" node_ref=stage
                                on:mousedown=move |event: ev::MouseEvent| {
                                    let current = transform.get_untracked();
                                    drag.set(Some((event.client_x(), event.client_y(), current.x, current.y)));
                                }
                                on:mousemove=move |event: ev::MouseEvent| {
                                    if let Some((x, y, start_x, start_y)) = drag.get_untracked() {
                                        transform.update(|value| {
                                            value.x = start_x + f64::from(event.client_x() - x);
                                            value.y = start_y + f64::from(event.client_y() - y);
                                        });
                                    }
                                }
                                on:mouseup=move |_| drag.set(None)
                                on:mouseleave=move |_| drag.set(None)
                                on:wheel=move |event: ev::WheelEvent| {
                                    if event.ctrl_key() || event.meta_key() {
                                        event.prevent_default();
                                        transform.update(|value| value.zoom = (value.zoom - event.delta_y() * 0.001).clamp(Viewport::MIN_ZOOM, Viewport::MAX_ZOOM));
                                    }
                                }
                            >
                                <Show when=move || drawing.get().is_some() fallback=|| ()>
                                    <div class="inspector-canvas" style=move || {
                                        let graph = drawing.get().verified("a mounted inspector canvas retains its graph until unmount");
                                        let transform = transform.get();
                                        format!("width:{}px;height:{}px;transform:translate({}px, {}px) scale({})", graph.width, graph.height, transform.x, transform.y, transform.zoom)
                                    }>
                                        <svg class="inspector-relations" viewBox=move || {
                                            let graph = drawing.get().verified("a mounted inspector canvas retains its graph until unmount");
                                            format!("0 0 {} {}", graph.width, graph.height)
                                        }>
                                            <For each=move || match drawing.get() {
                                                Some(graph) => graph.groups,
                                                None => Vec::new(),
                                            }
                                                key=|group| (group.branch.clone(), group.bands.clone())
                                                children=move |group| {
                                                    let name = group.branch.to_string();
                                                    view! {
                                                        <path class="inspector-branch" d=group.outline() data-branch=name />
                                                    }
                                                }
                                            />
                                            <For each=move || match drawing.get() {
                                                Some(graph) => graph.edges.into_values().collect::<Vec<_>>(),
                                                None => Vec::new(),
                                            }
                                                key=|edge| (edge.id.clone(), edge.presence, edge.route.points.clone())
                                                children=move |edge| {
                                                    let edge_id = edge.id.clone();
                                                    let relation = format!("{:?}", edge_id.relation);
                                                    let source = edge_id.source.name().to_string();
                                                    let target = edge_id.target.name().to_string();
                                                    let path = edge_path(&edge);
                                                    view! {
                                                        <path class="inspector-edge"
                                                            class:absent=move || !view.get().shows(edge.presence)
                                                            class:added=edge.presence == TopologyPresence::After
                                                            class:dropped=edge.presence == TopologyPresence::Before
                                                            class:transient=edge.presence == TopologyPresence::Transient
                                                            d=path
                                                            data-source=source
                                                            data-target=target
                                                            data-relation=relation.clone()
                                                            on:click=move |_| {
                                                                detail.set(Some(DetailSelection::Edge(edge_id.clone())));
                                                                focus_edge(&edge_id);
                                                            }
                                                        />
                                                    }
                                                }
                                            />
                                        </svg>
                                        <For each=move || match drawing.get() {
                                            Some(graph) => graph.groups,
                                            None => Vec::new(),
                                        }
                                            key=|group| (group.branch.clone(), group.bands.clone())
                                            children=move |group| {
                                                let Some(header) = group.header_anchor() else {
                                                    return ().into_any();
                                                };
                                                view! {
                                                    <span class="inspector-branch-label" style=format!(
                                                        "left:{}px;top:{}px", header.x + 8, header.y,
                                                    )>{group.branch.to_string()}</span>
                                                }.into_any()
                                            }
                                        />
                                        {move || {
                                            let graph = drawing.get()?;
                                            let outline = graph.domain.outline?;
                                            let frame = outline.frame;
                                            let label = outline.label;
                                            Some(view! {
                                                <div class="inspector-domain-outline" style=format!(
                                                    "left:{}px;top:{}px;width:{}px;height:{}px",
                                                    frame.x, frame.y, frame.width, frame.height,
                                                )>
                                                    <span class="inspector-domain-label" style=format!(
                                                        "left:{}px;top:{}px",
                                                        label.x - frame.x, label.y - frame.y,
                                                    )>"Whole domain pause"</span>
                                                </div>
                                            })
                                        }}
                                        <For each=move || match drawing.get() {
                                            Some(graph) => graph.items.into_values().collect::<Vec<_>>(),
                                            None => Vec::new(),
                                        }
                                            key=|item| (
                                                item.id.clone(),
                                                item.rect,
                                                item.presence,
                                                item.roles.keys().map(role_label).collect::<Vec<_>>().join(" · "),
                                            )
                                            children=move |item| {
                                                let id = item.id.clone();
                                                let name = id.name().to_string();
                                                let kind = id.caption().to_string();
                                                let style = format!("left:{}px;top:{}px;width:{}px;height:{}px", item.rect.x, item.rect.y, item.rect.width, item.rect.height);
                                                let marks = item.roles.keys().map(role_label).collect::<Vec<_>>().join(" · ");
                                                let aria_label = format!("{} {}: {}", kind, name, marks);
                                                view! {
                                                    <button type="button" class="inspector-item"
                                                        class:absent=move || !view.get().shows(item.presence)
                                                        class:added=item.presence == TopologyPresence::After
                                                        class:dropped=item.presence == TopologyPresence::Before
                                                        class:transient=item.presence == TopologyPresence::Transient
                                                        style=style
                                                        data-name=name.clone()
                                                        data-kind=kind.clone()
                                                        aria-label=aria_label
                                                        on:click=move |_| detail.set(Some(DetailSelection::Item(id.clone())))
                                                    >
                                                        <span class="inspector-kind">{kind.clone()}</span>
                                                        <span class="inspector-name">{name.clone()}</span>
                                                        <span class="inspector-marks">{marks.clone()}</span>
                                                    </button>
                                                }
                                            }
                                        />
                                    </div>
                                </Show>
                            </div>
                            <div class="inspector-detail" aria-live="polite">
                                {move || detail_text(
                                    drawing.get().as_ref(),
                                    detail.get().as_ref(),
                                    inspector.inspection.get().as_ref(),
                                    outcome.get(),
                                )}
                            </div>
                        </div>
                    </div>
                </Show>
            </section>
        </Show>
    }
}

fn project_impact(
    inspection: &TransactionInspection,
    selection: ScopeSelection,
    outcome: ImpactOutcome,
) -> ImpactGraph {
    let report = &inspection.report;
    match selection {
        ScopeSelection::Transaction => ImpactGraph::transaction(report, outcome),
        ScopeSelection::Step(range) => {
            match report
                .execution_steps()
                .iter()
                .find(|step| step.operations() == range)
            {
                Some(step) => ImpactGraph::execution_step(step, outcome),
                None => ImpactGraph::transaction(report, outcome),
            }
        }
        ScopeSelection::Operation(number) => match report.operations().get(number.index()) {
            Some(operation) if outcome == ImpactOutcome::Planned => {
                ImpactGraph::operation(operation)
            }
            Some(operation) => {
                match report
                    .execution_steps()
                    .iter()
                    .find(|step| step.operations() == operation.execution_step)
                {
                    Some(step) => ImpactGraph::execution_step(step, outcome),
                    None => ImpactGraph::transaction(report, outcome),
                }
            }
            None => ImpactGraph::transaction(report, outcome),
        },
    }
}

fn inspection_summary(
    inspection: Option<&TransactionInspection>,
    attached: Option<&TransactionStatus>,
    stale_preview: bool,
) -> String {
    let Some(inspection) = inspection else {
        return "Waiting for inspection report…".to_string();
    };
    let status = &inspection.transaction;
    let report = &inspection.report;
    let freshness = match attached {
        _ if stale_preview => "STALE",
        Some(attached)
            if attached.transaction_id() == status.transaction_id()
                && attached.accepted_operations() > report.position() =>
        {
            "STALE"
        }
        _ => "CURRENT",
    };
    let completeness = report.completeness().as_ref();
    let effects = report
        .execution_steps()
        .iter()
        .map(|step| &step.planned().effects);
    let mut moves = 0usize;
    let mut rebuilds = 0usize;
    let mut resets = 0usize;
    for effect in effects {
        moves += effect.ownership_moves.len();
        rebuilds += effect.rebuilds.len();
        resets += effect.state_resets.len();
    }
    let diagnostics = report
        .completeness()
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let mut summary = format!(
        "Transaction: {}\nDomain: {}\nState: {}\nPreview: {} · {}\nOperations: {} accepted, {} \
         applied, {} pending\nQuiesce: {}\nPlanning basis: {}\nEffects: {} relocations, {} \
         rebuilds, {} state resets",
        status.transaction_id(),
        status.domain(),
        status.lifecycle().as_ref(),
        freshness,
        completeness,
        status.accepted_operations().accepted_operations(),
        status.applied_operations(),
        status.pending_operations(),
        report.summary().level().as_str(),
        report.planning_basis(),
        moves,
        rebuilds,
        resets,
    );
    if !diagnostics.is_empty() {
        summary.push_str("\nDiagnostics: ");
        summary.push_str(&diagnostics);
    }
    summary
}

fn operation_label(operation: &OperationImpactReport) -> String {
    match &operation.operation {
        TransactionOperation::CreateConfiguration { node, .. } => {
            format!("CREATE {} {}", node.kind.keyword_phrase(), node.identifier)
        }
        TransactionOperation::AlterConfiguration { node, .. } => {
            format!("ALTER {} {}", node.kind.keyword_phrase(), node.identifier)
        }
        TransactionOperation::DropConfiguration { node, .. } => {
            format!("DROP {} {}", node.kind.keyword_phrase(), node.identifier)
        }
        TransactionOperation::AlterDomain { domain } => format!("ALTER DOMAIN {domain}"),
        TransactionOperation::StartDomain { domain } => format!("START DOMAIN {domain}"),
        TransactionOperation::StopDomain { domain } => format!("STOP DOMAIN {domain}"),
        TransactionOperation::CreateResource { resource, .. } => {
            format!("CREATE RESOURCE {resource}")
        }
        TransactionOperation::RebindResource { resource, .. } => {
            format!("REBIND RESOURCE {resource}")
        }
        TransactionOperation::ResetWasmState { processor, .. } => {
            format!("RESET WASM PROCESSOR {processor}")
        }
    }
}

fn scope_summary(
    inspection: Option<&TransactionInspection>,
    scope: ScopeSelection,
    outcome: ImpactOutcome,
) -> String {
    let Some(inspection) = inspection else {
        return String::new();
    };
    match scope {
        ScopeSelection::Transaction => format!(
            "Transaction scoped union · {} execution steps",
            inspection.report.execution_steps().len()
        ),
        ScopeSelection::Step(range) => format!(
            "Execution step {}–{} effective scope · {:?}",
            range.first(),
            range.last(),
            outcome
        ),
        ScopeSelection::Operation(number) => {
            let Some(operation) = inspection.report.operations().get(number.index()) else {
                return String::new();
            };
            let reasons = operation
                .reasons
                .iter()
                .map(|reason| format!("{reason:?}"))
                .collect::<Vec<_>>()
                .join("; ");
            if outcome == ImpactOutcome::Actual {
                format!(
                    "Operation {number} belongs to execution step {}–{}; actual outcomes belong \
                     to the effective step. {reasons}",
                    operation.execution_step.first(),
                    operation.execution_step.last()
                )
            } else {
                format!(
                    "Operation {number} contribution; containing execution step {}–{} has the \
                     effective pause and effects. {reasons}",
                    operation.execution_step.first(),
                    operation.execution_step.last()
                )
            }
        }
    }
}

fn role_label(role: &ImpactRole) -> String {
    match role {
        ImpactRole::Configuration(_) => "CONFIGURATION",
        ImpactRole::Pause { .. } => "PAUSE",
        ImpactRole::Gate { .. } => "GATE",
        ImpactRole::Move { .. } => "MOVE",
        ImpactRole::Rebuild { .. } => "REBUILD",
        ImpactRole::StateReset { .. } => "STATE RESET",
        ImpactRole::ForceFlush { .. } => "FLUSH",
        ImpactRole::Activation { .. } => "ACTIVATION",
        ImpactRole::Binding { .. } => "BINDING",
        ImpactRole::Catalog(_) => "CATALOG",
    }
    .to_string()
}

fn edge_path(edge: &ImpactEdge) -> String {
    let mut path = String::new();
    for (index, (x, y)) in edge.route.points.iter().enumerate() {
        if index == 0 {
            path.push_str(&format!("M{x} {y}"));
        } else {
            path.push_str(&format!(" L{x} {y}"));
        }
    }
    path
}

fn detail_text(
    graph: Option<&ImpactGraph>,
    selection: Option<&DetailSelection>,
    inspection: Option<&TransactionInspection>,
    outcome: ImpactOutcome,
) -> String {
    let Some(graph) = graph else {
        return String::new();
    };
    match selection {
        Some(DetailSelection::Item(id)) => {
            let Some(item) = graph.items.get(id) else {
                return String::new();
            };
            format!(
                "{} · {}",
                item_detail(item),
                impact_explanation(inspection, &item.contributors, outcome)
            )
        }
        Some(DetailSelection::Edge(id)) => {
            let Some(edge) = graph.edges.get(id) else {
                return String::new();
            };
            format!(
                "{} → {} · {:?} · {:?} · operations {} · routed through {} points · {}",
                id.source.name(),
                id.target.name(),
                id.relation,
                edge.presence,
                contributors(&edge.contributors),
                edge.route.points.len(),
                impact_explanation(inspection, &edge.contributors, outcome),
            )
        }
        None => "Select a node or relation to inspect its recorded impact.".to_string(),
    }
}

fn impact_explanation(
    inspection: Option<&TransactionInspection>,
    contributors: &nervix_web_console::graph::impact::Contributors,
    outcome: ImpactOutcome,
) -> String {
    let Some(inspection) = inspection else {
        return String::new();
    };
    let explanations = contributors
        .operations()
        .filter_map(|number| {
            let operation = inspection.report.operations().get(number.index())?;
            let reasons = operation
                .reasons
                .iter()
                .map(|reason| format!("{reason:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            let actual = match inspection
                .report
                .execution_steps()
                .iter()
                .find(|step| step.operations() == operation.execution_step)
            {
                Some(step) => step.actual().outcome.as_ref(),
                None => "UNAVAILABLE",
            };
            Some(format!(
                "operation {number}: {} · step actual {actual}",
                if reasons.is_empty() {
                    "no recorded reason".to_string()
                } else {
                    reasons
                }
            ))
        })
        .collect::<Vec<_>>()
        .join("; ");
    format!("{outcome:?} · {explanations}")
}

fn item_detail(item: &ImpactItem) -> String {
    let roles = item
        .roles
        .iter()
        .map(|(role, operations)| format!("{:?} (operations {})", role, contributors(operations)))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{} {} · {:?} · before {:?} · after {:?} · operations {} · {}",
        item.id.caption(),
        item.id.name(),
        item.presence,
        item.branches_before,
        item.branches_after,
        contributors(&item.contributors),
        roles
    )
}

fn contributors(contributors: &nervix_web_console::graph::impact::Contributors) -> String {
    contributors
        .operations()
        .map(|number| number.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use any_spawner::Executor;
    use leptos::prelude::Owner;
    use meticulous::ResultExt as _;
    use nervix_models::{
        ActualExecutionStepImpact, AttributedImpactNode, CanonicalImpactSet, ConfigurationImpact,
        ConfigurationTransition, DomainName, ExecutionStepImpactReport, ImpactAttribution,
        ImpactDiagnostic, ImpactDiagnosticKind, ImpactEffects, ImpactNodeCoverage,
        ImpactPlanningBasis, ImpactReportCompleteness, ImpactTopology, ModelKind, ModelName,
        NodeRef, PlannedExecutionStepImpact, TransactionImpactReport, TransactionLifecycle,
    };

    use super::*;

    fn inspection(id: &str, basis: u8) -> TransactionInspection {
        let domain =
            DomainName::parse("inspected").assured("the test's domain identifier is valid");
        let report = TransactionImpactReport::new(
            domain.clone(),
            TransactionPosition::new(0),
            ImpactPlanningBasis::new([basis; 32]),
            ImpactReportCompleteness::Complete,
            Vec::new(),
            Vec::new(),
        )
        .assured("an empty report has no operation-step inconsistencies");
        let transaction = TransactionStatus::new(
            id.to_string(),
            domain,
            TransactionLifecycle::Open,
            TransactionPosition::new(0),
            0,
        )
        .assured("the test status has no applied operations");
        TransactionInspection {
            transaction,
            operation: None,
            report,
        }
    }

    fn configured_inspection() -> TransactionInspection {
        let domain =
            DomainName::parse("inspected").assured("the test's domain identifier is valid");
        let name = ModelName::parse("inspected_event").assured("the test's node name is valid");
        let node = NodeRef::new(ModelKind::Schema, name);
        let number = TransactionOperationNumber::from_index(0)
            .assured("the first accepted operation has an index");
        let range = TransactionOperationRange::new(number, number)
            .assured("a one-operation range is valid");
        let attribution = ImpactAttribution::new([number])
            .assured("the test impact names one accepted operation");
        let effects = ImpactEffects {
            changed_configuration: CanonicalImpactSet::new([ConfigurationImpact {
                transition: ConfigurationTransition::Created { node: node.clone() },
                attribution: attribution.clone(),
            }]),
            topology: nervix_models::AffectedTopology {
                before: ImpactTopology::default(),
                after: ImpactTopology {
                    nodes: CanonicalImpactSet::new([AttributedImpactNode {
                        coverage: ImpactNodeCoverage::configuration(node.clone()),
                        attribution,
                    }]),
                    edges: CanonicalImpactSet::default(),
                },
            },
            ..ImpactEffects::default()
        };
        let report = TransactionImpactReport::new(
            domain.clone(),
            TransactionPosition::new(1),
            ImpactPlanningBasis::new([9; 32]),
            ImpactReportCompleteness::Complete,
            vec![OperationImpactReport {
                number,
                operation: TransactionOperation::CreateConfiguration {
                    domain: domain.clone(),
                    node,
                },
                execution_step: range,
                completeness: ImpactReportCompleteness::Complete,
                reasons: Vec::new(),
                contribution: effects.clone(),
            }],
            vec![ExecutionStepImpactReport::new(
                range,
                PlannedExecutionStepImpact {
                    completeness: ImpactReportCompleteness::Complete,
                    pause: nervix_models::PauseRequirement::NoPause,
                    effects,
                },
                ActualExecutionStepImpact::unattempted(),
            )],
        )
        .assured("the one operation belongs to its one execution step");
        let transaction = TransactionStatus::new(
            "attached".to_string(),
            domain,
            TransactionLifecycle::Open,
            TransactionPosition::new(1),
            0,
        )
        .assured("the accepted operation has not applied");
        TransactionInspection {
            transaction,
            operation: None,
            report,
        }
    }

    #[test]
    fn inspecting_another_transaction_preserves_the_attached_commit_basis() {
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            let attached = inspection("attached", 1);
            inspector.open_attached();
            inspector.requested(1);
            inspector.accept(
                attached.clone(),
                1,
                Some(&TransactionInspectionTarget::Attached),
                Some(&attached.transaction),
            );
            let basis = inspector
                .commit_basis(&attached.transaction)
                .assured("the complete attached inspection supplied a basis");

            inspector.open_transaction("other".to_string());
            inspector.requested(2);
            let other_target = TransactionInspectionTarget::Transaction {
                transaction_id: "other".to_string(),
            };
            inspector.accept(
                inspection("other", 2),
                2,
                Some(&other_target),
                Some(&attached.transaction),
            );

            assert_eq!(inspector.commit_basis(&attached.transaction), Some(basis));
            assert_eq!(
                inspector
                    .inspection
                    .get_untracked()
                    .assured("the other transaction was inspected")
                    .transaction
                    .transaction_id(),
                "other"
            );
        });
    }

    #[test]
    fn a_delayed_reply_cannot_replace_the_newer_preview_basis() {
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            let first = inspection("attached", 1);
            inspector.open_attached();
            inspector.requested(1);
            inspector.accept(
                first.clone(),
                1,
                Some(&TransactionInspectionTarget::Attached),
                Some(&first.transaction),
            );
            inspector.requested(3);
            inspector.accept(
                inspection("attached", 2),
                2,
                Some(&TransactionInspectionTarget::Attached),
                Some(&first.transaction),
            );
            assert_eq!(
                inspector
                    .commit_basis(&first.transaction)
                    .assured("the first inspection supplied a basis")
                    .planning_basis,
                first.report.planning_basis()
            );
            let current = inspection("attached", 3);
            inspector.accept(
                current.clone(),
                3,
                Some(&TransactionInspectionTarget::Attached),
                Some(&first.transaction),
            );
            assert_eq!(
                inspector
                    .commit_basis(&first.transaction)
                    .assured("the current inspection supplied a basis")
                    .planning_basis,
                current.report.planning_basis()
            );
        });
    }

    #[test]
    fn incomplete_refresh_invalidates_the_attached_preview_at_that_position() {
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            let complete = configured_inspection();
            inspector.open_attached();
            inspector.requested(1);
            inspector.accept(
                complete.clone(),
                1,
                Some(&TransactionInspectionTarget::Attached),
                Some(&complete.transaction),
            );
            assert!(inspector.commit_basis(&complete.transaction).is_some());

            let mut incomplete = complete.clone();
            incomplete.report = TransactionImpactReport::new(
                incomplete.report.domain().clone(),
                incomplete.report.position(),
                incomplete.report.planning_basis(),
                ImpactReportCompleteness::incomplete(vec![ImpactDiagnostic {
                    kind: ImpactDiagnosticKind::Planning,
                    operation: None,
                    message: "planning input changed".to_string(),
                }])
                .assured("the incomplete report has one diagnostic"),
                incomplete.report.operations().to_vec(),
                incomplete.report.execution_steps().to_vec(),
            )
            .assured("the report still has the same operation and step");
            inspector.requested(2);
            inspector.accept(
                incomplete.clone(),
                2,
                Some(&TransactionInspectionTarget::Attached),
                Some(&complete.transaction),
            );
            assert_eq!(inspector.commit_basis(&complete.transaction), None);
            assert!(
                inspection_summary(Some(&incomplete), Some(&complete.transaction), false)
                    .contains("planning input changed")
            );

            inspector.clear();
            assert!(inspector.inspection.get_untracked().is_none());
            assert!(inspector.target.get_untracked().is_none());
            assert!(!inspector.open.get_untracked());
        });
    }

    #[test]
    fn selection_ignores_an_answer_for_a_previous_transaction() {
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            inspector.open_transaction("current".to_string());
            inspector.requested(2);
            let previous = TransactionInspectionTarget::Transaction {
                transaction_id: "previous".to_string(),
            };
            inspector.accept(inspection("previous", 1), 1, Some(&previous), None);
            assert!(inspector.inspection.get_untracked().is_none());
            assert!(inspector.open.get_untracked());

            inspector.prepare_describe(TransactionInspectionTarget::Attached);
            assert!(!inspector.open.get_untracked());
            assert_eq!(
                inspector.target.get_untracked(),
                Some(TransactionInspectionTarget::Attached)
            );
            inspector.refresh();
            assert_eq!(inspector.refresh.get_untracked(), 1);
        });
    }

    #[test]
    fn a_finished_attached_transaction_switches_to_its_retained_identity() {
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            let current = configured_inspection();
            inspector.open_attached();
            inspector.requested(1);
            inspector.accept(
                current.clone(),
                1,
                Some(&TransactionInspectionTarget::Attached),
                Some(&current.transaction),
            );
            let finished = TransactionStatus::new(
                current.transaction.transaction_id().to_string(),
                current.transaction.domain().clone(),
                TransactionLifecycle::Committed,
                current.report.position(),
                1,
            )
            .assured("the one accepted operation has applied");
            inspector.retain_finished(&finished);
            assert_eq!(
                inspector.target.get_untracked(),
                Some(TransactionInspectionTarget::Transaction {
                    transaction_id: finished.transaction_id().to_string(),
                })
            );
            assert!(inspector.open.get_untracked());
        });
    }

    #[test]
    fn inspector_renders_the_typed_report_summary_and_controls() {
        static EXECUTOR: Once = Once::new();
        EXECUTOR.call_once(|| {
            Executor::init_futures_executor()
                .assured("the test process initializes the Leptos executor once");
        });
        Owner::new().with(|| {
            let inspector = InspectorSignals::new();
            let inspected = configured_inspection();
            inspector.inspection.set(Some(inspected.clone()));
            inspector.open.set(true);
            let status = RwSignal::new(Some(inspected.transaction));
            let request_tx = RwSignal::new(None);
            let props = TransactionInspectorProps::builder()
                .inspector(inspector)
                .transaction_status(status)
                .request_tx(request_tx)
                .run_command(|_| {})
                .build();
            let view = TransactionInspector(props);
            Executor::poll_local();
            let markup = view.to_html();
            assert!(markup.contains("Transaction inspector"));
            assert!(markup.contains("COMPLETE"));
            assert!(markup.contains("DYNAMIC"));
            assert!(markup.contains("Whole transaction"));
            assert!(markup.contains("inspected_event"));
            assert!(markup.contains("Operation 1"));
            assert!(markup.contains("inspector-item"));
        });
    }

    #[test]
    fn scope_projection_keeps_operation_contribution_separate_from_actual_step() {
        let inspected = configured_inspection();
        let operation = inspected.report.operations()[0].number;
        let step = inspected.report.execution_steps()[0].operations();
        let planned = project_impact(
            &inspected,
            ScopeSelection::Operation(operation),
            ImpactOutcome::Planned,
        );
        let actual = project_impact(
            &inspected,
            ScopeSelection::Operation(operation),
            ImpactOutcome::Actual,
        );
        assert_eq!(planned.items.len(), 1);
        assert!(actual.items.is_empty());
        assert_eq!(
            project_impact(
                &inspected,
                ScopeSelection::Step(step),
                ImpactOutcome::Planned
            )
            .items
            .len(),
            1
        );
        assert!(
            scope_summary(
                Some(&inspected),
                ScopeSelection::Operation(operation),
                ImpactOutcome::Planned
            )
            .contains("contribution")
        );
        assert!(
            scope_summary(
                Some(&inspected),
                ScopeSelection::Operation(operation),
                ImpactOutcome::Actual
            )
            .contains("effective step")
        );
        assert!(
            scope_summary(
                Some(&inspected),
                ScopeSelection::Step(step),
                ImpactOutcome::Planned
            )
            .contains("effective scope")
        );
        let next_operation = TransactionOperationNumber::from_index(1)
            .assured("the second operation number has an index");
        let missing_step = TransactionOperationRange::new(next_operation, next_operation)
            .assured("a one-operation range is valid");
        assert_eq!(
            project_impact(
                &inspected,
                ScopeSelection::Operation(next_operation),
                ImpactOutcome::Planned,
            )
            .items
            .len(),
            1
        );
        assert_eq!(
            project_impact(
                &inspected,
                ScopeSelection::Step(missing_step),
                ImpactOutcome::Planned,
            )
            .items
            .len(),
            1
        );
        let summary = inspection_summary(Some(&inspected), Some(&inspected.transaction), false);
        assert!(summary.contains("1 accepted, 0 applied"));
        assert!(summary.contains("CURRENT"));
        assert!(
            inspection_summary(Some(&inspected), Some(&inspected.transaction), true)
                .contains("STALE")
        );
        let newer_status = TransactionStatus::new(
            inspected.transaction.transaction_id().to_string(),
            inspected.transaction.domain().clone(),
            TransactionLifecycle::Open,
            TransactionPosition::new(2),
            0,
        )
        .assured("the newer attached status has not applied any operation");
        assert!(inspection_summary(Some(&inspected), Some(&newer_status), false).contains("STALE"));
        let item = planned
            .items
            .values()
            .next()
            .assured("the created schema is projected");
        let details = item_detail(item);
        assert!(details.contains("inspected_event"));
        assert!(details.contains("Created"));
        assert!(details.contains("operations 1"));
        let selected_detail = detail_text(
            Some(&planned),
            Some(&DetailSelection::Item(item.id.clone())),
            Some(&inspected),
            ImpactOutcome::Planned,
        );
        assert!(selected_detail.contains("Created"));
        assert!(selected_detail.contains("step actual UNATTEMPTED"));
        assert!(
            detail_text(
                Some(&planned),
                None,
                Some(&inspected),
                ImpactOutcome::Planned
            )
            .contains("Select a node")
        );
    }
}
