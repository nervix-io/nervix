//! Repository tooling, outside the product layer order.
//! Owns: resolved language, configuration-model, cluster-read and state contracts.
//! Depends on: compiler types, source-owned roles and the supported local effect graph.
//! Must not know: product filenames, identifier spelling or inferred write-once lifetimes.

use ahash::{HashMap, HashSet, HashSetExt};
use rustc_hir::{
    Expr, ExprKind, HirId, MatchSource, Node,
    def::{DefKind, Res},
};
use rustc_infer::infer::TyCtxtInferExt;
use rustc_lint::LateContext;
use rustc_lint_defs::{Lint, declare_tool_lint};
use rustc_middle::ty::{self, Ty, TyCtxt};
use rustc_span::{Span, Symbol, def_id::DefId};
use rustc_trait_selection::traits::type_known_to_meet_bound_modulo_regions;

use crate::contracts::{Flow, INVALID_CONTRACT, contract_attribute};

declare_tool_lint! { pub nervix::MODEL_IN_DATA_PLANE, Deny, "execution consumes plans rather than configuration models", report_in_external_macro: true }
declare_tool_lint! { pub nervix::PARSER_DEPENDENCY, Deny, "language definitions belong only to language and edge consumers", report_in_external_macro: true }
declare_tool_lint! { pub nervix::CLUSTER_AWAIT, Deny, "recurring execution retains published cluster observations", report_in_external_macro: true }
declare_tool_lint! { pub nervix::NODE_IDENTITY, Deny, "declared node identities use the shared typed vocabulary", report_in_external_macro: true }
declare_tool_lint! { pub nervix::INSTALLED_STATE, Deny, "installation-only fields cannot use replaceable read-write locks", report_in_external_macro: true }
declare_tool_lint! { pub nervix::STATE_CONTRACT, Deny, "optional names and shared handles behind read-write locks need an explicit lifetime contract", report_in_external_macro: true }

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Layer {
    Primitives,
    Vocabulary,
    Language,
    Engine,
    Decisions,
    DataPlane,
    ControlPlane,
    Edge,
    Harness,
}

impl Layer {
    fn parse(kind: &str) -> Option<Self> {
        Some(match kind {
            "primitives" => Self::Primitives,
            "vocabulary" => Self::Vocabulary,
            "language" => Self::Language,
            "engine" => Self::Engine,
            "decisions" => Self::Decisions,
            "data_plane" => Self::DataPlane,
            "control_plane" => Self::ControlPlane,
            "edge" => Self::Edge,
            "harness" => Self::Harness,
            _ => return None,
        })
    }

    fn permits_parser(self) -> bool {
        matches!(self, Self::Language | Self::Edge | Self::Harness)
    }

    fn consumes_plans(self) -> bool {
        matches!(self, Self::DataPlane)
    }
}

fn kind(attribute: &rustc_hir::Attribute) -> Option<Symbol> {
    let arguments = attribute.meta_item_list()?;
    if arguments.len() != 2 {
        return None;
    }
    let kind = arguments[0].meta_item()?;
    let reason = arguments[1].meta_item()?;
    if !kind.is_word()
        || !reason.has_name(Symbol::intern("reason"))
        || !reason
            .value_str()
            .is_some_and(|text| !text.as_str().trim().is_empty())
    {
        return None;
    }
    Some(kind.ident()?.name)
}

fn declared(tcx: TyCtxt<'_>, definition: DefId, name: &str) -> Option<Symbol> {
    kind(contract_attribute(tcx, definition, name)?)
}

/// An explicit trait/method boundary can be inherited; its unrelated module default cannot
/// replace the lexical owner of an implementation body.
fn trait_layer(tcx: TyCtxt<'_>, mut definition: DefId) -> Option<Layer> {
    loop {
        if matches!(tcx.def_kind(definition), DefKind::Mod) {
            return None;
        }
        if let Some(value) = declared(tcx, definition, "layer") {
            return Layer::parse(value.as_str());
        }
        definition = tcx.opt_parent(definition)?;
    }
}

/// The narrowest architectural owner wins, including trait and impl-self metadata.
pub fn layer(tcx: TyCtxt<'_>, mut definition: DefId) -> Option<Layer> {
    loop {
        if let Some(value) = declared(tcx, definition, "layer") {
            return Layer::parse(value.as_str());
        }
        if let Some(item) = tcx.opt_associated_item(definition)
            && let Some(trait_item) = item.trait_item_def_id()
            && let Some(value) = trait_layer(tcx, trait_item)
        {
            return Some(value);
        }
        if matches!(tcx.def_kind(definition), DefKind::Impl { .. })
            && let ty::Adt(adt, _) = tcx
                .type_of(definition)
                .instantiate_identity()
                .skip_norm_wip()
                .kind()
            && let Some(value) = declared(tcx, adt.did(), "layer")
        {
            return Layer::parse(value.as_str());
        }
        definition = tcx.opt_parent(definition)?;
    }
}

fn value_role(tcx: TyCtxt<'_>, mut definition: DefId, role: &str) -> bool {
    loop {
        if declared(tcx, definition, "value").is_some_and(|kind| kind.as_str() == role) {
            return true;
        }
        if matches!(tcx.def_kind(definition), DefKind::Mod) {
            return false;
        }
        let Some(parent) = tcx.opt_parent(definition) else {
            return false;
        };
        definition = parent;
    }
}

fn consumes_plans(tcx: TyCtxt<'_>, mut definition: DefId) -> bool {
    if layer(tcx, definition).is_some_and(Layer::consumes_plans) {
        return true;
    }
    loop {
        if declared(tcx, definition, "input").is_some_and(|kind| kind.as_str() == "plans") {
            return true;
        }
        if let Some(trait_item) = tcx
            .opt_associated_item(definition)
            .and_then(|item| item.trait_item_def_id())
            && consumes_plans(tcx, trait_item)
        {
            return true;
        }
        if matches!(tcx.def_kind(definition), DefKind::Impl { .. })
            && let ty::Adt(adt, _) = tcx
                .type_of(definition)
                .instantiate_identity()
                .skip_norm_wip()
                .kind()
            && consumes_plans(tcx, adt.did())
        {
            return true;
        }
        let Some(parent) = tcx.opt_parent(definition) else {
            return false;
        };
        definition = parent;
    }
}

pub fn validate(cx: &LateContext<'_>, node: HirId, attribute: &rustc_hir::Attribute) {
    let path = attribute.path();
    let Some(name) = path.get(1) else {
        return;
    };
    let owner = cx.tcx.hir_node(node);
    let named_type = matches!(owner, Node::Item(item) if matches!(item.kind,
        rustc_hir::ItemKind::Struct(..) | rustc_hir::ItemKind::Enum(..) | rustc_hir::ItemKind::Union(..)));
    let callable = matches!(
        owner,
        Node::Item(rustc_hir::Item {
            kind: rustc_hir::ItemKind::Fn { .. },
            ..
        }) | Node::ImplItem(rustc_hir::ImplItem {
            kind: rustc_hir::ImplItemKind::Fn(..),
            ..
        }) | Node::TraitItem(rustc_hir::TraitItem {
            kind: rustc_hir::TraitItemKind::Fn(..),
            ..
        })
    );
    let module = matches!(
        owner,
        Node::Crate(_)
            | Node::Item(rustc_hir::Item {
                kind: rustc_hir::ItemKind::Mod(..),
                ..
            })
    );
    let contract = kind(attribute);
    let valid = match name.as_str() {
        "layer" => {
            (module
                || named_type
                || callable
                || matches!(
                    owner,
                    Node::Item(rustc_hir::Item {
                        kind: rustc_hir::ItemKind::Impl(..),
                        ..
                    }) | Node::Item(rustc_hir::Item {
                        kind: rustc_hir::ItemKind::Trait { .. },
                        ..
                    })
                ))
                && contract.is_some_and(|kind| Layer::parse(kind.as_str()).is_some())
        }
        "value" => {
            (named_type
                || matches!(
                    owner,
                    Node::Item(rustc_hir::Item {
                        kind: rustc_hir::ItemKind::Trait { .. },
                        ..
                    })
                ))
                && contract.is_some_and(|kind| {
                    matches!(kind.as_str(), "model" | "name" | "cluster_access")
                })
        }
        "identity" => {
            (named_type
                || callable
                || matches!(
                    owner,
                    Node::Field(_)
                        | Node::Param(_)
                        | Node::Item(rustc_hir::Item {
                            kind: rustc_hir::ItemKind::TyAlias(..),
                            ..
                        })
                ))
                && contract.is_some_and(|kind| kind.as_str() == "node")
                && (!named_type
                    || layer(cx.tcx, node.owner.def_id.to_def_id()) == Some(Layer::Vocabulary))
        }
        "input" => {
            (module
                || named_type
                || callable
                || matches!(
                    owner,
                    Node::Item(rustc_hir::Item {
                        kind: rustc_hir::ItemKind::Trait { .. },
                        ..
                    })
                ))
                && contract.is_some_and(|kind| kind.as_str() == "plans")
        }
        "state" => {
            matches!(owner, Node::Field(_))
                && contract.is_some_and(|kind| matches!(kind.as_str(), "installed" | "mutable"))
        }
        _ => return,
    };
    let count = cx
        .tcx
        .hir_attrs(node)
        .iter()
        .filter(|other| other.path() == path)
        .count();
    if !valid || count != 1 {
        cx.tcx.emit_node_span_lint(
            INVALID_CONTRACT,
            node,
            attribute.span(),
            crate::contracts::Message {
                message: format!(
                    "{name} requires its supported kind and a meaningful reason on its exact \
                     architectural owner; duplicate or misplaced contracts are forbidden"
                ),
            },
        );
    }
}

struct ClusterWait {
    owner: rustc_span::def_id::LocalDefId,
    node: HirId,
    span: Span,
    definition: DefId,
}

#[derive(Default)]
pub struct ArchitectureRules {
    emitted: HashSet<(HirId, Symbol)>,
    cluster_waits: Vec<ClusterWait>,
    bindings: HashMap<HirId, HirId>,
}

impl ArchitectureRules {
    fn diagnostic(
        &mut self,
        cx: &LateContext<'_>,
        node: HirId,
        span: Span,
        lint: &'static Lint,
        message: String,
        flow: &mut Flow,
    ) {
        if self
            .emitted
            .insert((node, Symbol::intern(&lint.name_lower())))
        {
            flow.diagnostic(cx, node, span, lint, message);
        }
    }

    fn normalize<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> Ty<'tcx> {
        match cx
            .tcx
            .try_normalize_erasing_regions(cx.typing_env(), ty::Unnormalized::new_wip(value))
        {
            Ok(value) => value,
            Err(_) => value,
        }
    }

    fn has_role<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>, role: &str) -> bool {
        self.normalize(cx, value).walk().any(|argument| {
            let Some(value) = argument.as_type() else {
                return false;
            };
            match value.kind() {
                ty::Adt(adt, _) => value_role(cx.tcx, adt.did(), role),
                ty::Dynamic(predicates, _) => predicates
                    .principal_def_id()
                    .is_some_and(|definition| value_role(cx.tcx, definition, role)),
                _ => false,
            }
        })
    }

    fn check_type<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        node: HirId,
        span: Span,
        value: Ty<'tcx>,
        flow: &mut Flow,
    ) {
        let consumer = layer(cx.tcx, node.owner.def_id.to_def_id());
        if !consumer.is_some_and(Layer::permits_parser)
            && self.normalize(cx, value).walk().any(|argument| {
                let Some(value) = argument.as_type() else {
                    return false;
                };
                let definition = match value.kind() {
                    ty::Adt(adt, _) => Some(adt.did()),
                    ty::Dynamic(predicates, _) => predicates.principal_def_id(),
                    ty::FnDef(definition, _) => Some(*definition),
                    _ => None,
                };
                definition
                    .is_some_and(|definition| layer(cx.tcx, definition) == Some(Layer::Language))
            })
        {
            self.diagnostic(
                cx,
                node,
                span,
                PARSER_DEPENDENCY,
                format!(
                    "resolved interface or body type {value} carries a language definition; \
                     consume vocabulary at this owner"
                ),
                flow,
            );
        }
        if !self.has_role(cx, value, "model") {
            return;
        }
        let owner = node.owner.def_id;
        match layer(cx.tcx, owner.to_def_id()) {
            Some(_) if consumes_plans(cx.tcx, owner.to_def_id()) => self.diagnostic(
                cx,
                node,
                span,
                MODEL_IN_DATA_PLANE,
                format!(
                    "execution interface or body carries configuration model {value}; consume its \
                     validated execution plan"
                ),
                flow,
            ),
            None => self.diagnostic(
                cx,
                node,
                span,
                INVALID_CONTRACT,
                "configuration model consumer has no source-owned architectural layer".into(),
                flow,
            ),
            _ => {}
        }
    }

    pub fn reference(
        &mut self,
        cx: &LateContext<'_>,
        node: HirId,
        span: Span,
        definition: DefId,
        flow: &mut Flow,
    ) {
        let owner = node.owner.def_id;
        let consumer = layer(cx.tcx, owner.to_def_id());
        if layer(cx.tcx, definition) == Some(Layer::Language) {
            match consumer {
                Some(value) if value.permits_parser() => {}
                _ => self.diagnostic(
                    cx,
                    node,
                    span,
                    PARSER_DEPENDENCY,
                    format!(
                        "resolved language reference {} requires a language, edge or harness \
                         owner; other layers consume vocabulary",
                        cx.tcx.def_path_str(definition)
                    ),
                    flow,
                ),
            }
        }
        if value_role(cx.tcx, definition, "model") {
            match consumer {
                Some(_) if consumes_plans(cx.tcx, owner.to_def_id()) => self.diagnostic(
                    cx,
                    node,
                    span,
                    MODEL_IN_DATA_PLANE,
                    "execution references a configuration model definition; consume its validated \
                     execution plan"
                        .into(),
                    flow,
                ),
                None => self.diagnostic(
                    cx,
                    node,
                    span,
                    INVALID_CONTRACT,
                    "configuration model consumer has no source-owned architectural layer".into(),
                    flow,
                ),
                _ => {}
            }
        }
        // Abstract associated types have no type_of query in foreign metadata. Their concrete
        // projections are normalized at the consuming signature/field/body, not at a bare path.
        if matches!(cx.tcx.def_kind(definition), DefKind::TyAlias) {
            self.check_type(
                cx,
                node,
                span,
                cx.tcx
                    .type_of(definition)
                    .instantiate_identity()
                    .skip_norm_wip(),
                flow,
            );
        }
    }

    pub fn field(
        &mut self,
        cx: &LateContext<'_>,
        field: &rustc_hir::FieldDef<'_>,
        flow: &mut Flow,
    ) {
        let value = cx
            .tcx
            .type_of(field.def_id)
            .instantiate_identity()
            .skip_norm_wip();
        self.check_type(cx, field.hir_id, field.span, value, flow);
        self.identity(
            cx,
            field.hir_id,
            field.span,
            value,
            field.def_id.to_def_id(),
            flow,
        );
        let state = declared(cx.tcx, field.def_id.to_def_id(), "state");
        let mut installation_candidate = false;
        for argument in self.normalize(cx, value).walk() {
            let Some(value) = argument.as_type() else {
                continue;
            };
            let ty::Adt(adt, arguments) = value.kind() else {
                continue;
            };
            if !self.rwlock(cx.tcx, adt.did()) {
                continue;
            }
            if state.is_some_and(|kind| kind.as_str() == "installed") {
                self.diagnostic(
                    cx,
                    field.hir_id,
                    field.span,
                    INSTALLED_STATE,
                    "installation-only state requires immutable construction or one-time \
                     initialization, rather than a replaceable RwLock"
                        .into(),
                    flow,
                );
            }
            for payload in arguments.types() {
                let ty::Adt(option, arguments) = payload.kind() else {
                    continue;
                };
                if cx
                    .tcx
                    .is_diagnostic_item(Symbol::intern("Option"), option.did())
                {
                    let payload = arguments.type_at(0);
                    installation_candidate |=
                        self.has_role(cx, payload, "name") || self.shared_value(cx, payload);
                }
            }
        }
        if installation_candidate && state.is_none() {
            self.diagnostic(
                cx,
                field.hir_id,
                field.span,
                STATE_CONTRACT,
                "RwLock<Option<name or shared handle>> needs an installed or mutable field \
                 contract; its representation does not prove its lifetime"
                    .into(),
                flow,
            );
        }
    }

    fn rwlock(&self, tcx: TyCtxt<'_>, definition: DefId) -> bool {
        matches!(
            crate::definition_path(tcx, definition).as_str(),
            "lock_api::rwlock::RwLock"
                | "std::sync::poison::rwlock::RwLock"
                | "tokio::sync::rwlock::RwLock"
                | "shuttle::sync::rwlock::RwLock"
                | "shuttle_tokio_impl_inner::sync::rwlock::RwLock"
                | "shuttle::future::rwlock::RwLock"
                | "loom::sync::rwlock::RwLock"
                | "nervix_primitives::sync::blocking::tracked::RwLock"
        )
    }

    fn shared_value<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> bool {
        self.normalize(cx, value).walk().any(|argument| {
            let Some(value) = argument.as_type() else {
                return false;
            };
            let ty::Adt(adt, _) = value.kind() else {
                return false;
            };
            matches!(
                crate::definition_path(cx.tcx, adt.did()).as_str(),
                "alloc::sync::Arc" | "triomphe::arc::Arc"
            )
        })
    }

    fn typed_identity<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> bool {
        let value = self.normalize(cx, value).peel_refs();
        match value.kind() {
            ty::Slice(element) | ty::Array(element, _) => self.typed_identity(cx, *element),
            ty::Adt(adt, arguments) => {
                if declared(cx.tcx, adt.did(), "identity")
                    .is_some_and(|kind| kind.as_str() == "node")
                {
                    return layer(cx.tcx, adt.did()) == Some(Layer::Vocabulary);
                }
                let wrapper = cx
                    .tcx
                    .is_diagnostic_item(Symbol::intern("Option"), adt.did())
                    || cx.tcx.is_diagnostic_item(Symbol::intern("Vec"), adt.did())
                    || cx
                        .tcx
                        .is_diagnostic_item(Symbol::intern("Result"), adt.did())
                    || matches!(
                        crate::definition_path(cx.tcx, adt.did()).as_str(),
                        "alloc::boxed::Box" | "alloc::sync::Arc" | "triomphe::arc::Arc"
                    );
                wrapper && self.typed_identity(cx, arguments.type_at(0))
            }
            _ => false,
        }
    }

    fn identity<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        node: HirId,
        span: Span,
        value: Ty<'tcx>,
        definition: DefId,
        flow: &mut Flow,
    ) {
        let contracted = declared(cx.tcx, definition, "identity").is_some()
            || cx
                .tcx
                .opt_associated_item(definition)
                .and_then(|item| item.trait_item_def_id())
                .is_some_and(|item| declared(cx.tcx, item, "identity").is_some());
        if contracted && !self.typed_identity(cx, value) {
            self.diagnostic(
                cx,
                node,
                span,
                NODE_IDENTITY,
                format!(
                    "declared node identity uses {value}; use the shared typed node vocabulary at \
                     this owning interface"
                ),
                flow,
            );
        }
    }

    pub fn parameters<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        body: &rustc_hir::Body<'tcx>,
        flow: &mut Flow,
    ) {
        for parameter in body.params {
            if cx.tcx.hir_attrs(parameter.hir_id).iter().any(|attribute| {
                attribute.path_matches(&[Symbol::intern("nervix"), Symbol::intern("identity")])
            }) {
                let value = cx.typeck_results().pat_ty(parameter.pat);
                if !self.typed_identity(cx, value) {
                    self.diagnostic(
                        cx,
                        parameter.hir_id,
                        parameter.span,
                        NODE_IDENTITY,
                        format!(
                            "declared node identity parameter uses {value}; use the shared typed \
                             node vocabulary"
                        ),
                        flow,
                    );
                }
            }
        }
    }

    pub fn binding(&mut self, local: &rustc_hir::LetStmt<'_>) {
        if let rustc_hir::PatKind::Binding(_, binding, _, _) = local.pat.kind
            && let Some(initializer) = local.init
        {
            self.bindings.insert(binding, initializer.hir_id);
        }
    }

    pub fn signature(
        &mut self,
        cx: &LateContext<'_>,
        owner: rustc_span::def_id::LocalDefId,
        span: Span,
        flow: &mut Flow,
    ) {
        if !matches!(cx.tcx.def_kind(owner), DefKind::Fn | DefKind::AssocFn) {
            return;
        }
        let signature = cx.tcx.fn_sig(owner).instantiate_identity().skip_norm_wip();
        let signature = cx.tcx.instantiate_bound_regions_with_erased(signature);
        let node = cx.tcx.local_def_id_to_hir_id(owner);
        for value in signature.inputs_and_output {
            self.check_type(cx, node, span, value, flow);
        }
        self.identity(cx, node, span, signature.output(), owner.to_def_id(), flow);
    }

    pub fn alias(&mut self, cx: &LateContext<'_>, item: &rustc_hir::Item<'_>, flow: &mut Flow) {
        if matches!(item.kind, rustc_hir::ItemKind::TyAlias(..)) {
            let value = cx
                .tcx
                .type_of(item.owner_id)
                .instantiate_identity()
                .skip_norm_wip();
            self.check_type(cx, item.hir_id(), item.span, value, flow);
            self.identity(
                cx,
                item.hir_id(),
                item.span,
                value,
                item.owner_id.to_def_id(),
                flow,
            );
        }
    }

    pub fn expression<'tcx>(
        &mut self,
        cx: &LateContext<'tcx>,
        expression: &'tcx Expr<'tcx>,
        flow: &mut Flow,
    ) {
        self.check_type(
            cx,
            expression.hir_id,
            expression.span,
            cx.typeck_results().expr_ty(expression),
            flow,
        );
        if let ExprKind::Path(path) = expression.kind
            && let Res::Def(_, definition) = cx.qpath_res(&path, expression.hir_id)
        {
            self.reference(cx, expression.hir_id, expression.span, definition, flow);
        }
        if let ExprKind::MethodCall(..) = expression.kind
            && let Some(definition) = cx.typeck_results().type_dependent_def_id(expression.hir_id)
        {
            self.reference(cx, expression.hir_id, expression.span, definition, flow);
        }
        if let ExprKind::Call(function, _) = expression.kind
            && let ty::FnDef(definition, _) = cx.typeck_results().expr_ty(function).kind()
        {
            self.reference(cx, expression.hir_id, expression.span, *definition, flow);
        }
        if let ExprKind::Match(head, _, MatchSource::AwaitDesugar) = expression.kind {
            let mut waits = AwaitCalls {
                cx,
                rules: self,
                owner: cx.tcx.hir_enclosing_body_owner(expression.hir_id),
                node: expression.hir_id,
                span: expression.span,
                visited: HashSet::new(),
            };
            rustc_hir::intravisit::Visitor::visit_expr(&mut waits, head);
        }
    }

    fn future<'tcx>(&self, cx: &LateContext<'tcx>, value: Ty<'tcx>) -> bool {
        let Some(future) = cx.tcx.lang_items().future_trait() else {
            return false;
        };
        let (infcx, environment) = cx.tcx.infer_ctxt().build_with_typing_env(cx.typing_env());
        type_known_to_meet_bound_modulo_regions(&infcx, environment, value, future)
    }

    fn cluster_call(&self, cx: &LateContext<'_>, expression: &Expr<'_>) -> Option<DefId> {
        let (definition, arguments, receiver) = match expression.kind {
            ExprKind::MethodCall(_, receiver, _, _) => (
                cx.typeck_results()
                    .type_dependent_def_id(expression.hir_id)?,
                cx.typeck_results().node_args(expression.hir_id),
                cx.typeck_results().expr_ty_adjusted(receiver),
            ),
            ExprKind::Call(function, arguments) => {
                let ty::FnDef(definition, substitutions) =
                    cx.typeck_results().expr_ty(function).kind()
                else {
                    return None;
                };
                let receiver = cx.typeck_results().expr_ty_adjusted(arguments.first()?);
                (*definition, substitutions.skip_binder(), receiver)
            }
            _ => return None,
        };
        let selected = match ty::Instance::try_resolve(
            cx.tcx,
            cx.typing_env().with_post_analysis_normalized(cx.tcx),
            definition,
            arguments,
        ) {
            Ok(Some(instance)) => instance.def_id(),
            _ => definition,
        };
        if (self.has_role(cx, receiver, "cluster_access")
            || value_role(cx.tcx, definition, "cluster_access")
            || value_role(cx.tcx, selected, "cluster_access"))
            && self.future(cx, cx.typeck_results().expr_ty(expression))
        {
            Some(selected)
        } else {
            None
        }
    }

    pub fn finish(&mut self, cx: &LateContext<'_>, flow: &mut Flow) {
        let recurring = flow.recurring_owners(cx.tcx);
        let mut pending: Vec<_> = flow
            .nodes()
            .filter(|owner| {
                recurring.contains(owner)
                    && layer(cx.tcx, owner.to_def_id()) == Some(Layer::DataPlane)
            })
            .collect();
        let mut execution = HashSet::new();
        while let Some(owner) = pending.pop() {
            if execution.insert(owner) {
                pending.extend(flow.callees(owner));
            }
        }
        let waits = std::mem::take(&mut self.cluster_waits);
        for wait in waits {
            if execution.contains(&wait.owner) && recurring.contains(&wait.owner) {
                self.diagnostic(
                    cx,
                    wait.node,
                    wait.span,
                    CLUSTER_AWAIT,
                    format!(
                        "recurring execution awaits {}; retain the published cluster observation \
                         before processing",
                        cx.tcx.def_path_str(wait.definition)
                    ),
                    flow,
                );
            }
        }
    }
}

struct AwaitCalls<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    rules: &'a mut ArchitectureRules,
    owner: rustc_span::def_id::LocalDefId,
    node: HirId,
    span: Span,
    visited: HashSet<HirId>,
}

impl<'tcx> rustc_hir::intravisit::Visitor<'tcx> for AwaitCalls<'_, 'tcx> {
    fn visit_expr(&mut self, expression: &'tcx Expr<'tcx>) {
        if !self.visited.insert(expression.hir_id) {
            return;
        }
        if let ExprKind::Path(path) = expression.kind
            && let Res::Local(binding) = self.cx.qpath_res(&path, expression.hir_id)
            && let Some(initializer) = self.rules.bindings.get(&binding)
            && let Node::Expr(initializer) = self.cx.tcx.hir_node(*initializer)
        {
            self.visit_expr(initializer);
        }
        if let Some(definition) = self.rules.cluster_call(self.cx, expression) {
            self.rules.cluster_waits.push(ClusterWait {
                owner: self.owner,
                node: self.node,
                span: self.span,
                definition,
            });
        }
        rustc_hir::intravisit::walk_expr(self, expression);
    }
}
